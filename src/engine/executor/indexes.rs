use super::*;
use crate::planner::index_bounds::{
    target_for_index_column, ColumnRange as EngineColumnRange,
    ConstraintValue as IndexConstraintValue,
};

pub(super) fn index_access_residual_filter(
    table: &TableSchema,
    index: &IndexSchema,
    access: &EngineIndexAccess,
    expr: &Expr,
) -> Option<Expr> {
    let (low, high) = index_access_bounds(access);
    (!crate::planner::index_residual::all_covered(expr, table, index, low.as_ref(), high.as_ref()))
        .then(|| expr.clone())
}

pub(super) fn index_access_bounds(
    access: &EngineIndexAccess,
) -> (Option<IndexBound>, Option<IndexBound>) {
    match access {
        EngineIndexAccess::Lookup { key, .. } => {
            let bound = IndexBound {
                key: key.clone(),
                inclusive: true,
            };
            (Some(bound.clone()), Some(bound))
        }
        EngineIndexAccess::Range { low, high } => (low.clone(), high.clone()),
    }
}

pub(super) fn index_access_covers_all_conjuncts(
    table: &TableSchema,
    index: &IndexSchema,
    selection: &Expr,
    key: &IndexKey,
) -> bool {
    let bound = IndexBound {
        key: key.clone(),
        inclusive: true,
    };
    crate::planner::index_residual::all_covered(selection, table, index, Some(&bound), Some(&bound))
}

/// [`index_access_covers_all_conjuncts`] for a reusable plan whose table,
/// index, selection and key never change: the proof then depends only on the
/// installed parameters' lossless domains, so remember the last answer per
/// domain signature in `memo` (shared by every session using the plan).
pub(super) fn index_access_covers_all_conjuncts_memo(
    memo: &std::sync::atomic::AtomicU64,
    table: &TableSchema,
    index: &IndexSchema,
    selection: &Expr,
    key: &IndexKey,
) -> bool {
    use std::sync::atomic::Ordering;
    const VALID: u64 = 1 << 63;
    const COVERED: u64 = 1 << 62;
    let Some(signature) = crate::planner::index_residual::param_domain_signature() else {
        return index_access_covers_all_conjuncts(table, index, selection, key);
    };
    let cached = memo.load(Ordering::Relaxed);
    if cached & VALID != 0 && cached & !(VALID | COVERED) == signature {
        return cached & COVERED != 0;
    }
    let covered = index_access_covers_all_conjuncts(table, index, selection, key);
    memo.store(
        VALID | if covered { COVERED } else { 0 } | signature,
        Ordering::Relaxed,
    );
    covered
}

/// Compile-time proof for a reusable point plan. Runtime binding still has
/// to pass the lossless-domain proof before entering the mutation kernel.
pub(super) fn index_access_template_covers_all_conjuncts(
    table: &TableSchema,
    index: &IndexSchema,
    selection: &Expr,
    key: &IndexKey,
) -> bool {
    if let Expr::Binary {
        left,
        op: BinaryOp::And,
        right,
    } = selection
    {
        return index_access_template_covers_all_conjuncts(table, index, left, key)
            && index_access_template_covers_all_conjuncts(table, index, right, key);
    }
    if let Expr::Binary {
        left,
        op: BinaryOp::Eq,
        right,
    } = selection
    {
        let pair = expr_column_name(left)
            .map(|column| (column, right.as_ref()))
            .or_else(|| expr_column_name(right).map(|column| (column, left.as_ref())));
        if let Some((column, Expr::Placeholder(parameter))) = pair {
            if let Some(position) = index
                .columns
                .iter()
                .position(|name| name.eq_ignore_ascii_case(column))
            {
                return index.column_prefix_len(position).is_none()
                    && index.key_expression(position).is_none()
                    && key.0.get(position)
                        == Some(&KeyPart::Param {
                            idx: *parameter,
                            target: target_for_index_column(table, index, position),
                        });
            }
        }
    }
    index_access_covers_all_conjuncts(table, index, selection, key)
}

/// A single source of truth for write candidate access and DML EXPLAIN.
pub(super) enum WriteCandidateAccess<'a> {
    Single(&'a IndexSchema, EngineIndexAccess),
    Multi(crate::planner::IndexAccess),
    Merge(crate::planner::IndexMergeNode),
}

pub(super) fn plan_write_candidates<'a>(
    table: &'a TableSchema,
    expr: &Expr,
) -> Option<WriteCandidateAccess<'a>> {
    let single = best_index_access(table, expr);
    let unique_point = single.as_ref().is_some_and(|(index, access)| {
        index.unique && matches!(access, EngineIndexAccess::Lookup { exact: true, .. })
    });
    if !unique_point {
        if let Some(access) = crate::planner::write_in_index_access(table, expr) {
            return Some(WriteCandidateAccess::Multi(access));
        }
    }
    if let Some((index, access)) = single {
        return Some(WriteCandidateAccess::Single(index, access));
    }
    let (union_allowed, sort_union_allowed, intersection_allowed) =
        crate::planner::index_merge_switches();
    if let Some(root) = build_write_index_merge_node(
        table,
        expr,
        union_allowed,
        sort_union_allowed,
        intersection_allowed,
    )
    .filter(|root| root.leaf_count() >= 2)
    {
        return Some(WriteCandidateAccess::Merge(root));
    }
    let predicate = extract_index_predicate(expr)?;
    let index = table.best_index_for_columns(std::slice::from_ref(&predicate.column))?;
    let column = &table.columns[table.column_index(&predicate.column).ok()?];
    let target = target_for_index_column(table, index, 0);
    if !crate::planner::index_bounds::concrete_comparison_preserved(
        target,
        &IndexConstraintValue::Concrete(predicate.value.clone()),
    ) {
        return None;
    }
    let key = IndexKey::from(vec![KeyPart::from_column_value_with_collation(
        &predicate.value,
        &column.data_type,
        column.collation.as_deref(),
    )]);
    let access = match predicate.op {
        BinaryOp::Eq => EngineIndexAccess::Lookup { key, exact: true },
        BinaryOp::Gt | BinaryOp::GtEq => EngineIndexAccess::Range {
            low: Some(IndexBound {
                key,
                inclusive: predicate.op == BinaryOp::GtEq,
            }),
            high: None,
        },
        BinaryOp::Lt | BinaryOp::LtEq => EngineIndexAccess::Range {
            low: None,
            high: Some(IndexBound {
                key,
                inclusive: predicate.op == BinaryOp::LtEq,
            }),
        },
        _ => return None,
    };
    Some(WriteCandidateAccess::Single(index, access))
}

pub(super) fn candidate_row_ids_for_write(
    db: &Arc<Database>,
    txn: TxnView,
    table: &TableSchema,
    selection: Option<&Expr>,
) -> Result<Option<Vec<RowId>>, QueryError> {
    let Some(access) = selection.and_then(|expr| plan_write_candidates(table, expr)) else {
        return Ok(None);
    };
    let ids = match access {
        WriteCandidateAccess::Single(index, access) => match access {
            EngineIndexAccess::Lookup { key, exact } => {
                if exact && (index.unique || index.primary) && !key.contains_null() {
                    db.lookup_unique_index_for_write_one(txn, table, index, key)?
                        .into_iter()
                        .collect()
                } else if exact {
                    db.lookup_index_for_write(txn, table, index, key)?
                } else {
                    db.prefix_index(table, index, key)?
                }
            }
            EngineIndexAccess::Range { low, high } => {
                db.range_index_bounds(table, index, low, high)?
            }
        },
        WriteCandidateAccess::Multi(access) => {
            let crate::planner::IndexAccessKind::MultiLookup { keys, exact } = access.kind else {
                unreachable!("write IN access is a finite set of probes")
            };
            let index = &access.index;
            let mut ids = if exact && index.unique && !table.is_partitioned() {
                db.lookup_unique_index_for_write_many(txn, table, index, &keys)?
                    .into_iter()
                    .map(|(_, id)| id)
                    .collect::<Vec<_>>()
            } else {
                let mut ids = Vec::new();
                for key in keys {
                    crate::engine::cancel::check_cancelled()?;
                    let local = if exact {
                        db.lookup_index_for_write(txn, table, index, key)?
                    } else {
                        let bound = IndexBound {
                            key,
                            inclusive: true,
                        };
                        db.range_index_bounds(table, index, Some(bound.clone()), Some(bound))?
                    };
                    ids.extend(local);
                }
                ids
            };
            ids.sort_unstable();
            ids.dedup();
            crate::engine::executor::mem_budget::try_charge(
                ids.capacity().saturating_mul(std::mem::size_of::<RowId>()),
            )?;
            ids
        }
        WriteCandidateAccess::Merge(root) => {
            let ids = crate::engine::executor::select::plan::execute_index_merge_row_ids(
                db, txn, table, &root,
            )?;
            crate::engine::executor::mem_budget::try_charge(
                ids.capacity().saturating_mul(std::mem::size_of::<RowId>()),
            )?;
            ids
        }
    };
    crate::engine::api::add_statement_dml_candidate_rows(ids.len() as u64);
    Ok(Some(ids))
}

fn write_merge_leaf(table: &TableSchema, expr: &Expr) -> Option<crate::planner::IndexMergeNode> {
    let (index, access) = best_index_access(table, expr)?;
    let (access, row_id_ordered) = match access {
        EngineIndexAccess::Lookup { key, exact } => (
            crate::planner::IndexAccessKind::Lookup { key, exact },
            exact,
        ),
        EngineIndexAccess::Range { low, high } => {
            (crate::planner::IndexAccessKind::Range { low, high }, false)
        }
    };
    Some(crate::planner::IndexMergeNode::Leaf(
        crate::planner::IndexMergeLeaf {
            index: index.clone(),
            access,
            row_id_ordered,
        },
    ))
}

fn build_write_index_merge_node(
    table: &TableSchema,
    expr: &Expr,
    union_allowed: bool,
    sort_union_allowed: bool,
    intersection_allowed: bool,
) -> Option<crate::planner::IndexMergeNode> {
    match expr {
        Expr::Binary {
            left,
            op: BinaryOp::Or,
            right,
        } => {
            let left = build_write_index_merge_node(
                table,
                left,
                union_allowed,
                sort_union_allowed,
                intersection_allowed,
            )?;
            let right = build_write_index_merge_node(
                table,
                right,
                union_allowed,
                sort_union_allowed,
                intersection_allowed,
            )?;
            let needs_sort = !left.is_row_id_ordered() || !right.is_row_id_ordered();
            if (needs_sort && !sort_union_allowed) || (!needs_sort && !union_allowed) {
                return None;
            }
            Some(crate::planner::IndexMergeNode::Union {
                children: vec![left, right],
                sort_union: needs_sort,
            })
        }
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => {
            if left == right {
                return build_write_index_merge_node(
                    table,
                    left,
                    union_allowed,
                    sort_union_allowed,
                    intersection_allowed,
                );
            }
            let left = build_write_index_merge_node(
                table,
                left,
                union_allowed,
                sort_union_allowed,
                intersection_allowed,
            );
            let right = build_write_index_merge_node(
                table,
                right,
                union_allowed,
                sort_union_allowed,
                intersection_allowed,
            );
            match (left, right) {
                (Some(left), Some(right)) if intersection_allowed => {
                    Some(crate::planner::IndexMergeNode::Intersection {
                        children: vec![left, right],
                    })
                }
                (Some(node), _) | (_, Some(node)) => Some(node),
                _ => None,
            }
        }
        _ => write_merge_leaf(table, expr),
    }
}

/// Index candidates with an unambiguous physical row identity. Partitioned
/// indexes are probed one storage at a time because their RowIds are local to
/// each heap; returning a bare union would alias equal RowIds across parts.
pub(super) fn candidate_row_refs_for_write(
    db: &Arc<Database>,
    txn: TxnView,
    table: &TableSchema,
    selection: Option<&Expr>,
) -> Result<Option<Vec<crate::storage::DmlRowRef>>, QueryError> {
    let _partition_scope = super::partition_router::push_filter_partition_scope(table, selection);
    if !table.is_partitioned() {
        return candidate_row_ids_for_write(db, txn, table, selection).map(|candidates| {
            candidates.map(|rows| {
                rows.into_iter()
                    .map(|row_id| crate::storage::DmlRowRef {
                        storage_id: table.id,
                        row_id,
                    })
                    .collect()
            })
        });
    }

    let mut out = Vec::new();
    for storage_id in super::partition_router::in_scope_storage_ids(table) {
        let _scope = super::partition_router::push_pruned_partitions(vec![storage_id]);
        let Some(rows) = candidate_row_ids_for_write(db, txn, table, selection)? else {
            return Ok(None);
        };
        out.extend(
            rows.into_iter()
                .map(|row_id| crate::storage::DmlRowRef { storage_id, row_id }),
        );
    }
    Ok(Some(out))
}

pub(super) enum EngineIndexAccess {
    Lookup {
        key: IndexKey,
        exact: bool,
    },
    Range {
        low: Option<IndexBound>,
        high: Option<IndexBound>,
    },
}

/// A literal-or-placeholder operand collected from a WHERE predicate.
/// Phase 2.5: keep the placeholder reference symbolic through planning
/// so the resulting `IndexKey` can carry a `KeyPart::Param`. The plan
/// becomes parameter-independent (cacheable across EXECUTEs); the
/// storage boundary calls `IndexKey::resolve_params` to produce a
/// concrete key against the live thread-local parameter context.
#[inline]
fn constraint_key_part_for_index(
    value: &IndexConstraintValue,
    target: crate::storage::ParamTarget,
    index: &IndexSchema,
    pos: usize,
) -> Option<KeyPart> {
    // OLTP fast path: indexes without prefix metadata bypass the
    // per-call `column_prefix_len` chain and the truncation pass.
    if index.column_prefix_lengths.is_empty() {
        return value.clone().try_into_key_part(target);
    }
    let part = value.clone().try_into_key_part(target)?;
    // Deferred `KeyPart::Param` parts can't be truncated until the
    // param is bound. The storage boundary (`resolve_index_key` /
    // `resolve_index_bound`) re-applies `apply_prefix_to_key_part`
    // after `KeyPart::resolve_param` materialises the literal value,
    // so passing the un-truncated `Param` here is safe.
    if matches!(value, IndexConstraintValue::Param(_)) {
        return Some(part);
    }
    Some(index.apply_prefix_to_key_part(pos, part))
}

/// Extract a concrete value or a placeholder reference from an
/// expression operand. Mirrors `expr_literal_value` but, instead of
/// resolving `Expr::Placeholder` through the thread-local context (which
/// would bake the current param value into the plan), preserves the
/// placeholder index so the plan stays parameter-independent.
pub(super) fn expr_to_index_constraint(expr: &Expr) -> Option<IndexConstraintValue> {
    match expr {
        Expr::Literal(_) | Expr::BoundValue(_) => {
            expr_literal_value(expr).map(IndexConstraintValue::Concrete)
        }
        Expr::Placeholder(idx) => Some(IndexConstraintValue::Param(*idx)),
        Expr::Unary {
            op: crate::parser::UnaryOp::Neg,
            expr: inner,
        } => match inner.as_ref() {
            Expr::Literal(crate::parser::Literal::Int(v)) => {
                Some(IndexConstraintValue::Concrete(Value::Int(-*v)))
            }
            _ => None,
        },
        _ => None,
    }
}

pub(super) fn best_index_access<'a>(
    table: &'a TableSchema,
    expr: &Expr,
) -> Option<(&'a IndexSchema, EngineIndexAccess)> {
    let mut eq_values = HashMap::new();
    let mut ranges = HashMap::new();
    collect_index_constraints(table, expr, &mut eq_values, &mut ranges);
    let mut best = None;
    let mut best_score = 0usize;
    for index in table.iter_queryable_indexes() {
        let mut parts = Vec::new();
        for (pos, column) in index.columns.iter().enumerate() {
            let Some(value) = eq_values.get(&normalize_expr_column(column)) else {
                break;
            };
            let target = target_for_index_column(table, index, pos);
            if !crate::planner::index_bounds::concrete_comparison_preserved(target, value) {
                break;
            }
            let Some(part) = constraint_key_part_for_index(value, target, index, pos) else {
                break;
            };
            parts.push(part);
        }
        let mut score = parts.len() * 2;
        if parts.len() == index.columns.len() {
            if !parts.is_empty() && score > best_score {
                best_score = score;
                best = Some((
                    index,
                    EngineIndexAccess::Lookup {
                        key: IndexKey::from(parts),
                        exact: index.key_parts_are_exact(index.columns.len()),
                    },
                ));
            }
            continue;
        }
        let range_pos = parts.len();
        let range_column = index
            .columns
            .get(range_pos)
            .map(|column| normalize_expr_column(column));
        let range = range_column.as_ref().and_then(|column| {
            ranges.get(column).filter(|range| {
                index.column_prefix_len(range_pos).is_none()
                    || range.like_prefix.is_some()
                    || range.like_param.is_some()
            })
        });
        if let Some(range) = range {
            score += 1;
            let range_target = target_for_index_column(table, index, range_pos);
            if !range.low.iter().chain(range.high.iter()).all(|(value, _)| {
                crate::planner::index_bounds::concrete_comparison_preserved(range_target, value)
            }) {
                continue;
            }
            let Some((low, high)) = crate::planner::index_bounds::build_range_bounds(
                index,
                &parts,
                range_pos,
                range_target,
                range,
                IndexConstraintValue::try_into_key_part,
            ) else {
                continue;
            };
            if score > best_score {
                best_score = score;
                best = Some((index, EngineIndexAccess::Range { low, high }));
            }
        } else if !parts.is_empty() && score > best_score {
            best_score = score;
            best = Some((
                index,
                EngineIndexAccess::Lookup {
                    key: IndexKey::from(parts),
                    exact: false,
                },
            ));
        }
    }
    best
}

pub(super) fn collect_index_constraints(
    table: &TableSchema,
    expr: &Expr,
    eq_values: &mut HashMap<String, IndexConstraintValue>,
    ranges: &mut HashMap<String, EngineColumnRange>,
) {
    crate::planner::index_bounds::collect_index_constraints(
        table,
        expr,
        eq_values,
        ranges,
        &expr_to_index_constraint,
    );
}

pub(super) fn normalize_expr_column(column: &str) -> String {
    column.to_ascii_lowercase()
}

pub(super) struct IndexPredicate {
    column: String,
    op: BinaryOp,
    value: Value,
}

pub(super) fn extract_index_predicate(expr: &Expr) -> Option<IndexPredicate> {
    match expr {
        Expr::Binary { left, op, right } if *op == BinaryOp::And => {
            extract_index_predicate(left).or_else(|| extract_index_predicate(right))
        }
        Expr::Binary { left, op, right } => {
            if let (Some(column), Some(value)) = (expr_column_name(left), expr_literal_value(right))
            {
                return Some(IndexPredicate {
                    column: column.to_string(),
                    op: *op,
                    value,
                });
            }
            if let (Some(value), Some(column)) = (expr_literal_value(left), expr_column_name(right))
            {
                return Some(IndexPredicate {
                    column: column.to_string(),
                    op: flip_cmp(*op),
                    value,
                });
            }
            None
        }
        _ => None,
    }
}
