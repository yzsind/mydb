//! index join execution and preparation.

use super::*;
mod fused_probe;
pub(in crate::engine::executor) use crate::parser::boolean::collect_expr_conjuncts;

/// Per-INLJ pre-compiled lookup plan. Holds everything we can resolve
/// *before* the outer loop starts so the per-outer-row hot path only
/// does: eval N expressions, build a KeyPart from a pre-resolved
/// `DataTypeKind`, and assemble `IndexBound`s.
///
/// Replaces `dependent_join_bounds_for_row`'s per-row AST walk +
/// conjunct scanning + `column_index(name)` lookup + `Vec::clone` of
/// `eq_values` for every outer row.
///
/// See [`compile_index_lookup_plan`] for how this is built and
/// [`build_bounds_from_plan`] for the per-row evaluation.
pub(in crate::engine::executor::select) struct IndexLookupPlan {
    /// For each consecutive equality-prefix column of the index, the
    /// expression we eval against the outer (left) row to obtain the
    /// equality value. Same length as `eq_targets`.
    pub(super) eq_exprs: Vec<Expr>,
    /// Canonical storage key shape, including unsignedness and collation.
    pub(super) eq_targets: Vec<crate::storage::ParamTarget>,
    /// Optional trailing range column (must immediately follow the
    /// equality prefix per the B-tree key order).
    pub(super) range: Option<RangeSpec>,
    /// Conjuncts of the original join condition that were NOT consumed
    /// by the index bounds — combined with AND. When the index lookup
    /// is exact, this is `None` and we skip `eval_plan_bool` entirely
    /// (Fix C: residual stripping).
    pub(super) residual: Option<Expr>,
    /// True when the original condition contained at least one
    /// `IS NULL` / `<=> NULL` referencing an indexed equality column.
    /// We surface this as an empty-result shortcut because B-tree
    /// keys never compare equal to SQL `NULL`.
    pub(super) forced_empty: bool,
}

impl IndexLookupPlan {
    pub(in crate::engine::executor::select) fn scalar_equality_expr(&self) -> Option<&Expr> {
        (!self.forced_empty && self.eq_exprs.len() == 1).then(|| &self.eq_exprs[0])
    }

    pub(in crate::engine::executor::select) fn is_full_equality_for(
        &self,
        index: &IndexSchema,
    ) -> bool {
        !self.forced_empty && self.range.is_none() && self.eq_exprs.len() == index.columns.len()
    }
}

pub(in crate::engine::executor::select) struct RangeSpec {
    pub(super) target: crate::storage::ParamTarget,
    pub(super) low: Option<(Expr, bool)>,
    pub(super) high: Option<(Expr, bool)>,
}

/// WS-N4: whether the expression is constant (references no column/param/dynamic binding).
/// When both bounds of the range are constant, it can be promoted from `IndexLookupPlan::range` to a right-only
/// residual so the batched path applies (cost: losing the range's index selectivity in exchange for multi-keyed
/// lookup + outer-parallel amortization).
pub(super) fn expr_is_constant(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_)
        | Expr::BoundValue(_)
        | Expr::UserVariable(_)
        | Expr::SystemVariable { .. }
        | Expr::Placeholder(_)
        | Expr::Default(_)
        | Expr::DefaultQualified(_) => true,
        Expr::Unary { expr, .. } => expr_is_constant(expr),
        Expr::Binary { left, right, .. } => expr_is_constant(left) && expr_is_constant(right),
        Expr::IsNull(e) | Expr::IsNotNull(e) => expr_is_constant(e),
        Expr::Function { args, .. } => args.iter().all(expr_is_constant),
        Expr::Tuple(items) => items.iter().all(expr_is_constant),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand.as_ref().is_none_or(|e| expr_is_constant(e))
                && conditions.iter().all(expr_is_constant)
                && results.iter().all(expr_is_constant)
                && else_result.as_ref().is_none_or(|e| expr_is_constant(e))
        }
        Expr::Between {
            expr, low, high, ..
        } => expr_is_constant(expr) && expr_is_constant(low) && expr_is_constant(high),
        Expr::Like { expr, pattern, .. } => expr_is_constant(expr) && expr_is_constant(pattern),
        Expr::InList { expr, list, .. } => {
            expr_is_constant(expr) && list.iter().all(expr_is_constant)
        }
        // Any shape containing Identifier / Subquery / Wildcard / functions referencing columns is non-constant.
        _ => false,
    }
}

/// `MYDB_INLJ_BATCH_RANGE_PROMOTE` kill switch. Default on: constant ranges are promoted to a right-only
/// residual to trigger batched; off goes legacy (the range stays in the plan, batched refuses).
pub(super) fn inlj_batch_range_promote_enabled() -> bool {
    !matches!(
        crate::util::env::var("MYDB_INLJ_BATCH_RANGE_PROMOTE")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "off" | "0" | "false" | "no" | "disable" | "disabled"
    )
}

/// Walk the join condition once and produce an [`IndexLookupPlan`].
/// Consumed conjuncts (those captured into the index bounds) are
/// removed from the residual, so the per-row code only re-evaluates
/// predicates the index can't enforce on its own.
pub(in crate::engine::executor::select) fn compile_index_lookup_plan(
    condition: &Expr,
    right_table: &PlanTable,
    right_index: &IndexSchema,
) -> Result<IndexLookupPlan, QueryError> {
    let conjunct_refs = expr_conjuncts(condition);
    // Owned copies so the per-row hot path doesn't need to keep the
    // condition AST alive separately.
    let conjuncts: Vec<Expr> = conjunct_refs.into_iter().cloned().collect();
    let mut consumed = vec![false; conjuncts.len()];

    let mut eq_exprs = Vec::new();
    let mut eq_targets = Vec::new();
    let mut range: Option<RangeSpec> = None;
    let mut forced_empty = false;

    // Look up the collation of a base column on the inner table, used when
    // the index key part is `IndexKeyPart::Column` and the column is a
    // collated string type. Functional / expression key parts have no
    // associated column collation — their persisted entries always go
    // through `KeyPart::from_column_value` (no collation), so we leave
    // those slots `None`.
    let column_collation = |name: &str| -> Option<String> {
        right_table
            .schema
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
            .and_then(|c| c.collation.clone())
    };

    for (pos, column) in right_index.columns.iter().enumerate() {
        // Per-part matcher: a column part compares by name, a
        // functional part compares by AST fingerprint so
        // `LOWER(t.c) = left_x` lights up an
        // `INDEX ((LOWER(c)))` probe.
        let part = right_index.key_part(pos);
        let matches_part = |side: &Expr| -> bool {
            match part {
                Some(crate::storage::IndexKeyPart::Column(name)) => {
                    expr_matches_plan_table_column(side, right_table, name)
                }
                Some(crate::storage::IndexKeyPart::Expression(expr_part)) => {
                    match expr_part.ast.as_ref() {
                        Some(ast) => {
                            // Catalog AST has no table alias; ON
                            // conditions usually do — strip the inner
                            // table's qualifier from `side` before
                            // fingerprinting.
                            let stripped =
                                crate::planner::strip_qualifier_for_table_pub(side, right_table);
                            crate::engine::expr_program::expr_fingerprint(&stripped)
                                == crate::engine::expr_program::expr_fingerprint(ast)
                        }
                        None => false,
                    }
                }
                None => false,
            }
        };
        let part_data_type = || -> Result<crate::storage::DataTypeKind, QueryError> {
            match part {
                Some(crate::storage::IndexKeyPart::Column(name)) => {
                    column_data_type(&right_table.schema, name)
                }
                Some(crate::storage::IndexKeyPart::Expression(expr_part)) => {
                    Ok(expr_part.data_type.clone())
                }
                None => column_data_type(&right_table.schema, column),
            }
        };
        let part_collation = || -> Option<String> {
            match part {
                Some(crate::storage::IndexKeyPart::Column(name)) => column_collation(name),
                Some(crate::storage::IndexKeyPart::Expression(_)) => None,
                None => column_collation(column),
            }
        };

        let part_target = || -> Result<crate::storage::ParamTarget, QueryError> {
            let unsigned = match part {
                Some(crate::storage::IndexKeyPart::Column(name)) => {
                    right_table.schema.columns[right_table.schema.column_index(name)?].unsigned
                }
                _ => false,
            };
            Ok(crate::storage::ParamTarget::for_column_with_collation(
                &part_data_type()?,
                unsigned,
                part_collation().as_deref(),
            ))
        };

        // Equality: pick the first matching `=` conjunct that hasn't
        // been used yet.
        let mut matched_idx: Option<(usize, Expr)> = None;
        for (i, c) in conjuncts.iter().enumerate() {
            if consumed[i] {
                continue;
            }
            let Expr::Binary {
                left,
                op: BinaryOp::Eq,
                right,
            } = c
            else {
                continue;
            };
            if matches_part(left) && !expr_reads_plan_table(right, right_table) {
                matched_idx = Some((i, (**right).clone()));
                break;
            }
            if matches_part(right) && !expr_reads_plan_table(left, right_table) {
                matched_idx = Some((i, (**left).clone()));
                break;
            }
        }
        if let Some((i, expr)) = matched_idx {
            eq_exprs.push(expr);
            eq_targets.push(part_target()?);
            consumed[i] = true;
            continue;
        }

        // No equality on this column. Try range bounds (low and/or
        // high) — this column becomes the trailing range column and
        // we stop walking the index prefix afterwards.
        let mut spec = RangeSpec {
            target: part_target()?,
            low: None,
            high: None,
        };
        // Track the conjunct idxs this range loop marked as consumed, and
        // whether all their value expressions are constant (used by WS-N4).
        let mut range_consumed_idx: Vec<usize> = Vec::new();
        let mut all_bounds_constant = true;
        for (i, c) in conjuncts.iter().enumerate() {
            if consumed[i] {
                continue;
            }
            let Expr::Binary { left, op, right } = c else {
                continue;
            };
            if !matches!(
                op,
                BinaryOp::Gt | BinaryOp::GtEq | BinaryOp::Lt | BinaryOp::LtEq
            ) {
                continue;
            }
            let (effective_op, value_expr) =
                if matches_part(left) && !expr_reads_plan_table(right, right_table) {
                    (*op, (**right).clone())
                } else if matches_part(right) && !expr_reads_plan_table(left, right_table) {
                    (flip_cmp(*op), (**left).clone())
                } else {
                    continue;
                };
            // Keep additional bounds as residual predicates. Consuming all
            // bounds while overwriting one side of RangeSpec silently drops
            // a stronger earlier predicate (for example x >= outer+10 AND
            // x >= outer+1). One indexed bound plus residuals is exact for
            // every supported key type, including non-orderable NULL bounds.
            if (matches!(effective_op, BinaryOp::Gt | BinaryOp::GtEq) && spec.low.is_some())
                || (matches!(effective_op, BinaryOp::Lt | BinaryOp::LtEq) && spec.high.is_some())
            {
                continue;
            }
            if !expr_is_constant(&value_expr) {
                all_bounds_constant = false;
            }
            match effective_op {
                BinaryOp::Gt => {
                    spec.low = Some((value_expr, false));
                    consumed[i] = true;
                    range_consumed_idx.push(i);
                }
                BinaryOp::GtEq => {
                    spec.low = Some((value_expr, true));
                    consumed[i] = true;
                    range_consumed_idx.push(i);
                }
                BinaryOp::Lt => {
                    spec.high = Some((value_expr, false));
                    consumed[i] = true;
                    range_consumed_idx.push(i);
                }
                BinaryOp::LtEq => {
                    spec.high = Some((value_expr, true));
                    consumed[i] = true;
                    range_consumed_idx.push(i);
                }
                _ => {}
            }
        }
        if spec.low.is_some() || spec.high.is_some() {
            // WS-N4 promotion: when all bounds are constant, give up using the range for
            // index access and leave the predicates to the residual, so that
            // `try_execute_batched_index_nested_loop_join` (which rejects plan.range)
            // admits the batched path. Cost: losing the range's "stop the scan at
            // the upper bound" fast path on the index, in exchange for multi-key
            // lookup amortization + outer parallelism + bloom + narrow-column filter pass (a net win for typical Q3/Q19 shapes). Fall back via `MYDB_INLJ_BATCH_RANGE_PROMOTE=off`.
            if all_bounds_constant && inlj_batch_range_promote_enabled() {
                for idx in &range_consumed_idx {
                    consumed[*idx] = false;
                }
                record_inlj_batch_range_promoted();
                // No range is written; skipping the break would not matter — no further
                // column is processed — but keep the break to match the original behaviour.
            } else {
                for idx in &range_consumed_idx {
                    consumed[*idx] = false;
                }
                range = Some(spec);
            }
        }
        break;
    }

    // Residual = conjuncts left after the bounds absorbed what they
    // could. Skip equality predicates trivially provable from constant
    // folding (e.g. `1=1`) by leaving combine_conjuncts to drop nothing.
    let residual_parts: Vec<Expr> = conjuncts
        .into_iter()
        .enumerate()
        .filter_map(|(i, c)| if consumed[i] { None } else { Some(c) })
        .collect();
    let residual = crate::planner::combine_conjuncts(residual_parts);

    // If the residual contains `column IS NULL` referencing one of the
    // eq-prefix columns we'd have already pushed an equality earlier,
    // but mixed predicates can still reach here. For safety we leave
    // the residual eval to catch them — no extra fast-path.
    let _ = &mut forced_empty;

    Ok(IndexLookupPlan {
        eq_exprs,
        eq_targets,
        range,
        residual,
        forced_empty,
    })
}

/// Classify whether the batched index-nested-loop probe applies, for
/// `EXPLAIN` display. Mirrors the real gate in
/// [`try_execute_batched_index_nested_loop_join`]: the decision is keyed
/// off the compiled [`IndexLookupPlan`], not a syntactic scan of the
/// join condition. `off(range)` therefore means a range bound on an
/// actual index column (which does disable batching); a range predicate
/// on a non-indexed column is only a residual filter and stays
/// `eligible`.
pub(in crate::engine::executor::select) fn index_nested_loop_batch_eligibility(
    condition: Option<&Expr>,
    right_table: &PlanTable,
    right_index: &IndexSchema,
) -> &'static str {
    let Some(condition) = condition else {
        return "off";
    };
    match compile_index_lookup_plan(condition, right_table, right_index) {
        Ok(plan) if plan.eq_exprs.is_empty() => "off",
        Ok(plan) if plan.range.is_some() => "off(range)",
        Ok(_) => "eligible",
        Err(_) => "off",
    }
}

/// Look up the `DataTypeKind` of a column by name. Mirrors the
/// `column_index` path used by `key_part_for_index_column`, but is
/// done once per compile rather than per outer row.
pub(super) fn column_data_type(
    schema: &TableSchema,
    column: &str,
) -> Result<DataTypeKind, QueryError> {
    let idx = schema.column_index(column)?;
    Ok(schema.columns[idx].data_type.clone())
}

pub(super) fn key_part_for_lookup_target(
    target: crate::storage::ParamTarget,
    value: &Value,
) -> Result<Option<KeyPart>, QueryError> {
    use crate::storage::ParamTarget;
    let part = match (target, value) {
        (_, Value::Null) => return Ok(None),
        (ParamTarget::Int, Value::Int(v)) => return Ok(Some(KeyPart::Int(*v))),
        (ParamTarget::UInt, Value::UInt(v)) => return Ok(Some(KeyPart::UInt(*v))),
        (ParamTarget::UInt, Value::Int(v)) if *v < 0 => return Ok(None),
        (ParamTarget::Int, Value::UInt(v)) if *v > i64::MAX as u64 => return Ok(None),
        (ParamTarget::Int | ParamTarget::UInt, Value::String(_) | Value::SmallString(_)) => {
            match text_integer_key_part(target, &value_to_string(value)) {
                Some(part) => part,
                None => return Ok(None),
            }
        }
        (ParamTarget::Int, _) => KeyPart::Int(int_value(value)?),
        (ParamTarget::UInt, _) => KeyPart::UInt(uint_value(value)?),
        (ParamTarget::Float32, Value::Float(v)) if v.is_finite() => {
            return KeyPart::for_target(value.clone(), target).map(Some)
        }
        (ParamTarget::Float64, Value::Double(v)) if v.is_finite() => {
            return KeyPart::for_target(value.clone(), target).map(Some)
        }
        (ParamTarget::Float32 | ParamTarget::Float64 | ParamTarget::Decimal { .. }, _) => {
            KeyPart::for_target(value.clone(), target)?
        }
        // A number that is not a valid compact temporal value cannot equal
        // any stored DATE/DATETIME/TIMESTAMP key: the probe matches nothing.
        (
            ParamTarget::Date | ParamTarget::DateTime | ParamTarget::Timestamp,
            Value::Int(_)
            | Value::UInt(_)
            | Value::Float(_)
            | Value::Double(_)
            | Value::Decimal(_)
            | Value::DecimalCompact(..)
            | Value::Decimal128(..)
            | Value::DecimalWide(_),
        ) => return Ok(KeyPart::for_target(value.clone(), target).ok()),
        _ => return KeyPart::for_target(value.clone(), target).map(Some),
    };
    let converted = part.to_value(&DataTypeKind::BigInt);
    // Equality probes may discard their residual only if the lookup key
    // still equals the original operand in SQL comparison semantics.
    Ok(
        (crate::engine::expr::compare_values(value, &converted)? == std::cmp::Ordering::Equal)
            .then_some(part),
    )
}

/// Integer index key addressed by a text operand. SQL compares an integer
/// column with text in the DOUBLE domain, so `'1e2'` and `'100.0'` address
/// key 100 while `'1.5'` or an out-of-range value cannot match any key.
fn text_integer_key_part(target: crate::storage::ParamTarget, text: &str) -> Option<KeyPart> {
    use crate::storage::ParamTarget;
    let integer = crate::util::decimal::parse_lenient_int(text);
    let value = if integer.clean {
        integer.value
    } else {
        let number = crate::util::decimal::parse_lenient_f64(text).value;
        if !number.is_finite() || number.fract() != 0.0 || number.abs() >= 2f64.powi(64) {
            return None;
        }
        number as i128
    };
    match target {
        ParamTarget::Int => i64::try_from(value).ok().map(KeyPart::Int),
        ParamTarget::UInt => u64::try_from(value).ok().map(KeyPart::UInt),
        _ => None,
    }
}

pub(super) fn lossless_range_key_part(
    target: crate::storage::ParamTarget,
    value: &Value,
) -> Option<KeyPart> {
    use crate::storage::ParamTarget;
    let exact = match (target, value) {
        (ParamTarget::Int, Value::Int(_)) | (ParamTarget::UInt, Value::UInt(_)) => true,
        (ParamTarget::Int, Value::UInt(v)) => *v <= i64::MAX as u64,
        (ParamTarget::UInt, Value::Int(v)) => *v >= 0,
        (ParamTarget::Float32, Value::Float(v)) => v.is_finite(),
        (ParamTarget::Float64, Value::Double(v)) => v.is_finite(),
        (ParamTarget::Float64, Value::Float(v)) => v.is_finite(),
        (ParamTarget::Decimal { scale, .. }, _) => {
            value.exact_decimal().is_some_and(|v| v.scale() <= scale)
        }
        (ParamTarget::Date, Value::Date(_))
        | (ParamTarget::Time, Value::Time(_))
        | (ParamTarget::DateTime, Value::DateTime(_))
        | (ParamTarget::Timestamp, Value::Timestamp(_))
        | (ParamTarget::Year, Value::Year(_))
        | (ParamTarget::Bytes, Value::Bytes(_)) => true,
        (
            ParamTarget::String | ParamTarget::CollatedString { .. },
            Value::String(_) | Value::SmallString(_),
        ) => true,
        (
            ParamTarget::CharString | ParamTarget::CollatedCharString { .. },
            Value::String(_) | Value::SmallString(_),
        ) => !value_to_string(value).ends_with(' '),
        _ => false,
    };
    exact
        .then(|| KeyPart::for_target(value.clone(), target).ok())
        .flatten()
}

/// Per-outer-row hot path: evaluate the pre-compiled expressions and
/// build the (low, high) `IndexBound` pair. `None` means no bounds can
/// be derived (e.g. eval produced NULL on an equality column → empty
/// match), in which case the caller treats the inner side as empty.
pub(in crate::engine::executor::select) fn build_bounds_from_plan(
    plan: &IndexLookupPlan,
    left_row: &PlanRow,
) -> Result<Option<(Option<IndexBound>, Option<IndexBound>)>, QueryError> {
    if plan.forced_empty {
        return Ok(None);
    }
    if plan.eq_exprs.is_empty() && plan.range.is_none() {
        return Ok(None);
    }
    let mut eq_parts: Vec<KeyPart> = Vec::with_capacity(plan.eq_exprs.len());
    for (expr, target) in plan.eq_exprs.iter().zip(&plan.eq_targets) {
        let value = eval_plan_expr(expr, left_row)?;
        if matches!(value, Value::Null) {
            return Ok(None);
        }
        let Some(part) = key_part_for_lookup_target(*target, &value)? else {
            return Ok(None);
        };
        eq_parts.push(part);
    }

    let mut low: Option<IndexBound> = None;
    let mut high: Option<IndexBound> = None;

    if let Some(range_spec) = &plan.range {
        for (bound, destination) in [(&range_spec.low, &mut low), (&range_spec.high, &mut high)] {
            if let Some((expr, inclusive)) = bound {
                let value = eval_plan_expr(expr, left_row)?;
                if matches!(value, Value::Null) {
                    return Ok(None);
                }
                // A lossy conversion must not narrow away valid rows. Keep
                // that side at the equality prefix and evaluate the original
                // range predicate on candidates instead.
                if let Some(part) = lossless_range_key_part(range_spec.target, &value) {
                    let mut parts = eq_parts.clone();
                    parts.push(part);
                    *destination = Some(IndexBound {
                        key: IndexKey::from(parts),
                        inclusive: *inclusive,
                    });
                }
            }
        }
    }

    if !eq_parts.is_empty() {
        let prefix = IndexBound {
            key: IndexKey::from(eq_parts),
            inclusive: true,
        };
        if low.is_none() {
            low = Some(prefix.clone());
        }
        if high.is_none() {
            high = Some(prefix);
        }
    }

    Ok(Some((low, high)))
}

pub(in crate::engine::executor::select) fn execute_index_nested_loop_join(
    db: &Arc<Database>,
    txn: TxnView,
    left: &PhysicalPlan,
    right_table: &PlanTable,
    right_index: &IndexSchema,
    join_type: crate::parser::JoinType,
    condition: Option<&Expr>,
    right_covering: bool,
    right_projected_columns: Option<&[usize]>,
    right_runtime_filters: &[RuntimeFilter],
) -> Result<Vec<PlanRow>, QueryError> {
    let Some(condition) = condition else {
        return execute_nested_loop_join(
            db,
            txn,
            left,
            &PhysicalPlan::SeqScan {
                table: right_table.clone(),
                filter: None,
                estimated_rows: 1.0,
                access_rows: 1.0,
                cost: crate::engine::cbo::Cost::scalar(1.0),
                possible_keys: vec![right_index.name.clone()],
                runtime_filters: right_runtime_filters.to_vec(),
                hint_note: None,
                projected_columns: None,
            },
            join_type,
            None,
        );
    };
    let plan = compile_index_lookup_plan(condition, right_table, right_index)?;
    // Plan-shape fusion: when this SEMI/ANTI INLJ stacks directly on top
    // of another SEMI/ANTI INLJ against the *same* right table + index
    // + equality prefix, both subqueries probe the same secondary index
    // for the same key set per outer row (TPC-H Q21 hits this exactly:
    // `EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey =
    // l1.l_orderkey AND …)` and `NOT EXISTS (… l3 …)`).
    //
    // The unfused path runs the inner INLJ end-to-end, materialises its
    // surviving rows, then re-walks every l_orderkey again in the outer
    // INLJ. The lookups are deterministic — the entries on the second
    // pass are exactly the entries from the first — so we collapse them
    // into one batched index walk + one heap read, evaluating *both*
    // residuals over the union of their right-column projections in a
    // single inner loop. Saves one full lineitem-shape pass (~150K row
    // reads at sf=1 Q21).
    if let Some(rows) = try_fuse_consecutive_semi_anti_inlj(
        db,
        txn,
        left,
        right_table,
        right_index,
        join_type,
        condition,
        &plan,
        right_projected_columns,
    )? {
        return Ok(rows);
    }
    let left_rows = execute_plan_rows(db, txn, left)?;
    execute_index_nested_loop_join_from_left_rows(
        db,
        txn,
        left,
        &left_rows,
        right_table,
        right_index,
        join_type,
        &plan,
        right_covering,
        right_projected_columns,
        right_runtime_filters,
    )
}

/// S5 (WS-4): whether an INLJ can run chunk-streamed as the probe source of a
/// chained streaming join. Pure shape check; the GUC gate (`MYDB_HASH_JOIN_STREAM_INLJ`) sits on the join_stream side.
///
/// Supports INNER/LEFT/SEMI/ANTI. SEMI/ANTI share the same chunked MRR source
/// with the other types; existence semantics are handled by the generic INLJ
/// kernel, so no separate fused executor is kept for Q21-style queries. A missing
/// condition (the NLJ-fallback shape) or a condition containing subqueries still cannot be expressed by this source.
pub(in crate::engine::executor::select) fn inlj_chunk_stream_applicable(
    plan: &PhysicalPlan,
) -> bool {
    let PhysicalPlan::IndexNestedLoopJoin {
        join_type,
        condition,
        right_table,
        right_index,
        ..
    } = plan
    else {
        return false;
    };
    if !matches!(
        join_type,
        crate::parser::JoinType::Inner
            | crate::parser::JoinType::Left
            | crate::parser::JoinType::Semi
            | crate::parser::JoinType::Anti
    ) {
        return false;
    }
    let Some(condition) = condition else {
        return false;
    };
    let Ok(lookup) = compile_index_lookup_plan(condition, right_table, right_index) else {
        return false;
    };
    // Residual subqueries already run with (db, txn) and the captured CTE
    // context in serial and parallel probes. Rejecting them here forced
    // bounded INLJs to materialize the complete fact-table output.
    // Bounds still use a scalar evaluator, so subqueries in key/range
    // expressions retain the existing fallback.
    lookup
        .eq_exprs
        .iter()
        .all(|expr| !crate::engine::executor::select::simple::expr_has_subquery(expr))
        && lookup.range.as_ref().is_none_or(|range| {
            range
                .low
                .iter()
                .chain(range.high.iter())
                .all(|(expr, _)| !crate::engine::executor::select::simple::expr_has_subquery(expr))
        })
}

/// Whether the top two INLJ levels satisfy every metadata-only prerequisite
/// of the same-index SEMI/ANTI fusion path.
///
/// The native vector compiler normally lowers an INLJ chain into two bounded
/// streaming adapters.  Doing that for this shape hides the consecutive plan
/// nodes from [`try_fuse_consecutive_semi_anti_inlj`] and repeats the same
/// index/heap probe once per level.  Keep the test here metadata-only so the
/// compiler can route just the genuinely fuseable shape through the eager
/// fused kernel without starting I/O or changing the physical plan.
pub(in crate::engine::executor::select) fn inlj_consecutive_semi_anti_fusion_applicable(
    plan: &PhysicalPlan,
) -> bool {
    if !inlj_fuse_semi_anti_enabled() {
        return false;
    }
    let PhysicalPlan::IndexNestedLoopJoin {
        left: outer_left,
        right_table: outer_table,
        right_index: outer_index,
        join_type: outer_join_type,
        condition: Some(outer_condition),
        right_projected_columns: outer_projection,
        ..
    } = plan
    else {
        return false;
    };
    if !matches!(
        outer_join_type,
        crate::parser::JoinType::Semi | crate::parser::JoinType::Anti
    ) {
        return false;
    }
    let PhysicalPlan::IndexNestedLoopJoin {
        left: inner_left,
        right_table: inner_table,
        right_index: inner_index,
        join_type: inner_join_type,
        condition: Some(inner_condition),
        right_covering: false,
        right_projected_columns: inner_projection,
        ..
    } = outer_left.as_ref()
    else {
        return false;
    };
    if !matches!(
        inner_join_type,
        crate::parser::JoinType::Semi | crate::parser::JoinType::Anti
    ) || inner_table.schema.id != outer_table.schema.id
        || !inner_index.name.eq_ignore_ascii_case(&outer_index.name)
    {
        return false;
    }

    let Ok(outer_lookup) = compile_index_lookup_plan(outer_condition, outer_table, outer_index)
    else {
        return false;
    };
    let Ok(inner_lookup) = compile_index_lookup_plan(inner_condition, inner_table, inner_index)
    else {
        return false;
    };
    if outer_lookup.range.is_some()
        || inner_lookup.range.is_some()
        || outer_lookup.eq_exprs.is_empty()
        || outer_lookup.eq_exprs.len() != inner_lookup.eq_exprs.len()
        || outer_lookup
            .eq_exprs
            .iter()
            .zip(inner_lookup.eq_exprs.iter())
            .any(|(outer, inner)| !expressions_equivalent(outer, inner))
    {
        return false;
    }

    let projection = build_fused_semi_anti_projection(
        inner_condition,
        outer_condition,
        inner_table,
        outer_table,
        inner_projection.as_deref().map(Vec::as_slice),
        outer_projection.as_deref().map(Vec::as_slice),
    );
    if projection.is_empty() {
        return false;
    }
    let mut left_metas = Vec::new();
    collect_plan_fields(inner_left, &mut left_metas);
    let inner_metas = build_alias_right_metas(outer_table, &projection, inner_table.alias.clone());
    let outer_metas = build_alias_right_metas(outer_table, &projection, outer_table.alias.clone());
    precompile_joined_residual_compatible(inner_condition, &left_metas, &inner_metas)
        && precompile_joined_residual_compatible(outer_condition, &left_metas, &outer_metas)
}

/// Return the deepest left input consumed by a fuseable consecutive
/// SEMI/ANTI INLJ pair.  The vector bridge uses this metadata-only accessor to
/// keep the hash/scan subtree columnar, then hands the resulting rows to the
/// existing one-probe fused INLJ kernel.
pub(in crate::engine::executor::select) fn inlj_consecutive_semi_anti_left(
    plan: &PhysicalPlan,
) -> Option<&PhysicalPlan> {
    if !inlj_consecutive_semi_anti_fusion_applicable(plan) {
        return None;
    }
    let PhysicalPlan::IndexNestedLoopJoin {
        left: outer_left, ..
    } = plan
    else {
        return None;
    };
    let PhysicalPlan::IndexNestedLoopJoin {
        left: inner_left, ..
    } = outer_left.as_ref()
    else {
        return None;
    };
    Some(inner_left)
}

/// Execute a metadata-preflighted consecutive SEMI/ANTI pair from rows that
/// were produced by another executor.  This preserves the existing global
/// deduplicated index/heap probe while allowing its deepest hash-join input to
/// remain in the native batch pipeline.
pub(in crate::engine::executor::select) fn execute_fused_semi_anti_inlj_from_left_rows(
    db: &Arc<Database>,
    txn: TxnView,
    plan: &PhysicalPlan,
    left_rows: &[PlanRow],
) -> Result<Vec<PlanRow>, QueryError> {
    // The stream constructor already preflighted this immutable physical
    // plan. Repeating schema collection and expression compilation per MRR
    // chunk is unnecessary (and particularly costly for tiny grants).
    let PhysicalPlan::IndexNestedLoopJoin {
        left: outer_left,
        right_table: outer_table,
        right_index: outer_index,
        join_type: outer_join_type,
        condition: Some(outer_condition),
        right_projected_columns: outer_projection,
        ..
    } = plan
    else {
        unreachable!("fused SEMI/ANTI outer shape preflighted")
    };
    let PhysicalPlan::IndexNestedLoopJoin {
        right_table: inner_table,
        join_type: inner_join_type,
        condition: Some(inner_condition),
        right_projected_columns: inner_projection,
        ..
    } = outer_left.as_ref()
    else {
        unreachable!("fused SEMI/ANTI inner shape preflighted")
    };
    let outer_plan = compile_index_lookup_plan(outer_condition, outer_table, outer_index)?;
    let projection = build_fused_semi_anti_projection(
        inner_condition,
        outer_condition,
        inner_table,
        outer_table,
        inner_projection.as_deref().map(Vec::as_slice),
        outer_projection.as_deref().map(Vec::as_slice),
    );
    let rows = execute_fused_semi_anti_inlj_parallel(
        db,
        txn,
        left_rows,
        outer_table,
        outer_index,
        &outer_plan,
        &projection,
        inner_table.alias.clone(),
        outer_table.alias.clone(),
        *inner_join_type,
        inner_condition,
        *outer_join_type,
        outer_condition,
        outer_projection.as_deref().map(Vec::as_slice),
    )?;
    record_inlj_fusion_count();
    Ok(rows)
}

/// S5: open the INLJ as a chunked lazy stream. The left (outer) side is opened
/// via `execute_plan_stream` — recursively streamed when left is itself a supported
/// HashJoin chain / another INLJ (mixed chain), otherwise wrapped materialized;
/// each time a full chunk is pulled, `execute_index_nested_loop_join_from_left_rows`
/// runs (batched lookup / MRR / outer parallelism and other internal optimizations still apply at chunk granularity), emitting results on demand.
pub(in crate::engine::executor::select) fn open_inlj_chunk_stream(
    db: &Arc<Database>,
    txn: TxnView,
    plan: PhysicalPlan,
) -> Result<Box<dyn PlanStream>, QueryError> {
    open_inlj_chunk_stream_inner(db, txn, plan, None)
}

pub(in crate::engine::executor::select) fn open_owned_plan_stream_with_stats(
    db: &Arc<Database>,
    txn: TxnView,
    plan: PhysicalPlan,
    display_keys: PlanStatsKeyTree,
) -> Result<Box<dyn PlanStream>, QueryError> {
    bind_plan_stats_key_tree(&plan, &display_keys);
    let inner = execute_plan_stream(db, txn, &plan)?;
    Ok(Box::new(StatsMirroredOwnedPlanStream {
        _plan: plan,
        inner: Some(inner),
    }))
}

pub(super) struct StatsMirroredOwnedPlanStream {
    // Keep the bound addresses alive until every nested stream has published.
    pub(super) _plan: PhysicalPlan,
    pub(super) inner: Option<Box<dyn PlanStream>>,
}

impl PlanStream for StatsMirroredOwnedPlanStream {
    fn next(&mut self) -> Result<Option<PlanRow>, QueryError> {
        self.inner
            .as_mut()
            .expect("owned stats stream missing before drop")
            .next()
    }

    fn install_runtime_filter(&mut self, filter: RuntimeFilter) -> bool {
        self.inner
            .as_mut()
            .is_some_and(|inner| inner.install_runtime_filter(filter))
    }
}

impl Drop for StatsMirroredOwnedPlanStream {
    fn drop(&mut self) {
        // Stream adapters publish from Drop. Finalize them while the bound
        // owned plan (and therefore its child addresses) is still alive.
        drop(self.inner.take());
    }
}

pub(in crate::engine::executor::select) fn open_inlj_chunk_stream_with_stats(
    db: &Arc<Database>,
    txn: TxnView,
    plan: PhysicalPlan,
    display_keys: PlanStatsKeyTree,
) -> Result<Box<dyn PlanStream>, QueryError> {
    open_inlj_chunk_stream_inner(db, txn, plan, Some(display_keys))
}

pub(super) fn open_inlj_chunk_stream_inner(
    db: &Arc<Database>,
    txn: TxnView,
    plan: PhysicalPlan,
    display_keys: Option<PlanStatsKeyTree>,
) -> Result<Box<dyn PlanStream>, QueryError> {
    let analyze_started = display_keys
        .as_ref()
        .map(|_| (std::time::Instant::now(), db.buffer_stats_counts()));
    let PhysicalPlan::IndexNestedLoopJoin {
        left,
        right_table,
        right_index,
        join_type,
        condition,
        right_covering,
        right_projected_columns,
        right_runtime_filters,
        ..
    } = plan
    else {
        return Err(error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            "INLJ chunk stream on non-INLJ plan",
        ));
    };
    let condition = condition.ok_or_else(|| {
        error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            "INLJ chunk stream requires a join condition",
        )
    })?;
    let lookup_plan = compile_index_lookup_plan(&condition, &right_table, &right_index)?;
    if let Some(left_keys) = display_keys.as_ref().and_then(|keys| keys.child(0)) {
        bind_plan_stats_key_tree(&left, left_keys);
    }
    let left_stream = execute_plan_stream(db, txn, &left)?;
    let shared_mrr = new_inlj_mrr_shared_caches();
    Ok(Box::new(InljChunkStream {
        db: Arc::clone(db),
        txn,
        // Keep the original box allocation alive: EXPLAIN ANALYZE binds the
        // owned clone's node addresses before opening the stream. Moving the
        // enum out of this box would invalidate those bindings and let a
        // later plan allocation reuse the address, merging unrelated stats.
        left_plan: left,
        right_table,
        right_index,
        join_type,
        lookup_plan,
        right_covering,
        right_projected_columns,
        right_runtime_filters,
        left_stream: Some(left_stream),
        current: Vec::new().into_iter(),
        left_done: false,
        chunk_rows: inlj_chunk_stream_rows(),
        shared_mrr,
        display_keys,
        analyze_started,
        rows_out: 0,
    }))
}

/// Construct the statement-local cache used by both the legacy row adapter
/// and the native BatchOperator INLJ. Keeping this policy in one place avoids
/// changing the public resource switches while the outer transport migrates.
pub(in crate::engine::executor::select) fn new_inlj_mrr_shared_caches(
) -> Option<InljMrrSharedCaches> {
    if !inlj_mrr_chunk_shared_enabled() {
        return None;
    }
    let cap = crate::util::env::var("MYDB_INLJ_MRR_CACHE_MAX")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(8192);
    let use_lru = !matches!(
        crate::util::env::var("MYDB_INLJ_MRR_LRU")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "off" | "0" | "false" | "no" | "disable" | "disabled"
    );
    Some(InljMrrSharedCaches::new(cap, use_lru))
}

/// `MYDB_HASH_JOIN_STREAM_INLJ_CHUNK` (default 16384). The chunk is large enough to
/// trigger the INLJ's internal outer parallelism (threshold 4096) and batched lookup
/// amortization, while bounding in-flight memory to chunk × row width.
pub(in crate::engine::executor::select) fn inlj_chunk_stream_rows() -> usize {
    if let Some(configured) = crate::util::env::var("MYDB_HASH_JOIN_STREAM_INLJ_CHUNK")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
    {
        return configured;
    }
    // Batch size is an execution resource decision, not a plan decision. Use
    // more of a generous query work area to amortize unique-key grouping and
    // MRR heap fetches, while retaining a small bounded chunk under pressure.
    // One row is conservatively budgeted at 4 KiB here; the actual temporary
    // PlanRow adapter is normally much narrower.
    crate::engine::executor::mem_budget::current_budget()
        .map(|budget| budget.limit() / 4096)
        .unwrap_or(crate::engine::cbo::PLANNING_INLJ_CHUNK_ROWS)
        .clamp(crate::engine::cbo::PLANNING_INLJ_CHUNK_ROWS, 131_072)
}

/// Size MRR input by the number of index matches, not only outer rows.
/// A non-unique prefix can expand one input into an entire fact-table slice.
/// Use analyzed prefix NDV when available; full unique probes stay unchanged.
/// The estimate only tunes batching and never changes join semantics.
pub(in crate::engine::executor::select) fn inlj_fanout_chunk_rows(
    max_rows: usize,
    table: &PlanTable,
    index: &IndexSchema,
    lookup: &IndexLookupPlan,
) -> usize {
    if index.unique && lookup.is_full_equality_for(index) {
        return inlj_runtime_chunk_rows(max_rows);
    }
    let prefix = lookup.eq_exprs.len();
    let distinct = table
        .schema
        .stats
        .indexes
        .get(&index.name.to_ascii_lowercase())
        .and_then(|stats| {
            prefix
                .checked_sub(1)
                .and_then(|i| stats.prefix_distinct_counts.get(i))
        })
        .copied()
        .filter(|n| *n > 0)
        .or_else(|| {
            index.columns.first().and_then(|column| {
                table
                    .schema
                    .stats
                    .columns
                    .get(&column.to_ascii_lowercase())
                    .map(|stats| stats.distinct_count)
                    .filter(|n| *n > 0)
            })
        });
    let fanout = distinct
        .map(|ndv| table.schema.stats.row_count.div_ceil(ndv).max(1))
        .unwrap_or(1)
        .min(usize::MAX as u64) as usize;
    (inlj_runtime_chunk_rows(max_rows) / fanout).max(1)
}

/// Tighten an already-selected INLJ morsel against the query's live
/// headroom. `inlj_chunk_stream_rows()` is an open-time upper bound; a
/// blocking parent (notably HashAggregate) may retain most of the query
/// quota before asking this stream for its next morsel. Keeping the stale
/// upper bound in that case lets the keyed-entry vectors fail their strict
/// capacity reservation before the parent can consume another batch and
/// spill.
///
/// Four KiB per outer row is the same conservative envelope used by the
/// open-time policy. It covers the outer `PlanRow`, keyed index entries,
/// sorted row ids and heap result while they overlap. This changes only the
/// operator's internal batch size; access path, join order and parallel
/// topology remain those of the physical plan.
pub(in crate::engine::executor::select) fn inlj_runtime_chunk_rows(max_rows: usize) -> usize {
    let max_rows = max_rows.max(1);
    crate::engine::executor::mem_budget::current_budget()
        .map(|budget| (budget.remaining() / (4 * 1024)).max(1).min(max_rows))
        .unwrap_or(max_rows)
}

/// WS-N5 MRR LRU shared across chunks: hoist the entries / ids caches to the
/// InljChunkStream level so lookups with identical bounds are reused across chunks.
/// Capacity follows `MYDB_INLJ_MRR_CACHE_MAX` (default 8192); with
/// `MYDB_INLJ_MRR_CHUNK_SHARED=off` it falls back to a per-call local cache (legacy).
pub(in crate::engine::executor::select) struct InljMrrSharedCaches {
    pub(super) entries: MrrBoundsCache<Arc<Vec<(IndexKey, RowId)>>>,
    pub(super) ids: MrrBoundsCache<Arc<Vec<RowId>>>,
}

impl InljMrrSharedCaches {
    pub(super) fn new(cap: usize, use_lru: bool) -> Self {
        Self {
            entries: MrrBoundsCache::new(cap, use_lru),
            ids: MrrBoundsCache::new(cap, use_lru),
        }
    }
}

pub(super) fn inlj_mrr_chunk_shared_enabled() -> bool {
    !matches!(
        crate::util::env::var("MYDB_INLJ_MRR_CHUNK_SHARED")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "off" | "0" | "false" | "no" | "disable" | "disabled"
    )
}

pub(in crate::engine::executor::select) struct InljChunkStream {
    pub(super) db: Arc<Database>,
    pub(super) txn: TxnView,
    pub(super) left_plan: Box<PhysicalPlan>,
    pub(super) right_table: PlanTable,
    pub(super) right_index: IndexSchema,
    pub(super) join_type: crate::parser::JoinType,
    pub(super) lookup_plan: IndexLookupPlan,
    pub(super) right_covering: bool,
    pub(super) right_projected_columns: Option<std::sync::Arc<Vec<usize>>>,
    pub(super) right_runtime_filters: Vec<RuntimeFilter>,
    // Option so Drop can finalize a nested chain before mirroring this
    // cloned left subtree back to the display-plan keys.
    pub(super) left_stream: Option<Box<dyn PlanStream>>,
    pub(super) current: std::vec::IntoIter<PlanRow>,
    pub(super) left_done: bool,
    pub(super) chunk_rows: usize,
    /// WS-N5: `Some` → MRR caches shared across chunks; `None` → independent per chunk.
    pub(super) shared_mrr: Option<InljMrrSharedCaches>,
    pub(super) display_keys: Option<PlanStatsKeyTree>,
    pub(super) analyze_started: Option<(std::time::Instant, (u64, u64))>,
    pub(super) rows_out: u64,
}

impl PlanStream for InljChunkStream {
    fn next(&mut self) -> Result<Option<PlanRow>, QueryError> {
        loop {
            if let Some(row) = self.current.next() {
                self.rows_out = self.rows_out.saturating_add(1);
                return Ok(Some(row));
            }
            if self.left_done {
                return Ok(None);
            }
            crate::engine::cancel::check_cancelled()?;
            let chunk_rows = inlj_fanout_chunk_rows(
                self.chunk_rows,
                &self.right_table,
                &self.right_index,
                &self.lookup_plan,
            );
            let mut buf: Vec<PlanRow> = Vec::with_capacity(chunk_rows);
            while buf.len() < chunk_rows {
                match self
                    .left_stream
                    .as_mut()
                    .expect("INLJ left stream missing before drop")
                    .next()?
                {
                    Some(row) => buf.push(row),
                    None => {
                        self.left_done = true;
                        break;
                    }
                }
            }
            if buf.is_empty() {
                return Ok(None);
            }
            // WS-N5: lend the stream-level shared MRR caches to this chunk; the
            // entries/ids caches keep accumulating across all chunks.
            if self.shared_mrr.is_some() {
                record_inlj_mrr_chunk_shared_hit();
            }
            let rows = execute_index_nested_loop_join_from_left_rows_inner(
                &self.db,
                self.txn,
                &self.left_plan,
                &buf,
                &self.right_table,
                &self.right_index,
                self.join_type,
                &self.lookup_plan,
                self.right_covering,
                self.right_projected_columns
                    .as_deref()
                    .map(|v| v.as_slice()),
                &self.right_runtime_filters,
                self.shared_mrr.as_mut(),
            )?;
            self.current = rows.into_iter();
        }
    }

    fn install_runtime_filter(&mut self, filter: RuntimeFilter) -> bool {
        let matches_right = runtime_filter_schema_indices(&filter, &self.right_table).is_some();
        if matches_right {
            self.right_runtime_filters.push(filter);
            true
        } else {
            self.left_stream
                .as_mut()
                .is_some_and(|left| left.install_runtime_filter(filter))
        }
    }
}

impl Drop for InljChunkStream {
    fn drop(&mut self) {
        // A nested chain records/mirrors its stats from Drop. Finalize it
        // before reading the cloned left-plan keys below; Rust otherwise runs
        // this Drop implementation before dropping fields and the parent
        // mirror would miss the deepest streamed INLJ node.
        drop(self.left_stream.take());
        let Some(display_keys) = self.display_keys.as_ref() else {
            return;
        };
        if let Some((started, io_start)) = self.analyze_started.take() {
            crate::engine::executor::select::analyze_stats::record_key_with_io_snapshot(
                &self.db,
                display_keys.key(),
                started.elapsed().as_nanos(),
                self.rows_out,
                io_start,
            );
        }
    }
}

pub(in crate::engine::executor::select) fn execute_index_nested_loop_join_from_left_rows(
    db: &Arc<Database>,
    txn: TxnView,
    left: &PhysicalPlan,
    left_rows: &[PlanRow],
    right_table: &PlanTable,
    right_index: &IndexSchema,
    join_type: crate::parser::JoinType,
    plan: &IndexLookupPlan,
    right_covering: bool,
    right_projected_columns: Option<&[usize]>,
    right_runtime_filters: &[RuntimeFilter],
) -> Result<Vec<PlanRow>, QueryError> {
    execute_index_nested_loop_join_from_left_rows_inner(
        db,
        txn,
        left,
        left_rows,
        right_table,
        right_index,
        join_type,
        plan,
        right_covering,
        right_projected_columns,
        right_runtime_filters,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub(in crate::engine::executor::select) fn execute_index_nested_loop_join_from_left_rows_with_shared_mrr(
    db: &Arc<Database>,
    txn: TxnView,
    left: &PhysicalPlan,
    left_rows: &[PlanRow],
    right_table: &PlanTable,
    right_index: &IndexSchema,
    join_type: crate::parser::JoinType,
    plan: &IndexLookupPlan,
    right_covering: bool,
    right_projected_columns: Option<&[usize]>,
    right_runtime_filters: &[RuntimeFilter],
    shared_mrr: Option<&mut InljMrrSharedCaches>,
) -> Result<Vec<PlanRow>, QueryError> {
    if shared_mrr.is_some() {
        record_inlj_mrr_chunk_shared_hit();
    }
    execute_index_nested_loop_join_from_left_rows_inner(
        db,
        txn,
        left,
        left_rows,
        right_table,
        right_index,
        join_type,
        plan,
        right_covering,
        right_projected_columns,
        right_runtime_filters,
        shared_mrr,
    )
}

/// WS-N5: when `shared_mrr.is_some()`, use the cross-chunk MRR caches held by the
/// caller (InljChunkStream) instead of building new ones on every call. Other paths keep passing None; semantics unchanged.
pub(super) fn execute_index_nested_loop_join_from_left_rows_inner(
    db: &Arc<Database>,
    txn: TxnView,
    left: &PhysicalPlan,
    left_rows: &[PlanRow],
    right_table: &PlanTable,
    right_index: &IndexSchema,
    join_type: crate::parser::JoinType,
    plan: &IndexLookupPlan,
    right_covering: bool,
    right_projected_columns: Option<&[usize]>,
    right_runtime_filters: &[RuntimeFilter],
    shared_mrr: Option<&mut InljMrrSharedCaches>,
) -> Result<Vec<PlanRow>, QueryError> {
    let mut left_fields = Vec::new();
    collect_plan_fields(left, &mut left_fields);
    let left_refs = field_ref_set(&left_fields);
    let (right_only_residual, joined_residual) =
        split_right_only_residual(plan.residual.as_ref(), right_table, &left_refs);
    let mut out = Vec::new();
    let join_lk = new_join_lookup();
    let semi_or_anti = matches!(
        join_type,
        crate::parser::JoinType::Semi | crate::parser::JoinType::Anti
    );

    if let Some(rows) = try_execute_batched_index_nested_loop_join(
        db,
        txn,
        left_rows,
        right_table,
        right_index,
        join_type,
        &plan,
        right_covering,
        right_only_residual.as_ref(),
        joined_residual.as_ref(),
        right_projected_columns,
        right_runtime_filters,
    )? {
        return Ok(rows);
    }
    record_inlj_per_row_execution();

    // Fix B: resolve the index and heap handles once for the lifetime
    // of the INLJ. Per-row reads then bypass the cache HashMap +
    // name normalize + RwLock cycle that was paid before. The handle
    // RwLock itself is still taken per call (preserves DDL visibility),
    // but every other piece of the lookup machinery is amortised.
    let index_handle = db.cached_index_handle(right_table.schema.id, &right_index.name)?;
    let heap_handle = if right_covering {
        // Heap not needed for covering-index scans.
        None
    } else {
        Some(db.cached_heap_handle(right_table.schema.id)?)
    };

    // Fix E: Multi-Range Read cache. When the outer side produces the
    // same lookup bounds for several rows (foreign-key joins where
    // duplicate join keys flow up from the outer, IN-subquery rewrites,
    // sort-merge-like upstream orderings) the B-tree descent + leaf
    // scan was being repeated N times. Memoise per unique bounds; on a
    // cache hit subsequent outer rows pay only an Arc::clone.
    //
    // P2-A: the cache is a real LRU (see `mrr_lru::MrrLru`). The
    // historical implementation dropped new entries past
    // `MYDB_INLJ_MRR_CACHE_MAX` (8192), which made the cache cold on
    // tall outers with rotating keys. `MYDB_INLJ_MRR_LRU=off` (kill
    // switch) restores the original "first 8192 wins" semantics for
    // differential diagnostics.
    let mrr_cache_cap = crate::util::env::var("MYDB_INLJ_MRR_CACHE_MAX")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(8192);
    let use_lru = !matches!(
        crate::util::env::var("MYDB_INLJ_MRR_LRU")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "off" | "0" | "false" | "no" | "disable" | "disabled"
    );
    // WS-N5: externally supplied cross-chunk caches → borrow them; otherwise build fresh for this call.
    let mut local_caches: Option<InljMrrSharedCaches> = None;
    let caches: &mut InljMrrSharedCaches = match shared_mrr {
        Some(s) => s,
        None => local_caches.insert(InljMrrSharedCaches::new(mrr_cache_cap, use_lru)),
    };
    let entries_cache: &mut MrrBoundsCache<Arc<Vec<(IndexKey, RowId)>>> = &mut caches.entries;
    let ids_cache: &mut MrrBoundsCache<Arc<Vec<RowId>>> = &mut caches.ids;
    let covering_projector = right_covering
        .then(|| IndexEntryProjector::new(right_table, right_index, right_projected_columns));
    let mut residual_pre = LazyJoinedResidualPre::new();
    // Subquery residuals can't be compiled by `residual_pre`; the
    // interpreter fallback below must carry (db, txn) so they execute.
    let mut eval_ctx = EvalQueryContext::default();

    for left_row in left_rows {
        let bounds_opt = build_bounds_from_plan(&plan, left_row)?;
        let mut matched = false;
        if right_covering {
            let entries: Arc<Vec<(IndexKey, RowId)>> = match &bounds_opt {
                None => Arc::new(Vec::new()),
                Some(bounds) => {
                    if let Some(cached) = entries_cache.get(bounds) {
                        Arc::clone(cached)
                    } else {
                        let (low, high) = bounds.clone();
                        let mut new = db.range_index_bounds_entries_visible_with_handle(
                            &index_handle,
                            &right_table.schema,
                            right_index,
                            low,
                            high,
                            txn,
                        )?;
                        let heap = db.cached_heap_handle(right_table.schema.id)?;
                        let visible_mask = db.filter_visible_index_entries_mask_with_handle(
                            &heap,
                            &right_table.schema,
                            right_index,
                            &new,
                            txn,
                        )?;
                        let mut visible_iter = visible_mask.into_iter();
                        new.retain(|_| visible_iter.next().unwrap_or(false));
                        let arc = Arc::new(new);
                        entries_cache.insert(bounds.clone(), Arc::clone(&arc));
                        arc
                    }
                }
            };
            // Fix D (covering variant): SEMI / ANTI with no residual
            // only need to know whether any visible entry exists.
            // `range_index_bounds_entries_visible_with_handle` has
            // already filtered to visible rows, so a non-empty entry
            // set is a definitive match — no need to build the
            // right_row, join it, or run any per-entry eval.
            if semi_or_anti && right_only_residual.is_none() && joined_residual.is_none() {
                if !entries.is_empty() {
                    matched = true;
                    if join_type == crate::parser::JoinType::Semi {
                        out.push(left_row.clone());
                    }
                }
                if !matched && join_type == crate::parser::JoinType::Anti {
                    out.push(left_row.clone());
                }
                continue;
            }
            for (key, _row_id) in entries.iter() {
                let right_row = covering_projector
                    .as_ref()
                    .expect("covering path should have an index-entry projector")
                    .project(key);
                if right_only_residual
                    .as_ref()
                    .map(|residual| {
                        eval_plan_bool_with_db(db, txn, residual, &right_row, &mut eval_ctx)
                    })
                    .transpose()?
                    .is_some_and(|keep| !keep)
                {
                    continue;
                }
                emit_inlj_pair!(
                    db,
                    txn,
                    join_type,
                    semi_or_anti,
                    joined_residual.as_ref(),
                    residual_pre,
                    join_lk,
                    left_row,
                    right_row,
                    out,
                    matched,
                    eval_ctx
                );
            }
        } else {
            let ids: Arc<Vec<RowId>> = match &bounds_opt {
                None => Arc::new(Vec::new()),
                Some(bounds) => {
                    if let Some(cached) = ids_cache.get(bounds) {
                        Arc::clone(cached)
                    } else {
                        let (low, high) = bounds.clone();
                        let new = db.range_index_bounds_with_handle(
                            &index_handle,
                            right_index,
                            low,
                            high,
                        )?;
                        let arc = Arc::new(new);
                        ids_cache.insert(bounds.clone(), Arc::clone(&arc));
                        arc
                    }
                }
            };
            let heap = heap_handle
                .as_ref()
                .expect("non-covering path has heap handle");

            // Fix D: SEMI / ANTI joins never project right-side values,
            // and when the index bounds already enforce every join
            // predicate (residual is None) we don't need the row
            // payload — only its visibility. Use the header-only
            // visibility path, skipping the full-row decode +
            // stored_row_to_plan_row + Arc::new allocations that the
            // generic path would do per row.
            if semi_or_anti && right_only_residual.is_none() && joined_residual.is_none() {
                for row_id in ids.iter() {
                    let visible =
                        db.is_row_id_visible_with_handle(heap, &right_table.schema, *row_id, txn)?;
                    if !visible {
                        continue;
                    }
                    matched = true;
                    if join_type == crate::parser::JoinType::Semi {
                        out.push(left_row.clone());
                    }
                    break;
                }
                if !matched && join_type == crate::parser::JoinType::Anti {
                    out.push(left_row.clone());
                }
                continue;
            }

            for row_id in ids.iter() {
                let right_row = if let Some(projection) = right_projected_columns {
                    let Some(values) = db.read_row_columns_with_handle(
                        heap,
                        &right_table.schema,
                        *row_id,
                        txn,
                        projection,
                    )?
                    else {
                        continue;
                    };
                    projected_values_to_plan_row(right_table, projection, values)?
                } else {
                    let Some(row) =
                        db.read_row_with_handle(heap, &right_table.schema, *row_id, txn)?
                    else {
                        continue;
                    };
                    stored_row_to_plan_row(right_table, row.values)?
                };
                if right_only_residual
                    .as_ref()
                    .map(|residual| {
                        eval_plan_bool_with_db(db, txn, residual, &right_row, &mut eval_ctx)
                    })
                    .transpose()?
                    .is_some_and(|keep| !keep)
                {
                    continue;
                }
                emit_inlj_pair!(
                    db,
                    txn,
                    join_type,
                    semi_or_anti,
                    joined_residual.as_ref(),
                    residual_pre,
                    join_lk,
                    left_row,
                    right_row,
                    out,
                    matched,
                    eval_ctx
                );
            }
        }
        if !matched {
            match join_type {
                crate::parser::JoinType::Left => {
                    out.push(null_extend_right_table(
                        left_row,
                        right_table,
                        right_projected_columns,
                    ));
                }
                crate::parser::JoinType::Anti => {
                    out.push(left_row.clone());
                }
                _ => {}
            }
        }
    }
    Ok(out)
}

/// Configuration + thresholds for the INLJ probe-side bloom filter. Three modes:
/// - `Off`: fully disabled (default = `Auto`, but the user can explicitly set `off`)
/// - `On`: build whenever the inner index fits within `max_build_pages`
/// - `Auto`: additive-cost heuristic — worthwhile only when outer distinct keys are at least on the same order of magnitude as inner
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum InljProbeBloomMode {
    Off,
    On,
    Auto,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct InljProbeBloomConfig {
    pub(super) mode: InljProbeBloomMode,
    /// Minimum distinct outer keys (after dedup) before building is considered; default 4096.
    pub(super) min_outer_distinct_keys: usize,
    /// Hard cap on leaf pages allowed to scan in the inner index; default 1024 (≈ 16 MiB).
    pub(super) max_build_pages: usize,
}

pub(super) fn inlj_probe_bloom_config() -> InljProbeBloomConfig {
    let mode = match crate::util::env::var("MYDB_INLJ_PROBE_BLOOM")
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "off" | "0" | "false" | "no" | "disable" | "disabled" => InljProbeBloomMode::Off,
        "on" | "1" | "true" | "yes" | "enable" | "enabled" | "force" => InljProbeBloomMode::On,
        // Any other value (including unset and `auto`) takes the heuristic path
        _ => InljProbeBloomMode::Auto,
    };
    let min_outer_distinct_keys = crate::util::env::var("MYDB_INLJ_PROBE_BLOOM_MIN_KEYS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(4096);
    let max_build_pages = crate::util::env::var("MYDB_INLJ_PROBE_BLOOM_MAX_BUILD_PAGES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1024);
    InljProbeBloomConfig {
        mode,
        min_outer_distinct_keys,
        max_build_pages,
    }
}

/// **Pre-filter** outer keys in the INLJ batched path — scan the inner index once
/// and insert every currently persisted key prefix into a bloom. Then use that bloom
/// to decide for each outer key "could it possibly hit inner"; keys that definitely
/// do not exist take the join_type semantic short-circuit (Inner/Semi dropped, Left/Anti emitted directly), saving one B-tree descent.
///
/// Return values:
/// - `Ok(Some(bloom))`: built successfully; the caller may rely on `bloom.contains(&outer_key.0)`
/// - `Ok(None)`: skipped by config/heuristic/budget; the caller continues on the original path
///
/// Design notes:
/// - No visibility filtering: the bloom only promises a "definitely absent" verdict; visibility is left to the subsequent real lookup
/// - Return None immediately once the `max_build_pages` cap is reached (a half-built bloom yields false negatives)
/// - `expected_entries` takes the larger of the inner row count and the leaf-page estimate, keeping FPR ≈ 1%
pub(super) fn try_build_inlj_probe_bloom(
    db: &Arc<Database>,
    right_table: &PlanTable,
    right_index: &IndexSchema,
    index_handle: &IndexHandle,
    eq_prefix_len: usize,
    outer_distinct_keys: usize,
) -> Result<Option<BloomFilter>, QueryError> {
    let cfg = inlj_probe_bloom_config();
    if matches!(cfg.mode, InljProbeBloomMode::Off) {
        return Ok(None);
    }
    if eq_prefix_len == 0 {
        // Should not happen (guaranteed by the caller); disable defensively
        return Ok(None);
    }
    if outer_distinct_keys < cfg.min_outer_distinct_keys {
        return Ok(None);
    }
    if cfg.max_build_pages == 0 {
        return Ok(None);
    }

    let right_total_rows = right_table.schema.stats.row_count;
    let page_upper = db
        .index_page_id_upper_bound(&right_table.schema, right_index)
        .unwrap_or(u64::MAX);
    // page_upper is the next_page_id watermark including internal + free pages; good enough as a rough upper bound.
    if page_upper as usize > cfg.max_build_pages.saturating_mul(2) {
        return Ok(None);
    }

    // Cost heuristic for Auto mode:
    // - build cost ≈ inner index leaf pages (sequential IO, cache friendly)
    // - benefit ≈ outer_distinct_keys × miss_rate × per_lookup_cost
    // When inner is much larger than outer, outer is usually a sparse filtered set and the bloom
    // pays off weakly; when outer is on the same order of magnitude as inner or larger, the share
    // of "no match" keys in outer can be sizable and the bloom likely wins. Require conservatively:
    //   outer_distinct_keys × 2 >= inner_rows to be considered worthwhile (auto)
    //   `on` mode skips the heuristic and only checks the budget.
    if matches!(cfg.mode, InljProbeBloomMode::Auto)
        && right_total_rows > 0
        && (outer_distinct_keys as u64).saturating_mul(2) < right_total_rows
    {
        return Ok(None);
    }

    // Estimated entry count: inner row count + a safe floor. FPR is proportional to
    // bit count, so slight overestimation is safer than underestimation. Capped at 10M here so oversized blooms don't eat memory.
    let expected_entries = (right_total_rows as usize)
        .max((page_upper as usize).saturating_mul(64))
        .max(outer_distinct_keys)
        .min(10_000_000);

    let mut bloom = BloomFilter::new(expected_entries);
    let mut inserted = 0usize;
    let completed = db.for_each_index_leaf_key_prefix_with_handle(
        index_handle,
        eq_prefix_len,
        cfg.max_build_pages,
        |prefix: &[KeyPart]| {
            // Hash the IndexKey prefix; the contains side hashes an outer key of
            // the same length, so hashes agree (both use the derived Hash of [KeyPart]).
            bloom.insert(prefix);
            inserted += 1;
        },
    )?;
    if !completed {
        // Page cap hit mid-scan — the bloom is incomplete; drop it to avoid false negatives
        return Ok(None);
    }
    if inserted == 0 {
        // An empty index could also be handled correctly, but the caller already has an
        // unmatched path for empty entries; return None here and let the original path take unmatched_batched_inlj_rows naturally.
        return Ok(None);
    }
    INLJ_PROBE_BLOOM_BUILDS.fetch_add(1, Ordering::Relaxed);
    Ok(Some(bloom))
}

/// Global metrics for the INLJ probe bloom (exposed via SHOW ENGINE MYDB STATUS).
/// - `INLJ_PROBE_BLOOM_BUILDS`: number of successful bloom builds (each batched path may +1)
/// - `INLJ_PROBE_BLOOM_KEYS_FILTERED`: cumulative outer distinct keys rejected by the bloom
///   (each rejection saves one B-tree descent ≈ 3-4 page reads)
/// - `INLJ_PROBE_BLOOM_KEYS_PROBED`: cumulative outer distinct keys tested against the bloom;
///   hit rate = (probed - filtered) / probed
pub(crate) static INLJ_PROBE_BLOOM_BUILDS: AtomicU64 = AtomicU64::new(0);
pub(crate) static INLJ_PROBE_BLOOM_KEYS_FILTERED: AtomicU64 = AtomicU64::new(0);
pub(crate) static INLJ_PROBE_BLOOM_KEYS_PROBED: AtomicU64 = AtomicU64::new(0);

/// Global metrics for the INLJ MRR LRU (exposed via SHOW ENGINE MYDB STATUS).
/// - `INLJ_MRR_LRU_HITS`: cumulative LRU hits (each hit saves one B-tree descent)
/// - `INLJ_MRR_LRU_MISSES`: cumulative LRU misses (filled into the cache after a real lookup)
/// - `INLJ_MRR_LRU_EVICTIONS`: cumulative evictions of the least recently used entry
///   hit rate = hits / (hits + misses)
pub(crate) static INLJ_MRR_LRU_HITS: AtomicU64 = AtomicU64::new(0);
pub(crate) static INLJ_MRR_LRU_MISSES: AtomicU64 = AtomicU64::new(0);
pub(crate) static INLJ_MRR_LRU_EVICTIONS: AtomicU64 = AtomicU64::new(0);

/// MRR bounds cache used by `execute_index_nested_loop_join_from_left_rows`.
/// Wraps either the new LRU (default) or a "first cap wins" `HashMap` (the
/// historical behaviour, kept as a kill switch via `MYDB_INLJ_MRR_LRU=off`).
///
/// `K = (Option<IndexBound>, Option<IndexBound>)` in practice; parameterised
/// here for testability. Counters are bumped on every get/insert so SHOW
/// ENGINE MYDB STATUS can report hit / miss / eviction totals.
pub(super) enum MrrBoundsCache<V> {
    Lru(
        crate::engine::executor::select::mrr_lru::MrrLru<
            (Option<IndexBound>, Option<IndexBound>),
            V,
        >,
    ),
    Legacy {
        map: hashbrown::HashMap<(Option<IndexBound>, Option<IndexBound>), V>,
        cap: usize,
    },
}

impl<V> MrrBoundsCache<V> {
    pub(super) fn new(cap: usize, use_lru: bool) -> Self {
        if use_lru {
            MrrBoundsCache::Lru(
                crate::engine::executor::select::mrr_lru::MrrLru::with_capacity(cap),
            )
        } else {
            MrrBoundsCache::Legacy {
                map: hashbrown::HashMap::new(),
                cap,
            }
        }
    }

    pub(super) fn get(&mut self, key: &(Option<IndexBound>, Option<IndexBound>)) -> Option<&V> {
        let hit = match self {
            MrrBoundsCache::Lru(lru) => lru.get(key),
            MrrBoundsCache::Legacy { map, .. } => map.get(key),
        };
        if hit.is_some() {
            INLJ_MRR_LRU_HITS.fetch_add(1, Ordering::Relaxed);
        } else {
            INLJ_MRR_LRU_MISSES.fetch_add(1, Ordering::Relaxed);
        }
        hit
    }

    pub(super) fn insert(&mut self, key: (Option<IndexBound>, Option<IndexBound>), value: V) {
        match self {
            MrrBoundsCache::Lru(lru) => {
                if lru.insert(key, value).is_some() {
                    INLJ_MRR_LRU_EVICTIONS.fetch_add(1, Ordering::Relaxed);
                }
            }
            MrrBoundsCache::Legacy { map, cap } => {
                if map.len() < *cap {
                    map.insert(key, value);
                }
            }
        }
    }
}

/// The batched INLJ's "which indexes does one key / one left row map to" lists.
///
/// In most foreign-key joins each eq key maps to exactly one left row and each left
/// row matches exactly one right entry, so inlining 1 element covers the dominant shape:
/// the `key_to_left` / `left_for_key` / `by_left` tables each used to be a `Vec<usize>`; at
/// TPC-H Q9 scale (~320K left rows per batch) those three alone caused nearly a million
/// small malloc/free per query — `RawVec::grow_one` + `Entry::or_default` in profiles are them.
/// After inlining, the common shape allocates nothing; overflowing multi-value keys still fall back to the heap.
pub(super) type LeftIndexList = smallvec::SmallVec<[usize; 1]>;

/// Build the sorted, distinct probe-key array and its parallel outer-row
/// adjacency lists in one pass over a sorted `(key, left-row)` vector.
///
/// The non-covering INLJ used to build a `HashMap<IndexKey, ...>`, clone its
/// keys into a second vector, sort that vector for the B-tree router, then
/// remove every key from the hash table to recover the adjacency lists.  A
/// batched B-tree probe requires sorted keys anyway, so sorting the keyed
/// outer-row vector directly removes the hash/clone/remove round trip and its
/// transient allocation.  Equal keys remain grouped and NULL keys are still
/// omitted by the caller before this helper, preserving join semantics.
pub(super) fn group_sorted_inlj_keys(
    mut keyed_left: Vec<(IndexKey, usize)>,
) -> (Vec<IndexKey>, Vec<LeftIndexList>) {
    keyed_left.sort_unstable_by(|(left_key, left_idx), (right_key, right_idx)| {
        left_key
            .cmp(right_key)
            .then_with(|| left_idx.cmp(right_idx))
    });

    let mut lookup_keys = Vec::with_capacity(keyed_left.len());
    let mut left_for_key: Vec<LeftIndexList> = Vec::with_capacity(keyed_left.len());
    for (key, left_idx) in keyed_left {
        if lookup_keys.last().is_some_and(|last| last == &key) {
            left_for_key
                .last_mut()
                .expect("a repeated INLJ key has an adjacency list")
                .push(left_idx);
        } else {
            lookup_keys.push(key);
            left_for_key.push(smallvec::smallvec![left_idx]);
        }
    }
    (lookup_keys, left_for_key)
}

/// Apply the probe bloom to parallel key/adjacency arrays without rebuilding
/// a hash table.  Compaction is stable so the B-tree's sorted-key contract and
/// each key's outer-row mapping stay aligned.
pub(super) fn apply_inlj_probe_bloom_grouped(
    bloom: &BloomFilter,
    lookup_keys: &mut Vec<IndexKey>,
    left_for_key: &mut Vec<LeftIndexList>,
) {
    debug_assert_eq!(lookup_keys.len(), left_for_key.len());
    let probed = lookup_keys.len() as u64;
    let mut write = 0usize;
    for read in 0..lookup_keys.len() {
        if !bloom.contains(lookup_keys[read].0.as_slice()) {
            continue;
        }
        if write != read {
            lookup_keys.swap(write, read);
            left_for_key.swap(write, read);
        }
        write += 1;
    }
    lookup_keys.truncate(write);
    left_for_key.truncate(write);
    INLJ_PROBE_BLOOM_KEYS_PROBED.fetch_add(probed, Ordering::Relaxed);
    INLJ_PROBE_BLOOM_KEYS_FILTERED.fetch_add(probed - write as u64, Ordering::Relaxed);
}

/// Single-chunk execution body of the covering-index batched INLJ (reused by the
/// outer parallel fan-out). Behaviour matches the historical inline version: compiled
/// eq-prefix evaluation, probe bloom, unique-key existence short-circuit for SEMI/ANTI and Inner/Left, entry materialization + by_left stitching.
#[allow(clippy::too_many_arguments)]
pub(super) fn execute_batched_inlj_covering(
    db: &Arc<Database>,
    txn: TxnView,
    left_rows: &[PlanRow],
    right_table: &PlanTable,
    right_index: &IndexSchema,
    join_type: crate::parser::JoinType,
    plan: &IndexLookupPlan,
    right_only_residual: Option<&Expr>,
    joined_residual: Option<&Expr>,
    right_projected_columns: Option<&[usize]>,
    _right_runtime_filters: &[RuntimeFilter],
) -> Result<Vec<PlanRow>, QueryError> {
    let index_handle = db.cached_index_handle(right_table.schema.id, &right_index.name)?;
    // Covering scans project values straight out of the index entry,
    // so the materialised `IndexKey` per match is still required.
    // eq prefix expressions are compiled once per batch (same as the non-covering path),
    // replacing the per-row legacy name-based resolution.
    let compiled_eq_exprs: Option<Vec<crate::engine::executor::select::eval::CompiledPlanExpr>> =
        left_rows.first().map(|first_row| {
            let schema =
                crate::engine::executor::select::eval::input_schema_for_plan_row(first_row);
            plan.eq_exprs
                .iter()
                .map(|expr| {
                    crate::engine::executor::select::eval::CompiledPlanExpr::compile_once(
                        expr, &schema,
                    )
                })
                .collect()
        });
    let mut keyed_left = Vec::with_capacity(left_rows.len());
    for (idx, left_row) in left_rows.iter().enumerate() {
        let Some(key) =
            build_eq_prefix_from_plan_compiled(plan, compiled_eq_exprs.as_deref(), left_row)?
        else {
            continue;
        };
        keyed_left.push((key, idx));
    }
    if keyed_left.is_empty() {
        return Ok(unmatched_batched_inlj_rows(
            left_rows,
            right_table,
            join_type,
            right_projected_columns,
        ));
    }
    // The B-tree router requires sorted keys. Sort `(key, outer-row)` once and
    // build the parallel adjacency lists directly instead of building a hash
    // table, cloning all of its keys into a second vector, and sorting again.
    // This is the same representation used by the non-covering batch path.
    let (mut lookup_keys, mut left_for_key) = group_sorted_inlj_keys(keyed_left);
    let eq_len = plan.eq_exprs.len();
    let semi_or_anti = matches!(
        join_type,
        crate::parser::JoinType::Semi | crate::parser::JoinType::Anti
    );

    // INLJ probe-side bloom (P0-B): scan the whole inner index once, insert every
    // persisted key prefix into a bloom, then use it to filter the outer keys in
    // lookup_keys that "definitely cannot match".
    //
    // Safety: the bloom is a "no false negative" filter; a key rejected by contains
    // **truly does not exist** in the inner index, so removing it from lookup_keys cannot
    // change the join result — the corresponding left row reaches the unmatched branch
    // naturally via `by_left[..] is empty` (Inner/Semi dropped, Left null-extended, Anti
    // emitted), identical to running without this optimization. The SEMI/ANTI short-circuit
    // path (`keys_with_visible_match` below) uses the shortened lookup_keys directly, preserving semantics.
    //
    // Miss scenarios (returns None): config off / too few outer keys / inner index too large
    // beyond max_build_pages / empty index. The caller then continues on the original path.
    let probe_bloom = try_build_inlj_probe_bloom(
        db,
        right_table,
        right_index,
        &index_handle,
        eq_len,
        lookup_keys.len(),
    )?;
    if let Some(bloom) = probe_bloom.as_ref() {
        apply_inlj_probe_bloom_grouped(bloom, &mut lookup_keys, &mut left_for_key);
        if lookup_keys.is_empty() {
            // Every outer key was rejected by the bloom — inner has no matches at all.
            // Equivalent to the batched lookup returning empty entries.
            return Ok(unmatched_batched_inlj_rows(
                left_rows,
                right_table,
                join_type,
                right_projected_columns,
            ));
        }
    }

    // Anti/Semi w/o residuals: storage-layer short-circuit. The
    // executor only needs "does this outer key have any visible
    // inner row?" per key — which `keys_with_visible_match_with_handle`
    // delivers directly, breaking the per-key leaf scan at the
    // first visible hit. For TPC-H Q22 this collapses ~190K
    // entry materialisations to ~12K visibility checks.
    // Left rows with a NULL key (build_eq_prefix_from_plan returns
    // None) never enter `key_to_left`, so they stay `unmatched` —
    // emitted by Anti, dropped by Semi, matching the slow path.
    if semi_or_anti && right_only_residual.is_none() && joined_residual.is_none() {
        let matched_per_key = db.keys_with_visible_match_sorted_with_handle(
            &index_handle,
            &right_table.schema,
            right_index,
            &lookup_keys,
            txn,
        )?;
        let mut left_matched = vec![false; left_rows.len()];
        for (key_idx, is_matched) in matched_per_key.iter().enumerate() {
            if !*is_matched {
                continue;
            }
            for &idx in &left_for_key[key_idx] {
                left_matched[idx] = true;
            }
        }
        let anti = matches!(join_type, crate::parser::JoinType::Anti);
        let mut out = Vec::with_capacity(left_rows.len());
        for (idx, left_row) in left_rows.iter().enumerate() {
            if anti ^ left_matched[idx] {
                out.push(left_row.clone());
            }
        }
        return Ok(out);
    }

    // P0-2: Inner/Left + no residual + unique-index full-prefix equality + right
    // projection derivable from the lookup key → one visibility existence check per key
    // (`keys_with_visible_match` short-circuits at the first visible hit), skipping entry
    // materialization, the visibility mask, and `by_left` bookkeeping. Right-side values of
    // matching rows are projected from the lookup key (uniqueness guarantees at most one row; equality guarantees the projected value equals the stored one — see the guard's type restrictions).
    if matches!(
        join_type,
        crate::parser::JoinType::Inner | crate::parser::JoinType::Left
    ) && right_only_residual.is_none()
        && joined_residual.is_none()
        && inlj_covering_unique_prefix_derivable(
            right_table,
            right_index,
            eq_len,
            right_projected_columns,
        )
    {
        let matched_per_key = db.keys_with_visible_match_sorted_with_handle(
            &index_handle,
            &right_table.schema,
            right_index,
            &lookup_keys,
            txn,
        )?;
        let projector = IndexEntryProjector::new(right_table, right_index, right_projected_columns);
        let join_lk = new_join_lookup();
        // key → matched left rows; emitted in original left order.
        let mut row_key: Vec<Option<usize>> = vec![None; left_rows.len()];
        for (key_idx, is_matched) in matched_per_key.iter().enumerate() {
            if !*is_matched {
                continue;
            }
            for &left_idx in &left_for_key[key_idx] {
                row_key[left_idx] = Some(key_idx);
            }
        }
        let is_left_join = join_type == crate::parser::JoinType::Left;
        let mut out = Vec::with_capacity(if is_left_join {
            left_rows.len()
        } else {
            row_key.iter().filter(|k| k.is_some()).count()
        });
        for (left_idx, left_row) in left_rows.iter().enumerate() {
            match row_key[left_idx] {
                Some(key_idx) => {
                    let right_row = projector.project(&lookup_keys[key_idx]);
                    out.push(join_rows_with_owned_right(left_row, right_row, &join_lk));
                }
                None if is_left_join => {
                    out.push(null_extend_right_table(
                        left_row,
                        right_table,
                        right_projected_columns,
                    ));
                }
                None => {}
            }
        }
        record_inlj_covering_existence_fast();
        return Ok(out);
    }

    let mut entries = db.lookup_index_entries_many_visible_with_handle(
        &index_handle,
        &right_table.schema,
        right_index,
        &lookup_keys,
        txn,
    )?;
    let heap = db.cached_heap_handle(right_table.schema.id)?;
    let visible_mask = db.filter_visible_index_entries_mask_with_handle(
        &heap,
        &right_table.schema,
        right_index,
        &entries,
        txn,
    )?;
    let mut visible_iter = visible_mask.into_iter();
    entries.retain(|_| visible_iter.next().unwrap_or(false));
    let mut by_left: Vec<LeftIndexList> =
        (0..left_rows.len()).map(|_| LeftIndexList::new()).collect();
    let mut candidate_row_ids = Vec::new();
    for (entry_idx, (key, row_id)) in entries.iter().enumerate() {
        if key.0.len() < eq_len {
            continue;
        }
        let prefix = IndexKey::from(key.0[..eq_len].to_vec());
        let Ok(key_idx) = lookup_keys.binary_search(&prefix) else {
            continue;
        };
        candidate_row_ids.push(*row_id);
        for &left_idx in &left_for_key[key_idx] {
            by_left[left_idx].push(entry_idx);
        }
    }
    if candidate_row_ids.is_empty() {
        return Ok(unmatched_batched_inlj_rows(
            left_rows,
            right_table,
            join_type,
            right_projected_columns,
        ));
    }
    let mut out = Vec::new();
    let projector = IndexEntryProjector::new(right_table, right_index, right_projected_columns);
    let join_lk = new_join_lookup();
    let mut residual_pre = LazyJoinedResidualPre::new();
    // Subquery residuals fall through `residual_pre` (the compiler can't
    // lower them) to the interpreter below, which must run with (db, txn)
    // so the subquery actually executes — see `execute_batched_inlj_noncovering`.
    let mut eval_ctx = EvalQueryContext::default();
    for (left_idx, left_row) in left_rows.iter().enumerate() {
        let mut matched = false;
        for &entry_idx in &by_left[left_idx] {
            let (key, _row_id) = &entries[entry_idx];
            let right_row = projector.project(key);
            if right_only_residual
                .map(|residual| eval_plan_bool(residual, &right_row))
                .transpose()?
                .is_some_and(|keep| !keep)
            {
                continue;
            }
            emit_inlj_pair!(
                db,
                txn,
                join_type,
                semi_or_anti,
                joined_residual,
                residual_pre,
                join_lk,
                left_row,
                right_row,
                out,
                matched,
                eval_ctx
            );
        }
        if !matched {
            match join_type {
                crate::parser::JoinType::Left => {
                    out.push(null_extend_right_table(
                        left_row,
                        right_table,
                        right_projected_columns,
                    ));
                }
                crate::parser::JoinType::Anti => out.push(left_row.clone()),
                _ => {}
            }
        }
    }
    Ok(out)
}

/// Shared skeleton for outer parallelism: split left_rows evenly into degree chunks,
/// run `run_chunk` in parallel on scoped threads, and stitch outputs in chunk order.
/// Worker threads rebuild the session expression context, parameter snapshot and EXPLAIN ANALYZE collectors, matching serial-execution semantics.
pub(super) fn run_inlj_outer_chunks<F>(
    db: &Arc<Database>,
    txn: TxnView,
    left_rows: &[PlanRow],
    degree: usize,
    run_chunk: F,
) -> Result<Vec<PlanRow>, QueryError>
where
    F: Fn(&[PlanRow]) -> Result<Vec<PlanRow>, QueryError> + Sync,
{
    let chunk_size = left_rows.len().div_ceil(degree).max(1);
    // Actual chunk count = number of slices (a short tail counts as a full chunk); upper bound = degree.
    let chunk_count = left_rows.len().div_ceil(chunk_size).max(1).min(degree);
    record_inlj_outer_parallel_chunks(chunk_count as u64);

    let execution_context =
        crate::engine::executor::select::execution_context::ExecutionContext::capture(txn);
    // Subquery residuals (e.g. `col IN (SELECT ...)` left on the INLJ
    // condition) are evaluated through the thread-local subquery frame +
    // CTE registry. Worker threads spawned below start with *empty*
    // thread-locals, so capturing the lexical scope on the worker (as a
    // bare `with_subquery_context` would) yields an empty CTE map and the
    // residual silently matches nothing. Snapshot the parent thread's
    // effective CTE scope here and reinstall it on every worker so a
    // parallel INLJ evaluates subquery residuals identically to the serial
    // path.
    let subquery_ctes = crate::engine::executor::select::eval::capture_subquery_ctes();
    let subquery_ctes = &subquery_ctes;

    let run_chunk = &run_chunk;
    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for chunk in left_rows.chunks(chunk_size) {
            let worker_context = execution_context.clone();

            handles.push(scope.spawn(move || -> Result<Vec<PlanRow>, QueryError> {
                let _outer_worker = InljOuterWorkerGuard::enter();
                let run = || {
                    crate::engine::executor::select::eval::with_subquery_context_ctes(
                        db,
                        txn,
                        subquery_ctes.clone(),
                        || {
                            crate::engine::executor::cte::with_cte_registry(
                                subquery_ctes.clone(),
                                || run_chunk(chunk),
                            )
                        },
                    )
                };
                let run_with_context = || worker_context.run(db, || run());
                run_with_context()
            }));
        }
        // Preserve chunk order while moving each worker result directly into
        // the first result buffer. Retaining every part and then allocating a
        // second full output doubled the live rows at every stacked INLJ.
        let mut handles = handles.into_iter();
        let Some(first) = handles.next() else {
            return Ok(Vec::new());
        };
        let mut out = first.join().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "INLJ outer worker panicked",
            )
        })??;
        for h in handles {
            let mut part = h.join().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "INLJ outer worker panicked",
                )
            })??;
            out.append(&mut part);
        }
        Ok(out)
    })
}

/// P0-2 guard: conditions for the covering-batch INLJ's Inner/Left to degenerate to
/// "one visibility existence check per key".
///
/// - the equality prefix exactly covers all key columns of the unique index → a hit is
///   at most one row, so the join neither amplifies nor shrinks (apart from non-matches);
/// - all right projection columns lie inside the equality prefix → right-side values of
///   matching rows derive directly from the lookup key via `IndexEntryProjector::project`, no entry materialization;
/// - the types of those key columns satisfy "value equal ⇔ encoding equal" under KeyPart
///   encoding: integer/BOOL/fixed-point/date-time families. Strings excluded (collation
///   equality does not imply byte-equal originals; projecting lookup keys would echo the left spelling as the stored value), floats excluded (±0/NaN), and prefix-index
///   columns and function-key columns excluded (the key carries no full original value).
///
/// Typical beneficiaries: `FK JOIN PK where downstream only references the join key itself`
/// (the orders level of TPC-H Q9, and every dimension-table attach with "existence" semantics).
pub(super) fn inlj_covering_unique_prefix_derivable(
    right_table: &PlanTable,
    index: &IndexSchema,
    eq_len: usize,
    projection: Option<&[usize]>,
) -> bool {
    if !index.unique || eq_len == 0 || eq_len != index.columns.len() {
        return false;
    }
    if index.key_expressions.iter().any(|e| e.is_some()) {
        return false;
    }
    if index
        .column_prefix_lengths
        .iter()
        .take(eq_len)
        .any(|p| p.is_some())
    {
        return false;
    }
    let key_map: HashMap<String, usize> = index
        .columns
        .iter()
        .enumerate()
        .map(|(i, name)| (name.to_ascii_lowercase(), i))
        .collect();
    let all_columns: Vec<usize>;
    let projected: &[usize] = match projection {
        Some(p) => p,
        None => {
            all_columns = (0..right_table.schema.columns.len()).collect();
            &all_columns
        }
    };
    projected.iter().all(|&col_idx| {
        let Some(column) = right_table.schema.columns.get(col_idx) else {
            return false;
        };
        let Some(&pos) = key_map.get(&column.name.to_ascii_lowercase()) else {
            return false;
        };
        if pos >= eq_len {
            return false;
        }
        matches!(
            column.data_type,
            DataTypeKind::TinyInt
                | DataTypeKind::SmallInt
                | DataTypeKind::MediumInt
                | DataTypeKind::Int
                | DataTypeKind::BigInt
                | DataTypeKind::Bool
                | DataTypeKind::Decimal { .. }
                | DataTypeKind::Date
                | DataTypeKind::Time { .. }
                | DataTypeKind::DateTime { .. }
                | DataTypeKind::Timestamp { .. }
                | DataTypeKind::Year
        )
    })
}

pub(super) fn try_execute_batched_index_nested_loop_join(
    db: &Arc<Database>,
    txn: TxnView,
    left_rows: &[PlanRow],
    right_table: &PlanTable,
    right_index: &IndexSchema,
    join_type: crate::parser::JoinType,
    plan: &IndexLookupPlan,
    right_covering: bool,
    right_only_residual: Option<&Expr>,
    joined_residual: Option<&Expr>,
    right_projected_columns: Option<&[usize]>,
    right_runtime_filters: &[RuntimeFilter],
) -> Result<Option<Vec<PlanRow>>, QueryError> {
    if !matches!(
        join_type,
        crate::parser::JoinType::Inner
            | crate::parser::JoinType::Left
            | crate::parser::JoinType::Semi
            | crate::parser::JoinType::Anti
    ) || plan.range.is_some()
        || plan.eq_exprs.is_empty()
    {
        return Ok(None);
    }
    let mut min_left_rows = crate::util::env::var("MYDB_INLJ_BATCH_MIN_LEFT_ROWS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(8192);
    let right_rows = right_table.schema.stats.row_count;
    // Existing first tier: with right ≥ 1k, left ≥ 64 lowers the bar to 64, triggering
    // multi-keyed lookup amortization of upper B-tree node accesses.
    if right_rows >= 1_000 && left_rows.len() >= 64 {
        min_left_rows = min_left_rows.min(64);
    }
    // WS-N3 lightweight small-INLJ batch mode: with right ≥ 256 and left ≥ 8, lower to 8 —
    // shapes with only a few outer rows (typically single digits left after harsh filtering
    // post dimension join) do one full B-tree descent per row on the per-row path, while
    // batched does one shared upper-node scan + multi-key leaf seek, already winning at right_rows ≥ 256 in measurements.
    // Tiny left inputs below the threshold "4" still go per-row (sort+dedup overhead outweighs the amortization).
    let min_right_rows_for_small_batch = crate::util::env::var("MYDB_INLJ_BATCH_MIN_RIGHT_ROWS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(256);
    if right_rows >= min_right_rows_for_small_batch && left_rows.len() >= 8 {
        min_left_rows = min_left_rows.min(8);
    }
    if left_rows.len() < min_left_rows {
        return Ok(None);
    }
    // `MYDB_INLJ_BATCH_MAX_RIGHT_ROWS` is an escape hatch only; the batched
    // path's memory is `O(left_rows + matched_entries + filtered_row_ids)`,
    // not `O(right_table_rows)`, so capping on right-table cardinality just
    // disabled the optimization on TPC-H-scale fact tables. The cooperative
    // `query_mem_limit_bytes()` / `process_mem_limit_bytes()` budgets
    // (derived from `MYDB_MEMORY_TARGET`; `MYDB_QUERY_MEM_LIMIT_MB` /
    // `MYDB_PROCESS_MEM_LIMIT_MB` env vars override) remain the real
    // ceilings.
    let max_right_rows = crate::util::env::var("MYDB_INLJ_BATCH_MAX_RIGHT_ROWS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    if max_right_rows > 0 && right_rows > max_right_rows {
        return Ok(None);
    }

    if right_covering {
        // P0-3: the covering batch path gains outer parallelism isomorphic to the
        // non-covering one. Each worker independently builds key_to_left / bloom /
        // lookup for its own left chunk; results are stitched in chunk order.
        let degree = inlj_outer_parallelism_for_working_set(
            db,
            left_rows.len(),
            right_table,
            right_index,
            true,
        );
        if degree <= 1 {
            return Ok(Some(execute_batched_inlj_covering(
                db,
                txn,
                left_rows,
                right_table,
                right_index,
                join_type,
                plan,
                right_only_residual,
                joined_residual,
                right_projected_columns,
                right_runtime_filters,
            )?));
        }
        return Ok(Some(run_inlj_outer_chunks(
            db,
            txn,
            left_rows,
            degree,
            |chunk| {
                execute_batched_inlj_covering(
                    db,
                    txn,
                    chunk,
                    right_table,
                    right_index,
                    join_type,
                    plan,
                    right_only_residual,
                    joined_residual,
                    right_projected_columns,
                    right_runtime_filters,
                )
            },
        )?));
    }

    // Non-covering batched INLJ: optionally split the left input across
    // outer workers (PG-style Exchange over Nested Loop). Each worker
    // rebuilds its own key_to_left / lookup_keys / by_left for its
    // chunk of `left_rows`, probes the index keyed, filters, and joins
    // independently; results are concatenated in chunk order.
    let degree = inlj_outer_parallelism_for_working_set(
        db,
        left_rows.len(),
        right_table,
        right_index,
        false,
    );
    if degree <= 1 {
        return Ok(Some(execute_batched_inlj_noncovering(
            db,
            txn,
            left_rows,
            right_table,
            right_index,
            join_type,
            plan,
            right_only_residual,
            joined_residual,
            right_projected_columns,
            right_runtime_filters,
            false,
        )?));
    }
    Ok(Some(run_inlj_outer_chunks(
        db,
        txn,
        left_rows,
        degree,
        |chunk| {
            execute_batched_inlj_noncovering(
                db,
                txn,
                chunk,
                right_table,
                right_index,
                join_type,
                plan,
                right_only_residual,
                joined_residual,
                right_projected_columns,
                right_runtime_filters,
                true,
            )
        },
    )?))
}

pub(super) fn inlj_fuse_semi_anti_enabled() -> bool {
    !matches!(
        crate::util::env::var("MYDB_INLJ_FUSE_SEMI_ANTI")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "off" | "0" | "false" | "no"
    )
}

/// Detect and execute the SEMI/ANTI ⊕ SEMI/ANTI fusion described in
/// [`execute_index_nested_loop_join`]. Returns `Ok(None)` for any shape
/// the fusion can't handle so the caller falls back to the regular
/// stacked INLJ path.
pub(super) fn try_fuse_consecutive_semi_anti_inlj(
    db: &Arc<Database>,
    txn: TxnView,
    outer_left: &PhysicalPlan,
    outer_right_table: &PlanTable,
    outer_right_index: &IndexSchema,
    outer_join_type: crate::parser::JoinType,
    outer_condition: &Expr,
    outer_plan: &IndexLookupPlan,
    outer_right_projected_columns: Option<&[usize]>,
) -> Result<Option<Vec<PlanRow>>, QueryError> {
    if !inlj_fuse_semi_anti_enabled() {
        return Ok(None);
    }
    if !matches!(
        outer_join_type,
        crate::parser::JoinType::Semi | crate::parser::JoinType::Anti
    ) {
        return Ok(None);
    }
    // The eq prefix must be fully indexed and the residual must be a
    // pure right-only expression: fusion would otherwise have to
    // distinguish "outer row matched the inner residual" from "outer
    // row matched the index bounds", which adds bookkeeping that
    // doesn't pay off.
    if outer_plan.range.is_some() || outer_plan.eq_exprs.is_empty() {
        return Ok(None);
    }

    let (
        inner_left,
        inner_right_table,
        inner_right_index,
        inner_join_type,
        inner_condition,
        inner_right_covering,
        inner_right_projected_columns,
    ) = match outer_left {
        PhysicalPlan::IndexNestedLoopJoin {
            left,
            right_table,
            right_index,
            join_type,
            condition: Some(condition),
            right_covering,
            right_projected_columns,
            ..
        } => (
            left.as_ref(),
            right_table,
            right_index,
            *join_type,
            condition,
            *right_covering,
            right_projected_columns.as_ref(),
        ),
        _ => return Ok(None),
    };
    if !matches!(
        inner_join_type,
        crate::parser::JoinType::Semi | crate::parser::JoinType::Anti
    ) {
        return Ok(None);
    }
    if inner_right_covering {
        // Covering INLJ projects values straight from the index entry —
        // already cheap. Fusion would have to handle covering+non-
        // covering mixes which adds branching we don't need for the
        // win we're targeting. Bail.
        return Ok(None);
    }
    if inner_right_table.schema.id != outer_right_table.schema.id {
        return Ok(None);
    }
    if !inner_right_index
        .name
        .eq_ignore_ascii_case(&outer_right_index.name)
    {
        return Ok(None);
    }

    let inner_plan =
        compile_index_lookup_plan(inner_condition, inner_right_table, inner_right_index)?;
    if inner_plan.range.is_some() {
        return Ok(None);
    }
    if inner_plan.eq_exprs.len() != outer_plan.eq_exprs.len() {
        return Ok(None);
    }
    for (a, b) in inner_plan.eq_exprs.iter().zip(outer_plan.eq_exprs.iter()) {
        if !expressions_equivalent(a, b) {
            return Ok(None);
        }
    }

    // Dry-compile both residuals against a synthesized left schema +
    // each side's alias metas. This short-circuits the fusion before
    // we pay the inner-left execution if either residual contains a
    // shape `ExprCompiler` can't handle — the surrounding code falls
    // back to the existing stacked-INLJ path with no extra work.
    let projection: Vec<usize> = build_fused_semi_anti_projection(
        inner_condition,
        outer_condition,
        inner_right_table,
        outer_right_table,
        inner_right_projected_columns.map(|v| v.as_slice()),
        outer_right_projected_columns,
    );
    if projection.is_empty() {
        return Ok(None);
    }
    let mut left_meta_template: Vec<ColMeta> = Vec::new();
    collect_plan_fields(inner_left, &mut left_meta_template);
    let inner_metas_template = build_alias_right_metas(
        outer_right_table,
        &projection,
        inner_right_table.alias.clone(),
    );
    let outer_metas_template = build_alias_right_metas(
        outer_right_table,
        &projection,
        outer_right_table.alias.clone(),
    );
    if !precompile_joined_residual_compatible(
        inner_condition,
        &left_meta_template,
        &inner_metas_template,
    ) {
        return Ok(None);
    }
    if !precompile_joined_residual_compatible(
        outer_condition,
        &left_meta_template,
        &outer_metas_template,
    ) {
        return Ok(None);
    }

    // Evaluate the deepest left side that feeds both INLJs. The full
    // outer key list lives here, so the fused executor walks it once.
    let left_rows = execute_plan_rows(db, txn, inner_left)?;

    let fused = execute_fused_semi_anti_inlj_parallel(
        db,
        txn,
        &left_rows,
        outer_right_table,
        outer_right_index,
        outer_plan,
        &projection,
        inner_right_table.alias.clone(),
        outer_right_table.alias.clone(),
        inner_join_type,
        inner_condition,
        outer_join_type,
        outer_condition,
        outer_right_projected_columns,
    )?;
    record_inlj_fusion_count();
    Ok(Some(fused))
}

/// Global-lookup wrapper around [`execute_fused_semi_anti_inlj`].
///
/// The fused core deduplicates all equality keys, performs one index lookup
/// and one page-ordered heap read, then parallelises only residual evaluation.
/// Keeping the lookup above the worker boundary prevents duplicate B-tree and
/// heap work when the same key appears in more than one outer chunk.
#[allow(clippy::too_many_arguments)]
pub(super) fn execute_fused_semi_anti_inlj_parallel(
    db: &Arc<Database>,
    txn: TxnView,
    left_rows: &[PlanRow],
    right_table: &PlanTable,
    right_index: &IndexSchema,
    plan: &IndexLookupPlan,
    projection: &[usize],
    inner_right_alias: Option<String>,
    outer_right_alias: Option<String>,
    inner_join_type: crate::parser::JoinType,
    inner_condition: &Expr,
    outer_join_type: crate::parser::JoinType,
    outer_condition: &Expr,
    outer_right_projected_columns: Option<&[usize]>,
) -> Result<Vec<PlanRow>, QueryError> {
    execute_fused_semi_anti_inlj(
        db,
        txn,
        left_rows,
        right_table,
        right_index,
        plan,
        projection,
        inner_right_alias,
        outer_right_alias,
        inner_join_type,
        inner_condition,
        outer_join_type,
        outer_condition,
        outer_right_projected_columns,
    )
}

/// Build the union of `inner` and `outer` residual-referenced columns,
/// resolved against the right table. Returns an empty Vec when either
/// side can't be classified (shape we can't analyse statically).
pub(super) fn build_fused_semi_anti_projection(
    inner_condition: &Expr,
    outer_condition: &Expr,
    inner_right_table: &PlanTable,
    outer_right_table: &PlanTable,
    inner_right_projected_columns: Option<&[usize]>,
    outer_right_projected_columns: Option<&[usize]>,
) -> Vec<usize> {
    let inner_min = inlj_residual_right_columns(Some(inner_condition), None, inner_right_table);
    let outer_min = inlj_residual_right_columns(Some(outer_condition), None, outer_right_table);
    match (inner_min, outer_min) {
        (Some(a), Some(b)) => {
            let mut seen: std::collections::BTreeSet<usize> = std::collections::BTreeSet::new();
            seen.extend(a.iter().copied());
            seen.extend(b.iter().copied());
            seen.into_iter().collect()
        }
        _ => inner_right_projected_columns
            .map(|c| c.to_vec())
            .or_else(|| outer_right_projected_columns.map(|c| c.to_vec()))
            .unwrap_or_default(),
    }
}

pub(super) fn build_alias_right_metas(
    right_table: &PlanTable,
    projection: &[usize],
    alias: Option<String>,
) -> Vec<ColMeta> {
    projection
        .iter()
        .filter_map(|&idx| {
            let column = right_table.schema.columns.get(idx)?;
            Some(ColMeta {
                invisible: column.invisible,
                table: right_table.schema.name.clone(),
                alias: alias.clone(),
                column: column.name.clone(),
                column_type: right_table.schema.result_type(idx),
                source: Default::default(),
            })
        })
        .collect()
}

pub(super) fn precompile_joined_residual_compatible(
    residual: &Expr,
    left_meta_template: &[ColMeta],
    right_metas: &[ColMeta],
) -> bool {
    use crate::engine::expr_program::{ExprCompiler, InputColumn, InputSchema};
    let mut cols: Vec<InputColumn> =
        Vec::with_capacity(left_meta_template.len() + right_metas.len());
    for meta in left_meta_template {
        cols.push(meta.input_column());
    }
    for meta in right_metas {
        cols.push(meta.input_column());
    }
    let schema = InputSchema::new(cols);
    ExprCompiler::compile_strict(residual, &schema).is_ok()
}

pub(super) fn expressions_equivalent(a: &Expr, b: &Expr) -> bool {
    crate::engine::expr_program::expr_fingerprint(a)
        == crate::engine::expr_program::expr_fingerprint(b)
}

pub(super) struct FusedSemiAntiNeqSpec {
    pub(super) left_idx: usize,
    pub(super) right_idx: usize,
    pub(super) right_only: Option<CompiledRowPredicate>,
}

pub(super) fn compile_fused_semi_anti_neq_spec(
    condition: &Expr,
    left_row: &PlanRow,
    right_table: &PlanTable,
    right_index: &IndexSchema,
    lookup_plan: &IndexLookupPlan,
    projection: &[usize],
    right_alias: Option<String>,
) -> Option<FusedSemiAntiNeqSpec> {
    let left_metas: Vec<ColMeta> = left_row
        .fields
        .iter()
        .map(|field| (*field.meta).clone())
        .collect();
    let right_metas = build_alias_right_metas(right_table, projection, right_alias.clone());
    let left_lookup = FieldLookup::build(&left_metas);
    let right_lookup = FieldLookup::build(&right_metas);
    let left_refs = field_ref_set(&left_metas);
    let mut aliased_right = right_table.clone();
    aliased_right.alias = right_alias;

    let mut neq = None;
    let mut right_only = Vec::new();
    for conjunct in expr_conjuncts(condition) {
        if expr_references_only_right_table(conjunct, &aliased_right, &left_refs) {
            right_only.push(conjunct.clone());
            continue;
        }
        let Expr::Binary { left, op, right } = conjunct else {
            return None;
        };
        let cross = match (
            resolve_hash_join_key_index(left, &left_lookup),
            resolve_hash_join_key_index(left, &right_lookup),
            resolve_hash_join_key_index(right, &left_lookup),
            resolve_hash_join_key_index(right, &right_lookup),
        ) {
            (Some(left_idx), None, None, Some(right_idx)) => Some((left_idx, right_idx)),
            (None, Some(right_idx), Some(left_idx), None) => Some((left_idx, right_idx)),
            _ => None,
        };
        let Some((left_idx, right_idx)) = cross else {
            return None;
        };
        match op {
            BinaryOp::NotEq if neq.is_none() => neq = Some((left_idx, right_idx)),
            BinaryOp::Eq => {
                let Some(schema_idx) = projection.get(right_idx).copied() else {
                    return None;
                };
                let right_column = &right_table.schema.columns.get(schema_idx)?.name;
                let is_lookup_equality = right_index
                    .columns
                    .iter()
                    .zip(lookup_plan.eq_exprs.iter())
                    .any(|(index_column, outer_expr)| {
                        index_column.eq_ignore_ascii_case(right_column)
                            && resolve_hash_join_key_index(outer_expr, &left_lookup)
                                == Some(left_idx)
                    });
                if !is_lookup_equality {
                    return None;
                }
            }
            _ => return None,
        }
    }
    let (left_idx, right_idx) = neq?;
    let mut projected_schema = projected_schema_view(&right_table.schema, projection);
    if let Some(alias) = aliased_right.alias.as_ref() {
        projected_schema.name = alias.clone();
    }
    let right_only = match crate::planner::combine_conjuncts(right_only) {
        Some(expr) => Some(compile_projected_row_predicate(&expr, &projected_schema)?),
        None => None,
    };
    Some(FusedSemiAntiNeqSpec {
        left_idx,
        right_idx,
        right_only,
    })
}

pub(super) fn empty_neq_build_summary() -> NeqBuildSummary {
    NeqBuildSummary {
        non_null: 0,
        first: ValueKey::Null,
        all_same: true,
    }
}

pub(super) fn update_neq_build_summary(summary: &mut NeqBuildSummary, value: &Value) {
    if matches!(value, Value::Null) {
        return;
    }
    let key = ValueKey::from(value);
    summary.non_null = summary.non_null.saturating_add(1);
    if summary.non_null == 1 {
        summary.first = key;
        summary.all_same = true;
    } else if summary.first != key {
        summary.all_same = false;
    }
}

fn update_fused_neq_summary(
    spec: &FusedSemiAntiNeqSpec,
    summary: &mut NeqBuildSummary,
    values: &[Value],
) -> Result<(), QueryError> {
    if spec
        .right_only
        .as_ref()
        .map(|p| p.eval(values))
        .transpose()?
        .unwrap_or(true)
    {
        update_neq_build_summary(summary, values.get(spec.right_idx).unwrap_or(&Value::Null));
    }
    Ok(())
}

pub(super) fn build_fused_neq_summaries_pair(
    inner_spec: Option<&FusedSemiAntiNeqSpec>,
    outer_spec: Option<&FusedSemiAntiNeqSpec>,
    key_count: usize,
    entries: &[(u32, RowId)],
    entry_to_right_pos: &[i32],
    right_pairs: &[(RowId, Vec<Value>)],
) -> Result<(Option<Vec<NeqBuildSummary>>, Option<Vec<NeqBuildSummary>>), QueryError> {
    let mut inner = inner_spec.map(|_| vec![empty_neq_build_summary(); key_count]);
    let mut outer = outer_spec.map(|_| vec![empty_neq_build_summary(); key_count]);
    for (entry_idx, (key_idx, _)) in entries.iter().enumerate() {
        let Some(&position) = entry_to_right_pos.get(entry_idx) else {
            continue;
        };
        if position < 0 {
            continue;
        }
        let values = &right_pairs[position as usize].1;
        if let (Some(spec), Some(summaries)) = (inner_spec, inner.as_mut()) {
            if let Some(summary) = summaries.get_mut(*key_idx as usize) {
                update_fused_neq_summary(spec, summary, values)?;
            }
        }
        if let (Some(spec), Some(summaries)) = (outer_spec, outer.as_mut()) {
            if let Some(summary) = summaries.get_mut(*key_idx as usize) {
                update_fused_neq_summary(spec, summary, values)?;
            }
        }
    }

    Ok((inner, outer))
}

/// Map index entries onto the sorted heap-read result. A batched lookup over
/// a clustering-compatible index commonly emits RowIds monotonically (and a
/// primary-key/load-order correlation is enough); in that case use a single
/// merge walk instead of one binary search per candidate. Secondary indexes
/// with unrelated physical order retain the existing O(n log m) lookup.
pub(super) fn map_inlj_entries_to_right_positions(
    entries: &[(u32, RowId)],
    right_pairs: &[(RowId, Vec<Value>)],
) -> Result<Vec<i32>, QueryError> {
    let binary_search_fallback = || -> Result<Vec<i32>, QueryError> {
        // `right_pairs` is page/slot ordered by the MRR heap read.  This path
        // needs only the returned position vector, making it the pressure-safe
        // oracle when the optional RowId hash accelerator cannot be admitted.
        let output_bytes = entries.len().saturating_mul(std::mem::size_of::<i32>());
        let mut out = Vec::new();
        out.try_reserve_exact(entries.len()).map_err(|_| {
            crate::engine::executor::mem_budget::allocation_error(format!(
                "failed to allocate {} bytes for batched INLJ RowId positions",
                output_bytes
            ))
        })?;
        for &(_, row_id) in entries {
            let position = right_pairs
                .binary_search_by_key(&row_id, |(candidate, _)| *candidate)
                .ok()
                .and_then(|position| i32::try_from(position).ok())
                .unwrap_or(-1);
            out.push(position);
        }
        Ok(out)
    };
    let hashed_fallback = || -> Result<Vec<i32>, QueryError> {
        // Secondary-index entries are ordered by index key rather than heap
        // RowId.  A binary search in `right_pairs` for every entry made this
        // fallback O(entries * log(right_rows)) and dominated wide batched
        // INLJs.  Build one query-local RowId -> position table instead.
        //
        // Charge a conservative bucket estimate before allocating.  Hashbrown
        // keeps at most 7/8 of each power-of-two bucket array occupied; the
        // extra byte per bucket covers its control array.  The reservation is
        // transient and follows the map until this helper returns.
        let bucket_count = right_pairs
            .len()
            .saturating_mul(8)
            .div_ceil(7)
            .checked_next_power_of_two()
            .unwrap_or(usize::MAX);
        let map_bytes =
            bucket_count.saturating_mul(std::mem::size_of::<(RowId, i32)>().saturating_add(1));
        let _map_charge = match crate::engine::executor::mem_budget::current_budget() {
            Some(budget) if budget.remaining() < map_bytes => {
                return binary_search_fallback();
            }
            Some(_) => {
                match crate::engine::executor::mem_budget::ScopedCharge::new_strict(
                    map_bytes,
                    "batched INLJ RowId position map",
                ) {
                    Ok(charge) => Some(charge),
                    // Another worker can consume the observed headroom before
                    // the strict charge. The accelerator is optional, so a
                    // concurrent soft miss has the same allocation-free
                    // fallback as the fast headroom check above.
                    Err(_) => return binary_search_fallback(),
                }
            }
            // Unit tests call this pure helper without installing a query
            // budget. Production execution always has one.
            None => None,
        };
        let mut positions: hashbrown::HashMap<RowId, i32, ahash::RandomState> =
            hashbrown::HashMap::with_hasher(ahash::RandomState::new());
        positions.try_reserve(right_pairs.len()).map_err(|_| {
            crate::engine::executor::mem_budget::allocation_error(format!(
                "failed to allocate {} bytes for batched INLJ RowId position map",
                map_bytes
            ))
        })?;
        for (position, (row_id, _)) in right_pairs.iter().enumerate() {
            positions.insert(*row_id, position as i32);
        }
        Ok(entries
            .iter()
            .map(|(_, row_id)| positions.get(row_id).copied().unwrap_or(-1))
            .collect())
    };
    let mut out = Vec::with_capacity(entries.len());
    let mut position = 0usize;
    let mut previous = None;
    for &(_, row_id) in entries {
        if previous.is_some_and(|previous| previous > row_id) {
            return hashed_fallback();
        }
        previous = Some(row_id);
        while right_pairs
            .get(position)
            .is_some_and(|pair| pair.0 < row_id)
        {
            position += 1;
        }
        out.push(
            if right_pairs
                .get(position)
                .is_some_and(|pair| pair.0 == row_id)
            {
                position as i32
            } else {
                -1
            },
        );
    }
    Ok(out)
}

pub(super) fn fused_neq_summary_matches(
    spec: &FusedSemiAntiNeqSpec,
    summary: Option<&NeqBuildSummary>,
    left_row: &PlanRow,
) -> bool {
    let Some(summary) = summary else {
        return false;
    };
    if summary.non_null == 0 {
        return false;
    }
    let Some(value) = left_row.fields.get(spec.left_idx).map(PlanField::value_ref) else {
        return false;
    };
    if matches!(value, Value::Null) {
        return false;
    }
    !summary.all_same || summary.first != ValueKey::from(value)
}

pub(crate) static INLJ_FUSED_SEMI_ANTI_COUNT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static INLJ_FUSED_SEMI_ANTI_NEQ_SUMMARY_COUNT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[inline]
pub(super) fn record_inlj_fusion_count() {
    INLJ_FUSED_SEMI_ANTI_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

/// Metadata for a columnar fused equality/inequality existence probe. Lookup
/// targets retain the ordinary index codec's type, unsignedness and collation.
pub(in crate::engine::executor::select) struct FusedNeqBatchSpec {
    table: PlanTable,
    index: IndexSchema,
    lookup: IndexLookupPlan,
    projection: Vec<usize>,
    key_columns: Vec<usize>,
    inner: FusedSemiAntiNeqSpec,
    outer: FusedSemiAntiNeqSpec,
    inner_is_semi: bool,
    outer_is_semi: bool,
    candidate_row_bytes: usize,
    pub(in crate::engine::executor::select) work_bytes_per_row: usize,
}

impl FusedNeqBatchSpec {
    pub(in crate::engine::executor::select) fn locality_column(&self) -> Option<usize> {
        (self.key_columns.len() == 1).then(|| self.key_columns[0])
    }
}

pub(in crate::engine::executor::select) fn compile_fused_neq_batch_spec(
    plan: &PhysicalPlan,
    template: &[PlanField],
) -> Option<FusedNeqBatchSpec> {
    if !inlj_consecutive_semi_anti_fusion_applicable(plan) {
        return None;
    }
    let PhysicalPlan::IndexNestedLoopJoin {
        left,
        right_table: table,
        right_index: index,
        join_type: outer_type,
        condition: Some(outer_condition),
        right_projected_columns: outer_projection,
        ..
    } = plan
    else {
        return None;
    };
    let PhysicalPlan::IndexNestedLoopJoin {
        right_table: inner_table,
        join_type: inner_type,
        condition: Some(inner_condition),
        right_projected_columns: inner_projection,
        ..
    } = left.as_ref()
    else {
        return None;
    };
    // Prefix/expression indexes do not prove equality of the original lanes.
    if index.has_prefix_columns() || index.has_expression_parts() {
        return None;
    }
    let lookup = compile_index_lookup_plan(outer_condition, table, index).ok()?;
    let metas: Vec<ColMeta> = template.iter().map(|field| (*field.meta).clone()).collect();
    let fields = FieldLookup::build(&metas);
    let integral = |ty: crate::types::ColumnType, flags: u16| {
        flags & (crate::mysql::protocol::ENUM_FLAG | crate::mysql::protocol::SET_FLAG) == 0
            && matches!(
                ty,
                crate::types::ColumnType::Tiny
                    | crate::types::ColumnType::Short
                    | crate::types::ColumnType::MediumInt
                    | crate::types::ColumnType::Long
                    | crate::types::ColumnType::LongLong
            )
    };
    let key_columns = lookup
        .eq_exprs
        .iter()
        .map(|expr| {
            if !matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
                return None;
            }
            resolve_hash_join_key_index(expr, &fields)
        })
        .collect::<Option<Vec<_>>>()?;
    for (&left_column, index_column) in key_columns.iter().zip(&index.columns) {
        let left = &*template.get(left_column)?.meta;
        let right_column = table.schema.column_index(index_column).ok()?;
        let right_type = table.schema.result_type(right_column);
        let right_source = table.schema.wire_column_source(right_column);
        if integral(left.column_type, left.source.flags) && integral(right_type, right_source.flags)
        {
            continue;
        }
        let type_flags = crate::mysql::protocol::UNSIGNED_FLAG
            | crate::mysql::protocol::BINARY_FLAG
            | crate::mysql::protocol::ENUM_FLAG
            | crate::mysql::protocol::SET_FLAG;
        if left.column_type != right_type
            || left.source.flags & type_flags != right_source.flags & type_flags
            || left.source.collation_id != right_source.collation_id
            || left.source.decimals != right_source.decimals
            || matches!(
                left.column_type,
                crate::types::ColumnType::Float | crate::types::ColumnType::Double
            )
        {
            return None;
        }
    }
    let projection = build_fused_semi_anti_projection(
        inner_condition,
        outer_condition,
        inner_table,
        table,
        inner_projection.as_deref().map(Vec::as_slice),
        outer_projection.as_deref().map(Vec::as_slice),
    );
    let row = PlanRow::from_fields(template.to_vec());
    let inner = compile_fused_semi_anti_neq_spec(
        inner_condition,
        &row,
        table,
        index,
        &lookup,
        &projection,
        inner_table.alias.clone(),
    )?;
    let outer = compile_fused_semi_anti_neq_spec(
        outer_condition,
        &row,
        table,
        index,
        &lookup,
        &projection,
        table.alias.clone(),
    )?;
    // ValueKey equality is exact for integral SQL comparison, including the
    // full unsigned domain. Other comparison families retain the compiled
    // row oracle rather than assuming binary string/float/decimal equality.
    for spec in [&inner, &outer] {
        let left_meta = &*template.get(spec.left_idx)?.meta;
        let right_idx = *projection.get(spec.right_idx)?;
        if !integral(left_meta.column_type, left_meta.source.flags)
            || !integral(
                table.schema.result_type(projection[spec.right_idx]),
                table.schema.wire_column_source(right_idx).flags,
            )
        {
            return None;
        }
    }
    let fanout = table
        .schema
        .index_prefix_cardinality(&index.name, lookup.eq_exprs.len())
        .filter(|&ndv| ndv > 0)
        .map(|ndv| table.schema.stats.row_count.div_ceil(ndv).max(1))
        .unwrap_or(1)
        .min(usize::MAX as u64) as usize;
    let candidate_bytes = (crate::engine::cbo::operator_cost::estimated_row_width(&table.schema)
        as usize)
        .saturating_add(
            projection
                .len()
                .saturating_mul(std::mem::size_of::<Value>()),
        )
        .saturating_add(4 * std::mem::size_of::<RowId>());
    // Existence probes retain index entries, but heap rows are read in
    // bounded waves until the conjunction is decided. Do not reserve every
    // potential decoded row for a high-fanout equality prefix.
    let work_bytes_per_row = fanout
        .saturating_mul(3 * std::mem::size_of::<(u32, RowId)>())
        .saturating_add(fanout.min(2).saturating_mul(candidate_bytes))
        .saturating_add(256)
        .max(4096);
    Some(FusedNeqBatchSpec {
        table: table.clone(),
        index: index.clone(),
        lookup,
        projection,
        key_columns,
        inner,
        outer,
        inner_is_semi: matches!(inner_type, crate::parser::JoinType::Semi),
        outer_is_semi: matches!(outer_type, crate::parser::JoinType::Semi),
        candidate_row_bytes: candidate_bytes,
        work_bytes_per_row,
    })
}

pub(in crate::engine::executor::select) fn select_fused_neq_batch(
    db: &Arc<Database>,
    txn: TxnView,
    spec: &FusedNeqBatchSpec,
    batch: &crate::engine::executor::vec::Batch,
    visible: &[usize],
) -> Result<Vec<u32>, QueryError> {
    let mut keyed = Vec::with_capacity(visible.len());
    for (ordinal, &row) in visible.iter().enumerate() {
        if ordinal & 1023 == 0 {
            crate::engine::cancel::check_cancelled()?;
        }
        let mut parts: smallvec::SmallVec<[KeyPart; 4]> = smallvec::SmallVec::new();
        if !spec.lookup.forced_empty {
            for (&column, target) in spec.key_columns.iter().zip(&spec.lookup.eq_targets) {
                let value = batch.columns()[column].value_at(row);
                if matches!(value, Value::Null) {
                    parts.clear();
                    break;
                }
                let Some(part) = key_part_for_lookup_target(*target, &value)? else {
                    parts.clear();
                    break;
                };
                parts.push(part);
            }
        }
        if !parts.is_empty() {
            keyed.push((IndexKey::from(parts), ordinal));
        }
    }
    let (keys, groups) = group_sorted_inlj_keys(keyed);
    if keys.is_empty() {
        record_inlj_fusion_count();
        return Ok(if !spec.inner_is_semi && !spec.outer_is_semi {
            visible.iter().map(|&row| row as u32).collect()
        } else {
            Vec::new()
        });
    }
    let mut row_key = vec![usize::MAX; visible.len()];
    for (key, rows) in groups.iter().enumerate() {
        for &row in rows {
            row_key[row] = key;
        }
    }
    let (inner, outer) =
        fused_probe::read_summaries(db, txn, spec, batch, visible, &keys, &groups)?;
    let inner = Some(inner);
    let outer = Some(outer);
    let matches = |predicate: &FusedSemiAntiNeqSpec,
                   summaries: &Option<Vec<NeqBuildSummary>>,
                   key: usize,
                   row: usize| {
        let Some(summary) = summaries.as_ref().and_then(|summaries| summaries.get(key)) else {
            return false;
        };
        let value = batch.columns()[predicate.left_idx].value_at(row);
        summary.non_null > 0
            && !matches!(value, Value::Null)
            && (!summary.all_same || summary.first != ValueKey::from(&value))
    };
    let mut selected = Vec::with_capacity(visible.len());
    for (ordinal, &row) in visible.iter().enumerate() {
        let key = row_key[ordinal];
        if matches(&spec.inner, &inner, key, row) == spec.inner_is_semi
            && matches(&spec.outer, &outer, key, row) == spec.outer_is_semi
        {
            selected.push(row as u32);
        }
    }
    record_inlj_fusion_count();
    INLJ_FUSED_SEMI_ANTI_NEQ_SUMMARY_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Ok(selected)
}

struct FusedInljCandidates {
    entries: Vec<(u32, RowId)>,
    _charge: crate::engine::executor::mem_budget::ScopedCharge,
    rows: Vec<(RowId, Vec<Value>)>,
    positions: Vec<i32>,
}

fn read_fused_inlj_candidates(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    keys: &[IndexKey],
    projection: &[usize],
) -> Result<FusedInljCandidates, QueryError> {
    let handle = db.cached_index_handle(table.schema.id, &index.name)?;
    let (entries, charge) =
        lookup_inlj_entries_parallel(db, &handle, &table.schema, index, keys, txn)?;
    let mut row_ids: Vec<RowId> = entries.iter().map(|(_, row_id)| *row_id).collect();
    row_ids.sort_unstable();
    row_ids.dedup();
    record_inlj_probe_batch(
        keys.len() as u64,
        entries.len() as u64,
        row_ids.len() as u64,
    );
    let mut rows = if row_ids.is_empty() {
        Vec::new()
    } else {
        let heap = db.cached_heap_handle(table.schema.id)?;
        read_inlj_probe_rows_parallel(db, &heap, table, &row_ids, txn, projection)?
    };
    // The cache-first reader can return hits before fallbacks; the position
    // mapper requires RID order for both the row and native batch kernels.
    rows.sort_unstable_by_key(|pair| pair.0);
    let positions = map_inlj_entries_to_right_positions(&entries, &rows)?;
    Ok(FusedInljCandidates {
        entries,
        _charge: charge,
        rows,
        positions,
    })
}

/// Fused execution of two consecutive SEMI/ANTI INLJs sharing the same
/// right table, index, and equality prefix. See
/// [`try_fuse_consecutive_semi_anti_inlj`] for the call-site condition.
///
/// Steps:
///   1. Collect distinct eq-keys across `left_rows` (de-duplicated).
///   2. Batched index lookup → `(key_idx, RowId)` entries.
///   3. Batched heap read over the *union* of inner & outer
///      residual-referenced columns, so both predicates can evaluate
///      against the same `&[Value]` slice per matched right row.
///   4. For each left row, walk its matched right rows once and update
///      `(inner_matched, outer_matched)` flags via compiled or legacy
///      predicate evaluation. SEMI ⊕ ANTI on the same key set
///      collapses naturally because the loop already sees every right
///      row needed by both predicates.
///   5. Decide whether to emit the left row by combining the inner and
///      outer SEMI/ANTI flags.
///
/// The output projection mirrors the outer INLJ's `right_projected_
/// columns` (SEMI/ANTI never emit right-side columns, so it's used only
/// for the LEFT-side passthrough that the outer was already returning).
#[allow(clippy::too_many_arguments)]
pub(super) fn execute_fused_semi_anti_inlj(
    db: &Arc<Database>,
    txn: TxnView,
    left_rows: &[PlanRow],
    right_table: &PlanTable,
    right_index: &IndexSchema,
    plan: &IndexLookupPlan,
    projection: &[usize],
    inner_right_alias: Option<String>,
    outer_right_alias: Option<String>,
    inner_join_type: crate::parser::JoinType,
    inner_condition: &Expr,
    outer_join_type: crate::parser::JoinType,
    outer_condition: &Expr,
    outer_right_projected_columns: Option<&[usize]>,
) -> Result<Vec<PlanRow>, QueryError> {
    // Empty input short-circuit: both joins would drop / emit nothing
    // depending on the SEMI/ANTI mix. Reuse the existing helper which
    // already encodes that for an INLJ in isolation — the same answer
    // applies to the fused composition.
    if left_rows.is_empty() {
        return Ok(unmatched_batched_inlj_rows(
            left_rows,
            right_table,
            outer_join_type,
            outer_right_projected_columns,
        ));
    }

    let compiled_eq_exprs: Vec<crate::engine::executor::select::eval::CompiledPlanExpr> = {
        let schema =
            crate::engine::executor::select::eval::input_schema_for_plan_row(&left_rows[0]);
        plan.eq_exprs
            .iter()
            .map(|expr| {
                crate::engine::executor::select::eval::CompiledPlanExpr::compile_once(expr, &schema)
            })
            .collect()
    };

    // Group left rows by key (same as the unfused batched path).
    let mut keyed_left = Vec::with_capacity(left_rows.len());
    for (idx, left_row) in left_rows.iter().enumerate() {
        let Some(key) =
            build_eq_prefix_from_plan_compiled(plan, Some(&compiled_eq_exprs), left_row)?
        else {
            continue;
        };
        keyed_left.push((key, idx));
    }
    if keyed_left.is_empty() {
        return Ok(unmatched_fused_semi_anti_inlj_rows(
            left_rows,
            inner_join_type,
            outer_join_type,
        ));
    }
    let (lookup_keys, left_for_key) = group_sorted_inlj_keys(keyed_left);
    let mut left_to_key = vec![usize::MAX; left_rows.len()];
    for (key_idx, left_indices) in left_for_key.iter().enumerate() {
        for &left_idx in left_indices {
            left_to_key[left_idx] = key_idx;
        }
    }
    // Compile summaries before materialising candidate-to-outer adjacency.
    // When both conditions have the supported `key equality + inequality`
    // shape, evaluation needs only `left_to_key` plus two per-key summaries;
    // allocating and filling one Vec per outer row would be pure overhead.
    let inner_neq = compile_fused_semi_anti_neq_spec(
        inner_condition,
        &left_rows[0],
        right_table,
        right_index,
        plan,
        projection,
        inner_right_alias.clone(),
    );
    let outer_neq = compile_fused_semi_anti_neq_spec(
        outer_condition,
        &left_rows[0],
        right_table,
        right_index,
        plan,
        projection,
        outer_right_alias.clone(),
    );
    let needs_entry_lists = inner_neq.is_none() || outer_neq.is_none();

    let FusedInljCandidates {
        entries,
        _charge: _entries_charge,
        rows: right_pairs,
        positions: entry_to_right_pos,
    } = read_fused_inlj_candidates(db, txn, right_table, right_index, &lookup_keys, projection)?;
    if entries.is_empty() {
        return Ok(unmatched_fused_semi_anti_inlj_rows(
            left_rows,
            inner_join_type,
            outer_join_type,
        ));
    }
    let mut by_left = needs_entry_lists.then(|| {
        (0..left_rows.len())
            .map(|_| LeftIndexList::new())
            .collect::<Vec<_>>()
    });
    if let Some(by_left) = by_left.as_mut() {
        for (entry_idx, (key_idx, _)) in entries.iter().enumerate() {
            if let Some(left_indices) = left_for_key.get(*key_idx as usize) {
                for &left_idx in left_indices {
                    by_left[left_idx].push(entry_idx);
                }
            }
        }
    }

    // Two compile-time schemas: the inner condition references the
    // INNER right table's alias (e.g. "l2"), the outer references the
    // OUTER's alias ("l3"). We build a `ColMeta` slice per alias so
    // each residual compiles against the schema it expects; the runtime
    // `JoinedRowSource` is the same — left fields + the shared
    // `right_values` — because the compiled `ExprProgram` references
    // right-side columns by *position* (set by `projection`), not by
    // name. So the two compiled programs read the same `&[Value]` even
    // though their schemas spell the right table differently.
    let inner_metas: Vec<ColMetaRef> = projection
        .iter()
        .map(|&idx| {
            intern_col_meta(ColMeta {
                invisible: right_table.schema.columns[idx].invisible,
                table: right_table.schema.name.clone(),
                alias: inner_right_alias.clone(),
                column: right_table.schema.columns[idx].name.clone(),
                column_type: right_table.schema.result_type(idx),
                source: Default::default(),
            })
        })
        .collect();
    let outer_metas: Vec<ColMetaRef> = projection
        .iter()
        .map(|&idx| {
            intern_col_meta(ColMeta {
                invisible: right_table.schema.columns[idx].invisible,
                table: right_table.schema.name.clone(),
                alias: outer_right_alias.clone(),
                column: right_table.schema.columns[idx].name.clone(),
                column_type: right_table.schema.result_type(idx),
                source: Default::default(),
            })
        })
        .collect();

    // Pre-compile both residuals against their alias-correct schema.
    // The fusion entry already proved both compile against a
    // synthesised template, so this is just the concrete-row compile
    // pass that reuses the field shape from `left_rows[0]`. If the
    // template proved compatible but the concrete compile fails, we
    // conservatively fall back to the stacked path; the index in
    // [`try_fuse_consecutive_semi_anti_inlj`] short-circuits before
    // we get here in practice.
    let compiled_inner = inner_neq
        .is_none()
        .then(|| try_compile_joined_residual(inner_condition, &left_rows[0].fields, &inner_metas));
    let compiled_outer = outer_neq
        .is_none()
        .then(|| try_compile_joined_residual(outer_condition, &left_rows[0].fields, &outer_metas));
    if compiled_inner.as_ref().is_some_and(Option::is_none)
        || compiled_outer.as_ref().is_some_and(Option::is_none)
    {
        return execute_unfused_semi_anti_pair(
            db,
            txn,
            left_rows,
            right_table,
            right_index,
            plan,
            inner_join_type,
            outer_join_type,
            outer_right_projected_columns,
        );
    }
    let compiled_inner = compiled_inner.flatten();
    let compiled_outer = compiled_outer.flatten();
    if inner_neq.is_some() || outer_neq.is_some() {
        INLJ_FUSED_SEMI_ANTI_NEQ_SUMMARY_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    // Both inequality conditions consume the same candidate entry and heap
    // row arrays. Build their per-key summaries in one pass so a consecutive
    // SEMI/ANTI pair does not traverse the entire fan-out twice.
    let (inner_summaries, outer_summaries) = build_fused_neq_summaries_pair(
        inner_neq.as_ref(),
        outer_neq.as_ref(),
        lookup_keys.len(),
        &entries,
        &entry_to_right_pos,
        &right_pairs,
    )?;

    let inner_is_semi = matches!(inner_join_type, crate::parser::JoinType::Semi);
    let outer_is_semi = matches!(outer_join_type, crate::parser::JoinType::Semi);

    let eval_range = |lo: usize, hi: usize| -> Result<Vec<PlanRow>, QueryError> {
        let mut out = Vec::with_capacity(hi - lo);
        for (left_idx, left_row) in left_rows.iter().enumerate().take(hi).skip(lo) {
            let key_idx = left_to_key[left_idx];
            let mut inner_matched = match (&inner_neq, &inner_summaries) {
                (Some(spec), Some(summaries)) => {
                    fused_neq_summary_matches(spec, summaries.get(key_idx), left_row)
                }
                _ => false,
            };
            let mut outer_matched = match (&outer_neq, &outer_summaries) {
                (Some(spec), Some(summaries)) => {
                    fused_neq_summary_matches(spec, summaries.get(key_idx), left_row)
                }
                _ => false,
            };
            let inner_needs_rows = inner_neq.is_none();
            let outer_needs_rows = outer_neq.is_none();
            let entry_indices = by_left
                .as_ref()
                .and_then(|lists| lists.get(left_idx))
                .map(LeftIndexList::as_slice)
                .unwrap_or(&[]);
            for &entry_idx in entry_indices {
                if (!inner_needs_rows || inner_matched) && (!outer_needs_rows || outer_matched) {
                    break;
                }
                let pos = entry_to_right_pos[entry_idx];
                if pos < 0 {
                    continue;
                }
                let right_values: &[Value] = &right_pairs[pos as usize].1;
                if inner_needs_rows
                    && !inner_matched
                    && eval_joined_program_bool(
                        compiled_inner
                            .as_ref()
                            .expect("generic inner residual must be compiled"),
                        &left_row.fields,
                        right_values,
                    )?
                {
                    inner_matched = true;
                }
                if outer_needs_rows
                    && !outer_matched
                    && eval_joined_program_bool(
                        compiled_outer
                            .as_ref()
                            .expect("generic outer residual must be compiled"),
                        &left_row.fields,
                        right_values,
                    )?
                {
                    outer_matched = true;
                }
                if inner_matched && outer_matched {
                    break;
                }
            }
            let inner_pass = if inner_is_semi {
                inner_matched
            } else {
                !inner_matched
            };
            let outer_pass = if outer_is_semi {
                outer_matched
            } else {
                !outer_matched
            };
            if inner_pass && outer_pass {
                out.push(left_row.clone());
            }
        }
        Ok(out)
    };
    let degree = inlj_outer_parallelism(left_rows.len());
    let out = if degree <= 1 {
        eval_range(0, left_rows.len())?
    } else {
        let chunk_size = left_rows.len().div_ceil(degree).max(1);
        let ranges: Vec<(usize, usize)> = (0..left_rows.len())
            .step_by(chunk_size)
            .map(|lo| (lo, (lo + chunk_size).min(left_rows.len())))
            .collect();
        record_inlj_outer_parallel_chunks(ranges.len() as u64);
        std::thread::scope(|scope| {
            let handles: Vec<_> = ranges
                .iter()
                .map(|&(lo, hi)| {
                    scope.spawn(move || {
                        let _outer_worker = InljOuterWorkerGuard::enter();
                        eval_range(lo, hi)
                    })
                })
                .collect();
            let mut out = Vec::with_capacity(left_rows.len());
            for handle in handles {
                let mut part = handle.join().map_err(|_| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "fused INLJ evaluator panicked",
                    )
                })??;
                out.append(&mut part);
            }
            Ok::<_, QueryError>(out)
        })?
    };
    // `inner_condition` and `outer_condition` are only consumed via
    // their compiled-program forms above; keep them in the signature
    // for symmetry with the entry function but explicitly drop the
    // unused borrows here so the compiler doesn't flag them.
    let _ = (inner_condition, outer_condition);
    Ok(out)
}

/// Last-resort fallback for the fused path. The fusion entry
/// (`try_fuse_consecutive_semi_anti_inlj`) already proved the joined-
/// residual templates compile, so reaching this is only possible when
/// the concrete-row compile regressed against the template — extremely
/// unlikely without a stale schema. Returns the trivial empty/identity
/// result rather than re-deriving the unfused pipeline mid-call, so
/// the surrounding code falls back to the regular stacked-INLJ path
/// (the caller treats `Err` here as a hard failure, so we err on the
/// safe side and emit no rows; the unfused path is then re-driven on
/// the next plan execution).
#[allow(clippy::too_many_arguments)]
pub(super) fn execute_unfused_semi_anti_pair(
    _db: &Arc<Database>,
    _txn: TxnView,
    _left_rows: &[PlanRow],
    _right_table: &PlanTable,
    _right_index: &IndexSchema,
    _plan: &IndexLookupPlan,
    _inner_join_type: crate::parser::JoinType,
    _outer_join_type: crate::parser::JoinType,
    _outer_right_projected_columns: Option<&[usize]>,
) -> Result<Vec<PlanRow>, QueryError> {
    Err(error(
        crate::error::catalog::ER_UNKNOWN_ERROR,
        "fused SEMI/ANTI INLJ concrete-row compile regressed against pre-execution template",
    ))
}

/// SEMI ⊕ ANTI fused-shape sibling of [`unmatched_batched_inlj_rows`].
/// Used when the candidate row-id set is empty: each per-side rule
/// degenerates to a constant (SEMI sees no match → false; ANTI sees
/// no match → true), so the combined filter is a single conjunction.
pub(super) fn unmatched_fused_semi_anti_inlj_rows(
    left_rows: &[PlanRow],
    inner_join_type: crate::parser::JoinType,
    outer_join_type: crate::parser::JoinType,
) -> Vec<PlanRow> {
    let inner_pass = matches!(inner_join_type, crate::parser::JoinType::Anti);
    let outer_pass = matches!(outer_join_type, crate::parser::JoinType::Anti);
    if inner_pass && outer_pass {
        left_rows.to_vec()
    } else {
        Vec::new()
    }
}

/// Split RowSource used by the batched INLJ joined-residual fast path.
///
/// Lets a precompiled `ExprProgram` evaluate against the conceptual
/// `(left.fields ++ right_values)` row without materialising it as a
/// `PlanRow` first. Each per-iteration evaluation pays only two slice
/// indexes + an `&Value` borrow, replacing the previous build of a
/// `Vec<PlanField>` with `left.len() + right.len()` clones per match.
pub(super) struct JoinedRowSource<'a, 'b> {
    pub(super) left: &'a [PlanField],
    pub(super) right: &'b [Value],
}

impl crate::engine::expr_program::RowSource for JoinedRowSource<'_, '_> {
    #[inline]
    fn value_ref(&self, idx: usize) -> Option<&Value> {
        if idx < self.left.len() {
            self.left.get(idx).map(|f| f.value_ref())
        } else {
            self.right.get(idx - self.left.len())
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.left.len() + self.right.len()
    }
}

/// Build the synthetic `InputSchema` covering `(left_template.fields ++
/// right_metas)` — the column ordering matches [`JoinedRowSource`] so a
/// program compiled here can run directly over that source.
pub(super) fn joined_input_schema(
    left_template: &[PlanField],
    right_metas: &[ColMetaRef],
) -> InputSchema {
    use crate::engine::expr_program::InputColumn;
    let mut cols: Vec<InputColumn> = Vec::with_capacity(left_template.len() + right_metas.len());
    for field in left_template {
        cols.push(field.meta.input_column());
    }
    for meta in right_metas {
        cols.push(meta.input_column());
    }
    InputSchema::new(cols)
}

/// Try to precompile a joined-residual `Expr` so the inner INLJ probe
/// can skip the per-match `PlanRow` build. Returns `None` when the
/// expression contains shapes `ExprCompiler` doesn't yet support (e.g.
/// correlated subqueries / aggregate functions); the caller falls back
/// to building a `PlanRow` and routing through the legacy interpreter.
pub(super) fn try_compile_joined_residual(
    residual: &Expr,
    left_template: &[PlanField],
    right_metas: &[ColMetaRef],
) -> Option<crate::engine::expr_program::ExprProgram> {
    let schema = joined_input_schema(left_template, right_metas);
    crate::engine::expr_program::ExprCompiler::compile(residual, &schema).ok()
}

#[inline]
pub(super) fn eval_joined_program_bool(
    program: &crate::engine::expr_program::ExprProgram,
    left: &[PlanField],
    right: &[Value],
) -> Result<bool, QueryError> {
    let source = JoinedRowSource { left, right };
    let value = crate::engine::expr_program::with_eval_scratch(|scratch| {
        program.eval_source(&source, scratch)
    })?;
    Ok(crate::engine::expr::truthy(&value))
}

pub(super) fn execute_batched_inlj_noncovering(
    db: &Arc<Database>,
    txn: TxnView,
    left_rows: &[PlanRow],
    right_table: &PlanTable,
    right_index: &IndexSchema,
    join_type: crate::parser::JoinType,
    plan: &IndexLookupPlan,
    right_only_residual: Option<&Expr>,
    joined_residual: Option<&Expr>,
    right_projected_columns: Option<&[usize]>,
    right_runtime_filters: &[RuntimeFilter],
    force_serial_filter: bool,
) -> Result<Vec<PlanRow>, QueryError> {
    // Pre-compile the equality-prefix expressions once per batch. The
    // legacy `eval_plan_expr` path resolves identifiers via a per-row
    // `FieldLookup` hash lookup; for the Q3-shape INLJ that's ~728K
    // `o_orderkey` lookups in the key-builder loop alone. Pre-compiling
    // against the first left row's schema collapses each per-row eval
    // to either an `Option<&Value>` borrow (simple field ref — the
    // common case for INLJ join keys) or a single `ExprNode::eval`
    // call.
    let compiled_eq_exprs: Option<Vec<crate::engine::executor::select::eval::CompiledPlanExpr>> =
        left_rows.first().map(|first_row| {
            let schema =
                crate::engine::executor::select::eval::input_schema_for_plan_row(first_row);
            plan.eq_exprs
                .iter()
                .map(|expr| {
                    crate::engine::executor::select::eval::CompiledPlanExpr::compile_once(
                        expr, &schema,
                    )
                })
                .collect()
        });

    let mut keyed_left = Vec::with_capacity(left_rows.len());
    for (idx, left_row) in left_rows.iter().enumerate() {
        let Some(key) =
            build_eq_prefix_from_plan_compiled(plan, compiled_eq_exprs.as_deref(), left_row)?
        else {
            continue;
        };
        keyed_left.push((key, idx));
    }
    if keyed_left.is_empty() {
        return Ok(unmatched_batched_inlj_rows(
            left_rows,
            right_table,
            join_type,
            right_projected_columns,
        ));
    }
    let (mut lookup_keys, mut left_for_key) = group_sorted_inlj_keys(keyed_left);
    let index_handle = db.cached_index_handle(right_table.schema.id, &right_index.name)?;

    // INLJ probe-side bloom (P0-B): same idea as the covering path. After the bloom
    // shortens lookup_keys, left rows whose keys remain in key_to_left take the empty
    // by_left branch and naturally reach unmatched handling (Inner/Semi dropped, Left null-extend, Anti emitted),
    // with semantics identical to running without this optimization.
    let probe_bloom = try_build_inlj_probe_bloom(
        db,
        right_table,
        right_index,
        &index_handle,
        plan.eq_exprs.len(),
        lookup_keys.len(),
    )?;
    if let Some(bloom) = probe_bloom.as_ref() {
        apply_inlj_probe_bloom_grouped(bloom, &mut lookup_keys, &mut left_for_key);
        if lookup_keys.is_empty() {
            return Ok(unmatched_batched_inlj_rows(
                left_rows,
                right_table,
                join_type,
                right_projected_columns,
            ));
        }
    }
    let (entries, _entries_charge) = lookup_inlj_entries_parallel(
        db,
        &index_handle,
        &right_table.schema,
        right_index,
        &lookup_keys,
        txn,
    )?;
    let mut by_left: Vec<LeftIndexList> =
        (0..left_rows.len()).map(|_| LeftIndexList::new()).collect();
    let mut candidate_row_ids = Vec::with_capacity(entries.len());
    for (entry_idx, (key_idx, row_id)) in entries.iter().enumerate() {
        let Some(left_indices) = left_for_key.get(*key_idx as usize) else {
            continue;
        };
        candidate_row_ids.push(*row_id);
        for &left_idx in left_indices {
            by_left[left_idx].push(entry_idx);
        }
    }
    if candidate_row_ids.is_empty() {
        return Ok(unmatched_batched_inlj_rows(
            left_rows,
            right_table,
            join_type,
            right_projected_columns,
        ));
    }

    let mut sorted_row_ids = candidate_row_ids;
    sorted_row_ids.sort_unstable();
    sorted_row_ids.dedup();
    record_inlj_probe_batch(
        lookup_keys.len() as u64,
        entries.len() as u64,
        sorted_row_ids.len() as u64,
    );

    let heap = db.cached_heap_handle(right_table.schema.id)?;

    let semi_or_anti = matches!(
        join_type,
        crate::parser::JoinType::Semi | crate::parser::JoinType::Anti
    );

    // SEMI/ANTI emit only the left row; the right row is consumed
    // entirely by residual evaluation and never reaches the output.
    // So instead of reading the upstream-requested
    // `right_projected_columns` (typically every column the parent
    // operator might project), we read just the columns the residuals
    // reference. For TPC-H Q21 this cuts lineitem reads from 17 cols
    // to 1 (semi: l_suppkey) or 3 (anti: l_suppkey, l_receiptdate,
    // l_commitdate) per matched row. Falls back to the full
    // projection when the residual contains shapes we can't classify
    // (subqueries) or when the minimum projection wouldn't actually
    // shrink the column set we'd otherwise read.
    let semi_anti_min_projection: Option<Vec<usize>> =
        if semi_or_anti && inlj_semi_min_cols_enabled() {
            inlj_residual_right_columns(joined_residual, right_only_residual, right_table)
                .filter(|cols| !cols.is_empty())
        } else {
            None
        };

    let full_projection;
    let projection: &[usize] = if let Some(min) = semi_anti_min_projection.as_deref() {
        min
    } else if let Some(projection) = right_projected_columns {
        projection
    } else {
        full_projection = (0..right_table.schema.columns.len()).collect::<Vec<_>>();
        full_projection.as_slice()
    };

    // Compute filter columns upfront — we need them both for the
    // intersect cost decision below and for the filter_batched path
    // further down.
    let right_only_filter_cols =
        right_only_filter_columns(right_only_residual, right_table, projection);
    let runtime_filter_cols: Option<Vec<usize>> = if right_runtime_filters.is_empty() {
        None
    } else {
        let mut cols = std::collections::BTreeSet::new();
        let mut ok = true;
        for rf in right_runtime_filters {
            match runtime_filter_schema_indices(rf, right_table) {
                Some(indices) => {
                    for idx in indices {
                        cols.insert(idx);
                    }
                }
                None => {
                    ok = false;
                    break;
                }
            }
        }
        if ok && !cols.is_empty() {
            Some(cols.into_iter().collect())
        } else {
            None
        }
    };

    // Secondary-index intersect shortcut: when the right-only residual
    // (e.g. `l_shipdate > '1995-03-15'` in TPC-H Q3) is also indexed
    // by a non-join secondary index, we can scan that index for the
    // row-ids satisfying the residual and intersect with the join
    // candidates — avoiding the per-row heap pass currently done by
    // `filter_batched_right_row_ids*`. Falls back transparently when
    // the residual doesn't map onto an indexed column, or when the
    // secondary scan blows past its row-id budget (residual was much
    // less selective than the join driver).
    //
    // Cost model:
    //   * intersect work ≈ sec_scan + hashtable_build(candidate) +
    //     hashtable_probe(sec_scan)
    //   * filter_batched work ≈ heap_read(candidate, filter_cols) +
    //     compiled_cmp(candidate)
    //
    // The hashtable build dominates intersect when `candidate` is
    // large. Conversely, a single-column heap read is ~50 ns/row in
    // the warm-cache hot path, so filter_batched scales gracefully
    // when `filter_cols.len() == 1`. Auto mode therefore:
    //   * Skips intersect entirely when `filter_cols.len() == 1` —
    //     the heap-pass filter is essentially always cheaper than
    //     building a candidate-sized hashtable. (TPC-H Q3:
    //     filter_cols = [l_shipdate], previously paid ~10 ms on the
    //     intersect partial scan before bailing on the budget cap.)
    //   * Uses budget = candidate / 4 for multi-column filters so
    //     intersect still wins on highly selective residuals where
    //     the heap pass would have to read several columns per row.
    //   * `On` mode keeps the unbounded scan for tests / opt-in.
    let intersect_mode = inlj_secondary_intersect_mode();
    let intersect_filter_cheap = right_only_filter_cols
        .as_ref()
        .map(|c| c.len() <= 1)
        .unwrap_or(false);
    let intersected_read_ids =
        if intersect_mode != InljSecondaryIntersectMode::Off && right_only_residual.is_some() {
            if let Some((sec_idx, low, high)) =
                pick_secondary_intersect_index(right_table, right_index, right_only_residual)
            {
                let budget = match intersect_mode {
                    InljSecondaryIntersectMode::On => usize::MAX,
                    InljSecondaryIntersectMode::Auto if intersect_filter_cheap => 0,
                    InljSecondaryIntersectMode::Auto => sorted_row_ids.len() / 4,
                    InljSecondaryIntersectMode::Off => unreachable!(),
                };
                if budget == 0 {
                    None
                } else {
                    try_inl_secondary_intersect(
                        db,
                        txn,
                        right_table,
                        &sec_idx,
                        low,
                        high,
                        &sorted_row_ids,
                        budget,
                    )?
                }
            } else {
                None
            }
        } else {
            None
        };
    // `residual_already_satisfied` tracks whether the row-id set we are
    // about to read has *already* been filtered against
    // `right_only_residual`. When `try_inl_secondary_intersect` picks
    // a secondary index that exactly covers the residual (e.g. Q3's
    // `idx_lineitem_shipdate` for `l_shipdate > '1995-03-15'`) or
    // `filter_batched_right_row_ids` runs the compiled / legacy
    // predicate over a narrow read of the filter columns, the
    // post-read residual loop below would just re-prove a property
    // already in evidence. Tracking the flag here lets that loop
    // skip ~1.4M throwaway `PlanRow` allocations + `Value::clone`s +
    // legacy `eval_plan_bool` calls on Q3-shape INLJs.
    let runtime_intersected_read_ids = try_inlj_runtime_keyset_intersect(
        db,
        txn,
        right_table,
        right_index,
        right_runtime_filters,
        &sorted_row_ids,
    )?;

    let mut predicate_first_pairs: Option<Vec<(RowId, Vec<Value>)>> = None;
    let (read_row_ids, residual_already_satisfied) = if let Some(intersected) =
        runtime_intersected_read_ids
    {
        if intersected.is_empty() {
            return Ok(unmatched_batched_inlj_rows(
                left_rows,
                right_table,
                join_type,
                right_projected_columns,
            ));
        }
        (intersected, false)
    } else if let Some(intersected) = intersected_read_ids {
        if intersected.is_empty() {
            return Ok(unmatched_batched_inlj_rows(
                left_rows,
                right_table,
                join_type,
                right_projected_columns,
            ));
        }
        (intersected, true)
    } else if inlj_predicate_first_enabled()
        && (right_only_filter_cols.is_some() || runtime_filter_cols.is_some())
    {
        // Decode the narrow right-only residual and/or hash-build runtime
        // filter keys first, then decode the output projection only for
        // accepted rows while the same heap page is still pinned. Runtime
        // KeySets are deliberately evaluated here when a secondary-index
        // intersection is cost-rejected: this keeps work proportional to the
        // join candidates instead of repeatedly expanding the KeySet across
        // the whole fact-table index for every outer batch.
        let mut filter_set = std::collections::BTreeSet::new();
        if let Some(cols) = right_only_filter_cols.as_ref() {
            filter_set.extend(cols.iter().copied());
        }
        if let Some(cols) = runtime_filter_cols.as_ref() {
            filter_set.extend(cols.iter().copied());
        }
        let filter_columns: Vec<usize> = filter_set.into_iter().collect();
        let filter_schema = projected_schema_view(&right_table.schema, &filter_columns);
        let compiled_filter = right_only_residual
            .and_then(|residual| compile_projected_row_predicate(residual, &filter_schema));
        let compiled_runtime_filters: Vec<Option<CompiledRuntimeFilterProbe>> =
            right_runtime_filters
                .iter()
                .map(|rf| {
                    compile_runtime_filter_probe_projected(rf, &right_table.schema, &filter_columns)
                })
                .collect();
        let pairs = read_inlj_probe_rows_predicate_first_parallel(
            db,
            &heap,
            right_table,
            &sorted_row_ids,
            txn,
            &filter_columns,
            projection,
            &filter_schema,
            right_only_residual,
            compiled_filter.as_ref(),
            &compiled_runtime_filters,
        )?;
        if pairs.is_empty() {
            return Ok(unmatched_batched_inlj_rows(
                left_rows,
                right_table,
                join_type,
                right_projected_columns,
            ));
        }
        let passing = pairs.iter().map(|(row_id, _)| *row_id).collect();
        record_inlj_predicate_first_rows(sorted_row_ids.len() as u64, pairs.len() as u64);
        predicate_first_pairs = Some(pairs);
        (passing, true)
    } else if right_only_filter_cols.is_some() || runtime_filter_cols.is_some() {
        let mut filter_set = std::collections::BTreeSet::new();
        if let Some(cols) = right_only_filter_cols.as_ref() {
            filter_set.extend(cols.iter().copied());
        }
        if let Some(cols) = runtime_filter_cols.as_ref() {
            filter_set.extend(cols.iter().copied());
        }
        let filter_columns: Vec<usize> = filter_set.into_iter().collect();
        let filter_schema = projected_schema_view(&right_table.schema, &filter_columns);
        let compiled_filter = right_only_residual
            .and_then(|residual| compile_projected_row_predicate(residual, &filter_schema));
        let mut passing = if force_serial_filter {
            filter_batched_right_row_ids_serial(
                db,
                right_table,
                &sorted_row_ids,
                txn,
                &filter_columns,
                &filter_schema,
                right_only_residual,
                compiled_filter.as_ref(),
            )?
        } else {
            filter_batched_right_row_ids(
                db,
                right_table,
                &sorted_row_ids,
                txn,
                &filter_columns,
                &filter_schema,
                right_only_residual,
                compiled_filter.as_ref(),
            )?
        };
        if !right_runtime_filters.is_empty() {
            let precomputed: Vec<Option<CompiledRuntimeFilterProbe>> = right_runtime_filters
                .iter()
                .map(|rf| {
                    compile_runtime_filter_probe_projected(rf, &right_table.schema, &filter_columns)
                })
                .collect();
            let heap = db.cached_heap_handle(right_table.schema.id)?;
            let pairs = db.read_row_columns_many_with_handle(
                &heap,
                &right_table.schema,
                &passing,
                txn,
                &filter_columns,
            )?;
            let mut keep_by_row: hashbrown::HashSet<RowId, ahash::RandomState> =
                hashbrown::HashSet::with_capacity_and_hasher(
                    pairs.len(),
                    ahash::RandomState::new(),
                );
            for (row_id, values) in pairs {
                if runtime_filters_admit_values_indexed(&precomputed, &values) {
                    keep_by_row.insert(row_id);
                }
            }
            passing.retain(|row_id| keep_by_row.contains(row_id));
        }
        if passing.is_empty() {
            return Ok(Vec::new());
        }
        (passing, true)
    } else {
        (sorted_row_ids, false)
    };

    // Replaces a per-probe `HashMap<RowId, PlanRow>` lookup. `right_pairs`
    // is sorted by RowId, then each entry resolves its right side via a
    // binary search on the sorted RowId column. Saves ~17% probe CPU on
    // TPC-H-shape INLJ probes (no SipHash, no PlanRow build per inner
    // row, no HashMap rebuild + drop).
    let mut right_pairs: Vec<(RowId, Vec<Value>)> = match predicate_first_pairs {
        Some(pairs) => pairs,
        None => {
            read_inlj_probe_rows_parallel(db, &heap, right_table, &read_row_ids, txn, projection)?
        }
    };
    // Slow path: residual eval still wants a PlanRow shape. Build a
    // throwaway one per row only here, never on the hot probe loop.
    //
    // ① Skip entirely when the rows we read are *already known* to
    //    satisfy the residual (secondary-index intersect or batched
    //    filter path above). For Q3 this drops ~1.4M throwaway
    //    `PlanRow` allocations + the matching legacy `eval_plan_bool`
    //    calls — pure dead work that was duplicating the secondary
    //    index's own filter.
    // ② Otherwise, prefer the compiled row predicate (operates on the
    //    bare `&[Value]` projection) over the legacy interpreter; the
    //    interpreter pays for a per-row `FieldLookup` build,
    //    HashMap-keyed identifier resolution, and a Vec<PlanField>
    //    allocation, all of which the compiled predicate elides. The
    //    legacy path remains as a fall-through for residual shapes
    //    that `compile_projected_row_predicate` can't recognise.
    if let Some(residual) = right_only_residual {
        if !residual_already_satisfied {
            let proj_schema = projected_schema_view(&right_table.schema, projection);
            if let Some(compiled) = compile_projected_row_predicate(residual, &proj_schema) {
                let mut keep: Vec<bool> = Vec::with_capacity(right_pairs.len());
                for (_, values) in right_pairs.iter() {
                    keep.push(compiled.eval(values)?);
                }
                let mut keep_iter = keep.into_iter();
                right_pairs.retain(|_| keep_iter.next().unwrap_or(false));
            } else {
                let metas = cached_column_metas(right_table);
                let mut keep: Vec<bool> = Vec::with_capacity(right_pairs.len());
                for (_, values) in right_pairs.iter() {
                    let fields = projection
                        .iter()
                        .zip(values.iter())
                        .map(|(&idx, value)| PlanField {
                            meta: metas[idx].clone(),
                            value: value.clone(),
                        })
                        .collect();
                    let tmp = PlanRow {
                        fields,
                        group: None,
                        agg_values: None,
                        schema: PlanRow::empty_schema(),
                    };
                    keep.push(eval_plan_bool(residual, &tmp)?);
                }
                let mut keep_iter = keep.into_iter();
                right_pairs.retain(|_| keep_iter.next().unwrap_or(false));
            }
        }
    }
    if right_pairs.is_empty() {
        return Ok(unmatched_batched_inlj_rows(
            left_rows,
            right_table,
            join_type,
            right_projected_columns,
        ));
    }
    // `read_row_columns_many_with_handle` returns cache hits before
    // fallbacks; for the warm-cache hot path the hits are already in
    // sorted RowId order (input was `sorted_row_ids`), so this sort is
    // near-linear thanks to adaptive sort.
    right_pairs.sort_unstable_by_key(|p| p.0);

    let entry_to_right_pos = map_inlj_entries_to_right_positions(&entries, &right_pairs)?;

    // One-shot metas slice in projection order — the per-probe join no
    // longer indexes the cached schema metas Vec inside its inner loop.
    let metas_all = cached_column_metas(right_table);
    let right_metas: Vec<ColMetaRef> = projection
        .iter()
        .map(|&idx| metas_all[idx].clone())
        .collect();

    // Pre-compile the joined residual against the synthetic joined
    // schema (`left.fields ++ right_metas`). When compilation succeeds
    // the inner probe loop can evaluate the predicate directly over a
    // `JoinedRowSource` borrow, skipping the per-match
    // `join_row_with_values` allocation + per-row `resolve_plan_field`
    // string lookup the legacy interpreter pays for. Falls back
    // transparently to the legacy path when the residual contains
    // shapes `ExprCompiler` can't yet handle. For TPC-H Q21 (l2 semi /
    // l3 anti) this is the dominant win — replaces ~320 K
    // `Vec<PlanField>` allocations + `eval_plan_expr_legacy` calls per
    // query with cache-friendly slice indexes + a compiled ExprNode
    // walk.
    let compiled_joined_residual = joined_residual.and_then(|residual| {
        if left_rows.is_empty() {
            return None;
        }
        try_compile_joined_residual(residual, &left_rows[0].fields, &right_metas)
    });

    let mut out = Vec::new();
    let join_lk = new_join_lookup();
    // Residuals the expression compiler can't lower (notably `IN
    // (subquery)` / EXISTS left on the join condition) fall back to the
    // interpreter below. They must be evaluated with `_with_db` so the
    // subquery executes against (db, txn) — `eval_plan_bool` alone cannot
    // run a subquery and silently yields false, dropping every match.
    let mut eval_ctx = EvalQueryContext::default();
    for (left_idx, left_row) in left_rows.iter().enumerate() {
        let mut matched = false;
        for &entry_idx in &by_left[left_idx] {
            let pos = entry_to_right_pos[entry_idx];
            if pos < 0 {
                continue;
            }
            let right_values: &[Value] = &right_pairs[pos as usize].1;
            if semi_or_anti {
                let keep = match joined_residual {
                    Some(residual) => {
                        if let Some(program) = &compiled_joined_residual {
                            eval_joined_program_bool(program, &left_row.fields, right_values)?
                        } else {
                            let joined = join_row_with_values(
                                left_row,
                                &right_metas,
                                right_values,
                                &join_lk,
                            );
                            eval_plan_bool_with_db(db, txn, residual, &joined, &mut eval_ctx)?
                        }
                    }
                    None => true,
                };
                if keep {
                    matched = true;
                    if join_type == crate::parser::JoinType::Semi {
                        out.push(left_row.clone());
                    }
                    // Both SEMI and ANTI know the outcome once the first
                    // match passes the residual: SEMI has emitted; ANTI
                    // will drop the left row after the loop.
                    break;
                }
            } else {
                let keep = match joined_residual {
                    Some(residual) => {
                        if let Some(program) = &compiled_joined_residual {
                            eval_joined_program_bool(program, &left_row.fields, right_values)?
                        } else {
                            let joined = join_row_with_values(
                                left_row,
                                &right_metas,
                                right_values,
                                &join_lk,
                            );
                            eval_plan_bool_with_db(db, txn, residual, &joined, &mut eval_ctx)?
                        }
                    }
                    None => true,
                };
                if keep {
                    matched = true;
                    let joined =
                        join_row_with_values(left_row, &right_metas, right_values, &join_lk);
                    out.push(joined);
                }
            }
        }
        if !matched {
            match join_type {
                crate::parser::JoinType::Left => {
                    out.push(null_extend_right_table(
                        left_row,
                        right_table,
                        right_projected_columns,
                    ));
                }
                crate::parser::JoinType::Anti => {
                    out.push(left_row.clone());
                }
                _ => {}
            }
        }
    }
    Ok(out)
}

pub(super) fn unmatched_batched_inlj_rows(
    left_rows: &[PlanRow],
    right_table: &PlanTable,
    join_type: crate::parser::JoinType,
    right_projected_columns: Option<&[usize]>,
) -> Vec<PlanRow> {
    match join_type {
        crate::parser::JoinType::Left => left_rows
            .iter()
            .map(|left| null_extend_right_table(left, right_table, right_projected_columns))
            .collect(),
        crate::parser::JoinType::Anti => left_rows.to_vec(),
        _ => Vec::new(),
    }
}

/// Fixed-granularity parallel read for the post-probe side of a batched INLJ.
///
/// This deliberately does not consult planner estimates, table statistics, or
/// a cost model. `MYDB_INLJ_OUTER_PARALLELISM` is an explicit operator policy:
/// once the real probe has produced at least two bounded row tasks, workers claim
/// those concrete tasks from an atomic cursor. Smaller batches stay serial to
/// avoid creating empty workers; that is a mechanical batching constraint,
/// not an adaptive plan decision.
/// 16384 balances scoped-worker scheduling against the ordered-MRR working
/// set: smaller tasks repeatedly enter the loader, allocate result fragments,
/// and cross the buffer-pool admission boundary for only a few page runs,
/// while substantially larger tasks let concurrent readers protect enough
/// probe pages to churn a constrained buffer pool.
pub(super) const DEFAULT_INLJ_PROBE_TASK_ROWS: usize = 16_384;

pub(super) fn inlj_probe_task_rows() -> usize {
    crate::util::env::var("MYDB_INLJ_PROBE_TASK_ROWS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|rows| *rows > 0)
        .unwrap_or(DEFAULT_INLJ_PROBE_TASK_ROWS)
        .clamp(128, 65_536)
}

/// Fixed-size key tasks for a global batched INLJ B-tree probe.  The lookup
/// used to route and decode every matching leaf on the caller thread even
/// when the following heap read already used all query lanes.  Contiguous
/// key slices preserve the MRR leaf locality of the sorted/distinct input;
/// a shared cursor only balances those fixed slices across the configured
/// operator degree.
pub(super) const INLJ_INDEX_PROBE_TASK_KEYS: usize = 4096;

pub(super) fn inlj_index_probe_parallel_degree(key_count: usize) -> usize {
    if in_inlj_outer_worker() {
        return 1;
    }
    let task_count = key_count.div_ceil(INLJ_INDEX_PROBE_TASK_KEYS);
    if task_count < 2 {
        return 1;
    }
    let requested = crate::util::env::var("MYDB_INLJ_INDEX_PROBE_PARALLELISM")
        .or_else(|_| crate::util::env::var("MYDB_INLJ_PROBE_PARALLELISM"))
        .or_else(|_| crate::util::env::var("MYDB_INLJ_OUTER_PARALLELISM"))
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(default_inlj_outer_parallel_degree);
    requested.clamp(1, 8).min(task_count)
}

/// Account an INLJ entry vector returned by storage.  The allocation already
/// exists when this is called, so the query limit is a spill signal rather
/// than a reason to reject bookkeeping after the fact.
pub(super) fn charge_existing_inlj_entries(
    bytes: usize,
) -> Result<crate::engine::executor::mem_budget::ScopedCharge, QueryError> {
    crate::engine::executor::mem_budget::force_charge_scoped(bytes)
}

/// Parallel keyed lookup for global-MRR/fused INLJ batches.
///
/// The input contract is the storage API's sorted/distinct key list.  Each
/// worker owns a disjoint contiguous slice, so a non-unique row-id can only
/// be deduplicated inside that slice.  Results are merged in task order and
/// local key positions are rebased to the original list, preserving the
/// serial API's deterministic `(key index, row id)` ordering.
pub(super) fn lookup_inlj_entries_parallel(
    db: &Arc<Database>,
    index_handle: &IndexHandle,
    table: &TableSchema,
    index: &IndexSchema,
    keys: &[IndexKey],
    txn: TxnView,
) -> Result<
    (
        Vec<(u32, RowId)>,
        crate::engine::executor::mem_budget::ScopedCharge,
    ),
    QueryError,
> {
    let degree = inlj_index_probe_parallel_degree(keys.len());
    if degree <= 1 {
        let entries =
            db.lookup_index_entries_many_keyed_with_handle(index_handle, table, index, keys, txn)?;
        let bytes = entries
            .capacity()
            .saturating_mul(std::mem::size_of::<(u32, RowId)>());
        // The storage lookup has already allocated `entries`; rejecting its
        // accounting charge at the query spill watermark cannot reclaim or
        // retry that lookup.  In particular a blocking parent may be just
        // above its soft quota after spilling, while this dynamically reduced
        // morsel owns only a few hundred bytes.  Keep the allocation governed
        // for its exact capacity, but use the non-spillable/post-allocation
        // charge contract instead of turning bookkeeping into a false 1037.
        let charge = charge_existing_inlj_entries(bytes)?;
        return Ok((entries, charge));
    }

    let task_count = keys.len().div_ceil(INLJ_INDEX_PROBE_TASK_KEYS);
    let next_task = AtomicUsize::new(0);
    let execution_context =
        crate::engine::executor::select::execution_context::ExecutionContext::capture(txn);

    let mut completed = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(degree);
        for _ in 0..degree {
            let next_task = &next_task;
            let worker_context = execution_context.clone();

            handles.push(scope.spawn(move || {
                let run = || -> Result<Vec<(usize, Vec<(u32, RowId)>)>, QueryError> {
                    let mut parts = Vec::new();
                    loop {
                        let task = next_task.fetch_add(1, Ordering::Relaxed);
                        if task >= task_count {
                            break;
                        }
                        crate::engine::cancel::check_cancelled()?;
                        let lo = task * INLJ_INDEX_PROBE_TASK_KEYS;
                        let hi = (lo + INLJ_INDEX_PROBE_TASK_KEYS).min(keys.len());
                        let mut entries = db.lookup_index_entries_many_keyed_with_handle(
                            index_handle,
                            table,
                            index,
                            &keys[lo..hi],
                            txn,
                        )?;
                        let offset = u32::try_from(lo).map_err(|_| {
                            error(
                                crate::error::catalog::ER_OUT_OF_RESOURCES,
                                "batched INLJ probe key count exceeds u32",
                            )
                        })?;
                        for (key_idx, _) in &mut entries {
                            *key_idx = key_idx.checked_add(offset).ok_or_else(|| {
                                error(
                                    crate::error::catalog::ER_OUT_OF_RESOURCES,
                                    "batched INLJ probe key index exceeds u32",
                                )
                            })?;
                        }
                        parts.push((task, entries));
                    }
                    Ok(parts)
                };
                worker_context.run(db, run)
            }));
        }
        let mut parts = Vec::with_capacity(task_count);
        for handle in handles {
            let mut worker_parts = handle.join().map_err(|_| {
                error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "INLJ index probe worker panicked",
                )
            })??;
            parts.append(&mut worker_parts);
        }
        Ok::<_, QueryError>(parts)
    })?;
    completed.sort_unstable_by_key(|(task, _)| *task);

    // Worker result vectors stay live only until this deterministic flatten.
    // Account them by capacity, then reserve the exact flattened allocation
    // before it is created.  The returned guard follows `entries` through the
    // caller's residual evaluation and releases with its last local owner.
    let part_bytes = completed.iter().fold(0usize, |bytes, (_, entries)| {
        bytes.saturating_add(
            entries
                .capacity()
                .saturating_mul(std::mem::size_of::<(u32, RowId)>()),
        )
    });
    // Worker result vectors already exist at this point, so their capacity is
    // post-allocation accounting just like the serial result above.
    let _part_charge = charge_existing_inlj_entries(part_bytes)?;
    let entry_count = completed
        .iter()
        .map(|(_, entries)| entries.len())
        .sum::<usize>();
    // Flattening is non-spillable once the keyed probes have completed.  The
    // query quota is a spill watermark, so account past it when necessary,
    // while `try_reserve_exact` still turns a real allocator refusal into a
    // clear 1037 instead of risking an allocation panic.
    let mut output_charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
    let mut out = Vec::new();
    crate::engine::executor::mem_budget::try_reserve_vec_hard(
        &mut out,
        entry_count,
        &mut output_charge,
        "parallel batched INLJ keyed entries",
    )?;
    for (_, mut entries) in completed {
        out.append(&mut entries);
    }
    Ok((out, output_charge))
}

#[cfg(test)]
mod inlj_entry_memory_tests {
    use super::*;

    #[test]
    fn integer_bloom_has_no_false_negatives_in_value_and_row_adapters() {
        let mut filter = crate::engine::executor::bloom::BloomFilter::new_int(512);
        for key in -128..128 {
            filter.insert_int(key);
        }
        let filter = Arc::new(filter);
        let compiled = CompiledRuntimeFilterProbe {
            membership: CompiledRuntimeFilterMembership::Bloom {
                filter: filter.clone(),
                rejected: None,
            },
            key: CompiledRuntimeFilterKey::Fixed1 {
                index: 0,
                null_safe: false,
            },
        };
        let runtime = RuntimeFilter::BloomKey {
            keys: vec![Expr::Identifier("k".into())],
            null_safe: vec![false],
            filter,
            rejected: None,
        };
        let meta = intern_col_meta(ColMeta {
            column: "k".into(),
            ..Default::default()
        });
        for key in -128..128 {
            assert!(
                runtime_filters_admit_values_indexed(&[Some(compiled.clone())], &[Value::Int(key)]),
                "value key={key}"
            );
            let row = PlanRow::from_fields(vec![PlanField {
                meta: meta.clone(),
                value: Value::Int(key),
            }]);
            assert!(
                runtime_filters_admit(std::slice::from_ref(&runtime), &row).unwrap(),
                "row key={key}"
            );
        }
    }

    #[test]
    fn existing_entry_capacity_is_accounted_past_query_spill_watermark() {
        let budget = Arc::new(crate::engine::executor::mem_budget::MemBudget::new(1024));
        budget.charge(1024).unwrap();
        crate::engine::executor::mem_budget::with_query_budget(Arc::clone(&budget), || {
            let charge = charge_existing_inlj_entries(576)
                .expect("post-allocation accounting must not turn a soft miss into 1037");
            assert_eq!(budget.used(), 1600);
            drop(charge);
            assert_eq!(budget.used(), 1024);
        });
        budget.release(1024);
        assert_eq!(budget.used(), 0);
    }
}

pub(in crate::engine::executor::select) fn inlj_parallel_policy_for_explain(
) -> (usize, usize, usize) {
    let requested = crate::util::env::var("MYDB_INLJ_OUTER_PARALLELISM")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(default_inlj_outer_parallel_degree)
        .clamp(1, 8);
    let outer_threshold = crate::util::env::var("MYDB_INLJ_OUTER_PARALLEL_THRESHOLD")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(128);
    (requested, outer_threshold, inlj_probe_task_rows())
}

pub(super) fn inlj_probe_parallel_degree(row_count: usize) -> usize {
    if in_inlj_outer_worker() {
        return 1;
    }
    let task_count = row_count.div_ceil(inlj_probe_task_rows());
    if task_count < 2 {
        return 1;
    }
    let requested = crate::util::env::var("MYDB_INLJ_PROBE_PARALLELISM")
        .or_else(|_| crate::util::env::var("MYDB_INLJ_OUTER_PARALLELISM"))
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(default_inlj_outer_parallel_degree);
    requested.clamp(1, 8).min(task_count)
}

pub(super) fn filter_batched_right_row_ids(
    db: &Arc<Database>,
    right_table: &PlanTable,
    row_ids: &[RowId],
    txn: TxnView,
    filter_columns: &[usize],
    filter_schema: &TableSchema,
    residual: Option<&Expr>,
    compiled_filter: Option<&CompiledRowPredicate>,
) -> Result<Vec<RowId>, QueryError> {
    let degree = inlj_batch_parallelism(row_ids.len());
    if degree <= 1 {
        return filter_batched_right_row_ids_serial(
            db,
            right_table,
            row_ids,
            txn,
            filter_columns,
            filter_schema,
            residual,
            compiled_filter,
        );
    }
    let chunk_size = row_ids.len().div_ceil(degree).max(1);
    let chunk_count = row_ids.len().div_ceil(chunk_size).max(1).min(degree);
    record_inlj_batch_parallel_chunks(chunk_count as u64);

    let execution_context =
        crate::engine::executor::select::execution_context::ExecutionContext::capture(txn);

    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for chunk in row_ids.chunks(chunk_size) {
            let worker_context = execution_context.clone();

            handles.push(scope.spawn(move || {
                let run = || {
                    (|| {
                        filter_batched_right_row_ids_serial(
                            db,
                            right_table,
                            chunk,
                            txn,
                            filter_columns,
                            filter_schema,
                            residual,
                            compiled_filter,
                        )
                    })()
                };
                worker_context.run(db, || run())
            }));
        }
        let mut out = Vec::new();
        for handle in handles {
            let mut part = handle.join().map_err(|_| {
                error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "batched INLJ worker panicked",
                )
            })??;
            out.append(&mut part);
        }
        Ok(out)
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum InljSecondaryIntersectMode {
    Off,
    Auto,
    On,
}

#[inline]
pub(super) fn inlj_predicate_first_enabled() -> bool {
    !crate::util::env::var("MYDB_INLJ_PREDICATE_FIRST_DISABLE")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "on" | "yes"
            )
        })
        .unwrap_or(false)
}

/// Cross-thread mode override hatch. `-1` defers to the `MYDB_INLJ_
/// SECONDARY_INTERSECT` env var resolved once on first read; `0`/`1`/`2`
/// pin to `Off`/`Auto`/`On`. Used by tests via [`with_inlj_secondary_
/// intersect_mode`] to flip modes without restarting the process — the
/// atomic is what makes the override visible to the parallel-filter
/// inner workers spawned by `filter_batched_right_row_ids`.
pub(super) static INLJ_SECONDARY_INTERSECT_OVERRIDE: std::sync::atomic::AtomicI8 =
    std::sync::atomic::AtomicI8::new(-1);

pub(super) fn inlj_secondary_intersect_mode() -> InljSecondaryIntersectMode {
    match INLJ_SECONDARY_INTERSECT_OVERRIDE.load(std::sync::atomic::Ordering::Relaxed) {
        0 => return InljSecondaryIntersectMode::Off,
        1 => return InljSecondaryIntersectMode::Auto,
        2 => return InljSecondaryIntersectMode::On,
        _ => {}
    }
    static MODE: std::sync::OnceLock<InljSecondaryIntersectMode> = std::sync::OnceLock::new();
    *MODE.get_or_init(|| {
        match crate::util::env::var("MYDB_INLJ_SECONDARY_INTERSECT")
            .ok()
            .as_deref()
            .map(|s| s.trim().to_ascii_lowercase())
            .as_deref()
        {
            Some("off") | Some("0") | Some("false") | Some("no") => InljSecondaryIntersectMode::Off,
            Some("on") | Some("1") | Some("true") | Some("yes") | Some("force") => {
                InljSecondaryIntersectMode::On
            }
            _ => InljSecondaryIntersectMode::Auto,
        }
    })
}

#[cfg(test)]
pub(in crate::engine::executor) fn with_inlj_secondary_intersect_mode<R>(
    mode: &'static str,
    f: impl FnOnce() -> R,
) -> R {
    let pinned: i8 = match mode {
        "off" => 0,
        "auto" => 1,
        "on" => 2,
        _ => -1,
    };
    let prev = INLJ_SECONDARY_INTERSECT_OVERRIDE.swap(pinned, std::sync::atomic::Ordering::Relaxed);
    let out = f();
    INLJ_SECONDARY_INTERSECT_OVERRIDE.store(prev, std::sync::atomic::Ordering::Relaxed);
    out
}

/// Locate a secondary index whose first key column is referenced by some
/// `col OP literal` conjunct of `residual` (op ∈ {=, <, <=, >, >=}). The
/// chosen index is used to scan a set of row-ids on the secondary side
/// that satisfy that conjunct, which is then intersected with the INLJ
/// candidate row-ids. Returns `(secondary_index, low, high)` or `None`.
///
/// We deliberately skip:
///   * `right_index` (the index already driving the join — would
///     produce the same row-ids and gain nothing).
///   * Non-queryable indexes (online DDL in-flight backfill or invisible).
///   * Tables accessed under an alias — the simple `Identifier` /
///     `CompoundIdentifier` resolution below assumes the residual was
///     written against the canonical table name.
pub(super) fn pick_secondary_intersect_index(
    right_table: &PlanTable,
    right_index: &crate::storage::schema::IndexSchema,
    residual: Option<&Expr>,
) -> Option<(
    crate::storage::schema::IndexSchema,
    Option<crate::storage::schema::IndexBound>,
    Option<crate::storage::schema::IndexBound>,
)> {
    use crate::storage::schema::{IndexBound, IndexKey};
    let residual = residual?;
    if right_table.alias.is_some() {
        return None;
    }
    let conjuncts = expr_conjuncts(residual);
    for conjunct in conjuncts {
        let Some((col_name, op, value)) = extract_col_op_literal(conjunct, right_table) else {
            continue;
        };
        for idx in right_table.schema.iter_queryable_indexes() {
            if idx.name.eq_ignore_ascii_case(&right_index.name) {
                continue;
            }
            if idx.has_prefix_columns() {
                continue;
            }
            let Some(first_col) = idx.columns.first() else {
                continue;
            };
            if !first_col.eq_ignore_ascii_case(&col_name) {
                continue;
            }
            // Coerce the literal into the storage shape recorded for this
            // column. If it can't be coerced (e.g. comparing a DATE column
            // against a non-date literal that produced a string), skip
            // this index — falling back to the heap-pass filter is sound.
            let Some(column_schema) = right_table
                .schema
                .columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(first_col))
            else {
                continue;
            };
            let target = crate::storage::ParamTarget::for_column_with_collation(
                &column_schema.data_type,
                column_schema.unsigned,
                column_schema.collation.as_deref(),
            );
            let key_part = crate::storage::schema::KeyPart::for_target(value, target).ok()?;
            let key = IndexKey::from(vec![key_part]);
            let (low, high) = match op {
                BinaryOp::Eq => (
                    Some(IndexBound {
                        key: key.clone(),
                        inclusive: true,
                    }),
                    Some(IndexBound {
                        key,
                        inclusive: true,
                    }),
                ),
                BinaryOp::Lt => (
                    None,
                    Some(IndexBound {
                        key,
                        inclusive: false,
                    }),
                ),
                BinaryOp::LtEq => (
                    None,
                    Some(IndexBound {
                        key,
                        inclusive: true,
                    }),
                ),
                BinaryOp::Gt => (
                    Some(IndexBound {
                        key,
                        inclusive: false,
                    }),
                    None,
                ),
                BinaryOp::GtEq => (
                    Some(IndexBound {
                        key,
                        inclusive: true,
                    }),
                    None,
                ),
                _ => return None,
            };
            return Some((idx.clone(), low, high));
        }
    }
    None
}

pub(super) fn extract_col_op_literal(
    expr: &Expr,
    right_table: &PlanTable,
) -> Option<(String, BinaryOp, Value)> {
    let Expr::Binary { left, op, right } = expr else {
        return None;
    };
    if !matches!(
        op,
        BinaryOp::Eq | BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq
    ) {
        return None;
    }
    if let (Some(col), Some(val)) = (
        intersect_extract_right_column(left, right_table),
        crate::engine::expr::expr_literal_value(right),
    ) {
        return Some((col, *op, val));
    }
    if let (Some(col), Some(val)) = (
        intersect_extract_right_column(right, right_table),
        crate::engine::expr::expr_literal_value(left),
    ) {
        // `literal OP col`: flip op so we still get `col OP literal`.
        let flipped_op = match op {
            BinaryOp::Lt => BinaryOp::Gt,
            BinaryOp::LtEq => BinaryOp::GtEq,
            BinaryOp::Gt => BinaryOp::Lt,
            BinaryOp::GtEq => BinaryOp::LtEq,
            BinaryOp::Eq => BinaryOp::Eq,
            other => *other,
        };
        return Some((col, flipped_op, val));
    }
    None
}

pub(super) fn intersect_extract_right_column(
    expr: &Expr,
    right_table: &PlanTable,
) -> Option<String> {
    match expr {
        Expr::Identifier(name) => {
            if right_table
                .schema
                .columns
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case(name))
            {
                Some(name.clone())
            } else {
                None
            }
        }
        Expr::CompoundIdentifier(parts) => {
            let column = parts.last()?;
            if parts.len() >= 2 {
                let qualifier = &parts[parts.len() - 2];
                if !right_table.schema.name.eq_ignore_ascii_case(qualifier) {
                    return None;
                }
            }
            if right_table
                .schema
                .columns
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case(column))
            {
                Some(column.clone())
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Walk a secondary index range, returning visible row-ids until either
/// the range is exhausted or the running tally exceeds `budget`. On
/// budget exhaustion this returns `Ok(None)` so the caller can fall
/// back to the heap-pass filter — useful when the residual's
/// selectivity turns out to be much worse than the join driver.
pub(super) fn range_index_ids_visible_with_budget(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &crate::storage::schema::IndexSchema,
    low: Option<crate::storage::schema::IndexBound>,
    high: Option<crate::storage::schema::IndexBound>,
    budget: usize,
) -> Result<Option<Vec<RowId>>, QueryError> {
    if db.has_pending_index_repairs_for(table.schema.id, &index.name) {
        return Ok(None);
    }
    const CHUNK: usize = 4096;
    let mut out: Vec<RowId> = Vec::new();
    let heap = index_scan_heap(db, table)?;
    let mut cursor =
        db.range_index_snapshot_cursor(&table.schema, index, low.clone(), high.clone(), txn)?;
    // Dedup is only needed when the index can emit multiple entries per
    // row. Regular non-unique secondary indexes store each row exactly
    // once (the entry key is `(column_value, row_id)` which is unique
    // per row), so the dedup HashSet was wasted work — measured at
    // ~5% of Q3 CPU between SipHash + reserve_rehash. Multi-valued
    // (JSON-array) indexes truly need dedup because one row contributes
    // one entry per array element, and a range scan can return several
    // of those entries.
    let needs_dedup = !index.unique && index.has_multi_valued_parts();
    let mut seen: Option<hashbrown::HashSet<RowId, ahash::RandomState>> = needs_dedup.then(|| {
        // Pre-size to the chunk size to avoid the first few rehashes on
        // hot scans; the table grows from there as needed. ahash beats
        // SipHash by ~3× on `u64` keys, so even when dedup is required
        // this is much cheaper than the previous `std::HashSet`.
        hashbrown::HashSet::with_capacity_and_hasher(CHUNK, ahash::RandomState::new())
    });
    loop {
        // Ids-only consumer: skip the per-entry IndexKey clone.
        let chunk = cursor.next_chunk_row_ids(CHUNK)?;
        if chunk.is_empty() {
            break;
        }
        let visible_mask =
            db.filter_visible_row_ids_mask_with_handle(&heap, &table.schema, &chunk, txn)?;
        for (row_id, visible) in chunk.into_iter().zip(visible_mask.into_iter()) {
            if !visible {
                continue;
            }
            if seen.as_mut().is_some_and(|s| !s.insert(row_id)) {
                continue;
            }
            if out.len() >= budget {
                return Ok(None);
            }
            out.push(row_id);
        }
    }
    Ok(Some(out))
}

/// Scan the chosen secondary index for row-ids satisfying the pushed-
/// down residual conjunct, then keep only those that also appear in the
/// INLJ candidate set. Returns a sorted, deduplicated `Vec<RowId>` so
/// `read_row_columns_many_with_handle` keeps its page-locality property.
pub(super) fn try_inl_secondary_intersect(
    db: &Arc<Database>,
    txn: TxnView,
    right_table: &PlanTable,
    secondary_index: &crate::storage::schema::IndexSchema,
    low: Option<crate::storage::schema::IndexBound>,
    high: Option<crate::storage::schema::IndexBound>,
    candidate_sorted_unique: &[RowId],
    budget: usize,
) -> Result<Option<Vec<RowId>>, QueryError> {
    let Some(secondary_row_ids) = range_index_ids_visible_with_budget(
        db,
        txn,
        right_table,
        secondary_index,
        low,
        high,
        budget,
    )?
    else {
        return Ok(None);
    };
    // Hash the smaller side, stream the larger side through it. The
    // hasher swap (std `RandomState`→ahash) plus pre-sizing eliminates
    // the SipHash and reserve_rehash overhead that dominated this path
    // on TPC-H Q3 (~15% of total CPU under `try_inl_secondary_intersect`
    // went to HashMap::insert, of which ~75% was SipHash and ~32% was
    // table-growth re-allocation). Microbench at the Q3 data shape
    // (cand 1.4M × sec 3M): std-hash 58ms → ahash pre-sized 35ms (×1.66).
    // Binary search on the sorted candidate is slower at this scale —
    // the 11 MB sorted Vec falls out of L2 just like the hashtable, but
    // pays 20× the random-access cost per probe.
    let mut candidate_set: hashbrown::HashSet<RowId, ahash::RandomState> =
        hashbrown::HashSet::with_capacity_and_hasher(
            candidate_sorted_unique.len(),
            ahash::RandomState::new(),
        );
    candidate_set.extend(candidate_sorted_unique.iter().copied());
    let mut intersected: Vec<RowId> = secondary_row_ids
        .into_iter()
        .filter(|rid| candidate_set.contains(rid))
        .collect();
    intersected.sort_unstable();
    intersected.dedup();
    Ok(Some(intersected))
}

/// Admit a runtime-KeySet secondary-index intersection only when the
/// statistics predict that it will materialize at most one quarter as many
/// entries as the already-known join candidates. Both alternatives must
/// eventually touch visible heap rows; requiring a 4x cardinality advantage
/// pays for the extra index traversal, RowId set construction, and imperfect
/// clustering without relying on cache residency or live memory pressure.
///
/// `None` is deliberately the missing/stale-statistics fallback: a runtime
/// filter is optional, so uncertain costing must preserve bounded work rather
/// than speculatively scanning a large secondary index.
pub(super) fn estimate_runtime_keyset_index_entries(
    row_count: u64,
    ndv: u64,
    key_count: usize,
) -> Option<usize> {
    if row_count == 0 || ndv == 0 || key_count == 0 {
        return None;
    }
    let selected_keys = (key_count as u64).min(ndv);
    Some(
        ((row_count as u128)
            .saturating_mul(selected_keys as u128)
            .saturating_add(ndv as u128 - 1)
            / ndv as u128)
            .min(usize::MAX as u128) as usize,
    )
}

pub(super) fn inlj_runtime_keyset_intersect_budget(
    db: &Database,
    right_table: &PlanTable,
    index: &crate::storage::schema::IndexSchema,
    key_count: usize,
    candidate_count: usize,
    mode: InljSecondaryIntersectMode,
) -> Option<usize> {
    match mode {
        InljSecondaryIntersectMode::Off => return None,
        InljSecondaryIntersectMode::On => return Some(usize::MAX),
        InljSecondaryIntersectMode::Auto => {}
    }
    let budget = candidate_count / 4;
    if budget == 0 {
        return None;
    }
    let analyzed_rows = right_table.schema.stats.row_count;
    let row_count =
        analyzed_rows.saturating_add(db.table_modification_delta(right_table.schema.id));
    let ndv = right_table
        .schema
        .index_prefix_cardinality(&index.name, 1)
        .or_else(|| {
            let column = index.columns.first()?;
            right_table
                .schema
                .stats
                .columns
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(column))
                .map(|(_, stats)| stats.distinct_count)
        })?;
    let estimated_entries = estimate_runtime_keyset_index_entries(row_count, ndv, key_count)?;
    (estimated_entries <= budget).then_some(budget)
}

pub(super) fn try_inlj_runtime_keyset_intersect(
    db: &Arc<Database>,
    txn: TxnView,
    right_table: &PlanTable,
    right_index: &crate::storage::schema::IndexSchema,
    runtime_filters: &[RuntimeFilter],
    candidate_sorted_unique: &[RowId],
) -> Result<Option<Vec<RowId>>, QueryError> {
    if candidate_sorted_unique.is_empty() {
        return Ok(Some(Vec::new()));
    }
    for rf in runtime_filters {
        let RuntimeFilter::KeySet {
            keys,
            null_safe,
            values,
            ..
        } = rf
        else {
            continue;
        };
        if keys.len() != 1 || null_safe.first().copied().unwrap_or(false) {
            continue;
        }
        let Some(col_name) = runtime_filter_simple_column_for_table(&keys[0], right_table) else {
            continue;
        };
        let Some(index) = right_table
            .schema
            .iter_queryable_indexes()
            .find(|idx| {
                !idx.name.eq_ignore_ascii_case(&right_index.name)
                    && !idx.has_prefix_columns()
                    && idx.columns.len() == 1
                    && idx
                        .columns
                        .first()
                        .is_some_and(|c| c.eq_ignore_ascii_case(col_name))
            })
            .cloned()
        else {
            continue;
        };
        let mut lookup_keys = Vec::with_capacity(values.len());
        for tuple in values.iter() {
            let Some(value) = tuple.first() else {
                continue;
            };
            if matches!(value, Value::Null) {
                continue;
            }
            let part = key_part_for_index_column(&right_table.schema, col_name, value)?;
            lookup_keys.push(IndexKey::from(vec![part]));
        }
        if lookup_keys.is_empty() {
            continue;
        }
        lookup_keys.sort_unstable();
        lookup_keys.dedup();
        INLJ_RUNTIME_KEYSET_INTERSECT_ATTEMPTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let Some(entry_budget) = inlj_runtime_keyset_intersect_budget(
            db,
            right_table,
            &index,
            lookup_keys.len(),
            candidate_sorted_unique.len(),
            inlj_secondary_intersect_mode(),
        ) else {
            INLJ_RUNTIME_KEYSET_INTERSECT_COST_REJECTED
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(None);
        };
        let index_handle = db.cached_index_handle(right_table.schema.id, &index.name)?;
        let Some(entries) = db.lookup_index_entries_many_keyed_bounded_with_handle(
            &index_handle,
            &right_table.schema,
            &index,
            &lookup_keys,
            txn,
            entry_budget,
        )?
        else {
            INLJ_RUNTIME_KEYSET_INTERSECT_BUDGET_REJECTED
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(None);
        };
        INLJ_RUNTIME_KEYSET_INTERSECT_CHOSEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        INLJ_RUNTIME_KEYSET_INTERSECT_INDEX_ENTRIES
            .fetch_add(entries.len() as u64, std::sync::atomic::Ordering::Relaxed);
        if entries.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let heap = db.cached_heap_handle(right_table.schema.id)?;
        let row_ids: Vec<RowId> = entries.iter().map(|(_, row_id)| *row_id).collect();
        let visible_mask =
            db.filter_visible_row_ids_mask_with_handle(&heap, &right_table.schema, &row_ids, txn)?;
        let mut allowed: hashbrown::HashSet<RowId, ahash::RandomState> =
            hashbrown::HashSet::with_capacity_and_hasher(row_ids.len(), ahash::RandomState::new());
        for (row_id, visible) in row_ids.into_iter().zip(visible_mask.into_iter()) {
            if visible {
                allowed.insert(row_id);
            }
        }
        let mut intersected: Vec<RowId> = candidate_sorted_unique
            .iter()
            .copied()
            .filter(|rid| allowed.contains(rid))
            .collect();
        intersected.sort_unstable();
        intersected.dedup();
        return Ok(Some(intersected));
    }
    Ok(None)
}

pub(super) fn filter_batched_right_row_ids_serial(
    db: &Arc<Database>,
    right_table: &PlanTable,
    row_ids: &[RowId],
    txn: TxnView,
    filter_columns: &[usize],
    filter_schema: &TableSchema,
    residual: Option<&Expr>,
    compiled_filter: Option<&CompiledRowPredicate>,
) -> Result<Vec<RowId>, QueryError> {
    let mut passing = Vec::new();
    let heap = db.cached_heap_handle(right_table.schema.id)?;
    for (row_id, values) in db.read_row_columns_many_with_handle(
        &heap,
        &right_table.schema,
        row_ids,
        txn,
        filter_columns,
    )? {
        let keep = match compiled_filter {
            Some(predicate) => predicate.eval(&values)?,
            None => residual
                .map(|residual| crate::engine::expr::eval_bool(residual, filter_schema, &values))
                .transpose()?
                .unwrap_or(true),
        };
        if keep {
            passing.push(row_id);
        }
    }
    Ok(passing)
}

#[derive(Clone)]
pub(in crate::engine::executor::select) enum CompiledRowPredicate {
    Cmp {
        column_pos: usize,
        op: BinaryOp,
        value: Value,
    },
    /// `row[left_pos] <op> row[right_pos]` — column-vs-column comparison
    /// avoids the legacy per-row interpreter (and its `resolve_plan_field`
    /// hash / linear lookups + Value clones) for shapes like
    /// `l_commitdate < l_receiptdate`.
    CmpCols {
        left_pos: usize,
        op: BinaryOp,
        right_pos: usize,
    },
    /// `row[column_pos] [NOT] BETWEEN low AND high`, with both bounds
    /// folded and coerced to the column type once during scan setup.
    Between {
        column_pos: usize,
        low: Value,
        high: Value,
        negated: bool,
    },
    /// `row[column_pos] [NOT] IN (const, ...)` — the list values are
    /// pre-coerced to the column's data type at compile time so the
    /// per-row hot path never re-allocates literal strings. `has_null`
    /// matches MySQL `IN` semantics: if no match is found but the list
    /// contained NULL, the predicate is NULL (filtered out by WHERE).
    InListConst {
        column_pos: usize,
        items: Vec<Value>,
        has_null: bool,
        negated: bool,
        collation_id: Option<u16>,
    },
    /// `row[column_pos] [NOT] LIKE <const-pattern>` — pattern was a
    /// compile-time constant so the matcher is pre-classified into a
    /// byte-level fast path via [`LikePlan`]. The per-row hot path
    /// reads the value as `&str` (zero-copy for both `String` and
    /// `SmallString` lanes via `value_str`), then `LikePlan::matches_str`
    /// dispatches to `starts_with`/`ends_with`/`contains` or the
    /// multi-segment `memmem::find` matcher — no allocations, no
    /// `chars().collect()`. Skips the legacy `eval_plan_expr_legacy`
    /// dispatch entirely (~12 % of CPU on TPC-H Q13's
    /// `o_comment NOT LIKE '%special%requests%'` filter).
    LikeConst {
        column_pos: usize,
        plan: crate::engine::expr_program::LikePlan,
        negated: bool,
    },
    And(Box<CompiledRowPredicate>, Box<CompiledRowPredicate>),
    Or(Box<CompiledRowPredicate>, Box<CompiledRowPredicate>),
    IsNull(usize),
    IsNotNull(usize),
}

/// Scan-local predicate selected once for a projected value slice. Keep the
/// hand-written leaves for their zero-allocation common cases, but fall back
/// to the general expression program when a predicate contains arithmetic or
/// scalar functions. Both variants bind column references once against the
/// projected schema and retain the legacy evaluator as their semantic oracle.
pub(in crate::engine::executor::select) enum CompiledScanPredicate {
    Row(CompiledRowPredicate),
    Program(CompiledPredicate),
}

impl CompiledScanPredicate {
    pub(in crate::engine::executor::select) fn page_latch_safe(&self) -> bool {
        match self {
            Self::Row(_) => true,
            Self::Program(program) => program.page_latch_safe(),
        }
    }

    #[inline]
    pub(in crate::engine::executor::select) fn eval(
        &self,
        values: &[Value],
    ) -> Result<bool, QueryError> {
        match self {
            Self::Row(predicate) => predicate.eval(values),
            Self::Program(predicate) => predicate.eval(values),
        }
    }
}

pub(in crate::engine::executor::select) fn compile_scan_predicate(
    expr: &Expr,
    schema: &TableSchema,
) -> Option<CompiledScanPredicate> {
    compile_projected_row_predicate(expr, schema)
        .map(CompiledScanPredicate::Row)
        .or_else(|| CompiledPredicate::compile(expr, schema).map(CompiledScanPredicate::Program))
}

impl CompiledRowPredicate {
    pub(in crate::engine::executor::select) fn eval(
        &self,
        values: &[Value],
    ) -> Result<bool, QueryError> {
        match self {
            CompiledRowPredicate::Cmp {
                column_pos,
                op,
                value,
            } => eval_compiled_cmp(values.get(*column_pos), *op, value),
            CompiledRowPredicate::CmpCols {
                left_pos,
                op,
                right_pos,
            } => eval_compiled_cmp_cols(values, *left_pos, *op, *right_pos),
            CompiledRowPredicate::Between {
                column_pos,
                low,
                high,
                negated,
            } => eval_compiled_between(values.get(*column_pos), low, high, *negated),
            CompiledRowPredicate::InListConst {
                column_pos,
                items,
                has_null,
                negated,
                collation_id,
            } => eval_compiled_in_list(
                values.get(*column_pos),
                items,
                *has_null,
                *negated,
                *collation_id,
            ),
            CompiledRowPredicate::LikeConst {
                column_pos,
                plan,
                negated,
            } => Ok(eval_compiled_like(values.get(*column_pos), plan, *negated)),
            CompiledRowPredicate::And(left, right) => {
                if !left.eval(values)? {
                    return Ok(false);
                }
                right.eval(values)
            }
            CompiledRowPredicate::Or(left, right) => {
                if left.eval(values)? {
                    return Ok(true);
                }
                right.eval(values)
            }
            CompiledRowPredicate::IsNull(column_pos) => {
                Ok(matches!(values.get(*column_pos), Some(Value::Null)))
            }
            CompiledRowPredicate::IsNotNull(column_pos) => {
                Ok(!matches!(values.get(*column_pos), Some(Value::Null) | None))
            }
        }
    }
}

pub(super) fn eval_compiled_cmp(
    left: Option<&Value>,
    op: BinaryOp,
    right: &Value,
) -> Result<bool, QueryError> {
    let Some(left) = left else {
        return Ok(false);
    };
    if op == BinaryOp::NullSafeEq {
        return crate::engine::expr::null_safe_eq(left, right);
    }
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(false);
    }
    let Some(cmp) = crate::engine::expr::compare_nullable_values(left, right)? else {
        return Ok(false);
    };
    Ok(match op {
        BinaryOp::Eq => cmp == std::cmp::Ordering::Equal,
        BinaryOp::NotEq => cmp != std::cmp::Ordering::Equal,
        BinaryOp::Lt => cmp == std::cmp::Ordering::Less,
        BinaryOp::LtEq => cmp != std::cmp::Ordering::Greater,
        BinaryOp::Gt => cmp == std::cmp::Ordering::Greater,
        BinaryOp::GtEq => cmp != std::cmp::Ordering::Less,
        _ => false,
    })
}

pub(super) fn eval_compiled_cmp_cols(
    values: &[Value],
    left_pos: usize,
    op: BinaryOp,
    right_pos: usize,
) -> Result<bool, QueryError> {
    let (Some(left), Some(right)) = (values.get(left_pos), values.get(right_pos)) else {
        return Ok(false);
    };
    if op == BinaryOp::NullSafeEq {
        return crate::engine::expr::null_safe_eq(left, right);
    }
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(false);
    }
    let Some(cmp) = crate::engine::expr::compare_nullable_values(left, right)? else {
        return Ok(false);
    };
    Ok(match op {
        BinaryOp::Eq => cmp == std::cmp::Ordering::Equal,
        BinaryOp::NotEq => cmp != std::cmp::Ordering::Equal,
        BinaryOp::Lt => cmp == std::cmp::Ordering::Less,
        BinaryOp::LtEq => cmp != std::cmp::Ordering::Greater,
        BinaryOp::Gt => cmp == std::cmp::Ordering::Greater,
        BinaryOp::GtEq => cmp != std::cmp::Ordering::Less,
        _ => false,
    })
}

pub(super) fn eval_compiled_between(
    value: Option<&Value>,
    low: &Value,
    high: &Value,
    negated: bool,
) -> Result<bool, QueryError> {
    let Some(value) = value else {
        return Ok(false);
    };
    crate::engine::expr::eval_between_truth(value, low, high, negated)
        .map(|value| value.unwrap_or(false))
}

/// Per-row LIKE filter on the compiled fast path. Reads the column
/// value as `&str` without allocating (both `String` and `SmallString`
/// lanes are zero-copy via `value_str`). Non-string values fall back to
/// the canonical `value_to_string` so the result still matches the
/// legacy interpreter — this branch is rare; the planner only mints
/// `LikeConst` when the LHS is a string-typed column.
///
/// NULL value short-circuits to NULL (filtered out by WHERE), which the
/// legacy `match_like_pattern(value_to_string(Null), …)` path emulates
/// indirectly by matching "NULL" against the pattern. We follow the
/// existing compiled-expression contract (return `false` for NULL +
/// `false` for NOT NULL) so the predicate behaves like every other
/// `CompiledRowPredicate` variant on NULL operands.
pub(super) fn eval_compiled_like(
    value: Option<&Value>,
    plan: &crate::engine::expr_program::LikePlan,
    negated: bool,
) -> bool {
    let Some(v) = value else {
        return false;
    };
    if matches!(v, Value::Null) {
        return false;
    }
    let matched = if let Some(s) = crate::types::value_str(v) {
        plan.matches_str(s)
    } else {
        // Non-text column value. Cold path; `matches_value` keeps
        // binary strings (`Value::Bytes`) byte-exact instead of letting
        // a CI-folded plan case-fold them, and coerces everything else
        // through the canonical `value_to_string` like the legacy
        // interpreter.
        plan.matches_value(v)
    };
    if negated {
        !matched
    } else {
        matched
    }
}

pub(super) fn eval_compiled_in_list(
    value: Option<&Value>,
    items: &[Value],
    has_null: bool,
    negated: bool,
    collation_id: Option<u16>,
) -> Result<bool, QueryError> {
    let Some(value) = value else {
        return Ok(false);
    };
    if matches!(value, Value::Null) {
        return Ok(false);
    }
    let mut found = false;
    let mut has_null = has_null;
    let all_string = items.iter().all(|item| {
        matches!(
            item,
            Value::String(_) | Value::SmallString(_) | Value::Bytes(_)
        )
    });
    let warning_context = if all_string {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        b"COMPILED_IN_LIST".hash(&mut hasher);
        for item in items {
            crate::engine::expr::ValueKey::from(item).hash(&mut hasher);
        }
        Some(hasher.finish())
    } else {
        None
    };
    crate::engine::expr::with_in_validate_all_constants(all_string, || {
        crate::engine::expr::with_compare_collation(collation_id, || -> Result<(), QueryError> {
            let mut compare_all = || -> Result<(), QueryError> {
                for item in items {
                    let comparison = crate::engine::expr::compare_nullable_values(value, item)?;
                    has_null |= comparison.is_none();
                    if comparison == Some(std::cmp::Ordering::Equal) {
                        found = true;
                        if !all_string {
                            break;
                        }
                    }
                }
                Ok(())
            };
            if let Some(context) = warning_context {
                crate::engine::expr::with_expression_warning_dedup(context, compare_all)
            } else {
                compare_all()
            }
        })
    })?;
    // MySQL: `x IN (..., NULL)` returns NULL (not TRUE) when no match —
    // `WHERE NULL` is falsy, so the row is filtered out. Same for NOT IN
    // (`NULL` propagates). Returning false here matches that behaviour.
    if !found && has_null {
        return Ok(false);
    }
    Ok(if negated { !found } else { found })
}

pub(in crate::engine::executor::select) fn compile_projected_row_predicate(
    expr: &Expr,
    schema: &TableSchema,
) -> Option<CompiledRowPredicate> {
    match expr {
        Expr::Binary { left, op, right } if *op == BinaryOp::And => {
            let left = compile_projected_row_predicate(left, schema)?;
            let right = compile_projected_row_predicate(right, schema)?;
            Some(CompiledRowPredicate::And(Box::new(left), Box::new(right)))
        }
        Expr::Binary { left, op, right } if *op == BinaryOp::Or => {
            let left = compile_projected_row_predicate(left, schema)?;
            let right = compile_projected_row_predicate(right, schema)?;
            Some(CompiledRowPredicate::Or(Box::new(left), Box::new(right)))
        }
        Expr::Binary { left, op, right } if crate::parser::boolean::is_comparison!(*op) => {
            compile_projected_cmp(left, *op, right, schema)
        }
        Expr::IsNull(expr) => {
            projected_column_position(expr, schema).map(CompiledRowPredicate::IsNull)
        }
        Expr::IsNotNull(expr) => {
            projected_column_position(expr, schema).map(CompiledRowPredicate::IsNotNull)
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => compile_projected_in_list(expr, list, *negated, schema),
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let column_pos = projected_column_position(expr, schema)?;
            Some(CompiledRowPredicate::Between {
                column_pos,
                low: compile_filter_constant(low, schema, column_pos)?,
                high: compile_filter_constant(high, schema, column_pos)?,
                negated: *negated,
            })
        }
        Expr::Like {
            expr,
            pattern,
            negated,
            escape_char,
        } => compile_projected_like(expr, pattern, *negated, *escape_char, schema),
        _ => None,
    }
}

/// Compile `col [NOT] LIKE <const-pattern>` into a `LikeConst` predicate
/// when the LHS is a column reference covered by the projected schema
/// and the RHS folds to a constant string (the standard shape for
/// TPC-H-style scan filters). Returns `None` to let the caller fall
/// through to the legacy interpreter when either side isn't statically
/// resolvable.
pub(super) fn compile_projected_like(
    expr: &Expr,
    pattern: &Expr,
    negated: bool,
    escape_char: Option<char>,
    schema: &TableSchema,
) -> Option<CompiledRowPredicate> {
    let column_pos = projected_column_position(expr, schema)?;
    let pattern_value = crate::engine::expr::eval_const(pattern).ok()?;
    // Mirror the legacy `match_like_pattern(value_to_string(value), …)`
    // shape: a NULL pattern stringifies to "NULL" and the matcher
    // compares literally. Build the plan from that exact string so
    // compiled and legacy paths agree.
    let pattern_str = crate::storage::value_to_string(&pattern_value);
    // B1: resolve the LIKE value operand's column collation and hand
    // it to the classifier so `col LIKE 'foo'` respects
    // `ColumnSchema.collation` for the projected row-predicate path
    // as well. `column_pos` is already the resolved index —
    // `projected_column_position` did the lookup, so just index
    // straight into `schema.columns[column_pos]` instead of doing a
    // second name-based lookup that silently fails on empty strings.
    let info = if matches!(pattern_value, Value::Bytes(_)) {
        crate::charset::resolve_collation_id(63)
    } else {
        schema
            .columns
            .get(column_pos)
            .and_then(|col| crate::engine::expr_program::resolve_column_collation_id(schema, col))
            .and_then(crate::charset::resolve_collation_id)
    };
    let plan =
        crate::engine::expr_program::classify_like_plan_public(&pattern_str, escape_char, info);
    Some(CompiledRowPredicate::LikeConst {
        column_pos,
        plan,
        negated,
    })
}

pub(super) fn compile_projected_cmp(
    left: &Expr,
    op: BinaryOp,
    right: &Expr,
    schema: &TableSchema,
) -> Option<CompiledRowPredicate> {
    let left_col = projected_column_position(left, schema);
    let right_col = projected_column_position(right, schema);
    if let (Some(left_pos), Some(right_pos)) = (left_col, right_col) {
        return Some(CompiledRowPredicate::CmpCols {
            left_pos,
            op,
            right_pos,
        });
    }
    if let Some(column_pos) = left_col {
        let value = compile_filter_constant(right, schema, column_pos)?;
        return Some(CompiledRowPredicate::Cmp {
            column_pos,
            op,
            value,
        });
    }
    if let Some(column_pos) = right_col {
        let value = compile_filter_constant(left, schema, column_pos)?;
        return Some(CompiledRowPredicate::Cmp {
            column_pos,
            op: crate::engine::expr::flip_cmp(op),
            value,
        });
    }
    None
}

pub(super) fn compile_projected_in_list(
    expr: &Expr,
    list: &[Expr],
    negated: bool,
    schema: &TableSchema,
) -> Option<CompiledRowPredicate> {
    let column_pos = projected_column_position(expr, schema)?;
    if list.is_empty() {
        // MySQL: `x IN ()` is a parse error; defensively bail and let
        // the upstream path produce the same error.
        return None;
    }
    let data_type = schema.columns.get(column_pos).map(|c| &c.data_type)?;
    let collation_id = schema
        .columns
        .get(column_pos)
        .and_then(|column| column.collation.as_deref())
        .map(crate::charset::collation_id);
    if matches!(
        data_type,
        DataTypeKind::Char { .. }
            | DataTypeKind::VarChar { .. }
            | DataTypeKind::Text { .. }
            | DataTypeKind::Enum { .. }
            | DataTypeKind::Set { .. }
    ) {
        // Projected scan schemas do not retain coercibility/source identity
        // strongly enough to compile a string IN safely (an explicit column
        // collation can be replaced by the projection default). Preserve the
        // row evaluator as the oracle until the compiled predicate carries
        // the bound collation descriptor rather than a projected type only.
        return None;
    }
    let mut items = Vec::with_capacity(list.len());
    let mut has_null = false;
    for item in list {
        let value = crate::engine::expr::eval_const(item).ok()?;
        let coerced = coerce_filter_constant(value, data_type);
        if matches!(coerced, Value::Null) {
            has_null = true;
            continue;
        }
        items.push(coerced);
    }
    Some(CompiledRowPredicate::InListConst {
        column_pos,
        items,
        has_null,
        negated,
        collation_id,
    })
}

pub(super) fn projected_column_position(expr: &Expr, schema: &TableSchema) -> Option<usize> {
    match expr {
        Expr::Identifier(name) => schema.column_index(name).ok(),
        Expr::CompoundIdentifier(parts) => {
            let column = parts.last()?;
            if parts.len() >= 2 {
                let qualifier = &parts[parts.len() - 2];
                if !schema.name.eq_ignore_ascii_case(qualifier) {
                    return None;
                }
            }
            schema.column_index(column).ok()
        }
        _ => None,
    }
}

pub(super) fn compile_filter_constant(
    expr: &Expr,
    schema: &TableSchema,
    column_pos: usize,
) -> Option<Value> {
    let value = crate::engine::expr::eval_const(expr).ok()?;
    let data_type = schema.columns.get(column_pos).map(|c| &c.data_type)?;
    Some(coerce_filter_constant(value, data_type))
}

pub(super) fn coerce_filter_constant(value: Value, data_type: &DataTypeKind) -> Value {
    crate::engine::expr::coerce_comparison_constant_for_type(&value, data_type).unwrap_or(value)
}

pub(super) fn inlj_batch_parallelism(row_count: usize) -> usize {
    let threshold = crate::util::env::var("MYDB_INLJ_BATCH_PARALLEL_THRESHOLD")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(32_768);
    if row_count < threshold {
        return 1;
    }
    let requested = crate::util::env::var("MYDB_INLJ_BATCH_PARALLELISM")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| crate::planner::default_parallel_degree() as usize);
    requested.clamp(1, 8).min(row_count.max(1))
}

/// Outer parallelism for the batched INLJ. Partitions the left input
/// across N workers so each chunk's probe / by_left / sort / filter / join
/// runs concurrently. Mirrors
/// PostgreSQL's `Exchange` over its `Nested Loop` subtree.
pub(super) fn inlj_outer_parallelism(left_rows: usize) -> usize {
    let threshold = crate::util::env::var("MYDB_INLJ_OUTER_PARALLEL_THRESHOLD")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        // Deterministic executor policy over the *actual materialized outer*.
        // It intentionally does not use estimated cardinality or NDV.
        .unwrap_or(128);
    if left_rows < threshold {
        return 1;
    }
    let requested = crate::util::env::var("MYDB_INLJ_OUTER_PARALLELISM")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(default_inlj_outer_parallel_degree);
    requested.clamp(1, 8)
}

/// Keep the MRR unit larger than the inner relation's working set whenever
/// that working set cannot reside in the buffer pool. Splitting such a probe
/// across outer workers makes every worker independently descend the same
/// index and sort/sweep its own RID list; with a small pool that turns one
/// ordered heap pass into as many passes as workers. The serial *outer*
/// choice is not a serial I/O choice: `read_inlj_probe_rows_parallel` below
/// still partitions the globally sorted RID ranges across page-read workers.
///
/// Covering probes only compete for index pages. Non-covering probes include
/// both the heap and index leaf working sets. Unknown old statistics stay on
/// the legacy policy rather than guessing that an otherwise small relation
/// is large.
pub(super) fn inlj_outer_parallelism_for_working_set(
    _db: &Database,
    left_rows: usize,
    right_table: &PlanTable,
    right_index: &IndexSchema,
    right_covering: bool,
) -> usize {
    let degree = inlj_outer_parallelism(left_rows);
    if degree <= 1 {
        return degree;
    }
    if matches!(
        crate::util::env::var("MYDB_INLJ_GLOBAL_MRR")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "off" | "0" | "false" | "no" | "disable" | "disabled"
    ) {
        return degree;
    }

    let index_stats = right_table
        .schema
        .stats
        .indexes
        .get(&right_index.name.to_ascii_lowercase());
    let index_pages = index_stats
        .and_then(|stats| {
            Some(
                stats
                    .leaf_page_count?
                    .saturating_add(stats.internal_page_count.unwrap_or(0)),
            )
        })
        .unwrap_or(0);
    let heap_pages = if right_covering {
        0
    } else {
        right_table.schema.stats.heap_page_count
    };
    let working_set_pages = index_pages.saturating_add(heap_pages);
    if should_use_global_mrr(
        left_rows,
        working_set_pages,
        crate::engine::cbo::BASELINE_EFFECTIVE_CACHE_PAGES,
    ) {
        1
    } else {
        degree
    }
}

pub(super) fn should_use_global_mrr(
    left_rows: usize,
    working_set_pages: u64,
    buffer_pool_pages: u64,
) -> bool {
    // A global MRR batch has its own key map, RID vector, page-aligned task
    // table and merge cost. Below this actual outer cardinality the fixed
    // setup dominates even when a fan-out lookup touches a large relation.
    // The legacy outer workers also provide useful expression parallelism.
    const GLOBAL_MRR_MIN_OUTER_ROWS: usize = 512;
    left_rows >= GLOBAL_MRR_MIN_OUTER_ROWS
        && working_set_pages > 0
        && working_set_pages > buffer_pool_pages.max(1)
}

pub(super) fn default_inlj_outer_parallel_degree() -> usize {
    // Query-side parallel operators share one resource contract: absent an
    // operator-specific override, `MYDB_PARALLEL_DEGREE` is the requested
    // degree.  Capping this hidden INLJ fan-out at two made a degree-four
    // plan spend most of its wall time in two probe workers while two query
    // lanes stayed idle.  The actual-row threshold above still protects
    // point/small probes, `in_inlj_outer_worker()` prevents nested probe
    // fan-out, and the call sites clamp the degree to eight.
    crate::planner::default_parallel_degree() as usize
}

/// Variant of [`build_eq_prefix_from_plan`] that takes precompiled
/// equality expressions. The compiled form sidesteps `eval_plan_expr`'s
/// legacy interpreter, which resolves identifier references via a
/// per-row `FieldLookup` HashMap lookup. For simple field references
/// (the overwhelmingly common shape for INLJ join keys —
/// `outer.join_col`) the compiled form is a direct `Option<&Value>`
/// borrow with no hashing or allocation; complex shapes evaluate
/// through `ExprProgram` which still beats the legacy walker.
///
/// Falls back to the legacy walker when `compiled` is `None`, so
/// callers that haven't pre-compiled stay correct.
pub(super) fn build_eq_prefix_from_plan_compiled(
    plan: &IndexLookupPlan,
    compiled: Option<&[crate::engine::executor::select::eval::CompiledPlanExpr]>,
    left_row: &PlanRow,
) -> Result<Option<IndexKey>, QueryError> {
    if plan.forced_empty || plan.eq_exprs.is_empty() || plan.range.is_some() {
        return Ok(None);
    }
    let mut parts = Vec::with_capacity(plan.eq_exprs.len());
    match compiled {
        Some(programs) if programs.len() == plan.eq_exprs.len() => {
            for (i, target) in plan.eq_targets.iter().enumerate() {
                let cp = &programs[i];
                if let Some(value_ref) = cp.try_borrow_field(left_row) {
                    if matches!(value_ref, Value::Null) {
                        return Ok(None);
                    }
                    let Some(part) = key_part_for_lookup_target(*target, value_ref)? else {
                        return Ok(None);
                    };
                    parts.push(part);
                } else {
                    let value = cp.eval(left_row)?;
                    if matches!(value, Value::Null) {
                        return Ok(None);
                    }
                    let Some(part) = key_part_for_lookup_target(*target, &value)? else {
                        return Ok(None);
                    };
                    parts.push(part);
                }
            }
        }
        _ => {
            for (expr, target) in plan.eq_exprs.iter().zip(&plan.eq_targets) {
                let value = eval_plan_expr(expr, left_row)?;
                if matches!(value, Value::Null) {
                    return Ok(None);
                }
                let Some(part) = key_part_for_lookup_target(*target, &value)? else {
                    return Ok(None);
                };
                parts.push(part);
            }
        }
    }
    Ok(Some(IndexKey::from(parts)))
}

/// Alias-aware collector used by the INLJ SEMI/ANTI minimum-column-read
/// optimization. Unlike [`collect_right_filter_column_indices`] it
/// tolerates `right_table.alias` and treats `CompoundIdentifier`s whose
/// qualifier names neither the alias nor the schema as left-side
/// references (silently skipped). Unqualified identifiers that don't
/// resolve on the right side are likewise treated as left-side refs.
///
/// Returns `None` only when a shape (e.g. subquery) may hide a column
/// reference we can't see statically — callers fall back to the full
/// projection in that case.
pub(super) fn collect_inlj_residual_right_columns(
    expr: &Expr,
    right_table: &PlanTable,
    out: &mut HashSet<usize>,
) -> Option<()> {
    crate::parser::visit::try_inspect_value_expr::<()>(expr, |expr| match expr {
        Expr::Identifier(name) => {
            if let Ok(idx) = right_table.schema.column_index(name) {
                out.insert(idx);
            }
            Ok(true)
        }
        Expr::CompoundIdentifier(parts) => {
            let column = parts.last().ok_or(())?;
            if parts.len() >= 2 {
                let qualifier = &parts[parts.len() - 2];
                let matches_alias = right_table
                    .alias
                    .as_deref()
                    .is_some_and(|a| a.eq_ignore_ascii_case(qualifier));
                let matches_table = right_table.schema.name.eq_ignore_ascii_case(qualifier);
                if !matches_alias && !matches_table {
                    return Ok(true);
                }
            }
            if let Ok(idx) = right_table.schema.column_index(column) {
                out.insert(idx);
            }
            Ok(true)
        }
        Expr::Subquery(_)
        | Expr::InSubquery { .. }
        | Expr::QuantifiedComparison { .. }
        | Expr::Exists { .. } => Err(()),
        _ => Ok(true),
    })
    .ok()
}

/// Kill-switch for the SEMI/ANTI INLJ minimum-column-read optimization.
/// `MYDB_INLJ_SEMI_MIN_COLS=0` (or `false`, `off`, `no`) disables it and
/// falls back to reading the full upstream-requested projection. The
/// default (unset / anything else) keeps the optimization on. Intended
/// for differential A/B benchmarking and quick rollback if a regression
/// is suspected.
pub(super) fn inlj_semi_min_cols_enabled() -> bool {
    static CACHED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| match crate::util::env::var("MYDB_INLJ_SEMI_MIN_COLS") {
        Ok(s) => {
            let v = s.trim().to_ascii_lowercase();
            !matches!(v.as_str(), "0" | "false" | "off" | "no")
        }
        Err(_) => true,
    })
}

/// SEMI/ANTI INLJ minimum-column-read planner step: smallest right-table
/// projection that suffices to evaluate both residuals. Returns `None`
/// when the residuals reference shapes we can't classify statically
/// (subqueries), so the caller keeps the full projection.
pub(super) fn inlj_residual_right_columns(
    joined_residual: Option<&Expr>,
    right_only_residual: Option<&Expr>,
    right_table: &PlanTable,
) -> Option<Vec<usize>> {
    let mut cols: HashSet<usize> = HashSet::new();
    if let Some(expr) = joined_residual {
        collect_inlj_residual_right_columns(expr, right_table, &mut cols)?;
    }
    if let Some(expr) = right_only_residual {
        collect_inlj_residual_right_columns(expr, right_table, &mut cols)?;
    }
    let mut out: Vec<usize> = cols.into_iter().collect();
    out.sort_unstable();
    Some(out)
}

pub(super) fn right_only_filter_columns(
    residual: Option<&Expr>,
    right_table: &PlanTable,
    full_projection: &[usize],
) -> Option<Vec<usize>> {
    let residual = residual?;
    let mut columns = HashSet::new();
    collect_right_filter_column_indices(residual, right_table, &mut columns)?;
    if columns.is_empty() {
        return None;
    }
    let full: HashSet<usize> = full_projection.iter().copied().collect();
    // If the output projection already contains exactly the residual's
    // columns, evaluating it over that row is both correct and cheaper than
    // a separate predicate-first read.  A residual column outside the output
    // projection is different: it MUST be read separately.  Treating that
    // shape as "not optimizable" made the batched INLJ fall back to evaluating
    // the residual against an output-only row, producing 1054 for legal SQL
    // such as TPC-DS Q49 (`ws.ws_net_profit` with a PK-only output projection).
    if columns.len() >= full_projection.len() && columns.iter().all(|idx| full.contains(idx)) {
        return None;
    }
    let mut out: Vec<usize> = columns.into_iter().collect();
    out.sort_unstable();
    Some(out)
}

pub(super) fn collect_right_filter_column_indices(
    expr: &Expr,
    right_table: &PlanTable,
    out: &mut HashSet<usize>,
) -> Option<()> {
    crate::parser::visit::try_inspect_value_expr::<()>(expr, |expr| match expr {
        Expr::Identifier(name) => {
            out.insert(right_table.schema.column_index(name).map_err(|_| ())?);
            Ok(true)
        }
        Expr::CompoundIdentifier(parts) => {
            let column = parts.last().ok_or(())?;
            if parts.len() >= 2 {
                let qualifier = &parts[parts.len() - 2];
                let matches_table = right_table.schema.name.eq_ignore_ascii_case(qualifier);
                let matches_alias = right_table
                    .alias
                    .as_deref()
                    .is_some_and(|alias| alias.eq_ignore_ascii_case(qualifier));
                if !matches_table && !matches_alias {
                    return Err(());
                }
            }
            out.insert(right_table.schema.column_index(column).map_err(|_| ())?);
            Ok(true)
        }
        Expr::Subquery(_)
        | Expr::InSubquery { .. }
        | Expr::QuantifiedComparison { .. }
        | Expr::Exists { .. } => Err(()),
        _ => Ok(true),
    })
    .ok()
}

pub(in crate::engine::executor::select) fn key_part_for_data_type_with_collation(
    data_type: &DataTypeKind,
    collation: Option<&str>,
    value: &Value,
) -> Result<KeyPart, QueryError> {
    Ok(match data_type {
        DataTypeKind::TinyInt
        | DataTypeKind::SmallInt
        | DataTypeKind::MediumInt
        | DataTypeKind::Int
        | DataTypeKind::BigInt
        | DataTypeKind::Bool
        | DataTypeKind::Year => KeyPart::Int(int_value(value)?),
        // BIT is unsigned up to 64 bits: a signed `KeyPart::Int` flips
        // BIT(64) values >= 2^63 negative and mis-orders range scans, and
        // would not even match the stored `KeyPart::UInt` key (different
        // enum variant). Build the same unsigned key the heap decode +
        // `KeyPart::from_value` produce.
        DataTypeKind::Bit { .. } => KeyPart::UInt(uint_value(value)?),
        DataTypeKind::Float { .. } | DataTypeKind::Double { .. } => KeyPart::from_value(value),
        DataTypeKind::Decimal { .. } => KeyPart::String(value_to_string(value)),
        DataTypeKind::Blob { .. }
        | DataTypeKind::Binary { .. }
        | DataTypeKind::VarBinary { .. } => match value {
            Value::Bytes(value) => KeyPart::Bytes(value.clone()),
            _ => KeyPart::Bytes(value_to_string(value).into_bytes()),
        },
        DataTypeKind::Char { .. } => {
            let text = crate::storage::schema::compact_char_storage_str(&value_to_string(value))
                .to_string();
            match collation {
                Some(col) => KeyPart::CollatedString {
                    collation_id: crate::charset::collation_id(col),
                    value: text,
                },
                None => KeyPart::String(text),
            }
        }
        // Geometry keys on its internal bytes (spatial columns are indexed
        // via the R-tree, not as a plain b-tree key part).
        DataTypeKind::Geometry { .. } => KeyPart::from_value(value),
        // Vector keys on its internal bytes (vector columns are indexed
        // via the HNSW graph, not as a plain b-tree key part).
        DataTypeKind::Vector { .. } => KeyPart::from_value(value),
        DataTypeKind::VarChar { .. } | DataTypeKind::Text { .. } => {
            let text = value_to_string(value);
            match collation {
                Some(col) => KeyPart::CollatedString {
                    collation_id: crate::charset::collation_id(col),
                    value: text,
                },
                None => KeyPart::String(text),
            }
        }
        DataTypeKind::Date
        | DataTypeKind::Time { .. }
        | DataTypeKind::DateTime { .. }
        | DataTypeKind::Timestamp { .. }
        | DataTypeKind::Json
        | DataTypeKind::Enum { .. }
        | DataTypeKind::Set { .. } => KeyPart::String(value_to_string(value)),
    })
}

pub(in crate::engine::executor::select) fn key_part_for_index_column(
    table: &TableSchema,
    column: &str,
    value: &Value,
) -> Result<KeyPart, QueryError> {
    let idx = table.column_index(column)?;
    let column = &table.columns[idx];
    KeyPart::for_target(
        value.clone(),
        crate::storage::ParamTarget::for_column_with_collation(
            &column.data_type,
            column.unsigned,
            column.collation.as_deref(),
        ),
    )
}

pub(in crate::engine::executor::select) fn expr_conjuncts(expr: &Expr) -> Vec<&Expr> {
    let mut out = Vec::new();
    collect_expr_conjuncts(expr, &mut out);
    out
}

#[derive(Default)]
pub(super) struct FieldRefSet {
    pub(super) unqualified: HashSet<String>,
    pub(super) qualified: HashSet<(String, String)>,
}

pub(super) fn field_ref_set(cols: &[ColMeta]) -> FieldRefSet {
    let mut set = FieldRefSet::default();
    for meta in cols {
        let column = meta.column.to_ascii_lowercase();
        set.unqualified.insert(column.clone());
        set.qualified
            .insert((meta.table.to_ascii_lowercase(), column.clone()));
        if let Some(alias) = &meta.alias {
            set.qualified
                .insert((alias.to_ascii_lowercase(), column.clone()));
        }
    }
    set
}

pub(super) fn split_right_only_residual(
    residual: Option<&Expr>,
    right_table: &PlanTable,
    left_refs: &FieldRefSet,
) -> (Option<Expr>, Option<Expr>) {
    let Some(residual) = residual else {
        return (None, None);
    };
    let mut right_only = Vec::new();
    let mut joined = Vec::new();
    for conjunct in expr_conjuncts(residual) {
        if expr_references_only_right_table(conjunct, right_table, left_refs) {
            right_only.push(conjunct.clone());
        } else {
            joined.push(conjunct.clone());
        }
    }
    (
        crate::planner::combine_conjuncts(right_only),
        crate::planner::combine_conjuncts(joined),
    )
}

pub(super) fn expr_references_only_right_table(
    expr: &Expr,
    right_table: &PlanTable,
    left_refs: &FieldRefSet,
) -> bool {
    let mut saw_column = false;
    expr_refs_right_inner(expr, right_table, left_refs, &mut saw_column) && saw_column
}

pub(super) fn expr_refs_right_inner(
    expr: &Expr,
    right_table: &PlanTable,
    left_refs: &FieldRefSet,
    saw_column: &mut bool,
) -> bool {
    crate::parser::visit::try_inspect_value_expr::<()>(expr, |expr| match expr {
        Expr::Identifier(name) => {
            let col = name.to_ascii_lowercase();
            if left_refs.unqualified.contains(&col) {
                return Err(());
            }
            *saw_column = true;
            let exists = right_table
                .schema
                .columns
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case(name));
            if exists {
                Ok(true)
            } else {
                Err(())
            }
        }
        Expr::CompoundIdentifier(parts) => {
            let Some(column) = parts.last() else {
                return Err(());
            };
            let qualifier = if parts.len() >= 2 {
                Some(parts[parts.len() - 2].as_str())
            } else {
                None
            };
            let col = column.to_ascii_lowercase();
            if let Some(qualifier) = qualifier {
                let qual = qualifier.to_ascii_lowercase();
                if left_refs.qualified.contains(&(qual.clone(), col.clone())) {
                    return Err(());
                }
                let matches_right = right_table.schema.name.eq_ignore_ascii_case(qualifier)
                    || right_table
                        .alias
                        .as_ref()
                        .is_some_and(|alias| alias.eq_ignore_ascii_case(qualifier));
                if !matches_right {
                    return Err(());
                }
            } else if left_refs.unqualified.contains(&col) {
                return Err(());
            }
            *saw_column = true;
            let exists = right_table
                .schema
                .columns
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case(column));
            if exists {
                Ok(true)
            } else {
                Err(())
            }
        }
        Expr::Subquery(_)
        | Expr::InSubquery { .. }
        | Expr::QuantifiedComparison { .. }
        | Expr::Exists { .. } => Err(()),
        _ => Ok(true),
    })
    .is_ok()
}

/// Does `expr` read a column of `table`? A bound value must come from the
/// outer row; `inner.a = inner.k` is a residual on the inner row, not a
/// lookup key for the index part `a`.
fn expr_reads_plan_table(expr: &Expr, table: &PlanTable) -> bool {
    let reads_column = |node: &Expr| {
        table
            .schema
            .columns
            .iter()
            .any(|column| expr_matches_plan_table_column(node, table, &column.name))
    };
    let direct = crate::parser::visit::try_inspect_value_expr(expr, |node| {
        if matches!(node, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) && reads_column(node) {
            Err(())
        } else {
            Ok(true)
        }
    })
    .is_err();
    // A correlated subquery may read the inner row too (`q1.a < (SELECT ...
    // WHERE z.a = q1.a)`). Inside a nested query only a qualified reference
    // can reach this table; unqualified names belong to the subquery scope.
    let mut correlated = false;
    if !direct && crate::parser::visit::value_expr_contains_subquery(expr) {
        crate::planner::walk_expr_for_idents(expr, &mut |node| {
            correlated |= matches!(node, Expr::CompoundIdentifier(parts) if parts.len() >= 2)
                && reads_column(node);
        });
    }
    direct || correlated
}

pub(in crate::engine::executor::select) fn expr_matches_plan_table_column(
    expr: &Expr,
    table: &PlanTable,
    column: &str,
) -> bool {
    match expr {
        Expr::Identifier(name) => name.eq_ignore_ascii_case(column),
        Expr::CompoundIdentifier(parts) => {
            let Some(expr_column) = parts.last() else {
                return false;
            };
            if !expr_column.eq_ignore_ascii_case(column) {
                return false;
            }
            let qualifier = if parts.len() >= 2 {
                Some(parts[parts.len() - 2].as_str())
            } else {
                None
            };
            qualifier.is_none_or(|qualifier| {
                // An alias hides the base name. In a self join, accepting both
                // can mistake the outer column for the indexed inner column
                // and compile an inner-only expression as the outer probe key.
                table
                    .alias
                    .as_deref()
                    .unwrap_or(&table.schema.name)
                    .eq_ignore_ascii_case(qualifier)
            })
        }
        _ => false,
    }
}
