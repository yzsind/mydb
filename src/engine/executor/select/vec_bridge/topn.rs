//! topn execution and preparation.

use super::*;

#[derive(Clone)]
pub(super) enum TopNKeySource {
    Column(usize),
    /// Exact no-argument `RAND()`.  The program is retained for the rare
    /// row-major fallback when another volatile key requires expression
    /// interleaving; the common single-key path advances the session RNG once
    /// per visible row under a single batch-scoped lock.
    UnseededRand(Arc<ExprProgram>),
    Program {
        program: Arc<ExprProgram>,
        integer: Option<crate::engine::executor::select::plan::CompiledNumericExpr>,
    },
}

pub(super) struct TopNKeyPlan {
    pub(super) template: Vec<PlanField>,
    pub(super) sources: Vec<TopNKeySource>,
    pub(super) specs: Vec<vec_topn::SortColumnSpec>,
    pub(super) collations: Vec<Option<u16>>,
    pub(super) properties: BatchProperties,
}

pub(super) fn computed_sort_collation(expr: &Expr, schema: &BatchSchema) -> Option<u16> {
    let columns: Vec<crate::types::Column> = schema
        .fields()
        .iter()
        .map(|field| crate::types::Column {
            name: field.name.clone(),
            column_type: field.column_type,
            source: field.source.clone(),
        })
        .collect();
    let table = crate::storage::TableSchema::synthetic_for_planning("<sort input>", &columns);
    crate::engine::expr::expr_collation(expr, &table).map(|c| c.collation_id)
}

pub(super) fn scalar_order_key_preflight(
    template: Vec<PlanField>,
    order_by: &[OrderBy],
    set_operation_scope: bool,
) -> Result<Option<TopNKeyPlan>, QueryError> {
    if template.is_empty() || order_by.is_empty() {
        return Ok(None);
    }
    let schema = template_to_batch_schema(&template);
    let mut sources = Vec::with_capacity(order_by.len());
    let mut specs = Vec::with_capacity(order_by.len());
    let mut collations = Vec::with_capacity(order_by.len());
    let mut ordered_properties = Vec::with_capacity(order_by.len());
    let mut all_keys_are_columns = true;
    for (key_index, order) in order_by.iter().enumerate() {
        if set_operation_scope {
            if let Expr::CompoundIdentifier(parts) = &order.expr {
                let table = parts.first().map(String::as_str).unwrap_or("");
                return Err(crate::storage::error(
                    crate::error::catalog::ER_TABLENAME_NOT_ALLOWED_HERE,
                    format!(
                        "Table '{table}' from one of the SELECTs cannot be used in global ORDER BY"
                    ),
                ));
            }
            if crate::engine::executor::select::expr_has_aggregate(&order.expr) {
                return Err(crate::storage::error(
                    crate::error::catalog::ER_AGGREGATE_ORDER_FOR_UNION,
                    format!(
                        "Expression #{} of ORDER BY contains aggregate function and applies to a UNION",
                        key_index + 1
                    ),
                ));
            }
        }
        if crate::engine::executor::select::expr_has_subquery(&order.expr)
            || crate::engine::executor::select::expr_has_aggregate(&order.expr)
            || crate::engine::executor::select::expr_has_window(&order.expr)
        {
            return Ok(None);
        }
        let ordinal = if set_operation_scope {
            match &order.expr {
                Expr::Literal(crate::parser::Literal::Int(position)) if *position >= 0 => {
                    Some(*position as u64)
                }
                Expr::Literal(crate::parser::Literal::UInt(position)) => Some(*position),
                _ => None,
            }
        } else {
            None
        };
        let column = if let Some(position) = ordinal {
            if position == 0 || position as usize > template.len() {
                return Err(crate::storage::error(
                    crate::error::catalog::ER_BAD_FIELD_ERROR,
                    format!("Unknown column '{position}' in 'order clause'"),
                ));
            }
            Some(position as usize - 1)
        } else {
            expr_column_index_in_template(&order.expr, &template)
        };
        if let Some(column) = column {
            sources.push(TopNKeySource::Column(column));
            collations.push(schema.fields()[column].source.collation_id);
            ordered_properties.push(BatchSortKey {
                column_index: column,
                ascending: order.asc,
                nulls_first: order.nulls_first_effective(),
            });
        } else {
            if set_operation_scope && matches!(order.expr, Expr::Identifier(_)) {
                let Expr::Identifier(name) = &order.expr else {
                    unreachable!()
                };
                return Err(crate::storage::error(
                    crate::error::catalog::ER_BAD_FIELD_ERROR,
                    format!("Unknown column '{name}' in 'order clause'"),
                ));
            }
            let Some(program) = expr_batch::compile_for_batch(&order.expr, &schema)? else {
                return Ok(None);
            };
            if matches!(
                &order.expr,
                Expr::Function {
                    name,
                    args,
                    distinct: false,
                } if name.eq_ignore_ascii_case("RAND") && args.is_empty()
            ) {
                collations.push(computed_sort_collation(&order.expr, &schema));
                sources.push(TopNKeySource::UnseededRand(program));
                all_keys_are_columns = false;
                specs.push(vec_topn::SortColumnSpec {
                    column_index: key_index,
                    ascending: order.asc,
                    nulls_first: order.nulls_first_effective(),
                });
                continue;
            }
            let integer = crate::engine::executor::select::plan::CompiledNumericExpr::compile_with_column_resolver(
                &order.expr,
                &|expr| expr_column_index_in_template(expr, &template),
            );
            collations.push(computed_sort_collation(&order.expr, &schema));
            sources.push(TopNKeySource::Program { program, integer });
            all_keys_are_columns = false;
        }
        specs.push(vec_topn::SortColumnSpec {
            column_index: key_index,
            ascending: order.asc,
            nulls_first: order.nulls_first_effective(),
        });
    }
    Ok(Some(TopNKeyPlan {
        template,
        sources,
        specs,
        collations,
        properties: BatchProperties {
            ordering: if all_keys_are_columns {
                BatchOrdering::Ordered(ordered_properties)
            } else {
                BatchOrdering::Unknown
            },
            partitioning: BatchPartitioning::Single,
        },
    }))
}

/// Capability probes can revisit a sort many times. Bind only after its
/// native input is accepted, once for the operator that will consume rows.
fn bind_vector_order_keys_at_open(
    keys: &mut TopNKeyPlan,
    order_by: &[OrderBy],
) -> Result<(), QueryError> {
    for (source, order) in keys.sources.iter_mut().zip(order_by) {
        let TopNKeySource::Program { program, .. } = source else {
            continue;
        };
        let Some(bound) =
            crate::engine::expr_program::ExprCompiler::bind_vector_constants(&order.expr)
        else {
            continue;
        };
        let schema = template_to_batch_schema(&keys.template);
        if let Some(specialized) = expr_batch::compile_for_batch(&bound, &schema)? {
            *program = specialized;
        }
    }
    Ok(())
}

#[cfg(test)]
mod vector_order_binding_tests {
    use super::*;
    use crate::engine::vector::codec::{parse_typed_text, text_parse_calls};
    use crate::parser::Statement;
    use crate::vector::VectorDataType;

    #[test]
    fn vector_order_capability_preflight_keeps_binding_until_open() {
        let Statement::Select(select) =
            crate::parser::parse_statement("SELECT v FROM t ORDER BY L2_DISTANCE(v,TO_VECTOR(?))")
                .unwrap()
        else {
            panic!("SELECT expected")
        };
        let template = vec![PlanField::new(
            "t",
            None,
            "v",
            ColumnType::Vector,
            Value::Null,
        )];
        let vector = Value::Vector(parse_typed_text("[1,0]", VectorDataType::Float32).unwrap());
        crate::engine::params::with_params(vec![Value::String("[0,1]".into())], || {
            let before = text_parse_calls();
            for _ in 0..4 {
                assert!(
                    scalar_order_key_preflight(template.clone(), &select.order_by, false)
                        .unwrap()
                        .is_some()
                );
            }
            assert_eq!(text_parse_calls() - before, 0);
            let mut keys = scalar_order_key_preflight(template, &select.order_by, false)
                .unwrap()
                .unwrap();
            bind_vector_order_keys_at_open(&mut keys, &select.order_by).unwrap();
            assert_eq!(text_parse_calls() - before, 1);
            let schema = template_to_batch_schema(&keys.template);
            let batch = Batch::new(schema, vec![ColumnVector::from_values(vec![vector; 128])]);
            let TopNKeySource::Program { program, .. } = &keys.sources[0] else {
                panic!("program expected")
            };
            for _ in 0..3 {
                let values = expr_batch::eval_value_per_row(program, &batch).unwrap();
                assert!(matches!(values[0], Value::Double(_)));
                assert!(values.iter().all(|value| *value == values[0]));
            }
            assert_eq!(text_parse_calls() - before, 1);
        });
    }
}

/// Compile every ORDER BY key from immutable plan metadata. This must finish
/// before the upstream operator is opened: an unsupported key therefore
/// selects the row oracle without consuming and rescanning any input.
pub(super) fn topn_key_preflight(
    input: &PhysicalPlan,
    order_by: &[OrderBy],
) -> Result<Option<TopNKeyPlan>, QueryError> {
    if matches!(input, PhysicalPlan::SetOp { .. }) {
        // A chain of three or more set-operation branches is represented as a
        // nested SetOp on the left. Looking through that node with
        // `collect_plan_fields` exposes the first branch's pre-projection
        // relation schema (and, for aggregates, its pre-aggregate columns),
        // while the runtime SetOp emits the recursively unified projected
        // schema. Derive the ordering template through the same recursive
        // metadata path used by the runtime builder so both sides agree before
        // any input is opened.
        let Some(template) = preflight_query_output_template(input)? else {
            return Ok(None);
        };
        return scalar_order_key_preflight(template, order_by, true);
    }
    let aggregate_parts = hash_aggregate_node(input).and_then(PhysicalPlan::aggregate_input);
    if let Some(aggregate) = aggregate_parts {
        let (aggregate_input, group_by, having, aggregates, grouping) = (
            aggregate.input,
            aggregate.group_by,
            aggregate.having,
            aggregate.aggregates,
            aggregate.grouping,
        );
        let Some(layout) = (if grouping {
            grouping_output_preflight(aggregate_input, group_by, aggregates)
        } else {
            hash_agg_output_preflight(aggregate_input, group_by, having, aggregates)
        }) else {
            return Ok(None);
        };
        let mut sources = Vec::with_capacity(order_by.len());
        let mut specs = Vec::with_capacity(order_by.len());
        let mut collations = Vec::with_capacity(order_by.len());
        let mut ordered_properties = Vec::with_capacity(order_by.len());
        for (key_index, order) in order_by.iter().enumerate() {
            let column = if let Expr::GroupKeyRef { index, .. } = &order.expr {
                (*index < layout.group_count).then_some(*index)
            } else if let Some(column) = expr_column_index_in_template(
                &order.expr,
                &layout.output_template[..layout.group_count],
            ) {
                Some(column)
            } else if crate::engine::executor::select::expr_has_aggregate(&order.expr) {
                let key = crate::engine::executor::select::aggregate::aggregate_key(&order.expr);
                layout
                    .aggregate_keys
                    .iter()
                    .position(|candidate| *candidate == key)
                    .map(|slot| layout.group_count + slot)
            } else {
                None
            };
            if let Some(column) = column {
                sources.push(TopNKeySource::Column(column));
                collations.push(layout.output_schema.fields()[column].source.collation_id);
                ordered_properties.push(BatchSortKey {
                    column_index: column,
                    ascending: order.asc,
                    nulls_first: order.nulls_first_effective(),
                });
            } else {
                let Some(rewritten) = rewrite_hash_agg_output_expr(
                    &order.expr,
                    &layout.output_schema,
                    layout.group_count,
                    &layout.group_exprs,
                    &layout.aggregate_keys,
                ) else {
                    return Ok(None);
                };
                let Some(program) =
                    expr_batch::compile_post_aggregate(&rewritten, &layout.output_schema)?
                else {
                    return Ok(None);
                };
                if let Some(column) = program.as_direct_column() {
                    sources.push(TopNKeySource::Column(column));
                    collations.push(layout.output_schema.fields()[column].source.collation_id);
                    ordered_properties.push(BatchSortKey {
                        column_index: column,
                        ascending: order.asc,
                        nulls_first: order.nulls_first_effective(),
                    });
                } else {
                    collations.push(computed_sort_collation(&rewritten, &layout.output_schema));
                    sources.push(TopNKeySource::Program {
                        program,
                        integer: None,
                    });
                }
            }
            specs.push(vec_topn::SortColumnSpec {
                column_index: key_index,
                ascending: order.asc,
                nulls_first: order.nulls_first_effective(),
            });
        }
        return Ok(Some(TopNKeyPlan {
            template: layout.output_template,
            sources,
            specs,
            collations,
            properties: BatchProperties {
                ordering: if ordered_properties.len() == order_by.len() {
                    BatchOrdering::Ordered(ordered_properties)
                } else {
                    BatchOrdering::Unknown
                },
                partitioning: BatchPartitioning::Single,
            },
        }));
    }
    let template = plan_output_template(input);
    scalar_order_key_preflight(template, order_by, false)
}

/// Blocking Top-K state with a batch output boundary. The first pull drains
/// upstream into a bounded heap; subsequent pulls return columnar batches.
/// No `PlanRow` is created between the scan and the protocol cursor.
pub(super) struct ExactRandIndexTopOne<'a> {
    pub(super) db: &'a Arc<Database>,
    pub(super) txn: TxnView,
    pub(super) scan_plan: &'a PhysicalPlan,
    pub(super) table: &'a crate::planner::PlanTable,
    pub(super) index: &'a crate::storage::IndexSchema,
    pub(super) low: Option<crate::storage::IndexBound>,
    pub(super) high: Option<crate::storage::IndexBound>,
    pub(super) projected_columns: Option<&'a [usize]>,
    pub(super) filter: Option<&'a Expr>,
    pub(super) filter_covered_by_bounds: bool,
    pub(super) degree: usize,
    pub(super) ascending: bool,
}

pub(super) struct CoveringIndexEqPredicate {
    pub(super) terms: smallvec::SmallVec<[(usize, crate::storage::KeyPart); 4]>,
    pub(super) always_false: bool,
}

impl CoveringIndexEqPredicate {
    pub(super) fn compile(
        expr: &Expr,
        table: &crate::planner::PlanTable,
        index: &crate::storage::IndexSchema,
    ) -> Option<Self> {
        fn collect(
            expr: &Expr,
            table: &crate::planner::PlanTable,
            index: &crate::storage::IndexSchema,
            terms: &mut smallvec::SmallVec<[(usize, crate::storage::KeyPart); 4]>,
            always_false: &mut bool,
        ) -> Option<()> {
            if let Expr::Binary {
                left,
                op: BinaryOp::And,
                right,
            } = expr
            {
                collect(left, table, index, terms, always_false)?;
                collect(right, table, index, terms, always_false)?;
                return Some(());
            }
            let Expr::Binary {
                left,
                op: BinaryOp::Eq,
                right,
            } = expr
            else {
                return None;
            };
            let (column_expr, constant_expr) =
                if crate::engine::executor::select::plan::simple_column_index_of(
                    left,
                    &table.schema,
                )
                .is_some()
                {
                    (left.as_ref(), right.as_ref())
                } else {
                    (right.as_ref(), left.as_ref())
                };
            let column = crate::engine::executor::select::plan::simple_column_index_of(
                column_expr,
                &table.schema,
            )?;
            let column_schema = table.schema.columns.get(column)?;
            let position = index
                .effective_columns()
                .iter()
                .position(|name| name.eq_ignore_ascii_case(&column_schema.name))?;
            let value = crate::engine::expr::eval_const(constant_expr).ok()?;
            if matches!(value, Value::Null) {
                *always_false = true;
                return Some(());
            }
            let target = crate::storage::ParamTarget::for_column_with_collation(
                &column_schema.data_type,
                column_schema.unsigned,
                column_schema.collation.as_deref(),
            );
            let part = crate::storage::KeyPart::for_target(value, target).ok()?;
            terms.push((position, part));
            Some(())
        }

        let mut terms = smallvec::SmallVec::new();
        let mut always_false = false;
        collect(expr, table, index, &mut terms, &mut always_false)?;
        Some(Self {
            terms,
            always_false,
        })
    }

    #[inline]
    pub(super) fn matches(&self, key: &crate::storage::IndexKey) -> bool {
        !self.always_false
            && self.terms.iter().all(|(position, expected)| {
                key.0.get(*position).is_some_and(|part| part == expected)
            })
    }

    pub(super) fn terms(&self) -> &[(usize, crate::storage::KeyPart)] {
        self.terms.as_slice()
    }

    pub(super) fn is_always_false(&self) -> bool {
        self.always_false
    }
}

#[cfg(test)]
pub(crate) static EXACT_RAND_INDEX_TOP_ONE_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

pub(super) struct TopNBatchOperator<'a> {
    pub(super) data_dir: std::path::PathBuf,
    pub(super) external_output: Option<Box<dyn BatchOperator + 'a>>,
    pub(super) upstream: Box<dyn BatchOperator + 'a>,
    pub(super) schema: Arc<BatchSchema>,
    pub(super) properties: BatchProperties,
    pub(super) sources: Vec<TopNKeySource>,
    pub(super) row_major_keys: bool,
    pub(super) specs: Vec<vec_topn::SortColumnSpec>,
    pub(super) collations: Vec<Option<u16>>,
    /// Planner-fixed degree for ParallelTopK; one for serial TopK/TopWindow.
    pub(super) parallel_workers: usize,
    pub(super) k: usize,
    pub(super) offset: usize,
    pub(super) output: Option<std::vec::IntoIter<Batch>>,
    pub(super) reservation: Option<mem_budget::ScopedCharge>,
    pub(super) exact_rand_index_top_one: Option<ExactRandIndexTopOne<'a>>,
}

/// Fused GROUP BY hidden-first-value + TopN kernel.  It keeps only the
/// canonical encoded group keys and the K winning output rows: no per-group
/// accumulator object and no full aggregate result materialisation.  This is
/// valid only for MySQL's permissive non-grouped projection slots, whose
/// internal aggregate is exactly "take the first row, including NULL".
pub(super) struct HiddenFirstValueTopNBatchOperator<'a> {
    pub(super) db: &'a Arc<Database>,
    pub(super) aggregate_plan: &'a PhysicalPlan,
    pub(super) upstream: Box<dyn BatchOperator + 'a>,
    pub(super) schema: Arc<BatchSchema>,
    pub(super) properties: BatchProperties,
    pub(super) key_program: crate::engine::executor::hash_aggregate::GroupKeyProgram,
    pub(super) group_columns: Vec<usize>,
    /// When a UNIQUE inner dimension proves the remaining grouping
    /// attributes functionally dependent on this integer join key, the exact
    /// seen-set needs only the key. Winner rows still retain every projected
    /// attribute, so this changes neither output nor representative values.
    pub(super) fd_group_column: Option<usize>,
    pub(super) fd_group_unique: bool,
    pub(super) first_value_columns: Vec<usize>,
    pub(super) sources: Vec<TopNKeySource>,
    pub(super) specs: Vec<vec_topn::SortColumnSpec>,
    pub(super) collations: Vec<Option<u16>>,
    pub(super) k: usize,
    pub(super) offset: usize,
    pub(super) output: Option<std::vec::IntoIter<Batch>>,
    pub(super) group_charge: mem_budget::ScopedCharge,
    pub(super) topn_charge: Option<mem_budget::ScopedCharge>,
}

/// Exact single-scan kernel for high-NDV `GROUP BY ... ORDER BY COUNT(*) DESC
/// LIMIT K` plans.  It retains a compact, declared-width copy of the integer
/// group/aggregate inputs plus `(fingerprint, row ordinal)` records and radix
/// sorts the records by fingerprint. Every repeated SQL group necessarily
/// has a repeated fingerprint; collisions can only admit extra candidates
/// and therefore never affect correctness. All candidate rows plus K
/// arbitrary singleton rows are then fed into the ordinary HashAggregate.
/// Because the only ordering key is COUNT(*), omitted singleton groups all
/// tie at one and K representatives are sufficient for the exact Top-N.
///
/// The fingerprint buffers are capacity-accounted and released before the
/// reduced HashAggregate is built.  This is an internal algorithm of the
/// same Aggregate -> TopN physical pipeline, selected solely from stable
/// estimates and SQL shape.
pub(super) struct DuplicateAwareAggregateBatchOperator<'a> {
    pub(super) db: &'a Arc<Database>,
    pub(super) txn: TxnView,
    pub(super) aggregate_plan: &'a PhysicalPlan,
    pub(super) first_pass_plan: Box<PhysicalPlan>,
    pub(super) group_by: Vec<Expr>,
    pub(super) aggregates: Vec<Expr>,
    pub(super) group_columns: Vec<usize>,
    pub(super) input_template: Vec<PlanField>,
    pub(super) schema: Arc<BatchSchema>,
    pub(super) properties: BatchProperties,
    pub(super) worker_count: usize,
    pub(super) k: usize,
    pub(super) fingerprint_capacity: usize,
    pub(super) compact_charge: Option<mem_budget::ScopedCharge>,
    pub(super) radix_charge: Option<mem_budget::ScopedCharge>,
    pub(super) spill_partitions: Option<usize>,
    pub(super) output: Option<Box<dyn BatchOperator>>,
}

pub(super) const COUNT_TOPK_CMS_DEPTH: usize = 3;
// Keep the per-worker collision floor comfortably below moderately frequent
// Top-K groups on hundred-million-row inputs. At 2^16 cells the average
// residual load alone exceeds ~1,500 rows, so an exact candidate with a count
// around 1,000 can never be proven even with three independent rows. 2^19
// cells use 6 MiB per proof worker, remain bounded, and preserve the
// same deterministic upper-bound semantics.
pub(super) const COUNT_TOPK_CMS_WIDTH: usize = 1 << 19;
// Keep enough canonical bytes to separate long URL/path-like dimensions.
// A very short prefix makes the Count-Min residual bound converge on a
// shared scheme/host prefix instead of on a SQL group, forcing an expensive
// exact HashAggregate restart even when the true Top-K is highly skewed.
// This does not make the proof probabilistic: prefix collisions can still
// only raise the conservative residual bound.
pub(super) const COUNT_TOPK_STRING_PREFIX_BYTES: usize = 64;
// At high NDV, a narrow sketch's collision floor can exceed the gap between
// the Kth and (K+1)th groups even with good candidates. This bounded 48 MiB
// per-worker sketch is admitted up front; refusal retains spillable HashAgg.
pub(super) const INTEGER_COUNT_TOPK_CMS_WIDTH: usize = 1 << 22;
pub(super) const INDEX_COUNT_TOPK_CMS_WIDTH: usize = 1 << 20;
pub(super) const INTEGER_COUNT_TOPK_CANDIDATE_THRESHOLD: u32 = 64;
pub(super) const INTEGER_COUNT_TOPK_MAX_CANDIDATES_PER_WORKER: usize = 1 << 16;

/// Discovery is optional, but rejecting one oversized reservation must not
/// disable it entirely. Leave half the query headroom for candidate maps and
/// the proof, and shrink only the sample, never the planned worker topology.
pub(super) fn count_topk_discovery_sample() -> Option<(usize, mem_budget::ScopedCharge)> {
    const MAX_ROWS: usize = 1024 * 1024;
    const BYTES_PER_ROW: usize = 768;
    let budget = mem_budget::current_budget()?;
    let available = budget.limit().saturating_sub(budget.used()) / 2;
    let mut rows = MAX_ROWS.min(available / BYTES_PER_ROW);
    while rows >= DEFAULT_BATCH_ROWS {
        rows = rows / DEFAULT_BATCH_ROWS * DEFAULT_BATCH_ROWS;
        if let Ok(charge) = mem_budget::ScopedCharge::new_strict(
            rows * BYTES_PER_ROW,
            "stats-proven COUNT Top-K discovery sample",
        ) {
            return Some((rows, charge));
        }
        rows /= 2;
    }
    None
}

/// Keep the hottest prefix whose retained copies fit. Omitted candidates
/// remain in the residual proof, so shrinking cannot produce a false proof.
pub(super) fn charge_count_topk_discovered_candidates(
    hottest: &mut Vec<(Vec<Value>, u32)>,
    worker_count: usize,
    charge: &mut mem_budget::ScopedCharge,
) -> bool {
    hottest.sort_by_key(|(_, count)| std::cmp::Reverse(*count));
    while !hottest.is_empty() {
        let added_bytes = hottest
            .iter()
            .flat_map(|(values, _)| values)
            .fold(0usize, |bytes, value| {
                bytes.saturating_add(mem_budget::estimate_value_bytes(value).saturating_mul(3))
            })
            .saturating_add(hottest.len().saturating_mul(256))
            .saturating_mul(worker_count.saturating_add(2));
        if charge.add(added_bytes).is_ok() {
            return true;
        }
        hottest.truncate(hottest.len() / 2);
    }
    false
}

pub(super) fn count_topk_proof_workers(plan_workers: usize) -> usize {
    plan_workers.clamp(1, 16)
}

pub(super) fn integer_count_topk_proof_bytes(worker_count: usize, cms_width: usize) -> usize {
    let worker_candidates =
        worker_count.saturating_mul(INTEGER_COUNT_TOPK_MAX_CANDIDATES_PER_WORKER);
    worker_count
        .saturating_mul(COUNT_TOPK_CMS_DEPTH)
        .saturating_mul(cms_width)
        .saturating_mul(std::mem::size_of::<u32>())
        .saturating_add(
            worker_candidates
                .saturating_mul(std::mem::size_of::<(Option<i128>, u32)>().saturating_add(17)),
        )
        .saturating_add(
            worker_candidates
                .saturating_mul(std::mem::size_of::<(Option<i128>, usize)>().saturating_add(17)),
        )
        .saturating_add(
            worker_count
                .saturating_mul(worker_candidates)
                .saturating_mul(std::mem::size_of::<u64>()),
        )
}

/// Exact COUNT Top-K accelerator seeded by ANALYZE's frequent values.
/// Candidate counts are exact. Non-candidates enter a Count-Min table whose
/// cells are exact sums (hash collisions only increase them), so the minimum
/// row maximum is a deterministic upper bound for every omitted group. We
/// return candidates only when the Kth exact count is strictly above that
/// bound; otherwise execution reopens the ordinary spill-capable HashAgg.
pub(super) struct StatsProvenCountTopKBatchOperator<'a> {
    pub(super) db: &'a Arc<Database>,
    pub(super) txn: TxnView,
    pub(super) aggregate_plan: &'a PhysicalPlan,
    pub(super) input_plan: &'a PhysicalPlan,
    pub(super) first_pass_plan: Box<PhysicalPlan>,
    pub(super) group_by: Vec<Expr>,
    pub(super) aggregates: Vec<Expr>,
    pub(super) candidate_rows: Vec<Vec<Value>>,
    pub(super) integer_keys: bool,
    pub(super) collation_ids: Vec<Option<u16>>,
    pub(super) proof_group_columns: Vec<usize>,
    pub(super) proof_programs: Vec<Arc<ExprProgram>>,
    pub(super) proof_numeric_programs: Vec<Option<ComputedNumericProgram>>,
    pub(super) proof_schema: Arc<BatchSchema>,
    pub(super) proof_row_major: bool,
    pub(super) first_pass_template_len: usize,
    pub(super) distinct_input_column: Option<usize>,
    pub(super) schema: Arc<BatchSchema>,
    pub(super) input_template: Vec<PlanField>,
    pub(super) properties: BatchProperties,
    pub(super) worker_count: usize,
    pub(super) k: usize,
    pub(super) cms_charge: Option<mem_budget::ScopedCharge>,
    pub(super) candidate_charge: Option<mem_budget::ScopedCharge>,
    pub(super) fallback_output_charge: Option<mem_budget::ScopedCharge>,
    pub(super) output: Option<Box<dyn BatchOperator + 'a>>,
}

#[derive(Clone)]
pub(super) struct ProvenCountCandidate {
    pub(super) values: Vec<Value>,
    pub(super) count: u64,
}

pub(super) struct CountTopKProofRun {
    pub(super) candidates: Vec<ProvenCountCandidate>,
    pub(super) residual_upper_bound: u64,
    pub(super) input_rows: u64,
    pub(super) input_batches: u64,
    pub(super) overflow: bool,
}

pub(super) type CountTopKDistinctSets = Vec<hashbrown::HashSet<i128, ahash::RandomState>>;

#[inline]
pub(super) fn record_count_topk_candidate(
    counts: &mut [u64],
    distinct_sets: &mut Option<CountTopKDistinctSets>,
    candidate: usize,
    distinct_value: Option<i128>,
) {
    if let Some(sets) = distinct_sets {
        if let Some(value) = distinct_value {
            sets[candidate].insert(value);
        }
    } else {
        counts[candidate] = counts[candidate].saturating_add(1);
    }
}

#[inline]
pub(super) fn count_topk_distinct_integer(column: &ColumnVector, row: usize) -> Option<i128> {
    if column.is_null(row) {
        return None;
    }
    match column {
        ColumnVector::I64 { data, .. } => Some(i128::from(data[row])),
        ColumnVector::U64 { data, .. } => Some(i128::from(data[row])),
        _ => None,
    }
}

/// Sample midpoint pages across the complete set of disjoint heap ranges.
pub(super) fn sample_scan_tasks(
    tasks: &mut std::collections::VecDeque<crate::engine::executor::select::parallel::ScanTask>,
    max_pages: usize,
) {
    let total = tasks
        .iter()
        .map(|task| u128::from(task.page_hi - task.page_lo))
        .sum::<u128>();
    let count = total.min(max_pages as u128) as usize;
    if count == 0 {
        tasks.clear();
        return;
    }
    let mut sampled = std::collections::VecDeque::with_capacity(count);
    let mut ranges = tasks.iter();
    let mut task = ranges.next().expect("nonempty page ranges");
    let mut base = 0u128;
    for i in 0..count {
        // Midpoints of equal-sized strata avoid a clustered prefix bias.
        let position = ((2 * i as u128 + 1) * total) / (2 * count as u128);
        while position >= base + u128::from(task.page_hi - task.page_lo) {
            base += u128::from(task.page_hi - task.page_lo);
            task = ranges.next().expect("sample lies inside input ranges");
        }
        let page = task.page_lo + (position - base) as u64;
        sampled.push_back(crate::engine::executor::select::parallel::ScanTask {
            storage_id: task.storage_id,
            page_lo: page,
            page_hi: page + 1,
        });
    }
    *tasks = sampled;
}

/// Counts for fixed candidates are exact; only omitted keys enter the sketch.
/// Collisions can raise its bound but can never turn a failed proof into a hit.
pub(super) struct IntegerCountProofState {
    pub(super) cms: Vec<u32>,
    pub(super) counts: Vec<u64>,
    pub(super) rows: u64,
    pub(super) batches: u64,
    pub(super) overflow: bool,
}

impl IntegerCountProofState {
    pub(super) fn new(candidates: usize) -> Self {
        Self {
            cms: vec![0; COUNT_TOPK_CMS_DEPTH * INTEGER_COUNT_TOPK_CMS_WIDTH],
            counts: vec![0; candidates],
            rows: 0,
            batches: 0,
            overflow: false,
        }
    }

    pub(super) fn consume(
        &mut self,
        batch: &Batch,
        candidates: &hashbrown::HashMap<Option<i128>, usize, ahash::RandomState>,
    ) -> Result<(), QueryError> {
        crate::engine::cancel::check_cancelled()?;
        self.batches += 1;
        let column = batch.columns().first().ok_or_else(|| {
            crate::error::catalog::ER_INTERNAL_ERROR.error("integer COUNT Top-K lost its column")
        })?;
        for row in batch.visible_indices() {
            self.rows += 1;
            let integer = count_topk_integer_column_value(column, row).ok_or_else(|| {
                crate::error::catalog::ER_INTERNAL_ERROR.error("integer COUNT Top-K lane changed")
            })?;
            if let Some(&index) = candidates.get(&integer) {
                self.counts[index] += 1;
            } else {
                for (depth, index) in
                    count_topk_integer_cms_indexes(count_topk_integer_hash(integer))
                        .into_iter()
                        .enumerate()
                {
                    let cell = &mut self.cms[depth * INTEGER_COUNT_TOPK_CMS_WIDTH + index];
                    match cell.checked_add(1) {
                        Some(value) => *cell = value,
                        None => self.overflow = true,
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn merge(&mut self, other: Self) {
        for (count, other) in self.cms.iter_mut().zip(other.cms) {
            match count.checked_add(other) {
                Some(value) => *count = value,
                None => self.overflow = true,
            }
        }
        for (count, other) in self.counts.iter_mut().zip(other.counts) {
            *count += other;
        }
        self.rows += other.rows;
        self.batches += other.batches;
        self.overflow |= other.overflow;
    }

    pub(super) fn residual_bound(&self) -> u64 {
        if self.overflow {
            return u64::MAX;
        }
        self.cms
            .chunks_exact(INTEGER_COUNT_TOPK_CMS_WIDTH)
            .map(|lane| u64::from(lane.iter().copied().max().unwrap_or(0)))
            .min()
            .unwrap_or(0)
    }
}

/// Physical heap ranges support stratified discovery. Index streams retain
/// their cheap typed two-pass discovery/counting algorithm: an index prefix
/// is ordered by its keys and is not a representative heap-page sample.
pub(super) fn integer_count_topk_heap_scan(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::Exchange { input, .. } => integer_count_topk_heap_scan(input),
        PhysicalPlan::SeqScan { .. }
        | PhysicalPlan::ParallelSeqScan { .. }
        | PhysicalPlan::ZoneMapScan { .. }
        | PhysicalPlan::ParallelZoneMapScan { .. } => true,
        _ => false,
    }
}

pub(super) fn run_integer_count_topk_index_proof(
    db: &Arc<Database>,
    txn: TxnView,
    first_pass_plan: &PhysicalPlan,
    first_pass: &mut dyn BatchOperator,
    seed_candidates: &[Vec<Value>],
    worker_count: usize,
) -> Result<CountTopKProofRun, QueryError> {
    type IntegerSet = hashbrown::HashSet<Option<i128>, ahash::RandomState>;
    let seed_keys = seed_candidates
        .iter()
        .filter_map(|row| row.first().and_then(count_topk_integer_value))
        .collect::<Vec<_>>();
    let cancel_token = crate::engine::cancel::current_token();
    let worker_outputs = std::thread::scope(|scope| {
        let mut senders = Vec::with_capacity(worker_count);
        let mut handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let (sender, receiver) = std::sync::mpsc::sync_channel::<Batch>(2);
            senders.push(sender);
            let cancel_token = cancel_token.clone();
            handles.push(scope.spawn(move || -> Result<_, QueryError> {
                let _cancel_guard = cancel_token.map(crate::engine::cancel::enter);
                let cells = COUNT_TOPK_CMS_DEPTH.saturating_mul(INDEX_COUNT_TOPK_CMS_WIDTH);
                let mut cms = vec![0u32; cells];
                let mut candidates = IntegerSet::with_capacity_and_hasher(
                    INTEGER_COUNT_TOPK_MAX_CANDIDATES_PER_WORKER,
                    ahash::RandomState::new(),
                );
                let mut overflow = false;
                let mut rows = 0u64;
                let mut batches = 0u64;
                while let Ok(batch) = receiver.recv() {
                    crate::engine::cancel::check_cancelled()?;
                    batches = batches.saturating_add(1);
                    let Some(column) = batch.columns().first() else {
                        return Err(crate::storage::error(
                            crate::error::catalog::ER_INTERNAL_ERROR,
                            "stats-proven integer COUNT Top-K lost its grouping column",
                        ));
                    };
                    for row in batch.visible_indices() {
                        rows = rows.saturating_add(1);
                        let Some(integer) = count_topk_integer_column_value(column, row) else {
                            return Err(crate::storage::error(
                                crate::error::catalog::ER_INTERNAL_ERROR,
                                "stats-proven integer COUNT Top-K lane changed after preflight",
                            ));
                        };
                        let indexes =
                            count_topk_integer_index_cms_indexes(count_topk_integer_hash(integer));
                        let mut estimate = u32::MAX;
                        for (depth, index) in indexes.into_iter().enumerate() {
                            let cell = depth * INDEX_COUNT_TOPK_CMS_WIDTH + index;
                            cms[cell] = cms[cell].saturating_add(1);
                            estimate = estimate.min(cms[cell]);
                        }
                        if estimate >= INTEGER_COUNT_TOPK_CANDIDATE_THRESHOLD {
                            if candidates.len() < INTEGER_COUNT_TOPK_MAX_CANDIDATES_PER_WORKER {
                                candidates.insert(integer);
                            } else if !candidates.contains(&integer) {
                                overflow = true;
                            }
                        }
                    }
                }
                Ok((candidates, rows, batches, overflow))
            }));
        }

        let mut next_worker = 0usize;
        let produce = (|| -> Result<(), QueryError> {
            while let Some(batch) = first_pass.next_batch()? {
                crate::engine::cancel::check_cancelled()?;
                senders[next_worker].send(batch).map_err(|_| {
                    crate::storage::error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        "stats-proven integer COUNT Top-K worker queue closed",
                    )
                })?;
                next_worker = (next_worker + 1) % senders.len();
            }
            Ok(())
        })();
        drop(senders);
        let mut outputs = Vec::with_capacity(handles.len());
        for handle in handles {
            outputs.push(handle.join().map_err(|_| {
                crate::storage::error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "stats-proven integer COUNT Top-K worker panicked",
                )
            })??);
        }
        produce?;
        Ok::<_, QueryError>(outputs)
    })?;

    let mut candidate_keys = seed_keys;
    let mut first_rows = 0u64;
    let mut first_batches = 0u64;
    let mut overflow = false;
    for (candidates, rows, batches, worker_overflow) in worker_outputs {
        candidate_keys.extend(candidates);
        first_rows = first_rows.saturating_add(rows);
        first_batches = first_batches.saturating_add(batches);
        overflow |= worker_overflow;
    }
    if overflow {
        return Ok(CountTopKProofRun {
            candidates: Vec::new(),
            residual_upper_bound: u64::MAX,
            input_rows: first_rows,
            input_batches: first_batches,
            overflow: true,
        });
    }
    candidate_keys.sort_unstable();
    candidate_keys.dedup();

    let candidate_index = Arc::new(
        candidate_keys
            .iter()
            .copied()
            .enumerate()
            .map(|(index, key)| (key, index))
            .collect::<hashbrown::HashMap<_, _, ahash::RandomState>>(),
    );
    let Some(VecBatchInput {
        stream: mut second_pass,
        template,
    }) = try_native_vec_input_stream(db, txn, first_pass_plan)?
    else {
        return Err(crate::storage::error(
            crate::error::catalog::ER_INTERNAL_ERROR,
            "stats-proven integer COUNT Top-K second-pass capability changed",
        ));
    };
    if template.len() != 1 {
        return Err(crate::storage::error(
            crate::error::catalog::ER_INTERNAL_ERROR,
            "stats-proven integer COUNT Top-K second-pass projection changed",
        ));
    }

    let cancel_token = crate::engine::cancel::current_token();
    let exact_outputs = std::thread::scope(|scope| {
        let mut senders = Vec::with_capacity(worker_count);
        let mut handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let (sender, receiver) = std::sync::mpsc::sync_channel::<Batch>(2);
            senders.push(sender);
            let candidate_index = Arc::clone(&candidate_index);
            let cancel_token = cancel_token.clone();
            handles.push(scope.spawn(move || -> Result<_, QueryError> {
                let _cancel_guard = cancel_token.map(crate::engine::cancel::enter);
                let mut counts = vec![0u64; candidate_index.len()];
                let mut rows = 0u64;
                let mut batches = 0u64;
                while let Ok(batch) = receiver.recv() {
                    crate::engine::cancel::check_cancelled()?;
                    batches = batches.saturating_add(1);
                    let Some(column) = batch.columns().first() else {
                        return Err(crate::storage::error(
                            crate::error::catalog::ER_INTERNAL_ERROR,
                            "stats-proven integer COUNT Top-K lost its second-pass column",
                        ));
                    };
                    for row in batch.visible_indices() {
                        rows = rows.saturating_add(1);
                        let Some(integer) = count_topk_integer_column_value(column, row) else {
                            return Err(crate::storage::error(
                                crate::error::catalog::ER_INTERNAL_ERROR,
                                "stats-proven integer COUNT Top-K second-pass lane changed",
                            ));
                        };
                        if let Some(&candidate) = candidate_index.get(&integer) {
                            counts[candidate] = counts[candidate].saturating_add(1);
                        }
                    }
                }
                Ok((counts, rows, batches))
            }));
        }
        let mut next_worker = 0usize;
        let produce = (|| -> Result<(), QueryError> {
            while let Some(batch) = second_pass.next_batch()? {
                crate::engine::cancel::check_cancelled()?;
                senders[next_worker].send(batch).map_err(|_| {
                    crate::storage::error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        "stats-proven integer COUNT Top-K second-pass queue closed",
                    )
                })?;
                next_worker = (next_worker + 1) % senders.len();
            }
            Ok(())
        })();
        drop(senders);
        let mut outputs = Vec::with_capacity(handles.len());
        for handle in handles {
            outputs.push(handle.join().map_err(|_| {
                crate::storage::error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "stats-proven integer COUNT Top-K second-pass worker panicked",
                )
            })??);
        }
        produce?;
        Ok::<_, QueryError>(outputs)
    })?;
    drop(second_pass);

    let mut counts = vec![0u64; candidate_keys.len()];
    let mut second_rows = 0u64;
    let mut second_batches = 0u64;
    for (worker_counts, rows, batches) in exact_outputs {
        for (count, worker_count) in counts.iter_mut().zip(worker_counts) {
            *count = count.saturating_add(worker_count);
        }
        second_rows = second_rows.saturating_add(rows);
        second_batches = second_batches.saturating_add(batches);
    }
    let unsigned = template[0].source.flags & crate::mysql::protocol::UNSIGNED_FLAG != 0;
    let candidates = candidate_keys
        .into_iter()
        .zip(counts)
        .filter(|(_, count)| *count != 0)
        .map(|(key, count)| ProvenCountCandidate {
            values: vec![match key {
                None => Value::Null,
                Some(value) if unsigned => Value::UInt(value as u64),
                Some(value) => Value::Int(value as i64),
            }],
            count,
        })
        .collect();
    Ok(CountTopKProofRun {
        candidates,
        residual_upper_bound: (INTEGER_COUNT_TOPK_CANDIDATE_THRESHOLD as u64 - 1)
            .saturating_mul(worker_count as u64),
        input_rows: first_rows.saturating_add(second_rows),
        input_batches: first_batches.saturating_add(second_batches),
        overflow: false,
    })
}

#[inline]
pub(super) fn count_topk_integer_index_cms_indexes(hash: u64) -> [usize; COUNT_TOPK_CMS_DEPTH] {
    let mut indexes = [0usize; COUNT_TOPK_CMS_DEPTH];
    let mut lane = hash;
    for (depth, index) in indexes.iter_mut().enumerate() {
        lane =
            avalanche_group_fingerprint(lane ^ (depth as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15));
        *index = (lane as usize) & (INDEX_COUNT_TOPK_CMS_WIDTH - 1);
    }
    indexes
}

pub(super) fn run_integer_count_topk_proof(
    db: &Arc<Database>,
    txn: TxnView,
    first_pass_plan: &PhysicalPlan,
    mut first_pass: Box<dyn BatchOperator + '_>,
    seed_candidates: &[Vec<Value>],
    worker_count: usize,
    proof_charge: &mut mem_budget::ScopedCharge,
) -> Result<CountTopKProofRun, QueryError> {
    if !integer_count_topk_heap_scan(first_pass_plan) {
        return run_integer_count_topk_index_proof(
            db,
            txn,
            first_pass_plan,
            first_pass.as_mut(),
            seed_candidates,
            worker_count,
        );
    }
    let mut candidate_keys = seed_candidates
        .iter()
        .filter_map(|row| row.first().and_then(count_topk_integer_value))
        .collect::<Vec<_>>();
    // Optional discovery is bounded independently of relation cardinality.
    // 256 bytes/row covers the power-of-two hash capacity, its draining
    // selection vector and the current decoded batch, before allocation.
    // It is query workspace; refusal simply leaves ANALYZE's seeds in place.
    const SAMPLE_ROWS: usize = 256 * 1024;
    let mut sample_rows = 0u64;
    let mut sample_batches = 0u64;
    let sample_charge = mem_budget::ScopedCharge::new_strict(
        SAMPLE_ROWS * 256,
        "integer COUNT Top-K bounded discovery",
    )
    .ok();
    // Tuple field order drops the scan (and its sampled descriptors) before
    // releasing the sample reservation, including cancellation/error paths.
    let mut discovery = (first_pass, sample_charge);
    if discovery.1.is_some() {
        discovery.0.sample_scan_pages(8192);
        let mut sample =
            hashbrown::HashSet::<Option<i128>, ahash::RandomState>::with_capacity_and_hasher(
                SAMPLE_ROWS,
                ahash::RandomState::new(),
            );
        while sample_rows < SAMPLE_ROWS as u64 {
            let Some(batch) = discovery.0.next_batch()? else {
                break;
            };
            crate::engine::cancel::check_cancelled()?;
            sample_batches += 1;
            let column = &batch.columns()[0];
            for row in batch
                .visible_indices()
                .take(SAMPLE_ROWS - sample_rows as usize)
            {
                let integer = count_topk_integer_column_value(column, row).ok_or_else(|| {
                    crate::error::catalog::ER_INTERNAL_ERROR
                        .error("integer COUNT Top-K sample lane changed")
                })?;
                sample.insert(integer);
                sample_rows += 1;
            }
        }
        // Retain every sampled distinct key, including singletons: a hot
        // group clustered on disk may appear in only one sampled page.
        candidate_keys.extend(sample);
    }
    // Close the tiny discovery prefix before opening the exact pass. Both
    // use the captured TxnView; no cross-query data or results are retained.
    drop(discovery.0);
    if let Some(mut sample_charge) = discovery.1 {
        // The sampled keys survive discovery. Keep their capacity charge
        // with the proof through exact counting and output materialization.
        proof_charge.absorb(&mut sample_charge);
    }
    candidate_keys.sort_unstable();
    candidate_keys.dedup();
    let candidate_index = candidate_keys
        .iter()
        .copied()
        .enumerate()
        .map(|(index, key)| (key, index))
        .collect::<hashbrown::HashMap<_, _, ahash::RandomState>>();
    let Some(VecBatchInput {
        stream: mut exact_pass,
        template,
    }) = try_native_vec_input_stream(db, txn, first_pass_plan)?
    else {
        return Err(crate::error::catalog::ER_INTERNAL_ERROR
            .error("integer COUNT Top-K exact pass changed"));
    };
    let outputs = if let Some(workers) = exact_pass.take_parallel_workers(worker_count)? {
        // Reuse the physical scan's workers and governed source reservations;
        // decoding and counting stay on the same thread and own the same batch.
        let abort = std::sync::atomic::AtomicBool::new(false);
        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(workers.len());
            for worker in workers {
                let candidates = &candidate_index;
                let abort = &abort;
                handles.push(scope.spawn(move || {
                    struct AbortOnExit<'a>(&'a std::sync::atomic::AtomicBool, bool);
                    impl Drop for AbortOnExit<'_> {
                        fn drop(&mut self) {
                            if !self.1 {
                                self.0.store(true, std::sync::atomic::Ordering::Release);
                            }
                        }
                    }
                    let mut guard = AbortOnExit(abort, false);
                    let mut state = IntegerCountProofState::new(candidates.len());
                    worker.run(&mut |batch| state.consume(&batch, candidates), abort)?;
                    guard.1 = true;
                    Ok::<_, QueryError>(state)
                }));
            }
            let mut outputs = Vec::with_capacity(handles.len());
            let mut error = None;
            for handle in handles {
                match handle.join() {
                    Ok(Ok(state)) => outputs.push(state),
                    Ok(Err(err)) => {
                        if error
                            .as_ref()
                            .is_none_or(|old: &QueryError| old.code() == 1317)
                        {
                            error = Some(err);
                        }
                    }
                    Err(_) => {
                        error = Some(
                            crate::error::catalog::ER_UNKNOWN_ERROR
                                .error("integer COUNT Top-K worker panicked"),
                        )
                    }
                }
            }
            match error {
                Some(error) => Err(error),
                None => Ok(outputs),
            }
        })?
    } else {
        // Keep the planned proof degree for non-transferable sources too.
        // Each queue holds at most two governed batches; close every sender
        // before joining, including producer cancellation and decode errors.
        let cancel = crate::engine::cancel::current_token();
        std::thread::scope(|scope| {
            let mut senders = Vec::with_capacity(worker_count);
            let mut handles = Vec::with_capacity(worker_count);
            for _ in 0..worker_count {
                let (sender, receiver) = std::sync::mpsc::sync_channel::<Batch>(2);
                senders.push(sender);
                let candidates = &candidate_index;
                let cancel = cancel.clone();
                handles.push(scope.spawn(move || {
                    let _cancel = cancel.map(crate::engine::cancel::enter);
                    let mut state = IntegerCountProofState::new(candidates.len());
                    while let Ok(batch) = receiver.recv() {
                        state.consume(&batch, candidates)?;
                    }
                    Ok::<_, QueryError>(state)
                }));
            }
            let produce = (|| -> Result<(), QueryError> {
                let mut next = 0;
                while let Some(batch) = exact_pass.next_batch()? {
                    crate::engine::cancel::check_cancelled()?;
                    senders[next].send(batch).map_err(|_| {
                        crate::error::catalog::ER_UNKNOWN_ERROR
                            .error("integer COUNT Top-K worker queue closed")
                    })?;
                    next = (next + 1) % worker_count;
                }
                Ok(())
            })();
            drop(senders);
            let mut outputs = Vec::with_capacity(worker_count);
            for handle in handles {
                outputs.push(handle.join().map_err(|_| {
                    crate::error::catalog::ER_UNKNOWN_ERROR
                        .error("integer COUNT Top-K worker panicked")
                })??);
            }
            produce?;
            Ok::<_, QueryError>(outputs)
        })?
    };
    drop(exact_pass);
    let mut outputs = outputs.into_iter();
    let mut combined = outputs.next().ok_or_else(|| {
        crate::error::catalog::ER_INTERNAL_ERROR.error("integer COUNT Top-K has no workers")
    })?;
    for state in outputs {
        combined.merge(state);
    }
    let bound = combined.residual_bound();
    let unsigned = template[0].source.flags & crate::mysql::protocol::UNSIGNED_FLAG != 0;
    let candidates = candidate_keys
        .into_iter()
        .zip(combined.counts)
        .filter(|(_, count)| *count != 0)
        .map(|(key, count)| ProvenCountCandidate {
            values: vec![match key {
                None => Value::Null,
                Some(value) if unsigned => Value::UInt(value as u64),
                Some(value) => Value::Int(value as i64),
            }],
            count,
        })
        .collect();
    Ok(CountTopKProofRun {
        candidates,
        residual_upper_bound: bound,
        input_rows: sample_rows + combined.rows,
        input_batches: sample_batches + combined.batches,
        overflow: combined.overflow,
    })
}

#[inline]
pub(super) fn count_topk_cms_indexes(hash: u64) -> [usize; COUNT_TOPK_CMS_DEPTH] {
    let mut indexes = [0usize; COUNT_TOPK_CMS_DEPTH];
    let mut lane = hash;
    for (depth, index) in indexes.iter_mut().enumerate() {
        lane =
            avalanche_group_fingerprint(lane ^ (depth as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15));
        *index = (lane as usize) & (COUNT_TOPK_CMS_WIDTH - 1);
    }
    indexes
}

pub(super) fn encode_count_topk_raw_column(column: &ColumnVector, row: usize, out: &mut Vec<u8>) {
    if column.is_null(row) {
        out.push(0);
        return;
    }
    if let ColumnVector::Utf8 { offsets, data, .. } = column {
        let raw = &data[offsets[row] as usize..offsets[row + 1] as usize];
        out.push(3);
        out.extend_from_slice(&(raw.len() as u32).to_le_bytes());
        out.extend_from_slice(raw);
    } else {
        crate::engine::executor::vec::key_encode::encode_from_column_row_with_collation(
            column, row, None, out,
        );
    }
}

pub(super) fn count_topk_raw_probe_fits(batch: &Batch, columns: &[usize], row: usize) -> bool {
    let mut bytes = 0usize;
    for &column in columns {
        let column = &batch.columns()[column];
        bytes = bytes.saturating_add(if column.is_null(row) {
            1
        } else {
            match column {
                ColumnVector::Utf8 { offsets, .. } => {
                    5 + (offsets[row + 1] - offsets[row]) as usize
                }
                ColumnVector::I64 { .. } | ColumnVector::U64 { .. } => 17,
                _ => return false,
            }
        });
    }
    bytes <= 128
}

pub(super) fn encode_count_topk_raw_value(value: &Value, out: &mut Vec<u8>) {
    let text = match value {
        Value::String(value) => Some(value.as_bytes()),
        Value::SmallString(value) => Some(value.as_slice()),
        _ => None,
    };
    if let Some(bytes) = text {
        out.push(3);
        out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(bytes);
    } else {
        crate::engine::executor::vec::key_encode::encode(value, out);
    }
}

pub(super) fn encode_count_topk_value_prefix(
    value: &Value,
    collation_id: Option<u16>,
    out: &mut Vec<u8>,
) {
    let text = match value {
        Value::String(value) => Some(value.as_str()),
        Value::SmallString(value) => {
            Some(unsafe { std::str::from_utf8_unchecked(value.as_slice()) })
        }
        _ => None,
    };
    if let Some(text) = text {
        out.push(3);
        let length_offset = out.len();
        out.extend_from_slice(&0u32.to_le_bytes());
        let prefix_offset = out.len();
        crate::charset::collation_key_prefix_by_id_into(
            collation_id.unwrap_or(crate::charset::UTF8MB4_0900_AI_CI_ID),
            text,
            COUNT_TOPK_STRING_PREFIX_BYTES,
            out,
        );
        let prefix_len = (out.len() - prefix_offset) as u32;
        out[length_offset..length_offset + 4].copy_from_slice(&prefix_len.to_le_bytes());
    } else {
        crate::engine::executor::vec::key_encode::encode_with_collation(value, collation_id, out);
    }
}

#[inline]
pub(super) fn count_topk_integer_value(value: &Value) -> Option<Option<i128>> {
    match value {
        Value::Null => Some(None),
        Value::Int(value) => Some(Some(i128::from(*value))),
        Value::UInt(value) => Some(Some(i128::from(*value))),
        _ => None,
    }
}

#[inline]
pub(super) fn count_topk_integer_column_value(
    column: &ColumnVector,
    row: usize,
) -> Option<Option<i128>> {
    if column.is_null(row) {
        return Some(None);
    }
    match column {
        ColumnVector::I64 { data, .. } => Some(Some(i128::from(data[row]))),
        ColumnVector::U64 { data, .. } => Some(Some(i128::from(data[row]))),
        _ => None,
    }
}

#[inline]
pub(super) fn count_topk_integer_hash(value: Option<i128>) -> u64 {
    let Some(value) = value else {
        return 0xd6e8_feb8_6659_fd93;
    };
    let low = value as u64;
    let high = (value >> 64) as u64;
    avalanche_group_fingerprint(low ^ high.rotate_left(29) ^ 0xa5a3_58d3_4f1b_9c27)
}

#[inline]
pub(super) fn count_topk_integer_cms_indexes(hash: u64) -> [usize; COUNT_TOPK_CMS_DEPTH] {
    let mut indexes = [0usize; COUNT_TOPK_CMS_DEPTH];
    let mut lane = hash;
    for (depth, index) in indexes.iter_mut().enumerate() {
        lane =
            avalanche_group_fingerprint(lane ^ (depth as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15));
        *index = (lane as usize) & (INTEGER_COUNT_TOPK_CMS_WIDTH - 1);
    }
    indexes
}

pub(super) struct CompactIntegerColumn {
    pub(super) data: Vec<u8>,
    pub(super) width: usize,
    pub(super) unsigned: bool,
}

impl CompactIntegerColumn {
    pub(super) fn width_for_type(column_type: ColumnType) -> Option<usize> {
        Some(match column_type {
            ColumnType::Tiny => 1,
            ColumnType::Short => 2,
            ColumnType::MediumInt | ColumnType::Long => 4,
            ColumnType::LongLong => 8,
            _ => return None,
        })
    }

    pub(super) fn new(field: &BatchField, rows: usize) -> Result<Self, QueryError> {
        let width = Self::width_for_type(field.column_type).ok_or_else(|| {
            crate::storage::error(
                crate::error::catalog::ER_INTERNAL_ERROR,
                "duplicate-aware aggregate received a non-integer compact column",
            )
        })?;
        Ok(Self {
            data: mem_budget::try_vec_with_capacity(
                rows.saturating_mul(width),
                "duplicate-aware aggregate compact column",
            )?,
            width,
            unsigned: field.source.flags & crate::mysql::protocol::UNSIGNED_FLAG != 0,
        })
    }

    #[inline]
    pub(super) fn push(&mut self, column: &ColumnVector, row: usize) -> Result<(), QueryError> {
        let raw = match column {
            ColumnVector::I64 { data, .. } => (data[row] as u64).to_le_bytes(),
            ColumnVector::U64 { data, .. } => data[row].to_le_bytes(),
            _ => {
                return Err(crate::storage::error(
                    crate::error::catalog::ER_INTERNAL_ERROR,
                    "duplicate-aware aggregate integer lane changed after preflight",
                ));
            }
        };
        self.data.extend_from_slice(&raw[..self.width]);
        Ok(())
    }

    #[inline]
    pub(super) fn raw(&self, row: usize) -> u64 {
        let start = row * self.width;
        let mut bytes = [0u8; 8];
        bytes[..self.width].copy_from_slice(&self.data[start..start + self.width]);
        u64::from_le_bytes(bytes)
    }

    pub(super) fn gather(&self, rows: &[u32], null_masks: &[u8], bit: u8) -> ColumnVector {
        let mut nulls = Bitmap::new_all_valid(rows.len());
        let mut saw_null = false;
        if self.unsigned {
            let mut data = Vec::with_capacity(rows.len());
            for (output, &row) in rows.iter().enumerate() {
                let row = row as usize;
                if null_masks[row] & bit != 0 {
                    saw_null = true;
                    nulls.set(output, false);
                    data.push(0);
                } else {
                    data.push(self.raw(row));
                }
            }
            ColumnVector::U64 {
                data,
                nulls: saw_null.then_some(nulls),
            }
        } else {
            let mut data = Vec::with_capacity(rows.len());
            for (output, &row) in rows.iter().enumerate() {
                let row = row as usize;
                if null_masks[row] & bit != 0 {
                    saw_null = true;
                    nulls.set(output, false);
                    data.push(0);
                } else {
                    let raw = self.raw(row);
                    data.push(match self.width {
                        1 => i64::from(raw as i8),
                        2 => i64::from(raw as i16),
                        4 => i64::from(raw as i32),
                        8 => raw as i64,
                        _ => unreachable!("validated compact integer width"),
                    });
                }
            }
            ColumnVector::I64 {
                data,
                nulls: saw_null.then_some(nulls),
            }
        }
    }

    #[inline]
    pub(super) fn push_raw(&mut self, bytes: &[u8]) {
        debug_assert_eq!(bytes.len(), self.width);
        self.data.extend_from_slice(bytes);
    }
}

pub(super) fn duplicate_compact_capacity_bytes(
    columns: &[CompactIntegerColumn],
    null_mask_capacity: usize,
    record_capacity: usize,
) -> usize {
    columns
        .iter()
        .fold(0usize, |bytes, column| {
            bytes.saturating_add(column.data.capacity())
        })
        .saturating_add(null_mask_capacity)
        .saturating_add(record_capacity.saturating_mul(std::mem::size_of::<u64>()))
}

/// Grow the resident duplicate detector only after reserving both its compact
/// rows and the matching radix scratch. Cardinality estimates are an
/// admission hint, not a correctness boundary: if either soft reservation is
/// refused, the caller can discard this attempt and restart through the
/// partition-spill path without poisoning the query budget.
pub(super) fn try_grow_duplicate_resident_capacity(
    compact_columns: &mut [CompactIntegerColumn],
    null_masks: &mut Vec<u8>,
    records: &mut Vec<u64>,
    logical_capacity: &mut usize,
    required_rows: usize,
    compact_charge: &mut mem_budget::ScopedCharge,
    radix_charge: &mut mem_budget::ScopedCharge,
) -> Result<bool, QueryError> {
    if required_rows <= *logical_capacity {
        return Ok(true);
    }
    if required_rows > u32::MAX as usize {
        return Ok(false);
    }

    let growth = (*logical_capacity / 4).max(DEFAULT_BATCH_ROWS * 2);
    let target = logical_capacity
        .saturating_add(growth)
        .max(required_rows)
        .min(u32::MAX as usize);
    let old_compact_bytes = duplicate_compact_capacity_bytes(
        compact_columns,
        null_masks.capacity(),
        records.capacity(),
    );
    let desired_compact_bytes = compact_columns
        .iter()
        .fold(0usize, |bytes, column| {
            bytes.saturating_add(target.saturating_mul(column.width))
        })
        .saturating_add(target)
        .saturating_add(target.saturating_mul(std::mem::size_of::<u64>()));
    let compact_extra = desired_compact_bytes.saturating_sub(old_compact_bytes);
    let radix_extra = target
        .saturating_sub(*logical_capacity)
        .saturating_mul(std::mem::size_of::<u64>());

    if compact_charge.add(compact_extra).is_err() {
        return Ok(false);
    }
    if radix_charge.add(radix_extra).is_err() {
        compact_charge.release_bytes(compact_extra);
        return Ok(false);
    }

    for column in compact_columns.iter_mut() {
        let desired = target.saturating_mul(column.width);
        if desired > column.data.capacity() {
            let additional = desired.saturating_sub(column.data.len());
            mem_budget::try_reserve_vec_exact(
                &mut column.data,
                additional,
                "duplicate-aware aggregate compact column growth",
            )?;
        }
    }
    if target > null_masks.capacity() {
        mem_budget::try_reserve_vec_exact(
            null_masks,
            target.saturating_sub(null_masks.len()),
            "duplicate-aware aggregate null-mask growth",
        )?;
    }
    if target > records.capacity() {
        mem_budget::try_reserve_vec_exact(
            records,
            target.saturating_sub(records.len()),
            "duplicate-aware aggregate fingerprint growth",
        )?;
    }

    // `try_reserve_exact` may receive a larger allocator class than requested.
    // Reconcile the reservation against the capacities actually retained.
    let actual_compact_extra = duplicate_compact_capacity_bytes(
        compact_columns,
        null_masks.capacity(),
        records.capacity(),
    )
    .saturating_sub(old_compact_bytes);
    if actual_compact_extra > compact_extra {
        compact_charge.add_force(actual_compact_extra - compact_extra)?;
    } else {
        compact_charge.release_bytes(compact_extra - actual_compact_extra);
    }
    *logical_capacity = target;
    #[cfg(test)]
    DUPLICATE_AWARE_AGG_TOPN_GROWS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Ok(true)
}

#[inline]
pub(super) fn avalanche_group_fingerprint(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[inline]
pub(super) fn push_fixed_integer_group_fingerprint(
    hash: u64,
    index: usize,
    value: Option<i128>,
) -> u64 {
    let (low, high) = match value {
        Some(value) => (value as u64, (value >> 64) as u64),
        None => (0xd6e8_feb8_6659_fd93u64, 0xa5a3_58d3_4f1b_9c27u64),
    };
    let salt = (index as u64).wrapping_mul(0x517c_c1b7_2722_0a95);
    let lane = avalanche_group_fingerprint(low ^ salt)
        ^ avalanche_group_fingerprint(high ^ salt.rotate_left(23));
    avalanche_group_fingerprint(hash ^ lane)
}

/// Hash a tuple of fixed-width integer lanes without materialising Values or
/// encoded byte keys. Equal SQL group keys always produce the same result.
/// Different keys may collide; the duplicate-aware algorithm deliberately
/// treats collisions as candidates and resolves them in the canonical
/// HashAggregate over the retained compact rows.
#[inline]
pub(super) fn fixed_integer_group_fingerprint(
    batch: &Batch,
    group_columns: &[usize],
    row: usize,
) -> Option<u32> {
    fixed_integer_group_fingerprint_seeded(
        group_columns.len(),
        |column_index| {
            let column = batch.columns().get(column_index)?;
            if column.is_null(row) {
                return Some(None);
            }
            match column {
                ColumnVector::I64 { data, .. } => Some(Some(i128::from(data[row]))),
                ColumnVector::U64 { data, .. } => Some(Some(i128::from(data[row]))),
                _ => None,
            }
        },
        group_columns,
        0x9e37_79b9_7f4a_7c15,
    )
}

#[inline]
pub(super) fn fixed_integer_group_fingerprint_seeded(
    group_count: usize,
    mut value_at: impl FnMut(usize) -> Option<Option<i128>>,
    group_columns: &[usize],
    seed: u64,
) -> Option<u32> {
    let mut hash = seed ^ group_count as u64;
    for (index, &column_index) in group_columns.iter().enumerate() {
        let value = value_at(column_index)?;
        hash = push_fixed_integer_group_fingerprint(hash, index, value);
    }
    let folded = hash ^ (hash >> 32);
    Some(folded as u32)
}

#[inline]
pub(super) fn compact_integer_group_fingerprint(
    columns: &[CompactIntegerColumn],
    null_masks: &[u8],
    group_columns: &[usize],
    row: usize,
) -> Option<u32> {
    fixed_integer_group_fingerprint_seeded(
        group_columns.len(),
        |column_index| {
            let column = columns.get(column_index)?;
            if null_masks[row] & (1 << column_index) != 0 {
                Some(None)
            } else {
                let raw = column.raw(row);
                Some(Some(if column.unsigned {
                    i128::from(raw)
                } else {
                    match column.width {
                        1 => i128::from(raw as i8),
                        2 => i128::from(raw as i16),
                        4 => i128::from(raw as i32),
                        8 => i128::from(raw as i64),
                        _ => return None,
                    }
                }))
            }
        },
        group_columns,
        // Independent seed for refining the sparse set of primary
        // fingerprint collisions. Equality still implies equal hashes, so a
        // true repeated SQL group can never be discarded.
        0x243f_6a88_85a3_08d3,
    )
}

pub(super) fn radix_sort_fingerprint_records(
    values: &mut Vec<u64>,
    requested_workers: usize,
) -> Result<(), QueryError> {
    if values.len() < 2 {
        return Ok(());
    }
    let mut scratch = mem_budget::try_vec_with_capacity(
        values.len(),
        "duplicate-aware aggregate ordinal radix scratch",
    )?;
    scratch.resize(values.len(), 0);
    const RADIX: usize = 1 << 16;
    const PARALLEL_THRESHOLD: usize = 64 * 1024;
    let workers = if values.len() < PARALLEL_THRESHOLD {
        1
    } else {
        requested_workers
            .max(1)
            .min(values.len().div_ceil(PARALLEL_THRESHOLD))
    };
    let mut worker_counts = mem_budget::try_vec_with_capacity(
        workers,
        "duplicate-aware aggregate radix worker histograms",
    )?;
    for _ in 0..workers {
        let mut counts =
            mem_budget::try_vec_with_capacity(RADIX, "duplicate-aware aggregate radix counts")?;
        counts.resize(RADIX, 0usize);
        worker_counts.push(counts);
    }
    for shift in [32u32, 48] {
        for counts in &mut worker_counts {
            counts.fill(0);
        }
        let chunk_rows = values.len().div_ceil(workers);
        std::thread::scope(|scope| {
            for (chunk, counts) in values.chunks(chunk_rows).zip(worker_counts.iter_mut()) {
                scope.spawn(move || {
                    for &value in chunk {
                        counts[((value >> shift) & 0xffff) as usize] += 1;
                    }
                });
            }
        });

        // Convert per-worker histograms into disjoint stable scatter offsets.
        // Worker order follows source-chunk order, so equal fingerprints retain
        // their original ordinal order just as in the serial counting sort.
        let mut offset = 0usize;
        for digit in 0..RADIX {
            for counts in &mut worker_counts {
                let next = offset + counts[digit];
                counts[digit] = offset;
                offset = next;
            }
        }
        let destination = scratch.as_mut_ptr() as usize;
        std::thread::scope(|scope| {
            for (chunk, offsets) in values.chunks(chunk_rows).zip(worker_counts.iter_mut()) {
                scope.spawn(move || {
                    let destination = destination as *mut u64;
                    for &value in chunk {
                        let digit = ((value >> shift) & 0xffff) as usize;
                        let index = offsets[digit];
                        offsets[digit] += 1;
                        // SAFETY: prefix construction assigns every
                        // (worker,digit) pair a disjoint destination interval;
                        // their union is exactly `0..values.len()`. `u64` needs
                        // no drop, and scratch was initialized to full length.
                        unsafe { destination.add(index).write(value) };
                    }
                });
            }
        });
        std::mem::swap(values, &mut scratch);
    }
    Ok(())
}

pub(super) fn select_duplicate_fingerprint_ordinals(
    records: &mut [u64],
    k: usize,
    retained_columns: usize,
    mut secondary_fingerprint: impl FnMut(u32) -> Option<u32>,
) -> Result<(Vec<u32>, mem_budget::ScopedCharge), QueryError> {
    // Records are sorted by the primary 32-bit fingerprint. Birthday
    // collisions become material at tens of millions of rows, so refine only
    // the colliding runs with an independent fingerprint. We rewrite each run
    // in place as `(secondary, ordinal)` and compact selected ordinals into the
    // already-consumed prefix. Equal SQL keys necessarily agree in both
    // fingerprints; combined collisions merely admit extra rows to the
    // canonical HashAggregate and therefore cannot affect correctness.
    let mut selected_count = 0usize;
    let mut singleton_count = 0usize;
    let mut index = 0usize;
    while index < records.len() {
        let fingerprint = records[index] >> 32;
        let mut end = index + 1;
        while end < records.len() && records[end] >> 32 == fingerprint {
            end += 1;
        }
        if end - index == 1 {
            if singleton_count < k {
                records[selected_count] = records[index] as u32 as u64;
                selected_count += 1;
                singleton_count += 1;
            }
            index = end;
            continue;
        }

        for record in &mut records[index..end] {
            let ordinal = *record as u32;
            let secondary = secondary_fingerprint(ordinal).ok_or_else(|| {
                crate::storage::error(
                    crate::error::catalog::ER_INTERNAL_ERROR,
                    "duplicate-aware aggregate secondary fingerprint capability changed",
                )
            })?;
            *record = (u64::from(secondary) << 32) | u64::from(ordinal);
        }
        records[index..end].sort_unstable();
        let mut secondary_index = index;
        while secondary_index < end {
            let secondary = records[secondary_index] >> 32;
            let mut secondary_end = secondary_index + 1;
            while secondary_end < end && records[secondary_end] >> 32 == secondary {
                secondary_end += 1;
            }
            if secondary_end - secondary_index > 1 {
                for source in secondary_index..secondary_end {
                    records[selected_count] = records[source] as u32 as u64;
                    selected_count += 1;
                }
            } else if singleton_count < k {
                records[selected_count] = records[secondary_index] as u32 as u64;
                selected_count += 1;
                singleton_count += 1;
            }
            secondary_index = secondary_end;
        }
        index = end;
    }
    let expanded_bytes_per_row = retained_columns
        .saturating_mul(std::mem::size_of::<u64>())
        .saturating_add(retained_columns.div_ceil(u8::BITS as usize));
    let selected_bytes = selected_count
        .saturating_mul(std::mem::size_of::<u32>().saturating_add(expanded_bytes_per_row))
        .saturating_add(
            selected_count
                .div_ceil(DEFAULT_BATCH_ROWS)
                .saturating_mul(std::mem::size_of::<Batch>()),
        );
    let mut charge = mem_budget::ScopedCharge::new(0);
    charge.add(selected_bytes)?;
    let mut selected = mem_budget::try_vec_with_capacity(
        selected_count,
        "duplicate-aware aggregate selected ordinals",
    )?;
    selected.extend(
        records[..selected_count]
            .iter()
            .map(|record| *record as u32),
    );
    debug_assert_eq!(selected.len(), selected_count);
    Ok((selected, charge))
}

impl StatsProvenCountTopKBatchOperator<'_> {
    pub(super) fn finish_proof(
        &mut self,
        mut candidates: Vec<ProvenCountCandidate>,
        residual_upper_bound: u64,
        input_rows: u64,
        input_batches: u64,
        proof_overflow: bool,
        proof_workers: usize,
        proof_memory_bytes: usize,
    ) -> Result<(), QueryError> {
        candidates.sort_by_key(|candidate| std::cmp::Reverse(candidate.count));
        let kth_count = candidates
            .get(self.k.saturating_sub(1))
            .map(|candidate| candidate.count);
        let proven = !proof_overflow
            && kth_count.is_some_and(|count| count > residual_upper_bound)
            // ORDER BY COUNT alone leaves ties to the enclosing stable Top-K
            // oracle. Returning only K proof rows is safe only when the
            // candidate boundary itself is strict as well.
            && candidates
                .get(self.k)
                .is_none_or(|candidate| kth_count.is_some_and(|count| candidate.count < count));
        if crate::util::env::var("MYDB_DEBUG_COUNT_TOPK_PROOF").is_ok() {
            eprintln!(
                "stats-proven COUNT Top-K: kth={} residual_upper={} candidates={} rows={} overflow={} proven={}",
                candidates
                    .get(self.k.saturating_sub(1))
                    .map(|candidate| candidate.count)
                    .unwrap_or(0),
                residual_upper_bound,
                candidates.len(),
                input_rows,
                proof_overflow,
                proven
            );
        }
        if proven {
            // The boundary is strict, so no omitted candidate can reach Top-K.
            candidates.truncate(self.k);
            STATS_PROVEN_COUNT_TOPK_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let group_count = self.input_template.len();
            let mut columns = (0..group_count)
                .map(|_| Vec::with_capacity(candidates.len()))
                .collect::<Vec<_>>();
            let mut counts = Vec::with_capacity(candidates.len());
            for candidate in candidates {
                for (column, value) in columns.iter_mut().zip(candidate.values) {
                    column.push(value);
                }
                counts.push(candidate.count.min(i64::MAX as u64) as i64);
            }
            let mut vectors = columns
                .into_iter()
                .map(ColumnVector::from_values)
                .collect::<Vec<_>>();
            vectors.push(ColumnVector::I64 {
                data: counts,
                nulls: None,
            });
            let batch = Batch::new(self.schema.clone(), vectors);
            record_pending_agg_metrics(vec_hash_agg::AggMetrics {
                peak_memory_bytes: proof_memory_bytes as u64,
                is_streaming: true,
                batches: input_batches,
                workers: proof_workers as u32,
                aggregate_mode: Some("stats_proven_count_topk"),
                native_aggregate_rows: input_rows,
                ..vec_hash_agg::AggMetrics::default()
            });
            forward_vec_hash_agg_metrics(self.aggregate_plan);
            self.output = Some(Box::new(MaterializedBatchOperator::new(
                self.schema.clone(),
                vec![batch],
            )));
            return Ok(());
        }

        drop(candidates);
        self.cms_charge = None;
        self.candidate_charge = None;
        STATS_PROVEN_COUNT_TOPK_FALLBACKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let Some(input) = try_native_vec_input_stream(self.db, self.txn, self.input_plan)? else {
            return Err(crate::storage::error(
                crate::error::catalog::ER_INTERNAL_ERROR,
                "stats-proven COUNT Top-K fallback input capability changed",
            ));
        };
        let Some(built) = build_vec_hash_agg_stream_from_input(
            self.db,
            input,
            &self.group_by,
            &self.aggregates,
            false,
            self.worker_count,
            None,
            None,
        )?
        else {
            return Err(crate::storage::error(
                crate::error::catalog::ER_INTERNAL_ERROR,
                "stats-proven COUNT Top-K fallback aggregate capability changed",
            ));
        };
        record_pending_agg_metrics(built.metrics);
        forward_vec_hash_agg_metrics(self.aggregate_plan);
        let mut stream = built.stream;
        if self.integer_keys {
            // The enclosing Top-K remains the ordering/tie oracle. On an
            // unproven integer sketch, returning the complete exact aggregate
            // preserves its stable tie behaviour instead of choosing an
            // arbitrary K rows inside this fallback wrapper.
            self.output = Some(stream);
            return Ok(());
        }
        let mut top_rows: Vec<(u64, Vec<Value>, usize)> = Vec::with_capacity(self.k);
        let mut output_charge = mem_budget::ScopedCharge::new(0);
        output_charge.add_force(top_rows.capacity().saturating_mul(std::mem::size_of::<(
            u64,
            Vec<Value>,
            usize,
        )>()))?;
        while let Some(batch) = stream.next_batch()? {
            for row in batch.visible_indices() {
                let values = batch
                    .columns()
                    .iter()
                    .map(|column| column.value_at(row))
                    .collect::<Vec<_>>();
                let count = match values.last() {
                    Some(Value::Int(value)) => (*value).max(0) as u64,
                    Some(Value::UInt(value)) => *value,
                    _ => 0,
                };
                let bytes = mem_budget::estimate_row_bytes(&values);
                if top_rows.len() < self.k {
                    output_charge.add_force(bytes)?;
                    top_rows.push((count, values, bytes));
                    continue;
                }
                let Some((smallest, _)) = top_rows
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, (count, _, _))| *count)
                else {
                    continue;
                };
                if count > top_rows[smallest].0 {
                    output_charge.add_force(bytes)?;
                    let released = top_rows[smallest].2;
                    top_rows[smallest] = (count, values, bytes);
                    output_charge.release_bytes(released);
                }
            }
        }
        top_rows.sort_by_key(|row| std::cmp::Reverse(row.0));
        let rows = top_rows
            .into_iter()
            .map(|(_, values, _)| values)
            .collect::<Vec<_>>();
        let mut columns = (0..self.schema.len())
            .map(|_| Vec::with_capacity(rows.len()))
            .collect::<Vec<_>>();
        for row in rows {
            for (column, value) in columns.iter_mut().zip(row) {
                column.push(value);
            }
        }
        let batch = Batch::new(
            self.schema.clone(),
            columns.into_iter().map(ColumnVector::from_values).collect(),
        );
        self.fallback_output_charge = Some(output_charge);
        self.output = Some(Box::new(MaterializedBatchOperator::new(
            self.schema.clone(),
            vec![batch],
        )));
        Ok(())
    }

    pub(super) fn initialize(&mut self) -> Result<(), QueryError> {
        if self.output.is_some() {
            return Ok(());
        }
        let Some(VecBatchInput {
            stream: mut first_pass,
            template,
        }) = try_native_vec_input_stream(self.db, self.txn, self.first_pass_plan.as_ref())?
        else {
            return Err(crate::storage::error(
                crate::error::catalog::ER_INTERNAL_ERROR,
                "stats-proven COUNT Top-K input capability changed after preflight",
            ));
        };
        if template.len() != self.first_pass_template_len || template.is_empty() {
            return Err(crate::storage::error(
                crate::error::catalog::ER_INTERNAL_ERROR,
                "stats-proven COUNT Top-K projection changed after preflight",
            ));
        }
        if !self.proof_programs.is_empty() {
            first_pass = Box::new(ComputeColumnsBatchStream {
                pending_chunks: None,
                inner: first_pass,
                programs: self.proof_programs.clone(),
                numeric_programs: self.proof_numeric_programs.clone(),
                direct_expressions: Vec::new(),
                schema: self.proof_schema.clone(),
                row_major: self.proof_row_major,
            });
        }
        let worker_count = count_topk_proof_workers(self.worker_count);
        if self.integer_keys {
            let proof = run_integer_count_topk_proof(
                self.db,
                self.txn,
                self.first_pass_plan.as_ref(),
                first_pass,
                &self.candidate_rows,
                worker_count,
                self.cms_charge.as_mut().expect("integer proof reserved"),
            )?;
            return self.finish_proof(
                proof.candidates,
                proof.residual_upper_bound,
                proof.input_rows,
                proof.input_batches,
                proof.overflow,
                worker_count,
                self.cms_charge
                    .as_ref()
                    .map_or(0, mem_budget::ScopedCharge::charged)
                    + self
                        .candidate_charge
                        .as_ref()
                        .map_or(0, mem_budget::ScopedCharge::charged),
            );
        }

        // ANALYZE samples can miss a frequent composite combination even
        // when each marginal column statistic is accurate. Probe a small
        // prefix of the already-narrowed input to seed real composite keys.
        // Discovery is parallel and the sampled columnar batches are retained
        // for the exact proof, so heap pages are never scanned twice. This is
        // never part of the answer: missed candidates merely make the
        // conservative proof fall back to ordinary HashAggregate.
        const DISCOVERY_CANDIDATES: usize = 16 * 1024;
        let mut discovery_batches = std::collections::VecDeque::new();
        let mut discovery_charge = None;
        if let Some((discovery_rows, charge)) = count_topk_discovery_sample() {
            let mut sampled = 0usize;
            while sampled < discovery_rows {
                let Some(batch) = first_pass.next_batch()? else {
                    break;
                };
                sampled = sampled.saturating_add(batch.visible_rows());
                discovery_batches.push_back(batch);
            }
            let discovery_collations = self.collation_ids.clone();
            let discovery_group_columns = self.proof_group_columns.clone();
            let local_candidate_limit = DISCOVERY_CANDIDATES;
            let local_hottest = std::thread::scope(|scope| {
                let mut senders = Vec::with_capacity(worker_count);
                let mut handles = Vec::with_capacity(worker_count);
                for _ in 0..worker_count {
                    let (sender, receiver) = std::sync::mpsc::sync_channel::<Batch>(2);
                    senders.push(sender);
                    let collations = discovery_collations.clone();
                    let group_columns = discovery_group_columns.clone();
                    handles.push(scope.spawn(move || {
                        let mut discovered = hashbrown::HashMap::<
                            Vec<u8>,
                            (Vec<Value>, u32),
                            ahash::RandomState,
                        >::with_capacity_and_hasher(
                            discovery_rows / worker_count / 2,
                            ahash::RandomState::new(),
                        );
                        let mut sample_key = Vec::with_capacity(128);
                        while let Ok(batch) = receiver.recv() {
                            for row in batch.visible_indices() {
                                sample_key.clear();
                                for (&column, &collation_id) in
                                    group_columns.iter().zip(&collations)
                                {
                                    crate::engine::executor::vec::key_encode::encode_from_column_row_with_collation(
                                        &batch.columns()[column],
                                        row,
                                        collation_id,
                                        &mut sample_key,
                                    );
                                }
                                match discovered.entry(sample_key.clone()) {
                                    hashbrown::hash_map::Entry::Occupied(mut entry) => {
                                        let value = entry.get_mut();
                                        value.1 = value.1.saturating_add(1);
                                    }
                                    hashbrown::hash_map::Entry::Vacant(entry) => {
                                        entry.insert((
                                            group_columns
                                                .iter()
                                                .map(|&column| batch.columns()[column].value_at(row))
                                                .collect(),
                                            1,
                                        ));
                                    }
                                }
                            }
                        }
                        let mut local = discovered
                            .into_iter()
                            .map(|(key, (values, count))| (key, values, count))
                            .collect::<Vec<_>>();
                        if local.len() > local_candidate_limit {
                            local.select_nth_unstable_by_key(
                                local_candidate_limit - 1,
                                |(_, _, count)| std::cmp::Reverse(*count),
                            );
                            local.truncate(local_candidate_limit);
                        }
                        local
                    }));
                }
                for (index, batch) in discovery_batches.iter().cloned().enumerate() {
                    senders[index % worker_count]
                        .send(batch)
                        .expect("COUNT Top-K discovery worker remains live");
                }
                drop(senders);
                handles
                    .into_iter()
                    .flat_map(|handle| {
                        handle
                            .join()
                            .expect("COUNT Top-K discovery worker did not panic")
                    })
                    .collect::<Vec<_>>()
            });
            let mut merged = hashbrown::HashMap::<
                Vec<u8>,
                (Vec<Value>, u32),
                ahash::RandomState,
            >::with_capacity_and_hasher(local_hottest.len(), ahash::RandomState::new());
            for (key, values, count) in local_hottest {
                match merged.entry(key) {
                    hashbrown::hash_map::Entry::Occupied(mut entry) => {
                        let value = entry.get_mut();
                        value.1 = value.1.saturating_add(count);
                    }
                    hashbrown::hash_map::Entry::Vacant(entry) => {
                        entry.insert((values, count));
                    }
                }
            }
            let mut hottest = merged.into_values().collect::<Vec<_>>();
            if hottest.len() > DISCOVERY_CANDIDATES {
                hottest.select_nth_unstable_by_key(DISCOVERY_CANDIDATES - 1, |(_, count)| {
                    std::cmp::Reverse(*count)
                });
                hottest.truncate(DISCOVERY_CANDIDATES);
            }
            if self.candidate_charge.as_mut().is_some_and(|charge| {
                charge_count_topk_discovered_candidates(&mut hottest, worker_count, charge)
            }) {
                self.candidate_rows
                    .extend(hottest.into_iter().map(|(values, _)| values));
            }
            // Keep the already-columnar prefix alive and feed it into the
            // proof before continuing the same scan. The strict discovery
            // reservation follows these batches until every worker has
            // consumed them, avoiding a second heap/page-directory pass.
            discovery_charge = Some(charge);
        }

        let mut candidate_index =
            hashbrown::HashMap::<Vec<u8>, usize, ahash::RandomState>::with_capacity_and_hasher(
                self.candidate_rows.len(),
                ahash::RandomState::new(),
            );
        let mut raw_candidate_index =
            hashbrown::HashMap::<Vec<u8>, usize, ahash::RandomState>::with_capacity_and_hasher(
                self.candidate_rows.len(),
                ahash::RandomState::new(),
            );
        let mut candidate_prefixes =
            hashbrown::HashSet::<Vec<u8>, ahash::RandomState>::with_capacity_and_hasher(
                self.candidate_rows.len(),
                ahash::RandomState::new(),
            );
        let mut candidate_values = Vec::with_capacity(self.candidate_rows.len());
        let mut key = Vec::with_capacity(128);
        let mut raw_key = Vec::with_capacity(128);
        let mut prefix_key = Vec::with_capacity(64);
        for values in self.candidate_rows.drain(..) {
            key.clear();
            raw_key.clear();
            prefix_key.clear();
            for (value, &collation_id) in values.iter().zip(&self.collation_ids) {
                crate::engine::executor::vec::key_encode::encode_with_collation(
                    value,
                    collation_id,
                    &mut key,
                );
                encode_count_topk_raw_value(value, &mut raw_key);
                encode_count_topk_value_prefix(value, collation_id, &mut prefix_key);
            }
            if !candidate_index.contains_key(key.as_slice()) {
                let index = candidate_values.len();
                candidate_index.insert(key.clone(), index);
                raw_candidate_index.insert(raw_key.clone(), index);
                candidate_prefixes.insert(prefix_key.clone());
                candidate_values.push(values);
            }
        }

        let cells = COUNT_TOPK_CMS_DEPTH.saturating_mul(COUNT_TOPK_CMS_WIDTH);
        let collation_ids = self.collation_ids.clone();
        let candidate_count = candidate_values.len();
        let proof_group_columns = self.proof_group_columns.clone();
        let proof_column_count = self.proof_schema.len();
        let distinct_input_column = self.distinct_input_column;
        let cancel_token = crate::engine::cancel::current_token();
        let worker_outputs = std::thread::scope(|scope| {
            let mut senders = Vec::with_capacity(worker_count);
            let mut handles = Vec::with_capacity(worker_count);
            for _ in 0..worker_count {
                let (sender, receiver) = std::sync::mpsc::sync_channel::<Batch>(2);
                senders.push(sender);
                let local_index = candidate_index.clone();
                let local_raw_index = raw_candidate_index.clone();
                let local_candidate_prefixes = candidate_prefixes.clone();
                let local_collation_ids = collation_ids.clone();
                let local_group_columns = proof_group_columns.clone();
                let cancel_token = cancel_token.clone();
                handles.push(scope.spawn(move || -> Result<_, QueryError> {
                    let _cancel_guard = cancel_token.map(crate::engine::cancel::enter);
                    let mut cms = vec![0u32; cells];
                    let mut counts = vec![0u64; candidate_count];
                    let mut distinct_sets = distinct_input_column.is_some().then(|| {
                        (0..candidate_count)
                            .map(|_| {
                                hashbrown::HashSet::with_hasher(ahash::RandomState::new())
                            })
                            .collect::<CountTopKDistinctSets>()
                    });
                    let mut cms_overflow = false;
                    let mut key = Vec::with_capacity(128);
                    let mut raw_key = Vec::with_capacity(128);
                    let mut prefix_key = Vec::with_capacity(64);
                    let mut rows = 0u64;
                    let mut batches = 0u64;
                    while let Ok(batch) = receiver.recv() {
                        crate::engine::cancel::check_cancelled()?;
                        batches = batches.saturating_add(1);
                        if batch.columns().len() != proof_column_count {
                            return Err(crate::storage::error(
                                crate::error::catalog::ER_INTERNAL_ERROR,
                                "stats-proven COUNT Top-K lost a grouping column",
                            ));
                        }
                        // Both probes are exact and side-effect free. A
                        // bounded sample chooses which necessary check to do
                        // first; frequent byte-identical candidates should not
                        // pay Unicode prefix normalization on every row.
                        let mut sampled = 0usize;
                        let mut raw_hits = 0usize;
                        for row in batch.visible_indices().take(64) {
                            sampled += 1;
                            if !count_topk_raw_probe_fits(&batch, &local_group_columns, row) { continue; }
                            raw_key.clear();
                            for &column in &local_group_columns {
                                encode_count_topk_raw_column(&batch.columns()[column], row, &mut raw_key);
                            }
                            raw_hits += usize::from(local_raw_index.contains_key(raw_key.as_slice()));
                        }
                        let raw_first = sampled != 0 && raw_hits.saturating_mul(4) >= sampled;
                        // Borrow only a row index in this batch. Physical UTF-8
                        // equality is stronger than every supported collation;
                        // reuse both candidate hits and conservative residual hashes.
                        // DISTINCT still consumes the current row's input value.
                        let repeated_column = if local_group_columns.len() == 1 {
                            match &batch.columns()[local_group_columns[0]] {
                                ColumnVector::Utf8 { offsets, data, .. } => Some((offsets, data)),
                                _ => None,
                            }
                        } else { None };
                        let mut previous: Option<(usize, Option<usize>, u64)> = None;
                        for row in batch.visible_indices() {
                            rows = rows.saturating_add(1);
                            let distinct_value = distinct_input_column
                                .and_then(|column| batch.columns().get(column))
                                .and_then(|column| count_topk_distinct_integer(column, row));
                            if let (Some((offsets, data)), Some((prior, candidate, hash))) = (repeated_column, previous) {
                                let column = &batch.columns()[local_group_columns[0]];
                                if column.is_null(prior) == column.is_null(row)
                                    && (column.is_null(row) || data[offsets[prior] as usize..offsets[prior + 1] as usize]
                                        == data[offsets[row] as usize..offsets[row + 1] as usize]) {
                                    if let Some(candidate) = candidate {
                                        record_count_topk_candidate(&mut counts, &mut distinct_sets, candidate, distinct_value);
                                    } else {
                                        for (depth, index) in count_topk_cms_indexes(hash).into_iter().enumerate() {
                                            let cell = depth * COUNT_TOPK_CMS_WIDTH + index;
                                            match cms[cell].checked_add(1) {
                                                Some(value) => cms[cell] = value,
                                                None => cms_overflow = true,
                                            }
                                        }
                                    }
                                    previous = Some((row, candidate, hash));
                                    continue;
                                }
                            }
                            let raw_probed = raw_first && count_topk_raw_probe_fits(&batch, &local_group_columns, row);
                            if raw_probed {
                                raw_key.clear();
                                for &column in &local_group_columns {
                                    encode_count_topk_raw_column(&batch.columns()[column], row, &mut raw_key);
                                }
                                if let Some(&candidate) = local_raw_index.get(raw_key.as_slice()) {
                                    record_count_topk_candidate(&mut counts, &mut distinct_sets, candidate, distinct_value);
                                    previous = Some((row, Some(candidate), 0));
                                    continue;
                                }
                            }
                            prefix_key.clear();
                            for (&column, &collation_id) in
                                local_group_columns.iter().zip(&local_collation_ids)
                            {
                                crate::engine::executor::vec::key_encode::encode_from_column_row_with_collation_prefix(
                                    &batch.columns()[column],
                                    row,
                                    collation_id,
                                    COUNT_TOPK_STRING_PREFIX_BYTES,
                                    &mut prefix_key,
                                );
                            }
                            let residual_hash = if local_candidate_prefixes
                                .contains(prefix_key.as_slice())
                            {
                                if !raw_probed {
                                raw_key.clear();
                                for &column in &local_group_columns {
                                    encode_count_topk_raw_column(
                                        &batch.columns()[column],
                                        row,
                                        &mut raw_key,
                                    );
                                }
                                if let Some(&candidate) =
                                    local_raw_index.get(raw_key.as_slice())
                                {
                                    record_count_topk_candidate(
                                        &mut counts,
                                        &mut distinct_sets,
                                        candidate,
                                        distinct_value,
                                    );
                                    previous = Some((row, Some(candidate), 0));
                                    continue;
                                }
                                }
                                key.clear();
                                for (&column, &collation_id) in
                                    local_group_columns.iter().zip(&local_collation_ids)
                                {
                                    crate::engine::executor::vec::key_encode::encode_from_column_row_with_collation(
                                        &batch.columns()[column],
                                        row,
                                        collation_id,
                                        &mut key,
                                    );
                                }
                                if let Some(&candidate) = local_index.get(key.as_slice()) {
                                    record_count_topk_candidate(
                                        &mut counts,
                                        &mut distinct_sets,
                                        candidate,
                                        distinct_value,
                                    );
                                    previous = Some((row, Some(candidate), 0));
                                    continue;
                                }
                                // A hot candidate prefix can contain several
                                // distinct long-tail groups. We already paid
                                // to materialize their full canonical key for
                                // candidate rejection, so use it to refine
                                // the residual sketch at no extra collation
                                // cost.
                                crate::engine::executor::vec::key_encode::fxhash(&key)
                            } else {
                                crate::engine::executor::vec::key_encode::fxhash(&prefix_key)
                            };
                            previous = Some((row, None, residual_hash));
                            for (depth, index) in
                                count_topk_cms_indexes(residual_hash).into_iter().enumerate()
                            {
                                let cell = depth * COUNT_TOPK_CMS_WIDTH + index;
                                match cms[cell].checked_add(1) {
                                    Some(value) => cms[cell] = value,
                                    None => cms_overflow = true,
                                }
                            }
                        }
                    }
                    Ok((cms, counts, distinct_sets, rows, batches, cms_overflow))
                }));
            }

            let mut next_worker = 0usize;
            let produce = (|| -> Result<(), QueryError> {
                while let Some(batch) = discovery_batches.pop_front() {
                    senders[next_worker].send(batch).map_err(|_| {
                        crate::storage::error(
                            crate::error::catalog::ER_UNKNOWN_ERROR,
                            "stats-proven COUNT Top-K discovery queue closed",
                        )
                    })?;
                    next_worker = (next_worker + 1) % senders.len();
                }
                while let Some(batch) = first_pass.next_batch()? {
                    crate::engine::cancel::check_cancelled()?;
                    senders[next_worker].send(batch).map_err(|_| {
                        crate::storage::error(
                            crate::error::catalog::ER_UNKNOWN_ERROR,
                            "stats-proven COUNT Top-K worker queue closed",
                        )
                    })?;
                    next_worker = (next_worker + 1) % senders.len();
                }
                Ok(())
            })();
            drop(senders);
            let mut outputs = Vec::with_capacity(handles.len());
            for handle in handles {
                outputs.push(handle.join().map_err(|_| {
                    crate::storage::error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        "stats-proven COUNT Top-K worker panicked",
                    )
                })??);
            }
            produce?;
            Ok::<_, QueryError>(outputs)
        })?;
        drop(discovery_charge);
        drop(first_pass);

        let mut outputs = worker_outputs.into_iter();
        let Some((
            mut cms,
            mut counts,
            mut distinct_sets,
            mut input_rows,
            mut input_batches,
            mut cms_overflow,
        )) = outputs.next()
        else {
            return Err(crate::storage::error(
                crate::error::catalog::ER_INTERNAL_ERROR,
                "stats-proven COUNT Top-K started without workers",
            ));
        };
        for (other_cms, other_counts, other_distinct_sets, rows, batches, other_overflow) in outputs
        {
            for (count, other) in cms.iter_mut().zip(other_cms) {
                match count.checked_add(other) {
                    Some(value) => *count = value,
                    None => cms_overflow = true,
                }
            }
            for (count, other) in counts.iter_mut().zip(other_counts) {
                *count = count.saturating_add(other);
            }
            match (&mut distinct_sets, other_distinct_sets) {
                (Some(target), Some(source)) => {
                    for (target, source) in target.iter_mut().zip(source) {
                        target.extend(source);
                    }
                }
                (None, None) => {}
                _ => unreachable!("COUNT Top-K distinct state changed across workers"),
            }
            input_rows = input_rows.saturating_add(rows);
            input_batches = input_batches.saturating_add(batches);
            cms_overflow |= other_overflow;
        }

        let residual_upper_bound = (0..COUNT_TOPK_CMS_DEPTH)
            .map(|depth| {
                cms[depth * COUNT_TOPK_CMS_WIDTH..(depth + 1) * COUNT_TOPK_CMS_WIDTH]
                    .iter()
                    .copied()
                    .max()
                    .map(u64::from)
                    .unwrap_or(0)
            })
            .min()
            .unwrap_or(0);
        if let Some(distinct_sets) = distinct_sets {
            counts = distinct_sets
                .into_iter()
                .map(|values| values.len() as u64)
                .collect();
        }
        let mut candidates = candidate_values
            .into_iter()
            .zip(counts)
            .filter(|(_, count)| *count != 0)
            .map(|(values, count)| ProvenCountCandidate { values, count })
            .collect::<Vec<_>>();
        self.finish_proof(
            std::mem::take(&mut candidates),
            residual_upper_bound,
            input_rows,
            input_batches,
            cms_overflow,
            worker_count,
            worker_count
                .saturating_mul(COUNT_TOPK_CMS_DEPTH)
                .saturating_mul(COUNT_TOPK_CMS_WIDTH)
                .saturating_mul(std::mem::size_of::<u32>()),
        )
    }
}

impl BatchOperator for StatsProvenCountTopKBatchOperator<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }

    fn properties(&self) -> BatchProperties {
        self.properties.clone()
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        self.initialize()?;
        self.output
            .as_mut()
            .expect("stats-proven COUNT Top-K output initialized")
            .next_batch()
    }
}

pub(super) const DUPLICATE_SPILL_RECORD_MAX_BYTES: usize = 4 + 1 + u8::BITS as usize * 8;

pub(super) fn duplicate_spill_io_error(error: std::io::Error) -> QueryError {
    if let Some(error) = crate::engine::executor::spill_quota::query_error_from_io(&error) {
        return error;
    }
    crate::storage::error(
        crate::error::catalog::ER_UNKNOWN_ERROR,
        format!("duplicate-aware aggregate spill I/O: {error}"),
    )
}

/// Pick enough fingerprint partitions that one replay partition, including
/// its compact columns and radix scratch, fits comfortably below the current
/// query watermark.  This is an operator-internal batching choice: it depends
/// only on memory available to the already-selected physical operator and
/// does not alter scan/aggregate topology.
pub(super) fn duplicate_spill_partition_count(estimated_peak_bytes: usize) -> Option<usize> {
    const MIN_PARTITIONS: usize = 4;
    const MAX_PARTITIONS: usize = 256;
    const MIN_REPLAY_TARGET: usize = 256 * 1024;

    let available = mem_budget::current_budget()
        .map(|budget| budget.remaining())
        .unwrap_or(256 * 1024 * 1024);
    // Leave room for the reader buffer, selected-candidate batches, skew
    // between hash partitions, and the final tiny HashAggregate state.
    let replay_target = available.saturating_div(3).max(MIN_REPLAY_TARGET);
    let needed = estimated_peak_bytes
        .div_ceil(replay_target)
        .max(MIN_PARTITIONS)
        .next_power_of_two();
    (needed <= MAX_PARTITIONS).then_some(needed)
}

/// Low-memory sibling of the resident duplicate detector.  The first pass
/// writes fixed-size records partitioned by the primary fingerprint. Equal
/// SQL keys therefore always land together. Each file is replayed through the
/// same radix + independent-secondary-fingerprint proof used by the resident
/// path, while only one partition is charged and resident at a time.
pub(super) fn build_spilled_duplicate_candidates(
    db: &Arc<Database>,
    first_pass: &mut dyn BatchOperator,
    input_schema: &Arc<BatchSchema>,
    group_columns: &[usize],
    k: usize,
    partitions: usize,
    worker_count: usize,
) -> Result<(Vec<Batch>, mem_budget::ScopedCharge, u64), QueryError> {
    debug_assert!(partitions.is_power_of_two());
    let widths = input_schema
        .fields()
        .iter()
        .map(|field| CompactIntegerColumn::width_for_type(field.column_type))
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| {
            crate::storage::error(
                crate::error::catalog::ER_INTERNAL_ERROR,
                "duplicate-aware aggregate spill received a non-integer column",
            )
        })?;
    let record_width = 5usize.saturating_add(widths.iter().sum::<usize>());
    if record_width > DUPLICATE_SPILL_RECORD_MAX_BYTES {
        return Err(crate::storage::error(
            crate::error::catalog::ER_INTERNAL_ERROR,
            "duplicate-aware aggregate spill record is wider than its fixed buffer",
        ));
    }

    let spill_dir =
        crate::engine::executor::select::grace::SpillTmpDir::create_kind(db.data_dir(), "hashagg")?;
    let paths = (0..partitions)
        .map(|partition| {
            spill_dir
                .path()
                .join(format!("duplicate-{partition:03}.bin"))
        })
        .collect::<Vec<_>>();
    let writer_capacity = crate::engine::executor::spill_quota::writer_buffer_capacity(partitions);
    let mut writers = paths
        .iter()
        .map(|path| {
            crate::engine::executor::spill_quota::TrackedSpillWriter::create_new(
                path,
                spill_dir.charger(),
                writer_capacity,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut record = [0u8; DUPLICATE_SPILL_RECORD_MAX_BYTES];
    while let Some(batch) = first_pass.next_batch()? {
        crate::engine::cancel::check_cancelled()?;
        for row in batch.visible_indices() {
            let fingerprint = fixed_integer_group_fingerprint(&batch, group_columns, row)
                .ok_or_else(|| {
                    crate::storage::error(
                        crate::error::catalog::ER_INTERNAL_ERROR,
                        "duplicate-aware aggregate spill requires fixed-width integer keys",
                    )
                })?;
            record[..4].copy_from_slice(&fingerprint.to_le_bytes());
            let mut null_mask = 0u8;
            let mut offset = 5usize;
            for (column_index, (&width, column)) in widths.iter().zip(batch.columns()).enumerate() {
                if column.is_null(row) {
                    null_mask |= 1 << column_index;
                }
                let raw = match column {
                    ColumnVector::I64 { data, .. } => (data[row] as u64).to_le_bytes(),
                    ColumnVector::U64 { data, .. } => data[row].to_le_bytes(),
                    _ => {
                        return Err(crate::storage::error(
                            crate::error::catalog::ER_INTERNAL_ERROR,
                            "duplicate-aware aggregate spill integer lane changed after preflight",
                        ));
                    }
                };
                record[offset..offset + width].copy_from_slice(&raw[..width]);
                offset += width;
            }
            record[4] = null_mask;
            let partition = fingerprint as usize & (partitions - 1);
            writers[partition]
                .write_all(&record[..record_width])
                .map_err(duplicate_spill_io_error)?;
        }
    }
    let mut spill_bytes = 0u64;
    for writer in writers {
        spill_bytes = spill_bytes.saturating_add(writer.finish()?);
    }
    #[cfg(test)]
    DUPLICATE_AWARE_AGG_TOPN_SPILLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    let mut batches = Vec::new();
    let mut candidate_charge = mem_budget::ScopedCharge::new(0);
    for path in &paths {
        crate::engine::cancel::check_cancelled()?;
        let reader_capacity = crate::engine::executor::spill_quota::writer_buffer_capacity(1);
        let mut reader = crate::engine::executor::spill_quota::TrackedSpillReader::open(
            path,
            spill_dir.charger(),
            reader_capacity,
        )?;
        let file_bytes = reader
            .file()
            .metadata()
            .map_err(duplicate_spill_io_error)?
            .len() as usize;
        if file_bytes % record_width != 0 {
            return Err(crate::storage::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "duplicate-aware aggregate spill file is truncated",
            ));
        }
        let rows = file_bytes / record_width;
        if rows == 0 {
            continue;
        }
        if rows > u32::MAX as usize {
            return Err(crate::storage::error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "duplicate-aware aggregate spill partition exceeds ordinal capacity",
            ));
        }
        let replay_bytes = rows
            .saturating_mul(
                std::mem::size_of::<u64>()
                    .saturating_mul(2)
                    .saturating_add(widths.iter().sum::<usize>())
                    .saturating_add(1),
            )
            .saturating_add(
                worker_count
                    .max(1)
                    .saturating_mul(1usize << 16)
                    .saturating_mul(std::mem::size_of::<usize>()),
            );
        let _replay_charge = mem_budget::ScopedCharge::new_strict(
            replay_bytes,
            "duplicate-aware aggregate spill replay partition",
        )?;
        let mut compact_columns = input_schema
            .fields()
            .iter()
            .map(|field| CompactIntegerColumn::new(field, rows))
            .collect::<Result<Vec<_>, _>>()?;
        let mut null_masks =
            mem_budget::try_vec_with_capacity(rows, "duplicate-aware aggregate spill null masks")?;
        let mut records = mem_budget::try_vec_with_capacity(
            rows,
            "duplicate-aware aggregate spill fingerprint ordinals",
        )?;
        for ordinal in 0..rows {
            reader
                .read_exact(&mut record[..record_width])
                .map_err(duplicate_spill_io_error)?;
            let fingerprint = u32::from_le_bytes(record[..4].try_into().unwrap());
            records.push((u64::from(fingerprint) << 32) | ordinal as u64);
            null_masks.push(record[4]);
            let mut offset = 5usize;
            for (compact, &width) in compact_columns.iter_mut().zip(&widths) {
                compact.push_raw(&record[offset..offset + width]);
                offset += width;
            }
        }
        drop(reader);
        radix_sort_fingerprint_records(&mut records, worker_count)?;
        let (selected, mut selected_charge) = select_duplicate_fingerprint_ordinals(
            &mut records,
            k,
            input_schema.len(),
            |ordinal| {
                compact_integer_group_fingerprint(
                    &compact_columns,
                    &null_masks,
                    group_columns,
                    ordinal as usize,
                )
            },
        )?;
        for selected_rows in selected.chunks(DEFAULT_BATCH_ROWS) {
            let columns = compact_columns
                .iter()
                .enumerate()
                .map(|(column, compact)| compact.gather(selected_rows, &null_masks, 1 << column))
                .collect();
            batches.push(Batch::new(input_schema.clone(), columns));
        }
        candidate_charge.absorb(&mut selected_charge);
    }
    drop(spill_dir);
    Ok((batches, candidate_charge, spill_bytes))
}

impl DuplicateAwareAggregateBatchOperator<'_> {
    pub(super) fn restart_with_full_hash_aggregate(&mut self) -> Result<(), QueryError> {
        let Some(input) =
            try_native_vec_input_stream(self.db, self.txn, self.first_pass_plan.as_ref())?
        else {
            return Err(crate::storage::error(
                crate::error::catalog::ER_INTERNAL_ERROR,
                "duplicate-aware aggregate fallback input capability changed",
            ));
        };
        if input.template.len() != self.input_template.len() {
            return Err(crate::storage::error(
                crate::error::catalog::ER_INTERNAL_ERROR,
                "duplicate-aware aggregate fallback input schema changed",
            ));
        }
        let Some(built) = build_vec_hash_agg_stream_from_input(
            self.db,
            input,
            &self.group_by,
            &self.aggregates,
            false,
            self.worker_count,
            None,
            None,
        )?
        else {
            return Err(crate::storage::error(
                crate::error::catalog::ER_INTERNAL_ERROR,
                "duplicate-aware aggregate fallback capability changed",
            ));
        };
        record_pending_agg_metrics(built.metrics);
        forward_vec_hash_agg_metrics(self.aggregate_plan);
        self.output = Some(built.stream);
        Ok(())
    }

    pub(super) fn initialize(&mut self) -> Result<(), QueryError> {
        if self.output.is_some() {
            return Ok(());
        }
        DUPLICATE_AWARE_AGG_TOPN_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let Some(VecBatchInput {
            stream: mut first_pass,
            template: first_template,
        }) = try_native_vec_input_stream(self.db, self.txn, self.first_pass_plan.as_ref())?
        else {
            return Err(crate::storage::error(
                crate::error::catalog::ER_INTERNAL_ERROR,
                "duplicate-aware aggregate input capability changed after preflight",
            ));
        };
        if first_template.len() != self.input_template.len() {
            return Err(crate::storage::error(
                crate::error::catalog::ER_INTERNAL_ERROR,
                "duplicate-aware aggregate input schema changed after preflight",
            ));
        }
        if first_template.len() > u8::BITS as usize {
            return Err(crate::storage::error(
                crate::error::catalog::ER_INTERNAL_ERROR,
                "duplicate-aware aggregate compact input exceeds eight columns",
            ));
        }
        let input_schema = template_to_batch_schema(&first_template);
        let (batches, _candidate_charge, spill_bytes) = if let Some(partitions) =
            self.spill_partitions
        {
            let result = build_spilled_duplicate_candidates(
                self.db,
                first_pass.as_mut(),
                &input_schema,
                &self.group_columns,
                self.k,
                partitions,
                self.worker_count,
            )?;
            drop(first_pass);
            result
        } else {
            let mut compact_columns = input_schema
                .fields()
                .iter()
                .map(|field| CompactIntegerColumn::new(field, self.fingerprint_capacity))
                .collect::<Result<Vec<_>, _>>()?;
            let mut null_masks = mem_budget::try_vec_with_capacity(
                self.fingerprint_capacity,
                "duplicate-aware aggregate compact null masks",
            )?;
            let mut records = mem_budget::try_vec_with_capacity(
                self.fingerprint_capacity,
                "duplicate-aware aggregate fingerprint ordinals",
            )?;
            let mut restart_with_spill = false;
            while let Some(batch) = first_pass.next_batch()? {
                crate::engine::cancel::check_cancelled()?;
                let required_rows = records.len().saturating_add(batch.visible_rows());
                if required_rows > self.fingerprint_capacity {
                    let grew = try_grow_duplicate_resident_capacity(
                        &mut compact_columns,
                        &mut null_masks,
                        &mut records,
                        &mut self.fingerprint_capacity,
                        required_rows,
                        self.compact_charge
                            .as_mut()
                            .expect("resident duplicate aggregate has compact reservation"),
                        self.radix_charge
                            .as_mut()
                            .expect("resident duplicate aggregate has radix reservation"),
                    )?;
                    if !grew {
                        restart_with_spill = true;
                        #[cfg(test)]
                        DUPLICATE_AWARE_AGG_TOPN_RESTART_SPILLS
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        break;
                    }
                }
                for row in batch.visible_indices() {
                    let fingerprint =
                        fixed_integer_group_fingerprint(&batch, &self.group_columns, row)
                            .ok_or_else(|| {
                                crate::storage::error(
                            crate::error::catalog::ER_INTERNAL_ERROR,
                            "duplicate-aware aggregate requires fixed-width integer group keys",
                        )
                            })?;
                    let ordinal = records.len() as u32;
                    records.push((u64::from(fingerprint) << 32) | u64::from(ordinal));
                    let mut null_mask = 0u8;
                    for (column_index, compact) in compact_columns.iter_mut().enumerate() {
                        let column = &batch.columns()[column_index];
                        if column.is_null(row) {
                            null_mask |= 1 << column_index;
                        }
                        compact.push(column, row)?;
                    }
                    null_masks.push(null_mask);
                }
            }
            drop(first_pass);

            if restart_with_spill {
                let resident_peak_bytes = duplicate_compact_capacity_bytes(
                    &compact_columns,
                    null_masks.capacity(),
                    records.capacity(),
                )
                .saturating_add(
                    self.fingerprint_capacity
                        .saturating_mul(std::mem::size_of::<u64>()),
                )
                .saturating_add((1usize << 16) * std::mem::size_of::<usize>());
                drop(records);
                drop(compact_columns);
                drop(null_masks);
                self.compact_charge = None;
                self.radix_charge = None;

                let Some(partitions) = duplicate_spill_partition_count(resident_peak_bytes) else {
                    self.restart_with_full_hash_aggregate()?;
                    return Ok(());
                };
                let Some(VecBatchInput {
                    stream: mut retry,
                    template: retry_template,
                }) = try_native_vec_input_stream(self.db, self.txn, self.first_pass_plan.as_ref())?
                else {
                    return Err(crate::storage::error(
                        crate::error::catalog::ER_INTERNAL_ERROR,
                        "duplicate-aware aggregate spill retry capability changed",
                    ));
                };
                if retry_template.len() != self.input_template.len() {
                    return Err(crate::storage::error(
                        crate::error::catalog::ER_INTERNAL_ERROR,
                        "duplicate-aware aggregate spill retry schema changed",
                    ));
                }
                self.spill_partitions = Some(partitions);
                let result = build_spilled_duplicate_candidates(
                    self.db,
                    retry.as_mut(),
                    &input_schema,
                    &self.group_columns,
                    self.k,
                    partitions,
                    self.worker_count,
                );
                drop(retry);
                match result {
                    Ok(result) => result,
                    Err(error) if error.is(crate::error::catalog::ER_OUTOFMEMORY) => {
                        if let Err(terminal) = mem_budget::check_terminal_oom() {
                            return Err(terminal);
                        }
                        self.restart_with_full_hash_aggregate()?;
                        return Ok(());
                    }
                    Err(error) => return Err(error),
                }
            } else {
                radix_sort_fingerprint_records(&mut records, self.worker_count)?;
                // The radix scratch buffer is gone when the helper returns. Release
                // its reservation before sizing candidate batches so the two phases'
                // non-overlapping capacities are never double-counted.
                self.radix_charge = None;
                let (selected, selected_charge) = select_duplicate_fingerprint_ordinals(
                    &mut records,
                    self.k,
                    input_schema.len(),
                    |ordinal| {
                        compact_integer_group_fingerprint(
                            &compact_columns,
                            &null_masks,
                            &self.group_columns,
                            ordinal as usize,
                        )
                    },
                )?;
                let mut batches = mem_budget::try_vec_with_capacity(
                    selected.len().div_ceil(DEFAULT_BATCH_ROWS),
                    "duplicate-aware aggregate reduced batch list",
                )?;
                for rows in selected.chunks(DEFAULT_BATCH_ROWS) {
                    let columns = compact_columns
                        .iter()
                        .enumerate()
                        .map(|(column, compact)| compact.gather(rows, &null_masks, 1 << column))
                        .collect();
                    batches.push(Batch::new(input_schema.clone(), columns));
                }
                drop(records);
                drop(compact_columns);
                drop(null_masks);
                drop(selected);
                self.compact_charge = None;
                (batches, selected_charge, 0)
            }
        };
        if spill_bytes > 0 {
            analyze_stats::add_spill_bytes(self.aggregate_plan, spill_bytes);
            analyze_stats::add_spill_shape(
                self.aggregate_plan,
                1,
                1,
                self.spill_partitions.unwrap_or(1) as u64,
                "memory_limit",
            );
        }
        let reduced = VecBatchInput {
            stream: Box::new(MaterializedBatchOperator::new(input_schema, batches)),
            template: first_template,
        };
        let Some(built) = build_vec_hash_agg_stream_from_input(
            self.db,
            reduced,
            &self.group_by,
            &self.aggregates,
            false,
            self.worker_count,
            None,
            None,
        )?
        else {
            return Err(crate::storage::error(
                crate::error::catalog::ER_INTERNAL_ERROR,
                "duplicate-aware aggregate capability changed after preflight",
            ));
        };
        record_pending_agg_metrics(built.metrics);
        forward_vec_hash_agg_metrics(self.aggregate_plan);
        self.output = Some(built.stream);
        Ok(())
    }
}

impl BatchOperator for DuplicateAwareAggregateBatchOperator<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }

    fn properties(&self) -> BatchProperties {
        self.properties.clone()
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        self.initialize()?;
        self.output
            .as_mut()
            .expect("duplicate-aware aggregate output initialized")
            .next_batch()
    }
}

pub(super) fn aggregate_input_without_ordering(plan: &PhysicalPlan) -> &PhysicalPlan {
    match plan {
        PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::IncrementalSort { input, .. }
        | PhysicalPlan::ParallelSort { input, .. } => aggregate_input_without_ordering(input),
        other => other,
    }
}

pub(super) fn narrow_duplicate_first_pass_projection(
    plan: &mut PhysicalPlan,
    group_positions: &[usize],
) -> bool {
    match plan {
        PhysicalPlan::Exchange { input, .. } => {
            narrow_duplicate_first_pass_projection(input, group_positions)
        }
        PhysicalPlan::SeqScan {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::ZoneMapScan {
            table,
            projected_columns,
            ..
        }
        | PhysicalPlan::IndexFastScan {
            table,
            projected_columns,
            ..
        } => {
            let current = projected_columns.as_deref().cloned().unwrap_or_else(|| {
                crate::engine::executor::select::plan::logical_visible_column_indices(&table.schema)
            });
            let Some(narrowed) = group_positions
                .iter()
                .map(|&position| current.get(position).copied())
                .collect::<Option<Vec<_>>>()
            else {
                return false;
            };
            *projected_columns = Some(Arc::new(narrowed));
            true
        }
        PhysicalPlan::ParallelSeqScan {
            table,
            projected_columns,
            output_columns_in_projection,
            ..
        }
        | PhysicalPlan::ParallelZoneMapScan {
            table,
            projected_columns,
            output_columns_in_projection,
            ..
        } => {
            let current = projected_columns.as_deref().cloned().unwrap_or_else(|| {
                crate::engine::executor::select::plan::logical_visible_column_indices(&table.schema)
            });
            let Some(narrowed) = group_positions
                .iter()
                .map(|&position| current.get(position).copied())
                .collect::<Option<Vec<_>>>()
            else {
                return false;
            };
            *projected_columns = Some(Arc::new(narrowed));
            *output_columns_in_projection = None;
            true
        }
        PhysicalPlan::ParallelIndexFastScan {
            table,
            projected_columns,
            ..
        } => {
            let current = projected_columns.as_deref().cloned().unwrap_or_else(|| {
                crate::engine::executor::select::plan::logical_visible_column_indices(&table.schema)
            });
            let Some(narrowed) = group_positions
                .iter()
                .map(|&position| current.get(position).copied())
                .collect::<Option<Vec<_>>>()
            else {
                return false;
            };
            *projected_columns = Some(Arc::new(narrowed));
            true
        }
        _ => false,
    }
}

pub(super) fn fixed_integer_group_type(column_type: ColumnType) -> bool {
    matches!(
        column_type,
        ColumnType::Tiny
            | ColumnType::Short
            | ColumnType::MediumInt
            | ColumnType::Long
            | ColumnType::LongLong
    )
}

pub(super) fn count_topk_scan_supported(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::Exchange { input, .. } => count_topk_scan_supported(input),
        PhysicalPlan::SeqScan {
            runtime_filters, ..
        }
        | PhysicalPlan::ParallelSeqScan {
            runtime_filters, ..
        } => runtime_filters.is_empty(),
        PhysicalPlan::IndexFastScan {
            low, high, filter, ..
        }
        | PhysicalPlan::ParallelIndexFastScan {
            low, high, filter, ..
        } => low.is_none() && high.is_none() && filter.is_none(),
        _ => false,
    }
}

pub(super) fn count_topk_temporal_domain(program: &ExprProgram) -> Option<Vec<Value>> {
    use crate::engine::expr_program::TemporalExtractKind;
    let (first, last) = match program.as_temporal_extract()?.0 {
        TemporalExtractKind::Month => (0, 12),
        TemporalExtractKind::Day => (0, 31),
        TemporalExtractKind::Hour => (0, 23),
        TemporalExtractKind::Minute | TemporalExtractKind::Second => (0, 59),
        TemporalExtractKind::Quarter => (0, 4),
        TemporalExtractKind::Year => return None,
    };
    Some((first..=last).map(Value::Int).collect())
}

pub(super) fn try_build_stats_proven_count_topk_input<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    aggregate_plan: &'a PhysicalPlan,
    order_by: &[OrderBy],
    keys: &TopNKeyPlan,
    k: usize,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    if k == 0 || k > 4096 || order_by.len() != 1 || order_by[0].asc {
        return Ok(None);
    }
    let Some(aggregate) = aggregate_plan.ordinary_aggregate_input() else {
        return Ok(None);
    };
    let (raw_input, group_by, having, aggregates, worker_count) = (
        aggregate.input,
        aggregate.group_by,
        aggregate.having,
        aggregate.aggregates,
        aggregate.workers,
    );
    let aggregate_rows = crate::planner::estimated_rows(aggregate_plan);
    let distinct_arg = match aggregates {
        [Expr::Function {
            name,
            args,
            distinct: false,
        }] if name.eq_ignore_ascii_case("COUNT") && matches!(args.as_slice(), [Expr::Wildcard]) => {
            None
        }
        [Expr::Function {
            name,
            args,
            distinct: true,
        }] if name.eq_ignore_ascii_case("COUNT") && args.len() == 1 => Some(&args[0]),
        _ => return Ok(None),
    };
    if having.is_some()
        || group_by.is_empty()
        || group_by.len() > 4
        || crate::engine::executor::select::aggregate::aggregate_key(&order_by[0].expr)
            != crate::engine::executor::select::aggregate::aggregate_key(&aggregates[0])
    {
        return Ok(None);
    }
    let input_plan = aggregate_input_without_ordering(raw_input);
    if !count_topk_scan_supported(input_plan) {
        return Ok(None);
    }
    let input_rows = crate::planner::estimated_rows(input_plan);
    let input_template = plan_output_template(input_plan);
    let input_schema = template_to_batch_schema(&input_template);
    let Some((group_columns, programs, numeric_programs, computed_fields)) =
        compile_group_columns(group_by, &input_template, &input_schema)
    else {
        return Ok(None);
    };
    if group_columns.len() != group_by.len() {
        return Ok(None);
    }
    if programs.is_empty()
        && group_by.len() > 1
        && input_rows.is_finite()
        && input_rows >= 1_000_000.0
        && aggregate_rows.is_finite()
        && aggregate_rows / input_rows >= 0.875
    {
        // Near-unique direct composite dimensions rarely have their hottest
        // *tuple* in the Cartesian product of marginal ANALYZE heavy hitters.
        return Ok(None);
    }
    if !programs.is_empty() && input_rows >= 1_000_000.0 {
        // The computed-key proof must both materialize the expression and keep
        // a wide conservative sketch. At large scale its speculative pass can
        // cost more than the compact one-pass aggregate even when successful;
        // keep it for bounded relations and prefer the exact path here.
        return Ok(None);
    }
    let mut proof_fields = input_schema.fields().to_vec();
    proof_fields.extend(computed_fields);
    let extended_proof_schema = Arc::new(BatchSchema::new(proof_fields));
    let fields = group_columns
        .iter()
        .map(|&column| {
            extended_proof_schema
                .fields()
                .get(column)
                .map(batch_field_template)
        })
        .collect::<Option<Vec<_>>>();
    let Some(fields) = fields else {
        return Ok(None);
    };
    let distinct_source_column = if let Some(argument) = distinct_arg {
        let Some(column) = expr_column_index_in_template(argument, &input_template) else {
            return Ok(None);
        };
        if input_template
            .get(column)
            .is_none_or(|field| !fixed_integer_group_type(field.column_type))
        {
            return Ok(None);
        }
        Some(column)
    } else {
        None
    };
    let integer_keys = distinct_source_column.is_none()
        && fields.len() == 1
        && fixed_integer_group_type(fields[0].column_type);
    let supported_composite = fields.iter().all(|field| {
        fixed_integer_group_type(field.column_type)
            || (matches!(field.column_type, ColumnType::VarString | ColumnType::Blob)
                && field.source.collation_id != Some(crate::charset::BINARY_COLLATION_ID))
    }) && fields
        .iter()
        .any(|field| matches!(field.column_type, ColumnType::VarString | ColumnType::Blob));
    if !integer_keys && !supported_composite {
        return Ok(None);
    }
    // The integer proof has a bounded discovery prefix and an exact scan.
    // Keep small relations on ordinary aggregation, where setup and a possible
    // fallback cost more than compact grouping and can perturb its
    // established tie order.
    const MIN_INTEGER_PROOF_INPUT_ROWS: f64 = 65_536.0;
    if integer_keys && crate::planner::estimated_rows(input_plan) < MIN_INTEGER_PROOF_INPUT_ROWS {
        return Ok(None);
    }
    let tables = crate::planner::collect_physical_plan_tables(input_plan);
    let [table] = tables.as_slice() else {
        return Ok(None);
    };
    let mut per_column_candidates = Vec::with_capacity(fields.len());
    for (&group_column, field) in group_columns.iter().zip(&fields) {
        let values = if group_column >= input_template.len() {
            let program = programs.get(group_column - input_template.len());
            let Some(values) = program.and_then(|program| count_topk_temporal_domain(program))
            else {
                return Ok(None);
            };
            values
        } else {
            // With one exact low-domain computed key, spend the bounded
            // Cartesian candidate budget on numeric heavy hitters and only a
            // handful of text sentinels (empty/default categories dominate
            // many dimensional schemas). 64 * 60 * 4 remains below the same
            // 2^16 proof cap while covering substantially more hot ids than a
            // uniform 16 * 60 * 16 split.
            let limit = if programs.is_empty() {
                usize::MAX
            } else if fixed_integer_group_type(field.column_type) {
                64
            } else {
                4
            };
            table
                .schema
                .stats
                .columns
                .iter()
                .find_map(|(name, stats)| name.eq_ignore_ascii_case(&field.column).then_some(stats))
                .into_iter()
                .flat_map(|stats| stats.top_values.iter())
                .filter_map(|frequency| {
                    let compatible = if fixed_integer_group_type(field.column_type) {
                        matches!(frequency.value, Value::Int(_) | Value::UInt(_))
                    } else {
                        matches!(frequency.value, Value::String(_) | Value::SmallString(_))
                    };
                    compatible.then(|| frequency.value.clone())
                })
                .take(limit)
                .collect::<Vec<_>>()
        };
        per_column_candidates.push(values);
    }
    let candidate_rows = if integer_keys {
        per_column_candidates
            .pop()
            .unwrap_or_default()
            .into_iter()
            .map(|value| vec![value])
            .collect::<Vec<_>>()
    } else {
        const MAX_COMPOSITE_CANDIDATES: usize = 1 << 16;
        let candidate_count = per_column_candidates
            .iter()
            .try_fold(1usize, |count, values| count.checked_mul(values.len()));
        let Some(candidate_count) =
            candidate_count.filter(|count| *count >= k && *count <= MAX_COMPOSITE_CANDIDATES)
        else {
            return Ok(None);
        };
        let mut rows = vec![Vec::with_capacity(fields.len())];
        for values in per_column_candidates {
            let mut expanded = Vec::with_capacity(rows.len().saturating_mul(values.len()));
            for row in &rows {
                for value in &values {
                    let mut next = row.clone();
                    next.push(value.clone());
                    expanded.push(next);
                }
            }
            rows = expanded;
        }
        debug_assert_eq!(rows.len(), candidate_count);
        rows
    };

    let proof_workers = count_topk_proof_workers(worker_count);
    let cms_bytes = if integer_keys {
        integer_count_topk_proof_bytes(
            proof_workers,
            if integer_count_topk_heap_scan(input_plan) {
                INTEGER_COUNT_TOPK_CMS_WIDTH
            } else {
                INDEX_COUNT_TOPK_CMS_WIDTH
            },
        )
    } else {
        proof_workers
            .saturating_mul(COUNT_TOPK_CMS_DEPTH)
            .saturating_mul(COUNT_TOPK_CMS_WIDTH)
            .saturating_mul(std::mem::size_of::<u32>())
    };
    let Ok(cms_charge) =
        mem_budget::ScopedCharge::new_strict(cms_bytes, "stats-proven COUNT Top-K Count-Min table")
    else {
        return Ok(None);
    };
    let candidate_bytes_per_copy = candidate_rows
        .iter()
        .flatten()
        .fold(0usize, |bytes, value| {
            bytes.saturating_add(mem_budget::estimate_value_bytes(value).saturating_mul(3))
        });
    // The coordinator retains canonical/raw maps and every proof worker owns
    // an immutable clone. Charge all map/key capacities up front; release only
    // after the workers have joined and their maps are gone.
    let candidate_bytes = if integer_keys {
        candidate_bytes_per_copy.saturating_add(
            candidate_rows.len().saturating_mul(
                std::mem::size_of::<(Option<i128>, usize)>()
                    .saturating_add(17)
                    .saturating_add(proof_workers.saturating_mul(std::mem::size_of::<u64>())),
            ),
        )
    } else {
        candidate_bytes_per_copy
            .saturating_add(candidate_rows.len().saturating_mul(256))
            .saturating_mul(proof_workers.saturating_add(2))
    };
    let Ok(mut candidate_charge) = mem_budget::ScopedCharge::new_strict(
        candidate_bytes,
        "stats-proven COUNT Top-K candidates",
    ) else {
        return Ok(None);
    };
    let (first_pass_plan, proof_group_columns, distinct_input_column, proof_schema, first_pass_len) =
        if programs.is_empty() {
            let mut required_positions = group_columns.clone();
            let distinct_input_column = distinct_source_column.map(|column| {
                if let Some(position) = required_positions.iter().position(|&item| item == column) {
                    position
                } else {
                    required_positions.push(column);
                    required_positions.len() - 1
                }
            });
            let mut first_pass_plan = input_plan.clone();
            if !narrow_duplicate_first_pass_projection(&mut first_pass_plan, &required_positions) {
                return Ok(None);
            }
            let proof_schema = Arc::new(BatchSchema::new(
                required_positions
                    .iter()
                    .map(|&position| input_schema.fields()[position].clone())
                    .collect(),
            ));
            (
                first_pass_plan,
                (0..group_columns.len()).collect(),
                distinct_input_column,
                proof_schema,
                required_positions.len(),
            )
        } else {
            (
                input_plan.clone(),
                group_columns,
                distinct_source_column,
                extended_proof_schema,
                input_template.len(),
            )
        };
    if distinct_input_column.is_some() {
        // At most every qualifying input row contributes one distinct integer
        // entry. Reserve a conservative hash-table capacity envelope before
        // the optional proof starts; no proof allocation may cross the global
        // memory target, and refusal keeps the ordinary spillable aggregate.
        let max_rows = crate::planner::estimated_rows(input_plan).ceil().max(0.0) as usize;
        let state_headers = candidate_rows
            .len()
            .saturating_mul(proof_workers)
            .saturating_mul(std::mem::size_of::<
                hashbrown::HashSet<i128, ahash::RandomState>,
            >());
        let distinct_bytes = max_rows.saturating_mul(48).saturating_add(state_headers);
        if candidate_charge.add(distinct_bytes).is_err() {
            return Ok(None);
        }
    }
    let compact_template = fields.clone();
    let schema = template_to_batch_schema(&keys.template);
    if schema.len() != fields.len().saturating_add(1) {
        return Ok(None);
    }
    Ok(Some(VecBatchInput {
        stream: Box::new(StatsProvenCountTopKBatchOperator {
            db,
            txn,
            aggregate_plan,
            input_plan,
            first_pass_plan: Box::new(first_pass_plan),
            group_by: group_by.to_vec(),
            aggregates: aggregates.to_vec(),
            candidate_rows,
            integer_keys,
            collation_ids: fields
                .iter()
                .map(|field| field.source.collation_id)
                .collect(),
            proof_group_columns,
            proof_row_major: programs.len() > 1
                && programs.iter().any(|program| {
                    let capability = program.capability();
                    capability.may_error
                        || !capability.deterministic
                        || capability.depends_on_session
                }),
            proof_programs: programs,
            proof_numeric_programs: numeric_programs,
            proof_schema,
            first_pass_template_len: first_pass_len,
            distinct_input_column,
            schema,
            input_template: compact_template,
            properties: keys.properties.clone(),
            worker_count,
            k,
            cms_charge: Some(cms_charge),
            candidate_charge: Some(candidate_charge),
            fallback_output_charge: None,
            output: None,
        }),
        template: keys.template.clone(),
    }))
}

/// Preflight the exact high-NDV duplicate detector before either scan is
/// opened.  Admission is deliberately conservative: COUNT(*) must be the
/// sole descending ordering key, grouping keys must be direct fixed-width
/// integer columns, and analyzed estimates must say at least 7/8 of input
/// rows form distinct groups.  A failed memory reservation simply retains the
/// existing spill-capable Aggregate -> TopN implementation.
pub(super) fn try_build_duplicate_aware_aggregate_input<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    aggregate_plan: &'a PhysicalPlan,
    order_by: &[OrderBy],
    keys: &TopNKeyPlan,
    k: usize,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    const MIN_INPUT_ROWS: f64 = 65_536.0;
    const MIN_DISTINCT_RATIO: f64 = 0.875;
    const MAX_K: usize = 4_096;
    const CAPACITY_SLACK_NUMERATOR: usize = 11;
    const CAPACITY_SLACK_DENOMINATOR: usize = 10;

    if k == 0 || k > MAX_K || order_by.len() != 1 || order_by[0].asc {
        return Ok(None);
    }
    let Some(aggregate) = aggregate_plan.ordinary_aggregate_input() else {
        return Ok(None);
    };
    let (raw_input, group_by, having, aggregates, worker_count) = (
        aggregate.input,
        aggregate.group_by,
        aggregate.having,
        aggregate.aggregates,
        aggregate.workers,
    );
    let output_rows = crate::planner::estimated_rows(aggregate_plan);
    if having.is_some() || group_by.is_empty() || aggregates.is_empty() {
        return Ok(None);
    }
    let order_key = crate::engine::executor::select::aggregate::aggregate_key(&order_by[0].expr);
    if !aggregates.iter().any(|aggregate| {
        crate::engine::executor::select::aggregate::aggregate_key(aggregate) == order_key
            && matches!(
                aggregate,
                Expr::Function {
                    name,
                    args,
                    distinct: false,
                } if name.eq_ignore_ascii_case("COUNT")
                    && matches!(args.as_slice(), [Expr::Wildcard])
            )
    }) {
        return Ok(None);
    }

    // A grouping sort changes no rows and is unnecessary for the duplicate
    // detector or its reduced HashAggregate.  Peeling it is what prevents a
    // Sort+StreamingAggregate plan from paying for both blocking operators.
    let input_plan = aggregate_input_without_ordering(raw_input);
    let input_rows = crate::planner::estimated_rows(input_plan);
    if !input_rows.is_finite()
        || input_rows < MIN_INPUT_ROWS
        || !output_rows.is_finite()
        || output_rows / input_rows < MIN_DISTINCT_RATIO
    {
        return Ok(None);
    }

    let input_template = plan_output_template(input_plan);
    let input_schema = template_to_batch_schema(&input_template);
    let Some((group_columns, programs, _numeric, _fields)) =
        compile_group_columns(group_by, &input_template, &input_schema)
    else {
        return Ok(None);
    };
    if !programs.is_empty()
        || group_columns.iter().any(|&column| {
            input_template
                .get(column)
                .is_none_or(|field| !fixed_integer_group_type(field.column_type))
        })
    {
        return Ok(None);
    }
    let mut required_positions = group_columns.clone();
    for aggregate in aggregates {
        let Some((kind, input, _)) =
            translate_aggregate_extended(aggregate, &input_template, &input_schema)
        else {
            return Ok(None);
        };
        if aggregate_requires_semantic_input_coercion(aggregate, &input_template, &input_schema) {
            return Ok(None);
        }
        if kind == vec_hash_agg::AggregateKind::CountStar {
            continue;
        }
        let AggInputSpec::Column(column) = input else {
            return Ok(None);
        };
        if input_template
            .get(column)
            .is_none_or(|field| !fixed_integer_group_type(field.column_type))
        {
            return Ok(None);
        }
        if !required_positions.contains(&column) {
            required_positions.push(column);
        }
    }
    if required_positions.len() > u8::BITS as usize {
        return Ok(None);
    }
    let compact_template = required_positions
        .iter()
        .map(|&position| input_template[position].clone())
        .collect::<Vec<_>>();
    let compact_schema = template_to_batch_schema(&compact_template);
    let Some((compact_group_columns, compact_programs, _, _)) =
        compile_group_columns(group_by, &compact_template, &compact_schema)
    else {
        return Ok(None);
    };
    if !compact_programs.is_empty()
        || aggregates.iter().any(|aggregate| {
            translate_aggregate_extended(aggregate, &compact_template, &compact_schema).is_none()
        })
    {
        return Ok(None);
    }

    let estimated = (input_rows.ceil() as usize)
        .saturating_mul(CAPACITY_SLACK_NUMERATOR)
        .div_ceil(CAPACITY_SLACK_DENOMINATOR)
        .saturating_add(DEFAULT_BATCH_ROWS * 2);
    let compact_row_bytes = compact_template
        .iter()
        .map(|field| CompactIntegerColumn::width_for_type(field.column_type).unwrap_or(8))
        .sum::<usize>()
        .saturating_add(1);
    let compact_bytes =
        estimated.saturating_mul(std::mem::size_of::<u64>().saturating_add(compact_row_bytes));
    let radix_bytes = estimated
        .saturating_mul(std::mem::size_of::<u64>())
        .saturating_add(
            worker_count
                .max(1)
                .saturating_mul(1usize << 16)
                .saturating_mul(std::mem::size_of::<usize>()),
        );
    let (compact_charge, radix_charge, spill_partitions) =
        match mem_budget::ScopedCharge::new_strict(
            compact_bytes,
            "duplicate-aware aggregate compact rows",
        ) {
            Ok(compact_charge) => match mem_budget::ScopedCharge::new_strict(
                radix_bytes,
                "duplicate-aware aggregate radix scratch",
            ) {
                Ok(radix_charge) => (Some(compact_charge), Some(radix_charge), None),
                Err(_) => {
                    drop(compact_charge);
                    let Some(partitions) =
                        duplicate_spill_partition_count(compact_bytes.saturating_add(radix_bytes))
                    else {
                        return Ok(None);
                    };
                    (None, None, Some(partitions))
                }
            },
            Err(_) => {
                let Some(partitions) =
                    duplicate_spill_partition_count(compact_bytes.saturating_add(radix_bytes))
                else {
                    return Ok(None);
                };
                (None, None, Some(partitions))
            }
        };

    let mut first_pass_plan = input_plan.clone();
    if !narrow_duplicate_first_pass_projection(&mut first_pass_plan, &required_positions) {
        return Ok(None);
    }

    let schema = template_to_batch_schema(&keys.template);
    Ok(Some(VecBatchInput {
        stream: Box::new(DuplicateAwareAggregateBatchOperator {
            db,
            txn,
            aggregate_plan,
            first_pass_plan: Box::new(first_pass_plan),
            group_by: group_by.to_vec(),
            aggregates: aggregates.to_vec(),
            group_columns: compact_group_columns,
            input_template: compact_template,
            schema,
            properties: keys.properties.clone(),
            worker_count,
            k,
            fingerprint_capacity: estimated,
            compact_charge,
            radix_charge,
            spill_partitions,
            output: None,
        }),
        template: keys.template.clone(),
    }))
}

/// Exact packed key for the frequent `(text dimension attribute, integer id)`
/// GROUP BY shape. Text values receive collation-canonical dictionary tokens;
/// the full i64 bits plus a NULL bit occupy the low 65 bits. This removes one
/// encoded-Vec allocation per high-cardinality group without assuming the
/// text is binary or that the integer is positive.
pub(super) struct CompactHiddenGroupEncoder {
    pub(super) string_column: usize,
    pub(super) integer_column: usize,
    pub(super) raw_to_token: hashbrown::HashMap<Vec<u8>, u64, ahash::RandomState>,
    pub(super) canonical_to_token: hashbrown::HashMap<Vec<u8>, u64, ahash::RandomState>,
}

impl CompactHiddenGroupEncoder {
    pub(super) fn try_new(group_columns: &[usize], batch: &Batch) -> Option<Self> {
        let [first, second] = group_columns else {
            return None;
        };
        let (string_column, integer_column) =
            match (batch.columns().get(*first)?, batch.columns().get(*second)?) {
                (ColumnVector::Utf8 { .. }, ColumnVector::I64 { .. }) => (*first, *second),
                (ColumnVector::I64 { .. }, ColumnVector::Utf8 { .. }) => (*second, *first),
                _ => return None,
            };
        Some(Self {
            string_column,
            integer_column,
            raw_to_token: hashbrown::HashMap::with_hasher(ahash::RandomState::new()),
            canonical_to_token: hashbrown::HashMap::with_hasher(ahash::RandomState::new()),
        })
    }

    pub(super) fn map_structural_bytes(capacity: usize) -> usize {
        capacity.saturating_mul(std::mem::size_of::<Vec<u8>>() + std::mem::size_of::<u64>() + 64)
    }

    pub(super) fn encode_row(
        &mut self,
        batch: &Batch,
        row: usize,
        scratch: &mut Vec<u8>,
    ) -> Option<(u128, usize)> {
        let string_vector = batch.columns().get(self.string_column)?;
        let mut dictionary_charge = 0usize;
        let string_token = if string_vector.is_null(row) {
            0u64
        } else {
            let ColumnVector::Utf8 { offsets, data, .. } = string_vector else {
                return None;
            };
            let raw = &data[offsets[row] as usize..offsets[row + 1] as usize];
            if let Some(token) = self.raw_to_token.get(raw).copied() {
                token
            } else {
                let structural_before = Self::map_structural_bytes(self.raw_to_token.capacity())
                    .saturating_add(Self::map_structural_bytes(
                        self.canonical_to_token.capacity(),
                    ));
                scratch.clear();
                crate::engine::executor::vec::key_encode::encode_from_column_row_with_collation(
                    string_vector,
                    row,
                    batch.schema().fields()[self.string_column]
                        .source
                        .collation_id,
                    scratch,
                );
                let raw_owned = raw.to_vec();
                let mut owned_bytes = raw_owned.capacity();
                let token =
                    if let Some(token) = self.canonical_to_token.get(scratch.as_slice()).copied() {
                        token
                    } else {
                        let token = u64::try_from(self.canonical_to_token.len())
                            .ok()?
                            .checked_add(1)?;
                        let canonical = scratch.clone();
                        owned_bytes = owned_bytes.saturating_add(canonical.capacity());
                        self.canonical_to_token.insert(canonical, token);
                        token
                    };
                self.raw_to_token.insert(raw_owned, token);
                let structural_after = Self::map_structural_bytes(self.raw_to_token.capacity())
                    .saturating_add(Self::map_structural_bytes(
                        self.canonical_to_token.capacity(),
                    ));
                let charge =
                    owned_bytes.saturating_add(structural_after.saturating_sub(structural_before));
                dictionary_charge = charge;
                token
            }
        };
        if string_token >= (1u64 << 63) {
            return None;
        }
        let integer_vector = batch.columns().get(self.integer_column)?;
        let (integer_bits, integer_null) = match integer_vector {
            ColumnVector::I64 { .. } if integer_vector.is_null(row) => (0u64, 1u128),
            ColumnVector::I64 { data, .. } => (data[row] as u64, 0u128),
            _ => return None,
        };
        scratch.clear();
        Some((
            u128::from(integer_bits) | (integer_null << 64) | (u128::from(string_token) << 65),
            dictionary_charge,
        ))
    }
}

impl HiddenFirstValueTopNBatchOperator<'_> {
    pub(super) fn group_map_capacity_bytes(
        groups: &hashbrown::HashMap<Vec<u8>, (), ahash::RandomState>,
    ) -> usize {
        groups
            .capacity()
            .saturating_mul(std::mem::size_of::<Vec<u8>>() + 64)
    }

    #[inline]
    pub(super) fn input_column_for_output(&self, output_column: usize) -> usize {
        if output_column < self.group_columns.len() {
            self.group_columns[output_column]
        } else {
            self.first_value_columns[output_column - self.group_columns.len()]
        }
    }

    pub(super) fn initialize(&mut self) -> Result<(), QueryError> {
        if self.output.is_some() {
            return Ok(());
        }
        HIDDEN_FIRST_VALUE_TOPN_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if self.k == 0 {
            self.output = Some(Vec::new().into_iter());
            return Ok(());
        }

        let started = analyze_stats::is_recording().then(std::time::Instant::now);
        let io_start = started.map(|_| self.db.buffer_stats_counts());
        let mut groups: hashbrown::HashMap<Vec<u8>, (), ahash::RandomState> =
            hashbrown::HashMap::with_hasher(ahash::RandomState::new());
        let mut compact_groups: hashbrown::HashSet<u128, ahash::RandomState> =
            hashbrown::HashSet::with_hasher(ahash::RandomState::new());
        let mut fd_groups: hashbrown::HashSet<i64, ahash::RandomState> =
            hashbrown::HashSet::with_hasher(ahash::RandomState::new());
        let mut compact_encoder: Option<CompactHiddenGroupEncoder> = None;
        let mut compact_probed = false;
        let mut key = Vec::with_capacity(64);
        let mut accumulator = vec_topn::TopNAccumulator::new_with_width(
            std::mem::take(&mut self.specs),
            self.k,
            self.schema.fields().len(),
        )?
        .with_collations(std::mem::take(&mut self.collations));
        let direct_sources = self
            .sources
            .iter()
            .all(|source| matches!(source, TopNKeySource::Column(_)));
        let mut input_rows = 0u64;
        let mut input_batches = 0u64;

        while let Some(batch) = self.upstream.next_batch()? {
            crate::engine::cancel::check_cancelled()?;
            input_batches = input_batches.saturating_add(1);
            if !compact_probed {
                compact_probed = true;
                compact_encoder = CompactHiddenGroupEncoder::try_new(&self.group_columns, &batch);
            }
            for row in batch.visible_indices() {
                input_rows = input_rows.saturating_add(1);
                if self.fd_group_unique {
                    HIDDEN_FIRST_VALUE_FD_GROUP_ROWS
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                } else if let Some(column) = self.fd_group_column {
                    HIDDEN_FIRST_VALUE_FD_GROUP_ROWS
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let Some(ColumnVector::I64 { data, nulls }) = batch.columns().get(column)
                    else {
                        return Err(crate::storage::error(
                            crate::error::catalog::ER_INTERNAL_ERROR,
                            "functional dependency GROUP BY key schema changed during execution",
                        ));
                    };
                    if nulls.as_ref().is_some_and(|validity| !validity.get(row)) {
                        return Err(crate::storage::error(
                            crate::error::catalog::ER_INTERNAL_ERROR,
                            "UNIQUE inner-join GROUP BY key became NULL",
                        ));
                    }
                    let before = fd_groups.capacity();
                    if !fd_groups.insert(data[row]) {
                        continue;
                    }
                    self.group_charge.add(
                        fd_groups
                            .capacity()
                            .saturating_sub(before)
                            .saturating_mul(std::mem::size_of::<i64>() + 64),
                    )?;
                } else if let Some(encoder) = compact_encoder.as_mut() {
                    let Some((packed, dictionary_charge)) =
                        encoder.encode_row(&batch, row, &mut key)
                    else {
                        return Err(crate::storage::error(
                            crate::error::catalog::ER_INTERNAL_ERROR,
                            "compact hidden GROUP BY key schema changed during execution",
                        ));
                    };
                    let before = compact_groups.capacity();
                    if !compact_groups.insert(packed) {
                        continue;
                    }
                    let table_growth = compact_groups
                        .capacity()
                        .saturating_sub(before)
                        .saturating_mul(std::mem::size_of::<u128>() + 64);
                    self.group_charge
                        .add(dictionary_charge.saturating_add(table_growth))?;
                } else {
                    self.key_program.encode_row(&batch, row, &mut key);
                    if groups.contains_key(&key) {
                        continue;
                    }
                    let container_before = Self::group_map_capacity_bytes(&groups);
                    let owned_key = key.clone();
                    let key_bytes = owned_key.capacity();
                    groups.insert(owned_key, ());
                    let container_growth =
                        Self::group_map_capacity_bytes(&groups).saturating_sub(container_before);
                    self.group_charge
                        .add(container_growth.saturating_add(key_bytes))?;
                }

                if direct_sources {
                    let sort_keys = self
                        .sources
                        .iter()
                        .map(|source| {
                            let TopNKeySource::Column(output_column) = source else {
                                unreachable!()
                            };
                            batch.columns()[self.input_column_for_output(*output_column)]
                                .value_at(row)
                        })
                        .collect();
                    accumulator.push_lazy(sort_keys, || {
                        self.group_columns
                            .iter()
                            .chain(self.first_value_columns.iter())
                            .map(|&column| batch.columns()[column].value_at(row))
                            .collect()
                    })?;
                } else {
                    let output_values: Vec<Value> = self
                        .group_columns
                        .iter()
                        .chain(self.first_value_columns.iter())
                        .map(|&column| batch.columns()[column].value_at(row))
                        .collect();
                    let view = crate::engine::expr_program::RowView::from_values(&output_values);
                    let sort_keys = self
                        .sources
                        .iter()
                        .map(|source| match source {
                            TopNKeySource::Column(column) => Ok(output_values[*column].clone()),
                            TopNKeySource::Program { program, .. } => {
                                crate::engine::expr_program::with_eval_scratch(|scratch| {
                                    program.eval_row(&view, scratch)
                                })
                            }
                            TopNKeySource::UnseededRand(_) => {
                                unreachable!("preflight rejected RAND")
                            }
                        })
                        .collect::<Result<Vec<_>, QueryError>>()?;
                    accumulator.push_lazy(sort_keys, || output_values)?;
                }
            }
        }

        let unique_groups = groups
            .len()
            .saturating_add(compact_groups.len())
            .saturating_add(fd_groups.len()) as u64;
        if let (Some(started), Some(io_start)) = (started, io_start) {
            analyze_stats::record_with_io_snapshot(
                self.db,
                self.aggregate_plan,
                started.elapsed().as_nanos(),
                unique_groups,
                io_start,
            );
            analyze_stats::add_vector_runtime(self.aggregate_plan, input_batches, 0, 1);
            analyze_stats::add_aggregate_runtime(
                self.aggregate_plan,
                "first_value_topn",
                input_rows,
                0,
                0,
            );
        }
        let (rows, charge) = accumulator.finish_charged()?;
        self.topn_charge = Some(charge);
        let rows = rows.into_iter().skip(self.offset).collect();
        self.output = Some(value_rows_into_batches(self.schema.clone(), rows).into_iter());
        Ok(())
    }
}

impl BatchOperator for HiddenFirstValueTopNBatchOperator<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }

    fn properties(&self) -> BatchProperties {
        self.properties.clone()
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        self.initialize()?;
        Ok(self.output.as_mut().and_then(Iterator::next))
    }
}

impl TopNBatchOperator<'_> {
    pub(super) fn try_initialize_exact_rand_covering_residual(
        &mut self,
    ) -> Result<bool, QueryError> {
        let Some(exact) = &self.exact_rand_index_top_one else {
            return Ok(false);
        };
        let Some(filter) = exact.filter.filter(|_| !exact.filter_covered_by_bounds) else {
            return Ok(false);
        };
        let started = analyze_stats::is_recording().then(std::time::Instant::now);
        let io_start = started.map(|_| exact.db.buffer_stats_counts());
        let projector = crate::engine::executor::select::plan::IndexEntryProjector::new(
            exact.table,
            exact.index,
            exact.projected_columns,
        );
        let raw_filter = CoveringIndexEqPredicate::compile(filter, exact.table, exact.index);
        if let Some(raw_filter) = raw_filter.as_ref() {
            if raw_filter.is_always_false() {
                self.output = Some(Vec::new().into_iter());
                return Ok(true);
            }
            let selected = crate::engine::expr::with_unseeded_mysql_rand_state(|initial| {
                let mut pending_final = None;
                let result = exact
                    .db
                    .select_all_visible_index_range_entry_by_filtered_ordinal(
                        &exact.table.schema,
                        exact.index,
                        exact.low.clone(),
                        exact.high.clone(),
                        raw_filter.terms(),
                        exact.txn,
                        |row_count| {
                            let (ordinal, final_state) =
                                crate::engine::expr::preview_mysql_rand_arg_extreme(
                                    initial,
                                    row_count,
                                    exact.degree,
                                    exact.ascending,
                                );
                            pending_final = Some(final_state);
                            ordinal.expect("non-empty filtered RAND range has an arg-extreme")
                        },
                    );
                let commit = matches!(&result, Ok(Some(_)))
                    .then_some(pending_final)
                    .flatten();
                (result, commit)
            })?;
            if let Some((entry, row_count)) = selected {
                #[cfg(test)]
                EXACT_RAND_INDEX_TOP_ONE_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                crate::engine::executor::select::eval::ORDER_BY_KEY_ROWS_PREPARED
                    .fetch_add(row_count as u64, std::sync::atomic::Ordering::Relaxed);
                crate::engine::executor::select::analyze_stats::add_range_scan_work_for_plan(
                    exact.scan_plan,
                    row_count as u64,
                    0,
                    0,
                );
                let rows = entry
                    .map(|(key, _)| {
                        projector
                            .project(&key)
                            .fields
                            .into_iter()
                            .map(|field| field.value)
                            .collect()
                    })
                    .into_iter()
                    .collect();
                if let (Some(started), Some(io_start)) = (started, io_start) {
                    crate::engine::executor::select::analyze_stats::record_with_io_snapshot(
                        exact.db,
                        exact.scan_plan,
                        started.elapsed().as_nanos(),
                        row_count as u64,
                        io_start,
                    );
                    crate::engine::executor::select::analyze_stats::add_vector_runtime(
                        exact.scan_plan,
                        0,
                        0,
                        1,
                    );
                }
                self.output = Some(value_rows_into_batches(self.schema.clone(), rows).into_iter());
                return Ok(true);
            }
        }

        if exact
            .db
            .has_pending_index_repairs_for(exact.table.schema.id, &exact.index.name)
        {
            return Ok(false);
        }
        let heap = exact.db.cached_heap_handle(exact.table.schema.id)?;
        let filter = Some(filter.clone());
        let mut eval_ctx = crate::engine::executor::select::eval::EvalQueryContext::default();
        let mut compiled_filter = None;
        let mut best: Option<(u64, PlanRow)> = None;
        let mut qualifying_rows = 0usize;
        let mut cursor = exact.db.range_index_snapshot_cursor(
            &exact.table.schema,
            exact.index,
            exact.low.clone(),
            exact.high.clone(),
            exact.txn,
        )?;

        crate::engine::expr::with_unseeded_mysql_rand_seed_stream(|rng| {
            let mut consume = |entries: &[(&crate::storage::IndexKey, crate::storage::RowId)],
                               page_max_trx_id: u64| {
                let visible = exact
                    .db
                    .try_index_page_max_trx_shortcut_for(
                        exact.table.schema.id,
                        &exact.index.name,
                        page_max_trx_id,
                        exact.txn,
                        entries.len(),
                    )
                    .map(Ok::<_, QueryError>)
                    .unwrap_or_else(|| {
                        exact.db.filter_visible_index_entries_refs_mask_with_handle(
                            &heap,
                            &exact.table.schema,
                            &exact.index,
                            entries,
                            exact.txn,
                        )
                    })?;
                for ((key, _row_id), visible) in entries.iter().zip(visible) {
                    if !visible {
                        continue;
                    }
                    let projected = if let Some(raw_filter) = &raw_filter {
                        if !raw_filter.matches(key) {
                            continue;
                        }
                        None
                    } else {
                        let row = projector.project(key);
                        if !crate::engine::executor::select::plan::eval_covering_filter(
                            exact.db,
                            exact.txn,
                            &filter,
                            &row,
                            &mut eval_ctx,
                            &mut compiled_filter,
                        )? {
                            continue;
                        }
                        Some(row)
                    };
                    let rand_key = rng.next_seed();
                    qualifying_rows = qualifying_rows.saturating_add(1);
                    let better = best.as_ref().is_none_or(|(current, _)| {
                        if exact.ascending {
                            rand_key < *current
                        } else {
                            rand_key > *current
                        }
                    });
                    if better {
                        best = Some((
                            rand_key,
                            projected.unwrap_or_else(|| projector.project(key)),
                        ));
                    }
                }
                Ok::<(), QueryError>(())
            };
            cursor.visit_entries_with_bound(&mut consume)
        })?;

        crate::engine::executor::select::eval::ORDER_BY_KEY_ROWS_PREPARED
            .fetch_add(qualifying_rows as u64, std::sync::atomic::Ordering::Relaxed);
        #[cfg(test)]
        EXACT_RAND_INDEX_TOP_ONE_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        crate::engine::executor::select::analyze_stats::add_range_scan_work_for_plan(
            exact.scan_plan,
            qualifying_rows as u64,
            0,
            0,
        );
        if let (Some(started), Some(io_start)) = (started, io_start) {
            crate::engine::executor::select::analyze_stats::record_with_io_snapshot(
                exact.db,
                exact.scan_plan,
                started.elapsed().as_nanos(),
                qualifying_rows as u64,
                io_start,
            );
            crate::engine::executor::select::analyze_stats::add_vector_runtime(
                exact.scan_plan,
                0,
                0,
                1,
            );
        }
        let rows = best
            .map(|(_, row)| row.fields.into_iter().map(|field| field.value).collect())
            .into_iter()
            .collect();
        self.output = Some(value_rows_into_batches(self.schema.clone(), rows).into_iter());
        Ok(true)
    }

    pub(super) fn initialize(&mut self) -> Result<(), QueryError> {
        if self.output.is_some() || self.external_output.is_some() {
            return Ok(());
        }
        if self.k == 0 {
            self.output = Some(Vec::new().into_iter());
            return Ok(());
        }

        // Generic heaps reserve their actual growth internally. Keep only the
        // specialized single-row kernel and bounded producer queues here;
        // output reservations are transferred back from the heap on finish.
        let fixed_row = std::mem::size_of::<Vec<Value>>().saturating_add(
            self.schema
                .fields()
                .len()
                .saturating_mul(std::mem::size_of::<Value>()),
        );
        let parallel_direct_column_topn = self.parallel_workers > 1
            && matches!(
                self.upstream.properties().partitioning,
                BatchPartitioning::RoundRobin { partitions } if partitions > 1
            )
            && !self.row_major_keys
            && self
                .sources
                .iter()
                .all(|source| matches!(source, TopNKeySource::Column(_)));
        let heap_bytes = if parallel_direct_column_topn {
            0 // worker heaps carry their actual reservations through reduction
        } else if self.k == 1 || self.exact_rand_index_top_one.is_some() {
            self.k
                .saturating_mul(fixed_row.max(mem_budget::ESTIMATED_ROW_BYTES))
        } else {
            0
        };
        // The scheduler carries actual batch reservations through its queues.
        self.reservation = Some(mem_budget::ScopedCharge::new_strict(
            heap_bytes,
            "batch top-n specialized kernel",
        )?);

        if self.try_initialize_exact_rand_covering_residual()? {
            return Ok(());
        }

        // All work stays behind the first-pull boundary so EXPLAIN ANALYZE
        // attributes the header pass + RNG arg-min to this TopN node. The
        // ordinary upstream has been compiled but remains unopened; if an
        // MVCC bound is inconclusive, this attempt consumes no RAND draws and
        // initialization simply continues into the generic full scan below.
        if let Some(exact) = &self.exact_rand_index_top_one {
            let started = analyze_stats::is_recording().then(std::time::Instant::now);
            let io_start = started.map(|_| exact.db.buffer_stats_counts());
            let selected = crate::engine::expr::with_unseeded_mysql_rand_state(|initial| {
                let mut pending_final = None;
                let result = exact.db.select_all_visible_index_range_entry_by_ordinal(
                    &exact.table.schema,
                    exact.index,
                    exact.low.clone(),
                    exact.high.clone(),
                    exact.txn,
                    |row_count| {
                        let (ordinal, final_state) =
                            crate::engine::expr::preview_mysql_rand_arg_extreme(
                                initial,
                                row_count,
                                exact.degree,
                                exact.ascending,
                            );
                        pending_final = Some(final_state);
                        ordinal.expect("non-empty exact RAND range has an arg-extreme")
                    },
                );
                let commit = matches!(&result, Ok(Some(_)))
                    .then_some(pending_final)
                    .flatten();
                (result, commit)
            })?;
            if let Some((entry, row_count)) = selected {
                #[cfg(test)]
                EXACT_RAND_INDEX_TOP_ONE_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                crate::engine::executor::select::eval::ORDER_BY_KEY_ROWS_PREPARED
                    .fetch_add(row_count as u64, std::sync::atomic::Ordering::Relaxed);
                crate::engine::executor::select::analyze_stats::add_range_scan_work_for_plan(
                    exact.scan_plan,
                    row_count as u64,
                    0,
                    0,
                );
                let rows = entry
                    .map(|(key, _row_id)| {
                        let projector =
                            crate::engine::executor::select::plan::IndexEntryProjector::new(
                                exact.table,
                                exact.index,
                                exact.projected_columns,
                            );
                        projector
                            .project(&key)
                            .fields
                            .into_iter()
                            .map(|field| field.value)
                            .collect::<Vec<_>>()
                    })
                    .into_iter()
                    .collect();
                if let (Some(started), Some(io_start)) = (started, io_start) {
                    crate::engine::executor::select::analyze_stats::record_with_io_snapshot(
                        exact.db,
                        exact.scan_plan,
                        started.elapsed().as_nanos(),
                        row_count as u64,
                        io_start,
                    );
                    crate::engine::executor::select::analyze_stats::add_vector_runtime(
                        exact.scan_plan,
                        0,
                        0,
                        1,
                    );
                }
                self.output = Some(value_rows_into_batches(self.schema.clone(), rows).into_iter());
                return Ok(());
            }
        }

        // RAND() produces a non-NULL f64 key. For K=1, constructing a
        // one-element Vec<Value> and entering the generic heap comparator for
        // every input row dominates the scan (millions of tiny envelopes for
        // a payload that is materialised only O(log N) times). Track the best
        // primitive key directly while advancing the exact same session RNG
        // stream in the same batch/row order. Equal RNG keys retain the
        // generic Top-N deterministic payload tie-break.
        if self.k == 1
            && self.offset == 0
            && !self.row_major_keys
            && self.sources.len() == 1
            && matches!(self.sources[0], TopNKeySource::UnseededRand(_))
        {
            let ascending = self.specs[0].ascending;
            // RAND() is seed1 / (2^30-1). Comparing its integer numerator
            // preserves the exact ordering while avoiding six million f64
            // conversions for a typical large-table random pick.
            let mut best_key: Option<u64> = None;
            let mut best_row: Option<crate::engine::executor::row_spool::OwnedRow> = None;
            while let Some(batch) = self.upstream.next_batch()? {
                crate::engine::cancel::check_cancelled()?;
                crate::engine::executor::select::eval::record_order_by_key_rows_prepared(
                    batch.visible_rows(),
                );
                crate::engine::expr::for_each_unseeded_mysql_rand_seed_batch(
                    batch.visible_indices(),
                    |row_index, key| {
                        let ordering = best_key.map(|best| key.cmp(&best));
                        let better = ordering.is_none_or(|ordering| {
                            if ascending {
                                ordering.is_lt()
                            } else {
                                ordering.is_gt()
                            }
                        });
                        if better || ordering.is_some_and(std::cmp::Ordering::is_eq) {
                            let mut charge = mem_budget::ScopedCharge::new(0);
                            let bytes = batch.columns().iter().fold(0usize, |n, column| {
                                n.saturating_add(column.owned_value_bytes(row_index))
                            });
                            charge.add_force(bytes)?;
                            let mut values = mem_budget::try_vec_with_capacity(
                                batch.columns().len(),
                                "RAND Top-1 candidate",
                            )?;
                            for column in batch.columns() {
                                values.push(column.try_value_at(row_index)?);
                            }
                            let candidate =
                                crate::engine::executor::row_spool::OwnedRow { values, charge };
                            if better
                                || best_row.as_ref().is_none_or(|best| {
                                    topn_payload_rows_cmp(&candidate.values, &best.values).is_lt()
                                })
                            {
                                best_key = Some(key);
                                best_row = Some(candidate);
                            }
                        }
                        Ok(())
                    },
                )?;
            }
            let batches = match best_row {
                Some(row) => vec![row.into_batch(self.schema.clone())?],
                None => Vec::new(),
            };
            self.reservation = None;
            self.output = Some(batches.into_iter());
            return Ok(());
        }

        // Keep the parallel scan as a bounded producer and evaluate direct
        // ORDER BY columns in worker-local K-entry heaps. Only degree*K
        // candidates reach the final reduction instead of forcing the
        // coordinator to perform every collation comparison. Expression keys
        // retain the serial oracle until their warning/RAND order can be
        // transferred to workers without semantic drift.
        if parallel_direct_column_topn {
            let driver_keys = self
                .sources
                .iter()
                .enumerate()
                .map(|(index, source)| {
                    let TopNKeySource::Column(column) = source else {
                        unreachable!("parallel direct-column Top-K preflighted")
                    };
                    let spec = self
                        .specs
                        .get(index)
                        .expect("Top-K source/spec cardinality matched at preflight");
                    crate::engine::executor::vec::scheduler::topn_driver::SortKey {
                        column: *column,
                        ascending: spec.ascending,
                        nulls_first: spec.nulls_first,
                        collation_id: self.collations.get(index).copied().flatten(),
                    }
                })
                .collect();
            let output =
                crate::engine::executor::vec::scheduler::topn_driver::run_topn_parallel_spillable(
                    self.upstream.as_mut(),
                    driver_keys,
                    self.k,
                    self.parallel_workers,
                    &self.data_dir,
                    self.offset,
                )?;
            let crate::engine::executor::vec::scheduler::topn_driver::ParallelTopNOutput::Resident(
                rows,
                charge,
            ) = output
            else {
                if let crate::engine::executor::vec::scheduler::topn_driver::ParallelTopNOutput::External(output) = output {
                    self.external_output = Some(output);
                }
                return Ok(());
            };
            self.external_output = Some(Box::new(
                vec_sort::ExternalSortBatchOperator::from_sorted_rows(
                    self.schema.clone(),
                    self.properties.clone(),
                    rows,
                    charge,
                    self.offset,
                ),
            ));
            self.reservation = None;
            return Ok(());
        }

        let sources = std::mem::take(&mut self.sources)
            .into_iter()
            .map(|source| match source {
                TopNKeySource::Column(column) => vec_sort::SortKeySource::Column(column),
                TopNKeySource::UnseededRand(program) => {
                    vec_sort::SortKeySource::UnseededRand(program)
                }
                TopNKeySource::Program { program, integer } => {
                    vec_sort::SortKeySource::Program { program, integer }
                }
            })
            .collect();
        let fields = self
            .specs
            .iter()
            .enumerate()
            .map(|(index, spec)| crate::engine::executor::sort::SortField {
                ascending: spec.ascending,
                nulls_first: spec.nulls_first,
                collation_id: self.collations.get(index).copied().flatten(),
            })
            .collect();
        let upstream = std::mem::replace(
            &mut self.upstream,
            Box::new(MaterializedBatchOperator::new(
                self.schema.clone(),
                Vec::new(),
            )),
        );
        self.external_output = Some(Box::new(
            vec_sort::ExternalSortBatchOperator::new(
                None,
                upstream,
                self.schema.clone(),
                self.properties.clone(),
                sources,
                fields,
                &self.data_dir,
            )
            .with_topn(self.k, self.offset),
        ));
        Ok(())
    }
}

pub(super) fn topn_payload_rows_cmp(left: &[Value], right: &[Value]) -> std::cmp::Ordering {
    crate::engine::executor::sort::compare_payload_rows(left, right)
}

impl BatchOperator for TopNBatchOperator<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }

    fn properties(&self) -> BatchProperties {
        self.properties.clone()
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        self.initialize()?;
        if let Some(output) = &mut self.external_output {
            return output.next_batch();
        }
        Ok(self.output.as_mut().and_then(Iterator::next))
    }
}

/// Peel the parallel Exchange boundary and recognize a covering-index range
/// whose optional residual is completely enforced by its bounds. This shape
/// has a one-to-one mapping between visible in-range leaf entries and TopN
/// input rows, which lets the RAND() arg-min be evaluated by ordinal without
/// materialising the scan.
pub(super) fn exact_rand_covering_index_scan(
    plan: &PhysicalPlan,
) -> Option<(
    &crate::planner::PlanTable,
    &crate::storage::IndexSchema,
    Option<crate::storage::IndexBound>,
    Option<crate::storage::IndexBound>,
    Option<&Arc<Vec<usize>>>,
    Option<&Expr>,
    bool,
    usize,
    &PhysicalPlan,
)> {
    let plan = match plan {
        PhysicalPlan::Exchange { input, .. } => input.as_ref(),
        other => other,
    };
    match plan {
        PhysicalPlan::ParallelIndexFastScan {
            table,
            index,
            low,
            high,
            filter,
            projected_columns,
            degree,
            ..
        } => {
            let filter_covered_by_bounds = filter.as_ref().is_none_or(|filter| {
                crate::planner::index_residual::all_covered(
                    filter,
                    &table.schema,
                    index,
                    low.as_ref(),
                    high.as_ref(),
                )
            });
            Some((
                table,
                index,
                low.clone(),
                high.clone(),
                projected_columns.as_ref(),
                filter.as_ref(),
                filter_covered_by_bounds,
                *degree as usize,
                plan,
            ))
        }
        PhysicalPlan::IndexFastScan {
            table,
            index,
            low,
            high,
            filter,
            reverse: false,
            projected_columns,
            ..
        } => {
            let filter_covered_by_bounds = filter.as_ref().is_none_or(|filter| {
                crate::planner::index_residual::all_covered(
                    filter,
                    &table.schema,
                    index,
                    low.as_ref(),
                    high.as_ref(),
                )
            });
            Some((
                table,
                index,
                low.clone(),
                high.clone(),
                projected_columns.as_ref(),
                filter.as_ref(),
                filter_covered_by_bounds,
                1,
                plan,
            ))
        }
        PhysicalPlan::IndexLookup {
            table,
            index,
            key,
            covering: true,
            filter,
            projected_columns,
            ..
        } => {
            let filter_covered_by_bounds = filter.as_ref().is_none_or(|filter| {
                crate::planner::index_residual::lookup_covered(filter, &table.schema, index, key)
            });
            let bound = crate::storage::IndexBound {
                key: key.clone(),
                inclusive: true,
            };
            Some((
                table,
                index,
                Some(bound.clone()),
                Some(bound),
                projected_columns.as_ref(),
                filter.as_ref(),
                filter_covered_by_bounds,
                1,
                plan,
            ))
        }
        _ => None,
    }
}

/// Whether the finalized plan can use the bounded covering-index RAND Top-1
/// kernel. The auto-mode response router uses this metadata-only predicate to
/// keep the tiny result synchronous without falling through to the legacy
/// row TopK implementation.
pub(in crate::engine::executor::select) fn exact_rand_index_top_one_plan(
    plan: &PhysicalPlan,
) -> bool {
    let (input, order_by, dedup) = match plan {
        PhysicalPlan::TopK {
            input,
            order_by,
            dedup_projection,
            ..
        }
        | PhysicalPlan::ParallelTopK {
            input,
            order_by,
            dedup_projection,
            ..
        } => (
            input.as_ref(),
            order_by.as_slice(),
            dedup_projection.is_some(),
        ),
        PhysicalPlan::TopWindow {
            input, order_by, ..
        } => (input.as_ref(), order_by.as_slice(), false),
        PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Exchange { input, .. } => return exact_rand_index_top_one_plan(input),
        _ => return false,
    };
    !dedup
        && matches!(
            order_by,
            [OrderBy {
                expr: Expr::Function {
                    name,
                    args,
                    distinct: false,
                },
                ..
            }] if name.eq_ignore_ascii_case("RAND") && args.is_empty()
        )
        && exact_rand_covering_index_scan(input).is_some()
}

pub(super) fn aggregate_count_topk_hint(
    input: &PhysicalPlan,
    order_by: &[OrderBy],
    k: usize,
) -> Option<vec_hash_agg::AggregateCountTopKHint> {
    if k == 0 || order_by.len() != 1 {
        return None;
    }
    let aggregates = match input {
        PhysicalPlan::HashAggregate { aggregates, .. }
        | PhysicalPlan::StreamingAggregate { aggregates, .. }
        | PhysicalPlan::ParallelHashAggregate { aggregates, .. } => aggregates,
        _ => return None,
    };
    let order = &order_by[0];
    let order_key = crate::engine::executor::select::aggregate::aggregate_key(&order.expr);
    let aggregate_index = aggregates.iter().position(|aggregate| {
        crate::engine::executor::select::aggregate::aggregate_key(aggregate) == order_key
    })?;
    let slot = crate::engine::executor::select::aggregate::AggregateSlot::from_expr(
        &aggregates[aggregate_index],
    )?;
    if !slot.name.eq_ignore_ascii_case("count") {
        return None;
    }
    Some(vec_hash_agg::AggregateCountTopKHint {
        aggregate_index,
        k,
        descending: !order.asc,
    })
}

pub(super) fn try_build_vec_topn_input<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    let (input, order_by, k, output_offset, parallel_workers) = match plan {
        PhysicalPlan::TopK {
            input,
            order_by,
            dedup_projection,
            k,
            offset,
            binding,
            ..
        } => {
            if dedup_projection.is_some() {
                return Ok(None);
            }
            let (k, _) = binding.resolve_k_offset(*k, *offset);
            (input.as_ref(), order_by.as_slice(), k, 0, 1)
        }
        PhysicalPlan::TopWindow {
            input,
            order_by,
            limit,
            offset,
            binding,
            ..
        } => {
            let (limit, offset) = binding.resolve_limit_offset(*limit, *offset);
            (
                input.as_ref(),
                order_by.as_slice(),
                offset.saturating_add(limit),
                offset,
                1,
            )
        }
        PhysicalPlan::ParallelTopK {
            input,
            order_by,
            dedup_projection,
            k,
            offset,
            binding,
            degree,
            ..
        } => {
            if dedup_projection.is_some() {
                return Ok(None);
            }
            let (k, _) = binding.resolve_k_offset(*k, *offset);
            (
                input.as_ref(),
                order_by.as_slice(),
                k,
                0,
                (*degree as usize).max(1),
            )
        }
        _ => return Ok(None),
    };
    if k > vec_topn::max_topn_k_for_budget() {
        return Ok(None);
    }
    let Some(mut keys) = topn_key_preflight(input, order_by)? else {
        return Ok(None);
    };
    if let Some(fused) =
        try_build_hidden_first_value_topn_input(db, txn, input, &keys, k, output_offset)?
    {
        return Ok(Some(fused));
    }
    let aggregate_input =
        match try_build_stats_proven_count_topk_input(db, txn, input, order_by, &keys, k)? {
            Some(proven) => Some(proven),
            None => try_build_duplicate_aware_aggregate_input(db, txn, input, order_by, &keys, k)?,
        };
    let VecBatchInput { stream, template } = if let Some(input) = aggregate_input {
        input
    } else {
        let count_topk = aggregate_count_topk_hint(input, order_by, k);
        let specialized = if count_topk.is_some() {
            try_build_vec_hash_agg_input_with_count_topk(db, txn, input, count_topk)?
        } else {
            None
        };
        let Some(input) = (match specialized {
            Some(input) => Some(input),
            None => try_native_vec_input_stream(db, txn, input)?,
        }) else {
            return Ok(None);
        };
        input
    };
    if template.len() != keys.template.len() {
        return Err(crate::storage::error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            "batch Top-N schema diverged from its preflight metadata",
        ));
    }
    if hash_aggregate_node(input).is_none() {
        bind_vector_order_keys_at_open(&mut keys, order_by)?;
    }
    let schema = stream.schema().clone();
    let row_major_keys = keys
        .sources
        .iter()
        .filter_map(|source| match source {
            TopNKeySource::Program { program, .. } => Some(program.capability()),
            TopNKeySource::UnseededRand(program) => Some(program.capability()),
            TopNKeySource::Column(_) => None,
        })
        .filter(|capability| {
            capability.may_error || !capability.deterministic || capability.depends_on_session
        })
        .count()
        > 1;
    let exact_rand_index_top_one = if k == 1
        && output_offset == 0
        && keys.sources.len() == 1
        && matches!(keys.sources[0], TopNKeySource::UnseededRand(_))
    {
        exact_rand_covering_index_scan(input).and_then(
            |(
                table,
                index,
                low,
                high,
                projected_columns,
                filter,
                filter_covered_by_bounds,
                degree,
                scan_plan,
            )| {
                (!table.schema.is_partitioned()).then_some(ExactRandIndexTopOne {
                    db,
                    txn: crate::engine::executor::temporal::txn_for_table(txn, table),
                    scan_plan,
                    table,
                    index,
                    low,
                    high,
                    projected_columns: projected_columns.map(|columns| columns.as_slice()),
                    filter,
                    filter_covered_by_bounds,
                    degree,
                    ascending: keys.specs[0].ascending,
                })
            },
        )
    } else {
        None
    };
    Ok(Some(VecBatchInput {
        stream: Box::new(TopNBatchOperator {
            data_dir: db.data_dir().to_path_buf(),
            external_output: None,
            upstream: stream,
            schema,
            properties: keys.properties,
            sources: keys.sources,
            row_major_keys,
            specs: keys.specs,
            collations: keys.collations,
            parallel_workers,
            k,
            offset: output_offset,
            output: None,
            reservation: None,
            exact_rand_index_top_one,
        }),
        template,
    }))
}

pub(super) fn simple_column_parts(expr: &Expr) -> Option<(Option<&str>, &str)> {
    match expr {
        Expr::GroupKeyRef { expr, .. } => simple_column_parts(expr),
        Expr::Identifier(column) => Some((None, column)),
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => Some((
            Some(parts[parts.len() - 2].as_str()),
            parts[parts.len() - 1].as_str(),
        )),
        Expr::CompoundIdentifier(parts) if parts.len() == 1 => Some((None, &parts[0])),
        _ => None,
    }
}

pub(super) fn same_simple_column(left: &Expr, right: &Expr) -> bool {
    let Some((left_qualifier, left_column)) = simple_column_parts(left) else {
        return false;
    };
    let Some((right_qualifier, right_column)) = simple_column_parts(right) else {
        return false;
    };
    left_column.eq_ignore_ascii_case(right_column)
        && match (left_qualifier, right_qualifier) {
            (Some(left), Some(right)) => left.eq_ignore_ascii_case(right),
            _ => true,
        }
}

pub(super) fn field_belongs_to_table(field: &PlanField, table: &crate::planner::PlanTable) -> bool {
    let relation = field.alias.as_deref().unwrap_or(&field.table);
    table
        .alias
        .as_deref()
        .is_some_and(|alias| relation.eq_ignore_ascii_case(alias))
        || relation.eq_ignore_ascii_case(&table.schema.name)
        || field.table.eq_ignore_ascii_case(&table.schema.name)
}

pub(super) fn field_matches_column_expr(field: &PlanField, expr: &Expr) -> bool {
    let Some((qualifier, column)) = simple_column_parts(expr) else {
        return false;
    };
    field.column.eq_ignore_ascii_case(column)
        && qualifier.is_none_or(|qualifier| {
            field.table.eq_ignore_ascii_case(qualifier)
                || field
                    .alias
                    .as_deref()
                    .is_some_and(|alias| alias.eq_ignore_ascii_case(qualifier))
        })
}

pub(super) fn unique_table_for_join_key(
    plan: &PhysicalPlan,
    key: &Expr,
) -> Option<crate::planner::PlanTable> {
    let (qualifier, column) = simple_column_parts(key)?;
    crate::planner::collect_physical_plan_tables(plan)
        .into_iter()
        .filter(|table| {
            qualifier.is_none_or(|qualifier| {
                table
                    .alias
                    .as_deref()
                    .is_some_and(|alias| alias.eq_ignore_ascii_case(qualifier))
                    || table.schema.name.eq_ignore_ascii_case(qualifier)
            })
        })
        .find(|table| {
            table.schema.iter_queryable_indexes().any(|index| {
                (index.unique || index.primary)
                    && !index.has_prefix_columns()
                    && index
                        .effective_columns()
                        .first()
                        .is_some_and(|indexed| indexed.eq_ignore_ascii_case(column))
            })
        })
}

pub(super) fn unique_table_for_field(
    plan: &PhysicalPlan,
    field: &PlanField,
) -> Option<crate::planner::PlanTable> {
    crate::planner::collect_physical_plan_tables(plan)
        .into_iter()
        .filter(|table| field_belongs_to_table(field, table))
        .find(|table| {
            table.schema.iter_queryable_indexes().any(|index| {
                (index.unique || index.primary)
                    && !index.has_prefix_columns()
                    && index
                        .effective_columns()
                        .first()
                        .is_some_and(|indexed| indexed.eq_ignore_ascii_case(&field.column))
            })
        })
}

/// Prove the common fact-key + UNIQUE-dimension-attribute GROUP BY FD. The
/// returned input column may replace the full composite seen-set, but only for
/// an INNER hash join whose opposite key is backed by a complete UNIQUE index.
pub(super) fn unique_dimension_fd_group_column(
    input: &PhysicalPlan,
    group_by: &[Expr],
    group_columns: &[usize],
    template: &[PlanField],
    schema: &BatchSchema,
) -> Option<(usize, bool)> {
    let mut input = input;
    loop {
        input = match input {
            PhysicalPlan::Exchange { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::IncrementalSort { input, .. }
            | PhysicalPlan::ParallelSort { input, .. }
            | PhysicalPlan::Projection {
                input,
                distinct: false,
                ..
            }
            | PhysicalPlan::Filter { input, .. } => input,
            _ => break,
        };
    }
    let PhysicalPlan::HashJoin {
        left,
        right,
        join_type: crate::parser::JoinType::Inner,
        left_keys,
        right_keys,
        residual: None,
        ..
    } = input
    else {
        return None;
    };
    if group_by.len() != 2 || group_columns.len() != 2 {
        return None;
    }
    let integer_pos = group_columns.iter().position(|&column| {
        schema.fields().get(column).is_some_and(|field| {
            matches!(
                field.column_type,
                ColumnType::Tiny
                    | ColumnType::Short
                    | ColumnType::MediumInt
                    | ColumnType::Long
                    | ColumnType::LongLong
            )
        })
    })?;
    let attribute_pos = 1usize.checked_sub(integer_pos)?;
    let integer_expr = group_by.get(integer_pos)?;
    let integer_column = *group_columns.get(integer_pos)?;
    let integer_field = template.get(integer_column)?;
    let attribute_field = template.get(*group_columns.get(attribute_pos)?)?;

    if let Some(owner) = unique_table_for_field(input, integer_field) {
        if field_belongs_to_table(attribute_field, &owner) {
            return Some((integer_column, true));
        }
    }

    for (left_key, right_key) in left_keys.iter().zip(right_keys) {
        if unique_table_for_field(left, integer_field).is_some() {
            if let Some(dimension) = unique_table_for_join_key(right, right_key) {
                if field_belongs_to_table(attribute_field, &dimension) {
                    return Some((integer_column, true));
                }
            }
        }
        if unique_table_for_field(right, integer_field).is_some() {
            if let Some(dimension) = unique_table_for_join_key(left, left_key) {
                if field_belongs_to_table(attribute_field, &dimension) {
                    return Some((integer_column, true));
                }
            }
        }
        let left_is_group_key = same_simple_column(integer_expr, left_key)
            || expr_column_index_in_template(left_key, template) == Some(integer_column)
            || field_matches_column_expr(integer_field, left_key);
        let right_is_group_key = same_simple_column(integer_expr, right_key)
            || expr_column_index_in_template(right_key, template) == Some(integer_column)
            || field_matches_column_expr(integer_field, right_key);
        let (dimension_plan, dimension_key) = if left_is_group_key {
            (right.as_ref(), right_key)
        } else if right_is_group_key {
            (left.as_ref(), left_key)
        } else {
            continue;
        };
        let Some(dimension) = unique_table_for_join_key(dimension_plan, dimension_key) else {
            continue;
        };
        if field_belongs_to_table(attribute_field, &dimension) {
            return group_columns
                .get(integer_pos)
                .copied()
                .map(|column| (column, false));
        }
    }
    None
}

pub(super) fn try_build_hidden_first_value_topn_input<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    aggregate_plan: &'a PhysicalPlan,
    keys: &TopNKeyPlan,
    k: usize,
    offset: usize,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    let Some(aggregate) = aggregate_plan.ordinary_aggregate_input() else {
        return Ok(None);
    };
    let (input_plan, group_by, having, aggregates) = (
        aggregate.input,
        aggregate.group_by,
        aggregate.having,
        aggregate.aggregates,
    );
    if having.is_some() || aggregates.is_empty() {
        return Ok(None);
    }
    let sources_supported = keys
        .sources
        .iter()
        .all(|source| !matches!(source, TopNKeySource::UnseededRand(_)));
    if !sources_supported {
        return Ok(None);
    }

    // Native hash joins may execute their entire spill pipeline when opened.
    // Resolve every capability from plan metadata before constructing it.
    let input_template = plan_output_template(input_plan);
    let input_schema = template_to_batch_schema(&input_template);
    let Some((group_columns, programs, _numeric, _fields)) =
        compile_group_columns(group_by, &input_template, &input_schema)
    else {
        return Ok(None);
    };
    if !programs.is_empty() {
        return Ok(None);
    }
    let (fd_group_column, fd_group_unique) = unique_dimension_fd_group_column(
        input_plan,
        group_by,
        &group_columns,
        &input_template,
        &input_schema,
    )
    .map(|(column, unique)| (Some(column), unique))
    .unwrap_or((None, false));

    let mut first_value_columns = Vec::with_capacity(aggregates.len());
    for aggregate in aggregates {
        let Some(slot) =
            crate::engine::executor::select::aggregate::AggregateSlot::from_expr(aggregate)
        else {
            return Ok(None);
        };
        if !slot
            .name
            .eq_ignore_ascii_case(crate::parser::HIDDEN_FIRST_VALUE_FN)
            || slot.distinct
            || !slot.order_by.is_empty()
            || slot.args.len() != 1
        {
            return Ok(None);
        }
        let Some(column) = expr_column_index_in_template(&slot.args[0], &input_template) else {
            return Ok(None);
        };
        first_value_columns.push(column);
    }
    if keys.template.len() != group_columns.len() + first_value_columns.len()
        || keys.sources.iter().any(
            |source| matches!(source, TopNKeySource::Column(column) if *column >= keys.template.len()),
        )
    {
        return Ok(None);
    }
    let Some(key_program) = crate::engine::executor::hash_aggregate::GroupKeyProgram::compile(
        &group_columns,
        &input_schema,
    ) else {
        return Ok(None);
    };
    let Some(VecBatchInput {
        stream: upstream,
        template,
    }) = try_native_vec_input_stream(db, txn, input_plan)?
    else {
        return Ok(None);
    };
    if template.len() != input_template.len() {
        return Err(crate::storage::error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            "batch first-value Top-N schema diverged from its preflight metadata",
        ));
    }
    let schema = template_to_batch_schema(&keys.template);
    Ok(Some(VecBatchInput {
        stream: Box::new(HiddenFirstValueTopNBatchOperator {
            db,
            aggregate_plan,
            upstream,
            schema,
            properties: keys.properties.clone(),
            key_program,
            group_columns,
            fd_group_column,
            fd_group_unique,
            first_value_columns,
            sources: keys.sources.clone(),
            specs: keys.specs.clone(),
            collations: keys.collations.clone(),
            k,
            offset,
            output: None,
            group_charge: mem_budget::ScopedCharge::new(0),
            topn_charge: None,
        }),
        template: keys.template.clone(),
    }))
}

pub(super) fn try_build_vec_sort_input<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    let (input, order_by) = match plan {
        PhysicalPlan::Sort {
            input, order_by, ..
        }
        | PhysicalPlan::IncrementalSort {
            input, order_by, ..
        }
        | PhysicalPlan::ParallelSort {
            input, order_by, ..
        } => (input, order_by),
        _ => return Ok(None),
    };
    let Some(mut keys) = topn_key_preflight(input, order_by)? else {
        return Ok(None);
    };
    let Some(VecBatchInput { stream, template }) = try_native_vec_input_stream(db, txn, input)?
    else {
        return Ok(None);
    };
    if template.len() != keys.template.len() {
        return Err(crate::storage::error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            format!(
                "batch Sort schema diverged from its preflight metadata (runtime={} {:?}, preflight={} {:?})",
                template.len(),
                template.iter().map(|field| (&field.table, &field.column)).collect::<Vec<_>>(),
                keys.template.len(),
                keys.template.iter().map(|field| (&field.table, &field.column)).collect::<Vec<_>>()
            ),
        ));
    }
    if hash_aggregate_node(input).is_none() {
        bind_vector_order_keys_at_open(&mut keys, order_by)?;
    }
    let schema = stream.schema().clone();
    let sources = keys
        .sources
        .into_iter()
        .map(|source| match source {
            TopNKeySource::Column(column) => vec_sort::SortKeySource::Column(column),
            TopNKeySource::UnseededRand(program) => vec_sort::SortKeySource::Program {
                program,
                integer: None,
            },
            TopNKeySource::Program { program, integer } => {
                vec_sort::SortKeySource::Program { program, integer }
            }
        })
        .collect();
    let fields = keys
        .specs
        .iter()
        .zip(keys.collations.iter())
        .map(
            |(spec, collation_id)| crate::engine::executor::sort::SortField {
                ascending: spec.ascending,
                nulls_first: spec.nulls_first,
                collation_id: *collation_id,
            },
        )
        .collect();
    Ok(Some(VecBatchInput {
        stream: Box::new(vec_sort::ExternalSortBatchOperator::new(
            Some(plan),
            stream,
            schema,
            keys.properties,
            sources,
            fields,
            db.data_dir(),
        )),
        template,
    }))
}

pub(super) fn distinct_key_preflight(
    input: &PhysicalPlan,
    projection: &[SelectItem],
) -> Option<(Vec<PlanField>, Vec<usize>)> {
    if projection.is_empty() {
        return None;
    }
    let template = plan_output_template(input);
    if template.is_empty() {
        return None;
    }
    let mut columns = Vec::new();
    for item in projection {
        match item {
            SelectItem::Wildcard => columns.extend(0..template.len()),
            SelectItem::Expr { expr, .. } => {
                columns.push(expr_column_index_in_template(expr, &template)?);
            }
            SelectItem::QualifiedWildcard(_) => return None,
        }
    }
    Some((template, columns))
}

/// Attempt to satisfy an `ORDER BY` + `LIMIT` with the vectorized Top-N
/// heap. Returns `Ok(None)` if the shape isn't supported (NULL
/// ordering quirks, ORDER BY expressions that aren't pure column
/// references on the schema, no LIMIT, missing schema, etc.).
pub(in crate::engine::executor::select) fn try_vec_topn(
    db: &Arc<Database>,
    txn: TxnView,
    upstream: &PhysicalPlan,
    order_by: &[OrderBy],
    limit: Option<usize>,
    offset: Option<usize>,
) -> Result<Option<Vec<PlanRow>>, QueryError> {
    // TopK is an ordinary batch operator; admission is independent of actual
    // CTE cardinality and current memory pressure.
    let Some(limit) = limit else {
        return Ok(None);
    };
    if order_by.is_empty() {
        return Ok(None);
    }
    // We need k = offset + limit rows in the heap, then drop the first
    // `offset` after sorting. Cap k to keep memory bounded; for very
    // large k the legacy quickselect path is fine.
    let offset = offset.unwrap_or(0);
    let k = offset.saturating_add(limit);
    if k == 0 {
        return Ok(Some(Vec::new()));
    }
    if k > crate::engine::executor::vec::topn::max_topn_k_for_budget() {
        return Ok(None);
    }

    // The ordinary SELECT dispatcher reaches TopK through this eager bridge,
    // while EXPLAIN ANALYZE executes the physical BatchOperator tree directly.
    // Route the same hidden-first-value fusion here as well so diagnostics and
    // real execution cannot silently use different aggregate kernels.
    if let Some(keys) = topn_key_preflight(upstream, order_by)? {
        if let Some(mut fused) =
            try_build_hidden_first_value_topn_input(db, txn, upstream, &keys, k, offset)?
        {
            let aggregate_parts = hash_aggregate_node(upstream)
                .and_then(PhysicalPlan::ordinary_aggregate_input)
                .map(|aggregate| (aggregate.group_by.len(), aggregate.aggregates));
            let Some((group_count, aggregates)) = aggregate_parts else {
                return Ok(None);
            };
            let mut values = Vec::new();
            while let Some(batch) = fused.stream.next_batch()? {
                for row in batch.visible_indices() {
                    values.push(
                        batch
                            .columns()
                            .iter()
                            .map(|column| column.value_at(row))
                            .collect::<Vec<_>>(),
                    );
                }
            }
            let group_template = &fused.template[..group_count];
            let aggregate_keys = aggregates
                .iter()
                .map(crate::engine::executor::select::aggregate::aggregate_key)
                .collect::<Vec<_>>();
            let mut out = Vec::with_capacity(values.len());
            for row in values {
                let fields = vec_hash_agg_group_fields_from_values(group_template, &row);
                let mut cache = std::collections::HashMap::with_capacity(aggregate_keys.len());
                for (slot, key) in aggregate_keys.iter().enumerate() {
                    cache.insert(*key, row[group_count + slot].clone());
                }
                out.push(PlanRow {
                    fields,
                    group: Some(Vec::new()),
                    agg_values: Some(Arc::new(cache)),
                    schema: PlanRow::empty_schema(),
                });
            }
            return Ok(Some(out));
        }
    }

    // Fused fast path: `TopN(HashAggregate(input))`. The vec engine
    // builds the HashAggregate as a real BatchOperator operator and feeds
    // its output straight into `run_topn`, skipping a full PlanRow
    // round-trip on the post-aggregate result set. This is the SF=1
    // sweet spot for TPC-H queries like Q3/Q10/Q18 where the LIMIT
    // happens right after a GROUP BY ORDER BY.
    //
    // EXCEPTION: when the HashAggregate's input is an
    // `IndexNestedLoopJoin`, fall back to the legacy `execute_topk` ->
    // `BoundedTopK` path so the row-based INLJ + grouped-agg fused
    // implementation in `execute_hash_aggregate_keyed` can run. That
    // fused path collapses INLJ probe and group-state update into the
    // same outer-parallel workers. There is no native vec INLJ source,
    // so capability checking rejects this subtree before execution.
    if let Some(aggregate) = upstream.ordinary_aggregate_input() {
        if matches!(aggregate.input, PhysicalPlan::IndexNestedLoopJoin { .. }) {
            return Ok(None);
        }
        if let Some(rows) = try_vec_topn_over_hash_aggregate(
            db,
            txn,
            upstream,
            aggregate.input,
            aggregate.group_by,
            aggregate.having,
            aggregate.aggregates,
            order_by,
            limit,
            offset,
        )? {
            return Ok(Some(rows));
        }
    }

    // The generic path below round-trips upstream rows through a
    // `Batch`, which carries only column values — `PlanRow::group` and
    // `agg_values` are dropped. When the upstream emits aggregate rows
    // (any aggregate node the fused `HashAggregate` path above did not
    // fully handle, e.g. `ParallelHashAggregate` / `StreamingAggregate`),
    // the SELECT projection still holds `Expr::Function` aggregate calls
    // that must resolve against that dropped cache. Fall back to the
    // legacy ordered-limit path, which preserves the metadata.
    if crate::engine::executor::select::plan::plan_emits_aggregate_rows(upstream) {
        return Ok(None);
    }

    // *** Static pre-check, before any IO. ***
    // ORDER BY keys are either plain column references (the common
    // case, handled below with the parallel driver) or scalar
    // expressions the batch compiler can evaluate. Subquery / aggregate
    // / window shapes are rejected here, before the upstream stream is
    // built — if we kicked off a stream and then bailed, legacy would
    // run a second scan, doubling the IO.
    let mut has_expr_keys = false;
    for ob in order_by {
        if is_simple_column_ref(&ob.expr) {
            continue;
        }
        if crate::engine::executor::select::expr_has_subquery(&ob.expr)
            || crate::engine::executor::select::expr_has_aggregate(&ob.expr)
            || crate::engine::executor::select::expr_has_window(&ob.expr)
        {
            return Ok(None);
        }
        has_expr_keys = true;
    }

    let Some(VecBatchInput {
        stream: mut input_stream,
        template,
    }) = try_native_vec_input_stream(db, txn, upstream)?
    else {
        return Ok(None);
    };
    if template.is_empty() {
        // Zero output columns do not imply zero rows (source-free SELECT
        // starts from a one-row, zero-column ValuesScan). Keep the canonical
        // row heap until this eager adapter supports that cardinality.
        return Ok(None);
    }
    if has_expr_keys {
        return run_vec_topn_expr_keys(input_stream.as_mut(), &template, order_by, k, offset);
    }

    // Map each ORDER BY expression to a column index. If any expression
    // isn't a plain column reference into the schema, bail.
    let mut specs = Vec::with_capacity(order_by.len());
    let mut sort_cols = Vec::with_capacity(order_by.len());
    for ob in order_by {
        let idx = match expr_column_index_in_template(&ob.expr, &template) {
            Some(i) => i,
            None => return Ok(None),
        };
        // MySQL default placement (NULLs first when ascending, last when
        // descending) unless an explicit NULLS FIRST/LAST overrides it.
        let spec = vec_topn::SortColumnSpec {
            column_index: sort_cols.len(),
            ascending: ob.asc,
            nulls_first: ob.nulls_first_effective(),
        };
        specs.push(spec);
        sort_cols.push(idx);
    }

    // Output projection: all input columns, in order.
    let projection: Vec<usize> = (0..template.len()).collect();
    // sort_cols indices above were assigned 0..specs.len() relative to a
    // hypothetical key tuple. Replace with the real schema indices used
    // by `run_topn`.
    let final_specs: Vec<vec_topn::SortColumnSpec> = specs
        .iter()
        .enumerate()
        .map(|(i, s)| vec_topn::SortColumnSpec {
            column_index: sort_cols[i],
            ascending: s.ascending,
            nulls_first: s.nulls_first,
        })
        .collect();
    let real_sort_cols: Vec<usize> = sort_cols.clone();
    // C11 - route through the morsel-driven topn driver when the
    // user has parallel workers available. The driver collects the
    // input stream, fans batches across per-worker bounded heaps,
    // then K-way reduces into a single global Top-K. Its public
    // entry already falls back to a single-thread heap when there
    // are fewer than 2 input batches, so tiny inputs pay no fork
    // tax. Projection here is always identity (`0..template.len()`),
    // matching the driver's "emit all columns" contract.
    let workers = scheduler_topn_workers();
    // The old parallel driver materialises every input Batch before fan-out.
    // Keep it opt-in until it is converted to the bounded producer queue;
    // the serial Top-N is already a streaming O(K) state operator.
    let topn_scheduler_enabled = false;
    let projection_is_identity =
        projection.iter().enumerate().all(|(i, &p)| i == p) && projection.len() == template.len();
    let rows = if topn_scheduler_enabled && workers > 1 && projection_is_identity {
        let driver_keys: Vec<crate::engine::executor::vec::scheduler::topn_driver::SortKey> =
            final_specs
                .iter()
                .map(
                    |s| crate::engine::executor::vec::scheduler::topn_driver::SortKey {
                        column: s.column_index,
                        ascending: s.ascending,
                        nulls_first: s.nulls_first,
                        collation_id: template
                            .get(s.column_index)
                            .and_then(|field| field.meta.source.collation_id),
                    },
                )
                .collect();
        crate::engine::executor::vec::scheduler::topn_driver::run_topn_parallel(
            input_stream.as_mut(),
            driver_keys,
            k,
            workers,
        )?
    } else {
        vec_topn::run_topn(
            input_stream.as_mut(),
            &final_specs,
            &real_sort_cols,
            &projection,
            k,
        )?
    };
    // Apply offset.
    let trimmed: Vec<Vec<Value>> = rows.into_iter().skip(offset).collect();
    Ok(Some(
        trimmed
            .into_iter()
            .map(|vs| values_to_plan_row(vs, &template))
            .collect(),
    ))
}

/// Top-N with ORDER BY *expression* keys: each key is compiled against
/// the batch schema once and evaluated column-at-a-time per batch
/// (`expr_batch::eval_value_per_row`), then fed to the shared Top-N
/// heap. Plain column references resolve to direct batch columns;
/// anything the batch compiler cannot handle returns `Ok(None)` so the
/// legacy ordered-limit path takes over before the first input batch is read.
pub(super) fn run_vec_topn_expr_keys(
    stream: &mut dyn BatchOperator,
    template: &[PlanField],
    order_by: &[OrderBy],
    k: usize,
    offset: usize,
) -> Result<Option<Vec<PlanRow>>, QueryError> {
    enum KeySource {
        Column(usize),
        Program(std::sync::Arc<crate::engine::expr_program::ExprProgram>),
    }
    let schema = template_to_batch_schema(template);
    let mut sources = Vec::with_capacity(order_by.len());
    for ob in order_by {
        if let Some(idx) = expr_column_index_in_template(&ob.expr, template) {
            sources.push(KeySource::Column(idx));
            continue;
        }
        match crate::engine::executor::vec::expr_batch::compile_for_batch_at_open(
            &ob.expr, &schema,
        )? {
            Some(program) => sources.push(KeySource::Program(program)),
            None => return Ok(None),
        }
    }
    let specs: Vec<vec_topn::SortColumnSpec> = order_by
        .iter()
        .enumerate()
        .map(|(i, ob)| vec_topn::SortColumnSpec {
            column_index: i,
            ascending: ob.asc,
            nulls_first: ob.nulls_first_effective(),
        })
        .collect();
    let collations = sources
        .iter()
        .map(|source| match source {
            KeySource::Column(column) => schema.fields()[*column].source.collation_id,
            KeySource::Program(_) => None,
        })
        .collect();
    let mut acc = vec_topn::TopNAccumulator::new_with_width(specs, k, template.len())?
        .with_collations(collations);
    let mut key_columns: Vec<Vec<Value>> = Vec::with_capacity(sources.len());
    while let Some(batch) = stream.next_batch()? {
        // Evaluate every expression key for the whole batch first
        // (column-at-a-time; physical row indexing), then walk visible
        // rows once.
        key_columns.clear();
        for source in &sources {
            match source {
                KeySource::Column(_) => key_columns.push(Vec::new()),
                KeySource::Program(program) => key_columns.push(
                    crate::engine::executor::vec::expr_batch::eval_value_per_row(program, &batch)?,
                ),
            }
        }
        crate::engine::executor::select::eval::record_order_by_key_rows_prepared(
            batch.visible_rows(),
        );
        for r in batch.visible_indices() {
            let keys: Vec<Value> = sources
                .iter()
                .zip(key_columns.iter())
                .map(|(source, evaluated)| match source {
                    KeySource::Column(c) => batch.columns()[*c].value_at(r),
                    KeySource::Program(_) => evaluated.get(r).cloned().unwrap_or(Value::Null),
                })
                .collect();
            acc.push_lazy(keys, || {
                (0..template.len())
                    .map(|c| batch.columns()[c].value_at(r))
                    .collect()
            })?;
        }
    }
    let rows = acc.finish()?;
    Ok(Some(
        rows.into_iter()
            .skip(offset)
            .map(|vs| values_to_plan_row(vs, template))
            .collect(),
    ))
}

/// Fused `TopN(HashAggregate(input))` — builds the HashAggregate as a
/// streaming op and feeds its output BatchOperator straight into the
/// Top-N heap. Returns `Ok(None)` if either operator can't handle the
/// shape so the caller falls back to the row-based path.
#[allow(clippy::too_many_arguments)]
pub(in crate::engine::executor::select) fn try_vec_topn_over_hash_aggregate(
    db: &Arc<Database>,
    txn: TxnView,
    hash_agg_plan: &PhysicalPlan,
    agg_input: &PhysicalPlan,
    group_by: &[Expr],
    having: Option<&Expr>,
    aggregates: &[Expr],
    order_by: &[OrderBy],
    limit: usize,
    offset: usize,
) -> Result<Option<Vec<PlanRow>>, QueryError> {
    // Capability checks must precede opening the blocking aggregate. A rejected
    // sort expression must not execute the join/scan and then replay it in the
    // row TopK fallback (nor duplicate warnings or volatile evaluation).
    let Some(preflight) = hash_agg_output_preflight(agg_input, group_by, None, aggregates) else {
        return Ok(None);
    };
    let group_template = &preflight.output_template[..preflight.group_count];
    for order in order_by {
        if expr_column_index_in_template(&order.expr, group_template).is_none()
            && !aggregates.iter().any(|aggregate| {
                crate::engine::executor::select::aggregate::aggregate_key(aggregate)
                    == crate::engine::executor::select::aggregate::aggregate_key(&order.expr)
            })
        {
            return Ok(None);
        }
    }
    let k = offset.saturating_add(limit);
    let count_topk = (order_by.len() == 1)
        .then(|| {
            let order = &order_by[0];
            let order_key = crate::engine::executor::select::aggregate::aggregate_key(&order.expr);
            let aggregate_index = aggregates.iter().position(|aggregate| {
                crate::engine::executor::select::aggregate::aggregate_key(aggregate) == order_key
            })?;
            let slot = crate::engine::executor::select::aggregate::AggregateSlot::from_expr(
                &aggregates[aggregate_index],
            )?;
            (slot.name.eq_ignore_ascii_case("count") && !slot.distinct).then_some(
                vec_hash_agg::AggregateCountTopKHint {
                    aggregate_index,
                    k,
                    descending: !order.asc,
                },
            )
        })
        .flatten();
    let Some(HashAggregateStream {
        stream,
        group_template,
        agg_keys,
        aggregates: agg_exprs,
        schema: _,
        metrics,
        having_applied: _,
    }) = build_vec_hash_agg_stream_with_count_topk(
        db, txn, agg_input, group_by, having, aggregates, count_topk,
    )?
    else {
        return Ok(None);
    };
    record_pending_agg_metrics(metrics);
    // Forward HashAgg metrics to the actual HashAggregate plan node so
    // EXPLAIN ANALYZE shows `streaming=true` / `peak_mem` / `spill_bytes`
    // against the right operator. Without this, the fused path leaves
    // the HashAggregate node tagged "never executed".
    forward_vec_hash_agg_metrics(hash_agg_plan);

    let n_group = group_template.len();
    let n_agg = agg_keys.len();

    // Map each ORDER BY expression to a column index in the HashAgg
    // output. Two-step resolution:
    //   1. Try group_template (plain column references like `k`).
    //   2. Try matching the aggregate Expr by `aggregate_key`.
    let mut sort_cols: Vec<usize> = Vec::with_capacity(order_by.len());
    let mut specs: Vec<vec_topn::SortColumnSpec> = Vec::with_capacity(order_by.len());
    for ob in order_by {
        let col = if let Some(i) = expr_column_index_in_template(&ob.expr, &group_template) {
            i
        } else {
            let ob_key = crate::engine::executor::select::aggregate::aggregate_key(&ob.expr);
            let mut found: Option<usize> = None;
            for (i, agg) in agg_exprs.iter().enumerate() {
                if crate::engine::executor::select::aggregate::aggregate_key(agg) == ob_key {
                    found = Some(n_group + i);
                    break;
                }
            }
            match found {
                Some(i) => i,
                None => return Ok(None),
            }
        };
        let spec = vec_topn::SortColumnSpec {
            column_index: col,
            ascending: ob.asc,
            nulls_first: ob.nulls_first_effective(),
        };
        specs.push(spec);
        sort_cols.push(col);
    }

    // Output projection: all HashAgg output columns. We hand them all to
    // run_topn so the heap entry carries enough state to reconstruct a
    // full PlanRow + agg_values cache.
    let projection: Vec<usize> = (0..(n_group + n_agg)).collect();

    let mut stream_ref = stream;
    let rows = vec_topn::run_topn(stream_ref.as_mut(), &specs, &sort_cols, &projection, k)?;
    let trimmed: Vec<Vec<Value>> = rows.into_iter().skip(offset).collect();

    let mut out = Vec::with_capacity(trimmed.len());
    for row in trimmed {
        debug_assert_eq!(row.len(), n_group + n_agg);
        let fields = vec_hash_agg_group_fields_from_values(&group_template, &row);
        let mut cache: std::collections::HashMap<u64, Value> =
            std::collections::HashMap::with_capacity(n_agg);
        for (slot, key) in agg_keys.iter().enumerate() {
            cache.insert(*key, row[n_group + slot].clone());
        }
        out.push(PlanRow {
            fields,
            // Mark this row as aggregate output so downstream
            // `eval_row_or_aggregate_expr` routes alias lookups
            // (e.g. ORDER BY <SUM alias>) into `eval_aggregate_expr`
            // and finds them in `agg_values`. See twin fix in
            // `try_vec_hash_aggregate` above for the TPC-H Q5 repro.
            group: Some(Vec::new()),
            agg_values: Some(Arc::new(cache)),
            schema: PlanRow::empty_schema(),
        });
    }
    Ok(Some(out))
}

pub(super) fn expr_column_index_in_template(expr: &Expr, template: &[PlanField]) -> Option<usize> {
    let (qualifier, name) = match expr {
        Expr::Identifier(name) => (None, name.as_str()),
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => (
            Some(parts[parts.len() - 2].as_str()),
            parts[parts.len() - 1].as_str(),
        ),
        Expr::CompoundIdentifier(parts) if parts.len() == 1 => (None, parts[0].as_str()),
        _ => return None,
    };
    let mut matches = template.iter().enumerate().filter(|(_, field)| {
        field.column.eq_ignore_ascii_case(name)
            && qualifier.is_none_or(|qualifier| {
                field.table.eq_ignore_ascii_case(qualifier)
                    || field
                        .alias
                        .as_deref()
                        .is_some_and(|alias| alias.eq_ignore_ascii_case(qualifier))
            })
    });
    let first = matches.next();
    let second = matches.next();
    match (first, second) {
        (Some((i, _)), None) => Some(i),
        _ => None,
    }
}

// ---------------------------------------------------------------------
// Hash aggregate
// ---------------------------------------------------------------------

pub(super) struct HashAggPreflight {
    pub(super) output_schema: Arc<BatchSchema>,
    pub(super) output_template: Vec<PlanField>,
    pub(super) group_count: usize,
    pub(super) group_exprs: Vec<Expr>,
    pub(super) aggregate_keys: Vec<u64>,
    pub(super) having_program:
        Option<Arc<crate::engine::executor::vec::expr_batch::BatchExprProgram>>,
}
