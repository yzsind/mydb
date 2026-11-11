//! Reduce repeated exact-SUM groups before the bounded ordered ROLLUP pass.

use super::super::aggregate::AggregateSlot;
use super::*;
use std::collections::VecDeque;

#[cfg(test)]
pub(crate) static PREAGGREGATE_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

struct Prefix<'a> {
    batches: VecDeque<Batch>,
    input: Box<dyn BatchOperator + 'a>,
    _charge: mem_budget::ScopedCharge,
}
impl BatchOperator for Prefix<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        self.input.schema()
    }
    fn properties(&self) -> BatchProperties {
        self.input.properties()
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        if let Some(batch) = self.batches.pop_front() {
            Ok(Some(batch))
        } else {
            self.input.next_batch()
        }
    }
}

pub(super) fn reduce<'a>(
    db: &Arc<Database>,
    mut input: VecBatchInput<'a>,
    group_by: &[Expr],
    slots: &[AggregateSlot],
    estimated_rows: f64,
) -> Result<(VecBatchInput<'a>, Vec<AggregateSlot>), QueryError> {
    let original = slots.to_vec();
    let template_row = PlanRow {
        fields: input.template.clone(),
        ..Default::default()
    };
    // Partial finalization is associative only for exact SUM here. COUNT/AVG,
    // DISTINCT, ordered aggregates and approximate arithmetic retain raw input.
    if slots.is_empty()
        || !slots
            .iter()
            .all(|slot| slot.name.eq_ignore_ascii_case("SUM"))
        || !super::super::plan::ordered_rollup_can_cascade(slots, &template_row)
    {
        return Ok((input, original));
    }
    let Some(columns) = group_by
        .iter()
        .map(|expr| expr_column_index_in_template(expr, &input.template))
        .collect::<Option<Vec<_>>>()
    else {
        return Ok((input, original));
    };
    // Bound sampling scratch even for schemas containing unbounded values.
    if columns.iter().any(|&column| {
        let field = &input.template[column];
        !matches!(
            field.column_type,
            ColumnType::Tiny
                | ColumnType::Short
                | ColumnType::MediumInt
                | ColumnType::Long
                | ColumnType::LongLong
                | ColumnType::Date
                | ColumnType::Year
        ) && !(field.column_type == ColumnType::VarString
            && field.source.length.is_some_and(|length| length <= 1024))
    }) {
        return Ok((input, original));
    }
    let aggregates = slots
        .iter()
        .map(|slot| Expr::Function {
            name: slot.name.clone(),
            args: slot.args.clone(),
            distinct: false,
        })
        .collect::<Vec<_>>();
    if hash_agg_output_preflight_for_template(
        input.template.clone(),
        group_by,
        None,
        &aggregates,
        None,
    )
    .is_none()
    {
        return Ok((input, original));
    }
    let Some(budget) = mem_budget::current_budget() else {
        return Ok((input, original));
    };
    let sample_rows = (budget.remaining() / 1024).min(65536);
    if sample_rows < 4096 {
        return Ok((input, original));
    }
    let mut charge = mem_budget::ScopedCharge::new(0);
    const MAX_BATCHES: usize = 128;
    let headers = MAX_BATCHES * std::mem::size_of::<Batch>();
    if !charge.add_within(sample_rows * 32 + 65536 + headers, budget.remaining() / 8) {
        return Ok((input, original));
    }
    let mut seen = hashbrown::HashSet::<u64, ahash::RandomState>::with_capacity_and_hasher(
        sample_rows,
        ahash::RandomState::new(),
    );
    let hash = ahash::RandomState::new();
    let mut key = Vec::new();
    let mut batches = VecDeque::with_capacity(MAX_BATCHES);
    let mut rows = 0usize;
    let mut held_bytes = 0usize;
    while rows < sample_rows && held_bytes < budget.limit() / 16 && batches.len() < MAX_BATCHES {
        let Some(batch) = input.stream.next_batch()? else {
            break;
        };
        for row in batch.visible_indices().take(sample_rows - rows) {
            use std::hash::{BuildHasher, Hash, Hasher};
            let mut fingerprint = hash.build_hasher();
            for &column in &columns {
                let values = &batch.columns()[column];
                column.hash(&mut fingerprint);
                if values.is_null(row) {
                    0u8.hash(&mut fingerprint);
                    continue;
                }
                1u8.hash(&mut fingerprint);
                let text = values.valid_utf8_at(row).or_else(|| match values {
                    ColumnVector::Generic { values } => crate::types::value_str(&values[row]),
                    _ => None,
                });
                if let Some(text) = text {
                    // Raw text can underestimate collation-equivalent repeats,
                    // which conservatively declines this optional reduction.
                    text.hash(&mut fingerprint);
                } else {
                    key.clear();
                    crate::engine::executor::vec::key_encode::encode_from_column_row_with_collation(
                        values,
                        row,
                        input.template[column].source.collation_id,
                        &mut key,
                    );
                    key.hash(&mut fingerprint);
                }
            }
            // Fingerprint collisions affect only cost; the aggregate always
            // compares complete canonical SQL keys.
            seen.insert(fingerprint.finish());
            rows += 1;
        }
        held_bytes = held_bytes.saturating_add(batch.resident_bytes());
        batches.push_back(batch);
        crate::engine::cancel::check_cancelled()?;
    }
    let worth_reducing = sample_warrants_reduction(rows, seen.len(), estimated_rows);
    drop(seen);
    drop(key);
    let header_charge = charge.split(headers);
    drop(charge);
    input.stream = Box::new(Prefix {
        batches,
        input: input.stream,
        _charge: header_charge,
    });
    if !worth_reducing {
        return Ok((input, original));
    }
    let Some(partials) = build_vec_hash_agg_stream_from_input(
        db,
        input,
        group_by,
        &aggregates,
        false,
        scheduler_hash_agg_workers(),
        None,
        None,
    )?
    else {
        return Err(crate::error::catalog::ER_INTERNAL_ERROR
            .error("ROLLUP partial aggregation changed after preflight"));
    };
    record_pending_agg_metrics(partials.metrics);
    let mut template = partials.group_template;
    let mut merge_slots = original;
    for (index, slot) in merge_slots.iter_mut().enumerate() {
        let source = &partials.schema.fields()[group_by.len() + index];
        let name = format!("a{index}");
        let table = "\0mydb_rollup_partial";
        template.push(PlanField {
            meta: super::super::plan::intern_col_meta(ColMeta {
                table: table.into(),
                alias: None,
                column: name.clone(),
                column_type: source.column_type,
                source: source.source.clone(),
                invisible: false,
            }),
            value: Value::Null,
        });
        slot.args = vec![Expr::CompoundIdentifier(vec![table.into(), name])];
    }
    #[cfg(test)]
    PREAGGREGATE_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Ok((
        VecBatchInput {
            stream: partials.stream,
            template,
        },
        merge_slots,
    ))
}

fn sample_warrants_reduction(rows: usize, distinct: usize, estimated_rows: f64) -> bool {
    if rows < 4096 {
        return false;
    }
    if distinct * 10 <= rows * 9 {
        return true;
    }
    let repeats = rows.saturating_sub(distinct);
    // A short sample of a large domain can be mostly distinct even when the
    // complete stream repeats each key many times. Collision-frequency domain
    // estimation is a cost heuristic, not an equality proof. Require enough
    // observed repeats and a predicted twofold reduction to pay for the hash
    // stage; completely unique or unknown populations retain direct sorting.
    repeats >= 64.max(rows / 1024)
        && estimated_rows.is_finite()
        && (rows as f64 * (rows - 1) as f64) / (2.0 * repeats as f64) < estimated_rows * 0.5
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rollup_sampling_requires_evidence_before_projecting_reduction() {
        assert!(sample_warrants_reduction(65536, 65000, 12_000_000.0));
        assert!(!sample_warrants_reduction(65536, 65000, 1_000_000.0));
        assert!(!sample_warrants_reduction(65536, 65536, 1_000_000_000.0));
        assert!(!sample_warrants_reduction(1024, 512, 1_000_000.0));
        assert!(!sample_warrants_reduction(65536, 65000, f64::NAN));
    }

    #[test]
    fn sampled_rollup_reduction_preserves_exact_sums_and_replays_unique_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let engine =
            crate::engine::StorageQueryEngine::open_with_storage_config_without_background_workers(
                dir.path(),
                crate::engine::executor::tests::memory_test_storage_config(),
            )
            .unwrap();
        let schema = Arc::new(BatchSchema::new(vec![
            BatchField {
                table: "t".into(),
                alias: None,
                name: "g".into(),
                column_type: ColumnType::VarString,
                source: crate::types::ColumnSource {
                    length: Some(80),
                    collation_id: Some(255),
                    ..Default::default()
                },
            },
            BatchField {
                table: "t".into(),
                alias: None,
                name: "n".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            },
            BatchField {
                table: "t".into(),
                alias: None,
                name: "x".into(),
                column_type: ColumnType::Decimal {
                    precision: 30,
                    scale: 6,
                },
                source: Default::default(),
            },
        ]));
        let group_by = vec![Expr::Identifier("g".into()), Expr::Identifier("n".into())];
        let aggregate = Expr::Function {
            name: "SUM".into(),
            args: vec![Expr::Binary {
                left: Box::new(Expr::Identifier("x".into())),
                op: BinaryOp::Mul,
                right: Box::new(Expr::Literal(Literal::Int(2))),
            }],
            distinct: false,
        };
        let slots = super::super::super::aggregate::collect_aggregate_slots(&[&aggregate]);
        let make = |unique: bool| {
            let batch = Batch::new(
                schema.clone(),
                vec![
                    ColumnVector::from_values(
                        (0..16384)
                            .map(|i| {
                                if i % 17 == 0 {
                                    Value::Null
                                } else {
                                    Value::String(format!("label-{}", i % 7))
                                }
                            })
                            .collect(),
                    ),
                    ColumnVector::from_values(
                        (0..16384)
                            .map(|i| Value::Int(if unique { i } else { i % 23 }))
                            .collect(),
                    ),
                    ColumnVector::from_values(
                        (0..16384)
                            .map(|i| {
                                if i % 11 == 0 {
                                    Value::Null
                                } else {
                                    Value::DecimalCompact((i - 8000) * 1234567, 6)
                                }
                            })
                            .collect(),
                    ),
                ],
            );
            VecBatchInput {
                stream: Box::new(MaterializedBatchOperator::new(schema.clone(), vec![batch])),
                template: schema.fields().iter().map(batch_field_template).collect(),
            }
        };
        let finish = |input: VecBatchInput<'_>, slots: &[AggregateSlot]| {
            let schema = input.stream.schema().clone();
            let fields = (0..2)
                .map(|column| crate::engine::executor::sort::SortField {
                    ascending: true,
                    nulls_first: true,
                    collation_id: schema.fields()[column].source.collation_id,
                })
                .collect();
            let sorted = vec_sort::ExternalSortBatchOperator::new(
                None,
                input.stream,
                schema,
                BatchProperties::UNORDERED_SINGLE,
                vec![
                    vec_sort::SortKeySource::Column(0),
                    vec_sort::SortKeySource::Column(1),
                ],
                fields,
                dir.path(),
            );
            let stream = Box::new(SortedPlanRowStream {
                input: sorted,
                template: input.template,
            });
            let rows = super::super::super::plan::aggregate_rollup_plan_stream_sorted(
                stream, &group_by, None, slots,
            )
            .unwrap();
            let mut values = rows
                .into_iter()
                .map(|row| {
                    let mut values = row
                        .fields
                        .iter()
                        .map(|field| crate::engine::expr::ValueKey::from(&field.value))
                        .collect::<Vec<_>>();
                    let cache = row.agg_values.unwrap();
                    values.push(crate::engine::expr::ValueKey::from(&cache[&slots[0].key]));
                    values.push(crate::engine::expr::ValueKey::from(
                        &cache[&super::super::super::aggregate::ROLLUP_MASK_AGG_KEY],
                    ));
                    format!("{values:?}")
                })
                .collect::<Vec<_>>();
            values.sort();
            values
        };
        for unique in [false, true] {
            let expected = mem_budget::with_query_budget(
                Arc::new(mem_budget::MemBudget::new(64 << 20)),
                || finish(make(unique), &slots),
            );
            let actual = mem_budget::with_query_budget(
                Arc::new(mem_budget::MemBudget::new(64 << 20)),
                || {
                    let (input, merge_slots) =
                        reduce(engine.database(), make(unique), &group_by, &slots, 16384.0)
                            .unwrap();
                    assert_eq!(merge_slots[0].args == slots[0].args, unique);
                    finish(input, &merge_slots)
                },
            );
            assert_eq!(actual, expected, "unique={unique}");
        }
    }
}
