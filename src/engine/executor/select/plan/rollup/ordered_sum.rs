//! Reuse live exact-SUM prefixes and copy only retained window candidates.
use super::*;
use crate::engine::executor::mem_budget::{self, ScopedCharge};
use crate::engine::executor::memory_footprint::AllocatedBytes;
use crate::engine::executor::select::aggregate::{self, NumericAggState};

#[cfg(test)]
pub(super) static HITS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

struct Level {
    keys: Vec<Value>,
    sums: Vec<NumericAggState>,
    active: bool,
    _charge: ScopedCharge,
}
impl Level {
    fn new(prefix: usize, slots: usize) -> Result<Self, QueryError> {
        let mut charge = ScopedCharge::new(0);
        // Exact SQL numeric inputs have bounded precision. Leave room for
        // promoted subtotals as well as the flat state/representative arrays.
        charge.add_force(
            prefix
                .saturating_mul(std::mem::size_of::<Value>())
                .saturating_add(
                    slots.saturating_mul(std::mem::size_of::<NumericAggState>() + 1024),
                ),
        )?;
        let mut keys = mem_budget::try_vec_with_capacity(prefix, "ordered SUM prefix keys")?;
        keys.resize(prefix, Value::Null);
        let mut sums = mem_budget::try_vec_with_capacity(slots, "ordered SUM prefix states")?;
        sums.resize_with(slots, NumericAggState::default);
        Ok(Self {
            keys,
            sums,
            active: false,
            _charge: charge,
        })
    }
    fn seed(&mut self, values: &[std::borrow::Cow<'_, Value>]) -> Result<(), QueryError> {
        let metadata = self.keys.capacity() * std::mem::size_of::<Value>()
            + self.sums.capacity() * (std::mem::size_of::<NumericAggState>() + 1024);
        // clone_from reuses existing string buffers. Admit old/new overlap
        // and possible geometric growth before replacing a representative.
        let bound = metadata
            .saturating_add(
                self.keys
                    .iter()
                    .map(AllocatedBytes::allocated_bytes)
                    .sum::<usize>(),
            )
            .saturating_add(
                values
                    .iter()
                    .take(self.keys.len())
                    .map(|v| v.as_ref().allocated_bytes().saturating_mul(2))
                    .sum::<usize>(),
            );
        if bound > self._charge.charged() {
            self._charge.add_force(bound - self._charge.charged())?;
        }
        for (key, value) in self.keys.iter_mut().zip(values) {
            key.clone_from(value.as_ref());
        }
        self.active = true;
        Ok(())
    }
}

fn close(
    levels: &mut [Level],
    level: usize,
    slots: &[aggregate::AggregateSlot],
    out: &mut RollupTopKCollector,
) -> Result<(), QueryError> {
    if !levels[level].active {
        return Ok(());
    }
    let values = levels[level]
        .sums
        .iter()
        .map(aggregate::numeric_state_to_value)
        .collect::<smallvec::SmallVec<[Value; 4]>>();
    if level > 0 {
        let (parents, children) = levels.split_at_mut(level);
        let parent = &mut parents[level - 1];
        let child = &mut children[0];
        for (target, source) in parent.sums.iter_mut().zip(&mut child.sums) {
            aggregate::merge_numeric_state(target, std::mem::take(source));
        }
    } else {
        for sum in &mut levels[level].sums {
            *sum = NumericAggState::Empty;
        }
    }
    out.push_borrowed_sums(slots, level, &levels[level].keys, &values)?;
    levels[level].active = false;
    Ok(())
}

pub(super) fn execute(
    mut stream: Box<dyn PlanStream + '_>,
    mut row: PlanRow,
    groups: &[Expr],
    slots: &[aggregate::AggregateSlot],
    mut out: RollupTopKCollector,
) -> Result<Vec<PlanRow>, QueryError> {
    #[cfg(test)]
    HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let schema = super::super::eval::input_schema_for_plan_row(&row);
    let compiled = groups
        .iter()
        .map(|expr| super::super::eval::CompiledPlanExpr::compile_once(expr, &schema))
        .collect::<Vec<_>>();
    let args = slots
        .iter()
        .map(|slot| {
            super::super::eval::CompiledPlanExpr::compile_once(
                slot.args.first().expect("exact SUM argument"),
                &schema,
            )
        })
        .collect::<Vec<_>>();
    let collations = groups
        .iter()
        .map(|expr| super::super::eval::plan_expr_collation(expr, &row).map(|c| c.id))
        .collect::<Vec<_>>();
    let n = groups.len();
    let mut metadata = ScopedCharge::new(0);
    metadata.add_force(
        (n + 1)
            .saturating_mul(std::mem::size_of::<Level>())
            .saturating_add((n + slots.len()).saturating_mul(128)),
    )?;
    let mut levels = (0..=n)
        .map(|prefix| Level::new(prefix, slots.len()))
        .collect::<Result<Vec<_>, _>>()?;
    loop {
        crate::engine::cancel::check_cancelled()?;
        let values = compiled
            .iter()
            .map(|expr| {
                expr.try_borrow_field(&row).map_or_else(
                    || expr.eval(&row).map(std::borrow::Cow::Owned),
                    |v| Ok(std::borrow::Cow::Borrowed(v)),
                )
            })
            .collect::<Result<smallvec::SmallVec<[_; 8]>, QueryError>>()?;
        if levels[n].active {
            let lcp = levels[n]
                .keys
                .iter()
                .zip(&values)
                .zip(&collations)
                .take_while(|((old, new), collation)| rollup_group_value_eq(old, new, **collation))
                .count();
            for level in (lcp + 1..=n).rev() {
                close(&mut levels, level, slots, &mut out)?;
            }
        }
        for level in &mut levels {
            if !level.active {
                level.seed(&values)?;
            }
        }
        for (state, expr) in levels[n].sums.iter_mut().zip(&args) {
            if let Some(value) = expr.try_borrow_field(&row) {
                aggregate::accumulate_numeric_state(state, value)?;
            } else {
                aggregate::accumulate_numeric_state(state, &expr.eval(&row)?)?;
            }
        }
        drop(values);
        if !stream.next_into(&mut row)? {
            break;
        }
    }
    for level in (0..=n).rev() {
        close(&mut levels, level, slots, &mut out)?;
    }
    Ok(out.into_rows())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::dispatch::RankKind;
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    struct Rows(std::vec::IntoIter<PlanRow>);
    impl PlanStream for Rows {
        fn next(&mut self) -> Result<Option<PlanRow>, QueryError> {
            Ok(self.0.next())
        }
    }
    fn input() -> Vec<PlanRow> {
        let mut rows = Vec::new();
        for category in [
            Value::Null,
            Value::String(String::new()),
            Value::String("Å日本語 long representative".into()),
            Value::String("å日本語 long representative".into()),
            Value::String("tail".into()),
        ] {
            for key in [Value::Null, Value::Int(1), Value::Int(2), Value::Int(3)] {
                for value in [
                    Value::Null,
                    Value::DecimalCompact(-5, 2),
                    Value::Decimal128(i128::MAX / 2, 2),
                    Value::Decimal128(i128::MAX / 2, 2),
                ] {
                    let fields = [
                        ("k", ColumnType::VarString, category.clone()),
                        ("n", ColumnType::LongLong, key.clone()),
                        (
                            "v",
                            ColumnType::Decimal {
                                precision: 65,
                                scale: 2,
                            },
                            if key.storage_eq(&Value::Int(3)) {
                                Value::Null
                            } else {
                                value
                            },
                        ),
                        ("u", ColumnType::LongLong, Value::UInt(u64::MAX)),
                        ("f", ColumnType::Double, Value::Double(0.5)),
                    ]
                    .into_iter()
                    .map(|(name, column_type, value)| PlanField {
                        meta: intern_col_meta(ColMeta {
                            column: name.into(),
                            column_type,
                            source: crate::types::ColumnSource {
                                collation_id: (name == "k").then_some(45),
                                ..Default::default()
                            },
                            ..Default::default()
                        }),
                        value,
                    })
                    .collect();
                    rows.push(PlanRow::from_fields(fields));
                }
            }
        }
        let field = crate::engine::executor::sort::SortField {
            ascending: true,
            nulls_first: true,
            collation_id: Some(45),
        };
        rows.sort_by(|a, b| {
            crate::engine::executor::sort::compare_value_for_sort(
                &a.fields[0].value,
                &b.fields[0].value,
                field,
            )
            .then_with(|| {
                crate::engine::executor::sort::compare_value_for_sort(
                    &a.fields[1].value,
                    &b.fields[1].value,
                    field,
                )
            })
        });
        rows
    }
    fn canonical(rows: Vec<PlanRow>) -> Vec<(Vec<String>, Vec<(u64, String)>)> {
        let mut rows = rows
            .into_iter()
            .map(|row| {
                let values = row
                    .fields
                    .iter()
                    .map(|field| field.value.to_display_string())
                    .collect();
                let mut aggregates = row
                    .agg_values
                    .unwrap_or_default()
                    .iter()
                    .map(|(key, value)| (*key, value.to_display_string()))
                    .collect::<Vec<_>>();
                aggregates.sort();
                (values, aggregates)
            })
            .collect::<Vec<_>>();
        rows.sort();
        rows
    }
    #[test]
    fn reusable_ordered_sum_preserves_wide_exact_values_null_prefixes_and_rank_peers() {
        let groups = vec![Expr::Identifier("k".into()), Expr::Identifier("n".into())];
        let expressions = ["SUM(COALESCE(v,0))", "SUM(u)", "SUM(v)"]
            .into_iter()
            .map(|sql| crate::parser::parse_expr_fragment(sql).unwrap())
            .collect::<Vec<_>>();
        let slots = aggregate::collect_aggregate_slots(&expressions.iter().collect::<Vec<_>>());
        for kind in [RankKind::RowNumber, RankKind::Rank, RankKind::DenseRank] {
            for asc in [false, true] {
                let hint = RollupWindowTopKHint {
                    partition_by: vec![Expr::GroupKeyRef {
                        index: 0,
                        expr: Box::new(groups[0].clone()),
                    }],
                    order_by: expressions
                        .iter()
                        .map(|expr| crate::parser::OrderBy {
                            names_bound: false,
                            expr: expr.clone(),
                            asc,
                            nulls_first: Some(false),
                        })
                        .collect(),
                    k: 2,
                    kind,
                };
                let run = |enabled| {
                    crate::util::env::with_vars(
                        &[(
                            "MYDB_ORDERED_ROLLUP_REUSE",
                            Some(if enabled { "on" } else { "off" }),
                        )],
                        || {
                            let _hint = super::super::super::set_rollup_window_topk_hint(Some(
                                hint.clone(),
                            ));
                            aggregate_rollup_plan_stream_sorted(
                                Box::new(Rows(input().into_iter())),
                                &groups,
                                None,
                                &slots,
                            )
                            .unwrap()
                        },
                    )
                };
                let expected = run(false);
                let hits = HITS.load(std::sync::atomic::Ordering::Relaxed);
                let budget = Arc::new(MemBudget::new(8 << 20));
                let actual = with_query_budget(budget.clone(), || run(true));
                assert_eq!(canonical(actual), canonical(expected));
                assert!(HITS.load(std::sync::atomic::Ordering::Relaxed) > hits);
                assert_eq!(budget.used(), 0);
            }
        }
    }

    #[test]
    fn reusable_ordered_sum_declines_approximate_distinct_and_having_and_propagates_stream_errors()
    {
        let groups = vec![Expr::Identifier("k".into()), Expr::Identifier("n".into())];
        for (sql, having) in [
            ("SUM(f)", None),
            ("SUM(DISTINCT v)", None),
            ("SUM(v)", Some("SUM(v)>0")),
        ] {
            let expr = crate::parser::parse_expr_fragment(sql).unwrap();
            let slots = aggregate::collect_aggregate_slots(&[&expr]);
            let having = having.map(|sql| crate::parser::parse_expr_fragment(sql).unwrap());
            let hint = RollupWindowTopKHint {
                partition_by: vec![],
                order_by: vec![crate::parser::OrderBy {
                    names_bound: false,
                    expr,
                    asc: false,
                    nulls_first: Some(false),
                }],
                k: 2,
                kind: RankKind::Rank,
            };
            let _hint = super::super::super::set_rollup_window_topk_hint(Some(hint));
            let hits = HITS.load(std::sync::atomic::Ordering::Relaxed);
            aggregate_rollup_plan_stream_sorted(
                Box::new(Rows(input().into_iter())),
                &groups,
                having.as_ref(),
                &slots,
            )
            .unwrap();
            assert_eq!(HITS.load(std::sync::atomic::Ordering::Relaxed), hits);
        }
        struct Failed {
            rows: std::vec::IntoIter<PlanRow>,
            remaining: usize,
        }
        impl PlanStream for Failed {
            fn next(&mut self) -> Result<Option<PlanRow>, QueryError> {
                if self.remaining == 0 {
                    return Err(crate::error::catalog::ER_QUERY_INTERRUPTED
                        .error("injected stream failure"));
                }
                self.remaining -= 1;
                Ok(self.rows.next())
            }
        }
        let expr = crate::parser::parse_expr_fragment("SUM(v)").unwrap();
        let slots = aggregate::collect_aggregate_slots(&[&expr]);
        let hint = RollupWindowTopKHint {
            partition_by: vec![],
            order_by: vec![crate::parser::OrderBy {
                names_bound: false,
                expr,
                asc: false,
                nulls_first: Some(false),
            }],
            k: 2,
            kind: RankKind::Rank,
        };
        let _hint = super::super::super::set_rollup_window_topk_hint(Some(hint));
        let budget = Arc::new(MemBudget::new(8 << 20));
        let result = with_query_budget(budget.clone(), || {
            aggregate_rollup_plan_stream_sorted(
                Box::new(Failed {
                    rows: input().into_iter(),
                    remaining: 2,
                }),
                &groups,
                None,
                &slots,
            )
        });
        assert!(result.is_err());
        assert_eq!(budget.used(), 0);
    }
}
