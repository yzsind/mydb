//! Budgeted deterministic rank accumulation and final selection.
use super::*;

struct Entry {
    identity: Vec<Value>,
    representation: crate::engine::expr::InlineKeyBuf,
    score: f64,
    best: u64,
    matches: u64,
    ranks: [u64; 8],
    ordered: Option<Vec<Option<Vec<Value>>>>,
}

struct FusionStream {
    rows: std::vec::IntoIter<super::super::plan::PlanRow>,
    _charge: crate::engine::executor::mem_budget::ScopedCharge,
}
impl super::super::plan::PlanStream for FusionStream {
    fn next(&mut self) -> Result<Option<super::super::plan::PlanRow>, QueryError> {
        crate::engine::cancel::check_cancelled()?;
        Ok(self.rows.next())
    }
}

fn expression_value(row: &super::super::plan::PlanRow, expr: &Expr) -> Result<Value, QueryError> {
    let item = SelectItem::Expr {
        expr: expr.clone(),
        alias: None,
    };
    super::super::eval::project_item(row, &item)
        .into_iter()
        .next()
        .ok_or_else(|| invalid("empty identity expression"))?
}

pub(in crate::engine::executor::select) fn open_plan(
    db: &Arc<Database>,
    txn: TxnView,
    plan: &PhysicalPlan,
) -> Result<Box<dyn super::super::plan::PlanStream>, QueryError> {
    use super::super::plan::{intern_col_meta, ColMeta, PlanField, PlanRow};
    use crate::util::fast_hash::ah_hash_map;
    let PhysicalPlan::RrfFusion {
        inputs,
        spec,
        output_window,
        columns,
        ..
    } = plan
    else {
        unreachable!()
    };
    let (constant, weights) = bind_options(spec)?;
    let mut statistics_result = Ok(());
    for input in inputs {
        input.walk(|node| {
            if statistics_result.is_ok() {
                if let PhysicalPlan::FulltextScan { table, index, .. } = node {
                    statistics_result =
                        super::super::fulltext_context::statistics(table, index).map(|_| ());
                }
            }
        });
    }
    statistics_result?;
    let width = columns.len() - 3 - usize::from(spec.score_details);
    let collations = columns[..width]
        .iter()
        .map(|column| column.source.collation_id)
        .collect::<Vec<_>>();
    let mut state: crate::util::fast_hash::AhHashMap<crate::engine::expr::InlineKeyBuf, Entry> =
        ah_hash_map();
    let recording = super::super::analyze_stats::is_recording();
    let mut input_rows = [0u64; 8];
    let mut unique_rows = [0u64; 8];
    let mut fusion_ns = 0u128;
    let mut charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
    for (branch_index, input) in inputs.iter().enumerate() {
        crate::engine::cancel::check_cancelled()?;
        if weights[branch_index] == 0.0 {
            continue;
        }
        with_branch_scope(db, txn, &spec.branches[branch_index].query, |select| {
            let mut stream = super::super::plan::execute_plan_stream(db, txn, input)?;
            let mut rank = 0u64;
            let projection =
                super::super::vec_bridge::final_projection(input).unwrap_or(&select.projection);
            if projection.len() != width {
                return Err(invalid("planned identity width changed"));
            }
            while let Some(row) = stream.next()? {
                let start = recording.then(std::time::Instant::now);
                crate::engine::cancel::check_cancelled()?;
                input_rows[branch_index] += 1;
                rank = rank
                    .checked_add(1)
                    .ok_or_else(|| invalid("rank overflow"))?;
                let mut identity = Vec::with_capacity(width);
                for (item, column) in projection.iter().zip(&columns[..width]) {
                    let SelectItem::Expr { expr, .. } = item else {
                        return Err(invalid("explicit identity expressions are required"));
                    };
                    let mut value = coerce_identity(expression_value(&row, expr)?, column)?;
                    match &mut value {
                        Value::Null => {
                            return Err(invalid(
                                "identity components must not be NULL; filter NULL explicitly",
                            ))
                        }
                        Value::Float(n) if !n.is_finite() => {
                            return Err(invalid("identity must be finite"))
                        }
                        Value::Double(n) if !n.is_finite() => {
                            return Err(invalid("identity must be finite"))
                        }
                        Value::Float(n) if *n == 0.0 => *n = 0.0,
                        Value::Double(n) if *n == 0.0 => *n = 0.0,
                        _ => {}
                    }
                    identity.push(value);
                }
                let key = crate::engine::executor::dispatch::set_row_key(&collations, &identity);
                let representation = crate::engine::executor::dispatch::set_row_key(&[], &identity);
                if !state.contains_key(&key) {
                    let payload = identity.iter().map(Value::heap_capacity).sum::<usize>();
                    charge.add_non_spillable(
                        512usize
                            .saturating_add(width * std::mem::size_of::<Value>())
                            .saturating_add(payload)
                            .saturating_add(key.capacity())
                            .saturating_add(representation.capacity())
                            .saturating_add(if spec.score_details {
                                inputs.len() * 64
                            } else {
                                0
                            }),
                    )?;
                    state.try_reserve(1).map_err(|_| {
                        crate::error::catalog::ER_OUTOFMEMORY
                            .error("RRF identity map allocation failed")
                    })?;
                    state.insert(
                        key.clone(),
                        Entry {
                            identity,
                            representation,
                            score: 0.0,
                            best: u64::MAX,
                            matches: 0,
                            ranks: [0; 8],
                            ordered: spec.score_details.then(|| vec![None; inputs.len()]),
                        },
                    );
                } else if let Some(entry) = state.get_mut(&key) {
                    if representation < entry.representation {
                        let old = entry
                            .identity
                            .iter()
                            .map(Value::heap_capacity)
                            .sum::<usize>()
                            + entry.representation.capacity();
                        let new = identity.iter().map(Value::heap_capacity).sum::<usize>()
                            + representation.capacity();
                        charge.add_non_spillable(new.saturating_sub(old))?;
                        entry.identity = identity;
                        entry.representation = representation;
                    }
                }
                let entry = state.get_mut(&key).expect("identity inserted");
                if entry.ranks[branch_index] != 0 {
                    if let Some(start) = start {
                        fusion_ns += start.elapsed().as_nanos();
                    }
                    continue;
                }
                unique_rows[branch_index] += 1;
                entry.ranks[branch_index] = rank;
                entry.score += weights[branch_index] / (constant + rank as f64);
                entry.best = entry.best.min(rank);
                entry.matches += 1;
                if let Some(ordered) = entry.ordered.as_mut() {
                    let values = select
                        .order_by
                        .iter()
                        .map(|order| expression_value(&row, &order.expr))
                        .collect::<Result<Vec<_>, _>>()?;
                    charge.add_non_spillable(
                        values.capacity() * std::mem::size_of::<Value>()
                            + values.iter().map(Value::heap_capacity).sum::<usize>(),
                    )?;
                    ordered[branch_index] = Some(values);
                }
                if let Some(start) = start {
                    fusion_ns += start.elapsed().as_nanos();
                }
            }
            Ok(())
        })
        .map_err(|error| {
            let message = format!(
                "RRF branch '{}': {}",
                spec.branches[branch_index].name,
                error.message()
            );
            error.with_message(message)
        })?;
        #[cfg(test)]
        if let Some(hook) = branch_hook() {
            hook(branch_index);
        }
    }
    let start = recording.then(std::time::Instant::now);
    let union_rows = state.len() as u64;
    let overlap_rows = state.values().filter(|entry| entry.matches > 1).count() as u64;
    let window = output_window
        .as_ref()
        .map(|window| {
            let limit = super::super::limit_value_or_zero(Some(&window.limit))?;
            let offset = super::super::limit_value_or_zero(window.offset.as_ref())?;
            if limit == 0 {
                Ok(0)
            } else {
                offset
                    .checked_add(limit)
                    .ok_or_else(|| invalid("output OFFSET plus LIMIT overflow"))
            }
        })
        .transpose()?;
    let mut entries = if let Some(k) = window.filter(|k| *k < state.len()) {
        charge.add_non_spillable(k.saturating_mul(std::mem::size_of::<HeapEntry<'_>>()))?;
        let mut heap = std::collections::BinaryHeap::<HeapEntry<'_>>::with_capacity(k);
        for entry in state.into_values() {
            crate::engine::cancel::check_cancelled()?;
            if k == 0 {
                continue;
            }
            if heap.len() < k {
                heap.push(HeapEntry {
                    entry,
                    collations: &collations,
                });
            } else if heap
                .peek()
                .is_some_and(|worst| compare_entries(&entry, &worst.entry, &collations).is_lt())
            {
                *heap.peek_mut().expect("nonempty fusion heap") = HeapEntry {
                    entry,
                    collations: &collations,
                };
            }
        }
        heap.into_iter()
            .map(|entry| entry.entry)
            .collect::<Vec<_>>()
    } else {
        state.into_values().collect::<Vec<_>>()
    };
    entries.sort_unstable_by(|left, right| compare_entries(left, right, &collations));
    let metas = columns
        .iter()
        .map(|column| {
            intern_col_meta(ColMeta {
                table: "$rrf".into(),
                alias: None,
                column: column.name.clone(),
                column_type: column.column_type,
                source: column.source.clone(),
                invisible: false,
            })
        })
        .collect::<Vec<_>>();
    charge.add_non_spillable(entries.len().saturating_mul(
        std::mem::size_of::<PlanRow>() + columns.len() * std::mem::size_of::<PlanField>(),
    ))?;
    let mut rows = Vec::with_capacity(entries.len());
    for entry in entries {
        crate::engine::cancel::check_cancelled()?;
        let mut values = entry.identity;
        values.push(Value::Double(entry.score));
        values.push(Value::UInt(entry.best));
        values.push(Value::UInt(entry.matches));
        if spec.score_details {
            let details=spec.branches.iter().enumerate().map(|(i,branch)| {
                let rank=entry.ranks[i];
                let order_values=entry.ordered.as_ref().and_then(|ordered|ordered[i].as_ref()).map(|values|values.iter().map(ordered_json).collect::<Vec<_>>());
                serde_json::json!({"branch_id":i+1,"name":branch.name,"weight":weights[i],"disabled":weights[i]==0.0,"rank":(rank!=0).then_some(rank),"contribution":if rank==0 {0.0}else{weights[i]/(constant+rank as f64)},"ordered_values":order_values,"ordering":branch.query.leftmost_select().map(|select|select.order_by.iter().map(|order|serde_json::json!({"ascending":order.asc,"nulls_first":order.nulls_first_effective()})).collect::<Vec<_>>()),"ranker":ranker(&branch.query)})
            }).collect::<Vec<_>>();
            let bytes = crate::engine::json::from_serde(&serde_json::Value::Array(details));
            charge.add_non_spillable(bytes.capacity())?;
            values.push(Value::Json(bytes));
        }
        rows.push(PlanRow::from_fields(
            values
                .into_iter()
                .zip(&metas)
                .map(|(value, meta)| PlanField {
                    meta: meta.clone(),
                    value,
                })
                .collect(),
        ));
    }
    if let Some(start) = start {
        fusion_ns += start.elapsed().as_nanos();
    }
    super::super::analyze_stats::add_peak_memory(plan, charge.charged() as u64);
    super::super::analyze_stats::add_rrf_work(
        plan,
        inputs.len(),
        input_rows,
        unique_rows,
        union_rows,
        overlap_rows,
        fusion_ns,
    );
    Ok(Box::new(FusionStream {
        rows: rows.into_iter(),
        _charge: charge,
    }))
}

fn compare_entries(left: &Entry, right: &Entry, collations: &[Option<u16>]) -> std::cmp::Ordering {
    use crate::engine::executor::sort::{compare_value_for_sort, SortField};
    right
        .score
        .total_cmp(&left.score)
        .then_with(|| left.best.cmp(&right.best))
        .then_with(|| {
            left.identity
                .iter()
                .zip(&right.identity)
                .zip(collations)
                .map(|((left, right), collation)| {
                    compare_value_for_sort(
                        left,
                        right,
                        SortField {
                            ascending: true,
                            nulls_first: true,
                            collation_id: *collation,
                        },
                    )
                })
                .find(|order| !order.is_eq())
                .unwrap_or_else(|| left.representation.cmp(&right.representation))
        })
}
struct HeapEntry<'a> {
    entry: Entry,
    collations: &'a [Option<u16>],
}
impl PartialEq for HeapEntry<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for HeapEntry<'_> {}
impl PartialOrd for HeapEntry<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapEntry<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        compare_entries(&self.entry, &other.entry, self.collations)
    }
}
