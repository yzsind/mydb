//! Pull-based SQL ROWID scans. Physical codecs never see the virtual column.

use super::*;
use std::collections::VecDeque;

pub(in crate::engine::executor::select) fn is_scan(plan: &PhysicalPlan) -> bool {
    scan_parts(plan).is_some_and(|(table, _, _)| {
        crate::storage::rowid::query_column_index(&table.schema).is_some()
    })
}

pub(in crate::engine::executor::select) fn is_direct(
    db: &Database,
    plan: &PhysicalPlan,
) -> Result<bool, QueryError> {
    let Some((table, filter, _)) = scan_parts(plan) else {
        return Ok(false);
    };
    if crate::storage::rowid::query_column_index(&table.schema).is_none() {
        return Ok(false);
    }
    filter.as_ref().map_or(Ok(false), |filter| {
        crate::engine::executor::rowid::candidates_for(
            &table.schema,
            table.alias.as_deref(),
            filter,
        )
        .and_then(|rows| match rows {
            Some(rows) => crate::engine::executor::rowid::lookup_cost(db, table, &rows)
                .map(|(_, prefer)| prefer),
            None => Ok(false),
        })
    })
}

fn scan_parts(
    plan: &PhysicalPlan,
) -> Option<(&PlanTable, &Option<Expr>, &Option<Arc<Vec<usize>>>)> {
    match plan {
        PhysicalPlan::SeqScan {
            table,
            filter,
            projected_columns,
            ..
        }
        | PhysicalPlan::ZoneMapScan {
            table,
            filter,
            projected_columns,
            ..
        }
        | PhysicalPlan::ParallelSeqScan {
            table,
            filter,
            projected_columns,
            ..
        }
        | PhysicalPlan::ParallelZoneMapScan {
            table,
            filter,
            projected_columns,
            ..
        } => Some((table, filter, projected_columns)),
        _ => None,
    }
}

pub(super) fn open(
    db: &Arc<Database>,
    txn: TxnView,
    plan: &PhysicalPlan,
) -> Result<Option<Box<dyn PlanStream>>, QueryError> {
    if !is_scan(plan) {
        return Ok(None);
    }
    let (table, filter, projection) = scan_parts(plan).expect("ROWID scan source");
    let txn = crate::engine::executor::temporal::txn_for_table(txn, table);
    db.check_primary_read_view(&table.schema, txn)?;
    let columns = projection
        .as_ref()
        .map(|cols| cols.as_ref().clone())
        .unwrap_or_else(|| logical_visible_column_indices(&table.schema));
    let mut access = filter
        .as_ref()
        .map(|expr| {
            crate::engine::executor::rowid::access(&table.schema, table.alias.as_deref(), expr)
        })
        .transpose()?
        .flatten();
    if let Some(candidate) = &access {
        if !crate::engine::executor::rowid::lookup_cost(db, table, &candidate.rows)?.1 {
            access = None;
        }
    }
    let rowid_key = access
        .as_ref()
        .and_then(|_| super::super::analyze_stats::plan_key(plan));
    let mut work = access.as_ref().map(|access| access.stats);
    let mut ids = access.map(|access| access.rows);
    if let Some(ids) = &mut ids {
        let allowed = table.resolved_partition_storage_ids(filter.as_ref());
        let current = crate::engine::executor::partition_router::current_pruned_partitions();
        let before = ids.len();
        ids.retain(|row| {
            allowed
                .as_ref()
                .is_none_or(|ids| ids.contains(&row.storage_id))
                && current
                    .as_ref()
                    .is_none_or(|ids| ids.contains(&row.storage_id))
        });
        if let Some(work) = &mut work {
            work.misses += (before - ids.len()) as u64;
        }
    }
    if let Some(work) = work {
        super::super::analyze_stats::add_rowid_work(rowid_key, work);
    }
    let mut tasks = VecDeque::new();
    if ids.is_none() {
        let zone_map = matches!(
            plan,
            PhysicalPlan::ZoneMapScan { .. } | PhysicalPlan::ParallelZoneMapScan { .. }
        );
        tasks.extend(
            super::super::parallel::scan_tasks(db, table, filter.as_ref(), zone_map)?
                .into_iter()
                .map(|task| (task.storage_id, task.page_lo, task.page_hi)),
        );
    }
    let mut candidate_charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
    candidate_charge.add_force(ids.as_ref().map_or(0, |ids| {
        ids.capacity() * std::mem::size_of::<crate::storage::DmlRowRef>()
    }))?;
    Ok(Some(Box::new(RowIdStream {
        db: Arc::clone(db),
        txn,
        table: table.clone(),
        filter: filter.clone(),
        columns,
        ids: ids.map(VecDeque::from),
        tasks,
        pending: VecDeque::new(),
        pending_charge: crate::engine::executor::mem_budget::ScopedCharge::new(0),
        _candidate_charge: candidate_charge,
        rowid_key,
        last_storage: None,
        eval_ctx: EvalQueryContext::default(),
        compiled_filter: None,
    })))
}

struct RowIdStream {
    db: Arc<Database>,
    txn: TxnView,
    table: PlanTable,
    filter: Option<Expr>,
    columns: Vec<usize>,
    ids: Option<VecDeque<crate::storage::DmlRowRef>>,
    tasks: VecDeque<(u64, u64, u64)>,
    pending: VecDeque<PlanRow>,
    pending_charge: crate::engine::executor::mem_budget::ScopedCharge,
    _candidate_charge: crate::engine::executor::mem_budget::ScopedCharge,
    rowid_key: Option<crate::planner::PlanNodeId>,
    last_storage: Option<u64>,
    eval_ctx: EvalQueryContext,
    compiled_filter: Option<CompiledPlanPredicate>,
}

impl PlanStream for RowIdStream {
    fn next(&mut self) -> Result<Option<PlanRow>, QueryError> {
        loop {
            crate::engine::cancel::check_cancelled()?;
            if let Some(row) = self.pending.pop_front() {
                return Ok(Some(row));
            }
            self.pending_charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
            let mut values = Vec::new();
            if let Some(ids) = &mut self.ids {
                if ids.is_empty() {
                    return Ok(None);
                }
                let storage_id = ids.front().expect("nonempty address batch").storage_id;
                if self.table.schema.is_partitioned() && self.last_storage != Some(storage_id) {
                    crate::engine::executor::partition_router::record_partition_visit();
                    self.last_storage = Some(storage_id);
                }
                let mut row_ids = Vec::with_capacity(128);
                for _ in 0..128 {
                    if ids.front().is_none_or(|row| row.storage_id != storage_id) {
                        break;
                    }
                    let Some(row) = ids.pop_front() else { break };
                    row_ids.push(row.row_id);
                }
                let heap = self.db.cached_heap_handle(storage_id)?;
                let rows = self.db.read_row_columns_many_with_handle(
                    &heap,
                    &self.table.schema,
                    &row_ids,
                    self.txn,
                    &self.columns,
                )?;
                let fetched = rows.len() as u64;
                let misses = (row_ids.len() as u64).saturating_sub(fetched);
                for (_, row) in rows {
                    self.pending_charge.add_force(
                        crate::engine::executor::mem_budget::estimate_row_bytes(&row),
                    )?;
                    values.push(row);
                }
                super::super::analyze_stats::add_rowid_work(
                    self.rowid_key,
                    crate::engine::executor::rowid::AccessStats {
                        misses,
                        fetched,
                        ..Default::default()
                    },
                );
            } else {
                let Some((storage, lo, hi)) = self.tasks.pop_front() else {
                    return Ok(None);
                };
                let end = lo.saturating_add(16).min(hi);
                if end < hi {
                    self.tasks.push_front((storage, end, hi));
                }
                if self.table.schema.is_partitioned() && self.last_storage != Some(storage) {
                    crate::engine::executor::partition_router::record_partition_visit();
                    self.last_storage = Some(storage);
                }
                self.db.scan_rows_columns_storage_streaming_range_with_ids(
                    &self.table.schema,
                    storage,
                    self.txn,
                    &self.columns,
                    lo,
                    end,
                    |_, row| {
                        self.pending_charge.add_force(
                            crate::engine::executor::mem_budget::estimate_row_bytes(row),
                        )?;
                        values.push(row.to_vec());
                        Ok(())
                    },
                )?;
            }
            for values in values {
                let row = projected_values_to_plan_row(&self.table, &self.columns, values)?;
                self.pending_charge
                    .add_force(row.fields.capacity() * std::mem::size_of::<PlanField>())?;
                if eval_covering_filter(
                    &self.db,
                    self.txn,
                    &self.filter,
                    &row,
                    &mut self.eval_ctx,
                    &mut self.compiled_filter,
                )? {
                    self.pending.push_back(row);
                }
            }
        }
    }
}
