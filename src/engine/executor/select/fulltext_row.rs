//! Score providers for row-based locking reads, shared with FulltextScan.
use super::super::*;
use super::plan::{build_fulltext_match_runtime, fulltext_runtime_score, FtMatchRuntime};
use std::cell::RefCell;
thread_local! {static SCORES:RefCell<Vec<(u64,f64)>>=const {RefCell::new(Vec::new())};}
pub(in crate::engine::executor) struct RowScope(Vec<(u64, f64)>);
impl Drop for RowScope {
    fn drop(&mut self) {
        SCORES.with(|s| *s.borrow_mut() = std::mem::take(&mut self.0));
    }
}
pub(crate) fn current_score(expr: &Expr) -> Option<Value> {
    let id = crate::engine::expr_program::expr_fingerprint(expr);
    SCORES.with(|s| {
        s.borrow()
            .iter()
            .find(|(key, _)| *key == id)
            .map(|(_, score)| Value::Double(*score))
    })
}
pub(in crate::engine::executor) struct Providers {
    runtimes: Vec<FtMatchRuntime>,
    documents: Option<HashMap<crate::storage::DmlRowRef, crate::storage::RowId>>,
    _charge: crate::engine::executor::mem_budget::ScopedCharge,
}
impl Providers {
    pub(in crate::engine::executor) fn new(
        db: &Arc<Database>,
        txn: TxnView,
        table: &Arc<TableSchema>,
        stmt: &Select,
    ) -> Result<Self, QueryError> {
        let expressions = crate::planner::collect_fulltext_matches_in_select(stmt);
        let mut result = Vec::new();
        let mut plan_table = crate::planner::PlanTable::new(
            table.id,
            table.clone(),
            stmt.from.first().and_then(|t| t.alias.clone()),
        );
        plan_table.live_row_delta = db.table_live_row_delta(table.id);
        let mut charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
        let mut heap_rows = Vec::new();
        let mut documents = None;
        if !expressions.is_empty() && table.is_partitioned() {
            let mut ids = HashMap::new();
            for (storage_id, row) in db.scan_rows_with_storage(&table.qualified_name(), txn)? {
                charge.add_non_spillable(
                    crate::engine::executor::mem_budget::estimate_row_bytes(&row.values)
                        .saturating_mul(2)
                        .saturating_add(128),
                )?;
                let ordinal = heap_rows.len();
                let page = u32::try_from(ordinal >> 16).map_err(|_| {
                    crate::error::catalog::ER_OUTOFMEMORY.error("Too many FULLTEXT documents")
                })?;
                ids.insert(
                    crate::storage::DmlRowRef {
                        storage_id,
                        row_id: row.row_id,
                    },
                    crate::storage::RowId::new(page, ordinal as u16),
                );
                heap_rows.push(row.values);
            }
            documents = Some(ids);
        }
        for expr in expressions {
            let (mode, query, param, columns) = crate::planner::fulltext_match_parts(&expr)
                .ok_or_else(|| {
                    crate::error::catalog::ER_NOT_SUPPORTED_YET
                        .error("Unsupported MATCH expression")
                })?;
            let index =
                crate::planner::find_matching_fulltext_index(table, &columns).ok_or_else(|| {
                    crate::error::catalog::ER_FT_MATCHING_KEY_NOT_FOUND
                        .error("Can't find FULLTEXT index matching the column list")
                })?;
            result.push(build_fulltext_match_runtime(
                db,
                txn,
                &plan_table,
                index,
                mode,
                &query,
                param,
                &index.columns,
                crate::engine::expr_program::expr_fingerprint(&expr),
                false,
                true,
                documents.as_ref().map(|_| heap_rows.as_slice()),
            )?);
        }
        Ok(Self {
            runtimes: result,
            documents,
            _charge: charge,
        })
    }
    pub(in crate::engine::executor) fn is_empty(&self) -> bool {
        self.runtimes.is_empty()
    }
    pub(in crate::engine::executor) fn enter(
        &self,
        row: crate::storage::DmlRowRef,
    ) -> Option<RowScope> {
        if self.is_empty() {
            return None;
        }
        let id = self
            .documents
            .as_ref()
            .and_then(|ids| ids.get(&row).copied())
            .unwrap_or(row.row_id);
        let scores = self
            .runtimes
            .iter()
            .map(|p| (p.fingerprint, fulltext_runtime_score(p, id)))
            .collect();
        Some(RowScope(SCORES.with(|s| s.replace(scores))))
    }
}
