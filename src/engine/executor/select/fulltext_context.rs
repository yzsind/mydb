//! One query-scoped BM25 statistics baseline, inherited by operator workers.
use crate::{planner::PlanTable, storage::IndexSchema, types::QueryError};
use std::{cell::RefCell, collections::HashMap, sync::Arc};

#[derive(Clone, Copy)]
pub(super) struct Statistics {
    pub base_n: f64,
    pub corpus_avg: Option<f64>,
}
struct State {
    statistics: HashMap<(u64, String), Statistics>,
    charge: crate::engine::executor::mem_budget::ScopedCharge,
}
#[derive(Clone)]
pub(in crate::engine::executor) struct Context(Arc<parking_lot::Mutex<State>>);
impl Context {
    pub(super) fn new() -> Self {
        Self(Arc::new(parking_lot::Mutex::new(State {
            statistics: HashMap::new(),
            charge: crate::engine::executor::mem_budget::ScopedCharge::new(0),
        })))
    }
}
thread_local! { static CURRENT:RefCell<Option<Context>>=const {RefCell::new(None)}; }
pub(in crate::engine::executor) fn capture() -> Option<Context> {
    CURRENT.with(|slot| slot.borrow().clone())
}
pub(in crate::engine::executor) struct Scope(Option<Context>);
impl Drop for Scope {
    fn drop(&mut self) {
        CURRENT.with(|slot| *slot.borrow_mut() = self.0.take());
    }
}
pub(in crate::engine::executor) fn enter(context: Option<Context>) -> Scope {
    Scope(CURRENT.with(|slot| slot.replace(context)))
}
pub(super) fn ensure() -> Scope {
    enter(Some(capture().unwrap_or_else(Context::new)))
}

pub(in crate::engine::executor) fn enter_fusion(has_fusion: bool) -> Option<Scope> {
    (has_fusion && capture().is_none()).then(ensure)
}

pub(super) fn statistics(table: &PlanTable, index: &IndexSchema) -> Result<Statistics, QueryError> {
    let compute = || Statistics {
        base_n: (table.schema.stats.row_count as i128 + table.live_row_delta as i128).max(0) as f64,
        corpus_avg: table
            .schema
            .stats
            .indexes
            .get(&crate::storage::normalize_name(&index.name))
            .and_then(|stats| stats.fulltext.as_ref())
            .map(|stats| stats.avg_token_count)
            .filter(|avg| *avg > 0.0),
    };
    let Some(context) = capture() else {
        return Ok(compute());
    };
    let key = (table.table_id, crate::storage::normalize_name(&index.name));
    let mut state = context.0.lock();
    if let Some(stats) = state.statistics.get(&key) {
        return Ok(*stats);
    }
    state.charge.add_non_spillable(256 + key.1.capacity())?;
    let value = compute();
    state.statistics.insert(key, value);
    Ok(value)
}
