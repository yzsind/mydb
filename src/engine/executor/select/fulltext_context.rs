//! One query-scoped BM25 statistics baseline, inherited by operator workers.
use crate::{
    planner::PlanTable,
    storage::{Database, IndexSchema, TxnView},
    types::QueryError,
};
use std::{cell::RefCell, collections::HashMap, sync::Arc};

struct State {
    corpora: HashMap<String, Arc<CorpusStatistics>>,
    charge: crate::engine::executor::mem_budget::ScopedCharge,
}
#[derive(Clone)]
pub(in crate::engine::executor) struct Context(Arc<parking_lot::Mutex<State>>);
impl Context {
    pub(super) fn new() -> Self {
        Self(Arc::new(parking_lot::Mutex::new(State {
            corpora: HashMap::new(),
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

/// Exact corpus metadata contains only query terms, so its space is bounded
/// by the query rather than by the vocabulary or number of heap rows.
pub(super) struct CorpusStatistics {
    pub(super) documents: u64,
    pub(super) total_tokens: u64,
    pub(super) frequencies: HashMap<String, u64>,
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}
struct CorpusCacheEntry {
    db: std::sync::Weak<Database>,
    key: String,
    statistics: Arc<CorpusStatistics>,
    bytes: usize,
}
fn corpus_cache() -> &'static parking_lot::Mutex<std::collections::VecDeque<CorpusCacheEntry>> {
    static CACHE: std::sync::OnceLock<
        parking_lot::Mutex<std::collections::VecDeque<CorpusCacheEntry>>,
    > = std::sync::OnceLock::new();
    CACHE.get_or_init(Default::default)
}

/// Share one snapshot baseline across complete/partial indexes and workers.
/// Persistence and invalidation belong to MVCC, so cold queries use the same
/// bounded counter probes as warm queries.
pub(super) fn corpus_statistics(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    terms: &[String],
) -> Result<Arc<CorpusStatistics>, QueryError> {
    let mut terms = terms.to_vec();
    terms.sort();
    terms.dedup();
    let undo_head = db.fulltext_statistics_undo_head(txn.txn_id)?;
    let shared = undo_head.is_none()
        && txn.read_only
        && !txn.dirty_read
        && !db.physical_replica_mode()
        && !crate::storage::is_temp_table_id(table.table_id);
    let key = format!(
        "{}:{}:{}:{}:{}:{:?}:{:?}:{terms:?}",
        table.table_id,
        table.schema.schema_version,
        table.schema.active_generation,
        txn.snapshot_scn,
        txn.txn_id,
        index.fulltext,
        table.explicit_partition_storage_ids
    );
    let key = format!(
        "{key}:{}:{}:{undo_head:?}",
        db.table_write_epoch(table.table_id),
        db.replica_applied_lsn()
    );
    let context = capture();
    if let Some(context) = &context {
        if let Some(hit) = context.0.lock().corpora.get(&key).cloned() {
            return Ok(hit);
        }
    }
    if shared {
        if let Some(hit) = corpus_cache()
            .lock()
            .iter()
            .find(|entry| entry.key == key && entry.db.as_ptr() == Arc::as_ptr(db))
            .map(|entry| Arc::clone(&entry.statistics))
        {
            return Ok(hit);
        }
    }
    use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
    let bytes = 512
        + key.len().saturating_mul(2)
        + terms.iter().map(|term| 192 + term.len() * 2).sum::<usize>();
    let charge = ControlledMemoryCharge::try_new(ControlledMemoryCategory::DerivedCache, bytes)?;
    let persisted = db.fulltext_corpus_statistics(
        txn,
        &table.schema,
        index,
        &terms,
        table.explicit_partition_storage_ids.as_deref(),
    )?;
    if persisted.documents == 0 {
        return Err(crate::error::catalog::ER_GET_ERRNO
            .error("FULLTEXT statistics: missing scoring corpus"));
    }
    let stats = Arc::new(CorpusStatistics {
        documents: persisted.documents,
        total_tokens: persisted.total_tokens,
        frequencies: persisted.frequencies,
        _charge: charge,
    });
    if let Some(context) = &context {
        let mut state = context.0.lock();
        state.charge.add_non_spillable(96 + key.len())?;
        state.corpora.insert(key.clone(), Arc::clone(&stats));
    }
    if shared && bytes <= 64 * 1024 {
        let mut cache = corpus_cache().lock();
        cache.retain(|entry| entry.db.strong_count() != 0);
        while cache.len() >= 32
            || cache.iter().map(|entry| entry.bytes).sum::<usize>() + bytes > 2 * 1024 * 1024
        {
            cache.pop_front();
        }
        cache.push_back(CorpusCacheEntry {
            db: Arc::downgrade(db),
            key,
            statistics: Arc::clone(&stats),
            bytes,
        });
    }
    Ok(stats)
}

/// Compare weak identities without upgrading while holding the cache latch.
pub(crate) fn clear_corpus_cache(database: &Database) {
    corpus_cache()
        .lock()
        .retain(|entry| entry.db.as_ptr() != database as *const Database);
}
