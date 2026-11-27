use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use super::background::BackgroundWorker;
use crate::storage::Database;

fn active_workers() -> &'static parking_lot::Mutex<HashMap<usize, usize>> {
    static WORKERS: OnceLock<parking_lot::Mutex<HashMap<usize, usize>>> = OnceLock::new();
    WORKERS.get_or_init(Default::default)
}

pub(crate) fn is_active(db: &Database) -> bool {
    active_workers()
        .lock()
        .contains_key(&(db as *const Database as usize))
}

// The Arc outlives registration, so a database address cannot be reused while
// registered. Never hold the registry lock while acquiring the graph map.
struct ActiveWorker(Arc<Database>);

impl ActiveWorker {
    fn enter(db: Arc<Database>) -> Self {
        *active_workers()
            .lock()
            .entry(Arc::as_ptr(&db) as usize)
            .or_default() += 1;
        db.sync_vector_background_quant();
        Self(db)
    }
}

impl Drop for ActiveWorker {
    fn drop(&mut self) {
        {
            let mut workers = active_workers().lock();
            let key = Arc::as_ptr(&self.0) as usize;
            if let Some(count) = workers.get_mut(&key) {
                *count -= 1;
                if *count == 0 {
                    workers.remove(&key);
                }
            }
        }
        self.0.sync_vector_background_quant();
    }
}

/// Background vector-index maintenance: retires superseded HNSW versions
/// once no snapshot needs them and reclaims tombstones (slab/IVF compaction,
/// paged in-place detachment). Kept off commit and checkpoint paths so large
/// rebuilds never stall writers or WAL truncation.
pub struct VectorMaintenanceHandle {
    worker: BackgroundWorker,
}

impl VectorMaintenanceHandle {
    pub(crate) fn disabled() -> Self {
        Self {
            worker: BackgroundWorker::disabled(),
        }
    }

    pub fn spawn(db: Arc<Database>) -> Self {
        if db.is_snapshot_read_only() {
            return Self::disabled();
        }
        let worker = BackgroundWorker::spawn("mydb-vector-maintenance", move |worker_shutdown| {
            let _active = ActiveWorker::enter(db.clone());
            let _reclaim = super::executor::mem_budget::ReclaimDatabaseScope::enter(&db);
            while !worker_shutdown.is_requested() {
                if worker_shutdown.wait_timeout(Duration::from_secs(1)) {
                    break;
                }
                let Some(_write) = crate::repl::background_write_permit() else {
                    continue;
                };
                let round = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    db.maintain_vector_indexes(|| worker_shutdown.is_requested())
                }));
                match round {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => crate::util::logging::log_message(format!(
                        "vector index maintenance failed: {err:?}"
                    )),
                    Err(_) => {
                        db.fail_vector_maintenance_worker();
                        break;
                    }
                }
            }
        });
        Self { worker }
    }

    pub fn shutdown(&self) {
        self.worker.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn online_graph_publication_inherits_actual_worker_policy() {
        crate::util::env::with_vars(&[("MYDB_VECTOR_STORAGE", Some("memory"))], || {
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let engine = crate::engine::StorageQueryEngine::open_with_storage_config_without_background_workers(
                dir.path(), crate::storage::StorageConfig::from_env(),
            ).unwrap();
            let db = engine.db_handle().clone();
            let worker = ActiveWorker::enter(db.clone());
            engine
                .execute("CREATE TABLE t(id INT PRIMARY KEY,v VECTOR(2) NOT NULL)")
                .unwrap();
            engine
                .execute("INSERT INTO t VALUES(1,'[1,1]'),(2,'[2,2]')")
                .unwrap();
            engine
                .execute("CREATE INDEX ix ON t(v) USING HNSW LOCK=NONE")
                .unwrap();
            let table = db.table("test.t").unwrap();
            let index = table
                .indexes
                .iter()
                .find(|index| index.name == "ix")
                .unwrap();
            let graph = db.vector_graph_get_or_build(&table, index).unwrap();
            let crate::engine::vector::VectorGraph::Memory(memory) = graph.as_ref() else {
                panic!("resident graph required");
            };
            assert!(
                memory.background_quant_for_test(),
                "new graphs must inherit an already registered worker"
            );
            drop(worker);
            assert!(
                !memory.background_quant_for_test(),
                "worker removal must revoke background-only refresh"
            );
        });
    }

    #[test]
    fn quant_refresh_capability_tracks_actual_worker_lifetime() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(Database::open(dir.path()).unwrap());
        assert!(!is_active(&db));
        let first = ActiveWorker::enter(db.clone());
        let second = ActiveWorker::enter(db.clone());
        assert!(is_active(&db));
        drop(first);
        assert!(is_active(&db));
        drop(second);
        assert!(!is_active(&db));
    }
}
