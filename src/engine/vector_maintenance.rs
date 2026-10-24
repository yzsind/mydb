use std::sync::Arc;
use std::time::Duration;

use super::background::BackgroundWorker;
use crate::storage::Database;

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
            let _reclaim = super::executor::mem_budget::ReclaimDatabaseScope::enter(&db);
            while !worker_shutdown.is_requested() {
                if worker_shutdown.wait_timeout(Duration::from_secs(1)) {
                    break;
                }
                let round = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    db.maintain_vector_indexes(|| worker_shutdown.is_requested())
                }));
                if round.is_err() {
                    crate::util::logging::log_message(
                        "vector index maintenance panicked; retry next pass",
                    );
                }
            }
        });
        Self { worker }
    }

    pub fn shutdown(&self) {
        self.worker.shutdown();
    }
}
