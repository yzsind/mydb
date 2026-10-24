use std::sync::Arc;
use std::time::Duration;

use super::background::BackgroundWorker;
use crate::storage::Database;

/// Low-priority online builder for missing/stale zone summaries. Failures are
/// intentionally isolated from user transactions: the affected table remains
/// Unknown and the next ten-second pass retries it.
pub struct ZoneMapMaintenanceHandle {
    worker: BackgroundWorker,
}

impl ZoneMapMaintenanceHandle {
    pub(crate) fn disabled() -> Self {
        Self {
            worker: BackgroundWorker::disabled(),
        }
    }

    pub fn spawn(db: Arc<Database>) -> Self {
        if db.is_snapshot_read_only() {
            return Self::disabled();
        }
        let worker = BackgroundWorker::spawn("mydb-zone-map", move |worker_shutdown| {
            let _reclaim = super::executor::mem_budget::ReclaimDatabaseScope::enter(&db);
            let _cleanup = PartitionCleanupBackgroundGuard::enter(&db);
            while !worker_shutdown.is_requested() {
                if worker_shutdown.wait_timeout(Duration::from_secs(10)) {
                    break;
                }
                run_maintenance_task("partition cleanup", || {
                    db.maintain_partition_cleanup_until(|| worker_shutdown.is_requested())
                });
                run_maintenance_task("catalog retirement cleanup", || {
                    db.maintain_catalog_retirements_until(|| worker_shutdown.is_requested())
                });
                run_maintenance_task("zone-map maintenance", || {
                    db.maintain_zone_maps_round_until(|| worker_shutdown.is_requested())
                });
            }
        });
        Self { worker }
    }

    pub fn shutdown(&self) {
        self.worker.shutdown();
    }
}

struct PartitionCleanupBackgroundGuard<'a>(&'a Database);

impl<'a> PartitionCleanupBackgroundGuard<'a> {
    fn enter(db: &'a Database) -> Self {
        db.set_partition_cleanup_background(true);
        Self(db)
    }
}

impl Drop for PartitionCleanupBackgroundGuard<'_> {
    fn drop(&mut self) {
        self.0.set_partition_cleanup_background(false);
    }
}

// Isolate the two tasks independently: one failing task must not starve the
// other. The outer loop retains its ten-second wait even after a panic.
fn run_maintenance_task<T>(name: &str, task: impl FnOnce() -> Result<T, crate::types::QueryError>) {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(task)) {
        Ok(Ok(_)) => {}
        Ok(Err(err)) => crate::util::logging::log_message(format_args!("{name} retry: {err}")),
        Err(payload) => {
            let message = payload
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| payload.downcast_ref::<&str>().copied())
                .unwrap_or("non-string panic");
            crate::util::logging::log_message(format_args!(
                "{name} panicked; retry next pass: {message}"
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zone_map_audit_maintenance_survives_panic_and_resets_guard() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let db = Database::open(dir.path()).unwrap();
        let calls = std::cell::Cell::new(0);
        {
            let _guard = PartitionCleanupBackgroundGuard::enter(&db);
            assert!(db.partition_cleanup_background_for_tests());
            for pass in 0..2 {
                run_maintenance_task("injected maintenance", || {
                    calls.set(calls.get() + 1);
                    if pass == 0 {
                        panic!("injected maintenance failure");
                    }
                    Ok(0)
                });
            }
            assert_eq!(calls.get(), 2);
        }
        assert!(!db.partition_cleanup_background_for_tests());
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = PartitionCleanupBackgroundGuard::enter(&db);
            panic!("injected worker exit");
        }));
        assert!(!db.partition_cleanup_background_for_tests());
    }
}
