use super::*;
use crate::engine::api::ResultCursor;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
struct CheckedCursor {
    db: Arc<Database>,
    txn: TxnView,
    produced: usize,
    fail_at: Option<usize>,
    drops: Arc<AtomicUsize>,
}

impl ResultCursor for CheckedCursor {
    fn next_row(&mut self) -> Result<Option<Vec<Value>>, QueryError> {
        // Before the next batch is requested, the previous batch must already
        // be in storage. A whole-result materialization fails this assertion.
        if self.produced % CTAS_LOAD_CHUNK_ROWS == 0 {
            assert_eq!(
                self.db.scan_rows("test.sink", self.txn)?.len(),
                self.produced
            );
        }
        if self.fail_at == Some(self.produced) {
            return Err(error(
                crate::error::catalog::ER_QUERY_INTERRUPTED,
                "injected source failure",
            ));
        }
        if self.produced == CTAS_LOAD_CHUNK_ROWS * 3 {
            return Ok(None);
        }
        let id = self.produced;
        self.produced += 1;
        Ok(Some(vec![Value::Int(id as i64)]))
    }
}

impl Drop for CheckedCursor {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn ctas_sink_consumes_once_and_writes_before_source_exhaustion() {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir.path(),
        StorageConfig {
            durability: DurabilityMode::Memory,
            checkpoint_interval: Duration::from_secs(60),
            checkpoint_wal_bytes: 64 * 1024 * 1024,
            lock_wait: Duration::from_secs(1),
            group_commit_window: Duration::ZERO,
            wal_segment_bytes: 64 * 1024 * 1024,
            buffer_pool_pages: 512,
            buffer_pool_io_threads: 1,
            buffer_pool_prefetch_pages: 0,
            undo_retention: Duration::from_secs(60),
            heap_update_reserve_bytes: 2048,
            startup_validation: crate::storage::StartupValidationMode::Off,
            startup_validate_indexes: false,
        },
    )
    .unwrap();
    engine
        .execute("CREATE TABLE sink(extra INT DEFAULT 7,id INT)")
        .unwrap();
    let db = engine.database();
    for fail_at in [None, Some(CTAS_LOAD_CHUNK_ROWS + 7)] {
        let txn = db.begin_txn().unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let source = CtasSource {
            response: QueryResponse::StreamingResultSet(StreamingResultSet {
                columns: vec![],
                found_rows: None,
                cursor: Box::new(CheckedCursor {
                    db: db.clone(),
                    txn,
                    produced: 0,
                    fail_at,
                    drops: drops.clone(),
                }),
            }),
            inferred_columns: vec![],
            enum_source_columns: vec![false],
        };
        let scatter =
            CtasDeclaredScatter::new(&db.table("test.sink").unwrap(), &[1], false).unwrap();
        let result = ctas_load_rows(
            db,
            txn,
            "test.sink",
            source,
            Some(scatter),
            crate::parser::CtasConflictPolicy::Error,
        );
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        if fail_at.is_some() {
            assert_eq!(
                result.unwrap_err().code(),
                crate::error::catalog::ER_QUERY_INTERRUPTED.code()
            );
        } else {
            result.unwrap();
        }
        let rows = db.scan_rows("test.sink", txn).unwrap();
        assert_eq!(
            rows.len(),
            if fail_at.is_some() {
                CTAS_LOAD_CHUNK_ROWS
            } else {
                CTAS_LOAD_CHUNK_ROWS * 3
            }
        );
        for (id, row) in rows.iter().enumerate() {
            assert_eq!(row.values, vec![Value::Int(7), Value::Int(id as i64)]);
        }
        db.rollback_txn(txn.txn_id).unwrap();
    }
}
