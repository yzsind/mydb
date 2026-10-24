use super::*;
use crate::engine::StorageQueryEngine;

fn open_engine(dir: &Path) -> StorageQueryEngine {
    StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir,
        StorageConfig {
            durability: DurabilityMode::Strict,
            buffer_pool_pages: 256,
            buffer_pool_io_threads: 1,
            buffer_pool_prefetch_pages: 0,
            startup_validation: StartupValidationMode::Off,
            startup_validate_indexes: false,
            checkpoint_interval: Duration::from_secs(60),
            checkpoint_wal_bytes: 64 * 1024 * 1024,
            lock_wait: Duration::from_secs(5),
            group_commit_window: Duration::ZERO,
            wal_segment_bytes: 64 * 1024 * 1024,
            undo_retention: Duration::from_secs(60),
            heap_update_reserve_bytes: 2048,
        },
    )
    .unwrap()
}

#[test]
fn auto_increment_partition_max_uses_leaf_indexes_without_heap_scan() {
    for partitioning in [
        "PARTITION BY RANGE(k) (PARTITION p0 VALUES LESS THAN(1), PARTITION p1 VALUES LESS THAN(2), PARTITION p2 VALUES LESS THAN(3), PARTITION p3 VALUES LESS THAN MAXVALUE)",
        "PARTITION BY LIST(k) (PARTITION p0 VALUES IN(0), PARTITION p1 VALUES IN(1), PARTITION p2 VALUES IN(2), PARTITION p3 VALUES IN(3))",
        "PARTITION BY HASH(k) PARTITIONS 4",
        "PARTITION BY KEY(k) PARTITIONS 4",
        "PARTITION BY RANGE(k) SUBPARTITION BY HASH(id) SUBPARTITIONS 2 (PARTITION p0 VALUES LESS THAN(1), PARTITION p1 VALUES LESS THAN MAXVALUE)",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_engine(dir.path());
        engine.execute(&format!(
            "CREATE TABLE t (id BIGINT UNSIGNED AUTO_INCREMENT, k INT NOT NULL, payload TEXT, PRIMARY KEY(id,k)) {partitioning}"
        )).unwrap();
        engine.execute("INSERT INTO t VALUES (200,0,'a'),(7,1,'b'),(100,2,'c')").unwrap();
        let db = engine.database();
        let table = db.table("t").unwrap();
        db.auto_increment.forget_table(table.id);
        crate::engine::perf_events::clear_thread_wait_samples();
        assert_eq!(db.current_auto_increment_max(&table).unwrap(), 200);
        assert_eq!(
            crate::engine::perf_events::take_rows_examined(), 0,
            "cold maximum lookup must use physical indexes: {partitioning}"
        );
        assert!(
            !db.index_path(table.id, "PRIMARY").exists(),
            "maximum lookup must not create an index for the logical parent"
        );
    }
}

#[test]
fn auto_increment_partition_restart_preserves_deleted_high_water() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_engine(dir.path());
        engine.execute("CREATE TABLE t (id BIGINT UNSIGNED AUTO_INCREMENT, k INT NOT NULL, payload TEXT, PRIMARY KEY(id,k)) PARTITION BY RANGE(k) (PARTITION p0 VALUES LESS THAN(1), PARTITION p1 VALUES LESS THAN MAXVALUE)").unwrap();
        engine
            .execute("INSERT INTO t VALUES (200,0,'a'),(7,1,'b'),(800,1,'deleted')")
            .unwrap();
        engine
            .execute("DELETE FROM t WHERE id=800 AND k=1")
            .unwrap();
        engine.shutdown().unwrap();
    }
    let engine = open_engine(dir.path());
    let db = engine.database();
    let table = db.table("t").unwrap();
    assert_eq!(db.auto_increment.cached_max(table.id), None);
    crate::engine::perf_events::clear_thread_wait_samples();
    assert_eq!(db.auto_increment_initial_max(&table).unwrap(), 800);
    assert_eq!(crate::engine::perf_events::take_rows_examined(), 0);
    engine
        .execute("UPDATE t SET id=200, payload='changed' WHERE id=200 AND k=0 LIMIT 1")
        .unwrap();
    engine
        .execute("INSERT INTO t(k,payload) VALUES (1,'next')")
        .unwrap();
    let crate::engine::QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM t WHERE payload='next'")
        .unwrap()
    else {
        panic!("expected rows");
    };
    assert_eq!(result.rows, vec![vec![Value::UInt(801)]]);
}

#[test]
fn auto_increment_cold_initialization_coalesces_concurrent_fallback_scans() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_engine(dir.path());
    engine.execute("CREATE TABLE t (id BIGINT UNSIGNED AUTO_INCREMENT, k INT NOT NULL, payload TEXT, PRIMARY KEY(id,k)) PARTITION BY HASH(k) PARTITIONS 4").unwrap();
    let values = (1..=2048)
        .map(|id| format!("({id},{},'{}')", id % 4, "x".repeat(128)))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO t VALUES {values}"))
        .unwrap();
    let db = engine.database();
    let mut table = (*db.table("t").unwrap()).clone();
    // A non-public index cannot establish the maximum; exercise the generic
    // projected fallback and verify that concurrent cold callers share it.
    for index in &mut table.indexes {
        index.state = IndexState::Building;
    }
    db.auto_increment.forget_table(table.id);
    let barrier = std::sync::Barrier::new(8);
    let examined: u64 = std::thread::scope(|scope| {
        (0..8)
            .map(|_| {
                let table = &table;
                let barrier = &barrier;
                scope.spawn(move || {
                    crate::engine::perf_events::clear_thread_wait_samples();
                    barrier.wait();
                    assert_eq!(db.auto_increment_initial_max(table).unwrap(), 2048);
                    crate::engine::perf_events::take_rows_examined()
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .sum()
    });
    assert_eq!(examined, 2048, "only one cold initializer may scan");
}

#[test]
fn auto_increment_partition_unsigned_boundary_survives_cold_initialization() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_engine(dir.path());
    engine.execute("CREATE TABLE t (id BIGINT UNSIGNED AUTO_INCREMENT, k INT NOT NULL, PRIMARY KEY(id,k)) PARTITION BY HASH(k) PARTITIONS 4").unwrap();
    engine
        .execute("INSERT INTO t VALUES (9223372036854776832,0),(10,1)")
        .unwrap();
    let db = engine.database();
    let table = db.table("t").unwrap();
    db.auto_increment.forget_table(table.id);
    assert_eq!(
        db.auto_increment_initial_max(&table).unwrap(),
        9223372036854776832
    );
    engine.execute("INSERT INTO t(k) VALUES (2)").unwrap();
    let crate::engine::QueryResponse::ResultSet(result) =
        engine.execute("SELECT id FROM t WHERE k=2").unwrap()
    else {
        panic!("expected rows");
    };
    assert_eq!(result.rows, vec![vec![Value::UInt(9223372036854776833)]]);
    engine
        .execute("INSERT INTO t VALUES (18446744073709551615,3)")
        .unwrap();
    db.auto_increment.forget_table(table.id);
    assert_eq!(db.auto_increment_initial_max(&table).unwrap(), u64::MAX);
    assert!(engine.execute("INSERT INTO t(k) VALUES (3)").is_err());
}

#[test]
fn auto_increment_failed_cold_initialization_can_retry() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (100)").unwrap();
    let db = engine.database();
    let table = db.table("t").unwrap();
    db.auto_increment.forget_table(table.id);
    let mut unavailable = (*table).clone();
    unavailable.indexes.clear();
    unavailable.name = "unavailable".into();
    assert!(db.auto_increment_initial_max(&unavailable).is_err());
    assert_eq!(db.auto_increment.cached_max(table.id), None);
    assert_eq!(db.auto_increment_initial_max(&table).unwrap(), 100);
}

#[test]
fn auto_increment_empty_and_nonpositive_partition_indexes_need_no_scan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_engine(dir.path());
    engine.execute("CREATE TABLE t (id BIGINT AUTO_INCREMENT, k INT NOT NULL, PRIMARY KEY(id,k)) PARTITION BY RANGE(k) (PARTITION p0 VALUES LESS THAN(1), PARTITION p1 VALUES LESS THAN MAXVALUE)").unwrap();
    let db = engine.database();
    let table = db.table("t").unwrap();
    assert_eq!(db.current_auto_increment_max(&table).unwrap(), 0);
    engine
        .execute("INSERT INTO t VALUES (-9,0),(-1,0)")
        .unwrap();
    crate::engine::perf_events::clear_thread_wait_samples();
    assert_eq!(db.current_auto_increment_max(&table).unwrap(), 0);
    assert_eq!(crate::engine::perf_events::take_rows_examined(), 0);
    assert!(!db.index_path(table.id, "PRIMARY").exists());
}

#[test]
fn auto_increment_invisible_supporting_index_avoids_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_engine(dir.path());
    engine.execute("CREATE TABLE t (id BIGINT AUTO_INCREMENT, k INT NOT NULL, PRIMARY KEY(k,id), KEY ai(id) INVISIBLE) PARTITION BY HASH(k) PARTITIONS 4").unwrap();
    engine
        .execute("INSERT INTO t VALUES (70,0),(10,1)")
        .unwrap();
    let db = engine.database();
    let table = db.table("t").unwrap();
    crate::engine::perf_events::clear_thread_wait_samples();
    assert_eq!(db.current_auto_increment_max(&table).unwrap(), 70);
    assert_eq!(crate::engine::perf_events::take_rows_examined(), 0);
    assert!(!db.index_path(table.id, "ai").exists());
}
