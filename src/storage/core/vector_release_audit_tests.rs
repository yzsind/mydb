//! Failure fencing and generation lifetime regression coverage.
use super::*;
use crate::engine::StorageQueryEngine;

fn engine(path: &Path) -> StorageQueryEngine {
    StorageQueryEngine::open_with_storage_config_without_background_workers(
        path,
        StorageConfig {
            durability: DurabilityMode::Strict,
            ..StorageConfig::from_env()
        },
    )
    .unwrap()
}

#[test]
fn vector_release_failstop_retirement_does_not_persist_false_corruption() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    {
        let engine = engine(dir.path());
        engine
            .execute(
                "CREATE TABLE t(id INT PRIMARY KEY,v VECTOR(2) NOT NULL, INDEX ix(v) USING HNSW)",
            )
            .unwrap();
        engine.execute("INSERT INTO t VALUES(1,'[1,2]')").unwrap();
        let db = engine.database();
        let snapshot = db.register_statement_snapshot(db.visible_scn());
        engine.execute("UPDATE t SET v='[3,4]' WHERE id=1").unwrap();
        assert!(db.vector_purges_pending.load(Ordering::Acquire) > 0);
        db.quarantine_mvcc("injected transient storage failure");
        drop(snapshot);
        assert_eq!(db.drain_vector_purges(), 0);
        let table = db.table("test.t").unwrap();
        let index = table.indexes.iter().find(|i| i.is_vector()).unwrap();
        db.vector_maintenance_error(
            &table,
            index,
            table.id,
            "retirement",
            db.ensure_operational().unwrap_err(),
        );
        assert!(!dir.path().join("system/index-maintenance.failed").exists());
    }
    engine(dir.path()).execute("SELECT * FROM t").unwrap();
}

#[test]
fn vector_release_dropped_maintenance_handle_cannot_fence_replacement() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = engine(dir.path());
    engine
        .database()
        .set_recyclebin_variable("mydb_recyclebin", 0)
        .unwrap();
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY,v VECTOR(2) NOT NULL, INDEX ix(v) USING HNSW WITH(quantization=sq8))").unwrap();
    let db = engine.database();
    let table = db.table("test.t").unwrap();
    let index = table.indexes.iter().find(|i| i.is_vector()).unwrap();
    let graph = db
        .vector_graph_get_or_build_for_partition(&table, index, table.id)
        .unwrap();
    engine.execute("DROP TABLE t").unwrap();
    assert!(graph.ensure_valid().is_err());
    assert!(db.vector_retired_graphs.lock().is_empty());
    // A cold stale caller must not resurrect a graph while its old files are
    // retained for asynchronous retirement.
    db.evict_table_vector_graphs(table.id);
    assert!(db
        .vector_graph_get_or_build_for_partition(&table, index, table.id)
        .is_err());
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY,v VECTOR(2) NOT NULL, INDEX ix(v) USING HNSW WITH(quantization=sq8))").unwrap();
    db.vector_maintenance_error(
        &table,
        index,
        table.id,
        "stale worker",
        vector::persistence::corrupt(),
    );
    db.record_vector_maintenance_job_error(&table, index, table.id, vector::persistence::corrupt());
    db.ensure_operational().unwrap();
    assert!(!dir.path().join("system/index-maintenance.failed").exists());
    engine.execute("INSERT INTO t VALUES(1,'[1,2]')").unwrap();
}

#[test]
fn vector_release_optimize_retires_previous_generation_without_restart() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v VECTOR(2) NOT NULL,INDEX ix(v) USING HNSW)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,'[1,2]'),(2,'[3,4]')")
        .unwrap();
    let db = engine.database();
    let table = db.table("test.t").unwrap();
    let index = table.indexes.iter().find(|i| i.is_vector()).unwrap();
    let old = db
        .active_native_vector_path(&table, index, table.id)
        .unwrap()
        .0;
    assert!(old.exists());
    engine.execute("OPTIMIZE TABLE t").unwrap();
    // The catalog changes generation; maintenance must retain the old path's
    // original generation until its last reader releases the handle.
    engine
        .execute("SELECT /*+ HNSW_EF(64) */ id FROM t ORDER BY L2_DISTANCE(v,'[1,2]') LIMIT 1")
        .unwrap();
    db.maintain_vector_indexes(|| false).unwrap();
    assert!(
        !old.exists(),
        "retired file still exists: {}",
        old.display()
    );
}

#[test]
fn vector_release_other_tables_do_not_force_snapshot_or_xa_fallback() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = engine(dir.path());
    engine
        .execute("CREATE TABLE a(id INT PRIMARY KEY,v VECTOR(2) NOT NULL,INDEX ix(v) USING HNSW)")
        .unwrap();
    engine
        .execute("CREATE TABLE b(id INT PRIMARY KEY,v VECTOR(2) NOT NULL,INDEX ix(v) USING HNSW)")
        .unwrap();
    engine.execute("INSERT INTO a VALUES(1,'[1,2]')").unwrap();
    let db = engine.database();
    let a = db.table("test.a").unwrap();
    let b = db.table("test.b").unwrap();
    let txn = db.begin_read_only_txn().unwrap();
    let snapshot = db.register_statement_snapshot(txn.snapshot_scn);
    engine.execute("INSERT INTO b VALUES(1,'[1,2]')").unwrap();
    db.vector_table_snapshot_floor(b.id)
        .fetch_max(db.visible_scn(), Ordering::AcqRel);
    assert!(!db.vector_snapshot_requires_exact(&a, txn));
    assert!(db.vector_snapshot_requires_exact(&b, txn));
    let mut session = crate::engine::SessionContext::default();
    for sql in [
        "XA START 'other_table'",
        "UPDATE b SET v='[3,4]' WHERE id=1",
        "XA END 'other_table'",
        "XA PREPARE 'other_table'",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    assert!(!db.vector_table_has_prepared_writes(&a));
    assert!(db.vector_table_has_prepared_writes(&b));
    engine.execute("XA ROLLBACK 'other_table'").unwrap();
    drop(snapshot);
    db.rollback_txn(txn.txn_id).unwrap();
}

#[test]
fn vector_release_transient_maintenance_errors_recover_without_permanent_fence() {
    for spec in [
        crate::error::catalog::ER_QUERY_INTERRUPTED,
        crate::error::catalog::ER_QUERY_TIMEOUT,
        crate::error::catalog::ER_OUTOFMEMORY,
        crate::error::catalog::ER_DISK_FULL_NOWAIT,
    ] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        {
            let engine = engine(dir.path());
            engine.execute("CREATE TABLE t(id INT PRIMARY KEY,v VECTOR(2) NOT NULL,INDEX ix(v) USING HNSW)").unwrap();
            let db = engine.database();
            let table = db.table("test.t").unwrap();
            let index = table
                .indexes
                .iter()
                .find(|index| index.is_vector())
                .unwrap();
            db.vector_maintenance_error(
                &table,
                index,
                table.id,
                "injected transient",
                spec.error("transient maintenance failure"),
            );
            assert!(db.ensure_operational().is_err());
            assert!(!dir.path().join("system/index-maintenance.failed").exists());
        }
        engine(dir.path())
            .execute("INSERT INTO t VALUES(1,'[1,2]')")
            .unwrap();
    }
}

impl Database {
    pub(crate) fn invalidate_paged_vector_for_test(&self, table_name: &str) {
        let table = self.table(table_name).unwrap();
        let index = table
            .indexes
            .iter()
            .find(|index| index.is_vector())
            .unwrap();
        let graph = self
            .vector_graph_get_or_build_for_partition(&table, index, table.id)
            .unwrap();
        let VectorGraph::Paged(graph) = graph.as_ref() else {
            panic!("expected paged HNSW");
        };
        graph.file().invalidate_build(0).unwrap();
        graph.file().flush().unwrap();
        self.vector_indexes.write().clear();
        let Err(err) = self.vector_graph_get_or_build_for_partition(&table, index, table.id) else {
            panic!("expected unfinished vector maintenance");
        };
        assert_eq!(
            err.diagnostic_id(),
            Some("MYDB-DIAG-VECTOR-MAINTENANCE-PENDING")
        );
    }
}

#[test]
fn vector_release_startup_removes_orphan_manifest_and_directory() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let (manifest, orphan, live_manifest, live_image) = {
        let engine = engine(dir.path());
        engine
            .execute(
                "CREATE TABLE t(id INT PRIMARY KEY,v VECTOR(2) NOT NULL,INDEX ix(v) USING HNSW)",
            )
            .unwrap();
        engine.execute("INSERT INTO t VALUES(1,'[1,2]')").unwrap();
        engine.checkpoint().unwrap();
        let db = engine.database();
        let table = db.table("test.t").unwrap();
        let index = table
            .indexes
            .iter()
            .find(|index| index.is_vector())
            .unwrap();
        let live_manifest = db.vector_manifest_path(&table, index, table.id);
        let live_image = db
            .active_native_vector_path(&table, index, table.id)
            .unwrap()
            .0;
        let manifest = live_manifest.with_file_name("999999.vmanifest");
        let orphan = manifest.with_extension("vector");
        fs::write(&manifest, b"unpublished manifest").unwrap();
        fs::create_dir(&orphan).unwrap();
        (manifest, orphan, live_manifest, live_image)
    };
    let reopened = engine(dir.path());
    assert!(!manifest.exists());
    assert!(!orphan.exists());
    assert!(live_manifest.exists() && live_image.exists());
    reopened
        .execute("SELECT /*+ HNSW_EF(64) */ id FROM t ORDER BY L2_DISTANCE(v,'[1,2]') LIMIT 1")
        .unwrap();
}

#[test]
fn vector_release_drop_ignores_foreign_directory_entry() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v VECTOR(2) NOT NULL,INDEX ix(v) USING HNSW)")
        .unwrap();
    let db = engine.database();
    let table = db.table("test.t").unwrap();
    let index = table
        .indexes
        .iter()
        .find(|index| index.is_vector())
        .unwrap();
    let manifest = db.vector_manifest_path(&table, index, table.id);
    let foreign = manifest.with_extension("vector").join("foreign");
    fs::create_dir(&foreign).unwrap();
    let preserved = foreign.join("keep");
    fs::write(&preserved, b"foreign").unwrap();

    engine.execute("DROP INDEX ix ON t").unwrap();
    assert!(db
        .table("test.t")
        .unwrap()
        .indexes
        .iter()
        .all(|index| !index.is_vector()));
    engine.execute("INSERT INTO t VALUES(1,'[1,2]')").unwrap();
    assert_eq!(fs::read(preserved).unwrap(), b"foreign");
}

#[test]
fn vector_release_purge_drain_preserves_oom_and_requeues_work() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v VECTOR(2) NOT NULL,INDEX ix(v) USING HNSW)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,'[1,2]')").unwrap();
    let db = engine.database();
    let snapshot = db.register_statement_snapshot(db.visible_scn());
    engine.execute("UPDATE t SET v='[3,4]' WHERE id=1").unwrap();
    let table = db.table("test.t").unwrap();
    let index = table
        .indexes
        .iter()
        .find(|index| index.is_vector())
        .unwrap();
    db.vector_indexes.write().clear();
    db.vector_build_backoff.lock().insert(
        (
            table.id,
            table.id,
            table.active_generation.max(1),
            normalize_name(&index.name),
        ),
        Instant::now(),
    );
    // Suppress the snapshot guard's best-effort drain so recovery's caller
    // can observe the error directly.
    let drain = db.vector_purge_drain_gate.lock();
    drop(snapshot);
    drop(drain);
    let err = db.try_drain_vector_purges().unwrap_err();
    assert!(err.is(crate::error::catalog::ER_OUTOFMEMORY), "{err:?}");
    assert!(db.vector_purges_pending.load(Ordering::Acquire) > 0);
}
