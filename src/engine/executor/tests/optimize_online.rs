//! Physical reclamation and concurrency contracts for OPTIMIZE TABLE.
use super::*;

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap().materialize().unwrap()
    else {
        panic!("expected rows: {sql}");
    };
    result.rows
}

fn optimized(result: QueryResponse) {
    let QueryResponse::ResultSet(result) = result.materialize().unwrap() else {
        panic!("expected maintenance result");
    };
    assert_eq!(result.rows.len(), 1, "{:?}", result.rows);
    assert_eq!(result.rows[0][2], Value::String("status".into()));
    assert_eq!(result.rows[0][3], Value::String("OK".into()));
}

fn bytes(dir: &std::path::Path, extension: &str) -> u64 {
    let mut total = 0;
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            total += bytes(&path, extension);
        } else if path.extension().is_some_and(|ext| ext == extension) {
            total += path.metadata().unwrap().len();
        }
    }
    total
}

fn check_indexes(engine: &StorageQueryEngine) {
    let db = engine.database();
    let table = db.table("test.t").unwrap();
    let txn = db.begin_read_only_txn().unwrap();
    for sid in table.storage_ids() {
        let source = db
            .scan_rows_storage_range(&table, sid, txn, 0, u64::MAX)
            .unwrap();
        for index in &table.indexes {
            let mut expected = source
                .iter()
                .flat_map(|row| {
                    table
                        .index_keys(index, &row.values)
                        .unwrap()
                        .into_iter()
                        .map(move |key| (key, row.row_id))
                })
                .collect::<Vec<_>>();
            let mut leaf = table.as_ref().clone();
            leaf.id = sid;
            leaf.partitioning = None;
            // Query cursors collapse adjacent repeated RowIds for non-unique
            // scans. Inspect every physical posting, including multiple keys
            // from one FULLTEXT/MVI row, by disabling that cursor behavior.
            let mut physical = index.clone();
            physical.unique = true;
            let mut actual = db
                .range_index_bounds_entries(&leaf, &physical, None, None)
                .unwrap();
            expected.sort();
            actual.sort();
            assert_eq!(actual, expected, "storage {sid}, index {}", index.name);
        }
    }
    db.rollback_txn(txn.txn_id).unwrap();
}

#[test]
fn optimize_online_reclaims_heap_indexes_lob_and_reopens() {
    for partition in ["", " PARTITION BY HASH(id) PARTITIONS 3"] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_strict_test_engine(dir.path());
        engine.execute(&format!("CREATE TABLE t(id INT PRIMARY KEY, v VARCHAR(200), body LONGTEXT, INDEX vi(v)) {partition}")).unwrap();
        // The fixture needs fragmented heap/index/LOB pages, not repeated
        // SQL evaluation of identical padding expressions.
        let padding = "x".repeat(175);
        let body = "b".repeat(20_000);
        insert_fixture_rows(
            &engine,
            "test.t",
            (0..384).map(|i| {
                vec![
                    Value::Int(i),
                    Value::String(format!("{i}{padding}")),
                    Value::String(body.clone()),
                ]
            }),
        );
        engine.checkpoint().unwrap();
        let objects = dir.path().join("objects");
        let before = ["heap", "idx", "lob"].map(|ext| bytes(&objects, ext));
        engine.execute("DELETE FROM t WHERE id < 374").unwrap();
        let expected = rows(&engine, "SELECT id,v,LENGTH(body) FROM t ORDER BY id");
        optimized(engine.execute("OPTIMIZE TABLE t").unwrap());
        assert_eq!(
            rows(&engine, "SELECT id,v,LENGTH(body) FROM t ORDER BY id"),
            expected
        );
        let after = ["heap", "idx", "lob"].map(|ext| bytes(&objects, ext));
        for i in 0..3 {
            assert!(after[i] < before[i], "{partition}: {before:?} -> {after:?}");
        }
        check_indexes(&engine);
        engine.execute("DELETE FROM t").unwrap();
        optimized(engine.execute("OPTIMIZE TABLE t").unwrap());
        assert_eq!(
            rows(&engine, "SELECT COUNT(*) FROM t"),
            vec![vec![Value::Int(0)]]
        );
        let empty = ["heap", "idx", "lob"].map(|ext| bytes(&objects, ext));
        optimized(engine.execute("OPTIMIZE TABLE t").unwrap());
        assert_eq!(
            ["heap", "idx", "lob"].map(|ext| bytes(&objects, ext)),
            empty
        );
        drop(engine);
        let engine = open_strict_test_engine(dir.path());
        assert_eq!(
            rows(&engine, "SELECT COUNT(*) FROM t"),
            vec![vec![Value::Int(0)]]
        );
        engine
            .execute("INSERT INTO t VALUES(999,'new',REPEAT('c',24000))")
            .unwrap();
        check_indexes(&engine);
    }
}

#[test]
fn optimize_online_backfill_allows_transactions_and_partition_moves() {
    for partition in ["", "PARTITION BY HASH(id) PARTITIONS 4", "PARTITION BY RANGE(id) SUBPARTITION BY HASH(id) SUBPARTITIONS 2 (PARTITION p0 VALUES LESS THAN (4), PARTITION p1 VALUES LESS THAN MAXVALUE)"] {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(open_strict_test_engine(dir.path()));
        engine.execute(&format!("CREATE TABLE t(id INT PRIMARY KEY,v INT,payload LONGTEXT,INDEX vi(v)) {partition}")).unwrap();
        engine.execute("INSERT INTO t VALUES(1,10,REPEAT('a',20000)),(2,20,'b'),(3,30,'c')").unwrap();
        let old = engine.database().table("test.t").unwrap();
        let hook = engine.database().install_ddl_backfill_test_hook();
        let worker = engine.clone();
        let ddl = std::thread::spawn(move || {
            let mut session = SessionContext::default();
            session.connection_id = 8911;
            worker.execute_with_session(&mut session,"OPTIMIZE TABLE t")
        });
        let reached = hook.wait_until_reached(Duration::from_secs(5));
        let worker = engine.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let writer = std::thread::spawn(move || {
            let mut session = SessionContext::default(); session.connection_id=8912;
            let result = (|| {
                for sql in ["BEGIN", "UPDATE t SET v=999 WHERE id=1", "ROLLBACK", "BEGIN", "UPDATE t SET id=8 WHERE id=1", "SAVEPOINT s", "DELETE FROM t WHERE id=2", "ROLLBACK TO SAVEPOINT s", "COMMIT", "DELETE FROM t WHERE id=3", "INSERT INTO t VALUES(4,40,REPEAT('d',24000))"] {
                    worker.execute_with_session(&mut session,sql)?;
                }
                Ok::<_,QueryError>(())
            })();
            tx.send(result).unwrap();
        });
        let writes = rx.recv_timeout(Duration::from_secs(3));
        hook.release();
        writer.join().unwrap();
        let result = ddl.join().unwrap();
        assert!(reached, "backfill not reached: {result:?}");
        writes.expect("DML must finish before releasing backfill").unwrap();
        optimized(result.unwrap());
        let current = engine.database().table("test.t").unwrap();
        assert_eq!(current.id,old.id);
        assert!(current.schema_version>old.schema_version);
        let expected=vec![vec![Value::Int(2),Value::Int(20),Value::Int(1)],vec![Value::Int(4),Value::Int(40),Value::Int(24000)],vec![Value::Int(8),Value::Int(10),Value::Int(20000)]];
        assert_eq!(rows(&engine,"SELECT id,v,LENGTH(payload) FROM t ORDER BY id"),expected);
        check_indexes(&engine);
        drop(engine);
        let engine=open_strict_test_engine(dir.path());
        assert_eq!(rows(&engine,"SELECT id,v,LENGTH(payload) FROM t ORDER BY id"),expected);
        check_indexes(&engine);
    }
}

#[test]
fn optimize_online_final_tail_unique_keys_and_cancel() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(open_strict_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT UNIQUE)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,10),(2,20)").unwrap();
    for cancel in [true, false] {
        let old = engine.database().table("test.t").unwrap();
        let hook = engine.database().install_online_copy_publish_test_hook();
        let worker = engine.clone();
        let ddl = std::thread::spawn(move || {
            let mut session = SessionContext::default();
            session.connection_id = 8921;
            worker.execute_with_session(&mut session, "OPTIMIZE TABLE t")
        });
        let reached = hook.wait_until_reached(Duration::from_secs(5));
        let mut session = SessionContext::default();
        session.connection_id = 8922;
        let writes = (|| {
            for sql in [
                "BEGIN",
                "DELETE FROM t",
                "INSERT INTO t VALUES(1,20),(2,10)",
                "COMMIT",
            ] {
                engine.execute_with_session(&mut session, sql)?;
            }
            if cancel {
                let job = engine
                    .database()
                    .alter_copy_job_snapshots()
                    .into_iter()
                    .find(|job| job.table == "t")
                    .unwrap();
                engine.execute(&format!("CANCEL DDL JOB {}", job.job_id))?;
            }
            Ok::<_, QueryError>(())
        })();
        hook.release();
        let result = ddl.join().unwrap();
        assert!(reached, "{result:?}");
        writes.unwrap();
        if cancel {
            assert_eq!(result.unwrap_err().code(), 1317);
            assert_eq!(
                engine.database().table("test.t").unwrap().active_generation,
                old.active_generation
            );
        } else {
            optimized(result.unwrap());
        }
        check_indexes(&engine);
    }
    assert_eq!(
        rows(&engine, "SELECT id,v FROM t ORDER BY id"),
        vec![
            vec![Value::Int(1), Value::Int(20)],
            vec![Value::Int(2), Value::Int(10)]
        ]
    );
}

#[test]
fn optimize_online_reports_per_table_failures() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY)")
        .unwrap();
    let result = rows(&engine, "OPTIMIZE TABLE missing,t");
    assert_eq!(result.len(), 3);
    assert_eq!(result[0][2], Value::String("Error".into()));
    assert_eq!(result[1][3], Value::String("Operation failed".into()));
    assert_eq!(result[2][3], Value::String("OK".into()));
    let memory = tempfile::tempdir().unwrap();
    let engine = open_test_engine(memory.path());
    engine.execute("CREATE TABLE t(id INT)").unwrap();
    let result = rows(&engine, "OPTIMIZE TABLE t");
    assert_eq!(result[0][2], Value::String("Error".into()));
    assert!(result[0][3].to_display_string().contains("durable storage"));
}

#[test]
fn optimize_online_preserves_types_generated_columns_and_auto_increment() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine.execute("CREATE TABLE t(id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT, v DECIMAL(30,6), s VARCHAR(50), b VARBINARY(20), j JSON, d DATETIME(6), e ENUM('red','blue'), n INT NULL, g BIGINT GENERATED ALWAYS AS (id+1) STORED, UNIQUE KEY uk(s,id), KEY prefix_idx(s(3))) AUTO_INCREMENT=100").unwrap();
    engine.execute("INSERT INTO t(v,s,b,j,d,e,n) VALUES(1234.500001,'中文abc',X'00FF12',JSON_OBJECT('x',1),'2024-01-02 03:04:05.123456','red',NULL),(-0.125,'two',X'1122',JSON_ARRAY(1,2),'2025-02-03 04:05:06.000001','blue',3)").unwrap();
    engine
        .execute("ALTER TABLE t ADD COLUMN added INT DEFAULT 42, ALGORITHM=INSTANT")
        .unwrap();
    let expected = rows(&engine, "SELECT * FROM t ORDER BY id");
    optimized(engine.execute("OPTIMIZE TABLE t").unwrap());
    assert_eq!(rows(&engine, "SELECT * FROM t ORDER BY id"), expected);
    assert_eq!(
        engine.database().table("test.t").unwrap().row_layouts.len(),
        1
    );
    check_indexes(&engine);
    engine.execute("INSERT INTO t(s) VALUES('three')").unwrap();
    assert_eq!(
        rows(&engine, "SELECT id,g,added FROM t WHERE s='three'"),
        vec![vec![Value::UInt(102), Value::Int(103), Value::Int(42)]]
    );
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    check_indexes(&engine);
    assert_eq!(
        rows(&engine, "SELECT * FROM t WHERE id<102 ORDER BY id"),
        expected
    );
}

#[test]
fn optimize_online_without_primary_key_preserves_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(v INT,s VARCHAR(20),INDEX vi(v))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,'a'),(1,'a'),(NULL,NULL),(2,'b')")
        .unwrap();
    engine.execute("DELETE FROM t WHERE v=2").unwrap();
    let expected = rows(&engine, "SELECT * FROM t ORDER BY v,s");
    optimized(engine.execute("OPTIMIZE TABLE t").unwrap());
    assert_eq!(rows(&engine, "SELECT * FROM t ORDER BY v,s"), expected);
    check_indexes(&engine);
}

#[test]
fn optimize_online_waiting_for_reader_does_not_queue_new_dml() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(open_strict_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,10)").unwrap();
    let hook = engine.database().install_online_copy_publish_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut s = SessionContext::default();
        s.connection_id = 8931;
        worker
            .execute_with_session(&mut s, "SET SESSION lock_wait_timeout=1")
            .unwrap();
        worker.execute_with_session(&mut s, "OPTIMIZE TABLE t")
    });
    let reached = hook.wait_until_reached(Duration::from_secs(5));
    let mut reader = SessionContext::default();
    reader.connection_id = 8932;
    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut reader, "SELECT * FROM t")
        .unwrap()
        .materialize()
        .unwrap();
    hook.release();
    let mut writer = SessionContext::default();
    writer.connection_id = 8933;
    let write = engine.execute_with_session(&mut writer, "UPDATE t SET v=30 WHERE id=1");
    let result = ddl.join().unwrap();
    engine
        .execute_with_session(&mut reader, "ROLLBACK")
        .unwrap();
    assert!(reached);
    write.unwrap();
    let QueryResponse::ResultSet(result) = result.unwrap() else {
        panic!("status")
    };
    assert_eq!(result.rows[0][2], Value::String("Error".into()));
    assert!(result.rows[0][3]
        .to_display_string()
        .contains("metadata lock"));
    optimized(engine.execute("OPTIMIZE TABLE t").unwrap());
    assert_eq!(rows(&engine, "SELECT v FROM t"), vec![vec![Value::Int(30)]]);
}

#[test]
fn optimize_online_rejects_stale_snapshot_and_temporary_shadow() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine.execute("CREATE TABLE t(id INT)").unwrap();
    engine.execute("CREATE TABLE other_t(id INT)").unwrap();
    engine.execute("INSERT INTO t VALUES(1)").unwrap();
    let mut reader = SessionContext::default();
    reader.connection_id = 8941;
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )
        .unwrap();
    engine
        .execute_with_session(&mut reader, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .unwrap();
    engine
        .execute_with_session(&mut reader, "SELECT * FROM other_t")
        .unwrap();
    optimized(engine.execute("OPTIMIZE TABLE t").unwrap());
    let result = engine.execute_with_session(&mut reader, "SELECT * FROM t");
    engine
        .execute_with_session(&mut reader, "ROLLBACK")
        .unwrap();
    assert_eq!(result.unwrap_err().code(), 1412);
    let old = engine.database().table("test.t").unwrap();
    engine
        .execute_with_session(&mut reader, "CREATE TEMPORARY TABLE t(id INT)")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut reader, "OPTIMIZE TABLE t")
        .unwrap()
    else {
        panic!("status")
    };
    assert_eq!(result.rows[0][2], Value::String("Error".into()));
    assert!(result.rows[0][3].to_display_string().contains("temporary"));
    assert_eq!(
        engine.database().table("test.t").unwrap().active_generation,
        old.active_generation
    );
}

#[test]
fn optimize_online_unpublished_generation_recovers_source() {
    fn copy_dir(source: &std::path::Path, target: &std::path::Path) {
        std::fs::create_dir_all(target).unwrap();
        for entry in std::fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            let to = target.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_dir(&entry.path(), &to);
            } else {
                std::fs::copy(entry.path(), to).unwrap();
            }
        }
    }
    for partition in ["", "PARTITION BY HASH(id) PARTITIONS 3"] {
        let dir = tempfile::tempdir().unwrap();
        let recovered = tempfile::tempdir().unwrap();
        let engine = Arc::new(open_strict_test_engine(dir.path()));
        engine
            .execute(&format!(
                "CREATE TABLE t(id INT PRIMARY KEY,v INT,payload LONGTEXT,INDEX vi(v)) {partition}"
            ))
            .unwrap();
        engine
            .execute("INSERT INTO t VALUES(1,10,REPEAT('x',20000)),(2,20,'b')")
            .unwrap();
        engine.checkpoint().unwrap();
        let hook = if partition.is_empty() {
            engine.database().install_online_copy_publish_test_hook()
        } else {
            engine.database().install_ddl_backfill_test_hook()
        };
        let worker = engine.clone();
        let ddl = std::thread::spawn(move || {
            let mut s = SessionContext::default();
            s.connection_id = 8951;
            worker.execute_with_session(&mut s, "OPTIMIZE TABLE t")
        });
        let reached = hook.wait_until_reached(Duration::from_secs(5));
        let mut writer = SessionContext::default();
        writer.connection_id = 8952;
        let write = engine.execute_with_session(&mut writer, "UPDATE t SET v=99 WHERE id=1");
        if reached {
            copy_dir(dir.path(), recovered.path());
        }
        hook.release();
        let result = ddl.join().unwrap();
        assert!(reached, "{result:?}");
        write.unwrap();
        optimized(result.unwrap());
        let expected = rows(&engine, "SELECT id,v,LENGTH(payload) FROM t ORDER BY id");
        let recovered_engine = open_strict_test_engine(recovered.path());
        assert_eq!(
            rows(
                &recovered_engine,
                "SELECT id,v,LENGTH(payload) FROM t ORDER BY id"
            ),
            expected
        );
        check_indexes(&recovered_engine);
        optimized(recovered_engine.execute("OPTIMIZE TABLE t").unwrap());
    }
}

#[test]
fn optimize_online_keeps_foreign_key_enforcement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE parent(id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE t(id INT PRIMARY KEY,pid INT,FOREIGN KEY(pid) REFERENCES parent(id))",
        )
        .unwrap();
    engine.execute("INSERT INTO parent VALUES(1)").unwrap();
    engine.execute("INSERT INTO t VALUES(1,1)").unwrap();
    optimized(engine.execute("OPTIMIZE TABLE parent").unwrap());
    optimized(engine.execute("OPTIMIZE TABLE t").unwrap());
    assert_eq!(
        engine
            .execute("INSERT INTO t VALUES(2,9)")
            .unwrap_err()
            .code(),
        1452
    );
    assert_eq!(
        engine
            .execute("DELETE FROM parent WHERE id=1")
            .unwrap_err()
            .code(),
        1451
    );
    check_indexes(&engine);
}

#[test]
fn optimize_online_fulltext_and_spatial_follow_concurrent_dml() {
    for partition in ["", "PARTITION BY HASH(id) PARTITIONS 2"] {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(open_strict_test_engine(dir.path()));
        let fulltext = if partition.is_empty() {
            ",FULLTEXT KEY ft(body)"
        } else {
            ""
        };
        engine.execute(&format!("CREATE TABLE t(id INT PRIMARY KEY,body TEXT,g GEOMETRY NOT NULL,SPATIAL KEY sp(g){fulltext}) {partition}")).unwrap();
        engine.execute("INSERT INTO t VALUES(1,'alpha bravo',ST_GeomFromText('POINT(1 1)')),(2,'charlie delta',ST_GeomFromText('POINT(2 2)'))").unwrap();
        let hook = engine.database().install_ddl_backfill_test_hook();
        let worker = engine.clone();
        let ddl = std::thread::spawn(move || {
            let mut s = SessionContext::default();
            s.connection_id = 8961;
            worker.execute_with_session(&mut s, "OPTIMIZE TABLE t")
        });
        let reached = hook.wait_until_reached(Duration::from_secs(5));
        let mut writer = SessionContext::default();
        writer.connection_id = 8962;
        let writes = [
            "UPDATE t SET body='delta echo',g=ST_GeomFromText('POINT(3 3)') WHERE id=1",
            "DELETE FROM t WHERE id=2",
            "INSERT INTO t VALUES(4,'delta foxtrot',ST_GeomFromText('POINT(4 4)'))",
        ]
        .map(|sql| engine.execute_with_session(&mut writer, sql));
        hook.release();
        let result = ddl.join().unwrap();
        assert!(reached, "{result:?}");
        for write in writes {
            write.unwrap();
        }
        optimized(result.unwrap());
        check_indexes(&engine);
        let expected = vec![vec![Value::Int(1)], vec![Value::Int(4)]];
        let query = if partition.is_empty() {
            "SELECT id FROM t WHERE MATCH(body) AGAINST('delta' IN BOOLEAN MODE) ORDER BY id"
        } else {
            "SELECT id FROM t WHERE ST_X(g)>=3 ORDER BY id"
        };
        assert_eq!(rows(&engine, query), expected);
        assert_eq!(
            rows(&engine, "SELECT ST_AsText(g) FROM t WHERE id=1")[0][0].to_display_string(),
            "POINT(3 3)"
        );
        drop(engine);
        let engine = open_strict_test_engine(dir.path());
        check_indexes(&engine);
        assert_eq!(rows(&engine, query), expected);
    }
}

#[test]
fn optimize_online_cached_dml_rebind_keeps_explicit_transaction() {
    for already_wrote_target in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT)")
            .unwrap();
        engine
            .execute("CREATE TABLE other_t(id INT PRIMARY KEY)")
            .unwrap();
        engine.execute("INSERT INTO t VALUES(1,10)").unwrap();
        let mut session = SessionContext::default();
        session.connection_id = 8971;
        let statement =
            Arc::new(crate::parser::parse_statement("UPDATE test.t SET v=v+1 WHERE id=1").unwrap());
        let plan = crate::engine::executor::prepared_dml::compile_prepared_dml(
            engine.database(),
            &session,
            &statement,
            engine.prepared_dml_cache.current_epoch(),
        )
        .unwrap();
        engine.execute_with_session(&mut session, "BEGIN").unwrap();
        engine
            .execute_with_session(&mut session, "INSERT INTO other_t VALUES(1)")
            .unwrap();
        if already_wrote_target {
            engine
                .execute_with_session(&mut session, "UPDATE t SET v=20 WHERE id=1")
                .unwrap();
            // Statistics publication is allowed while transaction MDL remains
            // held, including OPTIMIZE's post-publish ANALYZE phase.
            engine.execute("ANALYZE TABLE t").unwrap();
        } else {
            optimized(engine.execute("OPTIMIZE TABLE t").unwrap());
        }
        engine
            .execute_prepared_dml_core(&mut session, &plan, &[], Instant::now())
            .0
            .unwrap();
        assert!(session.explicit_txn);
        let QueryResponse::ResultSet(result) = engine
            .execute_with_session(&mut session, "SELECT v FROM t WHERE id=1")
            .unwrap()
        else {
            panic!("rows")
        };
        assert_eq!(
            result.rows,
            vec![vec![Value::Int(if already_wrote_target { 21 } else { 11 })]]
        );
        engine
            .execute_with_session(&mut session, "ROLLBACK")
            .unwrap();
        assert_eq!(
            rows(&engine, "SELECT v FROM t WHERE id=1"),
            vec![vec![Value::Int(10)]]
        );
        assert!(
            rows(&engine, "SELECT * FROM other_t").is_empty(),
            "rebind must not commit prior writes"
        );
    }
}

#[test]
fn optimize_online_rejects_partitioned_fulltext_before_rebuild() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY,body TEXT,FULLTEXT KEY ft(body)) PARTITION BY HASH(id) PARTITIONS 2").unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,'alpha bravo'),(2,'charlie delta')")
        .unwrap();
    let old = engine.database().table("test.t").unwrap();
    let expected = rows(&engine, "SELECT * FROM t ORDER BY id");
    let result = rows(&engine, "OPTIMIZE TABLE t");
    assert_eq!(result[0][2], Value::String("Error".into()));
    assert!(result[0][3]
        .to_display_string()
        .contains("partitioned FULLTEXT"));
    assert_eq!(
        engine.database().table("test.t").unwrap().storage_ids(),
        old.storage_ids()
    );
    assert_eq!(rows(&engine, "SELECT * FROM t ORDER BY id"), expected);
}
