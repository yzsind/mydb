use super::*;

fn fixture() -> (tempfile::TempDir, StorageQueryEngine) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let mut config = memory_test_storage_config();
    config.lock_wait = Duration::from_millis(200);
    let engine =
        StorageQueryEngine::open_with_storage_config_without_background_workers(dir.path(), config)
            .unwrap();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,10),(2,20)").unwrap();
    (dir, engine)
}

#[test]
fn read_compatible_add_and_modify_columns_with_open_reader() {
    for sql in [
        "ALTER TABLE t ADD COLUMN extra INT DEFAULT 7",
        "ALTER TABLE t ADD COLUMN extra INT DEFAULT 7 AFTER id",
        "ALTER TABLE t MODIFY COLUMN c VARCHAR(128)",
        "ALTER TABLE t MODIFY COLUMN c VARCHAR(128) FIRST",
        "ALTER TABLE t MODIFY COLUMN v INT NOT NULL",
        "ALTER TABLE t MODIFY COLUMN v INT NOT NULL AFTER c",
    ] {
        let (_dir, engine) = fixture();
        engine
            .execute("ALTER TABLE t ADD COLUMN c VARCHAR(16) DEFAULT 'old'")
            .unwrap();
        let mut reader = SessionContext::default();
        reader.connection_id = 911;
        reader.isolation = crate::storage::IsolationLevel::RepeatableRead;
        engine.execute_with_session(&mut reader, "BEGIN").unwrap();
        engine
            .execute_with_session(&mut reader, "SELECT * FROM t")
            .unwrap();
        let mut ddl = SessionContext::default();
        ddl.connection_id = 912;
        ddl.sysvars.lock_wait_timeout = Some(1);
        let result = engine.execute_with_session(&mut ddl, sql);
        engine
            .execute_with_session(&mut reader, "ROLLBACK")
            .unwrap();
        result.unwrap_or_else(|err| panic!("{sql}: {err}"));
        let QueryResponse::ResultSet(rows) =
            engine.execute("SELECT id,v,c FROM t ORDER BY id").unwrap()
        else {
            panic!("expected rows")
        };
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(
            rows.rows[0],
            vec![Value::Int(1), Value::Int(10), Value::String("old".into())]
        );
    }
}

#[test]
fn read_compatible_column_change_preserves_stream_across_new_writes() {
    for (ddl_sql, update_sql) in [
        (
            "ALTER TABLE t ADD COLUMN extra INT DEFAULT 7",
            "UPDATE t SET v=20100 WHERE id=20000",
        ),
        (
            "ALTER TABLE t ADD COLUMN extra INT DEFAULT 7 FIRST",
            "UPDATE t SET v=20100 WHERE id=20000",
        ),
        (
            "ALTER TABLE t MODIFY COLUMN c VARCHAR(128)",
            "UPDATE t SET c='a value longer than the original column' WHERE id=20000",
        ),
        (
            "ALTER TABLE t MODIFY COLUMN c VARCHAR(128) FIRST",
            "UPDATE t SET c='a value longer than the original column' WHERE id=20000",
        ),
        (
            "ALTER TABLE t MODIFY COLUMN v INT NOT NULL",
            "UPDATE t SET v=20100 WHERE id=20000",
        ),
    ] {
        let (_dir, engine) = fixture();
        engine
            .execute("INSERT INTO t SELECT n,n FROM generate_series(3,20000) g(n)")
            .unwrap();
        engine
            .execute("ALTER TABLE t ADD COLUMN c VARCHAR(16) DEFAULT 'old'")
            .unwrap();
        let mut reader = SessionContext::default();
        reader.connection_id = 921;
        reader.isolation = crate::storage::IsolationLevel::RepeatableRead;
        engine.execute_with_session(&mut reader, "BEGIN").unwrap();
        let response = engine
            .execute_streaming_with_session_for_test(&mut reader, "SELECT * FROM t")
            .unwrap();
        assert!(matches!(response, QueryResponse::StreamingResultSet(_)));
        let mut ddl = SessionContext::default();
        ddl.connection_id = 922;
        ddl.sysvars.lock_wait_timeout = Some(1);
        let changed = engine.execute_with_session(&mut ddl, ddl_sql);
        if changed.is_ok() {
            engine.execute(update_sql).unwrap();
            engine
                .execute("INSERT INTO t(id,v,c) VALUES(20001,1,'new')")
                .unwrap();
        }
        let rows = response.materialize();
        engine
            .execute_with_session(&mut reader, "ROLLBACK")
            .unwrap();
        changed.unwrap_or_else(|err| panic!("{ddl_sql}: {err}"));
        let QueryResponse::ResultSet(mut rows) = rows.unwrap() else {
            panic!("expected rows")
        };
        assert_eq!(
            rows.columns.len(),
            3,
            "old wire metadata changed: {ddl_sql}"
        );
        assert_eq!(rows.rows.len(), 20000);
        rows.rows
            .sort_by_key(|row| row[0].to_display_string().parse::<i64>().unwrap());
        assert!(rows.rows.iter().all(|row| row.len() == 3));
        assert_eq!(
            rows.rows.last().unwrap(),
            &vec![
                Value::Int(20000),
                Value::Int(20000),
                Value::String("old".into())
            ],
            "{ddl_sql}"
        );
    }
}

#[test]
fn read_compatible_failed_not_null_keeps_schema_and_write_binding_valid() {
    let (_dir, engine) = fixture();
    engine.execute("INSERT INTO t VALUES(3,NULL)").unwrap();
    let mut reader = SessionContext::default();
    reader.connection_id = 931;
    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut reader, "SELECT * FROM t")
        .unwrap();
    let mut ddl = SessionContext::default();
    ddl.connection_id = 932;
    ddl.sysvars.lock_wait_timeout = Some(1);
    let err = engine
        .execute_with_session(&mut ddl, "ALTER TABLE t MODIFY v INT NOT NULL")
        .unwrap_err();
    engine
        .execute_with_session(&mut reader, "ROLLBACK")
        .unwrap();
    assert_eq!(err.code(), 1048);
    assert!(engine.database().table("test.t").unwrap().columns[1].nullable);
    engine.execute("INSERT INTO t VALUES(4,NULL)").unwrap();
    engine.execute("UPDATE t SET v=40 WHERE id=4").unwrap();
}

#[test]
fn read_compatible_not_null_preserves_old_snapshot_nulls() {
    let (_dir, engine) = fixture();
    engine.execute("INSERT INTO t VALUES(3,NULL)").unwrap();
    let mut reader = SessionContext::default();
    reader.connection_id = 941;
    reader.isolation = crate::storage::IsolationLevel::RepeatableRead;
    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut reader, "SELECT * FROM t")
        .unwrap();
    engine.execute("UPDATE t SET v=30 WHERE id=3").unwrap();
    engine
        .execute("ALTER TABLE t MODIFY v INT NOT NULL")
        .unwrap();
    // A current-snapshot plan may use COUNT(*) for the now-NOT-NULL column.
    // Reusing it under the old RR snapshot must restore COUNT(column).
    engine.execute("SELECT COUNT(v) FROM t").unwrap();
    let response = engine.execute_with_session(&mut reader, "SELECT id FROM t WHERE v IS NULL");
    let count = engine.execute_with_session(&mut reader, "SELECT COUNT(v) FROM t");
    engine
        .execute_with_session(&mut reader, "ROLLBACK")
        .unwrap();
    let QueryResponse::ResultSet(rows) = response.unwrap() else {
        panic!("expected rows")
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(3)]]);
    let QueryResponse::ResultSet(count) = count.unwrap() else {
        panic!("expected count")
    };
    assert_eq!(count.rows, vec![vec![Value::Int(2)]]);
}

#[test]
fn read_compatible_primary_key_keeps_old_duplicates_and_nulls_readable() {
    let (_dir, engine) = fixture();
    engine
        .execute("CREATE TABLE pk_later(k INT, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO pk_later VALUES(1,10),(1,20),(NULL,30)")
        .unwrap();
    let mut reader = SessionContext::default();
    reader.connection_id = 951;
    reader.isolation = crate::storage::IsolationLevel::RepeatableRead;
    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut reader, "SELECT * FROM pk_later")
        .unwrap();
    engine.execute("UPDATE pk_later SET k=v").unwrap();
    let mut ddl = SessionContext::default();
    ddl.connection_id = 952;
    ddl.sysvars.lock_wait_timeout = Some(1);
    let result = engine.execute_with_session(
        &mut ddl,
        "ALTER TABLE pk_later ADD PRIMARY KEY(k), LOCK=NONE",
    );
    if let Err(err) = result {
        engine
            .execute_with_session(&mut reader, "ROLLBACK")
            .unwrap();
        panic!("primary key addition blocked: {err}");
    }
    for (sql, expected) in [
        ("SELECT COUNT(k) FROM pk_later", vec![vec![Value::Int(2)]]),
        (
            "SELECT v FROM pk_later WHERE k IS NULL",
            vec![vec![Value::Int(30)]],
        ),
        (
            "SELECT v FROM pk_later WHERE k=1 ORDER BY v",
            vec![vec![Value::Int(10)], vec![Value::Int(20)]],
        ),
    ] {
        engine.execute(sql).unwrap();
        let QueryResponse::ResultSet(rows) = engine.execute_with_session(&mut reader, sql).unwrap()
        else {
            panic!("expected rows")
        };
        assert_eq!(rows.rows, expected, "{sql}");
    }
    engine
        .execute_with_session(&mut reader, "ROLLBACK")
        .unwrap();
    engine
        .execute("INSERT INTO pk_later VALUES(40,40)")
        .unwrap();
    assert_eq!(
        engine
            .execute("INSERT INTO pk_later VALUES(40,50)")
            .unwrap_err()
            .code(),
        1062
    );
}

#[test]
fn read_compatible_index_does_not_wait_for_read_transaction() {
    let (_dir, engine) = fixture();
    let mut reader = SessionContext::default();
    reader.connection_id = 801;
    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut reader, "SELECT * FROM t")
        .unwrap();
    let mut ddl = SessionContext::default();
    ddl.connection_id = 802;
    let result = engine.execute_with_session(&mut ddl, "CREATE INDEX v_idx ON t(v)");
    engine
        .execute_with_session(&mut reader, "ROLLBACK")
        .unwrap();
    result.expect("ordinary readers must not block online index publication");
}

#[test]
fn read_compatible_index_old_snapshot_keeps_historical_key() {
    let (_dir, engine) = fixture();
    let mut reader = SessionContext::default();
    reader.connection_id = 811;
    reader.isolation = crate::storage::IsolationLevel::RepeatableRead;
    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut reader, "SELECT * FROM t")
        .unwrap();
    engine.execute("UPDATE t SET v=30 WHERE id=1").unwrap();
    let mut ddl = SessionContext::default();
    ddl.connection_id = 812;
    let result = engine.execute_with_session(&mut ddl, "CREATE INDEX v_idx ON t(v)");
    if let Err(err) = result {
        engine
            .execute_with_session(&mut reader, "ROLLBACK")
            .unwrap();
        panic!("reader blocked index: {err}");
    }
    let result = engine
        .execute_with_session(&mut reader, "SELECT id FROM t WHERE v=10")
        .unwrap();
    let QueryResponse::ResultSet(result) = result else {
        panic!("expected rows")
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);
    engine
        .execute_with_session(&mut reader, "ROLLBACK")
        .unwrap();
}

#[test]
fn read_compatible_constraints_do_not_wait_for_read_transactions() {
    for ddl_sql in [
        "ALTER TABLE t ADD CONSTRAINT positive CHECK(v>0), LOCK=NONE",
        "ALTER TABLE t ADD CONSTRAINT fk_v FOREIGN KEY(v) REFERENCES p(v), LOCK=NONE",
    ] {
        let (_dir, engine) = fixture();
        engine.execute("CREATE TABLE p(v INT PRIMARY KEY)").unwrap();
        engine.execute("INSERT INTO p VALUES(10),(20)").unwrap();
        let mut reader = SessionContext::default();
        reader.connection_id = 821;
        engine.execute_with_session(&mut reader, "BEGIN").unwrap();
        engine
            .execute_with_session(&mut reader, "SELECT t.id FROM t JOIN p ON t.v=p.v")
            .unwrap();
        let mut ddl = SessionContext::default();
        ddl.connection_id = 822;
        let result = engine.execute_with_session(&mut ddl, ddl_sql);
        engine
            .execute_with_session(&mut reader, "ROLLBACK")
            .unwrap();
        result.unwrap_or_else(|err| panic!("{ddl_sql}: {err}"));
        assert!(engine.execute("INSERT INTO t VALUES(3,-1)").is_err());
    }
}

#[test]
fn read_compatible_index_keeps_unconsumed_stream_alive() {
    let (_dir, engine) = fixture();
    let mut reader = SessionContext::default();
    reader.connection_id = 831;
    let response = engine
        .execute_streaming_with_session_for_test(&mut reader, "SELECT id,v FROM t ORDER BY id")
        .unwrap();
    assert!(matches!(response, QueryResponse::StreamingResultSet(_)));
    let mut ddl = SessionContext::default();
    ddl.connection_id = 832;
    let result = engine.execute_with_session(&mut ddl, "CREATE INDEX v_idx ON t(v)");
    let QueryResponse::ResultSet(rows) = response.materialize().unwrap() else {
        panic!("expected rows")
    };
    result.unwrap();
    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)]
        ]
    );
}

#[test]
fn read_compatible_index_preserves_explicit_exclusive_lock() {
    let (_dir, engine) = fixture();
    let mut reader = SessionContext::default();
    reader.connection_id = 841;
    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut reader, "SELECT * FROM t")
        .unwrap();
    let mut ddl = SessionContext::default();
    ddl.connection_id = 842;
    let err = engine
        .execute_with_session(&mut ddl, "CREATE INDEX v_idx ON t(v) LOCK=EXCLUSIVE")
        .unwrap_err();
    assert_eq!(err.code(), 1205);
    engine
        .execute_with_session(&mut reader, "ROLLBACK")
        .unwrap();
    engine
        .execute_with_session(&mut ddl, "CREATE INDEX v_idx ON t(v) LOCK=EXCLUSIVE")
        .unwrap();
}

#[test]
fn read_compatible_index_accepts_serializable_reader_table_lock() {
    let (_dir, engine) = fixture();
    let mut reader = SessionContext::default();
    reader.connection_id = 851;
    reader.isolation = crate::storage::IsolationLevel::Serializable;
    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut reader, "SELECT * FROM t")
        .unwrap();
    let mut ddl = SessionContext::default();
    ddl.connection_id = 852;
    let result = engine.execute_with_session(&mut ddl, "CREATE INDEX v_idx ON t(v)");
    engine
        .execute_with_session(&mut reader, "ROLLBACK")
        .unwrap();
    result.unwrap();
}

#[test]
fn read_compatible_metadata_does_not_wait_for_read_transaction() {
    for sql in [
        "ALTER TABLE t COMMENT='read-compatible'",
        "ALTER TABLE t ALTER INDEX PRIMARY VISIBLE",
        "ALTER TABLE t ALTER COLUMN v SET DEFAULT 42",
        "ALTER TABLE t ALTER COLUMN v SET INVISIBLE",
        "ALTER TABLE t DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
        "ALTER TABLE t ROW_FORMAT=DYNAMIC",
    ] {
        let (_dir, engine) = fixture();
        let mut reader = SessionContext::default();
        reader.connection_id = 861;
        engine.execute_with_session(&mut reader, "BEGIN").unwrap();
        engine
            .execute_with_session(&mut reader, "SELECT * FROM t")
            .unwrap();
        let mut ddl = SessionContext::default();
        ddl.connection_id = 862;
        ddl.sysvars.lock_wait_timeout = Some(1);
        let result = engine.execute_with_session(&mut ddl, sql);
        engine
            .execute_with_session(&mut reader, "ROLLBACK")
            .unwrap();
        result.unwrap_or_else(|err| panic!("{sql}: {err}"));
    }
}

#[test]
fn read_compatible_unique_index_failure_leaves_readers_and_catalog_valid() {
    let (_dir, engine) = fixture();
    engine.execute("INSERT INTO t VALUES(3,10)").unwrap();
    let mut reader = SessionContext::default();
    reader.connection_id = 871;
    reader.isolation = crate::storage::IsolationLevel::RepeatableRead;
    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut reader, "SELECT * FROM t")
        .unwrap();
    let mut ddl = SessionContext::default();
    ddl.connection_id = 872;
    let err = engine
        .execute_with_session(&mut ddl, "CREATE UNIQUE INDEX v_idx ON t(v)")
        .unwrap_err();
    assert_eq!(err.code(), 1062);
    assert!(engine
        .database()
        .table("test.t")
        .unwrap()
        .indexes
        .iter()
        .all(|i| i.name != "v_idx"));
    let QueryResponse::ResultSet(rows) = engine
        .execute_with_session(&mut reader, "SELECT id FROM t WHERE v=10 ORDER BY id")
        .unwrap()
    else {
        panic!("expected rows")
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(1)], vec![Value::Int(3)]]);
    engine
        .execute_with_session(&mut reader, "ROLLBACK")
        .unwrap();
}

#[test]
fn read_compatible_old_snapshot_replans_cached_and_ordered_access_paths() {
    let (_dir, engine) = fixture();
    let mut reader = SessionContext::default();
    reader.connection_id = 881;
    reader.isolation = crate::storage::IsolationLevel::RepeatableRead;
    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut reader, "SELECT * FROM t")
        .unwrap();
    engine.execute("UPDATE t SET v=30 WHERE id=1").unwrap();
    let mut ddl = SessionContext::default();
    ddl.connection_id = 882;
    engine
        .execute_with_session(&mut ddl, "CREATE INDEX v_idx ON t(v)")
        .unwrap();
    for (sql, expected) in [
        ("SELECT id FROM t WHERE v=10", vec![vec![Value::Int(1)]]),
        (
            "SELECT v FROM t ORDER BY v LIMIT 1",
            vec![vec![Value::Int(10)]],
        ),
        (
            "SELECT COUNT(*) FROM t WHERE v<20",
            vec![vec![Value::Int(1)]],
        ),
        (
            "SELECT a.id FROM t a JOIN t b ON a.v=b.v WHERE b.v=10",
            vec![vec![Value::Int(1)]],
        ),
    ] {
        // Warm a plan for the newer view before running the identical SQL
        // under the older view; plan-cache keys do not include snapshot SCN.
        engine.execute(sql).unwrap();
        let QueryResponse::ResultSet(rows) = engine
            .execute_with_session(&mut reader, sql)
            .unwrap()
            .materialize()
            .unwrap()
        else {
            panic!("expected rows")
        };
        assert_eq!(rows.rows, expected, "{sql}");
    }
    engine
        .execute_with_session(&mut reader, "ROLLBACK")
        .unwrap();
}

#[test]
fn read_compatible_snapshot_scope_does_not_hide_indexes_from_function_writes() {
    let (_dir, engine) = fixture();
    engine.execute("CREATE FUNCTION duplicate_v() RETURNS INT MODIFIES SQL DATA BEGIN INSERT INTO t VALUES(3,10); RETURN 1; END").unwrap();
    let mut reader = SessionContext::default();
    reader.connection_id = 891;
    reader.isolation = crate::storage::IsolationLevel::RepeatableRead;
    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut reader, "SELECT * FROM t")
        .unwrap();
    let mut ddl = SessionContext::default();
    ddl.connection_id = 892;
    engine
        .execute_with_session(&mut ddl, "CREATE UNIQUE INDEX v_idx ON t(v)")
        .unwrap();
    let err = engine
        .execute_with_session(&mut reader, "SELECT duplicate_v()")
        .unwrap_err();
    assert_eq!(err.code(), 1062);
    engine
        .execute_with_session(&mut reader, "ROLLBACK")
        .unwrap();
    let QueryResponse::ResultSet(rows) = engine.execute("SELECT id FROM t ORDER BY id").unwrap()
    else {
        panic!("expected rows")
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
}

#[test]
fn read_compatible_index_snapshot_boundary_survives_reopen() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let mut config = memory_test_storage_config();
    config.durability = crate::storage::DurabilityMode::Strict;
    let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir.path(),
        config.clone(),
    )
    .unwrap();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,10)").unwrap();
    engine.execute("CREATE INDEX v_idx ON t(v)").unwrap();
    let boundary = engine
        .database()
        .table("test.t")
        .unwrap()
        .indexes
        .iter()
        .find(|i| i.name == "v_idx")
        .unwrap()
        .readable_from_scn;
    assert!(boundary > 0);
    drop(engine);
    let engine =
        StorageQueryEngine::open_with_storage_config_without_background_workers(dir.path(), config)
            .unwrap();
    let table = engine.database().table("test.t").unwrap();
    let index = table.indexes.iter().find(|i| i.name == "v_idx").unwrap();
    assert_eq!(index.readable_from_scn, boundary);
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT id FROM t FORCE INDEX(v_idx) WHERE v=10")
        .unwrap()
    else {
        panic!("expected rows")
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn read_compatible_shared_build_serializes_other_ddl_but_admits_readers() {
    let (_dir, engine) = fixture();
    let engine = std::sync::Arc::new(engine);
    let hook = engine.database().install_ddl_backfill_test_hook();
    let builder = engine.clone();
    let build = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 901;
        builder.execute_with_session(&mut session, "CREATE INDEX v_idx ON t(v) LOCK=SHARED")
    });
    let reached = hook.wait_until_reached(Duration::from_secs(5));
    if !reached {
        hook.release();
        panic!(
            "builder did not reach backfill: {:?}",
            build.join().unwrap()
        );
    }
    let mut other = SessionContext::default();
    other.connection_id = 902;
    let second =
        engine.execute_with_session(&mut other, "CREATE INDEX id_idx ON t(id) LOCK=SHARED");
    let read = engine.execute("SELECT id,v FROM t ORDER BY id");
    hook.release();
    build.join().unwrap().unwrap();
    assert_eq!(second.unwrap_err().code(), 1205);
    assert!(read.is_ok());
    engine
        .execute_with_session(&mut other, "CREATE INDEX id_idx ON t(id) LOCK=SHARED")
        .unwrap();
    let table = engine.database().table("test.t").unwrap();
    for name in ["v_idx", "id_idx"] {
        assert!(table
            .indexes
            .iter()
            .any(|i| i.name == name && i.state == crate::storage::IndexState::Public));
    }
}
