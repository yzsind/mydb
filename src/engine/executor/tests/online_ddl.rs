use super::*;

#[test]
fn load_data_private_index_generation_covers_old_wal() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    let old_shadow_hits = engine.database().load_shadow_perf_snapshot()[0];
    let old_lsn = engine.database().wal_log().lock().last_lsn();
    let input = dir.path().join("rows.csv");
    let rows = (1..=4096).map(|i| format!("{i},{i}\n")).collect::<String>();
    std::fs::write(&input, rows).unwrap();
    engine.execute(&format!("LOAD DATA /*+ PARALLEL(2) */ INFILE '{}' INTO TABLE t FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'", crate::util::file::sql_path(&input))).unwrap();
    assert_eq!(
        engine.database().load_shadow_perf_snapshot()[0],
        old_shadow_hits + 1
    );
    let table = engine.database().table("test.t").unwrap();
    let index = table.indexes.iter().find(|i| i.primary).unwrap();
    let path = crate::storage::naming::index_object_path_by_id(
        dir.path(),
        table.id,
        table.active_generation,
        index.index_id,
        "idx",
    );
    let bytes = std::fs::read(path).unwrap();
    assert!(
        crate::wal::page_lsn(&bytes) >= old_lsn,
        "new index generation LSN {} does not cover old initialization WAL {old_lsn}",
        crate::wal::page_lsn(&bytes)
    );
    assert_online_index_matches_heap(&engine, "PRIMARY");
}

#[test]
fn online_primary_key_allows_dml_and_defers_constraints() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_relaxed_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT NULL, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,10),(2,20)").unwrap();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 7101;
        worker.execute_with_session(&mut session, "ALTER TABLE t ADD PRIMARY KEY(id), LOCK=NONE")
    });
    let reached = hook.wait_until_reached(Duration::from_secs(5));
    let during = engine.database().table("test.t").unwrap();
    let mut writer = SessionContext::default();
    writer.connection_id = 7102;
    let writes = [
        "INSERT INTO t VALUES(3,30)",
        "UPDATE t SET v=11 WHERE id=1",
        "DELETE FROM t WHERE id=2",
    ]
    .map(|sql| engine.execute_with_session(&mut writer, sql));
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "build hook was not reached: {result:?}");
    for write in writes {
        write.expect("primary key build must permit DML");
    }
    assert!(
        during.primary_key.is_empty(),
        "candidate PK became public early"
    );
    assert!(
        during.columns[0].nullable,
        "candidate NOT NULL became public early"
    );
    result.unwrap();
    assert_online_index_matches_heap(&engine, "PRIMARY");
    assert_eq!(
        engine
            .execute("INSERT INTO t VALUES(NULL,40)")
            .unwrap_err()
            .code(),
        1048
    );
    assert_eq!(
        engine
            .execute("INSERT INTO t VALUES(1,40)")
            .unwrap_err()
            .code(),
        1062
    );
}

#[test]
fn online_primary_key_repairs_null_and_duplicate_groups_after_validation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_strict_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT NULL, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(NULL,10),(2,20),(2,30)")
        .unwrap();
    let hook = engine
        .database()
        .install_online_index_validation_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 7111;
        worker.execute_with_session(
            &mut session,
            "ALTER TABLE t ADD PRIMARY KEY(id), ADD INDEX v_idx(v), LOCK=NONE",
        )
    });
    let reached = hook.wait_until_reached(Duration::from_secs(5));
    engine
        .database()
        .force_cold_online_descriptor_for_test("test.t");
    let mut writer = SessionContext::default();
    writer.connection_id = 7112;
    let writes = [
        "UPDATE t SET id=1 WHERE v=10",
        "UPDATE t SET id=3 WHERE v=30",
    ]
    .map(|sql| engine.execute_with_session(&mut writer, sql));
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "validation hook not reached: {result:?}");
    for write in writes {
        write.unwrap();
    }
    result.unwrap();
    assert_online_index_matches_heap(&engine, "PRIMARY");
    assert_online_index_matches_heap(&engine, "v_idx");
    assert!(engine
        .database()
        .table("test.t")
        .unwrap()
        .pending_primary_key
        .is_none());
    drop(engine);
    let reopened = open_strict_cold_metadata_engine(dir.path());
    assert_online_index_matches_heap(&reopened, "PRIMARY");
    assert_eq!(
        reopened
            .execute("INSERT INTO t VALUES(NULL,40)")
            .unwrap_err()
            .code(),
        1048
    );
}

#[test]
fn online_primary_key_rollbacks_compensate_scanned_invalid_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_relaxed_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT NULL, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,10),(2,20)").unwrap();
    let build = engine.database().install_ddl_backfill_test_hook();
    let replay = engine
        .database()
        .install_online_index_validation_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 7121;
        worker.execute_with_session(&mut session, "ALTER TABLE t ADD PRIMARY KEY(id), LOCK=NONE")
    });
    let reached = build.wait_until_reached(Duration::from_secs(5));
    let mut writer = SessionContext::default();
    writer.connection_id = 7122;
    let changed = (|| {
        engine.execute_with_session(&mut writer, "BEGIN")?;
        engine.execute_with_session(&mut writer, "SAVEPOINT s")?;
        engine.execute_with_session(&mut writer, "UPDATE t SET id=NULL WHERE v=10")?;
        engine.execute_with_session(&mut writer, "INSERT INTO t VALUES(2,30)")?;
        Ok::<_, QueryError>(())
    })();
    build.release();
    let scanned = replay.wait_until_reached(Duration::from_secs(5));
    let restored = (|| {
        engine.execute_with_session(&mut writer, "ROLLBACK TO SAVEPOINT s")?;
        engine.execute_with_session(&mut writer, "COMMIT")?;
        Ok::<_, QueryError>(())
    })();
    replay.release();
    let result = ddl.join().unwrap();
    assert!(reached && scanned, "hooks were not reached: {result:?}");
    changed.unwrap();
    restored.unwrap();
    result.unwrap();
    assert_online_index_matches_heap(&engine, "PRIMARY");
}

#[test]
fn online_primary_key_failures_preserve_rows_and_old_schema() {
    for (value, code) in [("NULL", 1138), ("1", 1062)] {
        let dir = tempfile::tempdir().unwrap();
        let engine = std::sync::Arc::new(open_relaxed_test_engine(dir.path()));
        engine
            .execute("CREATE TABLE t(id INT NULL, v INT)")
            .unwrap();
        engine.execute("INSERT INTO t VALUES(1,10)").unwrap();
        let hook = engine
            .database()
            .install_online_index_validation_test_hook();
        let worker = engine.clone();
        let ddl = std::thread::spawn(move || {
            let mut session = SessionContext::default();
            session.connection_id = 7131;
            worker.execute_with_session(
                &mut session,
                "ALTER TABLE t ADD PRIMARY KEY(id), ADD INDEX v_idx(v), LOCK=NONE",
            )
        });
        let reached = hook.wait_until_reached(Duration::from_secs(5));
        let mut writer = SessionContext::default();
        writer.connection_id = 7132;
        let inserted =
            engine.execute_with_session(&mut writer, &format!("INSERT INTO t VALUES({value},20)"));
        hook.release();
        let result = ddl.join().unwrap();
        assert!(reached, "validation hook not reached: {result:?}");
        inserted.unwrap();
        assert_eq!(result.unwrap_err().code(), code);
        let table = engine.database().table("test.t").unwrap();
        assert!(table.primary_key.is_empty() && table.columns[0].nullable);
        assert!(table.indexes.is_empty() && table.pending_primary_key.is_none());
        let QueryResponse::ResultSet(rows) = engine.execute("SELECT COUNT(*) FROM t").unwrap()
        else {
            panic!("rows")
        };
        assert_eq!(rows.rows, vec![vec![Value::Int(2)]]);
        engine.execute("DELETE FROM t WHERE v=20").unwrap();
        engine
            .execute("ALTER TABLE t ADD PRIMARY KEY(id), LOCK=NONE")
            .unwrap();
        assert_online_index_matches_heap(&engine, "PRIMARY");
    }
}

#[test]
fn online_primary_key_waits_for_deferred_commit_completion() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_relaxed_test_engine(dir.path()));
    engine.execute("CREATE TABLE t(id INT, v INT)").unwrap();
    let db = engine.database();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(txn, "test.t", vec![vec![Value::Int(1), Value::Int(10)]])
        .unwrap();
    let mut commit = db.commit_txn_deferred(txn.txn_id).unwrap();
    let hook = db.install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 7141;
        worker.execute_with_session(&mut session, "ALTER TABLE t ADD PRIMARY KEY(id), LOCK=NONE")
    });
    let too_early = hook.wait_until_reached(Duration::from_millis(50));
    db.finish_deferred_commits(&mut [&mut commit]).unwrap();
    let reached = hook.wait_until_reached(Duration::from_secs(5));
    hook.release();
    let result = ddl.join().unwrap();
    assert!(
        !too_early,
        "DDL passed a commit whose durability/visibility had not finished"
    );
    assert!(reached, "DDL failed to continue: {result:?}");
    result.unwrap();
    assert_online_index_matches_heap(&engine, "PRIMARY");
}

#[test]
fn online_primary_key_preserves_older_unbound_read_views() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_relaxed_test_engine(dir.path());
    engine.execute("CREATE TABLE t(id INT, v INT)").unwrap();
    engine.execute("INSERT INTO t VALUES(1,10)").unwrap();
    let mut reader = SessionContext::default();
    reader.connection_id = 7151;
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut reader,
            "PREPARE old_count FROM 'SELECT COUNT(*) FROM t'",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut reader,
            "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY",
        )
        .unwrap();
    engine.execute("INSERT INTO t VALUES(2,20)").unwrap();
    engine.execute("ALTER TABLE t ADD PRIMARY KEY(id)").unwrap();
    for (sql, expected) in [
        ("SELECT COUNT(*) FROM t", vec![vec![Value::Int(1)]]),
        ("EXECUTE old_count", vec![vec![Value::Int(1)]]),
        (
            "SELECT * FROM t WHERE id=1",
            vec![vec![Value::Int(1), Value::Int(10)]],
        ),
        ("SELECT * FROM t WHERE 1=0", Vec::new()),
    ] {
        let QueryResponse::ResultSet(result) = engine
            .execute_with_session(&mut reader, sql)
            .unwrap()
            .materialize()
            .unwrap()
        else {
            panic!("expected rows for {sql}")
        };
        assert_eq!(result.rows, expected, "{sql}");
    }
    engine
        .execute_with_session(&mut reader, "ROLLBACK")
        .unwrap();
    engine
        .execute_with_session(&mut reader, "SELECT * FROM t")
        .unwrap();
}

#[test]
fn online_primary_key_partitioned_composite_and_autoincrement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_relaxed_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT, v INT, s VARCHAR(30)) PARTITION BY HASH(id) PARTITIONS 2")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,10,'a'),(2,20,'b')")
        .unwrap();
    let hook = engine
        .database()
        .install_online_index_validation_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 7161;
        worker.execute_with_session(
            &mut session,
            "ALTER TABLE t ADD PRIMARY KEY(id,v), ADD INDEX s_idx(s), LOCK=NONE",
        )
    });
    let reached = hook.wait_until_reached(Duration::from_secs(5));
    let mut writer = SessionContext::default();
    writer.connection_id = 7162;
    let writes = [
        "UPDATE t SET id=4,v=40 WHERE id=1",
        "INSERT INTO t VALUES(5,50,'e')",
        "DELETE FROM t WHERE id=2",
    ]
    .map(|sql| engine.execute_with_session(&mut writer, sql));
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "partition primary build failed: {result:?}");
    for write in writes {
        write.unwrap();
    }
    result.unwrap();
    assert_online_index_matches_heap(&engine, "PRIMARY");
    assert_online_index_matches_heap(&engine, "s_idx");
    engine.execute("CREATE TABLE ai(id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, UNIQUE KEY uid(id), v INT)").unwrap();
    engine.execute("INSERT INTO ai(id,v) VALUES(30,1)").unwrap();
    engine
        .execute("ALTER TABLE ai ADD PRIMARY KEY(id), LOCK=NONE")
        .unwrap();
    engine.execute("INSERT INTO ai(v) VALUES(2)").unwrap();
    let QueryResponse::ResultSet(rows) = engine.execute("SELECT id FROM ai WHERE v=2").unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(rows.rows[0][0].to_display_string(), "31");
}

#[test]
fn online_primary_key_cleanup_after_cancel_and_log_exhaustion() {
    for cancel in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = std::sync::Arc::new(open_relaxed_test_engine(dir.path()));
        engine
            .execute("CREATE TABLE t(id INT NULL, v INT)")
            .unwrap();
        engine.execute("INSERT INTO t VALUES(1,10)").unwrap();
        let hook = engine
            .database()
            .install_online_index_validation_test_hook();
        let token = crate::engine::cancel::QueryCancelToken::noop();
        let worker_token = token.clone();
        let worker = engine.clone();
        let ddl = std::thread::spawn(move || {
            let mut session = SessionContext::default();
            session.connection_id = 7171;
            session.current_query_token = Some(worker_token);
            worker.execute_with_session(
                &mut session,
                "ALTER TABLE t ADD PRIMARY KEY(id), ADD INDEX v_idx(v), LOCK=NONE",
            )
        });
        let reached = hook.wait_until_reached(Duration::from_secs(5));
        if cancel {
            token.cancel_local();
        } else {
            engine
                .database()
                .exhaust_online_index_log_for_test("test.t");
        }
        let mut writer = SessionContext::default();
        writer.connection_id = 7172;
        let inserted = engine.execute_with_session(&mut writer, "INSERT INTO t VALUES(NULL,20)");
        hook.release();
        let result = ddl.join().unwrap();
        assert!(reached, "validation hook not reached: {result:?}");
        inserted.expect("candidate cancellation/log pressure must not cancel the business write");
        assert_eq!(result.unwrap_err().code(), if cancel { 1317 } else { 1037 });
        let table = engine.database().table("test.t").unwrap();
        assert!(
            table.pending_primary_key.is_none()
                && table.indexes.is_empty()
                && table.columns[0].nullable
        );
        engine.execute("DELETE FROM t WHERE id IS NULL").unwrap();
        engine
            .execute("ALTER TABLE t ADD PRIMARY KEY(id), LOCK=NONE")
            .unwrap();
        assert_online_index_matches_heap(&engine, "PRIMARY");
    }
}

#[test]
fn online_primary_key_allows_load_data_into_empty_source() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_strict_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT NULL, v INT)")
        .unwrap();
    let input = dir.path().join("rows.csv");
    std::fs::write(
        &input,
        (1..=4096).map(|i| format!("{i},{i}\n")).collect::<String>(),
    )
    .unwrap();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 7181;
        worker.execute_with_session(&mut session, "ALTER TABLE t ADD PRIMARY KEY(id), LOCK=NONE")
    });
    let reached = hook.wait_until_reached(Duration::from_secs(5));
    let mut writer = SessionContext::default();
    writer.connection_id = 7182;
    let result = engine.execute_with_session(&mut writer,
        &format!("LOAD DATA /*+ PARALLEL(2) */ INFILE '{}' INTO TABLE t FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'", crate::util::file::sql_path(&input)));
    hook.release();
    let built = ddl.join().unwrap();
    assert!(reached, "build hook missing: {built:?}");
    result.unwrap();
    built.unwrap();
    assert_online_index_matches_heap(&engine, "PRIMARY");
}

#[test]
fn online_primary_key_identity_is_not_reused_after_drop_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let old_id;
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE t(id INT NULL, v INT)")
            .unwrap();
        engine.execute("INSERT INTO t VALUES(1,10)").unwrap();
        engine.execute("ALTER TABLE t ADD PRIMARY KEY(id)").unwrap();
        let table = engine.database().table("test.t").unwrap();
        old_id = table.indexes.iter().find(|i| i.primary).unwrap().index_id;
        engine.execute("ALTER TABLE t DROP PRIMARY KEY").unwrap();
        let path = crate::storage::naming::index_object_path_by_id(
            dir.path(),
            table.id,
            table.active_generation,
            old_id,
            "idx",
        );
        assert!(
            !path.exists(),
            "DROP PRIMARY KEY did not remove its actual physical identity"
        );
    }
    let engine = open_strict_cold_metadata_engine(dir.path());
    engine.execute("ALTER TABLE t ADD PRIMARY KEY(id)").unwrap();
    let table = engine.database().table("test.t").unwrap();
    assert!(table.indexes.iter().find(|i| i.primary).unwrap().index_id > old_id);
    assert_online_index_matches_heap(&engine, "PRIMARY");
}

#[test]
fn online_primary_key_type_shapes_and_blocking_failure_atomicity() {
    for (definition, rows, key) in [
        (
            "id BIGINT UNSIGNED, v DECIMAL(30,4)",
            "(18446744073709551615,123456789012345678901.2345),(0,-1.2500)",
            "id,v",
        ),
        (
            "id VARCHAR(80) COLLATE utf8mb4_unicode_ci, v INT",
            "('中文甲',1),('中文乙',2)",
            "id(3) DESC,v",
        ),
        (
            "id VARBINARY(30), v INT",
            "(X'000001',1),(X'00FF01',2)",
            "id,v",
        ),
        (
            "a INT, id INT GENERATED ALWAYS AS (a*2) STORED, v INT",
            "(1,DEFAULT,10),(2,DEFAULT,20)",
            "id",
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_relaxed_test_engine(dir.path());
        engine
            .execute(&format!("CREATE TABLE t({definition})"))
            .unwrap();
        engine
            .execute(&format!("INSERT INTO t VALUES {rows}"))
            .unwrap();
        engine
            .execute(&format!("ALTER TABLE t ADD PRIMARY KEY({key}), LOCK=NONE"))
            .unwrap_or_else(|e| panic!("{definition}: {e:?}"));
        assert_online_index_matches_heap(&engine, "PRIMARY");
    }
    for mode in ["SHARED", "EXCLUSIVE"] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_relaxed_test_engine(dir.path());
        engine
            .execute("CREATE TABLE t(id INT NULL, v INT)")
            .unwrap();
        engine.execute("INSERT INTO t VALUES(NULL,10)").unwrap();
        assert_eq!(
            engine
                .execute(&format!("ALTER TABLE t ADD PRIMARY KEY(id), LOCK={mode}"))
                .unwrap_err()
                .code(),
            1138
        );
        let table = engine.database().table("test.t").unwrap();
        assert!(
            table.primary_key.is_empty()
                && table.columns[0].nullable
                && table.pending_primary_key.is_none()
        );
    }
}

#[test]
fn online_primary_key_xa_resolution_and_existing_constraints() {
    for commit in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = std::sync::Arc::new(open_relaxed_test_engine(dir.path()));
        engine
            .execute("CREATE TABLE parent(id INT PRIMARY KEY)")
            .unwrap();
        engine.execute("INSERT INTO parent VALUES(1),(2)").unwrap();
        engine.execute("CREATE TABLE t(id INT NULL, v INT UNIQUE, p INT, FOREIGN KEY(p) REFERENCES parent(id) ON UPDATE CASCADE)").unwrap();
        engine
            .execute("INSERT INTO t VALUES(1,10,1),(2,20,2)")
            .unwrap();
        let hook = engine.database().install_ddl_backfill_test_hook();
        let worker = engine.clone();
        let ddl = std::thread::spawn(move || {
            let mut session = SessionContext::default();
            session.connection_id = 7201;
            worker
                .execute_with_session(&mut session, "ALTER TABLE t ADD PRIMARY KEY(id), LOCK=NONE")
        });
        let reached = hook.wait_until_reached(Duration::from_secs(5));
        let mut writer = SessionContext::default();
        writer.connection_id = 7202;
        let changed = (|| {
            engine.execute_with_session(&mut writer, "REPLACE INTO t VALUES(3,10,1)")?;
            engine.execute_with_session(
                &mut writer,
                "INSERT INTO t VALUES(4,20,2) ON DUPLICATE KEY UPDATE id=4",
            )?;
            engine.execute_with_session(&mut writer, "UPDATE parent SET id=5 WHERE id=1")?;
            engine.execute_with_session(&mut writer, "XA START 'online_pk'")?;
            engine.execute_with_session(&mut writer, "INSERT INTO t VALUES(6,60,2)")?;
            engine.execute_with_session(&mut writer, "XA END 'online_pk'")?;
            engine.execute_with_session(&mut writer, "XA PREPARE 'online_pk'")?;
            Ok::<_, QueryError>(())
        })();
        hook.release();
        let resolved = engine.execute_with_session(
            &mut writer,
            if commit {
                "XA COMMIT 'online_pk'"
            } else {
                "XA ROLLBACK 'online_pk'"
            },
        );
        let result = ddl.join().unwrap();
        assert!(reached, "build hook missing: {result:?}");
        changed.unwrap();
        resolved.unwrap();
        result.unwrap();
        assert_online_index_matches_heap(&engine, "PRIMARY");
        assert_eq!(
            engine
                .execute("ALTER TABLE t ADD PRIMARY KEY(v)")
                .unwrap_err()
                .code(),
            1068
        );
        assert_eq!(
            engine
                .execute("INSERT INTO t VALUES(7,70,99)")
                .unwrap_err()
                .code(),
            1452
        );
    }
}

#[test]
fn online_primary_key_rejects_unsupported_mixed_none() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t(id INT, v INT)").unwrap();
    engine.execute("INSERT INTO t VALUES(1,10)").unwrap();
    for sql in [
        "ALTER TABLE t ADD PRIMARY KEY(id), ADD COLUMN extra INT, LOCK=NONE",
        "ALTER TABLE t ADD PRIMARY KEY(id), ALGORITHM=INSTANT",
        "ALTER TABLE t ADD PRIMARY KEY(id), ALGORITHM=COPY, LOCK=NONE",
    ] {
        assert_eq!(engine.execute(sql).unwrap_err().code(), 1846, "{sql}");
        assert!(engine
            .database()
            .table("test.t")
            .unwrap()
            .primary_key
            .is_empty());
    }
}

fn copy_online_primary_data_dir(source: &std::path::Path, target: &std::path::Path) {
    std::fs::create_dir_all(target).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let to = target.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_online_primary_data_dir(&entry.path(), &to);
        } else {
            std::fs::copy(entry.path(), to).unwrap();
        }
    }
}

#[test]
fn online_primary_key_recovery_discards_durable_pending_group() {
    let dir = tempfile::tempdir().unwrap();
    let recovered_dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_strict_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT NULL, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,10),(2,20)").unwrap();
    let hook = engine
        .database()
        .install_online_index_validation_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 7191;
        worker.execute_with_session(
            &mut session,
            "ALTER TABLE t ADD PRIMARY KEY(id), ADD INDEX v_idx(v), LOCK=NONE",
        )
    });
    let reached = hook.wait_until_reached(Duration::from_secs(5));
    let candidate = engine.database().table("test.t").unwrap();
    copy_online_primary_data_dir(dir.path(), recovered_dir.path());
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "missing validation hook: {result:?}");
    result.unwrap();
    let recovered = open_strict_cold_metadata_engine(recovered_dir.path());
    let table = recovered.database().table("test.t").unwrap();
    assert!(
        table.primary_key.is_empty()
            && table.pending_primary_key.is_none()
            && table.columns[0].nullable
    );
    assert!(table.indexes.is_empty());
    for index in &candidate.indexes {
        let path = crate::storage::naming::index_object_path_by_id(
            recovered_dir.path(),
            table.id,
            table.active_generation,
            index.index_id,
            "idx",
        );
        assert!(!path.exists(), "pending physical index survived recovery");
    }
    recovered.execute("INSERT INTO t VALUES(NULL,30)").unwrap();
    drop(recovered);
    let recovered = open_strict_cold_metadata_engine(recovered_dir.path());
    assert!(recovered
        .database()
        .table("test.t")
        .unwrap()
        .pending_primary_key
        .is_none());
}

#[test]
fn online_primary_key_replica_recovery_materializes_received_publication() {
    let dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT NULL, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,10),(2,20),(3,30)")
        .unwrap();
    let stale = engine.database().table("test.t").unwrap();
    engine
        .execute("ALTER TABLE t ADD PRIMARY KEY(id), LOCK=NONE")
        .unwrap();
    let txn = engine.database().begin_txn().unwrap();
    let err = engine
        .database()
        .insert_bound_primary_test_row(txn, &stale, vec![Value::Null, Value::Int(40)])
        .unwrap_err();
    assert_eq!(err.code(), 1412);
    engine.database().rollback_txn(txn.txn_id).unwrap();
    copy_online_primary_data_dir(dir.path(), replica_dir.path());
    let table = engine.database().table("test.t").unwrap();
    let primary = table.indexes.iter().find(|i| i.primary).unwrap();
    let path = crate::storage::naming::index_object_path_by_id(
        replica_dir.path(),
        table.id,
        table.active_generation,
        primary.index_id,
        "idx",
    );
    std::fs::remove_file(path).unwrap();
    let mut role = crate::repl::role::RoleState::new_standalone();
    role.demote_to_replica("127.0.0.1:1".into());
    crate::repl::role::save(replica_dir.path(), &role).unwrap();
    let replica = open_strict_cold_metadata_engine(replica_dir.path());
    assert_online_index_matches_heap(&replica, "PRIMARY");
    let QueryResponse::ResultSet(rows) = replica
        .execute("SELECT id FROM t FORCE INDEX(PRIMARY) ORDER BY id")
        .unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ]
    );
}

fn assert_online_index_matches_heap(engine: &StorageQueryEngine, name: &str) {
    let db = engine.database();
    let table = db.table("test.t").unwrap();
    let index = table.indexes.iter().find(|i| i.name == name).unwrap();
    let txn = db.begin_read_only_txn().unwrap();
    let rows = db.scan_rows("test.t", txn).unwrap();
    db.rollback_txn(txn.txn_id).unwrap();
    let mut expected = rows
        .into_iter()
        .flat_map(|row| {
            table
                .index_keys(index, &row.values)
                .unwrap()
                .into_iter()
                .map(move |key| (key, row.row_id))
        })
        .collect::<Vec<_>>();
    expected.sort();
    let mut actual = db
        .range_index_bounds_entries(&table, index, None, None)
        .unwrap();
    actual.sort();
    assert_eq!(
        actual, expected,
        "physical index {name} differs from committed heap"
    );
}

#[test]
fn default_create_index_allows_dml_during_build() {
    assert_create_index_allows_dml_during_build("CREATE INDEX v_idx ON t (v)");
}

#[test]
fn create_index_explicit_lock_none_allows_dml_during_build() {
    assert_create_index_allows_dml_during_build(
        "CREATE INDEX v_idx ON t (v) ALGORITHM=INPLACE LOCK=NONE",
    );
}

fn assert_create_index_allows_dml_during_build(sql: &'static str) {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let ddl_engine = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 101;
        ddl_engine.execute_with_session(&mut session, sql)
    });
    let reached = hook.wait_until_reached(Duration::from_secs(5));
    if !reached {
        hook.release();
        let result = ddl.join().unwrap();
        panic!("index build did not reach pause point: {result:?}");
    }
    let mut writer = SessionContext::default();
    writer.connection_id = 102;
    // Storage callers bypass SQL MDL. The build must retain IS object
    // protection so a raw TRUNCATE cannot replace the heap underneath it.
    let raw_ddl = engine.database().truncate_table("test.t");
    let results = [
        "INSERT INTO t VALUES (3, 30)",
        "UPDATE t SET v = 11 WHERE id = 1",
        "DELETE FROM t WHERE id = 2",
    ]
    .map(|sql| engine.execute_with_session(&mut writer, sql));
    // Always release the builder, including on the unfixed blocking path.
    hook.release();
    ddl.join().unwrap().unwrap();
    assert_eq!(raw_ddl.unwrap_err().code(), 1205);
    for result in results {
        result.expect("DML must complete while CREATE INDEX is paused in its build phase");
    }
    let QueryResponse::ResultSet(indexed) = engine
        .execute("SELECT id, v FROM t FORCE INDEX (v_idx) WHERE v >= 0 ORDER BY id")
        .unwrap()
    else {
        panic!("expected rows")
    };
    assert_eq!(
        indexed.rows,
        vec![
            vec![Value::Int(1), Value::Int(11)],
            vec![Value::Int(3), Value::Int(30)],
        ]
    );
    assert_online_index_matches_heap(&engine, "v_idx");
}

#[test]
fn create_index_explicit_shared_and_exclusive_locks_are_honored() {
    for mode in ["SHARED", "EXCLUSIVE"] {
        let dir = tempfile::tempdir().unwrap();
        let engine = std::sync::Arc::new(open_test_engine(dir.path()));
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
            .unwrap();
        engine.execute("INSERT INTO t VALUES(1,10)").unwrap();
        let hook = engine.database().install_ddl_backfill_test_hook();
        let ddl_engine = engine.clone();
        let ddl = std::thread::spawn(move || {
            let mut session = SessionContext::default();
            session.connection_id = 601;
            ddl_engine.execute_with_session(
                &mut session,
                &format!("CREATE INDEX v_idx ON t(v) LOCK={mode}"),
            )
        });
        let reached = hook.wait_until_reached(Duration::from_secs(5));
        let mut writer = SessionContext::default();
        writer.connection_id = 602;
        let insert = engine.execute_with_session(&mut writer, "INSERT INTO t VALUES(2,20)");
        let read = engine.execute_with_session(&mut writer, "SELECT v FROM t WHERE id=1");
        hook.release();
        let result = ddl.join().unwrap();
        assert!(reached, "did not reach blocking build: {result:?}");
        result.unwrap();
        assert_eq!(insert.unwrap_err().code(), 1205);
        if mode == "SHARED" {
            read.unwrap();
        } else {
            assert_eq!(read.unwrap_err().code(), 1205);
        }
    }
}

#[test]
fn create_index_rejects_unsupported_algorithm_and_non_btree_lock_none() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t(id INT, s TEXT)").unwrap();
    for sql in [
        "CREATE INDEX idx ON t(id) ALGORITHM=INSTANT",
        "CREATE INDEX idx ON t(id) ALGORITHM=COPY LOCK=NONE",
        "CREATE FULLTEXT INDEX idx ON t(s) LOCK=NONE",
    ] {
        assert_eq!(engine.execute(sql).unwrap_err().code(), 1846, "{sql}");
    }
    assert!(engine
        .database()
        .table("test.t")
        .unwrap()
        .indexes
        .is_empty());
}

#[test]
fn online_btree_build_uses_active_heap_generation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_relaxed_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,10)").unwrap();
    engine
        .execute("ALTER TABLE t MODIFY COLUMN v BIGINT")
        .unwrap();
    engine
        .execute("CREATE INDEX v_idx ON t(v) LOCK=NONE")
        .unwrap();
    let table = engine.database().table("test.t").unwrap();
    assert!(table.active_generation > 1);
    let index = table.indexes.iter().find(|i| i.name == "v_idx").unwrap();
    let path = |generation| {
        crate::storage::naming::index_object_path_by_id(
            dir.path(),
            table.id,
            generation,
            index.index_id,
            "idx",
        )
    };
    assert!(path(table.active_generation).exists());
    assert!(
        !path(1).exists(),
        "online prepare leaked an index in the old generation"
    );
    assert_online_index_matches_heap(&engine, "v_idx");
}

#[test]
fn online_btree_build_replays_transactions_and_rollback_after_scan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_strict_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, s VARCHAR(40))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10, 'Alpha'), (2, 20, 'Beta')")
        .unwrap();
    let build = engine.database().install_ddl_backfill_test_hook();
    let replay = engine.database().install_online_index_replay_test_hook();
    let ddl_engine = engine.clone();
    let ddl =
        std::thread::spawn(move || {
            let mut session = SessionContext::default();
            session.connection_id = 201;
            ddl_engine.execute_with_session(&mut session,
            "ALTER TABLE t ADD INDEX v_idx (v DESC, s(3)), ADD INDEX s_idx ((LOWER(s))), LOCK=NONE")
        });
    let reached = build.wait_until_reached(Duration::from_secs(5));
    if !reached {
        build.release();
        replay.release();
        panic!(
            "builder did not reach prepare hook: {:?}",
            ddl.join().unwrap()
        );
    }
    let mut writer = SessionContext::default();
    writer.connection_id = 202;
    let changed = (|| {
        engine.execute_with_session(
            &mut writer,
            "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
        )?;
        engine.execute_with_session(&mut writer, "BEGIN")?;
        engine.execute_with_session(&mut writer, "UPDATE t SET v=99, s='Changed' WHERE id=1")?;
        engine.execute_with_session(&mut writer, "DELETE FROM t WHERE id=2")?;
        engine.execute_with_session(&mut writer, "INSERT INTO t VALUES (3, 30, 'Uncommitted')")?;
        Ok::<_, QueryError>(())
    })();
    build.release();
    let scanned = replay.wait_until_reached(Duration::from_secs(5));
    let rollback = engine.execute_with_session(&mut writer, "ROLLBACK");
    let committed = (|| {
        engine.execute_with_session(&mut writer, "BEGIN")?;
        engine.execute_with_session(&mut writer, "INSERT INTO t VALUES (4, 40, 'Delta')")?;
        engine.execute_with_session(&mut writer, "SAVEPOINT s")?;
        engine.execute_with_session(&mut writer, "UPDATE t SET v=77, s='Reverted' WHERE id=4")?;
        engine.execute_with_session(&mut writer, "ROLLBACK TO SAVEPOINT s")?;
        engine.execute_with_session(&mut writer, "DELETE FROM t WHERE id=1")?;
        engine.execute_with_session(&mut writer, "COMMIT")?;
        Ok::<_, QueryError>(())
    })();
    replay.release();
    let result = ddl.join().unwrap();
    changed.unwrap();
    rollback.unwrap();
    committed.unwrap();
    assert!(scanned, "build did not reach replay hook: {result:?}");
    result.unwrap();
    for name in ["v_idx", "s_idx"] {
        assert_online_index_matches_heap(&engine, name);
        let QueryResponse::ResultSet(rows) = engine
            .execute(&format!(
                "SELECT id,v,s FROM t FORCE INDEX ({name}) ORDER BY id"
            ))
            .unwrap()
        else {
            panic!("expected rows")
        };
        assert_eq!(
            rows.rows,
            vec![
                vec![Value::Int(2), Value::Int(20), Value::String("Beta".into())],
                vec![Value::Int(4), Value::Int(40), Value::String("Delta".into())],
            ]
        );
    }
    drop(engine);
    let reopened = open_strict_cold_metadata_engine(dir.path());
    assert_online_index_matches_heap(&reopened, "v_idx");
    assert_online_index_matches_heap(&reopened, "s_idx");
    let QueryResponse::ResultSet(rows) = reopened
        .execute("SELECT id FROM t FORCE INDEX (v_idx) WHERE v=40")
        .unwrap()
    else {
        panic!("expected rows")
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(4)]]);
}

#[test]
fn online_unique_btree_conflict_aborts_ddl_without_rejecting_dml() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1,10)").unwrap();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let ddl_engine = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 301;
        ddl_engine.execute_with_session(&mut session, "CREATE UNIQUE INDEX v_idx ON t(v)")
    });
    let reached = hook.wait_until_reached(Duration::from_secs(5));
    let mut writer = SessionContext::default();
    writer.connection_id = 302;
    let inserted = engine.execute_with_session(
        &mut writer,
        "INSERT INTO t VALUES (2,10), (3,NULL), (4,NULL)",
    );
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "builder did not reach hook: {result:?}");
    inserted.expect("candidate uniqueness must not reject business DML");
    assert_eq!(result.unwrap_err().code(), 1062);
    assert!(!engine
        .database()
        .table("test.t")
        .unwrap()
        .indexes
        .iter()
        .any(|i| i.name == "v_idx"));
    engine.execute("DELETE FROM t WHERE id=2").unwrap();
    engine.execute("CREATE UNIQUE INDEX v_idx ON t(v)").unwrap();
    assert_eq!(
        engine
            .execute("INSERT INTO t VALUES (5,10)")
            .unwrap_err()
            .code(),
        1062
    );
}

#[test]
fn online_btree_partition_move_during_build() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_relaxed_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT) PARTITION BY HASH(id) PARTITIONS 2")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,10), (2,20), (3,30)")
        .unwrap();
    let hook = engine.database().install_online_index_replay_test_hook();
    let ddl_engine = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 401;
        ddl_engine.execute_with_session(&mut session, "CREATE INDEX v_idx ON t(v)")
    });
    let reached = hook.wait_until_reached(Duration::from_secs(5));
    let mut writer = SessionContext::default();
    writer.connection_id = 402;
    let writes = [
        "UPDATE t SET id=4, v=40 WHERE id=1",
        "DELETE FROM t WHERE id=2",
        "INSERT INTO t VALUES (5,50), (6,60)",
    ]
    .map(|sql| engine.execute_with_session(&mut writer, sql));
    hook.release();
    let result = ddl.join().unwrap();
    assert!(
        reached,
        "partition build did not reach replay hook: {result:?}"
    );
    for write in writes {
        write.unwrap();
    }
    result.unwrap();
    for (id, v) in [(3, 30), (4, 40), (5, 50), (6, 60)] {
        let QueryResponse::ResultSet(rows) = engine
            .execute(&format!("SELECT id FROM t FORCE INDEX(v_idx) WHERE v={v}"))
            .unwrap()
        else {
            panic!("expected rows")
        };
        assert_eq!(rows.rows, vec![vec![Value::Int(id)]]);
    }
}

#[test]
fn online_btree_publish_waits_for_writer_without_blocking_new_dml() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1,10)").unwrap();
    let hook = engine.database().install_online_index_replay_test_hook();
    let (send, recv) = std::sync::mpsc::channel();
    let ddl_engine = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 501;
        let result = ddl_engine.execute_with_session(&mut session, "CREATE INDEX v_idx ON t(v)");
        send.send(result).unwrap();
    });
    let reached = hook.wait_until_reached(Duration::from_secs(5));
    let mut writer = SessionContext::default();
    writer.connection_id = 502;
    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    let changed = engine.execute_with_session(&mut writer, "UPDATE t SET v=99 WHERE id=1");
    hook.release();
    let early = recv.recv_timeout(Duration::from_millis(40));
    let mut other = SessionContext::default();
    other.connection_id = 503;
    let inserted = engine.execute_with_session(&mut other, "INSERT INTO t VALUES (2,20)");
    let rollback = engine.execute_with_session(&mut writer, "ROLLBACK");
    let result = match early {
        Ok(result) => {
            ddl.join().unwrap();
            panic!("DDL published before writer finished: {result:?}")
        }
        Err(_) => recv.recv_timeout(Duration::from_secs(5)).unwrap(),
    };
    ddl.join().unwrap();
    assert!(reached);
    changed.unwrap();
    inserted.unwrap();
    rollback.unwrap();
    result.unwrap();
    assert_online_index_matches_heap(&engine, "v_idx");
}

fn insert_t_values(engine: &StorageQueryEngine, start: i64, end: i64) {
    for batch_start in (start..=end).step_by(500) {
        let batch_end = (batch_start + 499).min(end);
        let values = (batch_start..=batch_end)
            .map(|i| format!("({i}, {})", i * 10))
            .collect::<Vec<_>>()
            .join(", ");
        engine
            .execute(&format!("INSERT INTO t VALUES {values}"))
            .unwrap();
    }
}

fn engine_status_u64(engine: &StorageQueryEngine, metric: &str) -> u64 {
    let QueryResponse::ResultSet(result) = engine.execute("SHOW ENGINE MYDB STATUS").unwrap()
    else {
        panic!("expected SHOW ENGINE result set");
    };
    result
        .rows
        .iter()
        .find(|row| row[1].to_display_string() == metric)
        .and_then(|row| row[2].to_display_string().parse().ok())
        .unwrap_or(0)
}

fn strict_online_ddl_config() -> StorageConfig {
    StorageConfig {
        durability: crate::storage::DurabilityMode::Strict,
        checkpoint_interval: Duration::from_millis(60_000),
        checkpoint_wal_bytes: 64 * 1024 * 1024,
        lock_wait: Duration::from_millis(1_000),
        group_commit_window: Duration::from_micros(0),
        wal_segment_bytes: 64 * 1024 * 1024,
        buffer_pool_pages: 2_048,
        buffer_pool_io_threads: 1,
        buffer_pool_prefetch_pages: 2,
        undo_retention: Duration::from_millis(60_000),
        heap_update_reserve_bytes: 2048,
        startup_validation: crate::storage::StartupValidationMode::Auto,
        startup_validate_indexes: true,
    }
}

/// INSTANT ADD COLUMN: pre-existing rows must read back the new
/// column's DEFAULT without any heap rewrite. The legacy implementation
/// scanned the entire heap in line; the new path only mutates the
/// catalog, so this test indirectly verifies that read-side default
/// backfill works (`Database::pad_full_row_to_schema`).
#[test]
fn instant_add_column_returns_default_for_existing_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    engine
        .execute("ALTER TABLE t ADD COLUMN status INT DEFAULT 7")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id, v, status FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(1), Value::Int(10), Value::Int(7)],
            vec![Value::Int(2), Value::Int(20), Value::Int(7)],
            vec![Value::Int(3), Value::Int(30), Value::Int(7)],
        ],
        "pre-existing rows must observe the INSTANT-added DEFAULT"
    );
}

/// INSTANT ADD COLUMN with a NULLable column lacking a DEFAULT must
/// produce NULL (not error) when projecting pre-existing rows.
#[test]
fn instant_add_column_nullable_returns_null_for_existing_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    engine
        .execute("ALTER TABLE t ADD COLUMN note VARCHAR(20)")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id, v, note FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![vec![Value::Int(1), Value::Int(10), Value::Null]]
    );
}

/// INSTANT ADD COLUMN must reject NOT NULL without DEFAULT — the
/// pre-existing rows would otherwise read back NULL through the
/// default-backfill path, violating the constraint.
#[test]
fn instant_add_column_not_null_backfills_implicit_default() {
    // MySQL: ADD COLUMN NOT NULL without DEFAULT is allowed and backfills
    // existing rows with the type's implicit default (INT→0). Only types
    // with no implicit default (JSON/GEOMETRY/VECTOR) still error 1364.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    engine
        .execute("ALTER TABLE t ADD COLUMN required INT NOT NULL")
        .expect("ADD COLUMN NOT NULL without DEFAULT backfills implicit default");
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT required FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(0));

    // JSON has no implicit default → still 1364.
    let err = engine
        .execute("ALTER TABLE t ADD COLUMN j JSON NOT NULL")
        .expect_err("JSON NOT NULL without DEFAULT must still fail");
    assert_eq!(err.code(), 1364);
}

/// After INSTANT ADD COLUMN, INSERTs that omit the new column should
/// pick up its DEFAULT, producing a full-length stored row that the
/// read path returns as-is (no backfill needed).
#[test]
fn insert_after_instant_add_column_uses_default() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    engine
        .execute("ALTER TABLE t ADD COLUMN status INT DEFAULT 99")
        .unwrap();

    // INSERT with explicit columns omits `status` — DEFAULT applies.
    engine
        .execute("INSERT INTO t (id, v) VALUES (2, 20)")
        .unwrap();
    // INSERT with all columns specified — explicit value wins.
    engine
        .execute("INSERT INTO t (id, v, status) VALUES (3, 30, 11)")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id, v, status FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(1), Value::Int(10), Value::Int(99)], // pre-DDL row
            vec![Value::Int(2), Value::Int(20), Value::Int(99)], // post-DDL, default
            vec![Value::Int(3), Value::Int(30), Value::Int(11)], // post-DDL, explicit
        ]
    );
}

/// UPDATE on a pre-DDL row should naturally migrate it to the
/// full-length encoding. Verified indirectly: after the UPDATE, the
/// row reads back the explicit value, and other rows still observe
/// the DEFAULT through the backfill path.
#[test]
fn update_after_instant_add_column_migrates_row() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();
    engine
        .execute("ALTER TABLE t ADD COLUMN status INT DEFAULT 5")
        .unwrap();

    engine
        .execute("UPDATE t SET status = 42 WHERE id = 1")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id, v, status FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(1), Value::Int(10), Value::Int(42)],
            vec![Value::Int(2), Value::Int(20), Value::Int(5)],
        ]
    );
}

/// Instant-added column should be filterable in a WHERE clause —
/// pre-existing rows match on the catalog DEFAULT.
#[test]
fn where_clause_against_instant_added_column_uses_default() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();
    engine
        .execute("ALTER TABLE t ADD COLUMN tier INT DEFAULT 3")
        .unwrap();
    engine
        .execute("INSERT INTO t (id, v, tier) VALUES (3, 30, 9)")
        .unwrap();

    let QueryResponse::ResultSet(default_rows) = engine
        .execute("SELECT id FROM t WHERE tier = 3 ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        default_rows.rows,
        vec![vec![Value::Int(1)], vec![Value::Int(2)]]
    );

    let QueryResponse::ResultSet(explicit_rows) =
        engine.execute("SELECT id FROM t WHERE tier = 9").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(explicit_rows.rows, vec![vec![Value::Int(3)]]);
}

/// Multiple back-to-back INSTANT ADD COLUMNs against a row that
/// pre-dates them should all read back as defaults.
#[test]
fn multiple_instant_add_columns_chain_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1)").unwrap();

    engine
        .execute("ALTER TABLE t ADD COLUMN a INT DEFAULT 10")
        .unwrap();
    engine
        .execute("ALTER TABLE t ADD COLUMN b INT DEFAULT 20")
        .unwrap();
    engine
        .execute("ALTER TABLE t ADD COLUMN c INT DEFAULT 30")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id, a, b, c FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![vec![
            Value::Int(1),
            Value::Int(10),
            Value::Int(20),
            Value::Int(30)
        ]]
    );
}

/// INSTANT DROP COLUMN — the column slot becomes a tombstone in the
/// catalog; the heap is not rewritten. SELECT * and SHOW COLUMNS must
/// hide the dropped column, and named references to it must fail
/// with "unknown column".
#[test]
fn instant_drop_column_hides_column_from_user_surfaces() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, status VARCHAR(20))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 100, 'a'), (2, 200, 'b')")
        .unwrap();

    engine.execute("ALTER TABLE t DROP COLUMN status").unwrap();

    // SELECT * width drops to 2 — tombstone is hidden.
    let QueryResponse::ResultSet(star) = engine.execute("SELECT * FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(star.columns.len(), 2);
    assert_eq!(star.rows[0], vec![Value::Int(1), Value::Int(100)]);
    assert_eq!(star.rows[1], vec![Value::Int(2), Value::Int(200)]);

    // Named reference to the dropped column must fail.
    let err = engine
        .execute("SELECT status FROM t")
        .expect_err("dropped column should not be referenceable");
    assert_eq!(err.code(), 1054);

    // SHOW COLUMNS reflects the post-drop schema.
    let QueryResponse::ResultSet(cols) = engine.execute("SHOW COLUMNS FROM t").unwrap() else {
        panic!("expected result set");
    };
    let names: Vec<&str> = cols
        .rows
        .iter()
        .filter_map(|r| match &r[0] {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(names, vec!["id", "v"]);
}

/// After INSTANT DROP COLUMN, INSERT-without-column-list expects the
/// reduced (visible) arity. Existing pre-DDL rows continue to read
/// their two visible columns correctly even though the heap row
/// physically still has three encoded slots.
#[test]
fn instant_drop_column_insert_uses_visible_arity() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, dead INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10, 999)").unwrap();
    engine.execute("ALTER TABLE t DROP COLUMN dead").unwrap();

    // Three-arg INSERT must now error: only two visible columns.
    let err = engine
        .execute("INSERT INTO t VALUES (2, 20, 888)")
        .expect_err("INSERT arity must match visible columns");
    assert_eq!(err.code(), 1136);

    // Two-arg INSERT works.
    engine.execute("INSERT INTO t VALUES (2, 20)").unwrap();

    let QueryResponse::ResultSet(rs) = engine.execute("SELECT * FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)],
        ]
    );
}

/// DROP + ADD same column name should re-allocate a fresh slot. Old
/// rows that still carry a value at the original physical_pos must
/// not bleed through as the new column's value — the new column is
/// physically distinct (next physical_pos) and reads back its DEFAULT
/// for pre-existing rows.
#[test]
fn instant_drop_then_add_same_name_uses_fresh_slot() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, status VARCHAR(20))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10, 'old-value')")
        .unwrap();

    engine.execute("ALTER TABLE t DROP COLUMN status").unwrap();
    engine
        .execute("ALTER TABLE t ADD COLUMN status VARCHAR(20) DEFAULT 'fresh'")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id, v, status FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![vec![
            Value::Int(1),
            Value::Int(10),
            Value::String("fresh".to_string())
        ]],
        "re-added column must read its own DEFAULT, not the dropped slot's stale value"
    );
}

/// INSTANT DROP COLUMN persists across restart: the catalog records
/// the tombstone and the next process recovers the same logical
/// schema.
#[test]
fn instant_drop_column_persists_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, dead INT)")
            .unwrap();
        engine
            .execute("INSERT INTO t VALUES (1, 10, 999), (2, 20, 888)")
            .unwrap();
        engine.execute("ALTER TABLE t DROP COLUMN dead").unwrap();
    }
    let engine = open_strict_test_engine(dir.path());
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT * FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.columns.len(), 2);
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)],
        ]
    );
}

/// INSTANT RENAME COLUMN — pure catalog rename. The same physical_pos
/// (and underlying heap slot) is now reachable under the new name.
#[test]
fn instant_rename_column_preserves_data() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();
    engine
        .execute("ALTER TABLE t RENAME COLUMN v TO val")
        .unwrap();

    // Old name no longer resolves.
    let err = engine
        .execute("SELECT v FROM t")
        .expect_err("renamed-away column should be unknown");
    assert_eq!(err.code(), 1054);

    // New name reads the same data.
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id, val FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)],
        ]
    );
}

/// INSTANT MODIFY COLUMN still handles metadata-only attribute changes,
/// while type changes that alter the heap codec now use the COPY rewrite.
#[test]
fn modify_column_type_change_uses_copy_rewrite() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v BIGINT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 100)").unwrap();

    // Binary-compatible MODIFY: same storage kind (BIGINT), only
    // tighten nullability. Existing rows must remain readable.
    engine
        .execute("ALTER TABLE t MODIFY COLUMN v BIGINT NOT NULL")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id, v FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1), Value::Int(100)]]);

    let QueryResponse::ResultSet(cols) = engine.execute("SHOW COLUMNS FROM t").unwrap() else {
        panic!("expected result set");
    };
    let v_row = cols
        .rows
        .iter()
        .find(|r| matches!(&r[0], Value::String(s) if s.eq_ignore_ascii_case("v")))
        .expect("v column row");
    assert!(matches!(&v_row[1], Value::String(s) if s.to_ascii_lowercase().contains("bigint")));

    engine.execute("ALTER TABLE t MODIFY COLUMN v INT").unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id, v FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1), Value::Int(100)]]);
}

#[test]
fn copy_modify_strict_failure_is_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "SET sql_mode='STRICT_TRANS_TABLES'")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(10))",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1, 'abcdef')")
        .unwrap();

    let err = engine
        .execute_with_session(&mut session, "ALTER TABLE t MODIFY COLUMN v VARCHAR(3)")
        .expect_err("strict COPY should fail before mutating storage");
    assert_eq!(err.code(), 1406);

    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (2, '12345')")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut session, "SELECT id, v FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(1), Value::String("abcdef".to_string())],
            vec![Value::Int(2), Value::String("12345".to_string())],
        ]
    );
}

#[test]
fn copy_modify_default_empty_sql_mode_is_strict() {
    // Regression for issue #60: the *default* (empty) sql_mode must behave
    // strict on ALTER TABLE narrowing, the same "empty = strict" posture
    // INSERT already uses — instead of silently truncating/clamping stored
    // data. Previously `is_strict_sql_mode()` treated empty sql_mode as
    // non-strict, so the COPY-rewrite went lossy and dropped data.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    // No SET sql_mode: the connection default is empty, which is strict.

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(10), n INT)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO t VALUES (1, 'helloworld', 100000)",
        )
        .unwrap();

    // String narrowing that would truncate → 1406, ALTER aborts.
    let err = engine
        .execute_with_session(&mut session, "ALTER TABLE t MODIFY COLUMN v VARCHAR(3)")
        .expect_err("narrowing VARCHAR under default sql_mode must error, not truncate");
    assert_eq!(err.code(), 1406);

    // Numeric narrowing that would overflow → 1264, ALTER aborts.
    let err = engine
        .execute_with_session(&mut session, "ALTER TABLE t MODIFY COLUMN n TINYINT")
        .expect_err("narrowing INT under default sql_mode must error, not clamp");
    assert_eq!(err.code(), 1264);

    // Original data is fully preserved.
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut session, "SELECT v, n FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![vec![
            Value::String("helloworld".to_string()),
            Value::Int(100000)
        ]]
    );
}

#[test]
fn copy_modify_non_strict_truncates_with_warning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "SET sql_mode='NO_ENGINE_SUBSTITUTION'")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(10))",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1, 'abcdef')")
        .unwrap();

    engine
        .execute_with_session(&mut session, "ALTER TABLE t MODIFY COLUMN v VARCHAR(3)")
        .unwrap();
    let QueryResponse::ResultSet(warnings) = engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
    else {
        panic!("expected warnings");
    };
    assert_eq!(warnings.rows.len(), 1);
    assert_eq!(warnings.rows[0][1], Value::Int(1265));

    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut session, "SELECT id, v FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![vec![Value::Int(1), Value::String("abc".to_string())]]
    );
}

#[test]
fn copy_modify_non_strict_workers_preserve_all_conversion_warnings() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET max_error_count=65535")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET sql_mode='NO_ENGINE_SUBSTITUTION'")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(10), b VARBINARY(10), n INT)",
        )
        .unwrap();
    let values = (0..300)
        .map(|id| format!("({id}, '中文字符', X'61626364', 1000)"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute_with_session(&mut session, &format!("INSERT INTO t VALUES {values}"))
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "ALTER TABLE t MODIFY v VARCHAR(2), MODIFY b VARBINARY(2), MODIFY n TINYINT",
        )
        .unwrap();
    let QueryResponse::ResultSet(warnings) = engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
    else {
        panic!("expected warnings");
    };
    // Exercise multiple worker batches and both string and numeric coercers.
    assert_eq!(warnings.rows.len(), 900);
    for (code, expected) in [(1265, 600), (1264, 300)] {
        assert_eq!(
            warnings
                .rows
                .iter()
                .filter(|row| row[1] == Value::Int(code))
                .count(),
            expected,
        );
    }
    let QueryResponse::ResultSet(rows) = engine
        .execute_with_session(&mut session, "SELECT v, b, n FROM t")
        .unwrap()
    else {
        panic!("expected rows");
    };
    assert_eq!(rows.rows.len(), 300);
    for row in rows.rows {
        assert_eq!(
            row,
            vec![
                Value::String("中文".into()),
                Value::Bytes(b"ab".to_vec()),
                Value::Int(127)
            ]
        );
    }
}

#[test]
fn copy_modify_non_strict_clamps_decimal_and_temporal_values() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "SET sql_mode='NO_ENGINE_SUBSTITUTION'")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id INT PRIMARY KEY, d DECIMAL(6,2), dt VARCHAR(20), y INT)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO t VALUES (1, 1234.56, 'not-a-date', 3000)",
        )
        .unwrap();

    engine
        .execute_with_session(&mut session, "ALTER TABLE t MODIFY COLUMN d DECIMAL(4,2)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "ALTER TABLE t MODIFY COLUMN dt DATETIME")
        .unwrap();
    engine
        .execute_with_session(&mut session, "ALTER TABLE t MODIFY COLUMN y YEAR")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut session, "SELECT d, dt, y FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0].to_display_string(), "99.99");
    assert_eq!(rs.rows[0][1], Value::DateTime(crate::types::ZERO_DATETIME));
    // Out-of-range YEAR (3000) stores 0000 in non-strict mode, matching
    // MySQL and INSERT's coercion single source — not a clamp to 2155.
    assert_eq!(rs.rows[0][2], Value::Year(0));
}

#[test]
fn copy_modify_non_strict_coerces_enum_set_but_rejects_lossy_vector() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "SET sql_mode='NO_ENGINE_SUBSTITUTION'")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id INT PRIMARY KEY, e VARCHAR(10), s VARCHAR(20), emb VECTOR(3))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO t VALUES (1, 'z', 'a,z,b', '[1,2,3]')",
        )
        .unwrap();

    engine
        .execute_with_session(&mut session, "ALTER TABLE t MODIFY COLUMN e ENUM('a','b')")
        .unwrap();
    engine
        .execute_with_session(&mut session, "ALTER TABLE t MODIFY COLUMN s SET('a','b')")
        .unwrap();
    let error = engine
        .execute_with_session(&mut session, "ALTER TABLE t MODIFY COLUMN emb VECTOR(2)")
        .unwrap_err();
    assert_eq!(error.code(), 50002);

    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut session, "SELECT e, s, VECTOR_TO_STRING(emb) FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows[0],
        vec![
            Value::Bytes(Vec::new()),
            Value::String("a,b".to_string()),
            Value::String("[1,2,3]".to_string()),
        ]
    );
}

#[test]
fn copy_modify_rebuilds_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, INDEX v_idx (v))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();
    engine
        .execute("ALTER TABLE t MODIFY COLUMN v BIGINT")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id FROM t WHERE v = 20").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(2)]]);
}

#[test]
fn copy_rewrite_clears_crash_marker_and_reopens() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 100)").unwrap();
    engine
        .execute("ALTER TABLE t MODIFY COLUMN v BIGINT")
        .unwrap();

    let marker_dir = dir.path().join("ddl").join("alter_copy");
    if marker_dir.exists() {
        let leftover = std::fs::read_dir(&marker_dir).unwrap().count();
        assert_eq!(leftover, 0, "COPY crash markers must be removed");
    }

    drop(engine);
    let reopened = open_strict_test_engine(dir.path());
    let QueryResponse::ResultSet(rs) = reopened.execute("SELECT id, v FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1), Value::Int(100)]]);
}

#[test]
fn streaming_copy_detects_converted_unique_duplicate_without_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(10), UNIQUE KEY uk_v (v))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, '01'), (2, '1')")
        .unwrap();
    let err = engine
        .execute("ALTER TABLE t MODIFY COLUMN v INT")
        .expect_err("converted UNIQUE keys must be checked in sorted build stream");
    assert_eq!(err.code(), 1062);

    let QueryResponse::ResultSet(rows) = engine.execute("SELECT id, v FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Int(1), Value::String("01".to_string())],
            vec![Value::Int(2), Value::String("1".to_string())],
        ]
    );
    let marker_dir = dir.path().join("ddl").join("alter_copy");
    if marker_dir.exists() {
        assert_eq!(std::fs::read_dir(marker_dir).unwrap().count(), 0);
    }
}

#[test]
fn streaming_copy_handles_heap_order_and_reopens_collated_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, KEY k_v (v))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (3, 30), (1, 10), (2, 20)")
        .unwrap();
    engine
        .execute("ALTER TABLE t MODIFY COLUMN v VARCHAR(10)")
        .unwrap();

    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT id, v FROM t FORCE INDEX (PRIMARY) ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Int(1), Value::String("10".into())],
            vec![Value::Int(2), Value::String("20".into())],
            vec![Value::Int(3), Value::String("30".into())],
        ]
    );
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT id FROM t FORCE INDEX (k_v) WHERE v = '20'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(2)]]);

    drop(engine);
    let reopened = open_strict_test_engine(dir.path());
    let QueryResponse::ResultSet(rows) = reopened
        .execute("SELECT id FROM t FORCE INDEX (k_v) WHERE v = '30'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(3)]]);
}

#[test]
fn startup_rejects_incomplete_copy_marker() {
    let dir = tempfile::tempdir().unwrap();
    let marker_dir = dir.path().join("ddl").join("alter_copy");
    std::fs::create_dir_all(&marker_dir).unwrap();
    std::fs::write(
        marker_dir.join("42.job"),
        b"version=1\nkind=ALTER_TABLE_COPY\n",
    )
    .unwrap();

    let err = match StorageQueryEngine::open_with_storage_config(
        dir.path(),
        strict_online_ddl_config(),
    ) {
        Ok(_) => panic!("startup must reject an incomplete ALTER TABLE COPY marker"),
        Err(err) => err,
    };
    assert_eq!(err.code(), 1105);
    assert!(err.message().contains("incomplete ALTER TABLE COPY job"));
}

/// INSTANT accepts encoding-compatible column changes that do not tighten
/// the existing value domain. Tightening is covered by ddl_hardening tests.
#[test]
fn algorithm_instant_accepts_column_ddl() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT NOT NULL)")
        .unwrap();
    // ADD COLUMN is INSTANT-eligible.
    engine
        .execute("ALTER TABLE t ALGORITHM=INSTANT, ADD COLUMN c INT DEFAULT 7")
        .unwrap();
    // DROP COLUMN is INSTANT-eligible (tombstone).
    engine
        .execute("ALTER TABLE t ALGORITHM=INSTANT, DROP COLUMN c")
        .unwrap();
    // RENAME COLUMN, MODIFY COLUMN, CHANGE COLUMN are all
    // catalog-only. MODIFY / CHANGE must keep the column's storage
    // shape (Phase B row codec dispatches on declared type; ONLINE
    // shadow-column rewrite isn't implemented yet), so the assertion
    // here uses same-type re-declarations.
    engine
        .execute("ALTER TABLE t ALGORITHM=INSTANT, RENAME COLUMN v TO val")
        .unwrap();
    engine
        .execute("ALTER TABLE t ALGORITHM=INSTANT, MODIFY COLUMN val INT NOT NULL")
        .unwrap();
    engine
        .execute("ALTER TABLE t ALGORITHM=INSTANT, CHANGE COLUMN val v INT")
        .unwrap();
}

/// `ALGORITHM=INSTANT` must reject ADD INDEX — the heap scan rules
/// out a catalog-only path. MySQL 8.0 returns
/// ER_ALTER_OPERATION_NOT_SUPPORTED_REASON (1846 / 0A000) for this.
#[test]
fn algorithm_instant_rejects_add_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let err = engine
        .execute("ALTER TABLE t ALGORITHM=INSTANT, ADD INDEX v_idx (v)")
        .expect_err("ALGORITHM=INSTANT cannot ADD INDEX");
    assert_eq!(err.code(), 1846);
    assert_eq!(err.sql_state(), "0A000");
}

#[test]
fn algorithm_instant_and_inplace_reject_copy_required_modify() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 7)").unwrap();

    let err = engine
        .execute("ALTER TABLE t ALGORITHM=INSTANT, MODIFY COLUMN v BIGINT")
        .expect_err("INSTANT cannot perform a type-changing rewrite");
    assert_eq!(err.code(), 1846);
    assert_eq!(err.sql_state(), "0A000");

    let err = engine
        .execute("ALTER TABLE t ALGORITHM=INPLACE, MODIFY COLUMN v BIGINT")
        .expect_err("INPLACE cannot perform a shadow-table COPY rewrite");
    assert_eq!(err.code(), 1846);
}

/// `ALGORITHM=INPLACE` accepts everything mydb can do (no shadow
/// table required).
#[test]
fn algorithm_inplace_accepts_all_supported_ops() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 100)").unwrap();
    engine
        .execute("ALTER TABLE t ALGORITHM=INPLACE, ADD COLUMN c INT DEFAULT 0")
        .unwrap();
    engine
        .execute("ALTER TABLE t ALGORITHM=INPLACE, ADD INDEX v_idx (v)")
        .unwrap();
}

/// `ALGORITHM=COPY` is accepted. Metadata-only actions may still run
/// through the cheaper instant path.
#[test]
fn algorithm_copy_is_accepted_for_column_ddl() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("ALTER TABLE t ALGORITHM=COPY, ADD COLUMN c INT")
        .unwrap();
}

#[test]
fn alter_table_lock_clause_is_validated() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();

    engine
        .execute("ALTER TABLE t LOCK=EXCLUSIVE, ADD COLUMN c INT")
        .unwrap();

    // With no explicit ALGORITHM, MySQL may select INPLACE to honour a
    // non-default LOCK request even when the operation is also
    // INSTANT-capable.
    engine
        .execute("ALTER TABLE t LOCK=NONE, ADD COLUMN d INT")
        .expect("LOCK=NONE is honoured for an ADD COLUMN");
    engine
        .execute("ALTER TABLE t LOCK=SHARED, ADD COLUMN e INT")
        .expect("LOCK=SHARED is honoured for an ADD COLUMN");
    // ADD INDEX (INPLACE, no row rewrite) likewise accepts LOCK=NONE.
    engine
        .execute("ALTER TABLE t LOCK=NONE, ADD INDEX idx_d (d)")
        .expect("LOCK=NONE is honoured for an INPLACE ADD INDEX");

    // MySQL 8.0.46 rejects every explicit INSTANT + non-DEFAULT LOCK
    // combination with ER_WRONG_USAGE (1221), rather than accepting the
    // LOCK clause as an inert promise.
    for lock in ["NONE", "SHARED", "EXCLUSIVE"] {
        let err = engine
            .execute(&format!(
                "ALTER TABLE t ALGORITHM=INSTANT, LOCK={lock}, ADD COLUMN bad_{lock} INT"
            ))
            .expect_err("explicit INSTANT only permits LOCK=DEFAULT");
        assert_eq!(err.code(), 1221, "LOCK={lock}: {}", err.message());
        assert_eq!(err.sql_state(), "HY000", "LOCK={lock}");
        assert_eq!(
            err.message(),
            "Incorrect usage of ALGORITHM=INSTANT and LOCK=NONE/SHARED/EXCLUSIVE"
        );
    }
}

#[test]
fn copy_rewrite_multi_action_applies_all_actions() {
    // Mixed multiaction COPY is supported by the MySQL table-DDL compatibility work;
    // actions execute sequentially, consistent with existing ALTER semantics.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 7)").unwrap();

    engine
        .execute("ALTER TABLE t ADD COLUMN c INT, MODIFY COLUMN v BIGINT")
        .expect("multi-action ALTER mixing a COPY rewrite is supported now");

    let QueryResponse::ResultSet(cols) = engine.execute("SHOW COLUMNS FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert!(cols
        .rows
        .iter()
        .any(|r| matches!(&r[0], Value::String(s) if s.eq_ignore_ascii_case("c"))));
    let v_row = cols
        .rows
        .iter()
        .find(|r| matches!(&r[0], Value::String(s) if s.eq_ignore_ascii_case("v")))
        .expect("v column row");
    assert!(matches!(&v_row[1], Value::String(s) if s.eq_ignore_ascii_case("bigint")));
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id, v FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1), Value::Int(7)]]);
}

#[test]
fn compound_copy_builds_one_generation_and_one_heap_scan() {
    let dir = tempfile::tempdir().unwrap();
    let engine =
        StorageQueryEngine::open_with_storage_config(dir.path(), strict_online_ddl_config())
            .unwrap();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,10,100),(2,20,200),(3,30,300)")
        .unwrap();
    let before = engine_status_u64(&engine, "ddl_copy_total");
    engine
        .execute(
            "ALTER TABLE t ALGORITHM=COPY, MODIFY a BIGINT, MODIFY b BIGINT, \
             ADD INDEX ix_ab(a,b)",
        )
        .unwrap();
    let after = engine_status_u64(&engine, "ddl_copy_total");
    assert_eq!(
        after,
        before + 1,
        "compound ALTER performed multiple COPY builds"
    );
    let table = engine.database().table("test.t").unwrap();
    assert_eq!(table.active_generation, 2);
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT id,a,b FROM t FORCE INDEX(ix_ab) WHERE a=20")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rows.rows,
        vec![vec![Value::Int(2), Value::Int(20), Value::Int(200)]]
    );
}

#[test]
fn explicit_copy_add_column_does_not_take_instant_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine =
        StorageQueryEngine::open_with_storage_config(dir.path(), strict_online_ddl_config())
            .unwrap();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1),(2)").unwrap();
    let before = engine_status_u64(&engine, "ddl_copy_total");
    engine
        .execute("ALTER TABLE t ALGORITHM=COPY, ADD COLUMN v INT DEFAULT 7")
        .unwrap();
    assert_eq!(engine_status_u64(&engine, "ddl_copy_total"), before + 1);
    assert_eq!(
        engine.database().table("test.t").unwrap().active_generation,
        2
    );
}

#[test]
fn copy_rewrite_partitioned_table_succeeds() {
    // Partitioned COPY rewrites erase each partition and reinsert with partition awareness,
    // preserving data while rerouting according to new types.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT, v INT) PARTITION BY HASH(id) PARTITIONS 2")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 7)").unwrap();

    engine
        .execute("ALTER TABLE t MODIFY COLUMN v BIGINT")
        .expect("partitioned COPY rewrite is supported now");

    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id, v FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1), Value::Int(7)]]);
}

#[test]
fn copy_rewrite_rejects_foreign_key_column_type_change() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE parent (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE child (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES parent(id))",
        )
        .unwrap();
    engine.execute("INSERT INTO parent VALUES (1)").unwrap();
    engine.execute("INSERT INTO child VALUES (10, 1)").unwrap();

    let err = engine
        .execute("ALTER TABLE child MODIFY COLUMN pid BIGINT")
        .expect_err("FK child columns cannot be type-changed by COPY v1");
    assert_eq!(err.code(), 1832);

    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id, pid FROM child").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(10), Value::Int(1)]]);
}

/// After a successful synchronous ADD INDEX, the index reaches the
/// `Public` state and is observable via SHOW INDEX. (The intermediate
/// `Building` state is not directly observable because the current
/// implementation is synchronous; the test in
/// `restart_after_crash_drops_building_indexes` exercises the
/// recovery path.)
#[test]
fn add_index_completes_in_public_state() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 100), (2, 200)")
        .unwrap();
    engine.execute("CREATE INDEX v_idx ON t (v)").unwrap();

    let QueryResponse::ResultSet(rs) = engine.execute("SHOW INDEX FROM t").unwrap() else {
        panic!("expected result set");
    };
    let names: Vec<&str> = rs
        .rows
        .iter()
        .filter_map(|r| match &r[2] {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();
    assert!(names.contains(&"PRIMARY"));
    assert!(names.contains(&"v_idx"));
}

/// CREATE UNIQUE INDEX on a clean table succeeds — the chunked
/// backfill path must produce a tree that the finalize-time
/// duplicate scan accepts. Sanity baseline.
#[test]
fn unique_index_builds_on_distinct_values() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_relaxed_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for i in 1..=200 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .unwrap();
    }
    engine
        .execute("CREATE UNIQUE INDEX v_uniq ON t (v)")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id FROM t WHERE v = 100").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(100)]]);
}

/// Existing-row ADD PRIMARY KEY over four signed integer columns exercises
/// the fixed-width offline-sort path. The resulting index must retain the
/// same lookup and uniqueness semantics as the generic IndexKey builder.
#[test]
fn four_int_primary_key_builds_on_existing_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_relaxed_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, c INT NOT NULL, d INT NOT NULL, payload INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,2,3,4,10), (1,2,3,5,20), (2,1,1,1,30)")
        .unwrap();
    engine
        .execute("ALTER TABLE t ADD PRIMARY KEY (a,b,c,d)")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT payload FROM t WHERE a=1 AND b=2 AND c=3 AND d=5")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(20)]]);
    let err = engine
        .execute("INSERT INTO t VALUES (1,2,3,4,99)")
        .expect_err("rebuilt four-column PRIMARY must enforce uniqueness");
    assert_eq!(err.code(), 1062);
}

#[test]
fn add_primary_key_still_rejects_existing_null_before_publish() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine.execute("CREATE TABLE t (a INT, b INT)").unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,2), (NULL,3)")
        .unwrap();
    let err = engine
        .execute("ALTER TABLE t ADD PRIMARY KEY (a,b)")
        .expect_err("existing NULL must reject ADD PRIMARY KEY");
    assert_eq!(err.code(), 1138);
}

/// CREATE UNIQUE INDEX on a table with pre-existing duplicate values is
/// rejected by the globally ordered build stream before catalog publication.
#[test]
fn unique_index_rejects_pre_existing_duplicate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 100), (2, 200), (3, 100)")
        .unwrap();

    let err = engine
        .execute("CREATE UNIQUE INDEX v_uniq ON t (v)")
        .expect_err("UNIQUE index over duplicate values must fail");
    assert_eq!(err.code(), 1062);

    // The failed DDL must not leave a half-built index visible.
    let QueryResponse::ResultSet(jobs) = engine
        .execute("SELECT * FROM information_schema.mydb_ddl_jobs")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(jobs.rows.len(), 1);
    assert_eq!(jobs.rows[0][3], Value::String("INDEX".into()));
    assert_eq!(jobs.rows[0][5], Value::String("FAILED".into()));
    assert!(!matches!(jobs.rows[0][9], Value::Null));
    assert_eq!(jobs.rows[0][10], Value::Int(0));
    let QueryResponse::ResultSet(idx) = engine.execute("SHOW INDEX FROM t").unwrap() else {
        panic!("expected result set");
    };
    let names: Vec<&str> = idx
        .rows
        .iter()
        .filter_map(|r| match &r[2] {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();
    assert!(
        !names.contains(&"v_uniq"),
        "v_uniq should be gone: {names:?}"
    );
}

#[test]
fn batched_add_unique_index_rejects_pre_existing_duplicate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_relaxed_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, payload INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 100, 10), (2, 200, 20), (3, 100, 30)")
        .unwrap();

    let err = engine
        .execute(
            "ALTER /*+ PARALLEL(4) */ TABLE t \
             ADD INDEX payload_idx(payload), ADD UNIQUE INDEX v_uniq(v)",
        )
        .expect_err("batched UNIQUE build must reject duplicate ordered keys");
    assert_eq!(err.code(), 1062);

    let QueryResponse::ResultSet(indexes) = engine.execute("SHOW INDEX FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert!(indexes.rows.iter().all(|row| {
        !matches!(&row[2], Value::String(name) if name == "payload_idx" || name == "v_uniq")
    }));
    let QueryResponse::ResultSet(jobs) = engine
        .execute("SELECT * FROM information_schema.mydb_ddl_jobs")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(jobs.rows.len(), 1);
    assert_eq!(jobs.rows[0][5], Value::String("FAILED".into()));
    assert!(!matches!(jobs.rows[0][9], Value::Null));
}

/// NULL values are allowed to repeat in a non-PRIMARY UNIQUE index
/// per MySQL semantics — finalize duplicate scan must not flag them.
#[test]
fn unique_index_accepts_repeated_null_values() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_relaxed_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t (id, v) VALUES (1, 1), (2, NULL), (3, NULL), (4, 4)")
        .unwrap();
    engine
        .execute("CREATE UNIQUE INDEX v_uniq ON t (v)")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id FROM t WHERE v = 4").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(4)]]);
}

/// Online ADD INDEX permits other connections to observe progress, but the
/// issuing statement must not return OK until the index is Public.
#[test]
fn online_add_index_waits_for_backfill_completion() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    engine.database().set_async_ddl(true);

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    insert_t_values(&engine, 1, 100);

    let hook = engine.database().install_ddl_backfill_test_hook();
    let ddl_done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let ddl_engine = std::sync::Arc::clone(&engine);
    let ddl_done_worker = std::sync::Arc::clone(&ddl_done);
    let ddl = std::thread::spawn(move || {
        let result = ddl_engine.execute("CREATE INDEX v_idx ON t (v)");
        ddl_done_worker.store(true, std::sync::atomic::Ordering::Release);
        result
    });
    assert!(
        hook.wait_until_reached(std::time::Duration::from_secs(2)),
        "online ADD INDEX did not reach the post-BUILDING backfill hook"
    );

    let QueryResponse::ResultSet(jobs) = engine
        .execute("SELECT * FROM information_schema.mydb_ddl_jobs")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let saw_building = jobs
        .rows
        .iter()
        .any(|r| matches!(&r[5], Value::String(s) if s == "INDEX_BUILD"));

    assert!(
        saw_building,
        "online ADD INDEX should expose INDEX_BUILD while the test hook pauses backfill: {:?}",
        jobs.rows
    );
    assert!(
        !ddl_done.load(std::sync::atomic::Ordering::Acquire),
        "CREATE INDEX returned OK before the index became Public"
    );

    hook.release();
    ddl.join().unwrap().unwrap();

    let QueryResponse::ResultSet(idx) = engine.execute("SHOW INDEX FROM t").unwrap() else {
        panic!("expected result set");
    };
    let names: Vec<&str> = idx
        .rows
        .iter()
        .filter_map(|r| match &r[2] {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();
    assert!(
        names.contains(&"v_idx"),
        "v_idx missing post-wait: {names:?}"
    );

    let QueryResponse::ResultSet(jobs_after) = engine
        .execute("SELECT * FROM information_schema.mydb_ddl_jobs")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(jobs_after.rows.len(), 1);
    assert_eq!(jobs_after.rows[0][5], Value::String("COMMITTED".into()));
    assert_eq!(jobs_after.rows[0][10], Value::Int(0));

    // Index points to the seeded data — sanity probe.
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id FROM t WHERE v = 500").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(50)]]);
}

/// `information_schema.mydb_ddl_jobs` is empty when no online DDL is
/// in flight. Smoke-test the view shape.
// P2/13 — the new mydb_undo_history view groups by (txn_id, table_id)
// and surfaces the in-memory undo mirror to operators. Each DML
// statement in autocommit produces at least one undo record; the
// total across all groups must reflect that.
#[test]
fn mydb_undo_history_view_groups_by_txn_and_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    engine.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    engine.execute("INSERT INTO t VALUES (3, 30)").unwrap();
    engine
        .execute("UPDATE t SET v = v + 1 WHERE id = 1")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "SELECT TXN_ID, TABLE_ID, RECORD_COUNT, CHAIN_DEPTH, OLDEST_CREATED_UNIX_MS \
             FROM information_schema.mydb_undo_history",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    // Aggregate total record_count across all groups — each DML lands
    // at least one undo, so we expect >= 4 across the four statements.
    let total: i64 = rs
        .rows
        .iter()
        .map(|row| match row[2] {
            Value::Int(n) => n,
            _ => panic!("RECORD_COUNT must be Int"),
        })
        .sum();
    assert!(
        total >= 4,
        "expected total undo records >= 4; got rows {:?}",
        rs.rows
    );
    // Every row's OLDEST_CREATED_UNIX_MS must be > 0 (sanity:
    // current_unix_ms is captured at undo push time, never the epoch).
    for row in &rs.rows {
        if let Value::Int(ms) = row[4] {
            assert!(ms > 0, "OLDEST_CREATED_UNIX_MS must be non-zero");
        }
    }
}

#[test]
fn mydb_ddl_jobs_view_retains_terminal_index_history_when_idle() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("CREATE INDEX v_idx ON t (v)").unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT * FROM information_schema.mydb_ddl_jobs")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.columns.len(),
        11,
        "mydb_ddl_jobs has identity, progress, replay, error, and cancellation columns"
    );
    assert_eq!(rs.rows.len(), 1);
    assert_eq!(rs.rows[0][3], Value::String("INDEX".into()));
    assert_eq!(rs.rows[0][4], Value::String("v_idx".into()));
    assert_eq!(rs.rows[0][5], Value::String("COMMITTED".into()));
    assert_eq!(rs.rows[0][9], Value::Null);
    assert_eq!(rs.rows[0][10], Value::Int(0));
}

/// `information_schema.mydb_ddl_jobs` may surface a `Building` index
/// while ADD INDEX is in flight on a parallel connection. The
/// observation is inherently racy because mydb's ADD INDEX is
/// synchronous from the issuing connection's perspective, but a
/// separate connection can read the catalog between Phase 1
/// (publish Building) and Phase 3 (flip to Public). Seeding a
/// large-enough table widens the window so the polling observer
/// reliably catches at least one `BUILDING` row across the run.
#[test]
fn mydb_ddl_jobs_view_surfaces_building_index_during_add() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    insert_t_values(&engine, 1, 100);

    let hook = engine.database().install_ddl_backfill_test_hook();
    let ddl_engine = std::sync::Arc::clone(&engine);
    let ddl = std::thread::spawn(move || ddl_engine.execute("CREATE INDEX v_idx ON t (v)"));

    assert!(
        hook.wait_until_reached(std::time::Duration::from_secs(2)),
        "ADD INDEX did not reach the post-BUILDING backfill hook"
    );

    let QueryResponse::ResultSet(during) = engine
        .execute("SELECT * FROM information_schema.mydb_ddl_jobs")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(
        during
            .rows
            .iter()
            .any(|row| matches!(&row[5], Value::String(s) if s == "INDEX_BUILD")),
        "expected to observe the v_idx job in INDEX_BUILD state: {:?}",
        during.rows
    );
    hook.release();
    ddl.join().unwrap().unwrap();

    // Completion leaves a bounded terminal record rather than an active job.
    let QueryResponse::ResultSet(after) = engine
        .execute("SELECT * FROM information_schema.mydb_ddl_jobs")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(after.rows.len(), 1);
    assert_eq!(after.rows[0][5], Value::String("COMMITTED".into()));
    assert_eq!(after.rows[0][10], Value::Int(0));
}

#[test]
fn mydb_ddl_jobs_view_surfaces_table_copy_progress() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    insert_t_values(&engine, 1, 25);

    let hook = engine.database().install_ddl_backfill_test_hook();
    let ddl_engine = std::sync::Arc::clone(&engine);
    let ddl =
        std::thread::spawn(move || ddl_engine.execute("ALTER TABLE t MODIFY COLUMN v BIGINT"));

    assert!(
        hook.wait_until_reached(std::time::Duration::from_secs(2)),
        "ALTER TABLE COPY did not reach the COPY progress hook"
    );

    let QueryResponse::ResultSet(during) = engine
        .execute("SELECT * FROM information_schema.mydb_ddl_jobs")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(
        during.rows.iter().any(|row| {
            matches!(&row[3], Value::String(kind) if kind == "TABLE")
                && matches!(&row[4], Value::String(name) if name == "COPY")
                && matches!(&row[5], Value::String(state) if state == "COPYING")
                && row[7] == Value::Int(25)
        }),
        "expected to observe ALTER TABLE COPY progress: {:?}",
        during.rows
    );

    hook.release();
    ddl.join().unwrap().unwrap();

    let QueryResponse::ResultSet(after) = engine
        .execute("SELECT * FROM information_schema.mydb_ddl_jobs")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(
        after.rows.iter().any(|row| {
            matches!(&row[3], Value::String(kind) if kind == "TABLE")
                && matches!(&row[4], Value::String(name) if name == "COPY")
                && matches!(&row[5], Value::String(state) if state == "COMMITTED")
        }),
        "completed COPY should remain in bounded terminal history: {:?}",
        after.rows
    );
}

#[test]
fn copy_lock_none_uses_online_type_copy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_relaxed_test_engine(dir.path());
    engine.database().set_async_ddl(true);
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("ALTER TABLE t ALGORITHM=COPY, LOCK=NONE, MODIFY COLUMN v BIGINT")
        .unwrap();
}

#[test]
fn cancel_ddl_job_aborts_before_final_swap_and_keeps_original_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    engine.database().set_async_ddl(true);
    engine
        .execute("CREATE TABLE cancel_t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO cancel_t VALUES (1,10),(2,20)")
        .unwrap();

    let hook = engine.database().install_ddl_backfill_test_hook();
    let ddl_engine = std::sync::Arc::clone(&engine);
    let ddl = std::thread::spawn(move || {
        ddl_engine.execute("ALTER TABLE cancel_t LOCK=SHARED, MODIFY COLUMN v BIGINT")
    });
    assert!(hook.wait_until_reached(std::time::Duration::from_secs(2)));

    let QueryResponse::ResultSet(jobs) = engine
        .execute("SELECT JOB_ID FROM information_schema.mydb_ddl_jobs WHERE TABLE_NAME='cancel_t'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let Value::Int(job_id) = jobs.rows[0][0] else {
        panic!("expected numeric job id");
    };
    engine.execute(&format!("CANCEL DDL JOB {job_id}")).unwrap();
    hook.release();
    let err = ddl
        .join()
        .unwrap()
        .expect_err("cancelled DDL must fail the issuing statement");
    assert_eq!(err.code(), 1317, "{}", err.message());

    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT id, v FROM cancel_t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows.len(), 2);
    let table = engine.database().table("test.cancel_t").unwrap();
    assert_eq!(
        table.columns[1].data_type,
        crate::storage::DataTypeKind::Int
    );
    let QueryResponse::ResultSet(history) = engine
        .execute(
            "SELECT STATE, ERROR_MESSAGE FROM information_schema.mydb_ddl_jobs \
             WHERE TABLE_NAME='cancel_t'",
        )
        .unwrap()
    else {
        panic!("expected history result");
    };
    assert!(history.rows.iter().any(|row| {
        row[0] == Value::String("CANCELLED".into()) && !matches!(row[1], Value::Null)
    }));
}

#[test]
fn copy_lock_none_returns_1846() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "SET sql_mode='STRICT_TRANS_TABLES'")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1, 10)")
        .unwrap();

    let err = engine
        .execute_with_session(
            &mut session,
            "ALTER TABLE t LOCK=NONE, MODIFY COLUMN v BIGINT",
        )
        .expect_err("LOCK=NONE COPY is unsupported");
    assert_eq!(err.code(), 1846);
    assert!(err.message().contains("LOCK=SHARED"));
}

/// Concurrent INSERT during ADD INDEX must end up in the resulting
/// index. Exercises the chunked incremental-backfill path: the empty
/// index file is laid down before the heap scan, so DML inserts
/// during the backfill flow into the same B-tree that backfill is
/// populating, and the (idempotent) leaf insert tolerates overlap
/// between the two paths.
#[test]
fn concurrent_insert_during_add_index_lands_in_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_strict_test_engine(dir.path()));

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    // Seed with enough rows that the backfill is observably non-trivial.
    insert_t_values(&engine, 1, 2000);

    let hook = engine.database().install_ddl_backfill_test_hook();
    let ddl_engine = std::sync::Arc::clone(&engine);
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 7301;
        ddl_engine.execute_with_session(&mut session, "CREATE INDEX v_idx ON t (v)")
    });
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let mut writer = SessionContext::default();
    writer.connection_id = 7302;
    let writes = (2001..=2200)
        .map(|i| {
            engine.execute_with_session(
                &mut writer,
                &format!("INSERT INTO t VALUES ({i}, {})", i * 10),
            )
        })
        .collect::<Vec<_>>();
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "online backfill hook not reached: {result:?}");
    for write in writes {
        write.unwrap();
    }
    result.unwrap();
    assert_online_index_matches_heap(&engine, "v_idx");

    // Spot-check rows from before the DDL, during the DDL, and the
    // tail of the writer's stream — every one should be reachable
    // through the index.
    for &id in &[1i64, 1000, 2000, 2001, 2150, 2200] {
        let v = id * 10;
        let QueryResponse::ResultSet(rs) = engine
            .execute(&format!("SELECT id FROM t WHERE v = {v}"))
            .unwrap()
        else {
            panic!("expected result set for v={v}");
        };
        assert_eq!(
            rs.rows,
            vec![vec![Value::Int(id)]],
            "row id={id} (v={v}) missing from index after concurrent ADD INDEX"
        );
    }

    // Sanity: total row count matches inserts.
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(*) FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(2200)]]);
}

/// Crash recovery: a `Building` index left behind in the on-disk
/// catalog (because the previous process died mid-ADD-INDEX) must be
/// scrubbed at startup. After reopening, the partial index is no
/// longer visible — neither in SHOW INDEX nor in the planner — and
/// the user is free to re-issue ADD INDEX.
#[test]
fn restart_after_crash_drops_building_indexes() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
            .unwrap();
        engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        // Simulate a mid-ADD-INDEX crash by persisting the catalog shape
        // left after Phase 1: a `Building` index entry whose backfill never
        // reached the Public flip. Inject it directly (and commit it to the
        // data-dictionary heaps) instead of starting an async worker, which
        // would race the background thread that may complete the build.
        engine
            .database()
            .inject_building_index_for_test("test", "t", "v_idx", vec!["v".into()])
            .unwrap();
        engine.database().flush_dirty_data().unwrap();
    }

    // Reopen — the recovery pass in `migrate_catalog` should drop the
    // Building entry.
    let engine = open_strict_test_engine(dir.path());
    let QueryResponse::ResultSet(rs) = engine.execute("SHOW INDEX FROM t").unwrap() else {
        panic!("expected result set");
    };
    let names: Vec<&str> = rs
        .rows
        .iter()
        .filter_map(|r| match &r[2] {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();
    assert!(
        !names.contains(&"v_idx"),
        "Building tombstone should have been scrubbed at startup; SHOW INDEX returned {names:?}"
    );
    assert!(names.contains(&"PRIMARY"), "PRIMARY must survive recovery");

    // The user can re-issue ADD INDEX cleanly. Insert after reopening so
    // this assertion focuses on the scrubbed Building tombstone, not on
    // the WAL surgery above.
    engine.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    engine.execute("CREATE INDEX v_idx ON t (v)").unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id FROM t WHERE v = 20").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(2)]]);
}

/// CHANGE COLUMN preserves the logical row while allowing COPY rewrite
/// when the declared type changes.
#[test]
fn instant_change_column_renames_and_modifies() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 7)").unwrap();

    // Compatible change: same INT type, rename only.
    engine
        .execute("ALTER TABLE t CHANGE COLUMN v val INT")
        .unwrap();

    let err = engine
        .execute("SELECT v FROM t")
        .expect_err("old name removed by CHANGE");
    assert_eq!(err.code(), 1054);

    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id, val FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1), Value::Int(7)]]);

    engine
        .execute("ALTER TABLE t CHANGE COLUMN val v BIGINT")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id, v FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1), Value::Int(7)]]);
}

/// Restart the engine after an INSTANT ADD COLUMN to ensure the
/// catalog-only DDL is durable and that recovery / migration of the
/// new column-identity fields (column_id, physical_pos, logical_pos)
/// keeps reading consistent.
#[test]
fn instant_add_column_persists_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
            .unwrap();
        engine
            .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
            .unwrap();
        engine
            .execute("ALTER TABLE t ADD COLUMN extra INT DEFAULT 77")
            .unwrap();
    }

    let engine = open_strict_test_engine(dir.path());
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id, v, extra FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(1), Value::Int(10), Value::Int(77)],
            vec![Value::Int(2), Value::Int(20), Value::Int(77)],
        ]
    );
}

/// Regression for S8 (alter-table-review-2026-07): a non-strict MODIFY to a
/// numeric type used to abort the whole ALTER when a value was a
/// non-numeric / partially-numeric string, because the lossy fallback only
/// handled out-of-range (1264) and not incorrect-value (1366) / truncation
/// (1265). MySQL non-strict parses the leading numeric prefix and warns.
#[test]
fn copy_modify_non_strict_coerces_non_numeric_string_to_int() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "SET sql_mode='NO_ENGINE_SUBSTITUTION'")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id INT PRIMARY KEY, c VARCHAR(20))",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1, 'abc'), (2, '12x')")
        .unwrap();

    // Must succeed (previously errored 1366), coercing 'abc' → 0, '12x' → 12.
    engine
        .execute_with_session(&mut session, "ALTER TABLE t MODIFY COLUMN c INT")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut session, "SELECT id, c FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(1), Value::Int(0)],
            vec![Value::Int(2), Value::Int(12)],
        ]
    );
}

/// The strict counterpart of S8 still fails hard (no silent data change).
#[test]
fn copy_modify_strict_rejects_non_numeric_string_to_int() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, c VARCHAR(20))")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 'abc')").unwrap();
    // Default (empty) sql_mode is strict.
    let err = engine
        .execute("ALTER TABLE t MODIFY COLUMN c INT")
        .expect_err("strict MODIFY must reject a non-numeric string");
    assert!(
        matches!(err.code(), 1366 | 1264 | 1265),
        "unexpected error: {err:?}"
    );
    // Data unchanged.
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT c FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::String("abc".to_string()));
}

/// Regression for S23 (alter-table-review-2026-07): a DECIMAL precision
/// increase that stays within the same physical width bucket and keeps the
/// scale is byte-identical on disk, so it must run INSTANT (no COPY) —
/// `ALGORITHM=INSTANT` must be accepted, and existing data preserved.
#[test]
fn decimal_precision_widen_same_bucket_is_instant() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, amt DECIMAL(5,2))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 123.45), (2, -9.99)")
        .unwrap();
    // Same 4-byte bucket (precision <= 9), same scale → INSTANT eligible.
    engine
        .execute("ALTER TABLE t ALGORITHM=INSTANT, MODIFY amt DECIMAL(8,2)")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id, amt FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][1].to_display_string(), "123.45");
    assert_eq!(rs.rows[1][1].to_display_string(), "-9.99");

    // Crossing a width bucket (<=9 → <=18) is NOT byte-identical, so INSTANT
    // must be rejected (it requires a COPY).
    let err = engine
        .execute("ALTER TABLE t ALGORITHM=INSTANT, MODIFY amt DECIMAL(15,2)")
        .expect_err("cross-bucket DECIMAL widen cannot be INSTANT");
    assert_eq!(err.code(), 1846);
}

/// Regression for S5 (alter-table-review-2026-07): the exact statement shape
/// standard online-DDL tools emit — `ALGORITHM=INPLACE, LOCK=NONE` — must be
/// accepted for non-rewriting operations. It used to be rejected with 1846,
/// breaking gh-ost / pt-online-schema-change / Rails / Django migrations.
#[test]
fn inplace_lock_none_accepted_for_online_ddl_tooling() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b VARCHAR(20))")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10, 'x')").unwrap();

    // INSTANT add column with the tooling's algorithm/lock hints.
    engine
        .execute("ALTER TABLE t ADD COLUMN c INT, ALGORITHM=INPLACE, LOCK=NONE")
        .expect("ADD COLUMN INPLACE/NONE must be accepted");
    // INPLACE add index with LOCK=NONE.
    engine
        .execute("ALTER TABLE t ADD INDEX idx_a (a), ALGORITHM=INPLACE, LOCK=NONE")
        .expect("ADD INDEX INPLACE/NONE must be accepted");

    // But a type-changing COPY rewrite with LOCK=NONE and no async DDL still
    // can't be honoured (kept behaviour).
    let err = engine
        .execute("ALTER TABLE t MODIFY a BIGINT, ALGORITHM=COPY, LOCK=NONE")
        .expect_err("COPY LOCK=NONE without async DDL must still be rejected");
    assert_eq!(err.code(), 1846);
}

#[test]
fn btree_ddl_parallel_hints_build_correct_indexes_and_publish_metrics() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT NOT NULL, b INT NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 7, 70), (2, 3, 30), (3, 7, 71), (4, 9, 90)")
        .unwrap();

    engine
        .execute("CREATE /*+ PARALLEL(4) */ INDEX ix_ab ON t(a, b)")
        .unwrap();
    engine
        .execute("ALTER /*+ NO_PARALLEL */ TABLE t ADD INDEX ix_b(b)")
        .unwrap();
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT id FROM t FORCE INDEX(ix_ab) WHERE a=7 ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(1)], vec![Value::Int(3)]]);

    let status = engine.database().storage_status();
    assert!(status.index_build_requested_workers >= 5);
    assert!(status.index_build_parallel_builds >= 1);
    assert!(status.index_build_serial_builds >= 1);
}

#[test]
fn explicit_parallel_fulltext_index_build_matches_existing_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_relaxed_test_engine(dir.path());
    engine
        .execute("CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)")
        .unwrap();
    engine
        .execute("INSERT INTO docs VALUES(1,'alpha beta'),(2,'beta gamma'),(3,NULL)")
        .unwrap();
    engine
        .execute("CREATE /*+ PARALLEL(2) */ FULLTEXT INDEX ft_body ON docs(body)")
        .unwrap();
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT id FROM docs WHERE MATCH(body) AGAINST('alpha') ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set")
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn online_type_copy_default_allows_writes_during_backfill() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_relaxed_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY, v INT, INDEX v_idx(v))")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,10),(2,20)").unwrap();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 7801;
        worker.execute_with_session(&mut session, "ALTER TABLE t MODIFY v BIGINT")
    });
    let reached = hook.wait_until_reached(Duration::from_secs(5));
    let mut writer = SessionContext::default();
    writer.connection_id = 7802;
    let writes = [
        "INSERT INTO t VALUES(3,30)",
        "UPDATE t SET v=11 WHERE id=1",
        "DELETE FROM t WHERE id=2",
    ]
    .map(|sql| engine.execute_with_session(&mut writer, sql));
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "COPY hook not reached: {result:?}");
    for write in writes {
        write.expect("type COPY must permit DML during backfill");
    }
    result.unwrap();
    let QueryResponse::ResultSet(rows) = engine.execute("SELECT id,v FROM t ORDER BY id").unwrap()
    else {
        panic!("rows");
    };
    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Int(1), Value::Int(11)],
            vec![Value::Int(3), Value::Int(30)]
        ]
    );
    assert_online_index_matches_heap(&engine, "PRIMARY");
    assert_online_index_matches_heap(&engine, "v_idx");
}

#[test]
fn online_type_copy_transactions_and_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_strict_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY, v INT UNIQUE)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,10),(2,20),(3,30)")
        .unwrap();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 7811;
        worker.execute_with_session(&mut session, "ALTER TABLE t MODIFY v BIGINT, LOCK=NONE")
    });
    assert!(hook.wait_until_reached(Duration::from_secs(5)));
    let mut writer = SessionContext::default();
    writer.connection_id = 7812;
    let writes = (|| -> Result<(), QueryError> {
        for sql in [
            "BEGIN",
            "UPDATE t SET v=99 WHERE id=1",
            "ROLLBACK",
            "BEGIN",
            "UPDATE t SET v=40 WHERE id=1",
            "SAVEPOINT s",
            "DELETE FROM t WHERE id=2",
            "INSERT INTO t VALUES(4,44)",
            "ROLLBACK TO SAVEPOINT s",
            "COMMIT",
            "BEGIN",
            "DELETE FROM t WHERE id IN (1,2)",
            "INSERT INTO t VALUES(1,20),(2,40)",
            "COMMIT",
        ] {
            engine.execute_with_session(&mut writer, sql)?;
        }
        Ok(())
    })();
    hook.release();
    let result = ddl.join().unwrap();
    writes.unwrap();
    result.unwrap();
    let expected = vec![
        vec![Value::Int(1), Value::Int(20)],
        vec![Value::Int(2), Value::Int(40)],
        vec![Value::Int(3), Value::Int(30)],
    ];
    let QueryResponse::ResultSet(rows) = engine.execute("SELECT id,v FROM t ORDER BY id").unwrap()
    else {
        panic!("rows");
    };
    assert_eq!(rows.rows, expected);
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    let QueryResponse::ResultSet(rows) = engine.execute("SELECT id,v FROM t ORDER BY id").unwrap()
    else {
        panic!("rows");
    };
    assert_eq!(rows.rows, expected);
    assert_eq!(
        engine
            .execute("INSERT INTO t VALUES(9,40)")
            .unwrap_err()
            .code(),
        1062
    );
}

#[test]
fn online_type_copy_invalid_concurrent_value_preserves_source() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_strict_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY, v BIGINT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,10)").unwrap();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 7821;
        worker.execute_with_session(&mut session, "ALTER TABLE t MODIFY v TINYINT")
    });
    assert!(hook.wait_until_reached(Duration::from_secs(5)));
    let mut writer = SessionContext::default();
    writer.connection_id = 7822;
    let write = engine.execute_with_session(&mut writer, "INSERT INTO t VALUES(2,1000)");
    hook.release();
    let result = ddl.join().unwrap();
    write.unwrap();
    assert_eq!(result.unwrap_err().code(), 1264);
    assert_eq!(
        engine.database().table("test.t").unwrap().columns[1].data_type,
        crate::storage::DataTypeKind::BigInt
    );
    let QueryResponse::ResultSet(rows) = engine.execute("SELECT v FROM t WHERE id=2").unwrap()
    else {
        panic!("rows");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(1000)]]);
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    let QueryResponse::ResultSet(rows) = engine.execute("SELECT v FROM t WHERE id=2").unwrap()
    else {
        panic!("rows");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(1000)]]);
}

#[test]
fn online_type_copy_lob_values_and_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_strict_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY, v INT, payload LONGTEXT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,10,REPEAT('a',20000)),(2,20,REPEAT('b',20000))")
        .unwrap();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 7831;
        worker.execute_with_session(&mut session, "ALTER TABLE t MODIFY v BIGINT, LOCK=NONE")
    });
    let reached = hook.wait_until_reached(Duration::from_secs(5));
    let mut writer = SessionContext::default();
    writer.connection_id = 7832;
    let writes = [
        "UPDATE t SET payload=REPEAT('c',24000) WHERE id=1",
        "DELETE FROM t WHERE id=2",
        "INSERT INTO t VALUES(3,30,REPEAT('d',32000))",
    ]
    .map(|sql| engine.execute_with_session(&mut writer, sql));
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "{result:?}");
    for write in writes {
        write.unwrap();
    }
    result.unwrap();
    let expected = vec![
        vec![Value::Int(1), Value::Int(24000), Value::String("c".into())],
        vec![Value::Int(3), Value::Int(32000), Value::String("d".into())],
    ];
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT id,LENGTH(payload),LEFT(payload,1) FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("rows");
    };
    assert_eq!(rows.rows, expected);
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT id,LENGTH(payload),LEFT(payload,1) FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("rows");
    };
    assert_eq!(rows.rows, expected);
}

#[test]
fn online_type_copy_rejects_read_view_from_before_generation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_relaxed_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("CREATE TABLE other_t(id INT)").unwrap();
    engine.execute("INSERT INTO t VALUES(1,10)").unwrap();
    let mut reader = SessionContext::default();
    reader.connection_id = 7841;
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
    engine.execute("ALTER TABLE t MODIFY v BIGINT").unwrap();
    let result = engine.execute_with_session(&mut reader, "SELECT * FROM t");
    engine
        .execute_with_session(&mut reader, "ROLLBACK")
        .unwrap();
    assert_eq!(result.unwrap_err().code(), 1412);
    engine
        .execute_with_session(&mut reader, "SELECT * FROM t")
        .unwrap();
}

#[test]
fn online_type_copy_compound_change_and_position() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_relaxed_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY, v INT, n INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,10,100)").unwrap();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 7851;
        worker.execute_with_session(
            &mut session,
            "ALTER TABLE t CHANGE v amount BIGINT FIRST, MODIFY n BIGINT, LOCK=NONE",
        )
    });
    let reached = hook.wait_until_reached(Duration::from_secs(5));
    let mut writer = SessionContext::default();
    writer.connection_id = 7852;
    let write = engine.execute_with_session(&mut writer, "INSERT INTO t VALUES(2,20,200)");
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "{result:?}");
    write.unwrap();
    result.unwrap();
    let QueryResponse::ResultSet(rows) = engine.execute("SELECT * FROM t ORDER BY id").unwrap()
    else {
        panic!("rows");
    };
    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Int(10), Value::Int(1), Value::Int(100)],
            vec![Value::Int(20), Value::Int(2), Value::Int(200)]
        ]
    );
}

#[test]
fn online_type_copy_lock_time_scaling_probe() {
    for count in [1000, 10000] {
        let dir = tempfile::tempdir().unwrap();
        let engine = std::sync::Arc::new(open_strict_test_engine(dir.path()));
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY, v INT, INDEX v_idx(v))")
            .unwrap();
        for start in (0..count).step_by(1000) {
            let values = (start..(start + 1000).min(count))
                .map(|i| format!("({i},{i})"))
                .collect::<Vec<_>>()
                .join(",");
            engine
                .execute(&format!("INSERT INTO t VALUES {values}"))
                .unwrap();
        }
        let before = engine.database().ddl_copy_perf_snapshot();
        let hook = engine.database().install_ddl_backfill_test_hook();
        let worker = engine.clone();
        let started = std::time::Instant::now();
        let ddl = std::thread::spawn(move || {
            let mut session = SessionContext::default();
            session.connection_id = 7861;
            worker.execute_with_session(&mut session, "ALTER TABLE t MODIFY v BIGINT")
        });
        let reached = hook.wait_until_reached(Duration::from_secs(5));
        let mut writer = SessionContext::default();
        writer.connection_id = 7862;
        let write_started = std::time::Instant::now();
        let write = engine.execute_with_session(&mut writer, "UPDATE t SET v=-1 WHERE id=0");
        let write_us = write_started.elapsed().as_micros();
        hook.release();
        let result = ddl.join().unwrap();
        assert!(reached, "{result:?}");
        write.unwrap();
        result.unwrap();
        let after = engine.database().ddl_copy_perf_snapshot();
        eprintln!("ONLINE_COPY_PROBE rows={count} elapsed_us={} concurrent_write_us={write_us} hold_us={} wait_us={} final_tail={} full_builds={}",
            started.elapsed().as_micros(), after[22]-before[22],after[21]-before[21],after[24]-before[24],after[9]-before[9]);
        assert_eq!(
            after[9] - before[9],
            1,
            "concurrent write caused a full rebuild"
        );
        assert_online_index_matches_heap(&engine, "v_idx");
    }
}

#[test]
fn online_type_copy_final_tail_and_uncommitted_generation_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let recovered = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_strict_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY, v INT, INDEX v_idx(v))")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,10),(2,20)").unwrap();
    let hook = engine.database().install_online_copy_publish_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 7871;
        worker.execute_with_session(&mut session, "ALTER TABLE t MODIFY v BIGINT")
    });
    let reached = hook.wait_until_reached(Duration::from_secs(5));
    let mut writer = SessionContext::default();
    writer.connection_id = 7872;
    let write = engine.execute_with_session(&mut writer, "UPDATE t SET v=90 WHERE id=1");
    if reached {
        copy_online_primary_data_dir(dir.path(), recovered.path());
    }
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "{result:?}");
    write.unwrap();
    result.unwrap();
    assert_online_index_matches_heap(&engine, "v_idx");
    let QueryResponse::ResultSet(rows) = engine.execute("SELECT v FROM t WHERE id=1").unwrap()
    else {
        panic!("rows");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(90)]]);
    let old = open_strict_test_engine(recovered.path());
    assert_eq!(
        old.database().table("test.t").unwrap().columns[1].data_type,
        crate::storage::DataTypeKind::Int
    );
    let QueryResponse::ResultSet(rows) = old.execute("SELECT v FROM t WHERE id=1").unwrap() else {
        panic!("rows");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(90)]]);
}

#[test]
fn online_type_copy_partition_migrations_and_recovery() {
    for partition in ["PARTITION BY HASH(id) PARTITIONS 4", "PARTITION BY RANGE(id) SUBPARTITION BY HASH(id) SUBPARTITIONS 2 (PARTITION p0 VALUES LESS THAN (4), PARTITION p1 VALUES LESS THAN MAXVALUE)"] {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_strict_test_engine(dir.path()));
    engine.execute(&format!("CREATE TABLE t(id INT PRIMARY KEY, v INT, payload LONGTEXT, INDEX v_idx(v)) {partition}")).unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,10,REPEAT('a',20000)),(2,20,'b'),(3,30,'c')")
        .unwrap();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 7881;
        worker.execute_with_session(
            &mut session,
            "ALTER TABLE t MODIFY id BIGINT, MODIFY v BIGINT, LOCK=NONE",
        )
    });
    let reached = hook.wait_until_reached(Duration::from_secs(5));
    let mut writer = SessionContext::default();
    writer.connection_id = 7882;
    let writes = [
        "UPDATE t SET id=8 WHERE id=1",
        "UPDATE t SET v=99 WHERE id=2",
        "DELETE FROM t WHERE id=3",
        "INSERT INTO t VALUES(4,40,REPEAT('d',24000))",
    ]
    .map(|sql| engine.execute_with_session(&mut writer, sql));
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "{result:?}");
    for write in writes {
        write.unwrap();
    }
    result.unwrap();
    let expected = vec![
        vec![Value::Int(2), Value::Int(99), Value::Int(1)],
        vec![Value::Int(4), Value::Int(40), Value::Int(24000)],
        vec![Value::Int(8), Value::Int(10), Value::Int(20000)],
    ];
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT id,v,LENGTH(payload) FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("rows");
    };
    assert_eq!(rows.rows, expected);
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT id,v,LENGTH(payload) FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("rows");
    };
    assert_eq!(rows.rows, expected);
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT id FROM t FORCE INDEX(v_idx) WHERE v=99")
        .unwrap()
    else {
        panic!("rows");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(2)]]);
    }
}

#[test]
fn online_type_copy_matches_exclusive_across_types() {
    let cases = [
        ("INT", "BIGINT", "-10,123"),
        ("BIGINT UNSIGNED", "VARCHAR(32)", "18446744073709551615,123"),
        ("VARCHAR(40)", "DECIMAL(30,6)", "'1234.500','-0.125'"),
        (
            "VARCHAR(40)",
            "DATETIME(6)",
            "'2024-01-02 03:04:05.123456','2025-02-03 04:05:06.000001'",
        ),
        ("DECIMAL(20,4)", "DOUBLE", "1234.5000,-0.1250"),
        ("VARBINARY(32)", "BLOB", "'abc','def'"),
        ("ENUM('red','blue')", "VARCHAR(32)", "'red','blue'"),
        ("TINYINT", "TINYINT UNSIGNED", "10,123"),
        ("DATE", "DATETIME", "'2024-01-02','2025-02-03'"),
    ];
    for (source, target, values) in cases {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_relaxed_test_engine(dir.path());
        engine
            .execute(&format!(
                "CREATE TABLE t(id INT PRIMARY KEY,v {source},INDEX v_idx(v))"
            ))
            .unwrap();
        let values = values
            .split(',')
            .enumerate()
            .map(|(i, v)| format!("({},{v})", i + 1))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO t VALUES {values}"))
            .unwrap();
        engine.execute("CREATE TABLE reference LIKE t").unwrap();
        engine
            .execute("INSERT INTO reference SELECT * FROM t")
            .unwrap();
        let online = engine.execute(&format!("ALTER TABLE t MODIFY v {target}"));
        let offline = engine.execute(&format!(
            "ALTER TABLE reference MODIFY v {target},LOCK=EXCLUSIVE"
        ));
        match (online, offline) {
            (Ok(_), Ok(_)) => {
                let QueryResponse::ResultSet(actual) =
                    engine.execute("SELECT id,v FROM t ORDER BY id").unwrap()
                else {
                    panic!("rows")
                };
                let QueryResponse::ResultSet(expected) = engine
                    .execute("SELECT id,v FROM reference ORDER BY id")
                    .unwrap()
                else {
                    panic!("rows")
                };
                assert_eq!(actual.rows, expected.rows, "{source} -> {target}");
                assert_online_index_matches_heap(&engine, "v_idx");
            }
            (Err(a), Err(b)) => assert_eq!(a.code(), b.code(), "{source} -> {target}"),
            results => panic!("{source} -> {target}: {results:?}"),
        }
    }
}

#[test]
fn online_type_copy_without_primary_key() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_relaxed_test_engine(dir.path()));
    engine.execute("CREATE TABLE t(v INT)").unwrap();
    engine.execute("INSERT INTO t VALUES(1),(1),(2)").unwrap();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut s = SessionContext::default();
        s.connection_id = 7901;
        worker.execute_with_session(&mut s, "ALTER TABLE t MODIFY v BIGINT")
    });
    let reached = hook.wait_until_reached(Duration::from_secs(5));
    let mut writer = SessionContext::default();
    writer.connection_id = 7902;
    let writes = [
        "UPDATE t SET v=10 WHERE v=1 LIMIT 1",
        "DELETE FROM t WHERE v=2",
        "INSERT INTO t VALUES(3)",
    ]
    .map(|sql| engine.execute_with_session(&mut writer, sql));
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "{result:?}");
    for write in writes {
        write.unwrap();
    }
    result.unwrap();
    let QueryResponse::ResultSet(rows) = engine.execute("SELECT v FROM t ORDER BY v").unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(3)],
            vec![Value::Int(10)]
        ]
    );
}

#[test]
fn online_type_copy_cancel_releases_locks_and_preserves_data() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_relaxed_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,10)").unwrap();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 7911;
        worker.execute_with_session(&mut session, "ALTER TABLE t MODIFY v BIGINT")
    });
    let reached = hook.wait_until_reached(Duration::from_secs(5));
    let jobs = engine.database().alter_copy_job_snapshots();
    if let Some(job) = jobs.iter().find(|job| job.table == "t") {
        engine
            .execute(&format!("CANCEL DDL JOB {}", job.job_id))
            .unwrap();
    }
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "{result:?}");
    assert_eq!(result.unwrap_err().code(), 1317);
    assert_eq!(
        engine.database().table("test.t").unwrap().columns[1].data_type,
        crate::storage::DataTypeKind::Int
    );
    engine.execute("INSERT INTO t VALUES(2,20)").unwrap();
    engine.execute("ALTER TABLE t MODIFY v BIGINT").unwrap();
    let QueryResponse::ResultSet(rows) = engine.execute("SELECT id,v FROM t ORDER BY id").unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)]
        ]
    );
}

#[test]
fn online_check_scan_allows_concurrent_read_and_write() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_relaxed_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,10)").unwrap();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 7901;
        worker.execute_with_session(
            &mut session,
            "ALTER TABLE t ADD CONSTRAINT positive CHECK(v>0), LOCK=NONE",
        )
    });
    let reached = hook.wait_until_reached(Duration::from_secs(5));
    let worker = engine.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 7902;
        let result = worker
            .execute_with_session(&mut session, "SELECT * FROM t")
            .and_then(|_| worker.execute_with_session(&mut session, "INSERT INTO t VALUES(2,20)"));
        tx.send(result).unwrap();
    });
    let concurrent = rx.recv_timeout(Duration::from_secs(2));
    hook.release();
    let result = ddl.join().unwrap();
    writer.join().unwrap();
    assert!(reached, "scan hook was not reached: {result:?}");
    result.unwrap();
    concurrent
        .expect("CHECK scan blocked concurrent DML")
        .unwrap();
    assert!(engine.execute("INSERT INTO t VALUES(3,-1)").is_err());
}

fn run_online_constraint_interleaving(
    setup: &[&str],
    alter: &str,
    writes: &[&str],
    expected_error: Option<u16>,
    verify: impl FnOnce(&StorageQueryEngine),
) {
    run_online_constraint_interleaving_with_limit(
        setup,
        alter,
        writes,
        expected_error,
        usize::MAX,
        verify,
    );
}

fn run_online_constraint_interleaving_with_limit(
    setup: &[&str],
    alter: &str,
    writes: &[&str],
    expected_error: Option<u16>,
    limit: usize,
    verify: impl FnOnce(&StorageQueryEngine),
) {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_strict_test_engine(dir.path()));
    for sql in setup {
        engine.execute(sql).unwrap();
    }
    let hook = engine
        .database()
        .install_online_index_validation_test_hook();
    let worker = engine.clone();
    let alter = alter.to_string();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 7911;
        worker
            .database()
            .with_constraint_validation_limit_for_test(limit, || {
                worker.execute_with_session(&mut session, &alter)
            })
    });
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let mut writer = SessionContext::default();
    writer.connection_id = 7912;
    let results = writes
        .iter()
        .map(|sql| engine.execute_with_session(&mut writer, sql))
        .collect::<Vec<_>>();
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "validation hook not reached: {result:?}");
    for (sql, result) in writes.iter().zip(results) {
        result.unwrap_or_else(|err| panic!("{sql}: {err:?}"));
    }
    match expected_error {
        Some(code) => assert_eq!(result.unwrap_err().code(), code),
        None => {
            result.unwrap();
        }
    }
    verify(&engine);
    drop(engine);
    let reopened = open_strict_cold_metadata_engine(dir.path());
    reopened.execute("SELECT * FROM t").unwrap();
}

#[test]
fn online_check_revalidates_commits_and_keeps_compound_alter_atomic() {
    run_online_constraint_interleaving(
        &[
            "CREATE TABLE t(id INT PRIMARY KEY, v INT)",
            "INSERT INTO t VALUES(1,10)",
        ],
        "ALTER TABLE t ADD CONSTRAINT a CHECK(v>0), ADD CONSTRAINT b CHECK(v<100), LOCK=NONE",
        &["INSERT INTO t VALUES(2,-1)"],
        Some(3819),
        |engine| {
            assert!(engine
                .database()
                .table("test.t")
                .unwrap()
                .check_constraints
                .is_empty());
        },
    );
}

#[test]
fn online_check_accepts_repairs_deletes_and_savepoint_rollback() {
    run_online_constraint_interleaving(
        &[
            "CREATE TABLE t(id INT PRIMARY KEY, v INT)",
            "INSERT INTO t VALUES(1,-1),(2,-2),(3,3)",
        ],
        "ALTER TABLE t ADD CONSTRAINT a CHECK(v>0), LOCK=NONE",
        &[
            "UPDATE t SET v=1 WHERE id=1",
            "DELETE FROM t WHERE id=2",
            "BEGIN",
            "SAVEPOINT s",
            "UPDATE t SET v=-3 WHERE id=3",
            "ROLLBACK TO SAVEPOINT s",
            "COMMIT",
            "BEGIN",
            "INSERT INTO t VALUES(4,-4)",
            "ROLLBACK",
        ],
        None,
        |engine| {
            assert!(engine.execute("UPDATE t SET v=-1 WHERE id=1").is_err());
        },
    );
}

#[test]
fn online_check_enable_and_partition_row_identity() {
    run_online_constraint_interleaving(
        &["CREATE TABLE t(id INT, v INT, CONSTRAINT a CHECK(v>0) NOT ENFORCED) PARTITION BY HASH(id) PARTITIONS 2",
            "INSERT INTO t VALUES(1,-1),(2,-2)"],
        "ALTER TABLE t ALTER CHECK a ENFORCED, LOCK=NONE",
        &["UPDATE t SET v=1 WHERE id=1", "UPDATE t SET v=2 WHERE id=2", "INSERT INTO t VALUES(3,3)"], None,
        |engine| { assert!(engine.execute("INSERT INTO t VALUES(4,-4)").is_err()); });
}

#[test]
fn online_fk_detects_parent_delete_and_removes_candidate_index() {
    run_online_constraint_interleaving(
        &[
            "CREATE TABLE p(id INT PRIMARY KEY)",
            "INSERT INTO p VALUES(1)",
            "CREATE TABLE t(id INT PRIMARY KEY, pid INT)",
            "INSERT INTO t VALUES(1,1)",
        ],
        "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY(pid) REFERENCES p(id), LOCK=NONE",
        &["DELETE FROM p WHERE id=1"],
        Some(1452),
        |engine| {
            let table = engine.database().table("test.t").unwrap();
            assert!(table.foreign_keys.is_empty());
            assert_eq!(table.indexes.len(), 1);
        },
    );
}

#[test]
fn online_fk_tracks_both_sides_and_publishes_backing_index() {
    run_online_constraint_interleaving(
        &[
            "CREATE TABLE p(id INT PRIMARY KEY)",
            "INSERT INTO p VALUES(1)",
            "CREATE TABLE t(id INT PRIMARY KEY, pid INT)",
            "INSERT INTO t VALUES(1,1),(2,2)",
        ],
        "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY(pid) REFERENCES p(id), LOCK=NONE",
        &[
            "INSERT INTO p VALUES(2)",
            "BEGIN",
            "DELETE FROM p WHERE id=1",
            "ROLLBACK",
            "INSERT INTO p VALUES(3)",
            "UPDATE t SET pid=3 WHERE id=1",
            "DELETE FROM p WHERE id=1",
        ],
        None,
        |engine| {
            let table = engine.database().table("test.t").unwrap();
            assert_eq!(table.foreign_keys.len(), 1);
            assert!(table
                .indexes
                .iter()
                .all(|i| i.state == crate::storage::IndexState::Public));
            assert_eq!(
                engine
                    .execute("DELETE FROM p WHERE id=2")
                    .unwrap_err()
                    .code(),
                1451
            );
            assert_eq!(
                engine
                    .execute("INSERT INTO t VALUES(3,99)")
                    .unwrap_err()
                    .code(),
                1452
            );
        },
    );
}

#[test]
fn online_fk_self_reference_supports_null_and_key_updates() {
    run_online_constraint_interleaving(
        &[
            "CREATE TABLE t(id INT PRIMARY KEY, pid INT)",
            "INSERT INTO t VALUES(1,NULL),(2,1)",
        ],
        "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY(pid) REFERENCES t(id), LOCK=NONE",
        &["INSERT INTO t VALUES(3,2)", "UPDATE t SET pid=3 WHERE id=2"],
        None,
        |engine| {
            assert_eq!(
                engine
                    .execute("DELETE FROM t WHERE id=3")
                    .unwrap_err()
                    .code(),
                1451
            );
        },
    );
}

#[test]
fn online_fk_rejects_nonunique_parent_without_candidate_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_relaxed_test_engine(dir.path());
    engine
        .execute("CREATE TABLE p(id INT PRIMARY KEY, k INT, INDEX(k))")
        .unwrap();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY, pid INT)")
        .unwrap();
    assert_eq!(
        engine
            .execute("ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY(pid) REFERENCES p(k), LOCK=NONE")
            .unwrap_err()
            .code(),
        1822
    );
    let table = engine.database().table("test.t").unwrap();
    assert!(table.foreign_keys.is_empty());
    assert_eq!(table.indexes.len(), 1);
}

#[test]
fn online_fk_compound_replacement_keeps_old_constraint_until_publish() {
    run_online_constraint_interleaving(
        &["CREATE TABLE p(id INT PRIMARY KEY)", "INSERT INTO p VALUES(1)",
            "CREATE TABLE t(id INT PRIMARY KEY, pid INT, CONSTRAINT fk FOREIGN KEY(pid) REFERENCES p(id))",
            "INSERT INTO t VALUES(1,1)"],
        "ALTER TABLE t DROP FOREIGN KEY fk, ADD CONSTRAINT fk FOREIGN KEY(pid) REFERENCES p(id) ON DELETE CASCADE, LOCK=NONE",
        &["INSERT INTO t VALUES(2,1)"], None, |engine| {
            engine.execute("DELETE FROM p WHERE id=1").unwrap();
            assert!(engine.database().scan_rows("test.t", crate::storage::TxnView::read_only(u64::MAX,
                crate::storage::IsolationLevel::ReadCommitted)).unwrap().is_empty());
        });
}

#[test]
fn online_constraint_metadata_only_and_unsupported_mix() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_relaxed_test_engine(dir.path());
    engine.execute("CREATE TABLE t(v INT)").unwrap();
    engine.execute("INSERT INTO t VALUES(-1)").unwrap();
    engine
        .execute("ALTER TABLE t ADD CONSTRAINT a CHECK(v>0) NOT ENFORCED, ALGORITHM=INSTANT")
        .unwrap();
    engine
        .execute("ALTER TABLE t ALTER CHECK a NOT ENFORCED, LOCK=NONE")
        .unwrap();
    engine
        .execute("ALTER TABLE t DROP CHECK a, LOCK=NONE")
        .unwrap();
    assert_eq!(
        engine
            .execute("ALTER TABLE t ADD COLUMN c INT, ADD CHECK(v>0), LOCK=NONE")
            .unwrap_err()
            .code(),
        1846
    );
    assert_eq!(engine.database().table("test.t").unwrap().columns.len(), 1);
}

#[test]
fn online_constraint_memory_failure_preserves_old_schema_and_cleans_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_relaxed_test_engine(dir.path());
    engine
        .execute("CREATE TABLE p(id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO p VALUES(1)").unwrap();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY, pid INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,1)").unwrap();
    let result = engine.database().with_constraint_validation_limit_for_test(0, || {
        engine.execute("ALTER TABLE t ADD CONSTRAINT a CHECK(pid>0), ADD CONSTRAINT fk FOREIGN KEY(pid) REFERENCES p(id), LOCK=NONE")
    });
    assert_eq!(result.unwrap_err().code(), 1037);
    let table = engine.database().table("test.t").unwrap();
    assert!(table.check_constraints.is_empty());
    assert!(table.foreign_keys.is_empty());
    assert_eq!(table.indexes.len(), 1);
    engine.execute("INSERT INTO t VALUES(2,-1)").unwrap();
    assert_eq!(engine.database().active_snapshot_count(), 0);
}

#[test]
fn online_fk_composite_collation_decimal_and_null() {
    run_online_constraint_interleaving(
        &["CREATE TABLE p(code VARCHAR(20) COLLATE utf8mb4_general_ci, n DECIMAL(10,2), PRIMARY KEY(code,n))",
            "INSERT INTO p VALUES('AbC',1.25)",
            "CREATE TABLE t(id INT PRIMARY KEY, code VARCHAR(20) COLLATE utf8mb4_general_ci, n DECIMAL(10,2))",
            "INSERT INTO t VALUES(1,'abc',1.25),(2,NULL,3.75)"],
        "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY(code,n) REFERENCES p(code,n), LOCK=NONE",
        &["INSERT INTO p VALUES('DEF',2.50)", "INSERT INTO t VALUES(3,'def',2.50)"], None,
        |engine| { assert_eq!(engine.execute("DELETE FROM p WHERE code='ABC'").unwrap_err().code(), 1451); });
}

#[test]
fn online_constraint_cancel_and_purge_during_scan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_relaxed_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,-1)").unwrap();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 7931;
        worker.execute_with_session(&mut session, "ALTER TABLE t ADD CHECK(v>0), LOCK=NONE")
    });
    let reached = hook.wait_until_reached(Duration::from_secs(5));
    assert!(engine.database().active_snapshot_count() > 0);
    let mut writer = SessionContext::default();
    writer.connection_id = 7932;
    let update = engine.execute_with_session(&mut writer, "UPDATE t SET v=1");
    engine.database().purge_obsolete_undo();
    let jobs = engine.database().alter_copy_job_snapshots();
    let cancel = jobs
        .first()
        .map(|job| engine.database().cancel_ddl_job(job.job_id));
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached);
    update.unwrap();
    cancel.unwrap().unwrap();
    assert_eq!(result.unwrap_err().code(), 1317);
    assert!(engine
        .database()
        .table("test.t")
        .unwrap()
        .check_constraints
        .is_empty());
    assert_eq!(engine.database().active_snapshot_count(), 0);
    engine
        .execute("ALTER TABLE t ADD CHECK(v>0), LOCK=NONE")
        .unwrap();
}

#[test]
fn online_constraint_shared_lock_allows_reads_and_blocks_writes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_relaxed_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,1)").unwrap();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 7941;
        worker.execute_with_session(&mut session, "ALTER TABLE t ADD CHECK(v>0), LOCK=SHARED")
    });
    let reached = hook.wait_until_reached(Duration::from_secs(5));
    let mut writer = SessionContext::default();
    writer.connection_id = 7942;
    let read = engine.execute_with_session(&mut writer, "SELECT * FROM t");
    let write = engine.execute_with_session(&mut writer, "INSERT INTO t VALUES(2,2)");
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "{result:?}");
    read.unwrap();
    assert_eq!(write.unwrap_err().code(), 1205);
    result.unwrap();
}

#[test]
fn online_constraint_rejects_stale_bound_dml_and_recovers_enforcement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    let stale = engine.database().table("test.t").unwrap();
    engine
        .execute("ALTER TABLE t ADD CHECK(v>0), LOCK=NONE")
        .unwrap();
    let txn = engine.database().begin_txn().unwrap();
    let error = engine
        .database()
        .insert_bound_primary_test_row(txn, &stale, vec![Value::Int(1), Value::Int(-1)])
        .unwrap_err();
    engine.database().rollback_txn(txn.txn_id).unwrap();
    assert_eq!(error.code(), 1412);
    drop(engine);
    let engine = open_strict_cold_metadata_engine(dir.path());
    assert_eq!(
        engine
            .execute("INSERT INTO t VALUES(1,-1)")
            .unwrap_err()
            .code(),
        3819
    );
}

#[test]
fn online_fk_identical_replacement_revalidates_and_preserves_original_on_failure() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_relaxed_test_engine(dir.path());
    let mut session = SessionContext::default();
    for statement in [
        "CREATE TABLE p(id INT PRIMARY KEY)",
        "CREATE TABLE t(id INT PRIMARY KEY, pid INT, CONSTRAINT fk FOREIGN KEY(pid) REFERENCES p(id))",
        "SET foreign_key_checks=0", "INSERT INTO t VALUES(1,99)", "SET foreign_key_checks=1",
    ] { engine.execute_with_session(&mut session, statement).unwrap(); }
    assert_eq!(engine.execute_with_session(&mut session,
        "ALTER TABLE t DROP FOREIGN KEY fk, ADD CONSTRAINT fk FOREIGN KEY(pid) REFERENCES p(id), LOCK=NONE")
        .unwrap_err().code(), 1452);
    let table = engine.database().table("test.t").unwrap();
    assert_eq!(table.foreign_keys.len(), 1);
    assert_eq!(table.foreign_keys[0].name, "fk");
    assert_eq!(
        engine
            .execute("ALTER TABLE t ADD CONSTRAINT FK FOREIGN KEY(pid) REFERENCES p(id), LOCK=NONE")
            .unwrap_err()
            .code(),
        1826
    );
}

#[test]
fn online_constraint_autocommit_cached_point_writer_rebinds_after_publication() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_relaxed_test_engine(dir.path());
    engine
        .execute("CREATE TABLE p(id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO p VALUES(1)").unwrap();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY, pid INT, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,1,10)").unwrap();
    let mut session = SessionContext::default();
    session.connection_id = 7951;
    let compile = |sql| {
        let statement = std::sync::Arc::new(crate::parser::parse_statement(sql).unwrap());
        crate::engine::executor::prepared_dml::compile_prepared_dml(
            engine.database(),
            &session,
            &statement,
            engine.prepared_dml_cache.current_epoch(),
        )
        .unwrap()
    };
    let increment = compile("UPDATE test.t SET v=v+1 WHERE id=1");
    let invalid_fk = compile("UPDATE test.t SET pid=99, v=v+1 WHERE id=1");
    let insert = compile("INSERT INTO test.t VALUES(2,99,1)");
    assert!(increment.point_write.is_some());
    assert!(invalid_fk.point_write.is_some());
    // Warm the validated-publication fast path before changing the schema.
    engine
        .execute_prepared_dml_core(&mut session, &increment, &[], Instant::now())
        .0
        .unwrap();
    engine.execute("ALTER TABLE t ADD CHECK(v>0), ADD CONSTRAINT fk FOREIGN KEY(pid) REFERENCES p(id), LOCK=NONE").unwrap();
    engine
        .execute_prepared_dml_core(&mut session, &increment, &[], Instant::now())
        .0
        .unwrap();
    assert_eq!(
        engine
            .execute_prepared_dml_core(&mut session, &invalid_fk, &[], Instant::now())
            .0
            .unwrap_err()
            .code(),
        1452
    );
    let QueryResponse::ResultSet(rows) = engine.execute("SELECT pid,v FROM t").unwrap() else {
        panic!("rows");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(1), Value::Int(12)]]);
    assert_eq!(
        engine
            .execute_prepared_dml_core(&mut session, &insert, &[], Instant::now())
            .0
            .unwrap_err()
            .code(),
        1452
    );
    assert!(session.current_txn.is_none());
}

#[test]
fn online_fk_cold_restart_resolves_parent_and_child_descriptors() {
    for cascade in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        {
            let engine = open_strict_test_engine(dir.path());
            engine
                .execute("CREATE TABLE p(id INT PRIMARY KEY)")
                .unwrap();
            engine
                .execute("CREATE TABLE t(id INT PRIMARY KEY, pid INT)")
                .unwrap();
            engine.execute("INSERT INTO p VALUES(1)").unwrap();
            engine.execute("INSERT INTO t VALUES(1,1)").unwrap();
            engine
                .database()
                .force_cold_online_descriptor_for_test("test.p");
            engine.execute(&format!("ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY(pid) REFERENCES p(id) {}, LOCK=NONE",
                if cascade { "ON DELETE CASCADE" } else { "" })).unwrap();
        }
        {
            let engine = open_strict_cold_metadata_engine(dir.path());
            // The parent descriptor has never been queried after restart.
            assert_eq!(
                engine
                    .execute("INSERT INTO t VALUES(2,99)")
                    .unwrap_err()
                    .code(),
                1452
            );
        }
        let engine = open_strict_cold_metadata_engine(dir.path());
        // Conversely, parent-side enforcement must load the cold child.
        let result = engine.execute("DELETE FROM p WHERE id=1");
        if cascade {
            result.unwrap();
            let QueryResponse::ResultSet(rows) = engine.execute("SELECT COUNT(*) FROM t").unwrap()
            else {
                panic!("rows");
            };
            assert_eq!(rows.rows[0][0].to_display_string(), "0");
        } else {
            assert_eq!(result.unwrap_err().code(), 1451);
        }
    }
}

#[test]
fn online_fk_publish_preserves_references_from_other_cold_children() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    for sql in [
        "CREATE TABLE p(id INT PRIMARY KEY)",
        "CREATE TABLE t(id INT PRIMARY KEY, pid INT)",
        "CREATE TABLE c(id INT PRIMARY KEY, pid INT, FOREIGN KEY(pid) REFERENCES p(id))",
        "INSERT INTO p VALUES(1),(2)",
        "INSERT INTO c VALUES(1,1)",
        "INSERT INTO t VALUES(2,2)",
    ] {
        engine.execute(sql).unwrap();
    }
    engine
        .database()
        .force_cold_online_descriptor_for_test("test.c");
    engine
        .execute("ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY(pid) REFERENCES p(id), LOCK=NONE")
        .unwrap();
    assert_eq!(
        engine
            .execute("DELETE FROM p WHERE id=1")
            .unwrap_err()
            .code(),
        1451
    );
    assert_eq!(
        engine
            .execute("DELETE FROM p WHERE id=2")
            .unwrap_err()
            .code(),
        1451
    );
}

#[test]
fn online_fk_validation_spills_under_small_budget() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_relaxed_test_engine(dir.path());
    engine
        .execute("CREATE TABLE spill_parent(a INT, b INT, PRIMARY KEY(a,b))")
        .unwrap();
    engine
        .execute("CREATE TABLE spill_child(id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    let parent = (0..4000)
        .map(|i| format!("({},{})", i / 100, i))
        .collect::<Vec<_>>()
        .join(",");
    let child = (0..8000)
        .map(|i| format!("({},{},{})", i, (i % 4000) / 100, i % 4000))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO spill_parent VALUES {parent}"))
        .unwrap();
    engine
        .execute(&format!("INSERT INTO spill_child VALUES {child}"))
        .unwrap();
    engine.database().with_constraint_validation_limit_for_test(128 * 1024, || {
        engine.execute("ALTER TABLE spill_child ADD CONSTRAINT fk_spill FOREIGN KEY(a,b) REFERENCES spill_parent(a,b), LOCK=NONE")
    }).unwrap();
    assert_eq!(
        engine
            .execute("INSERT INTO spill_child VALUES(9000,999,999)")
            .unwrap_err()
            .code(),
        1452
    );
    engine
        .execute("ALTER TABLE spill_child DROP FOREIGN KEY fk_spill")
        .unwrap();
    engine
        .execute("INSERT INTO spill_child VALUES(9000,999,999)")
        .unwrap();
    let err = engine.database().with_constraint_validation_limit_for_test(128 * 1024, || {
        engine.execute("ALTER TABLE spill_child ADD CONSTRAINT fk_spill FOREIGN KEY(a,b) REFERENCES spill_parent(a,b), LOCK=NONE")
    }).unwrap_err();
    assert_eq!(err.code(), 1452);
    assert!(engine
        .database()
        .table("test.spill_child")
        .unwrap()
        .foreign_keys
        .is_empty());
    assert_eq!(engine.database().active_snapshot_count(), 0);
}

#[test]
fn online_fk_spilled_counts_replay_collation_nulls_and_rollback() {
    run_online_constraint_interleaving_with_limit(
        &["CREATE TABLE p(code VARCHAR(20) COLLATE utf8mb4_general_ci, n DECIMAL(40,2), PRIMARY KEY(code,n))",
          "INSERT INTO p VALUES('AbC',1.25),('old',2.50)",
          "CREATE TABLE t(id INT PRIMARY KEY, code VARCHAR(20) COLLATE utf8mb4_general_ci, n DECIMAL(40,2))",
          "INSERT INTO t VALUES(1,'abc',1.25),(2,NULL,3.75),(3,'OLD',2.50)"],
        "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY(code,n) REFERENCES p(code,n), LOCK=NONE",
        &["BEGIN", "DELETE FROM p WHERE code='AbC'", "ROLLBACK",
          "UPDATE p SET code='new' WHERE code='old'", "UPDATE t SET code='NEW' WHERE id=3",
          "INSERT INTO p VALUES('DEF',2.50)", "INSERT INTO t VALUES(4,'def',2.50)"],
        None, 16 * 1024,
        |engine| { assert_eq!(engine.execute("DELETE FROM p WHERE code='ABC'").unwrap_err().code(), 1451); },
    );
}

#[test]
fn online_fk_spilled_counts_detect_parent_delete() {
    run_online_constraint_interleaving_with_limit(
        &[
            "CREATE TABLE p(id BIGINT UNSIGNED PRIMARY KEY)",
            "INSERT INTO p VALUES(18446744073709551615)",
            "CREATE TABLE t(id INT PRIMARY KEY, pid BIGINT UNSIGNED)",
            "INSERT INTO t VALUES(1,18446744073709551615)",
        ],
        "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY(pid) REFERENCES p(id), LOCK=NONE",
        &["DELETE FROM p"],
        Some(1452),
        16 * 1024,
        |engine| {
            assert!(engine
                .database()
                .table("test.t")
                .unwrap()
                .foreign_keys
                .is_empty());
        },
    );
}

#[test]
fn online_fk_auto_analyze_does_not_invalidate_validation() {
    for analyze_already_running in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = std::sync::Arc::new(open_relaxed_test_engine(dir.path()));
        for sql in [
            "CREATE TABLE p(id INT PRIMARY KEY)",
            "CREATE TABLE t(id INT PRIMARY KEY, pid INT)",
            "CREATE TABLE unrelated(id INT PRIMARY KEY)",
            "INSERT INTO p VALUES(1),(2)",
            "INSERT INTO t VALUES(1,1),(2,2)",
            "INSERT INTO unrelated VALUES(1)",
        ] {
            engine.execute(sql).unwrap();
        }
        let analyze_hook =
            analyze_already_running.then(|| engine.database().install_analyze_test_hook());
        let analyze = analyze_hook.as_ref().map(|hook| {
            let worker = engine.clone();
            let task = std::thread::spawn(move || worker.database().auto_analyze_stale_tables(0.1));
            assert!(hook.wait_until_reached(Duration::from_secs(10)));
            task
        });
        let hook = engine
            .database()
            .install_online_index_validation_test_hook();
        let worker = engine.clone();
        let ddl = std::thread::spawn(move || {
            let mut session = SessionContext::default();
            session.connection_id = 8921;
            worker.execute_with_session(
                &mut session,
                "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY(pid) REFERENCES p(id), LOCK=NONE",
            )
        });
        let reached = hook.wait_until_reached(Duration::from_secs(10));
        let refreshed = if let Some(analyze) = analyze {
            analyze_hook.unwrap().release();
            analyze.join().unwrap()
        } else {
            engine.database().auto_analyze_stale_tables(0.1)
        };
        hook.release();
        let result = ddl.join().unwrap();
        assert!(reached);
        result.unwrap();
        assert_eq!(
            refreshed.unwrap(),
            1,
            "only the unrelated table may refresh"
        );
        assert!(engine.database().auto_analyze_stale_tables(0.1).unwrap() >= 2);
        assert_eq!(
            engine
                .execute("INSERT INTO t VALUES(3,99)")
                .unwrap_err()
                .code(),
            1452
        );
        assert_eq!(
            engine
                .execute("DELETE FROM p WHERE id=1")
                .unwrap_err()
                .code(),
            1451
        );
        assert_eq!(engine.database().active_snapshot_count(), 0);
    }
}

#[test]
fn online_fk_covering_merge_existing_and_private_indexes() {
    for backing in ["", ", INDEX backing(a,b,id)"] {
        for orphan in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let engine = open_relaxed_test_engine(dir.path());
            engine.execute("CREATE TABLE p(a BIGINT UNSIGNED,b VARCHAR(30) COLLATE utf8mb4_general_ci, PRIMARY KEY(a,b))").unwrap();
            engine.execute(&format!("CREATE TABLE t(id INT PRIMARY KEY,a BIGINT UNSIGNED,b VARCHAR(30) COLLATE utf8mb4_general_ci{backing})")).unwrap();
            engine
                .execute("INSERT INTO p VALUES(18446744073709551615,'Hello'),(2,'two')")
                .unwrap();
            engine.execute("INSERT INTO t VALUES(1,18446744073709551615,'hello'),(2,2,'TWO'),(3,NULL,'missing'),(4,999,NULL),(5,2,'two')").unwrap();
            if orphan {
                engine
                    .execute("INSERT INTO t VALUES(6,3,'missing')")
                    .unwrap();
            }
            let result = engine.execute(
                "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY(a,b) REFERENCES p(a,b), LOCK=NONE",
            );
            assert!(
                engine
                    .database()
                    .last_constraint_used_index_merge_for_test(),
                "{result:?}"
            );
            if orphan {
                assert_eq!(result.unwrap_err().code(), 1452);
            } else {
                result.unwrap();
                assert_eq!(
                    engine
                        .execute("DELETE FROM p WHERE a=2")
                        .unwrap_err()
                        .code(),
                    1451
                );
            }
            assert_eq!(engine.database().active_snapshot_count(), 0);
        }
    }
}

#[test]
fn online_fk_snapshot_baseline_preserves_duplicates_nulls_and_collation() {
    run_online_constraint_interleaving(
        &[
            "CREATE TABLE p(a VARCHAR(20) COLLATE utf8mb4_general_ci,b DECIMAL(30,3),PRIMARY KEY(a,b))",
            "CREATE TABLE t(id INT PRIMARY KEY,a VARCHAR(20) COLLATE utf8mb4_general_ci,b DECIMAL(30,3),INDEX backing(a,b))",
            "INSERT INTO p VALUES('same',1.250),('old',2.000)",
            "INSERT INTO t VALUES(1,'SAME',1.250),(2,'Same',1.250),(3,'old',2),(4,NULL,9),(5,'repair',3)",
        ],
        "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY(a,b) REFERENCES p(a,b), LOCK=NONE",
        &[
            "INSERT INTO p VALUES('REPAIR',3)",
            "DELETE FROM t WHERE id=1",
            "UPDATE p SET a='new' WHERE a='old'",
            "UPDATE t SET a='NEW' WHERE id=3",
            "BEGIN", "SAVEPOINT s", "DELETE FROM p WHERE a='same'", "ROLLBACK TO SAVEPOINT s", "COMMIT",
        ],
        None,
        |engine| { assert_eq!(engine.execute("DELETE FROM p WHERE a='same'").unwrap_err().code(),1451); },
    );
}

#[test]
fn online_fk_merge_certificate_rejects_changed_existing_index() {
    run_online_constraint_interleaving(
        &[
            "CREATE TABLE p(id INT PRIMARY KEY)",
            "CREATE TABLE t(id INT PRIMARY KEY,pid INT,INDEX backing(pid))",
            "INSERT INTO p VALUES(1),(2)",
            "INSERT INTO t VALUES(1,1),(2,2)",
        ],
        "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY(pid) REFERENCES p(id), LOCK=NONE",
        &["UPDATE t SET pid=77 WHERE id=2"],
        Some(1452),
        |engine| {
            assert!(engine
                .database()
                .table("test.t")
                .unwrap()
                .foreign_keys
                .is_empty());
        },
    );
}

#[test]
fn online_fk_baseline_external_sort_retains_repeated_references() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_relaxed_test_engine(dir.path());
    engine
        .execute("CREATE TABLE p(id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,pid INT)")
        .unwrap();
    for start in (0..24000).step_by(1000) {
        let parent = (start..start + 1000)
            .map(|i| format!("({i})"))
            .collect::<Vec<_>>()
            .join(",");
        let child = (start..start + 1000)
            .map(|i| format!("({i},{})", i / 2))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO p VALUES {parent}"))
            .unwrap();
        engine
            .execute(&format!("INSERT INTO t VALUES {child}"))
            .unwrap();
    }
    // CHECK forces the general snapshot path; the 48k records exceed a sort run.
    engine.execute("ALTER TABLE t ADD CHECK(id>=0), ADD CONSTRAINT fk FOREIGN KEY(pid) REFERENCES p(id), LOCK=NONE").unwrap();
    assert!(!engine
        .database()
        .last_constraint_used_index_merge_for_test());
    assert_eq!(
        engine
            .execute("DELETE FROM p WHERE id=2")
            .unwrap_err()
            .code(),
        1451
    );
    engine.execute("DELETE FROM t WHERE id IN (4,5)").unwrap();
    engine.execute("DELETE FROM p WHERE id=2").unwrap();
}

#[test]
fn online_fk_snapshot_baseline_allows_sustained_key_writes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_relaxed_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE p(id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,pid INT,INDEX backing(pid))")
        .unwrap();
    for start in (0..8000).step_by(1000) {
        let parent = (start..start + 1000)
            .map(|i| format!("({i})"))
            .collect::<Vec<_>>()
            .join(",");
        let child = (start..start + 1000)
            .map(|i| format!("({i},{i})"))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO p VALUES {parent}"))
            .unwrap();
        engine
            .execute(&format!("INSERT INTO t VALUES {child}"))
            .unwrap();
    }
    let hook = engine
        .database()
        .install_online_index_validation_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 8991;
        worker.execute_with_session(
            &mut session,
            "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY(pid) REFERENCES p(id), LOCK=NONE",
        )
    });
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    // Invalidate the speculative merge, then keep writing while the initial
    // MVCC baseline is projected, sorted and merged outside the table locks.
    let mut writer = SessionContext::default();
    writer.connection_id = 8992;
    let first = engine.execute_with_session(&mut writer, "UPDATE t SET pid=1 WHERE id=0");
    hook.release();
    assert!(reached);
    first.unwrap();
    for id in 1..200 {
        engine
            .execute_with_session(
                &mut writer,
                &format!("UPDATE t SET pid={} WHERE id={id}", (id + 1) % 8000),
            )
            .unwrap();
    }
    ddl.join().unwrap().unwrap();
    assert_eq!(
        engine
            .execute("INSERT INTO t VALUES(9000,9000)")
            .unwrap_err()
            .code(),
        1452
    );
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT COUNT(*) FROM t LEFT JOIN p ON t.pid=p.id WHERE p.id IS NULL")
        .unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(rows.rows[0][0].to_display_string(), "0");
    assert_eq!(engine.database().active_snapshot_count(), 0);
}

#[test]
fn online_fk_snapshot_baseline_handles_order_and_encoding_mismatches() {
    for (parent, child, value, missing) in [
        (
            "BIGINT UNSIGNED",
            "BIGINT UNSIGNED",
            "18446744073709551615",
            "18446744073709551614",
        ),
        (
            "VARCHAR(30) COLLATE utf8mb4_general_ci",
            "VARCHAR(20) COLLATE utf8mb4_general_ci",
            "'AbC'",
            "'missing'",
        ),
        (
            "DECIMAL(40,3)",
            "DECIMAL(42,3)",
            "123456789012345678901234567890123456.125",
            "2.500",
        ),
        (
            "VARBINARY(30)",
            "VARBINARY(20)",
            "X'000102FF'",
            "X'000102FE'",
        ),
        (
            "DATETIME(3)",
            "DATETIME(6)",
            "'2026-09-18 01:02:03.125'",
            "'2026-09-18 01:02:03.126'",
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_relaxed_test_engine(dir.path());
        engine
            .execute(&format!("CREATE TABLE p(a {parent},PRIMARY KEY(a DESC))"))
            .unwrap();
        engine
            .execute(&format!(
                "CREATE TABLE t(id INT PRIMARY KEY,a {child},INDEX backing(a))"
            ))
            .unwrap();
        engine
            .execute(&format!("INSERT INTO p VALUES({value})"))
            .unwrap();
        engine
            .execute(&format!("INSERT INTO t VALUES(1,{value}),(2,NULL)"))
            .unwrap();
        engine
            .execute("ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY(a) REFERENCES p(a), LOCK=NONE")
            .unwrap();
        assert!(!engine
            .database()
            .last_constraint_used_index_merge_for_test());
        engine.execute("ALTER TABLE t DROP FOREIGN KEY fk").unwrap();
        engine
            .execute(&format!("INSERT INTO t VALUES(3,{missing})"))
            .unwrap();
        assert_eq!(
            engine
                .execute(
                    "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY(a) REFERENCES p(a), LOCK=NONE"
                )
                .unwrap_err()
                .code(),
            1452
        );
    }
}

#[test]
fn online_fk_parallel_ranges_match_serial_and_respect_memory_budget() {
    for backing in ["", ", INDEX backing(a,n,id)"] {
        let dir = tempfile::tempdir().unwrap();
        let engine = std::sync::Arc::new(open_relaxed_test_engine(dir.path()));
        engine
            .execute("CREATE TABLE p(a VARCHAR(160),n INT,PRIMARY KEY(a,n))")
            .unwrap();
        engine
            .execute(&format!(
                "CREATE TABLE t(id INT PRIMARY KEY,a VARCHAR(160),n INT{backing})"
            ))
            .unwrap();
        let key = |i| format!("{i:05}{}", "x".repeat(120));
        // Keep all rows, key widths and transaction boundaries: they trigger
        // parallel ranges and memory-budget fallback below. SQL parsing is
        // unrelated to the foreign-key validation under test.
        for start in (0..12000).step_by(1000) {
            insert_fixture_rows(
                &engine,
                "test.t",
                (start..start + 1000).map(|i| {
                    vec![
                        Value::Int(i),
                        Value::String(key(i % 3000)),
                        Value::Int(i % 3000 % 7),
                    ]
                }),
            );
            insert_fixture_rows(
                &engine,
                "test.p",
                (start / 4..(start + 1000) / 4)
                    .map(|i| vec![Value::String(key(i)), Value::Int(i % 7)]),
            );
        }
        engine
            .execute("INSERT INTO t VALUES(12000,NULL,1),(12001,'unmatched',NULL)")
            .unwrap();
        let add = "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY(a,n) REFERENCES p(a,n), LOCK=NONE";
        for degree in [4, 1] {
            engine
                .database()
                .with_constraint_merge_degree_for_test(degree, || engine.execute(add))
                .unwrap();
            assert!(engine
                .database()
                .last_constraint_used_index_merge_for_test());
            let workers = engine.database().last_constraint_merge_workers_for_test();
            assert!(workers <= degree);
            if degree > 1 && std::thread::available_parallelism().unwrap().get() > 1 {
                assert!(workers > 1, "large index should use parallel ranges");
            }
            engine.execute("ALTER TABLE t DROP FOREIGN KEY fk").unwrap();
        }
        engine
            .database()
            .with_constraint_merge_degree_for_test(4, || {
                engine
                    .database()
                    .with_constraint_validation_limit_for_test(12 * 1024 * 1024, || {
                        engine.execute(add)
                    })
            })
            .unwrap();
        assert_eq!(
            engine.database().last_constraint_merge_workers_for_test(),
            1
        );
        engine.execute("ALTER TABLE t DROP FOREIGN KEY fk").unwrap();
        let hook = engine
            .database()
            .install_online_index_validation_test_hook();
        let worker = engine.clone();
        let ddl = std::thread::spawn(move || {
            let mut session = SessionContext::default();
            session.connection_id = 9051;
            let result = worker
                .database()
                .with_constraint_merge_degree_for_test(4, || {
                    worker.execute_with_session(&mut session, add)
                });
            (
                result,
                worker.database().last_constraint_merge_workers_for_test(),
            )
        });
        let reached = hook.wait_until_reached(Duration::from_secs(10));
        let deleted = engine.execute(&format!(
            "DELETE FROM p WHERE a='{}' AND n={}",
            key(1500),
            1500 % 7
        ));
        hook.release();
        let (result, workers) = ddl.join().unwrap();
        assert!(reached);
        deleted.unwrap();
        assert_eq!(result.unwrap_err().code(), 1452);
        if std::thread::available_parallelism().unwrap().get() > 1 {
            assert!(workers > 1);
        }
        assert!(engine
            .database()
            .table("test.t")
            .unwrap()
            .foreign_keys
            .is_empty());
        engine
            .execute(&format!(
                "INSERT INTO p VALUES('{}',{})",
                key(1500),
                1500 % 7
            ))
            .unwrap();
        // The same child key occurs in different physical scan morsels. All
        // four decrements must cancel its baseline count, including across
        // workers; synthetic row identities must not deduplicate real rows.
        let hook = engine
            .database()
            .install_online_index_validation_test_hook();
        let worker = engine.clone();
        let ddl = std::thread::spawn(move || {
            let mut session = SessionContext::default();
            session.connection_id = 9052;
            worker
                .database()
                .with_constraint_merge_degree_for_test(4, || {
                    worker.execute_with_session(&mut session, add)
                })
        });
        let reached = hook.wait_until_reached(Duration::from_secs(10));
        let removed = engine.execute(&format!("DELETE FROM t WHERE a='{}'", key(1500)));
        let parent_removed = engine.execute(&format!("DELETE FROM p WHERE a='{}'", key(1500)));
        hook.release();
        let result = ddl.join().unwrap();
        assert!(reached);
        removed.unwrap();
        parent_removed.unwrap();
        result.unwrap();
        engine.execute("ALTER TABLE t DROP FOREIGN KEY fk").unwrap();
        engine
            .execute(&format!(
                "INSERT INTO p VALUES('{}',{})",
                key(1500),
                1500 % 7
            ))
            .unwrap();
        for id in [1500, 4500, 7500, 10500] {
            engine
                .execute(&format!(
                    "INSERT INTO t VALUES({id},'{}',{})",
                    key(1500),
                    1500 % 7
                ))
                .unwrap();
        }
        // Holes at the beginning, a sampled boundary and the end must all
        // fail, including repeated child prefixes and nullable key parts.
        for missing in [0, 1500, 2999] {
            engine
                .execute(&format!(
                    "DELETE FROM p WHERE a='{}' AND n={}",
                    key(missing),
                    missing % 7
                ))
                .unwrap();
            let err = engine
                .database()
                .with_constraint_merge_degree_for_test(4, || engine.execute(add))
                .unwrap_err();
            assert_eq!(err.code(), 1452);
            engine
                .execute(&format!(
                    "INSERT INTO p VALUES('{}',{})",
                    key(missing),
                    missing % 7
                ))
                .unwrap();
        }
        assert_eq!(engine.database().active_snapshot_count(), 0);
    }
}

/// Exercise the streamed generic-key counts both before the first replay and
/// after validation. Invalid physical scan rows may be repaired by captured
/// writes; new conflicts must still reject publication.
#[test]
fn online_composite_primary_streamed_validation_replays_repairs_and_conflicts() {
    for before_first_replay in [true, false] {
        for conflict in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let engine = std::sync::Arc::new(open_strict_test_engine(dir.path()));
            engine
                .execute("CREATE TABLE t(id INT, a INT, b INT, c INT, d INT)")
                .unwrap();
            engine
                .execute("INSERT INTO t VALUES(1,NULL,1,1,1),(2,2,2,2,2),(3,2,2,2,2)")
                .unwrap();
            let hook = if before_first_replay {
                engine.database().install_online_index_replay_test_hook()
            } else {
                engine
                    .database()
                    .install_online_index_validation_test_hook()
            };
            let worker = engine.clone();
            let ddl = std::thread::spawn(move || {
                let mut session = SessionContext::default();
                session.connection_id = 99101;
                worker.execute_with_session(
                    &mut session,
                    "ALTER TABLE t ADD PRIMARY KEY(a,b,c,d), LOCK=NONE",
                )
            });
            let reached = hook.wait_until_reached(Duration::from_secs(10));
            let mut writer = SessionContext::default();
            writer.connection_id = 99102;
            let repair_null =
                engine.execute_with_session(&mut writer, "UPDATE t SET a=1 WHERE id=1");
            let repair_duplicate =
                engine.execute_with_session(&mut writer, "UPDATE t SET a=3 WHERE id=3");
            let new_conflict = if conflict {
                Some(engine.execute_with_session(&mut writer, "INSERT INTO t VALUES(4,3,2,2,2)"))
            } else {
                None
            };
            hook.release();
            let result = ddl.join().unwrap();
            assert!(reached, "validation hook not reached: {result:?}");
            repair_null.unwrap();
            repair_duplicate.unwrap();
            if let Some(inserted) = new_conflict {
                inserted.unwrap();
                assert_eq!(result.unwrap_err().code(), 1062);
                assert!(engine
                    .database()
                    .table("test.t")
                    .unwrap()
                    .primary_key
                    .is_empty());
            } else {
                result.unwrap();
                assert_online_index_matches_heap(&engine, "PRIMARY");
                drop(engine);
                let reopened = open_strict_cold_metadata_engine(dir.path());
                assert_online_index_matches_heap(&reopened, "PRIMARY");
                assert_eq!(
                    reopened
                        .execute("INSERT INTO t VALUES(4,3,2,2,2)")
                        .unwrap_err()
                        .code(),
                    1062
                );
                assert_eq!(
                    reopened
                        .execute("INSERT INTO t VALUES(4,NULL,2,2,2)")
                        .unwrap_err()
                        .code(),
                    1048
                );
            }
        }
    }
}

#[test]
fn online_encoded_composite_nullable_variable_and_expression_keys() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY, s VARCHAR(64) COLLATE utf8mb4_0900_ai_ci, v DECIMAL(50,8), dt DATETIME(6))").unwrap();
    engine.execute("INSERT INTO t VALUES (1,'MixedCase',1234567890123456789012345678901234567890.12345678,'2024-01-02 03:04:05.123456'),(2,NULL,1,'2024-01-01'),(3,NULL,1,'2024-01-01'),(4,NULL,NULL,NULL)").unwrap();
    engine
        .execute("CREATE UNIQUE INDEX mixed_idx ON t(s,v,dt)")
        .unwrap();
    engine
        .execute("CREATE INDEX expr_idx ON t((id+1),v,dt)")
        .unwrap();
    assert_online_index_matches_heap(&engine, "mixed_idx");
    assert_online_index_matches_heap(&engine, "expr_idx");
    drop(engine);
    let engine = open_strict_cold_metadata_engine(dir.path());
    assert_online_index_matches_heap(&engine, "mixed_idx");
    assert_online_index_matches_heap(&engine, "expr_idx");
    assert_eq!(engine.execute("INSERT INTO t VALUES (5,'MIXEDCASE',1234567890123456789012345678901234567890.12345678,'2024-01-02 03:04:05.123456')").unwrap_err().code(), 1062);
    engine
        .execute("INSERT INTO t VALUES (5,NULL,1,'2024-01-01')")
        .unwrap();
    assert_online_index_matches_heap(&engine, "mixed_idx");
}
