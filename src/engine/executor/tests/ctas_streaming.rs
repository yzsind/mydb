use super::*;

fn rows(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> Vec<Vec<Value>> {
    match engine
        .execute_with_session(session, sql)
        .unwrap()
        .materialize()
        .unwrap()
    {
        QueryResponse::ResultSet(result) => result.rows,
        other => panic!("expected rows for {sql}: {other:?}"),
    }
}

fn seed(engine: &StorageQueryEngine) {
    engine
        .execute("CREATE TABLE source_rows(id INT NOT NULL, v VARCHAR(32))")
        .unwrap();
    let values = (0..160)
        .map(|id| format!("({id},'v{id}')"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO source_rows VALUES {values}"))
        .unwrap();
}

#[test]
fn ctas_streaming_query_shapes_and_declared_mapping() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed(&engine);
    let mut session = SessionContext::default();
    session.connection_id = 8301;
    for (i, query) in [
        "SELECT id,v FROM source_rows",
        "SELECT id,v FROM source_rows WHERE id<80 UNION ALL SELECT id,v FROM source_rows WHERE id>=80",
        "WITH q AS (SELECT id,v FROM source_rows) SELECT id,v FROM q",
        "SELECT q.id,q.v FROM (SELECT id,v FROM source_rows) q",
        "SELECT s.id,q.v FROM source_rows s JOIN LATERAL (SELECT s.v AS v) q ON TRUE",
        "SELECT id,MAX(v) AS v FROM source_rows GROUP BY id ORDER BY id DESC",
    ].iter().enumerate() {
        for temporary in [false, true] {
            let name = format!("copy_{i}_{}", u8::from(temporary));
            let prefix = if temporary { "TEMPORARY " } else { "" };
            let sql = format!("CREATE {prefix}TABLE {name}(extra INT DEFAULT 7, v VARCHAR(32), id INT PRIMARY KEY) AS {query}");
            engine.execute_with_session(&mut session, &sql).unwrap_or_else(|err| panic!("{sql}: {err:?}"));
            let result = rows(&engine, &mut session, &format!("SELECT id,v,extra FROM {name} ORDER BY id"));
            assert_eq!(result.len(), 160, "{sql}");
            for (id, row) in result.iter().enumerate() {
                assert_eq!(row[0], Value::Int(id as i64));
                assert_eq!(row[1].to_display_string(), format!("v{id}"));
                assert_eq!(row[2], Value::Int(7));
            }
        }
    }
}

#[test]
fn ctas_streaming_late_conflicts_and_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed(&engine);
    for autocommit in [true, false] {
        let mut session = SessionContext::default();
        session.connection_id = 8302;
        engine
            .execute_with_session(
                &mut session,
                if autocommit {
                    "SET autocommit=1"
                } else {
                    "SET autocommit=0"
                },
            )
            .unwrap();
        for temporary in [false, true] {
            let prefix = if temporary { "TEMPORARY " } else { "" };
            for (policy, expected) in [
                ("", None),
                ("IGNORE", Some("v0")),
                ("REPLACE", Some("last")),
            ] {
                let sql = format!("CREATE {prefix}TABLE conflict_copy(id INT PRIMARY KEY) {policy} AS SELECT id,v FROM source_rows UNION ALL SELECT 0,'last'");
                let result = engine.execute_with_session(&mut session, &sql);
                if let Some(expected) = expected {
                    result.unwrap();
                    assert_eq!(
                        rows(&engine, &mut session, "SELECT COUNT(*) FROM conflict_copy")[0][0],
                        Value::Int(160)
                    );
                    assert_eq!(
                        rows(
                            &engine,
                            &mut session,
                            "SELECT v FROM conflict_copy WHERE id=0"
                        )[0][0]
                            .to_display_string(),
                        expected
                    );
                    engine.execute_with_session(&mut session, "COMMIT").unwrap();
                    engine
                        .execute_with_session(
                            &mut session,
                            &format!("DROP {prefix}TABLE conflict_copy"),
                        )
                        .unwrap();
                } else {
                    assert_eq!(result.unwrap_err().code(), 1062);
                    assert_eq!(
                        engine
                            .execute_with_session(&mut session, "SELECT * FROM conflict_copy")
                            .unwrap_err()
                            .code(),
                        1146
                    );
                    if !temporary {
                        assert!(session.current_txn.is_none());
                    }
                }
            }
        }
        engine
            .execute_with_session(&mut session, "ROLLBACK")
            .unwrap();
    }
}

#[test]
fn ctas_streaming_late_conversion_error_cleans_target() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed(&engine);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET sql_mode='STRICT_ALL_TABLES'")
        .unwrap();
    for temporary in [false, true] {
        let prefix = if temporary { "TEMPORARY " } else { "" };
        let result = engine.execute_with_session(&mut session, &format!(
            "CREATE {prefix}TABLE invalid_copy(id INT) AS SELECT CAST(id AS CHAR) AS id FROM source_rows UNION ALL SELECT 'bad'"));
        assert!(result.is_err());
        assert_eq!(
            engine
                .execute_with_session(&mut session, "SELECT * FROM invalid_copy")
                .unwrap_err()
                .code(),
            1146
        );
    }
}

#[test]
fn ctas_streaming_metadata_covers_empty_and_late_values() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = memory_test_storage_config();
    config.buffer_pool_pages = 4096;
    let engine =
        StorageQueryEngine::open_with_storage_config_without_background_workers(dir.path(), config)
            .unwrap();
    seed(&engine);
    for (i, query) in [
        "SELECT REPEAT('x', IF(id=159,66000,1)) AS v FROM source_rows",
        "SELECT CASE WHEN id=159 THEN REPEAT('x',66000) ELSE 'x' END AS v FROM source_rows",
        "SELECT 'x' AS v UNION ALL SELECT REPEAT('x',id*1000) FROM source_rows",
        "SELECT q.v FROM (SELECT REPEAT('x', IF(id=159,66000,1)) AS v FROM source_rows) q",
    ]
    .iter()
    .enumerate()
    {
        let mut session = SessionContext::default();
        engine
            .execute_with_session(&mut session, &format!("CREATE TABLE wide_{i} AS {query}"))
            .unwrap();
        let longest = rows(
            &engine,
            &mut session,
            &format!("SELECT MAX(LENGTH(v)) FROM wide_{i}"),
        )[0][0]
            .to_display_string();
        assert_eq!(longest, if i == 2 { "159000" } else { "66000" });
    }
    engine
        .execute(
            "CREATE TABLE empty_wide AS SELECT REPEAT('x',id) AS v FROM source_rows WHERE id<0",
        )
        .unwrap();
    assert!(matches!(
        engine.database().table("test.empty_wide").unwrap().columns[0].data_type,
        DataTypeKind::Text {
            max_len: crate::storage::schema::LONG_LOB_MAX
        }
    ));
    for (i, query) in [
        "SELECT q.v FROM (SELECT r.id AS v FROM source_rows l LEFT JOIN source_rows r ON r.id=l.id+1000) q",
        "WITH q AS (SELECT r.id AS v FROM source_rows l LEFT JOIN source_rows r ON r.id=l.id+1000) SELECT v FROM q",
        "SELECT id AS v FROM source_rows UNION ALL SELECT NULL",
        "SELECT q.v FROM (SELECT r.id AS v FROM source_rows l LEFT JOIN source_rows r ON r.id=l.id+1000 WHERE l.id<0) q",
    ].iter().enumerate() {
        engine.execute(&format!("CREATE TABLE nullable_{i} AS {query}")).unwrap();
        assert!(engine.database().table(&format!("test.nullable_{i}")).unwrap().columns[0].nullable, "{query}");
    }
}

#[test]
fn ctas_streaming_temporary_load_obeys_user_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed(&engine);
    let mut session = SessionContext::default();
    session.connection_id = 8303;
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO source_rows VALUES(160,'uncommitted')",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TEMPORARY TABLE temp_copy(id INT PRIMARY KEY) AS SELECT id,v FROM source_rows",
        )
        .unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT COUNT(*) FROM temp_copy")[0][0],
        Value::Int(161)
    );
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT COUNT(*) FROM source_rows")[0][0],
        Value::Int(160)
    );
    assert_eq!(
        rows(&engine, &mut session, "SELECT COUNT(*) FROM temp_copy")[0][0],
        Value::Int(0)
    );
}

#[test]
fn ctas_streaming_unknown_string_branch_keeps_full_domain() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed(&engine);
    engine.execute("CREATE TABLE unknown_string AS SELECT CASE WHEN id=159 THEN REPLACE(v,'v',REPEAT('x',66000)) ELSE 'x' END AS v FROM source_rows").unwrap();
    let mut session = SessionContext::default();
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "SELECT MAX(LENGTH(v)) FROM unknown_string"
        )[0][0],
        Value::Int(66003)
    );
}

#[test]
fn ctas_streaming_holds_mdl_during_load_and_allows_source_dml() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(open_test_engine(dir.path()));
    seed(&engine);
    let (reached_tx, reached_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let worker = engine.clone();
    let load = std::thread::spawn(move || {
        super::super::dispatch::CTAS_BATCH_WRITTEN.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move |db| {
                let locks = db.metadata_lock_snapshot();
                assert!(locks.iter().any(|lock| lock.owner_thread_id == Some(8304)
                    && lock.object_name.as_deref() == Some("locked_copy")
                    && lock.lock_type == "EXCLUSIVE"));
                reached_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(15)).unwrap();
            }));
        });
        let mut session = SessionContext::default();
        session.connection_id = 8304;
        worker.execute_with_session(
            &mut session,
            "CREATE TABLE locked_copy AS SELECT id,v FROM source_rows",
        )
    });
    let reached = reached_rx.recv_timeout(Duration::from_secs(10));
    if reached.is_err() {
        let _ = release_tx.send(());
        panic!("load did not reach first batch: {:?}", load.join());
    }
    let mut other = SessionContext::default();
    other.connection_id = 8305;
    engine
        .execute_with_session(&mut other, "SET lock_wait_timeout=1")
        .unwrap();
    let write = engine.execute_with_session(
        &mut other,
        "INSERT INTO source_rows VALUES(160,'concurrent')",
    );
    let blocked_read = engine.execute_with_session(&mut other, "SELECT * FROM locked_copy");
    let blocked_ddl = engine.execute_with_session(
        &mut other,
        "ALTER TABLE source_rows RENAME COLUMN v TO renamed_v",
    );
    release_tx.send(()).unwrap();
    let loaded = load.join().unwrap();
    write.unwrap();
    assert_eq!(blocked_read.unwrap_err().code(), 1205);
    assert_eq!(blocked_ddl.unwrap_err().code(), 1205);
    loaded.unwrap();
    assert_eq!(
        rows(&engine, &mut other, "SELECT COUNT(*) FROM locked_copy")[0][0],
        Value::Int(160)
    );
    assert!(!engine
        .database()
        .metadata_lock_snapshot()
        .iter()
        .any(|lock| lock.owner_thread_id == Some(8304)));
}

#[test]
fn ctas_streaming_temporary_autocommit_releases_source_mdl() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed(&engine);
    let mut session = SessionContext::default();
    session.connection_id = 8306;
    engine
        .execute_with_session(
            &mut session,
            "CREATE TEMPORARY TABLE temp_mdl AS SELECT id FROM source_rows",
        )
        .unwrap();
    assert!(session.current_txn.is_none());
    assert!(!engine
        .database()
        .metadata_lock_snapshot()
        .iter()
        .any(|lock| lock.owner_thread_id == Some(8306)));
}

#[test]
fn ctas_streaming_autocommit_disabled_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        seed(&engine);
        let mut session = SessionContext::default();
        session.connection_id = 8307;
        engine
            .execute_with_session(&mut session, "SET autocommit=0")
            .unwrap();
        engine.execute_with_session(&mut session, "CREATE TABLE durable_copy(id INT PRIMARY KEY) AS WITH q AS (SELECT id,v FROM source_rows) SELECT * FROM q").unwrap();
        assert!(session.current_txn.is_none());
        engine
            .execute_with_session(&mut session, "ROLLBACK")
            .unwrap();
    }
    let engine = open_strict_test_engine(dir.path());
    let mut session = SessionContext::default();
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "SELECT COUNT(*),MAX(id) FROM durable_copy"
        )[0],
        vec![Value::Int(160), Value::Int(159)]
    );
}

#[test]
fn ctas_streaming_repeat_preserves_character_and_binary_domains() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed(&engine);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET NAMES utf8mb4")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE repeated_numbers AS SELECT REPEAT(id,2) AS v FROM source_rows",
        )
        .unwrap();
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "SELECT CHARSET(v) FROM repeated_numbers LIMIT 1"
        )[0][0]
            .to_display_string(),
        "utf8mb4"
    );
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE repeated_bytes AS SELECT REPEAT(CAST('ab' AS BINARY),3) AS v",
        )
        .unwrap();
    let result = rows(
        &engine,
        &mut session,
        "SELECT CHARSET(v),HEX(v) FROM repeated_bytes",
    );
    assert_eq!(result[0][0].to_display_string(), "binary");
    assert_eq!(result[0][1].to_display_string(), "616261626162");
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE repeated_raw AS SELECT REPEAT(X'FF00',3) AS v",
        )
        .unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT HEX(v) FROM repeated_raw")[0][0].to_display_string(),
        "FF00FF00FF00"
    );
    engine
        .execute_with_session(&mut session, "CREATE TABLE raw_source(v VARBINARY(2))")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO raw_source VALUES(X'FF00')")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE repeated_raw_column AS SELECT REPEAT(v,3) AS v FROM raw_source",
        )
        .unwrap();
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "SELECT HEX(v) FROM repeated_raw_column"
        )[0][0]
            .to_display_string(),
        "FF00FF00FF00"
    );
}
