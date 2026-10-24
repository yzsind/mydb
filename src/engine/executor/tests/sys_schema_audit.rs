use super::*;

fn query_rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected result set for {sql}");
    };
    result.rows
}

#[test]
fn sys_config_statement_atomicity_predicates_and_durable_file() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let err = engine.execute("INSERT INTO sys.sys_config (variable, value) VALUES ('audit_a', '1'), ('audit_b', '2'), ('audit_a', '3')").unwrap_err();
    assert_eq!(err.code(), crate::error::catalog::ER_DUP_ENTRY.code());
    assert!(query_rows(
        &engine,
        "SELECT variable FROM sys.sys_config WHERE variable LIKE 'audit_%'"
    )
    .is_empty());

    engine.execute("INSERT INTO sys.sys_config (variable, value, set_time) VALUES ('audit_null', 'NULL', '2001-01-01 00:00:00')").unwrap();
    assert_eq!(
        query_rows(
            &engine,
            "SELECT set_time FROM sys.sys_config WHERE variable='audit_null'"
        )[0][0],
        Value::String("2001-01-01 00:00:00".to_string())
    );
    engine
        .execute("DELETE FROM sys.sys_config WHERE value = NULL")
        .unwrap();
    assert_eq!(
        query_rows(
            &engine,
            "SELECT value FROM sys.sys_config WHERE variable='audit_null'"
        )[0][0],
        Value::String("NULL".to_string())
    );
    engine.execute("UPDATE sys.sys_config SET value='changed' WHERE variable IN ('audit_null') AND set_time < '2002-01-01'").unwrap();
    assert_eq!(
        query_rows(
            &engine,
            "SELECT value FROM sys.sys_config WHERE variable='audit_null'"
        )[0][0],
        Value::String("changed".to_string())
    );

    engine.execute("INSERT INTO sys.sys_config (variable, value) VALUES ('audit_null', 'again') ON DUPLICATE KEY UPDATE value='updated'").unwrap();
    assert_eq!(
        query_rows(
            &engine,
            "SELECT value FROM sys.sys_config WHERE variable='audit_null'"
        )[0][0],
        Value::String("updated".to_string())
    );
    engine.execute("INSERT INTO sys.sys_config (variable, value) VALUES ('audit_null', 'from_values') ON DUPLICATE KEY UPDATE value=VALUES(value)").unwrap();
    assert_eq!(
        query_rows(
            &engine,
            "SELECT value FROM sys.sys_config WHERE variable='audit_null'"
        )[0][0],
        Value::String("from_values".to_string())
    );
    engine
        .execute(
            "UPDATE sys.sys_config SET value=CONCAT(value, '_suffix') WHERE variable='audit_null'",
        )
        .unwrap();
    assert_eq!(
        query_rows(
            &engine,
            "SELECT value FROM sys.sys_config WHERE variable='audit_null'"
        )[0][0],
        Value::String("from_values_suffix".to_string())
    );
    engine.execute("UPDATE sys.sys_config SET value=sys.sys_get_config('audit_null', 'fallback') WHERE variable='audit_null'").unwrap();
    let replace = engine
        .execute("REPLACE INTO sys.sys_config (variable, value) VALUES ('audit_null', 'replaced')")
        .unwrap();
    assert!(matches!(
        replace,
        QueryResponse::OkAffected { affected_rows: 2 }
    ));
    let on_disk = std::fs::read_to_string(dir.path().join("system/sys_config.json")).unwrap();
    assert!(on_disk.contains("replaced"));
    let mut session = SessionContext::default();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    let err = engine
        .execute_with_session(
            &mut session,
            "INSERT INTO sys.sys_config (variable,value) VALUES ('in_txn','x')",
        )
        .unwrap_err();
    assert_eq!(
        err.code(),
        crate::error::catalog::ER_NOT_SUPPORTED_YET.code()
    );
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET autocommit=0")
        .unwrap();
    let err = engine
        .execute_with_session(
            &mut session,
            "UPDATE sys.sys_config SET value='bad' WHERE variable='audit_null'",
        )
        .unwrap_err();
    assert_eq!(
        err.code(),
        crate::error::catalog::ER_NOT_SUPPORTED_YET.code()
    );
}

#[test]
fn sys_schema_errors_and_metadata_are_honest() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let err = engine.execute("SELECT `ab系统`()").unwrap_err();
    assert!(!err.message().is_empty());
    let err = engine
        .execute("CALL sys.create_synonym_db('test', 'syn_test')")
        .unwrap_err();
    assert_eq!(
        err.code(),
        crate::error::catalog::ER_NOT_SUPPORTED_YET.code()
    );
    let err = engine
        .execute("CALL sys.ps_setup_enable_consumer()")
        .unwrap_err();
    assert_eq!(
        err.code(),
        crate::error::catalog::ER_SP_WRONG_NO_OF_ARGS.code()
    );
    assert_eq!(query_rows(&engine, "SELECT routine_name FROM information_schema.routines WHERE routine_schema='sys' AND routine_name='format_pico_time'").len(), 0);
    let create = query_rows(&engine, "SHOW CREATE FUNCTION sys.format_bytes");
    assert!(create[0][2].to_display_string().contains("`bytes` text"));
    assert!(!create[0][2].to_display_string().contains("RETURN NULL"));
    assert_eq!(
        query_rows(&engine, "SHOW CREATE FUNCTION sys.FORMAT_BYTES").len(),
        1
    );
    assert_eq!(query_rows(&engine, "SELECT parameter_name FROM information_schema.parameters WHERE specific_schema='sys' AND specific_name='format_bytes' AND ordinal_position=1")[0][0], Value::String("bytes".to_string()));
    assert_eq!(query_rows(&engine, "SELECT dtd_identifier FROM information_schema.parameters WHERE specific_schema='sys' AND specific_name='ps_setup_reset_to_default' AND ordinal_position=1")[0][0], Value::String("tinyint(1)".to_string()));
    assert_eq!(query_rows(&engine, "SELECT data_type FROM information_schema.columns WHERE table_schema='sys' AND table_name='x$processlist' AND column_name='trx_autocommit'")[0][0], Value::String("enum".to_string()));
    assert_eq!(
        query_rows(&engine, "SELECT * FROM sys.session LIMIT 1").len(),
        0
    );
    assert_eq!(
        query_rows(&engine, "SELECT * FROM sys.`x$session` LIMIT 1").len(),
        0
    );
    assert_eq!(query_rows(&engine, "SELECT data_type, is_nullable FROM information_schema.columns WHERE table_schema='sys' AND table_name='schema_object_overview' AND column_name='count'")[0], vec![Value::String("bigint".to_string()), Value::String("NO".to_string())]);
    assert_eq!(
        query_rows(
            &engine,
            "SHOW CREATE TRIGGER sys.sys_config_insert_set_user"
        )
        .len(),
        1
    );
}

#[test]
fn system_scan_keeps_session_dependent_filter() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let plan = query_rows(
        &engine,
        "EXPLAIN FORMAT=TREE SELECT conn_id FROM sys.processlist WHERE conn_id = CONNECTION_ID()",
    );
    assert!(
        plan.iter().any(|row| row
            .iter()
            .any(|value| value.to_display_string().contains("filter:"))),
        "{plan:?}"
    );
}

#[test]
fn sys_setup_procedures_match_substrings_without_changing_timed() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CALL sys.ps_setup_disable_consumer('events_statements_history')")
        .unwrap();
    engine
        .execute("CALL sys.ps_setup_enable_consumer('statements_history')")
        .unwrap();
    assert_eq!(query_rows(&engine, "SELECT ENABLED FROM performance_schema.setup_consumers WHERE NAME='events_statements_history'")[0][0], Value::String("YES".to_string()));

    let instrument = "wait/synch/mutex/pfs/LOCK_pfs_share_list";
    let before = query_rows(
        &engine,
        &format!(
            "SELECT TIMED FROM performance_schema.setup_instruments WHERE NAME='{instrument}'"
        ),
    )[0][0]
        .clone();
    engine
        .execute("CALL sys.ps_setup_enable_instrument('LOCK_pfs_share_list')")
        .unwrap();
    let after = query_rows(&engine, &format!("SELECT ENABLED, TIMED FROM performance_schema.setup_instruments WHERE NAME='{instrument}'"));
    assert_eq!(after[0][0], Value::String("YES".to_string()));
    assert_eq!(after[0][1], before);
}
