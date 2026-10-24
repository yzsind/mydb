use super::*;

fn scalar(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> String {
    let QueryResponse::ResultSet(result) = engine.execute_with_session(session, sql).unwrap()
    else {
        panic!("expected result set: {sql}");
    };
    result.rows[0][0].to_display_string()
}

#[test]
fn grant_all_keeps_dynamic_privileges_explicit() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    engine
        .execute_with_session(
            &mut root,
            "CREATE USER 'ms_lackey'@'%' IDENTIFIED BY 'pass'",
        )
        .unwrap();
    engine
        .execute_with_session(&mut root, "GRANT ALL ON *.* TO 'ms_lackey'@'%'")
        .unwrap();
    assert_eq!(
        scalar(
            &engine,
            &mut root,
            "SELECT COUNT(*) FROM mysql.global_grants WHERE USER='ms_lackey'"
        ),
        "0"
    );
    engine
        .execute_with_session(&mut root, "GRANT SYSTEM_USER ON *.* TO 'ms_lackey'@'%'")
        .unwrap();
    assert_eq!(
        scalar(
            &engine,
            &mut root,
            "SELECT COUNT(*) FROM mysql.global_grants WHERE USER='ms_lackey'"
        ),
        "1"
    );
}

#[test]
fn mysql_virtual_names_are_reserved_across_ddl_paths() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    engine
        .execute_with_session(&mut root, "CREATE TABLE mysql.t_user(id INT)")
        .unwrap();
    let sql = [
        "CREATE TABLE mysql.user(id INT)",
        "CREATE TABLE mysql.user LIKE mysql.t_user",
        "CREATE TABLE mysql.user AS SELECT id FROM mysql.t_user",
        "RENAME TABLE mysql.t_user TO mysql.user",
        "ALTER TABLE mysql.t_user RENAME TO mysql.user",
    ];
    for statement in sql {
        let err = engine
            .execute_with_session(&mut root, statement)
            .unwrap_err();
        assert_eq!(err.code(), 1050, "{statement}: {err:?}");
    }
    let err = engine
        .execute_with_session(&mut root, "DROP TABLE mysql.user")
        .unwrap_err();
    assert_eq!(err.code(), 3554);
    assert_eq!(scalar(&engine, &mut root, "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='mysql' AND TABLE_NAME='user'"), "1");
    engine
        .execute_with_session(&mut root, "DROP TABLE mysql.t_user")
        .unwrap();
}

#[test]
fn legacy_mysql_shadow_is_recovered_on_restart() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = memory_test_storage_config();
    config.durability = crate::storage::DurabilityMode::Strict;
    let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir.path(),
        config.clone(),
    )
    .unwrap();
    engine
        .execute("CREATE TABLE mysql.t_source(id INT)")
        .unwrap();
    let source = engine.db.table("mysql.t_source").unwrap();
    engine.db.create_table_like("mysql.user", &source).unwrap();
    drop(engine);

    let engine =
        StorageQueryEngine::open_with_storage_config_without_background_workers(dir.path(), config)
            .unwrap();
    let mut root = SessionContext::default();
    assert_eq!(
        scalar(&engine, &mut root, "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='mysql' AND TABLE_NAME='user'"),
        "1"
    );
    let QueryResponse::ResultSet(shown) = engine
        .execute_with_session(&mut root, "SHOW TABLES FROM mysql LIKE 'mydb_rec_user_%'")
        .unwrap()
    else {
        panic!("SHOW TABLES result");
    };
    assert_eq!(shown.rows.len(), 1);
}

#[test]
fn system_scalar_subquery_binds_inner_unqualified_column() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    // Zone identifiers depend on the host tzdata catalog, not SQL binding.
    let zone_id = scalar(
        &engine,
        &mut root,
        "SELECT Time_zone_id FROM mysql.time_zone_name WHERE Name='Asia/Shanghai'",
    );
    zone_id.parse::<u64>().unwrap();
    let plain = scalar(
        &engine,
        &mut root,
        &format!("SELECT COUNT(*) FROM mysql.time_zone_transition WHERE Time_zone_id={zone_id}"),
    );
    let subquery = scalar(&engine, &mut root, "SELECT COUNT(*) FROM mysql.time_zone_transition WHERE Time_zone_id=(SELECT Time_zone_id FROM mysql.time_zone_name WHERE Name='Asia/Shanghai')");
    assert_eq!(subquery, plain);
}

#[test]
fn mysql_schema_time_and_listing_match_other_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    engine
        .execute_with_session(&mut root, "SET time_zone='+08:00'")
        .unwrap();
    engine
        .execute_with_session(&mut root, "CREATE USER 'ms_clock'@'%' IDENTIFIED BY 'pass'")
        .unwrap();
    let now = scalar(&engine, &mut root, "SELECT NOW()");
    let changed = scalar(
        &engine,
        &mut root,
        "SELECT password_last_changed FROM mysql.user WHERE User='ms_clock'",
    );
    assert_eq!(&now[..13], &changed[..13]);
    engine
        .execute_with_session(&mut root, "CREATE TABLE mysql.t_visible(id INT)")
        .unwrap();
    let QueryResponse::ResultSet(shown) = engine
        .execute_with_session(&mut root, "SHOW TABLES FROM mysql LIKE 't_visible'")
        .unwrap()
    else {
        panic!("SHOW TABLES result");
    };
    assert_eq!(shown.rows.len(), 1);
    assert_eq!(scalar(&engine, &mut root, "SELECT CHARACTER_OCTET_LENGTH FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='mysql' AND TABLE_NAME='user' AND COLUMN_NAME='authentication_string'"), "196605");
    assert_eq!(scalar(&engine, &mut root, "SELECT CHARACTER_SET_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='mysql' AND TABLE_NAME='servers' AND COLUMN_NAME='Host'"), "ascii");
    assert_eq!(scalar(&engine, &mut root, "SELECT EXTRA FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='mysql' AND TABLE_NAME='component' AND COLUMN_NAME='component_id'"), "auto_increment");
    for (table, fragment) in [
        (
            "general_log",
            "DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)",
        ),
        (
            "slow_log",
            "DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)",
        ),
        ("component", "AUTO_INCREMENT"),
        ("password_history", "`Password_timestamp` DESC"),
        ("servers", "CHARACTER SET ascii"),
    ] {
        let QueryResponse::ResultSet(ddl) = engine
            .execute_with_session(&mut root, &format!("SHOW CREATE TABLE mysql.{table}"))
            .unwrap()
        else {
            panic!("SHOW CREATE result");
        };
        assert!(
            ddl.rows[0][1].to_display_string().contains(fragment),
            "{table}"
        );
    }
}

#[test]
fn innodb_stats_use_physical_partition_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = memory_test_storage_config();
    config.durability = crate::storage::DurabilityMode::Strict;
    let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir.path(),
        config.clone(),
    )
    .unwrap();
    let mut root = SessionContext::default();
    engine.execute("CREATE TABLE part_stat(id INT PRIMARY KEY) PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN(10), PARTITION p1 VALUES LESS THAN MAXVALUE)").unwrap();
    engine
        .execute("INSERT INTO part_stat VALUES (1),(11)")
        .unwrap();
    engine.execute("ANALYZE TABLE part_stat").unwrap();
    let QueryResponse::ResultSet(stats) = engine.execute_with_session(&mut root,
        "SELECT table_name,n_rows FROM mysql.innodb_table_stats WHERE database_name='test' AND table_name LIKE 'part_stat#P#%' ORDER BY table_name").unwrap() else { panic!("table stats result"); };
    assert_eq!(stats.rows.len(), 2);
    assert_eq!(stats.rows[0][0].to_display_string(), "part_stat#P#p0");
    assert_eq!(stats.rows[0][1].to_display_string(), "1");
    assert_eq!(stats.rows[1][0].to_display_string(), "part_stat#P#p1");
    assert_eq!(stats.rows[1][1].to_display_string(), "1");
    assert_eq!(scalar(&engine, &mut root, "SELECT COUNT(*) FROM mysql.innodb_index_stats WHERE database_name='test' AND table_name LIKE 'part_stat#P#%' AND stat_name='n_diff_pfx01'"), "2");
    drop(engine);
    let engine =
        StorageQueryEngine::open_with_storage_config_without_background_workers(dir.path(), config)
            .unwrap();
    assert_eq!(scalar(&engine, &mut root, "SELECT COUNT(*) FROM mysql.innodb_table_stats WHERE database_name='test' AND table_name LIKE 'part_stat#P#%'"), "2");
    assert_eq!(scalar(&engine, &mut root, "SELECT COUNT(*) FROM mysql.innodb_index_stats WHERE database_name='test' AND table_name LIKE 'part_stat#P#%' AND stat_name='n_diff_pfx01'"), "2");
}
