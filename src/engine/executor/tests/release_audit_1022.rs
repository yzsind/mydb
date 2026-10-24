use super::*;

fn setup() -> (tempfile::TempDir, StorageQueryEngine) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    (dir, engine)
}

fn rows(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> Vec<Vec<Value>> {
    match engine
        .execute_with_session(session, sql)
        .unwrap()
        .materialize()
        .unwrap()
    {
        QueryResponse::ResultSet(rs) => rs.rows,
        other => panic!("{sql}: {other:?}"),
    }
}

#[test]
fn audit1022_zero_literals_cannot_prove_a_different_parameter_mapping() {
    let (_dir, engine) = setup();
    let mut session = SessionContext::default();
    for (n, zero) in ["-0.0e0", "-0", "0.0e0", "-0e-1000"].iter().enumerate() {
        let name = format!("neg{n}");
        engine
            .execute(&format!(
                "CREATE TABLE {name}(id INT PRIMARY KEY,d DOUBLE,f FLOAT,KEY kd(d))"
            ))
            .unwrap();
        for (id, number) in [*zero, "-77.7", "-7.5e0", "18.5", "0-3.5"]
            .iter()
            .enumerate()
        {
            engine
                .execute_with_session(
                    &mut session,
                    &format!("INSERT INTO {name} VALUES({id},{number},{number})"),
                )
                .unwrap();
        }
        assert_eq!(
            rows(
                &engine,
                &mut session,
                &format!("SELECT COUNT(*) FROM {name} WHERE d<0 AND f<0")
            ),
            vec![vec![Value::Int(3)]]
        );
        assert_eq!(
            rows(
                &engine,
                &mut session,
                &format!("SELECT d FROM {name} WHERE d=-77.7")
            ),
            vec![vec![Value::Double(-77.7)]]
        );
    }
}

#[test]
fn audit1022_timestamp_conditionals_preserve_utc_across_writes() {
    let (_dir, engine) = setup();
    let mut session = SessionContext::default();
    engine
        .execute("CREATE TABLE ts1(id INT PRIMARY KEY,ts TIMESTAMP(6))")
        .unwrap();
    engine
        .execute("CREATE TABLE ts2(id INT PRIMARY KEY,ts TIMESTAMP(6))")
        .unwrap();
    for zone in ["+08:00", "-05:00", "America/New_York", "+00:00"] {
        engine
            .execute_with_session(&mut session, &format!("SET time_zone='{zone}'"))
            .unwrap();
        engine
            .execute_with_session(&mut session, "DELETE FROM ts1")
            .unwrap();
        engine
            .execute_with_session(&mut session, "DELETE FROM ts2")
            .unwrap();
        engine
            .execute_with_session(
                &mut session,
                "INSERT INTO ts1 VALUES(1,'2020-01-02 03:04:05.123456')",
            )
            .unwrap();
        let expected = rows(&engine, &mut session, "SELECT ts FROM ts1");
        for expr in [
            "COALESCE(ts)",
            "IFNULL(ts,ts)",
            "IF(1,ts,NULL)",
            "CASE WHEN 1 THEN ts END",
            "NULLIF(ts,NULL)",
        ] {
            assert_eq!(
                rows(&engine, &mut session, &format!("SELECT {expr} FROM ts1")),
                expected,
                "{zone}: {expr}"
            );
            engine
                .execute_with_session(&mut session, &format!("UPDATE ts1 SET ts={expr}"))
                .unwrap();
            assert_eq!(
                rows(&engine, &mut session, "SELECT ts FROM ts1"),
                expected,
                "{zone}: UPDATE {expr}"
            );
        }
        engine
            .execute_with_session(
                &mut session,
                "INSERT INTO ts2 SELECT id,COALESCE(ts) FROM ts1",
            )
            .unwrap();
        assert_eq!(
            rows(&engine, &mut session, "SELECT ts FROM ts2"),
            expected,
            "{zone}"
        );
        assert_eq!(
            rows(&engine, &mut session, "SELECT HOUR(COALESCE(ts)) FROM ts1"),
            vec![vec![Value::Int(3)]]
        );
    }
}

#[test]
fn audit1022_json_temporal_casts_and_assignments_unquote_scalars() {
    let (_dir, engine) = setup();
    let mut session = SessionContext::default();
    for (text, target, expected) in [
        ("2020-01-02", "DATE", "2020-01-02"),
        ("2020-01-02 03:04:05", "DATETIME", "2020-01-02 03:04:05"),
        ("03:04:05", "TIME", "03:04:05"),
    ] {
        let sql = format!(r#"SELECT CAST(CAST('"{text}"' AS JSON) AS {target})"#);
        assert_eq!(
            rows(&engine, &mut session, &sql)[0][0].to_display_string(),
            expected
        );
        engine
            .execute(&format!("CREATE TABLE jt{}(v {target})", target))
            .unwrap();
        engine
            .execute(&format!(
                r#"INSERT INTO jt{target} SELECT CAST('"{text}"' AS JSON)"#
            ))
            .unwrap();
        assert_eq!(
            rows(&engine, &mut session, &format!("SELECT v FROM jt{target}"))[0][0]
                .to_display_string(),
            expected
        );
    }
}

#[test]
fn audit1022_routine_substatements_publish_diagnostic_state() {
    let (_dir, engine) = setup();
    let mut session = SessionContext::default();
    engine
        .execute("CREATE TABLE ai(id INT AUTO_INCREMENT PRIMARY KEY,v INT)")
        .unwrap();
    engine.execute("CREATE PROCEDURE diag(OUT a BIGINT,OUT b BIGINT,OUT c INT) BEGIN INSERT INTO ai(v) VALUES(1); SET a=LAST_INSERT_ID(); INSERT INTO ai(v) VALUES(2); SET b=LAST_INSERT_ID(); UPDATE ai SET v=v+1; SET c=ROW_COUNT(); END").unwrap();
    engine
        .execute_with_session(&mut session, "CALL diag(@a,@b,@c)")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT @a,@b,@c,LAST_INSERT_ID()"),
        vec![vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(2),
            Value::Int(2)
        ]]
    );
}

#[test]
fn audit1022_lock_tables_resolves_constant_and_nested_views() {
    let (_dir, engine) = setup();
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE base(id INT)",
        "INSERT INTO base VALUES(1)",
        "CREATE VIEW constant_v AS SELECT 1 AS a",
        "CREATE VIEW v AS SELECT * FROM base",
        "CREATE VIEW vv AS SELECT * FROM v",
        "LOCK TABLES constant_v READ, vv READ",
        "UNLOCK TABLES",
    ] {
        engine
            .execute_with_session(&mut session, sql)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
}

#[test]
fn audit1022_binlog_commands_require_distinct_privileges() {
    use crate::engine::auth::Privilege;
    for (sql, required) in [
        ("SHOW BINARY LOGS", Privilege::ReplClient),
        ("SHOW BINLOG EVENTS", Privilege::ReplSlave),
        (
            "PURGE BINARY LOGS BEFORE '2035-01-01'",
            Privilege::Dynamic("BINLOG_ADMIN".into()),
        ),
        ("RESET MASTER", Privilege::Reload),
        ("FLUSH TABLES WITH READ LOCK", Privilege::Reload),
    ] {
        assert_eq!(crate::binlog::admin_privilege(sql), Some(required), "{sql}");
    }
}

#[test]
fn audit1022_uca_weights_agree_for_comparison_unique_keys_and_deduplication() {
    let (_dir, engine) = setup();
    let mut session = SessionContext::default();
    engine
        .execute("CREATE TABLE uca(v VARCHAR(30) UNIQUE)")
        .unwrap();
    for (a, b) in [("σ", "ς"), ("Σ", "ς"), ("❶", "1"), ("K", "k"), ("æ", "ae")] {
        assert_eq!(
            rows(
                &engine,
                &mut session,
                &format!("SELECT '{a}'='{b}',WEIGHT_STRING('{a}')=WEIGHT_STRING('{b}')")
            ),
            vec![vec![Value::Int(1), Value::Int(1)]]
        );
        engine.execute("DELETE FROM uca").unwrap();
        engine
            .execute(&format!("INSERT INTO uca VALUES('{a}')"))
            .unwrap();
        assert_eq!(
            engine
                .execute(&format!("INSERT INTO uca VALUES('{b}')"))
                .unwrap_err()
                .code(),
            1062
        );
        assert_eq!(
            rows(
                &engine,
                &mut session,
                &format!("SELECT COUNT(*) FROM uca WHERE v='{b}'")
            ),
            vec![vec![Value::Int(1)]]
        );
        assert_eq!(
            rows(
                &engine,
                &mut session,
                &format!("SELECT '{a}' UNION SELECT '{b}'")
            )
            .len(),
            1
        );
    }
}

#[test]
fn audit1022_json_integer_ranges_append_bounds_and_geometry_validity() {
    let (_dir, engine) = setup();
    let mut session = SessionContext::default();
    let result=rows(&engine,&mut session,"SELECT CAST(CAST('18446744073709551615' AS JSON) AS UNSIGNED),CAST(CAST('18446744073709551615' AS JSON) AS SIGNED),JSON_ARRAY_APPEND('1','$[5]',2),ST_IsValid(ST_GeomFromText('POLYGON((0 0,1 1,1 0,0 1,0 0))'))");
    assert_eq!(
        result[0]
            .iter()
            .map(Value::to_display_string)
            .collect::<Vec<_>>(),
        ["18446744073709551615", "-1", "1", "0"]
    );
}

#[test]
fn audit1022_fractional_time_rounding_and_finite_numeric_overflow() {
    let (_dir, engine) = setup();
    let mut session = SessionContext::default();
    for (mode, expected) in [("", 1_000_000), ("TIME_TRUNCATE_FRACTIONAL", 999_999)] {
        engine
            .execute_with_session(&mut session, &format!("SET sql_mode='{mode}'"))
            .unwrap();
        assert_eq!(
            rows(
                &engine,
                &mut session,
                "SELECT CAST('00:00:00.9999995' AS TIME(6))"
            )[0][0],
            Value::Time(expected)
        );
        engine
            .execute_with_session(&mut session, "CREATE TABLE tm(v TIME(6))")
            .unwrap();
        engine
            .execute_with_session(&mut session, "INSERT INTO tm VALUES('00:00:00.9999995')")
            .unwrap();
        assert_eq!(
            rows(&engine, &mut session, "SELECT v FROM tm")[0][0],
            Value::Time(expected)
        );
        engine.execute("DROP TABLE tm").unwrap();
    }
    let result = rows(&engine, &mut session, "SELECT '1e309'+0,'-1e309'+0");
    assert_eq!(
        result,
        vec![vec![Value::Double(f64::MAX), Value::Double(-f64::MAX)]]
    );
}

#[test]
fn audit1022_view_current_user_respects_definer_and_invoker_chains() {
    let (_dir, engine) = setup();
    let mut root = SessionContext::default();
    let owner = rows(&engine, &mut root, "SELECT CURRENT_USER()")[0][0].clone();
    for sql in [
        "CREATE USER 'reader'@'%' IDENTIFIED BY 'Reader123!'",
        "CREATE SQL SECURITY INVOKER VIEW inv AS SELECT CURRENT_USER() AS u",
        "CREATE SQL SECURITY DEFINER VIEW def AS SELECT u FROM inv",
        "GRANT SELECT ON test.inv TO 'reader'@'%'",
        "GRANT SELECT ON test.def TO 'reader'@'%'",
    ] {
        engine.execute(sql).unwrap();
    }
    let mut reader = SessionContext {
        authenticated_as: crate::engine::auth::AccountKey::new("reader", "%"),
        user: "reader".into(),
        ..SessionContext::default()
    };
    let result = rows(&engine, &mut reader, "SELECT CURRENT_USER(),u FROM def");
    assert_eq!(result[0][0].to_display_string(), "reader@%");
    assert_eq!(result[0][1], owner);
    assert_eq!(
        rows(&engine, &mut reader, "SELECT u FROM inv")[0][0].to_display_string(),
        "reader@%"
    );
}

#[test]
fn audit1022_current_user_password_change_and_binlog_use_the_same_account() {
    let (_dir, engine) = setup();
    engine
        .execute("CREATE USER 'pw'@'%' IDENTIFIED BY 'Old12345!'")
        .unwrap();
    let mut session = SessionContext {
        user: "pw".into(),
        authenticated_as: crate::engine::auth::AccountKey::new("pw", "%"),
        ..SessionContext::default()
    };
    let sql = "SET PASSWORD FOR CURRENT_USER()='New12345!'";
    engine.execute_with_session(&mut session, sql).unwrap();
    let replay = credential_safe_binlog_sql(engine.db_for_tests(), &session, sql).unwrap();
    assert!(replay.starts_with("ALTER USER 'pw'@'%'"), "{replay}");
    assert!(!replay.contains("New12345!"));
}

#[test]
fn audit1022_touches_preserves_isolated_points_in_mixed_collections() {
    let (_dir, engine) = setup();
    let mut session = SessionContext::default();
    for (collection, expected) in [
        ("GEOMETRYCOLLECTION(POINT(1 1),LINESTRING(0 0,2 0))", 1),
        ("GEOMETRYCOLLECTION(POINT(1.5 1.5),LINESTRING(0 1,1 1))", 0),
        (
            "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(1 1)),LINESTRING(0 0,2 0))",
            1,
        ),
    ] {
        let polygon = "POLYGON((1 1,2 1,2 2,1 2,1 1))";
        assert_eq!(rows(&engine,&mut session,&format!("SELECT ST_Touches(ST_GeomFromText('{collection}'),ST_GeomFromText('{polygon}')),ST_Touches(ST_GeomFromText('{polygon}'),ST_GeomFromText('{collection}'))")),vec![vec![Value::Int(expected),Value::Int(expected)]]);
    }
}

#[test]
fn audit1022_routine_set_and_select_into_finish_their_own_row_counts() {
    let (_dir, engine) = setup();
    let mut session = SessionContext::default();
    engine.execute("CREATE TABLE rc(id INT)").unwrap();
    engine.execute("INSERT INTO rc VALUES(1),(2)").unwrap();
    engine.execute("CREATE PROCEDURE rcp(OUT a INT,OUT b INT,OUT c INT) BEGIN UPDATE rc SET id=id+1; SET a=ROW_COUNT(); SET b=ROW_COUNT(); SELECT 1 INTO @x; SET c=ROW_COUNT(); END").unwrap();
    engine
        .execute_with_session(&mut session, "CALL rcp(@a,@b,@c)")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT @a,@b,@c"),
        vec![vec![Value::Int(2), Value::Int(0), Value::Int(1)]]
    );
}
