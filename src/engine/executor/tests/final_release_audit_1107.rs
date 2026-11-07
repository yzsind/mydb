use super::support::execute_materialized as exec;
use super::*;

fn rows(engine: &StorageQueryEngine, s: &mut SessionContext, sql: &str) -> Vec<Vec<String>> {
    let QueryResponse::ResultSet(result) = exec(engine, s, sql).expect(sql) else {
        panic!("expected rows: {sql}");
    };
    result
        .rows
        .iter()
        .map(|r| r.iter().map(Value::to_display_string).collect())
        .collect()
}

#[test]
fn final_1107_time_numeric_equality_keys() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in [
        "CREATE TABLE t(id INT PRIMARY KEY,v TIME(6))",
        "INSERT INTO t VALUES(1,'01:00:00'),(2,'-12:34:56.123456'),(3,'838:59:59'),(4,NULL)",
        "CREATE TABLE n(id INT PRIMARY KEY,v DECIMAL(20,6))",
        "INSERT INTO n VALUES(1,10000),(2,-123456.123456),(3,8385959),(4,NULL)",
    ] {
        exec(&engine, &mut s, sql).unwrap();
    }
    for sql in [
        "SELECT t.id FROM t JOIN n ON t.v=n.v ORDER BY t.id",
        "SELECT t.id FROM n JOIN t ON n.v=t.v ORDER BY t.id",
        "SELECT id FROM t WHERE v IN (SELECT v FROM n) ORDER BY id",
        "SELECT /*+ NO_HASH_JOIN(n) */ t.id FROM t JOIN n ON t.v=n.v ORDER BY t.id",
    ] {
        assert_eq!(
            rows(&engine, &mut s, sql),
            [vec!["1"], vec!["2"], vec!["3"]],
            "{sql}"
        );
    }
}

#[test]
fn final_1107_superaggregate_predicates_stay_above_grouping() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "CREATE TABLE t(k INT,v INT)").unwrap();
    exec(
        &engine,
        &mut s,
        "INSERT INTO t VALUES(1,10),(2,20),(6,60),(7,70)",
    )
    .unwrap();
    for group in ["k WITH ROLLUP", "GROUPING SETS ((k),())", "CUBE(k)"] {
        for predicate in ["k>5", "k IS NOT NULL AND k>5"] {
            let sql = format!("SELECT k,m FROM (SELECT k,AVG(v) m FROM t GROUP BY {group}) d WHERE {predicate} ORDER BY k");
            assert_eq!(
                rows(&engine, &mut s, &sql),
                [vec!["6", "60.0000"], vec!["7", "70.0000"]]
            );
        }
    }
}

#[test]
fn final_1107_scalar_derived_outer_scope() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "CREATE TABLE t(k INT,x INT)").unwrap();
    exec(&engine, &mut s, "INSERT INTO t VALUES(1,100),(2,200)").unwrap();
    for sql in [
        "SELECT d.a,(SELECT MAX(t2.x) FROM t t2 WHERE t2.k=d.a AND t2.x>a) FROM (SELECT k a FROM t) d ORDER BY d.a",
        "SELECT d.a,(SELECT MAX(t2.x) FROM t t2 WHERE t2.k=d.a AND t2.x>a) FROM (SELECT k FROM t) d(a) ORDER BY d.a",
        "WITH c AS(SELECT k a FROM t) SELECT d.a,(SELECT MAX(t2.x) FROM t t2 WHERE t2.k=d.a AND t2.x>a) FROM c d ORDER BY d.a",
    ] { assert_eq!(rows(&engine,&mut s,sql),[vec!["1","100"],vec!["2","200"]],"{sql}"); }
}

#[test]
fn final_1107_cte_window_columns_survive_pruning() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "CREATE TABLE t(id INT,v INT)").unwrap();
    exec(&engine, &mut s, "INSERT INTO t VALUES(1,10),(2,20),(3,30)").unwrap();
    for window in ["OVER(ORDER BY a.id)", "OVER w"] {
        let suffix = if window == "OVER w" {
            " WINDOW w AS(ORDER BY a.id)"
        } else {
            ""
        };
        let sql = format!("WITH c AS(SELECT id,v FROM t) SELECT a.v,ROW_NUMBER() {window} rn FROM c a JOIN c b ON a.v=b.v{suffix} ORDER BY a.v");
        assert_eq!(
            rows(&engine, &mut s, &sql),
            [vec!["10", "1"], vec!["20", "2"], vec!["30", "3"]]
        );
    }
    assert_eq!(rows(&engine,&mut s,"WITH c AS(SELECT id,v FROM t) SELECT SUM(a.id) OVER(PARTITION BY a.v ORDER BY a.id) FROM c a JOIN c b ON a.v=b.v ORDER BY a.v"),[vec!["1"],vec!["2"],vec!["3"]]);
}

#[test]
fn final_1107_repeated_update_assignments_in_source_order() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(
        &engine,
        &mut s,
        "CREATE TABLE t(id INT PRIMARY KEY,a INT,b INT)",
    )
    .unwrap();
    exec(&engine, &mut s, "INSERT INTO t VALUES(1,1,0)").unwrap();
    for sql in [
        "UPDATE t SET a=a+1,b=a,a=a*10 WHERE id=1",
        "UPDATE t SET a=a+1,b=a,a=a*10",
        "PREPARE u FROM 'UPDATE t SET a=a+1,b=a,a=a*10 WHERE id=1'",
    ] {
        exec(&engine, &mut s, "UPDATE t SET a=1,b=0").unwrap();
        exec(&engine, &mut s, sql).unwrap();
        if sql.starts_with("PREPARE") {
            exec(&engine, &mut s, "EXECUTE u").unwrap();
        }
        assert_eq!(
            rows(&engine, &mut s, "SELECT a,b FROM t"),
            [vec!["20", "2"]],
            "{sql}"
        );
    }
}

#[test]
fn final_1107_json_integer_assignment_checks_range() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "SET sql_mode='STRICT_ALL_TABLES'").unwrap();
    exec(
        &engine,
        &mut s,
        "CREATE TABLE t(a BIGINT,b BIGINT UNSIGNED)",
    )
    .unwrap();
    for (column, json) in [
        ("a", "18446744073709551615"),
        ("a", "1e300"),
        ("a", "-1e300"),
        ("b", "1e300"),
        ("b", "-1"),
    ] {
        let sql = format!("INSERT INTO t({column}) VALUES(CAST('{json}' AS JSON))");
        assert_eq!(
            exec(&engine, &mut s, &sql).unwrap_err().code(),
            1264,
            "{sql}"
        );
    }
    exec(&engine,&mut s,"INSERT INTO t VALUES(CAST('9223372036854775807' AS JSON),CAST('18446744073709551615' AS JSON))").unwrap();
    assert_eq!(
        rows(&engine, &mut s, "SELECT * FROM t"),
        [vec!["9223372036854775807", "18446744073709551615"]]
    );
}

#[test]
fn final_1107_wide_decimal_average_is_exact() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "CREATE TABLE t(v DECIMAL(65,0))").unwrap();
    exec(
        &engine,
        &mut s,
        &format!("INSERT INTO t VALUES({}),(1)", "9".repeat(65)),
    )
    .unwrap();
    let expected = format!("5{}.0000", "0".repeat(64));
    for sql in [
        "SELECT AVG(v) FROM t",
        "SELECT AVG(v) FROM t GROUP BY 1+0",
        "SELECT AVG(v) OVER() FROM t LIMIT 1",
    ] {
        assert_eq!(
            rows(&engine, &mut s, sql),
            [vec![expected.clone()]],
            "{sql}"
        );
    }
}

#[test]
fn final_1107_parser_quoted_identifier_boundaries() {
    let sql = "SELECT 1; SELECT `a\0b`";
    let (_, ranges) = crate::parser::split_query_statements(sql, 0);
    assert_eq!(ranges.len(), 2);
    assert!(crate::parser::parse_statement(&sql[ranges[1].clone()]).is_err());
    assert!(
        crate::parser::parse_statement("LOAD DATA INFILE '/tmp/mydb/a' INTO TABLE `a``b`").is_ok()
    );
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE `a\\`(a INT DEFAULT 1)")
        .unwrap();
    engine.execute("INSERT INTO `a\\` VALUES ()").unwrap();
}

#[test]
fn final_1107_wide_decimal_average_reserves_integer_words_before_fraction() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "CREATE TABLE t(v DECIMAL(65,0))").unwrap();
    for sign in ["", "-"] {
        exec(&engine, &mut s, "DELETE FROM t").unwrap();
        exec(
            &engine,
            &mut s,
            &format!(
                "INSERT INTO t VALUES({sign}{}),({sign}1),({sign}1)",
                "9".repeat(65)
            ),
        )
        .unwrap();
        for (increment, fraction) in [(4, "6667"), (9, "666666666"), (30, "666666666")] {
            exec(
                &engine,
                &mut s,
                &format!("SET SESSION div_precision_increment={increment}"),
            )
            .unwrap();
            let expected = format!("{sign}{}.{fraction}", "3".repeat(65));
            for sql in [
                "SELECT AVG(v) FROM t",
                "SELECT AVG(v) FROM t GROUP BY 1+0",
                "SELECT AVG(v) OVER() FROM t LIMIT 1",
            ] {
                assert_eq!(
                    rows(&engine, &mut s, sql),
                    [vec![expected.clone()]],
                    "{sign} increment={increment} {sql}"
                );
            }
        }
    }
    exec(&engine, &mut s, "CREATE TABLE fractional(v DECIMAL(65,10))").unwrap();
    exec(
        &engine,
        &mut s,
        &format!(
            "INSERT INTO fractional VALUES({}.9999999999)",
            "9".repeat(55)
        ),
    )
    .unwrap();
    assert_eq!(
        rows(&engine, &mut s, "SELECT AVG(v) FROM fractional"),
        [vec![format!("{}.999999999900000000", "9".repeat(55))]]
    );
}

#[test]
fn final_1107_function_cte_result_set_rejected_at_creation() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let sql="CREATE FUNCTION f() RETURNS INT DETERMINISTIC BEGIN WITH c AS(SELECT 1 a) SELECT a FROM c; RETURN 1; END";
    assert_eq!(engine.execute(sql).unwrap_err().code(), 1415);
    let err = engine
        .execute("CREATE TABLE t(a INT DEFAULT (unknown_function(1)))")
        .unwrap_err();
    assert_eq!(err.code(), 3770);
    for sql in [
        "CREATE TABLE cte_target(id INT PRIMARY KEY,v INT)",
        "INSERT INTO cte_target VALUES(1,0)",
        "CREATE FUNCTION f_update() RETURNS INT MODIFIES SQL DATA BEGIN WITH c AS(SELECT 1 id) UPDATE cte_target SET v=v+1 WHERE id IN(SELECT id FROM c); RETURN 1; END",
    ] {engine.execute(sql).unwrap();}
    let mut session = SessionContext::default();
    assert_eq!(
        rows(&engine, &mut session, "SELECT f_update()"),
        [vec!["1"]]
    );
    assert_eq!(
        rows(&engine, &mut session, "SELECT v FROM cte_target"),
        [vec!["1"]]
    );
}

#[test]
fn final_1107_session_uints_are_isolated_and_startup_limits_readonly() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut a = SessionContext::default();
    let mut b = SessionContext::default();
    let before = rows(&engine, &mut b, "SELECT @@max_join_size");
    exec(&engine, &mut a, "SET SESSION max_join_size=5").unwrap();
    assert_eq!(rows(&engine, &mut b, "SELECT @@max_join_size"), before);
    assert_eq!(rows(&engine, &mut a, "SELECT @@max_join_size"), [vec!["5"]]);
    exec(&engine, &mut a, "SET lc_messages='EN_US'").unwrap();
    assert_eq!(
        rows(&engine, &mut a, "SELECT @@lc_messages"),
        [vec!["en_US"]]
    );
    for sql in [
        "SET GLOBAL back_log=1000",
        "SET GLOBAL open_files_limit=10000",
        "SET lc_messages='made_up'",
        "SET lc_messages='de_DE'",
    ] {
        assert!(exec(&engine, &mut a, sql).is_err(), "{sql}");
    }
}

#[test]
fn final_1107_deadlock_rollback_releases_xa_binding() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "XA START 'deadlock_branch'").unwrap();
    let txn = s.current_txn.unwrap();
    engine
        .finalize_failed_statement_transaction(
            &mut s,
            &crate::error::catalog::ER_LOCK_DEADLOCK.error("injected deadlock"),
        )
        .unwrap();
    assert!(!engine.db_for_tests().is_txn_active(txn));
    assert!(s.xa_branch.is_none());
    assert_eq!(
        exec(&engine, &mut s, "XA END 'deadlock_branch'")
            .unwrap_err()
            .code(),
        1399
    );
    exec(&engine, &mut s, "XA START 'deadlock_branch'").unwrap();
    exec(&engine, &mut s, "XA END 'deadlock_branch'").unwrap();
    exec(&engine, &mut s, "XA ROLLBACK 'deadlock_branch'").unwrap();
}

#[test]
fn final_1107_multitable_alias_assignments_follow_set_order() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY,v INT)").unwrap();
    exec(&engine, &mut s, "INSERT INTO t VALUES(1,0)").unwrap();
    for assignments in ["a.v=1,b.v=2", "b.v=1,a.v=2", "a.v=0,b.v=1,a.v=2"] {
        exec(
            &engine,
            &mut s,
            &format!("UPDATE t a JOIN t b ON a.id=b.id SET {assignments}"),
        )
        .unwrap();
        assert_eq!(
            rows(&engine, &mut s, "SELECT v FROM t"),
            [vec!["2"]],
            "{assignments}"
        );
    }
}

#[test]
fn final_1107_load_decimal_integer_rounding_matches_insert() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    let path = dir.path().join("numbers.txt");
    std::fs::write(
        &path,
        "12.7\t12.7\n-12.7\t0\n1e2\t1e2\n9223372036854775806.7\t18446744073709551614.7\n",
    )
    .unwrap();
    exec(
        &engine,
        &mut s,
        "CREATE TABLE t(a BIGINT,b BIGINT UNSIGNED)",
    )
    .unwrap();
    exec(
        &engine,
        &mut s,
        &format!("LOAD DATA INFILE '{}' INTO TABLE t", path.display()),
    )
    .unwrap();
    assert_eq!(
        rows(&engine, &mut s, "SELECT a,b FROM t ORDER BY a"),
        [
            vec!["-13", "0"],
            vec!["13", "13"],
            vec!["100", "100"],
            vec!["9223372036854775807", "18446744073709551615"]
        ]
    );
}

#[test]
fn final_1107_spatial_simplicity_checks_components_and_intersections() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for (wkt, simple) in [
        ("POLYGON((0 0,2 2,0 2,2 0,0 0))", "1"),
        ("GEOMETRYCOLLECTION(LINESTRING(0 0,2 2,0 2,2 0))", "0"),
        ("GEOMETRYCOLLECTION(POINT(1 1),POINT(1 1))", "0"),
        ("GEOMETRYCOLLECTION(POINT(1 1),LINESTRING(0 0,2 2))", "0"),
        ("GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(0 0,2 2))", "1"),
        ("MULTILINESTRING((0 0,2 2),(0 2,2 0))", "0"),
        ("MULTILINESTRING((0 0,2 2),(1 1,3 3))", "0"),
        ("MULTILINESTRING((0 0,1 1),(1 1,2 0))", "1"),
        ("MULTILINESTRING((0 0,2 2),(1 1,2 0))", "0"),
        (
            "GEOMETRYCOLLECTION(LINESTRING(0 0,2 2),LINESTRING(1 1,2 0))",
            "1",
        ),
    ] {
        let sql = format!("SELECT ST_IsSimple(ST_GeomFromText('{wkt}'))");
        assert_eq!(rows(&engine, &mut s, &sql), [vec![simple]], "{wkt}");
    }
}

#[test]
fn final_1107_persisted_startup_error_is_logged_without_value() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let db = engine.db_for_tests();
    db.persisted_variables()
        .upsert(
            "max_connections",
            &crate::engine::sysvar::SysVarValue::String("persisted-secret-1107".into()),
            "root",
            "localhost",
            0,
        )
        .unwrap();
    crate::engine::sysvar::apply_persisted_on_startup(db);
    let rows = crate::engine::logging::error_log_rows();
    assert!(rows.iter().flatten().any(|value| value
        .to_display_string()
        .contains("failed to apply persisted variable 'max_connections'")));
    assert!(!rows
        .iter()
        .flatten()
        .any(|value| value.to_display_string().contains("persisted-secret-1107")));
}
