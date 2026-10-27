use super::*;

fn setup(sql: &[&str]) -> (tempfile::TempDir, StorageQueryEngine) {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    for statement in sql {
        engine.execute(statement).unwrap();
    }
    (dir, engine)
}

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<String>> {
    let QueryResponse::ResultSet(result) = engine
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
        .materialize()
        .unwrap()
    else {
        panic!("expected rows: {sql}");
    };
    result
        .rows
        .into_iter()
        .map(|row| row.into_iter().map(|v| v.to_display_string()).collect())
        .collect()
}

#[test]
fn sqlfix_b01_multi_dml_preserves_column_collation() {
    let (_dir, engine) = setup(&[
        "CREATE TABLE a(id INT PRIMARY KEY,s VARCHAR(10) COLLATE utf8mb4_bin,v INT)",
        "CREATE TABLE b(id INT PRIMARY KEY,s VARCHAR(10) COLLATE utf8mb4_bin)",
        "INSERT INTO a VALUES(1,'a',0),(2,'A',0)",
        "INSERT INTO b VALUES(1,'A')",
    ]);
    assert_eq!(
        rows(&engine, "SELECT a.id FROM a JOIN b ON a.s=b.s"),
        vec![vec!["2"]]
    );
    engine
        .execute("UPDATE a JOIN b ON a.s=b.s SET a.v=1")
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT v FROM a ORDER BY id"),
        vec![vec!["0"], vec!["1"]]
    );
    engine.execute("DELETE a FROM a JOIN b ON a.s=b.s").unwrap();
    assert_eq!(rows(&engine, "SELECT id FROM a"), vec![vec!["1"]]);
}

#[test]
fn sqlfix_b02_multi_default_uses_qualified_source() {
    let (_dir, engine) = setup(&[
        "CREATE TABLE a(id INT PRIMARY KEY,v INT DEFAULT 10)",
        "CREATE TABLE b(id INT PRIMARY KEY,v INT DEFAULT 20)",
        "INSERT INTO a VALUES(1,0)",
        "INSERT INTO b VALUES(1,0)",
    ]);
    engine
        .execute("UPDATE a x JOIN b y ON x.id=y.id SET y.v=DEFAULT(y.v)")
        .unwrap();
    assert_eq!(rows(&engine, "SELECT v FROM b"), vec![vec!["20"]]);
}

#[test]
fn sqlfix_b03_empty_multi_dml_still_binds_expressions() {
    let (_dir, engine) = setup(&["CREATE TABLE a(id INT,v INT)", "CREATE TABLE b(id INT)"]);
    for (sql, code) in [
        ("UPDATE a JOIN b ON a.missing=b.id SET a.v=2", 1054),
        ("DELETE a FROM a JOIN b ON a.missing=b.id", 1054),
        (
            "UPDATE a JOIN b ON a.id=b.id SET a.v=nonexistent_function(b.id)",
            1305,
        ),
        (
            "UPDATE a JOIN b ON a.id=b.id SET a.v=2 WHERE b.missing=0",
            1054,
        ),
    ] {
        assert_eq!(engine.execute(sql).unwrap_err().code(), code, "{sql}");
    }
}

#[test]
fn sqlfix_b04_setop_collation_is_branch_order_independent() {
    let (_dir, engine) = setup(&[
        "CREATE TABLE a(s VARCHAR(10) COLLATE utf8mb4_general_ci)",
        "CREATE TABLE b(s VARCHAR(10) COLLATE utf8mb4_bin)",
        "INSERT INTO a VALUES('a')",
        "INSERT INTO b VALUES('A')",
    ]);
    for (a, b) in [("a", "b"), ("b", "a")] {
        assert_eq!(
            rows(
                &engine,
                &format!("SELECT COUNT(*) FROM (SELECT s FROM {a} UNION SELECT s FROM {b}) u")
            ),
            vec![vec!["2"]]
        );
    }
    let err = engine
        .execute(
            "SELECT 'a' COLLATE utf8mb4_general_ci UNION SELECT 'A' COLLATE utf8mb4_unicode_ci",
        )
        .unwrap_err();
    assert!([1267, 1271].contains(&err.code()), "{err:?}");
}

#[test]
fn sqlfix_setop_preserves_explicit_derivation_through_derived() {
    let (_dir, engine) = setup(&[]);
    for sql in [
        "SELECT s FROM (SELECT 'a' COLLATE utf8mb4_bin AS s) d UNION SELECT 'A' COLLATE utf8mb4_general_ci",
        "WITH d AS (SELECT 'a' COLLATE utf8mb4_bin AS s) SELECT s FROM d UNION SELECT 'A' COLLATE utf8mb4_general_ci",
    ] {
        assert_eq!(engine.execute(sql).unwrap_err().code(),1267,"{sql}");
    }
    assert_eq!(
        rows(
            &engine,
            "SELECT COERCIBILITY(s) FROM (SELECT 'a' COLLATE utf8mb4_bin AS s) d"
        ),
        vec![vec!["0"]]
    );
}

#[test]
fn sqlfix_b05_temporal_range_preserves_microseconds() {
    let (_dir, engine) = setup(&[
        "CREATE TABLE t(id INT,ts DATETIME(6))",
        "INSERT INTO t VALUES(1,'2500-01-01 00:00:00.000001'),(2,'2500-01-01 00:00:00.000002'),(3,'2500-01-01 00:00:00.000003'),(4,NULL)",
    ]);
    for (direction, bound) in [("ASC", "PRECEDING"), ("DESC", "FOLLOWING")] {
        let frame = if bound == "PRECEDING" {
            "INTERVAL 1 MICROSECOND PRECEDING AND CURRENT ROW"
        } else {
            "CURRENT ROW AND INTERVAL 1 MICROSECOND FOLLOWING"
        };
        assert_eq!(rows(&engine,&format!("SELECT id,COUNT(*) OVER(ORDER BY ts {direction} RANGE BETWEEN {frame}) FROM t ORDER BY id")),vec![vec!["1","1"],vec!["2","2"],vec!["3","2"],vec!["4","1"]]);
    }
}

#[test]
fn sqlfix_b06_foreign_query_tails_are_rejected() {
    let (_dir, engine) = setup(&[]);
    for tail in ["FOR XML AUTO", "FOR JSON AUTO", "FOR BROWSE"] {
        let err = engine.execute(&format!("SELECT 1 {tail}")).unwrap_err();
        assert!([1064, 1235].contains(&err.code()), "{err:?}");
    }
}

#[test]
fn sqlfix_b07_minmax_preserves_argument_comparison_domain() {
    let (_dir, engine) = setup(&[
        "CREATE TABLE t(id INT,s VARCHAR(10) COLLATE utf8mb4_bin,INDEX ix(s))",
        "INSERT INTO t VALUES(1,'Z'),(2,'a'),(3,NULL)",
    ]);
    assert_eq!(
        rows(&engine, "SELECT MIN(s) FROM t FORCE INDEX(ix)"),
        vec![vec!["Z"]]
    );
    for sql in [
        "SELECT MIN(s),MAX(s) FROM t IGNORE INDEX(ix)",
        "SELECT MIN(s),MAX(s) FROM t GROUP BY id%1",
        "SELECT MIN(s COLLATE utf8mb4_bin),MAX(s COLLATE utf8mb4_bin) FROM t",
        "SELECT MIN(s) OVER(),MAX(s) OVER() FROM t LIMIT 1",
    ] {
        assert_eq!(rows(&engine, sql), vec![vec!["Z", "a"]], "{sql}");
    }
}

#[test]
fn sqlfix_b08_multi_dml_explain_reports_actual_join() {
    let (_dir, engine) = setup(&[
        "CREATE TABLE a(id INT PRIMARY KEY,v INT)",
        "CREATE TABLE b(id INT PRIMARY KEY)",
        "INSERT INTO a VALUES(1,0)",
        "INSERT INTO b VALUES(1)",
    ]);
    for sql in [
        "EXPLAIN UPDATE a JOIN b ON a.id=b.id SET a.v=2",
        "EXPLAIN DELETE a FROM a JOIN b ON a.id=b.id",
    ] {
        let text = rows(&engine, sql)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join(" ");
        assert!(!text.to_ascii_lowercase().contains("hash join"), "{text}");
        assert!(text.to_ascii_lowercase().contains("nested loop"), "{text}");
    }
}

#[test]
fn sqlfix_default_qualification_survives_printing_and_joins() {
    let (_dir, engine) = setup(&[
        "CREATE TABLE a(id INT PRIMARY KEY, `v.x` INT DEFAULT 11)",
        "CREATE TABLE b(id INT PRIMARY KEY, `v.x` INT DEFAULT 22)",
        "INSERT INTO a VALUES(1,0)",
        "INSERT INTO b VALUES(1,0)",
    ]);
    let expression = crate::parser::parse_expr_fragment("DEFAULT(`别名`.`v.x`)").unwrap();
    let printed = crate::parser::expr_to_sql(&expression);
    assert_eq!(
        crate::parser::parse_expr_fragment(&printed).unwrap(),
        expression
    );
    engine.execute("UPDATE a x JOIN b y ON x.id=y.id SET x.`v.x`=DEFAULT(y.`v.x`),y.`v.x`=DEFAULT(x.`v.x`)").unwrap();
    assert_eq!(
        rows(
            &engine,
            "SELECT x.`v.x`,y.`v.x` FROM a x JOIN b y ON x.id=y.id"
        ),
        vec![vec!["22", "11"]]
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT DEFAULT(y.`v.x`) FROM a x JOIN b y ON x.id=y.id"
        ),
        vec![vec!["22"]]
    );
    assert_eq!(
        engine
            .execute("UPDATE a x JOIN b y ON x.id=y.id SET x.`v.x`=DEFAULT(missing.`v.x`)")
            .unwrap_err()
            .code(),
        1054
    );
}

#[test]
fn sqlfix_multi_bound_expressions_keep_units_and_short_circuiting() {
    let (_dir, engine) = setup(&[
        "CREATE TABLE a(id INT PRIMARY KEY,v INT)",
        "CREATE TABLE b(id INT)",
        "INSERT INTO a VALUES(1,0)",
        "INSERT INTO b VALUES(1)",
    ]);
    engine.execute("UPDATE a JOIN b ON a.id=b.id SET a.v=IF(b.id=1,TIMESTAMPDIFF(DAY,'2026-01-01','2026-01-03'),1/0)").unwrap();
    assert_eq!(rows(&engine, "SELECT v FROM a"), vec![vec!["2"]]);
}

#[test]
fn sqlfix_minmax_mixed_collations_rollup_and_window_frames() {
    let (_dir, engine) = setup(&[
        "CREATE TABLE t(id INT,g INT,s VARCHAR(10) COLLATE utf8mb4_bin)",
        "INSERT INTO t VALUES(1,1,'Z'),(2,1,'a'),(3,2,'Y'),(4,2,'b')",
    ]);
    for mode in [
        crate::engine::executor::vec::config::VecMode::Off,
        crate::engine::executor::vec::config::VecMode::On,
        crate::engine::executor::vec::config::VecMode::Auto,
    ] {
        crate::engine::executor::vec::config::with_vec_mode(mode, || {
            assert_eq!(rows(&engine,"SELECT MIN(s),MIN(s COLLATE utf8mb4_general_ci),MAX(s),MAX(s COLLATE utf8mb4_general_ci) FROM t"),vec![vec!["Y","a","b","Z"]]);
            let mut actual = rows(
                &engine,
                "SELECT g,MIN(s),MAX(s) FROM t GROUP BY g WITH ROLLUP",
            );
            actual.sort();
            assert_eq!(
                actual,
                vec![
                    vec!["1", "Z", "a"],
                    vec!["2", "Y", "b"],
                    vec!["NULL", "Y", "b"]
                ]
            );
            assert_eq!(rows(&engine,"SELECT id,MIN(s) OVER(ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW),MAX(s) OVER(ORDER BY id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t ORDER BY id"),vec![vec!["1","Z","Z"],vec!["2","Z","a"],vec!["3","Y","a"],vec!["4","Y","b"]]);
        });
    }
}

#[test]
fn sqlfix_minmax_group_spill_matches_declared_domain() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    let (_dir, engine) = setup(&[
        "CREATE TABLE t(g INT,s VARCHAR(64) COLLATE utf8mb4_bin)",
        "INSERT INTO t SELECT n,'Z' FROM generate_series(1,2000) AS x(n)",
        "INSERT INTO t SELECT n,'a' FROM generate_series(1,2000) AS x(n)",
    ]);
    let sql = "SELECT g,MIN(s),MAX(s) FROM t GROUP BY g ORDER BY g";
    let expected = rows(&engine, sql);
    assert_eq!(expected.len(), 2000);
    assert!(expected.iter().all(|row| row[1] == "Z" && row[2] == "a"));
    let budget = Arc::new(MemBudget::new(64 * 1024));
    let actual = with_query_budget(budget.clone(), || rows(&engine, sql));
    assert_eq!(actual, expected);
    assert_eq!(budget.used(), 0);
}

#[test]
fn sqlfix_native_math_preserves_argument_domains() {
    let (_dir, engine) = setup(&[
        "CREATE TABLE t(id INT,x VARCHAR(20),y DECIMAL(18,4),z BIGINT UNSIGNED)",
        "INSERT INTO t VALUES(1,'1',2,0),(2,'0',1.5,1),(3,NULL,NULL,NULL)",
    ]);
    let result = support::query_rows(
        &engine,
        "SELECT LN(x),POWER(y,2),ATAN(z) FROM t ORDER BY id",
    );
    assert_eq!(
        result[0],
        vec![Value::Double(0.0), Value::Double(4.0), Value::Double(0.0)]
    );
    assert_eq!(result[1][0], Value::Null);
    assert_eq!(result[1][1], Value::Double(2.25));
    let Value::Double(angle) = result[1][2] else {
        panic!("expected a DOUBLE angle")
    };
    assert!((angle - std::f64::consts::FRAC_PI_4).abs() < 1e-14);
    assert_eq!(result[2], vec![Value::Null; 3]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlfix_binary_prepared_minmax_union_and_multi_update() {
    let (_dir, engine) = setup(&[
        "CREATE USER 'sqlfix'@'%' IDENTIFIED BY ''",
        "GRANT ALL ON test.* TO 'sqlfix'@'%'",
        "CREATE TABLE a(id INT PRIMARY KEY,s VARCHAR(10) COLLATE utf8mb4_bin,v INT DEFAULT 10)",
        "CREATE TABLE b(id INT PRIMARY KEY,s VARCHAR(10) COLLATE utf8mb4_bin,v INT DEFAULT 20)",
        "INSERT INTO a VALUES(1,'Z',0),(2,'a',0)",
        "INSERT INTO b VALUES(1,'A',0),(2,'a',0)",
    ]);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let client = tokio::task::spawn_blocking(move || {
        use ::mysql::prelude::Queryable;
        let options = ::mysql::OptsBuilder::new()
            .ip_or_hostname(Some("127.0.0.1"))
            .tcp_port(port)
            .user(Some("sqlfix"))
            .pass(Some(""))
            .db_name(Some("test"));
        let mut connection = ::mysql::Conn::new(options).unwrap();
        let aggregate = connection
            .prep("SELECT MIN(s),MAX(s) FROM a WHERE id>=?")
            .unwrap();
        assert_eq!(aggregate.columns().len(), 2);
        for _ in 0..2 {
            let result: Vec<(String, String)> = connection.exec(&aggregate, (1,)).unwrap();
            assert_eq!(result, vec![("Z".into(), "a".into())]);
        }
        let set = connection
            .prep("SELECT s FROM a UNION SELECT s FROM b ORDER BY s")
            .unwrap();
        let result: Vec<String> = connection.exec(&set, ()).unwrap();
        assert_eq!(result, vec!["A", "Z", "a"]);
        let update = connection
            .prep("UPDATE a JOIN b ON a.s=b.s SET a.v=DEFAULT(b.v)")
            .unwrap();
        connection.exec_drop(&update, ()).unwrap();
        let result: Vec<(i32, i32)> = connection.query("SELECT id,v FROM a ORDER BY id").unwrap();
        assert_eq!(result, vec![(1, 0), (2, 20)]);
    });
    let (socket, peer) = listener.accept().await.unwrap();
    let server = tokio::spawn(async move {
        let mut session = crate::mysql::session::Session::new(
            73,
            peer,
            socket,
            None,
            Arc::new(engine),
            Arc::new(SessionRegistry::new()),
        );
        session.run().await.unwrap();
    });
    tokio::time::timeout(Duration::from_secs(30), client)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .unwrap()
        .unwrap();
}

#[test]
fn sqlfix_projected_distinct_wide_expressions_complete_under_spill_watermark() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    let (_dir, engine) = setup(&[]);
    let sql = "SELECT DISTINCT MOD(n,4096) AS k,CONCAT(MOD(n,4096),REPEAT('x',512)) AS v FROM generate_series(1,24000) AS s(n) ORDER BY k,v";
    let expected = rows(&engine, sql);
    assert_eq!(expected.len(), 4096);
    let budget = Arc::new(MemBudget::new(1024 * 1024));
    let actual = with_query_budget(budget.clone(), || rows(&engine, sql));
    assert_eq!(actual.len(), expected.len());
    assert!(
        actual
            .iter()
            .zip(&expected)
            .all(|(actual, expected)| actual == expected),
        "spilled projected DISTINCT changed values or ordering"
    );
    assert_eq!(budget.used(), 0);
    assert_eq!(budget.spill_bytes(), 0);
}

#[test]
fn sqlfix_wide_computed_inputs_do_not_repeat_session_assignments_across_chunks() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    let (_dir, engine) = setup(&[]);
    let mut session = SessionContext::default();
    let mut run = |sql: &str| {
        let QueryResponse::ResultSet(result) = engine
            .execute_with_session(&mut session, sql)
            .unwrap()
            .materialize()
            .unwrap()
        else {
            panic!("rows");
        };
        result.rows
    };
    for mode in [
        crate::engine::executor::vec::config::VecMode::Off,
        crate::engine::executor::vec::config::VecMode::On,
    ] {
        let _mode = crate::engine::executor::vec::config::enter_mode(mode);
        run("SELECT @spill_counter := 0");
        let budget = Arc::new(MemBudget::new(1024 * 1024));
        let result = with_query_budget(budget.clone(), || {
            run(
            "SELECT LENGTH(MIN(CONCAT(@spill_counter:=@spill_counter+1,REPEAT('x',65536)))),MAX(@spill_counter) FROM generate_series(1,32) AS s(n)")
        });
        assert_eq!(result[0][1], Value::Int(32));
        assert_eq!(run("SELECT @spill_counter")[0][0], Value::Int(32));
        assert_eq!(budget.used(), 0);
        run("SELECT @spill_counter := 0");
        let result = with_query_budget(budget.clone(), || {
            run(
            "SELECT n FROM generate_series(1,32) AS s(n) ORDER BY CONCAT(LPAD(@spill_counter:=@spill_counter+1,3,'0'),REPEAT('x',65536)) DESC,@spill_counter")
        });
        assert_eq!(result.len(), 32);
        for (index, row) in result.iter().enumerate() {
            assert_eq!(row[0], Value::Int(32 - index as i64));
        }
        assert_eq!(run("SELECT @spill_counter")[0][0], Value::Int(32));
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.spill_bytes(), 0);
    }
}

#[test]
fn sqlfix_bounded_scalar_state_can_drain_an_indivisible_wide_input_row() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    let (_dir, engine) = setup(&[]);
    let budget = Arc::new(MemBudget::new(1024 * 1024));
    with_query_budget(budget.clone(), || {
        assert_eq!(rows(&engine,
            "SELECT LENGTH(GROUP_CONCAT(REPEAT('x',1048576) ORDER BY n)) FROM generate_series(1,3) AS s(n)"),
            vec![vec!["1024"]]);
    });
    assert_eq!(budget.used(), 0);
    assert_eq!(budget.spill_bytes(), 0);
}
