//! Regression coverage for the September 2026 window audit.
use super::*;

fn fixture() -> (tempfile::TempDir, StorageQueryEngine) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE w (id INT PRIMARY KEY, v INT, s VARCHAR(20) COLLATE utf8mb4_0900_ai_ci, d DECIMAL(10,2), dt DATE)").unwrap();
    engine.execute("INSERT INTO w VALUES (1,10,'Apple',1.00,'2024-01-31'),(2,10,'apple',1.50,'2024-02-29'),(3,20,'Banana',2.00,'2024-03-31'),(4,NULL,NULL,3.00,'2024-04-30')").unwrap();
    (dir, engine)
}
fn result(e: &StorageQueryEngine, sql: &str) -> ResultSet {
    match e
        .execute(sql)
        .unwrap_or_else(|err| panic!("{sql}: {err:?}"))
        .materialize()
        .unwrap()
    {
        QueryResponse::ResultSet(rs) => rs,
        other => panic!("{other:?}"),
    }
}
fn rows(e: &StorageQueryEngine, sql: &str) -> Vec<Vec<String>> {
    result(e, sql)
        .rows
        .iter()
        .map(|r| r.iter().map(Value::to_display_string).collect())
        .collect()
}
#[test]
fn window_audit_collation_peers() {
    let (_dir, e) = fixture();
    let r = result(
        &e,
        "SELECT CUME_DIST() OVER (ORDER BY s),PERCENT_RANK() OVER (ORDER BY s) FROM w ORDER BY id",
    );
    assert_eq!(r.rows[0], r.rows[1]);
    assert_eq!(
        r.rows[0],
        vec![Value::Double(0.75), Value::Double(1.0 / 3.0)]
    );
}
#[test]
fn window_audit_distinct_after_window_before_limit() {
    let (_dir, e) = fixture();
    assert_eq!(
        rows(
            &e,
            "SELECT DISTINCT v,RANK() OVER (ORDER BY v) r FROM w ORDER BY v"
        ),
        vec![vec!["NULL", "1"], vec!["10", "2"], vec!["20", "4"]]
    );
    assert_eq!(
        result(
            &e,
            "SELECT DISTINCT v,ROW_NUMBER() OVER (ORDER BY id) FROM w"
        )
        .rows
        .len(),
        4
    );
    assert_eq!(
        result(
            &e,
            "SELECT DISTINCT v,RANK() OVER (ORDER BY v) FROM w LIMIT 3"
        )
        .rows
        .len(),
        3
    );
}
#[test]
fn window_audit_qualified_join_dependencies() {
    let (_dir, e) = fixture();
    e.execute("CREATE TABLE b (id INT PRIMARY KEY,v INT)")
        .unwrap();
    e.execute("INSERT INTO b VALUES (1,400),(2,300),(3,200),(4,100)")
        .unwrap();
    assert_eq!(rows(&e,"SELECT a.id,a.v,b.v,ROW_NUMBER() OVER (ORDER BY b.v) FROM w a JOIN b ON a.id=b.id ORDER BY a.id"),vec![vec!["1","10","400","4"],vec!["2","10","300","3"],vec!["3","20","200","2"],vec!["4","NULL","100","1"]]);
    assert_eq!(rows(&e,"SELECT a.id,SUM(a.v) OVER (PARTITION BY b.v) FROM w a JOIN b ON a.id=b.id ORDER BY a.id"), vec![vec!["1","10"],vec!["2","10"],vec!["3","20"],vec!["4","NULL"]]);
}
#[test]
fn window_audit_set_operations() {
    let (_dir, e) = fixture();
    for (op, n) in [
        ("UNION ALL", 8),
        ("UNION", 4),
        ("INTERSECT", 4),
        ("EXCEPT", 0),
    ] {
        let sql=format!("SELECT id, RANK() OVER (ORDER BY v) r FROM w {op} SELECT id,RANK() OVER (ORDER BY v) FROM w");
        assert_eq!(result(&e, &sql).rows.len(), n, "{op}");
        assert_eq!(
            result(&e, &format!("SELECT * FROM ({sql}) q")).rows.len(),
            n,
            "derived {op}"
        );
    }
}
#[test]
fn window_audit_order_only_and_alias() {
    let (_dir, e) = fixture();
    assert_eq!(
        rows(&e, "SELECT id FROM w ORDER BY RANK() OVER (ORDER BY v),id"),
        vec![vec!["4"], vec!["1"], vec!["2"], vec!["3"]]
    );
    assert_eq!(
        rows(
            &e,
            "SELECT id,v AS x,RANK() OVER (ORDER BY x) FROM w ORDER BY id"
        ),
        rows(
            &e,
            "SELECT id,v,RANK() OVER (ORDER BY v) FROM w ORDER BY id"
        )
    );
}
#[test]
fn window_audit_decimal_range() {
    let (_dir, e) = fixture();
    assert_eq!(rows(&e,"SELECT SUM(v) OVER (ORDER BY d RANGE BETWEEN 0.5 PRECEDING AND 0.5 FOLLOWING) FROM w ORDER BY id"),vec![vec!["20"],vec!["40"],vec!["30"],vec!["NULL"]]);
}
#[test]
fn window_audit_bit_metadata() {
    let (_dir, e) = fixture();
    for f in ["BIT_AND", "BIT_OR", "BIT_XOR"] {
        let r = result(&e, &format!("SELECT {f}(v) OVER () FROM w"));
        assert_eq!(r.columns[0].column_type, ColumnType::LongLong);
        assert_ne!(
            r.columns[0].source.flags & crate::mysql::protocol::UNSIGNED_FLAG,
            0
        );
    }
}
#[test]
fn window_audit_invalid_context_and_arguments() {
    let (_dir, e) = fixture();
    for (sql, code) in [
        (
            "SELECT SUM(v) OVER (ROWS BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING) FROM w",
            3584,
        ),
        (
            "SELECT SUM(v) OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) FROM w",
            3585,
        ),
        ("ALTER TABLE w ADD COLUMN gc INT AS (RANK() OVER ())", 3593),
        (
            "SELECT id,RANK() OVER (ORDER BY v) rk FROM w HAVING rk<=2",
            3594,
        ),
    ] {
        assert_eq!(e.execute(sql).unwrap_err().code(), code, "{sql}");
    }
    assert!(e
        .execute("SELECT NTILE(1.5) OVER (ORDER BY id) FROM w")
        .is_err());
    assert!(e
        .execute("SELECT SUM(v) OVER (ROWS 1.5 PRECEDING) FROM w")
        .is_err());
    assert!(e
        .execute("SELECT NTILE(2+1) OVER (ORDER BY id) FROM w")
        .is_ok());
}
#[test]
fn window_audit_prefix_peer_aggregates() {
    let (_dir, e) = fixture();
    for f in [
        "SUM(v)", "AVG(v)", "COUNT(*)", "COUNT(v)", "MIN(s)", "MAX(s)", "SUM(d)", "AVG(d)",
    ] {
        assert_eq!(rows(&e,&format!("SELECT {f} OVER (ORDER BY v) FROM w ORDER BY id")),rows(&e,&format!("SELECT {f} OVER (ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE NO OTHERS) FROM w ORDER BY id")));
    }
    assert_eq!(
        rows(
            &e,
            "SELECT SUM(v) OVER (ORDER BY v),COUNT(*) OVER (ORDER BY v) FROM w ORDER BY id"
        ),
        vec![
            vec!["20", "3"],
            vec!["20", "3"],
            vec!["40", "4"],
            vec!["NULL", "1"]
        ]
    );
}
#[test]
fn window_audit_calendar_intervals() {
    let (_dir, e) = fixture();
    assert_eq!(rows(&e,"SELECT COUNT(*) OVER (ORDER BY dt RANGE BETWEEN INTERVAL 1 MONTH PRECEDING AND CURRENT ROW) FROM w ORDER BY id"),vec![vec!["1"],vec!["2"],vec!["2"],vec!["2"]]);
}

#[test]
fn window_audit_wide_decimal_boundaries() {
    let (_dir, e) = fixture();
    e.execute("CREATE TABLE wide (id INT, d DECIMAL(60,20))")
        .unwrap();
    e.execute("INSERT INTO wide VALUES (1,9007199254740992.00000000000000000000),(2,9007199254740992.00000000000000000001),(3,9007199254740992.00000000000000000002)").unwrap();
    assert_eq!(rows(&e,"SELECT COUNT(*) OVER (ORDER BY d RANGE BETWEEN 0.00000000000000000001 PRECEDING AND CURRENT ROW) FROM wide ORDER BY id"),vec![vec!["1"],vec!["2"],vec!["2"]]);
    assert!(e
        .execute("SELECT NTILE(1.00000000000000000001) OVER () FROM w")
        .is_err());
}

#[test]
fn window_audit_prepared_frame_rebinding() {
    let (_dir, e) = fixture();
    let mut session = SessionContext::default();
    let mut execute = |sql: &str| e.execute_with_session(&mut session, sql);
    execute("PREPARE wf FROM 'SELECT SUM(v) OVER (ORDER BY id ROWS BETWEEN ? PRECEDING AND CURRENT ROW) FROM w ORDER BY id'").unwrap();
    for (n, expected) in [
        (0, vec!["10", "10", "20", "NULL"]),
        (1, vec!["10", "20", "30", "20"]),
        (0, vec!["10", "10", "20", "NULL"]),
    ] {
        execute(&format!("SET @n={n}")).unwrap();
        let QueryResponse::ResultSet(r) = execute("EXECUTE wf USING @n")
            .unwrap()
            .materialize()
            .unwrap()
        else {
            panic!("expected rows")
        };
        assert_eq!(
            r.rows
                .iter()
                .map(|r| r[0].to_display_string())
                .collect::<Vec<_>>(),
            expected
        );
    }
    execute("SET @n=1.5").unwrap();
    assert!(execute("EXECUTE wf USING @n").is_err());
}

#[test]
fn window_audit_projection_and_hidden_sort_dependencies() {
    let (_dir, e) = fixture();
    assert_eq!(
        rows(
            &e,
            "SELECT id,v+RANK() OVER (ORDER BY v) r FROM w ORDER BY id"
        ),
        vec![
            vec!["1", "12"],
            vec!["2", "12"],
            vec!["3", "24"],
            vec!["4", "NULL"]
        ]
    );
    let r = result(
        &e,
        "SELECT *,ROW_NUMBER() OVER (ORDER BY id) rn FROM w ORDER BY id",
    );
    assert_eq!(r.columns.len(), 6);
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[0][1], Value::Int(10));
    assert_eq!(
        rows(
            &e,
            "SELECT id,ROW_NUMBER() OVER (ORDER BY id) FROM w ORDER BY v DESC,id"
        ),
        vec![
            vec!["3", "3"],
            vec!["1", "1"],
            vec!["2", "2"],
            vec!["4", "4"]
        ]
    );
    assert_eq!(
        rows(
            &e,
            "SELECT id,v AS x,RANK() OVER (ORDER BY x+1) FROM w ORDER BY id"
        ),
        rows(
            &e,
            "SELECT id,v,RANK() OVER (ORDER BY v+1) FROM w ORDER BY id"
        )
    );
}

#[test]
fn window_audit_input_column_precedes_alias() {
    let (_dir, e) = fixture();
    assert_eq!(
        rows(
            &e,
            "SELECT id,v AS d,RANK() OVER (ORDER BY d) FROM w ORDER BY id"
        ),
        rows(
            &e,
            "SELECT id,v,RANK() OVER (ORDER BY w.d) FROM w ORDER BY id"
        )
    );
}

#[test]
fn window_audit_qualified_output_sort_keeps_identity() {
    let (_dir, e) = fixture();
    e.execute("CREATE TABLE b (id INT,v INT)").unwrap();
    e.execute("INSERT INTO b VALUES (1,400),(2,300),(3,200),(4,100)")
        .unwrap();
    assert_eq!(rows(&e,"SELECT a.id,a.v,b.v,ROW_NUMBER() OVER (ORDER BY b.v) FROM w a JOIN b ON a.id=b.id ORDER BY a.v DESC,a.id"),vec![vec!["3","20","200","2"],vec!["1","10","400","4"],vec!["2","10","300","3"],vec!["4","NULL","100","1"]]);
}

#[test]
fn window_projection_rebinding_membership_orders_materialized_aggregates() {
    let (_dir, engine) = fixture();
    assert_eq!(
        rows(&engine, "SELECT v AS grp, SUM(d) AS total, ROW_NUMBER() OVER (ORDER BY v) AS rn FROM w GROUP BY v ORDER BY SUM(d) IN (2.5), v"),
        vec![vec!["NULL", "3.00", "1"], vec!["20", "2.00", "3"], vec!["10", "2.50", "2"]],
    );
}
