//! P2 SELECT compatibility regressions; oracle is local MySQL 8.0/9.6.
//! C1: unaliased expression column names retain original SQL text such as 1+1, COUNT(*), and NOW().
//! C2: honor ONLY_FULL_GROUP_BY with PK/UNIQUE/equality functional-dependency closure.
//! C4: basic SELECT INTO OUTFILE/DUMPFILE export.
//! C6a: bare TABLE t with optional ORDER BY/LIMIT is SELECT * FROM t.
//! C6b: derived-table column aliases AS x(a,b).
//! C6c: uniform-unit RANGE INTERVAL temporal frames.
//! FOUND_ROWS/SQL_CALC (C3) and optimizer_switch (C5) are covered separately.
//! mysql80_compat_select_modifiers.rs / mysql80_compat_index.rs。

use super::*;

fn cols(engine: &StorageQueryEngine, sql: &str) -> Vec<String> {
    let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
        panic!("expected result set for {sql}");
    };
    rs.columns.into_iter().map(|c| c.name).collect()
}

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
        panic!("expected result set for {sql}");
    };
    rs.rows
}

fn err(engine: &StorageQueryEngine, sql: &str) -> QueryError {
    match engine.execute(sql) {
        Err(e) => e,
        Ok(_) => panic!("expected error for {sql}"),
    }
}

// ---------- C1: expression column names retain original SQL text ----------

#[test]
fn c1_unaliased_expression_column_names_match_source_text() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t (a INT, b INT)").unwrap();
    engine.execute("INSERT INTO t VALUES (1, 2)").unwrap();

    // Compound expressions preserve original spelling and whitespace, matching MySQL.
    assert_eq!(cols(&engine, "SELECT 1+1"), vec!["1+1"]);
    assert_eq!(cols(&engine, "SELECT 1 + 1"), vec!["1 + 1"]);
    assert_eq!(cols(&engine, "SELECT -5"), vec!["-5"]);
    assert_eq!(cols(&engine, "SELECT COUNT(*) FROM t"), vec!["COUNT(*)"]);
    assert_eq!(
        cols(&engine, "SELECT CONCAT('x','y')"),
        vec!["CONCAT('x','y')"]
    );
    assert_eq!(cols(&engine, "SELECT a+b, b*2 FROM t"), vec!["a+b", "b*2"]);
    assert_eq!(
        cols(&engine, "SELECT CASE WHEN a>0 THEN 1 END FROM t"),
        vec!["CASE WHEN a>0 THEN 1 END"]
    );

    // Simple column, literal, and alias naming remains unchanged.
    assert_eq!(cols(&engine, "SELECT a FROM t"), vec!["a"]);
    assert_eq!(cols(&engine, "SELECT 'hi'"), vec!["hi"]);
    assert_eq!(cols(&engine, "SELECT 23"), vec!["23"]);
    assert_eq!(cols(&engine, "SELECT a+b AS s FROM t"), vec!["s"]);
}

// ---------- C2: ONLY_FULL_GROUP_BY ----------

fn ofgb_session(engine: &StorageQueryEngine) -> SessionContext {
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "SET SESSION sql_mode='ONLY_FULL_GROUP_BY'")
        .unwrap();
    s
}

fn run_s(
    engine: &StorageQueryEngine,
    s: &mut SessionContext,
    sql: &str,
) -> Result<QueryResponse, QueryError> {
    engine.execute_with_session(s, sql)
}

#[test]
fn c2_only_full_group_by_rejects_nondependent_and_accepts_fd() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT NOT NULL UNIQUE)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,10,100,1000),(2,10,200,2000)")
        .unwrap();
    let mut s = ofgb_session(&engine);

    // Violation: nongrouped, nonaggregate b without functional dependency returns 1055.
    let e = run_s(&engine, &mut s, "SELECT a, b FROM t GROUP BY a").unwrap_err();
    assert_eq!(
        e.code(),
        1055,
        "SELECT a,b GROUP BY a should be 1055: {e:?}"
    );

    // Violation: nonaggregate a in an implicit single group returns 1140.
    let e = run_s(&engine, &mut s, "SELECT a, COUNT(*) FROM t").unwrap_err();
    assert_eq!(e.code(), 1140, "implicit group violation: {e:?}");

    // Valid: grouping columns plus aggregates only.
    run_s(&engine, &mut s, "SELECT a, COUNT(*) FROM t GROUP BY a").unwrap();
    // Valid: grouping by the primary key makes all table columns functionally dependent.
    run_s(&engine, &mut s, "SELECT a, b FROM t GROUP BY id").unwrap();
    // Valid: grouping by UNIQUE NOT NULL creates a functional dependency.
    run_s(&engine, &mut s, "SELECT a FROM t GROUP BY c").unwrap();
    // Valid: WHERE equality propagation makes b determined by a.
    run_s(&engine, &mut s, "SELECT b FROM t WHERE a=b GROUP BY a").unwrap();
    // No aggregates or GROUP BY means no check.
    run_s(&engine, &mut s, "SELECT a, b FROM t").unwrap();
}

#[test]
fn c2_empty_sql_mode_stays_permissive() {
    // Default empty sql_mode keeps permissive hidden-first-value behavior without 1055.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t (a INT, b INT)").unwrap();
    engine.execute("INSERT INTO t VALUES (10, 100)").unwrap();
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "SET SESSION sql_mode=''")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut s, "SELECT a, b FROM t GROUP BY a")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
}

// ---------- C4: INTO OUTFILE/DUMPFILE ----------

#[test]
fn c4_select_into_outfile_and_dumpfile_write_server_files() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (a INT, b VARBINARY(32))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'a\\tb'), (2, NULL)")
        .unwrap();

    let outfile = dir.path().join("rows.tsv");
    let response = engine
        .execute(&format!(
            "SELECT a,b FROM t ORDER BY a INTO OUTFILE '{}'",
            crate::util::file::sql_path(&outfile)
        ))
        .unwrap();
    assert!(matches!(
        response,
        QueryResponse::OkAffected { affected_rows: 2 }
    ));
    assert_eq!(std::fs::read(&outfile).unwrap(), b"1\ta\\\tb\n2\t\\N\n");

    let dumpfile = dir.path().join("row.bin");
    let response = engine
        .execute(&format!(
            "SELECT b FROM t WHERE a=1 INTO DUMPFILE '{}'",
            crate::util::file::sql_path(&dumpfile)
        ))
        .unwrap();
    assert!(matches!(
        response,
        QueryResponse::OkAffected { affected_rows: 1 }
    ));
    assert_eq!(std::fs::read(&dumpfile).unwrap(), b"a\tb");

    let existing = err(
        &engine,
        &format!(
            "SELECT 1 INTO OUTFILE '{}'",
            crate::util::file::sql_path(&outfile)
        ),
    );
    assert_eq!(existing.code(), 1086);

    // Do not break valid queries using outfile as a column alias.
    assert_eq!(cols(&engine, "SELECT a AS outfile FROM t"), vec!["outfile"]);
}

// ---------- C6a: bare TABLE statement ----------

#[test]
fn c6a_bare_table_statement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t (id INT, v INT)").unwrap();
    engine
        .execute("INSERT INTO t VALUES (3,30),(1,10),(2,20)")
        .unwrap();

    assert_eq!(
        rows(&engine, "TABLE t ORDER BY id"),
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)],
            vec![Value::Int(3), Value::Int(30)],
        ]
    );
    assert_eq!(
        rows(&engine, "TABLE t ORDER BY id DESC LIMIT 1"),
        vec![vec![Value::Int(3), Value::Int(30)]]
    );
}

// ---------- C6b: derived-table column aliases ----------

#[test]
fn c6b_derived_table_column_aliases() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t (x INT, y INT)").unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,10),(2,20)")
        .unwrap();

    // Ordinary derived aliases rename output columns.
    assert_eq!(
        rows(
            &engine,
            "SELECT a, b FROM (SELECT x, y FROM t) AS d(a, b) ORDER BY a"
        ),
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)],
        ]
    );
    // Expression columns are renamed too and can be filtered by their new names.
    assert_eq!(
        rows(
            &engine,
            "SELECT a FROM (SELECT x+100, y FROM t) AS d(a, b) WHERE b=20"
        ),
        vec![vec![Value::Int(102)]]
    );
    // A column-count mismatch errors explicitly.
    let e = err(&engine, "SELECT * FROM (SELECT x FROM t) AS d(a, b)");
    assert!(
        e.message().contains("column aliases") || e.is(crate::error::catalog::ER_NOT_SUPPORTED_YET),
        "width mismatch should error: {e:?}"
    );
}

// ---------- C6c: RANGE INTERVAL temporal frames ----------

#[test]
fn c6c_range_interval_temporal_frame() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE sales (d DATE, amt INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO sales VALUES ('2024-01-01',10),('2024-01-02',20),\
             ('2024-01-03',40),('2024-01-05',80),('2024-01-08',160)",
        )
        .unwrap();

    // RANGE INTERVAL 2 DAY PRECEDING covers [d-2 days,d].
    let got = rows(
        &engine,
        "SELECT amt, SUM(amt) OVER (ORDER BY d RANGE INTERVAL 2 DAY PRECEDING) \
         FROM sales ORDER BY d",
    );
    let win: Vec<i64> = got
        .iter()
        .map(|r| match &r[1] {
            Value::Int(v) => *v,
            Value::Decimal(s) => s.parse().unwrap(),
            Value::DecimalCompact(m, 0) => *m as i64,
            other => panic!("unexpected sum {other:?}"),
        })
        .collect();
    // 01-01:10 | 01-02:10+20=30 | 01-03:10+20+40=70 |
    // 01-05 sums 40+80=120 because 01-01/02 are too old; 01-08 yields 160 because 01-05 is too old.
    assert_eq!(win, vec![10, 30, 70, 120, 160]);

    // Calendar MONTH intervals use date arithmetic, not a fixed duration.
    // All preceding January rows are within one month of these dates.
    let month = rows(
        &engine,
        "SELECT SUM(amt) OVER (ORDER BY d RANGE INTERVAL 1 MONTH PRECEDING) FROM sales ORDER BY d",
    );
    let sums: Vec<_> = month.iter().map(|row| row[0].to_display_string()).collect();
    assert_eq!(sums, ["10", "30", "70", "150", "310"]);
}
