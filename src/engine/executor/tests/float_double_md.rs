//! End-to-end tests for FLOAT(M,D) / DOUBLE(M,D) display-width metadata.
//! MySQL 8.0 has deprecated the form but still preserves (M, D) in
//! the catalog and reports it back through SHOW COLUMNS,
//! information_schema, and the wire column-definition packet
//! (decimals field).

use super::*;

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    match engine.execute(sql).unwrap() {
        QueryResponse::ResultSet(rs) => rs.rows,
        other => panic!("expected ResultSet, got {other:?}"),
    }
}

fn columns(engine: &StorageQueryEngine, sql: &str) -> Vec<crate::types::Column> {
    match engine.execute(sql).unwrap() {
        QueryResponse::ResultSet(rs) => rs.columns,
        other => panic!("expected ResultSet, got {other:?}"),
    }
}

#[test]
fn show_columns_emits_float_md() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (a FLOAT(7,2), b DOUBLE(10,3), c FLOAT, d DOUBLE)")
        .unwrap();
    let r = rows(&engine, "SHOW COLUMNS FROM t");
    let by_field: std::collections::HashMap<String, String> = r
        .into_iter()
        .map(|row| (row[0].to_display_string(), row[1].to_display_string()))
        .collect();
    assert_eq!(by_field.get("a").map(String::as_str), Some("float(7,2)"));
    assert_eq!(by_field.get("b").map(String::as_str), Some("double(10,3)"));
    // Bare FLOAT / DOUBLE — no (M,D) → no parens, matching MySQL.
    assert_eq!(by_field.get("c").map(String::as_str), Some("float"));
    assert_eq!(by_field.get("d").map(String::as_str), Some("double"));
}

#[test]
fn float_bit_precision_selects_storage_family_without_fixed_scale() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (f24 FLOAT(24), f25 FLOAT(25), f53 FLOAT(53))")
        .unwrap();
    let r = rows(&engine, "SHOW COLUMNS FROM t");
    let by_field: std::collections::HashMap<String, String> = r
        .into_iter()
        .map(|row| (row[0].to_display_string(), row[1].to_display_string()))
        .collect();
    assert_eq!(by_field.get("f24").map(String::as_str), Some("float"));
    assert_eq!(by_field.get("f25").map(String::as_str), Some("double"));
    assert_eq!(by_field.get("f53").map(String::as_str), Some("double"));

    engine
        .execute("INSERT INTO t VALUES (1234567890, 1234567890, 1e-150)")
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT * FROM t"),
        vec![vec![
            Value::Float(1_234_567_936.0),
            Value::Double(1_234_567_890.0),
            Value::Double(1e-150),
        ]]
    );
}

#[test]
fn information_schema_carries_float_md() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (a FLOAT(7,2), b DOUBLE(10,3), c FLOAT)")
        .unwrap();
    let r = rows(
        &engine,
        "SELECT column_name, numeric_precision, numeric_scale \
         FROM information_schema.columns WHERE table_name='t' \
         ORDER BY ordinal_position",
    );
    let triples: Vec<(String, String, String)> = r
        .into_iter()
        .map(|row| {
            (
                row[0].to_display_string(),
                row[1].to_display_string(),
                row[2].to_display_string(),
            )
        })
        .collect();
    assert_eq!(
        triples,
        vec![
            ("a".to_string(), "7".to_string(), "2".to_string()),
            ("b".to_string(), "10".to_string(), "3".to_string()),
            // FLOAT (no (M,D)) — precision falls back to 12, scale NULL.
            ("c".to_string(), "12".to_string(), "NULL".to_string()),
        ]
    );
}

#[test]
fn select_star_carries_float_decimals_in_source() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (a FLOAT(7,2), b DOUBLE(10,3), c FLOAT)")
        .unwrap();
    let cols = columns(&engine, "SELECT * FROM t");
    let by_name: std::collections::HashMap<&str, Option<u8>> = cols
        .iter()
        .map(|c| (c.name.as_str(), c.source.decimals))
        .collect();
    assert_eq!(by_name.get("a").copied().flatten(), Some(2));
    assert_eq!(by_name.get("b").copied().flatten(), Some(3));
    // Bare FLOAT — no decimals override, falls through to NOT_FIXED_DEC.
    assert_eq!(by_name.get("c").copied().flatten(), None);
}

#[test]
fn union_ctas_merges_float_md_domains_before_assignment() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE narrow_frac (d DOUBLE(10,9))")
        .unwrap();
    engine
        .execute("CREATE TABLE wide_int (d DOUBLE(10,1))")
        .unwrap();
    engine
        .execute("INSERT INTO narrow_frac VALUES (1.2345678)")
        .unwrap();
    engine
        .execute("INSERT INTO wide_int VALUES (100000000.0)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE merged AS \
             SELECT d FROM narrow_frac UNION SELECT d FROM wide_int",
        )
        .unwrap();

    let result = rows(&engine, "SELECT d FROM merged ORDER BY d");
    assert_eq!(result[0][0], Value::Double(1.2345678));
    assert_eq!(result[1][0], Value::Double(100000000.0));
    let shown = rows(&engine, "SHOW COLUMNS FROM merged");
    assert_eq!(shown[0][1].to_display_string(), "double(18,9)");

    engine
        .execute("CREATE TABLE small_double (d DOUBLE(4,1))")
        .unwrap();
    engine.execute("CREATE TABLE ints (d INT)").unwrap();
    engine
        .execute("INSERT INTO small_double VALUES (1.2)")
        .unwrap();
    engine.execute("INSERT INTO ints VALUES (100000)").unwrap();
    engine
        .execute(
            "CREATE TABLE mixed AS \
             SELECT d FROM small_double UNION SELECT d FROM ints",
        )
        .unwrap();
    let mixed = rows(&engine, "SELECT d FROM mixed ORDER BY d");
    assert_eq!(mixed[0][0], Value::Double(1.2));
    assert_eq!(mixed[1][0], Value::Double(100000.0));
    let shown = rows(&engine, "SHOW COLUMNS FROM mixed");
    assert_eq!(shown[0][1].to_display_string(), "double(11,1)");
}

#[test]
fn fixed_scale_double_sum_uses_declared_decimal_domain() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (g INT, amount DOUBLE(8,2))")
        .unwrap();
    engine
        .execute(
            "INSERT INTO t VALUES \
             (1,28.50),(1,121.85),(1,157.23),(1,1351.00),\
             (1,-1965.35),(1,217.08),(1,81.75),(1,7.94)",
        )
        .unwrap();

    let unfiltered = engine
        .execute("SELECT SUM(amount) AS total FROM t GROUP BY g")
        .unwrap();
    let QueryResponse::ResultSet(unfiltered) = unfiltered else {
        panic!("expected result set");
    };
    assert_eq!(
        unfiltered.rows,
        vec![vec![Value::Decimal("0.00".to_string())]]
    );
    assert_eq!(unfiltered.columns[0].source.decimals, Some(2));

    let result = engine
        .execute("SELECT SUM(amount) AS total FROM t GROUP BY g HAVING total <=> 0")
        .unwrap();
    let QueryResponse::ResultSet(result) = result else {
        panic!("expected result set");
    };
    assert_eq!(result.columns[0].source.decimals, Some(2));
    assert_eq!(result.rows, vec![vec![Value::Decimal("0.00".to_string())]]);

    engine.execute("ALTER TABLE t ADD KEY (g, amount)").unwrap();
    let indexed = rows(
        &engine,
        "SELECT SUM(amount) AS total FROM t GROUP BY g HAVING total <=> 0",
    );
    assert_eq!(indexed, vec![vec![Value::Decimal("0.00".to_string())]]);
}

#[test]
fn fixed_scale_approximate_avg_uses_declared_scale_plus_increment() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET div_precision_increment = 5")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE t1 (old_col BIGINT UNSIGNED)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "DROP TABLE t1")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t1 (id INT, f FLOAT(10,2), d DOUBLE(10,3), bare FLOAT)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO t1 VALUES \
             (1,0.00,0.000,0.0),(1,1.00,1.000,1.0),(1,2.00,2.000,2.0),\
             (2,10.00,10.000,10.0),(2,11.00,11.000,11.0),(2,12.00,12.000,12.0)",
        )
        .unwrap();

    let result = engine
        .execute_with_session(
            &mut session,
            "SELECT AVG(f), AVG(d), AVG(bare) FROM t1 GROUP BY id ORDER BY id",
        )
        .unwrap();
    let QueryResponse::ResultSet(result) = result else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![
                Value::Decimal("1.0000000".to_string()),
                Value::Decimal("1.00000000".to_string()),
                Value::Double(1.0),
            ],
            vec![
                Value::Decimal("11.0000000".to_string()),
                Value::Decimal("11.00000000".to_string()),
                Value::Double(11.0),
            ],
        ]
    );

    let result = engine
        .execute_with_session(
            &mut session,
            "SELECT id, AVG(f), STD(f), VARIANCE(f) FROM t1 GROUP BY id ORDER BY id",
        )
        .unwrap();
    let QueryResponse::ResultSet(result) = result else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][1], Value::Decimal("1.0000000".to_string()));
}
