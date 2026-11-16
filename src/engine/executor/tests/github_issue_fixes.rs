//! Regression tests for GitHub issue fixes (MySQL 8.0 compatibility bugs
//! surfaced by differential testing against a real MySQL server).
//!
//! Each test is named after the issue number it locks down. Expected values
//! were verified against the real MySQL gold standard cited in each issue.
use super::support::query_one_row as one_row;
use super::*;

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected result set for: {sql}");
    };
    result.rows
}

// ── #71: NOT on NULL must yield NULL (three-valued logic) ──────────────────
#[test]
fn issue_71_not_null_is_null() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let row = one_row(
        &engine,
        "SELECT NOT NULL, NOT (1 = NULL), NOT (1 > NULL), NOT 5, NOT 0",
    );
    assert_eq!(row[0], Value::Null);
    assert_eq!(row[1], Value::Null);
    assert_eq!(row[2], Value::Null);
    assert_eq!(row[3], Value::Int(0));
    assert_eq!(row[4], Value::Int(1));

    // WHERE NOT NULL must filter the row out (NULL is not TRUE).
    assert!(rows(&engine, "SELECT 1 WHERE NOT NULL").is_empty());

    // Column-level: NULL row's NOT a is NULL.
    engine.execute("CREATE TABLE t(a INT)").unwrap();
    engine.execute("INSERT INTO t VALUES (1),(NULL)").unwrap();
    let out = rows(&engine, "SELECT a, NOT a FROM t ORDER BY a");
    assert_eq!(out[0], vec![Value::Null, Value::Null]);
    assert_eq!(out[1], vec![Value::Int(1), Value::Int(0)]);
}

// ── #73: BETWEEN with a NULL operand follows three-valued AND ──────────────
#[test]
fn issue_73_between_null_three_valued() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let row = one_row(
        &engine,
        "SELECT \
            NULL BETWEEN 1 AND 2, \
            1 BETWEEN NULL AND 2, \
            5 BETWEEN 1 AND NULL, \
            NULL NOT BETWEEN 1 AND 2, \
            5 NOT BETWEEN 1 AND NULL, \
            5 BETWEEN 1 AND 3",
    );
    assert_eq!(row[0], Value::Null);
    assert_eq!(row[1], Value::Null);
    assert_eq!(row[2], Value::Null);
    assert_eq!(row[3], Value::Null);
    assert_eq!(row[4], Value::Null);
    assert_eq!(row[5], Value::Int(0));

    // FALSE-dominant cases: a NULL bound still yields 0 when the other bound
    // already excludes the value (10 <= 5 is FALSE ⇒ FALSE AND anything).
    let row2 = one_row(
        &engine,
        "SELECT 10 BETWEEN NULL AND 5, 0 BETWEEN 1 AND NULL",
    );
    assert_eq!(row2[0], Value::Int(0));
    assert_eq!(row2[1], Value::Int(0));

    // WHERE filter must exclude rows whose predicate is NULL.
    engine.execute("CREATE TABLE u(x INT)").unwrap();
    engine
        .execute("INSERT INTO u VALUES (1),(2),(5),(10)")
        .unwrap();
    let out = rows(
        &engine,
        "SELECT x FROM u WHERE x NOT BETWEEN 3 AND NULL ORDER BY x",
    );
    assert_eq!(out, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
}

// ── #72: `!` prefix NOT operator; `!!` runs = ceil(N/2) NOTs ────────────────
#[test]
fn issue_72_bang_not_operator() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(&engine, "SELECT !0, !1, !5, !NULL, !!3, !!!3, !!!!3");
    assert_eq!(row[0], Value::Int(1));
    assert_eq!(row[1], Value::Int(0));
    assert_eq!(row[2], Value::Int(0));
    assert_eq!(row[3], Value::Null);
    assert_eq!(row[4], Value::Int(0)); // !!3 == !3 == 0 (MySQL collapses pairs)
    assert_eq!(row[5], Value::Int(1)); // !!!3 == two NOTs == 1
    assert_eq!(row[6], Value::Int(1)); // !!!!3 == two NOTs == 1
    assert!(rows(&engine, "SELECT 1 WHERE !0")[0][0] == Value::Int(1));
    // `!=` operator must be unaffected.
    assert_eq!(rows(&engine, "SELECT 1 WHERE 3 != 4")[0][0], Value::Int(1));
}

// ── #63: TRIM(BOTH/LEADING/TRAILING FROM str) with no remstr ────────────────
#[test]
fn issue_63_trim_direction_without_remstr() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT TRIM(BOTH FROM '  x  '), TRIM(LEADING FROM '  x  '), \
                TRIM(TRAILING FROM '  x  '), TRIM(BOTH 'z' FROM 'zzxzz')",
    );
    assert_eq!(row[0], Value::String("x".into()));
    assert_eq!(row[1], Value::String("x  ".into()));
    assert_eq!(row[2], Value::String("  x".into()));
    assert_eq!(row[3], Value::String("x".into()));
}

// ── #67: INTERVAL on the left of `+` (commutative) ──────────────────────────
#[test]
fn issue_67_interval_left_of_plus() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT INTERVAL 1 DAY + DATE '2020-01-01', \
                INTERVAL 1 MONTH + DATE '2020-01-31', \
                INTERVAL 1 HOUR + TIMESTAMP '2020-01-01 10:00:00'",
    );
    assert_eq!(
        row[0],
        Value::Date(crate::storage::parse_date_to_days_bytes(b"2020-01-02").unwrap())
    );
    assert_eq!(
        row[1],
        Value::Date(crate::storage::parse_date_to_days_bytes(b"2020-02-29").unwrap())
    );
    assert_eq!(row[2], Value::DateTime(1_577_876_400_000_000));
}

// ── #81: comma-separated multi-table UPDATE ─────────────────────────────────
#[test]
fn issue_81_comma_multi_table_update() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE a(id INT, v INT)").unwrap();
    engine.execute("CREATE TABLE b(id INT, w INT)").unwrap();
    engine.execute("INSERT INTO a VALUES (1,10)").unwrap();
    engine.execute("INSERT INTO b VALUES (1,5)").unwrap();
    engine
        .execute("UPDATE a, b SET a.v = a.v + b.w WHERE a.id = b.id")
        .unwrap();
    assert_eq!(rows(&engine, "SELECT v FROM a")[0][0], Value::Int(15));
}

// ── #61: empty all-defaults VALUES () row ───────────────────────────────────
#[test]
fn issue_61_empty_default_values_row() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(a INT DEFAULT 7, b VARCHAR(4) DEFAULT 'zz')")
        .unwrap();
    engine.execute("INSERT INTO t () VALUES ()").unwrap();
    engine.execute("INSERT INTO t VALUES ()").unwrap();
    let out = rows(&engine, "SELECT a,b FROM t");
    assert_eq!(out.len(), 2);
    for r in out {
        assert_eq!(r, vec![Value::Int(7), Value::String("zz".into())]);
    }
    // DEFAULT expression column.
    engine
        .execute("CREATE TABLE t2(a INT DEFAULT (1+2))")
        .unwrap();
    engine.execute("INSERT INTO t2 VALUES ()").unwrap();
    assert_eq!(rows(&engine, "SELECT a FROM t2")[0][0], Value::Int(3));
}

// ── #62: REPLACE with an empty search string returns the subject unchanged ──
#[test]
fn issue_62_replace_empty_search() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT REPLACE('abc','','x'), REPLACE('hello','','-'), \
                REPLACE('aaa','a','bb'), REPLACE('aaa','a','')",
    );
    assert_eq!(row[0], Value::String("abc".into()));
    assert_eq!(row[1], Value::String("hello".into()));
    assert_eq!(row[2], Value::String("bbbbbb".into()));
    assert_eq!(row[3], Value::String(String::new()));
}

// ── #76: EXTRACT(WEEK) uses default_week_format (mode 0), matching WEEK() ────
#[test]
fn issue_76_extract_week_default_format() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT EXTRACT(WEEK FROM '2020-06-15'), WEEK('2020-06-15')",
    );
    assert_eq!(row[0], Value::Int(24));
    assert_eq!(row[1], Value::Int(24));
}

// ── #78: MOD result sign follows the dividend even with unsigned divisor ─────
#[test]
fn issue_78_mod_signed_dividend_unsigned_divisor() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT -7 MOD CAST(3 AS UNSIGNED), MOD(-7, CAST(3 AS UNSIGNED)), \
                CAST(7 AS UNSIGNED) MOD -3, -7 MOD 3",
    );
    assert_eq!(row[0], Value::Int(-1));
    assert_eq!(row[1], Value::Int(-1));
    assert_eq!(row[2], Value::UInt(1));
    assert_eq!(row[3], Value::Int(-1));
}

// ── #51: FORMAT rounds half-away-from-zero (not banker's); no `-0` ──────────
#[test]
fn issue_51_format_half_away_rounding() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT FORMAT(0.5,0), FORMAT(1.5,0), FORMAT(2.5,0), FORMAT(-0.5,0), \
                FORMAT(1234.5,0), FORMAT(1,2), FORMAT(1234567.891,2)",
    );
    assert_eq!(row[0], Value::String("1".into()));
    assert_eq!(row[1], Value::String("2".into()));
    assert_eq!(row[2], Value::String("3".into()));
    assert_eq!(row[3], Value::String("-1".into()));
    assert_eq!(row[4], Value::String("1,235".into()));
    assert_eq!(row[5], Value::String("1.00".into()));
    assert_eq!(row[6], Value::String("1,234,567.89".into()));
}

// ── #66: COUNT(DISTINCT a, b) dedups on the whole tuple ─────────────────────
#[test]
fn issue_66_count_distinct_multi_arg() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE s(g CHAR(1), v INT)").unwrap();
    engine
        .execute("INSERT INTO s VALUES ('a',10),('a',20),('b',5),('a',10)")
        .unwrap();
    let row = one_row(
        &engine,
        "SELECT COUNT(DISTINCT g, v), COUNT(DISTINCT g), COUNT(DISTINCT g, v, g) FROM s",
    );
    assert_eq!(row[0], Value::Int(3)); // (a,10),(a,20),(b,5)
    assert_eq!(row[1], Value::Int(2)); // a,b
    assert_eq!(row[2], Value::Int(3)); // duplicate g arg is a no-op
                                       // A NULL in any tuple element excludes the row.
    engine.execute("INSERT INTO s VALUES ('c', NULL)").unwrap();
    assert_eq!(
        one_row(&engine, "SELECT COUNT(DISTINCT g, v) FROM s")[0],
        Value::Int(3)
    );
}

// ── #65: HAVING / ORDER BY can reference a SELECT column alias ──────────────
#[test]
fn issue_65_having_order_by_alias() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE s(g CHAR(1), v INT)").unwrap();
    engine
        .execute("INSERT INTO s VALUES ('a',10),('a',20),('b',5)")
        .unwrap();
    // (`g` is CHAR -> SmallString; `SUM(v)` -> DecimalCompact.)
    let str_val = |r: &[Value]| match &r[0] {
        Value::String(s) => s.clone(),
        Value::SmallString(b) => String::from_utf8(b.to_vec()).unwrap(),
        other => panic!("unexpected group value: {other:?}"),
    };
    let num_val = |r: &[Value]| crate::engine::expr::numeric(&r[1]).unwrap() as i64;

    let r = rows(
        &engine,
        "SELECT g, SUM(v) AS tot FROM s GROUP BY g HAVING tot > 15",
    );
    assert_eq!(r.len(), 1);
    assert_eq!((str_val(&r[0]), num_val(&r[0])), ("a".to_string(), 30));

    let r = rows(
        &engine,
        "SELECT g, SUM(v) AS tot FROM s GROUP BY g HAVING tot > 0 ORDER BY tot DESC",
    );
    assert_eq!(
        r.iter()
            .map(|row| (str_val(row), num_val(row)))
            .collect::<Vec<_>>(),
        vec![("a".to_string(), 30), ("b".to_string(), 5)]
    );

    // Alias over a computed aggregate expression.
    let r = rows(
        &engine,
        "SELECT g, SUM(v)*2 AS tot FROM s GROUP BY g HAVING tot > 30",
    );
    assert_eq!(r.len(), 1);
    assert_eq!((str_val(&r[0]), num_val(&r[0])), ("a".to_string(), 60));

    let r = rows(
        &engine,
        "SELECT MAX(v) AS maximum FROM s HAVING maximum > 15",
    );
    assert_eq!(r.len(), 1);
    assert_eq!(crate::engine::expr::numeric(&r[0][0]).unwrap() as i64, 20);
}

// Issue #77: SELECT DISTINCT deduplicates using the column's case-insensitive collation.
#[test]
fn issue_77_select_distinct_case_insensitive() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t(s VARCHAR(20))").unwrap();
    engine
        .execute("INSERT INTO t VALUES ('Apple'),('apple'),('APPLE'),('banana')")
        .unwrap();
    // Default utf8mb4_0900_ai_ci merges Apple/apple/APPLE into two rows.
    let r = rows(&engine, "SELECT DISTINCT s FROM t ORDER BY s");
    assert_eq!(
        r.len(),
        2,
        "DISTINCT must merge case-insensitive equivalents, got {r:?}"
    );
    // Consistent with COUNT(DISTINCT) and GROUP BY.
    let c = one_row(&engine, "SELECT COUNT(DISTINCT s) FROM t");
    assert_eq!(c[0], Value::Int(2));
    let g = rows(&engine, "SELECT s FROM t GROUP BY s");
    assert_eq!(g.len(), 2);

    // Multicolumn and expression projections through the row path behave consistently.
    let r2 = rows(
        &engine,
        "SELECT DISTINCT CONCAT(s, '!') AS x FROM t ORDER BY x",
    );
    assert_eq!(
        r2.len(),
        2,
        "Expression-projection DISTINCT must also merge case-insensitive equivalents, got {r2:?}"
    );
}

// Issue #79: GROUP_CONCAT(DISTINCT) deduplicates using the column's case-insensitive collation.
#[test]
fn issue_79_group_concat_distinct_case_insensitive() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t(s VARCHAR(20))").unwrap();
    engine
        .execute("INSERT INTO t VALUES ('Apple'),('apple'),('banana')")
        .unwrap();
    // Merge Apple/apple while preserving the first spelling, 'Apple'.
    let r = one_row(&engine, "SELECT GROUP_CONCAT(DISTINCT s ORDER BY s) FROM t");
    assert_eq!(crate::storage::value_to_string(&r[0]), "Apple,banana");
    // Consistent with COUNT(DISTINCT).
    let c = one_row(&engine, "SELECT COUNT(DISTINCT s) FROM t");
    assert_eq!(c[0], Value::Int(2));
}

#[test]
fn grouped_scalar_subquery_aggregate_uses_outer_group_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t1 (a INT, b INT)").unwrap();
    engine
        .execute("INSERT INTO t1 VALUES (1,1),(1,2),(2,3)")
        .unwrap();

    let mut correlated = rows(
        &engine,
        "SELECT (SELECT COUNT(DISTINCT t1.b)) FROM t1 GROUP BY t1.a",
    );
    correlated.sort_by_key(|row| row[0].to_display_string());
    assert_eq!(correlated, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
    assert_eq!(
        rows(
            &engine,
            "SELECT (SELECT COUNT(DISTINCT 12)) FROM t1 GROUP BY t1.a",
        ),
        vec![vec![Value::Int(1)], vec![Value::Int(1)]]
    );
}

// Issue #74: DATE plus a time-unit interval returns DATETIME without dropping the time.
#[test]
fn issue_74_date_add_time_unit_returns_datetime() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let s = |v: &Value| crate::storage::value_to_string(v);

    let r = one_row(&engine, "SELECT DATE_ADD('2020-01-01', INTERVAL 5 HOUR)");
    assert_eq!(s(&r[0]), "2020-01-01 05:00:00");
    let r = one_row(&engine, "SELECT DATE_ADD('2020-01-01', INTERVAL 90 MINUTE)");
    assert_eq!(s(&r[0]), "2020-01-01 01:30:00");
    let r = one_row(&engine, "SELECT DATE_SUB('2020-01-01', INTERVAL 1 SECOND)");
    assert_eq!(s(&r[0]), "2019-12-31 23:59:59");
    let r = one_row(&engine, "SELECT DATE_ADD('2020-01-01', INTERVAL 25 HOUR)");
    assert_eq!(s(&r[0]), "2020-01-02 01:00:00");
    // Composite interval units include time components.
    let r = one_row(
        &engine,
        "SELECT DATE_ADD('2020-01-01', INTERVAL '1 6' DAY_HOUR)",
    );
    assert_eq!(s(&r[0]), "2020-01-02 06:00:00");

    // Numeric equality verifies actual value correctness, not just display formatting.
    let r = one_row(
        &engine,
        "SELECT DATE_ADD('2020-01-01', INTERVAL 5 HOUR) = '2020-01-01'",
    );
    assert_eq!(r[0], Value::Int(0));

    // Control case: date-only interval units still return DATE.
    let r = one_row(&engine, "SELECT DATE_ADD('2020-01-01', INTERVAL 1 DAY)");
    assert_eq!(s(&r[0]), "2020-01-02");
    // DATETIME input plus a time unit remains correct.
    let r = one_row(
        &engine,
        "SELECT DATE_ADD('2020-01-01 00:00:00', INTERVAL 5 HOUR)",
    );
    assert_eq!(s(&r[0]), "2020-01-01 05:00:00");
}

// Issue #75: CAST AS TIME, TIME(), and HOUR() accept short and extended TIME formats.
#[test]
fn issue_75_cast_time_short_and_extended() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let s = |v: &Value| crate::storage::value_to_string(v);

    assert_eq!(
        s(&one_row(&engine, "SELECT CAST('10:30' AS TIME)")[0]),
        "10:30:00"
    );
    assert_eq!(
        s(&one_row(&engine, "SELECT CAST('26:30:45' AS TIME)")[0]),
        "26:30:45"
    );
    assert_eq!(
        s(&one_row(&engine, "SELECT CAST('100:00:00' AS TIME)")[0]),
        "100:00:00"
    );
    assert_eq!(
        s(&one_row(&engine, "SELECT TIME('26:30:45')")[0]),
        "26:30:45"
    );
    assert_eq!(
        one_row(&engine, "SELECT HOUR('26:30:45')")[0],
        Value::Int(26)
    );
    assert_eq!(
        one_row(&engine, "SELECT MINUTE('26:30:45')")[0],
        Value::Int(30)
    );
    assert_eq!(
        one_row(&engine, "SELECT SECOND('26:30:45')")[0],
        Value::Int(45)
    );
    assert_eq!(one_row(&engine, "SELECT HOUR('10:30')")[0], Value::Int(10));

    // Keep behavior consistent with TIME_TO_SEC and SEC_TO_TIME.
    assert_eq!(
        one_row(&engine, "SELECT TIME_TO_SEC('26:00:00')")[0],
        Value::Int(93600)
    );
    assert_eq!(
        s(&one_row(&engine, "SELECT SEC_TO_TIME(93600)")[0]),
        "26:00:00"
    );

    // Ordinary datetime/TIME inputs still work.
    assert_eq!(
        one_row(&engine, "SELECT HOUR('2020-01-01 05:00:00')")[0],
        Value::Int(5)
    );
    assert_eq!(
        s(&one_row(&engine, "SELECT CAST('10:30:45' AS TIME)")[0]),
        "10:30:45"
    );
}

// Issue #68: UNION promotes combined INT and DECIMAL branches to DECIMAL.
#[test]
fn issue_68_union_int_decimal_promotes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let s = |v: &Value| crate::storage::value_to_string(v);

    let r = rows(&engine, "SELECT 1 UNION SELECT 2.5 ORDER BY 1");
    let vals: Vec<String> = r.iter().map(|row| s(&row[0])).collect();
    assert_eq!(vals, vec!["1.0", "2.5"]);

    let r = rows(&engine, "SELECT 2.5 UNION SELECT 1 ORDER BY 1");
    let vals: Vec<String> = r.iter().map(|row| s(&row[0])).collect();
    assert_eq!(vals, vec!["1.0", "2.5"]);

    // Table-column case: INT union DECIMAL(4,2) yields scale 2.
    engine.execute("CREATE TABLE a(x INT)").unwrap();
    engine.execute("INSERT INTO a VALUES (1)").unwrap();
    engine.execute("CREATE TABLE b(x DECIMAL(4,2))").unwrap();
    engine.execute("INSERT INTO b VALUES (2.5)").unwrap();
    let r = rows(&engine, "SELECT x FROM a UNION SELECT x FROM b ORDER BY x");
    let vals: Vec<String> = r.iter().map(|row| s(&row[0])).collect();
    assert_eq!(vals, vec!["1.00", "2.50"]);

    // Control: INT union FLOAT is unaffected; floating 1 still displays as 1.
    let r = rows(&engine, "SELECT 1 UNION SELECT 2.5e0 ORDER BY 1");
    let vals: Vec<String> = r.iter().map(|row| s(&row[0])).collect();
    assert_eq!(vals, vec!["1", "2.5"]);
}

// Issue #83: DROP COLUMN cascades index changes rather than always returning 1553.
#[test]
fn issue_83_drop_column_cascades_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // Single-column index: drop both the column and its index.
    engine
        .execute("CREATE TABLE t1(a INT, b INT, INDEX ib(b))")
        .unwrap();
    engine.execute("INSERT INTO t1 VALUES (1,2)").unwrap();
    engine.execute("ALTER TABLE t1 DROP COLUMN b").unwrap();
    let sc = rows(&engine, "SHOW INDEX FROM t1");
    assert!(
        sc.is_empty(),
        "A single-column index must be dropped with its column, got {sc:?}"
    );
    // The table remains queryable.
    assert_eq!(rows(&engine, "SELECT a FROM t1")[0][0], Value::Int(1));

    // Composite index: remove only the dropped column, preserving rows and other index columns.
    engine
        .execute("CREATE TABLE t2(a INT, b INT, c INT, INDEX ibc(b,c))")
        .unwrap();
    engine.execute("INSERT INTO t2 VALUES (1,2,3)").unwrap();
    engine.execute("ALTER TABLE t2 DROP COLUMN b").unwrap();
    let r = rows(&engine, "SELECT a, c FROM t2");
    assert_eq!(
        (r[0][0].clone(), r[0][1].clone()),
        (Value::Int(1), Value::Int(3))
    );
    // The composite index remains as (c) and can still serve queries.
    let r2 = rows(&engine, "SELECT a FROM t2 WHERE c = 3");
    assert_eq!(r2[0][0], Value::Int(1));

    // Drop a single-column UNIQUE index as well.
    engine
        .execute("CREATE TABLE t3(a INT, b INT UNIQUE)")
        .unwrap();
    engine.execute("INSERT INTO t3 VALUES (1,2)").unwrap();
    engine.execute("ALTER TABLE t3 DROP COLUMN b").unwrap();
    // After dropping the column, its UNIQUE constraint is gone and duplicate a values are allowed.
    engine.execute("INSERT INTO t3 VALUES (1)").unwrap();
    assert_eq!(
        rows(&engine, "SELECT COUNT(*) FROM t3")[0][0],
        Value::Int(2)
    );
}

// Issue #53: SQL booleans stored in JSON must become true/false, not integer 1/0.
#[test]
fn issue_53_json_boolean_encoding() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let s = |v: &Value| crate::storage::value_to_string(v);

    assert_eq!(
        s(&one_row(&engine, "SELECT JSON_ARRAY(1, TRUE, 0, FALSE, 2=2)")[0]),
        "[1, true, 0, false, true]"
    );
    assert_eq!(
        s(&one_row(&engine, "SELECT JSON_OBJECT('a', TRUE, 'b', FALSE)")[0]),
        r#"{"a": true, "b": false}"#
    );
    assert_eq!(s(&one_row(&engine, "SELECT CAST(TRUE AS JSON)")[0]), "true");
    assert_eq!(
        s(&one_row(&engine, "SELECT CAST(FALSE AS JSON)")[0]),
        "false"
    );
    assert_eq!(
        s(&one_row(&engine, "SELECT JSON_INSERT('{}', '$.x', TRUE)")[0]),
        r#"{"x": true}"#
    );
    assert_eq!(
        s(&one_row(&engine, "SELECT JSON_SET('{}', '$.x', FALSE)")[0]),
        r#"{"x": false}"#
    );
    assert_eq!(
        s(&one_row(&engine, "SELECT JSON_TYPE(CAST(TRUE AS JSON))")[0]),
        "BOOLEAN"
    );
    assert_eq!(
        one_row(&engine, "SELECT JSON_ARRAY(TRUE) = CAST('[true]' AS JSON)")[0],
        Value::Int(1)
    );

    // Conversely, integer 1 must not be mistaken for a boolean.
    assert_eq!(s(&one_row(&engine, "SELECT JSON_ARRAY(1)")[0]), "[1]");
    assert_eq!(
        s(&one_row(&engine, "SELECT JSON_TYPE(CAST(1 AS JSON))")[0]),
        "INTEGER"
    );
}

// ── Boolean type audit (2026-07): result-type metadata, HAVING BETWEEN 3VL,
//    CAST AS BOOLEAN, BOOL signedness/UNSIGNED rendering. Values verified
//    against MySQL 8.0. See docs/boolean-type-audit-2026-07.md. ────────────
fn col_types(engine: &StorageQueryEngine, sql: &str) -> Vec<crate::types::ColumnType> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected result set for: {sql}");
    };
    result.columns.iter().map(|c| c.column_type).collect()
}

#[test]
fn boolean_audit_result_type_metadata_is_longlong() {
    use crate::types::ColumnType;
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // Boolean literals and comparison/logical results advertise BIGINT, not
    // VAR_STRING (JDBC/ORM read them as integers).
    assert_eq!(
        col_types(&engine, "SELECT TRUE, FALSE, 1=1, NOT 'abc'"),
        vec![ColumnType::LongLong; 4]
    );

    // NOT over a VARCHAR column must be BIGINT (was leaking the inner type).
    engine
        .execute("CREATE TABLE t(c VARCHAR(10), a BIGINT, b BIGINT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES ('x', 3, NULL)")
        .unwrap();
    assert_eq!(
        col_types(&engine, "SELECT NOT c FROM t"),
        vec![ColumnType::LongLong]
    );

    // Conditional functions follow their branch types; NULL literals are
    // skipped so IFNULL(NULL, 1) types as BIGINT. Column-ref branches resolve
    // against the table (COALESCE/NULLIF over BIGINT columns → BIGINT).
    assert_eq!(
        col_types(&engine, "SELECT IFNULL(NULL, 1)"),
        vec![ColumnType::LongLong]
    );
    assert_eq!(
        col_types(&engine, "SELECT IF(1, 2, 3)"),
        vec![ColumnType::LongLong]
    );
    assert_eq!(
        col_types(&engine, "SELECT COALESCE(a, b) FROM t"),
        vec![ColumnType::LongLong]
    );
    assert_eq!(
        col_types(&engine, "SELECT NULLIF(a, 5) FROM t"),
        vec![ColumnType::LongLong]
    );
    // Mixed non-null branch types stay conservative (VAR_STRING), not a guess.
    assert_eq!(
        col_types(&engine, "SELECT IFNULL('s', 1)"),
        vec![ColumnType::VarString]
    );
}

#[test]
fn boolean_audit_having_between_null_is_false_dominant() {
    // Group key `g=10`. `10 BETWEEN NULL AND 5` is FALSE (10<=5 is FALSE ⇒
    // FALSE AND unknown = FALSE), so `NOT BETWEEN` is TRUE and the group is
    // kept. The old HAVING-BETWEEN path returned NULL for any NULL bound, which
    // (via NOT NULL = NULL) dropped the group. Exercises the aggregate/HAVING
    // `eval_between` SSOT.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t(g INT)").unwrap();
    engine.execute("INSERT INTO t VALUES (10), (10)").unwrap();

    let kept = rows(
        &engine,
        "SELECT g FROM t GROUP BY g HAVING g NOT BETWEEN NULL AND 5",
    );
    assert_eq!(kept, vec![vec![Value::Int(10)]]);
    // NULL-dominant bound (upper large) leaves the predicate unknown ⇒ NOT is
    // also unknown ⇒ group dropped.
    let dropped = rows(
        &engine,
        "SELECT g FROM t GROUP BY g HAVING g NOT BETWEEN NULL AND 20",
    );
    assert!(dropped.is_empty());
}

#[test]
fn boolean_audit_cast_as_boolean_is_rejected() {
    // MySQL 8.0 has no CAST(x AS BOOLEAN); mydb must raise 1064, not silently
    // degrade to CHAR (project rule: no silent no-op).
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let err = engine.execute("SELECT CAST(1 AS BOOLEAN)").unwrap_err();
    assert_eq!(err.code(), 1064, "unexpected error: {err:?}");
}

#[test]
fn boolean_audit_bool_is_signed_tinyint() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // BOOL is a signed TINYINT(1): -1 is a valid value and round-trips as a
    // signed Value::Int (not Value::UInt); 5 is preserved verbatim (no 0/1
    // truthy clamp).
    engine.execute("CREATE TABLE b(v BOOL)").unwrap();
    engine
        .execute("INSERT INTO b VALUES (-1), (0), (5)")
        .unwrap();
    let out = rows(&engine, "SELECT v FROM b ORDER BY v");
    assert_eq!(
        out,
        vec![
            vec![Value::Int(-1)],
            vec![Value::Int(0)],
            vec![Value::Int(5)]
        ]
    );

    // SHOW CREATE renders `tinyint(1)`. (`BOOL UNSIGNED` is not parseable by the
    // current DDL grammar — a separate limitation noted in the audit doc — so
    // the unsigned-suffix rendering path is covered by the is_integer() unit
    // guarantee rather than end-to-end here.)
    let show = crate::storage::value_to_string(&one_row(&engine, "SHOW CREATE TABLE b")[1]);
    assert!(
        show.contains("tinyint(1)"),
        "plain BOOL should render tinyint(1): {show}"
    );
}
