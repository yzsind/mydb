//! Regressions from the 2026-10-24 release audit: runtime filters bound to a
//! sibling relation by bare column name, integer index lookups addressed by
//! text operands, and DATE/DATETIME equality joins against numeric columns.

use super::support::query_rows;
use super::*;

fn exec_all(engine: &StorageQueryEngine, statements: &[&str]) {
    for sql in statements {
        engine
            .execute(sql)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
}

fn count(engine: &StorageQueryEngine, sql: &str) -> i64 {
    match &query_rows(engine, sql)[0][0] {
        Value::Int(v) => *v,
        other => panic!("{sql}: unexpected {other:?}"),
    }
}

/// A parent hash join pushes its build keys (`v2.id`) into the probe subtree.
/// The filter must land on `v2`, never on `v1.id` just because the name is
/// shared; fresh tables without statistics pick exactly that plan shape.
#[test]
fn runtime_filter_binds_only_to_its_qualified_relation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_all(
        &engine,
        &[
            "CREATE TABLE v1 (id INT, k INT)",
            "CREATE TABLE v2 (id INT, k INT)",
            "CREATE TABLE v3 (a INT)",
            "INSERT INTO v1 VALUES (5,1),(6,1),(1,9)",
            "INSERT INTO v2 VALUES (1,1)",
            "INSERT INTO v3 VALUES (1),(1)",
            "CREATE TABLE s (id INT, k INT)",
            "CREATE TABLE sk (a INT)",
            "INSERT INTO s VALUES (1,1),(5,1),(6,1)",
            "INSERT INTO sk VALUES (5)",
        ],
    );
    for sql in [
        "SELECT COUNT(*) FROM v2 JOIN v1 ON v2.k = v1.k JOIN v3 ON v3.a = v2.id",
        "SELECT COUNT(*) FROM v1 JOIN v2 ON v2.k = v1.k JOIN v3 ON v3.a = v2.id",
        "SELECT COUNT(*) FROM v2 a JOIN v1 b ON a.k = b.k JOIN v3 c ON c.a = a.id",
        "SELECT COUNT(*) FROM (SELECT v1.id FROM v2 JOIN v1 ON v2.k = v1.k JOIN v3 ON v3.a = v2.id) d",
    ] {
        assert_eq!(count(&engine, sql), 4, "{sql}");
    }
    assert_eq!(
        query_rows(
            &engine,
            "SELECT v1.id FROM v2 JOIN v1 ON v2.k = v1.k JOIN v3 ON v3.a = v2.id ORDER BY 1",
        ),
        vec![
            vec![Value::Int(5)],
            vec![Value::Int(5)],
            vec![Value::Int(6)],
            vec![Value::Int(6)],
        ]
    );
    assert_eq!(
        count(
            &engine,
            "SELECT COUNT(*) FROM s a JOIN s b ON a.k = b.k JOIN sk ON sk.a = a.id",
        ),
        3
    );
}

/// `int_col = text_col` compares in the DOUBLE domain on every access path.
#[test]
fn integer_index_lookup_by_text_uses_double_comparison() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_all(
        &engine,
        &[
            "CREATE TABLE ci (id INT PRIMARY KEY, a INT, u INT UNSIGNED, KEY ka(a), KEY ku(u))",
            "CREATE TABLE cs (id INT PRIMARY KEY, s VARCHAR(10), KEY ks(s))",
            "INSERT INTO ci VALUES (1,10,10),(2,100,100),(3,-5,5)",
            "INSERT INTO cs VALUES (1,'10'),(2,'1e2'),(3,'10.0'),(4,' 10'),(5,'1.5'),\
             (6,'-5'),(7,'-5.0'),(8,'1e30'),(9,'100.5'),(10,'abc')",
            "ANALYZE TABLE ci, cs",
        ],
    );
    let expected = query_rows(
        &engine,
        "SELECT cs.id, ci.id FROM cs LEFT JOIN ci IGNORE INDEX (ka) ON ci.a = cs.s ORDER BY 1, 2",
    );
    assert_eq!(expected.len(), 10);
    for sql in [
        "SELECT cs.id, ci.id FROM cs LEFT JOIN ci ON ci.a = cs.s ORDER BY 1, 2",
        "SELECT cs.id, ci.id FROM cs LEFT JOIN ci FORCE INDEX (ka) ON ci.a = cs.s ORDER BY 1, 2",
    ] {
        assert_eq!(query_rows(&engine, sql), expected, "{sql}");
    }
    let unsigned = query_rows(
        &engine,
        "SELECT cs.id, ci.id FROM cs LEFT JOIN ci FORCE INDEX (ku) ON ci.u = cs.s ORDER BY 1, 2",
    );
    let unsigned_expected = query_rows(
        &engine,
        "SELECT cs.id, ci.id FROM cs LEFT JOIN ci IGNORE INDEX (ku) ON ci.u = cs.s ORDER BY 1, 2",
    );
    assert_eq!(unsigned, unsigned_expected);
    assert_eq!(
        count(
            &engine,
            "SELECT COUNT(*) FROM cs JOIN ci FORCE INDEX (ka) ON ci.a = cs.s WHERE cs.id = 2"
        ),
        1
    );
}

/// `date_col = int_col` reads the number as a compact date on hash, index and
/// nested-loop paths alike; unconvertible numbers simply do not match.
#[test]
fn temporal_numeric_equality_join_matches_compact_values() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_all(
        &engine,
        &[
            "CREATE TABLE td (id INT PRIMARY KEY, d DATE, dt DATETIME(6), KEY kd(d))",
            "CREATE TABLE tn (id INT PRIMARY KEY, i INT, dc DECIMAL(24,8))",
            "INSERT INTO td VALUES (1,'2020-01-01','2020-01-01 10:11:12.5'),\
             (2,'2000-01-01','2000-01-01 00:00:00'),(3,'1999-12-31',NULL)",
            "INSERT INTO tn VALUES (1,20200101,20200101101112.5),(2,101,20000101),\
             (3,5,1.5),(4,NULL,NULL)",
        ],
    );
    let scalar = query_rows(
        &engine,
        "SELECT tn.id, td.id FROM tn, td WHERE (td.d = tn.i) IS TRUE ORDER BY 1, 2",
    );
    assert_eq!(
        scalar,
        vec![
            vec![Value::Int(1), Value::Int(1)],
            vec![Value::Int(2), Value::Int(2)],
        ]
    );
    for sql in [
        "SELECT tn.id, td.id FROM tn JOIN td ON td.d = tn.i ORDER BY 1, 2",
        "SELECT tn.id, td.id FROM tn JOIN td IGNORE INDEX (kd) ON td.d = tn.i ORDER BY 1, 2",
        "SELECT tn.id, td.id FROM tn JOIN td FORCE INDEX (kd) ON td.d = tn.i ORDER BY 1, 2",
        "SELECT tn.id, td.id FROM tn STRAIGHT_JOIN td ON td.d = tn.i WHERE td.id > 0 ORDER BY 1, 2",
    ] {
        assert_eq!(query_rows(&engine, sql), scalar, "{sql}");
    }
    assert_eq!(
        query_rows(
            &engine,
            "SELECT tn.id, td.id FROM tn JOIN td ON td.dt = tn.dc ORDER BY 1, 2"
        ),
        vec![
            vec![Value::Int(1), Value::Int(1)],
            vec![Value::Int(2), Value::Int(2)],
        ]
    );
}
