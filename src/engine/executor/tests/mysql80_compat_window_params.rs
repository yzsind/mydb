//! MySQL 8.0 dynamic window-function argument compatibility.
//!
//! Cover phase B of docs/expr-mysql80-compat-plan.md:
//! NTILE(n), LAG/LEAD(expr,offset,default), and NTH_VALUE(expr,n)
//! accept arbitrary constant expressions instead of integer literals only, evaluated
//! once per query. Reject column-dependent arguments and negative offsets explicitly.
use super::*;

fn rows_of(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    match engine.execute(sql).unwrap() {
        QueryResponse::ResultSet(r) => r.rows,
        other => panic!("expected result set for `{sql}`, got {other:?}"),
    }
}

fn setup(engine: &StorageQueryEngine) {
    engine
        .execute("CREATE TABLE w (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO w VALUES (1,10),(2,20),(3,30),(4,40)")
        .unwrap();
}

#[test]
fn ntile_accepts_constant_expression() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    setup(&engine);
    // NTILE(1+1) == NTILE(2): 4 rows → buckets 1,1,2,2.
    let rows = rows_of(
        &engine,
        "SELECT id, NTILE(1+1) OVER (ORDER BY id) FROM w ORDER BY id",
    );
    let buckets: Vec<i64> = rows
        .iter()
        .map(|r| match r[1] {
            Value::Int(v) => v,
            ref other => panic!("bucket not int: {other:?}"),
        })
        .collect();
    assert_eq!(buckets, vec![1, 1, 2, 2]);
}

#[test]
fn lag_accepts_constant_offset_expression() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    setup(&engine);
    // LAG(v, 1+1): offset 2. Rows ordered by id: v=10,20,30,40.
    // lag2 → NULL, NULL, 10, 20.
    let rows = rows_of(
        &engine,
        "SELECT id, LAG(v, 1+1) OVER (ORDER BY id) FROM w ORDER BY id",
    );
    assert!(matches!(rows[0][1], Value::Null));
    assert!(matches!(rows[1][1], Value::Null));
    assert_eq!(rows[2][1], Value::Int(10));
    assert_eq!(rows[3][1], Value::Int(20));
}

#[test]
fn lag_accepts_constant_default_expression() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    setup(&engine);
    // LAG(v, 2, 100+1): out-of-range rows use default 101.
    let rows = rows_of(
        &engine,
        "SELECT id, LAG(v, 2, 100+1) OVER (ORDER BY id) FROM w ORDER BY id",
    );
    assert_eq!(rows[0][1], Value::Int(101));
    assert_eq!(rows[1][1], Value::Int(101));
    assert_eq!(rows[2][1], Value::Int(10));
}

#[test]
fn lead_accepts_constant_offset_expression() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    setup(&engine);
    // LEAD(v, 3-1): offset 2. lead2 → 30, 40, NULL, NULL.
    let rows = rows_of(
        &engine,
        "SELECT id, LEAD(v, 3-1) OVER (ORDER BY id) FROM w ORDER BY id",
    );
    assert_eq!(rows[0][1], Value::Int(30));
    assert_eq!(rows[1][1], Value::Int(40));
    assert!(matches!(rows[2][1], Value::Null));
    assert!(matches!(rows[3][1], Value::Null));
}

#[test]
fn nth_value_accepts_constant_expression() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    setup(&engine);
    // NTH_VALUE(v, 1+1) over the whole partition frame = 2nd value = 20.
    let rows = rows_of(
        &engine,
        "SELECT id, NTH_VALUE(v, 1+1) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM w ORDER BY id",
    );
    for r in &rows {
        assert_eq!(r[1], Value::Int(20));
    }
}

#[test]
fn window_param_rejects_column_reference() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    setup(&engine);
    // A per-row column reference is not a constant → clear error, not silent.
    let err = engine
        .execute("SELECT NTILE(v) OVER (ORDER BY id) FROM w")
        .unwrap_err();
    assert!(
        err.is(crate::error::catalog::ER_NOT_SUPPORTED_YET)
            || err.is(crate::error::catalog::ER_BAD_FIELD_ERROR),
        "got {err:?}"
    );
}

#[test]
fn lag_rejects_negative_offset() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    setup(&engine);
    let err = engine
        .execute("SELECT LAG(v, 0-1) OVER (ORDER BY id) FROM w")
        .unwrap_err();
    assert_eq!(err.code(), 1235, "got {err:?}");
}

#[test]
fn ntile_literal_still_works() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    setup(&engine);
    // Regression: the plain literal fast path must still work.
    let rows = rows_of(
        &engine,
        "SELECT id, NTILE(2) OVER (ORDER BY id) FROM w ORDER BY id",
    );
    let buckets: Vec<i64> = rows
        .iter()
        .map(|r| match r[1] {
            Value::Int(v) => v,
            ref other => panic!("bucket not int: {other:?}"),
        })
        .collect();
    assert_eq!(buckets, vec![1, 1, 2, 2]);
}
