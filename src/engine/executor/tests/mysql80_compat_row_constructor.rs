//! MySQL 8.0 row-constructor comparison compatibility.
//!
//! Cover Phase A of docs/expr-mysql80-compat-plan.md:
//! Six comparisons between (a,b) and (c,d), plus null-safe equality.
//! Three-valued NULL logic: (1,NULL)=(1,2) is NULL; (1,NULL)=(2,2) is FALSE.
//! Lexicographic <, <=, >, and >=.
//! Row IN lists with NULL propagation.
//! Arity mismatch returns 1241.
//!
//! Use column-wise expansion plus three-valued logic as the oracle, matching MySQL 8.0.
use super::*;

fn scalar(engine: &StorageQueryEngine, sql: &str) -> Value {
    let resp = engine.execute(sql).unwrap();
    match resp {
        QueryResponse::ResultSet(result) => {
            assert_eq!(result.rows.len(), 1, "expected 1 row for `{sql}`");
            assert_eq!(result.rows[0].len(), 1, "expected 1 column for `{sql}`");
            result.rows[0][0].clone()
        }
        other => panic!("expected result set for `{sql}`, got {other:?}"),
    }
}

fn is_true(v: &Value) -> bool {
    matches!(v, Value::Int(1))
}
fn is_false(v: &Value) -> bool {
    matches!(v, Value::Int(0))
}
fn is_null(v: &Value) -> bool {
    matches!(v, Value::Null)
}

#[test]
fn row_projection_preserves_nulls_for_narrow_and_wide_arities() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE row_buffers(id INT PRIMARY KEY,n BIGINT)")
        .unwrap();
    engine
        .execute("INSERT INTO row_buffers VALUES(1,7),(2,NULL)")
        .unwrap();
    for arity in [2, 4, 5, 8] {
        let items = (0..arity).map(|_| "n").collect::<Vec<_>>().join(",");
        for (expr, expected) in [
            (
                format!("({items})=({items})"),
                vec![Value::Int(1), Value::Null],
            ),
            (
                format!("({items})<=>({items})"),
                vec![Value::Int(1), Value::Int(1)],
            ),
            (
                format!("({items}) IN (({items}))"),
                vec![Value::Int(1), Value::Null],
            ),
            (
                format!("({items}) NOT IN (({items}))"),
                vec![Value::Int(0), Value::Null],
            ),
        ] {
            let response = engine
                .execute(&format!("SELECT {expr} FROM row_buffers ORDER BY id"))
                .unwrap();
            let QueryResponse::ResultSet(result) = response else {
                panic!("rows")
            };
            assert_eq!(
                result
                    .rows
                    .into_iter()
                    .map(|row| row[0].clone())
                    .collect::<Vec<_>>(),
                expected,
                "{expr}"
            );
        }
    }
}

#[test]
fn row_projection_preserves_eager_arguments_and_in_list_short_circuit() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::system_default();
    let mut scalar = |sql: &str| {
        let response = engine
            .execute_with_session(&mut session, sql)
            .and_then(QueryResponse::materialize)
            .unwrap();
        match response {
            QueryResponse::ResultSet(result) => Some(result.rows[0][0].clone()),
            _ => None,
        }
    };
    scalar("SET @row_buffer=0");
    assert_eq!(
        scalar("SELECT ROW((@row_buffer:=@row_buffer+1),(@row_buffer:=@row_buffer+1))=ROW(9,9)"),
        Some(Value::Int(0))
    );
    assert_eq!(scalar("SELECT @row_buffer"), Some(Value::Int(2)));
    scalar("SET @row_buffer=0");
    assert_eq!(scalar("SELECT ROW((@row_buffer:=@row_buffer+1),0) IN (ROW(1,0),ROW((@row_buffer:=@row_buffer+1),0))"), Some(Value::Int(1)));
    assert_eq!(scalar("SELECT @row_buffer"), Some(Value::Int(1)));
}

#[test]
fn row_eq_basic() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    assert!(is_true(&scalar(&engine, "SELECT (1,2) = (1,2)")));
    assert!(is_false(&scalar(&engine, "SELECT (1,2) = (1,3)")));
    assert!(is_false(&scalar(&engine, "SELECT (1,2) = (9,2)")));
    // Three columns.
    assert!(is_true(&scalar(&engine, "SELECT (1,2,3) = (1,2,3)")));
    assert!(is_false(&scalar(&engine, "SELECT (1,2,3) = (1,2,4)")));
    assert!(is_true(&scalar(
        &engine,
        "SELECT ROW('a','b','c') = ROW('a','b','c')"
    )));
}

#[test]
fn row_eq_null_semantics() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    // NULL in a still-undecided position → NULL.
    assert!(is_null(&scalar(&engine, "SELECT (1,NULL) = (1,2)")));
    // FALSE dominates: first column already unequal → FALSE, not NULL.
    assert!(is_false(&scalar(&engine, "SELECT (1,NULL) = (2,2)")));
    assert!(is_null(&scalar(&engine, "SELECT (NULL,2) = (1,2)")));
}

#[test]
fn row_neq_semantics() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    assert!(is_true(&scalar(&engine, "SELECT (1,2) <> (1,3)")));
    assert!(is_false(&scalar(&engine, "SELECT (1,2) <> (1,2)")));
    // TRUE dominates: first column already unequal → TRUE.
    assert!(is_true(&scalar(&engine, "SELECT (1,NULL) <> (2,2)")));
    // Undecided with NULL → NULL.
    assert!(is_null(&scalar(&engine, "SELECT (1,NULL) <> (1,2)")));
}

#[test]
fn row_null_safe_eq() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    assert!(is_true(&scalar(&engine, "SELECT (1,NULL) <=> (1,NULL)")));
    assert!(is_false(&scalar(&engine, "SELECT (1,NULL) <=> (1,2)")));
    assert!(is_true(&scalar(&engine, "SELECT (1,2) <=> (1,2)")));
    // Never NULL.
    assert!(is_false(&scalar(&engine, "SELECT (NULL,2) <=> (1,2)")));
}

#[test]
fn row_lexicographic() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    // First column decides.
    assert!(is_true(&scalar(&engine, "SELECT (1,9) < (2,0)")));
    assert!(is_false(&scalar(&engine, "SELECT (2,0) < (1,9)")));
    // First equal, second decides.
    assert!(is_true(&scalar(&engine, "SELECT (1,2) < (1,3)")));
    assert!(is_false(&scalar(&engine, "SELECT (1,3) < (1,2)")));
    // Equality boundary.
    assert!(is_false(&scalar(&engine, "SELECT (1,2) < (1,2)")));
    assert!(is_true(&scalar(&engine, "SELECT (1,2) <= (1,2)")));
    assert!(is_true(&scalar(&engine, "SELECT (1,2) >= (1,2)")));
    assert!(is_false(&scalar(&engine, "SELECT (1,2) > (1,2)")));
    assert!(is_true(&scalar(&engine, "SELECT (2,1) > (1,9)")));
}

#[test]
fn row_lexicographic_null() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    // NULL at deciding position → NULL.
    assert!(is_null(&scalar(&engine, "SELECT (1,NULL) < (1,2)")));
    // Decided before the NULL is reached.
    assert!(is_true(&scalar(&engine, "SELECT (1,NULL) < (2,2)")));
    assert!(is_false(&scalar(&engine, "SELECT (2,NULL) < (1,2)")));
}

#[test]
fn row_in_list() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    assert!(is_true(&scalar(
        &engine,
        "SELECT (1,2) IN ((3,4),(1,2),(5,6))"
    )));
    assert!(is_false(&scalar(&engine, "SELECT (1,2) IN ((3,4),(5,6))")));
    assert!(is_true(&scalar(
        &engine,
        "SELECT (1,2) NOT IN ((3,4),(5,6))"
    )));
    assert!(is_false(&scalar(
        &engine,
        "SELECT (1,2) NOT IN ((1,2),(5,6))"
    )));
}

#[test]
fn row_in_list_null() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    // No TRUE match but a NULL comparison present → NULL (scalar-IN parity).
    assert!(is_null(&scalar(
        &engine,
        "SELECT (1,2) IN ((1,NULL),(3,4))"
    )));
    // A definite match wins over the NULL row.
    assert!(is_true(&scalar(
        &engine,
        "SELECT (1,2) IN ((1,NULL),(1,2))"
    )));
}

#[test]
fn row_over_table_columns() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine.execute("CREATE TABLE t (a INT, b INT)").unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,2),(3,4),(1,5)")
        .unwrap();
    let resp = engine
        .execute("SELECT a,b FROM t WHERE (a,b) = (1,2)")
        .unwrap();
    let rows = match resp {
        QueryResponse::ResultSet(r) => r.rows,
        other => panic!("expected result set, got {other:?}"),
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], vec![Value::Int(1), Value::Int(2)]);

    let resp = engine
        .execute("SELECT a FROM t WHERE (a,b) IN ((1,2),(3,4)) ORDER BY a")
        .unwrap();
    let rows = match resp {
        QueryResponse::ResultSet(r) => r.rows,
        other => panic!("expected result set, got {other:?}"),
    };
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Int(1));
    assert_eq!(rows[1][0], Value::Int(3));
}

#[test]
fn row_arity_mismatch_errors_1241() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    // Row vs row of different arity.
    let err = engine.execute("SELECT (1,2) = (1,2,3)").unwrap_err();
    assert_eq!(err.code(), 1241, "got {err:?}");
    // Row vs scalar.
    let err = engine.execute("SELECT (1,2) = 1").unwrap_err();
    assert_eq!(err.code(), 1241, "got {err:?}");
    // IN list element arity mismatch.
    let err = engine.execute("SELECT (1,2) IN ((1,2),(3))").unwrap_err();
    assert_eq!(err.code(), 1241, "got {err:?}");
}
