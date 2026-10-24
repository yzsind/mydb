//! Regressions for the 2026-09-27 constraint audit.
use super::*;

fn fixture() -> (tempfile::TempDir, StorageQueryEngine) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    (dir, e)
}

fn rows(e: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(r) = e.execute(sql).unwrap().materialize().unwrap() else {
        panic!("expected result set: {sql}")
    };
    r.rows
}

#[test]
fn constraints_audit_partition_migration_duplicates_are_atomic() {
    for seed in [
        "(1,1),(2,2)",
        "(1,1),(2,2),(3,3)",
        "(1,1),(11,2)",
        "(1,1),(21,2)",
    ] {
        let (_dir, e) = fixture();
        e.execute("CREATE TABLE p(id INT PRIMARY KEY,v INT) PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN(10), PARTITION p1 VALUES LESS THAN(20), PARTITION p2 VALUES LESS THAN(100))").unwrap();
        e.execute(&format!("INSERT INTO p VALUES {seed}")).unwrap();
        let before = rows(&e, "SELECT * FROM p ORDER BY id");
        assert_eq!(
            e.execute("UPDATE p SET id=25").unwrap_err().code(),
            1062,
            "{seed}"
        );
        assert_eq!(rows(&e, "SELECT * FROM p ORDER BY id"), before);
        let mut session = SessionContext::default();
        e.execute_with_session(&mut session, "UPDATE IGNORE p SET id=25")
            .unwrap();
        let QueryResponse::ResultSet(warnings) = e
            .execute_with_session(&mut session, "SHOW WARNINGS")
            .unwrap()
        else {
            panic!("expected warning result set")
        };
        let warnings = warnings.rows;
        assert_eq!(warnings.len(), before.len() - 1);
        assert!(warnings.iter().all(|w| w[1] == Value::Int(1062)));
        assert_eq!(
            rows(&e, "SELECT COUNT(*) FROM p WHERE id=25"),
            vec![vec![Value::Int(1)]]
        );
        assert_eq!(rows(&e, "SELECT * FROM p").len(), before.len());
    }
}

#[test]
fn constraints_audit_partition_secondary_key_semantics() {
    for (column, index, seed, should_conflict) in [
        ("v INT", "k,v", "(1,1,7),(2,2,7)", true),
        ("v INT", "k,v", "(1,1,NULL),(2,2,NULL)", false),
        (
            "v VARCHAR(20) COLLATE utf8mb4_general_ci",
            "k,v",
            "(1,1,'ABC'),(2,2,'abc')",
            true,
        ),
        ("v VARCHAR(20)", "k,v(2)", "(1,1,'ab1'),(2,2,'ab2')", true),
        (
            "v VARCHAR(20) COLLATE utf8mb4_0900_ai_ci",
            "k,v",
            "(1,1,'abc'),(2,2,'abc ')",
            false,
        ),
        (
            "v VARCHAR(20)",
            "k,(LOWER(v))",
            "(1,1,'ABC'),(2,2,'abc')",
            true,
        ),
        ("v DECIMAL(10,2)", "k,v", "(1,1,1.00),(2,2,1.0)", true),
        (
            "v INT, g INT GENERATED ALWAYS AS (ABS(v)) VIRTUAL",
            "k,g",
            "(1,1,-7),(2,2,7)",
            true,
        ),
        (
            "v INT, g INT GENERATED ALWAYS AS (ABS(v)) STORED",
            "k,g",
            "(1,1,-7),(2,2,7)",
            true,
        ),
    ] {
        let (_dir, e) = fixture();
        e.execute(&format!("CREATE TABLE p(id INT,k INT,{column},PRIMARY KEY(k,id),UNIQUE KEY uk({index})) PARTITION BY RANGE(k) (PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN(100))")).unwrap();
        e.execute(&format!("INSERT INTO p(id,k,v) VALUES {seed}"))
            .unwrap();
        let before = rows(&e, "SELECT * FROM p ORDER BY id");
        let result = e.execute("UPDATE p SET k=25");
        if should_conflict {
            assert_eq!(result.unwrap_err().code(), 1062, "{column}/{index}");
            assert_eq!(rows(&e, "SELECT * FROM p ORDER BY id"), before);
        } else {
            result.unwrap();
            assert_eq!(
                rows(&e, "SELECT COUNT(*) FROM p WHERE k=25"),
                vec![vec![Value::Int(2)]]
            );
        }
    }
}

#[test]
fn constraints_audit_ignore_does_not_reserve_rejected_rows_other_keys() {
    let (_dir, e) = fixture();
    e.execute("CREATE TABLE p(id INT,k INT,v INT,PRIMARY KEY(k,id),UNIQUE KEY uk(k,v)) PARTITION BY RANGE(k) (PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN(100))").unwrap();
    // Row 2 conflicts only on uk; its otherwise unused PRIMARY must not
    // prevent row 3 from taking that key.
    e.execute("INSERT INTO p VALUES(1,1,7),(2,2,7),(2,3,8)")
        .unwrap();
    e.execute("UPDATE IGNORE p SET k=25").unwrap();
    assert_eq!(
        rows(&e, "SELECT id,k,v FROM p ORDER BY k,id"),
        vec![
            vec![Value::Int(2), Value::Int(2), Value::Int(7)],
            vec![Value::Int(1), Value::Int(25), Value::Int(7)],
            vec![Value::Int(2), Value::Int(25), Value::Int(8)],
        ]
    );
}

#[test]
fn constraints_audit_rr_current_unique_conflict_preserves_transaction() {
    for isolation in ["REPEATABLE READ", "READ COMMITTED"] {
        for conflict in ["(1,9)", "(9,1)"] {
            let (_dir, e) = fixture();
            e.execute("CREATE TABLE t(id INT PRIMARY KEY,v INT UNIQUE)")
                .unwrap();
            let mut a = SessionContext::default();
            let mut b = SessionContext::default();
            e.execute_with_session(
                &mut b,
                &format!("SET SESSION TRANSACTION ISOLATION LEVEL {isolation}"),
            )
            .unwrap();
            e.execute_with_session(&mut b, "BEGIN").unwrap();
            e.execute_with_session(&mut b, "INSERT INTO t VALUES(2,2)")
                .unwrap();
            e.execute_with_session(&mut a, "INSERT INTO t VALUES(1,1)")
                .unwrap();
            assert_eq!(
                e.execute_with_session(&mut b, &format!("INSERT INTO t VALUES {conflict}"))
                    .unwrap_err()
                    .code(),
                1062,
                "{isolation} {conflict}"
            );
            e.execute_with_session(&mut b, "INSERT INTO t VALUES(3,3)")
                .unwrap();
            e.execute_with_session(&mut b, "COMMIT").unwrap();
            assert_eq!(
                rows(&e, "SELECT COUNT(*) FROM t"),
                vec![vec![Value::Int(3)]]
            );
        }
    }
}
