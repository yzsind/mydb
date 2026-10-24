use super::*;

fn result_rows(response: QueryResponse) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = response else {
        panic!("expected result set");
    };
    result.rows
}

#[test]
fn batch_update_preserves_unique_index_and_ignore_skips_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, u INT, UNIQUE KEY uk(u))")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1,1),(2,2)").unwrap();

    assert_eq!(engine.execute("UPDATE t SET u=7").unwrap_err().code(), 1062);
    assert_eq!(
        result_rows(engine.execute("SELECT id,u FROM t ORDER BY id").unwrap()),
        vec![
            vec![Value::Int(1), Value::Int(1)],
            vec![Value::Int(2), Value::Int(2)]
        ]
    );
    let response = engine.execute("UPDATE IGNORE t SET u=7").unwrap();
    assert_eq!(response, QueryResponse::OkAffected { affected_rows: 1 });
    assert_eq!(
        result_rows(
            engine
                .execute("SELECT COUNT(*) FROM t FORCE INDEX(uk) WHERE u=7")
                .unwrap()
        ),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn replace_coerces_probe_key_and_odku_reads_prior_assignments() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY,a INT,b INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (5,10,20)").unwrap();
    assert_eq!(
        engine.execute("REPLACE INTO t VALUES ('5',10,20)").unwrap(),
        QueryResponse::OkAffected { affected_rows: 2 }
    );
    engine
        .execute("INSERT INTO t VALUES (5,1,1) ON DUPLICATE KEY UPDATE a=a+1,b=a")
        .unwrap();
    assert_eq!(
        result_rows(engine.execute("SELECT a,b FROM t").unwrap()),
        vec![vec![Value::Int(11), Value::Int(11)]]
    );
}

#[test]
fn dml_subqueries_enforce_shape_and_bind_outer_columns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY,a INT,b INT)")
        .unwrap();
    engine.execute("CREATE TABLE s (x INT,y INT)").unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,0,9),(2,0,10)")
        .unwrap();
    engine
        .execute("INSERT INTO s VALUES (2,10),(3,11)")
        .unwrap();
    assert_eq!(
        engine
            .execute("UPDATE t SET a=(SELECT x FROM s)")
            .unwrap_err()
            .code(),
        1242
    );
    engine
        .execute("UPDATE t SET a=7 WHERE (id,b) IN (SELECT x,y FROM s)")
        .unwrap();
    assert_eq!(
        result_rows(engine.execute("SELECT id,a FROM t ORDER BY id").unwrap()),
        vec![
            vec![Value::Int(1), Value::Int(0)],
            vec![Value::Int(2), Value::Int(7)]
        ]
    );
    engine
        .execute("UPDATE t SET a=8 WHERE id IN (SELECT x FROM s WHERE x=id)")
        .unwrap();
    assert_eq!(
        result_rows(engine.execute("SELECT a FROM t WHERE id=2").unwrap()),
        vec![vec![Value::Int(8)]]
    );
    engine
        .execute("INSERT INTO t VALUES ((SELECT MAX(x) FROM s),0,0)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (3,0,0) ON DUPLICATE KEY UPDATE a=(SELECT MAX(y) FROM s)")
        .unwrap();
    assert_eq!(
        result_rows(engine.execute("SELECT a FROM t WHERE id=3").unwrap()),
        vec![vec![Value::Int(11)]]
    );
}

#[test]
fn ignore_repairs_values_and_warns_on_check_constraint() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY,x TINYINT NOT NULL,CHECK (x>=0))")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT IGNORE INTO t VALUES (1,NULL)")
        .unwrap();
    assert_eq!(session.warning_count(), 1);
    engine
        .execute_with_session(&mut session, "UPDATE IGNORE t SET x=999")
        .unwrap();
    assert_eq!(session.warning_count(), 1);
    assert_eq!(
        result_rows(engine.execute("SELECT x FROM t").unwrap()),
        vec![vec![Value::Int(127)]]
    );
    assert_eq!(
        engine
            .execute_with_session(&mut session, "INSERT IGNORE INTO t VALUES (2,-1)")
            .unwrap(),
        QueryResponse::OkAffected { affected_rows: 0 }
    );
    assert_eq!(session.warning_count(), 1);
    let warnings = result_rows(
        engine
            .execute_with_session(&mut session, "SHOW WARNINGS")
            .unwrap(),
    );
    assert_eq!(warnings[0][1], Value::Int(3819));
}

#[test]
fn ignore_skips_view_check_failure_with_warning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY,v INT)")
        .unwrap();
    engine
        .execute("CREATE VIEW positive AS SELECT id,v FROM t WHERE v>0 WITH CHECK OPTION")
        .unwrap();
    assert_eq!(
        engine
            .execute_with_session(
                &mut session,
                "INSERT IGNORE INTO positive VALUES (1,-5),(2,5)"
            )
            .unwrap(),
        QueryResponse::OkAffected { affected_rows: 1 }
    );
    assert_eq!(session.warning_count(), 1);
    let warnings = result_rows(
        engine
            .execute_with_session(&mut session, "SHOW WARNINGS")
            .unwrap(),
    );
    assert_eq!(warnings[0][1], Value::Int(1369));
    assert_eq!(
        result_rows(engine.execute("SELECT id FROM t").unwrap()),
        vec![vec![Value::Int(2)]]
    );
}

#[test]
fn invalid_delete_join_is_rejected_without_deleting_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("CREATE TABLE s (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1)").unwrap();
    engine.execute("INSERT INTO s VALUES (1)").unwrap();
    assert_eq!(
        engine
            .execute("DELETE FROM t JOIN s ON t.id=s.id")
            .unwrap_err()
            .code(),
        1064
    );
    assert_eq!(
        result_rows(engine.execute("SELECT id FROM t").unwrap()),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn bit_integer_overflow_reports_out_of_range() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t (b BIT(1))").unwrap();
    assert_eq!(
        engine
            .execute("INSERT INTO t VALUES (2)")
            .unwrap_err()
            .code(),
        1264
    );
}

#[test]
fn multi_update_counts_changed_and_matched_rows_separately() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY,x INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE s (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1,5),(2,0)").unwrap();
    engine.execute("INSERT INTO s VALUES (1),(2)").unwrap();
    assert_eq!(
        engine
            .execute("UPDATE t JOIN s ON t.id=s.id SET t.x=5")
            .unwrap(),
        QueryResponse::OkAffected { affected_rows: 1 }
    );
    assert_eq!(crate::engine::api::take_statement_matched_rows(), Some(2));
}
