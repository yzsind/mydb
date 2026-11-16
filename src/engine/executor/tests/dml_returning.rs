use super::*;

fn result(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> ResultSet {
    match engine.execute_with_session(session, sql).unwrap() {
        QueryResponse::ResultSet(rs) => rs,
        other => panic!("expected result set for {sql}, got {other:?}"),
    }
}

#[test]
fn pg18_basic_insert_update_delete_returning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY, v INT DEFAULT 7)",
        )
        .unwrap();

    let inserted = result(
        &engine,
        &mut session,
        "INSERT INTO t (v) VALUES (10), (20) RETURNING id, v",
    );
    assert_eq!(
        inserted.rows,
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)]
        ]
    );

    let updated = result(
        &engine,
        &mut session,
        "UPDATE t SET v = v + 1 WHERE id = 1 RETURNING old.v AS before_v, new.v AS after_v",
    );
    assert_eq!(updated.rows, vec![vec![Value::Int(10), Value::Int(11)]]);

    let deleted = result(
        &engine,
        &mut session,
        "DELETE FROM t WHERE id = 2 RETURNING WITH (OLD AS o, NEW AS n) o.*, n.v",
    );
    assert_eq!(
        deleted.rows,
        vec![vec![Value::Int(2), Value::Int(20), Value::Null]]
    );
}

#[test]
fn pg18_update_noop_still_returns_logical_row() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1, 9)")
        .unwrap();
    let rs = result(
        &engine,
        &mut session,
        "UPDATE t SET v = v WHERE id = 1 RETURNING *",
    );
    assert_eq!(rs.rows, vec![vec![Value::Int(1), Value::Int(9)]]);
}

#[test]
fn pg18_insert_conflict_modifiers_return_only_successful_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1, 10)")
        .unwrap();

    let ignored = result(
        &engine,
        &mut session,
        "INSERT IGNORE INTO t VALUES (1, 99), (2, 20) RETURNING *",
    );
    assert_eq!(ignored.rows, vec![vec![Value::Int(2), Value::Int(20)]]);

    let upserted = result(
        &engine,
        &mut session,
        "INSERT INTO t VALUES (1, 30) ON DUPLICATE KEY UPDATE v = VALUES(v) RETURNING old.v, new.v",
    );
    assert_eq!(upserted.rows, vec![vec![Value::Int(10), Value::Int(30)]]);

    let replaced = result(
        &engine,
        &mut session,
        "REPLACE INTO t VALUES (1, 40) RETURNING old.v, new.v",
    );
    assert_eq!(replaced.rows, vec![vec![Value::Null, Value::Int(40)]]);
}

#[test]
fn pg18_insert_select_returning_keeps_chunk_order() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE src (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE dst (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO src VALUES (1),(2),(3)")
        .unwrap();
    let rs = result(
        &engine,
        &mut session,
        "INSERT INTO dst SELECT id FROM src ORDER BY id RETURNING id",
    );
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ]
    );
}

#[test]
fn pg18_returning_prepared_metadata_is_described_without_execution() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(12))",
        )
        .unwrap();
    let columns = engine
        .describe_prepared_statement(
            &mut session,
            "UPDATE t SET v = ? WHERE id = ? RETURNING id, new.v AS value_after",
        )
        .unwrap();
    assert_eq!(
        columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
        vec!["id", "value_after"]
    );
    assert_eq!(columns[0].column_type, ColumnType::Long);
    assert_eq!(columns[1].column_type, ColumnType::VarString);
}

#[test]
fn pg18_returning_observes_before_trigger_post_image() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TRIGGER bi_t BEFORE INSERT ON t FOR EACH ROW SET NEW.v = NEW.v + 10",
        )
        .unwrap();
    let rs = result(
        &engine,
        &mut session,
        "INSERT INTO t VALUES (1, 5) RETURNING v",
    );
    assert_eq!(rs.rows, vec![vec![Value::Int(15)]]);
}

#[test]
fn pg18_returning_through_updatable_view_uses_view_columns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id INT PRIMARY KEY, v INT, hidden INT)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1, 10, 99)")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE VIEW vv AS SELECT id, v AS value FROM t",
        )
        .unwrap();
    let rs = result(
        &engine,
        &mut session,
        "UPDATE vv SET value = 11 WHERE id = 1 RETURNING *",
    );
    assert_eq!(
        rs.columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id", "value"]
    );
    assert_eq!(rs.rows, vec![vec![Value::Int(1), Value::Int(11)]]);
}

#[test]
fn pg18_single_target_join_dml_returning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE target (id INT PRIMARY KEY, v INT)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE source (id INT PRIMARY KEY, delta INT)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO target VALUES (1, 10), (2, 20)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO source VALUES (1, 3), (2, 4)")
        .unwrap();

    let updated = result(
        &engine,
        &mut session,
        "UPDATE target AS t JOIN source AS s ON t.id = s.id SET t.v = t.v + s.delta WHERE t.id = 1 RETURNING t.*",
    );
    assert_eq!(updated.rows, vec![vec![Value::Int(1), Value::Int(13)]]);

    let deleted = result(
        &engine,
        &mut session,
        "DELETE t FROM target AS t JOIN source AS s ON t.id = s.id WHERE t.id = 2 RETURNING old.*",
    );
    assert_eq!(deleted.rows, vec![vec![Value::Int(2), Value::Int(20)]]);
}

#[test]
fn pg18_multi_target_dml_returning_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE a (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE b (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO a VALUES (1, 10)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO b VALUES (1, 20)")
        .unwrap();

    let update_err = engine
        .execute_with_session(
            &mut session,
            "UPDATE a JOIN b ON a.id = b.id SET a.v = 11, b.v = 21 RETURNING *",
        )
        .unwrap_err();
    assert_eq!(update_err.code(), 1235);

    let delete_err = engine
        .execute_with_session(
            &mut session,
            "DELETE a, b FROM a JOIN b ON a.id = b.id RETURNING *",
        )
        .unwrap_err();
    assert_eq!(delete_err.code(), 1235);
}
