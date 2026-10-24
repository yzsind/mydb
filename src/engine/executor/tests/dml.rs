use super::*;

#[test]
fn failed_autocommit_statements_do_not_leak_transactions_or_locks() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE autocommit_cleanup (
                id INT PRIMARY KEY,
                v INT,
                CONSTRAINT positive_v CHECK (v > 0)
            )",
        )
        .unwrap();

    for id in 0..100 {
        let err = engine
            .execute_with_session(
                &mut session,
                &format!("INSERT INTO autocommit_cleanup VALUES ({id}, -1)"),
            )
            .unwrap_err();
        assert_eq!(err.code(), 3819, "{}", err.message());
        assert_eq!(
            session.current_txn, None,
            "failed autocommit statement {id} leaked a transaction"
        );
    }

    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO autocommit_cleanup VALUES (101, 1)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "DELETE FROM autocommit_cleanup")
        .unwrap();
    engine
        .execute_with_session(&mut session, "DROP TABLE autocommit_cleanup")
        .unwrap();
    assert_eq!(session.current_txn, None);
}

#[test]
fn select_into_var_list_assigns_columns_to_user_variables() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // Constant SELECT INTO (no FROM).
    engine
        .execute_with_session(&mut session, "SELECT 1, 'a' INTO @x, @y")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut session, "SELECT @x, @y")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(1));
    assert_eq!(rs.rows[0][1].to_display_string(), "a");

    // FROM-table SELECT INTO with a row.
    engine
        .execute_with_session(&mut session, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1, 100)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SELECT id, v INTO @p, @q FROM t WHERE id = 1")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut session, "SELECT @p, @q")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(1));
    assert_eq!(rs.rows[0][1], Value::Int(100));

    // >1 row → error 1172.
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (2, 200)")
        .unwrap();
    let err = engine
        .execute_with_session(&mut session, "SELECT id INTO @z FROM t")
        .unwrap_err();
    assert_eq!(err.code(), 1172, "{}", err.message());
}

#[test]
fn multi_delete_left_join_anti_semi_removes_unmatched_left_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "CREATE TABLE a (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE b (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO a VALUES (1),(2),(3),(4)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO b VALUES (1),(3)")
        .unwrap();

    engine
        .execute_with_session(
            &mut session,
            "DELETE a FROM a LEFT JOIN b ON a.id = b.id WHERE b.id IS NULL",
        )
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut session, "SELECT id FROM a ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let ids: Vec<i64> = rs
        .rows
        .into_iter()
        .map(|r| match r[0] {
            Value::Int(i) => i,
            _ => panic!("expected int"),
        })
        .collect();
    assert_eq!(ids, vec![1, 3]);
}

#[test]
fn multi_update_left_join_skips_null_complemented_outer_rows() {
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

    // UPDATE the LEFT side (`a`) is fine.
    engine
        .execute_with_session(
            &mut session,
            "UPDATE a LEFT JOIN b ON a.id = b.id SET a.v = COALESCE(b.v, -1)",
        )
        .unwrap();
    let row = match engine
        .execute_with_session(&mut session, "SELECT v FROM a")
        .unwrap()
    {
        QueryResponse::ResultSet(rs) => rs.rows[0][0].clone(),
        _ => panic!("expected result set"),
    };
    assert_eq!(row, Value::Int(-1));

    // Updating the non-preserved RIGHT side is legal, but the unmatched
    // null-complemented row is not materialised into the target table.
    let response = engine
        .execute_with_session(
            &mut session,
            "UPDATE a LEFT JOIN b ON a.id = b.id SET b.v = 99",
        )
        .unwrap();
    assert!(matches!(
        response,
        QueryResponse::OkAffected { affected_rows: 0 }
    ));
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut session, "SELECT COUNT(*) FROM b")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(0));
}

#[test]
fn delete_low_priority_quick_modifiers_are_accepted_as_noop() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1),(2),(3)")
        .unwrap();

    engine
        .execute_with_session(&mut session, "DELETE LOW_PRIORITY FROM t WHERE id = 1")
        .unwrap();
    engine
        .execute_with_session(&mut session, "DELETE QUICK FROM t WHERE id = 2")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "DELETE LOW_PRIORITY QUICK FROM t WHERE id = 3",
        )
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut session, "SELECT COUNT(*) FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(0));
}

#[test]
fn delete_ignore_swallows_foreign_key_restrict_errors() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "CREATE TABLE p (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE c (id INT PRIMARY KEY, pid INT, \
             FOREIGN KEY (pid) REFERENCES p (id))",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO p VALUES (1),(2),(3)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO c VALUES (10, 2)")
        .unwrap();

    // Without IGNORE the delete of p.id=2 would fail (1451 FK RESTRICT).
    // With IGNORE the row is skipped; p.id=1 and p.id=3 are still removed.
    engine
        .execute_with_session(&mut session, "DELETE IGNORE FROM p")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut session, "SELECT id FROM p ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
    assert_eq!(rs.rows[0][0], Value::Int(2));
}

#[test]
fn update_and_delete_order_by_limit_affect_only_window() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, INDEX v_idx (v))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 30), (2, 10), (3, 20), (4, 40)")
        .unwrap();

    engine
        .execute("UPDATE t SET v = v + 100 WHERE id > 0 ORDER BY v LIMIT 2")
        .unwrap();
    let QueryResponse::ResultSet(after_update) =
        engine.execute("SELECT id, v FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        after_update.rows,
        vec![
            vec![Value::Int(1), Value::Int(30)],
            vec![Value::Int(2), Value::Int(110)],
            vec![Value::Int(3), Value::Int(120)],
            vec![Value::Int(4), Value::Int(40)],
        ]
    );

    engine
        .execute("DELETE FROM t WHERE id > 0 ORDER BY v DESC LIMIT 1")
        .unwrap();
    let QueryResponse::ResultSet(after_delete) =
        engine.execute("SELECT id, v FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        after_delete.rows,
        vec![
            vec![Value::Int(1), Value::Int(30)],
            vec![Value::Int(2), Value::Int(110)],
            vec![Value::Int(4), Value::Int(40)],
        ]
    );
}

#[test]
fn delete_retries_across_concurrent_delete_insert_replacement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 500));
    let mut owner = SessionContext::default();

    engine
        .execute_with_session(&mut owner, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO t VALUES (1, 10)")
        .unwrap();
    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "DELETE FROM t WHERE id = 1")
        .unwrap();

    let waiter_engine = std::sync::Arc::clone(&engine);
    let waiter = std::thread::spawn(move || {
        let mut waiter = SessionContext::default();
        waiter_engine.execute_with_session(&mut waiter, "BEGIN")?;
        match waiter_engine.execute_with_session(&mut waiter, "DELETE FROM t WHERE id = 1")? {
            QueryResponse::OkAffected { affected_rows } => assert_eq!(affected_rows, 1),
            other => panic!("expected affected rows, got {other:?}"),
        }
        match waiter_engine.execute_with_session(&mut waiter, "SELECT id FROM t WHERE id = 1")? {
            QueryResponse::ResultSet(rs) => {
                assert!(
                    rs.rows.is_empty(),
                    "id=1 still visible after DELETE: {:?}",
                    rs.rows
                )
            }
            other => panic!("expected result set, got {other:?}"),
        }
        Ok::<SessionContext, QueryError>(waiter)
    });

    std::thread::sleep(Duration::from_millis(20));
    engine
        .execute_with_session(&mut owner, "INSERT INTO t VALUES (1, 11)")
        .unwrap();
    engine.execute_with_session(&mut owner, "COMMIT").unwrap();

    let mut waiter = waiter.join().unwrap().unwrap();
    engine
        .execute_with_session(&mut waiter, "INSERT INTO t VALUES (1, 12)")
        .unwrap();
    engine.execute_with_session(&mut waiter, "COMMIT").unwrap();

    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT id, v FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1), Value::Int(12)]]);
}

#[test]
fn ordered_update_limit_refills_after_waiting_candidate_stops_matching() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 500));
    let mut owner = SessionContext::default();
    engine
        .execute_with_session(
            &mut owner,
            "CREATE TABLE t (id INT PRIMARY KEY, v INT, mark INT)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO t VALUES (1,10,0),(2,20,0)")
        .unwrap();
    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "UPDATE t SET v=100 WHERE id=1")
        .unwrap();

    let waiter_engine = std::sync::Arc::clone(&engine);
    let waiter = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        waiter_engine.execute_with_session(
            &mut session,
            "UPDATE t SET mark=1 WHERE v<50 ORDER BY id LIMIT 1",
        )
    });
    std::thread::sleep(Duration::from_millis(20));
    engine.execute_with_session(&mut owner, "COMMIT").unwrap();

    let response = waiter.join().unwrap().unwrap();
    assert!(matches!(
        response,
        QueryResponse::OkAffected { affected_rows: 1 }
    ));
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id,v,mark FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::Int(100), Value::Int(0)],
            vec![Value::Int(2), Value::Int(20), Value::Int(1)],
        ]
    );
}

#[test]
fn ordered_delete_limit_refills_after_waiting_candidate_stops_matching() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 500));
    let mut owner = SessionContext::default();
    engine
        .execute_with_session(&mut owner, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO t VALUES (1,10),(2,20)")
        .unwrap();
    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "UPDATE t SET v=100 WHERE id=1")
        .unwrap();

    let waiter_engine = std::sync::Arc::clone(&engine);
    let waiter = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        waiter_engine
            .execute_with_session(&mut session, "DELETE FROM t WHERE v<50 ORDER BY id LIMIT 1")
    });
    std::thread::sleep(Duration::from_millis(20));
    engine.execute_with_session(&mut owner, "COMMIT").unwrap();

    let response = waiter.join().unwrap().unwrap();
    assert!(matches!(
        response,
        QueryResponse::OkAffected { affected_rows: 1 }
    ));
    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT id,v FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1), Value::Int(100)]]);
}

#[test]
fn delete_retries_after_prior_write_across_concurrent_replacement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 500));
    let mut owner = SessionContext::default();

    engine
        .execute_with_session(&mut owner, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();
    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "DELETE FROM t WHERE id = 1")
        .unwrap();

    let waiter_engine = std::sync::Arc::clone(&engine);
    let waiter = std::thread::spawn(move || {
        let mut waiter = SessionContext::default();
        waiter_engine.execute_with_session(&mut waiter, "BEGIN")?;
        waiter_engine.execute_with_session(&mut waiter, "UPDATE t SET v = v + 1 WHERE id = 2")?;
        waiter_engine.execute_with_session(&mut waiter, "DELETE FROM t WHERE id = 1")?;
        Ok::<SessionContext, QueryError>(waiter)
    });

    std::thread::sleep(Duration::from_millis(20));
    engine
        .execute_with_session(&mut owner, "INSERT INTO t VALUES (1, 11)")
        .unwrap();
    engine.execute_with_session(&mut owner, "COMMIT").unwrap();

    let mut waiter = waiter.join().unwrap().unwrap();
    engine
        .execute_with_session(&mut waiter, "INSERT INTO t VALUES (1, 12)")
        .unwrap();
    engine.execute_with_session(&mut waiter, "COMMIT").unwrap();

    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT id, v FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::Int(12)],
            vec![Value::Int(2), Value::Int(21)]
        ]
    );
}

#[test]
fn delete_retries_after_concurrent_update_then_replacement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 500));
    let mut owner = SessionContext::default();

    engine
        .execute_with_session(&mut owner, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO t VALUES (1, 10)")
        .unwrap();
    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "UPDATE t SET v = 11 WHERE id = 1")
        .unwrap();

    let waiter_engine = std::sync::Arc::clone(&engine);
    let waiter = std::thread::spawn(move || {
        let mut waiter = SessionContext::default();
        waiter_engine.execute_with_session(&mut waiter, "BEGIN")?;
        waiter_engine.execute_with_session(&mut waiter, "DELETE FROM t WHERE id = 1")?;
        Ok::<SessionContext, QueryError>(waiter)
    });

    std::thread::sleep(Duration::from_millis(20));
    engine
        .execute_with_session(&mut owner, "DELETE FROM t WHERE id = 1")
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO t VALUES (1, 12)")
        .unwrap();
    engine.execute_with_session(&mut owner, "COMMIT").unwrap();

    let mut waiter = waiter.join().unwrap().unwrap();
    engine
        .execute_with_session(&mut waiter, "INSERT INTO t VALUES (1, 13)")
        .unwrap();
    engine.execute_with_session(&mut waiter, "COMMIT").unwrap();

    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT id, v FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1), Value::Int(13)]]);
}

#[test]
fn update_rechecks_where_after_waiting_for_concurrent_writer() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 500));
    engine
        .execute("CREATE TABLE mvcc_recheck_u (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO mvcc_recheck_u VALUES (1, 10)")
        .unwrap();

    let mut owner = SessionContext::default();
    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "UPDATE mvcc_recheck_u SET v = 20 WHERE id = 1")
        .unwrap();

    let waiter_engine = std::sync::Arc::clone(&engine);
    let waiter = std::thread::spawn(move || {
        waiter_engine.execute("UPDATE mvcc_recheck_u SET v = 30 WHERE v = 10")
    });
    std::thread::sleep(Duration::from_millis(20));
    engine.execute_with_session(&mut owner, "COMMIT").unwrap();

    assert_eq!(
        waiter.join().unwrap().unwrap(),
        QueryResponse::OkAffected { affected_rows: 0 }
    );
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT v FROM mvcc_recheck_u WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(20)]]);
}

#[test]
fn delete_rechecks_where_after_waiting_for_concurrent_writer() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 500));
    engine
        .execute("CREATE TABLE mvcc_recheck_d (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO mvcc_recheck_d VALUES (1, 10)")
        .unwrap();

    let mut owner = SessionContext::default();
    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "UPDATE mvcc_recheck_d SET v = 20 WHERE id = 1")
        .unwrap();

    let waiter_engine = std::sync::Arc::clone(&engine);
    let waiter = std::thread::spawn(move || {
        waiter_engine.execute("DELETE FROM mvcc_recheck_d WHERE v = 10")
    });
    std::thread::sleep(Duration::from_millis(20));
    engine.execute_with_session(&mut owner, "COMMIT").unwrap();

    assert_eq!(
        waiter.join().unwrap().unwrap(),
        QueryResponse::OkAffected { affected_rows: 0 }
    );
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT v FROM mvcc_recheck_d WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(20)]]);
}

#[test]
fn cross_update_then_delete_deadlock_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 500));
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();

    let first_engine = std::sync::Arc::clone(&engine);
    let second_engine = std::sync::Arc::clone(&engine);
    let (first_ready_tx, first_ready_rx) = std::sync::mpsc::channel();
    let (second_ready_tx, second_ready_rx) = std::sync::mpsc::channel();

    let first = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        first_engine.execute_with_session(&mut session, "BEGIN")?;
        first_engine.execute_with_session(&mut session, "UPDATE t SET v = 11 WHERE id = 1")?;
        first_ready_tx.send(()).unwrap();
        second_ready_rx.recv().unwrap();
        first_engine.execute_with_session(&mut session, "DELETE FROM t WHERE id = 2")
    });

    let second = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        second_engine.execute_with_session(&mut session, "BEGIN")?;
        second_engine.execute_with_session(&mut session, "UPDATE t SET v = 21 WHERE id = 2")?;
        second_ready_tx.send(()).unwrap();
        first_ready_rx.recv().unwrap();
        second_engine.execute_with_session(&mut session, "DELETE FROM t WHERE id = 1")
    });

    let first = first.join().unwrap();
    let second = second.join().unwrap();
    assert!(
        matches!((&first, &second), (Err(err), _) | (_, Err(err)) if err.is(crate::error::catalog::ER_LOCK_DEADLOCK)),
        "expected one deadlock error, got first={first:?} second={second:?}"
    );
}

#[test]
fn repeatable_read_delete_after_select_uses_current_read_for_replacement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut reader = SessionContext::default();
    let mut writer = SessionContext::default();

    engine
        .execute_with_session(&mut reader, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut reader, "INSERT INTO t VALUES (1, 10)")
        .unwrap();

    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
    let QueryResponse::ResultSet(snapshot) = engine
        .execute_with_session(&mut reader, "SELECT v FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(snapshot.rows, vec![vec![Value::Int(10)]]);

    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut writer, "DELETE FROM t WHERE id = 1")
        .unwrap();
    engine
        .execute_with_session(&mut writer, "INSERT INTO t VALUES (1, 11)")
        .unwrap();
    engine.execute_with_session(&mut writer, "COMMIT").unwrap();

    let QueryResponse::OkAffected { affected_rows } = engine
        .execute_with_session(&mut reader, "DELETE FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected affected rows");
    };
    assert_eq!(affected_rows, 1);
    engine
        .execute_with_session(&mut reader, "INSERT INTO t VALUES (1, 12)")
        .unwrap();
    engine.execute_with_session(&mut reader, "COMMIT").unwrap();

    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT id, v FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1), Value::Int(12)]]);
}

#[test]
fn insert_after_delete_ignores_historical_unique_versions_from_stale_index_entries() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut first = SessionContext::default();
    let mut second = SessionContext::default();

    engine
        .execute_with_session(&mut first, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut first, "INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();

    let snapshot = engine
        .db_for_tests()
        .read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let _guard = engine
        .db_for_tests()
        .register_statement_snapshot(snapshot.snapshot_scn);

    engine.execute_with_session(&mut first, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut first, "UPDATE t SET v = 21 WHERE id = 2")
        .unwrap();

    engine.execute_with_session(&mut second, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut second, "DELETE FROM t WHERE id = 1")
        .unwrap();
    engine
        .execute_with_session(&mut second, "INSERT INTO t VALUES (1, 11)")
        .unwrap();
    engine.execute_with_session(&mut second, "COMMIT").unwrap();

    let QueryResponse::OkAffected { affected_rows } = engine
        .execute_with_session(&mut first, "DELETE FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected affected rows");
    };
    assert_eq!(affected_rows, 1);
    engine
        .execute_with_session(&mut first, "INSERT INTO t VALUES (1, 12)")
        .unwrap();
    engine.execute_with_session(&mut first, "COMMIT").unwrap();

    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT id, v FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::Int(12)],
            vec![Value::Int(2), Value::Int(21)]
        ]
    );
}

#[test]
fn different_row_updates_can_overlap_same_row_waits() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 200));
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();

    let first_engine = std::sync::Arc::clone(&engine);
    let second_engine = std::sync::Arc::clone(&engine);
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let first = std::thread::spawn(move || {
        let mut session = short_lock_wait_session();
        first_engine
            .execute_with_session(&mut session, "BEGIN")
            .unwrap();
        first_engine
            .execute_with_session(&mut session, "UPDATE t SET v = 11 WHERE id = 1")
            .unwrap();
        locked_tx.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(100));
        first_engine
            .execute_with_session(&mut session, "COMMIT")
            .unwrap();
    });
    locked_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let second = std::thread::spawn(move || {
        let mut session = short_lock_wait_session();
        let started = Instant::now();
        second_engine
            .execute_with_session(&mut session, "UPDATE t SET v = 21 WHERE id = 2")
            .unwrap();
        started.elapsed()
    });
    let elapsed = second.join().unwrap();
    first.join().unwrap();
    assert!(
        elapsed < Duration::from_millis(100),
        "different-row update waited behind unrelated row lock"
    );

    let mut holder = short_lock_wait_session();
    let mut waiter = short_lock_wait_session();
    engine.execute_with_session(&mut holder, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut holder, "UPDATE t SET v = 12 WHERE id = 1")
        .unwrap();
    engine.execute_with_session(&mut waiter, "BEGIN").unwrap();
    let err = engine
        .execute_with_session(&mut waiter, "UPDATE t SET v = 13 WHERE id = 1")
        .unwrap_err();
    assert_eq!(err.code(), 1205);
    engine
        .execute_with_session(&mut holder, "ROLLBACK")
        .unwrap();
    engine
        .execute_with_session(&mut waiter, "ROLLBACK")
        .unwrap();
}

#[test]
fn delete_waits_on_uncommitted_deleted_primary_key() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_lock_wait_ms(dir.path(), 5);
    let mut owner = short_lock_wait_session();
    let mut waiter = short_lock_wait_session();

    engine
        .execute_with_session(&mut owner, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO t VALUES (1, 10)")
        .unwrap();
    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "DELETE FROM t WHERE id = 1")
        .unwrap();

    engine.execute_with_session(&mut waiter, "BEGIN").unwrap();
    let blocked = engine
        .execute_with_session(&mut waiter, "DELETE FROM t WHERE id = 1")
        .unwrap_err();
    assert_eq!(blocked.code(), 1205);
}

#[test]
fn indexed_update_delete_respect_exclusive_range_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, INDEX v_idx (v))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 11), (3, 12), (4, 13)")
        .unwrap();

    assert_eq!(
        engine
            .execute("UPDATE t SET v = v + 100 WHERE v > 11")
            .unwrap(),
        QueryResponse::OkAffected { affected_rows: 2 }
    );
    assert_eq!(
        engine.execute("DELETE FROM t WHERE v < 11").unwrap(),
        QueryResponse::OkAffected { affected_rows: 1 }
    );

    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT id, v FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(2), Value::Int(11)],
            vec![Value::Int(3), Value::Int(112)],
            vec![Value::Int(4), Value::Int(113)],
        ]
    );
}

#[test]
fn full_primary_key_update_and_delete_take_direct_unique_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE bmsql_new_order (\
             no_w_id INT NOT NULL, no_d_id INT NOT NULL, no_o_id INT NOT NULL, carrier INT, \
             PRIMARY KEY (no_w_id, no_d_id, no_o_id))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO bmsql_new_order VALUES (1, 7, 28, 0), (1, 7, 29, 0)")
        .unwrap();

    engine
        .execute(
            "UPDATE bmsql_new_order SET carrier = 3 \
             WHERE no_w_id = 1 AND no_d_id = 7 AND no_o_id = 28",
        )
        .unwrap();
    engine
        .execute(
            "DELETE FROM bmsql_new_order \
             WHERE no_w_id = 1 AND no_d_id = 7 AND no_o_id = 29",
        )
        .unwrap();
    engine
        .execute(
            "DELETE FROM bmsql_new_order \
             WHERE no_w_id = 1 AND no_d_id = 7 AND no_o_id = 29",
        )
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT no_o_id, carrier FROM bmsql_new_order ORDER BY no_o_id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(28), Value::Int(3)]]);
}

#[test]
fn ordered_limited_dml_uses_index_order_without_materializing_all_matches() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE bmsql_new_order (\
             no_w_id INT NOT NULL, no_d_id INT NOT NULL, no_o_id INT NOT NULL, carrier INT, \
             PRIMARY KEY (no_w_id, no_d_id, no_o_id))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO bmsql_new_order VALUES \
             (1, 7, 30, 0), (1, 7, 28, 0), (1, 7, 29, 0), (1, 8, 1, 0)",
        )
        .unwrap();

    let table = engine.db.table("test.bmsql_new_order").unwrap();
    let selection = Expr::Binary {
        left: Box::new(Expr::Binary {
            left: Box::new(Expr::Identifier("no_w_id".into())),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Int(1))),
        }),
        op: BinaryOp::And,
        right: Box::new(Expr::Binary {
            left: Box::new(Expr::Identifier("no_d_id".into())),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Int(7))),
        }),
    };
    let candidates = ordered_index_limited_dml_candidates(
        &engine.db,
        engine
            .db
            .read_only_statement_view(IsolationLevel::ReadCommitted, None),
        &table,
        Some(&selection),
        &[OrderBy {
            expr: Expr::Identifier("no_o_id".into()),
            asc: true,
            nulls_first: None,
        }],
        Some(1),
        None,
    )
    .unwrap()
    .unwrap();
    assert_eq!(candidates.len(), 1);

    engine
        .execute(
            "UPDATE bmsql_new_order SET carrier = 9 \
             WHERE no_w_id = 1 AND no_d_id = 7 ORDER BY no_o_id LIMIT 1",
        )
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT no_o_id, carrier FROM bmsql_new_order WHERE no_w_id = 1 AND no_d_id = 7 ORDER BY no_o_id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(28), Value::Int(9)],
            vec![Value::Int(29), Value::Int(0)],
            vec![Value::Int(30), Value::Int(0)]
        ]
    );

    engine
        .execute(
            "DELETE FROM bmsql_new_order \
             WHERE no_w_id = 1 AND no_d_id = 7 ORDER BY no_o_id LIMIT 1",
        )
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT no_o_id FROM bmsql_new_order WHERE no_w_id = 1 AND no_d_id = 7 ORDER BY no_o_id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![Value::Int(29)], vec![Value::Int(30)]]
    );
}

#[test]
fn ordered_limited_dml_waits_on_locked_first_index_entry() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_lock_wait_ms(dir.path(), 5);
    let mut owner = short_lock_wait_session();
    let mut waiter = short_lock_wait_session();

    engine
        .execute_with_session(
            &mut owner,
            "CREATE TABLE bmsql_new_order (\
             no_w_id INT NOT NULL, no_d_id INT NOT NULL, no_o_id INT NOT NULL, carrier INT, \
             PRIMARY KEY (no_w_id, no_d_id, no_o_id))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut owner,
            "INSERT INTO bmsql_new_order VALUES (1, 7, 29, 0), (1, 7, 30, 0)",
        )
        .unwrap();

    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(
            &mut owner,
            "INSERT INTO bmsql_new_order VALUES (1, 7, 28, 0)",
        )
        .unwrap();

    let blocked = engine
        .execute_with_session(
            &mut waiter,
            "UPDATE bmsql_new_order SET carrier = 9 \
             WHERE no_w_id = 1 AND no_d_id = 7 ORDER BY no_o_id LIMIT 1",
        )
        .unwrap_err();
    assert_eq!(blocked.code(), 1205);

    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(
            &mut waiter,
            "SELECT no_o_id, carrier FROM bmsql_new_order \
             WHERE no_w_id = 1 AND no_d_id = 7 ORDER BY no_o_id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(29), Value::Int(0)],
            vec![Value::Int(30), Value::Int(0)]
        ]
    );

    engine.execute_with_session(&mut owner, "ROLLBACK").unwrap();
}

#[test]
fn ordered_limited_dml_fallback_waits_on_locked_first_sorted_row() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_lock_wait_ms(dir.path(), 5);
    let mut owner = short_lock_wait_session();
    let mut waiter = short_lock_wait_session();

    engine
        .execute_with_session(
            &mut owner,
            "CREATE TABLE queue (id INT PRIMARY KEY, grp INT, priority INT, handled INT)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut owner,
            "INSERT INTO queue VALUES (2, 7, 20, 0), (3, 7, 30, 0)",
        )
        .unwrap();

    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO queue VALUES (1, 7, 10, 0)")
        .unwrap();

    let blocked = engine
        .execute_with_session(
            &mut waiter,
            "UPDATE queue SET handled = 1 \
             WHERE grp = 7 ORDER BY priority LIMIT 1",
        )
        .unwrap_err();
    assert_eq!(blocked.code(), 1205);

    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(
            &mut waiter,
            "SELECT id, handled FROM queue WHERE grp = 7 ORDER BY priority",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(2), Value::Int(0)],
            vec![Value::Int(3), Value::Int(0)]
        ]
    );

    engine.execute_with_session(&mut owner, "ROLLBACK").unwrap();
}

/// TPC-C DELIVERY_BG reproducer: a 4-column UNIQUE index where the
/// third column is nullable and toggles from NULL → INT over many
/// updates. Mirrors `bmsql_oorder_idx1 (o_w_id, o_d_id, o_carrier_id, o_id)`
/// — DELIVERY_BG flips o_carrier_id from NULL to an INT, deleting the
/// (NULL) entry and inserting the (carrier) entry in the same lock. This
/// scenario sporadically tripped "index page is too large" under
/// concurrent UPDATE+INSERT load.
#[test]
fn oorder_idx1_delivery_bg_update_stress_does_not_overflow_index_page() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 2_000));
    engine
        .execute(
            "CREATE TABLE bmsql_oorder ( \
             o_w_id INT NOT NULL, o_d_id INT NOT NULL, o_id INT NOT NULL, \
             o_c_id INT, o_carrier_id INT, o_ol_cnt INT, o_all_local INT, \
             o_entry_d TIMESTAMP, \
             PRIMARY KEY (o_w_id, o_d_id, o_id), \
             UNIQUE KEY bmsql_oorder_idx1 (o_w_id, o_d_id, o_carrier_id, o_id))",
        )
        .unwrap();

    let mut session = SessionContext::default();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    // Reduced from 4w x 10d x 200o = 8000 rows to 2w x 5d x 100o = 1000 rows.
    // Still large enough to span multiple index pages and trigger NULL <-> INT
    // toggles across page boundaries (the regression case), at ~1/8 the cost.
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO bmsql_oorder \
             SELECT n DIV 500 + 1, MOD(n,500) DIV 100 + 1, MOD(n,100) + 1, \
                    1, NULL, 5, 1, NULL \
             FROM generate_series(0,999) AS gs(n)",
        )
        .unwrap();
    engine.execute_with_session(&mut session, "COMMIT").unwrap();

    let n_workers = 4;
    let mut handles = Vec::new();
    for w_seed in 0..n_workers {
        let engine = std::sync::Arc::clone(&engine);
        handles.push(std::thread::spawn(move || {
            let mut sess = SessionContext::default();
            for iter in 0..15 {
                let w = (w_seed % 2) + 1;
                let d = ((w_seed / 2 + iter) % 5) + 1;
                let o_id = (iter as i32) + (w_seed as i32) * 1000 + 1;
                let carrier = (iter % 10) + 1;
                let upd = format!(
                    "UPDATE bmsql_oorder SET o_carrier_id = {carrier} \
                     WHERE o_w_id = {w} AND o_d_id = {d} AND o_id = {oid}",
                    oid = (o_id % 100) + 1,
                );
                engine
                    .execute_with_session(&mut sess, &upd)
                    .unwrap_or_else(|err| panic!("delivery-style index update failed: {err:?}"));
                let ins = format!(
                    "INSERT INTO bmsql_oorder VALUES \
                     ({w}, {d}, {oid}, 1, NULL, 5, 1, NULL)",
                    oid = 1000 + o_id,
                );
                engine
                    .execute_with_session(&mut sess, &ins)
                    .unwrap_or_else(|err| panic!("concurrent oorder insert failed: {err:?}"));
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let QueryResponse::ResultSet(_) = engine
        .execute(
            "SELECT o_w_id, o_d_id, o_carrier_id, o_id FROM bmsql_oorder \
             WHERE o_w_id = 1 AND o_d_id = 1 ORDER BY o_carrier_id, o_id",
        )
        .unwrap()
    else {
        panic!("expected result set after stress run");
    };
}

// B1 — multi-table UPDATE must not lose a concurrently-committed write to a
// column it does not SET. The build-context snapshot is taken before the
// concurrent commit; the storage layer re-reads the row under the write lock
// (semi-consistent read). The write callback must patch only the SET columns
// onto that fresh row, leaving non-target columns at their latest committed
// values. Prior to the fix the callback wrote the whole snapshot back,
// clobbering `b` from 999 to its stale value 100.
#[test]
fn b1_multi_table_update_preserves_concurrent_write_to_untouched_column() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 2000));
    let mut owner = SessionContext::default();

    // `a` is variable-width VARCHAR so the SET forces the storage legacy
    // full-row rewrite path (the fixed-width column-delta fast path is
    // skipped when the changed column's encoded width changes). The legacy
    // path writes the whole row returned by the callback — exactly where a
    // snapshot write-back would clobber the untouched column `b`.
    engine
        .execute_with_session(
            &mut owner,
            "CREATE TABLE t1 (id INT PRIMARY KEY, a VARCHAR(32), b INT)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut owner, "CREATE TABLE t2 (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO t1 VALUES (1, 'X', 100)")
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO t2 VALUES (1)")
        .unwrap();

    // Owner holds an uncommitted change to the non-target column `b`.
    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "UPDATE t1 SET b = 999 WHERE id = 1")
        .unwrap();

    // Waiter runs a multi-table UPDATE that only SETs `a` (changing its
    // width); it blocks on the row lock owner holds, then re-reads after
    // owner commits.
    let waiter_engine = std::sync::Arc::clone(&engine);
    let waiter = std::thread::spawn(move || {
        let mut waiter = SessionContext::default();
        waiter_engine.execute_with_session(&mut waiter, "BEGIN")?;
        waiter_engine.execute_with_session(
            &mut waiter,
            "UPDATE t1, t2 SET t1.a = 'YYYY' WHERE t1.id = t2.id",
        )?;
        waiter_engine.execute_with_session(&mut waiter, "COMMIT")?;
        Ok::<(), QueryError>(())
    });

    std::thread::sleep(Duration::from_millis(30));
    engine.execute_with_session(&mut owner, "COMMIT").unwrap();
    waiter.join().unwrap().unwrap();

    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT a, b FROM t1 WHERE id = 1").unwrap()
    else {
        panic!("expected result set");
    };
    // a set to 'YYYY'; b preserved at owner's committed 999 (NOT clobbered
    // back to the snapshot's 100 by a full-row snapshot write-back).
    assert_eq!(
        result.rows,
        vec![vec![Value::String("YYYY".into()), Value::Int(999)]]
    );
}
