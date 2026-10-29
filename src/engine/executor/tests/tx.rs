use super::*;

#[test]
fn autocommit_set_variants_are_not_swallowed_by_jdbc_stub() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "SET SESSION autocommit = OFF")
        .unwrap();
    assert!(!session.autocommit);

    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT @@session.autocommit")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::String("0".into())]]);

    engine
        .execute_with_session(&mut session, "SET @@session.autocommit=TRUE")
        .unwrap();
    assert!(session.autocommit);
}

#[test]
fn user_variables_are_session_scoped_and_default_to_null() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut first = SessionContext::default();
    let mut second = SessionContext::default();

    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut first, "SELECT @a")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Null]]);

    engine
        .execute_with_session(&mut first, "SET @a = 1")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut first, "SELECT @a")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);

    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut second, "SELECT @a")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Null]]);
}

#[test]
fn user_variable_set_assignments_are_left_to_right() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "SET @a := 1, @b = @a + 1")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT @b")
        .unwrap()
    else {
        panic!("expected result set");
    };
    // `@a + 1` is integer arithmetic (MySQL keeps INT + INT integral).
    assert_eq!(result.rows, vec![vec![Value::Int(2)]]);
}

#[test]
fn set_autocommit_off_uses_lazy_transaction_and_idempotent_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET SESSION autocommit = OFF")
        .unwrap();

    assert!(!session.autocommit);
    assert!(!session.explicit_txn);
    assert!(session.current_txn.is_none());

    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    assert!(!session.autocommit);
    assert!(session.current_txn.is_none());

    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1)")
        .unwrap();
    assert!(session.current_txn.is_some());
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT id FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(result.rows.is_empty());
    assert!(!session.autocommit);
}

#[test]
fn session_rollback_tolerates_storage_txn_already_finished() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET SESSION autocommit = OFF")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1)")
        .unwrap();

    let txn_id = session.current_txn.unwrap();
    engine.db.rollback_txn(txn_id).unwrap();

    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    assert!(session.current_txn.is_none());
    assert!(!session.autocommit);

    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
}

#[test]
fn explicit_transaction_statement_error_rolls_back_partial_insert() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    let err = engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1, 'ok'), (1, 'dupe')")
        .unwrap_err();

    assert_eq!(err.code(), 1062);
    assert!(session.current_txn.is_some());

    let QueryResponse::ResultSet(empty) = engine
        .execute_with_session(&mut session, "SELECT id, v FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(empty.rows.is_empty());

    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (2, 'still active')")
        .unwrap();
    engine.execute_with_session(&mut session, "COMMIT").unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT id, v FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![Value::Int(2), Value::String("still active".into())]]
    );
}

#[test]
fn close_session_rolls_back_active_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1)")
        .unwrap();

    engine.close_session(&mut session).unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut SessionContext::default(), "SELECT id FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(result.rows.is_empty());
    assert!(session.current_txn.is_none());
}

#[test]
fn close_session_releases_row_locks() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_lock_wait_ms(dir.path(), 5);
    let mut owner = short_lock_wait_session();
    let mut waiter = short_lock_wait_session();

    engine
        .execute_with_session(&mut owner, "CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO t VALUES (1)")
        .unwrap();
    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "SELECT id FROM t WHERE id = 1 FOR UPDATE")
        .unwrap();
    engine.execute_with_session(&mut waiter, "BEGIN").unwrap();
    let blocked = engine
        .execute_with_session(&mut waiter, "SELECT id FROM t WHERE id = 1 FOR UPDATE")
        .unwrap_err();
    assert_eq!(blocked.code(), 1205);

    engine.close_session(&mut owner).unwrap();
    engine
        .execute_with_session(&mut waiter, "SELECT id FROM t WHERE id = 1 FOR UPDATE")
        .unwrap();
}

#[test]
fn information_schema_innodb_trx_and_lock_waits_reflect_live_locks() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 5_000));
    let mut owner = SessionContext::default();

    engine
        .execute_with_session(&mut owner, "CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO t VALUES (1)")
        .unwrap();
    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "SELECT id FROM t WHERE id = 1 FOR UPDATE")
        .unwrap();

    let waiter_engine = std::sync::Arc::clone(&engine);
    let waiter_thread = std::thread::spawn(move || {
        let mut waiter = SessionContext::default();
        waiter_engine
            .execute_with_session(&mut waiter, "BEGIN")
            .unwrap();
        waiter_engine
            .execute_with_session(&mut waiter, "SELECT id FROM t WHERE id = 1 FOR UPDATE")
            .unwrap();
        waiter_engine
            .execute_with_session(&mut waiter, "ROLLBACK")
            .unwrap();
    });

    let waits = {
        let mut rows = Vec::new();
        for _ in 0..200 {
            let QueryResponse::ResultSet(result) = engine
                .execute(
                    "SELECT REQUESTING_TRX_ID, REQUESTED_LOCK_ID, BLOCKING_TRX_ID, BLOCKING_LOCK_ID \
                     FROM information_schema.innodb_lock_waits",
                )
                .unwrap()
            else {
                panic!("expected innodb_lock_waits result set");
            };
            rows = result.rows;
            if rows.len() == 1 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        rows
    };

    let QueryResponse::ResultSet(trx) = engine
        .execute(
            "SELECT TRX_ID, TRX_STATE, TRX_REQUESTED_LOCK_ID \
             FROM information_schema.innodb_trx",
        )
        .unwrap()
    else {
        panic!("expected innodb_trx result set");
    };
    assert_eq!(
        trx.rows.len(),
        2,
        "owner and waiter transactions must be visible"
    );
    assert!(
        trx.rows
            .iter()
            .any(|row| row[1] == Value::String("LOCK WAIT".into()) && row[2] != Value::Null),
        "waiting transaction must expose requested lock id: {:?}",
        trx.rows
    );
    assert!(
        trx.rows
            .iter()
            .any(|row| row[1] == Value::String("RUNNING".into()) && row[2] == Value::Null),
        "lock owner should remain RUNNING without requested lock: {:?}",
        trx.rows
    );

    assert_eq!(waits.len(), 1);
    assert_ne!(waits[0][0], waits[0][2]);
    assert_ne!(waits[0][1], Value::Null);
    assert_ne!(waits[0][3], Value::Null);

    engine.execute_with_session(&mut owner, "ROLLBACK").unwrap();
    waiter_thread.join().unwrap();
}

#[test]
fn kill_query_interrupts_row_lock_wait_before_timeout() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 5_000));
    let registry = std::sync::Arc::clone(engine.registry());
    let owner_handle = registry.register(10, "root".into(), "localhost".into(), "test".into());
    let waiter_handle = registry.register(11, "root".into(), "localhost".into(), "test".into());
    let killer_handle = registry.register(12, "root".into(), "localhost".into(), "test".into());
    let mut owner = SessionContext::default();
    owner.connection_id = 10;
    owner.set_cancel_handle(Some(owner_handle.cancel_handle()));
    let mut waiter = SessionContext::default();
    waiter.connection_id = 11;
    waiter.set_cancel_handle(Some(waiter_handle.cancel_handle()));
    let mut killer = SessionContext::default();
    killer.connection_id = 12;
    killer.set_cancel_handle(Some(killer_handle.cancel_handle()));

    engine
        .execute_with_session(&mut owner, "CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO t VALUES (1)")
        .unwrap();
    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "SELECT id FROM t WHERE id = 1 FOR UPDATE")
        .unwrap();
    engine.execute_with_session(&mut waiter, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut waiter, "INSERT INTO t VALUES (2)")
        .unwrap();

    let blocked_engine = std::sync::Arc::clone(&engine);
    let waiter_thread = std::thread::spawn(move || {
        let started = Instant::now();
        let err = blocked_engine
            .execute_with_session(&mut waiter, "SELECT id FROM t WHERE id = 1 FOR UPDATE")
            .unwrap_err();
        (err, started.elapsed(), waiter)
    });

    std::thread::sleep(Duration::from_millis(50));
    engine
        .execute_with_session(&mut killer, "KILL QUERY 11")
        .unwrap();

    let (err, elapsed, mut waiter) = waiter_thread.join().unwrap();
    assert_eq!(err.code(), 1317);
    assert_eq!(err.sql_state(), "70100");
    assert!(elapsed < Duration::from_secs(1), "elapsed={elapsed:?}");
    assert!(waiter.current_txn.is_some());
    assert!(waiter.explicit_txn);
    waiter.refresh_cancel_generations();
    engine.execute_with_session(&mut waiter, "COMMIT").unwrap();
    let response = engine
        .execute("SELECT id FROM t WHERE id=2")
        .unwrap()
        .materialize()
        .unwrap();
    let QueryResponse::ResultSet(rows) = response else {
        panic!("expected rows")
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(2)]]);

    engine.close_session(&mut owner).unwrap();
    registry.unregister(10);
    registry.unregister(11);
    registry.unregister(12);
}

#[test]
fn timed_out_select_for_update_waiter_does_not_block_next_waiter() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_lock_wait_ms(dir.path(), 5);
    let mut owner = short_lock_wait_session();
    let mut timed_out = short_lock_wait_session();
    let mut next = short_lock_wait_session();

    engine
        .execute_with_session(&mut owner, "CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO t VALUES (1)")
        .unwrap();
    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "SELECT id FROM t WHERE id = 1 FOR UPDATE")
        .unwrap();

    engine
        .execute_with_session(&mut timed_out, "BEGIN")
        .unwrap();
    let blocked = engine
        .execute_with_session(&mut timed_out, "SELECT id FROM t WHERE id = 1 FOR UPDATE")
        .unwrap_err();
    assert_eq!(blocked.code(), 1205);

    engine.execute_with_session(&mut owner, "ROLLBACK").unwrap();
    engine.execute_with_session(&mut next, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut next, "SELECT id FROM t WHERE id = 1 FOR UPDATE")
        .unwrap();
}

#[test]
fn primary_key_update_waits_on_existing_select_for_update_lock() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_lock_wait_ms(dir.path(), 5);
    let mut owner = short_lock_wait_session();
    let mut updater = short_lock_wait_session();
    let mut reader = short_lock_wait_session();

    engine
        .execute_with_session(
            &mut owner,
            "CREATE TABLE district (w_id INT NOT NULL, d_id INT NOT NULL, next_o_id INT, \
             PRIMARY KEY (w_id, d_id))",
        )
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO district VALUES (1, 1, 3001)")
        .unwrap();

    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(
            &mut owner,
            "SELECT next_o_id FROM district WHERE w_id = 1 AND d_id = 1 FOR UPDATE",
        )
        .unwrap();

    engine.execute_with_session(&mut updater, "BEGIN").unwrap();
    let blocked = engine
        .execute_with_session(
            &mut updater,
            "UPDATE district SET next_o_id = next_o_id + 1 WHERE w_id = 1 AND d_id = 1",
        )
        .unwrap_err();
    assert_eq!(blocked.code(), 1205);

    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(
            &mut reader,
            "SELECT next_o_id FROM district WHERE w_id = 1 AND d_id = 1",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(3001)]]);

    engine
        .execute_with_session(&mut updater, "ROLLBACK")
        .unwrap();
    engine.execute_with_session(&mut owner, "ROLLBACK").unwrap();
}

#[test]
fn primary_key_delete_waits_on_existing_select_for_update_lock() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_lock_wait_ms(dir.path(), 5);
    let mut owner = short_lock_wait_session();
    let mut deleter = short_lock_wait_session();

    engine
        .execute_with_session(&mut owner, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO t VALUES (1, 10)")
        .unwrap();

    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "SELECT id FROM t WHERE id = 1 FOR UPDATE")
        .unwrap();

    engine.execute_with_session(&mut deleter, "BEGIN").unwrap();
    let blocked = engine
        .execute_with_session(&mut deleter, "DELETE FROM t WHERE id = 1")
        .unwrap_err();
    assert_eq!(blocked.code(), 1205);

    engine
        .execute_with_session(&mut deleter, "ROLLBACK")
        .unwrap();
    engine.execute_with_session(&mut owner, "ROLLBACK").unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut short_lock_wait_session(), "SELECT id FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn primary_key_delete_waits_inline_and_deletes_after_lock_release() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 5_000));
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
        let mut deleter = SessionContext::default();
        waiter_engine
            .execute_with_session(&mut deleter, "DELETE FROM t WHERE id = 1")
            .unwrap()
    });

    std::thread::sleep(Duration::from_millis(50));
    engine.execute_with_session(&mut owner, "COMMIT").unwrap();

    let QueryResponse::OkAffected { affected_rows } = waiter.join().unwrap() else {
        panic!("expected affected rows");
    };
    assert_eq!(affected_rows, 1);

    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut SessionContext::default(), "SELECT id FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(result.rows.is_empty());
}

#[test]
fn tpcc_order_line_multi_row_insert_rolls_back_reusable_keys() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE bmsql_order_line (\
             ol_w_id INT NOT NULL, ol_d_id INT NOT NULL, ol_o_id INT NOT NULL, \
             ol_number INT NOT NULL, ol_i_id INT NOT NULL, ol_delivery_d TIMESTAMP, \
             ol_amount DECIMAL(6,2), ol_supply_w_id INT, ol_quantity INT, ol_dist_info CHAR(24), \
             PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))",
        )
        .unwrap();

    let insert = "INSERT INTO bmsql_order_line \
        (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, \
         ol_quantity, ol_amount, ol_dist_info) VALUES \
        (3001, 1, 1, 1, 101, 1, 5, 12.50, 'dist-01'), \
        (3001, 1, 1, 2, 102, 1, 6, 13.50, 'dist-02'), \
        (3001, 1, 1, 3, 103, 1, 7, 14.50, 'dist-03')";

    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine.execute_with_session(&mut session, insert).unwrap();
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();

    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine.execute_with_session(&mut session, insert).unwrap();
    engine.execute_with_session(&mut session, "COMMIT").unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(
            &mut session,
            "SELECT ol_o_id, ol_d_id, ol_w_id, ol_number FROM bmsql_order_line \
             ORDER BY ol_number",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![
                Value::Int(3001),
                Value::Int(1),
                Value::Int(1),
                Value::Int(1)
            ],
            vec![
                Value::Int(3001),
                Value::Int(1),
                Value::Int(1),
                Value::Int(2)
            ],
            vec![
                Value::Int(3001),
                Value::Int(1),
                Value::Int(1),
                Value::Int(3)
            ],
        ]
    );
}

#[test]
fn tpcc_invalid_new_order_profile_rolls_back_after_batches() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE bmsql_district (\
             d_w_id INT NOT NULL, d_id INT NOT NULL, d_ytd DECIMAL(12,2), \
             d_tax DECIMAL(4,4), d_next_o_id INT, PRIMARY KEY (d_w_id, d_id))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE bmsql_stock (\
             s_w_id INT NOT NULL, s_i_id INT NOT NULL, s_quantity INT, s_ytd INT, \
             s_order_cnt INT, s_remote_cnt INT, s_data VARCHAR(50), s_dist_01 CHAR(24), \
             PRIMARY KEY (s_w_id, s_i_id))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE bmsql_oorder (\
             o_w_id INT NOT NULL, o_d_id INT NOT NULL, o_id INT NOT NULL, o_c_id INT, \
             o_carrier_id INT, o_ol_cnt INT, o_all_local INT, o_entry_d TIMESTAMP, \
             PRIMARY KEY (o_w_id, o_d_id, o_id))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE bmsql_new_order (\
             no_w_id INT NOT NULL, no_d_id INT NOT NULL, no_o_id INT NOT NULL, \
             PRIMARY KEY (no_w_id, no_d_id, no_o_id))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE bmsql_order_line (\
             ol_w_id INT NOT NULL, ol_d_id INT NOT NULL, ol_o_id INT NOT NULL, \
             ol_number INT NOT NULL, ol_i_id INT NOT NULL, ol_delivery_d TIMESTAMP, \
             ol_amount DECIMAL(6,2), ol_supply_w_id INT, ol_quantity INT, ol_dist_info CHAR(24), \
             PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))",
        )
        .unwrap();

    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO bmsql_district VALUES (1, 3, 0, 0.0100, 3001)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO bmsql_stock VALUES \
             (1, 101, 50, 0, 0, 0, 'data', 'dist-01'), \
             (1, 102, 50, 0, 0, 0, 'data', 'dist-01')",
        )
        .unwrap();

    engine
        .execute_with_session(&mut session, "SET SESSION autocommit = OFF")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "SELECT d_tax, d_next_o_id FROM bmsql_district \
             WHERE d_w_id = 1 AND d_id = 3 FOR UPDATE",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "UPDATE bmsql_district SET d_next_o_id = d_next_o_id + 1 \
             WHERE d_w_id = 1 AND d_id = 3",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO bmsql_oorder \
             (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) \
             VALUES (3001, 3, 1, 1634, '2026-05-07 00:00:00', 2, 1)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO bmsql_new_order (no_o_id, no_d_id, no_w_id) VALUES (3001, 3, 1)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "UPDATE bmsql_stock SET s_quantity = 45, s_ytd = s_ytd + 5, \
             s_order_cnt = s_order_cnt + 1, s_remote_cnt = s_remote_cnt + 0 \
             WHERE s_w_id = 1 AND s_i_id = 101",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "UPDATE bmsql_stock SET s_quantity = 44, s_ytd = s_ytd + 6, \
             s_order_cnt = s_order_cnt + 1, s_remote_cnt = s_remote_cnt + 0 \
             WHERE s_w_id = 1 AND s_i_id = 102",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO bmsql_order_line \
             (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, \
              ol_quantity, ol_amount, ol_dist_info) VALUES \
             (3001, 3, 1, 1, 101, 1, 5, 12.50, 'dist-01'), \
             (3001, 3, 1, 2, 102, 1, 6, 13.50, 'dist-01')",
        )
        .unwrap();

    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();

    let QueryResponse::ResultSet(district) = engine
        .execute_with_session(
            &mut session,
            "SELECT d_next_o_id FROM bmsql_district WHERE d_w_id = 1 AND d_id = 3",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(district.rows, vec![vec![Value::Int(3001)]]);

    let QueryResponse::ResultSet(orders) = engine
        .execute_with_session(&mut session, "SELECT COUNT(*) FROM bmsql_oorder")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(orders.rows, vec![vec![Value::Int(0)]]);

    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO bmsql_order_line \
             (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, \
              ol_quantity, ol_amount, ol_dist_info) VALUES \
             (3001, 3, 1, 1, 101, 1, 5, 12.50, 'dist-01'), \
             (3001, 3, 1, 2, 102, 1, 6, 13.50, 'dist-01')",
        )
        .unwrap();

    let QueryResponse::ResultSet(order_lines) = engine
        .execute_with_session(
            &mut session,
            "SELECT ol_number FROM bmsql_order_line ORDER BY ol_number",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        order_lines.rows,
        vec![vec![Value::Int(1)], vec![Value::Int(2)]]
    );
}

#[test]
fn autocommit_runtime_reuse_clears_transaction_local_wal_state() {
    use std::sync::{atomic::Ordering, Arc};
    let mut session = SessionContext::default();
    session.set_current_txn(Some(1));
    let first = session.ensure_txn_runtime(1).unwrap();
    first.wal_buffer_ready.store(true, Ordering::Release);
    let address = Arc::as_ptr(&first);
    drop(first);
    session.set_current_txn(None);
    session.set_current_txn(Some(2));
    let second = session.ensure_txn_runtime(2).unwrap();
    assert_eq!(
        Arc::as_ptr(&second),
        address,
        "private capacity should be reused"
    );
    assert_eq!(second.txn_id, 2);
    assert!(
        !second.wal_buffer_ready.load(Ordering::Acquire),
        "the preceding transaction's WAL state must not leak into a new one"
    );
}

#[test]
fn aliased_runtime_keeps_its_transaction_identity_when_owner_starts_another() {
    use std::sync::{atomic::Ordering, Arc};
    let mut session = SessionContext::default();
    session.set_current_txn(Some(1));
    let first = session.ensure_txn_runtime(1).unwrap();
    first.wal_buffer_ready.store(true, Ordering::Release);
    let mut continuation = session.clone();
    session.set_current_txn(None);
    session.set_current_txn(Some(2));
    let second = session.ensure_txn_runtime(2).unwrap();
    let retained = continuation.ensure_txn_runtime(1).unwrap();
    assert!(Arc::ptr_eq(&first, &retained));
    assert!(!Arc::ptr_eq(&first, &second));
    assert_eq!(first.txn_id, 1);
    assert!(first.wal_buffer_ready.load(Ordering::Acquire));
    assert_eq!(second.txn_id, 2);
    assert!(!second.wal_buffer_ready.load(Ordering::Acquire));
}

#[test]
fn cloned_session_transaction_runtime_serializes_statement_mutation() {
    use std::sync::{mpsc, Arc, Barrier};
    use std::time::Duration;

    let mut session = SessionContext::default();
    session.set_current_txn(Some(42));
    let runtime = session.ensure_txn_runtime(42).unwrap();
    let mut cloned = session.clone();
    let cloned_runtime = cloned.ensure_txn_runtime(42).unwrap();
    assert!(Arc::ptr_eq(&runtime, &cloned_runtime));

    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let (acquired_tx, acquired_rx) = mpsc::channel();
    std::thread::scope(|scope| {
        let first_runtime = Arc::clone(&runtime);
        let first_entered = Arc::clone(&entered);
        let first_release = Arc::clone(&release);
        scope.spawn(move || {
            let _guard = first_runtime.enter_statement();
            first_entered.wait();
            first_release.wait();
        });

        entered.wait();
        let second_runtime = Arc::clone(&cloned_runtime);
        scope.spawn(move || {
            let _guard = second_runtime.enter_statement();
            acquired_tx.send(()).unwrap();
        });

        assert!(acquired_rx.recv_timeout(Duration::from_millis(20)).is_err());
        release.wait();
        acquired_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    });
}

#[tokio::test(flavor = "current_thread")]
async fn row_lock_release_advances_async_retry_signal() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let db = engine.db_for_tests();
    let owner = db.begin_txn().unwrap().txn_id;
    let waiter = db.begin_txn().unwrap().txn_id;
    let target = crate::storage::NonblockingRowWaitTarget {
        txn_id: waiter,
        table_id: 7,
        row_id: crate::storage::RowId::new(3, 1),
        mode: crate::storage::LockMode::Exclusive,
    };
    db.acquire_row_lock_blocking(owner, target.table_id, target.row_id, target.mode)
        .unwrap();
    let mut signal = db.subscribe_row_lock_async_signal(target).unwrap();
    assert!(!db.async_row_lock_ready(target));
    db.notify_lock_waiters_for_rows([(target.table_id, crate::storage::RowId::new(3, 2))]);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(3), signal.changed())
            .await
            .is_err()
    );
    db.rollback_txn(owner).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), signal.changed())
        .await
        .unwrap()
        .unwrap();
    assert!(db.async_row_lock_ready(target));
    crate::storage::with_nonblocking_lock_waits(|| {
        db.acquire_row_lock_blocking(waiter, target.table_id, target.row_id, target.mode)
    })
    .unwrap();
    db.rollback_txn(waiter).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn async_row_lock_fifo_wakes_only_front_and_promotes_canceled_front() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let db = engine.db_for_tests();
    let owner = db.begin_txn().unwrap().txn_id;
    let first = db.begin_txn().unwrap().txn_id;
    let second = db.begin_txn().unwrap().txn_id;
    let target = crate::storage::NonblockingRowWaitTarget {
        txn_id: first,
        table_id: 7,
        row_id: crate::storage::RowId::new(3, 1),
        mode: crate::storage::LockMode::Exclusive,
    };
    let next = crate::storage::NonblockingRowWaitTarget {
        txn_id: second,
        ..target
    };
    db.acquire_row_lock_blocking(owner, target.table_id, target.row_id, target.mode)
        .unwrap();
    let mut first_signal = db.subscribe_row_lock_async_signal(target).unwrap();
    let mut second_signal = db.subscribe_row_lock_async_signal(next).unwrap();
    db.rollback_txn(owner).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), first_signal.changed())
        .await
        .unwrap()
        .unwrap();
    assert!(db.async_row_lock_ready(target));
    assert!(!db.async_row_lock_ready(next));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(3), second_signal.changed())
            .await
            .is_err()
    );
    drop(first_signal);
    db.rollback_txn(first).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), second_signal.changed())
        .await
        .unwrap()
        .unwrap();
    assert!(db.async_row_lock_ready(next));
    crate::storage::with_nonblocking_lock_waits(|| {
        db.acquire_row_lock_blocking(second, next.table_id, next.row_id, next.mode)
    })
    .unwrap();
    db.rollback_txn(second).unwrap();
}

#[test]
fn async_row_lock_release_before_registration_is_immediately_eligible() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let db = engine.db_for_tests();
    let owner = db.begin_txn().unwrap().txn_id;
    let waiter = db.begin_txn().unwrap().txn_id;
    let row = crate::storage::RowId::new(3, 1);
    db.acquire_row_lock_blocking(owner, 7, row, crate::storage::LockMode::Exclusive)
        .unwrap();
    let error = crate::storage::with_nonblocking_lock_waits(|| {
        db.acquire_row_lock_blocking(waiter, 7, row, crate::storage::LockMode::Exclusive)
    })
    .unwrap_err();
    assert!(crate::storage::is_lock_would_block_error(&error));
    let target = crate::storage::take_nonblocking_row_wait_target().unwrap();
    db.rollback_txn(owner).unwrap();
    let _signal = db.subscribe_row_lock_async_signal(target).unwrap();
    assert!(db.async_row_lock_ready(target));
    crate::storage::with_nonblocking_lock_waits(|| {
        db.acquire_row_lock_blocking(waiter, 7, row, target.mode)
    })
    .unwrap();
    db.rollback_txn(waiter).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn async_row_lock_upgrade_waits_without_eager_grant() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let db = engine.db_for_tests();
    let first = db.begin_txn().unwrap().txn_id;
    let second = db.begin_txn().unwrap().txn_id;
    let row = crate::storage::RowId::new(3, 1);
    for txn in [first, second] {
        db.acquire_row_lock_blocking(txn, 7, row, crate::storage::LockMode::Shared)
            .unwrap();
    }
    let target = crate::storage::NonblockingRowWaitTarget {
        txn_id: first,
        table_id: 7,
        row_id: row,
        mode: crate::storage::LockMode::Exclusive,
    };
    let mut signal = db.subscribe_row_lock_async_signal(target).unwrap();
    assert!(!db.async_row_lock_ready(target));
    db.rollback_txn(second).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), signal.changed())
        .await
        .unwrap()
        .unwrap();
    assert!(db.async_row_lock_ready(target));
    crate::storage::with_nonblocking_lock_waits(|| {
        db.acquire_row_lock_blocking(first, 7, row, target.mode)
    })
    .unwrap();
    db.rollback_txn(first).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn async_row_lock_deadlock_fallback_preserves_detection() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let db = engine.db_for_tests();
    let first = db.begin_txn().unwrap().txn_id;
    let second = db.begin_txn().unwrap().txn_id;
    let row1 = crate::storage::RowId::new(3, 1);
    let row2 = crate::storage::RowId::new(3, 2);
    let mode = crate::storage::LockMode::Exclusive;
    db.acquire_row_lock_blocking(first, 7, row1, mode).unwrap();
    db.acquire_row_lock_blocking(second, 7, row2, mode).unwrap();
    let target = crate::storage::NonblockingRowWaitTarget {
        txn_id: first,
        table_id: 7,
        row_id: row2,
        mode,
    };
    let mut signal = db.subscribe_row_lock_async_signal(target).unwrap();
    assert!(db
        .subscribe_row_lock_async_signal(crate::storage::NonblockingRowWaitTarget {
            txn_id: second,
            row_id: row1,
            ..target
        })
        .is_none());
    let error = db
        .acquire_row_lock_blocking(second, 7, row1, mode)
        .unwrap_err();
    assert!(error.is(crate::error::catalog::ER_LOCK_DEADLOCK));
    db.rollback_txn(second).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), signal.changed())
        .await
        .unwrap()
        .unwrap();
    assert!(db.async_row_lock_ready(target));
    db.rollback_txn(first).unwrap();
}

#[test]
fn statement_lock_wait_rollback_releases_new_row_locks() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_lock_wait_ms(dir.path(), 5);
    let mut owner = short_lock_wait_session();
    let mut waiter = short_lock_wait_session();
    let mut third = short_lock_wait_session();

    engine
        .execute_with_session(&mut owner, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();

    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "SELECT id FROM t WHERE id = 2 FOR UPDATE")
        .unwrap();

    engine.execute_with_session(&mut waiter, "BEGIN").unwrap();
    let blocked = engine
        .execute_with_session(&mut waiter, "UPDATE t SET v = v + 1")
        .unwrap_err();
    assert_eq!(blocked.code(), 1205);

    engine.execute_with_session(&mut third, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut third, "SELECT id FROM t WHERE id = 1 FOR UPDATE")
        .unwrap();
}

#[test]
fn lock_wait_timeout_preserves_transaction_and_prior_locks() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_lock_wait_ms(dir.path(), 5);
    let mut owner = short_lock_wait_session();
    let mut waiter = short_lock_wait_session();
    let mut third = short_lock_wait_session();

    engine
        .execute_with_session(&mut owner, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();

    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "SELECT id FROM t WHERE id = 2 FOR UPDATE")
        .unwrap();

    engine
        .execute_with_session(&mut waiter, "SET SESSION autocommit = OFF")
        .unwrap();
    engine
        .execute_with_session(&mut waiter, "UPDATE t SET v = 11 WHERE id = 1")
        .unwrap();
    let blocked = engine
        .execute_with_session(&mut waiter, "UPDATE t SET v = 21 WHERE id = 2")
        .unwrap_err();
    assert_eq!(blocked.code(), 1205);
    assert_eq!(blocked.message(), "lock wait timeout exceeded");
    assert!(waiter.current_txn.is_some());
    assert!(!waiter.autocommit);

    engine.execute_with_session(&mut third, "BEGIN").unwrap();
    assert_eq!(
        engine
            .execute_with_session(&mut third, "SELECT id FROM t WHERE id = 1 FOR UPDATE")
            .unwrap_err()
            .code(),
        1205
    );
    engine.execute_with_session(&mut third, "ROLLBACK").unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(
            &mut short_lock_wait_session(),
            "SELECT id, v FROM t ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)],
        ]
    );

    engine.execute_with_session(&mut waiter, "COMMIT").unwrap();
    engine.execute_with_session(&mut owner, "ROLLBACK").unwrap();
}

#[test]
fn select_for_update_lock_wait_rollback_releases_new_row_locks() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_lock_wait_ms(dir.path(), 5);
    let mut owner = short_lock_wait_session();
    let mut waiter = short_lock_wait_session();
    let mut third = short_lock_wait_session();

    engine
        .execute_with_session(&mut owner, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();

    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "SELECT id FROM t WHERE id = 2 FOR UPDATE")
        .unwrap();

    engine.execute_with_session(&mut waiter, "BEGIN").unwrap();
    let blocked = engine
        .execute_with_session(&mut waiter, "SELECT id FROM t ORDER BY id FOR UPDATE")
        .unwrap_err();
    assert_eq!(blocked.code(), 1205);

    engine.execute_with_session(&mut third, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut third, "SELECT id FROM t WHERE id = 1 FOR UPDATE")
        .unwrap();
}

#[test]
fn transaction_rollback_restores_insert_update_and_delete() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1, 'a')")
        .unwrap();

    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut session, "UPDATE t SET v = 'b' WHERE id = 1")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (2, 'x')")
        .unwrap();
    engine
        .execute_with_session(&mut session, "DELETE FROM t WHERE id = 1")
        .unwrap();
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT id, v FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![Value::Int(1), Value::String("a".into())]]
    );
}

#[test]
fn read_committed_uses_statement_snapshots() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut writer = SessionContext::default();
    let mut reader = SessionContext::default();

    engine
        .execute_with_session(&mut writer, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    engine
        .execute_with_session(&mut writer, "INSERT INTO t VALUES (1, 'a')")
        .unwrap();
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION transaction_isolation = 'READ-COMMITTED'",
        )
        .unwrap();
    engine.execute_with_session(&mut reader, "BEGIN").unwrap();

    let QueryResponse::ResultSet(before) = engine
        .execute_with_session(&mut reader, "SELECT v FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    engine
        .execute_with_session(&mut writer, "UPDATE t SET v = 'b' WHERE id = 1")
        .unwrap();
    let QueryResponse::ResultSet(after) = engine
        .execute_with_session(&mut reader, "SELECT v FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(before.rows, vec![vec![Value::String("a".into())]]);
    assert_eq!(after.rows, vec![vec![Value::String("b".into())]]);
}

#[test]
fn concurrent_transactions_do_not_dirty_read_uncommitted_changes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut writer = SessionContext::default();
    let mut reader = SessionContext::default();

    engine
        .execute_with_session(&mut writer, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    engine
        .execute_with_session(&mut writer, "INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        .unwrap();

    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut writer, "UPDATE t SET v = 'pending' WHERE id = 1")
        .unwrap();
    engine
        .execute_with_session(&mut writer, "DELETE FROM t WHERE id = 2")
        .unwrap();
    engine
        .execute_with_session(&mut writer, "INSERT INTO t VALUES (3, 'uncommitted')")
        .unwrap();

    let QueryResponse::ResultSet(during) = engine
        .execute_with_session(&mut reader, "SELECT id, v FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        during.rows,
        vec![
            vec![Value::Int(1), Value::String("a".into())],
            vec![Value::Int(2), Value::String("b".into())],
        ]
    );

    engine
        .execute_with_session(&mut writer, "ROLLBACK")
        .unwrap();
    let QueryResponse::ResultSet(after) = engine
        .execute_with_session(&mut reader, "SELECT id, v FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(after.rows, during.rows);
}

#[test]
fn read_uncommitted_reads_latest_uncommitted_versions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut writer = SessionContext::default();
    let mut reader = SessionContext::default();

    engine
        .execute_with_session(&mut writer, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    engine
        .execute_with_session(&mut writer, "INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        .unwrap();
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION transaction_isolation = 'READ-UNCOMMITTED'",
        )
        .unwrap();

    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut writer, "UPDATE t SET v = 'pending' WHERE id = 1")
        .unwrap();
    engine
        .execute_with_session(&mut writer, "DELETE FROM t WHERE id = 2")
        .unwrap();
    engine
        .execute_with_session(&mut writer, "INSERT INTO t VALUES (3, 'uncommitted')")
        .unwrap();

    let QueryResponse::ResultSet(during) = engine
        .execute_with_session(&mut reader, "SELECT id, v FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        during.rows,
        vec![
            vec![Value::Int(1), Value::String("pending".into())],
            vec![Value::Int(3), Value::String("uncommitted".into())],
        ]
    );

    engine
        .execute_with_session(&mut writer, "ROLLBACK")
        .unwrap();
    let QueryResponse::ResultSet(after) = engine
        .execute_with_session(&mut reader, "SELECT id, v FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        after.rows,
        vec![
            vec![Value::Int(1), Value::String("a".into())],
            vec![Value::Int(2), Value::String("b".into())],
        ]
    );
}

#[test]
fn read_uncommitted_dml_still_waits_on_uncommitted_writes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_lock_wait_ms(dir.path(), 5);
    let mut owner = short_lock_wait_session();
    let mut updater = short_lock_wait_session();

    engine
        .execute_with_session(&mut owner, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO t VALUES (1, 10)")
        .unwrap();
    engine
        .execute_with_session(
            &mut updater,
            "SET SESSION transaction_isolation = 'READ-UNCOMMITTED'",
        )
        .unwrap();

    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "UPDATE t SET v = 11 WHERE id = 1")
        .unwrap();
    let err = engine
        .execute_with_session(&mut updater, "UPDATE t SET v = 12 WHERE id = 1")
        .unwrap_err();
    assert_eq!(err.code(), 1205);
    engine.execute_with_session(&mut owner, "ROLLBACK").unwrap();
}

#[test]
fn read_committed_refreshes_snapshot_for_each_statement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut writer = SessionContext::default();
    let mut reader = SessionContext::default();

    engine
        .execute_with_session(&mut writer, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    engine
        .execute_with_session(&mut writer, "INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        .unwrap();
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION transaction_isolation = 'READ-COMMITTED'",
        )
        .unwrap();
    engine.execute_with_session(&mut reader, "BEGIN").unwrap();

    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut writer, "UPDATE t SET v = 'committed' WHERE id = 1")
        .unwrap();
    engine
        .execute_with_session(&mut writer, "DELETE FROM t WHERE id = 2")
        .unwrap();
    engine
        .execute_with_session(&mut writer, "INSERT INTO t VALUES (3, 'new')")
        .unwrap();

    let QueryResponse::ResultSet(before_commit) = engine
        .execute_with_session(&mut reader, "SELECT id, v FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        before_commit.rows,
        vec![
            vec![Value::Int(1), Value::String("a".into())],
            vec![Value::Int(2), Value::String("b".into())],
        ]
    );

    engine.execute_with_session(&mut writer, "COMMIT").unwrap();
    let QueryResponse::ResultSet(after_commit) = engine
        .execute_with_session(&mut reader, "SELECT id, v FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        after_commit.rows,
        vec![
            vec![Value::Int(1), Value::String("committed".into())],
            vec![Value::Int(3), Value::String("new".into())],
        ]
    );
    engine.execute_with_session(&mut reader, "COMMIT").unwrap();
}

#[test]
fn concurrent_updates_wait_and_apply_to_latest_committed_version() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 500));
    let mut owner = SessionContext::default();

    engine
        .execute_with_session(&mut owner, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO t VALUES (1, 0)")
        .unwrap();

    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "UPDATE t SET v = v + 1 WHERE id = 1")
        .unwrap();

    let waiter_engine = std::sync::Arc::clone(&engine);
    let waiter = std::thread::spawn(move || {
        let mut waiter = SessionContext::default();
        waiter_engine.execute_with_session(&mut waiter, "UPDATE t SET v = v + 1 WHERE id = 1")
    });

    std::thread::sleep(Duration::from_millis(20));
    let QueryResponse::ResultSet(before_commit) =
        engine.execute("SELECT v FROM t WHERE id = 1").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(before_commit.rows, vec![vec![Value::Int(0)]]);

    engine.execute_with_session(&mut owner, "COMMIT").unwrap();
    assert_eq!(
        waiter.join().unwrap().unwrap(),
        QueryResponse::OkAffected { affected_rows: 1 }
    );

    let QueryResponse::ResultSet(after) = engine.execute("SELECT v FROM t WHERE id = 1").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(after.rows, vec![vec![Value::Int(2)]]);
}

#[test]
fn concurrent_update_retries_from_fresh_snapshot_after_owner_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 500));
    let mut owner = SessionContext::default();

    engine
        .execute_with_session(&mut owner, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO t VALUES (1, 0)")
        .unwrap();

    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "UPDATE t SET v = 100 WHERE id = 1")
        .unwrap();

    let waiter_engine = std::sync::Arc::clone(&engine);
    let waiter = std::thread::spawn(move || {
        let mut waiter = SessionContext::default();
        waiter_engine.execute_with_session(&mut waiter, "UPDATE t SET v = v + 1 WHERE id = 1")
    });

    std::thread::sleep(Duration::from_millis(20));
    engine.execute_with_session(&mut owner, "ROLLBACK").unwrap();
    assert_eq!(
        waiter.join().unwrap().unwrap(),
        QueryResponse::OkAffected { affected_rows: 1 }
    );

    let QueryResponse::ResultSet(after) = engine.execute("SELECT v FROM t WHERE id = 1").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(after.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn committed_rolled_back_and_incomplete_transactions_recover_atomically() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        let mut committed = SessionContext::default();
        let mut rolled_back = SessionContext::default();
        let mut incomplete = SessionContext::default();

        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
            .unwrap();
        engine
            .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
            .unwrap();

        engine
            .execute_with_session(&mut committed, "BEGIN")
            .unwrap();
        engine
            .execute_with_session(&mut committed, "UPDATE t SET v = 11 WHERE id = 1")
            .unwrap();
        engine
            .execute_with_session(&mut committed, "DELETE FROM t WHERE id = 2")
            .unwrap();
        engine
            .execute_with_session(&mut committed, "INSERT INTO t VALUES (3, 30)")
            .unwrap();
        engine
            .execute_with_session(&mut committed, "COMMIT")
            .unwrap();

        engine
            .execute_with_session(&mut rolled_back, "BEGIN")
            .unwrap();
        engine
            .execute_with_session(&mut rolled_back, "UPDATE t SET v = 99 WHERE id = 1")
            .unwrap();
        engine
            .execute_with_session(&mut rolled_back, "DELETE FROM t WHERE id = 3")
            .unwrap();
        engine
            .execute_with_session(&mut rolled_back, "INSERT INTO t VALUES (4, 40)")
            .unwrap();
        engine
            .execute_with_session(&mut rolled_back, "ROLLBACK")
            .unwrap();

        engine
            .execute_with_session(&mut incomplete, "BEGIN")
            .unwrap();
        engine
            .execute_with_session(&mut incomplete, "UPDATE t SET v = 100 WHERE id = 1")
            .unwrap();
        engine
            .execute_with_session(&mut incomplete, "INSERT INTO t VALUES (5, 50)")
            .unwrap();
    }

    let reopened = open_strict_test_engine(dir.path());
    let QueryResponse::ResultSet(result) =
        reopened.execute("SELECT id, v FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::Int(11)],
            vec![Value::Int(3), Value::Int(30)],
        ]
    );
}

#[test]
fn mysql80_isolation_levels_and_next_transaction_scope_are_supported() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    assert_eq!(session.isolation, IsolationLevel::ReadCommitted);

    engine
        .execute_with_session(
            &mut session,
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
        )
        .unwrap();
    assert_eq!(session.isolation, IsolationLevel::ReadCommitted);
    assert_eq!(
        session.next_txn_isolation,
        Some(IsolationLevel::ReadCommitted)
    );

    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    assert_eq!(
        session.active_txn_isolation,
        Some(IsolationLevel::ReadCommitted)
    );
    engine
        .execute_with_session(
            &mut session,
            "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        )
        .unwrap();
    assert_eq!(session.isolation, IsolationLevel::Serializable);
    assert_eq!(
        session.active_txn_isolation,
        Some(IsolationLevel::ReadCommitted),
        "SET SESSION must not change the already-started transaction"
    );
    let err = engine
        .execute_with_session(
            &mut session,
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )
        .unwrap_err();
    assert_eq!(err.code(), 1568);
    engine.execute_with_session(&mut session, "COMMIT").unwrap();
    assert_eq!(session.active_txn_isolation, None);

    engine
        .execute_with_session(
            &mut session,
            "SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
        )
        .unwrap();
    assert_eq!(session.isolation, IsolationLevel::ReadUncommitted);
    engine
        .execute_with_session(
            &mut session,
            "SET @@session.transaction_isolation = 'READ-COMMITTED'",
        )
        .unwrap();
    assert_eq!(session.isolation, IsolationLevel::ReadCommitted);
    engine
        .execute_with_session(
            &mut session,
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )
        .unwrap();
    assert_eq!(
        session.next_txn_isolation,
        Some(IsolationLevel::RepeatableRead)
    );

    let original_global = crate::engine::api::global_transaction_isolation();
    engine
        .execute_with_session(
            &mut session,
            "SET GLOBAL TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
        )
        .unwrap();
    assert_eq!(
        crate::engine::api::global_transaction_isolation(),
        IsolationLevel::ReadUncommitted
    );
    assert_eq!(
        SessionContext::default().isolation,
        IsolationLevel::ReadUncommitted
    );
    crate::engine::api::set_global_transaction_isolation(original_global);
}

#[test]
fn serializable_transaction_plain_select_blocks_same_table_writes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_lock_wait_ms(dir.path(), 5);
    let mut reader = short_lock_wait_session();
    let mut writer = short_lock_wait_session();
    let mut other_writer = short_lock_wait_session();

    engine
        .execute_with_session(&mut reader, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut reader, "CREATE TABLE u (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut reader, "INSERT INTO t VALUES (1, 10)")
        .unwrap();
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION transaction_isolation = 'SERIALIZABLE'",
        )
        .unwrap();

    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
    let QueryResponse::ResultSet(rows) = engine
        .execute_with_session(&mut reader, "SELECT v FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(10)]]);

    let err = engine
        .execute_with_session(&mut writer, "INSERT INTO t VALUES (2, 20)")
        .unwrap_err();
    assert_eq!(err.code(), 1205);

    engine
        .execute_with_session(&mut other_writer, "INSERT INTO u VALUES (1)")
        .unwrap();
    engine.execute_with_session(&mut reader, "COMMIT").unwrap();
    engine
        .execute_with_session(&mut writer, "INSERT INTO t VALUES (2, 20)")
        .unwrap();
}

#[test]
fn serializable_autocommit_plain_select_does_not_hold_table_lock() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_lock_wait_ms(dir.path(), 5);
    let mut reader = SessionContext::default();
    let mut writer = SessionContext::default();

    engine
        .execute_with_session(&mut reader, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut reader, "INSERT INTO t VALUES (1, 10)")
        .unwrap();
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION transaction_isolation = 'SERIALIZABLE'",
        )
        .unwrap();
    engine
        .execute_with_session(&mut reader, "SELECT v FROM t WHERE id = 1")
        .unwrap();
    engine
        .execute_with_session(&mut writer, "INSERT INTO t VALUES (2, 20)")
        .unwrap();
}

#[test]
fn serializable_window_select_blocks_same_table_writes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_lock_wait_ms(dir.path(), 5);
    let mut reader = short_lock_wait_session();
    let mut writer = short_lock_wait_session();

    engine
        .execute_with_session(&mut reader, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut reader, "INSERT INTO t VALUES (1, 10)")
        .unwrap();
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION transaction_isolation = 'SERIALIZABLE'",
        )
        .unwrap();

    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
    engine
        .execute_with_session(
            &mut reader,
            "SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t",
        )
        .unwrap();

    let err = engine
        .execute_with_session(&mut writer, "INSERT INTO t VALUES (2, 20)")
        .unwrap_err();
    assert_eq!(err.code(), 1205);
    engine
        .execute_with_session(&mut reader, "ROLLBACK")
        .unwrap();
    engine
        .execute_with_session(&mut writer, "INSERT INTO t VALUES (2, 20)")
        .unwrap();
}

// ===== Milestone tests =====

/// M1.1 - START TRANSACTION READ ONLY rejects DML.
#[test]
fn read_only_transaction_rejects_dml() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "START TRANSACTION READ ONLY")
        .unwrap();
    assert!(session.read_only_txn);
    let err = engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (2)")
        .unwrap_err();
    assert_eq!(err.code(), 1792);
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut session, "SELECT id FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);
    engine.execute_with_session(&mut session, "COMMIT").unwrap();
}

/// M1.2 - Named SAVEPOINT and ROLLBACK TO SAVEPOINT.
#[test]
fn named_savepoint_rollback_to_named() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SAVEPOINT sp1")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (2)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (3)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "ROLLBACK TO SAVEPOINT sp1")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut session, "SELECT id FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);
    engine.execute_with_session(&mut session, "COMMIT").unwrap();
}

/// M1.2 - RELEASE SAVEPOINT removes the savepoint.
#[test]
fn release_savepoint_removes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut session, "SAVEPOINT sp1")
        .unwrap();
    engine
        .execute_with_session(&mut session, "RELEASE SAVEPOINT sp1")
        .unwrap();
    let err = engine
        .execute_with_session(&mut session, "ROLLBACK TO SAVEPOINT sp1")
        .unwrap_err();
    assert_eq!(err.code(), 1305);
    engine.execute_with_session(&mut session, "COMMIT").unwrap();
}

/// M1.4 - Atomic txn ID allocation (smoke test: starting many txns yields distinct IDs).
#[test]
fn atomic_txn_id_allocation_yields_unique_ids() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    for i in 0..50 {
        engine.execute_with_session(&mut session, "BEGIN").unwrap();
        engine
            .execute_with_session(&mut session, &format!("INSERT INTO t VALUES ({})", i))
            .unwrap();
        engine.execute_with_session(&mut session, "COMMIT").unwrap();
    }
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut session, "SELECT COUNT(*) FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(50));
}

// ----------------------------------------------------------------------
// Concurrency / lock / atomic-counter regression tests added after the
// session-dispatch and `active_write_txns_atomic` rework. These focus
// on bugs that recently shipped or are easy to ship: lost decrements,
// snapshot-view contention regressions, and SELECT-path lock-wait
// regressions.
// ----------------------------------------------------------------------

/// TPC-C-style regression: many concurrent transactions issuing
/// INSERT + UPDATE + SELECT + COMMIT against disjoint primary keys
/// must all succeed without spurious "lock wait timeout exceeded"
/// errors. Originally surfaced when prepared-EXECUTE ran inline on
/// tokio workers and blocked them in `Condvar::wait_timeout`,
/// producing a distributed deadlock; the storage layer must remain
/// well-behaved so the dispatcher fix is the only thing standing
/// between us and the regression.
#[test]
fn many_concurrent_disjoint_writers_complete_without_lock_timeouts() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 5_000));
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let mut bootstrap = SessionContext::default();
    engine
        .execute_with_session(&mut bootstrap, "BEGIN")
        .unwrap();
    for i in 0..16 {
        engine
            .execute_with_session(&mut bootstrap, &format!("INSERT INTO t VALUES ({}, 0)", i))
            .unwrap();
    }
    engine
        .execute_with_session(&mut bootstrap, "COMMIT")
        .unwrap();

    const THREADS: i64 = 8;
    const ITERS: i64 = 30;
    let mut handles = Vec::new();
    for tid in 0..THREADS {
        let engine = std::sync::Arc::clone(&engine);
        handles.push(std::thread::spawn(move || {
            let mut session = SessionContext::default();
            for i in 0..ITERS {
                let new_id = 1_000 + tid * ITERS + i;
                let upd_id = tid * 2;
                engine.execute_with_session(&mut session, "BEGIN").unwrap();
                engine
                    .execute_with_session(
                        &mut session,
                        &format!("INSERT INTO t VALUES ({}, {})", new_id, i),
                    )
                    .unwrap();
                engine
                    .execute_with_session(
                        &mut session,
                        &format!("UPDATE t SET v = v + 1 WHERE id = {}", upd_id),
                    )
                    .unwrap();
                engine
                    .execute_with_session(
                        &mut session,
                        &format!("SELECT v FROM t WHERE id = {}", upd_id),
                    )
                    .unwrap();
                engine.execute_with_session(&mut session, "COMMIT").unwrap();
            }
        }));
    }
    for h in handles {
        h.join().expect("worker thread panicked");
    }

    // After all workers finish, the active-writer atomic must be back
    // to zero. If commit/rollback paths failed to decrement on any
    // path the counter would drift, permanently pessimising SELECT
    // planning (residual filter cloning) for the rest of the process.
    assert!(!engine.db.has_active_write_txns());

    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(*) FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(16 + THREADS * ITERS));
}

/// A clean BEGIN / INSERT / COMMIT cycle must leave the writer atomic
/// at zero, and repeated cycles must not drift.
#[test]
fn commit_cycle_keeps_active_write_atomic_at_zero() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    assert!(!engine.db.has_active_write_txns());

    let mut session = SessionContext::default();
    for i in 0..30 {
        engine.execute_with_session(&mut session, "BEGIN").unwrap();
        engine
            .execute_with_session(&mut session, &format!("INSERT INTO t VALUES ({})", i))
            .unwrap();
        // Mid-txn the atomic must reflect the in-flight write.
        assert!(
            engine.db.has_active_write_txns(),
            "writer atomic missed INSERT in iteration {}",
            i
        );
        engine.execute_with_session(&mut session, "COMMIT").unwrap();
        // After commit it must drop back to zero.
        assert!(
            !engine.db.has_active_write_txns(),
            "writer atomic not cleared after COMMIT in iteration {}",
            i
        );
    }
}

/// Same invariant on the ROLLBACK path.
#[test]
fn rollback_cycle_keeps_active_write_atomic_at_zero() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();

    let mut session = SessionContext::default();
    for i in 0..30 {
        engine.execute_with_session(&mut session, "BEGIN").unwrap();
        engine
            .execute_with_session(&mut session, &format!("INSERT INTO t VALUES ({})", i))
            .unwrap();
        assert!(engine.db.has_active_write_txns());
        engine
            .execute_with_session(&mut session, "ROLLBACK")
            .unwrap();
        assert!(
            !engine.db.has_active_write_txns(),
            "writer atomic not cleared after ROLLBACK in iteration {}",
            i
        );
    }
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut session, "SELECT COUNT(*) FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(0));
}

/// Statement rollback that erases all of a txn's writes via savepoint
/// must take the writer atomic back to zero. Regression target: the
/// `restore_statement_savepoint` path must mirror the per-slot
/// undo_head transition into the Database-level atomic, otherwise the
/// counter stays elevated for the lifetime of the process.
#[test]
fn savepoint_rollback_clears_active_write_atomic_when_all_writes_undone() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();

    let mut session = SessionContext::default();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    // SAVEPOINT before any writes — restoring to it should put us
    // back to "no in-flight writes".
    engine
        .execute_with_session(&mut session, "SAVEPOINT sp0")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (2)")
        .unwrap();
    assert!(engine.db.has_active_write_txns());
    engine
        .execute_with_session(&mut session, "ROLLBACK TO SAVEPOINT sp0")
        .unwrap();
    // The transaction is still alive but no longer holds any writes.
    assert!(
        !engine.db.has_active_write_txns(),
        "writer atomic not cleared after ROLLBACK TO SAVEPOINT"
    );
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    assert!(!engine.db.has_active_write_txns());
}

/// Pure read-only transactions (no DML inside) must NEVER register
/// as a writer; otherwise the SELECT planning hot path keeps the
/// conservative residual-filter clone forever.
#[test]
fn read_only_transaction_does_not_register_as_writer() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1), (2), (3)")
        .unwrap();
    assert!(!engine.db.has_active_write_txns());

    let mut session = SessionContext::default();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    for _ in 0..10 {
        engine
            .execute_with_session(&mut session, "SELECT COUNT(*) FROM t")
            .unwrap();
        assert!(!engine.db.has_active_write_txns());
    }
    engine.execute_with_session(&mut session, "COMMIT").unwrap();
    assert!(!engine.db.has_active_write_txns());
}

/// A waiter that hits a `lock wait timeout` must leave the global
/// writer counter clean. Even though the waiter never wrote, the
/// commit/abort path that handles the failed waiter must not
/// double-decrement (would underflow saturate to 0 anyway, but the
/// invariant matters when later writers expect the count to be
/// accurate).
#[test]
fn lock_wait_timeout_does_not_leak_active_write_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 10));
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 0)").unwrap();

    let mut owner = short_lock_wait_session();
    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "UPDATE t SET v = 1 WHERE id = 1")
        .unwrap();
    assert!(engine.db.has_active_write_txns());

    let waiter_engine = std::sync::Arc::clone(&engine);
    let waiter = std::thread::spawn(move || {
        let mut session = short_lock_wait_session();
        waiter_engine
            .execute_with_session(&mut session, "BEGIN")
            .unwrap();
        let err = waiter_engine
            .execute_with_session(&mut session, "UPDATE t SET v = 2 WHERE id = 1")
            .unwrap_err();
        assert_eq!(err.code(), 1205);
        waiter_engine.close_session(&mut session).unwrap();
    });
    waiter.join().unwrap();

    // Owner still holds its write, so the counter must still be > 0.
    assert!(engine.db.has_active_write_txns());
    engine.execute_with_session(&mut owner, "COMMIT").unwrap();
    // After the only writer commits the counter must drop to 0,
    // including correct accounting of the timed-out waiter.
    assert!(!engine.db.has_active_write_txns());
}

/// Many concurrent readers on the same hot table must finish quickly.
/// This exercises the SELECT lock-safe inline path and the read-side
/// optimisations (heap_cache / index_cache RwLock, HeapFile read &self,
/// atomic SCN). A regression that re-introduced an exclusive lock on
/// the SELECT path would either timeout or run far past the deadline.
#[test]
fn many_concurrent_readers_complete_quickly() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 5_000));
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let mut session = SessionContext::default();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    for i in 0..200 {
        engine
            .execute_with_session(
                &mut session,
                &format!("INSERT INTO t VALUES ({}, {})", i, i * 2),
            )
            .unwrap();
    }
    engine.execute_with_session(&mut session, "COMMIT").unwrap();

    const THREADS: i64 = 8;
    const ITERS: i64 = 100;
    let started = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..THREADS {
        let engine = std::sync::Arc::clone(&engine);
        handles.push(std::thread::spawn(move || {
            let mut session = SessionContext::default();
            for i in 0..ITERS {
                let id = i % 200;
                let QueryResponse::ResultSet(rs) = engine
                    .execute_with_session(
                        &mut session,
                        &format!("SELECT v FROM t WHERE id = {}", id),
                    )
                    .unwrap()
                else {
                    panic!("expected result set");
                };
                assert_eq!(rs.rows, vec![vec![Value::Int(id * 2)]]);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    // 8 * 100 = 800 cached point lookups across threads; well under
    // five seconds on any developer machine. A real regression
    // (e.g. accidental row-lock acquisition by SELECT) would show up
    // as a 1205 error long before this deadline.
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "concurrent readers took {:?}",
        started.elapsed()
    );
}

/// READ COMMITTED: each statement inside an explicit read-only
/// transaction sees the latest committed state from concurrent
/// writers. Regression target: the lock-free `read_only_statement_view`
/// (atomic SCN load) must observe every concurrent commit's SCN bump.
#[test]
fn read_committed_explicit_txn_sees_concurrent_commits_per_statement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 5_000));
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 0)").unwrap();

    let mut reader = SessionContext::default();
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION transaction_isolation = 'READ-COMMITTED'",
        )
        .unwrap();
    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut reader, "SELECT v FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(0)]]);

    let writer_engine = std::sync::Arc::clone(&engine);
    let writer = std::thread::spawn(move || {
        writer_engine
            .execute("UPDATE t SET v = 7 WHERE id = 1")
            .unwrap();
    });
    writer.join().unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut reader, "SELECT v FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(7)]]);
    engine.execute_with_session(&mut reader, "COMMIT").unwrap();
}

#[test]
fn read_committed_read_only_txn_refreshes_snapshot_per_statement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 5_000));
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 0)").unwrap();

    let mut reader = SessionContext::default();
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION transaction_isolation = 'READ-COMMITTED'",
        )
        .unwrap();
    engine
        .execute_with_session(&mut reader, "START TRANSACTION READ ONLY")
        .unwrap();
    assert!(
        reader.txn_snapshot_scn.is_none(),
        "READ ONLY must not pin a snapshot at BEGIN"
    );
    let QueryResponse::ResultSet(first) = engine
        .execute_with_session(&mut reader, "SELECT v FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(first.rows, vec![vec![Value::Int(0)]]);

    engine.execute("UPDATE t SET v = 9 WHERE id = 1").unwrap();
    let QueryResponse::ResultSet(second) = engine
        .execute_with_session(&mut reader, "SELECT v FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(second.rows, vec![vec![Value::Int(9)]]);
    engine.execute_with_session(&mut reader, "COMMIT").unwrap();
}

#[test]
fn repeatable_read_read_only_txn_pins_on_first_consistent_read() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 5_000));
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 0)").unwrap();

    let mut reader = SessionContext::default();
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION transaction_isolation = 'REPEATABLE-READ'",
        )
        .unwrap();
    engine
        .execute_with_session(&mut reader, "START TRANSACTION READ ONLY")
        .unwrap();
    assert!(reader.txn_snapshot_scn.is_none());
    let QueryResponse::ResultSet(first) = engine
        .execute_with_session(&mut reader, "SELECT v FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(first.rows, vec![vec![Value::Int(0)]]);
    assert!(reader.txn_snapshot_guard.is_some());

    engine.execute("UPDATE t SET v = 9 WHERE id = 1").unwrap();
    let QueryResponse::ResultSet(second) = engine
        .execute_with_session(&mut reader, "SELECT v FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(second.rows, vec![vec![Value::Int(0)]]);
    engine.execute_with_session(&mut reader, "COMMIT").unwrap();
}

/// A plain SELECT (no FOR UPDATE) must never queue behind a row lock
/// held by an unrelated writer — it walks the MVCC undo chain to read
/// the prior committed version. A regression that caused SELECT to
/// take row locks would surface here within `lock_wait_ms` (kept very
/// short on purpose).
#[test]
fn read_path_does_not_block_on_unrelated_row_lock_holder() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 100));
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();

    let mut owner = SessionContext::default();
    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "UPDATE t SET v = 11 WHERE id = 1")
        .unwrap();

    let started = Instant::now();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT v FROM t WHERE id = 1").unwrap()
    else {
        panic!("expected result set");
    };
    // MVCC: pre-update committed value is visible.
    assert_eq!(rs.rows, vec![vec![Value::Int(10)]]);
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT v FROM t WHERE id = 2").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(20)]]);
    assert!(
        started.elapsed() < Duration::from_millis(50),
        "MVCC SELECT was apparently waiting on a row lock: {:?}",
        started.elapsed()
    );

    engine.execute_with_session(&mut owner, "COMMIT").unwrap();
}

/// Disjoint-key writers must run truly concurrently — neither a
/// shared catalog read lock, the txns RwLock, nor the heap/index
/// caches should serialize them.
#[test]
fn disjoint_key_writers_complete_in_parallel() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 2_000));
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let mut bootstrap = SessionContext::default();
    engine
        .execute_with_session(&mut bootstrap, "BEGIN")
        .unwrap();
    for i in 0..32 {
        engine
            .execute_with_session(&mut bootstrap, &format!("INSERT INTO t VALUES ({}, 0)", i))
            .unwrap();
    }
    engine
        .execute_with_session(&mut bootstrap, "COMMIT")
        .unwrap();

    const THREADS: i64 = 8;
    let mut handles = Vec::new();
    let started = Instant::now();
    for tid in 0..THREADS {
        let engine = std::sync::Arc::clone(&engine);
        handles.push(std::thread::spawn(move || {
            let mut session = SessionContext::default();
            engine.execute_with_session(&mut session, "BEGIN").unwrap();
            // Each thread updates its own dedicated row, then sleeps
            // briefly while still holding the row lock. With true
            // parallelism the test wall time is bounded by the sleep
            // duration; with serialization it would scale with THREADS.
            engine
                .execute_with_session(
                    &mut session,
                    &format!("UPDATE t SET v = {} WHERE id = {}", tid + 1, tid),
                )
                .unwrap();
            std::thread::sleep(Duration::from_millis(50));
            engine.execute_with_session(&mut session, "COMMIT").unwrap();
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let elapsed = started.elapsed();
    // Loose bound: 50ms per holder × THREADS would be 400ms if
    // serialized; even the slowest CI box should finish in under
    // 250ms when running them in parallel.
    assert!(
        elapsed < Duration::from_millis(300),
        "disjoint-key writers serialized: {:?}",
        elapsed
    );
    assert!(!engine.db.has_active_write_txns());
}

#[test]
fn repeatable_read_pins_snapshot_for_consistent_reads() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut setup = SessionContext::default();
    engine
        .execute_with_session(&mut setup, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut setup, "INSERT INTO t VALUES (1, 10)")
        .unwrap();

    let mut reader = SessionContext::default();
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION transaction_isolation = 'REPEATABLE-READ'",
        )
        .unwrap();
    engine
        .execute_with_session(&mut reader, "START TRANSACTION")
        .unwrap();
    // First consistent read pins the transaction read view at v=10.
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut reader, "SELECT v FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(10));

    // A concurrent autocommit session updates and commits.
    let mut writer = SessionContext::default();
    engine
        .execute_with_session(&mut writer, "UPDATE t SET v = 20 WHERE id = 1")
        .unwrap();

    // RR: the reader still observes the pinned snapshot, not the new commit.
    let QueryResponse::ResultSet(rs2) = engine
        .execute_with_session(&mut reader, "SELECT v FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs2.rows[0][0],
        Value::Int(10),
        "REPEATABLE READ must not see a concurrent commit within the transaction"
    );

    engine.execute_with_session(&mut reader, "COMMIT").unwrap();
    // After commit, a fresh read sees the latest value.
    let QueryResponse::ResultSet(rs3) = engine
        .execute_with_session(&mut reader, "SELECT v FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs3.rows[0][0], Value::Int(20));
}

#[test]
fn read_committed_sees_concurrent_commits_within_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut setup = SessionContext::default();
    engine
        .execute_with_session(&mut setup, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut setup, "INSERT INTO t VALUES (1, 10)")
        .unwrap();

    let mut reader = SessionContext::default();
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION transaction_isolation = 'READ-COMMITTED'",
        )
        .unwrap();
    engine
        .execute_with_session(&mut reader, "START TRANSACTION")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut reader, "SELECT v FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(10));

    let mut writer = SessionContext::default();
    engine
        .execute_with_session(&mut writer, "UPDATE t SET v = 20 WHERE id = 1")
        .unwrap();

    // RC: the second read sees the committed update (non-repeatable read) —
    // this is what proves the RR test above is actually pinning the snapshot.
    let QueryResponse::ResultSet(rs2) = engine
        .execute_with_session(&mut reader, "SELECT v FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs2.rows[0][0],
        Value::Int(20),
        "READ COMMITTED should observe the concurrent commit"
    );
    engine.execute_with_session(&mut reader, "COMMIT").unwrap();
}
#[test]
fn audit_repro_read_uncommitted_inside_cte_sees_dirty_version() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut writer = SessionContext::default();
    let mut reader = SessionContext::default();
    engine
        .execute_with_session(&mut writer, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    engine
        .execute_with_session(&mut writer, "INSERT INTO t VALUES (1, 'a')")
        .unwrap();
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION transaction_isolation = 'READ-UNCOMMITTED'",
        )
        .unwrap();
    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut writer, "UPDATE t SET v = 'pending' WHERE id = 1")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(
            &mut reader,
            "WITH c AS (SELECT id, v FROM t UNION ALL SELECT id, v FROM t) SELECT id, v FROM c ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::String("pending".into())],
            vec![Value::Int(1), Value::String("pending".into())],
        ]
    );
}

#[test]
fn serializable_two_readers_cannot_upgrade_s_to_ix_and_write_skew() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_lock_wait_ms(dir.path(), 5);
    let mut a = short_lock_wait_session();
    let mut b = short_lock_wait_session();
    engine
        .execute_with_session(&mut a, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut a, "INSERT INTO t VALUES (1, 1), (2, 1)")
        .unwrap();
    for session in [&mut a, &mut b] {
        engine
            .execute_with_session(
                session,
                "SET SESSION transaction_isolation = 'SERIALIZABLE'",
            )
            .unwrap();
        engine.execute_with_session(session, "BEGIN").unwrap();
        engine
            .execute_with_session(session, "SELECT SUM(v) FROM t")
            .unwrap();
    }
    let err = engine
        .execute_with_session(&mut a, "UPDATE t SET v = 0 WHERE id = 1")
        .unwrap_err();
    assert_eq!(err.code(), 1205);
    // A timeout preserves the transaction and its prior S locks. Explicitly
    // roll it back before the remaining reader can upgrade and commit.
    engine.execute_with_session(&mut a, "ROLLBACK").unwrap();
    engine
        .execute_with_session(&mut b, "UPDATE t SET v = 0 WHERE id = 2")
        .unwrap();
    engine.execute_with_session(&mut b, "COMMIT").unwrap();
    let QueryResponse::ResultSet(rows) = engine
        .execute_with_session(&mut a, "SELECT v FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected rows");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(1)], vec![Value::Int(0)]]);
}

#[test]
fn release_savepoint_preserves_later_sql_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_lock_wait_ms(dir.path(), 5);
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
        "INSERT INTO t VALUES (1, 10)",
        "BEGIN",
        "SAVEPOINT a",
        "UPDATE t SET v = 11",
        "SAVEPOINT b",
        "UPDATE t SET v = 12",
        "RELEASE SAVEPOINT a",
        "ROLLBACK TO SAVEPOINT b",
        "COMMIT",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    let QueryResponse::ResultSet(rows) = engine
        .execute_with_session(&mut session, "SELECT v FROM t")
        .unwrap()
    else {
        panic!("expected rows");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(11)]]);
}

#[test]
fn jepsen_unique_string_key_reuse_preserves_rc_and_rr_reads() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut writer = SessionContext::default();
    for sql in [
        "CREATE TABLE t (id INT PRIMARY KEY, k VARCHAR(40) NOT NULL UNIQUE, v TEXT)",
        "INSERT INTO t VALUES (1, 'old', 'original')",
    ] {
        engine.execute_with_session(&mut writer, sql).unwrap();
    }
    let mut reader = SessionContext::default();
    let mut frozen = SessionContext::default();
    for (session, isolation) in [
        (&mut reader, "READ-COMMITTED"),
        (&mut frozen, "REPEATABLE-READ"),
    ] {
        engine
            .execute_with_session(
                session,
                &format!("SET SESSION transaction_isolation = '{isolation}'"),
            )
            .unwrap();
        engine.execute_with_session(session, "BEGIN").unwrap();
    }
    let sql = "SELECT v FROM t WHERE k = ?";
    let parsed = engine.prepare_cached(&mut reader, sql, 1).unwrap();
    let mut memo = crate::engine::api::PreparedSelectMemoState::default();
    let mut read = |session: &mut SessionContext, key: &str| {
        let response = crate::engine::api::QueryEngine::execute_prepared_select(
            &engine,
            session,
            &mut memo,
            &parsed,
            sql,
            &[Value::String(key.into())],
        )
        .expect("must exercise the compiled unique point lookup")
        .unwrap();
        let QueryResponse::ResultSet(rows) = response.materialize().unwrap() else {
            panic!("expected result set");
        };
        rows.rows
    };
    for session in [&mut reader, &mut frozen] {
        assert_eq!(
            read(session, "old"),
            vec![vec![Value::String("original".into())]]
        );
    }
    for sql in [
        "BEGIN",
        "UPDATE t SET k = 'new', v = 'changed' WHERE id = 1",
        "INSERT INTO t VALUES (2, 'old', NULL)",
    ] {
        engine.execute_with_session(&mut writer, sql).unwrap();
    }
    for session in [&mut reader, &mut frozen] {
        assert_eq!(
            read(session, "old"),
            vec![vec![Value::String("original".into())]]
        );
        assert!(read(session, "new").is_empty());
    }
    assert_eq!(read(&mut writer, "old"), vec![vec![Value::Null]]);
    engine.execute_with_session(&mut writer, "COMMIT").unwrap();
    assert_eq!(read(&mut reader, "old"), vec![vec![Value::Null]]);
    assert_eq!(
        read(&mut reader, "new"),
        vec![vec![Value::String("changed".into())]]
    );
    assert_eq!(
        read(&mut frozen, "old"),
        vec![vec![Value::String("original".into())]]
    );
    assert!(read(&mut frozen, "new").is_empty());
    for session in [&mut reader, &mut frozen] {
        engine.execute_with_session(session, "COMMIT").unwrap();
    }
}
