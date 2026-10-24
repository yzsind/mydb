//! Differential + contention tests for the compile-once prepared-DML path.
//! Autocommit and explicit-transaction statements may use the memoized
//! executor when the shape is narrow enough to prove equivalent to the
//! legacy prepared path.

use super::*;
use crate::engine::api::{PreparedDmlState, QueryEngine};

fn fast(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    state: &mut PreparedDmlState,
    parsed: &std::sync::Arc<Statement>,
    sql: &str,
    params: &[Value],
) -> Option<Result<QueryResponse, QueryError>> {
    engine.execute_prepared_dml(session, state, parsed, sql, params)
}

fn legacy(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    parsed: &std::sync::Arc<Statement>,
    sql: &str,
    params: &[Value],
) -> Result<QueryResponse, QueryError> {
    crate::engine::params::with_params(params.to_vec(), || {
        engine
            .execute_prepared_statement(session, sql, (**parsed).clone())
            .expect("legacy prepared path ran")
    })
    .and_then(QueryResponse::materialize)
}

fn affected(response: QueryResponse) -> u64 {
    match response {
        QueryResponse::OkAffected { affected_rows } => affected_rows,
        other => panic!("expected OkAffected, got {other:?}"),
    }
}

fn result_rows(response: QueryResponse) -> Vec<Vec<Value>> {
    match response {
        QueryResponse::ResultSet(rs) => rs.rows,
        other => panic!("expected ResultSet, got {other:?}"),
    }
}

#[test]
fn prepared_transaction_control_uses_inline_fast_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE tx_ctl (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let mut session = SessionContext::default();
    let begin = engine.prepare_cached(&mut session, "BEGIN", 0).unwrap();
    let commit = engine.prepare_cached(&mut session, "COMMIT", 0).unwrap();
    let insert_sql = "INSERT INTO tx_ctl VALUES (?, ?)";
    let insert = engine.prepare_cached(&mut session, insert_sql, 2).unwrap();

    let before = engine.db_for_tests().storage_status();
    engine
        .execute_prepared_statement(&mut session, "BEGIN", (*begin).clone())
        .unwrap()
        .unwrap();
    assert!(session.explicit_txn);

    fast(
        &engine,
        &mut session,
        &mut PreparedDmlState::default(),
        &insert,
        insert_sql,
        &[Value::Int(1), Value::Int(10)],
    )
    .expect("plain INSERT should use prepared-DML fast path")
    .unwrap();

    engine
        .execute_prepared_statement(&mut session, "COMMIT", (*commit).clone())
        .unwrap()
        .unwrap();
    assert!(!session.explicit_txn);
    assert!(session.current_txn.is_none());
    assert_eq!(
        result_rows(engine.execute("SELECT v FROM tx_ctl WHERE id = 1").unwrap()),
        vec![vec![Value::Int(10)]]
    );

    let after = engine.db_for_tests().storage_status();
    assert_eq!(
        after.prepared_dml_compiles - before.prepared_dml_compiles,
        1,
        "BEGIN/COMMIT must not be counted as prepared-DML compiles"
    );
    assert_eq!(
        after.prepared_dml_reuses - before.prepared_dml_reuses,
        1,
        "only the INSERT should enter the prepared-DML execution path"
    );
}

#[test]
fn autocommit_prepared_dml_fast_path_binds_params_and_reuses_memos() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();

    let mut session = SessionContext::default();
    let insert_sql = "INSERT INTO t (id, v) VALUES (?, ?)";
    let insert = engine.prepare_cached(&mut session, insert_sql, 2).unwrap();
    let mut insert_state = PreparedDmlState::default();
    for params in [
        vec![Value::Int(1), Value::Int(10)],
        vec![Value::Int(2), Value::Int(20)],
    ] {
        let response = fast(
            &engine,
            &mut session,
            &mut insert_state,
            &insert,
            insert_sql,
            &params,
        )
        .expect("plain autocommit INSERT should use prepared-DML fast path")
        .unwrap();
        assert_eq!(affected(response), 1);
    }

    let update_sql = "UPDATE t SET v = ? WHERE id = ?";
    let update = engine.prepare_cached(&mut session, update_sql, 2).unwrap();
    let mut update_state = PreparedDmlState::default();
    for params in [
        vec![Value::Int(11), Value::Int(1)],
        vec![Value::Int(21), Value::Int(2)],
    ] {
        let response = fast(
            &engine,
            &mut session,
            &mut update_state,
            &update,
            update_sql,
            &params,
        )
        .expect("plain autocommit UPDATE should use prepared-DML fast path")
        .unwrap();
        assert_eq!(affected(response), 1);
    }
    let PreparedDmlState::Ready(update_plan) = &update_state else {
        panic!("prepared UPDATE did not compile a reusable plan");
    };
    let Some(crate::engine::api::PreparedPointWriteKind::Update(compiled_update)) =
        update_plan.point_write.as_ref().map(|point| &point.kind)
    else {
        panic!("prepared UPDATE did not compile a point-update kernel");
    };
    assert!(
        compiled_update.affected_indexes.is_empty(),
        "updating a non-key column must skip all index maintenance"
    );

    let delete_sql = "DELETE FROM t WHERE id = ?";
    let delete = engine.prepare_cached(&mut session, delete_sql, 1).unwrap();
    let mut delete_state = PreparedDmlState::default();
    let response = fast(
        &engine,
        &mut session,
        &mut delete_state,
        &delete,
        delete_sql,
        &[Value::Int(2)],
    )
    .expect("plain autocommit DELETE should use prepared-DML fast path")
    .unwrap();
    assert_eq!(affected(response), 1);
    let PreparedDmlState::Ready(delete_plan) = &delete_state else {
        panic!("prepared DELETE did not compile a reusable plan");
    };
    assert!(matches!(
        delete_plan.point_write.as_ref().map(|point| &point.kind),
        Some(crate::engine::api::PreparedPointWriteKind::Delete)
    ));

    let rows = result_rows(engine.execute("SELECT id, v FROM t ORDER BY id").unwrap());
    assert_eq!(rows, vec![vec![Value::Int(1), Value::Int(11)]]);

    let status = engine.db_for_tests().storage_status();
    assert_eq!(status.prepared_dml_compiles, 3);
    assert_eq!(status.prepared_dml_reuses, 5);
}

#[test]
fn prepared_point_update_maintains_only_compiled_affected_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, indexed_v INT, payload INT, UNIQUE KEY uk_v (indexed_v))")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10, 7)").unwrap();

    let mut session = SessionContext::default();
    let sql = "UPDATE t SET indexed_v = ?, payload = payload + 1 WHERE id = ?";
    let prepared = engine.prepare_cached(&mut session, sql, 2).unwrap();
    let mut state = PreparedDmlState::default();
    let response = fast(
        &engine,
        &mut session,
        &mut state,
        &prepared,
        sql,
        &[Value::Int(20), Value::Int(1)],
    )
    .expect("exact unique UPDATE should use prepared-DML fast path")
    .unwrap();
    assert_eq!(affected(response), 1);

    let PreparedDmlState::Ready(plan) = &state else {
        panic!("prepared UPDATE did not compile a reusable plan");
    };
    let Some(crate::engine::api::PreparedPointWriteKind::Update(update)) =
        plan.point_write.as_ref().map(|point| &point.kind)
    else {
        panic!("prepared UPDATE did not compile a point-update kernel");
    };
    assert_eq!(update.affected_indexes.as_ref(), &[1]);
    assert!(result_rows(
        engine
            .execute("SELECT id FROM t WHERE indexed_v = 10")
            .unwrap()
    )
    .is_empty());
    assert_eq!(
        result_rows(
            engine
                .execute("SELECT id, payload FROM t WHERE indexed_v = 20")
                .unwrap()
        ),
        vec![vec![Value::Int(1), Value::Int(8)]]
    );
}

#[test]
fn prepared_point_update_preserves_left_to_right_assignment_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10, 0)").unwrap();

    let mut session = SessionContext::default();
    let sql = "UPDATE t SET a = a + 1, b = a, a = a * 2 WHERE id = ?";
    let prepared = engine.prepare_cached(&mut session, sql, 1).unwrap();
    let mut state = PreparedDmlState::default();
    let response = fast(
        &engine,
        &mut session,
        &mut state,
        &prepared,
        sql,
        &[Value::Int(1)],
    )
    .expect("exact unique UPDATE should use prepared-DML fast path")
    .unwrap();

    assert_eq!(affected(response), 1);
    assert_eq!(
        result_rows(engine.execute("SELECT a, b FROM t WHERE id = 1").unwrap()),
        vec![vec![Value::Int(22), Value::Int(11)]]
    );
}

#[test]
fn prepared_point_update_mutates_wide_row_in_place_and_rolls_back_sparse_undo() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, a INT NOT NULL, b INT NOT NULL, payload VARCHAR(500))",
        )
        .unwrap();
    let payload = "x".repeat(300);
    engine
        .execute(&format!("INSERT INTO t VALUES (1, 10, 0, '{payload}')"))
        .unwrap();
    // Grow the post-insert short MVCC header once. The prepared executions
    // below are then eligible for the allocation-lean sparse patch path.
    engine.execute("UPDATE t SET a = 11 WHERE id = 1").unwrap();

    let mut session = SessionContext::default();
    let sql = "UPDATE t SET a = a + ?, b = a WHERE id = ?";
    let prepared = engine.prepare_cached(&mut session, sql, 2).unwrap();
    let mut state = PreparedDmlState::default();
    let hits_before = crate::storage::update_delta_hits();
    let sparse_hits_before = crate::storage::prepared_point_sparse_patch_hits_for_test();
    let response = fast(
        &engine,
        &mut session,
        &mut state,
        &prepared,
        sql,
        &[Value::Int(2), Value::Int(1)],
    )
    .expect("exact unique UPDATE should use prepared-DML fast path")
    .unwrap();
    assert_eq!(affected(response), 1);
    assert!(crate::storage::update_delta_hits() > hits_before);
    assert_eq!(
        crate::storage::prepared_point_sparse_patch_hits_for_test(),
        sparse_hits_before + 1,
        "the prepared UPDATE must use the sparse owned-row patch, not the full-row fallback"
    );
    assert_eq!(
        result_rows(
            engine
                .execute("SELECT a, b, payload FROM t WHERE id = 1")
                .unwrap()
        ),
        vec![vec![
            Value::Int(13),
            Value::Int(13),
            Value::String(payload.clone()),
        ]]
    );

    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    let response = fast(
        &engine,
        &mut session,
        &mut state,
        &prepared,
        sql,
        &[Value::Int(3), Value::Int(1)],
    )
    .expect("prepared point UPDATE should remain eligible in an explicit txn")
    .unwrap();
    assert_eq!(affected(response), 1);
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    assert_eq!(
        result_rows(engine.execute("SELECT a, b FROM t WHERE id = 1").unwrap()),
        vec![vec![Value::Int(13), Value::Int(13)]]
    );
}

#[test]
fn prepared_sparse_index_update_preserves_composite_keys_snapshot_and_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t (id INT PRIMARY KEY, k BIGINT NOT NULL, tag VARCHAR(40), payload VARCHAR(500), KEY ix (tag, k DESC), KEY ik (k))").unwrap();
    engine
        .execute(
            "INSERT INTO t VALUES (1, 10, 'Mixed Case', REPEAT('x', 300)), (2, 20, NULL, 'other')",
        )
        .unwrap();
    engine.execute("UPDATE t SET k = 11 WHERE id = 1").unwrap();

    let mut reader = SessionContext::default();
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )
        .unwrap();
    engine
        .execute_with_session(&mut reader, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .unwrap();
    let old = "SELECT id FROM t FORCE INDEX (ix) WHERE tag = 'Mixed Case' AND k = 11";
    assert_eq!(
        result_rows(engine.execute_with_session(&mut reader, old).unwrap()),
        vec![vec![Value::Int(1)]]
    );

    let mut session = SessionContext::default();
    let sql = "UPDATE t SET k = k + ? WHERE id = ?";
    let prepared = engine.prepare_cached(&mut session, sql, 2).unwrap();
    let mut state = PreparedDmlState::default();
    let before = crate::storage::prepared_point_sparse_patch_hits_for_test();
    assert_eq!(
        affected(
            fast(
                &engine,
                &mut session,
                &mut state,
                &prepared,
                sql,
                &[Value::Int(2), Value::Int(1)]
            )
            .unwrap()
            .unwrap()
        ),
        1
    );
    assert_eq!(
        crate::storage::prepared_point_sparse_patch_hits_for_test(),
        before + 1
    );
    assert_eq!(
        result_rows(
            engine
                .execute("SELECT id FROM t FORCE INDEX (ix) WHERE tag = 'Mixed Case' AND k = 13")
                .unwrap()
        ),
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(
        result_rows(
            engine
                .execute("SELECT id FROM t FORCE INDEX (ik) WHERE k = 13")
                .unwrap()
        ),
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(
        result_rows(engine.execute_with_session(&mut reader, old).unwrap()),
        vec![vec![Value::Int(1)]]
    );
    engine
        .execute_with_session(&mut reader, "ROLLBACK")
        .unwrap();

    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    fast(
        &engine,
        &mut session,
        &mut state,
        &prepared,
        sql,
        &[Value::Int(1000), Value::Int(1)],
    )
    .unwrap()
    .unwrap();
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    assert_eq!(
        result_rows(
            engine
                .execute("SELECT id FROM t FORCE INDEX (ik) WHERE k = 13")
                .unwrap()
        ),
        vec![vec![Value::Int(1)]]
    );
    assert!(result_rows(
        engine
            .execute("SELECT id FROM t FORCE INDEX (ik) WHERE k = 1013")
            .unwrap()
    )
    .is_empty());
    assert_eq!(
        result_rows(
            engine
                .execute("SELECT payload FROM t WHERE id = 1")
                .unwrap()
        ),
        vec![vec![Value::String("x".repeat(300))]]
    );

    let before = crate::storage::prepared_point_sparse_patch_hits_for_test();
    assert_eq!(
        affected(
            fast(
                &engine,
                &mut session,
                &mut state,
                &prepared,
                sql,
                &[Value::Int(0), Value::Int(1)]
            )
            .unwrap()
            .unwrap()
        ),
        0
    );
    assert_eq!(
        crate::storage::prepared_point_sparse_patch_hits_for_test(),
        before
    );
    fast(
        &engine,
        &mut session,
        &mut state,
        &prepared,
        sql,
        &[Value::Int(-7), Value::Int(2)],
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        result_rows(
            engine
                .execute("SELECT id FROM t FORCE INDEX (ix) WHERE tag IS NULL AND k = 13")
                .unwrap()
        ),
        vec![vec![Value::Int(2)]]
    );
    assert_eq!(
        result_rows(
            engine
                .execute("SELECT id FROM t FORCE INDEX (ik) WHERE k = 13 ORDER BY id")
                .unwrap()
        ),
        vec![vec![Value::Int(1)], vec![Value::Int(2)]]
    );
}

#[test]
fn prepared_point_sparse_update_allows_unaffected_foreign_key() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE parent (a INT, b INT, PRIMARY KEY (a, b))")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE child (id INT PRIMARY KEY, pa INT, pb INT, counter INT NOT NULL, \
             payload VARCHAR(500), CONSTRAINT child_parent_fk FOREIGN KEY (pa, pb) \
             REFERENCES parent (a, b))",
        )
        .unwrap();
    engine.execute("INSERT INTO parent VALUES (1, 2)").unwrap();
    let payload = "x".repeat(300);
    engine
        .execute(&format!(
            "INSERT INTO child VALUES (10, 1, 2, 10, '{payload}')"
        ))
        .unwrap();
    // Expand the compact INSERT header. The measured write below can then
    // prove that the unrelated FK no longer forces a full-row fallback.
    engine
        .execute("UPDATE child SET counter = 11 WHERE id = 10")
        .unwrap();

    let mut session = SessionContext::default();
    let counter_sql = "UPDATE child SET counter = counter + ? WHERE id = ?";
    let counter_stmt = engine.prepare_cached(&mut session, counter_sql, 2).unwrap();
    let mut counter_state = PreparedDmlState::default();
    let sparse_before = crate::storage::prepared_point_sparse_patch_hits_for_test();
    let response = fast(
        &engine,
        &mut session,
        &mut counter_state,
        &counter_stmt,
        counter_sql,
        &[Value::Int(1), Value::Int(10)],
    )
    .expect("unrelated counter write should use prepared DML")
    .unwrap();
    assert_eq!(affected(response), 1);
    assert_eq!(
        crate::storage::prepared_point_sparse_patch_hits_for_test(),
        sparse_before + 1,
        "an FK on unchanged columns must not disable the sparse patch"
    );

    // Touching one part of the composite child key must retain the full-row
    // FK check (which also needs the unchanged `pb` value) and reject the
    // nonexistent parent before any sparse heap patch.
    let fk_sql = "UPDATE child SET pa = ? WHERE id = ?";
    let fk_stmt = engine.prepare_cached(&mut session, fk_sql, 2).unwrap();
    let mut fk_state = PreparedDmlState::default();
    fast(
        &engine,
        &mut session,
        &mut fk_state,
        &fk_stmt,
        fk_sql,
        &[Value::Int(999), Value::Int(10)],
    )
    .expect("FK-key write should still use the prepared envelope")
    .expect_err("a missing composite parent must be rejected");
    assert_eq!(
        crate::storage::prepared_point_sparse_patch_hits_for_test(),
        sparse_before + 1,
        "an affected FK must fall back before the sparse patch"
    );
    assert_eq!(
        result_rows(
            engine
                .execute("SELECT pa, pb, counter, payload FROM child WHERE id = 10")
                .unwrap()
        ),
        vec![vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(12),
            Value::String(payload),
        ]]
    );
}

#[test]
fn prepared_point_scalar_update_does_not_read_unrelated_external_blob() {
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, counter INT NOT NULL, payload MEDIUMBLOB)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10, REPEAT('Z', 524288))")
        .unwrap();
    // Grow the compact INSERT header once; the measured prepared execution is
    // then eligible for the fixed-width sparse patch.
    engine
        .execute("UPDATE t SET counter = 11 WHERE id = 1")
        .unwrap();

    let mut session = SessionContext::default();
    let sql = "UPDATE t SET counter = counter + ? WHERE id = ?";
    let prepared = engine.prepare_cached(&mut session, sql, 2).unwrap();
    let mut state = PreparedDmlState::default();
    let reads_before = crate::storage::lob_full_materializations_for_test();
    let sparse_before = crate::storage::prepared_point_sparse_patch_hits_for_test();
    let response = fast(
        &engine,
        &mut session,
        &mut state,
        &prepared,
        sql,
        &[Value::Int(1), Value::Int(1)],
    )
    .expect("point update must use prepared DML")
    .unwrap();
    assert_eq!(affected(response), 1);
    assert_eq!(
        crate::storage::lob_full_materializations_for_test(),
        reads_before,
        "unreferenced external BLOB was materialized"
    );
    assert_eq!(
        crate::storage::prepared_point_sparse_patch_hits_for_test(),
        sparse_before + 1
    );
    assert_eq!(
        result_rows(
            engine
                .execute("SELECT counter FROM t WHERE id = 1")
                .unwrap()
        ),
        vec![vec![Value::Int(12)]]
    );
}

#[test]
fn prepared_point_sparse_update_enforces_check_and_skips_noop_patch() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, quantity INT NOT NULL, payload VARCHAR(500), \
             CONSTRAINT quantity_nonnegative CHECK (quantity >= 0))",
        )
        .unwrap();
    let payload = "x".repeat(300);
    engine
        .execute(&format!("INSERT INTO t VALUES (1, 5, '{payload}')"))
        .unwrap();
    // Expand the short insert header once so subsequent fixed-column writes
    // are eligible for the raw sparse patch.
    engine
        .execute("UPDATE t SET quantity = 6 WHERE id = 1")
        .unwrap();

    let mut session = SessionContext::default();
    let sql = "UPDATE t SET quantity = ? WHERE id = ?";
    let prepared = engine.prepare_cached(&mut session, sql, 2).unwrap();
    let mut state = PreparedDmlState::default();

    let sparse_hits_before = crate::storage::prepared_point_sparse_patch_hits_for_test();
    let err = fast(
        &engine,
        &mut session,
        &mut state,
        &prepared,
        sql,
        &[Value::Int(-1), Value::Int(1)],
    )
    .expect("exact unique UPDATE should use prepared-DML fast path")
    .expect_err("CHECK violation must not be bypassed by sparse patch");
    assert_eq!(
        err.code(),
        crate::error::catalog::ER_CHECK_CONSTRAINT_VIOLATED
            .code()
            .get()
    );
    assert_eq!(
        crate::storage::prepared_point_sparse_patch_hits_for_test(),
        sparse_hits_before,
        "constraint failure must happen before the heap patch"
    );
    assert_eq!(
        result_rows(
            engine
                .execute("SELECT quantity, payload FROM t WHERE id = 1")
                .unwrap()
        ),
        vec![vec![Value::Int(6), Value::String(payload)]]
    );

    let delta_hits_before = crate::storage::update_delta_hits();
    let response = fast(
        &engine,
        &mut session,
        &mut state,
        &prepared,
        sql,
        &[Value::Int(6), Value::Int(1)],
    )
    .expect("exact unique UPDATE should stay on prepared-DML fast path")
    .unwrap();
    assert_eq!(affected(response), 0);
    assert_eq!(crate::storage::update_delta_hits(), delta_hits_before);
    assert_eq!(
        crate::storage::prepared_point_sparse_patch_hits_for_test(),
        sparse_hits_before,
        "a semantic no-op must not append undo or write the heap"
    );
}

#[test]
fn prepared_tpcc_order_line_batch_sees_own_parent_order() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE bmsql_oorder (\
             o_w_id INT NOT NULL, o_d_id INT NOT NULL, o_id INT NOT NULL, \
             o_c_id INT, o_carrier_id INT, o_ol_cnt INT, o_all_local INT, \
             o_entry_d TIMESTAMP)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO bmsql_oorder VALUES \
             (2, 7, 3000, 90, NULL, 1, 1, '2026-07-28 13:00:00')",
        )
        .unwrap();
    engine
        .execute(
            "ALTER TABLE bmsql_oorder ADD CONSTRAINT bmsql_oorder_pkey \
             PRIMARY KEY (o_w_id, o_d_id, o_id)",
        )
        .unwrap();
    engine
        .execute(
            "CREATE UNIQUE INDEX bmsql_oorder_idx1 ON bmsql_oorder \
             (o_w_id, o_d_id, o_carrier_id, o_id)",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE bmsql_order_line (\
             ol_w_id INT NOT NULL, ol_d_id INT NOT NULL, ol_o_id INT NOT NULL, \
             ol_number INT NOT NULL, ol_i_id INT NOT NULL, \
             ol_supply_w_id INT, ol_quantity INT, ol_amount DECIMAL(6,2), \
             ol_dist_info CHAR(24))",
        )
        .unwrap();
    engine
        .execute(
            "ALTER TABLE bmsql_order_line ADD CONSTRAINT bmsql_order_line_pkey \
             PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)",
        )
        .unwrap();
    engine
        .execute(
            "ALTER TABLE bmsql_order_line ADD CONSTRAINT ol_order_fkey \
             FOREIGN KEY (ol_w_id, ol_d_id, ol_o_id) \
             REFERENCES bmsql_oorder (o_w_id, o_d_id, o_id)",
        )
        .unwrap();

    let parent_sql = "INSERT INTO bmsql_oorder (\
        o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) \
        VALUES (?, ?, ?, ?, ?, ?, ?)";
    let child_sql = "INSERT INTO bmsql_order_line (\
        ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, \
        ol_quantity, ol_amount, ol_dist_info) \
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    let mut session = SessionContext::default();
    let parent = engine.prepare_cached(&mut session, parent_sql, 7).unwrap();
    let child = engine.prepare_cached(&mut session, child_sql, 18).unwrap();
    let mut parent_state = PreparedDmlState::default();
    let mut child_state = PreparedDmlState::default();

    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    fast(
        &engine,
        &mut session,
        &mut parent_state,
        &parent,
        parent_sql,
        &[
            Value::Int(3001),
            Value::Int(7),
            Value::Int(2),
            Value::Int(91),
            Value::String("2026-07-28 14:00:12".to_string()),
            Value::Int(2),
            Value::Int(1),
        ],
    )
    .expect("parent INSERT should use prepared-DML")
    .unwrap();
    fast(
        &engine,
        &mut session,
        &mut child_state,
        &child,
        child_sql,
        &[
            Value::Int(3001),
            Value::Int(7),
            Value::Int(2),
            Value::Int(1),
            Value::Int(101),
            Value::Int(2),
            Value::Int(5),
            Value::Decimal("1.23".to_string()),
            Value::String("dist-a".to_string()),
            Value::Int(3001),
            Value::Int(7),
            Value::Int(2),
            Value::Int(2),
            Value::Int(102),
            Value::Int(2),
            Value::Int(5),
            Value::Decimal("4.56".to_string()),
            Value::String("dist-b".to_string()),
        ],
    )
    .expect("rewritten child batch should use prepared-DML")
    .expect("child batch must see the parent inserted by its transaction");
    engine.execute_with_session(&mut session, "COMMIT").unwrap();

    assert_eq!(
        result_rows(
            engine
                .execute("SELECT COUNT(*) FROM bmsql_order_line")
                .unwrap()
        ),
        vec![vec![Value::Int(2)]]
    );
}

#[test]
fn prepared_select_for_update_fast_path_waits_and_reads_latest_version() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 1_000));
    let mut owner = SessionContext::default();

    engine
        .execute_with_session(
            &mut owner,
            "CREATE TABLE counters (id INT PRIMARY KEY, next_id INT)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO counters VALUES (1, 3001)")
        .unwrap();

    let select_sql = "SELECT next_id FROM counters WHERE id = ? FOR UPDATE";
    let owner_select = engine.prepare_cached(&mut owner, select_sql, 1).unwrap();
    let mut owner_state = PreparedDmlState::default();
    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    assert_eq!(
        result_rows(
            fast(
                &engine,
                &mut owner,
                &mut owner_state,
                &owner_select,
                select_sql,
                &[Value::Int(1)],
            )
            .expect("locking read should use prepared-DML fast path")
            .unwrap()
        ),
        vec![vec![Value::Int(3001)]]
    );
    engine
        .execute_with_session(
            &mut owner,
            "UPDATE counters SET next_id = next_id + 1 WHERE id = 1",
        )
        .unwrap();

    let waiter_engine = std::sync::Arc::clone(&engine);
    let waiter = std::thread::spawn(move || {
        let mut waiter = SessionContext::default();
        let select = waiter_engine
            .prepare_cached(&mut waiter, select_sql, 1)
            .unwrap();
        let mut state = PreparedDmlState::default();
        waiter_engine.execute_with_session(&mut waiter, "BEGIN")?;
        let response = fast(
            &waiter_engine,
            &mut waiter,
            &mut state,
            &select,
            select_sql,
            &[Value::Int(1)],
        )
        .expect("waiter locking read should use prepared-DML fast path")?;
        waiter_engine.execute_with_session(&mut waiter, "ROLLBACK")?;
        Ok::<_, QueryError>(result_rows(response))
    });

    std::thread::sleep(Duration::from_millis(20));
    engine.execute_with_session(&mut owner, "COMMIT").unwrap();
    assert_eq!(
        waiter.join().unwrap().unwrap(),
        vec![vec![Value::Int(3002)]]
    );

    let status = engine.db_for_tests().storage_status();
    // The seed INSERT, two explicit locking reads and the default-on safe
    // integer-counter text UPDATE each compile and execute through prepared
    // DML.
    assert_eq!(status.prepared_dml_compiles, 4);
    assert_eq!(status.prepared_dml_reuses, 4);
}

#[test]
fn prepared_select_for_update_rebinds_composite_key_on_every_execute() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE district (\
             w_id INT NOT NULL, d_id INT NOT NULL, next_o_id INT NOT NULL, \
             PRIMARY KEY (w_id, d_id))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO district VALUES (1, 1, 3001), (1, 2, 4001), (2, 1, 5001)")
        .unwrap();

    let sql = "SELECT next_o_id FROM district WHERE w_id = ? AND d_id = ? FOR UPDATE";
    let mut session = SessionContext::default();
    let select = engine.prepare_cached(&mut session, sql, 2).unwrap();
    let mut state = PreparedDmlState::default();

    for (w_id, d_id, expected) in [(1, 1, 3001), (1, 2, 4001), (2, 1, 5001), (1, 1, 3001)] {
        engine.execute_with_session(&mut session, "BEGIN").unwrap();
        let response = fast(
            &engine,
            &mut session,
            &mut state,
            &select,
            sql,
            &[Value::Int(w_id), Value::Int(d_id)],
        )
        .expect("locking read should use prepared-DML fast path")
        .unwrap();
        assert_eq!(
            result_rows(response),
            vec![vec![Value::Int(expected)]],
            "prepared locking point read must bind the current composite key"
        );
        engine.execute_with_session(&mut session, "COMMIT").unwrap();
    }

    assert!(
        matches!(state, PreparedDmlState::Ready(_)),
        "the same prepared plan should be reused across parameter sets"
    );
}

#[test]
fn prepared_point_update_targets_replacement_after_generic_key_change() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE route_test (id INT PRIMARY KEY, v INT NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO route_test VALUES (1, 10)")
        .unwrap();
    let mut session = SessionContext::default();
    let select_sql = "SELECT v FROM route_test WHERE id = ? FOR UPDATE";
    let update_sql = "UPDATE route_test SET v = v + 1 WHERE id = ?";
    let select = engine.prepare_cached(&mut session, select_sql, 1).unwrap();
    let update = engine.prepare_cached(&mut session, update_sql, 1).unwrap();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    fast(
        &engine,
        &mut session,
        &mut PreparedDmlState::default(),
        &select,
        select_sql,
        &[Value::Int(1)],
    )
    .unwrap()
    .unwrap();
    engine
        .execute_with_session(&mut session, "UPDATE route_test SET id=id+2 WHERE v=10")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO route_test VALUES (1, 100)")
        .unwrap();
    assert_eq!(
        affected(
            fast(
                &engine,
                &mut session,
                &mut PreparedDmlState::default(),
                &update,
                update_sql,
                &[Value::Int(1)]
            )
            .unwrap()
            .unwrap()
        ),
        1
    );
    let actual = result_rows(
        engine
            .execute_with_session(&mut session, "SELECT id,v FROM route_test ORDER BY id")
            .unwrap(),
    );
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    assert_eq!(
        actual,
        vec![
            vec![Value::Int(1), Value::Int(101)],
            vec![Value::Int(3), Value::Int(10)]
        ]
    );
}

#[test]
fn prepared_point_update_tracks_delete_and_savepoint_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE route_rollback (id INT PRIMARY KEY, v INT NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO route_rollback VALUES (1,10)")
        .unwrap();
    let mut session = SessionContext::default();
    let select_sql = "SELECT v FROM route_rollback WHERE id = ? FOR UPDATE";
    let update_sql = "UPDATE route_rollback SET v=v+1 WHERE id=?";
    let select = engine.prepare_cached(&mut session, select_sql, 1).unwrap();
    let update = engine.prepare_cached(&mut session, update_sql, 1).unwrap();
    let cases: &[(&[&str], i64, &[&str], Vec<Vec<Value>>)] = &[
        (
            &[],
            1,
            &[
                "DELETE FROM route_rollback WHERE v=10",
                "INSERT INTO route_rollback VALUES (1,100)",
            ],
            vec![vec![Value::Int(1), Value::Int(101)]],
        ),
        (
            &["SAVEPOINT s", "UPDATE route_rollback SET id=2 WHERE v=10"],
            2,
            &["ROLLBACK TO s", "INSERT INTO route_rollback VALUES (2,200)"],
            vec![
                vec![Value::Int(1), Value::Int(10)],
                vec![Value::Int(2), Value::Int(201)],
            ],
        ),
        (
            &[
                "UPDATE route_rollback SET id=2 WHERE v=10",
                "UPDATE route_rollback SET id=1 WHERE v=10",
            ],
            1,
            &[
                "UPDATE route_rollback SET id=3 WHERE v=10",
                "INSERT INTO route_rollback VALUES (1,100)",
            ],
            vec![
                vec![Value::Int(1), Value::Int(101)],
                vec![Value::Int(3), Value::Int(10)],
            ],
        ),
    ];
    for (before, key, after, expected) in cases {
        engine.execute_with_session(&mut session, "BEGIN").unwrap();
        for sql in *before {
            engine.execute_with_session(&mut session, sql).unwrap();
        }
        fast(
            &engine,
            &mut session,
            &mut PreparedDmlState::default(),
            &select,
            select_sql,
            &[Value::Int(*key)],
        )
        .unwrap()
        .unwrap();
        for sql in *after {
            engine.execute_with_session(&mut session, sql).unwrap();
        }
        assert_eq!(
            affected(
                fast(
                    &engine,
                    &mut session,
                    &mut PreparedDmlState::default(),
                    &update,
                    update_sql,
                    &[Value::Int(*key)]
                )
                .unwrap()
                .unwrap()
            ),
            1
        );
        let actual = result_rows(
            engine
                .execute_with_session(&mut session, "SELECT id,v FROM route_rollback ORDER BY id")
                .unwrap(),
        );
        engine
            .execute_with_session(&mut session, "ROLLBACK")
            .unwrap();
        assert_eq!(&actual, expected);
    }
}

#[test]
fn prepared_first_update_uses_the_reserved_zero_undo_slot() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE first_patch (id INT PRIMARY KEY, n INT, note VARCHAR(500))")
        .unwrap();
    engine
        .execute("INSERT INTO first_patch VALUES (1,10,REPEAT('x',300))")
        .unwrap();
    let mut session = SessionContext::default();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    let sql = "UPDATE first_patch SET n=n+? WHERE id=?";
    let parsed = engine.prepare_cached(&mut session, sql, 2).unwrap();
    let before = crate::storage::prepared_point_sparse_patch_hits_for_test();
    fast(
        &engine,
        &mut session,
        &mut PreparedDmlState::default(),
        &parsed,
        sql,
        &[Value::Int(5), Value::Int(1)],
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        crate::storage::prepared_point_sparse_patch_hits_for_test() - before,
        1
    );
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    assert_eq!(
        result_rows(
            engine
                .execute("SELECT n, LENGTH(note) FROM first_patch")
                .unwrap()
        ),
        vec![vec![Value::Int(10), Value::Int(300)]]
    );
}

#[test]
fn prepared_select_for_update_rechecks_moved_key_from_locked_row() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 1_000));
    engine
        .execute("CREATE TABLE moving_pk (id INT PRIMARY KEY, v INT NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO moving_pk VALUES (1, 10)")
        .unwrap();

    let mut owner = SessionContext::default();
    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "UPDATE moving_pk SET id = 2 WHERE id = 1")
        .unwrap();

    let waiter_engine = std::sync::Arc::clone(&engine);
    let waiter = std::thread::spawn(move || {
        let sql = "SELECT v FROM moving_pk WHERE id = ? FOR UPDATE";
        let mut session = SessionContext::default();
        let parsed = waiter_engine.prepare_cached(&mut session, sql, 1)?;
        let mut state = PreparedDmlState::default();
        waiter_engine.execute_with_session(&mut session, "BEGIN")?;
        let response = fast(
            &waiter_engine,
            &mut session,
            &mut state,
            &parsed,
            sql,
            &[Value::Int(1)],
        )
        .expect("locking read should use prepared-DML fast path")?;
        waiter_engine.execute_with_session(&mut session, "ROLLBACK")?;
        Ok::<_, QueryError>(result_rows(response))
    });

    std::thread::sleep(Duration::from_millis(20));
    engine.execute_with_session(&mut owner, "COMMIT").unwrap();
    assert!(
        waiter.join().unwrap().unwrap().is_empty(),
        "row moved away from the probed key while the waiter acquired its lock"
    );
}

#[test]
fn prepared_select_for_update_uses_current_read_across_commit_frontier_gap() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Barrier};

    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(engine_with_lock_wait_ms(dir.path(), 1_000));
    engine
        .execute(
            "CREATE TABLE district (\
             w_id INT NOT NULL, d_id INT NOT NULL, next_o_id INT NOT NULL, \
             PRIMARY KEY (w_id, d_id))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO district VALUES (1, 1, 3001), (1, 2, 4001)")
        .unwrap();

    // Hold the first allocated commit SCN before publication. A later,
    // independent district update can publish and release its row lock while
    // the contiguous snapshot frontier remains behind that later commit.
    let allocated = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let first_commit = Arc::new(AtomicBool::new(true));
    {
        let allocated = Arc::clone(&allocated);
        let release = Arc::clone(&release);
        let first_commit = Arc::clone(&first_commit);
        crate::storage::commit_visibility_test_hook::install(Arc::new(move |_| {
            if first_commit.swap(false, Ordering::SeqCst) {
                allocated.wait();
                release.wait();
            }
        }));
    }

    let stalled_engine = Arc::clone(&engine);
    let stalled_commit = std::thread::spawn(move || -> Result<(), QueryError> {
        let mut session = SessionContext::default();
        stalled_engine.execute_with_session(&mut session, "BEGIN")?;
        stalled_engine.execute_with_session(
            &mut session,
            "UPDATE district SET next_o_id = next_o_id + 1 \
             WHERE w_id = 1 AND d_id = 1",
        )?;
        stalled_engine.execute_with_session(&mut session, "COMMIT")?;
        Ok(())
    });
    allocated.wait();

    let mut later_writer = SessionContext::default();
    engine
        .execute_with_session(&mut later_writer, "BEGIN")
        .unwrap();
    engine
        .execute_with_session(
            &mut later_writer,
            "UPDATE district SET next_o_id = next_o_id + 1 \
             WHERE w_id = 1 AND d_id = 2",
        )
        .unwrap();
    let later_txn = later_writer.current_txn.unwrap();
    let later_engine = Arc::clone(&engine);
    let later_commit =
        std::thread::spawn(move || later_engine.execute_with_session(&mut later_writer, "COMMIT"));
    // Publication still releases row locks before acknowledgement. Wait for
    // that stage, not COMMIT success: success now waits for the earlier gap.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while engine.db_for_tests().is_txn_active(later_txn) && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }

    let select_sql = "SELECT next_o_id FROM district WHERE w_id = ? AND d_id = ? FOR UPDATE";
    let mut reader = SessionContext::default();
    let prepared = engine.prepare_cached(&mut reader, select_sql, 2).unwrap();
    let mut state = PreparedDmlState::default();
    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
    let read_result = fast(
        &engine,
        &mut reader,
        &mut state,
        &prepared,
        select_sql,
        &[Value::Int(1), Value::Int(2)],
    )
    .expect("locking read should use prepared-DML fast path");
    let rollback_result = engine.execute_with_session(&mut reader, "ROLLBACK");

    // Always release and clear the process-global hook before asserting.
    release.wait();
    let stalled_result = stalled_commit.join().unwrap();
    let later_result = later_commit.join().unwrap();
    crate::storage::commit_visibility_test_hook::clear();

    rollback_result.unwrap();
    stalled_result.unwrap();
    later_result.unwrap();
    assert_eq!(
        result_rows(read_result.unwrap()),
        vec![vec![Value::Int(4002)]],
        "SELECT FOR UPDATE must see the latest committed row, not the lagging snapshot frontier"
    );
}

#[test]
fn explicit_transaction_prepared_dml_runs_through_fast_path_and_retries_delete_insert_replacement()
{
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 1_000));
    let mut owner = SessionContext::default();

    engine
        .execute_with_session(&mut owner, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO t VALUES (1, 10)")
        .unwrap();

    let delete_sql = "DELETE FROM t WHERE id = ?";
    let insert_sql = "INSERT INTO t (id, v) VALUES (?, ?)";
    let owner_delete = engine.prepare_cached(&mut owner, delete_sql, 1).unwrap();
    let owner_insert = engine.prepare_cached(&mut owner, insert_sql, 2).unwrap();

    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    let mut owner_fast_state = PreparedDmlState::default();
    // The fast path now covers explicit transactions; verify it
    // executes the DELETE in the current (still-uncommitted) txn and
    // reports affected=1 just like the legacy path used to here.
    assert_eq!(
        affected(
            fast(
                &engine,
                &mut owner,
                &mut owner_fast_state,
                &owner_delete,
                delete_sql,
                &[Value::Int(1)],
            )
            .expect("fast path runs for explicit txn")
            .unwrap()
        ),
        1
    );

    let waiter_engine = std::sync::Arc::clone(&engine);
    let waiter = std::thread::spawn(move || {
        let mut waiter = SessionContext::default();
        let delete = waiter_engine
            .prepare_cached(&mut waiter, delete_sql, 1)
            .unwrap();
        let insert = waiter_engine
            .prepare_cached(&mut waiter, insert_sql, 2)
            .unwrap();
        waiter_engine.execute_with_session(&mut waiter, "BEGIN")?;
        let mut fast_state = PreparedDmlState::default();
        // Waiter's DELETE must park on the owner's row lock until
        // the owner commits, then retry under the new committed
        // image (replaced by the owner with id=1, v=11) and succeed.
        let response = fast(
            &waiter_engine,
            &mut waiter,
            &mut fast_state,
            &delete,
            delete_sql,
            &[Value::Int(1)],
        )
        .expect("fast path runs for explicit txn")?;
        assert_eq!(
            affected(response),
            1,
            "waiter must retry after the owner commits and delete the replacement row"
        );
        let mut insert_state = PreparedDmlState::default();
        fast(
            &waiter_engine,
            &mut waiter,
            &mut insert_state,
            &insert,
            insert_sql,
            &[Value::Int(1), Value::Int(12)],
        )
        .expect("fast path runs for explicit txn")?;
        waiter_engine.execute_with_session(&mut waiter, "COMMIT")?;
        Ok::<(), QueryError>(())
    });

    std::thread::sleep(Duration::from_millis(20));
    let mut owner_insert_state = PreparedDmlState::default();
    assert_eq!(
        affected(
            fast(
                &engine,
                &mut owner,
                &mut owner_insert_state,
                &owner_insert,
                insert_sql,
                &[Value::Int(1), Value::Int(11)],
            )
            .expect("fast path runs for explicit txn")
            .unwrap()
        ),
        1
    );
    engine.execute_with_session(&mut owner, "COMMIT").unwrap();

    waiter.join().unwrap().unwrap();
    let rows = result_rows(engine.execute("SELECT id, v FROM t").unwrap());
    assert_eq!(rows, vec![vec![Value::Int(1), Value::Int(12)]]);
}

#[test]
fn concurrent_prepared_delete_insert_replacements_serialize_on_unique_key() {
    const WAITERS: usize = 8;

    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 2_000));
    let mut owner = SessionContext::default();
    engine
        .execute_with_session(&mut owner, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO t VALUES (1, 0)")
        .unwrap();

    let delete_sql = "DELETE FROM t WHERE id = ?";
    let insert_sql = "INSERT INTO t (id, v) VALUES (?, ?)";
    let owner_delete = engine.prepare_cached(&mut owner, delete_sql, 1).unwrap();
    let owner_insert = engine.prepare_cached(&mut owner, insert_sql, 2).unwrap();
    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    assert_eq!(
        affected(
            fast(
                &engine,
                &mut owner,
                &mut PreparedDmlState::default(),
                &owner_delete,
                delete_sql,
                &[Value::Int(1)],
            )
            .unwrap()
            .unwrap(),
        ),
        1
    );
    fast(
        &engine,
        &mut owner,
        &mut PreparedDmlState::default(),
        &owner_insert,
        insert_sql,
        &[Value::Int(1), Value::Int(1)],
    )
    .unwrap()
    .unwrap();

    let start = std::sync::Arc::new(std::sync::Barrier::new(WAITERS + 1));
    let mut waiters = Vec::with_capacity(WAITERS);
    for value in 2..=WAITERS + 1 {
        let engine = std::sync::Arc::clone(&engine);
        let start = std::sync::Arc::clone(&start);
        waiters.push(std::thread::spawn(move || -> Result<(), QueryError> {
            let mut session = SessionContext::default();
            let delete = engine.prepare_cached(&mut session, delete_sql, 1).unwrap();
            let insert = engine.prepare_cached(&mut session, insert_sql, 2).unwrap();
            engine.execute_with_session(&mut session, "BEGIN")?;
            start.wait();
            // Repeat on the same key so the test covers the handoff between
            // waiters, not only the initial owner's release. A check/probe
            // race can otherwise appear only once in many test processes.
            for round in 0..8 {
                if round != 0 {
                    engine.execute_with_session(&mut session, "BEGIN")?;
                }
                let deleted = fast(
                    &engine,
                    &mut session,
                    &mut PreparedDmlState::default(),
                    &delete,
                    delete_sql,
                    &[Value::Int(1)],
                )
                .expect("prepared DELETE fast path")?;
                assert_eq!(affected(deleted), 1);
                fast(
                    &engine,
                    &mut session,
                    &mut PreparedDmlState::default(),
                    &insert,
                    insert_sql,
                    &[Value::Int(1), Value::Int(value as i64)],
                )
                .expect("prepared INSERT fast path")?;
                engine.execute_with_session(&mut session, "COMMIT")?;
            }
            Ok(())
        }));
    }

    start.wait();
    std::thread::sleep(Duration::from_millis(20));
    engine.execute_with_session(&mut owner, "COMMIT").unwrap();
    for waiter in waiters {
        waiter.join().unwrap().unwrap();
    }
    assert_eq!(
        result_rows(
            engine
                .execute("SELECT COUNT(*) FROM t WHERE id = 1")
                .unwrap()
        ),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn nonblocking_prepared_dml_would_block_is_internal_and_log_suppressed() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut owner = SessionContext::default();
    let mut waiter = SessionContext::default();

    engine
        .execute_with_session(
            &mut owner,
            "CREATE TABLE retry_log (id INT PRIMARY KEY, v INT)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO retry_log VALUES (1, 10)")
        .unwrap();

    let update_sql = "UPDATE retry_log SET v = v + 1 WHERE id = ?";
    let update = engine.prepare_cached(&mut waiter, update_sql, 1).unwrap();

    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "UPDATE retry_log SET v = v + 1 WHERE id = 1")
        .unwrap();
    engine.execute_with_session(&mut waiter, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut waiter, "INSERT INTO retry_log VALUES (2, 20)")
        .unwrap();

    let waits_before = engine.db_for_tests().storage_status();
    let before_suppressed = crate::storage::inline_lock_would_block_log_suppressed_count();
    let mut state = PreparedDmlState::default();
    let result = crate::storage::with_nonblocking_lock_waits(|| {
        fast(
            &engine,
            &mut waiter,
            &mut state,
            &update,
            update_sql,
            &[Value::Int(1)],
        )
    })
    .expect("prepared UPDATE should use fast path");

    let err = result.expect_err("nonblocking lock wait must return internal marker");
    assert!(crate::storage::is_lock_would_block_error(&err));
    assert_eq!(
        crate::storage::inline_lock_would_block_log_suppressed_count(),
        before_suppressed + 1,
        "internal retry marker must not be recorded as a failed SQL"
    );
    let waits_after = engine.db_for_tests().storage_status();
    assert_eq!(
        waits_after.lock_waits, waits_before.lock_waits,
        "an inline scheduling probe must not publish a deadlock-graph wait"
    );
    assert_eq!(
        waits_after.active_lock_waits, waits_before.active_lock_waits,
        "an inline scheduling probe must not remain in the row FIFO"
    );
    assert!(
        waiter.current_txn.is_some(),
        "the internal scheduling marker must not abort the explicit transaction"
    );
    assert_eq!(
        result_rows(
            engine
                .execute_with_session(&mut waiter, "SELECT v FROM retry_log WHERE id = 2")
                .unwrap()
        ),
        vec![vec![Value::Int(20)]],
        "writes before the retried statement must survive its savepoint rollback"
    );

    engine.execute_with_session(&mut owner, "ROLLBACK").unwrap();
    let result = fast(
        &engine,
        &mut waiter,
        &mut state,
        &update,
        update_sql,
        &[Value::Int(1)],
    )
    .expect("prepared UPDATE retry should stay eligible")
    .unwrap();
    assert_eq!(affected(result), 1);
    engine.execute_with_session(&mut waiter, "COMMIT").unwrap();
    assert_eq!(
        result_rows(
            engine
                .execute("SELECT id, v FROM retry_log ORDER BY id")
                .unwrap()
        ),
        vec![
            vec![Value::Int(1), Value::Int(11)],
            vec![Value::Int(2), Value::Int(20)]
        ]
    );
}

#[test]
fn prepared_point_delete_releases_fresh_lock_when_waited_row_disappears() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 1_000));
    engine
        .execute("CREATE TABLE delete_race (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO delete_race VALUES (1, 10)")
        .unwrap();

    let mut owner = SessionContext::default();
    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "DELETE FROM delete_race WHERE id = 1")
        .unwrap();

    let waiter_engine = std::sync::Arc::clone(&engine);
    let waiter = std::thread::spawn(move || {
        let sql = "DELETE FROM delete_race WHERE id = ?";
        let mut session = SessionContext::default();
        let parsed = waiter_engine.prepare_cached(&mut session, sql, 1)?;
        let mut state = PreparedDmlState::default();
        waiter_engine.execute_with_session(&mut session, "BEGIN")?;
        let response = fast(
            &waiter_engine,
            &mut session,
            &mut state,
            &parsed,
            sql,
            &[Value::Int(1)],
        )
        .expect("exact DELETE should use the prepared point path")?;
        let affected = affected(response);
        let txn_id = session
            .current_txn
            .expect("explicit transaction remains active");
        let held = waiter_engine.db_for_tests().count_row_locks_held_by(txn_id);
        waiter_engine.execute_with_session(&mut session, "ROLLBACK")?;
        Ok::<_, QueryError>((affected, held))
    });

    std::thread::sleep(Duration::from_millis(20));
    engine.execute_with_session(&mut owner, "COMMIT").unwrap();
    let (affected, held) = waiter.join().unwrap().unwrap();
    assert_eq!(affected, 0, "the committed DELETE wins the race");
    assert_eq!(
        held, 0,
        "a no-op point DELETE must not convoy later waiters until transaction end"
    );
}

#[test]
fn prepared_delete_after_select_uses_current_read_for_committed_replacement() {
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

    let select_sql = "SELECT v FROM t WHERE id = ?";
    let delete_sql = "DELETE FROM t WHERE id = ?";
    let insert_sql = "INSERT INTO t (id, v) VALUES (?, ?)";
    let select = engine.prepare_cached(&mut reader, select_sql, 1).unwrap();
    let delete = engine.prepare_cached(&mut reader, delete_sql, 1).unwrap();
    let insert = engine.prepare_cached(&mut reader, insert_sql, 2).unwrap();

    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
    let rows =
        result_rows(legacy(&engine, &mut reader, &select, select_sql, &[Value::Int(1)]).unwrap());
    assert_eq!(rows, vec![vec![Value::Int(10)]]);

    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut writer, "DELETE FROM t WHERE id = 1")
        .unwrap();
    engine
        .execute_with_session(&mut writer, "INSERT INTO t VALUES (1, 11)")
        .unwrap();
    engine.execute_with_session(&mut writer, "COMMIT").unwrap();

    // The fast path runs under an explicit txn now; the DML must still
    // be a "current read" — DELETE has to find the writer's freshly
    // committed (1, 11) row, not the reader's snapshot (1, 10).
    let mut delete_state = PreparedDmlState::default();
    assert_eq!(
        affected(
            fast(
                &engine,
                &mut reader,
                &mut delete_state,
                &delete,
                delete_sql,
                &[Value::Int(1)],
            )
            .expect("fast path runs for explicit txn")
            .unwrap()
        ),
        1
    );
    let mut insert_state = PreparedDmlState::default();
    affected(
        fast(
            &engine,
            &mut reader,
            &mut insert_state,
            &insert,
            insert_sql,
            &[Value::Int(1), Value::Int(12)],
        )
        .expect("fast path runs for explicit txn")
        .unwrap(),
    );
    engine.execute_with_session(&mut reader, "COMMIT").unwrap();

    let rows = result_rows(engine.execute("SELECT id, v FROM t ORDER BY id").unwrap());
    assert_eq!(rows, vec![vec![Value::Int(1), Value::Int(12)]]);
}

// B7 — the prepared-DML fast path must apply MySQL no-op detection just like
// the text-dispatch path. A prepared UPDATE that sets a row to its current
// value reports Changed: 0 (affected_rows 0) and Rows matched: 1, and does
// NOT refresh ON UPDATE CURRENT_TIMESTAMP. Before the fix the fast path
// reported affected_rows = 1 (matched == changed), so the same statement
// returned different results depending on whether it hit the plan cache.
#[test]
fn b7_prepared_update_noop_reports_zero_changed_one_matched() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();

    let mut session = SessionContext::default();

    // Exact-write path (WHERE id = ?): set v to its current value.
    let sql = "UPDATE t SET v = ? WHERE id = ?";
    let update = engine.prepare_cached(&mut session, sql, 2).unwrap();
    let mut state = PreparedDmlState::default();
    let resp = fast(
        &engine,
        &mut session,
        &mut state,
        &update,
        sql,
        &[Value::Int(10), Value::Int(1)],
    )
    .expect("prepared UPDATE should use the fast path")
    .unwrap();
    assert_eq!(affected(resp), 0, "no-op UPDATE must report Changed: 0");
    assert_eq!(crate::engine::api::take_statement_matched_rows(), Some(1));

    // Real change through the same prepared statement still reports 1.
    let resp = fast(
        &engine,
        &mut session,
        &mut state,
        &update,
        sql,
        &[Value::Int(99), Value::Int(1)],
    )
    .expect("prepared UPDATE should use the fast path")
    .unwrap();
    assert_eq!(affected(resp), 1, "real change must report Changed: 1");
    assert_eq!(crate::engine::api::take_statement_matched_rows(), Some(1));

    // Batch path (WHERE v > ?): one row already at target, one changes.
    let bsql = "UPDATE t SET v = 20 WHERE v >= ?";
    let bupd = engine.prepare_cached(&mut session, bsql, 1).unwrap();
    let mut bstate = PreparedDmlState::default();
    // rows are now (1,99) and (2,20). v>=20 matches both; (2,20) is a no-op.
    let resp = fast(
        &engine,
        &mut session,
        &mut bstate,
        &bupd,
        bsql,
        &[Value::Int(20)],
    )
    .expect("prepared UPDATE should use the fast path")
    .unwrap();
    assert_eq!(
        affected(resp),
        1,
        "only the row that actually changed counts"
    );
    assert_eq!(
        crate::engine::api::take_statement_matched_rows(),
        Some(2),
        "both rows matched the WHERE"
    );
}

/// P4 — a prepared `INSERT … SELECT` now routes through the compile-once
/// prepared-DML plan (memoized privilege / MDL / qualification) instead of
/// deep-cloning the whole statement per execute on the legacy path. Verifies:
/// (1) it takes the fast path (memo becomes `Ready`), (2) the plan is reused
/// across executes with different bound params, (3) results are correct, and
/// (4) it matches a differential legacy run.
#[test]
fn prepared_insert_select_uses_compile_once_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE src (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO src VALUES (1,10),(2,20),(3,30),(4,40)")
        .unwrap();
    engine
        .execute("CREATE TABLE dst (id INT PRIMARY KEY, v INT)")
        .unwrap();

    let mut session = SessionContext::default();
    let sql = "INSERT INTO dst SELECT id, v FROM src WHERE v > ?";
    let stmt = engine.prepare_cached(&mut session, sql, 1).unwrap();
    let mut state = PreparedDmlState::default();

    // First execute: v > 25 → ids 3,4.
    let resp = fast(
        &engine,
        &mut session,
        &mut state,
        &stmt,
        sql,
        &[Value::Int(25)],
    )
    .expect("INSERT..SELECT must use the compile-once prepared path")
    .unwrap();
    assert_eq!(affected(resp), 2);
    assert!(
        matches!(state, PreparedDmlState::Ready(_)),
        "plan must be memoized after the first INSERT..SELECT execute"
    );

    // Second execute with a different param reuses the memo: v > 5 → all 4.
    engine
        .execute_with_session(&mut session, "DELETE FROM dst")
        .unwrap();
    let resp = fast(
        &engine,
        &mut session,
        &mut state,
        &stmt,
        sql,
        &[Value::Int(5)],
    )
    .expect("second execute reuses the memoized plan")
    .unwrap();
    assert_eq!(affected(resp), 4);

    let rows = result_rows(
        engine
            .execute_with_session(&mut session, "SELECT id, v FROM dst ORDER BY id")
            .unwrap()
            .materialize()
            .unwrap(),
    );
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0][0], Value::Int(1));
    assert_eq!(rows[3][1], Value::Int(40));

    // Differential: a legacy run of the same op yields the same affected count.
    engine
        .execute_with_session(&mut session, "DELETE FROM dst")
        .unwrap();
    let legacy_resp = legacy(&engine, &mut session, &stmt, sql, &[Value::Int(25)]);
    assert_eq!(affected(legacy_resp.unwrap()), 2, "legacy path parity");
}

/// P1 — a prepared INSERT…SELECT whose param-carrying source spans multiple
/// streaming chunks used to be forced eager (whole result materialised in
/// RAM) because `has_active_params()` short-circuited `select_requires_eager_response`.
/// The `StreamingParamSourceGuard` opt-in lets that source stream: the chunk
/// loader consumes rows incrementally and the scan worker re-installs the
/// snapshotted params. Verifies streaming produces the same result set and
/// affected-row count as the eager oracle (kill-switched via
/// `MYDB_STREAMING_PARAM_SOURCE_DISABLE`).
#[test]
fn prepared_insert_select_streams_param_source_across_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE src (id INT PRIMARY KEY, v INT)")
        .unwrap();
    // More than the cfg(test) 64-row chunk forces multiple streaming chunks.
    let n = 160i64;
    let mut bulk = String::from("INSERT INTO src VALUES ");
    for i in 0..n {
        if i > 0 {
            bulk.push(',');
        }
        bulk.push_str(&format!("({},{})", i, i * 10));
    }
    engine.execute(&bulk).unwrap();
    engine
        .execute("CREATE TABLE dst_stream (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE dst_eager (id INT PRIMARY KEY, v INT)")
        .unwrap();

    let sql = "INSERT INTO dst_stream SELECT id, v FROM src WHERE id >= ?";
    let mut session = SessionContext::default();
    let stmt = engine.prepare_cached(&mut session, sql, 1).unwrap();
    let mut state = PreparedDmlState::default();

    // Streaming path (default): param-carrying source streams.
    let resp = fast(
        &engine,
        &mut session,
        &mut state,
        &stmt,
        sql,
        &[Value::Int(10)],
    )
    .expect("param source must take the streaming prepared path")
    .unwrap();
    let stream_affected = affected(resp);
    assert_eq!(stream_affected, (n - 10) as u64);

    // Eager oracle: same statement with the kill switch on.
    std::env::set_var("MYDB_STREAMING_PARAM_SOURCE_DISABLE", "1");
    let sql_e = "INSERT INTO dst_eager SELECT id, v FROM src WHERE id >= ?";
    let stmt_e = engine.prepare_cached(&mut session, sql_e, 1).unwrap();
    let mut state_e = PreparedDmlState::default();
    let resp = fast(
        &engine,
        &mut session,
        &mut state_e,
        &stmt_e,
        sql_e,
        &[Value::Int(10)],
    )
    .expect("eager oracle path")
    .unwrap();
    let eager_affected = affected(resp);
    std::env::remove_var("MYDB_STREAMING_PARAM_SOURCE_DISABLE");

    assert_eq!(
        stream_affected, eager_affected,
        "streaming and eager must agree on affected rows"
    );

    // Row-level parity: same content in both destinations.
    let stream_rows = result_rows(
        engine
            .execute_with_session(&mut session, "SELECT id, v FROM dst_stream ORDER BY id")
            .unwrap()
            .materialize()
            .unwrap(),
    );
    let eager_rows = result_rows(
        engine
            .execute_with_session(&mut session, "SELECT id, v FROM dst_eager ORDER BY id")
            .unwrap()
            .materialize()
            .unwrap(),
    );
    assert_eq!(
        stream_rows, eager_rows,
        "streaming and eager must produce identical rows"
    );
    assert_eq!(stream_rows.len(), (n - 10) as usize);

    // Second execute reuses the compile-once memo with a different param and
    // still streams — proves the guard flag is scoped per invocation, not
    // sticky.
    engine
        .execute_with_session(&mut session, "DELETE FROM dst_stream")
        .unwrap();
    let resp = fast(
        &engine,
        &mut session,
        &mut state,
        &stmt,
        sql,
        &[Value::Int(120)],
    )
    .expect("second execute reuses memo and streams")
    .unwrap();
    assert_eq!(affected(resp), (n - 120) as u64);
}

#[test]
fn prepared_parameter_insert_uses_scalar_binding_and_preserves_typed_values() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE bind_fast (id INT PRIMARY KEY, n DECIMAL(12,3), s VARCHAR(40), b VARBINARY(8), d DATE)").unwrap();
    engine
        .execute("CREATE TABLE bind_oracle LIKE bind_fast")
        .unwrap();
    let mut session = SessionContext::default();
    let sql = "INSERT INTO bind_fast VALUES (?, ?, ?, ?, ?)";
    let oracle_sql = "INSERT INTO bind_oracle VALUES (?, ?, ?, ?, ?)";
    let prepared = engine.prepare_cached(&mut session, sql, 5).unwrap();
    let oracle = engine.prepare_cached(&mut session, oracle_sql, 5).unwrap();
    let mut state = PreparedDmlState::default();
    let before = crate::engine::executor::insert::SIMPLE_INSERT_VALUE_ROWS
        .load(std::sync::atomic::Ordering::Relaxed);
    for (id, number, text) in [(1, "-123.456", "中文"), (2, "0.125", "second")] {
        let params = vec![
            Value::Int(id),
            Value::String(number.into()),
            Value::String(text.into()),
            Value::Bytes(vec![0, 255, 1]),
            Value::String("2026-09-06".into()),
        ];
        assert_eq!(
            affected(
                fast(&engine, &mut session, &mut state, &prepared, sql, &params)
                    .unwrap()
                    .unwrap()
            ),
            1
        );
        legacy(&engine, &mut session, &oracle, oracle_sql, &params).unwrap();
    }
    assert!(
        crate::engine::executor::insert::SIMPLE_INSERT_VALUE_ROWS
            .load(std::sync::atomic::Ordering::Relaxed)
            >= before + 2
    );
    assert_eq!(
        result_rows(
            engine
                .execute("SELECT * FROM bind_fast ORDER BY id")
                .unwrap()
        ),
        result_rows(
            engine
                .execute("SELECT * FROM bind_oracle ORDER BY id")
                .unwrap()
        )
    );
}

#[test]
fn prepared_point_decode_recipe_is_shared_and_rebinds_after_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE decode_recipe (id INT PRIMARY KEY, n DECIMAL(12,3), v BIGINT, untouched VARCHAR(200))").unwrap();
    engine
        .execute("INSERT INTO decode_recipe VALUES (1, 1.125, 10, 'keep')")
        .unwrap();
    let mut session = SessionContext::default();
    let sql = "UPDATE decode_recipe SET n = n + ?, v = ? WHERE id = ?";
    let prepared = engine.prepare_cached(&mut session, sql, 3).unwrap();
    let mut state = PreparedDmlState::default();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    fast(
        &engine,
        &mut session,
        &mut state,
        &prepared,
        sql,
        &[Value::String("0.125".into()), Value::Int(20), Value::Int(1)],
    )
    .unwrap()
    .unwrap();
    let PreparedDmlState::Ready(plan) = &state else {
        panic!("missing plan")
    };
    let point = plan.point_write.as_ref().unwrap();
    let crate::engine::api::PreparedPointWriteKind::Update(update) = &point.kind else {
        panic!("not update")
    };
    let recipe = Arc::clone(
        update
            .decode_recipe
            .as_ref()
            .expect("compiled decode recipe"),
    );
    assert!(recipe.1.bytes() >= std::mem::size_of_val(&recipe.0));
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    fast(
        &engine,
        &mut session,
        &mut state,
        &prepared,
        sql,
        &[Value::String("0.250".into()), Value::Int(30), Value::Int(1)],
    )
    .unwrap()
    .unwrap();
    let PreparedDmlState::Ready(plan) = &state else {
        panic!("missing plan")
    };
    let crate::engine::api::PreparedPointWriteKind::Update(update) =
        &plan.point_write.as_ref().unwrap().kind
    else {
        panic!("not update")
    };
    assert!(Arc::ptr_eq(&recipe, update.decode_recipe.as_ref().unwrap()));
    assert_eq!(
        result_rows(
            engine
                .execute("SELECT n = 1.375, v, untouched FROM decode_recipe")
                .unwrap()
        ),
        vec![vec![
            Value::Int(1),
            Value::Int(30),
            Value::String("keep".into())
        ]]
    );
}

#[test]
fn prepared_select_reuses_output_capability_and_checks_new_parameters() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE preflight_t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO preflight_t VALUES (1,10),(2,11),(3,12),(4,13)")
        .unwrap();
    let mut session = SessionContext::default();
    let sql = "SELECT COUNT(*) FROM preflight_t a JOIN preflight_t b ON a.v = b.v WHERE a.id > ?";
    let parsed = engine.prepare_cached(&mut session, sql, 1).unwrap();
    let mut state = crate::engine::api::PreparedSelectMemoState::default();
    let before = crate::engine::executor::select::vec_bridge::NATIVE_OUTPUT_PREFLIGHT_REUSES
        .load(std::sync::atomic::Ordering::Relaxed);
    for bound in [0, 2] {
        let params = [Value::Int(bound)];
        let actual = engine
            .execute_prepared_select(&mut session, &mut state, &parsed, sql, &params)
            .expect("prepared SELECT accepted")
            .unwrap()
            .materialize()
            .unwrap();
        let expected = legacy(&engine, &mut session, &parsed, sql, &params).unwrap();
        assert_eq!(result_rows(actual), result_rows(expected));
    }
    assert!(
        crate::engine::executor::select::vec_bridge::NATIVE_OUTPUT_PREFLIGHT_REUSES
            .load(std::sync::atomic::Ordering::Relaxed)
            > before
    );
}

#[test]
fn sparse_rollback_preserves_untouched_external_values() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine.execute("CREATE TABLE rollback_lob (id INT PRIMARY KEY, n INT, stamp DATETIME, payload TEXT, bytes LONGBLOB)").unwrap();
    engine.execute("INSERT INTO rollback_lob VALUES (1,1,'2026-09-11 12:34:56',REPEAT('x',10000),REPEAT(UNHEX('00FF'),6000))").unwrap();
    let expected = result_rows(engine.execute("SELECT * FROM rollback_lob").unwrap());
    let mut session = SessionContext::default();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut session, "SAVEPOINT before_update")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "UPDATE rollback_lob SET n=n+1,stamp=NULL WHERE id=1",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "ROLLBACK TO before_update")
        .unwrap();
    engine.execute_with_session(&mut session, "COMMIT").unwrap();
    assert_eq!(
        expected,
        result_rows(engine.execute("SELECT * FROM rollback_lob").unwrap())
    );
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(
            &mut session,
            "UPDATE rollback_lob SET n=n+2,stamp=NULL WHERE id=1",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    assert_eq!(
        expected,
        result_rows(engine.execute("SELECT * FROM rollback_lob").unwrap())
    );
    drop(engine);
    let reopened = open_strict_test_engine(dir.path());
    assert_eq!(
        expected,
        result_rows(reopened.execute("SELECT * FROM rollback_lob").unwrap())
    );
}

#[test]
fn concurrent_sparse_updates_preserve_counters_under_eviction() {
    check_concurrent_sparse_updates_under_eviction(true);
}

#[test]
fn blocking_sparse_updates_preserve_counters_under_eviction() {
    check_concurrent_sparse_updates_under_eviction(false);
}

fn check_concurrent_sparse_updates_under_eviction(nonblocking: bool) {
    const ROWS: usize = 2048;
    const WORKERS: usize = 16;
    const ROUNDS: usize = 256;
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(
        StorageQueryEngine::open_with_storage_config_without_background_workers(
            dir.path(),
            StorageConfig {
                durability: crate::storage::DurabilityMode::Strict,
                checkpoint_interval: Duration::from_secs(60),
                checkpoint_wal_bytes: 64 * 1024 * 1024,
                lock_wait: Duration::from_secs(10),
                group_commit_window: Duration::ZERO,
                wal_segment_bytes: 64 * 1024 * 1024,
                buffer_pool_pages: 64,
                buffer_pool_io_threads: 4,
                buffer_pool_prefetch_pages: 4,
                undo_retention: Duration::ZERO,
                heap_update_reserve_bytes: 128,
                startup_validation: crate::storage::StartupValidationMode::Auto,
                startup_validate_indexes: true,
            },
        )
        .unwrap(),
    );
    engine.execute("CREATE TABLE counters (a INT, b INT, id INT, amount DECIMAL(12,2), n INT, padding VARCHAR(900), PRIMARY KEY(a,b,id))").unwrap();
    engine
        .execute("CREATE TABLE ledger (seq INT PRIMARY KEY, id INT)")
        .unwrap();
    let padding = "x".repeat(850);
    // Keep setup commit boundaries, wide rows and eviction pressure. The
    // regression exercises the concurrent prepared updates below, not SQL
    // parsing of the repeated padding literal.
    for start in (0..ROWS).step_by(64) {
        insert_fixture_rows(
            &engine,
            "test.counters",
            (start..start + 64).map(|id| {
                vec![
                    Value::Int(1),
                    Value::Int(1),
                    Value::Int(id as i64),
                    Value::DecimalCompact(1000, 2),
                    Value::Int(0),
                    Value::String(padding.clone()),
                ]
            }),
        );
    }
    let expected = std::sync::Arc::new(
        (0..ROWS)
            .map(|_| std::sync::atomic::AtomicU64::new(0))
            .collect::<Vec<_>>(),
    );
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(WORKERS));
    std::thread::scope(|scope| {
        let handles = (0..WORKERS)
            .map(|worker| {
                let engine = engine.clone();
                let expected = expected.clone();
                let barrier = barrier.clone();
                scope.spawn(move || {
                    let mut session = SessionContext::default();
                    engine
                        .execute_with_session(&mut session, "SET autocommit=OFF")
                        .unwrap();
                    let update_sql =
                        "UPDATE counters SET amount=amount+?, n=n+1 WHERE a=1 AND b=1 AND id=?";
                    let insert_sql = "INSERT INTO ledger VALUES (?,?)";
                    let update = engine.prepare_cached(&mut session, update_sql, 2).unwrap();
                    let insert = engine.prepare_cached(&mut session, insert_sql, 2).unwrap();
                    let mut update_state = PreparedDmlState::default();
                    let mut insert_state = PreparedDmlState::default();
                    barrier.wait();
                    let mut random = worker as u64 + 1;
                    for round in 0..ROUNDS {
                        random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                        let id = ((random >> 32) as usize) % ROWS;
                        let params = [Value::Decimal("4262.26".into()), Value::Int(id as i64)];
                        let mut result = if nonblocking {
                            crate::storage::with_nonblocking_lock_waits(|| {
                                fast(
                                    &engine,
                                    &mut session,
                                    &mut update_state,
                                    &update,
                                    update_sql,
                                    &params,
                                )
                            })
                            .unwrap()
                        } else {
                            fast(
                                &engine,
                                &mut session,
                                &mut update_state,
                                &update,
                                update_sql,
                                &params,
                            )
                            .unwrap()
                        };
                        if result
                            .as_ref()
                            .is_err_and(crate::storage::is_lock_would_block_error)
                        {
                            result = fast(
                                &engine,
                                &mut session,
                                &mut update_state,
                                &update,
                                update_sql,
                                &params,
                            )
                            .unwrap();
                        }
                        assert_eq!(
                            affected(result.unwrap()),
                            1,
                            "missing update: worker={worker}, round={round}, id={id}"
                        );
                        fast(
                            &engine,
                            &mut session,
                            &mut insert_state,
                            &insert,
                            insert_sql,
                            &[
                                Value::Int((worker * ROUNDS + round) as i64),
                                Value::Int(id as i64),
                            ],
                        )
                        .unwrap()
                        .unwrap();
                        engine.execute_with_session(&mut session, "COMMIT").unwrap();
                        expected[id].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.join().unwrap();
        }
    });
    let rows = result_rows(
        engine
            .execute("SELECT id,n FROM counters ORDER BY id")
            .unwrap()
            .materialize()
            .unwrap(),
    );
    assert_eq!(rows.len(), ROWS);
    let actual_total: u64 = rows
        .iter()
        .map(|row| row[1].to_display_string().parse::<u64>().unwrap())
        .sum();
    let mismatches = rows
        .iter()
        .enumerate()
        .filter_map(|(id, row)| {
            let n = expected[id].load(std::sync::atomic::Ordering::Relaxed);
            (row[1].to_display_string() != n.to_string()).then_some((id, row[1].clone(), n))
        })
        .collect::<Vec<_>>();
    for (id, row) in rows.iter().enumerate() {
        assert_eq!(row[0], Value::Int(id as i64));
        let wanted = expected[id]
            .load(std::sync::atomic::Ordering::Relaxed)
            .to_string();
        if row[1].to_display_string() != wanted {
            let sql = format!("SELECT id,n,amount FROM counters WHERE a=1 AND b=1 AND id={id}");
            let point = result_rows(engine.execute(&sql).unwrap().materialize().unwrap());
            let db = engine.db_handle();
            let table = db.table("test.counters").unwrap();
            let index = table.indexes.iter().find(|i| i.primary).unwrap();
            let ids = db
                .lookup_index(
                    &table,
                    index,
                    crate::storage::IndexKey::from(vec![
                        crate::storage::KeyPart::Int(1),
                        crate::storage::KeyPart::Int(1),
                        crate::storage::KeyPart::Int(id as i64),
                    ]),
                )
                .unwrap();
            let raw = ids
                .iter()
                .map(|&rid| {
                    db.read_mvcc_row_columns_with_plan(table.id, rid, &[3, 4], None)
                        .unwrap()
                })
                .collect::<Vec<_>>();

            let ledger = result_rows(
                engine
                    .execute(&format!("SELECT COUNT(*) FROM ledger WHERE id={id}"))
                    .unwrap()
                    .materialize()
                    .unwrap(),
            );
            engine.checkpoint().unwrap();
            let after_checkpoint =
                result_rows(engine.execute(&sql).unwrap().materialize().unwrap());
            panic!("lost update id={id}, total={actual_total}, mismatches={mismatches:?}, expected={wanted}, scan={row:?}, point={point:?}, raw={raw:?}, ledger={ledger:?}, checkpoint={after_checkpoint:?}");
        }
    }
    assert!(engine.db_handle().storage_status().buffer_pool_evictions > 0);
}

#[test]
fn prepared_range_update_sparse_rebind_noop_and_savepoint() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE range_u (g INT,n INT,v BIGINT,copy BIGINT,stamp TIMESTAMP,payload LONGTEXT,PRIMARY KEY(g,n))").unwrap();
    engine.execute("INSERT INTO range_u SELECT n DIV 4, MOD(n,4),n,0,NULL,REPEAT('x',12000) FROM generate_series(0,7) AS s(n)").unwrap();
    // External-payload INSERT headers require one ordinary rewrite before
    // the existing sparse patch contract accepts the row.
    engine.execute("UPDATE range_u SET copy=1").unwrap();
    let before = result_rows(
        engine
            .execute("SELECT * FROM range_u ORDER BY g,n")
            .unwrap(),
    );
    let mut session = SessionContext::default();
    let sql = "UPDATE range_u SET v=v+?,copy=v,stamp=? WHERE g=? AND n>=? AND v<?";
    let parsed = engine.prepare_cached(&mut session, sql, 5).unwrap();
    let mut state = PreparedDmlState::default();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut session, "SAVEPOINT s")
        .unwrap();
    let sparse = crate::storage::prepared_point_sparse_patch_hits_for_test();
    let lob_reads = crate::storage::lob_full_materializations_for_test();
    for (g, expected) in [(0, 3), (1, 2)] {
        let response = fast(
            &engine,
            &mut session,
            &mut state,
            &parsed,
            sql,
            &[
                Value::Int(10),
                Value::String("2026-10-08 12:00:00".into()),
                Value::Int(g),
                Value::Int(1),
                Value::Int(7),
            ],
        )
        .unwrap()
        .unwrap();
        assert_eq!(affected(response), expected);
    }
    assert_eq!(
        crate::storage::prepared_point_sparse_patch_hits_for_test() - sparse,
        5
    );
    assert_eq!(
        crate::storage::lob_full_materializations_for_test(),
        lob_reads
    );
    let PreparedDmlState::Ready(plan) = &state else {
        panic!("missing memo")
    };
    assert!(plan.range_update.is_some());
    let rows = result_rows(
        engine
            .execute_with_session(
                &mut session,
                "SELECT v,copy,LENGTH(payload) FROM range_u WHERE g=0 AND n=1",
            )
            .unwrap(),
    );
    assert_eq!(
        rows,
        vec![vec![Value::Int(11), Value::Int(11), Value::Int(12000)]]
    );
    engine
        .execute_with_session(&mut session, "ROLLBACK TO SAVEPOINT s")
        .unwrap();
    assert_eq!(
        result_rows(
            engine
                .execute_with_session(&mut session, "SELECT * FROM range_u ORDER BY g,n")
                .unwrap()
        ),
        before
    );
    engine.execute_with_session(&mut session, "COMMIT").unwrap();

    let sql = "UPDATE range_u SET v=v WHERE g=? AND n>=?";
    let parsed = engine.prepare_cached(&mut session, sql, 2).unwrap();
    let response = fast(
        &engine,
        &mut session,
        &mut PreparedDmlState::default(),
        &parsed,
        sql,
        &[Value::Int(1), Value::Int(1)],
    )
    .unwrap()
    .unwrap();
    assert_eq!(affected(response), 0);
    assert_eq!(crate::engine::api::take_statement_matched_rows(), Some(3));
}

#[test]
fn prepared_range_update_matches_legacy_types_indexes_and_large_batches() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE range_types (g INT,n INT,v INT,a DECIMAL(12,2),s VARCHAR(40),dt DATE,j JSON,PRIMARY KEY(g,n),KEY vi(v,g))").unwrap();
    engine.execute("INSERT INTO range_types SELECT 1,n,MOD(n,5),10.25,'abc','2026-10-08','{\"x\":1}' FROM generate_series(1,100) AS gs(n)").unwrap();
    let mut session = SessionContext::default();
    for upper in [12, 100] {
        for sql in [
            "UPDATE range_types SET v=v+?,a=a+1.25 WHERE g=1 AND n<=? AND v<3",
            "UPDATE range_types SET s=CONCAT(s,?),j=JSON_SET(j,'$.x',2) WHERE g=1 AND n<=?",
            "UPDATE range_types SET v=COALESCE(v,0)+?,dt=NULL WHERE g=1 AND n<=? AND (v<3 OR v IS NULL)",
        ] {
            let params=[if sql.contains("CONCAT") {Value::String("xyz".into())} else {Value::Int(10)},Value::Int(upper)];
            let parsed=engine.prepare_cached(&mut session,sql,2).unwrap();
            engine.execute_with_session(&mut session,"BEGIN").unwrap();
            let fast_count=affected(fast(&engine,&mut session,&mut PreparedDmlState::default(),&parsed,sql,&params).unwrap().unwrap());
            let fast_rows=result_rows(engine.execute_with_session(&mut session,"SELECT * FROM range_types ORDER BY g,n").unwrap());
            let fast_index=result_rows(engine.execute_with_session(&mut session,"SELECT n FROM range_types FORCE INDEX(vi) WHERE v>=10 ORDER BY n").unwrap());
            engine.execute_with_session(&mut session,"ROLLBACK").unwrap();
            engine.execute_with_session(&mut session,"BEGIN").unwrap();
            let legacy_count=affected(legacy(&engine,&mut session,&parsed,sql,&params).unwrap());
            assert_eq!(fast_count,legacy_count,"{sql} limit {upper}");
            assert_eq!(fast_rows,result_rows(engine.execute_with_session(&mut session,"SELECT * FROM range_types ORDER BY g,n").unwrap()),"{sql}");
            assert_eq!(fast_index,result_rows(engine.execute_with_session(&mut session,"SELECT n FROM range_types FORCE INDEX(vi) WHERE v>=10 ORDER BY n").unwrap()),"{sql}");
            engine.execute_with_session(&mut session,"ROLLBACK").unwrap();
        }
    }
}

#[test]
fn prepared_range_update_rechecks_predicate_after_lock_wait() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 2000));
    engine
        .execute("CREATE TABLE range_wait (g INT,n INT,v INT,PRIMARY KEY(g,n))")
        .unwrap();
    engine
        .execute("INSERT INTO range_wait VALUES (1,1,0),(1,2,0)")
        .unwrap();
    let mut owner = SessionContext::default();
    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "UPDATE range_wait SET v=10 WHERE g=1 AND n=1")
        .unwrap();
    let worker_engine = std::sync::Arc::clone(&engine);
    let worker = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        let sql = "UPDATE range_wait SET v=v+1 WHERE g=? AND v=0";
        let parsed = worker_engine.prepare_cached(&mut session, sql, 1).unwrap();
        engine_range_wait_run(&worker_engine, &mut session, &parsed, sql)
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while engine.db_for_tests().storage_status().active_lock_waits == 0
        && std::time::Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(1));
    }
    engine.execute_with_session(&mut owner, "COMMIT").unwrap();
    assert_eq!(worker.join().unwrap(), 1);
    assert_eq!(
        result_rows(
            engine
                .execute("SELECT v FROM range_wait ORDER BY n")
                .unwrap()
        ),
        vec![vec![Value::Int(10)], vec![Value::Int(1)]]
    );
}

fn engine_range_wait_run(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    parsed: &std::sync::Arc<Statement>,
    sql: &str,
) -> u64 {
    affected(
        fast(
            engine,
            session,
            &mut PreparedDmlState::default(),
            parsed,
            sql,
            &[Value::Int(1)],
        )
        .unwrap()
        .unwrap(),
    )
}

#[test]
fn prepared_range_update_constraint_error_rolls_back_statement_and_rebinds_ddl() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE range_check (g INT,n INT,v INT CHECK(v<5),PRIMARY KEY(g,n))")
        .unwrap();
    engine
        .execute("INSERT INTO range_check VALUES(1,1,1),(1,2,4)")
        .unwrap();
    let mut session = SessionContext::default();
    let sql = "UPDATE range_check SET v=v+? WHERE g=?";
    let parsed = engine.prepare_cached(&mut session, sql, 2).unwrap();
    let mut state = PreparedDmlState::default();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    assert!(fast(
        &engine,
        &mut session,
        &mut state,
        &parsed,
        sql,
        &[Value::Int(1), Value::Int(1)]
    )
    .unwrap()
    .is_err());
    assert_eq!(
        result_rows(
            engine
                .execute_with_session(&mut session, "SELECT v FROM range_check ORDER BY n")
                .unwrap()
        ),
        vec![vec![Value::Int(1)], vec![Value::Int(4)]]
    );
    engine.execute_with_session(&mut session, "COMMIT").unwrap();
    engine
        .execute("ALTER TABLE range_check ADD COLUMN extra VARCHAR(20) DEFAULT 'kept'")
        .unwrap();
    assert_eq!(
        affected(
            fast(
                &engine,
                &mut session,
                &mut state,
                &parsed,
                sql,
                &[Value::Int(-1), Value::Int(1)]
            )
            .unwrap()
            .unwrap()
        ),
        2
    );
    assert_eq!(
        result_rows(
            engine
                .execute("SELECT v,extra FROM range_check ORDER BY n")
                .unwrap()
        ),
        vec![
            vec![Value::Int(0), Value::String("kept".into())],
            vec![Value::Int(3), Value::String("kept".into())]
        ]
    );
}

#[test]
fn prepared_range_update_sparse_error_undo_and_unaffected_foreign_key() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE range_parent (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO range_parent VALUES(1)")
        .unwrap();
    engine.execute("CREATE TABLE range_child (g INT,n INT,v TINYINT,p INT,PRIMARY KEY(g,n),FOREIGN KEY(p) REFERENCES range_parent(id))").unwrap();
    engine
        .execute("INSERT INTO range_child VALUES(1,1,1,1),(1,2,127,1)")
        .unwrap();
    let mut session = SessionContext::default();
    let sql = "UPDATE range_child SET v=v+? WHERE g=?";
    let parsed = engine.prepare_cached(&mut session, sql, 2).unwrap();
    let mut state = PreparedDmlState::default();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    let hits = crate::storage::prepared_point_sparse_patch_hits_for_test();
    fast(
        &engine,
        &mut session,
        &mut state,
        &parsed,
        sql,
        &[Value::Int(1), Value::Int(1)],
    )
    .unwrap()
    .expect_err("overflow must roll back earlier row");
    assert!(crate::storage::prepared_point_sparse_patch_hits_for_test() > hits);
    assert_eq!(
        result_rows(
            engine
                .execute_with_session(&mut session, "SELECT v,p FROM range_child ORDER BY n")
                .unwrap()
        ),
        vec![
            vec![Value::Int(1), Value::Int(1)],
            vec![Value::Int(127), Value::Int(1)]
        ]
    );
    assert_eq!(
        affected(
            fast(
                &engine,
                &mut session,
                &mut state,
                &parsed,
                sql,
                &[Value::Int(-1), Value::Int(1)]
            )
            .unwrap()
            .unwrap()
        ),
        2
    );
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    let sql = "UPDATE range_child SET p=? WHERE g=?";
    let parsed = engine.prepare_cached(&mut session, sql, 2).unwrap();
    fast(
        &engine,
        &mut session,
        &mut PreparedDmlState::default(),
        &parsed,
        sql,
        &[Value::Int(2), Value::Int(1)],
    )
    .unwrap()
    .expect_err("affected foreign key must still be checked");
}
