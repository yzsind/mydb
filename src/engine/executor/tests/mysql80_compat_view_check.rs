//! P1-A4 — `WITH CHECK OPTION` enforcement on updatable views.
//!
//! The old src/storage/schema.rs:204 comment claiming enforcement was unimplemented
//! was incorrect. UpdatablePlan.check_predicate is already ANDed into
//! stmt.view_check_predicate in dispatch.rs:9611/9639; insert.rs::validate_insert_view_check
//! and dml.rs::validate_view_check evaluate INSERT/UPDATE after-images and return
//! 1369 ER_VIEW_CHECK_FAILED. These tests establish:
//!
//! INSERT through WITH CHECK OPTION failing WHERE returns 1369.
//! INSERT satisfying WHERE persists and is visible through the view.
//! UPDATE moving a row outside the view returns 1369.
//! UPDATE remaining inside the view succeeds.
//! Without CHECK OPTION, moving outside the view succeeds, matching MySQL.
//! CASCADED combines WHERE predicates from all levels; LOCAL uses only this level.

use super::support::execute_materialized as exec;
use super::*;

fn setup(engine: &StorageQueryEngine, session: &mut SessionContext) {
    exec(
        engine,
        session,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    exec(
        engine,
        session,
        "CREATE VIEW v_pos AS SELECT id, v FROM t WHERE v > 0 WITH CHECK OPTION",
    )
    .unwrap();
    exec(
        engine,
        session,
        "CREATE VIEW v_no_check AS SELECT id, v FROM t WHERE v > 0",
    )
    .unwrap();
}

#[test]
fn insert_violating_check_option_returns_1369() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    setup(&engine, &mut session);

    // The view's WHERE is v > 0; inserting v = -1 must be rejected.
    let err = exec(&engine, &mut session, "INSERT INTO v_pos VALUES (1, -1)").unwrap_err();
    assert_eq!(
        err.code(),
        1369,
        "INSERT violating CHECK OPTION must emit ER_VIEW_CHECK_FAILED 1369; got {err:?}"
    );
}

#[test]
fn insert_satisfying_check_option_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    setup(&engine, &mut session);

    // v = 5 satisfies v > 0.
    assert!(matches!(
        exec(&engine, &mut session, "INSERT INTO v_pos VALUES (1, 5)").unwrap(),
        QueryResponse::Ok | QueryResponse::OkAffected { .. }
    ));
    // The row is now visible through the view.
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SELECT id, v FROM v_pos").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
}

#[test]
fn update_pushing_row_out_of_view_with_check_returns_1369() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    setup(&engine, &mut session);
    exec(
        &engine,
        &mut session,
        "INSERT INTO t VALUES (1, 5), (2, 10)",
    )
    .unwrap();

    // Both rows currently satisfy v > 0; update id=1 to v = -3 should
    // push it out of the view's WHERE and trip CHECK OPTION.
    let err = exec(
        &engine,
        &mut session,
        "UPDATE v_pos SET v = -3 WHERE id = 1",
    )
    .unwrap_err();
    assert_eq!(err.code(), 1369);
}

#[test]
fn update_staying_within_view_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    setup(&engine, &mut session);
    exec(&engine, &mut session, "INSERT INTO t VALUES (1, 5)").unwrap();

    // 5 → 7 stays inside v > 0.
    assert!(matches!(
        exec(&engine, &mut session, "UPDATE v_pos SET v = 7 WHERE id = 1").unwrap(),
        QueryResponse::Ok | QueryResponse::OkAffected { .. }
    ));
}

#[test]
fn update_through_view_without_check_can_push_row_out() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    setup(&engine, &mut session);
    exec(&engine, &mut session, "INSERT INTO t VALUES (1, 5)").unwrap();

    // Same UPDATE through v_no_check (no WITH CHECK OPTION) — the row
    // silently leaves the view's WHERE. Matches MySQL behaviour: no
    // CHECK OPTION means no post-image validation.
    assert!(matches!(
        exec(
            &engine,
            &mut session,
            "UPDATE v_no_check SET v = -3 WHERE id = 1"
        )
        .unwrap(),
        QueryResponse::Ok | QueryResponse::OkAffected { .. }
    ));
    // The row's gone from the view's projection — but still in the base
    // table.
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SELECT COUNT(*) FROM v_no_check").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0].to_display_string(), "0");
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SELECT COUNT(*) FROM t").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0].to_display_string(), "1");
}

#[test]
fn cascaded_check_option_chains_outer_view_predicate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    // Inner view v_pos: v > 0
    // Outer view v_outer: v < 100, CASCADED check → must enforce
    // both layers (v > 0 AND v < 100).
    exec(
        &engine,
        &mut session,
        "CREATE VIEW v_pos AS SELECT id, v FROM t WHERE v > 0",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE VIEW v_outer AS SELECT id, v FROM v_pos WHERE v < 100 \
         WITH CASCADED CHECK OPTION",
    )
    .unwrap();

    // v = 50 satisfies both.
    exec(&engine, &mut session, "INSERT INTO v_outer VALUES (1, 50)").unwrap();
    // v = 200 violates outer predicate.
    let err = exec(&engine, &mut session, "INSERT INTO v_outer VALUES (2, 200)").unwrap_err();
    assert_eq!(err.code(), 1369);
    // v = -5 violates inner predicate (caught by CASCADED).
    let err = exec(&engine, &mut session, "INSERT INTO v_outer VALUES (3, -5)").unwrap_err();
    assert_eq!(err.code(), 1369);
}

#[test]
fn local_check_option_skips_outer_view_predicate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE VIEW v_pos AS SELECT id, v FROM t WHERE v > 0",
    )
    .unwrap();
    // LOCAL check option only enforces this layer's predicate
    // (v < 100). The inner view's v > 0 is NOT enforced via LOCAL.
    exec(
        &engine,
        &mut session,
        "CREATE VIEW v_local AS SELECT id, v FROM v_pos WHERE v < 100 \
         WITH LOCAL CHECK OPTION",
    )
    .unwrap();

    // v = 50 satisfies LOCAL predicate.
    exec(&engine, &mut session, "INSERT INTO v_local VALUES (1, 50)").unwrap();
    // v = 200 violates LOCAL predicate → reject.
    let err = exec(&engine, &mut session, "INSERT INTO v_local VALUES (2, 200)").unwrap_err();
    assert_eq!(err.code(), 1369);
}
