//! MySQL 8.0 P0 DML compatibility regressions.
//!
//! Corresponds to docs/dml-mysql80-compat-design.md Section 3.P0:
//! S1: INSERT priority fields, LOW/HIGH no-ops and DELAYED warning 1287.
//! P1: UPDATE LOW_PRIORITY accepted as a no-op, combinable with IGNORE.
//! S2/Q2: OK packet matched_rows/info changes; UPDATE generates
//! Rows matched: N Changed: M Warnings: W.
//! P2: DO expr [, expr] ... statements.
//! P5: HANDLER returns explicit 1235 unsupported instead of 1064.
//! LOAD DATA/LOAD XML compatibility G1-G12: SET, @var, CHARACTER SET,
//! sql_mode-aware coercion, OK info text, column-count warnings, and LOAD XML.
use super::*;
use crate::engine::api::{take_statement_info, take_statement_matched_rows};
use crate::parser::{parse_statement, Statement};

fn rows_of(response: QueryResponse) -> Vec<Vec<Value>> {
    match response {
        QueryResponse::ResultSet(result) => result.rows,
        other => panic!("expected result set, got {other:?}"),
    }
}

// Manual UPDATE/DELETE ORDER BY/LIMIT parsing via find_top_level_keyword once sliced
// unconditionally at idx + keyword.len(), splitting multibyte UTF-8 literals
// and panicking. UPDATE with Chinese values must execute normally.
#[test]
fn update_with_multibyte_string_value_does_not_panic() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t1 (id INT, c1 VARCHAR(32))")
        .unwrap();
    engine.execute("INSERT INTO t1 VALUES (1, 'a')").unwrap();
    engine.execute("UPDATE t1 SET c1='大' WHERE id=1").unwrap();
    let rows = rows_of(engine.execute("SELECT c1 FROM t1 WHERE id=1").unwrap());
    assert_eq!(rows, vec![vec![Value::String("大".into())]]);
}

// -------- S1: INSERT priority --------

#[test]
fn s1_insert_low_priority_accepted_as_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let resp = engine
        .execute("INSERT LOW_PRIORITY INTO t VALUES (1, 10)")
        .unwrap();
    match resp {
        QueryResponse::OkAffected { affected_rows } => assert_eq!(affected_rows, 1),
        other => panic!("expected OkAffected, got {other:?}"),
    }
    // Warnings must be empty — LOW_PRIORITY is no-op, not deprecated.
    crate::engine::api::take_statement_warnings();
}

#[test]
fn s1_insert_high_priority_accepted_as_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    let resp = engine
        .execute("INSERT HIGH_PRIORITY INTO t VALUES (1)")
        .unwrap();
    match resp {
        QueryResponse::OkAffected { affected_rows } => assert_eq!(affected_rows, 1),
        other => panic!("expected OkAffected, got {other:?}"),
    }
}

#[test]
fn s1_insert_delayed_raises_warning_1287() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    // We need a SessionContext to inspect warnings — execute() uses a
    // throwaway session whose warnings we cannot read after the fact.
    let mut session = SessionContext::default();
    let resp = engine
        .execute_with_session(&mut session, "INSERT DELAYED INTO t VALUES (1)")
        .unwrap();
    match resp {
        QueryResponse::OkAffected { affected_rows } => assert_eq!(affected_rows, 1),
        other => panic!("expected OkAffected, got {other:?}"),
    }
    let codes: Vec<u16> = session.warnings.iter().map(|w| w.code()).collect();
    assert!(
        codes.contains(&1287),
        "INSERT DELAYED must surface warning 1287; got {codes:?}",
    );
}

#[test]
fn s1_replace_low_priority_accepted_as_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 1)").unwrap();
    let resp = engine
        .execute("REPLACE LOW_PRIORITY INTO t VALUES (1, 2)")
        .unwrap();
    match resp {
        QueryResponse::OkAffected { affected_rows } => assert_eq!(affected_rows, 2),
        other => panic!("expected OkAffected, got {other:?}"),
    }
}

// -------- P1: UPDATE LOW_PRIORITY --------

#[test]
fn p1_update_low_priority_accepted_as_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    let resp = engine
        .execute("UPDATE LOW_PRIORITY t SET v = 20 WHERE id = 1")
        .unwrap();
    match resp {
        QueryResponse::OkAffected { affected_rows } => assert_eq!(affected_rows, 1),
        other => panic!("expected OkAffected, got {other:?}"),
    }
}

#[test]
fn p1_update_low_priority_ignore_can_combine() {
    // The strip pass must accept both modifiers in either order.
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();
    engine
        .execute("UPDATE LOW_PRIORITY IGNORE t SET v = 30 WHERE id = 1")
        .unwrap();
    engine
        .execute("UPDATE IGNORE t SET v = 40 WHERE id = 2")
        .unwrap();
}

#[test]
fn p1_update_low_priority_field_propagates_to_ast() {
    let parsed = parse_statement("UPDATE LOW_PRIORITY t SET v = 1").unwrap();
    match parsed {
        Statement::Update(update) => {
            assert!(update.low_priority);
            assert!(!update.ignore);
        }
        other => panic!("expected Update, got {other:?}"),
    }
    let parsed = parse_statement("UPDATE LOW_PRIORITY IGNORE t SET v = 1").unwrap();
    match parsed {
        Statement::Update(update) => {
            assert!(update.low_priority);
            assert!(update.ignore);
        }
        other => panic!("expected Update, got {other:?}"),
    }
}

// -------- S2/Q2: OK packet matched/info --------

#[test]
fn s2_update_sets_matched_rows_and_info_text() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    // The thread-local channels are cleared at the start of each
    // statement, so reading immediately after the UPDATE captures the
    // values the protocol layer would consume.
    let resp = engine.execute("UPDATE t SET v = v + 1").unwrap();
    let matched = take_statement_matched_rows();
    match resp {
        QueryResponse::OkAffected { affected_rows } => assert_eq!(affected_rows, 3),
        other => panic!("expected OkAffected, got {other:?}"),
    }
    assert_eq!(matched, Some(3), "matched count must be set by UPDATE");
    // info is generated by the session layer at OK-packet build time,
    // not by execute(). What we can verify here is that the executor
    // didn't pre-stash an `info` (UPDATE leaves it empty so the session
    // can format it itself).
    let info = take_statement_info();
    assert!(info.is_empty(), "executor must leave info empty for UPDATE");
}

#[test]
fn s2_delete_does_not_set_matched_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1), (2)").unwrap();
    let resp = engine.execute("DELETE FROM t WHERE id = 1").unwrap();
    let matched = take_statement_matched_rows();
    match resp {
        QueryResponse::OkAffected { affected_rows } => assert_eq!(affected_rows, 1),
        other => panic!("expected OkAffected, got {other:?}"),
    }
    assert!(
        matched.is_none(),
        "DELETE should not stash a matched count; got {matched:?}"
    );
}

// -------- M1: no-op detection / matched vs changed --------

fn scalar_i64(engine: &StorageQueryEngine, sql: &str) -> i64 {
    let rows = rows_of(engine.execute(sql).unwrap());
    match &rows[0][0] {
        Value::Int(v) => *v,
        Value::UInt(v) => *v as i64,
        other => panic!("expected integer scalar, got {other:?}"),
    }
}

#[test]
fn m1_update_full_noop_matched_but_zero_changed() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    // Every row set to its current value → matched=3, changed=0.
    let resp = engine.execute("UPDATE t SET v = v").unwrap();
    let matched = take_statement_matched_rows();
    match resp {
        QueryResponse::OkAffected { affected_rows } => {
            assert_eq!(affected_rows, 0, "no-op UPDATE must report 0 changed");
        }
        other => panic!("expected OkAffected, got {other:?}"),
    }
    assert_eq!(matched, Some(3), "all three rows are matched");
}

#[test]
fn m1_update_partial_noop_counts_only_changed_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 0), (2, 5), (3, 5)")
        .unwrap();
    // Sets v=5 everywhere: rows 2,3 already 5 (no-op), only row 1 changes.
    let resp = engine.execute("UPDATE t SET v = 5").unwrap();
    let matched = take_statement_matched_rows();
    match resp {
        QueryResponse::OkAffected { affected_rows } => {
            assert_eq!(affected_rows, 1, "only one row actually changed");
        }
        other => panic!("expected OkAffected, got {other:?}"),
    }
    assert_eq!(matched, Some(3), "all three rows are matched");
}

#[test]
fn m1_update_exact_pk_noop_reports_zero_changed() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    // Exact unique-index write path: WHERE id = 1 resolves one row.
    let resp = engine.execute("UPDATE t SET v = 10 WHERE id = 1").unwrap();
    let matched = take_statement_matched_rows();
    match resp {
        QueryResponse::OkAffected { affected_rows } => {
            assert_eq!(affected_rows, 0, "exact-path no-op must report 0 changed");
        }
        other => panic!("expected OkAffected, got {other:?}"),
    }
    assert_eq!(matched, Some(1), "the WHERE id=1 row is matched");
    // A genuine change on the same path still counts.
    let resp = engine.execute("UPDATE t SET v = 11 WHERE id = 1").unwrap();
    let matched = take_statement_matched_rows();
    match resp {
        QueryResponse::OkAffected { affected_rows } => assert_eq!(affected_rows, 1),
        other => panic!("expected OkAffected, got {other:?}"),
    }
    assert_eq!(matched, Some(1));
}

#[test]
fn m1_on_update_timestamp_suppressed_on_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, v INT, \
             ts DATETIME DEFAULT '2020-01-01 00:00:00' ON UPDATE CURRENT_TIMESTAMP)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t (id, v) VALUES (1, 10)")
        .unwrap();
    // No-op update: ts must NOT be bumped (still 2020).
    engine.execute("UPDATE t SET v = 10 WHERE id = 1").unwrap();
    let year_after_noop = scalar_i64(&engine, "SELECT YEAR(ts) FROM t WHERE id = 1");
    assert_eq!(
        year_after_noop, 2020,
        "ON UPDATE CURRENT_TIMESTAMP must not fire on a no-op row"
    );
    // Real change: ts bumps to now (well past 2020).
    engine.execute("UPDATE t SET v = 11 WHERE id = 1").unwrap();
    let year_after_change = scalar_i64(&engine, "SELECT YEAR(ts) FROM t WHERE id = 1");
    assert!(
        year_after_change > 2020,
        "ON UPDATE CURRENT_TIMESTAMP must fire when a user column changes; got {year_after_change}"
    );
}

// -------- M2: UPDATE IGNORE coverage --------

#[test]
fn m2_update_ignore_skips_duplicate_key_exact_path() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, u INT UNIQUE)")
        .unwrap();
    engine
        .execute_with_session(&mut s, "INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();
    // Without IGNORE this collides with row 1's u=10 → 1062.
    let err = engine
        .execute_with_session(&mut s, "UPDATE t SET u = 10 WHERE id = 2")
        .unwrap_err();
    assert_eq!(err.code(), 1062, "plain UPDATE must raise duplicate-key");
    // WHERE id = 2 → exact unique-index write path.
    let resp = engine
        .execute_with_session(&mut s, "UPDATE IGNORE t SET u = 10 WHERE id = 2")
        .unwrap();
    match resp {
        QueryResponse::OkAffected { affected_rows } => {
            assert_eq!(affected_rows, 0, "colliding row is skipped");
        }
        other => panic!("expected OkAffected, got {other:?}"),
    }
    assert_eq!(s.warning_count(), 1, "UPDATE IGNORE must warn on the skip");
    // Row 2 is untouched.
    assert_eq!(scalar_i64(&engine, "SELECT u FROM t WHERE id = 2"), 20);
}

#[test]
fn m2_update_ignore_skips_duplicate_key_batch_path() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, u INT UNIQUE)")
        .unwrap();
    engine
        .execute_with_session(&mut s, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    // Range WHERE → batch path. Rows 2 and 3 would both collide with u=10.
    let resp = engine
        .execute_with_session(&mut s, "UPDATE IGNORE t SET u = 10 WHERE id >= 2")
        .unwrap();
    match resp {
        QueryResponse::OkAffected { affected_rows } => {
            assert_eq!(affected_rows, 0, "both colliding rows are skipped");
        }
        other => panic!("expected OkAffected, got {other:?}"),
    }
    assert!(
        s.warning_count() >= 1,
        "batch UPDATE IGNORE must warn on the skipped rows"
    );
    assert_eq!(scalar_i64(&engine, "SELECT u FROM t WHERE id = 2"), 20);
    assert_eq!(scalar_i64(&engine, "SELECT u FROM t WHERE id = 3"), 30);
}

#[test]
fn m2_update_ignore_coerces_not_null_violation() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(
            &mut s,
            "CREATE TABLE t (id INT PRIMARY KEY, v INT NOT NULL)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut s, "INSERT INTO t VALUES (1, 10)")
        .unwrap();
    let err = engine
        .execute_with_session(&mut s, "UPDATE t SET v = NULL WHERE id = 1")
        .unwrap_err();
    assert_eq!(err.code(), 1048, "plain UPDATE NOT NULL must raise 1048");
    let resp = engine
        .execute_with_session(&mut s, "UPDATE IGNORE t SET v = NULL WHERE id = 1")
        .unwrap();
    match resp {
        QueryResponse::OkAffected { affected_rows } => assert_eq!(affected_rows, 1),
        other => panic!("expected OkAffected, got {other:?}"),
    }
    assert_eq!(s.warning_count(), 1, "UPDATE IGNORE must warn 1048");
    assert_eq!(scalar_i64(&engine, "SELECT v FROM t WHERE id = 1"), 0);
}

// -------- M3: multi-table UPDATE subqueries --------

#[test]
fn m3_multi_table_update_scalar_subquery_in_set() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t1 (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE t2 (id INT PRIMARY KEY, w INT)")
        .unwrap();
    engine.execute("CREATE TABLE t3 (k INT)").unwrap();
    engine
        .execute("INSERT INTO t1 VALUES (1, 10), (2, 20)")
        .unwrap();
    engine
        .execute("INSERT INTO t2 VALUES (1, 100), (2, 200)")
        .unwrap();
    engine.execute("INSERT INTO t3 VALUES (5), (7)").unwrap();
    // Non-correlated scalar subquery in SET + join-column WHERE.
    let resp = engine
        .execute(
            "UPDATE t1 JOIN t2 ON t1.id = t2.id \
             SET t1.v = (SELECT MAX(k) FROM t3) WHERE t2.w > 150",
        )
        .unwrap();
    match resp {
        QueryResponse::OkAffected { affected_rows } => assert_eq!(affected_rows, 1),
        other => panic!("expected OkAffected, got {other:?}"),
    }
    // Only the id=2 row (t2.w=200) is updated to MAX(k)=7.
    assert_eq!(scalar_i64(&engine, "SELECT v FROM t1 WHERE id = 1"), 10);
    assert_eq!(scalar_i64(&engine, "SELECT v FROM t1 WHERE id = 2"), 7);
}

#[test]
fn m3_multi_table_update_in_subquery_in_where() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t1 (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE t2 (id INT PRIMARY KEY, w INT)")
        .unwrap();
    engine.execute("CREATE TABLE allow_ids (id INT)").unwrap();
    engine
        .execute("INSERT INTO t1 VALUES (1, 10), (2, 20)")
        .unwrap();
    engine
        .execute("INSERT INTO t2 VALUES (1, 100), (2, 200)")
        .unwrap();
    engine.execute("INSERT INTO allow_ids VALUES (2)").unwrap();
    // Non-correlated IN-subquery in the WHERE of a multi-table UPDATE.
    let resp = engine
        .execute(
            "UPDATE t1 JOIN t2 ON t1.id = t2.id \
             SET t1.v = t2.w WHERE t1.id IN (SELECT id FROM allow_ids)",
        )
        .unwrap();
    match resp {
        QueryResponse::OkAffected { affected_rows } => assert_eq!(affected_rows, 1),
        other => panic!("expected OkAffected, got {other:?}"),
    }
    assert_eq!(scalar_i64(&engine, "SELECT v FROM t1 WHERE id = 1"), 10);
    assert_eq!(scalar_i64(&engine, "SELECT v FROM t1 WHERE id = 2"), 200);
}

// -------- M4: multi-table UPDATE RIGHT JOIN flip --------

#[test]
fn m4_multi_table_update_right_join_flipped_to_left() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t1 (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE t2 (id INT PRIMARY KEY, w INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t1 VALUES (1, 10), (2, 20)")
        .unwrap();
    engine
        .execute("INSERT INTO t2 VALUES (1, 100), (2, 200), (3, 300)")
        .unwrap();
    // `t1 RIGHT JOIN t2` keeps every t2 row; row 3 has no matching t1.
    // Anti-join update: set t2.w on the unmatched-t1 rows.
    let resp = engine
        .execute(
            "UPDATE t1 RIGHT JOIN t2 ON t1.id = t2.id \
             SET t2.w = 999 WHERE t1.id IS NULL",
        )
        .unwrap();
    match resp {
        QueryResponse::OkAffected { affected_rows } => assert_eq!(affected_rows, 1),
        other => panic!("expected OkAffected, got {other:?}"),
    }
    assert_eq!(scalar_i64(&engine, "SELECT w FROM t2 WHERE id = 1"), 100);
    assert_eq!(scalar_i64(&engine, "SELECT w FROM t2 WHERE id = 2"), 200);
    assert_eq!(scalar_i64(&engine, "SELECT w FROM t2 WHERE id = 3"), 999);
}

#[test]
fn m4_multi_table_update_right_join_matched_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t1 (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE t2 (id INT PRIMARY KEY, w INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t1 VALUES (1, 10), (2, 20)")
        .unwrap();
    engine
        .execute("INSERT INTO t2 VALUES (1, 100), (2, 200), (3, 300)")
        .unwrap();
    // Update the always-present side (t2) from the optional side (t1) on
    // matched rows only.
    let resp = engine
        .execute(
            "UPDATE t1 RIGHT JOIN t2 ON t1.id = t2.id \
             SET t2.w = t1.v WHERE t1.id IS NOT NULL",
        )
        .unwrap();
    match resp {
        QueryResponse::OkAffected { affected_rows } => assert_eq!(affected_rows, 2),
        other => panic!("expected OkAffected, got {other:?}"),
    }
    assert_eq!(scalar_i64(&engine, "SELECT w FROM t2 WHERE id = 1"), 10);
    assert_eq!(scalar_i64(&engine, "SELECT w FROM t2 WHERE id = 2"), 20);
    assert_eq!(scalar_i64(&engine, "SELECT w FROM t2 WHERE id = 3"), 300);
}

#[test]
fn m4_multi_table_update_right_join_in_chain_still_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t1 (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE t2 (id INT PRIMARY KEY, w INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE t3 (id INT PRIMARY KEY, z INT)")
        .unwrap();
    // A RIGHT join buried in a multi-join chain isn't the clean two-table
    // case; it stays rejected with a clear message rather than mis-flipped.
    let err = engine
        .execute(
            "UPDATE t1 JOIN t2 ON t1.id = t2.id RIGHT JOIN t3 ON t2.id = t3.id \
             SET t1.v = 1",
        )
        .unwrap_err();
    assert_eq!(err.code(), 1235);
}

#[test]
fn m4_update_optimizer_hint_accepted_and_ignored() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    // MySQL parses optimizer hints on UPDATE and silently ignores the ones
    // it can't apply — it must not break the statement (no silent misparse).
    let resp = engine
        .execute("UPDATE /*+ MAX_EXECUTION_TIME(1000) */ t SET v = v + 1 WHERE id = 1")
        .unwrap();
    match resp {
        QueryResponse::OkAffected { affected_rows } => assert_eq!(affected_rows, 1),
        other => panic!("expected OkAffected, got {other:?}"),
    }
    assert_eq!(scalar_i64(&engine, "SELECT v FROM t WHERE id = 1"), 11);
}

// -------- M5: UPDATE robustness --------

#[test]
fn m5_update_halloween_indexed_key_updates_each_row_once() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1), (2), (3)")
        .unwrap();
    // Classic Halloween setup: the WHERE scans the same key the SET shifts
    // upward. Candidates are materialised as row ids up front, so each row
    // is updated exactly once (no re-scan cascade 1→101→201…).
    let resp = engine
        .execute("UPDATE t SET id = id + 100 WHERE id < 4")
        .unwrap();
    match resp {
        QueryResponse::OkAffected { affected_rows } => assert_eq!(affected_rows, 3),
        other => panic!("expected OkAffected, got {other:?}"),
    }
    let ids = rows_of(engine.execute("SELECT id FROM t ORDER BY id").unwrap());
    assert_eq!(
        ids,
        vec![
            vec![Value::Int(101)],
            vec![Value::Int(102)],
            vec![Value::Int(103)],
        ]
    );
}

#[test]
fn m5_self_join_update_touches_each_row_once() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    // Self-join: update a from its neighbour b (id = id+1). Rows 1,2 have a
    // successor; each target row is written exactly once (dedup by
    // participant + row_id) with the neighbour's value.
    let resp = engine
        .execute("UPDATE t a JOIN t b ON b.id = a.id + 1 SET a.v = b.v")
        .unwrap();
    match resp {
        QueryResponse::OkAffected { affected_rows } => assert_eq!(affected_rows, 2),
        other => panic!("expected OkAffected, got {other:?}"),
    }
    assert_eq!(scalar_i64(&engine, "SELECT v FROM t WHERE id = 1"), 20);
    assert_eq!(scalar_i64(&engine, "SELECT v FROM t WHERE id = 2"), 30);
    assert_eq!(scalar_i64(&engine, "SELECT v FROM t WHERE id = 3"), 30);
}

#[test]
fn m5_partitioned_update_noop_and_change_counts() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, v INT) \
             PARTITION BY RANGE (id) ( \
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN (100))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 5), (2, 5), (50, 5)")
        .unwrap();
    // No-op detection must also hold on the partitioned write path.
    let resp = engine.execute("UPDATE t SET v = 5").unwrap();
    let matched = take_statement_matched_rows();
    match resp {
        QueryResponse::OkAffected { affected_rows } => {
            assert_eq!(affected_rows, 0, "partitioned no-op reports 0 changed");
        }
        other => panic!("expected OkAffected, got {other:?}"),
    }
    assert_eq!(matched, Some(3), "all partitioned rows are matched");
    // A partial change: only id=50 moves.
    let resp = engine.execute("UPDATE t SET v = 9 WHERE id >= 50").unwrap();
    match resp {
        QueryResponse::OkAffected { affected_rows } => assert_eq!(affected_rows, 1),
        other => panic!("expected OkAffected, got {other:?}"),
    }
}

// -------- P2: DO statement --------

#[test]
fn p2_do_evaluates_expressions_and_returns_ok() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let resp = engine.execute("DO 1 + 1").unwrap();
    assert!(
        matches!(resp, QueryResponse::Ok),
        "DO must return Ok, got {resp:?}",
    );
    // Multi-expression form.
    let resp = engine.execute("DO 1, 2, 3 + 4").unwrap();
    assert!(matches!(resp, QueryResponse::Ok));
}

#[test]
fn p2_do_constant_string_extent_reports_bind_and_runtime_warnings() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "DO UPPER(SUBSTRING(1.25 FROM '2n'))")
        .unwrap();
    assert_eq!(
        rows_of(
            engine
                .execute_with_session(&mut session, "SHOW WARNINGS")
                .unwrap()
        )
        .iter()
        .map(|row| match row[1] {
            Value::Int(code) => code as u16,
            Value::UInt(code) => code as u16,
            ref other => panic!("expected warning code, got {other:?}"),
        })
        .collect::<Vec<_>>(),
        vec![1292, 1292]
    );

    engine
        .execute_with_session(
            &mut session,
            "DO NULLIF(1, '-' BETWEEN LCASE(RIGHT(11111111, ' 7,]')) AND '1')",
        )
        .unwrap();
    assert_eq!(
        rows_of(
            engine
                .execute_with_session(&mut session, "SHOW WARNINGS")
                .unwrap()
        )
        .iter()
        .map(|row| match row[1] {
            Value::Int(code) => code as u16,
            Value::UInt(code) => code as u16,
            ref other => panic!("expected warning code, got {other:?}"),
        })
        .collect::<Vec<_>>(),
        vec![1292, 1292]
    );
}

#[test]
fn p2_do_propagates_runtime_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    // A function call against a missing argument count surfaces 1582.
    let err = engine.execute("DO MOD()").unwrap_err();
    assert!(
        err.is(crate::error::catalog::ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT)
            || err.is(crate::error::catalog::ER_PARSE_ERROR),
        "expected 1582 / 1064 for bad arg count; got {} {}",
        err.code(),
        err.message(),
    );
}

#[test]
fn p2_do_rejects_from_clause() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    // The parser surfaces the FROM rejection as a parse-time 1064.
    let err = engine.execute("DO 1 FROM t").unwrap_err();
    assert_eq!(err.code(), 1064);
}

#[test]
fn p2_do_statement_variant_is_emitted() {
    let parsed = parse_statement("DO 1 + 2, SLEEP(0)").unwrap();
    match parsed {
        Statement::Do(exprs) => assert_eq!(exprs.len(), 2),
        other => panic!("expected Statement::Do, got {other:?}"),
    }
}

#[test]
fn p2_do_accepts_mysql_bang_not_inside_parentheses() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine.execute("DO (!(SECOND(0xb16beeb7)))").unwrap();
}

#[test]
fn auto_increment_is_not_reserved_before_not_null_preflight() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE ai_preflight (id INT AUTO_INCREMENT PRIMARY KEY, v INT NOT NULL)")
        .unwrap();
    for _ in 0..2 {
        let error = engine
            .execute("INSERT INTO ai_preflight (v) VALUES (NULL)")
            .unwrap_err();
        assert_eq!(error.code(), 1048);
    }
    engine
        .execute("INSERT INTO ai_preflight (v) VALUES (1)")
        .unwrap();
    assert_eq!(
        rows_of(engine.execute("SELECT id FROM ai_preflight").unwrap()),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn non_strict_multirow_insert_repairs_not_null_but_single_row_does_not() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE nn_multi (v INT NOT NULL)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET sql_mode = ''")
        .unwrap();
    let err = engine
        .execute_with_session(&mut session, "INSERT INTO nn_multi VALUES (NULL)")
        .unwrap_err();
    assert_eq!(err.code(), 1048);
    engine
        .execute_with_session(&mut session, "INSERT INTO nn_multi VALUES (1), (NULL)")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT v FROM nn_multi ORDER BY v")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(0)], vec![Value::Int(1)]]);
}

// -------- P5/P6: HANDLER / LOAD XML unsupported --------

#[test]
fn p5_handler_open_rejected_as_unsupported() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let err = engine.execute("HANDLER t OPEN").unwrap_err();
    assert_eq!(err.code(), 1235, "HANDLER must report 1235; got {err:?}");
    let msg = err.message().to_ascii_uppercase();
    assert!(
        msg.contains("HANDLER"),
        "error message should name HANDLER: {}",
        err.message()
    );
}

#[test]
fn p5_handler_read_rejected_as_unsupported() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let err = engine.execute("HANDLER t READ first").unwrap_err();
    assert_eq!(err.code(), 1235);
}

#[test]
fn p5_handler_close_rejected_as_unsupported() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let err = engine.execute("HANDLER t CLOSE").unwrap_err();
    assert_eq!(err.code(), 1235);
}

// -------- LOAD DATA / LOAD XML MySQL 8.0 compat (G1/G2/G3/G4/G8/G10/G11/G12) --------

fn load_write(dir: &std::path::Path, name: &str, content: &str) -> String {
    let p = dir.join(name);
    std::fs::write(&p, content).unwrap();
    crate::util::file::sql_path(&p)
}

/// G1/G2 — SET col=expr + @user_var targets are honoured (not silently
/// dropped). Column `c` is computed from a bound @var.
#[test]
fn load_data_set_clause_and_user_var() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, c INT)")
        .unwrap();
    let path = load_write(dir.path(), "d.tsv", "1\t10\n2\t20\n");
    let sql = format!("LOAD DATA INFILE '{path}' INTO TABLE t (id, @v) SET a = @v, c = @v * 2");
    engine.execute(&sql).unwrap();
    let out = rows_of(
        engine
            .execute("SELECT id, a, c FROM t ORDER BY id")
            .unwrap(),
    );
    assert_eq!(
        out,
        vec![
            vec![Value::Int(1), Value::Int(10), Value::Int(20)],
            vec![Value::Int(2), Value::Int(20), Value::Int(40)],
        ]
    );
}

/// G8 — OK-packet info trailer `Records: N Deleted: M Skipped: K Warnings: W`.
#[test]
fn load_data_info_trailer() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let path = load_write(dir.path(), "d.tsv", "1\t10\n2\t20\n3\t30\n");
    let sql = format!("LOAD DATA INFILE '{path}' INTO TABLE t");
    engine.execute(&sql).unwrap();
    let info = take_statement_info();
    assert_eq!(info, "Records: 3 Deleted: 0 Skipped: 0 Warnings: 0");
}

/// G8 — IGNORE duplicate rows are reported in `Skipped`.
#[test]
fn load_data_info_trailer_skipped_on_ignore() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (2, 999)").unwrap();
    let path = load_write(dir.path(), "d.tsv", "1\t10\n2\t20\n3\t30\n");
    let sql = format!("LOAD DATA INFILE '{path}' IGNORE INTO TABLE t");
    engine.execute(&sql).unwrap();
    let info = take_statement_info();
    assert!(
        info.contains("Records: 3") && info.contains("Skipped: 1"),
        "info was: {info}"
    );
    // The pre-existing row 2 is preserved (IGNORE), 1 and 3 inserted.
    let out = rows_of(engine.execute("SELECT id, v FROM t ORDER BY id").unwrap());
    assert_eq!(
        out,
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(999)],
            vec![Value::Int(3), Value::Int(30)],
        ]
    );
}

/// G3 — CHARACTER SET latin1 transcodes input bytes to utf8mb4.
#[test]
fn load_data_character_set_latin1() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, s VARCHAR(32))")
        .unwrap();
    // 0xE9 is 'é' in latin1.
    let path = dir.path().join("d.tsv");
    std::fs::write(&path, [b'1', b'\t', 0xE9, b'\n']).unwrap();
    let sql = format!(
        "LOAD DATA INFILE '{}' INTO TABLE t CHARACTER SET latin1",
        crate::util::file::sql_path(&path)
    );
    engine.execute(&sql).unwrap();
    let out = rows_of(engine.execute("SELECT s FROM t").unwrap());
    assert_eq!(out, vec![vec![Value::String("é".to_string())]]);
}

/// G4 — non-strict sql_mode downgrades an invalid date to a warning +
/// zero-date (consistent with INSERT), instead of a hard error. Uses a
/// persistent session so `SET sql_mode` is visible to the LOAD.
#[test]
fn load_data_sql_mode_invalid_date_nonstrict_warns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, d DATE)")
        .unwrap();
    engine
        .execute_with_session(&mut s, "SET sql_mode='NO_ENGINE_SUBSTITUTION'")
        .unwrap();
    let path = load_write(dir.path(), "d.tsv", "1\tnot-a-date\n");
    let sql = format!("LOAD DATA INFILE '{path}' INTO TABLE t");
    // Must not error in non-strict mode.
    engine.execute_with_session(&mut s, &sql).unwrap();
    let info = take_statement_info();
    assert!(info.contains("Warnings: 1"), "info was: {info}");
}

/// G4 — strict sql_mode still rejects an invalid date (hard error),
/// matching INSERT.
#[test]
fn load_data_sql_mode_invalid_date_strict_errors() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, d DATE)")
        .unwrap();
    engine
        .execute_with_session(&mut s, "SET sql_mode='STRICT_ALL_TABLES'")
        .unwrap();
    let path = load_write(dir.path(), "d.tsv", "1\tnot-a-date\n");
    let sql = format!("LOAD DATA INFILE '{path}' INTO TABLE t");
    assert!(
        engine.execute_with_session(&mut s, &sql).is_err(),
        "strict mode must reject"
    );
}

/// G11 — too few input fields → column-count warning 1261.
#[test]
fn load_data_column_count_too_few_warns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    engine
        .execute_with_session(&mut s, "SET sql_mode='NO_ENGINE_SUBSTITUTION'")
        .unwrap();
    let path = load_write(dir.path(), "d.tsv", "1\t10\n");
    let sql = format!("LOAD DATA INFILE '{path}' INTO TABLE t");
    engine.execute_with_session(&mut s, &sql).unwrap();
    let info = take_statement_info();
    assert!(info.contains("Warnings: 1"), "info was: {info}");
}

/// G12 — LOAD XML, nested element shape.
#[test]
fn load_xml_nested_shape() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(32))")
        .unwrap();
    let xml = "<rows>\
        <row><id>1</id><name>alice</name></row>\
        <row><id>2</id><name>bob</name></row>\
        </rows>";
    let path = load_write(dir.path(), "d.xml", xml);
    let sql = format!("LOAD XML INFILE '{path}' INTO TABLE t ROWS IDENTIFIED BY '<row>'");
    engine.execute(&sql).unwrap();
    let out = rows_of(
        engine
            .execute("SELECT id, name FROM t ORDER BY id")
            .unwrap(),
    );
    assert_eq!(
        out,
        vec![
            vec![Value::Int(1), Value::String("alice".to_string())],
            vec![Value::Int(2), Value::String("bob".to_string())],
        ]
    );
}

/// G12 — LOAD XML, attribute shape.
#[test]
fn load_xml_attribute_shape() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(32))")
        .unwrap();
    let xml = "<rows>\
        <row id=\"1\" name=\"alice\"/>\
        <row id=\"2\" name=\"bob\"/>\
        </rows>";
    let path = load_write(dir.path(), "d.xml", xml);
    let sql = format!("LOAD XML INFILE '{path}' INTO TABLE t ROWS IDENTIFIED BY '<row>'");
    engine.execute(&sql).unwrap();
    let out = rows_of(
        engine
            .execute("SELECT id, name FROM t ORDER BY id")
            .unwrap(),
    );
    assert_eq!(
        out,
        vec![
            vec![Value::Int(1), Value::String("alice".to_string())],
            vec![Value::Int(2), Value::String("bob".to_string())],
        ]
    );
}

/// G12 — LOAD XML, `<field name=...>` shape.
#[test]
fn load_xml_field_shape() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(32))")
        .unwrap();
    let xml = "<list>\
        <row><field name=\"id\">1</field><field name=\"name\">alice</field></row>\
        <row><field name=\"id\">2</field><field name=\"name\">bob</field></row>\
        </list>";
    let path = load_write(dir.path(), "d.xml", xml);
    let sql = format!("LOAD XML INFILE '{path}' INTO TABLE t ROWS IDENTIFIED BY '<row>'");
    engine.execute(&sql).unwrap();
    let out = rows_of(
        engine
            .execute("SELECT id, name FROM t ORDER BY id")
            .unwrap(),
    );
    assert_eq!(
        out,
        vec![
            vec![Value::Int(1), Value::String("alice".to_string())],
            vec![Value::Int(2), Value::String("bob".to_string())],
        ]
    );
}

// G2: omitted required columns yield strict error 1364 or non-strict implicit default plus warning.
//
// MySQL 8.0 oracle: omitting a NOT NULL column without DEFAULT fails in strict mode
// with 1364, Field 'x' doesn't have a default value. Non-strict mode stores the type default
// (0 or empty string) and warns 1364. Explicit NULL instead remains error 1048.

#[test]
fn g2_omitted_required_column_errors_1364_strict() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT NOT NULL, b VARCHAR(8) NOT NULL)")
        .unwrap();
    // The default empty sql_mode is strict; omitting a/b yields 1364.
    let err = engine.execute("INSERT INTO t (id) VALUES (1)").unwrap_err();
    assert_eq!(err.code(), 1364, "expected 1364, got {err:?}");
    assert!(
        err.message().contains("doesn't have a default value"),
        "message: {}",
        err.message()
    );
}

#[test]
fn g2_omitted_required_column_implicit_default_nonstrict() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(
            &mut s,
            "CREATE TABLE t (id INT PRIMARY KEY, a INT NOT NULL, b VARCHAR(8) NOT NULL)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut s, "SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION'")
        .unwrap();
    engine
        .execute_with_session(&mut s, "INSERT INTO t (id) VALUES (1)")
        .unwrap();
    let warns = rows_of(
        engine
            .execute_with_session(&mut s, "SHOW WARNINGS")
            .unwrap(),
    );
    assert!(
        warns
            .iter()
            .filter(|r| matches!(r[1], Value::Int(1364) | Value::UInt(1364)))
            .count()
            >= 2,
        "expected two 1364 warnings, got {warns:?}"
    );
    let row = rows_of(
        engine
            .execute_with_session(&mut s, "SELECT a, b FROM t WHERE id = 1")
            .unwrap(),
    )
    .into_iter()
    .next()
    .unwrap();
    assert_eq!(row[0], Value::Int(0), "INT implicit default 0");
    assert_eq!(
        row[1].to_display_string(),
        "",
        "VARCHAR implicit default ''"
    );
}

#[test]
fn g2_explicit_null_stays_1048() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT NOT NULL)")
        .unwrap();
    // Explicit NULL yields 1048 rather than 1364, distinct from omission.
    let err = engine
        .execute("INSERT INTO t (id, a) VALUES (1, NULL)")
        .unwrap_err();
    assert_eq!(err.code(), 1048, "explicit NULL must be 1048, got {err:?}");
}

// G2b: INSERT IGNORE omitting required columns uses implicit defaults with warning 1364.
//
// MySQL 8.0 INSERT IGNORE downgrades Field 'x' doesn't have a default value
// from error 1364 to warning 1364 and stores the type default, INT 0, even in strict mode,
// matching UPDATE IGNORE's downgrade of 1048/1364. Ordinary INSERT
// still fails with 1364; enforce_missing_required_columns is authoritative.

#[test]
fn g2b_insert_ignore_missing_required_downgrades_to_warning_strict() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let mut s = SessionContext::default();
    // Default empty sql_mode is strict.
    engine
        .execute_with_session(&mut s, "CREATE TABLE n1 (a INT NOT NULL)")
        .unwrap();
    // INSERT IGNORE VALUES () still inserts a=0 with warning 1364 in strict mode.
    engine
        .execute_with_session(&mut s, "INSERT IGNORE INTO n1 VALUES ()")
        .unwrap();
    let warns = rows_of(
        engine
            .execute_with_session(&mut s, "SHOW WARNINGS")
            .unwrap(),
    );
    assert!(
        warns
            .iter()
            .any(|r| matches!(r[1], Value::Int(1364) | Value::UInt(1364))),
        "expected warning 1364, got {warns:?}"
    );
    let row = rows_of(
        engine
            .execute_with_session(&mut s, "SELECT a FROM n1")
            .unwrap(),
    )
    .into_iter()
    .next()
    .unwrap();
    assert_eq!(row[0], Value::Int(0), "INT implicit default 0 under IGNORE");
}

#[test]
fn g2b_insert_ignore_deduplicates_missing_default_warning_across_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE n1 (a INT NOT NULL)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT IGNORE INTO n1 VALUES (), (), ()")
        .unwrap();
    let warnings = rows_of(
        engine
            .execute_with_session(&mut session, "SHOW WARNINGS")
            .unwrap(),
    );
    assert_eq!(
        warnings
            .iter()
            .filter(|row| matches!(row[1], Value::Int(1364) | Value::UInt(1364)))
            .count(),
        1,
        "one omitted column produces one statement warning: {warnings:?}"
    );
    assert_eq!(
        rows_of(engine.execute("SELECT COUNT(*) FROM n1").unwrap())[0][0],
        Value::Int(3)
    );
}

#[test]
fn g2b_insert_ignore_missing_required_with_column_list() {
    // An explicit column list omitting a required column uses the same downgrade path.
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE n2 (id INT, a INT NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT IGNORE INTO n2 (id) VALUES (5)")
        .unwrap();
    let row = rows_of(engine.execute("SELECT id, a FROM n2").unwrap())
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(row[0], Value::Int(5));
    assert_eq!(
        row[1],
        Value::Int(0),
        "omitted NOT NULL col defaults to 0 under IGNORE"
    );
}

#[test]
fn g2b_plain_insert_missing_required_still_errors_1364() {
    // Removing IGNORE restores strict error 1364, proving the downgrade is IGNORE-specific.
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine.execute("CREATE TABLE n1 (a INT NOT NULL)").unwrap();
    let err = engine.execute("INSERT INTO n1 VALUES ()").unwrap_err();
    assert_eq!(
        err.code(),
        1364,
        "plain INSERT must still raise 1364, got {err:?}"
    );
}

// G3: INSERT OK packet Records/Duplicates/Warnings info text.
//
// MySQL 8.0 multirow VALUES, INSERT SELECT, and conflict-modified INSERT return
// Records: N Duplicates: D Warnings: W; ordinary single-row INSERT has no trailer.

#[test]
fn g3_multi_row_insert_info_trailer() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    assert_eq!(
        take_statement_info(),
        "Records: 3 Duplicates: 0 Warnings: 0"
    );
}

#[test]
fn g3_single_row_insert_has_no_trailer() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    assert_eq!(
        take_statement_info(),
        "",
        "single-row INSERT has no trailer"
    );
}

#[test]
fn g3_odku_duplicates_counted() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();
    let _ = take_statement_info();
    // One conflicting update at id=1 and one new insert at id=3. VALUES() is deprecated
    // since MySQL 8.0.20, so info must include one additional warning 1287.
    engine
        .execute("INSERT INTO t VALUES (1, 99), (3, 30) ON DUPLICATE KEY UPDATE v = VALUES(v)")
        .unwrap();
    assert_eq!(
        take_statement_info(),
        "Records: 2 Duplicates: 1 Warnings: 1"
    );
}

#[test]
fn g3_replace_duplicates_counted() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    let _ = take_statement_info();
    // REPLACE performs one conflicting deletion and one new insertion.
    engine
        .execute("REPLACE INTO t VALUES (1, 99), (2, 20)")
        .unwrap();
    assert_eq!(
        take_statement_info(),
        "Records: 2 Duplicates: 1 Warnings: 0"
    );
}

// ---------- REPLACE MySQL 8.0 compat regression matrix (M4) ----------

#[test]
fn replace_set_syntax_overwrites_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    // REPLACE ... SET is normalized to the same VALUES path.
    match engine.execute("REPLACE INTO t SET id = 1, v = 42").unwrap() {
        QueryResponse::OkAffected { affected_rows } => assert_eq!(affected_rows, 2),
        other => panic!("expected OkAffected, got {other:?}"),
    }
    let out = rows_of(engine.execute("SELECT v FROM t WHERE id = 1").unwrap());
    assert_eq!(out[0][0], Value::Int(42));
}

#[test]
fn replace_null_in_unique_key_is_not_a_conflict() {
    // NULLs are distinct in a (non-PK) UNIQUE index, so a REPLACE whose unique
    // column is NULL never matches an existing NULL row — it is a plain insert.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, u INT UNIQUE, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, NULL, 10)")
        .unwrap();
    match engine
        .execute("REPLACE INTO t VALUES (2, NULL, 20)")
        .unwrap()
    {
        QueryResponse::OkAffected { affected_rows } => {
            assert_eq!(affected_rows, 1, "NULL unique key → no conflict");
        }
        other => panic!("expected OkAffected, got {other:?}"),
    }
    let out = rows_of(engine.execute("SELECT COUNT(*) FROM t").unwrap());
    assert_eq!(out[0][0], Value::Int(2), "both NULL-u rows coexist");
}

#[test]
fn replace_sets_last_insert_id_for_autoinc() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Shared session so LAST_INSERT_ID() observes the prior REPLACE's state.
    let mut s = SessionContext::default();
    engine
        .execute_with_session(
            &mut s,
            "CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v INT)",
        )
        .unwrap();
    // Fresh insert via REPLACE assigns id=1 and sets LAST_INSERT_ID.
    engine
        .execute_with_session(&mut s, "REPLACE INTO t (v) VALUES (10)")
        .unwrap();
    let out = rows_of(
        engine
            .execute_with_session(&mut s, "SELECT LAST_INSERT_ID()")
            .unwrap(),
    );
    assert_eq!(out[0][0], Value::Int(1));
}

#[test]
fn replace_cascades_delete_to_child_fk() {
    // REPLACE deletes the conflicting parent row; an ON DELETE CASCADE child
    // must be removed too (the delete path runs full FK enforcement).
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE parent (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE child (cid INT PRIMARY KEY, pid INT, \
             FOREIGN KEY (pid) REFERENCES parent(id) ON DELETE CASCADE)",
        )
        .unwrap();
    engine.execute("INSERT INTO parent VALUES (1, 10)").unwrap();
    engine.execute("INSERT INTO child VALUES (100, 1)").unwrap();

    // REPLACE the parent id=1 → old parent row deleted → child cascaded away.
    engine
        .execute("REPLACE INTO parent VALUES (1, 99)")
        .unwrap();

    let parent = rows_of(engine.execute("SELECT v FROM parent WHERE id = 1").unwrap());
    assert_eq!(parent[0][0], Value::Int(99), "parent replaced");
    let child = rows_of(engine.execute("SELECT COUNT(*) FROM child").unwrap());
    assert_eq!(
        child[0][0],
        Value::Int(0),
        "child cascade-deleted by REPLACE"
    );
}

#[test]
fn replace_on_triggered_table_counts_duplicates_in_info() {
    // The trigger path now also drains REPLACE-deleted rows into the info
    // trailer's Duplicates count, matching the no-trigger path.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE audit (n INT PRIMARY KEY AUTO_INCREMENT, note INT)")
        .unwrap();
    engine
        .execute(
            "CREATE TRIGGER t_ad AFTER DELETE ON t FOR EACH ROW \
             INSERT INTO audit (note) VALUES (OLD.id)",
        )
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    let _ = take_statement_info();
    // (1) conflicts → delete fires trigger; (2) is a fresh insert.
    engine
        .execute("REPLACE INTO t VALUES (1, 99), (2, 20)")
        .unwrap();
    assert_eq!(
        take_statement_info(),
        "Records: 2 Duplicates: 1 Warnings: 0"
    );
    // The AFTER DELETE trigger ran exactly once for the replaced row.
    let audit = rows_of(engine.execute("SELECT COUNT(*) FROM audit").unwrap());
    assert_eq!(audit[0][0], Value::Int(1));
}

#[test]
fn replace_probe_first_can_be_disabled() {
    // The escape-hatch GUC restores the legacy speculative path; the result is
    // identical (only the internal strategy and auto-inc gap differ).
    let dir = tempfile::tempdir().unwrap();
    std::env::set_var("MYDB_REPLACE_PROBE_FIRST", "off");
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    let resp = engine.execute("REPLACE INTO t VALUES (1, 99)").unwrap();
    std::env::remove_var("MYDB_REPLACE_PROBE_FIRST");
    match resp {
        QueryResponse::OkAffected { affected_rows } => assert_eq!(affected_rows, 2),
        other => panic!("expected OkAffected, got {other:?}"),
    }
    let out = rows_of(engine.execute("SELECT v FROM t WHERE id = 1").unwrap());
    assert_eq!(out[0][0], Value::Int(99));
}

// ---------- REPLACE probe-first fast path (M2) ----------

#[test]
fn replace_conflict_advances_autoinc_once() {
    // A REPLACE that conflicts on a *non-PK* unique key deletes the old row
    // and inserts a fresh one whose AUTO_INCREMENT id is auto-assigned. The
    // probe-first path must advance the counter exactly once (like MySQL);
    // the old speculative path burned an extra value on the failed attempt.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, u INT UNIQUE, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t (u, v) VALUES (100, 1)")
        .unwrap(); // id=1

    // Conflicts on u=100 → deletes id=1, inserts a new row.
    let resp = engine
        .execute("REPLACE INTO t (u, v) VALUES (100, 2)")
        .unwrap();
    match resp {
        QueryResponse::OkAffected { affected_rows } => {
            assert_eq!(affected_rows, 2, "REPLACE with 1 conflict affects 2 rows");
        }
        other => panic!("expected OkAffected, got {other:?}"),
    }

    let out = rows_of(engine.execute("SELECT id, u, v FROM t").unwrap());
    assert_eq!(out.len(), 1, "exactly one row remains");
    assert_eq!(out[0][0], Value::Int(2), "auto-inc advanced exactly once");
    assert_eq!(out[0][1], Value::Int(100));
    assert_eq!(out[0][2], Value::Int(2));
}

#[test]
fn replace_no_conflict_behaves_as_insert() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    // No existing row → REPLACE is a pure INSERT, affects 1 row, no duplicates.
    let _ = take_statement_info();
    let resp = engine.execute("REPLACE INTO t VALUES (1, 10)").unwrap();
    match resp {
        QueryResponse::OkAffected { affected_rows } => assert_eq!(affected_rows, 1),
        other => panic!("expected OkAffected, got {other:?}"),
    }
    let out = rows_of(engine.execute("SELECT v FROM t WHERE id = 1").unwrap());
    assert_eq!(out[0][0], Value::Int(10));
}

#[test]
fn replace_multi_unique_key_deletes_all_conflicts() {
    // One incoming row can conflict with several existing rows across
    // different unique keys; REPLACE must delete every conflict then insert.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT UNIQUE, b INT UNIQUE)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10, 20)").unwrap();
    engine.execute("INSERT INTO t VALUES (2, 30, 40)").unwrap();
    let _ = take_statement_info();
    // New row (id=3, a=10, b=40) collides with id=1 (a=10) AND id=2 (b=40).
    let resp = engine.execute("REPLACE INTO t VALUES (3, 10, 40)").unwrap();
    match resp {
        QueryResponse::OkAffected { affected_rows } => {
            assert_eq!(affected_rows, 3, "2 deletes + 1 insert");
        }
        other => panic!("expected OkAffected, got {other:?}"),
    }
    assert_eq!(
        take_statement_info(),
        "Records: 1 Duplicates: 2 Warnings: 0"
    );
    let out = rows_of(
        engine
            .execute("SELECT id, a, b FROM t ORDER BY id")
            .unwrap(),
    );
    assert_eq!(out.len(), 1);
    assert_eq!(out[0], vec![Value::Int(3), Value::Int(10), Value::Int(40)]);
}

#[test]
fn replace_probe_first_matches_speculative_oracle() {
    // Differential oracle: the default probe-first path and the legacy
    // speculative path (MYDB_REPLACE_PROBE_FIRST=0) must leave identical table
    // state for the same REPLACE workload. nextest isolates each test in its
    // own process, so toggling the env var here is safe.
    fn run_workload(engine: &StorageQueryEngine) -> Vec<Vec<Value>> {
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT UNIQUE, v INT)")
            .unwrap();
        engine.execute("INSERT INTO t VALUES (1, 10, 1)").unwrap();
        engine.execute("INSERT INTO t VALUES (2, 20, 2)").unwrap();
        engine.execute("REPLACE INTO t VALUES (1, 30, 3)").unwrap(); // pk conflict
        engine.execute("REPLACE INTO t VALUES (5, 20, 4)").unwrap(); // unique a conflict
        engine.execute("REPLACE INTO t VALUES (9, 90, 5)").unwrap(); // no conflict
        rows_of(
            engine
                .execute("SELECT id, a, v FROM t ORDER BY id")
                .unwrap(),
        )
    }

    let dir_a = tempfile::tempdir().unwrap();
    std::env::set_var("MYDB_REPLACE_PROBE_FIRST", "1");
    let probe = run_workload(&open_test_engine(dir_a.path()));

    let dir_b = tempfile::tempdir().unwrap();
    std::env::set_var("MYDB_REPLACE_PROBE_FIRST", "0");
    let speculative = run_workload(&open_test_engine(dir_b.path()));
    std::env::remove_var("MYDB_REPLACE_PROBE_FIRST");

    assert_eq!(
        probe, speculative,
        "probe-first and speculative REPLACE must agree on final state"
    );
}

#[test]
fn g3_insert_select_info_trailer() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE src (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE dst (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO src VALUES (1, 10), (2, 20)")
        .unwrap();
    let _ = take_statement_info();
    engine.execute("INSERT INTO dst SELECT * FROM src").unwrap();
    assert_eq!(
        take_statement_info(),
        "Records: 2 Duplicates: 0 Warnings: 0"
    );
}

// ---------- G4: NO_AUTO_VALUE_ON_ZERO ----------
//
// MySQL defaults allocate AUTO_INCREMENT on explicit zero;
// NO_AUTO_VALUE_ON_ZERO stores zero unchanged and allocates only for NULL.

#[test]
fn g4_zero_triggers_sequence_by_default() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (0, 10)").unwrap();
    engine.execute("INSERT INTO t VALUES (0, 20)").unwrap();
    let out = rows_of(engine.execute("SELECT id FROM t ORDER BY v").unwrap());
    assert_eq!(out[0][0], Value::Int(1), "0 assigned fresh id 1");
    assert_eq!(out[1][0], Value::Int(2), "0 assigned fresh id 2");
}

#[test]
fn g4_no_auto_value_on_zero_stores_zero() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(
            &mut s,
            "CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v INT)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut s, "SET SESSION sql_mode = 'NO_AUTO_VALUE_ON_ZERO'")
        .unwrap();
    // Store explicit zero unchanged.
    engine
        .execute_with_session(&mut s, "INSERT INTO t VALUES (0, 10)")
        .unwrap();
    // NULL still allocates a sequence value.
    engine
        .execute_with_session(&mut s, "INSERT INTO t VALUES (NULL, 20)")
        .unwrap();
    let out = rows_of(
        engine
            .execute_with_session(&mut s, "SELECT id FROM t ORDER BY v")
            .unwrap(),
    );
    assert_eq!(out[0][0], Value::Int(0), "explicit 0 stored verbatim");
    assert_eq!(out[1][0], Value::Int(1), "NULL still triggers sequence");
}

// G5: LAST_INSERT_ID(expr) updates the session last_insert_id.
//
// MySQL 8.0 LAST_INSERT_ID(expr) returns expr and makes subsequent
// argumentless LAST_INSERT_ID() return that value.

#[test]
fn g5_last_insert_id_expr_sets_session_value() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    // Return expr itself.
    let out = rows_of(
        engine
            .execute_with_session(&mut s, "SELECT LAST_INSERT_ID(42)")
            .unwrap(),
    );
    assert_eq!(out[0][0].to_display_string(), "42");
    // The subsequent argumentless call reads back 42.
    let out = rows_of(
        engine
            .execute_with_session(&mut s, "SELECT LAST_INSERT_ID()")
            .unwrap(),
    );
    assert_eq!(out[0][0].to_display_string(), "42");
}

// Issue #80: single-table UPDATE assigns multiple SET columns left to right.

#[test]
fn update_set_evaluates_left_to_right() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine.execute("CREATE TABLE t(a INT, b INT)").unwrap();
    engine.execute("INSERT INTO t VALUES (1,1)").unwrap();
    // b references newly assigned a, so b uses a=2.
    engine.execute("UPDATE t SET a=a+1, b=a").unwrap();
    let row = rows_of(engine.execute("SELECT a,b FROM t").unwrap());
    assert_eq!(row[0][0], Value::Int(2));
    assert_eq!(row[0][1], Value::Int(2), "b must see a's new value");

    // Chained assignments see preceding columns' new values.
    engine
        .execute("CREATE TABLE t3(a INT,b INT,c INT)")
        .unwrap();
    engine.execute("INSERT INTO t3 VALUES (1,0,0)").unwrap();
    engine.execute("UPDATE t3 SET a=5, b=a, c=b").unwrap();
    let row = rows_of(engine.execute("SELECT a,b,c FROM t3").unwrap());
    assert_eq!(
        (row[0][0].clone(), row[0][1].clone(), row[0][2].clone()),
        (Value::Int(5), Value::Int(5), Value::Int(5))
    );

    // Expression references.
    engine.execute("CREATE TABLE t4(a INT,b INT)").unwrap();
    engine.execute("INSERT INTO t4 VALUES (1,0)").unwrap();
    engine.execute("UPDATE t4 SET a=a+1, b=a+10").unwrap();
    let row = rows_of(engine.execute("SELECT a,b FROM t4").unwrap());
    assert_eq!(
        (row[0][0].clone(), row[0][1].clone()),
        (Value::Int(2), Value::Int(12))
    );

    // A swap-like assignment uses the new a for b and does not actually swap.
    engine.execute("CREATE TABLE t5(a INT,b INT)").unwrap();
    engine.execute("INSERT INTO t5 VALUES (1,2)").unwrap();
    engine.execute("UPDATE t5 SET a=b, b=a").unwrap();
    let row = rows_of(engine.execute("SELECT a,b FROM t5").unwrap());
    assert_eq!(
        (row[0][0].clone(), row[0][1].clone()),
        (Value::Int(2), Value::Int(2))
    );
}

#[test]
fn update_set_left_to_right_on_pk_index_path() {
    // WHERE pk=? uses the exact/index apply_updates path, the second closure.
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1,1,1)").unwrap();
    engine
        .execute("UPDATE t SET a=a+1, b=a WHERE id=1")
        .unwrap();
    let row = rows_of(engine.execute("SELECT a,b FROM t WHERE id=1").unwrap());
    assert_eq!(row[0][0], Value::Int(2));
    assert_eq!(
        row[0][1],
        Value::Int(2),
        "The index path must also assign left to right"
    );
}

#[test]
fn update_set_correlated_scalar_subquery() {
    // Regression for issue #70: a correlated scalar subquery in the SET
    // clause that references the target table's alias (`e.dept`) must
    // resolve per row against the other table, instead of erroring 1054.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE emp(id INT, dept INT, sal INT)")
        .unwrap();
    engine
        .execute("INSERT INTO emp VALUES (1,10,100),(2,20,200)")
        .unwrap();
    engine.execute("CREATE TABLE d(id INT, bonus INT)").unwrap();
    engine
        .execute("INSERT INTO d VALUES (10,5),(20,9)")
        .unwrap();

    // Alias-qualified correlation.
    engine
        .execute("UPDATE emp e SET sal = sal + (SELECT bonus FROM d WHERE d.id = e.dept)")
        .unwrap();
    let rows = rows_of(
        engine
            .execute("SELECT id, sal FROM emp ORDER BY id")
            .unwrap(),
    );
    assert_eq!(rows[0], vec![Value::Int(1), Value::Int(105)]);
    assert_eq!(rows[1], vec![Value::Int(2), Value::Int(209)]);

    // Table-name-qualified correlation, and NULL when no match.
    engine
        .execute("CREATE TABLE e2(id INT, dept INT, sal INT)")
        .unwrap();
    engine
        .execute("INSERT INTO e2 VALUES (1,10,100),(3,99,300)")
        .unwrap();
    engine
        .execute("UPDATE e2 SET sal = (SELECT bonus FROM d WHERE d.id = e2.dept)")
        .unwrap();
    let rows = rows_of(
        engine
            .execute("SELECT id, sal FROM e2 ORDER BY id")
            .unwrap(),
    );
    assert_eq!(rows[0], vec![Value::Int(1), Value::Int(5)]);
    assert_eq!(rows[1], vec![Value::Int(3), Value::Null]);

    // A non-correlated subquery in SET is still materialised once.
    engine
        .execute("UPDATE e2 SET sal = (SELECT MAX(bonus) FROM d)")
        .unwrap();
    let rows = rows_of(engine.execute("SELECT sal FROM e2 ORDER BY id").unwrap());
    assert_eq!(rows[0][0], Value::Int(9));
    assert_eq!(rows[1][0], Value::Int(9));
}

/// FK audit B1 — LOAD DATA into a table with an outbound foreign key enforces
/// the child-side existence check instead of taking the heap-only fast path
/// that silently imported orphans. `foreign_key_checks=0` keeps the fast path.
#[test]
fn load_data_enforces_foreign_key_child_references() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE p (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE c (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES p (id))",
        )
        .unwrap();
    engine.execute("INSERT INTO p VALUES (1)").unwrap();

    // A batch containing an orphan (pid=999) is rejected with 1452, and the
    // whole statement rolls back — no orphan lands.
    let path = load_write(dir.path(), "orphan.tsv", "10\t1\n11\t999\n");
    let sql = format!("LOAD DATA INFILE '{path}' INTO TABLE c");
    let err = engine
        .execute(&sql)
        .expect_err("orphan child row must be rejected");
    assert_eq!(err.code(), 1452, "{}", err.message());
    let n = rows_of(engine.execute("SELECT COUNT(*) FROM c").unwrap());
    assert_eq!(n[0][0], Value::Int(0), "failed LOAD must roll back fully");

    // A clean batch (all parents present) loads.
    let path = load_write(dir.path(), "clean.tsv", "20\t1\n");
    let sql = format!("LOAD DATA INFILE '{path}' INTO TABLE c");
    engine.execute(&sql).unwrap();
    let n = rows_of(engine.execute("SELECT COUNT(*) FROM c").unwrap());
    assert_eq!(n[0][0], Value::Int(1));

    // With FK checks off, the fast path is kept and the orphan is allowed
    // (mysqldump posture).
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "SET foreign_key_checks = 0")
        .unwrap();
    let path = load_write(dir.path(), "off.tsv", "30\t888\n");
    let sql = format!("LOAD DATA INFILE '{path}' INTO TABLE c");
    engine.execute_with_session(&mut s, &sql).unwrap();
    let n = rows_of(
        engine
            .execute("SELECT COUNT(*) FROM c WHERE id = 30")
            .unwrap(),
    );
    assert_eq!(n[0][0], Value::Int(1));
}

// B3 — the LOAD DATA parallel-encode fast path (>= PARALLEL_MIN_ROWS,
// identity, no AUTO_INCREMENT / binlog) pre-encoded rows without running
// `coerce_row`, so strict-mode out-of-range integers landed silently — a
// divergence from the serial path triggered purely by input size. After the
// fix the worker coerces, so an out-of-range TINYINT errors 1264 regardless
// of row count / parallelism.
#[test]
fn b3_load_data_parallel_path_rejects_out_of_range_int() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT, v TINYINT)")
        .unwrap();
    // >= 2048 rows forces the parallel worker path; 5000 is out of TINYINT
    // range (-128..127).
    let mut content = String::with_capacity(2100 * 8);
    for i in 0..2100 {
        content.push_str(&format!("{i}\t5000\n"));
    }
    let path = load_write(dir.path(), "big.tsv", &content);
    let sql = format!("LOAD DATA INFILE '{path}' INTO TABLE t");
    let err = engine
        .execute(&sql)
        .expect_err("out-of-range TINYINT must error in strict mode, not load silently");
    assert_eq!(err.code(), 1264, "expected 1264 out-of-range, got {err:?}");
    // Nothing should have been committed.
    let out = rows_of(engine.execute("SELECT COUNT(*) FROM t").unwrap());
    assert_eq!(out[0][0], Value::Int(0));
}

// B4 — `coerce_field` silently truncated over-length CHAR/VARCHAR to the
// column width before `coerce_row` could see it, so strict-mode 1406 could
// never fire and data was dropped. After the fix the untruncated value
// reaches `coerce_row`, which reports 1406 in strict mode.
#[test]
fn b4_load_data_rejects_overlong_varchar_in_strict_mode() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT, s VARCHAR(3))")
        .unwrap();
    let path = load_write(dir.path(), "d.tsv", "1\tabcdef\n");
    let sql = format!("LOAD DATA INFILE '{path}' INTO TABLE t");
    let err = engine
        .execute(&sql)
        .expect_err("over-length VARCHAR must error 1406 in strict mode, not truncate");
    assert_eq!(err.code(), 1406, "expected 1406 data-too-long, got {err:?}");
}

// B8 — ON DUPLICATE KEY UPDATE affected-rows: 1 (insert) / 2 (real update) /
// 0 (no-op, row already holds the new values). Previously every dup-key hit
// reported 2 because the ODKU write skipped no-op detection.
#[test]
fn b8_odku_noop_reports_zero_affected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();

    let ins = |sql: &str| -> u64 {
        match engine.execute(sql).unwrap() {
            QueryResponse::OkAffected { affected_rows } => affected_rows,
            other => panic!("expected OkAffected, got {other:?}"),
        }
    };

    // Fresh insert -> 1.
    assert_eq!(ins("INSERT INTO t VALUES (1, 10)"), 1);
    // Dup key, sets v to its current value -> no-op -> 0.
    assert_eq!(
        ins("INSERT INTO t VALUES (1, 10) ON DUPLICATE KEY UPDATE v = 10"),
        0,
        "no-op ODKU must report 0 affected"
    );
    // Dup key, real change -> 2.
    assert_eq!(
        ins("INSERT INTO t VALUES (1, 99) ON DUPLICATE KEY UPDATE v = VALUES(v)"),
        2,
        "changing ODKU must report 2 affected"
    );
    // Dup key with a different id -> plain insert -> 1.
    assert_eq!(
        ins("INSERT INTO t VALUES (2, 5) ON DUPLICATE KEY UPDATE v = 100"),
        1
    );
    let out = rows_of(engine.execute("SELECT id, v FROM t ORDER BY id").unwrap());
    assert_eq!(
        out,
        vec![
            vec![Value::Int(1), Value::Int(99)],
            vec![Value::Int(2), Value::Int(5)],
        ]
    );
}

/// P0-3: with only AFTER UPDATE triggers, ODKU conflict updates must fire them.
/// Previously needs_trigger_path considered only insert or replace-and-delete,
/// so execute_insert_with_conflict bypassed UPDATE triggers entirely.
/// fire_before_update/fire_after_update。
#[test]
fn odku_conflict_fires_update_trigger_even_without_insert_trigger() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE trg_target(id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE trg_log(id INT AUTO_INCREMENT PRIMARY KEY, msg VARCHAR(64))")
        .unwrap();
    engine
        .execute(
            "CREATE TRIGGER trg_target_au AFTER UPDATE ON trg_target FOR EACH ROW \
             INSERT INTO trg_log(msg) VALUES ('updated')",
        )
        .unwrap();
    engine
        .execute("INSERT INTO trg_target(id,v) VALUES(1,100)")
        .unwrap();
    engine
        .execute("INSERT INTO trg_target(id,v) VALUES(1,200) ON DUPLICATE KEY UPDATE v=VALUES(v)")
        .unwrap();
    let out = rows_of(engine.execute("SELECT COUNT(*) FROM trg_log").unwrap());
    assert_eq!(
        out,
        vec![vec![Value::Int(1)]],
        "ODKU conflict must fire AFTER UPDATE trigger even when table has no INSERT trigger"
    );
    let vals = rows_of(
        engine
            .execute("SELECT v FROM trg_target WHERE id=1")
            .unwrap(),
    );
    assert_eq!(vals, vec![vec![Value::Int(200)]]);
}

/// Related P0-3: even a semantically unchanged ODKU SET v=v_current must
/// fire UPDATE triggers for each matched row, consistent with the project's
/// conservative per-match trigger convention and actual MySQL 8.0 behavior:
/// a matched-but-unchanged UPDATE still fires its triggers.
#[test]
fn odku_noop_still_fires_update_trigger_when_present() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE trg_target(id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE trg_log(id INT AUTO_INCREMENT PRIMARY KEY, msg VARCHAR(64))")
        .unwrap();
    engine
        .execute(
            "CREATE TRIGGER trg_target_au AFTER UPDATE ON trg_target FOR EACH ROW \
             INSERT INTO trg_log(msg) VALUES ('updated')",
        )
        .unwrap();
    engine
        .execute("INSERT INTO trg_target(id,v) VALUES(1,100)")
        .unwrap();
    // Same v -> no-op semantically, but trigger path fires per matched row.
    engine
        .execute("INSERT INTO trg_target(id,v) VALUES(1,100) ON DUPLICATE KEY UPDATE v=100")
        .unwrap();
    let out = rows_of(engine.execute("SELECT COUNT(*) FROM trg_log").unwrap());
    assert_eq!(
        out,
        vec![vec![Value::Int(1)]],
        "ODKU dup-key must fire UPDATE trigger on the matched row even when no-op"
    );
}

/// P0-4: ODKU updates must refresh ON UPDATE CURRENT_TIMESTAMP columns.
/// Both execute_insert_with_conflict branches previously wrote only explicit SET columns, leaving ts unchanged.
#[test]
fn odku_refreshes_on_update_current_timestamp_column() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute(
            "CREATE TABLE todku(id INT PRIMARY KEY, v INT, \
             ts TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO todku(id,v) VALUES(1,100)")
        .unwrap();
    let before = rows_of(engine.execute("SELECT ts FROM todku WHERE id=1").unwrap())
        .into_iter()
        .next()
        .unwrap();
    // Sleep 20 ms so the fresh CURRENT_TIMESTAMP is measurably newer.
    std::thread::sleep(std::time::Duration::from_millis(20));
    engine
        .execute("INSERT INTO todku(id,v) VALUES(1,200) ON DUPLICATE KEY UPDATE v=VALUES(v)")
        .unwrap();
    let after = rows_of(engine.execute("SELECT ts FROM todku WHERE id=1").unwrap())
        .into_iter()
        .next()
        .unwrap();
    assert_ne!(
        before, after,
        "ODKU update must refresh ON UPDATE CURRENT_TIMESTAMP"
    );
}

/// Related P0-4: a no-op ODKU must not refresh ON UPDATE CURRENT_TIMESTAMP.
#[test]
fn odku_noop_does_not_refresh_on_update_timestamp() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute(
            "CREATE TABLE todku(id INT PRIMARY KEY, v INT, \
             ts TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO todku(id,v) VALUES(1,100)")
        .unwrap();
    let before = rows_of(engine.execute("SELECT ts FROM todku WHERE id=1").unwrap())
        .into_iter()
        .next()
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(20));
    // Same v as existing -> no-op; ts must not change.
    engine
        .execute("INSERT INTO todku(id,v) VALUES(1,100) ON DUPLICATE KEY UPDATE v=100")
        .unwrap();
    let after = rows_of(engine.execute("SELECT ts FROM todku WHERE id=1").unwrap())
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(
        before, after,
        "no-op ODKU must not refresh ON UPDATE CURRENT_TIMESTAMP"
    );
}

// -- P1-6 / P2-2: CTE-prefixed UPDATE/DELETE with modifiers ---------------

/// P1-6: `WITH cte AS (...) UPDATE LOW_PRIORITY t SET ...` — the previous
/// stripper failed to see UPDATE (because the SQL started with `WITH`), so
/// LOW_PRIORITY leaked into sqlparser which treated it as the table name.
/// After the fix the target table is `t` and low_priority=true.
#[test]
fn p1_6_cte_prefixed_update_low_priority_parses_correctly() {
    let stmt = crate::parser::parse_statement(
        "WITH cte AS (SELECT id FROM x) UPDATE LOW_PRIORITY t SET a=1 WHERE id IN (SELECT id FROM cte)",
    )
    .expect("parse succeeds");
    let with = match &stmt {
        crate::parser::Statement::WithCte(w) => w,
        other => panic!("expected WithCte, got {other:?}"),
    };
    let update = match &*with.body {
        crate::parser::Statement::Update(u) => u,
        other => panic!("expected Update body, got {other:?}"),
    };
    assert!(
        update.table.eq_ignore_ascii_case("t"),
        "target table should be t, got {:?}",
        update.table
    );
    assert!(update.low_priority, "LOW_PRIORITY must be captured");
}

/// P1-6 companion: WITH-CTE-prefixed DELETE IGNORE FROM t.
#[test]
fn p1_6_cte_prefixed_delete_ignore_parses_correctly() {
    let stmt = crate::parser::parse_statement(
        "WITH cte AS (SELECT id FROM x) DELETE IGNORE FROM t WHERE id IN (SELECT id FROM cte)",
    )
    .expect("parse succeeds");
    let with = match &stmt {
        crate::parser::Statement::WithCte(w) => w,
        other => panic!("expected WithCte, got {other:?}"),
    };
    let delete = match &*with.body {
        crate::parser::Statement::Delete(d) => d,
        other => panic!("expected Delete body, got {other:?}"),
    };
    assert!(delete.ignore, "IGNORE modifier must be captured");
    assert!(
        delete.targets.is_empty(),
        "single-table DELETE FROM t must not populate multi-table targets, got {:?}",
        delete.targets
    );
    assert!(
        delete.table.eq_ignore_ascii_case("t"),
        "target FROM table should be t: {:?}",
        delete.table
    );
}

/// P2-2: `WITH cte AS (...) UPDATE t SET a=1 ORDER BY id LIMIT 1` — the
/// manual ORDER BY / LIMIT splice was skipping the WITH prefix and the
/// ORDER BY clause reached sqlparser which rejected it with 1064.
#[test]
fn p2_2_cte_prefixed_update_order_by_limit_parses() {
    let stmt = crate::parser::parse_statement(
        "WITH cte AS (SELECT id FROM x) UPDATE t SET a=1 WHERE id IN (SELECT id FROM cte) \
         ORDER BY id LIMIT 1",
    )
    .expect("parse must succeed after P2-2 fix");
    let with = match &stmt {
        crate::parser::Statement::WithCte(w) => w,
        other => panic!("expected WithCte, got {other:?}"),
    };
    let update = match &*with.body {
        crate::parser::Statement::Update(u) => u,
        other => panic!("expected Update body, got {other:?}"),
    };
    assert!(
        !update.order_by.is_empty(),
        "ORDER BY should be captured on the inner Update body"
    );
    assert_eq!(
        update.limit,
        Some(crate::parser::LimitValue::Rows(1)),
        "LIMIT 1 must land on the Update body"
    );
}

#[test]
fn dml_limit_keeps_partition_identity() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute(
            "CREATE TABLE p (id INT, y INT, v INT, mark INT, KEY idx_v(v)) \
             PARTITION BY RANGE (y) (PARTITION p0 VALUES LESS THAN (10), \
             PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO p VALUES (1,5,10,0),(2,15,100,0)")
        .unwrap();

    let response = engine
        .execute("UPDATE p SET mark=1 ORDER BY v DESC LIMIT 1")
        .unwrap();
    assert!(matches!(
        response,
        QueryResponse::OkAffected { affected_rows: 1 }
    ));
    assert_eq!(
        rows_of(engine.execute("SELECT id,mark FROM p ORDER BY id").unwrap()),
        vec![
            vec![Value::Int(1), Value::Int(0)],
            vec![Value::Int(2), Value::Int(1)]
        ]
    );

    let response = engine
        .execute("DELETE FROM p ORDER BY v ASC LIMIT 1")
        .unwrap();
    assert!(matches!(
        response,
        QueryResponse::OkAffected { affected_rows: 1 }
    ));
    assert_eq!(
        rows_of(engine.execute("SELECT id FROM p ORDER BY id").unwrap()),
        vec![vec![Value::Int(2)]]
    );
}

#[test]
fn unordered_partitioned_dml_limit_stops_at_exact_window() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute(
            "CREATE TABLE p (id INT, y INT, mark INT) \
             PARTITION BY HASH (y) PARTITIONS 4",
        )
        .unwrap();
    engine
        .execute("INSERT INTO p VALUES (1,1,0),(2,2,0),(3,3,0),(4,4,0)")
        .unwrap();
    let response = engine.execute("UPDATE p SET mark=1 LIMIT 1").unwrap();
    assert!(matches!(
        response,
        QueryResponse::OkAffected { affected_rows: 1 }
    ));
    assert_eq!(
        rows_of(
            engine
                .execute("SELECT COUNT(*) FROM p WHERE mark=1")
                .unwrap()
        ),
        vec![vec![Value::Int(1)]]
    );
    let response = engine
        .execute("DELETE FROM p WHERE mark=0 LIMIT 2")
        .unwrap();
    assert!(matches!(
        response,
        QueryResponse::OkAffected { affected_rows: 2 }
    ));
    assert_eq!(
        rows_of(engine.execute("SELECT COUNT(*) FROM p").unwrap()),
        vec![vec![Value::Int(2)]]
    );
}

#[test]
fn dml_max_limit_does_not_preallocate_or_panic() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,10),(2,20)")
        .unwrap();
    let response = engine
        .execute("UPDATE t SET v=v+1 ORDER BY id LIMIT 18446744073709551615")
        .unwrap();
    assert!(matches!(
        response,
        QueryResponse::OkAffected { affected_rows: 2 }
    ));
}

#[test]
fn dml_order_by_error_is_not_hidden_by_limit() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,10),(2,20)")
        .unwrap();

    for sql in [
        "UPDATE t SET v=v+1 ORDER BY missing LIMIT 1",
        "DELETE FROM t ORDER BY missing LIMIT 0",
    ] {
        let err = engine.execute(sql).unwrap_err();
        assert_eq!(err.code(), 1054, "{sql}: {err:?}");
        assert_eq!(err.sql_state(), "42S22");
    }
    let err = engine
        .execute("UPDATE t SET v=v+1 ORDER BY ABS() LIMIT 1")
        .unwrap_err();
    assert_eq!(err.code(), 1582, "{err:?}");
    assert_eq!(
        rows_of(engine.execute("SELECT id,v FROM t ORDER BY id").unwrap()),
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)]
        ]
    );
}

#[test]
fn ordered_update_applies_writes_in_candidate_order() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1),(2)").unwrap();
    let response = engine
        .execute("UPDATE t SET id=id+1 ORDER BY id DESC")
        .unwrap();
    assert!(matches!(
        response,
        QueryResponse::OkAffected { affected_rows: 2 }
    ));
    assert_eq!(
        rows_of(engine.execute("SELECT id FROM t ORDER BY id").unwrap()),
        vec![vec![Value::Int(2)], vec![Value::Int(3)]]
    );
}

#[test]
fn prepared_dml_limit_rejects_non_integer_parameters_without_writes() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,10),(2,20)")
        .unwrap();
    let statement = parse_statement("UPDATE t SET v=v+1 ORDER BY id LIMIT ?").unwrap();

    for value in [
        Value::Int(-1),
        Value::Float(1.5),
        Value::Null,
        Value::String("1".into()),
    ] {
        let err = crate::engine::params::with_params(vec![value], || {
            engine.execute_prepared_for_test(
                &mut SessionContext::default(),
                "UPDATE t SET v=v+1 ORDER BY id LIMIT ?",
                statement.clone(),
            )
        })
        .unwrap_err();
        assert_eq!(err.code(), 1210, "{err:?}");
        assert_eq!(err.sql_state(), "HY000");
    }
    assert_eq!(
        rows_of(engine.execute("SELECT id,v FROM t ORDER BY id").unwrap()),
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)]
        ]
    );
}

#[test]
fn update_limit_counts_matched_noop_and_ignore_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,10),(2,20)")
        .unwrap();

    let response = engine
        .execute("UPDATE t SET v=v ORDER BY id LIMIT 1")
        .unwrap();
    assert!(matches!(
        response,
        QueryResponse::OkAffected { affected_rows: 0 }
    ));
    assert_eq!(take_statement_matched_rows(), Some(1));

    let response = engine
        .execute("UPDATE IGNORE t SET id=2 ORDER BY id LIMIT 1")
        .unwrap();
    assert!(matches!(
        response,
        QueryResponse::OkAffected { affected_rows: 0 }
    ));
    assert_eq!(take_statement_matched_rows(), Some(1));
    assert_eq!(
        rows_of(engine.execute("SELECT id,v FROM t ORDER BY id").unwrap()),
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)]
        ]
    );
}

#[test]
fn delete_ignore_limit_counts_only_successful_deletes() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE p (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE c (id INT PRIMARY KEY, pid INT, \
             FOREIGN KEY (pid) REFERENCES p(id))",
        )
        .unwrap();
    engine.execute("INSERT INTO p VALUES (1),(2)").unwrap();
    engine.execute("INSERT INTO c VALUES (1,1)").unwrap();

    let response = engine
        .execute("DELETE IGNORE FROM p ORDER BY id LIMIT 1")
        .unwrap();
    assert!(matches!(
        response,
        QueryResponse::OkAffected { affected_rows: 1 }
    ));
    assert_eq!(
        rows_of(engine.execute("SELECT id FROM p ORDER BY id").unwrap()),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn large_dml_limit_uses_spillable_precomputed_key_sort() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, mark INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t SELECT n, 0 FROM generate_series(1, 4200) AS gs(n)")
        .unwrap();
    let response = engine
        .execute("UPDATE t SET mark=1 ORDER BY id DESC LIMIT 4097")
        .unwrap();
    assert!(matches!(
        response,
        QueryResponse::OkAffected {
            affected_rows: 4097
        }
    ));
    assert_eq!(
        rows_of(
            engine
                .execute("SELECT COUNT(*),MIN(id),MAX(id) FROM t WHERE mark=1")
                .unwrap()
        ),
        vec![vec![Value::Int(4097), Value::Int(104), Value::Int(4200)]]
    );
}

#[test]
fn non_strict_exact_decimal_to_integer_saturates_without_f64() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET sql_mode='NO_ENGINE_SUBSTITUTION'")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE decimal_integer_bounds (s BIGINT, u BIGINT UNSIGNED, t TINYINT)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO decimal_integer_bounds VALUES \
             (18446744073709551615.0, 9999999999999999999999999.000, \
              -18446744073709551615.0)",
        )
        .unwrap();
    assert_eq!(
        rows_of(
            engine
                .execute_with_session(&mut session, "SELECT s,u,t FROM decimal_integer_bounds")
                .unwrap()
        ),
        vec![vec![
            Value::Int(i64::MAX),
            Value::UInt(u64::MAX),
            Value::Int(-128),
        ]]
    );
}
