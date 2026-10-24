//! PostgreSQL-style SEQUENCE end-to-end tests (CREATE/ALTER/DROP SEQUENCE +
//! nextval/currval/lastval/setval). See
//! `docs/design/sequence-catalog-durability-replication.md`.
//!
//! These run against an in-memory engine; durability/crash-recovery of the
//! catalog state is covered separately where a strict engine is reopened.

use super::support::execute_materialized as exec;
use super::*;

fn scalar(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> Value {
    let QueryResponse::ResultSet(rs) = exec(engine, session, sql).unwrap() else {
        panic!("expected result set for {sql}");
    };
    assert_eq!(rs.rows.len(), 1, "expected single row from {sql}");
    rs.rows[0][0].clone()
}

fn nextval(engine: &StorageQueryEngine, session: &mut SessionContext, seq: &str) -> i64 {
    let v = scalar(engine, session, &format!("SELECT nextval('{seq}')"));
    match v {
        Value::Int(n) => n,
        Value::UInt(n) => n as i64,
        other => panic!("nextval('{seq}') returned non-integer {other:?}"),
    }
}

#[test]
fn sequence_lastval_checks_identity_and_setval() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE SEQUENCE lastval_identity").unwrap();
    assert_eq!(nextval(&engine, &mut session, "lastval_identity"), 1);
    scalar(
        &engine,
        &mut session,
        "SELECT setval('lastval_identity', 42)",
    );
    assert_eq!(
        scalar(&engine, &mut session, "SELECT lastval()"),
        Value::Int(42)
    );
    exec(&engine, &mut session, "DROP SEQUENCE lastval_identity").unwrap();
    assert!(exec(&engine, &mut session, "SELECT lastval()").is_err());
}

#[test]
fn sequence_owned_by_tracks_rename_and_cascades_column_and_table_drop() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE sequence_owner (id BIGINT, kept INT)",
        "CREATE SEQUENCE owned_sequence OWNED BY sequence_owner.id",
        "RENAME TABLE sequence_owner TO sequence_owner_renamed",
        "ALTER TABLE sequence_owner_renamed RENAME COLUMN id TO renamed_id",
    ] {
        exec(&engine, &mut session, sql).unwrap();
    }
    assert_eq!(nextval(&engine, &mut session, "owned_sequence"), 1);
    exec(
        &engine,
        &mut session,
        "ALTER TABLE sequence_owner_renamed DROP COLUMN renamed_id",
    )
    .unwrap();
    assert!(exec(&engine, &mut session, "SELECT nextval('owned_sequence')").is_err());
    exec(
        &engine,
        &mut session,
        "CREATE SEQUENCE owned_sequence OWNED BY sequence_owner_renamed.kept",
    )
    .unwrap();
    exec(&engine, &mut session, "DROP TABLE sequence_owner_renamed").unwrap();
    assert!(exec(&engine, &mut session, "SELECT nextval('owned_sequence')").is_err());
    assert!(exec(
        &engine,
        &mut session,
        "CREATE SEQUENCE invalid_owner OWNED BY missing_table.id"
    )
    .is_err());
}

#[test]
fn sequence_diagnostics_show_recovery_reservation_and_hide_raw_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut session = SessionContext::default();
    session.status =
        Some(crate::engine::status_counters::SessionStatus::try_new_governed().unwrap());
    exec(
        &engine,
        &mut session,
        "CREATE SEQUENCE diagnostic_sequence CACHE 10",
    )
    .unwrap();
    assert_eq!(nextval(&engine, &mut session, "diagnostic_sequence"), 1);
    assert_eq!(scalar(&engine, &mut session, "SELECT ALLOCATED_VALUE FROM information_schema.mydb_sequences WHERE SEQUENCE_NAME='diagnostic_sequence'"), Value::Int(10));
    assert_eq!(scalar(&engine, &mut session, "SELECT RESUME_VALUE FROM information_schema.mydb_sequences WHERE SEQUENCE_NAME='diagnostic_sequence'"), Value::Int(42));
    assert_eq!(nextval(&engine, &mut session, "diagnostic_sequence"), 2);
    for (name, expected) in [
        ("Mydb_sequence_values", "2"),
        ("Mydb_sequence_cache_hits", "1"),
        ("Mydb_sequence_refills", "1"),
    ] {
        let QueryResponse::ResultSet(result) = exec(
            &engine,
            &mut session,
            &format!("SHOW SESSION STATUS LIKE '{name}'"),
        )
        .unwrap() else {
            panic!("expected sequence status result");
        };
        assert_eq!(
            result.rows,
            vec![vec![
                Value::String(name.into()),
                Value::String(expected.into())
            ]]
        );
        let global = crate::engine::perf_schema::global_status_pairs(engine.database(), None);
        assert!(global
            .iter()
            .any(|(key, value)| key == name && value == "0"));
    }
    assert!(exec(&engine, &mut session, "SELECT * FROM mysql.sequence_values").is_err());
}

#[test]
fn sequence_view_repeated_reads_and_read_only_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE SEQUENCE view_sequence").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE VIEW sequence_view AS SELECT nextval('view_sequence') AS id",
    )
    .unwrap();
    for expected in 1..=10 {
        assert_eq!(
            scalar(&engine, &mut session, "SELECT id FROM sequence_view"),
            Value::Int(expected)
        );
    }
    exec(&engine, &mut session, "START TRANSACTION READ ONLY").unwrap();
    assert_eq!(
        exec(&engine, &mut session, "SELECT nextval('view_sequence')")
            .unwrap_err()
            .code(),
        1792
    );
    assert_eq!(
        exec(&engine, &mut session, "SELECT id FROM sequence_view")
            .unwrap_err()
            .code(),
        1792
    );
    exec(&engine, &mut session, "ROLLBACK").unwrap();
    assert_eq!(nextval(&engine, &mut session, "view_sequence"), 11);
}

#[test]
fn sequence_owned_by_survives_copy_rewrite() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE sequence_copy_owner(id INT, payload INT)",
        "CREATE SEQUENCE copy_owned_seq OWNED BY sequence_copy_owner.id",
        "INSERT INTO sequence_copy_owner VALUES(1, 2)",
        "ALTER TABLE sequence_copy_owner MODIFY COLUMN payload BIGINT, ALGORITHM=COPY",
    ] {
        exec(&engine, &mut session, sql).unwrap();
    }
    assert_eq!(nextval(&engine, &mut session, "copy_owned_seq"), 1);
    exec(&engine, &mut session, "DROP TABLE sequence_copy_owner").unwrap();
    assert!(exec(&engine, &mut session, "SELECT nextval('copy_owned_seq')").is_err());
}

#[test]
fn create_and_nextval_basic() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "CREATE SEQUENCE seq1").unwrap();
    assert_eq!(nextval(&engine, &mut s, "seq1"), 1);
    assert_eq!(nextval(&engine, &mut s, "seq1"), 2);
    assert_eq!(nextval(&engine, &mut s, "seq1"), 3);
}

#[test]
fn start_and_increment() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(
        &engine,
        &mut s,
        "CREATE SEQUENCE s START WITH 100 INCREMENT BY 5",
    )
    .unwrap();
    assert_eq!(nextval(&engine, &mut s, "s"), 100);
    assert_eq!(nextval(&engine, &mut s, "s"), 105);
    assert_eq!(nextval(&engine, &mut s, "s"), 110);
}

#[test]
fn currval_tracks_last_nextval_and_errors_before_first() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "CREATE SEQUENCE s").unwrap();
    // currval before any nextval in this session -> error.
    let err = exec(&engine, &mut s, "SELECT currval('s')").unwrap_err();
    assert_eq!(err.code(), 50013, "currval-before-nextval should be 50013");
    assert_eq!(nextval(&engine, &mut s, "s"), 1);
    assert_eq!(
        scalar(&engine, &mut s, "SELECT currval('s')"),
        Value::Int(1)
    );
    assert_eq!(nextval(&engine, &mut s, "s"), 2);
    assert_eq!(
        scalar(&engine, &mut s, "SELECT currval('s')"),
        Value::Int(2)
    );
}

#[test]
fn lastval_tracks_most_recent_nextval_any_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    let err = exec(&engine, &mut s, "SELECT lastval()").unwrap_err();
    assert_eq!(
        err.code(),
        50013,
        "lastval-before-any-nextval should be 50013"
    );
    exec(&engine, &mut s, "CREATE SEQUENCE a").unwrap();
    exec(&engine, &mut s, "CREATE SEQUENCE b START WITH 50").unwrap();
    assert_eq!(nextval(&engine, &mut s, "a"), 1);
    assert_eq!(scalar(&engine, &mut s, "SELECT lastval()"), Value::Int(1));
    assert_eq!(nextval(&engine, &mut s, "b"), 50);
    assert_eq!(scalar(&engine, &mut s, "SELECT lastval()"), Value::Int(50));
}

#[test]
fn setval_two_and_three_arg() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "CREATE SEQUENCE s").unwrap();
    // setval(s, 42) -> next nextval returns 43, currval becomes 42.
    assert_eq!(
        scalar(&engine, &mut s, "SELECT setval('s', 42)"),
        Value::Int(42)
    );
    assert_eq!(
        scalar(&engine, &mut s, "SELECT currval('s')"),
        Value::Int(42)
    );
    assert_eq!(nextval(&engine, &mut s, "s"), 43);
    // setval(s, 100, false) -> next nextval returns exactly 100.
    assert_eq!(
        scalar(&engine, &mut s, "SELECT setval('s', 100, false)"),
        Value::Int(100)
    );
    assert_eq!(nextval(&engine, &mut s, "s"), 100);
    assert_eq!(nextval(&engine, &mut s, "s"), 101);
}

#[test]
fn no_cycle_boundary_errors() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "CREATE SEQUENCE s MAXVALUE 3").unwrap();
    assert_eq!(nextval(&engine, &mut s, "s"), 1);
    assert_eq!(nextval(&engine, &mut s, "s"), 2);
    assert_eq!(nextval(&engine, &mut s, "s"), 3);
    let err = exec(&engine, &mut s, "SELECT nextval('s')").unwrap_err();
    assert_eq!(err.code(), 50012, "reached max -> 50012");
}

#[test]
fn cycle_wraps_to_min() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(
        &engine,
        &mut s,
        "CREATE SEQUENCE s MINVALUE 1 MAXVALUE 3 CYCLE",
    )
    .unwrap();
    let got: Vec<i64> = (0..7).map(|_| nextval(&engine, &mut s, "s")).collect();
    assert_eq!(got, vec![1, 2, 3, 1, 2, 3, 1]);
}

#[test]
fn descending_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(
        &engine,
        &mut s,
        "CREATE SEQUENCE s INCREMENT BY -1 MINVALUE 1 MAXVALUE 10 START WITH 10",
    )
    .unwrap();
    assert_eq!(nextval(&engine, &mut s, "s"), 10);
    assert_eq!(nextval(&engine, &mut s, "s"), 9);
    assert_eq!(nextval(&engine, &mut s, "s"), 8);
}

#[test]
fn cache_hands_out_distinct_contiguous_values() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "CREATE SEQUENCE s CACHE 10").unwrap();
    let got: Vec<i64> = (0..25).map(|_| nextval(&engine, &mut s, "s")).collect();
    let expected: Vec<i64> = (1..=25).collect();
    assert_eq!(got, expected);
}

#[test]
fn drop_sequence_then_use_errors() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "CREATE SEQUENCE s").unwrap();
    assert_eq!(nextval(&engine, &mut s, "s"), 1);
    exec(&engine, &mut s, "DROP SEQUENCE s").unwrap();
    let err = exec(&engine, &mut s, "SELECT nextval('s')").unwrap_err();
    assert_eq!(err.code(), 50011, "unknown sequence -> 50011");
    // DROP IF EXISTS on a missing sequence is a no-op.
    exec(&engine, &mut s, "DROP SEQUENCE IF EXISTS s").unwrap();
}

#[test]
fn if_not_exists_and_duplicate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "CREATE SEQUENCE s").unwrap();
    let err = exec(&engine, &mut s, "CREATE SEQUENCE s").unwrap_err();
    assert_eq!(err.code(), 50010, "duplicate sequence -> 50010");
    // IF NOT EXISTS swallows the duplicate.
    exec(&engine, &mut s, "CREATE SEQUENCE IF NOT EXISTS s").unwrap();
}

#[test]
fn alter_increment_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "CREATE SEQUENCE s").unwrap();
    assert_eq!(nextval(&engine, &mut s, "s"), 1);
    assert_eq!(nextval(&engine, &mut s, "s"), 2);
    exec(&engine, &mut s, "ALTER SEQUENCE s INCREMENT BY 10").unwrap();
    assert_eq!(nextval(&engine, &mut s, "s"), 12);
    // RESTART WITH resets the live counter; next nextval returns the value.
    exec(&engine, &mut s, "ALTER SEQUENCE s RESTART WITH 100").unwrap();
    assert_eq!(nextval(&engine, &mut s, "s"), 100);
    assert_eq!(nextval(&engine, &mut s, "s"), 110);
    // Bare RESTART goes back to START (1).
    exec(&engine, &mut s, "ALTER SEQUENCE s RESTART").unwrap();
    assert_eq!(nextval(&engine, &mut s, "s"), 1);
}

#[test]
fn alter_rename() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "CREATE SEQUENCE s").unwrap();
    assert_eq!(nextval(&engine, &mut s, "s"), 1);
    exec(&engine, &mut s, "ALTER SEQUENCE s RENAME TO s2").unwrap();
    // Old name gone, new name continues from where it was.
    assert!(exec(&engine, &mut s, "SELECT nextval('s')").is_err());
    assert_eq!(nextval(&engine, &mut s, "s2"), 2);
}

#[test]
fn namespace_conflict_with_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "CREATE TABLE t (id INT)").unwrap();
    let err = exec(&engine, &mut s, "CREATE SEQUENCE t").unwrap_err();
    assert_eq!(err.code(), 1050, "sequence vs existing table -> 1050");
}

#[test]
fn nextval_in_insert_default_column() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "CREATE SEQUENCE s").unwrap();
    exec(
        &engine,
        &mut s,
        "CREATE TABLE t (id BIGINT, label VARCHAR(16))",
    )
    .unwrap();
    exec(
        &engine,
        &mut s,
        "INSERT INTO t VALUES (nextval('s'), 'a'), (nextval('s'), 'b')",
    )
    .unwrap();
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut s, "SELECT id FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    let ids: Vec<i64> = rs
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Int(n) => *n,
            Value::UInt(n) => *n as i64,
            other => panic!("unexpected id {other:?}"),
        })
        .collect();
    assert_eq!(ids, vec![1, 2]);
}

#[test]
fn survives_reopen_without_reusing_values() {
    let dir = tempfile::tempdir().unwrap();
    let max_handed_out;
    {
        let engine = open_strict_test_engine(dir.path());
        let mut s = SessionContext::default();
        exec(&engine, &mut s, "CREATE SEQUENCE s START WITH 1").unwrap();
        let mut last = 0;
        for _ in 0..5 {
            last = nextval(&engine, &mut s, "s");
        }
        max_handed_out = last; // 5
        assert_eq!(max_handed_out, 5);
    }
    // Reopen the same data directory: the definition must survive and the
    // counter must never re-hand a value it already gave out (a crash may
    // skip ahead — never repeat).
    {
        let engine = open_strict_test_engine(dir.path());
        let mut s = SessionContext::default();
        let resumed = nextval(&engine, &mut s, "s");
        assert!(
            resumed > max_handed_out,
            "after reopen nextval {resumed} must exceed last handed-out {max_handed_out}"
        );
    }
}

#[test]
fn setval_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        let mut s = SessionContext::default();
        exec(&engine, &mut s, "CREATE SEQUENCE s").unwrap();
        exec(&engine, &mut s, "SELECT setval('s', 500, true)").unwrap();
    }
    {
        let engine = open_strict_test_engine(dir.path());
        let mut s = SessionContext::default();
        // After setval(500, true), the next nextval is >= 501 (no reuse below).
        let v = nextval(&engine, &mut s, "s");
        assert!(v >= 501, "expected >= 501 after setval(500,true), got {v}");
    }
}

#[test]
fn show_create_sequence_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(
        &engine,
        &mut s,
        "CREATE SEQUENCE s START WITH 5 INCREMENT BY 2 MAXVALUE 100 CYCLE",
    )
    .unwrap();
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut s, "SHOW CREATE SEQUENCE s").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
    let ddl = rs.rows[0][1].to_display_string();
    assert!(ddl.contains("CREATE SEQUENCE `s`"), "got {ddl}");
    assert!(ddl.contains("START WITH 5"), "got {ddl}");
    assert!(ddl.contains("INCREMENT BY 2"), "got {ddl}");
    assert!(ddl.contains("MAXVALUE 100"), "got {ddl}");
    assert!(
        ddl.contains("CYCLE") && !ddl.contains("NO CYCLE"),
        "got {ddl}"
    );
}

#[test]
fn show_sequences_lists_and_filters() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "CREATE SEQUENCE alpha").unwrap();
    exec(&engine, &mut s, "CREATE SEQUENCE beta").unwrap();

    let names = |rs: &crate::engine::api::ResultSet| -> Vec<String> {
        rs.rows.iter().map(|r| r[0].to_display_string()).collect()
    };

    let QueryResponse::ResultSet(rs) = exec(&engine, &mut s, "SHOW SEQUENCES").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(names(&rs), vec!["alpha".to_string(), "beta".to_string()]);

    let QueryResponse::ResultSet(rs) = exec(&engine, &mut s, "SHOW SEQUENCES LIKE 'al%'").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(names(&rs), vec!["alpha".to_string()]);
}

#[test]
fn invalid_options_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    // increment 0
    let err = exec(&engine, &mut s, "CREATE SEQUENCE s INCREMENT BY 0").unwrap_err();
    assert_eq!(err.code(), 50014);
    // min > max
    let err = exec(&engine, &mut s, "CREATE SEQUENCE s2 MINVALUE 10 MAXVALUE 1").unwrap_err();
    assert_eq!(err.code(), 50014);
}
