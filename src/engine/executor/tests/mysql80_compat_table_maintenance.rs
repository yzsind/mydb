//! P0-C1: implement CHECK/REPAIR/OPTIMIZE TABLE; ANALYZE already exists.
//!
//! Previously CHECK/REPAIR/OPTIMIZE failed with parse error 1064,
//! blocking mysqldump checks, ORM startup probes, and DBA scripts. Tests establish:
//!
//! Four MySQL columns: Table, Op, Msg_type, Msg_text.
//! - CHECK TABLE → status / OK
//! REPAIR TABLE returns a note that repair is unsupported, matching InnoDB.
//! - OPTIMIZE TABLE → online physical rebuild + status / OK
//! Comma-separated multiple tables return one row each.
//! Missing tables return an Error row and a status row.
//! OPTIMIZE LOCAL/NO_WRITE_TO_BINLOG occurs before TABLE and controls binlog.
//! CHECK/REPAIR options follow their respective grammars.

use super::support::execute_materialized as exec;
use super::*;

const EXPECTED_COLUMNS: &[&str] = &["Table", "Op", "Msg_type", "Msg_text"];

#[test]
fn check_table_returns_status_ok_for_existing_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();

    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "CHECK TABLE t").unwrap() else {
        panic!("expected result set");
    };
    let names: Vec<String> = rs.columns.iter().map(|c| c.name.clone()).collect();
    assert_eq!(
        names,
        EXPECTED_COLUMNS
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
    );
    assert_eq!(rs.rows.len(), 1);
    let row = &rs.rows[0];
    assert_eq!(row[1].to_display_string(), "check");
    assert_eq!(row[2].to_display_string(), "status");
    assert_eq!(row[3].to_display_string(), "OK");
}

#[test]
fn check_table_validates_externalized_lob_and_returns_ok() {
    // M6 (docs/lob-management-audit-2026-07.md) — CHECK TABLE now runs a
    // real LOB integrity scan (`.lob` structure + every live row's chain).
    // A healthy table with externalized LOB data must still report OK.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE big (id INT PRIMARY KEY, body LONGTEXT)",
    )
    .unwrap();
    let big = "x".repeat(50_000); // > inline threshold → externalized
    for i in 0..20 {
        exec(
            &engine,
            &mut session,
            &format!("INSERT INTO big VALUES ({i}, '{big}')"),
        )
        .unwrap();
    }
    // Update some rows (new chains) and delete others (freed chains) to
    // exercise the free list, then CHECK must still pass.
    exec(
        &engine,
        &mut session,
        "UPDATE big SET body = REPEAT('y', 60000) WHERE id < 5",
    )
    .unwrap();
    exec(&engine, &mut session, "DELETE FROM big WHERE id >= 15").unwrap();

    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "CHECK TABLE big").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1, "healthy table => single status row");
    let row = &rs.rows[0];
    assert_eq!(row[2].to_display_string(), "status");
    assert_eq!(
        row[3].to_display_string(),
        "OK",
        "healthy externalized-LOB table must CHECK OK, got {:?}",
        row
    );
}

#[test]
fn repair_table_returns_note_not_supported() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();

    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "REPAIR TABLE t").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
    let row = &rs.rows[0];
    assert_eq!(row[1].to_display_string(), "repair");
    assert_eq!(row[2].to_display_string(), "note");
    assert!(
        row[3]
            .to_display_string()
            .contains("doesn't support repair"),
        "got: {}",
        row[3].to_display_string()
    );
}

#[test]
fn optimize_table_returns_status_ok_after_rebuild() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    exec(&engine, &mut session, "INSERT INTO t VALUES (1), (2)").unwrap();

    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "OPTIMIZE TABLE t").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
    let row = &rs.rows[0];
    assert_eq!(row[1].to_display_string(), "optimize");
    assert_eq!(row[2].to_display_string(), "status");
    assert_eq!(row[3].to_display_string(), "OK");
}

#[test]
fn check_table_handles_comma_separated_list() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE a (id INT PRIMARY KEY)").unwrap();
    exec(&engine, &mut session, "CREATE TABLE b (id INT PRIMARY KEY)").unwrap();

    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "CHECK TABLE a, b").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 2);
}

#[test]
fn check_table_unknown_table_emits_error_row() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "CHECK TABLE no_such").unwrap()
    else {
        panic!("expected result set");
    };
    // MySQL emits two rows (Error + status) for an unknown table.
    assert_eq!(rs.rows.len(), 2);
    assert_eq!(rs.rows[0][2].to_display_string(), "Error");
}

#[test]
fn check_table_accepts_and_strips_quick_option() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();

    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "CHECK TABLE t QUICK").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
    assert_eq!(rs.rows[0][3].to_display_string(), "OK");
}

#[test]
fn optimize_table_accepts_no_write_to_binlog_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();

    // The binlog modifier precedes TABLE.
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "OPTIMIZE NO_WRITE_TO_BINLOG TABLE t").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
}
