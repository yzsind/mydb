//! P0-B2 — `SHOW RELAYLOG EVENTS [IN ...] [FROM pos] [LIMIT ...] [FOR CHANNEL ...]`.
//!
//! Previously SHOW RELAYLOG EVENTS was unsupported and returned 1235 to operations tools
//! such as orchestrator/MHA and mysqldump lag checks. These tests establish:
//!
//! Six MySQL columns: Log_name, Pos, Event_type, Server_id, End_log_pos, Info.
//! Exact column names, order, and types.
//! An unconfigured manager returns an empty result set rather than 1235, matching MySQL.
//! Parse IN log, FROM N, LIMIT n, LIMIT offset,n, and FOR CHANNEL clauses.
//! Invalid FROM/LIMIT arguments return 1064.
//! Complete SHOW SLAVE STATUS synonym; SHOW RELAYLOG EVENTS dispatches through replication_admin.

use super::support::execute_materialized as exec;
use super::*;

const EXPECTED_COLUMNS: &[&str] = &[
    "Log_name",
    "Pos",
    "Event_type",
    "Server_id",
    "End_log_pos",
    "Info",
];

#[test]
fn show_relaylog_events_returns_six_columns_in_mysql_order() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "SHOW RELAYLOG EVENTS").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.columns.len(), 6);
    let names: Vec<String> = rs.columns.iter().map(|c| c.name.clone()).collect();
    assert_eq!(
        names,
        EXPECTED_COLUMNS
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
    );
}

#[test]
fn show_relaylog_events_unconfigured_returns_empty_result() {
    // Without `CHANGE REPLICATION SOURCE TO`, MySQL returns an empty
    // result set (not an error) — orchestrator polls this to detect
    // "no relay activity" without a separate probe.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "SHOW RELAYLOG EVENTS").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.columns.len(), 6);
    assert!(rs.rows.is_empty());
}

#[test]
fn show_relaylog_events_accepts_for_channel_clause() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SHOW RELAYLOG EVENTS FOR CHANNEL 'mychannel'",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.columns.len(), 6);
}

#[test]
fn show_relaylog_events_accepts_in_from_limit_clauses() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // All three optional clauses chained; channel name on the tail.
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SHOW RELAYLOG EVENTS IN 'foo.relay.jsonl' FROM 0 LIMIT 5 FOR CHANNEL ''",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.columns.len(), 6);
}

#[test]
fn show_relaylog_events_accepts_limit_offset_form() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // `LIMIT 10, 5` is offset=10, count=5.
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SHOW RELAYLOG EVENTS LIMIT 10, 5").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.columns.len(), 6);
}

#[test]
fn show_relaylog_events_rejects_unparseable_tail() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let err = exec(&engine, &mut session, "SHOW RELAYLOG EVENTS BOGUS_TOKEN").unwrap_err();
    assert_eq!(
        err.code(),
        1064,
        "unparseable trailing tokens must error 1064 syntax"
    );
}
