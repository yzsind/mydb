//! P2-B1: phase 1 core MySQL 8.0 Enterprise data-masking functions.
//!
//! mydb provides mask_inner, mask_outer, mask_pan, mask_pan_relaxed,
//! and mask_ssn for common PAN/SSN PII masking. Remaining generators,
//! including gen_rnd_*, gen_dictionary, and gen_blacklist, are deferred to phase 2.
//!
//! These tests establish:
//! mask_inner preserves m/n outer characters.
//! mask_outer preserves middle characters and masks both ends.
//! Custom mask-character arguments.
//! mask_pan preserves the last four digits.
//! mask_pan_relaxed preserves the first six and last four.
//! mask_ssn preserves the last four.
//! NULL input returns NULL.
//! Short-string boundaries.

use super::support::execute_materialized as exec;
use super::*;

fn one_string(rs: QueryResponse) -> String {
    let QueryResponse::ResultSet(rs) = rs else {
        panic!("expected result set");
    };
    match &rs.rows[0][0] {
        Value::String(s) => s.clone(),
        Value::Null => "<NULL>".to_string(),
        other => panic!("expected string, got {other:?}"),
    }
}

#[test]
fn mask_inner_keeps_outer_chars_by_default() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let rs = exec(
        &engine,
        &mut session,
        "SELECT MASK_INNER('helloworld', 2, 3)",
    )
    .unwrap();
    assert_eq!(one_string(rs), "heXXXXXrld");
}

#[test]
fn mask_inner_custom_masker_char() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let rs = exec(
        &engine,
        &mut session,
        "SELECT MASK_INNER('helloworld', 2, 3, '*')",
    )
    .unwrap();
    assert_eq!(one_string(rs), "he*****rld");
}

#[test]
fn mask_inner_returns_input_when_window_covers_all() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let rs = exec(&engine, &mut session, "SELECT MASK_INNER('abc', 2, 2)").unwrap();
    assert_eq!(one_string(rs), "abc");
}

#[test]
fn mask_outer_masks_outer_keeps_inner() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let rs = exec(
        &engine,
        &mut session,
        "SELECT MASK_OUTER('helloworld', 2, 3)",
    )
    .unwrap();
    assert_eq!(one_string(rs), "XXllowoXXX");
}

#[test]
fn mask_outer_full_mask_when_window_too_wide() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let rs = exec(&engine, &mut session, "SELECT MASK_OUTER('abc', 2, 2)").unwrap();
    assert_eq!(one_string(rs), "XXX");
}

#[test]
fn mask_pan_keeps_last_four() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let rs = exec(&engine, &mut session, "SELECT MASK_PAN('4111111111111234')").unwrap();
    assert_eq!(one_string(rs), "XXXXXXXXXXXX1234");
}

#[test]
fn mask_pan_relaxed_keeps_first_six_and_last_four() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let rs = exec(
        &engine,
        &mut session,
        "SELECT MASK_PAN_RELAXED('4111111111111234')",
    )
    .unwrap();
    assert_eq!(one_string(rs), "411111XXXXXX1234");
}

#[test]
fn mask_ssn_masks_first_five_keeps_last_four() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let rs = exec(&engine, &mut session, "SELECT MASK_SSN('123-45-6789')").unwrap();
    // Length 11, last 4 kept → 'XXXXXXX6789'.
    assert_eq!(one_string(rs), "XXXXXXX6789");
}

#[test]
fn mask_functions_pass_through_null() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "SELECT MASK_INNER(NULL, 1, 1)",
        "SELECT MASK_OUTER(NULL, 1, 1)",
        "SELECT MASK_PAN(NULL)",
        "SELECT MASK_PAN_RELAXED(NULL)",
        "SELECT MASK_SSN(NULL)",
    ] {
        let rs = exec(&engine, &mut session, sql).unwrap();
        assert_eq!(one_string(rs), "<NULL>", "expected NULL for {sql}");
    }
}

#[test]
fn mask_inner_short_input_passthrough() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let rs = exec(&engine, &mut session, "SELECT MASK_INNER('hi', 5, 5)").unwrap();
    assert_eq!(one_string(rs), "hi");
}

#[test]
fn mask_pan_short_input_passthrough() {
    // Length <= keep window (last 4) → no masking.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let rs = exec(&engine, &mut session, "SELECT MASK_PAN('1234')").unwrap();
    assert_eq!(one_string(rs), "1234");
}
