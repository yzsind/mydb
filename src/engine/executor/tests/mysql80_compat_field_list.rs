//! P2-A1 — COM_FIELD_LIST (`mysql_list_fields`) backing implementation.
//!
//! COM_FIELD_LIST previously had no session.rs dispatch arm and returned
//! 1047 through unknown-command handling. Old mysqldump, Perl DBI, and
//! some ODBC bridges require this schema command, still retained by MySQL 8.0.
//!
//! Call field_list_columns directly to verify packet structure:
//!
//! An empty wildcard returns every non-invisible column.
//! Each result is a valid ColumnDefinition41 packet with the def catalog marker.
//! Wildcard c% returns only columns beginning with c.
//! Missing tables return 1146 ER_NO_SUCH_TABLE.
//! INVISIBLE columns are omitted.

use super::support::execute_materialized as exec;
use super::*;
use crate::engine::api::QueryEngine;

#[test]
fn field_list_returns_one_packet_per_column() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20), age INT)",
    )
    .unwrap();

    let packets = engine
        .field_list_columns(&SessionContext::default(), "t", "")
        .unwrap();
    assert_eq!(packets.len(), 3, "every column gets its own packet");
    // Each packet must start with the "def" catalog tag (len-encoded
    // string "def" = 0x03 0x64 0x65 0x66).
    for (i, p) in packets.iter().enumerate() {
        assert!(
            p.starts_with(&[0x03, b'd', b'e', b'f']),
            "packet {i} does not start with 'def' tag: {:02x?}",
            &p[..p.len().min(8)]
        );
    }
}

#[test]
fn field_list_filters_by_like_wildcard() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT, customer_name VARCHAR(20), customer_age INT, addr VARCHAR(40))",
    )
    .unwrap();

    // `customer%` → matches customer_name and customer_age, not id/addr.
    let packets = engine
        .field_list_columns(&SessionContext::default(), "t", "customer%")
        .unwrap();
    assert_eq!(packets.len(), 2);
}

#[test]
fn field_list_wildcard_underscore_matches_single_char() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (a INT, b INT, c INT, ab INT)",
    )
    .unwrap();

    // `_` matches exactly one character, so `_` matches a/b/c but not ab.
    let packets = engine
        .field_list_columns(&SessionContext::default(), "t", "_")
        .unwrap();
    assert_eq!(packets.len(), 3);
}

#[test]
fn field_list_unknown_table_returns_1146() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let _ = SessionContext::default();

    let err = engine
        .field_list_columns(&SessionContext::default(), "no_such_table", "")
        .unwrap_err();
    assert_eq!(err.code(), 1146);
}

#[test]
fn field_list_skips_invisible_columns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    // INVISIBLE columns are filtered out of mysql_list_fields in MySQL —
    // mirror that. We use a CREATE TABLE form that exercises the
    // visibility flag on the storage schema.
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT, hidden_col INT INVISIBLE, name VARCHAR(8))",
    )
    .unwrap();

    let packets = engine
        .field_list_columns(&SessionContext::default(), "t", "")
        .unwrap();
    // 2 visible columns surface; hidden_col is filtered.
    assert_eq!(packets.len(), 2);
}

#[test]
fn field_list_packet_carries_trailing_default_value() {
    // COM_FIELD_LIST extends ColumnDefinition41 with a lenenc default-value
    // tail. mysql 9.6 cli's auto-rehash parses strictly: a missing tail
    // shifts every subsequent client read by one byte, the client decides
    // the connection is desynced and silently closes it. The next user
    // query then surfaces as ER_LOST_CONNECTION on a fresh socket.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT, name VARCHAR(20) DEFAULT 'anon', age INT DEFAULT 0)",
    )
    .unwrap();

    let packets = engine
        .field_list_columns(&SessionContext::default(), "t", "")
        .unwrap();
    assert_eq!(packets.len(), 3);

    fn read_lenenc(buf: &[u8], pos: &mut usize) -> Option<u64> {
        if *pos >= buf.len() {
            return None;
        }
        let b = buf[*pos];
        *pos += 1;
        match b {
            0..=0xfa => Some(b as u64),
            0xfc => {
                let v = u16::from_le_bytes([buf[*pos], buf[*pos + 1]]);
                *pos += 2;
                Some(v as u64)
            }
            0xfd => {
                let v = (buf[*pos] as u64)
                    | ((buf[*pos + 1] as u64) << 8)
                    | ((buf[*pos + 2] as u64) << 16);
                *pos += 3;
                Some(v)
            }
            0xfe => {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&buf[*pos..*pos + 8]);
                *pos += 8;
                Some(u64::from_le_bytes(arr))
            }
            _ => None,
        }
    }

    let defaults: Vec<String> = packets
        .iter()
        .map(|p| {
            let mut pos = 0;
            // skip catalog/schema/table/org_table/name/org_name (6 lenenc strings)
            for _ in 0..6 {
                let l = read_lenenc(p, &mut pos).expect("lenenc field");
                pos += l as usize;
            }
            assert_eq!(p[pos], 0x0c, "expected 0x0c length-of-fixed marker");
            pos += 1 + 2 + 4 + 1 + 2 + 1 + 2; // collation+length+type+flags+decimals+filler
                                              // trailing default value (COM_FIELD_LIST extension)
            let l = read_lenenc(p, &mut pos).expect("default-value lenenc");
            let s = std::str::from_utf8(&p[pos..pos + l as usize])
                .unwrap()
                .to_string();
            pos += l as usize;
            assert_eq!(pos, p.len(), "no trailing garbage after default value");
            s
        })
        .collect();

    // All three currently report empty — the protocol fix is just about
    // emitting the lenenc tail so the client stays in sync. Surfacing
    // the actual DEFAULT expression is a separate enhancement.
    for d in &defaults {
        assert_eq!(d, "");
    }
}

#[test]
fn field_list_qualified_table_name_routes_to_explicit_database() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE DATABASE other").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE other.foo (x INT, y INT)",
    )
    .unwrap();

    // Pass current_database='test' but table_name='other.foo' — should
    // resolve to other.foo, not test.foo (which doesn't exist).
    let packets = engine
        .field_list_columns(&SessionContext::default(), "other.foo", "")
        .unwrap();
    assert_eq!(packets.len(), 2);
}
