//! P1-C3: implement actual CHECKSUM TABLE behavior.
//!
//! Previously CHECKSUM TABLE t returned 1064.
//! pt-table-checksum and mydb-replica-diff need stable row digests to compare
//! primary/replica data; add CRC32-based checksums. These tests establish:
//!
//! Two output columns, Table and Checksum.
//! Identical data yields deterministic identical checksums.
//! Data changes alter the checksum.
//! Missing tables yield NULL without blocking later tables.
//! Comma-separated multiple tables return one row each.
//! QUICK and EXTENDED options are accepted and ignored.
//! Empty tables yield CRC32's initial value, zero.

use super::support::execute_materialized as exec;
use super::*;

#[test]
fn checksum_table_returns_two_columns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    exec(&engine, &mut session, "INSERT INTO t VALUES (1), (2)").unwrap();

    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "CHECKSUM TABLE t").unwrap()
    else {
        panic!("expected result set");
    };
    let names: Vec<String> = rs.columns.iter().map(|c| c.name.clone()).collect();
    assert_eq!(names, vec!["Table".to_string(), "Checksum".to_string()]);
    assert_eq!(rs.rows.len(), 1);
    assert!(matches!(rs.rows[0][1], Value::Int(_)));
}

#[test]
fn checksum_table_is_deterministic_for_same_data() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, label VARCHAR(32))",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')",
    )
    .unwrap();

    let QueryResponse::ResultSet(rs1) = exec(&engine, &mut session, "CHECKSUM TABLE t").unwrap()
    else {
        panic!("expected result set");
    };
    let QueryResponse::ResultSet(rs2) = exec(&engine, &mut session, "CHECKSUM TABLE t").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs1.rows[0][1], rs2.rows[0][1]);
}

#[test]
fn checksum_table_changes_when_data_changes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    exec(&engine, &mut session, "INSERT INTO t VALUES (1)").unwrap();
    let QueryResponse::ResultSet(rs1) = exec(&engine, &mut session, "CHECKSUM TABLE t").unwrap()
    else {
        panic!("expected result set");
    };
    exec(&engine, &mut session, "INSERT INTO t VALUES (2)").unwrap();
    let QueryResponse::ResultSet(rs2) = exec(&engine, &mut session, "CHECKSUM TABLE t").unwrap()
    else {
        panic!("expected result set");
    };
    assert_ne!(
        rs1.rows[0][1], rs2.rows[0][1],
        "checksum must change after INSERT"
    );
}

#[test]
fn checksum_table_handles_missing_table_with_null() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "CHECKSUM TABLE no_such").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
    assert!(matches!(rs.rows[0][1], Value::Null));
}

#[test]
fn checksum_table_multi_table_returns_row_per_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE a (id INT PRIMARY KEY)").unwrap();
    exec(&engine, &mut session, "CREATE TABLE b (id INT PRIMARY KEY)").unwrap();
    exec(&engine, &mut session, "INSERT INTO a VALUES (1)").unwrap();
    exec(&engine, &mut session, "INSERT INTO b VALUES (1)").unwrap();

    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "CHECKSUM TABLE a, b").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 2);
}

#[test]
fn checksum_table_with_missing_table_still_returns_others() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE present (id INT PRIMARY KEY)",
    )
    .unwrap();
    exec(&engine, &mut session, "INSERT INTO present VALUES (1)").unwrap();

    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "CHECKSUM TABLE present, missing").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 2);
    // Real table comes first, gets a real checksum.
    assert!(matches!(rs.rows[0][1], Value::Int(_)));
    // Missing table gets NULL but is reported (no error).
    assert!(matches!(rs.rows[1][1], Value::Null));
}

#[test]
fn checksum_table_accepts_quick_option() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();

    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "CHECKSUM TABLE t QUICK").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
}

#[test]
fn checksum_table_accepts_extended_option() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();

    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "CHECKSUM TABLE t EXTENDED").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
}

#[test]
fn checksum_table_empty_table_returns_zero() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();

    // Empty table: CRC32 hasher with no input finalizes to 0.
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "CHECKSUM TABLE t").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][1], Value::Int(0));
}

fn checksum(engine: &StorageQueryEngine, session: &mut SessionContext, table: &str) -> Value {
    let QueryResponse::ResultSet(rs) =
        exec(engine, session, &format!("CHECKSUM TABLE {table}")).unwrap()
    else {
        panic!("expected result set");
    };
    rs.rows[0][1].clone()
}

/// Independent oracle: the previous materialized scan and per-field CRC.
fn materialized_checksum(engine: &StorageQueryEngine, table: &str) -> Value {
    let txn = engine.db.begin_read_only_txn().unwrap();
    let rows = engine.db.scan_rows(table, txn).unwrap();
    let mut hasher = crc32fast::Hasher::new();
    for row in rows {
        for (i, value) in row.values.iter().enumerate() {
            if i != 0 {
                hasher.update(&[0]);
            }
            hasher.update(value.to_display_string().as_bytes());
        }
        hasher.update(&[1]);
    }
    engine.db.rollback_txn(txn.txn_id).unwrap();
    Value::Int(hasher.finalize() as i64)
}

#[test]
fn checksum_table_matches_materialized_scan_across_batches_partitions_and_ddl() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for suffix in ["", " PARTITION BY HASH(id) PARTITIONS 3"] {
        exec(&engine, &mut session, &format!(
            "CREATE TABLE wide_rows (id INT PRIMARY KEY, c CHAR(25), payload VARCHAR(2000), n DECIMAL(15,2), d DATE){suffix}"
        )).unwrap();
        // >64 physical pages even within each partition; force range boundaries.
        for batch in 0..8 {
            let rows = (batch * 256..(batch + 1) * 256)
                .map(|i| {
                    format!(
                        "({i}, '中文  ', '{}', -12.34, '2026-09-15')",
                        "x".repeat(1800)
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            exec(
                &engine,
                &mut session,
                &format!("INSERT INTO wide_rows VALUES {rows}"),
            )
            .unwrap();
        }
        assert_eq!(
            checksum(&engine, &mut session, "wide_rows"),
            materialized_checksum(&engine, "test.wide_rows")
        );
        exec(&engine, &mut session, "ALTER TABLE wide_rows ADD COLUMN added CHAR(12) DEFAULT 'default  ', ALGORITHM=INSTANT").unwrap();
        assert_eq!(
            checksum(&engine, &mut session, "wide_rows"),
            materialized_checksum(&engine, "test.wide_rows")
        );
        exec(
            &engine,
            &mut session,
            "DELETE FROM wide_rows WHERE id IN (0,255,256,2047)",
        )
        .unwrap();
        assert_eq!(
            checksum(&engine, &mut session, "wide_rows"),
            materialized_checksum(&engine, "test.wide_rows")
        );
        exec(&engine, &mut session, "DROP TABLE wide_rows").unwrap();
    }
}

#[test]
fn checksum_table_preserves_snapshot_order_and_forwarded_lob_values() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut writer = SessionContext::default();
    exec(
        &engine,
        &mut writer,
        "CREATE TABLE changing (id INT PRIMARY KEY, payload MEDIUMTEXT, n INT) PCTFREE=0",
    )
    .unwrap();
    let rows = (0..200)
        .map(|id| format!("({id}, '{}', {id})", "s".repeat(128)))
        .collect::<Vec<_>>()
        .join(",");
    exec(
        &engine,
        &mut writer,
        &format!("INSERT INTO changing VALUES {rows}"),
    )
    .unwrap();
    let original = materialized_checksum(&engine, "test.changing");
    let mut reader = SessionContext::default();
    exec(
        &engine,
        &mut reader,
        "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
    )
    .unwrap();
    exec(
        &engine,
        &mut reader,
        "START TRANSACTION WITH CONSISTENT SNAPSHOT",
    )
    .unwrap();
    assert_eq!(checksum(&engine, &mut reader, "changing"), original);
    exec(&engine, &mut writer, "BEGIN").unwrap();
    exec(
        &engine,
        &mut writer,
        "UPDATE changing SET payload=REPEAT('g',8000), n=-1 WHERE id=0",
    )
    .unwrap();
    exec(
        &engine,
        &mut writer,
        "UPDATE changing SET payload=REPEAT('z',200000), n=-2 WHERE id=2",
    )
    .unwrap();
    exec(&engine, &mut writer, "DELETE FROM changing WHERE id=1").unwrap();
    exec(
        &engine,
        &mut writer,
        "INSERT INTO changing VALUES (999,'new',999)",
    )
    .unwrap();
    let own_writes = checksum(&engine, &mut writer, "changing");
    assert_ne!(own_writes, original);
    assert_eq!(checksum(&engine, &mut reader, "changing"), original);
    exec(&engine, &mut writer, "COMMIT").unwrap();
    // An undo row near the start must stay there, not be appended at scan end.
    assert_eq!(checksum(&engine, &mut reader, "changing"), original);
    assert_eq!(checksum(&engine, &mut writer, "changing"), own_writes);
    assert_eq!(own_writes, materialized_checksum(&engine, "test.changing"));
    exec(&engine, &mut reader, "COMMIT").unwrap();
    exec(&engine, &mut writer, "BEGIN").unwrap();
    exec(&engine, &mut writer, "UPDATE changing SET n=42 WHERE id=3").unwrap();
    exec(&engine, &mut writer, "ROLLBACK").unwrap();
    assert_eq!(checksum(&engine, &mut writer, "changing"), own_writes);
}

#[test]
fn checksum_table_read_committed_refreshes_between_statements() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut writer = SessionContext::default();
    exec(
        &engine,
        &mut writer,
        "CREATE TABLE changing (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    exec(&engine, &mut writer, "INSERT INTO changing VALUES (1,10)").unwrap();
    let mut reader = SessionContext::default();
    exec(
        &engine,
        &mut reader,
        "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
    )
    .unwrap();
    exec(&engine, &mut reader, "BEGIN").unwrap();
    let original = checksum(&engine, &mut reader, "changing");
    exec(&engine, &mut writer, "UPDATE changing SET v=99 WHERE id=1").unwrap();
    let updated = checksum(&engine, &mut reader, "changing");
    assert_ne!(updated, original);
    assert_eq!(updated, materialized_checksum(&engine, "test.changing"));
    exec(&engine, &mut reader, "COMMIT").unwrap();
}
