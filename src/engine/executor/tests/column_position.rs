//! Comprehensive ALTER TABLE FIRST/AFTER column-order regressions (2026-07).
//!
//! Cover column-order design S5 cases T1-T14: CHANGE with FIRST/AFTER, mixed actions,
//! ADD AFTER a dropped column, logical_pos persistence across cold restarts, WRITE_ROWS
//! ordering, atomic rollback, partitions, generated columns, FKs, missing/self AFTER, and MODIFY.
//! invisible + FIRST、ALGORITHM=INSTANT + FIRST/AFTER。
//!
//! Two coordinate systems: `physical_pos` fixes row slot layout at creation,
//! while `logical_pos` controls SHOW/SELECT * order; FIRST/AFTER changes only logical positions.

use super::*;

fn exec_ok(engine: &StorageQueryEngine, sql: &str) {
    engine
        .execute(sql)
        .unwrap_or_else(|err| panic!("expected success for `{sql}`: {err:?}"));
}

fn exec_err(engine: &StorageQueryEngine, sql: &str) -> QueryError {
    match engine.execute(sql) {
        Err(err) => err,
        Ok(_) => panic!("expected error for `{sql}`"),
    }
}

fn column_names(engine: &StorageQueryEngine, table: &str) -> Vec<String> {
    let QueryResponse::ResultSet(rs) = engine
        .execute(&format!("SHOW COLUMNS FROM {table}"))
        .unwrap()
    else {
        panic!("expected SHOW COLUMNS result set");
    };
    rs.rows
        .into_iter()
        .map(|row| row[0].to_display_string())
        .collect()
}

fn select_star(engine: &StorageQueryEngine, table: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(rs) = engine.execute(&format!("SELECT * FROM {table}")).unwrap()
    else {
        panic!("expected SELECT * result set");
    };
    rs.rows
}

fn ordinal_positions(engine: &StorageQueryEngine, table: &str) -> Vec<(String, i64)> {
    let sql = format!(
        "SELECT COLUMN_NAME, ORDINAL_POSITION FROM information_schema.COLUMNS \
         WHERE TABLE_SCHEMA = 'test' AND TABLE_NAME = '{table}' \
         ORDER BY ORDINAL_POSITION"
    );
    let QueryResponse::ResultSet(rs) = engine.execute(&sql).unwrap() else {
        panic!("expected IS.COLUMNS result set");
    };
    rs.rows
        .into_iter()
        .map(|row| {
            let name = row[0].to_display_string();
            let pos = match &row[1] {
                Value::Int(i) => *i,
                Value::UInt(u) => *u as i64,
                other => panic!("unexpected ORDINAL_POSITION: {other:?}"),
            };
            (name, pos)
        })
        .collect()
}

// ---------- T1: CHANGE COLUMN + FIRST/AFTER ----------

/// CHANGE COLUMN renames and moves a column. The parser creates ChangeColumn and MoveColumn
/// actions, with MoveColumn using the new name; sequential dispatch must recognize that name.
#[test]
fn change_column_with_after_moves_renamed_column() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    exec_ok(&engine, "CREATE TABLE t (a INT, b INT, c INT)");
    exec_ok(&engine, "INSERT INTO t VALUES (1, 2, 3)");
    exec_ok(
        &engine,
        "ALTER TABLE t CHANGE COLUMN a a_renamed BIGINT AFTER b",
    );

    assert_eq!(
        column_names(&engine, "t"),
        vec!["b".to_string(), "a_renamed".to_string(), "c".to_string()]
    );
    // Row data is not rewritten: a_renamed remains 1; only display order changes.
    let rows = select_star(&engine, "t");
    assert_eq!(
        rows[0],
        vec![Value::Int(2), Value::Int(1), Value::Int(3)],
        "logical order projection swaps b then a_renamed then c"
    );
}

/// CHANGE COLUMN with FIRST similarly moves the column to the front.
#[test]
fn change_column_with_first_moves_renamed_column_to_head() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    exec_ok(&engine, "CREATE TABLE t (a INT, b INT, c INT)");
    exec_ok(&engine, "INSERT INTO t VALUES (10, 20, 30)");
    exec_ok(&engine, "ALTER TABLE t CHANGE COLUMN c c BIGINT FIRST");

    assert_eq!(
        column_names(&engine, "t"),
        vec!["c".to_string(), "a".to_string(), "b".to_string()]
    );
    let rows = select_star(&engine, "t");
    assert_eq!(
        rows[0],
        vec![Value::Int(30), Value::Int(10), Value::Int(20)]
    );
}

// ---------- T2: mixed ADD/MODIFY/CHANGE/FIRST/AFTER actions ----------

/// One ALTER combines ADD FIRST, MODIFY AFTER, and CHANGE FIRST.
/// Dispatch applies actions sequentially through atomic_metadata and commits with one flush.
#[test]
fn multi_action_alter_combines_add_modify_change_with_positions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    exec_ok(&engine, "CREATE TABLE t (a INT, b INT, c INT)");
    exec_ok(&engine, "INSERT INTO t VALUES (1, 2, 3)");
    exec_ok(
        &engine,
        "ALTER TABLE t \
          ADD COLUMN d INT DEFAULT 40 AFTER a, \
          MODIFY COLUMN b BIGINT FIRST, \
          CHANGE COLUMN c c2 BIGINT AFTER d",
    );

    // Final logical order: apply each step to the evolving candidate.
    // Initial order: a b c.
    //   ADD d AFTER a → a d b c
    //   MODIFY b FIRST → b a d c
    //   CHANGE c c2 AFTER d → b a d c2
    assert_eq!(
        column_names(&engine, "t"),
        vec![
            "b".to_string(),
            "a".to_string(),
            "d".to_string(),
            "c2".to_string()
        ]
    );

    let rows = select_star(&engine, "t");
    assert_eq!(
        rows[0],
        vec![
            Value::Int(2),  // b
            Value::Int(1),  // a
            Value::Int(40), // d (INSTANT ADD default)
            Value::Int(3),  // c2 (renamed from c)
        ]
    );
}

// ---------- T3: DROP + ADD AFTER dropped ----------

/// DROP a column, then ADD another AFTER it in the same statement. Dispatch prevalidation
/// uses column_index on the resulting candidate and must report unknown column.
#[test]
fn add_after_dropped_column_in_same_statement_rejects() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    exec_ok(&engine, "CREATE TABLE t (a INT, b INT, c INT)");
    let err = exec_err(
        &engine,
        "ALTER TABLE t DROP COLUMN b, ADD COLUMN d INT AFTER b",
    );
    assert_eq!(err.code(), 1054, "{}", err.message());

    // Atomicity: reject the entire statement and retain b.
    assert_eq!(
        column_names(&engine, "t"),
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );
}

/// A separate `ALTER ... ADD AFTER dropped_col_tombstone` references a dropped column,
/// which column_index excludes (schema.rs:1190); expect error 1054.
#[test]
fn add_after_earlier_dropped_column_rejects() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    exec_ok(&engine, "CREATE TABLE t (a INT, b INT, c INT)");
    exec_ok(&engine, "ALTER TABLE t DROP COLUMN b");
    let err = exec_err(&engine, "ALTER TABLE t ADD COLUMN d INT AFTER b");
    assert_eq!(err.code(), 1054, "{}", err.message());

    // A failed statement leaves the schema unchanged.
    assert_eq!(
        column_names(&engine, "t"),
        vec!["a".to_string(), "c".to_string()]
    );
}

// ---------- T4: logical_pos persistence across cold restart ----------

/// Move a column, close and reopen the engine; SHOW COLUMNS/ORDINAL_POSITION retain the new order.
/// This basic catalog persistence assertion is guaranteed by strict durability.
#[test]
fn column_position_persists_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        exec_ok(&engine, "CREATE TABLE t (a INT, b INT, c INT)");
        exec_ok(&engine, "INSERT INTO t VALUES (1, 2, 3)");
        exec_ok(&engine, "ALTER TABLE t ADD COLUMN d INT DEFAULT 4 AFTER a");
        exec_ok(&engine, "ALTER TABLE t MODIFY COLUMN c INT FIRST");
        // Intermediate order: c a d b.
    }

    let engine = open_strict_test_engine(dir.path());
    assert_eq!(
        column_names(&engine, "t"),
        vec![
            "c".to_string(),
            "a".to_string(),
            "d".to_string(),
            "b".to_string()
        ]
    );
    let ords = ordinal_positions(&engine, "t");
    assert_eq!(
        ords,
        vec![
            ("c".to_string(), 1),
            ("a".to_string(), 2),
            ("d".to_string(), 3),
            ("b".to_string(), 4),
        ]
    );

    // Existing row data is not rewritten; SELECT * returns logical order.
    let rows = select_star(&engine, "t");
    assert_eq!(
        rows[0],
        vec![Value::Int(3), Value::Int(1), Value::Int(4), Value::Int(2)],
        "existing row values retained; column output reorders to (c,a,d,b)"
    );
}

// ---------- T5: binlog WRITE_ROWS order ----------

/// After MoveColumn, an INSERT WRITE_ROWS event must emit fields in the new logical order.
/// TABLE_MAP and WRITE_ROWS must agree to avoid misaligned replica columns.
#[test]
fn binlog_write_rows_after_move_column_matches_logical_order() {
    use crate::binlog::{self, BinlogConfig};

    let dir = tempfile::tempdir().unwrap();
    let blog_dir = dir.path().join("binlog");
    std::fs::create_dir_all(&blog_dir).unwrap();
    binlog::init_global(
        blog_dir.clone(),
        BinlogConfig {
            enabled: true,
            base_name: "mydb-bin".into(),
            ..BinlogConfig::default()
        },
    )
    .unwrap();

    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE t (a INT, b INT, c INT)");
    exec_ok(&engine, "ALTER TABLE t MODIFY COLUMN c INT FIRST");
    // Logical order: c a b.
    exec_ok(&engine, "INSERT INTO t (a, b, c) VALUES (10, 20, 30)");

    // Extract TABLE_MAP and WRITE_ROWS from the binlog and verify c a b field order.
    let mgr = binlog::manager().expect("binlog manager installed");
    let mut events = Vec::new();
    for log in mgr.binary_logs().unwrap() {
        events.extend(mgr.events_after(&log.name, 4).unwrap());
    }
    // WRITE_ROWS_EVENT_V2 = 0x1e
    let write_rows_ev = events
        .iter()
        .find(|ev| ev.event_type == 0x1e)
        .expect("expected WRITE_ROWS event after INSERT");

    // Assert 30 comes first (c moved to the head), then 10 and 20. Event type 0x1e=30,
    // timestamp, and end_log_pos may also contain 0x0a/0x1e/0x14 within event.bytes,
    // so search only within the row-values section.
    // WRITE_ROWS_EVENT_V2 payload starts after the 19-byte header:
    //   table_id(6) + flags(2) + extra_header_len(2)
    // Then columns_count (lenenc=1B), columns_bitmap (ceil(n/8)B), and null_bitmap (ceil(n/8)B).
    // For three INT columns: 6+2+2+1+1+1 = 13 bytes; 12 row-value bytes start at payload offset 13.
    let header_len = 19usize;
    let extras = 6 + 2 + 2 + 1 + 1 + 1;
    let row_start = header_len + extras;
    let row_bytes = &write_rows_ev.bytes[row_start..row_start + 12];
    // Each INT occupies 4 little-endian bytes; its first byte is the value because all values are <256.
    let v0 = row_bytes[0];
    let v1 = row_bytes[4];
    let v2 = row_bytes[8];
    assert_eq!(
        (v0, v1, v2),
        (30, 10, 20),
        "row image after MODIFY c FIRST should be (c=30, a=10, b=20); got ({v0}, {v1}, {v2}). row_bytes={row_bytes:02x?}"
    );

    let _ = binlog::init_global(blog_dir, BinlogConfig::default());
}

// ---------- T6: atomic rollback of multiple actions ----------

/// If the second ALTER action fails semantically (AFTER a missing column), atomicity requires
/// no statement side effects: the valid first action must also roll back.
#[test]
fn multi_action_alter_rolls_back_all_on_partial_failure() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    exec_ok(&engine, "CREATE TABLE t (a INT, b INT)");
    // Valid ADD followed by invalid MODIFY AFTER nowhere.
    let err = exec_err(
        &engine,
        "ALTER TABLE t ADD COLUMN d INT DEFAULT 9 FIRST, MODIFY COLUMN b INT AFTER not_a_col",
    );
    assert_eq!(err.code(), 1054, "{}", err.message());

    // No statement side effects: d is absent and column order is unchanged.
    assert_eq!(
        column_names(&engine, "t"),
        vec!["a".to_string(), "b".to_string()]
    );
    // An INSERT into the existing empty table uses (a, b) with original types and positions.
    exec_ok(&engine, "INSERT INTO t VALUES (1, 2)");
    let rows = select_star(&engine, "t");
    assert_eq!(rows[0], vec![Value::Int(1), Value::Int(2)]);
}

// ---------- T7: MoveColumn on a partitioned table ----------

/// Reorder a RANGE-partitioned table; resolving the partition key by name preserves semantics.
#[test]
fn move_column_on_range_partitioned_table_preserves_partition_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    exec_ok(
        &engine,
        "CREATE TABLE t (id INT, v INT, PRIMARY KEY (id)) \
         PARTITION BY RANGE (id) ( \
           PARTITION p0 VALUES LESS THAN (10), \
           PARTITION p1 VALUES LESS THAN (100) \
         )",
    );
    exec_ok(&engine, "INSERT INTO t VALUES (1, 100), (50, 500)");
    exec_ok(
        &engine,
        "ALTER TABLE t ADD COLUMN name VARCHAR(16) DEFAULT 'x' FIRST",
    );

    assert_eq!(
        column_names(&engine, "t"),
        vec!["name".to_string(), "id".to_string(), "v".to_string()]
    );
    // The id partition key retains its meaning and query pruning still works.
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id, v FROM t WHERE id < 10").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1), Value::Int(100)]]);
}

// ---------- T8: MoveColumn with generated columns ----------

/// Generated columns reference a and b by name; moving those columns preserves expression values.
#[test]
fn move_column_referenced_by_generated_column_is_transparent() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    exec_ok(
        &engine,
        "CREATE TABLE t (a INT, b INT, s INT AS (a + b) STORED)",
    );
    exec_ok(&engine, "INSERT INTO t (a, b) VALUES (3, 4)");
    exec_ok(&engine, "ALTER TABLE t MODIFY COLUMN b INT FIRST");

    // Logical order: b a s.
    assert_eq!(
        column_names(&engine, "t"),
        vec!["b".to_string(), "a".to_string(), "s".to_string()]
    );
    // The generated expression still evaluates a+b.
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT s FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(7)]]);
}

// ---------- T9: MoveColumn with FK references ----------

/// A child FK references the parent's PK; moving the PK preserves FK semantics.
#[test]
fn move_fk_referenced_column_preserves_fk_enforcement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    exec_ok(
        &engine,
        "CREATE TABLE parent (id INT PRIMARY KEY, name VARCHAR(16))",
    );
    exec_ok(
        &engine,
        "CREATE TABLE child (cid INT PRIMARY KEY, pid INT, \
         FOREIGN KEY (pid) REFERENCES parent (id))",
    );
    exec_ok(&engine, "INSERT INTO parent VALUES (1, 'a'), (2, 'b')");
    exec_ok(&engine, "INSERT INTO child VALUES (10, 1), (20, 2)");

    // Move parent columns without affecting the FK.
    exec_ok(
        &engine,
        "ALTER TABLE parent MODIFY COLUMN name VARCHAR(16) FIRST",
    );
    assert_eq!(
        column_names(&engine, "parent"),
        vec!["name".to_string(), "id".to_string()]
    );
    // The FK still rejects invalid references.
    let err = exec_err(&engine, "INSERT INTO child VALUES (30, 999)");
    assert_eq!(err.code(), 1452, "{}", err.message());
}

// ---------- T10/T11: AFTER or MODIFY referencing a missing column ----------

#[test]
fn add_column_after_missing_column_rejects() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    exec_ok(&engine, "CREATE TABLE t (a INT)");
    let err = exec_err(&engine, "ALTER TABLE t ADD COLUMN b INT AFTER nowhere");
    assert_eq!(err.code(), 1054, "{}", err.message());
    assert_eq!(column_names(&engine, "t"), vec!["a".to_string()]);
}

// ---------- T12: AFTER the column itself ----------

#[test]
fn modify_column_after_self_rejects_with_1064() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    exec_ok(&engine, "CREATE TABLE t (a INT, b INT)");
    let err = exec_err(&engine, "ALTER TABLE t MODIFY COLUMN a INT AFTER a");
    assert_eq!(err.code(), 1064, "{}", err.message());
    // Column order is unchanged.
    assert_eq!(
        column_names(&engine, "t"),
        vec!["a".to_string(), "b".to_string()]
    );
}

// ---------- T13: MODIFY invisible + FIRST ----------

#[test]
fn modify_invisible_column_to_first_keeps_invisible_and_reorders() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    exec_ok(&engine, "CREATE TABLE t (a INT, b INT INVISIBLE, c INT)");
    exec_ok(&engine, "INSERT INTO t (a, b, c) VALUES (1, 2, 3)");
    exec_ok(&engine, "ALTER TABLE t MODIFY COLUMN b INT INVISIBLE FIRST");

    // SHOW COLUMNS uses logical order, including invisible columns.
    let cols = column_names(&engine, "t");
    assert_eq!(
        cols,
        vec!["b".to_string(), "a".to_string(), "c".to_string()]
    );

    // SELECT * excludes invisible b, returning a c in logical order.
    let rows = select_star(&engine, "t");
    assert_eq!(rows[0], vec![Value::Int(1), Value::Int(3)]);
    // Explicitly selecting b still returns 2.
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT b FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(2)]]);
}

// ---------- T14: ALGORITHM=INSTANT + FIRST/AFTER ----------

#[test]
fn add_column_after_with_algorithm_instant_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    exec_ok(&engine, "CREATE TABLE t (a INT, b INT)");
    exec_ok(&engine, "INSERT INTO t VALUES (1, 2)");
    // FIRST/AFTER uses the Instant capability (dispatch.rs:64), so ALGORITHM=INSTANT must succeed.
    exec_ok(
        &engine,
        "ALTER TABLE t ADD COLUMN c INT DEFAULT 7 AFTER a, ALGORITHM=INSTANT",
    );
    assert_eq!(
        column_names(&engine, "t"),
        vec!["a".to_string(), "c".to_string(), "b".to_string()]
    );
    let rows = select_star(&engine, "t");
    assert_eq!(rows[0], vec![Value::Int(1), Value::Int(7), Value::Int(2)]);
}

// ---------- T15: combine with INSTANT DROP tombstones ----------

/// INSTANT DROP retains tombstones at the end of self.columns but excludes them from logical order.
/// Subsequent MoveColumn loops filter state != Dropped, so tombstones do not participate
/// in logical renumbering or occupy positions.
#[test]
fn move_column_ignores_dropped_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    exec_ok(&engine, "CREATE TABLE t (a INT, b INT, c INT, d INT)");
    exec_ok(&engine, "ALTER TABLE t DROP COLUMN b");
    // Current logical order: a c d.
    exec_ok(&engine, "ALTER TABLE t MODIFY COLUMN d INT FIRST");
    assert_eq!(
        column_names(&engine, "t"),
        vec!["d".to_string(), "a".to_string(), "c".to_string()]
    );
    // ORDINAL_POSITION must also be 1..=3 without tombstone slots.
    let ords = ordinal_positions(&engine, "t");
    assert_eq!(
        ords,
        vec![
            ("d".to_string(), 1),
            ("a".to_string(), 2),
            ("c".to_string(), 3),
        ]
    );
}

// ---------- Additional: preserve physical_pos and avoid rewriting existing rows ----------

/// FIRST/AFTER is INSTANT metadata DDL; existing physical slot layouts remain unchanged.
/// The codec maps existing row slots into the new logical SELECT order.
#[test]
fn move_column_does_not_rewrite_existing_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    exec_ok(&engine, "CREATE TABLE t (a INT, b INT, c INT)");
    // Three rows allow individual assertions after restart.
    exec_ok(&engine, "INSERT INTO t VALUES (1, 10, 100), (2, 20, 200)");
    exec_ok(&engine, "ALTER TABLE t MODIFY COLUMN c INT FIRST");

    let QueryResponse::ResultSet(rs) = engine.execute("SELECT * FROM t ORDER BY a").unwrap() else {
        panic!("expected result set");
    };
    // Logical order: c a b. Read original physical slots and align values with logical order.
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(100), Value::Int(1), Value::Int(10)],
            vec![Value::Int(200), Value::Int(2), Value::Int(20)],
        ]
    );
}

// ---------- Additional: SHOW CREATE TABLE emits logical order ----------

#[test]
fn show_create_table_reflects_current_logical_order() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    exec_ok(&engine, "CREATE TABLE t (a INT, b INT, c INT)");
    exec_ok(&engine, "ALTER TABLE t ADD COLUMN d INT AFTER a");
    exec_ok(&engine, "ALTER TABLE t MODIFY COLUMN c INT FIRST");

    let QueryResponse::ResultSet(rs) = engine.execute("SHOW CREATE TABLE t").unwrap() else {
        panic!("expected result set");
    };
    let ddl = match &rs.rows[0][1] {
        Value::String(s) => s.clone(),
        other => panic!("unexpected SHOW CREATE payload: {other:?}"),
    };
    // Expected declaration order: c a d b.
    let pos_c = ddl.find("`c`").expect("`c` in DDL");
    let pos_a = ddl.find("`a`").expect("`a` in DDL");
    let pos_d = ddl.find("`d`").expect("`d` in DDL");
    let pos_b = ddl.find("`b`").expect("`b` in DDL");
    assert!(
        pos_c < pos_a && pos_a < pos_d && pos_d < pos_b,
        "SHOW CREATE TABLE column declaration order must match logical order c,a,d,b: \
         c@{pos_c} a@{pos_a} d@{pos_d} b@{pos_b}\n{ddl}"
    );
}
