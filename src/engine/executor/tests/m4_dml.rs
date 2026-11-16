use super::support::result_rows as rows;
use super::*;

/// M4.B6 — Large-batch INSERT must still surface the *first* NOT
/// NULL violation, in the same row × column order the row-by-row
/// path would have used. We pre-coerce every row before running the
/// batched constraint check, so the violation row's position in the
/// batch is still respected when the batched helper reports the
/// error.
#[test]
fn batch_not_null_reports_first_violation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(32) NOT NULL, age INT)")
        .unwrap();
    // 200 valid rows + one NULL name at position 50, then more
    // valid rows. The batched helper must reject the offending row
    // (id=51) without complaining about later rows.
    let mut sql = String::from("INSERT INTO t VALUES ");
    for i in 0..200u32 {
        if i > 0 {
            sql.push(',');
        }
        if i == 50 {
            sql.push_str(&format!("({}, NULL, {})", i + 1, i + 1));
        } else {
            sql.push_str(&format!("({}, 'n{}', {})", i + 1, i + 1, i + 1));
        }
    }
    let err = engine.execute(&sql).expect_err("expected NOT NULL error");
    assert_eq!(err.code(), 1048);
    assert!(err.message().contains("name"));
    // Nothing should have landed in the heap — INSERT is atomic.
    let count = rows(engine.execute("SELECT COUNT(*) FROM t").unwrap());
    assert_eq!(count, vec![vec![Value::Int(0)]]);
}

/// M4.B6 — Same coverage for CHECK constraints over a large batch.
/// The pre-filter to enforced checks happens once for the whole
/// batch (rather than per row), and the row inside which the
/// violation hits is reported the same way the legacy path would.
#[test]
fn batch_check_reports_first_violation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, v INT, \
             CONSTRAINT v_pos CHECK (v > 0))",
        )
        .unwrap();
    let mut sql = String::from("INSERT INTO t VALUES ");
    for i in 0..200u32 {
        if i > 0 {
            sql.push(',');
        }
        if i == 80 {
            sql.push_str(&format!("({}, -1)", i + 1));
        } else {
            sql.push_str(&format!("({}, {})", i + 1, i + 1));
        }
    }
    let err = engine.execute(&sql).expect_err("expected CHECK error");
    assert_eq!(err.code(), 3819);
    assert!(err.message().contains("v_pos"));
    let count = rows(engine.execute("SELECT COUNT(*) FROM t").unwrap());
    assert_eq!(count, vec![vec![Value::Int(0)]]);
}

/// INSERT IGNORE repairs each explicit NULL independently and keeps every
/// row, while preserving one warning per repaired value.
#[test]
fn ignore_batch_repairs_each_null_independently() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(32) NOT NULL)",
        )
        .unwrap();
    let baseline = engine.database().dml_stats_snapshot().ignore_warnings;
    let response = engine
        .execute_with_session(
            &mut session,
            "INSERT IGNORE INTO t VALUES (1, 'a'), (2, NULL), (3, 'c'), (4, NULL), (5, 'e')",
        )
        .unwrap();
    assert_eq!(response, QueryResponse::OkAffected { affected_rows: 5 });
    let after = engine.database().dml_stats_snapshot().ignore_warnings;
    assert_eq!(after - baseline, 2);
    let stored = rows(
        engine
            .execute_with_session(&mut session, "SELECT id, name FROM t ORDER BY id")
            .unwrap(),
    );
    assert_eq!(
        stored,
        vec![
            vec![Value::Int(1), Value::String("a".into())],
            vec![Value::Int(2), Value::String(String::new())],
            vec![Value::Int(3), Value::String("c".into())],
            vec![Value::Int(4), Value::String(String::new())],
            vec![Value::Int(5), Value::String("e".into())],
        ]
    );
}

/// M4.B7 — Partitioned table with a secondary index used to fail
/// at end-of-LOAD-DATA because `build_table_indexes_bulk` looked
/// up the logical `table.id` for an index handle that only lives
/// under each partition's `storage_id`. Now `build_table_indexes_bulk`
/// no-ops for partitioned tables (their indexes are maintained
/// inline) so the LOAD DATA finishes cleanly and the index is
/// queryable.
#[test]
fn load_data_into_partitioned_table_with_index_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("rows.csv");
    let mut data = String::new();
    for i in 0..30u32 {
        data.push_str(&format!("{},{}\n", i + 1, (i % 4) * 10 + 5));
    }
    std::fs::write(&csv_path, data).unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, v INT, INDEX v_idx (v)) \
             PARTITION BY HASH(id) PARTITIONS 4",
        )
        .unwrap();
    let sql = format!(
        "LOAD DATA INFILE '{}' INTO TABLE t \
         FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'",
        crate::util::file::sql_path(&csv_path)
    );
    engine.execute(&sql).unwrap();

    // Row count survived; total = 30, and the secondary index
    // still answers WHERE v = ? correctly (it would either miss
    // rows or error out if the inline partition index maintenance
    // had been undone by a duplicate end-of-statement bulk build).
    let all = rows(engine.execute("SELECT id, v FROM t").unwrap());
    assert_eq!(all.len(), 30);
    let by_v = rows(engine.execute("SELECT id FROM t WHERE v = 15").unwrap());
    // v = (i%4)*10 + 5 = 15 when i%4 = 1 → rows i ∈ {1,5,9,13,17,21,25,29}
    assert_eq!(by_v.len(), 8);
}

/// M4.B7 — Bulk INSERT into a partitioned table with a secondary
/// index still maintains the index correctly (regression guard
/// for the partitioned inline-index path that B7 relied on).
#[test]
fn bulk_insert_into_partitioned_table_with_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, v INT, INDEX v_idx (v)) \
             PARTITION BY HASH(id) PARTITIONS 4",
        )
        .unwrap();
    let mut sql = String::from("INSERT INTO t VALUES ");
    for i in 0..40u32 {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({}, {})", i + 1, (i % 5) * 10));
    }
    engine.execute(&sql).unwrap();
    let all = rows(engine.execute("SELECT id, v FROM t").unwrap());
    assert_eq!(all.len(), 40);
    let by_v = rows(engine.execute("SELECT id FROM t WHERE v = 20").unwrap());
    // v = (i%5)*10 = 20 when i%5 = 2 → rows i ∈ {2,7,12,17,22,27,32,37}
    // (i+1 → ids 3,8,13,18,23,28,33,38)
    assert_eq!(by_v.len(), 8);
}
