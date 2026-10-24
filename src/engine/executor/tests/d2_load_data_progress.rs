use super::support::result_rows as rows;
use super::*;

/// D2 — LOAD DATA increments `load_data_rows_loaded` cumulatively, and the
/// `load_data_active` counter is zero before and after the statement
/// (matched RAII guard).
#[test]
fn load_data_bumps_rows_counter_and_returns_to_idle() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("rows.csv");
    let mut buf = String::new();
    for i in 0..1000u32 {
        buf.push_str(&format!("{},{}\n", i + 1, i * 10));
    }
    std::fs::write(&csv_path, buf).unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let baseline = engine.database().dml_stats_snapshot();
    assert_eq!(baseline.load_data_active, 0);
    let sql = format!(
        "LOAD DATA INFILE '{}' INTO TABLE t \
         FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'",
        crate::util::file::sql_path(&csv_path)
    );
    engine.execute(&sql).unwrap();
    let after = engine.database().dml_stats_snapshot();
    assert_eq!(
        after.load_data_rows_loaded - baseline.load_data_rows_loaded,
        1000,
        "1000 rows ingested through LOAD DATA"
    );
    assert_eq!(
        after.load_data_active, 0,
        "the active counter must drop back to zero on completion"
    );
    let stored = rows(engine.execute("SELECT COUNT(*) FROM t").unwrap());
    assert_eq!(stored, vec![vec![Value::Int(1000)]]);
}

/// D2 — Running two LOAD DATAs sequentially: the cumulative counter grows
/// by both row counts, and the active count returns to zero after each.
#[test]
fn load_data_counters_accumulate_across_statements() {
    let dir = tempfile::tempdir().unwrap();
    let csv1 = dir.path().join("a.csv");
    let csv2 = dir.path().join("b.csv");
    {
        let mut buf = String::new();
        for i in 0..50u32 {
            buf.push_str(&format!("{}\n", i + 1));
        }
        std::fs::write(&csv1, buf).unwrap();
    }
    {
        let mut buf = String::new();
        for i in 50..200u32 {
            buf.push_str(&format!("{}\n", i + 1));
        }
        std::fs::write(&csv2, buf).unwrap();
    }
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    let baseline = engine.database().dml_stats_snapshot();
    let sql1 = format!(
        "LOAD DATA INFILE '{}' INTO TABLE t LINES TERMINATED BY '\\n'",
        crate::util::file::sql_path(&csv1)
    );
    engine.execute(&sql1).unwrap();
    let sql2 = format!(
        "LOAD DATA INFILE '{}' INTO TABLE t LINES TERMINATED BY '\\n'",
        crate::util::file::sql_path(&csv2)
    );
    engine.execute(&sql2).unwrap();
    let after = engine.database().dml_stats_snapshot();
    assert_eq!(
        after.load_data_rows_loaded - baseline.load_data_rows_loaded,
        200
    );
    assert_eq!(after.load_data_active, 0);
}

/// D2 — A LOAD DATA that errors mid-way (here: NOT NULL violation on a
/// row past the first chunk) must still drop the active counter back to
/// zero — the RAII guard guarantees this even when the function returns
/// `Err`.
#[test]
fn load_data_active_counter_resets_on_error() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("bad.csv");
    // Mix of valid and a NULL on a NOT NULL column; INSERT-without-IGNORE
    // raises an error mid-stream.
    let mut buf = String::new();
    buf.push_str("1,'a'\n");
    buf.push_str("2,\\N\n");
    buf.push_str("3,'c'\n");
    std::fs::write(&csv_path, buf).unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(8) NOT NULL)")
        .unwrap();
    let baseline = engine.database().dml_stats_snapshot();
    let sql = format!(
        "LOAD DATA INFILE '{}' INTO TABLE t \
         FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'",
        crate::util::file::sql_path(&csv_path)
    );
    let _ = engine.execute(&sql);
    let after = engine.database().dml_stats_snapshot();
    assert_eq!(
        after.load_data_active, 0,
        "RAII guard must reset the active counter even when LOAD DATA fails"
    );
    // load_data_rows_loaded may be 0 or partial — we only assert idle.
    let _ = baseline;
}
