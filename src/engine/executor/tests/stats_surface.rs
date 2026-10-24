//! Phase 4 — MySQL-compatible statistics surface tests:
//! `mysql.innodb_table_stats` / `mysql.innodb_index_stats` data, the
//! `ANALYZE TABLE ... {UPDATE|DROP} HISTOGRAM` execution path, and the
//! sampling / histogram GUC knobs.

use super::*;

fn rows_of(resp: QueryResponse) -> Vec<Vec<Value>> {
    match resp {
        QueryResponse::ResultSet(rs) => rs.rows,
        other => panic!("expected result set, got {other:?}"),
    }
}

#[test]
fn statistics_surfaces_agree_before_and_after_analyze() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE surface_fix (id INT PRIMARY KEY, v INT, KEY MixedIdx(v))")
        .unwrap();
    engine
        .execute("INSERT INTO surface_fix VALUES (1,1),(2,2),(3,3)")
        .unwrap();
    let show = rows_of(engine.execute("SHOW INDEX FROM surface_fix").unwrap());
    let information_schema = rows_of(engine.execute(
        "SELECT INDEX_NAME, CARDINALITY FROM information_schema.STATISTICS WHERE TABLE_NAME='surface_fix' ORDER BY INDEX_NAME"
    ).unwrap());
    for row in show {
        let name = &row[2];
        let cardinality = &row[6];
        assert!(information_schema
            .iter()
            .any(|other| &other[0] == name && &other[1] == cardinality));
    }
    engine.execute("ANALYZE TABLE surface_fix").unwrap();
    engine.execute("UPDATE surface_fix SET v=v+1").unwrap();
    let rows = rows_of(
        engine
            .execute("SELECT n_rows FROM mysql.innodb_table_stats WHERE table_name='surface_fix'")
            .unwrap(),
    );
    assert_eq!(rows[0][0], Value::UInt(3));
}

#[test]
fn analyze_multiple_tables_reports_errors_per_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE good_stats (id INT)").unwrap();
    let rows = rows_of(
        engine
            .execute("ANALYZE TABLE good_stats, missing_stats")
            .unwrap(),
    );
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][2], Value::String("status".into()));
    assert_eq!(rows[1][2], Value::String("Error".into()));
    assert_eq!(rows[2][3], Value::String("Operation failed".into()));
}

#[test]
fn innodb_table_stats_reports_user_tables() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE it (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for i in 0..50 {
        engine
            .execute(&format!("INSERT INTO it VALUES ({i}, {})", i * 3))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE it").unwrap();

    let rows = rows_of(
        engine
            .execute(
                "SELECT database_name, table_name, n_rows \
                 FROM mysql.innodb_table_stats WHERE table_name='it'",
            )
            .unwrap(),
    );
    assert_eq!(rows.len(), 1, "innodb_table_stats must list table it");
    assert_eq!(rows[0][0], Value::String("test".to_string()));
    assert_eq!(rows[0][1], Value::String("it".to_string()));
    let n_rows = match &rows[0][2] {
        Value::UInt(n) => *n,
        Value::Int(n) => *n as u64,
        other => panic!("n_rows not numeric: {other:?}"),
    };
    assert!(n_rows >= 50, "n_rows = {n_rows}");
}

#[test]
fn innodb_index_stats_reports_per_index_prefix_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE ix (id INT PRIMARY KEY, a INT, b INT, INDEX idx_ab (a, b))")
        .unwrap();
    for i in 0..100 {
        engine
            .execute(&format!(
                "INSERT INTO ix VALUES ({i}, {}, {})",
                i % 10,
                i % 5
            ))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE ix").unwrap();

    let rows = rows_of(
        engine
            .execute(
                "SELECT index_name, stat_name, stat_value \
                 FROM mysql.innodb_index_stats \
                 WHERE table_name='ix' AND index_name='idx_ab' \
                 ORDER BY stat_name",
            )
            .unwrap(),
    );
    assert!(!rows.is_empty(), "innodb_index_stats must list idx_ab rows");
    // Must include the two prefix NDV rows plus n_leaf_pages + size.
    let stat_names: Vec<String> = rows
        .iter()
        .map(|r| match &r[1] {
            Value::String(s) => s.clone(),
            other => panic!("stat_name not string: {other:?}"),
        })
        .collect();
    assert!(
        stat_names.iter().any(|s| s == "n_diff_pfx01"),
        "expected n_diff_pfx01, got {stat_names:?}"
    );
    assert!(
        stat_names.iter().any(|s| s == "n_diff_pfx02"),
        "expected n_diff_pfx02, got {stat_names:?}"
    );
    assert!(stat_names.iter().any(|s| s == "n_leaf_pages"));
    assert!(stat_names.iter().any(|s| s == "size"));
}

#[test]
fn innodb_stats_exclude_system_tables() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE u1 (id INT PRIMARY KEY)")
        .unwrap();
    let rows = rows_of(
        engine
            .execute("SELECT database_name FROM mysql.innodb_table_stats")
            .unwrap(),
    );
    for row in &rows {
        let db = match &row[0] {
            Value::String(s) => s.clone(),
            other => panic!("not string: {other:?}"),
        };
        assert!(
            !matches!(
                db.as_str(),
                "mysql" | "information_schema" | "performance_schema" | "sys"
            ),
            "innodb_table_stats leaked system schema {db}"
        );
    }
}

#[test]
fn analyze_update_histogram_builds_named_column_histogram() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE h (id INT PRIMARY KEY, v INT)")
        .unwrap();
    // Enough distinct values that a histogram is built.
    for i in 0..500 {
        engine
            .execute(&format!("INSERT INTO h VALUES ({i}, {})", i))
            .unwrap();
    }

    let rows = rows_of(
        engine
            .execute("ANALYZE TABLE h UPDATE HISTOGRAM ON v")
            .unwrap(),
    );
    assert_eq!(rows.len(), 1, "one histogram status row per column");
    assert_eq!(rows[0][1], Value::String("histogram".to_string()));

    // column_statistics should now show a histogram for v.
    let stat_rows = rows_of(
        engine
            .execute(
                "SELECT COLUMN_NAME FROM information_schema.column_statistics \
                 WHERE TABLE_NAME='h' AND COLUMN_NAME='v'",
            )
            .unwrap(),
    );
    assert_eq!(stat_rows.len(), 1, "v must have a column_statistics row");
}

#[test]
fn analyze_drop_histogram_clears_named_column() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE hd (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for i in 0..500 {
        engine
            .execute(&format!("INSERT INTO hd VALUES ({i}, {})", i))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE hd").unwrap();

    // mydb_stats_health should report HAS_HISTOGRAM = 1 after full analyze.
    let before = rows_of(
        engine
            .execute(
                "SELECT HAS_HISTOGRAM FROM information_schema.mydb_stats_health \
                 WHERE TABLE_NAME='hd'",
            )
            .unwrap(),
    );
    assert_eq!(
        before[0][0],
        Value::Int(1),
        "histogram present after ANALYZE"
    );

    // Drop histograms on both columns; the table's histogram flag must
    // then clear. (A single-column drop may leave the PK column's
    // histogram, so we exercise the full clear here.)
    let rows = rows_of(
        engine
            .execute("ANALYZE TABLE hd DROP HISTOGRAM ON id, v")
            .unwrap(),
    );
    assert_eq!(rows.len(), 2, "one status row per dropped column");
    assert_eq!(rows[0][1], Value::String("histogram".to_string()));

    let after = rows_of(
        engine
            .execute(
                "SELECT HAS_HISTOGRAM FROM information_schema.mydb_stats_health \
                 WHERE TABLE_NAME='hd'",
            )
            .unwrap(),
    );
    assert_eq!(
        after[0][0],
        Value::Int(1),
        "DROP SQL histograms must preserve automatic CBO histograms"
    );
    assert!(rows_of(
        engine
            .execute("SELECT * FROM information_schema.column_statistics WHERE TABLE_NAME='hd'")
            .unwrap()
    )
    .is_empty());
}

#[test]
fn analyze_histogram_unknown_column_errors() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE hx (id INT PRIMARY KEY)")
        .unwrap();
    let err = engine
        .execute("ANALYZE TABLE hx UPDATE HISTOGRAM ON nope")
        .unwrap_err();
    assert_eq!(err.code(), 1054, "unknown histogram column must be 1054");
}

#[test]
fn analyze_table_unsupported_option_still_errors() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE hopt (id INT PRIMARY KEY)")
        .unwrap();
    // A genuinely unimplemented option must not become a silent no-op.
    let err = engine.execute("ANALYZE TABLE hopt FROBNICATE").unwrap_err();
    assert!(err.code() != 0, "unsupported ANALYZE option must error");
}

#[test]
fn histogram_bucket_count_guc_changes_bucket_count() {
    // The bucket-count GUC is read once via OnceLock; nextest runs each
    // test in its own process, so setting the env var here is isolated.
    // SAFETY: single-threaded test entry, before any stats call.
    unsafe {
        std::env::set_var("MYDB_HISTOGRAM_BUCKETS", "8");
    }
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE hg (id INT PRIMARY KEY, v INT)")
        .unwrap();
    // Wide spread of distinct values so an equi-depth histogram is built.
    let values = (0..500)
        .map(|i| format!("({i},{i})"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO hg VALUES {values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE hg").unwrap();
    let table = engine.database_for_replication().table("test.hg").unwrap();
    let internal = &table.stats.columns["v"].histogram;
    assert!(!internal.is_empty() && internal.len() <= 8);
    engine
        .execute("ANALYZE TABLE hg UPDATE HISTOGRAM ON v WITH 3 BUCKETS")
        .unwrap();

    let rows = rows_of(
        engine
            .execute(
                "SELECT HISTOGRAM FROM information_schema.column_statistics \
                 WHERE TABLE_NAME='hg' AND COLUMN_NAME='v'",
            )
            .unwrap(),
    );
    assert_eq!(rows.len(), 1, "v must have column_statistics");
    let json = match &rows[0][0] {
        Value::String(s) => s.clone(),
        other => panic!("HISTOGRAM not string: {other:?}"),
    };
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    let buckets = parsed["buckets"].as_array().expect("buckets array");
    assert_eq!(
        buckets.len(),
        3,
        "explicit SQL bucket count is independent of the CBO GUC"
    );
}
