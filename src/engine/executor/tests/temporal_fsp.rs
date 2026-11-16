//! End-to-end tests for TIME/DATETIME/TIMESTAMP fractional-seconds precision
//! (fsp). Cover the four observable surfaces:
//!  1. Storage round-trip — INSERT a literal with more sub-second digits than
//!     the declared fsp, SELECT back, see the value rounded to multiples
//!     of `10^(6-fsp)` (MySQL 5.6.4+ rounds by default; the
//!     TIME_TRUNCATE_FRACTIONAL truncation variant is covered in
//!     `mysql80_compat_temporal.rs`).
//!  2. `SHOW COLUMNS` — `time(3)` / `datetime(6)` / `timestamp` (omitting
//!     `(0)`), matching MySQL.
//!  3. `information_schema.columns.datetime_precision` — the integer fsp.
//!  4. Parse-time bounds check — fsp > 6 rejected with a clear error.

use super::support::query_rows as rows;
use super::*;

fn cell(engine: &StorageQueryEngine, sql: &str) -> String {
    let r = rows(engine, sql);
    assert_eq!(r.len(), 1, "expected one row for `{sql}`, got {}", r.len());
    assert_eq!(r[0].len(), 1, "expected one column for `{sql}`");
    r[0][0].to_display_string()
}

#[test]
fn time_fsp_truncates_to_declared_precision() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, t0 TIME, t3 TIME(3), t6 TIME(6))")
        .unwrap();
    // Same source literal, three target precisions. Microseconds are
    // 123_456; expected after rounding:
    //   t0 → 0          (.123456 < .5 rounds down)
    //   t3 → 123_000    (.456 of a millisecond rounds down)
    //   t6 → 123_456    (no quantization)
    engine
        .execute(
            "INSERT INTO t VALUES \
             (1, '01:02:03.123456', '01:02:03.123456', '01:02:03.123456')",
        )
        .unwrap();
    assert_eq!(cell(&engine, "SELECT t0 FROM t WHERE id=1"), "01:02:03");
    assert_eq!(cell(&engine, "SELECT t3 FROM t WHERE id=1"), "01:02:03.123");
    assert_eq!(
        cell(&engine, "SELECT t6 FROM t WHERE id=1"),
        "01:02:03.123456"
    );
}

#[test]
fn datetime_fsp_truncates_to_declared_precision() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, dt0 DATETIME, dt3 DATETIME(3), dt6 DATETIME(6))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO t VALUES \
             (1, '2024-05-31 12:34:56.987654', \
                 '2024-05-31 12:34:56.987654', \
                 '2024-05-31 12:34:56.987654')",
        )
        .unwrap();
    // MySQL rounds excess fractional digits: .987654 → fsp0 carries into
    // the next second; fsp3 rounds .987654 → .988.
    assert_eq!(
        cell(&engine, "SELECT dt0 FROM t WHERE id=1"),
        "2024-05-31 12:34:57"
    );
    assert_eq!(
        cell(&engine, "SELECT dt3 FROM t WHERE id=1"),
        "2024-05-31 12:34:56.988"
    );
    assert_eq!(
        cell(&engine, "SELECT dt6 FROM t WHERE id=1"),
        "2024-05-31 12:34:56.987654"
    );
}

#[test]
fn timestamp_fsp_truncates_to_declared_precision() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, ts0 TIMESTAMP, ts2 TIMESTAMP(2))")
        .unwrap();
    engine
        .execute(
            "INSERT INTO t VALUES \
             (1, '2024-05-31 12:34:56.987654', '2024-05-31 12:34:56.987654')",
        )
        .unwrap();
    // MySQL rounds: .987654 → fsp0 carries to :57; fsp2 rounds to .99.
    assert_eq!(
        cell(&engine, "SELECT ts0 FROM t WHERE id=1"),
        "2024-05-31 12:34:57"
    );
    assert_eq!(
        cell(&engine, "SELECT ts2 FROM t WHERE id=1"),
        "2024-05-31 12:34:56.99"
    );
}

#[test]
fn show_columns_emits_fsp_for_temporal_types() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (a TIME, b TIME(3), c DATETIME(6), d TIMESTAMP, e TIMESTAMP(2))")
        .unwrap();
    let r = rows(&engine, "SHOW COLUMNS FROM t");
    // SHOW COLUMNS rows: (Field, Type, Null, Key, Default, Extra)
    let mut by_field: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for row in r {
        by_field.insert(row[0].to_display_string(), row[1].to_display_string());
    }
    // fsp=0 is omitted to match MySQL's SHOW COLUMNS output.
    assert_eq!(by_field.get("a").map(String::as_str), Some("time"));
    assert_eq!(by_field.get("b").map(String::as_str), Some("time(3)"));
    assert_eq!(by_field.get("c").map(String::as_str), Some("datetime(6)"));
    assert_eq!(by_field.get("d").map(String::as_str), Some("timestamp"));
    assert_eq!(by_field.get("e").map(String::as_str), Some("timestamp(2)"));
}

#[test]
fn information_schema_datetime_precision_reflects_fsp() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (a TIME, b TIME(3), c DATETIME(6), d TIMESTAMP(2))")
        .unwrap();
    let r = rows(
        &engine,
        "SELECT column_name, datetime_precision \
         FROM information_schema.columns \
         WHERE table_name='t' ORDER BY ordinal_position",
    );
    let p: Vec<(String, String)> = r
        .into_iter()
        .map(|row| (row[0].to_display_string(), row[1].to_display_string()))
        .collect();
    assert_eq!(
        p,
        vec![
            ("a".to_string(), "0".to_string()),
            ("b".to_string(), "3".to_string()),
            ("c".to_string(), "6".to_string()),
            ("d".to_string(), "2".to_string()),
        ]
    );
}

#[test]
fn fsp_out_of_range_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let err = engine
        .execute("CREATE TABLE t (a TIME(7))")
        .expect_err("fsp=7 must be rejected");
    let msg = err.message().to_ascii_lowercase();
    assert!(
        msg.contains("fractional seconds precision") || msg.contains("fsp"),
        "expected fsp range error, got: {}",
        err.message()
    );
}
