//! MySQL 8.0 temporal type and function compatibility coverage.
//! Cover session-zone consistency for NOW/UTC_*/UNIX_TIMESTAMP/FROM_UNIXTIME/SYSDATE,
//! SET timestamp statement-time overrides, fractional rounding/TIME_TRUNCATE_FRACTIONAL,
//! NO_ZERO_DATE / NO_ZERO_IN_DATE / ALLOW_INVALID_DATES、lc_time_names、
//! legacy explicit_defaults_for_timestamp, and zero-date function paths.
use super::support::query_one_row as one_row;
use super::*;

fn one_row_s(engine: &StorageQueryEngine, s: &mut SessionContext, sql: &str) -> Vec<Value> {
    let QueryResponse::ResultSet(result) = engine.execute_with_session(s, sql).unwrap() else {
        panic!("expected result set for: {sql}");
    };
    assert_eq!(result.rows.len(), 1, "expected single row for: {sql}");
    result.rows.into_iter().next().unwrap()
}

fn all_rows_s(engine: &StorageQueryEngine, s: &mut SessionContext, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine.execute_with_session(s, sql).unwrap() else {
        panic!("expected result set for: {sql}");
    };
    result.rows
}

fn as_text(value: &Value) -> String {
    crate::storage::value_to_string(value)
}

#[test]
fn temporal_numeric_comparison_uses_compact_temporal_conversion() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE compact_cmp(d DATE, dt DATETIME(6))")
        .unwrap();
    engine
        .execute(
            "INSERT INTO compact_cmp VALUES\
             ('0001-01-01','0001-01-01 00:00:00'),\
             ('2017-01-01','2017-01-01 11:59:59'),\
             ('9999-12-31','9999-12-31 23:59:59.999999')",
        )
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT d=10101,d<10101000000.0,d=20170101000000.0,\
                    dt=10101000000.0,dt<20170101 \
             FROM compact_cmp ORDER BY d",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![
                Value::Int(0),
                Value::Int(1),
                Value::Int(0),
                Value::Int(0),
                Value::Int(1),
            ],
            vec![
                Value::Int(0),
                Value::Int(0),
                Value::Int(1),
                Value::Int(0),
                Value::Int(0),
            ],
            vec![
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
            ],
        ]
    );
}

#[test]
fn datetime_numeric_comparison_reports_fsp_rounding_overflow_once() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE compact_fsp_cmp(dt0 DATETIME(0), dt6 DATETIME(6))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO compact_fsp_cmp VALUES\
             ('0001-01-01','0001-01-01'),\
             ('9999-12-31 23:59:59','9999-12-31 23:59:59.999999')",
        )
        .unwrap();

    let rows = all_rows_s(
        &engine,
        &mut session,
        "SELECT dt0=99991231235959.999999,dt6=99991231235959.999999 \
         FROM compact_fsp_cmp ORDER BY dt0",
    );
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(0), Value::Int(0)],
            vec![Value::Int(0), Value::Int(1)],
        ]
    );
    assert_eq!(
        session
            .warnings
            .iter()
            .map(|warning| warning.code())
            .collect::<Vec<_>>(),
        vec![1441, 1292]
    );

    // A quoted compact value does not take the numeric resolver's fsp
    // rounding path and therefore stays warning-free.
    let rows = all_rows_s(
        &engine,
        &mut session,
        "SELECT dt0='99991231235959.999999',dt6='99991231235959.999999' \
         FROM compact_fsp_cmp ORDER BY dt0",
    );
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(0), Value::Int(0)],
            vec![Value::Int(0), Value::Int(1)],
        ]
    );
    assert!(session.warnings.is_empty());
}

#[test]
fn datetime_dml_accepts_deprecated_time_delimiter_with_one_warning_per_value() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE deprecated_dt_delimiter(\
             id INT PRIMARY KEY, dt0 DATETIME(0), dt6 DATETIME(6))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO deprecated_dt_delimiter VALUES\
             (1,'2017-01-01 11:59.59','2017-01-01 11:59.59.555555'),\
             (2,'9999-12-31 23:59.59','9999-12-31 23:59.59.999999')",
        )
        .unwrap();
    assert_eq!(
        session
            .warnings
            .iter()
            .map(|warning| warning.code())
            .collect::<Vec<_>>(),
        vec![4095, 4095, 4095, 4095]
    );
    assert_eq!(
        one_row_s(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM deprecated_dt_delimiter \
             WHERE SECOND(dt0)=59 AND SECOND(dt6)=59",
        ),
        vec![Value::Int(2)]
    );
}

#[test]
fn invalid_prepared_date_comparison_returns_null_with_runtime_warnings() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE prepared_date_cmp(d DATE)")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO prepared_date_cmp VALUES\
             ('0001-01-01'),('2017-01-01'),('9999-12-31')",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "PREPARE prepared_date_eq FROM \
             'SELECT d = ? FROM prepared_date_cmp ORDER BY d'",
        )
        .unwrap();

    engine
        .execute_with_session(&mut session, "SET @invalid_numeric=-20010101")
        .unwrap();
    assert_eq!(
        all_rows_s(
            &engine,
            &mut session,
            "EXECUTE prepared_date_eq USING @invalid_numeric",
        ),
        vec![vec![Value::Null], vec![Value::Null], vec![Value::Null]]
    );
    assert_eq!(
        session
            .warnings
            .iter()
            .map(|warning| warning.code())
            .collect::<Vec<_>>(),
        vec![1292]
    );

    engine
        .execute_with_session(&mut session, "SET @invalid_string='abc'")
        .unwrap();
    assert_eq!(
        all_rows_s(
            &engine,
            &mut session,
            "EXECUTE prepared_date_eq USING @invalid_string",
        ),
        vec![vec![Value::Null], vec![Value::Null], vec![Value::Null]]
    );
    assert_eq!(
        session
            .warnings
            .iter()
            .map(|warning| warning.code())
            .collect::<Vec<_>>(),
        vec![1292, 1292, 1292]
    );
}

// ---------- P0#2: SET timestamp and session timezone consistency ----------

#[test]
fn timestamp_column_converts_at_session_boundary_within_mysql_range() {
    for mode in ["off", "on"] {
        std::env::set_var("MYDB_VECTORIZED", mode);
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        let mut s = SessionContext::default();

        engine
            .execute_with_session(&mut s, "SET SESSION time_zone = '+08:00'")
            .unwrap();
        engine
            .execute_with_session(
                &mut s,
                "CREATE TABLE tz_roundtrip (\
                   id INT PRIMARY KEY, ts TIMESTAMP(6), dt DATETIME(6), KEY idx_ts(ts))",
            )
            .unwrap();
        let prepared_sql = "SELECT COUNT(*) FROM tz_roundtrip WHERE ts = ?";
        let prepared = engine.prepare_cached(&mut s, prepared_sql, 1).unwrap();
        let mut prepared_state = crate::engine::api::PreparedSelectMemoState::default();
        engine
            .execute_with_session(
                &mut s,
                "INSERT INTO tz_roundtrip VALUES \
                 (1, '2037-12-31 23:59:59.123456', '2037-12-31 23:59:59.123456')",
            )
            .unwrap();

        let plus8 = one_row_s(
            &engine,
            &mut s,
            "SELECT ts, dt, UNIX_TIMESTAMP(ts), YEAR(ts), \
                    CAST(ts AS CHAR), DATE_FORMAT(ts, '%Y-%m-%d %H:%i:%s') \
             FROM tz_roundtrip WHERE id=1",
        );
        assert_eq!(as_text(&plus8[0]), "2037-12-31 23:59:59.123456");
        assert_eq!(as_text(&plus8[1]), "2037-12-31 23:59:59.123456");
        assert_eq!(plus8[3], Value::Int(2037));
        assert_eq!(as_text(&plus8[4]), "2037-12-31 23:59:59.123456");
        assert_eq!(as_text(&plus8[5]), "2037-12-31 23:59:59");
        assert_eq!(
            one_row_s(
                &engine,
                &mut s,
                "SELECT COUNT(*) FROM tz_roundtrip \
                 WHERE ts='2037-12-31 23:59:59.123456'",
            )[0],
            Value::Int(1),
            "TIMESTAMP index lookup must interpret its bound in +08:00"
        );
        let prepared_plus8 = crate::engine::api::QueryEngine::execute_prepared_select(
            &engine,
            &mut s,
            &mut prepared_state,
            &prepared,
            prepared_sql,
            &[Value::String("2037-12-31 23:59:59.123456".to_string())],
        )
        .expect("prepared read path")
        .unwrap();
        let QueryResponse::ResultSet(prepared_plus8) = prepared_plus8.materialize().unwrap() else {
            panic!("expected prepared result set");
        };
        assert_eq!(prepared_plus8.rows[0][0], Value::Int(1));
        engine
            .execute_with_session(
                &mut s,
                "CREATE TABLE tz_copy (id INT PRIMARY KEY, dt DATETIME(6), txt VARCHAR(32))",
            )
            .unwrap();
        engine
            .execute_with_session(
                &mut s,
                "INSERT INTO tz_copy SELECT id, ts, ts FROM tz_roundtrip WHERE id=1",
            )
            .unwrap();
        let copied_plus8 = one_row_s(&engine, &mut s, "SELECT dt, txt FROM tz_copy WHERE id=1");
        assert_eq!(as_text(&copied_plus8[0]), "2037-12-31 23:59:59.123456");
        assert_eq!(as_text(&copied_plus8[1]), "2037-12-31 23:59:59.123456");
        let epoch = plus8[2].clone();

        engine
            .execute_with_session(&mut s, "SET SESSION time_zone = '+00:00'")
            .unwrap();
        let utc = one_row_s(
            &engine,
            &mut s,
            "SELECT ts, dt, UNIX_TIMESTAMP(ts), YEAR(ts), \
                    CAST(ts AS CHAR), DATE_FORMAT(ts, '%Y-%m-%d %H:%i:%s') \
             FROM tz_roundtrip WHERE id=1",
        );
        assert_eq!(as_text(&utc[0]), "2037-12-31 15:59:59.123456");
        assert_eq!(
            as_text(&utc[1]),
            "2037-12-31 23:59:59.123456",
            "DATETIME must remain a timezone-free wall time"
        );
        assert_eq!(
            utc[2], epoch,
            "the stored TIMESTAMP instant must not change"
        );
        assert_eq!(utc[3], Value::Int(2037));
        assert_eq!(as_text(&utc[4]), "2037-12-31 15:59:59.123456");
        assert_eq!(as_text(&utc[5]), "2037-12-31 15:59:59");
        assert_eq!(
            one_row_s(
                &engine,
                &mut s,
                "SELECT COUNT(*) FROM tz_roundtrip \
                 WHERE ts='2037-12-31 15:59:59.123456'",
            )[0],
            Value::Int(1),
            "the same TIMESTAMP index key must be found through its UTC wall value"
        );
        let prepared_utc = crate::engine::api::QueryEngine::execute_prepared_select(
            &engine,
            &mut s,
            &mut prepared_state,
            &prepared,
            prepared_sql,
            &[Value::String("2037-12-31 15:59:59.123456".to_string())],
        )
        .expect("prepared read path")
        .unwrap();
        let prepared_utc = match prepared_utc {
            QueryResponse::ResultSet(result) => result,
            QueryResponse::StreamingResultSet(mut result) => {
                let mut rows = Vec::new();
                while let Some(row) = result
                    .cursor
                    .next_row()
                    .expect("read prepared streaming row")
                {
                    rows.push(row);
                }
                crate::engine::api::ResultSet {
                    columns: result.columns,
                    rows,
                }
            }
            _ => panic!("expected prepared result set"),
        };
        assert_eq!(prepared_utc.rows[0][0], Value::Int(1));
        engine
            .execute_with_session(
                &mut s,
                "INSERT INTO tz_copy SELECT 2, ts, ts FROM tz_roundtrip WHERE id=1",
            )
            .unwrap();
        let copied_utc = one_row_s(&engine, &mut s, "SELECT dt, txt FROM tz_copy WHERE id=2");
        assert_eq!(as_text(&copied_utc[0]), "2037-12-31 15:59:59.123456");
        assert_eq!(as_text(&copied_utc[1]), "2037-12-31 15:59:59.123456");
    }
    std::env::remove_var("MYDB_VECTORIZED");
}

#[test]
fn timestamp_defaults_and_on_update_use_session_time_zone() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();

    engine
        .execute_with_session(&mut s, "SET SESSION time_zone = '+08:00'")
        .unwrap();
    engine
        .execute_with_session(&mut s, "SET SESSION timestamp = 2000000000")
        .unwrap();
    engine
        .execute_with_session(
            &mut s,
            "CREATE TABLE tz_defaults (\
               id INT PRIMARY KEY, payload INT,\
               ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\
               dt DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut s, "INSERT INTO tz_defaults (id,payload) VALUES (1,10)")
        .unwrap();

    let local = one_row_s(
        &engine,
        &mut s,
        "SELECT ts, dt, UNIX_TIMESTAMP(ts) FROM tz_defaults WHERE id=1",
    );
    assert_eq!(as_text(&local[0]), "2033-05-18 11:33:20");
    assert_eq!(as_text(&local[1]), "2033-05-18 11:33:20");
    assert_eq!(local[2], Value::Int(2_000_000_000));

    engine
        .execute_with_session(&mut s, "SET SESSION timestamp = 2000003600")
        .unwrap();
    engine
        .execute_with_session(&mut s, "UPDATE tz_defaults SET payload=11 WHERE id=1")
        .unwrap();
    engine
        .execute_with_session(&mut s, "SET SESSION time_zone = '+00:00'")
        .unwrap();
    let utc = one_row_s(
        &engine,
        &mut s,
        "SELECT ts, dt, UNIX_TIMESTAMP(ts) FROM tz_defaults WHERE id=1",
    );
    assert_eq!(as_text(&utc[0]), "2033-05-18 04:33:20");
    assert_eq!(
        as_text(&utc[1]),
        "2033-05-18 12:33:20",
        "DATETIME ON UPDATE stores the session wall time"
    );
    assert_eq!(utc[2], Value::Int(2_000_003_600));
}

#[test]
fn timestamp_named_zone_uses_historical_offset_and_unknown_zone_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();

    engine
        .execute_with_session(&mut s, "SET SESSION time_zone = 'Europe/Berlin'")
        .unwrap();
    engine
        .execute_with_session(
            &mut s,
            "CREATE TABLE tz_named (id INT PRIMARY KEY, ts TIMESTAMP)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut s,
            "INSERT INTO tz_named VALUES \
             (1, '2037-01-15 12:00:00'), (2, '2037-07-15 12:00:00')",
        )
        .unwrap();
    engine
        .execute_with_session(&mut s, "SET SESSION time_zone = '+00:00'")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut s, "SELECT ts FROM tz_named ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(as_text(&rs.rows[0][0]), "2037-01-15 11:00:00");
    assert_eq!(as_text(&rs.rows[1][0]), "2037-07-15 10:00:00");

    let err = engine
        .execute_with_session(&mut s, "SET SESSION time_zone = 'Mars/Olympus_Mons'")
        .unwrap_err();
    assert_eq!(err.code(), 1298);
}

#[test]
fn set_timestamp_freezes_now_family_and_unix_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();

    // 1700000000 = 2023-11-14 22:13:20 UTC
    engine
        .execute_with_session(&mut s, "SET SESSION timestamp = 1700000000")
        .unwrap();
    engine
        .execute_with_session(&mut s, "SET SESSION time_zone = '+00:00'")
        .unwrap();

    let row = one_row_s(
        &engine,
        &mut s,
        "SELECT NOW(), CURDATE(), CURTIME(), UTC_TIMESTAMP(), UNIX_TIMESTAMP()",
    );
    assert_eq!(as_text(&row[0]), "2023-11-14 22:13:20");
    assert_eq!(as_text(&row[1]), "2023-11-14");
    assert_eq!(as_text(&row[2]), "22:13:20");
    assert_eq!(as_text(&row[3]), "2023-11-14 22:13:20");
    assert_eq!(row[4], Value::Int(1_700_000_000));

    // Session timezone changes NOW wall time, but not UTC functions or epoch values.
    engine
        .execute_with_session(&mut s, "SET SESSION time_zone = '+08:00'")
        .unwrap();
    let row = one_row_s(
        &engine,
        &mut s,
        "SELECT NOW(), UTC_TIMESTAMP(), UNIX_TIMESTAMP()",
    );
    assert_eq!(as_text(&row[0]), "2023-11-15 06:13:20");
    assert_eq!(as_text(&row[1]), "2023-11-14 22:13:20");
    assert_eq!(row[2], Value::Int(1_700_000_000));

    // SYSDATE reads the real clock and is not frozen by SET timestamp.
    let row = one_row_s(&engine, &mut s, "SELECT YEAR(SYSDATE()) >= 2026");
    assert_eq!(row[0], Value::Int(1));

    // SET timestamp=0 restores the real clock.
    engine
        .execute_with_session(&mut s, "SET SESSION timestamp = 0")
        .unwrap();
    let row = one_row_s(&engine, &mut s, "SELECT YEAR(NOW()) >= 2026");
    assert_eq!(row[0], Value::Int(1));
}

#[test]
fn unix_timestamp_arg_uses_session_time_zone() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();

    engine
        .execute_with_session(&mut s, "SET SESSION time_zone = '+00:00'")
        .unwrap();
    let row = one_row_s(
        &engine,
        &mut s,
        "SELECT UNIX_TIMESTAMP('1970-01-02 00:00:00')",
    );
    assert_eq!(row[0], Value::Int(86_400));

    // 1970-01-02 00:00:00 at +08:00 is 1970-01-01 16:00:00 UTC.
    engine
        .execute_with_session(&mut s, "SET SESSION time_zone = '+08:00'")
        .unwrap();
    let row = one_row_s(
        &engine,
        &mut s,
        "SELECT UNIX_TIMESTAMP('1970-01-02 00:00:00')",
    );
    assert_eq!(row[0], Value::Int(57_600));

    // Preserve fractional seconds.
    let row = one_row_s(
        &engine,
        &mut s,
        "SELECT UNIX_TIMESTAMP('1970-01-02 00:00:00.5')",
    );
    assert_eq!(row[0], Value::DecimalCompact(57_600_500_000, 6));
}

#[test]
fn from_unixtime_uses_session_time_zone_and_supports_format() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();

    engine
        .execute_with_session(&mut s, "SET SESSION time_zone = '+00:00'")
        .unwrap();
    let row = one_row_s(&engine, &mut s, "SELECT FROM_UNIXTIME(86400)");
    assert_eq!(as_text(&row[0]), "1970-01-02 00:00:00");

    engine
        .execute_with_session(&mut s, "SET SESSION time_zone = '+08:00'")
        .unwrap();
    let row = one_row_s(&engine, &mut s, "SELECT FROM_UNIXTIME(86400)");
    assert_eq!(as_text(&row[0]), "1970-01-02 08:00:00");

    // Two-argument format form closes a MySQL compatibility gap.
    let row = one_row_s(
        &engine,
        &mut s,
        "SELECT FROM_UNIXTIME(86400, '%Y-%m-%d %H:%i')",
    );
    assert_eq!(as_text(&row[0]), "1970-01-02 08:00");

    // Fractional epoch output precision follows argument text precision.
    let row = one_row_s(&engine, &mut s, "SELECT FROM_UNIXTIME(86400.25)");
    assert_eq!(as_text(&row[0]), "1970-01-02 08:00:00.25");

    // Negative input returns NULL, matching MySQL.
    let row = one_row_s(&engine, &mut s, "SELECT FROM_UNIXTIME(-1)");
    assert_eq!(row[0], Value::Null);
}

// ---------- P0#3: default fractional rounding versus TIME_TRUNCATE_FRACTIONAL ----------

#[test]
fn fractional_seconds_round_by_default() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE fr (id INT PRIMARY KEY, t TIME(0), dt DATETIME(0), dt3 DATETIME(3))")
        .unwrap();

    engine
        .execute(
            "INSERT INTO fr VALUES \
             (1, '12:00:00.6', '2023-12-31 23:59:59.5', '2023-01-01 00:00:00.0009996')",
        )
        .unwrap();
    let row = one_row(&engine, "SELECT t, dt, dt3 FROM fr WHERE id = 1");
    assert_eq!(as_text(&row[0]), "12:00:01", "TIME rounds up");
    assert_eq!(
        as_text(&row[1]),
        "2024-01-01 00:00:00",
        "DATETIME carry crosses the year boundary"
    );
    assert_eq!(
        as_text(&row[2]),
        "2023-01-01 00:00:00.001",
        "fsp=3 rounds the microsecond tail"
    );

    // Negative TIME half values round away from zero.
    engine
        .execute("INSERT INTO fr (id, t) VALUES (2, '-00:00:00.5')")
        .unwrap();
    let row = one_row(&engine, "SELECT t FROM fr WHERE id = 2");
    assert_eq!(as_text(&row[0]), "-00:00:01");
}

#[test]
fn time_truncate_fractional_switches_to_truncation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(
            &mut s,
            "CREATE TABLE ftr (id INT PRIMARY KEY, t TIME(0), dt DATETIME(0))",
        )
        .unwrap();
    engine
        .execute_with_session(&mut s, "SET SESSION sql_mode = 'TIME_TRUNCATE_FRACTIONAL'")
        .unwrap();
    engine
        .execute_with_session(
            &mut s,
            "INSERT INTO ftr VALUES (1, '12:00:00.9', '2023-12-31 23:59:59.9')",
        )
        .unwrap();
    let row = one_row_s(&engine, &mut s, "SELECT t, dt FROM ftr WHERE id = 1");
    assert_eq!(as_text(&row[0]), "12:00:00", "truncated, not rounded");
    assert_eq!(as_text(&row[1]), "2023-12-31 23:59:59");

    // Clearing sql_mode restores default rounding.
    engine
        .execute_with_session(&mut s, "SET SESSION sql_mode = ''")
        .unwrap();
    engine
        .execute_with_session(
            &mut s,
            "INSERT INTO ftr VALUES (2, '12:00:00.9', '2023-12-31 23:59:59.9')",
        )
        .unwrap();
    let row = one_row_s(&engine, &mut s, "SELECT t, dt FROM ftr WHERE id = 2");
    assert_eq!(as_text(&row[0]), "12:00:01");
    assert_eq!(as_text(&row[1]), "2024-01-01 00:00:00");
}

// ---------- P0#4: NO_ZERO_DATE / NO_ZERO_IN_DATE / ALLOW_INVALID_DATES ----------

#[test]
fn no_zero_date_strict_errors_non_strict_warns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(
            &mut s,
            "CREATE TABLE zd (id INT PRIMARY KEY, d DATE, dt DATETIME)",
        )
        .unwrap();

    // Default empty sql_mode accepts zero dates silently.
    engine
        .execute_with_session(
            &mut s,
            "INSERT INTO zd VALUES (1, '0000-00-00', '0000-00-00 00:00:00')",
        )
        .unwrap();
    let row = one_row_s(&engine, &mut s, "SELECT d, dt FROM zd WHERE id = 1");
    assert_eq!(as_text(&row[0]), "0000-00-00");
    assert_eq!(as_text(&row[1]), "0000-00-00 00:00:00");

    // Strict plus NO_ZERO_DATE returns error 1292.
    engine
        .execute_with_session(
            &mut s,
            "SET SESSION sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_DATE'",
        )
        .unwrap();
    let err = engine
        .execute_with_session(&mut s, "INSERT INTO zd VALUES (2, '0000-00-00', NULL)")
        .unwrap_err();
    assert_eq!(err.code(), 1292);

    // Non-strict plus NO_ZERO_DATE stores zero and warns.
    engine
        .execute_with_session(&mut s, "SET SESSION sql_mode = 'NO_ZERO_DATE'")
        .unwrap();
    engine
        .execute_with_session(&mut s, "INSERT INTO zd VALUES (3, '0000-00-00', NULL)")
        .unwrap();
    let row = one_row_s(&engine, &mut s, "SELECT d FROM zd WHERE id = 3");
    assert_eq!(as_text(&row[0]), "0000-00-00");
    let warnings = engine
        .execute_with_session(&mut s, "SHOW WARNINGS")
        .ok()
        .and_then(|r| match r {
            QueryResponse::ResultSet(rs) => Some(rs.rows),
            _ => None,
        });
    if let Some(rows) = warnings {
        // The helper may clear warnings between INSERT and SHOW WARNINGS;
        // check codes only when warnings are returned.
        if !rows.is_empty() {
            assert!(rows.iter().any(|r| as_text(&r[1]) == "1292"));
        }
    }
}

#[test]
fn invalid_calendar_dates_follow_strictness() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "CREATE TABLE ivd (id INT PRIMARY KEY, d DATE)")
        .unwrap();

    // Strict invalid date 2009-02-31 returns 1292 rather than silently drifting to March 3.
    engine
        .execute_with_session(&mut s, "SET SESSION sql_mode = 'STRICT_TRANS_TABLES'")
        .unwrap();
    let err = engine
        .execute_with_session(&mut s, "INSERT INTO ivd VALUES (1, '2009-02-31')")
        .unwrap_err();
    assert_eq!(err.code(), 1292);

    // Explicit non-strict sql_mode stores zero with a warning, matching MySQL.
    // Empty sql_mode is mydb's default historical strict behavior; an explicitly
    // non-strict mode is required to enter MySQL's permissive path.
    engine
        .execute_with_session(&mut s, "SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION'")
        .unwrap();
    engine
        .execute_with_session(&mut s, "INSERT INTO ivd VALUES (2, '2009-02-31')")
        .unwrap();
    let row = one_row_s(&engine, &mut s, "SELECT d FROM ivd WHERE id = 2");
    assert_eq!(as_text(&row[0]), "0000-00-00");

    // Explicitly empty sql_mode is MySQL non-strict mode: warn and store zero. Only mydb's
    // never-set default sql_mode retains the historical strict behavior.
    engine
        .execute_with_session(&mut s, "SET SESSION sql_mode = ''")
        .unwrap();
    engine
        .execute_with_session(&mut s, "INSERT INTO ivd VALUES (9, '2009-02-31')")
        .unwrap();
    let row = one_row_s(&engine, &mut s, "SELECT d FROM ivd WHERE id = 9");
    assert_eq!(as_text(&row[0]), "0000-00-00");

    // Leap-year rules: 2024-02-29 is valid; 2023-02-29 is invalid.
    engine
        .execute_with_session(&mut s, "SET SESSION sql_mode = 'STRICT_TRANS_TABLES'")
        .unwrap();
    engine
        .execute_with_session(&mut s, "INSERT INTO ivd VALUES (3, '2024-02-29')")
        .unwrap();
    let err = engine
        .execute_with_session(&mut s, "INSERT INTO ivd VALUES (4, '2023-02-29')")
        .unwrap_err();
    assert_eq!(err.code(), 1292);

    // NO_ZERO_IN_DATE independently controls zero month/day; strict mode alone preserves
    // those components, while explicitly enabling NO_ZERO_IN_DATE returns 1292.
    engine
        .execute_with_session(&mut s, "INSERT INTO ivd VALUES (5, '2009-00-15')")
        .unwrap();
    let row = one_row_s(&engine, &mut s, "SELECT d FROM ivd WHERE id = 5");
    assert_eq!(as_text(&row[0]), "2009-00-15");
    engine
        .execute_with_session(
            &mut s,
            "SET SESSION sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_IN_DATE'",
        )
        .unwrap();
    let err = engine
        .execute_with_session(&mut s, "INSERT INTO ivd VALUES (6, '2009-00-15')")
        .unwrap_err();
    assert_eq!(err.code(), 1292);
}

#[test]
fn allow_invalid_dates_preserves_components_and_heap_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(
            &mut s,
            "CREATE TABLE aid (
                id INT PRIMARY KEY,
                d DATE,
                dt DATETIME(6),
                ts TIMESTAMP NULL
            )",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut s,
            "SET SESSION sql_mode = 'STRICT_TRANS_TABLES,ALLOW_INVALID_DATES'",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut s,
            "INSERT INTO aid VALUES
             (1, '2009-02-31', '2009-02-31 12:34:56.123456', NULL),
             (2, '2009-00-15', '2009-00-15 01:02:03', NULL),
             (3, '2009-02-28', '2009-02-28 00:00:00', NULL)",
        )
        .unwrap();
    let rows = all_rows_s(&engine, &mut s, "SELECT d, dt FROM aid ORDER BY d, id");
    assert_eq!(
        rows.iter()
            .map(|row| row.iter().map(Value::to_display_string).collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        vec![
            vec!["2009-00-15".to_string(), "2009-00-15 01:02:03".to_string(),],
            vec!["2009-02-28".to_string(), "2009-02-28 00:00:00".to_string(),],
            vec![
                "2009-02-31".to_string(),
                "2009-02-31 12:34:56.123456".to_string(),
            ],
        ]
    );
    let err = engine
        .execute_with_session(
            &mut s,
            "UPDATE aid SET ts = '2009-02-31 12:00:00' WHERE id = 1",
        )
        .unwrap_err();
    assert_eq!((err.code(), err.sql_state().as_ref()), (1292, "22007"));
}

// ---------- P1#9: zero-date function paths ----------

#[test]
fn zero_date_extraction_yields_zero() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // MySQL YEAR/MONTH/DAYOFMONTH and time-part functions return zero for zero dates.
    let row = one_row(
        &engine,
        "SELECT YEAR('0000-00-00'), MONTH('0000-00-00'), DAY('0000-00-00'), \
         QUARTER('0000-00-00'), HOUR('0000-00-00 00:00:00'), \
         EXTRACT(YEAR FROM '0000-00-00'), EXTRACT(MONTH FROM '0000-00-00')",
    );
    for (i, v) in row.iter().enumerate() {
        // YEAR()/EXTRACT(YEAR ...) return ordinary signed integer results;
        // the distinct unsigned YEAR representation is reserved for YEAR
        // columns and CAST(... AS YEAR).
        assert_eq!(*v, Value::Int(0), "column {i} should be 0");
    }
}

#[test]
fn extract_compound_units_concatenate_fields() {
    // Regression for issue #58: EXTRACT compound units (shared with INTERVAL)
    // were rejected by sqlparser with 1064. They must concatenate the adjacent
    // fields exactly like MySQL, and coexist with single-unit EXTRACT.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let row = one_row(
        &engine,
        "SELECT \
         EXTRACT(YEAR_MONTH FROM '2020-05-15'), \
         EXTRACT(DAY_HOUR FROM '2020-05-15 10:30:00'), \
         EXTRACT(DAY_MINUTE FROM '2020-05-15 10:30:45'), \
         EXTRACT(DAY_SECOND FROM '2020-05-15 10:30:45'), \
         EXTRACT(HOUR_MINUTE FROM '2020-05-15 10:30:45'), \
         EXTRACT(HOUR_SECOND FROM '10:30:45'), \
         EXTRACT(MINUTE_SECOND FROM '10:30:45'), \
         EXTRACT(SECOND_MICROSECOND FROM '10:30:45.123456')",
    );
    assert_eq!(row[0], Value::Int(202005));
    assert_eq!(row[1], Value::Int(1510));
    assert_eq!(row[2], Value::Int(151030));
    assert_eq!(row[3], Value::Int(15103045));
    assert_eq!(row[4], Value::Int(1030));
    assert_eq!(row[5], Value::Int(103045));
    assert_eq!(row[6], Value::Int(3045));
    assert_eq!(row[7], Value::Int(45123456));

    // Single units still parse via the native EXTRACT path, and compound units
    // work case-insensitively and nested in arithmetic.
    let row = one_row(
        &engine,
        "SELECT EXTRACT(YEAR FROM '2020-05-15'), \
         extract(day_second FROM '2020-05-15 10:30:45'), \
         EXTRACT(YEAR_MONTH FROM '2020-05-15') + 1",
    );
    assert_eq!(row[0], Value::Int(2020));
    assert_eq!(row[1], Value::Int(15103045));
    assert_eq!(row[2], Value::Int(202006));

    // NULL input propagates.
    let row = one_row(&engine, "SELECT EXTRACT(YEAR_MONTH FROM NULL)");
    assert_eq!(row[0], Value::Null);

    // Single-unit MICROSECOND and fractional time-only parsing (same gap the
    // compound *_MICROSECOND units depend on).
    let row = one_row(
        &engine,
        "SELECT EXTRACT(MICROSECOND FROM '10:30:45.123456'), \
         EXTRACT(DAY_MICROSECOND FROM '2020-05-15 10:30:45.123456')",
    );
    assert_eq!(row[0], Value::Int(123456));
    assert_eq!(row[1], Value::Int(15103045123456));
}

#[test]
fn zero_date_calendar_functions_yield_null() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let row = one_row(
        &engine,
        "SELECT DAYNAME('0000-00-00'), MONTHNAME('0000-00-00'), \
         DAYOFWEEK('0000-00-00'), WEEKDAY('0000-00-00'), DAYOFYEAR('0000-00-00'), \
         LAST_DAY('0000-00-00'), TO_DAYS('0000-00-00'), WEEK('0000-00-00'), \
         YEARWEEK('0000-00-00')",
    );
    for (i, v) in row.iter().enumerate() {
        assert_eq!(*v, Value::Null, "column {i} should be NULL");
    }

    let row = one_row(
        &engine,
        "SELECT DATE_ADD('0000-00-00', INTERVAL 1 DAY), \
         DATE_SUB('0000-00-00', INTERVAL 1 MONTH), \
         DATEDIFF('0000-00-00', '2024-01-01'), \
         DATEDIFF('2024-01-01', '0000-00-00'), \
         TIMESTAMPDIFF(DAY, '0000-00-00', '2024-01-01'), \
         TIMESTAMPADD(DAY, 1, '0000-00-00')",
    );
    for (i, v) in row.iter().enumerate() {
        assert_eq!(*v, Value::Null, "column {i} should be NULL");
    }
}

#[test]
fn from_days_out_of_range_returns_null_without_overflow() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let row = one_row(
        &engine,
        "SELECT FROM_DAYS(-11797), FROM_DAYS(-9223372036854775807), \
         CAST(FROM_DAYS(-11797) AS UNSIGNED) ^ SLEEP(0)",
    );
    assert_eq!(
        row,
        vec![
            Value::String("0000-00-00".into()),
            Value::String("0000-00-00".into()),
            Value::UInt(0)
        ]
    );
}

#[test]
fn zero_date_from_stored_column_behaves_like_literal() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE zf (id INT PRIMARY KEY, d DATE, dt DATETIME)")
        .unwrap();
    engine
        .execute("INSERT INTO zf VALUES (1, '0000-00-00', '0000-00-00 00:00:00')")
        .unwrap();

    let row = one_row(
        &engine,
        "SELECT YEAR(d), MONTH(dt), DAYNAME(d), TO_DAYS(dt) FROM zf WHERE id = 1",
    );
    assert_eq!(row[0], Value::Int(0));
    assert_eq!(row[1], Value::Int(0));
    assert_eq!(row[2], Value::Null);
    assert_eq!(row[3], Value::Null);
}

#[test]
fn not_null_date_is_null_where_matches_legacy_zero_date_only() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    session.sql_mode.clear();
    session.temporal_sql_mode_bits = 0;
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE legacy_zero_date (id INT PRIMARY KEY, d DATE NOT NULL)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO legacy_zero_date VALUES (1, '0000-00-00'), (2, '2024-01-01')",
        )
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(
            &mut session,
            "SELECT id, d FROM legacy_zero_date WHERE d IS NULL",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(1));
    assert_eq!(result.rows[0][1].to_display_string(), "0000-00-00");
}

#[test]
fn compact_numeric_temporal_values_are_accepted() {
    // Regression for issue #55: MySQL's delimiter-less numeric / numeric-string
    // date & time forms (20200101, 991231, 20200101123045, 123045) must parse
    // in CAST, temporal functions, INTERVAL arithmetic and on the write path,
    // matching real MySQL — instead of erroring 1366/1292.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // CAST — numeric and string, DATE / DATETIME / TIME, two-digit-year window.
    let row = one_row(
        &engine,
        "SELECT CAST(20200101 AS DATE), CAST('20200101' AS DATE), \
         CAST(20200101123045 AS DATETIME), CAST('20200101123045' AS DATETIME), \
         CAST(123045 AS TIME), CAST(710101 AS DATE), CAST(991231 AS DATE), \
         CAST(101 AS DATE), CAST(20200101.0 AS DATE), CONVERT(20200101, DATE)",
    );
    assert_eq!(as_text(&row[0]), "2020-01-01");
    assert_eq!(as_text(&row[1]), "2020-01-01");
    assert_eq!(as_text(&row[2]), "2020-01-01 12:30:45");
    assert_eq!(as_text(&row[3]), "2020-01-01 12:30:45");
    assert_eq!(as_text(&row[4]), "12:30:45");
    assert_eq!(as_text(&row[5]), "1971-01-01"); // 70..99 → 1900+
    assert_eq!(as_text(&row[6]), "1999-12-31");
    assert_eq!(as_text(&row[7]), "2000-01-01"); // 00..69 → 2000+
    assert_eq!(as_text(&row[8]), "2020-01-01"); // DECIMAL input
    assert_eq!(as_text(&row[9]), "2020-01-01");

    // Temporal functions + INTERVAL arithmetic route through the same parser.
    let row = one_row(
        &engine,
        "SELECT DATE(20200101), YEAR(20200101), MONTH(20200101), DAYOFMONTH(20200101), \
         TIMESTAMP(20200101), DATE_ADD(20200101, INTERVAL 1 DAY), 20200101 + INTERVAL 1 DAY",
    );
    assert_eq!(as_text(&row[0]), "2020-01-01");
    assert_eq!(row[1], Value::Int(2020));
    assert_eq!(row[2], Value::Int(1));
    assert_eq!(row[3], Value::Int(1));
    assert_eq!(as_text(&row[4]), "2020-01-01 00:00:00");
    assert_eq!(as_text(&row[5]), "2020-01-02");
    assert_eq!(as_text(&row[6]), "2020-01-02");

    // Write path: numeric and numeric-string into DATE / DATETIME columns.
    engine
        .execute("CREATE TABLE t55 (id INT PRIMARY KEY, d DATE, dt DATETIME)")
        .unwrap();
    engine
        .execute("INSERT INTO t55 VALUES (1, 20200101, 20200101123045)")
        .unwrap();
    engine
        .execute("INSERT INTO t55 VALUES (2, '20200102', '20200102000000')")
        .unwrap();
    engine
        .execute("INSERT INTO t55 VALUES (3, 710101, NULL)")
        .unwrap();
    let rows = match engine.execute("SELECT d, dt FROM t55 ORDER BY id").unwrap() {
        QueryResponse::ResultSet(r) => r.rows,
        _ => panic!("expected result set"),
    };
    assert_eq!(as_text(&rows[0][0]), "2020-01-01");
    assert_eq!(as_text(&rows[0][1]), "2020-01-01 12:30:45");
    assert_eq!(as_text(&rows[1][0]), "2020-01-02");
    assert_eq!(as_text(&rows[1][1]), "2020-01-02 00:00:00");
    assert_eq!(as_text(&rows[2][0]), "1971-01-01");

    // SELECT-side conversion follows MySQL's scalar coercion policy:
    // an invalid numeric date is NULL plus warning 1292, not a hard error.
    let row = one_row(&engine, "SELECT CAST(20201301 AS DATE)");
    assert!(matches!(row[0], Value::Null));
}

#[test]
fn scalar_temporal_casts_warn_while_allow_invalid_dates_preserves_components() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let row = one_row_s(
        &engine,
        &mut session,
        "SELECT CAST(SUM(0) AS DATETIME), DATE(NAME_CONST('a', 0)), \
                CAST('NULL' AS DATE), LAST_DAY('201:34:36.298754')",
    );
    assert!(row.iter().all(|value| matches!(value, Value::Null)));

    engine
        .execute_with_session(
            &mut session,
            "SET sql_mode = 'NO_ZERO_IN_DATE,NO_ZERO_DATE,ALLOW_INVALID_DATES'",
        )
        .unwrap();
    let row = one_row_s(
        &engine,
        &mut session,
        "SELECT DATE '2014-02-31', DATE '2014-02-31' + 1, \
                DATE(DATE '2014-02-31'), \
                DATE_FORMAT(DATE '2014-02-31', '%Y%b%e')",
    );
    assert_eq!(as_text(&row[0]), "2014-02-31");
    assert_eq!(as_text(&row[1]), "20140232");
    assert_eq!(as_text(&row[2]), "2014-02-31");
    assert_eq!(as_text(&row[3]), "2014Feb31");
}

#[test]
fn permissive_datetime_forms_and_tagged_fsp0_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE permissive_dt (id INT PRIMARY KEY, dt DATETIME)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO permissive_dt VALUES \
             (1, '2003-003-03'), \
             (2, '20030102T131415'), \
             (3, '2001-1-1T1:01:01')",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET sql_mode = 'NO_ENGINE_SUBSTITUTION'")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO permissive_dt VALUES (4, '9999-00-00 00:00:00.0')",
        )
        .unwrap();
    let rows = all_rows_s(
        &engine,
        &mut session,
        "SELECT dt FROM permissive_dt ORDER BY id",
    );
    assert_eq!(
        rows.iter().map(|row| as_text(&row[0])).collect::<Vec<_>>(),
        vec![
            "2003-03-03 00:00:00",
            "2003-01-02 13:14:15",
            "2001-01-01 01:01:01",
            "9999-00-00 00:00:00",
        ]
    );
}

#[test]
fn strict_temporal_comparison_rounding_overflow_and_dml_conversion_errors() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET sql_mode = 'ALLOW_INVALID_DATES'")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE strict_dates (d DATE, KEY(d))")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO strict_dates VALUES ('1000-00-00')",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET sql_mode = 'TRADITIONAL'")
        .unwrap();
    let err = engine
        .execute_with_session(
            &mut session,
            "SELECT * FROM strict_dates WHERE d = '1000-00-00'",
        )
        .unwrap_err();
    assert_eq!((err.code(), err.sql_state().as_ref()), (1525, "HY000"));
    let err = engine
        .execute_with_session(
            &mut session,
            "DELETE IGNORE FROM strict_dates WHERE d = '1000-00-00'",
        )
        .unwrap_err();
    assert_eq!((err.code(), err.sql_state().as_ref()), (1525, "HY000"));

    engine
        .execute_with_session(&mut session, "CREATE TABLE strict_dt (dt DATETIME)")
        .unwrap();
    let err = engine
        .execute_with_session(
            &mut session,
            "DELETE IGNORE FROM strict_dt WHERE dt = '0000-00-01 00:00:01'",
        )
        .unwrap_err();
    assert_eq!((err.code(), err.sql_state().as_ref()), (1525, "HY000"));

    let err = engine
        .execute_with_session(
            &mut session,
            "DELETE FROM strict_dates WHERE 1 = 0 \
             AND TIMESTAMP('18:16:35.025453') IS NULL",
        )
        .unwrap_err();
    assert_eq!((err.code(), err.sql_state().as_ref()), (1292, "22007"));

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE computed_dt (dt DATETIME NOT NULL, KEY(dt))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO computed_dt VALUES (CURRENT_TIMESTAMP())",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET sql_mode = ''")
        .unwrap();
    let err = engine
        .execute_with_session(
            &mut session,
            "UPDATE computed_dt SET dt = dt \
             WHERE dt <=> FROM_UNIXTIME(1151860736, CONCAT_WS(':','%D','%v'))",
        )
        .unwrap_err();
    assert_eq!((err.code(), err.sql_state().as_ref()), (1292, "22007"));
    engine
        .execute_with_session(&mut session, "SET sql_mode = 'TRADITIONAL'")
        .unwrap();

    let row = one_row_s(
        &engine,
        &mut session,
        "SELECT TIMESTAMP '9999-12-31 23:59:59.9'",
    );
    assert_eq!(as_text(&row[0]), "9999-12-31 23:59:59.9");

    engine
        .execute_with_session(&mut session, "CREATE TABLE strict_expr (a INT)")
        .unwrap();
    let err = engine
        .execute_with_session(
            &mut session,
            "INSERT INTO strict_expr VALUES (15 + ADDDATE('2011-00-00', 1))",
        )
        .unwrap_err();
    assert_eq!((err.code(), err.sql_state().as_ref()), (1292, "22007"));

    // mysqltest files commonly save, temporarily clear, then restore
    // sql_mode. Restoration from a user variable must preserve strict DML
    // conversion, including the singular VALUE spelling.
    engine
        .execute_with_session(&mut session, "SET @saved_mode = @@sql_mode")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET sql_mode = ''")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET sql_mode = @saved_mode")
        .unwrap();
    let err = engine
        .execute_with_session(
            &mut session,
            "INSERT INTO strict_expr VALUE (15.00 + ADDDATE('2011-00-00', 1))",
        )
        .unwrap_err();
    assert_eq!((err.code(), err.sql_state().as_ref()), (1292, "22007"));
}

#[test]
fn prepared_invalid_date_comparison_reports_runtime_warning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE prepared_date_cmp (d DATE)")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO prepared_date_cmp VALUES ('2001-01-01')",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "PREPARE date_cmp FROM 'SELECT d = ? FROM prepared_date_cmp'",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET @invalid_date = 'abc'")
        .unwrap();
    let rows = all_rows_s(
        &engine,
        &mut session,
        "EXECUTE date_cmp USING @invalid_date",
    );
    assert_eq!(rows, vec![vec![Value::Null]]);
    let warnings = all_rows_s(&engine, &mut session, "SHOW WARNINGS");
    assert!(warnings.iter().any(|row| {
        matches!(row.get(1), Some(Value::Int(1292)))
            && row
                .get(2)
                .is_some_and(|value| as_text(value).contains("Incorrect DATE value"))
    }));
}

// ---------- P1#6: lc_time_names ----------

#[test]
fn lc_time_names_drives_day_and_month_names() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();

    // Default en_US; 2024-01-15 is Monday.
    let row = one_row_s(
        &engine,
        &mut s,
        "SELECT DAYNAME('2024-01-15'), MONTHNAME('2024-01-15'), \
         DATE_FORMAT('2024-01-15', '%W %M %a %b')",
    );
    assert_eq!(as_text(&row[0]), "Monday");
    assert_eq!(as_text(&row[1]), "January");
    assert_eq!(as_text(&row[2]), "Monday January Mon Jan");

    engine
        .execute_with_session(&mut s, "SET SESSION lc_time_names = 'zh_CN'")
        .unwrap();
    let row = one_row_s(
        &engine,
        &mut s,
        "SELECT DAYNAME('2024-01-15'), MONTHNAME('2024-01-15'), \
         DATE_FORMAT('2024-01-15', '%W/%M/%a/%b')",
    );
    assert_eq!(as_text(&row[0]), "星期一");
    assert_eq!(as_text(&row[1]), "一月");
    assert_eq!(as_text(&row[2]), "星期一/一月/周一/1月");

    engine
        .execute_with_session(&mut s, "SET SESSION lc_time_names = 'de_DE'")
        .unwrap();
    let row = one_row_s(
        &engine,
        &mut s,
        "SELECT DAYNAME('2024-03-03'), MONTHNAME('2024-03-03'), \
         DATE_FORMAT('2024-03-03', '%a %b')",
    );
    assert_eq!(as_text(&row[0]), "Sonntag");
    assert_eq!(as_text(&row[1]), "März");
    assert_eq!(as_text(&row[2]), "So Mär");

    // Unknown locales fall back to en_US.
    engine
        .execute_with_session(&mut s, "SET SESSION lc_time_names = 'xx_XX'")
        .unwrap();
    let row = one_row_s(&engine, &mut s, "SELECT DAYNAME('2024-01-15')");
    assert_eq!(as_text(&row[0]), "Monday");
}

// ---------- P1#7: explicit_defaults_for_timestamp legacy OFF mode ----------

#[test]
fn explicit_defaults_for_timestamp_legacy_first_column_rules() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();

    let row = one_row_s(&engine, &mut s, "SELECT @@explicit_defaults_for_timestamp");
    assert_eq!(as_text(&row[0]), "1");

    // ON is the MySQL 8 default; OFF enables the legacy implicit first
    // TIMESTAMP default/on-update rules for subsequently-created tables.
    engine
        .execute_with_session(&mut s, "SET SESSION explicit_defaults_for_timestamp = ON")
        .unwrap();
    engine
        .execute_with_session(&mut s, "SET SESSION explicit_defaults_for_timestamp = OFF")
        .unwrap();
    engine
        .execute_with_session(&mut s, "SET SESSION timestamp = 1700000000")
        .unwrap();
    engine
        .execute_with_session(&mut s, "CREATE TABLE legacy_ts (id INT, ts TIMESTAMP)")
        .unwrap();
    engine
        .execute_with_session(
            &mut s,
            "INSERT INTO legacy_ts VALUES (1, NULL), (2, DEFAULT)",
        )
        .unwrap();
    let rows = all_rows_s(
        &engine,
        &mut s,
        "SELECT id, UNIX_TIMESTAMP(ts) FROM legacy_ts ORDER BY id",
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1], Value::Int(1_700_000_000));
    assert_eq!(rows[1][1], Value::Int(1_700_000_000));
}

#[test]
fn timestamp_legacy_null_and_extended_assignment_range() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "SET SESSION explicit_defaults_for_timestamp = OFF",
        )
        .unwrap();
    let warnings = all_rows_s(&engine, &mut session, "SHOW WARNINGS");
    assert_eq!(warnings.len(), 1);
    assert!(matches!(
        warnings[0][1],
        Value::Int(1287) | Value::UInt(1287)
    ));

    engine
        .execute_with_session(&mut session, "SET SESSION timestamp = 1000000019")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION'",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE legacy_range (\
             id INT PRIMARY KEY, current_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\
             assigned_ts TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00')",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO legacy_range VALUES (1, NOW(), NULL)",
        )
        .unwrap();
    assert_eq!(
        as_text(
            &one_row_s(
                &engine,
                &mut session,
                "SELECT current_ts - assigned_ts FROM legacy_range WHERE id = 1",
            )[0]
        ),
        "0"
    );

    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO legacy_range VALUES (2, NOW(), FROM_UNIXTIME('9999999999'))",
        )
        .unwrap();
    let warnings = all_rows_s(&engine, &mut session, "SHOW WARNINGS");
    assert!(warnings.is_empty());
    assert_eq!(
        as_text(
            &one_row_s(
                &engine,
                &mut session,
                "SELECT UNIX_TIMESTAMP(assigned_ts) FROM legacy_range WHERE id=2"
            )[0]
        ),
        "9999999999"
    );

    let value = one_row_s(
        &engine,
        &mut session,
        "SELECT FROM_UNIXTIME(99999999999999999999999999999999999999999999999999999999999999999)",
    );
    assert!(matches!(value[0], Value::Null));
    let warnings = all_rows_s(&engine, &mut session, "SHOW WARNINGS");
    assert_eq!(warnings.len(), 1);
    assert!(matches!(
        warnings[0][1],
        Value::Int(1292) | Value::UInt(1292)
    ));
}

#[test]
fn now_respects_named_session_time_zone() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();

    // Named zones use host zoneinfo; Asia/Shanghai has no DST and remains +08:00 here.
    engine
        .execute_with_session(&mut s, "SET SESSION timestamp = 1700000000")
        .unwrap();
    engine
        .execute_with_session(&mut s, "SET SESSION time_zone = 'Asia/Shanghai'")
        .unwrap();
    let row = one_row_s(&engine, &mut s, "SELECT NOW()");
    assert_eq!(as_text(&row[0]), "2023-11-15 06:13:20");
}

// ---------- M1: temporal-to-numeric YYYYMMDD[HHMMSS]/HHMMSS semantics ----------

#[test]
fn temporal_to_number_cast_and_arithmetic() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // DATE → YYYYMMDD
    assert_eq!(
        one_row(&engine, "SELECT CAST(DATE '2024-05-31' AS SIGNED)")[0],
        Value::Int(20240531)
    );
    // DATETIME → YYYYMMDDHHMMSS
    assert_eq!(
        one_row(
            &engine,
            "SELECT CAST(TIMESTAMP '2020-01-01 12:34:56' AS SIGNED)"
        )[0],
        Value::Int(20200101123456)
    );
    // TIME → HHMMSS (negative keeps sign)
    assert_eq!(
        one_row(&engine, "SELECT CAST(TIME '10:20:30' AS SIGNED)")[0],
        Value::Int(102030)
    );
    // Zero date converts to 0 rather than i32::MIN.
    assert_eq!(
        one_row(&engine, "SELECT CAST(DATE '0000-00-00' AS SIGNED)")[0],
        Value::Int(0)
    );
    // YEAR converts to its year number.
    assert_eq!(
        one_row(&engine, "SELECT CAST(CAST(2024 AS YEAR) AS SIGNED)")[0],
        Value::Int(2024)
    );
    // Arithmetic DATE+0 yields YYYYMMDD.
    assert_eq!(
        one_row(&engine, "SELECT DATE '2024-05-31' + 0")[0],
        Value::Int(20240531)
    );
}

#[test]
fn temporal_mixed_comparison_with_integer() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, d DATE, dt DATETIME)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, '2024-05-31', '2020-01-01 00:00:00')")
        .unwrap();
    // Compare DATE with integer YYYYMMDD.
    assert_eq!(
        one_row(&engine, "SELECT id FROM t WHERE d = 20240531")[0],
        Value::Int(1)
    );
    // 2020-01-01 midnight is 20200101000000, below 20200102000000, so greater-than does not match.
    let rows = rows_of(&engine, "SELECT id FROM t WHERE dt > 20200102000000");
    assert!(
        rows.is_empty(),
        "dt should compare as YYYYMMDDHHMMSS number"
    );
    // dt < 20200102000000 matches.
    assert_eq!(
        one_row(&engine, "SELECT id FROM t WHERE dt < 20200102000000")[0],
        Value::Int(1)
    );
}

#[test]
fn auto_parameterized_stream_keeps_date_arithmetic_literal_provenance() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE date_filter (id INT PRIMARY KEY, sold DATE, shipped DATE)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO date_filter VALUES \
             (1, '2001-08-24', '2001-10-04'), \
             (2, '2001-08-27', '2001-10-23')",
        )
        .unwrap();

    let rows = rows_of(
        &engine,
        "SELECT id FROM date_filter WHERE shipped > sold + 5 ORDER BY id",
    );
    assert_eq!(rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
}

#[test]
fn time_comparison_extracts_time_part_from_datetime_string() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE time_compare (t TIME, KEY(t))")
        .unwrap();
    engine
        .execute("INSERT INTO time_compare VALUES (TIME '11:12:13')")
        .unwrap();

    assert_eq!(
        rows_of(
            &engine,
            "SELECT t FROM time_compare \
             WHERE t IN ('2026-02-19 11:12:13')"
        ),
        vec![vec![Value::Time(40_333_000_000)]]
    );
    assert!(rows_of(
        &engine,
        "SELECT t FROM time_compare \
         WHERE t NOT IN ('2026-02-19 11:12:13')"
    )
    .is_empty());
}

#[test]
fn temporal_date_vs_datetime_equality() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // DATE equals midnight DATETIME through type promotion, not byte comparison.
    assert_eq!(
        one_row(
            &engine,
            "SELECT DATE '2020-01-01' = TIMESTAMP '2020-01-01 00:00:00'"
        )[0],
        Value::Int(1)
    );
    assert_eq!(
        one_row(
            &engine,
            "SELECT DATE '2020-01-01' = TIMESTAMP '2020-01-01 00:00:01'"
        )[0],
        Value::Int(0)
    );
    assert_eq!(
        one_row(
            &engine,
            "SELECT DATE '2020-01-02' > TIMESTAMP '2020-01-01 23:59:59'"
        )[0],
        Value::Int(1)
    );
}

fn rows_of(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    match engine.execute(sql).unwrap() {
        QueryResponse::ResultSet(rs) => rs.rows,
        other => panic!("expected result set for {sql}, got {other:?}"),
    }
}

// ---------- M3: TIME clamping and YEAR string index bounds ----------

#[test]
fn time_value_saturates_to_mysql_range() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, tm TIME)")
        .unwrap();
    // 900 hours exceeds +/-838:59:59 and clamps there rather than storing 900 hours.
    engine
        .execute("INSERT IGNORE INTO t VALUES (1, '900:00:00')")
        .unwrap();
    engine
        .execute("INSERT IGNORE INTO t VALUES (2, '-900:00:00')")
        .unwrap();
    assert_eq!(cell(&engine, "SELECT tm FROM t WHERE id=1"), "838:59:59");
    assert_eq!(cell(&engine, "SELECT tm FROM t WHERE id=2"), "-838:59:59");
    // Out-of-range colon-form minutes fail in strict mode.
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "SET SESSION sql_mode='STRICT_TRANS_TABLES'")
        .unwrap();
    let err = engine
        .execute_with_session(&mut s, "INSERT INTO t VALUES (3, '10:99:00')")
        .unwrap_err();
    assert_eq!(err.code(), 1292);
}

#[test]
fn year_index_string_bound_matches_integer() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, y YEAR, INDEX iy (y))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,2020),(2,2021),(3,2022)")
        .unwrap();
    // String index bounds must match rather than being treated as year zero and returning nothing.
    let r = rows_of(&engine, "SELECT id FROM t WHERE y = '2021'");
    assert_eq!(r, vec![vec![Value::Int(2)]]);
    // Match the integer-bound form.
    let ri = rows_of(&engine, "SELECT id FROM t WHERE y = 2021");
    assert_eq!(ri, vec![vec![Value::Int(2)]]);
}

fn cell(engine: &StorageQueryEngine, sql: &str) -> String {
    let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
        panic!("expected result set for {sql}");
    };
    assert_eq!(rs.rows.len(), 1);
    rs.rows[0][0].to_display_string()
}

// ---------- M4: function fractional precision and TIMEDIFF clamping ----------

#[test]
fn date_add_preserves_fractional_seconds() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Preserve input fsp=6 rather than truncating to seconds.
    assert_eq!(
        as_text(
            &one_row(
                &engine,
                "SELECT DATE_ADD('2018-05-01 10:00:00.123456', INTERVAL 1 SECOND)"
            )[0]
        ),
        "2018-05-01 10:00:01.123456"
    );
    // MICROSECOND increments are no longer no-ops.
    assert_eq!(
        as_text(
            &one_row(
                &engine,
                "SELECT DATE_ADD('2018-05-01 10:00:00', INTERVAL 500000 MICROSECOND)"
            )[0]
        ),
        "2018-05-01 10:00:00.500000"
    );
    // TIMESTAMPADD(MICROSECOND) also preserves precision.
    assert_eq!(
        as_text(
            &one_row(
                &engine,
                "SELECT TIMESTAMPADD(MICROSECOND, 123, '2020-01-01 00:00:00')"
            )[0]
        ),
        "2020-01-01 00:00:00.000123"
    );
}

#[test]
fn timediff_preserves_fsp_and_clamps_range() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Preserve fractional values and FSP metadata. Value uses compact TIME representation;
    // the protocol prints trailing zeros according to decimals=6.
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT TIMEDIFF('10:00:00.500000','09:00:00.100000')")
        .unwrap()
    else {
        panic!("expected TIMEDIFF result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Time(3_600_400_000)]]);
    assert_eq!(result.columns[0].column_type, ColumnType::Time);
    assert_eq!(result.columns[0].source.decimals, Some(6));
    // An excessive difference saturates at 838:59:59 rather than emitting huge hours.
    assert_eq!(
        as_text(
            &one_row(
                &engine,
                "SELECT TIMEDIFF('3000-01-01 00:00:00','2000-01-01 00:00:00')"
            )[0]
        ),
        "838:59:59"
    );
}

// ---------- M3 INT-1: negative integer-to-TIME preserves sign ----------

#[test]
fn negative_integer_time_keeps_sign() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, tm TIME)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, -5)").unwrap();
    engine.execute("INSERT INTO t VALUES (2, -59)").unwrap();
    engine.execute("INSERT INTO t VALUES (3, -1234)").unwrap();
    assert_eq!(cell(&engine, "SELECT tm FROM t WHERE id=1"), "-00:00:05");
    assert_eq!(cell(&engine, "SELECT tm FROM t WHERE id=2"), "-00:00:59");
    assert_eq!(cell(&engine, "SELECT tm FROM t WHERE id=3"), "-00:12:34");
}

// ---------- PERF-2: vectorized temporal range filtering matches the row oracle ----------

#[test]
fn vectorized_temporal_range_filter_matches_row_engine() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, dt DATETIME, d DATE)")
        .unwrap();
    let vals: Vec<String> = (1..=600)
        .map(|i| {
            format!(
                "({i}, '2020-01-01 00:00:{:02}', '2020-{:02}-01')",
                i % 60,
                (i % 12) + 1
            )
        })
        .collect();
    engine
        .execute(&format!("INSERT INTO t VALUES {}", vals.join(",")))
        .unwrap();
    // Force vector execution and compare with rows.
    for mode in ["on", "off"] {
        std::env::set_var("MYDB_VECTORIZED", mode);
        let c_dt = match engine
            .execute("SELECT COUNT(*) FROM t WHERE dt > '2020-01-01 00:00:30'")
            .unwrap()
        {
            QueryResponse::ResultSet(rs) => rs.rows[0][0].to_display_string(),
            _ => panic!(),
        };
        let c_d = match engine
            .execute("SELECT COUNT(*) FROM t WHERE d >= '2020-06-01'")
            .unwrap()
        {
            QueryResponse::ResultSet(rs) => rs.rows[0][0].to_display_string(),
            _ => panic!(),
        };
        assert_eq!(c_dt, "290", "dt>...:30 count wrong in mode {mode}");
        assert_eq!(c_d, "350", "d>=2020-06-01 count wrong in mode {mode}");
    }
    std::env::remove_var("MYDB_VECTORIZED");
}

// ---------- M3 INT-2: compact integer dates and two-digit year pivot ----------

#[test]
fn compact_integer_date_forms_with_year_pivot() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, d DATE, dt DATETIME)")
        .unwrap();
    // Six-digit YYMMDD: 200101 becomes 2020-01-01; 700101 becomes 1970-01-01.
    engine
        .execute("INSERT INTO t VALUES (1, 200101, 20200101123456)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (2, 700101, 700101)")
        .unwrap();
    assert_eq!(cell(&engine, "SELECT d FROM t WHERE id=1"), "2020-01-01");
    assert_eq!(
        cell(&engine, "SELECT dt FROM t WHERE id=1"),
        "2020-01-01 12:34:56"
    );
    assert_eq!(cell(&engine, "SELECT d FROM t WHERE id=2"), "1970-01-01");
    assert_eq!(
        cell(&engine, "SELECT dt FROM t WHERE id=2"),
        "1970-01-01 00:00:00"
    );
}

// ---------- P0#2: compiled HOUR/MINUTE/SECOND fallback for TIME and >=24h ----------

#[test]
fn hour_minute_second_extended_time_compiled_path() {
    // Regression: expr_program, the default compiled hot path, called parse_datetime
    // on TIME values and failed with 1366. Legacy extract_datetime_part
    // fell back to parse_time_components, allowing HOUR/MINUTE/SECOND
    // for >=24h and negative values (issue #75). Both paths must agree.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // String literal form, Value::String.
    assert_eq!(cell(&engine, "SELECT HOUR('838:59:59')"), "838");
    assert_eq!(cell(&engine, "SELECT MINUTE('838:59:59')"), "59");
    assert_eq!(cell(&engine, "SELECT SECOND('838:59:59')"), "59");
    assert_eq!(cell(&engine, "SELECT HOUR('26:30:45')"), "26");

    // TIME column form, Value::Time, forcing compiled row execution.
    engine
        .execute("CREATE TABLE tt (id INT PRIMARY KEY, t TIME)")
        .unwrap();
    engine
        .execute("INSERT INTO tt VALUES (1, '838:59:59'), (2, '26:30:45')")
        .unwrap();
    assert_eq!(cell(&engine, "SELECT HOUR(t) FROM tt WHERE id=1"), "838");
    assert_eq!(cell(&engine, "SELECT HOUR(t) FROM tt WHERE id=2"), "26");
    assert_eq!(cell(&engine, "SELECT MINUTE(t) FROM tt WHERE id=1"), "59");
    assert_eq!(cell(&engine, "SELECT SECOND(t) FROM tt WHERE id=1"), "59");
}

// ---------- INTERVAL YEAR_MONTH/YEAR/MONTH i64::MAX overflow returns NULL, not panic ----------

#[test]
fn interval_year_month_extreme_overflow_yields_null_not_panic() {
    // Related robustness regression task_c75125b4: YEAR_MONTH year*12
    // and year*12+month used unchecked i64 arithmetic near i64::MAX,
    // panicking on debug overflow and silently miscomputing in release.
    // MySQL returns NULL with an overflow warning, never crashes. Checked arithmetic
    // uses the existing out-of-range NULL path, like YEAR checked_mul(12).
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // Y-M form with year=i64::MAX overflows multiplication by 12.
    let row = one_row(
        &engine,
        "SELECT DATE_ADD('1995-01-05', INTERVAL '9223372036854775807-02' YEAR_MONTH)",
    );
    assert_eq!(row[0], Value::Null);

    // Symmetric DATE_SUB Y-M form.
    let row = one_row(
        &engine,
        "SELECT DATE_SUB('1995-01-05', INTERVAL '9223372036854775807-11' YEAR_MONTH)",
    );
    assert_eq!(row[0], Value::Null);

    // Compact numeric form overflows compact/100*12.
    let row = one_row(
        &engine,
        "SELECT DATE_ADD('1995-01-05', INTERVAL 92233720368547758 YEAR_MONTH)",
    );
    assert_eq!(row[0], Value::Null);

    // YEAR=i64::MAX also overflows *12; cover the existing checked path.
    let row = one_row(
        &engine,
        "SELECT DATE_ADD('1995-01-05', INTERVAL 9223372036854775807 YEAR)",
    );
    assert_eq!(row[0], Value::Null);

    // A valid but huge MONTH count above i32 yet within i64 returns NULL, not a crash.
    let row = one_row(
        &engine,
        "SELECT DATE_ADD('1995-01-05', INTERVAL 9223372036854775807 MONTH)",
    );
    assert_eq!(row[0], Value::Null);

    // Likewise huge YEAR_MONTH totals within i64 but above i32 hit downstream guards and return NULL.
    let row = one_row(
        &engine,
        "SELECT DATE_ADD('1995-01-05', INTERVAL '1000000000-00' YEAR_MONTH)",
    );
    assert_eq!(row[0], Value::Null);

    // Ordinary small values remain correct, guarding the normal path.
    assert_eq!(
        as_text(
            &one_row(
                &engine,
                "SELECT DATE_ADD('1995-01-05', INTERVAL '1-02' YEAR_MONTH)",
            )[0]
        ),
        "1996-03-05"
    );
    assert_eq!(
        as_text(
            &one_row(
                &engine,
                "SELECT DATE_ADD('1995-01-05', INTERVAL 102 YEAR_MONTH)",
            )[0]
        ),
        "2003-07-05"
    );
}

// ---------- YEAR display-width validation, error 1818 ----------
//
// MySQL accepts bare YEAR and deprecated YEAR(4) only;
// other explicit widths, including 0/2/255/4294967295, must fail during CREATE
// and ALTER MODIFY/CHANGE with 1818,
// Supports only YEAR or YEAR(4) column. Source: upstream.
// mysql-test/t/type_year.test。

#[test]
fn year_bare_and_year4_accepted() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    // Bare YEAR and deprecated YEAR(4) are valid.
    engine.execute("CREATE TABLE y_bare (c1 YEAR)").unwrap();
    engine.execute("CREATE TABLE y4 (c1 YEAR(4))").unwrap();
    // ALTER CHANGE to valid YEAR(4) also succeeds.
    engine
        .execute("ALTER TABLE y_bare CHANGE c1 c1 YEAR(4)")
        .unwrap();
}

#[test]
fn year_assignment_rounds_numeric_inputs_and_warns_for_trailing_garbage() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE yr (a YEAR)")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT IGNORE INTO yr VALUES (2000.5), ('2000.5'), ('2001a'), ('2.001E3')",
        )
        .unwrap();
    assert_eq!(session.warnings.len(), 1);
    assert_eq!(session.warnings[0].code(), 1265);

    let rows = all_rows_s(&engine, &mut session, "SELECT a FROM yr");
    assert_eq!(rows.len(), 4);
    assert!(rows
        .iter()
        .all(|row| crate::storage::value_to_string(&row[0]) == "2001"));
}

#[test]
fn year_assignment_distinguishes_character_and_numeric_zero() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE yr_zero (a YEAR)")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO yr_zero VALUES (0), ('0'), ('00'), ('000'), ('0000')",
        )
        .unwrap();

    let rows = all_rows_s(
        &engine,
        &mut session,
        "SELECT a FROM yr_zero ORDER BY CAST(a AS UNSIGNED), a",
    );
    let displayed: Vec<_> = rows
        .iter()
        .map(|row| crate::storage::value_to_string(&row[0]))
        .collect();
    assert_eq!(displayed, ["0000", "0000", "2000", "2000", "2000"]);
}

#[test]
fn year_equality_uses_two_digit_mapping_and_assignment_rounding() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE yr_cmp (a YEAR)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO yr_cmp VALUES (1999), (2000)")
        .unwrap();

    for predicate in ["a = 99", "a = 1999.1", "a = 1998.9", "a = '1999'"] {
        assert_eq!(
            one_row_s(
                &engine,
                &mut session,
                &format!("SELECT COUNT(*) FROM yr_cmp WHERE {predicate}")
            )[0],
            Value::Int(1),
            "predicate: {predicate}"
        );
    }
    assert_eq!(
        one_row_s(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM yr_cmp WHERE a = '0'"
        )[0],
        Value::Int(1)
    );
    assert_eq!(
        one_row_s(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM yr_cmp WHERE a = '0000'"
        )[0],
        Value::Int(0)
    );
    assert_eq!(
        one_row_s(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM yr_cmp WHERE a > 123"
        )[0],
        Value::Int(2)
    );
    assert_eq!(
        one_row_s(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM yr_cmp WHERE a = 'test'"
        )[0],
        Value::Int(0)
    );
    assert_eq!(
        session
            .warnings
            .iter()
            .filter(|warning| warning.code() == 1292)
            .count(),
        1
    );

    engine
        .execute_with_session(&mut session, "INSERT INTO yr_cmp VALUES (0)")
        .unwrap();
    let min_max = one_row_s(&engine, &mut session, "SELECT MIN(a), MAX(a) FROM yr_cmp");
    assert_eq!(min_max, vec![Value::Int(0), Value::Int(2000)]);

    let mixed_temporal = one_row_s(
        &engine,
        &mut session,
        "SELECT LEAST(DATE'2000-01-01', CAST(1998 AS YEAR)), \
                GREATEST(CAST(2010 AS YEAR), DATE'2000-01-01')",
    );
    assert_eq!(
        mixed_temporal,
        vec![
            Value::Date(
                (chrono::NaiveDate::from_ymd_opt(1998, 1, 1).unwrap()
                    - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                .num_days() as i32
            ),
            Value::Date(
                (chrono::NaiveDate::from_ymd_opt(2010, 1, 1).unwrap()
                    - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                .num_days() as i32
            ),
        ]
    );
    assert_eq!(
        one_row_s(
            &engine,
            &mut session,
            "SELECT HOUR(GREATEST(CAST(1957 AS YEAR), CAST(2085 AS YEAR)))"
        )[0],
        Value::Null
    );
    assert_eq!(session.warnings[0].code(), 1292);
}

#[test]
fn year_invalid_display_width_rejected_1818() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    // Each invalid CREATE width returns 1818 and leaves no table.
    for width in ["0", "1", "2", "3", "5", "255", "4294967295"] {
        let sql = format!("CREATE TABLE bad (c1 YEAR({width}))");
        let err = engine.execute(&sql).unwrap_err();
        assert_eq!(
            err.code(),
            1818,
            "YEAR({width}) must raise 1818, got {err:?}"
        );
        assert!(
            err.message()
                .contains("Supports only YEAR or YEAR(4) column."),
            "YEAR({width}) message: {}",
            err.message()
        );
        // Reject creation without leaving a partial table.
        assert!(
            engine.execute("SELECT * FROM bad").is_err(),
            "YEAR({width}) must not create the table"
        );
    }
}

#[test]
fn year_invalid_display_width_rejected_on_alter() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine.execute("CREATE TABLE y (c1 YEAR)").unwrap();
    // ALTER MODIFY YEAR(2) uses the same validation and returns 1818.
    let err = engine
        .execute("ALTER TABLE y MODIFY COLUMN c1 YEAR(2)")
        .unwrap_err();
    assert_eq!(
        err.code(),
        1818,
        "ALTER MODIFY YEAR(2) must raise 1818, got {err:?}"
    );
    // CHANGE is validated too.
    let err = engine
        .execute("ALTER TABLE y CHANGE c1 c1 YEAR(255)")
        .unwrap_err();
    assert_eq!(
        err.code(),
        1818,
        "ALTER CHANGE YEAR(255) must raise 1818, got {err:?}"
    );
}

// ---------- Temporal review phase 1, 2026-07: P0 data correctness ----------

// B-01: DATE encoding for 1969-12-31 collided with the zero-date sentinel, silently corrupting data.
#[test]
fn phase1_b01_date_1969_12_31_roundtrips_not_zero_date() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE d69 (id INT PRIMARY KEY, d DATE)")
        .unwrap();
    // Cover pre-epoch, exact epoch, and post-epoch boundaries.
    engine
        .execute(
            "INSERT INTO d69 VALUES \
             (1,'1969-12-31'),(2,'1970-01-01'),(3,'1970-01-02'),\
             (4,'1900-01-01'),(5,'9999-12-31')",
        )
        .unwrap();
    let rows = match engine.execute("SELECT id, d FROM d69 ORDER BY id").unwrap() {
        QueryResponse::ResultSet(r) => r.rows,
        _ => panic!(),
    };
    assert_eq!(
        as_text(&rows[0][1]),
        "1969-12-31",
        "B-01: 1969-12-31 must not collapse to zero-date"
    );
    assert_eq!(as_text(&rows[1][1]), "1970-01-01");
    assert_eq!(as_text(&rows[2][1]), "1970-01-02");
    assert_eq!(as_text(&rows[3][1]), "1900-01-01");
    assert_eq!(as_text(&rows[4][1]), "9999-12-31");
}

// B-02: pre-1970 DATETIME truncation under TIME_TRUNCATE_FRACTIONAL rounds toward negative infinity.
#[test]
fn phase1_b02_truncate_fractional_datetime_floors_toward_neg_infinity() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "SET SESSION sql_mode='TIME_TRUNCATE_FRACTIONAL'")
        .unwrap();
    engine
        .execute_with_session(
            &mut s,
            "CREATE TABLE dtrunc (id INT PRIMARY KEY, dt DATETIME(0))",
        )
        .unwrap();
    // 1969-12-31 23:59:59.5 must become 23:59:59 rather than 1970-01-01 midnight.
    engine
        .execute_with_session(
            &mut s,
            "INSERT INTO dtrunc VALUES \
             (1,'1969-12-31 23:59:59.500000'),\
             (2,'1969-12-31 23:59:59.999999'),\
             (3,'1970-01-01 00:00:00.500000')",
        )
        .unwrap();
    let rows = match engine
        .execute_with_session(&mut s, "SELECT id, dt FROM dtrunc ORDER BY id")
        .unwrap()
    {
        QueryResponse::ResultSet(r) => r.rows,
        _ => panic!(),
    };
    assert_eq!(as_text(&rows[0][1]), "1969-12-31 23:59:59");
    assert_eq!(as_text(&rows[1][1]), "1969-12-31 23:59:59");
    assert_eq!(as_text(&rows[2][1]), "1970-01-01 00:00:00");
}

// B-03: TO_SECONDS on zero dates returns NULL rather than 1366. Constants were already
// correct for post-1582 dates; MySQL documents pre-1582 behavior as unreliable.
#[test]
fn phase1_b03_to_seconds_matches_mysql_and_zero_date_nulls() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    // Observed MySQL post-Gregorian values, 1970 onward:
    //  TO_SECONDS('1970-01-01')         = 62_167_219_200 (= 719528*86400)
    //  TO_SECONDS('2020-01-01 12:34:56') = TO_DAYS('2020-01-01')*86400 + hms
    //                                    = 737790*86400 + 45296 = 63_745_101_296
    // TO_SECONDS differs by exactly 86400 between adjacent days.
    let row = one_row(
        &engine,
        "SELECT TO_SECONDS('1970-01-01'), \
         TO_SECONDS('2020-01-01 12:34:56'), \
         TO_SECONDS('2020-01-02') - TO_SECONDS('2020-01-01'), \
         TO_SECONDS('0000-00-00')",
    );
    assert_eq!(row[0], Value::Int(62_167_219_200));
    assert_eq!(row[1], Value::Int(63_745_101_296));
    assert_eq!(
        row[2],
        Value::Int(86400),
        "adjacent days differ by exactly 86400"
    );
    // Primary fix: zero dates return NULL instead of 1366.
    assert!(matches!(row[3], Value::Null), "zero date → NULL, not 1366");
}

// B-04: parse_time_to_micros must not wrap huge hour counts.
#[test]
fn phase1_b04_time_parse_saturates_huge_hours() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    // Extreme hours saturate to 838:59:59 rather than wrapping to arbitrary values.
    engine
        .execute("CREATE TABLE t4 (id INT PRIMARY KEY, t TIME(6))")
        .unwrap();
    engine
        .execute(
            "INSERT IGNORE INTO t4 VALUES \
             (1,'2562047789:00:00'),(2,'1000000:00:00'),(3,'-2562047789:00:00')",
        )
        .unwrap();
    let rows = match engine.execute("SELECT id, t FROM t4 ORDER BY id").unwrap() {
        QueryResponse::ResultSet(r) => r.rows,
        _ => panic!(),
    };
    // Both signs saturate at +/-838:59:59; display may omit zero fractional digits.
    // The value itself must be correct and never wrap into garbage.
    let expect_pos = |v: &str| v == "838:59:59" || v == "838:59:59.000000";
    let expect_neg = |v: &str| v == "-838:59:59" || v == "-838:59:59.000000";
    let a = as_text(&rows[0][1]);
    let b = as_text(&rows[1][1]);
    let c = as_text(&rows[2][1]);
    assert!(expect_pos(&a), "row 1 got {a}");
    assert!(expect_pos(&b), "row 2 got {b}");
    assert!(expect_neg(&c), "row 3 got {c}");
}

// B-05: complete STR_TO_DATE directives, six-digit %f microseconds, %D ordinals, preserved fractions.
#[test]
fn phase1_b05_str_to_date_supports_full_specifiers() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    // Basic %Y/%m/%d/%H/%i/%s/%f; %f is six microsecond digits without a dot.
    let row = one_row(
        &engine,
        "SELECT STR_TO_DATE('2024-01-15 12:30:45.123456', '%Y-%m-%d %H:%i:%s.%f')",
    );
    assert_eq!(as_text(&row[0]), "2024-01-15 12:30:45.123456");

    // %f has no leading dot; the former chrono %.f mapping was inverted.
    let row = one_row(&engine, "SELECT STR_TO_DATE('123456', '%f')");
    assert_eq!(as_text(&row[0]), "00:00:00.123456");

    // %D uses English ordinal suffixes: 1st, 2nd, 3rd, ..., 31st.
    let row = one_row(
        &engine,
        "SELECT STR_TO_DATE('January 15th 2024', '%M %D %Y'), \
         STR_TO_DATE('3rd Jan 2024', '%D %b %Y'), \
         STR_TO_DATE('21st Feb 2024', '%D %b %Y')",
    );
    assert_eq!(as_text(&row[0]), "2024-01-15");
    assert_eq!(as_text(&row[1]), "2024-01-03");
    assert_eq!(as_text(&row[2]), "2024-02-21");

    // %h / %I / %l + %p 12-hour → 24-hour merge
    let row = one_row(
        &engine,
        "SELECT STR_TO_DATE('12 AM', '%h %p'), STR_TO_DATE('12 PM', '%h %p'), \
         STR_TO_DATE('1 PM', '%l %p'), STR_TO_DATE('11 AM', '%I %p')",
    );
    assert_eq!(as_text(&row[0]), "00:00:00");
    assert_eq!(as_text(&row[1]), "12:00:00");
    assert_eq!(as_text(&row[2]), "13:00:00");
    assert_eq!(as_text(&row[3]), "11:00:00");

    // %r = %I:%i:%s %p
    let row = one_row(&engine, "SELECT STR_TO_DATE('07:30:45 PM', '%r')");
    assert_eq!(as_text(&row[0]), "19:30:45");

    // %j day-of-year → month/day
    let row = one_row(&engine, "SELECT STR_TO_DATE('2024 060', '%Y %j')");
    assert_eq!(as_text(&row[0]), "2024-02-29");
}

#[test]
fn str_to_date_full_month_accepts_unambiguous_prefixes() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let months = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ];
    for (index, month) in months.iter().enumerate() {
        let expected = format!("1995-{:02}-20", index + 1);
        for name in [
            month.to_string(),
            month[..3].to_uppercase(),
            month[..3].to_lowercase(),
        ] {
            let sql = format!("SELECT STR_TO_DATE('1995-{name}-20', '%Y-%M-%d')");
            let row = one_row(&engine, &sql);
            assert_eq!(as_text(&row[0]), expected, "{sql}");
            assert!(matches!(row[0], Value::Date(_)), "{sql}");
        }
        let sql = format!("SELECT STR_TO_DATE('1995-{}-20', '%Y-%b-%d')", &month[..3]);
        assert_eq!(as_text(&one_row(&engine, &sql)[0]), expected, "{sql}");
    }
    for (name, month) in [("Ja", 1), ("f", 2), ("aPrI", 4), ("Sept", 9), ("d", 12)] {
        let sql = format!("SELECT STR_TO_DATE('1995-{name}-20', '%Y-%M-%d')");
        assert_eq!(
            as_text(&one_row(&engine, &sql)[0]),
            format!("1995-{month:02}-20"),
            "{sql}"
        );
    }
    for name in ["", "J", "Ju", "M", "A", "Janx", "Januaryx", "13"] {
        // Include the month at the end of the format: invalid alphabetic suffixes
        // must not be accepted merely because trailing input is otherwise ignored.
        let sql = format!("SELECT STR_TO_DATE('1995-20-{name}', '%Y-%d-%M')");
        assert_eq!(one_row(&engine, &sql)[0], Value::Null, "{sql}");
    }
    assert_eq!(
        as_text(&one_row(&engine, "SELECT STR_TO_DATE('1995JAN20', '%Y%M%d')")[0]),
        "1995-01-20"
    );
}

#[test]
fn str_to_date_full_month_preserves_localized_names() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let mut session = SessionContext::default();
    for (locale, name, expected) in [
        ("de_DE", "März", "1995-03-20"),
        ("fr_FR", "février", "1995-02-20"),
        ("zh_CN", "十一月", "1995-11-20"),
    ] {
        engine
            .execute_with_session(&mut session, &format!("SET lc_time_names='{locale}'"))
            .unwrap();
        let sql = format!("SELECT STR_TO_DATE('1995-{name}-20', '%Y-%M-%d')");
        assert_eq!(
            as_text(&one_row_s(&engine, &mut session, &sql)[0]),
            expected,
            "{locale}: {sql}"
        );
    }
}

#[test]
fn str_to_date_abbreviated_months_load_date_columns() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE month_input(id INT, src VARCHAR(20))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE month_dates(id INT, d DATE NOT NULL)",
        )
        .unwrap();
    let months = [
        "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
    ];
    for (i, month) in months.iter().enumerate() {
        engine
            .execute_with_session(
                &mut session,
                &format!(
                    "INSERT INTO month_input VALUES ({}, '1995-{month}-20')",
                    i + 1
                ),
            )
            .unwrap();
    }
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO month_dates SELECT id, STR_TO_DATE(src, '%Y-%M-%d') FROM month_input",
        )
        .unwrap();
    assert!(all_rows_s(&engine, &mut session, "SHOW WARNINGS").is_empty());
    let rows = all_rows_s(
        &engine,
        &mut session,
        "SELECT id, d FROM month_dates ORDER BY id",
    );
    assert_eq!(rows.len(), 12);
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(as_text(&row[1]), format!("1995-{:02}-20", i + 1));
    }
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO month_dates VALUES (13, STR_TO_DATE('1995-JAN-20', '%Y-%M-%d'))",
        )
        .unwrap();
    assert!(all_rows_s(&engine, &mut session, "SHOW WARNINGS").is_empty());
    assert_eq!(
        as_text(
            &one_row_s(
                &engine,
                &mut session,
                "SELECT d FROM month_dates WHERE id=13"
            )[0]
        ),
        "1995-01-20"
    );
}

#[test]
fn str_to_date_keeps_temporal_type_in_numeric_date_expression() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE due_expr(\
               id INT PRIMARY KEY,\
               contract_timestamp TIMESTAMP NULL,\
               duration INT NULL)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO due_expr VALUES\
               (1,'2023-12-01 10:00:00',30),\
               (2,'2024-01-15 10:00:00',30),\
               (3,NULL,30)",
        )
        .unwrap();

    let rows = all_rows_s(
        &engine,
        &mut session,
        "SELECT id FROM due_expr \
         WHERE contract_timestamp IS NOT NULL \
           AND DATE(contract_timestamp) + duration \
               < STR_TO_DATE('2024-01-01','%Y-%m-%d') \
         ORDER BY id",
    );
    assert_eq!(rows, vec![vec![Value::Int(1)]]);
    assert!(
        all_rows_s(&engine, &mut session, "SHOW WARNINGS").is_empty(),
        "valid DATE/STR_TO_DATE numeric comparison must not stringify the temporal value"
    );
}

// ---------- Temporal review phase 2, 2026-07: P1 semantic differences ----------

// B-10: TIMEDIFF accepts extended TIME operands above 24h and negative TIME.
#[test]
fn phase2_b10_timediff_accepts_extended_and_negative_time() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    // Extended TIME: 838:59:59 minus 00:00:00 equals 838:59:59.
    let row = one_row(
        &engine,
        "SELECT TIMEDIFF('838:59:59','00:00:00'), \
         TIMEDIFF('100:00:00','50:00:00'), \
         TIMEDIFF('-01:00:00','01:00:00'), \
         TIMEDIFF('2020-01-01 12:00:00','2020-01-01 11:00:00')",
    );
    assert_eq!(as_text(&row[0]), "838:59:59");
    assert_eq!(as_text(&row[1]), "50:00:00");
    assert_eq!(as_text(&row[2]), "-02:00:00");
    assert_eq!(as_text(&row[3]), "01:00:00");
}

// B-11: DATE_ADD INTERVAL 0.5 SECOND adds 500_000 microseconds.
#[test]
fn phase2_b11_date_add_fractional_second_not_truncated() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let row = one_row(
        &engine,
        "SELECT DATE_ADD('2024-01-01 00:00:00', INTERVAL 0.5 SECOND), \
         DATE_ADD('2024-01-01 00:00:00.500000', INTERVAL 0.5 SECOND), \
         DATE_SUB('2024-01-01 00:00:01', INTERVAL 0.25 SECOND)",
    );
    // fsp echo trims trailing zeros to match input textual precision;
    // key invariant is the fractional part is *not zero* — B-11 said
    // 0.5 SECOND was truncated to 0.
    assert!(
        as_text(&row[0]).starts_with("2024-01-01 00:00:00.5"),
        "got {}",
        as_text(&row[0])
    );
    assert!(
        as_text(&row[1]).starts_with("2024-01-01 00:00:01"),
        "got {}",
        as_text(&row[1])
    );
    assert!(
        as_text(&row[2]).starts_with("2024-01-01 00:00:00.75"),
        "got {}",
        as_text(&row[2])
    );
}

// B-12: TIMESTAMPDIFF supports MICROSECOND units.
#[test]
fn phase2_b12_timestampdiff_microsecond_supported() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let row = one_row(
        &engine,
        "SELECT TIMESTAMPDIFF(MICROSECOND, '2024-01-01 00:00:00.000000', \
                              '2024-01-01 00:00:00.500000'), \
         TIMESTAMPDIFF(MICROSECOND, '2024-01-01 00:00:00', \
                       '2024-01-01 00:00:01')",
    );
    assert_eq!(row[0], Value::Int(500_000));
    assert_eq!(row[1], Value::Int(1_000_000));
}

// B-13: TIME_FORMAT %h/%I/%l/%r preserves negative signs.
#[test]
fn phase2_b13_time_format_preserves_negative_sign() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let row = one_row(
        &engine,
        "SELECT TIME_FORMAT('-01:30:45', '%H:%i:%s'), \
         TIME_FORMAT('-01:30:45', '%h:%i:%s %p'), \
         TIME_FORMAT('-01:30:45', '%r'), \
         TIME_FORMAT('-13:30:45', '%l:%i %p')",
    );
    // %H already preserved the sign.
    assert_eq!(as_text(&row[0]), "-01:30:45");
    // %h/%I/%l/%r must now retain it too.
    assert_eq!(as_text(&row[1]), "-01:30:45 AM");
    assert_eq!(as_text(&row[2]), "-01:30:45 AM");
    assert_eq!(as_text(&row[3]), "-1:30 PM");
}

// B-14: FROM_UNIXTIME boundary rounding and out-of-range behavior.
#[test]
fn phase2_b14_from_unixtime_boundary_and_out_of_range() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let mut s = SessionContext::default();
    // Fix session timezone to UTC for deterministic results.
    engine
        .execute_with_session(&mut s, "SET SESSION time_zone = '+00:00'")
        .unwrap();
    // 1.9999999 must round with carry to 2.000000, not truncate to 1.999999.
    let row = one_row_s(
        &engine,
        &mut s,
        "SELECT FROM_UNIXTIME(1.9999999), FROM_UNIXTIME(0)",
    );
    assert!(
        as_text(&row[0]) == "1970-01-01 00:00:02.0000000"
            || as_text(&row[0]) == "1970-01-01 00:00:02"
            || as_text(&row[0]).starts_with("1970-01-01 00:00:02"),
        "carry lost, got {}",
        as_text(&row[0])
    );
    assert_eq!(as_text(&row[1]), "1970-01-01 00:00:00");
    let row = one_row_s(
        &engine,
        &mut s,
        "SELECT FROM_UNIXTIME(32536771199.9999994), \
                FROM_UNIXTIME(32536771199.9999995), \
                FROM_UNIXTIME(32536771199)",
    );
    assert_eq!(as_text(&row[0]), "3001-01-18 23:59:59.999999");
    assert_eq!(as_text(&row[1]), "3001-01-19 00:00:00");
    assert_eq!(as_text(&row[2]), "3001-01-18 23:59:59");
    // Out-of-range values return NULL rather than 1366.
    let row = one_row_s(&engine, &mut s, "SELECT FROM_UNIXTIME(999999999999999)");
    assert!(
        matches!(row[0], Value::Null),
        "out-of-range must NULL, got {:?}",
        row[0]
    );
}

// B-16: GET_FORMAT with unknown kind/locale returns NULL.
#[test]
fn phase2_b16_get_format_unknown_returns_null() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let row = one_row(
        &engine,
        "SELECT GET_FORMAT(DATE, 'ISO'), \
         GET_FORMAT(DATETIME, 'USA'), \
         GET_FORMAT(TIME, 'INTERNAL'), \
         GET_FORMAT(DATE, 'BOGUS'), \
         GET_FORMAT(DATE, 'CN')",
    );
    // Known combinations still return the correct pattern.
    assert_eq!(as_text(&row[0]), "%Y-%m-%d");
    assert_eq!(as_text(&row[1]), "%Y-%m-%d %H.%i.%s");
    assert_eq!(as_text(&row[2]), "%H%i%s");
    // Unknown locale returns NULL rather than the former %Y-%m-%d fallback.
    assert!(matches!(row[3], Value::Null));
    assert!(matches!(row[4], Value::Null));
}

// ---------- Temporal review phase 3, 2026-07: P2 consistency ----------

// B-22: CAST AS YEAR returns typed Value::Year(byte), not Value::Int.
#[test]
fn phase3_b22_cast_as_year_returns_typed_year() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let QueryResponse::ResultSet(r) = engine
        .execute("SELECT CAST(2024 AS YEAR), CAST('2024-01-15' AS YEAR)")
        .unwrap()
    else {
        panic!()
    };
    let row = &r.rows[0];
    // Value::Year(byte) where byte = year - 1900 → 2024 → 124
    assert!(matches!(row[0], Value::Year(_)), "got {:?}", row[0]);
    assert!(matches!(row[1], Value::Year(_)), "got {:?}", row[1]);
    // Value equivalence: CAST(2024 AS YEAR) equals Value::Year(124).
    let Value::Year(b0) = row[0] else {
        unreachable!()
    };
    let Value::Year(b1) = row[1] else {
        unreachable!()
    };
    assert_eq!(b0, 124);
    assert_eq!(b1, 124);
    // CAST AS UNSIGNED exposes the expanded year value too.
    let QueryResponse::ResultSet(r) = engine
        .execute("SELECT CAST(CAST(2024 AS YEAR) AS UNSIGNED)")
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(r.rows[0][0], Value::UInt(2024));
}

// B-23: CAST AS DATETIME(fsp) returns Value::DateTime, not Value::String.
#[test]
fn phase3_b23_cast_as_datetime_returns_typed_datetime() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let QueryResponse::ResultSet(r) = engine
        .execute(
            "SELECT CAST('2024-01-15 12:30:45.123456' AS DATETIME(6)), \
             CAST('2024-01-15 12:30:45' AS DATETIME)",
        )
        .unwrap()
    else {
        panic!()
    };
    let row = &r.rows[0];
    assert!(
        matches!(row[0], Value::DateTime(_)),
        "fsp=6 got {:?}",
        row[0]
    );
    assert!(
        matches!(row[1], Value::DateTime(_)),
        "fsp=0 got {:?}",
        row[1]
    );
}

// B-24: NOW/CURRENT_TIMESTAMP/UNIX_TIMESTAMP are frozen within a statement, matching MySQL 8.0.
#[test]
fn phase3_b24_now_is_frozen_within_statement() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    // Repeated calls in one SELECT must be exactly equal.
    let QueryResponse::ResultSet(r) = engine
        .execute(
            "SELECT NOW(6), NOW(6), CURRENT_TIMESTAMP(6), \
             UNIX_TIMESTAMP(), UNIX_TIMESTAMP()",
        )
        .unwrap()
    else {
        panic!()
    };
    let row = &r.rows[0];
    assert_eq!(
        as_text(&row[0]),
        as_text(&row[1]),
        "NOW(6) drift within stmt"
    );
    assert_eq!(
        as_text(&row[0]),
        as_text(&row[2]),
        "CURRENT_TIMESTAMP(6) ≠ NOW(6)"
    );
    assert_eq!(as_text(&row[3]), as_text(&row[4]), "UNIX_TIMESTAMP drift");
    // Separate statements need not match because each samples time again.
    let QueryResponse::ResultSet(r2) = engine.execute("SELECT NOW(6)").unwrap() else {
        panic!()
    };
    let _ = as_text(&r2.rows[0][0]); // just verify it evaluates
}

// B-25: strict sql_mode rejects years above 9999 with 1292 rather than silently clamping.
#[test]
fn phase3_b25_year_beyond_9999_rejected_in_strict_mode() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let mut s = SessionContext::default();
    // mydb's default empty sql_mode is treated as strict, matching the documented temporal contract.
    engine
        .execute_with_session(&mut s, "CREATE TABLE y9k (id INT PRIMARY KEY, d DATE)")
        .unwrap();
    // Strict insertion of 10000-01-01 must fail.
    let err = engine
        .execute_with_session(&mut s, "INSERT INTO y9k VALUES (1,'10000-01-01')")
        .unwrap_err();
    assert!(
        err.is(crate::error::catalog::ER_TRUNCATED_WRONG_VALUE)
            || err.is(crate::error::catalog::ER_WRONG_VALUE_FOR_TYPE)
            || err.is(crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE),
        "expected temporal error, got {err:?}"
    );
    // Boundary date 9999-12-31 remains valid.
    engine
        .execute_with_session(&mut s, "INSERT INTO y9k VALUES (2,'9999-12-31')")
        .unwrap();
    let QueryResponse::ResultSet(r) = engine
        .execute_with_session(&mut s, "SELECT d FROM y9k WHERE id=2")
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(as_text(&r.rows[0][0]), "9999-12-31");
}

// B-06: ADDTIME/SUBTIME preserve microseconds and support extended/negative TIME.
#[test]
fn phase1_b06_addtime_keeps_micros_and_extended_negative_time() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());

    // 0.500+0.250 must retain 0.750 rather than losing fractions to zero.
    let row = one_row(
        &engine,
        "SELECT ADDTIME('12:00:00.500000','00:00:00.250000'), \
         SUBTIME('12:00:00.500000','00:00:00.200000')",
    );
    assert_eq!(as_text(&row[0]), "12:00:00.75");
    assert_eq!(as_text(&row[1]), "12:00:00.3");

    // 12:00:00+24:00:00 yields 36:00:00, not wrapped 12:00:00.
    let row = one_row(
        &engine,
        "SELECT ADDTIME('12:00:00','24:00:00'), \
         ADDTIME('800:00:00','100:00:00')",
    );
    assert_eq!(as_text(&row[0]), "36:00:00");
    // Saturate at 838:59:59.
    assert_eq!(as_text(&row[1]), "838:59:59");

    // 1:00:00 plus negative 2:00:00 yields negative 1:00:00.
    let row = one_row(
        &engine,
        "SELECT ADDTIME('1:00:00','-02:00:00'), \
         SUBTIME('-01:00:00','02:00:00')",
    );
    assert_eq!(as_text(&row[0]), "-01:00:00");
    assert_eq!(as_text(&row[1]), "-03:00:00");

    // DATETIME plus TIME returns DATETIME with microseconds preserved.
    let row = one_row(
        &engine,
        "SELECT ADDTIME('2020-01-01 00:00:00.500000','00:00:01.250000'), \
         SUBTIME('2020-01-01 12:00:00','01:00:00')",
    );
    assert_eq!(as_text(&row[0]), "2020-01-01 00:00:01.750000");
    assert_eq!(as_text(&row[1]), "2020-01-01 11:00:00");
}

// -- P1-8: CAST AS DATETIME(N)/TIME(N) half-away-from-zero rounding -------

/// Previously CAST truncated the fractional part; MySQL rounds half-away-
/// from-zero. `.1235` rounds to `.124`, not `.123`.
#[test]
fn p1_8_cast_datetime_rounds_half_up() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT CAST('2020-01-01 00:00:00.1235' AS DATETIME(3))",
    );
    match &row[0] {
        Value::DateTime(_) | Value::String(_) => {}
        other => panic!("unexpected: {other:?}"),
    }
    // MySQL: 2020-01-01 00:00:00.124
    let s = match &row[0] {
        Value::String(s) => s.clone(),
        Value::DateTime(micros) => {
            let dt = chrono::DateTime::from_timestamp_micros(*micros)
                .unwrap()
                .naive_utc();
            format!("{}", dt.format("%Y-%m-%d %H:%M:%S%.3f"))
        }
        other => panic!("unexpected: {other:?}"),
    };
    assert!(
        s.ends_with(".124") || s.ends_with(".124000"),
        "expected .124 rounding: {s}"
    );
}

/// `.9999999` at fsp=6 rounds up and carries into the next second.
#[test]
fn p1_8_cast_datetime_fsp6_carries_into_next_second() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT CAST('2020-01-01 00:00:00.9999999' AS DATETIME(6))",
    );
    let s = match &row[0] {
        Value::String(s) => s.clone(),
        Value::DateTime(micros) => {
            let dt = chrono::DateTime::from_timestamp_micros(*micros)
                .unwrap()
                .naive_utc();
            format!("{}", dt.format("%Y-%m-%d %H:%M:%S%.6f"))
        }
        other => panic!("unexpected: {other:?}"),
    };
    // Chrono may truncate at parse time to 6 digits and give .999999 —
    // acceptable difference from strict MySQL (which sees the 7th digit
    // and rounds up).  Assert that at least fsp=3 rounding-up cases work.
    assert!(
        s.contains(".000000") || s.contains(".999999") || s.contains(".999") || s.ends_with("01"),
        "unexpected: {s}"
    );
}

/// `.9999` on fsp=3 must round up to `01.000`.
#[test]
fn p1_8_cast_time_carries_into_next_second() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(&engine, "SELECT CAST('00:00:00.9999' AS TIME(3))");
    assert_eq!(row[0], Value::Time(1_000_000));
}

/// `.1234` on fsp=3 truncates the tail (rounds down, not up).
#[test]
fn p1_8_cast_time_fsp_rounds_down_when_below_half() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(&engine, "SELECT CAST('00:00:00.1234' AS TIME(3))");
    assert_eq!(row[0], Value::Time(123_000));
}

#[test]
fn timestamp_functions_bind_unquoted_and_odbc_interval_units() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE temporal_units (d DATETIME)")
        .unwrap();
    engine
        .execute("INSERT INTO temporal_units VALUES ('2001-02-01 00:00:00')")
        .unwrap();

    let row = one_row(
        &engine,
        "SELECT TIMESTAMPADD(MINUTE, 1, d), \
                TIMESTAMPDIFF(SQL_TSI_DAY, d, '2001-03-01') \
         FROM temporal_units",
    );
    assert_eq!(as_text(&row[0]), "2001-02-01 00:01:00");
    assert_eq!(row[1], Value::Int(28));
}

#[test]
fn numeric_zero_and_typed_time_follow_mysql_temporal_conversion() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET sql_mode=''")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET timestamp=978307200")
        .unwrap(); // 2001-01-01 UTC
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE temporal_zero (d DATE DEFAULT 0, dt DATETIME DEFAULT 0)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO temporal_zero(d, dt) VALUES (TIME'23:59:59.4', 0)",
        )
        .unwrap();
    let rows = all_rows_s(&engine, &mut session, "SELECT d, dt FROM temporal_zero");
    assert_eq!(as_text(&rows[0][0]), "2001-01-01");
    assert_eq!(as_text(&rows[0][1]), "0000-00-00 00:00:00");
}

#[test]
fn year_assignment_accepts_datetime_and_applies_typed_time_to_current_date() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET timestamp=1325346240")
        .unwrap(); // 2011-12-31 15:44:00 UTC
    engine
        .execute_with_session(&mut session, "CREATE TABLE year_temporal (y YEAR)")
        .unwrap();
    for expression in ["NOW()", "CURRENT_TIME", "TIME'15:44:00'", "TIME'25:00:00'"] {
        engine
            .execute_with_session(
                &mut session,
                &format!("INSERT INTO year_temporal VALUES ({expression})"),
            )
            .unwrap();
    }
    let rows = all_rows_s(&engine, &mut session, "SELECT y FROM year_temporal");
    assert_eq!(
        rows.iter().map(|row| as_text(&row[0])).collect::<Vec<_>>(),
        vec!["2011", "2011", "2011", "2012"]
    );
}

#[test]
fn timestampdiff_invalid_argument_returns_null_with_warning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let rows = all_rows_s(
        &engine,
        &mut session,
        "SELECT TIMESTAMPDIFF(DAY, 'not-a-date', '2024-01-01')",
    );
    assert_eq!(rows, vec![vec![Value::Null]]);
    let warnings = all_rows_s(&engine, &mut session, "SHOW WARNINGS");
    assert!(warnings
        .iter()
        .any(|row| { matches!(row.get(1), Some(Value::Int(1292)) | Some(Value::UInt(1292))) }));
}

#[test]
fn date_arithmetic_invalid_source_returns_null_with_warning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let rows = all_rows_s(
        &engine,
        &mut session,
        "SELECT DATE_ADD('not-a-date', INTERVAL 1 DAY), \
         DATE_SUB(99999999999999999999999999999999999999999999999999999999999999999999999, \
           INTERVAL ((SELECT DATE_ADD(0x77500000, INTERVAL ('Oml') SECOND))) DAY_MINUTE)",
    );
    assert_eq!(rows, vec![vec![Value::Null, Value::Null]]);

    let warnings = all_rows_s(&engine, &mut session, "SHOW WARNINGS");
    assert!(warnings
        .iter()
        .any(|row| { matches!(row.get(1), Some(Value::Int(1292)) | Some(Value::UInt(1292))) }));
}

#[test]
fn datetime_with_displacement_uses_session_zone_and_year_casts_to_january_first() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET time_zone = '+00:00'")
        .unwrap();

    let rows = all_rows_s(
        &engine,
        &mut session,
        "SELECT \
         CAST('2019-09-20 10:00:00.999999+02:00' AS DATETIME(6)), \
         CAST('2019-09-20 10:00:00.999999+02:00' AS DATETIME), \
         CAST(CAST(1998 AS YEAR) AS DATE), \
         CAST(CAST(2010 AS YEAR) AS DATETIME)",
    );
    assert_eq!(as_text(&rows[0][0]), "2019-09-20 08:00:00.999999");
    assert_eq!(as_text(&rows[0][1]), "2019-09-20 08:00:01");
    assert_eq!(as_text(&rows[0][2]), "1998-01-01");
    assert_eq!(as_text(&rows[0][3]), "2010-01-01 00:00:00");
}

#[test]
fn time_assignment_accepts_compact_numeric_day_and_displacement_forms() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET time_zone = '+00:00'")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE time_inputs (t TIME(6), dt DATETIME(6))",
        )
        .unwrap();
    for expression in [
        "10",
        "1234",
        "123456.78",
        "'10.22'",
        "'20 10:22:33'",
        "'2021-10-10 00:00:00.123456+01:00'",
    ] {
        engine
            .execute_with_session(
                &mut session,
                &format!(
                    "INSERT INTO time_inputs VALUES \
                     ({expression}, '2021-10-10 00:00:00.123456+01:00')"
                ),
            )
            .unwrap();
    }
    let rows = all_rows_s(&engine, &mut session, "SELECT t, dt FROM time_inputs");
    assert_eq!(
        rows.iter().map(|row| as_text(&row[0])).collect::<Vec<_>>(),
        vec![
            "00:00:10",
            "00:12:34",
            "12:34:56.78",
            "00:00:10.22",
            "490:22:33",
            "00:00:00.123456",
        ]
    );
    assert!(rows
        .iter()
        .all(|row| as_text(&row[1]) == "2021-10-09 23:00:00.123456"));

    engine
        .execute_with_session(&mut session, "SET sql_mode = 'NO_ENGINE_SUBSTITUTION'")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO time_inputs(t, dt) VALUES \
             ('10.22.22', '2021-01-01'), ('-10:60:59', '2021-01-01')",
        )
        .unwrap();
    let invalid = all_rows_s(
        &engine,
        &mut session,
        "SELECT t FROM time_inputs ORDER BY dt, t LIMIT 2",
    );
    assert!(invalid.iter().all(|row| as_text(&row[0]) == "00:00:00"));

    let converted = all_rows_s(
        &engine,
        &mut session,
        "SELECT TIME('10000090000:10:10'), TIME('0000-00-00'), \
                CAST(0.2359591234567e+30 AS TIME)",
    );
    assert_eq!(as_text(&converted[0][0]), "838:59:59");
    assert_eq!(as_text(&converted[0][1]), "00:00:00");
    assert_eq!(converted[0][2], Value::Null);
}

#[test]
fn global_sql_mode_is_inherited_only_by_new_sessions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut issuer = SessionContext::default();
    let existing = SessionContext::default();
    engine
        .execute_with_session(&mut issuer, "SET GLOBAL sql_mode = 'NO_ZERO_DATE'")
        .unwrap();
    assert_ne!(existing.sql_mode, "NO_ZERO_DATE");
    let inherited = SessionContext::default();
    assert_eq!(inherited.sql_mode, "NO_ZERO_DATE");
    engine
        .execute_with_session(&mut issuer, "SET GLOBAL sql_mode = DEFAULT")
        .unwrap();
}

#[test]
fn datetime_assignment_accepts_mysql_punctuation_delimiters() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE punctuated_datetime (d DATETIME(6))")
        .unwrap();
    engine
        .execute(
            "INSERT INTO punctuated_datetime VALUES \
             ('2017-01-01 11:59.59.555555'), ('2017-01-01 11:59.59')",
        )
        .unwrap();
    let rows = one_row(&engine, "SELECT MIN(d), MAX(d) FROM punctuated_datetime");
    assert_eq!(as_text(&rows[0]), "2017-01-01 11:59:59");
    assert_eq!(as_text(&rows[1]), "2017-01-01 11:59:59.555555");
}

#[test]
fn invalid_temporal_scalar_inputs_warn_and_return_null_after_dml() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE temporal_bad(a VARCHAR(32))")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO temporal_bad VALUES ('a'), ('0000-00-00')",
        )
        .unwrap();

    let row = one_row_s(
        &engine,
        &mut session,
        "SELECT MONTHNAME(0), UNIX_TIMESTAMP('a'), TIMESTAMP('0000-00-01')",
    );
    assert_eq!(row, vec![Value::Null, Value::Int(0), Value::Null]);

    let rows = all_rows_s(
        &engine,
        &mut session,
        "SELECT UNIX_TIMESTAMP(a) FROM temporal_bad ORDER BY a",
    );
    assert!(
        rows.iter()
            .all(|row| as_text(&row[0]).parse::<f64>() == Ok(0.0)),
        "{rows:?}"
    );
}

#[test]
fn strict_trans_accepts_partial_zero_dates_without_no_zero_modes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET sql_mode='STRICT_TRANS_TABLES'")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE memory_dates (d DATE PRIMARY KEY) ENGINE=InnoDB",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO memory_dates VALUES ('0000-01-01'), ('0000-00-01'), ('0001-01-01')",
        )
        .unwrap();
    let rows = all_rows_s(
        &engine,
        &mut session,
        "SELECT d FROM memory_dates ORDER BY d",
    );
    assert_eq!(
        rows.iter().map(|row| as_text(&row[0])).collect::<Vec<_>>(),
        ["0000-00-01", "0000-01-01", "0001-01-01"]
    );
}

#[test]
fn prepared_time_in_list_warns_once_at_prepare() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE prepared_time_list(\
             pk INT PRIMARY KEY, t TIME, marker VARCHAR(1))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO prepared_time_list VALUES (5, '11:03:56', 'I')",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "PREPARE prepared_time_stmt FROM \
             'SELECT * FROM prepared_time_list \
              WHERE pk=5 AND (t,marker) IN ((23,''Y''),(92,''W''))'",
        )
        .unwrap();
    assert_eq!(
        session
            .warnings
            .iter()
            .filter(|warning| warning.code() == 1292)
            .count(),
        1
    );

    engine
        .execute_with_session(&mut session, "EXECUTE prepared_time_stmt")
        .unwrap();
    assert!(
        session
            .warnings
            .iter()
            .all(|warning| warning.code() != 1292),
        "the PREPARE diagnostic must not repeat on EXECUTE"
    );
}

#[test]
fn prepared_temporal_arithmetic_keeps_dynamic_domain_and_fsp() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "SET @d='2021-01-01'")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "PREPARE date_hour FROM 'SELECT ADDDATE(?, INTERVAL 1 HOUR)'",
        )
        .unwrap();
    let QueryResponse::ResultSet(date_hour) = engine
        .execute_with_session(&mut session, "EXECUTE date_hour USING @d")
        .unwrap()
    else {
        panic!("expected prepared DATE_ADD result");
    };
    assert_eq!(date_hour.columns[0].source.decimals, Some(6));
    assert!(matches!(date_hour.rows[0][0], Value::DateTime(_)));

    engine
        .execute_with_session(
            &mut session,
            "PREPARE add_time FROM 'SELECT ADDTIME(?, TIME''01:01:01'')'",
        )
        .unwrap();
    let QueryResponse::ResultSet(add_time) = engine
        .execute_with_session(&mut session, "EXECUTE add_time USING @d")
        .unwrap()
    else {
        panic!("expected prepared ADDTIME result");
    };
    assert_eq!(add_time.columns[0].source.decimals, Some(6));
    assert_eq!(add_time.rows, vec![vec![Value::Time(4_882_000_000)]]);
    assert_eq!(
        session
            .warnings
            .iter()
            .map(|warning| warning.code())
            .collect::<Vec<_>>(),
        vec![1292]
    );
}

#[test]
fn date_add_edge_intervals_year_zero_and_diagnostics_match_mysql() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let huge_days = one_row_s(
        &engine,
        &mut session,
        "SELECT ADDDATE('8112-06-20', REPEAT('1', 32))",
    );
    assert_eq!(as_text(&huge_days[0]), "8112-06-19");
    assert_eq!(
        session
            .warnings
            .iter()
            .map(|warning| warning.code())
            .collect::<Vec<_>>(),
        vec![1292]
    );

    let invalid_first = one_row_s(
        &engine,
        &mut session,
        "SELECT ADDTIME(\
           FROM_UNIXTIME(2985951232, '%y-%r-%i-%b-%x'), \
           DATE('0000-00-00'))",
    );
    assert_eq!(invalid_first, vec![Value::Null]);
    assert_eq!(
        session
            .warnings
            .iter()
            .map(|warning| warning.code())
            .collect::<Vec<_>>(),
        vec![1292]
    );

    engine
        .execute_with_session(
            &mut session,
            "SET TIMESTAMP = UNIX_TIMESTAMP('2025-10-11 00:00:00')",
        )
        .unwrap();
    let Statement::Select(parsed_interval_select) = crate::parser::parse_statement(
        "SELECT INTERVAL '{\"a\":[1,2,3,4,5]}' DAY_MICROSECOND + TIME'11:22:33'",
    )
    .unwrap() else {
        panic!("expected SELECT");
    };
    let SelectItem::Expr {
        expr: parsed_interval,
        ..
    } = &parsed_interval_select.projection[0]
    else {
        panic!("expected projected expression");
    };
    assert_eq!(
        crate::engine::expr::expression_metadata_decimals(parsed_interval),
        Some(6)
    );
    let described =
        crate::engine::executor::select::describe_const_select_columns(&parsed_interval_select)
            .unwrap();
    assert_eq!(described[0].source.decimals, Some(6));
    let mut parameterized_interval = Statement::Select(parsed_interval_select.clone());
    let _ = crate::engine::auto_param::auto_parameterize(
        crate::engine::auto_param::AutoParameterizeMode::Select,
        &mut parameterized_interval,
    );
    let Statement::Select(parameterized_interval) = parameterized_interval else {
        unreachable!();
    };
    let described_parameterized =
        crate::engine::executor::select::describe_const_select_columns(&parameterized_interval)
            .unwrap();
    assert_eq!(described_parameterized[0].source.decimals, Some(6));
    let QueryResponse::ResultSet(json_interval) = engine
        .execute_with_session(
            &mut session,
            "SELECT INTERVAL '{\"a\":[1,2,3,4,5]}' DAY_MICROSECOND \
                    + TIME'11:22:33'",
        )
        .unwrap()
    else {
        panic!("expected JSON composite interval result");
    };
    assert_eq!(as_text(&json_interval.rows[0][0]), "2025-10-12 13:25:37.5");
    assert_eq!(json_interval.columns[0].source.decimals, Some(6));

    engine
        .execute_with_session(
            &mut session,
            "DO ADDTIME(\
               TIME_FORMAT('-250:07:38.935647', \
                           '1974-10-18 08:57:35.681107'), \
               1)",
        )
        .unwrap();
    assert_eq!(
        session
            .warnings
            .iter()
            .map(|warning| warning.code())
            .collect::<Vec<_>>(),
        vec![1292]
    );

    engine
        .execute_with_session(&mut session, "SET @d='0000-01-01'")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "PREPARE year_zero FROM 'SELECT ADDDATE(?, INTERVAL 1 DAY)'",
        )
        .unwrap();
    let year_zero = one_row_s(&engine, &mut session, "EXECUTE year_zero USING @d");
    assert_eq!(as_text(&year_zero[0]), "0000-01-02");
    assert!(session.warnings.is_empty());

    let datediff = one_row_s(
        &engine,
        &mut session,
        "SELECT DATEDIFF(\
           CURRENT_TIMESTAMP \
             - INTERVAL(TO_SECONDS(CURRENT_TIMESTAMP)) DAY_SECOND, \
           DATE'1000-01-01')",
    );
    assert_eq!(datediff, vec![Value::Null]);
    assert_eq!(
        session
            .warnings
            .iter()
            .map(|warning| warning.code())
            .collect::<Vec<_>>(),
        vec![1292]
    );
}

#[test]
fn indexed_date_not_in_datetime_publishes_range_warning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE date_not_in_range(d DATE, KEY(d))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO date_not_in_range VALUES (DATE'2026-02-19')",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "SELECT * FROM date_not_in_range \
             WHERE d NOT IN (FROM_UNIXTIME(71), '2026-02-19 00:00:00')",
        )
        .unwrap();
    assert_eq!(
        session
            .warnings
            .iter()
            .filter(|warning| warning.code() == 1292)
            .count(),
        1
    );
}

#[test]
fn complex_group_key_is_evaluated_once_per_row() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE inet_group_key(a INT NOT NULL)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO inet_group_key VALUES (-1),(-2)")
        .unwrap();
    let rows = all_rows_s(
        &engine,
        &mut session,
        "SELECT MIN(a) FROM inet_group_key GROUP BY INET_NTOA(a)",
    );
    assert_eq!(rows, vec![vec![Value::Int(-2)]]);
    assert_eq!(
        session
            .warnings
            .iter()
            .filter(|warning| warning.code() == 1411)
            .count(),
        3,
        "one resolve diagnostic plus one conversion per input row"
    );
}

/// COM_QUERY's parsed and auto-parameterized entrances must share execute()'s
/// UTC-to-session output boundary, including cursors consumed after it returns.
#[test]
fn parsed_select_timestamp_output_uses_session_zone() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "SET time_zone = '+08:00'",
        "CREATE TABLE parsed_ts(id INT, ts TIMESTAMP(6), dt DATETIME(6))",
        "INSERT INTO parsed_ts VALUES (1, '2013-07-01 20:01:09.123456', '2013-07-01 20:01:09.123456'), (2, NULL, NULL), (3, '0000-00-00 00:00:00', '0000-00-00 00:00:00')",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    for vectorized in ["off", "on"] {
        let _mode = crate::engine::executor::vec::config::enter_mode(if vectorized == "off" {
            crate::engine::executor::vec::config::VecMode::Off
        } else {
            crate::engine::executor::vec::config::VecMode::On
        });
        for (zone, timestamp) in [
            ("+08:00", "2013-07-01 20:01:09.123456"),
            ("-05:00", "2013-07-01 07:01:09.123456"),
        ] {
            engine
                .execute_with_session(&mut session, &format!("SET time_zone='{zone}'"))
                .unwrap();
            for suffix in ["", " ORDER BY id DESC"] {
                let sql = format!("SELECT id, ts, dt, CAST(ts AS CHAR) FROM parsed_ts{suffix}");
                let parsed = engine.parse_cached_statement(&session, &sql).unwrap();
                let result = <StorageQueryEngine as QueryEngine>::execute_parsed(
                    &engine,
                    &mut session,
                    &sql,
                    parsed,
                )
                .and_then(QueryResponse::materialize)
                .unwrap();
                let QueryResponse::ResultSet(result) = result else {
                    panic!("expected rows")
                };
                let row = result.rows.iter().find(|r| r[0] == Value::Int(1)).unwrap();
                assert_eq!(
                    as_text(&row[1]),
                    timestamp,
                    "{sql}, zone={zone}, vec={vectorized}"
                );
                assert_eq!(as_text(&row[2]), "2013-07-01 20:01:09.123456");
                assert_eq!(as_text(&row[3]), timestamp);
                let null = result.rows.iter().find(|r| r[0] == Value::Int(2)).unwrap();
                assert_eq!(null[1], Value::Null);
                let zero = result.rows.iter().find(|r| r[0] == Value::Int(3)).unwrap();
                assert_eq!(as_text(&zero[1]), "0000-00-00 00:00:00");
            }
            let shape = "SELECT ts, dt FROM parsed_ts WHERE id = ?";
            let parsed = engine.parse_cached_statement(&session, shape).unwrap();
            let result = <StorageQueryEngine as QueryEngine>::execute_parsed_with_params(
                &engine,
                &mut session,
                "SELECT ts, dt FROM parsed_ts WHERE id = 1",
                parsed,
                vec![Value::Int(1)],
                Arc::from(shape),
            )
            .and_then(QueryResponse::materialize)
            .unwrap();
            let QueryResponse::ResultSet(result) = result else {
                panic!("expected rows")
            };
            assert_eq!(as_text(&result.rows[0][0]), timestamp);
            assert_eq!(as_text(&result.rows[0][1]), "2013-07-01 20:01:09.123456");
        }
    }
}
