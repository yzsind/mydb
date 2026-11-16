//! Product regressions for mydb's extended UTC TIMESTAMP domain.
use super::*;

fn rows(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> Vec<Vec<Value>> {
    match engine.execute_with_session(session, sql).unwrap() {
        QueryResponse::ResultSet(result) => result.rows,
        other => panic!("expected result set for {sql}: {other:?}"),
    }
}

fn utc_session(engine: &StorageQueryEngine) -> SessionContext {
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET time_zone='+00:00'")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "SET sql_mode='STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'",
        )
        .unwrap();
    session
}

#[test]
fn timestamp_9999_all_precisions_and_assignment_lanes() {
    for mode in [vec::config::VecMode::Off, vec::config::VecMode::On] {
        let _scope = vec::config::enter_mode(mode);
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_test_engine(dir.path());
        let mut session = utc_session(&engine);
        for fsp in 0..=6 {
            let suffix = if fsp == 0 {
                String::new()
            } else {
                format!(".{}", "9".repeat(fsp))
            };
            let text = format!("9999-12-31 23:59:59{suffix}");
            engine
                .execute_with_session(
                    &mut session,
                    &format!(
                "CREATE TABLE t{fsp}(id INT PRIMARY KEY, ts TIMESTAMP({fsp}), KEY idx_ts(ts))"
            ),
                )
                .unwrap();
            engine
                .execute_with_session(
                    &mut session,
                    &format!(
                        "INSERT INTO t{fsp} VALUES (1,'{text}'),(2,99991231235959{suffix}),\
                 (3,TIMESTAMP('{text}'))"
                    ),
                )
                .unwrap();
            let actual = rows(
                &engine,
                &mut session,
                &format!("SELECT ts FROM t{fsp} ORDER BY id"),
            );
            assert_eq!(actual.len(), 3);
            for row in actual {
                assert_eq!(crate::storage::value_to_string(&row[0]), text);
            }
            for hint in ["FORCE INDEX(idx_ts)", "FORCE INDEX(PRIMARY)"] {
                assert_eq!(
                    rows(
                        &engine,
                        &mut session,
                        &format!("SELECT COUNT(*) FROM t{fsp} {hint} WHERE ts='{text}'")
                    )[0][0],
                    Value::Int(3)
                );
            }
            engine
                .execute_with_session(
                    &mut session,
                    &format!("UPDATE t{fsp} SET ts='2040-01-01 00:00:00' WHERE id=1"),
                )
                .unwrap();
            engine
                .execute_with_session(
                    &mut session,
                    &format!("INSERT INTO t{fsp} SELECT 4,ts FROM t{fsp} WHERE id=3"),
                )
                .unwrap();
            assert_eq!(
                rows(
                    &engine,
                    &mut session,
                    &format!("SELECT COUNT(*) FROM t{fsp} WHERE ts='{text}'")
                )[0][0],
                Value::Int(3)
            );
        }
    }
}

#[test]
fn timestamp_9999_assignment_rounds_once_at_declared_precision() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = utc_session(&engine);
    engine.execute_with_session(&mut session,
        "CREATE TABLE fraction_ts(t0 TIMESTAMP(0), t3 TIMESTAMP(3), t6 TIMESTAMP(6), d6 DATETIME(6))").unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO fraction_ts VALUES('4000-01-01 00:00:00.4999999',\
         '4000-01-01 00:00:00.1234999','4000-01-01 00:00:00.1234565',\
         '4000-01-01 00:00:00.1234565')",
        )
        .unwrap();
    let result = rows(&engine, &mut session, "SELECT * FROM fraction_ts");
    let text: Vec<_> = result[0]
        .iter()
        .map(crate::storage::value_to_string)
        .collect();
    assert_eq!(
        text,
        [
            "4000-01-01 00:00:00",
            "4000-01-01 00:00:00.123",
            "4000-01-01 00:00:00.123457",
            "4000-01-01 00:00:00.123457"
        ]
    );
    assert_eq!(
        engine
            .execute_with_session(
                &mut session,
                "INSERT INTO fraction_ts(t6) VALUES(99991231235959.9999995)"
            )
            .unwrap_err()
            .code(),
        1264
    );
    engine
        .execute_with_session(&mut session, "CREATE TABLE cast_edge(ts TIMESTAMP(6))")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO cast_edge VALUES('9999-12-31 23:59:59.999999')",
        )
        .unwrap();
    let actual = rows(
        &engine,
        &mut session,
        "SELECT CAST(ts AS DATETIME),CAST(CAST(ts AS DATETIME(6)) AS DATETIME) FROM cast_edge",
    );
    assert_eq!(actual[0], vec![Value::Null, Value::Null]);
    assert!(!rows(&engine, &mut session, "SHOW WARNINGS").is_empty());
    engine
        .execute_with_session(&mut session, "SET sql_mode='TIME_TRUNCATE_FRACTIONAL'")
        .unwrap();
    let actual = rows(
        &engine,
        &mut session,
        "SELECT CAST(CAST(ts AS DATETIME(6)) AS DATETIME) FROM cast_edge",
    );
    assert_eq!(
        crate::storage::value_to_string(&actual[0][0]),
        "9999-12-31 23:59:59"
    );
    engine
        .execute_with_session(
            &mut session,
            "SET time_zone='+00:00',sql_mode='STRICT_TRANS_TABLES'",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "DELETE FROM cast_edge")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO cast_edge VALUES('1970-01-01 00:00:01.500000')",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET time_zone='-12:00'")
        .unwrap();
    let actual = rows(
        &engine,
        &mut session,
        "SELECT CAST(ts AS DATETIME),CAST(CAST(ts AS DATETIME(6)) AS DATETIME) FROM cast_edge",
    );
    for value in &actual[0] {
        assert_eq!(
            crate::storage::value_to_string(value),
            "1969-12-31 12:00:02"
        );
    }
}

#[test]
fn timestamp_9999_timezone_identity_and_prepared_indexes() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = utc_session(&engine);
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE future_ts(id INT PRIMARY KEY, ts TIMESTAMP(6), KEY idx_ts(ts))",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET time_zone='+14:00'")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO future_ts VALUES(1,'9999-12-31 23:59:59.999999')",
        )
        .unwrap();
    let sql = "SELECT COUNT(*) FROM future_ts WHERE ts=?";
    let prepared = engine.prepare_cached(&mut session, sql, 1).unwrap();
    let mut state = crate::engine::api::PreparedSelectMemoState::default();
    for (zone, wall) in [
        ("+14:00", "9999-12-31 23:59:59.999999"),
        ("+00:00", "9999-12-31 09:59:59.999999"),
        ("-12:00", "9999-12-30 21:59:59.999999"),
    ] {
        engine
            .execute_with_session(&mut session, &format!("SET time_zone='{zone}'"))
            .unwrap();
        assert_eq!(
            crate::storage::value_to_string(
                &rows(&engine, &mut session, "SELECT ts FROM future_ts")[0][0]
            ),
            wall
        );
        let result = crate::engine::api::QueryEngine::execute_prepared_select(
            &engine,
            &mut session,
            &mut state,
            &prepared,
            sql,
            &[Value::String(wall.to_string())],
        )
        .unwrap()
        .unwrap()
        .materialize()
        .unwrap();
        let QueryResponse::ResultSet(result) = result else {
            panic!("prepared result");
        };
        assert_eq!(result.rows[0][0], Value::Int(1));
    }
    engine
        .execute_with_session(&mut session, "SET time_zone='America/New_York'")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO future_ts VALUES(2,'9999-01-01 12:00:00.123456'),\
         (3,'9999-07-01 12:00:00.123456')",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET time_zone='+00:00'")
        .unwrap();
    let actual = rows(
        &engine,
        &mut session,
        "SELECT ts FROM future_ts WHERE id>1 ORDER BY id",
    );
    assert_eq!(
        crate::storage::value_to_string(&actual[0][0]),
        "9999-01-01 17:00:00.123456"
    );
    assert_eq!(
        crate::storage::value_to_string(&actual[1][0]),
        "9999-07-01 16:00:00.123456"
    );
}

#[test]
fn timestamp_9999_defaults_alter_and_current_timestamp() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = utc_session(&engine);
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE future_default(id INT PRIMARY KEY, ts TIMESTAMP(6)\
         DEFAULT '9999-12-31 23:59:59.999999')",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO future_default(id) VALUES(1)")
        .unwrap();
    assert_eq!(
        crate::storage::value_to_string(
            &rows(&engine, &mut session, "SELECT ts FROM future_default")[0][0]
        ),
        "9999-12-31 23:59:59.999999"
    );
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE future_alter(id INT PRIMARY KEY, dt DATETIME(6), KEY(dt))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO future_alter VALUES(1,'9999-12-31 23:59:59.999999')",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "ALTER TABLE future_alter MODIFY dt TIMESTAMP(6)",
        )
        .unwrap();
    assert_eq!(
        crate::storage::value_to_string(
            &rows(&engine, &mut session, "SELECT dt FROM future_alter")[0][0]
        ),
        "9999-12-31 23:59:59.999999"
    );
    engine
        .execute_with_session(&mut session, "SET timestamp=253402214400")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE future_now(id INT PRIMARY KEY, ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP \
         ON UPDATE CURRENT_TIMESTAMP)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO future_now(id) VALUES(1)")
        .unwrap();
    assert_eq!(
        crate::storage::value_to_string(
            &rows(&engine, &mut session, "SELECT ts FROM future_now")[0][0]
        ),
        "9999-12-31 00:00:00"
    );
    engine
        .execute_with_session(&mut session, "SET timestamp=253402300799")
        .unwrap();
    engine
        .execute_with_session(&mut session, "UPDATE future_now SET id=2 WHERE id=1")
        .unwrap();
    assert_eq!(
        crate::storage::value_to_string(
            &rows(&engine, &mut session, "SELECT ts FROM future_now")[0][0]
        ),
        "9999-12-31 23:59:59"
    );
}

#[test]
fn timestamp_9999_rounding_and_overflow_are_atomic() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = utc_session(&engine);
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE future_bounds(id INT PRIMARY KEY, ts TIMESTAMP(6))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO future_bounds VALUES(1,'9999-12-31 23:59:59.999999')",
        )
        .unwrap();
    for sql in [
        "INSERT INTO future_bounds VALUES(2,'2040-01-01'),(3,'9999-12-31 23:59:59.9999995')",
        "UPDATE future_bounds SET ts='9999-12-31 23:59:59.9999995' WHERE id=1",
        "INSERT INTO future_bounds VALUES(4,'1970-01-01 00:00:00')",
    ] {
        assert_eq!(
            engine
                .execute_with_session(&mut session, sql)
                .unwrap_err()
                .code(),
            1264,
            "{sql}"
        );
    }
    assert_eq!(
        rows(&engine, &mut session, "SELECT COUNT(*) FROM future_bounds")[0][0],
        Value::Int(1)
    );
    engine
        .execute_with_session(&mut session, "SET time_zone='-12:00'")
        .unwrap();
    assert_eq!(
        engine
            .execute_with_session(
                &mut session,
                "INSERT INTO future_bounds VALUES(5,'9999-12-31 23:59:59.999999')"
            )
            .unwrap_err()
            .code(),
        1264
    );
    engine
        .execute_with_session(&mut session, "SET time_zone='+14:00'")
        .unwrap();
    assert_eq!(
        engine
            .execute_with_session(
                &mut session,
                "INSERT INTO future_bounds VALUES(6,'9999-12-31 23:59:59.9999995')"
            )
            .unwrap_err()
            .code(),
        1264
    );
    for mode in [vec::config::VecMode::Off, vec::config::VecMode::On] {
        let _scope = vec::config::enter_mode(mode);
        assert_eq!(
            engine
                .execute_with_session(&mut session, "SELECT ts FROM future_bounds")
                .unwrap_err()
                .code(),
            1292
        );
    }
    engine
        .execute_with_session(
            &mut session,
            "SET time_zone='+00:00',sql_mode='NO_ENGINE_SUBSTITUTION'",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO future_bounds VALUES(7,'9999-12-31 23:59:59.9999995')",
        )
        .unwrap();
    let warnings = rows(&engine, &mut session, "SHOW WARNINGS");
    assert_eq!(warnings.len(), 1);
    assert_eq!(crate::storage::value_to_string(&warnings[0][1]), "1264");
    assert_eq!(
        crate::storage::value_to_string(
            &rows(
                &engine,
                &mut session,
                "SELECT ts FROM future_bounds WHERE id=7"
            )[0][0]
        ),
        "0000-00-00 00:00:00"
    );
    engine
        .execute_with_session(
            &mut session,
            "SET sql_mode='STRICT_TRANS_TABLES,TIME_TRUNCATE_FRACTIONAL'",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO future_bounds VALUES(8,'9999-12-31 23:59:59.9999999')",
        )
        .unwrap();
    assert_eq!(
        crate::storage::value_to_string(
            &rows(
                &engine,
                &mut session,
                "SELECT ts FROM future_bounds WHERE id=8"
            )[0][0]
        ),
        "9999-12-31 23:59:59.999999"
    );
}

#[test]
fn timestamp_9999_unix_functions_and_zone_conversion() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = utc_session(&engine);
    let actual = rows(
        &engine,
        &mut session,
        "SELECT UNIX_TIMESTAMP('9999-12-31 23:59:59.999999'),\
         FROM_UNIXTIME(253402300799.9999994),FROM_UNIXTIME('253402300799.999999'),\
         FROM_UNIXTIME(253402300799.9999995),FROM_UNIXTIME(253402300800),\
         FROM_UNIXTIME(253402300799,'%Y-%m-%d %H:%i:%s'),\
         CONVERT_TZ('9999-12-31 23:59:59.999999','+00:00','-12:00'),\
         CONVERT_TZ('9999-12-31 23:59:59.999999','+00:00','+14:00')",
    );
    let text: Vec<_> = actual[0]
        .iter()
        .map(crate::storage::value_to_string)
        .collect();
    assert_eq!(
        text,
        [
            "253402300799.999999",
            "9999-12-31 23:59:59.999999",
            "9999-12-31 23:59:59.999999",
            "NULL",
            "NULL",
            "9999-12-31 23:59:59",
            "9999-12-31 11:59:59.999999",
            "NULL"
        ]
    );
    engine
        .execute_with_session(&mut session, "SET time_zone='+14:00'")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT FROM_UNIXTIME(253402300799)")[0][0],
        Value::Null
    );
    engine
        .execute_with_session(
            &mut session,
            "SET time_zone='+00:00',sql_mode='TIME_TRUNCATE_FRACTIONAL'",
        )
        .unwrap();
    assert_eq!(
        crate::storage::value_to_string(
            &rows(
                &engine,
                &mut session,
                "SELECT FROM_UNIXTIME(253402300799.9999999)"
            )[0][0]
        ),
        "9999-12-31 23:59:59.999999"
    );
}

#[test]
fn timestamp_9999_parallel_load_and_zone_maps() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_relaxed_test_engine(dir.path());
    let mut session = utc_session(&engine);
    engine
        .execute_with_session(&mut session, "SET time_zone='+14:00'")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE future_load(id INT PRIMARY KEY,ts TIMESTAMP(6),KEY idx_ts(ts)) \
         ZONE_MAP=ON ZONE_SIZE=64K",
        )
        .unwrap();
    let input = dir.path().join("future.tsv");
    let data: String = (0..4096)
        .map(|id| {
            let date = if id % 2 == 0 {
                "9999-12-31 23:59:59.999999"
            } else {
                "4000-01-01 00:00:00.123456"
            };
            format!("{id}\t{date}\n")
        })
        .collect();
    std::fs::write(&input, data).unwrap();
    engine
        .execute_with_session(
            &mut session,
            &format!(
                "LOAD DATA /*+ PARALLEL(4) */ INFILE '{}' INTO TABLE future_load",
                crate::util::file::sql_path(&input)
            ),
        )
        .unwrap();
    for hint in ["ZONE_MAP(future_load)", "NO_ZONE_MAP(future_load)"] {
        assert_eq!(
            rows(
                &engine,
                &mut session,
                &format!(
                    "SELECT /*+ {hint} */ COUNT(*) FROM future_load FORCE INDEX(PRIMARY) \
             WHERE ts>'9000-01-01'"
                )
            )[0][0],
            Value::Int(2048)
        );
    }
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM future_load FORCE INDEX(idx_ts) \
         WHERE ts='9999-12-31 23:59:59.999999'"
        )[0][0],
        Value::Int(2048)
    );
    let bad = dir.path().join("overflow.tsv");
    std::fs::write(&bad, "5000\t9999-12-31 23:59:59.9999995\n").unwrap();
    assert_eq!(
        engine
            .execute_with_session(
                &mut session,
                &format!(
                    "LOAD DATA INFILE '{}' INTO TABLE future_load",
                    crate::util::file::sql_path(&bad)
                )
            )
            .unwrap_err()
            .code(),
        1264
    );
    assert_eq!(
        rows(&engine, &mut session, "SELECT COUNT(*) FROM future_load")[0][0],
        Value::Int(4096)
    );
}

#[test]
fn timestamp_9999_clock_and_json_overflow_produce_diagnostics() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = utc_session(&engine);
    engine
        .execute_with_session(&mut session, "CREATE TABLE edge_clock(ts TIMESTAMP(6))")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO edge_clock VALUES('9999-12-31 23:59:59.999999')",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "SET time_zone='+14:00',timestamp=253402300799",
        )
        .unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT NOW(6),CURDATE()")[0],
        vec![Value::Null, Value::Null]
    );
    assert!(!rows(&engine, &mut session, "SHOW WARNINGS").is_empty());
    let actual = rows(
        &engine,
        &mut session,
        "SELECT JSON_ARRAY(ts) FROM edge_clock",
    );
    assert_eq!(crate::storage::value_to_string(&actual[0][0]), "[null]");
    let warnings = rows(&engine, &mut session, "SHOW WARNINGS");
    assert!(!warnings.is_empty());
    assert_eq!(crate::storage::value_to_string(&warnings[0][1]), "1292");
    engine
        .execute_with_session(&mut session, "CREATE TABLE edge_text(txt VARCHAR(32))")
        .unwrap();
    assert_eq!(
        engine
            .execute_with_session(
                &mut session,
                "INSERT INTO edge_text SELECT ts FROM edge_clock"
            )
            .unwrap_err()
            .code(),
        1292
    );
}
