use super::support::execute_materialized as exec;
use super::*;

#[test]
fn create_event_persists_recurring_definition() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE TABLE log (id INT PRIMARY KEY AUTO_INCREMENT, msg VARCHAR(32))",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE EVENT e_ping ON SCHEDULE EVERY 5 MINUTE \
         STARTS '2099-01-01 00:00:00' \
         DISABLE \
         DO INSERT INTO log (msg) VALUES ('tick')",
    )
    .unwrap();

    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "SHOW EVENTS").unwrap() else {
        panic!("expected SHOW EVENTS result set");
    };
    assert_eq!(rs.rows.len(), 1);
    let name_col = rs
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case("Name"))
        .unwrap();
    let status_col = rs
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case("Status"))
        .unwrap();
    let type_col = rs
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case("Type"))
        .unwrap();
    let originator_col = rs
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case("Originator"))
        .unwrap();
    let charset_col = rs
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case("character_set_client"))
        .unwrap();
    let collation_col = rs
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case("collation_connection"))
        .unwrap();
    let db_collation_col = rs
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case("Database Collation"))
        .unwrap();
    assert_eq!(rs.rows[0][name_col], Value::String("e_ping".to_string()));
    assert_eq!(
        rs.rows[0][status_col],
        Value::String("DISABLED".to_string())
    );
    assert_eq!(rs.rows[0][type_col], Value::String("RECURRING".to_string()));
    assert_eq!(
        rs.rows[0][originator_col],
        Value::Int(crate::engine::executor::events::current_server_id() as i64)
    );
    assert_eq!(
        rs.rows[0][charset_col],
        Value::String("utf8mb4".to_string())
    );
    assert_eq!(
        rs.rows[0][collation_col],
        Value::String("utf8mb4_0900_ai_ci".to_string())
    );
    assert_eq!(
        rs.rows[0][db_collation_col],
        Value::String("utf8mb4_0900_ai_ci".to_string())
    );
}

#[test]
fn show_create_event_round_trips_through_parser() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(&engine, &mut session, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE EVENT e1 ON SCHEDULE EVERY 1 HOUR ON COMPLETION PRESERVE DISABLE \
         COMMENT 'hello' DO INSERT INTO t VALUES (1)",
    )
    .unwrap();

    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "SHOW CREATE EVENT e1").unwrap()
    else {
        panic!("expected SHOW CREATE EVENT result set");
    };
    assert_eq!(rs.rows.len(), 1);
    let create_col = rs
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case("Create Event"))
        .unwrap();
    let create_sql = match &rs.rows[0][create_col] {
        Value::String(s) => s.clone(),
        other => panic!("unexpected value {other:?}"),
    };
    let upper = create_sql.to_ascii_uppercase();
    assert!(upper.contains("EVERY 1 HOUR"));
    assert!(upper.contains("ON COMPLETION PRESERVE"));
    assert!(upper.contains("DISABLE"));
    assert!(create_sql.contains("COMMENT 'hello'"));
    assert!(upper.contains("DO INSERT INTO T VALUES (1)"));
}

#[test]
fn alter_event_can_disable_and_rename() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(&engine, &mut session, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE EVENT e_orig ON SCHEDULE EVERY 30 SECOND DISABLE DO INSERT INTO t VALUES (1)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "ALTER EVENT e_orig RENAME TO e_renamed COMMENT 'updated'",
    )
    .unwrap();

    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SELECT EVENT_NAME, EVENT_COMMENT FROM information_schema.events",
    )
    .unwrap() else {
        panic!("expected information_schema.events result set");
    };
    assert_eq!(rs.rows.len(), 1);
    assert_eq!(rs.rows[0][0], Value::String("e_renamed".to_string()));
    assert_eq!(rs.rows[0][1], Value::String("updated".to_string()));
}

#[test]
fn drop_event_removes_catalog_entry() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(&engine, &mut session, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE EVENT e_drop ON SCHEDULE EVERY 1 MINUTE DISABLE DO INSERT INTO t VALUES (1)",
    )
    .unwrap();
    exec(&engine, &mut session, "DROP EVENT e_drop").unwrap();
    let err = exec(&engine, &mut session, "SHOW CREATE EVENT e_drop").unwrap_err();
    assert_eq!(err.code(), 1539);

    // DROP EVENT IF EXISTS is a no-op when missing.
    exec(&engine, &mut session, "DROP EVENT IF EXISTS e_drop").unwrap();
}

#[test]
fn duplicate_event_without_if_not_exists_errors() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(&engine, &mut session, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE EVENT dup ON SCHEDULE EVERY 1 MINUTE DISABLE DO INSERT INTO t VALUES (1)",
    )
    .unwrap();
    let err = exec(
        &engine,
        &mut session,
        "CREATE EVENT dup ON SCHEDULE EVERY 1 MINUTE DISABLE DO INSERT INTO t VALUES (1)",
    )
    .unwrap_err();
    assert_eq!(err.code(), 1537);

    // IF NOT EXISTS should turn the duplicate into a warning rather than
    // a hard error.
    exec(
        &engine,
        &mut session,
        "CREATE EVENT IF NOT EXISTS dup ON SCHEDULE EVERY 1 MINUTE DISABLE DO INSERT INTO t VALUES (1)",
    )
    .unwrap();
}

#[test]
fn at_in_the_past_not_preserve_never_runs_and_is_dropped() {
    // G3: AT in the past with the default ON COMPLETION NOT PRESERVE is
    // not an error — the body never runs and the event is not
    // stored.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, n INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE EVENT past ON SCHEDULE AT '1970-01-01 00:00:00' DO INSERT INTO t (n) VALUES (1)",
    )
    .unwrap();

    // No business DML is executed by this definition.
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SELECT COUNT(*) FROM t").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(0));

    // Event was not persisted.
    let err = exec(&engine, &mut session, "SHOW CREATE EVENT past").unwrap_err();
    assert_eq!(err.code(), 1539);
}

#[test]
fn at_in_the_past_preserve_stored_disabled() {
    // G3: AT in the past with ON COMPLETION PRESERVE stores the event as
    // DISABLED without executing it.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, n INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE EVENT past2 ON SCHEDULE AT '1970-01-01 00:00:00' ON COMPLETION PRESERVE \
         DO INSERT INTO t (n) VALUES (1)",
    )
    .unwrap();

    // Body NOT executed.
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SELECT COUNT(*) FROM t").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(0));

    // Stored as DISABLED.
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SELECT STATUS FROM information_schema.events WHERE EVENT_NAME = 'past2'",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
    assert_eq!(rs.rows[0][0], Value::String("DISABLED".to_string()));
}

#[test]
fn create_event_captures_session_sql_mode() {
    // G1: the session sql_mode at CREATE time is captured and surfaced
    // on information_schema.events / SHOW CREATE EVENT.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    exec(
        &engine,
        &mut session,
        "SET SESSION sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_DATE'",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE EVENT e_mode ON SCHEDULE EVERY 1 DAY DISABLE DO INSERT INTO t VALUES (1)",
    )
    .unwrap();
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SELECT SQL_MODE FROM information_schema.events WHERE EVENT_NAME = 'e_mode'",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
    let Value::String(mode) = &rs.rows[0][0] else {
        panic!("expected string sql_mode, got {:?}", rs.rows[0][0]);
    };
    assert!(mode.contains("STRICT_ALL_TABLES"), "got {mode}");
    assert!(mode.contains("NO_ZERO_DATE"), "got {mode}");
}

#[test]
fn create_event_rejects_malformed_body() {
    // G8: a body that does not parse is rejected at CREATE time (1064),
    // not deferred to firing.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let err = exec(
        &engine,
        &mut session,
        "CREATE EVENT bad ON SCHEDULE EVERY 1 DAY DISABLE DO INSERTX INTO nope",
    )
    .unwrap_err();
    assert_eq!(err.code(), 1064);
}

#[test]
fn alter_event_reschedule_resets_baseline() {
    // G12: changing ON SCHEDULE clears last_executed so next_due is
    // computed fresh from the new schedule (no immediate back-fill).
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE EVENT e_reschedule ON SCHEDULE EVERY 1 SECOND \
         STARTS '2000-01-01 00:00:00' DISABLE DO INSERT INTO t VALUES (1)",
    )
    .unwrap();
    // Pretend it ran far in the past by altering to a future schedule.
    exec(
        &engine,
        &mut session,
        "ALTER EVENT e_reschedule ON SCHEDULE EVERY 1 DAY STARTS '2099-01-01 00:00:00'",
    )
    .unwrap();
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SELECT LAST_EXECUTED, STARTS FROM information_schema.events WHERE EVENT_NAME = 'e_reschedule'",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
    // last_executed reset to NULL after reschedule.
    assert!(
        matches!(rs.rows[0][0], Value::Null),
        "expected LAST_EXECUTED NULL after reschedule, got {:?}",
        rs.rows[0][0]
    );
}

#[test]
fn scheduler_state_variable_is_visible() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SHOW VARIABLES LIKE 'event_scheduler'",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
    assert_eq!(rs.rows[0][0], Value::String("event_scheduler".to_string()));
}

#[test]
fn scheduler_fires_recurring_event() {
    use std::time::{Duration, Instant};
    let dir = tempfile::tempdir().unwrap();
    let engine = open_background_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE TABLE ticks (id INT PRIMARY KEY AUTO_INCREMENT, ts INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE EVENT ticker ON SCHEDULE EVERY 1 SECOND DO INSERT INTO ticks (ts) VALUES (1)",
    )
    .unwrap();

    // Wait up to 4 seconds for the scheduler to fire at least once.
    let deadline = Instant::now() + Duration::from_secs(4);
    let mut fired = 0u64;
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(250));
        let QueryResponse::ResultSet(rs) =
            exec(&engine, &mut session, "SELECT COUNT(*) FROM ticks").unwrap()
        else {
            panic!("expected result set");
        };
        if let Value::Int(n) = rs.rows[0][0] {
            fired = n as u64;
            if fired >= 1 {
                break;
            }
        }
    }
    assert!(
        fired >= 1,
        "expected at least one scheduler tick, got {fired}"
    );

    // Verify last_executed shows up on information_schema.
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SELECT LAST_EXECUTED FROM information_schema.events WHERE EVENT_NAME = 'ticker'",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
    assert!(
        !matches!(rs.rows[0][0], Value::Null),
        "expected LAST_EXECUTED to be populated, got {:?}",
        rs.rows[0][0]
    );
}

#[test]
fn scheduler_runs_independent_due_events_concurrently() {
    use std::time::{Duration, Instant};

    with_test_env(&[("MYDB_EVENT_WORKERS", Some("2"))], || {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_background_test_engine(dir.path());
        let mut session = SessionContext::default();
        exec(
            &engine,
            &mut session,
            "CREATE TABLE event_done (name VARCHAR(32) PRIMARY KEY)",
        )
        .unwrap();
        // Formatting drops subsecond precision. Leave enough time to publish
        // both DDL definitions before either event becomes due under load.
        let due = chrono::Local::now() + chrono::Duration::seconds(3);
        let due = due.format("%Y-%m-%d %H:%M:%S").to_string();
        for name in ["parallel_a", "parallel_b"] {
            exec(
                &engine,
                &mut session,
                &format!(
                    "CREATE EVENT {name} ON SCHEDULE AT '{due}' ON COMPLETION PRESERVE \
                     DO INSERT INTO event_done SELECT '{name}' WHERE SLEEP(0.25) = 0"
                ),
            )
            .unwrap();
        }

        let deadline = Instant::now() + Duration::from_secs(5);
        let mut simultaneous = 0usize;
        while Instant::now() < deadline {
            simultaneous = engine
                .registry()
                .snapshot()
                .into_iter()
                .filter(|process| {
                    process.user == "event_scheduler"
                        && process
                            .info
                            .as_deref()
                            .is_some_and(|sql| sql.contains("SLEEP(0.25)"))
                })
                .count();
            if simultaneous >= 2 {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(
            simultaneous, 2,
            "two events with the same due time must occupy two bounded workers"
        );
    });
}

// ---- Unit tests for scheduling helpers (G4/G5/G6) ----

use crate::engine::executor::events::next_due_ms;
use crate::storage::{
    EventIntervalSchema, EventOnCompletionSchema, EventScheduleSchema, EventSchema,
    EventStatusSchema,
};

fn ms(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> u64 {
    use chrono::{NaiveDate, TimeZone, Utc};
    let dt = NaiveDate::from_ymd_opt(y, mo, d)
        .unwrap()
        .and_hms_opt(h, mi, s)
        .unwrap();
    Utc.from_utc_datetime(&dt).timestamp_millis() as u64
}

fn recurring_event(value: &str, unit: &str, starts_ms: u64, last_executed_ms: u64) -> EventSchema {
    EventSchema {
        database: "test".into(),
        name: "e".into(),
        definer_user: "root".into(),
        definer_host: "localhost".into(),
        schedule: EventScheduleSchema::Recurring {
            interval: EventIntervalSchema {
                value: value.into(),
                unit: unit.into(),
            },
            starts_ms,
            ends_ms: None,
        },
        on_completion: EventOnCompletionSchema::NotPreserve,
        status: EventStatusSchema::Enabled,
        comment: String::new(),
        body_sql: "DO 1".into(),
        create_sql: String::new(),
        created_ms: 0,
        last_altered_ms: 0,
        last_executed_ms,
        schema_version: 1,
        sql_mode: String::new(),
        time_zone: "UTC".into(),
        character_set_client: "utf8mb4".into(),
        collation_connection: "utf8mb4_0900_ai_ci".into(),
        database_collation: "utf8mb4_0900_ai_ci".into(),
    }
}

#[test]
fn next_due_skips_missed_periods() {
    // G5: STARTS 10s ago, EVERY 1 SECOND, never executed. On a single
    // wake the next due must be the most recent aligned tick <= now, not
    // the first tick 10s ago — so the scheduler fires once, not 10 times.
    let starts = ms(2000, 1, 1, 0, 0, 0);
    let now = starts + 10_000; // 10 seconds later
    let event = recurring_event("1", "SECOND", starts, 0);
    let due = next_due_ms(&event, now).unwrap();
    // Most recent aligned tick at or before now.
    assert_eq!(due, now, "expected catch-up to align to now, got {due}");
}

#[test]
fn next_due_month_steps_calendar_end_of_month() {
    // G4: STARTS Jan 31, EVERY 1 MONTH. After running on Jan 31, the next
    // due is Feb 28 (calendar clamp), then Mar 31 — not +30 days.
    let starts = ms(2026, 1, 31, 12, 0, 0);
    // last_executed exactly at the Jan 31 tick.
    let event = recurring_event("1", "MONTH", starts, starts);
    let due = next_due_ms(&event, starts + 1).unwrap();
    assert_eq!(due, ms(2026, 2, 28, 12, 0, 0));

    // From the Feb 28 tick, the next is Mar 31 (MySQL re-anchors on the
    // original day-of-month each step from STARTS).
    let event2 = recurring_event("1", "MONTH", starts, ms(2026, 2, 28, 12, 0, 0));
    let due2 = next_due_ms(&event2, ms(2026, 2, 28, 12, 0, 0) + 1).unwrap();
    assert_eq!(due2, ms(2026, 3, 31, 12, 0, 0));
}

#[test]
fn next_due_compound_year_month() {
    // G6: EVERY '1-6' YEAR_MONTH = 18 months.
    let starts = ms(2026, 1, 15, 0, 0, 0);
    let event = recurring_event("'1-6'", "YEAR_MONTH", starts, starts);
    let due = next_due_ms(&event, starts + 1).unwrap();
    assert_eq!(due, ms(2027, 7, 15, 0, 0, 0));
}

#[test]
fn next_due_compound_day_second() {
    // G6: EVERY '1 12:30:45' DAY_SECOND.
    let starts = ms(2026, 1, 1, 0, 0, 0);
    let event = recurring_event("'1 12:30:45'", "DAY_SECOND", starts, starts);
    let due = next_due_ms(&event, starts + 1).unwrap();
    assert_eq!(due, ms(2026, 1, 2, 12, 30, 45));
}

#[test]
fn interval_microsecond_floors_to_one_ms() {
    // G6: MICROSECOND is accepted; sub-ms rounds up to the 1ms floor.
    let starts = ms(2026, 1, 1, 0, 0, 0);
    let event = recurring_event("500", "MICROSECOND", starts, starts);
    let due = next_due_ms(&event, starts + 1).unwrap();
    assert_eq!(due, starts + 1, "500us should floor to +1ms");
}

// ---- M4/M5: cascade, startup config, privilege filtering ----

#[test]
fn drop_database_cascades_events() {
    // E1/G13: dropping a schema removes its events too, so the scheduler
    // never fires an orphan against a database that no longer exists.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let home = session.current_database.clone();

    exec(&engine, &mut session, "CREATE DATABASE evtdb").unwrap();
    session.current_database = "evtdb".to_string();
    exec(&engine, &mut session, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE EVENT e_x ON SCHEDULE EVERY 1 HOUR DISABLE DO INSERT INTO t (id) VALUES (1)",
    )
    .unwrap();
    assert!(
        engine
            .db
            .all_events()
            .iter()
            .any(|e| e.database == "evtdb" && e.name == "e_x"),
        "event should exist before DROP DATABASE"
    );

    session.current_database = home;
    exec(&engine, &mut session, "DROP DATABASE evtdb").unwrap();
    assert!(
        engine.db.all_events().iter().all(|e| e.database != "evtdb"),
        "events must be cascaded away on DROP DATABASE, got {:?}",
        engine.db.all_events()
    );
}

#[test]
fn drop_database_cascades_views_routines_triggers() {
    // issue #82: DROP DATABASE must also cascade views, stored
    // procedures/functions and triggers — otherwise re-creating same-named
    // objects in a rebuilt schema fails (1050/1304) and information_schema
    // leaks ghost rows.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let home = session.current_database.clone();

    exec(&engine, &mut session, "CREATE DATABASE dv").unwrap();
    session.current_database = "dv".to_string();
    exec(&engine, &mut session, "CREATE TABLE t(a INT)").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE VIEW myview AS SELECT a FROM t",
    )
    .unwrap();
    exec(&engine, &mut session, "CREATE PROCEDURE p() SELECT 1").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE FUNCTION f() RETURNS INT DETERMINISTIC RETURN 1",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW SET @x = 1",
    )
    .unwrap();

    session.current_database = home;
    exec(&engine, &mut session, "DROP DATABASE dv").unwrap();

    // information_schema must not leak ghost objects.
    let mut ghost = |sql: &str| -> usize {
        let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, sql).unwrap() else {
            panic!("expected result set");
        };
        rs.rows.len()
    };
    assert_eq!(
        ghost("SELECT table_name FROM information_schema.views WHERE table_schema='dv'"),
        0,
        "view leaked in IS.views"
    );
    assert_eq!(
        ghost("SELECT routine_name FROM information_schema.routines WHERE routine_schema='dv'"),
        0,
        "routine leaked in IS.routines"
    );
    assert_eq!(
        ghost("SELECT trigger_name FROM information_schema.triggers WHERE trigger_schema='dv'"),
        0,
        "trigger leaked in IS.triggers"
    );

    // Re-create same-named objects in a rebuilt schema — must succeed.
    exec(&engine, &mut session, "CREATE DATABASE dv").unwrap();
    session.current_database = "dv".to_string();
    exec(&engine, &mut session, "CREATE TABLE t(a INT)").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE VIEW myview AS SELECT a FROM t",
    )
    .unwrap();
    exec(&engine, &mut session, "CREATE PROCEDURE p() SELECT 1").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE FUNCTION f() RETURNS INT DETERMINISTIC RETURN 1",
    )
    .unwrap();
}

#[test]
fn create_event_honours_qualified_schema_name() {
    // `CREATE EVENT other_db.evt` must land in other_db, not the current
    // schema. Regression for create_event re-splitting a bare name.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(&engine, &mut session, "CREATE DATABASE other_db").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE other_db.t (id INT PRIMARY KEY)",
    )
    .unwrap();
    // current_database stays as the default (test), event is qualified.
    exec(
        &engine,
        &mut session,
        "CREATE EVENT other_db.evt ON SCHEDULE EVERY 1 HOUR DISABLE \
         DO INSERT INTO other_db.t (id) VALUES (1)",
    )
    .unwrap();

    let placed = engine.db.all_events();
    assert!(
        placed
            .iter()
            .any(|e| e.database == "other_db" && e.name == "evt"),
        "qualified event must live in other_db, got {placed:?}"
    );
    assert!(
        !placed
            .iter()
            .any(|e| e.database == "test" && e.name == "evt"),
        "qualified event must not leak into the current schema"
    );
}

#[test]
fn startup_env_configures_scheduler_state() {
    // E3/G10: MYDB_EVENT_SCHEDULER sets the start-time state, and unlike
    // the runtime sysvar it accepts DISABLED.
    use crate::engine::executor::events::{
        apply_startup_state_from_env, scheduler_state, set_scheduler_state, EventSchedulerState,
    };
    let saved = scheduler_state();

    std::env::set_var("MYDB_EVENT_SCHEDULER", "off");
    apply_startup_state_from_env();
    assert_eq!(scheduler_state(), EventSchedulerState::Off);

    std::env::set_var("MYDB_EVENT_SCHEDULER", "disabled");
    apply_startup_state_from_env();
    assert_eq!(scheduler_state(), EventSchedulerState::Disabled);

    std::env::set_var("MYDB_EVENT_SCHEDULER", "on");
    apply_startup_state_from_env();
    assert_eq!(scheduler_state(), EventSchedulerState::On);

    std::env::remove_var("MYDB_EVENT_SCHEDULER");
    // Restore so parallel/other tests see the default ON state.
    set_scheduler_state(saved);
}

#[test]
fn show_events_respects_event_privilege() {
    // E4/G11: SHOW EVENTS lists nothing without the EVENT privilege, and
    // SHOW CREATE EVENT hides the event's existence (1539). A grant makes
    // both visible.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();

    exec(
        &engine,
        &mut root,
        "CREATE TABLE log (id INT PRIMARY KEY AUTO_INCREMENT, msg VARCHAR(16))",
    )
    .unwrap();
    exec(
        &engine,
        &mut root,
        "CREATE EVENT e_secret ON SCHEDULE EVERY 1 HOUR DISABLE DO INSERT INTO log (msg) VALUES ('x')",
    )
    .unwrap();
    exec(
        &engine,
        &mut root,
        "CREATE USER 'bob'@'%' IDENTIFIED BY 'pw'",
    )
    .unwrap();

    let mut bob = SessionContext::default();
    bob.user = "bob".to_string();
    bob.login_user = "bob".to_string();
    bob.authenticated_as = crate::engine::auth::AccountKey::new("bob", "%");
    bob.auth_epoch = engine.db.auth_catalog().epoch;

    let QueryResponse::ResultSet(rs) = exec(&engine, &mut bob, "SHOW EVENTS").unwrap() else {
        panic!("expected SHOW EVENTS result set");
    };
    assert_eq!(rs.rows.len(), 0, "unprivileged user must not see events");
    let err = exec(&engine, &mut bob, "SHOW CREATE EVENT e_secret").unwrap_err();
    assert_eq!(err.code(), 1539, "SHOW CREATE EVENT must hide existence");

    exec(&engine, &mut root, "GRANT EVENT ON test.* TO 'bob'@'%'").unwrap();
    bob.auth_epoch = engine.db.auth_catalog().epoch;

    let QueryResponse::ResultSet(rs) = exec(&engine, &mut bob, "SHOW EVENTS").unwrap() else {
        panic!("expected SHOW EVENTS result set");
    };
    assert_eq!(rs.rows.len(), 1, "granted user must see the event");
    exec(&engine, &mut bob, "SHOW CREATE EVENT e_secret").unwrap();
}

#[test]
fn event_audit_compound_body_executes_control_flow() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_background_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE event_audit_log (v INT)",
    )
    .unwrap();
    exec(&engine, &mut session, "CREATE EVENT compound ON SCHEDULE AT CURRENT_TIMESTAMP + INTERVAL 1 SECOND DO BEGIN DECLARE n INT DEFAULT 0; WHILE n < 3 DO INSERT INTO event_audit_log VALUES (n); SET n = n + 1; END WHILE; END").unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while engine
        .database()
        .event_definition("test", "compound")
        .is_some()
    {
        assert!(
            std::time::Instant::now() < deadline,
            "scheduler did not finish compound body"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SELECT v FROM event_audit_log ORDER BY v",
    )
    .unwrap() else {
        panic!()
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(0)],
            vec![Value::Int(1)],
            vec![Value::Int(2)]
        ]
    );
}

#[test]
fn event_audit_rejects_numeric_schedule() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for schedule in [
        "AT UNIX_TIMESTAMP() + 3",
        "EVERY 1 DAY STARTS 1800000000",
        "EVERY 1 DAY ENDS 2000000000",
    ] {
        assert!(
            exec(
                &engine,
                &mut session,
                &format!("CREATE EVENT numeric_time ON SCHEDULE {schedule} DISABLE DO SELECT 1")
            )
            .is_err(),
            "{schedule}"
        );
    }
}

#[test]
fn event_audit_rejects_negative_intervals() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for interval in [
        "-1 DAY",
        "'-1-6' YEAR_MONTH",
        "'-1 12:30:45' DAY_SECOND",
        "'1:-2' HOUR_MINUTE",
    ] {
        assert!(
            exec(
                &engine,
                &mut session,
                &format!("CREATE EVENT negative ON SCHEDULE EVERY {interval} DISABLE DO SELECT 1")
            )
            .is_err(),
            "{interval}"
        );
    }
}

#[test]
fn event_audit_show_requires_database() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    assert!(exec(
        &engine,
        &mut session,
        "SHOW EVENTS FROM nonexistent_event_audit_db"
    )
    .is_err());
    session.current_database.clear();
    for sql in ["SHOW EVENTS", "SHOW CREATE EVENT absent"] {
        let err = exec(&engine, &mut session, sql).unwrap_err();
        assert_eq!(err.code(), 1046, "{sql}: {err:?}");
    }
    exec(&engine, &mut session, "SHOW EVENTS FROM test").unwrap();
}

#[test]
fn event_audit_database_collation() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE DATABASE event_audit_db COLLATE utf8mb4_general_ci",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "SET collation_connection = 'utf8mb4_bin'",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE EVENT event_audit_db.e ON SCHEDULE EVERY 1 DAY DISABLE DO SELECT 1",
    )
    .unwrap();
    let event = engine
        .database()
        .event_definition("event_audit_db", "e")
        .unwrap();
    assert_eq!(event.database_collation, "utf8mb4_general_ci");
    assert_eq!(event.collation_connection, "utf8mb4_bin");
}

#[test]
fn event_audit_stale_worker_cannot_mutate_altered_definition() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for completion in ["PRESERVE", "NOT PRESERVE"] {
        exec(&engine, &mut session, &format!("CREATE OR REPLACE EVENT stale ON SCHEDULE EVERY 1 DAY ON COMPLETION {completion} DISABLE DO SELECT 1")).unwrap();
        let db = engine.database();
        let claimed = db.event_definition("test", "stale").unwrap();
        exec(
            &engine,
            &mut session,
            "ALTER EVENT stale ON SCHEDULE EVERY 1 DAY STARTS '2099-01-01' ENABLE",
        )
        .unwrap();
        let changed = db.event_definition("test", "stale").unwrap();
        assert!(db
            .record_event_run("test", "stale", claimed.schema_version, 12345)
            .unwrap()
            .is_none());
        assert!(!db.complete_event_if_version(&claimed).unwrap());
        let current = db.event_definition("test", "stale").unwrap();
        assert_eq!(current.schema_version, changed.schema_version);
        assert_eq!(current.last_executed_ms, 0);
        assert_eq!(current.status, crate::storage::EventStatusSchema::Enabled);
        let recorded = db
            .record_event_run("test", "stale", current.schema_version, 12345)
            .unwrap()
            .unwrap();
        // Out-of-order completions cannot move a version's watermark backwards.
        assert_eq!(
            db.record_event_run("test", "stale", current.schema_version, 12000)
                .unwrap()
                .unwrap()
                .last_executed_ms,
            12345
        );
        assert!(db.complete_event_if_version(&recorded).unwrap());
        match db.event_definition("test", "stale") {
            Some(event) => {
                assert_eq!(completion, "PRESERVE");
                assert_eq!(event.status, crate::storage::EventStatusSchema::Disabled);
            }
            None => assert_eq!(completion, "NOT PRESERVE"),
        }
    }
}

#[test]
fn event_audit_last_executed_uses_event_timezone() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "SET time_zone = '+08:00'").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE EVENT tz_event ON SCHEDULE EVERY 1 DAY DISABLE DO SELECT 1",
    )
    .unwrap();
    let event = engine
        .database()
        .event_definition("test", "tz_event")
        .unwrap();
    engine
        .database()
        .record_event_run(
            "test",
            "tz_event",
            event.schema_version,
            ms(2026, 9, 25, 0, 0, 0),
        )
        .unwrap();
    for tz in ["+08:00", "+00:00", "-05:00"] {
        exec(&engine, &mut session, &format!("SET time_zone = '{tz}'")).unwrap();
        let QueryResponse::ResultSet(rs) = exec(
            &engine,
            &mut session,
            "SELECT LAST_EXECUTED FROM information_schema.EVENTS WHERE EVENT_NAME='tz_event'",
        )
        .unwrap() else {
            panic!()
        };
        assert_eq!(rs.rows[0][0].to_display_string(), "2026-09-25 08:00:00");
    }
}

#[test]
fn event_audit_disabled_startup_rejects_runtime_changes() {
    use crate::engine::executor::events::{
        scheduler_state, set_scheduler_state, EventSchedulerState,
    };
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let saved = scheduler_state();
    set_scheduler_state(EventSchedulerState::Disabled);
    for state in ["ON", "OFF"] {
        let err = exec(
            &engine,
            &mut session,
            &format!("SET GLOBAL event_scheduler = {state}"),
        )
        .unwrap_err();
        assert_eq!(err.code(), 1290);
        assert_eq!(scheduler_state(), EventSchedulerState::Disabled);
    }
    set_scheduler_state(saved);
}

#[test]
fn event_audit_compound_body_closes_background_transactions() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE event_txn_log(v INT)").unwrap();
    for body in [
        "BEGIN DECLARE n INT DEFAULT 0; SET n=1; END",
        "BEGIN START TRANSACTION; INSERT INTO event_txn_log VALUES(1); END",
        "BEGIN SIGNAL SQLSTATE '45000'; END",
    ] {
        exec(
            &engine,
            &mut session,
            &format!("CREATE OR REPLACE EVENT cleanup ON SCHEDULE EVERY 1 DAY DISABLE DO {body}"),
        )
        .unwrap();
        let event = engine
            .database()
            .event_definition("test", "cleanup")
            .unwrap();
        let mut background = SessionContext::default();
        let result = crate::engine::executor::routines::execute_event_body(
            engine.database(),
            &mut background,
            &event,
            engine.registry(),
        );
        assert_eq!(result.is_err(), body.contains("SIGNAL"));
        assert!(background.current_txn.is_none(), "{body}");
        assert!(!background.explicit_txn, "{body}");
    }
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SELECT COUNT(*) FROM event_txn_log").unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(0)]]);
}
