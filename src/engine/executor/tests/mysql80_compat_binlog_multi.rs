//! Row-binlog coverage for one COM_QUERY carrying multiple statements.

use super::*;

use crate::binlog::{self, BinlogConfig, BinlogEventChunk};

const QUERY_EVENT: u8 = 0x02;
const XID_EVENT: u8 = 0x10;
const TABLE_MAP_EVENT: u8 = 0x13;
const WRITE_ROWS_EVENT_V2: u8 = 0x1e;
const UPDATE_ROWS_EVENT_V2: u8 = 0x1f;
const DELETE_ROWS_EVENT_V2: u8 = 0x20;
const GTID_EVENT: u8 = 0x21;

#[test]
fn binlog_consistent_snapshot_pins_position_and_gtids_together() {
    for read_only in [false, true] {
        let fixture = BinlogFixture::open_strict();
        let engine = &fixture.engine;
        engine
            .execute("CREATE TABLE snapshot_t(id INT PRIMARY KEY)")
            .unwrap();
        let mut session = SessionContext::default();
        engine
            .execute_with_session(
                &mut session,
                "SET SESSION transaction_isolation='REPEATABLE-READ'",
            )
            .unwrap();
        if read_only {
            engine
                .execute_with_session(&mut session, "SET SESSION transaction_read_only=1")
                .unwrap();
        }
        engine
            .execute_with_session(&mut session, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
            .unwrap();
        let status = |session: &mut SessionContext| {
            let QueryResponse::ResultSet(result) = engine
                .execute_with_session(session, "SHOW MASTER STATUS")
                .unwrap()
            else {
                panic!("status");
            };
            result.rows
        };
        let before = status(&mut session);
        engine.execute("INSERT INTO snapshot_t VALUES(1)").unwrap();
        let after = status(&mut session);
        assert_eq!(
            before, after,
            "snapshot coordinates moved after a later commit"
        );
        let QueryResponse::ResultSet(rows) = engine
            .execute_with_session(&mut session, "SELECT id FROM snapshot_t")
            .unwrap()
        else {
            panic!("snapshot rows");
        };
        assert!(rows.rows.is_empty());
        engine
            .execute_with_session(&mut session, "ROLLBACK")
            .unwrap();
        assert_ne!(status(&mut session)[0][4], before[0][4]);
    }
}

#[test]
fn binlog_consistent_snapshot_waits_for_deferred_commit_publication() {
    use std::time::Duration;
    let fixture = BinlogFixture::open_strict();
    let engine = &fixture.engine;
    engine
        .execute("CREATE TABLE snapshot_t(id INT PRIMARY KEY)")
        .unwrap();
    let db = engine.database_for_replication();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(txn, "test.snapshot_t", vec![vec![Value::Int(1)]])
        .unwrap();
    let mut commit = db.commit_txn_deferred(txn.txn_id).unwrap();
    let (started, ready) = std::sync::mpsc::channel();
    let (finished, result) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        let worker = scope.spawn(move || {
            let mut session = SessionContext::default();
            engine
                .execute_with_session(
                    &mut session,
                    "SET SESSION transaction_isolation='REPEATABLE-READ'",
                )
                .unwrap();
            started.send(()).unwrap();
            engine
                .execute_with_session(&mut session, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
                .unwrap();
            let QueryResponse::ResultSet(rows) = engine
                .execute_with_session(&mut session, "SELECT id FROM snapshot_t")
                .unwrap()
            else {
                panic!("snapshot rows");
            };
            engine
                .execute_with_session(&mut session, "ROLLBACK")
                .unwrap();
            finished.send(rows.rows).unwrap();
        });
        ready.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(
            result.recv_timeout(Duration::from_millis(100)).is_err(),
            "snapshot escaped before its binlog transaction was published"
        );
        db.finish_deferred_commits(&mut [&mut commit]).unwrap();
        let rows = result.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(rows, vec![vec![Value::Int(1)]]);
        worker.join().unwrap();
    });
}

#[test]
fn binlog_size_setting_rotates_and_unsupported_limit_settings_are_read_only() {
    let fixture = BinlogFixture::open();
    for name in [
        "max_binlog_cache_size",
        "max_binlog_stmt_cache_size",
        "expire_logs_days",
    ] {
        let err = fixture
            .engine
            .execute(&format!("SET GLOBAL {name}=12345"))
            .unwrap_err();
        assert_eq!(err.code(), 1238, "{name}");
    }
    fixture
        .engine
        .execute("SET GLOBAL max_binlog_size=4096")
        .unwrap();
    let manager = binlog::manager().unwrap();
    let first = manager.current_position().unwrap().file;
    fixture
        .engine
        .execute("CREATE TABLE sized(id INT PRIMARY KEY, v VARCHAR(200))")
        .unwrap();
    for id in 0..32 {
        fixture
            .engine
            .execute(&format!("INSERT INTO sized VALUES({id}, REPEAT('x',100))"))
            .unwrap();
    }
    assert_ne!(manager.current_position().unwrap().file, first);
    assert_eq!(binlog::config_snapshot().rotate_bytes, Some(4096));
}

struct BinlogFixture {
    _dir: tempfile::TempDir,
    engine: StorageQueryEngine,
    binlog_dir: std::path::PathBuf,
}

impl BinlogFixture {
    fn open() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        let binlog_dir = dir.path().join("binlog");
        std::fs::create_dir_all(&binlog_dir).unwrap();
        binlog::init_global(
            binlog_dir.clone(),
            BinlogConfig {
                enabled: true,
                base_name: "mydb-bin".into(),
                ..BinlogConfig::default()
            },
        )
        .unwrap();
        Self {
            _dir: dir,
            engine,
            binlog_dir,
        }
    }

    fn open_strict() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageQueryEngine::open(dir.path()).unwrap();
        let binlog_dir = dir.path().join("binlog");
        std::fs::create_dir_all(&binlog_dir).unwrap();
        binlog::init_global(
            binlog_dir.clone(),
            BinlogConfig {
                enabled: true,
                base_name: "mydb-bin".into(),
                ..BinlogConfig::default()
            },
        )
        .unwrap();
        Self {
            _dir: dir,
            engine,
            binlog_dir,
        }
    }

    fn events(&self) -> Vec<BinlogEventChunk> {
        let manager = binlog::manager().expect("binlog manager installed");
        let first = manager
            .binary_logs()
            .unwrap()
            .into_iter()
            .next()
            .expect("active binlog");
        manager.events_after(&first.name, 4).unwrap()
    }
}

#[test]
fn physical_alter_commits_dd_wal_and_query_event_under_one_gtid() {
    let fixture = BinlogFixture::open_strict();
    fixture
        .engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    fixture
        .engine
        .execute("INSERT INTO t VALUES(1,10),(2,20)")
        .unwrap();
    let before = fixture.events().len();
    fixture
        .engine
        .execute("ALTER TABLE t ALGORITHM=COPY, MODIFY v BIGINT")
        .unwrap();
    let events = fixture.events();
    let tail = &events[before..];
    assert_eq!(count_type(tail, GTID_EVENT), 1, "{:#?}", event_types(tail));
    assert_eq!(count_type(tail, XID_EVENT), 0, "{:#?}", event_types(tail));
    let alter_queries = tail
        .iter()
        .filter(|event| event.event_type == QUERY_EVENT)
        .map(decode_query_event)
        .filter(|event| event.sql.to_ascii_uppercase().starts_with("ALTER TABLE"))
        .count();
    assert_eq!(alter_queries, 1, "{:#?}", event_types(tail));
}

#[test]
fn ddl_audit_final_catalog_publications_have_one_logical_transaction() {
    let fixture = BinlogFixture::open_strict();
    for sql in [
        "CREATE DATABASE ddl_db",
        "ALTER DATABASE ddl_db CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
        "CREATE TABLE ddl_db.t(id INT PRIMARY KEY,v INT)",
        "ALTER TABLE ddl_db.t COMMENT='published'",
        "CREATE INDEX vi ON ddl_db.t(v)",
        "DROP INDEX vi ON ddl_db.t",
        "TRUNCATE TABLE ddl_db.t",
        "CREATE SEQUENCE ddl_db.s MAXVALUE 1000",
        "ALTER SEQUENCE ddl_db.s RESTART WITH 500",
        "DROP SEQUENCE ddl_db.s",
        "DROP DATABASE ddl_db",
    ] {
        let before = fixture.events().len();
        fixture.engine.execute(sql).unwrap();
        let events = fixture.events();
        let tail = &events[before..];
        assert_eq!(
            count_type(tail, GTID_EVENT),
            1,
            "{sql}: {:?}",
            event_types(tail)
        );
        assert_eq!(
            count_type(tail, XID_EVENT),
            0,
            "{sql}: {:?}",
            event_types(tail)
        );
        assert_eq!(
            tail.iter()
                .filter(|event| event.event_type == QUERY_EVENT)
                .map(decode_query_event)
                .filter(|query| query.sql.eq_ignore_ascii_case(sql))
                .count(),
            1,
            "logical DDL missing or duplicated: {sql}"
        );
    }
}

impl Drop for BinlogFixture {
    fn drop(&mut self) {
        let _ = binlog::init_global(self.binlog_dir.clone(), BinlogConfig::default());
    }
}

fn event_types(events: &[BinlogEventChunk]) -> Vec<u8> {
    events.iter().map(|event| event.event_type).collect()
}

fn count_type(events: &[BinlogEventChunk], event_type: u8) -> usize {
    events
        .iter()
        .filter(|event| event.event_type == event_type)
        .count()
}

struct QueryEventView {
    database: String,
    sql: String,
    flags2: u32,
    sql_mode: u64,
}

fn decode_query_event(event: &BinlogEventChunk) -> QueryEventView {
    assert_eq!(event.event_type, QUERY_EVENT);
    // Common header is 19 bytes and these fixtures use CRC32 (4-byte trailer).
    let payload = &event.bytes[19..event.bytes.len() - 4];
    assert!(payload.len() >= 13);
    let database_len = payload[8] as usize;
    let status_len = u16::from_le_bytes([payload[11], payload[12]]) as usize;
    let status = &payload[13..13 + status_len];
    assert_eq!(status[0], 0x00, "Q_FLAGS2_CODE must lead status vars");
    let flags2 = u32::from_le_bytes(status[1..5].try_into().unwrap());
    assert_eq!(status[5], 0x01, "Q_SQL_MODE_CODE must follow flags2");
    let sql_mode = u64::from_le_bytes(status[6..14].try_into().unwrap());
    let database_start = 13 + status_len;
    let sql_start = database_start + database_len + 1;
    QueryEventView {
        database: String::from_utf8(
            payload[database_start..database_start + database_len].to_vec(),
        )
        .unwrap(),
        sql: String::from_utf8(payload[sql_start..].to_vec()).unwrap(),
        flags2,
        sql_mode,
    }
}

fn query_events(events: &[BinlogEventChunk]) -> Vec<QueryEventView> {
    events
        .iter()
        .filter(|event| event.event_type == QUERY_EVENT)
        .map(decode_query_event)
        .collect()
}

#[test]
fn binlog_multi_statement_preserves_autocommit_and_explicit_txn_boundaries() {
    let fixture = BinlogFixture::open();
    let engine = &fixture.engine;
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE DATABASE ms; USE ms; CREATE TABLE t(id INT PRIMARY KEY, n INT)",
        )
        .unwrap();

    let baseline = fixture.events().len();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO t VALUES (1,10); \
             UPDATE t SET n=11 WHERE id=1; \
             DELETE FROM t WHERE id=1",
        )
        .unwrap();
    let events = fixture.events();
    let tail = &events[baseline..];
    assert_eq!(count_type(tail, GTID_EVENT), 3);
    assert_eq!(count_type(tail, XID_EVENT), 3);
    assert_eq!(count_type(tail, WRITE_ROWS_EVENT_V2), 1);
    assert_eq!(count_type(tail, UPDATE_ROWS_EVENT_V2), 1);
    assert_eq!(count_type(tail, DELETE_ROWS_EVENT_V2), 1);

    let baseline = events.len();
    engine
        .execute_with_session(
            &mut session,
            "BEGIN; INSERT INTO t VALUES (2,20); \
             UPDATE t SET n=21 WHERE id=2; COMMIT",
        )
        .unwrap();
    let events = fixture.events();
    let tail = &events[baseline..];
    assert_eq!(count_type(tail, GTID_EVENT), 1);
    assert_eq!(count_type(tail, XID_EVENT), 1);
    assert_eq!(count_type(tail, WRITE_ROWS_EVENT_V2), 1);
    assert_eq!(count_type(tail, UPDATE_ROWS_EVENT_V2), 1);
    let types = event_types(tail);
    assert!(
        types.iter().position(|kind| *kind == WRITE_ROWS_EVENT_V2)
            < types.iter().position(|kind| *kind == UPDATE_ROWS_EVENT_V2)
    );
}

#[test]
fn binlog_multi_statement_mixed_ddl_uses_child_sql_and_implicit_commits() {
    let fixture = BinlogFixture::open();
    let engine = &fixture.engine;
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE DATABASE mixed; USE mixed; CREATE TABLE t(id INT PRIMARY KEY)",
        )
        .unwrap();

    let baseline = fixture.events().len();
    engine
        .execute_with_session(
            &mut session,
            "BEGIN; INSERT INTO t VALUES (1); \
             CREATE TABLE u(id INT PRIMARY KEY); INSERT INTO t VALUES (2); COMMIT",
        )
        .unwrap();
    let events = fixture.events();
    let tail = &events[baseline..];
    assert_eq!(count_type(tail, GTID_EVENT), 3);
    assert_eq!(count_type(tail, XID_EVENT), 2);
    assert_eq!(count_type(tail, WRITE_ROWS_EVENT_V2), 2);
    let queries = query_events(tail);
    assert_eq!(
        queries.len(),
        3,
        "two row transaction BEGIN events plus the standalone DDL query"
    );
    let ddl = queries
        .iter()
        .find(|query| query.sql.contains("create table u"))
        .expect("child CREATE TABLE query event");
    assert_eq!(ddl.database, "mixed");
    assert_eq!(ddl.sql, "create table u(id int primary key)");
    assert!(!ddl.sql.contains("insert into"));

    let QueryResponse::ResultSet(rows) = engine
        .execute_with_session(&mut session, "SELECT id FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
}

#[test]
fn binlog_multi_statement_captures_session_state_and_sql_log_bin_per_child() {
    let fixture = BinlogFixture::open();
    let engine = &fixture.engine;
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE DATABASE stateful; USE stateful")
        .unwrap();

    let baseline = fixture.events().len();
    engine
        .execute_with_session(
            &mut session,
            "SET SESSION sql_log_bin=0; \
             CREATE TABLE suppressed(id INT PRIMARY KEY); \
             INSERT INTO suppressed VALUES (1); \
             SET SESSION sql_log_bin=1; \
             SET SESSION sql_mode='ANSI_QUOTES'; \
             SET foreign_key_checks=0; SET unique_checks=0; \
             CREATE TABLE logged(id INT PRIMARY KEY); \
             INSERT INTO logged VALUES (2)",
        )
        .unwrap();
    let events = fixture.events();
    let tail = &events[baseline..];
    assert_eq!(count_type(tail, GTID_EVENT), 2);
    assert_eq!(count_type(tail, WRITE_ROWS_EVENT_V2), 1);
    assert_eq!(count_type(tail, XID_EVENT), 1);
    let queries = query_events(tail);
    assert_eq!(queries.len(), 2, "only the row transaction has a BEGIN");
    let ddl = queries
        .iter()
        .find(|query| query.sql.contains("create table logged"))
        .expect("logged DDL");
    assert!(!queries.iter().any(|query| query.sql.contains("suppressed")));
    assert_eq!(
        ddl.sql_mode,
        crate::binlog::sql_mode_str_to_bits("ANSI_QUOTES")
    );
    assert_ne!(ddl.flags2 & 0x0400_0000, 0);
    assert_ne!(ddl.flags2 & 0x0800_0000, 0);

    let baseline = events.len();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TEMPORARY TABLE tmp(id INT); INSERT INTO tmp VALUES (9); \
             CREATE TABLE copied AS SELECT id FROM logged",
        )
        .unwrap();
    let events = fixture.events();
    let tail = &events[baseline..];
    assert_eq!(count_type(tail, GTID_EVENT), 1);
    assert_eq!(count_type(tail, XID_EVENT), 1);
    assert_eq!(count_type(tail, WRITE_ROWS_EVENT_V2), 1);
    let queries = query_events(tail);
    assert_eq!(
        queries
            .iter()
            .filter(|query| {
                let sql = query.sql.to_ascii_lowercase();
                sql.contains("create table") && sql.contains("copied")
            })
            .count(),
        1,
        "CTAS must log its resolved CREATE exactly once"
    );
    assert!(!queries
        .iter()
        .any(|query| query.sql.to_ascii_lowercase().contains("tmp")));
}

#[test]
fn binlog_multi_statement_stops_on_error_and_truncates_savepoint_ops() {
    let fixture = BinlogFixture::open();
    let engine = &fixture.engine;
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE DATABASE failures; USE failures; CREATE TABLE t(id INT PRIMARY KEY)",
        )
        .unwrap();

    let baseline = fixture.events().len();
    let err = engine
        .execute_with_session(
            &mut session,
            "INSERT INTO t VALUES (1); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)",
        )
        .unwrap_err();
    assert_eq!(err.code(), 1062);
    let events = fixture.events();
    let tail = &events[baseline..];
    assert_eq!(count_type(tail, GTID_EVENT), 1);
    assert_eq!(count_type(tail, WRITE_ROWS_EVENT_V2), 1);
    assert_eq!(count_type(tail, XID_EVENT), 1);

    let baseline = events.len();
    let err = engine
        .execute_with_session(
            &mut session,
            "BEGIN; INSERT INTO t VALUES (5); \
             INSERT INTO t VALUES (1); INSERT INTO t VALUES (6)",
        )
        .unwrap_err();
    assert_eq!(err.code(), 1062);
    assert!(
        session.in_transaction(),
        "statement error must leave the explicit transaction open"
    );
    assert_eq!(
        fixture.events().len(),
        baseline,
        "uncommitted row ops must not be published"
    );
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    assert_eq!(fixture.events().len(), baseline);

    engine
        .execute_with_session(
            &mut session,
            "BEGIN; INSERT INTO t VALUES (3); SAVEPOINT s; \
             INSERT INTO t VALUES (4); ROLLBACK TO SAVEPOINT s; COMMIT",
        )
        .unwrap();
    let events = fixture.events();
    let tail = &events[baseline..];
    assert_eq!(count_type(tail, GTID_EVENT), 1);
    assert_eq!(count_type(tail, WRITE_ROWS_EVENT_V2), 1);
    assert_eq!(count_type(tail, XID_EVENT), 1);
    assert_eq!(count_type(tail, TABLE_MAP_EVENT), 1);

    let QueryResponse::ResultSet(rows) = engine
        .execute_with_session(&mut session, "SELECT id FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(1)], vec![Value::Int(3)]]);
}

#[test]
fn optimize_online_binlog_modifiers_and_per_table_success() {
    let fixture = BinlogFixture::open_strict();
    let engine = &fixture.engine;
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1),(2)").unwrap();
    let before = fixture.events().len();
    for sql in [
        "OPTIMIZE LOCAL TABLE t",
        "OPTIMIZE NO_WRITE_TO_BINLOG TABLE t",
    ] {
        let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
            panic!("status")
        };
        assert_eq!(result.rows[0][3], Value::String("OK".into()));
    }
    assert_eq!(
        fixture.events().len(),
        before,
        "suppressed maintenance must not binlog private rows or DDL"
    );
    engine.execute("OPTIMIZE TABLE missing,t").unwrap();
    let events = fixture.events();
    let tail = &events[before..];
    let queries = query_events(tail);
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].sql, "OPTIMIZE TABLE `test`.`t`");
    assert_eq!(count_type(tail, WRITE_ROWS_EVENT_V2), 0);
    assert_eq!(count_type(tail, DELETE_ROWS_EVENT_V2), 0);
    assert_eq!(count_type(tail, UPDATE_ROWS_EVENT_V2), 0);
}

#[test]
fn ddl_metadata_rollback_does_not_binlog_the_rejected_statement() {
    let fixture = BinlogFixture::open_strict();
    let engine = &fixture.engine;
    engine
        .execute("CREATE TABLE p(id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("CREATE TABLE c(id INT PRIMARY KEY,pid INT,KEY backing(pid),CONSTRAINT fk FOREIGN KEY(pid) REFERENCES p(id) ON DELETE CASCADE)").unwrap();
    engine.execute("INSERT INTO p VALUES(1)").unwrap();
    engine.execute("INSERT INTO c VALUES(1,1)").unwrap();
    for ddl in [
        "ALTER TABLE c RENAME INDEX backing TO rejected",
        "ALTER TABLE c COMMENT='rejected'",
    ] {
        let before = fixture.events().len();
        engine.database().fail_next_catalog_commit_for_test();
        assert!(engine.execute(ddl).is_err());
        assert_eq!(
            fixture.events().len(),
            before,
            "rollback must not replicate rejected SQL: {ddl}"
        );
    }
    engine.execute("DELETE FROM p WHERE id=1").unwrap();
    let QueryResponse::ResultSet(result) = engine.execute("SELECT * FROM c").unwrap() else {
        panic!()
    };
    assert!(
        result.rows.is_empty(),
        "rollback must restore the parent's inbound FK backing name"
    );
}
