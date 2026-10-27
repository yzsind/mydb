use super::*;

// XA happy paths + state-machine error coverage. Splits into:
//   1. session-bound state machine (XA START / END / ROLLBACK / ONE PHASE)
//   2. detached PREPARE → COMMIT / ROLLBACK
//   3. crash-recovery round-trip
//   4. cross-session conflict / invisibility
//   5. parse-rejected / unsupported sub-clauses
//
// Tests use `open_strict_test_engine` so `XA PREPARE` actually fsyncs the
// PrepareTxn record — the recovery cases rely on this.

fn expect_ok(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) {
    engine
        .execute_with_session(session, sql)
        .unwrap_or_else(|err| panic!("expected `{sql}` to succeed, got {err:?}"));
}

fn expect_err_code(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    sql: &str,
    code: u16,
) {
    let err = engine
        .execute_with_session(session, sql)
        .err()
        .unwrap_or_else(|| panic!("expected `{sql}` to error with code {code}"));
    assert_eq!(err.code(), code, "got {err:?} from `{sql}`");
}

fn fetch_int_rows(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    sql: &str,
) -> Vec<i64> {
    let QueryResponse::ResultSet(rs) = engine.execute_with_session(session, sql).unwrap() else {
        panic!("expected ResultSet for `{sql}`");
    };
    rs.rows
        .into_iter()
        .map(|row| match row[0] {
            Value::Int(v) => v,
            ref other => panic!("expected integer column 0, got {other:?}"),
        })
        .collect()
}

fn row_count(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> usize {
    let QueryResponse::ResultSet(rs) = engine.execute_with_session(session, sql).unwrap() else {
        panic!("expected ResultSet for `{sql}`");
    };
    rs.rows.len()
}

fn fresh_root_session() -> SessionContext {
    SessionContext::default()
}

#[test]
fn xa_two_phase_commit_happy_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    expect_ok(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
    expect_ok(&engine, &mut s, "XA START 'gtx1'");
    expect_ok(&engine, &mut s, "INSERT INTO t VALUES (1), (2)");
    expect_ok(&engine, &mut s, "XA END 'gtx1'");
    expect_ok(&engine, &mut s, "XA PREPARE 'gtx1'");
    // Branch is now PREPARED & detached. Either same or other session
    // can commit. Use a fresh session to prove that.
    let mut other = fresh_root_session();
    expect_ok(&engine, &mut other, "XA COMMIT 'gtx1'");
    let rows = fetch_int_rows(&engine, &mut other, "SELECT id FROM t ORDER BY id");
    assert_eq!(rows, vec![1, 2]);
}

#[test]
fn xa_start_rejects_reduced_redo_session() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut session = fresh_root_session();
    expect_ok(&engine, &mut session, "SET SESSION mydb_redo_enabled=OFF");
    expect_err_code(&engine, &mut session, "XA START 'reduced'", 3983);
}

#[test]
fn xa_one_phase_commit_skips_prepare() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    expect_ok(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
    expect_ok(&engine, &mut s, "XA START 'one'");
    expect_ok(&engine, &mut s, "INSERT INTO t VALUES (7)");
    expect_ok(&engine, &mut s, "XA END 'one'");
    expect_ok(&engine, &mut s, "XA COMMIT 'one' ONE PHASE");
    let rows = fetch_int_rows(&engine, &mut s, "SELECT id FROM t");
    assert_eq!(rows, vec![7]);
    // After commit the session is no longer in an XA branch.
    assert!(s.xa_branch.is_none());
    // And no PREPARED entry exists.
    assert_eq!(
        row_count(
            &engine,
            &mut s,
            "SELECT FORMAT_ID FROM information_schema.mydb_xa_recover"
        ),
        0
    );
}

#[test]
fn xa_rollback_after_prepare_undoes_writes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    expect_ok(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
    expect_ok(&engine, &mut s, "XA START 'rb'");
    expect_ok(&engine, &mut s, "INSERT INTO t VALUES (99)");
    expect_ok(&engine, &mut s, "XA END 'rb'");
    expect_ok(&engine, &mut s, "XA PREPARE 'rb'");
    let mut other = fresh_root_session();
    expect_ok(&engine, &mut other, "XA ROLLBACK 'rb'");
    assert_eq!(row_count(&engine, &mut other, "SELECT id FROM t"), 0);
}

#[test]
fn xa_rollback_in_active_state_undoes_writes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    expect_ok(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
    expect_ok(&engine, &mut s, "XA START 'a'");
    expect_ok(&engine, &mut s, "INSERT INTO t VALUES (10)");
    expect_ok(&engine, &mut s, "XA END 'a'");
    expect_ok(&engine, &mut s, "XA ROLLBACK 'a'");
    assert_eq!(row_count(&engine, &mut s, "SELECT id FROM t"), 0);
    assert!(s.xa_branch.is_none());
}

#[test]
fn xa_recover_lists_prepared_branch() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    expect_ok(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
    expect_ok(&engine, &mut s, "XA START 'gtxA'");
    expect_ok(&engine, &mut s, "INSERT INTO t VALUES (1)");
    expect_ok(&engine, &mut s, "XA END 'gtxA'");
    expect_ok(&engine, &mut s, "XA PREPARE 'gtxA'");
    let QueryResponse::ResultSet(rs) = engine.execute_with_session(&mut s, "XA RECOVER").unwrap()
    else {
        panic!("expected ResultSet");
    };
    assert_eq!(rs.rows.len(), 1);
    let row = &rs.rows[0];
    // (formatID, gtrid_length, bqual_length, data)
    assert_eq!(row[0], Value::Int(1));
    assert_eq!(row[1], Value::Int(4)); // "gtxA"
    assert_eq!(row[2], Value::Int(0));
    assert_eq!(row[3], Value::String("gtxA".to_string()));
    // Roll back to leave a clean state.
    expect_ok(&engine, &mut s, "XA ROLLBACK 'gtxA'");
}

#[test]
fn xa_recover_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let xid = "gtx-survive";
    {
        let engine = open_strict_test_engine(dir.path());
        let mut s = fresh_root_session();
        expect_ok(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
        expect_ok(&engine, &mut s, &format!("XA START '{xid}'"));
        expect_ok(&engine, &mut s, "INSERT INTO t VALUES (123)");
        expect_ok(&engine, &mut s, &format!("XA END '{xid}'"));
        expect_ok(&engine, &mut s, &format!("XA PREPARE '{xid}'"));
        drop(engine);
    }
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    let QueryResponse::ResultSet(rs) = engine.execute_with_session(&mut s, "XA RECOVER").unwrap()
    else {
        panic!("expected ResultSet");
    };
    assert_eq!(rs.rows.len(), 1, "PREPARED branch should survive restart");
    assert_eq!(rs.rows[0][3], Value::String(xid.to_string()));
    // Clean up.
    expect_ok(&engine, &mut s, &format!("XA ROLLBACK '{xid}'"));
}

#[test]
fn xa_prepared_mdl_survives_restart_and_blocks_ddl() {
    let dir = tempfile::tempdir().unwrap();
    let xid = "mdl-survive";
    {
        let engine = open_strict_test_engine(dir.path());
        let mut s = fresh_root_session();
        s.connection_id = 70;
        expect_ok(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
        expect_ok(&engine, &mut s, &format!("XA START '{xid}'"));
        expect_ok(&engine, &mut s, "INSERT INTO t VALUES (1)");
        expect_ok(&engine, &mut s, &format!("XA END '{xid}'"));
        expect_ok(&engine, &mut s, &format!("XA PREPARE '{xid}'"));
    }

    let engine = std::sync::Arc::new(open_strict_test_engine(dir.path()));
    let worker_engine = std::sync::Arc::clone(&engine);
    let handle = std::thread::spawn(move || {
        let mut ddl = fresh_root_session();
        ddl.connection_id = 71;
        worker_engine.execute_with_session(&mut ddl, "ALTER TABLE t ADD COLUMN v INT")
    });

    let mut observer = fresh_root_session();
    observer.connection_id = 72;
    let mut saw_pending = false;
    for _ in 0..100 {
        let QueryResponse::ResultSet(mdl) = engine
            .execute_with_session(
                &mut observer,
                "SELECT LOCK_STATUS FROM performance_schema.metadata_locks",
            )
            .unwrap()
        else {
            panic!("expected metadata_locks result");
        };
        if mdl
            .rows
            .iter()
            .any(|row| row[0] == Value::String("PENDING".to_string()))
        {
            saw_pending = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(saw_pending, "DDL should wait on recovered prepared XA MDL");

    expect_ok(
        engine.as_ref(),
        &mut observer,
        &format!("XA COMMIT '{xid}'"),
    );
    handle.join().unwrap().unwrap();
}

#[test]
fn xa_commit_after_restart_commits_writes() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        let mut s = fresh_root_session();
        expect_ok(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
        expect_ok(&engine, &mut s, "XA START 'go'");
        expect_ok(&engine, &mut s, "INSERT INTO t VALUES (5), (6)");
        expect_ok(&engine, &mut s, "XA END 'go'");
        expect_ok(&engine, &mut s, "XA PREPARE 'go'");
        drop(engine);
    }
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    expect_ok(&engine, &mut s, "XA COMMIT 'go'");
    let rows = fetch_int_rows(&engine, &mut s, "SELECT id FROM t ORDER BY id");
    assert_eq!(rows, vec![5, 6]);
}

#[test]
fn xa_prepare_restart_commit_replays_zone_map_before_pruning() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        let mut s = fresh_root_session();
        expect_ok(
            &engine,
            &mut s,
            "CREATE TABLE zm_xa(id INT PRIMARY KEY, pad VARCHAR(700)) \
             ZONE_MAP=ON ZONE_SIZE=64K",
        );
        expect_ok(&engine, &mut s, "INSERT INTO zm_xa VALUES (1,'base')");
        engine.db_for_tests().checkpoint().unwrap();
        expect_ok(&engine, &mut s, "XA START 'zm-xa'");
        expect_ok(
            &engine,
            &mut s,
            &format!("INSERT INTO zm_xa VALUES (9000,'{}')", "x".repeat(600)),
        );
        expect_ok(&engine, &mut s, "XA END 'zm-xa'");
        expect_ok(&engine, &mut s, "XA PREPARE 'zm-xa'");
    }

    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    expect_ok(&engine, &mut s, "XA COMMIT 'zm-xa'");
    let zone = engine
        .execute_with_session(
            &mut s,
            "SELECT /*+ ZONE_MAP(zm_xa) */ id FROM zm_xa WHERE id=9000",
        )
        .unwrap();
    let oracle = engine
        .execute_with_session(
            &mut s,
            "SELECT /*+ NO_ZONE_MAP(zm_xa) */ id FROM zm_xa WHERE id=9000",
        )
        .unwrap();
    let QueryResponse::ResultSet(zone) = zone else {
        panic!("expected Zone Map result set")
    };
    let QueryResponse::ResultSet(oracle) = oracle else {
        panic!("expected SeqScan result set")
    };
    assert_eq!(zone.rows, oracle.rows);
}

#[test]
fn xa_rollback_after_restart_drops_writes() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        let mut s = fresh_root_session();
        expect_ok(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
        expect_ok(&engine, &mut s, "XA START 'nope'");
        expect_ok(&engine, &mut s, "INSERT INTO t VALUES (200)");
        expect_ok(&engine, &mut s, "XA END 'nope'");
        expect_ok(&engine, &mut s, "XA PREPARE 'nope'");
        drop(engine);
    }
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    expect_ok(&engine, &mut s, "XA ROLLBACK 'nope'");
    assert_eq!(row_count(&engine, &mut s, "SELECT id FROM t"), 0);
}

#[test]
fn xa_state_error_begin_in_active() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    expect_ok(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
    expect_ok(&engine, &mut s, "XA START 'g1'");
    // Local transaction control is invalid in the XA branch state.
    expect_err_code(&engine, &mut s, "BEGIN", 1399);
    expect_ok(&engine, &mut s, "XA END 'g1'");
    expect_ok(&engine, &mut s, "XA ROLLBACK 'g1'");
}

#[test]
fn xa_state_error_commit_in_active() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    expect_ok(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
    expect_ok(&engine, &mut s, "XA START 'g2'");
    expect_err_code(&engine, &mut s, "COMMIT", 1399);
    expect_err_code(&engine, &mut s, "ROLLBACK", 1399);
    expect_ok(&engine, &mut s, "SAVEPOINT s1");
    expect_ok(&engine, &mut s, "XA END 'g2'");
    expect_ok(&engine, &mut s, "XA ROLLBACK 'g2'");
}

#[test]
fn xa_state_error_prepare_without_end_is_rmfail() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    expect_ok(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
    expect_ok(&engine, &mut s, "XA START 'pre'");
    // PREPARE requires IDLE state; ACTIVE → XAER_RMFAIL (1399).
    expect_err_code(&engine, &mut s, "XA PREPARE 'pre'", 1399);
    expect_ok(&engine, &mut s, "XA END 'pre'");
    expect_ok(&engine, &mut s, "XA ROLLBACK 'pre'");
}

#[test]
fn xa_state_error_double_start_is_rmfail() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    expect_ok(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
    expect_ok(&engine, &mut s, "XA START 'a'");
    expect_err_code(&engine, &mut s, "XA START 'b'", 1399);
    expect_ok(&engine, &mut s, "XA END 'a'");
    expect_ok(&engine, &mut s, "XA ROLLBACK 'a'");
}

#[test]
fn xa_commit_unknown_xid_is_nota() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    expect_err_code(&engine, &mut s, "XA COMMIT 'nope'", 1397);
    expect_err_code(&engine, &mut s, "XA ROLLBACK 'nope'", 1397);
    expect_err_code(&engine, &mut s, "XA COMMIT 'nope' ONE PHASE", 1397);
}

#[test]
fn xa_duplicate_xid_after_prepare_is_dupid() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s1 = fresh_root_session();
    let mut s2 = fresh_root_session();
    expect_ok(&engine, &mut s1, "CREATE TABLE t(id INT PRIMARY KEY)");
    expect_ok(&engine, &mut s1, "XA START 'dup'");
    expect_ok(&engine, &mut s1, "XA END 'dup'");
    expect_ok(&engine, &mut s1, "XA PREPARE 'dup'");
    // Second session attempting XA START with the same XID — must reject.
    expect_err_code(&engine, &mut s2, "XA START 'dup'", 1440);
    expect_ok(&engine, &mut s1, "XA ROLLBACK 'dup'");
}

#[test]
fn xa_prepared_branch_is_invisible_to_other_sessions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut writer = fresh_root_session();
    let mut reader = fresh_root_session();
    expect_ok(&engine, &mut writer, "CREATE TABLE t(id INT PRIMARY KEY)");
    expect_ok(&engine, &mut writer, "XA START 'iso'");
    expect_ok(&engine, &mut writer, "INSERT INTO t VALUES (42)");
    expect_ok(&engine, &mut writer, "XA END 'iso'");
    expect_ok(&engine, &mut writer, "XA PREPARE 'iso'");
    // Reader at default RC isolation must NOT see the PREPARED row.
    assert_eq!(row_count(&engine, &mut reader, "SELECT id FROM t"), 0);
    expect_ok(&engine, &mut writer, "XA COMMIT 'iso'");
    let rows = fetch_int_rows(&engine, &mut reader, "SELECT id FROM t");
    assert_eq!(rows, vec![42]);
}

#[test]
fn xa_xid_with_bqual_and_format_id_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    expect_ok(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
    expect_ok(&engine, &mut s, "XA START 'g','b',42");
    expect_ok(&engine, &mut s, "INSERT INTO t VALUES (1)");
    expect_ok(&engine, &mut s, "XA END 'g','b',42");
    expect_ok(&engine, &mut s, "XA PREPARE 'g','b',42");
    let QueryResponse::ResultSet(rs) = engine.execute_with_session(&mut s, "XA RECOVER").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
    let row = &rs.rows[0];
    assert_eq!(row[0], Value::Int(42));
    assert_eq!(row[1], Value::Int(1));
    assert_eq!(row[2], Value::Int(1));
    assert_eq!(row[3], Value::String("gb".to_string()));
    expect_ok(&engine, &mut s, "XA ROLLBACK 'g','b',42");
}

#[test]
fn xa_join_resume_suspend_unsupported() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    expect_ok(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
    // mydb routes Unsupported through the executor as 1235 (the standard
    // "parser knows about this but the engine doesn't run it yet").
    expect_err_code(&engine, &mut s, "XA START 'j' JOIN", 1235);
    expect_err_code(&engine, &mut s, "XA START 'j' RESUME", 1235);
    expect_err_code(&engine, &mut s, "XA END 'j' SUSPEND", 1235);
}

#[test]
fn xa_recover_convert_xid_emits_hex_data() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    expect_ok(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
    expect_ok(&engine, &mut s, "XA START 'gtx2'");
    expect_ok(&engine, &mut s, "XA END 'gtx2'");
    expect_ok(&engine, &mut s, "XA PREPARE 'gtx2'");
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut s, "XA RECOVER CONVERT XID")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
    // 'gtx2' bytes = 0x67 0x74 0x78 0x32 → 0x67747832
    assert_eq!(rs.rows[0][3], Value::String("0x67747832".to_string()));
    expect_ok(&engine, &mut s, "XA ROLLBACK 'gtx2'");
}

#[test]
fn xa_information_schema_recover_view_lists_branch() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    expect_ok(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
    expect_ok(&engine, &mut s, "XA START 'view'");
    expect_ok(&engine, &mut s, "XA END 'view'");
    expect_ok(&engine, &mut s, "XA PREPARE 'view'");
    assert_eq!(
        row_count(
            &engine,
            &mut s,
            "SELECT FORMAT_ID FROM information_schema.mydb_xa_recover",
        ),
        1
    );
    expect_ok(&engine, &mut s, "XA ROLLBACK 'view'");
    assert_eq!(
        row_count(
            &engine,
            &mut s,
            "SELECT FORMAT_ID FROM information_schema.mydb_xa_recover",
        ),
        0
    );
}

#[test]
fn xa_show_engines_advertises_yes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    let QueryResponse::ResultSet(rs) = engine.execute_with_session(&mut s, "SHOW ENGINES").unwrap()
    else {
        panic!("expected result set");
    };
    // Columns: Engine, Support, Comment, Transactions, XA, Savepoints
    let row = rs
        .rows
        .iter()
        .find(|r| matches!(&r[0], Value::String(s) if s.eq_ignore_ascii_case("mydb")))
        .expect("expected mydb row");
    assert_eq!(row[4], Value::String("YES".to_string()));
    assert_eq!(row[5], Value::String("YES".to_string()));
}

#[test]
fn xa_counters_advance_in_engine_status() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    expect_ok(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
    let (active0, commits0, rollbacks0) = engine.database().xa_status_counters();
    assert_eq!(active0, 0);
    expect_ok(&engine, &mut s, "XA START 'c1'");
    expect_ok(&engine, &mut s, "XA END 'c1'");
    expect_ok(&engine, &mut s, "XA PREPARE 'c1'");
    let (active1, _, _) = engine.database().xa_status_counters();
    assert_eq!(active1, 1, "PREPARE bumps prepared_active");
    expect_ok(&engine, &mut s, "XA COMMIT 'c1'");
    let (active2, commits1, _) = engine.database().xa_status_counters();
    assert_eq!(active2, 0, "COMMIT clears prepared_active");
    assert_eq!(commits1, commits0 + 1);
    let mut other = fresh_root_session();
    expect_ok(&engine, &mut other, "XA START 'c2'");
    expect_ok(&engine, &mut other, "XA END 'c2'");
    expect_ok(&engine, &mut other, "XA PREPARE 'c2'");
    expect_ok(&engine, &mut other, "XA ROLLBACK 'c2'");
    let (_, _, rollbacks1) = engine.database().xa_status_counters();
    assert_eq!(rollbacks1, rollbacks0 + 1);
}

#[test]
fn xa_two_phase_commit_writes_durably_with_strict() {
    // Smoke test the WAL fsync path: PREPARE under DurabilityMode::Strict
    // must reach durable_lsn (otherwise we'd lose PREPARED branches on a
    // crash before commit).
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    expect_ok(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
    expect_ok(&engine, &mut s, "XA START 'flush'");
    expect_ok(&engine, &mut s, "INSERT INTO t VALUES (1)");
    expect_ok(&engine, &mut s, "XA END 'flush'");
    let status_before = engine.database().storage_status();
    expect_ok(&engine, &mut s, "XA PREPARE 'flush'");
    let status_after = engine.database().storage_status();
    assert!(
        status_after.durable_lsn > status_before.durable_lsn,
        "XA PREPARE should advance durable_lsn under strict durability"
    );
    expect_ok(&engine, &mut s, "XA ROLLBACK 'flush'");
}

#[test]
fn session_exit_rolls_back_owned_xa_active_and_idle_branches() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    for idle in [false, true] {
        for writes in [false, true] {
            for reset in [false, true] {
                let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
                let engine = open_strict_test_engine(dir.path());
                let mut s = fresh_root_session();
                s.connection_id = 7101;
                expect_ok(&engine, &mut s, "CREATE TABLE exit_xa(id INT PRIMARY KEY)");
                expect_ok(&engine, &mut s, "INSERT INTO exit_xa VALUES (0)");
                expect_ok(&engine, &mut s, "XA START 'exit_owned'");
                let txn = s.xa_branch.as_ref().unwrap().txn_id;
                expect_ok(&engine, &mut s, "SELECT * FROM exit_xa FOR UPDATE");
                if writes {
                    expect_ok(&engine, &mut s, "INSERT INTO exit_xa VALUES (1)");
                }
                if idle {
                    expect_ok(&engine, &mut s, "XA END 'exit_owned'");
                    assert!(s.current_txn.is_none());
                }
                if reset {
                    engine.reset_session(&mut s).unwrap();
                } else {
                    engine.close_session(&mut s).unwrap();
                }
                assert!(
                    !engine.database().is_txn_active(txn),
                    "orphan txn={txn}, idle={idle}, writes={writes}, reset={reset}"
                );
                assert!(s.xa_branch.is_none(), "cleanup retained XA association");
                assert!(s.current_txn.is_none());
                let mut observer = fresh_root_session();
                observer.connection_id = 7102;
                assert_eq!(
                    fetch_int_rows(&engine, &mut observer, "SELECT id FROM exit_xa"),
                    vec![0]
                );
                expect_ok(&engine, &mut observer, "SET lock_wait_timeout=1");
                expect_ok(&engine, &mut observer, "DROP TABLE exit_xa");
                // Reset is reusable, and repeated cleanup must be harmless.
                engine.reset_session(&mut s).unwrap();
                expect_ok(&engine, &mut s, "XA START 'exit_owned'");
                expect_ok(&engine, &mut s, "XA END 'exit_owned'");
                expect_ok(&engine, &mut s, "XA ROLLBACK 'exit_owned'");
            }
        }
    }
}

#[test]
fn session_exit_preserves_detached_prepared_xa_branch() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    for reset in [false, true] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_strict_test_engine(dir.path());
        let mut s = fresh_root_session();
        s.connection_id = 7201;
        expect_ok(
            &engine,
            &mut s,
            "CREATE TABLE exit_prepared(id INT PRIMARY KEY)",
        );
        expect_ok(&engine, &mut s, "XA START 'exit_prepared'");
        expect_ok(&engine, &mut s, "INSERT INTO exit_prepared VALUES (1)");
        let txn = s.xa_branch.as_ref().unwrap().txn_id;
        expect_ok(&engine, &mut s, "XA END 'exit_prepared'");
        expect_ok(&engine, &mut s, "XA PREPARE 'exit_prepared'");
        if reset {
            engine.reset_session(&mut s).unwrap();
        } else {
            engine.close_session(&mut s).unwrap();
        }
        assert!(engine.database().is_txn_active(txn));
        let mut other = fresh_root_session();
        other.connection_id = 7202;
        assert_eq!(row_count(&engine, &mut other, "XA RECOVER"), 1);
        assert_eq!(
            row_count(&engine, &mut other, "SELECT * FROM exit_prepared"),
            0
        );
        expect_ok(&engine, &mut other, "XA COMMIT 'exit_prepared'");
        assert_eq!(
            row_count(&engine, &mut other, "SELECT * FROM exit_prepared"),
            1
        );
        expect_ok(&engine, &mut other, "DROP TABLE exit_prepared");
    }
}

#[test]
fn audit_idle_xa_rejects_queries() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    expect_ok(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
    expect_ok(&engine, &mut s, "XA START 'idle'");
    expect_ok(&engine, &mut s, "INSERT INTO t VALUES (1)");
    expect_ok(&engine, &mut s, "XA END 'idle'");
    for sql in ["INSERT INTO t VALUES (2)", "SELECT * FROM t", "SELECT 1"] {
        expect_err_code(&engine, &mut s, sql, 1399);
        assert!(s.current_txn.is_none());
    }
    expect_ok(&engine, &mut s, "XA COMMIT 'idle' ONE PHASE");
    assert_eq!(fetch_int_rows(&engine, &mut s, "SELECT id FROM t"), vec![1]);
}

#[test]
fn audit_xa_duplicate_active_and_foreign_completion() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    let mut other = fresh_root_session();
    expect_err_code(&engine, &mut other, "XA PREPARE 'missing'", 1399);
    expect_ok(&engine, &mut s, "XA START 'dup_active'");
    expect_err_code(&engine, &mut other, "XA START 'dup_active'", 1440);
    expect_ok(&engine, &mut other, "XA START 'other'");
    expect_ok(&engine, &mut other, "XA END 'other'");
    expect_ok(&engine, &mut other, "XA PREPARE 'other'");
    for sql in ["XA COMMIT 'other'", "XA ROLLBACK 'other'"] {
        expect_err_code(&engine, &mut s, sql, 1399);
    }
    expect_ok(&engine, &mut s, "XA ROLLBACK 'dup_active'");
    expect_ok(&engine, &mut s, "XA ROLLBACK 'other'");
}

#[test]
fn audit_empty_prepared_checkpoint_recovery() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        let mut s = fresh_root_session();
        expect_ok(&engine, &mut s, "XA START 'empty'");
        expect_ok(&engine, &mut s, "XA END 'empty'");
        expect_ok(&engine, &mut s, "XA PREPARE 'empty'");
        engine.database().checkpoint().unwrap();
    }
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    assert_eq!(row_count(&engine, &mut s, "XA RECOVER"), 1);
    expect_ok(&engine, &mut s, "XA COMMIT 'empty'");
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(row_count(&engine, &mut s, "XA RECOVER"), 0);
}

#[test]
fn audit_completed_prepared_recovery_releases_locks() {
    for completion in ["COMMIT", "ROLLBACK"] {
        let dir = tempfile::tempdir().unwrap();
        {
            let engine = open_strict_test_engine(dir.path());
            let mut s = fresh_root_session();
            expect_ok(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY, v INT)");
            expect_ok(&engine, &mut s, "INSERT INTO t VALUES (1,0)");
            expect_ok(&engine, &mut s, "XA START 'completed'");
            expect_ok(&engine, &mut s, "UPDATE t SET v=1 WHERE id=1");
            expect_ok(&engine, &mut s, "XA END 'completed'");
            expect_ok(&engine, &mut s, "XA PREPARE 'completed'");
            expect_ok(&engine, &mut s, &format!("XA {completion} 'completed'"));
        }
        let engine = open_strict_test_engine(dir.path());
        let mut s = fresh_root_session();
        assert_eq!(row_count(&engine, &mut s, "XA RECOVER"), 0);
        expect_ok(&engine, &mut s, "SET innodb_lock_wait_timeout=1");
        expect_ok(&engine, &mut s, "UPDATE t SET v=2 WHERE id=1");
        assert_eq!(fetch_int_rows(&engine, &mut s, "SELECT v FROM t"), vec![2]);
        expect_ok(&engine, &mut s, "DROP TABLE t");
    }
}

#[test]
fn audit_rr_autocommit_preserves_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    let mut other = fresh_root_session();
    expect_ok(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
    expect_ok(&engine, &mut s, "INSERT INTO t VALUES (1)");
    expect_ok(
        &engine,
        &mut s,
        "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
    );
    expect_ok(&engine, &mut s, "BEGIN");
    assert_eq!(row_count(&engine, &mut s, "SELECT * FROM t"), 1);
    expect_ok(&engine, &mut other, "INSERT INTO t VALUES (2)");
    expect_ok(&engine, &mut s, "SET autocommit=0");
    assert_eq!(row_count(&engine, &mut s, "SELECT * FROM t"), 1);
    expect_ok(&engine, &mut s, "ROLLBACK");
}

#[test]
fn audit_savepoints_and_implicit_read_only() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    expect_ok(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
    expect_ok(&engine, &mut s, "SAVEPOINT sp");
    assert!(s.current_txn.is_none());
    expect_err_code(&engine, &mut s, "ROLLBACK TO sp", 1305);
    expect_ok(&engine, &mut s, "BEGIN");
    expect_ok(&engine, &mut s, "SAVEPOINT Abc");
    expect_ok(&engine, &mut s, "INSERT INTO t VALUES (1)");
    expect_ok(&engine, &mut s, "ROLLBACK TO ABC");
    expect_ok(&engine, &mut s, "RELEASE SAVEPOINT aBc");
    expect_ok(&engine, &mut s, "SAVEPOINT `ÄBc`");
    expect_ok(&engine, &mut s, "ROLLBACK TO `äBC`");
    expect_ok(&engine, &mut s, "RELEASE SAVEPOINT `Äbc`");
    expect_ok(&engine, &mut s, "ROLLBACK");
    expect_ok(&engine, &mut s, "SET TRANSACTION READ ONLY");
    expect_ok(&engine, &mut s, "SET autocommit=0");
    expect_err_code(&engine, &mut s, "INSERT INTO t VALUES (2)", 1792);
    expect_ok(&engine, &mut s, "ROLLBACK");
}

#[test]
fn audit_autocommit_read_consumes_next_access_mode() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    expect_ok(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
    expect_ok(&engine, &mut s, "SET TRANSACTION READ ONLY");
    assert_eq!(row_count(&engine, &mut s, "SELECT * FROM t"), 0);
    // The SELECT completed the one transaction covered by SET TRANSACTION.
    expect_ok(&engine, &mut s, "INSERT INTO t VALUES (1)");

    let sql = "SELECT id FROM t WHERE id=?";
    let parsed = engine.prepare_cached(&mut s, sql, 1).unwrap();
    let mut memo = crate::engine::api::PreparedSelectMemoState::default();
    for _ in 0..2 {
        expect_ok(&engine, &mut s, "SET TRANSACTION READ ONLY");
        engine
            .execute_prepared_select(&mut s, &mut memo, &parsed, sql, &[Value::Int(1)])
            .unwrap()
            .unwrap();
        expect_ok(&engine, &mut s, "UPDATE t SET id=1 WHERE id=1");
    }
    assert!(!s.read_only_txn);
    assert!(s.next_txn_read_only.is_none());
}

#[test]
fn audit_autocommit_locking_read_restores_session_access_mode() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    expect_ok(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
    expect_ok(&engine, &mut s, "INSERT INTO t VALUES (1)");
    expect_ok(&engine, &mut s, "SET TRANSACTION READ ONLY");
    assert_eq!(row_count(&engine, &mut s, "SELECT * FROM t FOR SHARE"), 1);
    assert!(s.current_txn.is_none());
    assert!(s.active_txn_isolation.is_none());
    expect_ok(&engine, &mut s, "INSERT INTO t VALUES (2)");
    assert!(!s.read_only_txn);

    let mut other = fresh_root_session();
    expect_ok(&engine, &mut other, "BEGIN");
    expect_ok(&engine, &mut other, "UPDATE t SET id=1 WHERE id=1");
    expect_ok(&engine, &mut s, "SET TRANSACTION READ ONLY");
    expect_err_code(
        &engine,
        &mut s,
        "SELECT * FROM t WHERE id=1 FOR SHARE NOWAIT",
        3572,
    );
    expect_ok(&engine, &mut s, "INSERT INTO t VALUES (3)");
    expect_ok(&engine, &mut other, "ROLLBACK");

    expect_ok(&engine, &mut s, "SET SESSION TRANSACTION READ ONLY");
    assert_eq!(row_count(&engine, &mut s, "SELECT * FROM t FOR SHARE"), 3);
    expect_err_code(&engine, &mut s, "INSERT INTO t VALUES (4)", 1792);
    expect_err_code(&engine, &mut s, "SELECT * FROM t FOR UPDATE", 1792);
}

#[test]
fn audit_autocommit_write_overrides_session_access_mode_once() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    expect_ok(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
    expect_ok(&engine, &mut s, "SET SESSION TRANSACTION READ ONLY");
    expect_ok(&engine, &mut s, "SET TRANSACTION READ WRITE");
    expect_ok(&engine, &mut s, "INSERT INTO t VALUES (1)");
    expect_err_code(&engine, &mut s, "INSERT INTO t VALUES (2)", 1792);
    assert!(s.read_only_txn);
    assert!(s.next_txn_read_only.is_none());
    assert!(s.active_txn_isolation.is_none());
    assert_eq!(fetch_int_rows(&engine, &mut s, "SELECT id FROM t"), vec![1]);
}

#[test]
fn audit_account_ddl_commits_dml() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    expect_ok(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
    expect_ok(&engine, &mut s, "BEGIN");
    expect_ok(&engine, &mut s, "INSERT INTO t VALUES (1)");
    expect_ok(
        &engine,
        &mut s,
        "CREATE USER 'audit_user'@'%' IDENTIFIED BY 'abc'",
    );
    expect_ok(&engine, &mut s, "ROLLBACK");
    assert_eq!(row_count(&engine, &mut s, "SELECT * FROM t"), 1);
}

#[test]
fn audit_xa_concurrent_completion_has_one_winner() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(open_strict_test_engine(dir.path()));
    let mut s = fresh_root_session();
    expect_ok(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
    for id in 0..100 {
        let xid = format!("race_{id}");
        expect_ok(&engine, &mut s, &format!("XA START '{xid}'"));
        expect_ok(&engine, &mut s, &format!("INSERT INTO t VALUES ({id})"));
        expect_ok(&engine, &mut s, &format!("XA END '{xid}'"));
        expect_ok(&engine, &mut s, &format!("XA PREPARE '{xid}'"));
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let results = std::thread::scope(|scope| {
            let commit = scope.spawn(|| {
                barrier.wait();
                engine
                    .execute_with_session(&mut fresh_root_session(), &format!("XA COMMIT '{xid}'"))
            });
            let rollback = scope.spawn(|| {
                barrier.wait();
                engine.execute_with_session(
                    &mut fresh_root_session(),
                    &format!("XA ROLLBACK '{xid}'"),
                )
            });
            (commit.join().unwrap(), rollback.join().unwrap())
        });
        assert_ne!(
            results.0.is_ok(),
            results.1.is_ok(),
            "exactly one completion must succeed: {results:?}"
        );
        let committed = results.0.is_ok();
        let err = results.0.err().or_else(|| results.1.err()).unwrap();
        assert_eq!(err.code(), 1397);
        assert_eq!(
            row_count(&engine, &mut s, &format!("SELECT * FROM t WHERE id={id}")),
            usize::from(committed)
        );
    }
}

#[test]
fn audit_read_only_prepared_commit_does_not_resurrect() {
    let dir = tempfile::tempdir().unwrap();
    let xid = crate::xa::Xid::new(1, b"read_only".to_vec(), vec![]).unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        let db = engine.database();
        let txn = db.begin_read_only_txn().unwrap();
        db.prepare_xa(txn.txn_id, 0, xid.clone()).unwrap();
        db.commit_prepared(&xid).unwrap();
    }
    let engine = open_strict_test_engine(dir.path());
    assert!(engine.database().list_prepared_xa().is_empty());
}

#[test]
fn audit_prepared_select_respects_idle_xa() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    expect_ok(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
    expect_ok(&engine, &mut s, "INSERT INTO t VALUES (1)");
    let sql = "SELECT id FROM t WHERE id=?";
    let parsed = engine.prepare_cached(&mut s, sql, 1).unwrap();
    let mut memo = crate::engine::api::PreparedSelectMemoState::default();
    engine
        .execute_prepared_select(&mut s, &mut memo, &parsed, sql, &[Value::Int(1)])
        .unwrap()
        .unwrap();
    expect_ok(&engine, &mut s, "XA START 'prepared_read'");
    expect_ok(&engine, &mut s, "XA END 'prepared_read'");
    let constant = engine.parse_cached_statement(&s, "SELECT 1").unwrap();
    let err = engine
        .execute_parsed(&mut s, "SELECT 1", constant)
        .unwrap_err();
    assert_eq!(err.code(), 1399);
    assert!(s.current_txn.is_none());
    let err = engine
        .execute_prepared_select(&mut s, &mut memo, &parsed, sql, &[Value::Int(1)])
        .unwrap()
        .unwrap_err();
    assert_eq!(err.code(), 1399);
    expect_ok(&engine, &mut s, "XA ROLLBACK 'prepared_read'");
    expect_ok(&engine, &mut s, "SET TRANSACTION READ ONLY");
    expect_ok(&engine, &mut s, "SET autocommit=0");
    assert_eq!(row_count(&engine, &mut s, "SELECT * FROM t"), 1);
    expect_err_code(&engine, &mut s, "DELETE FROM t", 1792);
    expect_ok(&engine, &mut s, "ROLLBACK");
    expect_ok(&engine, &mut s, "INSERT INTO t VALUES (2)");
    expect_ok(&engine, &mut s, "ROLLBACK");
}

#[test]
fn txn_audit_xa_savepoints_and_mdl_transfer() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    expect_ok(&engine, &mut s, "CREATE TABLE audit_sp(id INT PRIMARY KEY)");
    expect_ok(&engine, &mut s, "XA START 'audit_sp'");
    expect_ok(&engine, &mut s, "INSERT INTO audit_sp VALUES (1)");
    expect_ok(&engine, &mut s, "SAVEPOINT a");
    expect_ok(&engine, &mut s, "INSERT INTO audit_sp VALUES (2)");
    expect_ok(&engine, &mut s, "ROLLBACK TO a");
    expect_ok(&engine, &mut s, "RELEASE SAVEPOINT a");
    expect_ok(&engine, &mut s, "XA END 'audit_sp'");
    for sql in ["SAVEPOINT b", "ROLLBACK TO a", "RELEASE SAVEPOINT a"] {
        expect_err_code(&engine, &mut s, sql, 1399);
    }
    expect_ok(&engine, &mut s, "XA PREPARE 'audit_sp'");
    expect_ok(&engine, &mut s, "XA COMMIT 'audit_sp'");
    assert_eq!(
        fetch_int_rows(&engine, &mut s, "SELECT id FROM audit_sp"),
        vec![1]
    );
}

#[test]
fn txn_audit_prepare_clears_mdl_memo_on_id_collision() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    expect_ok(
        &engine,
        &mut s,
        "CREATE TABLE audit_mdl(id INT PRIMARY KEY)",
    );
    expect_ok(&engine, &mut s, "SET autocommit=0");
    expect_ok(&engine, &mut s, "XA START 'audit_mdl'");
    s.mdl_txn_generation = s.current_txn.unwrap();
    expect_ok(&engine, &mut s, "INSERT INTO audit_mdl VALUES (1)");
    assert!(!s.mdl_txn_grants.borrow().1.is_empty());
    expect_ok(&engine, &mut s, "XA END 'audit_mdl'");
    expect_ok(&engine, &mut s, "XA PREPARE 'audit_mdl'");
    assert!(s.mdl_txn_grants.borrow().1.is_empty());
    expect_ok(&engine, &mut s, "SELECT * FROM audit_mdl");
    let mut other = fresh_root_session();
    other.connection_id = s.connection_id + 1;
    expect_ok(&engine, &mut other, "XA ROLLBACK 'audit_mdl'");
    expect_ok(&engine, &mut other, "SET lock_wait_timeout=1");
    expect_err_code(&engine, &mut other, "DROP TABLE audit_mdl", 1205);
    expect_ok(&engine, &mut s, "COMMIT");
    expect_ok(&engine, &mut other, "DROP TABLE audit_mdl");
}

#[test]
fn txn_audit_duplicate_characteristics_rejected() {
    for sql in [
        "SET TRANSACTION READ ONLY, READ WRITE",
        "SET TRANSACTION ISOLATION LEVEL READ COMMITTED, ISOLATION LEVEL SERIALIZABLE, READ ONLY",
        "SET SESSION TRANSACTION READ ONLY, READ ONLY",
    ] {
        assert!(crate::parser::parse_statement(sql).is_err(), "{sql}");
    }
}

#[test]
fn txn_audit_flush_privileges_commits() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    expect_ok(
        &engine,
        &mut s,
        "CREATE TABLE audit_flush(id INT PRIMARY KEY)",
    );
    expect_ok(&engine, &mut s, "BEGIN");
    expect_ok(&engine, &mut s, "INSERT INTO audit_flush VALUES (1)");
    expect_ok(&engine, &mut s, "FLUSH PRIVILEGES");
    expect_ok(&engine, &mut s, "ROLLBACK");
    assert_eq!(
        fetch_int_rows(&engine, &mut s, "SELECT id FROM audit_flush"),
        vec![1]
    );
}

#[test]
fn txn_audit_invalid_xid_keeps_xa_error_family() {
    for verb in ["START", "END", "PREPARE", "COMMIT", "ROLLBACK"] {
        for xid in [
            "''".to_string(),
            format!("'{}'", "a".repeat(65)),
            format!("'a','{}'", "b".repeat(65)),
        ] {
            let err = crate::parser::parse_statement(&format!("XA {verb} {xid}")).unwrap_err();
            assert_eq!(err.code(), 1398);
        }
        assert!(crate::parser::parse_statement(&format!(
            "XA {verb} '{}','{}'",
            "a".repeat(64),
            "b".repeat(64)
        ))
        .is_ok());
    }
}

#[test]
fn txn_audit_mixed_set_does_not_leave_autocommit_read_open() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut s = fresh_root_session();
    expect_ok(&engine, &mut s, "SET @a=(SELECT 1), autocommit=0");
    assert!(!s.in_transaction());
    assert!(s.current_txn.is_none());
    assert!(!s.autocommit);
    expect_ok(&engine, &mut s, "SET autocommit=1");
    expect_err_code(
        &engine,
        &mut s,
        "SET @b=(SELECT 1), autocommit=0, transaction_isolation='invalid'",
        1231,
    );
    assert!(s.autocommit);
    assert!(!s.in_transaction());
    expect_ok(
        &engine,
        &mut s,
        "CREATE TABLE set_lifetime(id INT PRIMARY KEY)",
    );
    expect_ok(&engine, &mut s, "BEGIN");
    expect_ok(&engine, &mut s, "INSERT INTO set_lifetime VALUES (1)");
    let txn = s.current_txn;
    expect_ok(
        &engine,
        &mut s,
        "SET @c=(SELECT id FROM set_lifetime), autocommit=0",
    );
    assert_eq!(s.current_txn, txn);
    expect_ok(&engine, &mut s, "ROLLBACK");
    assert!(fetch_int_rows(&engine, &mut s, "SELECT id FROM set_lifetime").is_empty());
}
