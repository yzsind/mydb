use super::support::result_rows as rows;
use super::*;

/// M1.D1 — Every successful INSERT/UPDATE/DELETE bumps the matching
/// DML counter; SHOW ENGINE MYDB STATUS surfaces them via the
/// `storage_status` snapshot. The plain row-level numbers are the
/// simplest sanity check that the engine is wiring the new counters
/// through the standard paths and not, say, double-counting batches.
#[test]
fn dml_counters_track_insert_update_delete() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let baseline = engine.database().dml_stats_snapshot();

    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    engine
        .execute("UPDATE t SET v = v + 1 WHERE id <= 2")
        .unwrap();
    engine.execute("DELETE FROM t WHERE id = 3").unwrap();

    let after = engine.database().dml_stats_snapshot();
    assert_eq!(after.insert_rows - baseline.insert_rows, 3);
    assert_eq!(after.update_rows - baseline.update_rows, 2);
    assert_eq!(after.delete_rows - baseline.delete_rows, 1);
}

/// INSERT IGNORE repairs an explicit NULL in a NOT NULL column using the
/// implicit default, reports 1048, and counts the warning.
#[test]
fn insert_ignore_swallows_not_null_violation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(32) NOT NULL)",
        )
        .unwrap();

    let baseline_ignores = engine.database().dml_stats_snapshot().ignore_warnings;
    assert_eq!(
        engine
            .execute_with_session(
                &mut session,
                "INSERT IGNORE INTO t VALUES (1, 'a'), (2, NULL), (3, 'c')",
            )
            .unwrap(),
        QueryResponse::OkAffected { affected_rows: 3 }
    );
    assert_eq!(session.warning_count(), 1);
    let warnings = rows(
        engine
            .execute_with_session(&mut session, "SHOW WARNINGS")
            .unwrap(),
    );
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0][1], Value::Int(1048));
    let after = engine.database().dml_stats_snapshot();
    assert_eq!(after.ignore_warnings - baseline_ignores, 1);

    // The NULL row is retained with VARCHAR's implicit empty-string default.
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
        ]
    );
}

/// M1.A4 — INSERT IGNORE on a FK violation (MySQL 1452) swallows
/// the row and reports a warning, matching MySQL's documented
/// behaviour. Without this, IGNORE was strictly narrower than
/// upstream and migrations driving fixture loads failed loudly.
#[test]
fn insert_ignore_swallows_fk_violation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "CREATE TABLE parent (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE child (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES parent(id))",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO parent VALUES (1)")
        .unwrap();

    let baseline_ignores = engine.database().dml_stats_snapshot().ignore_warnings;
    // Row (10, 99) violates the FK because parent.id=99 does not exist;
    // row (11, 1) does not. Without IGNORE the whole statement aborts.
    assert_eq!(
        engine
            .execute_with_session(
                &mut session,
                "INSERT IGNORE INTO child VALUES (10, 99), (11, 1)",
            )
            .unwrap(),
        QueryResponse::OkAffected { affected_rows: 1 }
    );
    assert_eq!(session.warning_count(), 1);
    let warnings = rows(
        engine
            .execute_with_session(&mut session, "SHOW WARNINGS")
            .unwrap(),
    );
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0][1], Value::Int(1452));
    let after = engine.database().dml_stats_snapshot();
    assert_eq!(after.ignore_warnings - baseline_ignores, 1);

    let stored = rows(
        engine
            .execute_with_session(&mut session, "SELECT id, pid FROM child ORDER BY id")
            .unwrap(),
    );
    assert_eq!(stored, vec![vec![Value::Int(11), Value::Int(1)]]);
}

/// M1.A4 — IGNORE must NOT swallow errors outside the documented
/// data-error set. A missing table (1146) is a hard error in any
/// dialect; downgrading it to a warning would mask schema bugs.
#[test]
fn insert_ignore_does_not_swallow_unrelated_errors() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let result = engine.execute("INSERT IGNORE INTO does_not_exist VALUES (1)");
    let err = result.expect_err("missing table must error");
    assert_eq!(err.code(), 1146);
}

/// M1.A2 — Rolling back a transaction that inserted rows with
/// off-page LOB chains has to physically reclaim those chains and
/// surface the reclamation through `dml_lob_leaks_recovered`. The
/// counter is the SHOW ENGINE MYDB STATUS signal that the engine
/// successfully recovered a chain that, pre-A2, the recovery path
/// would silently leave behind.
#[test]
fn rollback_after_insert_with_lob_reclaims_chains() {
    // Force every row to externalize at least one LOB chain. The
    // inline threshold caps each variable column at 4096 bytes, so
    // a 5000-byte value goes off-page deterministically.
    let _env_lock = test_env_lock();
    with_test_env(&[("MYDB_LOB_INLINE_MAX", Some("4096"))], || {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        let mut session = SessionContext::default();
        engine
            .execute_with_session(
                &mut session,
                "CREATE TABLE blobs (id INT PRIMARY KEY, payload BLOB)",
            )
            .unwrap();
        let baseline_reclaimed = engine.database().dml_stats_snapshot().lob_leaks_recovered;

        engine.execute_with_session(&mut session, "BEGIN").unwrap();
        let big = "A".repeat(5000);
        engine
            .execute_with_session(
                &mut session,
                &format!(
                    "INSERT INTO blobs VALUES (1, '{0}'), (2, '{0}'), (3, '{0}')",
                    big
                ),
            )
            .unwrap();
        engine
            .execute_with_session(&mut session, "ROLLBACK")
            .unwrap();

        let after = engine.database().dml_stats_snapshot();
        // 3 inserted rows × ≥1 chain each — the exact number depends on
        // whether other variable columns also externalize, so we only
        // assert that the recovery path did fire for at least 3 chains.
        assert!(
            after.lob_leaks_recovered - baseline_reclaimed >= 3,
            "expected >=3 LOB chains reclaimed, saw {}",
            after.lob_leaks_recovered - baseline_reclaimed
        );

        // The rows themselves must be gone (rollback reverted INSERT).
        let stored = rows(
            engine
                .execute_with_session(&mut session, "SELECT COUNT(*) FROM blobs")
                .unwrap(),
        );
        assert_eq!(stored, vec![vec![Value::Int(0)]]);
    });
}
