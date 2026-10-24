//! Regressions from the 2026-10-23 release audit: crash recovery of LOB
//! allocations and fragmented heap pages, unit keywords of TIMESTAMPDIFF-style
//! functions under aggregation, labels of unaliased projection items, and
//! exact-unique write paths for `IS NULL` keys.

use super::*;

fn crash_dir() -> tempfile::TempDir {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    tempfile::tempdir_in("/tmp/mydb").unwrap()
}

/// Strict engine with the server's default heap update reserve, so heap page
/// layouts match a default `mydb` server.
fn open_default_reserve_engine(dir: &std::path::Path) -> StorageQueryEngine {
    StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir,
        StorageConfig {
            durability: crate::storage::DurabilityMode::Strict,
            checkpoint_interval: Duration::from_millis(60_000),
            checkpoint_wal_bytes: 64 * 1024 * 1024,
            lock_wait: Duration::from_millis(1_000),
            group_commit_window: Duration::from_micros(0),
            wal_segment_bytes: 64 * 1024 * 1024,
            buffer_pool_pages: 2_048,
            buffer_pool_io_threads: 1,
            buffer_pool_prefetch_pages: 2,
            undo_retention: Duration::from_millis(60_000),
            heap_update_reserve_bytes: crate::storage::PAGE_SIZE / 16,
            startup_validation: crate::storage::StartupValidationMode::Auto,
            startup_validate_indexes: true,
        },
    )
    .unwrap()
}

fn rows(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> Vec<Vec<Value>> {
    match engine
        .execute_with_session(session, sql)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
    {
        QueryResponse::ResultSet(result) => result.rows,
        other => panic!("expected rows: {other:?}"),
    }
}

fn exec(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) {
    engine
        .execute_with_session(session, sql)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
}

/// `CHECK TABLE` status plus every row's `(id, length, intact)` where intact
/// means the value is still one character repeated `length` times.
fn lob_table_state(engine: &StorageQueryEngine) -> (Vec<Vec<Value>>, Vec<Vec<Value>>) {
    let mut session = SessionContext::default();
    let check = rows(engine, &mut session, "CHECK TABLE c");
    let body = rows(
        engine,
        &mut session,
        "SELECT id, LENGTH(t), t = REPEAT(LEFT(t, 1), LENGTH(t)) FROM c ORDER BY id",
    );
    (check, body)
}

fn assert_check_ok(check: &[Vec<Value>]) {
    assert!(
        check
            .iter()
            .all(|row| row[2] == Value::String("status".into())
                && row[3] == Value::String("OK".into())),
        "{check:?}"
    );
}

const CRASH_DIR_ENV: &str = "MYDB_RELEASE_AUDIT_1023_CRASH_DIR";
const CRASH_SCENARIO_ENV: &str = "MYDB_RELEASE_AUDIT_1023_CRASH_SCENARIO";

const SEED_LOB_TABLE: [&str; 2] = [
    "CREATE TABLE c (id INT PRIMARY KEY, t LONGTEXT)",
    "INSERT INTO c VALUES (1, REPEAT('a', 70000)), (2, REPEAT('b', 70000)), \
     (3, REPEAT('c', 70000)), (4, REPEAT('d', 70000))",
];

/// Statements executed before the simulated crash, as `(session, sql)`, and
/// whether the scenario needs the server's default heap update reserve.
fn crash_scenario(name: &str) -> (bool, Vec<(usize, &'static str)>) {
    let seeded = |rest: &[&'static str]| {
        SEED_LOB_TABLE
            .iter()
            .chain(rest)
            .map(|sql| (0, *sql))
            .collect::<Vec<_>>()
    };
    match name {
        "lob-rollback-update" => (
            false,
            seeded(&[
                "BEGIN",
                "UPDATE c SET t = REPEAT('x', 90000) WHERE id = 1",
                "ROLLBACK",
            ]),
        ),
        "lob-rollback-insert" => (
            false,
            seeded(&[
                "BEGIN",
                "INSERT INTO c VALUES (9, REPEAT('x', 90000))",
                "ROLLBACK",
            ]),
        ),
        "lob-rollback-regrow" => (
            false,
            seeded(&[
                "UPDATE c SET t = 's' WHERE id = 1",
                "BEGIN",
                "UPDATE c SET t = REPEAT('x', 90000) WHERE id = 1",
                "ROLLBACK",
            ]),
        ),
        "lob-loser" => {
            let mut statements = seeded(&[]);
            statements.extend([
                (1, "BEGIN"),
                (1, "INSERT INTO c VALUES (10, REPEAT('x', 50000))"),
                // Committed allocations made after the loser's.
                (2, "INSERT INTO c VALUES (11, REPEAT('y', 50000))"),
                (2, "INSERT INTO c VALUES (12, REPEAT('z', 90000))"),
            ]);
            (false, statements)
        }
        "heap-fragment" => (
            true,
            [
                "CREATE TABLE h (id INT PRIMARY KEY, t VARCHAR(4000))",
                "INSERT INTO h VALUES (1, REPEAT('a', 3000)), (2, REPEAT('b', 3000)), \
                 (3, REPEAT('c', 3000)), (4, REPEAT('d', 3000)), (5, REPEAT('e', 3000))",
                // Committed in-place shrink leaves a 2000-byte fragment.
                "UPDATE h SET t = REPEAT('B', 1000) WHERE id = 2",
                // The rollback regrows row 3, compacting the page at runtime;
                // recovery skips the aborted shrink and never sees that regrowth.
                "BEGIN",
                "UPDATE h SET t = 'x' WHERE id = 3",
                "ROLLBACK",
                // Fits only into the space the runtime compaction reclaimed.
                "INSERT INTO h VALUES (6, REPEAT('f', 1800))",
            ]
            .map(|sql| (0, sql))
            .to_vec(),
        ),
        "heap-migration" => (
            true,
            [
                "CREATE TABLE h (id INT PRIMARY KEY, t VARCHAR(4000))",
                "INSERT INTO h VALUES (1, REPEAT('a', 2500)), (2, REPEAT('b', 2500)), \
                 (3, REPEAT('c', 2500)), (4, REPEAT('d', 2500)), (5, REPEAT('e', 2500)), \
                 (6, REPEAT('f', 2500))",
                // The growth migrates row 1 to another page behind a forwarding
                // stub; the rollback restores the old image at the new location.
                "BEGIN",
                "UPDATE h SET t = REPEAT('x', 3990) WHERE id = 1",
                "ROLLBACK",
                "UPDATE h SET t = 'committed' WHERE id = 1",
                "INSERT INTO h VALUES (7, 'g')",
                "DELETE FROM h WHERE id = 7",
            ]
            .map(|sql| (0, sql))
            .to_vec(),
        ),
        other => panic!("unknown crash scenario {other}"),
    }
}

fn open_scenario_engine(default_reserve: bool, dir: &std::path::Path) -> StorageQueryEngine {
    if default_reserve {
        open_default_reserve_engine(dir)
    } else {
        open_strict_test_engine(dir)
    }
}

/// Child half of the crash tests: run the scenario and exit without
/// destructors, checkpoint or buffer-pool writeback. A forgotten in-process
/// engine would keep its IO thread alive and could still write pages after
/// the parent's recovery.
#[test]
fn release_audit_1023_crash_child() {
    let Some(dir) = std::env::var_os(CRASH_DIR_ENV) else {
        return;
    };
    let scenario = std::env::var(CRASH_SCENARIO_ENV).unwrap();
    let (default_reserve, statements) = crash_scenario(&scenario);
    let engine = open_scenario_engine(default_reserve, std::path::Path::new(&dir));
    let mut sessions = [(); 3].map(|_| SessionContext::default());
    for (session, sql) in statements {
        exec(&engine, &mut sessions[session], sql);
    }
    std::process::exit(0);
}

/// Run `scenario` in a child process that exits abruptly, then reopen.
fn recover_after_crash(scenario: &str) -> (tempfile::TempDir, StorageQueryEngine) {
    let dir = crash_dir();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "engine::executor::tests::release_audit_1023::release_audit_1023_crash_child",
            "--nocapture",
        ])
        .env(CRASH_DIR_ENV, dir.path())
        .env(CRASH_SCENARIO_ENV, scenario)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{scenario} child failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let (default_reserve, _) = crash_scenario(scenario);
    let engine = open_scenario_engine(default_reserve, dir.path());
    (dir, engine)
}

#[test]
fn release_audit_1023_rolled_back_lob_allocations_survive_crash() {
    for scenario in [
        "lob-rollback-update",
        "lob-rollback-insert",
        "lob-rollback-regrow",
    ] {
        let (_dir, engine) = recover_after_crash(scenario);
        let (check, body) = lob_table_state(&engine);
        assert_check_ok(&check);
        assert_eq!(body.len(), 4, "{scenario}: {body:?}");
        assert!(
            body.iter().all(|row| row[2] == Value::Int(1)),
            "{scenario}: {body:?}"
        );
        // The allocator must be usable again after recovery.
        let mut session = SessionContext::default();
        exec(
            &engine,
            &mut session,
            "INSERT INTO c VALUES (20, REPEAT('p', 60000)), (21, REPEAT('q', 120000))",
        );
        let (check, body) = lob_table_state(&engine);
        assert_check_ok(&check);
        assert_eq!(body.len(), 6, "{scenario}: {body:?}");
        assert!(
            body.iter().all(|row| row[2] == Value::Int(1)),
            "{scenario}: {body:?}"
        );
    }
}

#[test]
fn release_audit_1023_lob_crash_loser_keeps_committed_allocations() {
    let (_dir, engine) = recover_after_crash("lob-loser");
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "INSERT INTO c VALUES (20, REPEAT('p', 60000)), (21, REPEAT('q', 120000))",
    );
    let (check, body) = lob_table_state(&engine);
    assert_check_ok(&check);
    let ids: Vec<Value> = body.iter().map(|row| row[0].clone()).collect();
    assert_eq!(
        ids,
        [1, 2, 3, 4, 11, 12, 20, 21].map(Value::Int).to_vec(),
        "{body:?}"
    );
    assert!(body.iter().all(|row| row[2] == Value::Int(1)), "{body:?}");
}

#[test]
fn release_audit_1023_redo_insert_compacts_fragmented_heap_page() {
    let (_dir, engine) = recover_after_crash("heap-fragment");
    let mut session = SessionContext::default();
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "SELECT id, LENGTH(t) FROM h ORDER BY id"
        ),
        [
            (1, 3000),
            (2, 1000),
            (3, 3000),
            (4, 3000),
            (5, 3000),
            (6, 1800)
        ]
        .map(|(id, len)| vec![Value::Int(id), Value::Int(len)])
        .to_vec()
    );
}

#[test]
fn release_audit_1023_rolled_back_row_migration_keeps_later_commits() {
    let (_dir, engine) = recover_after_crash("heap-migration");
    let mut session = SessionContext::default();
    let expected: Vec<Vec<Value>> = [
        (1, "committed", 9),
        (2, "bbbbbbbbb", 2500),
        (3, "ccccccccc", 2500),
        (4, "ddddddddd", 2500),
        (5, "eeeeeeeee", 2500),
        (6, "fffffffff", 2500),
    ]
    .map(|(id, prefix, len)| {
        vec![
            Value::Int(id),
            Value::String(prefix.into()),
            Value::Int(len),
        ]
    })
    .to_vec();
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "SELECT id, LEFT(t, 9), LENGTH(t) FROM h ORDER BY id"
        ),
        expected
    );
    assert_eq!(
        rows(&engine, &mut session, "SELECT t FROM h WHERE id = 1"),
        vec![vec![Value::String("committed".into())]]
    );
}

#[test]
fn release_audit_1023_unit_keyword_arguments_are_not_columns() {
    let dir = crash_dir();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE a (g INT, d DATE)");
    exec(
        &engine,
        &mut session,
        "INSERT INTO a VALUES (1, '2024-01-01'), (1, '2024-03-15'), (2, '2024-02-01')",
    );
    for sql_mode in ["ONLY_FULL_GROUP_BY", ""] {
        exec(
            &engine,
            &mut session,
            &format!("SET sql_mode = '{sql_mode}'"),
        );
        assert_eq!(
            rows(
                &engine,
                &mut session,
                "SELECT g, TIMESTAMPDIFF(MONTH, MIN(d), MAX(d)) FROM a GROUP BY g ORDER BY g"
            ),
            vec![
                vec![Value::Int(1), Value::Int(2)],
                vec![Value::Int(2), Value::Int(0)]
            ],
            "sql_mode={sql_mode}"
        );
        assert_eq!(
            rows(
                &engine,
                &mut session,
                "SELECT COUNT(*), TIMESTAMPDIFF(DAY, MIN(d), MAX(d)), \
                 CAST(TIMESTAMPADD(DAY, 1, MIN(d)) AS CHAR), GET_FORMAT(DATE, 'ISO') FROM a"
            ),
            vec![vec![
                Value::Int(3),
                Value::Int(74),
                Value::String("2024-01-02".into()),
                Value::String("%Y-%m-%d".into()),
            ]],
            "sql_mode={sql_mode}"
        );
    }
}

#[test]
fn release_audit_1023_unaliased_items_are_labelled_by_full_text() {
    let dir = crash_dir();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE l (id INT, t VARCHAR(20), f INT)",
    );
    let sql = "SELECT substring(t, 1, 2), SUBSTR(t,1,1), substring(t from 2 for 1), \
               ceil(id), FLOOR(id + 1), position('b' in t), EXISTS(SELECT 1), not f, \
               -(id), binary t, interval 1 day + '2024-01-01', t, (SELECT 1), \
               CAST(id AS CHAR), DISTINCT_COUNT FROM l, \
               (SELECT 1 AS DISTINCT_COUNT) d";
    let QueryResponse::ResultSet(result) = engine.execute_with_session(&mut session, sql).unwrap()
    else {
        panic!("expected rows");
    };
    let names: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "substring(t, 1, 2)",
            "SUBSTR(t,1,1)",
            "substring(t from 2 for 1)",
            "ceil(id)",
            "FLOOR(id + 1)",
            "position('b' in t)",
            "EXISTS(SELECT 1)",
            "not f",
            "-(id)",
            "binary t",
            "interval 1 day + '2024-01-01'",
            "t",
            "(SELECT 1)",
            "CAST(id AS CHAR)",
            "DISTINCT_COUNT",
        ]
    );
    // Distinct labels keep views and CTAS over such items valid.
    exec(
        &engine,
        &mut session,
        "CREATE VIEW v AS SELECT ceil(id), floor(id + 1), id FROM l",
    );
    exec(
        &engine,
        &mut session,
        "CREATE TABLE ctas AS SELECT substring(t, 1, 2), substr(t, 1, 1) FROM l",
    );
}

#[test]
fn release_audit_1023_unique_is_null_writes_touch_every_null_row() {
    let dir = crash_dir();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE u (id INT PRIMARY KEY, k VARCHAR(10) UNIQUE, n INT NOT NULL DEFAULT 0)",
    );
    exec(
        &engine,
        &mut session,
        "INSERT INTO u VALUES (1, 'a', 0), (2, NULL, 0), (3, NULL, 0), (4, NULL, 0)",
    );
    exec(
        &engine,
        &mut session,
        "UPDATE u SET n = n + 1 WHERE k IS NULL",
    );
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "SELECT id FROM u WHERE n = 1 ORDER BY id"
        ),
        vec![
            vec![Value::Int(2)],
            vec![Value::Int(3)],
            vec![Value::Int(4)]
        ]
    );
    exec(&engine, &mut session, "BEGIN");
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "SELECT id FROM u WHERE k IS NULL ORDER BY id FOR UPDATE"
        )
        .len(),
        3
    );
    exec(&engine, &mut session, "COMMIT");
    exec(&engine, &mut session, "DELETE FROM u WHERE k IS NULL");
    assert_eq!(
        rows(&engine, &mut session, "SELECT id FROM u"),
        vec![vec![Value::Int(1)]]
    );

    exec(
        &engine,
        &mut session,
        "CREATE TABLE m (a INT, b INT, c INT, PRIMARY KEY (a, b), UNIQUE KEY uk (b, c))",
    );
    exec(
        &engine,
        &mut session,
        "INSERT INTO m VALUES (1, 1, NULL), (2, 1, NULL), (3, 1, 5)",
    );
    exec(
        &engine,
        &mut session,
        "DELETE FROM m WHERE b = 1 AND c IS NULL",
    );
    assert_eq!(
        rows(&engine, &mut session, "SELECT a FROM m"),
        vec![vec![Value::Int(3)]]
    );
}
