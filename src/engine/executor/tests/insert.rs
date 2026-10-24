use super::*;

#[test]
fn storage_engine_executes_ddl_dml_and_reopens() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());

    engine
        .execute("CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR(64) NOT NULL, age INT, INDEX age_idx (age))")
        .unwrap();
    engine
        .execute("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 36), (2, 'Grace', 40)")
        .unwrap();
    engine
        .execute("UPDATE users SET age = 41 WHERE id = 2")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT name, age FROM users WHERE age >= 40 ORDER BY id LIMIT 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![Value::String("Grace".into()), Value::Int(41)]]
    );

    engine.execute("DELETE FROM users WHERE id = 1").unwrap();
    drop(engine);

    let reopened = open_strict_test_engine(dir.path());
    let QueryResponse::ResultSet(result) = reopened.execute("SELECT * FROM users").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(2));
}

#[test]
fn large_text_blob_types_round_trip_via_sql() {
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());

    // All MySQL large-field keywords must parse and create a table.
    engine
        .execute(
            "CREATE TABLE docs (\
                 id INT PRIMARY KEY, \
                 tiny TINYTEXT, \
                 body LONGTEXT, \
                 blob_col LONGBLOB, \
                 med MEDIUMTEXT)",
        )
        .unwrap();

    // A value over the inline threshold is externalized off-page.
    let big = "字A".repeat(4000); // multi-byte, > 4096 bytes
    engine
        .execute(&format!(
            "INSERT INTO docs (id, tiny, body, blob_col, med) \
             VALUES (1, 'short', '{big}', 'bin-data', 'm')"
        ))
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT body FROM docs WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::String(big.clone()));

    // UPDATE the externalized column to a new large value via SQL.
    let big2 = "Z".repeat(9000);
    engine
        .execute(&format!("UPDATE docs SET body = '{big2}' WHERE id = 1"))
        .unwrap();

    // Reopen: the overflow chain must survive recovery.
    drop(engine);
    let reopened = open_strict_test_engine(dir.path());
    let QueryResponse::ResultSet(result) = reopened
        .execute("SELECT body, tiny FROM docs WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::String(big2));
    assert_eq!(result.rows[0][1], Value::String("short".into()));

    reopened.execute("DELETE FROM docs WHERE id = 1").unwrap();
    let QueryResponse::ResultSet(result) = reopened.execute("SELECT COUNT(*) FROM docs").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(0));
}

#[test]
fn per_type_lob_byte_caps_enforced() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, tb TINYBLOB, b BLOB, mb MEDIUMBLOB, \
             lb LONGBLOB, tt TINYTEXT)",
        )
        .unwrap();

    // TINYBLOB caps at 255 bytes.
    engine
        .execute("INSERT INTO t (id, tb) VALUES (1, REPEAT('x', 255))")
        .unwrap();
    let err = engine
        .execute("INSERT INTO t (id, tb) VALUES (2, REPEAT('x', 256))")
        .unwrap_err();
    assert_eq!(err.code(), 1406, "TINYBLOB must reject > 255 bytes");

    // BLOB caps at 65535 bytes.
    engine
        .execute("INSERT INTO t (id, b) VALUES (3, REPEAT('y', 65535))")
        .unwrap();
    let err = engine
        .execute("INSERT INTO t (id, b) VALUES (4, REPEAT('y', 65536))")
        .unwrap_err();
    assert_eq!(err.code(), 1406, "BLOB must reject > 64KB");

    // MEDIUMBLOB and LONGBLOB accept a 200KB value that BLOB rejects —
    // proving each family applies its own larger cap.
    engine
        .execute("INSERT INTO t (id, mb) VALUES (5, REPEAT('z', 200000))")
        .unwrap();
    engine
        .execute("INSERT INTO t (id, lb) VALUES (6, REPEAT('z', 200000))")
        .unwrap();
    let err = engine
        .execute("INSERT INTO t (id, b) VALUES (7, REPEAT('z', 200000))")
        .unwrap_err();
    assert_eq!(err.code(), 1406, "plain BLOB must reject 200KB");

    // TINYTEXT cap is byte-based, like the BLOB family.
    let err = engine
        .execute("INSERT INTO t (id, tt) VALUES (8, REPEAT('w', 256))")
        .unwrap_err();
    assert_eq!(err.code(), 1406, "TINYTEXT must reject > 255 bytes");

    // The accepted large values round-trip.
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT LENGTH(lb) FROM t WHERE id = 6")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(200000));
}

#[test]
fn storage_engine_rejects_duplicate_primary_key() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v TEXT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    let err = engine.execute("INSERT INTO t VALUES (1, 'b')").unwrap_err();

    assert_eq!(err.code(), 1062);
    assert_eq!(err.sql_state(), "23000");
}

#[test]
fn generated_columns_materialize_on_insert_update_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE t (a INT, b INT, \
             total INT GENERATED ALWAYS AS (a + b) STORED, \
             label VARCHAR(32) AS (CONCAT(a, '-', b)) VIRTUAL, \
             INDEX total_idx (total))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t (a, b) VALUES (1, 2), (5, 7)")
        .unwrap();
    engine.execute("UPDATE t SET b = 4 WHERE a = 1").unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT a, b, total, label FROM t WHERE total >= 5 ORDER BY total")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![
                Value::Int(1),
                Value::Int(4),
                Value::Int(5),
                Value::String("1-4".to_string())
            ],
            vec![
                Value::Int(5),
                Value::Int(7),
                Value::Int(12),
                Value::String("5-7".to_string())
            ],
        ]
    );

    drop(engine);
    let reopened = open_strict_test_engine(dir.path());
    let QueryResponse::ResultSet(result) = reopened
        .execute("SELECT total, label FROM t ORDER BY total")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(5), Value::String("1-4".to_string())],
            vec![Value::Int(12), Value::String("5-7".to_string())],
        ]
    );
}

#[test]
fn generated_columns_accept_only_default_assignment() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (a INT, g INT AS (a + 1) VIRTUAL)")
        .unwrap();
    let err = engine
        .execute("INSERT INTO t (a, g) VALUES (1, 2)")
        .unwrap_err();
    assert_eq!(err.code(), 3105);

    engine.execute("INSERT INTO t VALUES (1, DEFAULT)").unwrap();
    let err = engine.execute("UPDATE t SET g = 9").unwrap_err();
    assert_eq!(err.code(), 3105);
    engine.execute("UPDATE t SET g = DEFAULT").unwrap();

    let QueryResponse::ResultSet(result) = engine.execute("SELECT a, g FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1), Value::Int(2)]]);
}

#[test]
fn odku_assignment_to_generated_column_is_rejected() {
    // gcol suite (gcol_ins_upd): an ON DUPLICATE KEY UPDATE assignment that
    // targets a generated column must raise ER_NON_DEFAULT_VALUE_FOR_
    // GENERATED_COLUMN (3105), exactly like the INSERT column list and plain
    // UPDATE. Both the FROM-less SELECT source and the VALUES source previously
    // dropped the assignment silently. The check is static: it fires whether or
    // not a row conflict actually occurs.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t2 (i1 INT, i2 INT AS (i1 * 2) STORED, UNIQUE (i1))")
        .unwrap();
    engine.execute("INSERT INTO t2 (i1) SELECT 5").unwrap();

    // FROM-less SELECT source (the reported repro) — rejected even though the
    // conflict on i1=5 exists.
    let err = engine
        .execute("INSERT INTO t2 (i1) SELECT 5 ON DUPLICATE KEY UPDATE i2 = 4")
        .unwrap_err();
    assert_eq!(err.code(), 3105);
    // VALUES source, no conflict (i1=9 is new) — still rejected at prepare time.
    let err = engine
        .execute("INSERT INTO t2 (i1) VALUES (9) ON DUPLICATE KEY UPDATE i2 = 4")
        .unwrap_err();
    assert_eq!(err.code(), 3105);
    // `= DEFAULT` is permitted (recomputes the generated value).
    engine
        .execute("INSERT INTO t2 (i1) VALUES (5) ON DUPLICATE KEY UPDATE i2 = DEFAULT")
        .unwrap();
    // Nothing was written by the rejected statements; the generated value holds.
    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT i1, i2 FROM t2 ORDER BY i1").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(5), Value::Int(10)]]);
}

#[test]
fn odku_on_fromless_select_updates_non_generated_column() {
    // Companion to the parser fix: the FROM-less SELECT + ODKU form must not
    // only parse but actually apply the update on a plain column.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t3 (i1 INT, v INT, UNIQUE (i1))")
        .unwrap();
    engine
        .execute("INSERT INTO t3 (i1, v) SELECT 5, 10")
        .unwrap();
    engine
        .execute("INSERT INTO t3 (i1, v) SELECT 5, 99 ON DUPLICATE KEY UPDATE v = v + 100")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine.execute("SELECT i1, v FROM t3").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(5), Value::Int(110)]]);
}

#[test]
fn sysbench_prepare_shape_stays_within_small_buffer_pool() {
    let dir = tempfile::tempdir().unwrap();
    // Buffer-pool eviction behaviour is independent of fsync mode; Relaxed
    // drops the per-INSERT fsync wall-time without changing the buffer-pool
    // accounting being asserted.
    let engine = StorageQueryEngine::open_with_storage_config(
        dir.path(),
        StorageConfig {
            durability: DurabilityMode::Relaxed,
            checkpoint_interval: Duration::from_millis(60_000),
            checkpoint_wal_bytes: 64 * 1024 * 1024,
            lock_wait: Duration::from_millis(500),
            group_commit_window: Duration::from_micros(0),
            wal_segment_bytes: 256 * 1024 * 1024,
            // Accommodate one 96-row statement's deferred-WAL working set
            // while remaining tiny enough that 1,152 rows continuously evict.
            buffer_pool_pages: 32,
            buffer_pool_io_threads: 1,
            buffer_pool_prefetch_pages: 4,
            undo_retention: Duration::from_millis(60_000),
            heap_update_reserve_bytes: 2048,
            startup_validation: crate::storage::StartupValidationMode::Auto,
            startup_validate_indexes: true,
        },
    )
    .unwrap();

    engine
        .execute(
            "CREATE TABLE sbtest1 (id INT PRIMARY KEY AUTO_INCREMENT, k INT NOT NULL, c CHAR(120) NOT NULL, pad CHAR(60) NOT NULL, INDEX k_1 (k))",
        )
        .unwrap();
    // Keep the fixture larger than the current compact heap encoding can fit
    // in 32 pages; otherwise this ceases to exercise eviction at all.
    for chunk in 0..16 {
        let values = (0..96)
            .map(|offset| {
                let id = chunk * 96 + offset + 1;
                format!(
                    "({}, '{}', '{}')",
                    500_000 + id,
                    "1234567890-".repeat(10),
                    "0987654321-".repeat(5),
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO sbtest1(k, c, pad) VALUES {values}"))
            .unwrap();
    }

    let status = engine.db.storage_status();
    assert!(
        status.buffer_pool_used_pages <= status.buffer_pool_pages,
        "{status:?}"
    );
    assert!(status.buffer_pool_evictions > 0, "{status:?}");

    let QueryResponse::ResultSet(count) = engine.execute("SELECT COUNT(*) FROM sbtest1").unwrap()
    else {
        panic!("expected count result set");
    };
    assert_eq!(count.rows[0][0], Value::Int(1536));
}

#[test]
fn sysbench_large_bulk_prepare_insert_stays_within_small_buffer_pool() {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageQueryEngine::open_with_storage_config(
        dir.path(),
        StorageConfig {
            durability: DurabilityMode::Relaxed,
            checkpoint_interval: Duration::from_millis(60_000),
            checkpoint_wal_bytes: 64 * 1024 * 1024,
            lock_wait: Duration::from_millis(500),
            group_commit_window: Duration::from_micros(0),
            wal_segment_bytes: 256 * 1024 * 1024,
            // Bumped from 3 to 8 pages: still forces eviction during a
            // 500KB+ bulk insert (the row width × count guarantees more than
            // 8 pages get touched) but spares the test ~30% of the thrash
            // wall-time without changing what's being asserted.
            buffer_pool_pages: 8,
            buffer_pool_io_threads: 1,
            buffer_pool_prefetch_pages: 4,
            undo_retention: Duration::from_millis(60_000),
            heap_update_reserve_bytes: 2048,
            startup_validation: crate::storage::StartupValidationMode::Auto,
            startup_validate_indexes: true,
        },
    )
    .unwrap();

    engine
        .execute(
            "CREATE TABLE sbtest2 (id INT PRIMARY KEY AUTO_INCREMENT, k INT NOT NULL, c CHAR(120) NOT NULL, pad CHAR(60) NOT NULL, INDEX k_2 (k))",
        )
        .unwrap();

    // 2900 rows keep the assembled SQL just above the 500 KiB threshold
    // the assertion requires (each row is ~182 bytes) while shaving ~5%
    // off the original 3072.
    let values = (0..2900)
        .map(|offset| {
            format!(
                "({}, '{}', '{}')",
                250_000 + offset,
                "1234567890-".repeat(10),
                "0987654321-".repeat(5),
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!("INSERT INTO sbtest2(k, c, pad) VALUES{values}");
    assert!(
        sql.len() > 500 * 1024,
        "large insert was only {}",
        sql.len()
    );

    engine.execute(&sql).unwrap();
    let status = engine.db.storage_status();
    assert!(
        status.buffer_pool_used_pages <= status.buffer_pool_pages,
        "{status:?}"
    );
    assert_eq!(status.buffer_pool_pinned_pages, 0, "{status:?}");
    assert!(status.buffer_pool_evictions > 0, "{status:?}");

    let QueryResponse::ResultSet(count) = engine.execute("SELECT COUNT(*) FROM sbtest2").unwrap()
    else {
        panic!("expected count result set");
    };
    assert_eq!(count.rows[0][0], Value::Int(2900));
}

#[test]
fn insert_fills_auto_increment_primary_key() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE sbtest1 (id INTEGER NOT NULL AUTO_INCREMENT, k INTEGER NOT NULL, PRIMARY KEY (id))")
        .unwrap();
    engine
        .execute("INSERT INTO sbtest1(k) VALUES (10), (11), (12)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id, k FROM sbtest1 ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(11)],
            vec![Value::Int(3), Value::Int(12)],
        ]
    );
}

#[test]
fn auto_increment_advances_after_explicit_values() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT, v INT, PRIMARY KEY (id))")
        .unwrap();
    engine
        .execute("INSERT INTO t(id, v) VALUES (7, 70), (3, 30)")
        .unwrap();
    engine.execute("INSERT INTO t(v) VALUES (80)").unwrap();

    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT id, v FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(3), Value::Int(30)],
            vec![Value::Int(7), Value::Int(70)],
            vec![Value::Int(8), Value::Int(80)],
        ]
    );
}

#[test]
fn auto_increment_treats_zero_like_mysql_default_mode() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT, v INT, PRIMARY KEY (id))")
        .unwrap();
    engine
        .execute("INSERT INTO t(id, v) VALUES (0, 10), (0, 11)")
        .unwrap();

    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT id, v FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(11)],
        ]
    );
}

#[test]
fn auto_increment_cache_does_not_reuse_deleted_max_value() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT, v INT, PRIMARY KEY (id))")
        .unwrap();
    engine
        .execute("INSERT INTO t(v) VALUES (10), (11), (12)")
        .unwrap();
    engine.execute("DELETE FROM t WHERE id = 3").unwrap();
    engine.execute("INSERT INTO t(v) VALUES (13)").unwrap();

    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT id, v FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(11)],
            vec![Value::Int(4), Value::Int(13)],
        ]
    );
}

#[test]
fn auto_increment_cache_advances_after_deleted_explicit_value() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT, v INT, PRIMARY KEY (id))")
        .unwrap();
    engine
        .execute("INSERT INTO t(id, v) VALUES (100, 10)")
        .unwrap();
    engine.execute("DELETE FROM t WHERE id = 100").unwrap();
    engine.execute("INSERT INTO t(v) VALUES (11)").unwrap();

    let QueryResponse::ResultSet(result) = engine.execute("SELECT id, v FROM t").unwrap() else {
        panic!("expected result set");
    };

    assert_eq!(result.rows, vec![vec![Value::Int(101), Value::Int(11)]]);
}

#[test]
fn auto_increment_cache_advances_after_update() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT, v INT, PRIMARY KEY (id))")
        .unwrap();
    engine.execute("INSERT INTO t(v) VALUES (10)").unwrap();
    engine.execute("UPDATE t SET id = 50 WHERE id = 1").unwrap();
    engine.execute("INSERT INTO t(v) VALUES (11)").unwrap();

    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT id, v FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(50), Value::Int(10)],
            vec![Value::Int(51), Value::Int(11)],
        ]
    );
}

#[test]
fn create_table_auto_increment_option_sets_first_value() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT, v INT, PRIMARY KEY (id)) AUTO_INCREMENT=100",
        )
        .unwrap();
    engine.execute("INSERT INTO t(v) VALUES (1), (2)").unwrap();

    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT id, v FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(100), Value::Int(1)],
            vec![Value::Int(101), Value::Int(2)],
        ]
    );
}

#[test]
fn alter_table_auto_increment_sets_next_value() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT, v INT, PRIMARY KEY (id))")
        .unwrap();
    engine.execute("INSERT INTO t(v) VALUES (1)").unwrap();
    engine.execute("ALTER TABLE t AUTO_INCREMENT = 50").unwrap();
    engine.execute("INSERT INTO t(v) VALUES (2)").unwrap();
    engine.execute("ALTER TABLE t AUTO_INCREMENT = 10").unwrap();
    engine.execute("INSERT INTO t(v) VALUES (3)").unwrap();

    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT id, v FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::Int(1)],
            vec![Value::Int(50), Value::Int(2)],
            vec![Value::Int(51), Value::Int(3)],
        ]
    );
}

#[test]
fn last_insert_id_returns_first_generated_value() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT, v INT, PRIMARY KEY (id))",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t(v) VALUES (10), (11), (12)")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT LAST_INSERT_ID()")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);

    engine
        .execute_with_session(&mut session, "INSERT INTO t(id, v) VALUES (100, 100)")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT LAST_INSERT_ID()")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn auto_increment_persists_across_restart_after_delete() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT, v INT, PRIMARY KEY (id))")
            .unwrap();
        engine
            .execute("INSERT INTO t(id, v) VALUES (100, 10)")
            .unwrap();
        engine.execute("DELETE FROM t WHERE id = 100").unwrap();
        fn contains_ai_file(path: &std::path::Path) -> bool {
            std::fs::read_dir(path)
                .ok()
                .into_iter()
                .flatten()
                .filter_map(Result::ok)
                .any(|entry| {
                    let path = entry.path();
                    if path.is_dir() {
                        contains_ai_file(&path)
                    } else {
                        path.extension().is_some_and(|ext| ext == "ai")
                    }
                })
        }
        assert!(
            !contains_ai_file(dir.path()),
            "AUTO_INCREMENT must not create .ai sidecars"
        );
    }

    let reopened = open_strict_test_engine(dir.path());
    reopened.execute("INSERT INTO t(v) VALUES (11)").unwrap();
    let QueryResponse::ResultSet(result) = reopened.execute("SELECT id, v FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(101), Value::Int(11)]]);
}

#[test]
fn auto_increment_batch_emits_one_compact_redo_and_no_sidecar() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id BIGINT AUTO_INCREMENT PRIMARY KEY, v INT)")
        .unwrap();
    let values = (0..100)
        .map(|value| format!("({value})"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO t(v) VALUES {values}"))
        .unwrap();

    let scan = crate::wal::LogManager::scan(dir.path()).unwrap();
    let advances = scan
        .records
        .iter()
        .filter(|record| {
            matches!(
                record.payload,
                crate::wal::WalPayload::AutoIncrementAdvance { .. }
            )
        })
        .count();
    assert_eq!(
        advances, 2,
        "CREATE and the 100-row INSERT each emit one compact redo"
    );

    fn contains_ai_file(path: &std::path::Path) -> bool {
        std::fs::read_dir(path)
            .ok()
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .any(|entry| {
                let path = entry.path();
                path.is_dir()
                    .then(|| contains_ai_file(&path))
                    .unwrap_or_else(|| path.extension().is_some_and(|extension| extension == "ai"))
            })
    }
    assert!(!contains_ai_file(dir.path()));
}

#[test]
fn concurrent_batch_inserts_allocate_distinct_auto_increment_ranges() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    engine
        .execute(
            "CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT, worker INT, v INT, PRIMARY KEY (id))",
        )
        .unwrap();

    let mut handles = Vec::new();
    for worker in 0..8 {
        let engine = std::sync::Arc::clone(&engine);
        handles.push(std::thread::spawn(move || {
            let mut session = SessionContext::default();
            for batch in 0..25 {
                let values = (0..20)
                    .map(|i| format!("({worker}, {})", batch * 20 + i))
                    .collect::<Vec<_>>()
                    .join(", ");
                engine
                    .execute_with_session(
                        &mut session,
                        &format!("INSERT INTO t(worker, v) VALUES {values}"),
                    )
                    .unwrap();
            }
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT COUNT(*), COUNT(DISTINCT id), MIN(id), MAX(id) FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![
            Value::Int(4000),
            Value::Int(4000),
            Value::Int(1),
            Value::Int(4000),
        ]]
    );
}

#[test]
fn auto_increment_requires_leftmost_key_and_preserves_catalog_on_failed_drop() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let err = engine
        .execute("CREATE TABLE bad (id INT NOT NULL AUTO_INCREMENT, v INT)")
        .unwrap_err();
    assert_eq!(err.code(), 1075);

    engine
        .execute("CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT, v INT, KEY id_idx (id))")
        .unwrap();
    let err = engine
        .execute("ALTER TABLE t DROP INDEX id_idx")
        .unwrap_err();
    assert_eq!(err.code(), 1075);

    engine.execute("INSERT INTO t(v) VALUES (10)").unwrap();
    let QueryResponse::ResultSet(result) = engine.execute("SELECT id, v FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1), Value::Int(10)]]);
}

#[test]
fn batched_insert_rejects_duplicate_primary_key_within_batch() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    let err = engine
        .execute("INSERT INTO t VALUES (1, 'a'), (1, 'b')")
        .unwrap_err();

    assert_eq!(err.code(), 1062);
    assert_eq!(err.sql_state(), "23000");
}

#[test]
fn multi_row_order_line_insert_preserves_per_row_key_values() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE bmsql_order_line (\
             ol_w_id INT NOT NULL, ol_d_id INT NOT NULL, ol_o_id INT NOT NULL, \
             ol_number INT NOT NULL, ol_i_id INT NOT NULL, ol_delivery_d TIMESTAMP, \
             ol_amount DECIMAL(6,2), ol_supply_w_id INT, ol_quantity INT, ol_dist_info CHAR(24), \
             PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO bmsql_order_line (\
             ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, \
             ol_quantity, ol_amount, ol_dist_info) VALUES \
             (3001, 1, 1, 1, 101, 1, 5, 12.34, 'dist-01'), \
             (3001, 1, 1, 2, 102, 1, 6, 23.45, 'dist-01')",
        )
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT ol_w_id, ol_d_id, ol_o_id, ol_number \
             FROM bmsql_order_line ORDER BY ol_number",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![
                Value::Int(1),
                Value::Int(1),
                Value::Int(3001),
                Value::Int(1)
            ],
            vec![
                Value::Int(1),
                Value::Int(1),
                Value::Int(3001),
                Value::Int(2)
            ],
        ]
    );
}

#[test]
fn unique_insert_waits_on_uncommitted_duplicate_key() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_lock_wait_ms(dir.path(), 5);
    let mut owner = short_lock_wait_session();
    let mut waiter = short_lock_wait_session();

    engine
        .execute_with_session(&mut owner, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO t VALUES (1, 'a')")
        .unwrap();
    engine.execute_with_session(&mut waiter, "BEGIN").unwrap();
    let blocked = engine
        .execute_with_session(&mut waiter, "INSERT INTO t VALUES (1, 'b')")
        .unwrap_err();

    assert_eq!(blocked.code(), 1205);

    engine.execute_with_session(&mut owner, "ROLLBACK").unwrap();
    engine
        .execute_with_session(&mut waiter, "INSERT INTO t VALUES (1, 'b')")
        .unwrap();
    engine.execute_with_session(&mut waiter, "COMMIT").unwrap();
    let QueryResponse::ResultSet(result) = engine.execute("SELECT id, v FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![Value::Int(1), Value::String("b".into())]]
    );
}

#[test]
fn defaults_primary_key_not_null_and_unique_index_build_are_enforced() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // `name` is VARCHAR (not TEXT): MySQL forbids a literal DEFAULT on
    // BLOB/TEXT columns (ER 1101, enforced by GAP-4), so this test — which
    // exercises DEFAULT application, not LOB defaults — uses VARCHAR.
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, v INT DEFAULT 7, name VARCHAR(64) DEFAULT 'anon')",
        )
        .unwrap();
    engine.execute("INSERT INTO t(id) VALUES (1)").unwrap();
    let err = engine
        .execute("INSERT INTO t(id) VALUES (NULL)")
        .unwrap_err();
    assert_eq!(err.code(), 1048);

    let QueryResponse::ResultSet(defaulted) = engine.execute("SELECT id, v, name FROM t").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        defaulted.rows,
        vec![vec![
            Value::Int(1),
            Value::Int(7),
            Value::String("anon".into())
        ]]
    );

    engine.execute("CREATE TABLE dupe (id INT, v INT)").unwrap();
    engine
        .execute("INSERT INTO dupe VALUES (1, 10), (2, 10)")
        .unwrap();
    let err = engine
        .execute("CREATE UNIQUE INDEX v_uniq ON dupe (v)")
        .unwrap_err();
    assert_eq!(err.code(), 1062);

    engine.execute("DELETE FROM dupe WHERE id = 2").unwrap();
    engine
        .execute("CREATE UNIQUE INDEX v_uniq ON dupe (v)")
        .unwrap();
}

#[test]
fn active_transaction_is_rolled_back_on_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        let mut session = SessionContext::default();
        engine
            .execute_with_session(&mut session, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
            .unwrap();
        engine.execute_with_session(&mut session, "BEGIN").unwrap();
        engine
            .execute_with_session(&mut session, "INSERT INTO t VALUES (1, 'a')")
            .unwrap();
    }

    let reopened = open_strict_test_engine(dir.path());
    let QueryResponse::ResultSet(result) = reopened.execute("SELECT * FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert!(result.rows.is_empty());
}

#[test]
fn tinyint_range_validation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v TINYINT)")
        .unwrap();

    engine.execute("INSERT INTO t VALUES (1, 127)").unwrap();
    engine.execute("INSERT INTO t VALUES (2, -128)").unwrap();
    assert!(engine.execute("INSERT INTO t VALUES (3, 128)").is_err());
    assert!(engine.execute("INSERT INTO t VALUES (4, -129)").is_err());
}

#[test]
fn smallint_range_validation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v SMALLINT)")
        .unwrap();

    engine.execute("INSERT INTO t VALUES (1, 32767)").unwrap();
    engine.execute("INSERT INTO t VALUES (2, -32768)").unwrap();
    assert!(engine.execute("INSERT INTO t VALUES (3, 32768)").is_err());
    assert!(engine.execute("INSERT INTO t VALUES (4, -32769)").is_err());
}

#[test]
fn int_range_validation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v INT)")
        .unwrap();

    engine
        .execute("INSERT INTO t VALUES (1, 2147483647)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (2, -2147483648)")
        .unwrap();
    assert!(engine
        .execute("INSERT INTO t VALUES (3, 2147483648)")
        .is_err());
    assert!(engine
        .execute("INSERT INTO t VALUES (4, -2147483649)")
        .is_err());
}

#[test]
fn decimal_precision_scale_validation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v DECIMAL(5,2))")
        .unwrap();

    engine.execute("INSERT INTO t VALUES (1, 123.45)").unwrap();
    engine.execute("INSERT INTO t VALUES (2, 0.01)").unwrap();
    assert!(engine.execute("INSERT INTO t VALUES (3, 1234.5)").is_err());

    let QueryResponse::ResultSet(result) = engine.execute("SELECT v FROM t WHERE id = 1").unwrap()
    else {
        panic!("expected result set");
    };
    // Codec emits the binary `DecimalCompact` form for values that
    // fit in i64 (every DECIMAL column TPC-C / TPC-H actually use).
    // The display form is unchanged: "123.45".
    assert_eq!(result.rows[0][0].to_display_string(), "123.45");
}

#[test]
fn date_validation_rejects_invalid() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, d DATE)")
        .unwrap();

    engine
        .execute("INSERT INTO t VALUES (1, '2024-01-15')")
        .unwrap();
    assert!(engine
        .execute("INSERT INTO t VALUES (2, 'not-a-date')")
        .is_err());
    assert!(engine
        .execute("INSERT INTO t VALUES (3, '2024-02-30')")
        .is_err());
}

#[test]
fn datetime_validation_rejects_invalid() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, dt DATETIME)")
        .unwrap();

    engine
        .execute("INSERT INTO t VALUES (1, '2024-01-15 10:30:00')")
        .unwrap();
    assert!(engine
        .execute("INSERT INTO t VALUES (2, 'not-a-datetime')")
        .is_err());
}

#[test]
fn char_reads_back_compact_value() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, c CHAR(5))")
        .unwrap();

    engine.execute("INSERT INTO t VALUES (1, 'ab')").unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT c, LENGTH(c) FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::String("ab".into()));
    assert_eq!(result.rows[0][1], Value::Int(2));
}

#[test]
fn varchar_uses_character_count_not_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(4))")
        .unwrap();

    engine
        .execute("INSERT INTO t VALUES (1, '你好世界')")
        .unwrap();
    assert!(engine
        .execute("INSERT INTO t VALUES (2, '你好世界!')")
        .is_err());
}

// ------------------------------------------------------------
// Per-table atomic AUTO_INCREMENT allocation.
// ------------------------------------------------------------

/// Two sessions issuing autocommit INSERTs against the same table must see
/// strictly ascending, non-overlapping ids.
#[test]
fn auto_increment_atomic_counter_is_unique_across_sessions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, owner INT, v INT)")
        .unwrap();
    let mut s1 = SessionContext::default();
    s1.connection_id = 101;
    let mut s2 = SessionContext::default();
    s2.connection_id = 202;

    let mut s1_ids = Vec::new();
    let mut s2_ids = Vec::new();
    for v in 0..6 {
        engine
            .execute_with_session(&mut s1, &format!("INSERT INTO t(owner, v) VALUES (1, {v})"))
            .unwrap();
        let QueryResponse::ResultSet(r) = engine
            .execute_with_session(&mut s1, "SELECT LAST_INSERT_ID()")
            .unwrap()
        else {
            panic!("expected result set");
        };
        s1_ids.push(match r.rows[0][0] {
            Value::Int(n) => n,
            _ => panic!("int"),
        });

        engine
            .execute_with_session(&mut s2, &format!("INSERT INTO t(owner, v) VALUES (2, {v})"))
            .unwrap();
        let QueryResponse::ResultSet(r) = engine
            .execute_with_session(&mut s2, "SELECT LAST_INSERT_ID()")
            .unwrap()
        else {
            panic!("expected result set");
        };
        s2_ids.push(match r.rows[0][0] {
            Value::Int(n) => n,
            _ => panic!("int"),
        });
    }
    // Both sessions get strictly increasing ids and the two
    // streams never collide.
    for w in s1_ids.windows(2) {
        assert!(w[1] > w[0], "s1 ids must be strictly ascending: {s1_ids:?}");
    }
    for w in s2_ids.windows(2) {
        assert!(w[1] > w[0], "s2 ids must be strictly ascending: {s2_ids:?}");
    }
    let mut all = s1_ids.clone();
    all.extend(s2_ids.iter().copied());
    all.sort_unstable();
    all.dedup();
    assert_eq!(all.len(), 12, "all 12 ids must be unique");
}

/// ALTER TABLE … AUTO_INCREMENT=N must atomically raise the table counter.
#[test]
fn auto_increment_atomic_counter_honours_alter() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    session.connection_id = 7;
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, v INT)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t(v) VALUES (1)")
        .unwrap();
    // Bump the counter well past the generated value.
    engine
        .execute_with_session(&mut session, "ALTER TABLE t AUTO_INCREMENT = 1000")
        .unwrap();
    // The next INSERT must respect the new counter.
    engine
        .execute_with_session(&mut session, "INSERT INTO t(v) VALUES (2)")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT id FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![Value::Int(1)], vec![Value::Int(1000)]]
    );
}

/// TRUNCATE publishes a new counter version and restarts at 1.
#[test]
fn auto_increment_version_resets_on_truncate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    session.connection_id = 9;
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, v INT)",
        )
        .unwrap();
    // Force the counter past 1 before the version reset.
    for v in 0..3 {
        engine
            .execute_with_session(&mut session, &format!("INSERT INTO t(v) VALUES ({v})"))
            .unwrap();
    }
    engine
        .execute_with_session(&mut session, "TRUNCATE TABLE t")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t(v) VALUES (99)")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT id FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);
}

/// Counter ownership is table-scoped rather than session-scoped.
#[test]
fn auto_increment_counter_survives_session_close() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, v INT)")
        .unwrap();
    let mut first = SessionContext::default();
    first.connection_id = 17;
    engine
        .execute_with_session(&mut first, "INSERT INTO t(v) VALUES (1)")
        .unwrap();
    engine.close_session(&mut first).unwrap();
    // A brand-new session continues the table-scoped counter.
    let mut second = SessionContext::default();
    second.connection_id = 18;
    engine
        .execute_with_session(&mut second, "INSERT INTO t(v) VALUES (2)")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut second, "SELECT id FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let ids: Vec<i64> = result
        .rows
        .iter()
        .map(|r| match r[0] {
            Value::Int(n) => n,
            _ => panic!("int"),
        })
        .collect();
    assert_eq!(ids.len(), 2);
    assert!(ids[1] > ids[0], "ids must be monotone: {ids:?}");
}

/// Repeated autocommit INSERTs allocate exact consecutive values.
#[test]
fn auto_increment_repeated_autocommit_is_consecutive() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    session.connection_id = 42;
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, v INT)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t(v) VALUES (10)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t(v) VALUES (11)")
        .unwrap();
    // Third INSERT obtains id=3 from another exact atomic allocation.
    engine
        .execute_with_session(&mut session, "INSERT INTO t(v) VALUES (12)")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT id FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)],
        ]
    );
}

#[test]
fn reduced_redo_bulk_append_skips_heap_payload_and_survives_crash_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        let mut session = SessionContext::default();
        engine
            .execute_with_session(
                &mut session,
                "CREATE TABLE reduced_bulk (id INT PRIMARY KEY, payload VARCHAR(128))",
            )
            .unwrap();
        engine
            .execute_with_session(&mut session, "SET SESSION mydb_redo_enabled=OFF")
            .unwrap();
        let values = (0..4_000)
            .map(|id| format!("({id},'payload-{id:04}-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')"))
            .collect::<Vec<_>>()
            .join(",");
        let before = engine.database().storage_status();
        engine
            .execute_with_session(
                &mut session,
                &format!("INSERT INTO reduced_bulk VALUES {values}"),
            )
            .unwrap();
        engine
            .execute_with_session(
                &mut session,
                "CREATE INDEX reduced_payload_idx ON reduced_bulk(payload)",
            )
            .unwrap();
        let after = engine.database().storage_status();
        assert!(
            after.reduced_redo_skipped_records > before.reduced_redo_skipped_records,
            "bulk append should omit at least one HeapInsertBatch"
        );
        assert!(
            after.reduced_redo_skipped_payload_bytes > before.reduced_redo_skipped_payload_bytes,
            "skipped payload bytes should be observable"
        );
        assert!(
            after.reduced_redo_force_flushes > before.reduced_redo_force_flushes,
            "reduced transaction must force data before CommitTxn"
        );
        // Plain drop is the repository's supported in-process crash model: it
        // does not run the clean-shutdown checkpoint.
    }

    let reopened = open_strict_test_engine(dir.path());
    let QueryResponse::ResultSet(result) = reopened
        .execute("SELECT COUNT(*), MIN(id), MAX(id) FROM reduced_bulk")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![Value::Int(4_000), Value::Int(0), Value::Int(3_999)]]
    );
    let QueryResponse::ResultSet(indexed) = reopened
        .execute(
            "SELECT COUNT(*) FROM reduced_bulk FORCE INDEX(reduced_payload_idx) \
             WHERE payload >= 'payload-2000'",
        )
        .unwrap()
    else {
        panic!("expected indexed result set");
    };
    assert_eq!(indexed.rows, vec![vec![Value::Int(2_000)]]);
    reopened.execute("CHECK TABLE reduced_bulk").unwrap();
}

#[test]
fn reduced_redo_crash_loser_truncates_uncommitted_extent() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE reduced_loser (id INT PRIMARY KEY, payload VARCHAR(128))")
            .unwrap();
        engine.checkpoint().unwrap();
        let mut session = SessionContext::default();
        engine
            .execute_with_session(&mut session, "SET SESSION mydb_redo_enabled=OFF")
            .unwrap();
        engine
            .execute_with_session(&mut session, "START TRANSACTION")
            .unwrap();
        let values = (0..2_000)
            .map(|id| format!("({id},'loser-{id:04}-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')"))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute_with_session(
                &mut session,
                &format!("INSERT INTO reduced_loser VALUES {values}"),
            )
            .unwrap();
        // No COMMIT/ROLLBACK and no clean shutdown checkpoint.
    }

    let reopened = open_strict_test_engine(dir.path());
    let QueryResponse::ResultSet(result) = reopened
        .execute("SELECT COUNT(*) FROM reduced_loser")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(0)]]);
    reopened.execute("CHECK TABLE reduced_loser").unwrap();
}

#[test]
fn reduced_redo_keeps_in_place_update_recoverable() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        let mut session = SessionContext::default();
        engine
            .execute_with_session(
                &mut session,
                "CREATE TABLE reduced_update (id INT PRIMARY KEY, v INT)",
            )
            .unwrap();
        engine
            .execute_with_session(&mut session, "INSERT INTO reduced_update VALUES (1,10)")
            .unwrap();
        engine
            .execute_with_session(&mut session, "SET SESSION mydb_redo_enabled=OFF")
            .unwrap();
        let before = engine.database().storage_status().wal_bytes;
        engine
            .execute_with_session(&mut session, "UPDATE reduced_update SET v=99 WHERE id=1")
            .unwrap();
        assert!(
            engine.database().storage_status().wal_bytes > before,
            "in-place UPDATE must retain recovery WAL in reduced mode"
        );
    }
    let reopened = open_strict_test_engine(dir.path());
    let QueryResponse::ResultSet(result) = reopened
        .execute("SELECT v FROM reduced_update WHERE id=1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(99)]]);
}

#[test]
fn reduced_redo_loser_preserves_later_committed_extent() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE audit_extent (id INT PRIMARY KEY, payload VARCHAR(128))")
            .unwrap();
        engine.checkpoint().unwrap();
        let mut loser = SessionContext::default();
        engine
            .execute_with_session(&mut loser, "SET SESSION mydb_redo_enabled=OFF")
            .unwrap();
        engine.execute_with_session(&mut loser, "BEGIN").unwrap();
        let values = (0..100)
            .map(|id| format!("({id},'loser-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')"))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute_with_session(
                &mut loser,
                &format!("INSERT INTO audit_extent VALUES {values}"),
            )
            .unwrap();
        let values = (100..200)
            .map(|id| format!("({id},'winner-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')"))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO audit_extent VALUES {values}"))
            .unwrap();
        let QueryResponse::ResultSet(before) =
            engine.execute("SELECT COUNT(*) FROM audit_extent").unwrap()
        else {
            panic!("result set");
        };
        assert_eq!(before.rows, vec![vec![Value::Int(100)]]);
        // Drop follows the repository's in-process crash model, without checkpoint.
    }
    let reopened = open_strict_test_engine(dir.path());
    let QueryResponse::ResultSet(after) = reopened
        .execute("SELECT COUNT(*) FROM audit_extent")
        .unwrap()
    else {
        panic!("result set");
    };
    assert_eq!(
        after.rows,
        vec![vec![Value::Int(100)]],
        "uncommitted earlier extent must not truncate the later winner"
    );
}

#[test]
fn reduced_redo_recovery_preserves_shared_tail_rows_and_types() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let expected;
    {
        let engine = open_strict_test_engine(dir.path());
        engine.execute("CREATE TABLE shared_extent (id BIGINT PRIMARY KEY, n DECIMAL(18,4), txt LONGTEXT, b BLOB, ts DATETIME(6), extra INT NULL)").unwrap();
        engine.checkpoint().unwrap();
        let mut loser = SessionContext::default();
        engine
            .execute_with_session(&mut loser, "SET SESSION mydb_redo_enabled=OFF")
            .unwrap();
        engine.execute_with_session(&mut loser, "BEGIN").unwrap();
        let values = (0..32)
            .map(|id| format!("({id},123.4567,'loser','abc','2026-10-15 12:13:14.123456',NULL)"))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute_with_session(
                &mut loser,
                &format!("INSERT INTO shared_extent VALUES {values}"),
            )
            .unwrap();
        let text = "字x".repeat(5000);
        engine.execute(&format!("INSERT INTO shared_extent VALUES (100,987.6543,'{text}',X'00FF1234','2026-10-15 12:13:14.654321',NULL)")).unwrap();
        let QueryResponse::ResultSet(rows) = engine
            .execute("SELECT id,n,txt,b,ts,extra FROM shared_extent")
            .unwrap()
        else {
            panic!("result set");
        };
        expected = rows.rows;
        assert_eq!(expected.len(), 1);
    }
    let engine = open_strict_test_engine(dir.path());
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT id,n,txt,b,ts,extra FROM shared_extent")
        .unwrap()
    else {
        panic!("result set");
    };
    assert_eq!(rows.rows, expected);
    engine.execute("CHECK TABLE shared_extent").unwrap();
}

#[test]
fn reduced_redo_shared_page_fpi_recovers_a_torn_base() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let path;
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE torn_reduced (id INT PRIMARY KEY, s VARCHAR(128))")
            .unwrap();
        engine.checkpoint().unwrap();
        let table = engine.database().table("test.torn_reduced").unwrap();
        path = crate::storage::naming::table_object_path(
            dir.path(),
            "test",
            "torn_reduced",
            table.id,
            "heap",
        );
        let mut loser = SessionContext::default();
        engine
            .execute_with_session(&mut loser, "SET SESSION mydb_redo_enabled=OFF")
            .unwrap();
        engine.execute_with_session(&mut loser, "BEGIN").unwrap();
        let values = (0..32)
            .map(|id| format!("({id},'loser')"))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute_with_session(
                &mut loser,
                &format!("INSERT INTO torn_reduced VALUES {values}"),
            )
            .unwrap();
        engine
            .execute("INSERT INTO torn_reduced VALUES (100,'winner')")
            .unwrap();
    }
    // The committed writer's WAL must recover an earlier reduced-redo base
    // even when the data-page write was torn and its LSN survived intact.
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[1000] ^= 1;
    std::fs::write(&path, bytes).unwrap();
    let engine = open_strict_test_engine(dir.path());
    let QueryResponse::ResultSet(rows) = engine.execute("SELECT * FROM torn_reduced").unwrap()
    else {
        panic!("result set");
    };
    assert_eq!(
        rows.rows,
        vec![vec![Value::Int(100), Value::String("winner".into())]]
    );
    engine.execute("CHECK TABLE torn_reduced").unwrap();
}

#[test]
fn fresh_logged_heap_pages_recover_a_torn_first_write() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let path;
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE fresh_torn (id INT PRIMARY KEY, s VARCHAR(128))")
            .unwrap();
        let table = engine.database().table("test.fresh_torn").unwrap();
        path = crate::storage::naming::table_object_path(
            dir.path(),
            "test",
            "fresh_torn",
            table.id,
            "heap",
        );
        let values = (0..1000)
            .map(|id| format!("({id},'payload-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')"))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO fresh_torn VALUES {values}"))
            .unwrap();
        engine.database().flush_dirty_data().unwrap();
    }
    let mut bytes = std::fs::read(&path).unwrap();
    assert!(bytes.len() > 3 * 16384);
    bytes[2 * 16384 + 1000] ^= 1;
    std::fs::write(&path, bytes).unwrap();
    let engine = open_strict_test_engine(dir.path());
    let QueryResponse::ResultSet(rows) = engine.execute("SELECT COUNT(*) FROM fresh_torn").unwrap()
    else {
        panic!("result set");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(1000)]]);
    engine.execute("CHECK TABLE fresh_torn").unwrap();
}
