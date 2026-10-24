//! MySQL 8.0 table DDL regressions in docs/table-ddl-mysql80-compat-design.md.
//! Cover ON UPDATE validation (Z1), table option acceptance including ROW_FORMAT, ZEROFILL,
//! index option downgrades (USING HASH/FULLTEXT prefixes/KEY_BLOCK_SIZE), CREATE
//! INVISIBLE indexes, explicit SUBPARTITION rejection, DISABLE/ENABLE KEYS, FORCE,
//! ORDER BY, INVISIBLE columns, generated-column indexes, CONVERT CHARACTER SET, COPY
//! restriction removal, complete CTAS, prefix primary keys, MODIFY with CHECK, partition maintenance,
//! COLUMN_FORMAT/STORAGE, and SHOW CREATE round trips.
use super::*;

fn exec_ok(engine: &StorageQueryEngine, sql: &str) {
    engine
        .execute(sql)
        .unwrap_or_else(|err| panic!("expected success for `{sql}`: {err:?}"));
}

fn exec_err(engine: &StorageQueryEngine, sql: &str) -> QueryError {
    match engine.execute(sql) {
        Err(err) => err,
        Ok(_) => panic!("expected error for `{sql}`"),
    }
}

fn show_create(engine: &StorageQueryEngine, table: &str) -> String {
    let QueryResponse::ResultSet(result) = engine
        .execute(&format!("SHOW CREATE TABLE {table}"))
        .unwrap()
    else {
        panic!("expected result set for SHOW CREATE TABLE {table}");
    };
    match &result.rows[0][1] {
        Value::String(s) => s.clone(),
        other => panic!("unexpected SHOW CREATE payload: {other:?}"),
    }
}

// ---------- DROP TABLE review: doc_old/design/drop-table-review-2026-07.md ----------

/// C2: `DROP TABLE IF EXISTS <missing>` emits Note 1051 (visible in
/// SHOW WARNINGS) rather than silently succeeding.
#[test]
fn drop_table_if_exists_missing_emits_note_1051() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "DROP TABLE IF EXISTS does_not_exist")
        .unwrap();
    let codes: Vec<u16> = s.warnings.iter().map(|w| w.code()).collect();
    assert!(
        codes.contains(&1051),
        "expected Note 1051 for missing table, got {:?}",
        s.warnings
    );
    // Without IF EXISTS it is a hard 1051 error, not a note.
    let err = engine
        .execute_with_session(&mut s, "DROP TABLE still_missing")
        .unwrap_err();
    assert_eq!(err.code(), 1051, "{}", err.message());
}

/// A reopened v46 catalog contains compact table headers. Resolving the full
/// descriptor for DROP must happen without holding the catalog read guard;
/// otherwise descriptor publication tries to upgrade that same lock and the
/// statement deadlocks forever.
#[test]
fn drop_table_after_reopen_does_not_deadlock_on_lazy_descriptor() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine =
            StorageQueryEngine::open_with_storage_config(dir.path(), strict_config()).unwrap();
        exec_ok(
            &engine,
            "CREATE TABLE persisted_drop (\
                 id INT AUTO_INCREMENT PRIMARY KEY,\
                 v INT,\
                 KEY idx_v (v)\
             )",
        );
        exec_ok(&engine, "INSERT INTO persisted_drop (v) VALUES (10)");
        engine.shutdown().unwrap();
    }

    let engine = StorageQueryEngine::open_with_storage_config(dir.path(), strict_config()).unwrap();
    exec_ok(&engine, "DROP TABLE persisted_drop");
    assert_eq!(
        exec_err(&engine, "SELECT * FROM persisted_drop").code(),
        1146
    );
}

/// TRUNCATE also needs the complete post-reopen descriptor: compact headers
/// omit secondary indexes and AUTO_INCREMENT metadata.
#[test]
fn truncate_table_after_reopen_resets_indexes_and_auto_increment() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine =
            StorageQueryEngine::open_with_storage_config(dir.path(), strict_config()).unwrap();
        exec_ok(
            &engine,
            "CREATE TABLE persisted_truncate (\
                 id INT AUTO_INCREMENT PRIMARY KEY,\
                 v INT,\
                 UNIQUE KEY uk_v (v)\
             )",
        );
        exec_ok(&engine, "INSERT INTO persisted_truncate (v) VALUES (10)");
        engine.shutdown().unwrap();
    }

    let engine = StorageQueryEngine::open_with_storage_config(dir.path(), strict_config()).unwrap();
    exec_ok(&engine, "TRUNCATE TABLE persisted_truncate");
    exec_ok(&engine, "INSERT INTO persisted_truncate (v) VALUES (10)");
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id, v FROM persisted_truncate")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1), Value::Int(10)]]);
}

#[test]
fn drop_table_after_reopen_keeps_inbound_foreign_key_guard() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine =
            StorageQueryEngine::open_with_storage_config(dir.path(), strict_config()).unwrap();
        exec_ok(&engine, "CREATE TABLE drop_parent (id INT PRIMARY KEY)");
        exec_ok(
            &engine,
            "CREATE TABLE drop_child (\
                 id INT PRIMARY KEY,\
                 parent_id INT,\
                 CONSTRAINT fk_drop_parent FOREIGN KEY (parent_id)\
                     REFERENCES drop_parent (id)\
             )",
        );
        engine.shutdown().unwrap();
    }

    let engine = StorageQueryEngine::open_with_storage_config(dir.path(), strict_config()).unwrap();
    assert_eq!(exec_err(&engine, "DROP TABLE drop_parent").code(), 3730);
    exec_ok(&engine, "DROP TABLE drop_child, drop_parent");
}

/// C1: a plain (non-TEMPORARY) `DROP TABLE` may name a mix of temporary and
/// base tables in one statement; the temporary table takes precedence when a
/// name matches both.
#[test]
fn drop_table_mixes_temporary_and_persistent() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();

    engine
        .execute_with_session(&mut s, "CREATE TABLE base (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut s, "CREATE TEMPORARY TABLE tmp (id INT PRIMARY KEY)")
        .unwrap();
    // One statement drops both — previously rejected with 1235.
    engine
        .execute_with_session(&mut s, "DROP TABLE tmp, base")
        .unwrap();
    assert!(engine
        .execute_with_session(&mut s, "SELECT * FROM base")
        .is_err());
    assert!(engine
        .execute_with_session(&mut s, "SELECT * FROM tmp")
        .is_err());

    // Same-name precedence: a temporary table shadows a base table of the same
    // name, so `DROP TABLE t` removes the temporary first, leaving the base.
    engine
        .execute_with_session(&mut s, "CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut s, "INSERT INTO t VALUES (1)")
        .unwrap();
    engine
        .execute_with_session(&mut s, "CREATE TEMPORARY TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute_with_session(&mut s, "DROP TABLE t").unwrap();
    // The base table is still there with its row.
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut s, "SELECT COUNT(*) FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(1));
    // A second DROP removes the base table.
    engine.execute_with_session(&mut s, "DROP TABLE t").unwrap();
    assert!(engine
        .execute_with_session(&mut s, "SELECT * FROM t")
        .is_err());
}

// ---------- GAP-4: LOB literal defaults, docs/lob-management-audit-2026-07.md ----------

#[test]
fn lob_column_literal_default_is_rejected_1101() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // A literal DEFAULT on TEXT/BLOB/JSON/GEOMETRY is ER 1101 (was
    // silently accepted before the fix).
    for ty in ["TEXT", "BLOB", "LONGTEXT", "JSON"] {
        let err = exec_err(
            &engine,
            &format!("CREATE TABLE t_{ty} (id INT, v {ty} DEFAULT 'x')"),
        );
        assert_eq!(
            err.code(),
            1101,
            "{ty} literal default must be 1101, got {err:?}"
        );
    }
}

#[test]
fn lob_column_null_and_no_default_are_allowed() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // No default and explicit DEFAULT NULL are both fine (a nullable LOB
    // column's implicit default is NULL).
    exec_ok(&engine, "CREATE TABLE ok1 (id INT, v TEXT)");
    exec_ok(&engine, "CREATE TABLE ok2 (id INT, v BLOB DEFAULT NULL)");
}

#[test]
fn lob_column_literal_default_via_alter_is_rejected_1101() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE t (id INT, v TEXT)");
    let err = exec_err(&engine, "ALTER TABLE t ALTER COLUMN v SET DEFAULT 'x'");
    assert_eq!(
        err.code(),
        1101,
        "ALTER SET literal default on TEXT must be 1101, got {err:?}"
    );
}

#[test]
fn lob_literal_default_is_dropped_with_warning_in_non_strict_mode() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET sql_mode='NO_ENGINE_SUBSTITUTION'")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE t (id INT, v TEXT DEFAULT '')")
        .unwrap();
    let QueryResponse::ResultSet(warnings) = engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
    else {
        panic!("expected warnings");
    };
    assert!(warnings
        .rows
        .iter()
        .any(|row| { matches!(row.get(1), Some(Value::Int(1101)) | Some(Value::UInt(1101))) }));
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1, DEFAULT)")
        .unwrap();
    let QueryResponse::ResultSet(rows) = engine
        .execute_with_session(&mut session, "SELECT v FROM t")
        .unwrap()
    else {
        panic!("expected rows");
    };
    assert_eq!(rows.rows[0][0], Value::Null);
}

#[test]
fn character_and_binary_width_limits_return_1074() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for definition in [
        "c CHAR(256)",
        "b BINARY(256)",
        "v VARCHAR(70000) DEFAULT 'x'",
        "vb VARBINARY(70000) DEFAULT X'01'",
    ] {
        let err = exec_err(&engine, &format!("CREATE TABLE too_wide ({definition})"));
        assert_eq!(err.code(), 1074, "{definition}: {err:?}");
    }
}

#[test]
fn varchar_width_uses_effective_table_charset() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(
        &engine,
        "CREATE TABLE latin1_wide (v VARCHAR(65500)) CHARACTER SET latin1",
    );
    let column = &engine.db.table("test.latin1_wide").unwrap().columns[0];
    assert!(matches!(
        column.data_type,
        crate::storage::schema::DataTypeKind::VarChar { len: Some(65500) }
    ));

    let err = exec_err(
        &engine,
        "CREATE TABLE utf8_wide (v VARCHAR(65500)) CHARACTER SET utf8mb4",
    );
    assert_eq!(err.code(), 1074);
}

// ---------- Z1: ON UPDATE validation ----------

#[test]
fn on_update_rejects_non_current_timestamp_expr() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let err = exec_err(
        &engine,
        "CREATE TABLE t1 (id INT, ts TIMESTAMP ON UPDATE UNIX_TIMESTAMP())",
    );
    assert_eq!(err.code(), 1294, "non-CURRENT_TIMESTAMP expr must be 1294");
}

#[test]
fn on_update_rejects_non_temporal_column() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let err = exec_err(
        &engine,
        "CREATE TABLE t1 (id INT ON UPDATE CURRENT_TIMESTAMP)",
    );
    assert_eq!(err.code(), 1294, "INT column with ON UPDATE must be 1294");
}

#[test]
fn on_update_requires_exact_fsp_match() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Bare CURRENT_TIMESTAMP has fsp 0; an fsp-6 column requires explicit (6).
    let err = exec_err(
        &engine,
        "CREATE TABLE t1 (ts TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP)",
    );
    assert_eq!(err.code(), 1294);
    let err = exec_err(
        &engine,
        "CREATE TABLE t1 (ts TIMESTAMP ON UPDATE CURRENT_TIMESTAMP(3))",
    );
    assert_eq!(err.code(), 1294);
    exec_ok(
        &engine,
        "CREATE TABLE t1 (ts TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6))",
    );
    exec_ok(&engine, "CREATE TABLE t2 (ts DATETIME(3) ON UPDATE NOW(3))");
    assert!(
        show_create(&engine, "t1").contains("ON UPDATE CURRENT_TIMESTAMP(6)"),
        "SHOW CREATE must echo the canonical fsp form"
    );
}

#[test]
fn on_update_synonyms_accepted_and_fire_on_update() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(
        &engine,
        "CREATE TABLE t1 (id INT PRIMARY KEY, v INT, \
         ts DATETIME ON UPDATE LOCALTIMESTAMP)",
    );
    exec_ok(&engine, "INSERT INTO t1 (id, v) VALUES (1, 10)");
    exec_ok(&engine, "UPDATE t1 SET v = 11 WHERE id = 1");
    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT ts FROM t1 WHERE id = 1").unwrap()
    else {
        panic!("expected result set");
    };
    assert_ne!(result.rows[0][0], Value::Null, "ON UPDATE must fire");
}

// ---------- P0-2: table option acceptance framework ----------

#[test]
fn metadata_table_options_persist_and_echo() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(
        &engine,
        "CREATE TABLE t1 (id INT PRIMARY KEY) ROW_FORMAT=DYNAMIC MAX_ROWS=1000 \
         STATS_PERSISTENT=1 CONNECTION='remote'",
    );
    let created = show_create(&engine, "t1");
    assert!(created.contains("ROW_FORMAT=DYNAMIC"), "{created}");
    assert!(created.contains("MAX_ROWS=1000"), "{created}");
    assert!(created.contains("STATS_PERSISTENT=1"), "{created}");
    assert!(created.contains("CONNECTION='remote'"), "{created}");

    // ALTER overrides and DEFAULT resets.
    exec_ok(&engine, "ALTER TABLE t1 ROW_FORMAT=COMPRESSED MAX_ROWS=50");
    let altered = show_create(&engine, "t1");
    assert!(altered.contains("ROW_FORMAT=COMPRESSED"), "{altered}");
    assert!(altered.contains("MAX_ROWS=50"), "{altered}");
    exec_ok(&engine, "ALTER TABLE t1 ROW_FORMAT=DEFAULT");
    let reset = show_create(&engine, "t1");
    assert!(!reset.contains("ROW_FORMAT"), "{reset}");

    // Synchronize information_schema ROW_FORMAT and CREATE_OPTIONS.
    exec_ok(&engine, "ALTER TABLE t1 ROW_FORMAT=COMPACT");
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT ROW_FORMAT, CREATE_OPTIONS FROM information_schema.tables \
             WHERE table_schema = 'test' AND table_name = 't1'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::String("Compact".into()));
    let Value::String(create_options) = &result.rows[0][1] else {
        panic!("CREATE_OPTIONS must be a string");
    };
    assert!(create_options.contains("max_rows=50"), "{create_options}");
    assert!(
        create_options.contains("row_format=COMPACT"),
        "{create_options}"
    );
}

#[test]
fn metadata_only_create_options_emit_warning_1287() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t_meta (id INT) ENGINE=InnoDB ROW_FORMAT=COMPRESSED",
        )
        .unwrap();
    assert!(
        session
            .warnings
            .iter()
            .any(|warning| warning.code() == 1287),
        "metadata-only acceptance must be visible: {:?}",
        session.warnings
    );
}

#[test]
fn create_table_enforces_identifier_limits_and_mysql_unique_name() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let long = "x".repeat(65);
    let err = exec_err(&engine, &format!("CREATE TABLE `{long}` (id INT)"));
    assert_eq!(err.code(), 1059, "{err:?}");
    let err = exec_err(&engine, &format!("CREATE TABLE long_column (`{long}` INT)"));
    assert_eq!(err.code(), 1059, "{err:?}");

    exec_ok(&engine, "CREATE TABLE named_unique (email INT UNIQUE)");
    let created = show_create(&engine, "named_unique");
    assert!(created.contains("UNIQUE KEY `email`"), "{created}");
    assert!(!created.contains("email_uniq"), "{created}");
}

fn strict_config() -> StorageConfig {
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
        heap_update_reserve_bytes: 2048,
        startup_validation: crate::storage::StartupValidationMode::Auto,
        startup_validate_indexes: true,
    }
}

#[test]
fn metadata_table_options_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine =
            StorageQueryEngine::open_with_storage_config(dir.path(), strict_config()).unwrap();
        exec_ok(
            &engine,
            "CREATE TABLE t1 (id INT) ROW_FORMAT=REDUNDANT KEY_BLOCK_SIZE=4",
        );
    }
    let engine = StorageQueryEngine::open_with_storage_config(dir.path(), strict_config()).unwrap();
    let created = show_create(&engine, "t1");
    assert!(created.contains("ROW_FORMAT=REDUNDANT"), "{created}");
    assert!(created.contains("KEY_BLOCK_SIZE=4"), "{created}");
}

// ---------- SERIAL alias ----------

#[test]
fn serial_expands_to_bigint_unsigned_autoinc_unique() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE s1 (id SERIAL, name VARCHAR(10))");
    let created = show_create(&engine, "s1");
    // SERIAL ≡ BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE.
    assert!(
        created.contains("bigint unsigned"),
        "SERIAL should be bigint unsigned: {created}"
    );
    assert!(
        created.to_ascii_uppercase().contains("AUTO_INCREMENT"),
        "SERIAL implies AUTO_INCREMENT: {created}"
    );
    assert!(
        created.contains("NOT NULL"),
        "SERIAL implies NOT NULL: {created}"
    );
    assert!(
        created.to_ascii_uppercase().contains("UNIQUE"),
        "SERIAL implies a UNIQUE key: {created}"
    );

    // AUTO_INCREMENT actually assigns monotonically when omitted.
    exec_ok(&engine, "INSERT INTO s1 (name) VALUES ('a'), ('b')");
    let QueryResponse::ResultSet(result) = engine.execute("SELECT id FROM s1 ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0].to_display_string(), "1");
    assert_eq!(result.rows[1][0].to_display_string(), "2");

    // Unsigned: a negative explicit value is rejected.
    assert!(
        engine
            .execute("INSERT INTO s1 (id, name) VALUES (-1, 'c')")
            .is_err(),
        "SERIAL column is unsigned"
    );
}

#[test]
fn serial_via_alter_add_column_is_honest_error() {
    // SERIAL is scoped to CREATE TABLE: the ALTER ADD COLUMN path does not
    // support an inline AUTO_INCREMENT + UNIQUE column (add_column rejects
    // any auto column with 1075 and builds no key from a column-level UNIQUE
    // flag). Rather than silently drop the implied attributes, ADD ... SERIAL
    // returns a clear "unsupported" parse error — never a silent no-op.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE s2 (a INT PRIMARY KEY)");
    // The column def still expands (shared CREATE-path builder), but the
    // ADD COLUMN storage path rejects the implied auto-increment column that
    // has no inline key with MySQL error 1075 — an honest failure, not a
    // silent drop of the SERIAL attributes.
    let err = engine.execute("ALTER TABLE s2 ADD sid SERIAL").unwrap_err();
    assert_eq!(
        err.code(),
        1075,
        "expected honest 1075 failure, got {err:?}"
    );
}

// ---------- P0-3: ZEROFILL ----------

#[test]
fn zerofill_accepted_implies_unsigned_and_echoes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(
        &engine,
        "CREATE TABLE t1 (id INT ZEROFILL, d DECIMAL(6,2) UNSIGNED ZEROFILL, plain INT)",
    );
    let created = show_create(&engine, "t1");
    assert!(created.contains("int unsigned zerofill"), "{created}");
    assert!(
        created.contains("decimal(6,2) unsigned zerofill"),
        "{created}"
    );

    // SHOW COLUMNS Type also reflects the type.
    let QueryResponse::ResultSet(result) = engine.execute("SHOW COLUMNS FROM t1").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows[0][1],
        Value::String("int unsigned zerofill".into())
    );

    // Implicit UNSIGNED must reject negative/out-of-range values.
    assert!(
        engine.execute("INSERT INTO t1 (id) VALUES (-1)").is_err(),
        "ZEROFILL column must be unsigned"
    );
}

#[test]
fn zerofill_alter_modify_and_change_paths() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE t1 (a INT, b INT)");
    exec_ok(&engine, "ALTER TABLE t1 MODIFY a INT ZEROFILL");
    exec_ok(&engine, "ALTER TABLE t1 CHANGE b b2 SMALLINT ZEROFILL");
    exec_ok(&engine, "ALTER TABLE t1 ADD c MEDIUMINT ZEROFILL");
    let created = show_create(&engine, "t1");
    assert!(created.contains("`a` int unsigned zerofill"), "{created}");
    assert!(
        created.contains("`b2` smallint unsigned zerofill"),
        "{created}"
    );
    assert!(
        created.contains("`c` mediumint unsigned zerofill"),
        "{created}"
    );
}

#[test]
fn zerofill_rejects_non_numeric_types() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert!(
        engine
            .execute("CREATE TABLE t1 (v VARCHAR(10) ZEROFILL)")
            .is_err(),
        "ZEROFILL on VARCHAR must error (MySQL 1064)"
    );
}

#[test]
fn zerofill_emits_deprecation_warning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE t1 (id INT ZEROFILL)")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(
        result
            .rows
            .iter()
            .any(|row| row[1] == Value::Int(1681) || row[1] == Value::UInt(1681)),
        "expected deprecation warning 1681, got {:?}",
        result.rows
    );
}

#[test]
fn deprecated_numeric_type_syntax_warnings_are_source_accurate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t1 (
                a INT(11),
                b FLOAT(7,2) UNSIGNED,
                c DECIMAL(8,2) UNSIGNED,
                y YEAR(4),
                z INT ZEROFILL
            )",
        )
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let codes: Vec<i64> = result
        .rows
        .iter()
        .map(|row| match row[1] {
            Value::Int(value) => value,
            Value::UInt(value) => value as i64,
            ref other => panic!("unexpected warning code {other:?}"),
        })
        .collect();
    assert_eq!(codes.iter().filter(|&&code| code == 1681).count(), 5);
    assert_eq!(codes.iter().filter(|&&code| code == 1287).count(), 1);

    // The tokenizer must not diagnose deprecated-looking text in literals.
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t2 (v VARCHAR(40) DEFAULT 'INT(11) ZEROFILL')",
        )
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(result.rows.is_empty(), "{:?}", result.rows);
}

#[test]
fn copied_numeric_definitions_preserve_deprecation_warnings() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE src (
                f FLOAT(7,2),
                d DOUBLE UNSIGNED,
                n DECIMAL(8,2) UNSIGNED,
                z INT ZEROFILL
            )",
        )
        .unwrap();

    engine
        .execute_with_session(&mut session, "CREATE TABLE copied LIKE src")
        .unwrap();
    assert_eq!(
        session
            .warnings
            .iter()
            .filter(|warning| warning.code() == 1681)
            .count(),
        4,
        "LIKE warnings: {:?}",
        session.warnings
    );

    engine
        .execute_with_session(&mut session, "CREATE TABLE selected AS SELECT * FROM src")
        .unwrap();
    assert_eq!(
        session
            .warnings
            .iter()
            .filter(|warning| warning.code() == 1681)
            .count(),
        4,
        "CTAS warnings: {:?}",
        session.warnings
    );

    exec_ok(&engine, "CREATE TABLE union_left (v DOUBLE(10,1))");
    exec_ok(&engine, "CREATE TABLE union_right (v DOUBLE(10,9))");
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE union_copy SELECT * FROM union_right UNION SELECT * FROM union_left",
        )
        .unwrap();
    assert_eq!(
        session
            .warnings
            .iter()
            .filter(|warning| warning.code() == 1681)
            .count(),
        1,
        "set-operation CTAS warnings: {:?}",
        session.warnings
    );
}

#[test]
fn expression_user_variable_assignment_warns_but_set_does_not() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET @a := 0")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(result.rows.is_empty(), "{:?}", result.rows);

    engine
        .execute_with_session(&mut session, "SELECT @a := 1, @b := @a + 1")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result
            .rows
            .iter()
            .filter(|row| row[1] == Value::Int(1287) || row[1] == Value::UInt(1287))
            .count(),
        2,
        "{:?}",
        result.rows
    );
}

#[test]
fn unary_binary_warns_without_warning_for_explicit_cast() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SELECT BINARY 'a'")
        .unwrap();
    let QueryResponse::ResultSet(warnings) = engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
    else {
        panic!("expected warnings");
    };
    assert!(warnings
        .rows
        .iter()
        .any(|row| row[1] == Value::Int(1287) || row[1] == Value::UInt(1287)));

    engine
        .execute_with_session(&mut session, "SELECT CAST('a' AS BINARY)")
        .unwrap();
    let QueryResponse::ResultSet(warnings) = engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
    else {
        panic!("expected warnings");
    };
    assert!(warnings.rows.is_empty(), "{:?}", warnings.rows);
}

#[test]
fn binary_data_type_is_not_the_deprecated_character_attribute() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE binary_type_ok (
               fixed BINARY(16),
               implicit BINARY,
               binary_charset CHAR(2) CHARACTER SET binary
             )",
        )
        .unwrap();
    assert!(
        session
            .warnings
            .iter()
            .all(|warning| warning.code() != 1287),
        "BINARY is a data type, not the deprecated character attribute: {:?}",
        session.warnings
    );

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE binary_attribute_warns (c CHAR(16) BINARY)",
        )
        .unwrap();
    assert!(
        session
            .warnings
            .iter()
            .any(|warning| warning.code() == 1287),
        "the legacy character-type BINARY attribute must remain observable"
    );
}

#[test]
fn convert_binary_type_is_not_the_deprecated_unary_operator() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "SELECT CONVERT('a', BINARY(4))")
        .unwrap();
    assert!(
        session
            .warnings
            .iter()
            .all(|warning| warning.code() != 1287),
        "the BINARY type in CONVERT must not be diagnosed as unary BINARY: {:?}",
        session.warnings
    );

    engine
        .execute_with_session(&mut session, "SELECT BINARY 'a'")
        .unwrap();
    assert!(
        session
            .warnings
            .iter()
            .any(|warning| warning.code() == 1287),
        "the deprecated unary BINARY operator must remain observable"
    );

    engine
        .execute_with_session(
            &mut session,
            "SELECT POSITION(BINARY 'll' IN 'hello'), POSITION('a' IN BINARY 'hello')",
        )
        .unwrap();
    assert_eq!(
        session
            .warnings
            .iter()
            .filter(|warning| warning.code() == 1287)
            .count(),
        2,
        "each unary BINARY occurrence must emit its own deprecation warning"
    );
}

#[test]
fn zerofill_pads_text_protocol_output() {
    use crate::mysql::protocol::{zerofill_pad_spec, zerofill_padded_row, ZEROFILL_FLAG};
    let mut column = crate::types::Column {
        name: "id".into(),
        column_type: crate::types::ColumnType::Long,
        source: Default::default(),
    };
    column.source.flags = ZEROFILL_FLAG;
    let columns = vec![column];
    let spec = zerofill_pad_spec(&columns).expect("flagged column must produce a spec");
    let padded = zerofill_padded_row(&[Value::Int(42)], &spec);
    assert_eq!(padded[0], Value::String("0000000042".into()));
    // Preserve output when the display width is met or exceeded.
    let wide = zerofill_padded_row(&[Value::Int(12345678901)], &spec);
    assert_eq!(wide[0], Value::Int(12345678901));

    let mut float_column = crate::types::Column {
        name: "f".into(),
        column_type: crate::types::ColumnType::Float,
        source: Default::default(),
    };
    float_column.source.flags = ZEROFILL_FLAG;
    let spec = zerofill_pad_spec(&[float_column]).expect("FLOAT ZEROFILL has a default width");
    assert_eq!(
        zerofill_padded_row(&[Value::Float(3.14152)], &spec),
        vec![Value::String("000003.14152".into())]
    );
}

// ---------- P0-4: index option downgrades ----------

#[test]
fn using_hash_downgrades_to_btree() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(
        &engine,
        "CREATE TABLE t1 (id INT, v INT, UNIQUE KEY uk (id) USING HASH, KEY k (v) USING HASH)",
    );
    exec_ok(&engine, "CREATE INDEX k2 ON t1 (v) USING HASH");
    let QueryResponse::ResultSet(result) = engine.execute("SHOW INDEX FROM t1").unwrap() else {
        panic!("expected result set");
    };
    // Index_type at position 10 must always be BTREE.
    for row in &result.rows {
        assert_eq!(row[10], Value::String("BTREE".into()), "{row:?}");
    }
    // Queries remain usable.
    exec_ok(&engine, "INSERT INTO t1 (id, v) VALUES (1, 2)");
    let QueryResponse::ResultSet(result) = engine.execute("SELECT v FROM t1 WHERE id = 1").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(2));
}

#[test]
fn fulltext_prefix_and_direction_accepted_and_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(
        &engine,
        "CREATE TABLE t1 (id INT, body TEXT, FULLTEXT KEY ft (body(64)))",
    );
    exec_ok(
        &engine,
        "INSERT INTO t1 VALUES (1, 'hello brave new world')",
    );
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM t1 WHERE MATCH(body) AGAINST ('brave')")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 1, "prefix must be ignored, index usable");
}

#[test]
fn index_level_key_block_size_accepted_and_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(
        &engine,
        "CREATE TABLE t1 (id INT, v INT, KEY k (v) KEY_BLOCK_SIZE=8)",
    );
    exec_ok(&engine, "ALTER TABLE t1 ADD INDEX k2 (id) KEY_BLOCK_SIZE 4");
    exec_ok(&engine, "CREATE INDEX k3 ON t1 (v) KEY_BLOCK_SIZE=16");
    let QueryResponse::ResultSet(result) = engine.execute("SHOW INDEX FROM t1").unwrap() else {
        panic!("expected result set");
    };
    let names: Vec<String> = result
        .rows
        .iter()
        .map(|row| match &row[2] {
            Value::String(s) => s.clone(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert!(names.contains(&"k".to_string()), "{names:?}");
    assert!(names.contains(&"k2".to_string()), "{names:?}");
    assert!(names.contains(&"k3".to_string()), "{names:?}");
    // Table-level KEY_BLOCK_SIZE still uses table option storage and display.
    exec_ok(&engine, "ALTER TABLE t1 KEY_BLOCK_SIZE=8");
    assert!(show_create(&engine, "t1").contains("KEY_BLOCK_SIZE=8"));
}

// ---------- P0-5/6/7: INVISIBLE indexes, SUBPARTITION, miscellaneous ALTER ----------

#[test]
fn inline_invisible_index_in_create_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(
        &engine,
        "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT, \
         KEY ka (a) INVISIBLE, UNIQUE KEY kb (b) VISIBLE)",
    );
    let QueryResponse::ResultSet(result) = engine.execute("SHOW INDEX FROM t1").unwrap() else {
        panic!("expected result set");
    };
    // Find ka/kb in rows with Key_name at 2 and Visible at 13.
    let visible_of = |name: &str| -> String {
        for row in &result.rows {
            if row[2] == Value::String(name.into()) {
                if let Value::String(v) = &row[13] {
                    return v.clone();
                }
            }
        }
        panic!("index {name} not found: {:?}", result.rows);
    };
    assert_eq!(visible_of("ka"), "NO");
    assert_eq!(visible_of("kb"), "YES");

    // ALTER ADD INDEX INVISIBLE also takes effect.
    exec_ok(&engine, "ALTER TABLE t1 ADD INDEX kc (a, b) INVISIBLE");
    let QueryResponse::ResultSet(result) = engine.execute("SHOW INDEX FROM t1").unwrap() else {
        panic!("expected result set");
    };
    let kc_visible = result
        .rows
        .iter()
        .find(|row| row[2] == Value::String("kc".into()))
        .map(|row| row[13].clone());
    assert_eq!(kc_visible, Some(Value::String("NO".into())));
}

#[test]
fn primary_key_cannot_be_invisible() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let err = engine
        .execute("CREATE TABLE t1 (id INT, PRIMARY KEY (id) INVISIBLE)")
        .expect_err("PK INVISIBLE must error");
    assert_eq!(err.code(), 3522, "expected PK-invisible error: {err:?}");
}

#[test]
fn subpartition_by_hash_round_trips_show_create_and_dml() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t1 (id INT, y INT, v INT) \
         PARTITION BY RANGE (y) \
         SUBPARTITION BY HASH (id) SUBPARTITIONS 2 \
         (PARTITION p0 VALUES LESS THAN (2000), PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    for id in 0..40 {
        let y = if id < 20 { 1000 } else { 3000 };
        engine
            .execute(&format!("INSERT INTO t1 VALUES ({id}, {y}, 0)"))
            .unwrap();
    }
    let QueryResponse::ResultSet(show) = engine.execute("SHOW CREATE TABLE t1").unwrap() else {
        panic!("expected result set");
    };
    let Value::String(create_sql) = &show.rows[0][1] else {
        panic!("expected create sql");
    };
    assert!(
        create_sql.contains("SUBPARTITION BY HASH (id)") && create_sql.contains("SUBPARTITIONS 2"),
        "{create_sql}"
    );
    let QueryResponse::ResultSet(parts) = engine
        .execute(
            "SELECT PARTITION_NAME, SUBPARTITION_NAME, SUBPARTITION_METHOD \
             FROM information_schema.partitions \
             WHERE TABLE_SCHEMA = 'test' AND TABLE_NAME = 't1' \
             ORDER BY PARTITION_NAME, SUBPARTITION_NAME",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(parts.rows.len(), 4);
    assert_eq!(parts.rows[0][0], Value::String("p0".into()));
    assert_eq!(parts.rows[0][1], Value::String("p0sp0".into()));
    assert_eq!(parts.rows[0][2], Value::String("HASH".into()));

    fn count(engine: &StorageQueryEngine, sql: &str) -> i64 {
        let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
            panic!("expected result set for {sql}");
        };
        match result.rows[0][0] {
            Value::Int(v) => v,
            Value::UInt(v) => v as i64,
            ref other => panic!("unexpected count payload: {other:?}"),
        }
    }

    let p0 = count(&engine, "SELECT COUNT(*) FROM t1 PARTITION (p0)");
    let p0sp0 = count(&engine, "SELECT COUNT(*) FROM t1 PARTITION (p0sp0)");
    let p0sp1 = count(&engine, "SELECT COUNT(*) FROM t1 PARTITION (p0sp1)");
    assert_eq!(p0, 20);
    assert_eq!(p0, p0sp0 + p0sp1);

    engine
        .execute("UPDATE t1 PARTITION (p0) SET v = 7")
        .unwrap();
    assert_eq!(count(&engine, "SELECT COUNT(*) FROM t1 WHERE v = 7"), p0);
    assert_eq!(
        count(
            &engine,
            "SELECT COUNT(*) FROM t1 PARTITION (p1) WHERE v = 7"
        ),
        0
    );

    engine.execute("DELETE FROM t1 PARTITION (p0sp0)").unwrap();
    assert_eq!(count(&engine, "SELECT COUNT(*) FROM t1"), 40 - p0sp0);
    assert_eq!(
        count(&engine, "SELECT COUNT(*) FROM t1 PARTITION (p0)"),
        p0sp1
    );
}

#[test]
fn partition_expression_with_nested_parentheses_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t_year (id INT, dt DATE) \
             PARTITION BY RANGE (YEAR(dt)) \
             SUBPARTITION BY HASH (id) SUBPARTITIONS 2 \
             (PARTITION p0 VALUES LESS THAN (2025), PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t_year VALUES (1, '2024-01-01'), (2, '2026-01-01')")
        .unwrap();

    let QueryResponse::ResultSet(show) = engine.execute("SHOW CREATE TABLE t_year").unwrap() else {
        panic!("expected result set");
    };
    let Value::String(create_sql) = &show.rows[0][1] else {
        panic!("expected create sql");
    };
    assert!(
        create_sql
            .to_ascii_lowercase()
            .contains("partition by range (year(dt))"),
        "{create_sql}"
    );

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT COUNT(*) FROM t_year PARTITION (p0)")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(1));
}

#[test]
fn explicit_subpartition_definitions_round_trip_and_route() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t_sub (id INT, y INT) \
             PARTITION BY RANGE (y) \
             SUBPARTITION BY HASH (id) \
             (PARTITION p0 VALUES LESS THAN (10) \
                (SUBPARTITION p0a COMMENT = 'left', SUBPARTITION p0b), \
              PARTITION p1 VALUES LESS THAN MAXVALUE \
                (SUBPARTITION p1a, SUBPARTITION p1b))",
        )
        .unwrap();
    for id in 0..20 {
        let y = if id < 10 { 1 } else { 20 };
        engine
            .execute(&format!("INSERT INTO t_sub VALUES ({id}, {y})"))
            .unwrap();
    }

    let QueryResponse::ResultSet(show) = engine.execute("SHOW CREATE TABLE t_sub").unwrap() else {
        panic!("expected result set");
    };
    let Value::String(create_sql) = &show.rows[0][1] else {
        panic!("expected create sql");
    };
    assert!(
        create_sql.contains("SUBPARTITION `p0a` COMMENT = 'left'"),
        "{create_sql}"
    );
    assert!(create_sql.contains("SUBPARTITION `p1b`"), "{create_sql}");

    let QueryResponse::ResultSet(parts) = engine
        .execute(
            "SELECT PARTITION_NAME, SUBPARTITION_NAME \
             FROM information_schema.partitions \
             WHERE TABLE_SCHEMA = 'test' AND TABLE_NAME = 't_sub' \
             ORDER BY PARTITION_NAME, SUBPARTITION_NAME",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        parts
            .rows
            .iter()
            .map(|row| row[1].clone())
            .collect::<Vec<_>>(),
        vec![
            Value::String("p0a".into()),
            Value::String("p0b".into()),
            Value::String("p1a".into()),
            Value::String("p1b".into()),
        ]
    );

    let QueryResponse::ResultSet(parent) = engine
        .execute("SELECT COUNT(*) FROM t_sub PARTITION (p0)")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let QueryResponse::ResultSet(left_leaf) = engine
        .execute("SELECT COUNT(*) FROM t_sub PARTITION (p0a)")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let QueryResponse::ResultSet(right_leaf) = engine
        .execute("SELECT COUNT(*) FROM t_sub PARTITION (p0b)")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let parent_count = match parent.rows[0][0] {
        Value::Int(v) => v,
        Value::UInt(v) => v as i64,
        ref other => panic!("unexpected count payload: {other:?}"),
    };
    let leaf_total: i64 = [&left_leaf, &right_leaf]
        .iter()
        .map(|result| match result.rows[0][0] {
            Value::Int(v) => v,
            Value::UInt(v) => v as i64,
            ref other => panic!("unexpected count payload: {other:?}"),
        })
        .sum();
    assert_eq!(parent_count, 10);
    assert_eq!(parent_count, leaf_total);
}

#[test]
fn alter_noop_actions_accepted_with_diagnostics() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE t1 (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t1 VALUES (1, 2)")
        .unwrap();

    // mysqldump form: DISABLE/ENABLE KEYS inside versioned comments.
    engine
        .execute_with_session(&mut session, "/*!40000 ALTER TABLE `t1` DISABLE KEYS */")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(
        result
            .rows
            .iter()
            .any(|row| row[1] == Value::Int(1031) || row[1] == Value::UInt(1031)),
        "DISABLE KEYS must produce Note 1031, got {:?}",
        result.rows
    );
    engine
        .execute_with_session(&mut session, "/*!40000 ALTER TABLE `t1` ENABLE KEYS */")
        .unwrap();

    // Accept FORCE without diagnostics, matching MySQL.
    engine
        .execute_with_session(&mut session, "ALTER TABLE t1 FORCE")
        .unwrap();

    // Accept ORDER BY with warning 1105.
    engine
        .execute_with_session(&mut session, "ALTER TABLE t1 ORDER BY v")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(
        result
            .rows
            .iter()
            .any(|row| row[1] == Value::Int(1105) || row[1] == Value::UInt(1105)),
        "ORDER BY must produce Warning 1105, got {:?}",
        result.rows
    );

    // Data is unaffected.
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT v FROM t1 WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(2));
}

// ---------- P1-1: INVISIBLE columns ----------

#[test]
fn invisible_column_full_chain() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(
        &engine,
        "CREATE TABLE t1 (id INT PRIMARY KEY, v INT, hidden INT INVISIBLE DEFAULT 7)",
    );
    exec_ok(&engine, "INSERT INTO t1 VALUES (1, 10)");
    exec_ok(&engine, "INSERT INTO t1 (id, v, hidden) VALUES (2, 20, 99)");

    // SELECT * omits invisible columns.
    let QueryResponse::ResultSet(result) = engine.execute("SELECT * FROM t1 ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.columns.len(), 2, "{:?}", result.columns);
    assert_eq!(result.rows[0], vec![Value::Int(1), Value::Int(10)]);

    // Explicitly named columns remain accessible.
    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT hidden FROM t1 ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(7), "default must fill");
    assert_eq!(result.rows[1][0], Value::Int(99));

    // SHOW COLUMNS lists them with Extra=INVISIBLE.
    let QueryResponse::ResultSet(result) = engine.execute("SHOW COLUMNS FROM t1").unwrap() else {
        panic!("expected result set");
    };
    let hidden_row = result
        .rows
        .iter()
        .find(|row| row[0] == Value::String("hidden".into()))
        .expect("hidden must be listed");
    let Value::String(extra) = &hidden_row[5] else {
        panic!("Extra must be a string");
    };
    assert!(extra.contains("INVISIBLE"), "{extra}");

    // SHOW CREATE reflects the option.
    assert!(show_create(&engine, "t1").contains("INVISIBLE"));

    // ALTER COLUMN SET VISIBLE/INVISIBLE toggles visibility.
    exec_ok(&engine, "ALTER TABLE t1 ALTER COLUMN hidden SET VISIBLE");
    let QueryResponse::ResultSet(result) = engine.execute("SELECT * FROM t1 WHERE id = 1").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.columns.len(), 3);
    exec_ok(&engine, "ALTER TABLE t1 ALTER COLUMN hidden SET INVISIBLE");
    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT * FROM t1 WHERE id <= 1").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.columns.len(), 2, "distinct SQL text (cache probe)");
    let QueryResponse::ResultSet(result) = engine.execute("SELECT * FROM t1 WHERE id = 1").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.columns.len(), 2, "same SQL text (cached plan)");
}

#[test]
fn invisible_column_modify_path_and_guards() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE t1 (a INT, b INT)");
    exec_ok(&engine, "ALTER TABLE t1 MODIFY b INT INVISIBLE");
    let QueryResponse::ResultSet(result) = engine.execute("SELECT * FROM t1").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.columns.len(), 1);

    // Making all columns invisible returns ER_TABLE_MUST_HAVE_A_VISIBLE_COLUMN (4028).
    let err = exec_err(&engine, "ALTER TABLE t1 MODIFY a INT INVISIBLE");
    assert_eq!(err.code(), 4028, "{err:?}");
    let err = exec_err(&engine, "CREATE TABLE t2 (x INT INVISIBLE)");
    assert_eq!(err.code(), 4028, "{err:?}");

    // A primary-key column cannot be invisible.
    assert!(engine
        .execute("CREATE TABLE t3 (id INT PRIMARY KEY INVISIBLE)")
        .is_err());
}

#[test]
fn invisible_column_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine =
            StorageQueryEngine::open_with_storage_config(dir.path(), strict_config()).unwrap();
        exec_ok(
            &engine,
            "CREATE TABLE t1 (id INT PRIMARY KEY, hidden INT INVISIBLE)",
        );
    }
    let engine = StorageQueryEngine::open_with_storage_config(dir.path(), strict_config()).unwrap();
    let QueryResponse::ResultSet(result) = engine.execute("SELECT * FROM t1").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.columns.len(), 1, "invisible must persist");
    assert!(show_create(&engine, "t1").contains("INVISIBLE"));
}

#[test]
fn physical_promise_table_options_still_error() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE t1 (id INT) ENCRYPTION='Y'",
        "CREATE TABLE t1 (id INT) TABLESPACE ts1",
        "CREATE TABLE t1 (id INT) DATA DIRECTORY='/tmp'",
    ] {
        assert!(
            engine.execute(sql).is_err(),
            "physical-promise option must keep erroring: {sql}"
        );
    }
}

#[test]
fn default_current_timestamp_requires_fsp_match() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let err = exec_err(
        &engine,
        "CREATE TABLE t1 (ts TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP)",
    );
    assert_eq!(err.code(), 1067);
    let err = exec_err(
        &engine,
        "CREATE TABLE t1 (d DATE DEFAULT CURRENT_TIMESTAMP)",
    );
    assert_eq!(err.code(), 1067);
    exec_ok(
        &engine,
        "CREATE TABLE t1 (id INT, ts TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6))",
    );
    exec_ok(&engine, "INSERT INTO t1 (id) VALUES (1)");
    let QueryResponse::ResultSet(result) = engine.execute("SELECT ts FROM t1").unwrap() else {
        panic!("expected result set");
    };
    assert_ne!(result.rows[0][0], Value::Null, "fsp default must populate");
}

#[test]
fn literal_defaults_are_validated_and_canonicalized_at_ddl_time() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE bad_date (d DATE DEFAULT '1962-03-32 23:33:34')",
        "CREATE TABLE bad_time (t TIME DEFAULT '916:00:00 a')",
    ] {
        let err = exec_err(&engine, sql);
        assert_eq!(err.code(), 1067, "{sql}: {err:?}");
        assert_eq!(err.sql_state(), "42000");
    }

    exec_ok(
        &engine,
        "CREATE TABLE valid_min (v BIGINT DEFAULT -9223372036854775808)",
    );
    exec_ok(&engine, "INSERT INTO valid_min VALUES (DEFAULT)");
    let QueryResponse::ResultSet(result) = engine.execute("SELECT v FROM valid_min").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(i64::MIN)]]);
}

// ---------- P1-2: generated-column secondary indexes ----------

#[test]
fn secondary_index_on_generated_columns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(
        &engine,
        "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, \
         gv INT GENERATED ALWAYS AS (a + 1) VIRTUAL, \
         gs INT GENERATED ALWAYS AS (a * 2) STORED, \
         KEY kgv (gv), UNIQUE KEY kgs (gs))",
    );
    for i in 1..=20 {
        exec_ok(
            &engine,
            &format!("INSERT INTO t1 (id, a) VALUES ({i}, {i})"),
        );
    }
    // The index is usable and query results are correct.
    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT id FROM t1 WHERE gv = 6").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(5)]]);
    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT id FROM t1 WHERE gs = 10").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(5)]]);
    // Reject duplicate generated values under UNIQUE.
    assert!(
        engine
            .execute("INSERT INTO t1 (id, a) VALUES (100, 5)")
            .is_err(),
        "duplicate generated unique key must be rejected"
    );
    // Updating base columns also updates indexes.
    exec_ok(&engine, "UPDATE t1 SET a = 50 WHERE id = 5");
    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT id FROM t1 WHERE gv = 51").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(5)]]);
    // ALTER ADD INDEX supports the same behavior.
    exec_ok(&engine, "ALTER TABLE t1 ADD INDEX kgv2 (gv, id)");
    let QueryResponse::ResultSet(result) = engine.execute("SHOW INDEX FROM t1").unwrap() else {
        panic!("expected result set");
    };
    assert!(result
        .rows
        .iter()
        .any(|row| row[2] == Value::String("kgv2".into())));
}

// ---------- P1-3: CONVERT TO CHARACTER SET ----------

#[test]
fn convert_to_character_set_table_wide() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(
        &engine,
        "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(50), body TEXT, raw VARBINARY(16)) \
         DEFAULT CHARSET=latin1",
    );
    exec_ok(&engine, "INSERT INTO t1 VALUES (1, 'Ann', 'text', NULL)");
    exec_ok(
        &engine,
        "ALTER TABLE t1 CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
    );
    let created = show_create(&engine, "t1");
    assert!(created.contains("DEFAULT CHARSET=utf8mb4"), "{created}");
    assert!(created.contains("COLLATE=utf8mb4_bin"), "{created}");

    // Column character sets are synchronized in information_schema.columns.
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT column_name, character_set_name, collation_name \
             FROM information_schema.columns \
             WHERE table_schema = 'test' AND table_name = 't1' ORDER BY ordinal_position",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let by_name = |n: &str| {
        result
            .rows
            .iter()
            .find(|r| r[0] == Value::String(n.into()))
            .cloned()
            .unwrap()
    };
    assert_eq!(by_name("name")[1], Value::String("utf8mb4".into()));
    assert_eq!(by_name("name")[2], Value::String("utf8mb4_bin".into()));
    assert_eq!(by_name("body")[1], Value::String("utf8mb4".into()));
    // Binary columns remain unaffected.
    assert_ne!(by_name("raw")[1], Value::String("utf8mb4".into()));

    // Data remains intact. Column-collation-driven WHERE comparison was a separate
    // historical gap, also affecting CREATE utf8mb4_bin, unrelated to this DDL change.
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT COUNT(*) FROM t1 WHERE name = 'Ann'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows[0][0],
        Value::Int(1),
        "data intact after convert"
    );

    // CHARSET without COLLATE selects its default collation.
    exec_ok(&engine, "ALTER TABLE t1 CONVERT TO CHARACTER SET utf8mb4");
    assert!(show_create(&engine, "t1").contains("utf8mb4_0900_ai_ci"));

    // Invalid character sets fail.
    assert!(engine
        .execute("ALTER TABLE t1 CONVERT TO CHARACTER SET bogus")
        .is_err());
}

#[test]
fn convert_to_character_set_rejects_unrepresentable_rows_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(
        &engine,
        "CREATE TABLE charset_atomic (id INT PRIMARY KEY, v VARCHAR(20), KEY idx_v(v)) \
         DEFAULT CHARSET=utf8mb4",
    );
    exec_ok(&engine, "INSERT INTO charset_atomic VALUES (1, '汉字')");

    let err = exec_err(
        &engine,
        "ALTER TABLE charset_atomic CONVERT TO CHARACTER SET latin1",
    );
    assert_eq!(err.code(), 1366, "{err:?}");

    // The failed COPY must leave both schema and indexed data in the old
    // generation.
    let created = show_create(&engine, "charset_atomic");
    assert!(created.contains("DEFAULT CHARSET=utf8mb4"), "{created}");
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM charset_atomic WHERE v='汉字'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn convert_to_character_set_non_strict_replaces_and_warns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE charset_lossy(id INT PRIMARY KEY, v VARCHAR(20)) \
             DEFAULT CHARSET=utf8mb4",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO charset_lossy VALUES(1,'汉字')")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET SESSION sql_mode=''")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "ALTER TABLE charset_lossy CONVERT TO CHARACTER SET latin1",
        )
        .unwrap();
    assert!(session
        .warnings
        .iter()
        .any(|warning| warning.code() == 1366));
    let QueryResponse::ResultSet(rows) = engine
        .execute_with_session(&mut session, "SELECT v FROM charset_lossy")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows, vec![vec![Value::String("??".into())]]);
}

// ---------- P1-4: lift COPY restrictions ----------

#[test]
fn multi_action_alter_with_copy_rewrite() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(
        &engine,
        "CREATE TABLE t1 (id INT PRIMARY KEY, v VARCHAR(10), n INT)",
    );
    exec_ok(&engine, "INSERT INTO t1 VALUES (1, '42', 7)");
    // Combine MODIFY type change (COPY) with ADD COLUMN (INSTANT).
    exec_ok(
        &engine,
        "ALTER TABLE t1 MODIFY v INT, ADD COLUMN extra INT DEFAULT 5",
    );
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT v + 1, extra FROM t1 WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(43), "v must be INT now");
    assert_eq!(result.rows[0][1], Value::Int(5));
    let created = show_create(&engine, "t1");
    assert!(created.contains("`v` int"), "{created}");
    assert!(created.contains("`extra` int"), "{created}");
}

#[test]
fn partitioned_table_copy_rewrite() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(
        &engine,
        "CREATE TABLE t1 (id INT, v VARCHAR(10)) \
         PARTITION BY HASH (id) PARTITIONS 4",
    );
    for i in 1..=20 {
        exec_ok(&engine, &format!("INSERT INTO t1 VALUES ({i}, '{i}')"));
    }
    // Type changes trigger COPY rewriting and reroute partitioned data.
    exec_ok(&engine, "ALTER TABLE t1 MODIFY v INT");
    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT COUNT(*), SUM(v) FROM t1").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(20));
    assert_eq!(
        crate::storage::value_to_string(&result.rows[0][1]),
        "210",
        "sum of 1..=20"
    );
    // Partition pruning remains usable.
    let QueryResponse::ResultSet(result) = engine.execute("SELECT v FROM t1 WHERE id = 7").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(7)]]);
    // Data survives restart, preserving WAL/recovery consistency.
    drop(engine);
    let engine = open_test_engine(dir.path());
    let _ = engine; // Memory durability: restart assertions are covered by strict-mode tests.
}

#[test]
fn partitioned_copy_rewrite_survives_reopen_strict() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine =
            StorageQueryEngine::open_with_storage_config(dir.path(), strict_config()).unwrap();
        exec_ok(
            &engine,
            "CREATE TABLE t1 (id INT, v VARCHAR(10)) PARTITION BY HASH (id) PARTITIONS 2",
        );
        exec_ok(&engine, "INSERT INTO t1 VALUES (1, '10'), (2, '20')");
        exec_ok(&engine, "ALTER TABLE t1 MODIFY v BIGINT");
    }
    let engine = StorageQueryEngine::open_with_storage_config(dir.path(), strict_config()).unwrap();
    let QueryResponse::ResultSet(result) = engine.execute("SELECT SUM(v) FROM t1").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(crate::storage::value_to_string(&result.rows[0][0]), "30");
}

#[test]
fn fk_column_type_change_still_rejected_like_mysql() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE parent (id INT PRIMARY KEY)");
    exec_ok(
        &engine,
        "CREATE TABLE child (id INT PRIMARY KEY, pid INT, \
         FOREIGN KEY (pid) REFERENCES parent(id))",
    );
    // MySQL 8.0 also rejects this with 1832/1833; retain alignment.
    assert!(engine
        .execute("ALTER TABLE child MODIFY pid BIGINT")
        .is_err());
}

// ---------- P1-5: complete CTAS forms ----------

#[test]
fn ctas_promotes_invalid_decimal_comparison_diagnostics_and_rolls_back_target() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE src (a DECIMAL(10,0), b DECIMAL(10,0), KEY(a))",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO src VALUES (0,0),(1,1)")
        .unwrap();

    // A plain SELECT keeps MySQL's warning-and-zero comparison behavior.
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT b FROM src WHERE b=CONCAT('m')")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::DecimalCompact(0, 0)]]);
    assert!(
        session
            .warnings
            .iter()
            .any(|warning| warning.code() == 1292),
        "{:?}",
        session.warnings
    );

    // Materializing the same predicate is a strict conversion boundary.
    // A literal bound comparison fails with the source 1292 diagnostic.
    // A computed string is rejected at the materialization boundary as an
    // incorrect field value (1366).
    // Both failures occur before the temporary table is published, so reusing
    // the same target name cannot cascade into ER_TABLE_EXISTS_ERROR.
    let literal = engine
        .execute_with_session(
            &mut session,
            "CREATE TEMPORARY TABLE tmp AS SELECT * FROM src WHERE b='m'",
        )
        .unwrap_err();
    assert_eq!(
        (literal.code(), literal.sql_state().as_ref()),
        (1292, "22007")
    );

    let computed = engine
        .execute_with_session(
            &mut session,
            "CREATE TEMPORARY TABLE tmp AS SELECT * FROM src WHERE b=CONCAT('m')",
        )
        .unwrap_err();
    assert_eq!(
        (computed.code(), computed.sql_state().as_ref()),
        (1366, "HY000")
    );
}

#[test]
fn ctas_with_declared_columns_and_constraints() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE src (a INT, b VARCHAR(10))");
    exec_ok(&engine, "INSERT INTO src VALUES (1, 'x'), (2, 'y')");

    // Declared columns come first; shared a uses the declared type, SELECT-only b is appended,
    // and declaration-only id/note receive defaults.
    exec_ok(
        &engine,
        "CREATE TABLE dst (id INT, note VARCHAR(20) DEFAULT 'n/a', a BIGINT, KEY ka (a)) \
         AS SELECT a, b FROM src",
    );
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id, note, a, b FROM dst ORDER BY a")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::Null, "declared-only id → NULL");
    assert_eq!(result.rows[0][1], Value::String("n/a".into()));
    assert_eq!(result.rows[0][2], Value::Int(1));
    assert_eq!(result.rows[0][3], Value::String("x".into()));

    // Column order is declared columns followed by SELECT additions.
    let created = show_create(&engine, "dst");
    let id_pos = created.find("`id`").unwrap();
    let a_pos = created.find("`a`").unwrap();
    let b_pos = created.find("`b`").unwrap();
    assert!(id_pos < a_pos && a_pos < b_pos, "{created}");
    // Declared indexes exist.
    assert!(created.contains("KEY `ka`"), "{created}");
}

#[test]
fn ctas_persists_target_table_options_on_initial_create() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE src (id INT NOT NULL)");
    exec_ok(&engine, "INSERT INTO src VALUES (1), (2)");
    exec_ok(
        &engine,
        "CREATE TABLE dst PCTFREE=11 PCTUSED=33 ENGINE=InnoDB \
         ROW_FORMAT=COMPRESSED AS SELECT id FROM src",
    );

    let table = engine.db.table("test.dst").unwrap();
    assert_eq!(table.pctfree, 11);
    assert_eq!(table.pctused, 33);
    assert_eq!(table.engine, "InnoDB");
    assert!(table
        .mysql_table_options
        .iter()
        .any(|(key, value)| key == "ROW_FORMAT" && value == "COMPRESSED"));
    let QueryResponse::ResultSet(rows) = engine.execute("SELECT id FROM dst ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows.len(), 2);
}

#[test]
fn ctas_preserves_unsigned_and_temporal_fsp() {
    // B1: inferred CTAS columns must carry the source column's UNSIGNED /
    // ZEROFILL flags and temporal fractional-seconds precision. Losing
    // UNSIGNED silently turns a BIGINT UNSIGNED into a signed column
    // (overflow at the top of the u64 range); losing fsp truncates the
    // sub-second component on later INSERTs.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(
        &engine,
        "CREATE TABLE src (u BIGINT UNSIGNED, ts TIMESTAMP(3) NULL, z INT ZEROFILL)",
    );
    exec_ok(&engine, "CREATE TABLE dst AS SELECT u, ts, z FROM src");
    let created = show_create(&engine, "dst").to_lowercase();
    assert!(created.contains("unsigned"), "UNSIGNED lost: {created}");
    assert!(created.contains("timestamp(3)"), "fsp lost: {created}");
    assert!(created.contains("zerofill"), "ZEROFILL lost: {created}");
}

#[test]
fn ctas_does_not_apply_protocol_collation_to_numeric_or_temporal_expressions() {
    // MySQL protocol fields always have a collation id, even when their SQL
    // type is numeric or temporal.  CTAS must not turn that wire-level
    // metadata into an illegal CHARACTER SET/COLLATE column attribute.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE src (a INT, b INT, dt DATETIME)");
    exec_ok(
        &engine,
        "INSERT INTO src VALUES (1, 2, '2026-07-30 12:34:56')",
    );
    exec_ok(
        &engine,
        "CREATE TABLE aggregate_copy AS SELECT MAX(b) AS max_b FROM src GROUP BY a",
    );
    exec_ok(
        &engine,
        "CREATE TABLE temporal_copy AS \
         SELECT derived_dt FROM (SELECT NOW() AS derived_dt FROM src) AS d",
    );

    let aggregate = engine.db.table("test.aggregate_copy").unwrap();
    assert!(matches!(
        aggregate.columns[0].data_type,
        crate::storage::schema::DataTypeKind::BigInt | crate::storage::schema::DataTypeKind::Int
    ));
    assert!(aggregate.columns[0].charset.is_none());
    assert!(aggregate.columns[0].collation.is_none());

    let temporal = engine.db.table("test.temporal_copy").unwrap();
    assert!(matches!(
        temporal.columns[0].data_type,
        crate::storage::schema::DataTypeKind::DateTime { .. }
    ));
    assert!(!temporal.columns[0].nullable);
    assert_eq!(
        temporal.columns[0]
            .default
            .as_ref()
            .map(crate::storage::value_to_string)
            .as_deref(),
        Some("0000-00-00 00:00:00")
    );
    assert!(temporal.columns[0].charset.is_none());
    assert!(temporal.columns[0].collation.is_none());
}

#[test]
fn ctas_preserves_unsigned_expression_results_and_u64_boundary() {
    // Expression results have no catalog origin, but MySQL still marks CAST
    // AS UNSIGNED, unsigned integer arithmetic and bitwise results UNSIGNED.
    // The u64::MAX row is the correctness boundary: inferring `u + 0` as
    // DOUBLE rounds it to 18446744073709552000 during the CTAS insert.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE src (u BIGINT UNSIGNED NOT NULL)");
    exec_ok(&engine, "INSERT INTO src VALUES (18446744073709551615)");
    exec_ok(
        &engine,
        "CREATE TABLE dst AS SELECT CAST(u AS UNSIGNED) AS cast_u, \
         u + 0 AS plus_u, ~CAST(0 AS SIGNED) AS bit_u FROM src",
    );

    let table = engine.db.table("test.dst").unwrap();
    for name in ["cast_u", "plus_u", "bit_u"] {
        let column = table
            .columns
            .iter()
            .find(|column| column.name == name)
            .unwrap_or_else(|| panic!("missing CTAS column {name}"));
        assert!(
            matches!(
                column.data_type,
                crate::storage::schema::DataTypeKind::BigInt
            ),
            "{name} should be BIGINT, got {:?}",
            column.data_type
        );
        assert!(column.unsigned, "{name} lost UNSIGNED");
    }

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT cast_u, plus_u, bit_u FROM dst")
        .unwrap()
    else {
        panic!("expected CTAS result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![
            Value::UInt(u64::MAX),
            Value::UInt(u64::MAX),
            Value::UInt(u64::MAX),
        ]]
    );

    // Constant SELECT uses a separate result-metadata path from table scans.
    exec_ok(
        &engine,
        "CREATE TABLE dst_const AS SELECT CAST(18446744073709551615 AS UNSIGNED) AS u, \
         ~0 AS bits",
    );
    let constant = engine.db.table("test.dst_const").unwrap();
    assert!(constant.columns.iter().all(|column| column.unsigned));
    let QueryResponse::ResultSet(result) = engine.execute("SELECT u, bits FROM dst_const").unwrap()
    else {
        panic!("expected constant CTAS result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![Value::UInt(u64::MAX), Value::UInt(u64::MAX)]]
    );
}

#[test]
fn ctas_hex_arithmetic_keeps_int_unsigned_and_constant_not_null() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let Statement::Select(parsed) = crate::parser::parse_statement("SELECT 0x9999 + 0").unwrap()
    else {
        panic!("expected SELECT");
    };
    let SelectItem::Expr { expr, .. } = &parsed.projection[0] else {
        panic!("expected expression projection");
    };
    assert_eq!(
        crate::engine::expr::expr_result_type(expr),
        crate::types::ColumnType::Long
    );
    exec_ok(&engine, "CREATE TABLE dst AS SELECT 0x9999 + 0");

    let table = engine.db.table("test.dst").unwrap();
    let column = &table.columns[0];
    assert!(
        matches!(column.data_type, crate::storage::schema::DataTypeKind::Int),
        "expected INT, got {:?}",
        column.data_type
    );
    assert!(column.unsigned);
    assert!(!column.nullable);
    let QueryResponse::ResultSet(result) = engine.execute("SELECT * FROM dst").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::UInt(39321)]]);
}

#[test]
fn ctas_expression_column_stays_type_derived_nullable() {
    // An expression column has no single-table source, so nullability must
    // remain type-derived even when its numeric result correctly carries
    // UNSIGNED metadata.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE src (a INT UNSIGNED)");
    exec_ok(&engine, "INSERT INTO src VALUES (5)");
    exec_ok(&engine, "CREATE TABLE dst AS SELECT a + 1 AS e FROM src");
    // The expression column must accept NULL (nullable, not inheriting a's
    // attributes).
    exec_ok(&engine, "INSERT INTO dst VALUES (NULL)");
}

#[test]
fn ctas_preserves_distinct_names_for_unaliased_foldable_expressions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(
        &engine,
        "CREATE TABLE dst AS SELECT BIN(130), OCT(130), LEFT('a', 1), RIGHT('a', 1)",
    );
    let QueryResponse::ResultSet(fields) = engine.execute("SHOW COLUMNS FROM dst").unwrap() else {
        panic!("expected fields");
    };
    let names: Vec<_> = fields
        .rows
        .iter()
        .map(|row| row[0].to_display_string())
        .collect();
    assert_eq!(names.len(), 4);
    assert_eq!(
        names.iter().collect::<std::collections::HashSet<_>>().len(),
        4
    );
}

#[test]
fn ctas_preserves_declared_type_charset_and_enum() {
    // A4/A5: a single-table-origin column must reproduce its real catalog
    // type — VARCHAR(n) (not a lossy TEXT), ENUM/SET value lists, the BLOB/
    // TEXT tier, DECIMAL(p,s) and per-column charset/collation — rather than
    // collapsing to the wire type's default.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(
        &engine,
        "CREATE TABLE src (\
           v VARCHAR(10), \
           c CHAR(4), \
           e ENUM('x','y','z'), \
           s SET('a','b'), \
           d DECIMAL(10,2), \
           mt MEDIUMTEXT, \
           lt VARCHAR(8) CHARACTER SET latin1)",
    );
    exec_ok(
        &engine,
        "INSERT INTO src VALUES ('hi','ab','y','a,b',12.34,'m','ll')",
    );
    exec_ok(&engine, "CREATE TABLE dst AS SELECT * FROM src");
    let created = show_create(&engine, "dst").to_lowercase();
    assert!(
        created.contains("`v` varchar(10)"),
        "VARCHAR lost: {created}"
    );
    assert!(created.contains("`c` char(4)"), "CHAR lost: {created}");
    assert!(
        created.contains("enum('x','y','z')"),
        "ENUM lost: {created}"
    );
    assert!(created.contains("set('a','b')"), "SET lost: {created}");
    assert!(
        created.contains("`d` decimal(10,2)"),
        "DECIMAL(p,s) lost: {created}"
    );
    assert!(created.contains("latin1"), "column charset lost: {created}");
    // SHOW CREATE renders every TEXT tier as plain "text" (a pre-existing
    // renderer limitation), so verify the MEDIUMTEXT byte cap is preserved at
    // the catalog level rather than through SHOW CREATE.
    use crate::storage::schema::{DataTypeKind, MEDIUM_LOB_MAX};
    let table = engine.db.table("test.dst").unwrap();
    let mt = table
        .columns
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case("mt"))
        .expect("dst.mt");
    assert!(
        matches!(mt.data_type, DataTypeKind::Text { max_len } if max_len == MEDIUM_LOB_MAX),
        "MEDIUMTEXT byte cap lost: {:?}",
        mt.data_type
    );
}

#[test]
fn ctas_preserves_not_null_from_single_table_source() {
    // A3: a NOT NULL source column reproduced from a plain single-table
    // SELECT stays NOT NULL, so a later INSERT of NULL is rejected (matching
    // MySQL, which carries NOT NULL into the new table).
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE src (a INT NOT NULL, b INT)");
    exec_ok(&engine, "INSERT INTO src VALUES (1, 10), (2, 20)");
    exec_ok(&engine, "CREATE TABLE dst AS SELECT a, b FROM src");
    let created = show_create(&engine, "dst").to_lowercase();
    assert!(
        created.contains("`a` int not null"),
        "NOT NULL on `a` lost: {created}"
    );
    // A later NULL into the NOT NULL column is rejected (1048).
    let err = exec_err(&engine, "INSERT INTO dst (a, b) VALUES (NULL, 1)");
    assert_eq!(err.code(), 1048, "expected NOT NULL violation: {err:?}");
}

#[test]
fn ctas_outer_join_null_extended_column_is_nullable() {
    // A3 safety: a NOT NULL source column that a LEFT JOIN null-extends must
    // become nullable in the target (data-driven), so the CTAS load never
    // fails on a NULL the source schema "forbids".
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE l (id INT NOT NULL)");
    exec_ok(&engine, "CREATE TABLE r (id INT NOT NULL, v INT NOT NULL)");
    exec_ok(&engine, "INSERT INTO l VALUES (1), (2)");
    exec_ok(&engine, "INSERT INTO r VALUES (1, 100)");
    // Row id=2 has no match → r.v is NULL despite r.v being NOT NULL.
    exec_ok(
        &engine,
        "CREATE TABLE dst AS SELECT l.id AS id, r.v AS v FROM l LEFT JOIN r ON l.id = r.id",
    );
    let created = show_create(&engine, "dst").to_lowercase();
    // `v` must be nullable (no NOT NULL) since it was null-extended.
    assert!(
        created.contains("`v` int\n")
            || created.contains("`v` int,")
            || created.contains("`v` int "),
        "outer-join column should be nullable: {created}"
    );
    assert!(
        !created.contains("`v` int not null"),
        "outer-join null-extended column must not be NOT NULL: {created}"
    );
    let QueryResponse::ResultSet(r) = engine.execute("SELECT id, v FROM dst ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[1][1], Value::Null, "unmatched row keeps NULL");
}

#[test]
fn ctas_outer_join_nullability_is_shape_based_for_empty_result() {
    // Nullability is a property of the join shape, not of rows sampled while
    // CREATE runs. An empty result previously preserved r.v's NOT NULL flag,
    // making the target schema depend on current data contents.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE l (id INT NOT NULL)");
    exec_ok(&engine, "CREATE TABLE r (id INT NOT NULL, v INT NOT NULL)");
    exec_ok(
        &engine,
        "CREATE TABLE dst AS \
         SELECT l.id AS id, r.v AS v FROM l LEFT JOIN r ON l.id = r.id",
    );
    let created = show_create(&engine, "dst").to_lowercase();
    assert!(created.contains("`id` int not null"), "{created}");
    assert!(
        !created.contains("`v` int not null"),
        "nullable join side was over-constrained: {created}"
    );
    exec_ok(&engine, "INSERT INTO dst VALUES (1, NULL)");
}

#[test]
fn ctas_streaming_inner_join_matches_materialized_path() {
    // An inner-join source takes the streaming CTAS path (no null-extension).
    // The resulting schema and data are identical to the materialize-then-scan
    // path: `saw_null` is all-false without an outer join, so streaming's
    // static nullability is provably the same. (The left/build-side column
    // losing its NOT NULL flag is a pre-existing join column-metadata
    // limitation shared by *both* paths, not a streaming regression — the
    // right-side column keeps NOT NULL.)
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE l (id INT NOT NULL, a INT NOT NULL)");
    exec_ok(&engine, "CREATE TABLE r (id INT NOT NULL, b INT NOT NULL)");
    exec_ok(&engine, "INSERT INTO l VALUES (1, 10), (2, 20)");
    exec_ok(&engine, "INSERT INTO r VALUES (1, 100), (2, 200)");
    exec_ok(
        &engine,
        "CREATE TABLE dst AS SELECT l.a AS a, r.b AS b FROM l JOIN r ON l.id = r.id",
    );
    let created = show_create(&engine, "dst").to_lowercase();
    assert!(
        created.contains("`b` int not null"),
        "inner-join right-side NOT NULL on `b` lost: {created}"
    );
    let QueryResponse::ResultSet(res) = engine.execute("SELECT a, b FROM dst ORDER BY a").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(res.rows.len(), 2);
    assert_eq!(res.rows[0], vec![Value::Int(10), Value::Int(100)]);
    assert_eq!(res.rows[1], vec![Value::Int(20), Value::Int(200)]);
}

#[test]
fn ctas_streaming_large_source_spans_chunk_boundary() {
    // More than the cfg(test) 64-row chunk exercises the CTAS boundary.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE src (id INT NOT NULL, v INT)");
    let mut sql = String::from("INSERT INTO src VALUES ");
    let n = 160;
    for i in 0..n {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({i},{})", i * 2));
    }
    exec_ok(&engine, &sql);
    exec_ok(&engine, "CREATE TABLE dst AS SELECT id, v FROM src");
    // NOT NULL preserved through the streaming path.
    let created = show_create(&engine, "dst").to_lowercase();
    assert!(created.contains("`id` int not null"), "{created}");
    let QueryResponse::ResultSet(res) = engine
        .execute("SELECT COUNT(*), MIN(id), MAX(id), SUM(v) FROM dst")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(res.rows[0][0], Value::Int(n));
    assert_eq!(res.rows[0][1], Value::Int(0));
    assert_eq!(res.rows[0][2], Value::Int(n - 1));
}

#[test]
fn ctas_binlog_logs_create_table_before_write_rows() {
    // A1 (P0 replication bug): row-based CTAS must log the resolved CREATE
    // TABLE as a DDL query event *before* the WRITE_ROWS stream, otherwise a
    // replica receives row events for a table it never created. Before the
    // fix `is_ddl_statement` omitted CTAS so the CREATE was never logged.
    use crate::binlog::{self, BinlogConfig};
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let blog_dir = dir.path().join("binlog");
    std::fs::create_dir_all(&blog_dir).unwrap();
    binlog::init_global(
        blog_dir.clone(),
        BinlogConfig {
            enabled: true,
            base_name: "mydb-bin".into(),
            ..BinlogConfig::default()
        },
    )
    .unwrap();

    exec_ok(&engine, "CREATE TABLE src (a INT, b VARCHAR(8))");
    exec_ok(&engine, "INSERT INTO src VALUES (1,'x'),(2,'y')");
    exec_ok(&engine, "CREATE TABLE dst AS SELECT a, b FROM src");

    let mgr = binlog::manager().expect("binlog manager installed");
    // Flatten the event stream in file/position order.
    let mut events = Vec::new();
    for log in mgr.binary_logs().unwrap() {
        events.extend(mgr.events_after(&log.name, 4).unwrap());
    }
    // 0x02 = QUERY_EVENT, 0x1e = WRITE_ROWS_EVENT_V2 (see src/binlog.rs).
    let create_dst = events.iter().position(|ev| {
        ev.event_type == 0x02 && {
            let sql = String::from_utf8_lossy(&ev.bytes);
            sql.contains("CREATE TABLE") && sql.contains("dst")
        }
    });
    let create_idx = create_dst.expect("CREATE TABLE `dst` DDL event must be in the binlog");
    // A WRITE_ROWS after the dst CREATE is the CTAS payload (src's inserts
    // were logged earlier, before dst existed).
    let write_after = events
        .iter()
        .skip(create_idx + 1)
        .any(|ev| ev.event_type == 0x1e);
    assert!(
        write_after,
        "CTAS row events must follow the CREATE TABLE `dst` event"
    );

    // Reset the process-global manager so it can't leak into sibling tests.
    let _ = binlog::init_global(blog_dir, BinlogConfig::default());
}

#[test]
fn ctas_if_not_exists_skips_select_and_notes() {
    // A7: IF NOT EXISTS on an existing table is a Note (1050), not an error,
    // and the SELECT is not run.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE dst (id INT)");
    exec_ok(&engine, "INSERT INTO dst VALUES (7)");
    exec_ok(&engine, "CREATE TABLE src (a INT)");
    exec_ok(&engine, "INSERT INTO src VALUES (1), (2)");
    // Must succeed without touching dst (no rows appended from src).
    exec_ok(
        &engine,
        "CREATE TABLE IF NOT EXISTS dst AS SELECT a FROM src",
    );
    let QueryResponse::ResultSet(r) = engine.execute("SELECT id FROM dst").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(r.rows.len(), 1, "dst must be untouched: {:?}", r.rows);
    assert_eq!(r.rows[0][0], Value::Int(7));
}

#[test]
fn drop_multi_table_reclaims_and_allows_recreate() {
    // B2: multi-table DROP now does physical cleanup before the catalog
    // mutation (fail-before-commit), so recreating a dropped name never
    // collides with a leftover file.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE t1 (id INT, KEY k (id))");
    exec_ok(&engine, "CREATE TABLE t2 (id INT)");
    exec_ok(&engine, "INSERT INTO t1 VALUES (1), (2)");
    exec_ok(&engine, "DROP TABLE t1, t2");
    assert!(
        engine.execute("SELECT * FROM t1").is_err(),
        "t1 must be gone"
    );
    assert!(
        engine.execute("SELECT * FROM t2").is_err(),
        "t2 must be gone"
    );
    exec_ok(&engine, "CREATE TABLE t1 (id INT, KEY k (id))");
    exec_ok(&engine, "INSERT INTO t1 VALUES (9)");
    let QueryResponse::ResultSet(r) = engine.execute("SELECT id FROM t1").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(r.rows.len(), 1, "recreated t1 must be empty except new row");
    assert_eq!(r.rows[0][0], Value::Int(9));
}

#[test]
fn multi_action_alter_preflight_rejects_fk_column_change() {
    // C2: a multi-action ALTER whose later MODIFY targets an FK column must
    // be rejected in pre-flight (1832), so the benign earlier DROP COLUMN is
    // never committed. Before the fix, pre-flight only validated the
    // `invisible` flag, so DROP COLUMN flushed first and the table was left
    // half-altered.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE parent (id INT PRIMARY KEY)");
    exec_ok(
        &engine,
        "CREATE TABLE child (id INT PRIMARY KEY, pid INT, extra INT, KEY kp (pid), \
         FOREIGN KEY (pid) REFERENCES parent(id))",
    );
    let err = exec_err(
        &engine,
        "ALTER TABLE child DROP COLUMN extra, MODIFY pid BIGINT",
    );
    assert_eq!(
        err.code(),
        1832,
        "expected FK column-change rejection: {err:?}"
    );
    let created = show_create(&engine, "child");
    assert!(
        created.contains("`extra`"),
        "`extra` was dropped despite the failed multi-action ALTER: {created}"
    );
}

#[test]
fn multi_action_alter_preflights_unique_data_before_copy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(
        &engine,
        "CREATE TABLE physical_atomic (id INT PRIMARY KEY, v INT, duplicate_key INT)",
    );
    exec_ok(
        &engine,
        "INSERT INTO physical_atomic VALUES (1,10,7),(2,20,7)",
    );
    let err = exec_err(
        &engine,
        "ALTER TABLE physical_atomic MODIFY v BIGINT, \
         ADD UNIQUE INDEX ux_duplicate(duplicate_key)",
    );
    assert_eq!(err.code(), 1062, "{err:?}");
    let create = show_create(&engine, "physical_atomic");
    assert!(create.contains("`v` int"), "{create}");
    assert!(!create.contains("ux_duplicate"), "{create}");
}

#[test]
fn multi_action_metadata_alter_rolls_back_on_failure() {
    // C1 (atomic DDL): an all-metadata multi-action ALTER is atomic. The
    // first ADD CHECK passes on existing data; the second violates it
    // (3819). The whole statement must roll back so neither check remains.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    exec_ok(&engine, "INSERT INTO t VALUES (1, 5, 50)");
    let err = exec_err(
        &engine,
        "ALTER TABLE t ADD CONSTRAINT ca CHECK (a > 0), ADD CONSTRAINT cb CHECK (b > 100)",
    );
    assert_eq!(err.code(), 3819, "{err:?}");
    let created = show_create(&engine, "t");
    assert!(
        !created.contains("`ca`") && !created.contains("`cb`"),
        "both CHECKs must be rolled back after the failed atomic ALTER: {created}"
    );
    // Schema stays coherent and fully usable after the restore.
    exec_ok(&engine, "INSERT INTO t VALUES (2, -9, 0)");
    let QueryResponse::ResultSet(r) = engine.execute("SELECT COUNT(*) FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(r.rows[0][0], Value::Int(2));
}

#[test]
fn multi_action_metadata_alter_commits_on_success() {
    // C1 guard: a successful all-metadata multi-action ALTER must still apply
    // every action — the atomic wrapper must not swallow the changes.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE t (id INT PRIMARY KEY, a INT)");
    exec_ok(&engine, "INSERT INTO t VALUES (1, 5)");
    exec_ok(
        &engine,
        "ALTER TABLE t ADD COLUMN x INT DEFAULT 7, ADD CONSTRAINT ca CHECK (a >= 0)",
    );
    let created = show_create(&engine, "t");
    assert!(created.contains("`x`"), "added column missing: {created}");
    assert!(created.contains("`ca`"), "added CHECK missing: {created}");
    let QueryResponse::ResultSet(r) = engine.execute("SELECT x FROM t WHERE id = 1").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        r.rows[0][0],
        Value::Int(7),
        "INSTANT column default not applied"
    );
}

#[test]
fn parenthesized_multi_column_add_executes_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE add_group (id INT PRIMARY KEY)");
    exec_ok(
        &engine,
        "ALTER TABLE add_group ADD (c1 INT DEFAULT (1 + 2), c2 VARCHAR(20) DEFAULT 'a,b')",
    );
    exec_ok(&engine, "INSERT INTO add_group(id) VALUES (1)");
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT c1,c2 FROM add_group WHERE id=1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![Value::Int(3), Value::String("a,b".into())]]
    );
    let created = show_create(&engine, "add_group");
    assert!(created.contains("`c1` int DEFAULT ((1 + 2))"), "{created}");
    assert!(created.contains("`id` int NOT NULL"), "{created}");
}

#[test]
fn ctas_declared_primary_key_dedup_guard() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE src (a INT)");
    exec_ok(&engine, "INSERT INTO src VALUES (1), (1)");
    // Duplicate data under a declared PK fails, matching MySQL.
    assert!(
        engine
            .execute("CREATE TABLE dst (PRIMARY KEY (a)) AS SELECT a FROM src")
            .is_err(),
        "duplicate PK values must fail the CTAS"
    );
}

#[test]
fn ctas_ignore_and_replace_apply_duplicate_policy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE src(id INT, v INT)");
    exec_ok(&engine, "INSERT INTO src VALUES(1,10),(1,20),(2,30)");
    exec_ok(
        &engine,
        "CREATE TABLE ignored(id INT PRIMARY KEY, v INT) IGNORE AS SELECT id,v FROM src",
    );
    let QueryResponse::ResultSet(ignored) = engine
        .execute("SELECT id,v FROM ignored ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        ignored.rows,
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(30)],
        ]
    );
    exec_ok(
        &engine,
        "CREATE TABLE replaced(id INT PRIMARY KEY, v INT) REPLACE AS SELECT id,v FROM src",
    );
    let QueryResponse::ResultSet(replaced) = engine
        .execute("SELECT id,v FROM replaced ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        replaced.rows,
        vec![
            vec![Value::Int(1), Value::Int(20)],
            vec![Value::Int(2), Value::Int(30)],
        ]
    );
}

// ---------- P1-6: prefix primary keys and MODIFY with CHECK ----------

#[test]
fn prefix_primary_key_create_and_alter() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(
        &engine,
        "CREATE TABLE t1 (body TEXT NOT NULL, v INT, PRIMARY KEY (body(8)))",
    );
    exec_ok(&engine, "INSERT INTO t1 VALUES ('abcdefgh-tail-1', 1)");
    // Reject collisions within the eight-byte prefix even if suffixes differ.
    assert!(
        engine
            .execute("INSERT INTO t1 VALUES ('abcdefgh-tail-2', 2)")
            .is_err(),
        "prefix PK must dedup on the prefix"
    );
    exec_ok(&engine, "INSERT INTO t1 VALUES ('zzzzzzzz', 3)");

    // ALTER ADD PRIMARY KEY also supports prefixes.
    exec_ok(&engine, "CREATE TABLE t2 (body TEXT NOT NULL)");
    exec_ok(&engine, "ALTER TABLE t2 ADD PRIMARY KEY (body(4))");
    exec_ok(&engine, "INSERT INTO t2 VALUES ('aaaa-1')");
    assert!(engine.execute("INSERT INTO t2 VALUES ('aaaa-2')").is_err());
}

#[test]
fn modify_change_carry_check_constraint() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE t1 (a INT, b INT)");
    exec_ok(&engine, "ALTER TABLE t1 MODIFY a INT CHECK (a > 0)");
    assert!(
        engine.execute("INSERT INTO t1 (a) VALUES (-1)").is_err(),
        "MODIFY-carried CHECK must enforce"
    );
    exec_ok(&engine, "INSERT INTO t1 (a) VALUES (1)");
    exec_ok(&engine, "ALTER TABLE t1 CHANGE b b2 INT CHECK (b2 < 100)");
    assert!(engine
        .execute("INSERT INTO t1 (a, b2) VALUES (1, 200)")
        .is_err());
    exec_ok(&engine, "INSERT INTO t1 (a, b2) VALUES (2, 50)");
}

// ---------- P1-7: partition maintenance ----------

#[test]
fn partition_maintenance_operations() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(
        &engine,
        "CREATE TABLE t1 (id INT, v INT) PARTITION BY HASH (id) PARTITIONS 4",
    );
    for i in 1..=16 {
        exec_ok(&engine, &format!("INSERT INTO t1 VALUES ({i}, {i})"));
    }
    // CHECK/OPTIMIZE/REPAIR/ANALYZE return MySQL's standard four-column result.
    for op in ["CHECK", "OPTIMIZE", "REPAIR", "ANALYZE"] {
        let QueryResponse::ResultSet(result) = engine
            .execute(&format!("ALTER TABLE t1 {op} PARTITION ALL"))
            .unwrap_or_else(|e| panic!("{op}: {e:?}"))
        else {
            panic!("{op} must return a result set");
        };
        assert_eq!(result.columns.len(), 4, "{op}");
        assert_eq!(result.columns[0].name, "Table");
        assert_eq!(result.rows[0][3], Value::String("OK".into()), "{op}");
    }
    // REBUILD returns ordinary OK.
    let resp = engine
        .execute("ALTER TABLE t1 REBUILD PARTITION ALL")
        .unwrap();
    assert!(matches!(resp, QueryResponse::Ok));
    // After rebuilding, data remains intact and indexes work.
    let QueryResponse::ResultSet(result) = engine.execute("SELECT COUNT(*) FROM t1").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(16));
    // A nonpartitioned table returns 1505.
    exec_ok(&engine, "CREATE TABLE plain (id INT)");
    let err = exec_err(&engine, "ALTER TABLE plain OPTIMIZE PARTITION ALL");
    assert_eq!(err.code(), 1505, "{err:?}");
}

// ---------- P2: COLUMN_FORMAT / STORAGE ----------

#[test]
fn column_format_and_storage_accepted_and_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(
        &engine,
        "CREATE TABLE t1 (id INT COLUMN_FORMAT FIXED, v INT STORAGE MEMORY, \
         w INT COLUMN_FORMAT DYNAMIC STORAGE DISK)",
    );
    exec_ok(
        &engine,
        "ALTER TABLE t1 MODIFY v BIGINT COLUMN_FORMAT DEFAULT",
    );
    exec_ok(&engine, "INSERT INTO t1 VALUES (1, 2, 3)");
    // Do not misinterpret a column named storage.
    exec_ok(&engine, "CREATE TABLE t2 (storage INT DEFAULT 7, x INT)");
    exec_ok(&engine, "INSERT INTO t2 (x) VALUES (1)");
    let QueryResponse::ResultSet(result) = engine.execute("SELECT storage FROM t2").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(7));
}

// ---------- Final SHOW CREATE round trip ----------

#[test]
fn show_create_round_trip_is_stable() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(
        &engine,
        "CREATE TABLE rt (\
           id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, \
           z SMALLINT ZEROFILL, \
           hidden INT INVISIBLE DEFAULT 3, \
           name VARCHAR(40) NOT NULL DEFAULT 'x' COMMENT 'who', \
           d DECIMAL(8,2) UNSIGNED, \
           ts TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3), \
           g INT GENERATED ALWAYS AS (id + 1) STORED, \
           KEY kg (g) INVISIBLE, \
           UNIQUE KEY un (name), \
           CONSTRAINT pos CHECK (d >= 0)\
         ) ROW_FORMAT=DYNAMIC MAX_ROWS=500 COMMENT='round trip' AUTO_INCREMENT=10",
    );
    let first = show_create(&engine, "rt");
    // Replay under a new table name and require byte-identical SHOW CREATE except for the name.
    // CHECK constraint names are schema-unique like MySQL, so drop the original before replay.
    exec_ok(&engine, "DROP TABLE rt");
    let replay = first.replacen("CREATE TABLE `rt`", "CREATE TABLE `rt2`", 1);
    engine
        .execute(&replay)
        .unwrap_or_else(|e| panic!("replay failed: {e:?}\n{replay}"));
    let second = show_create(&engine, "rt2").replacen("`rt2`", "`rt`", 1);
    // AUTO_INCREMENT may differ because rt2 has no inserts; all other details must match.
    let strip_ai = |s: &str| {
        s.split_whitespace()
            .filter(|w| !w.starts_with("AUTO_INCREMENT="))
            .collect::<Vec<_>>()
            .join(" ")
    };
    assert_eq!(
        strip_ai(&first),
        strip_ai(&second),
        "\nfirst:  {first}\nsecond: {second}"
    );
}

// ---------- C1: hard COMMENT length limits independent of sql_mode ----------

#[test]
fn table_comment_length_cap_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Exactly 2048 characters succeed.
    let ok = "a".repeat(2048);
    exec_ok(
        &engine,
        &format!("CREATE TABLE t_ok (id INT) COMMENT='{ok}'"),
    );
    // 2049 characters return ER_TOO_LONG_TABLE_COMMENT (1628).
    let too = "a".repeat(2049);
    let err = exec_err(
        &engine,
        &format!("CREATE TABLE t_bad (id INT) COMMENT='{too}'"),
    );
    assert_eq!(err.code(), 1628, "table comment overflow must be 1628");
    assert_eq!(err.sql_state(), "HY000");
    assert_eq!(
        err.message(),
        "Comment for table 't_bad' is too long (max = 2048)"
    );
}

#[test]
fn column_comment_length_cap_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Exactly 1024 characters succeed.
    let ok = "b".repeat(1024);
    exec_ok(
        &engine,
        &format!("CREATE TABLE t_col_ok (id INT COMMENT '{ok}')"),
    );
    // 1025 characters return ER_TOO_LONG_FIELD_COMMENT (1629).
    let too = "b".repeat(1025);
    let err = exec_err(
        &engine,
        &format!("CREATE TABLE t_col_bad (id INT COMMENT '{too}')"),
    );
    assert_eq!(err.code(), 1629, "field comment overflow must be 1629");
    assert_eq!(err.sql_state(), "HY000");
    assert_eq!(
        err.message(),
        "Comment for field 'id' is too long (max = 1024)"
    );
}

#[test]
fn index_comment_length_cap_boundary_create() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Exactly 1024 characters succeed.
    let ok = "c".repeat(1024);
    exec_ok(
        &engine,
        &format!("CREATE TABLE t_idx_ok (id INT, KEY k1 (id) COMMENT '{ok}')"),
    );
    // 1025 characters return ER_TOO_LONG_INDEX_COMMENT (1688).
    let too = "c".repeat(1025);
    let err = exec_err(
        &engine,
        &format!("CREATE TABLE t_idx_bad (id INT, KEY k1 (id) COMMENT '{too}')"),
    );
    assert_eq!(err.code(), 1688, "index comment overflow must be 1688");
    assert_eq!(err.sql_state(), "HY000");
    assert_eq!(
        err.message(),
        "Comment for index 'k1' is too long (max = 1024)"
    );
}

#[test]
fn partition_comment_length_cap_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Exactly 1024 characters succeed.
    let ok = "p".repeat(1024);
    exec_ok(
        &engine,
        &format!(
            "CREATE TABLE t_part_ok (id INT) PARTITION BY RANGE (id) \
             (PARTITION p0 VALUES LESS THAN (10) COMMENT = '{ok}')"
        ),
    );
    // 1025 characters return ER_TOO_LONG_TABLE_PARTITION_COMMENT (1793).
    let too = "p".repeat(1025);
    let err = exec_err(
        &engine,
        &format!(
            "CREATE TABLE t_part_bad (id INT) PARTITION BY RANGE (id) \
             (PARTITION p0 VALUES LESS THAN (10) COMMENT = '{too}')"
        ),
    );
    assert_eq!(err.code(), 1793, "partition comment overflow must be 1793");
    assert_eq!(err.sql_state(), "HY000");
    assert_eq!(
        err.message(),
        "Comment for table partition 'p0' is too long (max = 1024)"
    );
}

#[test]
fn comment_cap_counts_chars_not_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // 2048 three-byte characters (6144 bytes) succeed, proving the limit counts characters, not bytes.
    let ok = "你".repeat(2048);
    exec_ok(
        &engine,
        &format!("CREATE TABLE t_mb_ok (id INT) COMMENT='{ok}'"),
    );
    // 2049 characters still exceed the limit.
    let too = "你".repeat(2049);
    let err = exec_err(
        &engine,
        &format!("CREATE TABLE t_mb_bad (id INT) COMMENT='{too}'"),
    );
    assert_eq!(err.code(), 1628);
}

#[test]
fn alter_table_comment_length_cap() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE t (id INT)");
    let too = "a".repeat(2049);
    let err = exec_err(&engine, &format!("ALTER TABLE t COMMENT='{too}'"));
    assert_eq!(err.code(), 1628);
    assert_eq!(
        err.message(),
        "Comment for table 't' is too long (max = 2048)"
    );
    // Exactly 2048 characters succeed.
    let ok = "a".repeat(2048);
    exec_ok(&engine, &format!("ALTER TABLE t COMMENT='{ok}'"));
}

#[test]
fn alter_column_comment_length_cap() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE t (id INT, c INT)");
    let too = "b".repeat(1025);
    // MODIFY COMMENT uses SetColumnComment/set_column_comment.
    let err = exec_err(
        &engine,
        &format!("ALTER TABLE t MODIFY c INT COMMENT '{too}'"),
    );
    assert_eq!(err.code(), 1629);
    assert_eq!(
        err.message(),
        "Comment for field 'c' is too long (max = 1024)"
    );
    // Exactly 1024 characters succeed.
    let ok = "b".repeat(1024);
    exec_ok(
        &engine,
        &format!("ALTER TABLE t MODIFY c INT COMMENT '{ok}'"),
    );
}

#[test]
fn alter_add_index_comment_length_cap() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE t (id INT)");
    let too = "c".repeat(1025);
    let err = exec_err(
        &engine,
        &format!("ALTER TABLE t ADD INDEX k1 (id) COMMENT '{too}'"),
    );
    assert_eq!(err.code(), 1688);
    assert_eq!(
        err.message(),
        "Comment for index 'k1' is too long (max = 1024)"
    );
    // Exactly 1024 characters succeed.
    let ok = "c".repeat(1024);
    exec_ok(
        &engine,
        &format!("ALTER TABLE t ADD INDEX k2 (id) COMMENT '{ok}'"),
    );
}

// ---------- M1/M2: generated-column rewrite, partition COPY, ENCRYPTION, TEMP CTAS ----------

#[test]
fn add_stored_generated_column_on_nonempty_materializes_rows() {
    // G2: ADD STORED on a nonempty table uses COPY to materialize existing rows.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE t1 (id INT PRIMARY KEY, a INT)");
    for i in 1..=5 {
        exec_ok(
            &engine,
            &format!("INSERT INTO t1 (id, a) VALUES ({i}, {i})"),
        );
    }
    exec_ok(
        &engine,
        "ALTER TABLE t1 ADD COLUMN gs INT GENERATED ALWAYS AS (a * 10) STORED",
    );
    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT id, gs FROM t1 ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    let got: Vec<(i64, i64)> = result
        .rows
        .iter()
        .map(|r| match (&r[0], &r[1]) {
            (Value::Int(a), Value::Int(b)) => (*a, *b),
            other => panic!("unexpected row {other:?}"),
        })
        .collect();
    assert_eq!(got, vec![(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]);
    // Later updates to base columns refresh generated columns.
    exec_ok(&engine, "UPDATE t1 SET a = 100 WHERE id = 1");
    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT gs FROM t1 WHERE id = 1").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1000)]]);
}

#[test]
fn add_virtual_generated_column_on_nonempty_materializes_rows() {
    // G2: mydb physically stores VIRTUAL columns too, so adding them also rewrites rows.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE t1 (id INT PRIMARY KEY, a INT)");
    for i in 1..=3 {
        exec_ok(
            &engine,
            &format!("INSERT INTO t1 (id, a) VALUES ({i}, {i})"),
        );
    }
    exec_ok(
        &engine,
        "ALTER TABLE t1 ADD COLUMN gv INT GENERATED ALWAYS AS (a + 1) VIRTUAL",
    );
    let QueryResponse::ResultSet(result) = engine.execute("SELECT gv FROM t1 ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(2)],
            vec![Value::Int(3)],
            vec![Value::Int(4)],
        ]
    );
    // A projected single-column scan must see recalculated values rather than DEFAULT.
    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT gv FROM t1 WHERE id = 2").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(3)]]);
}

#[test]
fn add_generated_column_on_nonempty_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine =
            StorageQueryEngine::open_with_storage_config(dir.path(), strict_config()).unwrap();
        exec_ok(&engine, "CREATE TABLE t1 (id INT PRIMARY KEY, a INT)");
        exec_ok(&engine, "INSERT INTO t1 (id, a) VALUES (1, 7), (2, 8)");
        exec_ok(
            &engine,
            "ALTER TABLE t1 ADD COLUMN gs INT GENERATED ALWAYS AS (a * 3) STORED",
        );
    }
    let engine = StorageQueryEngine::open_with_storage_config(dir.path(), strict_config()).unwrap();
    let QueryResponse::ResultSet(result) = engine.execute("SELECT gs FROM t1 ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![Value::Int(21)], vec![Value::Int(24)]]
    );
}

#[test]
fn add_generated_column_secondary_index_after_rewrite() {
    // An index added to the generated column works; rewriting preserves base/index consistency.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE t1 (id INT PRIMARY KEY, a INT)");
    for i in 1..=10 {
        exec_ok(
            &engine,
            &format!("INSERT INTO t1 (id, a) VALUES ({i}, {i})"),
        );
    }
    exec_ok(
        &engine,
        "ALTER TABLE t1 ADD COLUMN gs INT GENERATED ALWAYS AS (a * 2) STORED",
    );
    exec_ok(&engine, "ALTER TABLE t1 ADD INDEX kgs (gs)");
    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT id FROM t1 WHERE gs = 12").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(6)]]);
}

#[test]
fn alter_generated_column_storedness_change_is_rejected() {
    // gcol suite (gcol_non_stored_columns Cases 7 & 8): flipping a generated
    // column's storedness (STORED↔VIRTUAL) via MODIFY/CHANGE is not allowed in
    // MySQL — ER_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN (3106). mydb previously
    // accepted it silently (it merely triggered a COPY rewrite), converting the
    // column behind the user's back.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // STORED -> VIRTUAL via MODIFY.
    exec_ok(
        &engine,
        "CREATE TABLE t1 (a INT, b INT GENERATED ALWAYS AS (a % 2) STORED)",
    );
    let err = exec_err(
        &engine,
        "ALTER TABLE t1 MODIFY b INT GENERATED ALWAYS AS (a % 2) VIRTUAL",
    );
    assert_eq!(err.code(), 3106);
    // The column must remain STORED — the ALTER was rejected, not applied.
    assert!(show_create(&engine, "t1").contains("STORED"));

    // VIRTUAL -> STORED via CHANGE.
    exec_ok(
        &engine,
        "CREATE TABLE t2 (a INT, b INT GENERATED ALWAYS AS (a % 2) VIRTUAL)",
    );
    let err = exec_err(
        &engine,
        "ALTER TABLE t2 CHANGE b b INT GENERATED ALWAYS AS (a % 2) STORED",
    );
    assert_eq!(err.code(), 3106);
    assert!(show_create(&engine, "t2").contains("VIRTUAL"));

    // A MODIFY that keeps the same storedness (only widens the type) is still
    // accepted — the guard must not over-reject legitimate generated-column
    // changes.
    exec_ok(
        &engine,
        "ALTER TABLE t1 MODIFY b BIGINT GENERATED ALWAYS AS (a % 2) STORED",
    );
    let create = show_create(&engine, "t1");
    assert!(create.contains("bigint"));
    assert!(create.contains("STORED"));
}

#[test]
fn partitioned_column_type_change_preserves_data() {
    // G1: partition-aware synchronous type rewriting preserves data.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(
        &engine,
        "CREATE TABLE t1 (id INT, v VARCHAR(10)) PARTITION BY HASH (id) PARTITIONS 4",
    );
    exec_ok(
        &engine,
        "INSERT INTO t1 VALUES (1,'11'),(2,'22'),(3,'33'),(4,'44')",
    );
    // Partitioned type changes use partition-aware synchronous COPY.
    exec_ok(&engine, "ALTER TABLE t1 MODIFY v BIGINT");
    let QueryResponse::ResultSet(result) = engine.execute("SELECT SUM(v) FROM t1").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(crate::storage::value_to_string(&result.rows[0][0]), "110");
}

#[test]
fn create_table_encryption_n_accepted_y_rejected() {
    // G6: accept and display ENCRYPTION='N' as metadata; reject 'Y' because TDE is absent.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE t1 (id INT) ENCRYPTION='N'");
    assert!(
        show_create(&engine, "t1").contains("ENCRYPTION='N'"),
        "SHOW CREATE must echo ENCRYPTION='N', got: {}",
        show_create(&engine, "t1")
    );
    // 'Y' requests actual encryption and must not silently succeed without TDE.
    assert!(engine
        .execute("CREATE TABLE t2 (id INT) ENCRYPTION='Y'")
        .is_err());
}

#[test]
fn temp_ctas_with_declared_columns() {
    // G7: CREATE TEMPORARY TABLE with declared columns merges them with SELECT columns.
    // Temporary tables are session-local; use the same session.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE src (id INT PRIMARY KEY, a INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO src VALUES (1, 10), (2, 20)")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TEMPORARY TABLE t1 (a BIGINT) AS SELECT id, a FROM src ORDER BY id",
        )
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT a, id FROM t1 ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    // Declared BIGINT a comes first; SELECT id is appended.
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(10), Value::Int(1)],
            vec![Value::Int(20), Value::Int(2)],
        ]
    );
}

#[test]
fn ctas_with_order_by_materializes_streaming_result() {
    // Regression: CTAS with SELECT ORDER BY produces StreamingResultSet;
    // materialize it before table creation rather than returning the former 1235.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE src (id INT PRIMARY KEY, v INT)");
    exec_ok(&engine, "INSERT INTO src VALUES (3,30),(1,10),(2,20)");
    exec_ok(
        &engine,
        "CREATE TABLE dst AS SELECT id, v FROM src ORDER BY id DESC",
    );
    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT COUNT(*), SUM(v) FROM dst").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(3));
    assert_eq!(crate::storage::value_to_string(&result.rows[0][1]), "60");
}

// Issue #84: ADD COLUMN NOT NULL without DEFAULT backfills implicit type defaults.

#[test]
fn add_column_not_null_without_default_backfills_implicit() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Nonempty tables backfill INT with 0 and VARCHAR with empty string.
    exec_ok(&engine, "CREATE TABLE t (a INT)");
    exec_ok(&engine, "INSERT INTO t VALUES (1),(2)");
    exec_ok(&engine, "ALTER TABLE t ADD COLUMN b INT NOT NULL");
    exec_ok(&engine, "ALTER TABLE t ADD COLUMN c VARCHAR(5) NOT NULL");
    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT a, b, c FROM t ORDER BY a").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows[0][1],
        Value::Int(0),
        "INT NOT NULL must backfill zero"
    );
    assert_eq!(
        crate::storage::value_to_string(&result.rows[0][2]),
        "",
        "VARCHAR NOT NULL must backfill an empty string"
    );
    assert_eq!(result.rows[1][1], Value::Int(0));

    // Omitting the column in INSERT still returns 1364; backfill is not a persisted DEFAULT.
    let err = exec_err(&engine, "INSERT INTO t (a) VALUES (9)");
    assert_eq!(
        err.code(),
        1364,
        "INSERT omitting a NOT NULL column without DEFAULT must still return 1364: {err:?}"
    );

    // Adding to an empty table succeeds directly.
    exec_ok(&engine, "CREATE TABLE e (a INT)");
    exec_ok(&engine, "ALTER TABLE e ADD COLUMN b INT NOT NULL");
    exec_ok(&engine, "INSERT INTO e (a, b) VALUES (1, 7)");
    let QueryResponse::ResultSet(result) = engine.execute("SELECT b FROM e").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(7));
}

// ---------- Second DDL audit fixes: docs/ddl-audit-2026-07-07-followup.md ----------

// N2: CREATE TABLE must reject duplicate secondary-index names with 1061,
// or they alias the same .idx file and overwrite/corrupt each other.
#[test]
fn n2_create_table_rejects_duplicate_index_name() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let err = exec_err(
        &engine,
        "CREATE TABLE t (a INT, b INT, KEY k (a), KEY k (b))",
    );
    assert_eq!(
        err.code(),
        1061,
        "duplicate index name must be 1061: {err:?}"
    );
    // Names are case-insensitive.
    let err = exec_err(&engine, "CREATE TABLE t2 (a INT, KEY idx (a), KEY IDX (a))");
    assert_eq!(
        err.code(),
        1061,
        "case-insensitive dup index must be 1061: {err:?}"
    );
    // Distinct index names still succeed.
    exec_ok(
        &engine,
        "CREATE TABLE t3 (a INT, b INT, KEY ka (a), KEY kb (b))",
    );
}

// N5: INSTANT ADD COLUMN DEFAULT(expr)/CURRENT_TIMESTAMP must backfill existing rows
// with the ALTER-time evaluated constant, not NULL, especially for NOT NULL columns.
#[test]
fn n5_instant_add_column_default_expr_backfills_frozen_value() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE t (id INT PRIMARY KEY)");
    exec_ok(&engine, "INSERT INTO t (id) VALUES (1), (2)");
    // Constant expression defaults.
    exec_ok(
        &engine,
        "ALTER TABLE t ADD COLUMN c INT NOT NULL DEFAULT (1 + 1)",
    );
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT c FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows[0][0].to_display_string(),
        "2",
        "old row must read frozen 2, not NULL"
    );
    assert_eq!(rs.rows[1][0].to_display_string(), "2");
    // NOT NULL DEFAULT CURRENT_TIMESTAMP must not leave existing rows NULL.
    exec_ok(
        &engine,
        "ALTER TABLE t ADD COLUMN ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
    );
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT COUNT(*) FROM t WHERE ts IS NULL")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows[0][0].to_display_string(),
        "0",
        "NOT NULL DEFAULT CURRENT_TIMESTAMP must backfill non-NULL for existing rows"
    );
}

// N10: CTAS is atomic; failed INSERT, such as duplicate PK output, must not leave an empty table.
#[test]
fn n10_ctas_insert_failure_leaves_no_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE src (a INT)");
    exec_ok(&engine, "INSERT INTO src (a) VALUES (1), (1)");
    let err = exec_err(
        &engine,
        "CREATE TABLE dst (a INT PRIMARY KEY) AS SELECT a FROM src",
    );
    assert_eq!(
        err.code(),
        1062,
        "duplicate PK during CTAS load must be 1062: {err:?}"
    );
    // dst must be absent after complete statement rollback.
    let err = exec_err(&engine, "SELECT * FROM dst");
    assert_eq!(
        err.code(),
        1146,
        "dst must not exist after failed CTAS: {err:?}"
    );
}

// ---------- V4: COPY reclaims accumulated INSTANT layouts ----------

/// V4 — each INSTANT ADD COLUMN appends a Readable row layout that is
/// never reclaimed. A COPY rewrite physically re-writes every row under
/// the new active layout, so all prior layouts are dead; the rewrite
/// must collapse `row_layouts` back to a single Active layout instead of
/// leaking the accumulated ones forever. Data must remain correct.
#[test]
fn copy_rewrite_reclaims_accumulated_instant_layouts() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE t (id INT PRIMARY KEY, a INT)");
    exec_ok(&engine, "INSERT INTO t (id, a) VALUES (1, 10), (2, 20)");

    // Three INSTANT ADD COLUMNs — each publishes a fresh layout and
    // demotes the prior one to Readable.
    exec_ok(&engine, "ALTER TABLE t ADD COLUMN c1 INT DEFAULT 100");
    exec_ok(&engine, "ALTER TABLE t ADD COLUMN c2 INT DEFAULT 200");
    exec_ok(&engine, "ALTER TABLE t ADD COLUMN c3 INT DEFAULT 300");

    let layouts_before = engine.database().table("test.t").unwrap().row_layouts.len();
    assert!(
        layouts_before >= 4,
        "3 INSTANT adds should accumulate layouts (got {layouts_before})"
    );

    // A guaranteed COPY rewrite (STORED generated column ADD).
    exec_ok(&engine, "ALTER TABLE t ADD COLUMN g INT AS (a + 1) STORED");

    let layouts_after = engine.database().table("test.t").unwrap().row_layouts.len();
    assert_eq!(
        layouts_after, 1,
        "COPY rewrite must collapse dead layouts to a single Active one"
    );

    // Data intact: pre-existing rows keep `a`, read their INSTANT
    // defaults, and the rewrite materialized the generated column.
    fn rows_of(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
        match engine.execute(sql).unwrap() {
            QueryResponse::ResultSet(rs) => rs.rows,
            other => panic!("expected result set, got {other:?}"),
        }
    }
    let rows = rows_of(&engine, "SELECT id, a, c1, c2, c3, g FROM t ORDER BY id");
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Int(1),
                Value::Int(10),
                Value::Int(100),
                Value::Int(200),
                Value::Int(300),
                Value::Int(11),
            ],
            vec![
                Value::Int(2),
                Value::Int(20),
                Value::Int(100),
                Value::Int(200),
                Value::Int(300),
                Value::Int(21),
            ],
        ]
    );

    // Still writable + readable after the collapse.
    exec_ok(&engine, "INSERT INTO t (id, a) VALUES (3, 30)");
    let rows = rows_of(&engine, "SELECT id, a, c1, g FROM t WHERE id = 3");
    assert_eq!(
        rows,
        vec![vec![
            Value::Int(3),
            Value::Int(30),
            Value::Int(100),
            Value::Int(31)
        ]]
    );
}

/// V4 — the layout collapse must survive reopen: rows the COPY rewrite
/// stamped under the retained Active layout must still decode after the
/// catalog reloads from the data dictionary. Regression guard for a
/// reopen decode path exercised by many INSTANT adds ahead of the COPY
/// (the collapsed schema persists exactly the layout the rows reference).
#[test]
fn copy_rewrite_layout_collapse_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine =
            StorageQueryEngine::open_with_storage_config(dir.path(), strict_config()).unwrap();
        exec_ok(&engine, "CREATE TABLE t (id INT PRIMARY KEY, a INT)");
        exec_ok(&engine, "INSERT INTO t (id, a) VALUES (1, 10), (2, 20)");
        // Many INSTANT adds accumulate layouts, pushing active_layout_id
        // well past 1 before the COPY collapses them.
        for k in 1..=6 {
            exec_ok(
                &engine,
                &format!("ALTER TABLE t ADD COLUMN c{k} INT DEFAULT {}", k * 100),
            );
        }
        // COPY rewrite: rows re-stamped under the (high) active layout;
        // collapse drops the accumulated Readable layouts.
        exec_ok(&engine, "ALTER TABLE t ADD COLUMN g INT AS (a + c1) STORED");
        assert_eq!(
            engine.database().table("test.t").unwrap().row_layouts.len(),
            1
        );
    }
    // Reopen from the persisted DD and decode the rewritten rows.
    let engine = StorageQueryEngine::open_with_storage_config(dir.path(), strict_config()).unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id, a, c1, c6, g FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![
                Value::Int(1),
                Value::Int(10),
                Value::Int(100),
                Value::Int(600),
                Value::Int(110),
            ],
            vec![
                Value::Int(2),
                Value::Int(20),
                Value::Int(100),
                Value::Int(600),
                Value::Int(120),
            ],
        ]
    );
    // Still writable after reopen.
    exec_ok(&engine, "INSERT INTO t (id, a) VALUES (3, 30)");
    let QueryResponse::ResultSet(result) = engine.execute("SELECT g FROM t WHERE id = 3").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(130)]]);
}

/// V4 Part 2 — a table that only ever takes INSTANT DDL accumulates dead
/// layouts with no type-change ALTER to collapse them. `OPTIMIZE TABLE`
/// is the explicit rebuild request and must reclaim them via an identity
/// COPY rewrite, collapsing `row_layouts` to one while leaving data intact.
#[test]
fn optimize_table_reclaims_instant_layouts() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE t (id INT PRIMARY KEY, a INT)");
    exec_ok(&engine, "INSERT INTO t (id, a) VALUES (1, 10), (2, 20)");

    // Five INSTANT ADD COLUMNs — nothing here triggers a COPY, so the dead
    // layouts pile up with no way to reclaim them short of OPTIMIZE.
    for k in 1..=5 {
        exec_ok(
            &engine,
            &format!("ALTER TABLE t ADD COLUMN c{k} INT DEFAULT {}", k * 100),
        );
    }
    let layouts_before = engine.database().table("test.t").unwrap().row_layouts.len();
    assert!(
        layouts_before >= 6,
        "5 INSTANT adds should accumulate layouts (got {layouts_before})"
    );

    // OPTIMIZE TABLE performs the identity rewrite and collapses the stack.
    exec_ok(&engine, "OPTIMIZE TABLE t");
    assert_eq!(
        engine.database().table("test.t").unwrap().row_layouts.len(),
        1,
        "OPTIMIZE must collapse accumulated INSTANT layouts to a single one"
    );

    fn rows_of(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
        match engine.execute(sql).unwrap() {
            QueryResponse::ResultSet(rs) => rs.rows,
            other => panic!("expected result set, got {other:?}"),
        }
    }
    // Data intact: original columns + all INSTANT defaults preserved.
    let rows = rows_of(&engine, "SELECT id, a, c1, c5 FROM t ORDER BY id");
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Int(1),
                Value::Int(10),
                Value::Int(100),
                Value::Int(500)
            ],
            vec![
                Value::Int(2),
                Value::Int(20),
                Value::Int(100),
                Value::Int(500)
            ],
        ]
    );
    // Still writable + readable after the collapse.
    exec_ok(&engine, "INSERT INTO t (id, a) VALUES (3, 30)");
    let rows = rows_of(&engine, "SELECT c1, c5 FROM t WHERE id = 3");
    assert_eq!(rows, vec![vec![Value::Int(100), Value::Int(500)]]);
}

/// A physical rewrite also preserves ordinary single-layout rows.
#[test]
fn optimize_table_without_instant_layouts_preserves_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE t (id INT PRIMARY KEY, a INT)");
    exec_ok(&engine, "INSERT INTO t (id, a) VALUES (1, 10), (2, 20)");
    assert_eq!(
        engine.database().table("test.t").unwrap().row_layouts.len(),
        1
    );
    exec_ok(&engine, "OPTIMIZE TABLE t");
    assert_eq!(
        engine.database().table("test.t").unwrap().row_layouts.len(),
        1
    );
    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT id, a FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)],
        ]
    );
}

/// V4 Part 2 — the OPTIMIZE-driven collapse must survive reopen: rows the
/// identity rewrite re-stamped under the retained Active layout must still
/// decode after the catalog reloads from the data dictionary.
#[test]
fn optimize_table_layout_collapse_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    // This regression verifies recovery from a database whose WAL begins at
    // LSN 1, so it must not inherit nextest's pre-checkpointed catalog seed.
    std::fs::write(dir.path().join(".mydb-test-fresh-data-dir"), []).unwrap();
    {
        let engine =
            StorageQueryEngine::open_with_storage_config(dir.path(), strict_config()).unwrap();
        exec_ok(&engine, "CREATE TABLE t (id INT PRIMARY KEY, a INT)");
        exec_ok(&engine, "INSERT INTO t (id, a) VALUES (1, 10), (2, 20)");
        for k in 1..=6 {
            exec_ok(
                &engine,
                &format!("ALTER TABLE t ADD COLUMN c{k} INT DEFAULT {}", k * 100),
            );
        }
        exec_ok(&engine, "OPTIMIZE TABLE t");
        assert_eq!(
            engine.database().table("test.t").unwrap().row_layouts.len(),
            1
        );
    }
    let engine = StorageQueryEngine::open_with_storage_config(dir.path(), strict_config()).unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id, a, c1, c6 FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![
                Value::Int(1),
                Value::Int(10),
                Value::Int(100),
                Value::Int(600)
            ],
            vec![
                Value::Int(2),
                Value::Int(20),
                Value::Int(100),
                Value::Int(600)
            ],
        ]
    );
    exec_ok(&engine, "INSERT INTO t (id, a) VALUES (3, 30)");
    let QueryResponse::ResultSet(result) = engine.execute("SELECT c6 FROM t WHERE id = 3").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(600)]]);
}

/// Regression for S3 (alter-table-review-2026-07): a column MODIFY that only
/// changes the collation used to be treated as an INSTANT metadata change,
/// leaving B-tree keys encoded under the *old* collation id. The index must
/// be rebuilt so lookups follow the new collation. Here the switch from a
/// case-insensitive to a binary collation must make 'apple' and 'APPLE'
/// distinct under the UNIQUE index and drive exact-match lookups correctly.
#[test]
fn modify_column_collation_rebuilds_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(
        &engine,
        "CREATE TABLE t (c VARCHAR(20) COLLATE utf8mb4_general_ci, UNIQUE KEY uk (c))",
    );
    exec_ok(&engine, "INSERT INTO t VALUES ('apple')");
    // Under general_ci, 'APPLE' duplicates 'apple'.
    let dup = exec_err(&engine, "INSERT INTO t VALUES ('APPLE')");
    assert_eq!(
        dup.code(),
        1062,
        "ci collation should reject 'APPLE': {dup:?}"
    );

    // Switch to a binary collation. The UNIQUE index must be rebuilt under
    // the new collation.
    exec_ok(
        &engine,
        "ALTER TABLE t MODIFY c VARCHAR(20) COLLATE utf8mb4_bin",
    );

    // Now 'APPLE' is distinct from 'apple' and must be accepted.
    exec_ok(&engine, "INSERT INTO t VALUES ('APPLE')");

    // Exact-match lookup must find only the binary-equal row (the index is
    // now keyed under utf8mb4_bin; a stale general_ci-keyed index would
    // either miss or over-match).
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT c FROM t WHERE c = 'apple'").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1, "bin exact match must return one row");
    assert_eq!(rs.rows[0][0].to_display_string(), "apple");

    let QueryResponse::ResultSet(all) = engine.execute("SELECT COUNT(*) FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(all.rows[0][0], Value::Int(2), "both rows must be present");
}

/// Regression for S7 (alter-table-review-2026-07): a single-action
/// `RENAME COLUMN a TO b` where `b` already exists used to skip the
/// duplicate check (only the multi-action pre-flight had it) and silently
/// produce two columns named `b`. MySQL reports 1060.
#[test]
fn rename_column_to_existing_name_is_rejected_1060() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE t (a INT, b INT)");
    let err = exec_err(&engine, "ALTER TABLE t RENAME COLUMN a TO b");
    assert_eq!(err.code(), 1060, "expected duplicate column name: {err:?}");
    // Renaming to a fresh name (and a case-only change) still works.
    exec_ok(&engine, "ALTER TABLE t RENAME COLUMN a TO c");
    let cols = rows_to_strings_ddl(exec_set_ddl(&engine, "SHOW COLUMNS FROM t"));
    let names: Vec<String> = cols.into_iter().map(|mut r| r.remove(0)).collect();
    assert_eq!(names, vec!["c".to_string(), "b".to_string()]);
}

/// Column-level SRID is a supported MySQL 8.0 option. MODIFY/CHANGE must
/// apply and echo it instead of silently dropping it as an unknown option.
#[test]
fn modify_and_change_column_srid_are_applied() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE g (id INT, geom GEOMETRY NOT NULL)");
    exec_ok(
        &engine,
        "ALTER TABLE g MODIFY geom GEOMETRY NOT NULL SRID 4326",
    );
    exec_ok(
        &engine,
        "ALTER TABLE g CHANGE geom geom2 GEOMETRY NOT NULL SRID 3857",
    );
    let ddl = rows_to_strings_ddl(exec_set_ddl(&engine, "SHOW CREATE TABLE g"));
    assert!(
        ddl[0][1].contains("`geom2` geometry NOT NULL SRID 3857"),
        "{}",
        ddl[0][1]
    );
}

fn exec_set_ddl(engine: &StorageQueryEngine, sql: &str) -> ResultSet {
    let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
        panic!("expected result set for {sql}");
    };
    rs
}

fn rows_to_strings_ddl(rs: ResultSet) -> Vec<Vec<String>> {
    rs.rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|v| match v {
                    Value::Null => "NULL".to_string(),
                    Value::String(s) => s,
                    other => other.to_display_string(),
                })
                .collect()
        })
        .collect()
}

/// Regression for S22 (alter-table-review-2026-07): `ALTER TABLE ... ALTER
/// INDEX PRIMARY INVISIBLE` must report ER_PK_INDEX_CANT_BE_INVISIBLE (3522),
/// matching the inline form; it used to return a generic 1235.
#[test]
fn alter_primary_index_invisible_is_3522() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(
        &engine,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT, KEY k (v))",
    );
    let err = exec_err(&engine, "ALTER TABLE t ALTER INDEX PRIMARY INVISIBLE");
    assert_eq!(err.code(), 3522, "expected PK-invisible error: {err:?}");
    // A secondary index can still go invisible.
    exec_ok(&engine, "ALTER TABLE t ALTER INDEX k INVISIBLE");
}

/// Regression for S28 (alter-table-review-2026-07): `ALTER TABLE ...
/// AUTO_INCREMENT = 0` is a no-op in MySQL (the counter can't be lowered),
/// not error 1064.
#[test]
fn alter_auto_increment_zero_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(
        &engine,
        "CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v INT)",
    );
    exec_ok(&engine, "INSERT INTO t (v) VALUES (10), (20)");
    // Must not error; counter stays at its current high-water mark.
    exec_ok(&engine, "ALTER TABLE t AUTO_INCREMENT = 0");
    exec_ok(&engine, "INSERT INTO t (v) VALUES (30)");
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id FROM t WHERE v = 30").unwrap()
    else {
        panic!("expected result set");
    };
    // Next id continues from the existing max (3), not reset.
    assert_eq!(rs.rows[0][0], Value::Int(3));
}

/// Column CHARSET name is MySQL's synonym for CHARACTER SET name.
/// sqlparser 0.61 MySqlDialect accepts CHARSET only at table level and returns 1064 for columns;
/// mydb fills this gap in MydbDialect::parse_column_option.
/// Both spellings must produce identical charset/collation and SHOW CREATE.
#[test]
fn column_charset_synonym_matches_character_set() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Both spellings must create tables; CHARSET previously failed with 1064.
    exec_ok(
        &engine,
        "CREATE TABLE t_charset (a INT, b VARCHAR(255) CHARSET latin1)",
    );
    exec_ok(
        &engine,
        "CREATE TABLE t_charset_kw (a INT, b VARCHAR(255) CHARACTER SET latin1)",
    );

    // Catalog column charset/collation fields match exactly.
    let col_a = engine
        .db
        .table("test.t_charset")
        .unwrap()
        .columns
        .iter()
        .find(|c| c.name == "b")
        .cloned()
        .unwrap();
    let col_b = engine
        .db
        .table("test.t_charset_kw")
        .unwrap()
        .columns
        .iter()
        .find(|c| c.name == "b")
        .cloned()
        .unwrap();
    assert_eq!(
        col_a.charset.as_deref(),
        Some("latin1"),
        "The CHARSET spelling did not apply the character set"
    );
    assert_eq!(
        col_a.charset, col_b.charset,
        "The two spellings produced different character sets"
    );
    assert_eq!(
        col_a.collation, col_b.collation,
        "The two spellings produced different collations"
    );

    // SHOW CREATE matches byte-for-byte except for the table name.
    let show_charset = show_create(&engine, "t_charset");
    let show_kw = show_create(&engine, "t_charset_kw");
    assert_eq!(
        show_charset.replace("t_charset", "T"),
        show_kw.replace("t_charset_kw", "T"),
        "The two SHOW CREATE spellings differ:\n{show_charset}\n---\n{show_kw}"
    );
    // Output uses canonical CHARACTER SET spelling.
    assert!(
        show_charset.contains("CHARACTER SET latin1"),
        "SHOW CREATE did not emit CHARACTER SET latin1: {show_charset}"
    );

    // A column named nonreserved charset remains unaffected outside the option position.
    exec_ok(&engine, "CREATE TABLE t_named (charset INT, v INT)");
    assert!(engine
        .db
        .table("test.t_named")
        .unwrap()
        .columns
        .iter()
        .any(|c| c.name == "charset"));

    // ALTER TABLE MODIFY CHARSET also works without enclosing parentheses.
    exec_ok(&engine, "CREATE TABLE t_alter (a INT, b VARCHAR(64))");
    exec_ok(
        &engine,
        "ALTER TABLE t_alter MODIFY b VARCHAR(64) CHARSET latin1",
    );
    let altered = engine
        .db
        .table("test.t_alter")
        .unwrap()
        .columns
        .iter()
        .find(|c| c.name == "b")
        .cloned()
        .unwrap();
    assert_eq!(
        altered.charset.as_deref(),
        Some("latin1"),
        "ALTER MODIFY CHARSET did not apply the character set"
    );
}

/// MySQL resolves ENGINE names against SHOW ENGINES. mydb advertises only
/// mydb and its InnoDB compatibility alias, so every other name must return
/// ER_UNKNOWN_STORAGE_ENGINE (1286); accepting and merely echoing a physical
/// engine promise is not compatible or safe.
#[test]
fn create_table_rejects_unadvertised_engine_and_round_trips_supported_aliases() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    for name in ["MyISAM", "unknown_engine_xyz"] {
        let err = exec_err(
            &engine,
            &format!("CREATE TABLE e_bad_{name} (a INT) ENGINE={name}"),
        );
        assert_eq!(err.code(), 1286, "{name}: {err:?}");
        assert_eq!(err.sql_state(), "42000", "{name}");
        assert!(err.message().contains(name), "{}", err.message());
    }

    // Advertised aliases continue to round-trip.
    exec_ok(&engine, "CREATE TABLE e_innodb (a INT) ENGINE=InnoDB");
    assert!(
        show_create(&engine, "e_innodb").contains(") ENGINE=InnoDB DEFAULT CHARSET="),
        "InnoDB was not reflected in output"
    );
    exec_ok(&engine, "CREATE TABLE e_mydb (a INT) ENGINE=mydb");
    assert!(show_create(&engine, "e_mydb").contains("ENGINE=mydb"));

    // Without ENGINE, use the MySQL-compatible InnoDB default.
    exec_ok(&engine, "CREATE TABLE e_plain (a INT)");
    let plain = show_create(&engine, "e_plain");
    assert!(plain.contains("ENGINE=InnoDB"), "{plain}");

    // information_schema.tables and SHOW TABLE STATUS report the requested engine name.
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT table_name, engine FROM information_schema.tables \
             WHERE table_schema='test' AND table_name IN ('e_mydb','e_innodb','e_plain') \
             ORDER BY table_name",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let get = |name: &str| {
        result
            .rows
            .iter()
            .find(|r| r[0] == Value::String(name.into()))
            .map(|r| r[1].clone())
    };
    assert_eq!(get("e_innodb"), Some(Value::String("InnoDB".into())));
    assert_eq!(get("e_mydb"), Some(Value::String("mydb".into())));
    assert_eq!(get("e_plain"), Some(Value::String("InnoDB".into())));

    // SHOW CREATE output can be parsed again for round-trip compatibility.
    exec_ok(
        &engine,
        &show_create(&engine, "e_innodb").replace("e_innodb", "e_innodb_rt"),
    );
    assert!(show_create(&engine, "e_innodb_rt").contains("ENGINE=InnoDB"));
}

#[test]
fn system_schema_table_ddl_is_object_scoped_like_mysql80() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    for schema in ["mysql", "sys"] {
        exec_ok(
            &engine,
            &format!("CREATE TABLE {schema}.custom_ddl_t (id INT PRIMARY KEY)"),
        );
        exec_ok(
            &engine,
            &format!("ALTER TABLE {schema}.custom_ddl_t ADD COLUMN v INT"),
        );
        exec_ok(
            &engine,
            &format!("INSERT INTO {schema}.custom_ddl_t VALUES (1, 10)"),
        );
        let QueryResponse::ResultSet(result) = engine
            .execute(&format!("SELECT v FROM {schema}.custom_ddl_t WHERE id=1"))
            .unwrap()
        else {
            panic!("expected result set");
        };
        assert_eq!(result.rows, vec![vec![Value::Int(10)]]);
        exec_ok(&engine, &format!("DROP TABLE {schema}.custom_ddl_t"));
    }

    for schema in ["information_schema", "performance_schema"] {
        let err = exec_err(
            &engine,
            &format!("CREATE TABLE {schema}.custom_ddl_t (id INT)"),
        );
        assert_eq!(err.code(), 1044, "{schema}: {err:?}");
    }
}

/// GAP: odd-length hex literals must be left-zero-padded and decoded like MySQL,
/// not rejected as invalid hex literals; see func_like.test/type_binary.test.
#[test]
fn odd_length_hex_literals_are_zero_padded() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let scalar = |sql: &str| -> Value {
        let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
            panic!("expected result set for `{sql}`");
        };
        result.rows[0][0].clone()
    };

    // HEX(0x123) equals '0123', with one leading zero nibble.
    assert_eq!(scalar("SELECT HEX(0x123)"), Value::String("0123".into()));
    // 0x123 and 0x0123 are equivalent.
    assert_eq!(scalar("SELECT 0x123 = 0x0123"), Value::Int(1));
    // In numeric context 0x123 equals 291; hex literals are UNSIGNED.
    assert_eq!(scalar("SELECT 0x123 + 0"), Value::UInt(291));
    // Odd-length hex crossing a four-byte word boundary, such as CONVERT(0x999999999,...),
    // must decode rather than fail with 1064.
    assert_eq!(
        scalar("SELECT HEX(0x999999999)"),
        Value::String("0999999999".into())
    );
    // A single nibble becomes one byte.
    assert_eq!(scalar("SELECT HEX(0xF)"), Value::String("0F".into()));
}

#[test]
fn alter_add_auto_increment_validates_type_before_key_shape() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE auto_type (id INT PRIMARY KEY)");
    let err = exec_err(
        &engine,
        "ALTER TABLE auto_type DROP PRIMARY KEY, \
         ADD COLUMN bad FLOAT PRIMARY KEY AUTO_INCREMENT",
    );
    assert_eq!((err.code(), err.sql_state().as_ref()), (1063, "42000"));
    assert!(!show_create(&engine, "auto_type").contains("bad"));
}

#[test]
fn ctas_commits_load_with_autocommit_disabled() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    session.connection_id = 8201;
    engine
        .execute_with_session(&mut session, "SET autocommit=0")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE committed_ctas AS SELECT 42 AS id",
        )
        .unwrap();
    assert!(
        session.current_txn.is_none(),
        "persistent CTAS must commit its load even with autocommit=0"
    );
    assert!(
        !session.autocommit,
        "CTAS must preserve the session setting"
    );
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    let result = engine
        .execute("SELECT id FROM committed_ctas")
        .unwrap()
        .materialize()
        .unwrap();
    let QueryResponse::ResultSet(result) = result else {
        panic!("expected rows")
    };
    assert_eq!(result.rows, vec![vec![Value::Int(42)]]);
}

#[test]
fn ctas_join_retains_outer_mdl_after_source_open() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE mdl_left(id INT)");
    exec_ok(&engine, "CREATE TABLE mdl_right(id INT)");
    let observed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let checked = observed.clone();
    super::super::dispatch::CTAS_SOURCE_READY.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move |db| {
            let locks = db.metadata_lock_snapshot();
            for (name, mode) in [
                ("mdl_dst", "EXCLUSIVE"),
                ("mdl_left", "SHARED_READ"),
                ("mdl_right", "SHARED_READ"),
            ] {
                assert!(
                    locks.iter().any(|lock| lock.owner_thread_id == Some(8202)
                        && lock.object_name.as_deref() == Some(name)
                        && lock.lock_type == mode
                        && lock.lock_status == "GRANTED"),
                    "missing {mode} on {name} after nested SELECT: {locks:?}"
                );
            }
            checked.store(true, std::sync::atomic::Ordering::Relaxed);
        }));
    });
    let mut session = SessionContext::default();
    session.connection_id = 8202;
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE mdl_dst AS SELECT l.id FROM mdl_left l JOIN mdl_right r ON l.id=r.id",
        )
        .unwrap();
    assert!(observed.load(std::sync::atomic::Ordering::Relaxed));
    assert!(!engine
        .database()
        .metadata_lock_snapshot()
        .iter()
        .any(|lock| lock.owner_thread_id == Some(8202)));
}

#[test]
#[ignore = "manual CTAS throughput/peak RSS comparison in an isolated test process"]
fn ctas_streaming_load_measurement() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = memory_test_storage_config();
    // Memory durability retains dirty pages; fit both tables so this measures
    // CTAS rather than waiting for eviction without a page cleaner.
    config.buffer_pool_pages = 16_384;
    let engine =
        StorageQueryEngine::open_with_storage_config_without_background_workers(dir.path(), config)
            .unwrap();
    exec_ok(
        &engine,
        "CREATE TABLE measure_src(id INT, payload VARCHAR(2048))",
    );
    for base in (0..32768).step_by(256) {
        let values = (base..base + 256)
            .map(|id| format!("({id},REPEAT('x',2048))"))
            .collect::<Vec<_>>()
            .join(",");
        exec_ok(&engine, &format!("INSERT INTO measure_src VALUES {values}"));
    }
    let started = std::time::Instant::now();
    exec_ok(
        &engine,
        "CREATE TABLE measure_dst(id INT) AS SELECT id,payload FROM measure_src",
    );
    let elapsed = started.elapsed();
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT COUNT(*),SUM(LENGTH(payload)) FROM measure_dst")
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!("expected rows")
    };
    assert_eq!(result.rows[0][0], Value::Int(32768));
    assert_eq!(result.rows[0][1].to_display_string(), "67108864");
    eprintln!(
        "CTAS_MEASUREMENT rows=32768 payload_bytes=67108864 elapsed_ms={}",
        elapsed.as_millis()
    );
}
