//! MySQL 8.0 SHOW compatibility coverage.
//!
//! Cover docs/show-mysql80-compat-design.md:
//! A1 SHOW PLUGINS: column shape and truthful built-in plugin listing.
//! A2 SHOW OPEN TABLES: FROM/LIKE/WHERE filters and column shape.
//! A3 SHOW MASTER LOGS aliases SHOW BINARY LOGS.
//! A5 SHOW ENGINE name STATUS/MUTEX: accept MYDB and return 1286 for other engines.
//! - A6/A7 `SHOW EXTENDED COLUMNS/TABLES`（accept+ignore）
//! Synonyms: SHOW FIELDS, SHOW INDEXES, SHOW KEYS, and SHOW SCHEMAS.
//! SHOW LOCAL VARIABLES/STATUS aliases SESSION.
//! B1 SHOW PRIVILEGES lists all static and dynamic privileges.
//! B2 SHOW STATUS distinguishes GLOBAL and SESSION.
//! B4 SHOW PROCEDURE STATUS populates Comment.
//! C1 SHOW BINARY LOGS includes Encrypted.
//!
//! Assert protocol-visible column names, ordering, and counts, which directly affect
//! JDBC, ORM, and monitoring metadata discovery.

use super::support::execute_materialized as exec;
use super::*;

fn column_names(rs: &ResultSet) -> Vec<String> {
    rs.columns.iter().map(|c| c.name.clone()).collect()
}

fn first_col_strings(rs: &ResultSet) -> Vec<String> {
    rs.rows.iter().map(|r| r[0].to_display_string()).collect()
}

// ---------------------------------------------------------------------------
// A1 SHOW PLUGINS
// ---------------------------------------------------------------------------

#[test]
fn show_plugins_returns_mysql_8_column_shape() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "SHOW PLUGINS").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(
        column_names(&rs),
        vec!["Name", "Status", "Type", "Library", "License"],
        "MySQL 8.0 mandates these five column names in this order"
    );

    // mydb's compiled-in plugins surface here: the storage engine + the
    // three authentication plugins mydb actually implements.
    let names: Vec<String> = rs.rows.iter().map(|r| r[0].to_display_string()).collect();
    for required in [
        "mydb",
        "mysql_native_password",
        "sha256_password",
        "caching_sha2_password",
    ] {
        assert!(
            names.iter().any(|n| n == required),
            "SHOW PLUGINS missing {required}; got {names:?}"
        );
    }

    // Built-ins must report Library=NULL (cannot UNINSTALL) and Status=ACTIVE.
    for row in &rs.rows {
        assert_eq!(row[1], Value::String("ACTIVE".to_string()));
        assert!(
            matches!(row[3], Value::Null),
            "Library should be NULL for compiled-in plugins"
        );
        assert_eq!(row[4], Value::String("GPL".to_string()));
    }
}

// ---------------------------------------------------------------------------
// A2 SHOW OPEN TABLES
// ---------------------------------------------------------------------------

#[test]
fn show_open_tables_columns_and_filtering() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE TABLE customers (id INT PRIMARY KEY)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE products  (id INT PRIMARY KEY)",
    )
    .unwrap();

    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "SHOW OPEN TABLES").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        column_names(&rs),
        vec!["Database", "Table", "In_use", "Name_locked"]
    );
    let tables: Vec<String> = rs.rows.iter().map(|r| r[1].to_display_string()).collect();
    assert!(tables.contains(&"customers".to_string()));
    assert!(tables.contains(&"products".to_string()));

    // LIKE filter on Table column.
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SHOW OPEN TABLES LIKE 'cust%'").unwrap()
    else {
        panic!("expected result set");
    };
    let tables: Vec<String> = rs.rows.iter().map(|r| r[1].to_display_string()).collect();
    assert_eq!(tables, vec!["customers"]);

    // FROM db scoping returns the same rows when explicitly named.
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SHOW OPEN TABLES FROM test LIKE 'prod%'",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
    assert_eq!(rs.rows[0][1], Value::String("products".to_string()));
}

// ---------------------------------------------------------------------------
// A3 SHOW MASTER LOGS  (=  SHOW BINARY LOGS  synonym) + C1 Encrypted column
// ---------------------------------------------------------------------------

#[test]
fn show_master_logs_is_alias_of_show_binary_logs_and_carries_encrypted() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // Both spellings must reach the same handler — even if binlog is
    // disabled (the manager reports 1235 in that case). What matters here
    // is that parsing accepts both and dispatches identically.
    let binary = exec(&engine, &mut session, "SHOW BINARY LOGS");
    let master = exec(&engine, &mut session, "SHOW MASTER LOGS");
    match (binary, master) {
        (Ok(QueryResponse::ResultSet(b)), Ok(QueryResponse::ResultSet(m))) => {
            // Both forms must surface the 8.0 column shape — three columns
            // ending in Encrypted.
            let want = vec!["Log_name", "File_size", "Encrypted"];
            assert_eq!(column_names(&b), want);
            assert_eq!(column_names(&m), want);
        }
        (Err(eb), Err(em)) => {
            // When binlog is off both return the same 1235 — accept that.
            assert_eq!(eb.code(), em.code());
        }
        other => panic!("BINARY LOGS / MASTER LOGS must dispatch alike; got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// A5 SHOW ENGINE <name> {STATUS | MUTEX}
// ---------------------------------------------------------------------------

#[test]
fn show_engine_mydb_status_via_generalised_parse() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // Both spellings (the dedicated fast path + the generalised parse) must
    // produce the same Type/Name/Status column shape.
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SHOW ENGINE MYDB STATUS").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(column_names(&rs), vec!["Type", "Name", "Status"]);
    assert!(!rs.rows.is_empty(), "MYDB status must surface real metrics");
}

#[test]
fn show_engine_mydb_mutex_returns_empty_three_column_shape() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SHOW ENGINE MYDB MUTEX").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(column_names(&rs), vec!["Type", "Name", "Status"]);
    // mydb does not produce per-latch counters — empty rather than fake.
    assert!(rs.rows.is_empty());
}

#[test]
fn show_engine_innodb_status_returns_1286_unknown_engine() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let err = exec(&engine, &mut session, "SHOW ENGINE INNODB STATUS").unwrap_err();
    assert_eq!(err.code(), 1286, "expected ER_UNKNOWN_STORAGE_ENGINE");
    assert!(
        err.message().to_ascii_uppercase().contains("INNODB"),
        "1286 message should name the unknown engine; got {:?}",
        err.message()
    );
}

// ---------------------------------------------------------------------------
// A6/A7 EXTENDED COLUMNS/TABLES (accept + ignore)
// ---------------------------------------------------------------------------

#[test]
fn show_extended_columns_and_tables_are_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)",
    )
    .unwrap();

    // EXTENDED on COLUMNS/TABLES is silently legal in MySQL — mydb has no
    // hidden columns/tables, so the shape matches the plain form.
    let QueryResponse::ResultSet(plain) =
        exec(&engine, &mut session, "SHOW COLUMNS FROM t").unwrap()
    else {
        panic!("expected result set");
    };
    let QueryResponse::ResultSet(ext) =
        exec(&engine, &mut session, "SHOW EXTENDED COLUMNS FROM t").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(column_names(&plain), column_names(&ext));
    assert_eq!(plain.rows.len(), ext.rows.len());

    let QueryResponse::ResultSet(plain) = exec(&engine, &mut session, "SHOW TABLES").unwrap()
    else {
        panic!("expected result set");
    };
    let QueryResponse::ResultSet(ext) =
        exec(&engine, &mut session, "SHOW EXTENDED TABLES").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(column_names(&plain), column_names(&ext));
    assert_eq!(plain.rows.len(), ext.rows.len());
}

// ---------------------------------------------------------------------------
// Synonyms: FIELDS/INDEXES/KEYS/SCHEMAS.
// ---------------------------------------------------------------------------

#[test]
fn show_fields_indexes_keys_schemas_are_normalised() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT, KEY idx_v (v))",
    )
    .unwrap();

    // FIELDS = COLUMNS
    let QueryResponse::ResultSet(cols) =
        exec(&engine, &mut session, "SHOW COLUMNS FROM t").unwrap()
    else {
        panic!("expected result set");
    };
    let QueryResponse::ResultSet(fields) =
        exec(&engine, &mut session, "SHOW FIELDS FROM t").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(column_names(&cols), column_names(&fields));
    assert_eq!(cols.rows.len(), fields.rows.len());

    // INDEX / INDEXES / KEYS all alias.
    let QueryResponse::ResultSet(by_index) =
        exec(&engine, &mut session, "SHOW INDEX FROM t").unwrap()
    else {
        panic!("expected result set");
    };
    let QueryResponse::ResultSet(by_indexes) =
        exec(&engine, &mut session, "SHOW INDEXES FROM t").unwrap()
    else {
        panic!("expected result set");
    };
    let QueryResponse::ResultSet(by_keys) =
        exec(&engine, &mut session, "SHOW KEYS FROM t").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(column_names(&by_index), column_names(&by_indexes));
    assert_eq!(column_names(&by_index), column_names(&by_keys));
    assert_eq!(by_index.rows.len(), by_indexes.rows.len());
    assert_eq!(by_index.rows.len(), by_keys.rows.len());

    // SCHEMAS = DATABASES
    let QueryResponse::ResultSet(dbs) = exec(&engine, &mut session, "SHOW DATABASES").unwrap()
    else {
        panic!("expected result set");
    };
    let QueryResponse::ResultSet(schemas) = exec(&engine, &mut session, "SHOW SCHEMAS").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(column_names(&dbs), column_names(&schemas));
    assert_eq!(first_col_strings(&dbs), first_col_strings(&schemas));
}

// ---------------------------------------------------------------------------
// SHOW LOCAL VARIABLES/STATUS equals SHOW SESSION.
// ---------------------------------------------------------------------------

#[test]
fn show_local_variables_and_status_alias_session() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let QueryResponse::ResultSet(session_vars) = exec(
        &engine,
        &mut session,
        "SHOW SESSION VARIABLES LIKE 'autocommit'",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    let QueryResponse::ResultSet(local_vars) = exec(
        &engine,
        &mut session,
        "SHOW LOCAL VARIABLES LIKE 'autocommit'",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(column_names(&session_vars), column_names(&local_vars));
    assert_eq!(session_vars.rows, local_vars.rows);

    let QueryResponse::ResultSet(session_st) =
        exec(&engine, &mut session, "SHOW SESSION STATUS LIKE 'Uptime'").unwrap()
    else {
        panic!("expected result set");
    };
    let QueryResponse::ResultSet(local_st) =
        exec(&engine, &mut session, "SHOW LOCAL STATUS LIKE 'Uptime'").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(column_names(&session_st), column_names(&local_st));
    assert_eq!(session_st.rows, local_st.rows);
}

// ---------------------------------------------------------------------------
// B1 SHOW PRIVILEGES — full static + dynamic catalog
// ---------------------------------------------------------------------------

#[test]
fn show_privileges_lists_full_mysql_8_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "SHOW PRIVILEGES").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(column_names(&rs), vec!["Privilege", "Context", "Comment"]);

    // MySQL 8.0 ships ~33 static privileges. mydb's catalogue must cover
    // every static name a client may match on; the dynamic privileges then
    // pad the row count further. Pin a representative subset.
    let names: Vec<String> = rs.rows.iter().map(|r| r[0].to_display_string()).collect();
    for required in [
        "Select",
        "Insert",
        "Update",
        "Delete",
        "Create",
        "Drop",
        "Alter",
        "Index",
        "Grant option",
        "References",
        "Execute",
        "Create routine",
        "Alter routine",
        "Create view",
        "Show view",
        "Trigger",
        "Event",
        "Lock tables",
        "Create temporary tables",
        "Create user",
        "Create role",
        "Drop role",
        "Create tablespace",
        "Replication client",
        "Replication slave",
        "Reload",
        "Shutdown",
        "Process",
        "File",
        "Super",
        "Show databases",
        "Usage",
        // Dynamic privileges — at least a few that orchestrator/pt-toolkit
        // commonly grep for.
        "BACKUP_ADMIN",
        "ROLE_ADMIN",
        "SYSTEM_VARIABLES_ADMIN",
        "REPLICATION_SLAVE_ADMIN",
        "XA_RECOVER_ADMIN",
    ] {
        assert!(
            names.iter().any(|n| n == required),
            "SHOW PRIVILEGES missing {required}; got {} rows",
            names.len()
        );
    }

    // The historical stub returned exactly 3 rows. Guard against any
    // future regression that would silently shrink the catalog.
    assert!(
        rs.rows.len() >= 60,
        "expected full catalog, got {}",
        rs.rows.len()
    );
}

// ---------------------------------------------------------------------------
// B2 SHOW STATUS — GLOBAL vs SESSION scope
// ---------------------------------------------------------------------------

#[test]
fn show_status_scope_differs_for_per_connection_variables() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // MySQL 8.0.46 declares Ssl_cipher/Ssl_version with BOTH scope. GLOBAL
    // falls back to the current connection's value for these string fields.
    let QueryResponse::ResultSet(session_rs) = exec(
        &engine,
        &mut session,
        "SHOW SESSION STATUS LIKE 'Ssl_cipher'",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    let QueryResponse::ResultSet(global_rs) = exec(
        &engine,
        &mut session,
        "SHOW GLOBAL STATUS LIKE 'Ssl_cipher'",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(session_rs.rows.len(), 1);
    assert_eq!(global_rs.rows.len(), 1);
    assert_eq!(session_rs.rows[0][1], global_rs.rows[0][1]);

    // A truly global counter (Uptime) appears under both scopes.
    let QueryResponse::ResultSet(session_up) =
        exec(&engine, &mut session, "SHOW SESSION STATUS LIKE 'Uptime'").unwrap()
    else {
        panic!("expected result set");
    };
    let QueryResponse::ResultSet(global_up) =
        exec(&engine, &mut session, "SHOW GLOBAL STATUS LIKE 'Uptime'").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(session_up.rows.len(), 1);
    assert_eq!(global_up.rows.len(), 1);
}

// ---------------------------------------------------------------------------
// B4 SHOW PROCEDURE STATUS — Comment column is no longer a stub
// ---------------------------------------------------------------------------

#[test]
fn show_procedure_status_returns_real_comment() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE meta_p(IN x INT) COMMENT 'audit log writer' SELECT x",
    )
    .unwrap();

    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SHOW PROCEDURE STATUS LIKE 'meta_p'").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
    // Column order per MySQL 8.0 manual: Db, Name, Type, Definer, Modified,
    // Created, Security_type, Comment, character_set_client,
    // collation_connection, Database Collation — 11 columns.
    assert_eq!(rs.columns.len(), 11);
    assert_eq!(
        rs.columns[7].name, "Comment",
        "MySQL 8.0 column #8 must be Comment"
    );
    assert_eq!(
        rs.rows[0][7],
        Value::String("audit log writer".to_string()),
        "B4: Comment must round-trip from CREATE PROCEDURE ... COMMENT '...'"
    );
}

// ---------------------------------------------------------------------------
// C2 COMMENT escaping round trip: routine/event/partition SHOW CREATE must replay
// quotes and backslashes losslessly, doubling quotes and backslashes consistently.
// Use SHOW CREATE idempotence as the oracle: drop and replay its text,
// then require byte-identical SHOW CREATE output; lost escape bytes would cause drift.
// ---------------------------------------------------------------------------

/// Extract the "Create ..." statement cell from a single-row SHOW CREATE
/// result (the column whose name begins with "Create").
fn show_create_stmt(rs: &ResultSet) -> String {
    let col = rs
        .columns
        .iter()
        .position(|c| c.name.starts_with("Create"))
        .expect("SHOW CREATE result must have a `Create ...` column");
    match &rs.rows[0][col] {
        Value::String(s) => s.clone(),
        other => panic!("unexpected SHOW CREATE cell: {other:?}"),
    }
}

fn show_create(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> String {
    let QueryResponse::ResultSet(rs) = exec(engine, session, sql).unwrap() else {
        panic!("expected result set for `{sql}`");
    };
    show_create_stmt(&rs)
}

#[test]
fn procedure_comment_escaping_roundtrips() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // Comment literal carries an embedded quote (`''`) and backslashes (`\\`).
    exec(
        &engine,
        &mut session,
        "CREATE PROCEDURE esc_p() COMMENT 'a''b\\\\c\\\\' SELECT 1",
    )
    .unwrap();

    let s1 = show_create(&engine, &mut session, "SHOW CREATE PROCEDURE esc_p");
    exec(&engine, &mut session, "DROP PROCEDURE esc_p").unwrap();
    exec(&engine, &mut session, &s1).unwrap();
    let s2 = show_create(&engine, &mut session, "SHOW CREATE PROCEDURE esc_p");
    assert_eq!(s1, s2, "routine COMMENT must round-trip losslessly");
}

#[test]
fn event_comment_escaping_roundtrips() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE EVENT esc_e ON SCHEDULE EVERY 1 DAY \
         COMMENT 'x''y\\\\z\\\\' DO SELECT 1",
    )
    .unwrap();

    let s1 = show_create(&engine, &mut session, "SHOW CREATE EVENT esc_e");
    // The emitted COMMENT must escape backslashes; otherwise re-parsing eats
    // them and the second SHOW CREATE drifts.
    assert!(
        s1.contains("\\\\"),
        "event SHOW CREATE must escape backslashes: {s1}"
    );
    exec(&engine, &mut session, "DROP EVENT esc_e").unwrap();
    exec(&engine, &mut session, &s1).unwrap();
    let s2 = show_create(&engine, &mut session, "SHOW CREATE EVENT esc_e");
    assert_eq!(s1, s2, "event COMMENT must round-trip losslessly");
}

#[test]
fn partition_comment_escaping_roundtrips() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE TABLE esc_part (id INT) PARTITION BY RANGE (id) (\
         PARTITION p0 VALUES LESS THAN (10) COMMENT = 'q''r\\\\s\\\\', \
         PARTITION p1 VALUES LESS THAN (20))",
    )
    .unwrap();

    let s1 = show_create(&engine, &mut session, "SHOW CREATE TABLE esc_part");
    assert!(
        s1.contains("\\\\"),
        "partition SHOW CREATE must escape backslashes: {s1}"
    );
    exec(&engine, &mut session, "DROP TABLE esc_part").unwrap();
    exec(&engine, &mut session, &s1).unwrap();
    let s2 = show_create(&engine, &mut session, "SHOW CREATE TABLE esc_part");
    assert_eq!(s1, s2, "partition COMMENT must round-trip losslessly");
}

#[test]
fn subpartition_comment_escaping_roundtrips() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE TABLE esc_sub (id INT) PARTITION BY RANGE (id) \
         SUBPARTITION BY HASH (id) SUBPARTITIONS 2 (\
         PARTITION p0 VALUES LESS THAN (10) (\
         SUBPARTITION sp0 COMMENT = 't''u\\\\v\\\\', SUBPARTITION sp1))",
    )
    .unwrap();

    let s1 = show_create(&engine, &mut session, "SHOW CREATE TABLE esc_sub");
    assert!(
        s1.contains("\\\\"),
        "subpartition SHOW CREATE must escape backslashes: {s1}"
    );
    exec(&engine, &mut session, "DROP TABLE esc_sub").unwrap();
    exec(&engine, &mut session, &s1).unwrap();
    let s2 = show_create(&engine, &mut session, "SHOW CREATE TABLE esc_sub");
    assert_eq!(s1, s2, "subpartition COMMENT must round-trip losslessly");
}

// ---------------------------------------------------------------------------
// M1 SHOW PROCESSLIST — PROCESS-privilege scoping + UTF-8-safe Info truncation
// ---------------------------------------------------------------------------

#[test]
fn show_processlist_scopes_by_process_privilege_and_truncates_info_safely() {
    use crate::engine::session_registry::SessionRegistry;
    use crate::engine::show::show_processlist_result;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    exec(
        &engine,
        &mut root,
        "CREATE USER 'bob'@'%' IDENTIFIED BY 'pw'",
    )
    .unwrap();
    exec(
        &engine,
        &mut root,
        "CREATE USER 'eve'@'%' IDENTIFIED BY 'pw'",
    )
    .unwrap();

    // Three live connections: root, bob, eve. bob's Info is a long multi-byte
    // (3-byte UTF-8) string that would panic a byte-index slice at 100.
    let registry = SessionRegistry::new();
    let _root_h = registry.register(1, "root".into(), "localhost".into(), "test".into());
    let bob_h = registry.register(2, "bob".into(), "h2".into(), "test".into());
    let _eve_h = registry.register(3, "eve".into(), "h3:4567".into(), "".into());
    let _background = registry.register_background(
        "bob".into(),
        "localhost".into(),
        None,
        "Daemon",
        crate::engine::session_registry::ProcessState::WaitingForNextActivation,
        None,
    );
    let long_info = "中".repeat(150);
    bob_h.mark_busy("Query", Some(long_info.clone()), "test");

    // bob (no PROCESS privilege) sees only his own thread.
    let mut bob = SessionContext::default();
    bob.user = "bob".to_string();
    bob.login_user = "bob".to_string();
    bob.connection_id = 2;
    bob.authenticated_as = crate::engine::auth::AccountKey::new("bob", "%");
    bob.auth_epoch = engine.db.auth_catalog().epoch;

    let QueryResponse::ResultSet(rs) = show_processlist_result(&engine.db, &bob, &registry, false)
    else {
        panic!("expected result set");
    };
    let users: Vec<String> = rs.rows.iter().map(|r| r[1].to_display_string()).collect();
    assert!(
        users.iter().all(|u| u == "bob"),
        "without PROCESS, only own foreground threads are visible; got {users:?}"
    );
    // Non-FULL Info is truncated to 100 *characters* without panicking.
    let bob_row = rs.rows.iter().find(|r| r[0] == Value::UInt(2)).unwrap();
    assert_eq!(
        bob_row[7].to_display_string().chars().count(),
        100,
        "Info must be truncated to 100 chars"
    );

    // FULL keeps the whole Info untruncated.
    let QueryResponse::ResultSet(full_rs) =
        show_processlist_result(&engine.db, &bob, &registry, true)
    else {
        panic!("expected result set");
    };
    let bob_full = full_rs
        .rows
        .iter()
        .find(|r| r[0] == Value::UInt(2))
        .unwrap();
    assert_eq!(bob_full[7], Value::String(long_info));

    // root (default session = full privileges) sees every thread.
    let QueryResponse::ResultSet(root_rs) =
        show_processlist_result(&engine.db, &root, &registry, false)
    else {
        panic!("expected result set");
    };
    let root_users: std::collections::BTreeSet<String> = root_rs
        .rows
        .iter()
        .map(|r| r[1].to_display_string())
        .collect();
    assert!(
        root_users.contains("bob") && root_users.contains("eve") && root_users.contains("root"),
        "with PROCESS, all threads are visible; got {root_users:?}"
    );
    let eve_row = root_rs
        .rows
        .iter()
        .find(|row| row[0] == Value::UInt(3))
        .unwrap();
    assert_eq!(eve_row[2], Value::String("h3:4567".into()));
    assert_eq!(eve_row[3], Value::Null);
    assert_eq!(eve_row[6], Value::Null);

    // Granting PROCESS lifts the scoping for bob.
    exec(&engine, &mut root, "GRANT PROCESS ON *.* TO 'bob'@'%'").unwrap();
    bob.auth_epoch = engine.db.auth_catalog().epoch;
    let QueryResponse::ResultSet(bob2) =
        show_processlist_result(&engine.db, &bob, &registry, false)
    else {
        panic!("expected result set");
    };
    let bob2_users: std::collections::BTreeSet<String> =
        bob2.rows.iter().map(|r| r[1].to_display_string()).collect();
    assert!(
        bob2_users.contains("eve"),
        "after GRANT PROCESS, bob sees other users' threads; got {bob2_users:?}"
    );
}

// ---------------------------------------------------------------------------
// M7-G10 SHOW {PROCEDURE|FUNCTION} CODE — clean 1235, not a raw 1064 syntax err
// ---------------------------------------------------------------------------

#[test]
fn show_procedure_and_function_code_report_unsupported_not_syntax_error() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    for sql in ["SHOW PROCEDURE CODE p", "SHOW FUNCTION CODE f"] {
        let err = exec(&engine, &mut session, sql).unwrap_err();
        assert_eq!(
            err.code(),
            1235,
            "{sql} should be a clean unsupported error"
        );
    }
}

// ---------------------------------------------------------------------------
// M6 SHOW TABLES / DATABASES — MySQL-style object visibility by privilege
// ---------------------------------------------------------------------------

#[test]
fn show_tables_and_databases_filter_by_account_privilege() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    exec(
        &engine,
        &mut root,
        "CREATE TABLE visible_t (id INT PRIMARY KEY)",
    )
    .unwrap();
    exec(
        &engine,
        &mut root,
        "CREATE TABLE hidden_t (id INT PRIMARY KEY)",
    )
    .unwrap();
    exec(
        &engine,
        &mut root,
        "CREATE USER 'carol'@'%' IDENTIFIED BY 'pw'",
    )
    .unwrap();
    exec(
        &engine,
        &mut root,
        "GRANT SELECT ON test.visible_t TO 'carol'@'%'",
    )
    .unwrap();

    let mut carol = SessionContext::default();
    carol.user = "carol".to_string();
    carol.login_user = "carol".to_string();
    carol.current_database = "test".to_string();
    carol.authenticated_as = crate::engine::auth::AccountKey::new("carol", "%");
    carol.auth_epoch = engine.db.auth_catalog().epoch;

    let QueryResponse::ResultSet(tables) = exec(&engine, &mut carol, "SHOW TABLES").unwrap() else {
        panic!("expected result set");
    };
    let names = first_col_strings(&tables);
    assert!(
        names.contains(&"visible_t".to_string()),
        "carol sees the table she has SELECT on; got {names:?}"
    );
    assert!(
        !names.contains(&"hidden_t".to_string()),
        "carol must not see a table she has no privilege on; got {names:?}"
    );

    // root still sees both (full privileges).
    let QueryResponse::ResultSet(root_tables) = exec(&engine, &mut root, "SHOW TABLES").unwrap()
    else {
        panic!("expected result set");
    };
    let root_names = first_col_strings(&root_tables);
    assert!(root_names.contains(&"visible_t".to_string()));
    assert!(root_names.contains(&"hidden_t".to_string()));

    // SHOW DATABASES: carol sees `test` (has a grant there) but not a schema
    // she holds nothing on.
    exec(&engine, &mut root, "CREATE DATABASE secret_db").unwrap();
    let QueryResponse::ResultSet(dbs) = exec(&engine, &mut carol, "SHOW DATABASES").unwrap() else {
        panic!("expected result set");
    };
    let db_names = first_col_strings(&dbs);
    assert!(db_names.contains(&"test".to_string()));
    assert!(
        !db_names.contains(&"secret_db".to_string()),
        "carol must not see a schema she has no privilege on; got {db_names:?}"
    );
}

// ---------------------------------------------------------------------------
// M5a SHOW GLOBAL STATUS — exporter-facing counters present with real sources
// ---------------------------------------------------------------------------

#[test]
fn show_global_status_exposes_exporter_counter_set() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "SHOW GLOBAL STATUS").unwrap()
    else {
        panic!("expected result set");
    };
    let names: std::collections::BTreeSet<String> =
        rs.rows.iter().map(|r| r[0].to_display_string()).collect();
    for required in [
        "Questions",
        "Queries",
        "Threads_connected",
        "Threads_running",
        "Threads_cached",
        "Uptime",
        "Uptime_since_flush_status",
        "Connections",
    ] {
        assert!(
            names.contains(required),
            "SHOW GLOBAL STATUS missing exporter counter {required}"
        );
    }
}

// MySQL 8.0-parity extended counter set — every new exporter-facing status
// variable must be present with a real name.
#[test]
fn show_global_status_exposes_extended_counter_set() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "SHOW GLOBAL STATUS").unwrap()
    else {
        panic!("expected result set");
    };
    let names: std::collections::BTreeSet<String> =
        rs.rows.iter().map(|r| r[0].to_display_string()).collect();
    for required in [
        // Network throughput
        "Bytes_received",
        "Bytes_sent",
        // Command counters (statement-level + protocol-level)
        "Com_select",
        "Com_insert",
        "Com_update",
        "Com_delete",
        "Com_commit",
        "Com_rollback",
        "Com_begin",
        "Com_set_option",
        "Com_stmt_prepare",
        "Com_stmt_execute",
        "Com_stmt_close",
        "Com_ping",
        // Handler / row counters
        "Handler_write",
        "Handler_update",
        "Handler_delete",
        "Handler_commit",
        "Handler_rollback",
        "Handler_read_rnd_next",
        "Innodb_rows_inserted",
        "Innodb_rows_updated",
        "Innodb_rows_deleted",
        "Innodb_rows_read",
        // Query execution
        "Slow_queries",
        "Table_locks_immediate",
        "Prepared_stmt_count",
        "Mydb_txn_admission_waits",
        "Mydb_txn_admission_wait_us",
        "Mydb_mixed_admission_waits",
        "Mydb_mixed_admission_wait_us",
        "Mydb_parallel_read_pressure_entries",
        "Mydb_parallel_read_pressure_suppressed_rw_txn",
        "Mydb_parallel_read_pressure_active",
    ] {
        assert!(
            names.contains(required),
            "SHOW GLOBAL STATUS missing exporter counter {required}"
        );
    }
}

#[test]
fn show_global_status_is_sorted_unique_and_fully_registered() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "SHOW GLOBAL STATUS").unwrap()
    else {
        panic!("expected result set");
    };
    let names = rs
        .rows
        .iter()
        .map(|row| row[0].to_display_string())
        .collect::<Vec<_>>();
    assert!(names.windows(2).all(|pair| pair[0] < pair[1]));
    for name in &names {
        assert!(
            crate::engine::status_counters::status_var_def(name).is_some(),
            "SHOW GLOBAL STATUS emitted unregistered variable {name}"
        );
    }
}

#[test]
fn binlog_diagnostic_status_names_match_pfs_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for name in [
        "Mydb_binlog_cached_events",
        "Mydb_binlog_cached_payload_bytes",
        "Mydb_binlog_file_opens",
    ] {
        let definition = crate::engine::status_counters::status_var_def(name).unwrap();
        assert!(definition.pfs_visible);
        let sql = format!("SHOW GLOBAL STATUS LIKE '{name}'");
        let QueryResponse::ResultSet(shown) = exec(&engine, &mut session, &sql).unwrap() else {
            panic!("expected SHOW result set");
        };
        let sql = format!("SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = '{name}'");
        let QueryResponse::ResultSet(pfs) = exec(&engine, &mut session, &sql).unwrap() else {
            panic!("expected PFS result set");
        };
        assert_eq!(shown.rows.len(), 1, "{name}");
        assert_eq!(pfs.rows.len(), 1, "{name}");
        assert_eq!(shown.rows[0][1], pfs.rows[0][0], "{name}");
    }
}

#[test]
fn innodb_row_lock_time_is_elapsed_time_not_timeout_setting() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let value = global_status_value(&engine, &mut session, "Innodb_row_lock_time");
    assert_eq!(value, 0, "fresh engine has not waited for a row lock");
}

#[test]
fn exact_status_like_skips_storage_snapshot_for_session_counter() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let before = crate::storage::mysql_status_snapshot_calls();
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SHOW SESSION STATUS LIKE 'Questions'",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
    assert_eq!(crate::storage::mysql_status_snapshot_calls(), before);
}

#[test]
fn show_status_where_errors_are_not_silently_filtered() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let err = exec(
        &engine,
        &mut session,
        "SHOW STATUS WHERE definitely_not_a_column = 1",
    )
    .unwrap_err();
    assert_eq!(err.code(), 1054);
}

/// Read a single `SHOW GLOBAL STATUS` counter value (0 if absent).
fn global_status_value(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    name: &str,
) -> u64 {
    let QueryResponse::ResultSet(rs) = exec(engine, session, "SHOW GLOBAL STATUS").unwrap() else {
        panic!("expected result set");
    };
    rs.rows
        .iter()
        .find(|r| r[0].to_display_string() == name)
        .map(|r| r[1].to_display_string().parse::<u64>().unwrap_or(0))
        .unwrap_or(0)
}

// Storage-sourced Handler_/Innodb_rows_ counters track real DML through the
// executor path. Counters are process-global, so assert monotonic deltas
// (parallel tests only ever add more), never absolute values.
#[test]
fn show_global_status_handler_counters_track_dml() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE TABLE t_status (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();

    let writes_before = global_status_value(&engine, &mut session, "Handler_write");
    let rows_ins_before = global_status_value(&engine, &mut session, "Innodb_rows_inserted");
    let commit_before = global_status_value(&engine, &mut session, "Handler_commit");

    exec(
        &engine,
        &mut session,
        "INSERT INTO t_status VALUES (1,10),(2,20),(3,30)",
    )
    .unwrap();

    let writes_after = global_status_value(&engine, &mut session, "Handler_write");
    let rows_ins_after = global_status_value(&engine, &mut session, "Innodb_rows_inserted");
    let commit_after = global_status_value(&engine, &mut session, "Handler_commit");

    assert!(
        writes_after >= writes_before + 3,
        "Handler_write should count 3 inserted rows ({writes_before} -> {writes_after})"
    );
    assert!(
        rows_ins_after >= rows_ins_before + 3,
        "Innodb_rows_inserted should count 3 rows ({rows_ins_before} -> {rows_ins_after})"
    );
    assert!(
        commit_after > commit_before,
        "Handler_commit should advance on the autocommit INSERT ({commit_before} -> {commit_after})"
    );
}
