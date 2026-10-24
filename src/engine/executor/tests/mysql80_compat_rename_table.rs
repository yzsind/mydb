//! MySQL 8.0 RENAME TABLE/ALTER TABLE RENAME TO compatibility regressions.
//! Cover gaps from docs/rename-table-mysql80-compat-design.md:
//! G1: source ALTER+DROP and target CREATE+INSERT privileges; ALTER RENAME uses the same gate.
//! G2: RENAME TABLE supports catalog-only view renames.
//! G3: multiple rename pairs commit atomically, including idempotent swaps and failure rollback.
//! G4: swaps preserve views whose referenced names remain; rename-away invalidates dependent views.
//! G6: RENAME TABLE on temporary tables errors with an ALTER hint; same-name base tables take precedence.
//! G7: RENAME while holding LOCK TABLES returns 1192.
//! G8: a sequence source returns 1347; a sequence occupying the target returns 1050.

use super::support::result_rows as rows;
use super::*;
use crate::engine::auth::AccountKey;

fn table_set(engine: &StorageQueryEngine) -> std::collections::BTreeSet<String> {
    rows(engine.execute("SHOW TABLES").unwrap())
        .into_iter()
        .filter_map(|r| r.into_iter().next().map(|v| v.to_display_string()))
        .collect()
}

fn limited_user_session(engine: &StorageQueryEngine, user: &str) -> SessionContext {
    let mut s = SessionContext::default();
    s.user = user.to_string();
    s.login_user = user.to_string();
    s.authenticated_as = AccountKey::new(user, "%");
    s.auth_epoch = engine.db.auth_catalog().epoch;
    s
}

// -- G1: RENAME TABLE privilege gating -------------------------------------

#[test]
fn rename_table_requires_alter_drop_create_insert() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    for sql in [
        "CREATE TABLE a (id INT PRIMARY KEY)",
        "CREATE USER 'bob'@'%' IDENTIFIED BY 'pw'",
        // Everything a rename needs EXCEPT DROP on the source.
        "GRANT ALTER, CREATE, INSERT ON test.* TO 'bob'@'%'",
    ] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }

    let mut bob = limited_user_session(&engine, "bob");
    // Missing DROP on the source → denied.
    let denied = engine
        .execute_with_session(&mut bob, "RENAME TABLE a TO a2")
        .unwrap_err();
    assert_eq!(
        denied.code(),
        1142,
        "RENAME without DROP on source must be denied: {denied:?}"
    );

    // Grant the missing DROP → rename now succeeds.
    engine
        .execute_with_session(&mut root, "GRANT DROP ON test.* TO 'bob'@'%'")
        .unwrap();
    bob.auth_epoch = engine.db.auth_catalog().epoch;
    engine
        .execute_with_session(&mut bob, "RENAME TABLE a TO a2")
        .unwrap();
    assert!(table_set(&engine).contains("a2"));
}

#[test]
fn rename_table_requires_create_on_target() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    for sql in [
        "CREATE TABLE a (id INT PRIMARY KEY)",
        "CREATE USER 'bob'@'%' IDENTIFIED BY 'pw'",
        // ALTER + DROP on source, INSERT on target — but NOT CREATE.
        "GRANT ALTER, DROP, INSERT ON test.* TO 'bob'@'%'",
    ] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    let mut bob = limited_user_session(&engine, "bob");
    let denied = engine
        .execute_with_session(&mut bob, "RENAME TABLE a TO a2")
        .unwrap_err();
    assert_eq!(
        denied.code(),
        1142,
        "RENAME without CREATE on target must be denied: {denied:?}"
    );
}

// -- G1b: ALTER TABLE ... RENAME TO privilege gating ------------------------

#[test]
fn alter_table_rename_requires_rename_privileges() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    for sql in [
        "CREATE TABLE a (id INT PRIMARY KEY)",
        "CREATE USER 'bob'@'%' IDENTIFIED BY 'pw'",
        // ALTER alone is enough for most ALTER TABLE, but a RENAME needs
        // DROP + CREATE + INSERT too.
        "GRANT ALTER ON test.* TO 'bob'@'%'",
    ] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    let mut bob = limited_user_session(&engine, "bob");
    let denied = engine
        .execute_with_session(&mut bob, "ALTER TABLE a RENAME TO a2")
        .unwrap_err();
    assert_eq!(
        denied.code(),
        1142,
        "ALTER ... RENAME TO without DROP/CREATE/INSERT must be denied: {denied:?}"
    );

    engine
        .execute_with_session(
            &mut root,
            "GRANT DROP, CREATE, INSERT ON test.* TO 'bob'@'%'",
        )
        .unwrap();
    bob.auth_epoch = engine.db.auth_catalog().epoch;
    engine
        .execute_with_session(&mut bob, "ALTER TABLE a RENAME TO a2")
        .unwrap();
    assert!(table_set(&engine).contains("a2"));
}

// -- G2: RENAME TABLE on a view --------------------------------------------

#[test]
fn rename_table_renames_a_view() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1), (2)").unwrap();
    engine.execute("CREATE VIEW v AS SELECT id FROM t").unwrap();

    // 8.0 allows renaming a view via RENAME TABLE.
    engine.execute("RENAME TABLE v TO v2").unwrap();

    // The old name is gone; the new name resolves and still reads the base.
    let err = engine.execute("SELECT * FROM v").unwrap_err();
    assert_eq!(err.code(), 1146, "old view name must be gone: {err:?}");
    assert_eq!(rows(engine.execute("SELECT * FROM v2").unwrap()).len(), 2);
}

#[test]
fn rename_table_view_onto_existing_base_is_1050() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("CREATE VIEW v AS SELECT id FROM t").unwrap();
    // `t` already exists as a base table → target occupied.
    let err = engine.execute("RENAME TABLE v TO t").unwrap_err();
    assert_eq!(
        err.code(),
        1050,
        "renaming a view onto a base name: {err:?}"
    );
}

// -- G3: multi-pair atomicity + swap ---------------------------------------

#[test]
fn rename_table_swap_idiom_is_atomic() {
    let dir = tempfile::tempdir().unwrap();
    // Strict (real-disk) durability: the swap moves base-table heap files, so
    // the data must survive. (Under DurabilityMode::Memory the buffer pool is
    // the only copy and the rename's cache-discard drops it — a known,
    // separately-tracked memory-mode-only bug; see memory-mode-rename-dataloss.)
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO a VALUES (1)").unwrap();
    engine.execute("INSERT INTO b VALUES (2)").unwrap();

    // Classic three-step atomic swap through a temporary name.
    engine
        .execute("RENAME TABLE a TO tmp_swap, b TO a, tmp_swap TO b")
        .unwrap();

    // Data followed the names: `a` now holds b's row, `b` holds a's row.
    assert_eq!(
        rows(engine.execute("SELECT id FROM a").unwrap())[0][0].to_display_string(),
        "2"
    );
    assert_eq!(
        rows(engine.execute("SELECT id FROM b").unwrap())[0][0].to_display_string(),
        "1"
    );
    let tables = table_set(&engine);
    assert!(tables.contains("a") && tables.contains("b"));
    assert!(!tables.contains("tmp_swap"));
}

#[test]
fn rename_table_multi_pair_failure_leaves_everything_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE a (id INT)").unwrap();
    engine.execute("CREATE TABLE b (id INT)").unwrap();
    engine.execute("CREATE TABLE c (id INT)").unwrap();

    // Second pair's target `c` is a still-existing, untouched table → 1050.
    // (The first pair `a TO a2` must then roll back.)
    let err = engine.execute("RENAME TABLE a TO a2, b TO c").unwrap_err();
    assert_eq!(err.code(), 1050, "{}", err.message());

    // Nothing applied: original names intact, no half-committed pair.
    assert_eq!(
        table_set(&engine),
        ["a", "b", "c"].iter().map(|s| s.to_string()).collect()
    );
}

// -- G4: view (in)validation across rename ---------------------------------

#[test]
fn rename_table_swap_keeps_view_over_surviving_name_valid() {
    let dir = tempfile::tempdir().unwrap();
    // Strict mode: the swap moves base-table heap files (see the swap-idiom
    // test) so the view reads real post-swap data.
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO a VALUES (10)").unwrap();
    engine.execute("INSERT INTO b VALUES (20)").unwrap();
    // View references name `a`, which still exists after the swap.
    engine
        .execute("CREATE VIEW va AS SELECT id FROM a")
        .unwrap();

    engine
        .execute("RENAME TABLE a TO tmp_swap, b TO a, tmp_swap TO b")
        .unwrap();

    // `a` still exists (now b's old data) → the view stays valid and reads it.
    let out = rows(engine.execute("SELECT id FROM va").unwrap());
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0].to_display_string(), "20");
}

#[test]
fn rename_table_away_invalidates_dependent_view() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("CREATE VIEW v AS SELECT id FROM t").unwrap();
    // `t` is renamed away and never re-created → dependent view must fail loud.
    engine.execute("RENAME TABLE t TO t2").unwrap();
    let err = engine.execute("SELECT * FROM v").unwrap_err();
    assert!(
        err.is(crate::error::catalog::ER_VIEW_INVALID)
            || err.is(crate::error::catalog::ER_NO_SUCH_TABLE),
        "view over renamed-away base must raise a clear error; got {err:?}"
    );
}

// -- G6: TEMPORARY tables --------------------------------------------------

#[test]
fn rename_table_on_temporary_only_name_is_rejected_with_hint() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TEMPORARY TABLE tmp (id INT)")
        .unwrap();
    let err = engine
        .execute_with_session(&mut session, "RENAME TABLE tmp TO tmp2")
        .unwrap_err();
    assert_eq!(err.code(), 1146, "{}", err.message());
    assert!(
        err.message().contains("TEMPORARY") && err.message().contains("ALTER TABLE"),
        "temp-table RENAME should point at ALTER TABLE: {err:?}"
    );
}

#[test]
fn rename_table_prefers_base_table_over_shadowing_temp() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    // A base table and a session temp table share the name `s`.
    engine
        .execute_with_session(&mut session, "CREATE TABLE s (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO s VALUES (7)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TEMPORARY TABLE s (id INT)")
        .unwrap();
    // RENAME TABLE targets the *base* namespace — the base table moves.
    engine
        .execute_with_session(&mut session, "RENAME TABLE s TO s_base")
        .unwrap();
    assert!(table_set(&engine).contains("s_base"));
}

// -- G7: LOCK TABLES -------------------------------------------------------

#[test]
fn rename_table_under_lock_tables_is_1192() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE a (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "LOCK TABLES a WRITE")
        .unwrap();
    let err = engine
        .execute_with_session(&mut session, "RENAME TABLE a TO a2")
        .unwrap_err();
    assert_eq!(
        err.code(),
        1192,
        "RENAME with active LOCK TABLES must be rejected: {err:?}"
    );

    // After UNLOCK the rename succeeds.
    engine
        .execute_with_session(&mut session, "UNLOCK TABLES")
        .unwrap();
    engine
        .execute_with_session(&mut session, "RENAME TABLE a TO a2")
        .unwrap();
    assert!(table_set(&engine).contains("a2"));
}

#[test]
fn alter_table_under_lock_tables_write_is_allowed() {
    // mysqldump wraps each table's data section in
    //   LOCK TABLES `t` WRITE; ALTER TABLE `t` DISABLE KEYS; INSERT …;
    //   ALTER TABLE `t` ENABLE KEYS; UNLOCK TABLES;
    // MySQL accepts ALTER TABLE / INSERT on the WRITE-locked table, so mydb
    // must not reject them with 1192.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "LOCK TABLES t WRITE")
        .unwrap();
    // ALTER on the held table + INSERT both succeed.
    engine
        .execute_with_session(&mut session, "ALTER TABLE t DISABLE KEYS")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1, 10)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "ALTER TABLE t ENABLE KEYS")
        .unwrap();
    engine
        .execute_with_session(&mut session, "ALTER TABLE t ADD COLUMN w INT")
        .unwrap();
    engine
        .execute_with_session(&mut session, "UNLOCK TABLES")
        .unwrap();
}

#[test]
fn alter_table_under_lock_tables_on_unlocked_table_is_1100() {
    // DDL on a table that is NOT part of the lock set is rejected — the target
    // must be held (MySQL ER_TABLE_NOT_LOCKED 1100), not a blanket 1192.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE a (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE b (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "LOCK TABLES a WRITE")
        .unwrap();
    let err = engine
        .execute_with_session(&mut session, "ALTER TABLE b ADD COLUMN x INT")
        .unwrap_err();
    assert_eq!(
        err.code(),
        1100,
        "ALTER on an unlocked table under LOCK TABLES must be 1100: {err:?}"
    );
    engine
        .execute_with_session(&mut session, "UNLOCK TABLES")
        .unwrap();
}

// -- G8: sequences ---------------------------------------------------------

#[test]
fn rename_table_on_sequence_source_is_1347() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE SEQUENCE seq_a").unwrap();
    let err = engine.execute("RENAME TABLE seq_a TO seq_b").unwrap_err();
    assert_eq!(
        err.code(),
        1347,
        "RENAME TABLE on a sequence must be ER_WRONG_OBJECT: {err:?}"
    );
}

#[test]
fn rename_table_onto_sequence_name_is_1050() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("CREATE SEQUENCE seq_a").unwrap();
    let err = engine.execute("RENAME TABLE t TO seq_a").unwrap_err();
    assert_eq!(
        err.code(),
        1050,
        "renaming a table onto a sequence name must be 1050: {err:?}"
    );
}
