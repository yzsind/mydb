//! MySQL 8.0 view compatibility — definition normalization, column type
//! persistence, DEFINER timing, and join-view updatability.
//!
//! Phases:
//! Phase 0: baseline SHOW CREATE VIEW, TEMPTABLE, CHECK OPTION, and DEFINER snapshots.
//! Phase 1: canonical definitions, quoted identifiers, qualified tables, star expansion, AS aliases, round trips.
//! Phase 2: persist inferred expression-column data types.
//! Phase 3: missing DEFINER timing, CREATE/ALTER warning 1449 and read error 1449.
//! Phase 4: updatable join views.

use super::support::execute_materialized as exec;
use super::*;
use crate::engine::auth::AccountKey;

fn show_create_view(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    name: &str,
) -> String {
    let QueryResponse::ResultSet(rs) =
        exec(engine, session, &format!("SHOW CREATE VIEW {name}")).unwrap()
    else {
        panic!("expected SHOW CREATE VIEW result set");
    };
    rs.rows[0][1].to_display_string()
}

fn view_definition_is(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    name: &str,
) -> String {
    let QueryResponse::ResultSet(rs) = exec(
        engine,
        session,
        &format!(
            "SELECT VIEW_DEFINITION FROM information_schema.views \
             WHERE TABLE_NAME = '{name}'"
        ),
    )
    .unwrap() else {
        panic!("expected IS.views result set");
    };
    assert!(!rs.rows.is_empty(), "view {name} not found in IS.views");
    rs.rows[0][0].to_display_string()
}

fn warning_codes(engine: &StorageQueryEngine, session: &mut SessionContext) -> Vec<i64> {
    let QueryResponse::ResultSet(rs) = exec(engine, session, "SHOW WARNINGS").unwrap() else {
        panic!("expected SHOW WARNINGS result set");
    };
    rs.rows
        .iter()
        .map(|r| match &r[1] {
            Value::Int(i) => *i,
            Value::UInt(u) => *u as i64,
            _ => -1,
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Phase 0: baseline snapshots.
// ---------------------------------------------------------------------------

#[test]
fn baseline_show_create_view_header_shape() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE VIEW v1 AS SELECT id, v FROM t WHERE v > 0",
    )
    .unwrap();

    let ddl = show_create_view(&engine, &mut session, "v1");
    // Header always carries ALGORITHM / DEFINER / SQL SECURITY / VIEW name.
    assert!(ddl.contains("ALGORITHM="), "{ddl}");
    assert!(ddl.contains("DEFINER=`root`@`localhost`"), "{ddl}");
    assert!(ddl.contains("SQL SECURITY DEFINER"), "{ddl}");
    assert!(ddl.contains("VIEW `v1`"), "{ddl}");
}

#[test]
fn baseline_temptable_materializes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO t VALUES (1, 10), (2, 20)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE ALGORITHM=TEMPTABLE VIEW vt AS SELECT id, v FROM t",
    )
    .unwrap();
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SELECT * FROM vt ORDER BY id").unwrap()
    else {
        panic!("result set");
    };
    assert_eq!(rs.rows.len(), 2);
    // A TEMPTABLE view is never updatable.
    let err = exec(&engine, &mut session, "INSERT INTO vt VALUES (3, 30)").unwrap_err();
    assert!(
        err.is(crate::error::catalog::ER_NON_INSERTABLE_TABLE),
        "got {err:?}"
    );
}

#[test]
fn baseline_check_option_local_and_cascaded() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE VIEW vpos AS SELECT id, v FROM t WHERE v > 0 WITH CASCADED CHECK OPTION",
    )
    .unwrap();
    // Violating row rejected with 1369.
    let err = exec(&engine, &mut session, "INSERT INTO vpos VALUES (1, -5)").unwrap_err();
    assert_eq!(err.code(), 1369, "got {err:?}");
    // Satisfying row accepted.
    exec(&engine, &mut session, "INSERT INTO vpos VALUES (2, 5)").unwrap();
}

// ---------------------------------------------------------------------------
// Phase 1: canonical view definition output.
// ---------------------------------------------------------------------------

#[test]
fn normalize_quotes_identifiers_and_qualifies_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE VIEW v1 AS SELECT id, v FROM t WHERE v > 0",
    )
    .unwrap();

    let ddl = show_create_view(&engine, &mut session, "v1");
    // Columns become `` `id` `` AS `` `id` ``; table becomes `` `test`.`t` ``.
    assert!(ddl.contains("`id` AS `id`"), "{ddl}");
    assert!(ddl.contains("`v` AS `v`"), "{ddl}");
    assert!(ddl.contains("from `test`.`t`"), "{ddl}");
    assert!(ddl.contains("where (`v` > 0)"), "{ddl}");
}

#[test]
fn normalize_expands_wildcard() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE VIEW vstar AS SELECT * FROM t",
    )
    .unwrap();

    let ddl = show_create_view(&engine, &mut session, "vstar");
    // `*` is expanded to explicit qualified columns (alias = table name `t`).
    assert!(ddl.contains("`t`.`id`"), "{ddl}");
    assert!(ddl.contains("`t`.`a`"), "{ddl}");
    assert!(ddl.contains("`t`.`b`"), "{ddl}");
    assert!(
        !ddl.contains(" AS SELECT *"),
        "wildcard must be expanded: {ddl}"
    );
}

#[test]
fn normalize_is_views_definition_matches() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    exec(&engine, &mut session, "CREATE VIEW v1 AS SELECT id FROM t").unwrap();

    let def = view_definition_is(&engine, &mut session, "v1");
    // IS.views.VIEW_DEFINITION carries the same canonical body (no header).
    assert!(def.contains("`id` AS `id`"), "{def}");
    assert!(def.contains("from `test`.`t`"), "{def}");
    assert!(!def.to_ascii_uppercase().contains("CREATE"), "{def}");
}

#[test]
fn normalize_round_trip_recreate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE VIEW v1 AS SELECT id, v FROM t WHERE v >= 1",
    )
    .unwrap();

    // Round-trip: SHOW CREATE → recreate as v2 → SHOW CREATE must match body.
    let ddl1 = show_create_view(&engine, &mut session, "v1");
    let body1 = ddl1.split(" AS ").nth(1).unwrap().to_string();
    let recreate = ddl1.replace("VIEW `v1`", "VIEW `v2`");
    exec(&engine, &mut session, &recreate).unwrap();
    let ddl2 = show_create_view(&engine, &mut session, "v2");
    let body2 = ddl2.split(" AS ").nth(1).unwrap().to_string();
    assert_eq!(body1, body2, "round-trip body mismatch:\n{ddl1}\n{ddl2}");
}

#[test]
fn normalize_falls_back_for_subquery_body() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    // A correlated/subquery body is not faithfully renderable → fall back to
    // raw text. Must still produce a working, non-empty definition.
    exec(
        &engine,
        &mut session,
        "CREATE VIEW vsub AS SELECT id FROM t WHERE v IN (SELECT v FROM t WHERE v > 0)",
    )
    .unwrap();
    let ddl = show_create_view(&engine, &mut session, "vsub");
    assert!(ddl.contains("VIEW `vsub`"), "{ddl}");
    // The view must remain queryable.
    exec(&engine, &mut session, "SELECT * FROM vsub").unwrap();
}

// ---------------------------------------------------------------------------
// Phase 3.2: missing DEFINER timing.
// ---------------------------------------------------------------------------

#[test]
fn create_view_missing_definer_is_warning_not_error() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    exec(&engine, &mut root, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();

    // DEFINER points at a user that does not exist yet — MySQL creates the
    // view and emits warning 1449 rather than failing.
    let resp = exec(
        &engine,
        &mut root,
        "CREATE DEFINER=`ghost`@`%` VIEW vg AS SELECT id FROM t",
    );
    assert!(
        resp.is_ok(),
        "CREATE with missing definer must succeed: {resp:?}"
    );
    let codes = warning_codes(&engine, &mut root);
    assert!(
        codes.contains(&1449),
        "expected warning 1449; got {codes:?}"
    );

    // Reading the view under DEFINER security must now raise error 1449.
    let err = exec(&engine, &mut root, "SELECT * FROM vg").unwrap_err();
    assert_eq!(err.code(), 1449, "read path must error 1449; got {err:?}");
}

#[test]
fn create_view_then_create_definer_user_migration_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    exec(&engine, &mut root, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    exec(&engine, &mut root, "INSERT INTO t VALUES (1)").unwrap();
    // Define the view before its definer exists (dump-restore order).
    exec(
        &engine,
        &mut root,
        "CREATE DEFINER=`later`@`%` VIEW vm AS SELECT id FROM t",
    )
    .unwrap();
    // Create the definer user; the view becomes usable.
    exec(
        &engine,
        &mut root,
        "CREATE USER 'later'@'%' IDENTIFIED BY 'pw'",
    )
    .unwrap();
    exec(&engine, &mut root, "GRANT SELECT ON test.* TO 'later'@'%'").unwrap();
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut root, "SELECT * FROM vm").unwrap() else {
        panic!("result set");
    };
    assert_eq!(rs.rows.len(), 1);
}

// ---------------------------------------------------------------------------
// Phase 3.1: guard information_schema.VIEWS behavior.
//
// information_schema row generation through rows_for_table_name receives only
// db and registry, not the session account; tables/columns/views lack a shared row-privilege filter.
// Filtering VIEWS alone would be inconsistent, so this phase conservatively retains
// the existing contract: VIEW_DEFINITION always contains the canonical definition, never NULL, and views remain visible.
// See phase 3.1 of docs/view-mysql80-compat-design.md.
// ---------------------------------------------------------------------------

#[test]
fn is_views_definition_is_always_populated_guard() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    exec(&engine, &mut session, "CREATE VIEW v AS SELECT id FROM t").unwrap();
    let def = view_definition_is(&engine, &mut session, "v");
    // Current behavior: definition is never NULL'd (no SHOW VIEW gating yet).
    assert!(!def.is_empty(), "VIEW_DEFINITION should be populated");
    assert!(def.contains("`id`"), "{def}");
}

// ---------------------------------------------------------------------------
// Phase 2: persist inferred view-column types.
// ---------------------------------------------------------------------------

/// `(DATA_TYPE, IS_NULLABLE)` for one column via information_schema.columns.
fn column_type_nullable(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    view: &str,
    col: &str,
) -> (String, String) {
    let QueryResponse::ResultSet(rs) = exec(
        engine,
        session,
        &format!(
            "SELECT DATA_TYPE, IS_NULLABLE FROM information_schema.columns \
             WHERE TABLE_NAME = '{view}' AND COLUMN_NAME = '{col}'"
        ),
    )
    .unwrap() else {
        panic!("expected IS.columns result set");
    };
    assert!(!rs.rows.is_empty(), "column {view}.{col} not found");
    (
        rs.rows[0][0].to_display_string(),
        rs.rows[0][1].to_display_string(),
    )
}

#[test]
fn column_type_direct_reference_preserved() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(32) NOT NULL, amt DECIMAL(10,2))",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE VIEW v AS SELECT id, name, amt FROM t",
    )
    .unwrap();
    assert_eq!(
        column_type_nullable(&engine, &mut session, "v", "id").0,
        "int"
    );
    let (ty, nul) = column_type_nullable(&engine, &mut session, "v", "name");
    assert_eq!(ty, "varchar");
    assert_eq!(nul, "NO", "NOT NULL column must stay non-nullable");
    assert_eq!(
        column_type_nullable(&engine, &mut session, "v", "amt").0,
        "decimal"
    );
}

#[test]
fn column_type_arithmetic_is_numeric() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE VIEW v AS SELECT a + b AS s, a * 2 AS d FROM t",
    )
    .unwrap();
    // Arithmetic over integers yields a numeric type (not the old varchar(255)
    // default). MySQL fixes these at CREATE time.
    let (ty, _) = column_type_nullable(&engine, &mut session, "v", "s");
    assert!(
        ty != "varchar",
        "arithmetic column must not fall back to varchar; got {ty}"
    );
}

#[test]
fn column_type_concat_is_text() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(16))",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE VIEW v AS SELECT CONCAT(name, '!') AS g FROM t",
    )
    .unwrap();
    let (ty, _) = column_type_nullable(&engine, &mut session, "v", "g");
    // String function result is a character type.
    assert!(
        ty.contains("char") || ty == "text" || ty == "longtext",
        "CONCAT column should be a string type; got {ty}"
    );
}

#[test]
fn column_type_count_is_integer() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, g INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE VIEW v AS SELECT g, COUNT(*) AS c FROM t GROUP BY g",
    )
    .unwrap();
    let (ty, _) = column_type_nullable(&engine, &mut session, "v", "c");
    assert!(
        ty == "bigint" || ty == "int",
        "COUNT(*) column should be an integer type; got {ty}"
    );
}

#[test]
fn column_type_left_join_widens_nullable() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE a (id INT PRIMARY KEY)").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE b (id INT PRIMARY KEY, v INT NOT NULL)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE VIEW v AS SELECT a.id AS aid, b.v AS bv FROM a LEFT JOIN b ON a.id = b.id",
    )
    .unwrap();
    // b.v is NOT NULL in the base table, but on the nullable side of a LEFT
    // JOIN it can be NULL in the view → IS_NULLABLE = YES.
    let (_, nul) = column_type_nullable(&engine, &mut session, "v", "bv");
    assert_eq!(nul, "YES", "LEFT JOIN right-side column must be nullable");
}

// ---------------------------------------------------------------------------
// Phase 4: conservatively retain rejection of JOIN-view DML.
//
// Correct JOIN-view DML must preserve joins and update only key-preserved tables; it cannot
// use single-base-table folding with flat column rewriting without silently modifying wrong rows.
// Retain SELECT support and DML error 1288; see design phase 4.
// ---------------------------------------------------------------------------

fn setup_join_view(engine: &StorageQueryEngine, session: &mut SessionContext) {
    exec(
        engine,
        session,
        "CREATE TABLE dept (did INT PRIMARY KEY, dname VARCHAR(32))",
    )
    .unwrap();
    exec(
        engine,
        session,
        "CREATE TABLE emp (eid INT PRIMARY KEY, ename VARCHAR(32), did INT)",
    )
    .unwrap();
    exec(
        engine,
        session,
        "INSERT INTO dept VALUES (1, 'eng'), (2, 'sales')",
    )
    .unwrap();
    exec(
        engine,
        session,
        "INSERT INTO emp VALUES (10, 'a', 1), (11, 'b', 2)",
    )
    .unwrap();
    exec(
        engine,
        session,
        "CREATE VIEW ev AS SELECT e.eid, e.ename, d.dname \
         FROM emp e JOIN dept d ON e.did = d.did",
    )
    .unwrap();
}

#[test]
fn join_view_select_works() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    setup_join_view(&engine, &mut session);
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SELECT * FROM ev ORDER BY eid").unwrap()
    else {
        panic!("result set");
    };
    assert_eq!(rs.rows.len(), 2);
}

#[test]
fn join_view_update_changes_only_its_target() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    setup_join_view(&engine, &mut session);
    exec(
        &engine,
        &mut session,
        "UPDATE ev SET ename = 'z' WHERE eid = 10",
    )
    .unwrap();
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SELECT ename FROM emp WHERE eid = 10",
    )
    .unwrap() else {
        panic!("result set");
    };
    assert_eq!(rs.rows[0][0], Value::String("z".into()));
}

#[test]
fn join_view_delete_is_rejected_1395() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    setup_join_view(&engine, &mut session);
    let err = exec(&engine, &mut session, "DELETE FROM ev WHERE eid = 10").unwrap_err();
    assert_eq!(
        err.code(),
        1395,
        "join-view DELETE must be rejected; got {err:?}"
    );
}

#[test]
fn join_view_insert_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    setup_join_view(&engine, &mut session);
    let err = exec(
        &engine,
        &mut session,
        "INSERT INTO ev (eid, ename, dname) VALUES (12, 'c', 'eng')",
    )
    .unwrap_err();
    // The explicit fields span two underlying tables: reject before writing.
    assert!(
        err.is(crate::error::catalog::ER_VIEW_MULTIUPDATE),
        "join-view INSERT must be rejected; got {err:?}"
    );
}

#[test]
fn single_table_updatable_view_still_works() {
    // Joined UPDATE support must retain the single-table updatable-view path.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE VIEW sv AS SELECT id, v FROM t WHERE v > 0",
    )
    .unwrap();
    exec(&engine, &mut session, "INSERT INTO sv VALUES (1, 5)").unwrap();
    exec(&engine, &mut session, "UPDATE sv SET v = 7 WHERE id = 1").unwrap();
    exec(&engine, &mut session, "DELETE FROM sv WHERE id = 1").unwrap();
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SELECT COUNT(*) FROM t").unwrap()
    else {
        panic!("result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(0));
}

// ---------------------------------------------------------------------------
// Phase 5: refine boundaries.
// ---------------------------------------------------------------------------

#[test]
fn insert_select_through_view_enforces_check_option() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE src (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO src VALUES (1, -5), (2, 9)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE VIEW vpos AS SELECT id, v FROM t WHERE v > 0 WITH CHECK OPTION",
    )
    .unwrap();
    // INSERT ... SELECT a row that violates v > 0 must raise 1369.
    let err = exec(
        &engine,
        &mut session,
        "INSERT INTO vpos SELECT id, v FROM src WHERE id = 1",
    )
    .unwrap_err();
    assert_eq!(
        err.code(),
        1369,
        "INSERT...SELECT must enforce CHECK OPTION; got {err:?}"
    );
    // A satisfying row goes through.
    exec(
        &engine,
        &mut session,
        "INSERT INTO vpos SELECT id, v FROM src WHERE id = 2",
    )
    .unwrap();
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SELECT COUNT(*) FROM t").unwrap()
    else {
        panic!("result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(1));
}

#[test]
fn replace_through_view_enforces_check_option() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE VIEW vpos AS SELECT id, v FROM t WHERE v > 0 WITH CHECK OPTION",
    )
    .unwrap();
    let err = exec(&engine, &mut session, "REPLACE INTO vpos VALUES (1, -1)").unwrap_err();
    assert_eq!(
        err.code(),
        1369,
        "REPLACE must enforce CHECK OPTION; got {err:?}"
    );
}

#[test]
fn odku_through_view_enforces_check_option() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    exec(&engine, &mut session, "INSERT INTO t VALUES (1, 5)").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE VIEW vpos AS SELECT id, v FROM t WHERE v > 0 WITH CHECK OPTION",
    )
    .unwrap();
    // ON DUPLICATE KEY UPDATE that drives the row out of the view must 1369.
    let err = exec(
        &engine,
        &mut session,
        "INSERT INTO vpos VALUES (1, 5) ON DUPLICATE KEY UPDATE v = -3",
    )
    .unwrap_err();
    assert_eq!(
        err.code(),
        1369,
        "ODKU must enforce CHECK OPTION; got {err:?}"
    );
}

#[test]
fn rename_table_invalidates_dependent_view() {
    // Guard current behavior: renaming a base table a view depends on marks the
    // view invalid (1356 on next read) rather than silently producing wrong
    // results.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    exec(&engine, &mut session, "CREATE VIEW v AS SELECT id FROM t").unwrap();
    exec(&engine, &mut session, "RENAME TABLE t TO t2").unwrap();
    let err = exec(&engine, &mut session, "SELECT * FROM v").unwrap_err();
    // Current behavior: a loud error (1356 invalid-view or 1146 missing-table),
    // never silent wrong results. Either is acceptable.
    assert!(
        err.is(crate::error::catalog::ER_VIEW_INVALID)
            || err.is(crate::error::catalog::ER_NO_SUCH_TABLE),
        "view over renamed table must raise a clear error; got {err:?}"
    );
}

#[test]
fn invoker_view_uses_invoker_account_for_nested_view() {
    // INVOKER security: a view referencing another INVOKER view runs the whole
    // chain under the caller's account. Define a chain and confirm a caller
    // lacking base privilege is denied (i.e. account propagates, not definer).
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    exec(
        &engine,
        &mut root,
        "CREATE TABLE secret (id INT PRIMARY KEY, x INT)",
    )
    .unwrap();
    exec(&engine, &mut root, "INSERT INTO secret VALUES (1, 42)").unwrap();
    exec(
        &engine,
        &mut root,
        "CREATE SQL SECURITY INVOKER VIEW iv AS SELECT id, x FROM secret",
    )
    .unwrap();
    exec(
        &engine,
        &mut root,
        "CREATE SQL SECURITY INVOKER VIEW iv2 AS SELECT id FROM iv",
    )
    .unwrap();
    exec(
        &engine,
        &mut root,
        "CREATE USER 'bob'@'%' IDENTIFIED BY 'pw'",
    )
    .unwrap();
    // Grant bob access to the views but NOT the base table.
    exec(&engine, &mut root, "GRANT SELECT ON test.iv TO 'bob'@'%'").unwrap();
    exec(&engine, &mut root, "GRANT SELECT ON test.iv2 TO 'bob'@'%'").unwrap();

    let mut bob = SessionContext::default();
    bob.authenticated_as = AccountKey::new("bob", "%");
    bob.current_database = "test".to_string();
    // Under INVOKER security, bob's own (missing) base privilege is checked →
    // access denied. (If it propagated definer rights it would succeed.)
    let err = exec(&engine, &mut bob, "SELECT * FROM iv2").unwrap_err();
    assert!(
        err.is(crate::error::catalog::ER_TABLEACCESS_DENIED_ERROR)
            || err.is(crate::error::catalog::ER_COLUMNACCESS_DENIED_ERROR)
            || err.is(crate::error::catalog::ER_VIEW_INVALID),
        "INVOKER nested view must enforce caller privilege; got {err:?}"
    );
}

fn audit_view_dir() -> tempfile::TempDir {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    tempfile::Builder::new()
        .prefix("view-audit-")
        .tempdir_in("/tmp/mydb")
        .unwrap()
}

#[test]
fn audit_view_literal_and_expression_regressions() {
    let dir = audit_view_dir();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE t(a INT PRIMARY KEY, b VARCHAR(80))",
        r"INSERT INTO t VALUES (1,'a\\b'),(2,'C:\\Users\\bob'),(3,'100%')",
        r"CREATE VIEW v AS SELECT a FROM t WHERE b='a\\b'",
    ] {
        exec(&engine, &mut session, sql).unwrap();
    }
    for sql in [
        "SELECT a FROM v",
        "WITH c AS (SELECT a FROM v) SELECT a FROM c",
        "SELECT (SELECT MAX(a) FROM v)",
        "SELECT a FROM t WHERE a=1 ORDER BY (SELECT MAX(a) FROM v)",
    ] {
        let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, sql).unwrap() else {
            panic!()
        };
        assert_eq!(rs.rows, vec![vec![Value::Int(1)]], "{sql}");
    }
    let ddl = show_create_view(&engine, &mut session, "v");
    exec(&engine, &mut session, "DROP VIEW v").unwrap();
    exec(&engine, &mut session, &ddl).unwrap();
    exec(
        &engine,
        &mut session,
        r"ALTER VIEW v AS SELECT a FROM t WHERE b='C:\\Users\\bob'",
    )
    .unwrap();
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "SELECT a FROM v").unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(2)]]);
    exec(
        &engine,
        &mut session,
        r"ALTER VIEW v AS SELECT a FROM t WHERE b LIKE '100\\%'",
    )
    .unwrap();
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "SELECT a FROM v").unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(3)]]);
}

#[test]
fn audit_view_recovery_union_and_dml() {
    let dir = audit_view_dir();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE t(a INT PRIMARY KEY,b VARCHAR(20))",
        "INSERT INTO t VALUES(1,'x'),(2,'y')",
        "CREATE VIEW v AS SELECT a AS va,b AS vb FROM t",
        "UPDATE v SET va=3 WHERE UPPER(vb)='Y'",
        "UPDATE v SET vb='z' WHERE va IN (SELECT 3)",
        "INSERT INTO v(va,vb) VALUES(1,'q') ON DUPLICATE KEY UPDATE vb=UPPER(VALUES(vb))",
        "CREATE VIEW vu AS SELECT a FROM t UNION SELECT a+10 FROM t",
        "CREATE VIEW vc AS WITH c AS (SELECT a FROM t) SELECT a FROM c",
    ] {
        exec(&engine, &mut session, sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SELECT a,b FROM t ORDER BY a").unwrap()
    else {
        panic!()
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(1), Value::String("Q".into())],
            vec![Value::Int(3), Value::String("z".into())]
        ]
    );
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SELECT a FROM vu ORDER BY a").unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows.len(), 4);
    exec(&engine, &mut session, "RENAME TABLE t TO t2").unwrap();
    assert!(exec(&engine, &mut session, "SELECT * FROM v").is_err());
    exec(&engine, &mut session, "RENAME TABLE t2 TO t").unwrap();
    exec(&engine, &mut session, "SELECT * FROM v").unwrap();
    exec(&engine, &mut session, "DROP TABLE t").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t(a INT PRIMARY KEY,b VARCHAR(20))",
    )
    .unwrap();
    exec(&engine, &mut session, "SELECT * FROM v").unwrap();
}

#[test]
fn audit_view_cte_scope_modes_and_prepare() {
    let dir = audit_view_dir();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE t(a INT PRIMARY KEY)",
        "INSERT INTO t VALUES(1)",
        "CREATE VIEW v AS SELECT a FROM t",
        "PREPARE s FROM 'CREATE VIEW prepared_v AS SELECT a FROM t LIMIT 1'",
        "EXECUTE s",
        "DEALLOCATE PREPARE s",
    ] {
        exec(&engine, &mut session, sql).unwrap();
    }
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "WITH v AS (SELECT 9 AS a) SELECT a FROM v",
    )
    .unwrap() else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(9)]]);
    let err = exec(
        &engine,
        &mut session,
        "PREPARE bad FROM 'CREATE VIEW bad AS SELECT a FROM t LIMIT ?'",
    )
    .unwrap_err();
    assert!(err.is(crate::error::catalog::ER_VIEW_SELECT_VARIABLE));
    exec(&engine, &mut session, "SET sql_mode='NO_BACKSLASH_ESCAPES'").unwrap();
    exec(
        &engine,
        &mut session,
        r"CREATE VIEW literal_v AS SELECT 'a\b' AS s",
    )
    .unwrap();
    exec(&engine, &mut session, "SET sql_mode=''").unwrap();
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SELECT s FROM literal_v").unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::String(r"a\b".into())]]);
}

#[test]
fn audit_union_view_preserves_wildcard_aliases_and_nested_views() {
    let dir = audit_view_dir();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE t(a INT,b VARCHAR(10))",
        "INSERT INTO t VALUES(1,'x')",
        "CREATE VIEW v AS SELECT * FROM t",
        "CREATE VIEW u(x,y) AS SELECT * FROM v UNION ALL SELECT * FROM t",
    ] {
        exec(&engine, &mut session, sql).unwrap();
    }
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "SELECT x,y FROM u").unwrap()
    else {
        panic!()
    };
    assert_eq!(
        rs.rows,
        vec![vec![Value::Int(1), Value::String("x".into())]; 2]
    );
}
