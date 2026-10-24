use super::support::execute_materialized as exec;
use super::*;

#[test]
fn before_after_insert_triggers_can_mutate_new_and_log() {
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
        "CREATE TABLE log (msg VARCHAR(32), id INT, v INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TRIGGER bi_t BEFORE INSERT ON t FOR EACH ROW SET NEW.v = NEW.v + 10",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TRIGGER ai_t AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES ('ins', NEW.id, NEW.v)",
    )
    .unwrap();

    exec(&engine, &mut session, "INSERT INTO t VALUES (1, 5)").unwrap();
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "SELECT id, v FROM t").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1), Value::Int(15)]]);
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SELECT msg, id, v FROM log").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![vec![
            Value::String("ins".to_string()),
            Value::Int(1),
            Value::Int(15)
        ]]
    );
}

#[test]
fn update_and_delete_triggers_see_old_and_new_rows() {
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
        "CREATE TABLE log (event VARCHAR(16), old_v INT, new_v INT)",
    )
    .unwrap();
    exec(&engine, &mut session, "INSERT INTO t VALUES (1, 10)").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TRIGGER bu_t BEFORE UPDATE ON t FOR EACH ROW SET NEW.v = NEW.v + 1",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TRIGGER au_t AFTER UPDATE ON t FOR EACH ROW INSERT INTO log VALUES ('upd', OLD.v, NEW.v)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TRIGGER bd_t BEFORE DELETE ON t FOR EACH ROW INSERT INTO log VALUES ('del', OLD.v, NULL)",
    )
    .unwrap();

    exec(&engine, &mut session, "UPDATE t SET v = 20 WHERE id = 1").unwrap();
    exec(&engine, &mut session, "DELETE FROM t WHERE id = 1").unwrap();
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SELECT event, old_v, new_v FROM log ORDER BY event DESC",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![
                Value::String("upd".to_string()),
                Value::Int(10),
                Value::Int(21)
            ],
            vec![
                Value::String("del".to_string()),
                Value::Int(21),
                Value::Null
            ],
        ]
    );
}

#[test]
fn partitioned_ordered_limit_triggers_use_exact_candidate_partition() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE p (id INT, part_key INT, v INT) \
         PARTITION BY HASH(part_key) PARTITIONS 2",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE log (event VARCHAR(8), id INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TRIGGER au_p AFTER UPDATE ON p FOR EACH ROW \
         INSERT INTO log VALUES ('upd', NEW.id)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TRIGGER ad_p AFTER DELETE ON p FOR EACH ROW \
         INSERT INTO log VALUES ('del', OLD.id)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO p VALUES (1,1,10),(2,2,100)",
    )
    .unwrap();

    exec(
        &engine,
        &mut session,
        "UPDATE p SET v=v+1 ORDER BY v DESC LIMIT 1",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "DELETE FROM p ORDER BY v ASC LIMIT 1",
    )
    .unwrap();
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SELECT event,id FROM log ORDER BY event DESC",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::String("upd".into()), Value::Int(2)],
            vec![Value::String("del".into()), Value::Int(1)],
        ]
    );
}

#[test]
fn before_update_trigger_changes_unassigned_indexed_columns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, k INT NOT NULL, v INT, UNIQUE KEY k_uniq (k))",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO t VALUES (1, 10, 1), (2, 20, 2)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TRIGGER bu_t BEFORE UPDATE ON t FOR EACH ROW SET NEW.k = NEW.v * 10",
    )
    .unwrap();

    exec(&engine, &mut session, "UPDATE t SET v = 3 WHERE id = 1").unwrap();
    let QueryResponse::ResultSet(new_key) = exec(
        &engine,
        &mut session,
        "SELECT id, k, v FROM t FORCE INDEX (k_uniq) WHERE k = 30",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(
        new_key.rows,
        vec![vec![Value::Int(1), Value::Int(30), Value::Int(3)]]
    );
    let QueryResponse::ResultSet(old_key) = exec(
        &engine,
        &mut session,
        "SELECT id, k, v FROM t FORCE INDEX (k_uniq) WHERE k = 10",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert!(old_key.rows.is_empty());

    let err = exec(&engine, &mut session, "UPDATE t SET v = 2 WHERE id = 1").unwrap_err();
    assert_eq!(err.code(), 1062);
    let QueryResponse::ResultSet(after_error) =
        exec(&engine, &mut session, "SELECT id, k, v FROM t WHERE id = 1").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        after_error.rows,
        vec![vec![Value::Int(1), Value::Int(30), Value::Int(3)]]
    );
}

#[test]
fn trigger_ordering_and_metadata_are_visible() {
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
        "CREATE TRIGGER a_t BEFORE INSERT ON t FOR EACH ROW SET NEW.v = NEW.v + 1",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TRIGGER b_t BEFORE INSERT ON t FOR EACH ROW FOLLOWS a_t SET NEW.v = NEW.v * 2",
    )
    .unwrap();
    exec(&engine, &mut session, "INSERT INTO t VALUES (1, 3)").unwrap();

    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "SELECT v FROM t").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(8)]]);

    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SELECT trigger_name, action_order FROM information_schema.triggers \
         WHERE trigger_schema = 'test' ORDER BY action_order",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::String("a_t".to_string()), Value::Int(1)],
            vec![Value::String("b_t".to_string()), Value::Int(2)],
        ]
    );

    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SHOW TRIGGERS LIKE 't'").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 2);

    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SHOW TRIGGERS WHERE `Trigger` LIKE 'a_%'",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
    assert_eq!(rs.rows[0][0], Value::String("a_t".to_string()));
}

#[test]
fn triggers_reject_result_sets_and_same_table_writes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(&engine, &mut session, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    let err = exec(
        &engine,
        &mut session,
        "CREATE TRIGGER bad_select BEFORE INSERT ON t FOR EACH ROW SELECT NEW.id",
    )
    .unwrap_err();
    assert_eq!(err.code(), 1415);

    exec(
        &engine,
        &mut session,
        "CREATE TRIGGER bad_self AFTER INSERT ON t FOR EACH ROW INSERT INTO t VALUES (NEW.id + 1)",
    )
    .unwrap();
    let err = exec(&engine, &mut session, "INSERT INTO t VALUES (1)").unwrap_err();
    assert_eq!(err.code(), 1442);
}

#[test]
fn duplicate_key_update_and_replace_fire_trigger_paths() {
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
        "CREATE TABLE log (event VARCHAR(16), old_v INT, new_v INT)",
    )
    .unwrap();
    exec(&engine, &mut session, "INSERT INTO t VALUES (1, 10)").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TRIGGER au_t AFTER UPDATE ON t FOR EACH ROW INSERT INTO log VALUES ('upd', OLD.v, NEW.v)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TRIGGER bd_t BEFORE DELETE ON t FOR EACH ROW INSERT INTO log VALUES ('del', OLD.v, NULL)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TRIGGER ai_t AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES ('ins', NULL, NEW.v)",
    )
    .unwrap();

    exec(
        &engine,
        &mut session,
        "INSERT INTO t VALUES (1, 2) ON DUPLICATE KEY UPDATE v = VALUES(v) + 5",
    )
    .unwrap();
    exec(&engine, &mut session, "REPLACE INTO t VALUES (1, 99)").unwrap();

    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SELECT event, old_v, new_v FROM log ORDER BY event, new_v",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::String("del".to_string()), Value::Int(7), Value::Null],
            vec![
                Value::String("ins".to_string()),
                Value::Null,
                Value::Int(99)
            ],
            vec![
                Value::String("upd".to_string()),
                Value::Int(10),
                Value::Int(7)
            ],
        ]
    );
}

#[test]
fn load_data_uses_trigger_aware_insert_path() {
    let dir = tempfile::tempdir().unwrap();
    let data_path = dir.path().join("rows.tsv");
    std::fs::write(&data_path, "1\t5\n2\t7\n").unwrap();
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
        "CREATE TRIGGER bi_t BEFORE INSERT ON t FOR EACH ROW SET NEW.v = NEW.v + 100",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        &format!(
            "LOAD DATA INFILE '{}' INTO TABLE t",
            crate::util::file::sql_path(&data_path)
        ),
    )
    .unwrap();

    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SELECT id, v FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(1), Value::Int(105)],
            vec![Value::Int(2), Value::Int(107)],
        ]
    );
}

// ── A1 — multi-table UPDATE / DELETE fire triggers ──────────────────────────

#[test]
fn multi_table_update_fires_before_and_after_triggers() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE TABLE t1 (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t2 (id INT PRIMARY KEY, w INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE log (msg VARCHAR(16), old_v INT, new_v INT)",
    )
    .unwrap();
    exec(&engine, &mut session, "INSERT INTO t1 VALUES (1, 10)").unwrap();
    exec(&engine, &mut session, "INSERT INTO t2 VALUES (1, 100)").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TRIGGER bu BEFORE UPDATE ON t1 FOR EACH ROW SET NEW.v = NEW.v + 1",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TRIGGER au AFTER UPDATE ON t1 FOR EACH ROW INSERT INTO log VALUES ('upd', OLD.v, NEW.v)",
    )
    .unwrap();

    exec(
        &engine,
        &mut session,
        "UPDATE t1 JOIN t2 ON t1.id = t2.id SET t1.v = t2.w WHERE t2.w = 100",
    )
    .unwrap();

    // SET t1.v = 100, then BEFORE UPDATE trigger adds 1 → 101.
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "SELECT v FROM t1").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(101)]]);
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SELECT msg, old_v, new_v FROM log").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![vec![
            Value::String("upd".to_string()),
            Value::Int(10),
            Value::Int(101)
        ]]
    );
}

#[test]
fn multi_table_delete_fires_before_and_after_triggers() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE TABLE t1 (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t2 (id INT PRIMARY KEY)",
    )
    .unwrap();
    exec(&engine, &mut session, "CREATE TABLE log (v INT)").unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO t1 VALUES (1, 10), (2, 20)",
    )
    .unwrap();
    exec(&engine, &mut session, "INSERT INTO t2 VALUES (1)").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TRIGGER bd BEFORE DELETE ON t1 FOR EACH ROW INSERT INTO log VALUES (OLD.v)",
    )
    .unwrap();

    exec(
        &engine,
        &mut session,
        "DELETE t1 FROM t1 JOIN t2 ON t1.id = t2.id",
    )
    .unwrap();

    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "SELECT v FROM log").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(10)]]);
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SELECT id FROM t1 ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(2)]]);
}

// ── A2 — trigger target must be a base table ─────────────────────────────────

#[test]
fn create_trigger_on_view_reports_1347() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE TABLE base (id INT PRIMARY KEY)",
    )
    .unwrap();
    exec(&engine, &mut session, "CREATE VIEW v AS SELECT * FROM base").unwrap();
    let err = exec(
        &engine,
        &mut session,
        "CREATE TRIGGER x BEFORE INSERT ON v FOR EACH ROW SET NEW.id = 1",
    )
    .unwrap_err();
    assert_eq!(err.code(), 1347);
}

#[test]
fn create_trigger_on_temporary_table_reports_1361() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE TEMPORARY TABLE tmp (id INT PRIMARY KEY)",
    )
    .unwrap();
    let err = exec(
        &engine,
        &mut session,
        "CREATE TRIGGER y BEFORE INSERT ON tmp FOR EACH ROW SET NEW.id = 1",
    )
    .unwrap_err();
    assert_eq!(err.code(), 1361);
}

// ── A3 — cross-schema RENAME TABLE with triggers is rejected ─────────────────

#[test]
fn cross_db_rename_of_table_with_trigger_reports_1435() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(&engine, &mut session, "CREATE DATABASE db1").unwrap();
    exec(&engine, &mut session, "CREATE DATABASE db2").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE db1.t (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TRIGGER db1.tr BEFORE INSERT ON db1.t FOR EACH ROW SET NEW.v = NEW.v + 1",
    )
    .unwrap();

    let err = exec(&engine, &mut session, "RENAME TABLE db1.t TO db2.t").unwrap_err();
    assert_eq!(err.code(), 1435);

    // Same-schema rename of a triggered table still works.
    exec(&engine, &mut session, "RENAME TABLE db1.t TO db1.t2").unwrap();
    exec(&engine, &mut session, "INSERT INTO db1.t2 VALUES (1, 5)").unwrap();
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "SELECT v FROM db1.t2").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(6)]]);
}

// ── B1 — sql_mode captured at CREATE time and replayed at fire time ──────────

#[test]
fn trigger_captures_and_replays_sql_mode() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, a VARCHAR(8), b VARCHAR(8), c VARCHAR(16))",
    )
    .unwrap();
    exec(&engine, &mut session, "SET sql_mode = 'PIPES_AS_CONCAT'").unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TRIGGER bi BEFORE INSERT ON t FOR EACH ROW SET NEW.c = NEW.a || NEW.b",
    )
    .unwrap();
    // Reset the session mode: without replay, `||` reverts to logical OR.
    exec(&engine, &mut session, "SET sql_mode = ''").unwrap();

    exec(
        &engine,
        &mut session,
        "INSERT INTO t VALUES (1, 'x', 'y', '')",
    )
    .unwrap();
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "SELECT c FROM t").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::String("xy".to_string())]]);

    // SHOW CREATE TRIGGER echoes the captured sql_mode (column index 1).
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SHOW CREATE TRIGGER bi").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][1], Value::String("PIPES_AS_CONCAT".to_string()));
}

// ── B2 — illegal OLD/NEW pseudo-row references report 1363 ───────────────────

#[test]
fn old_reference_in_insert_trigger_reports_1363() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    let err = exec(
        &engine,
        &mut session,
        "CREATE TRIGGER bi BEFORE INSERT ON t FOR EACH ROW SET @x = OLD.v",
    )
    .unwrap_err();
    assert_eq!(err.code(), 1363);
}

#[test]
fn new_reference_in_delete_trigger_reports_1363() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    exec(&engine, &mut session, "CREATE TABLE log (v INT)").unwrap();
    exec(&engine, &mut session, "INSERT INTO t VALUES (1, 5)").unwrap();
    // Illegal NEW reference lives inside a sub-statement, exercising the
    // bind_expr_locals path rather than the direct SET path.
    let err = exec(
        &engine,
        &mut session,
        "CREATE TRIGGER bd BEFORE DELETE ON t FOR EACH ROW INSERT INTO log VALUES (NEW.v)",
    )
    .unwrap_err();
    assert_eq!(err.code(), 1363);
}

// ── B3 — foreign DEFINER requires SET_USER_ID ────────────────────────────────

#[test]
fn foreign_definer_trigger_requires_set_user_id() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();

    exec(
        &engine,
        &mut root,
        "CREATE USER 'alice'@'%' IDENTIFIED BY 'pw'",
    )
    .unwrap();
    exec(&engine, &mut root, "GRANT TRIGGER ON test.* TO 'alice'@'%'").unwrap();
    exec(
        &engine,
        &mut root,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();

    let mut alice = SessionContext::default();
    alice.user = "alice".to_string();
    alice.login_user = "alice".to_string();
    alice.authenticated_as = crate::engine::auth::AccountKey::new("alice", "%");
    alice.auth_epoch = engine.db.auth_catalog().epoch;

    let err = exec(
        &engine,
        &mut alice,
        "CREATE DEFINER = 'bob'@'%' TRIGGER bad BEFORE INSERT ON t FOR EACH ROW SET NEW.v = 1",
    )
    .unwrap_err();
    assert_eq!(err.code(), 1227);

    // Her own account as DEFINER is permitted.
    exec(
        &engine,
        &mut alice,
        "CREATE DEFINER = 'alice'@'%' TRIGGER mine BEFORE INSERT ON t FOR EACH ROW SET NEW.v = 1",
    )
    .unwrap();
}

// ── B4 — generated columns readable in OLD/NEW, assignment rejected ──────────

#[test]
fn generated_column_visible_in_trigger_and_assignment_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, b INT, g INT GENERATED ALWAYS AS (b * 2) STORED)",
    )
    .unwrap();
    exec(&engine, &mut session, "CREATE TABLE log (gval INT)").unwrap();
    // AFTER INSERT can read the computed generated column value.
    exec(
        &engine,
        &mut session,
        "CREATE TRIGGER ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES (NEW.g)",
    )
    .unwrap();
    exec(&engine, &mut session, "INSERT INTO t (id, b) VALUES (1, 5)").unwrap();
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "SELECT gval FROM log").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(10)]]);

    // A BEFORE UPDATE trigger that only touches the base column still lets the
    // storage layer recompute the generated column.
    exec(
        &engine,
        &mut session,
        "CREATE TRIGGER bu BEFORE UPDATE ON t FOR EACH ROW SET NEW.b = NEW.b + 5",
    )
    .unwrap();
    exec(&engine, &mut session, "UPDATE t SET id = id WHERE id = 1").unwrap();
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "SELECT b, g FROM t").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(10), Value::Int(20)]]);

    // Assigning a generated NEW column is rejected with 3105.
    exec(
        &engine,
        &mut session,
        "CREATE TRIGGER bad BEFORE INSERT ON t FOR EACH ROW SET NEW.g = 99",
    )
    .unwrap();
    let err = exec(&engine, &mut session, "INSERT INTO t (id, b) VALUES (2, 3)").unwrap_err();
    assert_eq!(err.code(), 3105);
}

// ── C2 — triggers fire on partitioned tables ─────────────────────────────────

#[test]
fn triggers_fire_on_partitioned_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE TABLE p (id INT PRIMARY KEY, v INT) PARTITION BY RANGE (id) (\
         PARTITION p0 VALUES LESS THAN (10), \
         PARTITION p1 VALUES LESS THAN (100))",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE log (ev VARCHAR(8), v INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TRIGGER bi BEFORE INSERT ON p FOR EACH ROW SET NEW.v = NEW.v + 1",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TRIGGER au AFTER UPDATE ON p FOR EACH ROW INSERT INTO log VALUES ('upd', NEW.v)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TRIGGER ad AFTER DELETE ON p FOR EACH ROW INSERT INTO log VALUES ('del', OLD.v)",
    )
    .unwrap();

    exec(
        &engine,
        &mut session,
        "INSERT INTO p VALUES (5, 10), (50, 20)",
    )
    .unwrap();
    exec(&engine, &mut session, "UPDATE p SET v = 100 WHERE id = 5").unwrap();
    exec(&engine, &mut session, "DELETE FROM p WHERE id = 50").unwrap();

    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, "SELECT id, v FROM p").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(5), Value::Int(100)]]);
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SELECT ev, v FROM log ORDER BY ev").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::String("del".to_string()), Value::Int(21)],
            vec![Value::String("upd".to_string()), Value::Int(100)],
        ]
    );
}

// ── B5 — DATABASE_COLLATION reflects the schema collation ────────────────────

#[test]
fn trigger_metadata_populates_database_collation() {
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
        "CREATE TRIGGER bi BEFORE INSERT ON t FOR EACH ROW SET NEW.v = NEW.v + 1",
    )
    .unwrap();
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SELECT database_collation FROM information_schema.triggers \
         WHERE trigger_name = 'bi'",
    )
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![vec![Value::String("utf8mb4_0900_ai_ci".to_string())]]
    );
}

#[test]
fn trigger_audit_partition_scope_isolation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in [
        "CREATE TABLE a (id INT PRIMARY KEY, v INT) PARTITION BY HASH(id) PARTITIONS 4",
        "CREATE TABLE b (id INT PRIMARY KEY, v INT) PARTITION BY HASH(id) PARTITIONS 4",
        "CREATE TABLE logs (n INT, v INT)",
        "INSERT INTO a VALUES (1,1),(2,2),(3,3),(4,4)",
        "INSERT INTO b VALUES (11,11),(12,12),(13,13),(14,14)",
        "CREATE TRIGGER au AFTER UPDATE ON a FOR EACH ROW BEGIN INSERT INTO logs VALUES ((SELECT COUNT(*) FROM b), (SELECT v FROM b WHERE id=13)); UPDATE b SET v=7 WHERE id=12; END",
        "UPDATE a SET v=v+1",
    ] { exec(&engine, &mut s, sql).unwrap(); }
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut s, "SELECT * FROM logs").unwrap() else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(4), Value::Int(13)]; 4]);
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut s, "SELECT v FROM b WHERE id=12").unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(7)]]);
    for sql in [
        "CREATE TRIGGER bd BEFORE DELETE ON a FOR EACH ROW INSERT INTO logs VALUES ((SELECT COUNT(*) FROM b), 0)",
        "REPLACE INTO a VALUES (1,100)",
        "DELETE FROM a",
    ] { exec(&engine, &mut s, sql).unwrap(); }
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut s, "SELECT COUNT(*) FROM logs WHERE n=4").unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(9)]]);
}

#[test]
fn trigger_audit_create_validation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(
        &engine,
        &mut s,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut s,
        "CREATE TRIGGER IF NOT EXISTS ai AFTER INSERT ON t FOR EACH ROW DO 1",
    )
    .unwrap();
    exec(
        &engine,
        &mut s,
        "CREATE TRIGGER IF NOT EXISTS ai AFTER INSERT ON t FOR EACH ROW DO 2",
    )
    .unwrap();
    exec(&engine, &mut s, "CREATE DATABASE other").unwrap();
    exec(&engine, &mut s, "CREATE TABLE another(id INT)").unwrap();
    for (sql, code) in [
        ("CREATE TRIGGER IF NOT EXISTS ai AFTER INSERT ON another FOR EACH ROW DO 1", 4100),
        ("CREATE TRIGGER bad BEFORE INSERT ON t FOR EACH ROW BEGIN DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET @x=OLD.id; DO 1; END", 1363),
        ("CREATE TRIGGER bad BEFORE DELETE ON t FOR EACH ROW BEGIN DECLARE c CURSOR FOR SELECT NEW.id; DO 1; END", 1363),
        ("CREATE TRIGGER bad AFTER INSERT ON t FOR EACH ROW /* ignored */ (SELECT 1)", 1415),
        ("CREATE TRIGGER bad AFTER INSERT ON t FOR EACH ROW /* ignored */ COMMIT", 1422),
        ("CREATE TRIGGER other.bad AFTER INSERT ON test.t FOR EACH ROW DO 1", 1435),
        ("CREATE TRIGGER bad AFTER INSERT ON t FOR EACH ROW FOLLOWS missing DO 1", 3011),
        ("CREATE TRIGGER bad AFTER INSERT ON t FOR EACH ROW SET NEW.id=1", 1362),
        ("CREATE TRIGGER bad BEFORE INSERT ON t FOR EACH ROW DO OLD.id", 1363),
        ("CREATE TRIGGER bad BEFORE DELETE ON t FOR EACH ROW DO NEW.id", 1363),
        ("CREATE TRIGGER bad AFTER INSERT ON t FOR EACH ROW (SELECT 1)", 1415),
        ("CREATE TRIGGER bad BEFORE INSERT ON t FOR EACH ROW BEGIN IF 0 THEN SET NEW.id=OLD.id; END IF; END", 1363),
    ] {
        let err = exec(&engine, &mut s, sql).expect_err(sql);
        assert_eq!(err.code(), code, "{sql}: {err:?}");
    }
    // A quoted identifier containing a dot is a column name, not a pseudo-row.
    exec(&engine, &mut s, "CREATE TABLE quoted_source (`OLD.id` INT)").unwrap();
    exec(&engine, &mut s, "INSERT INTO quoted_source VALUES(42)").unwrap();
    exec(&engine, &mut s, "CREATE TRIGGER quoted BEFORE INSERT ON t FOR EACH ROW SET @quoted=(SELECT `OLD.id` FROM quoted_source)").unwrap();
    exec(&engine, &mut s, "INSERT INTO t VALUES(1,1)").unwrap();
    assert_eq!(s.user_variable("quoted"), Some(&Value::Int(42)));
}

#[test]
fn trigger_audit_odku_noop_and_auto_increment() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in [
        "CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v INT)",
        "CREATE TABLE logs (v INT)",
        "CREATE TRIGGER bi BEFORE INSERT ON t FOR EACH ROW INSERT INTO logs VALUES (NEW.id)",
        "CREATE TRIGGER au AFTER UPDATE ON t FOR EACH ROW INSERT INTO logs VALUES (NEW.v)",
        "INSERT INTO t(v) VALUES (10)",
    ] {
        exec(&engine, &mut s, sql).unwrap();
    }
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut s, "SELECT v FROM logs").unwrap() else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(0)]]);
    let r = exec(
        &engine,
        &mut s,
        "INSERT INTO t VALUES (1,10) ON DUPLICATE KEY UPDATE v=10",
    )
    .unwrap();
    assert!(
        matches!(r, QueryResponse::OkAffected { affected_rows: 0 }),
        "{r:?}"
    );
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut s, "SELECT COUNT(*) FROM logs WHERE v=10").unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn trigger_audit_sql_execute_releases_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut a = SessionContext::default();
    a.connection_id = 9001;
    let mut b = SessionContext::default();
    b.connection_id = 9002;
    for sql in [
        "CREATE TABLE prepared_t (id INT PRIMARY KEY)",
        "PREPARE st FROM 'INSERT INTO prepared_t VALUES (?)'",
        "SET @v=1",
        "EXECUTE st USING @v",
    ] {
        exec(&engine, &mut a, sql).unwrap();
    }
    assert!(a.current_txn.is_none());
    exec(&engine, &mut b, "SET lock_wait_timeout=1").unwrap();
    exec(
        &engine,
        &mut b,
        "CREATE TRIGGER prepared_ai AFTER INSERT ON prepared_t FOR EACH ROW DO 1",
    )
    .unwrap();
    exec(&engine, &mut a, "SET @v=2").unwrap();
    exec(&engine, &mut a, "EXECUTE st USING @v").unwrap();
    exec(&engine, &mut b, "DROP TRIGGER prepared_ai").unwrap();
    exec(&engine, &mut a, "BEGIN").unwrap();
    exec(&engine, &mut a, "SET @v=3").unwrap();
    exec(&engine, &mut a, "EXECUTE st USING @v").unwrap();
    exec(&engine, &mut a, "ROLLBACK").unwrap();
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut b, "SELECT COUNT(*) FROM prepared_t").unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(2)]]);
}

#[test]
fn trigger_audit_auto_increment_zero_mode_and_explicit_assignment() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in ["SET sql_mode='NO_AUTO_VALUE_ON_ZERO'",
        "CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v INT)",
        "CREATE TABLE logs (v INT)",
        "CREATE TRIGGER bi BEFORE INSERT ON t FOR EACH ROW BEGIN INSERT INTO logs VALUES (NEW.id); IF NEW.v=1 THEN SET NEW.id=0; END IF; END",
        "INSERT INTO t(v) VALUES (2)", "INSERT INTO t(v) VALUES (1)",
    ] { exec(&engine, &mut s, sql).unwrap(); }
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut s, "SELECT id FROM t ORDER BY id").unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(0)], vec![Value::Int(1)]]);
}

#[test]
fn trigger_audit_set_unknown_variable_and_local_shadowing() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "CREATE TABLE t (id INT)").unwrap();
    exec(
        &engine,
        &mut s,
        "CREATE TRIGGER bi BEFORE INSERT ON t FOR EACH ROW SET marker=1",
    )
    .unwrap();
    assert_eq!(
        exec(&engine, &mut s, "INSERT INTO t VALUES (1)")
            .unwrap_err()
            .code(),
        1193
    );
    exec(&engine, &mut s, "DROP TRIGGER bi").unwrap();
    exec(
        &engine,
        &mut s,
        "CREATE TRIGGER bi BEFORE INSERT ON t FOR EACH ROW SET autocommit=1",
    )
    .unwrap();
    assert_eq!(
        exec(&engine, &mut s, "INSERT INTO t VALUES(1)")
            .unwrap_err()
            .code(),
        1422
    );
    exec(&engine, &mut s, "DROP TRIGGER bi").unwrap();

    exec(&engine, &mut s, "CREATE TRIGGER bi BEFORE INSERT ON t FOR EACH ROW BEGIN DECLARE marker INT; SET marker=7; SET NEW.id=marker; END").unwrap();
    exec(&engine, &mut s, "INSERT INTO t VALUES (1)").unwrap();
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut s, "SELECT id FROM t").unwrap() else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(7)]]);
}

#[test]
fn trigger_audit_odku_normalized_values_and_before_changes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in [
        "CREATE TABLE t(id INT PRIMARY KEY,v INT,g INT GENERATED ALWAYS AS (v*2) STORED)",
        "CREATE TABLE logs(v INT,g INT)",
        "INSERT INTO t(id,v) VALUES(1,10)",
        "CREATE TRIGGER au AFTER UPDATE ON t FOR EACH ROW INSERT INTO logs VALUES(NEW.v,NEW.g)",
    ] {
        exec(&engine, &mut s, sql).unwrap();
    }
    for (sql, expected) in [
        (
            "INSERT INTO t(id,v) VALUES(1,10) ON DUPLICATE KEY UPDATE v='10'",
            0,
        ),
        (
            "INSERT INTO t(id,v) VALUES(1,12) ON DUPLICATE KEY UPDATE v='12'",
            2,
        ),
    ] {
        let result = exec(&engine, &mut s, sql).unwrap();
        assert!(
            matches!(result, QueryResponse::OkAffected { affected_rows } if affected_rows==expected),
            "{result:?}"
        );
    }
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut s, "SELECT v,g FROM logs ORDER BY v").unwrap()
    else {
        panic!()
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(10), Value::Int(20)],
            vec![Value::Int(12), Value::Int(24)]
        ]
    );
    exec(
        &engine,
        &mut s,
        "CREATE TRIGGER bu BEFORE UPDATE ON t FOR EACH ROW SET NEW.v=NEW.v+1",
    )
    .unwrap();
    let result = exec(
        &engine,
        &mut s,
        "INSERT INTO t(id,v) VALUES(1,12) ON DUPLICATE KEY UPDATE v=12",
    )
    .unwrap();
    assert!(matches!(
        result,
        QueryResponse::OkAffected { affected_rows: 2 }
    ));
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut s, "SELECT v,g FROM t").unwrap() else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(13), Value::Int(26)]]);
}

#[test]
fn trigger_audit_partition_error_rolls_back_and_restores_scope() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in [
        "CREATE TABLE a(id INT PRIMARY KEY,v INT) PARTITION BY HASH(id) PARTITIONS 4",
        "CREATE TABLE b(id INT PRIMARY KEY,v INT) PARTITION BY HASH(id) PARTITIONS 4",
        "INSERT INTO a VALUES(1,1),(2,2),(3,3),(4,4)",
        "INSERT INTO b VALUES(11,11),(12,12),(13,13),(14,14)",
        "CREATE TRIGGER au AFTER UPDATE ON a FOR EACH ROW BEGIN DELETE FROM b WHERE id=12; SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='stop'; END",
    ] { exec(&engine, &mut s, sql).unwrap(); }
    assert_eq!(
        exec(&engine, &mut s, "UPDATE a SET v=v+10")
            .unwrap_err()
            .code(),
        1644
    );
    assert!(super::super::partition_router::current_pruned_partitions().is_none());
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut s, "SELECT CAST(SUM(v) AS SIGNED) FROM a").unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(10)]]);
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut s, "SELECT COUNT(*) FROM b").unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(4)]]);
    exec(&engine, &mut s, "DROP TRIGGER au").unwrap();
    exec(
        &engine,
        &mut s,
        "CREATE TRIGGER au AFTER UPDATE ON a FOR EACH ROW DELETE FROM b WHERE id=12",
    )
    .unwrap();
    exec(&engine, &mut s, "UPDATE a SET v=v+10").unwrap();
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut s, "SELECT COUNT(*) FROM b").unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(3)]]);
}

#[test]
fn trigger_audit_expression_subqueries_use_trigger_database() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in [
        "CREATE DATABASE trigger_expression_db",
        "USE trigger_expression_db",
        "CREATE TABLE t(id INT PRIMARY KEY,v INT)",
        "CREATE TABLE source_values(id INT PRIMARY KEY,v INT)",
        "INSERT INTO source_values VALUES(1,42)",
        "CREATE TRIGGER bi BEFORE INSERT ON t FOR EACH ROW BEGIN IF EXISTS(SELECT 1 FROM source_values WHERE id=NEW.id) THEN SET NEW.v=(SELECT v FROM source_values WHERE id=NEW.id); END IF; END",
        "USE test",
        "CREATE TABLE source_values(id INT PRIMARY KEY,v INT)",
        "INSERT INTO source_values VALUES(1,99)",
        "INSERT INTO trigger_expression_db.t VALUES(1,0),(2,7)",
    ] { exec(&engine, &mut s, sql).unwrap(); }
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut s,
        "SELECT id,v FROM trigger_expression_db.t ORDER BY id",
    )
    .unwrap() else {
        panic!()
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(1), Value::Int(42)],
            vec![Value::Int(2), Value::Int(7)]
        ]
    );
    assert_eq!(s.current_database, "test");
}
