use super::support::result_rows as rows;
use super::*;

/// B2: `DROP TABLE parent` referenced by a child is rejected with 3730 while
/// `foreign_key_checks=1`, but allowed once the session sets
/// `foreign_key_checks=0` (MySQL 8.0 leaves the child's FK dangling — the
/// mysqldump / ordered-rebuild idiom). Re-checked for both single-table and
/// multi-table DROP.
#[test]
fn drop_referenced_table_honours_foreign_key_checks() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();

    setup_parent_child(&engine, "", "");
    // With checks ON, dropping the referenced parent is rejected.
    let err = engine
        .execute_with_session(&mut s, "DROP TABLE parent")
        .unwrap_err();
    assert_eq!(err.code(), 3730, "{}", err.message());

    // With checks OFF, the same DROP succeeds; the child survives with a now
    // dangling FK.
    engine
        .execute_with_session(&mut s, "SET foreign_key_checks = 0")
        .unwrap();
    engine
        .execute_with_session(&mut s, "DROP TABLE parent")
        .unwrap();
    // Parent is gone; child remains.
    assert!(engine
        .execute_with_session(&mut s, "SELECT * FROM parent")
        .is_err());
    engine
        .execute_with_session(&mut s, "SELECT * FROM child")
        .unwrap();

    // Multi-table DROP path (drop_tables) honours it too: recreate a referenced
    // parent, then drop it alongside an unrelated table in one statement.
    engine
        .execute_with_session(&mut s, "SET foreign_key_checks = 1")
        .unwrap();
    engine
        .execute_with_session(&mut s, "CREATE TABLE parent (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut s, "CREATE TABLE spare (id INT PRIMARY KEY)")
        .unwrap();
    let err = engine
        .execute_with_session(&mut s, "DROP TABLE parent, spare")
        .unwrap_err();
    assert_eq!(err.code(), 3730, "{}", err.message());
    engine
        .execute_with_session(&mut s, "SET foreign_key_checks = 0")
        .unwrap();
    engine
        .execute_with_session(&mut s, "DROP TABLE parent, spare")
        .unwrap();
    assert!(engine
        .execute_with_session(&mut s, "SELECT * FROM spare")
        .is_err());
}

/// Standard parent/child pair: `parent(id PK)` + `child(id PK, pid → parent.id)`.
fn setup_parent_child(engine: &StorageQueryEngine, on_delete: &str, on_update: &str) {
    engine
        .execute("CREATE TABLE parent (id INT PRIMARY KEY, label VARCHAR(32))")
        .unwrap();
    engine
        .execute(&format!(
            "CREATE TABLE child (\
                 id INT PRIMARY KEY, \
                 pid INT, \
                 CONSTRAINT fk_child_parent FOREIGN KEY (pid) REFERENCES parent (id) {on_delete} {on_update})"
        ))
        .unwrap();
    engine
        .execute("INSERT INTO parent VALUES (1, 'a'), (2, 'b')")
        .unwrap();
}

#[test]
fn fk_insert_requires_existing_parent() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_parent_child(&engine, "", "");

    // Matching parent → ok.
    engine.execute("INSERT INTO child VALUES (10, 1)").unwrap();
    // NULL FK column → MATCH SIMPLE, skip the check.
    engine
        .execute("INSERT INTO child VALUES (11, NULL)")
        .unwrap();
    // Missing parent → 1452.
    let err = engine
        .execute("INSERT INTO child VALUES (12, 99)")
        .unwrap_err();
    assert_eq!(err.code(), 1452, "{}", err.message());
}

#[test]
fn fk_update_child_requires_existing_parent() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_parent_child(&engine, "", "");
    engine.execute("INSERT INTO child VALUES (10, 1)").unwrap();

    // Re-point to an existing parent → ok.
    engine
        .execute("UPDATE child SET pid = 2 WHERE id = 10")
        .unwrap();
    // Re-point to a missing parent → 1452.
    let err = engine
        .execute("UPDATE child SET pid = 99 WHERE id = 10")
        .unwrap_err();
    assert_eq!(err.code(), 1452, "{}", err.message());
    // Set NULL → allowed.
    engine
        .execute("UPDATE child SET pid = NULL WHERE id = 10")
        .unwrap();
}

#[test]
fn fk_same_transaction_parent_then_child_visible() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE parent (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("CREATE TABLE child (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES parent (id))")
        .unwrap();

    let mut s = SessionContext::default();
    engine.execute_with_session(&mut s, "BEGIN").unwrap();
    // Parent inserted but not yet committed; child must still see it.
    engine
        .execute_with_session(&mut s, "INSERT INTO parent VALUES (5)")
        .unwrap();
    engine
        .execute_with_session(&mut s, "INSERT INTO child VALUES (1, 5)")
        .unwrap();
    engine.execute_with_session(&mut s, "COMMIT").unwrap();

    let n = rows(engine.execute("SELECT COUNT(*) FROM child").unwrap());
    assert_eq!(n[0][0], Value::Int(1));
}

#[test]
fn fk_delete_parent_restrict() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // No ON DELETE clause → NO ACTION (== RESTRICT, immediate).
    setup_parent_child(&engine, "", "");
    engine.execute("INSERT INTO child VALUES (10, 1)").unwrap();

    let err = engine
        .execute("DELETE FROM parent WHERE id = 1")
        .unwrap_err();
    assert_eq!(err.code(), 1451, "{}", err.message());

    // Deleting an unreferenced parent is fine.
    engine.execute("DELETE FROM parent WHERE id = 2").unwrap();
    // Updating the referenced key is also blocked.
    let err = engine
        .execute("UPDATE parent SET id = 3 WHERE id = 1")
        .unwrap_err();
    assert_eq!(err.code(), 1451, "{}", err.message());
}

#[test]
fn fk_cascade_delete() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_parent_child(&engine, "ON DELETE CASCADE", "");
    engine
        .execute("INSERT INTO child VALUES (10, 1), (11, 1), (12, 2)")
        .unwrap();

    engine.execute("DELETE FROM parent WHERE id = 1").unwrap();
    let remaining = rows(engine.execute("SELECT id FROM child ORDER BY id").unwrap());
    assert_eq!(remaining, vec![vec![Value::Int(12)]]);
}

#[test]
fn fk_cascade_update() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_parent_child(&engine, "ON DELETE CASCADE", "ON UPDATE CASCADE");
    engine
        .execute("INSERT INTO child VALUES (10, 1), (11, 1)")
        .unwrap();

    engine
        .execute("UPDATE parent SET id = 7 WHERE id = 1")
        .unwrap();
    let pids = rows(engine.execute("SELECT DISTINCT pid FROM child").unwrap());
    assert_eq!(pids, vec![vec![Value::Int(7)]]);
}

#[test]
fn fk_set_null_on_delete() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_parent_child(&engine, "ON DELETE SET NULL", "");
    engine
        .execute("INSERT INTO child VALUES (10, 1), (11, 2)")
        .unwrap();

    engine.execute("DELETE FROM parent WHERE id = 1").unwrap();
    let r = rows(
        engine
            .execute("SELECT id, pid FROM child ORDER BY id")
            .unwrap(),
    );
    assert_eq!(
        r,
        vec![
            vec![Value::Int(10), Value::Null],
            vec![Value::Int(11), Value::Int(2)],
        ]
    );
}

#[test]
fn fk_self_reference_cascade_chain() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE node (\
                 id INT PRIMARY KEY, \
                 parent INT, \
                 FOREIGN KEY (parent) REFERENCES node (id) ON DELETE CASCADE)",
        )
        .unwrap();
    // Linear chain 1 <- 2 <- 3.
    engine
        .execute("INSERT INTO node VALUES (1, NULL), (2, 1), (3, 2)")
        .unwrap();
    engine.execute("DELETE FROM node WHERE id = 1").unwrap();
    let n = rows(engine.execute("SELECT COUNT(*) FROM node").unwrap());
    assert_eq!(n[0][0], Value::Int(0));
}

#[test]
fn fk_cascade_depth_limit() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    std::env::set_var("MYDB_FK_MAX_DEPTH", "8");
    engine
        .execute(
            "CREATE TABLE chain (\
                 id INT PRIMARY KEY, \
                 parent INT, \
                 FOREIGN KEY (parent) REFERENCES chain (id) ON DELETE CASCADE)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO chain VALUES (1, NULL)")
        .unwrap();
    for i in 2..=20 {
        engine
            .execute(&format!("INSERT INTO chain VALUES ({i}, {})", i - 1))
            .unwrap();
    }
    let err = engine
        .execute("DELETE FROM chain WHERE id = 1")
        .unwrap_err();
    assert_eq!(err.code(), 3008, "{}", err.message());
    std::env::remove_var("MYDB_FK_MAX_DEPTH");
    // Whole statement rolled back — nothing deleted.
    let n = rows(engine.execute("SELECT COUNT(*) FROM chain").unwrap());
    assert_eq!(n[0][0], Value::Int(20));
}

#[test]
fn fk_multirow_insert_rolls_back_on_violation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_parent_child(&engine, "", "");

    // Third row has no parent; the whole INSERT must fail atomically.
    let err = engine
        .execute("INSERT INTO child VALUES (10, 1), (11, 2), (12, 99)")
        .unwrap_err();
    assert_eq!(err.code(), 1452, "{}", err.message());
    let n = rows(engine.execute("SELECT COUNT(*) FROM child").unwrap());
    assert_eq!(n[0][0], Value::Int(0));
}

#[test]
fn fk_checks_off_skips_and_does_not_retroactively_validate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_parent_child(&engine, "", "");

    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "SET foreign_key_checks = 0")
        .unwrap();
    // Orphan insert allowed while checks are off.
    engine
        .execute_with_session(&mut s, "INSERT INTO child VALUES (10, 99)")
        .unwrap();
    // Re-enabling does not retro-validate the existing orphan...
    engine
        .execute_with_session(&mut s, "SET foreign_key_checks = 1")
        .unwrap();
    let n = rows(
        engine
            .execute_with_session(&mut s, "SELECT COUNT(*) FROM child")
            .unwrap(),
    );
    assert_eq!(n[0][0], Value::Int(1));
    // ...but new violations are caught again.
    let err = engine
        .execute_with_session(&mut s, "INSERT INTO child VALUES (11, 98)")
        .unwrap_err();
    assert_eq!(err.code(), 1452, "{}", err.message());

    // @@foreign_key_checks reflects the session value.
    let v = rows(
        engine
            .execute_with_session(&mut s, "SELECT @@foreign_key_checks")
            .unwrap(),
    );
    assert_eq!(v[0][0].to_display_string(), "1");
}

#[test]
fn fk_checks_off_defers_missing_parent_then_enforces_once_created() {
    // The mysqldump import shape: `SET FOREIGN_KEY_CHECKS=0`, then create a
    // child that references a parent emitted later in the dump. With checks on
    // this is 1824; with checks off MySQL records the constraint verbatim,
    // creates the child backing index, and enforces it once the parent exists.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();

    engine
        .execute_with_session(&mut s, "SET foreign_key_checks = 0")
        .unwrap();
    // Child created BEFORE its parent — accepted only because checks are off.
    engine
        .execute_with_session(
            &mut s,
            "CREATE TABLE child (id INT PRIMARY KEY, pid INT, \
             CONSTRAINT fk FOREIGN KEY (pid) REFERENCES parent (id))",
        )
        .unwrap();
    // The child's backing index is still created (as MySQL does).
    let idx = rows(
        engine
            .execute_with_session(&mut s, "SHOW INDEX FROM child")
            .unwrap(),
    );
    assert!(
        idx.iter().any(|r| r[2].to_display_string() == "fk"),
        "child backing index `fk` should exist: {idx:?}"
    );
    // Data loaded while the parent is still absent.
    engine
        .execute_with_session(&mut s, "INSERT INTO child VALUES (1, 99)")
        .unwrap();
    // Parent arrives; the reverse edge links up on the next catalog rebuild.
    engine
        .execute_with_session(&mut s, "CREATE TABLE parent (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut s, "INSERT INTO parent VALUES (1)")
        .unwrap();

    // Re-enable checks: the pre-existing orphan is not re-validated, but the
    // now-resolved constraint is enforced for new DML.
    engine
        .execute_with_session(&mut s, "SET foreign_key_checks = 1")
        .unwrap();
    let err = engine
        .execute_with_session(&mut s, "INSERT INTO child VALUES (2, 77)")
        .unwrap_err();
    assert_eq!(err.code(), 1452, "{}", err.message());
    engine
        .execute_with_session(&mut s, "INSERT INTO child VALUES (3, 1)")
        .unwrap();
}

#[test]
fn fk_checks_on_still_rejects_missing_parent_1824() {
    // With FK checks at their default (on), a forward reference to a
    // nonexistent table remains a hard 1824 — deferral is gated on checks off.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let err = engine
        .execute(
            "CREATE TABLE child (id INT PRIMARY KEY, pid INT, \
             CONSTRAINT fk FOREIGN KEY (pid) REFERENCES parent (id))",
        )
        .unwrap_err();
    assert_eq!(err.code(), 1824, "{}", err.message());
}

#[test]
fn fk_ddl_guards() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_parent_child(&engine, "", "");
    engine.execute("INSERT INTO child VALUES (10, 1)").unwrap();

    // DROP TABLE on a referenced parent → 3730.
    let err = engine.execute("DROP TABLE parent").unwrap_err();
    assert_eq!(err.code(), 3730, "{}", err.message());
    // TRUNCATE on a referenced parent → 1701.
    let err = engine.execute("TRUNCATE TABLE parent").unwrap_err();
    assert_eq!(err.code(), 1701, "{}", err.message());
    // DROP the child FK column → 1828.
    let err = engine
        .execute("ALTER TABLE child DROP COLUMN pid")
        .unwrap_err();
    assert_eq!(err.code(), 1828, "{}", err.message());
    // DROP the backing index → 1553.
    let err = engine
        .execute("ALTER TABLE child DROP INDEX fk_child_parent")
        .unwrap_err();
    assert_eq!(err.code(), 1553, "{}", err.message());

    // Dropping the child first releases the parent.
    engine.execute("DROP TABLE child").unwrap();
    engine.execute("DROP TABLE parent").unwrap();
}

#[test]
fn drop_database_ignores_intra_database_fk() {
    // Regression: DROP DATABASE must drop the whole schema even when its
    // tables reference each other via foreign keys. Previously the
    // per-table drop loop rejected a parent still referenced by a sibling
    // that had not been dropped yet (ERROR 3730).
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE DATABASE shopping").unwrap();
    engine
        .execute("CREATE TABLE shopping.autoparts (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE shopping.comment (\
                 id INT PRIMARY KEY, \
                 part_id INT, \
                 CONSTRAINT fk_comment_autoparts FOREIGN KEY (part_id) \
                     REFERENCES shopping.autoparts (id))",
        )
        .unwrap();

    // The whole schema drops cleanly regardless of drop order.
    engine.execute("DROP DATABASE shopping").unwrap();

    // Gone: recreating and re-dropping still works.
    engine.execute("CREATE DATABASE shopping").unwrap();
    engine.execute("DROP DATABASE shopping").unwrap();
}

#[test]
fn fk_create_rejections() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE parent (id INT PRIMARY KEY, name VARCHAR(32))")
        .unwrap();

    // SET DEFAULT is parsed but rejected → 1215.
    let err = engine
        .execute("CREATE TABLE c1 (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES parent (id) ON DELETE SET DEFAULT)")
        .unwrap_err();
    assert_eq!(err.code(), 1215, "{}", err.message());

    // Type mismatch (VARCHAR child → INT parent) → 3780.
    let err = engine
        .execute("CREATE TABLE c2 (id INT PRIMARY KEY, pid VARCHAR(10), FOREIGN KEY (pid) REFERENCES parent (id))")
        .unwrap_err();
    assert_eq!(err.code(), 3780, "{}", err.message());

    // Referencing a non-key column → 1822.
    let err = engine
        .execute("CREATE TABLE c3 (id INT PRIMARY KEY, pn VARCHAR(32), FOREIGN KEY (pn) REFERENCES parent (name))")
        .unwrap_err();
    assert_eq!(err.code(), 1822, "{}", err.message());

    // Partitioned child with an FK → 1506.
    let err = engine
        .execute(
            "CREATE TABLE c4 (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES parent (id)) \
             PARTITION BY HASH(id) PARTITIONS 2",
        )
        .unwrap_err();
    assert_eq!(err.code(), 1506, "{}", err.message());
}

#[test]
fn fk_alter_add_and_drop() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE parent (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("CREATE TABLE child (id INT PRIMARY KEY, pid INT)")
        .unwrap();
    engine.execute("INSERT INTO parent VALUES (1)").unwrap();

    // Existing orphan blocks ADD FOREIGN KEY with 1452.
    engine.execute("INSERT INTO child VALUES (10, 99)").unwrap();
    let err = engine
        .execute("ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (pid) REFERENCES parent (id)")
        .unwrap_err();
    assert_eq!(err.code(), 1452, "{}", err.message());
    let QueryResponse::ResultSet(indexes) = engine.execute("SHOW INDEX FROM child").unwrap() else {
        panic!("expected index rows");
    };
    assert!(
        !indexes
            .rows
            .iter()
            .any(|row| row[2] == Value::String("fk1".into())),
        "failed ADD FOREIGN KEY must remove its automatic backing index"
    );

    // Fix the orphan, then ADD succeeds and is enforced.
    engine
        .execute("UPDATE child SET pid = 1 WHERE id = 10")
        .unwrap();
    engine
        .execute("ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (pid) REFERENCES parent (id)")
        .unwrap();
    let err = engine
        .execute("INSERT INTO child VALUES (11, 77)")
        .unwrap_err();
    assert_eq!(err.code(), 1452, "{}", err.message());

    // DROP FOREIGN KEY lifts enforcement.
    engine
        .execute("ALTER TABLE child DROP FOREIGN KEY fk1")
        .unwrap();
    engine.execute("INSERT INTO child VALUES (12, 77)").unwrap();
}

#[test]
fn fk_show_create_and_information_schema() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_parent_child(&engine, "ON DELETE CASCADE", "ON UPDATE SET NULL");

    let create = rows(engine.execute("SHOW CREATE TABLE child").unwrap())[0][1].to_display_string();
    assert!(
        create.contains(
            "CONSTRAINT `fk_child_parent` FOREIGN KEY (`pid`) REFERENCES `parent` (`id`)"
        ),
        "{create}"
    );
    assert!(create.contains("ON DELETE CASCADE"), "{create}");
    assert!(create.contains("ON UPDATE SET NULL"), "{create}");

    // referential_constraints
    let rc = rows(
        engine
            .execute(
                "SELECT constraint_name, update_rule, delete_rule, table_name, referenced_table_name \
                 FROM information_schema.referential_constraints \
                 WHERE constraint_schema = 'test'",
            )
            .unwrap(),
    );
    assert_eq!(
        rc,
        vec![vec![
            Value::String("fk_child_parent".into()),
            Value::String("SET NULL".into()),
            Value::String("CASCADE".into()),
            Value::String("child".into()),
            Value::String("parent".into()),
        ]]
    );

    // table_constraints lists a FOREIGN KEY row.
    let tc = rows(
        engine
            .execute(
                "SELECT constraint_type FROM information_schema.table_constraints \
                 WHERE table_schema='test' AND table_name='child' AND constraint_name='fk_child_parent'",
            )
            .unwrap(),
    );
    assert_eq!(tc, vec![vec![Value::String("FOREIGN KEY".into())]]);

    // key_column_usage carries the referenced columns.
    let kcu = rows(
        engine
            .execute(
                "SELECT column_name, referenced_table_name, referenced_column_name \
                 FROM information_schema.key_column_usage \
                 WHERE table_schema='test' AND table_name='child' AND constraint_name='fk_child_parent'",
            )
            .unwrap(),
    );
    assert_eq!(
        kcu,
        vec![vec![
            Value::String("pid".into()),
            Value::String("parent".into()),
            Value::String("id".into()),
        ]]
    );
}

/// JDBC introspection tools (NineData, DBeaver, …) probe primary-key
/// columns with `SELECT GROUP_CONCAT(column_name ORDER BY ordinal_position)
/// FROM information_schema.key_column_usage WHERE constraint_name='PRIMARY'`.
/// The dedicated `execute_system_select` evaluator doesn't have an
/// aggregate pipeline, so without explicit routing the aggregate falls
/// through to the legacy expression evaluator and raises
/// 1111 "invalid use of group function 'group_concat'".
#[test]
fn information_schema_aggregate_supports_group_concat() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE region (r_regionkey INT PRIMARY KEY, r_name VARCHAR(32))")
        .unwrap();

    let pk = rows(
        engine
            .execute(
                "SELECT GROUP_CONCAT(c.column_name ORDER BY ordinal_position) AS key_columns \
                 FROM information_schema.key_column_usage c \
                 WHERE c.table_schema='test' AND c.table_name='region' \
                   AND constraint_name='PRIMARY'",
            )
            .unwrap(),
    );
    assert_eq!(pk, vec![vec![Value::String("r_regionkey".into())]]);

    // COUNT(*) on a filtered information_schema view exercises the same
    // routing path (implicit aggregation without GROUP BY).
    let cnt = rows(
        engine
            .execute(
                "SELECT COUNT(*) FROM information_schema.columns \
                 WHERE table_schema='test' AND table_name='region'",
            )
            .unwrap(),
    );
    assert_eq!(cnt, vec![vec![Value::Int(2)]]);
}

/// The ORDER BY clause inside GROUP_CONCAT (parsed via the
/// `__gc_ordered` wrapper) must order PK columns by their declared
/// position even when the catalog row order is different. JDBC
/// metadata drivers rely on this for composite-PK descriptions.
#[test]
fn information_schema_group_concat_orders_composite_primary_key() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE order_line (\
                 ol_w_id INT, \
                 ol_d_id INT, \
                 ol_o_id INT, \
                 ol_number INT, \
                 ol_qty INT, \
                 PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))",
        )
        .unwrap();

    let pk = rows(
        engine
            .execute(
                "SELECT GROUP_CONCAT(c.column_name ORDER BY c.ordinal_position) AS key_columns \
                 FROM information_schema.key_column_usage c \
                 WHERE c.table_schema='test' \
                   AND c.table_name='order_line' \
                   AND c.constraint_name='PRIMARY'",
            )
            .unwrap(),
    );
    assert_eq!(
        pk,
        vec![vec![Value::String(
            "ol_w_id,ol_d_id,ol_o_id,ol_number".into()
        )]]
    );

    // DESC reverses the order — confirms direction is honoured.
    let pk_desc = rows(
        engine
            .execute(
                "SELECT GROUP_CONCAT(c.column_name ORDER BY c.ordinal_position DESC SEPARATOR ' | ') \
                 FROM information_schema.key_column_usage c \
                 WHERE c.table_schema='test' \
                   AND c.table_name='order_line' \
                   AND c.constraint_name='PRIMARY'",
            )
            .unwrap(),
    );
    assert_eq!(
        pk_desc,
        vec![vec![Value::String(
            "ol_number | ol_o_id | ol_d_id | ol_w_id".into()
        )]]
    );
}

#[test]
fn fk_auto_backing_index_created() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE parent (id INT PRIMARY KEY)")
        .unwrap();
    // No index on pid → one is created automatically for the FK.
    engine
        .execute("CREATE TABLE child (id INT PRIMARY KEY, pid INT, CONSTRAINT fk FOREIGN KEY (pid) REFERENCES parent (id))")
        .unwrap();
    let idx = rows(
        engine
            .execute("SELECT DISTINCT index_name FROM information_schema.statistics WHERE table_name='child' AND index_name='fk'")
            .unwrap(),
    );
    assert_eq!(idx, vec![vec![Value::String("fk".into())]]);
}

#[test]
fn fk_persists_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE parent (id INT PRIMARY KEY)")
            .unwrap();
        engine
            .execute("CREATE TABLE child (id INT PRIMARY KEY, pid INT, CONSTRAINT fk FOREIGN KEY (pid) REFERENCES parent (id) ON DELETE CASCADE)")
            .unwrap();
        engine.execute("INSERT INTO parent VALUES (1)").unwrap();
        engine.execute("INSERT INTO child VALUES (10, 1)").unwrap();
    }
    // Reopen: FK definition + runtime caches must rebuild from the DD.
    let engine = open_strict_test_engine(dir.path());
    // Child-side enforcement survives restart.
    let err = engine
        .execute("INSERT INTO child VALUES (11, 99)")
        .unwrap_err();
    assert_eq!(err.code(), 1452, "{}", err.message());
    // Parent-side cascade survives restart.
    engine.execute("DELETE FROM parent WHERE id = 1").unwrap();
    let n = rows(engine.execute("SELECT COUNT(*) FROM child").unwrap());
    assert_eq!(n[0][0], Value::Int(0));
    // SHOW CREATE TABLE still renders the FK.
    let create = rows(engine.execute("SHOW CREATE TABLE child").unwrap())[0][1].to_display_string();
    assert!(
        create.contains("FOREIGN KEY (`pid`) REFERENCES `parent`"),
        "{create}"
    );
}

#[test]
fn fk_concurrent_child_insert_blocks_parent_delete() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE parent (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("CREATE TABLE child (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES parent (id))")
        .unwrap();
    engine.execute("INSERT INTO parent VALUES (1)").unwrap();

    let (tx, rx) = std::sync::mpsc::channel();
    let e1 = engine.clone();
    let inserter = std::thread::spawn(move || {
        let mut s = SessionContext::default();
        e1.execute_with_session(&mut s, "BEGIN").unwrap();
        e1.execute_with_session(&mut s, "INSERT INTO child VALUES (10, 1)")
            .unwrap();
        // Child row now holds a shared lock on parent(1); signal the deleter.
        tx.send(()).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(300));
        e1.execute_with_session(&mut s, "COMMIT").unwrap();
    });

    let e2 = engine.clone();
    let deleter = std::thread::spawn(move || {
        rx.recv().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(50));
        let mut s = SessionContext::default();
        e2.execute_with_session(&mut s, "DELETE FROM parent WHERE id = 1")
    });

    inserter.join().unwrap();
    let result = deleter.join().unwrap();
    // The parent delete must not orphan the child: it is blocked until the
    // child commits, then fails RESTRICT (1451) — or times out (1205).
    let err = result.expect_err("parent delete should not succeed");
    assert!(
        err.is(crate::error::catalog::ER_ROW_IS_REFERENCED_2)
            || err.is(crate::error::catalog::ER_LOCK_WAIT_TIMEOUT),
        "unexpected: {err:?}"
    );

    // The parent row is still present (delete did not go through).
    let n = rows(
        engine
            .execute("SELECT COUNT(*) FROM parent WHERE id = 1")
            .unwrap(),
    );
    assert_eq!(n[0][0], Value::Int(1));
}

/// Regression for the TPC-C PAYMENT deadlock (BenchmarkSQL, 1213 at the
/// warehouse/district update). Two transactions each do a non-key UPDATE of
/// their own parent row (like `UPDATE district SET d_ytd = …`) and then insert
/// a child that FK-references the *other* transaction's parent row (like the
/// remote-customer `INSERT history`). With the legacy parent-data-row shared
/// lock, the child FK check (S) falsely conflicts with the other txn's non-key
/// UPDATE (X), forming an X↔S cycle → one txn aborts with 1213. The
/// fine-grained reference lock keys FK coordination on the referenced value in
/// a separate namespace, so a non-key parent UPDATE no longer blocks a child
/// FK check — both transactions commit. Orphan prevention is unchanged
/// (`fk_concurrent_child_insert_blocks_parent_delete` covers it).
#[test]
fn fk_reference_lock_avoids_payment_style_deadlock() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 2_000));
    engine
        .execute("CREATE TABLE district (d_id INT PRIMARY KEY, d_ytd INT)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE history (h_id INT PRIMARY KEY, h_d_id INT, \
             FOREIGN KEY (h_d_id) REFERENCES district (d_id))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO district VALUES (1, 0), (2, 0)")
        .unwrap();

    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let run = |engine: std::sync::Arc<StorageQueryEngine>,
               barrier: std::sync::Arc<std::sync::Barrier>,
               home: i32,
               remote: i32,
               hid: i32| {
        std::thread::spawn(move || {
            let mut s = SessionContext::default();
            engine.execute_with_session(&mut s, "BEGIN")?;
            // Non-key UPDATE of our own district row — X-locks district(home).
            engine.execute_with_session(
                &mut s,
                &format!("UPDATE district SET d_ytd = d_ytd + 1 WHERE d_id = {home}"),
            )?;
            barrier.wait();
            // Child insert referencing the *other* district — needs the FK
            // coordination lock on district(remote), which the other thread is
            // X-updating. Legacy row-lock => deadlock; ref-lock => fine.
            engine.execute_with_session(
                &mut s,
                &format!("INSERT INTO history VALUES ({hid}, {remote})"),
            )?;
            engine.execute_with_session(&mut s, "COMMIT")
        })
    };

    let t1 = run(engine.clone(), barrier.clone(), 1, 2, 100);
    let t2 = run(engine.clone(), barrier.clone(), 2, 1, 200);
    let r1 = t1.join().unwrap();
    let r2 = t2.join().unwrap();

    // Both must commit — no deadlock (1213) and no lock-wait timeout (1205).
    assert!(r1.is_ok(), "t1 failed: {r1:?}");
    assert!(r2.is_ok(), "t2 failed: {r2:?}");
    let n = rows(engine.execute("SELECT COUNT(*) FROM history").unwrap());
    assert_eq!(n[0][0], Value::Int(2));
}

/// Regression for the TPC-C `o_customer_fkey` failure observed on
/// the `feat/lazy-index-encoding` branch. With `UPDATE_DELTA`
/// enabled (default) and the prepared-DML fast path covering
/// explicit transactions, a concurrent Payment-style
/// `UPDATE customer SET <non-PK column> = …` writes an
/// `UndoKind::UpdateDelta` undo whose `before_values` carries only
/// the changed columns — sparse data keyed by `before_cols`. The
/// FK enforcement walker (`read_row_committed_or_own`) used to
/// replace the working row outright with that sparse vector, then
/// ask `table.index_key(pk_index, &sparse_row)` for the PK key.
/// The PK columns sat at positions outside the sparse vector so
/// the computed key compared unequal to the FK probe key and the
/// child INSERT was rejected with "Cannot add or update a child
/// row — FK fails", even though the parent row plainly exists.
///
/// Pins the fix: when the FK walker traverses an `UndoKind::UpdateDelta`
/// record, it must apply the sparse before-image column-by-column
/// onto the current row instead of wholesale-replacing it.
///
/// The parent table here uses a fixed-width non-PK column
/// (`balance INT`) so the UPDATE drives `try_in_place_update_delta`
/// and produces an `UndoKind::UpdateDelta` rather than the legacy
/// full-row `UndoKind::Update`. The TPC-C Payment shape modifies
/// fixed-width customer columns (`c_balance`, `c_ytd_payment`,
/// `c_payment_cnt`), matching this pattern.
#[test]
fn fk_child_insert_sees_parent_through_uncommitted_update_delta() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE parent (id INT PRIMARY KEY, balance INT)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE child (id INT PRIMARY KEY, pid INT, \
             CONSTRAINT fk_child_parent FOREIGN KEY (pid) REFERENCES parent (id))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO parent VALUES (1, 100)")
        .unwrap();
    engine
        .execute("INSERT INTO parent VALUES (2, 200)")
        .unwrap();
    // Bump the parent row once with a committed UpdateDelta to make
    // sure the row's MVCC header carries both `FLAG_HAS_LAST_TXN_ID`
    // and `FLAG_HAS_UNDO_PTR` (the two bits `try_write_mvcc_patched`
    // pre-checks). A fresh INSERT can leave the undo-ptr slot
    // unflagged on the heap row after autocommit purge runs in
    // tests, which would otherwise force the next UPDATE down the
    // legacy full-row path and bypass the bug under test.
    engine
        .execute("UPDATE parent SET balance = 101 WHERE id = 1")
        .unwrap();

    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let updater_engine = std::sync::Arc::clone(&engine);
    let updater = std::thread::spawn(move || {
        let mut s = SessionContext::default();
        updater_engine
            .execute_with_session(&mut s, "BEGIN")
            .unwrap();
        // Fixed-width non-PK update — the heap takes the
        // `try_in_place_update_delta` path, which writes the row
        // patch + a sparse `UndoKind::UpdateDelta` undo record.
        // The before-image stored in the undo is just the
        // pre-image of `balance` (column index 1), not the full
        // row, which is exactly the input that broke the FK
        // walker.
        updater_engine
            .execute_with_session(&mut s, "UPDATE parent SET balance = 999 WHERE id = 1")
            .unwrap();
        started_tx.send(()).unwrap();
        // Hold the X lock briefly so the inserter's first
        // `fk_row_key_matches` probe (run *before* the shared lock
        // is acquired) sees the active-writer state and walks the
        // UpdateDelta. Short enough to stay under the default
        // lock-wait timeout, long enough to force the wait.
        std::thread::sleep(std::time::Duration::from_millis(80));
        updater_engine
            .execute_with_session(&mut s, "COMMIT")
            .unwrap();
    });

    started_rx.recv().unwrap();
    // Confirm the updater actually drove the in-place patch path
    // (and therefore an `UndoKind::UpdateDelta` record). If this
    // ever regresses to the full-row legacy path the test would
    // pass for the wrong reason — the walker's UpdateDelta branch
    // would no longer be exercised at all.
    let delta_hits_before = crate::storage::update_delta_hits();
    let delta_misses_before = crate::storage::update_delta_misses();
    assert!(
        delta_hits_before >= 1,
        "expected updater to have produced an UndoKind::UpdateDelta record \
         (update_delta_hits={delta_hits_before}, misses={delta_misses_before})"
    );
    // INSERT child(pid=1) must succeed. With the buggy walker the
    // first fk_row_key_matches check (taken before the shared lock
    // is acquired) returns `false` because the working row is the
    // sparse delta — the loop then exhausts every candidate rid
    // without ever finding a match, falls into `satisfied=false`,
    // and emits the 1452 child-row-FK error. With the fix it
    // returns true, acquires the shared lock (briefly waiting on
    // the updater's X lock), confirms the match, and commits.
    engine
        .execute("INSERT INTO child VALUES (100, 1)")
        .expect("FK probe must succeed through the UpdateDelta walker");

    updater.join().unwrap();

    let n = rows(
        engine
            .execute("SELECT COUNT(*) FROM child WHERE pid = 1")
            .unwrap(),
    );
    assert_eq!(n[0][0], Value::Int(1));
}

// ===========================================================================
// Regression tests for the 2026-07 foreign-key audit fixes
// (docs/foreign-key-audit-2026-07.md).
// ===========================================================================

/// B4 — `SET NULL` referencing a NOT NULL child column is rejected at DDL
/// time with 1830, instead of deferring to a runtime 1048.
#[test]
fn fk_set_null_on_not_null_column_rejected_1830() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE parent (id INT PRIMARY KEY)")
        .unwrap();
    // pid is NOT NULL, so ON DELETE SET NULL is illegal.
    let err = engine
        .execute(
            "CREATE TABLE child (id INT PRIMARY KEY, pid INT NOT NULL, \
             FOREIGN KEY (pid) REFERENCES parent (id) ON DELETE SET NULL)",
        )
        .expect_err("SET NULL on NOT NULL column must be rejected");
    assert_eq!(err.code(), 1830, "{}", err.message());

    // ON UPDATE SET NULL is likewise rejected.
    let err = engine
        .execute(
            "CREATE TABLE child2 (id INT PRIMARY KEY, pid INT NOT NULL, \
             FOREIGN KEY (pid) REFERENCES parent (id) ON UPDATE SET NULL)",
        )
        .expect_err("SET NULL on NOT NULL column must be rejected");
    assert_eq!(err.code(), 1830, "{}", err.message());

    // Nullable column is fine.
    engine
        .execute(
            "CREATE TABLE child3 (id INT PRIMARY KEY, pid INT, \
             FOREIGN KEY (pid) REFERENCES parent (id) ON DELETE SET NULL)",
        )
        .unwrap();
}

/// B7 — the referenced columns must form a *complete* UNIQUE/PRIMARY key;
/// referencing a non-unique prefix of a composite key is rejected (1822).
#[test]
fn fk_referencing_nonunique_prefix_of_composite_key_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE parent (a INT, b INT, PRIMARY KEY (a, b))")
        .unwrap();
    // (a) alone is not a key of parent → 1822.
    let err = engine
        .execute(
            "CREATE TABLE child (id INT PRIMARY KEY, pa INT, \
             FOREIGN KEY (pa) REFERENCES parent (a))",
        )
        .expect_err("prefix of composite key is not a valid FK target");
    assert_eq!(err.code(), 1822, "{}", err.message());

    // Referencing the full composite key succeeds.
    engine
        .execute(
            "CREATE TABLE child_ok (id INT PRIMARY KEY, pa INT, pb INT, \
             FOREIGN KEY (pa, pb) REFERENCES parent (a, b))",
        )
        .unwrap();
}

/// Composite (multi-column) FK end-to-end: existence check, MATCH SIMPLE
/// partial-NULL skip, and ON DELETE CASCADE across a two-column key.
#[test]
fn fk_composite_key_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE parent (a INT, b INT, PRIMARY KEY (a, b))")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE child (id INT PRIMARY KEY, pa INT, pb INT, \
             FOREIGN KEY (pa, pb) REFERENCES parent (a, b) ON DELETE CASCADE)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO parent VALUES (1, 10), (2, 20)")
        .unwrap();

    // Both key columns match a parent row → ok.
    engine
        .execute("INSERT INTO child VALUES (100, 1, 10)")
        .unwrap();
    // Partial NULL → MATCH SIMPLE, constraint satisfied even though (1, 99)
    // has no parent.
    engine
        .execute("INSERT INTO child VALUES (101, 1, NULL)")
        .unwrap();
    // Non-NULL pair with no matching parent → 1452.
    let err = engine
        .execute("INSERT INTO child VALUES (102, 9, 99)")
        .expect_err("orphan composite child must be rejected");
    assert_eq!(err.code(), 1452, "{}", err.message());

    // ON DELETE CASCADE removes the matching composite child.
    engine
        .execute("DELETE FROM parent WHERE a = 1 AND b = 10")
        .unwrap();
    let n = rows(
        engine
            .execute("SELECT COUNT(*) FROM child WHERE id = 100")
            .unwrap(),
    );
    assert_eq!(n[0][0], Value::Int(0));
    // The partial-NULL child is untouched.
    let n = rows(
        engine
            .execute("SELECT COUNT(*) FROM child WHERE id = 101")
            .unwrap(),
    );
    assert_eq!(n[0][0], Value::Int(1));
}

/// B3 — SHOW CREATE qualifies a cross-database referenced table with its
/// schema, and leaves a same-schema reference bare.
#[test]
fn fk_cross_database_show_create_qualifies_schema() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE DATABASE d1").unwrap();
    engine.execute("CREATE DATABASE d2").unwrap();
    engine
        .execute("CREATE TABLE d2.parent (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE d1.child (id INT PRIMARY KEY, pid INT, \
             CONSTRAINT fk_x FOREIGN KEY (pid) REFERENCES d2.parent (id))",
        )
        .unwrap();
    let create =
        rows(engine.execute("SHOW CREATE TABLE d1.child").unwrap())[0][1].to_display_string();
    assert!(
        create.contains("REFERENCES `d2`.`parent` (`id`)"),
        "cross-db FK must be schema-qualified: {create}"
    );

    // Same-schema reference stays bare.
    engine
        .execute("CREATE TABLE d1.parent (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE d1.child2 (id INT PRIMARY KEY, pid INT, \
             FOREIGN KEY (pid) REFERENCES d1.parent (id))",
        )
        .unwrap();
    let create =
        rows(engine.execute("SHOW CREATE TABLE d1.child2").unwrap())[0][1].to_display_string();
    assert!(
        create.contains("REFERENCES `parent` (`id`)") && !create.contains("`d1`.`parent`"),
        "same-db FK must not be schema-qualified: {create}"
    );
}

/// B2 — RENAME COLUMN propagates into FK metadata on both sides (child key
/// column and parent referenced column), keeping SHOW CREATE and enforcement
/// correct rather than silently dangling the constraint.
#[test]
fn fk_rename_column_propagates_to_constraint() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_parent_child(&engine, "", "");
    engine.execute("INSERT INTO child VALUES (10, 1)").unwrap();

    // Rename the child FK column.
    engine
        .execute("ALTER TABLE child RENAME COLUMN pid TO parent_id")
        .unwrap();
    let create = rows(engine.execute("SHOW CREATE TABLE child").unwrap())[0][1].to_display_string();
    assert!(
        create.contains("FOREIGN KEY (`parent_id`)"),
        "child FK column must be renamed in the constraint: {create}"
    );
    // Enforcement still works against the renamed column.
    let err = engine
        .execute("INSERT INTO child VALUES (11, 999)")
        .expect_err("FK must still enforce after rename");
    assert_eq!(err.code(), 1452, "{}", err.message());

    // Rename the parent referenced column; child's REFERENCES updates.
    engine
        .execute("ALTER TABLE parent RENAME COLUMN id TO pk")
        .unwrap();
    let create = rows(engine.execute("SHOW CREATE TABLE child").unwrap())[0][1].to_display_string();
    assert!(
        create.contains("REFERENCES `parent` (`pk`)"),
        "parent referenced column must be renamed in the constraint: {create}"
    );
    // Still enforcing.
    let err = engine
        .execute("INSERT INTO child VALUES (12, 888)")
        .expect_err("FK must still enforce after parent rename");
    assert_eq!(err.code(), 1452, "{}", err.message());
    engine.execute("INSERT INTO child VALUES (13, 2)").unwrap();
}

/// B8 — a `MATCH` clause parses and is accepted (InnoDB ignores it; mydb
/// implements MATCH SIMPLE) rather than erroring or being dropped by a
/// non-irrefutable destructure.
#[test]
fn fk_match_clause_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE parent (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE child (id INT PRIMARY KEY, pid INT, \
             FOREIGN KEY (pid) REFERENCES parent (id) MATCH SIMPLE)",
        )
        .unwrap();
    engine.execute("INSERT INTO parent VALUES (1)").unwrap();
    engine.execute("INSERT INTO child VALUES (10, 1)").unwrap();
    let err = engine
        .execute("INSERT INTO child VALUES (11, 2)")
        .expect_err("FK still enforced with MATCH clause");
    assert_eq!(err.code(), 1452, "{}", err.message());
}

/// G1 — the cascade depth backstop is runtime-tunable via
/// `SET GLOBAL mydb_fk_max_depth`.
#[test]
fn fk_max_depth_sysvar_runtime_tunable() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Readable and writable.
    engine.execute("SET GLOBAL mydb_fk_max_depth = 7").unwrap();
    let v = rows(engine.execute("SELECT @@global.mydb_fk_max_depth").unwrap());
    assert_eq!(v[0][0].to_display_string(), "7");
    // Restore a sane default for other tests sharing the process.
    engine.execute("SET GLOBAL mydb_fk_max_depth = 64").unwrap();
}

/// B5 — a string FK whose child/parent columns have different collations is
/// rejected (3780); matching collations are accepted.
#[test]
fn fk_string_collation_mismatch_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE parent (id VARCHAR(20) COLLATE utf8mb4_bin PRIMARY KEY)")
        .unwrap();
    let err = engine
        .execute(
            "CREATE TABLE child (id INT PRIMARY KEY, pid VARCHAR(20) COLLATE utf8mb4_general_ci, \
             FOREIGN KEY (pid) REFERENCES parent (id))",
        )
        .expect_err("string FK across differing collations must be rejected");
    assert_eq!(err.code(), 3780, "{}", err.message());

    // Same collation on both sides is accepted.
    engine
        .execute(
            "CREATE TABLE child_ok (id INT PRIMARY KEY, pid VARCHAR(20) COLLATE utf8mb4_bin, \
             FOREIGN KEY (pid) REFERENCES parent (id))",
        )
        .unwrap();
}

/// B6 — a virtual generated column cannot participate in a foreign key.
#[test]
fn fk_virtual_generated_column_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE parent (id INT PRIMARY KEY)")
        .unwrap();
    let err = engine
        .execute(
            "CREATE TABLE child (id INT PRIMARY KEY, base INT, \
             pid INT AS (base + 1) VIRTUAL, \
             FOREIGN KEY (pid) REFERENCES parent (id))",
        )
        .expect_err("virtual generated FK column must be rejected");
    assert_eq!(err.code(), 3733, "{}", err.message());
}

/// B9 — a child row referenced by two inbound FKs with different actions has
/// each action applied. With the coarse `(table, row)` visited set the second
/// action was swallowed, leaving an orphan; the per-edge discriminator fixes
/// it. Here one FK is SET NULL and the other CASCADE, so the row must be
/// deleted (CASCADE), never left dangling.
#[test]
fn fk_diamond_two_actions_apply_both() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE parent (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE child (id INT PRIMARY KEY, a INT, b INT, \
             CONSTRAINT fka FOREIGN KEY (a) REFERENCES parent (id) ON DELETE SET NULL, \
             CONSTRAINT fkb FOREIGN KEY (b) REFERENCES parent (id) ON DELETE CASCADE)",
        )
        .unwrap();
    engine.execute("INSERT INTO parent VALUES (5)").unwrap();
    engine
        .execute("INSERT INTO child VALUES (1, 5, 5)")
        .unwrap();

    engine.execute("DELETE FROM parent WHERE id = 5").unwrap();
    // CASCADE (fkb) must delete the row; no orphan with b = 5 survives.
    let n = rows(engine.execute("SELECT COUNT(*) FROM child").unwrap());
    assert_eq!(
        n[0][0],
        Value::Int(0),
        "child row must be cascade-deleted, not orphaned"
    );
}

/// Strict-durability engine for tests that must exercise real on-disk index
/// files (e.g. RENAME INDEX physical rename + reopen).
fn open_strict_fk_engine(dir: &std::path::Path) -> StorageQueryEngine {
    StorageQueryEngine::open_with_storage_config(
        dir,
        StorageConfig {
            durability: crate::storage::DurabilityMode::Strict,
            checkpoint_interval: std::time::Duration::from_millis(60_000),
            checkpoint_wal_bytes: 64 * 1024 * 1024,
            lock_wait: std::time::Duration::from_millis(1_000),
            group_commit_window: std::time::Duration::from_micros(0),
            wal_segment_bytes: 64 * 1024 * 1024,
            buffer_pool_pages: 512,
            buffer_pool_io_threads: 1,
            buffer_pool_prefetch_pages: 0,
            undo_retention: std::time::Duration::from_millis(60_000),
            heap_update_reserve_bytes: 2048,
            startup_validation: crate::storage::StartupValidationMode::Off,
            startup_validate_indexes: false,
        },
    )
    .unwrap()
}

/// Regression for S4 (alter-table-review-2026-07): renaming the index that
/// backs a foreign key must keep the FK's `backing_index` in sync, otherwise
/// parent-side CASCADE enforcement (which resolves the child index by name)
/// silently stops firing and leaves orphan rows. Runs on strict durability
/// so the index has a real on-disk file that the rename must carry (the fix
/// also flushes dirty index pages before the physical rename).
#[test]
fn rename_index_keeps_fk_backing_index_and_cascade() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_fk_engine(dir.path());
    engine
        .execute("CREATE TABLE parent (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE child (\
                 cid INT PRIMARY KEY, \
                 pid INT, \
                 KEY fk_idx (pid), \
                 CONSTRAINT fk_cp FOREIGN KEY (pid) REFERENCES parent (id) ON DELETE CASCADE)",
        )
        .unwrap();
    engine.execute("INSERT INTO parent VALUES (1)").unwrap();
    engine.execute("INSERT INTO child VALUES (10, 1)").unwrap();

    // The renamed index still reads back correctly (guards the flush fix).
    engine
        .execute("ALTER TABLE child RENAME INDEX fk_idx TO fk_idx2")
        .unwrap();
    let via_idx = rows(
        engine
            .execute("SELECT cid FROM child WHERE pid = 1")
            .unwrap(),
    );
    assert_eq!(via_idx.len(), 1, "renamed index must still read back rows");

    // Deleting the parent must still cascade to the child row.
    engine.execute("DELETE FROM parent WHERE id = 1").unwrap();
    let remaining = rows(engine.execute("SELECT COUNT(*) FROM child").unwrap());
    assert_eq!(
        remaining[0][0],
        Value::Int(0),
        "CASCADE must delete the child row after the backing index was renamed"
    );
}

#[test]
fn batch_fk_interleaved_parent_keys_keep_error_order_and_statement_atomicity() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE fk_batch_parent (id VARCHAR(20) COLLATE utf8mb4_0900_ai_ci PRIMARY KEY)",
        "CREATE TABLE fk_batch_child (id INT PRIMARY KEY,p VARCHAR(20) COLLATE utf8mb4_0900_ai_ci,FOREIGN KEY(p) REFERENCES fk_batch_parent(id))",
        "INSERT INTO fk_batch_parent VALUES ('alpha'),('beta'),('gamma')",
        "INSERT INTO fk_batch_child VALUES (1,'alpha'),(2,'beta'),(3,'ALPHA'),(4,'gamma'),(5,'beta'),(6,NULL)",
    ] {engine.execute(sql).unwrap();}
    let err=engine.execute("INSERT INTO fk_batch_child VALUES (7,'alpha'),(8,'beta'),(9,'ALPHA'),(10,'missing'),(11,'gamma')").unwrap_err();
    assert_eq!(err.code(), 1452);
    assert_eq!(
        rows(
            engine
                .execute("SELECT COUNT(*) FROM fk_batch_child")
                .unwrap()
        ),
        vec![vec![Value::Int(6)]]
    );
    engine
        .execute("INSERT INTO fk_batch_child VALUES (7,'gamma'),(8,'beta'),(9,'gamma')")
        .unwrap();
    assert_eq!(
        engine
            .execute("DELETE FROM fk_batch_parent WHERE id='gamma'")
            .unwrap_err()
            .code(),
        1451
    );
}
