use super::support::result_rows as rows;
use super::*;

#[test]
fn create_and_alter_table_comments_defaults_and_column_position() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (a INT COMMENT 'first', b INT) ENGINE=InnoDB COMMENT='table c'")
        .unwrap();
    let create = rows(engine.execute("SHOW CREATE TABLE t").unwrap())[0][1].to_display_string();
    assert!(
        create.contains("`a` int DEFAULT NULL COMMENT 'first'"),
        "{create}"
    );
    assert!(create.contains("COMMENT='table c'"), "{create}");

    engine.execute("INSERT INTO t VALUES (1, 2)").unwrap();
    engine
        .execute("ALTER TABLE t ADD COLUMN c INT DEFAULT 7 AFTER a")
        .unwrap();
    assert_eq!(
        rows(engine.execute("SELECT * FROM t").unwrap())[0],
        vec![Value::Int(1), Value::Int(7), Value::Int(2)]
    );

    engine
        .execute("ALTER TABLE t ALTER COLUMN c SET DEFAULT 9")
        .unwrap();
    engine
        .execute("INSERT INTO t (a, b) VALUES (3, 4)")
        .unwrap();
    assert_eq!(
        rows(engine.execute("SELECT c FROM t WHERE a = 3").unwrap())[0][0],
        Value::Int(9)
    );

    engine
        .execute("ALTER TABLE t MODIFY COLUMN c INT COMMENT 'middle' FIRST")
        .unwrap();
    let columns = rows(engine.execute("SHOW FULL COLUMNS FROM t").unwrap());
    assert_eq!(columns[0][0], Value::String("c".to_string()));
    assert_eq!(columns[0][8], Value::String("middle".to_string()));
}

#[test]
fn rename_index_drop_primary_and_drop_multiple_tables() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, KEY v_idx (v))")
        .unwrap();
    engine
        .execute("ALTER TABLE t RENAME INDEX v_idx TO v2")
        .unwrap();
    let indexes = rows(engine.execute("SHOW INDEX FROM t").unwrap());
    assert!(indexes
        .iter()
        .any(|row| row[2] == Value::String("v2".to_string())));

    engine.execute("ALTER TABLE t DROP PRIMARY KEY").unwrap();
    let create = rows(engine.execute("SHOW CREATE TABLE t").unwrap())[0][1].to_display_string();
    assert!(!create.contains("PRIMARY KEY"), "{create}");

    engine.execute("CREATE TABLE a (id INT)").unwrap();
    engine.execute("CREATE TABLE b (id INT)").unwrap();
    engine
        .execute("DROP TABLE IF EXISTS a, b, missing")
        .unwrap();
    assert!(engine.execute("SELECT * FROM a").is_err());
    assert!(engine.execute("SELECT * FROM b").is_err());
}

#[test]
fn multi_action_alter_is_prevalidated_before_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine.execute("CREATE TABLE t (a INT)").unwrap();
    let err = engine
        .execute("ALTER TABLE t ADD COLUMN b INT, DROP COLUMN missing")
        .unwrap_err();
    assert_eq!(err.code(), 1054, "{}", err.message());

    let columns = rows(engine.execute("SHOW COLUMNS FROM t").unwrap());
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0][0], Value::String("a".to_string()));
}

#[test]
fn truncate_allows_self_referencing_fk_but_rejects_external_reference() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE node (id INT PRIMARY KEY, parent_id INT, \
             FOREIGN KEY (parent_id) REFERENCES node (id))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO node VALUES (1, NULL), (2, 1)")
        .unwrap();
    engine.execute("TRUNCATE TABLE node").unwrap();
    assert_eq!(rows(engine.execute("SELECT * FROM node").unwrap()).len(), 0);

    engine
        .execute("CREATE TABLE parent (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("CREATE TABLE child (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES parent (id))")
        .unwrap();
    let err = engine.execute("TRUNCATE TABLE parent").unwrap_err();
    assert_eq!(err.code(), 1701, "{}", err.message());
}

#[test]
fn rename_table_multi_pair_renames_each_pair_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY)")
        .unwrap();

    // Two independent pairs.
    engine.execute("RENAME TABLE a TO a2, b TO b2").unwrap();

    // The catalog reflects both renames.
    let tables: std::collections::BTreeSet<String> = rows(engine.execute("SHOW TABLES").unwrap())
        .into_iter()
        .filter_map(|r| r.into_iter().next().map(|v| v.to_display_string()))
        .collect();
    assert!(tables.contains("a2"));
    assert!(tables.contains("b2"));
    assert!(!tables.contains("a"));
    assert!(!tables.contains("b"));
}

#[test]
fn rename_table_multi_pair_rolls_back_on_mid_failure() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine.execute("CREATE TABLE a (id INT)").unwrap();
    engine.execute("CREATE TABLE b (id INT)").unwrap();

    // The second pair fails (nonexistent source). The first pair must roll back.
    let err = engine
        .execute("RENAME TABLE a TO a2, nonexistent TO x")
        .unwrap_err();
    assert_eq!(err.code(), 1146, "{}", err.message());

    // `a` is still `a`; `a2` was rolled back.
    assert_eq!(
        rows(engine.execute("SHOW TABLES").unwrap())
            .into_iter()
            .map(|r| r[0].to_display_string())
            .collect::<std::collections::BTreeSet<_>>(),
        ["a", "b"]
            .iter()
            .map(|s| s.to_string())
            .collect::<std::collections::BTreeSet<_>>()
    );
}

#[test]
fn default_expression_default_now_uuid_evaluated_at_insert() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE dft (\
             id INT PRIMARY KEY, \
             t DATETIME DEFAULT (NOW()), \
             u CHAR(36) DEFAULT (UUID()), \
             prefix CHAR(4) DEFAULT (LEFT('hello', 4)))",
        )
        .unwrap();
    engine.execute("INSERT INTO dft (id) VALUES (1)").unwrap();

    let row = rows(
        engine
            .execute("SELECT t IS NOT NULL, LENGTH(u), prefix FROM dft")
            .unwrap(),
    );
    assert_eq!(row[0][0], Value::Int(1));
    assert_eq!(row[0][1], Value::Int(36));
    assert_eq!(row[0][2].to_display_string(), "hell");
}

/// T3 — TRUNCATE on a VIEW must return 1347 ('db.v' is not BASE TABLE),
/// not 1146 (unknown table). MySQL 8.0 rejects TRUNCATE on views and
/// updatable views alike; base-table-only is a hard error class.
#[test]
fn truncate_on_view_returns_1347_not_1146() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE base (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO base VALUES (1, 10)").unwrap();
    engine
        .execute("CREATE VIEW v AS SELECT * FROM base")
        .unwrap();

    let err = engine.execute("TRUNCATE TABLE v").unwrap_err();
    assert_eq!(err.code(), 1347, "{}", err.message());
    assert!(
        err.message().contains("not BASE TABLE"),
        "unexpected message: {}",
        err.message()
    );
}

/// T5 — TRUNCATE deletes index files (WAL FileDelete) rather than
/// rebuilding them via a direct-file write. The next INSERT after
/// TRUNCATE must find or create an empty index and populate it.
/// Regression for the replica-side "primary rebuild bypassed WAL, replica
/// has no .idx file" divergence and the wasted `rebuild_indexes` heap
/// scan on the primary.
#[test]
fn truncate_reuses_secondary_index_after_wipe() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, INDEX idx_v (v))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    engine.execute("TRUNCATE TABLE t").unwrap();
    // Table is empty.
    assert_eq!(rows(engine.execute("SELECT * FROM t").unwrap()).len(), 0);
    // The secondary index survives lazily and takes new rows.
    engine
        .execute("INSERT INTO t VALUES (10, 100), (20, 200)")
        .unwrap();
    let out = rows(engine.execute("SELECT id FROM t WHERE v = 200").unwrap());
    assert_eq!(out, vec![vec![Value::Int(20)]]);
    let all = rows(engine.execute("SELECT id, v FROM t ORDER BY id").unwrap());
    assert_eq!(
        all,
        vec![
            vec![Value::Int(10), Value::Int(100)],
            vec![Value::Int(20), Value::Int(200)],
        ]
    );
}

/// T1 — TRUNCATE on a table with an out-of-line LOB column must not
/// leave the heap referencing overflow chains that got unlinked. The
/// old ordering unlinked `.lob` before emptying the heap, so a crash
/// between the two left dangling LobRefs.
///
/// Even without crash injection, this end-to-end assert covers the
/// happy path: after TRUNCATE, no query can be tricked into reading
/// the old LOB chain, and subsequent inserts to the LOB column round-
/// trip correctly.
#[test]
fn truncate_with_lob_column_leaves_no_dangling_ref() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, body LONGBLOB)")
        .unwrap();
    // ~10 KiB blob, guaranteed to exceed inline threshold and go to
    // overflow chain.
    let big = "x".repeat(10 * 1024);
    engine
        .execute(&format!("INSERT INTO t VALUES (1, '{big}')"))
        .unwrap();
    engine.execute("TRUNCATE TABLE t").unwrap();
    assert_eq!(rows(engine.execute("SELECT * FROM t").unwrap()).len(), 0);
    // Reinsert to the same table id via the fresh LOB chain.
    let big2 = "y".repeat(20 * 1024);
    engine
        .execute(&format!("INSERT INTO t VALUES (2, '{big2}')"))
        .unwrap();
    let out = rows(
        engine
            .execute("SELECT LENGTH(body) FROM t WHERE id = 2")
            .unwrap(),
    );
    assert_eq!(out, vec![vec![Value::Int((20 * 1024) as i64)]]);
}

/// T7 — resetting stats bumps schema_version so cached physical plans
/// costed against the pre-truncate row count re-cost against 0. We
/// observe the effect indirectly: the second EXPLAIN after TRUNCATE must
/// see an updated row estimate.
#[test]
fn truncate_bumps_schema_version_for_plan_recost() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for i in 0..200 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE t").unwrap();
    let pre_ver = engine
        .db_for_tests()
        .table("test.t")
        .expect("table")
        .schema_version;
    engine.execute("TRUNCATE TABLE t").unwrap();
    let post_ver = engine
        .db_for_tests()
        .table("test.t")
        .expect("table")
        .schema_version;
    assert!(
        post_ver > pre_ver,
        "schema_version must bump after TRUNCATE (pre={pre_ver}, post={post_ver})"
    );
}

#[test]
fn delimiter_is_a_rejected_client_directive() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for sql in ["DELIMITER ;;", "DELIMITER //"] {
        assert_eq!(engine.execute(sql).unwrap_err().code(), 1235);
    }
}
