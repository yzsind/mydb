//! Regressions and corrected audit claims from the 2026-09-27 table DDL audit.
use super::*;

fn fixture() -> tempfile::TempDir {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    tempfile::Builder::new()
        .prefix("table-ddl-audit-")
        .tempdir_in("/tmp/mydb")
        .unwrap()
}

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected rows: {sql}");
    };
    result.rows
}

#[test]
fn ddl_audit_short_rows_keep_historical_values_after_copy_and_restart() {
    let dir = fixture();
    {
        let engine = open_strict_test_engine(dir.path());
        for (name, ty, default) in [
            ("ints", "INT", "0"),
            ("strings", "VARCHAR(20)", "''"),
            ("decimals", "DECIMAL(10,2)", "0"),
        ] {
            engine
                .execute(&format!("CREATE TABLE {name}(id INT PRIMARY KEY, b INT)"))
                .unwrap();
            engine
                .execute(&format!("INSERT INTO {name} VALUES(1,10),(2,20)"))
                .unwrap();
            engine
                .execute(&format!("ALTER TABLE {name} ADD c {ty} NOT NULL"))
                .unwrap();
            engine
                .execute(&format!(
                    "ALTER TABLE {name} MODIFY c {ty} NULL DEFAULT '5'"
                ))
                .unwrap();
            assert_eq!(
                rows(
                    &engine,
                    &format!("SELECT COUNT(*) FROM {name} WHERE c={default}")
                ),
                vec![vec![Value::Int(2)]]
            );
        }
        engine
            .execute("CREATE TABLE explicit_default(id INT PRIMARY KEY)")
            .unwrap();
        engine
            .execute("INSERT INTO explicit_default VALUES(1)")
            .unwrap();
        engine
            .execute("ALTER TABLE explicit_default ADD c INT DEFAULT 3")
            .unwrap();
        engine
            .execute("ALTER TABLE explicit_default MODIFY c INT DEFAULT 5")
            .unwrap();
        assert_eq!(
            rows(&engine, "SELECT c FROM explicit_default"),
            vec![vec![Value::Int(3)]]
        );
        engine
            .execute("CREATE TABLE mixed(id INT PRIMARY KEY,b INT)")
            .unwrap();
        engine.execute("INSERT INTO mixed VALUES(1,10)").unwrap();
        engine
            .execute("ALTER TABLE mixed ADD c INT NOT NULL, MODIFY b BIGINT")
            .unwrap();
        assert_eq!(
            rows(&engine, "SELECT c FROM mixed"),
            vec![vec![Value::Int(0)]]
        );
    }
    let reopened = open_strict_test_engine(dir.path());
    assert_eq!(
        rows(&reopened, "SELECT c FROM ints ORDER BY id"),
        vec![vec![Value::Int(0)], vec![Value::Int(0)]]
    );
    assert_eq!(
        rows(&reopened, "SELECT c FROM explicit_default"),
        vec![vec![Value::Int(3)]]
    );
}

#[test]
fn ddl_audit_duplicate_primary_definitions_rejected() {
    let dir = fixture();
    let engine = open_test_engine(dir.path());
    for definition in [
        "a INT PRIMARY KEY,b INT PRIMARY KEY",
        "a INT PRIMARY KEY,b INT,PRIMARY KEY(b)",
        "a INT,b INT,PRIMARY KEY(a),PRIMARY KEY(b)",
        "a VARCHAR(20) PRIMARY KEY,PRIMARY KEY(a(10))",
        "a VARCHAR(20),PRIMARY KEY(a(10)),PRIMARY KEY(a)",
    ] {
        let err = engine
            .execute(&format!("CREATE TABLE bad({definition})"))
            .unwrap_err();
        assert_eq!(err.code(), 1068, "{definition}: {err:?}");
    }
    engine
        .execute("CREATE TABLE good(a INT,b INT,PRIMARY KEY(a,b))")
        .unwrap();
}

#[test]
fn ddl_audit_like_preserves_expression_indexes_and_uniqueness() {
    let dir = fixture();
    let engine = open_strict_test_engine(dir.path());
    engine.execute("CREATE TABLE source(a VARCHAR(20), b JSON, UNIQUE KEY fun ((LOWER(a))), KEY mv ((CAST(b->'$.x' AS UNSIGNED ARRAY))))").unwrap();
    for temporary in [false, true] {
        let name = if temporary { "tmp_copy" } else { "copy" };
        let mut session = SessionContext::default();
        engine
            .execute_with_session(
                &mut session,
                &format!(
                    "CREATE {} TABLE {name} LIKE source",
                    if temporary { "TEMPORARY" } else { "" }
                ),
            )
            .unwrap();
        engine
            .execute_with_session(
                &mut session,
                &format!("INSERT INTO {name} VALUES('A','{{\"x\":[1,2]}}')"),
            )
            .unwrap();
        let err = engine
            .execute_with_session(
                &mut session,
                &format!("INSERT INTO {name} VALUES('a','{{\"x\":[3]}}')"),
            )
            .unwrap_err();
        assert_eq!(err.code(), 1062);
        let QueryResponse::ResultSet(result) = engine
            .execute_with_session(&mut session, &format!("SHOW CREATE TABLE {name}"))
            .unwrap()
        else {
            panic!()
        };
        let ddl = result.rows[0][1].to_display_string();
        assert!(ddl.contains("`fun`") && ddl.contains("`mv`"), "{ddl}");
    }
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(
        engine
            .execute("INSERT INTO copy VALUES('a',NULL)")
            .unwrap_err()
            .code(),
        1062
    );
}

#[test]
fn ddl_audit_visibility_is_bound_to_its_index() {
    let dir = fixture();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE vis(a INT UNIQUE,b INT, KEY k_b(b) INVISIBLE, KEY(b) VISIBLE)")
        .unwrap();
    let schema = engine.database().table("test.vis").unwrap();
    assert!(
        schema
            .indexes
            .iter()
            .find(|i| i.name == "a")
            .unwrap()
            .visible
    );
    assert!(
        !schema
            .indexes
            .iter()
            .find(|i| i.name == "k_b")
            .unwrap()
            .visible
    );
    assert!(
        schema
            .indexes
            .iter()
            .find(|i| i.name == "b")
            .unwrap()
            .visible
    );
    engine
        .execute("CREATE TABLE prefix_pk(a BLOB, PRIMARY KEY(a(10)), KEY k(a(10)) INVISIBLE)")
        .unwrap();
    engine
        .execute("ALTER TABLE vis ADD COLUMN c INT UNIQUE, ADD KEY k_c(c) INVISIBLE")
        .unwrap();
    assert!(
        !engine
            .database()
            .table("test.vis")
            .unwrap()
            .indexes
            .iter()
            .find(|i| i.name == "k_c")
            .unwrap()
            .visible
    );
}

#[test]
fn ddl_audit_xa_ddl_rejection_preserves_rollback() {
    let dir = fixture();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE xa_t(a INT, KEY k(a))")
        .unwrap();
    for (i, ddl) in [
        "CREATE TABLE xa_new(a INT)",
        "ALTER TABLE xa_t ADD b INT",
        "DROP TABLE xa_t",
        "TRUNCATE TABLE xa_t",
        "RENAME TABLE xa_t TO xa_new",
        "CREATE INDEX k2 ON xa_t(a)",
        "DROP INDEX k ON xa_t",
    ]
    .iter()
    .enumerate()
    {
        let mut session = SessionContext::default();
        let xid = format!("'ddl_audit_{i}'");
        engine
            .execute_with_session(&mut session, &format!("XA START {xid}"))
            .unwrap();
        engine
            .execute_with_session(&mut session, "INSERT INTO xa_t VALUES(1)")
            .unwrap();
        assert_eq!(
            engine
                .execute_with_session(&mut session, ddl)
                .unwrap_err()
                .code(),
            1399,
            "{ddl}"
        );
        assert!(rows(&engine, "SELECT * FROM xa_t").is_empty());
        engine
            .execute_with_session(&mut session, &format!("XA END {xid}"))
            .unwrap();
        engine
            .execute_with_session(&mut session, &format!("XA PREPARE {xid}"))
            .unwrap();
        engine
            .execute_with_session(&mut session, &format!("XA ROLLBACK {xid}"))
            .unwrap();
        assert!(rows(&engine, "SELECT * FROM xa_t").is_empty());
    }
}

#[test]
fn ddl_audit_nonstrict_null_conversion_and_strict_rollback() {
    let dir = fixture();
    let engine = open_relaxed_test_engine(dir.path());
    for (i, ty) in [
        "INT",
        "VARCHAR(20)",
        "DECIMAL(10,2)",
        "DATE",
        "ENUM('a','b')",
    ]
    .iter()
    .enumerate()
    {
        let name = format!("nulls_{i}");
        engine
            .execute(&format!("CREATE TABLE {name}(c {ty})"))
            .unwrap();
        engine
            .execute(&format!("INSERT INTO {name} VALUES(NULL)"))
            .unwrap();
        let ddl = format!("ALTER TABLE {name} MODIFY c {ty} NOT NULL");
        assert_eq!(engine.execute(&ddl).unwrap_err().code(), 1048);
        assert_eq!(
            rows(&engine, &format!("SELECT c FROM {name}")),
            vec![vec![Value::Null]]
        );
        let mut session = SessionContext::default();
        engine
            .execute_with_session(&mut session, "SET sql_mode=''")
            .unwrap();
        engine.execute_with_session(&mut session, &ddl).unwrap();
        let QueryResponse::ResultSet(warnings) = engine
            .execute_with_session(&mut session, "SHOW WARNINGS")
            .unwrap()
        else {
            panic!()
        };
        assert!(!warnings.rows.is_empty(), "{ty}");
        // DATE NOT NULL's zero date intentionally matches IS NULL for
        // MySQL legacy compatibility; inspect the stored value directly.
        assert!(
            !matches!(
                rows(&engine, &format!("SELECT c FROM {name}"))[0][0],
                Value::Null
            ),
            "{ty}"
        );
    }
}

#[test]
fn ddl_audit_names_and_check_determinism() {
    let dir = fixture();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE names(`MyCol` INT, INDEX(`MyCol`), INDEX(`MyCol`))")
        .unwrap();
    let schema = engine.database().table("test.names").unwrap();
    assert!(schema.indexes.iter().any(|i| i.name == "MyCol"));
    assert!(schema.indexes.iter().any(|i| i.name == "MyCol_2"));
    engine
        .execute("ALTER TABLE names RENAME INDEX MyCol TO MYCOL")
        .unwrap();
    assert!(engine
        .database()
        .table("test.names")
        .unwrap()
        .indexes
        .iter()
        .any(|i| i.name == "MYCOL"));
    assert_eq!(
        engine
            .execute("ALTER TABLE names RENAME INDEX missing TO MISSING")
            .unwrap_err()
            .code(),
        1091
    );
    assert_eq!(
        engine
            .execute("RENAME TABLE names TO NAMES")
            .unwrap_err()
            .code(),
        1050
    );
    assert_eq!(
        engine
            .execute("RENAME TABLE missing TO MISSING")
            .unwrap_err()
            .code(),
        1146
    );
    for expr in [
        "NOW()",
        "RAND()",
        "CURRENT_TIMESTAMP",
        "COALESCE(NOW(),0)",
        "CURRENT_USER()",
    ] {
        let err = engine
            .execute(&format!("CREATE TABLE bad_check(a INT,CHECK(a>{expr}))"))
            .unwrap_err();
        assert_eq!(err.code(), 3814, "{expr}: {err:?}");
    }
    engine
        .execute("CREATE TABLE good_check(a INT,CHECK(ABS(a)<10))")
        .unwrap();
    engine.execute("CREATE DATABASE other_schema").unwrap();
    engine
        .execute("CREATE VIEW name_view AS SELECT * FROM names")
        .unwrap();
    assert_eq!(
        engine
            .execute("RENAME TABLE name_view TO other_schema.name_view")
            .unwrap_err()
            .code(),
        1450
    );
    engine
        .execute("RENAME TABLE name_view TO renamed_view")
        .unwrap();
    engine.execute("SELECT * FROM renamed_view").unwrap();
}

#[test]
fn ddl_audit_corrupt_rename_intent_does_not_wedge_reopen() {
    let dir = fixture();
    {
        let engine = open_strict_test_engine(dir.path());
        engine.execute("CREATE TABLE keep_me(a INT)").unwrap();
        engine.execute("INSERT INTO keep_me VALUES(7)").unwrap();
    }
    let journal = dir.path().join("ddl/rename_table");
    std::fs::create_dir_all(&journal).unwrap();
    std::fs::write(journal.join("broken.json"), b"{\"version\":1,\"tab").unwrap();
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(
        rows(&engine, "SELECT a FROM keep_me"),
        vec![vec![Value::Int(7)]]
    );
    assert!(journal.join("broken.json").exists());
}

#[test]
fn ddl_audit_corrected_compatibility_claims() {
    let dir = fixture();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE ctas(a INT) AS SELECT 7 AS x")
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT a,x FROM ctas"),
        vec![vec![Value::Null, Value::Int(7)]]
    );
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TEMPORARY TABLE shadowed(a INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE shadowed(b INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "DROP TEMPORARY TABLE shadowed")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SELECT b FROM shadowed")
        .unwrap();
    engine
        .execute("CREATE TABLE gen(a INT,g INT AS(a+1))")
        .unwrap();
    engine.execute("INSERT INTO gen(a) VALUES(3)").unwrap();
    engine
        .execute("ALTER TABLE gen MODIFY g INT AS(a*2)")
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT g FROM gen"),
        vec![vec![Value::Int(6)]]
    );
    for bounds in [
        "100),PARTITION p1 VALUES LESS THAN(50",
        "100),PARTITION p1 VALUES LESS THAN(100",
        "MAXVALUE),PARTITION p1 VALUES LESS THAN(100",
    ] {
        assert_eq!(engine.execute(&format!("CREATE TABLE bad_range(a INT) PARTITION BY RANGE(a)(PARTITION p0 VALUES LESS THAN({bounds}))")).unwrap_err().code(),if bounds.starts_with("MAXVALUE") {1481} else {1493});
    }
}

#[test]
fn ddl_audit_fk_declarations_lock_the_parent_schema() {
    use crate::storage::mdl::{MdlDuration, MdlLockType, MdlObjectKey, MdlRequest};
    let dir = fixture();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE DATABASE fk_schema").unwrap();
    engine
        .execute("CREATE TABLE fk_schema.parent(id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("CREATE TABLE fk_schema.altered(id INT)")
        .unwrap();
    let db = engine.database();
    for sql in [
        "CREATE TABLE fk_schema.child(id INT, FOREIGN KEY(id) REFERENCES parent(id))",
        "CREATE TABLE fk_schema.ctas(id INT, FOREIGN KEY(id) REFERENCES parent(id)) AS SELECT id FROM fk_schema.parent",
        "ALTER TABLE fk_schema.altered ADD FOREIGN KEY(id) REFERENCES parent(id)",
    ] {
        db.acquire_mdl(987654, Some(987654), MdlDuration::Explicit,
            vec![MdlRequest { key: MdlObjectKey::table("fk_schema", "parent"), lock_type: MdlLockType::Exclusive }],
            "DDL audit parent lock").unwrap();
        let mut session = SessionContext::default();
        engine.execute_with_session(&mut session,"SET lock_wait_timeout=1").unwrap();
        let result = engine.execute_with_session(&mut session,sql);
        db.release_mdl_owner(987654);
        assert_eq!(result.unwrap_err().code(),1205,"{sql}");
        engine.execute(sql).unwrap();
    }
    assert_eq!(
        engine
            .execute("DROP TABLE fk_schema.parent")
            .unwrap_err()
            .code(),
        3730
    );
}

#[test]
fn ddl_audit_like_io_failure_removes_unpublished_table() {
    let dir = fixture();
    let engine = open_strict_test_engine(dir.path());
    engine.execute("CREATE TABLE source_like(a INT)").unwrap();
    let db = engine.database();
    let source = db.table("test.source_like").unwrap();
    let id = db.catalog().next_table_id;
    let heap = crate::storage::naming::table_object_generation_path(dir.path(), id, 1, "heap");
    std::fs::create_dir_all(&heap).unwrap();
    assert!(db.create_table_like("test.failed_like", &source).is_err());
    assert!(!db.relation_name_exists("test.failed_like"));
    std::fs::remove_dir(&heap).unwrap();
    db.create_table_like("test.failed_like", &source).unwrap();
}

#[test]
fn ddl_audit_temp_like_io_failure_removes_unpublished_table() {
    let dir = fixture();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE source_like(a INT)").unwrap();
    let db = engine.database();
    let source = db.table("test.source_like").unwrap();
    let temp_path = crate::storage::naming::temp_tables_dir(dir.path());
    std::fs::create_dir_all(&temp_path).unwrap();
    let id = 1_u64 << 63;
    let heap = temp_path.join(format!("{id}.heap"));
    std::fs::create_dir_all(&heap).unwrap();
    assert!(db
        .create_temp_table_like(
            "test.failed_temp",
            333,
            &Arc::new(crate::storage::temp_quota::SessionTempQuota::default()),
            &source
        )
        .is_err());
    assert!(db.table(&format!("__mydb_temp.333_{id}")).is_err());
    std::fs::remove_dir(&heap).unwrap();
    db.create_temp_table_like(
        "test.failed_temp",
        333,
        &Arc::new(crate::storage::temp_quota::SessionTempQuota::default()),
        &source,
    )
    .unwrap();
}

#[test]
fn ddl_audit_building_index_cannot_be_renamed() {
    let dir = fixture();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE building(a INT)").unwrap();
    let db = engine.database();
    db.inject_building_index_for_test("test", "building", "pending", vec!["a".into()])
        .unwrap();
    assert_eq!(
        db.rename_index("test.building", "pending", "renamed")
            .unwrap_err()
            .code(),
        1235
    );
    assert!(db
        .table("test.building")
        .unwrap()
        .indexes
        .iter()
        .any(|index| index.name == "pending"));
}

#[test]
fn ddl_audit_instant_backfill_is_independent_of_insert_default() {
    let dir = fixture();
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE frozen(id INT PRIMARY KEY)")
            .unwrap();
        engine.execute("INSERT INTO frozen VALUES(1)").unwrap();
        engine.execute("ALTER TABLE frozen ADD c INT NULL").unwrap();
        engine
            .execute("ALTER TABLE frozen ALTER c SET DEFAULT 5")
            .unwrap();
        engine.execute("INSERT INTO frozen(id) VALUES(2)").unwrap();
        assert_eq!(
            rows(&engine, "SELECT c FROM frozen ORDER BY id"),
            vec![vec![Value::Null], vec![Value::Int(5)]]
        );
        engine
            .execute("ALTER TABLE frozen MODIFY c BIGINT DEFAULT 8")
            .unwrap();
        engine.execute("INSERT INTO frozen(id) VALUES(3)").unwrap();
        assert_eq!(
            rows(&engine, "SELECT c FROM frozen ORDER BY id"),
            vec![vec![Value::Null], vec![Value::Int(5)], vec![Value::Int(8)]]
        );
    }
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(
        rows(&engine, "SELECT c FROM frozen ORDER BY id"),
        vec![vec![Value::Null], vec![Value::Int(5)], vec![Value::Int(8)]]
    );
}

#[test]
fn ddl_audit_compound_not_null_add_works_on_memory_and_partition_paths() {
    let dir = fixture();
    let engine = open_test_engine(dir.path());
    for (name, partition) in [
        ("plain", ""),
        ("partitioned", "PARTITION BY HASH(id) PARTITIONS 2"),
    ] {
        engine
            .execute(&format!(
                "CREATE TABLE {name}(id INT PRIMARY KEY,b INT) {partition}"
            ))
            .unwrap();
        engine
            .execute(&format!("INSERT INTO {name} VALUES(1,10),(2,20)"))
            .unwrap();
        engine
            .execute(&format!(
                "ALTER TABLE {name} ADD c INT NOT NULL, MODIFY b BIGINT"
            ))
            .unwrap();
        assert_eq!(
            rows(&engine, &format!("SELECT c FROM {name} ORDER BY id")),
            vec![vec![Value::Int(0)], vec![Value::Int(0)]]
        );
    }
}
