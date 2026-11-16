use super::*;

fn result(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap().materialize().unwrap()
    else {
        panic!("{sql}")
    };
    result.rows
}

fn ddl(engine: &StorageQueryEngine, table: &str) -> String {
    result(engine, &format!("SHOW CREATE TABLE {table}"))[0][1].to_display_string()
}

#[test]
fn ddl1108_inline_keys_are_enforced_and_atomic() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine.execute("CREATE TABLE t(a INT)").unwrap();
    engine.execute("INSERT INTO t VALUES(1),(2)").unwrap();
    engine.execute("ALTER TABLE t ADD c INT UNIQUE").unwrap();
    engine.execute("INSERT INTO t VALUES(3,7)").unwrap();
    assert_eq!(
        engine
            .execute("INSERT INTO t VALUES(4,7)")
            .unwrap_err()
            .code(),
        1062
    );
    engine
        .execute("ALTER TABLE t MODIFY a INT PRIMARY KEY")
        .unwrap();
    assert!(ddl(&engine, "t").contains("PRIMARY KEY"));
    assert_eq!(
        engine
            .execute("ALTER TABLE t ADD x INT PRIMARY KEY")
            .unwrap_err()
            .code(),
        1068
    );
    assert!(!ddl(&engine, "t").contains("`x`"));
}

#[test]
fn ddl1108_primary_build_failure_preserves_nullability() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine.execute("CREATE TABLE t(id INT, v INT)").unwrap();
    engine.execute("INSERT INTO t VALUES(1,1),(2,2)").unwrap();
    let response = engine
        .execute("ALTER TABLE t MODIFY v BIGINT,ALGORITHM=COPY")
        .unwrap();
    assert!(matches!(
        response,
        QueryResponse::OkAffected { affected_rows: 2 }
    ));
    engine.execute("ALTER TABLE t ADD PRIMARY KEY(id)").unwrap();
    engine.execute("ALTER TABLE t DROP PRIMARY KEY").unwrap();
    engine.execute("ALTER TABLE t MODIFY id INT NULL").unwrap();
    engine.execute("INSERT INTO t VALUES(1,3)").unwrap();
    let before = ddl(&engine, "t");
    assert_eq!(
        engine
            .execute("ALTER TABLE t ADD PRIMARY KEY(id)")
            .unwrap_err()
            .code(),
        1062
    );
    assert_eq!(ddl(&engine, "t"), before);
    engine.execute("INSERT INTO t VALUES(NULL,4)").unwrap();
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(ddl(&engine, "t"), before);
    assert_eq!(
        result(&engine, "SELECT COUNT(*) FROM t")[0][0].to_display_string(),
        "4"
    );
    // The compatibility storage API uses the offline builder rather than the
    // SQL online path. Its failed candidate must preserve the same invariant.
    engine.execute("CREATE TABLE offline_t(id INT)").unwrap();
    engine
        .execute("INSERT INTO offline_t VALUES(1),(1)")
        .unwrap();
    engine
        .execute("CREATE TABLE template(id INT PRIMARY KEY)")
        .unwrap();
    let db = engine.database();
    let primary = db
        .table("test.template")
        .unwrap()
        .indexes
        .iter()
        .find(|index| index.primary)
        .unwrap()
        .clone();
    assert_eq!(
        db.add_index("test.offline_t", primary).unwrap_err().code(),
        1062
    );
    let current = db.table("test.offline_t").unwrap();
    assert!(current.primary_key.is_empty());
    assert!(current.columns[0].nullable);
    engine
        .execute("INSERT INTO offline_t VALUES(NULL)")
        .unwrap();
}

#[test]
fn ddl1108_show_create_roundtrips_typed_defaults_and_primary_parts() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine.execute("CREATE TABLE t(body TEXT NOT NULL,d DATE DEFAULT '2020-01-01',tm TIME DEFAULT '08:30:00',b BIT(8) DEFAULT NULL,PRIMARY KEY(body(8) DESC))").unwrap();
    let sql = ddl(&engine, "t");
    assert!(sql.contains("`body`(8) DESC"), "{sql}");
    assert!(sql.contains("DEFAULT '2020-01-01'"), "{sql}");
    assert!(sql.contains("DEFAULT '08:30:00'"), "{sql}");
    assert!(sql.contains("bit(8) DEFAULT NULL"), "{sql}");
    engine
        .execute(&sql.replacen("CREATE TABLE `t`", "CREATE TABLE `copy`", 1))
        .unwrap();
    engine
        .execute("INSERT INTO copy(body) VALUES('abcdefgh')")
        .unwrap();
    assert_eq!(
        result(&engine, "SELECT d,tm,b IS NULL FROM copy")[0]
            .iter()
            .map(Value::to_display_string)
            .collect::<Vec<_>>(),
        ["2020-01-01", "08:30:00", "1"]
    );
}

#[test]
fn ddl1108_tombstone_names_and_generated_dependencies() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t(id INT PRIMARY KEY,a INT,b INT,g INT GENERATED ALWAYS AS (a+1) STORED)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t(id,a,b) VALUES(1,10,20)")
        .unwrap();
    engine.execute("ALTER TABLE t DROP g").unwrap();
    engine.execute("ALTER TABLE t DROP a").unwrap();
    engine
        .execute("ALTER TABLE t ADD a INT DEFAULT 42")
        .unwrap();
    for sql in [
        "SELECT a FROM t",
        "SELECT t.a+1-1 FROM t",
        "SELECT a FROM t GROUP BY a",
    ] {
        assert_eq!(result(&engine, sql), vec![vec![Value::Int(42)]]);
    }
    engine.execute("ALTER TABLE t DROP b").unwrap();
    engine
        .execute("ALTER TABLE t RENAME COLUMN a TO b")
        .unwrap();
    assert_eq!(
        result(&engine, "SELECT b FROM t"),
        vec![vec![Value::Int(42)]]
    );
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(
        result(&engine, "SELECT b FROM t"),
        vec![vec![Value::Int(42)]]
    );
}

#[test]
fn ddl1108_invalid_definitions_leave_no_changes() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    for (sql, code) in [
        ("CREATE TABLE bad(a INT NOT NULL DEFAULT NULL)", 1067),
        ("CREATE TABLE bad(a INT,KEY k(a,a))", 1060),
        (
            "CREATE TABLE bad(a VARCHAR(10) CHARACTER SET latin1 DEFAULT '日本語')",
            1067,
        ),
        (
            "CREATE TABLE bad(id INT,g INT AS(id+1) VIRTUAL,PRIMARY KEY(g))",
            3106,
        ),
    ] {
        assert_eq!(engine.execute(sql).unwrap_err().code(), code, "{sql}");
    }
    engine
        .execute("CREATE TABLE t(a INT NOT NULL,b VARCHAR(3))")
        .unwrap();
    let before = ddl(&engine, "t");
    for sql in [
        "ALTER TABLE t ALTER a SET DEFAULT 'abc'",
        "ALTER TABLE t ALTER a SET DEFAULT NULL",
        "ALTER TABLE t ALTER b SET DEFAULT 'long'",
    ] {
        assert_eq!(engine.execute(sql).unwrap_err().code(), 1067, "{sql}");
        assert_eq!(ddl(&engine, "t"), before);
    }
}

#[test]
fn ddl1108_sequence_dependencies_do_not_stop_instance() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine.execute("CREATE DATABASE seqdb").unwrap();
    engine
        .execute("CREATE TABLE seqdb.t(id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("CREATE SEQUENCE seqdb.s OWNED BY seqdb.t.id")
        .unwrap();
    let db = engine.database();
    db.acquire_mdl(
        987654,
        None,
        MdlDuration::Explicit,
        vec![MdlRequest {
            key: MdlObjectKey::table("seqdb", "s"),
            lock_type: MdlLockType::SharedRead,
        }],
        "sequence reader during unrelated DDL",
    )
    .unwrap();
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET lock_wait_timeout=0")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE INDEX ix ON seqdb.t(id)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "ALTER TABLE seqdb.t ADD extra INT")
        .unwrap();
    assert_eq!(
        result(&engine, "SELECT NEXTVAL('seqdb.s')")[0][0].to_display_string(),
        "1"
    );
    assert_eq!(
        engine
            .execute_with_session(&mut session, "ALTER TABLE seqdb.t RENAME COLUMN id TO id2")
            .unwrap_err()
            .code(),
        1205
    );
    db.release_mdl_owner(987654);
    assert_eq!(
        engine
            .execute("RENAME TABLE seqdb.t TO test.moved")
            .unwrap_err()
            .code(),
        1235
    );
    engine.execute("CREATE TABLE healthy(a INT)").unwrap();
    engine
        .execute("ALTER TABLE seqdb.t RENAME COLUMN id TO id2")
        .unwrap();
    engine.execute("DROP DATABASE seqdb").unwrap();
    engine.execute("INSERT INTO healthy VALUES(1)").unwrap();
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(
        result(&engine, "SELECT a FROM healthy"),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn ddl1108_binary_spatial_and_vector_assignments() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(g GEOMETRY,v VECTOR(2))")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(X'000000000101000000000000000000F03F0000000000000040',X'0000803F00000040')").unwrap();
    assert_eq!(
        result(&engine, "SELECT ST_ASTEXT(g),VECTOR_TO_STRING(v) FROM t")[0]
            .iter()
            .map(Value::to_display_string)
            .collect::<Vec<_>>(),
        ["POINT(1 2)", "[1,2]"]
    );
    assert!(engine
        .execute("INSERT INTO t VALUES(X'00',X'0000803F00000040')")
        .is_err());
    assert!(engine
        .execute("INSERT INTO t VALUES(NULL,X'0000807F00000040')")
        .is_err());
    assert_eq!(
        result(&engine, "SELECT COUNT(*) FROM t")[0][0].to_display_string(),
        "1"
    );
}

#[test]
fn ddl1108_duplicate_drop_rejected_before_effects() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine.execute("CREATE TABLE t(a INT)").unwrap();
    assert_eq!(engine.execute("DROP TABLE t,t").unwrap_err().code(), 1066);
    engine.execute("ALTER TABLE t RENAME TO t").unwrap();
    engine.execute("INSERT INTO t VALUES(1)").unwrap();
    assert_eq!(
        engine
            .execute("SELECT * FROM t FORCE INDEX(missing)")
            .unwrap_err()
            .code(),
        1176
    );
}

#[test]
fn ddl1108_auto_increment_recalibrates_after_reenable() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT AUTO_INCREMENT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(NULL),(NULL)").unwrap();
    engine.execute("ALTER TABLE t MODIFY id INT").unwrap();
    engine.execute("INSERT INTO t VALUES(3),(50)").unwrap();
    engine
        .execute("ALTER TABLE t MODIFY id INT AUTO_INCREMENT")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(NULL)").unwrap();
    assert_eq!(
        result(&engine, "SELECT MAX(id) FROM t")[0][0].to_display_string(),
        "51"
    );
}

#[test]
fn ddl1108_histograms_copy_actions_and_view_creation_time() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine.execute("CREATE TABLE t(a INT,b INT)").unwrap();
    engine.execute("INSERT INTO t VALUES(1,1),(2,2)").unwrap();
    engine
        .execute("ANALYZE TABLE t UPDATE HISTOGRAM ON b WITH 2 BUCKETS")
        .unwrap();
    engine.execute("ALTER TABLE t ADD extra INT").unwrap();
    assert_eq!(result(&engine,"SELECT COLUMN_NAME FROM information_schema.COLUMN_STATISTICS WHERE TABLE_NAME='t' AND SCHEMA_NAME='test'"), vec![vec![Value::String("b".into())]]);
    engine
        .execute("ALTER TABLE t ADD CONSTRAINT ck CHECK(a>0),ALGORITHM=COPY")
        .unwrap();
    engine
        .execute("ALTER TABLE t RENAME COLUMN a TO id,ALGORITHM=COPY")
        .unwrap();
    engine.execute("CREATE VIEW v AS SELECT b FROM t").unwrap();
    let created=result(&engine,"SELECT CREATE_TIME FROM information_schema.TABLES WHERE TABLE_SCHEMA='test' AND TABLE_NAME='v'");
    assert!(!matches!(created[0][0], Value::Null));
    for sql in [
        "ALTER TABLE v ADD x INT",
        "CREATE INDEX bad ON v(b)",
        "DROP INDEX bad ON v",
    ] {
        assert_eq!(engine.execute(sql).unwrap_err().code(), 1347, "{sql}");
    }
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(result(&engine,"SELECT CREATE_TIME FROM information_schema.TABLES WHERE TABLE_SCHEMA='test' AND TABLE_NAME='v'"),created);
}

#[test]
fn ddl1108_width_metadata_and_srid_storage_survive_reopen() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(a INT(8) ZEROFILL,b TINYINT(1),g GEOMETRY)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(42,2,ST_GEOMFROMTEXT('POINT(1 2)',4326))")
        .unwrap();
    let expected = result(
        &engine,
        "SELECT LENGTH(a),CONCAT('[',a,']'),a+1,HEX(g),ST_ASTEXT(g) FROM t",
    );
    assert_eq!(expected[0][0], Value::Int(8));
    assert_eq!(expected[0][1], Value::String("[00000042]".into()));
    assert_eq!(
        expected[0][3],
        Value::String("E610000001010000000000000000000040000000000000F03F".into())
    );
    let before = ddl(&engine, "t");
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(ddl(&engine, "t"), before);
    assert_eq!(
        result(
            &engine,
            "SELECT LENGTH(a),CONCAT('[',a,']'),a+1,HEX(g),ST_ASTEXT(g) FROM t"
        ),
        expected
    );
}

#[test]
fn ddl1108_optimize_reclaims_dropped_lob_without_losing_live_columns() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,b LONGBLOB NOT NULL,s VARCHAR(30))")
        .unwrap();
    let mut seed = 7_u32;
    let bytes = (0..128 * 1024)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            (seed & 255) as u8
        })
        .collect::<Vec<_>>();
    let hex = crate::util::hex::encode_upper(&bytes);
    engine
        .execute(&format!("INSERT INTO t VALUES(1,X'{hex}','retained')"))
        .unwrap();
    engine.execute("ALTER SYSTEM CHECKPOINT").unwrap();
    let schema = engine.database().table("test.t").unwrap();
    let lob = |schema: &crate::storage::schema::TableSchema| {
        crate::storage::naming::table_object_generation_path(
            dir.path(),
            schema.id,
            schema.active_generation.max(1),
            "lob",
        )
    };
    let before = std::fs::metadata(lob(&schema)).unwrap().len();
    assert!(before > 128 * 1024);
    engine.execute("ALTER TABLE t DROP b").unwrap();
    engine.execute("OPTIMIZE TABLE t").unwrap();
    engine.execute("ALTER SYSTEM CHECKPOINT").unwrap();
    let current = engine.database().table("test.t").unwrap();
    let after = std::fs::metadata(lob(&current))
        .map(|m| m.len())
        .unwrap_or(0);
    assert!(after < before / 2, "before={before}, after={after}");
    assert_eq!(
        result(&engine, "SELECT id,s FROM t"),
        vec![vec![Value::Int(1), Value::String("retained".into())]]
    );
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(
        result(&engine, "SELECT id,s FROM t"),
        vec![vec![Value::Int(1), Value::String("retained".into())]]
    );
}

#[test]
fn ddl1108_numeric_string_consumers_preserve_all_numeric_widths() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY,a DECIMAL(8,2) ZEROFILL,b FLOAT(8,2) ZEROFILL,c DOUBLE(8,2) ZEROFILL,d BOOL ZEROFILL)").unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,4.25,4.25,4.25,2)")
        .unwrap();
    for sql in [
        "SELECT LENGTH(a),LENGTH(b),LENGTH(c),CONCAT(a,':',b,':',c),LENGTH(d) FROM t",
        "SELECT LENGTH(t.a),LENGTH(t.b),LENGTH(t.c),CONCAT(t.a,':',t.b,':',t.c),LENGTH(t.d) FROM t JOIN (SELECT 1 AS x) q ON t.id=q.x",
    ] {
        assert_eq!(result(&engine, sql), vec![vec![Value::Int(9),Value::Int(8),Value::Int(8),Value::String("000004.25:00004.25:00004.25".into()),Value::Int(1)]], "{sql}");
    }
    assert_eq!(
        result(&engine, "SELECT SUM(LENGTH(a)+LENGTH(b)+LENGTH(c)) FROM t")[0][0]
            .to_display_string(),
        "25"
    );
}
