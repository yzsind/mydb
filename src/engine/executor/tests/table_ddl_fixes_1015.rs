//! Integrity regressions for the table DDL audit, independent of MySQL syntax parity.
use super::support::execute_materialized as exec;
use super::*;

fn fixture() -> tempfile::TempDir {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    tempfile::Builder::new()
        .prefix("table-ddl-fixes-")
        .tempdir_in("/tmp/mydb")
        .unwrap()
}

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap().materialize().unwrap()
    else {
        panic!("expected rows for {sql}");
    };
    result.rows
}

#[test]
fn copy_recovery_ignores_unpublished_partial_intents() {
    let dir = fixture();
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT,KEY vi(v))")
            .unwrap();
        engine.execute("INSERT INTO t VALUES(1,7)").unwrap();
    }
    let journal_dir = dir.path().join("ddl/alter_copy");
    std::fs::create_dir_all(&journal_dir).unwrap();
    std::fs::write(journal_dir.join("1.job.tmp"), b"").unwrap();
    std::fs::write(journal_dir.join("2.job.tmp"), b"{\"version\":").unwrap();
    for _ in 0..2 {
        let engine = open_strict_test_engine(dir.path());
        assert_eq!(
            rows(&engine, "SELECT id,v FROM t FORCE INDEX(vi) WHERE v=7"),
            vec![vec![Value::Int(1), Value::Int(7)]]
        );
    }
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("ALTER TABLE t MODIFY v BIGINT,ALGORITHM=COPY")
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT * FROM t"),
        vec![vec![Value::Int(1), Value::Int(7)]]
    );
}

#[test]
fn primary_key_nullability_survives_modify_change_and_copy() {
    for partition in ["", " PARTITION BY HASH(id) PARTITIONS 2"] {
        for alter in [
            "MODIFY id INT NULL",
            "MODIFY id INT",
            "MODIFY id BIGINT NULL, ALGORITHM=COPY",
            "CHANGE id renamed BIGINT NULL",
            "ADD extra INT, MODIFY id BIGINT NULL",
            "CHANGE id renamed BIGINT NULL, ALGORITHM=COPY, LOCK=NONE",
        ] {
            let dir = fixture();
            let prep = crate::platform::fs::FixtureSyncBatch::new(dir.path()).unwrap();
            let engine = open_strict_test_engine(dir.path());
            engine.execute(&format!("CREATE TABLE t(id INT NOT NULL,k VARCHAR(10) NOT NULL,PRIMARY KEY(id,k)){partition}")).unwrap();
            engine.execute("INSERT INTO t VALUES(1,'a')").unwrap();
            prep.finish().unwrap();
            engine.execute(&format!("ALTER TABLE t {alter}")).unwrap();
            let name = if alter.contains("CHANGE") {
                "renamed"
            } else {
                "id"
            };
            let verify = |engine: &StorageQueryEngine| {
                let table = engine.database().table("test.t").unwrap();
                assert!(
                    !table.columns[table.column_index(name).unwrap()].nullable,
                    "{alter}{partition}"
                );
                assert_eq!(
                    engine
                        .execute(&format!("INSERT INTO t({name},k) VALUES(NULL,'b')"))
                        .unwrap_err()
                        .code(),
                    1048
                );
                assert_eq!(
                    rows(engine, &format!("SELECT {name},k FROM t")),
                    vec![vec![Value::Int(1), Value::String("a".into())]]
                );
            };
            verify(&engine);
            if !partition.is_empty() {
                engine
                    .execute("ALTER TABLE t REBUILD PARTITION p0")
                    .unwrap();
            }
            drop(engine);
            let engine = open_strict_test_engine(dir.path());
            verify(&engine);
        }
    }
}

#[test]
fn string_primary_key_is_non_null_until_primary_key_is_dropped() {
    let dir = fixture();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(k VARCHAR(10) PRIMARY KEY,v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES('a',1)").unwrap();
    engine
        .execute("ALTER TABLE t MODIFY k VARCHAR(20) NULL")
        .unwrap();
    assert_eq!(
        engine
            .execute("INSERT INTO t VALUES(NULL,2)")
            .unwrap_err()
            .code(),
        1048
    );
    engine
        .execute("ALTER TABLE t DROP PRIMARY KEY, MODIFY k VARCHAR(20) NULL")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(NULL,2)").unwrap();
    assert_eq!(
        rows(&engine, "SELECT COUNT(*) FROM t WHERE k IS NULL"),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn temporary_primary_keys_remain_non_null_after_modify_and_change() {
    let dir = fixture();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TEMPORARY TABLE t(id INT PRIMARY KEY,v INT)",
        "INSERT INTO t VALUES(1,2)",
        "ALTER TABLE t MODIFY id BIGINT NULL",
        "ALTER TABLE t CHANGE id renamed BIGINT NULL",
    ] {
        exec(&engine, &mut session, sql).unwrap();
    }
    assert_eq!(
        exec(&engine, &mut session, "INSERT INTO t VALUES(NULL,3)")
            .unwrap_err()
            .code(),
        1048
    );
    let QueryResponse::ResultSet(result) = exec(&engine, &mut session, "SELECT * FROM t").unwrap()
    else {
        panic!("expected rows");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1), Value::Int(2)]]);
}

#[test]
fn drop_check_never_removes_foreign_key() {
    let dir = fixture();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE p(id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY,pid INT,v INT,CONSTRAINT fk FOREIGN KEY(pid) REFERENCES p(id),CONSTRAINT ck CHECK(v>0))").unwrap();
    for sql in [
        "ALTER TABLE t DROP CHECK fk",
        "ALTER TABLE t DROP CHECK ck, DROP CHECK fk",
    ] {
        assert!(engine.execute(sql).is_err(), "{sql}");
        assert_eq!(
            engine
                .database()
                .table("test.t")
                .unwrap()
                .check_constraints
                .len(),
            1
        );
        assert_eq!(
            engine
                .execute("INSERT INTO t VALUES(1,999,1)")
                .unwrap_err()
                .code(),
            1452
        );
    }
    engine
        .execute("ALTER TABLE t DROP CHECK IF EXISTS fk")
        .unwrap();
    assert!(engine
        .database()
        .drop_check_constraint("test.t", "fk", false, false)
        .is_err());
    engine
        .database()
        .drop_check_constraint("test.t", "fk", true, false)
        .unwrap();
    assert_eq!(
        engine
            .database()
            .table("test.t")
            .unwrap()
            .foreign_keys
            .len(),
        1
    );
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(
        engine
            .execute("INSERT INTO t VALUES(1,999,1)")
            .unwrap_err()
            .code(),
        1452
    );
    engine.execute("ALTER TABLE t DROP CONSTRAINT fk").unwrap();
    engine.execute("INSERT INTO t VALUES(1,999,1)").unwrap();
    engine.execute("ALTER TABLE t DROP CONSTRAINT ck").unwrap();
    engine.execute("INSERT INTO t VALUES(2,999,-1)").unwrap();
}

#[test]
fn exchange_checks_definitions_in_both_directions_even_without_validation() {
    for checks in ["", ",CHECK(k>=1)", ",CHECK(k>=0) NOT ENFORCED"] {
        for validation in ["", " WITHOUT VALIDATION"] {
            for reverse in [false, true] {
                let dir = fixture();
                let prep = crate::platform::fs::FixtureSyncBatch::new(dir.path()).unwrap();
                let engine = open_strict_test_engine(dir.path());
                let (left, right) = if reverse {
                    (checks, ",CHECK(k>=0)")
                } else {
                    (",CHECK(k>=0)", checks)
                };
                engine.execute(&format!("CREATE TABLE p(id INT NOT NULL,k INT NOT NULL,PRIMARY KEY(id,k){left}) PARTITION BY RANGE(k)(PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)")).unwrap();
                engine
                    .execute(&format!(
                        "CREATE TABLE t(id INT NOT NULL,k INT NOT NULL,PRIMARY KEY(id,k){right})"
                    ))
                    .unwrap();
                engine.execute("INSERT INTO p VALUES(1,2)").unwrap();
                engine.execute("INSERT INTO t VALUES(2,3)").unwrap();
                prep.finish().unwrap();
                assert_eq!(
                    engine
                        .execute(&format!(
                            "ALTER TABLE p EXCHANGE PARTITION p0 WITH TABLE t{validation}"
                        ))
                        .unwrap_err()
                        .code(),
                    1736
                );
                drop(engine);
                let engine = open_strict_test_engine(dir.path());
                assert_eq!(
                    rows(&engine, "SELECT id,k FROM p"),
                    vec![vec![Value::Int(1), Value::Int(2)]]
                );
                assert_eq!(
                    rows(&engine, "SELECT id,k FROM t"),
                    vec![vec![Value::Int(2), Value::Int(3)]]
                );
            }
        }
    }
}

#[test]
fn exchange_accepts_equal_checks_with_different_names_and_order() {
    let dir = fixture();
    let engine = open_strict_test_engine(dir.path());
    engine.execute("CREATE TABLE p(id INT NOT NULL,k INT NOT NULL,PRIMARY KEY(id,k),CONSTRAINT c1 CHECK(k>=0),CONSTRAINT c2 CHECK(id>0)) PARTITION BY RANGE(k)(PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)").unwrap();
    engine.execute("CREATE TABLE t(id INT NOT NULL,k INT NOT NULL,PRIMARY KEY(id,k),CONSTRAINT c3 CHECK(id>0),CONSTRAINT c4 CHECK(k>=0))").unwrap();
    engine.execute("INSERT INTO p VALUES(1,2)").unwrap();
    engine.execute("INSERT INTO t VALUES(2,3)").unwrap();
    engine
        .execute("ALTER TABLE p EXCHANGE PARTITION p0 WITH TABLE t")
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT id,k FROM p"),
        vec![vec![Value::Int(2), Value::Int(3)]]
    );
    assert_eq!(
        engine
            .execute("INSERT INTO p VALUES(3,-1)")
            .unwrap_err()
            .code(),
        3819
    );
}

#[test]
fn modify_change_inline_check_copy_publishes_atomically() {
    for lock in ["DEFAULT", "SHARED", "EXCLUSIVE", "NONE"] {
        for change in [false, true] {
            for invalid in [false, true] {
                let dir = fixture();
                let prep = crate::platform::fs::FixtureSyncBatch::new(dir.path()).unwrap();
                let engine = open_strict_test_engine(dir.path());
                engine
                    .execute("CREATE TABLE t(id INT PRIMARY KEY,a INT,KEY ai(a))")
                    .unwrap();
                let value = if invalid { -5 } else { 5 };
                engine
                    .execute(&format!("INSERT INTO t VALUES(1,{value})"))
                    .unwrap();
                prep.finish().unwrap();
                let (action, column) = if change {
                    ("CHANGE a b BIGINT CHECK(b>0)", "b")
                } else {
                    ("MODIFY a BIGINT CHECK(a>0)", "a")
                };
                let result = engine.execute(&format!(
                    "ALTER TABLE t {action},ALGORITHM=COPY,LOCK={lock}"
                ));
                if invalid {
                    assert_eq!(result.unwrap_err().code(), 3819);
                    let schema = engine.database().table("test.t").unwrap();
                    assert_eq!(schema.columns[1].name, "a");
                    assert_eq!(
                        schema.columns[1].data_type,
                        crate::storage::DataTypeKind::Int
                    );
                    assert!(schema.check_constraints.is_empty());
                } else {
                    result.unwrap();
                    assert_eq!(
                        engine
                            .execute(&format!("INSERT INTO t(id,{column}) VALUES(2,-1)"))
                            .unwrap_err()
                            .code(),
                        3819
                    );
                }
                let column = if invalid { "a" } else { column };
                assert_eq!(
                    rows(
                        &engine,
                        &format!(
                            "SELECT id,{column} FROM t FORCE INDEX(ai) WHERE {column}={value}"
                        )
                    ),
                    vec![vec![Value::Int(1), Value::Int(value)]]
                );
                drop(engine);
                let engine = open_strict_test_engine(dir.path());
                assert_eq!(
                    rows(&engine, &format!("SELECT id,{column} FROM t")),
                    vec![vec![Value::Int(1), Value::Int(value)]]
                );
            }
        }
    }
}

#[test]
fn inline_check_online_copy_validates_concurrent_deltas_before_publish() {
    for partition in ["", " PARTITION BY HASH(id) PARTITIONS 2"] {
        for invalid in [false, true] {
            let dir = fixture();
            let engine = std::sync::Arc::new(open_relaxed_test_engine(dir.path()));
            engine
                .execute(&format!(
                    "CREATE TABLE t(id INT PRIMARY KEY,a INT){partition}"
                ))
                .unwrap();
            engine.execute("INSERT INTO t VALUES(1,1)").unwrap();
            let hook = engine.database().install_ddl_backfill_test_hook();
            let worker = engine.clone();
            let ddl = std::thread::spawn(move || {
                let mut session = SessionContext::default();
                session.connection_id = 15201;
                exec(
                    &worker,
                    &mut session,
                    "ALTER TABLE t MODIFY a BIGINT CHECK(a>0),LOCK=NONE",
                )
            });
            let reached = hook.wait_until_reached(Duration::from_secs(5));
            let mut writer = SessionContext::default();
            writer.connection_id = 15202;
            let value = if invalid { -1 } else { 2 };
            let update = exec(
                &engine,
                &mut writer,
                &format!("UPDATE t SET a={value} WHERE id=1"),
            );
            hook.release();
            let result = ddl.join().unwrap();
            assert!(reached, "COPY never reached backfill: {result:?}");
            update.unwrap();
            if invalid {
                assert_eq!(result.unwrap_err().code(), 3819);
                assert!(engine
                    .database()
                    .table("test.t")
                    .unwrap()
                    .check_constraints
                    .is_empty());
            } else {
                result.unwrap();
                assert_eq!(
                    engine
                        .execute("INSERT INTO t VALUES(2,-1)")
                        .unwrap_err()
                        .code(),
                    3819
                );
            }
            assert_eq!(
                rows(&engine, "SELECT a FROM t"),
                vec![vec![Value::Int(value)]]
            );
        }
    }
}
