use super::*;

fn fixture() -> tempfile::TempDir {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    tempfile::Builder::new()
        .prefix("ddl-1027-")
        .tempdir_in("/tmp/mydb")
        .unwrap()
}

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap().materialize().unwrap()
    else {
        panic!("expected rows: {sql}");
    };
    result.rows
}

#[test]
fn ddl_1027_partition_defaults_evaluate_each_new_column() {
    let dir = fixture();
    let engine = open_relaxed_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(a INT, b VARCHAR(20)) PARTITION BY HASH(a) PARTITIONS 2")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,'x'),(2,'y'),(3,'z')")
        .unwrap();
    engine.execute("ALTER TABLE t ADD c INT NOT NULL DEFAULT(a+1), ADD d VARCHAR(30) DEFAULT(CONCAT(b,'!'))").unwrap();
    assert_eq!(
        rows(&engine, "SELECT a,c,d FROM t ORDER BY a"),
        vec![
            vec![Value::Int(1), Value::Int(2), Value::String("x!".into())],
            vec![Value::Int(2), Value::Int(3), Value::String("y!".into())],
            vec![Value::Int(3), Value::Int(4), Value::String("z!".into())],
        ]
    );
}

#[test]
fn ddl_1027_online_defaults_replay_committed_rows() {
    let dir = fixture();
    let engine = Arc::new(open_relaxed_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(a INT PRIMARY KEY, b INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,10),(2,20)").unwrap();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 10271;
        worker.execute_with_session(
            &mut session,
            "ALTER TABLE t ADD c INT NOT NULL DEFAULT(b+1), LOCK=NONE",
        )
    });
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let mut writer = SessionContext::default();
    writer.connection_id = 10272;
    let write = engine.execute_with_session(&mut writer, "UPDATE t SET b=11 WHERE a=1");
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "{result:?}");
    write.unwrap();
    result.unwrap();
    assert_eq!(
        rows(&engine, "SELECT a,b,c FROM t ORDER BY a"),
        vec![
            vec![Value::Int(1), Value::Int(11), Value::Int(12)],
            vec![Value::Int(2), Value::Int(20), Value::Int(21)],
        ]
    );
}

#[test]
fn ddl_1027_unnamed_index_does_not_take_primary_name() {
    let dir = fixture();
    let engine = open_relaxed_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT, `primary` INT, KEY(`primary`), KEY(`primary`))")
        .unwrap();
    let table = engine.database().table("test.t").unwrap();
    assert_eq!(
        table
            .indexes
            .iter()
            .map(|i| i.name.as_str())
            .collect::<Vec<_>>(),
        vec!["primary_2", "primary_3"]
    );
    engine.execute("ALTER TABLE t ADD PRIMARY KEY(id)").unwrap();
}

#[test]
fn ddl_1027_rename_rewrites_default_dependencies_and_is_durable() {
    let dir = fixture();
    {
        let engine = open_strict_test_engine(dir.path());
        engine.execute("CREATE TABLE t(a INT, k INT PRIMARY KEY, c INT DEFAULT(a+1), d VARCHAR(20) DEFAULT(CONCAT('a',a)))").unwrap();
        engine
            .execute("ALTER TABLE t RENAME COLUMN a TO a2")
            .unwrap();
        engine
            .execute("ALTER TABLE t CHANGE COLUMN a2 a3 BIGINT")
            .unwrap();
        engine.execute("INSERT INTO t(a3,k) VALUES(7,1)").unwrap();
        assert_eq!(
            rows(&engine, "SELECT c,d FROM t"),
            vec![vec![Value::Int(8), Value::String("a7".into())]]
        );
    }
    let engine = open_strict_test_engine(dir.path());
    engine.execute("INSERT INTO t(a3,k) VALUES(8,2)").unwrap();
    assert_eq!(
        rows(&engine, "SELECT c FROM t ORDER BY k"),
        vec![vec![Value::Int(8)], vec![Value::Int(9)]]
    );
}

#[test]
fn ddl_1027_add_auto_increment_allocates_existing_rows_and_continues() {
    for suffix in ["", ", ALGORITHM=COPY"] {
        let dir = fixture();
        let engine = open_relaxed_test_engine(dir.path());
        engine.execute("CREATE TABLE t(b INT)").unwrap();
        engine
            .execute("INSERT INTO t VALUES(10),(20),(30)")
            .unwrap();
        engine
            .execute(&format!(
                "ALTER TABLE t ADD id BIGINT UNSIGNED AUTO_INCREMENT, ADD PRIMARY KEY(id){suffix}"
            ))
            .unwrap();
        assert_eq!(
            rows(&engine, "SELECT id FROM t ORDER BY id"),
            vec![
                vec![Value::UInt(1)],
                vec![Value::UInt(2)],
                vec![Value::UInt(3)]
            ]
        );
        engine.execute("INSERT INTO t(b) VALUES(40)").unwrap();
        assert_eq!(
            rows(&engine, "SELECT id FROM t WHERE b=40"),
            vec![vec![Value::UInt(4)]]
        );
    }
}

#[test]
fn ddl_1027_auto_increment_partition_shared_step_and_restart() {
    for partitioned in [false, true] {
        let dir = fixture();
        {
            let engine = open_strict_test_engine(dir.path());
            let partition = if partitioned {
                " PARTITION BY HASH(b) PARTITIONS 2"
            } else {
                ""
            };
            engine
                .execute(&format!("CREATE TABLE t(b INT){partition}"))
                .unwrap();
            engine
                .execute("INSERT INTO t VALUES(10),(20),(30)")
                .unwrap();
            let mut session = SessionContext::default();
            engine
                .execute_with_session(
                    &mut session,
                    "SET auto_increment_increment=3, auto_increment_offset=2",
                )
                .unwrap();
            engine.execute_with_session(&mut session, "ALTER TABLE t ADD id INT AUTO_INCREMENT, ADD c INT DEFAULT(b+1), ADD PRIMARY KEY(id,b), LOCK=SHARED").unwrap();
            assert_eq!(
                rows(&engine, "SELECT id,c FROM t ORDER BY id"),
                vec![
                    vec![Value::Int(2), Value::Int(11)],
                    vec![Value::Int(5), Value::Int(21)],
                    vec![Value::Int(8), Value::Int(31)],
                ]
            );
        }
        let engine = open_strict_test_engine(dir.path());
        engine.execute("INSERT INTO t(b) VALUES(40)").unwrap();
        assert_eq!(
            rows(&engine, "SELECT id,c FROM t WHERE b=40"),
            vec![vec![Value::Int(9), Value::Int(41)]]
        );
    }
}

#[test]
fn ddl_1027_auto_increment_overflow_and_unsupported_online_are_atomic() {
    let dir = fixture();
    let engine = open_relaxed_test_engine(dir.path());
    engine.execute("CREATE TABLE t(b INT)").unwrap();
    let input = (1..=128)
        .map(|n| format!("({n})"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO t VALUES{input}"))
        .unwrap();
    assert_eq!(
        engine
            .execute("ALTER TABLE t ADD id INT AUTO_INCREMENT, ADD PRIMARY KEY(id), LOCK=NONE")
            .unwrap_err()
            .code(),
        1846
    );
    assert!(engine
        .execute("ALTER TABLE t ADD id TINYINT AUTO_INCREMENT, ADD PRIMARY KEY(id)")
        .is_err());
    assert_eq!(
        engine
            .database()
            .table("test.t")
            .unwrap()
            .visible_column_count(),
        1
    );
    assert_eq!(
        rows(&engine, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::Int(128)]]
    );
    engine
        .execute("ALTER TABLE t ADD id INT UNSIGNED AUTO_INCREMENT, ADD PRIMARY KEY(id)")
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT COUNT(DISTINCT id),MIN(id),MAX(id) FROM t"),
        vec![vec![Value::Int(128), Value::UInt(1), Value::UInt(128)]]
    );
}

#[test]
fn ddl_1027_null_validation_reports_data_error_when_budget_is_small() {
    let dir = fixture();
    let engine = open_relaxed_test_engine(dir.path());
    engine.execute("CREATE TABLE t(a INT)").unwrap();
    engine
        .execute("INSERT INTO t VALUES(NULL),(NULL),(NULL),(NULL)")
        .unwrap();
    let result = engine
        .database()
        .with_constraint_validation_limit_for_test(128, || {
            engine.execute("ALTER TABLE t MODIFY a INT NOT NULL, LOCK=NONE")
        });
    assert_eq!(result.unwrap_err().code(), 1048);
    assert!(engine.database().table("test.t").unwrap().columns[0].nullable);
    assert_eq!(
        rows(&engine, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::Int(4)]]
    );
}

#[test]
fn ddl_1027_drop_removes_empty_object_directories() {
    let dir = fixture();
    let engine = open_strict_test_engine(dir.path());
    engine.execute("CREATE TABLE t(a INT PRIMARY KEY)").unwrap();
    engine.execute("INSERT INTO t VALUES(1)").unwrap();
    let id = engine.database().table("test.t").unwrap().id;
    engine.execute("DROP TABLE t").unwrap();
    assert!(!dir
        .path()
        .join("objects/tables")
        .join(id.to_string())
        .exists());
}

#[test]
fn ddl_1027_copy_and_drop_remove_empty_shadow_directories() {
    for lock in ["EXCLUSIVE", "SHARED", "NONE"] {
        let dir = fixture();
        let engine = open_relaxed_test_engine(dir.path());
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT)")
            .unwrap();
        engine.execute("INSERT INTO t VALUES(1,1000)").unwrap();
        let id = engine.database().table("test.t").unwrap().id.to_string();
        engine
            .execute(&format!("ALTER TABLE t MODIFY v BIGINT,LOCK={lock}"))
            .unwrap();
        let root = dir.path().join("objects/tables");
        let objects = || {
            std::fs::read_dir(&root)
                .unwrap()
                .map(|p| p.unwrap().file_name().to_string_lossy().into_owned())
                .collect::<Vec<_>>()
        };
        assert_eq!(objects(), vec![id.clone()]);
        assert!(engine
            .execute(&format!("ALTER TABLE t MODIFY v TINYINT,LOCK={lock}"))
            .is_err());
        assert_eq!(objects(), vec![id]);
        engine.execute("DROP TABLE t").unwrap();
        assert!(objects().is_empty());
        assert!(std::fs::read_dir(dir.path().join("objects/indexes"))
            .unwrap()
            .next()
            .is_none());
        drop(engine);
        let _reopened = open_strict_test_engine(dir.path());
        assert!(
            objects().is_empty(),
            "recovery resurrected retired COPY files"
        );
    }
}

#[test]
fn ddl_1027_create_losing_drop_race_cleans_files_after_restart() {
    for (cleanup_failure, replacement, partitioned) in [
        (false, false, false),
        (true, false, false),
        (false, true, false),
        (true, true, false),
        (true, true, true),
    ] {
        let dir = fixture();
        let storage_ids;
        {
            let engine = Arc::new(open_strict_test_engine(dir.path()));
            let hook = engine.database().install_create_finalize_test_hook();
            let worker = engine.clone();
            let create = std::thread::spawn(move || {
                if cleanup_failure {
                    worker.database().fail_next_ddl_cleanup_for_test();
                }
                let partition = if partitioned {
                    " PARTITION BY HASH(a) PARTITIONS 2"
                } else {
                    ""
                };
                worker.execute(&format!("CREATE TABLE t(a INT PRIMARY KEY){partition}"))
            });
            let reached = hook.wait_until_reached(Duration::from_secs(10));
            if !reached {
                hook.release();
            }
            assert!(reached);
            let table = engine.database().table("test.t").unwrap();
            storage_ids = table.storage_ids();
            let dropped = engine.database().drop_table("test.t");
            let replaced = if replacement {
                let crate::parser::Statement::CreateTable(def) =
                    crate::parser::parse_statement("CREATE TABLE t(a INT PRIMARY KEY)").unwrap()
                else {
                    panic!("CREATE expected")
                };
                engine
                    .database()
                    .create_table("test.t", def.columns, def.primary_key, def.indexes)
            } else {
                Ok(())
            };
            hook.release();
            let result = create.join().unwrap();
            dropped.unwrap();
            replaced.unwrap();
            assert!(
                result.is_err(),
                "CREATE must notice the changed catalog identity"
            );
            for id in &storage_ids {
                let path = dir
                    .path()
                    .join("objects/tables")
                    .join(id.to_string())
                    .join("1.heap");
                assert_eq!(
                    path.exists(),
                    cleanup_failure,
                    "failed CREATE cleanup: {path:?}"
                );
            }
            if cleanup_failure {
                assert!(dir
                    .path()
                    .join("ddl/drop_table")
                    .join(format!("{}.json", table.id))
                    .exists());
            }
            if replacement {
                engine.execute("INSERT INTO t VALUES(91)").unwrap();
            }
        }
        let engine = open_strict_test_engine(dir.path());
        if replacement {
            assert_eq!(rows(&engine, "SELECT a FROM t"), vec![vec![Value::Int(91)]]);
        } else {
            assert!(engine.database().table("test.t").is_err());
        }
        for id in storage_ids {
            assert!(!dir
                .path()
                .join("objects/tables")
                .join(id.to_string())
                .exists());
        }
    }
}

#[test]
fn ddl_1027_default_cannot_reference_pending_auto_increment_values() {
    let dir = fixture();
    let engine = open_relaxed_test_engine(dir.path());
    assert_eq!(
        engine
            .execute("CREATE TABLE bad(id INT AUTO_INCREMENT PRIMARY KEY,c INT DEFAULT(id+1))")
            .unwrap_err()
            .code(),
        3768
    );
    engine.execute("CREATE TABLE t(b INT)").unwrap();
    engine.execute("INSERT INTO t VALUES(10)").unwrap();
    assert_eq!(engine.execute("ALTER TABLE t ADD id INT AUTO_INCREMENT,ADD c INT DEFAULT(id+1),ADD PRIMARY KEY(id)").unwrap_err().code(),3768);
    assert_eq!(
        engine
            .database()
            .table("test.t")
            .unwrap()
            .visible_column_count(),
        1
    );
    assert_eq!(rows(&engine, "SELECT b FROM t"), vec![vec![Value::Int(10)]]);
}
