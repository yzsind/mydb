//! Behavioral contracts for DDL binding, cancellation, and definition validation.
use super::*;

fn fixture() -> tempfile::TempDir {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    tempfile::tempdir_in("/tmp/mydb").unwrap()
}

#[test]
fn alter_rename_unqualified_target_inherits_source_database() {
    let dir = fixture();
    let engine = open_relaxed_test_engine(dir.path());
    engine.execute("CREATE DATABASE source_db").unwrap();
    engine
        .execute("CREATE TABLE source_db.t(id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO source_db.t VALUES(7)").unwrap();
    engine
        .execute("ALTER TABLE source_db.t RENAME TO renamed")
        .unwrap();
    assert!(engine.database().table("source_db.renamed").is_ok());
    assert!(engine.database().table("test.renamed").is_err());
    engine
        .execute("CREATE TABLE source_db.occupied(id INT)")
        .unwrap();
    assert_eq!(
        engine
            .execute("ALTER TABLE source_db.renamed RENAME TO occupied")
            .unwrap_err()
            .code(),
        1050
    );
    engine
        .execute("ALTER TABLE source_db.renamed RENAME TO test.moved")
        .unwrap();
    let table = engine.database().table("test.moved").unwrap();
    assert_eq!(table.database, "test");
}

#[test]
fn alter_rename_authorizes_the_same_bound_database_as_execution() {
    let dir = fixture();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE DATABASE source_db",
        "CREATE TABLE source_db.t(id INT)",
        "CREATE USER 'renamer'@'%' IDENTIFIED BY 'pw'",
        "GRANT ALTER,DROP,CREATE,INSERT ON source_db.* TO 'renamer'@'%'",
    ] {
        engine.execute(sql).unwrap();
    }
    let mut user = SessionContext::default();
    user.user = "renamer".into();
    user.login_user = "renamer".into();
    user.authenticated_as = crate::engine::auth::AccountKey::new("renamer", "%");
    user.auth_epoch = engine.database().auth_catalog().epoch;
    engine
        .execute_with_session(&mut user, "ALTER TABLE source_db.t RENAME TO renamed")
        .unwrap();
    assert!(engine
        .execute_with_session(
            &mut user,
            "ALTER TABLE source_db.renamed RENAME TO test.forbidden"
        )
        .is_err());
    assert!(engine.database().table("source_db.renamed").is_ok());
}

#[test]
fn temporary_rename_uses_source_schema_and_failed_rename_keeps_binding() {
    let dir = fixture();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE DATABASE source_db",
        "CREATE TEMPORARY TABLE source_db.t(id INT)",
        "INSERT INTO source_db.t VALUES(7)",
        "ALTER TABLE source_db.t RENAME TO renamed",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    let QueryResponse::ResultSet(rows) = engine
        .execute_with_session(&mut session, "SELECT * FROM source_db.renamed")
        .unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(7)]]);
    assert!(engine
        .execute_with_session(
            &mut session,
            "ALTER TABLE source_db.renamed RENAME TO information_schema.forbidden"
        )
        .is_err());
    engine
        .execute_with_session(&mut session, "INSERT INTO source_db.renamed VALUES(8)")
        .unwrap();
}

#[test]
fn online_constraint_barrier_uses_session_metadata_timeout() {
    let dir = fixture();
    let mut config = memory_test_storage_config();
    config.lock_wait = Duration::from_millis(40);
    let engine =
        StorageQueryEngine::open_with_storage_config_without_background_workers(dir.path(), config)
            .unwrap();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,1)").unwrap();
    let mut writer = SessionContext::default();
    writer.connection_id = 19001;
    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut writer, "UPDATE t SET v=2 WHERE id=1")
        .unwrap();
    let mut ddl = SessionContext::default();
    ddl.connection_id = 19002;
    ddl.sysvars.lock_wait_timeout = Some(1);
    let started = Instant::now();
    let result = engine.execute_with_session(&mut ddl, "ALTER TABLE t ADD CHECK(v>0),LOCK=NONE");
    let elapsed = started.elapsed();
    engine
        .execute_with_session(&mut writer, "ROLLBACK")
        .unwrap();
    assert_eq!(result.unwrap_err().code(), 1205);
    assert!(
        elapsed >= Duration::from_millis(850),
        "session timeout was replaced by storage timeout: {elapsed:?}"
    );
    assert!(engine
        .database()
        .metadata_lock_snapshot()
        .iter()
        .all(|row| row.owner_thread_id != Some(19002)));
    engine
        .execute("ALTER TABLE t ADD CHECK(v>0),LOCK=NONE")
        .unwrap();
}

fn cancel_copy_at_publish(wait_for_reader: bool) {
    let dir = fixture();
    let engine = Arc::new(open_relaxed_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT,KEY vi(v))")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,10),(2,20)").unwrap();
    let hook = engine.database().install_online_copy_publish_test_hook();
    let worker = engine.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 19102;
        let result = worker.execute_with_session(
            &mut session,
            "ALTER TABLE t MODIFY v BIGINT,ALGORITHM=COPY,LOCK=NONE",
        );
        tx.send(result.map(|_| ())).unwrap();
    });
    assert!(hook.wait_until_reached(Duration::from_secs(5)));
    let job = engine
        .database()
        .alter_copy_job_snapshots()
        .into_iter()
        .find(|job| job.table == "t" && job.state == "CATCH_UP")
        .unwrap();
    let mut reader = SessionContext::default();
    reader.connection_id = 19103;
    if wait_for_reader {
        engine.execute_with_session(&mut reader, "BEGIN").unwrap();
        engine
            .execute_with_session(&mut reader, "SELECT * FROM t")
            .unwrap()
            .materialize()
            .unwrap();
        hook.release();
        let deadline = Instant::now() + Duration::from_secs(3);
        while !engine
            .database()
            .metadata_lock_snapshot()
            .iter()
            .any(|row| row.owner_thread_id == Some(19102) && row.lock_status == "PENDING")
        {
            assert!(
                Instant::now() < deadline,
                "DDL did not enter the publication wait"
            );
            std::thread::yield_now();
        }
    }
    engine.database().cancel_ddl_job(job.job_id).unwrap();
    if !wait_for_reader {
        hook.release();
    }
    let early = rx.recv_timeout(Duration::from_millis(500));
    let completed_while_blocked = early.is_ok();
    if wait_for_reader {
        engine
            .execute_with_session(&mut reader, "ROLLBACK")
            .unwrap();
    }
    let result = early.unwrap_or_else(|_| rx.recv_timeout(Duration::from_secs(5)).unwrap());
    ddl.join().unwrap();
    assert_eq!(
        result
            .expect_err("acknowledged cancellation must prevent publication")
            .code(),
        1317
    );
    assert!(
        completed_while_blocked,
        "job cancellation did not wake publication wait"
    );
    let table = engine.database().table("test.t").unwrap();
    assert_eq!(
        table.columns[1].data_type,
        crate::storage::DataTypeKind::Int
    );
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT v FROM t FORCE INDEX(vi) WHERE v>=0 ORDER BY id")
        .unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(10)], vec![Value::Int(20)]]);
}

#[test]
fn copy_empty_tail_cancellation_prevents_commit() {
    cancel_copy_at_publish(false);
}

#[test]
fn copy_job_cancellation_wakes_publication_wait() {
    cancel_copy_at_publish(true);
}

#[test]
fn alter_and_rename_reject_overlong_identifiers_before_mutation() {
    let dir = fixture();
    let engine = open_relaxed_test_engine(dir.path());
    let name = "x".repeat(65);
    for temporary in [false, true] {
        let prefix = if temporary { "TEMPORARY " } else { "" };
        let table = if temporary { "tmp" } else { "t" };
        let mut session = SessionContext::default();
        session.connection_id = 19201;
        engine
            .execute_with_session(
                &mut session,
                &format!("CREATE {prefix}TABLE {table}(id INT,v INT)"),
            )
            .unwrap();
        for action in [
            format!("ADD `{name}` INT"),
            format!("CHANGE v `{name}` INT"),
            format!("RENAME COLUMN v TO `{name}`"),
            format!("RENAME TO `{name}`"),
        ] {
            assert_eq!(
                engine
                    .execute_with_session(&mut session, &format!("ALTER TABLE {table} {action}"))
                    .unwrap_err()
                    .code(),
                1059,
                "{prefix}{action}"
            );
        }
    }
    assert_eq!(
        engine
            .execute(&format!("RENAME TABLE t TO `{name}`"))
            .unwrap_err()
            .code(),
        1059
    );
    engine.execute("INSERT INTO t VALUES(1,2)").unwrap();
}

#[test]
fn alter_column_limit_counts_visible_columns_without_publishing_invalid_schema() {
    let dir = fixture();
    let engine = open_relaxed_test_engine(dir.path());
    let columns = (0..1017)
        .map(|i| format!("c{i} TINYINT"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("CREATE TABLE wide({columns})"))
        .unwrap();
    assert_eq!(
        engine
            .execute("ALTER TABLE wide ADD extra TINYINT")
            .unwrap_err()
            .code(),
        1117
    );
    engine
        .execute("ALTER TABLE wide DROP c0, ADD extra TINYINT")
        .unwrap();
    engine.execute("CREATE TABLE copied LIKE wide").unwrap();
    assert_eq!(
        engine
            .database()
            .table("test.wide")
            .unwrap()
            .visible_column_count(),
        1017
    );
}

#[test]
fn compound_alter_binds_new_columns_and_functional_indexes_to_candidate() {
    let dir = fixture();
    let engine = open_relaxed_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1),(2)").unwrap();
    engine.execute("ALTER TABLE t ADD c VARCHAR(20) DEFAULT '7',MODIFY c BIGINT,ADD INDEX ci((c+1)),LOCK=NONE").unwrap();
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT id,c FROM t WHERE c+1=8 ORDER BY id")
        .unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Int(1), Value::Int(7)],
            vec![Value::Int(2), Value::Int(7)]
        ]
    );
    engine.execute("ALTER TABLE t ADD extra INT DEFAULT 3,CHANGE extra renamed BIGINT,ADD INDEX ri(renamed),LOCK=NONE").unwrap();
    assert!(engine
        .database()
        .table("test.t")
        .unwrap()
        .column_index("renamed")
        .is_ok());
}

#[test]
fn failing_candidate_conversion_does_not_publish_any_added_column() {
    let dir = fixture();
    let engine = open_relaxed_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1)").unwrap();
    assert!(engine
        .execute("ALTER TABLE t ADD c VARCHAR(20) DEFAULT 'bad',MODIFY c INT,ADD INDEX ci(c)")
        .is_err());
    assert_eq!(
        engine
            .database()
            .table("test.t")
            .unwrap()
            .visible_column_count(),
        1
    );
    engine.execute("INSERT INTO t VALUES(2)").unwrap();
    engine.shutdown().unwrap();
}

#[test]
fn index_retirement_preparation_failure_preserves_definition_and_files() {
    let dir = fixture();
    {
        let engine = open_strict_test_engine(dir.path());
        assert_eq!(
            engine
                .execute("SELECT * FROM mysql.mydb_ddl_retirements")
                .unwrap_err()
                .code(),
            3554
        );
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT,KEY vi(v))")
            .unwrap();
        engine.execute("INSERT INTO t VALUES(1,10)").unwrap();
        engine.database().fail_next_catalog_commit_for_test();
        assert!(engine.execute("DROP INDEX vi ON t").is_err());
        assert_eq!(engine.database().catalog_retirement_count_for_test(), 0);
        engine.execute("UPDATE t SET v=20 WHERE id=1").unwrap();
        engine.shutdown().unwrap();
    }
    let engine = open_strict_test_engine(dir.path());
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT id FROM t FORCE INDEX(vi) WHERE v=20")
        .unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(1)]]);
    engine.shutdown().unwrap();
}

#[test]
fn relaxed_index_retirement_waits_for_background_durability_and_retries() {
    let dir = fixture();
    {
        let engine = open_relaxed_test_engine(dir.path());
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT,KEY vi(v))")
            .unwrap();
        engine.execute("INSERT INTO t VALUES(1,10)").unwrap();
        engine.execute("DROP INDEX vi ON t").unwrap();
        assert_eq!(engine.database().catalog_retirement_count_for_test(), 1);
        engine.database().fail_next_ddl_cleanup_for_test();
        assert_eq!(
            engine
                .database()
                .maintain_catalog_retirements_until(|| false)
                .unwrap(),
            0
        );
        assert_eq!(engine.database().catalog_retirement_count_for_test(), 1);
        // The cursor wraps after a failed entry, then retries it.
        for _ in 0..2 {
            engine
                .database()
                .maintain_catalog_retirements_until(|| false)
                .unwrap();
        }
        assert_eq!(engine.database().catalog_retirement_count_for_test(), 0);
        engine.execute("CREATE INDEX vi ON t(v)").unwrap();
        engine.shutdown().unwrap();
    }
    let engine = open_relaxed_test_engine(dir.path());
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT id FROM t FORCE INDEX(vi) WHERE v=10")
        .unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(1)]]);
    engine.shutdown().unwrap();
}

#[test]
fn retired_index_cleanup_survives_failure_reopen_and_same_name_recreation() {
    let dir = fixture();
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT,KEY vi(v))")
            .unwrap();
        engine.execute("INSERT INTO t VALUES(1,10),(2,20)").unwrap();
        engine.database().fail_next_ddl_cleanup_for_test();
        engine.execute("DROP INDEX vi ON t").unwrap();
        let pending = engine.database().catalog_retirement_count_for_test();
        assert_eq!(pending, 1);
        engine.execute("CREATE INDEX vi ON t(v)").unwrap();
        engine.shutdown().unwrap();
    }
    for iteration in 0..2 {
        let engine = open_strict_test_engine(dir.path());
        let QueryResponse::ResultSet(rows) = engine
            .execute("SELECT id,v FROM t FORCE INDEX(vi) WHERE v>=0 ORDER BY id")
            .unwrap()
        else {
            panic!("rows")
        };
        let mut expected = vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)],
        ];
        if iteration != 0 {
            expected.push(vec![Value::Int(3), Value::Int(30)]);
        }
        assert_eq!(rows.rows, expected);
        engine
            .execute("INSERT INTO t VALUES(3,30) ON DUPLICATE KEY UPDATE v=30")
            .unwrap();
        engine.shutdown().unwrap();
    }
}

#[test]
fn unpublished_partial_drop_intents_do_not_affect_live_tables() {
    let dir = fixture();
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY)")
            .unwrap();
        engine.execute("INSERT INTO t VALUES(7)").unwrap();
        engine.shutdown().unwrap();
    }
    let intents = dir.path().join("ddl/drop_table");
    std::fs::create_dir_all(&intents).unwrap();
    std::fs::write(intents.join("1.json.tmp"), b"{\"table_id\":").unwrap();
    let engine = open_strict_test_engine(dir.path());
    let QueryResponse::ResultSet(rows) = engine.execute("SELECT * FROM t").unwrap() else {
        panic!("rows")
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(7)]]);
    engine.execute("DROP TABLE t").unwrap();
    engine.shutdown().unwrap();
}

#[test]
fn retained_partition_counts_follow_committed_replay_and_cross_leaf_moves() {
    let dir = fixture();
    {
        let engine = Arc::new(open_strict_test_engine(dir.path()));
        engine.execute("CREATE TABLE t(id INT PRIMARY KEY,payload TEXT,KEY pi(payload(10))) PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)").unwrap();
        engine
            .execute("INSERT INTO t VALUES(1,'a'),(2,'b'),(11,REPEAT('c',9000)),(12,'d')")
            .unwrap();
        let hook = engine.database().install_ddl_backfill_test_hook();
        let worker = engine.clone();
        let ddl = std::thread::spawn(move || {
            worker.execute("ALTER TABLE t REBUILD PARTITION p0,LOCK=NONE")
        });
        assert!(hook.wait_until_reached(Duration::from_secs(5)));
        let mut writer = SessionContext::default();
        writer.connection_id = 19301;
        for sql in [
            "BEGIN",
            "INSERT INTO t VALUES(13,'new')",
            "DELETE FROM t WHERE id=11",
            "UPDATE t SET id=3 WHERE id=12",
            "SAVEPOINT s",
            "INSERT INTO t VALUES(14,'rolled back')",
            "ROLLBACK TO s",
            "COMMIT",
        ] {
            engine.execute_with_session(&mut writer, sql).unwrap();
        }
        hook.release();
        ddl.join().unwrap().unwrap();
        let table = engine.database().table("test.t").unwrap();
        assert_eq!(table.stats.row_count, 4);
        assert_eq!(
            table
                .stats
                .partition_stats
                .values()
                .map(|stats| stats.row_count)
                .sum::<u64>(),
            4
        );
        let QueryResponse::ResultSet(rows) =
            engine.execute("SELECT id FROM t ORDER BY id").unwrap()
        else {
            panic!("rows")
        };
        assert_eq!(
            rows.rows,
            vec![
                vec![Value::Int(1)],
                vec![Value::Int(2)],
                vec![Value::Int(3)],
                vec![Value::Int(13)]
            ]
        );
        engine.shutdown().unwrap();
    }
    let engine = open_strict_test_engine(dir.path());
    let table = engine.database().table("test.t").unwrap();
    assert_eq!(table.stats.row_count, 4);
    engine.shutdown().unwrap();
}

#[test]
fn rename_updates_only_related_fk_edges_with_cold_unrelated_children() {
    let dir = fixture();
    let engine = open_strict_test_engine(dir.path());
    for sql in ["CREATE TABLE p(id INT PRIMARY KEY)","CREATE TABLE c(id INT PRIMARY KEY,pid INT,FOREIGN KEY(pid) REFERENCES p(id) ON DELETE CASCADE)",
        "CREATE TABLE u(id INT PRIMARY KEY)","CREATE TABLE v(id INT PRIMARY KEY,uid INT,FOREIGN KEY(uid) REFERENCES u(id) ON DELETE CASCADE)",
        "INSERT INTO p VALUES(1)","INSERT INTO c VALUES(1,1)","INSERT INTO u VALUES(1)","INSERT INTO v VALUES(1,1)"] {
        engine.execute(sql).unwrap();
    }
    engine
        .database()
        .force_cold_online_descriptor_for_test("test.v");
    for sql in [
        "RENAME TABLE c TO renamed_c",
        "RENAME TABLE p TO renamed_p",
        "ALTER TABLE renamed_p RENAME COLUMN id TO renamed_id",
        "DELETE FROM renamed_p WHERE renamed_id=1",
        "DELETE FROM u WHERE id=1",
    ] {
        engine.execute(sql).unwrap();
    }
    for name in ["renamed_c", "v"] {
        let QueryResponse::ResultSet(rows) =
            engine.execute(&format!("SELECT * FROM {name}")).unwrap()
        else {
            panic!("rows")
        };
        assert!(rows.rows.is_empty(), "FK cascade lost after rename: {name}");
    }
    engine.shutdown().unwrap();
}

#[test]
fn online_index_job_is_visible_and_cancellable_during_build() {
    let dir = fixture();
    let engine = Arc::new(open_relaxed_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,10),(2,20)").unwrap();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || worker.execute("CREATE INDEX vi ON t(v) LOCK=NONE"));
    assert!(hook.wait_until_reached(Duration::from_secs(5)));
    let jobs = engine.database().alter_copy_job_snapshots();
    let job = jobs
        .iter()
        .find(|job| job.table == "t" && job.operation == "INDEX" && job.cancel_allowed)
        .unwrap();
    engine.database().cancel_ddl_job(job.job_id).unwrap();
    hook.release();
    assert_eq!(ddl.join().unwrap().unwrap_err().code(), 1317);
    engine.execute("INSERT INTO t VALUES(3,30)").unwrap();
    assert!(engine
        .database()
        .table("test.t")
        .unwrap()
        .indexes
        .iter()
        .all(|index| index.name != "vi"));
    assert!(engine
        .database()
        .alter_copy_job_snapshots()
        .iter()
        .any(|job| job.operation == "INDEX" && job.state == "CANCELLED"));
    engine.execute("CREATE INDEX vi ON t(v)").unwrap();
    engine.shutdown().unwrap();
}

#[test]
fn shared_copy_excludes_direct_storage_writers_during_snapshot_build() {
    let dir = fixture();
    let engine = Arc::new(open_relaxed_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,10),(2,20)").unwrap();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        worker.execute("ALTER TABLE t MODIFY v BIGINT,ALGORITHM=COPY,LOCK=SHARED")
    });
    assert!(hook.wait_until_reached(Duration::from_secs(5)));
    let txn = engine.database().begin_txn().unwrap();
    let result =
        engine
            .database()
            .insert_rows(txn, "test.t", vec![vec![Value::Int(3), Value::Int(30)]]);
    engine.database().rollback_txn(txn.txn_id).unwrap();
    hook.release();
    ddl.join().unwrap().unwrap();
    assert!(result.is_err(), "LOCK=SHARED admitted a raw storage writer");
    engine.execute("INSERT INTO t VALUES(3,30)").unwrap();
    engine.shutdown().unwrap();
}

#[test]
fn floating_md_changes_materialize_rounding_and_validate_range() {
    let dir = fixture();
    let engine = open_relaxed_test_engine(dir.path());
    for (ordinal, kind) in ["FLOAT", "DOUBLE"].iter().enumerate() {
        let name = format!("floating_{ordinal}");
        engine
            .execute(&format!("CREATE TABLE {name}(v {kind}(8,3),KEY vi(v))"))
            .unwrap();
        engine
            .execute(&format!("INSERT INTO {name} VALUES(1.234),(42.123)"))
            .unwrap();
        engine
            .execute(&format!(
                "ALTER TABLE {name} MODIFY v {kind}(8,2),LOCK=NONE"
            ))
            .unwrap();
        let QueryResponse::ResultSet(rows) = engine
            .execute(&format!(
                "SELECT COUNT(*) FROM {name} FORCE INDEX(vi) WHERE ABS(v+0-1.23)<0.000001"
            ))
            .unwrap()
        else {
            panic!("rows")
        };
        assert_eq!(rows.rows, vec![vec![Value::Int(1)]]);
        let generation = engine
            .database()
            .table(&format!("test.{name}"))
            .unwrap()
            .active_generation;
        engine
            .execute(&format!(
                "ALTER TABLE {name} MODIFY v {kind}(9,2),ALGORITHM=INSTANT"
            ))
            .unwrap();
        assert_eq!(
            engine
                .database()
                .table(&format!("test.{name}"))
                .unwrap()
                .active_generation,
            generation
        );
        assert_eq!(
            engine
                .execute(&format!(
                    "ALTER TABLE {name} MODIFY v {kind}(3,2),LOCK=NONE"
                ))
                .unwrap_err()
                .code(),
            1264
        );
        let QueryResponse::ResultSet(rows) = engine
            .execute(&format!(
                "SELECT COUNT(*) FROM {name} WHERE ABS(v+0-42.12)<0.00001"
            ))
            .unwrap()
        else {
            panic!("rows")
        };
        assert_eq!(rows.rows, vec![vec![Value::Int(1)]]);
    }
    engine.shutdown().unwrap();
}

#[test]
fn unsigned_numeric_domain_changes_validate_old_rows_and_preserve_sql_mode() {
    let dir = fixture();
    let engine = open_relaxed_test_engine(dir.path());
    for (ordinal, kind) in ["DECIMAL(8,2)", "FLOAT", "DOUBLE"].iter().enumerate() {
        let name = format!("numeric_{ordinal}");
        engine
            .execute(&format!("CREATE TABLE {name}(v {kind},KEY vi(v))"))
            .unwrap();
        engine
            .execute(&format!("INSERT INTO {name} VALUES(-1.25)"))
            .unwrap();
        assert_eq!(
            engine
                .execute(&format!(
                    "ALTER TABLE {name} MODIFY v {kind} UNSIGNED,LOCK=NONE"
                ))
                .unwrap_err()
                .code(),
            1264
        );
        assert!(
            !engine
                .database()
                .table(&format!("test.{name}"))
                .unwrap()
                .columns[0]
                .unsigned
        );
        engine
            .execute(&format!("UPDATE {name} SET v=1.25"))
            .unwrap();
        let generation = engine
            .database()
            .table(&format!("test.{name}"))
            .unwrap()
            .active_generation;
        engine
            .execute(&format!(
                "ALTER TABLE {name} MODIFY v {kind} UNSIGNED,LOCK=NONE"
            ))
            .unwrap();
        assert_eq!(
            engine
                .database()
                .table(&format!("test.{name}"))
                .unwrap()
                .active_generation,
            generation
        );
        assert_eq!(
            engine
                .execute(&format!("INSERT INTO {name} VALUES(-1.25)"))
                .unwrap_err()
                .code(),
            1264
        );
        engine
            .execute(&format!("ALTER TABLE {name} MODIFY v {kind} SIGNED"))
            .unwrap();
        engine
            .execute(&format!("UPDATE {name} SET v=-1.25"))
            .unwrap();
        let mut non_strict = SessionContext::default();
        engine
            .execute_with_session(&mut non_strict, "SET sql_mode=''")
            .unwrap();
        engine
            .execute_with_session(
                &mut non_strict,
                &format!("ALTER TABLE {name} MODIFY v {kind} UNSIGNED,ALGORITHM=COPY,LOCK=SHARED"),
            )
            .unwrap();
        let QueryResponse::ResultSet(rows) = engine
            .execute(&format!("SELECT v=0 FROM {name} FORCE INDEX(vi)"))
            .unwrap()
        else {
            panic!("rows")
        };
        assert_eq!(rows.rows, vec![vec![Value::Int(1)]]);
    }
    engine.shutdown().unwrap();
}

#[test]
fn online_column_validation_replays_writes_without_copying_unrelated_lobs() {
    let dir = fixture();
    let engine = Arc::new(open_relaxed_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT,s VARBINARY(80),payload LONGTEXT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,10,'a',REPEAT('x',100000)),(2,20,'b',REPEAT('y',100000))")
        .unwrap();
    let generation = engine.database().table("test.t").unwrap().active_generation;
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        worker.execute("ALTER TABLE t MODIFY v INT NOT NULL,MODIFY s VARBINARY(12),LOCK=NONE")
    });
    assert!(hook.wait_until_reached(Duration::from_secs(5)));
    let write = engine.execute("INSERT INTO t VALUES(3,30,'new',REPEAT('z',100000))");
    let update = engine.execute("UPDATE t SET v=11 WHERE id=1");
    hook.release();
    let result = ddl.join().unwrap();
    write.unwrap();
    update.unwrap();
    result.unwrap();
    let table = engine.database().table("test.t").unwrap();
    assert_eq!(table.active_generation, generation);
    assert!(!table.columns[1].nullable);
    assert_eq!(
        engine
            .execute("INSERT INTO t VALUES(4,NULL,'a','b')")
            .unwrap_err()
            .code(),
        1048
    );
    assert_eq!(
        engine
            .execute("INSERT INTO t VALUES(4,40,'a string that is too long','b')")
            .unwrap_err()
            .code(),
        1406
    );
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT id,v,LENGTH(payload) FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Int(1), Value::Int(11), Value::Int(100000)],
            vec![Value::Int(2), Value::Int(20), Value::Int(100000)],
            vec![Value::Int(3), Value::Int(30), Value::Int(100000)]
        ]
    );
    engine.shutdown().unwrap();
}

#[test]
fn online_column_validation_failure_preserves_concurrent_source_commits() {
    let dir = fixture();
    let engine = Arc::new(open_relaxed_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,10)").unwrap();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl =
        std::thread::spawn(move || worker.execute("ALTER TABLE t MODIFY v INT NOT NULL,LOCK=NONE"));
    assert!(hook.wait_until_reached(Duration::from_secs(5)));
    let write = engine.execute("INSERT INTO t VALUES(2,NULL)");
    hook.release();
    let result = ddl.join().unwrap();
    write.unwrap();
    assert_eq!(result.unwrap_err().code(), 1048);
    assert!(engine.database().table("test.t").unwrap().columns[1].nullable);
    let QueryResponse::ResultSet(rows) = engine.execute("SELECT id,v FROM t ORDER BY id").unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Null]
        ]
    );
    engine.shutdown().unwrap();
}

#[test]
fn equivalent_layout_reuse_preserves_rows_undo_and_fresh_layout_identity() {
    let dir = fixture();
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT)")
            .unwrap();
        engine.execute("INSERT INTO t VALUES(0,NULL)").unwrap();
        let mut reader = SessionContext::default();
        reader.connection_id = 19401;
        reader.isolation = crate::storage::IsolationLevel::RepeatableRead;
        engine.execute_with_session(&mut reader, "BEGIN").unwrap();
        engine
            .execute_with_session(&mut reader, "SELECT v FROM t WHERE id=0")
            .unwrap();
        for iteration in 0..16 {
            engine.execute("UPDATE t SET v=0 WHERE v IS NULL").unwrap();
            engine
                .execute("ALTER TABLE t MODIFY v INT NOT NULL,LOCK=NONE")
                .unwrap();
            let QueryResponse::ResultSet(rows) = engine
                .execute_with_session(&mut reader, "SELECT v FROM t WHERE id=0")
                .unwrap()
            else {
                panic!("rows")
            };
            assert_eq!(rows.rows, vec![vec![Value::Null]]);
            engine
                .execute(&format!("INSERT INTO t VALUES({},10)", iteration * 2 + 1))
                .unwrap();
            engine.execute("ALTER TABLE t MODIFY v INT NULL").unwrap();
            engine
                .execute(&format!("INSERT INTO t VALUES({},NULL)", iteration * 2 + 2))
                .unwrap();
        }
        let table = engine.database().table("test.t").unwrap();
        assert!(
            table.row_layouts.len() <= 2,
            "equivalent decoders accumulated with DDL count: {}",
            table.row_layouts.len()
        );
        let old_ids = table
            .row_layouts
            .iter()
            .map(|layout| layout.layout_id)
            .collect::<std::collections::BTreeSet<_>>();
        engine
            .execute("ALTER TABLE t ADD extra INT DEFAULT 7")
            .unwrap();
        let table = engine.database().table("test.t").unwrap();
        assert!(!old_ids.contains(&table.active_layout_id));
        assert_eq!(
            table
                .row_layouts
                .iter()
                .map(|layout| layout.layout_id)
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            table.row_layouts.len()
        );
        engine
            .execute_with_session(&mut reader, "ROLLBACK")
            .unwrap();
        engine.shutdown().unwrap();
    }
    let engine = open_strict_test_engine(dir.path());
    let QueryResponse::ResultSet(rows) =
        engine.execute("SELECT COUNT(*),SUM(extra) FROM t").unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(rows.rows[0][0].to_display_string(), "33");
    assert_eq!(rows.rows[0][1].to_display_string(), "231");
    engine.shutdown().unwrap();
}
