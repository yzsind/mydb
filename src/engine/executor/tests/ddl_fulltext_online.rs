use super::*;

fn rows(e: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(r) = e.execute(sql).unwrap().materialize().unwrap() else {
        panic!("rows")
    };
    r.rows
}
fn assert_corpus(e: &StorageQueryEngine) {
    let db = e.database();
    let txn = db.begin_read_only_txn().unwrap();
    super::fulltext_incremental::assert_corpora(e, "test.t", txn);
    db.rollback_txn(txn.txn_id).unwrap();
}

#[test]
fn partition_fulltext_online_rewrites_replay_qualified_rows_and_corpora() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    for memory in [false, true] {
        for operation in [
            "ALTER TABLE t REBUILD PARTITION p0,LOCK=NONE",
            "ALTER TABLE t REPAIR PARTITION ALL,LOCK=NONE",
            "ALTER TABLE t TRUNCATE PARTITION p0,LOCK=NONE",
            "ALTER TABLE t REORGANIZE PARTITION p0 INTO(PARTITION pa VALUES LESS THAN(5),PARTITION pb VALUES LESS THAN(10)),LOCK=NONE",
            "ALTER TABLE t PARTITION BY HASH(id) PARTITIONS 3,LOCK=NONE",
            "ALTER TABLE t REMOVE PARTITIONING,LOCK=NONE",
            "ALTER TABLE t MODIFY body LONGTEXT,ALGORITHM=COPY,LOCK=NONE",
            "OPTIMIZE TABLE t",
        ] {
            if memory && operation.starts_with("OPTIMIZE") {continue;}
            let dir=tempfile::tempdir_in("/tmp/mydb").unwrap();
            let e=Arc::new(if memory {open_complete_memory_test_engine(dir.path())} else {open_strict_test_engine(dir.path())});
            e.execute("CREATE TABLE t(id INT PRIMARY KEY,body TEXT,title TEXT,active INT,FULLTEXT f(body),FULLTEXT fp(body) WHERE active=1,FULLTEXT ng(body,title) WITH PARSER ngram) PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)").unwrap();
            // Distinct physical leaves deliberately start with colliding RowIds.
            e.execute("INSERT INTO t VALUES(1,'quartz quartz','数据库',1),(2,'amber','全文检索',0),(11,'quartz ruby','数据库',1),(12,NULL,NULL,1)").unwrap();
            let hook=e.database().install_ddl_backfill_test_hook();
            let worker=e.clone();
            let sql=operation.to_owned();
            let ddl=std::thread::spawn(move ||worker.execute(&sql));
            let reached=hook.wait_until_reached(Duration::from_secs(10));
            let mut s=SessionContext::default();s.connection_id=38701;s.sysvars.lock_wait_timeout=Some(0);
            let writes=["BEGIN","UPDATE t SET id=13,body='quartz topaz',title='数据检索' WHERE id=1","DELETE FROM t WHERE id=2","UPDATE t SET id=3,body='ruby quartz',active=0 WHERE id=11","INSERT INTO t VALUES(14,'quartz amber','全文数据',1)","SAVEPOINT s","UPDATE t SET body='rolled back' WHERE id=14","ROLLBACK TO s","COMMIT"]
                .map(|sql|e.execute_with_session(&mut s,sql));
            hook.release();
            let result=ddl.join().unwrap();
            assert!(reached,"{operation}: {result:?}");
            for write in writes {write.unwrap_or_else(|err|panic!("{operation}: {err}"));}
            let response=result.unwrap_or_else(|err|panic!("{operation}: {err}"));
            if let QueryResponse::ResultSet(r)=response {assert!(r.rows.iter().all(|row|row[2].to_display_string()!="Error"),"{:?}",r.rows);}
            let expected=if operation.contains("TRUNCATE") {vec![12,13,14]} else {vec![3,12,13,14]};
            assert_eq!(rows(&e,"SELECT id FROM t ORDER BY id"),expected.iter().map(|i|vec![Value::Int(*i)]).collect::<Vec<_>>(),"{operation}");
            let matched=rows(&e,"SELECT id FROM t WHERE MATCH(body) AGAINST('quartz' IN BOOLEAN MODE) ORDER BY id");
            assert_eq!(matched,expected.iter().filter(|i|**i!=12).map(|i|vec![Value::Int(*i)]).collect::<Vec<_>>(),"{operation}");
            assert_corpus(&e);
            if !memory {
                drop(e);let e=open_strict_test_engine(dir.path());assert_corpus(&e);
                e.execute("UPDATE t SET body='quartz violet' WHERE id=13").unwrap();assert_corpus(&e);
            }
        }
    }
}

#[test]
fn mixed_copy_adds_fulltext_and_spatial_in_one_private_generation() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    for partition in ["", " PARTITION BY HASH(id) PARTITIONS 2"] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let e = Arc::new(open_strict_test_engine(dir.path()));
        e.execute(&format!(
            "CREATE TABLE t(id INT PRIMARY KEY,body TEXT,g GEOMETRY NOT NULL){partition}"
        ))
        .unwrap();
        e.execute("INSERT INTO t VALUES(1,'quartz',POINT(1,1)),(2,'amber',POINT(2,2))")
            .unwrap();
        let hook = e.database().install_ddl_backfill_test_hook();
        let worker = e.clone();
        let ddl = std::thread::spawn(move || {
            worker.execute("ALTER TABLE t MODIFY body LONGTEXT,ADD FULLTEXT f(body),ADD SPATIAL sp(g),ALGORITHM=COPY,LOCK=NONE")
        });
        let reached = hook.wait_until_reached(Duration::from_secs(10));
        let mut s = SessionContext::default();
        s.connection_id = 38702;
        s.sysvars.lock_wait_timeout = Some(0);
        let write = e.execute_with_session(
            &mut s,
            "UPDATE t SET body='quartz ruby',g=POINT(9,9) WHERE id=2",
        );
        hook.release();
        let result = ddl.join().unwrap();
        assert!(reached, "{result:?}");
        write.unwrap();
        result.unwrap();
        assert_corpus(&e);
        assert_eq!(
            rows(
                &e,
                "SELECT id FROM t WHERE MATCH(body) AGAINST('quartz' IN BOOLEAN MODE) ORDER BY id"
            ),
            vec![vec![Value::Int(1)], vec![Value::Int(2)]]
        );
        let QueryResponse::ResultSet(r) = e.execute("CHECK TABLE t").unwrap() else {
            panic!("CHECK")
        };
        assert!(
            r.rows.iter().all(|r| r[2].to_display_string() != "error"),
            "{:?}",
            r.rows
        );
        drop(e);
        let e = open_strict_test_engine(dir.path());
        assert_corpus(&e);
    }
}

#[test]
fn partition_fulltext_recovery_process_child() {
    let Ok(path) = std::env::var("MYDB_FT_DDL_CHILD_DIR") else {
        return;
    };
    let e = open_strict_test_engine(std::path::Path::new(&path));
    e.execute("CREATE TABLE t(id INT PRIMARY KEY,body TEXT,FULLTEXT f(body),FULLTEXT ng(body) WITH PARSER ngram) PARTITION BY HASH(id) PARTITIONS 2").unwrap();
    e.execute("INSERT INTO t VALUES(1,'quartz 数据库'),(2,'ruby 数据'),(3,'quartz 检索')")
        .unwrap();
    e.execute("ALTER TABLE t REBUILD PARTITION ALL,LOCK=NONE")
        .unwrap();
    panic!("crash injection missed");
}

#[test]
fn partition_fulltext_rebuild_recovers_before_and_after_publication() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    for point in ["abort_before_swap", "abort_after_swap"] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let log = std::fs::File::create(dir.path().join("child.log")).unwrap();
        let status=std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact","engine::executor::tests::ddl_fulltext_online::partition_fulltext_recovery_process_child","--nocapture"])
            .env("MYDB_FT_DDL_CHILD_DIR",dir.path()).env("MYDB_PARTITION_REORG_CRASH_POINT",point)
            .stdout(log.try_clone().unwrap()).stderr(log).status().unwrap();
        assert_eq!(
            status.code(),
            Some(86),
            "{}",
            std::fs::read_to_string(dir.path().join("child.log")).unwrap()
        );
        let e = open_strict_test_engine(dir.path());
        assert_corpus(&e);
        assert_eq!(
            rows(
                &e,
                "SELECT id FROM t WHERE MATCH(body) AGAINST('quartz' IN BOOLEAN MODE) ORDER BY id"
            ),
            vec![vec![Value::Int(1)], vec![Value::Int(3)]]
        );
        e.execute("UPDATE t SET body='quartz 新数据' WHERE id=2")
            .unwrap();
        assert_corpus(&e);
        e.execute("ALTER TABLE t CHECK PARTITION ALL").unwrap();
    }
}

#[test]
fn fulltext_exchange_prepares_future_corpora_without_excluding_writers() {
    for validation in ["WITH VALIDATION", "WITHOUT VALIDATION"] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let e = Arc::new(open_strict_test_engine(dir.path()));
        let def="(id INT PRIMARY KEY,body TEXT,active INT,FULLTEXT f(body),FULLTEXT fp(body) WHERE active=1,FULLTEXT ng(body) WITH PARSER ngram)";
        e.execute(&format!("CREATE TABLE t{def} PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)")).unwrap();
        e.execute(&format!("CREATE TABLE o{def}")).unwrap();
        e.execute("INSERT INTO t VALUES(1,'quartz 数据库',1),(11,'ruby 数据库',0)")
            .unwrap();
        e.execute("INSERT INTO o VALUES(3,'amber amber 检索',1),(4,NULL,0)")
            .unwrap();
        let hook = e.database().install_fulltext_copy_statistics_test_hook();
        let worker = e.clone();
        let sql =
            format!("ALTER TABLE t EXCHANGE PARTITION p0 WITH TABLE o {validation},LOCK=NONE");
        let ddl = std::thread::spawn(move || worker.execute(&sql));
        let reached = hook.wait_until_reached(Duration::from_secs(10));
        let mut s = SessionContext::default();
        s.connection_id = 38703;
        s.sysvars.lock_wait_timeout = Some(0);
        let writes = [
            "BEGIN",
            "UPDATE o SET body='quartz sapphire 数据' WHERE id=3",
            "DELETE FROM o WHERE id=4",
            "INSERT INTO o VALUES(5,'ruby 检索',0)",
            "UPDATE t SET body='quartz outgoing' WHERE id=1",
            "COMMIT",
        ]
        .map(|sql| e.execute_with_session(&mut s, sql));
        hook.release();
        let result = ddl.join().unwrap();
        assert!(reached, "{result:?}");
        for write in writes {
            write.unwrap();
        }
        result.unwrap();
        for _ in 0..3 {
            assert_corpus(&e);
            let db = e.database();
            let txn = db.begin_read_only_txn().unwrap();
            super::fulltext_incremental::assert_corpora(&e, "test.o", txn);
            db.rollback_txn(txn.txn_id).unwrap();
            e.execute(&format!(
                "ALTER TABLE t EXCHANGE PARTITION p0 WITH TABLE o {validation},LOCK=NONE"
            ))
            .unwrap();
        }
        drop(e);
        let e = open_strict_test_engine(dir.path());
        assert_corpus(&e);
        let db = e.database();
        let txn = db.begin_read_only_txn().unwrap();
        super::fulltext_incremental::assert_corpora(&e, "test.o", txn);
        db.rollback_txn(txn.txn_id).unwrap();
    }
}

#[test]
#[ignore = "local FULLTEXT EXCHANGE vocabulary scaling comparison"]
fn fulltext_exchange_lock_scaling_probe() {
    for count in [1000, 10000] {
        for mode in ["EXCLUSIVE", "NONE"] {
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let e = open_strict_test_engine(dir.path());
            e.execute(&format!("CREATE TABLE t(id INT PRIMARY KEY,body TEXT,FULLTEXT f(body)) PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN({}),PARTITION p1 VALUES LESS THAN MAXVALUE)",count+1)).unwrap();
            e.execute("CREATE TABLE o(id INT PRIMARY KEY,body TEXT,FULLTEXT f(body))")
                .unwrap();
            e.execute(&format!(
                "INSERT INTO t VALUES(1,'outgoing'),({},'retained')",
                count + 2
            ))
            .unwrap();
            insert_fixture_rows(
                &e,
                "test.o",
                (1..=count).map(|i| vec![Value::Int(i), Value::String(format!("quartz word{i}"))]),
            );
            e.checkpoint().unwrap();
            let before = e.database().exchange_hold_us_for_test();
            let started = Instant::now();
            e.execute(&format!(
                "ALTER TABLE t EXCHANGE PARTITION p0 WITH TABLE o,LOCK={mode}"
            ))
            .unwrap();
            eprintln!(
                "FT_EXCHANGE_PROBE rows={count} mode={mode} elapsed_us={} hold_us={}",
                started.elapsed().as_micros(),
                e.database().exchange_hold_us_for_test() - before
            );
            assert_corpus(&e);
        }
    }
}

#[test]
fn fulltext_exchange_crash_process_child() {
    let Ok(path) = std::env::var("MYDB_FT_EXCHANGE_CHILD_DIR") else {
        return;
    };
    let e = open_strict_test_engine(std::path::Path::new(&path));
    let def = "(id INT PRIMARY KEY,body TEXT,FULLTEXT f(body),FULLTEXT ng(body) WITH PARSER ngram)";
    e.execute(&format!("CREATE TABLE t{def} PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)")).unwrap();
    e.execute(&format!("CREATE TABLE o{def}")).unwrap();
    e.execute("INSERT INTO t VALUES(1,'quartz 数据库'),(11,'ruby 数据')")
        .unwrap();
    e.execute("INSERT INTO o VALUES(3,'quartz 检索'),(4,NULL)")
        .unwrap();
    e.execute("ALTER TABLE t EXCHANGE PARTITION p0 WITH TABLE o,LOCK=NONE")
        .unwrap();
    panic!("crash point missed");
}

#[test]
fn fulltext_exchange_corpora_share_the_catalog_recovery_decision() {
    for phase in 1..=3 {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let log = std::fs::File::create(dir.path().join("child.log")).unwrap();
        let status=std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact","engine::executor::tests::ddl_fulltext_online::fulltext_exchange_crash_process_child","--nocapture"])
            .env("MYDB_FT_EXCHANGE_CHILD_DIR",dir.path()).env("MYDB_EXCHANGE_CRASH_STAGE",phase.to_string())
            .stdout(log.try_clone().unwrap()).stderr(log).status().unwrap();
        assert_eq!(
            status.code(),
            Some(86),
            "{}",
            std::fs::read_to_string(dir.path().join("child.log")).unwrap()
        );
        let e = open_strict_test_engine(dir.path());
        assert_corpus(&e);
        let db = e.database();
        let txn = db.begin_read_only_txn().unwrap();
        super::fulltext_incremental::assert_corpora(&e, "test.o", txn);
        db.rollback_txn(txn.txn_id).unwrap();
        assert_eq!(
            rows(&e, "SELECT id FROM o ORDER BY id"),
            if phase == 1 {
                vec![vec![Value::Int(3)], vec![Value::Int(4)]]
            } else {
                vec![vec![Value::Int(1)]]
            }
        );
        e.execute("UPDATE o SET body='quartz recovered'").unwrap();
        let txn = db.begin_read_only_txn().unwrap();
        super::fulltext_incremental::assert_corpora(&e, "test.o", txn);
        db.rollback_txn(txn.txn_id).unwrap();
    }
}
