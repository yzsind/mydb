use super::*;

#[test]
fn check_structure_allows_writes_and_reads_during_validation() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = Arc::new(open_strict_test_engine(dir.path()));
    e.execute("CREATE TABLE t(id INT PRIMARY KEY,v INT,INDEX vi(v))")
        .unwrap();
    e.execute("INSERT INTO t VALUES(1,10),(2,20)").unwrap();
    let hook = e.database().install_ddl_backfill_test_hook();
    let worker = e.clone();
    let ddl = std::thread::spawn(move || worker.execute("CHECK TABLE t"));
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let (tx, rx) = std::sync::mpsc::channel();
    let writer = e.clone();
    let write = std::thread::spawn(move || {
        tx.send(writer.execute("UPDATE t SET id=3,v=30 WHERE id=1"))
            .unwrap()
    });
    let during = rx.recv_timeout(Duration::from_millis(300));
    hook.release();
    write.join().unwrap();
    let result = ddl.join().unwrap();
    assert!(reached, "{result:?}");
    during
        .expect("CHECK must not hold the index handle exclusively during validation")
        .unwrap();
    let QueryResponse::ResultSet(r) = result.unwrap() else {
        panic!("CHECK rows")
    };
    assert!(
        r.rows
            .iter()
            .all(|row| row[2].to_display_string() != "error"),
        "{:?}",
        r.rows
    );
}

#[test]
fn check_lob_batches_admit_replacement_and_snapshot_chains() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = Arc::new(open_strict_test_engine(dir.path()));
    e.execute("CREATE TABLE t(id INT PRIMARY KEY,v LONGTEXT,g GEOMETRY NOT NULL,SPATIAL sp(g))")
        .unwrap();
    e.execute(
        "INSERT INTO t VALUES(1,REPEAT('x',20000),POINT(1,1)),(2,REPEAT('y',24000),POINT(2,2))",
    )
    .unwrap();
    let hook = e.database().install_ddl_backfill_test_hook();
    let worker = e.clone();
    let ddl = std::thread::spawn(move || worker.execute("CHECK TABLE t"));
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let (tx, rx) = std::sync::mpsc::channel();
    let writer = e.clone();
    let write = std::thread::spawn(move || {
        tx.send(writer.execute("UPDATE t SET v=REPEAT('z',30000),g=POINT(9,9) WHERE id=1"))
            .unwrap()
    });
    let during = rx.recv_timeout(Duration::from_secs(1));
    hook.release();
    write.join().unwrap();
    let result = ddl.join().unwrap();
    assert!(reached, "{result:?}");
    during
        .expect("LOB validation must admit replacement")
        .unwrap();
    let QueryResponse::ResultSet(r) = result.unwrap() else {
        panic!("CHECK rows")
    };
    assert!(
        r.rows
            .iter()
            .all(|row| row[2].to_display_string() != "error"),
        "{:?}",
        r.rows
    );
}

#[test]
fn check_index_retry_budget_reports_busy_without_blocking_writes() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = Arc::new(open_strict_test_engine(dir.path()));
    e.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    e.execute("INSERT INTO t VALUES(1)").unwrap();
    let mut hook = e.database().install_ddl_backfill_test_hook();
    let worker = e.clone();
    let check = std::thread::spawn(move || worker.execute("CHECK TABLE t"));
    for attempt in 0..8 {
        assert!(hook.wait_until_reached(Duration::from_secs(10)));
        e.execute(&format!(
            "UPDATE t SET id={} WHERE id={}",
            attempt + 2,
            attempt + 1
        ))
        .unwrap();
        let next = (attempt < 7).then(|| e.database().install_ddl_backfill_test_hook());
        hook.release();
        if let Some(next) = next {
            hook = next;
        }
    }
    let QueryResponse::ResultSet(r) = check.join().unwrap().unwrap() else {
        panic!("CHECK rows")
    };
    assert!(
        r.rows
            .iter()
            .any(|row| row[2].to_display_string() == "error"
                && row[3].to_display_string().contains("stable index view")),
        "{:?}",
        r.rows
    );
    let QueryResponse::ResultSet(r) = e.execute("CHECK TABLE t").unwrap() else {
        panic!("CHECK rows")
    };
    assert!(r
        .rows
        .iter()
        .all(|row| row[2].to_display_string() != "error"));
}

#[test]
#[ignore = "local CHECK concurrency scaling comparison"]
fn check_writer_wait_scaling_probe() {
    let rows = std::env::var("MYDB_CHECK_PROBE_ROWS")
        .unwrap_or_else(|_| "100000".into())
        .parse::<i64>()
        .unwrap();
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = Arc::new(open_strict_test_engine(dir.path()));
    e.execute("CREATE TABLE t(id BIGINT PRIMARY KEY,v VARCHAR(100),INDEX vi(v))")
        .unwrap();
    insert_fixture_rows(
        &e,
        "test.t",
        (1..=rows).map(|i| vec![Value::Int(i), Value::String(format!("{i:090}"))]),
    );
    e.checkpoint().unwrap();
    let worker = e.clone();
    let check = std::thread::spawn(move || {
        let start = Instant::now();
        let result = worker.execute("CHECK TABLE t");
        (start.elapsed().as_micros(), result)
    });
    let mut latencies = Vec::new();
    while !check.is_finished() {
        let started = Instant::now();
        e.execute("UPDATE t SET v=CONCAT(SUBSTR(v,2),'x') WHERE id=1")
            .unwrap();
        latencies.push(started.elapsed().as_micros());
        std::thread::sleep(Duration::from_millis(1));
    }
    let (elapsed, result) = check.join().unwrap();
    result.unwrap();
    latencies.sort_unstable();
    eprintln!(
        "CHECK_PROBE rows={rows} oracle={} elapsed_us={elapsed} writers={} p50_us={} max_us={}",
        std::env::var("MYDB_CHECK_BLOCKING_ORACLE").unwrap_or_default(),
        latencies.len(),
        latencies[latencies.len() / 2],
        latencies.last().unwrap()
    );
}

#[test]
fn check_partition_retries_cross_leaf_writes_and_accepts_owned_historical_keys() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = Arc::new(open_strict_test_engine(dir.path()));
    e.execute("CREATE TABLE t(id INT PRIMARY KEY,v TEXT,g GEOMETRY NOT NULL,INDEX vi(v(8)),INDEX vp(v(8)) WHERE id<10,FULLTEXT ft(v),SPATIAL sp(g)) PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)").unwrap();
    e.execute("INSERT INTO t VALUES(1,'quartz old',POINT(1,1)),(11,'ruby retained',POINT(11,11))")
        .unwrap();
    let mut reader = SessionContext::default();
    reader.connection_id = 38801;
    reader.isolation = crate::storage::IsolationLevel::RepeatableRead;
    e.execute_with_session(&mut reader, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .unwrap();
    e.execute_with_session(&mut reader, "SELECT * FROM t")
        .unwrap()
        .materialize()
        .unwrap();
    let hook = e.database().install_ddl_backfill_test_hook();
    let worker = e.clone();
    let check =
        std::thread::spawn(move || worker.execute("ALTER TABLE t CHECK PARTITION ALL,LOCK=NONE"));
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let mut writer = SessionContext::default();
    writer.connection_id = 38802;
    writer.sysvars.lock_wait_timeout = Some(0);
    let writes = [
        "BEGIN",
        "UPDATE t SET id=12,v='quartz incoming',g=POINT(12,12) WHERE id=1",
        "UPDATE t SET id=2,v='ruby outgoing',g=POINT(2,2) WHERE id=11",
        "INSERT INTO t VALUES(3,'sapphire',POINT(3,3))",
        "SAVEPOINT s",
        "DELETE FROM t WHERE id=3",
        "ROLLBACK TO s",
        "COMMIT",
    ]
    .map(|sql| e.execute_with_session(&mut writer, sql));
    hook.release();
    let result = check.join().unwrap();
    assert!(reached, "{result:?}");
    for write in writes {
        write.unwrap();
    }
    result.unwrap();
    e.execute_with_session(&mut reader, "ROLLBACK").unwrap();
    e.execute("ALTER TABLE t CHECK PARTITION ALL,LOCK=NONE")
        .unwrap();
    drop(e);
    let e = open_strict_test_engine(dir.path());
    e.execute("ALTER TABLE t CHECK PARTITION ALL,LOCK=NONE")
        .unwrap();
}

#[test]
fn check_partition_retry_budget_does_not_publish_partial_results() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = Arc::new(open_strict_test_engine(dir.path()));
    e.execute("CREATE TABLE t(id INT PRIMARY KEY) PARTITION BY HASH(id) PARTITIONS 2")
        .unwrap();
    e.execute("INSERT INTO t VALUES(1)").unwrap();
    let version = e.database().table("test.t").unwrap().schema_version;
    let mut hook = e.database().install_ddl_backfill_test_hook();
    let worker = e.clone();
    let check =
        std::thread::spawn(move || worker.execute("ALTER TABLE t CHECK PARTITION ALL,LOCK=NONE"));
    for attempt in 0..8 {
        assert!(hook.wait_until_reached(Duration::from_secs(10)));
        e.execute(&format!(
            "UPDATE t SET id={} WHERE id={}",
            attempt + 2,
            attempt + 1
        ))
        .unwrap();
        let next = (attempt < 7).then(|| e.database().install_ddl_backfill_test_hook());
        hook.release();
        if let Some(next) = next {
            hook = next;
        }
    }
    assert_eq!(check.join().unwrap().unwrap_err().code(), 1205);
    assert_eq!(
        e.database().table("test.t").unwrap().schema_version,
        version
    );
    e.execute("ALTER TABLE t CHECK PARTITION ALL,LOCK=NONE")
        .unwrap();
}

#[test]
fn check_partition_rejects_extra_keys_and_keeps_failed_statistics_private() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_strict_test_engine(dir.path());
    e.execute(
        "CREATE TABLE t(id INT PRIMARY KEY,v INT,INDEX vi(v)) PARTITION BY HASH(id) PARTITIONS 2",
    )
    .unwrap();
    e.execute("INSERT INTO t VALUES(1,10)").unwrap();
    let db = e.database();
    let table = db.table("test.t").unwrap();
    let version = table.schema_version;
    db.fail_next_catalog_commit_for_test();
    assert!(e
        .execute("ALTER TABLE t CHECK PARTITION ALL,LOCK=NONE")
        .is_err());
    assert_eq!(db.table("test.t").unwrap().schema_version, version);
    assert!(db
        .table("test.t")
        .unwrap()
        .stats
        .partition_stats
        .values()
        .all(|p| p.check_ok.is_none()));
    let txn = db.begin_read_only_txn().unwrap();
    let (sid, row) = table
        .storage_ids()
        .into_iter()
        .find_map(|sid| {
            db.scan_rows_storage_range(&table, sid, txn, 0, u64::MAX)
                .unwrap()
                .into_iter()
                .next()
                .map(|row| (sid, row))
        })
        .unwrap();
    db.rollback_txn(txn.txn_id).unwrap();
    let index = table.indexes.iter().find(|i| i.name == "vi").unwrap();
    let mut values = row.values.clone();
    values[1] = Value::Int(9999);
    let key = table.index_keys(index, &values).unwrap().remove(0);
    let txn = db.begin_txn().unwrap();
    db.insert_index_batches_deferred_at(
        txn.txn_id,
        sid,
        &table,
        [(index.name.clone(), vec![(key, row.row_id)])]
            .into_iter()
            .collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    assert_eq!(
        e.execute("ALTER TABLE t CHECK PARTITION ALL,LOCK=NONE")
            .unwrap_err()
            .code(),
        1030
    );
    e.execute("ALTER TABLE t REPAIR PARTITION ALL,LOCK=NONE")
        .unwrap();
    e.execute("ALTER TABLE t CHECK PARTITION ALL,LOCK=NONE")
        .unwrap();
}

#[test]
#[ignore = "local CHECK PARTITION lock scaling comparison"]
fn check_partition_lock_scaling_probe() {
    for count in [1000, 10000, 100000] {
        for mode in ["SHARED", "NONE"] {
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let e = open_strict_test_engine(dir.path());
            e.execute("CREATE TABLE t(id INT PRIMARY KEY,v INT,INDEX vi(v)) PARTITION BY HASH(id) PARTITIONS 2").unwrap();
            insert_fixture_rows(
                &e,
                "test.t",
                (0..count).map(|i| vec![Value::Int(i), Value::Int(i)]),
            );
            e.checkpoint().unwrap();
            let before = e.database().check_partition_hold_us_for_test();
            let started = Instant::now();
            e.execute(&format!("ALTER TABLE t CHECK PARTITION ALL,LOCK={mode}"))
                .unwrap();
            eprintln!(
                "CHECK_PARTITION_PROBE rows={count} mode={mode} elapsed_us={} barrier_us={}",
                started.elapsed().as_micros(),
                e.database().check_partition_hold_us_for_test() - before
            );
        }
    }
}
