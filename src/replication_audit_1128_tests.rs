// Regression coverage for the 2026-11-28 binlog replication audit.

#[test]
fn audit1128_timestamp2_decoder_preserves_unsigned_seconds() {
    use mysql_common::{
        binlog::value::BinlogValue, constants::ColumnType, io::ParseBuf, proto::MyDeserialize,
    };
    for seconds in [0x7fff_ffff_u32, 0x8000_0000, 2_208_988_800, u32::MAX] {
        for fsp in 0..=6 {
            let mut bytes = seconds.to_be_bytes().to_vec();
            bytes.extend(std::iter::repeat_n(0, usize::from((fsp + 1) / 2)));
            let meta = [fsp];
            let decoded = BinlogValue::deserialize(
                (ColumnType::MYSQL_TYPE_TIMESTAMP2, &meta, false, false),
                &mut ParseBuf(&bytes),
            )
            .unwrap();
            let value = binlog_value_to_mydb(
                &decoded,
                &DataTypeKind::Timestamp { fsp },
                false,
                None,
                None,
            )
            .unwrap();
            assert_eq!(
                value,
                Value::Timestamp(i64::from(seconds) * 1_000_000),
                "seconds={seconds} fsp={fsp}"
            );
        }
    }
}

#[test]
fn audit1128_keyless_binary_images_do_not_collapse() {
    for ty in ["VARBINARY(8)", "LONGBLOB"] {
        for hashed in [false, true] {
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let engine = StorageQueryEngine::open(dir.path()).unwrap();
            engine.execute(&format!("CREATE TABLE t(b {ty})")).unwrap();
            engine
                .execute("INSERT INTO t VALUES(X'FF'),(X'FE')")
                .unwrap();
            let db = engine.database_for_replication();
            let table = db.table("test.t").unwrap();
            let txn = db.begin_txn().unwrap();
            let image = vec![Value::Bytes(vec![0xfe])];
            let id = if hashed {
                KeylessRowIndex::build(db, txn, &table, [image.as_slice()])
                    .unwrap()
                    .take_match(&image)
            } else {
                find_matching_row(db, txn, &table, &image).unwrap()
            }
            .unwrap();
            assert_eq!(
                db.read_row(&table, id, txn).unwrap().unwrap().values,
                image,
                "{ty}, hashed={hashed}"
            );
            db.rollback_txn(txn.txn_id).unwrap();
        }
    }
}

#[test]
fn audit1128_enum_zero_remains_distinct_from_empty_member() {
    let target = DataTypeKind::Enum {
        values: vec![String::new(), "a".into()],
    };
    assert_eq!(
        mysql_value_to_mydb(&mysql::Value::UInt(0), &target, false, None).unwrap(),
        Value::Bytes(Vec::new())
    );
    assert_eq!(
        mysql_value_to_mydb(&mysql::Value::UInt(1), &target, false, None).unwrap(),
        Value::String(String::new())
    );
}

#[test]
fn audit1128_empty_sql_mode_overrides_strict_default() {
    let mut session = SessionContext::default();
    let state = DdlSessionState::default();
    apply_ddl_session_state(&mut session, &state);
    assert!(session.sql_mode.is_empty());
    assert_eq!(session.temporal_sql_mode_bits, 0);
}

#[test]
fn audit1128_virtual_unique_before_images_update_and_delete() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = StorageQueryEngine::open(dir.path()).unwrap();
    engine.execute("CREATE TABLE t(a INT,b INT GENERATED ALWAYS AS (a+1) VIRTUAL NOT NULL, UNIQUE KEY ub(b))").unwrap();
    engine.execute("INSERT INTO t(a) VALUES(1)").unwrap();
    let db = engine.database_for_replication();
    let txn = db.begin_txn().unwrap();
    apply_update_rows(
        db,
        txn,
        "test",
        "t",
        &[RowUpdate {
            before: vec![Value::Int(1), Value::Null],
            after: vec![Value::Int(10), Value::Null],
        }],
    )
    .unwrap();
    apply_delete_rows(db, txn, "test", "t", &[vec![Value::Int(10), Value::Null]]).unwrap();
    db.commit_txn(txn.txn_id).unwrap();
}

#[test]
fn audit1128_virtual_partition_before_images_update_and_delete() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = StorageQueryEngine::open(dir.path()).unwrap();
    engine.execute("CREATE TABLE t(d DATE,y INT GENERATED ALWAYS AS (YEAR(d)) VIRTUAL,v INT) PARTITION BY RANGE(y)(PARTITION p0 VALUES LESS THAN(2020),PARTITION p1 VALUES LESS THAN(2030))").unwrap();
    engine
        .execute("INSERT INTO t(d,v) VALUES('2024-01-01',1)")
        .unwrap();
    let db = engine.database_for_replication();
    let date = NaiveDate::from_ymd_opt(2024, 1, 1)
        .unwrap()
        .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32;
    let txn = db.begin_txn().unwrap();
    apply_update_rows(
        db,
        txn,
        "test",
        "t",
        &[RowUpdate {
            before: vec![Value::Date(date), Value::Null, Value::Int(1)],
            after: vec![Value::Date(date), Value::Null, Value::Int(2)],
        }],
    )
    .unwrap();
    apply_delete_rows(
        db,
        txn,
        "test",
        "t",
        &[vec![Value::Date(date), Value::Null, Value::Int(2)]],
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
}

#[test]
fn audit1128_idempotent_partition_batch_retains_new_rows() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = StorageQueryEngine::open(dir.path()).unwrap();
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY) PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN(10), PARTITION p1 VALUES LESS THAN(20))").unwrap();
    engine.execute("INSERT INTO t VALUES(1)").unwrap();
    let db = engine.database_for_replication();
    let txn = db.begin_txn().unwrap();
    with_recovery_idempotent(|| {
        apply_write_rows(
            db,
            txn,
            "test",
            "t",
            &[vec![Value::Int(1)], vec![Value::Int(11)]],
        )
    })
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id FROM t ORDER BY id").unwrap()
    else {
        panic!("expected rows")
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1)], vec![Value::Int(11)]]);
}

#[test]
fn audit1128_network_errors_preserve_transport_diagnostics() {
    let err = source_io_error(
        "read binlog event",
        mysql::Error::DriverError(mysql::DriverError::Timeout),
    );
    assert_eq!(err.code(), 2013);
    assert!(err.message().contains("timed out"));
}

#[test]
fn audit1128_geometry_identity_includes_srid_and_enum_sentinel_is_typed() {
    let geom = crate::engine::geo::parse_wkt("POINT(1 2)").unwrap();
    let first = crate::engine::geo::write_geometry(&crate::engine::geo::types::Geometry {
        srid: 0,
        geom: geom.clone(),
    })
    .unwrap();
    let second = crate::engine::geo::write_geometry(&crate::engine::geo::types::Geometry {
        srid: 4326,
        geom,
    })
    .unwrap();
    assert_ne!(
        image_match_value(&Value::Geometry(first), false),
        image_match_value(&Value::Geometry(second), false)
    );
    assert_ne!(
        image_match_value(&Value::Bytes(Vec::new()), false),
        image_match_value(&Value::String(String::new()), false)
    );
}

#[test]
fn audit1128_disconnected_or_stale_source_never_reports_caught_up() {
    let cfg = ReplicationChannelConfig::default();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let mut status = ReplicationStatus::from_config(&cfg, dir.path());
    status.running = true;
    status.sql_running = true;
    status.source_log_file = "bin.000001".into();
    status.exec_source_log_file = status.source_log_file.clone();
    status.source_log_pos = 100;
    status.exec_source_log_pos = 100;
    for seen in [None, Some(Instant::now() - Duration::from_secs(10))] {
        status.last_source_activity = seen;
        assert_eq!(status.row(&cfg)[10], Value::String("Connecting".into()));
        assert_eq!(status.row(&cfg)[32], Value::Null);
    }
    status.last_source_activity = Some(Instant::now());
    assert_eq!(status.row(&cfg)[10], Value::String("Yes".into()));
    assert_eq!(status.row(&cfg)[32], Value::UInt(0));
}

#[test]
fn audit1128_heartbeat_only_flushes_dirty_progress() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
    let (runtime, runner) = make_split_runner(dir.path(), engine);
    runtime.progress_persist.lock().unwrap().last_sync = Instant::now() - Duration::from_secs(10);
    runner.persist_runtime_maybe(false);
    assert!(!channel_path(dir.path(), DEFAULT_CHANNEL).exists());
    runtime.progress_persist.lock().unwrap().pending_updates = 1;
    runner.persist_runtime_maybe(false);
    assert!(channel_path(dir.path(), DEFAULT_CHANNEL).exists());
    assert_eq!(runtime.progress_persist.lock().unwrap().pending_updates, 0);
}

#[test]
fn audit1128_connect_timeout_is_bounded_independently_of_kernel_syn_retries() {
    for retry in [0, 100, 1000, 60_000, u64::MAX] {
        let cfg = ReplicationChannelConfig {
            connect_retry_ms: retry,
            ..Default::default()
        };
        let opts: mysql::Opts = source_connection_options(&cfg).into();
        assert_eq!(
            opts.get_tcp_connect_timeout(),
            Some(Duration::from_millis(retry.clamp(100, 5000)))
        );
        assert_eq!(
            opts.get_read_timeout().copied(),
            Some(SOURCE_RECEIVE_TIMEOUT)
        );
    }
}

#[test]
fn audit1128_io_only_start_preserves_pending_until() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
    let (runtime, _) = make_split_runner(dir.path(), engine.clone());
    let manager = ReplicationManager {
        data_dir: dir.path().into(),
        engine,
        channels: Mutex::new(HashMap::from([(DEFAULT_CHANNEL.into(), runtime.clone())])),
        retired_gtids: Mutex::new(None),
    };
    *runtime.until.lock().unwrap() = Some(UntilCondition::Position {
        file: "bin.000002".into(),
        pos: 700,
    });
    let stop = runtime.io_stop.clone();
    *runtime.io_join.lock().unwrap() = Some(thread::spawn(move || {
        while !stop.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(1));
        }
    }));
    manager
        .start_channel_threads(DEFAULT_CHANNEL, None, ReplThreadSel::Io)
        .unwrap();
    let retained = matches!(
        *runtime.until.lock().unwrap(),
        Some(UntilCondition::Position { pos: 700, .. })
    );
    manager.stop_channel(DEFAULT_CHANNEL).unwrap();
    assert!(retained);
}

#[test]
fn audit1128_enum_zero_survives_strict_storage_apply_and_keyless_identity() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = StorageQueryEngine::open(dir.path()).unwrap();
    engine.execute("CREATE TABLE t(e ENUM('','a'))").unwrap();
    let db = engine.database_for_replication();
    let target = DataTypeKind::Enum {
        values: vec![String::new(), "a".into()],
    };
    let rows = [mysql::Value::Int(0), mysql::Value::Int(1)]
        .iter()
        .map(|v| vec![mysql_value_to_mydb(v, &target, false, None).unwrap()])
        .collect::<Vec<_>>();
    let txn = db.begin_txn().unwrap();
    with_replication_applier(|| apply_write_rows(db, txn, "test", "t", &rows)).unwrap();
    with_replication_applier(|| apply_delete_rows(db, txn, "test", "t", &rows[..1])).unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    let table = db.table("test.t").unwrap();
    let txn = db.begin_txn().unwrap();
    assert!(find_matching_row(db, txn, &table, &rows[0])
        .unwrap()
        .is_none());
    assert!(find_matching_row(db, txn, &table, &rows[1])
        .unwrap()
        .is_some());
    db.rollback_txn(txn.txn_id).unwrap();
}

#[test]
fn audit1128_completed_until_is_cleared_when_sql_restarts() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
    let (runtime, runner) = make_split_runner(dir.path(), engine.clone());
    let manager = ReplicationManager { data_dir: dir.path().into(), engine, channels: Mutex::new(HashMap::from([(DEFAULT_CHANNEL.into(),runtime.clone())])), retired_gtids: Mutex::new(None) };
    *runtime.until.lock().unwrap() = Some(UntilCondition::Position { file:"bin.000001".into(),pos:4 });
    runner.stop_on_until();
    manager.start_channel_threads(DEFAULT_CHANNEL,None,ReplThreadSel::Sql).unwrap();
    manager.stop_channel(DEFAULT_CHANNEL).unwrap();
    assert!(runtime.until.lock().unwrap().is_none());
}
