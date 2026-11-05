fn release_audit_engine() -> (tempfile::TempDir, Arc<StorageQueryEngine>) {
    std::fs::create_dir_all("/tmp/mydb/audit-fixes/tests").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb/audit-fixes/tests").unwrap();
    let mut config = crate::storage::StorageConfig::from_env();
    config.buffer_pool_pages = 512;
    config.buffer_pool_io_threads = 1;
    let engine = Arc::new(
        StorageQueryEngine::open_with_storage_config_without_background_workers(dir.path(), config)
            .unwrap(),
    );
    (dir, engine)
}

#[test]
fn release_audit_replication_enum_bit_images_locate_stored_rows() {
    let (_dir, engine) = release_audit_engine();
    for (name, key) in [
        ("pk", "PRIMARY KEY(s)"),
        ("uk", "UNIQUE KEY(s)"),
        ("heap", "KEY(v)"),
    ] {
        engine
            .execute(&format!(
                "CREATE TABLE {name}(s ENUM('a','b') NOT NULL,b BIT(9),v INT,{key})"
            ))
            .unwrap();
        engine
            .execute(&format!("INSERT INTO {name} VALUES('a',257,1),('b',5,2)"))
            .unwrap();
        let enum_type = DataTypeKind::Enum {
            values: vec!["a".into(), "b".into()],
        };
        let before = vec![
            mysql_value_to_mydb(&mysql::Value::Int(1), &enum_type, false, None).unwrap(),
            mysql_value_to_mydb(
                &mysql::Value::Bytes(vec![1, 1]),
                &DataTypeKind::Bit { len: Some(9) },
                false,
                None,
            )
            .unwrap(),
            Value::Int(1),
        ];
        let mut after = before.clone();
        after[2] = Value::Int(9);
        let db = engine.database_for_replication();
        let txn = db.begin_txn().unwrap();
        apply_update_rows(
            db,
            txn,
            "test",
            name,
            &[RowUpdate {
                before: before.clone(),
                after: after.clone(),
            }],
        )
        .unwrap();
        apply_delete_rows(db, txn, "test", name, &[after]).unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        let QueryResponse::ResultSet(result) = engine
            .execute(&format!("SELECT s,b+0,v FROM {name}"))
            .unwrap()
            .materialize()
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(
            result
                .rows
                .iter()
                .map(|r| r.iter().map(Value::to_display_string).collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            vec![vec!["b".to_string(), "5".to_string(), "2".to_string()]]
        );
    }
}

#[test]
fn release_audit_replication_row_flags_scope_foreign_key_checks() {
    let (_dir, engine) = release_audit_engine();
    engine
        .execute("CREATE TABLE p(id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("CREATE TABLE c(id INT PRIMARY KEY,pid INT,FOREIGN KEY(pid) REFERENCES p(id))")
        .unwrap();
    let db = engine.database_for_replication();
    let op = RelayOp::RowsWithForeignKeyChecks {
        enabled: false,
        op: Box::new(RelayOp::WriteRows {
            database: "test".into(),
            table: "c".into(),
            rows: vec![vec![Value::Int(2), Value::Int(2)]],
        }),
    };
    let txn = db.begin_txn().unwrap();
    apply_ops_to_storage(
        &engine,
        db,
        txn,
        &[
            op,
            RelayOp::WriteRows {
                database: "test".into(),
                table: "p".into(),
                rows: vec![vec![Value::Int(2)]],
            },
        ],
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    assert!(crate::storage::fk_checks_enabled());
    let txn = db.begin_txn().unwrap();
    let err = apply_ops_to_storage(
        &engine,
        db,
        txn,
        &[RelayOp::RowsWithForeignKeyChecks {
            enabled: true,
            op: Box::new(RelayOp::WriteRows {
                database: "test".into(),
                table: "c".into(),
                rows: vec![vec![Value::Int(3), Value::Int(99)]],
            }),
        }],
    )
    .unwrap_err();
    assert_eq!(err.code(), 1452);
    assert!(!err.message().contains("Int("));
    db.rollback_txn(txn.txn_id).unwrap();
}

#[test]
fn release_audit_replication_reset_all_preserves_global_gtids_on_reopen() {
    let (dir, engine) = release_audit_engine();
    let manager = ReplicationManager {
        data_dir: dir.path().into(),
        engine: engine.clone(),
        channels: Mutex::new(HashMap::new()),
        retired_gtids: Mutex::new(None),
    };
    let cfg = ReplicationChannelConfig {
        source_host: "127.0.0.1".into(),
        auto_position: true,
        ..Default::default()
    };
    manager.change_source(cfg.clone()).unwrap();
    let gtid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-17";
    manager
        .ensure_channel(DEFAULT_CHANNEL)
        .unwrap()
        .status
        .lock()
        .unwrap()
        .executed_gtid_set = gtid.into();
    manager.reset_channel_all(DEFAULT_CHANNEL).unwrap();
    assert!(manager.snapshot_all_channels().is_empty());
    assert_eq!(manager.executed_gtid_set().unwrap(), gtid);
    drop(manager);
    drop(engine);
    let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
    let manager = ReplicationManager {
        data_dir: dir.path().into(),
        engine,
        channels: Mutex::new(HashMap::new()),
        retired_gtids: Mutex::new(None),
    };
    manager.change_source(cfg).unwrap();
    assert_eq!(
        manager
            .ensure_channel(DEFAULT_CHANNEL)
            .unwrap()
            .status
            .lock()
            .unwrap()
            .executed_gtid_set,
        gtid
    );
    manager.stop_channel("absent").unwrap();
    manager.reset_channel("absent").unwrap();
    manager.reset_channel_all("absent").unwrap();
}

#[test]
fn release_audit_replication_control_parser_rejects_unsafe_values() {
    for value in ["yes", "2", "-1", "garbage"] {
        assert!(parse_change_source(&format!(
            "CHANGE REPLICATION SOURCE TO SOURCE_AUTO_POSITION={value}"
        ))
        .is_err());
        assert!(
            parse_change_source(&format!("CHANGE REPLICATION SOURCE TO SOURCE_SSL={value}"))
                .is_err()
        );
    }
    for value in ["True", "oN", "1"] {
        assert!(
            parse_change_source(&format!(
                "CHANGE REPLICATION SOURCE TO SOURCE_AUTO_POSITION={value}"
            ))
            .unwrap()
            .auto_position
        );
    }
    for value in ["FALSE", "off", "0"] {
        assert!(
            !parse_change_source(&format!(
                "CHANGE REPLICATION SOURCE TO SOURCE_AUTO_POSITION={value}"
            ))
            .unwrap()
            .auto_position
        );
    }
    let sql =
        "CHANGE REPLICATION SOURCE TO SOURCE_PASSWORD='abc FOR CHANNEL evil' FOR CHANNEL 'safe'";
    let cfg = parse_change_source(sql).unwrap();
    assert_eq!(cfg.channel, "safe");
    assert_eq!(cfg.source_password, "abc FOR CHANNEL evil");
    assert_eq!(parse_change_source_channel(sql), "safe");
    assert_eq!(
        parse_change_source("CHANGE REPLICATION SOURCE TO SOURCE_PASSWORD='abc FOR CHANNEL evil'")
            .unwrap()
            .channel,
        DEFAULT_CHANNEL
    );
    for name in ["../escape", "a/b", "a\\b", ".."] {
        assert!(validate_channel_name(name).is_err());
    }
}

#[test]
fn release_audit_replication_source_errors_preserve_diagnostics() {
    let err = source_io_error(
        "read binlog",
        mysql::Error::MySqlError(mysql::MySqlError {
            code: 1236,
            state: "HY000".into(),
            message: "Requested binlog was purged".into(),
        }),
    );
    assert_eq!(err.code(), 1236);
    assert_eq!(err.message(), "Requested binlog was purged");
    assert!(io_error_is_fatal(&err, take_io_error_fatal()));
    let err = source_io_error(
        "read binlog",
        mysql::Error::IoError(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "connection reset",
        )),
    );
    assert!(!io_error_is_fatal(&err, take_io_error_fatal()));
    assert!(!err.message().contains("IoError"));
}

#[test]
fn release_audit_replication_invalid_calendar_components_survive_decode() {
    for (year, month, day) in [(2026, 2, 29), (2026, 0, 12), (2026, 1, 0)] {
        let date = mysql_value_to_mydb(
            &mysql::Value::Date(year, month, day, 0, 0, 0, 0),
            &DataTypeKind::Date,
            false,
            None,
        )
        .unwrap();
        let Value::Date(value) = date else { panic!() };
        assert_eq!(
            crate::types::decode_tagged_date(value),
            Some((year as u32, month as u32, day as u32))
        );
        let dt = mysql_value_to_mydb(
            &mysql::Value::Date(year, month, day, 12, 34, 56, 123456),
            &DataTypeKind::DateTime { fsp: 6 },
            false,
            None,
        )
        .unwrap();
        assert_eq!(
            dt,
            Value::DateTime(
                crate::types::encode_tagged_datetime(
                    year as u32,
                    month as u32,
                    day as u32,
                    12,
                    34,
                    56,
                    123456
                )
                .unwrap()
            )
        );
    }
}

#[test]
fn release_audit_replication_long_retry_wait_is_interruptible() {
    let (dir, engine) = release_audit_engine();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let manager = ReplicationManager {
        data_dir: dir.path().into(),
        engine,
        channels: Mutex::new(HashMap::new()),
        retired_gtids: Mutex::new(None),
    };
    manager
        .change_source(ReplicationChannelConfig {
            source_host: "127.0.0.1".into(),
            source_port: port,
            connect_retry_ms: 300_000,
            ..Default::default()
        })
        .unwrap();
    manager
        .start_channel_threads(DEFAULT_CHANNEL, None, ReplThreadSel::Io)
        .unwrap();
    let runtime = manager.ensure_channel(DEFAULT_CHANNEL).unwrap();
    let started = Instant::now();
    while runtime.status.lock().unwrap().last_io_error.is_none()
        && started.elapsed() < Duration::from_secs(3)
    {
        thread::sleep(Duration::from_millis(10));
    }
    thread::sleep(Duration::from_millis(50));
    let started = Instant::now();
    manager.stop_channel(DEFAULT_CHANNEL).unwrap();
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(!runtime.status.lock().unwrap().running);
}

#[test]
fn release_audit_replication_applier_status_tracks_sql_thread() {
    let (dir, engine) = release_audit_engine();
    let manager = init_global(dir.path(), engine.clone());
    manager
        .change_source(ReplicationChannelConfig {
            source_host: "127.0.0.1".into(),
            ..Default::default()
        })
        .unwrap();
    let runtime = manager.ensure_channel(DEFAULT_CHANNEL).unwrap();
    runtime.status.lock().unwrap().running = true;
    runtime.status.lock().unwrap().sql_running = false;
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT SERVICE_STATE FROM performance_schema.replication_applier_status")
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(result.rows, vec![vec![Value::String("OFF".into())]]);
    runtime.status.lock().unwrap().running = false;
    runtime.status.lock().unwrap().sql_running = true;
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT SERVICE_STATE FROM performance_schema.replication_applier_status")
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(result.rows, vec![vec![Value::String("ON".into())]]);
}

#[test]
fn release_audit_replication_show_until_reports_condition_and_reset_clears_it() {
    let (dir, engine) = release_audit_engine();
    let manager = ReplicationManager {
        data_dir: dir.path().into(),
        engine,
        channels: Mutex::new(HashMap::new()),
        retired_gtids: Mutex::new(None),
    };
    manager
        .change_source(ReplicationChannelConfig {
            source_host: "127.0.0.1".into(),
            ..Default::default()
        })
        .unwrap();
    let runtime = manager.ensure_channel(DEFAULT_CHANNEL).unwrap();
    *runtime.until.lock().unwrap() = Some(UntilCondition::Position {
        file: "bin.000123".into(),
        pos: 456,
    });
    let QueryResponse::ResultSet(result) = manager.show_status(None).unwrap() else {
        panic!()
    };
    assert_eq!(
        &result.rows[0][23..26],
        &[
            Value::String("Source".into()),
            Value::String("bin.000123".into()),
            Value::UInt(456)
        ]
    );
    manager.reset_channel(DEFAULT_CHANNEL).unwrap();
    let QueryResponse::ResultSet(result) = manager.show_status(None).unwrap() else {
        panic!()
    };
    assert_eq!(result.rows[0][23], Value::String("None".into()));
}
