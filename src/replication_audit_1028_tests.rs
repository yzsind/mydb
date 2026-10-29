// Regression cases for the 2026-10-28 receiver audit.
use mysql_common::constants::ColumnType as WireType;

fn audit1028_render_opaque(ty: WireType, bytes: &[u8]) -> Result<String, std::fmt::Error> {
    let opaque = mysql_common::binlog::jsonb::OpaqueValue::new(ty, bytes);
    let mut out = String::new();
    render_jsonb_opaque(&opaque, &mut out)?;
    Ok(out)
}

#[test]
fn audit1028_json_opaque_decimal_keeps_numeric_type_and_precision() {
    for text in [
        "12.345",
        "-0.000001",
        "0.00",
        "99999999999999999999999999999999999.123456789012345678901234567890",
    ] {
        let decimal = mysql_common::binlog::decimal::Decimal::from_str(text).unwrap();
        let scale = text.split_once('.').map_or(0, |(_, f)| f.len());
        let precision = text.bytes().filter(u8::is_ascii_digit).count();
        let mut bytes = vec![precision as u8, scale as u8];
        decimal.write_bin(&mut bytes).unwrap();
        assert_eq!(
            audit1028_render_opaque(WireType::MYSQL_TYPE_NEWDECIMAL, &bytes).unwrap(),
            text
        );
    }
}

#[test]
fn audit1028_json_opaque_temporals_use_internal_packed_little_endian() {
    let ymd = ((2026_i64 * 13 + 1) << 5) | 15;
    let date = (ymd << 17) << 24;
    let hms = (10 << 12) | (30 << 6) | 45;
    let datetime = date | (hms << 24) | 500001;
    for (ty, packed, expected) in [
        (WireType::MYSQL_TYPE_DATE, date, "\"2026-01-15\""),
        (
            WireType::MYSQL_TYPE_DATETIME,
            datetime,
            "\"2026-01-15 10:30:45.500001\"",
        ),
        (
            WireType::MYSQL_TYPE_TIMESTAMP,
            datetime,
            "\"2026-01-15 10:30:45.500001\"",
        ),
        (
            WireType::MYSQL_TYPE_TIME,
            -(((838_i64 << 12) | (59 << 6) | 58) << 24 | 123456),
            "\"-838:59:58.123456\"",
        ),
        (WireType::MYSQL_TYPE_DATE, 0, "\"0000-00-00\""),
        (
            WireType::MYSQL_TYPE_DATETIME,
            0,
            "\"0000-00-00 00:00:00.000000\"",
        ),
        (WireType::MYSQL_TYPE_TIME, 0, "\"00:00:00.000000\""),
    ] {
        assert_eq!(
            audit1028_render_opaque(ty, &packed.to_le_bytes()).unwrap(),
            expected
        );
    }
}

#[test]
fn audit1028_json_opaque_binary_uses_mysql_tag_padding_and_line_breaks() {
    assert_eq!(
        audit1028_render_opaque(WireType::MYSQL_TYPE_VARCHAR, b"a").unwrap(),
        "\"base64:type15:YQ==\""
    );
    let rendered = audit1028_render_opaque(WireType::MYSQL_TYPE_BLOB, &[0; 60]).unwrap();
    assert_eq!(
        rendered,
        format!("\"base64:type252:{}\\n{}\"", "A".repeat(76), "AAAA")
    );
}

#[test]
fn audit1028_drop_column_row_mapping_retains_target_slots() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = StorageQueryEngine::open(dir.path()).unwrap();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, gone INT, v INT)")
        .unwrap();
    engine.execute("ALTER TABLE t DROP COLUMN gone").unwrap();
    let table = engine.database_for_replication().table("test.t").unwrap();
    let row = mysql::binlog::row::BinlogRow::new(
        vec![
            Some(mysql::binlog::value::BinlogValue::Value(mysql::Value::Int(
                1,
            ))),
            Some(mysql::binlog::value::BinlogValue::Value(mysql::Value::Int(
                9,
            ))),
        ],
        Arc::from(vec![mysql::Column::new(WireType::MYSQL_TYPE_LONG); 2]),
    );
    assert_eq!(
        convert_binlog_row(
            &table,
            &table
                .iter_visible_columns()
                .map(|(i, _)| i)
                .collect::<Vec<_>>(),
            row,
            None
        )
        .unwrap(),
        vec![Value::Int(1), Value::Null, Value::Int(9)]
    );
}

#[test]
fn audit1028_strict_update_missing_before_must_not_skip_existing_after() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = StorageQueryEngine::open(dir.path()).unwrap();
    for (table, key) in [("keyed", "PRIMARY KEY"), ("keyless", "")] {
        engine
            .execute(&format!("CREATE TABLE {table}(id INT {key}, v INT)"))
            .unwrap();
        engine
            .execute(&format!("INSERT INTO {table} VALUES(2,20)"))
            .unwrap();
        let db = engine.database_for_replication();
        let update = RowUpdate {
            before: vec![Value::Int(1), Value::Int(10)],
            after: vec![Value::Int(2), Value::Int(20)],
        };
        let txn = db.begin_txn().unwrap();
        let result = apply_update_rows(db, txn, "test", table, std::slice::from_ref(&update));
        db.rollback_txn(txn.txn_id).unwrap();
        assert_eq!(result.unwrap_err().code(), 1032);
        let txn = db.begin_txn().unwrap();
        with_recovery_idempotent(|| apply_update_rows(db, txn, "test", table, &[update])).unwrap();
        db.commit_txn(txn.txn_id).unwrap();
    }
}

#[test]
fn audit1028_delay_uses_current_commit_and_stop_keeps_boundary_unapplied() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY)")
        .unwrap();
    let (runtime, runner) = make_split_runner(dir.path(), engine.clone());
    runtime.config.lock().unwrap().source_delay_secs = 30;
    runtime.status.lock().unwrap().last_event_timestamp = 0;
    runner
        .append_relay(
            "bin.000001",
            100,
            None,
            &RelayOp::WriteRows {
                database: "test".into(),
                table: "t".into(),
                rows: vec![vec![Value::Int(1)]],
            },
        )
        .unwrap();
    runner
        .append_relay(
            "bin.000001",
            180,
            None,
            &RelayOp::TxnCommit {
                event_count: 1,
                commit_ts: unix_secs(),
            },
        )
        .unwrap();
    let durable = runtime.relay_gate.lock().unwrap().durable_offset;
    let worker = thread::spawn(move || runner.sql_apply_available(durable));
    for _ in 0..100 {
        if runtime.status.lock().unwrap().sql_delay_until.is_some() {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    let waiting = runtime.status.lock().unwrap().sql_delay_until.is_some();
    runtime.sql_stop.store(true, Ordering::Release);
    worker.join().unwrap().unwrap();
    assert!(
        waiting,
        "heartbeat/global IO timestamp must not disable the delay"
    );
    assert_eq!(runtime.status.lock().unwrap().relay_log_pos, 0);
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT * FROM t")
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!()
    };
    assert!(rows.rows.is_empty(), "STOP during delay must not commit");
    runtime.sql_stop.store(false, Ordering::Release);
    runtime.config.lock().unwrap().source_delay_secs = 0;
    let runner = ChannelRunner::new(dir.path().to_path_buf(), engine, runtime.clone());
    runner.sql_apply_available(durable).unwrap();
    assert_eq!(runtime.status.lock().unwrap().relay_log_pos, durable);
}

#[test]
fn audit1028_ddl_recovery_after_catalog_commit_before_channel_progress() {
    for ddl in [
        "CREATE TABLE t(id INT PRIMARY KEY)",
        "ALTER TABLE t ADD v INT DEFAULT 7",
    ] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        {
            let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
            if ddl.starts_with("ALTER") {
                engine
                    .execute("CREATE TABLE t(id INT PRIMARY KEY)")
                    .unwrap();
            }
            let (runtime, runner) = make_split_runner(dir.path(), engine.clone());
            let initial = runtime.status.lock().unwrap().clone();
            runner
                .append_relay(
                    "bin.000001",
                    100,
                    None,
                    &RelayOp::Ddl {
                        source_ts: 0,
                        database: "test".into(),
                        sql: ddl.into(),
                        session: Default::default(),
                        transactional: false,
                    },
                )
                .unwrap();
            let durable = runtime.relay_gate.lock().unwrap().durable_offset;
            runner.sql_apply_available(durable).unwrap();
            // Lose only the channel progress, as in a crash after catalog WAL
            // commit but before the channel metadata atomic rename.
            *runtime.status.lock().unwrap() = initial;
            runner.sql_apply_available(durable).unwrap();
        }
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        let (runtime, runner) = make_split_runner(dir.path(), engine.clone());
        let len = fs::metadata(relay_log_path(dir.path(), DEFAULT_CHANNEL))
            .unwrap()
            .len();
        runtime.relay_gate.lock().unwrap().durable_offset = len;
        runner.replay_relay_tail().unwrap();
        engine.execute("INSERT INTO t(id) VALUES(1)").unwrap();
    }
}

#[test]
fn audit1028_json_malformed_typed_opaque_is_an_error() {
    for bytes in [
        vec![],
        vec![0; 7],
        vec![0xff; 8],
        (1_i64 << 24 | 1_000_000).to_le_bytes().to_vec(),
    ] {
        assert!(audit1028_render_opaque(WireType::MYSQL_TYPE_DATETIME, &bytes).is_err());
    }
    for bytes in [
        vec![],
        vec![1, 2, 0x80],
        vec![82, 0, 0x80],
        vec![2, 0, 0xff],
        vec![2, 0, 0x80, 0],
    ] {
        assert!(audit1028_render_opaque(WireType::MYSQL_TYPE_NEWDECIMAL, &bytes).is_err());
    }
}

#[test]
fn audit1028_xa_prepare_and_resolution_survive_storage_reopen_and_stale_cursor() {
    for commit in [true, false] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let xid = crate::xa::Xid::new(-1, b"branch".to_vec(), vec![0, 255]).unwrap();
        {
            let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
            engine
                .execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
                .unwrap();
            let (runtime, runner) = make_split_runner(dir.path(), engine.clone());
            runner
                .append_relay(
                    "bin.000001",
                    100,
                    None,
                    &RelayOp::WriteRows {
                        database: "test".into(),
                        table: "t".into(),
                        rows: vec![vec![Value::Int(1), Value::Int(10)]],
                    },
                )
                .unwrap();
            runner
                .append_relay(
                    "bin.000001",
                    180,
                    None,
                    &RelayOp::XaPrepare {
                        xid: xid.clone(),
                        source_ts: 0,
                    },
                )
                .unwrap();
            let durable = runtime.relay_gate.lock().unwrap().durable_offset;
            runner.sql_apply_available(durable).unwrap();
            let QueryResponse::ResultSet(rows) = engine
                .execute("SELECT * FROM t")
                .unwrap()
                .materialize()
                .unwrap()
            else {
                panic!()
            };
            assert!(
                rows.rows.is_empty(),
                "prepared source writes must be invisible"
            );
            assert_eq!(
                engine.database_for_replication().list_prepared_xa().len(),
                1
            );
            runtime.status.lock().unwrap().relay_log_pos = 0;
            runner.sql_apply_available(durable).unwrap();
        }
        {
            let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
            let (_runtime, runner) = make_split_runner(dir.path(), engine.clone());
            assert_eq!(
                engine.database_for_replication().list_prepared_xa().len(),
                1
            );
            runner.apply_xa_finish(&xid, commit).unwrap();
            // Outcome durable but cursor still at PREPARE: keep receipts,
            // then reopen and replay the exact completion boundary.
        }
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        let (runtime, runner) = make_split_runner(dir.path(), engine.clone());
        runtime.status.lock().unwrap().relay_log_pos =
            fs::metadata(relay_log_path(dir.path(), DEFAULT_CHANNEL))
                .unwrap()
                .len();
        runner
            .append_relay(
                "bin.000001",
                240,
                None,
                &RelayOp::XaFinish {
                    xid: xid.clone(),
                    commit,
                    source_ts: 0,
                },
            )
            .unwrap();
        let durable = runtime.relay_gate.lock().unwrap().durable_offset;
        runner.sql_apply_available(durable).unwrap();
        let QueryResponse::ResultSet(rows) = engine
            .execute("SELECT * FROM t")
            .unwrap()
            .materialize()
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(rows.rows.len(), usize::from(commit));
        assert!(engine
            .database_for_replication()
            .list_prepared_xa()
            .is_empty());
        assert_eq!(runtime.status.lock().unwrap().relay_log_pos, durable);
    }
}

#[test]
fn audit1028_xa_binary_prepare_validates_lengths_and_one_phase() {
    for one_phase in [0, 1] {
        let mut data = vec![one_phase];
        data.extend_from_slice(&(-1_i32).to_le_bytes());
        data.extend_from_slice(&2_u32.to_le_bytes());
        data.extend_from_slice(&1_u32.to_le_bytes());
        data.extend_from_slice(&[0, 255, 128]);
        let (one, xid) = decode_xa_prepare_event(&data).unwrap();
        assert_eq!(one, one_phase != 0);
        assert_eq!(
            xid,
            crate::xa::Xid::new(u32::MAX as i64, vec![0, 255], vec![128]).unwrap()
        );
        data.pop();
        assert!(decode_xa_prepare_event(&data).is_err());
    }
    let (_, empty) = decode_xa_prepare_event(&[0; 13]).unwrap();
    assert!(empty.gtrid.is_empty() && empty.bqual.is_empty());
    let mut invalid = [0; 13];
    invalid[5] = 65;
    assert!(decode_xa_prepare_event(&invalid).is_err());
    assert!(decode_xa_prepare_event(&[255; 13]).is_err());
}

#[test]
fn audit1028_running_channel_change_source_uses_channel_must_stop() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
    let (runtime, _) = make_split_runner(dir.path(), engine.clone());
    let (send, recv) = std::sync::mpsc::channel();
    *runtime.io_join.lock().unwrap() = Some(thread::spawn(move || {
        recv.recv().unwrap();
    }));
    let manager = ReplicationManager {
        data_dir: dir.path().to_path_buf(),
        engine,
        channels: Mutex::new(HashMap::from([(
            DEFAULT_CHANNEL.to_owned(),
            runtime.clone(),
        )])),
    };
    let cfg = runtime.config.lock().unwrap().clone();
    let result = manager.change_source(cfg);
    send.send(()).unwrap();
    runtime
        .io_join
        .lock()
        .unwrap()
        .take()
        .unwrap()
        .join()
        .unwrap();
    assert_eq!(result.unwrap_err().code(), 3081);
}

#[test]
fn audit1028_json_native_decode_and_reexport_preserve_rich_scalars() {
    use crate::engine::json::{JsonType, JsonbRef};
    use mysql_common::binlog::jsonb::{OpaqueValue, Value as J};
    use mysql_common::io::ParseBuf;
    use mysql_common::proto::MyDeserialize;
    let text = "99999999999999999999999999999999999.123456789012345678901234567890";
    let decimal = mysql_common::binlog::decimal::Decimal::from_str(text).unwrap();
    let mut packed = vec![65, 30];
    decimal.write_bin(&mut packed).unwrap();
    for (ty, data, expected) in [
        (WireType::MYSQL_TYPE_NEWDECIMAL, packed, "DECIMAL"),
        (
            WireType::MYSQL_TYPE_DATE,
            0_i64.to_le_bytes().to_vec(),
            "DATE",
        ),
        (
            WireType::MYSQL_TYPE_TIME,
            0_i64.to_le_bytes().to_vec(),
            "TIME",
        ),
        (
            WireType::MYSQL_TYPE_DATETIME,
            0_i64.to_le_bytes().to_vec(),
            "DATETIME",
        ),
        (
            WireType::MYSQL_TYPE_TIMESTAMP,
            0_i64.to_le_bytes().to_vec(),
            "TIMESTAMP",
        ),
        (WireType::MYSQL_TYPE_VARCHAR, vec![0, 255, 128, 1], "BLOB"),
        (WireType::MYSQL_TYPE_BIT, vec![255], "BIT"),
    ] {
        let source = J::Opaque(OpaqueValue::new(ty, data));
        let value = decode_jsonb_to_mydb(&source).unwrap();
        let Value::Json(bytes) = &value else { panic!() };
        let node = JsonbRef::new(bytes).unwrap();
        assert_eq!(node.type_name(), expected);
        if node.json_type() == JsonType::Decimal {
            assert_eq!(value.to_display_string(), text);
            // Source decimal_t keeps leading base-10 group padding that
            // makes this valid 65-digit SQL value advertise precision 66.
            let J::Opaque(op) = &source else { panic!() };
            let mut padded = op.data_raw().to_vec();
            padded[0] = 66;
            assert_eq!(
                decode_jsonb_to_mydb(&J::Opaque(OpaqueValue::new(ty, padded))).unwrap(),
                value
            );
        }
        if node.json_type() == JsonType::Opaque {
            let (ty, data) = node.opaque_value().unwrap();
            assert_eq!(
                crate::engine::json::unquote_text(bytes),
                crate::engine::json::opaque_text(ty, data)
            );
        }
        let wire = crate::engine::json::mysql_binary::encode(bytes).unwrap();
        let redecoded = J::deserialize((), &mut ParseBuf(&wire)).unwrap();
        assert_eq!(redecoded, source);
        let base = Value::Json(crate::engine::json::parse_text("{}", 1, "test").unwrap());
        let changed = crate::engine::expr::apply_replication_json_diffs(
            &base,
            &[(1, "$.x".into(), Some(bytes.clone()))],
        )
        .unwrap();
        let Value::Json(changed) = changed else {
            panic!()
        };
        assert_eq!(
            JsonbRef::new(&changed)
                .unwrap()
                .object_iter()
                .find(|(key, _)| *key == "x")
                .unwrap()
                .1
                .type_name(),
            expected
        );
    }
}

#[test]
fn audit1028_ddl_progress_failure_preserves_recovery_boundary() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
    let (runtime, runner) = make_split_runner(dir.path(), engine.clone());
    runner
        .append_relay(
            "bin.000001",
            100,
            None,
            &RelayOp::Ddl {
                source_ts: 0,
                database: "test".into(),
                sql: "CREATE TABLE t(id INT)".into(),
                session: Default::default(),
                transactional: false,
            },
        )
        .unwrap();
    let durable = runtime.relay_gate.lock().unwrap().durable_offset;
    let metadata = channel_path(dir.path(), DEFAULT_CHANNEL);
    fs::create_dir_all(&metadata).unwrap();
    assert!(runner.sql_apply_available(durable).is_err());
    assert_eq!(runtime.status.lock().unwrap().relay_log_pos, 0);
    fs::remove_dir(&metadata).unwrap();
    runner.sql_apply_available(durable).unwrap();
    assert_eq!(runtime.status.lock().unwrap().relay_log_pos, durable);
}

#[test]
fn audit1028_json_opaque_order_remains_transitive_across_scalar_types() {
    use crate::engine::json::{compare, from_serde, opaque_scalar, typed_text_scalar, JsonType};
    let values = [
        from_serde(&serde_json::json!("x")),
        opaque_scalar(15, b"x"),
        opaque_scalar(252, b"x"),
        typed_text_scalar(JsonType::Date, "2026-01-15").unwrap(),
    ];
    for a in &values {
        for b in &values {
            assert_eq!(compare(a, b), compare(b, a).reverse());
            for c in &values {
                if compare(a, b).is_le() && compare(b, c).is_le() {
                    assert!(compare(a, c).is_le());
                }
            }
        }
    }
}

#[test]
fn audit1028_source_xa_identifier_bounds_preserve_empty_and_unsigned_ids() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
    let (_, runner) = make_split_runner(dir.path(), engine);
    let mut txn = TxnBuffer::default();
    runner
        .handle_xa_query("XA START X'',X'',4294967295", "bin.000001", 100, &mut txn)
        .unwrap();
    assert_eq!(txn.xa_xid.as_ref().unwrap().format_id, u32::MAX as i64);
    assert!(txn.xa_xid.as_ref().unwrap().gtrid.is_empty());
    runner
        .handle_xa_query("XA END X'',X'',4294967295", "bin.000001", 120, &mut txn)
        .unwrap();
    assert!(txn.xa_ended);
    assert!(runner
        .handle_xa_query(
            "XA START 'bad','',4294967296",
            "bin.000001",
            140,
            &mut TxnBuffer::default()
        )
        .is_err());
    take_io_error_fatal();
}

#[test]
fn audit1028_keyless_update_ignores_tombstones_and_virtual_placeholders() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = StorageQueryEngine::open(dir.path()).unwrap();
    engine.execute("CREATE TABLE t(id INT,gone INT,v VARCHAR(20),g INT AS(id+1) VIRTUAL,s INT AS(id*2) STORED)").unwrap();
    engine
        .execute("INSERT INTO t(id,gone,v) VALUES(1,10,'abc')")
        .unwrap();
    engine.execute("ALTER TABLE t DROP COLUMN gone").unwrap();
    let db = engine.database_for_replication();
    let txn = db.begin_txn().unwrap();
    let table = db.table("test.t").unwrap();
    let before = vec![
        Value::Int(1),
        Value::Null,
        Value::String("abc".into()),
        Value::Null,
        Value::Int(2),
    ];
    let after = vec![
        Value::Int(3),
        Value::Null,
        Value::String("updated".into()),
        Value::Null,
        Value::Int(6),
    ];
    assert!(find_matching_row(db, txn, &table, &before)
        .unwrap()
        .is_some());
    apply_update_rows(db, txn, "test", "t", &[RowUpdate { before, after }]).unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT id,v,g,s FROM t")
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(
        rows.rows,
        vec![vec![
            Value::Int(3),
            Value::String("updated".into()),
            Value::Int(4),
            Value::Int(6)
        ]]
    );
}
