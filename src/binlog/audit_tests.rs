//! Regression coverage for publication, recovery and protocol boundaries.
use super::tests::{stage_test_write, test_config};
use super::*;

fn directory() -> tempfile::TempDir {
    fs::create_dir_all("/tmp/mydb").unwrap();
    tempfile::tempdir_in("/tmp/mydb").unwrap()
}

#[test]
fn audit_staged_positions_are_not_public() {
    let dir = directory();
    let manager = BinlogSourceManager::open(dir.path().into(), test_config(dir.path())).unwrap();
    let before = manager.current_position().unwrap();
    let commit = stage_test_write(&manager, 1);
    assert!(commit.reference.end_pos > before.position);
    assert_eq!(
        manager.current_position().unwrap().position,
        before.position
    );
    assert_eq!(manager.binary_logs().unwrap()[0].size, before.position);
    let heartbeat = manager.heartbeat_event().unwrap();
    assert_eq!(
        u32::from_le_bytes(heartbeat[13..17].try_into().unwrap()) as u64,
        before.position
    );
    assert!(manager
        .events_after(&before.file, commit.reference.end_pos)
        .is_err());
    manager.sync_staged_commit(&commit).unwrap();
    manager.commit_order.finish_wal(commit.order);
    let end = commit.reference.end_pos;
    manager.publish_txn_commit(Some(commit)).unwrap();
    assert_eq!(manager.current_position().unwrap().position, end);
}

#[test]
fn audit_live_and_cold_reject_positions_inside_events() {
    for cold in [false, true] {
        let dir = directory();
        let mut manager =
            BinlogSourceManager::open(dir.path().into(), test_config(dir.path())).unwrap();
        let position = manager.current_position().unwrap();
        if cold {
            manager.checkpoint_manifest().unwrap();
            drop(manager);
            manager =
                BinlogSourceManager::open(dir.path().into(), test_config(dir.path())).unwrap();
        }
        for budget in [0, 1] {
            assert!(
                manager
                    .events_after_bounded(&position.file, 5, budget, 1024)
                    .is_err(),
                "cold={cold}, budget={budget}"
            );
        }
    }
}

#[test]
fn audit_purge_does_not_checkpoint_unpublished_tail() {
    let dir = directory();
    let config = test_config(dir.path());
    let manager = BinlogSourceManager::open(dir.path().into(), config.clone()).unwrap();
    manager.rotate().unwrap();
    let published = manager.current_position().unwrap();
    let commit = stage_test_write(&manager, 1);
    manager.sync_staged_commit(&commit).unwrap();
    manager.purge_to(&published.file).unwrap();
    drop(commit);
    drop(manager);
    let reopened = BinlogSourceManager::open(dir.path().into(), config).unwrap();
    assert_eq!(
        reopened.current_position().unwrap().position,
        published.position
    );
    assert!(!reopened
        .events_after(&published.file, 4)
        .unwrap()
        .iter()
        .any(|e| e.event_type == GTID_EVENT));
}

#[test]
fn audit_cold_reader_rejects_payload_corruption() {
    let dir = directory();
    let config = test_config(dir.path());
    let manager = BinlogSourceManager::open(dir.path().into(), config.clone()).unwrap();
    let commit = stage_test_write(&manager, 1);
    manager.sync_staged_commit(&commit).unwrap();
    manager.commit_order.finish_wal(commit.order);
    manager.publish_txn_commit(Some(commit)).unwrap();
    let pos = manager.current_position().unwrap();
    let row = manager
        .events_after(&pos.file, 4)
        .unwrap()
        .into_iter()
        .find(|e| e.event_type == WRITE_ROWS_EVENT_V2)
        .unwrap();
    manager.checkpoint_manifest().unwrap();
    drop(manager);
    let path = dir.path().join("binlog").join(&pos.file);
    let mut bytes = fs::read(&path).unwrap();
    bytes[row.end_pos as usize - 5] ^= 0x80;
    fs::write(&path, bytes).unwrap();
    let reopened = BinlogSourceManager::open(dir.path().into(), config).unwrap();
    assert!(reopened.events_after(&pos.file, 4).is_err());
    assert!(disk::validate_range(&path, row.start_pos, row.end_pos).is_err());
}

#[test]
fn audit_missing_manifest_cannot_republish_unknown_transactions() {
    let dir = directory();
    let config = test_config(dir.path());
    let manager = BinlogSourceManager::open(dir.path().into(), config.clone()).unwrap();
    let commit = stage_test_write(&manager, 1);
    manager.sync_staged_commit(&commit).unwrap();
    drop(commit);
    drop(manager);
    let path = dir.path().join("binlog/manifest.json");
    fs::remove_file(path).unwrap();
    assert!(BinlogSourceManager::open(dir.path().into(), config).is_err());
}

#[test]
fn audit_sid_block_rejects_invalid_intervals_and_trailing_bytes() {
    for (start, end) in [
        (0u64, 2u64),
        (2, 2),
        (3, 2),
        (i64::MAX as u64, i64::MAX as u64 + 1),
    ] {
        let mut bytes = 1u64.to_le_bytes().to_vec();
        bytes.extend_from_slice(&[0xaa; 16]);
        bytes.extend_from_slice(&1u64.to_le_bytes());
        bytes.extend_from_slice(&start.to_le_bytes());
        bytes.extend_from_slice(&end.to_le_bytes());
        assert!(
            parse_sid_block_to_gtid_set(&bytes).is_err(),
            "{start}..{end}"
        );
    }
    let mut bytes = 0u64.to_le_bytes().to_vec();
    bytes.push(0);
    assert!(parse_sid_block_to_gtid_set(&bytes).is_err());
}

#[test]
fn audit_purge_before_epoch_does_not_delete_recent_logs() {
    let dir = directory();
    let manager = BinlogSourceManager::open(dir.path().into(), test_config(dir.path())).unwrap();
    manager.rotate().unwrap();
    let count = manager.binary_logs().unwrap().len();
    manager.purge_before("1960-01-01 00:00:00").unwrap();
    assert_eq!(manager.binary_logs().unwrap().len(), count);
}

#[test]
fn audit_direct_append_failure_does_not_publish_events() {
    let dir = directory();
    let manager = BinlogSourceManager::open(dir.path().into(), test_config(dir.path())).unwrap();
    let before = manager.current_position().unwrap();
    let path = manager.dir.join(&before.file);
    fs::remove_file(&path).unwrap();
    fs::create_dir(&path).unwrap();
    assert!(manager.rotate().is_err());
    assert_eq!(
        manager.current_position().unwrap().position,
        before.position
    );
    assert!(!manager
        .events_after(&before.file, 4)
        .unwrap()
        .iter()
        .any(|e| e.event_type == ROTATE_EVENT));
}

#[test]
fn audit_semi_sync_wait_no_replica_on_waits_for_timeout() {
    let dir = directory();
    let mut config = test_config(dir.path());
    config.semi_sync_source_enabled = true;
    config.semi_sync_source_timeout = 20;
    config.semi_sync_source_wait_no_replica = true;
    let manager = BinlogSourceManager::open(dir.path().into(), config).unwrap();
    assert_eq!(
        manager.semi_sync_wait("bin.000001", 100),
        SemiSyncOutcome::TimedOut
    );
}

#[test]
fn audit_semi_sync_off_degrades_when_quorum_is_unavailable() {
    let dir = directory();
    let mut config = test_config(dir.path());
    config.semi_sync_source_enabled = true;
    config.semi_sync_source_timeout = 20;
    config.semi_sync_source_wait_no_replica = false;
    config.semi_sync_source_wait_for_replica_count = 2;
    let manager = Arc::new(BinlogSourceManager::open(dir.path().into(), config).unwrap());
    let _guard = manager
        .register_replica(super::tests::semi_sync_replica(9))
        .unwrap();
    manager.semi_sync_mark_replica(9);
    assert_eq!(
        manager.semi_sync_wait("bin.000001", 100),
        SemiSyncOutcome::NoReplicaDegrade
    );
}

#[test]
fn audit_gtid_exhaustion_does_not_reuse_last_sequence() {
    let dir = directory();
    let manager = BinlogSourceManager::open(dir.path().into(), test_config(dir.path())).unwrap();
    let mut state = manager.state.lock();
    state.next_gno = i64::MAX as u64;
    assert!(manager.next_gtid_locked(&mut state).is_err());
}

#[test]
fn audit_checksum_change_cannot_mix_algorithms_in_active_file() {
    let dir = directory();
    let config = test_config(dir.path());
    let manager = BinlogSourceManager::open(dir.path().into(), config.clone()).unwrap();
    manager.checkpoint_manifest().unwrap();
    drop(manager);
    let mut changed = config;
    changed.checksum = BinlogChecksum::None;
    assert!(BinlogSourceManager::open(dir.path().into(), changed).is_err());
}

#[test]
fn audit_sync_binlog_counts_commit_groups() {
    let dir = directory();
    let mut config = test_config(dir.path());
    config.sync_binlog = 3;
    let manager = BinlogSourceManager::open(dir.path().into(), config).unwrap();
    for id in 1..=3 {
        let commit = stage_test_write(&manager, id);
        manager.sync_staged_commit(&commit).unwrap();
        manager.commit_order.finish_wal(commit.order);
        manager.publish_txn_commit(Some(commit)).unwrap();
        let elapsed = manager.group_sync.sync_total_us.load(Ordering::Relaxed);
        if id < 3 {
            assert_eq!(elapsed, 0, "group {id} must not trigger fsync");
        } else {
            assert!(elapsed > 0);
        }
    }
}

#[test]
fn audit_restart_reports_the_persisted_server_uuid() {
    let dir = directory();
    let config = test_config(dir.path());
    let expected = config.server_uuid.clone();
    init_global(dir.path().into(), config.clone()).unwrap();
    let mut restarted = config;
    restarted.server_uuid = "00000000-0000-4000-8000-000000000099".into();
    init_global(dir.path().into(), restarted).unwrap();
    assert_eq!(require_manager().unwrap().config.server_uuid, expected);
    assert_eq!(config_snapshot().server_uuid, expected);
}

#[test]
fn audit_reset_bootstrap_keeps_one_checksum_and_correct_descriptor() {
    for checksum in [BinlogChecksum::None, BinlogChecksum::Crc32] {
        let dir = directory();
        let mut config = test_config(dir.path());
        config.checksum = checksum;
        let manager = BinlogSourceManager::open(dir.path().into(), config).unwrap();
        manager
            .reset_master(|| test_reset_wal_boundary(&manager))
            .unwrap();
        let events = manager.events_after("", 4).unwrap();
        let fde = &events[0].bytes;
        assert_eq!(fde[fde.len() - 5], checksum.descriptor_byte());
        assert_eq!(
            fde.len(),
            encode_format_description_event(&manager.config, 4).len()
        );
        let pos = manager.current_position().unwrap();
        disk::validate_range(&manager.dir.join(&pos.file), 4, pos.position).unwrap();
    }
}

#[test]
fn audit_text_rows_use_declared_charset_bytes_and_width() {
    let mut schema = super::tests::test_table();
    schema.columns[1].data_type = DataTypeKind::VarChar { len: Some(100) };
    schema.columns[1].charset = Some("latin1".into());
    schema.columns[1].collation = Some("latin1_swedish_ci".into());
    let table = binlog_table_from_schema(&schema).unwrap();
    let column = &table.columns[1];
    assert_eq!(column.metadata, 100u16.to_le_bytes());
    let mut encoded = Vec::new();
    encode_value(column, &Value::String("café".into()), &mut encoded).unwrap();
    assert_eq!(encoded, b"\x04caf\xe9");
}

#[test]
fn audit_text_charsets_cover_char_varchar_and_text_without_changing_binary() {
    for (charset, text, bytes) in [
        ("latin1", "café", b"caf\xe9".as_slice()),
        ("gbk", "中", b"\xd6\xd0".as_slice()),
        ("utf16", "中", b"\x4e\x2d".as_slice()),
        ("ucs2", "中", b"\x4e\x2d".as_slice()),
        ("utf32", "中", b"\x00\x00\x4e\x2d".as_slice()),
        ("utf8mb3", "中", b"\xe4\xb8\xad".as_slice()),
        ("utf8mb4", "😀", b"\xf0\x9f\x98\x80".as_slice()),
        ("ascii", "a\0z", b"a\0z".as_slice()),
    ] {
        for data_type in [
            DataTypeKind::Char { len: Some(4) },
            DataTypeKind::VarChar { len: Some(4) },
            DataTypeKind::Text { max_len: 65535 },
        ] {
            let mut schema = super::tests::test_table();
            schema.columns[1].data_type = data_type;
            schema.columns[1].charset = Some(charset.into());
            schema.columns[1].collation = Some(
                crate::charset::resolve_charset(charset)
                    .unwrap()
                    .default_collation
                    .into(),
            );
            let table = binlog_table_from_schema(&schema).unwrap();
            let column = &table.columns[1];
            let mut encoded = Vec::new();
            encode_value(column, &Value::String(text.into()), &mut encoded).unwrap();
            let prefix = if column.column_type == MYSQL_TYPE_BLOB {
                2
            } else {
                1
            };
            assert_eq!(
                &encoded[prefix..],
                bytes,
                "{charset}, {:?}",
                schema.columns[1].data_type
            );
            assert_eq!(encoded[0] as usize, bytes.len());
        }
    }
    let mut schema = super::tests::test_table();
    schema.columns[1].data_type = DataTypeKind::VarBinary { len: Some(4) };
    let table = binlog_table_from_schema(&schema).unwrap();
    let mut encoded = Vec::new();
    encode_value(
        &table.columns[1],
        &Value::Bytes(vec![0xff, 0, 0x80]),
        &mut encoded,
    )
    .unwrap();
    assert_eq!(encoded, [3, 0xff, 0, 0x80]);
}

#[test]
fn audit_varchar_rejects_length_prefix_overflow() {
    let mut schema = super::tests::test_table();
    schema.columns[1].data_type = DataTypeKind::VarBinary { len: Some(255) };
    let table = binlog_table_from_schema(&schema).unwrap();
    let mut encoded = vec![0xaa];
    assert!(encode_value(&table.columns[1], &Value::Bytes(vec![0; 256]), &mut encoded).is_err());
    assert_eq!(encoded, [0xaa]);
}

#[test]
fn audit_show_query_without_checksum_preserves_sql_suffix() {
    let config = BinlogConfig {
        checksum: BinlogChecksum::None,
        ..BinlogConfig::default()
    };
    let sql = "CREATE TABLE example (id INT)";
    let bytes = encode_event(
        &config,
        QUERY_EVENT,
        4,
        &encode_query_payload(1, "test", sql),
        false,
    );
    let chunk = BinlogEventChunk {
        file: "bin.000001".into(),
        start_pos: 4,
        end_pos: 4 + bytes.len() as u64,
        event_type: QUERY_EVENT,
        gtid: None,
        bytes: Arc::new(bytes),
    };
    assert_eq!(format_binlog_event_info(&chunk, config.checksum), sql);
}

#[test]
fn audit_query_event_sql_uses_the_advertised_client_charset() {
    let context = BinlogQueryContext {
        client_charset_id: 8,
        connection_collation_id: 8,
        ..BinlogQueryContext::default()
    };
    let payload =
        encode_query_payload_with("test", "CREATE TABLE t(id INT) COMMENT='café'", &context)
            .unwrap();
    let offset =
        13 + u16::from_le_bytes([payload[11], payload[12]]) as usize + payload[8] as usize + 1;
    assert_eq!(
        &payload[offset..],
        b"CREATE TABLE t(id INT) COMMENT='caf\xe9'"
    );
    let config = BinlogConfig::default();
    let bytes = encode_event(&config, QUERY_EVENT, 4, &payload, false);
    let chunk = BinlogEventChunk {
        file: "bin.000001".into(),
        start_pos: 4,
        end_pos: 4 + bytes.len() as u64,
        event_type: QUERY_EVENT,
        gtid: None,
        bytes: Arc::new(bytes),
    };
    assert_eq!(
        format_binlog_event_info(&chunk, config.checksum),
        "CREATE TABLE t(id INT) COMMENT='café'"
    );
    assert!(encode_query_payload_with("test", "SELECT '中文'", &context).is_err());
    assert!(encode_query_payload_with(&"x".repeat(256), "SELECT 1", &context).is_err());
}

#[test]
fn audit_gtid_filter_tracks_transactions_across_batches_and_rotation() {
    let dir = directory();
    let manager = BinlogSourceManager::open(dir.path().into(), test_config(dir.path())).unwrap();
    let mut gtids = Vec::new();
    for id in 1..=6 {
        let commit = stage_test_write(&manager, id);
        gtids.push(commit.reference.gtid.clone().unwrap());
        manager.sync_staged_commit(&commit).unwrap();
        manager.commit_order.finish_wal(commit.order);
        manager.publish_txn_commit(Some(commit)).unwrap();
        if id == 3 {
            manager.rotate().unwrap();
        }
    }
    let excluded = [gtids[1].as_str(), gtids[3].as_str(), gtids[5].as_str()].join(",");
    let mut filter = BinlogGtidFilter::new(&excluded);
    let first = manager.binary_logs().unwrap()[0].name.clone();
    let mut cursor = (first, 4);
    let mut received = Vec::new();
    let mut rows = 0;
    loop {
        let batch = manager
            .events_after_bounded(&cursor.0, cursor.1, 2, 1024)
            .unwrap();
        if batch.is_empty() {
            break;
        }
        for event in batch {
            cursor = (event.file.clone(), event.end_pos);
            if filter.should_send(&event) {
                if let Some(gtid) = event.gtid {
                    received.push(gtid);
                }
                rows += usize::from(event.event_type == WRITE_ROWS_EVENT_V2);
            }
        }
    }
    assert_eq!(
        received,
        [gtids[0].clone(), gtids[2].clone(), gtids[4].clone()]
    );
    assert_eq!(rows, 3);
}

#[test]
fn audit_gtid_flags_distinguish_row_only_and_ddl_transactions() {
    let dir = directory();
    let manager = BinlogSourceManager::open(dir.path().into(), test_config(dir.path())).unwrap();
    let row = stage_test_write(&manager, 1);
    assert_eq!(row.chunks[0].bytes[19], 0);
    manager.sync_staged_commit(&row).unwrap();
    manager.commit_order.finish_wal(row.order);
    manager.publish_txn_commit(Some(row)).unwrap();
    let ddl = manager
        .stage_encoded_txn_commit(
            vec![(
                QUERY_EVENT,
                encode_query_payload(1, "test", "CREATE TABLE t(id INT)"),
                false,
            )],
            2,
        )
        .unwrap();
    assert_eq!(ddl.chunks[0].bytes[19], 1);
    assert_eq!(
        ddl.chunks.iter().map(|e| e.event_type).collect::<Vec<_>>(),
        [GTID_EVENT, QUERY_EVENT]
    );
    manager.sync_staged_commit(&ddl).unwrap();
    manager.commit_order.finish_wal(ddl.order);
    manager.publish_txn_commit(Some(ddl)).unwrap();
    manager
        .append_query_event(
            "test",
            "CREATE TABLE direct(id INT)",
            &BinlogQueryContext::default(),
        )
        .unwrap();
    let events = manager.events_after("", 4).unwrap();
    assert_eq!(
        events
            .iter()
            .rev()
            .find(|e| e.event_type == GTID_EVENT)
            .unwrap()
            .bytes[19],
        1
    );
    let empty_ctas = manager
        .stage_encoded_txn_commit(
            vec![(
                QUERY_EVENT,
                encode_query_payload(
                    1,
                    "test",
                    "CREATE TABLE empty_ctas(id INT) START TRANSACTION",
                ),
                false,
            )],
            3,
        )
        .unwrap();
    assert_eq!(
        empty_ctas
            .chunks
            .iter()
            .map(|e| e.event_type)
            .collect::<Vec<_>>(),
        [GTID_EVENT, QUERY_EVENT, QUERY_EVENT, XID_EVENT]
    );
    assert_eq!(empty_ctas.chunks[0].bytes[19], 1);
    manager.sync_staged_commit(&empty_ctas).unwrap();
    manager.commit_order.finish_wal(empty_ctas.order);
    manager.publish_txn_commit(Some(empty_ctas)).unwrap();
}

#[test]
fn audit_abandoned_staged_commit_cannot_leave_a_writable_position_gap() {
    let dir = directory();
    let manager = BinlogSourceManager::open(dir.path().into(), test_config(dir.path())).unwrap();
    let commit = stage_test_write(&manager, 1);
    drop(commit);
    assert!(manager.stage_encoded_txn_commit(Vec::new(), 2).is_err());
}

#[test]
fn audit_abandoned_durable_commit_wakes_order_waiters_with_errors() {
    let dir = directory();
    let manager = BinlogSourceManager::open(dir.path().into(), test_config(dir.path())).unwrap();
    let first = stage_test_write(&manager, 1);
    let second = stage_test_write(&manager, 2);
    manager.sync_staged_commit(&first).unwrap();
    let (send, receive) = std::sync::mpsc::channel();
    let order = Arc::clone(&manager.commit_order);
    let second_order = second.order;
    let waiter = std::thread::spawn(move || {
        send.send(order.wait_wal(second_order).is_err()).unwrap();
        send.send(order.wait_publish(second_order).is_err())
            .unwrap();
    });
    drop(first);
    assert!(receive.recv_timeout(Duration::from_secs(2)).unwrap());
    assert!(receive.recv_timeout(Duration::from_secs(2)).unwrap());
    waiter.join().unwrap();
    assert!(manager.sync_staged_commit(&second).is_err());
}

#[test]
fn audit_none_preamble_rotate_has_no_checksum_tail() {
    let dir = directory();
    let mut config = test_config(dir.path());
    config.checksum = BinlogChecksum::None;
    let manager = BinlogSourceManager::open(dir.path().into(), config).unwrap();
    let file = manager.current_position().unwrap().file;
    for pos in [4, 100] {
        let preamble = manager.dump_preamble(&file, pos, BinlogChecksum::None);
        assert_eq!(&preamble[0][27..], file.as_bytes());
        if pos > 4 {
            let fde = &preamble[1];
            assert_eq!(fde[fde.len() - 5], 0);
            assert_eq!(
                crc32fast::hash(&fde[..fde.len() - 4]),
                u32::from_le_bytes(fde[fde.len() - 4..].try_into().unwrap())
            );
        }
    }
}

#[test]
fn blackbox_fake_rotate_uses_negotiation_before_source_fde() {
    let dir = directory();
    let manager = BinlogSourceManager::open(dir.path().into(), test_config(dir.path())).unwrap();
    let file = manager.current_position().unwrap().file;
    for position in [4, 100] {
        let plain = manager.dump_preamble(&file, position, BinlogChecksum::None);
        let crc = manager.dump_preamble(&file, position, BinlogChecksum::Crc32);
        assert_eq!(&plain[0][27..], file.as_bytes());
        assert_eq!(&crc[0][27..crc[0].len() - 4], file.as_bytes());
        assert_eq!(crc[0].len(), plain[0].len() + 4);
        assert_eq!(
            crc32fast::hash(&crc[0][..crc[0].len() - 4]),
            u32::from_le_bytes(crc[0][crc[0].len() - 4..].try_into().unwrap())
        );
        if position > 4 {
            assert_eq!(plain[1], crc[1]);
            assert_eq!(plain[1][plain[1].len() - 5], 1);
        }
    }
}

#[test]
fn audit_large_direct_ddl_obeys_cache_budget_and_remains_readable() {
    let dir = directory();
    let manager = BinlogSourceManager::open(dir.path().into(), test_config(dir.path())).unwrap();
    let sql = format!(
        "CREATE VIEW v AS SELECT 1 /*{}*/",
        "x".repeat(5 * 1024 * 1024)
    );
    manager
        .append_query_event("test", &sql, &BinlogQueryContext::default())
        .unwrap();
    assert!(manager.state.lock().live_event_bytes <= 4 * 1024 * 1024);
    let events = manager.events_after("", 4).unwrap();
    let query = events.iter().find(|e| e.event_type == QUERY_EVENT).unwrap();
    assert_eq!(
        format_binlog_event_info(query, manager.config.checksum),
        sql
    );
}

#[test]
fn audit_unregistered_dump_reader_blocks_reset_and_releases_lease() {
    let dir = directory();
    let manager =
        Arc::new(BinlogSourceManager::open(dir.path().into(), test_config(dir.path())).unwrap());
    let generation = manager.reset_generation.load(Ordering::Acquire);
    let lease = manager.lease_dump();
    assert!(manager.registered_replicas().is_empty());
    assert!(manager
        .reset_master(|| test_reset_wal_boundary(&manager))
        .is_err());
    manager.check_read_generation(generation).unwrap();
    drop(lease);
    manager
        .reset_master(|| test_reset_wal_boundary(&manager))
        .unwrap();
    assert!(manager.check_read_generation(generation).is_err());
}

#[test]
fn audit_semi_sync_wait_rechecks_quorum_and_disconnects() {
    for lower_quorum in [false, true] {
        let dir = directory();
        let mut config = test_config(dir.path());
        config.semi_sync_source_enabled = true;
        config.semi_sync_source_timeout = 30_000;
        config.semi_sync_source_wait_no_replica = lower_quorum;
        config.semi_sync_source_wait_for_replica_count = if lower_quorum { 2 } else { 1 };
        let manager = Arc::new(BinlogSourceManager::open(dir.path().into(), config).unwrap());
        let guard = manager
            .register_replica(super::tests::semi_sync_replica(9))
            .unwrap();
        manager.semi_sync_mark_replica(9);
        if lower_quorum {
            manager.record_semi_sync_ack(9, "bin.000001", 100);
        }
        let worker = manager.clone();
        let (send, receive) = std::sync::mpsc::channel();
        let waiter = std::thread::spawn(move || {
            send.send(worker.semi_sync_wait("bin.000001", 100)).unwrap();
        });
        let deadline = Instant::now() + Duration::from_secs(2);
        while manager.semi_sync.wait_sessions.load(Ordering::Relaxed) == 0 {
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        }
        if lower_quorum {
            manager
                .semi_sync
                .wait_for_replica_count
                .store(1, Ordering::Relaxed);
            manager.semi_sync.notify_ack();
        } else {
            drop(guard);
        }
        assert_eq!(
            receive.recv_timeout(Duration::from_secs(2)).unwrap(),
            if lower_quorum {
                SemiSyncOutcome::Acked
            } else {
                SemiSyncOutcome::NoReplicaDegrade
            }
        );
        waiter.join().unwrap();
    }
}

#[test]
fn audit_recovery_rejects_crc_corruption_in_wal_committed_tail() {
    let dir = directory();
    let config = test_config(dir.path());
    let manager = BinlogSourceManager::open(dir.path().into(), config.clone()).unwrap();
    let commit = stage_test_write(&manager, 1);
    manager.sync_staged_commit(&commit).unwrap();
    let reference = commit.reference.clone();
    let row = commit
        .chunks
        .iter()
        .find(|e| e.event_type == WRITE_ROWS_EVENT_V2)
        .unwrap();
    let path = manager.dir.join(&reference.file);
    let mut bytes = fs::read(&path).unwrap();
    bytes[row.end_pos as usize - 5] ^= 1;
    fs::write(&path, bytes).unwrap();
    drop(commit);
    drop(manager);
    let mut wal = crate::wal::LogManager::open(dir.path()).unwrap();
    wal.append(
        1,
        crate::wal::WalPayload::CommitTxn {
            commit_scn: 1,
            commit_unix_us: 0,
            binlog: Some(reference),
        },
    )
    .unwrap();
    wal.flush().unwrap();
    drop(wal);
    assert!(BinlogSourceManager::open(dir.path().into(), config).is_err());
}

#[test]
#[ignore = "manual deterministic workload timing"]
fn audit_binlog_read_benchmark() {
    let dir = directory();
    let mut config = test_config(dir.path());
    config.sync_binlog = 0;
    let manager = BinlogSourceManager::open(dir.path().into(), config.clone()).unwrap();
    for id in 1..=600 {
        let commit = stage_test_write(&manager, id);
        manager.sync_staged_commit(&commit).unwrap();
        manager.commit_order.finish_wal(commit.order);
        manager.publish_txn_commit(Some(commit)).unwrap();
    }
    let executed = manager.state.lock().gtid_executed.clone();
    let expected = manager.current_position().unwrap();
    for cold in [false, true] {
        let started = Instant::now();
        for _ in 0..100 {
            let position = manager.position_for_gtid_dump("", 4, &executed).unwrap();
            assert_eq!(position.position, expected.position);
        }
        eprintln!(
            "gtid_lookup cold={cold} transactions=600 iterations=100 us={}",
            started.elapsed().as_micros()
        );
        let started = Instant::now();
        for _ in 0..30 {
            let events = manager.events_after("", 4).unwrap();
            assert_eq!(events.len(), 3002);
        }
        eprintln!(
            "read_all cold={cold} events=3002 iterations=30 us={}",
            started.elapsed().as_micros()
        );
        // Evict every live event to exercise identical bytes through the disk reader.
        let mut state = manager.state.lock();
        state
            .disk_prefixes
            .insert(expected.file.clone(), expected.position);
        state.events.clear();
        state.rebuild_file_index();
    }
}
