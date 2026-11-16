#[test]
fn release_audit_binlog_none_fde_keeps_checksum_descriptor_and_trailer() {
    use mysql_common::binlog::{consts::BinlogVersion, events::EventData, EventStreamReader};
    let config = BinlogConfig {
        checksum: BinlogChecksum::None,
        ..Default::default()
    };
    let bytes = encode_format_description_event(&config, 4);
    assert_eq!(bytes[bytes.len() - 5], 0);
    assert_eq!(
        crc32fast::hash(&bytes[..bytes.len() - 4]),
        u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().unwrap())
    );
    let mut reader = EventStreamReader::new(BinlogVersion::Version4);
    let event = reader.read(std::io::Cursor::new(bytes)).unwrap().unwrap();
    let EventData::FormatDescriptionEvent(fde) = event.read_data().unwrap().unwrap() else {
        panic!()
    };
    assert_eq!(fde.event_type_header_lengths().len(), 41);
    let query = encode_event(
        &config,
        QUERY_EVENT,
        126,
        &encode_query_payload_with("test", "CREATE TABLE t(id INT)", &Default::default()).unwrap(),
        false,
    );
    let event = reader.read(std::io::Cursor::new(query)).unwrap().unwrap();
    assert!(matches!(
        event.read_data().unwrap(),
        Some(EventData::QueryEvent(_))
    ));
    assert!(event.checksum().is_none());
}

#[test]
fn release_audit_binlog_row_chunks_keep_foreign_key_flags_after_session_ends() {
    let table = {
        let _fk = crate::storage::FkChecksGuard::new(false);
        binlog_table_from_schema(&test_table()).unwrap()
    };
    assert!(crate::storage::fk_checks_enabled());
    let rows: Vec<_> = (0..5)
        .map(|id| vec![Value::Int(id), Value::String("x".repeat(700))])
        .collect();
    for kind in [RowsPayload::Write(&rows), RowsPayload::Delete(&rows)] {
        let payloads = encode_rows_payloads(&table, kind, 1024).unwrap();
        assert!(payloads.len() > 1);
        for (index, payload) in payloads.iter().enumerate() {
            assert_eq!(
                u16::from_le_bytes(payload[6..8].try_into().unwrap()),
                2 | u16::from(index + 1 == payloads.len())
            );
        }
    }
    let map = encode_table_map_payload(&table);
    assert_eq!(u16::from_le_bytes(map[6..8].try_into().unwrap()), 0);
}

#[tokio::test]
async fn release_audit_binlog_replacement_cancels_only_previous_registration() {
    std::fs::create_dir_all("/tmp/mydb/audit-fixes/tests").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb/audit-fixes/tests").unwrap();
    let manager =
        Arc::new(BinlogSourceManager::open(dir.path().into(), test_config(dir.path())).unwrap());
    let first = manager.register_replica(semi_sync_replica(42)).unwrap();
    let notify = first.cancellation();
    let second = manager.register_replica(semi_sync_replica(42)).unwrap();
    tokio::time::timeout(Duration::from_millis(100), notify.notified())
        .await
        .unwrap();
    drop(first);
    assert_eq!(manager.registered_replicas().len(), 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), second.cancellation().notified())
            .await
            .is_err()
    );
    drop(second);
    assert!(manager.registered_replicas().is_empty());
}
