/// Fuzz ROW/FULL wire bodies with a coherent TABLE_MAP and event envelope.
/// Raw lengths are bounded before allocating, while semantic body errors pass
/// through the same decoders and conversions as the MySQL receiver.
pub(crate) fn fuzz_binlog_events(bytes: &[u8]) {
    use mysql_common::binlog::consts::{EventFlags, EventType};
    use mysql_common::binlog::events::{BinlogEventHeader, EventData, OptionalMetaExtractor};
    use mysql_common::proto::MySerialize;
    fn frame(kind: EventType, body: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        BinlogEventHeader::new(0, kind, 1, (19 + body.len()) as u32, 4, EventFlags::empty()).serialize(&mut out);
        out.extend_from_slice(body);
        out
    }
    let tag = bytes.first().copied().unwrap_or(0);
    let raw = &bytes.get(1..).unwrap_or_default()[..bytes.len().saturating_sub(1).min(4096)];
    let mut reader = EventStreamReader::new(BinlogVersion::Version4);
    let (wire, metadata, target) = match tag % 10 {
        0 => (8, vec![], DataTypeKind::BigInt),
        1 => (246, vec![38, 8], DataTypeKind::Decimal { precision: Some(38), scale: Some(8) }),
        2 => (15, vec![255, 0], DataTypeKind::VarChar { len: Some(255) }),
        3 => (252, vec![2], DataTypeKind::Blob { max_len: 65535 }),
        4 => (245, vec![4], DataTypeKind::Json),
        5 => (18, vec![6], DataTypeKind::DateTime { fsp: 6 }),
        6 => (19, vec![6], DataTypeKind::Time { fsp: 6 }),
        7 => (17, vec![6], DataTypeKind::Timestamp { fsp: 6 }),
        8 => (10, vec![], DataTypeKind::Date),
        _ => (5, vec![], DataTypeKind::Double { precision: None, scale: None }),
    };
    let mut map = vec![1,0,0,0,0,0,0,0,4];
    map.extend_from_slice(b"test\0");
    map.extend_from_slice(&[1,b't',0,1,wire,metadata.len() as u8]);
    map.extend(metadata);
    map.push(0); // Nullable-column bitmap; row images independently carry NULLs.
    let table = reader.read(&frame(EventType::TABLE_MAP_EVENT, &map)[..]).unwrap().unwrap();
    let Some(EventData::TableMapEvent(map)) = table.read_data().unwrap() else { panic!("TABLE_MAP missing") };
    let _ = OptionalMetaExtractor::new(map.iter_optional_meta());
    for kind in [EventType::WRITE_ROWS_EVENT, EventType::UPDATE_ROWS_EVENT, EventType::DELETE_ROWS_EVENT] {
        let mut body = vec![1,0,0,0,0,0,0,0,2,0,1,1];
        if kind == EventType::UPDATE_ROWS_EVENT { body.push(1); }
        body.extend_from_slice(raw);
        if let Ok(Some(event)) = reader.read(&frame(kind, &body)[..]) {
            if let Ok(Some(EventData::RowsEvent(rows))) = event.read_data() {
                for row in rows.rows(&map).take(64) {
                    let Ok((before, after)) = row else { break };
                    for image in [before, after].into_iter().flatten() {
                        if let Some(value) = image.as_ref(0) {
                            let _ = binlog_value_to_mydb(value, &target, tag & 0x80 != 0, Some("utf8mb4"), None);
                        }
                    }
                }
            }
        }
    }
    // Mutate complete event bodies separately from the structured row path.
    for kind in [EventType::TABLE_MAP_EVENT, EventType::QUERY_EVENT, EventType::GTID_EVENT, EventType::XA_PREPARE_LOG_EVENT] {
        if let Ok(Some(event)) = reader.read(&frame(kind, raw)[..]) {
            if let Ok(Some(EventData::QueryEvent(query))) = event.read_data() {
                let state = ddl_session_state_from_query_event(&query);
                let _ = decode_query_event_sql(&query, &state);
            }
        }
    }
}
