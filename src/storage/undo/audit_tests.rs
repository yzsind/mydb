use super::purge_tests::make_record;
use super::*;

#[test]
fn refactor_undo_full_and_metadata_share_headers_lobs_and_reject_truncated_tail() {
    let mut record = make_record(17, 3, 5, Some(11));
    record.kind = UndoKind::Update;
    record.before_values = Some(vec![Value::String("汉字".into()), Value::Int(-7)]);
    record.before_header = Some(RowHeader {
        flags: 0,
        last_txn_id: 2,
        undo_ptr: Some(9),
    });
    record.before_external = vec![(
        1,
        LobRef {
            first_page_id: 3,
            last_page_id: 4,
            total_len: 8192,
            generation: 7,
        },
    )];
    record.borrowed_external = record.before_external.clone();
    record.chain_depth = 13;
    let bytes = encode_undo_record(&record).unwrap();
    assert!(decode_undo_record(&bytes[..bytes.len() - 4]).is_err());
    assert!(decode_undo_record_metadata(&bytes[..bytes.len() - 4]).is_err());
    for (frame, depth) in [(&bytes[..], 13)] {
        let full = decode_undo_record(frame).unwrap();
        let metadata = decode_undo_record_metadata(frame).unwrap();
        assert_eq!(
            (full.id, full.txn_id, full.prev_undo_ptr),
            (metadata.id, metadata.txn_id, metadata.prev_undo_ptr)
        );
        assert_eq!(full.before_header, metadata.before_header);
        assert_eq!(full.before_external, metadata.before_external);
        assert_eq!(full.borrowed_external, metadata.borrowed_external);
        assert_eq!(full.chain_depth, depth);
        assert_eq!(metadata.chain_depth, depth);
        assert!(metadata.before_values.is_none());
        assert!(full.resident);
        assert!(!metadata.resident);
    }
    assert_eq!(
        encode_undo_record(&decode_undo_record(&bytes).unwrap()).unwrap(),
        bytes
    );
}

fn tempdir() -> tempfile::TempDir {
    std::fs::create_dir_all("/tmp/mydb/undo-fix").unwrap();
    tempfile::tempdir_in("/tmp/mydb/undo-fix").unwrap()
}

#[test]
fn undo_audit_oversize_append_is_atomic() {
    let dir = tempdir();
    let mut undo = UndoManager::open_with_segments(&[], dir.path(), 1024).unwrap();
    let before = undo.disk_bytes;
    let bytes = vec![0; MAX_UNDO_FRAME_PAYLOAD + 1];
    assert!(undo
        .append_record_preencoded(make_record(0, 1, 1, None), bytes.clone(), true)
        .is_err());
    assert_eq!(undo.disk_bytes, before);
    assert_eq!(undo.record_count(), 0);
    let shard = undo.begin_concurrent_persistent_append().unwrap().unwrap();
    let offset = shard.lock().active_offset;
    assert!(UndoManager::write_concurrent_persistent_frame(&shard, &mut bytes.clone()).is_err());
    assert_eq!(shard.lock().active_offset, offset);
}

#[test]
fn undo_audit_compaction_preserves_chain_depth() {
    check_compaction_depth(false);
}

#[test]
fn undo_audit_compaction_preserves_evicted_chain_depth() {
    check_compaction_depth(true);
}

fn check_compaction_depth(evict: bool) {
    let dir = tempdir();
    let mut undo = UndoManager::open_with_segments(&[], dir.path(), 1024).unwrap();
    let mut chain = Vec::new();
    for _ in 0..20 {
        undo.next_shard.store(0, Ordering::Relaxed);
        let mut record = make_record(0, 1, 1, chain.last().copied());
        record.kind = UndoKind::Update;
        record.before_values = Some(vec![Value::String("x".repeat(128))]);
        let (id, _, _) = undo.append_record(record, true).unwrap();
        chain.push(id);
    }
    undo.flush().unwrap();
    let active = undo.active_segment_ids();
    let candidates = undo
        .segment_file_bytes
        .keys()
        .copied()
        .filter(|id| !active.contains(id))
        .collect::<Vec<_>>();
    assert!(!candidates.is_empty());
    if evict {
        undo.set_mem_cache_limit_bytes(1);
        undo.evict_to_cache_budget();
        assert!(undo.evicted_records > 0);
    }
    undo.compact_segments(&candidates).unwrap();
    for (i, id) in chain.iter().enumerate() {
        assert_eq!(
            undo.records[&undo.canonical_ptr(*id)].chain_depth,
            i as u32 + 1
        );
    }
    undo.flush().unwrap();
    let reopened = UndoManager::open_with_segments(&[], dir.path(), 1024).unwrap();
    for (i, id) in chain.iter().enumerate() {
        assert_eq!(reopened.get(*id).unwrap().chain_depth, i as u32 + 1);
    }
}

#[test]
fn undo_audit_reorder_entries_are_bounded() {
    let mut undo = UndoManager::default();
    let bytes = encode_undo_record(&make_record(0, 1, 1, None)).unwrap();
    for id in 1..=4096 {
        undo.queue_replicated_frame(id, &bytes).unwrap();
    }
    let charged = undo.resident_charge_bytes();
    undo.queue_replicated_frame(1, &bytes).unwrap();
    assert!(undo.apply_replicated_wal_record(4097, &bytes).is_err());
    assert_eq!(undo.replicated_pending_frames.len(), 4096);
    assert_eq!(undo.resident_charge_bytes(), charged);
    assert_eq!(undo.record_count(), 0);
}

#[test]
fn undo_audit_discard_survives_segment_and_wal_reload_without_id_reuse() {
    let dir = tempdir();
    let mut undo = UndoManager::open_with_segments(&[], dir.path(), 1024).unwrap();
    let (id, bytes, _) = undo
        .append_record(make_record(0, 7, 1, None), true)
        .unwrap();
    let wal = vec![WalRecord {
        source_time_us: 0,
        lsn: 1,
        txn_id: 7,
        prev_txn_lsn: 0,
        payload: WalPayload::UndoRecord { undo_id: id, bytes },
    }];
    // Leave the append buffered: discard must persist the allocation itself.
    undo.discard_unpublished_record(id).unwrap();
    assert_eq!(undo.record_count(), 0);
    assert_eq!(undo.txn_undo_bytes(7), 0);
    drop(undo);
    let no_wal = UndoManager::open_with_segments(&[], dir.path(), 1024).unwrap();
    assert_eq!(no_wal.record_count(), 0);
    let shard = &no_wal.write_shards[shard_of_segment(undo_ptr_segment(id)) as usize];
    assert!(shard.lock().active_offset > undo_ptr_offset(id));
    drop(no_wal);
    let mut reopened = UndoManager::open_with_segments(&wal, dir.path(), 1024).unwrap();
    assert_eq!(reopened.record_count(), 0);
    reopened.next_shard.store(
        shard_of_segment(undo_ptr_segment(id)) as u64,
        Ordering::Relaxed,
    );
    let (next, _, _) = reopened
        .append_record(make_record(0, 8, 2, None), true)
        .unwrap();
    assert_ne!(next, id);
    assert!(reopened.get(id).is_none());
}

#[test]
fn undo_audit_payload_boundary_and_reorder_byte_budget() {
    assert!(validate_undo_payload_len(MAX_UNDO_FRAME_PAYLOAD).is_ok());
    assert!(validate_undo_payload_len(MAX_UNDO_FRAME_PAYLOAD + 1).is_err());
    assert!(validate_undo_payload_len(0).is_err());
    let mut record = make_record(0, 7, 1, None);
    record.before_values = Some(vec![Value::String("x".repeat(MAX_UNDO_FRAME_PAYLOAD))]);
    assert!(encode_undo_record(&record).is_err());
    let mut undo = UndoManager::default();
    // Two individually valid frames can exhaust the independent byte budget.
    let frame = vec![0; MAX_REPLICATED_PENDING_BYTES / 2];
    undo.queue_replicated_frame(1, &frame).unwrap();
    undo.queue_replicated_frame(2, &frame).unwrap();
    let charged = undo.resident_charge_bytes();
    assert!(undo.queue_replicated_frame(3, &[1]).is_err());
    assert_eq!(undo.replicated_pending_bytes, MAX_REPLICATED_PENDING_BYTES);
    assert_eq!(undo.resident_charge_bytes(), charged);
}

#[test]
fn undo_audit_ordered_replication_drains_at_limit_and_releases_budget() {
    let source_dir = tempdir();
    let target_dir = tempdir();
    let mut source = UndoManager::open_with_segments(&[], source_dir.path(), 1024 * 1024).unwrap();
    let mut target = UndoManager::open_with_segments(&[], target_dir.path(), 1024 * 1024).unwrap();
    let mut first = None;
    for _ in 0..MAX_REPLICATED_PENDING_FRAMES + 1 {
        let (id, bytes, _) = source
            .append_record(make_record(0, 7, 1, None), true)
            .unwrap();
        first.get_or_insert(id);
        target.apply_replicated_wal_record(id, &bytes).unwrap();
        assert!(target.replicated_pending_frames.len() <= MAX_REPLICATED_PENDING_FRAMES);
    }
    target.drain_replicated_pending_frames().unwrap();
    assert_eq!(target.replicated_pending_bytes, 0);
    assert!(target.replicated_pending_frames.is_empty());
    // The common WAL repair helper itself persists frames; no target.flush().
    let reopened = UndoManager::open_with_segments(&[], target_dir.path(), 1024 * 1024).unwrap();
    assert_eq!(reopened.record_count(), MAX_REPLICATED_PENDING_FRAMES + 1);
    assert!(reopened.get(first.unwrap()).is_some());
}
