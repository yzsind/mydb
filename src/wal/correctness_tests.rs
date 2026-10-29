use super::*;

#[cfg(test)]
mod crash_consistency {
    use super::*;
    #[test]
    fn checkpoint_published_before_old_unlink_reopens() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let mut log = LogManager::open(dir.path()).unwrap();
        log.set_max_segment_bytes(1024 * 1024);
        log.append(0, WalPayload::CheckpointBegin).unwrap();
        let end = log
            .append(
                0,
                WalPayload::CheckpointEnd {
                    redo_lsn: 1,
                    next_txn_id: 10,
                    next_scn: 10,
                    auto_increment_covered_lsn: 1,
                    sequence_covered_lsn: 1,
                    gc_safe_txn_frontier: 0,
                },
            )
            .unwrap();
        log.flush().unwrap();
        set_wal_cleanup_failpoint(2);
        let result = log.compact_to_checkpoint(end, 10, 10);
        set_wal_cleanup_failpoint(0);
        assert!(result.is_err());
        drop(log);
        let reopened = LogManager::open(dir.path());
        assert!(
            reopened.is_ok(),
            "published checkpoint must survive old generations: {:?}",
            reopened.err()
        );
    }
    #[test]
    fn checkpoint_directory_failure_then_append_reopens() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let mut log = LogManager::open(dir.path()).unwrap();
        log.set_max_segment_bytes(1024 * 1024);
        log.append(0, WalPayload::CheckpointBegin).unwrap();
        let end = log
            .append(
                0,
                WalPayload::CheckpointEnd {
                    redo_lsn: 1,
                    next_txn_id: 10,
                    next_scn: 10,
                    auto_increment_covered_lsn: 1,
                    sequence_covered_lsn: 1,
                    gc_safe_txn_frontier: 0,
                },
            )
            .unwrap();
        log.flush().unwrap();
        set_wal_cleanup_failpoint(5);
        let result = log.compact_to_checkpoint(end, 10, 10);
        set_wal_cleanup_failpoint(0);
        assert!(result.is_err());
        log.append(9, WalPayload::BeginTxn { start_scn: 10 })
            .unwrap();
        log.append(
            9,
            WalPayload::CommitTxn {
                commit_scn: 11,
                commit_unix_us: 1,
                binlog: None,
            },
        )
        .unwrap();
        log.flush().unwrap();
        drop(log);
        let reopened = LogManager::open(dir.path());
        assert!(
            reopened.is_ok(),
            "later acknowledged writes must remain recoverable: {:?}",
            reopened.err()
        );
    }
}

#[cfg(test)]
mod rotation_consistency {
    use super::*;
    #[test]
    fn crash_consistency_unsynced_rotated_tail_is_recoverable() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        log.lock().set_max_segment_bytes(1024 * 1024);
        let first = log.lock().append(0, WalPayload::CheckpointBegin).unwrap();
        LogManager::sync_durable_to(&log, first).unwrap();
        let (path, offset) = {
            let mut log = log.lock();
            let path = log.path().to_path_buf();
            let offset = log.active_info.size_bytes;
            log.append(7, WalPayload::BeginTxn { start_scn: 1 })
                .unwrap();
            log.rotate_segment().unwrap();
            assert_eq!(log.durable_lsn(), first);
            (path, offset)
        };
        drop(log);
        // Preserve the preallocated file length and previously durable prefix.
        // Only the first 20 bytes of the unsynced frame survived power loss.
        let mut bytes = fs::read(&path).unwrap();
        bytes[offset as usize + 20..].fill(0);
        fs::write(path, bytes).unwrap();
        let reopened = LogManager::open(dir.path());
        assert!(
            reopened.is_ok(),
            "an uncommitted rotated suffix must not make recovery fatal: {:?}",
            reopened.err()
        );
    }
}

#[test]
fn rotated_prefix_is_published_in_order_before_commit_ack() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(dir.path()).unwrap(),
    ));
    log.lock().set_max_segment_bytes(512);
    log.lock()
        .append(7, WalPayload::BeginTxn { start_scn: 1 })
        .unwrap();
    for id in 0..64 {
        log.lock()
            .append(
                7,
                WalPayload::UndoRecord {
                    undo_id: id,
                    bytes: vec![0; 80],
                },
            )
            .unwrap();
    }
    let end = log
        .lock()
        .append(
            7,
            WalPayload::CommitTxn {
                commit_scn: 2,
                commit_unix_us: 1,
                binlog: None,
            },
        )
        .unwrap();
    assert!(log.lock().pending_publications.len() > 1);
    LogManager::sync_durable_to(&log, end).unwrap();
    drop(log);
    let reopened = LogManager::open(dir.path()).unwrap();
    assert_eq!(reopened.next_lsn(), end + 1);
    let scan = LogManager::scan(dir.path()).unwrap();
    assert_eq!(scan.records.len(), 66);
    assert!(matches!(
        scan.records.last().unwrap().payload,
        WalPayload::CommitTxn { .. }
    ));
}

#[test]
fn durable_reader_leaves_unrelated_write_buffer_parked() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(dir.path()).unwrap(),
    ));
    log.lock().set_write_buffer_enabled(true).unwrap();
    let end = log
        .lock()
        .append(1, WalPayload::BeginTxn { start_scn: 1 })
        .unwrap();
    LogManager::sync_durable_to(&log, end).unwrap();
    let mut reader = WalReader::open(&log, 1).unwrap();
    log.lock()
        .append(2, WalPayload::BeginTxn { start_scn: 2 })
        .unwrap();
    let parked = log.lock().pending_write_bytes.len();
    assert!(parked > 0);
    let chunk = reader.next_durable_chunk(u64::MAX, 4096).unwrap().unwrap();
    assert_eq!(chunk.end_lsn, end);
    assert_eq!(log.lock().pending_write_bytes.len(), parked);
    assert!(reader.next_durable_chunk(u64::MAX, 4096).unwrap().is_none());
}

#[test]
fn replacement_header_cannot_hide_history_without_valid_checkpoint() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let mut log = LogManager::open(dir.path()).unwrap();
    log.append(7, WalPayload::BeginTxn { start_scn: 1 })
        .unwrap();
    log.flush().unwrap();
    drop(log);
    let path = segment_path(&crate::storage::naming::system_wal_dir(dir.path()), 2);
    let mut bytes = segment_header_with_replacement(&path, 2, 5).unwrap();
    bytes.extend_from_slice(
        &encode_record(&WalRecord {
            source_time_us: 0,
            lsn: 5,
            prev_txn_lsn: 0,
            txn_id: 0,
            payload: WalPayload::CheckpointEnd {
                redo_lsn: 4,
                next_txn_id: 10,
                next_scn: 10,
                auto_increment_covered_lsn: 4,
                sequence_covered_lsn: 4,
                gc_safe_txn_frontier: 0,
            },
        })
        .unwrap(),
    );
    fs::write(path, bytes).unwrap();
    assert!(LogManager::open(dir.path()).is_err());
}

#[test]
fn file_compensation_codec_covers_all_actions() {
    for action in [
        FileCompensation::Delete,
        FileCompensation::Restore(vec![0, 1, 255]),
        FileCompensation::Truncate(32768),
    ] {
        let record = WalRecord {
            source_time_us: 0,
            lsn: 9,
            prev_txn_lsn: 8,
            txn_id: 7,
            payload: WalPayload::FileClr {
                path: "objects/tables/7/1.heap".into(),
                undo_next_lsn: 3,
                action,
            },
        };
        let bytes = encode_record(&record).unwrap();
        let decoded = decode_frame_for_replication(&bytes).unwrap();
        let restored = WalRecord {
            source_time_us: 0,
            lsn: decoded.header.lsn,
            prev_txn_lsn: decoded.header.prev_txn_lsn,
            txn_id: decoded.header.txn_id,
            payload: decoded.payload,
        };
        assert_eq!(encode_record(&restored).unwrap(), bytes);
    }
}
