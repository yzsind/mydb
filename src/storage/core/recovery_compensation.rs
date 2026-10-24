fn apply_file_compensation(
    data_dir: &Path,
    path: &str,
    action: &crate::wal::FileCompensation,
) -> Result<(), QueryError> {
    match action {
        crate::wal::FileCompensation::Delete => {
            invalidate_recovery_page_io(data_dir, path);
            match fs::remove_file(data_dir.join(path)) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(io_err(err)),
            }
            note_recovery_write(data_dir, data_dir.join(path))?;
            Ok(())
        }
        crate::wal::FileCompensation::Restore(bytes) => write_recovered_file(data_dir, path, bytes),
        crate::wal::FileCompensation::Truncate(len) => {
            resize_recovered_file(data_dir, path, *len, false)
        }
    }
}

fn compensate_recovery_file(
    data_dir: &Path,
    log: &Arc<crate::wal::LogMutex>,
    txn_id: u64,
    path: &str,
    undo_next_lsn: u64,
    action: crate::wal::FileCompensation,
) -> Result<(), QueryError> {
    let lsn = log.lock().append(
        txn_id,
        WalPayload::FileClr {
            path: path.to_owned(),
            undo_next_lsn,
            action: action.clone(),
        },
    )?;
    // File effects cannot precede their durable recovery decision.
    LogManager::sync_durable_to(log, lsn)?;
    apply_file_compensation(data_dir, path, &action)
}

/// Remove only this loser's rows. File growth is shared with later writers;
/// its old length is not an ownership boundary for those writers' data.
fn undo_reduced_extent(
    data_dir: &Path,
    log: &Arc<crate::wal::LogMutex>,
    record: &WalRecord,
    path: &str,
    old_len: u64,
    new_len: u64,
) -> Result<(), QueryError> {
    if old_len > new_len || old_len % PAGE_SIZE as u64 != 0 || new_len % PAGE_SIZE as u64 != 0 {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "invalid reduced-redo extent",
        ));
    }
    let len = fs::metadata(data_dir.join(path)).map_err(io_err)?.len();
    let mut shared = len > new_len;
    for page_id in old_len / PAGE_SIZE as u64..new_len.min(len) / PAGE_SIZE as u64 {
        let mut page = Page::decode(&read_recovery_page(data_dir, path, page_id)?)?;
        let mut changed = false;
        for slot in 0..page.slots.len() {
            let Some(bytes) = page.read(slot as u16) else {
                continue;
            };
            let header = peek_mvcc_header(bytes)
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "invalid reduced-redo row header",
                    )
                })?
                .0;
            if header.last_txn_id == record.txn_id {
                page.delete(slot as u16)?;
                changed = true;
            } else {
                shared = true;
            }
        }
        if changed {
            let lsn = log.lock().reserve_lsn()?;
            let mut after = page.encode();
            set_page_lsn_if_formatted(&mut after, lsn);
            log.lock().append_reserved(
                lsn,
                record.txn_id,
                WalPayload::Clr {
                    path: path.to_owned(),
                    page_id,
                    undo_next_lsn: record.lsn,
                    after: after.clone(),
                },
            )?;
            if RECOVERY_PAGE_IO.with(|slot| slot.borrow().is_none()) {
                LogManager::sync_durable_to(log, lsn)?;
            }
            redo_page_update(data_dir, path, page_id, lsn, &after)?;
        }
    }
    if !shared && len <= new_len {
        compensate_recovery_file(
            data_dir,
            log,
            record.txn_id,
            path,
            record.prev_txn_lsn,
            crate::wal::FileCompensation::Truncate(old_len),
        )?;
    } else {
        // All physical row compensations precede this completion link.
        log.lock().append(
            record.txn_id,
            WalPayload::RollbackTo {
                undo_head: None,
                undo_next_lsn: record.prev_txn_lsn,
                skip_lsn_start: 0,
                skip_lsn_end: 0,
                compensated: Vec::new(),
            },
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod crash_consistency {
    use super::*;
    #[test]
    fn heap_fpi_repairs_torn_page_with_preserved_lsn() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let relative = "objects/tables/999/1.heap";
        let path = dir.path().join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut image = Page::new().encode();
        set_page_lsn(&mut image, 1);
        refresh_page_checksum(&mut image);
        let mut log = LogManager::open(dir.path()).unwrap();
        log.append(
            0,
            WalPayload::FullPageImage {
                path: relative.into(),
                page_id: 0,
                after: image.clone(),
            },
        )
        .unwrap();
        log.flush().unwrap();
        drop(log);
        let mut torn = image.clone();
        torn[1000] ^= 1;
        assert!(Page::decode(&torn).is_err());
        fs::write(&path, torn).unwrap();
        recover_from_wal(dir.path(), DurabilityMode::Strict).unwrap();
        assert!(
            Page::decode(&fs::read(&path).unwrap()).is_ok(),
            "valid retained FPI must repair torn heap bytes even with intact page LSN"
        );
    }
    #[test]
    fn file_delete_undo_survives_second_crash() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let relative = "objects/tables/999/1.heap";
        let path = dir.path().join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let image = Page::new().encode();
        let mut log = LogManager::open(dir.path()).unwrap();
        log.append(7, WalPayload::BeginTxn { start_scn: 1 })
            .unwrap();
        log.append(
            7,
            WalPayload::FileDelete {
                path: relative.into(),
                before: image.clone(),
            },
        )
        .unwrap();
        log.flush().unwrap();
        drop(log);
        recover_from_wal(dir.path(), DurabilityMode::Strict).unwrap();
        assert_eq!(fs::read(&path).unwrap(), image);
        // Model a power loss: EndTxn reached durable WAL, but the unsynced
        // file restoration and directory entry did not survive.
        fs::remove_file(&path).unwrap();
        recover_from_wal(dir.path(), DurabilityMode::Strict).unwrap();
        assert!(
            path.exists(),
            "durable EndTxn must not suppress the only file restoration"
        );
    }
}
