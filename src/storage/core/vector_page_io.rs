// Shared buffer/WAL plumbing for paged secondary vector indexes. Layout,
// metadata latches, pristine-page tracking and build policy stay with each index.
fn read_vector_index_page_image(
    buffer_pool: &BufferPool,
    file: &parking_lot::Mutex<Option<File>>,
    key: PageKey,
) -> Result<Vec<u8>, QueryError> {
    if let Some(image) = buffer_pool.get_cached_bytes(&key)? {
        return Ok(image);
    }
    let mut guard = file.lock();
    let Some(file) = guard.as_mut() else {
        return Ok(vec![0u8; PAGE_SIZE]);
    };
    let offset = key.page_id * PAGE_SIZE as u64;
    if file.metadata().map_err(io_err)?.len() < offset + PAGE_SIZE as u64 {
        return Ok(vec![0u8; PAGE_SIZE]);
    }
    read_index_page(file, key.page_id)
}

/// None is an explicit redo-only policy; Some retains the transaction's
/// before-image. Reserve and stamp under the same log lock before publication.
fn log_vector_index_page_image(
    log: &crate::wal::LogMutex,
    txn_id: u64,
    path: String,
    page_id: u64,
    before: Option<Vec<u8>>,
    after: &mut Vec<u8>,
) -> Result<u64, QueryError> {
    let mut log = log.lock();
    let lsn = log.reserve_lsn()?;
    set_page_lsn_if_formatted(after, lsn);
    let after = after.clone();
    let payload = match before {
        Some(before) => WalPayload::PageUpdate {
            path,
            page_id,
            before,
            after,
            undo_lsn: None,
        },
        None => WalPayload::full_page_image(path, page_id, after),
    };
    log.append_reserved(lsn, txn_id, payload)?;
    Ok(lsn)
}
