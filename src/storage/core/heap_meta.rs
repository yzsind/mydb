// Page zero is an allocation authority, never a row-addressable page.
const HEAP_META_MAGIC: u32 = 0x4d484d31;
const HEAP_META_HAS_LOB: u64 = 1;

fn encode_heap_meta(path: &Path, pages: u64) -> Vec<u8> {
    let mut bytes = vec![0; PAGE_SIZE];
    write_u32(&mut bytes, 0, HEAP_META_MAGIC);
    write_u16(&mut bytes, 4, HEAP_PAGE_VERSION);
    write_u32(&mut bytes, 20, PAGE_SIZE as u32);
    write_u64(&mut bytes, 24, pages);
    stamp_page_owner(&mut bytes, &page_owner(path), 0);
    refresh_page_checksum(&mut bytes);
    bytes
}

fn decode_heap_meta(bytes: &[u8], owner: &[u8; 16]) -> Result<u64, QueryError> {
    if bytes.len() != PAGE_SIZE
        || read_u32(bytes, 0) != HEAP_META_MAGIC
        || read_u16(bytes, 4) != HEAP_PAGE_VERSION
        || read_u32(bytes, 20) != PAGE_SIZE as u32
        || read_u32(bytes, 16) != page_checksum(bytes, 16)
        || read_u64(bytes, 24) == 0
        || read_u64(bytes, 24) > u64::from(u32::MAX) + 1
        || read_u64(bytes, 32) & !HEAP_META_HAS_LOB != 0
        || bytes[64..].iter().any(|b| *b != 0)
    {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt or unsupported heap allocation metadata",
        ));
    }
    validate_page_owner(bytes, owner, 0)?;
    Ok(read_u64(bytes, 24))
}

fn update_heap_meta(
    path: &Path,
    table_id: u64,
    pool: &Arc<BufferPool>,
    log: &Arc<crate::wal::LogMutex>,
    durability: DurabilityMode,
    change: impl FnOnce(&mut [u8]) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    let key = PageKey {
        file: Arc::new(BufferPool::heap_file_key(path.to_owned(), table_id)),
        page_id: 0,
    };
    update_heap_meta_with_key(key, pool, log, durability, change)
}

fn update_heap_meta_with_key(
    key: PageKey,
    pool: &Arc<BufferPool>,
    log: &Arc<crate::wal::LogMutex>,
    durability: DurabilityMode,
    change: impl FnOnce(&mut [u8]) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    debug_assert_eq!(key.page_id, 0);
    let _pinned = pool.pin_page(key.clone())?;
    let _publication = pool.page_wal_publish_gate.read();
    pool.try_replace_resident_logged_page(&key, |before, _, _| {
        decode_heap_meta(before, &key.file.owner)?;
        let mut after = before.to_vec();
        change(&mut after)?;
        refresh_page_checksum(&mut after);
        let lsn = if durability == DurabilityMode::Memory {
            0
        } else {
            log.lock().append(
                0,
                WalPayload::full_page_image(
                    crate::storage::naming::wal_relative_object_path(&key.file.path),
                    0,
                    after.clone(),
                ),
            )?
        };
        set_page_lsn_if_formatted(&mut after, lsn);
        Ok((lsn, after))
    })?
    .ok_or_else(|| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            "pinned heap allocation page disappeared",
        )
    })?;
    Ok(())
}

fn create_empty_heap_image(path: &Path, bytes: Vec<u8>) -> Vec<u8> {
    if bytes.is_empty() && path.extension().is_some_and(|ext| ext == "heap") {
        encode_heap_meta(path, 1)
    } else {
        bytes
    }
}

/// Seed admission may ignore a proven empty allocation authority, but never
/// treats an unknown or damaged nonempty file as disposable scaffolding.
pub(crate) fn heap_is_empty_allocation_file(path: &Path) -> Result<bool, QueryError> {
    if fs::metadata(path).map_err(io_err)?.len() != PAGE_SIZE as u64 {
        return Ok(false);
    }
    let bytes = fs::read(path).map_err(io_err)?;
    Ok(decode_heap_meta(&bytes, &page_owner_result(path)?)? == 1)
}
