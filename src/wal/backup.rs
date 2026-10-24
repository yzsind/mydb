//! Exact, read-only WAL-prefix decoding for physical backup verification.
use super::*;

/// An opened inode and captured EOF are the identity boundary. Never reopen
/// its path, inspect live EOF, initialize an empty header, or repair a tail.
#[cfg(test)]
pub(crate) fn visit_backup_prefix(
    file: &File,
    len: u64,
    visit: impl FnMut(WalRecord) -> Result<(), QueryError>,
) -> Result<(u64, u64), QueryError> {
    visit_backup_prefix_frames(file, len, visit, |_, _| Ok(()))
}

#[cfg(test)]
pub(crate) fn visit_backup_prefix_frames(
    file: &File,
    len: u64,
    visit: impl FnMut(WalRecord) -> Result<(), QueryError>,
    frame_boundary: impl FnMut(u64, u64) -> Result<(), QueryError>,
) -> Result<(u64, u64), QueryError> {
    visit_prefix(file, len, true, visit, frame_boundary)
}

#[cfg(test)]
fn inspect_backup_prefix(file: &File, len: u64) -> Result<(u64, u64), QueryError> {
    visit_prefix(file, len, false, |_| Ok(()), |_, _| Ok(()))
}

#[cfg(test)]
fn visit_prefix(
    file: &File,
    len: u64,
    decode_records: bool,
    mut visit: impl FnMut(WalRecord) -> Result<(), QueryError>,
    mut frame_boundary: impl FnMut(u64, u64) -> Result<(), QueryError>,
) -> Result<(u64, u64), QueryError> {
    use std::io::{BufReader, Read, Seek, SeekFrom};
    if len < WAL_HEADER_LEN as u64 {
        return Err(error(
            crate::error::catalog::MYDB_ER_BACKUP_FAILED,
            "short captured WAL header",
        ));
    }
    let mut input = file.try_clone().map_err(io_err)?;
    input.seek(SeekFrom::Start(0)).map_err(io_err)?;
    let mut input = BufReader::new(input.take(len));
    let mut header = [0u8; WAL_HEADER_LEN];
    input.read_exact(&mut header).map_err(io_err)?;
    if &header[..8] != WAL_MAGIC
        || read_u32(&header, 8) != WAL_VERSION
        || read_u32(&header, 36) != wal_checksum(&header[..36])
    {
        return Err(error(
            crate::error::catalog::MYDB_ER_BACKUP_FAILED,
            "invalid captured WAL header",
        ));
    }
    let mut remaining = len - WAL_HEADER_LEN as u64;
    let mut first = 0;
    let mut previous = 0;
    let mut bytes = Vec::new();
    let mut charge = replay::replay_decode_charge(0)?;
    while remaining != 0 {
        if remaining < WAL_FRAME_HEADER_LEN as u64 {
            return Err(error(
                crate::error::catalog::MYDB_ER_BACKUP_FAILED,
                "partial captured WAL frame",
            ));
        }
        let mut frame_header = [0u8; WAL_FRAME_HEADER_LEN];
        input.read_exact(&mut frame_header).map_err(io_err)?;
        let frame_len = WAL_FRAME_HEADER_LEN + read_u32(&frame_header, 4) as usize;
        if frame_len as u64 > remaining {
            return Err(error(
                crate::error::catalog::MYDB_ER_BACKUP_FAILED,
                "captured WAL frame exceeds prefix",
            ));
        }
        // Decode expands payload structures; reserve before allocating either.
        let required = frame_len
            .checked_mul(if decode_records { 32 } else { 2 })
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_OUTOFMEMORY,
                    "WAL frame allocation overflow",
                )
            })?;
        charge.try_grow(required.saturating_sub(charge.bytes()))?;
        bytes.resize(frame_len, 0);
        bytes[..WAL_FRAME_HEADER_LEN].copy_from_slice(&frame_header);
        input
            .read_exact(&mut bytes[WAL_FRAME_HEADER_LEN..])
            .map_err(io_err)?;
        let (record_header, _) = inspect_wal_frame(&bytes)?;
        let logical_start = frame_logical_start(
            record_header.record_type,
            record_header.lsn,
            &bytes[WAL_FRAME_HEADER_LEN..],
        )?;
        if logical_start == 0 || (previous != 0 && logical_start != previous + 1) {
            return Err(error(
                crate::error::catalog::MYDB_ER_BACKUP_FAILED,
                "captured WAL has an LSN gap or overlap",
            ));
        }
        if first == 0 {
            first = logical_start;
        }
        previous = record_header.lsn;
        frame_boundary(logical_start, previous)?;
        if !decode_records {
            remaining -= frame_len as u64;
            continue;
        }
        match decode_payload(record_header.record_type, &bytes[WAL_FRAME_HEADER_LEN..])? {
            WalPayload::TxnBatch { records } => {
                let mut prev_txn_lsn = record_header.prev_txn_lsn;
                for (index, payload) in records.into_iter().enumerate() {
                    let lsn = logical_start + index as u64;
                    visit(WalRecord {
                        source_time_us: record_header.source_time_us,
                        lsn,
                        prev_txn_lsn,
                        txn_id: record_header.txn_id,
                        payload,
                    })?;
                    prev_txn_lsn = lsn;
                }
            }
            payload => visit(WalRecord {
                source_time_us: record_header.source_time_us,
                lsn: record_header.lsn,
                prev_txn_lsn: record_header.prev_txn_lsn,
                txn_id: record_header.txn_id,
                payload,
            })?,
        }
        remaining -= frame_len as u64;
    }
    Ok((first, previous))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backup_wal_collection_counts_are_bounded_before_allocation() {
        for kind in [WalRecordType::HeapInsertBatch, WalRecordType::IndexBatch] {
            let mut input = 0u32.to_le_bytes().to_vec();
            input.extend_from_slice(&u32::MAX.to_le_bytes());
            assert!(decode_payload(kind, &input).is_err());
        }
        let mut input = vec![0; 4 + 4 + 2];
        input.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_payload(WalRecordType::HeapUpdateLogical, &input).is_err());
        let payload = WalPayload::PrepareTxn {
            xid: crate::xa::Xid::new(1, vec![b'x'], vec![]).unwrap(),
            prepare_scn: 1,
            locks: vec![],
            mdl_locks: vec![],
            binlog: None,
        };
        let mut valid = Vec::new();
        encode_payload_into(&payload, &mut valid).unwrap();
        for offset in [25, 29] {
            let mut corrupt = valid.clone();
            corrupt[offset..offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
            assert!(decode_payload(WalRecordType::PrepareTxn, &corrupt).is_err());
        }
        let bomb = WalPayload::FullPageImageCompressed {
            path: "objects/tables/1/1.heap".into(),
            page_id: 0,
            compressed_after: u32::MAX.to_le_bytes().to_vec(),
        };
        assert!(wal_full_page_image_after(&bomb).is_err());
        let bomb = WalPayload::PageUpdateCompressed {
            path: "objects/indexes/1/1/1.idx".into(),
            page_id: 0,
            undo_lsn: None,
            compressed_before: u32::MAX.to_le_bytes().to_vec(),
            compressed_after: lz4_flex::compress_prepend_size(&[0; 32 * 1024]),
        };
        assert!(wal_compressed_page_update_images(&bomb).is_err());
    }

    #[test]
    fn authenticated_backup_payload_error_must_not_be_trimmed_as_a_crash_tail() {
        let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let mut log = LogManager::open(root.path()).unwrap();
        let path = "objects/indexes/1/1/1.idx";
        log.append(
            0,
            WalPayload::IndexBatch {
                path: path.into(),
                ops: vec![],
            },
        )
        .unwrap();
        log.flush().unwrap();
        let info = log.segment_snapshot().pop().unwrap();
        let mut bytes = fs::read(&info.path).unwrap()[..info.size_bytes as usize].to_vec();
        drop(log);
        let count = WAL_HEADER_LEN + WAL_FRAME_HEADER_LEN + 4 + path.len();
        bytes[count..count + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        let checksum = wal_frame_checksum(
            &bytes[WAL_HEADER_LEN..],
            &bytes[WAL_HEADER_LEN + WAL_FRAME_HEADER_LEN..],
        );
        bytes[WAL_HEADER_LEN + 44..WAL_HEADER_LEN + 48].copy_from_slice(&checksum.to_le_bytes());
        fs::write(&info.path, &bytes).unwrap();
        assert_eq!(
            inspect_backup_prefix(&File::open(&info.path).unwrap(), bytes.len() as u64).unwrap(),
            (1, 1)
        );
        assert!(LogManager::scan_with_mode(root.path(), WalReadMode::Strict).is_err());
        assert_eq!(fs::read(&info.path).unwrap(), bytes);
    }

    #[test]
    fn strict_and_read_only_scans_never_initialize_or_salvage() {
        let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let mut log = LogManager::open(root.path()).unwrap();
        log.append(0, WalPayload::CheckpointBegin).unwrap();
        log.flush().unwrap();
        let info = log.segment_snapshot().pop().unwrap();
        drop(log);
        let valid = fs::read(&info.path).unwrap();
        let mut damaged = valid.clone();
        damaged[WAL_HEADER_LEN + 36] ^= 1;
        for bytes in [&[][..], &valid[..10], damaged.as_slice()] {
            fs::write(&info.path, bytes).unwrap();
            for mode in [WalReadMode::Strict, WalReadMode::ReadOnly] {
                assert!(LogManager::scan_with_mode(root.path(), mode).is_err());
                assert_eq!(fs::read(&info.path).unwrap(), bytes);
                assert!(LogManager::scan_replay_with_mode(root.path(), mode).is_err());
                assert_eq!(fs::read(&info.path).unwrap(), bytes);
            }
        }
        fs::write(&info.path, &valid).unwrap();
        for mode in [WalReadMode::Strict, WalReadMode::ReadOnly] {
            assert_eq!(
                LogManager::scan_with_mode(root.path(), mode)
                    .unwrap()
                    .records
                    .len(),
                1
            );
            assert_eq!(
                LogManager::scan_replay_with_mode(root.path(), mode)
                    .unwrap()
                    .records
                    .len(),
                1
            );
            assert_eq!(fs::read(&info.path).unwrap(), valid);
        }
    }

    #[test]
    fn captured_prefix_ignores_concurrent_tail_and_never_truncates() {
        let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let mut log = LogManager::open(root.path()).unwrap();
        log.append(0, WalPayload::CheckpointBegin).unwrap();
        log.flush().unwrap();
        let info = log.segment_snapshot().pop().unwrap();
        let input = File::open(&info.path).unwrap();
        let original = fs::read(&info.path).unwrap();
        // Deterministically pause the writer halfway through its next header.
        let mut output = OpenOptions::new().append(true).open(&info.path).unwrap();
        output
            .write_all(&original[WAL_HEADER_LEN..WAL_HEADER_LEN + 20])
            .unwrap();
        let before = fs::read(&info.path).unwrap();
        let mut count = 0;
        assert_eq!(
            visit_backup_prefix(&input, info.size_bytes, |_| {
                count += 1;
                Ok(())
            })
            .unwrap(),
            (1, 1)
        );
        assert_eq!(count, 1);
        assert!(visit_backup_prefix(&input, before.len() as u64, |_| Ok(())).is_err());
        assert_eq!(fs::read(&info.path).unwrap(), before);
    }
}

/// Visit complete immutable frames without changing transaction identities or batches.
pub(crate) fn visit_raw_prefix(
    file: &File,
    len: u64,
    mut visit: impl FnMut(u64, u64, &[u8]) -> Result<(), QueryError>,
) -> Result<(u64, u64), QueryError> {
    use std::io::{BufReader, Read, Seek, SeekFrom};
    let mut file = file.try_clone().map_err(io_err)?;
    file.seek(SeekFrom::Start(0)).map_err(io_err)?;
    let mut input = BufReader::new(file.take(len));
    let mut header = [0; WAL_HEADER_LEN];
    input.read_exact(&mut header).map_err(io_err)?;
    if &header[..8] != WAL_MAGIC
        || read_u32(&header, 8) != WAL_VERSION
        || read_u32(&header, 36) != wal_checksum(&header[..36])
    {
        return Err(error(
            crate::error::catalog::MYDB_ER_BACKUP_FAILED,
            "invalid WAL prefix header",
        ));
    }
    let mut left = len.checked_sub(WAL_HEADER_LEN as u64).ok_or_else(|| {
        error(
            crate::error::catalog::MYDB_ER_BACKUP_FAILED,
            "short WAL prefix",
        )
    })?;
    let mut first = 0;
    let mut previous = 0;
    let mut bytes = Vec::new();
    let mut charge = replay::replay_decode_charge(0)?;
    while left > 0 {
        let mut frame_header = [0; WAL_FRAME_HEADER_LEN];
        input.read_exact(&mut frame_header).map_err(io_err)?;
        let size = WAL_FRAME_HEADER_LEN
            .checked_add(read_u32(&frame_header, 4) as usize)
            .ok_or_else(|| {
                error(
                    crate::error::catalog::MYDB_ER_BACKUP_FAILED,
                    "frame length overflow",
                )
            })?;
        if size as u64 > left {
            return Err(error(
                crate::error::catalog::MYDB_ER_BACKUP_FAILED,
                "partial WAL frame",
            ));
        }
        charge.try_grow(size.saturating_sub(charge.bytes()))?;
        bytes.resize(size, 0);
        bytes[..WAL_FRAME_HEADER_LEN].copy_from_slice(&frame_header);
        input
            .read_exact(&mut bytes[WAL_FRAME_HEADER_LEN..])
            .map_err(io_err)?;
        let (record, _) = inspect_wal_frame(&bytes)?;
        let start = frame_logical_start(
            record.record_type,
            record.lsn,
            &bytes[WAL_FRAME_HEADER_LEN..],
        )?;
        if start == 0 || (previous != 0 && start != previous + 1) {
            return Err(error(
                crate::error::catalog::MYDB_ER_BACKUP_FAILED,
                "WAL frame gap",
            ));
        }
        if first == 0 {
            first = start;
        }
        previous = record.lsn;
        visit(start, previous, &bytes)?;
        left -= size as u64;
    }
    Ok((first, previous))
}

pub(crate) struct PinnedPrefix {
    pub segment_id: u64,
    pub file: crate::storage::file_generation::GenerationFile,
    _memory: crate::engine::executor::mem_budget::ControlledMemoryCharge,
    pub len: u64,
    pub start_offset: u64,
    pub first_lsn: u64,
}
impl LogManager {
    pub(crate) fn pin_prefix_after(
        &mut self,
        after: u64,
        hint: Option<(u64, u64)>,
    ) -> Result<Vec<PinnedPrefix>, QueryError> {
        self.flush_pending_writes()?;
        let start = self
            .segments
            .partition_point(|segment| segment.last_lsn <= after);
        let count = self.segments.len() - start + 1;
        let mut out = Vec::with_capacity(count);
        for info in self.segments[start..]
            .iter()
            .chain(std::iter::once(&self.active_info))
            .filter(|s| s.last_lsn > after && s.first_lsn <= s.last_lsn)
        {
            let file = File::open(&info.path).map_err(io_err)?;
            let start_offset = hint
                .filter(|(id, offset)| {
                    *id == info.segment_id
                        && *offset >= WAL_HEADER_LEN as u64
                        && *offset <= info.size_bytes
                })
                .map(|(_, offset)| offset)
                .unwrap_or(WAL_HEADER_LEN as u64);
            out.push(PinnedPrefix {
                segment_id: info.segment_id,
                file: crate::storage::file_generation::GenerationFile::immutable(
                    info.path.clone(),
                    file,
                    info.size_bytes,
                )
                .map_err(io_err)?,
                _memory: crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                    512,
                )?,
                len: info.size_bytes,
                start_offset,
                first_lsn: info.first_lsn,
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
pub(crate) static RAW_PREFIX_READ_BYTES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) fn visit_pinned_range(
    prefix: &PinnedPrefix,
    after: u64,
    through: u64,
    mut visit: impl FnMut(u64, u64, u64, u64, &[u8]) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    use std::io::{BufReader, Read, Seek, SeekFrom};
    let mut input = prefix
        .file
        .open()
        .map_err(io_err)?
        .try_clone()
        .map_err(io_err)?;
    input
        .seek(SeekFrom::Start(prefix.start_offset))
        .map_err(io_err)?;
    let mut input = BufReader::new(input.take(prefix.len - prefix.start_offset));
    let mut offset = prefix.start_offset;
    let mut expected = after.checked_add(1).ok_or_else(|| {
        error(
            crate::error::catalog::MYDB_ER_BACKUP_FAILED,
            "WAL cursor overflow",
        )
    })?;
    let mut bytes = Vec::new();
    let mut charge = replay::replay_decode_charge(0)?;
    while offset < prefix.len {
        if expected > through {
            break;
        }
        let mut header = [0; WAL_FRAME_HEADER_LEN];
        input.read_exact(&mut header).map_err(io_err)?;
        let size = WAL_FRAME_HEADER_LEN + (read_u32(&header, 4) as usize);
        if size as u64 > prefix.len - offset {
            return Err(error(
                crate::error::catalog::MYDB_ER_BACKUP_FAILED,
                "partial pinned frame",
            ));
        }
        charge.try_grow(size.saturating_sub(charge.bytes()))?;
        bytes.resize(size, 0);
        bytes[..WAL_FRAME_HEADER_LEN].copy_from_slice(&header);
        input
            .read_exact(&mut bytes[WAL_FRAME_HEADER_LEN..])
            .map_err(io_err)?;
        #[cfg(test)]
        RAW_PREFIX_READ_BYTES.fetch_add(size as u64, std::sync::atomic::Ordering::Relaxed);
        let (record, _) = inspect_wal_frame(&bytes)?;
        let first = frame_logical_start(
            record.record_type,
            record.lsn,
            &bytes[WAL_FRAME_HEADER_LEN..],
        )?;
        offset += size as u64;
        if record.lsn <= after {
            continue;
        }
        if first != expected {
            return Err(error(
                crate::error::catalog::MYDB_ER_BACKUP_FAILED,
                "pinned WAL gap or split batch",
            ));
        }
        visit(first, record.lsn, record.source_time_us, offset, &bytes)?;
        expected = record.lsn + 1;
    }
    Ok(())
}
