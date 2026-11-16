//! Historical binlog prefixes are immutable after recovery. Keep only their
//! published lengths in memory and read bounded batches at replica positions.
use super::*;
use std::io::BufReader;

fn invalid(path: &Path, position: u64) -> QueryError {
    error(
        crate::error::catalog::ER_SOURCE_FATAL_ERROR_READING_BINLOG,
        format!(
            "invalid or incomplete binlog event in '{}' at position {position}",
            path.display()
        ),
    )
}

pub(super) fn validated_file_size(path: &Path) -> Result<u64, QueryError> {
    let mut file = File::open(path).map_err(|err| {
        error(
            crate::error::catalog::ER_SOURCE_FATAL_ERROR_READING_BINLOG,
            format!("cannot read binlog '{}': {err}", path.display()),
        )
    })?;
    let mut magic = [0; 4];
    file.read_exact(&mut magic).map_err(io_error)?;
    if &magic != BINLOG_MAGIC {
        return Err(invalid(path, 0));
    }
    Ok(file.metadata().map_err(io_error)?.len())
}

fn header(
    reader: &mut BufReader<File>,
    path: &Path,
    pos: u64,
    end: u64,
) -> Result<([u8; EVENT_HEADER_LEN as usize], u64), QueryError> {
    if end.saturating_sub(pos) < EVENT_HEADER_LEN as u64 {
        return Err(invalid(path, pos));
    }
    let mut bytes = [0; EVENT_HEADER_LEN as usize];
    reader.read_exact(&mut bytes).map_err(io_error)?;
    let size = event_size(&bytes, pos, end).ok_or_else(|| invalid(path, pos))?;
    Ok((bytes, size))
}

fn event_size(bytes: &[u8; EVENT_HEADER_LEN as usize], pos: u64, end: u64) -> Option<u64> {
    let size = u32::from_le_bytes(bytes[9..13].try_into().unwrap()) as u64;
    if size < EVENT_HEADER_LEN as u64 || size > end.checked_sub(pos)? {
        return None;
    }
    let next = u32::from_le_bytes(bytes[13..17].try_into().unwrap());
    (pos + size <= BINLOG_FILE_POSITION_LIMIT && u64::from(next) == pos + size).then_some(size)
}

fn verify_crc(path: &Path, pos: u64, bytes: &[u8]) -> Result<(), QueryError> {
    let end = bytes
        .len()
        .checked_sub(4)
        .filter(|end| *end >= EVENT_HEADER_LEN as usize)
        .ok_or_else(|| invalid(path, pos))?;
    let expected = u32::from_le_bytes(bytes[end..].try_into().unwrap());
    if crc32fast::hash(&bytes[..end]) != expected {
        return Err(error(
            crate::error::catalog::ER_SOURCE_FATAL_ERROR_READING_BINLOG,
            format!(
                "binlog checksum mismatch in '{}' at position {pos}",
                path.display()
            ),
        ));
    }
    Ok(())
}

/// The FDE always has its own checksum, including for a NONE-checksum file.
/// Read the algorithm from the file, never from mutable global configuration.
fn file_checksum(
    reader: &mut BufReader<File>,
    path: &Path,
    end: u64,
) -> Result<BinlogChecksum, QueryError> {
    reader.seek(SeekFrom::Start(4)).map_err(io_error)?;
    let (head, size) = header(reader, path, 4, end)?;
    if head[4] != FORMAT_DESCRIPTION_EVENT || !(81..=4096).contains(&size) {
        return Err(invalid(path, 4));
    }
    let mut bytes = vec![0; size as usize];
    bytes[..EVENT_HEADER_LEN as usize].copy_from_slice(&head);
    reader
        .read_exact(&mut bytes[EVENT_HEADER_LEN as usize..])
        .map_err(io_error)?;
    verify_crc(path, 4, &bytes)?;
    match bytes[bytes.len() - 5] {
        0 => Ok(BinlogChecksum::None),
        1 => Ok(BinlogChecksum::Crc32),
        _ => Err(invalid(path, 4)),
    }
}

fn event_has_checksum(kind: u8, checksum: BinlogChecksum) -> bool {
    kind == FORMAT_DESCRIPTION_EVENT || checksum == BinlogChecksum::Crc32
}

/// Validate a payload with bounded scratch. Callers may seed the hasher with
/// a small prefix already read (for example the fixed GTID fields).
fn check_tail(
    reader: &mut BufReader<File>,
    path: &Path,
    pos: u64,
    mut remaining: u64,
    mut hasher: Option<Hasher>,
) -> Result<(), QueryError> {
    let Some(hasher) = hasher.as_mut() else {
        reader.seek_relative(remaining as i64).map_err(io_error)?;
        return Ok(());
    };
    remaining = remaining.checked_sub(4).ok_or_else(|| invalid(path, pos))?;
    let mut scratch = [0; 16 * 1024];
    while remaining != 0 {
        let take = remaining.min(scratch.len() as u64) as usize;
        reader.read_exact(&mut scratch[..take]).map_err(io_error)?;
        hasher.update(&scratch[..take]);
        remaining -= take as u64;
    }
    let mut expected = [0; 4];
    reader.read_exact(&mut expected).map_err(io_error)?;
    if hasher.clone().finalize() != u32::from_le_bytes(expected) {
        return Err(invalid(path, pos));
    }
    Ok(())
}

/// Without the manifest, complete transaction bytes are not evidence of WAL
/// commitment (and PREVIOUS_GTIDS may describe already reclaimed history).
/// Only recover an empty bootstrap, never infer a transaction publication.
pub(super) fn recover_complete_prefix(path: &Path, end: u64) -> Result<u64, QueryError> {
    let mut reader = BufReader::new(File::open(path).map_err(io_error)?);
    reader.seek(SeekFrom::Start(4)).map_err(io_error)?;
    let mut pos = 4;
    while pos < end {
        if end - pos < EVENT_HEADER_LEN as u64 {
            break;
        }
        let mut bytes = [0; EVENT_HEADER_LEN as usize];
        // An I/O error must not be mistaken for a torn tail and truncate data.
        reader.read_exact(&mut bytes).map_err(io_error)?;
        if !matches!(bytes[4], FORMAT_DESCRIPTION_EVENT | PREVIOUS_GTIDS_EVENT) {
            return Err(error(
                crate::error::catalog::ER_SOURCE_FATAL_ERROR_READING_BINLOG,
                "binlog transaction history exists without a publication manifest; restore the manifest before startup",
            ));
        }
        let Some(size) = event_size(&bytes, pos, end) else {
            break;
        };
        if bytes[4] == PREVIOUS_GTIDS_EVENT {
            let mut count = [0; 8];
            if size < EVENT_HEADER_LEN as u64 + 8 {
                return Err(invalid(path, pos));
            }
            reader.read_exact(&mut count).map_err(io_error)?;
            if u64::from_le_bytes(count) != 0 {
                return Err(error(
                    crate::error::catalog::ER_SOURCE_FATAL_ERROR_READING_BINLOG,
                    "binlog GTID history exists without a publication manifest; restore the manifest before startup",
                ));
            }
        }
        reader.seek(SeekFrom::Start(pos + size)).map_err(io_error)?;
        pos += size;
    }
    if pos > 4 {
        validate_range(path, 4, pos)?;
    }
    if pos != end {
        OpenOptions::new()
            .write(true)
            .open(path)
            .and_then(|f| f.set_len(pos))
            .map_err(io_error)?;
    }
    Ok(pos)
}

/// Returns false when the output budget is exhausted. The first event may
/// exceed max_bytes, as in the live publication path, to ensure progress.
pub(super) fn read_events(
    path: &Path,
    name: &str,
    mut pos: u64,
    end: u64,
    max_events: usize,
    max_bytes: usize,
    used: &mut usize,
    out: &mut Vec<BinlogEventChunk>,
    strict_byte_limit: bool,
) -> Result<bool, QueryError> {
    let mut reader = BufReader::new(File::open(path).map_err(io_error)?);
    let checksum = file_checksum(&mut reader, path, end)?;
    reader.seek(SeekFrom::Start(pos)).map_err(io_error)?;
    while pos < end {
        if out.len() >= max_events {
            return Ok(false);
        }
        let (head, size) = header(&mut reader, path, pos, end)?;
        if strict_byte_limit && size > max_bytes as u64 {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "SHOW BINLOG EVENTS contains an event larger than 64 MiB",
            ));
        }
        let next = used.saturating_add(size as usize);
        if !out.is_empty() && next > max_bytes {
            return Ok(false);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(size as usize)
            .map_err(|_| invalid(path, pos))?;
        bytes.extend_from_slice(&head);
        reader
            .by_ref()
            .take(size - EVENT_HEADER_LEN as u64)
            .read_to_end(&mut bytes)
            .map_err(io_error)?;
        if bytes.len() != size as usize {
            return Err(invalid(path, pos));
        }
        if event_has_checksum(head[4], checksum) {
            verify_crc(path, pos, &bytes)?;
        }
        out.push(BinlogEventChunk {
            file: name.to_string(),
            start_pos: pos,
            end_pos: pos + size,
            event_type: head[4],
            gtid: extract_gtid_from_event(&bytes),
            bytes: Arc::new(bytes),
        });
        *used = next;
        pos += size;
    }
    Ok(true)
}

/// GTID lookups/purge need only headers and the fixed GTID payload; row
/// images are skipped and never allocated. Stop early when the visitor asks.
pub(super) fn visit_gtids(
    path: &Path,
    end: u64,
    mut visit: impl FnMut(u64, &str) -> bool,
) -> Result<(), QueryError> {
    let mut reader = BufReader::with_capacity(64 * 1024, File::open(path).map_err(io_error)?);
    let checksum = file_checksum(&mut reader, path, end)?;
    reader.seek(SeekFrom::Start(4)).map_err(io_error)?;
    let mut pos = 4;
    while pos < end {
        let (head, size) = header(&mut reader, path, pos, end)?;
        let mut remaining = size - EVENT_HEADER_LEN as u64;
        if head[4] == GTID_EVENT {
            if remaining < 25 {
                return Err(invalid(path, pos));
            }
            let mut payload = [0; 25];
            reader.read_exact(&mut payload).map_err(io_error)?;
            remaining -= 25;
            let hasher = event_has_checksum(head[4], checksum).then(|| {
                let mut hasher = Hasher::new();
                hasher.update(&head);
                hasher.update(&payload);
                hasher
            });
            check_tail(&mut reader, path, pos, remaining, hasher)?;
            remaining = 0;
            let gtid = format!(
                "{}:{}",
                format_uuid(&payload[1..17]),
                u64::from_le_bytes(payload[17..25].try_into().unwrap())
            );
            if !visit(pos, &gtid) {
                return Ok(());
            }
        }
        reader.seek_relative(remaining as i64).map_err(io_error)?;
        pos += size;
    }
    Ok(())
}

pub(super) fn validate_range(path: &Path, mut pos: u64, end: u64) -> Result<(), QueryError> {
    let mut reader = BufReader::with_capacity(64 * 1024, File::open(path).map_err(io_error)?);
    let checksum = file_checksum(&mut reader, path, end)?;
    reader.seek(SeekFrom::Start(pos)).map_err(io_error)?;
    while pos < end {
        let (head, size) = header(&mut reader, path, pos, end)?;
        let hasher = event_has_checksum(head[4], checksum).then(|| {
            let mut hasher = Hasher::new();
            hasher.update(&head);
            hasher
        });
        check_tail(
            &mut reader,
            path,
            pos,
            size - EVENT_HEADER_LEN as u64,
            hasher,
        )?;
        pos += size;
    }
    Ok(())
}

pub(super) fn validate_position(path: &Path, pos: u64, end: u64) -> Result<(), QueryError> {
    let mut reader = BufReader::new(File::open(path).map_err(io_error)?);
    reader.seek(SeekFrom::Start(pos)).map_err(io_error)?;
    header(&mut reader, path, pos, end).map(|_| ())
}
