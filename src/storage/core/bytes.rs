use crate::util::bytes::read_u16;
use crate::util::bytes::read_u32;
use crate::util::bytes::read_u64;

fn write_u16(buf: &mut [u8], offset: usize, value: u16) {
    buf[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn write_u32(buf: &mut [u8], offset: usize, value: u32) {
    buf[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn write_u64(buf: &mut [u8], offset: usize, value: u64) {
    buf[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn page_checksum(buf: &[u8], checksum_offset: usize) -> u32 {
    // CRC32 (IEEE polynomial) via `crc32fast`, which lights up the
    // ARMv8 CRC32 / SSE4.2 instructions when present. Treats the
    // checksum field as zeroed during the hash so callers don't need
    // to copy the page buffer before refreshing the checksum.
    //
    // This replaced an in-place FNV-1a byte-loop that consumed ~20 %
    // of LOAD DATA CPU on TPC-H SF=1 (heap + index page rewrites).
    let mut h = crc32fast::Hasher::new();
    h.update(&buf[..checksum_offset]);
    h.update(&[0u8; 4]);
    h.update(&buf[checksum_offset + 4..]);
    h.finalize()
}

/// Physical backup must never accept a stable but torn formatted page. All
/// persistent paged formats share the CRC field at byte 16; zero pages are
/// unallocated extents and remain valid baseline images for WAL redo.
pub(crate) fn backup_page_valid(buf: &[u8]) -> bool {
    buf.len() == PAGE_SIZE
        && (buf.iter().all(|byte| *byte == 0)
            || (matches!(
                read_u32(buf, 0),
                HEAP_PAGE_MAGIC
                    | HEAP_META_MAGIC
                    | INDEX_PAGE_MAGIC
                    | spatial_index::MAGIC
                    | INDEX_META_MAGIC
                    | OVFL_MAGIC
                    | VEC_META_MAGIC
                    | VEC_NODE_MAGIC
            ) || read_u32(buf, 0) == zone_map_page::MAGIC)
                && read_u32(buf, 16) == page_checksum(buf, 16))
}

fn refresh_page_checksum(buf: &mut [u8]) {
    if buf.len() != PAGE_SIZE {
        return;
    }
    let offset = match read_u32(buf, 0) {
        HEAP_PAGE_MAGIC
        | HEAP_META_MAGIC
        | INDEX_PAGE_MAGIC
        | spatial_index::MAGIC
        | INDEX_META_MAGIC
        | OVFL_MAGIC => 16,
        VEC_META_MAGIC | VEC_NODE_MAGIC => 16,
        zone_map_page::MAGIC => 16,
        _ => return,
    };
    // No need to pre-zero the checksum field: page_checksum() treats those
    // 4 bytes as zero regardless of the current value in the buffer.
    let checksum = page_checksum(buf, offset);
    write_u32(buf, offset, checksum);
}

fn set_page_lsn_if_formatted(buf: &mut [u8], lsn: u64) {
    if buf.len() != PAGE_SIZE {
        return;
    }
    match read_u32(buf, 0) {
        HEAP_PAGE_MAGIC
        | HEAP_META_MAGIC
        | INDEX_PAGE_MAGIC
        | spatial_index::MAGIC
        | INDEX_META_MAGIC
        | OVFL_MAGIC => {
            set_page_lsn(buf, lsn);
            refresh_page_checksum(buf);
        }
        VEC_META_MAGIC | VEC_NODE_MAGIC => {
            set_page_lsn(buf, lsn);
            refresh_page_checksum(buf);
        }

        zone_map_page::MAGIC => {
            set_page_lsn(buf, lsn);
            refresh_page_checksum(buf);
        }
        _ => {}
    }
}

// Keep fixed-width checked reads in their callers: a per-key out-of-line
// Result return otherwise spills the full error representation on the hot path.
#[inline(always)]
fn read_u8_checked(buf: &[u8], offset: &mut usize) -> Result<u8, QueryError> {
    if *offset >= buf.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: short page",
        ));
    }
    let value = buf[*offset];
    *offset += 1;
    Ok(value)
}

#[inline(always)]
fn read_u16_checked(buf: &[u8], offset: &mut usize) -> Result<u16, QueryError> {
    if *offset + 2 > buf.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: short page",
        ));
    }
    let value = read_u16(buf, *offset);
    *offset += 2;
    Ok(value)
}

#[inline(always)]
fn read_u32_checked(buf: &[u8], offset: &mut usize) -> Result<u32, QueryError> {
    if *offset + 4 > buf.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: short page",
        ));
    }
    let value = read_u32(buf, *offset);
    *offset += 4;
    Ok(value)
}

#[inline(always)]
fn read_u64_checked(buf: &[u8], offset: &mut usize) -> Result<u64, QueryError> {
    if *offset + 8 > buf.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: short page",
        ));
    }
    let value = read_u64(buf, *offset);
    *offset += 8;
    Ok(value)
}

#[inline(always)]
fn read_i64_checked(buf: &[u8], offset: &mut usize) -> Result<i64, QueryError> {
    Ok(read_u64_checked(buf, offset)? as i64)
}

#[inline(always)]
fn read_i32_checked(buf: &[u8], offset: &mut usize) -> Result<i32, QueryError> {
    Ok(read_u32_checked(buf, offset)? as i32)
}

#[inline(always)]
fn read_i128_checked(buf: &[u8], offset: &mut usize) -> Result<i128, QueryError> {
    if *offset + 16 > buf.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: short page",
        ));
    }
    let bytes: [u8; 16] = buf[*offset..*offset + 16].try_into().unwrap();
    *offset += 16;
    Ok(i128::from_le_bytes(bytes))
}
