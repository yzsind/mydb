//! Sidecar payload for FULLTEXT inverted entries (per-(token, document)).
//!
//! The inverted index still reuses the B-tree: one row of text emits multiple inverted entries
//! after tokenization. In Phase 1 the value was a bare `RowId`; the `tf` / document length `|D|`
//! needed by BM25 and the positions needed by phrases were recomputed via row lookups at query
//! time. This module encodes those statistics into the inverted key's **second `Bytes` segment** (`[KeyPart::String(token), KeyPart::Bytes(payload)]`), so that:
//! - natural-language scoring takes `tf` / `doc_len` directly from the posting — zero row lookups, zero re-tokenization;
//! - phrase/proximity checks verify against persisted positions — zero row lookups;
//! - UPDATE maintenance reuses the existing "old∖new / new∖old" diff — with the payload in the key,
//!   an unchanged token whose positions/tf changed naturally yields a different key, which the diff detects.
//!
//! Placing the payload in the key rather than the B-tree value is meant to **fully reuse** the
//! existing leaf-page encoding, WAL record format, and `insert_deferred`/`delete_deferred`
//! interfaces (`Bytes` is an existing `KeyPart`), without touching the highly optimized compact
//! leaf codec and without bumping `WAL_VERSION`. See [`super`] for the module-level doc.
//!
//! ## Encoding (self-describing, no separate header byte)
//! ```text
//! varint(doc_len)            # total token count |D| of the document under the indexed column set (no inter-column gap)
//! varint(tf)                 # occurrences of this token in the document (== positions.len())
//! [ varint(pos_0)            # when positions are stored: first position, absolute
//!   varint(pos_1 - pos_0)    #               the remaining deltas (positions ascending, small gaps)
//!   ... ]                    # tf entries in total; the whole block is omitted when positions are off
//! ```
//! The persisted FulltextMeta determines whether positions are required. Remaining
//! bytes alone cannot distinguish disabled positions from a truncated posting.

use crate::util::env::process_bool as parse_env_bool;
use std::sync::OnceLock;

/// The sidecar statistics of one inverted entry. Empty `positions` means this entry stored no
/// positions (positions off); phrase verification then falls back to row lookup + re-tokenization.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FtPayload {
    /// Document length |D|: total token count of the row under the indexed column set (with repeats, no inter-column gap).
    pub doc_len: u32,
    /// Term frequency tf: occurrences of this token in this row. Always `== positions.len()` (if positions were stored).
    pub tf: u32,
    /// Token positions in the document (ascending, including inter-column gaps). Empty = not stored.
    pub positions: Vec<u32>,
}

/// Reject unrepresentable documents before any posting reaches an index.
#[inline]
pub(crate) fn validate_document_extent(
    doc_len: u64,
    last_position: Option<u64>,
) -> Result<(), crate::types::QueryError> {
    if doc_len > u64::from(u32::MAX)
        || last_position.is_some_and(|position| position > u64::from(u32::MAX))
    {
        return Err(crate::error::catalog::ER_DATA_OUT_OF_RANGE
            .error("FULLTEXT document length or token position exceeds the 32-bit index format"));
    }
    Ok(())
}

impl FtPayload {
    /// Encode as the `Bytes` key segment. With `with_positions=false`, only `doc_len`/`tf` are written.
    pub fn encode(&self, with_positions: bool) -> Vec<u8> {
        let mut out = Vec::with_capacity(
            4 + if with_positions {
                self.positions.len()
            } else {
                0
            },
        );
        write_varint(&mut out, self.doc_len as u64);
        write_varint(&mut out, self.tf as u64);
        if with_positions {
            let mut prev: u32 = 0;
            for (i, &p) in self.positions.iter().enumerate() {
                let delta = if i == 0 { p } else { p.wrapping_sub(prev) };
                write_varint(&mut out, delta as u64);
                prev = p;
            }
        }
        out
    }

    /// Validate a persisted posting without allocating a positions array.
    #[inline]
    pub fn decode_scoring_only(
        bytes: &[u8],
        with_positions: bool,
    ) -> Result<(u32, u32), crate::types::QueryError> {
        // A complete one-byte stream has an exact, bounded position sum:
        // tf <= doc_len < 128 and every delta < 128. Validate every delta;
        // no positions are skipped or inferred from a truncated stream.
        if bytes.len() >= 2 && bytes[0] < 0x80 && bytes[1] < 0x80 {
            let doc_len = u32::from(bytes[0]);
            let tf = u32::from(bytes[1]);
            if tf == 0 || tf > doc_len {
                return Err(corrupt_payload());
            }
            if !with_positions {
                return if bytes.len() == 2 {
                    Ok((doc_len, tf))
                } else {
                    Err(corrupt_payload())
                };
            }
            if bytes.len() == tf as usize + 2 {
                let mut index = 2;
                while index < bytes.len() {
                    let delta = bytes[index];
                    if delta >= 0x80 || (index != 2 && delta == 0) {
                        return Err(corrupt_payload());
                    }
                    index += 1;
                }
                return Ok((doc_len, tf));
            }
        }
        Self::decode_checked(bytes, with_positions, |_| {}).ok_or_else(corrupt_payload)
    }

    /// Decode a complete posting; failed decoding never leaves partial positions.
    #[inline]
    pub fn decode_into_arena(
        bytes: &[u8],
        arena: &mut Vec<u32>,
        with_positions: bool,
    ) -> Result<(u32, u32, u32, u32), crate::types::QueryError> {
        let start = u32::try_from(arena.len()).map_err(|_| corrupt_payload())?;
        match Self::decode_checked(bytes, with_positions, |position| arena.push(position)) {
            Some((doc_len, tf)) => {
                let count = if with_positions { tf } else { 0 };
                if start.checked_add(count).is_none() {
                    arena.truncate(start as usize);
                    return Err(corrupt_payload());
                }
                Ok((doc_len, tf, start, count))
            }
            None => {
                arena.truncate(start as usize);
                Err(corrupt_payload())
            }
        }
    }

    pub(crate) fn decode(
        bytes: &[u8],
        with_positions: bool,
    ) -> Result<FtPayload, crate::types::QueryError> {
        let mut positions = Vec::new();
        let (doc_len, tf, _, _) = Self::decode_into_arena(bytes, &mut positions, with_positions)?;
        Ok(FtPayload {
            doc_len,
            tf,
            positions,
        })
    }

    #[inline]
    fn decode_checked(
        bytes: &[u8],
        with_positions: bool,
        mut visit: impl FnMut(u32),
    ) -> Option<(u32, u32)> {
        let mut off = 0;
        let doc_len = read_varint_u32(bytes, &mut off)?;
        let tf = read_varint_u32(bytes, &mut off)?;
        if tf == 0 || tf > doc_len {
            return None;
        }
        if with_positions {
            // Each position consumes at least one byte. Reject impossible counts
            // before iterating or growing the caller's arena.
            if tf as usize > bytes.len().saturating_sub(off) {
                return None;
            }
            let mut position = 0u32;
            for i in 0..tf {
                let delta = read_varint_u32(bytes, &mut off)?;
                if i != 0 && delta == 0 {
                    return None;
                }
                position = position.checked_add(delta)?;
                visit(position);
            }
        }
        if off != bytes.len() {
            return None;
        }
        Some((doc_len, tf))
    }
}

/// LEB128 unsigned varint write.
pub(crate) fn write_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let mut byte = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if v == 0 {
            break;
        }
    }
}

fn corrupt_payload() -> crate::types::QueryError {
    crate::storage::error(
        crate::error::catalog::ER_INDEX_CORRUPT,
        "corrupt FULLTEXT posting payload",
    )
}

#[inline(always)]
fn read_varint_u32(buf: &[u8], off: &mut usize) -> Option<u32> {
    let first = *buf.get(*off)?;
    *off += 1;
    if first < 0x80 {
        return Some(u32::from(first));
    }
    let mut value = u32::from(first & 0x7f);
    let mut shift = 7;
    while shift <= 28 {
        let byte = *buf.get(*off)?;
        *off += 1;
        if shift == 28 && byte > 0x0f {
            return None;
        }
        value |= u32::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            if byte == 0 {
                return None;
            }
            return Some(value);
        }
        shift += 7;
    }
    None
}

/// FULLTEXT payload configuration (process-wide, `MYDB_FT_*`, cached on first access).
#[derive(Debug, Clone, Copy)]
pub struct FtPayloadConfig {
    /// Whether positions are stored in inverted entries (when off, phrases fall back to row lookup). `MYDB_FT_POSITIONS`, default on.
    pub positions: bool,
    /// Position increment between columns of a multi-column FULLTEXT, preventing cross-column phrase hits. `MYDB_FT_POSITION_GAP`, default 100.
    pub position_gap: u32,
}

impl FtPayloadConfig {
    fn from_env() -> Self {
        FtPayloadConfig {
            positions: parse_env_bool("MYDB_FT_POSITIONS", true),
            position_gap: parse_env_u32("MYDB_FT_POSITION_GAP", 100).max(1),
        }
    }
}

/// Process-wide global payload configuration.
pub fn config() -> &'static FtPayloadConfig {
    static CFG: OnceLock<FtPayloadConfig> = OnceLock::new();
    CFG.get_or_init(FtPayloadConfig::from_env)
}

/// Resolve the persisted key format before consulting process defaults.
pub fn config_from_meta(meta: &crate::storage::schema::FulltextMeta) -> FtPayloadConfig {
    let defaults = config();
    FtPayloadConfig {
        positions: meta.positions.unwrap_or(defaults.positions),
        position_gap: meta.position_gap.unwrap_or(defaults.position_gap).max(1),
    }
}

/// The persisted definition determines whether a posting must contain positions.
pub(crate) fn stored_positions(index: &crate::storage::schema::IndexSchema) -> bool {
    index
        .fulltext
        .as_ref()
        .map(config_from_meta)
        .unwrap_or_else(|| *config())
        .positions
}

fn parse_env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<u32>().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_extent_accepts_last_representable_position_and_length() {
        assert!(validate_document_extent(u64::from(u32::MAX), Some(u64::from(u32::MAX))).is_ok());
        assert_eq!(
            validate_document_extent(u64::from(u32::MAX) + 1, None)
                .unwrap_err()
                .code(),
            1690
        );
        assert_eq!(
            validate_document_extent(1, Some(u64::from(u32::MAX) + 1))
                .unwrap_err()
                .code(),
            1690
        );
    }

    fn roundtrip(p: &FtPayload, with_positions: bool) -> FtPayload {
        FtPayload::decode(&p.encode(with_positions), with_positions).unwrap()
    }

    #[test]
    fn varint_roundtrip_boundaries() {
        for v in [0u64, 1, 127, 128, 300, 16384, u32::MAX as u64] {
            let mut buf = Vec::new();
            write_varint(&mut buf, v);
            let mut off = 0;
            assert_eq!(read_varint_u32(&buf, &mut off).unwrap() as u64, v);
            assert_eq!(off, buf.len());
        }
    }

    #[test]
    fn payload_roundtrip_with_positions() {
        let p = FtPayload {
            doc_len: 42,
            tf: 3,
            positions: vec![0, 5, 130],
        };
        assert_eq!(roundtrip(&p, true), p);
    }

    #[test]
    fn payload_roundtrip_without_positions() {
        let p = FtPayload {
            doc_len: 42,
            tf: 3,
            positions: vec![0, 5, 130],
        };
        // Disabling positions preserves doc_len/tf but leaves positions empty.
        let got = roundtrip(&p, false);
        assert_eq!(got.doc_len, 42);
        assert_eq!(got.tf, 3);
        assert!(got.positions.is_empty());
    }

    #[test]
    fn single_position_zero_is_distinguishable() {
        // For tf=1 at position 0, positions contains one 0x00 byte, distinct from absent positions.
        let p = FtPayload {
            doc_len: 1,
            tf: 1,
            positions: vec![0],
        };
        let got = roundtrip(&p, true);
        assert_eq!(got.positions, vec![0]);
    }

    #[test]
    fn corrupt_postings_are_rejected_without_partial_positions() {
        let full = FtPayload {
            doc_len: 10,
            tf: 2,
            positions: vec![3, 7],
        }
        .encode(true);
        for end in 0..full.len() {
            let bytes = &full[..end];
            assert!(FtPayload::decode_scoring_only(bytes, true).is_err());
            assert!(FtPayload::decode(bytes, true).is_err());
            let mut arena = vec![42];
            assert!(FtPayload::decode_into_arena(bytes, &mut arena, true).is_err());
            assert_eq!(arena, vec![42]);
        }
        for bytes in [
            vec![1, 0],                            // Zero term frequency.
            vec![1, 2, 0, 1],                      // Frequency exceeds document length.
            vec![2, 2, 0, 0],                      // Duplicate position.
            vec![2, 2, 255, 255, 255, 255, 15, 1], // Position overflow.
            vec![128, 0, 1, 0],                    // Noncanonical varint.
            vec![255, 255, 255, 255, 31, 1],       // Integer overflow.
            vec![1, 1, 0, 0],                      // Trailing bytes.
        ] {
            assert!(
                FtPayload::decode_scoring_only(&bytes, true).is_err(),
                "{bytes:?}"
            );
        }
        assert!(FtPayload::decode(&[], false).is_err());
        assert!(FtPayload::decode(&[1, 1], true).is_err());
        assert!(FtPayload::decode(&[1, 1, 0], false).is_err());
        let without = FtPayload::decode(&[1, 1], false).unwrap();
        assert_eq!((without.doc_len, without.tf), (1, 1));
        assert!(without.positions.is_empty());
    }
}

#[cfg(test)]
mod strict_fast_path_tests {
    use super::*;
    #[test]
    fn single_byte_positions_validate_the_complete_stream() {
        for positions in [vec![0], vec![127], (0..127).map(|i| i * 127).collect()] {
            let payload = FtPayload {
                doc_len: 127,
                tf: positions.len() as u32,
                positions,
            };
            let bytes = payload.encode(true);
            assert_eq!(
                FtPayload::decode_scoring_only(&bytes, true).unwrap(),
                (payload.doc_len, payload.tf)
            );
            assert_eq!(FtPayload::decode(&bytes, true).unwrap(), payload);
            for end in 0..bytes.len() {
                assert!(FtPayload::decode_scoring_only(&bytes[..end], true).is_err());
            }
        }
        for bytes in [
            &[1, 0, 0][..],
            &[1, 2, 0, 1],
            &[4, 2, 1, 0],
            &[4, 2, 0x80, 1],
            &[4, 1, 0x80],
            &[4, 1, 0, 1],
        ] {
            assert!(
                FtPayload::decode_scoring_only(bytes, true).is_err(),
                "{bytes:?}"
            );
            let mut arena = vec![99];
            assert!(FtPayload::decode_into_arena(bytes, &mut arena, true).is_err());
            assert_eq!(arena, vec![99]);
        }
        assert_eq!(
            FtPayload::decode_scoring_only(&[4, 1], false).unwrap(),
            (4, 1)
        );
        assert!(FtPayload::decode_scoring_only(&[4, 1, 1], false).is_err());
    }
    #[test]
    fn multi_byte_boundaries_keep_overflow_and_canonical_checks() {
        for number in [0, 127, 128, 16383, 16384, u32::MAX] {
            let mut bytes = Vec::new();
            write_varint(&mut bytes, u64::from(number));
            let mut off = 0;
            assert_eq!(read_varint_u32(&bytes, &mut off), Some(number));
            assert_eq!(off, bytes.len());
            if number >= 128 {
                assert!(read_varint_u32(&bytes[..bytes.len() - 1], &mut 0).is_none());
            }
        }
        for bytes in [
            &[0x80, 0][..],
            &[0xff, 0xff, 0xff, 0xff, 0x10],
            &[0xff, 0xff, 0xff, 0xff, 0x8f],
            &[0x80],
        ] {
            assert!(read_varint_u32(bytes, &mut 0).is_none());
        }
        assert!(
            FtPayload::decode_scoring_only(&[2, 2, 0xff, 0xff, 0xff, 0xff, 0x0f, 1], true).is_err()
        );
    }
}
