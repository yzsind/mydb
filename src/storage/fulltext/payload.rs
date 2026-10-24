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
//! `tf >= 1` always holds for any inverted entry (the token must occur in the document), so "whether
//! any bytes remain after doc_len/tf" uniquely tells whether positions were stored — no extra flag bit needed.

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

    /// Decode only `doc_len`/`tf`, skipping the positions array. **BM25 scoring and natural-language
    /// candidate generation need just these two**; skipping the positions loop saves one `Vec` allocation
    /// + tf varint reads — a hot path for high-tf/df corpora. Truncated/corrupt input returns `(0, 0)`; never panics.
    #[inline]
    pub fn decode_scoring_only(bytes: &[u8]) -> (u32, u32) {
        let mut off = 0usize;
        let doc_len = read_varint(bytes, &mut off).unwrap_or(0) as u32;
        let tf = read_varint(bytes, &mut off).unwrap_or(0) as u32;
        (doc_len, tf)
    }

    /// Decode `doc_len`/`tf` and append positions to `arena` (ascending, with inter-column gaps).
    /// Returns `(doc_len, tf, arena_start_index, position_count)`. Compared with [`Self::decode`],
    /// this saves one `Vec<u32>` allocation per posting — heavily used on the phrase path: 22k postings
    /// mean 22k heap allocations collapse into one arena growth.
    /// On truncation/corruption `position_count == 0` (the arena rolls back to its start) — the phrase
    /// path falls back to row lookup, semantically identical to [`Self::decode`].
    #[inline]
    pub fn decode_into_arena(bytes: &[u8], arena: &mut Vec<u32>) -> (u32, u32, u32, u32) {
        let mut off = 0usize;
        let doc_len = read_varint(bytes, &mut off).unwrap_or(0) as u32;
        let tf = read_varint(bytes, &mut off).unwrap_or(0) as u32;
        let start = arena.len() as u32;
        if off >= bytes.len() {
            return (doc_len, tf, start, 0);
        }
        let mut prev: u32 = 0;
        for i in 0..tf {
            match read_varint(bytes, &mut off) {
                Some(delta) => {
                    let p = if i == 0 {
                        delta as u32
                    } else {
                        prev.wrapping_add(delta as u32)
                    };
                    arena.push(p);
                    prev = p;
                }
                None => {
                    arena.truncate(start as usize);
                    return (doc_len, tf, start, 0);
                }
            }
        }
        (doc_len, tf, start, tf)
    }

    /// Decode from the `Bytes` key segment. Fault tolerance: a truncated/corrupt payload degrades to
    /// "no positions"; only `doc_len`/`tf` are trusted (queries use this to fall phrases back to row lookup); never panics.
    pub(crate) fn decode(bytes: &[u8]) -> FtPayload {
        let mut off = 0usize;
        let doc_len = read_varint(bytes, &mut off).unwrap_or(0) as u32;
        let tf = read_varint(bytes, &mut off).unwrap_or(0) as u32;
        let mut positions = Vec::new();
        if off < bytes.len() {
            let mut prev: u32 = 0;
            for i in 0..tf {
                match read_varint(bytes, &mut off) {
                    Some(delta) => {
                        let p = if i == 0 {
                            delta as u32
                        } else {
                            prev.wrapping_add(delta as u32)
                        };
                        positions.push(p);
                        prev = p;
                    }
                    None => {
                        // Truncation: drop the partial positions and degrade to no-positions (phrase falls back to row lookup).
                        positions.clear();
                        break;
                    }
                }
            }
        }
        FtPayload {
            doc_len,
            tf,
            positions,
        }
    }
}

/// LEB128 unsigned varint write.
fn write_varint(out: &mut Vec<u8>, mut v: u64) {
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

/// LEB128 unsigned varint read; returns `None` on out-of-bounds or more than 10 bytes.
fn read_varint(buf: &[u8], off: &mut usize) -> Option<u64> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        let byte = *buf.get(*off)?;
        *off += 1;
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some(result);
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
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
            position_gap: parse_env_u32("MYDB_FT_POSITION_GAP", 100),
        }
    }
}

/// Process-wide global payload configuration.
pub fn config() -> &'static FtPayloadConfig {
    static CFG: OnceLock<FtPayloadConfig> = OnceLock::new();
    CFG.get_or_init(FtPayloadConfig::from_env)
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

    fn roundtrip(p: &FtPayload, with_positions: bool) -> FtPayload {
        FtPayload::decode(&p.encode(with_positions))
    }

    #[test]
    fn varint_roundtrip_boundaries() {
        for v in [0u64, 1, 127, 128, 300, 16384, u32::MAX as u64, u64::MAX] {
            let mut buf = Vec::new();
            write_varint(&mut buf, v);
            let mut off = 0;
            assert_eq!(read_varint(&buf, &mut off), Some(v));
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
    fn decode_truncated_is_lenient() {
        // Truncate after doc_len/tf, discarding positions only.
        let full = FtPayload {
            doc_len: 10,
            tf: 2,
            positions: vec![3, 7],
        }
        .encode(true);
        // Remove the last position byte.
        let got = FtPayload::decode(&full[..full.len() - 1]);
        assert_eq!(got.doc_len, 10);
        assert_eq!(got.tf, 2);
        assert!(got.positions.is_empty());
        // The SQL phrase path uses arena decoding. A partial position list
        // must not suppress fallback or leak into the next posting.
        let mut arena = vec![42];
        assert_eq!(
            FtPayload::decode_into_arena(&full[..full.len() - 1], &mut arena),
            (10, 2, 1, 0)
        );
        assert_eq!(arena, vec![42]);
    }

    #[test]
    fn decode_empty_is_zero() {
        let got = FtPayload::decode(&[]);
        assert_eq!(got, FtPayload::default());
    }
}
