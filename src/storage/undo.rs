use crate::platform::file::{File, OpenOptions};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

use serde::{Deserialize, Serialize};

use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
use crate::types::{QueryError, Value};
use crate::wal::WalPayload;
#[cfg(test)]
use crate::wal::WalRecord;

use super::core::{
    decode_row, encode_row_into, encoded_row_skip_len, error, json_err, LobRef, RowHeader, RowId,
};

type FastHashMap<K, V> = HashMap<K, V, ahash::RandomState>;
type FastHashSet<T> = HashSet<T, ahash::RandomState>;

// Binary undo version. v2 used the legacy 8-tag self-describing value
// section that detoured DecimalCompact / Date / Time / DateTime / Year
// through their ASCII display form. v3 widens the tag set to cover all
// 12 `Value` variants natively so binary temporal / decimal values
// round-trip without an ASCII conversion. The on-disk row payload now
// has different tag bytes for the binary variants, so this version is
// a hard break — older undo logs cannot be decoded by this build.
// Bumped 3 → 4 alongside the RowId compaction: undo records now
// serialise RowId as 8 bytes (page_id u32 + slot u16 + gen u16)
// instead of the legacy 20 bytes (table_id u64 + page_id u64 + …).
// The owning `table_id` is still carried in the surrounding
// `UndoRecord` envelope, so undo readers can still scope rows.
// Bumped 4 → 5 for off-page BLOB/TEXT: a record now carries
// `before_external` — the old row version's externalized-column
// `(col_pos, LobRef)` descriptors. The undo log stores 24-byte chain
// pointers, never the multi-GB bytes, and the externalized columns sit
// in `before_values` as `Value::Null` placeholders.
// Bumped 5 → 7 for the destructive segmented tablespace upgrade:
// records now live in CRC-protected segment frames and legacy undo.tbl
// / JSON-line replay is intentionally rejected for persisted storage.
// Bumped 7 → 8 for the Phase 2 column-level UPDATE delta:
// `UndoKind::UpdateDelta` stores only the changed columns' before
// values instead of the entire row — a typical OLTP UPDATE touching
// 1–3 columns of a wide row drops its undo footprint by 60-90%. The
// new variant uses tag 4; tag 2 (`UndoKind::Update`) is retained for
// the row-piece / NULL-toggle fallback path.
// Bumped 8 → 9: rowid-6bytes refactor shrinks every embedded RowId
// in undo from 8 B to 6 B (Insert / InsertBatch / Update / Delete /
// UpdateDelta — wherever `encode_row_id` is called). Old undo
// segments can't be replayed by a v9 binary.
// Bumped 12 → 13 with overflow v3: each LOB descriptor adds an allocation
// generation u64 so stale undo cannot free a newer chain after page-id reuse.
use crate::storage::format_versions::{UNDO_BINARY_VERSION, UNDO_TABLESPACE_VERSION};
pub(crate) const DEFAULT_UNDO_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const DEFAULT_UNDO_PURGE_INTERVAL_MS: u64 = 1_000;
pub(crate) const DEFAULT_UNDO_PURGE_BATCH_RECORDS: u64 = 4_096;
/// Bound the hot tail of undo before-images when an operator configures a
/// post-snapshot history window. Sustained writers can create several million
/// versions inside a long window; leaving every payload resident inflated RSS
/// by multiple GiB before purge became eligible. Older payloads remain fully
/// recoverable from their segment frames. Set the corresponding environment
/// variable to `0` to restore the historical all-resident behavior.
pub(crate) const DEFAULT_UNDO_MEM_CACHE_BYTES: u64 = 64 * 1024 * 1024;
/// On-disk INSERT undo payload base (no before-image; fixed envelope
/// fields). Matches the byte count `encode_undo_record` emits before
/// the `row_ids` vector. Single-row Insert uses just this number;
/// InsertBatch adds `row_count * INSERT_UNDO_PER_ROW_BYTES` for the
/// rowid-6bytes row_ids tail.
pub(crate) const INSERT_UNDO_PAYLOAD_BASE: u64 = 53;
pub(crate) const INSERT_UNDO_PER_ROW_BYTES: u64 = 6;
/// Default undo segment write-buffer threshold. Buffered frames replace
/// the per-row seek+write+flush syscalls under the global undo lock; the
/// WAL `UndoRecord` twin covers the crash window. `MYDB_UNDO_BUFFER_BYTES=0`
/// restores write-through. Bumped 256 KiB → 1 MiB at P1/9 — OLTP write
/// bursts past 256 KiB used to flush per ~5 K Insert undos and re-enter
/// the seek+write loop; 1 MiB matches the buffered append patterns of
/// the WAL group commit at the steady state.
pub(crate) const DEFAULT_UNDO_BUFFER_BYTES: u64 = 1024 * 1024;

fn default_undo_buffer_bytes() -> u64 {
    std::env::var("MYDB_UNDO_BUFFER_BYTES")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_UNDO_BUFFER_BYTES)
}

/// Phase 3 — recovery-time resident cap, read from the same env var the
/// runtime config uses (`MYDB_UNDO_MEM_CACHE_BYTES`). Known here so
/// `scan_segments` can bound peak memory *while* it loads, rather than
/// blowing up on a larger-than-RAM undo tablespace and only trimming
/// afterwards. `0` explicitly restores the historical full-load behavior.
pub(crate) fn default_undo_mem_cache_bytes() -> u64 {
    std::env::var("MYDB_UNDO_MEM_CACHE_BYTES")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_UNDO_MEM_CACHE_BYTES)
}

const UNDO_SEGMENT_MAGIC: &[u8; 8] = b"MYUNDO1\0";
const UNDO_SEGMENT_HEADER_LEN: u64 = 64;
const UNDO_FRAME_MAGIC: u32 = 0x4d_55_46_52; // MUFR
const UNDO_FRAME_HEADER_LEN: usize = 24;
fn undo_header_crc(header: &[u8], segment: u32) -> u32 {
    let mut crc = crc32fast::Hasher::new();
    crc.update(&header[..16]);
    crc.update(&header[20..24]);
    crc.update(&segment.to_le_bytes());
    crc.finalize()
}

fn undo_frame_header(payload: &[u8]) -> [u8; UNDO_FRAME_HEADER_LEN] {
    let ptr = u64::from_le_bytes(
        payload[UNDO_ENCODED_ID_OFFSET..UNDO_ENCODED_ID_OFFSET + 8]
            .try_into()
            .unwrap(),
    );
    let mut header = [0; UNDO_FRAME_HEADER_LEN];
    header[..4].copy_from_slice(&UNDO_FRAME_MAGIC.to_le_bytes());
    header[4..8].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    header[8..12].copy_from_slice(&crc32fast::hash(payload).to_le_bytes());
    header[12..16].copy_from_slice(&UNDO_TABLESPACE_VERSION.to_le_bytes());
    header[20..24].copy_from_slice(&(undo_ptr_offset(ptr) as u32).to_le_bytes());
    let crc = undo_header_crc(&header, undo_ptr_segment(ptr));
    header[16..20].copy_from_slice(&crc.to_le_bytes());
    header
}

fn validate_undo_frame_header(header: &[u8], segment: u32, offset: u64) -> Result<(), QueryError> {
    if header.len() != UNDO_FRAME_HEADER_LEN
        || u32::from_le_bytes(header[20..24].try_into().unwrap()) as u64 != offset
        || u32::from_le_bytes(header[16..20].try_into().unwrap())
            != undo_header_crc(header, segment)
    {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt undo frame header checksum or physical identity",
        ));
    }
    Ok(())
}

#[cfg(feature = "fuzzing")]
pub(crate) fn fuzz_decode_framing(bytes: &[u8]) {
    if let Some(payload) = bytes.get(UNDO_FRAME_HEADER_LEN..) {
        let _ = decode_undo_record(payload);
        let _ = decode_undo_record_metadata(payload);
        if let Some(id) = payload.get(UNDO_ENCODED_ID_OFFSET..UNDO_ENCODED_ID_OFFSET + 8) {
            let ptr = u64::from_le_bytes(id.try_into().unwrap());
            let _ = validate_undo_frame_header(
                &bytes[..UNDO_FRAME_HEADER_LEN],
                undo_ptr_segment(ptr),
                undo_ptr_offset(ptr),
            );
        }
    }
}

fn undo_storage_uuid(directory: &Path) -> Result<[u8; 16], QueryError> {
    let root = directory.parent().and_then(Path::parent).ok_or_else(|| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            "invalid undo directory",
        )
    })?;
    Ok(crate::storage::control::Control::load(root)?.map_or([0; 16], |control| control.uuid))
}

fn encode_undo_segment_header(
    segment: u32,
    uuid: [u8; 16],
) -> [u8; UNDO_SEGMENT_HEADER_LEN as usize] {
    let mut header = [0; UNDO_SEGMENT_HEADER_LEN as usize];
    header[..8].copy_from_slice(UNDO_SEGMENT_MAGIC);
    header[8..12].copy_from_slice(&UNDO_TABLESPACE_VERSION.to_le_bytes());
    header[12..16].copy_from_slice(&segment.to_le_bytes());
    header[24..40].copy_from_slice(&uuid);
    let crc = crc32fast::hash(&header[..60]);
    header[60..64].copy_from_slice(&crc.to_le_bytes());
    header
}

const MAX_UNDO_FRAME_PAYLOAD: usize = 64 * 1024 * 1024;
const MAX_REPLICATED_PENDING_FRAMES: usize = 4096;
const MAX_REPLICATED_PENDING_BYTES: usize = 64 * 1024 * 1024;

fn validate_undo_payload_len(len: usize) -> Result<(), QueryError> {
    if len == 0 || len > MAX_UNDO_FRAME_PAYLOAD {
        return Err(error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            format!("undo frame payload {len} bytes is outside 1..={MAX_UNDO_FRAME_PAYLOAD}"),
        ));
    }
    Ok(())
}

/// P1/6 — number of write shards (rollback segments).
/// Each shard owns a disjoint `segment_id` subspace via
/// `segment_id = (per_shard_seq << SHARD_BITS) | shard_id`. The default
/// 4 fits typical 4-8-core OLTP without fragmenting the per-shard tail
/// buffer too thinly — each shard sees 1/N of pushes, so smaller N
/// keeps buffers denser and per-shard fsync trips fewer. Bump via
/// `MYDB_UNDO_WRITE_SHARDS` (clamped to 1..=16); set to 1 to restore
/// pre-sharding single-segment behaviour for diagnosis.
const SHARD_BITS: u32 = 4;
const SHARD_MASK: u32 = (1u32 << SHARD_BITS) - 1;
const DEFAULT_UNDO_WRITE_SHARDS: usize = 4;
/// P1/10 build-then-swap — the top shard lane (`SHARD_MASK` == 15) is
/// reserved exclusively for compaction *output* segments, so write
/// shards are capped one below it. Foreground `pick_shard` only ever
/// selects `0..write_shards.len()`, so it never collides with a
/// compaction segment, and recovery's per-shard active-selection loop
/// skips lane `>= write_shards.len()` — a compaction output stays a
/// frozen data segment across restarts and is never resumed for append.
const MAX_UNDO_WRITE_SHARDS: usize = SHARD_MASK as usize;
/// Shard-id lane owning compaction output segment ids. Never picked by
/// any foreground append shard (which live in `0..=MAX_UNDO_WRITE_SHARDS-1`).
const COMPACTION_SHARD_LANE: u32 = SHARD_MASK;

fn default_undo_write_shards() -> usize {
    let parsed = std::env::var("MYDB_UNDO_WRITE_SHARDS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(DEFAULT_UNDO_WRITE_SHARDS);
    parsed.max(1).min(MAX_UNDO_WRITE_SHARDS)
}

#[inline]
fn shard_of_segment(segment_id: u32) -> u32 {
    segment_id & SHARD_MASK
}

#[inline]
fn make_segment_id(shard_id: u32, seq: u32) -> u32 {
    debug_assert!(shard_id <= SHARD_MASK);
    (seq << SHARD_BITS) | shard_id
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum UndoKind {
    Insert,
    InsertBatch {
        row_ids: Vec<RowId>,
    },
    Update,
    Delete,
    /// Inline tombstone retaining its exact original payload. Only the
    /// before_header is needed to recover the preceding logical version.
    DeleteHeader,
    /// Column-level UPDATE delta — produced by the Phase 2 in-place
    /// patch path when every changed column is fixed-width and the
    /// row's NULL bitmap is unchanged. `before_cols` carries the
    /// catalog column indices that were patched (sorted ascending);
    /// the record's `before_values` (sibling field on
    /// [`UndoRecord`]) carries the matching pre-UPDATE values in
    /// the SAME order — i.e. `before_cols[i]` ⇄ `before_values[i]`.
    /// All other columns are unchanged from the live row, so the
    /// MVCC walker / rollback path read the current heap row and
    /// substitute just these positions to reconstruct the old
    /// version.
    UpdateDelta {
        before_cols: Vec<u16>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct UndoRecord {
    pub(crate) id: u64,
    pub(crate) txn_id: u64,
    #[serde(default)]
    pub(crate) created_unix_ms: u64,
    pub(crate) prev_undo_ptr: Option<u64>,
    pub(crate) kind: UndoKind,
    pub(crate) table_id: u64,
    pub(crate) row_id: RowId,
    pub(crate) before_header: Option<RowHeader>,
    pub(crate) before_values: Option<Vec<Value>>,
    /// Externalized columns of the old row version: `(physical_pos,
    /// LobRef)`. The matching slot in `before_values` is `Value::Null`;
    /// MVCC reconstruction materializes from these, and purge/rollback
    /// free the chains. Empty for rows with no off-page columns.
    #[serde(default)]
    pub(crate) before_external: Vec<(usize, LobRef)>,
    /// Immutable chains shared with the newer row version. They reconstruct
    /// this historical version exactly like `before_external`, but purge must
    /// not free them because ownership remains with a newer version/undo.
    #[serde(default)]
    pub(crate) borrowed_external: Vec<(usize, LobRef)>,
    #[serde(default)]
    pub(crate) chain_depth: u32,
    /// In-memory only: the record's `estimate_undo_record_bytes` value,
    /// computed once at insert so the remove/purge paths never re-walk
    /// the before-image to subtract accounting. Not part of the encoded
    /// format; rebuilt on load.
    #[serde(skip)]
    pub(crate) cached_size: u32,
    /// In-memory only: bytes actually retained by this directory entry,
    /// measured from container capacities. Unlike `cached_size` (logical
    /// full-record size), this shrinks to the metadata footprint when the
    /// payload is evicted or startup decodes a metadata-only record.
    #[serde(skip)]
    pub(crate) resident_size: u32,
    #[serde(skip)]
    pub(crate) temp_reservation: Option<Arc<crate::storage::temp_quota::ByteReservation>>,
    /// Phase 2 (two-level mirror) — in-memory only: `true` while the heavy
    /// payload (`before_values` / `before_external`) is resident. Under
    /// memory pressure `evict_to_cache_budget` drops those vectors and
    /// flips this to `false`; the record's lightweight metadata stays in
    /// `records` (so purge planning and accounting are unaffected) and a
    /// consistent read pages the payload back in via `load_owned`. Every
    /// Newly appended records enter resident; recovery may deliberately
    /// create metadata-only entries with this set to `false`.
    #[serde(skip)]
    pub(crate) resident: bool,
    /// M1 (P1-A) — in-memory only: exact encoded frame payload length in
    /// bytes (what `encode_undo_record` produced), captured at append /
    /// scan / page-in. `remove_record` uses it to compute the on-disk
    /// frame end for active-tail truncation without re-encoding the whole
    /// record. `0` means "unknown" (test-constructed records that never
    /// reach a real segment, since `remove_record`'s truncation branch is
    /// gated on `segment_id != 0`); the remove path falls back to a
    /// re-encode in that case. Not part of the on-disk format.
    #[serde(skip)]
    pub(crate) encoded_len: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UndoCreatorState {
    Active,
    Prepared,
    Committed(u64),
    RolledBack,
}

/// P2/13 — one row's worth of in-memory undo history for the
/// `information_schema.mydb_undo_history` view. Flat shape so the
/// view aggregator can group by `(txn_id, table_id)` without holding
/// the undo lock across the catalog query.
#[derive(Debug, Clone, Copy)]
pub struct UndoHistoryEntry {
    pub txn_id: u64,
    pub table_id: u64,
    pub bytes: u64,
    pub chain_depth: u32,
    pub created_unix_ms: u64,
}

/// Output of a single `purge_except_chains` pass.
///
/// `freed_chains`: off-page LOB descriptors owned by purged versions
/// that the caller must release through the generation-checked overflow
/// allocator.
///
/// `reclaim_targets`: rowid-6bytes M2 — `(table_id, row_id)` for every
/// `UndoKind::Delete` purged in this pass. The slot is provably
/// reclaim-safe (`commit_scn <= oldest_active_snapshot_scn` AND
/// `delete_indexes_deferred_at` ran at DELETE commit, so no
/// secondary-index entry still points at it). The caller flips the
/// heap slot's `ROW_FLAG_DELETED → ROW_FLAG_FREE` and journals a
/// `SlotReclaim` WAL record.
#[derive(Debug, Default)]
pub(crate) struct PurgeOutcome {
    pub(crate) freed_chains: Vec<(u64, Vec<(usize, LobRef)>)>,
    pub(crate) reclaim_targets: Vec<(u64, RowId)>,
    /// Fully-dead segment files already detached from the bookkeeping;
    /// the caller unlinks them AFTER releasing the undo lock.
    pub(crate) removable_segments: Vec<PathBuf>,
}

impl PurgeOutcome {
    /// Merge the outcome of a later lock round of the same purge run.
    pub(crate) fn absorb(&mut self, other: PurgeOutcome) {
        self.freed_chains.extend(other.freed_chains);
        self.reclaim_targets.extend(other.reclaim_targets);
        self.removable_segments.extend(other.removable_segments);
    }
}

/// One live record earmarked for relocation by build-then-swap
/// compaction. Captured under the shared lock in `plan_compaction`.
#[derive(Debug)]
pub(crate) struct PlannedMove {
    /// Current undo_ptr of the record in its candidate segment.
    old_ptr: u64,
    /// Full before-image if the payload was resident at plan time;
    /// `None` means it was evicted and phase 2 pages it in off-lock.
    resident_record: Option<UndoRecord>,
    /// Coordinates for the off-lock page-in of an evicted record from
    /// the immutable candidate file.
    src_offset: u64,
    src_path: PathBuf,
}

/// Immutable snapshot produced by `plan_compaction` (phase 1) and
/// consumed by `build_compaction_segments` (phase 2) + `commit_compaction`
/// (phase 3). Owns everything the off-lock build needs so no undo lock is
/// held while the segment file is rewritten.
#[derive(Debug)]
pub(crate) struct CompactionPlan {
    /// Source segments being drained (all non-active).
    candidates: Vec<u32>,
    /// Total on-disk bytes of the candidate segments (reported as freed).
    size_before: u64,
    /// Live records to relocate, in ascending source ptr order.
    moves: Vec<PlannedMove>,
    /// First reserved compaction-lane sequence for output segments.
    out_base_seq: u32,
    /// Per-segment byte budget (mirrors `UndoManager.segment_bytes`).
    segment_bytes: u64,
    /// Undo segment directory.
    dir: PathBuf,
}

impl CompactionPlan {
    /// Whether `ptr` is one of the immutable records this plan will relocate.
    /// `moves` is sorted by `old_ptr` when the plan is built, so callers can
    /// cheaply select only the active transaction heads that need to be
    /// fenced during the final pointer swap.
    pub(crate) fn relocates_ptr(&self, ptr: u64) -> bool {
        self.moves
            .binary_search_by_key(&ptr, |planned| planned.old_ptr)
            .is_ok()
    }
}

/// One freshly built compaction output segment (still at its `.compacting`
/// temp path until `commit_compaction` renames it in).
#[derive(Debug)]
pub(crate) struct BuiltOutSegment {
    seg_id: u32,
    tmp_path: PathBuf,
    final_path: PathBuf,
    file_len: u64,
}

/// Result of phase 2: the durable output segments plus the relocation
/// map (`old_ptr`, `new_ptr`, record-to-insert) the swap applies.
#[derive(Debug, Default)]
pub(crate) struct BuiltCompaction {
    out_segments: Vec<BuiltOutSegment>,
    moves: Vec<(u64, u64, UndoRecord)>,
}

impl BuiltCompaction {
    /// No live record survived to relocate (all candidates raced to
    /// death between planning and building) — the caller skips the swap.
    pub(crate) fn is_empty(&self) -> bool {
        self.moves.is_empty()
    }
}

/// Byte offset of `record.id` inside `encode_undo_record` output: the id
/// is a fixed-width u64 LE directly after the version byte, which is what
/// lets `append_record_preencoded` encode OUTSIDE the undo lock with a
/// placeholder id and patch the allocated ptr in-place under the lock.
pub(crate) const UNDO_ENCODED_ID_OFFSET: usize = 1;

pub(crate) fn encode_undo_record(record: &UndoRecord) -> Result<Vec<u8>, QueryError> {
    let mut out = Vec::with_capacity(128);
    out.push(UNDO_BINARY_VERSION);
    out.extend_from_slice(&record.id.to_le_bytes());
    out.extend_from_slice(&record.txn_id.to_le_bytes());
    out.extend_from_slice(&record.created_unix_ms.to_le_bytes());
    match record.prev_undo_ptr {
        None => out.push(0),
        Some(ptr) => {
            out.push(1);
            out.extend_from_slice(&ptr.to_le_bytes());
        }
    }
    match &record.kind {
        UndoKind::Insert => out.push(0),
        UndoKind::InsertBatch { row_ids } => {
            out.push(1);
            out.extend_from_slice(&(row_ids.len() as u32).to_le_bytes());
            for rid in row_ids {
                encode_row_id(&mut out, rid);
            }
        }
        UndoKind::Update => out.push(2),
        UndoKind::Delete => out.push(3),
        UndoKind::DeleteHeader => out.push(5),
        UndoKind::UpdateDelta { before_cols } => {
            out.push(4);
            out.extend_from_slice(&(before_cols.len() as u32).to_le_bytes());
            for col in before_cols {
                out.extend_from_slice(&col.to_le_bytes());
            }
        }
    }
    out.extend_from_slice(&record.table_id.to_le_bytes());
    encode_row_id(&mut out, &record.row_id);
    match &record.before_header {
        None => out.push(0),
        Some(h) => {
            out.push(1);
            out.push(h.flags);
            out.extend_from_slice(&h.last_txn_id.to_le_bytes());
            match h.undo_ptr {
                None => out.push(0),
                Some(ptr) => {
                    out.push(1);
                    out.extend_from_slice(&ptr.to_le_bytes());
                }
            }
        }
    }
    match &record.before_values {
        None => out.push(0),
        Some(values) => {
            out.push(1);
            encode_row_into(values, &mut out)?;
        }
    }
    out.extend_from_slice(
        &u32::try_from(record.before_external.len())
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                    "too many undo LOB descriptors",
                )
            })?
            .to_le_bytes(),
    );
    for (col, lob) in &record.before_external {
        out.extend_from_slice(
            &u32::try_from(*col)
                .map_err(|_| {
                    error(
                        crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                        "undo LOB column index exceeds descriptor range",
                    )
                })?
                .to_le_bytes(),
        );
        out.extend_from_slice(
            &u32::try_from(lob.first_page_id)
                .map_err(|_| {
                    error(
                        crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                        "LOB first page id exceeds descriptor range",
                    )
                })?
                .to_le_bytes(),
        );
        out.extend_from_slice(
            &u32::try_from(lob.last_page_id)
                .map_err(|_| {
                    error(
                        crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                        "LOB last page id exceeds descriptor range",
                    )
                })?
                .to_le_bytes(),
        );
        out.extend_from_slice(
            &u32::try_from(lob.total_len)
                .map_err(|_| {
                    error(
                        crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                        "LOB total length exceeds descriptor range",
                    )
                })?
                .to_le_bytes(),
        );
        out.extend_from_slice(&lob.generation.to_le_bytes());
    }
    out.extend_from_slice(
        &u32::try_from(record.borrowed_external.len())
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                    "too many borrowed undo LOB descriptors",
                )
            })?
            .to_le_bytes(),
    );
    for (col, lob) in &record.borrowed_external {
        out.extend_from_slice(
            &u32::try_from(*col)
                .map_err(|_| {
                    error(
                        crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                        "borrowed undo LOB column index exceeds descriptor range",
                    )
                })?
                .to_le_bytes(),
        );
        out.extend_from_slice(
            &u32::try_from(lob.first_page_id)
                .map_err(|_| {
                    error(
                        crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                        "borrowed LOB first page id exceeds descriptor range",
                    )
                })?
                .to_le_bytes(),
        );
        out.extend_from_slice(
            &u32::try_from(lob.last_page_id)
                .map_err(|_| {
                    error(
                        crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                        "borrowed LOB last page id exceeds descriptor range",
                    )
                })?
                .to_le_bytes(),
        );
        out.extend_from_slice(
            &u32::try_from(lob.total_len)
                .map_err(|_| {
                    error(
                        crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                        "borrowed LOB length exceeds descriptor range",
                    )
                })?
                .to_le_bytes(),
        );
        out.extend_from_slice(&lob.generation.to_le_bytes());
    }
    out.extend_from_slice(&record.chain_depth.to_le_bytes());
    validate_undo_payload_len(out.len())?;
    Ok(out)
}

pub(crate) fn decode_undo_record(input: &[u8]) -> Result<UndoRecord, QueryError> {
    let mut pos = 0usize;
    let (id, txn_id, created_unix_ms, prev_undo_ptr) = decode_undo_prefix(input, &mut pos)?;
    let kind = match undo_read_u8(input, &mut pos)? {
        0 => UndoKind::Insert,
        1 => {
            let count = undo_read_u32(input, &mut pos)? as usize;
            if count > input.len().saturating_sub(pos) / 6 {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt undo row-id count",
                ));
            }
            let mut row_ids = Vec::with_capacity(count);
            for _ in 0..count {
                row_ids.push(decode_row_id(input, &mut pos)?);
            }
            UndoKind::InsertBatch { row_ids }
        }
        2 => UndoKind::Update,
        3 => UndoKind::Delete,
        5 => UndoKind::DeleteHeader,
        4 => {
            let count = undo_read_u32(input, &mut pos)? as usize;
            if count > input.len().saturating_sub(pos) / 2 {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt undo changed-column count",
                ));
            }
            let mut before_cols = Vec::with_capacity(count);
            for _ in 0..count {
                before_cols.push(undo_read_u16(input, &mut pos)?);
            }
            UndoKind::UpdateDelta { before_cols }
        }
        tag => {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                format!("bad undo kind tag {}", tag),
            ))
        }
    };
    let table_id = undo_read_u64(input, &mut pos)?;
    let row_id = decode_row_id(input, &mut pos)?;
    let before_header = decode_undo_before_header(input, &mut pos)?;
    let before_values = match undo_read_u8(input, &mut pos)? {
        0 => None,
        _ => {
            // The row codec walks the same layout it emits (varint count
            // + NULL bitmap + tagged non-null values); use it to advance
            // `pos` past the row so the trailing fields decode correctly.
            let values = decode_row(&input[pos..])?;
            pos += encoded_row_skip_len(&input[pos..])?;
            Some(values)
        }
    };
    let before_external = decode_undo_lob_refs(input, &mut pos, "corrupt undo record")?;
    let borrowed_external =
        decode_undo_lob_refs(input, &mut pos, "corrupt undo borrowed LOB descriptors")?;
    let chain_depth = undo_read_u32(input, &mut pos)?;
    if pos != input.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "unexpected trailing undo record bytes",
        ));
    }
    Ok(UndoRecord {
        id,
        txn_id,
        created_unix_ms,
        prev_undo_ptr,
        kind,
        table_id,
        row_id,
        before_header,
        before_values,
        before_external,
        borrowed_external,
        chain_depth,
        cached_size: 0,
        resident_size: 0,
        temp_reservation: None,
        resident: true,
        // Set by the caller that owns the exact frame length
        // (page-in / scan); the WAL replay path leaves it 0.
        encoded_len: 0,
    })
}

/// Decode only the directory/cleanup metadata that must stay resident while
/// leaving every variable-size row payload on disk. The returned record is
/// deliberately marked nonresident, so `get` misses and `load_owned` pages the
/// canonical full frame in when rollback or an MVCC walk actually needs it.
pub(crate) fn decode_undo_record_metadata(input: &[u8]) -> Result<UndoRecord, QueryError> {
    fn skip(input: &[u8], pos: &mut usize, bytes: usize) -> Result<(), QueryError> {
        let end = pos
            .checked_add(bytes)
            .filter(|end| *end <= input.len())
            .ok_or_else(|| error(crate::error::catalog::ER_GET_ERRNO, "corrupt undo record"))?;
        *pos = end;
        Ok(())
    }

    let mut pos = 0usize;
    let (id, txn_id, created_unix_ms, prev_undo_ptr) = decode_undo_prefix(input, &mut pos)?;
    let (kind, logical_size_adjustment) = match undo_read_u8(input, &mut pos)? {
        0 => (UndoKind::Insert, 0usize),
        1 => {
            let count = undo_read_u32(input, &mut pos)? as usize;
            skip(
                input,
                &mut pos,
                count.saturating_mul(INSERT_UNDO_PER_ROW_BYTES as usize),
            )?;
            (
                UndoKind::InsertBatch {
                    row_ids: Vec::new(),
                },
                count.saturating_mul(18usize.saturating_sub(INSERT_UNDO_PER_ROW_BYTES as usize)),
            )
        }
        2 => (UndoKind::Update, 0),
        3 => (UndoKind::Delete, 0),
        5 => (UndoKind::DeleteHeader, 0),
        4 => {
            let count = undo_read_u32(input, &mut pos)? as usize;
            skip(
                input,
                &mut pos,
                count.saturating_mul(std::mem::size_of::<u16>()),
            )?;
            (
                UndoKind::UpdateDelta {
                    before_cols: Vec::new(),
                },
                0,
            )
        }
        tag => {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                format!("bad undo kind tag {}", tag),
            ))
        }
    };
    let table_id = undo_read_u64(input, &mut pos)?;
    let row_id = decode_row_id(input, &mut pos)?;
    let before_header = decode_undo_before_header(input, &mut pos)?;
    if undo_read_u8(input, &mut pos)? != 0 {
        let row_len = encoded_row_skip_len(&input[pos..])?;
        skip(input, &mut pos, row_len)?;
    }
    let before_external = decode_undo_lob_refs(input, &mut pos, "corrupt undo record")?;
    let borrowed_external =
        decode_undo_lob_refs(input, &mut pos, "corrupt undo borrowed LOB descriptors")?;
    let chain_depth = undo_read_u32(input, &mut pos)?;
    if pos != input.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "unexpected trailing undo record bytes",
        ));
    }
    Ok(UndoRecord {
        id,
        txn_id,
        created_unix_ms,
        prev_undo_ptr,
        kind,
        table_id,
        row_id,
        before_header,
        before_values: None,
        before_external,
        borrowed_external,
        chain_depth,
        cached_size: u32::try_from(input.len().saturating_add(logical_size_adjustment))
            .unwrap_or(u32::MAX),
        resident_size: 0,
        temp_reservation: None,
        resident: false,
        encoded_len: u32::try_from(input.len()).unwrap_or(u32::MAX),
    })
}

/// Undo RowId encoding: 6 bytes. `[u32 page_id][u16 slot]`. Was
/// 8 bytes before the rowid-6bytes refactor — the per-slot generation
/// ABA counter has been replaced by a slot state machine; see the
/// `RowId` doc-comment in `storage/core.rs`.
fn encode_row_id(out: &mut Vec<u8>, rid: &RowId) {
    out.extend_from_slice(&rid.page_id.to_le_bytes());
    out.extend_from_slice(&rid.slot_id.to_le_bytes());
}

fn decode_row_id(input: &[u8], pos: &mut usize) -> Result<RowId, QueryError> {
    let page_id = undo_read_u32(input, pos)?;
    let slot_id = undo_read_u16(input, pos)?;
    Ok(RowId { page_id, slot_id })
}

fn undo_read_u8(input: &[u8], pos: &mut usize) -> Result<u8, QueryError> {
    if *pos >= input.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt undo record",
        ));
    }
    let v = input[*pos];
    *pos += 1;
    Ok(v)
}

fn undo_read_u16(input: &[u8], pos: &mut usize) -> Result<u16, QueryError> {
    crate::util::bytes::read_word(input, pos)
        .map(u16::from_le_bytes)
        .ok_or_else(|| error(crate::error::catalog::ER_GET_ERRNO, "corrupt undo record"))
}

fn undo_read_u32(input: &[u8], pos: &mut usize) -> Result<u32, QueryError> {
    crate::util::bytes::read_word(input, pos)
        .map(u32::from_le_bytes)
        .ok_or_else(|| error(crate::error::catalog::ER_GET_ERRNO, "corrupt undo record"))
}

fn undo_read_u64(input: &[u8], pos: &mut usize) -> Result<u64, QueryError> {
    crate::util::bytes::read_word(input, pos)
        .map(u64::from_le_bytes)
        .ok_or_else(|| error(crate::error::catalog::ER_GET_ERRNO, "corrupt undo record"))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct UndoManifest {
    /// Fsynced recovery outputs awaiting rename; published with their aliases.
    pending_recovery_segments: Vec<u32>,
    compaction_seq_high: u32,
    version: u32,
    storage_uuid: [u8; 16],
    segment_bytes: u64,
    /// Legacy field: pre-sharding, the manager held a single active
    /// segment. Now `shard_active_segments` is authoritative; this
    /// field is kept for forward read of pre-sharding manifests and
    /// always equals `shard_active_segments[0]` when present.
    active_segment_id: u32,
    /// P1/6 — one entry per shard. Index = shard_id, value = active
    /// segment_id for that shard. Older manifests without this field
    /// rebuild shard mapping from on-disk segments.
    shard_active_segments: Vec<u32>,
    compensated: HashSet<u64>,
    /// Frames that never became a transaction head. Never reuse their IDs.
    discarded: HashSet<u64>,
    forwardings: HashMap<u64, u64>,
}

fn decode_undo_manifest(bytes: &[u8]) -> Result<UndoManifest, QueryError> {
    crate::storage::metadata::decode(
        crate::storage::metadata::UNDO_MANIFEST,
        UNDO_TABLESPACE_VERSION as u16,
        bytes,
    )
    .map_err(json_err)
}

fn encode_undo_manifest(value: &UndoManifest) -> Result<Vec<u8>, QueryError> {
    crate::storage::metadata::encode(
        crate::storage::metadata::UNDO_MANIFEST,
        UNDO_TABLESPACE_VERSION as u16,
        value,
    )
    .map_err(json_err)
}

/// One rollback-segment shard. Each shard owns:
///   * an active segment file + a buffered tail not yet flushed
///   * its `active_segment_id` (low SHARD_BITS == this shard's id)
///   * its `next_seg_seq` counter for the next rotation
///   * its own latency histograms
///
/// Shards are wrapped in `parking_lot::Mutex` inside `UndoManager.write_shards`;
/// the hot append path locks ONLY the picked shard, so N concurrent pushes
/// hitting distinct shards do their seek+write+flush in parallel instead of
/// serializing on a single global undo lock.
/// A segment cannot be retired while a frame is being appended or is live
/// outside the canonical directory. The weak registry never resurrects a
/// closed generation; a lease is acquired under its writer-shard mutex.
#[derive(Debug)]
#[repr(align(128))]
struct UndoSegmentGeneration;

#[derive(Debug, Clone)]
pub(crate) struct UndoSegmentLease {
    segment_id: u32,
    _pin: Arc<UndoSegmentGeneration>,
}

#[derive(Debug)]
struct UndoSegmentPins {
    pins: FastHashMap<u32, std::sync::Weak<UndoSegmentGeneration>>,
    charge: ControlledMemoryCharge,
}
impl Default for UndoSegmentPins {
    fn default() -> Self {
        Self {
            pins: Default::default(),
            charge: ControlledMemoryCharge::try_new(ControlledMemoryCategory::SystemMetadata, 0)
                .expect("zero-byte segment registry reservation"),
        }
    }
}
impl UndoSegmentPins {
    fn lease(&mut self, segment_id: u32) -> Result<UndoSegmentLease, QueryError> {
        if let Some(pin) = self
            .pins
            .get(&segment_id)
            .and_then(std::sync::Weak::upgrade)
        {
            return Ok(UndoSegmentLease {
                segment_id,
                _pin: pin,
            });
        }
        if !self.pins.contains_key(&segment_id) && self.pins.len() == self.pins.capacity() {
            let buckets = (self.pins.len() + 1).next_power_of_two().max(4) * 2;
            let bytes = buckets
                * (std::mem::size_of::<(u32, std::sync::Weak<UndoSegmentGeneration>)>() + 161);
            // Registration runs under a writer-lane latch. Do not invoke
            // reclaim callbacks that could reenter storage diagnostics.
            if !self
                .charge
                .try_grow_optional(bytes.saturating_sub(self.charge.bytes()))
            {
                return Err(error(
                    crate::error::catalog::ER_OUT_OF_RESOURCES,
                    "undo segment pin registry cannot reserve memory within MYDB_MEMORY_TARGET",
                ));
            }
            self.pins.try_reserve(1).map_err(|_| {
                error(
                    crate::error::catalog::ER_OUT_OF_RESOURCES,
                    "undo segment pin registry allocation failed",
                )
            })?;
        }
        let pin = Arc::new(UndoSegmentGeneration);
        self.pins.insert(segment_id, Arc::downgrade(&pin));
        Ok(UndoSegmentLease {
            segment_id,
            _pin: pin,
        })
    }
}

#[derive(Debug)]
pub(crate) struct UndoWriteShard {
    pub(crate) shard_id: u32,
    pub(crate) active_file: Option<File>,
    active_pin: Option<UndoSegmentLease>,
    segment_pins: Arc<parking_lot::Mutex<UndoSegmentPins>>,
    pub(crate) tail_buffer: Vec<u8>,
    pub(crate) buffer_start: u64,
    pub(crate) active_segment_id: u32,
    pub(crate) active_offset: u64,
    pub(crate) buffer_limit: u64,
    pub(crate) write_frame_latency: LatencyHistogram,
    pub(crate) flush_latency: LatencyHistogram,
    pub(crate) buffer_flushes: u64,
    /// Segments written since the last checkpoint barrier, including rotated ones.
    pending_sync: BTreeSet<u32>,
    /// The current checkpoint's obligations while its I/O releases this latch.
    /// Only explicit GC may remove an obligation before its barrier succeeds.
    syncing_segments: Option<BTreeSet<u32>>,
    pub(crate) next_seg_seq: u32,
    /// Snapshot of `segment_bytes` at the last `ensure`/`rotate` — read
    /// here without locking the manager so the rotation decision is
    /// shard-local. Manager-level `set_segment_bytes` rewrites all shards
    /// under an exclusive guard.
    pub(crate) segment_bytes: u64,
    /// Pointer back into the `undo_dir` so the shard can build its own
    /// segment paths without going through the manager. `None` for the
    /// in-memory test default.
    pub(crate) undo_dir: Option<PathBuf>,
}

pub(crate) type UndoWriteShardHandle = Arc<parking_lot::Mutex<UndoWriteShard>>;

impl UndoWriteShard {
    fn new(
        shard_id: u32,
        segment_bytes: u64,
        buffer_limit: u64,
        undo_dir: Option<PathBuf>,
        segment_pins: Arc<parking_lot::Mutex<UndoSegmentPins>>,
    ) -> Self {
        // The first segment for this shard is `(1 << SHARD_BITS) | shard_id`
        // so `seq=0` (which would collapse to `shard_id`) is never picked —
        // this keeps `0` reserved for "no segment yet" while still letting
        // the inverse map `segment_id & SHARD_MASK == shard_id` hold.
        Self {
            shard_id,
            active_file: None,
            active_pin: None,
            segment_pins,
            tail_buffer: Vec::new(),
            buffer_start: UNDO_SEGMENT_HEADER_LEN,
            active_segment_id: make_segment_id(shard_id, 1),
            active_offset: UNDO_SEGMENT_HEADER_LEN,
            buffer_limit,
            write_frame_latency: LatencyHistogram::default(),
            flush_latency: LatencyHistogram::default(),
            buffer_flushes: 0,
            pending_sync: BTreeSet::new(),
            syncing_segments: None,
            next_seg_seq: 1,
            segment_bytes: segment_bytes.max(UNDO_SEGMENT_HEADER_LEN * 2),
            undo_dir,
        }
    }

    fn segment_path(&self, segment_id: u32) -> Result<PathBuf, QueryError> {
        self.undo_dir
            .as_ref()
            .map(|dir| dir.join(segment_file_name(segment_id)))
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "undo tablespace has no segment directory",
                )
            })
    }

    fn ensure_active_segment(&mut self) -> Result<(), QueryError> {
        if self.active_file.is_some() {
            if self
                .active_pin
                .as_ref()
                .is_none_or(|pin| pin.segment_id != self.active_segment_id)
            {
                self.active_pin = Some(self.segment_pins.lock().lease(self.active_segment_id)?);
            }
            return Ok(());
        }
        if self.undo_dir.is_none() {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "undo tablespace has no segment directory",
            ));
        }
        let path = self.segment_path(self.active_segment_id)?;
        let exists = path.exists();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(io_err)?;
        if !exists || file.metadata().map_err(io_err)?.len() == 0 {
            write_segment_header(&mut file, self.active_segment_id, &path)?;
            self.pending_sync.insert(self.active_segment_id);
            self.active_offset = UNDO_SEGMENT_HEADER_LEN;
        } else {
            self.active_offset = file
                .metadata()
                .map_err(io_err)?
                .len()
                .max(UNDO_SEGMENT_HEADER_LEN);
        }
        file.seek(SeekFrom::Start(self.active_offset))
            .map_err(io_err)?;
        self.active_pin = Some(self.segment_pins.lock().lease(self.active_segment_id)?);
        self.active_file = Some(file);
        self.tail_buffer.clear();
        self.buffer_start = self.active_offset;
        Ok(())
    }

    fn rotate(&mut self) -> Result<(), QueryError> {
        // Finalize the outgoing segment.
        self.flush_tail_buffer()?;
        if let Some(file) = self.active_file.as_mut() {
            file.flush().map_err(io_err)?;
        }
        self.active_file = None;
        self.next_seg_seq = recovery::next_recovery_sequence(self.next_seg_seq)?;
        self.active_segment_id = make_segment_id(self.shard_id, self.next_seg_seq);
        self.active_offset = UNDO_SEGMENT_HEADER_LEN;
        self.ensure_active_segment()
    }

    fn write_frame(&mut self, ptr: u64, payload: &[u8]) -> Result<(), QueryError> {
        validate_undo_payload_len(payload.len())?;
        let segment_id = undo_ptr_segment(ptr);
        let offset = undo_ptr_offset(ptr);
        if self.active_file.is_none() {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "undo segment is not open",
            ));
        }
        if segment_id != self.active_segment_id || offset != self.active_offset {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "undo pointer allocation mismatch",
            ));
        }
        let header = undo_frame_header(&payload);
        self.pending_sync.insert(segment_id);
        let started = std::time::Instant::now();
        if self.buffer_limit == 0 {
            let file = self.active_file.as_mut().expect("checked above");
            file.seek(SeekFrom::Start(offset)).map_err(io_err)?;
            file.write_all(&header).map_err(io_err)?;
            file.write_all(payload).map_err(io_err)?;
            file.flush().map_err(io_err)?;
        } else {
            if self.tail_buffer.is_empty() {
                self.buffer_start = offset;
            }
            debug_assert_eq!(offset, self.buffer_start + self.tail_buffer.len() as u64);
            self.tail_buffer.extend_from_slice(&header);
            self.tail_buffer.extend_from_slice(payload);
        }
        self.write_frame_latency
            .record(started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64);
        self.active_offset = offset + UNDO_FRAME_HEADER_LEN as u64 + payload.len() as u64;
        if self.buffer_limit != 0 && self.tail_buffer.len() as u64 >= self.buffer_limit {
            self.flush_tail_buffer()?;
        }
        Ok(())
    }

    fn flush_tail_buffer(&mut self) -> Result<(), QueryError> {
        if self.tail_buffer.is_empty() {
            return Ok(());
        }
        let started = std::time::Instant::now();
        let file = self.active_file.as_mut().ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "undo segment is not open",
            )
        })?;
        file.seek(SeekFrom::Start(self.buffer_start))
            .map_err(io_err)?;
        file.write_all(&self.tail_buffer).map_err(io_err)?;
        file.flush().map_err(io_err)?;
        // Undo segments are only fsynced by checkpoints. Start writeback now
        // so the page cache does not accumulate them into an expiry burst
        // that stalls WAL fsyncs; a failed hint leaves them to the flusher.
        let _ = crate::util::file::start_file_writeback(
            file,
            self.buffer_start,
            self.tail_buffer.len(),
        );
        self.buffer_start += self.tail_buffer.len() as u64;
        self.tail_buffer.clear();
        self.buffer_flushes = self.buffer_flushes.saturating_add(1);
        self.flush_latency
            .record(started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64);
        Ok(())
    }

    fn set_buffer_limit(&mut self, bytes: u64) -> Result<(), QueryError> {
        if bytes == 0 && !self.tail_buffer.is_empty() {
            self.flush_tail_buffer()?;
        }
        self.buffer_limit = bytes;
        Ok(())
    }

    pub(crate) fn checkpoint_sync(shard: &UndoWriteShardHandle) -> Result<(), QueryError> {
        Self::checkpoint_sync_with(shard, || {})
    }

    fn checkpoint_sync_with(
        shard: &UndoWriteShardHandle,
        before_sync: impl FnOnce(),
    ) -> Result<(), QueryError> {
        let (directory, pending) = {
            let mut guard = shard.lock();
            if guard.syncing_segments.is_some() {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "undo shard checkpoint synchronization is already in progress",
                ));
            }
            guard.flush_all()?;
            let pending = std::mem::take(&mut guard.pending_sync);
            guard.syncing_segments = Some(pending.clone());
            (guard.undo_dir.clone(), pending)
        };
        before_sync();
        let result = (|| {
            #[cfg(target_os = "macos")]
            let mut sync_batch = crate::platform::fs::DeviceSyncBatch::new();
            for &segment_id in &pending {
                let dir = directory.as_ref().expect("persistent shard");
                let path = dir.join(segment_file_name(segment_id));
                match OpenOptions::new().write(true).open(&path) {
                    #[cfg(target_os = "macos")]
                    Ok(file) => sync_batch.sync_file(file, &path).map_err(io_err)?,
                    #[cfg(not(target_os = "macos"))]
                    Ok(file) => file.sync_all().map_err(io_err)?,
                    // A missing live segment is an error, even if the parent
                    // directory survives. GC must explicitly retire this
                    // checkpoint's obligation before absence can be accepted.
                    Err(err)
                        if err.kind() == std::io::ErrorKind::NotFound
                            && dir.is_dir()
                            && shard
                                .lock()
                                .syncing_segments
                                .as_ref()
                                .is_some_and(|ids| !ids.contains(&segment_id)) => {}
                    Err(err) => return Err(io_err(err)),
                }
            }
            #[cfg(target_os = "macos")]
            sync_batch.finish().map_err(io_err)?;
            Ok(())
        })();
        let mut guard = shard.lock();
        let outstanding = guard
            .syncing_segments
            .take()
            .expect("checkpoint owns its in-flight segment obligations");
        if result.is_err() {
            // Merge the failed batch with writes that arrived during fsync.
            // Explicitly reclaimed segments no longer need a future barrier.
            guard.pending_sync.extend(outstanding);
        }
        // Directory entries are synced once, with the checkpoint manifest,
        // after ALL shards have finished. A failure there still prevents End.
        result
    }

    fn flush_all(&mut self) -> Result<(), QueryError> {
        self.flush_tail_buffer()?;
        if let Some(file) = self.active_file.as_mut() {
            file.flush().map_err(io_err)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct UndoManager {
    /// Unified-governor reservation for all resident undo directory metadata
    /// and payload. Shared with short-lived pre-admission guards so the
    /// concurrent segment writer can reserve before doing frame I/O.
    resident_charge: Arc<parking_lot::Mutex<ControlledMemoryCharge>>,
    segment_pins: Arc<parking_lot::Mutex<UndoSegmentPins>>,
    pub(crate) next_undo_id: u64,
    /// Keyed by `undo_ptr`. AHash keeps steady-state insert/remove/lookup O(1).
    /// Records stay inline: the directory reaches its small workload high-water
    /// early, then reuses capacity, avoiding one allocator round-trip for every
    /// short-lived undo record. The only consumer that needs pointer order is
    /// background compaction; it sorts its bounded candidate list explicitly.
    pub(crate) records: FastHashMap<u64, UndoRecord>,
    /// Side index keyed by `created_unix_ms` → undo ids created in that ms.
    /// AHashSet keeps arbitrary removal O(1) under dense write bursts.
    pub(crate) created_index: BTreeMap<u64, FastHashSet<u64>>,
    /// Only resident, persistent records with releasable payload. Keeping this
    /// separate from retention history prevents rescanning evicted metadata on
    /// every append when the metadata floor alone exceeds the cache limit.
    eviction_candidates: BTreeSet<(u64, u64)>,
    pub(crate) current_bytes: u64,
    /// Capacity-based bytes actually owned by the undo directory: fixed
    /// metadata plus any currently resident payload. `current_bytes` remains
    /// the logical full-record total for SHOW STATUS. Payload eviction drives
    /// this toward `mem_cache_limit_bytes`, but the accounted metadata floor
    /// is non-evictable.
    pub(crate) resident_bytes: u64,
    /// Phase 2 — in-memory payload cap (bytes; 0 = unbounded, no eviction).
    /// Once `resident_bytes` exceeds it the append path evicts the oldest
    /// flushed payloads down to a low-water mark or the metadata floor.
    /// Reads page full records back in without caching them.
    pub(crate) mem_cache_limit_bytes: u64,
    /// Phase 2 — cumulative count of payload evictions (SHOW STATUS).
    pub(crate) evicted_records: u64,
    /// Phase 4 — logical undo bytes per transaction (resident + evicted),
    /// maintained alongside `current_bytes`. Drives the per-txn cap and
    /// "which txn is pinning undo" blocker diagnostics.
    pub(crate) txn_bytes: FastHashMap<u64, u64>,
    /// Phase 4 — per-transaction undo byte cap (0 = unlimited).
    pub(crate) txn_max_bytes: u64,
    /// Shared safety gate for the lock-free shard-routing append path. The
    /// Database hot path owns a clone; manager setters publish `false` before
    /// enabling a policy that requires pre-IO admission, and publish `true`
    /// only after all such policies are disabled.
    concurrent_append_eligible: Arc<AtomicBool>,
    /// Phase 2 — open segment-file read handles for page-in. A cold MVCC
    /// read uses `read_at` on a cached `Arc<File>` instead of paying an
    /// `open()` per frame. Invalidated when a segment is unlinked.
    segment_readers: parking_lot::Mutex<SegmentReaderCache>,
    pub(crate) purged_records: u64,
    pub(crate) peak_bytes: u64,
    pub(crate) peak_records: u64,
    pub(crate) longest_chain_depth: u32,
    pub(crate) retention_pressure_count: u64,
    pub(crate) retention_guarantee: bool,
    pub(crate) tablespace_max_bytes: u64,
    pub(crate) mem_soft_limit_bytes: u64,
    pub(crate) mem_pressure_count: u64,
    pub(crate) disk_cap_softfail_warn: bool,
    pub(crate) disk_cap_softfail_warns_emitted: u64,
    pub(crate) segment_bytes: u64,
    /// P1/6 — rollback-segment shards. The hot append path clones one
    /// shard handle while briefly holding the manager read lock, then
    /// writes the segment frame after the global undo lock has been
    /// released. This keeps frame IO parallel across shards and prevents
    /// phase-1 append from blocking phase-2 publish on the manager RwLock.
    pub(crate) write_shards: Arc<[UndoWriteShardHandle]>,
    /// Round-robin shard selector. AtomicU64 so the hot path on the
    /// shared (read) lock can advance it without contention.
    pub(crate) next_shard: AtomicU64,
    undo_dir: Option<PathBuf>,
    strict_recovery: bool,
    pub(crate) capture_active: Arc<AtomicBool>,
    pub(crate) archive_floor: Arc<AtomicU64>,
    manifest_path: Option<PathBuf>,
    pub(crate) compensated: HashSet<u64>,
    discarded: HashSet<u64>,
    compensation_manifest_dirty: AtomicBool,
    #[cfg(target_os = "macos")]
    manifest_sync_snapshot: parking_lot::Mutex<Option<ManifestSyncSnapshot>>,
    segment_file_bytes: BTreeMap<u32, u64>,
    segment_live_records: BTreeMap<u32, u64>,
    /// Online physical replication can observe WAL records in source commit
    /// order while concurrent undo shard writes publish physical offsets in a
    /// different order. Hold those valid out-of-order frames until the missing
    /// contiguous prefix arrives; startup recovery remains strict after it has
    /// the complete durable WAL.
    replicated_pending_frames: BTreeMap<u64, Vec<u8>>,
    replicated_pending_bytes: usize,
    pub(crate) disk_bytes: u64,
    pub(crate) reclaimed_bytes: u64,
    pub(crate) purge_runs: u64,
    pub(crate) purge_errors: u64,
    pub(crate) purge_last_unix_ms: u64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    /// P2/14 — cumulative wall-clock spent under `decode_segment_file`
    /// during startup. Recorded once per process (sum of all segments)
    /// so SHOW STATUS can attribute "slow startup" to undo scan vs WAL.
    pub(crate) scan_total_ns: AtomicU64,
    keep_cache: parking_lot::Mutex<PurgeKeepCache>,
    /// P1/10 — `old_ptr → new_ptr` forwarding for records physically
    /// relocated by compaction. Compaction remaps active heads, while both
    /// the transaction and MVCC
    /// row-version chain reaches records through pointers that live
    /// OUTSIDE the undo manager — heap row headers (`RowHeader.undo_ptr`)
    /// and `UndoRecord.before_header.undo_ptr`, which compaction cannot
    /// rewrite atomically without a heap-lock-under-undo-lock inversion.
    /// `get` / `load_owned` consult this map on a miss so a stale ptr
    /// transparently resolves to the record's new home, keeping
    /// snapshot-pinned reads correct across relocation. Persisted with the
    /// ownership directory so prepared heads and compensation WAL aliases
    /// resolve after restart too. Entries whose target is no longer live
    /// are pruned on compaction.
    compaction_forwarded: HashMap<u64, u64>,
    /// Canonical physical pointer -> original WAL identity (replicas do not compact in lockstep).
    compaction_origins: HashMap<u64, u64>,
    /// P1/10 — highest compaction-lane (`COMPACTION_SHARD_LANE`) segment
    /// sequence ever allocated this process. Output segment ids are chosen
    /// strictly above it so a lane-15 id is NEVER reused within a run even
    /// after its segment is fully purged and detached — otherwise a stale
    /// `compaction_forwarded` entry could alias a reused id. Reset on
    /// restart; surviving output segments establish the allocation floor.
    compaction_seq_high: u32,
}

/// Per-txn keep-set cache for the purge planner. See `UndoManager.keep_cache`.
#[derive(Debug, Default)]
struct PurgeKeepCache {
    /// `txn_id -> last seen undo_head` from the previous purge call.
    head_per_txn: HashMap<u64, u64>,
    /// `txn_id -> set of undo_ptrs reachable from that txn's chain`.
    /// Stored disjointly per txn so a finalized txn's contribution can
    /// be removed in O(chain_size) without rescanning others.
    chain_per_txn: HashMap<u64, HashSet<u64>>,
    /// Union of `chain_per_txn` values, materialized for O(1) membership
    /// probes. Maintained alongside the per-txn sets — never rebuilt.
    flat_keep: HashSet<u64>,
}

pub(crate) type UndoTablespace = UndoManager;

pub(crate) fn publication_shard(ptr: u64) -> usize {
    shard_of_segment(undo_ptr_segment(ptr)) as usize
}

/// Stable admission owner shared with appenders. Reserving memory does not
/// inspect undo directory metadata and must not queue behind its write latch.
#[derive(Clone, Debug)]
pub(crate) struct UndoResidentAdmission(Arc<parking_lot::Mutex<ControlledMemoryCharge>>);

impl UndoResidentAdmission {
    pub(crate) fn reserve(
        &self,
        record: &UndoRecord,
    ) -> Result<UndoResidentReservation, QueryError> {
        reserve_undo_resident(&self.0, record)
    }
}

fn reserve_undo_resident(
    charge: &Arc<parking_lot::Mutex<ControlledMemoryCharge>>,
    record: &UndoRecord,
) -> Result<UndoResidentReservation, QueryError> {
    let bytes = if record.resident_size > 0 {
        record.resident_size as usize
    } else {
        usize::try_from(estimate_undo_resident_bytes(record)).unwrap_or(usize::MAX)
    };
    charge.lock().try_grow(bytes)?;
    Ok(UndoResidentReservation {
        charge: Arc::clone(charge),
        bytes,
        committed: false,
    })
}

pub(crate) struct UndoResidentReservation {
    charge: Arc<parking_lot::Mutex<ControlledMemoryCharge>>,
    bytes: usize,
    committed: bool,
}

impl UndoResidentReservation {
    fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for UndoResidentReservation {
    fn drop(&mut self) {
        if !self.committed {
            self.charge.lock().shrink(self.bytes);
        }
    }
}

/// Immutable result of resolving an undo pointer under the directory latch.
/// Segment I/O is deliberately deferred to [`Self::finish`], after the caller
/// releases the global undo latch. A captured `Arc<File>` remains valid across
/// compaction unlink, while SnapshotGuard/active-transaction retention keeps
/// the logical record from becoming purge-eligible.
pub(crate) enum PreparedUndoRead {
    Resident(UndoRecord),
    Segment {
        file: Arc<File>,
        segment_id: u32,
        offset: u64,
        file_end: Option<u64>,
    },
}

impl PreparedUndoRead {
    pub(crate) fn finish(self) -> Result<Option<UndoRecord>, QueryError> {
        match self {
            Self::Resident(record) => Ok(Some(record)),
            Self::Segment {
                file,
                segment_id,
                offset,
                file_end,
            } => read_undo_frame_from_open_file(&file, segment_id, offset, file_end),
        }
    }
}

fn read_undo_frame_from_open_file(
    file: &File,
    segment_id: u32,
    offset: u64,
    file_end: Option<u64>,
) -> Result<Option<UndoRecord>, QueryError> {
    if file_end.is_some_and(|end| offset.saturating_add(UNDO_FRAME_HEADER_LEN as u64) > end) {
        return Ok(None);
    }
    let mut header = [0u8; UNDO_FRAME_HEADER_LEN];
    match undo_read_exact_at(file, &mut header, offset) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(io_err(err)),
    }
    validate_undo_frame_header(&header, segment_id, offset)?;
    let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
    let payload_len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
    let checksum = u32::from_le_bytes(header[8..12].try_into().unwrap());
    let frame_version = u32::from_le_bytes(header[12..16].try_into().unwrap());
    if magic != UNDO_FRAME_MAGIC || frame_version != UNDO_TABLESPACE_VERSION {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt undo frame header (page-in)",
        ));
    }
    if payload_len == 0 || payload_len > MAX_UNDO_FRAME_PAYLOAD {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt undo frame length (page-in)",
        ));
    }
    let mut payload = vec![0u8; payload_len];
    match undo_read_exact_at(file, &mut payload, offset + UNDO_FRAME_HEADER_LEN as u64) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(io_err(err)),
    }
    if crc32fast::hash(&payload) != checksum {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt undo frame checksum (page-in)",
        ));
    }
    let mut record = decode_undo_record(&payload)?;
    record.id = make_undo_ptr(segment_id, offset)?;
    record.encoded_len = payload_len as u32;
    Ok(Some(record))
}

impl Default for UndoManager {
    fn default() -> Self {
        let shard_count = default_undo_write_shards();
        let buffer_limit = default_undo_buffer_bytes();
        let segment_pins = Arc::new(parking_lot::Mutex::new(UndoSegmentPins::default()));
        let write_shards: Arc<[UndoWriteShardHandle]> = (0..shard_count)
            .map(|i| {
                Arc::new(parking_lot::Mutex::new(UndoWriteShard::new(
                    i as u32,
                    DEFAULT_UNDO_SEGMENT_BYTES,
                    buffer_limit,
                    None,
                    Arc::clone(&segment_pins),
                )))
            })
            .collect();
        Self {
            resident_charge: Arc::new(parking_lot::Mutex::new(
                ControlledMemoryCharge::try_new(ControlledMemoryCategory::SystemMetadata, 0)
                    .expect("zero-byte undo governor reservation must succeed"),
            )),
            segment_pins,
            next_undo_id: 1,
            records: FastHashMap::default(),
            created_index: BTreeMap::new(),
            eviction_candidates: BTreeSet::new(),
            current_bytes: 0,
            resident_bytes: 0,
            mem_cache_limit_bytes: 0,
            evicted_records: 0,
            txn_bytes: FastHashMap::default(),
            txn_max_bytes: 0,
            concurrent_append_eligible: Arc::new(AtomicBool::new(false)),
            segment_readers: parking_lot::Mutex::new(SegmentReaderCache::default()),
            purged_records: 0,
            peak_bytes: 0,
            peak_records: 0,
            longest_chain_depth: 0,
            retention_pressure_count: 0,
            retention_guarantee: false,
            tablespace_max_bytes: 0,
            mem_soft_limit_bytes: 0,
            disk_cap_softfail_warn: false,
            disk_cap_softfail_warns_emitted: 0,
            mem_pressure_count: 0,
            segment_bytes: DEFAULT_UNDO_SEGMENT_BYTES,
            write_shards,
            next_shard: AtomicU64::new(0),
            undo_dir: None,
            strict_recovery: false,
            capture_active: Arc::new(AtomicBool::new(false)),
            archive_floor: Arc::new(AtomicU64::new(0)),
            manifest_path: None,
            compensated: HashSet::new(),
            discarded: HashSet::new(),
            compensation_manifest_dirty: AtomicBool::new(false),
            #[cfg(target_os = "macos")]
            manifest_sync_snapshot: parking_lot::Mutex::new(None),
            segment_file_bytes: BTreeMap::new(),
            segment_live_records: BTreeMap::new(),
            replicated_pending_frames: BTreeMap::new(),
            replicated_pending_bytes: 0,
            disk_bytes: 0,
            reclaimed_bytes: 0,
            purge_runs: 0,
            purge_errors: 0,
            purge_last_unix_ms: 0,
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            scan_total_ns: AtomicU64::new(0),
            keep_cache: parking_lot::Mutex::new(PurgeKeepCache::default()),
            compaction_forwarded: HashMap::new(),
            compaction_origins: HashMap::new(),
            compaction_seq_high: 0,
        }
    }
}

/// Accounting size of a record: the insert-time cached estimate when
/// available, else recomputed (records built before `insert_loaded_record`
/// stamped them, e.g. in unit tests).
fn record_size(record: &UndoRecord) -> u64 {
    if record.cached_size > 0 {
        u64::from(record.cached_size)
    } else {
        estimate_undo_record_bytes(record)
    }
}

/// Capacity-based memory owned by one live undo directory entry. This is
/// deliberately separate from `estimate_undo_record_bytes`, which is a
/// logical/admission estimate of the full record even after its payload has
/// been evicted. The fixed overhead conservatively covers the B-tree node and
/// creation/transaction side-index entries associated with the record.
pub(crate) fn estimate_undo_resident_bytes(record: &UndoRecord) -> u64 {
    const DIRECTORY_AND_INDEX_OVERHEAD: u64 = 128;

    let mut size = std::mem::size_of::<UndoRecord>() as u64 + DIRECTORY_AND_INDEX_OVERHEAD;
    size = size.saturating_add(match &record.kind {
        UndoKind::InsertBatch { row_ids } => {
            (row_ids.capacity() * std::mem::size_of::<RowId>()) as u64
        }
        UndoKind::UpdateDelta { before_cols } => {
            (before_cols.capacity() * std::mem::size_of::<u16>()) as u64
        }
        UndoKind::Insert | UndoKind::Update | UndoKind::Delete | UndoKind::DeleteHeader => 0,
    });
    if let Some(values) = &record.before_values {
        size = size.saturating_add((values.capacity() * std::mem::size_of::<Value>()) as u64);
        for value in values {
            let heap_capacity = match value {
                Value::Decimal(value) | Value::String(value) => value.capacity(),
                Value::DecimalWide(_) => value.decimal_heap_bytes(),
                Value::Bytes(value) | Value::Json(value) | Value::Geometry(value) => {
                    value.capacity()
                }
                Value::Vector(value) => value.capacity(),
                Value::SmallString(value) if value.spilled() => value.capacity(),
                _ => 0,
            };
            size = size.saturating_add(heap_capacity as u64);
        }
    }
    size = size.saturating_add(
        (record.before_external.capacity() * std::mem::size_of::<(usize, LobRef)>()) as u64,
    );
    size.saturating_add(
        (record.borrowed_external.capacity() * std::mem::size_of::<(usize, LobRef)>()) as u64,
    )
}

fn resident_size(record: &UndoRecord) -> u64 {
    if record.resident_size > 0 {
        u64::from(record.resident_size)
    } else {
        estimate_undo_resident_bytes(record)
    }
}

fn metadata_resident_bytes(record: &UndoRecord) -> u64 {
    const DIRECTORY_AND_INDEX_OVERHEAD: u64 = 128;
    (std::mem::size_of::<UndoRecord>() as u64)
        .saturating_add(DIRECTORY_AND_INDEX_OVERHEAD)
        .saturating_add(
            (record.before_external.capacity() * std::mem::size_of::<(usize, LobRef)>()) as u64,
        )
        .saturating_add(
            (record.borrowed_external.capacity() * std::mem::size_of::<(usize, LobRef)>()) as u64,
        )
}

pub(crate) fn estimate_undo_record_bytes(record: &UndoRecord) -> u64 {
    let mut size: u64 = 8 + 8 + 8 + 8 + 8 + 4;
    size += match &record.kind {
        UndoKind::Insert => 1,
        // Per-RowId footprint shrank from 8 B to 6 B with the
        // rowid-6bytes refactor; the rest of the per-entry envelope
        // (table_id 8 + ... + chain_depth 4) lives in the surrounding
        // record so we charge ~18 B per row, not 20.
        UndoKind::InsertBatch { row_ids } => 1 + (row_ids.len() as u64) * 18,
        UndoKind::Update | UndoKind::Delete | UndoKind::DeleteHeader => 1,
        UndoKind::UpdateDelta { before_cols } => 1 + 4 + (before_cols.len() as u64) * 2,
    };
    // table_id (8) + RowId (6) + before_header (~6) = 20-ish per
    // record (was 22 with 8-byte RowId).
    size += 18;
    if record.before_header.is_some() {
        size += 17;
    }
    if let Some(values) = &record.before_values {
        for v in values {
            size += match v {
                Value::Null => 1,
                Value::Int(_) | Value::UInt(_) => 9,
                Value::Float(_) | Value::Double(_) => 9,
                Value::String(s) => 5 + s.len() as u64,
                Value::SmallString(b) => 5 + b.len() as u64,
                Value::Decimal(s) => 5 + s.len() as u64,
                Value::Bytes(b) => 5 + b.len() as u64,
                // jsonb bytes are stored length-prefixed like Bytes.
                Value::Json(b) => 5 + b.len() as u64,
                Value::Geometry(b) => 5 + b.len() as u64,
                Value::Vector(b) => 5 + b.len() as u64,
                // P2/15 — DecimalCompact / Date / Time / DateTime / Year
                // use native binary tags in `encode_row_into` (see the
                // TAG_DECIMAL_COMPACT, TAG_DATE etc. paths there). The
                // pre-2026 accounting used to add `to_display_string()`
                // length here — a +5-10× overestimate that made
                // `current_bytes` and the admission heuristic both
                // pessimistic. Use the real on-disk sizes instead.
                Value::DecimalCompact(..) => 1 + 1 + 8, // tag + scale + mag
                Value::Decimal128(..) => 1 + 1 + 16,
                Value::DecimalWide(_) => 1 + 1 + 5 + v.decimal_heap_bytes() as u64,
                Value::Date(_) => 1 + 4,
                Value::Time(_) | Value::DateTime(_) | Value::Timestamp(_) => 1 + 8,
                Value::Year(_) => 1 + 1,
            };
        }
    }
    // Externalized columns cost a fixed 4 (count) + 24 each (col + 20B
    // generation-bearing descriptor) — the multi-GB bytes live off-page.
    size += 8
        + (record.before_external.len() as u64) * 24
        + (record.borrowed_external.len() as u64) * 24;
    size
}

fn make_undo_ptr(segment_id: u32, offset: u64) -> Result<u64, QueryError> {
    let offset = u32::try_from(offset).map_err(|_| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            "undo segment offset exceeds pointer range",
        )
    })?;
    if segment_id == 0 || offset == 0 {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "invalid undo pointer",
        ));
    }
    Ok(((segment_id as u64) << 32) | u64::from(offset))
}

pub(crate) fn undo_ptr_segment(ptr: u64) -> u32 {
    (ptr >> 32) as u32
}

fn undo_ptr_offset(ptr: u64) -> u64 {
    ptr & 0xffff_ffff
}

fn segment_file_name(segment_id: u32) -> String {
    format!("undo-{segment_id:08x}.seg")
}

fn parse_segment_name(name: &str) -> Option<u32> {
    let rest = name.strip_prefix("undo-")?.strip_suffix(".seg")?;
    u32::from_str_radix(rest, 16).ok()
}

/// P2/14 — coarse log-scale histogram for I/O latency observability.
/// Each bucket counts observations in `[prev_upper, upper)` (the last
/// bucket is the unbounded `>=1s` tail). Cheap fast-path: pick the
/// bucket with a constant-time match and `fetch_add`.
const LATENCY_BUCKET_BOUNDS_NS: [u64; 7] = [
    1_000,         // <1us
    10_000,        // <10us
    100_000,       // <100us
    1_000_000,     // <1ms
    10_000_000,    // <10ms
    100_000_000,   // <100ms
    1_000_000_000, // <1s
];
const LATENCY_BUCKET_COUNT: usize = LATENCY_BUCKET_BOUNDS_NS.len() + 1;

#[derive(Debug)]
pub(crate) struct LatencyHistogram {
    buckets: [AtomicU64; LATENCY_BUCKET_COUNT],
    total_ns: AtomicU64,
    samples: AtomicU64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        // AtomicU64 is Copy-less so we can't use `[AtomicU64::new(0); N]`;
        // build the array element-by-element.
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            total_ns: AtomicU64::new(0),
            samples: AtomicU64::new(0),
        }
    }
}

impl LatencyHistogram {
    fn record(&self, ns: u64) {
        let mut idx = LATENCY_BUCKET_COUNT - 1;
        for (i, bound) in LATENCY_BUCKET_BOUNDS_NS.iter().enumerate() {
            if ns < *bound {
                idx = i;
                break;
            }
        }
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.total_ns.fetch_add(ns, Ordering::Relaxed);
        self.samples.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn samples(&self) -> u64 {
        self.samples.load(Ordering::Relaxed)
    }

    pub(crate) fn total_ns(&self) -> u64 {
        self.total_ns.load(Ordering::Relaxed)
    }

    /// Return the upper-bound of the bucket that contains the requested
    /// percentile (0..100). Approximate to bucket granularity (factor
    /// of 10). Returns `u64::MAX` if the percentile falls in the
    /// unbounded tail bucket.
    pub(crate) fn percentile(&self, pct: u8) -> u64 {
        let samples = self.samples();
        if samples == 0 {
            return 0;
        }
        let target = (samples * pct as u64 + 99) / 100;
        let mut acc: u64 = 0;
        for (i, b) in self.buckets.iter().enumerate() {
            acc = acc.saturating_add(b.load(Ordering::Relaxed));
            if acc >= target {
                return LATENCY_BUCKET_BOUNDS_NS.get(i).copied().unwrap_or(u64::MAX);
            }
        }
        u64::MAX
    }
}

/// Output of [`decode_segment_file`] — pure-decode result that callers
/// can merge into an `UndoManager` mirror in series. Separated from
/// the mutating apply step so multiple segments can be decoded in
/// parallel during startup (P1/7).
struct DecodedSegment {
    segment_id: u32,
    /// Records decoded from the segment, in append order. Their `id`
    /// is already patched to the canonical `make_undo_ptr` value.
    records: Vec<UndoRecord>,
    /// Byte offset of the first byte past the last valid frame — what
    /// `segment_file_bytes` should record for this segment.
    valid_end: u64,
    /// `Some(len)` if the segment file needs to be truncated to that
    /// length to drop a partial / corrupt tail; `None` otherwise.
    truncate_to: Option<u64>,
    /// Highest canonical undo pointer seen in the segment, including frames
    /// discarded by startup filtering. Keeps allocation watermarks monotone
    /// without retaining obsolete payloads.
    max_undo_id: u64,
    /// Finalized records skipped by the startup recovery filter.
    discarded_records: u64,
}

fn validate_scanned_undo_tail(
    decoded: &DecodedSegment,
    writable_tails: &std::collections::HashMap<u32, u32>,
    forced: bool,
) -> Result<(), QueryError> {
    if decoded.truncate_to.is_some()
        && !forced
        && writable_tails.get(&shard_of_segment(decoded.segment_id)) != Some(&decoded.segment_id)
    {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            format!(
                "incomplete sealed undo segment {:#x}; refusing tail truncation",
                decoded.segment_id
            ),
        ));
    }
    Ok(())
}

fn decode_segment_file(
    path: &Path,
    segment_id: u32,
    force_recover: bool,
) -> Result<DecodedSegment, QueryError> {
    decode_segment_file_filtered(path, segment_id, force_recover, None)
}

/// Decode one segment while optionally retaining undo only for the supplied
/// in-flight transaction ids. Startup has no surviving read snapshots: after
/// WAL REDO/UNDO has completed, finalized creators' versions can never be
/// observed again. Read the fixed-width txn id before decoding the variable
/// payload so a large obsolete `InsertBatch.row_ids` vector is never
/// materialized merely to be purged immediately after open.
fn decode_segment_file_filtered(
    path: &Path,
    segment_id: u32,
    force_recover: bool,
    retained_txn_ids: Option<&HashSet<u64>>,
) -> Result<DecodedSegment, QueryError> {
    // Read the whole segment once instead of frame-by-frame. A 64 MiB
    // segment holds ~500 K small undo frames; the old per-frame
    // `seek + read_exact(header) + vec![0u8; payload_len] + read_exact`
    // paid two syscalls and one allocation per frame during startup.
    // Segments are bounded (`segment_bytes`, default 64 MiB) and decoded
    // one at a time (the mem-cache-bounded path is serial; the unbounded
    // path is not memory-constrained), so a single transient buffer is
    // cheap and the payload can be decoded straight from the slice with
    // zero per-frame copy.
    let buf = fs::read(path).map_err(io_err)?;
    let len = buf.len() as u64;
    if len < UNDO_SEGMENT_HEADER_LEN {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            format!("corrupt undo segment header in {}: length {len}, expected at least {UNDO_SEGMENT_HEADER_LEN}", path.display()),
        ));
    }
    if &buf[..8] != UNDO_SEGMENT_MAGIC {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt undo segment magic",
        ));
    }
    let version = u32::from_le_bytes(buf[8..12].try_into().unwrap());
    let stored_id = u32::from_le_bytes(buf[12..16].try_into().unwrap());
    if version != UNDO_TABLESPACE_VERSION
        || stored_id != segment_id
        || u32::from_le_bytes(buf[60..64].try_into().unwrap()) != crc32fast::hash(&buf[..60])
        || buf[16..24]
            .iter()
            .chain(buf[40..60].iter())
            .any(|b| *b != 0)
        || buf[24..40] != undo_storage_uuid(path.parent().unwrap())?
    {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            format!("corrupt undo segment header in {}: version {version}, segment {stored_id:#x}; expected version {UNDO_TABLESPACE_VERSION}, segment {segment_id:#x}", path.display()),
        ));
    }
    // Phase 6 — under MYDB_UNDO_FORCE_RECOVER, a *mid-stream* corrupt frame
    // is treated like a torn tail: stop at the corrupt offset, drop
    // everything from there on (frames are append-order, so the suffix is
    // unrecoverable anyway), warn, and keep the CRC-clean prefix. The
    // affected undo versions become unreadable (consistent reads needing
    // them get 1236; rollback of those txns may be incomplete) — the
    // explicit availability-over-completeness trade the operator opted in
    // to. Without the flag, corruption still hard-fails startup.
    let bail = |what: &str, offset: u64| -> Result<Option<u64>, QueryError> {
        if force_recover {
            eprintln!(
                "[mydb][undo] WARNING: {what} at segment {segment_id:#x} offset {offset}; \
                 MYDB_UNDO_FORCE_RECOVER set — truncating segment tail and continuing \
                 (undo versions in the dropped tail are lost)"
            );
            Ok(Some(offset))
        } else {
            Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                format!(
                    "{what} at undo segment {segment_id:#x} offset {offset}; \
                     set MYDB_UNDO_FORCE_RECOVER=1 to truncate the corrupt tail and continue"
                ),
            ))
        }
    };
    let mut offset = UNDO_SEGMENT_HEADER_LEN;
    let mut records: Vec<UndoRecord> = Vec::new();
    let mut truncate_to: Option<u64> = None;
    let mut max_undo_id = 0u64;
    let mut discarded_records = 0u64;
    loop {
        if offset >= len {
            break;
        }
        let hstart = offset as usize;
        // Fewer than a full header of bytes remain → torn tail (equivalent
        // to the old `read_exact(frame_header)` returning UnexpectedEof).
        if offset.saturating_add(UNDO_FRAME_HEADER_LEN as u64) > len {
            truncate_to = Some(offset);
            break;
        }
        let frame_header = &buf[hstart..hstart + UNDO_FRAME_HEADER_LEN];
        if let Err(err) = validate_undo_frame_header(frame_header, segment_id, offset) {
            truncate_to = bail(err.message(), offset)?;
            break;
        }
        let magic = u32::from_le_bytes(frame_header[0..4].try_into().unwrap());
        let payload_len = u32::from_le_bytes(frame_header[4..8].try_into().unwrap()) as usize;
        let checksum = u32::from_le_bytes(frame_header[8..12].try_into().unwrap());
        let frame_version = u32::from_le_bytes(frame_header[12..16].try_into().unwrap());
        if magic != UNDO_FRAME_MAGIC || frame_version != UNDO_TABLESPACE_VERSION {
            truncate_to = bail("corrupt undo frame header", offset)?;
            break;
        }
        if payload_len == 0 || payload_len > MAX_UNDO_FRAME_PAYLOAD {
            truncate_to = bail("corrupt undo frame length", offset)?;
            break;
        }
        if offset
            .saturating_add(UNDO_FRAME_HEADER_LEN as u64)
            .saturating_add(payload_len as u64)
            > len
        {
            truncate_to = Some(offset);
            break;
        }
        let pstart = hstart + UNDO_FRAME_HEADER_LEN;
        let payload = &buf[pstart..pstart + payload_len];
        if crc32fast::hash(payload) != checksum {
            truncate_to = bail("corrupt undo frame checksum", offset)?;
            break;
        }
        let ptr = make_undo_ptr(segment_id, offset)?;
        max_undo_id = max_undo_id.max(ptr);
        if retained_txn_ids.is_none() {
            let mut record = match decode_undo_record(payload) {
                Ok(record) => record,
                Err(err) => {
                    if force_recover {
                        truncate_to = bail("undecodable undo frame", offset)?;
                        break;
                    }
                    return Err(err);
                }
            };
            record.id = ptr;
            record.encoded_len = payload_len as u32;
            records.push(record);
        } else {
            let mut record = match decode_undo_record_metadata(payload) {
                Ok(record) => record,
                Err(err) => {
                    if force_recover {
                        truncate_to = bail("undecodable undo frame", offset)?;
                        break;
                    }
                    return Err(err);
                }
            };
            if retained_txn_ids.is_some_and(|ids| ids.contains(&record.txn_id)) {
                record.id = ptr;
                records.push(record);
            } else if matches!(record.kind, UndoKind::Insert | UndoKind::InsertBatch { .. }) {
                // INSERT undo has no before-image, external LOB ownership, or
                // tombstone cleanup. Once its creator is finalized and startup
                // has no surviving snapshot, the whole payload is obsolete.
                // The metadata decoder validates but never allocates an
                // InsertBatch's RowId vector.
                discarded_records = discarded_records.saturating_add(1);
            } else {
                // UPDATE/DELETE may still own post-purge cleanup even though
                // their historical row image is no longer observable. Retain
                // only the tiny metadata needed by the established apply_purge
                // path: external LOB descriptors and/or a DELETE tombstone
                // target. The heavy before-image stays on disk.
                let needs_cleanup =
                    matches!(record.kind, UndoKind::Delete | UndoKind::DeleteHeader)
                        || !record.before_external.is_empty();
                if needs_cleanup {
                    if !matches!(record.kind, UndoKind::Delete | UndoKind::DeleteHeader) {
                        // Column lists are irrelevant to LOB cleanup.
                        record.kind = UndoKind::Update;
                    }
                    record.id = ptr;
                    record.prev_undo_ptr = None;
                    record.before_header = None;
                    record.chain_depth = 0;
                    record.cached_size =
                        u32::try_from(estimate_undo_record_bytes(&record)).unwrap_or(u32::MAX);
                    record.resident = false;
                    record.encoded_len = payload_len as u32;
                    records.push(record);
                } else {
                    discarded_records = discarded_records.saturating_add(1);
                }
            }
        }
        offset += UNDO_FRAME_HEADER_LEN as u64 + payload_len as u64;
    }
    Ok(DecodedSegment {
        segment_id,
        records,
        valid_end: offset,
        truncate_to,
        max_undo_id,
        discarded_records,
    })
}

/// Phase 6 — escape hatch: when set, recovery truncates a corrupt undo
/// segment tail and continues instead of refusing to start.
fn undo_force_recover() -> bool {
    std::env::var("MYDB_UNDO_FORCE_RECOVER")
        .map(|v| {
            let t = v.trim();
            t == "1" || t.eq_ignore_ascii_case("true")
        })
        .unwrap_or(false)
}

fn write_segment_header(file: &mut File, segment_id: u32, path: &Path) -> Result<(), QueryError> {
    let header = encode_undo_segment_header(segment_id, undo_storage_uuid(path.parent().unwrap())?);
    file.seek(SeekFrom::Start(0)).map_err(io_err)?;
    file.write_all(&header).map_err(io_err)?;
    file.flush().map_err(io_err)
}

fn write_segment_header_into(
    buf: &mut Vec<u8>,
    segment_id: u32,
    directory: &Path,
) -> Result<(), QueryError> {
    let header = encode_undo_segment_header(segment_id, undo_storage_uuid(directory)?);
    buf.clear();
    buf.extend_from_slice(&header);
    Ok(())
}

/// Decode a single undo frame directly from a segment file at `offset`,
/// with no lock and no manager state — used by build-then-swap phase 2
/// to page in an evicted before-image from an immutable candidate file.
/// Returns `Ok(None)` when the frame is truncated / past EOF, `Err` on a
/// corrupt-but-present frame. Mirrors `read_frame_from_segment`.
fn read_undo_frame_at(path: &Path, offset: u64) -> Result<Option<UndoRecord>, QueryError> {
    let file = match OpenOptions::new().read(true).open(path) {
        Ok(f) => f,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(io_err(err)),
    };
    let mut segment_header = [0; UNDO_SEGMENT_HEADER_LEN as usize];
    undo_read_exact_at(&file, &mut segment_header, 0).map_err(io_err)?;
    let segment_id = u32::from_le_bytes(segment_header[12..16].try_into().unwrap());
    let mut header = [0u8; UNDO_FRAME_HEADER_LEN];
    match undo_read_exact_at(&file, &mut header, offset) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(io_err(err)),
    }
    validate_undo_frame_header(&header, segment_id, offset)?;
    let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
    let payload_len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
    let checksum = u32::from_le_bytes(header[8..12].try_into().unwrap());
    let frame_version = u32::from_le_bytes(header[12..16].try_into().unwrap());
    if magic != UNDO_FRAME_MAGIC || frame_version != UNDO_TABLESPACE_VERSION {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt undo frame header (compaction)",
        ));
    }
    if payload_len == 0 || payload_len > MAX_UNDO_FRAME_PAYLOAD {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt undo frame length (compaction)",
        ));
    }
    let mut payload = vec![0u8; payload_len];
    match undo_read_exact_at(&file, &mut payload, offset + UNDO_FRAME_HEADER_LEN as u64) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(io_err(err)),
    }
    if crc32fast::hash(&payload) != checksum {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt undo frame checksum (compaction)",
        ));
    }
    Ok(Some(decode_undo_record(&payload)?))
}

fn io_err(err: std::io::Error) -> QueryError {
    error(
        crate::error::catalog::ER_GET_ERRNO,
        format!("undo tablespace I/O error: {err}"),
    )
}

/// Phase 2 — max cached segment read handles. Each handle is a single fd;
/// a process touching more than this many distinct segments for page-in
/// evicts the least-recently-used handle (which reopens lazily if touched
/// again). Historically the cache was flushed wholesale (`clear()`) on
/// reaching the cap — a cold or wide MVCC scan straddling many segments
/// then thrashed the *hot* handles it had just opened. LRU keeps the
/// working set resident instead.
const UNDO_SEGMENT_READER_CACHE_CAP: usize = 256;

/// Bounded LRU cache of open segment read handles. `tick` is a monotonic
/// access counter; each entry records the tick at its last touch, so the
/// eviction victim is the entry with the smallest recorded tick. The cap
/// is small (256) so the O(n) victim scan on the rare over-cap insert is
/// negligible next to the `open()` it guards.
#[derive(Debug, Default)]
struct SegmentReaderCache {
    map: HashMap<u32, (std::sync::Arc<File>, u64)>,
    tick: u64,
    evictions: u64,
}

impl SegmentReaderCache {
    fn get(&mut self, segment_id: u32) -> Option<std::sync::Arc<File>> {
        self.tick = self.tick.wrapping_add(1);
        let tick = self.tick;
        let entry = self.map.get_mut(&segment_id)?;
        entry.1 = tick;
        Some(entry.0.clone())
    }

    fn insert(&mut self, segment_id: u32, file: std::sync::Arc<File>) {
        self.tick = self.tick.wrapping_add(1);
        let tick = self.tick;
        if !self.map.contains_key(&segment_id) && self.map.len() >= UNDO_SEGMENT_READER_CACHE_CAP {
            if let Some((&victim, _)) = self.map.iter().min_by_key(|(_, (_, t))| *t) {
                self.map.remove(&victim);
                self.evictions = self.evictions.saturating_add(1);
            }
        }
        self.map.insert(segment_id, (file, tick));
    }

    fn remove(&mut self, segment_id: u32) {
        self.map.remove(&segment_id);
    }
}

/// Positioned read (no seek), portable across unix/windows/other. Used
/// by the page-in path so concurrent reads share one `Arc<File>`.
#[cfg(unix)]
fn undo_read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    std::os::unix::fs::FileExt::read_exact_at(file, buf, offset)
}

#[cfg(windows)]
fn undo_read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    file.read_exact_at(buf, offset)
}

#[cfg(not(any(unix, windows)))]
fn undo_read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    let mut file = file.try_clone()?;
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(buf)
}

impl UndoManager {
    pub(crate) fn apply_replicated_wal_record(
        &mut self,
        undo_id: u64,
        bytes: &[u8],
    ) -> Result<(), QueryError> {
        self.admit_replicated_frame(undo_id, bytes.len())?;
        if !self.records.contains_key(&self.canonical_ptr(undo_id)) {
            let mut record = decode_undo_record(bytes)
                .or_else(|_| serde_json::from_slice::<UndoRecord>(bytes).map_err(json_err))?;
            record.id = undo_id.max(record.id).max(1);
            self.insert_loaded_record(record)?;
        }
        self.queue_replicated_frame(undo_id, bytes)
    }

    fn check_replicated_frame_admission(&self, undo_id: u64, len: usize) -> Result<(), QueryError> {
        validate_undo_payload_len(len)?;
        if !self.replicated_pending_frames.contains_key(&undo_id)
            && (self.replicated_pending_frames.len() >= MAX_REPLICATED_PENDING_FRAMES
                || len > MAX_REPLICATED_PENDING_BYTES.saturating_sub(self.replicated_pending_bytes))
        {
            return Err(error(crate::error::catalog::ER_OUTOFMEMORY,
                "replicated undo reorder buffer limit exceeded; missing segment prefixes must be recovered before advancing the applier"));
        }
        Ok(())
    }

    fn admit_replicated_frame(&mut self, undo_id: u64, len: usize) -> Result<(), QueryError> {
        validate_undo_payload_len(len)?;
        if let Err(err) = self.check_replicated_frame_admission(undo_id, len) {
            if self.undo_dir.is_none() {
                return Err(err);
            }
            // A network chunk may contain many ordered frames. Retire its
            // contiguous prefix before treating this as a reorder overflow.
            self.drain_replicated_pending_frames()?;
            self.check_replicated_frame_admission(undo_id, len)?;
        }
        Ok(())
    }

    fn queue_replicated_frame(&mut self, undo_id: u64, bytes: &[u8]) -> Result<(), QueryError> {
        // A relocated source id already has a durable local home. Replaying
        // its WAL twin must not reintroduce the obsolete physical gap.
        if self.canonical_ptr(undo_id) != undo_id {
            return Ok(());
        }
        self.admit_replicated_frame(undo_id, bytes.len())?;
        if self.replicated_pending_frames.contains_key(&undo_id) {
            return Ok(());
        }
        let mut owned = Vec::new();
        owned.try_reserve_exact(bytes.len()).map_err(|_| {
            error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "cannot reserve replicated undo reorder buffer",
            )
        })?;
        owned.extend_from_slice(bytes);
        let capacity = owned.capacity();
        self.resident_charge.lock().try_grow(capacity)?;
        self.replicated_pending_bytes += owned.len();
        self.replicated_pending_frames.insert(undo_id, owned);
        Ok(())
    }

    pub(crate) fn has_replicated_pending_frames(&self) -> bool {
        !self.replicated_pending_frames.is_empty()
    }

    pub(crate) fn persist_replicated_frames_for_promotion(&mut self) -> Result<(), QueryError> {
        self.drain_replicated_pending_frames()?;
        if self.replicated_pending_frames.is_empty() {
            return Ok(());
        }
        // Physical allocation order is independent of transaction WAL order.
        // A crashed source can lose an unlogged allocation preceding a fully
        // replicated frame forever. The frame is complete: publish it at a
        // local offset with a durable source-id alias, just as crash recovery
        // does. Logical transaction chains remain unchanged and promotion's
        // normal rollback still rejects missing logical before-images.
        let frames = std::mem::take(&mut self.replicated_pending_frames);
        let borrowed = frames
            .iter()
            .map(|(ptr, bytes)| (*ptr, bytes.as_slice()))
            .collect();
        if let Err(err) = self.repair_recovery_segments_from_wal(borrowed) {
            self.replicated_pending_frames = frames;
            return Err(err);
        }
        self.replicated_pending_bytes = 0;
        self.resident_charge
            .lock()
            .shrink(frames.values().map(Vec::capacity).sum());
        self.evict_to_cache_budget();
        Ok(())
    }

    pub(crate) fn drain_replicated_pending_frames(&mut self) -> Result<(), QueryError> {
        loop {
            let keys = self
                .replicated_pending_frames
                .keys()
                .copied()
                .collect::<Vec<_>>();
            let mut virtual_lengths = BTreeMap::<u32, u64>::new();
            let mut ready_keys = Vec::new();
            for ptr in keys {
                let segment_id = undo_ptr_segment(ptr);
                let offset = undo_ptr_offset(ptr);
                let len = if let Some(len) = virtual_lengths.get(&segment_id) {
                    *len
                } else {
                    let path = self.segment_path(segment_id)?;
                    let len = match fs::metadata(&path) {
                        Ok(meta) => meta.len().max(UNDO_SEGMENT_HEADER_LEN),
                        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                            UNDO_SEGMENT_HEADER_LEN
                        }
                        Err(err) => return Err(io_err(err)),
                    };
                    virtual_lengths.insert(segment_id, len);
                    len
                };
                if offset > len {
                    continue;
                }
                let payload_len = self
                    .replicated_pending_frames
                    .get(&ptr)
                    .map(Vec::len)
                    .expect("pending replicated undo key");
                if offset == len {
                    virtual_lengths.insert(
                        segment_id,
                        offset + UNDO_FRAME_HEADER_LEN as u64 + payload_len as u64,
                    );
                }
                ready_keys.push(ptr);
            }
            if ready_keys.is_empty() {
                break;
            }
            let mut ready = Vec::<(u64, Vec<u8>)>::with_capacity(ready_keys.len());
            for ptr in ready_keys {
                ready.push((
                    ptr,
                    self.replicated_pending_frames
                        .remove(&ptr)
                        .expect("pending replicated undo key"),
                ));
            }
            let borrowed = ready
                .iter()
                .map(|(ptr, bytes)| (*ptr, bytes.as_slice()))
                .collect::<Vec<_>>();
            if let Err(err) = self.repair_segments_from_wal(borrowed) {
                for (ptr, bytes) in ready {
                    self.replicated_pending_frames.insert(ptr, bytes);
                }
                return Err(err);
            }
            self.replicated_pending_bytes -=
                ready.iter().map(|(_, bytes)| bytes.len()).sum::<usize>();
            let released = ready.iter().map(|(_, bytes)| bytes.capacity()).sum();
            self.resident_charge.lock().shrink(released);
        }
        // The ordinary source append path enforces the two-level undo cache
        // after publishing each durable frame. Replicated frames take a
        // different reorder/repair path, so apply the same policy only after
        // every contiguous frame above is on disk. Omitting this call kept all
        // decoded before-images resident forever and could consume the entire
        // process memory target during a fast catch-up even though
        // `MYDB_UNDO_MEM_CACHE_LIMIT` was 64 MiB.
        self.evict_to_cache_budget();
        Ok(())
    }

    pub(crate) fn from_wal(
        records: &(impl crate::wal::WalRecordSource + ?Sized),
    ) -> Result<Self, QueryError> {
        let mut undo = Self::default();
        for record in records
            .records()
            .only(&[crate::wal::WalRecordType::UndoRecord])
        {
            let record = record?;
            if let WalPayload::UndoRecord { undo_id, bytes } = &record.payload {
                let mut undo_record = decode_undo_record(bytes)
                    .or_else(|_| serde_json::from_slice::<UndoRecord>(bytes).map_err(json_err))?;
                undo_record.id = (*undo_id).max(undo_record.id).max(1);
                undo.insert_loaded_record(undo_record)?;
            }
        }
        for record in records
            .records()
            .only(&[crate::wal::WalRecordType::RollbackTo])
        {
            let record = record?;
            if let WalPayload::RollbackTo { compensated, .. } = &record.payload {
                undo.mark_compensated(compensated);
            }
        }
        undo.refresh_metrics();
        Ok(undo)
    }

    #[cfg(test)]
    pub(crate) fn open_with_segments(
        wal_records: &(impl crate::wal::WalRecordSource + ?Sized),
        data_dir: &Path,
        segment_bytes: u64,
    ) -> Result<Self, QueryError> {
        Self::open_with_segments_impl(wal_records, data_dir, segment_bytes, None, false, false)
    }

    pub(crate) fn open_with_segments_for_replica(
        wal_records: &(impl crate::wal::WalRecordSource + ?Sized),
        data_dir: &Path,
        segment_bytes: u64,
    ) -> Result<Self, QueryError> {
        Self::open_with_segments_impl(wal_records, data_dir, segment_bytes, None, true, false)
    }

    /// A sealed image must keep source undo segment identities and offsets.
    /// Unlike a live receiver, the captured prefix is complete: gaps are fatal.
    pub(crate) fn open_preserving_source(
        wal_records: &(impl crate::wal::WalRecordSource + ?Sized),
        data_dir: &Path,
        segment_bytes: u64,
    ) -> Result<Self, QueryError> {
        Self::open_with_segments_impl(wal_records, data_dir, segment_bytes, None, false, true)
    }

    /// Production startup path. WAL recovery has already resolved ordinary
    /// crash losers and there are no process-surviving read snapshots, so only
    /// ACTIVE/PREPARED creators can still need their undo chains.
    pub(crate) fn open_with_segments_for_recovery(
        wal_records: &(impl crate::wal::WalRecordSource + ?Sized),
        data_dir: &Path,
        segment_bytes: u64,
        retained_txn_ids: &HashSet<u64>,
        strict_recovery: bool,
    ) -> Result<Self, QueryError> {
        Self::open_with_segments_impl(
            wal_records,
            data_dir,
            segment_bytes,
            Some(retained_txn_ids),
            false,
            strict_recovery,
        )
    }

    pub(crate) fn open_read_only(data_dir: &Path, segment_bytes: u64) -> Result<Self, QueryError> {
        let dir = crate::storage::naming::system_undo_dir(data_dir);
        let mut undo = Self {
            undo_dir: Some(dir.clone()),
            strict_recovery: true,
            manifest_path: Some(dir.join("undo.manifest")),
            segment_bytes,
            mem_cache_limit_bytes: default_undo_mem_cache_bytes(),
            ..Self::default()
        };
        undo.load_manifest_mode(false)?;
        undo.scan_segments(None)?;
        undo.refresh_metrics();
        undo.evict_to_cache_budget();
        Ok(undo)
    }

    fn open_with_segments_impl(
        wal_records: &(impl crate::wal::WalRecordSource + ?Sized),
        data_dir: &Path,
        segment_bytes: u64,
        retained_txn_ids: Option<&HashSet<u64>>,
        allow_replication_gaps: bool,
        strict_recovery: bool,
    ) -> Result<Self, QueryError> {
        let undo_dir = crate::storage::naming::system_undo_dir(data_dir);
        let legacy = undo_dir.join("undo.tbl");
        if legacy.exists() {
            return Err(error(crate::error::catalog::ER_UNKNOWN_ERROR,
                "legacy undo/undo.tbl is incompatible with the segmented undo tablespace; use a new data directory or rebuild data",
            ));
        }
        crate::storage::control::Control::admit(data_dir)?;
        fs::create_dir_all(&undo_dir).map_err(io_err)?;
        if segment_bytes > u32::MAX as u64 {
            return Err(error(
                crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                "mydb_undo_segment_bytes exceeds the 32-bit undo offset limit",
            ));
        }
        let segment_bytes = segment_bytes.max(UNDO_SEGMENT_HEADER_LEN * 2);
        let mut undo = Self {
            undo_dir: Some(undo_dir.clone()),
            strict_recovery,
            manifest_path: Some(undo_dir.join("undo.manifest")),
            segment_bytes,
            // Phase 3 — learn the resident cap up front so recovery can
            // page-bound itself instead of loading the whole tablespace
            // into RAM (the facade re-applies the same value post-open).
            mem_cache_limit_bytes: default_undo_mem_cache_bytes(),
            ..Self::default()
        };
        // Plumb undo_dir + segment_bytes into each shard so they can
        // open their own active segment file.
        for shard_mu in undo.write_shards.iter() {
            let mut shard = shard_mu.lock();
            shard.undo_dir = Some(undo_dir.clone());
            shard.segment_bytes = segment_bytes;
        }
        undo.load_manifest()?;
        undo.scan_segments(retained_txn_ids)?;
        // Frames present in the WAL but missing from the segment files —
        // the crash window the segment tail buffer (and, before it, the
        // unsynced per-frame write) leaves open. They are re-persisted
        // below so the files and the live id space agree; otherwise a
        // fresh append would allocate the same ptr as a WAL-recovered
        // record and collide.
        let mut repair_frames: Vec<(u64, usize)> = Vec::new();
        let mut repair_locator_charge =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                0,
            )?;
        for record_index in 0..wal_records.len() {
            if let Some(record) = wal_records.undo_record(record_index)? {
                let undo_id = &record.undo_id;
                let bytes = record.bytes.as_ref();
                let mut undo_record = if let Some(retained) = retained_txn_ids {
                    let mut metadata = decode_undo_record_metadata(bytes)?;
                    let retained_payload = retained.contains(&metadata.txn_id);
                    if !retained_payload {
                        if matches!(
                            metadata.kind,
                            UndoKind::Insert | UndoKind::InsertBatch { .. }
                        ) {
                            continue;
                        }
                        let needs_cleanup =
                            matches!(metadata.kind, UndoKind::Delete | UndoKind::DeleteHeader)
                                || !metadata.before_external.is_empty();
                        if !needs_cleanup {
                            continue;
                        }
                        if !matches!(metadata.kind, UndoKind::Delete | UndoKind::DeleteHeader) {
                            metadata.kind = UndoKind::Update;
                        }
                        metadata.prev_undo_ptr = None;
                        metadata.before_header = None;
                        metadata.chain_depth = 0;
                    }
                    metadata
                } else {
                    decode_undo_record(bytes)?
                };
                undo_record.id = (*undo_id).max(undo_record.id).max(1);
                if undo
                    .records
                    .contains_key(&undo.canonical_ptr(undo_record.id))
                {
                    continue;
                }
                let ptr = undo_record.id;
                undo.insert_loaded_record(undo_record)?;
                if undo_ptr_segment(ptr) != 0 {
                    // Reconstruct only missing frames that recovery retained.
                    // The recovery repairer already handles allocation gaps by
                    // relocating retained frames and durably publishing aliases;
                    // restoring obsolete prefixes merely rehydrates purged WAL.
                    repair_locator_charge.try_grow(4 * std::mem::size_of::<(u64, usize)>())?;
                    repair_frames.push((ptr, record_index));
                }
            }
        }
        // Sort compact locators, then materialize at most one repair batch.
        // Payloads must not remain resident for the entire WAL suffix.
        repair_frames.sort_unstable_by_key(|(ptr, _)| *ptr);
        let mut pending = repair_frames.into_iter().peekable();
        while pending.peek().is_some() {
            let mut batch = Vec::new();
            let mut batch_bytes = 0usize;
            while let Some((ptr, index)) = pending.next() {
                let record = wal_records.record(index)?;
                if let WalPayload::UndoRecord { bytes, .. } = &record.payload {
                    batch_bytes += bytes.len();
                }
                batch.push((ptr, record));
                if batch_bytes >= 8 * 1024 * 1024 {
                    break;
                }
            }
            let frames = batch
                .iter()
                .map(|(ptr, record)| {
                    let WalPayload::UndoRecord { bytes, .. } = &record.payload else {
                        unreachable!()
                    };
                    (*ptr, bytes.as_slice())
                })
                .collect::<Vec<_>>();
            if allow_replication_gaps {
                for (ptr, bytes) in frames {
                    undo.queue_replicated_frame(ptr, bytes)?;
                }
                undo.drain_replicated_pending_frames()?;
            } else if retained_txn_ids.is_some() {
                undo.repair_recovery_segments_from_wal(frames)?;
            } else {
                undo.repair_segments_from_wal(frames)?;
            }
        }
        for record in wal_records
            .records()
            .only(&[crate::wal::WalRecordType::RollbackTo])
        {
            let record = record?;
            if let WalPayload::RollbackTo { compensated, .. } = &record.payload {
                undo.mark_compensated(compensated);
            }
        }
        if retained_txn_ids.is_some() {
            undo.reclaim_empty_segments_after_recovery()?;
        }
        // Eagerly ensure every shard has an open active segment file so
        // the first hot-path push observes warm state instead of paying
        // the open() syscall under the shared undo lock.
        for shard_mu in undo.write_shards.iter() {
            shard_mu.lock().ensure_active_segment()?;
        }
        // Track every shard's freshly-opened active segment in the
        // global segment_file_bytes map so the disk_bytes accounting
        // includes them from the start.
        for shard_mu in undo.write_shards.iter() {
            let shard = shard_mu.lock();
            undo.segment_file_bytes
                .entry(shard.active_segment_id)
                .or_insert(shard.active_offset);
        }
        undo.disk_bytes = undo
            .segment_file_bytes
            .values()
            .copied()
            .fold(0u64, u64::saturating_add);
        undo.refresh_metrics();
        // Phase 3 — WAL repair may have re-inserted a few resident
        // records; trim back to the cap so the post-recovery resident
        // footprint honors `mem_cache_limit_bytes` (no-op when unset).
        undo.evict_to_cache_budget();
        undo.persist_manifest()?;
        Ok(undo)
    }

    /// Write back undo frames recovered from the WAL whose segment-file
    /// twin was lost in the crash (buffered tail, or an unsynced write).
    /// Frames are an append-order suffix per segment, so each lands
    /// exactly at its segment's current end; a gap means a frame is
    /// missing from BOTH the segment and the WAL and startup refuses
    /// rather than leave a hole in the allocated id space.
    fn repair_segments_from_wal(
        &mut self,
        mut frames: Vec<(u64, &[u8])>,
    ) -> Result<(), QueryError> {
        if frames.is_empty() || self.undo_dir.is_none() {
            return Ok(());
        }
        for (_, payload) in &frames {
            validate_undo_payload_len(payload.len())?;
        }
        frames.sort_unstable_by_key(|(ptr, _)| *ptr);
        let mut open_files = HashMap::<u32, (File, u64)>::new();
        for (ptr, original_payload) in frames {
            let segment_id = undo_ptr_segment(ptr);
            let offset = undo_ptr_offset(ptr);
            if !open_files.contains_key(&segment_id) {
                let path = self.segment_path(segment_id)?;
                let mut file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(&path)
                    .map_err(io_err)?;
                let mut len = file.metadata().map_err(io_err)?.len();
                if len < UNDO_SEGMENT_HEADER_LEN {
                    write_segment_header(&mut file, segment_id, &path)?;
                    len = UNDO_SEGMENT_HEADER_LEN;
                }
                open_files.insert(segment_id, (file, len));
            }
            let (file, len) = open_files
                .get_mut(&segment_id)
                .expect("replicated undo file inserted");
            if offset < *len {
                // Already on disk (scan_segment would have indexed it had
                // it decoded; either way the bytes exist) — skip.
                continue;
            }
            if offset > *len {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!(
                        "undo segment {segment_id} has a hole at offset {}: \
                         WAL-recovered frame expects offset {offset}; \
                         the data directory is damaged beyond automatic repair",
                        *len
                    ),
                ));
            }
            let mut patched_payload = Vec::new();
            let payload =
                match original_payload.get(UNDO_ENCODED_ID_OFFSET..UNDO_ENCODED_ID_OFFSET + 8) {
                    Some(encoded) if encoded == ptr.to_le_bytes() => original_payload,
                    _ => {
                        patched_payload.extend_from_slice(original_payload);
                        Self::patch_encoded_id(&mut patched_payload, ptr)?;
                        patched_payload.as_slice()
                    }
                };
            let header = undo_frame_header(&payload);
            file.seek(SeekFrom::Start(offset)).map_err(io_err)?;
            file.write_all(&header).map_err(io_err)?;
            file.write_all(&payload).map_err(io_err)?;
            let end = offset + UNDO_FRAME_HEADER_LEN as u64 + payload.len() as u64;
            *len = end;
            self.segment_file_bytes.insert(segment_id, end);
            // Keep the owning shard's active_offset in sync: WAL repair
            // populated the segment file but the shard's bookkeeping
            // still pointed at the pre-crash offset. If the repaired
            // segment IS the shard's active segment, advance the shard's
            // `active_offset` so the next append picks up where the
            // WAL left off rather than overwriting it.
            let shard_id = shard_of_segment(segment_id);
            if let Some(shard_mu) = self.write_shards.get(shard_id as usize) {
                let mut shard = shard_mu.lock();
                if shard.active_segment_id == segment_id {
                    shard.active_offset = end;
                    shard.buffer_start = end;
                }
                // Bump per-shard seq so a future rotation doesn't collide.
                let seq = segment_id >> SHARD_BITS;
                if seq > shard.next_seg_seq {
                    shard.next_seg_seq = seq;
                    shard.active_segment_id = segment_id;
                    shard.active_offset = end;
                    shard.buffer_start = end;
                }
            }
        }
        // A repaired segment can be inactive: flushing active append shards
        // at checkpoint cannot make it durable. Order every repaired file
        // before the directory's final device barrier; only then may queued
        // frames be released or application acknowledged.
        for (&_segment_id, (file, _)) in &mut open_files {
            #[cfg(target_os = "macos")]
            crate::platform::fs::sync_before_atomic_rename_at(
                file,
                &self.segment_path(_segment_id)?,
            )
            .map_err(io_err)?;
            #[cfg(not(target_os = "macos"))]
            file.sync_all().map_err(io_err)?;
        }
        crate::platform::fs::sync_path(self.undo_dir.as_ref().expect("persistent undo repair"))
            .map_err(io_err)?;
        self.disk_bytes = self
            .segment_file_bytes
            .values()
            .copied()
            .fold(0u64, u64::saturating_add);
        Ok(())
    }

    #[cfg(test)]
    /// Test-facing wrapper around `append_record_preencoded`.
    pub(crate) fn append_record(
        &mut self,
        record: UndoRecord,
        persistent: bool,
    ) -> Result<(u64, Vec<u8>, bool), QueryError> {
        let bytes = encode_undo_record(&record)?;
        self.append_record_preencoded(record, bytes, persistent)
    }

    /// Append a record whose payload was already encoded (with any id —
    /// typically the `0` placeholder) OUTSIDE the undo lock. The encoding
    /// + CRC of a wide row dominates the old in-lock cost; here the lock
    /// only covers ptr allocation, an 8-byte id patch, and the frame IO.
    /// Segment rotation re-patches the id instead of re-encoding — the
    /// payload length never changes.
    ///
    /// `&mut self` variant: caller already holds the exclusive guard
    /// (tests / recovery / migration). Internally routes through shard 0
    /// since contention is impossible under exclusive access.
    pub(crate) fn append_record_preencoded(
        &mut self,
        mut record: UndoRecord,
        mut bytes: Vec<u8>,
        persistent: bool,
    ) -> Result<(u64, Vec<u8>, bool), QueryError> {
        validate_undo_payload_len(bytes.len())?;
        // Phase 4 — per-transaction undo cap. Reject a single runaway txn
        // on its own growth so it cannot exhaust the shared tablespace and
        // fail every other session. Checked before any IO so the reject is
        // clean (no half-written frame). The new record's size is the
        // encoded frame payload (a close proxy for `cached_size`).
        if self.txn_max_bytes != 0 {
            let new_bytes = bytes.len() as u64;
            let projected = self.txn_undo_bytes(record.txn_id).saturating_add(new_bytes);
            if projected > self.txn_max_bytes {
                return Err(error(
                    crate::error::catalog::MYDB_ER_UNDO_TXN_LIMIT,
                    format!(
                        "transaction undo exceeds per-transaction cap \
                         (txn={}, would use {} bytes, limit={} bytes); \
                         commit or roll back, or raise mydb_undo_txn_max_bytes",
                        record.txn_id, projected, self.txn_max_bytes
                    ),
                ));
            }
        }
        // Reserve manager-resident memory before segment IO. On any early
        // return the RAII reservation rolls back automatically; on successful
        // directory publication `insert_reserved` commits it.
        let resident_reservation = self.reserve_record_resident(&record)?;
        if persistent && self.undo_dir.is_some() {
            let frame_len = UNDO_FRAME_HEADER_LEN as u64 + bytes.len() as u64;
            let pressure = self.check_admission_for_bytes(frame_len)?;
            let shard_idx = self.pick_shard();
            let (ptr, old_end, new_end) = {
                let shard_mu = self.write_shards.get(shard_idx).ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "no undo write shard available",
                    )
                })?;
                let mut shard = shard_mu.lock();
                shard.ensure_active_segment()?;
                let mut ptr = make_undo_ptr(shard.active_segment_id, shard.active_offset)?;
                if shard.active_offset > UNDO_SEGMENT_HEADER_LEN
                    && shard.active_offset.saturating_add(frame_len) > shard.segment_bytes
                {
                    shard.rotate()?;
                    ptr = make_undo_ptr(shard.active_segment_id, shard.active_offset)?;
                }
                record.id = ptr;
                Self::patch_encoded_id(&mut bytes, ptr)?;
                shard.write_frame(ptr, &bytes)?;
                let new_end = shard.active_offset;
                let old_end = self
                    .segment_file_bytes
                    .insert(shard.active_segment_id, new_end)
                    .unwrap_or(0);
                (ptr, old_end, new_end)
            };
            self.disk_bytes = self
                .disk_bytes
                .saturating_sub(old_end)
                .saturating_add(new_end);
            // M1 (P1-A) — record the exact frame payload length so the
            // remove/truncation path never re-encodes to learn it.
            record.encoded_len = bytes.len() as u32;
            self.insert_reserved(record, resident_reservation)?;
            // Phase 2 — keep the resident payload within the cache cap.
            // Cheap no-op (one branch) until the cap is exceeded.
            self.evict_to_cache_budget();
            Ok((ptr, bytes, pressure))
        } else {
            let id = self.next_undo_id.max(1);
            self.next_undo_id = self.next_undo_id.saturating_add(1).max(id + 1);
            record.id = id;
            Self::patch_encoded_id(&mut bytes, id)?;
            let pressure = self.check_admission_for_bytes(bytes.len() as u64)?;
            record.encoded_len = bytes.len() as u32;
            self.insert_reserved(record, resident_reservation)?;
            Ok((id, bytes, pressure))
        }
    }

    /// Whether the OLTP hot append path may bypass the global undo write lock
    /// while it writes the segment frame. Resident-cache eviction is safe with
    /// this two-phase path: phase 1 writes under the per-shard mutex and phase 2
    /// publishes under the manager write lock, then performs the same bounded
    /// eviction as `append_record_preencoded`. Keeping a non-zero cache cap out
    /// of this predicate is important — the previous gate silently serialized
    /// every UPDATE as soon as operators enabled memory protection.
    pub(crate) fn concurrent_persistent_append_eligible(&self) -> bool {
        self.undo_dir.is_some() && self.txn_max_bytes == 0 && !self.retention_guarantee
    }

    /// Policies needing synchronous global accounting retain the canonical
    /// publication path. Cache pressure alone can be handled at a directory
    /// consolidation barrier, after reserving every record with the governor.
    pub(crate) fn sharded_publication_eligible(&self) -> bool {
        self.concurrent_persistent_append_eligible()
            && self.tablespace_max_bytes == 0
            && self.mem_soft_limit_bytes == 0
    }

    /// A publication barrier exclusively fences new shard publications. Their
    /// memory was admitted before publication, and ids come from disjoint
    /// segment offsets, so consolidation cannot introduce an admission error.
    pub(crate) fn consolidate_published_record(
        &mut self,
        record: UndoRecord,
        reservation: UndoResidentReservation,
    ) {
        let segment_id = undo_ptr_segment(record.id);
        let end = undo_ptr_offset(record.id)
            + UNDO_FRAME_HEADER_LEN as u64
            + u64::from(record.encoded_len);
        let tracked = self.segment_file_bytes.entry(segment_id).or_default();
        let previous = *tracked;
        *tracked = previous.max(end);
        self.disk_bytes = self.disk_bytes.saturating_add(*tracked - previous);
        assert!(
            !self.records.contains_key(&record.id),
            "duplicate published undo pointer"
        );
        self.insert_reserved(record, reservation)
            .expect("pre-admitted undo consolidation is infallible");
    }

    pub(crate) fn finish_publication_barrier(&mut self) {
        self.evict_to_cache_budget();
    }

    /// Clone the one authoritative eligibility gate used by Database. Refresh
    /// from the recovered manager state before publishing the handle.
    pub(crate) fn concurrent_persistent_append_eligibility_handle(&self) -> Arc<AtomicBool> {
        self.concurrent_append_eligible.store(
            self.concurrent_persistent_append_eligible(),
            Ordering::Release,
        );
        Arc::clone(&self.concurrent_append_eligible)
    }

    /// Immutable, Arc-owned routing table for persistent append shards.
    /// Database keeps a clone so the per-row append path can choose a shard
    /// without taking the manager RwLock. The shard mutexes remain the sole
    /// owners of mutable file/tail-buffer state.
    pub(crate) fn concurrent_persistent_append_shards(&self) -> Arc<[UndoWriteShardHandle]> {
        Arc::clone(&self.write_shards)
    }

    #[cfg(test)]
    /// Hot-path persistent append setup: choose the rollback-segment shard
    /// under the manager read lock and clone its handle. The caller should
    /// drop the read guard before calling `write_concurrent_persistent_frame`
    /// so the segment frame IO does not contend with phase-2 publish on the
    /// manager RwLock.
    pub(crate) fn begin_concurrent_persistent_append(
        &self,
    ) -> Result<Option<UndoWriteShardHandle>, QueryError> {
        if !self.concurrent_persistent_append_eligible() {
            return Ok(None);
        }
        let shard_idx = self.pick_shard();
        let shard = self.write_shards.get(shard_idx).ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "no undo write shard available",
            )
        })?;
        Ok(Some(Arc::clone(shard)))
    }

    /// Hot-path persistent append, phase 1: allocate a ptr in exactly one
    /// rollback-segment shard and write the encoded frame while holding only
    /// that shard mutex. The caller must subsequently call
    /// `finish_concurrent_persistent_append` under the manager write lock to
    /// publish the in-memory mirror/accounting.
    #[cfg(test)]
    pub(crate) fn write_concurrent_persistent_frame(
        shard: &UndoWriteShardHandle,
        bytes: &mut [u8],
    ) -> Result<(u64, u32, u64), QueryError> {
        let (ptr, segment, end, _lease) = Self::write_leased_persistent_frame(shard, bytes)?;
        Ok((ptr, segment, end))
    }

    pub(crate) fn write_leased_persistent_frame(
        shard: &UndoWriteShardHandle,
        bytes: &mut [u8],
    ) -> Result<(u64, u32, u64, UndoSegmentLease), QueryError> {
        validate_undo_payload_len(bytes.len())?;
        let frame_len = UNDO_FRAME_HEADER_LEN as u64 + bytes.len() as u64;
        let mut shard = shard.lock();
        shard.ensure_active_segment()?;
        if shard.active_offset > UNDO_SEGMENT_HEADER_LEN
            && shard.active_offset.saturating_add(frame_len) > shard.segment_bytes
        {
            shard.rotate()?;
        }
        let ptr = make_undo_ptr(shard.active_segment_id, shard.active_offset)?;
        Self::patch_encoded_id(bytes, ptr)?;
        shard.write_frame(ptr, bytes)?;
        let lease = shard
            .active_pin
            .as_ref()
            .expect("open segment has a lifetime pin")
            .clone();
        Ok((ptr, shard.active_segment_id, shard.active_offset, lease))
    }

    /// Hot-path persistent append, phase 2: publish the frame written by
    /// `write_concurrent_persistent_frame` into the manager mirror. Kept short
    /// and free of segment IO so concurrent writers do not queue behind a
    /// global undo lock while bytes are copied/flushed into shard tail buffers.
    pub(crate) fn finish_concurrent_persistent_append(
        &mut self,
        mut record: UndoRecord,
        bytes: Vec<u8>,
        ptr: u64,
        segment_id: u32,
        segment_end: u64,
        resident_reservation: UndoResidentReservation,
    ) -> Result<(u64, Vec<u8>, bool), QueryError> {
        let frame_len = UNDO_FRAME_HEADER_LEN as u64 + bytes.len() as u64;
        let pressure = self.check_admission_for_bytes(frame_len)?;
        record.id = ptr;
        record.encoded_len = bytes.len() as u32;
        // Phase-1 writes to one shard are ordered by its mutex, but the
        // writers can acquire the manager lock for phase 2 in the opposite
        // order. Never let a late publication of an earlier frame move the
        // recorded segment tail backwards: evicted records at offsets between
        // the stale and actual tails would otherwise be treated as absent by
        // `read_frame_from_segment`.
        let tracked_end = self.segment_file_bytes.entry(segment_id).or_default();
        let old_end = *tracked_end;
        *tracked_end = old_end.max(segment_end);
        let new_end = *tracked_end;
        self.disk_bytes = self
            .disk_bytes
            .saturating_sub(old_end)
            .saturating_add(new_end);
        self.insert_reserved(record, resident_reservation)?;
        // Match `append_record_preencoded`: usually one cheap limit branch;
        // after crossing the high-water, flush the shard tails and evict an
        // oldest prefix down to the 87.5% low-water in one batch.
        self.evict_to_cache_budget();
        Ok((ptr, bytes, pressure))
    }

    /// Round-robin shard picker used by all append paths.
    pub(crate) fn pick_shard(&self) -> usize {
        let n = self.write_shards.len().max(1);
        let v = self.next_shard.fetch_add(1, Ordering::Relaxed);
        (v as usize) % n
    }

    fn patch_encoded_id(bytes: &mut [u8], id: u64) -> Result<(), QueryError> {
        let Some(slot) = bytes.get_mut(UNDO_ENCODED_ID_OFFSET..UNDO_ENCODED_ID_OFFSET + 8) else {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "undo payload too short to patch id",
            ));
        };
        slot.copy_from_slice(&id.to_le_bytes());
        Ok(())
    }

    /// Apply the source's atomic relocation manifest to an online receiver.
    /// The accompanying output files have already been installed by WAL redo.
    pub(crate) fn apply_source_manifest(&mut self, bytes: &[u8]) -> Result<(), QueryError> {
        let manifest: UndoManifest = decode_undo_manifest(bytes)?;
        if manifest.version != UNDO_TABLESPACE_VERSION
            || !manifest.pending_recovery_segments.is_empty()
        {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "invalid source undo relocation manifest",
            ));
        }
        for (&old, &new) in &manifest.forwardings {
            if old == new || undo_ptr_segment(new) == 0 {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "invalid source undo relocation address",
                ));
            }
            if let Some(mut record) = self.records.remove(&old) {
                record.id = new;
                self.records.insert(new, record);
                let origin = self.compaction_origins.remove(&old).unwrap_or(old);
                self.compaction_origins.insert(new, origin);
            }
            let segment = undo_ptr_segment(new);
            if !self.segment_file_bytes.contains_key(&segment) {
                let length = fs::metadata(self.segment_path(segment)?)
                    .map_err(io_err)?
                    .len();
                self.segment_file_bytes.insert(segment, length);
            }
        }
        self.compaction_forwarded.extend(manifest.forwardings);
        self.compensated.extend(manifest.compensated);
        self.discarded.extend(manifest.discarded);
        self.compaction_seq_high = self.compaction_seq_high.max(manifest.compaction_seq_high);
        self.invalidate_keep_cache();
        self.refresh_metrics();
        Ok(())
    }

    fn load_manifest(&mut self) -> Result<(), QueryError> {
        self.load_manifest_mode(true)
    }

    fn load_manifest_mode(&mut self, writable: bool) -> Result<(), QueryError> {
        let Some(path) = self.manifest_path.as_ref() else {
            return Ok(());
        };
        if !path.exists() {
            return Ok(());
        }
        let bytes = fs::read(path).map_err(io_err)?;
        let manifest: UndoManifest = decode_undo_manifest(&bytes)?;
        if manifest.version != UNDO_TABLESPACE_VERSION
            || manifest.storage_uuid != undo_storage_uuid(self.undo_dir.as_ref().unwrap())?
            || manifest.shard_active_segments.is_empty()
            || manifest.active_segment_id != manifest.shard_active_segments[0]
        {
            return Err(error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "unsupported undo tablespace version; use a new data directory or rebuild data",
            ));
        }
        if !writable && !manifest.pending_recovery_segments.is_empty() {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "read-only undo tablespace requires recovery publication",
            ));
        }
        self.finish_recovery_segment_publication(&manifest.pending_recovery_segments)?;
        self.compensated = manifest.compensated;
        self.discarded = manifest.discarded;
        self.compaction_forwarded = manifest.forwardings;
        self.compaction_seq_high = manifest.compaction_seq_high.max(
            self.compaction_forwarded
                .values()
                .filter(|ptr| shard_of_segment(undo_ptr_segment(**ptr)) == COMPACTION_SHARD_LANE)
                .map(|ptr| undo_ptr_segment(*ptr) >> SHARD_BITS)
                .max()
                .unwrap_or(0),
        );
        for (&old, &new) in &self.compaction_forwarded {
            self.compaction_origins
                .entry(new)
                .and_modify(|origin| *origin = (*origin).min(old))
                .or_insert(old);
        }
        if manifest.segment_bytes > u32::MAX as u64 {
            return Err(error(
                crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                "persisted mydb_undo_segment_bytes exceeds the 32-bit undo offset limit",
            ));
        }
        if manifest.segment_bytes < UNDO_SEGMENT_HEADER_LEN * 2 {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "invalid undo segment size in manifest",
            ));
        }
        self.segment_bytes = manifest.segment_bytes;
        let shard_active = manifest.shard_active_segments;
        for (shard_idx, seg_id) in shard_active.into_iter().enumerate() {
            let Some(shard_mu) = self.write_shards.get(shard_idx) else {
                continue;
            };
            let mut shard = shard_mu.lock();
            shard.segment_bytes = self.segment_bytes;
            if seg_id != 0 {
                shard.active_segment_id = seg_id;
                shard.next_seg_seq = seg_id >> SHARD_BITS;
            }
        }
        if !manifest.pending_recovery_segments.is_empty() {
            // Finish the intent before startup filtering can reclaim outputs.
            self.compensation_manifest_dirty
                .store(true, Ordering::Release);
            self.persist_manifest()?;
        }
        Ok(())
    }

    fn persist_manifest(&self) -> Result<(), QueryError> {
        self.persist_manifest_with_recovery_segments(&[])
    }

    fn persist_manifest_with_recovery_segments(&self, pending: &[u32]) -> Result<(), QueryError> {
        let Some(path) = self.manifest_path.as_ref() else {
            return Ok(());
        };
        let shard_active: Vec<u32> = self
            .write_shards
            .iter()
            .map(|s| s.lock().active_segment_id)
            .collect();
        let legacy_active = shard_active.first().copied().unwrap_or(1);
        let manifest = UndoManifest {
            pending_recovery_segments: pending.to_vec(),
            compaction_seq_high: self.compaction_seq_high,
            version: UNDO_TABLESPACE_VERSION,
            storage_uuid: undo_storage_uuid(self.undo_dir.as_ref().unwrap())?,
            segment_bytes: self.segment_bytes,
            active_segment_id: legacy_active,
            shard_active_segments: shard_active,
            compensated: self.compensated.clone(),
            discarded: self.discarded.clone(),
            forwardings: self.compaction_forwarded.clone(),
        };
        let tmp = path.with_extension("manifest.tmp");
        let bytes = encode_undo_manifest(&manifest)?;
        let unchanged = manifest_contents_match(path, &bytes);
        let sync = self.compensation_manifest_dirty.load(Ordering::Acquire);
        #[cfg(target_os = "macos")]
        let mut last_sync = self.manifest_sync_snapshot.lock();
        #[cfg(target_os = "macos")]
        let mut snapshot = None;
        #[cfg(target_os = "macos")]
        if sync && unchanged {
            snapshot = if self.segment_file_bytes.len() < 64 {
                manifest_sync_snapshot(path, &bytes)
            } else {
                None
            };
            if snapshot.is_some() && *last_sync == snapshot {
                // Shard data barriers are independent. The exact manifest
                // bytes and namespace mapping already reached a successful
                // barrier, so publishing them again adds no durability.
                self.compensation_manifest_dirty
                    .store(false, Ordering::Release);
                return Ok(());
            }
        }
        #[cfg(target_os = "macos")]
        if sync {
            // Any attempted new barrier invalidates the old proof until the
            // complete operation succeeds, including its error/retry path.
            *last_sync = None;
        }
        if !unchanged {
            #[cfg(target_os = "macos")]
            {
                *last_sync = None;
            }
            fs::write(&tmp, &bytes).map_err(io_err)?;
        }
        let sync_path = if unchanged { path } else { &tmp };
        if sync {
            #[cfg(target_os = "macos")]
            crate::platform::fs::sync_before_atomic_rename_at(
                &File::open(sync_path).map_err(io_err)?,
                sync_path,
            )
            .map_err(io_err)?;
            #[cfg(not(target_os = "macos"))]
            crate::platform::fs::sync_path(sync_path).map_err(io_err)?;
        }
        if !unchanged {
            fs::rename(&tmp, path).map_err(io_err)?;
        }
        if sync {
            // Capture before the barrier: names created after it must force
            // a fresh sync on the next checkpoint, even if this fsync happens
            // to persist those racing names too.
            #[cfg(target_os = "macos")]
            if !unchanged {
                snapshot = if self.segment_file_bytes.len() < 64 {
                    manifest_sync_snapshot(path, &bytes)
                } else {
                    None
                };
            }
            #[cfg(all(test, target_os = "macos"))]
            if FAIL_NEXT_MANIFEST_DIRECTORY_SYNC.with(|flag| flag.replace(false)) {
                return Err(io_err(std::io::Error::other(
                    "injected undo manifest directory sync failure",
                )));
            }
            crate::platform::fs::sync_path(path.parent().unwrap()).map_err(io_err)?;
            #[cfg(target_os = "macos")]
            {
                *last_sync = snapshot;
            }
            self.compensation_manifest_dirty
                .store(false, Ordering::Release);
        }
        Ok(())
    }

    fn scan_segments(&mut self, retained_txn_ids: Option<&HashSet<u64>>) -> Result<(), QueryError> {
        let Some(dir) = self.undo_dir.as_ref().cloned() else {
            return Ok(());
        };
        let mut ids = Vec::new();
        for entry in fs::read_dir(&dir).map_err(io_err)? {
            let entry = entry.map_err(io_err)?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if let Some(id) = parse_segment_name(name) {
                ids.push(id);
            }
        }
        ids.sort_unstable();

        // Rotation flushes the old shard before publishing a successor.
        // Only the highest allocation segment in a lane may have a torn append;
        // atomically published compaction outputs are always sealed.
        let mut writable_tails = std::collections::HashMap::<u32, u32>::new();
        for id in self
            .active_segment_ids()
            .into_iter()
            .chain(ids.iter().copied())
        {
            let lane = shard_of_segment(id);
            if lane != COMPACTION_SHARD_LANE {
                writable_tails
                    .entry(lane)
                    .and_modify(|old| *old = (*old).max(id))
                    .or_insert(id);
            }
        }
        let scan_started = std::time::Instant::now();
        // P1/7 — decode segments in parallel, then apply sequentially.
        // Each segment file is independent at the byte level (no shared
        // header, no cross-file invariants), so decode is embarrassingly
        // parallel. The apply step touches `records` / `segment_file_bytes`
        // / `disk_bytes` which are not thread-safe, so it stays serial —
        // its cost is bounded by HashMap inserts and is dwarfed by the
        // file IO + CRC the workers cover.
        // Phase 3 — when a resident cap is configured, decode and apply
        // one segment at a time and evict after each, so peak memory is
        // O(one segment + cap) instead of O(whole tablespace). This is
        // what lets a larger-than-RAM undo tablespace recover without
        // OOM; the trade is giving up the parallel-decode speedup, which
        // only matters when the operator has *not* opted into a cap.
        let bounded = self.mem_cache_limit_bytes != 0;
        let force_recover = !self.strict_recovery && undo_force_recover();
        if ids.len() <= 1 || bounded || retained_txn_ids.is_some() {
            for id in &ids {
                let path = dir.join(segment_file_name(*id));
                let decoded =
                    decode_segment_file_filtered(&path, *id, force_recover, retained_txn_ids)?;
                validate_scanned_undo_tail(&decoded, &writable_tails, force_recover)?;
                self.apply_decoded_segment(decoded)?;
                if bounded {
                    self.evict_to_cache_budget();
                }
            }
        } else {
            let workers = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(2)
                .min(ids.len())
                .max(1);
            let paths: Vec<(u32, PathBuf)> = ids
                .iter()
                .map(|id| (*id, dir.join(segment_file_name(*id))))
                .collect();
            let chunk_size = paths.len().div_ceil(workers).max(1);
            let decoded_chunks: Vec<Result<Vec<DecodedSegment>, QueryError>> =
                std::thread::scope(|scope| {
                    let mut handles = Vec::with_capacity(workers);
                    for chunk in paths.chunks(chunk_size) {
                        let chunk = chunk.to_vec();
                        let writable_tails = &writable_tails;
                        handles.push(scope.spawn(
                            move || -> Result<Vec<DecodedSegment>, QueryError> {
                                let mut out = Vec::with_capacity(chunk.len());
                                for (id, path) in chunk {
                                    let decoded = decode_segment_file(&path, id, force_recover)?;
                                    validate_scanned_undo_tail(
                                        &decoded,
                                        writable_tails,
                                        force_recover,
                                    )?;
                                    out.push(decoded);
                                }
                                Ok(out)
                            },
                        ));
                    }
                    handles
                        .into_iter()
                        .map(|h| {
                            h.join().unwrap_or_else(|_| {
                                Err(error(
                                    crate::error::catalog::ER_GET_ERRNO,
                                    "undo segment scan worker panicked",
                                ))
                            })
                        })
                        .collect()
                });
            // Apply in segment_id order so insert_loaded_record sees
            // append-order records (matters for next_undo_id maintenance).
            let mut all_decoded: Vec<DecodedSegment> = Vec::with_capacity(ids.len());
            for chunk in decoded_chunks {
                all_decoded.extend(chunk?);
            }
            all_decoded.sort_unstable_by_key(|d| d.segment_id);
            for decoded in all_decoded {
                self.apply_decoded_segment(decoded)?;
            }
        }

        // Each shard's active segment = max segment_id in its shard
        // subspace that we scanned; `next_seg_seq` follows. If a shard
        // has no on-disk segments yet, keep the default `(seq=1)`
        // placeholder so a fresh `ensure_active_segment` allocates a
        // virgin file at first push.
        let mut per_shard_max: Vec<Option<u32>> = vec![None; self.write_shards.len()];
        for &seg_id in self.segment_file_bytes.keys() {
            let shard_id = shard_of_segment(seg_id) as usize;
            if shard_id >= self.write_shards.len() {
                continue;
            }
            per_shard_max[shard_id] = Some(
                per_shard_max[shard_id]
                    .map(|cur| cur.max(seg_id))
                    .unwrap_or(seg_id),
            );
        }
        for (shard_idx, maybe_max) in per_shard_max.into_iter().enumerate() {
            if let Some(max_id) = maybe_max {
                let shard_mu = &self.write_shards[shard_idx];
                let mut shard = shard_mu.lock();
                // A durable recovery manifest can retire a segment whose
                // WAL-only offsets must never be allocated again.
                let max_id = max_id.max(shard.active_segment_id);
                shard.active_segment_id = max_id;
                shard.next_seg_seq = max_id >> SHARD_BITS;
                shard.active_offset = self
                    .segment_file_bytes
                    .get(&max_id)
                    .copied()
                    .unwrap_or(UNDO_SEGMENT_HEADER_LEN)
                    .max(UNDO_SEGMENT_HEADER_LEN);
                shard.buffer_start = shard.active_offset;
            }
        }
        self.scan_total_ns.fetch_add(
            scan_started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        Ok(())
    }

    fn apply_decoded_segment(&mut self, decoded: DecodedSegment) -> Result<(), QueryError> {
        let DecodedSegment {
            segment_id,
            records,
            valid_end,
            truncate_to,
            max_undo_id,
            discarded_records,
        } = decoded;
        if let Some(truncate) = truncate_to {
            let path = self.segment_path(segment_id)?;
            if crate::storage::image_io::immutable_path(&path) {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "native snapshot undo requires repair",
                ));
            }
            let file = OpenOptions::new().write(true).open(&path).map_err(io_err)?;
            file.set_len(truncate).map_err(io_err)?;
        }
        for record in records {
            self.insert_loaded_record(record)?;
        }
        self.next_undo_id = self.next_undo_id.max(max_undo_id.saturating_add(1)).max(1);
        self.purged_records = self.purged_records.saturating_add(discarded_records);
        self.segment_file_bytes
            .insert(segment_id, valid_end.max(UNDO_SEGMENT_HEADER_LEN));
        self.disk_bytes = self
            .segment_file_bytes
            .values()
            .copied()
            .fold(0u64, u64::saturating_add);
        Ok(())
    }

    fn segment_path(&self, segment_id: u32) -> Result<PathBuf, QueryError> {
        self.undo_dir
            .as_ref()
            .map(|dir| dir.join(segment_file_name(segment_id)))
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "undo tablespace has no segment directory",
                )
            })
    }

    pub(crate) fn resident_admission(&self) -> UndoResidentAdmission {
        UndoResidentAdmission(Arc::clone(&self.resident_charge))
    }

    pub(crate) fn reserve_record_resident(
        &self,
        record: &UndoRecord,
    ) -> Result<UndoResidentReservation, QueryError> {
        reserve_undo_resident(&self.resident_charge, record)
    }

    fn release_resident_charge(&self, bytes: u64) {
        self.resident_charge
            .lock()
            .shrink(usize::try_from(bytes).unwrap_or(usize::MAX));
    }

    fn insert_loaded_record(&mut self, record: UndoRecord) -> Result<(), QueryError> {
        if self.discarded.contains(&record.id.max(1))
            || self.records.contains_key(&record.id.max(1))
        {
            return Ok(());
        }
        let reservation = self.reserve_record_resident(&record)?;
        self.insert_loaded_record_reserved(record, reservation)
    }

    fn insert_loaded_record_reserved(
        &mut self,
        mut record: UndoRecord,
        reservation: UndoResidentReservation,
    ) -> Result<(), QueryError> {
        let id = record.id.max(1);
        self.next_undo_id = self.next_undo_id.max(id.saturating_add(1));
        if self.records.contains_key(&id) {
            return Ok(());
        }
        let segment_id = undo_ptr_segment(id);
        if segment_id != 0 {
            *self.segment_live_records.entry(segment_id).or_default() += 1;
        }
        let created_ms = record.created_unix_ms;
        // Compute the size estimate once; the remove/purge paths subtract
        // the cached value instead of re-walking the before-image.
        if record.cached_size == 0 {
            record.cached_size =
                u32::try_from(estimate_undo_record_bytes(&record)).unwrap_or(u32::MAX);
        }
        if record.resident_size == 0 {
            record.resident_size =
                u32::try_from(estimate_undo_resident_bytes(&record)).unwrap_or(u32::MAX);
        }
        self.current_bytes = self
            .current_bytes
            .saturating_add(u64::from(record.cached_size));
        self.resident_bytes = self
            .resident_bytes
            .saturating_add(u64::from(record.resident_size));
        // Phase 4 — per-txn accounting (logical bytes, resident or not).
        *self.txn_bytes.entry(record.txn_id).or_default() += u64::from(record.cached_size);
        self.created_index.entry(created_ms).or_default().insert(id);
        if segment_id != 0
            && record.resident
            && resident_size(&record) > metadata_resident_bytes(&record)
        {
            self.eviction_candidates.insert((created_ms, id));
        }
        self.records.insert(id, UndoRecord { id, ..record });
        reservation.commit();
        Ok(())
    }

    fn refresh_metrics(&mut self) {
        self.created_index.clear();
        self.eviction_candidates.clear();
        self.segment_live_records.clear();
        self.txn_bytes.clear();
        self.longest_chain_depth = 0;
        self.current_bytes = 0;
        self.resident_bytes = 0;
        let ids = self.records.keys().copied().collect::<Vec<_>>();
        for id in &ids {
            let needs_depth = self.records.get(id).is_some_and(|r| r.chain_depth == 0);
            if needs_depth {
                let mut depth = 1u32;
                let mut prev = self.records.get(id).and_then(|r| r.prev_undo_ptr);
                while let Some(ptr) = prev {
                    depth = depth.saturating_add(1);
                    if depth > 64 {
                        break;
                    }
                    prev = self
                        .records
                        .get(&self.canonical_ptr(ptr))
                        .and_then(|r| r.prev_undo_ptr);
                }
                if let Some(record) = self.records.get_mut(id) {
                    record.chain_depth = depth;
                }
            }
            if let Some(record) = self.records.get_mut(id) {
                if record.resident_size == 0 {
                    record.resident_size =
                        u32::try_from(estimate_undo_resident_bytes(record)).unwrap_or(u32::MAX);
                }
            }
        }
        for (id, record) in self.records.iter() {
            if undo_ptr_segment(*id) != 0
                && record.resident
                && resident_size(record) > metadata_resident_bytes(record)
            {
                self.eviction_candidates
                    .insert((record.created_unix_ms, *id));
            }
            self.created_index
                .entry(record.created_unix_ms)
                .or_default()
                .insert(*id);
            self.longest_chain_depth = self.longest_chain_depth.max(record.chain_depth);
            self.current_bytes = self.current_bytes.saturating_add(record_size(record));
            self.resident_bytes = self.resident_bytes.saturating_add(resident_size(record));
            *self.txn_bytes.entry(record.txn_id).or_default() += record_size(record);
            let segment_id = undo_ptr_segment(*id);
            if segment_id != 0 {
                *self.segment_live_records.entry(segment_id).or_default() += 1;
            }
        }
        let count = self.records.len() as u64;
        self.peak_records = self.peak_records.max(count);
        self.peak_bytes = self.peak_bytes.max(self.current_bytes);
        self.disk_bytes = self
            .segment_file_bytes
            .values()
            .copied()
            .fold(0u64, u64::saturating_add);
    }

    pub(crate) fn set_retention_guarantee(&mut self, enabled: bool) {
        if enabled {
            self.concurrent_append_eligible
                .store(false, Ordering::Release);
        }
        self.retention_guarantee = enabled;
        if !enabled && self.txn_max_bytes == 0 && self.undo_dir.is_some() {
            self.concurrent_append_eligible
                .store(true, Ordering::Release);
        }
    }

    pub(crate) fn set_tablespace_max_bytes(&mut self, bytes: u64) {
        self.tablespace_max_bytes = bytes;
    }

    /// P0/B2: set the in-memory `records` soft cap (bytes; 0 = disabled).
    pub(crate) fn set_mem_soft_limit_bytes(&mut self, bytes: u64) {
        self.mem_soft_limit_bytes = bytes;
    }

    /// P2/12 — disk-cap soft-fail warning toggle. Reads and writes
    /// access `disk_cap_softfail_warn` directly; the admission path
    /// reads it to decide whether to bump the warns counter.
    pub(crate) fn disk_cap_softfail_warn(&self) -> bool {
        self.disk_cap_softfail_warn
    }

    pub(crate) fn set_disk_cap_softfail_warn(&mut self, enabled: bool) {
        self.disk_cap_softfail_warn = enabled;
    }

    /// Set every rollback lane's write-buffer threshold. Zero switches to
    /// write-through; configuration reads use the authoritative lane directly.
    pub(crate) fn set_buffer_limit(&mut self, bytes: u64) -> Result<(), QueryError> {
        // Crossing into write-through must drain every shard's buffered
        // tail so subsequent appends observe the new policy on a clean
        // tail. The caller holds the undo write lock; `set_buffer_limit`
        // is safe under that.
        for shard_mu in self.write_shards.iter() {
            shard_mu.lock().set_buffer_limit(bytes)?;
        }
        Ok(())
    }

    pub(crate) fn set_segment_bytes(&mut self, bytes: u64) -> Result<(), QueryError> {
        if bytes < UNDO_SEGMENT_HEADER_LEN * 2 || bytes > u32::MAX as u64 {
            return Err(error(
                crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                "variable 'mydb_undo_segment_bytes' must fit the 32-bit undo offset",
            ));
        }
        self.segment_bytes = bytes;
        for shard_mu in self.write_shards.iter() {
            shard_mu.lock().segment_bytes = bytes;
        }
        self.persist_manifest()
    }

    /// Per-shard aggregate getters — sum of buffer_flushes / latency
    /// histogram samples across shards. The legacy single-field accessors
    /// (`self.buffer_flushes`, `self.write_frame_latency.samples()`, etc.)
    /// are gone; callers go through these.
    #[cfg(test)]
    pub(crate) fn buffer_flushes_total(&self) -> u64 {
        self.write_shards
            .iter()
            .map(|s| s.lock().buffer_flushes)
            .fold(0u64, u64::saturating_add)
    }

    pub(crate) fn write_frame_latency_samples(&self) -> u64 {
        self.write_shards
            .iter()
            .map(|s| s.lock().write_frame_latency.samples())
            .fold(0u64, u64::saturating_add)
    }

    pub(crate) fn write_frame_latency_total_ns(&self) -> u64 {
        self.write_shards
            .iter()
            .map(|s| s.lock().write_frame_latency.total_ns())
            .fold(0u64, u64::saturating_add)
    }

    pub(crate) fn write_frame_latency_percentile(&self, pct: u8) -> u64 {
        // Take the maximum bucket across shards as an approximation —
        // accurate to bucket granularity (factor of 10) and avoids
        // building a global histogram, which would need temporary copies.
        self.write_shards
            .iter()
            .map(|s| s.lock().write_frame_latency.percentile(pct))
            .max()
            .unwrap_or(0)
    }

    pub(crate) fn flush_latency_samples(&self) -> u64 {
        self.write_shards
            .iter()
            .map(|s| s.lock().flush_latency.samples())
            .fold(0u64, u64::saturating_add)
    }

    pub(crate) fn flush_latency_total_ns(&self) -> u64 {
        self.write_shards
            .iter()
            .map(|s| s.lock().flush_latency.total_ns())
            .fold(0u64, u64::saturating_add)
    }

    pub(crate) fn flush_latency_percentile(&self, pct: u8) -> u64 {
        self.write_shards
            .iter()
            .map(|s| s.lock().flush_latency.percentile(pct))
            .max()
            .unwrap_or(0)
    }

    /// Returns the count of write shards in use (1..=16). Surfaced for
    /// SHOW STATUS so operators can confirm sharding is live.
    #[allow(dead_code)] // surfaced through SHOW STATUS hooks
    pub(crate) fn shard_count(&self) -> usize {
        self.write_shards.len()
    }

    pub(crate) fn longest_chain_depth(&self) -> u32 {
        self.longest_chain_depth
    }

    pub(crate) fn retention_pressure_count(&self) -> u64 {
        self.retention_pressure_count
    }

    pub(crate) fn peak_bytes(&self) -> u64 {
        self.peak_bytes
    }

    pub(crate) fn peak_records(&self) -> u64 {
        self.peak_records
    }

    pub(crate) fn segment_count(&self) -> u64 {
        self.segment_file_bytes.len() as u64
    }

    pub(crate) fn reclaimable_bytes(&self) -> u64 {
        let active: HashSet<u32> = self
            .write_shards
            .iter()
            .map(|s| s.lock().active_segment_id)
            .collect();
        self.segment_file_bytes
            .iter()
            .filter(|(id, _)| !active.contains(id))
            .filter(|(id, _)| self.segment_live_records.get(id).copied().unwrap_or(0) == 0)
            .map(|(_, bytes)| *bytes)
            .sum()
    }

    /// Set of segment ids that are some shard's current active segment.
    /// Used by `detach_empty_segments` to leave those alone — the shard's
    /// next append still expects to find the file.
    fn protected_segment_ids(&self) -> HashSet<u32> {
        let mut protected = self.active_segment_ids();
        protected.extend(
            self.segment_pins
                .lock()
                .pins
                .iter()
                .filter_map(|(id, pin)| (pin.strong_count() != 0).then_some(*id)),
        );
        protected
    }

    fn active_segment_ids(&self) -> HashSet<u32> {
        self.write_shards
            .iter()
            .map(|s| s.lock().active_segment_id)
            .collect()
    }

    fn check_admission_for_bytes(&mut self, extra_bytes: u64) -> Result<bool, QueryError> {
        let mut pressure = false;
        // P0/B2: soft cap on the in-memory `records` mirror. Crossing it
        // flags pressure so the caller fires an immediate
        // `purge_obsolete_undo()`, bounding the mirror between periodic
        // purge ticks under a write burst. It never errors: when nothing
        // is purgeable (every record still pinned by a live snapshot) the
        // mirror grows legitimately and the disk cap below is the hard
        // backstop.
        if self.mem_soft_limit_bytes != 0
            && self.current_bytes.saturating_add(extra_bytes) > self.mem_soft_limit_bytes
        {
            self.mem_pressure_count = self.mem_pressure_count.saturating_add(1);
            pressure = true;
        }
        // Disk tablespace cap (pre-existing semantics, unchanged).
        if self.tablespace_max_bytes != 0
            && self.disk_bytes.saturating_add(extra_bytes) > self.tablespace_max_bytes
        {
            self.retention_pressure_count = self.retention_pressure_count.saturating_add(1);
            if self.retention_guarantee {
                return Err(error(
                    crate::error::catalog::MYDB_ER_UNDO_RETENTION_EXHAUSTED,
                    format!(
                        "undo tablespace exhausted under RETENTION GUARANTEE (limit={} bytes); \
                         reduce concurrent DML or wait for active queries to finish",
                        self.tablespace_max_bytes
                    ),
                ));
            }
            pressure = true;
        }
        Ok(pressure)
    }

    /// Preflight admission for an INSERT undo record (kind = `Insert`
    /// or `InsertBatch`). INSERT undo carries no before-image — only
    /// `row_ids` for the batch variant — so the encoded payload is
    /// well-bounded by `INSERT_UNDO_PAYLOAD_BASE + row_count *
    /// INSERT_UNDO_PER_ROW_BYTES` (see `estimate_undo_record_bytes`
    /// for the exact envelope). The pre-2026 estimate was a constant
    /// 96 + N*8 — a +50% overestimate for wide batches that pushed
    /// the cap pressure flag conservatively. The refined number below
    /// is within a frame header of the real on-disk frame for both
    /// the single-row Insert and the batch path.
    pub(crate) fn check_insert_admission(
        &mut self,
        row_count: usize,
        persistent: bool,
    ) -> Result<bool, QueryError> {
        if row_count == 0 {
            return Ok(false);
        }
        self.check_undo_admission(Self::insert_payload_estimate(row_count), persistent)
    }

    fn insert_payload_estimate(row_count: usize) -> u64 {
        if row_count == 1 {
            INSERT_UNDO_PAYLOAD_BASE
        } else {
            INSERT_UNDO_PAYLOAD_BASE
                .saturating_add((row_count as u64).saturating_mul(INSERT_UNDO_PER_ROW_BYTES))
        }
    }

    /// A read-only success probe; pressure, warnings and hard-limit errors
    /// still go through the existing exclusive admission path.
    pub(crate) fn insert_admission_unpressured(&self, row_count: usize, persistent: bool) -> bool {
        if row_count == 0 {
            return true;
        }
        let mut extra = Self::insert_payload_estimate(row_count);
        if persistent && self.undo_dir.is_some() {
            extra = extra.saturating_add(UNDO_FRAME_HEADER_LEN as u64);
        }
        (self.mem_soft_limit_bytes == 0
            || self.current_bytes.saturating_add(extra) <= self.mem_soft_limit_bytes)
            && (self.tablespace_max_bytes == 0
                || self.disk_bytes.saturating_add(extra) <= self.tablespace_max_bytes)
    }

    /// General-purpose admission entry that uses a caller-supplied
    /// per-record payload estimate, so UPDATE/DELETE preflights can
    /// account for the actual before-image cost (wide rows, externals,
    /// `before_values`). For INSERT, prefer `check_insert_admission`
    /// which has a tighter built-in formula.
    pub(crate) fn check_undo_admission(
        &mut self,
        expected_payload_bytes: u64,
        persistent: bool,
    ) -> Result<bool, QueryError> {
        let extra = if persistent && self.undo_dir.is_some() {
            (UNDO_FRAME_HEADER_LEN as u64).saturating_add(expected_payload_bytes)
        } else {
            expected_payload_bytes
        };
        self.check_admission_for_bytes(extra)
    }

    pub(crate) fn cache_hits(&self) -> u64 {
        self.cache_hits.load(Ordering::Relaxed)
    }

    pub(crate) fn cache_misses(&self) -> u64 {
        self.cache_misses.load(Ordering::Relaxed)
    }

    /// Zero-copy borrow of a *resident* record. Returns `None` both when
    /// the id is absent and when its payload has been evicted from memory
    /// (Phase 2) — in the latter case the caller pages it back in with
    /// `load_owned`. The MVCC read walker relies on exactly this: a
    /// `None` here routes to `load_owned`, never straight to 1236.
    /// Follow the compaction forwarding chain from `ptr` to the record's
    /// current home. Returns the resolved ptr when `ptr` was relocated
    /// (possibly across several compactions), or `None` when it was not
    /// forwarded. The map length bounds the walk so a hypothetical cycle can
    /// never spin; ordinary chains are path-compressed after every compaction.
    pub(crate) fn mark_compensated(&mut self, ids: &[u64]) {
        if ids.is_empty() {
            return;
        }
        for &id in ids {
            let canonical = self.canonical_ptr(id);
            self.compensated.insert(canonical);
        }
        self.compensation_manifest_dirty
            .store(true, Ordering::Release);
    }

    pub(crate) fn wal_identity(&self, ptr: u64) -> u64 {
        let canonical = self.canonical_ptr(ptr);
        self.compaction_origins
            .get(&canonical)
            .copied()
            .unwrap_or(canonical)
    }

    pub(crate) fn canonical_ptr(&self, ptr: u64) -> u64 {
        self.resolve_forwarded(ptr).unwrap_or(ptr)
    }

    fn resolve_forwarded(&self, ptr: u64) -> Option<u64> {
        if self.compaction_forwarded.is_empty() {
            return None;
        }
        let mut cur = ptr;
        for _ in 0..=self.compaction_forwarded.len() {
            match self.compaction_forwarded.get(&cur) {
                Some(&next) => cur = next,
                None => return (cur != ptr).then_some(cur),
            }
        }
        None
    }

    /// Install `old_ptr → new_ptr` forwardings for a batch of relocated
    /// records, path-compress every older alias to its newest live home, and
    /// prune aliases whose final home was genuinely purged.
    ///
    /// Path compression is required for repeated compaction: after `A → B`
    /// and then `B → C`, removing `A → B` merely because B is no longer a
    /// direct record loses the heap/header alias even though C is still live.
    /// Rewriting it to `A → C` keeps every external stale pointer valid. The
    /// old implementation already scanned the whole map to prune, so this
    /// remains O(number of outstanding aliases) per compaction while keeping
    /// subsequent hot lookups one hop.
    fn register_forwardings(&mut self, remap: &[(u64, u64)]) {
        for &(old, new) in remap {
            let origin = self.compaction_origins.remove(&old).unwrap_or(old);
            self.compaction_origins.insert(new, origin);
            if self.compensated.contains(&old) {
                self.compensated.insert(new);
            }
            self.compaction_forwarded.insert(old, new);
        }
        if !remap.is_empty() {
            self.compensation_manifest_dirty
                .store(true, Ordering::Release);
        }
        if !self.compaction_forwarded.is_empty() {
            let aliases: Vec<u64> = self.compaction_forwarded.keys().copied().collect();
            for alias in aliases {
                let home = self.resolve_forwarded(alias);
                match home.filter(|home| self.records.contains_key(home)) {
                    Some(home) => {
                        self.compaction_forwarded.insert(alias, home);
                    }
                    None => {
                        self.compaction_forwarded.remove(&alias);
                    }
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn get(&self, undo_ptr: u64) -> Option<&UndoRecord> {
        // Hot path: a single direct lookup. Only a true miss (the record
        // was relocated by compaction) consults the forwarding table,
        // which short-circuits when empty — the steady-state cost is nil.
        if let Some(record) = self.records.get(&undo_ptr) {
            if record.resident {
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                return Some(record);
            }
            self.cache_misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        if let Some(fwd) = self.resolve_forwarded(undo_ptr) {
            if let Some(record) = self.records.get(&fwd) {
                if record.resident {
                    self.cache_hits.fetch_add(1, Ordering::Relaxed);
                    return Some(record);
                }
            }
        }
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Cold-path read: returns an owned `UndoRecord`.
    ///
    /// Three cases, by directory (`records`) membership:
    /// * Resident → clone the in-memory entry.
    /// * Present but payload evicted (Phase 2) → page the frame back in
    ///   from the segment file (validated: magic / version / CRC).
    /// * Absent → `None`. An absent id means the record was *purged*, not
    ///   merely evicted; its off-page LOB chains and heap slot may already
    ///   be reclaimed, so a frame still lingering in an un-rotated segment
    ///   must never be served (that would be a use-after-free). This is
    ///   what upholds the "snapshot too old" (1236) contract: purge is the
    ///   single authority on when a version becomes unreadable.
    pub(crate) fn load_owned(&self, undo_ptr: u64) -> Option<UndoRecord> {
        // A ptr the caller still holds may have been physically relocated
        // by compaction (heap headers / before_header chains are not
        // rewritten in place). Hot path is a single direct lookup; only a
        // miss resolves through the (usually empty) forwarding table so
        // the page-in reads the NEW segment, not the unlinked old one.
        let undo_ptr = if self.records.contains_key(&undo_ptr) {
            undo_ptr
        } else {
            self.resolve_forwarded(undo_ptr).unwrap_or(undo_ptr)
        };
        match self.records.get(&undo_ptr) {
            Some(record) if record.resident => {
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                Some(record.clone())
            }
            Some(_) => {
                // Present but payload evicted — page in from disk.
                self.cache_misses.fetch_add(1, Ordering::Relaxed);
                if let Some(bytes) = self.replicated_pending_frames.get(&undo_ptr) {
                    return Self::decode_pending_replicated_record(undo_ptr, bytes).ok();
                }
                let segment_id = undo_ptr_segment(undo_ptr);
                if segment_id == 0 {
                    return None;
                }
                match self.read_frame_from_segment(segment_id, undo_ptr_offset(undo_ptr)) {
                    Ok(Some(record)) => Some(record),
                    _ => None,
                }
            }
            None => {
                // Absent = purged (or never existed): never read disk.
                self.cache_misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Resolve an undo lookup while the directory latch is held, but defer a
    /// cold segment read until after that latch is released.
    pub(crate) fn prepare_load_owned(
        &self,
        undo_ptr: u64,
    ) -> Result<Option<PreparedUndoRead>, QueryError> {
        let undo_ptr = if self.records.contains_key(&undo_ptr) {
            undo_ptr
        } else {
            self.resolve_forwarded(undo_ptr).unwrap_or(undo_ptr)
        };
        match self.records.get(&undo_ptr) {
            Some(record) if record.resident => {
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                Ok(Some(PreparedUndoRead::Resident(record.clone())))
            }
            Some(_) => {
                self.cache_misses.fetch_add(1, Ordering::Relaxed);
                // A physical allocation gap does not make the WAL-backed
                // before-image unavailable. Its decoded cache may be evicted
                // before the segment prefix arrives; read the bounded reorder
                // frame rather than a segment offset that does not exist yet.
                if let Some(bytes) = self.replicated_pending_frames.get(&undo_ptr) {
                    return Ok(Some(PreparedUndoRead::Resident(
                        Self::decode_pending_replicated_record(undo_ptr, bytes)?,
                    )));
                }
                let segment_id = undo_ptr_segment(undo_ptr);
                if segment_id == 0 {
                    return Ok(None);
                }
                let Some(file) = self.segment_reader(segment_id)? else {
                    return Ok(None);
                };
                Ok(Some(PreparedUndoRead::Segment {
                    file,
                    segment_id,
                    offset: undo_ptr_offset(undo_ptr),
                    file_end: self.segment_file_bytes.get(&segment_id).copied(),
                }))
            }
            None => {
                self.cache_misses.fetch_add(1, Ordering::Relaxed);
                Ok(None)
            }
        }
    }

    fn decode_pending_replicated_record(ptr: u64, bytes: &[u8]) -> Result<UndoRecord, QueryError> {
        let mut record = decode_undo_record(bytes)
            .or_else(|_| serde_json::from_slice::<UndoRecord>(bytes).map_err(json_err))?;
        record.id = ptr.max(record.id).max(1);
        Ok(record)
    }

    /// Resolve (opening + caching if needed) the read handle for a
    /// segment file. The handle is positioned-read (`read_at`) only, so
    /// one shared `Arc<File>` serves concurrent page-ins without seek
    /// races. Returns `None` when the file does not exist (purged).
    fn segment_reader(&self, segment_id: u32) -> Result<Option<std::sync::Arc<File>>, QueryError> {
        if let Some(file) = self.segment_readers.lock().get(segment_id) {
            return Ok(Some(file));
        }
        let path = self.segment_path(segment_id)?;
        if !path.exists() {
            return Ok(None);
        }
        let file = std::sync::Arc::new(OpenOptions::new().read(true).open(&path).map_err(io_err)?);
        // LRU-insert: past the cap the least-recently-used handle is
        // evicted (and reopens lazily if touched again) rather than
        // flushing the whole cache and thrashing hot handles.
        self.segment_readers.lock().insert(segment_id, file.clone());
        Ok(Some(file))
    }

    /// Drop any cached read handle for `segment_id` (called when a
    /// segment file is unlinked or rewritten so stale fds are released).
    fn invalidate_segment_reader(&self, segment_id: u32) {
        self.segment_readers.lock().remove(segment_id);
    }

    /// Read the frame backing `(segment_id, offset)` and decode it.
    /// Returns `Ok(None)` when the segment file is missing (purged) or
    /// the offset is past the segment's recorded tail; returns `Err`
    /// when the frame is physically present but corrupt.
    fn read_frame_from_segment(
        &self,
        segment_id: u32,
        offset: u64,
    ) -> Result<Option<UndoRecord>, QueryError> {
        let Some(file) = self.segment_reader(segment_id)? else {
            return Ok(None);
        };
        read_undo_frame_from_open_file(
            &file,
            segment_id,
            offset,
            self.segment_file_bytes.get(&segment_id).copied(),
        )
    }

    /// Phase 2 — evict the heavy payload of the oldest flushed records
    /// once `resident_bytes` exceeds `mem_cache_limit_bytes`, bringing
    /// memory back to a low-water mark. Evicted records keep their
    /// metadata in `records` (purge/accounting untouched) and are paged
    /// back in on demand via `load_owned`. No-op when the cap is 0 or
    /// memory is already within budget — the hot append path pays only a
    /// branch in that case.
    ///
    /// Victim selection consumes only resident payload candidates in creation
    /// order. Each payload is visited once; the retained metadata floor never
    /// triggers a history scan or a flush when there is nothing left to evict.
    /// Flush shard tails before removing candidates so failed I/O retains both
    /// payloads and candidates for a later retry.
    fn evict_to_cache_budget(&mut self) {
        let limit = self.mem_cache_limit_bytes;
        if limit == 0 || self.resident_bytes <= limit || self.eviction_candidates.is_empty() {
            return;
        }
        // Make every persistent frame durable on its segment file so an
        // evicted payload can always be paged back in. If the flush fails
        // we keep payloads resident rather than risk an unrecoverable
        // eviction. Only the shard tail buffers need draining here.
        for shard_mu in self.write_shards.iter() {
            if shard_mu.lock().flush_all().is_err() {
                return;
            }
        }
        let target = limit.saturating_sub(limit / 8); // ~87.5% low-water
        let mut projected = self.resident_bytes;
        let mut victims: Vec<u64> = Vec::new();
        while projected > target {
            let Some((_created_ms, id)) = self.eviction_candidates.pop_first() else {
                break;
            };
            let Some(rec) = self.records.get(&id) else {
                continue;
            };
            let metadata_bytes = metadata_resident_bytes(rec);
            projected = projected.saturating_sub(resident_size(rec).saturating_sub(metadata_bytes));
            victims.push(id);
        }
        for id in victims {
            let mut released_charge = 0u64;
            if let Some(rec) = self.records.get_mut(&id) {
                if rec.resident {
                    rec.resident = false;
                    // Keep only directory and physical-cleanup metadata.
                    // `before_external` stays resident so `apply_purge` can
                    // free off-page chains without paging the row image in.
                    rec.before_values = None;
                    match &mut rec.kind {
                        UndoKind::InsertBatch { row_ids } => *row_ids = Vec::new(),
                        UndoKind::UpdateDelta { before_cols } => *before_cols = Vec::new(),
                        UndoKind::Insert
                        | UndoKind::Update
                        | UndoKind::Delete
                        | UndoKind::DeleteHeader => {}
                    }
                    let old_resident = resident_size(rec);
                    rec.resident_size =
                        u32::try_from(metadata_resident_bytes(rec)).unwrap_or(u32::MAX);
                    released_charge = old_resident.saturating_sub(u64::from(rec.resident_size));
                    self.resident_bytes = self.resident_bytes.saturating_sub(released_charge);
                    self.evicted_records = self.evicted_records.saturating_add(1);
                }
            }
            self.release_resident_charge(released_charge);
        }
    }

    /// Phase 2 — in-memory payload cap setter (bytes; 0 = unbounded).
    pub(crate) fn set_mem_cache_limit_bytes(&mut self, bytes: u64) {
        self.mem_cache_limit_bytes = bytes;
        self.evict_to_cache_budget();
    }

    pub(crate) fn mem_cache_limit_bytes(&self) -> u64 {
        self.mem_cache_limit_bytes
    }

    pub(crate) fn resident_bytes(&self) -> u64 {
        self.resident_bytes
    }

    #[cfg(test)]
    pub(crate) fn resident_metadata_floor_bytes(&self) -> u64 {
        self.records
            .values()
            .map(|record| metadata_resident_bytes(record))
            .fold(0u64, u64::saturating_add)
    }

    pub(crate) fn evicted_records(&self) -> u64 {
        self.evicted_records
    }

    /// Phase 4 — per-transaction undo byte cap (0 = unlimited). A single
    /// runaway transaction is rejected on its own undo growth instead of
    /// being allowed to exhaust the whole tablespace and take every other
    /// session down with a global 1236.
    pub(crate) fn set_txn_max_bytes(&mut self, bytes: u64) {
        if bytes != 0 {
            self.concurrent_append_eligible
                .store(false, Ordering::Release);
        }
        self.txn_max_bytes = bytes;
        if bytes == 0 && !self.retention_guarantee && self.undo_dir.is_some() {
            self.concurrent_append_eligible
                .store(true, Ordering::Release);
        }
    }

    pub(crate) fn txn_max_bytes(&self) -> u64 {
        self.txn_max_bytes
    }

    /// Phase 4 — logical undo bytes attributed to `txn_id` (resident +
    /// evicted), used for the per-txn cap check and blocker diagnostics.
    pub(crate) fn txn_undo_bytes(&self, txn_id: u64) -> u64 {
        self.txn_bytes.get(&txn_id).copied().unwrap_or(0)
    }

    /// BUG-1 (docs/lob-management-audit-2026-07.md) — the set of txn ids
    /// that still own at least one un-purged undo record.
    ///
    /// Slot GC must retain a **rolled-back** txn's slot until this set no
    /// longer contains it. Otherwise the slot is dropped and its creator
    /// state degrades to `None` at purge time — indistinguishable from a
    /// committed-then-GC'd txn, whose before-image chains *are* obsolete
    /// and must be freed. Keeping the rolled-back slot alive lets
    /// `apply_purge` positively identify `RolledBack` creators and skip
    /// freeing before-image chains that rollback already handed back to
    /// the live row (a use-after-free / double-free otherwise).
    /// Whether `txn_id` still owns any un-purged undo records. O(1) map probe,
    /// used by post-commit GC to decide if a rolled-back slot's before-image
    /// chains have drained. This deliberately replaces an earlier
    /// `txn_ids_with_undo()` helper that materialized a set of *every* retained
    /// writer (tens to hundreds of thousands under a 60s retention at OLTP
    /// rates) on each call and, held under the undo read lock, blocked every
    /// concurrent undo append while it built.
    pub(crate) fn has_undo_for_txn(&self, txn_id: u64) -> bool {
        self.txn_bytes.contains_key(&txn_id)
    }

    /// Distinct creators that can actually participate in the next purge.
    /// Using this bounded-to-undo set avoids materializing transaction state
    /// for every retained read-only/committed slot on every purge tick.
    pub(crate) fn creator_txn_ids(&self) -> Vec<u64> {
        self.txn_bytes.keys().copied().collect()
    }

    /// Phase 4 — the transaction holding the most undo bytes right now,
    /// as `(txn_id, bytes)` (`(0, 0)` when there is no undo). This is the
    /// "who is hogging undo / blocking purge" diagnostic surfaced through
    /// SHOW ENGINE MYDB STATUS so an operator can pinpoint a runaway or
    /// long-running transaction instead of guessing.
    pub(crate) fn largest_txn(&self) -> (u64, u64) {
        self.txn_bytes
            .iter()
            .max_by_key(|(_, bytes)| **bytes)
            .map(|(txn, bytes)| (*txn, *bytes))
            .unwrap_or((0, 0))
    }

    /// Phase 4 — subtract a removed record's bytes from its txn tally,
    /// dropping the entry once it reaches zero.
    fn debit_txn_bytes(&mut self, txn_id: u64, bytes: u64) {
        if let Some(slot) = self.txn_bytes.get_mut(&txn_id) {
            *slot = slot.saturating_sub(bytes);
            if *slot == 0 {
                self.txn_bytes.remove(&txn_id);
            }
        }
    }

    /// Exceptional head-publication failure after a WAL twin exists. Persist
    /// an exclusion before dropping the mirror; segment scans and WAL reloads
    /// must not resurrect it. Unlike remove_record, do not rewind allocation:
    /// the old WAL identity is still durable and must never alias a new frame.
    pub(crate) fn discard_unpublished_record(&mut self, ptr: u64) -> Result<(), QueryError> {
        self.discarded.insert(ptr);
        self.compensation_manifest_dirty
            .store(true, Ordering::Release);
        // The WAL twin may still be transaction-buffered. Force the segment
        // tail before the exclusion so restart cannot reuse this physical ID
        // even when that WAL buffer never became durable.
        self.flush()?;
        self.mark_compensated(&[ptr]);
        let outcome = self.apply_purge(&[ptr], &BTreeMap::new());
        debug_assert!(outcome.freed_chains.is_empty());
        Self::unlink_segments(&outcome.removable_segments);
        Ok(())
    }

    pub(crate) fn remove_record(&mut self, undo_ptr: u64) -> Result<(), QueryError> {
        let Some(record) = self.records.remove(&undo_ptr) else {
            return Ok(());
        };
        self.eviction_candidates
            .remove(&(record.created_unix_ms, undo_ptr));
        self.debit_txn_bytes(record.txn_id, record_size(&record));
        if let Some(ids) = self.created_index.get_mut(&record.created_unix_ms) {
            ids.remove(&undo_ptr);
            if ids.is_empty() {
                self.created_index.remove(&record.created_unix_ms);
            }
        }
        self.current_bytes = self.current_bytes.saturating_sub(record_size(&record));
        self.resident_bytes = self.resident_bytes.saturating_sub(resident_size(&record));
        self.release_resident_charge(resident_size(&record));
        let segment_id = undo_ptr_segment(undo_ptr);
        if segment_id != 0 {
            if let Some(live) = self.segment_live_records.get_mut(&segment_id) {
                *live = live.saturating_sub(1);
            }
            let offset = undo_ptr_offset(undo_ptr);
            // M1 (P1-A) — use the frame length captured at append/scan/
            // page-in instead of re-encoding the whole record just to
            // learn its byte count. Only a record that never reached a
            // real segment (test synthetic) has `encoded_len == 0`, and
            // those never enter this `segment_id != 0` branch — the
            // re-encode fallback is a defensive belt-and-suspenders.
            let frame_payload_len = if record.encoded_len > 0 {
                u64::from(record.encoded_len)
            } else {
                encode_undo_record(&record)?.len() as u64
            };
            let frame_end = offset
                .saturating_add(UNDO_FRAME_HEADER_LEN as u64)
                .saturating_add(frame_payload_len);
            // Route the active-tail truncation to the segment's owning
            // shard. A compaction-lane segment (`COMPACTION_SHARD_LANE`)
            // has no write shard and can never be an active tail, so there
            // is nothing to truncate — skip it rather than erroring.
            let shard_idx = shard_of_segment(segment_id) as usize;
            let shard_mu = match self.write_shards.get(shard_idx) {
                Some(s) => s,
                None => {
                    return Ok(());
                }
            };
            let mut new_end_opt: Option<u64> = None;
            {
                let mut shard = shard_mu.lock();
                if !self.capture_active.load(Ordering::Acquire)
                    && self.archive_floor.load(Ordering::Acquire) == 0
                    && !crate::repl::physical_replication_active()
                    && segment_id == shard.active_segment_id
                    && frame_end == shard.active_offset
                {
                    if !shard.tail_buffer.is_empty() && offset >= shard.buffer_start {
                        let drop_bytes = (offset - shard.buffer_start) as usize;
                        shard.tail_buffer.truncate(drop_bytes);
                    } else {
                        shard.flush_tail_buffer()?;
                        if let Some(file) = shard.active_file.as_mut() {
                            file.set_len(offset).map_err(io_err)?;
                            file.seek(SeekFrom::Start(offset)).map_err(io_err)?;
                        }
                        shard.buffer_start = offset;
                    }
                    shard.active_offset = offset;
                    new_end_opt = Some(offset);
                }
            }
            if let Some(new_end) = new_end_opt {
                // M1 (P1-D) — maintain `disk_bytes` incrementally instead
                // of folding over every segment on each removal. A LIFO
                // rollback of a large txn calls this once per row; the old
                // O(#segments) fold made it O(rows × segments).
                let old_end = self
                    .segment_file_bytes
                    .insert(segment_id, new_end)
                    .unwrap_or(new_end);
                self.disk_bytes = self
                    .disk_bytes
                    .saturating_sub(old_end)
                    .saturating_add(new_end);
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn insert(
        &mut self,
        record: UndoRecord,
        _wal_logged: bool,
        _journal_logged: bool,
    ) -> Result<(), QueryError> {
        let reservation = self.reserve_record_resident(&record)?;
        self.insert_reserved(record, reservation)
    }

    fn insert_reserved(
        &mut self,
        mut record: UndoRecord,
        reservation: UndoResidentReservation,
    ) -> Result<(), QueryError> {
        if record.id == 0 {
            record.id = self.next_undo_id.max(1);
        }
        let id = record.id;
        if self.records.contains_key(&id) {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                format!("undo record id collision: {}", id),
            ));
        }
        let depth = match record.prev_undo_ptr {
            None => 1,
            // Resolve through forwarding: a freshly appended record's prev
            // is the txn head, which `remap_active_undo_heads` keeps
            // current, but a compaction that raced just before the head
            // remap can leave it pointing at a relocated record's old ptr.
            Some(ptr) => self
                .records
                .get(&self.resolve_forwarded(ptr).unwrap_or(ptr))
                .map(|r| r.chain_depth)
                .unwrap_or(0)
                .saturating_add(1)
                .max(1),
        };
        record.chain_depth = depth;
        if record.cached_size == 0 {
            record.cached_size =
                u32::try_from(estimate_undo_record_bytes(&record)).unwrap_or(u32::MAX);
        }
        if record.resident_size == 0 {
            record.resident_size =
                u32::try_from(estimate_undo_resident_bytes(&record)).unwrap_or(u32::MAX);
        }
        self.insert_loaded_record_reserved(record, reservation)?;
        self.refresh_peak_after_insert(depth);
        Ok(())
    }

    fn refresh_peak_after_insert(&mut self, inserted_depth: u32) {
        self.longest_chain_depth = self.longest_chain_depth.max(inserted_depth);
        let count = self.records.len() as u64;
        self.peak_records = self.peak_records.max(count);
        self.peak_bytes = self.peak_bytes.max(self.current_bytes);
    }

    pub(crate) fn bytes(&self) -> u64 {
        self.current_bytes
    }

    pub(crate) fn record_count(&self) -> usize {
        self.records.len()
    }

    /// Creation timestamp (unix ms) of the oldest still-present undo
    /// record, or `None` when the tablespace is empty. `created_index`
    /// is keyed by `created_unix_ms`, so this is the map's first key.
    /// Used as a cheap stranding detector: when purge keeps up, the
    /// oldest record hovers around the retention window; a value far
    /// past retention means records are pinned (long txn / long read
    /// snapshot) and normal purge cannot drain them — the only case
    /// where segment compaction earns its cost.
    pub(crate) fn oldest_created_unix_ms(&self) -> Option<u64> {
        self.created_index.keys().next().copied()
    }

    /// Phase 1 of the two-phase purge: pick victim ids under a SHARED
    /// lock so concurrent DML undo appends are not stalled by the scan.
    /// Eligibility is monotone — a record unreachable from every
    /// in-flight chain, with a finalized creator below the oldest
    /// snapshot, and at least as old as the retention window can never become
    /// reachable again — so victims chosen here stay valid when
    /// `apply_purge` re-takes the write lock.
    ///
    /// `active_txn_heads` is `&[(txn_id, undo_head)]`. The `txn_id`
    /// drives the per-txn keep-set cache (see
    /// [`PurgeKeepCache`]): only the delta between the cached head
    /// and the current head is walked, and finalized txns are dropped
    /// wholesale. Steady-state cost on long-running OLTP collapses
    /// from `O(Σ chain_depth)` to `O(Σ undos_pushed_since_last_purge)`.
    #[cfg(test)]
    pub(crate) fn plan_purge_victims(
        &self,
        active_txn_heads: &[(u64, u64)],
        retain_since_unix_ms: u64,
        oldest_snapshot_scn: Option<u64>,
        creator_states: &BTreeMap<u64, UndoCreatorState>,
        committed_creator_watermark: u64,
        batch_limit: usize,
    ) -> Vec<u64> {
        self.plan_purge_victims_with_recent(
            active_txn_heads,
            retain_since_unix_ms,
            oldest_snapshot_scn,
            creator_states,
            committed_creator_watermark,
            &HashSet::new(),
            &mut 0,
            batch_limit,
        )
    }

    /// Select up to `batch_limit` purge victims in creation order, starting
    /// at the `created_unix_ms` bucket `*resume_unix_ms` and leaving it at
    /// the last bucket visited. A purge run that applies its victims in
    /// several short lock rounds resumes there instead of rescanning the
    /// records earlier rounds had to keep; that bucket is revisited because
    /// the limit may have cut it short.
    pub(crate) fn plan_purge_victims_with_recent(
        &self,
        active_txn_heads: &[(u64, u64)],
        retain_since_unix_ms: u64,
        oldest_snapshot_scn: Option<u64>,
        creator_states: &BTreeMap<u64, UndoCreatorState>,
        committed_creator_watermark: u64,
        recent_committed_txns: &HashSet<u64>,
        resume_unix_ms: &mut u64,
        batch_limit: usize,
    ) -> Vec<u64> {
        self.refresh_keep_cache(active_txn_heads);
        let cache = self.keep_cache.lock();
        let keep = &cache.flat_keep;
        let limit = batch_limit.max(1);
        let mut victims: Vec<u64> = Vec::new();
        if *resume_unix_ms > retain_since_unix_ms {
            return victims;
        }
        'buckets: for (&created_unix_ms, ids) in self
            .created_index
            .range(*resume_unix_ms..=retain_since_unix_ms)
        {
            *resume_unix_ms = created_unix_ms;
            for id in ids {
                if victims.len() >= limit {
                    break 'buckets;
                }
                let keep_for_creator = self.records.get(id).is_some_and(|record| {
                    if recent_committed_txns.contains(&record.txn_id) {
                        return true;
                    }
                    match creator_states.get(&record.txn_id).copied() {
                        Some(UndoCreatorState::Active | UndoCreatorState::Prepared) => true,
                        Some(UndoCreatorState::Committed(commit_scn)) => {
                            oldest_snapshot_scn.is_some_and(|oldest| commit_scn > oldest)
                        }
                        Some(UndoCreatorState::RolledBack) => false,
                        // `creator_states` and the undo map are sampled under
                        // different locks. A transaction can begin, append
                        // undo, and even commit after the transaction-state
                        // snapshot but before this shared undo scan. Such a
                        // creator is absent from the stale map, but is NOT an
                        // old committed slot: retain it for this sweep and
                        // classify it on the next one. Only an absent creator
                        // at/below the transaction-slot GC watermark is known
                        // to have committed and been collected.
                        None => record.txn_id > committed_creator_watermark,
                    }
                });
                if !keep.contains(id) && !keep_for_creator && self.records.contains_key(id) {
                    victims.push(*id);
                }
            }
        }
        victims
    }

    /// Bring `keep_cache` into sync with the latest active-txn snapshot.
    /// * New active txn → walk its full chain.
    /// * Existing txn whose head advanced → walk from new head down to
    ///   the cached head (the cached head and everything below it is
    ///   already in `flat_keep`; we stop the descent there).
    /// * Existing txn whose head unchanged → no walking.
    /// * Txn no longer active → drop the txn's chain from `flat_keep`
    ///   and the per-txn maps.
    /// The records BTreeMap is authoritative for `prev_undo_ptr` — if a
    /// cached entry's record was purged the chain naturally truncates.
    fn refresh_keep_cache(&self, active_txn_heads: &[(u64, u64)]) {
        let mut cache = self.keep_cache.lock();
        let active_set: HashSet<u64> = active_txn_heads.iter().map(|(t, _)| *t).collect();
        // Drop finalized txns. Take ownership of their chain sets so
        // we can subtract them from flat_keep without re-iterating
        // chain_per_txn.
        let drop_keys: Vec<u64> = cache
            .head_per_txn
            .keys()
            .copied()
            .filter(|t| !active_set.contains(t))
            .collect();
        for txn_id in drop_keys {
            cache.head_per_txn.remove(&txn_id);
            if let Some(chain) = cache.chain_per_txn.remove(&txn_id) {
                for ptr in chain {
                    cache.flat_keep.remove(&ptr);
                }
            }
        }
        // Walk the delta for each active txn. Split the cache borrow
        // into disjoint fields so the inner chain set and flat_keep
        // can both be mutated inside the walk loop.
        let PurgeKeepCache {
            head_per_txn,
            chain_per_txn,
            flat_keep,
        } = &mut *cache;
        for (txn_id, head) in active_txn_heads {
            let stop = head_per_txn.get(txn_id).copied();
            if stop == Some(*head) {
                continue;
            }
            let chain = chain_per_txn.entry(*txn_id).or_default();
            let mut current = Some(*head);
            while let Some(raw) = current {
                // Resolve through the compaction forwarding table BEFORE
                // the record enters the keep-set or is compared to `stop`.
                // Since P1/10 the referrer's on-disk / in-memory
                // `prev_undo_ptr` is left pointing at a relocated record's
                // OLD ptr (compaction rewrites only the moved records, not
                // their referrers), so a raw descent would insert a dead
                // old ptr into `flat_keep` and drop the record's live home
                // — letting purge reclaim a version still reachable from an
                // active txn chain. `resolve_forwarded` is a no-op (empty
                // table short-circuit) in steady state, so this costs
                // nothing until a compaction has actually relocated records.
                let ptr = self.resolve_forwarded(raw).unwrap_or(raw);
                if Some(ptr) == stop {
                    break;
                }
                if !chain.insert(ptr) {
                    break;
                }
                flat_keep.insert(ptr);
                current = self
                    .records
                    .get(&ptr)
                    .and_then(|record| record.prev_undo_ptr);
            }
            head_per_txn.insert(*txn_id, *head);
        }
    }

    /// Reset the keep-set cache. Called after [`apply_purge`] removes
    /// records — purged ids may live in `flat_keep` only if they were
    /// `keep`-set members at planning time, in which case they were
    /// NOT selected as victims. So in practice the cache stays
    /// consistent; this hook exists for the safety net (and for tests
    /// that manually mutate records).
    fn invalidate_keep_cache(&self) {
        let mut cache = self.keep_cache.lock();
        cache.head_per_txn.clear();
        cache.chain_per_txn.clear();
        cache.flat_keep.clear();
    }

    /// Account one purge run, whether or not it found anything to drop and
    /// however many lock rounds it applied its victims in.
    pub(crate) fn record_purge_run(&mut self) {
        self.purge_runs = self.purge_runs.saturating_add(1);
        self.purge_last_unix_ms = crate::storage::naming::current_unix_ms();
    }

    /// Phase 2 as one run: drop the victims under the write lock.
    pub(crate) fn apply_purge(
        &mut self,
        victims: &[u64],
        creator_states: &BTreeMap<u64, UndoCreatorState>,
    ) -> PurgeOutcome {
        self.record_purge_run();
        self.apply_purge_victims(victims, creator_states)
    }

    /// Phase 2: drop the victims under the write lock, without accounting a
    /// run (see `record_purge_run`). File deletion is NOT performed here —
    /// fully-dead segments are returned in
    /// `PurgeOutcome::removable_segments` so the caller can unlink them
    /// after releasing the undo lock.
    pub(crate) fn apply_purge_victims(
        &mut self,
        victims: &[u64],
        creator_states: &BTreeMap<u64, UndoCreatorState>,
    ) -> PurgeOutcome {
        let mut freed_chains: Vec<(u64, Vec<(usize, LobRef)>)> = Vec::new();
        // rowid-6bytes M2: every Delete undo record purged in this run
        // describes a heap tombstone whose `oldest_active_snapshot_scn`
        // has advanced past its `deleted_scn`. By the time the executor
        // commits the DELETE, `delete_indexes_deferred_at` has already
        // removed every secondary-index entry pointing at the row, so
        // the only remaining reference to the slot is the heap itself
        // — flipping `ROW_FLAG_DELETED → ROW_FLAG_FREE` is now safe and
        // makes the slot reusable on the next INSERT.
        let mut reclaim_targets: Vec<(u64, RowId)> = Vec::new();
        let mut purged: u64 = 0;
        let mut released_resident = 0u64;
        for id in victims {
            let Some(removed) = self.records.remove(id) else {
                continue;
            };
            self.eviction_candidates
                .remove(&(removed.created_unix_ms, *id));
            if let Some(ids) = self.created_index.get_mut(&removed.created_unix_ms) {
                ids.remove(id);
                if ids.is_empty() {
                    self.created_index.remove(&removed.created_unix_ms);
                }
            }
            self.current_bytes = self.current_bytes.saturating_sub(record_size(&removed));
            self.resident_bytes = self.resident_bytes.saturating_sub(resident_size(&removed));
            released_resident = released_resident.saturating_add(resident_size(&removed));
            self.debit_txn_bytes(removed.txn_id, record_size(&removed));
            let segment_id = undo_ptr_segment(*id);
            if segment_id != 0 {
                if let Some(live) = self.segment_live_records.get_mut(&segment_id) {
                    *live = live.saturating_sub(1);
                }
            }
            // BUG-1 (docs/lob-management-audit-2026-07.md) — a purged
            // record from a *rolled-back* creator must NOT trigger any
            // reclaim of the pre-image it restored. Rollback already
            // handed the before-image (its slot AND its off-page LOB
            // chains) back to the live row; freeing those here is a
            // use-after-free / double-free. `RolledBack` is positively
            // identifiable because slot GC retains the rolled-back slot
            // until its undo bytes drain (see `has_undo_for_txn`), so a
            // rolled-back creator never degrades to the ambiguous `None`
            // (which also covers legitimately committed-then-GC'd records
            // whose old chains *are* obsolete and must be freed).
            let creator_rolled_back = self.compensated.contains(id)
                || matches!(
                    creator_states.get(&removed.txn_id),
                    Some(UndoCreatorState::RolledBack)
                );
            if undo_ptr_segment(*id) == 0 {
                self.compensated.remove(id);
            }
            // rowid-6bytes M2 — Delete undo records carry the heap
            // tombstone's `(table_id, row_id)`. For a committed creator the
            // `Committed(commit_scn <= oldest_snapshot_scn)` branch proves
            // the slot is a genuine tombstone; `reclaim_slot_logged` also
            // re-checks `is_slot_mvcc_tombstone`, so a rolled-back restore
            // would be a safe no-op — but we skip it outright to avoid
            // churning the reclaim quarantine on a live slot.
            if matches!(removed.kind, UndoKind::Delete | UndoKind::DeleteHeader)
                && !creator_rolled_back
            {
                reclaim_targets.push((removed.table_id, removed.row_id));
            }
            if !creator_rolled_back && !removed.before_external.is_empty() {
                freed_chains.push((removed.table_id, removed.before_external));
            }
            purged = purged.saturating_add(1);
        }
        // Keep removed bytes conservatively charged until the batch finishes,
        // then release once instead of serializing every record with append
        // admission on the shared charge mutex.
        self.release_resident_charge(released_resident);
        self.purged_records = self.purged_records.saturating_add(purged);
        let removable_segments = self.detach_empty_segments();
        PurgeOutcome {
            freed_chains,
            reclaim_targets,
            removable_segments,
        }
    }

    /// Replica-only resident-directory eviction. Source segment files and
    /// allocation high-water marks remain byte-for-byte intact; no LOB/slot
    /// reclamation or segment live-count mutation is allowed locally.
    /// Like `apply_purge_victims`, it does not account a run.
    pub(crate) fn evict_replica_records(&mut self, victims: &[u64]) {
        let mut released_resident = 0u64;
        for id in victims {
            let Some(removed) = self.records.remove(id) else {
                continue;
            };
            self.eviction_candidates
                .remove(&(removed.created_unix_ms, *id));
            if let Some(ids) = self.created_index.get_mut(&removed.created_unix_ms) {
                ids.remove(id);
                if ids.is_empty() {
                    self.created_index.remove(&removed.created_unix_ms);
                }
            }
            self.current_bytes = self.current_bytes.saturating_sub(record_size(&removed));
            self.resident_bytes = self.resident_bytes.saturating_sub(resident_size(&removed));
            released_resident = released_resident.saturating_add(resident_size(&removed));
            self.debit_txn_bytes(removed.txn_id, record_size(&removed));
            self.purged_records = self.purged_records.saturating_add(1);
        }
        self.release_resident_charge(released_resident);
        self.invalidate_keep_cache();
    }

    #[cfg(test)]
    /// Compatibility wrapper (tests, single-lock callers): plan + apply
    /// under the caller's existing exclusive access, then unlink the
    /// detached segment files inline.
    ///
    /// `heads` here is the legacy `&[u64]` shape — synthetic txn ids
    /// are derived by enumerating, so each head gets a fresh slot in
    /// the keep-set cache. Tests that exercise the same UndoManager
    /// across multiple `purge_except_chains` calls with shifting head
    /// sets should reset the cache via [`invalidate_keep_cache`] or
    /// keep their head order stable across calls.
    pub(crate) fn purge_except_chains(
        &mut self,
        heads: &[u64],
        retain_since_unix_ms: u64,
        oldest_snapshot_scn: Option<u64>,
        creator_states: &BTreeMap<u64, UndoCreatorState>,
        batch_limit: usize,
    ) -> PurgeOutcome {
        let active_txn_heads: Vec<(u64, u64)> = heads
            .iter()
            .enumerate()
            .map(|(i, head)| (i as u64 + 1, *head))
            .collect();
        // Tests reuse the same UndoManager across calls with completely
        // different head sets — invalidate the cache so a previous
        // call's stale txn entries can't pin records the current call
        // expects to drop.
        self.invalidate_keep_cache();
        let victims = self.plan_purge_victims(
            &active_txn_heads,
            retain_since_unix_ms,
            oldest_snapshot_scn,
            creator_states,
            u64::MAX,
            batch_limit,
        );
        let mut outcome = self.apply_purge(&victims, creator_states);
        Self::unlink_segments(&outcome.removable_segments);
        outcome.removable_segments.clear();
        outcome
    }

    /// P1/10 — sub-segment compaction: list segments whose live-record
    /// share has dropped below `min_live_ratio_pct` (approximated as
    /// `live_count * avg_record_bytes / segment_size`). Returned ids
    /// are sorted by live-record count ascending, so the cheapest
    /// rewrites come first. Active segments are excluded.
    pub(crate) fn compaction_candidates(
        &self,
        min_live_ratio_pct: u32,
        batch_limit: usize,
    ) -> Vec<u32> {
        let active = self.protected_segment_ids();
        let estimated_bytes = self
            .segment_file_bytes
            .len()
            .saturating_mul(std::mem::size_of::<(u32, u64, u64, u64)>());
        let Some(_scratch_charge) =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                estimated_bytes,
            )
        else {
            return Vec::new();
        };
        let mut segments: Vec<(u32, u64, u64, u64)> = self
            .segment_file_bytes
            .iter()
            .filter(|(id, _)| !active.contains(*id))
            .filter_map(|(id, bytes)| {
                let live = self.segment_live_records.get(id).copied().unwrap_or(0);
                if live == 0 {
                    // Fully dead — already eligible for detach_empty_segments.
                    return None;
                }
                Some((*id, live, *bytes, 0))
            })
            .collect();
        segments.sort_unstable_by_key(|entry| entry.0);
        // `records` is a hash directory, not an ordered range map. Accumulate
        // every segment's live bytes in one pass instead of rescanning the
        // entire directory once per candidate segment.
        for (ptr, record) in &self.records {
            let segment_id = undo_ptr_segment(*ptr);
            if let Ok(pos) = segments.binary_search_by_key(&segment_id, |entry| entry.0) {
                segments[pos].3 = segments[pos]
                    .3
                    .saturating_add(u64::from(record.cached_size));
            }
        }
        let mut candidates: Vec<(u32, u64, u64)> = segments
            .into_iter()
            .filter_map(|(id, live, bytes, live_bytes)| {
                let ratio_pct = (live_bytes.saturating_mul(100) / bytes.max(1)) as u32;
                (ratio_pct < min_live_ratio_pct).then_some((id, live, bytes))
            })
            .collect();
        candidates.sort_by_key(|(_, live, _)| *live);
        candidates.truncate(batch_limit);
        candidates.into_iter().map(|(id, _, _)| id).collect()
    }

    /// P1/10 — rewrite every live record in `seg_ids` into a freshly
    /// allocated tail slot, returning `(bytes_freed, remap)`.
    ///
    /// `remap`: `(old_ptr, new_ptr)` pairs for every record moved.
    /// The caller uses this list to update external pointers — the
    /// only one outside the undo manager is `txn.undo_head`, handled
    /// via `Database::compact_undo_segments`.
    ///
    /// Referrers' `prev_undo_ptr` are NOT rewritten here (that was an
    /// O(total-records) scan). Instead `register_forwardings` installs the
    /// `old→new` mapping; any reader that follows `prev_undo_ptr`
    /// (the purge keep-set walk, `get`/`load_owned`) resolves the stale
    /// ptr to its relocated home through that table.
    ///
    /// Concurrency: must run with the outer undo write lock held —
    /// no concurrent push / get / load_owned. The records map IS
    /// mutated here.
    pub(crate) fn compact_segments(
        &mut self,
        seg_ids: &[u32],
    ) -> Result<(u64, Vec<(u64, u64)>), QueryError> {
        if seg_ids.is_empty() {
            return Ok((0, Vec::new()));
        }
        let active = self.protected_segment_ids();
        let mut remap: Vec<(u64, u64)> = Vec::new();
        // Snapshot the segment's old size so the caller can reason
        // about the disk bytes freed.
        let mut size_before: u64 = 0;
        for seg in seg_ids {
            if active.contains(seg) {
                continue;
            }
            size_before =
                size_before.saturating_add(self.segment_file_bytes.get(seg).copied().unwrap_or(0));
        }
        // Collect live record ptrs grouped by source segment.
        let mut by_segment: BTreeMap<u32, Vec<u64>> = BTreeMap::new();
        let mut sorted_seg_ids = seg_ids.to_vec();
        sorted_seg_ids.sort_unstable();
        sorted_seg_ids.dedup();
        for &ptr in self.records.keys() {
            let seg = undo_ptr_segment(ptr);
            if sorted_seg_ids.binary_search(&seg).is_ok() && !active.contains(&seg) {
                by_segment.entry(seg).or_default().push(ptr);
            }
        }
        let relocate = (|| -> Result<(), QueryError> {
            for (_, mut ptrs) in by_segment {
                ptrs.sort_unstable();
                for old_ptr in ptrs {
                    // Phase 2 — a candidate record may have had its payload
                    // evicted; page it back in from disk so the rewrite
                    // carries the full before-image rather than an empty shell.
                    let (was_resident, chain_depth) = match self.records.get(&old_ptr) {
                        Some(r) => (r.resident, r.chain_depth),
                        None => continue,
                    };
                    let old_rec = if was_resident {
                        match self.records.get(&old_ptr) {
                            Some(r) => r.clone(),
                            None => continue,
                        }
                    } else {
                        match self.load_owned(old_ptr) {
                            Some(r) => r,
                            None => continue,
                        }
                    };
                    let old_size = record_size(&old_rec);
                    // Relocation does not change logical chain depth. Preserve it
                    // in the frame even if an earlier predecessor has moved.
                    let rewritten = UndoRecord {
                        id: 0,
                        chain_depth,
                        ..old_rec.clone()
                    };
                    let bytes = encode_undo_record(&rewritten)?;
                    // append_record_preencoded distributes via round-robin
                    // shard pick and never re-uses the old segment (the
                    // candidate segment is not anyone's active segment).
                    let (new_ptr, _, _) = self.append_record_preencoded(rewritten, bytes, true)?;
                    self.records
                        .get_mut(&new_ptr)
                        .expect("relocated undo")
                        .chain_depth = chain_depth;
                    // The append above counted size_of(rewritten) into
                    // current_bytes; the original is still in records and
                    // also counted. Remove the original so accounting is
                    // not double-charged.
                    let removed = self
                        .records
                        .remove(&old_ptr)
                        .expect("relocated source undo");
                    self.eviction_candidates
                        .remove(&(old_rec.created_unix_ms, old_ptr));
                    self.current_bytes = self.current_bytes.saturating_sub(old_size);
                    self.resident_bytes =
                        self.resident_bytes.saturating_sub(resident_size(&removed));
                    self.release_resident_charge(resident_size(&removed));
                    self.debit_txn_bytes(old_rec.txn_id, old_size);
                    if let Some(ids) = self.created_index.get_mut(&old_rec.created_unix_ms) {
                        ids.remove(&old_ptr);
                        if ids.is_empty() {
                            self.created_index.remove(&old_rec.created_unix_ms);
                        }
                    }
                    let old_seg = undo_ptr_segment(old_ptr);
                    if old_seg != 0 {
                        if let Some(live) = self.segment_live_records.get_mut(&old_seg) {
                            *live = live.saturating_sub(1);
                        }
                    }
                    remap.push((old_ptr, new_ptr));
                }
            }
            Ok(())
        })();
        // Forward every stale ptr (heap-header / before_header MVCC
        // row-version chains AND referrers' un-rewritten `prev_undo_ptr`)
        // to the relocated home. Referrer chains are deliberately NOT
        // remapped in place — that was an O(total-records) write-lock scan;
        // the forwarding table is the single old→new authority and the
        // purge keep-set walk (`refresh_keep_cache`) resolves through it.
        // Kept identical to the build-then-swap path so this stays a valid
        // differential oracle.
        if !remap.is_empty() {
            self.register_forwardings(&remap);
            self.invalidate_keep_cache();
        }
        relocate?;
        self.persist_manifest()?;
        // Now the candidate segments should be at 0 live records;
        // detach_empty_segments drops them.
        let removable = self.detach_empty_segments();
        let removable_paths = removable.clone();
        Self::unlink_segments(&removable);
        // `reclaimed_bytes` was already bumped by detach_empty_segments
        // with the segment's recorded size; the size_before snapshot
        // is what the caller reports as "freed by compaction".
        let _ = removable_paths;
        // Invalidate the purge keep-set cache since prev_undo_ptr
        // pointers have shifted; the next plan run rebuilds it.
        self.invalidate_keep_cache();
        Ok((size_before, remap))
    }

    /// P1/10 build-then-swap, phase 1 — plan under the SHARED lock.
    ///
    /// Selects candidate segments, snapshots every live record to
    /// relocate (cloning resident payloads; recording disk coordinates
    /// for evicted ones so phase 2 pages them in off-lock from the
    /// immutable candidate file), and reserves the output segment-id
    /// lane. Returns `None` when there is nothing to compact or no
    /// segment directory. Read-only: no `records` mutation, so it holds
    /// only the shared undo lock and never blocks foreground appends for
    /// the segment rewrite itself.
    pub(crate) fn plan_compaction(
        &self,
        min_live_ratio_pct: u32,
        batch_limit: usize,
    ) -> Option<CompactionPlan> {
        let dir = self.undo_dir.as_ref()?.clone();
        let candidates = self.compaction_candidates(min_live_ratio_pct, batch_limit);
        if candidates.is_empty() {
            return None;
        }
        let mut size_before = 0u64;
        let mut moves: Vec<PlannedMove> = Vec::new();
        for &seg in &candidates {
            size_before =
                size_before.saturating_add(self.segment_file_bytes.get(&seg).copied().unwrap_or(0));
        }
        let mut sorted_candidates = candidates.clone();
        sorted_candidates.sort_unstable();
        for (ptr, rec) in &self.records {
            let seg = undo_ptr_segment(*ptr);
            if sorted_candidates.binary_search(&seg).is_ok() {
                let src_path = dir.join(segment_file_name(seg));
                moves.push(PlannedMove {
                    old_ptr: *ptr,
                    resident_record: if rec.resident {
                        Some(rec.clone())
                    } else {
                        None
                    },
                    src_offset: undo_ptr_offset(*ptr),
                    src_path,
                });
            }
        }
        moves.sort_unstable_by_key(|planned| planned.old_ptr);
        if moves.is_empty() {
            return None;
        }
        // Reserve the next free compaction-lane sequence. Output segment
        // ids never collide with foreground shards (distinct lane) and
        // land strictly above BOTH every lane-15 segment currently on disk
        // AND the monotonic high-water mark — so an id is never reused this
        // run even after a prior output segment was purged and detached
        // (a reused id could alias a stale forwarding entry).
        let disk_max = self
            .segment_file_bytes
            .keys()
            .filter(|id| shard_of_segment(**id) == COMPACTION_SHARD_LANE)
            .map(|id| id >> SHARD_BITS)
            .max()
            .unwrap_or(0);
        let base_seq =
            recovery::next_recovery_sequence(disk_max.max(self.compaction_seq_high)).ok()?;
        Some(CompactionPlan {
            candidates,
            size_before,
            moves,
            out_base_seq: base_seq,
            segment_bytes: self.segment_bytes,
            dir,
        })
    }

    /// P1/10 build-then-swap, phase 2 — build the replacement segment
    /// file(s) with NO lock held. Reads live before-images (resident
    /// clones from the plan, or paged in from the immutable candidate
    /// files), re-encodes them into freshly numbered lane-15 segments
    /// written to `.compacting` temp files, and `fsync`s each so the
    /// relocated frames are durable before the swap unlinks the
    /// originals. Returns the `(old_ptr → new_ptr)` moves plus the
    /// records to insert. On any error every temp file it created is
    /// unlinked before returning.
    pub(crate) fn build_compaction_segments(
        plan: &CompactionPlan,
    ) -> Result<BuiltCompaction, QueryError> {
        let mut built = BuiltCompaction::default();
        let result = Self::build_compaction_inner(plan, &mut built);
        if result.is_err() {
            for seg in &built.out_segments {
                let _ = fs::remove_file(&seg.tmp_path);
            }
        }
        result.map(|_| built)
    }

    fn build_compaction_inner(
        plan: &CompactionPlan,
        built: &mut BuiltCompaction,
    ) -> Result<(), QueryError> {
        let seg_bytes = plan.segment_bytes.max(UNDO_SEGMENT_HEADER_LEN * 2);
        let mut out_seq = plan.out_base_seq;
        let mut seg_id = make_segment_id(COMPACTION_SHARD_LANE, out_seq);
        let mut buf: Vec<u8> = Vec::new();
        write_segment_header_into(&mut buf, seg_id, &plan.dir)?;
        for mv in &plan.moves {
            // Resolve the before-image: resident clone or an off-lock
            // page-in from the immutable candidate file.
            let rec = match &mv.resident_record {
                Some(r) => r.clone(),
                None => match read_undo_frame_at(&mv.src_path, mv.src_offset)? {
                    Some(r) => r,
                    // Absent frame under an immutable candidate file means
                    // the plan raced a rewrite that cannot happen (candidate
                    // segments are frozen); treat as unrecoverable.
                    None => {
                        return Err(error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "undo compaction could not page in a candidate before-image",
                        ))
                    }
                },
            };
            // Canonicalize like the in-lock path: strip id (patched per
            // slot) and chain_depth (recomputed on insert). prev_undo_ptr
            // is left as-is on disk and remapped in-memory at commit,
            // matching `compact_segments`.
            let mut rewritten = UndoRecord {
                id: 0,
                chain_depth: 0,
                ..rec
            };
            let mut payload = encode_undo_record(&rewritten)?;
            let frame_len = UNDO_FRAME_HEADER_LEN as u64 + payload.len() as u64;
            // Roll to a new output segment when the current one is full.
            let cur_offset = buf.len() as u64;
            if cur_offset > UNDO_SEGMENT_HEADER_LEN
                && cur_offset.saturating_add(frame_len) > seg_bytes
            {
                Self::flush_compaction_segment(plan, seg_id, std::mem::take(&mut buf), built)?;
                out_seq = recovery::next_recovery_sequence(out_seq)?;
                seg_id = make_segment_id(COMPACTION_SHARD_LANE, out_seq);
                write_segment_header_into(&mut buf, seg_id, &plan.dir)?;
            }
            let new_ptr = make_undo_ptr(seg_id, buf.len() as u64)?;
            Self::patch_encoded_id(&mut payload, new_ptr)?;
            let header = undo_frame_header(&payload);
            buf.extend_from_slice(&header);
            buf.extend_from_slice(&payload);
            rewritten.id = new_ptr;
            rewritten.resident = true;
            built.moves.push((mv.old_ptr, new_ptr, rewritten));
        }
        if buf.len() as u64 > UNDO_SEGMENT_HEADER_LEN {
            Self::flush_compaction_segment(plan, seg_id, buf, built)?;
        }
        Ok(())
    }

    fn flush_compaction_segment(
        plan: &CompactionPlan,
        seg_id: u32,
        buf: Vec<u8>,
        built: &mut BuiltCompaction,
    ) -> Result<(), QueryError> {
        let final_path = plan.dir.join(segment_file_name(seg_id));
        let tmp_path = final_path.with_extension("seg.compacting");
        let file_len = buf.len() as u64;
        {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)
                .map_err(io_err)?;
            file.write_all(&buf).map_err(io_err)?;
            // Durable before the swap drops the originals: a crash after
            // this fsync but before the swap leaves an orphan `.compacting`
            // file that recovery ignores (it only scans `*.seg`).
            crate::platform::fs::sync_file_at(&file, &tmp_path).map_err(io_err)?;
        }
        built.out_segments.push(BuiltOutSegment {
            seg_id,
            tmp_path,
            final_path,
            file_len,
        });
        Ok(())
    }

    /// P1/10 build-then-swap, phase 3 — atomically install the built
    /// segments under the WRITE lock. Holds the lock only for pointer /
    /// map surgery (no file rewrite, no re-encode), so foreground writes
    /// stall for microseconds instead of the full segment rewrite.
    ///
    /// Drift guard: the off-lock build window admits concurrent purge /
    /// rollback that could remove a planned record. Undo records are
    /// immutable and only ever removed, so if any planned `old_ptr` is
    /// gone we abort the whole swap (unlink temp files, return 0) and let
    /// the next tick re-plan against a fresh snapshot — never a partial,
    /// chain-breaking application.
    #[cfg(test)]
    pub(crate) fn commit_compaction(
        &mut self,
        plan: CompactionPlan,
        built: BuiltCompaction,
    ) -> Result<(u64, Vec<(u64, u64)>), QueryError> {
        self.commit_compaction_logged(plan, built, |_| Ok(()))
    }

    /// Publish the replacement closure to WAL before exposing relocated pointers.
    /// A single physical batch makes every possible source target atomic.
    pub(crate) fn commit_compaction_logged(
        &mut self,
        plan: CompactionPlan,
        built: BuiltCompaction,
        publish: impl FnOnce(Vec<crate::wal::WalPayload>) -> Result<(), QueryError>,
    ) -> Result<(u64, Vec<(u64, u64)>), QueryError> {
        if built.moves.is_empty() {
            for seg in &built.out_segments {
                let _ = fs::remove_file(&seg.tmp_path);
            }
            return Ok((0, Vec::new()));
        }
        // Drift check — every relocated original must still be present.
        let drift = built
            .moves
            .iter()
            .any(|(old_ptr, _, _)| !self.records.contains_key(old_ptr));
        if drift {
            for seg in &built.out_segments {
                let _ = fs::remove_file(&seg.tmp_path);
            }
            return Ok((0, Vec::new()));
        }
        // Reserve every output directory entry before publishing files or
        // removing originals. This makes the swap fail atomically under memory
        // pressure and temporarily double-charges the small relocation batch.
        let reservations = built
            .moves
            .iter()
            .map(|(_, _, rec)| self.reserve_record_resident(rec))
            .collect::<Result<Vec<_>, _>>()?;
        if self.archive_floor.load(Ordering::Acquire) != 0
            || crate::repl::physical_replication_active()
        {
            let mut charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                0,
            )?;
            let mut payloads = Vec::new();
            for output in &built.out_segments {
                charge.try_grow(
                    usize::try_from(output.file_len)
                        .map_err(|_| io_err(std::io::Error::other("undo relocation too large")))?
                        .saturating_mul(3),
                )?;
                payloads.push(crate::wal::WalPayload::FileCreate {
                    path: format!("system/undo/{}", segment_file_name(output.seg_id)),
                    after: fs::read(&output.tmp_path).map_err(io_err)?,
                });
            }
            let active: Vec<u32> = self
                .write_shards
                .iter()
                .map(|s| s.lock().active_segment_id)
                .collect();
            let mut forwarded = self.compaction_forwarded.clone();
            let mut compensated = self.compensated.clone();
            for (old, new, _) in &built.moves {
                forwarded.insert(*old, *new);
                if compensated.contains(old) {
                    compensated.insert(*new);
                }
            }
            let manifest = UndoManifest {
                pending_recovery_segments: Vec::new(),
                compaction_seq_high: built
                    .out_segments
                    .iter()
                    .map(|s| s.seg_id >> SHARD_BITS)
                    .max()
                    .unwrap_or(self.compaction_seq_high)
                    .max(self.compaction_seq_high),
                version: UNDO_TABLESPACE_VERSION,
                storage_uuid: undo_storage_uuid(self.undo_dir.as_ref().unwrap())?,
                segment_bytes: self.segment_bytes,
                active_segment_id: active.first().copied().unwrap_or(1),
                shard_active_segments: active,
                compensated,
                discarded: self.discarded.clone(),
                forwardings: forwarded,
            };
            payloads.push(crate::wal::WalPayload::FileCreate {
                path: "system/undo/undo.manifest".into(),
                after: encode_undo_manifest(&manifest)?,
            });
            publish(payloads)?;
        }
        // WAL is durable before a future writer can reference a new address.
        for seg in &built.out_segments {
            fs::rename(&seg.tmp_path, &seg.final_path).map_err(io_err)?;
            self.segment_file_bytes.insert(seg.seg_id, seg.file_len);
            // Advance the monotonic high-water mark so this id is never
            // reused even after the segment is later purged + detached.
            self.compaction_seq_high = self.compaction_seq_high.max(seg.seg_id >> SHARD_BITS);
        }
        let mut remap: Vec<(u64, u64)> = Vec::with_capacity(built.moves.len());
        for ((old_ptr, new_ptr, rec), reservation) in
            built.moves.into_iter().zip(reservations.into_iter())
        {
            // Remove the original with the same accounting as the in-lock
            // `compact_segments` path.
            if let Some(old) = self.records.remove(&old_ptr) {
                self.eviction_candidates
                    .remove(&(old.created_unix_ms, old_ptr));
                let old_size = record_size(&old);
                self.current_bytes = self.current_bytes.saturating_sub(old_size);
                self.resident_bytes = self.resident_bytes.saturating_sub(resident_size(&old));
                self.release_resident_charge(resident_size(&old));
                self.debit_txn_bytes(old.txn_id, old_size);
                if let Some(ids) = self.created_index.get_mut(&old.created_unix_ms) {
                    ids.remove(&old_ptr);
                    if ids.is_empty() {
                        self.created_index.remove(&old.created_unix_ms);
                    }
                }
                let old_seg = undo_ptr_segment(old_ptr);
                if old_seg != 0 {
                    if let Some(live) = self.segment_live_records.get_mut(&old_seg) {
                        *live = live.saturating_sub(1);
                    }
                }
            }
            // Insert the relocated copy (bumps segment_live_records for the
            // output segment, current/resident/txn bytes, created_index).
            self.insert_loaded_record_reserved(rec, reservation)?;
            remap.push((old_ptr, new_ptr));
        }
        // Record the relocation so EVERY stale ptr (the MVCC row-version
        // chain in heap headers / before_header, AND referrers'
        // `prev_undo_ptr` which we intentionally leave un-rewritten) still
        // resolves to the record's relocated home; then prune entries whose
        // target is no longer live. P1/10 note: the referrer chains are NOT
        // remapped in place here — that used to be an O(total-records) scan
        // under the write lock. The forwarding table is now the single
        // authority for old→new, and the one hot reader that walks
        // `prev_undo_ptr` (the purge keep-set in `refresh_keep_cache`) is
        // forwarding-aware, so the swap stays O(relocated), not O(total).
        self.register_forwardings(&remap);
        self.persist_manifest()?;
        // Every candidate's live records were relocated, so each must now
        // be at zero live — `detach_empty_segments` then drops them. (An
        // active candidate can't happen: candidates are non-active by
        // selection and lane-15 outputs are never active.)
        debug_assert!(
            plan.candidates.iter().all(|seg| self
                .segment_live_records
                .get(seg)
                .copied()
                .unwrap_or(0)
                == 0),
            "build-then-swap left a candidate segment with live records"
        );
        // Drop now-empty candidate segments and their read handles.
        let removable = self.detach_empty_segments();
        Self::unlink_segments(&removable);
        self.invalidate_keep_cache();
        self.disk_bytes = self
            .segment_file_bytes
            .values()
            .copied()
            .fold(0u64, u64::saturating_add);
        Ok((plan.size_before, remap))
    }

    /// Remove fully-dead, non-active segments from the in-memory
    /// bookkeeping and return their paths for out-of-lock deletion.
    fn detach_empty_segments(&mut self) -> Vec<PathBuf> {
        let Some(dir) = self.undo_dir.as_ref() else {
            return Vec::new();
        };
        let active = self.protected_segment_ids();
        let removable = self
            .segment_file_bytes
            .keys()
            .copied()
            .filter(|id| !active.contains(id))
            .filter(|id| self.segment_live_records.get(id).copied().unwrap_or(0) == 0)
            .collect::<Vec<_>>();
        let mut paths = Vec::with_capacity(removable.len());
        for id in removable {
            self.segment_pins.lock().pins.remove(&id);
            let bytes = self.segment_file_bytes.remove(&id).unwrap_or(0);
            self.segment_live_records.remove(&id);
            if let Some(shard) = self.write_shards.get(shard_of_segment(id) as usize) {
                let mut shard = shard.lock();
                shard.pending_sync.remove(&id);
                if let Some(syncing) = shard.syncing_segments.as_mut() {
                    syncing.remove(&id);
                }
            }
            self.reclaimed_bytes = self.reclaimed_bytes.saturating_add(bytes);
            // Phase 2 — drop any cached read handle for the file we are
            // about to unlink so no stale fd lingers.
            self.invalidate_segment_reader(id);
            paths.push(dir.join(segment_file_name(id)));
        }
        if !paths.is_empty() {
            self.disk_bytes = self
                .segment_file_bytes
                .values()
                .copied()
                .fold(0u64, u64::saturating_add);
        }
        self.compaction_origins
            .retain(|ptr, _| self.records.contains_key(ptr));
        self.compensated.retain(|ptr| {
            let segment = undo_ptr_segment(*ptr);
            if segment == 0 {
                self.records.contains_key(ptr)
            } else {
                self.segment_file_bytes
                    .get(&segment)
                    .is_some_and(|len| undo_ptr_offset(*ptr) < *len)
            }
        });
        paths
    }

    /// Reclaim segments made empty by startup filtering. Non-active segments
    /// can be unlinked outright. The newest segment in each write shard stays
    /// as that shard's allocation anchor, but when it has no retained records
    /// its obsolete frames are truncated back to the durable 32-byte header.
    fn reclaim_empty_segments_after_recovery(&mut self) -> Result<(), QueryError> {
        #[cfg(target_os = "macos")]
        let mut sync_batch = crate::platform::fs::DeviceSyncBatch::new();
        let active = self.active_segment_ids();
        for segment_id in active {
            if self
                .segment_live_records
                .get(&segment_id)
                .copied()
                .unwrap_or(0)
                != 0
            {
                continue;
            }
            let Some(old_len) = self.segment_file_bytes.get(&segment_id).copied() else {
                continue;
            };
            if old_len <= UNDO_SEGMENT_HEADER_LEN {
                continue;
            }
            let path = self.segment_path(segment_id)?;
            let file = OpenOptions::new().write(true).open(&path).map_err(io_err)?;
            file.set_len(UNDO_SEGMENT_HEADER_LEN).map_err(io_err)?;
            #[cfg(target_os = "macos")]
            sync_batch.sync_file(file, &path).map_err(io_err)?;
            #[cfg(not(target_os = "macos"))]
            crate::platform::fs::sync_file_at(&file, &path).map_err(io_err)?;
            self.segment_file_bytes
                .insert(segment_id, UNDO_SEGMENT_HEADER_LEN);
            self.reclaimed_bytes = self
                .reclaimed_bytes
                .saturating_add(old_len.saturating_sub(UNDO_SEGMENT_HEADER_LEN));
            if let Some(shard) = self.write_shards.get(shard_of_segment(segment_id) as usize) {
                let mut shard = shard.lock();
                if shard.active_segment_id == segment_id {
                    shard.active_offset = UNDO_SEGMENT_HEADER_LEN;
                    shard.buffer_start = UNDO_SEGMENT_HEADER_LEN;
                    shard.tail_buffer.clear();
                }
            }
        }
        #[cfg(target_os = "macos")]
        sync_batch.finish().map_err(io_err)?;
        let removable = self.detach_empty_segments();
        Self::unlink_segments(&removable);
        self.disk_bytes = self
            .segment_file_bytes
            .values()
            .copied()
            .fold(0u64, u64::saturating_add);
        Ok(())
    }

    /// Unlink detached segment files; safe to call without the undo lock.
    pub(crate) fn unlink_segments(paths: &[PathBuf]) {
        for path in paths {
            let _ = fs::remove_file(path);
        }
    }

    pub(crate) fn checkpoint_write_shards(&self) -> Arc<[UndoWriteShardHandle]> {
        Arc::clone(&self.write_shards)
    }

    pub(crate) fn checkpoint_sync_manifest(&mut self) -> Result<(), QueryError> {
        self.compensation_manifest_dirty
            .store(true, Ordering::Release);
        self.persist_manifest()
    }

    /// Test entry point; Database releases its undo latch before shard fsync.
    #[cfg(test)]
    fn checkpoint_sync(&mut self) -> Result<(), QueryError> {
        for shard in self.write_shards.iter() {
            UndoWriteShard::checkpoint_sync(shard)?;
        }
        self.checkpoint_sync_manifest()
    }

    pub(crate) fn flush(&mut self) -> Result<(), QueryError> {
        for shard_mu in self.write_shards.iter() {
            shard_mu.lock().flush_all()?;
        }
        self.persist_manifest()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.records.len()
    }

    #[cfg(test)]
    fn resident_charge_bytes(&self) -> usize {
        self.resident_charge.lock().bytes()
    }
}

#[cfg(target_os = "macos")]
#[derive(Debug, PartialEq, Eq)]
struct ManifestSyncSnapshot {
    digest: [u8; 32],
    directory_device: u64,
    directory_inode: u64,
    namespace_digest: [u8; 32],
    #[cfg(test)]
    fixture_epoch: u64,
}

#[cfg(target_os = "macos")]
fn manifest_metadata_signature(metadata: &fs::Metadata) -> [u64; 10] {
    use std::os::unix::fs::MetadataExt;
    [
        metadata.dev(),
        metadata.ino(),
        metadata.len(),
        metadata.mode() as u64,
        metadata.uid() as u64,
        metadata.gid() as u64,
        metadata.mtime() as u64,
        metadata.mtime_nsec() as u64,
        metadata.ctime() as u64,
        metadata.ctime_nsec() as u64,
    ]
}

#[cfg(target_os = "macos")]
fn manifest_sync_snapshot(path: &Path, bytes: &[u8]) -> Option<ManifestSyncSnapshot> {
    use sha2::Digest;
    use std::os::unix::fs::{DirEntryExt, MetadataExt};
    #[cfg(test)]
    let fixture_epoch = crate::platform::fs::fixture_sync_epoch()?;
    let directory = path.parent()?;
    let metadata = fs::metadata(directory).ok()?;
    let mut entries = Vec::new();
    for entry in fs::read_dir(directory).ok()? {
        let entry = entry.ok()?;
        // Bound probe work and temporary memory. Temporary compaction outputs,
        // links and unfamiliar entries use the normal unconditional barrier.
        if entries.len() == 64 || !entry.file_type().ok()?.is_file() {
            return None;
        }
        let name = entry.file_name();
        let text = name.to_str()?;
        if text != "undo.manifest" && parse_segment_name(text).is_none() {
            return None;
        }
        entries.push((name.into_string().ok()?, entry.ino()));
    }
    let directory_state = manifest_metadata_signature(&metadata);
    if directory_state != manifest_metadata_signature(&fs::metadata(directory).ok()?) {
        return None;
    }
    entries.sort_unstable();
    let mut namespace = sha2::Sha256::new();
    for word in directory_state {
        namespace.update(word.to_le_bytes());
    }
    namespace.update((entries.len() as u64).to_le_bytes());
    for (name, inode) in entries {
        namespace.update((name.len() as u64).to_le_bytes());
        namespace.update(name.as_bytes());
        namespace.update(inode.to_le_bytes());
    }
    let mut content = sha2::Sha256::new();
    content.update(bytes);
    for word in manifest_metadata_signature(&fs::metadata(path).ok()?) {
        content.update(word.to_le_bytes());
    }
    Some(ManifestSyncSnapshot {
        digest: content.finalize().into(),
        directory_device: metadata.dev(),
        directory_inode: metadata.ino(),
        namespace_digest: namespace.finalize().into(),
        #[cfg(test)]
        fixture_epoch,
    })
}

#[cfg(all(test, target_os = "macos"))]
thread_local! {
    static FAIL_NEXT_MANIFEST_DIRECTORY_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

// Reusing an identical manifest avoids metadata writes on clean startup and
// checkpoint. Read in bounded chunks and reject links; a failed comparison
// falls back to the normal atomic replacement, never to an unsynced success.
fn manifest_contents_match(path: &Path, expected: &[u8]) -> bool {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return false;
    };
    if !metadata.is_file()
        || crate::platform::fs::is_link(&metadata)
        || metadata.len() != expected.len() as u64
    {
        return false;
    }
    let Ok(mut file) = File::open(path) else {
        return false;
    };
    let mut buffer = [0u8; 8192];
    for chunk in expected.chunks(buffer.len()) {
        if file.read_exact(&mut buffer[..chunk.len()]).is_err() || &buffer[..chunk.len()] != chunk {
            return false;
        }
    }
    matches!(file.read(&mut buffer[..1]), Ok(0))
}

#[cfg(test)]
mod purge_tests {
    use super::*;

    #[test]
    fn segment_reader_cache_evicts_lru_not_whole_cache() {
        // One real file shared by every logical entry — we exercise the LRU
        // bookkeeping, not the files themselves.
        // Use the same shareable handles as persistent undo readers. Windows
        // anonymous tempfiles are exclusive and cannot be reopened for I/O.
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(dir.path().join("reader-cache"))
            .unwrap();
        let file = std::sync::Arc::new(file);
        let mut cache = SegmentReaderCache::default();
        // Fill exactly to the cap.
        for id in 1..=(UNDO_SEGMENT_READER_CACHE_CAP as u32) {
            cache.insert(id, file.clone());
        }
        assert_eq!(cache.map.len(), UNDO_SEGMENT_READER_CACHE_CAP);
        assert_eq!(cache.evictions, 0);
        // Touch id 1 so it becomes most-recently-used; id 2 is now the LRU.
        assert!(cache.get(1).is_some());
        // One more distinct segment forces a single eviction — the LRU
        // victim (id 2), NOT the whole cache (the old `clear()` behavior
        // would have dropped the just-touched id 1 as well).
        let overflow = UNDO_SEGMENT_READER_CACHE_CAP as u32 + 1;
        cache.insert(overflow, file.clone());
        assert_eq!(cache.map.len(), UNDO_SEGMENT_READER_CACHE_CAP);
        assert_eq!(cache.evictions, 1);
        assert!(cache.map.contains_key(&1), "hot handle must survive");
        assert!(!cache.map.contains_key(&2), "LRU victim must be evicted");
        assert!(cache.map.contains_key(&overflow));
        // Re-inserting an existing id does not evict.
        cache.insert(1, file.clone());
        assert_eq!(cache.evictions, 1);
        assert_eq!(cache.map.len(), UNDO_SEGMENT_READER_CACHE_CAP);
    }

    pub(super) fn make_record(
        id: u64,
        txn_id: u64,
        created_ms: u64,
        prev: Option<u64>,
    ) -> UndoRecord {
        let _ = txn_id;
        UndoRecord {
            id,
            txn_id,
            created_unix_ms: created_ms,
            prev_undo_ptr: prev,
            kind: UndoKind::Insert,
            table_id: 1,
            row_id: RowId {
                page_id: 0,
                slot_id: 0,
            },
            before_header: None,
            before_values: None,
            before_external: Vec::new(),
            borrowed_external: Vec::new(),
            chain_depth: 0,
            cached_size: 0,
            resident_size: 0,
            temp_reservation: None,
            resident: true,
            encoded_len: 0,
        }
    }

    #[test]
    fn resident_admission_is_independent_of_directory_latch_and_releases_on_drop() {
        let undo = Arc::new(parking_lot::RwLock::new(UndoManager::default()));
        let admission = undo.read().resident_admission();
        let held = undo.write();
        let record = make_record(1, 7, 1, None);
        let expected = estimate_undo_resident_bytes(&record) as usize;
        let (tx, rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || tx.send(admission.reserve(&record)).unwrap());
        let reserved = rx.recv_timeout(std::time::Duration::from_secs(1));
        drop(held);
        worker.join().unwrap();
        let reservation = reserved
            .expect("admission waited for undo metadata")
            .unwrap();
        assert_eq!(undo.read().resident_charge_bytes(), expected);
        drop(reservation);
        assert_eq!(undo.read().resident_charge_bytes(), 0);
    }

    #[test]
    fn insert_admission_shared_probe_matches_exclusive_policy() {
        for rows in [0, 1, 17, usize::MAX] {
            for persistent in [false, true] {
                for directory in [false, true] {
                    for cap in [0, 64, 1024, u64::MAX] {
                        for guarantee in [false, true] {
                            let mut undo = UndoManager::default();
                            undo.current_bytes = 47;
                            undo.disk_bytes = 53;
                            undo.mem_soft_limit_bytes = cap;
                            undo.tablespace_max_bytes = cap;
                            undo.retention_guarantee = guarantee;
                            if directory {
                                undo.undo_dir = Some(PathBuf::from("/tmp/mydb/admission-probe"));
                            }
                            let unpressured = undo.insert_admission_unpressured(rows, persistent);
                            assert_eq!(undo.mem_pressure_count, 0);
                            assert_eq!(undo.retention_pressure_count, 0);
                            let result = undo.check_insert_admission(rows, persistent);
                            assert_eq!(unpressured, matches!(result, Ok(false)));
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn purge_charge_batch_preserves_unpublished_reservations() {
        for replica in [false, true] {
            let mut undo = UndoManager::default();
            let mut first = make_record(1, 7, 1, None);
            first.before_values = Some(vec![Value::String("wide".repeat(256))]);
            undo.insert(first, false, false).unwrap();
            undo.insert(make_record(2, 8, 1, None), false, false)
                .unwrap();
            let pending = make_record(3, 9, 1, None);
            let reservation = undo.reserve_record_resident(&pending).unwrap();
            let reserved_bytes = estimate_undo_resident_bytes(&pending) as usize;
            if replica {
                undo.evict_replica_records(&[1, 1, 999]);
            } else {
                undo.apply_purge(&[1, 1, 999], &BTreeMap::new());
            }
            assert_eq!(undo.record_count(), 1);
            assert_eq!(
                undo.resident_charge_bytes(),
                undo.resident_bytes() as usize + reserved_bytes
            );
            drop(reservation);
            assert_eq!(undo.resident_charge_bytes(), undo.resident_bytes() as usize);
            undo.remove_record(2).unwrap();
            assert_eq!(undo.resident_charge_bytes(), 0);
        }
    }

    #[test]
    fn resident_governor_charge_tracks_insert_and_remove() {
        let mut undo = UndoManager::default();
        assert_eq!(undo.resident_charge_bytes(), 0);
        let record = make_record(1, 7, 1, None);
        undo.insert(record, false, false).unwrap();
        assert_eq!(
            undo.resident_charge_bytes(),
            usize::try_from(undo.resident_bytes()).unwrap()
        );
        undo.remove_record(1).unwrap();
        assert_eq!(undo.resident_charge_bytes(), 0);
        assert_eq!(undo.resident_bytes(), 0);
    }

    #[test]
    fn mem_soft_limit_flags_pressure_for_immediate_purge() {
        // P0/B2: the in-memory `records` mirror soft cap. Crossing it makes
        // admission return `pressure = true`, which the caller turns into an
        // immediate `purge_obsolete_undo()`, and bumps the observability
        // counter. Default (0) keeps the guard disabled.
        let mut undo = UndoTablespace::default();
        assert!(!undo.check_insert_admission(1, false).unwrap());
        assert_eq!(undo.mem_pressure_count, 0);

        undo.insert(make_record(1, 10, 1, None), false, false)
            .unwrap();
        let used = undo.bytes();
        assert!(used > 0, "insert must grow the in-memory mirror");

        // Cap far below the current mirror → next admission flags pressure.
        undo.set_mem_soft_limit_bytes(1);
        assert!(
            undo.check_insert_admission(1, false).unwrap(),
            "over the in-memory soft cap must signal pressure"
        );
        assert!(undo.mem_pressure_count >= 1);

        // Raising the cap above the mirror clears pressure (no double-count).
        undo.set_mem_soft_limit_bytes(used.saturating_add(1 << 20));
        let before = undo.mem_pressure_count;
        assert!(!undo.check_insert_admission(1, false).unwrap());
        assert_eq!(undo.mem_pressure_count, before);
    }

    // P0/5 — InsertBatch admission must scale with row_count using the
    // rowid-6bytes encoding (NOT the legacy 8-byte estimate). Pre-fix
    // estimate was 96 + N*8 and was about 50% high for wide batches.
    // The new formula is INSERT_UNDO_PAYLOAD_BASE + N *
    // INSERT_UNDO_PER_ROW_BYTES, tracking the actual on-disk frame.
    #[test]
    fn insert_admission_estimate_tracks_rowid_6bytes_encoding() {
        let mut undo = UndoTablespace::default();
        // Set the disk cap such that the OLD (96 + 8N) formula would
        // pass an N=64 batch (96 + 512 = 608) but the NEW formula
        // (53 + 6*64 = 437) leaves clear headroom. Choose 600 so we
        // can tell them apart.
        undo.set_tablespace_max_bytes(600);
        // Persistent admission queries are short-circuited when the
        // undo_dir is None (non-persistent path skips frame-header
        // addition), so use persistent=false here — `tablespace_max_bytes`
        // is checked against `disk_bytes` which is 0 in this harness.
        // Instead, exercise the mem_soft_limit pressure path.
        undo.set_mem_soft_limit_bytes(500);
        // Old formula 96+64*8=608 would have flagged pressure; new
        // formula 53+64*6=437 stays under the 500 cap.
        let pressure = undo.check_insert_admission(64, false).unwrap();
        assert!(!pressure, "refined estimate should fit under cap");
        // A clearly over-budget batch still flags pressure.
        let pressure_big = undo.check_insert_admission(1000, false).unwrap();
        assert!(pressure_big, "1000-row batch must cross the soft cap");
    }

    // P0/3 — keep-set cache: walking the same txn head twice in a row
    // touches the inner `records` map only the first time (the second
    // call short-circuits via head_per_txn). We can't observe walk
    // count directly, so we assert via the cache snapshot.
    #[test]
    fn keep_cache_walks_only_delta_for_repeated_head() {
        let mut undo = UndoTablespace::default();
        // Build a chain: u1 → u2 → u3, all owned by txn 7.
        undo.insert(make_record(1, 7, 1, None), false, false)
            .unwrap();
        undo.insert(make_record(2, 7, 1, Some(1)), false, false)
            .unwrap();
        undo.insert(make_record(3, 7, 1, Some(2)), false, false)
            .unwrap();
        // First plan run: walk from head=3 back to root. flat_keep ← {1,2,3}.
        let _ = undo.plan_purge_victims(&[(7, 3)], 0, None, &BTreeMap::new(), 0, 0);
        let snap = undo.keep_cache.lock();
        assert_eq!(snap.flat_keep.len(), 3);
        assert_eq!(snap.head_per_txn.get(&7), Some(&3));
        let baseline_chain = snap.chain_per_txn.get(&7).cloned().unwrap_or_default();
        drop(snap);
        // Second call with the same head: cache hit, no growth.
        let _ = undo.plan_purge_victims(&[(7, 3)], 0, None, &BTreeMap::new(), 0, 0);
        let snap = undo.keep_cache.lock();
        assert_eq!(snap.flat_keep.len(), 3);
        assert_eq!(snap.chain_per_txn.get(&7).unwrap(), &baseline_chain);
    }

    // P0/3 — when a txn finalizes (drops from active list), its chain
    // must be removed from the cache wholesale so the next purge call
    // sees the records as candidates.
    #[test]
    fn keep_cache_drops_finalized_txn_chain() {
        let mut undo = UndoTablespace::default();
        undo.insert(make_record(1, 7, 1, None), false, false)
            .unwrap();
        undo.insert(make_record(2, 7, 1, Some(1)), false, false)
            .unwrap();
        let _ = undo.plan_purge_victims(&[(7, 2)], 0, None, &BTreeMap::new(), 0, 0);
        assert_eq!(undo.keep_cache.lock().flat_keep.len(), 2);
        // Txn 7 finalized — no longer in active set. flat_keep clears.
        let _ = undo.plan_purge_victims(&[], 0, None, &BTreeMap::new(), 0, 0);
        let snap = undo.keep_cache.lock();
        assert!(snap.flat_keep.is_empty());
        assert!(snap.chain_per_txn.is_empty());
        assert!(snap.head_per_txn.is_empty());
    }

    // P0/3 — head advancing only walks the delta. Build a 3-node chain,
    // run the cache at head=2, then push a new tip 4 → 3 and assert
    // the second walk only added ptr 4 (it stops at the cached head).
    #[test]
    fn keep_cache_walks_only_new_records_when_head_advances() {
        let mut undo = UndoTablespace::default();
        undo.insert(make_record(1, 7, 1, None), false, false)
            .unwrap();
        undo.insert(make_record(2, 7, 1, Some(1)), false, false)
            .unwrap();
        let _ = undo.plan_purge_victims(&[(7, 2)], 0, None, &BTreeMap::new(), 0, 0);
        let first = undo.keep_cache.lock().flat_keep.clone();
        assert_eq!(first.len(), 2);
        // Push two more records, chain still owned by txn 7.
        undo.insert(make_record(3, 7, 1, Some(2)), false, false)
            .unwrap();
        undo.insert(make_record(4, 7, 1, Some(3)), false, false)
            .unwrap();
        let _ = undo.plan_purge_victims(&[(7, 4)], 0, None, &BTreeMap::new(), 0, 0);
        let after = undo.keep_cache.lock().flat_keep.clone();
        assert!(after.is_superset(&first));
        let added: HashSet<u64> = after.difference(&first).copied().collect();
        assert_eq!(added, [3u64, 4u64].into_iter().collect::<HashSet<u64>>());
    }

    // P1/7 — open_with_segments must produce the same records / disk
    // bytes when fan-out across multiple segment files, exercising
    // the parallel decode + sequential apply path.
    #[test]
    fn open_with_segments_parallel_decode_handles_many_segment_files() {
        let dir = tempfile::tempdir().unwrap();
        // 8 segments × 4 records each — exercise the parallel path
        // (ids.len() > 1) and the sort-by-segment_id ordering on apply.
        let mut undo = UndoTablespace::open_with_segments(&[], dir.path(), 384).unwrap();
        let mut expected_ids = Vec::new();
        for _ in 0..32 {
            let r = make_record(0, 1, 1, None);
            let (id, _, _) = undo.append_record(r, true).unwrap();
            expected_ids.push(id);
        }
        undo.flush().unwrap();
        let segment_count = undo.segment_count();
        assert!(
            segment_count >= 2,
            "test must produce at least 2 segments to cover the parallel path; got {segment_count}"
        );
        drop(undo);
        let reopened = UndoTablespace::open_with_segments(&[], dir.path(), 384).unwrap();
        assert_eq!(reopened.len(), expected_ids.len());
        for id in &expected_ids {
            assert!(reopened.records.contains_key(id));
        }
        assert_eq!(reopened.segment_count(), segment_count);
    }

    // P2/15 — temporal / decimal-compact accounting now uses native
    // on-disk sizes, not their ASCII display length. The estimate
    // should never exceed the actual encoded payload by more than a
    // small fixed envelope (record-level fields + value tags).
    #[test]
    fn estimate_undo_record_bytes_tracks_native_temporal_encoding() {
        let mut rec = make_record(0, 1, 1, None);
        rec.kind = UndoKind::Update;
        rec.before_header = Some(RowHeader {
            flags: 0,
            last_txn_id: 1,
            undo_ptr: None,
        });
        // Mix of temporal / decimal-compact / year — all paths that
        // used to be charged via to_display_string.
        rec.before_values = Some(vec![
            Value::DecimalCompact(123_456_789, 4),
            Value::Date(20_240_315),
            Value::Time(12_345_678),
            Value::DateTime(1_710_000_000_000),
            Value::Year(124),
        ]);
        let estimate = estimate_undo_record_bytes(&rec);
        let actual = encode_undo_record(&rec).unwrap().len() as u64;
        // The estimate is allowed to overshoot the on-disk payload by
        // up to 64 bytes (varint headers + bitmap + small slop). Pre-fix,
        // the gap was easily 50-100 bytes for a 5-value row of these
        // shapes.
        assert!(
            actual >= estimate.saturating_sub(64),
            "estimate {estimate} far above actual {actual}; size-accounting drift returned"
        );
        assert!(
            estimate >= actual.saturating_sub(64),
            "estimate {estimate} far below actual {actual}; under-accounting risks cap blow-through"
        );
    }

    // P2/14 — every persistent append should bump write_frame_latency
    // counts and a flush-inducing write should bump flush_latency. The
    // bucket-level approximations stay sane: writing N small frames
    // should land them in the lowest bucket.
    #[test]
    fn io_latency_histogram_records_write_and_flush() {
        let dir = tempfile::tempdir().unwrap();
        let mut undo =
            UndoTablespace::open_with_segments(&[], dir.path(), DEFAULT_UNDO_SEGMENT_BYTES)
                .unwrap();
        // Tiny buffer so a few appends force a flush.
        undo.set_buffer_limit(256).unwrap();
        assert_eq!(undo.write_frame_latency_samples(), 0);
        assert_eq!(undo.flush_latency_samples(), 0);
        for _ in 0..50 {
            undo.append_record(make_record(0, 1, 1, None), true)
                .unwrap();
        }
        undo.flush().unwrap();
        assert!(
            undo.write_frame_latency_samples() >= 50,
            "every append records once"
        );
        assert!(
            undo.flush_latency_samples() >= 1,
            "at least one buffer flush"
        );
        // Buffered append < 1us in the common case but the dev machine
        // can dip into 10us; assert the bucket is at most 100us.
        let p50 = undo.write_frame_latency_percentile(50);
        assert!(
            p50 <= 100_000,
            "buffered append p50 should be in the <100us bucket; got {p50}ns"
        );
    }

    // P0/1 — load_owned must fall back to the segment file when the
    // in-memory mirror entry is missing. Synthesize an "evicted" state
    // by manually removing the record from `records` after appending,
    // then assert that load_owned re-reads it from disk with all
    // fields intact.
    #[test]
    fn load_owned_pages_in_from_segment_on_in_memory_miss() {
        let dir = tempfile::tempdir().unwrap();
        let mut undo =
            UndoTablespace::open_with_segments(&[], dir.path(), DEFAULT_UNDO_SEGMENT_BYTES)
                .unwrap();
        undo.set_buffer_limit(0).unwrap(); // force write-through so the frame hits disk.
        let mut rec = make_record(0, 99, 42, None);
        rec.kind = UndoKind::Update;
        rec.before_header = Some(RowHeader {
            flags: 0,
            last_txn_id: 99,
            undo_ptr: None,
        });
        rec.before_values = Some(vec![Value::Int(5), Value::String("v0".into())]);
        let (id, _, _) = undo.append_record(rec.clone(), true).unwrap();

        // Hit path: load_owned returns the in-memory clone.
        let from_mem = undo.load_owned(id).expect("hit");
        assert_eq!(from_mem.txn_id, 99);
        assert_eq!(from_mem.created_unix_ms, 42);
        let cache_misses_before = undo.cache_misses();

        // Synthesize Phase 2 eviction: keep the record's metadata in the
        // directory but flip it non-resident and drop the heavy payload
        // (the segment file still holds the frame). An *absent* id would
        // mean purged — load_owned must page in only for evicted records.
        let evicted_values = rec.before_values.clone();
        {
            let entry = undo.records.get_mut(&id).expect("seeded record");
            entry.resident = false;
            entry.before_values = None;
        }
        let from_disk = undo.load_owned(id).expect("page-in must recover");
        assert_eq!(from_disk.txn_id, 99);
        assert_eq!(from_disk.created_unix_ms, 42);
        assert_eq!(from_disk.before_values, evicted_values);
        assert!(undo.cache_misses() > cache_misses_before);

        // A purged (absent) id must NOT be served from a lingering frame.
        undo.records.remove(&id);
        assert!(
            undo.load_owned(id).is_none(),
            "purged record must never page in from a stale frame"
        );
    }

    // Phase 3 — recovery must bound resident memory when a cache cap is
    // configured: a larger-than-cap on-disk tablespace reopens with only
    // the metadata directory fully in RAM, payloads paged back on demand.
    #[test]
    fn recovery_bounds_resident_memory_with_cache_cap() {
        let dir = tempfile::tempdir().unwrap();
        // Build a tablespace with many full-payload update records on disk,
        // small segments so they spill across several segment files.
        {
            let mut undo = UndoTablespace::open_with_segments(&[], dir.path(), 4096).unwrap();
            undo.set_buffer_limit(0).unwrap(); // write-through so frames hit disk
            for i in 0..400u64 {
                let mut rec = make_record(0, i + 1, i + 1, None);
                rec.kind = UndoKind::Update;
                rec.before_header = Some(RowHeader {
                    flags: 0,
                    last_txn_id: i + 1,
                    undo_ptr: None,
                });
                rec.before_values = Some(vec![
                    Value::Int(i as i64),
                    Value::String(format!("payload-{i}-0123456789abcdef0123456789")),
                ]);
                undo.append_record(rec, true).unwrap();
            }
            undo.flush().unwrap();
        }

        // Reopen with a tiny cap (via the same env the runtime config uses)
        // so recovery page-bounds itself as it scans.
        std::env::set_var("MYDB_UNDO_MEM_CACHE_BYTES", "8192");
        let reopened = UndoTablespace::open_with_segments(&[], dir.path(), 4096).unwrap();
        std::env::remove_var("MYDB_UNDO_MEM_CACHE_BYTES");

        // Full directory present, but resident payload bounded near the cap.
        assert_eq!(reopened.len(), 400);
        assert!(
            reopened.evicted_records() > 0,
            "recovery under a tiny cap must evict payloads"
        );
        let metadata_floor = reopened
            .records
            .values()
            .map(|record| metadata_resident_bytes(record))
            .fold(0u64, u64::saturating_add);
        assert!(
            reopened.resident_bytes() <= metadata_floor + 8192,
            "resident {} must stay within metadata {} plus the 8192-byte payload cap",
            reopened.resident_bytes(),
            metadata_floor
        );

        // Every record still reconstructs correctly — evicted ones page in
        // from their segment file.
        let ids: Vec<u64> = reopened.records.keys().copied().collect();
        for id in ids {
            let rec = reopened
                .load_owned(id)
                .expect("page-in must recover record");
            assert!(rec.before_values.is_some(), "before-image must survive");
        }
    }

    #[test]
    fn startup_recovery_stream_discards_finalized_insert_batches() {
        let dir = tempfile::tempdir().unwrap();
        let mut finalized_ptrs = Vec::new();
        let active_ptr;
        let delete_row_id = RowId {
            page_id: 42,
            slot_id: 7,
        };
        let disk_bytes_before;
        {
            // Each 20k-row batch is about 120 KiB on disk and about 360 KiB
            // by resident accounting. Two rounds across four shards force
            // rollover, exercising both empty non-active deletion and active
            // segment truncation during recovery.
            let mut undo = UndoTablespace::open_with_segments(&[], dir.path(), 128 * 1024).unwrap();
            undo.set_buffer_limit(0).unwrap();
            for txn_id in 1..=8u64 {
                let mut record = make_record(0, txn_id, txn_id, None);
                record.kind = UndoKind::InsertBatch {
                    row_ids: (0..20_000u32)
                        .map(|page_id| RowId {
                            page_id,
                            slot_id: 0,
                        })
                        .collect(),
                };
                finalized_ptrs.push(undo.append_record(record, true).unwrap().0);
            }
            // Finalized non-INSERT records may carry physical cleanup work.
            // Recovery must retain only that metadata while dropping their
            // heavy row images.
            let mut external_update = make_record(0, 20, 20, None);
            external_update.kind = UndoKind::Update;
            external_update.before_values = Some(vec![Value::String("x".repeat(256 * 1024))]);
            external_update.before_external = vec![(
                2,
                LobRef {
                    first_page_id: 100,
                    last_page_id: 101,
                    total_len: 4096,
                    generation: 20,
                },
            )];
            undo.append_record(external_update, true).unwrap();

            let mut delete = make_record(0, 21, 21, None);
            delete.kind = UndoKind::Delete;
            delete.row_id = delete_row_id;
            delete.before_values = Some(vec![Value::String("y".repeat(256 * 1024))]);
            undo.append_record(delete, true).unwrap();

            let mut active_batch = make_record(0, 99, 99, None);
            active_batch.kind = UndoKind::InsertBatch {
                row_ids: (0..30_000u32)
                    .map(|page_id| RowId {
                        page_id,
                        slot_id: 1,
                    })
                    .collect(),
            };
            active_ptr = undo.append_record(active_batch, true).unwrap().0;
            undo.flush().unwrap();
            disk_bytes_before = undo.disk_bytes;
        }

        let retained = HashSet::from([99u64]);
        let mut reopened = UndoTablespace::open_with_segments_for_recovery(
            &[],
            dir.path(),
            128 * 1024,
            &retained,
            false,
        )
        .unwrap();

        assert_eq!(
            reopened.record_count(),
            3,
            "active undo plus two cleanup-only records should remain"
        );
        assert!(
            reopened.get(active_ptr).is_none(),
            "active payload should stay on disk until rollback/MVCC needs it"
        );
        assert!(
            reopened.resident_bytes() > 0 && reopened.resident_bytes() < 4096,
            "startup should account directory metadata but retain no heavy payload: {} bytes",
            reopened.resident_bytes()
        );
        let loaded_active = reopened
            .load_owned(active_ptr)
            .expect("active undo must page in on demand");
        assert_eq!(loaded_active.txn_id, 99);
        assert!(
            matches!(
                loaded_active.kind,
                UndoKind::InsertBatch { ref row_ids } if row_ids.len() == 30_000
            ),
            "on-demand page-in must restore the full InsertBatch payload"
        );
        assert_eq!(reopened.purged_records, finalized_ptrs.len() as u64);
        for ptr in finalized_ptrs {
            assert!(
                reopened.get(ptr).is_none(),
                "finalized undo must not enter the in-memory directory"
            );
            assert!(
                reopened.load_owned(ptr).is_none(),
                "discarded frames must not page back in"
            );
        }
        assert!(
            reopened.disk_bytes < disk_bytes_before,
            "empty obsolete segments should be reclaimed at startup"
        );

        let creators = BTreeMap::from([(99u64, UndoCreatorState::Active)]);
        let cleanup =
            reopened.purge_except_chains(&[active_ptr], u64::MAX, None, &creators, usize::MAX);
        assert_eq!(reopened.record_count(), 1);
        assert!(
            cleanup
                .freed_chains
                .iter()
                .any(|(table_id, refs)| *table_id == 1 && refs.len() == 1),
            "external LOB cleanup must survive startup filtering"
        );
        assert!(
            cleanup.reclaim_targets.contains(&(1, delete_row_id)),
            "DELETE tombstone cleanup must survive startup filtering"
        );
    }

    // Phase 6 — a mid-stream corrupt undo frame hard-fails startup by
    // default, but MYDB_UNDO_FORCE_RECOVER truncates the corrupt tail and
    // recovers the CRC-clean prefix. (nextest isolates the env mutation.)
    #[test]
    fn force_recover_truncates_corrupt_undo_tail() {
        let dir = tempfile::tempdir().unwrap();
        let big_seg = 16 * 1024 * 1024; // one segment, no rotation
        let mut ptrs = Vec::new();
        {
            let mut undo = UndoTablespace::open_with_segments(&[], dir.path(), big_seg).unwrap();
            undo.set_buffer_limit(0).unwrap(); // write-through to disk
            for i in 0..6u64 {
                undo.next_shard.store(0, Ordering::Relaxed); // pin to shard 0
                let mut r = make_record(0, i + 1, 1000 + i, None);
                r.kind = UndoKind::Update;
                r.before_header = Some(RowHeader {
                    flags: 0,
                    last_txn_id: i + 1,
                    undo_ptr: None,
                });
                r.before_values = Some(vec![
                    Value::Int(i as i64),
                    Value::String(format!("rec-{i}-payloadpayloadpayload")),
                ]);
                let (id, _, _) = undo.append_record(r, true).unwrap();
                ptrs.push(id);
            }
            undo.flush().unwrap();
        }
        // Corrupt a byte inside the 4th frame's payload → breaks its CRC.
        let seg = undo_ptr_segment(ptrs[3]);
        let corrupt_frame_off = undo_ptr_offset(ptrs[3]);
        let seg_path =
            crate::storage::naming::system_undo_dir(dir.path()).join(segment_file_name(seg));
        {
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&seg_path)
                .unwrap();
            let pos = corrupt_frame_off + UNDO_FRAME_HEADER_LEN as u64 + 2;
            f.seek(SeekFrom::Start(pos)).unwrap();
            let mut b = [0u8; 1];
            f.read_exact(&mut b).unwrap();
            b[0] ^= 0xFF;
            f.seek(SeekFrom::Start(pos)).unwrap();
            f.write_all(&b).unwrap();
            f.flush().unwrap();
        }

        // Default: refuse to start on corruption.
        std::env::remove_var("MYDB_UNDO_FORCE_RECOVER");
        let err = UndoTablespace::open_with_segments(&[], dir.path(), big_seg).unwrap_err();
        assert_eq!(err.code(), 1030, "corruption must hard-fail by default");

        // Force-recover: truncate the corrupt tail, keep the clean prefix.
        std::env::set_var("MYDB_UNDO_FORCE_RECOVER", "1");
        let recovered = UndoTablespace::open_with_segments(&[], dir.path(), big_seg).unwrap();
        std::env::remove_var("MYDB_UNDO_FORCE_RECOVER");
        assert_eq!(
            recovered.len(),
            3,
            "only the 3 CRC-clean prefix records survive force-recover"
        );
        for ptr in &ptrs[0..3] {
            assert!(recovered.records.contains_key(ptr), "prefix record kept");
        }
        for ptr in &ptrs[3..6] {
            assert!(
                !recovered.records.contains_key(ptr),
                "corrupt frame and its suffix are dropped"
            );
        }
    }

    // P0/1 — a non-persistent (in-memory only) ptr still returns None,
    // since there is no segment to page it back in from.
    #[test]
    fn load_owned_returns_none_for_inmemory_only_ptr() {
        let mut undo = UndoTablespace::default();
        let rec = make_record(7, 1, 1, None);
        undo.insert(rec, false, false).unwrap();
        // Synthesize eviction: drop from records.
        undo.records.remove(&7);
        assert!(undo.load_owned(7).is_none());
    }

    // P0/5 — generalized admission API: callers can preflight any
    // expected payload size, not just INSERT.
    #[test]
    fn check_undo_admission_uses_caller_payload_estimate() {
        let mut undo = UndoTablespace::default();
        undo.set_mem_soft_limit_bytes(2048);
        assert!(!undo.check_undo_admission(1000, false).unwrap());
        // Estimate above the soft cap flags pressure.
        assert!(undo.check_undo_admission(5000, false).unwrap());
    }

    #[test]
    fn purge_uses_created_index_to_drop_only_stale_records() {
        let mut undo = UndoTablespace::default();
        // 1000 stale records (created_ms < 100), all with no live chains.
        for i in 0..1000 {
            undo.insert(make_record(i + 1, i + 1, (i % 50) + 1, None), false, true)
                .unwrap();
        }
        // 200 fresh records (created_ms = 200).
        for i in 0..200 {
            undo.insert(make_record(2_000 + i, 2_000 + i, 200, None), false, true)
                .unwrap();
        }
        let pre_count = undo.records.len();
        assert_eq!(pre_count, 1200);

        // Purge with cutoff 100: stale records (created < 100) eligible to
        // remove. Fresh records (>=200) untouched. No active chains.
        undo.purge_except_chains(&[], 100, None, &BTreeMap::new(), usize::MAX);

        assert_eq!(undo.records.len(), 200, "all stale records removed");
        // The created_index should only contain the fresh bucket.
        let buckets: Vec<u64> = undo.created_index.keys().copied().collect();
        assert_eq!(buckets, vec![200]);
        assert_eq!(undo.purged_records, 1000);
    }

    #[test]
    fn purge_includes_records_exactly_at_retention_cutoff() {
        let mut undo = UndoTablespace::default();
        undo.insert(make_record(1, 1, 100, None), false, true)
            .unwrap();
        undo.insert(make_record(2, 2, 101, None), false, true)
            .unwrap();

        let victims =
            undo.plan_purge_victims(&[], 100, None, &BTreeMap::new(), u64::MAX, usize::MAX);

        assert_eq!(victims, vec![1]);
    }

    #[test]
    fn purge_keeps_creator_missing_from_stale_txn_snapshot_until_gc_classifies_it() {
        let mut undo = UndoTablespace::default();
        undo.insert(make_record(1, 42, 100, None), false, true)
            .unwrap();

        // Model the production race: transaction state was captured before
        // txn 42 existed, then txn 42 appended this undo before the undo scan.
        // The old code treated the absent creator as an already-GC'd commit
        // and selected the live record immediately.
        let victims = undo.plan_purge_victims(&[], 100, None, &BTreeMap::new(), 41, usize::MAX);
        assert!(
            victims.is_empty(),
            "a creator newer than the slot-GC watermark is unclassified and must survive"
        );

        let victims = undo.plan_purge_victims(&[], 100, None, &BTreeMap::new(), 42, usize::MAX);
        assert_eq!(
            victims,
            vec![1],
            "once slot GC classifies the absent creator as committed, purge may reclaim it"
        );
    }

    #[test]
    fn purge_keeps_records_pinned_by_active_chain_even_when_stale() {
        let mut undo = UndoTablespace::default();
        // Build a chain: 1 → 2 → 3, all stale.
        undo.insert(make_record(1, 1, 10, None), false, true)
            .unwrap();
        undo.insert(make_record(2, 1, 20, Some(1)), false, true)
            .unwrap();
        undo.insert(make_record(3, 1, 30, Some(2)), false, true)
            .unwrap();
        // An unrelated stale record.
        undo.insert(make_record(4, 2, 15, None), false, true)
            .unwrap();

        // heads=[3] so chain 3→2→1 must be retained even though all are stale.
        undo.purge_except_chains(&[3], 100, None, &BTreeMap::new(), usize::MAX);

        assert!(undo.records.contains_key(&1));
        assert!(undo.records.contains_key(&2));
        assert!(undo.records.contains_key(&3));
        assert!(!undo.records.contains_key(&4), "unpinned stale dropped");

        // Side index must still have entries for all surviving records.
        let mut surviving_ids: Vec<u64> = undo
            .created_index
            .values()
            .flat_map(|v| v.iter().copied())
            .collect();
        surviving_ids.sort();
        assert_eq!(surviving_ids, vec![1, 2, 3]);
    }

    #[test]
    fn purge_skips_buckets_above_cutoff() {
        let mut undo = UndoTablespace::default();
        for i in 0..500 {
            undo.insert(make_record(i + 1, i + 1, 1_000 + i, None), false, true)
                .unwrap();
        }
        // All records are >= 1_000; cutoff 999 should purge nothing AND not
        // touch any bucket.
        let bucket_count_before = undo.created_index.len();
        undo.purge_except_chains(&[], 999, None, &BTreeMap::new(), usize::MAX);
        assert_eq!(undo.records.len(), 500);
        assert_eq!(undo.created_index.len(), bucket_count_before);
        assert_eq!(undo.purged_records, 0);
    }

    #[test]
    fn unchanged_manifest_reuses_identity_and_repairs_changed_contents() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let mut undo = UndoTablespace::open_with_segments(&[], dir.path(), 1024 * 1024).unwrap();
        let path = crate::storage::naming::system_undo_dir(dir.path()).join("undo.manifest");
        let original = File::open(&path).unwrap();
        let bytes = fs::read(&path).unwrap();
        for _ in 0..2 {
            undo.checkpoint_sync_manifest().unwrap();
            assert_eq!(fs::read(&path).unwrap(), bytes);
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                assert_eq!(
                    original.metadata().unwrap().ino(),
                    fs::metadata(&path).unwrap().ino()
                );
            }
        }
        #[cfg(target_os = "macos")]
        {
            assert!(undo.manifest_sync_snapshot.lock().is_some());
            FAIL_NEXT_MANIFEST_DIRECTORY_SYNC.with(|flag| flag.set(true));
            undo.checkpoint_sync_manifest().unwrap();
            // Reusing a successful namespace barrier performs no new sync.
            assert!(FAIL_NEXT_MANIFEST_DIRECTORY_SYNC.with(|flag| flag.get()));
            let staging = path.with_extension("compacting");
            fs::write(&staging, b"unpublished").unwrap();
            assert!(undo.checkpoint_sync_manifest().is_err());
            assert!(undo.manifest_sync_snapshot.lock().is_none());
            assert!(undo.compensation_manifest_dirty.load(Ordering::Acquire));
            undo.checkpoint_sync_manifest().unwrap();
            assert!(undo.manifest_sync_snapshot.lock().is_none());
            fs::remove_file(staging).unwrap();
            undo.checkpoint_sync_manifest().unwrap();
            assert!(undo.manifest_sync_snapshot.lock().is_some());
            // Equal contents alone cannot prove metadata is still durable.
            use std::os::unix::fs::PermissionsExt;
            let permissions = fs::metadata(&path).unwrap().permissions();
            fs::set_permissions(
                &path,
                fs::Permissions::from_mode(permissions.mode() ^ 0o100),
            )
            .unwrap();
            FAIL_NEXT_MANIFEST_DIRECTORY_SYNC.with(|flag| flag.set(true));
            assert!(undo.checkpoint_sync_manifest().is_err());
            assert!(undo.manifest_sync_snapshot.lock().is_none());
            undo.checkpoint_sync_manifest().unwrap();
            fs::set_permissions(&path, permissions).unwrap();
            undo.checkpoint_sync_manifest().unwrap();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _batch = crate::platform::fs::FixtureSyncBatch::new(dir.path()).unwrap();
                undo.checkpoint_sync_manifest().unwrap();
                panic!("abort private fixture construction");
            }));
            assert!(result.is_err());
            // An abandoned deferred barrier cannot certify later real IO.
            FAIL_NEXT_MANIFEST_DIRECTORY_SYNC.with(|flag| flag.set(true));
            assert!(undo.checkpoint_sync_manifest().is_err());
            assert!(undo.manifest_sync_snapshot.lock().is_none());
            undo.checkpoint_sync_manifest().unwrap();
        }
        drop(original);
        fs::write(&path, b"corrupt manifest").unwrap();
        undo.checkpoint_sync_manifest().unwrap();
        assert_eq!(fs::read(&path).unwrap(), bytes);
        let (ptr, _, _) = undo
            .append_record(make_record(0, 7, 10, None), true)
            .unwrap();
        undo.flush().unwrap();
        undo.mark_compensated(&[ptr]);
        #[cfg(target_os = "macos")]
        {
            FAIL_NEXT_MANIFEST_DIRECTORY_SYNC.with(|flag| flag.set(true));
            assert!(undo.checkpoint_sync_manifest().is_err());
            assert!(undo.manifest_sync_snapshot.lock().is_none());
        }
        undo.checkpoint_sync_manifest().unwrap();
        let manifest: UndoManifest = decode_undo_manifest(&fs::read(&path).unwrap()).unwrap();
        assert!(manifest.compensated.contains(&ptr));
        #[cfg(unix)]
        {
            let outside = dir.path().join("outside");
            let expected = fs::read(&path).unwrap();
            fs::write(&outside, &expected).unwrap();
            fs::remove_file(&path).unwrap();
            std::os::unix::fs::symlink(&outside, &path).unwrap();
            undo.checkpoint_sync_manifest().unwrap();
            assert!(!fs::symlink_metadata(&path)
                .unwrap()
                .file_type()
                .is_symlink());
            assert_eq!(fs::read(&outside).unwrap(), expected);
        }
        drop(undo);
        let reopened = UndoTablespace::open_with_segments(&[], dir.path(), 1024 * 1024).unwrap();
        assert!(reopened.compensated.contains(&ptr));
    }

    #[test]
    fn segment_append_reopen_round_trips_record() {
        let dir = tempfile::tempdir().unwrap();
        let mut undo = UndoTablespace::open_with_segments(&[], dir.path(), 1024 * 1024).unwrap();
        let (ptr, _, _) = undo
            .append_record(make_record(0, 7, 10, None), true)
            .unwrap();
        undo.flush().unwrap();

        let reopened = UndoTablespace::open_with_segments(&[], dir.path(), 1024 * 1024).unwrap();
        let record = reopened
            .get(ptr)
            .expect("record should reload from segment");
        assert_eq!(record.txn_id, 7);
        assert_eq!(record.id, ptr);
        assert!(crate::storage::naming::system_undo_dir(dir.path())
            .join("undo.manifest")
            .exists());
    }

    #[test]
    fn online_replication_buffers_out_of_order_segment_offsets() {
        let source_dir = tempfile::tempdir().unwrap();
        let mut source =
            UndoTablespace::open_with_segments(&[], source_dir.path(), 1024 * 1024).unwrap();
        let mut frames = Vec::new();
        for txn_id in 7..12 {
            let mut record = make_record(0, txn_id, 10 + txn_id, None);
            record.before_values = Some(vec![Value::String("x".repeat(8 * 1024))]);
            frames.push(source.append_record(record, true).unwrap());
        }
        // Four shards are selected round-robin; records 0 and 4 are two
        // consecutive physical offsets in the same segment.
        let (first_ptr, first_bytes, _) = frames[0].clone();
        let (second_ptr, second_bytes, _) = frames[4].clone();
        assert_eq!(undo_ptr_segment(first_ptr), undo_ptr_segment(second_ptr));

        let target_dir = tempfile::tempdir().unwrap();
        let mut target =
            UndoTablespace::open_with_segments(&[], target_dir.path(), 1024 * 1024).unwrap();
        target.set_mem_cache_limit_bytes(1024);
        target
            .apply_replicated_wal_record(second_ptr, &second_bytes)
            .expect("temporary offset gap is buffered, not corruption");
        assert!(target.replicated_pending_frames.contains_key(&second_ptr));
        target
            .apply_replicated_wal_record(first_ptr, &first_bytes)
            .unwrap();
        target.drain_replicated_pending_frames().unwrap();
        assert!(target.replicated_pending_frames.is_empty());
        assert!(
            target.evicted_records > 0,
            "durable replicated frames must obey the undo payload cache cap"
        );
        target.flush().unwrap();

        let reopened =
            UndoTablespace::open_with_segments(&[], target_dir.path(), 1024 * 1024).unwrap();
        assert_eq!(reopened.get(first_ptr).unwrap().txn_id, 7);
        assert_eq!(reopened.get(second_ptr).unwrap().txn_id, 11);
    }

    #[test]
    fn fresh_audit_replication_gap_promotion_relocates_complete_frames() {
        let source_dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let mut source =
            UndoTablespace::open_with_segments(&[], source_dir.path(), 1024 * 1024).unwrap();
        let mut frames = Vec::new();
        for txn_id in 7..12 {
            let mut record = make_record(0, txn_id, 10 + txn_id, None);
            record.before_values = Some(vec![Value::String("x".repeat(8 * 1024))]);
            frames.push(source.append_record(record, true).unwrap());
        }
        let (missing_ptr, missing_bytes, _) = &frames[0];
        let (ptr, bytes, _) = &frames[4];
        let target_dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        {
            let mut target =
                UndoTablespace::open_with_segments(&[], target_dir.path(), 1024 * 1024).unwrap();
            target.apply_replicated_wal_record(*ptr, bytes).unwrap();
            assert!(target.has_replicated_pending_frames());
            target.persist_replicated_frames_for_promotion().unwrap();
            assert!(!target.has_replicated_pending_frames());
            assert_ne!(target.canonical_ptr(*ptr), *ptr);
            target.set_mem_cache_limit_bytes(1);
            target.evict_to_cache_budget();
            assert_eq!(target.load_owned(*ptr).unwrap().txn_id, 11);
            // The late prefix and duplicate later frame still resolve to
            // distinct logical records without replacing the durable alias.
            target
                .apply_replicated_wal_record(*missing_ptr, missing_bytes)
                .unwrap();
            target.apply_replicated_wal_record(*ptr, bytes).unwrap();
            target.drain_replicated_pending_frames().unwrap();
            assert!(!target.has_replicated_pending_frames());
            target.flush().unwrap();
        }
        let reopened =
            UndoTablespace::open_with_segments(&[], target_dir.path(), 1024 * 1024).unwrap();
        assert_eq!(reopened.load_owned(*ptr).unwrap().txn_id, 11);
        assert_eq!(reopened.load_owned(*missing_ptr).unwrap().txn_id, 7);
    }

    #[test]
    fn replicated_undo_gap_remains_readable_after_payload_eviction() {
        let source_dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let mut source =
            UndoTablespace::open_with_segments(&[], source_dir.path(), 1024 * 1024).unwrap();
        let mut frames = Vec::new();
        for txn_id in 7..12 {
            let mut record = make_record(0, txn_id, 10 + txn_id, None);
            record.before_values = Some(vec![Value::String("x".repeat(8 * 1024))]);
            frames.push(source.append_record(record, true).unwrap());
        }
        let (first_ptr, first_bytes, _) = &frames[0];
        let (later_ptr, later_bytes, _) = &frames[4];
        let target_dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let mut target =
            UndoTablespace::open_with_segments(&[], target_dir.path(), 1024 * 1024).unwrap();
        target.set_mem_cache_limit_bytes(1024);
        target
            .apply_replicated_wal_record(*later_ptr, later_bytes)
            .unwrap();
        target.drain_replicated_pending_frames().unwrap();
        assert!(target.has_replicated_pending_frames());
        assert!(target.evicted_records > 0);
        let read = target
            .prepare_load_owned(*later_ptr)
            .unwrap()
            .expect("pending WAL frame is still a valid before-image");
        let PreparedUndoRead::Resident(record) = read else {
            panic!("the pending frame has no segment offset yet")
        };
        assert_eq!(
            record.before_values,
            Some(vec![Value::String("x".repeat(8 * 1024))])
        );
        assert_eq!(target.load_owned(*later_ptr).unwrap().txn_id, 11);
        target
            .apply_replicated_wal_record(*first_ptr, first_bytes)
            .unwrap();
        target.drain_replicated_pending_frames().unwrap();
        assert!(!target.has_replicated_pending_frames());
        assert_eq!(target.load_owned(*later_ptr).unwrap().txn_id, 11);
    }

    /// Frame-by-frame decoder identical to the pre-optimization
    /// `decode_segment_file` body (seek + read_exact(header) +
    /// `vec![0u8; len]` + read_exact(payload) per frame). Kept in the test
    /// only, as the A/B baseline for the whole-file decode path.
    fn decode_segment_file_old_style(path: &Path, segment_id: u32) -> (usize, u64) {
        let mut file = OpenOptions::new().read(true).open(path).unwrap();
        let len = file.metadata().unwrap().len();
        let mut header = [0u8; UNDO_SEGMENT_HEADER_LEN as usize];
        file.read_exact(&mut header).unwrap();
        let mut offset = UNDO_SEGMENT_HEADER_LEN;
        let mut count = 0usize;
        loop {
            if offset >= len {
                break;
            }
            file.seek(SeekFrom::Start(offset)).unwrap();
            let mut frame_header = [0u8; UNDO_FRAME_HEADER_LEN];
            if file.read_exact(&mut frame_header).is_err() {
                break;
            }
            let payload_len = u32::from_le_bytes(frame_header[4..8].try_into().unwrap()) as usize;
            let checksum = u32::from_le_bytes(frame_header[8..12].try_into().unwrap());
            if offset
                .saturating_add(UNDO_FRAME_HEADER_LEN as u64)
                .saturating_add(payload_len as u64)
                > len
            {
                break;
            }
            let mut payload = vec![0u8; payload_len];
            file.read_exact(&mut payload).unwrap();
            assert_eq!(crc32fast::hash(&payload), checksum);
            let mut record = decode_undo_record(&payload).unwrap();
            record.id = make_undo_ptr(segment_id, offset).unwrap();
            count += 1;
            offset += UNDO_FRAME_HEADER_LEN as u64 + payload_len as u64;
        }
        (count, offset)
    }

    #[test]
    fn decode_segment_whole_file_matches_old_frame_by_frame() {
        let dir = tempfile::tempdir().unwrap();
        // Large per-segment budget so many frames land in each file.
        let mut undo =
            UndoTablespace::open_with_segments(&[], dir.path(), 256 * 1024 * 1024).unwrap();
        const N: u64 = 20_000;
        for i in 0..N {
            let mut record = make_record(0, i + 1, i + 1, None);
            record.before_values =
                Some(vec![Value::Int(i as i64), Value::String("payload".into())]);
            undo.append_record(record, true).unwrap();
        }
        undo.flush().unwrap();
        let undo_dir = crate::storage::naming::system_undo_dir(dir.path());
        // Decode the fattest segment file (the one with the most frames).
        let target_seg = undo
            .write_shards
            .iter()
            .map(|s| s.lock().active_segment_id)
            .max_by_key(|seg| {
                std::fs::metadata(undo_dir.join(segment_file_name(*seg)))
                    .map(|m| m.len())
                    .unwrap_or(0)
            })
            .unwrap();
        let path = undo_dir.join(segment_file_name(target_seg));
        let frames = std::fs::metadata(&path).unwrap().len() / 64; // rough

        // Warm the page cache equally, then A/B time.
        let (old_count, old_end) = decode_segment_file_old_style(&path, target_seg);
        let t_old = std::time::Instant::now();
        for _ in 0..5 {
            decode_segment_file_old_style(&path, target_seg);
        }
        let old_ns = t_old.elapsed().as_nanos() / 5;

        let decoded = decode_segment_file(&path, target_seg, false).unwrap();
        let t_new = std::time::Instant::now();
        for _ in 0..5 {
            decode_segment_file(&path, target_seg, false).unwrap();
        }
        let new_ns = t_new.elapsed().as_nanos() / 5;

        // Equivalence: same number of records, same valid_end.
        assert_eq!(decoded.records.len(), old_count);
        assert_eq!(decoded.valid_end, old_end);
        assert!(decoded.records.len() >= 3_000, "expected a fat segment");
        eprintln!(
            "[decode A/B] frames≈{frames} records={} old={}µs new={}µs speedup={:.2}x",
            decoded.records.len(),
            old_ns / 1000,
            new_ns / 1000,
            old_ns as f64 / new_ns.max(1) as f64
        );
    }

    #[test]
    fn segment_rollover_creates_multiple_segment_files() {
        let dir = tempfile::tempdir().unwrap();
        let mut undo = UndoTablespace::open_with_segments(&[], dir.path(), 256).unwrap();
        for i in 0..8 {
            let mut record = make_record(0, i + 1, i + 1, None);
            record.before_values = Some(vec![Value::String("x".repeat(160))]);
            undo.append_record(record, true).unwrap();
        }
        undo.flush().unwrap();
        assert!(undo.segment_count() > 1);
    }

    #[test]
    fn segment_replay_truncates_partial_tail() {
        let dir = tempfile::tempdir().unwrap();
        let mut undo = UndoTablespace::open_with_segments(&[], dir.path(), 1024 * 1024).unwrap();
        undo.append_record(make_record(0, 1, 1, None), true)
            .unwrap();
        undo.flush().unwrap();
        // P1/6: the appended record lands in whichever shard the
        // round-robin counter picked. Find the segment that actually
        // grew past the bare header.
        let undo_dir = crate::storage::naming::system_undo_dir(dir.path());
        let target_seg = undo
            .write_shards
            .iter()
            .map(|s| s.lock().active_segment_id)
            .find(|seg| {
                std::fs::metadata(undo_dir.join(segment_file_name(*seg)))
                    .map(|m| m.len() > UNDO_SEGMENT_HEADER_LEN)
                    .unwrap_or(false)
            })
            .expect("at least one shard's active segment must have grown");
        drop(undo);
        let path = undo_dir.join(segment_file_name(target_seg));
        let before = std::fs::metadata(&path).unwrap().len();
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(&[1, 2, 3, 4]).unwrap();
        }
        let reopened = UndoTablespace::open_with_segments(&[], dir.path(), 1024 * 1024).unwrap();
        assert_eq!(reopened.record_count(), 1);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), before);
    }

    #[test]
    fn legacy_undo_tbl_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let undo_dir = crate::storage::naming::system_undo_dir(dir.path());
        std::fs::create_dir_all(&undo_dir).unwrap();
        std::fs::write(undo_dir.join("undo.tbl"), b"old").unwrap();
        let err = UndoTablespace::open_with_segments(&[], dir.path(), 1024 * 1024).unwrap_err();
        assert!(err.message().contains("legacy undo/undo.tbl"));
    }

    #[test]
    fn purge_keeps_update_records_when_snapshot_is_active() {
        let mut undo = UndoTablespace::default();
        let mut record = make_record(1, 1, 10, None);
        record.kind = UndoKind::Update;
        record.before_header = Some(RowHeader {
            flags: 0,
            last_txn_id: 1,
            undo_ptr: None,
        });
        record.before_values = Some(vec![Value::Int(1)]);
        undo.insert(record, false, true).unwrap();

        let mut states = BTreeMap::new();
        states.insert(1, UndoCreatorState::Committed(2));
        undo.purge_except_chains(&[], 100, Some(1), &states, usize::MAX);
        assert!(undo.records.contains_key(&1));
        undo.purge_except_chains(&[], 100, Some(2), &states, usize::MAX);
        assert!(!undo.records.contains_key(&1));
    }

    #[test]
    fn purge_keeps_insert_records_needed_by_older_snapshot() {
        let mut undo = UndoTablespace::default();
        undo.insert(make_record(1, 1, 10, None), false, true)
            .unwrap();

        let mut states = BTreeMap::new();
        states.insert(1, UndoCreatorState::Committed(5));
        undo.purge_except_chains(&[], 100, Some(4), &states, usize::MAX);
        assert!(undo.records.contains_key(&1));
        undo.purge_except_chains(&[], 100, Some(5), &states, usize::MAX);
        assert!(!undo.records.contains_key(&1));
    }
}

#[cfg(test)]
mod update_delta_codec_tests {
    use super::*;

    fn record_with_kind(kind: UndoKind, before_values: Option<Vec<Value>>) -> UndoRecord {
        UndoRecord {
            id: 42,
            txn_id: 7,
            created_unix_ms: 1234,
            prev_undo_ptr: Some(11),
            kind,
            table_id: 99,
            row_id: RowId {
                page_id: 5,
                slot_id: 2,
            },
            before_header: Some(RowHeader {
                flags: 0,
                last_txn_id: 5,
                undo_ptr: Some(8),
            }),
            before_values,
            before_external: Vec::new(),
            borrowed_external: Vec::new(),
            chain_depth: 1,
            cached_size: 0,
            resident_size: 0,
            temp_reservation: None,
            resident: true,
            encoded_len: 0,
        }
    }

    #[test]
    fn header_delete_codec_and_metadata_preserve_purge_ownership() {
        let record = record_with_kind(UndoKind::DeleteHeader, None);
        let bytes = encode_undo_record(&record).unwrap();
        let decoded = decode_undo_record(&bytes).unwrap();
        let metadata = decode_undo_record_metadata(&bytes).unwrap();
        assert!(matches!(decoded.kind, UndoKind::DeleteHeader));
        assert!(matches!(metadata.kind, UndoKind::DeleteHeader));
        assert_eq!(decoded.before_header, record.before_header);
        assert!(decoded.before_values.is_none() && decoded.before_external.is_empty());
        let full = record_with_kind(UndoKind::Delete, Some(vec![Value::String("x".repeat(120))]));
        assert!(bytes.len() < encode_undo_record(&full).unwrap().len());
    }

    #[test]
    fn update_delta_round_trip_two_columns() {
        let kind = UndoKind::UpdateDelta {
            before_cols: vec![3, 7],
        };
        let record = record_with_kind(kind, Some(vec![Value::Int(100), Value::Int(-1)]));
        let bytes = encode_undo_record(&record).unwrap();
        let decoded = decode_undo_record(&bytes).unwrap();
        assert!(
            matches!(decoded.kind, UndoKind::UpdateDelta { ref before_cols } if before_cols == &vec![3u16, 7])
        );
        assert_eq!(decoded.before_values, record.before_values);
        assert_eq!(decoded.row_id, record.row_id);
        assert_eq!(decoded.table_id, record.table_id);
    }

    #[test]
    fn borrowed_lob_descriptors_round_trip_without_payload() {
        let mut record = record_with_kind(
            UndoKind::Update,
            Some(vec![Value::Int(1), Value::Null, Value::Null]),
        );
        record.before_external = vec![(
            1,
            LobRef {
                first_page_id: 10,
                last_page_id: 12,
                total_len: 32_000,
                generation: 21,
            },
        )];
        record.borrowed_external = vec![(
            2,
            LobRef {
                first_page_id: 20,
                last_page_id: 24,
                total_len: 64_000,
                generation: 22,
            },
        )];
        let bytes = encode_undo_record(&record).unwrap();
        assert!(
            bytes.len() < 256,
            "descriptors unexpectedly copied LOB payload"
        );
        let decoded = decode_undo_record(&bytes).unwrap();
        assert_eq!(decoded.before_external, record.before_external);
        assert_eq!(decoded.borrowed_external, record.borrowed_external);
        assert_eq!(decoded.before_values, record.before_values);
    }

    #[test]
    fn update_delta_round_trip_empty_cols() {
        // No-op delta — still a valid record (e.g. UPDATE that ended
        // up not changing anything but the txn still wrote undo).
        let record = record_with_kind(
            UndoKind::UpdateDelta {
                before_cols: Vec::new(),
            },
            Some(Vec::new()),
        );
        let bytes = encode_undo_record(&record).unwrap();
        let decoded = decode_undo_record(&bytes).unwrap();
        assert!(
            matches!(decoded.kind, UndoKind::UpdateDelta { ref before_cols } if before_cols.is_empty())
        );
        assert_eq!(decoded.before_values, Some(Vec::new()));
    }

    #[test]
    fn update_delta_round_trip_many_columns() {
        // 17 columns exercises the u16 col_idx encoding and forces
        // the encoder/decoder to walk a non-trivial length.
        let cols: Vec<u16> = (0..17).map(|i| i as u16 * 2 + 1).collect();
        let vals: Vec<Value> = (0..17).map(|i| Value::Int(i * 13 - 5)).collect();
        let record = record_with_kind(
            UndoKind::UpdateDelta {
                before_cols: cols.clone(),
            },
            Some(vals.clone()),
        );
        let bytes = encode_undo_record(&record).unwrap();
        let decoded = decode_undo_record(&bytes).unwrap();
        assert!(
            matches!(decoded.kind, UndoKind::UpdateDelta { ref before_cols } if before_cols == &cols)
        );
        assert_eq!(decoded.before_values, Some(vals));
    }

    #[test]
    fn update_delta_round_trips_decimal128_and_wide_tags() {
        let wide = Value::DecimalWide(Box::new(crate::types::DecimalWide {
            magnitude: num_bigint::BigInt::parse_bytes(
                b"99999999999999999999999999999999999999999999999999999999999999999",
                10,
            )
            .unwrap(),
            scale: 0,
        }));
        let values = vec![Value::Decimal128(i128::MAX, 3), wide];
        let record = record_with_kind(
            UndoKind::UpdateDelta {
                before_cols: vec![1, 2],
            },
            Some(values.clone()),
        );
        let bytes = encode_undo_record(&record).unwrap();
        assert_eq!(bytes[0], UNDO_BINARY_VERSION);
        assert_eq!(
            decode_undo_record(&bytes).unwrap().before_values,
            Some(values)
        );

        let mut old = bytes;
        old[0] = UNDO_BINARY_VERSION - 1;
        let error = decode_undo_record(&old).unwrap_err();
        assert_eq!(error.code(), crate::error::catalog::ER_GET_ERRNO.code());
    }

    #[test]
    fn update_delta_record_size_is_smaller_than_full_row() {
        // Phase 2 motivation check: a 1-col delta of a 20-col row
        // must encode well under the size of a full-row before-image.
        let cols_one = vec![5u16];
        let val_one = vec![Value::Int(42)];
        let mut full_vals = Vec::with_capacity(20);
        for i in 0..20 {
            full_vals.push(Value::Int(i));
        }
        let delta_record = record_with_kind(
            UndoKind::UpdateDelta {
                before_cols: cols_one,
            },
            Some(val_one),
        );
        let full_record = record_with_kind(UndoKind::Update, Some(full_vals));
        let delta_bytes = encode_undo_record(&delta_record).unwrap();
        let full_bytes = encode_undo_record(&full_record).unwrap();
        // For a 20-col row updating 1 col, the delta should be
        // at least 50% smaller than the full-row before-image undo.
        assert!(
            delta_bytes.len() * 2 < full_bytes.len(),
            "delta {}B should be << full {}B",
            delta_bytes.len(),
            full_bytes.len()
        );
    }
}

#[cfg(test)]
mod buffer_tests {
    use super::*;

    fn record(txn_id: u64) -> UndoRecord {
        UndoRecord {
            id: 0,
            txn_id,
            created_unix_ms: 1000 + txn_id,
            prev_undo_ptr: None,
            kind: UndoKind::Update,
            table_id: 7,
            row_id: RowId {
                page_id: 1,
                slot_id: txn_id as u16,
            },
            before_header: Some(RowHeader {
                flags: 0,
                last_txn_id: txn_id,
                undo_ptr: None,
            }),
            before_values: Some(vec![Value::Int(txn_id as i64), Value::Null]),
            before_external: Vec::new(),
            borrowed_external: Vec::new(),
            chain_depth: 0,
            cached_size: 0,
            resident_size: 0,
            temp_reservation: None,
            resident: true,
            encoded_len: 0,
        }
    }

    fn open(dir: &Path) -> UndoManager {
        UndoManager::open_with_segments(&[], dir, DEFAULT_UNDO_SEGMENT_BYTES).unwrap()
    }

    #[test]
    fn release_format_undo_frames_are_bound_to_segment_and_offset() {
        let mut value = record(7);
        value.id = make_undo_ptr(16, UNDO_SEGMENT_HEADER_LEN).unwrap();
        let payload = encode_undo_record(&value).unwrap();
        let header = undo_frame_header(&payload);
        validate_undo_frame_header(&header, 16, UNDO_SEGMENT_HEADER_LEN).unwrap();
        assert!(validate_undo_frame_header(&header, 17, UNDO_SEGMENT_HEADER_LEN).is_err());
        assert!(validate_undo_frame_header(&header, 16, UNDO_SEGMENT_HEADER_LEN + 1).is_err());
    }

    #[test]
    fn release_format_sealed_undo_tail_is_preserved() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let mut undo = open(dir.path());
        let ptr = undo.append_record(record(1), true).unwrap().0;
        undo.flush().unwrap();
        let segment = undo_ptr_segment(ptr);
        let lane = shard_of_segment(segment) as usize;
        undo.write_shards[lane].lock().rotate().unwrap();
        undo.flush().unwrap();
        let path = undo.segment_path(segment).unwrap();
        drop(undo);
        let mut bytes = fs::read(&path).unwrap();
        bytes.extend_from_slice(&[1, 2, 3]);
        fs::write(&path, &bytes).unwrap();
        assert!(
            UndoManager::open_with_segments(&[], dir.path(), DEFAULT_UNDO_SEGMENT_BYTES).is_err()
        );
        assert_eq!(fs::read(path).unwrap(), bytes);
    }

    #[test]
    fn release_format_golden_undo_frame() {
        crate::storage::format_golden::golden!(
            "undo-segment",
            &encode_undo_segment_header(16, [0xa5; 16])
        );
        let mut value = record(7);
        value.id = make_undo_ptr(16, UNDO_SEGMENT_HEADER_LEN).unwrap();
        let bytes = encode_undo_record(&value).unwrap();
        let mut frame = undo_frame_header(&bytes).to_vec();
        frame.extend_from_slice(&bytes);
        crate::storage::format_golden::golden!("undo-frame", &frame);
    }

    #[test]
    fn release_format_undo_rejects_length_corruption_without_discarding_suffix() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let mut undo = open(dir.path());
        let ptr = undo.append_record(record(1), true).unwrap().0;
        undo.append_record(record(2), true).unwrap();
        undo.flush().unwrap();
        let segment = undo_ptr_segment(ptr);
        let path = undo.segment_path(segment).unwrap();
        drop(undo);
        let mut bytes = fs::read(&path).unwrap();
        bytes[undo_ptr_offset(ptr) as usize + 6] ^= 0x10;
        fs::write(&path, &bytes).unwrap();
        assert!(decode_segment_file(&path, segment, false).is_err());
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn checkpoint_pending_sync_releases_reclaimed_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mut undo = open(dir.path());
        undo.set_segment_bytes(512).unwrap();
        let ids: Vec<_> = (0..128)
            .map(|_| undo.append_record(record(1), true).unwrap().0)
            .collect();
        for id in ids {
            undo.remove_record(id).unwrap();
        }
        let obsolete = undo.detach_empty_segments();
        assert!(!obsolete.is_empty());
        for shard in undo.write_shards.iter() {
            let shard = shard.lock();
            assert!(shard
                .pending_sync
                .iter()
                .all(|id| *id == shard.active_segment_id));
        }
    }

    #[test]
    fn checkpoint_sync_unlocked_preserves_concurrent_undo_writes() {
        let dir = tempfile::tempdir().unwrap();
        let mut undo = open(dir.path());
        undo.append_record(record(1), true).unwrap();
        let shard = undo
            .write_shards
            .iter()
            .find(|shard| !shard.lock().pending_sync.is_empty())
            .unwrap()
            .clone();
        UndoWriteShard::checkpoint_sync_with(&shard, || {
            assert!(
                shard.try_lock().is_some(),
                "fsync must release the append latch"
            );
            // The allocator distributes writes round-robin across shards.
            for _ in 0..undo.write_shards.len() {
                undo.append_record(record(1), true).unwrap();
            }
        })
        .unwrap();
        assert!(
            !shard.lock().pending_sync.is_empty(),
            "concurrent writes must remain dirty"
        );
        undo.checkpoint_sync().unwrap();
        assert!(shard.lock().pending_sync.is_empty());
    }

    #[test]
    fn checkpoint_sync_rejects_missing_active_segment_and_preserves_retry() {
        check_missing_checkpoint_segment(true);
    }

    #[test]
    fn checkpoint_sync_rejects_missing_rotated_segment_and_preserves_retry() {
        check_missing_checkpoint_segment(false);
    }

    fn check_missing_checkpoint_segment(active: bool) {
        let dir = tempfile::tempdir().unwrap();
        let mut undo = open(dir.path());
        undo.set_segment_bytes(512).unwrap();
        let ids: Vec<_> = (0..128)
            .map(|_| undo.append_record(record(1), true).unwrap().0)
            .collect();
        undo.flush().unwrap();
        let shard = undo
            .write_shards
            .iter()
            .find(|shard| shard.lock().pending_sync.len() > 1)
            .unwrap()
            .clone();
        let segment = {
            let guard = shard.lock();
            if active {
                guard.active_segment_id
            } else {
                *guard
                    .pending_sync
                    .iter()
                    .find(|&&id| id != guard.active_segment_id)
                    .unwrap()
            }
        };
        let path = undo.segment_path(segment).unwrap();
        let hidden = path.with_extension("hidden");
        fs::rename(&path, &hidden).unwrap();
        assert!(dir.path().is_dir());
        let failure = undo.checkpoint_sync();
        assert!(
            failure.is_err(),
            "a missing live undo segment must not complete checkpoint: {failure:?}"
        );
        assert!(shard.lock().pending_sync.contains(&segment));
        fs::rename(&hidden, &path).unwrap();
        undo.checkpoint_sync().unwrap();
        assert!(undo
            .write_shards
            .iter()
            .all(|shard| shard.lock().pending_sync.is_empty()));
        drop(undo);
        let reopened = open(dir.path());
        for id in ids {
            assert_eq!(
                reopened.load_owned(id).unwrap().before_values,
                record(1).before_values
            );
        }
    }

    #[test]
    fn checkpoint_sync_allows_explicit_gc_while_io_is_unlocked() {
        let dir = tempfile::tempdir().unwrap();
        let mut undo = open(dir.path());
        undo.set_segment_bytes(512).unwrap();
        let ids: Vec<_> = (0..128)
            .map(|_| undo.append_record(record(1), true).unwrap().0)
            .collect();
        let shard = undo
            .write_shards
            .iter()
            .find(|shard| shard.lock().pending_sync.len() > 1)
            .unwrap()
            .clone();
        UndoWriteShard::checkpoint_sync_with(&shard, || {
            for id in ids {
                undo.remove_record(id).unwrap();
            }
            let obsolete = undo.detach_empty_segments();
            assert!(!obsolete.is_empty());
            UndoManager::unlink_segments(&obsolete);
        })
        .unwrap();
        undo.checkpoint_sync().unwrap();
    }

    #[test]
    fn checkpoint_sync_covers_rotated_undo_and_retries_errors() {
        let dir = tempfile::tempdir().unwrap();
        let mut undo = open(dir.path());
        undo.set_segment_bytes(512).unwrap();
        let mut ids = Vec::new();
        for _ in 0..128 {
            ids.push(undo.append_record(record(1), true).unwrap().0);
        }
        let pending: usize = undo
            .write_shards
            .iter()
            .map(|s| s.lock().pending_sync.len())
            .sum();
        assert!(
            pending > undo.write_shards.len(),
            "must cover rotated segments as well as active ones"
        );
        // Force a lookup failure without privileged chmod or directory rename
        // semantics. Windows refuses renaming a directory with live writers.
        let original = undo.undo_dir.clone().unwrap();
        for shard in undo.write_shards.iter() {
            shard.lock().undo_dir = Some(original.join("missing"));
        }
        assert!(undo.checkpoint_sync().is_err());
        assert!(undo
            .write_shards
            .iter()
            .any(|s| !s.lock().pending_sync.is_empty()));
        for shard in undo.write_shards.iter() {
            shard.lock().undo_dir = Some(original.clone());
        }
        undo.checkpoint_sync().unwrap();
        assert!(undo
            .write_shards
            .iter()
            .all(|s| s.lock().pending_sync.is_empty()));
        drop(undo);
        let recovered = open(dir.path());
        for id in ids {
            assert_eq!(
                recovered.load_owned(id).unwrap().before_values,
                record(1).before_values
            );
        }
    }

    #[test]
    fn eviction_candidates_exclude_retained_metadata_and_survive_reload() {
        let dir = tempfile::tempdir().unwrap();
        let mut undo = open(dir.path());
        undo.set_mem_cache_limit_bytes(0);
        let mut ids = Vec::new();
        for i in 1..=512 {
            ids.push(undo.append_record(record(i), true).unwrap().0);
        }
        assert_eq!(undo.eviction_candidates.len(), 512);
        undo.remove_record(ids.pop().unwrap()).unwrap();
        assert_eq!(undo.eviction_candidates.len(), 511);
        undo.set_mem_cache_limit_bytes(1);
        assert!(undo.resident_bytes > 1, "metadata exceeds the cap");
        assert!(undo.eviction_candidates.is_empty());
        let evicted = undo.evicted_records;
        // Reading evicted payloads returns owned values, not resident cache
        // entries; rebuilding bookkeeping must not re-enqueue metadata.
        for id in ids {
            assert!(undo.load_owned(id).unwrap().before_values.is_some());
        }
        undo.refresh_metrics();
        assert!(undo.eviction_candidates.is_empty());
        for i in 513..=520 {
            let id = undo.append_record(record(i), true).unwrap().0;
            assert!(undo.eviction_candidates.is_empty());
            assert!(undo.load_owned(id).unwrap().before_values.is_some());
        }
        assert_eq!(undo.evicted_records, evicted + 8);
        // A payload-free INSERT remains resident and never becomes a victim.
        let mut insert = record(521);
        insert.kind = UndoKind::Insert;
        insert.before_values = None;
        let id = undo.append_record(insert, true).unwrap().0;
        assert!(undo.records[&id].resident);
        assert!(undo.eviction_candidates.is_empty());
    }

    fn active_segment_len(undo: &UndoManager) -> u64 {
        // Sum every shard's active segment file length. The pre-sharding
        // test inspected a single file; with N shards, "the active
        // segment hasn't been flushed yet" means EVERY shard's active
        // file still equals the bare header, and "post flush" means at
        // least one of them grew.
        undo.write_shards
            .iter()
            .map(|s| {
                let seg = s.lock().active_segment_id;
                let path = undo.segment_path(seg).unwrap();
                fs::metadata(path).map(|m| m.len()).unwrap_or(0)
            })
            .max()
            .unwrap_or(0)
    }

    /// M1 (P1-A/D) — `remove_record` computes the on-disk frame end from
    /// the cached `encoded_len` (no re-encode) and maintains `disk_bytes`
    /// incrementally. Both must stay bit-identical to the authoritative
    /// fold over `segment_file_bytes` across a LIFO rollback-style drain,
    /// and every emptied `created_index` bucket must be dropped.
    #[test]
    fn remove_record_keeps_disk_bytes_and_index_consistent() {
        let dir = tempfile::tempdir().unwrap();
        let mut undo = UndoManager::open_with_segments(&[], dir.path(), 4096).unwrap();
        // Pin to shard 0 so appends land contiguously and removals hit the
        // active-tail truncation branch (the path exercised by rollback).
        undo.next_shard.store(0, Ordering::Relaxed);
        let mut ids = Vec::new();
        for i in 0..30u64 {
            undo.next_shard.store(0, Ordering::Relaxed);
            let mut r = record(i + 1);
            // Vary the before-image width so encoded_len differs per record;
            // a stale/constant length would desync the truncation check.
            r.before_values = Some(vec![Value::String("y".repeat((i as usize % 7) + 1))]);
            let (id, _, _) = undo.append_record(r, true).unwrap();
            ids.push(id);
        }
        let fold = |u: &UndoManager| {
            u.segment_file_bytes
                .values()
                .copied()
                .fold(0u64, u64::saturating_add)
        };
        assert_eq!(
            undo.disk_bytes,
            fold(&undo),
            "disk_bytes drifted after append"
        );
        // LIFO drain (rollback order): the incrementally-maintained
        // disk_bytes must match the fold at every step.
        for id in ids.iter().rev() {
            undo.remove_record(*id).unwrap();
            assert_eq!(
                undo.disk_bytes,
                fold(&undo),
                "disk_bytes drifted after remove_record (id={id})"
            );
        }
        assert!(undo.records.is_empty(), "all records removed");
        assert!(
            undo.created_index.is_empty(),
            "emptied created_index buckets must be dropped"
        );
    }

    /// P1/10 — compaction selector: a segment with low live ratio
    /// becomes a candidate; the active segment of each shard is never
    /// a candidate. Fully-dead segments (live=0) are also excluded
    /// because `detach_empty_segments` already handles those.
    #[test]
    fn compaction_candidates_pick_low_live_ratio_segments() {
        let dir = tempfile::tempdir().unwrap();
        // Small segment_bytes so rotation kicks in after few appends.
        let mut undo = UndoTablespace::open_with_segments(&[], dir.path(), 512).unwrap();
        // Pin every append to shard 0 so we can reason about which
        // segment each record lands in. Otherwise round-robin spreads
        // the survivors so thinly that every segment ends up fully
        // dead (and is reclaimed by detach_empty_segments before
        // compaction looks).
        let pin = |u: &UndoManager| u.next_shard.store(0, Ordering::Relaxed);
        let mut ids = Vec::new();
        for i in 0..40u64 {
            pin(&undo);
            let mut r = record(i + 1);
            r.before_values = Some(vec![Value::String("x".repeat(40))]);
            let (id, _, _) = undo.append_record(r, true).unwrap();
            ids.push(id);
        }
        undo.flush().unwrap();
        assert!(
            undo.segment_count() > 1,
            "need multiple segments to test compaction"
        );
        // Remove every OTHER record so each segment ends up about
        // half-live — clearly below a 60% threshold without being
        // fully dead.
        for (i, id) in ids.iter().enumerate() {
            if i % 2 == 0 {
                undo.remove_record(*id).unwrap();
            }
        }
        let candidates = undo.compaction_candidates(60, 16);
        assert!(
            !candidates.is_empty(),
            "compaction_candidates should surface at least one half-live segment"
        );
        let active = undo.active_segment_ids();
        for id in &candidates {
            assert!(
                !active.contains(id),
                "candidate must not be any shard's active segment"
            );
            assert!(
                undo.segment_live_records.get(id).copied().unwrap_or(0) > 0,
                "candidate must still have live records (empty = detach handles)"
            );
        }
    }

    #[test]
    fn audit_compaction_partial_failure_preserves_old_pointers() {
        let dir = tempfile::tempdir().unwrap();
        let mut undo = UndoTablespace::open_with_segments(&[], dir.path(), 1024).unwrap();
        let mut pointers = Vec::new();
        for id in 0..256 {
            pointers.push(undo.append_record(record(id), true).unwrap().0);
        }
        undo.flush().unwrap();
        let seg = undo_ptr_segment(pointers[0]);
        let candidates: Vec<_> = pointers
            .iter()
            .copied()
            .filter(|p| undo_ptr_segment(*p) == seg)
            .collect();
        assert!(candidates.len() >= 2);
        assert!(!undo.active_segment_ids().contains(&seg));
        let failing_txn = undo.records[&candidates[1]].txn_id;
        let old_charge = undo.txn_bytes[&failing_txn];
        undo.set_txn_max_bytes(1 << 20);
        undo.txn_bytes.insert(failing_txn, 1 << 20);
        assert!(undo.compact_segments(&[seg]).is_err());
        assert!(
            !undo.records.contains_key(&candidates[0]),
            "first record must have moved before failure"
        );
        for ptr in pointers {
            assert!(undo.get(ptr).is_some(), "lost undo pointer {ptr}");
        }
        undo.txn_bytes.insert(failing_txn, old_charge);
        undo.set_txn_max_bytes(0);
        undo.compact_segments(&[seg]).unwrap();
        for ptr in candidates {
            assert!(undo.get(ptr).is_some());
        }
    }

    #[test]
    fn audit_undo_segment_offset_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let mut undo = UndoTablespace::open_with_segments(&[], dir.path(), 1024).unwrap();
        assert!(undo.set_segment_bytes(u32::MAX as u64 + 1).is_err());
        assert_eq!(undo.segment_bytes, 1024);
        undo.set_segment_bytes(u32::MAX as u64).unwrap();
        drop(undo);
        let reopened = UndoTablespace::open_with_segments(&[], dir.path(), 1024).unwrap();
        assert_eq!(reopened.segment_bytes, u32::MAX as u64);
        assert!(UndoTablespace::open_with_segments(&[], dir.path(), 1 << 33).is_err());
    }

    /// P1/10 — `compact_segments` rewrites every live record from the
    /// candidate segments into the tail and drops the old files.
    /// Chain integrity (prev_undo_ptr) is restored across the move.
    #[test]
    fn compact_segments_rewrites_live_records_and_drops_old_files() {
        let dir = tempfile::tempdir().unwrap();
        let mut undo = UndoTablespace::open_with_segments(&[], dir.path(), 512).unwrap();
        // Build a chain so we can assert prev_undo_ptr remap.
        let (a, _, _) = undo.append_record(record(7), true).unwrap();
        let mut b_rec = record(8);
        b_rec.prev_undo_ptr = Some(a);
        let (b, _, _) = undo.append_record(b_rec, true).unwrap();
        // Force enough segments that A's segment is no longer active.
        for i in 0..40u64 {
            let mut r = record(i + 100);
            r.before_values = Some(vec![Value::String("p".repeat(60))]);
            undo.append_record(r, true).unwrap();
        }
        undo.flush().unwrap();
        let active = undo.active_segment_ids();
        let target_seg = undo_ptr_segment(a);
        assert!(
            !active.contains(&target_seg),
            "A's segment should have rotated out of active"
        );
        let prev_count = undo.record_count();
        let candidates = vec![target_seg];
        let (bytes_freed, remap) = undo.compact_segments(&candidates).unwrap();
        assert!(bytes_freed > 0, "compaction must report bytes freed");
        assert!(
            remap.iter().any(|(old, _)| *old == a),
            "record A must be remapped"
        );
        // Chain integrity: B's prev_undo_ptr is left at A's OLD id on the
        // record itself, but forwards to A's new home (P1/10 — referrers
        // are not rewritten in place; the forwarding table resolves them).
        let new_a = remap.iter().find(|(old, _)| *old == a).unwrap().1;
        let b_rec_after = undo.records.get(&b).expect("B is still alive");
        assert_eq!(b_rec_after.prev_undo_ptr, Some(a));
        assert_eq!(undo.resolve_forwarded(a), Some(new_a));
        assert_eq!(
            undo.get(b_rec_after.prev_undo_ptr.unwrap())
                .expect("B's prev forwards to A's home")
                .txn_id,
            7
        );
        // Record count unchanged (rewrite, not purge).
        assert_eq!(undo.record_count(), prev_count);
        // Old segment file is gone.
        let undo_dir = crate::storage::naming::system_undo_dir(dir.path());
        assert!(
            !undo_dir.join(segment_file_name(target_seg)).exists(),
            "compacted segment file must be unlinked"
        );
    }

    /// P1/10 build-then-swap — helper: append a chained scenario (A ← B)
    /// surrounded by filler, rotate A's segment out of active, then
    /// delete A's segment-mates so that segment is a genuine low-live-
    /// ratio compaction candidate (A and B stay live). Pinned to shard 0
    /// so two instances produce byte-identical layouts. Returns
    /// `(undo, a_ptr, b_ptr, candidate_segment)`.
    fn populate_compaction_scenario(dir: &Path) -> (UndoManager, u64, u64, u32) {
        let mut undo = UndoTablespace::open_with_segments(&[], dir, 512).unwrap();
        let pin = |u: &UndoManager| u.next_shard.store(0, Ordering::Relaxed);
        // Pre-fillers so A's segment carries removable segment-mates.
        for i in 0..6u64 {
            pin(&undo);
            let mut r = record(i + 200);
            r.before_values = Some(vec![Value::String("q".repeat(60))]);
            undo.append_record(r, true).unwrap();
        }
        undo.write_shards[0].lock().rotate().unwrap();
        pin(&undo);
        let mut dead = record(250);
        dead.before_values = Some(vec![Value::String("q".repeat(60))]);
        undo.append_record(dead, true).unwrap();
        pin(&undo);
        let (a, _, _) = undo.append_record(record(7), true).unwrap();
        pin(&undo);
        let mut b_rec = record(8);
        b_rec.prev_undo_ptr = Some(a);
        let (b, _, _) = undo.append_record(b_rec, true).unwrap();
        for i in 0..40u64 {
            pin(&undo);
            let mut r = record(i + 100);
            r.before_values = Some(vec![Value::String("p".repeat(60))]);
            undo.append_record(r, true).unwrap();
        }
        undo.flush().unwrap();
        let target_seg = undo_ptr_segment(a);
        assert!(
            !undo.active_segment_ids().contains(&target_seg),
            "A's segment should have rotated out of active"
        );
        // Delete every record in A's segment except A and B → low ratio.
        let victims: Vec<u64> = undo
            .records
            .keys()
            .copied()
            .filter(|p| undo_ptr_segment(*p) == target_seg && *p != a && *p != b)
            .collect();
        assert!(
            !victims.is_empty(),
            "candidate segment must have removable segment-mates"
        );
        for v in victims {
            undo.remove_record(v).unwrap();
        }
        (undo, a, b, target_seg)
    }

    /// Canonical identity of a surviving record, independent of its
    /// (possibly relocated) ptr — for differential comparison. `Value`
    /// is not `Ord`, so the before-image is rendered to a stable string.
    fn survivor_key(rec: &UndoRecord) -> (u64, u64, u16, String) {
        (
            rec.txn_id,
            rec.table_id,
            rec.row_id.slot_id,
            format!("{:?}", rec.before_values),
        )
    }

    fn survivor_multiset(undo: &UndoManager) -> Vec<(u64, u64, u16, String)> {
        let mut v: Vec<_> = undo
            .records
            .values()
            .map(|record| survivor_key(record))
            .collect();
        v.sort();
        v
    }

    /// Every prev_undo_ptr must resolve to a live record (chain integrity).
    /// Since P1/10 the referrer's `prev_undo_ptr` is left pointing at the
    /// relocated record's OLD ptr; the compaction forwarding table is what
    /// maps it to the live home, so a direct `records` hit OR a forwarded
    /// hit both count as "resolves".
    fn assert_chains_resolve(undo: &UndoManager) {
        for rec in undo.records.values() {
            if let Some(prev) = rec.prev_undo_ptr {
                let home = undo.resolve_forwarded(prev).unwrap_or(prev);
                assert!(
                    undo.records.contains_key(&home),
                    "dangling prev_undo_ptr {prev} (home {home}) after compaction"
                );
            }
        }
    }

    /// P1/10 — build-then-swap must produce the SAME surviving state as
    /// the legacy in-lock rewrite: identical surviving record multiset,
    /// intact chains, candidate file dropped, output in the reserved lane.
    #[test]
    fn build_swap_matches_in_lock_compaction() {
        // Reference: legacy in-lock path.
        let dir_ref = tempfile::tempdir().unwrap();
        let (mut undo_ref, _a_ref, _b_ref, seg_ref) = populate_compaction_scenario(dir_ref.path());
        let count_ref = undo_ref.record_count();
        let (_, remap_ref) = undo_ref.compact_segments(&[seg_ref]).unwrap();
        assert!(!remap_ref.is_empty(), "oracle must relocate records");
        assert_chains_resolve(&undo_ref);

        // Subject: build-then-swap path.
        let dir = tempfile::tempdir().unwrap();
        let (mut undo, a, b, _seg) = populate_compaction_scenario(dir.path());
        let count_before = undo.record_count();
        assert_eq!(count_before, count_ref, "identical starting states");
        let plan = undo
            .plan_compaction(60, 16)
            .expect("a candidate must exist");
        let built = UndoManager::build_compaction_segments(&plan).unwrap();
        assert!(!built.is_empty());
        let (bytes_freed, remap) = undo.commit_compaction(plan, built).unwrap();

        assert!(bytes_freed > 0, "compaction must report bytes freed");
        assert_eq!(undo.record_count(), count_before, "rewrite, not purge");
        // Same surviving state as the in-lock oracle.
        assert_eq!(survivor_multiset(&undo), survivor_multiset(&undo_ref));
        assert_chains_resolve(&undo);
        let new_a = remap.iter().find(|(o, _)| *o == a).unwrap().1;
        assert_eq!(
            shard_of_segment(undo_ptr_segment(new_a)),
            COMPACTION_SHARD_LANE,
            "relocated record must live in the compaction lane"
        );
        let _ = b;
        // B (txn 8) still chains to A's new location: its `prev_undo_ptr`
        // holds A's OLD ptr, which forwards to A's relocated home.
        let b_after = undo.records.values().find(|r| r.txn_id == 8).unwrap();
        assert_eq!(b_after.prev_undo_ptr, Some(a));
        assert_eq!(undo.resolve_forwarded(a), Some(new_a));
        assert_eq!(undo.records.get(&new_a).unwrap().txn_id, 7);
        // Old candidate file gone; no orphan temp file remains.
        let undo_dir = crate::storage::naming::system_undo_dir(dir.path());
        assert!(!undo_dir
            .join(segment_file_name(undo_ptr_segment(a)))
            .exists());
        let leftover = fs::read_dir(&undo_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .any(|e| e.file_name().to_string_lossy().ends_with(".compacting"));
        assert!(!leftover, "no temp segment should survive commit");
    }

    /// P1/10 — after a build-then-swap compaction the relocated records
    /// must reload from the new lane-15 segment on reopen (recovery reads
    /// it as an ordinary frozen data segment), that segment must NOT be
    /// resumed as any shard's active segment, and the reopened state must
    /// match what the legacy in-lock path leaves behind (differential —
    /// both paths remap referrers' prev only in memory, so on-disk chains
    /// reload identically).
    #[test]
    fn build_swap_survives_reopen() {
        // Reference: in-lock compaction, then reopen.
        let dir_ref = tempfile::tempdir().unwrap();
        let (mut undo_ref, _a_ref, _b_ref, seg_ref) = populate_compaction_scenario(dir_ref.path());
        undo_ref.compact_segments(&[seg_ref]).unwrap();
        undo_ref.flush().unwrap();
        drop(undo_ref);
        let reopened_ref =
            UndoTablespace::open_with_segments(&[], dir_ref.path(), DEFAULT_UNDO_SEGMENT_BYTES)
                .unwrap();

        // Subject: build-then-swap, then reopen.
        let dir = tempfile::tempdir().unwrap();
        let (mut undo, a, _b, _seg) = populate_compaction_scenario(dir.path());
        let plan = undo.plan_compaction(60, 16).unwrap();
        let built = UndoManager::build_compaction_segments(&plan).unwrap();
        let (_, remap) = undo.commit_compaction(plan, built).unwrap();
        let new_a = remap.iter().find(|(o, _)| *o == a).unwrap().1;
        undo.flush().unwrap();
        drop(undo);
        let reopened =
            UndoTablespace::open_with_segments(&[], dir.path(), DEFAULT_UNDO_SEGMENT_BYTES)
                .unwrap();

        // Relocated A reloads at its new lane-15 ptr with the right payload.
        assert_eq!(
            shard_of_segment(undo_ptr_segment(new_a)),
            COMPACTION_SHARD_LANE
        );
        assert_eq!(reopened.get(new_a).expect("relocated A reloads").txn_id, 7);
        // Reopened survivor set matches the in-lock oracle's reopened set.
        assert_eq!(
            survivor_multiset(&reopened),
            survivor_multiset(&reopened_ref)
        );
        // The compaction lane is never resumed for foreground append.
        for shard_mu in reopened.write_shards.iter() {
            assert_ne!(
                shard_of_segment(shard_mu.lock().active_segment_id),
                COMPACTION_SHARD_LANE,
                "compaction lane must never become an active append segment"
            );
        }
    }

    /// P1/10 — drift guard: if a planned record is removed during the
    /// off-lock build window, the swap aborts wholesale (no partial
    /// apply), leaves the originals intact, and unlinks the temp file.
    #[test]
    fn build_swap_aborts_on_concurrent_removal() {
        let dir = tempfile::tempdir().unwrap();
        let (mut undo, a, _b, _seg) = populate_compaction_scenario(dir.path());
        let count_before = undo.record_count();
        let plan = undo.plan_compaction(60, 16).unwrap();
        let built = UndoManager::build_compaction_segments(&plan).unwrap();
        assert!(!built.is_empty());
        // Simulate concurrent purge/rollback dropping a planned original.
        undo.remove_record(a).unwrap();
        let (bytes_freed, remap) = undo.commit_compaction(plan, built).unwrap();
        assert_eq!(bytes_freed, 0, "drift must abort with zero freed");
        assert!(remap.is_empty(), "drift must produce no remap");
        // Only the record we explicitly removed is gone; no partial apply.
        assert_eq!(undo.record_count(), count_before - 1);
        // No relocated copies leaked into the records map.
        assert!(
            !undo
                .records
                .keys()
                .any(|p| shard_of_segment(undo_ptr_segment(*p)) == COMPACTION_SHARD_LANE),
            "aborted swap must not insert any relocated (lane-15) record"
        );
        let undo_dir = crate::storage::naming::system_undo_dir(dir.path());
        let leftover = fs::read_dir(&undo_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .any(|e| e.file_name().to_string_lossy().ends_with(".compacting"));
        assert!(!leftover, "aborted build must unlink its temp segment");
    }

    /// P1/10 — the compaction forwarding table lets a stale ptr (a heap
    /// header / before_header pointer that compaction cannot rewrite in
    /// place) resolve to the record's relocated home; and once the
    /// relocated record is itself removed, the stale ptr safely resolves
    /// to `None` (no alias to a wrong record).
    #[test]
    fn build_swap_forwards_stale_ptr_then_prunes() {
        let dir = tempfile::tempdir().unwrap();
        let (mut undo, a, _b, _seg) = populate_compaction_scenario(dir.path());
        // Before compaction the original ptr resolves directly.
        assert_eq!(undo.get(a).unwrap().txn_id, 7);
        let plan = undo.plan_compaction(60, 16).unwrap();
        let built = UndoManager::build_compaction_segments(&plan).unwrap();
        let (_, remap) = undo.commit_compaction(plan, built).unwrap();
        let new_a = remap.iter().find(|(o, _)| *o == a).unwrap().1;
        assert_ne!(a, new_a, "record must have relocated");
        // Stale original ptr now transparently forwards to the new home.
        assert!(!undo.records.contains_key(&a), "old ptr is gone from map");
        assert_eq!(
            undo.get(a).expect("forwarding resolves stale ptr").txn_id,
            7
        );
        assert_eq!(undo.load_owned(a).expect("forwarded load_owned").txn_id, 7);
        // Remove the relocated record (as purge would once unpinned): the
        // stale ptr must now resolve to None, never to a wrong record.
        undo.remove_record(new_a).unwrap();
        assert!(
            undo.get(a).is_none(),
            "stale ptr must not resolve after its target is purged"
        );
    }

    /// A heap row header keeps the undo pointer that was current when the row
    /// version was written; compaction cannot rewrite that external pointer.
    /// If its record is compacted twice, the original alias must be compressed
    /// from A→B to A→C rather than pruned when B disappears. Losing A is the
    /// long-running OLTP failure that surfaced as a false snapshot-too-old.
    #[test]
    fn build_swap_preserves_original_ptr_across_repeated_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let (mut undo, original_a, original_b, _seg) = populate_compaction_scenario(dir.path());

        let first_plan = undo.plan_compaction(60, 16).unwrap();
        let first_built = UndoManager::build_compaction_segments(&first_plan).unwrap();
        let (_, first_remap) = undo.commit_compaction(first_plan, first_built).unwrap();
        let moved_a = first_remap
            .iter()
            .find(|(old, _)| *old == original_a)
            .unwrap()
            .1;
        let moved_b = first_remap
            .iter()
            .find(|(old, _)| *old == original_b)
            .unwrap()
            .1;
        assert_eq!(undo.resolve_forwarded(original_a), Some(moved_a));

        // Leave A as the sole live record in its frozen compaction output
        // segment. Header/frame overhead makes that segment <100% live and
        // therefore a deterministic candidate for a second relocation.
        undo.remove_record(moved_b).unwrap();
        let second_plan = undo
            .plan_compaction(100, 16)
            .expect("first compaction output must be compactable again");
        assert!(second_plan.relocates_ptr(moved_a));
        let second_built = UndoManager::build_compaction_segments(&second_plan).unwrap();
        let (_, second_remap) = undo.commit_compaction(second_plan, second_built).unwrap();
        let newest_a = second_remap
            .iter()
            .find(|(old, _)| *old == moved_a)
            .unwrap()
            .1;

        assert_eq!(undo.resolve_forwarded(original_a), Some(newest_a));
        assert_eq!(undo.resolve_forwarded(moved_a), Some(newest_a));
        assert_eq!(
            undo.get(original_a)
                .expect("original heap pointer must resolve after two compactions")
                .txn_id,
            7
        );
    }

    /// P1/10 lock-hold opt — RED LINE. After compaction relocates a
    /// record `A` that an active transaction's undo chain still reaches
    /// (via a referrer `B` whose `prev_undo_ptr` we leave pointing at A's
    /// OLD ptr), the purge keep-set walk MUST resolve that stale prev
    /// through the forwarding table and protect A's relocated home. Here
    /// A's creator is deliberately `RolledBack` so the creator-state gate
    /// alone would let A be purged — only the forwarding-aware keep-set
    /// stands between the still-referenced record and a wrongful purge
    /// (snapshot-too-old / data corruption). Fails if the keep-set walk in
    /// `refresh_keep_cache` descends the raw (dead) old ptr.
    #[test]
    fn compaction_keep_set_prevents_wrongful_purge_of_referenced_record() {
        let dir = tempfile::tempdir().unwrap();
        let (mut undo, a, b, _seg) = populate_compaction_scenario(dir.path());
        let plan = undo.plan_compaction(60, 16).unwrap();
        let built = UndoManager::build_compaction_segments(&plan).unwrap();
        let (_, remap) = undo.commit_compaction(plan, built).unwrap();
        let new_a = remap.iter().find(|(o, _)| *o == a).unwrap().1;
        // B is in the same candidate segment as A, so it relocates too; its
        // `prev_undo_ptr` is left holding A's OLD ptr, which must forward to
        // A's relocated home.
        let new_b = remap.iter().find(|(o, _)| *o == b).unwrap().1;
        assert_ne!(new_a, a, "A must have relocated");
        assert_eq!(undo.records.get(&new_b).unwrap().prev_undo_ptr, Some(a));
        assert_eq!(undo.resolve_forwarded(a), Some(new_a));
        // txn 8 (B) is the sole active chain; txn 7 (A) is rolled-back so
        // the creator-state gate alone would purge A.
        let mut creators = BTreeMap::new();
        creators.insert(7u64, UndoCreatorState::RolledBack);
        creators.insert(8u64, UndoCreatorState::Active);
        // Drive the keep-set from B's OLD head ptr — it must forward to
        // new_b just as an un-remapped active undo_head would after a
        // compaction that raced ahead of `remap_active_undo_heads`.
        let victims =
            undo.plan_purge_victims(&[(8, b)], u64::MAX, None, &creators, u64::MAX, usize::MAX);
        assert!(
            !victims.contains(&new_a),
            "relocated record still reachable from an active chain must NOT be purged (got victims {victims:?}, new_a={new_a})"
        );
        assert!(!victims.contains(&b), "the active head must not be purged");
        // And the keep-set itself must carry A's live home, never the dead ptr.
        undo.refresh_keep_cache(&[(8, b)]);
        let cache = undo.keep_cache.lock();
        assert!(
            cache.flat_keep.contains(&new_a),
            "keep-set must forward B.prev to A's relocated home"
        );
        assert!(
            !cache.flat_keep.contains(&a),
            "keep-set must not retain the dead pre-compaction ptr"
        );
    }

    /// P1/10 lock-hold opt — recovery neutrality. Removing the in-memory
    /// full-table `prev_undo_ptr` remap must not change anything a crash /
    /// reopen can observe: recovery rebuilds `records` from the on-disk
    /// segments, where referrers already carry their ORIGINAL (pre-
    /// compaction) prev in BOTH the in-lock oracle and the build-then-swap
    /// path (compaction never rewrites referrers). This differential
    /// pins that equivalence.
    #[test]
    fn compaction_reopen_referrer_prev_is_forwarding_neutral() {
        // In-lock oracle: compact, flush, reopen.
        let dir_ref = tempfile::tempdir().unwrap();
        let (mut undo_ref, a_ref, b_ref, seg_ref) = populate_compaction_scenario(dir_ref.path());
        undo_ref.compact_segments(&[seg_ref]).unwrap();
        undo_ref.flush().unwrap();
        drop(undo_ref);
        let reopened_ref =
            UndoTablespace::open_with_segments(&[], dir_ref.path(), DEFAULT_UNDO_SEGMENT_BYTES)
                .unwrap();

        // Build-then-swap: compact, flush, reopen.
        let dir = tempfile::tempdir().unwrap();
        let (mut undo, a, b, _seg) = populate_compaction_scenario(dir.path());
        let plan = undo.plan_compaction(60, 16).unwrap();
        let built = UndoManager::build_compaction_segments(&plan).unwrap();
        undo.commit_compaction(plan, built).unwrap();
        undo.flush().unwrap();
        drop(undo);
        let reopened =
            UndoTablespace::open_with_segments(&[], dir.path(), DEFAULT_UNDO_SEGMENT_BYTES)
                .unwrap();

        // Identical populate ⇒ identical original ptrs for A and B.
        assert_eq!(a, a_ref);
        assert_eq!(b, b_ref);
        let _ = b_ref;
        // B relocates (same candidate segment as A) — its new ptr differs
        // between the two compaction strategies (the in-lock oracle appends
        // relocated records round-robin, build-swap uses the compaction
        // lane), so find the referrer by its txn id. After reopen there is
        // no forwarding table; the referrer's on-disk prev is A's ORIGINAL
        // ptr in BOTH paths — the recovery-visible chain is identical.
        let ref_prev = |u: &UndoManager| -> Option<u64> {
            u.records
                .values()
                .find(|r| r.txn_id == 8)
                .expect("referrer B reloads")
                .prev_undo_ptr
        };
        assert_eq!(
            ref_prev(&reopened),
            ref_prev(&reopened_ref),
            "referrer's on-disk prev must match the oracle on reopen"
        );
        assert_eq!(
            ref_prev(&reopened),
            Some(a),
            "referrer prev is A's original ptr, unchanged by either path"
        );
        // Whole surviving state (identity, ignoring relocated ptrs) matches.
        assert_eq!(
            survivor_multiset(&reopened),
            survivor_multiset(&reopened_ref)
        );
    }

    /// P1/10 — measure the write-lock-held portion of each compaction
    /// strategy on a large fragmented candidate set. Build-then-swap does
    /// the read + re-encode + segment write + fsync OFF the lock and holds
    /// it only for the map swap, so its lock-held time is a fraction of
    /// the legacy in-lock rewrite. `#[ignore]`d (timing-dependent); run
    /// with `cargo nextest run --run-ignored all build_swap_lock_hold`.
    #[test]
    #[ignore = "perf measurement; run explicitly"]
    fn build_swap_lock_hold_is_shorter_than_in_lock() {
        use std::time::Instant;
        // Build a big, heavily fragmented tablespace: many wide records
        // pinned to shard 0 across many 64 KiB segments, then delete most
        // so every non-active segment is a low-live-ratio candidate.
        let build_fixture = |dir: &Path| -> UndoManager {
            let mut undo = UndoTablespace::open_with_segments(&[], dir, 64 * 1024).unwrap();
            let mut ids = Vec::new();
            for i in 0..4000u64 {
                undo.next_shard.store(0, Ordering::Relaxed);
                let mut r = record(i + 1);
                r.before_values = Some(vec![Value::String("x".repeat(400))]);
                let (id, _, _) = undo.append_record(r, true).unwrap();
                ids.push(id);
            }
            undo.flush().unwrap();
            for (i, id) in ids.iter().enumerate() {
                if i % 4 != 0 {
                    undo.remove_record(*id).unwrap();
                }
            }
            undo
        };

        let dir_a = tempfile::tempdir().unwrap();
        let mut in_lock = build_fixture(dir_a.path());
        let candidates = in_lock.compaction_candidates(90, usize::MAX);
        let t = Instant::now();
        let _ = in_lock.compact_segments(&candidates).unwrap();
        let in_lock_hold = t.elapsed();

        let dir_b = tempfile::tempdir().unwrap();
        let bs = build_fixture(dir_b.path());
        let plan = bs.plan_compaction(90, usize::MAX).unwrap();
        // Build phase runs OFF the write lock.
        let t = Instant::now();
        let built = UndoManager::build_compaction_segments(&plan).unwrap();
        let build_off_lock = t.elapsed();
        // Only this commit holds the write lock.
        let mut bs = bs;
        let t = Instant::now();
        let _ = bs.commit_compaction(plan, built).unwrap();
        let swap_hold = t.elapsed();

        eprintln!(
            "in-lock hold={:?}  |  build-swap: off-lock build={:?} + lock-held swap={:?}",
            in_lock_hold, build_off_lock, swap_hold
        );
        // The bulk of the work (read + re-encode + segment write + fsync)
        // now runs OFF the write lock, and the lock-held swap is strictly
        // below the legacy in-lock hold. The absolute gap grows with
        // segment size / real segment IO cost (this fixture is tiny), so
        // these are conservative lower bounds on the production win.
        assert!(
            build_off_lock > swap_hold,
            "the heavy rewrite must dominate the off-lock phase, not the swap"
        );
        assert!(
            swap_hold < in_lock_hold,
            "lock-held swap ({swap_hold:?}) must be below in-lock rewrite ({in_lock_hold:?})"
        );

        // P1/10 lock-hold opt — the commit_compaction swap used to scan the
        // WHOLE surviving records map to remap `prev_undo_ptr` (O(total)).
        // That scan is gone; the swap is now O(relocated). Demonstrate by
        // holding the relocated (candidate) set FIXED while growing the
        // count of NON-candidate survivor records by 40×: the lock-held
        // swap time must stay roughly flat instead of scaling with the
        // survivor population.
        let commit_hold_for = |survivors: u64| -> (std::time::Duration, usize, usize) {
            let dir = tempfile::tempdir().unwrap();
            let mut u = UndoTablespace::open_with_segments(&[], dir.path(), 64 * 1024).unwrap();
            // A small, fixed fragmented candidate region on shard 0.
            let mut cand_ids = Vec::new();
            for i in 0..400u64 {
                u.next_shard.store(0, Ordering::Relaxed);
                let mut r = record(i + 1);
                r.before_values = Some(vec![Value::String("x".repeat(400))]);
                let (id, _, _) = u.append_record(r, true).unwrap();
                cand_ids.push(id);
            }
            // Many LIVE, non-candidate survivors spread across the other
            // shards (never selected for compaction, but walked by the old
            // O(total) remap). Keep them dense so their segments stay high
            // live-ratio and out of the candidate set.
            for i in 0..survivors {
                let _ = u.append_record(record(1_000_000 + i), true).unwrap();
            }
            u.flush().unwrap();
            // Fragment ONLY the candidate region (delete 3 of every 4).
            for (i, id) in cand_ids.iter().enumerate() {
                if i % 4 != 0 {
                    u.remove_record(*id).unwrap();
                }
            }
            let total = u.record_count();
            let plan = u.plan_compaction(90, usize::MAX).unwrap();
            let moves = plan.moves.len();
            let built = UndoManager::build_compaction_segments(&plan).unwrap();
            let t = Instant::now();
            let _ = u.commit_compaction(plan, built).unwrap();
            (t.elapsed(), moves, total)
        };
        let (small_hold, small_moves, small_total) = commit_hold_for(500);
        let (large_hold, large_moves, large_total) = commit_hold_for(20_000);
        eprintln!(
            "commit swap-hold: survivors≈{small_total} (moves={small_moves}) => {small_hold:?}  |  \
             survivors≈{large_total} (moves={large_moves}) => {large_hold:?}"
        );
        assert_eq!(
            small_moves, large_moves,
            "the relocated set must be identical so only survivor count varies"
        );
        // 40× more survivors; an O(total) scan would blow the swap-hold up
        // proportionally. Allow generous slack for allocator / timer noise
        // on this tiny in-memory fixture — the point is it does NOT scale
        // with survivors.
        assert!(
            large_hold <= small_hold * 8 + std::time::Duration::from_millis(2),
            "swap-hold must not scale with non-relocated survivor count \
             (small={small_hold:?}, large={large_hold:?})"
        );
    }

    /// P1/6 — round-robin append must spread frames across every
    /// configured rollback-segment shard, and each shard's active
    /// segment must have a distinct on-disk file (no shared file
    /// handle / no shared segment_id).
    #[test]
    fn unpublished_segment_lease_fences_compaction_and_empty_retirement() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let mut undo = open(dir.path());
        let (seed, _, _) = undo.append_record(record(1), true).unwrap();
        let segment = undo_ptr_segment(seed);
        let lane = undo.write_shards[shard_of_segment(segment) as usize].clone();
        let mut pending = record(2);
        pending.before_values = Some(vec![Value::String("unpublished".repeat(1000))]);
        let mut bytes = encode_undo_record(&pending).unwrap();
        let (_, _, end, lease) =
            UndoManager::write_leased_persistent_frame(&lane, &mut bytes).unwrap();
        lane.lock().rotate().unwrap();
        // Model a later completed publisher already advancing the tracked tail
        // past this still-private frame. Lease protection must be independent
        // of both the canonical record count and that allocation watermark.
        undo.segment_file_bytes.insert(segment, end);
        assert!(!undo.compaction_candidates(99, 100).contains(&segment));
        undo.remove_record(seed).unwrap();
        let path = undo
            .undo_dir
            .as_ref()
            .unwrap()
            .join(segment_file_name(segment));
        assert!(!undo.detach_empty_segments().contains(&path));
        assert!(path.exists());
        drop(lease);
        assert!(undo.detach_empty_segments().contains(&path));
    }

    #[test]
    fn round_robin_spreads_pushes_across_shards() {
        let dir = tempfile::tempdir().unwrap();
        let mut undo = open(dir.path());
        undo.set_buffer_limit(0).unwrap(); // write-through so we can stat sizes
        let shards = undo.write_shards.len();
        assert!(shards >= 2, "default SHARD_COUNT must be >= 2");
        // Push enough records that each shard is hit at least twice.
        for i in 0..(shards as u64 * 4) {
            undo.append_record(record(i + 1), true).unwrap();
        }
        undo.flush().unwrap();
        // Every shard's active segment file must exist AND be > header.
        let undo_dir = crate::storage::naming::system_undo_dir(dir.path());
        let mut nonempty = 0;
        for shard_mu in undo.write_shards.iter() {
            let seg = shard_mu.lock().active_segment_id;
            // Sharded segment ids must encode the owning shard in the
            // low SHARD_BITS — the inverse map is the contract.
            let derived_shard = shard_of_segment(seg);
            assert!(
                (derived_shard as usize) < shards,
                "shard_of_segment({seg}) = {derived_shard} out of range"
            );
            let path = undo_dir.join(segment_file_name(seg));
            let len = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            if len > UNDO_SEGMENT_HEADER_LEN {
                nonempty += 1;
            }
        }
        // With 4N pushes and N shards, every shard must have grown.
        assert_eq!(
            nonempty, shards,
            "expected every shard's active segment to receive frames; only {nonempty}/{shards} did"
        );
    }

    #[test]
    fn concurrent_persistent_append_publishes_all_records() {
        let dir = tempfile::tempdir().unwrap();
        let undo = std::sync::Arc::new(parking_lot::RwLock::new(open(dir.path())));
        {
            let mut guard = undo.write();
            guard.set_buffer_limit(0).unwrap(); // make segment sizes visible immediately
                                                // A resident cap must not disable the per-shard append path. The
                                                // test is intentionally tiny so concurrent publishes cross the
                                                // cap and exercise post-publish batch eviction as well.
            guard.set_mem_cache_limit_bytes(4 * 1024);
            assert!(guard.concurrent_persistent_append_eligible());
        }

        let threads = 8u64;
        let per_thread = 64u64;
        let mut handles = Vec::new();
        for t in 0..threads {
            let undo = std::sync::Arc::clone(&undo);
            handles.push(std::thread::spawn(move || {
                let mut ids = Vec::new();
                for i in 0..per_thread {
                    let txn = t * 10_000 + i + 1;
                    let rec = record(txn);
                    let mut bytes = encode_undo_record(&rec).unwrap();
                    let (shard, reservation) = {
                        let guard = undo.read();
                        (
                            guard
                                .begin_concurrent_persistent_append()
                                .unwrap()
                                .expect("concurrent append eligible"),
                            guard.reserve_record_resident(&rec).unwrap(),
                        )
                    };
                    let (ptr, segment_id, segment_end) =
                        UndoManager::write_concurrent_persistent_frame(&shard, &mut bytes).unwrap();
                    let (id, _, _) = {
                        let mut guard = undo.write();
                        guard
                            .finish_concurrent_persistent_append(
                                rec,
                                bytes,
                                ptr,
                                segment_id,
                                segment_end,
                                reservation,
                            )
                            .unwrap()
                    };
                    ids.push(id);
                }
                ids
            }));
        }

        let mut all_ids = Vec::new();
        for handle in handles {
            all_ids.extend(handle.join().unwrap());
        }
        all_ids.sort_unstable();
        all_ids.dedup();

        let mut guard = undo.write();
        guard.flush().unwrap();
        assert_eq!(all_ids.len() as u64, threads * per_thread);
        assert_eq!(guard.record_count(), (threads * per_thread) as usize);
        assert!(
            guard.evicted_records() > 0,
            "bounded concurrent append must evict old resident payloads"
        );
        for id in all_ids {
            let loaded = guard
                .load_owned(id)
                .unwrap_or_else(|| panic!("concurrently appended undo record {id} was lost"));
            assert!(
                loaded.before_values.is_some(),
                "evicted record {id} must page its before-image back from disk"
            );
        }
    }

    #[test]
    fn out_of_order_concurrent_publish_keeps_segment_tail_monotone() {
        let dir = tempfile::tempdir().unwrap();
        let mut undo = open(dir.path());
        undo.set_buffer_limit(0).unwrap();

        let shard = std::sync::Arc::clone(&undo.write_shards[0]);
        let first_record = record(1);
        let first_reservation = undo.reserve_record_resident(&first_record).unwrap();
        let mut first_bytes = encode_undo_record(&first_record).unwrap();
        let (first_ptr, segment_id, first_end) =
            UndoManager::write_concurrent_persistent_frame(&shard, &mut first_bytes).unwrap();

        let second_record = record(2);
        let second_reservation = undo.reserve_record_resident(&second_record).unwrap();
        let mut second_bytes = encode_undo_record(&second_record).unwrap();
        let (second_ptr, second_segment_id, second_end) =
            UndoManager::write_concurrent_persistent_frame(&shard, &mut second_bytes).unwrap();
        assert_eq!(second_segment_id, segment_id);
        assert!(second_end > first_end);

        // Publish in the reverse of the shard write order, matching the race
        // where the first writer is descheduled before taking the manager
        // write lock.
        undo.finish_concurrent_persistent_append(
            second_record,
            second_bytes,
            second_ptr,
            second_segment_id,
            second_end,
            second_reservation,
        )
        .unwrap();
        undo.finish_concurrent_persistent_append(
            first_record,
            first_bytes,
            first_ptr,
            segment_id,
            first_end,
            first_reservation,
        )
        .unwrap();
        assert_eq!(undo.segment_file_bytes.get(&segment_id), Some(&second_end));

        // Force both payloads out of the resident cache. They must still page
        // in from their durable frames, including the later offset.
        undo.set_mem_cache_limit_bytes(1);
        undo.evict_to_cache_budget();
        assert!(undo.load_owned(first_ptr).is_some());
        assert!(undo.load_owned(second_ptr).is_some());
    }

    /// P1/6 — recovery after a crash that left buffered tails in each
    /// shard must repair every shard's segment from the WAL twins, not
    /// just shard 0.
    #[test]
    fn wal_repair_handles_all_shards() {
        let dir = tempfile::tempdir().unwrap();
        let mut undo = open(dir.path());
        undo.set_buffer_limit(1024 * 1024).unwrap();
        let mut wal = Vec::new();
        let mut expected_ids = Vec::new();
        for i in 1..=(undo.write_shards.len() as u64 * 3) {
            let (id, bytes, _) = undo.append_record(record(i), true).unwrap();
            expected_ids.push(id);
            wal.push(wal_twin(i, id, bytes));
        }
        drop(undo); // simulate crash with buffered tail loss
        let recovered =
            UndoManager::open_with_segments(&wal, dir.path(), DEFAULT_UNDO_SEGMENT_BYTES).unwrap();
        assert_eq!(recovered.len(), expected_ids.len());
        for id in &expected_ids {
            assert!(
                recovered.records.contains_key(id),
                "id {id} (shard {}) lost in WAL repair",
                shard_of_segment(*id as u32)
            );
        }
    }

    #[test]
    fn recovery_retained_frame_does_not_require_unlogged_predecessor() {
        let dir = tempfile::tempdir().unwrap();
        let later_ptr;
        let later_bytes;
        {
            let mut undo = open(dir.path());
            undo.set_buffer_limit(1024 * 1024).unwrap();
            let shard = Arc::clone(&undo.write_shards[0]);
            let mut earlier = encode_undo_record(&record(1)).unwrap();
            UndoManager::write_concurrent_persistent_frame(&shard, &mut earlier).unwrap();
            let mut later = encode_undo_record(&record(2)).unwrap();
            later_ptr = UndoManager::write_concurrent_persistent_frame(&shard, &mut later)
                .unwrap()
                .0;
            later_bytes = later;
            // Txn 1 has not flushed its transaction-local WAL; txn 2 has.
            // Neither frame has reached the buffered segment file at crash.
        }
        let wal = vec![wal_twin(1, later_ptr, later_bytes)];
        let recovered = UndoManager::open_with_segments_for_recovery(
            &wal,
            dir.path(),
            DEFAULT_UNDO_SEGMENT_BYTES,
            &HashSet::from([2]),
            false,
        )
        .unwrap();
        assert_eq!(
            recovered.load_owned(later_ptr).unwrap().before_values,
            Some(vec![Value::Int(2), Value::Null])
        );
    }

    #[test]
    fn recovery_gap_alias_survives_restarts_and_later_allocations() {
        let dir = tempfile::tempdir().unwrap();
        let ptr = make_undo_ptr(make_segment_id(0, 7), 4096).unwrap();
        let mut rec = record(2);
        rec.id = ptr;
        rec.kind = UndoKind::Delete;
        let wal = vec![wal_twin(1, ptr, encode_undo_record(&rec).unwrap())];
        let mut home = None;
        for _ in 0..3 {
            let mut undo = UndoManager::open_with_segments_for_recovery(
                &wal,
                dir.path(),
                DEFAULT_UNDO_SEGMENT_BYTES,
                &HashSet::new(),
                false,
            )
            .unwrap();
            let canonical = undo.canonical_ptr(ptr);
            assert_ne!(canonical, ptr);
            assert_eq!(*home.get_or_insert(canonical), canonical);
            assert_eq!(undo.load_owned(ptr).unwrap().txn_id, 2);
            assert_eq!(undo.record_count(), 1);
            let (next, _, _) = undo.append_record(record(3), true).unwrap();
            assert_ne!(undo_ptr_segment(next), undo_ptr_segment(ptr));
            undo.flush().unwrap();
        }
    }

    #[test]
    fn recovery_gap_publication_resumes_after_manifest_and_partial_rename() {
        for rename_first in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let ptr = make_undo_ptr(make_segment_id(0, 7), 4096).unwrap();
            let mut rec = record(2);
            rec.id = ptr;
            rec.kind = UndoKind::Delete;
            let second_ptr = make_undo_ptr(make_segment_id(0, 7), 8192).unwrap();
            let mut second = rec.clone();
            second.id = second_ptr;
            second.txn_id = 3;
            let wal = vec![
                wal_twin(1, ptr, encode_undo_record(&rec).unwrap()),
                wal_twin(2, second_ptr, encode_undo_record(&second).unwrap()),
            ];
            super::recovery::FAIL_AFTER_RECOVERY_INTENT.with(|flag| flag.set(true));
            assert!(UndoManager::open_with_segments_for_recovery(
                &wal,
                dir.path(),
                128,
                &HashSet::new(),
                false
            )
            .is_err());
            let undo_dir = crate::storage::naming::system_undo_dir(dir.path());
            let manifest: UndoManifest =
                decode_undo_manifest(&fs::read(undo_dir.join("undo.manifest")).unwrap()).unwrap();
            assert_eq!(manifest.pending_recovery_segments.len(), 2);
            assert!(UndoManager::open_read_only(dir.path(), 128).is_err());
            if rename_first {
                let path = undo_dir.join(segment_file_name(manifest.pending_recovery_segments[0]));
                fs::rename(path.with_extension("seg.compacting"), path).unwrap();
            }
            let recovered = UndoManager::open_with_segments_for_recovery(
                &wal,
                dir.path(),
                128,
                &HashSet::new(),
                false,
            )
            .unwrap();
            assert_eq!(
                recovered.load_owned(ptr).unwrap().before_values,
                rec.before_values
            );
            assert_eq!(recovered.load_owned(second_ptr).unwrap().txn_id, 3);
            let manifest: UndoManifest =
                decode_undo_manifest(&fs::read(undo_dir.join("undo.manifest")).unwrap()).unwrap();
            assert!(manifest.pending_recovery_segments.is_empty());
        }
    }

    #[test]
    fn recovery_gap_retained_chain_and_lob_metadata_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let first = make_undo_ptr(make_segment_id(0, 7), 4096).unwrap();
        let second = make_undo_ptr(make_segment_id(0, 7), 8192).unwrap();
        let mut a = record(2);
        a.id = first;
        a.before_values = Some(vec![Value::String("历史行".repeat(1000)), Value::Null]);
        a.before_external = vec![(
            1,
            LobRef {
                first_page_id: 10,
                last_page_id: 12,
                total_len: 2048,
                generation: 3,
            },
        )];
        a.borrowed_external = a.before_external.clone();
        let mut b = record(2);
        b.id = second;
        b.prev_undo_ptr = Some(first);
        b.before_header.as_mut().unwrap().undo_ptr = Some(first);
        let wal = vec![
            wal_twin(1, first, encode_undo_record(&a).unwrap()),
            wal_twin(2, second, encode_undo_record(&b).unwrap()),
        ];
        for _ in 0..2 {
            let undo = UndoManager::open_with_segments_for_recovery(
                &wal,
                dir.path(),
                1024,
                &HashSet::from([2]),
                false,
            )
            .unwrap();
            let loaded = undo.load_owned(second).unwrap();
            assert_eq!(loaded.prev_undo_ptr, Some(first));
            assert_eq!(loaded.before_header.unwrap().undo_ptr, Some(first));
            let prior = undo.load_owned(first).unwrap();
            assert_eq!(prior.before_values, a.before_values);
            assert_eq!(prior.before_external, a.before_external);
            assert_eq!(prior.borrowed_external, a.borrowed_external);
            assert_eq!(undo.wal_identity(undo.canonical_ptr(second)), second);
        }
    }

    fn wal_twin(lsn: u64, undo_id: u64, bytes: Vec<u8>) -> WalRecord {
        WalRecord {
            source_time_us: 0,
            lsn,
            prev_txn_lsn: 0,
            txn_id: 1,
            payload: WalPayload::UndoRecord { undo_id, bytes },
        }
    }

    #[test]
    fn buffered_appends_stay_in_memory_until_flush() {
        let dir = tempfile::tempdir().unwrap();
        let mut undo = open(dir.path());
        undo.set_buffer_limit(1024 * 1024).unwrap();
        for i in 1..=3 {
            undo.append_record(record(i), true).unwrap();
        }
        // Frames are buffered: every shard's active segment still holds
        // only the bare header until we explicitly flush.
        assert_eq!(active_segment_len(&undo), UNDO_SEGMENT_HEADER_LEN);
        assert_eq!(undo.len(), 3);
        undo.flush().unwrap();
        assert!(active_segment_len(&undo) > UNDO_SEGMENT_HEADER_LEN);
        // Three records may round-robin over multiple shards; each
        // shard that took at least one record will record a flush.
        assert!(undo.buffer_flushes_total() >= 1);
        drop(undo);
        // Reload from the segment alone (empty WAL): all frames present.
        let reloaded = open(dir.path());
        assert_eq!(reloaded.len(), 3);
    }

    #[test]
    fn buffer_threshold_triggers_flush() {
        let dir = tempfile::tempdir().unwrap();
        let mut undo = open(dir.path());
        undo.set_buffer_limit(1).unwrap(); // every frame crosses the threshold
        undo.append_record(record(1), true).unwrap();
        assert!(active_segment_len(&undo) > UNDO_SEGMENT_HEADER_LEN);
        assert_eq!(undo.buffer_flushes_total(), 1);
    }

    #[test]
    fn wal_repair_recovers_buffered_tail_lost_in_crash() {
        let dir = tempfile::tempdir().unwrap();
        let mut undo = open(dir.path());
        undo.set_buffer_limit(1024 * 1024).unwrap();
        let mut wal = Vec::new();
        let mut ids = Vec::new();
        for i in 1..=3 {
            let (id, bytes, _) = undo.append_record(record(i), true).unwrap();
            ids.push(id);
            wal.push(wal_twin(i, id, bytes));
        }
        // Simulate a crash: drop without flush — every shard's buffered
        // tail is lost so every active segment file is bare-header sized.
        let active_files: Vec<PathBuf> = {
            let shard_segs: Vec<u32> = undo
                .write_shards
                .iter()
                .map(|s| s.lock().active_segment_id)
                .collect();
            shard_segs
                .into_iter()
                .map(|seg| dir.path().join("system/undo").join(segment_file_name(seg)))
                .collect()
        };
        drop(undo);
        for path in &active_files {
            assert_eq!(
                fs::metadata(path).map(|m| m.len()).unwrap_or(0),
                UNDO_SEGMENT_HEADER_LEN,
                "active segment must still be at bare header before WAL repair"
            );
        }
        // Recovery: WAL twins repair the segment file and the id space.
        let mut recovered =
            UndoManager::open_with_segments(&wal, dir.path(), DEFAULT_UNDO_SEGMENT_BYTES).unwrap();
        assert_eq!(recovered.len(), 3);
        assert!(active_segment_len(&recovered) > UNDO_SEGMENT_HEADER_LEN);
        for id in &ids {
            assert!(recovered.get(*id).is_some());
        }
        // New appends must not collide with the repaired id space.
        let (new_id, _, _) = recovered.append_record(record(9), true).unwrap();
        assert!(!ids.contains(&new_id));
        recovered.flush().unwrap();
        drop(recovered);
        let reloaded = open(dir.path());
        assert_eq!(reloaded.len(), 4);
    }

    #[test]
    fn remove_record_truncates_buffered_tail() {
        let dir = tempfile::tempdir().unwrap();
        let mut undo = open(dir.path());
        undo.set_buffer_limit(1024 * 1024).unwrap();
        // P1/6: with sharding, slot reuse is a per-shard property.
        // Pin every append in this test to shard 0 by rewinding the
        // round-robin counter to (n * N - 1) so the next fetch_add
        // wraps to 0.
        let pin_shard0 = |u: &UndoManager| {
            let n = u.write_shards.len() as u64;
            u.next_shard
                .store(n.saturating_sub(1).wrapping_neg(), Ordering::Relaxed);
            // simpler: set so next fetch_add returns a multiple of n
            u.next_shard.store(0, Ordering::Relaxed);
        };
        pin_shard0(&undo);
        let (id_a, _, _) = undo.append_record(record(1), true).unwrap();
        pin_shard0(&undo);
        let (id_b, _, _) = undo.append_record(record(2), true).unwrap();
        undo.remove_record(id_b).unwrap();
        pin_shard0(&undo);
        let (id_c, _, _) = undo.append_record(record(3), true).unwrap();
        // The freed tail slot is reused by the same-shard append.
        assert_eq!(id_b, id_c, "shard 0 must reuse the truncated tail slot");
        undo.flush().unwrap();
        drop(undo);
        let reloaded = open(dir.path());
        assert_eq!(reloaded.len(), 2);
        assert!(reloaded.get(id_a).is_some());
        assert!(reloaded.get(id_c).is_some());
    }

    #[test]
    fn write_through_mode_persists_every_frame() {
        let dir = tempfile::tempdir().unwrap();
        let mut undo = open(dir.path());
        undo.set_buffer_limit(0).unwrap(); // MYDB_UNDO_BUFFER_BYTES=0 behaviour
        undo.append_record(record(1), true).unwrap();
        assert!(active_segment_len(&undo) > UNDO_SEGMENT_HEADER_LEN);
        assert_eq!(undo.buffer_flushes_total(), 0);
    }

    // P0/2: remove_record used to call refresh_metrics() at the end —
    // O(N) over the full BTreeMap. The incremental decrements should
    // keep current_bytes / created_index / segment_live_records /
    // disk_bytes exactly consistent without that full re-walk.
    #[test]
    fn remove_record_keeps_metrics_consistent_without_refresh() {
        let dir = tempfile::tempdir().unwrap();
        let mut undo = open(dir.path());
        let mut ids = Vec::new();
        let mut total_size: u64 = 0;
        for i in 1..=20 {
            let (id, _, _) = undo.append_record(record(i), true).unwrap();
            total_size = total_size.saturating_add(
                undo.records
                    .get(&id)
                    .map(|r| u64::from(r.cached_size))
                    .unwrap_or(0),
            );
            ids.push(id);
        }
        assert_eq!(undo.records.len(), 20);
        assert_eq!(undo.current_bytes, total_size);
        let initial_disk = undo.disk_bytes;
        assert!(initial_disk > 0);
        // Pop the tail one at a time — exercises the "active segment tip
        // truncation" branch, the cheapest remove path. Each step must
        // shrink current_bytes by exactly that record's cached_size and
        // recompute disk_bytes from the segment table.
        let mut running = total_size;
        for id in ids.iter().rev() {
            let size_before = undo
                .records
                .get(id)
                .map(|r| u64::from(r.cached_size))
                .unwrap_or(0);
            undo.remove_record(*id).unwrap();
            running = running.saturating_sub(size_before);
            assert_eq!(undo.current_bytes, running);
            let expected_disk = undo
                .segment_file_bytes
                .values()
                .copied()
                .fold(0u64, u64::saturating_add);
            assert_eq!(undo.disk_bytes, expected_disk);
        }
        assert_eq!(undo.records.len(), 0);
        assert_eq!(undo.current_bytes, 0);
        assert_eq!(undo.created_index.len(), 0);
        // Active segment back to bare header.
        assert_eq!(active_segment_len(&undo), UNDO_SEGMENT_HEADER_LEN);
    }
}

#[path = "undo/recovery.rs"]
mod recovery;

#[cfg(test)]
mod audit_tests;

fn decode_undo_prefix(
    input: &[u8],
    pos: &mut usize,
) -> Result<(u64, u64, u64, Option<u64>), QueryError> {
    let version = undo_read_u8(input, pos)?;
    if version != UNDO_BINARY_VERSION {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            format!(
                "undo binary format version {version} is incompatible with {UNDO_BINARY_VERSION}; use a new data directory or rebuild data"
            ),
        ));
    }
    let id = undo_read_u64(input, pos)?;
    let txn_id = undo_read_u64(input, pos)?;
    let created_unix_ms = undo_read_u64(input, pos)?;
    let prev_undo_ptr = match undo_read_u8(input, pos)? {
        0 => None,
        _ => Some(undo_read_u64(input, pos)?),
    };
    Ok((id, txn_id, created_unix_ms, prev_undo_ptr))
}

fn decode_undo_before_header(
    input: &[u8],
    pos: &mut usize,
) -> Result<Option<RowHeader>, QueryError> {
    Ok(match undo_read_u8(input, pos)? {
        0 => None,
        _ => {
            let flags = undo_read_u8(input, pos)?;
            let last_txn_id = undo_read_u64(input, pos)?;
            let undo_ptr = match undo_read_u8(input, pos)? {
                0 => None,
                _ => Some(undo_read_u64(input, pos)?),
            };
            Some(RowHeader {
                flags,
                last_txn_id,
                undo_ptr,
            })
        }
    })
}

fn decode_undo_lob_refs(
    input: &[u8],
    pos: &mut usize,
    message: &str,
) -> Result<Vec<(usize, LobRef)>, QueryError> {
    let count = undo_read_u32(input, pos)? as usize;
    if count > input.len().saturating_sub(*pos) / 24 {
        return Err(error(crate::error::catalog::ER_GET_ERRNO, message));
    }
    let mut descriptors = Vec::with_capacity(count);
    for _ in 0..count {
        descriptors.push((
            undo_read_u32(input, pos)? as usize,
            LobRef {
                first_page_id: undo_read_u32(input, pos)? as u64,
                last_page_id: undo_read_u32(input, pos)? as u64,
                total_len: undo_read_u32(input, pos)? as u64,
                generation: undo_read_u64(input, pos)?,
            },
        ));
    }
    Ok(descriptors)
}
