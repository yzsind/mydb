mod backup;
#[cfg(test)]
pub(crate) use backup::RAW_PREFIX_READ_BYTES;
mod checksum;
mod clock;
mod ordered_time;
pub(crate) use clock::{persist_source_clock, read_source_clock};
mod replay;
use crate::platform::file::{File, OpenOptions};
use crate::util::bytes::read_u16;
use crate::util::bytes::read_u32;
use crate::util::bytes::read_u64;
pub(crate) use backup::{visit_pinned_range, visit_raw_prefix};
use checksum::{CrcShift, PayloadChecksum};
pub(crate) use ordered_time::{advance_ordered_time, TimedRecord};
pub(crate) use replay::{ReplayScan, WalRecordSource};
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// `LogManager` mutex type. parking_lot::Mutex is a strict upgrade for
/// the WAL hot path: ~30-50% lower acquire/release cost under high
/// contention than `std::sync::Mutex` (no poison check, smaller
/// footprint, futex-backed park) — which matters because
/// `oltp_update_index` pegs ~163K samples on `LogManager::append`'s
/// mutex.
pub type LogMutex = parking_lot::Mutex<LogManager>;

#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::unix::fs::FileExt;

use tokio::sync::watch;

use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
use crate::storage::error;
use crate::types::QueryError;

/// Whether a scan may repair a live recovery tail or use operator salvage.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum WalReadMode {
    #[default]
    Recovery,
    Strict,
    ReadOnly,
}

const WAL_MAGIC: &[u8; 8] = b"MYDBWAL1";
// Bumped 6 → 7 when `WalRowId` shrank from 4 fields / 20B
// (`u64 table_id + u64 page_id + u16 slot + u16 gen`) down to 3
// fields / 8B (`u32 page_id + u16 gen + u16 slot`). The owning
// `path` already lives in the surrounding `WalPayload` record, so
// `table_id` is gone from the per-RowId encoding entirely.
//
// Bumped 7 → 8 to introduce `WalPayload::PrepareTxn` (XA two-phase
// commit). Older WAL streams have no `PrepareTxn` record but the
// header version makes the format change explicit; recovery refuses
// to open a v7 segment so we never confuse a missing PrepareTxn for
// "no XA branches at all" when in fact the WAL predates the format.
// Bumped 10 → 11 with the on-disk layout reshuffle (`CATALOG_VERSION
// 33`): the `path` strings persisted in heap/index WAL records now
// carry the new `schemas/<db>/(tables|indexes)/<name>__<id>__<part>.<ext>`
// form, so a v10 segment cannot be replayed by a v11 binary.
// Bumped 11 → 12 to introduce `WalPayload::HeapUpdateLogical` — a
// column-level UPDATE redo record carrying only the patched byte
// ranges (typically 4-30B) instead of the full before/after row
// images (typically 60-200B). Recovery still understands `HeapUpdate`
// for the row-piece/NULL-toggle fallback path; new updates that hit
// the in-place patch path emit the smaller logical record.
// Bumped 12 → 13 to narrow the `page_id` field of every heap-write
// record (`HeapInsert` / `HeapInsertBatch` / `HeapUpdate` /
// `HeapUpdateLogical` / `HeapDelete`) from u64 to u32 — `RowId.page_id`
// has always been a u32 (16 KiB pages × 4 G = 64 TiB per table), so the
// extra 4 bytes on every record were dead weight. Saves 4 B per
// HeapInsert/Update/Delete record and 4 B per row in HeapInsertBatch.
// Bumped 13 → 14 with the rowid-6bytes refactor: every heap-write
// record drops its 2-byte `generation` field (HeapInsert /
// HeapInsertBatch / HeapUpdate / HeapUpdateLogical / HeapDelete), and
// IndexInsert / IndexDelete drop generation from the embedded RowId.
// Saves 2 B per heap record and 2 B per index entry, in addition to
// the on-disk index savings. Older segments are rejected at recovery.
//
// Bumped 14 → 15 with first-wave storage compression (C+D in
// docs/storage-compression-opportunities.md): the per-frame `flags:
// u16` slot was always 0 and rejected non-zero on decode, and
// `frame_len: u32` was a redundant re-encoding of
// `WAL_FRAME_HEADER_LEN + payload_len`. Both dropped, header shrinks
// 44 → 40 B. New layout:
//   [0..4]   magic
//   [4..8]   payload_len   (frame_len = payload_len + 40 implicit)
//   [8..16]  lsn
//   [16..24] prev_txn_lsn
//   [24..32] txn_id
//   [32..34] record_type
//   [34..36] reserved      (kept for future versioning, writes 0)
//   [36..40] crc32
//
// Bumped 15 → 16 with the WAL persistence overhaul:
// 1. Switched the per-frame and per-segment-header checksum from the
//    in-tree FNV-1 variants (`checksum32` / `checksum32_fast`) to
//    hardware-accelerated CRC32 (IEEE 802.3, the same polynomial as
//    `zlib`) via `crc32fast`. SSE4.2 / ARMv8 hardware instructions
//    move the per-byte cost from ~2.0 ns/B (FNV) to ~0.15 ns/B (HW
//    CRC). LOAD DATA / large-batch profiles previously spent 5-10 %
//    on the FNV hash; this drops it under 1 %.
// 2. Segments are now `posix_fallocate`'d (Linux/Unix) or
//    `F_PREALLOCATE`'d + `ftruncate`'d (macOS) to `max_segment_bytes`
//    on creation, so `fdatasync` no longer has to journal the inode
//    extent metadata on every commit; combined with switching from
//    `fsync` to `fdatasync` on Linux that lops ~30 % off WAL sync
//    latency on ext4/xfs (macOS still uses `fsync` because
//    `fdatasync` is a fsync alias there; F_FULLFSYNC for true disk
//    cache flushing is a separate follow-up).
//
// Bumped 17 → 18 to add `FullPageImageCompressed`: first-write-after-
// checkpoint FPIs can now be lz4-compressed in WAL while preserving the
// exact same redo semantics (recovery inflates to the original page image
// before applying). Older WAL readers would not understand record type 23.
// Bumped 18 → 19 to add `TxnBatch`: a commit-ending transaction tail is
// encoded as one checksummed frame containing its ordered logical payloads.
// The batch is atomic at the frame CRC boundary and always ends in CommitTxn.
// v21 accompanies index-page v14. Physical page images in this WAL always
// contain the canonical raw slotted representation.
// Bumped 22 → 23: CheckpointEnd now stores one explicit `redo_lsn`
// instead of serialising the complete dirty-page table. Recovery only ever
// consumed the minimum rec_lsn, so the old representation made checkpoint
// memory and WAL bytes O(dirty pages) without enabling page-selective redo.
// Bumped 23 -> 24: `FileExtend` is the compact recovery contract for
// reduced-redo append-only writes. Winners have synced the extended bytes
// before CommitTxn; recovery truncates losers to `old_len`.
// v29 protects frame headers together with payloads.
// v28 adds system-scoped PageDelta: dictionary-compressed standard-page redo
// with exact base LSN/CRC and after-image CRC, anchored by retained FPIs.
use crate::storage::format_versions::WAL_VERSION;
const WAL_RECORD_MAGIC: u32 = 0x4d_57_41_4c;
pub(crate) const WAL_HEADER_LEN: usize = 8 + 4 + 8 + 8 + 8 + 4;
pub(crate) const WAL_FRAME_HEADER_LEN: usize = 48;

#[cfg(test)]
thread_local! {
    static WAL_CLEANUP_FAILPOINT: std::cell::Cell<u8> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn set_wal_cleanup_failpoint(stage: u8) {
    WAL_CLEANUP_FAILPOINT.with(|slot| slot.set(stage));
}

#[inline]
fn wal_cleanup_failpoint(stage: u8) -> std::io::Result<()> {
    #[cfg(test)]
    if WAL_CLEANUP_FAILPOINT.with(|slot| slot.get() == stage) {
        return Err(std::io::Error::other(format!(
            "injected WAL cleanup failure at stage {stage}"
        )));
    }
    #[cfg(not(test))]
    let _ = stage;
    Ok(())
}
const FIRST_LSN: u64 = 1;
/// Maximum logical records carried by one physical `TxnBatch` frame.
///
/// Transaction WAL buffering normally parks at most 256 data records and
/// adds one terminal `CommitTxn`. Keep the bound shared by every writer and
/// decoder so no accepted write can create WAL that recovery later rejects.
pub(crate) const MAX_TXN_BATCH_RECORDS: usize = 257;

fn wal_fpi_compression_enabled() -> bool {
    #[cfg(test)]
    {
        !matches!(
            std::env::var("MYDB_WAL_FPI_COMPRESSION")
                .unwrap_or_default()
                .to_ascii_lowercase()
                .as_str(),
            "0" | "off" | "false" | "no"
        )
    }
    #[cfg(not(test))]
    {
        static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *ENABLED.get_or_init(|| {
            !matches!(
                std::env::var("MYDB_WAL_FPI_COMPRESSION")
                    .unwrap_or_default()
                    .to_ascii_lowercase()
                    .as_str(),
                "0" | "off" | "false" | "no"
            )
        })
    }
}

fn decode_bounded_wal_page(bytes: &[u8], context: &str) -> Result<Vec<u8>, QueryError> {
    if bytes.len() < 4 || read_u32(bytes, 0) as usize > crate::storage::PAGE_SIZE {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            format!("{context}: invalid expanded page size"),
        ));
    }
    lz4_flex::decompress_size_prepended(bytes).map_err(|err| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            format!("{context}: {err}"),
        )
    })
}

pub struct WalPageImage {
    bytes: Vec<u8>,
    memory: ControlledMemoryCharge,
}

impl std::ops::Deref for WalPageImage {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.bytes
    }
}
impl std::ops::DerefMut for WalPageImage {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}
impl AsRef<[u8]> for WalPageImage {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl WalPageImage {
    pub(crate) fn into_parts(self) -> (Vec<u8>, ControlledMemoryCharge) {
        (self.bytes, self.memory)
    }
}

fn decode_governed_wal_image(bytes: &[u8], context: &str) -> Result<WalPageImage, QueryError> {
    if bytes.len() < 4 {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "short compressed WAL image",
        ));
    }
    let len = read_u32(bytes, 0) as usize;
    // A size prefix cannot authorize allocation. Even a maximum-length LZ4
    // match needs one length-extension byte per 255 output bytes.
    if len
        > bytes
            .len()
            .saturating_sub(4)
            .saturating_mul(255)
            .saturating_add(16)
    {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "impossible compressed WAL image length",
        ));
    }
    let memory = ControlledMemoryCharge::try_new(ControlledMemoryCategory::StorageScratch, len)?;
    let decoded = lz4_flex::decompress_size_prepended(bytes).map_err(|err| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            format!("{context}: {err}"),
        )
    })?;
    Ok(WalPageImage {
        bytes: decoded,
        memory,
    })
}

pub fn wal_full_page_image_after(payload: &WalPayload) -> Result<Vec<u8>, QueryError> {
    match payload {
        WalPayload::FullPageImage { after, .. } => Ok(after.clone()),
        WalPayload::FullPageImageCompressed {
            compressed_after, ..
        } => decode_bounded_wal_page(compressed_after, "corrupt WAL compressed full-page image"),
        _ => Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "WAL payload is not a full-page image",
        )),
    }
}

pub fn wal_compressed_page_update_images(
    payload: &WalPayload,
) -> Result<(WalPageImage, WalPageImage), QueryError> {
    let WalPayload::PageUpdateCompressed {
        compressed_before,
        compressed_after,
        ..
    } = payload
    else {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "WAL payload is not a compressed page update",
        ));
    };
    let before = decode_governed_wal_image(
        compressed_before,
        "corrupt WAL compressed page before-image",
    )?;
    let after =
        decode_governed_wal_image(compressed_after, "corrupt WAL compressed page after-image")?;
    Ok((before, after))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalRecordType {
    BeginTxn = 1,
    CommitTxn = 2,
    AbortTxn = 3,
    EndTxn = 4,
    PageUpdate = 5,
    FileCreate = 6,
    FileDelete = 7,
    CheckpointBegin = 8,
    CheckpointEnd = 9,
    Clr = 10,
    UndoRecord = 11,
    HeapInsert = 12,
    HeapUpdate = 13,
    HeapDelete = 14,
    IndexInsert = 15,
    IndexDelete = 16,
    IndexBatch = 17,
    HeapInsertBatch = 18,
    /// XA `PREPARE`: the carrying transaction has moved from ACTIVE to
    /// PREPARED. Holds the XID plus a snapshot of the locks/uniques
    /// the txn holds so that recovery can rebuild conflict detection
    /// without having to scan the heap.
    PrepareTxn = 19,
    /// Column-level UPDATE redo. Replaces `HeapUpdate` on the fast
    /// path when every changed column is fixed-width and the row
    /// shape is unchanged (no NULL bitmap toggle, no row-piece). The
    /// record carries `(byte_offset, after_bytes)` patches that the
    /// recovery path applies via `Page::try_patch_columns`. No
    /// before-image — that is in undo.
    HeapUpdateLogical = 20,
    /// Slot transitioned from `DELETED` to `FREE`. Reserved for the
    /// rowid-6bytes slot-reclaim pass: once `oldest_active_snapshot_scn`
    /// has advanced past a tombstone's `deleted_scn` and every
    /// secondary-index entry pointing at `(page_id, slot_id)` has been
    /// removed, the slot becomes safe to reuse. Recovery clears the
    /// `ROW_FLAG_DELETED` bit and sets `ROW_FLAG_FREE` so the post-
    /// crash heap matches the pre-crash logical state. M1 does not yet
    /// emit this record (no FREE transitions happen until the M2
    /// reclaim worker lands); decoding survives so that a future M2
    /// binary can replay logs written after this point.
    SlotReclaim = 21,
    /// After-only full-page image used for first-write-after-checkpoint
    /// torn-page protection. Unlike `PageUpdate`, this record carries no
    /// physical before-image and is always system-scoped (`txn_id = 0`),
    /// so recovery never treats it as a loser-transaction undo step.
    FullPageImage = 22,
    /// lz4-compressed form of `FullPageImage`; redo inflates it to the
    /// original page bytes before applying.
    FullPageImageCompressed = 23,
    /// Ordered transaction tail ending in CommitTxn. One outer frame/header/
    /// CRC replaces the tiny Begin/Undo/Heap/Index/Commit frame train.
    TxnBatch = 24,
    /// Idempotent, monotonic widening of per-zone column summaries.  It is
    /// intentionally after-only and needs neither undo nor CLR.
    ZoneMapMergeBatch = 25,
    /// Old-key versions retired by this transaction. The opaque payload is
    /// decoded by the temporal history sidecar and is published only if the
    /// following CommitTxn becomes durable.
    HistoryIndexBatch = 26,
    FileExtend = 27,
    PageUpdateCompressed = 28,
    /// Compact cache-invalidation summary for an incremental DD transaction.
    /// DD row data remains in ordinary heap/index WAL records.
    CatalogDelta = 29,
    /// Durable DDL publication fact carried in the same transaction as DD
    /// rows, CatalogDelta and the staged binlog reference.
    DdlCommit = 30,
    /// Non-transactional, monotonic AUTO_INCREMENT high-water update.
    AutoIncrementAdvance = 31,
    RollbackTo = 32,
    /// System-scoped standard-page delta anchored to a checksummed full image.
    PageDelta = 33,
    FileClr = 34,
    SequenceState = 35,
    VectorIndexBatch = 36,
    VectorIndexImage = 37,
    VectorIndexComplete = 39,
    VectorIndexManifest = 40,
    VectorIndexModel = 38,
    SpatialPageBatch = 41,
    SpatialIndexUndo = 42,
    SpatialIndexClr = 43,
}

impl WalRecordType {
    fn from_u16(value: u16) -> Result<Self, QueryError> {
        Ok(match value {
            1 => Self::BeginTxn,
            2 => Self::CommitTxn,
            3 => Self::AbortTxn,
            4 => Self::EndTxn,
            5 => Self::PageUpdate,
            6 => Self::FileCreate,
            7 => Self::FileDelete,
            8 => Self::CheckpointBegin,
            9 => Self::CheckpointEnd,
            10 => Self::Clr,
            11 => Self::UndoRecord,
            12 => Self::HeapInsert,
            13 => Self::HeapUpdate,
            14 => Self::HeapDelete,
            15 => Self::IndexInsert,
            16 => Self::IndexDelete,
            17 => Self::IndexBatch,
            18 => Self::HeapInsertBatch,
            19 => Self::PrepareTxn,
            20 => Self::HeapUpdateLogical,
            21 => Self::SlotReclaim,
            22 => Self::FullPageImage,
            23 => Self::FullPageImageCompressed,
            24 => Self::TxnBatch,
            25 => Self::ZoneMapMergeBatch,
            26 => Self::HistoryIndexBatch,
            27 => Self::FileExtend,
            28 => Self::PageUpdateCompressed,
            29 => Self::CatalogDelta,
            30 => Self::DdlCommit,
            31 => Self::AutoIncrementAdvance,
            32 => Self::RollbackTo,
            33 => Self::PageDelta,
            34 => Self::FileClr,
            35 => Self::SequenceState,
            36 => Self::VectorIndexBatch,
            37 => Self::VectorIndexImage,
            39 => Self::VectorIndexComplete,
            40 => Self::VectorIndexManifest,
            41 => Self::SpatialPageBatch,
            42 => Self::SpatialIndexUndo,
            43 => Self::SpatialIndexClr,
            38 => Self::VectorIndexModel,
            _ => {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt WAL: unknown record type",
                ))
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    pub source_time_us: u64,
    pub lsn: u64,
    pub prev_txn_lsn: u64,
    pub txn_id: u64,
    pub payload: WalPayload,
}

/// Maximum compressed delta size. Output is always one standard 16 KiB page.
pub const WAL_PAGE_DELTA_MAX_BYTES: usize = 2048;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalPayload {
    /// Redo-only file compensation with a backward transaction undo link.
    FileClr {
        path: String,
        undo_next_lsn: u64,
        action: FileCompensation,
    },
    /// Completed physical compensation, including intermediate rollback batches.
    /// `undo_head` is the remaining logical chain; `undo_next_lsn` is a separate
    /// physical recovery cursor. An intermediate batch retains the pre-rollback
    /// cursor because bulk/parallel WAL order can differ from logical undo order.
    /// Only a completed rollback range may skip to its savepoint WAL boundary.
    RollbackTo {
        undo_head: Option<u64>,
        undo_next_lsn: u64,
        skip_lsn_start: u64,
        skip_lsn_end: u64,
        compensated: Vec<u64>,
    },
    BeginTxn {
        start_scn: u64,
    },
    CommitTxn {
        commit_scn: u64,
        commit_unix_us: u64,
        binlog: Option<WalBinlogRef>,
    },
    AbortTxn,
    EndTxn,
    PageUpdate {
        path: String,
        page_id: u64,
        before: Vec<u8>,
        after: Vec<u8>,
        undo_lsn: Option<u64>,
    },
    PageUpdateCompressed {
        path: String,
        page_id: u64,
        compressed_before: Vec<u8>,
        compressed_after: Vec<u8>,
        undo_lsn: Option<u64>,
    },
    /// Dictionary-compressed complete image. Its standard page LSN is either
    /// the record LSN or zero (pre-encoded before WAL reservation). after_crc
    /// covers that encoded image; redo validates it before stamping the LSN.
    PageDelta {
        path: String,
        page_id: u64,
        base_lsn: u64,
        base_crc: u32,
        after_crc: u32,
        compressed_after: Vec<u8>,
    },
    FullPageImage {
        path: String,
        page_id: u64,
        after: Vec<u8>,
    },
    FullPageImageCompressed {
        path: String,
        page_id: u64,
        compressed_after: Vec<u8>,
    },
    FileCreate {
        path: String,
        after: Vec<u8>,
    },
    FileDelete {
        path: String,
        before: Vec<u8>,
    },
    /// Compact recovery record for append-only reduced-redo writes. The
    /// extended bytes are forced before CommitTxn. REDO makes the length
    /// monotonic for winners; UNDO truncates losers to `old_len`.
    FileExtend {
        path: String,
        old_len: u64,
        new_len: u64,
    },
    CheckpointBegin,
    CheckpointEnd {
        /// Earliest WAL record recovery must reconsider after this
        /// checkpoint. This folds together CheckpointBegin, the oldest dirty
        /// page, and the begin LSN of writers active at the checkpoint
        /// boundary.
        redo_lsn: u64,
        next_txn_id: u64,
        next_scn: u64,
        /// AUTO_INCREMENT redo records below this LSN are represented by the
        /// dynamic-metadata DD rows written by this checkpoint.
        auto_increment_covered_lsn: u64,
        sequence_covered_lsn: u64,
        /// Source GC-safe prefix; allocation or registration alone is insufficient.
        gc_safe_txn_frontier: u64,
    },
    Clr {
        path: String,
        page_id: u64,
        undo_next_lsn: u64,
        after: Vec<u8>,
    },
    UndoRecord {
        undo_id: u64,
        bytes: Vec<u8>,
    },
    HeapInsert {
        path: String,
        page_id: u32,
        slot_id: u16,
        payload: Vec<u8>,
        undo_lsn: Option<u64>,
    },
    HeapInsertBatch {
        path: String,
        inserts: Vec<WalHeapInsert>,
    },
    HeapUpdate {
        path: String,
        page_id: u32,
        slot_id: u16,
        before: Vec<u8>,
        after: Vec<u8>,
        undo_lsn: Option<u64>,
    },
    /// Column-level UPDATE redo. `patches` is a list of
    /// `(byte_offset_in_slot, after_bytes)` pairs that overwrite
    /// individual column bodies in place. Replaces the full-row
    /// before/after image of [`WalPayload::HeapUpdate`] when the
    /// in-place patch path applies — typically 4-30B per record vs
    /// 60-200B for the full image. Recovery applies the patches via
    /// `Page::try_patch_columns`; rollback / MVCC reverse-apply via
    /// the matching `UndoKind::UpdateDelta`.
    HeapUpdateLogical {
        path: String,
        page_id: u32,
        slot_id: u16,
        patches: Vec<WalColumnPatch>,
        undo_lsn: Option<u64>,
    },
    HeapDelete {
        path: String,
        page_id: u32,
        slot_id: u16,
        before: Vec<u8>,
        undo_lsn: Option<u64>,
    },
    /// rowid-6bytes: slot in `path`'s heap page `page_id` transitions
    /// from `DELETED` to `FREE`. Emitted by the slot-reclaim pass once
    /// (a) `oldest_active_snapshot_scn > deleted_scn` and (b) every
    /// secondary-index entry pointing at `(page_id, slot_id)` has been
    /// removed. Recovery clears `ROW_FLAG_DELETED` and sets
    /// `ROW_FLAG_FREE` so the slot becomes reusable. Idempotent — a
    /// double replay produces the same outcome.
    SlotReclaim {
        path: String,
        page_id: u32,
        slot_id: u16,
    },
    IndexInsert {
        path: String,
        key: Vec<u8>,
        row_id: WalRowId,
    },
    IndexDelete {
        path: String,
        key: Vec<u8>,
        row_id: WalRowId,
    },
    IndexBatch {
        path: String,
        ops: Vec<WalIndexOp>,
    },
    ZoneMapMergeBatch {
        bytes: Vec<u8>,
    },
    HistoryIndexBatch {
        bytes: Vec<u8>,
    },
    /// Committed vector-index row changes, sufficient to finish interrupted maintenance.
    VectorIndexBatch {
        bytes: Vec<u8>,
    },
    VectorIndexModel {
        bytes: Vec<u8>,
    },
    /// A bounded chunk of an immutable initial index image. The last chunk
    /// publishes the file; subsequent physical records retain source page LSNs.
    VectorIndexImage {
        path: String,
        offset: u64,
        total: u64,
        bytes: Vec<u8>,
    },
    VectorIndexComplete {
        txn_id: u64,
        commit_scn: u64,
    },
    /// Transaction-owned logical inverse; structural pages remain redo-only.
    SpatialIndexUndo {
        path: String,
        bytes: Vec<u8>,
    },
    SpatialIndexClr {
        undo_next_lsn: u64,
    },
    SpatialPageBatch {
        path: String,
        bytes: Vec<u8>,
    },
    VectorIndexManifest {
        path: String,
        bytes: Vec<u8>,
    },
    CatalogDelta {
        object_ids: Vec<u64>,
        invalidate_all: bool,
    },
    DdlCommit {
        job_id: u64,
        /// Physical generations superseded only if this transaction commits.
        retired_paths: Vec<String>,
    },
    /// Complete nontransactional sequence recovery state, ordered by frame LSN.
    SequenceState {
        sequence_id: u64,
        counter_id: u64,
        resume_value: i64,
        is_called: bool,
        logged_unix_us: u64,
    },
    AutoIncrementAdvance {
        table_id: u64,
        version: u64,
        max_value: u64,
    },
    TxnBatch {
        records: Vec<WalPayload>,
    },
    /// XA branch's `PREPARE` record. Once this record is durable the
    /// branch survives crashes and can be committed/rolled back by any
    /// session via `XA COMMIT` / `XA ROLLBACK` after recovery.
    PrepareTxn {
        xid: crate::xa::Xid,
        prepare_scn: u64,
        /// Snapshot of the locks held by the transaction at PREPARE time.
        /// Replayed on recovery so concurrent writers see the PREPARED
        /// branch as a regular conflicting writer.
        locks: Vec<PreparedLockEntry>,
        /// Snapshot of transaction-duration metadata locks held by the XA
        /// branch. This is optional on decode so existing v8 WAL streams
        /// without MDL tails still open cleanly.
        mdl_locks: Vec<PreparedMdlEntry>,
        binlog: Option<PreparedBinlogRef>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileCompensation {
    Delete,
    Restore(Vec<u8>),
    Truncate(u64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedBinlogRef {
    pub chunks: u32,
    pub bytes: u64,
    pub crc32: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalBinlogRef {
    pub file: String,
    pub start_pos: u64,
    pub end_pos: u64,
    pub gtid: Option<String>,
}

/// Compact description of a single lock held by a PREPARED XA branch.
/// Persisted as part of `WalPayload::PrepareTxn` so the conflict-detection
/// state survives restart without requiring the lock manager to scan
/// the heap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreparedLockEntry {
    Row {
        table_id: u64,
        page_id: u32,
        slot_id: u16,
        /// 0 = Shared, 1 = Exclusive — matches `storage::txn::LockMode`.
        mode: u8,
    },
    UniqueKey {
        table_id: u64,
        index_name: String,
        key_bytes: Vec<u8>,
    },
    Table {
        table_id: u64,
        /// 0=IS 1=IX 2=S 3=SIX 4=X — matches `storage::txn::TableLockMode`.
        mode: u8,
    },
}

/// Compact metadata-lock description persisted with `PrepareTxn`.
///
/// The integer fields deliberately mirror `storage::mdl` discriminants through
/// conversion helpers there, keeping WAL independent from enum layout changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedMdlEntry {
    pub object_type: u8,
    pub schema: Option<String>,
    pub name: Option<String>,
    pub lock_type: u8,
}

/// WAL-side mirror of [`crate::storage::RowId`]. 6-byte on-disk
/// layout (`page_id` u32 + `slot_id` u16) after the rowid-6bytes
/// refactor. The owning index path is already carried in the
/// surrounding `WalPayload::IndexInsert { path, .. }` /
/// `IndexDelete { .. }` record, so `table_id` is not stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalRowId {
    pub page_id: u32,
    pub slot_id: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalHeapInsert {
    pub page_id: u32,
    pub slot_id: u16,
    pub payload: Vec<u8>,
    pub undo_lsn: Option<u64>,
}

/// One byte range to overwrite inside a heap row's encoded payload as
/// part of a [`WalPayload::HeapUpdateLogical`] redo record. `offset` is
/// measured from the start of the row payload (the MVCC version byte);
/// `before` / `after` carry the old / new bytes for this column body.
/// Recovery redo writes `after`; recovery undo writes `before` (we
/// carry both because the catalog is not loaded yet at WAL undo
/// time, so we cannot derive the before image from the matching
/// `UndoKind::UpdateDelta` record's `Value`s without schema). For
/// fixed-width columns `before.len() == after.len()`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalColumnPatch {
    pub offset: u16,
    pub before: Vec<u8>,
    pub after: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalIndexOpKind {
    Insert = 1,
    Delete = 2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalIndexOp {
    pub kind: WalIndexOpKind,
    pub key: Vec<u8>,
    pub row_id: WalRowId,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalStats {
    pub logical_records: u64,
    pub physical_write_calls: u64,
    pub physical_write_bytes: u64,
    pub page_fallback_records: u64,
    pub index_batch_records: u64,
    pub heap_payload_bytes: u64,
    pub index_payload_bytes: u64,
    pub page_payload_bytes: u64,
    pub heap_fpi_payload_bytes: u64,
    pub index_page_payload_bytes: u64,
    pub other_page_payload_bytes: u64,
    pub sync_total_us: u64,
    pub sync_max_us: u64,
    pub rotation_count: u64,
    pub rotation_critical_total_us: u64,
    pub rotation_critical_max_us: u64,
}

/// M4.1 - WAL segment metadata. Each WAL segment is an independent file
/// participating in log rotation. The current implementation uses a single
/// active segment (`wal-0000000000000001.log`); the segment registry tracks
/// it plus any historical (archive-pending) segments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalSegmentInfo {
    pub segment_id: u64,
    pub path: PathBuf,
    pub first_lsn: u64,
    pub last_lsn: u64,
    pub size_bytes: u64,
    pub status: WalSegmentStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalSegmentStatus {
    Active,
    Reclaimable,
    Archived,
}

impl WalSegmentStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            WalSegmentStatus::Active => "active",
            WalSegmentStatus::Reclaimable => "reclaimable",
            WalSegmentStatus::Archived => "archived",
        }
    }
}

/// M4.2 - WAL archive manager. When enabled, segment files are copied to
/// `archive_dir` before they can be reclaimed. This is the foundation for
/// future point-in-time recovery (PITR) support.
#[derive(Debug, Default)]
pub struct ArchiveManager {
    pub enabled: bool,
    pub archive_dir: Option<PathBuf>,
    pub archived_segments: Vec<WalSegmentInfo>,
    pub archived_bytes: u64,
}

impl ArchiveManager {
    pub fn enable(&mut self, dir: PathBuf) -> Result<(), QueryError> {
        std::fs::create_dir_all(&dir).map_err(io_err)?;
        self.enabled = true;
        self.archive_dir = Some(dir);
        Ok(())
    }

    pub fn disable(&mut self) {
        self.enabled = false;
    }

    pub fn archive_segment(&mut self, segment: &WalSegmentInfo) -> Result<(), QueryError> {
        if !self.enabled {
            return Ok(());
        }
        let Some(dir) = &self.archive_dir else {
            return Ok(());
        };
        wal_cleanup_failpoint(1).map_err(io_err)?;
        let dest = dir.join(format!("arch-{:016x}.log", segment.segment_id));
        if dest.exists()
            && self
                .archived_segments
                .iter()
                .any(|archived| archived.segment_id == segment.segment_id)
        {
            return Ok(());
        }
        std::fs::copy(&segment.path, &dest).map_err(io_err)?;
        crate::platform::fs::sync_path(&dest).map_err(io_err)?;
        crate::platform::fs::sync_path(dir).map_err(io_err)?;
        self.archived_bytes = self.archived_bytes.saturating_add(segment.size_bytes);
        let mut archived = segment.clone();
        archived.path = dest;
        archived.status = WalSegmentStatus::Archived;
        self.archived_segments.push(archived);
        Ok(())
    }
}

/// Atomic mirror of the few LogManager fields that the hot path
/// (`Database::should_checkpoint` and friends) wants to probe without
/// taking the WAL mutex. Profiling showed that
/// `should_checkpoint` was called after every non-pure-read statement
/// and `self.log.lock()` for `next_lsn()` + `wal_bytes()` was a major
/// contention site on the WAL global mutex.
///
/// LogManager owns this `Arc` and updates the atomics under its own
/// `&mut self` write paths; `Database` clones the `Arc` so it can read
/// them without locking. The values are a near-real-time mirror of the
/// authoritative state inside the mutex. A reader can observe a slightly
/// stale value, which is acceptable for checkpoint scheduling: it may
/// delay a checkpoint by one probe or trigger a redundant outer
/// `should_checkpoint() -> checkpoint()` call, and the real checkpoint
/// path still re-checks under the WAL/checkpoint locks.
#[derive(Debug, Default)]
pub struct LogStateAtomics {
    pub next_lsn: AtomicU64,
    durable_lsn: AtomicU64,
    physical_replica: std::sync::atomic::AtomicBool,
    pub active_size_bytes: AtomicU64,
    pub sealed_size_bytes: AtomicU64,
}

impl LogStateAtomics {
    pub(crate) fn physical_replica(&self) -> bool {
        self.physical_replica.load(Ordering::Acquire)
    }

    /// Lock-free view of `LogManager::next_lsn`.
    /// Each reservation publishes before returning. A serialized transaction
    /// can use `next_lsn() - 1` to separate its completed eager appends from
    /// future allocations. This is not a durable watermark or a transaction
    /// backlink: it may include other transactions and in-flight appends.
    pub fn next_lsn(&self) -> u64 {
        self.next_lsn.load(Ordering::Acquire)
    }

    /// Lock-free view of `LogManager::wal_bytes` (sealed + active).
    /// May be slightly stale relative to the WAL mutex but is
    /// monotonically increasing within a segment and adequate for
    /// "is it time to checkpoint?" probes.
    pub fn wal_bytes(&self) -> u64 {
        self.sealed_size_bytes
            .load(Ordering::Acquire)
            .saturating_add(self.active_size_bytes.load(Ordering::Acquire))
    }
}

/// A next WAL segment file that the background preparer created, header-wrote
/// and fsynced ahead of time (under a temp name `discover_segments` ignores),
/// so segment rotation on the commit critical path is a cheap metadata
/// `rename` + fd swap instead of a synchronous file create + `sync_all`.
///
/// The pre-opened `file` fd was opened on `tmp_path`; a POSIX `rename` keeps
/// the same inode, so rotation adopts this fd directly after promoting the
/// name.
#[derive(Debug)]
struct PreparedSegment {
    segment_id: u64,
    tmp_path: PathBuf,
    file: Arc<File>,
    fill: SegmentFill,
}

/// A segment that has been detached from the append path but is not yet
/// covered by a completed durability barrier. Keeping its fd here lets the
/// next group-commit fsync it outside the WAL mutex, in LSN order, before an
/// acknowledgement can advance into the new active segment.
#[derive(Debug)]
struct PendingSealedSync {
    file: Arc<File>,
    last_lsn: u64,
}

/// Fixed temp name for the pre-created next segment. Ends in `.tmp` (not
/// `.log`) so `discover_segments` skips it at recovery: a crash between
/// preparation and promotion leaves a stray file that is invisible to the
/// WAL scanner and reclaimed at the next `open`. Only one preparation is ever
/// in flight (gated by `next_segment` / `next_segment_prep_in_flight`), so a
/// single fixed name suffices.
const WAL_PREP_TMP_NAME: &str = "wal-prep.tmp";

thread_local! {
    static PROMOTION_WAL: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(crate) fn promotion_wal_active() -> bool {
    PROMOTION_WAL.with(std::cell::Cell::get)
}

/// Only the stopped receiver's promotion rollback may fork the source WAL.
pub(crate) fn with_promotion_wal<T>(f: impl FnOnce() -> T) -> T {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            PROMOTION_WAL.with(|flag| flag.set(self.0));
        }
    }
    let _restore = Restore(PROMOTION_WAL.with(|flag| flag.replace(true)));
    f()
}

/// Differential-oracle / escape-hatch switch for background segment
/// pre-creation. On by default; `MYDB_WAL_SEGMENT_PREP=0` forces every
/// rotation back through the synchronous create + `sync_all` path (the
/// pre-change behaviour) for A/B measurement or if the prepared path ever
/// misbehaves on an exotic filesystem.
fn wal_segment_prep_enabled() -> bool {
    !matches!(
        std::env::var("MYDB_WAL_SEGMENT_PREP").ok().as_deref(),
        Some("0") | Some("off") | Some("false")
    )
}

#[derive(Debug)]
pub struct LogManager {
    read_only: bool,
    source_clock: Arc<AtomicU64>,
    budget: Option<Arc<crate::storage::capture::WalBudget>>,
    local_recovery: bool,
    last_frame_time: u64,
    /// `<data-dir>/redo` directory holding all WAL segment files.
    dir: PathBuf,
    /// Currently-writing segment metadata (size_bytes / last_lsn updated
    /// on every append).
    active_info: WalSegmentInfo,
    /// File handle for the active segment, opened read+write.
    ///
    /// Kept behind `Arc` so commit fsync can run outside the WAL mutex without
    /// paying `dup`/`try_clone` on every group-commit batch. Segment rotation
    /// swaps in a new Arc; any in-flight fsync keeps the old fd alive until it
    /// finishes.
    active_file: Arc<File>,
    /// Sealed segments older than the active one, sorted by segment_id
    /// ascending (i.e. ordered by first_lsn). These are read-only and
    /// candidates for retention pruning.
    pub segments: Vec<WalSegmentInfo>,
    /// Next LSN to be allocated.
    next_lsn: u64,
    durable_lsn: u64,
    /// Serialize durability barriers without holding the WAL append mutex.
    /// Buffer-pool cleaners can concurrently request the same WAL prefix;
    /// without this gate they all pass `prepare_sync_to` before the first
    /// fsync publishes `durable_lsn`, issuing duplicate fsyncs on one fd.
    /// The leader still releases the WAL mutex during the syscall, so appends
    /// and segment rotation continue; followers re-check the durable
    /// high-water after acquiring the gate and normally return immediately.
    sync_gate: Arc<parking_lot::Mutex<()>>,
    sync_count: u64,
    stats: WalStats,
    /// `txn_id -> last LSN this txn appended`, used to fill each record's
    /// `prev_txn_lsn` back-link under the global WAL mutex. ahash (not the std
    /// SipHash default) because this get+insert runs on EVERY WAL record inside
    /// the contended critical section — Time Profiler put SipHash here at the
    /// top of the WAL-lock's on-CPU cost, and shrinking the critical section
    /// pays off super-linearly under 20-way contention.
    txn_last_lsn: hashbrown::HashMap<u64, u64, ahash::RandomState>,
    /// Active segment rotation threshold (bytes). Default
    /// [`DEFAULT_WAL_SEGMENT_BYTES`]; configurable via
    /// `MYDB_WAL_SEGMENT_BYTES` and `set_max_segment_bytes`.
    pub max_segment_bytes: u64,
    // M4.2 - Archive manager (disabled by default).
    pub archive: ArchiveManager,
    /// Single-value broadcast of the most recently appended LSN. Drives
    /// the physical-replication `WalReader` wake-ups; receivers see at
    /// least the latest committed LSN, never miss a wake-up. Senderless
    /// `send` failures (no receivers) are ignored.
    new_lsn_tx: watch::Sender<u64>,
    /// Single-value broadcast of the durable (fsynced) LSN. The
    /// physical-replication sender caps the frames it ships at this
    /// watermark so a master crash that loses an unsynced WAL tail can
    /// never leave a replica ahead of the recovered master (silent
    /// fork). Advanced by `finish_sync_to` and checkpoint compaction.
    durable_lsn_tx: watch::Sender<u64>,
    /// Lock-free mirror of `next_lsn` / `active_info.size_bytes` /
    /// sealed-segment byte total. See `LogStateAtomics` doc for the
    /// motivation.
    state_atomics: Arc<LogStateAtomics>,
    /// Reusable scratch buffer for the `encode_record` hot path. Each
    /// `append_reserved` call refills this buffer, writes it to the
    /// active segment, and leaves the capacity for the next call. Avoids
    /// per-append `Vec::with_capacity` + drop of the encoded frame —
    /// LOAD DATA writes ~6 MB-sized frames every 50K-row batch, so
    /// recycling the underlying allocation eliminated the
    /// `drop_in_place::<WalPayload>` / `encode_record`-side malloc churn
    /// that profile-p2-fix3 measured at 5%+ of total CPU.
    encode_scratch: Vec<u8>,
    encode_scratch_charge: ControlledMemoryCharge,
    /// Transaction whose consecutive large frames may reuse
    /// `encode_scratch`. A different transaction or a terminal record drops
    /// the retained capacity so an isolated bulk write cannot pin memory for
    /// the lifetime of the server.
    large_scratch_txn: Option<u64>,
    /// Buffered WAL frames not yet written to the active segment fd.
    ///
    /// Appends still allocate LSNs and advance `active_info.size_bytes`
    /// synchronously, but strict-mode tiny OLTP records are coalesced here
    /// and drained by `prepare_sync_to` / `flush` / segment rotation.
    /// Strict durability is unchanged because commit fsync always calls
    /// `prepare_sync_to` before cloning the fd. Relaxed-mode commit barriers
    /// drain the buffer into the OS page cache before acknowledging commits.
    pending_write_offset: u64,
    pending_write_bytes: Vec<u8>,
    pending_write_charge: ControlledMemoryCharge,
    write_buffer_enabled: bool,
    write_buffer_bytes: usize,
    /// A next segment file the background preparer created ahead of the
    /// rotation point (see [`PreparedSegment`]). Consumed by
    /// `rotate_active_internal`; `None` falls back to the synchronous create.
    next_segment: Option<PreparedSegment>,
    /// Set while a `ensure_next_segment` preparation is building the file
    /// off-lock, so a second poll tick does not start a duplicate create for
    /// the same slot.
    next_segment_prep_in_flight: bool,
    /// Whether the active segment's preallocated tail was overwritten with
    /// zeros ([`SegmentFill::Zero`]). A reservation-only active segment is
    /// replaced by a zero-filled successor as soon as the background preparer
    /// has one ready (see `ensure_next_segment`).
    active_fill: SegmentFill,
    /// `(physical_write_bytes, sync_count, sync_total_us)` at the previous
    /// preparation poll. The delta is the recent WAL bytes-per-sync density
    /// and sync latency read by [`Self::next_segment_fill`]; a per-poll window
    /// lets the choice follow a phase change (bulk load -> OLTP) within one
    /// poll.
    fill_sample: (u64, u64, u64),
    /// Average durability-sync latency last measured while the active segment
    /// was reservation-only: the cost zero filling would remove. `None` until
    /// a poll window saw enough syncs on such a segment.
    reserve_sync_us: Option<u64>,
    /// Minimum `reserve_sync_us` for which zero filling pays off
    /// ([`ZERO_FILL_MIN_RESERVE_SYNC_US`]; tests override it).
    zero_fill_min_sync_us: u64,
    pending_sealed_sync: Vec<PendingSealedSync>,
    directory_sync_pending: bool,
    pending_publications: Vec<u64>,
}

/// Default per-segment rotation size; matches `MYDB_WAL_SEGMENT_BYTES`
/// default and the value baked into `StorageConfig`.
pub const DEFAULT_WAL_SEGMENT_BYTES: u64 = 256 * 1024 * 1024;
pub const DEFAULT_WAL_WRITE_BUFFER_BYTES: usize = 256 * 1024;
pub const MAX_WAL_WRITE_BUFFER_BYTES: u64 = 1024 * 1024 * 1024;
// Scratch reuse is independent of the configurable write-coalescing threshold.
const WAL_ENCODE_SCRATCH_REUSE_MIN_BYTES: usize = 256 * 1024;
const WAL_ENCODE_SCRATCH_IDLE_BYTES: usize = 8 * 1024;
const WAL_ENCODE_SCRATCH_RETAIN_LIMIT: usize = 64 * 1024;

fn controlled_wal_buffer(capacity: usize) -> Result<(Vec<u8>, ControlledMemoryCharge), QueryError> {
    let mut charge =
        ControlledMemoryCharge::try_new(ControlledMemoryCategory::StorageScratch, capacity)?;
    let mut bytes = Vec::new();
    if bytes.try_reserve_exact(capacity).is_err() {
        return Err(error(
            crate::error::catalog::ER_OUTOFMEMORY,
            format!("Out of memory allocating WAL buffer ({capacity} bytes)"),
        ));
    }
    let actual = bytes.capacity();
    if actual > capacity {
        charge.try_grow(actual - capacity)?;
    } else if actual < capacity {
        charge.shrink(capacity - actual);
    }
    Ok((bytes, charge))
}

/// Build the canonical WAL segment file name for a segment id under
/// `<data-dir>/system/wal/`.
/// Output of `LogManager::pre_encode_record`. Carries the fully encoded
/// frame bytes (lsn / prev_txn_lsn patched in later) plus the payload
/// kept owned so the mutex-side `append_pre_encoded` can run
/// `accumulate_wal_stats` without re-decoding.
pub struct PreEncodedRecord {
    source_time_us: u64,
    bytes: Vec<u8>,
    bytes_charge: Option<ControlledMemoryCharge>,
    payload: WalPayload,
    txn_id: u64,
    lsn_span: u64,
    retain_large_buffer: bool,
    payload_checksum: Option<PayloadChecksum>,
    #[cfg(test)]
    drop_observer: Option<Box<dyn FnOnce() + Send + Sync>>,
}

struct WalRecordBuffer {
    bytes: Vec<u8>,
    charge: ControlledMemoryCharge,
    owner_txn: Option<u64>,
}

/// Cap for the per-thread WAL record byte-buffer pool. On the sysbench
/// oltp_update_index hot path, one transaction pre-encodes ~5 records
/// (BeginTxn / heap-patch / undo / index / commit). A pool of 8 covers
/// that with headroom for the rare mid-transaction flush without pinning
/// unbounded memory when a thread runs a large-batch DML and then
/// switches to reads.
const WAL_RECORD_BUF_POOL_CAP: usize = 8;
/// Maximum buffer capacity we keep in the pool. Records larger than this
/// (bulk INSERT batches, large index-batch payloads) are dropped instead
/// of returned so the pool memory stays bounded.
const WAL_RECORD_BUF_KEEP_CAP: usize = 4 * 1024;

thread_local! {
    /// Thread-local pool of `Vec<u8>` buffers reclaimed from dropped
    /// `PreEncodedRecord`s. The sysbench oltp_update_index profile showed
    /// ~10% of RUNNING-CPU time in `_xzm_xzone_malloc_tiny` / `_xzm_free`,
    /// most of it churning these per-record byte buffers (5 allocations
    /// + 5 frees per UPDATE). Recycling them cuts the tiny-malloc traffic
    /// to near zero for the steady-state workload.
    static WAL_RECORD_BUF_POOL: std::cell::RefCell<Vec<WalRecordBuffer>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

fn wal_record_buf_take(capacity: usize, txn_id: u64) -> Result<WalRecordBuffer, QueryError> {
    // Fall back to a fresh allocation if the TLS slot is being destroyed
    // (rare — happens only during thread teardown).
    WAL_RECORD_BUF_POOL
        .try_with(|pool| {
            if let Ok(mut pool) = pool.try_borrow_mut() {
                while let Some(mut buffer) = pool.pop() {
                    if buffer.owner_txn.is_some_and(|owner| owner != txn_id) {
                        continue;
                    }
                    if buffer.bytes.capacity() >= capacity {
                        buffer.owner_txn = None;
                        return Ok(buffer);
                    }
                }
            }
            controlled_wal_buffer(capacity).map(|(bytes, charge)| WalRecordBuffer {
                bytes,
                charge,
                owner_txn: None,
            })
        })
        .unwrap_or_else(|_| {
            controlled_wal_buffer(capacity).map(|(bytes, charge)| WalRecordBuffer {
                bytes,
                charge,
                owner_txn: None,
            })
        })
}

fn wal_record_buf_return(mut buffer: WalRecordBuffer) {
    if buffer.bytes.capacity() > WAL_RECORD_BUF_KEEP_CAP && buffer.owner_txn.is_none() {
        return;
    }
    buffer.bytes.clear();
    // Silently drop the buffer if the TLS slot is already gone
    // (thread teardown path). This must not panic — it runs from
    // `Drop for PreEncodedRecord`, and a panic-on-drop aborts the process.
    let _ = WAL_RECORD_BUF_POOL.try_with(|pool| {
        if let Ok(mut pool) = pool.try_borrow_mut() {
            if pool.len() < WAL_RECORD_BUF_POOL_CAP {
                pool.push(buffer);
            }
        }
    });
}

impl Drop for PreEncodedRecord {
    fn drop(&mut self) {
        #[cfg(test)]
        if let Some(observer) = self.drop_observer.take() {
            observer();
        }
        // Move out the buffer without touching `payload`/`txn_id` — those
        // continue their normal Drop. `mem::take` swaps in an empty Vec
        // (capacity 0, no allocation) so the pool receives the real
        // buffer with its actual capacity for reuse.
        let bytes = std::mem::take(&mut self.bytes);
        let Some(charge) = self.bytes_charge.take() else {
            return;
        };
        if !bytes.is_empty() || bytes.capacity() > 0 {
            let keep_large = self.retain_large_buffer
                && bytes.capacity() > WAL_RECORD_BUF_KEEP_CAP
                && self.txn_id != 0
                && !matches!(
                    &self.payload,
                    WalPayload::CommitTxn { .. }
                        | WalPayload::AbortTxn
                        | WalPayload::EndTxn
                        | WalPayload::TxnBatch { .. }
                );
            wal_record_buf_return(WalRecordBuffer {
                bytes,
                charge,
                owner_txn: keep_large.then_some(self.txn_id),
            });
        }
    }
}

pub fn segment_path(dir: &Path, segment_id: u64) -> PathBuf {
    dir.join(format!("wal-{:016x}.log", segment_id))
}

/// Discover all WAL segments under a redo directory.
/// Returns `(segment_id, path)` pairs sorted by segment_id ascending.
fn discover_all_segments(dir: &Path) -> Result<Vec<(u64, PathBuf)>, QueryError> {
    let read = match fs::read_dir(dir) {
        Ok(it) => it,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(io_err(err)),
    };
    let mut segments = Vec::new();
    for entry in read {
        let entry = entry.map_err(io_err)?;
        let name = entry.file_name();
        let name_str = match name.to_str() {
            Some(s) => s,
            None => continue,
        };
        if !name_str.starts_with("wal-") || !name_str.ends_with(".log") {
            continue;
        }
        let id_part = &name_str[4..name_str.len() - 4];
        if let Ok(id) = u64::from_str_radix(id_part, 16) {
            segments.push((id, entry.path()));
        }
    }
    segments.sort_by_key(|(id, _)| *id);
    Ok(segments)
}

fn discover_segments(dir: &Path) -> Result<Vec<(u64, PathBuf)>, QueryError> {
    let mut segments = discover_all_segments(dir)?;
    for (id, path) in segments.iter().rev() {
        let Some(_) = replacement_floor(path, *id)? else {
            continue;
        };
        let first_id = *id;
        segments.retain(|(id, _)| *id >= first_id);
        break;
    }
    Ok(segments)
}

fn replacement_floor(path: &Path, id: u64) -> Result<Option<u64>, QueryError> {
    // Empty latest files can occur before the initial header is forced.
    if fs::metadata(path).map_err(io_err)?.len() == 0 {
        return Ok(None);
    }
    ensure_segment_header(path, id)?;
    let mut header = [0; WAL_HEADER_LEN];
    File::open(path)
        .map_err(io_err)?
        .read_exact(&mut header)
        .map_err(io_err)?;
    let floor = read_u64(&header, 28);
    if floor != 0 {
        let mut file = File::open(path).map_err(io_err)?;
        file.seek(SeekFrom::Start(WAL_HEADER_LEN as u64))
            .map_err(io_err)?;
        let mut frame = [0; WAL_FRAME_HEADER_LEN + 48];
        file.read_exact(&mut frame).map_err(io_err)?;
        let decoded = decode_frame_for_replication(&frame)?;
        if decoded.frame_len != frame.len()
            || decoded.header.lsn != floor
            || decoded.header.txn_id != 0
            || !matches!(decoded.payload, WalPayload::CheckpointEnd { redo_lsn, .. } if redo_lsn == floor)
        {
            return Err(io_err(std::io::Error::other(
                "invalid WAL replacement checkpoint",
            )));
        }
    }
    Ok((floor != 0).then_some(floor))
}

#[derive(Debug, Default)]
pub struct WalScan {
    pub records: Vec<WalRecord>,
    pub next_lsn: u64,
    pub end_offset: u64,
}

impl LogManager {
    pub fn open(data_dir: &Path) -> Result<Self, QueryError> {
        let scan = Self::scan_replay(data_dir)?;
        Self::open_from_replay(data_dir, &scan)
    }

    #[cfg(test)]
    /// Recovery retains the records for REDO/UNDO; borrow them instead of
    /// decoding and retaining a second complete WAL to open its writer.
    pub(crate) fn open_from_scan_ref(data_dir: &Path, scan: &WalScan) -> Result<Self, QueryError> {
        Self::open_from_source(data_dir, &scan.records, scan.next_lsn, scan.end_offset)
    }

    fn open_from_source(
        data_dir: &Path,
        records: &impl WalRecordSource,
        next_lsn: u64,
        end_offset: u64,
    ) -> Result<Self, QueryError> {
        let write_buffer_bytes = match crate::util::env::var("MYDB_WAL_WRITE_BUFFER_BYTES") {
            Ok(raw) => crate::startup_config::parse_scaled_u64(&raw, 1)
                .filter(|bytes| *bytes <= MAX_WAL_WRITE_BUFFER_BYTES)
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                        "MYDB_WAL_WRITE_BUFFER_BYTES must be 0..=1G bytes (K/M/G/T suffixes supported)",
                    )
                })? as usize,
            Err(std::env::VarError::NotPresent) => DEFAULT_WAL_WRITE_BUFFER_BYTES,
            Err(_) => {
                return Err(error(
                    crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                    "MYDB_WAL_WRITE_BUFFER_BYTES must be valid UTF-8",
                ))
            }
        };
        let source_clock = Arc::new(AtomicU64::new(
            records
                .bootstrap()
                .map_or(0, |bootstrap| bootstrap.max_source_time)
                .max(read_source_clock(data_dir)?),
        ));
        let last_frame_time = records
            .bootstrap()
            .map_or(0, |bootstrap| bootstrap.max_source_time);
        let read_only = crate::storage::image_io::immutable_path(data_dir);
        let dir = crate::storage::naming::system_wal_dir(data_dir);
        if !read_only {
            fs::create_dir_all(&dir).map_err(io_err)?;
        }
        // Reclaim a background-prepared next segment that a crash left
        // un-promoted. It carries no committed records (rotation renames it to
        // its real name only after sealing the previous segment), so removing
        // it is always safe; `discover_segments` already ignores it.
        if !read_only {
            let _ = fs::remove_file(dir.join(WAL_PREP_TMP_NAME));
        }
        let discovered = discover_segments(&dir)?;
        if !read_only {
            for entry in fs::read_dir(&dir).map_err(io_err)? {
                let entry = entry.map_err(io_err)?;
                let name = entry.file_name();
                if name
                    .to_str()
                    .is_some_and(|name| name.starts_with("wal-") && name.ends_with(".log.pending"))
                {
                    fs::remove_file(entry.path()).map_err(io_err)?;
                }
            }
        }

        // No segments yet — bootstrap segment 1. Don't preallocate the
        // extent up front: the caller (production: `Database::open_with_config`,
        // tests that need a small cap: `open_log_with_segment_cap`)
        // configures the actual segment-byte budget right after open,
        // and `set_max_segment_bytes` reapplies preallocation up to
        // whatever ceiling it picks.
        if discovered.is_empty() {
            if read_only {
                return Err(crate::storage::image_io::read_only_error());
            }
            let path = segment_path(&dir, 1);
            initialize_segment(&path, 1, 0, SegmentFill::Reserve)?;
            // Open in plain read+write mode (not append): preallocation
            // makes the on-disk size larger than the valid-bytes
            // watermark, so writes need to land at an explicit
            // `size_bytes` offset rather than at file end. `pwrite`
            // (via `FileExt::write_at`) is atomic-positional under
            // POSIX, so concurrent reader fds can still scan safely.
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .map_err(io_err)?;
            // Bookkeeping size is valid-bytes, not extent — only the
            // header has been written so far.
            let size_bytes = WAL_HEADER_LEN as u64;
            let active_info = WalSegmentInfo {
                segment_id: 1,
                path,
                first_lsn: FIRST_LSN,
                last_lsn: FIRST_LSN.saturating_sub(1),
                size_bytes,
                status: WalSegmentStatus::Active,
            };
            let (new_lsn_tx, _) = watch::channel(0);
            let (durable_lsn_tx, _) = watch::channel(0);
            let state_atomics = Arc::new(LogStateAtomics::default());
            state_atomics.next_lsn.store(FIRST_LSN, Ordering::Release);
            state_atomics
                .active_size_bytes
                .store(active_info.size_bytes, Ordering::Release);
            let (encode_scratch, encode_scratch_charge) = controlled_wal_buffer(0)?;
            let (pending_write_bytes, pending_write_charge) =
                controlled_wal_buffer(write_buffer_bytes)?;
            return Ok(Self {
                read_only,
                source_clock,
                budget: None,
                last_frame_time,
                local_recovery: false,
                dir,
                active_info,
                active_file: Arc::new(file),
                segments: Vec::new(),
                next_lsn: FIRST_LSN,
                durable_lsn: 0,
                sync_gate: Arc::new(parking_lot::Mutex::new(())),
                sync_count: 0,
                stats: WalStats::default(),
                txn_last_lsn: hashbrown::HashMap::default(),
                max_segment_bytes: DEFAULT_WAL_SEGMENT_BYTES,
                archive: ArchiveManager::default(),
                new_lsn_tx,
                durable_lsn_tx,
                state_atomics,
                encode_scratch,
                encode_scratch_charge,
                large_scratch_txn: None,
                pending_write_offset: size_bytes,
                pending_write_bytes,
                pending_write_charge,
                write_buffer_enabled: false,
                write_buffer_bytes,
                next_segment: None,
                next_segment_prep_in_flight: false,
                active_fill: SegmentFill::Reserve,
                fill_sample: (0, 0, 0),
                reserve_sync_us: None,
                zero_fill_min_sync_us: ZERO_FILL_MIN_RESERVE_SYNC_US,
                pending_sealed_sync: Vec::new(),
                directory_sync_pending: false,
                pending_publications: Vec::new(),
            });
        }

        // Last segment is the active one; everything before is sealed.
        let (active_id, active_path) = discovered.last().cloned().unwrap();
        let (mut sealed_meta, txn_last_lsn, stats, active_first_lsn, active_last_lsn) =
            if let Some(bootstrap) = records.bootstrap() {
                let active = bootstrap.segments.last().ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "missing WAL bootstrap segment",
                    )
                })?;
                if active.segment_id != active_id {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "WAL changed since recovery snapshot",
                    ));
                }
                (
                    bootstrap.segments[..bootstrap.segments.len() - 1].to_vec(),
                    bootstrap.txn_last_lsn.clone(),
                    bootstrap.stats,
                    active.first_lsn,
                    active.last_lsn,
                )
            } else {
                let mut sealed_meta = Vec::with_capacity(discovered.len().saturating_sub(1));
                for (id, path) in discovered.iter().take(discovered.len() - 1) {
                    sealed_meta.push(inspect_segment(path, *id)?);
                }
                let sealed_last_lsn = sealed_meta.last().map(|s| s.last_lsn).unwrap_or(0);
                let mut active_first_lsn = next_lsn;
                let mut active_last_lsn = 0;
                let mut txn_last_lsn =
                    hashbrown::HashMap::<u64, u64, ahash::RandomState>::default();
                let mut stats = WalStats::default();
                for record in records.records() {
                    let record = record?;
                    if record.lsn > sealed_last_lsn {
                        active_first_lsn = active_first_lsn.min(record.lsn);
                    }
                    active_last_lsn = active_last_lsn.max(record.lsn);
                    if record.txn_id != 0 {
                        if Self::is_terminal_txn_record(&record.payload) {
                            txn_last_lsn.remove(&record.txn_id);
                        } else {
                            txn_last_lsn.insert(record.txn_id, record.lsn);
                        }
                    }
                    accumulate_wal_stats(&mut stats, &record.payload);
                }
                (
                    sealed_meta,
                    txn_last_lsn,
                    stats,
                    active_first_lsn,
                    active_last_lsn,
                )
            };

        if let Some((first_id, first_path)) = discovered.first() {
            if let Some(floor) = replacement_floor(first_path, *first_id)? {
                for (id, path) in discover_all_segments(&dir)? {
                    if id >= *first_id {
                        break;
                    }
                    sealed_meta.push(WalSegmentInfo {
                        segment_id: id,
                        size_bytes: fs::metadata(&path).map_err(io_err)?.len(),
                        path,
                        first_lsn: floor,
                        last_lsn: floor.saturating_sub(1),
                        status: WalSegmentStatus::Reclaimable,
                    });
                }
                sealed_meta.sort_unstable_by_key(|segment| segment.segment_id);
            }
        }

        // Truncate the active segment's tail if scan was cut short by
        // corruption (or by the legacy non-preallocated path's zero
        // tail). `scan_segment_inner` already did this when it ran in
        // `truncate_tail` mode, but if the data dir was opened by a
        // pre-v16 binary that left a partial frame, this catches it.
        if !read_only && end_offset > 0 {
            let len = fs::metadata(&active_path).map_err(io_err)?.len();
            if len > end_offset {
                let file = OpenOptions::new()
                    .write(true)
                    .open(&active_path)
                    .map_err(io_err)?;
                file.set_len(end_offset).map_err(io_err)?;
            }
        }

        let file = OpenOptions::new()
            .read(true)
            .write(!read_only)
            .open(&active_path)
            .map_err(io_err)?;
        // Post-trim size — the count of *valid* WAL bytes the active
        // segment carries. If the caller later calls
        // `set_max_segment_bytes` (the production path always does;
        // tests that need a small segment cap set it explicitly), the
        // setter will reapply preallocation up to the new ceiling.
        let size_bytes = fs::metadata(&active_path).map(|m| m.len()).unwrap_or(0);

        // For the active segment, derive [first_lsn, last_lsn] from the
        // scan: any record whose lsn falls outside every sealed range
        // belongs to the active segment.
        let active_info = WalSegmentInfo {
            segment_id: active_id,
            path: active_path,
            first_lsn: active_first_lsn,
            last_lsn: active_last_lsn,
            size_bytes,
            status: WalSegmentStatus::Active,
        };

        // Records the scan returned are already on disk; fsync the
        // (possibly tail-truncated) active segment once so claiming
        // them durable below is honest even after an unclean shutdown.
        if !read_only {
            sync_wal_file(&file)?;
        }
        let (new_lsn_tx, _) = watch::channel(active_last_lsn);
        let (durable_lsn_tx, _) = watch::channel(active_last_lsn);
        let state_atomics = Arc::new(LogStateAtomics::default());
        state_atomics.next_lsn.store(next_lsn, Ordering::Release);
        state_atomics
            .durable_lsn
            .store(active_last_lsn, Ordering::Release);
        state_atomics
            .active_size_bytes
            .store(active_info.size_bytes, Ordering::Release);
        state_atomics.sealed_size_bytes.store(
            sealed_meta.iter().map(|s| s.size_bytes).sum(),
            Ordering::Release,
        );
        let (encode_scratch, encode_scratch_charge) = controlled_wal_buffer(0)?;
        let (pending_write_bytes, pending_write_charge) =
            controlled_wal_buffer(write_buffer_bytes)?;
        Ok(Self {
            read_only,
            source_clock,
            budget: None,
            last_frame_time,
            local_recovery: false,
            dir,
            active_info,
            active_file: Arc::new(file),
            segments: sealed_meta,
            next_lsn,
            durable_lsn: active_last_lsn,
            sync_gate: Arc::new(parking_lot::Mutex::new(())),
            sync_count: 0,
            stats,
            txn_last_lsn,
            max_segment_bytes: DEFAULT_WAL_SEGMENT_BYTES,
            archive: ArchiveManager::default(),
            new_lsn_tx,
            durable_lsn_tx,
            state_atomics,
            encode_scratch,
            encode_scratch_charge,
            large_scratch_txn: None,
            pending_write_offset: size_bytes,
            pending_write_bytes,
            pending_write_charge,
            write_buffer_enabled: false,
            write_buffer_bytes,
            next_segment: None,
            next_segment_prep_in_flight: false,
            active_fill: SegmentFill::Reserve,
            fill_sample: (0, 0, 0),
            reserve_sync_us: None,
            zero_fill_min_sync_us: ZERO_FILL_MIN_RESERVE_SYNC_US,
            pending_sealed_sync: Vec::new(),
            directory_sync_pending: false,
            pending_publications: Vec::new(),
        })
    }

    /// Lock-free handle to the few LogManager fields the hot path
    /// probes outside the WAL mutex. See `LogStateAtomics`.
    pub fn state_atomics(&self) -> Arc<LogStateAtomics> {
        Arc::clone(&self.state_atomics)
    }

    /// Returns the count of all segments on disk, sealed + 1 active.
    pub fn segment_count(&self) -> usize {
        self.segments.len() + 1
    }

    /// Returns a snapshot of segment metadata for observability / the
    /// physical replication WAL reader. Order: oldest sealed first,
    /// active last.
    pub fn segment_snapshot(&self) -> Vec<WalSegmentInfo> {
        let mut out = self.segments.clone();
        out.push(self.active_info.clone());
        out
    }

    /// Smallest LSN still on disk (start of the oldest segment).
    pub fn min_available_lsn(&self) -> u64 {
        if let Some(seg) = self
            .segments
            .iter()
            .find(|seg| seg.first_lsn <= seg.last_lsn)
        {
            seg.first_lsn
        } else {
            self.active_info.first_lsn
        }
    }

    /// Subscribe to "new LSN appended" notifications. The watch sender
    /// holds the most recently appended LSN. New `WalReader`s call this
    /// once at construction so they don't miss notifications between
    /// `next_chunk` calls.
    pub fn subscribe_new_lsn(&self) -> watch::Receiver<u64> {
        self.new_lsn_tx.subscribe()
    }

    /// Subscribe to durable-LSN advancement (fsync completion). Used by
    /// the physical-replication sender to wake up when previously
    /// appended-but-unsynced frames become safe to ship.
    pub fn subscribe_durable_lsn(&self) -> watch::Receiver<u64> {
        self.durable_lsn_tx.subscribe()
    }

    /// Configure the active-segment rotation threshold. Takes effect on
    /// the next append. If the new cap exceeds the active segment's
    /// current on-disk size we reapply preallocation up to the new
    /// ceiling so commits land on a non-extending file and
    /// `fdatasync` doesn't have to journal the inode-extent metadata.
    /// Idempotent: a second call with the same byte budget is a no-op
    /// at the filesystem layer.
    pub fn set_max_segment_bytes(&mut self, bytes: u64) {
        if self.read_only {
            return;
        }
        let new_max = bytes.max(WAL_HEADER_LEN as u64 * 2);
        self.max_segment_bytes = new_max;
        // With preallocation disabled, preallocate_segment is a successful
        // no-op. No file changed, so neither metadata I/O nor a durability
        // barrier is needed merely to configure the rotation threshold.
        if !wal_preallocate_enabled() {
            return;
        }
        if let Ok(metadata) = fs::metadata(&self.active_info.path) {
            if metadata.len() < new_max {
                if preallocate_segment(&self.active_file, new_max).is_ok() {
                    let _ = self.active_file.sync_all();
                }
            }
        }
    }

    pub fn write_buffer_bytes(&self) -> u64 {
        self.write_buffer_bytes as u64
    }

    /// Drain the old buffer before applying a new threshold. Zero uses direct writes.
    pub fn set_write_buffer_bytes(&mut self, bytes: u64) -> Result<(), QueryError> {
        if bytes > MAX_WAL_WRITE_BUFFER_BYTES {
            return Err(error(
                crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                "mydb_wal_write_buffer_bytes must be between 0 and 1073741824",
            ));
        }
        let bytes = bytes as usize;
        if bytes == self.write_buffer_bytes {
            return Ok(());
        }
        self.flush_pending_writes()?;
        Self::reserve_buffer_capacity(
            &mut self.pending_write_bytes,
            &mut self.pending_write_charge,
            bytes,
            "WAL write buffer",
        )?;
        let old_capacity = self.pending_write_bytes.capacity();
        self.pending_write_bytes.shrink_to(bytes);
        self.pending_write_charge
            .shrink(old_capacity - self.pending_write_bytes.capacity());
        self.write_buffer_bytes = bytes;
        Ok(())
    }

    pub fn set_write_buffer_enabled(&mut self, enabled: bool) -> Result<(), QueryError> {
        if !enabled {
            self.flush_pending_writes()?;
        }
        self.write_buffer_enabled = enabled;
        Ok(())
    }

    /// Prune every sealed segment whose `last_lsn` is strictly below
    /// `keep_above_lsn`. Returns the total bytes reclaimed. Archive mode
    /// (if enabled) copies the segments out before deletion.
    pub fn prune_segments_below_lsn(&mut self, keep_above_lsn: u64) -> Result<u64, QueryError> {
        let (removed, result) = Self::reclaim_segments(
            &self.dir,
            &mut self.archive,
            self.segments
                .iter()
                .filter(|seg| seg.last_lsn < keep_above_lsn)
                .cloned()
                .collect(),
        );
        self.finish_reclaim(&removed);
        result
    }

    /// The caller serializes maintenance; append and rotation remain free to run.
    pub(crate) fn prune_checkpoint_segments(
        log: &Arc<LogMutex>,
        floor: u64,
    ) -> Result<u64, QueryError> {
        Self::prune_checkpoint_segments_with(log, floor, || {})
    }

    fn prune_checkpoint_segments_with(
        log: &Arc<LogMutex>,
        floor: u64,
        before_io: impl FnOnce(),
    ) -> Result<u64, QueryError> {
        let (dir, mut archive, segments) = {
            let guard = log.lock();
            (
                guard.dir.clone(),
                ArchiveManager {
                    enabled: guard.archive.enabled,
                    archive_dir: guard.archive.archive_dir.clone(),
                    archived_segments: guard.archive.archived_segments.clone(),
                    ..ArchiveManager::default()
                },
                guard
                    .segments
                    .iter()
                    .filter(|seg| seg.last_lsn < floor)
                    .cloned()
                    .collect(),
            )
        };
        let archived_before = archive.archived_segments.len();
        before_io();
        let (removed, result) = Self::reclaim_segments(&dir, &mut archive, segments);
        let mut guard = log.lock();
        guard.archive.archived_bytes = guard
            .archive
            .archived_bytes
            .saturating_add(archive.archived_bytes);
        guard
            .archive
            .archived_segments
            .extend(archive.archived_segments.into_iter().skip(archived_before));
        guard.finish_reclaim(&removed);
        result
    }

    fn finish_reclaim(&mut self, removed: &[u64]) {
        // Failed descriptors stayed registered throughout IO; concurrent rotation
        // can add new descriptors and must not be overwritten by our snapshot.
        self.segments
            .retain(|seg| removed.binary_search(&seg.segment_id).is_err());
        self.state_atomics.sealed_size_bytes.store(
            self.segments.iter().map(|seg| seg.size_bytes).sum(),
            Ordering::Release,
        );
    }

    fn reclaim_segments(
        dir: &Path,
        archive: &mut ArchiveManager,
        to_remove: Vec<WalSegmentInfo>,
    ) -> (Vec<u64>, Result<u64, QueryError>) {
        let mut removed_bytes = 0u64;
        let mut unlinked = Vec::new();
        let mut first_error = None;
        for seg in to_remove {
            if archive.enabled {
                if let Err(err) = archive.archive_segment(&seg) {
                    first_error.get_or_insert(err);
                    continue;
                }
            }
            match wal_cleanup_failpoint(2).and_then(|()| fs::remove_file(&seg.path)) {
                Ok(()) => {
                    unlinked.push(seg);
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    // A previous crash may have persisted the unlink before
                    // in-memory bookkeeping advanced. Treat it as reclaimed.
                    unlinked.push(seg);
                }
                Err(err) => {
                    first_error.get_or_insert_with(|| io_err(err));
                }
            }
        }
        if !unlinked.is_empty() {
            match wal_cleanup_failpoint(4).and_then(|()| crate::platform::fs::sync_path(dir)) {
                Ok(()) => {
                    removed_bytes = unlinked.iter().map(|seg| seg.size_bytes).sum();
                }
                Err(err) => {
                    // The unlink is not crash-durable yet. Keep the segment
                    // descriptors so the next checkpoint retries the
                    // directory barrier (NotFound is accepted above).
                    first_error.get_or_insert_with(|| io_err(err));
                    unlinked.clear();
                }
            }
        }
        let mut removed: Vec<_> = unlinked.iter().map(|seg| seg.segment_id).collect();
        removed.sort_unstable();
        let result = match first_error {
            Some(err) => Err(err),
            None => Ok(removed_bytes),
        };
        (removed, result)
    }

    #[cfg(test)]
    /// Force-rotate the active segment. Test hook + used by some
    /// administrative paths. Returns the new segment's id.
    pub fn rotate_segment(&mut self) -> Result<u64, QueryError> {
        self.rotate_active_internal()
    }

    fn rotate_active_internal(&mut self) -> Result<u64, QueryError> {
        let rotation_started = Instant::now();
        // Detach the full segment without an in-mutex fsync. The next
        // durability barrier includes this fd before the new active fd and
        // only then advances durable_lsn, preserving strict ordering while
        // removing multi-second filesystem stalls from the append mutex.
        // Recovery already recognises a zero-filled preallocated tail, so an
        // immediate truncate is not required for correctness.
        self.flush_pending_writes()?;
        self.active_file.flush().map_err(io_err)?;
        let next_id = self.active_info.segment_id.saturating_add(1);
        let path = segment_path(&self.dir, next_id).with_extension("log.pending");
        // Fast path: the background preparer (`ensure_next_segment`) already
        // created + header-wrote + fsynced this segment under a temp name off
        // the commit critical path. Promote it with a cheap metadata rename —
        // the pre-opened fd keeps pointing at the same inode — instead of the
        // synchronous create + `sync_all` that would block every concurrent
        // committer (they all wait on the WAL mutex this method holds). The
        // synchronous branch below stays as the correctness fallback whenever
        // no prepared segment is ready (the preparer didn't keep up) or a
        // stale one is left over from a `max_segment_bytes` change.
        //
        // The fallback only reserves extents: zero-filling a whole segment
        // here would stall every committer behind the WAL mutex. The
        // background preparer replaces such a segment early (see
        // `ensure_next_segment`).
        let (file, fill) = match self.next_segment.take() {
            Some(prep) if prep.segment_id == next_id => {
                fs::rename(&prep.tmp_path, &path).map_err(io_err)?;
                (prep.file, prep.fill)
            }
            stale => {
                if let Some(prep) = stale {
                    let _ = fs::remove_file(&prep.tmp_path);
                }
                initialize_segment(&path, next_id, self.max_segment_bytes, SegmentFill::Reserve)?;
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&path)
                    .map_err(io_err)?;
                (Arc::new(file), SegmentFill::Reserve)
            }
        };
        // Preparing the successor can fail. Until it succeeds, the current
        // descriptor is still active and must never enter the reclaimable
        // list: checkpoint cleanup could otherwise unlink a live WAL file.
        self.pending_sealed_sync.push(PendingSealedSync {
            file: Arc::clone(&self.active_file),
            last_lsn: self.active_info.last_lsn,
        });
        let mut sealed = self.active_info.clone();
        sealed.status = WalSegmentStatus::Reclaimable;
        let sealed_bytes = sealed.size_bytes;
        self.segments.push(sealed);
        self.state_atomics
            .sealed_size_bytes
            .fetch_add(sealed_bytes, Ordering::AcqRel);
        // Active-segment size tracks valid bytes, not the preallocated
        // extent.
        let size_bytes = WAL_HEADER_LEN as u64;
        self.active_info = WalSegmentInfo {
            segment_id: next_id,
            path,
            first_lsn: self.next_lsn,
            last_lsn: self.next_lsn.saturating_sub(1),
            size_bytes,
            status: WalSegmentStatus::Active,
        };
        self.state_atomics
            .active_size_bytes
            .store(size_bytes, Ordering::Release);
        self.pending_write_offset = size_bytes;
        self.pending_write_bytes.clear();
        self.shrink_encode_scratch();
        self.active_file = file;
        self.active_fill = fill;
        self.pending_publications.push(next_id);
        let rotation_us = elapsed_us(rotation_started);
        self.stats.rotation_count = self.stats.rotation_count.saturating_add(1);
        self.stats.rotation_critical_total_us = self
            .stats
            .rotation_critical_total_us
            .saturating_add(rotation_us);
        self.stats.rotation_critical_max_us = self.stats.rotation_critical_max_us.max(rotation_us);
        Ok(next_id)
    }

    /// Plan a background pre-creation of the next segment, if one is warranted.
    /// Runs under the WAL mutex but does no IO: it only decides *whether* to
    /// prepare and returns the parameters for the (slow) file creation that
    /// `ensure_next_segment` then performs with the mutex released. Returns
    /// `None` when a prepared segment already exists, one is being built, or
    /// the active segment is not yet close enough to its rotation threshold.
    fn plan_next_segment_prep(&mut self) -> Option<(u64, PathBuf, u64, SegmentFill)> {
        if !wal_segment_prep_enabled() {
            return None;
        }
        self.reserve_sync_us = self.observed_reserve_sync_us();
        let fill = self.next_segment_fill();
        self.fill_sample = (
            self.stats.physical_write_bytes,
            self.sync_count,
            self.stats.sync_total_us,
        );
        if self.next_segment.is_some() || self.next_segment_prep_in_flight {
            return None;
        }
        // Prepare once the active segment is at least half full: far enough
        // ahead that the file is ready before rotation at realistic fill
        // rates, but we never keep a spare segment for a mostly-idle log.
        // A reservation-only active segment (bootstrap, restart, checkpoint
        // collapse, synchronous rotation fallback) pays an extent-conversion
        // journal commit on most `fdatasync`s; as soon as it shows sustained
        // write traffic, prepare a zero-filled successor to replace it.
        let threshold = if self.active_replaceable_by(fill) {
            self.max_segment_bytes / 16
        } else {
            self.max_segment_bytes / 2
        };
        if self.active_info.size_bytes < threshold {
            return None;
        }
        let next_id = self.active_info.segment_id.saturating_add(1);
        let tmp_path = self.dir.join(WAL_PREP_TMP_NAME);
        self.next_segment_prep_in_flight = true;
        Some((next_id, tmp_path, self.max_segment_bytes, fill))
    }

    /// Choose the next segment's fill from the WAL bytes per durability sync
    /// observed since the previous preparation poll and the measured sync
    /// latency of reservation-only segments. Zero filling costs one extra
    /// sequential write of the whole segment; it pays off only when the
    /// segment absorbs many small commit syncs, each of which would otherwise
    /// wait for an extent-conversion journal commit. Bulk loads sync megabytes
    /// at a time, so the conversion is amortized and the extra write bandwidth
    /// would only slow them down. On storage where a reservation-only sync is
    /// already fast, retain reservation for larger writes. Small syncs amortize
    /// the extra sequential write over many more barriers, so their latency
    /// threshold is lower even on a fast device.
    fn next_segment_fill(&self) -> SegmentFill {
        if !wal_zero_fill_enabled() {
            return SegmentFill::Reserve;
        }
        let (sample_bytes, sample_syncs, _) = self.fill_sample;
        let bytes = self.stats.physical_write_bytes.saturating_sub(sample_bytes);
        let syncs = self.sync_count.saturating_sub(sample_syncs);
        if syncs == 0 {
            return SegmentFill::Reserve;
        }
        let bytes_per_sync = bytes / syncs;
        let dense = bytes_per_sync <= ZERO_FILL_MAX_BYTES_PER_SYNC;
        let min_sync_us = if bytes_per_sync <= ZERO_FILL_SMALL_BYTES_PER_SYNC {
            self.zero_fill_min_sync_us / 8
        } else {
            self.zero_fill_min_sync_us
        };
        let slow_reserve_sync = self
            .observed_reserve_sync_us()
            .is_some_and(|us| us >= min_sync_us);
        if dense && slow_reserve_sync {
            SegmentFill::Zero
        } else {
            SegmentFill::Reserve
        }
    }

    /// Average sync latency of the current poll window when the active segment
    /// is reservation-only and the window holds enough syncs to be
    /// representative; otherwise the last such measurement.
    fn observed_reserve_sync_us(&self) -> Option<u64> {
        let (_, sample_syncs, sample_us) = self.fill_sample;
        let syncs = self.sync_count.saturating_sub(sample_syncs);
        if self.active_fill == SegmentFill::Reserve && syncs >= ZERO_FILL_MIN_SAMPLE_SYNCS {
            Some(self.stats.sync_total_us.saturating_sub(sample_us) / syncs)
        } else {
            self.reserve_sync_us
        }
    }

    /// Whether promoting a prepared segment with `fill` would replace a
    /// reservation-only active segment with a zero-filled one.
    fn active_replaceable_by(&self, fill: SegmentFill) -> bool {
        fill == SegmentFill::Zero && self.active_fill == SegmentFill::Reserve
    }

    /// Install (or discard) a segment file that `ensure_next_segment` built
    /// off-lock. Re-validates under the mutex: a rotation may have advanced
    /// the active id via the synchronous fallback while the file was being
    /// created, in which case the freshly built file is stale and removed.
    fn install_prepared_segment(
        &mut self,
        segment_id: u64,
        tmp_path: PathBuf,
        file: Arc<File>,
        fill: SegmentFill,
    ) -> Result<(), QueryError> {
        self.next_segment_prep_in_flight = false;
        if self.next_segment.is_none()
            && segment_id == self.active_info.segment_id.saturating_add(1)
        {
            self.next_segment = Some(PreparedSegment {
                segment_id,
                tmp_path,
                file,
                fill,
            });
            // Promote immediately instead of waiting for the size threshold:
            // rotation is a rename + fd swap, and every later commit then
            // syncs data blocks only.
            if self.active_replaceable_by(fill) {
                self.rotate_active_internal()?;
            }
        } else {
            drop(file);
            let _ = fs::remove_file(&tmp_path);
        }
        Ok(())
    }

    /// Background pre-creation of the next WAL segment. Creates, header-writes,
    /// zero-fills and fsyncs the file with the WAL mutex **released**, so none
    /// of that IO lands on the commit critical path; segment rotation then only
    /// has to rename it into place. A reservation-only active segment is
    /// replaced by the prepared one right away. Idempotent and cheap when a segment is already
    /// prepared or the active segment is far from rotation. Driven by the
    /// background checkpointer poll (see `Database::maybe_checkpoint`'s worker).
    pub fn ensure_next_segment(log: &Arc<LogMutex>) -> Result<(), QueryError> {
        let Some((next_id, tmp_path, prealloc, fill)) = ({
            let mut guard = log.lock();
            guard.plan_next_segment_prep()
        }) else {
            return Ok(());
        };
        // Slow file create + zero fill + sync, all outside the mutex.
        let built = (|| -> Result<Arc<File>, QueryError> {
            initialize_segment(&tmp_path, next_id, prealloc, fill)?;
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&tmp_path)
                .map_err(io_err)?;
            Ok(Arc::new(file))
        })();
        let mut guard = log.lock();
        match built {
            Ok(file) => guard.install_prepared_segment(next_id, tmp_path, file, fill),
            Err(err) => {
                // Let a later poll retry; the partial temp file (if any) is
                // truncate-overwritten by the next `initialize_segment`.
                guard.next_segment_prep_in_flight = false;
                Err(err)
            }
        }
    }

    /// M4.2 - Number of archived segments.
    pub fn archived_segment_count(&self) -> usize {
        self.archive.archived_segments.len()
    }

    /// Scan all WAL segments under `data_dir/redo` and return their
    /// records in global LSN order, plus the LSN to allocate next and
    /// the offset where the active segment ends (used to truncate a
    /// partial tail on the active segment).
    pub fn scan(data_dir: &Path) -> Result<WalScan, QueryError> {
        Self::scan_with_mode(data_dir, WalReadMode::Recovery)
    }

    pub(crate) fn scan_with_mode(
        data_dir: &Path,
        mode: WalReadMode,
    ) -> Result<WalScan, QueryError> {
        let dir = crate::storage::naming::system_wal_dir(data_dir);
        let discovered = discover_segments(&dir)?;
        if discovered.is_empty() {
            return Ok(WalScan {
                records: Vec::new(),
                next_lsn: FIRST_LSN,
                end_offset: 0,
            });
        }
        let last_idx = discovered.len() - 1;
        let mut records: Vec<WalRecord> = Vec::new();
        let mut active_end_offset = 0u64;
        for (idx, (id, path)) in discovered.iter().enumerate() {
            // Only the active (last) segment gets the truncate-tail
            // treatment — earlier segments are sealed and immutable.
            let truncate_tail =
                idx == last_idx && !crate::storage::image_io::immutable_path(data_dir);
            let scan = scan_segment_inner_with_mode(path, *id, truncate_tail, mode)?;
            if idx == last_idx {
                active_end_offset = scan.end_offset;
            }
            records.extend(scan.records);
        }
        let next_lsn = records
            .iter()
            .map(|r| r.lsn)
            .max()
            .unwrap_or(0)
            .saturating_add(1)
            .max(FIRST_LSN);
        Ok(WalScan {
            records,
            next_lsn,
            end_offset: active_end_offset,
        })
    }

    /// Pre-encode a WAL record outside of the LogManager mutex. The
    /// returned `PreEncodedRecord` carries the framed bytes (with
    /// `lsn` / `prev_txn_lsn` left as zero placeholders) and the
    /// payload CRC (which doesn't depend on the LSN — it covers only
    /// the [40..]-byte payload range). Callers should run this before
    /// taking `log.lock()` and feed the result to `append_pre_encoded`.
    ///
    /// Why: `LogManager::append_reserved` used to do everything inside
    /// the WAL mutex — including the encode pass (memcpy + a few
    /// `to_le_bytes()` per field, ~0.5-5 µs per record) and the CRC
    /// pass (~0.15 ns/B with hardware CRC32). For oltp_update_index
    /// (~163K profile samples on the WAL mutex) every microsecond
    /// shaved off the critical section is multiplied by 20 contending
    /// writers, so factoring CPU work out of the lock is the cheapest
    /// way to widen `LogManager::append`'s throughput without touching
    /// the on-disk format.
    pub fn pre_encode_record(
        txn_id: u64,
        payload: WalPayload,
    ) -> Result<PreEncodedRecord, QueryError> {
        Self::pre_encode_record_inner(txn_id, payload, true)
    }

    #[inline]
    pub(crate) fn pre_encode_timed_record(
        txn_id: u64,
        payload: WalPayload,
    ) -> Result<TimedRecord, QueryError> {
        TimedRecord::new(Self::pre_encode_record_inner(txn_id, payload, false)?)
    }

    fn pre_encode_record_inner(
        txn_id: u64,
        payload: WalPayload,
        prehash: bool,
    ) -> Result<PreEncodedRecord, QueryError> {
        if let WalPayload::TxnBatch { records } = &payload {
            validate_txn_batch_records(records)?;
        }
        let payload_len = encoded_payload_len(&payload)?;
        let frame_len = WAL_FRAME_HEADER_LEN + payload_len;
        // Reuse a thread-local Vec if one is available with sufficient
        // capacity; falls back to a fresh allocation on cold start or
        // when the record is larger than any pooled buffer. Cuts the
        // tiny-malloc hot path (~5 alloc + 5 free per UPDATE) to near
        // zero on the sysbench oltp_update_index steady state.
        let WalRecordBuffer {
            mut bytes, charge, ..
        } = wal_record_buf_take(frame_len, txn_id)?;
        bytes.clear();
        // Frame header — lsn / prev_lsn left as zero placeholders, the
        // outer `append_pre_encoded` patches them under the mutex.
        write_frame_header(
            &mut bytes,
            payload_len,
            WalRecordHeader {
                source_time_us: 0,
                lsn: 0,
                prev_txn_lsn: 0,
                txn_id,
                record_type: payload.record_type(),
            },
        );
        encode_payload_into(&payload, &mut bytes)?;
        // CRC combination costs more than hashing a tiny OLTP frame. Hash
        // those frames once after finalizing the header; keep wide payload
        // work outside the mutex and combine it without rescanning.
        let payload_checksum = (prehash && payload_len > 256)
            .then(|| PayloadChecksum::new(&bytes[WAL_FRAME_HEADER_LEN..]));
        let lsn_span = match &payload {
            WalPayload::TxnBatch { records } => records.len() as u64,
            _ => 1,
        };
        Ok(PreEncodedRecord {
            source_time_us: 0,
            payload_checksum,
            bytes,
            bytes_charge: Some(charge),
            payload,
            txn_id,
            lsn_span,
            retain_large_buffer: true,
            #[cfg(test)]
            drop_observer: None,
        })
    }

    /// Append a pre-encoded record under the WAL mutex. The critical
    /// section is now just: reserve LSN, look up prev_txn_lsn, patch
    /// frame header bytes [8..24], `pwrite_at(offset)`, advance
    /// bookkeeping. The encode + CRC work has already happened outside
    /// the mutex in `pre_encode_record`.
    pub fn append_pre_encoded(&mut self, mut rec: PreEncodedRecord) -> Result<u64, QueryError> {
        self.append_pre_encoded_ref(&mut rec)
    }

    /// Release owned payloads and governed buffers after unlocking the WAL.
    /// Large batches can otherwise free thousands of allocations in the
    /// serialization point after their bytes have already been appended.
    pub(crate) fn append_pre_encoded_to(
        log: &Arc<LogMutex>,
        mut rec: PreEncodedRecord,
    ) -> Result<u64, QueryError> {
        let result = {
            let mut guard = log.lock();
            guard.append_pre_encoded_ref(&mut rec)
        };
        drop(rec);
        result
    }

    /// Keep ownership at the caller so a guarded batch can retire its records
    /// after releasing the append mutex.
    pub(crate) fn append_pre_encoded_ref(
        &mut self,
        rec: &mut PreEncodedRecord,
    ) -> Result<u64, QueryError> {
        self.check_local_writer()?;
        if self.read_only {
            return Err(crate::storage::image_io::read_only_error());
        }
        self.append_pre_encoded_core(rec, false)
    }

    /// A transaction's *terminal* WAL record — nothing more will ever link back
    /// to it, so its `txn_last_lsn` back-link entry can be dropped. Committed
    /// txns end at `CommitTxn`; rolled-back txns end at `EndTxn` (which follows
    /// `AbortTxn` + any CLRs). Dropping the entry here is what keeps the map
    /// bounded — without it every distinct `txn_id` leaks one entry forever,
    /// growing RAM and slowly degrading the per-record lookup on a long-running
    /// server. Recovery does not depend on this map (it rebuilds `prev_txn_lsn`
    /// links straight from the persisted frame bytes), so removal is purely a
    /// runtime-memory concern.
    #[inline]
    fn is_terminal_txn_record(payload: &WalPayload) -> bool {
        matches!(payload, WalPayload::CommitTxn { .. } | WalPayload::EndTxn)
            || matches!(
                payload,
                WalPayload::TxnBatch { records }
                    if matches!(records.last(), Some(WalPayload::CommitTxn { .. }))
            )
    }

    /// Append a commit-ending ordered transaction tail as one physical WAL
    /// frame. The outer CRC is the atomicity boundary; callers must keep
    /// mid-transaction/loser records on the ordinary append path.
    pub fn append_txn_batch(
        &mut self,
        txn_id: u64,
        records: Vec<WalPayload>,
    ) -> Result<u64, QueryError> {
        if self.read_only {
            return Err(crate::storage::image_io::read_only_error());
        }
        validate_txn_batch_records(&records)?;
        let pre = Self::pre_encode_record(txn_id, WalPayload::TxnBatch { records })?;
        self.append_pre_encoded(pre)
    }

    pub(crate) fn append_administrative_file(
        &mut self,
        path: String,
        after: Vec<u8>,
    ) -> Result<u64, QueryError> {
        self.check_local_writer()?;
        if self.read_only {
            return Err(crate::storage::image_io::read_only_error());
        }
        let mut record = Self::pre_encode_record(0, WalPayload::FileCreate { path, after })?;
        self.append_pre_encoded_core(&mut record, true)
    }
    fn append_pre_encoded_core(
        &mut self,
        rec: &mut PreEncodedRecord,
        administrative: bool,
    ) -> Result<u64, QueryError> {
        let _disk = if !administrative {
            self.check_wal_budget(&rec.payload, rec.bytes.len() as u64)?
        } else {
            self.budget
                .as_ref()
                .map(|b| b.reserve_disk(rec.bytes.len() as u64, true))
                .transpose()?
                .flatten()
        };
        let source_time = if self.local_recovery {
            0
        } else if rec.source_time_us != 0 {
            rec.source_time_us
        } else {
            advance_ordered_time(&self.source_clock)
        };
        rec.bytes[36..44].copy_from_slice(&source_time.to_le_bytes());
        self.last_frame_time = source_time;
        let lsn_span = rec.lsn_span.max(1);
        let lsn = self.next_lsn.saturating_add(lsn_span - 1);
        self.next_lsn = self.next_lsn.saturating_add(lsn_span);
        self.state_atomics
            .next_lsn
            .store(self.next_lsn, Ordering::Release);
        let prev_txn_lsn = if rec.txn_id == 0 {
            0
        } else {
            self.txn_last_lsn.get(&rec.txn_id).copied().unwrap_or(0)
        };
        // Protect every header field as well as the payload. Wide payloads
        // were hashed outside the mutex; tiny frames avoid combination work.
        rec.bytes[8..16].copy_from_slice(&lsn.to_le_bytes());
        rec.bytes[16..24].copy_from_slice(&prev_txn_lsn.to_le_bytes());
        let checksum = if let Some(payload_checksum) = &rec.payload_checksum {
            payload_checksum.with_prefix(crc32fast::hash(&rec.bytes[..44]))
        } else {
            wal_frame_checksum(&rec.bytes, &rec.bytes[WAL_FRAME_HEADER_LEN..])
        };
        rec.bytes[44..48].copy_from_slice(&checksum.to_le_bytes());
        if let Err(err) = self.buffer_frame_bytes(&rec.bytes) {
            rec.retain_large_buffer = false;
            return Err(err);
        }
        if let Some(budget) = &self.budget {
            budget.appended(rec.bytes.len() as u64);
        }
        if self.active_info.first_lsn > self.active_info.last_lsn {
            self.active_info.first_lsn = lsn.saturating_add(1).saturating_sub(lsn_span);
        }
        self.active_info.last_lsn = lsn;
        if rec.txn_id != 0 {
            if Self::is_terminal_txn_record(&rec.payload) {
                self.txn_last_lsn.remove(&rec.txn_id);
            } else {
                self.txn_last_lsn.insert(rec.txn_id, lsn);
            }
        }
        accumulate_wal_stats(&mut self.stats, &rec.payload);
        let _ = self.new_lsn_tx.send(lsn);

        if self.active_info.size_bytes >= self.max_segment_bytes {
            rec.retain_large_buffer = false;
            self.rotate_active_internal()?;
        }
        Ok(lsn)
    }

    fn reserve_buffer_capacity(
        bytes: &mut Vec<u8>,
        charge: &mut ControlledMemoryCharge,
        required: usize,
        label: &str,
    ) -> Result<(), QueryError> {
        // LogManager is normally called under its mutex. A failed grow must
        // not re-enter dirty-page eviction and attempt to acquire it again.
        let _admission = crate::engine::executor::mem_budget::StorageLatchAdmissionScope::enter();
        if required <= bytes.capacity() {
            return Ok(());
        }
        debug_assert!(
            bytes.is_empty(),
            "governed WAL buffers must be drained before capacity growth"
        );
        let old_capacity = bytes.capacity();
        let requested_growth = required.saturating_sub(old_capacity);
        charge.try_grow(requested_growth)?;
        if bytes
            .try_reserve_exact(required.saturating_sub(bytes.len()))
            .is_err()
        {
            charge.shrink(requested_growth);
            return Err(error(
                crate::error::catalog::ER_OUTOFMEMORY,
                format!("Out of memory growing {label} to {required} bytes"),
            ));
        }
        let actual_capacity = bytes.capacity();
        if actual_capacity > required {
            if let Err(err) = charge.try_grow(actual_capacity - required) {
                *bytes = Vec::new();
                charge.shrink(charge.bytes());
                return Err(err);
            }
        } else if actual_capacity < required {
            charge.shrink(required - actual_capacity);
        }
        Ok(())
    }

    fn reserve_encode_scratch(&mut self, required: usize) -> Result<(), QueryError> {
        Self::reserve_buffer_capacity(
            &mut self.encode_scratch,
            &mut self.encode_scratch_charge,
            required,
            "WAL encode buffer",
        )
    }

    fn reserve_pending_write(&mut self, additional: usize) -> Result<(), QueryError> {
        if self.pending_write_bytes.len().saturating_add(additional)
            > self.pending_write_bytes.capacity()
        {
            self.flush_pending_writes()?;
        }
        let required = self.pending_write_bytes.len().saturating_add(additional);
        Self::reserve_buffer_capacity(
            &mut self.pending_write_bytes,
            &mut self.pending_write_charge,
            required,
            "WAL write buffer",
        )
    }

    fn shrink_encode_scratch(&mut self) {
        let old_capacity = self.encode_scratch.capacity();
        if old_capacity <= WAL_ENCODE_SCRATCH_RETAIN_LIMIT {
            self.large_scratch_txn = None;
            return;
        }
        self.encode_scratch.clear();
        self.encode_scratch.shrink_to(WAL_ENCODE_SCRATCH_IDLE_BYTES);
        let new_capacity = self.encode_scratch.capacity();
        self.encode_scratch_charge
            .shrink(old_capacity.saturating_sub(new_capacity));
        self.large_scratch_txn = None;
    }

    fn direct_write_bytes(&mut self, bytes: &[u8]) -> Result<(), QueryError> {
        self.flush_pending_writes()?;
        let offset = self.active_info.size_bytes;
        write_all_at(&self.active_file, bytes, offset)?;
        self.record_physical_write(bytes.len());
        self.active_info.size_bytes = self
            .active_info
            .size_bytes
            .saturating_add(bytes.len() as u64);
        self.state_atomics
            .active_size_bytes
            .store(self.active_info.size_bytes, Ordering::Release);
        self.pending_write_offset = self.active_info.size_bytes;
        Ok(())
    }

    fn buffer_frame_bytes(&mut self, bytes: &[u8]) -> Result<(), QueryError> {
        if bytes.is_empty() {
            return Ok(());
        }
        if !self.write_buffer_enabled {
            write_all_at(&self.active_file, bytes, self.active_info.size_bytes)?;
            self.record_physical_write(bytes.len());
            self.active_info.size_bytes = self
                .active_info
                .size_bytes
                .saturating_add(bytes.len() as u64);
            self.state_atomics
                .active_size_bytes
                .store(self.active_info.size_bytes, Ordering::Release);
            return Ok(());
        }
        if bytes.len() >= self.write_buffer_bytes {
            return self.direct_write_bytes(bytes);
        }
        if self.pending_write_bytes.is_empty() {
            self.pending_write_offset = self.active_info.size_bytes;
        }
        self.reserve_pending_write(bytes.len())?;
        self.pending_write_bytes.extend_from_slice(bytes);
        self.active_info.size_bytes = self
            .active_info
            .size_bytes
            .saturating_add(bytes.len() as u64);
        self.state_atomics
            .active_size_bytes
            .store(self.active_info.size_bytes, Ordering::Release);
        if self.pending_write_bytes.len() >= self.write_buffer_bytes {
            self.flush_pending_writes()?;
        }
        Ok(())
    }

    fn buffer_encode_scratch(&mut self) -> Result<(), QueryError> {
        if self.encode_scratch.is_empty() {
            return Ok(());
        }
        if !self.write_buffer_enabled {
            write_all_at(
                &self.active_file,
                &self.encode_scratch,
                self.active_info.size_bytes,
            )?;
            self.record_physical_write(self.encode_scratch.len());
            self.active_info.size_bytes = self
                .active_info
                .size_bytes
                .saturating_add(self.encode_scratch.len() as u64);
            self.state_atomics
                .active_size_bytes
                .store(self.active_info.size_bytes, Ordering::Release);
            return Ok(());
        }
        if self.encode_scratch.len() >= self.write_buffer_bytes {
            self.flush_pending_writes()?;
            let offset = self.active_info.size_bytes;
            write_all_at(&self.active_file, &self.encode_scratch, offset)?;
            self.record_physical_write(self.encode_scratch.len());
            self.active_info.size_bytes = self
                .active_info
                .size_bytes
                .saturating_add(self.encode_scratch.len() as u64);
            self.state_atomics
                .active_size_bytes
                .store(self.active_info.size_bytes, Ordering::Release);
            self.pending_write_offset = self.active_info.size_bytes;
            return Ok(());
        }
        if self.pending_write_bytes.is_empty() {
            self.pending_write_offset = self.active_info.size_bytes;
        }
        self.reserve_pending_write(self.encode_scratch.len())?;
        self.pending_write_bytes
            .extend_from_slice(&self.encode_scratch);
        self.active_info.size_bytes = self
            .active_info
            .size_bytes
            .saturating_add(self.encode_scratch.len() as u64);
        self.state_atomics
            .active_size_bytes
            .store(self.active_info.size_bytes, Ordering::Release);
        if self.pending_write_bytes.len() >= self.write_buffer_bytes {
            self.flush_pending_writes()?;
        }
        Ok(())
    }

    fn flush_pending_writes(&mut self) -> Result<(), QueryError> {
        if self.pending_write_bytes.is_empty() {
            self.pending_write_offset = self.active_info.size_bytes;
            return Ok(());
        }
        write_all_at(
            &self.active_file,
            &self.pending_write_bytes,
            self.pending_write_offset,
        )?;
        self.record_physical_write(self.pending_write_bytes.len());
        self.pending_write_bytes.clear();
        if self.pending_write_bytes.capacity() > self.write_buffer_bytes.saturating_mul(4) {
            let old_capacity = self.pending_write_bytes.capacity();
            self.pending_write_bytes.shrink_to(self.write_buffer_bytes);
            self.pending_write_charge
                .shrink(old_capacity.saturating_sub(self.pending_write_bytes.capacity()));
        }
        self.pending_write_offset = self.active_info.size_bytes;
        Ok(())
    }

    fn record_physical_write(&mut self, bytes: usize) {
        self.stats.physical_write_calls = self.stats.physical_write_calls.saturating_add(1);
        self.stats.physical_write_bytes =
            self.stats.physical_write_bytes.saturating_add(bytes as u64);
    }

    pub(crate) fn last_txn_lsn(&self, txn_id: u64) -> u64 {
        self.txn_last_lsn.get(&txn_id).copied().unwrap_or(0)
    }

    pub(crate) fn set_physical_replica(&mut self, enabled: bool) {
        self.state_atomics
            .physical_replica
            .store(enabled, Ordering::Release);
    }

    fn check_local_writer(&self) -> Result<(), QueryError> {
        if self.state_atomics.physical_replica() && !promotion_wal_active() {
            return Err(error(
                crate::error::catalog::ER_OPTION_PREVENTS_STATEMENT,
                "local WAL writes are disabled on a physical replica",
            ));
        }
        Ok(())
    }

    pub fn reserve_lsn(&mut self) -> Result<u64, QueryError> {
        self.check_local_writer()?;
        let lsn = self.next_lsn;
        self.next_lsn += 1;
        // Mirror to the lock-free atomic so `Database::should_checkpoint`
        // can read it without locking us. Release ordering pairs with
        // the Acquire load on the reader side.
        self.state_atomics
            .next_lsn
            .store(self.next_lsn, Ordering::Release);
        Ok(lsn)
    }

    pub fn append(&mut self, txn_id: u64, payload: WalPayload) -> Result<u64, QueryError> {
        if self.read_only {
            return Err(crate::storage::image_io::read_only_error());
        }
        let lsn = self.reserve_lsn()?;
        self.append_reserved(lsn, txn_id, payload)
    }

    pub fn append_reserved(
        &mut self,
        lsn: u64,
        txn_id: u64,
        payload: WalPayload,
    ) -> Result<u64, QueryError> {
        if lsn <= self.last_lsn() {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                format!("WAL append LSN {lsn} does not follow {}", self.last_lsn()),
            ));
        }
        let _admission = crate::engine::executor::mem_budget::StorageLatchAdmissionScope::enter();
        if matches!(&payload, WalPayload::PageDelta { .. }) && txn_id != 0 {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "page delta must be system-scoped",
            ));
        }
        if self.read_only {
            return Err(crate::storage::image_io::read_only_error());
        }
        if self.large_scratch_txn.is_some_and(|owner| owner != txn_id) {
            self.shrink_encode_scratch();
        }
        let estimated_len = WAL_FRAME_HEADER_LEN
            .checked_add(encoded_payload_len(&payload)?)
            .ok_or_else(|| error(crate::error::catalog::ER_OUTOFMEMORY, "WAL size overflow"))?;
        let _disk = match self.check_wal_budget(&payload, estimated_len as u64) {
            Ok(token) => token,
            Err(error) => {
                if self.next_lsn == lsn + 1 && self.last_lsn() < lsn {
                    self.next_lsn = lsn;
                    self.state_atomics.next_lsn.store(lsn, Ordering::Release);
                }
                return Err(error);
            }
        };
        if lsn >= self.next_lsn {
            self.next_lsn = lsn + 1;
            self.state_atomics
                .next_lsn
                .store(self.next_lsn, Ordering::Release);
        }
        let prev_txn_lsn = if txn_id == 0 {
            0
        } else {
            self.txn_last_lsn.get(&txn_id).copied().unwrap_or(0)
        };
        let source_time_us = if self.local_recovery {
            0
        } else {
            advance_ordered_time(&self.source_clock)
        };
        self.last_frame_time = source_time_us;
        let record = WalRecord {
            source_time_us,
            lsn,
            prev_txn_lsn,
            txn_id,
            payload,
        };
        let terminal = Self::is_terminal_txn_record(&record.payload);
        let release_scratch = terminal || matches!(&record.payload, WalPayload::AbortTxn);
        let frame_len = WAL_FRAME_HEADER_LEN
            .checked_add(encoded_payload_len(&record.payload)?)
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_OUTOFMEMORY,
                    "WAL frame length overflow",
                )
            })?;
        // Reuse the long-lived scratch buffer instead of allocating a
        // fresh Vec per append; we never need the encoded bytes after
        // the write returns.
        self.encode_scratch.clear();
        self.reserve_encode_scratch(frame_len)?;
        encode_record_into(&record, &mut self.encode_scratch)?;
        // Append at the explicit valid-bytes offset. The active segment
        // is opened in plain read+write mode (not `append(true)`)
        // because preallocation makes the file's on-disk size larger
        // than the watermark we want to write at; positional `pwrite`
        // (`FileExt::write_at`) lands the bytes at the correct
        // offset and leaves the reserved tail of zeros untouched. The
        // outer `LogManager` mutex already serialises appends, so the
        // shared `size_bytes` cursor is race-free.
        if let Err(err) = self.buffer_encode_scratch() {
            self.shrink_encode_scratch();
            return Err(err);
        }
        if let Some(budget) = &self.budget {
            budget.appended(frame_len as u64);
        }
        // Large records from one active transaction are a stream, not a
        // sequence of one-offs. Retain the charged allocation until that
        // transaction terminates; a small terminal record then releases it.
        // Non-transactional large records are still shrunk immediately.
        if frame_len >= WAL_ENCODE_SCRATCH_REUSE_MIN_BYTES && txn_id != 0 && !release_scratch {
            self.large_scratch_txn = Some(txn_id);
        } else if release_scratch
            || txn_id == 0
            || (frame_len < WAL_ENCODE_SCRATCH_REUSE_MIN_BYTES && self.large_scratch_txn.is_none())
        {
            self.shrink_encode_scratch();
        }
        // First-time bookkeeping: a fresh segment starts with
        // first_lsn=next_lsn and last_lsn=first_lsn-1 (i.e. empty).
        // Once a record lands, last_lsn = lsn and first_lsn stays.
        if self.active_info.first_lsn > self.active_info.last_lsn {
            self.active_info.first_lsn = lsn;
        }
        self.active_info.last_lsn = lsn;
        if txn_id != 0 {
            if Self::is_terminal_txn_record(&record.payload) {
                self.txn_last_lsn.remove(&txn_id);
            } else {
                self.txn_last_lsn.insert(txn_id, lsn);
            }
        }
        accumulate_wal_stats(&mut self.stats, &record.payload);
        // Notify physical-replication readers that a new LSN exists.
        // The `_` ignores the "no receivers" case — only ever the
        // sender side of `watch::channel` and we can drop sends.
        let _ = self.new_lsn_tx.send(lsn);

        // Rotate the active segment if it has grown past the threshold.
        // We rotate AFTER the write so records never cross segments.
        if self.active_info.size_bytes >= self.max_segment_bytes {
            self.rotate_active_internal()?;
        }
        Ok(lsn)
    }

    pub(crate) fn append_replication_frame(
        &mut self,
        bytes: &[u8],
        frame: &ReplicationFrame,
    ) -> Result<(), QueryError> {
        if self.read_only {
            return Err(crate::storage::image_io::read_only_error());
        }
        let header = frame.header;
        self.source_clock
            .fetch_max(header.source_time_us, Ordering::Relaxed);
        self.last_frame_time = header.source_time_us;
        if header.lsn <= self.last_lsn() {
            return Err(io_err(std::io::Error::other(
                "replication WAL LSN regression",
            )));
        }
        let span = match &frame.payload {
            WalPayload::TxnBatch { records } => records.len() as u64,
            _ => 1,
        };
        let first = header
            .lsn
            .checked_add(1)
            .and_then(|end| end.checked_sub(span))
            .filter(|first| *first != 0)
            .ok_or_else(|| io_err(std::io::Error::other("invalid replication LSN span")))?;
        let next_lsn = header
            .lsn
            .checked_add(1)
            .ok_or_else(|| io_err(std::io::Error::other("replication LSN overflow")))?;
        self.buffer_frame_bytes(bytes)?;
        if self.active_info.first_lsn > self.active_info.last_lsn {
            self.active_info.first_lsn = first;
        }
        self.active_info.last_lsn = header.lsn;
        self.next_lsn = next_lsn;
        self.state_atomics
            .next_lsn
            .store(self.next_lsn, Ordering::Release);
        if header.txn_id != 0 {
            if Self::is_terminal_txn_record(&frame.payload) {
                self.txn_last_lsn.remove(&header.txn_id);
            } else {
                self.txn_last_lsn.insert(header.txn_id, header.lsn);
            }
        }
        accumulate_wal_stats(&mut self.stats, &frame.payload);
        let _ = self.new_lsn_tx.send(header.lsn);
        if self.active_info.size_bytes >= self.max_segment_bytes {
            self.rotate_active_internal()?;
        }
        Ok(())
    }

    /// Persist an allocation boundary above every previously allocated LSN.
    /// The system-scoped EndTxn has no user-transaction or redo effect. It
    /// keeps recovery's allocator above the returned boundary even if older
    /// allocations were still buffered when this administrative barrier ran.
    pub(crate) fn sync_allocation_boundary(log: &Arc<LogMutex>) -> Result<u64, QueryError> {
        let boundary = log.lock().append(0, WalPayload::EndTxn)?;
        Self::sync_durable_to(log, boundary)?;
        Ok(boundary)
    }

    /// Drive `prepare_sync_to` → `fsync` → `finish_sync_to` under a
    /// briefly-held mutex on the WAL. Doing it this way means the
    /// actual `fsync` runs without the mutex held, so concurrent
    /// `append` callers don't pile up behind a checkpoint /
    /// buffer-pool flush waiting for the disk. The legacy
    /// `LogManager::sync_to` (which used to do the fsync inline with
    /// the mutex held) is gone; every caller now reaches durability
    /// through this entry point. Returns immediately if `lsn` is
    /// already durable.
    pub fn sync_durable_to(log: &Arc<LogMutex>, lsn: u64) -> Result<(), QueryError> {
        Self::sync_durable_high_water_to(log, lsn).map(|_| ())
    }

    /// Share one durability barrier between committers and page cleaners.
    /// Return the durable high-water and whether this caller performed I/O.
    pub(crate) fn sync_durable_high_water_to(
        log: &Arc<LogMutex>,
        lsn: u64,
    ) -> Result<(u64, bool), QueryError> {
        // Only durability barriers are serialized. Never hold the WAL append
        // mutex while waiting for the current fsync: the leader may need it
        // to publish its high-water, and appenders must remain independent of
        // storage latency. Once admitted, prepare_sync_to re-checks
        // durable_lsn, collapsing every follower already covered by the
        // leader into a no-op.
        let sync_gate = {
            let log_guard = log.lock();
            // A page cleaner with an already durable image must not queue
            // behind an unrelated commit's in-flight fsync.
            if lsn <= log_guard.durable_lsn && !log_guard.directory_sync_pending {
                return Ok((log_guard.durable_lsn, false));
            }
            Arc::clone(&log_guard.sync_gate)
        };
        let _sync_guard = sync_gate.lock();
        let (sync_files, high_water) = {
            let mut log_guard = log.lock();
            match log_guard.prepare_sync_to(lsn)? {
                Some(prepared) => prepared,
                None => return Ok((log_guard.durable_lsn, false)),
            }
        };
        let started = Instant::now();
        let fsync_result = sync_files.iter().try_for_each(|file| sync_wal_file(file));
        let elapsed_us = elapsed_us(started);
        let mut log_guard = log.lock();
        // Drop the cloned fd before we publish the durable LSN —
        // makes a leak / partial state easier to spot in profilers,
        // and matches the explicit close that `GroupCommitCoordinator`
        // performs on the cloned fd via `Drop`.
        drop(sync_files);
        fsync_result?;
        // Advance to the flushed high-water, not just the requested LSN:
        // the single fsync above made every byte up to `high_water`
        // durable, so reporting the smaller `lsn` would force later
        // committers whose records were already covered to fsync again.
        log_guard.publish_synced_prefix(high_water)?;
        log_guard.finish_sync_to(high_water, elapsed_us);
        Ok((log_guard.durable_lsn, true))
    }

    /// Prepare an fsync up to (at least) `lsn`. Flushes the in-memory WAL
    /// buffer to the OS file and returns a cloned fd to fsync **plus the
    /// actual high-water LSN** now resident in that file. Because a single
    /// fsync of the returned fd makes every byte up to the high-water
    /// durable, callers advance their durable watermark to the returned
    /// high-water (≥ `lsn`) rather than to `lsn` — this is what lets a
    /// group-commit leader release natural-batch stragglers (committers who
    /// appended during the fsync) for free instead of making each pay its
    /// own fsync. Returns `None` when `lsn` is already durable.
    pub(crate) fn prepare_sync_to(
        &mut self,
        lsn: u64,
    ) -> Result<Option<(Vec<Arc<File>>, u64)>, QueryError> {
        if self.directory_sync_pending {
            self.publish_synced_prefix(self.durable_lsn)?;
        }
        if lsn <= self.durable_lsn {
            return Ok(None);
        }
        self.flush_pending_writes()?;
        self.active_file.flush().map_err(io_err)?;
        // `last_lsn` is captured under the log mutex right after the flush, so
        // it is a conservative lower bound on what the fsync will persist:
        // records appended *after* this point are not guaranteed flushed and
        // are deliberately excluded. Never below the requested `lsn` (the
        // caller's record was appended, so `last_lsn >= lsn`), but `.max` keeps
        // it defensive.
        let high_water = self
            .active_info
            .last_lsn
            .max(
                self.pending_sealed_sync
                    .last()
                    .map(|s| s.last_lsn)
                    .unwrap_or(0),
            )
            .max(lsn);
        let mut files = self
            .pending_sealed_sync
            .iter()
            .map(|sealed| Arc::clone(&sealed.file))
            .collect::<Vec<_>>();
        // An empty freshly-rotated active segment has no bytes relevant to
        // this barrier. Otherwise sync it after every sealed predecessor.
        if self.active_info.first_lsn <= self.active_info.last_lsn {
            files.push(Arc::clone(&self.active_file));
        }
        Ok(Some((files, high_water)))
    }

    /// Highest LSN whose bytes are in the WAL write buffer / file (not
    /// necessarily fsynced). Used by the relaxed-durability group-commit
    /// barrier, whose `sync_op` only flushes to the OS page cache: after the
    /// flush, everything up to `last_lsn` is in the OS cache, so the
    /// coordinator can advance its (flush-level) durable watermark to it.
    pub fn last_lsn(&self) -> u64 {
        self.active_info.last_lsn
    }

    pub fn finish_sync_to(&mut self, lsn: u64, elapsed_us: u64) {
        if lsn <= self.durable_lsn {
            return;
        }
        self.durable_lsn = lsn;
        self.state_atomics.durable_lsn.store(lsn, Ordering::Release);
        self.pending_sealed_sync
            .retain(|sealed| sealed.last_lsn > lsn);
        let _ = self.durable_lsn_tx.send(lsn);
        self.sync_count = self.sync_count.saturating_add(1);
        self.stats.sync_total_us = self.stats.sync_total_us.saturating_add(elapsed_us);
        self.stats.sync_max_us = self.stats.sync_max_us.max(elapsed_us);
    }

    fn publish_synced_prefix(&mut self, lsn: u64) -> Result<(), QueryError> {
        let mut published = 0;
        for id in &self.pending_publications {
            let segment = if *id == self.active_info.segment_id {
                Some(&mut self.active_info)
            } else {
                self.segments
                    .binary_search_by_key(id, |segment| segment.segment_id)
                    .ok()
                    .map(|index| &mut self.segments[index])
            };
            let Some(segment) = segment else {
                published += 1;
                continue;
            };
            if segment.first_lsn > lsn
                && !(segment.first_lsn > segment.last_lsn && segment.last_lsn <= lsn)
            {
                break;
            }
            if segment.path.extension().is_some_and(|ext| ext == "pending") {
                let path = segment_path(&self.dir, segment.segment_id);
                fs::rename(&segment.path, &path).map_err(io_err)?;
                segment.path = path;
                self.directory_sync_pending = true;
            }
            published += 1;
        }
        self.pending_publications.drain(..published);
        if self.directory_sync_pending {
            crate::platform::fs::sync_path(&self.dir).map_err(io_err)?;
            self.directory_sync_pending = false;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), QueryError> {
        self.flush_pending_writes()?;
        self.active_file.flush().map_err(io_err)?;
        // Relaxed commits still preserve process-crash recovery. Publishing a
        // rotated successor requires a durable preceding prefix; pay this
        // barrier only at rotation, not on ordinary relaxed commits.
        let last = self.last_lsn();
        if self.directory_sync_pending || !self.pending_publications.is_empty() {
            for sealed in &self.pending_sealed_sync {
                sync_wal_file(&sealed.file)?;
            }
            sync_wal_file(&self.active_file)?;
            self.publish_synced_prefix(last)?;
            self.finish_sync_to(last, 0);
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn compact_to_checkpoint(
        &mut self,
        checkpoint_lsn: u64,
        next_txn_id: u64,
        next_scn: u64,
    ) -> Result<(), QueryError> {
        // No retention floor below the checkpoint LSN — equivalent to a
        // master that has no replication slots.
        self.compact_to_checkpoint_with_min_required(
            checkpoint_lsn,
            next_txn_id,
            next_scn,
            checkpoint_lsn,
            true,
        )
    }

    #[cfg(test)]
    /// Checkpoint variant that respects an external retention floor.
    /// `min_required_lsn` is typically `min(checkpoint_lsn, slot_min_acked_lsn)` —
    /// when a replication slot still needs records below `checkpoint_lsn`, we
    /// must NOT collapse the active segment (its history is still required)
    /// and must NOT prune sealed segments that contain LSNs at or above the
    /// floor. When `min_required_lsn >= checkpoint_lsn`, this behaves
    /// identically to the slot-less path.
    pub fn compact_to_checkpoint_with_min_required(
        &mut self,
        checkpoint_lsn: u64,
        next_txn_id: u64,
        next_scn: u64,
        min_required_lsn: u64,
        allow_active_collapse: bool,
    ) -> Result<(), QueryError> {
        self.compact_to_checkpoint_with_registration(
            checkpoint_lsn,
            next_txn_id,
            next_scn,
            min_required_lsn,
            allow_active_collapse,
            0,
        )
    }

    #[cfg(test)]
    pub(crate) fn compact_to_checkpoint_with_registration(
        &mut self,
        checkpoint_lsn: u64,
        next_txn_id: u64,
        next_scn: u64,
        min_required_lsn: u64,
        allow_active_collapse: bool,
        gc_safe_txn_frontier: u64,
    ) -> Result<(), QueryError> {
        self.compact_checkpoint(
            checkpoint_lsn,
            next_txn_id,
            next_scn,
            min_required_lsn,
            allow_active_collapse,
            gc_safe_txn_frontier,
            false,
        )
    }

    fn compact_checkpoint(
        &mut self,
        checkpoint_lsn: u64,
        next_txn_id: u64,
        next_scn: u64,
        min_required_lsn: u64,
        allow_active_collapse: bool,
        gc_safe_txn_frontier: u64,
        defer_cleanup: bool,
    ) -> Result<(), QueryError> {
        self.flush_pending_writes()?;
        // Collapse only when the WAL tail is clean: nothing was appended
        // after the CheckpointEnd at `checkpoint_lsn`. (`next_lsn ==
        // checkpoint_lsn` happens when the caller synthesizes the record
        // at a fresh LSN instead of appending it first — equally clean.)
        // The check runs under the same `LogManager` mutex that
        // serialises `append`, so a record appended by a concurrent
        // transaction between the CheckpointEnd sync and this call can
        // never be destroyed — we observe it here and fall back to
        // prune-only.
        if allow_active_collapse
            && min_required_lsn >= checkpoint_lsn
            && self.next_lsn <= checkpoint_lsn.saturating_add(1)
        {
            return self.compact_collapsing_active(
                checkpoint_lsn,
                next_txn_id,
                next_scn,
                gc_safe_txn_frontier,
                defer_cleanup,
            );
        }
        // Records at or above the floor are still needed: in-flight
        // transaction undo chains, replication-slot history, or appends
        // that raced past the CheckpointEnd. Keep the active segment
        // untouched and prune sealed segments entirely below the floor.
        // Everything below `checkpoint_lsn` is already durable in the
        // data files (the checkpoint flushed before appending
        // CheckpointEnd), so the floor is capped at `checkpoint_lsn`.
        if !defer_cleanup {
            self.prune_segments_below_lsn(min_required_lsn.min(checkpoint_lsn))?;
        }
        // CheckpointEnd is already in the active segment from append_wal,
        // so we do NOT advance `next_lsn` / `durable_lsn` / etc. here.
        Ok(())
    }

    fn compact_collapsing_active(
        &mut self,
        checkpoint_lsn: u64,
        next_txn_id: u64,
        next_scn: u64,
        gc_safe_txn_frontier: u64,
        defer_cleanup: bool,
    ) -> Result<(), QueryError> {
        // Build the new "first record after checkpoint" — a fresh
        // CheckpointEnd at `checkpoint_lsn` — in a REPLACEMENT segment
        // (id = active + 1), fsync it, rename it into place, and only
        // then delete the obsolete segments. Crash-atomic at every step:
        //  - before the rename the replacement is invisible (the `.tmp`
        //    suffix is ignored by `discover_segments`) and the old
        //    segment set is intact;
        //  - after the rename but before the deletes finish, both
        //    generations coexist — every old record is below
        //    `checkpoint_lsn`, hence already durable in the data files,
        //    and recovery's checkpoint cutoff skips its redo.
        // The previous in-place `set_len(0)` rewrite had a crash window
        // that left the active segment empty with the sealed segments
        // gone, rewinding `next_lsn` for the whole system on restart.
        let record = WalRecord {
            source_time_us: self.last_frame_time,
            lsn: checkpoint_lsn,
            prev_txn_lsn: 0,
            txn_id: 0,
            payload: WalPayload::CheckpointEnd {
                redo_lsn: checkpoint_lsn,
                next_txn_id,
                next_scn,
                auto_increment_covered_lsn: checkpoint_lsn,
                sequence_covered_lsn: checkpoint_lsn,
                gc_safe_txn_frontier,
            },
        };
        let encoded = encode_record(&record)?;
        let started = Instant::now();
        let new_id = self.active_info.segment_id.saturating_add(1);
        let new_path = segment_path(&self.dir, new_id);
        let tmp_path = new_path.with_extension("log.tmp");
        let valid_bytes = (WAL_HEADER_LEN + encoded.len()) as u64;
        {
            let mut tmp = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)
                .map_err(io_err)?;
            // Reserve the full segment up front so the new active
            // segment carries the same `fdatasync`-fast-path property
            // as a freshly-rotated one. Best-effort: failure leaves a
            // sparse segment, which still works (just falls back to
            // metadata-journal fsyncs until the next rotation).
            let _ = preallocate_segment(&tmp, self.max_segment_bytes);
            tmp.write_all(&segment_header_with_replacement(new_id, checkpoint_lsn))
                .map_err(io_err)?;
            tmp.write_all(&encoded).map_err(io_err)?;
            // Order both data and extent metadata before rename. The full
            // directory sync below makes the new segment durable before any
            // old generation can be reclaimed.
            sync_before_directory_publication(&tmp, &tmp_path).map_err(io_err)?;
        }
        // Open before publication so adoption has no fallible IO after changing
        // segment bookkeeping. This descriptor follows the rename atomically.
        let replacement = Arc::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&tmp_path)
                .map_err(io_err)?,
        );
        wal_cleanup_failpoint(3).map_err(io_err)?;
        fs::rename(&tmp_path, &new_path).map_err(io_err)?;
        // Persist the rename. Errors must propagate here — without a
        // durable directory entry a crash could lose the new segment
        // entirely, leaving recovery to see only the (about-to-be-
        // deleted) old generations and unable to find the LSN we just
        // promised to durable.

        self.directory_sync_pending = true;
        let publication =
            wal_cleanup_failpoint(5).and_then(|()| crate::platform::fs::sync_path(&self.dir));
        if publication.is_ok() {
            self.directory_sync_pending = false;
        }
        // Register the old active generation for retryable reclamation. The
        // checkpoint entry point performs archive/unlink/directory sync unlocked.
        let mut old_active = self.active_info.clone();
        old_active.status = WalSegmentStatus::Reclaimable;
        // The old CheckpointEnd itself has a durable replacement; only the
        // preceding records count towards this generation's retention range.
        old_active.last_lsn = checkpoint_lsn.saturating_sub(1);
        self.segments.push(old_active);
        // Adopt the replacement as the active segment.
        self.active_file = replacement;
        self.active_fill = SegmentFill::Reserve;

        self.next_lsn = checkpoint_lsn.saturating_add(1).max(FIRST_LSN);
        self.state_atomics
            .next_lsn
            .store(self.next_lsn, Ordering::Release);
        self.durable_lsn = checkpoint_lsn;
        self.state_atomics
            .durable_lsn
            .store(checkpoint_lsn, Ordering::Release);
        let _ = self.durable_lsn_tx.send(checkpoint_lsn);
        self.sync_count = self.sync_count.saturating_add(1);
        let elapsed_us = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        self.stats.sync_total_us = self.stats.sync_total_us.saturating_add(elapsed_us);
        self.stats.sync_max_us = self.stats.sync_max_us.max(elapsed_us);
        self.txn_last_lsn.clear();
        // Logical size — the bytes that `discover_segments` / scan will
        // actually traverse. After preallocation `fs::metadata().len()`
        // returns the reserved extent (up to `max_segment_bytes`); the
        // bookkeeping wants the count of valid WAL bytes.
        let new_size_bytes = valid_bytes;
        self.active_info = WalSegmentInfo {
            segment_id: new_id,
            path: new_path,
            first_lsn: checkpoint_lsn,
            last_lsn: checkpoint_lsn,
            size_bytes: new_size_bytes,
            status: WalSegmentStatus::Active,
        };
        // Active segment was replaced. Segments whose archive/unlink failed
        // remain tracked and charged so a later checkpoint can retry them.
        self.state_atomics
            .active_size_bytes
            .store(new_size_bytes, Ordering::Release);
        self.pending_write_offset = new_size_bytes;
        self.pending_write_bytes.clear();
        self.shrink_encode_scratch();
        self.state_atomics.sealed_size_bytes.store(
            self.segments.iter().map(|seg| seg.size_bytes).sum(),
            Ordering::Release,
        );
        let mut stats = WalStats::default();
        stats.sync_total_us = self.stats.sync_total_us;
        stats.sync_max_us = self.stats.sync_max_us;
        accumulate_wal_stats(&mut stats, &record.payload);
        self.stats = stats;
        let _ = self.new_lsn_tx.send(checkpoint_lsn);
        publication.map_err(io_err)?;
        if !defer_cleanup {
            self.prune_segments_below_lsn(checkpoint_lsn)?;
        }
        Ok(())
    }

    /// CheckpointEnd is already durable. Reclamation failures do not invalidate it.
    pub(crate) fn reclaim_checkpoint(
        log: &Arc<LogMutex>,
        checkpoint_lsn: u64,
        next_txn_id: u64,
        next_scn: u64,
        floor: u64,
        collapse: bool,
        gc_safe_txn_frontier: u64,
    ) -> Result<(), QueryError> {
        log.lock().compact_checkpoint(
            checkpoint_lsn,
            next_txn_id,
            next_scn,
            floor,
            collapse,
            gc_safe_txn_frontier,
            true,
        )?;
        Self::prune_checkpoint_segments(log, floor.min(checkpoint_lsn))?;
        Ok(())
    }

    pub(crate) fn bind_wal_budget(&mut self, budget: Arc<crate::storage::capture::WalBudget>) {
        self.budget = Some(budget);
    }
    fn check_wal_budget(
        &self,
        payload: &WalPayload,
        bytes: u64,
    ) -> Result<Option<crate::engine::executor::spill_quota::PendingSpillWrite>, QueryError> {
        // Compensation and durability barriers retain an emergency path. They
        // are counted, but can never strand an already admitted transaction.
        if matches!(
            payload,
            WalPayload::BeginTxn { .. }
                | WalPayload::CommitTxn { .. }
                | WalPayload::Clr { .. }
                | WalPayload::FileClr { .. }
                | WalPayload::SpatialIndexClr { .. }
                | WalPayload::RollbackTo { .. }
                | WalPayload::AbortTxn
                | WalPayload::EndTxn
                | WalPayload::CheckpointBegin
                | WalPayload::CheckpointEnd { .. }
        ) {
            return self
                .budget
                .as_ref()
                .map(|b| b.reserve_disk(bytes, true))
                .transpose()
                .map(Option::flatten);
        }
        if let Some(budget) = &self.budget {
            budget.check(bytes)?;
            return budget.reserve_disk(bytes, false);
        }
        Ok(None)
    }
    pub(crate) fn source_clock(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.source_clock)
    }
    pub(crate) fn set_local_recovery(&mut self, local: bool) {
        self.local_recovery = local;
    }

    pub fn next_lsn(&self) -> u64 {
        self.next_lsn
    }

    pub fn durable_lsn(&self) -> u64 {
        self.durable_lsn
    }

    pub fn pending_sealed_sync_count(&self) -> usize {
        self.pending_sealed_sync.len()
    }

    pub fn sync_count(&self) -> u64 {
        self.sync_count
    }

    pub fn wal_bytes(&self) -> u64 {
        // `active_info.size_bytes` is updated atomically with every
        // `write_all` (see `append_reserved`), and on segment truncate
        // (`compact_collapsing_active`), so it tracks the real on-disk
        // size without a per-call `metadata().len()` syscall.
        let sealed: u64 = self.segments.iter().map(|s| s.size_bytes).sum();
        sealed.saturating_add(self.active_info.size_bytes)
    }

    pub fn stats(&self) -> WalStats {
        self.stats
    }

    /// The path of the currently-writing (active) segment file.
    pub fn path(&self) -> &Path {
        &self.active_info.path
    }

    /// Segment id of the currently-writing segment. New segments get
    /// `active_segment_id + 1` on rotation.
    pub fn active_segment_id(&self) -> u64 {
        self.active_info.segment_id
    }

    #[cfg(test)]
    /// Snapshot of just the active segment's metadata (used by SHOW
    /// ENGINE MYDB STATUS and the physical-replication observability).
    pub fn active_segment_info(&self) -> WalSegmentInfo {
        self.active_info.clone()
    }

    /// Test hook: id of the background-prepared next segment, if any.
    #[cfg(test)]
    pub(crate) fn prepared_segment_id(&self) -> Option<u64> {
        self.next_segment.as_ref().map(|p| p.segment_id)
    }
}

// ============================================================
// Replication codec entry points.
//
// `decode_frame_for_replication` decodes a single WAL frame from a
// chunk-of-frames buffer and returns the parsed header + payload along
// with how many bytes were consumed. Used by the replica applier to
// walk through a `WalChunk.frames_bytes` blob without re-implementing
// the WAL frame layout.
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalRecordHeader {
    pub source_time_us: u64,
    pub lsn: u64,
    pub prev_txn_lsn: u64,
    pub txn_id: u64,
    pub record_type: WalRecordType,
}

#[derive(Debug, Clone)]
pub struct ReplicationFrame {
    pub header: WalRecordHeader,
    pub payload: WalPayload,
    pub frame_len: usize,
}

/// v34: source time at [36..44], CRC at [44..48].
/// LSN and time are patched under the WAL mutex; CRC covers header and payload.
#[inline]
fn write_frame_header(out: &mut Vec<u8>, payload_len: usize, header: WalRecordHeader) {
    out.extend_from_slice(&WAL_RECORD_MAGIC.to_le_bytes());
    out.extend_from_slice(&(payload_len as u32).to_le_bytes());
    out.extend_from_slice(&header.lsn.to_le_bytes());
    out.extend_from_slice(&header.prev_txn_lsn.to_le_bytes());
    out.extend_from_slice(&header.txn_id.to_le_bytes());
    out.extend_from_slice(&(header.record_type as u16).to_le_bytes());
    out.extend_from_slice(&0_u16.to_le_bytes());
    out.extend_from_slice(&header.source_time_us.to_le_bytes());
    out.extend_from_slice(&0_u32.to_le_bytes());
}

/// Callers validate length, magic, CRC and recovery policy before/after this
/// pure field decode in their original order.
#[inline]
fn read_record_header(bytes: &[u8]) -> Result<WalRecordHeader, QueryError> {
    Ok(WalRecordHeader {
        source_time_us: read_u64(bytes, 36),
        lsn: read_u64(bytes, 8),
        prev_txn_lsn: read_u64(bytes, 16),
        txn_id: read_u64(bytes, 24),
        record_type: WalRecordType::from_u16(read_u16(bytes, 32))?,
    })
}

pub fn decode_frame_for_replication(bytes: &[u8]) -> Result<ReplicationFrame, QueryError> {
    let (header, frame_len) = inspect_wal_frame(bytes)?;
    let payload = decode_payload(header.record_type, &bytes[WAL_FRAME_HEADER_LEN..frame_len])?;
    Ok(ReplicationFrame {
        header,
        payload,
        frame_len,
    })
}

fn inspect_wal_frame(bytes: &[u8]) -> Result<(WalRecordHeader, usize), QueryError> {
    if bytes.len() < WAL_FRAME_HEADER_LEN {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "replication frame: short header",
        ));
    }
    let magic = read_u32(bytes, 0);
    if magic != WAL_RECORD_MAGIC {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "replication frame: bad magic",
        ));
    }
    let payload_len = read_u32(bytes, 4) as usize;
    let frame_len = WAL_FRAME_HEADER_LEN + payload_len;
    if frame_len > bytes.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "replication frame: bad length",
        ));
    }
    let payload_bytes = &bytes[WAL_FRAME_HEADER_LEN..WAL_FRAME_HEADER_LEN + payload_len];
    let crc = read_u32(bytes, 44);
    if wal_frame_checksum(bytes, payload_bytes) != crc {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "replication frame: checksum mismatch",
        ));
    }
    let header = read_record_header(bytes)?;
    let reserved = read_u16(bytes, 34);
    if reserved != 0 {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "replication frame: unknown reserved bits",
        ));
    }
    Ok((header, frame_len))
}

// ============================================================
// WalReader: streaming reader for physical replication.
// ============================================================

/// Output of [`WalReader::next_chunk`]: a contiguous run of WAL frames
/// (each frame is the on-disk binary form, with its own 40-byte header
/// and CRC32) starting at `start_lsn` and ending at `end_lsn` inclusive.
#[derive(Debug, Clone)]
pub struct WalReadChunk {
    pub start_lsn: u64,
    pub end_lsn: u64,
    pub frame_count: u32,
    /// Concatenated raw frame bytes, ready to be wrapped in a
    /// `protocol::WalChunk` message and shipped over the wire.
    pub bytes: Vec<u8>,
}

/// Errors specific to streaming WAL reads. WAL-truncated/future-LSN
/// cases need different protocol responses from the master, so we
/// surface them as variants rather than collapsing into `QueryError`.
#[derive(Debug)]
pub enum WalReaderError {
    /// `requested` is older than `min_available` on this master — the
    /// caller (typically a replica) needs to do a base backup.
    Truncated { requested: u64, min_available: u64 },
    /// `requested` is beyond the master's next-LSN, which can't happen
    /// in a well-behaved cluster; treat as a fatal client error.
    FutureLsn {
        requested: u64,
        master_next_lsn: u64,
    },
    /// I/O or decode error reading the segment file.
    Io(QueryError),
}

impl From<QueryError> for WalReaderError {
    fn from(err: QueryError) -> Self {
        WalReaderError::Io(err)
    }
}

/// Streaming reader over the WAL log segments, used by the master-side
/// replication sender. Holds at most one open file handle (for the
/// segment it's currently reading). Lazily transitions to the next
/// segment when the cursor crosses a boundary.
///
/// Concurrency contract: every read briefly locks `LogManager` to get
/// the current segment snapshot, then reads from the file without
/// holding the lock. The active segment is appended to concurrently,
/// but [`WalReader::next_chunk`] only returns frames whose LSN is ≤ the
/// `safe_high_lsn` the caller supplies — typically the most recent
/// value broadcast on the `subscribe_new_lsn` watch channel.
#[derive(Debug)]
pub struct WalReader {
    state: Arc<LogStateAtomics>,
    log: Arc<LogMutex>,
    /// Next LSN we will hand out. Advances by frame after each read.
    cursor_lsn: u64,
    /// Open read handle for the segment containing `cursor_lsn`.
    current_file: Option<File>,
    /// Id of the segment whose file is currently open.
    current_segment_id: Option<u64>,
    /// File offset of the next frame to read.
    current_offset: u64,
    /// `subscribe_new_lsn` subscription handed to the reader so we
    /// don't lose wake-ups between read calls.
    new_lsn_rx: watch::Receiver<u64>,
}

impl WalReader {
    /// Open a reader positioned at `start_lsn`. Errors out if the LSN
    /// is past the master's retention boundary or in the future.
    pub fn open(log: &Arc<LogMutex>, start_lsn: u64) -> Result<Self, WalReaderError> {
        let (new_lsn_rx, state) = {
            let log = log.lock();
            (log.subscribe_new_lsn(), log.state_atomics())
        };
        let mut reader = Self {
            state,
            log: Arc::clone(log),
            cursor_lsn: start_lsn,
            current_file: None,
            current_segment_id: None,
            current_offset: 0,
            new_lsn_rx,
        };
        reader.position_to_lsn(start_lsn)?;
        Ok(reader)
    }

    /// The LSN this reader will hand out next.
    pub fn cursor_lsn(&self) -> u64 {
        self.cursor_lsn
    }

    /// Read at most `max_bytes` of frame data, capped to frames whose
    /// LSN is ≤ `safe_high_lsn`. Returns `Ok(None)` when there's
    /// nothing new to read (caller should await
    /// [`wait_for_new_data`]). Errors are returned as-is.
    pub fn next_chunk(
        &mut self,
        safe_high_lsn: u64,
        max_bytes: usize,
    ) -> Result<Option<WalReadChunk>, WalReaderError> {
        if self.cursor_lsn > safe_high_lsn {
            return Ok(None);
        }
        self.log.lock().flush_pending_writes()?;
        self.read_chunk(safe_high_lsn, max_bytes)
    }

    /// Read only the durable prefix, without flushing or locking the append path.
    pub fn next_durable_chunk(
        &mut self,
        safe_high_lsn: u64,
        max_bytes: usize,
    ) -> Result<Option<WalReadChunk>, WalReaderError> {
        let high = safe_high_lsn.min(self.state.durable_lsn.load(Ordering::Acquire));
        if self.cursor_lsn > high {
            return Ok(None);
        }
        self.read_chunk(high, max_bytes)
    }

    fn read_chunk(
        &mut self,
        safe_high_lsn: u64,
        max_bytes: usize,
    ) -> Result<Option<WalReadChunk>, WalReaderError> {
        let mut chunk_bytes: Vec<u8> = Vec::new();
        let mut start_lsn = 0u64;
        let mut end_lsn = 0u64;
        let mut frame_count = 0u32;

        loop {
            self.ensure_segment_open()?;
            if self.current_file.is_none() {
                // Caller is fully caught up — nothing to read yet.
                break;
            }
            let file = self.current_file.as_mut().expect("file opened above");
            file.seek(SeekFrom::Start(self.current_offset))
                .map_err(io_err)?;
            let mut header = [0u8; WAL_FRAME_HEADER_LEN];
            match file.read_exact(&mut header) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Hit end of current segment — try next segment.
                    if self.switch_to_next_segment()? {
                        continue;
                    } else {
                        break;
                    }
                }
                Err(err) => return Err(io_err(err).into()),
            }
            let magic = read_u32(&header, 0);
            if magic != WAL_RECORD_MAGIC {
                // End-of-data sentinel (we'll see zeroes past the
                // last record).
                if self.switch_to_next_segment()? {
                    continue;
                } else {
                    break;
                }
            }
            let payload_len = read_u32(&header, 4) as usize;
            let frame_len = (payload_len + WAL_FRAME_HEADER_LEN) as u64;
            let lsn = read_u64(&header, 8);
            if lsn > safe_high_lsn {
                break;
            }
            // First check that we haven't exceeded the byte budget
            // *before* reading this frame (so callers can cap chunk
            // size). Always read at least one frame per call.
            if !chunk_bytes.is_empty() && chunk_bytes.len() + frame_len as usize > max_bytes {
                break;
            }
            // Read straight into the outgoing chunk: no per-frame payload
            // allocation or second copy, including large binary/page images.
            let frame_offset = chunk_bytes.len();
            chunk_bytes.try_reserve(frame_len as usize).map_err(|_| {
                WalReaderError::Io(error(
                    crate::error::catalog::ER_OUTOFMEMORY,
                    "Out of memory reading WAL chunk",
                ))
            })?;
            chunk_bytes.extend_from_slice(&header);
            chunk_bytes.resize(frame_offset + frame_len as usize, 0);
            let payload = &mut chunk_bytes[frame_offset + WAL_FRAME_HEADER_LEN..];
            file.read_exact(payload).map_err(io_err)?;
            // Verify checksum so we never ship a corrupt frame downstream.
            let stored_crc = read_u32(&header, 44);
            if wal_frame_checksum(&header, &payload) != stored_crc {
                return Err(WalReaderError::Io(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "WAL stream: frame checksum mismatch",
                )));
            }
            let record_type =
                WalRecordType::from_u16(read_u16(&header, 32)).map_err(WalReaderError::Io)?;
            let logical_start_lsn =
                frame_logical_start(record_type, lsn, &payload).map_err(WalReaderError::Io)?;
            if logical_start_lsn != self.cursor_lsn {
                return Err(WalReaderError::Io(error(crate::error::catalog::ER_GET_ERRNO,
                    format!(
                        "WAL stream: frame logical start LSN {logical_start_lsn} doesn't match expected {}",
                        self.cursor_lsn
                    ),
                )));
            }
            if frame_offset == 0 {
                start_lsn = logical_start_lsn;
            }
            end_lsn = lsn;
            frame_count += 1;
            self.current_offset += frame_len;
            self.cursor_lsn = lsn.saturating_add(1);
            if chunk_bytes.len() >= max_bytes {
                break;
            }
        }

        if frame_count == 0 {
            Ok(None)
        } else {
            Ok(Some(WalReadChunk {
                start_lsn,
                end_lsn,
                frame_count,
                bytes: chunk_bytes,
            }))
        }
    }

    #[cfg(test)]
    /// Block (asynchronously) until the master notifies us that a new
    /// LSN has been appended. The notification carries the newest LSN.
    pub async fn wait_for_new_data(&mut self) -> Result<u64, tokio::sync::watch::error::RecvError> {
        self.new_lsn_rx.changed().await?;
        Ok(*self.new_lsn_rx.borrow_and_update())
    }

    /// Synchronous probe of the latest broadcast LSN — useful when the
    /// reader just opened and wants to know an initial high-water mark.
    pub fn latest_known_lsn(&self) -> u64 {
        *self.new_lsn_rx.borrow()
    }

    // ---- internal helpers ----

    fn ensure_segment_open(&mut self) -> Result<(), WalReaderError> {
        if self.current_file.is_some() {
            return Ok(());
        }
        self.position_to_lsn(self.cursor_lsn)
    }

    fn position_to_lsn(&mut self, lsn: u64) -> Result<(), WalReaderError> {
        let snapshot = {
            let mut log = self.log.lock();
            log.flush_pending_writes()?;
            (
                log.segment_snapshot(),
                log.next_lsn(),
                log.min_available_lsn(),
            )
        };
        let (segments, master_next_lsn, min_available) = snapshot;
        if lsn < min_available {
            return Err(WalReaderError::Truncated {
                requested: lsn,
                min_available,
            });
        }
        if lsn > master_next_lsn {
            return Err(WalReaderError::FutureLsn {
                requested: lsn,
                master_next_lsn,
            });
        }
        if lsn == master_next_lsn {
            // Caller is fully caught up; nothing to position to yet.
            self.current_file = None;
            self.current_segment_id = None;
            self.current_offset = 0;
            return Ok(());
        }
        let segment = segments
            .into_iter()
            .find(|seg| {
                lsn >= seg.first_lsn
                    && (lsn <= seg.last_lsn || seg.status == WalSegmentStatus::Active)
            })
            .ok_or_else(|| {
                WalReaderError::Io(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!("no WAL segment contains LSN {lsn}"),
                ))
            })?;
        let mut file = OpenOptions::new()
            .read(true)
            .open(&segment.path)
            .map_err(io_err)?;
        file.seek(SeekFrom::Start(WAL_HEADER_LEN as u64))
            .map_err(io_err)?;
        // Scan forward to the requested LSN.
        let mut offset = WAL_HEADER_LEN as u64;
        loop {
            let mut header = [0u8; WAL_FRAME_HEADER_LEN];
            match file.read_exact(&mut header) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Err(WalReaderError::Io(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        format!("LSN {lsn} not found in segment {}", segment.segment_id),
                    )));
                }
                Err(err) => return Err(io_err(err).into()),
            }
            // Zero header = preallocated tail (or sealed-trimmed end);
            // the LSN we're looking for isn't in this segment.
            if header.iter().all(|&b| b == 0) {
                return Err(WalReaderError::Io(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!("LSN {lsn} not present (segment end reached)"),
                )));
            }
            let magic = read_u32(&header, 0);
            if magic != WAL_RECORD_MAGIC {
                return Err(WalReaderError::Io(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!("LSN {lsn} not present (segment end reached)"),
                )));
            }
            let payload_len = read_u32(&header, 4) as u64;
            let frame_len = payload_len + WAL_FRAME_HEADER_LEN as u64;
            let frame_lsn = read_u64(&header, 8);
            if frame_lsn == lsn {
                file.seek(SeekFrom::Start(offset)).map_err(io_err)?;
                self.current_file = Some(file);
                self.current_segment_id = Some(segment.segment_id);
                self.current_offset = offset;
                return Ok(());
            }
            if frame_lsn > lsn {
                let record_type =
                    WalRecordType::from_u16(read_u16(&header, 32)).map_err(WalReaderError::Io)?;
                if record_type == WalRecordType::TxnBatch {
                    let mut payload = vec![0u8; payload_len as usize];
                    file.read_exact(&mut payload).map_err(io_err)?;
                    let decoded =
                        decode_payload(record_type, &payload).map_err(WalReaderError::Io)?;
                    let WalPayload::TxnBatch { records } = decoded else {
                        unreachable!("TxnBatch record type must decode to TxnBatch payload");
                    };
                    let logical_start = frame_lsn
                        .checked_add(1)
                        .and_then(|end| end.checked_sub(records.len() as u64))
                        .ok_or_else(|| {
                            WalReaderError::Io(error(
                                crate::error::catalog::ER_GET_ERRNO,
                                "invalid WAL batch LSN span",
                            ))
                        })?;
                    if logical_start <= lsn {
                        file.seek(SeekFrom::Start(offset)).map_err(io_err)?;
                        self.current_file = Some(file);
                        self.current_segment_id = Some(segment.segment_id);
                        self.current_offset = offset;
                        return Ok(());
                    }
                }
                return Err(WalReaderError::Io(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!(
                        "LSN {lsn} not present in segment {} (skipped over)",
                        segment.segment_id
                    ),
                )));
            }
            // Skip this frame's payload.
            file.seek(SeekFrom::Current(payload_len as i64))
                .map_err(io_err)?;
            offset += frame_len;
        }
    }

    fn switch_to_next_segment(&mut self) -> Result<bool, WalReaderError> {
        let current_id = match self.current_segment_id {
            Some(id) => id,
            None => return Ok(false),
        };
        let snapshot = {
            let log = self.log.lock();
            log.segment_snapshot()
        };
        let next = snapshot.iter().find(|s| s.segment_id > current_id);
        if let Some(seg) = next {
            let mut file = OpenOptions::new()
                .read(true)
                .open(&seg.path)
                .map_err(io_err)?;
            file.seek(SeekFrom::Start(WAL_HEADER_LEN as u64))
                .map_err(io_err)?;
            self.current_file = Some(file);
            self.current_segment_id = Some(seg.segment_id);
            self.current_offset = WAL_HEADER_LEN as u64;
            Ok(true)
        } else {
            // No more segments — caller should await new data.
            self.current_file = None;
            self.current_segment_id = None;
            self.current_offset = 0;
            Ok(false)
        }
    }
}

fn frame_logical_start(ty: WalRecordType, lsn: u64, payload: &[u8]) -> Result<u64, QueryError> {
    if ty != WalRecordType::TxnBatch {
        return Ok(lsn);
    }
    let mut offset = 0;
    let count = read_u32_checked(payload, &mut offset)? as usize;
    if count == 0 || count > MAX_TXN_BATCH_RECORDS {
        return Err(io_err(std::io::Error::other("invalid WAL batch count")));
    }
    let mut files_only = true;
    for index in 0..count {
        let ty = WalRecordType::from_u16(read_u16_checked(payload, &mut offset)?)?;
        let len = read_u32_checked(payload, &mut offset)? as usize;
        files_only &= ty == WalRecordType::FileCreate;
        if matches!(ty, WalRecordType::TxnBatch | WalRecordType::PageDelta)
            || (index + 1 == count && ty != WalRecordType::CommitTxn && !files_only)
        {
            return Err(io_err(std::io::Error::other("invalid WAL batch structure")));
        }
        offset = offset
            .checked_add(len)
            .filter(|end| *end <= payload.len())
            .ok_or_else(|| io_err(std::io::Error::other("truncated WAL batch")))?;
    }
    if offset != payload.len() {
        return Err(io_err(std::io::Error::other("trailing WAL batch bytes")));
    }
    lsn.checked_add(1)
        .and_then(|end| end.checked_sub(count as u64))
        .filter(|first| *first != 0)
        .ok_or_else(|| io_err(std::io::Error::other("invalid WAL batch LSN span")))
}

fn validate_txn_batch_records(records: &[WalPayload]) -> Result<(), QueryError> {
    if records.is_empty()
        || records.len() > MAX_TXN_BATCH_RECORDS
        || (!matches!(records.last(), Some(WalPayload::CommitTxn { .. }))
            && !records
                .iter()
                .all(|r| matches!(r, WalPayload::FileCreate { .. })))
        || records.iter().any(|record| {
            matches!(
                record,
                WalPayload::TxnBatch { .. } | WalPayload::PageDelta { .. }
            )
        })
    {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "invalid WAL transaction batch",
        ));
    }
    Ok(())
}

/// Encode sparse, position-preserving edits as ordinary LZ4 dictionary
/// matches. Page updates usually retain almost every byte at its old offset;
/// those matches need no hash table or dictionary indexing. Limit the encoded
/// size so moves, repacking and dense edits promptly use the general codec.
/// The result uses the existing PageDelta wire format and decoder.
fn compress_sparse_page_delta(before: &[u8], after: &[u8]) -> Option<Vec<u8>> {
    const PAGE: usize = 16384;
    const MAX_ENCODED: usize = PAGE / 64 - 16;
    if before.len() != PAGE || after.len() != PAGE {
        return None;
    }
    // Reject common moved/dense pages before allocating or walking a literal
    // run. A false negative only selects the general dictionary compressor.
    if [64, 128, 192]
        .iter()
        .all(|&at| before[at..at + 8] != after[at..at + 8])
    {
        return None;
    }
    fn length_bytes(output: &mut Vec<u8>, mut length: usize) {
        while length >= 255 {
            output.push(255);
            length -= 255;
        }
        output.push(length as u8);
    }
    let mut output = Vec::with_capacity(MAX_ENCODED);
    let mut literals = 0;
    let mut cursor = 0;
    // LZ4 ends in at least five literals and the last match starts at least
    // twelve bytes before the end. Eight-byte blocks satisfy both constraints.
    let match_end = PAGE - 8;
    while cursor + 8 <= match_end {
        if before[cursor..cursor + 8] != after[cursor..cursor + 8] {
            cursor += 8;
            if output.len() + cursor - literals > MAX_ENCODED {
                return None;
            }
            continue;
        }
        let start = cursor;
        cursor += 8;
        while cursor + 64 <= match_end && before[cursor..cursor + 64] == after[cursor..cursor + 64]
        {
            cursor += 64;
        }
        while cursor + 8 <= match_end && before[cursor..cursor + 8] == after[cursor..cursor + 8] {
            cursor += 8;
        }
        let literal_len = start - literals;
        let match_len = cursor - start - 4;
        let encoded_len = 3
            + literal_len
            + if literal_len >= 15 {
                (literal_len - 15) / 255 + 1
            } else {
                0
            }
            + if match_len >= 15 {
                (match_len - 15) / 255 + 1
            } else {
                0
            };
        if output.len() + encoded_len > MAX_ENCODED {
            return None;
        }
        output.push(((literal_len.min(15) << 4) | match_len.min(15)) as u8);
        if literal_len >= 15 {
            length_bytes(&mut output, literal_len - 15);
        }
        output.extend_from_slice(&after[literals..start]);
        // At every output position this distance addresses the same offset
        // in the previous page, which is the external LZ4 dictionary.
        output.extend_from_slice(&(PAGE as u16).to_le_bytes());
        if match_len >= 15 {
            length_bytes(&mut output, match_len - 15);
        }
        literals = cursor;
    }
    let literal_len = PAGE - literals;
    let encoded_len = 1
        + literal_len
        + if literal_len >= 15 {
            (literal_len - 15) / 255 + 1
        } else {
            0
        };
    if output.len() + encoded_len > MAX_ENCODED {
        return None;
    }
    output.push((literal_len.min(15) << 4) as u8);
    if literal_len >= 15 {
        length_bytes(&mut output, literal_len - 15);
    }
    output.extend_from_slice(&after[literals..]);
    Some(output)
}

impl WalPayload {
    /// Caller must retain a preceding FPI after the checkpoint redo horizon.
    /// `after` already contains the reserved record LSN and page checksum.
    pub fn page_delta(
        path: String,
        page_id: u64,
        base_lsn: u64,
        before: &[u8],
        after: &[u8],
    ) -> Option<Self> {
        Self::page_delta_with_hashes(path, page_id, base_lsn, before, after, None)
    }

    /// Identical to `page_delta`, for formats that can calculate the full-page
    /// CRC from a shorter canonical representation. Hashes include every byte,
    /// including the page's stored checksum and zero padding.
    pub(crate) fn page_delta_prehashed(
        path: String,
        page_id: u64,
        base_lsn: u64,
        before: &[u8],
        after: &[u8],
        base_crc: u32,
        after_crc: u32,
    ) -> Option<Self> {
        Self::page_delta_with_hashes(
            path,
            page_id,
            base_lsn,
            before,
            after,
            Some((base_crc, after_crc)),
        )
    }

    fn page_delta_with_hashes(
        path: String,
        page_id: u64,
        base_lsn: u64,
        before: &[u8],
        after: &[u8],
        hashes: Option<(u32, u32)>,
    ) -> Option<Self> {
        if before.len() != 16384 || after.len() != 16384 || base_lsn == 0 {
            return None;
        }
        let compressed_after = compress_sparse_page_delta(before, after)
            .unwrap_or_else(|| lz4_flex::block::compress_with_dict(after, before));
        if compressed_after.len() > WAL_PAGE_DELTA_MAX_BYTES {
            return None;
        }
        let (base_crc, after_crc) =
            hashes.unwrap_or_else(|| (crc32fast::hash(before), crc32fast::hash(after)));
        Some(Self::PageDelta {
            path,
            page_id,
            base_lsn,
            base_crc,
            after_crc,
            compressed_after,
        })
    }

    pub fn full_page_image(path: String, page_id: u64, after: Vec<u8>) -> Self {
        if wal_fpi_compression_enabled() {
            let compressed = lz4_flex::compress_prepend_size(&after);
            // Encoding has already been paid for here. Retain useful savings
            // on dense pages too, rather than discarding a 12.5-50% reduction.
            // Leave near-incompressible pages raw to bound decode overhead.
            if compressed.len().saturating_mul(8) < after.len().saturating_mul(7) {
                return WalPayload::FullPageImageCompressed {
                    path,
                    page_id,
                    compressed_after: compressed,
                };
            }
        }
        WalPayload::FullPageImage {
            path,
            page_id,
            after,
        }
    }

    pub fn record_type(&self) -> WalRecordType {
        match self {
            WalPayload::FileClr { .. } => WalRecordType::FileClr,
            WalPayload::RollbackTo { .. } => WalRecordType::RollbackTo,
            WalPayload::PageDelta { .. } => WalRecordType::PageDelta,
            WalPayload::BeginTxn { .. } => WalRecordType::BeginTxn,
            WalPayload::CommitTxn { .. } => WalRecordType::CommitTxn,
            WalPayload::AbortTxn => WalRecordType::AbortTxn,
            WalPayload::EndTxn => WalRecordType::EndTxn,
            WalPayload::PageUpdate { .. } => WalRecordType::PageUpdate,
            WalPayload::PageUpdateCompressed { .. } => WalRecordType::PageUpdateCompressed,
            WalPayload::FullPageImage { .. } => WalRecordType::FullPageImage,
            WalPayload::FullPageImageCompressed { .. } => WalRecordType::FullPageImageCompressed,
            WalPayload::FileCreate { .. } => WalRecordType::FileCreate,
            WalPayload::FileDelete { .. } => WalRecordType::FileDelete,
            WalPayload::FileExtend { .. } => WalRecordType::FileExtend,
            WalPayload::CheckpointBegin => WalRecordType::CheckpointBegin,
            WalPayload::CheckpointEnd { .. } => WalRecordType::CheckpointEnd,
            WalPayload::Clr { .. } => WalRecordType::Clr,
            WalPayload::UndoRecord { .. } => WalRecordType::UndoRecord,
            WalPayload::HeapInsert { .. } => WalRecordType::HeapInsert,
            WalPayload::HeapInsertBatch { .. } => WalRecordType::HeapInsertBatch,
            WalPayload::HeapUpdate { .. } => WalRecordType::HeapUpdate,
            WalPayload::HeapUpdateLogical { .. } => WalRecordType::HeapUpdateLogical,
            WalPayload::HeapDelete { .. } => WalRecordType::HeapDelete,
            WalPayload::SlotReclaim { .. } => WalRecordType::SlotReclaim,
            WalPayload::IndexInsert { .. } => WalRecordType::IndexInsert,
            WalPayload::IndexDelete { .. } => WalRecordType::IndexDelete,
            WalPayload::IndexBatch { .. } => WalRecordType::IndexBatch,
            WalPayload::ZoneMapMergeBatch { .. } => WalRecordType::ZoneMapMergeBatch,
            WalPayload::HistoryIndexBatch { .. } => WalRecordType::HistoryIndexBatch,
            WalPayload::VectorIndexBatch { .. } => WalRecordType::VectorIndexBatch,
            WalPayload::VectorIndexModel { .. } => WalRecordType::VectorIndexModel,
            WalPayload::VectorIndexImage { .. } => WalRecordType::VectorIndexImage,
            WalPayload::VectorIndexComplete { .. } => WalRecordType::VectorIndexComplete,
            WalPayload::VectorIndexManifest { .. } => WalRecordType::VectorIndexManifest,
            WalPayload::SpatialPageBatch { .. } => WalRecordType::SpatialPageBatch,
            WalPayload::SpatialIndexUndo { .. } => WalRecordType::SpatialIndexUndo,
            WalPayload::SpatialIndexClr { .. } => WalRecordType::SpatialIndexClr,
            WalPayload::CatalogDelta { .. } => WalRecordType::CatalogDelta,
            WalPayload::DdlCommit { .. } => WalRecordType::DdlCommit,
            WalPayload::AutoIncrementAdvance { .. } => WalRecordType::AutoIncrementAdvance,
            WalPayload::SequenceState { .. } => WalRecordType::SequenceState,
            WalPayload::PrepareTxn { .. } => WalRecordType::PrepareTxn,
            WalPayload::TxnBatch { .. } => WalRecordType::TxnBatch,
        }
    }
}

#[cfg(test)]
pub fn wal_stats<'a>(records: impl IntoIterator<Item = &'a WalRecord>) -> WalStats {
    let mut stats = WalStats::default();
    for record in records {
        accumulate_wal_stats(&mut stats, &record.payload);
    }
    stats
}

fn accumulate_wal_stats(stats: &mut WalStats, payload: &WalPayload) {
    match payload {
        WalPayload::TxnBatch { records } => {
            for record in records {
                accumulate_wal_stats(stats, record);
            }
        }
        WalPayload::HeapInsert { payload, .. } => {
            stats.logical_records += 1;
            stats.heap_payload_bytes = stats
                .heap_payload_bytes
                .saturating_add(payload.len() as u64);
        }
        WalPayload::HeapInsertBatch { inserts, .. } => {
            stats.logical_records += 1;
            stats.heap_payload_bytes = stats.heap_payload_bytes.saturating_add(
                inserts
                    .iter()
                    .map(|insert| insert.payload.len() as u64)
                    .sum::<u64>(),
            );
        }
        WalPayload::HeapUpdate { before, after, .. } => {
            stats.logical_records += 1;
            stats.heap_payload_bytes = stats
                .heap_payload_bytes
                .saturating_add(before.len() as u64)
                .saturating_add(after.len() as u64);
        }
        WalPayload::HeapUpdateLogical { patches, .. } => {
            stats.logical_records += 1;
            stats.heap_payload_bytes = stats.heap_payload_bytes.saturating_add(
                patches
                    .iter()
                    .map(|p| (p.before.len() + p.after.len()) as u64)
                    .sum::<u64>(),
            );
        }
        WalPayload::HeapDelete { before, .. } => {
            stats.logical_records += 1;
            stats.heap_payload_bytes = stats.heap_payload_bytes.saturating_add(before.len() as u64);
        }
        WalPayload::IndexInsert { key, .. } | WalPayload::IndexDelete { key, .. } => {
            stats.logical_records += 1;
            stats.index_payload_bytes = stats.index_payload_bytes.saturating_add(key.len() as u64);
        }
        WalPayload::IndexBatch { ops, .. } => {
            stats.logical_records += 1;
            stats.index_batch_records += 1;
            stats.index_payload_bytes = stats
                .index_payload_bytes
                .saturating_add(ops.iter().map(|op| op.key.len() as u64 + 20).sum::<u64>());
        }
        WalPayload::SpatialIndexUndo { bytes, .. }
        | WalPayload::SpatialPageBatch { bytes, .. }
        | WalPayload::HistoryIndexBatch { bytes }
        | WalPayload::VectorIndexBatch { bytes }
        | WalPayload::VectorIndexModel { bytes } => {
            stats.logical_records += 1;
            stats.index_batch_records += 1;
            stats.index_payload_bytes =
                stats.index_payload_bytes.saturating_add(bytes.len() as u64);
        }
        WalPayload::PageUpdate {
            path,
            before,
            after,
            ..
        } => {
            stats.page_fallback_records += 1;
            let payload_bytes = (before.len() as u64).saturating_add(after.len() as u64);
            stats.page_payload_bytes = stats.page_payload_bytes.saturating_add(payload_bytes);
            if path.ends_with(".idx") {
                stats.index_page_payload_bytes =
                    stats.index_page_payload_bytes.saturating_add(payload_bytes);
            } else if path.ends_with(".heap") {
                stats.heap_fpi_payload_bytes =
                    stats.heap_fpi_payload_bytes.saturating_add(payload_bytes);
            } else {
                stats.other_page_payload_bytes =
                    stats.other_page_payload_bytes.saturating_add(payload_bytes);
            }
        }
        WalPayload::PageUpdateCompressed {
            path,
            compressed_before,
            compressed_after,
            ..
        } => {
            stats.page_fallback_records += 1;
            let payload_bytes =
                (compressed_before.len() as u64).saturating_add(compressed_after.len() as u64);
            stats.page_payload_bytes = stats.page_payload_bytes.saturating_add(payload_bytes);
            if path.ends_with(".idx") {
                stats.index_page_payload_bytes =
                    stats.index_page_payload_bytes.saturating_add(payload_bytes);
            } else if path.ends_with(".heap") {
                stats.heap_fpi_payload_bytes =
                    stats.heap_fpi_payload_bytes.saturating_add(payload_bytes);
            } else {
                stats.other_page_payload_bytes =
                    stats.other_page_payload_bytes.saturating_add(payload_bytes);
            }
        }
        WalPayload::PageDelta {
            path,
            compressed_after,
            ..
        } => {
            stats.page_fallback_records += 1;
            let bytes = compressed_after.len() as u64 + 16;
            stats.page_payload_bytes = stats.page_payload_bytes.saturating_add(bytes);
            if path.ends_with(".heap") {
                stats.heap_fpi_payload_bytes = stats.heap_fpi_payload_bytes.saturating_add(bytes);
            } else if path.ends_with(".idx") {
                stats.index_page_payload_bytes =
                    stats.index_page_payload_bytes.saturating_add(bytes);
            } else {
                stats.other_page_payload_bytes =
                    stats.other_page_payload_bytes.saturating_add(bytes);
            }
        }
        WalPayload::FullPageImage { path, after, .. } => {
            stats.page_fallback_records += 1;
            let payload_bytes = after.len() as u64;
            stats.page_payload_bytes = stats.page_payload_bytes.saturating_add(payload_bytes);
            if path.ends_with(".heap") {
                stats.heap_fpi_payload_bytes =
                    stats.heap_fpi_payload_bytes.saturating_add(payload_bytes);
            } else if path.ends_with(".idx") {
                stats.index_page_payload_bytes =
                    stats.index_page_payload_bytes.saturating_add(payload_bytes);
            } else {
                stats.other_page_payload_bytes =
                    stats.other_page_payload_bytes.saturating_add(payload_bytes);
            }
        }
        WalPayload::FullPageImageCompressed {
            path,
            compressed_after,
            ..
        } => {
            stats.page_fallback_records += 1;
            let payload_bytes = compressed_after.len() as u64;
            stats.page_payload_bytes = stats.page_payload_bytes.saturating_add(payload_bytes);
            if path.ends_with(".heap") {
                stats.heap_fpi_payload_bytes =
                    stats.heap_fpi_payload_bytes.saturating_add(payload_bytes);
            } else if path.ends_with(".idx") {
                stats.index_page_payload_bytes =
                    stats.index_page_payload_bytes.saturating_add(payload_bytes);
            } else {
                stats.other_page_payload_bytes =
                    stats.other_page_payload_bytes.saturating_add(payload_bytes);
            }
        }
        _ => {}
    }
}

pub fn page_lsn(buf: &[u8]) -> u64 {
    if buf.len() < 16 {
        return 0;
    }
    u64::from_le_bytes(buf[8..16].try_into().unwrap_or([0; 8]))
}

pub fn set_page_lsn(buf: &mut [u8], lsn: u64) {
    if buf.len() >= 16 {
        buf[8..16].copy_from_slice(&lsn.to_le_bytes());
    }
}

fn sync_before_directory_publication(file: &File, _path: &Path) -> std::io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        crate::platform::fs::sync_before_atomic_rename_at(file, _path)
    }
    #[cfg(not(target_os = "macos"))]
    {
        file.sync_all()
    }
}

fn initialize_segment(
    path: &Path,
    segment_id: u64,
    preallocate_bytes: u64,
    fill: SegmentFill,
) -> Result<(), QueryError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(io_err)?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .map_err(io_err)?;
    // Reserve disk space for the entire segment up front so subsequent
    // appends don't have to extend the file (`fdatasync` then doesn't
    // need to journal the inode-extent metadata). Best-effort: a
    // filesystem that doesn't support extent reservation (tmpfs, NFS
    // without fallocate) just gets the legacy sparse-segment behaviour.
    if preallocate_bytes > 0 {
        let _ = preallocate_segment(&file, preallocate_bytes);
    }
    let header = segment_header(segment_id);
    file.write_all(&header).map_err(io_err)?;
    if fill == SegmentFill::Zero && preallocate_bytes > header.len() as u64 {
        zero_fill_segment(&file, header.len() as u64, preallocate_bytes)?;
    }
    // Persist data and inode metadata in order before the full parent-directory
    // publication barrier. On macOS only that final barrier needs to drain the
    // device cache; the preceding ordering barrier must not acknowledge a WAL
    // segment by itself.
    sync_before_directory_publication(&file, path).map_err(io_err)?;
    if let Some(parent) = path.parent() {
        crate::platform::fs::sync_path(parent).map_err(io_err)?;
    }
    Ok(())
}

/// On-disk layout of a new segment's preallocated byte budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SegmentFill {
    /// Reserve extents only. Cheap, but ext4/xfs mark reserved extents
    /// *unwritten*: the first `fdatasync` over each newly written block has to
    /// convert the extent and wait for a journal commit, which measured
    /// 3-4x slower per commit than syncing data alone on NVMe.
    Reserve,
    /// Reserve and overwrite with zeros, so commit `fdatasync`s flush data
    /// blocks only. Recovery already treats an all-zero frame header as the
    /// end of the valid WAL, exactly as for a reserved tail.
    Zero,
}

/// Densest WAL traffic, in bytes per durability sync, for which the
/// background preparer zero-fills the next segment. Break-even on NVMe/cloud
/// block storage: an extent-conversion journal commit adds ~1 ms per sync,
/// while zero-filling a 256 MiB segment costs ~0.5-1 s of sequential write
/// bandwidth, i.e. ~500 syncs per segment. OLTP commits sync tens of KiB;
/// bulk loads sync several MiB.
const ZERO_FILL_MAX_BYTES_PER_SYNC: u64 = 512 * 1024;

/// Minimum average sync latency on a reservation-only segment for which the
/// background preparer zero-fills. Cloud block storage measured ~1.5-1.8 ms
/// per reserved-extent sync vs ~0.5 ms zero-filled (TPC-C +12%). Local NVMe
/// measured ~0.24 ms vs ~0.08 ms; once commits stopped paying a blocking-pool
/// hand-off, that commit latency became visible again and zero filling won
/// there too (TPC-C +3%), whereas it had lost (-4%) while statement CPU
/// dominated. Below this bound a reserved-extent sync is already about as
/// cheap as a data-only one, so the extra sequential write cannot pay off.
const ZERO_FILL_MIN_RESERVE_SYNC_US: u64 = 100;

/// Small WAL batches incur an extent-conversion barrier for very few bytes.
/// A 256 MiB segment then serves at least 32K syncs, amortizing its background
/// zero fill even when reservation-only syncs take much less than 500 us.
/// Keep the larger-write threshold unchanged to avoid adding sequential I/O
/// to workloads that already amortize filesystem metadata across large writes.
const ZERO_FILL_SMALL_BYTES_PER_SYNC: u64 = 8 * 1024;

/// Syncs a poll window needs before its average latency is trusted.
const ZERO_FILL_MIN_SAMPLE_SYNCS: u64 = 64;

/// Whether the background preparer may zero-fill segments (see
/// [`SegmentFill::Zero`]). Default on Linux whenever preallocation is enabled;
/// `MYDB_WAL_ZERO_FILL=0` keeps reservation only for A/B measurement or
/// space-constrained filesystems.
fn wal_zero_fill_enabled() -> bool {
    static CACHED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        let enabled = match std::env::var_os("MYDB_WAL_ZERO_FILL") {
            Some(v) => !matches!(
                v.to_string_lossy().to_ascii_lowercase().as_str(),
                "0" | "off" | "false" | "no" | ""
            ),
            None => cfg!(target_os = "linux"),
        };
        enabled && wal_preallocate_enabled()
    })
}

/// Overwrite `[from, to)` with zeros, syncing every chunk. Concurrent commit
/// `fdatasync`s then queue behind at most one chunk of writeback: syncing every
/// 16 MiB stalled them for up to ~90 ms (every 20 terminals' commit at once,
/// once per segment), while 256 KiB steps keep the worst case within a few ms
/// and still fill 256 MiB in ~2 s. The caller performs the final sync.
fn zero_fill_segment(file: &File, from: u64, to: u64) -> Result<(), QueryError> {
    const CHUNK: usize = 256 * 1024;
    static ZEROS: [u8; CHUNK] = [0; CHUNK];
    let mut offset = from;
    while offset < to {
        let len = (to - offset).min(CHUNK as u64) as usize;
        write_all_at(file, &ZEROS[..len], offset)?;
        offset += len as u64;
        if offset < to {
            file.sync_data().map_err(io_err)?;
        }
    }
    Ok(())
}

fn segment_header(segment_id: u64) -> Vec<u8> {
    segment_header_with_replacement(segment_id, 0)
}

fn segment_header_with_replacement(segment_id: u64, replaces_through: u64) -> Vec<u8> {
    let mut header = Vec::with_capacity(WAL_HEADER_LEN);
    header.extend_from_slice(WAL_MAGIC);
    header.extend_from_slice(&WAL_VERSION.to_le_bytes());
    header.extend_from_slice(&segment_id.to_le_bytes());
    header.extend_from_slice(&current_unix_ms().to_le_bytes());
    header.extend_from_slice(&replaces_through.to_le_bytes());
    let checksum = wal_checksum(&header);
    header.extend_from_slice(&checksum.to_le_bytes());
    header
}

fn ensure_segment_header(path: &Path, segment_id: u64) -> Result<(), QueryError> {
    let metadata = fs::metadata(path).map_err(io_err)?;
    if metadata.len() == 0 {
        // Empty file — happens when a previous boot saw a zero-byte
        // segment (e.g. created but not yet flushed). Re-initialise
        // without preallocation; the active-segment open path will
        // reapply the extent reservation once the LogManager is built.
        return initialize_segment(path, segment_id, 0, SegmentFill::Reserve);
    }
    if metadata.len() < WAL_HEADER_LEN as u64 {
        return Err(error(crate::error::catalog::ER_GET_ERRNO, format!(
            "truncated WAL segment header '{}' ({} bytes, expected {}); restore from a verified backup; do not discard the data directory",
            path.display(), metadata.len(), WAL_HEADER_LEN
        )));
    }
    let mut file = File::open(path).map_err(io_err)?;
    let mut header = vec![0; WAL_HEADER_LEN];
    file.read_exact(&mut header).map_err(io_err)?;
    if &header[..8] != WAL_MAGIC {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "unrecognized or corrupt WAL header; restore from a verified backup",
        ));
    }
    let version = read_u32(&header, 8);
    let stored_segment = read_u64(&header, 12);
    let stored_crc = read_u32(&header, 36);
    let actual_crc = wal_checksum(&header[..36]);
    if version != WAL_VERSION {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            format!(
                "unsupported WAL version {version}; expected {WAL_VERSION} (header may be corrupt); verify the binary version and restore from a verified backup"
            ),
        ));
    }
    if stored_segment != segment_id || stored_crc != actual_crc {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt WAL segment header",
        ));
    }
    Ok(())
}

/// Scan a segment using its filename-encoded id.
#[cfg(test)]
fn scan_segment_with_id(
    path: &Path,
    segment_id: u64,
    truncate_tail: bool,
) -> Result<WalScan, QueryError> {
    scan_segment_inner(path, segment_id, truncate_tail)
}

/// Inspect a (sealed) segment to get its registry-friendly metadata
/// without retaining decoded records. Used at boot to populate
/// `LogManager.segments` for everything except the active segment.
fn inspect_segment(path: &Path, segment_id: u64) -> Result<WalSegmentInfo, QueryError> {
    let mut first_lsn = None;
    let mut last_lsn = 0;
    visit_segment_records(path, segment_id, false, |record| {
        first_lsn.get_or_insert(record.lsn);
        last_lsn = record.lsn;
        Ok(())
    })?;
    let size_bytes = fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    let first_lsn = first_lsn.unwrap_or(FIRST_LSN);
    let last_lsn = last_lsn.max(first_lsn.saturating_sub(1));
    Ok(WalSegmentInfo {
        segment_id,
        path: path.to_path_buf(),
        first_lsn,
        last_lsn,
        size_bytes,
        status: WalSegmentStatus::Reclaimable,
    })
}

#[cfg(test)]
fn scan_segment(path: &Path, truncate_tail: bool) -> Result<WalScan, QueryError> {
    scan_segment_inner(path, 1, truncate_tail)
}

#[cfg(test)]
fn scan_segment_inner(
    path: &Path,
    segment_id: u64,
    truncate_tail: bool,
) -> Result<WalScan, QueryError> {
    scan_segment_inner_with_mode(path, segment_id, truncate_tail, WalReadMode::Recovery)
}

fn scan_segment_inner_with_mode(
    path: &Path,
    segment_id: u64,
    truncate_tail: bool,
    mode: WalReadMode,
) -> Result<WalScan, QueryError> {
    let mut records = Vec::new();
    let (next_lsn, end_offset) = visit_segment_records_located_with_mode(
        path,
        segment_id,
        truncate_tail,
        mode,
        |record, _, _| {
            records.push(record);
            Ok(())
        },
    )?;
    Ok(WalScan {
        records,
        next_lsn,
        end_offset,
    })
}

/// Corruption-only scan with bounded memory. A later frame marker means this
/// is not merely a torn final append; fail closed even for a coincidental marker.
fn wal_has_frame_after(path: &Path, offset: u64) -> Result<bool, QueryError> {
    let mut file = File::open(path).map_err(io_err)?;
    file.seek(SeekFrom::Start(offset)).map_err(io_err)?;
    let magic = WAL_RECORD_MAGIC.to_le_bytes();
    let mut buffer = [0u8; 64 * 1024 + 3];
    let mut carry = 0;
    loop {
        let n = file.read(&mut buffer[carry..]).map_err(io_err)?;
        if n == 0 {
            return Ok(false);
        }
        let end = carry + n;
        if buffer[..end].windows(4).any(|window| window == magic) {
            return Ok(true);
        }
        carry = end.min(3);
        buffer.copy_within(end - carry..end, 0);
    }
}

/// Validate every frame and payload while allowing metadata-only callers to
/// discard each decoded record immediately, independent of segment size.
fn visit_segment_records(
    path: &Path,
    segment_id: u64,
    truncate_tail: bool,
    mut visit: impl FnMut(WalRecord) -> Result<(), QueryError>,
) -> Result<(u64, u64), QueryError> {
    visit_segment_records_located(path, segment_id, truncate_tail, |record, _, _| {
        visit(record)
    })
}

/// The callback receives the validated payload's immutable byte range in the
/// segment, including the inner payload location for a transaction batch.
fn visit_segment_records_located(
    path: &Path,
    segment_id: u64,
    truncate_tail: bool,
    visit: impl FnMut(WalRecord, u64, usize) -> Result<(), QueryError>,
) -> Result<(u64, u64), QueryError> {
    visit_segment_records_located_with_mode(
        path,
        segment_id,
        truncate_tail,
        WalReadMode::Recovery,
        visit,
    )
}

fn visit_segment_records_located_with_mode(
    path: &Path,
    segment_id: u64,
    truncate_tail: bool,
    mode: WalReadMode,
    visit: impl FnMut(WalRecord, u64, usize) -> Result<(), QueryError>,
) -> Result<(u64, u64), QueryError> {
    visit_segment_records_located_from(
        path,
        segment_id,
        truncate_tail,
        mode,
        WAL_HEADER_LEN as u64,
        0,
        visit,
    )
}

fn visit_segment_records_located_from(
    path: &Path,
    segment_id: u64,
    truncate_tail: bool,
    mode: WalReadMode,
    start_offset: u64,
    mut previous_lsn: u64,
    mut visit: impl FnMut(WalRecord, u64, usize) -> Result<(), QueryError>,
) -> Result<(u64, u64), QueryError> {
    if mode != WalReadMode::Recovery
        && fs::metadata(path).map_err(io_err)?.len() < WAL_HEADER_LEN as u64
    {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "short WAL header in strict/read-only scan",
        ));
    }
    ensure_segment_header(path, segment_id)?;
    let truncate_tail = truncate_tail && mode == WalReadMode::Recovery;
    // `recover` = we are allowed to physically truncate the segment at the
    // first bad byte. True for the normal active-segment tail-trim
    // (`truncate_tail`), and also when the operator set
    // `MYDB_WAL_FORCE_RECOVER` to force past mid-stream corruption in an
    // otherwise sealed segment. `forced` is the force-recover sub-case,
    // distinguished only so we emit a loud warning (a normal preallocated
    // tail trim is routine and silent).
    let recover = mode == WalReadMode::Recovery
        && (truncate_tail || wal_force_recover_enabled())
        && !crate::storage::image_io::immutable_path(path);
    let forced = recover && wal_force_recover_enabled();
    let mut file = OpenOptions::new()
        .read(true)
        .write(recover)
        .open(path)
        .map_err(io_err)?;
    // Buffered reader: scan is purely sequential, so a 64 KiB read-ahead
    // buffer collapses the per-frame `read_exact` syscall storm into a
    // handful of bulk reads. A 256 MiB segment with 200 B average
    // records previously made ~2.6 million `read` syscalls per startup;
    // the buffer cuts that to ~4 K.
    file.seek(SeekFrom::Start(start_offset)).map_err(io_err)?;
    let metadata = file.metadata().map_err(io_err)?;
    let len = metadata.len();
    let mut reader = std::io::BufReader::with_capacity(64 * 1024, file);
    let mut offset = start_offset;
    if start_offset > len {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "WAL snapshot prefix was truncated",
        ));
    }
    // Reusable payload buffer — sized up by `Vec::resize` on each frame
    // and reused for the next. Recovery used to do `vec![0; payload_len]`
    // per record, which was a fresh allocation + drop per frame and
    // pinned ~`Σpayload_len` bytes through the Vec<WalRecord> output
    // (the bytes still alloc inside `decode_payload`, but at least we
    // avoid one redundant alloc per frame).
    let mut payload_buf: Vec<u8> = Vec::new();
    let mut decode_charge = replay::replay_decode_charge(0)?;
    // Uniform corruption handling: when `recover` is set, truncate the
    // segment at the start of the offending frame and stop scanning
    // (returning the clean prefix); otherwise fall through to the caller's
    // hard-fail `return Err(..)`. The force-recover sub-case logs loudly so
    // the discarded-records decision is never silent.
    macro_rules! recover_or_fail {
        ($reason:expr) => {{
            if !recover && !truncate_tail {
                return Err(error(crate::error::catalog::ER_GET_ERRNO, format!(
                    "corrupt sealed WAL '{}' at offset {}: {}; refusing to discard potentially committed history; restore a verified backup, or explicitly use MYDB_WAL_FORCE_RECOVER=1 for salvage (may lose data)",
                    path.display(), offset, $reason
                )));
            }
            if recover {
                if !forced && wal_has_frame_after(path, offset + 1)? {
                    return Err(error(crate::error::catalog::ER_GET_ERRNO, format!(
                        "corrupt WAL '{}' at offset {}: {}; later frame detected, refusing destructive tail truncation",
                        path.display(), offset, $reason
                    )));
                }
                if forced {
                    let backup = path.with_extension(format!("log.force-recover-{}-{}.bak", std::process::id(), offset));
                    let mut output = OpenOptions::new().write(true).create_new(true).open(&backup).map_err(io_err)?;
                    std::io::copy(&mut File::open(path).map_err(io_err)?, &mut output).map_err(io_err)?;
                    output.sync_all().map_err(io_err)?;
                    if let Some(parent) = backup.parent() {
                        crate::platform::fs::sync_path(parent).map_err(io_err)?;
                    }
                    crate::util::logging::log_message(format_args!("WARNING: WAL recovery discards {} bytes; original preserved at {}", len - offset, backup.display()));
                    crate::util::logging::log_message(format_args!(
                        "WAL force-recover: {} at offset {} in segment {} ({}); \
                         truncating tail — this frame and all later records are discarded",
                        $reason,
                        offset,
                        segment_id,
                        path.display()
                    ));
                }
                crate::util::logging::log_message(format_args!(
                    "WARNING: WAL tail truncation: {} at {} in {}; discarded {} bytes (forced={})",
                    $reason, offset, path.display(), len - offset, forced
                ));
                reader.get_mut().set_len(offset).map_err(io_err)?;
                reader.get_mut().sync_all().map_err(io_err)?;
                break;
            }
        }};
    }
    while offset < len {
        let mut header = [0u8; WAL_FRAME_HEADER_LEN];
        if let Err(err) = reader.read_exact(&mut header) {
            recover_or_fail!("truncated frame header");
            return Err(io_err(err));
        }
        let magic = read_u32(&header, 0);
        let stored_payload_len = read_u32(&header, 4) as usize;
        let frame_len = (stored_payload_len + WAL_FRAME_HEADER_LEN) as u64;
        // Detect "end of valid bytes" — every byte of the frame header
        // is zero. This is the canonical signature of preallocated
        // segment tail (under `posix_fallocate` / `F_PREALLOCATE` the
        // unused portion of the segment is zeroed by the kernel), and
        // it also matches a sealed segment that a previous boot
        // truncated to a valid offset. Treating it as the segment
        // terminator lets `truncate_tail=false` callers (sealed-segment
        // inspection, replication, recovery scans of a restored
        // generation) stop cleanly instead of reporting a phantom
        // "corrupt record frame". A non-zero header that doesn't match
        // the magic is still a real corruption and bails out.
        if header.iter().all(|&b| b == 0) {
            if recover {
                let file = reader.get_mut();
                file.set_len(offset).map_err(io_err)?;
            }
            break;
        }
        if magic != WAL_RECORD_MAGIC || offset.saturating_add(frame_len) > len {
            recover_or_fail!("corrupt record frame");
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt WAL record frame",
            ));
        }
        let required = stored_payload_len.saturating_mul(32).saturating_add(4096);
        decode_charge.try_grow(required.saturating_sub(decode_charge.bytes()))?;
        payload_buf.resize(stored_payload_len, 0);
        if let Err(err) = reader.read_exact(&mut payload_buf) {
            recover_or_fail!("truncated frame payload");
            return Err(io_err(err));
        }
        let stored_crc = read_u32(&header, 44);
        if wal_frame_checksum(&header, &payload_buf) != stored_crc {
            recover_or_fail!("frame checksum mismatch");
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt WAL frame checksum",
            ));
        }
        let record_header = match read_record_header(&header) {
            Ok(t) => t,
            Err(err) => {
                recover_or_fail!("unknown record type");
                return Err(err);
            }
        };
        let record_type = record_header.record_type;
        let reserved = read_u16(&header, 34);
        if reserved != 0 {
            recover_or_fail!("unknown reserved bits");
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt WAL: unknown reserved bits",
            ));
        }
        let lsn = record_header.lsn;
        if lsn == 0 || lsn <= previous_lsn {
            recover_or_fail!("non-monotonic LSN");
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt WAL: non-monotonic LSN",
            ));
        }
        let payload = match decode_payload(record_type, &payload_buf) {
            Ok(p) => p,
            Err(err) => {
                recover_or_fail!("undecodable payload");
                return Err(err);
            }
        };
        previous_lsn = lsn;
        let prev_txn_lsn = record_header.prev_txn_lsn;
        let txn_id = record_header.txn_id;
        match payload {
            WalPayload::TxnBatch { records: logical } => {
                // Startup recovery consumes the established logical-record
                // stream. Expand the CRC-atomic outer frame only after it has
                // passed checksum and structural validation; subrecords share
                // the outer LSN and retain their encoded order.
                let first_logical_lsn = lsn
                    .checked_add(1)
                    .and_then(|end| end.checked_sub(logical.len() as u64))
                    .ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "corrupt WAL batch LSN span",
                        )
                    })?;
                let mut logical_prev = prev_txn_lsn;
                let mut payload_offset = 4usize;
                for (idx, payload) in logical.into_iter().enumerate() {
                    let payload_len = read_u32(&payload_buf, payload_offset + 2) as usize;
                    payload_offset += 6;
                    let logical_lsn = first_logical_lsn + idx as u64;
                    let record = WalRecord {
                        source_time_us: record_header.source_time_us,
                        lsn: logical_lsn,
                        prev_txn_lsn: logical_prev,
                        txn_id,
                        payload,
                    };
                    logical_prev = logical_lsn;
                    visit(
                        record,
                        offset + WAL_FRAME_HEADER_LEN as u64 + payload_offset as u64,
                        payload_len,
                    )?;
                    payload_offset += payload_len;
                }
            }
            payload => visit(
                WalRecord {
                    source_time_us: record_header.source_time_us,
                    lsn,
                    prev_txn_lsn,
                    txn_id,
                    payload,
                },
                offset + WAL_FRAME_HEADER_LEN as u64,
                stored_payload_len,
            )?,
        }
        offset += frame_len;
    }
    Ok((previous_lsn.saturating_add(1).max(FIRST_LSN), offset))
}

impl Drop for LogManager {
    fn drop(&mut self) {
        let _ = self.flush_pending_writes();
        let _ = self.active_file.flush();
        let _ = WAL_RECORD_BUF_POOL.try_with(|pool| {
            if let Ok(mut pool) = pool.try_borrow_mut() {
                pool.clear();
            }
        });
    }
}

fn encode_record(record: &WalRecord) -> Result<Vec<u8>, QueryError> {
    let mut out = Vec::new();
    encode_record_into(record, &mut out)?;
    Ok(out)
}

/// Encode `record` and append the framed bytes to `out`. Hot-path callers
/// (`LogManager::append_reserved`) reuse a long-lived scratch buffer so
/// the encode no longer allocates a fresh `Vec` per WAL append; the
/// previous shape (`Vec::with_capacity` then drop) showed up at 5%+ of
/// total CPU on LOAD DATA, between the malloc, the
/// `drop_in_place::<WalPayload>` of the encoded frame, and the memmoves
/// that Vec growth still cost when the per-record buffer outgrew its
/// initial 8-byte capacity.
fn encode_record_into(record: &WalRecord, out: &mut Vec<u8>) -> Result<(), QueryError> {
    if matches!(&record.payload, WalPayload::PageDelta { .. }) && record.txn_id != 0 {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "page delta must be system-scoped",
        ));
    }
    let payload_len = encoded_payload_len(&record.payload)?;
    let frame_len = WAL_FRAME_HEADER_LEN + payload_len;
    let start = out.len();
    out.reserve(frame_len);
    write_frame_header(
        out,
        payload_len,
        WalRecordHeader {
            source_time_us: record.source_time_us,
            lsn: record.lsn,
            prev_txn_lsn: record.prev_txn_lsn,
            txn_id: record.txn_id,
            record_type: record.payload.record_type(),
        },
    );
    encode_payload_into(&record.payload, out)?;
    let checksum = wal_frame_checksum(&out[start..], &out[start + WAL_FRAME_HEADER_LEN..]);
    out[start + 44..start + 48].copy_from_slice(&checksum.to_le_bytes());
    Ok(())
}

fn encode_payload_into(payload: &WalPayload, mut out: &mut Vec<u8>) -> Result<(), QueryError> {
    match payload {
        WalPayload::FileClr {
            path,
            undo_next_lsn,
            action,
        } => {
            write_string(&mut out, path)?;
            write_u64_vec(&mut out, *undo_next_lsn);
            match action {
                FileCompensation::Delete => out.push(0),
                FileCompensation::Restore(bytes) => {
                    out.push(1);
                    write_bytes(&mut out, bytes)?;
                }
                FileCompensation::Truncate(len) => {
                    out.push(2);
                    write_u64_vec(&mut out, *len);
                }
            }
        }
        WalPayload::BeginTxn { start_scn } => write_u64_vec(&mut out, *start_scn),
        WalPayload::CommitTxn {
            commit_scn,
            commit_unix_us,
            binlog,
        } => {
            write_u64_vec(&mut out, *commit_scn);
            write_u64_vec(&mut out, *commit_unix_us);
            if let Some(binlog) = binlog {
                write_string(&mut out, &binlog.file)?;
                write_u64_vec(&mut out, binlog.start_pos);
                write_u64_vec(&mut out, binlog.end_pos);
                write_string(&mut out, binlog.gtid.as_deref().unwrap_or(""))?;
            }
        }
        WalPayload::RollbackTo {
            undo_head,
            undo_next_lsn,
            skip_lsn_start,
            skip_lsn_end,
            compensated,
        } => {
            write_u64_vec(&mut out, undo_head.unwrap_or(0));
            write_u64_vec(&mut out, *undo_next_lsn);
            write_u64_vec(&mut out, *skip_lsn_start);
            write_u64_vec(&mut out, *skip_lsn_end);
            write_u64_vec(&mut out, compensated.len() as u64);
            for id in compensated {
                write_u64_vec(&mut out, *id);
            }
        }
        WalPayload::AbortTxn | WalPayload::EndTxn | WalPayload::CheckpointBegin => {}
        WalPayload::PageUpdate {
            path,
            page_id,
            before,
            after,
            undo_lsn,
        } => {
            write_string(&mut out, path)?;
            write_u64_vec(&mut out, *page_id);
            write_bytes(&mut out, before)?;
            write_bytes(&mut out, after)?;
            write_u64_vec(&mut out, undo_lsn.unwrap_or(0));
        }
        WalPayload::PageUpdateCompressed {
            path,
            page_id,
            compressed_before,
            compressed_after,
            undo_lsn,
        } => {
            write_string(&mut out, path)?;
            write_u64_vec(&mut out, *page_id);
            write_bytes(&mut out, compressed_before)?;
            write_bytes(&mut out, compressed_after)?;
            write_u64_vec(&mut out, undo_lsn.unwrap_or(0));
        }
        WalPayload::PageDelta {
            path,
            page_id,
            base_lsn,
            base_crc,
            after_crc,
            compressed_after,
        } => {
            if compressed_after.is_empty() || compressed_after.len() > WAL_PAGE_DELTA_MAX_BYTES {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "invalid WAL page delta length",
                ));
            }
            write_string(&mut out, path)?;
            write_u64_vec(&mut out, *page_id);
            write_u64_vec(&mut out, *base_lsn);
            write_u32_vec(&mut out, *base_crc);
            write_u32_vec(&mut out, *after_crc);
            write_bytes(&mut out, compressed_after)?;
        }
        WalPayload::FullPageImage {
            path,
            page_id,
            after,
        } => {
            write_string(&mut out, path)?;
            write_u64_vec(&mut out, *page_id);
            write_bytes(&mut out, after)?;
        }
        WalPayload::FullPageImageCompressed {
            path,
            page_id,
            compressed_after,
        } => {
            write_string(&mut out, path)?;
            write_u64_vec(&mut out, *page_id);
            write_bytes(&mut out, compressed_after)?;
        }
        WalPayload::FileCreate { path, after } => {
            write_string(&mut out, path)?;
            write_bytes(&mut out, after)?;
        }
        WalPayload::FileDelete { path, before } => {
            write_string(&mut out, path)?;
            write_bytes(&mut out, before)?;
        }
        WalPayload::FileExtend {
            path,
            old_len,
            new_len,
        } => {
            write_string(&mut out, path)?;
            write_u64_vec(&mut out, *old_len);
            write_u64_vec(&mut out, *new_len);
        }
        WalPayload::CheckpointEnd {
            redo_lsn,
            next_txn_id,
            next_scn,
            auto_increment_covered_lsn,
            sequence_covered_lsn,
            gc_safe_txn_frontier,
        } => {
            write_u64_vec(&mut out, *redo_lsn);
            write_u64_vec(&mut out, *next_txn_id);
            write_u64_vec(&mut out, *next_scn);
            write_u64_vec(&mut out, *auto_increment_covered_lsn);
            write_u64_vec(&mut out, *sequence_covered_lsn);
            write_u64_vec(&mut out, *gc_safe_txn_frontier);
        }
        WalPayload::Clr {
            path,
            page_id,
            undo_next_lsn,
            after,
        } => {
            write_string(&mut out, path)?;
            write_u64_vec(&mut out, *page_id);
            write_u64_vec(&mut out, *undo_next_lsn);
            write_bytes(&mut out, after)?;
        }
        WalPayload::UndoRecord { undo_id, bytes } => {
            write_u64_vec(&mut out, *undo_id);
            write_bytes(&mut out, bytes)?;
        }
        WalPayload::HeapInsert {
            path,
            page_id,
            slot_id,
            payload,
            undo_lsn,
        } => {
            write_string(&mut out, path)?;
            write_u32_vec(&mut out, *page_id);
            write_u16_vec(&mut out, *slot_id);
            write_bytes(&mut out, payload)?;
            write_u64_vec(&mut out, undo_lsn.unwrap_or(0));
        }
        WalPayload::HeapInsertBatch { path, inserts } => {
            write_string(&mut out, path)?;
            write_u32_vec(&mut out, inserts.len() as u32);
            // Pack the leading fixed-size fields of each insert
            // (page_id 4 + slot_id 2 + payload-len 4 = 10 bytes) into
            // one stack buffer and copy it in a single
            // `extend_from_slice`, then the variable payload, then the
            // trailing `undo_lsn`. The previous shape did five separate
            // `extend_from_slice` calls per insert (50K rows × 5 = 250K
            // calls per batch) — profile-p2-fix4 attributed ~14% of the
            // writer thread's CPU to `encode_record_into` because every
            // small extend re-checks Vec capacity even though the
            // surrounding `encode_record_into` has already reserved the
            // exact frame length.
            for insert in inserts {
                let mut head = [0u8; 10];
                head[0..4].copy_from_slice(&insert.page_id.to_le_bytes());
                head[4..6].copy_from_slice(&insert.slot_id.to_le_bytes());
                head[6..10].copy_from_slice(&(insert.payload.len() as u32).to_le_bytes());
                out.extend_from_slice(&head);
                out.extend_from_slice(&insert.payload);
                out.extend_from_slice(&insert.undo_lsn.unwrap_or(0).to_le_bytes());
            }
        }
        WalPayload::HeapUpdate {
            path,
            page_id,
            slot_id,
            before,
            after,
            undo_lsn,
        } => {
            write_string(&mut out, path)?;
            write_u32_vec(&mut out, *page_id);
            write_u16_vec(&mut out, *slot_id);
            write_bytes(&mut out, before)?;
            write_bytes(&mut out, after)?;
            write_u64_vec(&mut out, undo_lsn.unwrap_or(0));
        }
        WalPayload::HeapUpdateLogical {
            path,
            page_id,
            slot_id,
            patches,
            undo_lsn,
        } => {
            write_string(&mut out, path)?;
            write_u32_vec(&mut out, *page_id);
            write_u16_vec(&mut out, *slot_id);
            write_u32_vec(&mut out, patches.len() as u32);
            for patch in patches {
                write_u16_vec(&mut out, patch.offset);
                write_bytes(&mut out, &patch.before)?;
                write_bytes(&mut out, &patch.after)?;
            }
            write_u64_vec(&mut out, undo_lsn.unwrap_or(0));
        }
        WalPayload::HeapDelete {
            path,
            page_id,
            slot_id,
            before,
            undo_lsn,
        } => {
            write_string(&mut out, path)?;
            write_u32_vec(&mut out, *page_id);
            write_u16_vec(&mut out, *slot_id);
            write_bytes(&mut out, before)?;
            write_u64_vec(&mut out, undo_lsn.unwrap_or(0));
        }
        WalPayload::SlotReclaim {
            path,
            page_id,
            slot_id,
        } => {
            write_string(&mut out, path)?;
            write_u32_vec(&mut out, *page_id);
            write_u16_vec(&mut out, *slot_id);
        }
        WalPayload::IndexInsert { path, key, row_id } => {
            write_string(&mut out, path)?;
            write_bytes(&mut out, key)?;
            encode_wal_row_id(&mut out, *row_id);
        }
        WalPayload::IndexDelete { path, key, row_id } => {
            write_string(&mut out, path)?;
            write_bytes(&mut out, key)?;
            encode_wal_row_id(&mut out, *row_id);
        }
        WalPayload::IndexBatch { path, ops } => {
            write_string(&mut out, path)?;
            write_u32_vec(&mut out, ops.len() as u32);
            for op in ops {
                out.push(op.kind as u8);
                write_bytes(&mut out, &op.key)?;
                encode_wal_row_id(&mut out, op.row_id);
            }
        }
        WalPayload::ZoneMapMergeBatch { bytes }
        | WalPayload::HistoryIndexBatch { bytes }
        | WalPayload::VectorIndexBatch { bytes }
        | WalPayload::VectorIndexModel { bytes } => write_bytes(&mut out, bytes)?,
        WalPayload::SpatialIndexClr { undo_next_lsn } => write_u64_vec(&mut out, *undo_next_lsn),
        WalPayload::SpatialIndexUndo { path, bytes }
        | WalPayload::SpatialPageBatch { path, bytes }
        | WalPayload::VectorIndexManifest { path, bytes } => {
            write_string(&mut out, path)?;
            write_bytes(&mut out, bytes)?;
        }
        WalPayload::VectorIndexImage {
            path,
            offset,
            total,
            bytes,
        } => {
            write_string(&mut out, path)?;
            write_u64_vec(&mut out, *offset);
            write_u64_vec(&mut out, *total);
            write_bytes(&mut out, bytes)?;
        }
        WalPayload::VectorIndexComplete { txn_id, commit_scn } => {
            write_u64_vec(&mut out, *txn_id);
            write_u64_vec(&mut out, *commit_scn);
        }
        WalPayload::CatalogDelta {
            object_ids,
            invalidate_all,
        } => {
            out.push(u8::from(*invalidate_all));
            write_u32_vec(&mut out, object_ids.len() as u32);
            for object_id in object_ids {
                write_u64_vec(&mut out, *object_id);
            }
        }
        WalPayload::DdlCommit {
            job_id,
            retired_paths,
        } => {
            write_u64_vec(&mut out, *job_id);
            write_u32_vec(&mut out, retired_paths.len() as u32);
            for path in retired_paths {
                write_bytes(&mut out, path.as_bytes())?;
            }
        }
        WalPayload::SequenceState {
            sequence_id,
            counter_id,
            resume_value,
            is_called,
            logged_unix_us,
        } => {
            write_u64_vec(&mut out, *sequence_id);
            write_u64_vec(&mut out, *counter_id);
            out.extend_from_slice(&resume_value.to_le_bytes());
            out.push(u8::from(*is_called));
            write_u64_vec(&mut out, *logged_unix_us);
        }
        WalPayload::AutoIncrementAdvance {
            table_id,
            version,
            max_value,
        } => {
            write_u64_vec(&mut out, *table_id);
            write_u64_vec(&mut out, *version);
            write_u64_vec(&mut out, *max_value);
        }
        WalPayload::PrepareTxn {
            xid,
            prepare_scn,
            locks,
            mdl_locks,
            binlog,
        } => {
            encode_xid(&mut out, xid)?;
            write_u64_vec(&mut out, *prepare_scn);
            write_u32_vec(&mut out, locks.len() as u32);
            for lock in locks {
                encode_prepared_lock(&mut out, lock)?;
            }
            write_u32_vec(&mut out, mdl_locks.len() as u32);
            for lock in mdl_locks {
                encode_prepared_mdl_lock(&mut out, lock)?;
            }
            out.push(u8::from(binlog.is_some()));
            if let Some(intent) = binlog {
                write_u32_vec(&mut out, intent.chunks);
                write_u64_vec(&mut out, intent.bytes);
                write_u32_vec(&mut out, intent.crc32);
            }
        }
        WalPayload::TxnBatch { records } => {
            write_u32_vec(&mut out, records.len() as u32);
            for record in records {
                if matches!(record, WalPayload::TxnBatch { .. }) {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "nested WAL transaction batch",
                    ));
                }
                let len = encoded_payload_len(record)?;
                write_u16_vec(&mut out, record.record_type() as u16);
                write_u32_vec(&mut out, len as u32);
                encode_payload_into(record, out)?;
            }
        }
    }
    Ok(())
}

fn encode_xid(out: &mut Vec<u8>, xid: &crate::xa::Xid) -> Result<(), QueryError> {
    out.extend_from_slice(&xid.format_id.to_le_bytes());
    write_bytes(out, &xid.gtrid)?;
    write_bytes(out, &xid.bqual)?;
    Ok(())
}

fn decode_xid(input: &[u8], offset: &mut usize) -> Result<crate::xa::Xid, QueryError> {
    let format_id = read_i64_checked(input, offset)?;
    let gtrid = read_bytes(input, offset)?;
    let bqual = read_bytes(input, offset)?;
    crate::xa::Xid::new(format_id, gtrid, bqual).map_err(|err| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            format!("corrupt WAL XID: {}", err.message()),
        )
    })
}

fn encoded_xid_len(xid: &crate::xa::Xid) -> Result<usize, QueryError> {
    checked_sum([
        8,
        encoded_bytes_len(xid.gtrid.len())?,
        encoded_bytes_len(xid.bqual.len())?,
    ])
}

fn encode_prepared_lock(out: &mut Vec<u8>, lock: &PreparedLockEntry) -> Result<(), QueryError> {
    match lock {
        PreparedLockEntry::Row {
            table_id,
            page_id,
            slot_id,
            mode,
        } => {
            out.push(0);
            write_u64_vec(out, *table_id);
            out.extend_from_slice(&page_id.to_le_bytes());
            write_u16_vec(out, *slot_id);
            out.push(*mode);
        }
        PreparedLockEntry::UniqueKey {
            table_id,
            index_name,
            key_bytes,
        } => {
            out.push(1);
            write_u64_vec(out, *table_id);
            write_string(out, index_name)?;
            write_bytes(out, key_bytes)?;
        }
        PreparedLockEntry::Table { table_id, mode } => {
            out.push(2);
            write_u64_vec(out, *table_id);
            out.push(*mode);
        }
    }
    Ok(())
}

fn decode_prepared_lock(input: &[u8], offset: &mut usize) -> Result<PreparedLockEntry, QueryError> {
    let tag = read_u8_checked(input, offset)?;
    Ok(match tag {
        0 => PreparedLockEntry::Row {
            table_id: read_u64_checked(input, offset)?,
            page_id: read_u32_checked(input, offset)?,
            slot_id: read_u16_checked(input, offset)?,
            mode: read_u8_checked(input, offset)?,
        },
        1 => PreparedLockEntry::UniqueKey {
            table_id: read_u64_checked(input, offset)?,
            index_name: read_string(input, offset)?,
            key_bytes: read_bytes(input, offset)?,
        },
        2 => PreparedLockEntry::Table {
            table_id: read_u64_checked(input, offset)?,
            mode: read_u8_checked(input, offset)?,
        },
        _ => {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt WAL prepared-lock tag",
            ))
        }
    })
}

fn encoded_prepared_lock_len(lock: &PreparedLockEntry) -> Result<usize, QueryError> {
    Ok(match lock {
        PreparedLockEntry::Row { .. } => 1 + 8 + 4 + 2 + 1,
        PreparedLockEntry::UniqueKey {
            index_name,
            key_bytes,
            ..
        } => checked_sum([
            1 + 8,
            encoded_bytes_len(index_name.len())?,
            encoded_bytes_len(key_bytes.len())?,
        ])?,
        PreparedLockEntry::Table { .. } => 1 + 8 + 1,
    })
}

fn encode_prepared_mdl_lock(out: &mut Vec<u8>, lock: &PreparedMdlEntry) -> Result<(), QueryError> {
    out.push(lock.object_type);
    write_optional_string(out, lock.schema.as_deref())?;
    write_optional_string(out, lock.name.as_deref())?;
    out.push(lock.lock_type);
    Ok(())
}

fn decode_prepared_mdl_lock(
    input: &[u8],
    offset: &mut usize,
) -> Result<PreparedMdlEntry, QueryError> {
    Ok(PreparedMdlEntry {
        object_type: read_u8_checked(input, offset)?,
        schema: read_optional_string(input, offset)?,
        name: read_optional_string(input, offset)?,
        lock_type: read_u8_checked(input, offset)?,
    })
}

fn encoded_prepared_mdl_lock_len(lock: &PreparedMdlEntry) -> Result<usize, QueryError> {
    checked_sum([
        1,
        encoded_optional_string_len(lock.schema.as_deref())?,
        encoded_optional_string_len(lock.name.as_deref())?,
        1,
    ])
}

fn encoded_payload_len(payload: &WalPayload) -> Result<usize, QueryError> {
    match payload {
        WalPayload::FileClr { path, action, .. } => checked_sum([
            encoded_bytes_len(path.len())?,
            8,
            1,
            match action {
                FileCompensation::Delete => 0,
                FileCompensation::Restore(bytes) => encoded_bytes_len(bytes.len())?,
                FileCompensation::Truncate(_) => 8,
            },
        ]),
        WalPayload::BeginTxn { .. } => Ok(8),
        WalPayload::CommitTxn { binlog: None, .. } => Ok(16),
        WalPayload::CommitTxn {
            binlog: Some(binlog),
            ..
        } => checked_sum([
            16,
            encoded_bytes_len(binlog.file.len())?,
            8,
            8,
            encoded_bytes_len(binlog.gtid.as_deref().unwrap_or("").len())?,
        ]),
        WalPayload::RollbackTo { compensated, .. } => checked_sum([
            40,
            compensated.len().checked_mul(8).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "rollback WAL too large",
                )
            })?,
        ]),
        WalPayload::AbortTxn | WalPayload::EndTxn | WalPayload::CheckpointBegin => Ok(0),
        WalPayload::PageUpdate {
            path,
            before,
            after,
            ..
        } => checked_sum([
            encoded_bytes_len(path.len())?,
            8,
            encoded_bytes_len(before.len())?,
            encoded_bytes_len(after.len())?,
            8,
        ]),
        WalPayload::PageUpdateCompressed {
            path,
            compressed_before,
            compressed_after,
            ..
        } => checked_sum([
            encoded_bytes_len(path.len())?,
            8,
            encoded_bytes_len(compressed_before.len())?,
            encoded_bytes_len(compressed_after.len())?,
            8,
        ]),
        WalPayload::PageDelta {
            path,
            compressed_after,
            ..
        } => {
            if compressed_after.is_empty() || compressed_after.len() > WAL_PAGE_DELTA_MAX_BYTES {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "invalid WAL page delta length",
                ));
            }
            checked_sum([
                encoded_bytes_len(path.len())?,
                24,
                encoded_bytes_len(compressed_after.len())?,
            ])
        }
        WalPayload::FullPageImage { path, after, .. } => checked_sum([
            encoded_bytes_len(path.len())?,
            8,
            encoded_bytes_len(after.len())?,
        ]),
        WalPayload::FullPageImageCompressed {
            path,
            compressed_after,
            ..
        } => checked_sum([
            encoded_bytes_len(path.len())?,
            8,
            encoded_bytes_len(compressed_after.len())?,
        ]),
        WalPayload::FileCreate { path, after } => checked_sum([
            encoded_bytes_len(path.len())?,
            encoded_bytes_len(after.len())?,
        ]),
        WalPayload::FileDelete { path, before } => checked_sum([
            encoded_bytes_len(path.len())?,
            encoded_bytes_len(before.len())?,
        ]),
        WalPayload::FileExtend { path, .. } => checked_sum([encoded_bytes_len(path.len())?, 8, 8]),
        WalPayload::CheckpointEnd { .. } => Ok(8 + 8 + 8 + 8 + 8 + 8),
        WalPayload::Clr { path, after, .. } => checked_sum([
            encoded_bytes_len(path.len())?,
            8,
            8,
            encoded_bytes_len(after.len())?,
        ]),
        WalPayload::UndoRecord { bytes, .. } => checked_sum([8, encoded_bytes_len(bytes.len())?]),
        // After rowid-6bytes the per-record RowId is 6 bytes (page_id 4
        // + slot_id 2); the legacy 2-byte `generation` is gone.
        WalPayload::HeapInsert { path, payload, .. } => checked_sum([
            encoded_bytes_len(path.len())?,
            4,
            2,
            encoded_bytes_len(payload.len())?,
            8,
        ]),
        WalPayload::HeapInsertBatch { path, inserts } => {
            let mut len = checked_sum([encoded_bytes_len(path.len())?, 4])?;
            for insert in inserts {
                len = checked_add(len, 4 + 2)?;
                len = checked_add(len, encoded_bytes_len(insert.payload.len())?)?;
                len = checked_add(len, 8)?;
            }
            Ok(len)
        }
        WalPayload::HeapUpdate {
            path,
            before,
            after,
            ..
        } => checked_sum([
            encoded_bytes_len(path.len())?,
            4,
            2,
            encoded_bytes_len(before.len())?,
            encoded_bytes_len(after.len())?,
            8,
        ]),
        WalPayload::HeapUpdateLogical { path, patches, .. } => {
            let mut len = checked_sum([encoded_bytes_len(path.len())?, 4, 2, 4])?;
            for patch in patches {
                len = checked_add(len, 2)?;
                len = checked_add(len, encoded_bytes_len(patch.before.len())?)?;
                len = checked_add(len, encoded_bytes_len(patch.after.len())?)?;
            }
            checked_add(len, 8)
        }
        WalPayload::HeapDelete { path, before, .. } => checked_sum([
            encoded_bytes_len(path.len())?,
            4,
            2,
            encoded_bytes_len(before.len())?,
            8,
        ]),
        WalPayload::SlotReclaim { path, .. } => checked_sum([encoded_bytes_len(path.len())?, 4, 2]),
        WalPayload::IndexInsert { path, key, .. } | WalPayload::IndexDelete { path, key, .. } => {
            checked_sum([
                encoded_bytes_len(path.len())?,
                encoded_bytes_len(key.len())?,
                encoded_wal_row_id_len(),
            ])
        }
        WalPayload::IndexBatch { path, ops } => {
            let mut len = checked_sum([encoded_bytes_len(path.len())?, 4])?;
            for op in ops {
                len = checked_add(len, 1)?;
                len = checked_add(len, encoded_bytes_len(op.key.len())?)?;
                len = checked_add(len, encoded_wal_row_id_len())?;
            }
            Ok(len)
        }
        WalPayload::ZoneMapMergeBatch { bytes }
        | WalPayload::HistoryIndexBatch { bytes }
        | WalPayload::VectorIndexBatch { bytes }
        | WalPayload::VectorIndexModel { bytes } => encoded_bytes_len(bytes.len()),
        WalPayload::SpatialIndexClr { .. } => Ok(8),
        WalPayload::SpatialIndexUndo { path, bytes }
        | WalPayload::SpatialPageBatch { path, bytes }
        | WalPayload::VectorIndexManifest { path, bytes } => checked_sum([
            encoded_bytes_len(path.len())?,
            encoded_bytes_len(bytes.len())?,
        ]),
        WalPayload::VectorIndexImage { path, bytes, .. } => checked_sum([
            encoded_bytes_len(path.len())?,
            16,
            encoded_bytes_len(bytes.len())?,
        ]),
        WalPayload::VectorIndexComplete { .. } => Ok(16),
        WalPayload::CatalogDelta { object_ids, .. } => {
            checked_add(1 + 4, object_ids.len().saturating_mul(8))
        }
        WalPayload::DdlCommit { retired_paths, .. } => {
            let mut len = 12;
            for path in retired_paths {
                len = checked_add(len, encoded_bytes_len(path.len())?)?;
            }
            Ok(len)
        }
        WalPayload::AutoIncrementAdvance { .. } => Ok(24),
        WalPayload::SequenceState { .. } => Ok(33),
        WalPayload::PrepareTxn {
            xid,
            locks,
            mdl_locks,
            binlog,
            ..
        } => {
            let mut len = checked_sum([
                encoded_xid_len(xid)?,
                8,
                4,
                1 + usize::from(binlog.is_some()) * 16,
            ])?;
            for lock in locks {
                len = checked_add(len, encoded_prepared_lock_len(lock)?)?;
            }
            len = checked_add(len, 4)?;
            for lock in mdl_locks {
                len = checked_add(len, encoded_prepared_mdl_lock_len(lock)?)?;
            }
            Ok(len)
        }
        WalPayload::TxnBatch { records } => {
            let mut len = 4usize;
            for record in records {
                if matches!(record, WalPayload::TxnBatch { .. }) {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "nested WAL transaction batch",
                    ));
                }
                len = checked_add(len, 2 + 4)?;
                len = checked_add(len, encoded_payload_len(record)?)?;
            }
            Ok(len)
        }
    }
}

fn encoded_bytes_len(len: usize) -> Result<usize, QueryError> {
    if len > u32::MAX as usize {
        return Err(error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "WAL payload is too large",
        ));
    }
    checked_add(4, len)
}

fn encoded_wal_row_id_len() -> usize {
    // rowid-6bytes WAL row-id layout: u32 page_id + u16 slot = 6 B.
    // Was 8 B (with u16 generation) before, and 20 B (u64 table_id +
    // u64 page_id + …) before per-page table_id deduplication.
    4 + 2
}

fn checked_sum<const N: usize>(parts: [usize; N]) -> Result<usize, QueryError> {
    parts.into_iter().try_fold(0usize, checked_add)
}

fn checked_add(left: usize, right: usize) -> Result<usize, QueryError> {
    left.checked_add(right).ok_or_else(|| {
        error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "WAL payload is too large",
        )
    })
}

fn bounded_wal_count(
    input: &[u8],
    offset: &mut usize,
    minimum_bytes: usize,
) -> Result<usize, QueryError> {
    let count = read_u32_checked(input, offset)? as usize;
    if count > input.len().saturating_sub(*offset) / minimum_bytes {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "WAL collection count exceeds its payload",
        ));
    }
    Ok(count)
}

fn decode_payload(record_type: WalRecordType, input: &[u8]) -> Result<WalPayload, QueryError> {
    let mut offset = 0;
    Ok(match record_type {
        WalRecordType::FileClr => {
            let path = read_string(input, &mut offset)?;
            let undo_next_lsn = read_u64_checked(input, &mut offset)?;
            let action = match read_u8_checked(input, &mut offset)? {
                0 => FileCompensation::Delete,
                1 => FileCompensation::Restore(read_bytes(input, &mut offset)?),
                2 => FileCompensation::Truncate(read_u64_checked(input, &mut offset)?),
                _ => {
                    return Err(io_err(std::io::Error::other(
                        "invalid file compensation action",
                    )))
                }
            };
            WalPayload::FileClr {
                path,
                undo_next_lsn,
                action,
            }
        }
        WalRecordType::BeginTxn => WalPayload::BeginTxn {
            start_scn: read_u64_checked(input, &mut offset)?,
        },
        WalRecordType::CommitTxn => {
            let commit_scn = read_u64_checked(input, &mut offset)?;
            let commit_unix_us = read_u64_checked(input, &mut offset)?;
            let binlog = if offset < input.len() {
                let file = read_string(input, &mut offset)?;
                let start_pos = read_u64_checked(input, &mut offset)?;
                let end_pos = read_u64_checked(input, &mut offset)?;
                let gtid = read_string(input, &mut offset)?;
                Some(WalBinlogRef {
                    file,
                    start_pos,
                    end_pos,
                    gtid: (!gtid.is_empty()).then_some(gtid),
                })
            } else {
                None
            };
            WalPayload::CommitTxn {
                commit_scn,
                commit_unix_us,
                binlog,
            }
        }
        WalRecordType::RollbackTo => {
            let head = read_u64_checked(input, &mut offset)?;
            let undo_next_lsn = read_u64_checked(input, &mut offset)?;
            let skip_lsn_start = read_u64_checked(input, &mut offset)?;
            let skip_lsn_end = read_u64_checked(input, &mut offset)?;
            if skip_lsn_start > skip_lsn_end {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt rollback WAL interval",
                ));
            }
            let count = read_u64_checked(input, &mut offset)?;
            if count > ((input.len() - offset) / 8) as u64 {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt rollback WAL",
                ));
            }
            let mut compensated = Vec::with_capacity(count as usize);
            for _ in 0..count {
                compensated.push(read_u64_checked(input, &mut offset)?);
            }
            WalPayload::RollbackTo {
                undo_head: (head != 0).then_some(head),
                undo_next_lsn,
                skip_lsn_start,
                skip_lsn_end,
                compensated,
            }
        }
        WalRecordType::AbortTxn => WalPayload::AbortTxn,
        WalRecordType::EndTxn => WalPayload::EndTxn,
        WalRecordType::PageUpdate => {
            let path = read_string(input, &mut offset)?;
            let page_id = read_u64_checked(input, &mut offset)?;
            let before = read_bytes(input, &mut offset)?;
            let after = read_bytes(input, &mut offset)?;
            let undo_lsn = read_u64_checked(input, &mut offset)?;
            WalPayload::PageUpdate {
                path,
                page_id,
                before,
                after,
                undo_lsn: (undo_lsn != 0).then_some(undo_lsn),
            }
        }
        WalRecordType::PageUpdateCompressed => {
            let path = read_string(input, &mut offset)?;
            let page_id = read_u64_checked(input, &mut offset)?;
            let compressed_before = read_bytes(input, &mut offset)?;
            let compressed_after = read_bytes(input, &mut offset)?;
            let undo_lsn = read_u64_checked(input, &mut offset)?;
            WalPayload::PageUpdateCompressed {
                path,
                page_id,
                compressed_before,
                compressed_after,
                undo_lsn: (undo_lsn != 0).then_some(undo_lsn),
            }
        }
        WalRecordType::PageDelta => {
            let path = read_string(input, &mut offset)?;
            let page_id = read_u64_checked(input, &mut offset)?;
            let base_lsn = read_u64_checked(input, &mut offset)?;
            let base_crc = read_u32_checked(input, &mut offset)?;
            let after_crc = read_u32_checked(input, &mut offset)?;
            let len = read_u32_checked(input, &mut offset)? as usize;
            if len == 0
                || len > WAL_PAGE_DELTA_MAX_BYTES
                || len != input.len().saturating_sub(offset)
            {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "invalid WAL page delta length",
                ));
            }
            let compressed_after = input[offset..offset + len].to_vec();
            WalPayload::PageDelta {
                path,
                page_id,
                base_lsn,
                base_crc,
                after_crc,
                compressed_after,
            }
        }
        WalRecordType::FullPageImage => WalPayload::FullPageImage {
            path: read_string(input, &mut offset)?,
            page_id: read_u64_checked(input, &mut offset)?,
            after: read_bytes(input, &mut offset)?,
        },
        WalRecordType::FullPageImageCompressed => WalPayload::FullPageImageCompressed {
            path: read_string(input, &mut offset)?,
            page_id: read_u64_checked(input, &mut offset)?,
            compressed_after: read_bytes(input, &mut offset)?,
        },
        WalRecordType::FileCreate => WalPayload::FileCreate {
            path: read_string(input, &mut offset)?,
            after: read_bytes(input, &mut offset)?,
        },
        WalRecordType::FileDelete => WalPayload::FileDelete {
            path: read_string(input, &mut offset)?,
            before: read_bytes(input, &mut offset)?,
        },
        WalRecordType::FileExtend => WalPayload::FileExtend {
            path: read_string(input, &mut offset)?,
            old_len: read_u64_checked(input, &mut offset)?,
            new_len: read_u64_checked(input, &mut offset)?,
        },
        WalRecordType::CheckpointBegin => WalPayload::CheckpointBegin,
        WalRecordType::CheckpointEnd => {
            let redo_lsn = read_u64_checked(input, &mut offset)?;
            let next_txn_id = read_u64_checked(input, &mut offset)?;
            let next_scn = read_u64_checked(input, &mut offset)?;
            let auto_increment_covered_lsn = read_u64_checked(input, &mut offset)?;
            let sequence_covered_lsn = read_u64_checked(input, &mut offset)?;
            // Old checkpoints provide allocation, but no registration proof.
            let gc_safe_txn_frontier = if offset == input.len() {
                0
            } else {
                read_u64_checked(input, &mut offset)?
            };
            WalPayload::CheckpointEnd {
                redo_lsn,
                next_txn_id,
                next_scn,
                auto_increment_covered_lsn,
                sequence_covered_lsn,
                gc_safe_txn_frontier,
            }
        }
        WalRecordType::Clr => WalPayload::Clr {
            path: read_string(input, &mut offset)?,
            page_id: read_u64_checked(input, &mut offset)?,
            undo_next_lsn: read_u64_checked(input, &mut offset)?,
            after: read_bytes(input, &mut offset)?,
        },
        WalRecordType::UndoRecord => WalPayload::UndoRecord {
            undo_id: read_u64_checked(input, &mut offset)?,
            bytes: read_bytes(input, &mut offset)?,
        },
        WalRecordType::HeapInsert => {
            let path = read_string(input, &mut offset)?;
            let page_id = read_u32_checked(input, &mut offset)?;
            let slot_id = read_u16_checked(input, &mut offset)?;
            let payload = read_bytes(input, &mut offset)?;
            let undo_lsn = read_u64_checked(input, &mut offset)?;
            WalPayload::HeapInsert {
                path,
                page_id,
                slot_id,
                payload,
                undo_lsn: (undo_lsn != 0).then_some(undo_lsn),
            }
        }
        WalRecordType::HeapInsertBatch => {
            let path = read_string(input, &mut offset)?;
            let count = bounded_wal_count(input, &mut offset, 18)?;
            let mut inserts = Vec::with_capacity(count);
            for _ in 0..count {
                let page_id = read_u32_checked(input, &mut offset)?;
                let slot_id = read_u16_checked(input, &mut offset)?;
                let payload = read_bytes(input, &mut offset)?;
                let undo_lsn = read_u64_checked(input, &mut offset)?;
                inserts.push(WalHeapInsert {
                    page_id,
                    slot_id,
                    payload,
                    undo_lsn: (undo_lsn != 0).then_some(undo_lsn),
                });
            }
            WalPayload::HeapInsertBatch { path, inserts }
        }
        WalRecordType::HeapUpdate => {
            let path = read_string(input, &mut offset)?;
            let page_id = read_u32_checked(input, &mut offset)?;
            let slot_id = read_u16_checked(input, &mut offset)?;
            let before = read_bytes(input, &mut offset)?;
            let after = read_bytes(input, &mut offset)?;
            let undo_lsn = read_u64_checked(input, &mut offset)?;
            WalPayload::HeapUpdate {
                path,
                page_id,
                slot_id,
                before,
                after,
                undo_lsn: (undo_lsn != 0).then_some(undo_lsn),
            }
        }
        WalRecordType::HeapUpdateLogical => {
            let path = read_string(input, &mut offset)?;
            let page_id = read_u32_checked(input, &mut offset)?;
            let slot_id = read_u16_checked(input, &mut offset)?;
            let count = bounded_wal_count(input, &mut offset, 10)?;
            let mut patches = Vec::with_capacity(count);
            for _ in 0..count {
                let off = read_u16_checked(input, &mut offset)?;
                let before = read_bytes(input, &mut offset)?;
                let after = read_bytes(input, &mut offset)?;
                patches.push(WalColumnPatch {
                    offset: off,
                    before,
                    after,
                });
            }
            let undo_lsn = read_u64_checked(input, &mut offset)?;
            WalPayload::HeapUpdateLogical {
                path,
                page_id,
                slot_id,
                patches,
                undo_lsn: (undo_lsn != 0).then_some(undo_lsn),
            }
        }
        WalRecordType::HeapDelete => {
            let path = read_string(input, &mut offset)?;
            let page_id = read_u32_checked(input, &mut offset)?;
            let slot_id = read_u16_checked(input, &mut offset)?;
            let before = read_bytes(input, &mut offset)?;
            let undo_lsn = read_u64_checked(input, &mut offset)?;
            WalPayload::HeapDelete {
                path,
                page_id,
                slot_id,
                before,
                undo_lsn: (undo_lsn != 0).then_some(undo_lsn),
            }
        }
        WalRecordType::SlotReclaim => {
            let path = read_string(input, &mut offset)?;
            let page_id = read_u32_checked(input, &mut offset)?;
            let slot_id = read_u16_checked(input, &mut offset)?;
            WalPayload::SlotReclaim {
                path,
                page_id,
                slot_id,
            }
        }
        WalRecordType::IndexInsert => WalPayload::IndexInsert {
            path: read_string(input, &mut offset)?,
            key: read_bytes(input, &mut offset)?,
            row_id: decode_wal_row_id(input, &mut offset)?,
        },
        WalRecordType::IndexDelete => WalPayload::IndexDelete {
            path: read_string(input, &mut offset)?,
            key: read_bytes(input, &mut offset)?,
            row_id: decode_wal_row_id(input, &mut offset)?,
        },
        WalRecordType::IndexBatch => {
            let path = read_string(input, &mut offset)?;
            let count = bounded_wal_count(input, &mut offset, 11)?;
            let mut ops = Vec::with_capacity(count);
            for _ in 0..count {
                let kind = match read_u8_checked(input, &mut offset)? {
                    1 => WalIndexOpKind::Insert,
                    2 => WalIndexOpKind::Delete,
                    _ => {
                        return Err(error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "corrupt WAL index op kind",
                        ))
                    }
                };
                ops.push(WalIndexOp {
                    kind,
                    key: read_bytes(input, &mut offset)?,
                    row_id: decode_wal_row_id(input, &mut offset)?,
                });
            }
            WalPayload::IndexBatch { path, ops }
        }
        WalRecordType::PrepareTxn => {
            let xid = decode_xid(input, &mut offset)?;
            let prepare_scn = read_u64_checked(input, &mut offset)?;
            let count = bounded_wal_count(input, &mut offset, 10)?;
            let mut locks = Vec::with_capacity(count);
            for _ in 0..count {
                locks.push(decode_prepared_lock(input, &mut offset)?);
            }
            let mut mdl_locks = Vec::new();
            if offset < input.len() {
                let count = bounded_wal_count(input, &mut offset, 4)?;
                mdl_locks.reserve(count);
                for _ in 0..count {
                    mdl_locks.push(decode_prepared_mdl_lock(input, &mut offset)?);
                }
            }
            let binlog = match read_u8_checked(input, &mut offset)? {
                0 => None,
                1 => Some(PreparedBinlogRef {
                    chunks: read_u32_checked(input, &mut offset)?,
                    bytes: read_u64_checked(input, &mut offset)?,
                    crc32: read_u32_checked(input, &mut offset)?,
                }),
                _ => {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "invalid XA binlog intent flag",
                    ))
                }
            };
            WalPayload::PrepareTxn {
                xid,
                prepare_scn,
                locks,
                mdl_locks,
                binlog,
            }
        }
        WalRecordType::ZoneMapMergeBatch => WalPayload::ZoneMapMergeBatch {
            bytes: read_bytes(input, &mut offset)?,
        },
        WalRecordType::HistoryIndexBatch => WalPayload::HistoryIndexBatch {
            bytes: read_bytes(input, &mut offset)?,
        },
        WalRecordType::VectorIndexBatch => WalPayload::VectorIndexBatch {
            bytes: read_bytes(input, &mut offset)?,
        },
        WalRecordType::VectorIndexModel => WalPayload::VectorIndexModel {
            bytes: read_bytes(input, &mut offset)?,
        },
        WalRecordType::SpatialIndexUndo => WalPayload::SpatialIndexUndo {
            path: read_string(input, &mut offset)?,
            bytes: read_bytes(input, &mut offset)?,
        },
        WalRecordType::SpatialIndexClr => WalPayload::SpatialIndexClr {
            undo_next_lsn: read_u64_checked(input, &mut offset)?,
        },
        WalRecordType::SpatialPageBatch => WalPayload::SpatialPageBatch {
            path: read_string(input, &mut offset)?,
            bytes: read_bytes(input, &mut offset)?,
        },
        WalRecordType::VectorIndexManifest => WalPayload::VectorIndexManifest {
            path: read_string(input, &mut offset)?,
            bytes: read_bytes(input, &mut offset)?,
        },
        WalRecordType::VectorIndexImage => WalPayload::VectorIndexImage {
            path: read_string(input, &mut offset)?,
            offset: read_u64_checked(input, &mut offset)?,
            total: read_u64_checked(input, &mut offset)?,
            bytes: read_bytes(input, &mut offset)?,
        },
        WalRecordType::VectorIndexComplete => WalPayload::VectorIndexComplete {
            txn_id: read_u64_checked(input, &mut offset)?,
            commit_scn: read_u64_checked(input, &mut offset)?,
        },
        WalRecordType::CatalogDelta => {
            let invalidate_all = read_u8_checked(input, &mut offset)? != 0;
            let count = bounded_wal_count(input, &mut offset, 8)?;
            if count > 4096 {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt WAL catalog delta count",
                ));
            }
            let mut object_ids = Vec::with_capacity(count);
            for _ in 0..count {
                object_ids.push(read_u64_checked(input, &mut offset)?);
            }
            WalPayload::CatalogDelta {
                object_ids,
                invalidate_all,
            }
        }
        WalRecordType::DdlCommit => {
            let job_id = read_u64_checked(input, &mut offset)?;
            let count = read_u32_checked(input, &mut offset)? as usize;
            if count > input.len().saturating_sub(offset) / 4 {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt WAL DDL retirement count",
                ));
            }
            let mut retired_paths = Vec::with_capacity(count);
            for _ in 0..count {
                retired_paths.push(read_string(input, &mut offset)?);
            }
            WalPayload::DdlCommit {
                job_id,
                retired_paths,
            }
        }
        WalRecordType::SequenceState => {
            if input.len() != 33 {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "invalid sequence WAL length",
                ));
            }
            let sequence_id = read_u64_checked(input, &mut offset)?;
            let counter_id = read_u64_checked(input, &mut offset)?;
            let resume_value = read_i64_checked(input, &mut offset)?;
            let called = *input.get(offset).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "truncated sequence WAL",
                )
            })?;
            if sequence_id == 0 || counter_id == 0 || called > 1 {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "invalid sequence WAL state",
                ));
            }
            offset += 1;
            let logged_unix_us = read_u64_checked(input, &mut offset)?;
            if logged_unix_us == 0 {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "invalid sequence WAL time",
                ));
            }
            WalPayload::SequenceState {
                sequence_id,
                counter_id,
                resume_value,
                is_called: called != 0,
                logged_unix_us,
            }
        }
        WalRecordType::AutoIncrementAdvance => WalPayload::AutoIncrementAdvance {
            table_id: read_u64_checked(input, &mut offset)?,
            version: read_u64_checked(input, &mut offset)?,
            max_value: read_u64_checked(input, &mut offset)?,
        },
        WalRecordType::TxnBatch => {
            let count = bounded_wal_count(input, &mut offset, 6)?;
            if count == 0 || count > MAX_TXN_BATCH_RECORDS {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt WAL transaction batch count",
                ));
            }
            let mut records = Vec::with_capacity(count);
            for _ in 0..count {
                let ty = WalRecordType::from_u16(read_u16_checked(input, &mut offset)?)?;
                if ty == WalRecordType::TxnBatch {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "nested WAL transaction batch",
                    ));
                }
                let len = read_u32_checked(input, &mut offset)? as usize;
                let end = offset
                    .checked_add(len)
                    .filter(|end| *end <= input.len())
                    .ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "corrupt WAL transaction batch",
                        )
                    })?;
                records.push(decode_payload(ty, &input[offset..end])?);
                offset = end;
            }
            if !matches!(records.last(), Some(WalPayload::CommitTxn { .. }))
                && !records
                    .iter()
                    .all(|r| matches!(r, WalPayload::FileCreate { .. }))
            {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "WAL transaction batch lacks commit tail",
                ));
            }
            WalPayload::TxnBatch { records }
        }
    })
}

fn write_string(out: &mut Vec<u8>, value: &str) -> Result<(), QueryError> {
    write_bytes(out, value.as_bytes())
}

fn write_optional_string(out: &mut Vec<u8>, value: Option<&str>) -> Result<(), QueryError> {
    match value {
        Some(value) => {
            out.push(1);
            write_string(out, value)
        }
        None => {
            out.push(0);
            Ok(())
        }
    }
}

fn read_string(input: &[u8], offset: &mut usize) -> Result<String, QueryError> {
    String::from_utf8(read_bytes(input, offset)?).map_err(|err| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            format!("corrupt WAL string: {err}"),
        )
    })
}

fn read_optional_string(input: &[u8], offset: &mut usize) -> Result<Option<String>, QueryError> {
    match read_u8_checked(input, offset)? {
        0 => Ok(None),
        1 => read_string(input, offset).map(Some),
        _ => Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt WAL optional string tag",
        )),
    }
}

fn encoded_optional_string_len(value: Option<&str>) -> Result<usize, QueryError> {
    Ok(1 + match value {
        Some(value) => encoded_bytes_len(value.len())?,
        None => 0,
    })
}

fn write_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), QueryError> {
    if bytes.len() > u32::MAX as usize {
        return Err(error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "WAL payload is too large",
        ));
    }
    write_u32_vec(out, bytes.len() as u32);
    out.extend_from_slice(bytes);
    Ok(())
}

fn read_bytes(input: &[u8], offset: &mut usize) -> Result<Vec<u8>, QueryError> {
    let len = read_u32_checked(input, offset)? as usize;
    let end = offset.checked_add(len).ok_or_else(|| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt WAL bytes length",
        )
    })?;
    if end > input.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt WAL bytes payload",
        ));
    }
    let bytes = input[*offset..end].to_vec();
    *offset = end;
    Ok(bytes)
}

fn write_u32_vec(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_u16_vec(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_u64_vec(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

/// WAL row-id encoding: 6 bytes, little-endian. `page_id`
/// (4B) + `slot_id` (2B). Was 8 B before the rowid-6bytes refactor;
/// the per-slot generation ABA counter is gone. The owning index
/// file path is in the parent `WalPayload` record, so no `table_id`
/// is carried here.
fn encode_wal_row_id(out: &mut Vec<u8>, row_id: WalRowId) {
    out.extend_from_slice(&row_id.page_id.to_le_bytes());
    out.extend_from_slice(&row_id.slot_id.to_le_bytes());
}

fn decode_wal_row_id(input: &[u8], offset: &mut usize) -> Result<WalRowId, QueryError> {
    let page_id = read_u32_checked(input, offset)?;
    let slot_id = read_u16_checked(input, offset)?;
    Ok(WalRowId { page_id, slot_id })
}

fn read_u8_checked(buf: &[u8], offset: &mut usize) -> Result<u8, QueryError> {
    if *offset >= buf.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt WAL: short payload",
        ));
    }
    let value = buf[*offset];
    *offset += 1;
    Ok(value)
}

fn read_u16_checked(buf: &[u8], offset: &mut usize) -> Result<u16, QueryError> {
    crate::util::bytes::read_word(buf, offset)
        .map(u16::from_le_bytes)
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt WAL: short payload",
            )
        })
}

fn read_u32_checked(buf: &[u8], offset: &mut usize) -> Result<u32, QueryError> {
    crate::util::bytes::read_word(buf, offset)
        .map(u32::from_le_bytes)
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt WAL: short payload",
            )
        })
}

fn read_u64_checked(buf: &[u8], offset: &mut usize) -> Result<u64, QueryError> {
    crate::util::bytes::read_word(buf, offset)
        .map(u64::from_le_bytes)
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt WAL: short payload",
            )
        })
}

fn read_i64_checked(buf: &[u8], offset: &mut usize) -> Result<i64, QueryError> {
    if *offset + 8 > buf.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt WAL: short payload",
        ));
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&buf[*offset..*offset + 8]);
    *offset += 8;
    Ok(i64::from_le_bytes(bytes))
}

/// Hardware-accelerated WAL checksum. `crc32fast` dispatches to
/// SSE4.2 PCLMULQDQ on x86-64 and the `crc32` instruction family on
/// ARMv8; throughput is in the 5-10 GB/s range, against ~500 MB/s for
/// the previous in-tree FNV-1 variants. Encode and decode (including
/// segment headers and replication frames) share this primitive so
/// there is no fast/slow ambiguity at decode time.
fn wal_frame_checksum(header: &[u8], payload: &[u8]) -> u32 {
    let mut checksum = crc32fast::Hasher::new();
    checksum.update(&header[..44]);
    checksum.update(payload);
    checksum.finalize()
}

fn wal_checksum(bytes: &[u8]) -> u32 {
    crc32fast::hash(bytes)
}

fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn elapsed_us(started: Instant) -> u64 {
    started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64
}

/// Position-explicit `write_all`, used by the WAL hot path so that
/// preallocated segments still receive valid bytes at the correct
/// offset (`File::write_all` on a non-append fd would write at the
/// current cursor; `pwrite` is atomic with respect to other readers
/// of the same fd). Windows uses a separate overlapped file description.
#[cfg(unix)]
fn write_all_at(file: &File, buf: &[u8], offset: u64) -> Result<(), QueryError> {
    file.write_all_at(buf, offset).map_err(io_err)
}

#[cfg(windows)]
fn write_all_at(file: &File, buf: &[u8], offset: u64) -> Result<(), QueryError> {
    file.write_all_at(buf, offset).map_err(io_err)
}

/// How the WAL is flushed to durable media. Resolved once at
/// `Database` open from durability mode + platform, overridable by
/// `MYDB_WAL_SYNC_METHOD` / the `mydb_wal_sync_method` sysvar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalSyncMethod {
    /// `fdatasync` — data pages only, skips inode-metadata journal.
    /// Linux fast path; on Darwin it is an alias for `fsync` and does
    /// NOT flush the disk write cache.
    Fdatasync,
    /// `fsync` — data + metadata. On Darwin this still does NOT flush the
    /// drive's volatile write cache (so it is not power-loss durable).
    Fsync,
    /// macOS `fcntl(F_FULLFSYNC)` — the only call that flushes the drive
    /// write cache on Darwin, hence the only truly power-loss-durable
    /// option there (5-10× slower than `fsync`). On Linux it falls back
    /// to `fsync` (which already issues a write barrier on mainstream
    /// filesystems).
    Fullfsync,
    /// macOS `fcntl(F_BARRIERFSYNC)` — issues an I/O barrier to the drive
    /// so this commit's bytes are ordered ahead of any later writes,
    /// WITHOUT waiting for the full write-cache flush that `F_FULLFSYNC`
    /// forces. Much cheaper than `F_FULLFSYNC` while still preventing the
    /// drive from reordering a later write ahead of this one (the failure
    /// mode that silently corrupts a WAL). Same intent as SQLite's
    /// barrier sync. Not a full power-loss guarantee — the last barrier's
    /// bytes can still be lost on power loss — but it bounds the loss to a
    /// recent suffix and preserves WAL ordering. On Linux / other Unix it
    /// falls back to `fsync`; only meaningful on Darwin.
    Barrierfsync,
    /// No flush at all — the bytes reach the OS page cache only. Survives
    /// a process crash but not an OS crash / power loss. For benchmarking
    /// the non-durable ceiling; never a strict-mode default.
    Nosync,
}

const SYNC_METHOD_FDATASYNC: u8 = 0;
const SYNC_METHOD_FSYNC: u8 = 1;
const SYNC_METHOD_FULLFSYNC: u8 = 2;
const SYNC_METHOD_NOSYNC: u8 = 3;
const SYNC_METHOD_BARRIERFSYNC: u8 = 4;
const SYNC_METHOD_UNSET: u8 = 255;

static WAL_SYNC_METHOD: AtomicU8 = AtomicU8::new(SYNC_METHOD_UNSET);

impl WalSyncMethod {
    pub fn as_str(self) -> &'static str {
        match self {
            WalSyncMethod::Fdatasync => "fdatasync",
            WalSyncMethod::Fsync => "fsync",
            WalSyncMethod::Fullfsync => "fullfsync",
            WalSyncMethod::Barrierfsync => "barrierfsync",
            WalSyncMethod::Nosync => "nosync",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "fdatasync" | "datasync" => Some(WalSyncMethod::Fdatasync),
            "fsync" => Some(WalSyncMethod::Fsync),
            "fullfsync" | "f_fullfsync" | "full_fsync" => Some(WalSyncMethod::Fullfsync),
            "barrierfsync" | "f_barrierfsync" | "barrier_fsync" | "barrier" => {
                Some(WalSyncMethod::Barrierfsync)
            }
            "nosync" | "none" | "off" => Some(WalSyncMethod::Nosync),
            _ => None,
        }
    }

    fn to_u8(self) -> u8 {
        match self {
            WalSyncMethod::Fdatasync => SYNC_METHOD_FDATASYNC,
            WalSyncMethod::Fsync => SYNC_METHOD_FSYNC,
            WalSyncMethod::Fullfsync => SYNC_METHOD_FULLFSYNC,
            WalSyncMethod::Barrierfsync => SYNC_METHOD_BARRIERFSYNC,
            WalSyncMethod::Nosync => SYNC_METHOD_NOSYNC,
        }
    }

    fn from_u8(v: u8) -> WalSyncMethod {
        match v {
            SYNC_METHOD_FDATASYNC => WalSyncMethod::Fdatasync,
            SYNC_METHOD_FULLFSYNC => WalSyncMethod::Fullfsync,
            SYNC_METHOD_BARRIERFSYNC => WalSyncMethod::Barrierfsync,
            SYNC_METHOD_NOSYNC => WalSyncMethod::Nosync,
            _ => WalSyncMethod::Fsync,
        }
    }
}

fn wal_sync_method_from_env() -> Option<WalSyncMethod> {
    crate::util::env::var("MYDB_WAL_SYNC_METHOD")
        .ok()
        .and_then(|v| WalSyncMethod::parse(&v))
}

/// Platform-correct default, matching the conventions of the databases
/// mydb is measured against. macOS defaults to `Fsync` (like MySQL's
/// `innodb_flush_method` and PostgreSQL's `wal_sync_method=fsync`, which
/// both leave the slow `F_FULLFSYNC` / `fsync_writethrough` path as an
/// explicit opt-in): a plain `fsync` on Darwin is NOT power-loss durable,
/// but `F_FULLFSYNC` is 5-10× slower (it regressed TPC-C ~25 %), so it is
/// not forced on every strict user. Operators that need true power-loss
/// durability on macOS opt in with `MYDB_WAL_SYNC_METHOD=fullfsync` (or
/// the `mydb_wal_sync_method` sysvar). Linux defaults to `Fdatasync`;
/// everything else to `Fsync`. `strict` is currently unused but kept so
/// the policy can become durability-aware again without a signature
/// change.
fn default_wal_sync_method(strict: bool) -> WalSyncMethod {
    let _ = strict;
    #[cfg(target_os = "macos")]
    {
        WalSyncMethod::Fsync
    }
    #[cfg(target_os = "linux")]
    {
        WalSyncMethod::Fdatasync
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        WalSyncMethod::Fsync
    }
}

/// Resolve and pin the process-wide WAL sync method. `MYDB_WAL_SYNC_METHOD`
/// always wins; otherwise a platform-correct default is chosen from the
/// durability strictness. Called once when the `Database` opens.
pub fn configure_wal_sync_method(strict: bool) {
    let method = wal_sync_method_from_env().unwrap_or_else(|| default_wal_sync_method(strict));
    WAL_SYNC_METHOD.store(method.to_u8(), Ordering::Release);
}

/// Override the pinned method at runtime (the `mydb_wal_sync_method`
/// sysvar `SET GLOBAL` path).
pub fn set_wal_sync_method(method: WalSyncMethod) {
    WAL_SYNC_METHOD.store(method.to_u8(), Ordering::Release);
}

/// The method `sync_wal_file` will use. Before `configure_wal_sync_method`
/// runs (early tools / unit tests that fsync without opening a Database)
/// it falls back to env-or-non-strict-default so those paths keep their
/// historical low cost.
pub fn resolved_wal_sync_method() -> WalSyncMethod {
    let v = WAL_SYNC_METHOD.load(Ordering::Acquire);
    if v != SYNC_METHOD_UNSET {
        return WalSyncMethod::from_u8(v);
    }
    wal_sync_method_from_env().unwrap_or_else(|| default_wal_sync_method(false))
}

/// Persist the WAL file's data pages to durable media using the
/// configured [`WalSyncMethod`]. See the enum docs for the per-platform
/// durability/latency trade-offs.
pub fn sync_wal_file(file: &std::fs::File) -> Result<(), QueryError> {
    sync_wal_file_with(file, resolved_wal_sync_method())
}

#[cfg(target_os = "linux")]
fn sync_wal_file_with(file: &std::fs::File, method: WalSyncMethod) -> Result<(), QueryError> {
    let fd = file.as_raw_fd();
    let rc = match method {
        WalSyncMethod::Nosync => return Ok(()),
        WalSyncMethod::Fdatasync => unsafe { libc::fdatasync(fd) },
        // Linux has no F_FULLFSYNC / F_BARRIERFSYNC; `fsync` already
        // issues a write barrier on mainstream filesystems, so both
        // `Fullfsync` and `Barrierfsync` map to it.
        WalSyncMethod::Fsync | WalSyncMethod::Fullfsync | WalSyncMethod::Barrierfsync => unsafe {
            libc::fsync(fd)
        },
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(io_err(std::io::Error::last_os_error()))
    }
}

#[cfg(target_os = "macos")]
fn sync_wal_file_with(file: &std::fs::File, method: WalSyncMethod) -> Result<(), QueryError> {
    #[cfg(test)]
    if method == WalSyncMethod::Fullfsync {
        if let Some(result) = crate::platform::fs::try_fixture_sync_file(file) {
            return result.map_err(io_err);
        }
    }
    let fd = file.as_raw_fd();
    let rc = match method {
        WalSyncMethod::Nosync => return Ok(()),
        // Darwin aliases `fdatasync` to `fsync`; neither flushes the
        // drive write cache.
        WalSyncMethod::Fdatasync | WalSyncMethod::Fsync => unsafe { libc::fsync(fd) },
        // The only power-loss-durable call on Darwin.
        WalSyncMethod::Fullfsync => unsafe { libc::fcntl(fd, libc::F_FULLFSYNC) },
        // I/O barrier: orders this commit ahead of later writes without
        // the full cache flush — cheaper than F_FULLFSYNC, stronger than
        // a plain `fsync` (which on Darwin does not even reach the drive).
        WalSyncMethod::Barrierfsync => unsafe { libc::fcntl(fd, libc::F_BARRIERFSYNC) },
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(io_err(std::io::Error::last_os_error()))
    }
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn sync_wal_file_with(file: &std::fs::File, method: WalSyncMethod) -> Result<(), QueryError> {
    if method == WalSyncMethod::Nosync {
        return Ok(());
    }
    let rc = unsafe { libc::fsync(file.as_raw_fd()) };
    if rc == 0 {
        Ok(())
    } else {
        Err(io_err(std::io::Error::last_os_error()))
    }
}

#[cfg(not(unix))]
fn sync_wal_file_with(file: &std::fs::File, method: WalSyncMethod) -> Result<(), QueryError> {
    if method == WalSyncMethod::Nosync {
        return Ok(());
    }
    file.sync_data().map_err(io_err)
}

/// Opt-in segment preallocation. With the segment's full byte budget
/// reserved up front, appends never grow the file, so `fdatasync`
/// doesn't have to commit inode-extent metadata and the filesystem
/// doesn't fragment the segment across many extents.
///
/// On Linux this uses `posix_fallocate`; on macOS
/// `fcntl(F_PREALLOCATE)` + `ftruncate` (both steps because
/// `F_PREALLOCATE` only reserves blocks).
///
/// Disabled by default after TPC-C 4w/20t/1m on macOS APFS showed a
/// ~30 % tpmC regression with preallocation on: APFS commits the
/// reserved-extent metadata on every `fsync`, and Darwin treats
/// `fdatasync` as an alias for `fsync` so the metadata-save can't be
/// elided. The Linux fdatasync + ext4 / xfs case is where this
/// originally lands value; set `MYDB_WAL_PREALLOCATE=1` to enable
/// (a future iteration that lifts the macOS regression can flip the
/// default back).
///
/// Failures are not fatal: a missing extent (tmpfs, NFS, ENOSPC) just
/// falls back to a sparse segment that grows on write, matching the
/// pre-overhaul behaviour. The caller is responsible for the actual
/// header write.
fn wal_preallocate_enabled() -> bool {
    static CACHED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        match std::env::var_os("MYDB_WAL_PREALLOCATE") {
            Some(v) => !matches!(
                v.to_string_lossy().to_ascii_lowercase().as_str(),
                "0" | "off" | "false" | "no" | ""
            ),
            // Platform-default split (Phase 1, WAL improvement plan): on
            // Linux ext4/xfs, preallocation is the precondition for the
            // `fdatasync` fast path (a file whose size never changes lets
            // `fdatasync` skip the inode-extent metadata journal), so it
            // is ON by default. On macOS, APFS commits the reserved-extent
            // metadata on every `fsync`/`F_FULLFSYNC`, which made TPC-C
            // regress ~30 %, so it stays OFF by default there. An explicit
            // env value overrides either way.
            None => cfg!(target_os = "linux"),
        }
    })
}

/// Operator escape valve for an otherwise un-openable WAL: when set, a
/// corrupt frame in the middle of the redo stream (bad magic / length /
/// CRC / decode / non-monotonic LSN) truncates the segment tail at that
/// frame and continues, instead of hard-failing startup with 1030. The
/// corrupt frame and every record after it are discarded — availability
/// is traded for completeness, identical in spirit to undo's
/// `MYDB_UNDO_FORCE_RECOVER`. Default off: corruption is fatal so silent
/// data loss never happens without an explicit opt-in.
pub(crate) fn wal_force_recover_enabled() -> bool {
    // Read fresh (not cached): this is only consulted on the cold
    // recovery/scan path, never in a hot loop, and a fresh read keeps the
    // flag deterministically toggleable in tests.
    crate::util::env::var_os("MYDB_WAL_FORCE_RECOVER")
        .map(|v| {
            !matches!(
                v.to_string_lossy().to_ascii_lowercase().as_str(),
                "0" | "off" | "false" | "no" | ""
            )
        })
        .unwrap_or(false)
}

#[cfg(target_os = "linux")]
fn preallocate_segment(file: &File, bytes: u64) -> std::io::Result<()> {
    if bytes == 0 || !wal_preallocate_enabled() {
        return Ok(());
    }
    let rc = unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, bytes as libc::off_t) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::from_raw_os_error(rc))
    }
}

#[cfg(target_os = "macos")]
fn preallocate_segment(file: &File, bytes: u64) -> std::io::Result<()> {
    if bytes == 0 || !wal_preallocate_enabled() {
        return Ok(());
    }
    // `fstore_t` layout matches `<sys/fcntl.h>` on macOS. We ask for a
    // contiguous range first; on failure the kernel falls back to a
    // best-effort allocation which is still useful.
    #[repr(C)]
    struct Fstore {
        fst_flags: u32,
        fst_posmode: i32,
        fst_offset: libc::off_t,
        fst_length: libc::off_t,
        fst_bytesalloc: libc::off_t,
    }
    const F_PREALLOCATE: libc::c_int = 42;
    const F_ALLOCATECONTIG: u32 = 0x00000002;
    const F_ALLOCATEALL: u32 = 0x00000004;
    const F_PEOFPOSMODE: i32 = 3;
    let fd = file.as_raw_fd();
    let mut store = Fstore {
        fst_flags: F_ALLOCATECONTIG | F_ALLOCATEALL,
        fst_posmode: F_PEOFPOSMODE,
        fst_offset: 0,
        fst_length: bytes as libc::off_t,
        fst_bytesalloc: 0,
    };
    let rc = unsafe { libc::fcntl(fd, F_PREALLOCATE, &mut store as *mut _) };
    if rc == -1 {
        // Retry without the contiguous flag; non-contiguous preallocation
        // is still a win over a sparse segment.
        store.fst_flags = F_ALLOCATEALL;
        let rc2 = unsafe { libc::fcntl(fd, F_PREALLOCATE, &mut store as *mut _) };
        if rc2 == -1 {
            return Err(std::io::Error::last_os_error());
        }
    }
    // Extend the logical file size to match the reserved range so writes
    // past the current EOF don't have to grow it again.
    let rc = unsafe { libc::ftruncate(fd, bytes as libc::off_t) };
    if rc == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn preallocate_segment(_file: &File, _bytes: u64) -> std::io::Result<()> {
    Ok(())
}

fn io_err(err: std::io::Error) -> QueryError {
    crate::util::io_error::map_io_error("WAL I/O", err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn checkpoint_end_registration_proof_round_trips_in_96_bytes() {
        let record = WalRecord {
            source_time_us: 0,
            lsn: 91,
            txn_id: 7,
            prev_txn_lsn: 42,
            payload: WalPayload::CheckpointEnd {
                redo_lsn: 11,
                next_txn_id: 123,
                next_scn: 456,
                auto_increment_covered_lsn: 10,
                sequence_covered_lsn: 10,
                gc_safe_txn_frontier: 87,
            },
        };
        let encoded = encode_record(&record).unwrap();
        assert_eq!(encoded.len(), 96);

        assert_eq!(u64::from_le_bytes(encoded[8..16].try_into().unwrap()), 91);
        assert_eq!(u64::from_le_bytes(encoded[16..24].try_into().unwrap()), 42);
        assert_eq!(u64::from_le_bytes(encoded[24..32].try_into().unwrap()), 7);
        let decoded = decode_payload(
            WalRecordType::CheckpointEnd,
            &encoded[WAL_FRAME_HEADER_LEN..],
        )
        .unwrap();
        assert!(matches!(
            decoded,
            WalPayload::CheckpointEnd {
                redo_lsn: 11,
                next_txn_id: 123,
                next_scn: 456,
                auto_increment_covered_lsn: 10,
                sequence_covered_lsn: 10,
                gc_safe_txn_frontier: 87,
            }
        ));
    }

    #[test]
    fn sequence_state_wal_roundtrip_and_strict_payload_validation() {
        for value in [i64::MIN, -1, 0, i64::MAX] {
            for called in [false, true] {
                let record = WalRecord {
                    source_time_us: 0,
                    lsn: 92,
                    txn_id: 0,
                    prev_txn_lsn: 0,
                    payload: WalPayload::SequenceState {
                        sequence_id: 7,
                        counter_id: 19,
                        resume_value: value,
                        is_called: called,
                        logged_unix_us: 123456,
                    },
                };
                let bytes = encode_record(&record).unwrap();
                assert_eq!(bytes.len(), WAL_FRAME_HEADER_LEN + 33);
                let decoded = decode_frame_for_replication(&bytes).unwrap();
                assert!(
                    matches!(decoded.payload, WalPayload::SequenceState { resume_value, is_called, logged_unix_us: 123456, .. }
                    if resume_value == value && is_called == called)
                );
                let payload = &bytes[WAL_FRAME_HEADER_LEN..];
                assert!(decode_payload(WalRecordType::SequenceState, &payload[..32]).is_err());
                let mut bad = payload.to_vec();
                bad[24] = 2;
                assert!(decode_payload(WalRecordType::SequenceState, &bad).is_err());
                let mut bad = payload.to_vec();
                bad.push(0);
                assert!(decode_payload(WalRecordType::SequenceState, &bad).is_err());
            }
        }
    }

    #[test]
    fn auto_increment_advance_v27_is_fixed_size_and_round_trips() {
        let record = WalRecord {
            source_time_us: 0,
            lsn: 92,
            txn_id: 0,
            prev_txn_lsn: 0,
            payload: WalPayload::AutoIncrementAdvance {
                table_id: 7,
                version: 3,
                max_value: 99,
            },
        };
        let encoded = encode_record(&record).unwrap();
        assert_eq!(encoded.len(), WAL_FRAME_HEADER_LEN + 24);
        assert!(matches!(
            decode_payload(
                WalRecordType::AutoIncrementAdvance,
                &encoded[WAL_FRAME_HEADER_LEN..]
            )
            .unwrap(),
            WalPayload::AutoIncrementAdvance {
                table_id: 7,
                version: 3,
                max_value: 99
            }
        ));
    }

    #[test]
    fn file_extend_v24_round_trips() {
        let payload = WalPayload::FileExtend {
            path: "schemas/test/tables/t__42.heap".to_string(),
            old_len: 16_384,
            new_len: 16_384 * 9,
        };
        let record = WalRecord {
            source_time_us: 0,
            lsn: 12,
            prev_txn_lsn: 8,
            txn_id: 3,
            payload,
        };
        let encoded = encode_record(&record).unwrap();
        let decoded =
            decode_payload(WalRecordType::FileExtend, &encoded[WAL_FRAME_HEADER_LEN..]).unwrap();
        match decoded {
            WalPayload::FileExtend {
                path,
                old_len,
                new_len,
            } => {
                assert_eq!(path, "schemas/test/tables/t__42.heap");
                assert_eq!(old_len, 16_384);
                assert_eq!(new_len, 16_384 * 9);
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn compressed_page_update_v24_round_trips_images() {
        let before = vec![0_u8; 32 * 1024];
        let mut after = before.clone();
        after[128..136].copy_from_slice(&42_u64.to_le_bytes());
        let record = WalRecord {
            source_time_us: 0,
            lsn: 15,
            prev_txn_lsn: 12,
            txn_id: 3,
            payload: WalPayload::PageUpdateCompressed {
                path: "schemas/test/indexes/t__42__PRIMARY.idx".to_string(),
                page_id: 7,
                compressed_before: lz4_flex::compress_prepend_size(&before),
                compressed_after: lz4_flex::compress_prepend_size(&after),
                undo_lsn: None,
            },
        };
        let encoded = encode_record(&record).unwrap();
        let decoded = decode_payload(
            WalRecordType::PageUpdateCompressed,
            &encoded[WAL_FRAME_HEADER_LEN..],
        )
        .unwrap();
        let (decoded_before, decoded_after) = wal_compressed_page_update_images(&decoded).unwrap();
        assert_eq!(decoded_before.as_ref(), before.as_slice());
        assert_eq!(decoded_after.as_ref(), after.as_slice());
    }

    #[test]
    fn audit14_replica_wal_fences_local_allocations() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = LogManager::open(dir.path()).unwrap();
        let next = log.next_lsn();
        log.set_physical_replica(true);
        assert!(log.reserve_lsn().is_err());
        assert!(log.append(0, WalPayload::CheckpointBegin).is_err());
        let encoded = LogManager::pre_encode_record(0, WalPayload::CheckpointBegin).unwrap();
        assert!(log.append_pre_encoded(encoded).is_err());
        assert_eq!(log.next_lsn(), next);
        log.append_reserved(next, 0, WalPayload::CheckpointBegin)
            .unwrap();
        log.set_physical_replica(false);
        assert_eq!(
            log.append(0, WalPayload::CheckpointBegin).unwrap(),
            next + 1
        );
    }

    #[test]
    fn release_audit_wal_rejects_regressed_reserved_lsn() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = LogManager::open(dir.path()).unwrap();
        let first = log.append(0, WalPayload::CheckpointBegin).unwrap();
        let next = log.reserve_lsn().unwrap();
        log.append_reserved(next, 0, WalPayload::CheckpointBegin)
            .unwrap();
        assert!(log
            .append_reserved(first, 0, WalPayload::CheckpointBegin)
            .is_err());
        assert!(log
            .append_reserved(next, 0, WalPayload::CheckpointBegin)
            .is_err());
        log.flush().unwrap();
        drop(log);
        LogManager::open(dir.path()).unwrap();
    }

    #[test]
    fn wal_v22_header_is_rejected_explicitly() {
        let dir = tempfile::tempdir().unwrap();
        let log = LogManager::open(dir.path()).unwrap();
        let path = log.path().to_path_buf();
        drop(log);

        let mut header = std::fs::read(&path).unwrap();
        header[8..12].copy_from_slice(&22_u32.to_le_bytes());
        let crc = wal_checksum(&header[..WAL_HEADER_LEN - 4]);
        header[WAL_HEADER_LEN - 4..WAL_HEADER_LEN].copy_from_slice(&crc.to_le_bytes());
        std::fs::write(&path, header).unwrap();

        let err = LogManager::scan(dir.path()).unwrap_err();
        assert_eq!(err.code(), 1030);
        assert!(err.message().contains("unsupported WAL version 22"));
    }

    #[test]
    fn txn_batch_round_trips_and_scan_expands_after_crc_validation() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = LogManager::open(dir.path()).unwrap();
        let lsn = log
            .append_txn_batch(
                41,
                vec![
                    WalPayload::UndoRecord {
                        undo_id: 9,
                        bytes: vec![1, 2, 3],
                    },
                    WalPayload::IndexBatch {
                        path: "schemas/test/indexes/t__1__k.idx".into(),
                        ops: Vec::new(),
                    },
                    WalPayload::CommitTxn {
                        commit_scn: 77,
                        commit_unix_us: 77,
                        binlog: None,
                    },
                ],
            )
            .unwrap();
        log.flush().unwrap();
        let path = log.active_info.path.clone();
        drop(log);

        let scan = scan_segment_with_id(&path, 1, true).unwrap();
        assert_eq!(scan.next_lsn, lsn + 1);
        assert_eq!(scan.records.len(), 3);
        assert_eq!(
            scan.records
                .iter()
                .map(|record| record.lsn)
                .collect::<Vec<_>>(),
            vec![lsn - 2, lsn - 1, lsn]
        );
        assert_eq!(scan.records[1].prev_txn_lsn, lsn - 2);
        assert_eq!(scan.records[2].prev_txn_lsn, lsn - 1);
        assert!(matches!(
            scan.records.last().map(|record| &record.payload),
            Some(WalPayload::CommitTxn { commit_scn: 77, .. })
        ));
    }

    #[test]
    fn txn_batch_first_frame_uses_logical_segment_start_lsn() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = LogManager::open(dir.path()).unwrap();
        log.rotate_segment().unwrap();
        let end_lsn = log
            .append_txn_batch(
                9,
                vec![
                    WalPayload::UndoRecord {
                        undo_id: 1,
                        bytes: Vec::new(),
                    },
                    WalPayload::UndoRecord {
                        undo_id: 2,
                        bytes: Vec::new(),
                    },
                    WalPayload::CommitTxn {
                        commit_scn: 3,
                        commit_unix_us: 3,
                        binlog: None,
                    },
                ],
            )
            .unwrap();
        log.flush().unwrap();
        let logical_start_lsn = end_lsn - 2;
        assert_eq!(log.active_segment_info().first_lsn, logical_start_lsn);

        let high_lsn = end_lsn;
        let log = Arc::new(parking_lot::Mutex::new(log));
        let mut reader = WalReader::open(&log, logical_start_lsn).unwrap();
        let chunk = reader.next_chunk(high_lsn, 4096).unwrap().unwrap();
        assert_eq!(chunk.start_lsn, logical_start_lsn);
        assert_eq!(chunk.end_lsn, end_lsn);
        assert_eq!(chunk.frame_count, 1);
    }

    #[test]
    fn txn_batch_rejects_missing_commit_and_nested_batch() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = LogManager::open(dir.path()).unwrap();
        assert!(log.append_txn_batch(1, vec![WalPayload::AbortTxn]).is_err());
        assert!(log
            .append_txn_batch(
                1,
                vec![
                    WalPayload::TxnBatch {
                        records: vec![WalPayload::CommitTxn {
                            commit_scn: 1,
                            commit_unix_us: 1,
                            binlog: None,
                        }],
                    },
                    WalPayload::CommitTxn {
                        commit_scn: 2,
                        commit_unix_us: 2,
                        binlog: None,
                    },
                ],
            )
            .is_err());

        let mut oversized = (0..MAX_TXN_BATCH_RECORDS)
            .map(|undo_id| WalPayload::UndoRecord {
                undo_id: undo_id as u64,
                bytes: Vec::new(),
            })
            .collect::<Vec<_>>();
        oversized.push(WalPayload::CommitTxn {
            commit_scn: 3,
            commit_unix_us: 3,
            binlog: None,
        });
        assert!(
            LogManager::pre_encode_record(1, WalPayload::TxnBatch { records: oversized }).is_err()
        );
    }

    #[test]
    fn wal_record_round_trips_page_update() {
        let record = WalRecord {
            source_time_us: 0,
            lsn: 7,
            prev_txn_lsn: 3,
            txn_id: 11,
            payload: WalPayload::PageUpdate {
                path: "tables/1.heap".into(),
                page_id: 2,
                before: vec![1, 2],
                after: vec![3, 4],
                undo_lsn: Some(6),
            },
        };
        let encoded = encode_record(&record).unwrap();
        let frame_payload = &encoded[WAL_FRAME_HEADER_LEN..];
        let decoded = decode_payload(WalRecordType::PageUpdate, frame_payload).unwrap();
        match decoded {
            WalPayload::PageUpdate {
                path,
                page_id,
                before,
                after,
                undo_lsn,
            } => {
                assert_eq!(path, "tables/1.heap");
                assert_eq!(page_id, 2);
                assert_eq!(before, vec![1, 2]);
                assert_eq!(after, vec![3, 4]);
                assert_eq!(undo_lsn, Some(6));
            }
            _ => panic!("expected page update"),
        }
    }

    #[test]
    fn sparse_page_delta_round_trips_boundaries_and_random_edits() {
        let mut random = 7u64;
        let mut next = || {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            random
        };
        let before: Vec<_> = (0..16384).map(|_| next() as u8).collect();
        for position in [0, 1, 7, 8, 15, 255, 256, 4095, 8192, 16367, 16375, 16383] {
            for length in [0, 1, 4, 8, 15, 16, 31, 64, 128] {
                let mut after = before.clone();
                let end = (position + length).min(after.len());
                for byte in &mut after[position..end] {
                    *byte ^= 0x5a;
                }
                let compressed = compress_sparse_page_delta(&before, &after).unwrap();
                let mut decoded = vec![0; before.len()];
                assert_eq!(
                    lz4_flex::block::decompress_into_with_dict(&compressed, &mut decoded, &before)
                        .unwrap(),
                    after.len()
                );
                assert_eq!(decoded, after, "position={position}, length={length}");
            }
        }
        for _ in 0..512 {
            let mut after = before.clone();
            for _ in 0..8 {
                let position = next() as usize % after.len();
                after[position] ^= 1;
            }
            let payload = WalPayload::page_delta("page".into(), 0, 1, &before, &after).unwrap();
            let WalPayload::PageDelta {
                compressed_after,
                base_crc,
                after_crc,
                ..
            } = payload
            else {
                unreachable!()
            };
            let mut decoded = vec![0; after.len()];
            lz4_flex::block::decompress_into_with_dict(&compressed_after, &mut decoded, &before)
                .unwrap();
            assert_eq!(decoded, after);
            assert_eq!(base_crc, crc32fast::hash(&before));
            assert_eq!(after_crc, crc32fast::hash(&after));
        }
        let mut moved = before.clone();
        moved.copy_within(0..16383, 1);
        assert!(compress_sparse_page_delta(&before, &moved).is_none());
        assert!(compress_sparse_page_delta(&before, &vec![0; before.len()]).is_none());
        assert!(compress_sparse_page_delta(&before[..128], &before[..128]).is_none());
    }

    #[test]
    fn page_delta_codec_round_trip_move_and_bounds() {
        let mut before = vec![0u8; 16384];
        let mut random = 7u64;
        for byte in &mut before {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            *byte = random as u8;
        }
        let mut after = before.clone();
        after.copy_within(128..16000, 129);
        after[128] = 123;
        let payload =
            WalPayload::page_delta("objects/tables/1/1.zm".into(), 0, 8, &before, &after).unwrap();
        let record = WalRecord {
            source_time_us: 0,
            lsn: 9,
            prev_txn_lsn: 0,
            txn_id: 0,
            payload,
        };
        let encoded = encode_record(&record).unwrap();
        assert_eq!(
            encoded.len(),
            WAL_FRAME_HEADER_LEN + encoded_payload_len(&record.payload).unwrap()
        );
        let decoded =
            decode_payload(WalRecordType::PageDelta, &encoded[WAL_FRAME_HEADER_LEN..]).unwrap();
        let WalPayload::PageDelta {
            compressed_after,
            base_crc,
            after_crc,
            ..
        } = decoded
        else {
            panic!("wrong delta kind")
        };
        let mut restored = vec![0; 16384];
        assert_eq!(
            lz4_flex::block::decompress_into_with_dict(&compressed_after, &mut restored, &before)
                .unwrap(),
            16384
        );
        assert_eq!(restored, after);
        assert_eq!(base_crc, crc32fast::hash(&before));
        assert_eq!(after_crc, crc32fast::hash(&after));
        assert!(
            compressed_after.len() < 256,
            "whole-page movement should reference the dictionary"
        );
        assert!(WalPayload::page_delta("x".into(), 0, 8, &[0; 16384], &before).is_none());
        let mut invalid = record.clone();
        invalid.txn_id = 1;
        assert!(encode_record(&invalid).is_err());
        for len in [0, WAL_PAGE_DELTA_MAX_BYTES + 1] {
            let mut invalid = record.clone();
            if let WalPayload::PageDelta {
                compressed_after, ..
            } = &mut invalid.payload
            {
                *compressed_after = vec![1; len];
            }
            assert!(encode_record(&invalid).is_err());
        }
        assert!(decode_payload(
            WalRecordType::PageDelta,
            &encoded[WAL_FRAME_HEADER_LEN..encoded.len() - 1]
        )
        .is_err());
    }

    #[test]
    fn zone_map_merge_batch_round_trips() {
        let record = WalRecord {
            source_time_us: 0,
            lsn: 9,
            prev_txn_lsn: 7,
            txn_id: 11,
            payload: WalPayload::ZoneMapMergeBatch {
                bytes: vec![1, 2, 3, 4],
            },
        };
        let encoded = encode_record(&record).unwrap();
        let decoded = decode_payload(
            WalRecordType::ZoneMapMergeBatch,
            &encoded[WAL_FRAME_HEADER_LEN..],
        )
        .unwrap();
        assert!(
            matches!(decoded, WalPayload::ZoneMapMergeBatch { bytes } if bytes == vec![1, 2, 3, 4])
        );
    }

    #[test]
    fn catalog_delta_round_trips() {
        let record = WalRecord {
            source_time_us: 0,
            lsn: 10,
            prev_txn_lsn: 9,
            txn_id: 12,
            payload: WalPayload::CatalogDelta {
                object_ids: vec![7, 11, 42],
                invalidate_all: false,
            },
        };
        let encoded = encode_record(&record).unwrap();
        let decoded = decode_payload(
            WalRecordType::CatalogDelta,
            &encoded[WAL_FRAME_HEADER_LEN..],
        )
        .unwrap();
        match decoded {
            WalPayload::CatalogDelta {
                object_ids,
                invalidate_all,
            } => {
                assert_eq!(object_ids, vec![7, 11, 42]);
                assert!(!invalidate_all);
            }
            _ => panic!("expected catalog delta"),
        }
    }

    #[test]
    fn ddl_commit_round_trips() {
        let record = WalRecord {
            source_time_us: 0,
            lsn: 11,
            prev_txn_lsn: 10,
            txn_id: 13,
            payload: WalPayload::DdlCommit {
                job_id: 99,
                retired_paths: vec!["objects/tables/1/3.heap".into()],
            },
        };
        let encoded = encode_record(&record).unwrap();
        let decoded =
            decode_payload(WalRecordType::DdlCommit, &encoded[WAL_FRAME_HEADER_LEN..]).unwrap();
        assert!(
            matches!(decoded, WalPayload::DdlCommit { job_id: 99, retired_paths } if retired_paths == ["objects/tables/1/3.heap"])
        );
    }

    #[test]
    fn history_index_batch_round_trips() {
        let record = WalRecord {
            source_time_us: 0,
            lsn: 10,
            prev_txn_lsn: 9,
            txn_id: 12,
            payload: WalPayload::HistoryIndexBatch {
                bytes: vec![9, 8, 7, 6],
            },
        };
        let encoded = encode_record(&record).unwrap();
        let decoded = decode_payload(
            WalRecordType::HistoryIndexBatch,
            &encoded[WAL_FRAME_HEADER_LEN..],
        )
        .unwrap();
        assert!(
            matches!(decoded, WalPayload::HistoryIndexBatch { bytes } if bytes == vec![9, 8, 7, 6])
        );
    }

    #[test]
    fn wal_record_round_trips_full_page_image() {
        let record = WalRecord {
            source_time_us: 0,
            lsn: 8,
            prev_txn_lsn: 0,
            txn_id: 0,
            payload: WalPayload::FullPageImage {
                path: "tables/1.heap".into(),
                page_id: 3,
                after: vec![5, 6, 7],
            },
        };
        let encoded = encode_record(&record).unwrap();
        let frame_payload = &encoded[WAL_FRAME_HEADER_LEN..];
        let decoded = decode_payload(WalRecordType::FullPageImage, frame_payload).unwrap();
        match decoded {
            WalPayload::FullPageImage {
                path,
                page_id,
                after,
            } => {
                assert_eq!(path, "tables/1.heap");
                assert_eq!(page_id, 3);
                assert_eq!(after, vec![5, 6, 7]);
            }
            _ => panic!("expected full page image"),
        }
    }

    #[test]
    fn moderately_compressible_fpi_retains_savings_and_round_trips() {
        let mut page = vec![0u8; 16384];
        let mut state = 0x9e3779b97f4a7c15u64;
        for byte in &mut page[..12000] {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state as u8;
        }
        let compressed = lz4_flex::compress_prepend_size(&page);
        assert!(compressed.len() > page.len() / 2);
        assert!(compressed.len() < page.len() * 7 / 8);
        let payload = WalPayload::full_page_image("objects/test.heap".into(), 4, page.clone());
        assert!(matches!(
            payload,
            WalPayload::FullPageImageCompressed { .. }
        ));
        let record = WalRecord {
            source_time_us: 0,
            lsn: 8,
            prev_txn_lsn: 0,
            txn_id: 0,
            payload,
        };
        let encoded = encode_record(&record).unwrap();
        let decoded = decode_payload(
            WalRecordType::FullPageImageCompressed,
            &encoded[WAL_FRAME_HEADER_LEN..],
        )
        .unwrap();
        assert_eq!(wal_full_page_image_after(&decoded).unwrap(), page);

        let mut incompressible = page.clone();
        for byte in &mut incompressible[12000..] {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state as u8;
        }
        let raw =
            WalPayload::full_page_image("objects/test.heap".into(), 5, incompressible.clone());
        assert!(matches!(raw, WalPayload::FullPageImage { .. }));
        assert_eq!(wal_full_page_image_after(&raw).unwrap(), incompressible);
    }

    #[test]
    fn wal_record_round_trips_compressed_full_page_image() {
        let path = "tables/1.heap".to_string();
        let mut page = vec![0u8; 16 * 1024];
        page[8..16].copy_from_slice(&42u64.to_le_bytes());
        page[4096..4104].copy_from_slice(b"mydb-fpi");
        let payload = WalPayload::full_page_image(path.clone(), 3, page.clone());
        assert!(
            matches!(payload, WalPayload::FullPageImageCompressed { .. }),
            "zero-heavy page should be compressed"
        );
        let record = WalRecord {
            source_time_us: 0,
            lsn: 8,
            prev_txn_lsn: 0,
            txn_id: 0,
            payload,
        };
        let encoded = encode_record(&record).unwrap();
        let frame_payload = &encoded[WAL_FRAME_HEADER_LEN..];
        let decoded =
            decode_payload(WalRecordType::FullPageImageCompressed, frame_payload).unwrap();
        assert_eq!(wal_full_page_image_after(&decoded).unwrap(), page);
        match decoded {
            WalPayload::FullPageImageCompressed {
                path: decoded_path,
                page_id,
                ..
            } => {
                assert_eq!(decoded_path, path);
                assert_eq!(page_id, 3);
            }
            _ => panic!("expected compressed full page image"),
        }
    }

    #[test]
    fn wal_commit_round_trips_binlog_reference() {
        let reference = WalBinlogRef {
            file: "mydb-bin.000001".into(),
            start_pos: 123,
            end_pos: 456,
            gtid: Some("00000000-0000-4000-8000-000000000042:7".into()),
        };
        let record = WalRecord {
            source_time_us: 0,
            lsn: 8,
            prev_txn_lsn: 7,
            txn_id: 11,
            payload: WalPayload::CommitTxn {
                commit_scn: 99,
                commit_unix_us: 123,
                binlog: Some(reference.clone()),
            },
        };
        let encoded = encode_record(&record).unwrap();
        let frame_payload = &encoded[WAL_FRAME_HEADER_LEN..];
        let decoded = decode_payload(WalRecordType::CommitTxn, frame_payload).unwrap();
        match decoded {
            WalPayload::CommitTxn {
                commit_scn, binlog, ..
            } => {
                assert_eq!(commit_scn, 99);
                assert_eq!(binlog, Some(reference));
            }
            _ => panic!("expected commit"),
        }
    }

    #[test]
    fn wal_record_round_trips_logical_heap_and_index_records() {
        let row_id = WalRowId {
            page_id: 2,
            slot_id: 3,
        };
        let records = vec![
            WalPayload::HeapInsert {
                path: "tables/9.heap".into(),
                page_id: 2,
                slot_id: 3,
                payload: vec![1, 2, 3],
                undo_lsn: Some(8),
            },
            WalPayload::HeapInsertBatch {
                path: "tables/9.heap".into(),
                inserts: vec![
                    WalHeapInsert {
                        page_id: 2,
                        slot_id: 3,
                        payload: vec![1, 2, 3],
                        undo_lsn: Some(8),
                    },
                    WalHeapInsert {
                        page_id: 2,
                        slot_id: 4,
                        payload: vec![4, 5, 6],
                        undo_lsn: None,
                    },
                ],
            },
            WalPayload::HeapUpdate {
                path: "tables/9.heap".into(),
                page_id: 2,
                slot_id: 3,
                before: vec![1],
                after: vec![2],
                undo_lsn: Some(8),
            },
            WalPayload::HeapDelete {
                path: "tables/9.heap".into(),
                page_id: 2,
                slot_id: 3,
                before: vec![2],
                undo_lsn: Some(8),
            },
            WalPayload::IndexInsert {
                path: "indexes/9_PRIMARY.idx".into(),
                key: vec![7],
                row_id,
            },
            WalPayload::IndexDelete {
                path: "indexes/9_PRIMARY.idx".into(),
                key: vec![7],
                row_id,
            },
            WalPayload::IndexBatch {
                path: "indexes/9_PRIMARY.idx".into(),
                ops: vec![
                    WalIndexOp {
                        kind: WalIndexOpKind::Insert,
                        key: vec![1],
                        row_id,
                    },
                    WalIndexOp {
                        kind: WalIndexOpKind::Delete,
                        key: vec![2],
                        row_id,
                    },
                ],
            },
        ];

        for payload in records {
            let record = WalRecord {
                source_time_us: 0,
                lsn: 7,
                prev_txn_lsn: 3,
                txn_id: 11,
                payload,
            };
            let encoded = encode_record(&record).unwrap();
            let frame_payload = &encoded[WAL_FRAME_HEADER_LEN..];
            let decoded = decode_payload(record.payload.record_type(), frame_payload).unwrap();
            assert_eq!(decoded.record_type(), record.payload.record_type());
        }
    }

    #[test]
    fn wal_scan_truncates_partial_tail() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path();
        let mut log = LogManager::open(data_dir).unwrap();
        log.append(1, WalPayload::BeginTxn { start_scn: 1 })
            .unwrap();
        log.flush().unwrap();
        let path = log.path().to_path_buf();
        drop(log);
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(b"partial").unwrap();
        }
        let scan = scan_segment(&path, true).unwrap();
        assert_eq!(scan.records.len(), 1);
        let scan = scan_segment(&path, false).unwrap();
        assert_eq!(scan.records.len(), 1);
    }

    #[test]
    fn txn_last_lsn_map_stays_bounded_via_terminal_records() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = LogManager::open(dir.path()).unwrap();

        // Committed txn: BeginTxn ... CommitTxn (terminal) → entry dropped.
        log.append(1, WalPayload::BeginTxn { start_scn: 1 })
            .unwrap();
        assert!(log.txn_last_lsn.contains_key(&1));
        log.append(
            1,
            WalPayload::CommitTxn {
                commit_scn: 2,
                commit_unix_us: 2,
                binlog: None,
            },
        )
        .unwrap();
        assert!(
            !log.txn_last_lsn.contains_key(&1),
            "CommitTxn is terminal and must drop the back-link entry"
        );

        // Rolled-back txn: BeginTxn, AbortTxn (NOT terminal — CLRs may follow),
        // then EndTxn (terminal) → entry dropped.
        log.append(2, WalPayload::BeginTxn { start_scn: 3 })
            .unwrap();
        log.append(2, WalPayload::AbortTxn).unwrap();
        assert!(
            log.txn_last_lsn.contains_key(&2),
            "AbortTxn is not terminal; the entry must survive for trailing CLRs"
        );
        log.append(2, WalPayload::EndTxn).unwrap();
        assert!(
            !log.txn_last_lsn.contains_key(&2),
            "EndTxn is terminal and must drop the back-link entry"
        );

        // Many finished txns must not accumulate — the whole point of the fix.
        for t in 100..1100u64 {
            log.append(t, WalPayload::BeginTxn { start_scn: t })
                .unwrap();
            log.append(
                t,
                WalPayload::CommitTxn {
                    commit_scn: t,
                    commit_unix_us: t,
                    binlog: None,
                },
            )
            .unwrap();
        }
        assert!(
            log.txn_last_lsn.is_empty(),
            "terminated txns leaked: map retained {} entries",
            log.txn_last_lsn.len()
        );
    }

    #[test]
    fn wal_stats_are_kept_without_retaining_records() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path();
        let mut log = LogManager::open(data_dir).unwrap();
        let first_lsn = log
            .append(1, WalPayload::BeginTxn { start_scn: 1 })
            .unwrap();
        log.append(
            1,
            WalPayload::HeapInsertBatch {
                path: "tables/1.heap".into(),
                inserts: vec![WalHeapInsert {
                    page_id: 0,
                    slot_id: 0,
                    payload: vec![1, 2, 3],
                    undo_lsn: None,
                }],
            },
        )
        .unwrap();
        let commit_lsn = log
            .append(
                1,
                WalPayload::CommitTxn {
                    commit_scn: 2,
                    commit_unix_us: 2,
                    binlog: None,
                },
            )
            .unwrap();

        let stats = log.stats();
        assert_eq!(stats.logical_records, 1);
        assert_eq!(log.next_lsn(), commit_lsn + 1);

        // Drive the unlocked-fsync path: wrap the LogManager in a mutex
        // momentarily so we can exercise the same code production
        // commits go through. Requesting a sync to `first_lsn` fsyncs the
        // whole WAL file, so durability advances to the flushed high-water
        // (the last appended LSN, = `commit_lsn`), not just `first_lsn` —
        // this is the M1 high-water optimization that lets one fsync cover
        // every already-appended record.
        let log_arc = Arc::new(parking_lot::Mutex::new(log));
        LogManager::sync_durable_to(&log_arc, first_lsn).unwrap();
        assert_eq!(log_arc.lock().durable_lsn(), commit_lsn);
    }

    #[test]
    fn concurrent_durability_requests_share_one_fsync() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = LogManager::open(dir.path()).unwrap();
        let lsn = log
            .append(1, WalPayload::BeginTxn { start_scn: 1 })
            .unwrap();
        let log = Arc::new(parking_lot::Mutex::new(log));

        // Queue every caller behind the same gate before allowing a leader to
        // fsync. The first caller advances durable_lsn; all followers must
        // observe that high-water and return without another syscall.
        let gate = Arc::clone(&log.lock().sync_gate);
        let held = gate.lock();
        let callers = 8;
        let ready = Arc::new(std::sync::Barrier::new(callers + 1));
        let handles = (0..callers)
            .map(|_| {
                let log = Arc::clone(&log);
                let ready = Arc::clone(&ready);
                std::thread::spawn(move || {
                    ready.wait();
                    LogManager::sync_durable_to(&log, lsn)
                })
            })
            .collect::<Vec<_>>();
        ready.wait();
        drop(held);

        for handle in handles {
            handle.join().unwrap().unwrap();
        }
        let log = log.lock();
        assert!(log.durable_lsn() >= lsn);
        assert_eq!(log.sync_count(), 1);
    }

    #[test]
    fn durability_barrier_keeps_wal_append_available() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let mut manager = LogManager::open(dir.path()).unwrap();
        let first = manager
            .append(1, WalPayload::BeginTxn { start_scn: 1 })
            .unwrap();
        let log = Arc::new(parking_lot::Mutex::new(manager));
        let gate = Arc::clone(&log.lock().sync_gate);
        let held = gate.lock();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let worker_log = Arc::clone(&log);
        let worker = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let result = LogManager::sync_durable_high_water_to(&worker_log, first);
            done_tx.send(result).unwrap();
        });
        started_rx.recv().unwrap();
        let early = done_rx.recv_timeout(Duration::from_millis(100));
        // Waiting on the I/O gate must leave the append mutex available.
        let last = log
            .lock()
            .append(2, WalPayload::BeginTxn { start_scn: 2 })
            .unwrap();
        drop(held);
        if early.is_err() {
            done_rx
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap();
        }
        worker.join().unwrap();
        assert!(
            matches!(early, Err(std::sync::mpsc::RecvTimeoutError::Timeout)),
            "durability request bypassed the shared I/O gate"
        );
        assert_eq!(log.lock().durable_lsn(), last);
        assert_eq!(log.lock().sync_count(), 1);
    }

    #[test]
    fn already_durable_request_does_not_wait_for_unrelated_sync() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let mut manager = LogManager::open(dir.path()).unwrap();
        let lsn = manager
            .append(1, WalPayload::BeginTxn { start_scn: 1 })
            .unwrap();
        let log = Arc::new(parking_lot::Mutex::new(manager));
        LogManager::sync_durable_to(&log, lsn).unwrap();
        let gate = Arc::clone(&log.lock().sync_gate);
        let held = gate.lock();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let worker_log = Arc::clone(&log);
        let worker = std::thread::spawn(move || {
            done_tx
                .send(LogManager::sync_durable_high_water_to(&worker_log, lsn))
                .unwrap();
        });
        let result = done_rx.recv_timeout(Duration::from_secs(1));
        drop(held);
        worker.join().unwrap();
        assert_eq!(result.unwrap().unwrap(), (lsn, false));
        assert_eq!(log.lock().sync_count(), 1);
    }

    #[test]
    fn wal_scan_rejects_checksum_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path();
        let mut log = LogManager::open(data_dir).unwrap();
        log.append(1, WalPayload::BeginTxn { start_scn: 1 })
            .unwrap();
        log.flush().unwrap();
        let path = log.path().to_path_buf();
        drop(log);

        {
            let mut file = OpenOptions::new().write(true).open(&path).unwrap();
            file.seek(SeekFrom::Start(
                WAL_HEADER_LEN as u64 + WAL_FRAME_HEADER_LEN as u64,
            ))
            .unwrap();
            file.write_all(&[0xff]).unwrap();
            file.sync_data().unwrap();
        }

        let err = scan_segment(&path, false).unwrap_err();
        assert_eq!(err.code(), 1030);
        assert!(err.message().contains("frame checksum mismatch"));
        assert!(err.message().contains(&path.display().to_string()));
        assert!(err.message().contains("offset"));
        assert!(err.message().contains("MYDB_WAL_FORCE_RECOVER=1"));
        assert!(err.message().contains("may lose data"));
    }

    #[test]
    fn fresh_audit_wal_crc_protects_transaction_header_fields() {
        for pre_encoded in [false, true] {
            for offset in [8, 16, 24, 32, 34] {
                let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
                let mut log = LogManager::open(dir.path()).unwrap();
                let payload = WalPayload::BeginTxn { start_scn: 1 };
                if pre_encoded {
                    log.append_pre_encoded(LogManager::pre_encode_record(7, payload).unwrap())
                        .unwrap();
                } else {
                    log.append(7, payload).unwrap();
                }
                log.append(8, WalPayload::BeginTxn { start_scn: 2 })
                    .unwrap();
                log.flush().unwrap();
                let path = log.path().to_path_buf();
                drop(log);
                let mut bytes = fs::read(&path).unwrap();
                let frame = &bytes[WAL_HEADER_LEN..WAL_HEADER_LEN + WAL_FRAME_HEADER_LEN + 8];
                decode_frame_for_replication(frame).unwrap();
                bytes[WAL_HEADER_LEN + offset] ^= 1;
                let frame = &bytes[WAL_HEADER_LEN..WAL_HEADER_LEN + WAL_FRAME_HEADER_LEN + 8];
                assert!(decode_frame_for_replication(frame)
                    .unwrap_err()
                    .message()
                    .contains("checksum"));
                fs::write(&path, &bytes).unwrap();
                assert!(scan_segment(&path, true)
                    .unwrap_err()
                    .message()
                    .contains("checksum"));
            }
        }
    }

    #[test]
    fn wal_scan_rejects_non_monotonic_lsn() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path();
        let mut log = LogManager::open(data_dir).unwrap();
        log.append(1, WalPayload::BeginTxn { start_scn: 1 })
            .unwrap();
        log.append(
            1,
            WalPayload::CommitTxn {
                commit_scn: 1,
                commit_unix_us: 1,
                binlog: None,
            },
        )
        .unwrap();
        log.flush().unwrap();
        let path = log.path().to_path_buf();
        drop(log);

        {
            let mut bytes = fs::read(&path).unwrap();
            let start = WAL_HEADER_LEN + WAL_FRAME_HEADER_LEN + 8;
            bytes[start + 8..start + 16].copy_from_slice(&1_u64.to_le_bytes());
            let payload_len = read_u32(&bytes[start..], 4) as usize;
            // Keep the frame CRC valid so this exercises the structural LSN
            // guard independently of the header-corruption checksum guard.
            let checksum = wal_frame_checksum(
                &bytes[start..],
                &bytes[start + WAL_FRAME_HEADER_LEN..start + WAL_FRAME_HEADER_LEN + payload_len],
            );
            bytes[start + 44..start + 48].copy_from_slice(&checksum.to_le_bytes());
            fs::write(&path, bytes).unwrap();
        }

        let err = scan_segment(&path, false).unwrap_err();
        assert_eq!(err.code(), 1030);
        assert!(
            err.message().contains("non-monotonic LSN"),
            "unexpected error: {}",
            err.message()
        );
    }

    #[test]
    fn wal_record_round_trips_prepare_txn() {
        let xid = crate::xa::Xid::new(42, b"gtx-1".to_vec(), b"branch".to_vec()).unwrap();
        let locks = vec![
            PreparedLockEntry::Row {
                table_id: 5,
                page_id: 7,
                slot_id: 13,
                mode: 1,
            },
            PreparedLockEntry::UniqueKey {
                table_id: 5,
                index_name: "uq_email".into(),
                key_bytes: vec![1, 2, 3, 4],
            },
            PreparedLockEntry::Table {
                table_id: 5,
                mode: 1,
            },
        ];
        let record = WalRecord {
            source_time_us: 0,
            lsn: 100,
            prev_txn_lsn: 99,
            txn_id: 17,
            payload: WalPayload::PrepareTxn {
                xid: xid.clone(),
                prepare_scn: 250,
                binlog: Some(PreparedBinlogRef {
                    chunks: 2,
                    bytes: 1048580,
                    crc32: 1234,
                }),
                locks: locks.clone(),
                mdl_locks: vec![PreparedMdlEntry {
                    object_type: 2,
                    schema: Some("test".into()),
                    name: Some("t".into()),
                    lock_type: 1,
                }],
            },
        };
        let encoded = encode_record(&record).unwrap();
        let frame_payload = &encoded[WAL_FRAME_HEADER_LEN..];
        let decoded = decode_payload(WalRecordType::PrepareTxn, frame_payload).unwrap();
        let WalPayload::PrepareTxn {
            xid: decoded_xid,
            prepare_scn,
            locks: decoded_locks,
            mdl_locks,
            binlog,
        } = decoded
        else {
            panic!("expected PrepareTxn");
        };
        assert_eq!(decoded_xid, xid);
        assert_eq!(prepare_scn, 250);
        assert_eq!(
            binlog,
            Some(PreparedBinlogRef {
                chunks: 2,
                bytes: 1048580,
                crc32: 1234
            })
        );
        assert_eq!(decoded_locks, locks);
        assert_eq!(
            mdl_locks,
            vec![PreparedMdlEntry {
                object_type: 2,
                schema: Some("test".into()),
                name: Some("t".into()),
                lock_type: 1,
            }]
        );
    }

    // ===================== Phase 1 — multi-segment + reader =====================

    fn open_log_with_segment_cap(dir: &Path, max_segment_bytes: u64) -> LogManager {
        let mut log = LogManager::open(dir).unwrap();
        log.set_max_segment_bytes(max_segment_bytes);
        log
    }

    fn append_filler(log: &mut LogManager, txn_id: u64, n: usize) -> Vec<u64> {
        let mut lsns = Vec::with_capacity(n);
        for _ in 0..n {
            let payload = WalPayload::HeapInsertBatch {
                path: "tables/1.heap".into(),
                inserts: vec![WalHeapInsert {
                    page_id: 1,
                    slot_id: 0, // ~256B payload per record so the size threshold is easy to cross.
                    payload: vec![0xab; 256],
                    undo_lsn: None,
                }],
            };
            let lsn = log.append(txn_id, payload).unwrap();
            lsns.push(lsn);
        }
        lsns
    }

    fn large_heap_batch(fill: u8) -> WalPayload {
        WalPayload::HeapInsertBatch {
            path: "tables/1.heap".into(),
            inserts: vec![WalHeapInsert {
                page_id: 1,
                slot_id: 0,
                payload: vec![fill; DEFAULT_WAL_WRITE_BUFFER_BYTES + 4096],
                undo_lsn: None,
            }],
        }
    }

    #[test]
    fn recovery_borrowed_scan_preserves_segments_and_transaction_chain() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let mut log = open_log_with_segment_cap(dir.path(), 4096);
        log.append(7, WalPayload::BeginTxn { start_scn: 1 })
            .unwrap();
        let lsns = append_filler(&mut log, 7, 40);
        log.flush().unwrap();
        drop(log);
        let scan = LogManager::scan(dir.path()).unwrap();
        let segments =
            discover_segments(&crate::storage::naming::system_wal_dir(dir.path())).unwrap();
        assert!(segments.len() > 1);
        for (id, path) in segments {
            let full = scan_segment_with_id(&path, id, false).unwrap();
            let metadata = inspect_segment(&path, id).unwrap();
            assert_eq!(metadata.first_lsn, full.records.first().unwrap().lsn);
            assert_eq!(metadata.last_lsn, full.records.last().unwrap().lsn);
        }
        let mut reopened = LogManager::open_from_scan_ref(dir.path(), &scan).unwrap();
        assert_eq!(reopened.next_lsn, scan.next_lsn);
        assert_eq!(reopened.txn_last_lsn.get(&7), lsns.last());
        let end = reopened.append(7, WalPayload::EndTxn).unwrap();
        assert_eq!(end, scan.next_lsn);
        drop(reopened);
        // The writer borrows the scan only during construction; the retained
        // recovery records remain usable and the appended chain is correct.
        let after = LogManager::scan(dir.path()).unwrap();
        assert_eq!(after.records.len(), scan.records.len() + 1);
        assert_eq!(
            after.records.last().unwrap().prev_txn_lsn,
            *lsns.last().unwrap()
        );
    }

    #[test]
    fn configurable_write_buffer_resize_preserves_replay() {
        for pre_encoded in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut log = LogManager::open(dir.path()).unwrap();
            log.set_write_buffer_enabled(true).unwrap();
            log.set_max_segment_bytes(4096);
            let mut lsns = Vec::new();
            for limit in [1024, 0, 48, 49, 64, 8192, 128] {
                log.set_write_buffer_bytes(limit).unwrap();
                assert_eq!(log.write_buffer_bytes(), limit);
                assert!(log.pending_write_bytes.is_empty());
                for txn in 1..=80 {
                    let payload = WalPayload::BeginTxn { start_scn: txn };
                    let lsn = if pre_encoded {
                        let record = LogManager::pre_encode_record(txn, payload).unwrap();
                        log.append_pre_encoded(record).unwrap()
                    } else {
                        log.append(txn, payload).unwrap()
                    };
                    lsns.push(lsn);
                    assert!(log.pending_write_bytes.len() < limit.max(1) as usize);
                    if txn == 1 {
                        assert_eq!(
                            log.pending_write_bytes.is_empty(),
                            limit <= (WAL_FRAME_HEADER_LEN + 8) as u64
                        );
                    }
                    assert_eq!(
                        log.pending_write_charge.bytes(),
                        log.pending_write_bytes.capacity()
                    );
                }
            }
            let before = log.write_buffer_bytes();
            assert!(log
                .set_write_buffer_bytes(MAX_WAL_WRITE_BUFFER_BYTES + 1)
                .is_err());
            assert_eq!(log.write_buffer_bytes(), before);
            log.flush().unwrap();
            drop(log);
            let scan = LogManager::scan(dir.path()).unwrap();
            assert_eq!(scan.records.iter().map(|r| r.lsn).collect::<Vec<_>>(), lsns);
        }
    }

    #[test]
    fn configurable_write_buffer_environment() {
        for (raw, expected) in [("0", 0), ("4K", 4096), ("1M", 1048576)] {
            crate::util::env::with_vars(&[("MYDB_WAL_WRITE_BUFFER_BYTES", Some(raw))], || {
                let dir = tempfile::tempdir().unwrap();
                let log = LogManager::open(dir.path()).unwrap();
                assert_eq!(log.write_buffer_bytes(), expected);
                assert!(log.pending_write_bytes.capacity() >= expected as usize);
            });
        }
        for raw in ["-1", "2G", "invalid", "18446744073709551615T"] {
            crate::util::env::with_vars(&[("MYDB_WAL_WRITE_BUFFER_BYTES", Some(raw))], || {
                let dir = tempfile::tempdir().unwrap();
                assert!(LogManager::open(dir.path()).is_err());
            });
        }
    }

    #[test]
    fn large_frames_bypass_pending_buffer_and_reuse_scratch_until_commit() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = LogManager::open(dir.path()).unwrap();
        log.set_write_buffer_enabled(true).unwrap();

        let begin_lsn = log
            .append(7, WalPayload::BeginTxn { start_scn: 1 })
            .unwrap();
        assert!(!log.pending_write_bytes.is_empty());

        let first_large_lsn = log.append(7, large_heap_batch(0xa5)).unwrap();
        assert!(log.pending_write_bytes.is_empty());
        assert_eq!(log.large_scratch_txn, Some(7));
        let retained_capacity = log.encode_scratch.capacity();
        assert!(retained_capacity >= DEFAULT_WAL_WRITE_BUFFER_BYTES);
        assert_eq!(log.encode_scratch_charge.bytes(), retained_capacity);

        let second_large_lsn = log.append(7, large_heap_batch(0x5a)).unwrap();
        assert!(log.pending_write_bytes.is_empty());
        assert_eq!(log.large_scratch_txn, Some(7));
        assert_eq!(log.encode_scratch.capacity(), retained_capacity);
        assert_eq!(log.encode_scratch_charge.bytes(), retained_capacity);

        let commit_lsn = log
            .append(
                7,
                WalPayload::CommitTxn {
                    commit_scn: 2,
                    commit_unix_us: 2,
                    binlog: None,
                },
            )
            .unwrap();
        assert_eq!(log.large_scratch_txn, None);
        assert!(log.encode_scratch.capacity() <= WAL_ENCODE_SCRATCH_RETAIN_LIMIT);
        assert_eq!(
            log.encode_scratch_charge.bytes(),
            log.encode_scratch.capacity()
        );
        assert_eq!(
            log.pending_write_charge.bytes(),
            log.pending_write_bytes.capacity()
        );

        log.flush().unwrap();
        drop(log);
        let scan = LogManager::scan(dir.path()).unwrap();
        let lsns = scan
            .records
            .iter()
            .map(|record| record.lsn)
            .collect::<Vec<_>>();
        assert_eq!(
            lsns,
            vec![begin_lsn, first_large_lsn, second_large_lsn, commit_lsn]
        );
    }

    #[test]
    fn large_direct_frame_rotates_as_one_crc_checked_record() {
        let dir = tempfile::tempdir().unwrap();
        let mut log =
            open_log_with_segment_cap(dir.path(), DEFAULT_WAL_WRITE_BUFFER_BYTES as u64 / 2);
        log.set_write_buffer_enabled(true).unwrap();
        let first = log
            .append(9, WalPayload::BeginTxn { start_scn: 1 })
            .unwrap();
        let large = log.append(9, large_heap_batch(0xcc)).unwrap();
        let commit = log
            .append(
                9,
                WalPayload::CommitTxn {
                    commit_scn: 3,
                    commit_unix_us: 3,
                    binlog: None,
                },
            )
            .unwrap();
        log.flush().unwrap();
        assert!(log.segment_count() >= 2);
        drop(log);

        let scan = LogManager::scan(dir.path()).unwrap();
        assert_eq!(
            scan.records
                .iter()
                .map(|record| record.lsn)
                .collect::<Vec<_>>(),
            vec![first, large, commit]
        );
        assert!(matches!(
            scan.records[1].payload,
            WalPayload::HeapInsertBatch { .. }
        ));
    }

    #[test]
    fn preencoded_large_frames_reuse_governed_buffer_until_terminal_record() {
        WAL_RECORD_BUF_POOL.with(|pool| pool.borrow_mut().clear());
        let dir = tempfile::tempdir().unwrap();
        let mut log = LogManager::open(dir.path()).unwrap();
        log.set_write_buffer_enabled(true).unwrap();

        let first = LogManager::pre_encode_record(17, large_heap_batch(0x11)).unwrap();
        let first_ptr = first.bytes.as_ptr();
        let first_capacity = first.bytes.capacity();
        assert_eq!(first.bytes_charge.as_ref().unwrap().bytes(), first_capacity);
        log.append_pre_encoded(first).unwrap();

        let second = LogManager::pre_encode_record(17, large_heap_batch(0x22)).unwrap();
        assert_eq!(second.bytes.as_ptr(), first_ptr);
        assert_eq!(second.bytes.capacity(), first_capacity);
        assert_eq!(
            second.bytes_charge.as_ref().unwrap().bytes(),
            first_capacity
        );
        log.append_pre_encoded(second).unwrap();

        let terminal = LogManager::pre_encode_record(
            17,
            WalPayload::CommitTxn {
                commit_scn: 2,
                commit_unix_us: 2,
                binlog: None,
            },
        )
        .unwrap();
        assert_eq!(terminal.bytes.as_ptr(), first_ptr);
        assert_eq!(terminal.bytes.capacity(), first_capacity);
        log.append_pre_encoded(terminal).unwrap();
        WAL_RECORD_BUF_POOL.with(|pool| {
            assert!(pool
                .borrow()
                .iter()
                .all(|buffer| buffer.bytes.capacity() <= WAL_RECORD_BUF_KEEP_CAP));
        });
    }

    fn assert_log_state_atomics_match(log: &LogManager) {
        let atomics = log.state_atomics();
        assert_eq!(atomics.next_lsn(), log.next_lsn(), "next_lsn mirror");
        assert_eq!(atomics.wal_bytes(), log.wal_bytes(), "wal_bytes mirror");
    }

    #[test]
    fn log_state_atomics_track_open_append_rotate_prune_checkpoint_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = open_log_with_segment_cap(dir.path(), 2048);
        assert_log_state_atomics_match(&log);

        append_filler(&mut log, 1, 30);
        log.flush().unwrap();
        assert!(
            log.segment_count() >= 3,
            "test must trigger multiple segments"
        );
        assert_log_state_atomics_match(&log);

        let snap_before = log.segment_snapshot();
        let cut = snap_before[1].first_lsn;
        assert!(log.prune_segments_below_lsn(cut).unwrap() > 0);
        assert_log_state_atomics_match(&log);

        let checkpoint_lsn = log.next_lsn();
        log.compact_to_checkpoint(checkpoint_lsn, 50, 100).unwrap();
        assert_log_state_atomics_match(&log);

        drop(log);
        let reopened = LogManager::open(dir.path()).unwrap();
        assert_log_state_atomics_match(&reopened);
    }

    #[test]
    fn btree_release_failed_rotation_cannot_publish_active_as_reclaimable() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let mut log = LogManager::open(dir.path()).unwrap();
        let begin = log
            .append(7, WalPayload::BeginTxn { start_scn: 1 })
            .unwrap();
        log.flush().unwrap();
        let pending =
            segment_path(&log.dir, log.active_segment_id() + 1).with_extension("log.pending");
        fs::create_dir(&pending).unwrap();
        assert!(log.rotate_segment().is_err());
        assert!(
            log.segments.is_empty(),
            "a failed rotation must not make the active WAL reclaimable"
        );
        assert!(log.pending_sealed_sync.is_empty());
        fs::remove_dir(&pending).unwrap();
        log.rotate_segment().unwrap();
        let end = log.append(7, WalPayload::AbortTxn).unwrap();
        log.flush().unwrap();
        let scan = LogManager::scan(dir.path()).unwrap();
        assert_eq!(
            scan.records.iter().map(|r| r.lsn).collect::<Vec<_>>(),
            vec![begin, end]
        );
        assert_eq!(scan.records[1].prev_txn_lsn, begin);
    }

    #[test]
    fn active_segment_rotates_when_size_threshold_crossed() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = open_log_with_segment_cap(dir.path(), 4096);
        assert_eq!(log.segment_count(), 1);
        assert_eq!(log.active_segment_id(), 1);

        append_filler(&mut log, 1, 20);
        log.flush().unwrap();

        // We should have rotated at least once.
        assert!(log.active_segment_id() >= 2);
        assert!(log.segment_count() >= 2);
        // Sealed segments must precede the active segment.
        let snap = log.segment_snapshot();
        for (i, seg) in snap.iter().enumerate() {
            if i + 1 < snap.len() {
                assert!(
                    seg.last_lsn < snap[i + 1].first_lsn,
                    "segments must not overlap: {:?}",
                    snap
                );
            }
        }
        // Active info must agree.
        let active = log.active_segment_info();
        assert_eq!(active.segment_id, log.active_segment_id());
        assert_eq!(active.status, WalSegmentStatus::Active);
    }

    #[test]
    fn durability_barrier_syncs_sealed_segments_before_new_active() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = open_log_with_segment_cap(dir.path(), 2048);
        append_filler(&mut log, 1, 12);
        assert!(
            !log.pending_sealed_sync.is_empty(),
            "small segment cap must leave an asynchronously sealed predecessor"
        );
        // Ensure the new active file also contains a record, so the barrier
        // has to return both the sealed predecessor(s) and the active fd.
        let target = log
            .append(
                2,
                WalPayload::CommitTxn {
                    commit_scn: 7,
                    commit_unix_us: 7,
                    binlog: None,
                },
            )
            .unwrap();
        let (files, high_water) = log.prepare_sync_to(target).unwrap().unwrap();
        assert!(
            files.len() >= 2,
            "barrier must preserve cross-segment order"
        );
        for file in &files {
            sync_wal_file(file).unwrap();
        }
        log.finish_sync_to(high_water, 0);
        assert!(log.durable_lsn() >= target);
        assert!(log.pending_sealed_sync.is_empty());
    }

    #[test]
    fn rotation_preserves_lsn_continuity_across_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = open_log_with_segment_cap(dir.path(), 2048);
        let mut lsns = Vec::new();
        // Use HeapInsertBatch with ~256B payload to force rotation under
        // the 2048-byte cap.
        for _ in 0..30 {
            let lsn = log
                .append(
                    1,
                    WalPayload::HeapInsertBatch {
                        path: "tables/1.heap".into(),
                        inserts: vec![WalHeapInsert {
                            page_id: 1,
                            slot_id: 0,
                            payload: vec![0xcd; 256],
                            undo_lsn: None,
                        }],
                    },
                )
                .unwrap();
            lsns.push(lsn);
        }
        log.flush().unwrap();
        for window in lsns.windows(2) {
            assert_eq!(window[1], window[0] + 1, "LSNs must be densely consecutive");
        }
        assert!(log.segment_count() >= 2, "should have rotated");

        // Re-open: scan must surface all records and re-establish the same next_lsn.
        let next_lsn_before = log.next_lsn();
        drop(log);
        let scan = LogManager::scan(dir.path()).unwrap();
        assert_eq!(scan.records.len(), 30);
        assert_eq!(scan.next_lsn, next_lsn_before);
        let log2 = LogManager::open(dir.path()).unwrap();
        assert_eq!(log2.next_lsn(), next_lsn_before);
        // Discovered all segments.
        assert!(log2.segment_count() >= 2);
    }

    fn prep_filler_payload() -> WalPayload {
        WalPayload::HeapInsertBatch {
            path: "tables/1.heap".into(),
            inserts: vec![WalHeapInsert {
                page_id: 1,
                slot_id: 0,
                payload: vec![0xab; 256],
                undo_lsn: None,
            }],
        }
    }

    #[test]
    fn prepared_segment_rotation_promotes_and_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = crate::storage::naming::system_wal_dir(dir.path());
        let cap = 2048u64;
        let log = Arc::new(parking_lot::Mutex::new(open_log_with_segment_cap(
            dir.path(),
            cap,
        )));

        // Fill the active segment past the half-full prep threshold but short
        // of the rotation point.
        let mut appended = 0usize;
        loop {
            let (size, id) = {
                let g = log.lock();
                (g.active_segment_info().size_bytes, g.active_segment_id())
            };
            assert_eq!(id, 1, "must not rotate before the prep step");
            if size >= cap / 2 {
                break;
            }
            log.lock().append(1, prep_filler_payload()).unwrap();
            appended += 1;
            assert!(appended < 1000, "did not reach half-full threshold");
        }

        // Prepare the next segment off-lock: it appears in-memory and on disk
        // under the temp name that `discover_segments` ignores.
        LogManager::ensure_next_segment(&log).unwrap();
        assert_eq!(log.lock().prepared_segment_id(), Some(2));
        let tmp = wal_dir.join(WAL_PREP_TMP_NAME);
        assert!(tmp.exists(), "prepared temp segment file must exist");

        // Keep appending until a rotation fires; it must consume the prepared
        // segment (fast path) rather than create a new one synchronously.
        loop {
            log.lock().append(1, prep_filler_payload()).unwrap();
            appended += 1;
            if log.lock().active_segment_id() >= 2 {
                break;
            }
            assert!(appended < 1000, "rotation never triggered");
        }
        {
            let g = log.lock();
            assert_eq!(g.active_segment_id(), 2);
            assert_eq!(
                g.prepared_segment_id(),
                None,
                "rotation must have consumed the prepared segment"
            );
        }
        assert!(!tmp.exists(), "temp file must be renamed away on promotion");
        assert!(
            segment_path(&wal_dir, 2)
                .with_extension("log.pending")
                .exists(),
            "rotation stages the successor until its prefix is durable"
        );

        // Records survive a reopen across the prepared-then-rotated boundary.
        let next_before = log.lock().next_lsn();
        log.lock().flush().unwrap();
        drop(log);
        let scan = LogManager::scan(dir.path()).unwrap();
        assert_eq!(scan.records.len(), appended, "all records recovered");
        assert_eq!(scan.next_lsn, next_before);
        let reopened = LogManager::open(dir.path()).unwrap();
        assert_eq!(reopened.next_lsn(), next_before);
        assert!(reopened.segment_count() >= 2);
    }

    #[test]
    fn dense_commits_promote_zero_filled_segment_early() {
        // Exercise the policy explicitly on every supported local platform;
        // macOS defaults to reservation-only allocation.
        std::env::set_var("MYDB_WAL_ZERO_FILL", "1");
        std::env::set_var("MYDB_WAL_PREALLOCATE", "1");
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = crate::storage::naming::system_wal_dir(dir.path());
        let cap = 1024 * 1024u64;
        let log = Arc::new(parking_lot::Mutex::new(open_log_with_segment_cap(
            dir.path(),
            cap,
        )));
        // Treat every reservation-only sync as slow (cloud block storage).
        log.lock().zero_fill_min_sync_us = 0;
        // OLTP-shaped traffic: one durability sync per small commit, past the
        // early-replacement threshold of a reservation-only active segment.
        let mut appended = 0usize;
        while log.lock().active_segment_info().size_bytes < cap / 16 {
            let lsn = log.lock().append(1, prep_filler_payload()).unwrap();
            LogManager::sync_durable_to(&log, lsn).unwrap();
            appended += 1;
        }
        assert_eq!(log.lock().next_segment_fill(), SegmentFill::Zero);
        LogManager::ensure_next_segment(&log).unwrap();
        {
            let g = log.lock();
            assert_eq!(g.active_segment_id(), 2, "zero-filled successor promoted");
            assert_eq!(g.active_fill, SegmentFill::Zero);
            assert_eq!(g.prepared_segment_id(), None);
        }
        assert!(!wal_dir.join(WAL_PREP_TMP_NAME).exists());
        let promoted = segment_path(&wal_dir, 2).with_extension("log.pending");
        let bytes = fs::read(&promoted).unwrap();
        assert_eq!(bytes.len() as u64, cap, "whole segment budget is written");
        assert!(bytes[WAL_HEADER_LEN..].iter().all(|&b| b == 0));

        // A zero-filled active segment is only rotated at its size threshold.
        let lsn = log.lock().append(1, prep_filler_payload()).unwrap();
        LogManager::sync_durable_to(&log, lsn).unwrap();
        appended += 1;
        LogManager::ensure_next_segment(&log).unwrap();
        assert_eq!(log.lock().active_segment_id(), 2);

        let next_before = log.lock().next_lsn();
        log.lock().flush().unwrap();
        drop(log);
        let scan = LogManager::scan(dir.path()).unwrap();
        assert_eq!(
            scan.records.len(),
            appended,
            "zero tail ends the scan cleanly"
        );
        assert_eq!(scan.next_lsn, next_before);
        let reopened = LogManager::open(dir.path()).unwrap();
        assert_eq!(reopened.next_lsn(), next_before);
        assert_eq!(reopened.active_segment_id(), 2);
    }

    #[test]
    fn dense_commits_on_fast_storage_keep_reservation_only_segments() {
        let dir = tempfile::tempdir().unwrap();
        let cap = 1024 * 1024u64;
        let log = Arc::new(parking_lot::Mutex::new(open_log_with_segment_cap(
            dir.path(),
            cap,
        )));
        // No measured reservation-only sync latency is slow enough to repay
        // zero filling (local NVMe).
        log.lock().zero_fill_min_sync_us = u64::MAX;
        while log.lock().active_segment_info().size_bytes < cap / 2 {
            let lsn = log.lock().append(1, prep_filler_payload()).unwrap();
            LogManager::sync_durable_to(&log, lsn).unwrap();
        }
        assert!(log.lock().observed_reserve_sync_us().is_some());
        assert_eq!(log.lock().next_segment_fill(), SegmentFill::Reserve);
        LogManager::ensure_next_segment(&log).unwrap();
        let g = log.lock();
        assert_eq!(g.active_segment_id(), 1, "no early promotion");
        assert_eq!(
            g.next_segment.as_ref().map(|prep| prep.fill),
            Some(SegmentFill::Reserve)
        );
    }

    #[test]
    fn segment_fill_accounts_for_sync_density_and_latency() {
        if !wal_zero_fill_enabled() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let mut log = open_log_with_segment_cap(dir.path(), 1024 * 1024);
        let syncs = ZERO_FILL_MIN_SAMPLE_SYNCS;
        log.active_fill = SegmentFill::Reserve;
        log.fill_sample = (0, 0, 0);
        log.sync_count = syncs;
        for (bytes_per_sync, sync_us, expected) in [
            (2048, 147, SegmentFill::Zero),
            (ZERO_FILL_SMALL_BYTES_PER_SYNC, 147, SegmentFill::Zero),
            (2048, 11, SegmentFill::Reserve),
            (2048, 12, SegmentFill::Zero),
            (2048, 30, SegmentFill::Zero),
            (ZERO_FILL_SMALL_BYTES_PER_SYNC + 1, 99, SegmentFill::Reserve),
            (ZERO_FILL_SMALL_BYTES_PER_SYNC + 1, 100, SegmentFill::Zero),
            (64 * 1024, 600, SegmentFill::Zero),
            (ZERO_FILL_MAX_BYTES_PER_SYNC + 1, 600, SegmentFill::Reserve),
        ] {
            log.stats.physical_write_bytes = bytes_per_sync * syncs;
            log.stats.sync_total_us = sync_us * syncs;
            assert_eq!(
                log.next_segment_fill(),
                expected,
                "bytes_per_sync={bytes_per_sync}, sync_us={sync_us}",
            );
        }

        // Neither an idle poll nor a short, unrepresentative sample may
        // trigger the extra background I/O.
        log.stats.physical_write_bytes = 2048 * syncs;
        log.stats.sync_total_us = 147 * syncs;
        log.sync_count = syncs - 1;
        assert_eq!(log.next_segment_fill(), SegmentFill::Reserve);
        log.sync_count = 0;
        assert_eq!(log.next_segment_fill(), SegmentFill::Reserve);
    }

    #[test]
    fn bulk_writes_keep_reservation_only_segments() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = crate::storage::naming::system_wal_dir(dir.path());
        let cap = 4 * 1024 * 1024u64;
        let log = Arc::new(parking_lot::Mutex::new(open_log_with_segment_cap(
            dir.path(),
            cap,
        )));
        let payload = || WalPayload::HeapInsertBatch {
            path: "tables/1.heap".into(),
            inserts: vec![WalHeapInsert {
                page_id: 1,
                slot_id: 0,
                payload: vec![0xcd; 64 * 1024],
                undo_lsn: None,
            }],
        };
        // Load-shaped traffic: one sync per ~1 MiB of WAL.
        while log.lock().active_segment_info().size_bytes < cap / 2 {
            let mut lsn = 0;
            for _ in 0..16 {
                lsn = log.lock().append(1, payload()).unwrap();
            }
            LogManager::sync_durable_to(&log, lsn).unwrap();
        }
        assert_eq!(log.lock().next_segment_fill(), SegmentFill::Reserve);
        LogManager::ensure_next_segment(&log).unwrap();
        let g = log.lock();
        assert_eq!(
            g.active_segment_id(),
            1,
            "no early promotion for bulk writes"
        );
        assert_eq!(g.prepared_segment_id(), Some(2));
        assert_eq!(
            g.next_segment.as_ref().map(|prep| prep.fill),
            Some(SegmentFill::Reserve)
        );
        assert!(wal_dir.join(WAL_PREP_TMP_NAME).exists());
    }

    #[test]
    fn prepared_segment_is_reclaimed_if_never_promoted() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = crate::storage::naming::system_wal_dir(dir.path());
        let cap = 2048u64;
        let log = Arc::new(parking_lot::Mutex::new(open_log_with_segment_cap(
            dir.path(),
            cap,
        )));

        let mut appended = 0usize;
        while log.lock().active_segment_info().size_bytes < cap / 2 {
            log.lock().append(1, prep_filler_payload()).unwrap();
            appended += 1;
            assert!(appended < 1000);
        }
        LogManager::ensure_next_segment(&log).unwrap();
        let tmp = wal_dir.join(WAL_PREP_TMP_NAME);
        assert!(tmp.exists(), "prepared temp segment file must exist");
        let next_before = log.lock().next_lsn();
        log.lock().flush().unwrap();
        // Simulate a crash before the prepared segment is ever promoted: drop
        // the log with the temp file still on disk.
        drop(log);
        assert!(tmp.exists(), "temp file still present pre-reopen");

        // The scan ignores the stray temp file; open reclaims it and recovers
        // every committed record unaffected.
        let scan = LogManager::scan(dir.path()).unwrap();
        assert_eq!(scan.records.len(), appended);
        assert_eq!(scan.next_lsn, next_before);
        let reopened = LogManager::open(dir.path()).unwrap();
        assert!(!tmp.exists(), "stray prepared temp reclaimed on open");
        assert_eq!(reopened.next_lsn(), next_before);
        assert_eq!(reopened.active_segment_id(), 1, "no phantom rotation");
    }

    #[test]
    fn wal_reader_streams_across_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = open_log_with_segment_cap(dir.path(), 4096);
        let lsns = append_filler(&mut log, 7, 40);
        log.flush().unwrap();
        let last_lsn = *lsns.last().unwrap();
        assert!(log.segment_count() >= 2, "test must trigger rotation");

        let log = Arc::new(parking_lot::Mutex::new(log));
        let mut reader = WalReader::open(&log, lsns[0]).expect("reader open");
        let mut seen = Vec::new();
        loop {
            let chunk = reader.next_chunk(last_lsn, 1024).expect("chunk");
            match chunk {
                Some(c) => {
                    assert!(c.frame_count >= 1);
                    assert!(c.start_lsn >= lsns[0]);
                    assert!(c.end_lsn <= last_lsn);
                    seen.push((c.start_lsn, c.end_lsn, c.frame_count));
                }
                None => break,
            }
        }
        let total_frames: u32 = seen.iter().map(|(_, _, n)| *n).sum();
        assert_eq!(total_frames as usize, lsns.len());
        // First chunk starts at our start_lsn; last chunk ends at last_lsn.
        assert_eq!(seen.first().unwrap().0, lsns[0]);
        assert_eq!(seen.last().unwrap().1, last_lsn);
    }

    #[test]
    fn wal_reader_caps_at_safe_high_lsn() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = open_log_with_segment_cap(dir.path(), 4096);
        let lsns = append_filler(&mut log, 1, 30);
        log.flush().unwrap();

        let cap = lsns[10];
        let log = Arc::new(parking_lot::Mutex::new(log));
        let mut reader = WalReader::open(&log, lsns[0]).unwrap();
        let mut yielded_max_lsn = 0;
        while let Some(c) = reader.next_chunk(cap, 4096).unwrap() {
            yielded_max_lsn = yielded_max_lsn.max(c.end_lsn);
        }
        assert_eq!(yielded_max_lsn, cap);
        // After hitting the cap, the cursor moved past the cap by 1.
        assert_eq!(reader.cursor_lsn(), cap + 1);
        // Further read with same cap returns None.
        assert!(reader.next_chunk(cap, 4096).unwrap().is_none());
        // Bumping the cap surfaces the rest.
        let next_lsn = log.lock().next_lsn();
        let mut yielded2 = 0;
        while let Some(c) = reader.next_chunk(next_lsn - 1, 4096).unwrap() {
            yielded2 = yielded2.max(c.end_lsn);
        }
        assert_eq!(yielded2, *lsns.last().unwrap());
    }

    #[test]
    fn wal_reader_oldest_lsn_after_checkpoint_reopen_is_an_existing_record() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = open_log_with_segment_cap(dir.path(), 4096);
        let lsns = append_filler(&mut log, 1, 20);
        let checkpoint = lsns.last().unwrap() + 1;
        log.compact_to_checkpoint(checkpoint, 100, 200).unwrap();
        drop(log);
        let reopened = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        assert_eq!(reopened.lock().min_available_lsn(), checkpoint);
        let oldest = reopened.lock().min_available_lsn();
        let mut reader = WalReader::open(&reopened, oldest).unwrap();
        let chunk = reader.next_chunk(checkpoint, 4096).unwrap().unwrap();
        assert_eq!(chunk.start_lsn, checkpoint);
    }

    #[test]
    fn wal_reader_rejects_truncated_request() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = open_log_with_segment_cap(dir.path(), 4096);
        let lsns = append_filler(&mut log, 1, 20);
        log.flush().unwrap();
        // Trigger a checkpoint compact (will prune all sealed segments and
        // collapse the active to header + a CheckpointEnd record).
        let last_lsn = *lsns.last().unwrap();
        log.compact_to_checkpoint(last_lsn + 1, 100, 200).unwrap();
        let log = Arc::new(parking_lot::Mutex::new(log));

        // Asking for the original first LSN must error as Truncated.
        let err = WalReader::open(&log, lsns[0]).unwrap_err();
        match err {
            WalReaderError::Truncated {
                requested,
                min_available,
            } => {
                assert_eq!(requested, lsns[0]);
                assert!(min_available > lsns[0]);
            }
            other => panic!("expected Truncated, got {other:?}"),
        }
    }

    #[test]
    fn wal_reader_rejects_future_lsn() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = open_log_with_segment_cap(dir.path(), 16 * 1024);
        let lsns = append_filler(&mut log, 1, 4);
        let next = log.next_lsn();
        let log = Arc::new(parking_lot::Mutex::new(log));
        // Beyond next_lsn should fail.
        let err = WalReader::open(&log, next + 5).unwrap_err();
        assert!(matches!(err, WalReaderError::FutureLsn { .. }));
        // Equal to next_lsn is fine (no data yet, returns None on next_chunk).
        let mut reader = WalReader::open(&log, next).unwrap();
        assert!(reader.next_chunk(next, 1024).unwrap().is_none());
        let _ = lsns;
    }

    #[test]
    fn checkpoint_compaction_prunes_all_sealed_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = open_log_with_segment_cap(dir.path(), 4096);
        append_filler(&mut log, 1, 40);
        log.flush().unwrap();
        assert!(log.segment_count() >= 2);
        let next = log.next_lsn();
        log.compact_to_checkpoint(next, 50, 100).unwrap();
        // After compaction: zero sealed segments, single CheckpointEnd record.
        assert_eq!(log.segment_count(), 1);
        let scan = LogManager::scan(dir.path()).unwrap();
        assert_eq!(scan.records.len(), 1);
        assert!(matches!(
            scan.records[0].payload,
            WalPayload::CheckpointEnd { .. }
        ));
    }

    #[test]
    fn fuzzy_checkpoint_does_not_collapse_a_small_active_segment() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = open_log_with_segment_cap(dir.path(), 64 * 1024);
        append_filler(&mut log, 1, 3);
        let end_lsn = log
            .append(
                0,
                WalPayload::CheckpointEnd {
                    redo_lsn: 1,
                    next_txn_id: 10,
                    next_scn: 20,
                    auto_increment_covered_lsn: 1,
                    sequence_covered_lsn: 1,
                    gc_safe_txn_frontier: 0,
                },
            )
            .unwrap();
        log.flush().unwrap();
        let active_id = log.active_segment_id();
        let active_size = log.active_segment_info().size_bytes;

        log.compact_to_checkpoint_with_min_required(end_lsn, 10, 20, end_lsn, false)
            .unwrap();

        assert_eq!(log.active_segment_id(), active_id);
        assert_eq!(log.active_segment_info().size_bytes, active_size);
        let scan = LogManager::scan(dir.path()).unwrap();
        assert_eq!(scan.records.len(), 4);
    }

    #[test]
    fn checkpoint_cleanup_unlocked_preserves_concurrent_rotation_and_retry() {
        let dir = tempfile::tempdir().unwrap();
        let mut manager = open_log_with_segment_cap(dir.path(), 2048);
        append_filler(&mut manager, 1, 30);
        manager.flush().unwrap();
        let floor = manager.active_segment_info().first_lsn;
        let log = Arc::new(LogMutex::new(manager));
        let before = log.lock().wal_bytes();
        set_wal_cleanup_failpoint(2);
        assert!(LogManager::prune_checkpoint_segments(&log, floor).is_err());
        set_wal_cleanup_failpoint(0);
        assert_eq!(log.lock().wal_bytes(), before);
        let mut new_ids = Vec::new();
        LogManager::prune_checkpoint_segments_with(&log, floor, || {
            let mut guard = log
                .try_lock()
                .expect("cleanup must release the commit mutex");
            append_filler(&mut guard, 2, 30);
            guard.flush().unwrap();
            new_ids = guard
                .segment_snapshot()
                .iter()
                .filter(|seg| seg.last_lsn >= floor)
                .map(|seg| seg.segment_id)
                .collect();
        })
        .unwrap();
        let guard = log.lock();
        assert_eq!(
            guard
                .segment_snapshot()
                .iter()
                .map(|seg| seg.segment_id)
                .collect::<Vec<_>>(),
            new_ids
        );
        assert_eq!(
            guard.wal_bytes(),
            guard
                .segment_snapshot()
                .iter()
                .map(|seg| seg.size_bytes)
                .sum::<u64>()
        );
        drop(guard);
        assert!(!LogManager::scan(dir.path()).unwrap().records.is_empty());
    }

    #[test]
    fn checkpoint_archive_directory_failure_retries_missing_source() {
        let dir = tempfile::tempdir().unwrap();
        let mut manager = open_log_with_segment_cap(dir.path(), 2048);
        append_filler(&mut manager, 1, 30);
        manager.flush().unwrap();
        manager.archive.enable(dir.path().join("archive")).unwrap();
        let floor = manager.active_segment_info().first_lsn;
        let log = Arc::new(LogMutex::new(manager));
        set_wal_cleanup_failpoint(4);
        assert!(LogManager::prune_checkpoint_segments(&log, floor).is_err());
        set_wal_cleanup_failpoint(0);
        let archived_bytes = log.lock().archive.archived_bytes;
        assert!(archived_bytes > 0);
        assert!(LogManager::prune_checkpoint_segments(&log, floor).unwrap() > 0);
        assert_eq!(log.lock().segment_count(), 1);
        assert_eq!(log.lock().archive.archived_bytes, archived_bytes);
    }

    #[test]
    fn prune_archive_failure_retains_segment_and_wal_byte_accounting() {
        let dir = tempfile::tempdir().unwrap();
        let archive_dir = dir.path().join("archive");
        let mut log = open_log_with_segment_cap(dir.path(), 2048);
        append_filler(&mut log, 1, 30);
        log.flush().unwrap();
        log.archive.enable(archive_dir).unwrap();
        let before = log.segment_snapshot();
        let bytes_before = log.wal_bytes();
        let cut = log.active_segment_info().first_lsn;

        set_wal_cleanup_failpoint(1);
        let result = log.prune_segments_below_lsn(cut);
        set_wal_cleanup_failpoint(0);

        assert!(result.is_err());
        assert_eq!(
            log.segment_snapshot()
                .iter()
                .map(|seg| seg.segment_id)
                .collect::<Vec<_>>(),
            before.iter().map(|seg| seg.segment_id).collect::<Vec<_>>()
        );
        assert_eq!(log.wal_bytes(), bytes_before);
    }

    #[test]
    fn prune_remove_failure_is_retryable_without_false_reclaim() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = open_log_with_segment_cap(dir.path(), 2048);
        append_filler(&mut log, 1, 30);
        log.flush().unwrap();
        let before = log.segment_snapshot();
        let bytes_before = log.wal_bytes();
        let cut = log.active_segment_info().first_lsn;

        set_wal_cleanup_failpoint(2);
        let result = log.prune_segments_below_lsn(cut);
        set_wal_cleanup_failpoint(0);

        assert!(result.is_err());
        assert_eq!(
            log.segment_snapshot()
                .iter()
                .map(|seg| seg.segment_id)
                .collect::<Vec<_>>(),
            before.iter().map(|seg| seg.segment_id).collect::<Vec<_>>()
        );
        assert_eq!(log.wal_bytes(), bytes_before);
        assert!(log.prune_segments_below_lsn(cut).unwrap() > 0);
        assert!(log.wal_bytes() < bytes_before);
    }

    #[test]
    fn prune_directory_sync_failure_keeps_retry_bookkeeping() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = open_log_with_segment_cap(dir.path(), 2048);
        append_filler(&mut log, 1, 30);
        log.flush().unwrap();
        let segment_count_before = log.segment_count();
        let bytes_before = log.wal_bytes();
        let cut = log.active_segment_info().first_lsn;

        set_wal_cleanup_failpoint(4);
        let result = log.prune_segments_below_lsn(cut);
        set_wal_cleanup_failpoint(0);

        assert!(result.is_err());
        assert_eq!(log.segment_count(), segment_count_before);
        assert_eq!(log.wal_bytes(), bytes_before);
        assert!(log.prune_segments_below_lsn(cut).unwrap() > 0);
        assert_eq!(log.segment_count(), 1);
    }

    #[test]
    fn checkpoint_rename_and_directory_sync_failures_preserve_recoverable_wal() {
        for stage in [3_u8, 5_u8] {
            let dir = tempfile::tempdir().unwrap();
            let mut log = open_log_with_segment_cap(dir.path(), 4096);
            let lsns = append_filler(&mut log, 1, 20);
            log.flush().unwrap();
            let checkpoint_lsn = lsns.last().copied().unwrap().saturating_add(1);

            set_wal_cleanup_failpoint(stage);
            let result = log.compact_to_checkpoint(checkpoint_lsn, 50, 100);
            set_wal_cleanup_failpoint(0);

            assert!(result.is_err(), "stage {stage} must fail");
            let scan = LogManager::scan(dir.path()).unwrap();
            assert!(LogManager::open(dir.path()).is_ok());
            if stage == 3 {
                assert!(scan.records.iter().any(|record| record.lsn == lsns[0]));
            } else {
                assert!(
                    matches!(scan.records.first().unwrap().payload, WalPayload::CheckpointEnd { redo_lsn, .. } if redo_lsn == checkpoint_lsn)
                );
            }
        }
    }

    #[test]
    fn prune_segments_below_lsn_keeps_segments_at_or_above_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = open_log_with_segment_cap(dir.path(), 2048);
        let lsns = append_filler(&mut log, 1, 30);
        log.flush().unwrap();
        let snap_before = log.segment_snapshot();
        assert!(snap_before.len() >= 3, "need at least 3 segments");
        let cut = snap_before[1].first_lsn;
        let removed = log.prune_segments_below_lsn(cut).unwrap();
        assert!(removed > 0);
        let snap_after = log.segment_snapshot();
        assert!(snap_after.first().unwrap().last_lsn >= cut);
        // Active segment is intact.
        assert_eq!(
            snap_after.last().unwrap().segment_id,
            log.active_segment_id()
        );
        let _ = lsns;
    }

    #[tokio::test]
    async fn wal_reader_wait_for_new_data_wakes_on_append() {
        let dir = tempfile::tempdir().unwrap();
        let log = LogManager::open(dir.path()).unwrap();
        let log = Arc::new(parking_lot::Mutex::new(log));
        let next = log.lock().next_lsn();
        let mut reader = WalReader::open(&log, next).unwrap();

        let log_clone = Arc::clone(&log);
        let writer = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            let mut guard = log_clone.lock();
            guard
                .append(1, WalPayload::BeginTxn { start_scn: 11 })
                .unwrap();
        });

        let lsn = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            reader.wait_for_new_data(),
        )
        .await
        .expect("wait should not hang")
        .expect("watch live");
        assert!(lsn >= next);
        writer.await.unwrap();
    }

    #[test]
    fn heap_update_logical_round_trips_multiple_patches() {
        let record = WalRecord {
            source_time_us: 0,
            lsn: 42,
            prev_txn_lsn: 30,
            txn_id: 7,
            payload: WalPayload::HeapUpdateLogical {
                path: "schemas/test/tables/bench__99__data.heap".into(),
                page_id: 5,
                slot_id: 2,
                patches: vec![
                    WalColumnPatch {
                        offset: 8,
                        before: vec![1, 0, 0, 0, 0, 0, 0, 0],
                        after: vec![2, 0, 0, 0, 0, 0, 0, 0],
                    },
                    WalColumnPatch {
                        offset: 24,
                        before: vec![10, 0],
                        after: vec![20, 0],
                    },
                ],
                undo_lsn: Some(11),
            },
        };
        let encoded = encode_record(&record).unwrap();
        let frame_payload = &encoded[WAL_FRAME_HEADER_LEN..];
        let decoded = decode_payload(WalRecordType::HeapUpdateLogical, frame_payload).unwrap();
        match decoded {
            WalPayload::HeapUpdateLogical {
                path,
                page_id,
                slot_id,
                patches,
                undo_lsn,
            } => {
                assert_eq!(page_id, 5);
                assert_eq!(slot_id, 2);
                assert_eq!(path, "schemas/test/tables/bench__99__data.heap");
                assert_eq!(patches.len(), 2);
                assert_eq!(patches[0].offset, 8);
                assert_eq!(patches[0].before, vec![1, 0, 0, 0, 0, 0, 0, 0]);
                assert_eq!(patches[0].after, vec![2, 0, 0, 0, 0, 0, 0, 0]);
                assert_eq!(patches[1].offset, 24);
                assert_eq!(patches[1].before, vec![10, 0]);
                assert_eq!(patches[1].after, vec![20, 0]);
                assert_eq!(undo_lsn, Some(11));
            }
            _ => panic!("expected HeapUpdateLogical"),
        }
    }

    #[test]
    fn heap_update_logical_is_smaller_than_full_heap_update() {
        // The whole point of the new record: a 1-col INT8 UPDATE of a
        // 200B row writes a small logical patch vs the full-row
        // before+after. WAL frame overhead (frame header + path
        // string + page coordinates) is fixed across the two records;
        // the per-row savings dominate once the row is wide enough.
        let path = "schemas/test/tables/bench__99__data.heap";
        let full_before = vec![0u8; 200];
        let full_after = vec![1u8; 200];
        let full = WalRecord {
            source_time_us: 0,
            lsn: 1,
            prev_txn_lsn: 0,
            txn_id: 7,
            payload: WalPayload::HeapUpdate {
                path: path.into(),
                page_id: 1,
                slot_id: 0,
                before: full_before,
                after: full_after,
                undo_lsn: None,
            },
        };
        let logical = WalRecord {
            source_time_us: 0,
            lsn: 2,
            prev_txn_lsn: 0,
            txn_id: 7,
            payload: WalPayload::HeapUpdateLogical {
                path: path.into(),
                page_id: 1,
                slot_id: 0,
                patches: vec![WalColumnPatch {
                    offset: 16,
                    before: vec![0u8; 8],
                    after: vec![1u8; 8],
                }],
                undo_lsn: None,
            },
        };
        let full_size = encode_record(&full).unwrap().len();
        let logical_size = encode_record(&logical).unwrap().len();
        // For a 200B row updating 1 INT8 column, the logical record
        // is roughly 30% the size of the full record. Assert at
        // least 50% smaller — the conservative bound that still
        // proves the format is saving real bytes.
        assert!(
            logical_size * 2 < full_size,
            "logical {}B should be << full {}B",
            logical_size,
            full_size
        );
    }

    #[test]
    fn full_page_image_is_after_only_page_payload() {
        let path = "schemas/test/tables/t__1__data.heap";
        let before_after = WalRecord {
            source_time_us: 0,
            lsn: 1,
            prev_txn_lsn: 0,
            txn_id: 1,
            payload: WalPayload::PageUpdate {
                path: path.into(),
                page_id: 1,
                before: vec![0u8; 16 * 1024],
                after: vec![1u8; 16 * 1024],
                undo_lsn: None,
            },
        };
        let fpi = WalRecord {
            source_time_us: 0,
            lsn: 2,
            prev_txn_lsn: 0,
            txn_id: 0,
            payload: WalPayload::FullPageImage {
                path: path.into(),
                page_id: 1,
                after: vec![1u8; 16 * 1024],
            },
        };
        let before_after_len = encode_record(&before_after).unwrap().len();
        let fpi_len = encode_record(&fpi).unwrap().len();
        assert!(
            before_after_len - fpi_len >= 16 * 1024,
            "FPI {}B should save a full page vs PageUpdate {}B",
            fpi_len,
            before_after_len
        );
    }

    /// v13→v14 (rowid-6bytes) record-shape check: every heap-write
    /// record drops the per-record 2-byte `generation` field. Verify
    /// the on-wire byte count matches the new fixed-field budget
    /// exactly (so a future drift back to gen-carrying records trips
    /// this test). HeapInsertBatch's per-row head shrinks 12 → 10 B.
    #[test]
    fn heap_records_page_id_field_is_four_bytes() {
        let path = "schemas/test/tables/t__1__data.heap";
        let path_enc = WAL_FRAME_HEADER_LEN + 4 + path.len(); // length prefix + bytes
                                                              // Bytes::encode_bytes layout: u32 len + body.
        let bytes_overhead = |body_len: usize| 4 + body_len;

        // HeapInsert: path + page_id(4) + slot(2) + bytes(payload) + undo_lsn(8).
        let insert = WalRecord {
            source_time_us: 0,
            lsn: 1,
            prev_txn_lsn: 0,
            txn_id: 1,
            payload: WalPayload::HeapInsert {
                path: path.into(),
                page_id: 1,
                slot_id: 0,
                payload: vec![0u8; 64],
                undo_lsn: Some(7),
            },
        };
        let expect_insert = path_enc + 4 + 2 + bytes_overhead(64) + 8;
        assert_eq!(
            encode_record(&insert).unwrap().len(),
            expect_insert,
            "HeapInsert size should be path + 4 (page_id u32) + 2 (slot u16) + payload + 8 undo_lsn"
        );

        // HeapUpdate
        let update = WalRecord {
            source_time_us: 0,
            lsn: 2,
            prev_txn_lsn: 0,
            txn_id: 1,
            payload: WalPayload::HeapUpdate {
                path: path.into(),
                page_id: 1,
                slot_id: 0,
                before: vec![0u8; 64],
                after: vec![1u8; 64],
                undo_lsn: Some(7),
            },
        };
        let expect_update = path_enc + 4 + 2 + bytes_overhead(64) + bytes_overhead(64) + 8;
        assert_eq!(encode_record(&update).unwrap().len(), expect_update);

        // HeapDelete
        let delete = WalRecord {
            source_time_us: 0,
            lsn: 3,
            prev_txn_lsn: 0,
            txn_id: 1,
            payload: WalPayload::HeapDelete {
                path: path.into(),
                page_id: 1,
                slot_id: 0,
                before: vec![0u8; 64],
                undo_lsn: Some(7),
            },
        };
        let expect_delete = path_enc + 4 + 2 + bytes_overhead(64) + 8;
        assert_eq!(encode_record(&delete).unwrap().len(), expect_delete);

        // HeapInsertBatch: per-row head is 4 (page_id) + 2 (slot) +
        // 4 (payload-len prefix) = 10 B, then payload bytes, then 8 B
        // undo_lsn. (The path is encoded once at the start, plus the
        // row-count u32.) Was 12 B per row before rowid-6bytes.
        let batch_rows = 1000usize;
        let row_payload = vec![0u8; 64];
        let batch = WalRecord {
            source_time_us: 0,
            lsn: 4,
            prev_txn_lsn: 0,
            txn_id: 1,
            payload: WalPayload::HeapInsertBatch {
                path: path.into(),
                inserts: (0..batch_rows)
                    .map(|i| WalHeapInsert {
                        page_id: i as u32,
                        slot_id: (i % 100) as u16,
                        payload: row_payload.clone(),
                        undo_lsn: None,
                    })
                    .collect(),
            },
        };
        let expect_batch_payload = path_enc + 4 // row count u32
            + batch_rows * (10 + row_payload.len() + 8);
        assert_eq!(encode_record(&batch).unwrap().len(), expect_batch_payload);

        // Reference what the previous v13 (gen-carrying) batch would
        // have cost: per-row head 12 instead of 10. The delta of 2 B
        // per row is the on-wire saving from the rowid-6bytes refactor.
        let v13_equivalent = path_enc + 4 + batch_rows * (12 + row_payload.len() + 8);
        let saving = v13_equivalent - expect_batch_payload;
        assert_eq!(saving, batch_rows * 2);
    }

    // ===================== Phase 1 — durability + escape valve =====================

    #[test]
    fn wal_sync_method_parses_and_defaults() {
        assert_eq!(
            WalSyncMethod::parse("fdatasync"),
            Some(WalSyncMethod::Fdatasync)
        );
        assert_eq!(WalSyncMethod::parse("FSYNC"), Some(WalSyncMethod::Fsync));
        assert_eq!(
            WalSyncMethod::parse(" fullfsync "),
            Some(WalSyncMethod::Fullfsync)
        );
        assert_eq!(
            WalSyncMethod::parse("barrierfsync"),
            Some(WalSyncMethod::Barrierfsync)
        );
        assert_eq!(
            WalSyncMethod::parse("barrier"),
            Some(WalSyncMethod::Barrierfsync)
        );
        assert_eq!(WalSyncMethod::parse("nosync"), Some(WalSyncMethod::Nosync));
        assert_eq!(WalSyncMethod::parse("bogus"), None);
        // Round-trip through the u8 wire used by the global atomic.
        for m in [
            WalSyncMethod::Fdatasync,
            WalSyncMethod::Fsync,
            WalSyncMethod::Fullfsync,
            WalSyncMethod::Barrierfsync,
            WalSyncMethod::Nosync,
        ] {
            assert_eq!(WalSyncMethod::from_u8(m.to_u8()), m);
        }
        // Platform-correct default matches the peer-DB convention: macOS
        // defaults to plain `fsync` (MySQL/PostgreSQL leave F_FULLFSYNC an
        // opt-in), Linux to the metadata-light `fdatasync`. Strictness no
        // longer forces F_FULLFSYNC — operators opt in explicitly.
        let strict = default_wal_sync_method(true);
        if cfg!(target_os = "macos") {
            assert_eq!(strict, WalSyncMethod::Fsync);
        } else if cfg!(target_os = "linux") {
            assert_eq!(strict, WalSyncMethod::Fdatasync);
        }
        // F_FULLFSYNC remains reachable as an explicit opt-in.
        assert_eq!(
            WalSyncMethod::parse("fullfsync"),
            Some(WalSyncMethod::Fullfsync)
        );
        // A real flush with whatever method is resolved must succeed.
        let dir = tempfile::tempdir().unwrap();
        let mut log = LogManager::open(dir.path()).unwrap();
        let lsn = log
            .append(1, WalPayload::BeginTxn { start_scn: 1 })
            .unwrap();
        let (files, high_water) = log.prepare_sync_to(lsn).unwrap().unwrap();
        assert!(high_water >= lsn);
        for file in &files {
            sync_wal_file(file).unwrap();
        }
    }

    /// Corrupt a frame in the MIDDLE of a sealed segment (not the tail).
    /// Default scan must hard-fail 1030; with `MYDB_WAL_FORCE_RECOVER`
    /// set it must truncate at the bad frame and return the clean prefix.
    #[test]
    fn force_recover_truncates_corrupt_mid_stream_frame() {
        let dir = tempfile::tempdir().unwrap();
        let seg_path = {
            let mut log = LogManager::open(dir.path()).unwrap();
            // Disable the write buffer so frames land on disk directly,
            // then append a handful of records and capture their offsets.
            let lsns = append_filler(&mut log, 1, 8);
            log.flush().unwrap();
            assert_eq!(lsns.len(), 8);
            log.path().to_path_buf()
        };

        // Re-scan the clean segment to find the byte offset of, say, the
        // 5th frame, then flip a payload byte there to break its CRC.
        let clean = scan_segment_with_id(&seg_path, 1, false).unwrap();
        assert_eq!(clean.records.len(), 8);
        let target_lsn = clean.records[4].lsn;

        // Walk frames to compute the offset of the target frame.
        let mut offset = WAL_HEADER_LEN as u64;
        let mut bytes = std::fs::read(&seg_path).unwrap();
        let mut target_off = None;
        let mut idx = 0usize;
        while (offset as usize) + WAL_FRAME_HEADER_LEN <= bytes.len() {
            let header = &bytes[offset as usize..offset as usize + WAL_FRAME_HEADER_LEN];
            if header.iter().all(|&b| b == 0) {
                break;
            }
            let payload_len = read_u32(header, 4) as u64;
            if idx == 4 {
                target_off = Some(offset);
                break;
            }
            offset += WAL_FRAME_HEADER_LEN as u64 + payload_len;
            idx += 1;
        }
        let target_off = target_off.expect("found target frame") as usize;
        // Flip one byte in the payload (past the 40B frame header) to
        // corrupt the CRC of exactly this frame.
        let corrupt_byte = target_off + WAL_FRAME_HEADER_LEN + 1;
        bytes[corrupt_byte] ^= 0xff;
        std::fs::write(&seg_path, &bytes).unwrap();

        // Default: sealed-segment scan (truncate_tail=false) hard-fails.
        std::env::remove_var("MYDB_WAL_FORCE_RECOVER");
        let err = scan_segment_with_id(&seg_path, 1, false).unwrap_err();
        assert_eq!(err.code(), 1030);

        // Active segments must also reject mid-stream corruption without
        // modifying the source; an active segment is not evidence of a tail tear.
        let err = scan_segment_with_id(&seg_path, 1, true).unwrap_err();
        assert!(err.message().contains("later frame detected"));
        assert_eq!(std::fs::read(&seg_path).unwrap(), bytes);

        // Force-recover: truncate at the bad frame, keep the clean prefix.
        std::env::set_var("MYDB_WAL_FORCE_RECOVER", "1");
        let recovered = scan_segment_with_id(&seg_path, 1, false).unwrap();
        std::env::remove_var("MYDB_WAL_FORCE_RECOVER");
        // The first 4 records (before the corrupt 5th) survive; the
        // corrupt frame and everything after are discarded.
        assert_eq!(recovered.records.len(), 4);
        assert!(recovered.records.iter().all(|r| r.lsn < target_lsn));
        // The segment was physically truncated at the bad frame.
        let truncated_len = std::fs::metadata(&seg_path).unwrap().len();
        assert_eq!(truncated_len as usize, target_off);
        let backup = seg_path.with_extension(format!(
            "log.force-recover-{}-{}.bak",
            std::process::id(),
            target_off
        ));
        assert_eq!(std::fs::read(backup).unwrap(), bytes);
    }
}

#[cfg(test)]
mod compensation_codec_tests {
    use super::*;

    #[test]
    fn rollback_to_wal_roundtrip_and_truncated_count() {
        let payload = WalPayload::RollbackTo {
            undo_head: Some(17),
            undo_next_lsn: 80,
            skip_lsn_start: 40,
            skip_lsn_end: 60,
            compensated: vec![91, 105],
        };
        let mut bytes = Vec::new();
        encode_payload_into(&payload, &mut bytes).unwrap();
        assert_eq!(bytes.len(), encoded_payload_len(&payload).unwrap());
        let WalPayload::RollbackTo {
            undo_head,
            undo_next_lsn,
            skip_lsn_start,
            skip_lsn_end,
            compensated,
        } = decode_payload(WalRecordType::RollbackTo, &bytes).unwrap()
        else {
            panic!("wrong kind");
        };
        assert_eq!(
            (undo_head, undo_next_lsn, skip_lsn_start, skip_lsn_end),
            (Some(17), 80, 40, 60)
        );
        assert_eq!(compensated, vec![91, 105]);
        for len in 0..bytes.len() {
            assert!(
                decode_payload(WalRecordType::RollbackTo, &bytes[..len]).is_err(),
                "prefix {len}"
            );
        }
    }
}

#[cfg(test)]
#[path = "wal/correctness_tests.rs"]
mod correctness_tests;
