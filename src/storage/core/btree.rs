const INDEX_MAGIC: &[u8; 8] = b"MYDBIDX1";
// Bumped from 5 → 6 when the per-KeyPart on-disk tag set was widened
// from 6 variants (Null/Int/UInt/Float/Bytes/String) to 12 variants
// covering every `ColumnKind` (adding Float32/Float64 in
// order-preserving bit form plus binary Decimal/Date/Time/DateTime/Year).
// Old .idx files cannot be read by this build — `decode_index_meta`
// rejects mismatched versions and tells the operator to rebuild.
// Bumped from 6 → 7 when the per-entry RowId encoding was folded so
// that the 8-byte `table_id` is stored once per page (in the page
// header, immediately after the 24-byte common header) instead of
// being repeated on every entry.
// Bumped 7 → 8 when leaf pages gained a trailing slot array (one
// u16 byte-offset per entry, in key-sorted order, packed at the
// **end** of the page payload). This unlocks O(log N) byte-level
// binary search directly on the raw page buffer — the cold-page
// lookup path no longer pays for full O(N) `decode_index_tree_page`
// work; only the O(log N) entries the search actually visits get
// key-decoded. Older .idx files trip `validate_index_page_header`
// and `startup::validate_or_rebuild_indexes` rebuilds from the heap.
// Bumped 8 → 9 when leaf pages gained a 1B `key_layout` byte and
// an 8B `base_time` slot in the payload header. Layout 0 keeps the
// v8 generic per-entry encoding bit-for-bit; layout 1 packs each
// entry's key as a 4B `delta = (time_value - base_time) as u32`
// (used for single-column DATETIME / TIME indexes whose page-local
// value range fits in u32). Older v8 .idx files trip
// `validate_index_page_header` and `startup::validate_or_rebuild_indexes`
// rebuilds from the heap.
// Bumped 9 → 10 when two per-entry overheads in the GENERIC leaf
// layout and every internal-page entry were shrunk:
//   * `part_count` shrinks from u32 (4 B) to u8 (1 B). MySQL caps
//     index part count at 16; u8 holds 255, so the wider field was
//     pure waste. Saves 3 B per entry on the GENERIC leaf layout
//     and on every internal-page separator (the compact DATE /
//     DATETIME leaf layouts have already pinned part_count = 1 at
//     the page level, so they're unaffected).
//   * `page_id` is now an unsigned LEB128 varint (1–9 B). lineitem
//     at SF=1 has ~52 K heap pages → 3 B per page_id; even a TB-class
//     workload tops out at ~36 bits → 5 B. The fixed 8 B was overkill
//     for every realistic shape. Slot / generation stay fixed u16.
// Both savings stack on top of the existing slot-array indirection
// (trailing u16 offsets), so binary search remains O(log N) with
// variable-stride entries. WAL / undo / spill-stream formats are
// untouched — see `encode_index_key_into`,
// `encode_index_entry_for_run_stream`, and `encode_wal_row_id`.
// Older v9 .idx files trip `decode_index_meta` (meta page version
// check) and `validate_index_page_header`; both error paths route
// into `startup::validate_or_rebuild_indexes` which rebuilds from
// the heap.
// Bumped 11 → 12 when leaf pages gained two compact DECIMAL layouts
// (i32 / i64 absolute-value encoding for single-column DECIMAL(P≤9)
// and DECIMAL(P≤18) indexes). Old v11 `.idx` files are rejected by
// startup validation and rebuilt from heap.
// Bumped 12 → 13 when leaf pages gained two compact INT layouts
// (base + u16 delta and base + u32 delta) for single-column
// INT/BIGINT/SMALLINT/TINYINT indexes. This is the single biggest
// shape in real schemas (PKs, FKs), and the generic layout was
// paying 1 B part_count + 1 B tag + 8 B i64 = 10 B per entry vs the
// new layouts' 2 B (delta+base) and 4 B (delta+base). Old v12 `.idx`
// files are rejected by startup validation and rebuilt from heap.
// Bumped 13 → 14 when leaf and internal pages gained a compact
// two-column INT layout (KEY_LAYOUT_COMPACT_INT2). The dominant
// shape this targets is multi-column INT PKs — TPC-H lineitem's
// (l_orderkey, l_linenumber), composite FKs like
// (l_partkey, l_suppkey), and BenchmarkSQL's (c_w_id, c_d_id, c_id)
// prefixes. Generic encoding for such keys was 1 B part_count + 2 B
// tags + 2×8 B i64 + RowId = ~27 B/entry; INT2 encodes both columns
// as page-local base+delta (widths chosen per page from u8/u16/u32),
// dropping to ~13 B/entry for typical sequential PKs. Old v13 .idx
// files are rejected and rebuilt from heap.
// Bumped 14 → 15 when CHAR index keys became canonicalized without
// trailing pad spaces. The physical page layout is unchanged, but old
// CHAR entries may contain padded keys and must be rebuilt from heap
// rows so lookups use the compact key form.
// Bumped 16 → 17 when MySQL 8.0 charset/collation metadata expanded
// and utf8mb4_0900_ai_ci switched to its true NO PAD comparison key.
// The page layout is unchanged, but collated string keys must be
// rebuilt from heap rows so uniqueness/range scans use the new keys.
// Bumped 17 → 18 with the rowid-6bytes refactor: every embedded
// RowId drops its 2-byte `generation` field. Per-entry on-disk size
// shrinks by 2 B (full RID) or 2 B (same-page RID), saving ~25 % per
// leaf entry on tightly packed indexes. Old .idx files are rejected.
//
// Bumped 18 → 19 alongside `INDEX_PAGE_VERSION 10 → 11`. First-wave
// storage compression (storage-compression-opportunities.md):
//   A) page_table_id field dropped from every page payload — the v12
//      "always zero" sentinel is gone, saving 8 B/page.
//   G) leaf entry_count widened to u32 in v9 even though a 16 KB leaf
//      can never hold more than a few thousand entries; back to u16,
//      saving 2 B/page.
// Net: 10 B/page off the leaf header, 8 B/page off the internal header.
//
// Bumped 19 → 20 alongside `INDEX_PAGE_VERSION 11 → 12`: two new
// compact leaf/internal layouts for single-column `INT UNSIGNED`
// (`KeyPart::UInt`) keys — `KEY_LAYOUT_COMPACT_UINT_U16` (8) and
// `KEY_LAYOUT_COMPACT_UINT_U32` (9). Sequential `… UNSIGNED`
// auto-increment PKs (the most common production PK shape) previously
// only had the generic 1 B part_count + 1 B tag + 8 B u64 layout; they
// now pack the same base+delta way the signed-INT layouts already do.
// New .idx files can carry layout bytes 8/9 that a v19 binary would
// mis-decode, so the version gate rejects across the boundary.
//
// Bumped 20 → 21 for the FULLTEXT posting-format upgrade
// (docs/fulltext-posting-format-upgrade-design.md): inverted keys went
// from single-part `[String(token)]` to two-part
// `[String(token), Bytes(payload)]` where the payload carries per-doc
// `tf` / `doc_len` / positions. The leaf/WAL codec is unchanged (Bytes is
// an existing KeyPart), but a v20 FULLTEXT index has single-part entries a
// v21 binary would neither score nor delete correctly, so the version gate
// forces a rebuild (from the heap, regenerating payload keys) across the
// boundary. Non-FULLTEXT indexes are byte-identical; they just rebuild too.
//
// Bumped 21 → 22 with index page v14's mutable `(offset,len)` slots and
// separator-local right-child pointers.
// Raw slotted pages are now the only authoritative in-memory representation.
// Slot order, rather than physical record order, is therefore part of the
// persisted contract and old index files must be rebuilt.
//
// Bumped 22 → 23 for the GIS compatibility boundary. The index-page and WAL
// physical layouts are unchanged; rebuilding prevents a pre-SRID-contract
// spatial index from being opened under the new planner/cache invariants.
// v24 adds the explicit tag-14 BigInt coefficient used by indexed
// DECIMAL(39..65) columns. Older files are rejected at the meta page.
use crate::storage::format_versions::{INDEX_PAGE_VERSION, INDEX_VERSION};
const INDEX_META_PAGE: u64 = 0;
const INDEX_EMPTY_ROOT: u64 = 1;
// A WAL frame is the physical-replication unit and the replication envelope
// caps payloads at 64 MiB. Keep logical index batches much smaller than that:
// besides leaving room for both envelopes, 8 MiB bounds the temporary encoded
// key/op vectors on the primary and the decoded replay batch on a 1 GiB node.
// This is a storage/WAL invariant, not a TPC-H tuning knob; every bulk index
// writer uses the same cap.
const INDEX_BATCH_WAL_TARGET_BYTES: usize = 8 * 1024 * 1024;
// Empty-index bulk builds are a derived, whole-file operation.  Keep their
// accounting process-wide so SHOW ENGINE MYDB STATUS can distinguish the
// sequential direct builder from the buffer-pool oracle without threading a
// Database-owned stats handle through every IndexFile.
static BULK_INDEX_DIRECT_BUILDS: AtomicU64 = AtomicU64::new(0);
static BULK_INDEX_DIRECT_PAGES: AtomicU64 = AtomicU64::new(0);
static BULK_INDEX_DIRECT_US: AtomicU64 = AtomicU64::new(0);
static BULK_INDEX_BUFFERED_BUILDS: AtomicU64 = AtomicU64::new(0);
static BULK_INDEX_BUFFERED_PAGES: AtomicU64 = AtomicU64::new(0);
static BULK_INDEX_BUFFERED_US: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
thread_local! {
    static BULK_INDEX_FAIL_BEFORE_META: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static INDEX_TREE_PAGE_ENCODINGS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn install_bulk_index_fail_before_meta() {
    BULK_INDEX_FAIL_BEFORE_META.with(|enabled| enabled.set(true));
}

#[cfg(test)]
fn take_bulk_index_fail_before_meta() -> bool {
    BULK_INDEX_FAIL_BEFORE_META.with(|enabled| enabled.replace(false))
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct BulkIndexBuildStatus {
    pub direct_builds: u64,
    pub direct_pages: u64,
    pub direct_us: u64,
    pub buffered_builds: u64,
    pub buffered_pages: u64,
    pub buffered_us: u64,
}

pub(super) fn bulk_index_build_status() -> BulkIndexBuildStatus {
    BulkIndexBuildStatus {
        direct_builds: BULK_INDEX_DIRECT_BUILDS.load(Ordering::Relaxed),
        direct_pages: BULK_INDEX_DIRECT_PAGES.load(Ordering::Relaxed),
        direct_us: BULK_INDEX_DIRECT_US.load(Ordering::Relaxed),
        buffered_builds: BULK_INDEX_BUFFERED_BUILDS.load(Ordering::Relaxed),
        buffered_pages: BULK_INDEX_BUFFERED_PAGES.load(Ordering::Relaxed),
        buffered_us: BULK_INDEX_BUFFERED_US.load(Ordering::Relaxed),
    }
}

fn record_bulk_index_build(index: &IndexFile, direct: bool, started: Instant) {
    let elapsed_us = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
    let pages = index
        .meta_state
        .lock()
        .meta
        .next_page_id
        .saturating_sub(INDEX_EMPTY_ROOT);
    if direct {
        BULK_INDEX_DIRECT_BUILDS.fetch_add(1, Ordering::Relaxed);
        BULK_INDEX_DIRECT_PAGES.fetch_add(pages, Ordering::Relaxed);
        BULK_INDEX_DIRECT_US.fetch_add(elapsed_us, Ordering::Relaxed);
    } else {
        BULK_INDEX_BUFFERED_BUILDS.fetch_add(1, Ordering::Relaxed);
        BULK_INDEX_BUFFERED_PAGES.fetch_add(pages, Ordering::Relaxed);
        BULK_INDEX_BUFFERED_US.fetch_add(elapsed_us, Ordering::Relaxed);
    }
}
const INDEX_MIN_PAGE_BYTES: usize = PAGE_SIZE / 4;
const INDEX_HEADER_BYTES: usize = 64;
/// Common index page header (offsets 0..24). The buffer-pool +
/// WAL layer manages this header uniformly for every page-typed
/// file (heap / index / overflow / vector / ivf):
///   [0..4]   magic          u32
///   [4..6]   page version   u16
///   [6..8]   reserved       u16 (zero)
///   [8..16]  page LSN       u64  — stamped by `set_page_lsn`,
///                                  DO NOT reuse for page-level
///                                  metadata (early Phase 1b prototype
///                                  learned this the hard way: the
///                                  LSN write silently clobbered the
///                                  writer bound).
///   [16..20] checksum       u32
///   [20..24] payload len    u32
///
/// The writer bound (`page_max_trx_id`) lives inside the leaf
/// payload header instead — see `LEAF_OFF_PAGE_MAX_TRX_ID`.
// v20 leaf payload header layout (offsets are payload-relative — add
// `INDEX_HEADER_BYTES` to convert to absolute page offsets):
//     [0]       page_type (u8, = 1 for leaf)
//     [1]       key_layout (u8): 0=generic, 1=compact i64+u32 delta, ...
//     [2..10]   base_time (i64; 0 when layout=0)
//     [10..18]  next_leaf (u64; 0 = None)
//     [18..20]  entry_count (u16) — saved 2 B vs v18 u32
//     [20..28]  page_max_trx_id (u64) — Phase 1b: upper bound on the
//               writer transaction id that has ever modified this
//               leaf. Monotonically raised in `write_page` before
//               encoding; preserved on split/merge/borrow. Consumed
//               by the Phase 2 `view.sees(v)` shortcut in the
//               visibility filter. Zero on freshly bulk-loaded or
//               empty pages (all readers trivially see 0, so the
//               shortcut safely succeeds for them).
//     [28..]    entries (per-layout encoding)
//     trailing slot array (count * 4B) at the end of the payload:
//       (record_offset:u16, record_len:u16)
const LEAF_PAYLOAD_HEADER_BYTES: usize = 1 + 1 + 8 + 8 + 2 + 8;
// Bytes consumed by a leaf-page header (excluding entries):
//   common header (24) + leaf payload header (20) = 44 (was 54 in v18)
const LEAF_HEADER_BYTES: usize = INDEX_HEADER_BYTES + LEAF_PAYLOAD_HEADER_BYTES;
// Bytes consumed by an internal-page header (excluding separator records):
//   common header (24) + page_type (1) + key_layout (1) + base_time (8)
//                      + first_child (8) + key_count (4) = 46
// v13 added `key_layout u8 + base_time i64` immediately after
// `page_type` so internal separators can ride the same compact-delta
// layouts as leaf entries (single-column INT/DATE/DATETIME/DECIMAL).
const INTERNAL_HEADER_BYTES: usize = INDEX_HEADER_BYTES + 1 + 1 + 8 + 8 + 4;
// Internal payload offsets. Each slotted separator record owns its
// `right_child`; the fixed header stores only `first_child`.
const INTERNAL_OFF_PAGE_TYPE: usize = 0;
const INTERNAL_OFF_KEY_LAYOUT: usize = 1;
const INTERNAL_OFF_BASE_TIME: usize = 2;
const INTERNAL_OFF_FIRST_CHILD: usize = 10;
const INTERNAL_OFF_KEY_COUNT: usize = 18;
// Magic byte 4 advanced again — '7' (0x37) → '8' (0x38) — so v10
// .idx files (FNV-1a page checksum) trip `validate_index_page_header`
// before any version/checksum probe; v9 (u32 part_count + fixed 8B
// page_id) and earlier are still also rejected.
const INDEX_PAGE_MAGIC: u32 = 0x4d_49_50_38;
const INDEX_META_MAGIC: u32 = 0x4d_49_4d_34;
// Single slot entry: u16 little-endian byte offset of an entry,
// relative to the start of the page payload.
const SLOT_OFFSET_BYTES: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IndexRawSlot {
    offset: u16,
    len: u16,
}
// Bumped 7 → 8 alongside `INDEX_VERSION` — page checksum algorithm
// changed from FNV-1a (byte loop) to CRC32 (`crc32fast`, ARMv8 CRC32
// / SSE4.2). Same byte at offset 16 (4B little-endian), incompatible
// values across the boundary.
// Bumped 9 → 10 with the rowid-6bytes refactor: every embedded
// RowId entry drops its 2-byte `generation` field, so the per-entry
// byte layout (RID_KIND_FULL: 8 → 6 B, RID_KIND_SAME_PAGE: 4 → 2 B)
// changed. Old leaf pages mis-decode under a v18 binary.
//
// Bumped 10 → 11 alongside `INDEX_VERSION 18 → 19`:
//   A) page_table_id (8 B) removed from page payload — the v12 sentinel
//      slot that was always zero. Every `LEAF_OFF_*`/`INTERNAL_*`
//      offset shifts by -8.
//   G) entry_count widened slot u32 → u16 (saves 2 B/page).
//
// Bumped 11 → 12 alongside `INDEX_VERSION 19 → 20`: adds the
// `KEY_LAYOUT_COMPACT_UINT_U{16,32}` leaf/internal layouts. The page
// structure is unchanged, but a page may now carry `key_layout` bytes
// 8/9 that older binaries don't recognize, so they must reject the
// page rather than mis-decode it.
//
// Bumped 12 → 13 for `page_max_trx_id` (Phase 1b):
// the leaf payload header grows by 8 bytes (a u64 writer bound at
// payload offset 20). v12 leaves have `LEAF_OFF_ENTRIES = 20` while
// v13 leaves have `LEAF_OFF_ENTRIES = 28`; a v12 binary would
// mis-decode a v13 page and vice versa, so the version gate rejects
// the file and startup validation rebuilds the index from the heap.
//
// Bumped 13 → 14 for raw mutable slotted pages: slots are now
// `(offset:u16,len:u16)`, and internal pages store `first_child` in the
// fixed header plus `right_child` in each separator record.

// Leaf payload offsets (payload-relative; valid for v20 leaves).
const LEAF_OFF_PAGE_TYPE: usize = 0;
const LEAF_OFF_KEY_LAYOUT: usize = 1;
const LEAF_OFF_BASE_TIME: usize = 2;
const LEAF_OFF_NEXT_LEAF: usize = 10;
const LEAF_OFF_ENTRY_COUNT: usize = 18;
/// v13: 8 bytes carrying `IndexLeafPage::page_max_trx_id`. Placed
/// after `entry_count` (2B) so the pre-existing scalar offsets
/// shift only for the entries region.
const LEAF_OFF_PAGE_MAX_TRX_ID: usize = 20;
const LEAF_OFF_ENTRIES: usize = 28;
const _: () = assert!(LEAF_OFF_ENTRIES == LEAF_PAYLOAD_HEADER_BYTES);

// v9 key layouts.
const KEY_LAYOUT_GENERIC: u8 = 0;
/// Compact layout for single-column `DATETIME` indexes whose
/// page-local value range fits in 32 bits. Each entry encodes the
/// time component as `delta = (value - base_time) as u32` (4 B) plus
/// the RowId — saving 9 B / entry vs the generic 4 B part_count +
/// 1 B tag + 8 B i64 layout.
const KEY_LAYOUT_COMPACT_TIME_U32: u8 = 1;
/// Compact layout for single-column `DATE` indexes. `DATE` is an i32
/// day count, so a whole 16 KB leaf holds at most a few hundred
/// distinct days — the page-local delta always fits in `u16`. Each
/// entry encodes `delta = (days - base_time) as u16` (2 B) plus the
/// RowId, saving 7 B / entry vs the generic 4 B part_count + 1 B tag
/// + 4 B i32 layout. `base_time` carries the base day count.
const KEY_LAYOUT_COMPACT_DATE_U16: u8 = 2;
/// Compact layout for single-column `DECIMAL(P, S)` indexes with
/// `P ≤ 9`. Each entry encodes the magnitude as `i32` LE (4 B) plus
/// the RowId — saving 14 B / entry vs the generic 1 B part_count +
/// 1 B tag + 16 B i128 + 1 B scale layout. The `base_time` slot of
/// the leaf header carries the column's `scale` value in its low
/// byte (decimal layouts don't use delta encoding so the rest is 0).
const KEY_LAYOUT_COMPACT_DECIMAL_I32: u8 = 3;
/// Compact layout for single-column `DECIMAL(P, S)` indexes with
/// `P ≤ 18`. Each entry encodes the magnitude as `i64` LE (8 B) plus
/// the RowId — saving 10 B / entry vs the generic 19 B layout. Same
/// `base_time = scale` convention as the i32 layout.
const KEY_LAYOUT_COMPACT_DECIMAL_I64: u8 = 4;
/// Compact layout for single-column `INT` (i64) indexes whose
/// page-local value span fits in 16 bits. Each entry encodes
/// `delta = (value - base_time) as u16` (2 B) plus the RowId,
/// saving 8 B / entry vs the generic 1 B part_count + 1 B tag +
/// 8 B i64 layout. `base_time` carries the chunk's minimum value.
/// Sequential INT/BIGINT primary keys (which is most production
/// PKs) typically pack ≥ 4000 entries into a single 16 KB leaf
/// at this layout vs ≤ 800 at generic.
const KEY_LAYOUT_COMPACT_INT_U16: u8 = 5;
/// Compact layout for single-column `INT` (i64) indexes whose
/// page-local value span exceeds 16 bits but fits in 32 bits.
/// Each entry encodes `delta = (value - base_time) as u32` (4 B)
/// plus the RowId, saving 6 B / entry vs the generic layout.
/// Covers gappy INT distributions, monotonically-increasing
/// BIGINT IDs over wide page ranges, etc.
const KEY_LAYOUT_COMPACT_INT_U32: u8 = 6;
/// Compact layout for two-column INT keys (e.g. TPC-H lineitem's
/// `PRIMARY KEY (l_orderkey, l_linenumber)`, composite FKs like
/// `(l_partkey, l_suppkey)`, or any 2-column INT secondary index).
/// Generic encoding pays 1 B part_count + 2 B tags + 2×8 B i64
/// = ~19 B for the key alone. INT2 encodes each column as a
/// page-local `base + delta` with independently-chosen delta width
/// (1/2/4 bytes), commonly dropping the key portion to 3-6 B.
///
/// Per-page metadata sits in a 20 B prefix block immediately after
/// the standard leaf header:
///   col1_base i64 (8 B) + col2_base i64 (8 B) +
///   width1 u8 (1 B)    + width2 u8 (1 B)     + padding (2 B)
///
/// `base_time` field of the standard header is reused as
/// `col1_base`; the prefix block carries `col2_base` and both
/// widths so the encoder/decoder don't need to recompute them.
const KEY_LAYOUT_COMPACT_INT2: u8 = 7;
/// Compact layout for single-column `INT UNSIGNED` (`KeyPart::UInt`,
/// i.e. `u64`) indexes whose page-local value span fits in 16 bits.
/// The unsigned twin of `KEY_LAYOUT_COMPACT_INT_U16`: each entry
/// encodes `delta = (value - base) as u16` (2 B) plus the RowId. The
/// page `base_time` slot stores the chunk minimum reinterpreted as
/// `i64` (`base as u64` recovers it) so the full `u64` range — past
/// `i64::MAX`, where `BIGINT UNSIGNED` auto-increment PKs live — is
/// representable. Sequential `… UNSIGNED` PKs pack densely here.
const KEY_LAYOUT_COMPACT_UINT_U16: u8 = 8;
/// Compact layout for single-column `INT UNSIGNED` indexes whose
/// page-local value span exceeds 16 bits but fits in 32 bits. The
/// unsigned twin of `KEY_LAYOUT_COMPACT_INT_U32`.
const KEY_LAYOUT_COMPACT_UINT_U32: u8 = 9;

/// Bytes consumed by the INT2 prefix block (between the standard
/// leaf header and the first entry).
const COMPACT_INT2_PREFIX_BYTES: usize = 20;

type BtreeDedupSet = crate::util::fast_hash::AhHashSet<RowId>;

/// Brent's cycle detector: constant storage, no extra page probes or metadata
/// mutex on point lookups. The tree read lock keeps descent and sibling topology fixed.
struct IndexPageProgress {
    checkpoint: u64,
    distance: usize,
    power: usize,
}

impl IndexPageProgress {
    fn new(first: u64) -> Self {
        Self {
            checkpoint: first,
            distance: 0,
            power: 1,
        }
    }

    fn advance(&mut self, next: u64) -> Result<(), QueryError> {
        if next == self.checkpoint {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: cyclic page chain",
            ));
        }
        self.distance += 1;
        if self.distance == self.power {
            self.checkpoint = next;
            self.distance = 0;
            self.power = self.power.saturating_mul(2);
        }
        Ok(())
    }
}

/// Owns the navigation locks for a cross-call leaf-id scan. Workers share one
/// lease, so split/merge/reuse cannot invalidate pages already assigned to them.
/// Leaf-only writers retain shared access. Drop releases both locks on any path.
pub struct IndexRangeLease {
    _structure: parking_lot::ArcRwLockReadGuard<parking_lot::RawRwLock, ()>,
    _handle: Option<parking_lot::ArcRwLockReadGuard<parking_lot::RawRwLock, IndexFile>>,
}

pub struct IndexLeafRange {
    ids: Vec<u64>,
    pub(crate) lease: Option<Arc<IndexRangeLease>>,
}

impl std::ops::Deref for IndexLeafRange {
    type Target = [u64];
    fn deref(&self) -> &Self::Target {
        &self.ids
    }
}

#[cfg(test)]
thread_local! {
    static INDEX_STRUCTURE_PREPARED_TEST_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);
    static INDEX_FAIL_WRITE_PERSIST: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static INDEX_FAIL_WRITE_AFTER: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

#[derive(Debug)]
enum BufferedIndexInsert {
    Complete,
    Remaining { consumed: usize, lsn: u64 },
}

#[derive(Debug)]
struct PreparedIndexInsert {
    pins: Vec<PinnedFrame>,
    route_epochs: Vec<u64>,
    path: Vec<(u64, usize)>,
    leaf_id: u64,
    leaf: IndexLeafPage,
    duplicate: bool,
    next_page: u64,
    free_page: Option<u64>,
    root_page: u64,
}

#[derive(Debug)]
pub(crate) struct IndexFile {
    spatial: Option<Box<spatial_index::SpatialIndexFile>>,
    /// A pinned BUILDING handle records DML while its private tree is bulk loaded.
    online_build_log: Option<Arc<OnlineIndexBuildLog>>,
    path: PathBuf,
    /// File handle. Wrapped in `parking_lot::Mutex` so methods can mutate
    /// (seek + read/write) under shared `&self` access — required by the
    /// concurrent-write refactor where `with_index_mut` gives a read lock
    /// on the outer `IndexHandle` and many concurrent writers share the
    /// same IndexFile.
    file: parking_lot::Mutex<Option<File>>,
    log: Arc<crate::wal::LogMutex>,
    log_atomics: Arc<crate::wal::LogStateAtomics>,
    durability: DurabilityMode,
    /// Hot lock-free read mirror of `meta_state.lock().meta.root_page`.
    /// Every `find_leaf_for_entry` reads this without taking the meta
    /// lock; structural modifications (root split) update both.
    root_page: AtomicU64,
    /// Bounded weak root-page hint; it never prevents buffer eviction.
    root_frame_hint: parking_lot::RwLock<std::sync::Weak<Frame>>,
    /// Serializes structural B-tree changes (split/merge/root/meta).
    /// Ordinary leaf-only DML can hold this in shared mode and rely on
    /// per-leaf latches; any fallback that may change topology takes it
    /// exclusively and preserves the legacy single-writer semantics.
    structure_lock: Arc<parking_lot::RwLock<()>>,
    // Split preparation may perform cache I/O without excluding leaf writers.
    split_prepare: parking_lot::Mutex<()>,
    /// Authoritative store for B-tree metadata + freelist + meta-page
    /// byte image. Touched only on rare
    /// structural events (page alloc/free, root split, meta flush) so
    /// the per-call lock cost is amortized across many in-place
    /// insert/delete operations that go through the lock-free fast path.
    meta_state: parking_lot::Mutex<IndexMetaState>,
    file_key: Arc<BufferPageFileKey>,
    buffer_pool: Arc<BufferPool>,
    dirty_meta: AtomicBool,
    dirty: AtomicBool,
    /// Leaf write latches keyed by page id. These let independent leaf-only
    /// DML updates proceed under `structure_lock.read()` without racing on
    /// the same encoded buffer-pool page.
    page_latches: Box<[CachePadded<Mutex<()>>]>,
    /// `resolved_page_latch_count() - 1`; `page_id & page_latch_mask`
    /// selects a latch bucket. Stored per file so the count can vary by
    /// process without a recompile (shared with `HeapFile`).
    page_latch_mask: u64,
    mutation_generation: AtomicU64,
    active_mutations: AtomicUsize,
    order_stats_cache: parking_lot::Mutex<HashMap<RangeOrderStatsKey, Arc<CachedRangeOrderStats>>>,
    rel_path: String,
    /// A speculative cache-miss opener that loses the install race shares
    /// this index's buffer-pool file key but does not own its lifecycle. Such
    /// a handle must not flush stale metadata when it is dropped.
    flush_on_drop: bool,
    failed: AtomicBool,
}

pub(crate) static ORDER_STATS_CACHE_HITS: AtomicU64 = AtomicU64::new(0);
pub(crate) static ORDER_STATS_CACHE_MISSES: AtomicU64 = AtomicU64::new(0);
pub(crate) static ORDER_STATS_CACHE_INVALIDATIONS: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RangeOrderStatsKey {
    low: Option<IndexBound>,
    high: Option<IndexBound>,
    filter_terms: Vec<(usize, KeyPart)>,
}

#[derive(Debug, Clone)]
struct RangeOrderLeafSpan {
    page_id: u64,
    start: usize,
    end: usize,
    prefix_end: usize,
    page_max_trx_id: u64,
}

#[derive(Debug, Clone)]
struct RangeOrderStats {
    leaves: Vec<RangeOrderLeafSpan>,
    total: usize,
}

#[derive(Debug)]
struct CachedRangeOrderStats {
    generation: u64,
    stats: RangeOrderStats,
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

fn range_order_key_part_capacity_bytes(part: &KeyPart) -> usize {
    match part {
        KeyPart::Bytes(value) => value.capacity(),
        KeyPart::String(value) | KeyPart::CollatedString { value, .. } => value.capacity(),
        KeyPart::DecimalWide { value, .. } => value.bits().div_ceil(8) as usize,
        _ => 0,
    }
}

fn range_order_bound_capacity_bytes(bound: Option<&IndexBound>) -> usize {
    let Some(bound) = bound else { return 0 };
    let nested = bound.key.0.iter().fold(0usize, |bytes, part| {
        bytes.saturating_add(range_order_key_part_capacity_bytes(part))
    });
    bound
        .key
        .0
        .capacity()
        .saturating_mul(std::mem::size_of::<KeyPart>())
        .saturating_add(nested)
}

/// Cold-path metadata bundled behind one mutex.
///
/// `logical_wal_lsn` is **not** stored here — it became thread-local
/// (`crate::storage::core::btree_logical_wal`) so a thread driving
/// `apply_single_index_op_logged` can stamp the FPI sentinel and have
/// concurrent threads on the same `IndexFile` still see their own
/// (independent) logical-WAL state.
#[derive(Debug)]
struct IndexMetaState {
    /// B-tree metadata. `meta.root_page` is mirrored to the atomic
    /// `IndexFile::root_page`; readers (find_leaf) consult the atomic
    /// without locking. Writers (root split) update both.
    meta: IndexMeta,
    /// Encoded meta-page bytes; rewritten when `meta` changes so the
    /// next `write_meta_page` / FPI before-image stays current.
    meta_image: Vec<u8>,
}

#[derive(Debug, Clone)]
struct IndexMeta {
    root_page: u64,
    next_page_id: u64,
    free_page_head: Option<u64>,
}

/// Layout-specific extra metadata for layouts that need more than
/// a single i64 to describe themselves. Currently only populated
/// for `KEY_LAYOUT_COMPACT_INT2`, which carries a second base and
/// per-column delta widths. Other layouts leave this `None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LayoutAux {
    /// INT2: base for column 2 (column 1's base lives in `base_time`).
    col2_base: i64,
    /// INT2: byte width of column 1's delta (1, 2, or 4).
    width1: u8,
    /// INT2: byte width of column 2's delta (1, 2, or 4).
    width2: u8,
}

#[derive(Debug, Clone)]
struct IndexLeafPage {
    entries: Vec<IndexEntry>,
    next_leaf: Option<u64>,
    /// v9 page format selector. Defaults to `KEY_LAYOUT_GENERIC`;
    /// callers that build a leaf for a known compatible layout
    /// (e.g. a single-column DATETIME index after `pick_leaf_layout`
    /// approves it) bump this to `KEY_LAYOUT_COMPACT_TIME_U32` for
    /// denser on-disk packing. The byte-level encoder dispatches on
    /// this; in-memory access still uses fully-decoded `entries`.
    key_layout: u8,
    /// Time base subtracted from every entry's time component when
    /// `key_layout == KEY_LAYOUT_COMPACT_TIME_U32`. Ignored (and
    /// written as 0) for the generic layout.
    base_time: i64,
    /// Layout-specific extras (currently only `KEY_LAYOUT_COMPACT_INT2`
    /// populates this). `None` for every other layout.
    layout_aux: Option<LayoutAux>,
    /// Cached encoded size against `(key_layout, base_time, layout_aux)`.
    /// Replaces the per-call O(N) walk inside `encoded_leaf_page_len`,
    /// which sysbench oltp_update_index profiling showed as the #1 hot
    /// function (~2200 samples per 25 s / 12 threads — every UPDATE was
    /// re-summing 100+ entries × 2-3 size checks). Maintained
    /// incrementally on insert / delete and recomputed in full only on
    /// layout demote. Invariant: equal to what
    /// `Self::compute_encoded_size(&entries, key_layout, layout_aux)`
    /// would return; `debug_assert_eq!` in `encoded_leaf_page_len`
    /// guards against staleness.
    cached_size: usize,
    /// Upper bound on the writer transaction id that has ever modified
    /// this leaf page. Monotonically non-decreasing under normal writes:
    /// every insert/update/delete (and split/merge that produces this
    /// leaf) raises it via `page_max_trx_id.max(writer_txn_id)`.
    ///
    /// Persisted at common index header offset 8 (previously an
    /// always-zero reserved slot). Zero means "no writer has claimed
    /// this page yet"; freshly built leaves from bulk-load / rebuild
    /// / decode-of-legacy-page all read as 0.
    ///
    /// Phase 1a: the field is stored and round-tripped through
    /// encode/decode, but no read path consumes it yet. Phase 1b
    /// wires it into insert/update/delete/split-merge; Phase 2 adds
    /// the `view.sees(page_max_trx_id)` shortcut in the visibility
    /// filter.
    page_max_trx_id: u64,
}

impl IndexLeafPage {
    #[cfg(test)]
    fn new(entries: Vec<IndexEntry>, next_leaf: Option<u64>) -> Self {
        Self::new_with_layout(entries, next_leaf, KEY_LAYOUT_GENERIC, 0)
    }

    #[cfg(test)]
    fn new_with_layout(
        entries: Vec<IndexEntry>,
        next_leaf: Option<u64>,
        key_layout: u8,
        base_time: i64,
    ) -> Self {
        // Trust the caller's layout. Used both by the decoder (where
        // `key_layout` is what's on disk and must be preserved) and
        // by encoder-side callers (where they've already run
        // `resolve_leaf_layout` if they want a fresh pick).
        let cached_size = Self::compute_encoded_size(&entries, key_layout, None);
        Self {
            entries,
            next_leaf,
            key_layout,
            base_time,
            layout_aux: None,
            cached_size,
            page_max_trx_id: 0,
        }
    }

    fn new_with_layout_aux(
        entries: Vec<IndexEntry>,
        next_leaf: Option<u64>,
        key_layout: u8,
        base_time: i64,
        layout_aux: Option<LayoutAux>,
    ) -> Self {
        let cached_size = Self::compute_encoded_size(&entries, key_layout, layout_aux);
        Self {
            entries,
            next_leaf,
            key_layout,
            base_time,
            layout_aux,
            cached_size,
            page_max_trx_id: 0,
        }
    }

    /// Construct a leaf from a fresh entry set, auto-picking the
    /// densest viable layout. Used by split / merge paths that
    /// produce new leaves; the row-by-row `insert` path uses the
    /// lighter in-place demote check in `insert_internal` instead.
    fn new_with_resolved_layout(entries: Vec<IndexEntry>, next_leaf: Option<u64>) -> Self {
        let (key_layout, base_time, layout_aux) =
            resolve_entry_layout_full(KEY_LAYOUT_GENERIC, 0, None, &entries);
        let cached_size = Self::compute_encoded_size(&entries, key_layout, layout_aux);
        Self {
            entries,
            next_leaf,
            key_layout,
            base_time,
            layout_aux,
            cached_size,
            page_max_trx_id: 0,
        }
    }

    /// Getter for the persisted page-level upper-bound writer trx id.
    /// Zero means "no writer has claimed this page yet" (fresh empty
    /// root, freshly bulk-loaded page, or a page decoded from an
    /// older format that lacked this field).
    #[inline]
    pub(crate) fn page_max_trx_id(&self) -> u64 {
        self.page_max_trx_id
    }

    /// Direct setter, used by `decode_index_tree_page` to install the
    /// on-disk value onto the freshly constructed in-memory page.
    #[allow(dead_code)]
    #[inline]
    fn set_page_max_trx_id(&mut self, v: u64) {
        self.page_max_trx_id = v;
    }

    /// Monotonically raise the page-level writer bound. Called by the
    /// write path (Phase 1b) so every insert/update/delete stamps the
    /// current writer's txn id, matching InnoDB's PAGE_MAX_TRX_ID
    /// semantic (doc_old/design/index-page-max-trx-2026-07.md).
    #[allow(dead_code)]
    #[inline]
    fn raise_page_max_trx_id(&mut self, v: u64) {
        if v > self.page_max_trx_id {
            self.page_max_trx_id = v;
        }
    }

    /// Per-entry encoded byte width under the given layout. Used by both
    /// the full recompute (`compute_encoded_size`) and the incremental
    /// `bump_size_*` helpers so the two paths can never disagree.
    #[inline]
    fn entry_bytes_in_layout(
        entry: &IndexEntry,
        key_layout: u8,
        layout_aux: Option<LayoutAux>,
    ) -> usize {
        match key_layout {
            KEY_LAYOUT_COMPACT_TIME_U32 => compact_time_entry_bytes(entry.row_id),
            KEY_LAYOUT_COMPACT_DATE_U16 => compact_date_entry_bytes(entry.row_id),
            KEY_LAYOUT_COMPACT_DECIMAL_I32 => compact_decimal_i32_entry_bytes(entry.row_id),
            KEY_LAYOUT_COMPACT_DECIMAL_I64 => compact_decimal_i64_entry_bytes(entry.row_id),
            KEY_LAYOUT_COMPACT_INT_U16 => compact_int_u16_entry_bytes(entry.row_id),
            KEY_LAYOUT_COMPACT_INT_U32 => compact_int_u32_entry_bytes(entry.row_id),
            KEY_LAYOUT_COMPACT_UINT_U16 => compact_uint_u16_entry_bytes(entry.row_id),
            KEY_LAYOUT_COMPACT_UINT_U32 => compact_uint_u32_entry_bytes(entry.row_id),
            KEY_LAYOUT_COMPACT_INT2 => {
                let (w1, w2) = match layout_aux {
                    Some(a) => (a.width1, a.width2),
                    None => (4, 4),
                };
                compact_int2_entry_bytes(w1, w2, entry.row_id)
            }
            _ => encoded_index_entry_len(entry),
        }
    }

    /// Full recompute. O(N). Called only on construction and on the
    /// rare layout-demote path; the hot insert/delete path uses the
    /// incremental `bump_size_*` helpers instead.
    fn compute_encoded_size(
        entries: &[IndexEntry],
        key_layout: u8,
        layout_aux: Option<LayoutAux>,
    ) -> usize {
        let mut prefix_bytes = 0usize;
        if key_layout == KEY_LAYOUT_COMPACT_INT2 {
            prefix_bytes = COMPACT_INT2_PREFIX_BYTES;
        }
        let per_entry_bytes: usize = entries
            .iter()
            .map(|e| Self::entry_bytes_in_layout(e, key_layout, layout_aux))
            .sum();
        LEAF_HEADER_BYTES + prefix_bytes + per_entry_bytes + entries.len() * SLOT_OFFSET_BYTES
    }

    /// Recompute the cache from the current entry set + layout. Called
    /// after layout demote or after any operation that's too complex to
    /// adjust incrementally.
    #[inline]
    fn recompute_cached_size(&mut self) {
        self.cached_size =
            Self::compute_encoded_size(&self.entries, self.key_layout, self.layout_aux);
    }

    /// Delta one entry's contribution onto the cache after an insert.
    /// Caller must have already pushed/inserted `entry` into `self.entries`.
    #[inline]
    fn bump_size_for_insert(&mut self, entry: &IndexEntry) {
        let entry_bytes = Self::entry_bytes_in_layout(entry, self.key_layout, self.layout_aux);
        self.cached_size = self
            .cached_size
            .saturating_add(entry_bytes + SLOT_OFFSET_BYTES);
    }

    /// Subtract one entry's contribution from the cache after a delete.
    /// Caller passes the removed entry (still valid before drop).
    #[inline]
    fn bump_size_for_remove(&mut self, entry: &IndexEntry) {
        let entry_bytes = Self::entry_bytes_in_layout(entry, self.key_layout, self.layout_aux);
        self.cached_size = self
            .cached_size
            .saturating_sub(entry_bytes + SLOT_OFFSET_BYTES);
    }
}

#[derive(Debug, Clone)]
struct IndexInternalPage {
    keys: Vec<IndexEntry>,
    children: Vec<u64>,
    /// v13 internal pages also dispatch on `key_layout` / `base_time`,
    /// mirroring the leaf encoding. Single-column INT/DECIMAL/DATE/
    /// DATETIME separators ride the same compact-delta paths as leaf
    /// entries, lifting internal fanout 30-40 % for INT-heavy indexes
    /// (the dominant OLTP PK shape). Generic layout falls back to the
    /// pre-v13 per-entry encoding.
    key_layout: u8,
    base_time: i64,
    /// v14: layout-specific extras (currently only INT2 uses this).
    layout_aux: Option<LayoutAux>,
}

impl IndexInternalPage {
    /// Construct an internal page and immediately resolve the densest
    /// viable `key_layout` for its keys. This keeps the in-memory
    /// representation (cached by `write_page`'s buffer-pool entry) in
    /// sync with what `encode_index_tree_page` writes to disk —
    /// otherwise the cached `IndexPage` reports `GENERIC` while the
    /// on-disk page is compact, breaking decode-cache invariants.
    fn new_generic(keys: Vec<IndexEntry>, children: Vec<u64>) -> Self {
        let (key_layout, base_time, layout_aux) =
            resolve_entry_layout_full(KEY_LAYOUT_GENERIC, 0, None, &keys);
        Self {
            keys,
            children,
            key_layout,
            base_time,
            layout_aux,
        }
    }
}

#[derive(Debug, Clone)]
struct IndexFreePage {
    next_free: Option<u64>,
}

#[derive(Debug, Clone)]
enum IndexPage {
    Leaf(IndexLeafPage),
    Internal(IndexInternalPage),
    Free(IndexFreePage),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct IndexEntry {
    key: IndexKey,
    row_id: RowId,
}

/// An immutable, validated nondecreasing interval. Passing this proof to the
/// leaf merger prevents unvalidated slices from bypassing its routing/order
/// assumptions, without repeating comparisons under each page latch.
#[derive(Clone, Copy)]
struct SortedIndexEntryRun<'a>(&'a [IndexEntry]);

/// Every entry routes to the latched leaf under the current topology guard.
/// Construct only from a bounded parent descent or a verified rightmost leaf.
/// Unlike a sorted prefix alone, this proof permits extending an observed
/// maximum on a non-rightmost leaf, up to its parent's separator.
#[derive(Clone, Copy)]
struct RoutedIndexEntryRun<'a>(SortedIndexEntryRun<'a>);

/// A batch-local routing proof, bounded by tree height rather than row count.
/// The interval refers to the immutable sorted input, never to cloned keys.
struct InsertRouteFrame {
    pinned: PinnedFrame,
    epoch: u64,
    start: usize,
    end: usize,
    child: Option<u64>,
    search_end: usize,
    next_slot: usize,
}

impl<'a> SortedIndexEntryRun<'a> {
    fn strictly_sorted(entries: &'a [IndexEntry]) -> Option<Self> {
        index_entries_are_strictly_sorted(entries).then_some(Self(entries))
    }

    fn prefix(entries: &'a [IndexEntry]) -> Self {
        let len = entries
            .windows(2)
            .position(|pair| pair[0] > pair[1])
            .map_or(entries.len(), |pos| pos + 1);
        Self(&entries[..len])
    }

    fn suffix(self, start: usize) -> Self {
        Self(&self.0[start..])
    }
}

/// Validate the `(key, RowId)` order of an incoming batch. Single fixed-scalar
/// indexes stay entirely in the inline `KeyPart` lane and compare their scalar
/// payload directly; composite, collated, and mixed shapes retain
/// `IndexEntry::cmp` as the correctness oracle.
#[inline]
fn index_entries_are_strictly_sorted(entries: &[IndexEntry]) -> bool {
    entries.windows(2).all(|pair| {
        let scalar_order = match (pair[0].key.0.as_slice(), pair[1].key.0.as_slice()) {
            ([KeyPart::Int(left)], [KeyPart::Int(right)])
            | ([KeyPart::Time(left)], [KeyPart::Time(right)])
            | ([KeyPart::DateTime(left)], [KeyPart::DateTime(right)]) => Some(left.cmp(right)),
            ([KeyPart::UInt(left)], [KeyPart::UInt(right)])
            | ([KeyPart::Float64(left)], [KeyPart::Float64(right)]) => Some(left.cmp(right)),
            ([KeyPart::Float32(left)], [KeyPart::Float32(right)]) => Some(left.cmp(right)),
            ([KeyPart::Date(left)], [KeyPart::Date(right)]) => Some(left.cmp(right)),
            ([KeyPart::Year(left)], [KeyPart::Year(right)]) => Some(left.cmp(right)),
            _ => None,
        };
        match scalar_order {
            Some(std::cmp::Ordering::Less) => true,
            Some(std::cmp::Ordering::Equal) => pair[0].row_id < pair[1].row_id,
            Some(std::cmp::Ordering::Greater) => false,
            None => pair[0] < pair[1],
        }
    })
}

/// Locate the old entry and the slot the replacement will occupy after the
/// old entry is removed. Searching before mutation lets the hot UPDATE path
/// reject duplicates without a remove/insert rollback.
#[inline]
fn leaf_reposition_positions(
    entries: &[IndexEntry],
    old_entry: &IndexEntry,
    new_entry: &IndexEntry,
) -> Option<(usize, usize)> {
    // Single-integer secondary indexes are the dominant OLTP shape. Compare
    // their scalar payload and RowId directly instead of walking
    // IndexKey::from(Vec<KeyPart>) through the generic derived Ord implementation
    // for both searches. Mixed/composite keys retain the generic oracle.
    let search = |target: &IndexEntry| match target.key.0.as_slice() {
        [KeyPart::Int(target_key)] => {
            entries.binary_search_by(|entry| match entry.key.0.as_slice() {
                [KeyPart::Int(key)] => key
                    .cmp(target_key)
                    .then_with(|| entry.row_id.cmp(&target.row_id)),
                _ => entry.cmp(target),
            })
        }
        [KeyPart::UInt(target_key)] => {
            entries.binary_search_by(|entry| match entry.key.0.as_slice() {
                [KeyPart::UInt(key)] => key
                    .cmp(target_key)
                    .then_with(|| entry.row_id.cmp(&target.row_id)),
                _ => entry.cmp(target),
            })
        }
        _ => entries.binary_search(target),
    };
    let old_pos = search(old_entry).ok()?;
    // Near-key updates are common beyond benchmarks too (counters, sequence
    // numbers, timestamps). Check the adjacent routing interval before paying
    // for a second full binary search. Arbitrary far moves retain the generic
    // binary-search oracle.
    let insertion_pos = if new_entry > old_entry
        && old_pos + 1 < entries.len()
        && new_entry < &entries[old_pos + 1]
    {
        old_pos + 1
    } else if new_entry < old_entry && old_pos > 0 && new_entry > &entries[old_pos - 1] {
        old_pos
    } else {
        match search(new_entry) {
            Ok(_) => return None,
            Err(pos) => pos,
        }
    };
    let target_pos = if insertion_pos > old_pos {
        insertion_pos - 1
    } else {
        insertion_pos
    };
    Some((old_pos, target_pos))
}

/// Replace one sorted leaf entry while preserving order with a single slice
/// rotation. `Vec::remove` followed by `Vec::insert` shifts the intervening
/// entries twice; UPDATE keeps cardinality constant, so one rotation is enough.
#[inline]
fn apply_leaf_reposition(
    entries: &mut [IndexEntry],
    old_pos: usize,
    target_pos: usize,
    new_entry: &IndexEntry,
) -> IndexEntry {
    let removed = std::mem::replace(&mut entries[old_pos], new_entry.clone());
    if target_pos > old_pos {
        entries[old_pos..=target_pos].rotate_left(1);
    } else if target_pos < old_pos {
        entries[target_pos..=old_pos].rotate_right(1);
    }
    removed
}

/// Undo [`apply_leaf_reposition`] after an in-place size/layout check declines
/// the mutation. The operation-local oracle must remain logically equivalent
/// to its pre-call state when the closure reports `modified=false`.
#[inline]
fn revert_leaf_reposition(
    entries: &mut [IndexEntry],
    old_pos: usize,
    target_pos: usize,
    removed: IndexEntry,
) {
    if target_pos > old_pos {
        entries[old_pos..=target_pos].rotate_right(1);
    } else if target_pos < old_pos {
        entries[target_pos..=old_pos].rotate_left(1);
    }
    entries[old_pos] = removed;
}

#[cfg(test)]
mod leaf_reposition_tests {
    use super::*;

    fn entry(key: i64, row: u16) -> IndexEntry {
        IndexEntry {
            key: IndexKey::from(vec![KeyPart::Int(key)]),
            row_id: RowId {
                page_id: 1,
                slot_id: row,
            },
        }
    }

    fn assert_reposition(mut entries: Vec<IndexEntry>, old: IndexEntry, new: IndexEntry) {
        let original = entries.clone();
        let (old_pos, target_pos) =
            leaf_reposition_positions(&entries, &old, &new).expect("valid replacement");
        let removed = apply_leaf_reposition(&mut entries, old_pos, target_pos, &new);
        assert_eq!(removed, old);
        assert!(entries.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(!entries.contains(&old));
        assert!(entries.contains(&new));
        revert_leaf_reposition(&mut entries, old_pos, target_pos, removed);
        assert_eq!(entries, original);
    }

    #[test]
    fn leaf_reposition_moves_forward_once_and_reverts() {
        let entries = vec![entry(1, 1), entry(2, 2), entry(3, 3), entry(4, 4)];
        assert_reposition(entries, entry(2, 2), entry(4, 2));
    }

    #[test]
    fn leaf_reposition_moves_backward_once_and_reverts() {
        let entries = vec![entry(1, 1), entry(2, 2), entry(3, 3), entry(4, 4)];
        assert_reposition(entries, entry(4, 4), entry(2, 4));
    }

    #[test]
    fn leaf_reposition_can_keep_the_same_slot() {
        let entries = vec![entry(10, 1), entry(20, 2), entry(30, 3)];
        assert_reposition(entries, entry(20, 2), entry(21, 2));
    }

    #[test]
    fn leaf_reposition_rejects_duplicate_without_mutation() {
        let entries = vec![entry(1, 1), entry(2, 2), entry(3, 3)];
        assert!(leaf_reposition_positions(&entries, &entry(2, 2), &entry(3, 3)).is_none());
    }
}

#[cfg(test)]
mod page_max_trx_id_tests {
    //! Phase 1a coverage for the `page_max_trx_id` field on
    //! `IndexLeafPage`: encoders write it inside the leaf payload
    //! header at `LEAF_OFF_PAGE_MAX_TRX_ID`, decoders read it back,
    //! and the byte-level `parse_leaf_payload_header` exposes it
    //! without a full decode. Phase 1b wires the writer (via the
    //! `write_page` choke-point stamp); Phase 2 will add the
    //! `view.sees(v)` shortcut in the visibility filter.
    //!
    //! The field intentionally lives inside the leaf payload
    //! (not the common index header) because the common header
    //! offset 8..16 is claimed by the page LSN — an earlier draft
    //! that put page_max_trx_id there was silently overwritten by
    //! `set_page_lsn_if_formatted` on every FPI write.
    use super::*;

    fn entry(key: i64, slot: u16) -> IndexEntry {
        IndexEntry {
            key: IndexKey::from(vec![KeyPart::Int(key)]),
            row_id: RowId {
                page_id: 1,
                slot_id: slot,
            },
        }
    }

    #[test]
    fn fresh_leaf_defaults_page_max_trx_id_to_zero() {
        let leaf = IndexLeafPage::new(vec![entry(1, 1)], None);
        assert_eq!(leaf.page_max_trx_id(), 0);
    }

    #[test]
    fn raise_is_monotonic() {
        let mut leaf = IndexLeafPage::new(vec![], None);
        leaf.raise_page_max_trx_id(10);
        assert_eq!(leaf.page_max_trx_id(), 10);
        // Lower txn ids must NOT overwrite — the field is a max bound.
        leaf.raise_page_max_trx_id(5);
        assert_eq!(leaf.page_max_trx_id(), 10);
        // Higher txn ids do advance it.
        leaf.raise_page_max_trx_id(42);
        assert_eq!(leaf.page_max_trx_id(), 42);
        // Equal is a no-op observable via the getter.
        leaf.raise_page_max_trx_id(42);
        assert_eq!(leaf.page_max_trx_id(), 42);
    }

    #[test]
    fn set_overrides_regardless_of_direction() {
        // `set_page_max_trx_id` is used by the decoder to install the
        // on-disk value. It replaces unconditionally, unlike `raise`.
        let mut leaf = IndexLeafPage::new(vec![], None);
        leaf.set_page_max_trx_id(100);
        assert_eq!(leaf.page_max_trx_id(), 100);
        leaf.set_page_max_trx_id(1);
        assert_eq!(leaf.page_max_trx_id(), 1);
    }

    fn roundtrip_leaf_preserves_page_max(entries: Vec<IndexEntry>, stamp: u64) {
        let mut leaf = IndexLeafPage::new_with_resolved_layout(entries.clone(), None);
        leaf.set_page_max_trx_id(stamp);
        let page = IndexPage::Leaf(leaf);
        let buf = encode_index_tree_page(&page).expect("encode");
        assert_eq!(buf.len(), PAGE_SIZE);
        // Leaf-payload writer bound slot must contain exactly what
        // we asked for. Payload starts at INDEX_HEADER_BYTES; the
        // field sits at LEAF_OFF_PAGE_MAX_TRX_ID inside it.
        assert_eq!(
            read_u64(&buf, INDEX_HEADER_BYTES + LEAF_OFF_PAGE_MAX_TRX_ID),
            stamp,
            "encoder must persist page_max_trx_id at LEAF_OFF_PAGE_MAX_TRX_ID",
        );
        let decoded = decode_index_tree_page(&buf).expect("decode");
        let IndexPage::Leaf(decoded_leaf) = decoded else {
            panic!("expected leaf variant after roundtrip");
        };
        assert_eq!(decoded_leaf.page_max_trx_id(), stamp);
        assert_eq!(decoded_leaf.entries.len(), entries.len());
        // Byte-level parser must agree without doing the full decode.
        let hdr = parse_leaf_payload_header(&buf).expect("parse header");
        assert_eq!(hdr.page_max_trx_id, stamp);
    }

    #[test]
    fn roundtrip_empty_leaf() {
        roundtrip_leaf_preserves_page_max(vec![], 0);
        roundtrip_leaf_preserves_page_max(vec![], 1);
        roundtrip_leaf_preserves_page_max(vec![], u64::MAX);
    }

    #[test]
    fn roundtrip_generic_layout_leaf() {
        let entries = vec![entry(-5, 1), entry(3, 2), entry(1_000_000, 3)];
        roundtrip_leaf_preserves_page_max(entries, 7);
    }

    #[test]
    fn roundtrip_compact_int_layout_leaf() {
        // Sequential integers fall into KEY_LAYOUT_COMPACT_INT_U16 —
        // exercise the compact path so we know the writer bound
        // survives every leaf layout, not just generic.
        let entries: Vec<IndexEntry> = (0..64).map(|i| entry(1_000 + i, i as u16)).collect();
        roundtrip_leaf_preserves_page_max(entries, 314_159);
    }

    #[test]
    fn roundtrip_extreme_values() {
        let entries = vec![entry(1, 1)];
        roundtrip_leaf_preserves_page_max(entries.clone(), 1);
        roundtrip_leaf_preserves_page_max(entries.clone(), u64::MAX - 1);
        roundtrip_leaf_preserves_page_max(entries, u64::MAX);
    }

    #[test]
    fn internal_and_free_pages_dont_reserve_page_max_trx_id() {
        // `page_max_trx_id` is a leaf-only field (per
        // `LEAF_OFF_PAGE_MAX_TRX_ID` docs). Internal / free page
        // encodings have their own payload layouts and don't touch
        // the leaf payload's slot. Round-trip both to confirm the
        // encoding paths still work after the leaf-payload growth.
        let internal =
            IndexInternalPage::new_generic(vec![entry(10, 0), entry(20, 0)], vec![100, 101, 102]);
        let internal_buf =
            encode_index_tree_page(&IndexPage::Internal(internal)).expect("encode internal page");
        assert_eq!(internal_buf.len(), PAGE_SIZE);
        // Common-header offset 8..16 is the page LSN slot (zero
        // until `set_page_lsn_if_formatted` stamps it), NOT the
        // writer bound. This assertion documents the layout so a
        // future reader can't confuse the two.
        assert_eq!(read_u64(&internal_buf, 8), 0, "common header LSN slot");

        let free = IndexFreePage {
            next_free: Some(42),
        };
        let free_buf = encode_index_tree_page(&IndexPage::Free(free)).expect("encode free page");
        assert_eq!(free_buf.len(), PAGE_SIZE);
        assert_eq!(read_u64(&free_buf, 8), 0, "common header LSN slot");
    }

    #[test]
    fn meta_page_roundtrips_unchanged() {
        // Meta pages don't have a leaf payload so they're unaffected
        // by the writer-bound field addition. Same guard as for
        // internal/free — LSN slot at offset 8..16 stays zero pre-stamp.
        let meta = IndexMeta {
            root_page: 1,
            next_page_id: 2,
            free_page_head: None,
        };
        let buf = encode_index_meta(&meta);
        assert_eq!(read_u64(&buf, 8), 0, "meta page LSN slot at encode time");
        let decoded = decode_index_meta(&buf).expect("decode meta");
        assert_eq!(decoded.root_page, 1);
        assert_eq!(decoded.next_page_id, 2);
        assert!(decoded.free_page_head.is_none());
    }

    #[test]
    fn demote_preserves_page_max_trx_id() {
        // `demote_page_if_unfit_owned` re-picks the leaf layout when
        // the cached one no longer fits; the writer bound must survive
        // the transformation.
        let entries: Vec<IndexEntry> = (0..8).map(|i| entry(i, i as u16)).collect();
        let mut leaf = IndexLeafPage::new_with_resolved_layout(entries, None);
        leaf.set_page_max_trx_id(999);
        let page = IndexPage::Leaf(leaf);
        let demoted = demote_page_if_unfit_owned(&page);
        let IndexPage::Leaf(demoted_leaf) = demoted else {
            panic!("expected leaf after demote");
        };
        assert_eq!(demoted_leaf.page_max_trx_id(), 999);
    }

    #[test]
    fn checksum_covers_page_max_trx_id() {
        // Tamper with the slot after encoding; the checksum must
        // reject the page. Without this guarantee, a corrupted writer
        // bound would silently mislead the Phase 2 shortcut.
        let mut leaf = IndexLeafPage::new(vec![entry(1, 1)], None);
        leaf.set_page_max_trx_id(50);
        let mut buf = encode_index_tree_page(&IndexPage::Leaf(leaf)).expect("encode");
        // Flip the stored bound at its payload-relative offset.
        let absolute_off = INDEX_HEADER_BYTES + LEAF_OFF_PAGE_MAX_TRX_ID;
        write_u64(&mut buf, absolute_off, 51);
        let err = decode_index_tree_page(&buf).expect_err("checksum should fail");
        assert!(
            err.message().contains("checksum"),
            "unexpected error: {}",
            err.message(),
        );
    }
}

#[cfg(test)]
mod raw_internal_route_tests {
    use super::*;

    fn row(page_id: u32, slot_id: u16) -> RowId {
        RowId { page_id, slot_id }
    }

    fn assert_routes(keys: Vec<IndexEntry>, probes: Vec<(IndexKey, RowId)>) {
        let children: Vec<u64> = (0..=keys.len()).map(|idx| 100 + idx as u64).collect();
        let page = IndexInternalPage::new_generic(keys, children);
        let encoded = encode_index_tree_page(&IndexPage::Internal(page.clone())).unwrap();
        validate_index_page_header(&encoded).unwrap();
        for (key, row_id) in probes {
            let expected_idx = internal_child_index_by_key(&page, &key, row_id);
            assert_eq!(
                route_index_page_bytes(&encoded, &key, row_id).unwrap(),
                RawIndexPageRoute::Child {
                    page_id: page.children[expected_idx],
                    child_idx: expected_idx,
                },
                "raw routing diverged for key {key:?}, row_id {row_id:?}, layout {}",
                page.key_layout,
            );
        }
    }

    #[test]
    fn raw_route_matches_decoded_for_compact_int_and_row_id_ties() {
        let keys = vec![10, 20, 30, 40]
            .into_iter()
            .enumerate()
            .map(|(idx, value)| IndexEntry {
                key: IndexKey::from(vec![KeyPart::Int(value)]),
                row_id: row(7, idx as u16 + 1),
            })
            .collect();
        assert_routes(
            keys,
            vec![
                (IndexKey::from(vec![KeyPart::Int(5)]), row(0, 0)),
                (IndexKey::from(vec![KeyPart::Int(20)]), row(0, 0)),
                (IndexKey::from(vec![KeyPart::Int(20)]), row(7, 2)),
                (IndexKey::from(vec![KeyPart::Int(25)]), row(0, 0)),
                (
                    IndexKey::from(vec![KeyPart::Int(50)]),
                    row(u32::MAX, u16::MAX),
                ),
            ],
        );
    }

    #[test]
    fn raw_leaf_entry_scalar_compare_matches_decoded_ordering() {
        for keys in [
            vec![
                IndexEntry {
                    key: IndexKey::from(vec![KeyPart::Int(10)]),
                    row_id: row(3, 1),
                },
                IndexEntry {
                    key: IndexKey::from(vec![KeyPart::Int(20)]),
                    row_id: row(3, 2),
                },
            ],
            vec![
                IndexEntry {
                    key: IndexKey::from(vec![KeyPart::Int(1), KeyPart::Int(10)]),
                    row_id: row(4, 1),
                },
                IndexEntry {
                    key: IndexKey::from(vec![KeyPart::Int(2), KeyPart::Int(20)]),
                    row_id: row(4, 2),
                },
            ],
        ] {
            let leaf = IndexLeafPage::new_with_resolved_layout(keys.clone(), None);
            assert_ne!(leaf.key_layout, KEY_LAYOUT_GENERIC);
            let encoded = encode_index_tree_page(&IndexPage::Leaf(leaf)).unwrap();
            let header = parse_leaf_payload_header(&encoded).unwrap();
            for (slot, stored) in keys.iter().enumerate() {
                for target in [
                    IndexEntry {
                        key: stored.key.clone(),
                        row_id: RowId {
                            page_id: stored.row_id.page_id,
                            slot_id: stored.row_id.slot_id.saturating_sub(1),
                        },
                    },
                    stored.clone(),
                    IndexEntry {
                        key: stored.key.clone(),
                        row_id: RowId {
                            page_id: stored.row_id.page_id,
                            slot_id: stored.row_id.slot_id.saturating_add(1),
                        },
                    },
                ] {
                    assert_eq!(
                        compare_raw_leaf_slot_to_entry(&header, slot, &target).unwrap(),
                        stored.cmp(&target),
                    );
                }
            }
        }
    }

    #[test]
    fn raw_last_leaf_append_gate_matches_decoded_entry_order() {
        for keys in [
            vec![
                IndexEntry {
                    key: IndexKey::from(vec![KeyPart::Int(10), KeyPart::Int(1)]),
                    row_id: row(5, 1),
                },
                IndexEntry {
                    key: IndexKey::from(vec![KeyPart::Int(20), KeyPart::Int(2)]),
                    row_id: row(5, 2),
                },
            ],
            vec![
                IndexEntry {
                    key: IndexKey::from(vec![KeyPart::String("a".into())]),
                    row_id: row(6, 1),
                },
                IndexEntry {
                    key: IndexKey::from(vec![KeyPart::String("z".into())]),
                    row_id: row(6, 2),
                },
            ],
        ] {
            let leaf = IndexLeafPage::new_with_resolved_layout(keys.clone(), None);
            let encoded = encode_index_tree_page(&IndexPage::Leaf(leaf)).unwrap();
            let last = keys.last().unwrap();
            let candidates = [
                IndexEntry {
                    key: last.key.clone(),
                    row_id: row(last.row_id.page_id, last.row_id.slot_id.saturating_sub(1)),
                },
                last.clone(),
                IndexEntry {
                    key: last.key.clone(),
                    row_id: row(last.row_id.page_id, last.row_id.slot_id.saturating_add(1)),
                },
            ];
            for candidate in candidates {
                assert_eq!(
                    raw_last_leaf_entry_less_than(&encoded, &candidate).unwrap(),
                    last < &candidate,
                );
            }
        }

        let empty = encode_index_tree_page(&IndexPage::Leaf(
            IndexLeafPage::new_with_resolved_layout(Vec::new(), None),
        ))
        .unwrap();
        let candidate = IndexEntry {
            key: IndexKey::from(vec![KeyPart::Int(1)]),
            row_id: row(1, 1),
        };
        assert!(raw_last_leaf_entry_less_than(&empty, &candidate).unwrap());
    }

    #[test]
    fn raw_sorted_batch_route_matches_independent_point_routes() {
        fn check(keys: Vec<IndexEntry>, mut probes: Vec<IndexKey>) {
            let children: Vec<u64> = (0..=keys.len()).map(|idx| 700 + idx as u64).collect();
            let page = IndexInternalPage::new_generic(keys, children);
            let encoded = encode_index_tree_page(&IndexPage::Internal(page)).unwrap();
            validate_index_page_header(&encoded).unwrap();
            probes.sort_unstable();
            probes.dedup();

            let row_id = min_lookup_row_id();
            let ranges =
                route_index_page_key_ranges_bytes(&encoded, &probes, 0, probes.len(), row_id)
                    .unwrap()
                    .expect("internal page must return child ranges");
            let mut routed = vec![0_u64; probes.len()];
            for range in ranges {
                for child in &mut routed[range.start..range.end] {
                    *child = range.leaf_id;
                }
            }
            for (idx, key) in probes.iter().enumerate() {
                let RawIndexPageRoute::Child { page_id, .. } =
                    route_index_page_bytes(&encoded, key, row_id).unwrap()
                else {
                    panic!("internal page routed as a leaf");
                };
                assert_eq!(routed[idx], page_id, "batch route diverged for {key:?}");
            }
        }

        let scalar_keys = [10, 20, 30, 40]
            .into_iter()
            .enumerate()
            .map(|(idx, value)| IndexEntry {
                key: IndexKey::from(vec![KeyPart::Int(value)]),
                row_id: row(7, idx as u16 + 1),
            })
            .collect();
        check(
            scalar_keys,
            [5, 10, 11, 19, 20, 21, 39, 40, 50]
                .into_iter()
                .map(|value| IndexKey::from(vec![KeyPart::Int(value)]))
                .collect(),
        );

        let generic_keys = [(1, "a"), (2, "m"), (3, "z")]
            .into_iter()
            .enumerate()
            .map(|(idx, (a, b))| IndexEntry {
                key: IndexKey::from(vec![KeyPart::Int(a), KeyPart::String(b.into())]),
                row_id: row(9, idx as u16),
            })
            .collect();
        check(
            generic_keys,
            [(0, "z"), (1, "a"), (1, "b"), (2, "m"), (4, "")]
                .into_iter()
                .map(|(a, b)| IndexKey::from(vec![KeyPart::Int(a), KeyPart::String(b.into())]))
                .collect(),
        );
    }

    #[test]
    fn raw_first_entry_step_matches_leaf_and_internal_oracles() {
        let entries: Vec<IndexEntry> = [(3, 7), (3, 9), (4, 1)]
            .into_iter()
            .enumerate()
            .map(|(idx, (a, b))| IndexEntry {
                key: IndexKey::from(vec![KeyPart::Int(a), KeyPart::Int(b)]),
                row_id: row(11, idx as u16 + 1),
            })
            .collect();
        let leaf = IndexLeafPage::new_with_resolved_layout(entries.clone(), None);
        let leaf_bytes = encode_index_tree_page(&IndexPage::Leaf(leaf)).unwrap();
        let RawFirstEntryStep::Leaf(first) = raw_first_entry_step(&leaf_bytes).unwrap() else {
            panic!("expected leaf step");
        };
        assert_eq!(first, entries.first().cloned());

        let empty_bytes = encode_index_tree_page(&IndexPage::Leaf(
            IndexLeafPage::new_with_resolved_layout(Vec::new(), None),
        ))
        .unwrap();
        let RawFirstEntryStep::Leaf(first) = raw_first_entry_step(&empty_bytes).unwrap() else {
            panic!("expected empty leaf step");
        };
        assert!(first.is_none());

        let internal = IndexInternalPage::new_generic(entries, vec![70, 71, 72, 73]);
        let internal_bytes = encode_index_tree_page(&IndexPage::Internal(internal)).unwrap();
        let RawFirstEntryStep::Child(child) = raw_first_entry_step(&internal_bytes).unwrap() else {
            panic!("expected internal step");
        };
        assert_eq!(child, 70);
    }

    #[test]
    fn raw_internal_separator_replace_matches_decoded_oracle() {
        let keys: Vec<IndexEntry> = [(1, 1), (1, 9), (2, 3), (10, 4)]
            .into_iter()
            .enumerate()
            .map(|(idx, (a, b))| IndexEntry {
                key: IndexKey::from(vec![KeyPart::Int(a), KeyPart::Int(b)]),
                row_id: row(8, idx as u16),
            })
            .collect();
        let children = vec![100, 101, 102, 103, 104];
        let page = IndexInternalPage::new_generic(keys.clone(), children.clone());
        assert_eq!(page.key_layout, KEY_LAYOUT_COMPACT_INT2);
        let mut encoded = encode_index_tree_page(&IndexPage::Internal(page)).unwrap();
        let replacement = IndexEntry {
            key: IndexKey::from(vec![KeyPart::Int(1), KeyPart::Int(8)]),
            row_id: keys[1].row_id,
        };

        assert_eq!(
            try_replace_raw_internal_separator(&mut encoded, 1, &replacement).unwrap(),
            Some(()),
        );
        refresh_page_checksum(&mut encoded);
        validate_index_page_header(&encoded).unwrap();
        let IndexPage::Internal(decoded) = decode_index_tree_page(&encoded).unwrap() else {
            panic!("expected internal page");
        };
        let mut expected = keys;
        expected[1] = replacement;
        assert_eq!(decoded.keys, expected);
        assert_eq!(decoded.children, children);
    }

    #[test]
    fn raw_internal_separator_replace_defers_length_and_layout_changes() {
        let generic_keys: Vec<IndexEntry> = ["aa", "bb", "cc"]
            .into_iter()
            .enumerate()
            .map(|(idx, value)| IndexEntry {
                key: IndexKey::from(vec![KeyPart::String(value.to_string())]),
                row_id: row(9, idx as u16),
            })
            .collect();
        let generic = IndexInternalPage::new_generic(generic_keys.clone(), vec![10, 11, 12, 13]);
        assert_eq!(generic.key_layout, KEY_LAYOUT_GENERIC);
        let mut generic_bytes = encode_index_tree_page(&IndexPage::Internal(generic)).unwrap();
        let generic_before = generic_bytes.clone();
        let longer = IndexEntry {
            key: IndexKey::from(vec![KeyPart::String("much-longer".to_string())]),
            row_id: generic_keys[1].row_id,
        };
        assert_eq!(
            try_replace_raw_internal_separator(&mut generic_bytes, 1, &longer).unwrap(),
            None,
        );
        assert_eq!(generic_bytes, generic_before);

        let compact_keys: Vec<IndexEntry> = [10, 20, 30]
            .into_iter()
            .enumerate()
            .map(|(idx, value)| IndexEntry {
                key: IndexKey::from(vec![KeyPart::Int(value)]),
                row_id: row(10, idx as u16),
            })
            .collect();
        let compact = IndexInternalPage::new_generic(compact_keys.clone(), vec![20, 21, 22, 23]);
        assert_eq!(compact.key_layout, KEY_LAYOUT_COMPACT_INT_U16);
        let mut compact_bytes = encode_index_tree_page(&IndexPage::Internal(compact)).unwrap();
        let compact_before = compact_bytes.clone();
        let outside_layout = IndexEntry {
            key: IndexKey::from(vec![KeyPart::Int(100_000)]),
            row_id: compact_keys[1].row_id,
        };
        assert_eq!(
            try_replace_raw_internal_separator(&mut compact_bytes, 1, &outside_layout).unwrap(),
            None,
        );
        assert_eq!(compact_bytes, compact_before);
    }

    #[test]
    fn raw_route_matches_decoded_for_compact_int2_and_generic_keys() {
        let int2_keys = vec![(1, 1), (1, 9), (2, 3), (10, 4)]
            .into_iter()
            .enumerate()
            .map(|(idx, (a, b))| IndexEntry {
                key: IndexKey::from(vec![KeyPart::Int(a), KeyPart::Int(b)]),
                row_id: row(8, idx as u16),
            })
            .collect();
        assert_routes(
            int2_keys,
            vec![
                (
                    IndexKey::from(vec![KeyPart::Int(1), KeyPart::Int(0)]),
                    row(0, 0),
                ),
                (
                    IndexKey::from(vec![KeyPart::Int(1), KeyPart::Int(9)]),
                    row(8, 1),
                ),
                (
                    IndexKey::from(vec![KeyPart::Int(2), KeyPart::Int(4)]),
                    row(0, 0),
                ),
                (
                    IndexKey::from(vec![KeyPart::Int(99), KeyPart::Int(0)]),
                    row(0, 0),
                ),
            ],
        );

        let generic_keys = vec![(1, "a"), (2, "m"), (3, "z")]
            .into_iter()
            .enumerate()
            .map(|(idx, (a, b))| IndexEntry {
                key: IndexKey::from(vec![KeyPart::Int(a), KeyPart::String(b.to_string())]),
                row_id: row(9, idx as u16),
            })
            .collect();
        assert_routes(
            generic_keys,
            vec![
                (
                    IndexKey::from(vec![KeyPart::Int(0), KeyPart::String("z".into())]),
                    row(0, 0),
                ),
                (
                    IndexKey::from(vec![KeyPart::Int(2), KeyPart::String("m".into())]),
                    row(9, 1),
                ),
                (
                    IndexKey::from(vec![KeyPart::Int(4), KeyPart::String(String::new())]),
                    row(0, 0),
                ),
            ],
        );
    }

    #[test]
    fn numeric_composite_projection_matches_full_key_decoder() {
        let parts = vec![
            KeyPart::Int(i64::MIN),
            KeyPart::UInt(u64::MAX),
            KeyPart::Date(-1),
            KeyPart::String("中文 and Русский".into()),
            KeyPart::Null,
            KeyPart::DateTime(i64::MAX),
        ];
        let leaf = IndexLeafPage::new(
            vec![IndexEntry {
                key: IndexKey::from(parts.clone()),
                row_id: row(11, 0),
            }],
            None,
        );
        let encoded = encode_index_tree_page(&IndexPage::Leaf(leaf)).unwrap();
        let header = parse_leaf_payload_header(&encoded).unwrap();
        for position in [0, 1, 2, 4, 5] {
            let mut cursor = header.read_slot_offset(0);
            let actual = header
                .decode_numeric_part_at(&mut cursor, position)
                .unwrap();
            assert_eq!(actual, parts[position]);
            assert_eq!(
                decode_row_id_v7_from(header.payload, &mut cursor, header.page_table_id).unwrap(),
                row(11, 0)
            );
        }
        let mut cursor = header.read_slot_offset(0);
        assert!(header
            .decode_numeric_part_at(&mut cursor, parts.len())
            .is_err());
        // An invalid unprojected string must remain an error.
        let mut corrupted = encoded.clone();
        let text_at = corrupted
            .windows("中文".len())
            .position(|bytes| bytes == "中文".as_bytes())
            .unwrap();
        corrupted[text_at] = 0xff;
        let mut header = header;
        header.payload = &corrupted;
        let mut cursor = header.read_slot_offset(0);
        assert!(header.decode_numeric_part_at(&mut cursor, 1).is_err());
    }

    #[test]
    fn generic_composite_leaf_exact_lookup_compares_in_place() {
        let entries: Vec<IndexEntry> = [(1, 1, 1), (1, 2, 3), (2, 1, 1)]
            .into_iter()
            .enumerate()
            .map(|(idx, (a, b, c))| IndexEntry {
                key: IndexKey::from(vec![KeyPart::Int(a), KeyPart::Int(b), KeyPart::Int(c)]),
                row_id: row(11, idx as u16),
            })
            .collect();
        let leaf = IndexLeafPage::new(entries.clone(), None);
        assert_eq!(leaf.key_layout, KEY_LAYOUT_GENERIC);
        let encoded = encode_index_tree_page(&IndexPage::Leaf(leaf)).unwrap();
        validate_index_page_header(&encoded).unwrap();

        let target = entries[1].key.clone();
        assert_eq!(
            lookup_leaf_page_bytes_one(&encoded, &target).unwrap().row,
            Some(entries[1].row_id),
        );
        assert_eq!(
            lookup_leaf_page_bytes(&encoded, &target).unwrap().rows,
            vec![entries[1].row_id],
        );
        let missing = IndexKey::from(vec![KeyPart::Int(1), KeyPart::Int(2), KeyPart::Int(4)]);
        assert_eq!(
            lookup_leaf_page_bytes_one(&encoded, &missing).unwrap().row,
            None,
        );

        // The remaining materializing decoder also keeps ordinary composite
        // keys in IndexKey's inline SmallVec storage.
        let header = parse_leaf_payload_header(&encoded).unwrap();
        let mut cursor = header.read_slot_offset(0);
        let decoded = header.decode_key_at(&mut cursor).unwrap();
        assert!(!decoded.0.spilled());
        assert_eq!(decoded, entries[0].key);
    }

    #[test]
    fn compact_int_u16_direct_slot_preserves_dense_and_sparse_lookup() {
        fn check(values: &[i64]) {
            let entries: Vec<IndexEntry> = values
                .iter()
                .enumerate()
                .map(|(idx, value)| IndexEntry {
                    key: IndexKey::from(vec![KeyPart::Int(*value)]),
                    row_id: row(41, idx as u16),
                })
                .collect();
            let leaf = IndexLeafPage::new_with_resolved_layout(entries, None);
            assert_eq!(leaf.key_layout, KEY_LAYOUT_COMPACT_INT_U16);
            let encoded = encode_index_tree_page(&IndexPage::Leaf(leaf)).unwrap();

            for target in (values[0] - 1..=values[values.len() - 1] + 1).step_by(7) {
                let expected = values
                    .binary_search(&target)
                    .ok()
                    .map(|slot| row(41, slot as u16));
                assert_eq!(
                    lookup_leaf_page_bytes_one(
                        &encoded,
                        &IndexKey::from(vec![KeyPart::Int(target)]),
                    )
                    .unwrap()
                    .row,
                    expected,
                    "lookup diverged for {target}",
                );
            }
            for (slot, target) in values.iter().enumerate() {
                assert_eq!(
                    lookup_leaf_page_bytes_one(
                        &encoded,
                        &IndexKey::from(vec![KeyPart::Int(*target)]),
                    )
                    .unwrap()
                    .row,
                    Some(row(41, slot as u16)),
                );
            }
        }

        let dense: Vec<i64> = (-300..900).collect();
        let sparse: Vec<i64> = (0..220).map(|value| -10_000 + value * value).collect();
        check(&dense);
        check(&sparse);
    }

    #[test]
    fn packed_int2_lookup_matches_tuple_oracle_for_all_widths_and_boundaries() {
        for width1 in [1u8, 2, 4] {
            for width2 in [1u8, 2, 4] {
                for base in [-100i64, i64::MIN, i64::MAX - 8] {
                    let step1 = if base > 0 {
                        1
                    } else {
                        1i64 << (8 * (width1 - 1))
                    };
                    let step2 = if base > 0 {
                        1
                    } else {
                        1i64 << (8 * (width2 - 1))
                    };
                    let mut entries = Vec::new();
                    for a in 0..4 {
                        for b in [0, 2, 4, 6] {
                            entries.push(IndexEntry {
                                key: IndexKey::from(vec![
                                    KeyPart::Int(base + a * step1),
                                    KeyPart::Int(base + b * step2),
                                ]),
                                row_id: row(51, entries.len() as u16),
                            });
                        }
                    }
                    let leaf = IndexLeafPage::new_with_layout_aux(
                        entries.clone(),
                        None,
                        KEY_LAYOUT_COMPACT_INT2,
                        base,
                        Some(LayoutAux {
                            col2_base: base,
                            width1,
                            width2,
                        }),
                    );
                    let encoded = encode_index_tree_page(&IndexPage::Leaf(leaf)).unwrap();
                    for a in -1..6 {
                        for b in -1..8 {
                            let (Some(first), Some(second)) =
                                (base.checked_add(a * step1), base.checked_add(b * step2))
                            else {
                                continue;
                            };
                            let key =
                                IndexKey::from(vec![KeyPart::Int(first), KeyPart::Int(second)]);
                            let expected = entries
                                .iter()
                                .find(|entry| entry.key == key)
                                .map(|entry| entry.row_id);
                            assert_eq!(
                                lookup_leaf_page_bytes_one(&encoded, &key).unwrap().row,
                                expected
                            );
                            assert_eq!(
                                lookup_leaf_page_bytes(&encoded, &key).unwrap().rows,
                                expected.into_iter().collect::<Vec<_>>()
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn generic_integer_lower_bound_keeps_null_and_mixed_key_ordering() {
        for arity in 1..=4 {
            let values = [i64::MIN, -1, 0, 1, i64::MAX];
            let mut entries: Vec<_> = values
                .iter()
                .enumerate()
                .map(|(slot, value)| IndexEntry {
                    key: IndexKey::from(vec![KeyPart::Int(*value); arity]),
                    row_id: row(61, slot as u16),
                })
                .collect();
            entries.push(IndexEntry {
                key: IndexKey::from(vec![KeyPart::Null; arity]),
                row_id: row(61, 8),
            });
            entries.push(IndexEntry {
                key: IndexKey::from(vec![KeyPart::UInt(0); arity]),
                row_id: row(61, 9),
            });
            entries.sort_by(|a, b| a.key.cmp(&b.key).then(a.row_id.cmp(&b.row_id)));
            let encoded =
                encode_index_tree_page(&IndexPage::Leaf(IndexLeafPage::new(entries.clone(), None)))
                    .unwrap();
            for value in [i64::MIN, -2, -1, 0, 1, 2, i64::MAX] {
                let target = IndexKey::from(vec![KeyPart::Int(value); arity]);
                let expected = entries
                    .iter()
                    .filter(|entry| entry.key == target)
                    .map(|entry| entry.row_id)
                    .collect::<Vec<_>>();
                assert_eq!(
                    lookup_leaf_page_bytes(&encoded, &target).unwrap().rows,
                    expected
                );
                assert_eq!(
                    lookup_leaf_page_bytes_one(&encoded, &target).unwrap().row,
                    expected.first().copied()
                );
            }
        }
    }

    #[test]
    fn generic_integer_tuple_fast_compare_matches_general_comparator() {
        let keys = vec![
            IndexKey::from(vec![
                KeyPart::Null,
                KeyPart::Int(9),
                KeyPart::Int(9),
                KeyPart::Int(9),
            ]),
            IndexKey::from(vec![
                KeyPart::Int(-2),
                KeyPart::Int(4),
                KeyPart::Int(7),
                KeyPart::Int(1),
            ]),
            IndexKey::from(vec![
                KeyPart::Int(1),
                KeyPart::Int(2),
                KeyPart::Int(3),
                KeyPart::Int(4),
            ]),
            IndexKey::from(vec![
                KeyPart::Int(1),
                KeyPart::Int(2),
                KeyPart::Int(5),
                KeyPart::Int(0),
            ]),
        ];
        let entries: Vec<IndexEntry> = keys
            .into_iter()
            .enumerate()
            .map(|(idx, key)| IndexEntry {
                key,
                row_id: row(12, idx as u16),
            })
            .collect();
        let leaf = IndexLeafPage::new(entries, None);
        assert_eq!(leaf.key_layout, KEY_LAYOUT_GENERIC);
        let encoded = encode_index_tree_page(&IndexPage::Leaf(leaf)).unwrap();
        validate_index_page_header(&encoded).unwrap();
        let header = parse_leaf_payload_header(&encoded).unwrap();
        let target = IndexKey::from(vec![
            KeyPart::Int(1),
            KeyPart::Int(2),
            KeyPart::Int(3),
            KeyPart::Int(4),
        ]);
        let fast = header
            .fast_scalar_target(&target)
            .expect("complete four-integer key should use the fast comparator");

        for slot in 0..header.count {
            let fast_result = header.cmp_scalar_at(slot, fast).unwrap();
            let general_result = header.cmp_key_at(slot, &target).unwrap();
            assert_eq!(fast_result, general_result);
        }
        assert_eq!(
            lookup_leaf_page_bytes_one(&encoded, &target).unwrap().row,
            Some(row(12, 2)),
        );

        let prefix = IndexKey::from(vec![KeyPart::Int(1), KeyPart::Int(2)]);
        assert!(header.fast_scalar_target(&prefix).is_none());
    }

    #[test]
    fn raw_covering_lookup_matches_exact_and_composite_prefix_entries() {
        let entries: Vec<IndexEntry> = [(1, 1, 4), (1, 2, 3), (2, 1, 2)]
            .into_iter()
            .enumerate()
            .map(|(idx, (a, b, c))| IndexEntry {
                key: IndexKey::from(vec![KeyPart::Int(a), KeyPart::Int(b), KeyPart::Int(c)]),
                row_id: row(21, idx as u16 + 1),
            })
            .collect();
        let mut leaf = IndexLeafPage::new(entries.clone(), Some(99));
        leaf.set_page_max_trx_id(77);
        let encoded = encode_index_tree_page(&IndexPage::Leaf(leaf)).unwrap();
        validate_index_page_header(&encoded).unwrap();

        let mut seen = None;
        let exact = lookup_leaf_entry_page_bytes(
            &encoded,
            &entries[1].key,
            0,
            true,
            &mut seen,
            &|_, key, row_id| (key, row_id),
        )
        .unwrap();
        assert_eq!(
            exact.entries,
            vec![(entries[1].key.clone(), entries[1].row_id)]
        );
        assert!(!exact.should_continue);
        assert_eq!(exact.page_max_trx_id, 77);

        let prefix = IndexKey::from(vec![KeyPart::Int(1)]);
        let mut seen = None;
        let prefixed = lookup_leaf_entry_page_bytes(
            &encoded,
            &prefix,
            0,
            false,
            &mut seen,
            &|_, key, row_id| (key, row_id),
        )
        .unwrap();
        assert_eq!(
            prefixed.entries,
            vec![
                (entries[0].key.clone(), entries[0].row_id),
                (entries[1].key.clone(), entries[1].row_id),
            ],
        );
        assert!(!prefixed.should_continue);
        assert_eq!(prefixed.page_max_trx_id, 77);

        let row_ids = prefix_leaf_page_bytes(&encoded, &prefix).unwrap();
        assert_eq!(row_ids.rows, vec![entries[0].row_id, entries[1].row_id]);
        assert!(!row_ids.should_continue);
    }

    #[test]
    fn raw_route_identifies_leaf_and_rejects_free_page() {
        let leaf = encode_index_tree_page(&IndexPage::Leaf(
            IndexLeafPage::new_with_resolved_layout(Vec::new(), None),
        ))
        .unwrap();
        assert_eq!(
            route_index_page_bytes(&leaf, &IndexKey::from(vec![KeyPart::Int(1)]), row(0, 0))
                .unwrap(),
            RawIndexPageRoute::Leaf,
        );

        let free =
            encode_index_tree_page(&IndexPage::Free(IndexFreePage { next_free: None })).unwrap();
        let err = route_index_page_bytes(&free, &IndexKey::from(vec![KeyPart::Int(1)]), row(0, 0))
            .unwrap_err();
        assert!(err.message().contains("free page"));
    }

    #[test]
    fn raw_candidate_leaf_hint_matches_decoded_range_oracle() {
        let entries = vec![10, 20, 30]
            .into_iter()
            .enumerate()
            .map(|(idx, value)| IndexEntry {
                key: IndexKey::from(vec![KeyPart::Int(value)]),
                row_id: row(12, idx as u16),
            })
            .collect::<Vec<_>>();
        let candidates = [
            IndexEntry {
                key: IndexKey::from(vec![KeyPart::Int(5)]),
                row_id: row(1, 0),
            },
            entries[1].clone(),
            IndexEntry {
                key: IndexKey::from(vec![KeyPart::Int(25)]),
                row_id: row(1, 0),
            },
            IndexEntry {
                key: IndexKey::from(vec![KeyPart::Int(40)]),
                row_id: row(1, 0),
            },
            IndexEntry {
                key: entries[0].key.clone(),
                row_id: row(1, 0),
            },
            IndexEntry {
                key: entries[2].key.clone(),
                row_id: row(99, 0),
            },
        ];
        for next_leaf in [Some(99), None] {
            let leaf = IndexLeafPage::new(entries.clone(), next_leaf);
            let encoded = encode_index_tree_page(&IndexPage::Leaf(leaf.clone())).unwrap();
            for candidate in &candidates {
                let within = leaf
                    .entries
                    .first()
                    .zip(leaf.entries.last())
                    .is_some_and(|(first, last)| candidate >= first && candidate <= last);
                let rightmost_extension = leaf.next_leaf.is_none()
                    && leaf.entries.last().is_some_and(|last| candidate >= last);
                assert_eq!(
                    raw_leaf_candidate_stays(&encoded, candidate).unwrap(),
                    within || rightmost_extension,
                    "candidate {candidate:?}, next_leaf {next_leaf:?}",
                );
            }
        }

        let empty = encode_index_tree_page(&IndexPage::Leaf(
            IndexLeafPage::new_with_resolved_layout(Vec::new(), None),
        ))
        .unwrap();
        assert!(!raw_leaf_candidate_stays(&empty, &candidates[0]).unwrap());
    }
}

#[cfg(test)]
mod raw_range_rows_tests {
    use super::*;

    fn row(slot_id: u16) -> RowId {
        RowId {
            page_id: 7,
            slot_id,
        }
    }

    fn int_entry(value: i64) -> IndexEntry {
        IndexEntry {
            key: IndexKey::from(vec![KeyPart::Int(value)]),
            row_id: row(value as u16),
        }
    }

    fn bound(value: i64, inclusive: bool) -> IndexBound {
        IndexBound {
            key: IndexKey::from(vec![KeyPart::Int(value)]),
            inclusive,
        }
    }

    #[test]
    fn raw_leaf_list_rows_match_keys_for_types_bounds_and_cross_page_duplicates() {
        for kind in 0..8 {
            let dir = tempfile::tempdir().unwrap();
            let log = Arc::new(parking_lot::Mutex::new(
                LogManager::open(dir.path()).unwrap(),
            ));
            let pool = BufferPool::new_with_options(
                8,
                log.clone(),
                DurabilityMode::Memory,
                Duration::ZERO,
                1,
                0,
            );
            let index = IndexFile::open_with_durability(
                dir.path().join("range.idx"),
                log,
                DurabilityMode::Memory,
                pool,
            )
            .unwrap();
            let mut entries: Vec<_> = (0..96)
                .map(|i| {
                    let parts = match kind {
                        0 => vec![KeyPart::Int(i)],
                        1 => vec![KeyPart::UInt(i as u64)],
                        2 => vec![KeyPart::Date(i as i32)],
                        3 => vec![KeyPart::Decimal {
                            value: i as i128,
                            scale: 2,
                        }],
                        4 => vec![KeyPart::String(format!("key-{i:03}"))],
                        5 => vec![KeyPart::Bytes(vec![i as u8; 12])],
                        6 => vec![KeyPart::DecimalWide {
                            value: i.into(),
                            scale: 2,
                        }],
                        _ => vec![
                            KeyPart::Int(i / 8),
                            KeyPart::Null,
                            KeyPart::Int(i),
                            KeyPart::Int(0),
                            KeyPart::Int(1),
                            KeyPart::Int(2),
                        ],
                    };
                    IndexEntry {
                        key: IndexKey::from(parts),
                        row_id: row(i as u16),
                    }
                })
                .collect();
            // A multi-valued index can reference the same row across leaves.
            entries[32].row_id = entries[31].row_id;
            entries[64].row_id = entries[63].row_id;
            for (n, chunk) in entries.chunks(32).enumerate() {
                let page_id = n as u64 + 1;
                let leaf = IndexLeafPage::new_with_resolved_layout(
                    chunk.to_vec(),
                    (n < 2).then_some(page_id + 1),
                );
                index
                    .buffer_pool
                    .store_page(
                        index.page_key(page_id),
                        encode_index_tree_page(&IndexPage::Leaf(leaf)).unwrap(),
                        0,
                    )
                    .unwrap();
            }
            let mut bounds = vec![None];
            for offset in [0, 31, 32, 63, 64, 95] {
                for inclusive in [false, true] {
                    bounds.push(Some(IndexBound {
                        key: entries[offset].key.clone(),
                        inclusive,
                    }));
                }
            }
            if kind == 7 {
                for inclusive in [false, true] {
                    bounds.push(Some(IndexBound {
                        key: IndexKey::from(vec![KeyPart::Int(4)]),
                        inclusive,
                    }));
                }
            }
            for low in &bounds {
                for high in &bounds {
                    let mut expected: Vec<_> = entries
                        .iter()
                        .filter(|entry| {
                            index_key_in_bounds(&entry.key, low.as_ref(), high.as_ref())
                        })
                        .map(|entry| entry.row_id)
                        .collect();
                    expected.dedup();
                    assert_eq!(
                        index
                            .range_row_ids_for_leaf_ids(&[1, 2, 3], low.as_ref(), high.as_ref())
                            .unwrap(),
                        expected,
                        "kind={kind}, low={low:?}, high={high:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn raw_range_rows_resume_and_stop_at_inclusive_high_bound() {
        let mut leaf = IndexLeafPage::new((1..=10).map(int_entry).collect(), Some(42));
        leaf.set_page_max_trx_id(77);
        let page = encode_index_tree_page(&IndexPage::Leaf(leaf)).unwrap();
        validate_index_page_header(&page).unwrap();

        let first = range_leaf_row_ids_page_bytes(
            &page,
            Some(&bound(3, true)),
            Some(&bound(6, true)),
            0,
            false,
            None,
            true,
            false,
            2,
        )
        .unwrap();
        assert_eq!(first.rows, vec![row(3), row(4)]);
        assert!(!first.exhausted_leaf);
        assert!(!first.terminal);
        assert_eq!(first.page_max_trx_id, 77);

        let second = range_leaf_row_ids_page_bytes(
            &page,
            Some(&bound(3, true)),
            Some(&bound(6, true)),
            first.next_entry_idx,
            first.positioned,
            first.last_row_id,
            true,
            false,
            10,
        )
        .unwrap();
        assert_eq!(second.rows, vec![row(5), row(6)]);
        assert!(second.terminal);
        assert_eq!(second.next_leaf, Some(42));
    }

    #[test]
    fn raw_range_rows_honour_exclusive_bounds() {
        let page = encode_index_tree_page(&IndexPage::Leaf(IndexLeafPage::new(
            (1..=8).map(int_entry).collect(),
            None,
        )))
        .unwrap();
        let result = range_leaf_row_ids_page_bytes(
            &page,
            Some(&bound(3, false)),
            Some(&bound(6, false)),
            0,
            false,
            None,
            true,
            false,
            20,
        )
        .unwrap();
        assert_eq!(result.rows, vec![row(4), row(5)]);
        assert!(result.terminal);
    }

    #[test]
    fn raw_range_row_id_sink_does_not_decode_unneeded_keys() {
        let entries: Vec<IndexEntry> = ["alpha", "bravo", "charlie"]
            .into_iter()
            .enumerate()
            .map(|(idx, value)| IndexEntry {
                key: IndexKey::from(vec![KeyPart::String(value.to_string())]),
                row_id: row(idx as u16 + 1),
            })
            .collect();
        let mut page =
            encode_index_tree_page(&IndexPage::Leaf(IndexLeafPage::new(entries, None))).unwrap();
        let key_ranges = {
            let header = parse_leaf_payload_header(&page).unwrap();
            (0..header.count)
                .map(|idx| {
                    let slot = header.read_slot(idx);
                    let start = INDEX_HEADER_BYTES + slot.offset as usize;
                    let end = INDEX_HEADER_BYTES + slot.offset as usize + slot.len as usize
                        - ROW_ID_V7_ENCODED_LEN;
                    (start, end)
                })
                .collect::<Vec<_>>()
        };
        // Production reaches this function only after page checksum
        // validation. Deliberately poison just the key bodies here to prove
        // that the RowId-only sink locates the fixed trailer from the slot
        // metadata instead of reconstructing unused IndexKey values.
        for (start, end) in key_ranges {
            page[start..end].fill(0xff);
        }

        let result =
            range_leaf_row_ids_page_bytes(&page, None, None, 0, false, None, true, false, 10)
                .unwrap();
        assert_eq!(result.rows, vec![row(1), row(2), row(3)]);
    }

    #[test]
    fn raw_range_rows_preserve_multi_column_prefix_bounds() {
        let entries = [(1, 1), (1, 2), (2, 1)]
            .into_iter()
            .enumerate()
            .map(|(idx, (a, b))| IndexEntry {
                key: IndexKey::from(vec![KeyPart::Int(a), KeyPart::Int(b)]),
                row_id: row(idx as u16 + 1),
            })
            .collect();
        let page =
            encode_index_tree_page(&IndexPage::Leaf(IndexLeafPage::new(entries, None))).unwrap();
        let prefix = IndexBound {
            key: IndexKey::from(vec![KeyPart::Int(1)]),
            inclusive: true,
        };
        let result = range_leaf_row_ids_page_bytes(
            &page,
            Some(&prefix),
            Some(&prefix),
            0,
            false,
            None,
            true,
            false,
            20,
        )
        .unwrap();
        assert_eq!(result.rows, vec![row(1), row(2)]);
        assert!(result.terminal);
    }
}

#[derive(Debug, Clone)]
struct LeafKeyRange {
    leaf_id: u64,
    start: usize,
    end: usize,
}

/// Outcome of `IndexFile::try_insert_leaf_in_place`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InPlaceInsert {
    /// Entry installed directly into the resident encoded leaf.
    Inserted,
    /// Entry was already present; no mutation took place.
    Duplicate,
    /// Insert would overflow the leaf; mutation was rolled back. The
    /// caller must take the slow take/clone/split path.
    NeedsSplit,
}

/// Outcome of `IndexFile::try_delete_leaf_in_place`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InPlaceDelete {
    /// Entry removed from the resident encoded leaf. The `bool` is
    /// `true` when the removed entry was at position 0 (the leaf's
    /// previous first key, i.e. the parent's separator needs to be
    /// refreshed). When `false`, the parent separator is unchanged
    /// and the caller may skip `refresh_parent_separator`.
    Deleted { was_first: bool },
    /// Entry was not present; no mutation took place.
    NotFound,
}

/// The epoch covers leaf-only writes as well as structural writes. Counting
/// active writers separately is necessary because independent leaves may be
/// modified concurrently; an odd/even seqlock would not be sufficient.
struct IndexMutationGuard<'a> {
    index: &'a IndexFile,
}

impl Drop for IndexMutationGuard<'_> {
    fn drop(&mut self) {
        self.index
            .mutation_generation
            .fetch_add(1, Ordering::Release);
        self.index.active_mutations.fetch_sub(1, Ordering::Release);
    }
}

/// Buffered direct writes for private index builds. The caller owns key
/// layout and page allocation; this owner keeps scratch admission alive until
/// the buffered images are released. Flush is explicit so I/O errors propagate.
struct BulkIndexPageWriter {
    file: File,
    durability: DurabilityMode,
    images: Vec<u8>,
    first_page: Option<u64>,
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
    owner: [u8; 16],
}

impl BulkIndexPageWriter {
    const RUN_BYTES: usize = 128 * PAGE_SIZE;

    fn new(index: &IndexFile, missing_file: &'static str) -> Result<Self, QueryError> {
        let charge = index.reserve_direct_build_scratch(Self::RUN_BYTES)?;
        let file = index
            .file
            .lock()
            .as_ref()
            .ok_or_else(|| error(crate::error::catalog::ER_GET_ERRNO, missing_file))?
            .try_clone()
            .map_err(io_err)?;
        Ok(Self {
            file,
            durability: index.durability,
            images: Vec::with_capacity(Self::RUN_BYTES),
            first_page: None,
            _charge: charge,
            owner: index.file_key.owner,
        })
    }

    fn append(&mut self, page_id: u64, image: &[u8]) {
        debug_assert_eq!(image.len(), PAGE_SIZE);
        debug_assert!(self
            .first_page
            .is_none_or(|first| { page_id == first + (self.images.len() / PAGE_SIZE) as u64 }));
        self.first_page.get_or_insert(page_id);
        let start = self.images.len();
        self.images.extend_from_slice(image);
        stamp_page_owner(&mut self.images[start..], &self.owner, page_id);
        refresh_page_checksum(&mut self.images[start..]);
    }

    fn append_leaf(&mut self, page_id: u64, image: &mut [u8], txn_id: u64, lsn: u64) {
        // Direct stream builders bypass write_page's writer-bound stamp.
        // Publish the same conservative bound for every encoded leaf layout
        // before refreshing its checksum, so an active loader cannot appear
        // all-visible to covering scans. DDL's zero bound stays unchanged.
        let offset = INDEX_HEADER_BYTES + LEAF_OFF_PAGE_MAX_TRX_ID;
        write_u64(image, offset, read_u64(image, offset).max(txn_id));
        if lsn != 0 {
            set_page_lsn_if_formatted(image, lsn);
        } else {
            refresh_page_checksum(image);
        }
        self.append(page_id, image);
    }

    fn flush_full(&mut self) -> Result<(), QueryError> {
        if self.images.len() == Self::RUN_BYTES {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), QueryError> {
        if self.images.is_empty() {
            return Ok(());
        }
        write_bulk_pages_at(
            self.durability,
            &self.file,
            &self.images,
            self.first_page
                .take()
                .expect("non-empty index page run has a first page")
                .saturating_mul(PAGE_SIZE as u64),
        )
        .map_err(io_err)?;
        self.images.clear();
        Ok(())
    }
}

fn stamp_bulk_index_images(index: &IndexFile, first_page: u64, images: &mut [u8]) {
    for (position, image) in images.chunks_exact_mut(PAGE_SIZE).enumerate() {
        stamp_page_owner(image, &index.file_key.owner, first_page + position as u64);
        refresh_page_checksum(image);
    }
}

/// Streamed external merges retain one producer and a bounded page queue.
/// Positional writes can overlap both the merge and one another; no worker
/// publishes a root or performs the final durability barrier.
fn with_parallel_bulk_image_writes<T>(
    index: &IndexFile,
    file: &File,
    degree: usize,
    produce: impl FnOnce(
        &mut dyn FnMut(u64, &mut Vec<u8>) -> Result<(), QueryError>,
    ) -> Result<T, QueryError>,
) -> Result<T, QueryError> {
    let degree = degree.max(1);
    if degree == 1 {
        return produce(&mut |page, images| {
            stamp_bulk_index_images(index, page, images);
            write_bulk_pages_at(index.durability, file, images, page * PAGE_SIZE as u64)
                .map_err(io_err)?;
            images.clear();
            Ok(())
        });
    }
    let _scratch = index.reserve_direct_build_scratch(
        // Active writes, one queued batch, and the producer's blocked send
        // coexist with the producer buffer covered by the caller's lease.
        (degree + 2).saturating_mul(BulkIndexPageWriter::RUN_BYTES),
    )?;
    let cancel = crate::engine::cancel::CancellationSnapshot::capture();
    let (sender, receiver) = std::sync::mpsc::sync_channel::<(u64, Vec<u8>)>(1);
    let receiver = Mutex::new(receiver);
    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(degree);
        for _ in 0..degree {
            let receiver = &receiver;
            let cancel = cancel.clone();
            handles.push(scope.spawn(move || -> Result<(), QueryError> {
                let _worker = enter_index_build_worker();
                let _cancel = cancel.enter();
                loop {
                    let item = receiver.lock().map_err(|_| poisoned_sorter_error())?.recv();
                    let Ok((page, images)) = item else {
                        return Ok(());
                    };
                    crate::engine::cancel::check_cancelled()?;
                    write_bulk_pages_at(index.durability, file, &images, page * PAGE_SIZE as u64)
                        .map_err(io_err)?;
                }
            }));
        }
        let result = produce(&mut |page, images| {
            stamp_bulk_index_images(index, page, images);
            let batch =
                std::mem::replace(images, Vec::with_capacity(BulkIndexPageWriter::RUN_BYTES));
            sender.send((page, batch)).map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "bulk index page writer stopped",
                )
            })
        });
        drop(sender);
        let mut failure = None;
        for handle in handles {
            if let Err(err) = handle.join().map_err(|_| fulltext_build_worker_panic())? {
                failure.get_or_insert(err);
            }
        }
        if let Some(err) = failure {
            return Err(err);
        }
        result
    })
}

impl IndexFile {
    fn check_rebuild_marker(path: &Path) -> Result<(), QueryError> {
        if path
            .with_extension("idx.corrupt")
            .try_exists()
            .map_err(io_err)?
        {
            return Err(error(
                crate::error::catalog::ER_INDEX_CORRUPT,
                format!(
                    "index is fenced after persistent corruption: {}",
                    path.display()
                ),
            ));
        }

        if path
            .with_extension("idx.failed")
            .try_exists()
            .map_err(io_err)?
        {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "index requires rebuild after failed structural recovery",
            ));
        }
        Ok(())
    }

    pub(super) fn persist_rebuild_marker(path: &Path) -> Result<(), QueryError> {
        let marker = File::create(path.with_extension("idx.failed")).map_err(io_err)?;
        marker.sync_all().map_err(io_err)?;
        if let Some(parent) = path.parent() {
            crate::platform::fs::sync_path(parent).map_err(io_err)?;
        }
        Ok(())
    }

    pub(super) fn persist_planned_rebuild_marker(path: &Path) -> Result<(), QueryError> {
        let mut marker = File::create(path.with_extension("idx.failed")).map_err(io_err)?;
        marker
            .write_all(b"planned logical redo rebuild")
            .map_err(io_err)?;
        marker.sync_all().map_err(io_err)?;
        if let Some(parent) = path.parent() {
            crate::platform::fs::sync_path(parent).map_err(io_err)?;
        }
        Ok(())
    }

    fn mark_requires_rebuild(&self) -> Result<(), QueryError> {
        self.failed.store(true, Ordering::Release);
        if self.durability != DurabilityMode::Memory {
            Self::persist_rebuild_marker(&self.path)?;
        }
        Ok(())
    }

    fn fence_persistent_corruption(&self, err: QueryError) -> QueryError {
        self.failed.store(true, Ordering::Release);
        if self.durability == DurabilityMode::Memory
            || crate::storage::image_io::immutable_path(&self.path)
        {
            return err;
        }
        let marker = self.path.with_extension(if self.spatial.is_some() {
            "rtree.corrupt"
        } else {
            "idx.corrupt"
        });
        let persist = (|| -> std::io::Result<()> {
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&marker)
            {
                Ok(mut file) => {
                    std::io::Write::write_all(&mut file, err.message().as_bytes())?;
                    file.sync_all()?;
                }
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(err) => return Err(err),
            }
            crate::platform::fs::sync_path(marker.parent().expect("index directory"))
        })();
        match persist {
            Ok(()) => err,
            Err(persist_err) => err.map_message(|message| {
                format!("{message}; failed to persist index corruption fence: {persist_err}")
            }),
        }
    }

    fn ensure_healthy(&self) -> Result<(), QueryError> {
        if let Some(spatial) = &self.spatial {
            if self.failed.load(Ordering::Acquire) {
                return Err(error(
                    crate::error::catalog::ER_INDEX_CORRUPT,
                    "persistent spatial index corruption requires explicit repair",
                ));
            }
            return spatial.healthy();
        }
        if self.failed.load(Ordering::Acquire) {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "index requires rebuild after failed structural recovery",
            ));
        }
        Ok(())
    }

    /// Structural writers own the exclusive topology lock. An operation can
    /// fail after publishing only part of its page set, while logical redo can
    /// later reapply the complete operation. Fence that tree durably before
    /// releasing the lock; startup rebuilds it from recovered heap state.
    /// Normal structural writes need no before-image copies or journal locks.
    fn with_structural_recovery(
        &self,
        apply: impl FnOnce() -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        if let Err(original) = apply() {
            self.mark_requires_rebuild()?;
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                format!(
                    "index structural write failed ({}); index requires rebuild",
                    original.message()
                ),
            ));
        }
        Ok(())
    }

    fn begin_mutation(&self) -> IndexMutationGuard<'_> {
        self.active_mutations.fetch_add(1, Ordering::AcqRel);
        self.mutation_generation.fetch_add(1, Ordering::AcqRel);
        IndexMutationGuard { index: self }
    }

    fn stable_generation(&self) -> Option<u64> {
        let active = self.active_mutations.load(Ordering::Acquire);
        let generation = self.mutation_generation.load(Ordering::Acquire);
        (active == 0).then_some(generation)
    }

    /// Required direct-builder scratch gets priority over optional clean
    /// buffer pages. Bulk DDL normally leaves explicit headroom, but another
    /// optional scanner may race that admission. Reclaim clean/flushable pages
    /// for a bounded interval before returning 1037; the hard governor remains
    /// authoritative and no allocation is force-charged.
    fn reserve_direct_build_scratch(
        &self,
        bytes: usize,
    ) -> Result<crate::engine::executor::mem_budget::ControlledMemoryCharge, QueryError> {
        use crate::engine::executor::mem_budget::{
            ControlledMemoryCategory, ControlledMemoryCharge,
        };

        if let Some(charge) = ControlledMemoryCharge::try_new_optional(
            ControlledMemoryCategory::StorageScratch,
            bytes,
        ) {
            return Ok(charge);
        }
        let target =
            crate::engine::executor::mem_budget::memory_target_bytes().saturating_sub(bytes);
        let _ = self
            .buffer_pool
            .reclaim_for_memory_target(target, Duration::from_millis(250));
        ControlledMemoryCharge::try_new(ControlledMemoryCategory::StorageScratch, bytes)
    }

    fn page_latch_index(&self, page_id: u64) -> usize {
        (page_id & self.page_latch_mask) as usize
    }

    fn lock_leaf_latches(
        &self,
        a: u64,
        b: u64,
    ) -> Result<(MutexGuard<'_, ()>, Option<MutexGuard<'_, ()>>), QueryError> {
        let ai = self.page_latch_index(a);
        let bi = self.page_latch_index(b);
        if ai == bi {
            let guard = self.page_latches[ai].lock().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "index page latch poisoned",
                )
            })?;
            return Ok((guard, None));
        }
        let (lo, hi) = if ai < bi { (ai, bi) } else { (bi, ai) };
        let lo_guard = self.page_latches[lo].lock().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "index page latch poisoned",
            )
        })?;
        let hi_guard = self.page_latches[hi].lock().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "index page latch poisoned",
            )
        })?;
        Ok((lo_guard, Some(hi_guard)))
    }

    #[cfg(test)]
    fn open(path: PathBuf, log: Arc<crate::wal::LogMutex>) -> Result<Self, QueryError> {
        let durability = DurabilityMode::from_env();
        let pool = BufferPool::new(
            DEFAULT_BUFFER_POOL_PAGES,
            log.clone(),
            durability,
            Duration::from_millis(DEFAULT_LOCK_WAIT_MS),
        );
        Self::open_with_durability(path, log, durability, pool)
    }

    fn open_with_durability(
        path: PathBuf,
        log: Arc<crate::wal::LogMutex>,
        durability: DurabilityMode,
        buffer_pool: Arc<BufferPool>,
    ) -> Result<Self, QueryError> {
        if path.extension().is_some_and(|e| e == "rtree") {
            return Self::open_spatial(path, log, durability, buffer_pool, false);
        }
        if durability != DurabilityMode::Memory {
            Self::check_rebuild_marker(&path)?;
        }
        // Temporary indexes skip WAL but still have process-local backing
        // files; snapshot restoration must be able to reopen those pages.
        let mut file = if durability == DurabilityMode::Memory
            && !crate::storage::temp_quota::is_temporary_path(&path)
            && !path.try_exists().map_err(io_err)?
        {
            None
        } else {
            Some(crate::storage::image_io::open_data_file(&path).map_err(io_err)?)
        };
        let len = file
            .as_ref()
            .map(|file| file.metadata().map_err(io_err).map(|meta| meta.len()))
            .transpose()?
            .unwrap_or(0);
        if len > 0 && len < PAGE_SIZE as u64 {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index file length",
            ));
        }
        let cached_meta = if durability == DurabilityMode::Memory
            || crate::storage::temp_quota::is_temporary_path(&path)
        {
            // Handle eviction need not evict the buffer frames. Read their
            // current metadata before falling back to the backing file.
            buffer_pool.with_cached_bytes(
                &PageKey {
                    file: Arc::new(BufferPool::index_file_key(path.clone())),
                    page_id: INDEX_META_PAGE,
                },
                |bytes| Ok(bytes.to_vec()),
            )?
        } else {
            None
        };
        let initialized = cached_meta.is_some() || len >= PAGE_SIZE as u64;
        let (meta, meta_image) = if initialized {
            let meta_page = match cached_meta {
                Some(image) => image,
                None => read_index_page(
                    file.as_mut()
                        .expect("index file should be open outside memory mode"),
                    INDEX_META_PAGE,
                )?,
            };
            validate_page_owner(&meta_page, &page_owner(&path), 0)?;
            (decode_index_meta(&meta_page)?, meta_page)
        } else {
            let meta = IndexMeta {
                root_page: INDEX_EMPTY_ROOT,
                next_page_id: INDEX_EMPTY_ROOT + 1,
                free_page_head: None,
            };
            (meta.clone(), vec![0; PAGE_SIZE])
        };
        crate::storage::temp_quota::reserve_extent(&path, meta.next_page_id * PAGE_SIZE as u64)?;
        let file_key = Arc::new(BufferPool::index_file_key(path.clone()));
        let dirty_meta = false;
        let mut dirty = false;
        if !initialized {
            let root = IndexPage::Leaf(IndexLeafPage {
                entries: Vec::new(),
                next_leaf: None,
                key_layout: KEY_LAYOUT_GENERIC,
                base_time: 0,
                layout_aux: None,
                cached_size: IndexLeafPage::compute_encoded_size(&[], KEY_LAYOUT_GENERIC, None),
                page_max_trx_id: 0,
            });
            buffer_pool.store_page(
                PageKey {
                    file: file_key.clone(),
                    page_id: meta.root_page,
                },
                encode_index_tree_page(&root)?,
                0,
            )?;
            dirty = true;
        }
        let rel_path = Self::compute_relative_path(&path);
        let log_atomics = log.lock().state_atomics();
        let root_page_atomic = AtomicU64::new(meta.root_page);
        let (page_latches, page_latch_mask) = new_page_latch_array();
        Ok(Self {
            path,
            file: parking_lot::Mutex::new(file),
            log,
            log_atomics,
            durability,
            root_page: root_page_atomic,
            root_frame_hint: parking_lot::RwLock::new(std::sync::Weak::new()),
            structure_lock: Arc::new(parking_lot::RwLock::new(())),
            split_prepare: parking_lot::Mutex::new(()),
            meta_state: parking_lot::Mutex::new(IndexMetaState { meta, meta_image }),
            file_key,
            buffer_pool,
            dirty_meta: AtomicBool::new(dirty_meta),
            dirty: AtomicBool::new(dirty),
            page_latches,
            page_latch_mask,
            mutation_generation: AtomicU64::new(0),
            active_mutations: AtomicUsize::new(0),
            order_stats_cache: parking_lot::Mutex::new(HashMap::new()),
            rel_path,
            flush_on_drop: true,
            failed: AtomicBool::new(false),
            online_build_log: None,
            spatial: None,
        }
        .initialize_meta_if_needed(initialized)?)
    }

    fn open_spatial(
        path: PathBuf,
        log: Arc<crate::wal::LogMutex>,
        durability: DurabilityMode,
        buffer_pool: Arc<BufferPool>,
        create: bool,
    ) -> Result<Self, QueryError> {
        let spatial = spatial_index::SpatialIndexFile::open(
            path.clone(),
            log.clone(),
            durability,
            buffer_pool.clone(),
            create,
        )?;
        let meta = IndexMeta {
            root_page: 1,
            next_page_id: 2,
            free_page_head: None,
        };
        let meta_image = Vec::new();
        let file_key = Arc::new(BufferPool::index_file_key(path.clone()));
        let dirty = false;
        let dirty_meta = false;
        let rel_path = Self::compute_relative_path(&path);
        let log_atomics = log.lock().state_atomics();
        let root_page_atomic = AtomicU64::new(meta.root_page);
        let (page_latches, page_latch_mask) = new_page_latch_array();
        Ok(Self {
            path,
            file: parking_lot::Mutex::new(None),
            log,
            log_atomics,
            durability,
            root_page: root_page_atomic,
            root_frame_hint: parking_lot::RwLock::new(std::sync::Weak::new()),
            structure_lock: Arc::new(parking_lot::RwLock::new(())),
            split_prepare: parking_lot::Mutex::new(()),
            meta_state: parking_lot::Mutex::new(IndexMetaState { meta, meta_image }),
            file_key,
            buffer_pool,
            dirty_meta: AtomicBool::new(dirty_meta),
            dirty: AtomicBool::new(dirty),
            page_latches,
            page_latch_mask,
            mutation_generation: AtomicU64::new(0),
            active_mutations: AtomicUsize::new(0),
            order_stats_cache: parking_lot::Mutex::new(HashMap::new()),
            rel_path,
            flush_on_drop: true,
            failed: AtomicBool::new(false),
            online_build_log: None,
            spatial: Some(Box::new(spatial)),
        })
    }

    fn initialize_meta_if_needed(mut self, initialized: bool) -> Result<Self, QueryError> {
        if !initialized {
            let result = (|| {
                if self.durability != DurabilityMode::Memory
                    && !physical_wal_apply_active()
                    && !(self.log_atomics.physical_replica() && !crate::wal::promotion_wal_active())
                    && btree_logical_wal::current().is_none()
                {
                    // Empty-tree publication is one system operation. A failed
                    // user WAL reservation cannot leave a root without meta.
                    let mut meta = self.meta_state.lock();
                    let mut meta_image = encode_index_meta(&meta.meta);
                    let root = self.read_page(INDEX_EMPTY_ROOT)?;
                    let mut root_image = encode_index_tree_page(root.as_ref())?;
                    stamp_page_owner(&mut meta_image, &self.file_key.owner, 0);
                    stamp_page_owner(&mut root_image, &self.file_key.owner, INDEX_EMPTY_ROOT);
                    let mut log = self.log.lock();
                    let lsn = log.next_lsn();
                    set_page_lsn_if_formatted(&mut meta_image, lsn);
                    set_page_lsn_if_formatted(&mut root_image, lsn);
                    let mut image = meta_image.clone();
                    image.extend_from_slice(&root_image);
                    log.append_administrative_file(self.relative_path(), image)?;
                    drop(log);
                    self.buffer_pool.store_page(
                        self.page_key(INDEX_EMPTY_ROOT),
                        root_image,
                        lsn,
                    )?;
                    self.buffer_pool.store_page(
                        self.page_key(INDEX_META_PAGE),
                        meta_image.clone(),
                        lsn,
                    )?;
                    meta.meta_image = meta_image;
                    self.dirty_meta.store(true, Ordering::Release);
                    self.dirty.store(true, Ordering::Release);
                } else {
                    let root = self.read_page(INDEX_EMPTY_ROOT)?;
                    self.write_page(txn_id_for_ddl(), INDEX_EMPTY_ROOT, root.as_ref().clone())?;
                    self.write_meta_page(txn_id_for_ddl())?;
                }
                Ok(())
            })();
            if let Err(error) = result {
                self.flush_on_drop = false;
                let _ = self.buffer_pool.discard_file(&self.file_key);
                return Err(error);
            }
        }
        Ok(self)
    }

    fn validate(path: PathBuf) -> Result<(), QueryError> {
        if path.extension().is_some_and(|e| e == "rtree") {
            return spatial_index::SpatialIndexFile::quick_validate(path);
        }
        Self::check_rebuild_marker(&path)?;
        let mut file = OpenOptions::new().read(true).open(path).map_err(io_err)?;
        let meta = decode_index_meta(&read_index_page(&mut file, INDEX_META_PAGE)?)?;
        validate_index_page_tree(&mut file, meta.root_page)?;
        validate_index_freelist(&mut file, meta.free_page_head)?;
        Ok(())
    }

    /// Caller holds the index handle exclusively, preventing concurrent splits.
    /// Read through the buffer pool so dirty pages and memory-only databases
    /// receive the same structural checks as persisted indexes.
    fn validate_structure(&self) -> Result<(), QueryError> {
        if let Some(spatial) = &self.spatial {
            return spatial.validate();
        }
        let meta = self.meta_state.lock().meta.clone();
        validate_index_tree(meta.root_page, meta.next_page_id, &mut |id| {
            self.read_page_with_intent(id, AccessIntent::Scan)
                .map(|page| page.as_ref().clone())
        })
    }

    fn quick_validate(path: PathBuf) -> Result<(), QueryError> {
        if path.extension().is_some_and(|e| e == "rtree") {
            return spatial_index::SpatialIndexFile::quick_validate(path);
        }
        Self::check_rebuild_marker(&path)?;
        let mut file = OpenOptions::new().read(true).open(path).map_err(io_err)?;
        let len = file.metadata().map_err(io_err)?.len();
        if len == 0 || len % PAGE_SIZE as u64 != 0 {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index file length",
            ));
        }
        let page_count = len / PAGE_SIZE as u64;
        let meta = decode_index_meta(&read_index_page(&mut file, INDEX_META_PAGE)?)?;
        if meta.root_page == INDEX_META_PAGE || meta.root_page >= page_count {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: root page out of range",
            ));
        }
        validate_index_page_header(&read_index_page(&mut file, meta.root_page)?)?;
        Ok(())
    }

    fn open_for_recovery(
        path: PathBuf,
        log: Arc<crate::wal::LogMutex>,
        rec_lsn: u64,
    ) -> Result<Self, QueryError> {
        Self::check_rebuild_marker(&path)?;
        // Each replay partition has one owner and already supplies file-level
        // parallelism. Use synchronous eviction without speculative readers:
        // background workers can retain this private pool after IndexFile drops,
        // stranding raw-page charges before the live database pool is admitted.
        let buffer_pool = BufferPool::new_with_options(
            DEFAULT_BUFFER_POOL_PAGES,
            log.clone(),
            DurabilityMode::Strict,
            Duration::ZERO,
            1,
            0,
        );
        buffer_pool.shutdown_flusher();
        let mut file = crate::storage::image_io::open_data_file(&path).map_err(io_err)?;
        let len = file.metadata().map_err(io_err)?.len();
        if len > 0 && len < PAGE_SIZE as u64 {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index file length",
            ));
        }
        let initialized = len >= PAGE_SIZE as u64;
        let (mut meta, mut meta_image, mut dirty_meta) = if initialized {
            let meta_page = read_index_page(&mut file, INDEX_META_PAGE)?;
            (decode_index_meta(&meta_page)?, meta_page, false)
        } else {
            let meta = IndexMeta {
                root_page: INDEX_EMPTY_ROOT,
                next_page_id: INDEX_EMPTY_ROOT + 1,
                free_page_head: None,
            };
            let mut meta_image = encode_index_meta(&meta);
            set_page_lsn_if_formatted(&mut meta_image, rec_lsn);
            (meta, meta_image, true)
        };
        if initialized {
            // Logical redo is idempotent on a complete tree, not on a torn
            // split/merge whose parent and leaf-chain views disagree. Validate
            // before publishing any replay mutation; the caller fences failures
            // and rebuilds the derived index from its authoritative heap.
            validate_index_page_tree(&mut file, meta.root_page)?;
            validate_index_freelist(&mut file, meta.free_page_head)?;
        }
        // Data pages can reach disk before the allocation watermark in the
        // meta page. Reusing such a page ID during logical REDO overwrites a
        // live leaf/internal node and manufactures corruption. The physical
        // extent is a conservative lower bound, including orphan/torn tails.
        let physical_next = len.div_ceil(PAGE_SIZE as u64);
        if meta.next_page_id < physical_next {
            meta.next_page_id = physical_next;
            let original_lsn = page_lsn(&meta_image);
            meta_image = encode_index_meta(&meta);
            set_page_lsn_if_formatted(&mut meta_image, original_lsn);
            dirty_meta = true;
        }
        crate::storage::temp_quota::reserve_extent(&path, meta.next_page_id * PAGE_SIZE as u64)?;
        let file_key = Arc::new(BufferPool::index_file_key(path.clone()));
        if dirty_meta {
            // `flush` persists buffer-pool frames only. A fresh file or a
            // corrected allocation watermark must therefore enter the pool
            // as a dirty frame; otherwise a batch that never rewrites the
            // meta page (no split, no root change) leaves page 0 zeroed
            // while the data pages and file length reach disk.
            buffer_pool.store_page(
                PageKey {
                    file: file_key.clone(),
                    page_id: INDEX_META_PAGE,
                },
                meta_image.clone(),
                rec_lsn,
            )?;
        }
        let mut dirty = dirty_meta;
        if len < (meta.root_page + 1) * PAGE_SIZE as u64 {
            let root = IndexPage::Leaf(IndexLeafPage {
                entries: Vec::new(),
                next_leaf: None,
                key_layout: KEY_LAYOUT_GENERIC,
                base_time: 0,
                layout_aux: None,
                cached_size: IndexLeafPage::compute_encoded_size(&[], KEY_LAYOUT_GENERIC, None),
                page_max_trx_id: 0,
            });
            buffer_pool.store_page(
                PageKey {
                    file: file_key.clone(),
                    page_id: meta.root_page,
                },
                encode_index_tree_page(&root)?,
                rec_lsn,
            )?;
            dirty = true;
        }
        let rel_path = Self::compute_relative_path(&path);
        let log_atomics = log.lock().state_atomics();
        let root_page_atomic = AtomicU64::new(meta.root_page);
        // Recovery only runs single-threaded, so seeding the thread-local
        // logical-WAL sentinel from the active recovery LSN is safe — any
        // FPIs `write_page` emits during replay land < the replayed
        // record's own LSN, matching the original `Some(rec_lsn)` semantic.
        btree_logical_wal::set(Some(rec_lsn));
        let (page_latches, page_latch_mask) = new_page_latch_array();
        Ok(Self {
            path,
            file: parking_lot::Mutex::new(Some(file)),
            log,
            log_atomics,
            durability: DurabilityMode::Strict,
            root_page: root_page_atomic,
            root_frame_hint: parking_lot::RwLock::new(std::sync::Weak::new()),
            structure_lock: Arc::new(parking_lot::RwLock::new(())),
            split_prepare: parking_lot::Mutex::new(()),
            meta_state: parking_lot::Mutex::new(IndexMetaState { meta, meta_image }),
            file_key,
            buffer_pool,
            dirty_meta: AtomicBool::new(dirty_meta),
            dirty: AtomicBool::new(dirty),
            page_latches,
            page_latch_mask,
            mutation_generation: AtomicU64::new(0),
            active_mutations: AtomicUsize::new(0),
            order_stats_cache: parking_lot::Mutex::new(HashMap::new()),
            rel_path,
            flush_on_drop: true,
            failed: AtomicBool::new(false),
            online_build_log: None,
            spatial: None,
        })
    }

    fn create_rebuilt_from_sorted_with_durability(
        path: PathBuf,
        entries: Vec<IndexEntry>,
        log: Arc<crate::wal::LogMutex>,
        durability: DurabilityMode,
        buffer_pool: Arc<BufferPool>,
    ) -> Result<Self, QueryError> {
        if path.extension().is_some_and(|e| e == "rtree") {
            let index = Self::open_spatial(path, log, durability, buffer_pool, true)?;
            index
                .spatial
                .as_ref()
                .unwrap()
                .build_sorted(entries.into_iter().map(Ok))?;
            return Ok(index);
        }
        let _ = fs::remove_file(&path);
        let index = Self::open_rebuilt_with_durability(path, log, durability, buffer_pool)?;
        if entries.is_empty() {
            // Direct (non-WAL) meta write: rebuilds are whole-file
            // direct writes everywhere else, and the physical-replica
            // applier reuses this path where ANY local WAL append
            // would collide with the master's LSN sequence.
            if durability == DurabilityMode::Memory {
                index.write_meta_page(txn_id_for_ddl())?;
            } else {
                index.write_meta_page_direct(btree_logical_wal::current().unwrap_or(0))?;
            }
            return Ok(index);
        }
        let started = Instant::now();
        if durability == DurabilityMode::Memory {
            index.bulk_load_sorted_with_lsn(txn_id_for_ddl(), entries)?;
        } else {
            index.bulk_load_sorted_direct_with_lsn(txn_id_for_ddl(), entries)?;
        }
        record_bulk_index_build(&index, durability != DurabilityMode::Memory, started);
        Ok(index)
    }

    fn create_rebuilt_from_sorted_stream_with_durability<I>(
        path: PathBuf,
        entries: I,
        log: Arc<crate::wal::LogMutex>,
        durability: DurabilityMode,
        buffer_pool: Arc<BufferPool>,
    ) -> Result<Self, QueryError>
    where
        I: Iterator<Item = Result<IndexEntry, QueryError>>,
    {
        if path.extension().is_some_and(|e| e == "rtree") {
            let index = Self::open_spatial(path, log, durability, buffer_pool, true)?;
            index.spatial.as_ref().unwrap().build_sorted(entries)?;
            return Ok(index);
        }

        let _ = fs::remove_file(&path);
        let index = Self::open_rebuilt_with_durability(path, log, durability, buffer_pool)?;
        let started = Instant::now();
        if durability == DurabilityMode::Memory {
            index.bulk_load_sorted_stream_with_lsn(txn_id_for_ddl(), entries)?;
        } else {
            index.bulk_load_sorted_direct_stream_with_lsn(txn_id_for_ddl(), entries)?;
        }
        record_bulk_index_build(&index, durability != DurabilityMode::Memory, started);
        Ok(index)
    }

    fn create_rebuilt_from_sorted_single_int_stream_with_durability<I>(
        path: PathBuf,
        entries: I,
        log: Arc<crate::wal::LogMutex>,
        durability: DurabilityMode,
        buffer_pool: Arc<BufferPool>,
    ) -> Result<Self, QueryError>
    where
        I: Iterator<Item = Result<(i64, RowId), QueryError>>,
    {
        let _ = fs::remove_file(&path);
        let index = Self::open_rebuilt_with_durability(path, log, durability, buffer_pool)?;
        let started = Instant::now();
        if durability == DurabilityMode::Memory {
            let stream = entries.map(|entry| {
                let (value, row_id) = entry?;
                Ok(single_int_index_entry(value, row_id))
            });
            index.bulk_load_sorted_stream_with_lsn(txn_id_for_ddl(), stream)?;
        } else {
            index.bulk_load_sorted_single_int_direct_stream_with_lsn(txn_id_for_ddl(), entries)?;
        }
        record_bulk_index_build(&index, durability != DurabilityMode::Memory, started);
        Ok(index)
    }

    fn create_rebuilt_from_sorted_date_stream_with_durability<I>(
        path: PathBuf,
        entries: I,
        log: Arc<crate::wal::LogMutex>,
        durability: DurabilityMode,
        buffer_pool: Arc<BufferPool>,
    ) -> Result<Self, QueryError>
    where
        I: Iterator<Item = Result<(i32, RowId), QueryError>>,
    {
        let _ = fs::remove_file(&path);
        let index = Self::open_rebuilt_with_durability(path, log, durability, buffer_pool)?;
        let started = Instant::now();
        if durability == DurabilityMode::Memory {
            let stream = entries.map(|entry| {
                let (days, row_id) = entry?;
                Ok(date_index_entry(days, row_id))
            });
            index.bulk_load_sorted_stream_with_lsn(txn_id_for_ddl(), stream)?;
        } else {
            index.bulk_load_sorted_date_direct_stream_with_lsn(txn_id_for_ddl(), entries)?;
        }
        record_bulk_index_build(&index, durability != DurabilityMode::Memory, started);
        Ok(index)
    }

    fn create_rebuilt_from_sorted_pair_int_stream_with_durability<I>(
        path: PathBuf,
        entries: I,
        log: Arc<crate::wal::LogMutex>,
        durability: DurabilityMode,
        buffer_pool: Arc<BufferPool>,
    ) -> Result<Self, QueryError>
    where
        I: Iterator<Item = Result<((i64, i64), RowId), QueryError>>,
    {
        let _ = fs::remove_file(&path);
        let index = Self::open_rebuilt_with_durability(path, log, durability, buffer_pool)?;
        let started = Instant::now();
        if durability == DurabilityMode::Memory {
            let stream = entries.map(|entry| {
                let ((left, right), row_id) = entry?;
                Ok(pair_int_index_entry(left, right, row_id))
            });
            index.bulk_load_sorted_stream_with_lsn(txn_id_for_ddl(), stream)?;
        } else {
            index.bulk_load_sorted_pair_int_direct_stream_with_lsn(txn_id_for_ddl(), entries)?;
        }
        record_bulk_index_build(&index, durability != DurabilityMode::Memory, started);
        Ok(index)
    }

    /// Low-cardinality variable-key specialization. Dictionary keys already
    /// contain the canonical run-stream key payload; the direct builder
    /// converts that payload to the page's u8-part-count representation and
    /// writes 128 contiguous leaf images per syscall without constructing one
    /// `IndexKey`/`KeyPart` object per source row.
    fn create_rebuilt_from_sorted_interned_with_durability(
        path: PathBuf,
        keys: Vec<EncodedInternedKey>,
        records: Vec<u128>,
        index_schema: &IndexSchema,
        log: Arc<crate::wal::LogMutex>,
        durability: DurabilityMode,
        buffer_pool: Arc<BufferPool>,
        degree: usize,
    ) -> Result<Self, QueryError> {
        let _ = fs::remove_file(&path);
        let index = Self::open_rebuilt_with_durability(path, log, durability, buffer_pool)?;
        let started = Instant::now();
        if durability == DurabilityMode::Memory {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "interned direct index build requires persistent durability",
            ));
        }
        if index_schema.is_fulltext() && degree > 1 && records.len() >= 256 * 1024 {
            let lsn = index.prepare_direct_bulk_load()?;
            let level =
                index.bulk_load_fulltext_leaf_level_parallel(&keys, &records, degree, lsn)?;
            index.finish_fixed_direct_stream_level(level, lsn)?;
        } else {
            index.bulk_load_sorted_interned_direct_with_lsn(
                txn_id_for_ddl(),
                &keys,
                &records,
                index_schema,
            )?;
        }
        record_bulk_index_build(&index, true, started);
        Ok(index)
    }

    /// Build generic composite leaves from canonical page-key bytes. The
    /// producer borrows one reusable key buffer; only page separators become
    /// IndexKeys. The existing WAL barrier/root publication protocol is shared
    /// with the other direct builders.
    fn create_rebuilt_from_encoded_keys<F>(
        path: PathBuf,
        produce: F,
        log: Arc<crate::wal::LogMutex>,
        durability: DurabilityMode,
        buffer_pool: Arc<BufferPool>,
        degree: usize,
    ) -> Result<Self, QueryError>
    where
        F: FnOnce(&mut dyn FnMut(&[u8], RowId) -> Result<(), QueryError>) -> Result<(), QueryError>,
    {
        let _ = fs::remove_file(&path);
        let index = Self::open_rebuilt_with_durability(path, log, durability, buffer_pool)?;
        let started = Instant::now();
        let lsn = index.prepare_direct_bulk_load()?;
        const RUN_PAGES: usize = 128;
        let _scratch = index.reserve_direct_build_scratch((RUN_PAGES + 3) * PAGE_SIZE)?;
        let file = index
            .file
            .lock()
            .as_ref()
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "encoded build file is closed",
                )
            })?
            .try_clone()
            .map_err(io_err)?;
        let leaf_started = Instant::now();
        let level = with_parallel_bulk_image_writes(&index, &file, degree, |output| {
            let mut images = Vec::with_capacity(RUN_PAGES * PAGE_SIZE);
            let mut run_first = None;
            let mut level = Vec::new();
            let mut page_id = index.reserve_page_id()?;
            let mut leaf = EncodedLeafPageBuilder::new();
            let mut flush =
                |images: &mut Vec<u8>, first: &mut Option<u64>| -> Result<(), QueryError> {
                    if !images.is_empty() {
                        output(first.take().unwrap(), images)?;
                    }
                    Ok(())
                };
            produce(&mut |key, row_id| {
                let entry_bytes = key.len() + row_id_v7_encoded_len(row_id) + SLOT_OFFSET_BYTES;
                if LEAF_HEADER_BYTES.saturating_add(entry_bytes) > PAGE_SIZE {
                    return Err(error(
                        crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                        "index entry exceeds page size",
                    ));
                }
                if !leaf.slots.is_empty()
                    && leaf.offset + leaf.slots.len() * SLOT_OFFSET_BYTES + entry_bytes > PAGE_SIZE
                {
                    let next = index.reserve_page_id()?;
                    let first = leaf.first.take().unwrap();
                    let image = leaf.finish(Some(next), lsn)?;
                    run_first.get_or_insert(page_id);
                    images.extend_from_slice(&image);
                    level.push((page_id, first));
                    page_id = next;
                    if images.len() == RUN_PAGES * PAGE_SIZE {
                        flush(&mut images, &mut run_first)?;
                        crate::engine::cancel::check_cancelled()?;
                    }
                }
                leaf.push(key, row_id)
            })?;
            if !leaf.slots.is_empty() {
                let first = leaf.first.take().unwrap();
                let image = leaf.finish(None, lsn)?;
                run_first.get_or_insert(page_id);
                images.extend_from_slice(&image);
                level.push((page_id, first));
            }
            flush(&mut images, &mut run_first)?;
            Ok(level)
        })?;
        INDEX_BUILD_LEAF_US.fetch_add(leaf_started.elapsed().as_micros() as u64, Ordering::Relaxed);
        index.finish_fixed_direct_stream_level(level, lsn)?;
        record_bulk_index_build(&index, true, started);
        Ok(index)
    }
    fn open_rebuilt_with_durability(
        path: PathBuf,
        log: Arc<crate::wal::LogMutex>,
        durability: DurabilityMode,
        buffer_pool: Arc<BufferPool>,
    ) -> Result<Self, QueryError> {
        if let Some(parent) = path.parent() {
            if !crate::storage::image_io::immutable_path(&path) {
                fs::create_dir_all(parent).map_err(io_err)?;
            }
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .map_err(io_err)?;
        let meta = IndexMeta {
            root_page: INDEX_EMPTY_ROOT,
            next_page_id: INDEX_EMPTY_ROOT + 1,
            free_page_head: None,
        };
        crate::storage::temp_quota::reserve_extent(&path, meta.next_page_id * PAGE_SIZE as u64)?;
        let file_key = Arc::new(BufferPool::index_file_key(path.clone()));
        let root = IndexPage::Leaf(IndexLeafPage {
            entries: Vec::new(),
            next_leaf: None,
            key_layout: KEY_LAYOUT_GENERIC,
            base_time: 0,
            layout_aux: None,
            cached_size: IndexLeafPage::compute_encoded_size(&[], KEY_LAYOUT_GENERIC, None),
            page_max_trx_id: 0,
        });
        buffer_pool.store_page(
            PageKey {
                file: file_key.clone(),
                page_id: meta.root_page,
            },
            encode_index_tree_page(&root)?,
            0,
        )?;
        let rel_path = Self::compute_relative_path(&path);
        let log_atomics = log.lock().state_atomics();
        let root_page_atomic = AtomicU64::new(meta.root_page);
        let meta_image = encode_index_meta(&meta);
        let (page_latches, page_latch_mask) = new_page_latch_array();
        Ok(Self {
            path,
            file: parking_lot::Mutex::new(Some(file)),
            log,
            log_atomics,
            durability,
            root_page: root_page_atomic,
            root_frame_hint: parking_lot::RwLock::new(std::sync::Weak::new()),
            structure_lock: Arc::new(parking_lot::RwLock::new(())),
            split_prepare: parking_lot::Mutex::new(()),
            meta_state: parking_lot::Mutex::new(IndexMetaState { meta, meta_image }),
            file_key,
            buffer_pool,
            dirty_meta: AtomicBool::new(true),
            dirty: AtomicBool::new(true),
            page_latches,
            page_latch_mask,
            mutation_generation: AtomicU64::new(0),
            active_mutations: AtomicUsize::new(0),
            order_stats_cache: parking_lot::Mutex::new(HashMap::new()),
            rel_path,
            flush_on_drop: true,
            failed: AtomicBool::new(false),
            online_build_log: None,
            spatial: None,
        })
    }

    #[cfg(test)]
    fn insert(&mut self, key: IndexKey, row_id: RowId, _unique: bool) -> Result<(), QueryError> {
        self.insert_internal(txn_id_for_ddl(), IndexEntry { key, row_id }, true)
    }

    /// Insert an entry under an explicit writer txn id. Phase 1b test
    /// helper: exercises the `write_page` choke-point stamp so we can
    /// assert `IndexLeafPage::page_max_trx_id` was raised.
    #[cfg(test)]
    fn insert_with_txn(&self, txn_id: u64, key: IndexKey, row_id: RowId) -> Result<(), QueryError> {
        self.insert_deferred(txn_id, key, row_id)
    }

    /// Test-only accessor for a leaf page's persisted writer bound.
    /// Exposed so `page_max_trx_id_tests` can assert the stamp reached
    /// disk (encode → decode roundtrip is already covered separately).
    #[cfg(test)]
    pub(crate) fn leaf_page_max_trx_id(&self, page_id: u64) -> Result<u64, QueryError> {
        Ok(self.read_leaf(page_id)?.page_max_trx_id())
    }

    /// Test entry for the durable single-delete path (including the shared
    /// topology fast path); production callers reach this via Database DML.
    #[cfg(test)]
    pub(crate) fn delete_with_txn(
        &self,
        txn_id: u64,
        key: IndexKey,
        row_id: RowId,
    ) -> Result<(), QueryError> {
        self.delete_deferred(txn_id, key, row_id)
    }

    /// Test-only helper: current root page id. Root migrates on splits
    /// so tests must query it, not cache the initial value.
    #[cfg(test)]
    pub(crate) fn root_page_id(&self) -> u64 {
        self.root_page.load(Ordering::Acquire)
    }

    /// Test-only helper: collect (page_id, page_max_trx_id) for every
    /// leaf in the tree by walking the leaf-chain from the leftmost
    /// leaf via `next_leaf`. Used by the Phase 1b split/merge
    /// preservation tests.
    #[cfg(test)]
    pub(crate) fn leaf_page_max_trx_id_chain(&self) -> Result<Vec<(u64, u64)>, QueryError> {
        // Descend to the leftmost leaf, then follow the leaf chain.
        let mut page_id = self.root_page.load(Ordering::Acquire);
        loop {
            let page = self.read_page(page_id)?;
            match page.as_ref() {
                IndexPage::Leaf(_) => break,
                IndexPage::Internal(int) => {
                    page_id = *int.children.first().ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "corrupt index: empty internal page",
                        )
                    })?;
                }
                IndexPage::Free(_) => {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: descent reached free page",
                    ));
                }
            }
        }
        let mut out = Vec::new();
        loop {
            let leaf = self.read_leaf(page_id)?;
            out.push((page_id, leaf.page_max_trx_id()));
            match leaf.next_leaf {
                Some(next) => page_id = next,
                None => break,
            }
        }
        Ok(out)
    }

    fn insert_deferred(&self, txn_id: u64, key: IndexKey, row_id: RowId) -> Result<(), QueryError> {
        if let Some(spatial) = &self.spatial {
            return spatial.apply(
                txn_id,
                [IndexBatchEntry {
                    kind: WalIndexOpKind::Insert,
                    key,
                    row_id,
                }],
            );
        }
        let entry = IndexEntry { key, row_id };
        match self.try_insert_entries_buffered(txn_id, std::slice::from_ref(&entry))? {
            Some(BufferedIndexInsert::Complete) => Ok(()),
            Some(BufferedIndexInsert::Remaining { consumed: 0, lsn }) => {
                let _logical = btree_logical_wal::LsnScope::new(Some(lsn));
                self.insert_entry_concurrent(txn_id, entry)
            }
            Some(BufferedIndexInsert::Remaining { .. }) => {
                unreachable!("a consumed single entry completes the buffered insert")
            }
            None => self.apply_single_index_op_logged(
                txn_id,
                WalIndexOpKind::Insert,
                entry,
                |index, entry| index.insert_entry_concurrent(txn_id, entry),
            ),
        }
    }

    /// Insert a batch of `(key, row_id)` entries.
    ///
    /// `exclusive_handle` records whether the caller holds the outer
    /// `IndexHandle` in **write** mode (`with_index_mut` /
    /// `with_index_exclusive`). Only then is the empty-tree
    /// `bulk_load_sorted` fast path safe: it `discard_file`s the index's
    /// entire buffer-pool image and rebuilds bottom-up, and during that
    /// window the backing file is still zero-length, so any concurrent
    /// reader that faults a tree page in gets a zero page ("corrupt
    /// index: invalid page magic"). Concurrent DML goes through
    /// `with_index_dml` (shared handle lock) where such readers exist, so
    /// it must pass `false` and take the structurally-safe incremental
    /// path instead.
    fn insert_batch_deferred(
        &self,
        txn_id: u64,
        entries: Vec<(IndexKey, RowId)>,
        exclusive_handle: bool,
    ) -> Result<(), QueryError> {
        if let Some(spatial) = &self.spatial {
            return spatial.insert_many(txn_id, entries);
        }
        if entries.is_empty() {
            return Ok(());
        }
        let entries = entries
            .into_iter()
            .map(|(key, row_id)| IndexEntry { key, row_id })
            .collect();
        self.insert_entries_logged(txn_id, entries, exclusive_handle)?;
        Ok(())
    }

    /// Apply a batch whose logical `IndexBatch` WAL was emitted earlier by
    /// the LOAD DATA sorter. This is the race fallback for two loaders that
    /// both observed an empty index: the first finisher publishes the bottom-
    /// up tree, while the later finisher inserts its already-logged stream
    /// into that tree without logging the same operations a second time.
    fn insert_prelogged_batch_deferred(
        &self,
        txn_id: u64,
        entries: Vec<(IndexKey, RowId)>,
        lsn: Option<u64>,
    ) -> Result<(), QueryError> {
        if let Some(log) = &self.online_build_log {
            log.record(entries.into_iter().map(|(key, row_id)| IndexBatchEntry {
                kind: WalIndexOpKind::Insert,
                key,
                row_id,
            }));
            return Ok(());
        }
        if entries.is_empty() {
            return Ok(());
        }
        let entries = entries
            .into_iter()
            .map(|(key, row_id)| IndexEntry { key, row_id })
            .collect();
        self.insert_entries_with_lsn(txn_id, entries, lsn)
    }

    fn apply_batch_deferred(
        &self,
        txn_id: u64,
        entries: Vec<IndexBatchEntry>,
    ) -> Result<(), QueryError> {
        if let Some(spatial) = &self.spatial {
            return spatial.apply(txn_id, entries);
        }
        if let Some(log) = &self.online_build_log {
            log.record(entries);
            return Ok(());
        }
        if entries.is_empty() {
            return Ok(());
        }
        let lsn = self.append_mixed_index_batch_wal(txn_id, &entries)?;
        self.apply_batch_with_lsn(txn_id, entries, lsn)
    }

    pub(super) fn apply_preencoded_batch_deferred(
        &self,
        txn_id: u64,
        entries: smallvec::SmallVec<[IndexBatchEntry; 2]>,
        payload: Option<WalPayload>,
    ) -> Result<(), QueryError> {
        if let Some(spatial) = &self.spatial {
            return spatial.apply(txn_id, entries);
        }
        if let Some(log) = &self.online_build_log {
            log.record(entries);
            return Ok(());
        }
        if entries.is_empty() {
            return Ok(());
        }
        // Txn-buffering fast path (`MYDB_WAL_TXN_BUFFER`): park the IndexBatch
        // record instead of appending it under the WAL mutex now. The two-op
        // leaf fast path applies the mutation with the touched leaves pinned
        // un-flushable (stamped `peek_lsn`, collected in `btree_logical_wal`);
        // the record + those leaf keys are handed to the transaction WAL buffer
        // and appended — with the real LSN published to each leaf via
        // `finalize_deferred` — in one lock acquisition at commit. This removes
        // the per-transaction eager index-record append, which was the last
        // remaining WAL-mutex acquisition in the single-row secondary-index
        // update path (heap + begin + commit already coalesce). Falls back to
        // the eager path below when buffering is inactive, the record is absent
        // (Memory durability), or the fast path can't apply (split/structural).
        if payload.is_some()
            && self.durability != DurabilityMode::Memory
            && txn_wal_buffering_active(&self.log, txn_id)
            && txn_wal_buffer_index_enabled()
        {
            let payload = payload.expect("payload checked is_some");
            btree_logical_wal::begin_deferred();
            let applied = (|| {
                let prev = btree_logical_wal::current();
                btree_logical_wal::set(None);
                let r = self.try_apply_two_op_update_batch_fast(txn_id, &entries, None);
                btree_logical_wal::set(prev);
                r
            })();
            let keys = btree_logical_wal::take_deferred();
            match applied {
                Ok(true) => {
                    // Park the record with the leaf frames it dirtied. When the
                    // buffer drains at commit, each key is finalized (real LSN
                    // published, pin released) right after the record's append.
                    if let Err((payload, keys)) =
                        push_txn_wal_record(&self.log, txn_id, payload, keys)
                    {
                        // Cancellation/rollback can end the transaction buffer
                        // after the eligibility probe above. The leaf mutation
                        // and its deferred pins already exist at this point, so
                        // silently dropping `false` loses redo and strands the
                        // frames permanently unflushable. Fall back to the
                        // canonical eager append and always balance every pin.
                        let appended = crate::wal::LogManager::pre_encode_record(txn_id, payload)
                            .and_then(|pre| {
                                crate::wal::LogManager::append_pre_encoded_to(&self.log, pre)
                            });
                        match appended {
                            Ok(lsn) => {
                                for key in &keys {
                                    self.buffer_pool.finalize_deferred(key, lsn);
                                }
                            }
                            Err(err) => {
                                for key in &keys {
                                    self.buffer_pool.finalize_deferred(key, 0);
                                }
                                return Err(err);
                            }
                        }
                    }
                    return Ok(());
                }
                Ok(false) => {
                    // Fast path declined (no store happened ⇒ `keys` empty, no
                    // stray pins). Release any pins defensively, then fall
                    // through to the eager append + slow path below.
                    for key in &keys {
                        self.buffer_pool.finalize_deferred(key, 0);
                    }
                    let pre = crate::wal::LogManager::pre_encode_record(txn_id, payload)?;
                    let lsn = Some(crate::wal::LogManager::append_pre_encoded_to(
                        &self.log, pre,
                    )?);
                    return self.apply_batch_with_lsn(txn_id, entries.into_vec(), lsn);
                }
                Err(e) => {
                    // A store may have pinned a leaf before the error; release
                    // collected pins so the frames stay evictable. The record is
                    // dropped (never appended) — the aborting transaction rolls
                    // back and its parked heap records are discarded too.
                    for key in &keys {
                        self.buffer_pool.finalize_deferred(key, 0);
                    }
                    return Err(e);
                }
            }
        }
        let lsn = match payload {
            Some(payload) => {
                let pre = crate::wal::LogManager::pre_encode_record(txn_id, payload)?;
                Some(crate::wal::LogManager::append_pre_encoded_to(
                    &self.log, pre,
                )?)
            }
            None => None,
        };
        if self.try_apply_two_op_update_batch_fast(txn_id, &entries, lsn)? {
            return Ok(());
        }
        self.apply_batch_with_lsn(txn_id, entries.into_vec(), lsn)
    }

    /// Apply an already-WAL-backed logical batch received by a physical
    /// replica. This mirrors the primary DML path after WAL append: common
    /// `Delete(old key) + Insert(new key)` updates first use the shared
    /// two-leaf/same-leaf fast path, while structural cases retain the
    /// exclusive-lock delete/rebalance/insert oracle.
    pub(super) fn apply_replicated_batch(
        &self,
        txn_id: u64,
        entries: Vec<IndexBatchEntry>,
        lsn: u64,
    ) -> Result<(), QueryError> {
        if let Some(log) = &self.online_build_log {
            log.record(entries);
            return Ok(());
        }
        if entries.is_empty() {
            return Ok(());
        }
        if self.try_apply_two_op_update_batch_fast(txn_id, &entries, Some(lsn))? {
            #[cfg(test)]
            REPLICATED_TWO_OP_FAST_PATH_HITS.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        if entries
            .iter()
            .all(|entry| entry.kind == WalIndexOpKind::Insert)
        {
            let mut inserts = entries
                .into_iter()
                .map(|entry| IndexEntry {
                    key: entry.key,
                    row_id: entry.row_id,
                })
                .collect::<Vec<_>>();
            if self.is_empty()? {
                // Empty-tree recovery has always sorted/deduplicated before
                // the direct builder. Reuse that exact correctness contract
                // for online replay while the caller owns the IndexHandle in
                // write mode, so no reader can observe the file-generation
                // replacement in progress.
                inserts.sort_unstable();
                inserts.dedup();
                self.bulk_load_sorted_for_recovery(txn_id, lsn, inserts)?;
                #[cfg(test)]
                REPLICATED_SORTED_INSERT_FAST_PATH_HITS.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
            if self.can_append_sorted_entries(&inserts)? {
                let previous_lsn = btree_logical_wal::current();
                btree_logical_wal::set(Some(lsn));
                let result = {
                    let _structure = self.structure_lock.write();
                    let _mutation = self.begin_mutation();
                    self.append_sorted_entries(txn_id, inserts)
                };
                btree_logical_wal::set(previous_lsn);
                result?;
                #[cfg(test)]
                REPLICATED_SORTED_INSERT_FAST_PATH_HITS.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }

            // Preserve the existing mixed/unsorted oracle. Reconstruct the
            // batch only on this uncommon fallback; the hot sorted path moves
            // each decoded key exactly once.
            let entries = inserts
                .into_iter()
                .map(|entry| IndexBatchEntry {
                    kind: WalIndexOpKind::Insert,
                    key: entry.key,
                    row_id: entry.row_id,
                })
                .collect();
            return self.apply_batch_with_lsn(txn_id, entries, Some(lsn));
        }
        self.apply_batch_with_lsn(txn_id, entries, Some(lsn))
    }

    fn try_apply_two_op_update_batch_fast(
        &self,
        txn_id: u64,
        entries: &[IndexBatchEntry],
        lsn: Option<u64>,
    ) -> Result<bool, QueryError> {
        let [delete, insert] = entries else {
            return Ok(false);
        };
        if delete.kind != WalIndexOpKind::Delete
            || insert.kind != WalIndexOpKind::Insert
            || delete.row_id != insert.row_id
            || delete.key == insert.key
        {
            return Ok(false);
        }
        // The in-place leaf fast path runs under a *shared* `structure_lock`
        // (plus per-leaf latches); the fallback `apply_batch_with_lsn` takes
        // the *exclusive* `structure_lock`, serializing every writer on the
        // whole index. The guard below only exists to stop a durable update
        // from silently skipping its WAL record: when no logical-WAL sentinel
        // is set and no LSN was assigned, a Strict/Relaxed update has no WAL
        // backing and must take the logged slow path. Memory durability writes
        // no WAL at all (pages carry lsn 0), so `lsn.is_none()` is the normal,
        // correct state there — exempt it, otherwise every Memory-mode
        // secondary-index update is forced onto the exclusive-lock slow path
        // and 20 writers fully serialize (Memory ends up far slower than
        // Strict, which keeps the shared-lock fast path via its WAL LSN).
        if self.durability != DurabilityMode::Memory
            && btree_logical_wal::current().is_none()
            && lsn.is_none()
            && !btree_logical_wal::is_deferred()
        {
            return Ok(false);
        }
        let old_entry = IndexEntry {
            key: delete.key.clone(),
            row_id: delete.row_id,
        };
        let new_entry = IndexEntry {
            key: insert.key.clone(),
            row_id: insert.row_id,
        };
        let previous_lsn = btree_logical_wal::current();
        btree_logical_wal::set(lsn);
        let result = (|| {
            let _structure = self.structure_lock.read();
            // Locate the old key and use that terminal leaf's key range to
            // answer the overwhelmingly common "new key stays in this leaf"
            // case. The previous path performed two complete root-to-leaf
            // traversals and allocated two path Vecs even though neither path
            // was consumed. Generic updates that cross a leaf boundary still
            // take the second traversal, preserving the exact routing oracle.
            let (old_leaf, new_stays_in_old_leaf) =
                self.find_leaf_for_entry_with_candidate(&old_entry, &new_entry)?;
            let old_leaf_id = old_leaf.frame.key.page_id;
            let new_leaf_id = if new_stays_in_old_leaf {
                old_leaf_id
            } else {
                self.find_leaf_for_entry_with_candidate(&new_entry, &new_entry)?
                    .0
                    .frame
                    .key
                    .page_id
            };
            let (latch_a, latch_b) = self.lock_leaf_latches(old_leaf_id, new_leaf_id)?;
            let _mutation = self.begin_mutation();
            let applied = if old_leaf_id == new_leaf_id {
                self.try_apply_update_same_leaf_fast(
                    txn_id,
                    old_leaf_id,
                    &old_entry,
                    &new_entry,
                    Some(&old_leaf),
                )?
            } else {
                self.try_apply_update_two_leaf_fast(
                    txn_id,
                    old_leaf_id,
                    new_leaf_id,
                    &old_entry,
                    &new_entry,
                )?
            };
            drop(latch_b);
            drop(latch_a);
            Ok(applied)
        })();
        btree_logical_wal::set(previous_lsn);
        result
    }

    fn try_apply_update_same_leaf_fast(
        &self,
        txn_id: u64,
        leaf_id: u64,
        old_entry: &IndexEntry,
        new_entry: &IndexEntry,
        pinned_leaf: Option<&PinnedFrame>,
    ) -> Result<bool, QueryError> {
        if let Some(applied) = self.try_apply_update_same_leaf_in_place(
            txn_id,
            leaf_id,
            old_entry,
            new_entry,
            pinned_leaf,
        )? {
            if applied {
                self.dirty.store(true, Ordering::Release);
                return Ok(true);
            }
            return Ok(false);
        }
        let root_page = self.root_page.load(Ordering::Acquire);
        let mut leaf = self.read_leaf(leaf_id)?;
        let Some((old_pos, target_pos)) =
            leaf_reposition_positions(&leaf.entries, old_entry, new_entry)
        else {
            return Ok(false);
        };
        // Parent separators are conservative lower bounds. Raising a leaf's
        // minimum preserves that bound; only a decrease needs the oracle.
        if leaf_id != root_page && target_pos == 0 && (old_pos != 0 || new_entry < old_entry) {
            return Ok(false);
        }
        let removed = apply_leaf_reposition(&mut leaf.entries, old_pos, target_pos, new_entry);
        leaf.bump_size_for_remove(&removed);
        let layout_before_demote = leaf.key_layout;
        Self::demote_leaf_layout_if_needed(&mut leaf, new_entry);
        if leaf.key_layout == layout_before_demote {
            leaf.bump_size_for_insert(new_entry);
        }
        if encoded_leaf_page_len(&leaf) > PAGE_SIZE {
            return Ok(false);
        }
        if leaf_id != root_page {
            if leaf.entries.is_empty() || encoded_leaf_page_len(&leaf) < INDEX_MIN_PAGE_BYTES {
                return Ok(false);
            }
        }
        self.write_page(txn_id, leaf_id, IndexPage::Leaf(leaf))?;
        self.dirty.store(true, Ordering::Release);
        Ok(true)
    }

    fn try_apply_update_same_leaf_in_place(
        &self,
        txn_id: u64,
        leaf_id: u64,
        old_entry: &IndexEntry,
        new_entry: &IndexEntry,
        pinned_leaf: Option<&PinnedFrame>,
    ) -> Result<Option<bool>, QueryError> {
        if !self.lazy_write_eligible(leaf_id)? {
            return Ok(None);
        }
        let root_page = self.root_page.load(Ordering::Acquire);
        let key = self.page_key(leaf_id);
        let peek_lsn = self.lazy_mutation_lsn();
        if let Some(pinned) = pinned_leaf {
            if let Some(applied) =
                self.buffer_pool
                    .try_modify_existing_packed_leaf(pinned, peek_lsn, |leaf| {
                        leaf.reposition(old_entry, new_entry, txn_id, leaf_id == root_page)
                            .map(|applied| (applied, applied))
                    })?
            {
                return Ok(Some(applied));
            }
        }
        let mutate = |bytes: &mut [u8]| {
            let result =
                try_reposition_raw_leaf(bytes, old_entry, new_entry, txn_id, leaf_id == root_page)?;
            Ok((result == Some(true), result))
        };
        let raw_result = if let Some(pinned) = pinned_leaf {
            Some(
                self.buffer_pool
                    .modify_pinned_raw_page(pinned, peek_lsn, mutate)?,
            )
        } else {
            self.buffer_pool
                .try_modify_raw_page(&key, peek_lsn, mutate)?
        };
        if let Some(result) = raw_result {
            if result.is_some() {
                return Ok(result);
            }
        }
        self.buffer_pool
            .modify_index_page_fallback(&key, peek_lsn, |page| {
                let IndexPage::Leaf(leaf) = page else {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: expected leaf page",
                    ));
                };
                let original_layout = leaf.key_layout;
                let original_base_time = leaf.base_time;
                let original_aux = leaf.layout_aux;
                let original_cached_size = leaf.cached_size;
                let Some((old_pos, target_pos)) =
                    leaf_reposition_positions(&leaf.entries, old_entry, new_entry)
                else {
                    return Ok((false, false));
                };
                if leaf_id != root_page
                    && target_pos == 0
                    && (old_pos != 0 || new_entry < old_entry)
                {
                    return Ok((false, false));
                }
                let removed =
                    apply_leaf_reposition(&mut leaf.entries, old_pos, target_pos, new_entry);
                leaf.bump_size_for_remove(&removed);
                let layout_before_demote = leaf.key_layout;
                Self::demote_leaf_layout_if_needed(leaf, new_entry);
                if leaf.key_layout == layout_before_demote {
                    leaf.bump_size_for_insert(new_entry);
                }
                let needs_fallback = encoded_leaf_page_len(leaf) > PAGE_SIZE
                    || (leaf_id != root_page
                        && (leaf.entries.is_empty()
                            || encoded_leaf_page_len(leaf) < INDEX_MIN_PAGE_BYTES));
                if needs_fallback {
                    revert_leaf_reposition(&mut leaf.entries, old_pos, target_pos, removed);
                    leaf.key_layout = original_layout;
                    leaf.base_time = original_base_time;
                    leaf.layout_aux = original_aux;
                    leaf.cached_size = original_cached_size;
                    return Ok((false, false));
                }
                // Phase 1b: bypasses `write_page`; stamp inline.
                leaf.raise_page_max_trx_id(txn_id);
                Ok((true, true))
            })
    }

    fn try_apply_update_two_leaf_fast(
        &self,
        txn_id: u64,
        old_leaf_id: u64,
        new_leaf_id: u64,
        old_entry: &IndexEntry,
        new_entry: &IndexEntry,
    ) -> Result<bool, QueryError> {
        if let Some(applied) = self.try_apply_update_two_leaf_in_place(
            txn_id,
            old_leaf_id,
            new_leaf_id,
            old_entry,
            new_entry,
        )? {
            if applied {
                self.dirty.store(true, Ordering::Release);
            }
            return Ok(applied);
        }
        let root_page = self.root_page.load(Ordering::Acquire);
        let mut old_leaf = self.read_leaf(old_leaf_id)?;
        let mut new_leaf = self.read_leaf(new_leaf_id)?;
        let new_original_first = new_leaf.entries.first().cloned();
        let Ok(delete_pos) = old_leaf.entries.binary_search(old_entry) else {
            return Ok(false);
        };
        let removed = old_leaf.entries.remove(delete_pos);
        old_leaf.bump_size_for_remove(&removed);
        let insert_pos = match new_leaf.entries.binary_search(new_entry) {
            Ok(_) => return Ok(false),
            Err(pos) => pos,
        };
        new_leaf.entries.insert(insert_pos, new_entry.clone());
        let layout_before_demote = new_leaf.key_layout;
        Self::demote_leaf_layout_if_needed(&mut new_leaf, new_entry);
        if new_leaf.key_layout == layout_before_demote {
            new_leaf.bump_size_for_insert(new_entry);
        }
        if encoded_leaf_page_len(&new_leaf) > PAGE_SIZE {
            return Ok(false);
        }
        if old_leaf_id != root_page {
            if old_leaf.entries.is_empty()
                || encoded_leaf_page_len(&old_leaf) < INDEX_MIN_PAGE_BYTES
            {
                return Ok(false);
            }
        }
        if new_leaf_id != root_page && new_leaf.entries.first().cloned() != new_original_first {
            return Ok(false);
        }
        self.write_page(txn_id, old_leaf_id, IndexPage::Leaf(old_leaf))?;
        self.write_page(txn_id, new_leaf_id, IndexPage::Leaf(new_leaf))?;
        self.dirty.store(true, Ordering::Release);
        Ok(true)
    }

    fn try_apply_update_two_leaf_in_place(
        &self,
        txn_id: u64,
        old_leaf_id: u64,
        new_leaf_id: u64,
        old_entry: &IndexEntry,
        new_entry: &IndexEntry,
    ) -> Result<Option<bool>, QueryError> {
        if !btree_two_leaf_in_place_enabled() {
            return Ok(None);
        }
        if !self.lazy_write_eligible(old_leaf_id)? || !self.lazy_write_eligible(new_leaf_id)? {
            return Ok(None);
        }
        let root_page = self.root_page.load(Ordering::Acquire);
        let old_key = self.page_key(old_leaf_id);
        let new_key = self.page_key(new_leaf_id);
        let peek_lsn = self.lazy_mutation_lsn();
        if let Some(result) = self.buffer_pool.try_modify_two_raw_pages(
            &old_key,
            &new_key,
            peek_lsn,
            |old_bytes, new_bytes| {
                let result = try_move_between_raw_leaves(
                    old_bytes,
                    new_bytes,
                    old_entry,
                    new_entry,
                    txn_id,
                    old_leaf_id == root_page,
                    new_leaf_id == root_page,
                )?;
                Ok((result == Some(true), result))
            },
        )? {
            if result.is_some() {
                return Ok(result);
            }
        }
        self.buffer_pool.modify_two_index_pages_fallback(
            &old_key,
            &new_key,
            peek_lsn,
            |old_page, new_page| {
                let (IndexPage::Leaf(old_leaf), IndexPage::Leaf(new_leaf)) = (old_page, new_page)
                else {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: expected leaf page",
                    ));
                };
                let Ok(delete_pos) = old_leaf.entries.binary_search(old_entry) else {
                    return Ok((false, false));
                };
                let insert_pos = match new_leaf.entries.binary_search(new_entry) {
                    Ok(_) => return Ok((false, false)),
                    Err(pos) => pos,
                };
                // Source deletion raises its minimum and keeps the existing
                // separator valid. Destination minimum decreases still use
                // the structural oracle.
                if new_leaf_id != root_page && insert_pos == 0 {
                    return Ok((false, false));
                }

                let old_layout = (old_leaf.key_layout, old_leaf.base_time, old_leaf.layout_aux);
                let new_layout = (new_leaf.key_layout, new_leaf.base_time, new_leaf.layout_aux);
                let old_cached_size = old_leaf.cached_size;
                let new_cached_size = new_leaf.cached_size;

                let removed = old_leaf.entries.remove(delete_pos);
                old_leaf.bump_size_for_remove(&removed);
                new_leaf.entries.insert(insert_pos, new_entry.clone());
                let layout_before_demote = new_leaf.key_layout;
                Self::demote_leaf_layout_if_needed(new_leaf, new_entry);
                if new_leaf.key_layout == layout_before_demote {
                    new_leaf.bump_size_for_insert(new_entry);
                }
                let invalid = (old_leaf_id != root_page
                    && (old_leaf.entries.is_empty()
                        || encoded_leaf_page_len(old_leaf) < INDEX_MIN_PAGE_BYTES))
                    || encoded_leaf_page_len(new_leaf) > PAGE_SIZE;
                if invalid {
                    new_leaf.entries.remove(insert_pos);
                    old_leaf.entries.insert(delete_pos, removed);
                    (old_leaf.key_layout, old_leaf.base_time, old_leaf.layout_aux) = old_layout;
                    (new_leaf.key_layout, new_leaf.base_time, new_leaf.layout_aux) = new_layout;
                    old_leaf.cached_size = old_cached_size;
                    new_leaf.cached_size = new_cached_size;
                    return Ok((false, false));
                }
                // Phase 1b: bypasses `write_page`; stamp both leaves
                // touched by the update.
                old_leaf.raise_page_max_trx_id(txn_id);
                new_leaf.raise_page_max_trx_id(txn_id);
                Ok((true, true))
            },
        )
    }

    fn apply_batch_with_lsn(
        &self,
        txn_id: u64,
        entries: Vec<IndexBatchEntry>,
        lsn: Option<u64>,
    ) -> Result<(), QueryError> {
        let previous_lsn = btree_logical_wal::current();
        btree_logical_wal::set(lsn);
        // Logged deletions share the point-delete oracle: fitting leaves need
        // only their leaf latch and a shared topology guard. Underflow still
        // takes the exclusive structural fallback for that entry. Keep the
        // batch mutation epoch open across entries for range-cursor repair.
        if lsn.is_some()
            && entries
                .iter()
                .all(|entry| entry.kind == WalIndexOpKind::Delete)
        {
            let result = (|| {
                let _mutation = (entries.len() > 1).then(|| self.begin_mutation());
                for entry in entries {
                    self.delete_entry_concurrent(
                        txn_id,
                        IndexEntry {
                            key: entry.key,
                            row_id: entry.row_id,
                        },
                    )?;
                }
                Ok(())
            })();
            btree_logical_wal::set(previous_lsn);
            return result;
        }
        let result = (|| {
            let _structure = self.structure_lock.write();
            let _mutation = self.begin_mutation();
            for entry in entries {
                match entry.kind {
                    WalIndexOpKind::Insert => {
                        self.insert_internal_unlocked(
                            txn_id,
                            IndexEntry {
                                key: entry.key,
                                row_id: entry.row_id,
                            },
                            false,
                        )?;
                    }
                    WalIndexOpKind::Delete => {
                        self.delete_internal_unlocked(txn_id, entry.key, entry.row_id, false)?;
                    }
                }
            }
            Ok(())
        })();
        btree_logical_wal::set(previous_lsn);
        result
    }

    fn insert_entries_logged(
        &self,
        txn_id: u64,
        entries: Vec<IndexEntry>,
        exclusive_handle: bool,
    ) -> Result<(), QueryError> {
        if let Some(log) = &self.online_build_log {
            log.record(entries.into_iter().map(|entry| IndexBatchEntry {
                kind: WalIndexOpKind::Insert,
                key: entry.key,
                row_id: entry.row_id,
            }));
            return Ok(());
        }
        if entries.is_empty() {
            return Ok(());
        }
        // The empty-tree `bulk_load_sorted` shortcut tears the index down
        // to a zero-length file (`discard_file`) and rebuilds it without
        // the `structure_lock`, so it is only sound when the caller owns
        // the index exclusively (no concurrent navigator can fault in a
        // page mid-rebuild). Shared-lock DML must build the empty tree
        // incrementally under `structure_lock.write()` — the sorted-append
        // fast path below already handles an empty tree efficiently.
        if exclusive_handle && self.is_empty()? {
            let mut entries = entries;
            if !index_entries_are_strictly_sorted(&entries) {
                entries.sort_unstable();
                entries.dedup();
            }
            return self.bulk_load_sorted(txn_id, entries);
        }
        if let Some(outcome) = self.try_insert_entries_buffered(txn_id, &entries)? {
            return match outcome {
                BufferedIndexInsert::Complete => Ok(()),
                BufferedIndexInsert::Remaining { consumed, lsn } => {
                    let mut entries = entries;
                    entries.drain(..consumed);
                    self.insert_entries_with_lsn(txn_id, entries, Some(lsn))
                }
            };
        }
        let lsn = self.append_index_batch_wal(txn_id, WalIndexOpKind::Insert, &entries)?;
        self.insert_entries_with_lsn(txn_id, entries, lsn)
    }

    /// Buffer bounded leaf-only insert runs. An overflow/structural decline
    /// appends the complete logical batch before entering the structural
    /// oracle. Return the applied prefix length so the oracle handles only
    /// the remaining suffix. Replaying a full leaf's already-applied prefix
    /// would repeatedly decode that leaf just to rediscover duplicates and
    /// would prevent a sorted suffix from using the right-edge append path.
    /// The complete batch remains in WAL for crash recovery and replication.
    /// None means ineligible; an eligible batch completes or returns its
    /// remaining position and the LSN covering the complete batch.
    fn try_insert_entries_buffered(
        &self,
        txn_id: u64,
        entries: &[IndexEntry],
    ) -> Result<Option<BufferedIndexInsert>, QueryError> {
        if self.durability == DurabilityMode::Memory
            || !txn_wal_buffer_index_enabled()
            || !txn_wal_buffering_active(&self.log, txn_id)
            || btree_logical_wal::is_deferred()
            || physical_wal_apply_active()
        {
            return Ok(None);
        }
        let path = self.relative_path();
        let mut payload_bytes = 8usize.saturating_add(path.len());
        let mut ops = Vec::new();
        for entry in entries {
            let key_len = encoded_index_key_len(&entry.key)?;
            payload_bytes = payload_bytes.saturating_add(11).saturating_add(key_len);
            if payload_bytes > INDEX_BATCH_WAL_TARGET_BYTES {
                return Ok(None);
            }
            let mut key = Vec::with_capacity(key_len);
            encode_index_key_into(&entry.key, &mut key)?;
            ops.push(WalIndexOp {
                kind: WalIndexOpKind::Insert,
                key,
                row_id: WalRowId {
                    page_id: entry.row_id.page_id,
                    slot_id: entry.row_id.slot_id,
                },
            });
        }
        let Some(reservation) = self.buffer_pool.reserve_deferred_wal_pages(entries.len()) else {
            return Ok(None);
        };
        let payload = WalPayload::IndexBatch { path, ops };
        let _logical = btree_logical_wal::LsnScope::new(None);
        btree_logical_wal::begin_deferred();
        let mut position = 0;
        let applied = (|| -> Result<bool, QueryError> {
            let mut route = smallvec::SmallVec::<[InsertRouteFrame; 4]>::new();
            while position < entries.len() {
                let run = SortedIndexEntryRun::prefix(&entries[position..]);
                let _structure = self.structure_lock.read();
                let single;
                let (pinned, end) = if entries.len() == 1 {
                    single = self
                        .find_leaf_for_entry_with_candidate(&entries[0], &entries[0])?
                        .0;
                    (&single, 1)
                } else {
                    self.find_leaf_for_insert_run(
                        entries,
                        position,
                        position + run.0.len(),
                        &mut route,
                    )?
                };
                let leaf = pinned.frame.key.page_id;
                let (_latch, _same) = self.lock_leaf_latches(leaf, leaf)?;
                let _mutation = self.begin_mutation();
                let run = RoutedIndexEntryRun(SortedIndexEntryRun(&entries[position..end]));
                let consumed = if entries.len() == 1 {
                    self.try_insert_leaf_run_in_place::<false>(txn_id, pinned, run)?
                } else {
                    self.try_insert_leaf_run_in_place::<true>(txn_id, pinned, run)?
                };
                match consumed {
                    Some(n) if n > 0 => {
                        position += n;
                        self.dirty.store(true, Ordering::Release);
                    }
                    _ => return Ok(false),
                }
            }
            Ok(true)
        })();
        let keys = btree_logical_wal::take_deferred();
        let (payload, keys, _reservation) = if matches!(applied, Ok(true)) {
            match push_reserved_txn_wal_record(&self.log, txn_id, payload, keys, Some(reservation))
            {
                Ok(()) => return Ok(Some(BufferedIndexInsert::Complete)),
                Err(owned) => owned,
            }
        } else {
            (payload, keys, Some(reservation))
        };
        // Drop deferred collection before appending or entering an oracle.
        // No leaf or topology guard is held while touching the WAL mutex.
        let appended = crate::wal::LogManager::pre_encode_record(txn_id, payload)
            .and_then(|pre| crate::wal::LogManager::append_pre_encoded_to(&self.log, pre));
        let lsn = match appended {
            Ok(lsn) => {
                for key in &keys {
                    self.buffer_pool.finalize_deferred(key, lsn);
                }
                lsn
            }
            Err(err) => {
                // Never make an unlogged dirty image flushable at LSN zero.
                for key in &keys {
                    let _ = self.buffer_pool.discard_page(key);
                }
                return Err(err);
            }
        };
        match applied {
            Ok(true) => Ok(Some(BufferedIndexInsert::Complete)),
            Ok(false) => Ok(Some(BufferedIndexInsert::Remaining {
                consumed: position,
                lsn,
            })),
            Err(err) => Err(err),
        }
    }

    fn insert_entries_with_lsn(
        &self,
        txn_id: u64,
        entries: Vec<IndexEntry>,
        lsn: Option<u64>,
    ) -> Result<(), QueryError> {
        let previous_lsn = btree_logical_wal::current();
        btree_logical_wal::set(lsn);
        let result = (|| {
            // Ordinary memory writes have no logical WAL LSN. Keep admission
            // under one topology guard instead of acquiring it per entry;
            // resident leaves can still use the in-place mutation path.
            if self.durability == DurabilityMode::Memory {
                let _structure = self.structure_lock.write();
                let _mutation = self.begin_mutation();
                if self.can_append_sorted_entries(&entries)? {
                    return self.append_sorted_entries(txn_id, entries);
                }
                for entry in entries {
                    self.insert_internal_unlocked(txn_id, entry, false)?;
                }
                return Ok(());
            }

            // Reuse the append gate's order validation as a proof for leaf
            // runs. Perform it once, outside leaf latches, even if another
            // writer prevents the opportunistic append probe below.
            let sorted_run = SortedIndexEntryRun::strictly_sorted(&entries);
            // Durable DML normally changes only existing leaves. This path is
            // intentionally shared by one-row OLTP inserts and real batches:
            // a fitting leaf needs only the shared topology guard plus its
            // page latch. Giving the overwhelmingly common one-row case the
            // whole-tree write guard serializes unrelated leaves and defeats
            // the concurrent B-tree design.
            //
            // The append hint below reuses the routed leaf. A batch in the
            // middle of the tree never visits the right edge or probes an
            // exclusive structural guard just to discover that it cannot append.
            //
            // Route each entry under the shared topology guard and serialize
            // only writers that hash to the same leaf latch. A fitting leaf
            // can then be changed concurrently with fitting leaves in other
            // parts of the same index. Overflow (or any in-place fast-path
            // decline) drops both guards and reuses the structural oracle for
            // that entry; after its split is published the following entries
            // return to the shared path.
            //
            // No second batch-sized work vector is allocated here: entries
            // are consumed in place, so this changes neither the memory target
            // nor the batch reservation lifetime.
            let mut insert_path = smallvec::SmallVec::<[InsertRouteFrame; 4]>::new();
            let mut pos = 0usize;
            let mut append_attempted = entries.len() == 1 || sorted_run.is_none();
            let mut run = sorted_run.unwrap_or_else(|| SortedIndexEntryRun::prefix(&entries));
            while pos < entries.len() {
                if run.0.is_empty() {
                    run = SortedIndexEntryRun::prefix(&entries[pos..]);
                }
                let mut append_candidate = false;
                let consumed_without_structure_change = {
                    let _structure = self.structure_lock.read();
                    // Single-row OLTP keeps its allocation-free navigator.
                    // Batches reuse versioned parent routing intervals.
                    let single_pin;
                    let (pinned, leaf_run) = if entries.len() == 1 {
                        single_pin = self
                            .find_leaf_for_entry_with_candidate(&entries[pos], &entries[pos])?
                            .0;
                        (&single_pin, RoutedIndexEntryRun(run))
                    } else {
                        let (pinned, end) = self.find_leaf_for_insert_run(
                            &entries,
                            pos,
                            pos + run.0.len(),
                            &mut insert_path,
                        )?;
                        (
                            pinned,
                            RoutedIndexEntryRun(SortedIndexEntryRun(&entries[pos..end])),
                        )
                    };
                    if !append_attempted
                        && pos == 0
                        && match pinned.packed_leaf_append_candidate(&entries[0])? {
                            Some(candidate) => candidate,
                            None => pinned.with_index_raw_bytes(|bytes| {
                                Ok(parse_leaf_payload_header(bytes)?.next_leaf.is_none()
                                    && raw_last_leaf_entry_less_than(bytes, &entries[0])?)
                            })?,
                        }
                    {
                        append_candidate = true;
                        0
                    } else {
                        let leaf_id = pinned.frame.key.page_id;
                        let (leaf_latch, same_bucket) = self.lock_leaf_latches(leaf_id, leaf_id)?;
                        let _mutation = self.begin_mutation();
                        let consumed = if entries.len() == 1 {
                            self.try_insert_leaf_run_in_place::<false>(txn_id, pinned, leaf_run)?
                        } else {
                            self.try_insert_leaf_run_in_place::<true>(txn_id, pinned, leaf_run)?
                        };
                        drop(same_bucket);
                        drop(leaf_latch);
                        match consumed {
                            Some(consumed) => {
                                self.dirty.store(true, Ordering::Release);
                                consumed
                            }
                            None => 0,
                        }
                    }
                };
                if append_candidate {
                    append_attempted = true;
                    insert_path.clear();
                    // A shared leaf observation is only a hint. A racing
                    // writer may extend or replace the edge before election.
                    if let Some(_structure) = self.structure_lock.try_write() {
                        if self.first_entry_follows_right_edge(&entries[0])? {
                            return self.append_sorted_entries(txn_id, entries);
                        }
                    }
                    continue;
                }
                if consumed_without_structure_change > 0 {
                    pos += consumed_without_structure_change;
                    run = run.suffix(consumed_without_structure_change);
                } else {
                    // Structural work may replace/recycle pages. Do not keep
                    // this batch's old pins alive through that operation.
                    insert_path.clear();
                    self.insert_with_prepared_structure(txn_id, entries[pos].clone())?;
                    pos += 1;
                    run = run.suffix(1);
                }
            }
            Ok(())
        })();
        btree_logical_wal::set(previous_lsn);
        result
    }

    fn bulk_load_sorted(&self, txn_id: u64, entries: Vec<IndexEntry>) -> Result<(), QueryError> {
        if entries.is_empty() {
            return Ok(());
        }
        if !index_entries_are_strictly_sorted(&entries) {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "bulk index input must be sorted and unique",
            ));
        }
        let first_lsn = self.log_atomics.next_lsn();
        let lsn = match self.append_index_batch_wal(txn_id, WalIndexOpKind::Insert, &entries) {
            Ok(lsn) => lsn,
            Err(err) => {
                // A multi-record batch may have appended a prefix. Do not let
                // a later commit/reopen treat that prefix as a successful build.
                self.mark_requires_rebuild()?;
                return Err(err);
            }
        };
        let previous_lsn = btree_logical_wal::current();
        btree_logical_wal::set(lsn);
        let direct = self.durability != DurabilityMode::Memory;
        let started = Instant::now();
        // The caller owns an exclusive handle and has already proved that the
        // tree is empty.  In durable modes write the new generation in page-id
        // order instead of admitting both a raw and an authoritative decoded
        // frame for every output page.  Besides avoiding random buffer churn,
        // this keeps the scarce buffer quota available to the heap being
        // loaded. Memory durability deliberately retains the buffered path as
        // the differential oracle (there is no durable file contract there).
        let mut result = if direct {
            self.bulk_load_sorted_direct_with_lsn(txn_id, entries)
        } else {
            self.bulk_load_sorted_with_lsn(txn_id, entries)
        };
        let mut cleanup_failed = false;
        if direct && result.is_err() {
            if let Err(cleanup) = self.reset_empty_index_direct() {
                cleanup_failed = true;
                let original = result.expect_err("direct bulk build failed");
                result = Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!(
                        "direct bulk index build failed ({}); empty-index cleanup failed ({})",
                        original.message(),
                        cleanup.message()
                    ),
                ));
            }
        }
        if cleanup_failed {
            if let Err(marker) = self.mark_requires_rebuild() {
                result = Err(marker);
            }
        }
        if result.is_err() && direct && !cleanup_failed {
            if let Some(last_lsn) = lsn {
                if let Err(compensation) =
                    self.compensate_bulk_insert_wal(txn_id, first_lsn, last_lsn)
                {
                    result = match self.mark_requires_rebuild() {
                        Ok(()) => Err(compensation),
                        Err(marker) => Err(marker),
                    };
                }
            }
        }
        let elapsed_us = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        if result.is_ok() {
            let pages = self
                .meta_state
                .lock()
                .meta
                .next_page_id
                .saturating_sub(INDEX_EMPTY_ROOT);
            if direct {
                BULK_INDEX_DIRECT_BUILDS.fetch_add(1, Ordering::Relaxed);
                BULK_INDEX_DIRECT_PAGES.fetch_add(pages, Ordering::Relaxed);
                BULK_INDEX_DIRECT_US.fetch_add(elapsed_us, Ordering::Relaxed);
            } else {
                BULK_INDEX_BUFFERED_BUILDS.fetch_add(1, Ordering::Relaxed);
                BULK_INDEX_BUFFERED_PAGES.fetch_add(pages, Ordering::Relaxed);
                BULK_INDEX_BUFFERED_US.fetch_add(elapsed_us, Ordering::Relaxed);
            }
        }
        btree_logical_wal::set(previous_lsn);
        result
    }

    /// Error-only path: stream the already encoded insert records back from
    /// WAL and append their logical inverse. This also reaches replicas that
    /// intentionally ignore primary index page images. Successful builds retain
    /// neither a second copy of the keys nor a second encoding of the batch.
    fn compensate_bulk_insert_wal(
        &self,
        txn_id: u64,
        first_lsn: u64,
        last_lsn: u64,
    ) -> Result<(), QueryError> {
        let map_error = |err| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                format!("cannot read bulk index redo for compensation: {err:?}"),
            )
        };
        self.log.lock().flush()?;
        let mut reader = crate::wal::WalReader::open(&self.log, first_lsn).map_err(map_error)?;
        let mut compensated_last_record = false;
        while let Some(chunk) = reader
            .next_chunk(last_lsn, INDEX_BATCH_WAL_TARGET_BYTES)
            .map_err(map_error)?
        {
            let mut offset = 0;
            while offset < chunk.bytes.len() {
                let frame = crate::wal::decode_frame_for_replication(&chunk.bytes[offset..])?;
                offset += frame.frame_len;
                if frame.header.txn_id != txn_id {
                    continue;
                }
                let WalPayload::IndexBatch { path, mut ops } = frame.payload else {
                    continue;
                };
                if path != self.rel_path {
                    continue;
                }
                for op in &mut ops {
                    if op.kind != WalIndexOpKind::Insert {
                        return Err(error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "bulk compensation encountered a non-insert operation",
                        ));
                    }
                    op.kind = WalIndexOpKind::Delete;
                }
                let pre =
                    LogManager::pre_encode_record(txn_id, WalPayload::IndexBatch { path, ops })?;
                crate::wal::LogManager::append_pre_encoded_to(&self.log, pre)?;
                compensated_last_record |= frame.header.lsn == last_lsn;
            }
        }
        if !compensated_last_record {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "bulk index compensation could not read its final WAL record",
            ));
        }
        Ok(())
    }

    /// Build an empty index from a sorted stream whose logical IndexBatch WAL
    /// was appended incrementally by the LOAD DATA pipeline. Keeping WAL
    /// emission ahead of page publication preserves write-ahead ordering
    /// without retaining the complete entry vector solely so
    /// `append_index_batch_wal` can walk it a second time.
    fn bulk_load_prelogged_sorted_stream<I>(
        &self,
        txn_id: u64,
        lsn: Option<u64>,
        entries: I,
    ) -> Result<(), QueryError>
    where
        I: Iterator<Item = Result<IndexEntry, QueryError>>,
    {
        if !self.is_empty()? {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "prelogged bulk index stream requires an empty index",
            ));
        }
        let previous_lsn = btree_logical_wal::current();
        btree_logical_wal::set(lsn);
        let direct = self.durability != DurabilityMode::Memory;
        let started = Instant::now();
        let mut result = if direct {
            self.bulk_load_sorted_direct_stream_with_lsn(txn_id, entries)
        } else {
            self.bulk_load_sorted_stream_with_lsn(txn_id, entries)
        };
        if result.is_err() {
            let cleanup = if direct {
                self.reset_empty_index_direct()
            } else {
                self.reset_empty_index_buffered()
            };
            if let Err(cleanup) = cleanup {
                let original = result.expect_err("direct streamed bulk build failed");
                result = Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!(
                        "streamed bulk index build failed ({}); empty-index cleanup failed ({})",
                        original.message(),
                        cleanup.message()
                    ),
                ));
            }
        }
        if result.is_ok() {
            record_bulk_index_build(self, direct, started);
        }
        btree_logical_wal::set(previous_lsn);
        result
    }

    /// Fixed-width counterpart of `bulk_load_prelogged_sorted_stream` for a
    /// resident, sorted two-INT key set.  Keeping `(i64, i64, RowId)` compact
    /// through leaf construction avoids materialising and dropping millions
    /// of transient `IndexEntry`/`KeyPart` values after LOAD DATA has already
    /// paid for a compact sorter.  Memory durability stays on the generic
    /// builder as the format/semantic oracle.
    fn bulk_load_prelogged_sorted_single_int_stream<I>(
        &self,
        txn_id: u64,
        lsn: Option<u64>,
        entries: I,
    ) -> Result<(), QueryError>
    where
        I: Iterator<Item = Result<(i64, RowId), QueryError>>,
    {
        if !self.is_empty()? {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "prelogged bulk index single-int stream requires an empty index",
            ));
        }
        let previous_lsn = btree_logical_wal::current();
        btree_logical_wal::set(lsn);
        let direct = self.durability != DurabilityMode::Memory;
        let started = Instant::now();
        let mut result = if direct {
            self.bulk_load_sorted_single_int_direct_stream_with_lsn(txn_id, entries)
        } else {
            let stream = entries.map(|entry| {
                let (value, row_id) = entry?;
                Ok(single_int_index_entry(value, row_id))
            });
            self.bulk_load_sorted_stream_with_lsn(txn_id, stream)
        };
        if result.is_err() {
            let cleanup = if direct {
                self.reset_empty_index_direct()
            } else {
                self.reset_empty_index_buffered()
            };
            if let Err(cleanup) = cleanup {
                let original = result.expect_err("direct single-int stream build failed");
                result = Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!(
                        "single-int stream build failed ({}); empty-index cleanup failed ({})",
                        original.message(),
                        cleanup.message()
                    ),
                ));
            }
        }
        if result.is_ok() {
            record_bulk_index_build(self, direct, started);
        }
        btree_logical_wal::set(previous_lsn);
        result
    }

    fn bulk_load_prelogged_sorted_date_stream<I>(
        &self,
        txn_id: u64,
        lsn: Option<u64>,
        entries: I,
    ) -> Result<(), QueryError>
    where
        I: Iterator<Item = Result<(i32, RowId), QueryError>>,
    {
        if !self.is_empty()? {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "prelogged bulk index date stream requires an empty index",
            ));
        }
        let previous_lsn = btree_logical_wal::current();
        btree_logical_wal::set(lsn);
        let direct = self.durability != DurabilityMode::Memory;
        let started = Instant::now();
        let mut result = if direct {
            self.bulk_load_sorted_date_direct_stream_with_lsn(txn_id, entries)
        } else {
            let stream = entries.map(|entry| {
                let (days, row_id) = entry?;
                Ok(date_index_entry(days, row_id))
            });
            self.bulk_load_sorted_stream_with_lsn(txn_id, stream)
        };
        if result.is_err() {
            let cleanup = if direct {
                self.reset_empty_index_direct()
            } else {
                self.reset_empty_index_buffered()
            };
            if let Err(cleanup) = cleanup {
                let original = result.expect_err("direct date stream build failed");
                result = Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!(
                        "date stream build failed ({}); empty-index cleanup failed ({})",
                        original.message(),
                        cleanup.message()
                    ),
                ));
            }
        }
        if result.is_ok() {
            record_bulk_index_build(self, direct, started);
        }
        btree_logical_wal::set(previous_lsn);
        result
    }

    /// Spill-run counterpart of the resident pair-int builder. Ordered LOAD
    /// input produces non-overlapping sorted runs; consuming their fixed-width
    /// tuples directly avoids the generic k-way heap, `IndexKey` allocation,
    /// and unique-check key cloning at EOF.
    fn bulk_load_prelogged_sorted_pair_int_stream<I>(
        &self,
        txn_id: u64,
        lsn: Option<u64>,
        entries: I,
    ) -> Result<(), QueryError>
    where
        I: Iterator<Item = Result<((i64, i64), RowId), QueryError>>,
    {
        if !self.is_empty()? {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "prelogged bulk index pair-int stream requires an empty index",
            ));
        }
        let previous_lsn = btree_logical_wal::current();
        btree_logical_wal::set(lsn);
        let direct = self.durability != DurabilityMode::Memory;
        let started = Instant::now();
        let mut result = if direct {
            self.bulk_load_sorted_pair_int_direct_stream_with_lsn(txn_id, entries)
        } else {
            let stream = entries.map(|entry| {
                let ((left, right), row_id) = entry?;
                Ok(pair_int_index_entry(left, right, row_id))
            });
            self.bulk_load_sorted_stream_with_lsn(txn_id, stream)
        };
        if result.is_err() {
            let cleanup = if direct {
                self.reset_empty_index_direct()
            } else {
                self.reset_empty_index_buffered()
            };
            if let Err(cleanup) = cleanup {
                let original = result.expect_err("direct pair-int stream build failed");
                result = Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!(
                        "pair-int stream build failed ({}); empty-index cleanup failed ({})",
                        original.message(),
                        cleanup.message()
                    ),
                ));
            }
        }
        if result.is_ok() {
            record_bulk_index_build(self, direct, started);
        }
        btree_logical_wal::set(previous_lsn);
        result
    }

    fn reset_empty_index_buffered(&self) -> Result<(), QueryError> {
        self.buffer_pool.discard_file(&self.file_key)?;
        {
            let mut state = self.meta_state.lock();
            state.meta.root_page = INDEX_EMPTY_ROOT;
            state.meta.next_page_id = INDEX_EMPTY_ROOT + 1;
            state.meta.free_page_head = None;
        }
        self.root_page.store(INDEX_EMPTY_ROOT, Ordering::Release);
        let empty_root = IndexPage::Leaf(IndexLeafPage {
            entries: Vec::new(),
            next_leaf: None,
            key_layout: KEY_LAYOUT_GENERIC,
            base_time: 0,
            layout_aux: None,
            cached_size: IndexLeafPage::compute_encoded_size(&[], KEY_LAYOUT_GENERIC, None),
            page_max_trx_id: 0,
        });
        self.write_page(0, INDEX_EMPTY_ROOT, empty_root)?;
        self.write_meta_page(0)?;
        self.dirty.store(true, Ordering::Release);
        Ok(())
    }

    /// Restore the pre-build empty-tree generation after a direct builder
    /// error. The caller must separately compensate already logged changes
    /// before allowing that transaction to commit. This cleanup keeps the live
    /// handle usable and ensures no partial leaf/internal generation is
    /// published through meta in the meantime.
    fn reset_empty_index_direct(&self) -> Result<(), QueryError> {
        self.buffer_pool.discard_file(&self.file_key)?;
        {
            let mut state = self.meta_state.lock();
            state.meta.root_page = INDEX_EMPTY_ROOT;
            state.meta.next_page_id = INDEX_EMPTY_ROOT + 1;
            state.meta.free_page_head = None;
        }
        self.root_page.store(INDEX_EMPTY_ROOT, Ordering::Release);
        let empty_root = IndexPage::Leaf(IndexLeafPage {
            entries: Vec::new(),
            next_leaf: None,
            key_layout: KEY_LAYOUT_GENERIC,
            base_time: 0,
            layout_aux: None,
            cached_size: IndexLeafPage::compute_encoded_size(&[], KEY_LAYOUT_GENERIC, None),
            page_max_trx_id: 0,
        });
        self.write_page_direct(INDEX_EMPTY_ROOT, empty_root, 0)?;
        self.write_meta_page_direct(0)?;
        let mut file_guard = self.file.lock();
        let file = file_guard.as_mut().ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "index file should be open for bulk cleanup",
            )
        })?;
        file.set_len((INDEX_EMPTY_ROOT + 1) * PAGE_SIZE as u64)
            .map_err(io_err)?;
        file.flush().map_err(io_err)?;
        if self.durability == DurabilityMode::Strict {
            crate::platform::fs::sync_file_data_at(file, &self.path).map_err(io_err)?;
        }
        self.dirty_meta.store(false, Ordering::Release);
        self.dirty.store(false, Ordering::Release);
        drop(file_guard);
        self.buffer_pool.discard_file(&self.file_key)?;
        Ok(())
    }

    fn bulk_load_sorted_for_recovery(
        &self,
        txn_id: u64,
        lsn: u64,
        entries: Vec<IndexEntry>,
    ) -> Result<(), QueryError> {
        let previous_lsn = btree_logical_wal::current();
        btree_logical_wal::set(Some(lsn));
        // Recovery must publish the rebuilt meta page and tree pages as one
        // coherent on-disk generation before a later IndexBatch continues
        // incrementally from it.  The lazy buffer-pool variant can otherwise
        // retain pre-crash frames for page ids that the rebuild reuses; a
        // following append may then observe a stale high-water mark and reuse
        // its current rightmost leaf as the split target (a self-referencing
        // next_leaf chain).  The direct builder discards all frames, writes
        // the complete generation, truncates to next_page_id, and only then
        // permits the next WAL record to proceed.
        let result = self.bulk_load_sorted_direct_with_lsn(txn_id, entries);
        btree_logical_wal::set(previous_lsn);
        result
    }

    fn bulk_load_sorted_with_lsn(
        &self,
        txn_id: u64,
        entries: Vec<IndexEntry>,
    ) -> Result<(), QueryError> {
        self.buffer_pool.discard_file(&self.file_key)?;
        self.meta_state.lock().meta.free_page_head = None;
        self.meta_state.lock().meta.next_page_id = INDEX_EMPTY_ROOT;

        let mut level = self.bulk_load_leaf_level(txn_id, entries)?;
        while level.len() > 1 {
            level = self.bulk_load_internal_level(txn_id, level)?;
        }
        self.publish_bulk_root(&level, "bulk index build produced no root")?;
        self.write_meta_page(txn_id)?;
        self.dirty.store(true, Ordering::Release);
        Ok(())
    }

    fn bulk_load_sorted_stream_with_lsn<I>(&self, txn_id: u64, entries: I) -> Result<(), QueryError>
    where
        I: Iterator<Item = Result<IndexEntry, QueryError>>,
    {
        self.buffer_pool.discard_file(&self.file_key)?;
        self.meta_state.lock().meta.free_page_head = None;
        self.meta_state.lock().meta.next_page_id = INDEX_EMPTY_ROOT;

        let mut level = self.bulk_load_leaf_level_stream(txn_id, entries)?;
        if level.is_empty() {
            return self.reset_empty_index_buffered();
        }
        while level.len() > 1 {
            level = self.bulk_load_internal_level(txn_id, level)?;
        }
        self.publish_bulk_root(&level, "bulk index build produced no root")?;
        self.write_meta_page(txn_id)?;
        self.dirty.store(true, Ordering::Release);
        Ok(())
    }

    fn bulk_load_sorted_direct_with_lsn(
        &self,
        txn_id: u64,
        entries: Vec<IndexEntry>,
    ) -> Result<(), QueryError> {
        if self.durability == DurabilityMode::Memory {
            return self.bulk_load_sorted_with_lsn(txn_id, entries);
        }
        let lsn = self.prepare_direct_bulk_load()?;

        let mut level = self.bulk_load_leaf_level_direct(entries, lsn, txn_id)?;
        while level.len() > 1 {
            level = self.bulk_load_internal_level_direct(level, lsn)?;
        }
        self.publish_bulk_root(&level, "bulk index build produced no root")?;
        #[cfg(test)]
        if take_bulk_index_fail_before_meta() {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "bulk index failpoint before meta publication",
            ));
        }
        self.write_meta_page_direct(lsn)?;

        self.finish_direct_bulk_file("index file should be open for bulk load")
    }

    fn bulk_load_sorted_direct_stream_with_lsn<I>(
        &self,
        txn_id: u64,
        entries: I,
    ) -> Result<(), QueryError>
    where
        I: Iterator<Item = Result<IndexEntry, QueryError>>,
    {
        if self.durability == DurabilityMode::Memory {
            return self.bulk_load_sorted_stream_with_lsn(txn_id, entries);
        }
        let lsn = self.prepare_direct_bulk_load()?;

        let mut level = self.bulk_load_leaf_level_direct_stream(entries, lsn, txn_id)?;
        if level.is_empty() {
            return self.reset_empty_index_direct();
        } else {
            while level.len() > 1 {
                level = self.bulk_load_internal_level_direct(level, lsn)?;
            }
            self.publish_bulk_root(&level, "bulk index build produced no root")?;
            self.write_meta_page_direct(lsn)?;
        }

        self.finish_direct_bulk_file("index file should be open for bulk load")
    }

    fn finish_fixed_direct_stream_level(
        &self,
        mut level: Vec<(u64, IndexEntry)>,
        lsn: u64,
    ) -> Result<(), QueryError> {
        if level.is_empty() {
            return self.reset_empty_index_direct();
        } else {
            while level.len() > 1 {
                level = self.bulk_load_internal_level_direct(level, lsn)?;
            }
            self.publish_bulk_root(&level, "fixed-width index stream produced no root")?;
            self.write_meta_page_direct(lsn)?;
        }

        self.finish_direct_bulk_file("index file should be open for fixed-width stream build")
    }

    fn publish_bulk_root(
        &self,
        level: &[(u64, IndexEntry)],
        missing_root: &'static str,
    ) -> Result<(), QueryError> {
        let root_page = level
            .first()
            .map(|(page_id, _)| *page_id)
            .ok_or_else(|| error(crate::error::catalog::ER_GET_ERRNO, missing_root))?;
        self.meta_state.lock().meta.root_page = root_page;
        self.root_page.store(root_page, Ordering::Release);
        Ok(())
    }

    /// Called after metadata publication. Preserve truncate/flush/sync order
    /// and retire frames admitted by prefetch during the direct build.
    fn finish_direct_bulk_file(&self, missing_file: &'static str) -> Result<(), QueryError> {
        let next_page_id = self.meta_state.lock().meta.next_page_id;
        let mut file_guard = self.file.lock();
        let file = file_guard
            .as_mut()
            .ok_or_else(|| error(crate::error::catalog::ER_GET_ERRNO, missing_file))?;
        file.set_len(next_page_id * PAGE_SIZE as u64)
            .map_err(io_err)?;
        file.flush().map_err(io_err)?;
        if self.durability == DurabilityMode::Strict && !btree_logical_wal::bulk_sync_deferred() {
            let started = Instant::now();
            crate::platform::fs::sync_file_data_at(file, &self.path).map_err(io_err)?;
            INDEX_BUILD_FSYNC_US.fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
        }
        drop(file_guard);
        // A prefetch already dequeued before the first discard may admit
        // old bytes while the direct builder replaces the file. Retire that
        // intermediate cache generation after the complete image is visible.
        self.buffer_pool.discard_file(&self.file_key)?;
        Ok(())
    }

    fn prepare_direct_bulk_load(&self) -> Result<u64, QueryError> {
        let lsn = btree_logical_wal::current().unwrap_or(0);
        if lsn != 0 {
            crate::wal::LogManager::sync_durable_to(&self.log, lsn)?;
        }
        self.buffer_pool.discard_file(&self.file_key)?;
        self.dirty_meta.store(false, Ordering::Release);
        self.dirty.store(false, Ordering::Release);
        let mut state = self.meta_state.lock();
        state.meta.free_page_head = None;
        state.meta.next_page_id = INDEX_EMPTY_ROOT;
        drop(state);
        Ok(lsn)
    }

    fn bulk_load_sorted_single_int_direct_stream_with_lsn<I>(
        &self,
        txn_id: u64,
        entries: I,
    ) -> Result<(), QueryError>
    where
        I: Iterator<Item = Result<(i64, RowId), QueryError>>,
    {
        if self.durability == DurabilityMode::Memory {
            let stream = entries.map(|entry| {
                let (value, row_id) = entry?;
                Ok(single_int_index_entry(value, row_id))
            });
            return self.bulk_load_sorted_stream_with_lsn(txn_id, stream);
        }
        let lsn = self.prepare_direct_bulk_load()?;
        let level = self.bulk_load_single_int_leaf_level_direct_stream(entries, lsn, txn_id)?;
        self.finish_fixed_direct_stream_level(level, lsn)
    }

    fn bulk_load_sorted_date_direct_stream_with_lsn<I>(
        &self,
        txn_id: u64,
        entries: I,
    ) -> Result<(), QueryError>
    where
        I: Iterator<Item = Result<(i32, RowId), QueryError>>,
    {
        if self.durability == DurabilityMode::Memory {
            let stream = entries.map(|entry| {
                let (days, row_id) = entry?;
                Ok(date_index_entry(days, row_id))
            });
            return self.bulk_load_sorted_stream_with_lsn(txn_id, stream);
        }
        let lsn = self.prepare_direct_bulk_load()?;
        let level = self.bulk_load_date_leaf_level_direct_stream(entries, lsn, txn_id)?;
        self.finish_fixed_direct_stream_level(level, lsn)
    }

    fn bulk_load_sorted_pair_int_direct_stream_with_lsn<I>(
        &self,
        txn_id: u64,
        entries: I,
    ) -> Result<(), QueryError>
    where
        I: Iterator<Item = Result<((i64, i64), RowId), QueryError>>,
    {
        if self.durability == DurabilityMode::Memory {
            let stream = entries.map(|entry| {
                let ((left, right), row_id) = entry?;
                Ok(pair_int_index_entry(left, right, row_id))
            });
            return self.bulk_load_sorted_stream_with_lsn(txn_id, stream);
        }
        let lsn = self.prepare_direct_bulk_load()?;
        let level = self.bulk_load_pair_int_leaf_level_direct_stream(entries, lsn, txn_id)?;
        self.finish_fixed_direct_stream_level(level, lsn)
    }

    fn bulk_load_sorted_interned_direct_with_lsn(
        &self,
        txn_id: u64,
        keys: &[EncodedInternedKey],
        records: &[u128],
        index: &IndexSchema,
    ) -> Result<(), QueryError> {
        let lsn = self.prepare_direct_bulk_load()?;
        let started = Instant::now();
        let level = self.bulk_load_interned_leaf_level_direct(keys, records, index, lsn, txn_id)?;
        INDEX_BUILD_LEAF_US.fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
        self.finish_fixed_direct_stream_level(level, lsn)
    }

    /// Disjoint leaf ranges are encoded and written by independent workers.
    /// Plan page boundaries before allocating ids so every last leaf links to
    /// the next worker's first page. Root publication and fsync remain after
    /// all workers have joined, using the ordinary direct-build protocol.
    fn bulk_load_fulltext_leaf_level_parallel(
        &self,
        keys: &[EncodedInternedKey],
        records: &[u128],
        degree: usize,
        lsn: u64,
    ) -> Result<Vec<(u64, IndexEntry)>, QueryError> {
        let started = Instant::now();
        let key_bytes = keys
            .iter()
            .map(|key| {
                let payload = key.materialized_key.as_deref().ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "FULLTEXT dictionary payload missing",
                    )
                })?;
                let len = encoded_materialized_leaf_key_len(payload)?;
                if LEAF_HEADER_BYTES + len + ROW_ID_V7_ENCODED_LEN + SLOT_OFFSET_BYTES > PAGE_SIZE {
                    return Err(error(
                        crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                        "index entry exceeds page size",
                    ));
                }
                Ok(len)
            })
            .collect::<Result<Vec<_>, QueryError>>()?;
        let degree = degree.min(records.len().div_ceil(128 * 1024)).max(1);
        let chunk = records.len().div_ceil(degree);
        let cancel = crate::engine::cancel::CancellationSnapshot::capture();
        let plans = std::thread::scope(|scope| -> Result<Vec<Vec<(usize, usize)>>, QueryError> {
            let mut handles = Vec::new();
            for (worker, slice) in records.chunks(chunk).enumerate() {
                let key_bytes = &key_bytes;
                handles.push(scope.spawn(move || {
                    let mut pages = Vec::new();
                    let mut start = 0usize;
                    let mut bytes = LEAF_HEADER_BYTES;
                    for (offset, record) in slice.iter().copied().enumerate() {
                        let len = key_bytes[interned_record_key_id(record) as usize]
                            + row_id_v7_encoded_len(interned_record_row_id(record))
                            + SLOT_OFFSET_BYTES;
                        if bytes + len > PAGE_SIZE {
                            pages.push((worker * chunk + start, worker * chunk + offset));
                            start = offset;
                            bytes = LEAF_HEADER_BYTES;
                        }
                        bytes += len;
                    }
                    if start < slice.len() {
                        pages.push((worker * chunk + start, worker * chunk + slice.len()));
                    }
                    pages
                }));
            }
            handles
                .into_iter()
                .map(|handle| handle.join().map_err(|_| fulltext_build_worker_panic()))
                .collect()
        })?;
        let count = plans.iter().map(Vec::len).sum::<usize>();
        let mut ids = Vec::with_capacity(count);
        for _ in 0..count {
            ids.push(self.reserve_page_id()?);
        }
        let level = std::thread::scope(|scope| -> Result<Vec<(u64, IndexEntry)>, QueryError> {
            let mut handles = Vec::new();
            let mut offset = 0usize;
            for pages in plans {
                let first = offset;
                offset += pages.len();
                let ids = &ids;
                let cancel = cancel.clone();
                handles.push(
                    scope.spawn(move || -> Result<Vec<(u64, IndexEntry)>, QueryError> {
                        let _worker = enter_index_build_worker();
                        let _cancel = cancel.enter();
                        let mut writer =
                            BulkIndexPageWriter::new(self, "FULLTEXT bulk file is closed")?;
                        let mut level = Vec::with_capacity(pages.len());
                        for (page, (start, end)) in pages.into_iter().enumerate() {
                            crate::engine::cancel::check_cancelled()?;
                            let position = first + page;
                            let record = records[start];
                            let key = &keys[interned_record_key_id(record) as usize];
                            let mut cursor = 0;
                            let separator = decode_index_key_from(
                                key.materialized_key.as_deref().unwrap(),
                                &mut cursor,
                            )?;
                            level.push((
                                ids[position],
                                IndexEntry {
                                    key: separator,
                                    row_id: interned_record_row_id(record),
                                },
                            ));
                            let mut image = encode_interned_leaf_page(
                                keys,
                                &records[start..end],
                                ids.get(position + 1).copied(),
                            )?;
                            writer.append_leaf(ids[position], &mut image, txn_id_for_ddl(), lsn);
                            writer.flush_full()?;
                        }
                        writer.flush()?;
                        Ok(level)
                    }),
                );
            }
            let mut level = Vec::with_capacity(count);
            for handle in handles {
                level.extend(handle.join().map_err(|_| fulltext_build_worker_panic())??);
            }
            Ok(level)
        })?;
        INDEX_BUILD_LEAF_US.fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
        Ok(level)
    }

    fn bulk_load_interned_leaf_level_direct(
        &self,
        keys: &[EncodedInternedKey],
        records: &[u128],
        index: &IndexSchema,
        lsn: u64,
        txn_id: u64,
    ) -> Result<Vec<(u64, IndexEntry)>, QueryError> {
        let mut writer =
            BulkIndexPageWriter::new(self, "index file should be open for interned stream build")?;

        let mut level = Vec::new();
        let mut page_id = self.reserve_page_id()?;
        let mut current = Vec::<u128>::new();
        let mut current_bytes = LEAF_HEADER_BYTES;
        let mut previous_unique_rank = None::<u32>;

        let separator = |record: u128| -> Result<IndexEntry, QueryError> {
            let key = keys
                .get(interned_record_key_id(record) as usize)
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "interned index record references an unknown key",
                    )
                })?;
            let payload = key.materialized_key.as_deref().ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "interned index key has no materialized payload",
                )
            })?;
            let mut offset = 0usize;
            let decoded = decode_index_key_from(payload, &mut offset)?;
            if offset != payload.len() {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "interned index key payload has trailing bytes",
                ));
            }
            Ok(IndexEntry {
                key: decoded,
                row_id: interned_record_row_id(record),
            })
        };

        for record in records.iter().copied() {
            let key = keys
                .get(interned_record_key_id(record) as usize)
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "interned index record references an unknown key",
                    )
                })?;
            if index.primary && key.has_null {
                return Err(error(
                    crate::error::catalog::ER_INVALID_USE_OF_NULL,
                    "invalid use of NULL value for primary key",
                ));
            }
            let rank = interned_record_rank(record);
            if index.unique && !key.has_null && previous_unique_rank == Some(rank) {
                return Err(error(
                    crate::error::catalog::ER_DUP_ENTRY,
                    format!("duplicate entry for key '{}'", index.name),
                ));
            }
            if index.unique && !key.has_null {
                previous_unique_rank = Some(rank);
            }
            let payload = key.materialized_key.as_deref().ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "interned index key has no materialized payload",
                )
            })?;
            let entry_bytes = encoded_materialized_leaf_key_len(payload)?
                .saturating_add(row_id_v7_encoded_len(interned_record_row_id(record)))
                .saturating_add(SLOT_OFFSET_BYTES);
            if !current.is_empty() && current_bytes.saturating_add(entry_bytes) > PAGE_SIZE {
                let next_page_id = self.reserve_page_id()?;
                let first = separator(current[0])?;
                let mut image = encode_interned_leaf_page(keys, &current, Some(next_page_id))?;
                writer.append_leaf(page_id, &mut image, txn_id, lsn);
                writer.flush_full()?;
                level.push((page_id, first));
                page_id = next_page_id;
                current.clear();
                current_bytes = LEAF_HEADER_BYTES;
            }
            current_bytes = current_bytes.saturating_add(entry_bytes);
            current.push(record);
        }
        if !current.is_empty() {
            let first = separator(current[0])?;
            let mut image = encode_interned_leaf_page(keys, &current, None)?;
            writer.append_leaf(page_id, &mut image, txn_id, lsn);
            level.push((page_id, first));
        }
        writer.flush()?;
        Ok(level)
    }

    fn bulk_load_leaf_level(
        &self,
        txn_id: u64,
        entries: Vec<IndexEntry>,
    ) -> Result<Vec<(u64, IndexEntry)>, QueryError> {
        self.bulk_load_leaf_level_stream(txn_id, entries.into_iter().map(Ok))
    }

    fn bulk_load_leaf_level_stream<I>(
        &self,
        txn_id: u64,
        entries: I,
    ) -> Result<Vec<(u64, IndexEntry)>, QueryError>
    where
        I: Iterator<Item = Result<IndexEntry, QueryError>>,
    {
        // Compact-aware sizing shared by Vec and fallible stream inputs:
        // `CompactChunkAccum::size_with` projects the leaf size in the
        // cheapest viable layout, so single-column INT/DATE/etc.
        // chunks pack as compact_int_u16/etc. instead of generic.
        let mut level = Vec::new();
        let mut page_id = self.reserve_page_id()?;
        let mut current = Vec::new();
        let mut chunk = CompactChunkAccum::new();
        for entry in entries {
            let entry = entry?;
            let proposed = chunk.size_with(&entry);
            if !current.is_empty() && proposed > PAGE_SIZE {
                let next_page_id = self.reserve_page_id()?;
                let first = current.first().cloned().ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "bulk index leaf is empty",
                    )
                })?;
                let (key_layout, base_time, layout_aux) = chunk.layout_full();
                self.write_page(
                    txn_id,
                    page_id,
                    IndexPage::Leaf(IndexLeafPage::new_with_layout_aux(
                        std::mem::take(&mut current),
                        Some(next_page_id),
                        key_layout,
                        base_time,
                        layout_aux,
                    )),
                )?;
                level.push((page_id, first));
                page_id = next_page_id;
                chunk = CompactChunkAccum::new();
            }
            chunk.record(&entry);
            current.push(entry);
        }
        if !current.is_empty() {
            let first = current.first().cloned().ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "bulk index leaf is empty",
                )
            })?;
            let (key_layout, base_time, layout_aux) = chunk.layout_full();
            self.write_page(
                txn_id,
                page_id,
                IndexPage::Leaf(IndexLeafPage::new_with_layout_aux(
                    current, None, key_layout, base_time, layout_aux,
                )),
            )?;
            level.push((page_id, first));
        }
        Ok(level)
    }

    fn bulk_load_leaf_level_direct(
        &self,
        entries: Vec<IndexEntry>,
        lsn: u64,
        txn_id: u64,
    ) -> Result<Vec<(u64, IndexEntry)>, QueryError> {
        // Compact-aware sizing (mirrors `bulk_load_leaf_level`). The
        // direct path skips the buffer pool and writes raw pages, but
        // the layout selection is identical: `CompactChunkAccum`
        // tracks the cheapest projection in either layout and picks
        // at flush time.
        let mut level = Vec::new();
        let mut page_id = self.reserve_page_id()?;
        let mut current = Vec::new();
        let mut chunk = CompactChunkAccum::new();
        for entry in entries {
            let proposed = chunk.size_with(&entry);
            if !current.is_empty() && proposed > PAGE_SIZE {
                let next_page_id = self.reserve_page_id()?;
                let first = current.first().cloned().ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "bulk index leaf is empty",
                    )
                })?;
                let (key_layout, base_time, layout_aux) = chunk.layout_full();
                self.write_bulk_leaf_direct(
                    page_id,
                    IndexPage::Leaf(IndexLeafPage::new_with_layout_aux(
                        std::mem::take(&mut current),
                        Some(next_page_id),
                        key_layout,
                        base_time,
                        layout_aux,
                    )),
                    lsn,
                    txn_id,
                )?;
                level.push((page_id, first));
                page_id = next_page_id;
                chunk = CompactChunkAccum::new();
            }
            chunk.record(&entry);
            current.push(entry);
        }
        if !current.is_empty() {
            let first = current.first().cloned().ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "bulk index leaf is empty",
                )
            })?;
            let (key_layout, base_time, layout_aux) = chunk.layout_full();
            self.write_bulk_leaf_direct(
                page_id,
                IndexPage::Leaf(IndexLeafPage::new_with_layout_aux(
                    current, None, key_layout, base_time, layout_aux,
                )),
                lsn,
                txn_id,
            )?;
            level.push((page_id, first));
        }
        Ok(level)
    }

    fn bulk_load_leaf_level_direct_stream<I>(
        &self,
        entries: I,
        lsn: u64,
        txn_id: u64,
    ) -> Result<Vec<(u64, IndexEntry)>, QueryError>
    where
        I: Iterator<Item = Result<IndexEntry, QueryError>>,
    {
        let mut writer =
            BulkIndexPageWriter::new(self, "index file should be open for streamed bulk load")?;

        // LOAD DATA / TPC-H bulk-load hot path. Use the same
        // `CompactChunkAccum`-driven layout selection as
        // `bulk_load_leaf_level` so single-column INT/DATE/etc.
        // indexes ride the dense compact lanes; without this, all
        // bulk-loaded leaves silently stayed GENERIC regardless of
        // shape.
        let mut level = Vec::new();
        let mut page_id = self.reserve_page_id()?;
        let mut current = Vec::new();
        let mut chunk = CompactChunkAccum::new();
        for entry in entries {
            let entry = entry?;
            let proposed = chunk.size_with(&entry);
            if !current.is_empty() && proposed > PAGE_SIZE {
                let next_page_id = self.reserve_page_id()?;
                let first = current.first().cloned().ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "bulk index leaf is empty",
                    )
                })?;
                let (key_layout, base_time, layout_aux) = chunk.layout_full();
                let mut image =
                    encode_index_tree_page(&IndexPage::Leaf(IndexLeafPage::new_with_layout_aux(
                        std::mem::take(&mut current),
                        Some(next_page_id),
                        key_layout,
                        base_time,
                        layout_aux,
                    )))?;
                writer.append_leaf(page_id, &mut image, txn_id, lsn);
                writer.flush_full()?;
                level.push((page_id, first));
                page_id = next_page_id;
                chunk = CompactChunkAccum::new();
            }
            chunk.record(&entry);
            current.push(entry);
        }
        if !current.is_empty() {
            let first = current.first().cloned().ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "bulk index leaf is empty",
                )
            })?;
            let (key_layout, base_time, layout_aux) = chunk.layout_full();
            let mut image =
                encode_index_tree_page(&IndexPage::Leaf(IndexLeafPage::new_with_layout_aux(
                    current, None, key_layout, base_time, layout_aux,
                )))?;
            writer.append_leaf(page_id, &mut image, txn_id, lsn);
            level.push((page_id, first));
        }
        writer.flush()?;
        Ok(level)
    }

    fn bulk_load_date_leaf_level_direct_stream<I>(
        &self,
        entries: I,
        lsn: u64,
        txn_id: u64,
    ) -> Result<Vec<(u64, IndexEntry)>, QueryError>
    where
        I: Iterator<Item = Result<(i32, RowId), QueryError>>,
    {
        let mut writer =
            BulkIndexPageWriter::new(self, "index file should be open for DATE stream build")?;

        let mut level = Vec::new();
        let mut page_id = self.reserve_page_id()?;
        let mut current = Vec::<(i32, RowId)>::new();
        let mut chunk_min = i64::MAX;
        let mut chunk_max = i64::MIN;
        let mut compact_bytes = LEAF_HEADER_BYTES;
        let mut generic_bytes = LEAF_HEADER_BYTES;
        for entry in entries {
            let (days_value, row_id) = entry?;
            let days = days_value as i64;
            let compact_entry = compact_date_entry_bytes(row_id) + SLOT_OFFSET_BYTES;
            let generic_entry = date_generic_entry_bytes(row_id) + SLOT_OFFSET_BYTES;
            let new_min = chunk_min.min(days);
            let new_max = chunk_max.max(days);
            let span = (new_max as i128) - (new_min as i128);
            let next_compact = compact_bytes + compact_entry;
            let next_generic = generic_bytes + generic_entry;
            let proposed = if span >= 0 && span <= u16::MAX as i128 {
                next_compact.min(next_generic)
            } else {
                next_generic
            };
            if !current.is_empty() && proposed > PAGE_SIZE {
                let next_page_id = self.reserve_page_id()?;
                let (first_days, first_row_id) = current[0];
                let mut image =
                    encode_date_leaf_page(&current, chunk_min, chunk_max, Some(next_page_id))?;
                writer.append_leaf(page_id, &mut image, txn_id, lsn);
                writer.flush_full()?;
                level.push((page_id, date_index_entry(first_days, first_row_id)));
                page_id = next_page_id;
                current.clear();
                chunk_min = i64::MAX;
                chunk_max = i64::MIN;
                compact_bytes = LEAF_HEADER_BYTES;
                generic_bytes = LEAF_HEADER_BYTES;
            }
            chunk_min = chunk_min.min(days);
            chunk_max = chunk_max.max(days);
            compact_bytes += compact_entry;
            generic_bytes += generic_entry;
            current.push((days_value, row_id));
        }
        if !current.is_empty() {
            let (first_days, first_row_id) = current[0];
            let mut image = encode_date_leaf_page(&current, chunk_min, chunk_max, None)?;
            writer.append_leaf(page_id, &mut image, txn_id, lsn);
            level.push((page_id, date_index_entry(first_days, first_row_id)));
        }
        writer.flush()?;
        Ok(level)
    }

    fn bulk_load_single_int_leaf_level_direct_stream<I>(
        &self,
        entries: I,
        lsn: u64,
        txn_id: u64,
    ) -> Result<Vec<(u64, IndexEntry)>, QueryError>
    where
        I: Iterator<Item = Result<(i64, RowId), QueryError>>,
    {
        let mut writer =
            BulkIndexPageWriter::new(self, "index file should be open for INT stream build")?;

        let mut level = Vec::new();
        let mut page_id = self.reserve_page_id()?;
        let mut current = Vec::<(i64, RowId)>::new();
        let mut chunk_min = i64::MAX;
        let mut chunk_max = i64::MIN;
        let mut compact_u16_bytes = LEAF_HEADER_BYTES;
        let mut compact_u32_bytes = LEAF_HEADER_BYTES;
        let mut generic_bytes = LEAF_HEADER_BYTES;
        for entry in entries {
            let (value, row_id) = entry?;
            let new_min = chunk_min.min(value);
            let new_max = chunk_max.max(value);
            let span = (new_max as i128) - (new_min as i128);
            let next_u16 =
                compact_u16_bytes + compact_int_u16_entry_bytes(row_id) + SLOT_OFFSET_BYTES;
            let next_u32 =
                compact_u32_bytes + compact_int_u32_entry_bytes(row_id) + SLOT_OFFSET_BYTES;
            let next_generic =
                generic_bytes + 1 + 1 + 8 + row_id_v7_encoded_len(row_id) + SLOT_OFFSET_BYTES;
            let proposed = if span >= 0 && span <= u16::MAX as i128 {
                next_u16
            } else if span >= 0 && span <= u32::MAX as i128 {
                next_u32.min(next_generic)
            } else {
                next_generic
            };
            if !current.is_empty() && proposed > PAGE_SIZE {
                let next_page_id = self.reserve_page_id()?;
                let (first_value, first_row_id) = current[0];
                let mut image = encode_single_int_leaf_page(
                    &current,
                    chunk_min,
                    chunk_max,
                    Some(next_page_id),
                )?;
                writer.append_leaf(page_id, &mut image, txn_id, lsn);
                writer.flush_full()?;
                level.push((page_id, single_int_index_entry(first_value, first_row_id)));
                page_id = next_page_id;
                current.clear();
                chunk_min = i64::MAX;
                chunk_max = i64::MIN;
                compact_u16_bytes = LEAF_HEADER_BYTES;
                compact_u32_bytes = LEAF_HEADER_BYTES;
                generic_bytes = LEAF_HEADER_BYTES;
            }
            chunk_min = chunk_min.min(value);
            chunk_max = chunk_max.max(value);
            compact_u16_bytes += compact_int_u16_entry_bytes(row_id) + SLOT_OFFSET_BYTES;
            compact_u32_bytes += compact_int_u32_entry_bytes(row_id) + SLOT_OFFSET_BYTES;
            generic_bytes += 1 + 1 + 8 + row_id_v7_encoded_len(row_id) + SLOT_OFFSET_BYTES;
            current.push((value, row_id));
        }
        if !current.is_empty() {
            let (first_value, first_row_id) = current[0];
            let mut image = encode_single_int_leaf_page(&current, chunk_min, chunk_max, None)?;
            writer.append_leaf(page_id, &mut image, txn_id, lsn);
            level.push((page_id, single_int_index_entry(first_value, first_row_id)));
        }
        writer.flush()?;
        Ok(level)
    }

    fn bulk_load_pair_int_leaf_level_direct_stream<I>(
        &self,
        entries: I,
        lsn: u64,
        txn_id: u64,
    ) -> Result<Vec<(u64, IndexEntry)>, QueryError>
    where
        I: Iterator<Item = Result<((i64, i64), RowId), QueryError>>,
    {
        let mut writer =
            BulkIndexPageWriter::new(self, "index file should be open for streamed bulk load")?;
        let mut level = Vec::new();
        let mut current = Vec::<((i64, i64), RowId)>::new();
        let mut page_id = self.reserve_page_id()?;
        let mut min1 = i64::MAX;
        let mut max1 = i64::MIN;
        let mut min2 = i64::MAX;
        let mut max2 = i64::MIN;

        for entry in entries {
            let (key @ (value1, value2), row_id) = entry?;
            let new_min1 = min1.min(value1);
            let new_max1 = max1.max(value1);
            let new_min2 = min2.min(value2);
            let new_max2 = max2.max(value2);
            let width1 = int2_width_for_span((new_max1 as i128) - (new_min1 as i128));
            let width2 = int2_width_for_span((new_max2 as i128) - (new_min2 as i128));
            let count = current.len() + 1;
            let compact = width1.zip(width2).map(|(w1, w2)| {
                LEAF_HEADER_BYTES
                    + COMPACT_INT2_PREFIX_BYTES
                    + count
                        * (w1 as usize + w2 as usize + ROW_ID_V7_ENCODED_LEN + SLOT_OFFSET_BYTES)
            });
            let generic = LEAF_HEADER_BYTES
                + count * (1 + 2 * (1 + 8) + ROW_ID_V7_ENCODED_LEN + SLOT_OFFSET_BYTES);
            if !current.is_empty()
                && compact.map_or(generic, |bytes| bytes.min(generic)) > PAGE_SIZE
            {
                let next_page_id = self.reserve_page_id()?;
                let (first_key, first_row_id) = current[0];
                let mut image = encode_pair_int_leaf_page(
                    &current,
                    min1,
                    max1,
                    min2,
                    max2,
                    Some(next_page_id),
                )?;
                writer.append_leaf(page_id, &mut image, txn_id, lsn);
                writer.flush_full()?;
                level.push((
                    page_id,
                    pair_int_index_entry(first_key.0, first_key.1, first_row_id),
                ));
                page_id = next_page_id;
                current.clear();
                min1 = value1;
                max1 = value1;
                min2 = value2;
                max2 = value2;
            } else {
                min1 = new_min1;
                max1 = new_max1;
                min2 = new_min2;
                max2 = new_max2;
            }
            current.push((key, row_id));
        }
        if !current.is_empty() {
            let (first_key, first_row_id) = current[0];
            let mut image = encode_pair_int_leaf_page(&current, min1, max1, min2, max2, None)?;
            writer.append_leaf(page_id, &mut image, txn_id, lsn);
            level.push((
                page_id,
                pair_int_index_entry(first_key.0, first_key.1, first_row_id),
            ));
        }
        writer.flush()?;
        Ok(level)
    }

    fn bulk_load_internal_level(
        &self,
        txn_id: u64,
        children: Vec<(u64, IndexEntry)>,
    ) -> Result<Vec<(u64, IndexEntry)>, QueryError> {
        self.build_internal_level(children, |page_id, page| {
            self.write_page(txn_id, page_id, page)
        })
    }

    fn bulk_load_internal_level_direct(
        &self,
        children: Vec<(u64, IndexEntry)>,
        lsn: u64,
    ) -> Result<Vec<(u64, IndexEntry)>, QueryError> {
        let started = Instant::now();
        let result = self.build_internal_level(children, |page_id, page| {
            self.write_page_direct(page_id, page, lsn)
        });
        INDEX_BUILD_INTERNAL_US.fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
        result
    }

    fn build_internal_level(
        &self,
        children: Vec<(u64, IndexEntry)>,
        mut write: impl FnMut(u64, IndexPage) -> Result<(), QueryError>,
    ) -> Result<Vec<(u64, IndexEntry)>, QueryError> {
        let mut level = Vec::new();
        let mut page_id = self.reserve_page_id()?;
        let mut current = Vec::new();
        let mut current_bytes = INTERNAL_HEADER_BYTES;
        for child in children {
            let separator_len = if current.is_empty() {
                0
            } else {
                encoded_index_entry_len(&child.1)
            };
            let child_len = 8 + separator_len;
            if !current.is_empty() && current_bytes + child_len > PAGE_SIZE {
                let (first, page) = build_bulk_internal_page(&current)?;
                write(page_id, page)?;
                level.push((page_id, first));
                page_id = self.reserve_page_id()?;
                current = Vec::new();
                current_bytes = INTERNAL_HEADER_BYTES;
            }
            current_bytes += child_len + SLOT_OFFSET_BYTES;
            current.push(child);
        }
        if !current.is_empty() {
            let (first, page) = build_bulk_internal_page(&current)?;
            write(page_id, page)?;
            level.push((page_id, first));
        }
        Ok(level)
    }

    fn reserve_page_id(&self) -> Result<u64, QueryError> {
        let mut state = self.meta_state.lock();
        self.extend_page_range(&mut state)
    }

    fn extend_page_range(&self, state: &mut IndexMetaState) -> Result<u64, QueryError> {
        let page_id = state.meta.next_page_id;
        let next = page_id.checked_add(1).ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "index page range overflow",
            )
        })?;
        let bytes = next.checked_mul(PAGE_SIZE as u64).ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "index page range overflow",
            )
        })?;
        crate::storage::temp_quota::reserve_extent(&self.path, bytes)?;
        state.meta.next_page_id = next;
        Ok(page_id)
    }

    fn can_append_sorted_entries(&self, entries: &[IndexEntry]) -> Result<bool, QueryError> {
        if entries.is_empty() || !index_entries_are_strictly_sorted(entries) {
            return Ok(false);
        }
        self.first_entry_follows_right_edge(&entries[0])
    }

    fn first_entry_follows_right_edge(&self, entry: &IndexEntry) -> Result<bool, QueryError> {
        let leaf_id = self.rightmost_leaf()?;
        let pinned = self.buffer_pool.pin_page(self.page_key(leaf_id))?;
        let can_append =
            pinned.with_index_raw_bytes(|bytes| raw_last_leaf_entry_less_than(bytes, entry))?;
        drop(pinned);
        Ok(can_append)
    }

    fn append_sorted_entries(
        &self,
        txn_id: u64,
        entries: Vec<IndexEntry>,
    ) -> Result<(), QueryError> {
        self.with_structural_recovery(|| self.append_sorted_entries_guarded(txn_id, entries))
    }

    fn append_sorted_entries_guarded(
        &self,
        txn_id: u64,
        entries: Vec<IndexEntry>,
    ) -> Result<(), QueryError> {
        let mut pos = 0;
        while pos < entries.len() {
            let mut path = Vec::new();
            let leaf_id = self.find_leaf_for_entry(&entries[pos], &mut path)?;
            // Reuse the generic encoded-run writer before materializing the
            // right edge. Fitting batches need neither a whole-leaf decode
            // nor a re-encode; splits and layout changes retain the oracle.
            if self.durability != DurabilityMode::Memory {
                let pinned = self.buffer_pool.pin_page(self.page_key(leaf_id))?;
                let append = pinned.with_index_raw_bytes(|bytes| {
                    Ok(parse_leaf_payload_header(bytes)?.next_leaf.is_none()
                        && raw_last_leaf_entry_less_than(bytes, &entries[pos])?)
                })?;
                if append {
                    let (_leaf_latch, _same_bucket) = self.lock_leaf_latches(leaf_id, leaf_id)?;
                    let _mutation = self.begin_mutation();
                    if let Some(consumed) = self.try_insert_leaf_run_in_place::<true>(
                        txn_id,
                        &pinned,
                        RoutedIndexEntryRun(SortedIndexEntryRun(&entries[pos..])),
                    )? {
                        self.dirty.store(true, Ordering::Release);
                        pos += consumed;
                        if pos == entries.len() {
                            return Ok(());
                        }
                        continue;
                    }
                }
            }
            let mut leaf = self.read_leaf(leaf_id)?;
            if leaf.next_leaf.is_some()
                || leaf
                    .entries
                    .last()
                    .is_some_and(|last| last >= &entries[pos])
            {
                for entry in entries.into_iter().skip(pos) {
                    self.insert_internal_unlocked(txn_id, entry, false)?;
                }
                return Ok(());
            }

            // Incremental size tracking: `encoded_leaf_page_len`
            // walks every entry, so calling it on each loop iteration
            // is O(N²). We compute it once for the starting leaf and
            // then accumulate per-entry deltas using the generic
            // layout's worst-case width. A compact leaf can still be
            // forced to demote by an appended outlier, which makes the
            // existing entries grow too; after the loop we re-resolve the
            // whole leaf under its effective layout and pop the overfilled
            // tail back into the batch if it no longer fits.
            let start_pos = pos;
            let mut leaf_bytes = encoded_leaf_page_len(&leaf);
            while pos < entries.len() {
                let entry_len = encoded_index_entry_len(&entries[pos]) + SLOT_OFFSET_BYTES;
                if leaf_bytes + entry_len > PAGE_SIZE {
                    break;
                }
                leaf_bytes += entry_len;
                let cloned = entries[pos].clone();
                leaf.entries.push(cloned);
                pos += 1;
            }
            // Re-resolve the leaf onto the layout it will actually encode
            // under. The fill loop scored the *existing* entries under the
            // leaf's PRE-append `key_layout` (via the cached size) and the
            // *new* entries under GENERIC widths. That estimate is blind to
            // a layout widening: a wide-gap appended key (e.g. a disjoint
            // high key range inserted after a low range) can push a compact
            // leaf from `compact_int_u16` (2 B/key) to `compact_int_u32`
            // (4 B/key) or GENERIC, which grows EVERY existing entry too.
            // Without re-resolving, `recompute_cached_size` would score the
            // page under the stale narrow layout, undercount, pass the size
            // check, and then overflow at serialize time (error 1118 "index
            // page is too large" — see
            // insert_select_review::btree_batch_single_int_leaf_overflow_known_bug).
            resolve_appended_leaf_layout(&mut leaf, &mut pos, start_pos);

            if pos == entries.len() {
                self.write_page(txn_id, leaf_id, IndexPage::Leaf(leaf))?;
                return Ok(());
            }

            let new_leaf_id = self.alloc_page(txn_id)?;
            let mut new_entries = Vec::new();
            let mut new_leaf_bytes = LEAF_HEADER_BYTES;
            while pos < entries.len() {
                let entry_len = encoded_index_entry_len(&entries[pos]);
                if !new_entries.is_empty()
                    && new_leaf_bytes + entry_len + SLOT_OFFSET_BYTES > PAGE_SIZE
                {
                    break;
                }
                new_leaf_bytes += entry_len + SLOT_OFFSET_BYTES;
                new_entries.push(entries[pos].clone());
                pos += 1;
            }
            let separator = new_entries.first().cloned().ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "bulk append produced empty leaf",
                )
            })?;
            leaf.next_leaf = Some(new_leaf_id);
            // Preserve the source page's writer bound onto both split
            // halves before rebuilding them, so a snapshot older than
            // the source page's max writer still gets a correct answer
            // from the Phase 2 shortcut. `write_page(txn_id, ...)`
            // below further raises the bound to the current writer.
            let source_bound = leaf.page_max_trx_id;
            // Resolve layout on both halves of the append-driven
            // split: this is the OLTP append fast path for sorted PKs,
            // and a `new_entries` block that's all single-column INT
            // (which is what sequential PK inserts produce) should
            // pack as compact_int_u16 rather than generic.
            let mut leaf = IndexLeafPage::new_with_resolved_layout(
                std::mem::take(&mut leaf.entries),
                leaf.next_leaf,
            );
            leaf.set_page_max_trx_id(source_bound);
            let mut new_leaf = IndexLeafPage::new_with_resolved_layout(new_entries, None);
            new_leaf.set_page_max_trx_id(source_bound);
            // Publish the new right leaf before exposing it through the
            // left leaf's next_leaf. Readers do not take structure_lock,
            // so the opposite order lets a range scan follow a dangling
            // page id and decode the file hole as an all-zero index page.
            self.write_page(txn_id, new_leaf_id, IndexPage::Leaf(new_leaf))?;
            self.write_page(txn_id, leaf_id, IndexPage::Leaf(leaf))?;
            self.insert_separator(txn_id, path, leaf_id, new_leaf_id, separator)?;
        }
        Ok(())
    }

    /// Apply the O(1) compact-layout demote check used by every leaf
    /// mutation: if a non-generic layout can't admit `entry.key`,
    /// drop to generic before the size check. Returns the previous
    /// layout tuple so the caller can roll it back if the post-insert
    /// size check fails.
    fn demote_leaf_layout_if_needed(
        leaf: &mut IndexLeafPage,
        entry: &IndexEntry,
    ) -> (u8, i64, Option<LayoutAux>) {
        let saved = (leaf.key_layout, leaf.base_time, leaf.layout_aux);
        if leaf.key_layout == KEY_LAYOUT_GENERIC {
            return saved;
        }
        let still_fits = match leaf.key_layout {
            KEY_LAYOUT_COMPACT_TIME_U32 => compact_time_delta(&entry.key, leaf.base_time).is_ok(),
            KEY_LAYOUT_COMPACT_DATE_U16 => compact_date_delta(&entry.key, leaf.base_time).is_ok(),
            KEY_LAYOUT_COMPACT_DECIMAL_I32 => {
                compact_decimal_magnitude_i32(&entry.key, leaf.base_time).is_ok()
            }
            KEY_LAYOUT_COMPACT_DECIMAL_I64 => {
                compact_decimal_magnitude_i64(&entry.key, leaf.base_time).is_ok()
            }
            KEY_LAYOUT_COMPACT_INT_U16 => compact_int_delta_u16(&entry.key, leaf.base_time).is_ok(),
            KEY_LAYOUT_COMPACT_INT_U32 => compact_int_delta_u32(&entry.key, leaf.base_time).is_ok(),
            KEY_LAYOUT_COMPACT_UINT_U16 => {
                compact_uint_delta_u16(&entry.key, leaf.base_time).is_ok()
            }
            KEY_LAYOUT_COMPACT_UINT_U32 => {
                compact_uint_delta_u32(&entry.key, leaf.base_time).is_ok()
            }
            KEY_LAYOUT_COMPACT_INT2 => match leaf.layout_aux {
                Some(aux) => int2_admits(
                    &entry.key,
                    leaf.base_time,
                    aux.col2_base,
                    aux.width1,
                    aux.width2,
                ),
                None => false,
            },
            _ => false,
        };
        if !still_fits {
            leaf.key_layout = KEY_LAYOUT_GENERIC;
            leaf.base_time = 0;
            leaf.layout_aux = None;
            // Layout changed; every existing entry now encodes under
            // GENERIC widths so the cached size needs a full rebuild.
            leaf.recompute_cached_size();
        }
        saved
    }

    /// Persistent in-place mutations require logical WAL coverage. Memory
    /// durability has no crash-redo obligation and can modify the same
    /// resident bytes without serializing a replacement page per entry.
    fn lazy_write_eligible(&self, page_id: u64) -> Result<bool, QueryError> {
        // Deferred index buffering runs with `current() == None` (the record's
        // LSN is unknown until the batched commit-time append), but the leaf is
        // still logically WAL-backed — the parked IndexBatch record covers it.
        // Without the `is_deferred()` escape the in-place fast path would be
        // disabled here, routing every deferred update onto the far heavier
        // read_leaf + `write_page`/`store_page_decoded_lazy` path (~2× slower,
        // measured). Unlogged physical apply retains the source's page-image
        // path even when its resident cache uses memory durability.
        if (self.durability != DurabilityMode::Memory
            || physical_wal_apply_active()
            || self.log_atomics.physical_replica())
            && btree_logical_wal::current().is_none()
            && !btree_logical_wal::is_deferred()
        {
            return Ok(false);
        }
        let _ = page_id;
        Ok(true)
    }

    fn lazy_mutation_lsn(&self) -> u64 {
        if self.durability == DurabilityMode::Memory
            && btree_logical_wal::current().is_none()
            && !btree_logical_wal::is_deferred()
            && !physical_wal_apply_active()
            && !self.log_atomics.physical_replica()
        {
            0
        } else {
            // Explicit logical/replica replay retains its existing WAL anchor,
            // including a memory-backed cache of the source's index pages.
            self.log_atomics.next_lsn().max(1)
        }
    }

    /// Fast path for `insert_internal`: try to insert `entry` directly
    /// into the buffer pool's encoded bytes without materializing the leaf.
    /// Returns:
    ///
    ///   - `Some(Inserted)` — entry installed; mark the index dirty.
    ///   - `Some(Duplicate)` — entry already present; no-op.
    ///   - `Some(NeedsSplit)` — leaf would overflow with this entry;
    ///     the mutation was rolled back, caller must take the slow
    ///     take/clone/split path.
    ///   - `None` — frame isn't suitable (not cached / FPI needed /
    ///     unlogged persistent mutation); caller falls back to the slow path.
    fn try_insert_leaf_in_place(
        &self,
        txn_id: u64,
        leaf_id: u64,
        entry: &IndexEntry,
    ) -> Result<Option<InPlaceInsert>, QueryError> {
        if !self.lazy_write_eligible(leaf_id)? {
            return Ok(None);
        }
        let key = self.page_key(leaf_id);
        let peek_lsn = self.lazy_mutation_lsn();
        if let Some(result) = self
            .buffer_pool
            .try_modify_raw_page(&key, peek_lsn, |bytes| {
                let result = try_insert_raw_leaf(bytes, entry, txn_id)?;
                let modified = matches!(result, Some(InPlaceInsert::Inserted));
                Ok((modified, result))
            })?
        {
            if result.is_some() {
                return Ok(result);
            }
        }
        self.buffer_pool
            .modify_index_page_fallback(&key, peek_lsn, |page| {
                let IndexPage::Leaf(leaf) = page else {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: expected leaf page",
                    ));
                };
                let pos = match leaf.entries.binary_search(entry) {
                    Ok(_) => return Ok((false, InPlaceInsert::Duplicate)),
                    Err(p) => p,
                };
                leaf.entries.insert(pos, entry.clone());
                let saved = Self::demote_leaf_layout_if_needed(leaf, entry);
                // demote already rebuilt cached_size if it triggered;
                // otherwise it's still in the pre-insert state and we
                // just need to delta this one entry.
                if saved.0 == leaf.key_layout {
                    leaf.bump_size_for_insert(entry);
                }
                if encoded_leaf_page_len(leaf) <= PAGE_SIZE {
                    // Phase 1b: this in-place fast path bypasses
                    // `write_page`, so stamp `page_max_trx_id` inline
                    // to keep the invariant. Monotonic (max-only)
                    // matches the `write_page` choke-point semantics.
                    leaf.raise_page_max_trx_id(txn_id);
                    return Ok((true, InPlaceInsert::Inserted));
                }
                // Overflow: revert insertion + layout demotion. The
                // O(N) `Vec::remove` is the same shift the insert
                // paid, so the worst-case cost of taking the slow
                // path here is one extra shift — vs. the slow path
                // unconditionally cloning the entire `Vec<IndexEntry>`.
                leaf.entries.remove(pos);
                leaf.key_layout = saved.0;
                leaf.base_time = saved.1;
                leaf.layout_aux = saved.2;
                // Restoring layout invalidates any delta we'd done; full
                // recompute is safe and rare (only on overflow-revert).
                leaf.recompute_cached_size();
                Ok((false, InPlaceInsert::NeedsSplit))
            })
    }

    /// Insert the longest safe prefix of a sorted batch into one leaf.
    ///
    /// Every entry must route to this leaf, with the topology guard retained
    /// through mutation. The parent fence may exceed the observed last key:
    /// low-cardinality secondary keys commonly extend a non-rightmost leaf's
    /// last RowId. Consume those rows together rather than repeating page
    /// latching, navigation and slot-directory movement for every row.
    ///
    /// `None` means the first entry needs a structural fallback (normally a
    /// split). `Some(n)` consumes `n > 0` entries; exact duplicates count as
    /// consumed but do not dirty the page.
    fn try_insert_leaf_run_in_place<const BATCH: bool>(
        &self,
        txn_id: u64,
        pinned: &PinnedFrame,
        run: RoutedIndexEntryRun<'_>,
    ) -> Result<Option<usize>, QueryError> {
        let run = run.0;
        let entries = run.0;
        debug_assert!(!entries.is_empty());
        let leaf_id = pinned.frame.key.page_id;
        if !self.lazy_write_eligible(leaf_id)? {
            return Ok(None);
        }
        let peek_lsn = self.lazy_mutation_lsn();
        // Reuse an existing packed image for point writes too, but only
        // real batches may create one. Fresh single-row workloads keep
        // their existing allocation-free encoded-page kernel.
        if BATCH || pinned.frame.has_packed_leaf() {
            if let Some(result) = self
                .buffer_pool
                .try_insert_packed_leaf_run(pinned, peek_lsn, entries, txn_id, BATCH)?
            {
                return Ok(match result {
                    PackedLeafRunOutcome::Consumed(consumed) => Some(consumed),
                    PackedLeafRunOutcome::Structural => None,
                });
            }
        }
        // Retain the navigation pin through mutation. Re-looking up the same
        // leaf adds a shard lock, Arc clone and pin/unpin on every local run.
        // Separate callbacks keep both capture lists small. Capturing a run
        // and a second view of its entries together forces indirect argument
        // passing on common ABIs, even for a single-row mutation.
        let consumed = if entries.len() == 1 {
            let entry = &entries[0];
            self.buffer_pool
                .modify_pinned_raw_page(pinned, peek_lsn, |bytes| {
                    let result = try_insert_raw_leaf_search::<BATCH>(bytes, entry, txn_id)?;
                    Ok((
                        matches!(result, Some(InPlaceInsert::Inserted)),
                        matches!(
                            result,
                            Some(InPlaceInsert::Inserted | InPlaceInsert::Duplicate)
                        )
                        .then_some(1),
                    ))
                })?
        } else {
            self.buffer_pool
                .modify_pinned_raw_page(pinned, peek_lsn, |bytes| {
                    try_insert_raw_leaf_run_with_bound::<true>(bytes, run, txn_id)
                })?
        };
        if let Some(consumed) = consumed {
            return Ok(Some(consumed));
        }

        // The raw path already owns a pin; clone the file key only when the
        // decoded compatibility path actually needs another lookup.
        let key = self.page_key(leaf_id);
        let consumed = self
            .buffer_pool
            .modify_index_page_fallback(&key, peek_lsn, |page| {
                let IndexPage::Leaf(leaf) = page else {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: expected leaf page",
                    ));
                };
                let mut consumed = 0usize;
                let mut modified = false;
                for entry in entries {
                    let pos = match leaf.entries.binary_search(entry) {
                        Ok(_) => {
                            consumed += 1;
                            continue;
                        }
                        Err(pos) => pos,
                    };
                    leaf.entries.insert(pos, entry.clone());
                    let saved = Self::demote_leaf_layout_if_needed(leaf, entry);
                    if saved.0 == leaf.key_layout {
                        leaf.bump_size_for_insert(entry);
                    }
                    if encoded_leaf_page_len(leaf) <= PAGE_SIZE {
                        consumed += 1;
                        modified = true;
                        continue;
                    }
                    leaf.entries.remove(pos);
                    leaf.key_layout = saved.0;
                    leaf.base_time = saved.1;
                    leaf.layout_aux = saved.2;
                    leaf.recompute_cached_size();
                    break;
                }
                if modified {
                    leaf.raise_page_max_trx_id(txn_id);
                }
                Ok((modified, consumed))
            })?;
        Ok(consumed.filter(|consumed| *consumed > 0))
    }

    /// Fast path for `delete_internal`: try to remove `entry` directly
    /// from the buffer pool's encoded bytes without materializing the leaf.
    /// Returns:
    ///
    ///   - `Some(Deleted)` — entry removed; caller still runs
    ///     `rebalance_after_leaf_delete` to handle underflow.
    ///   - `Some(NotFound)` — entry wasn't present; no-op.
    ///   - `None` — frame isn't suitable; caller falls back.
    fn try_delete_leaf_in_place(
        &self,
        txn_id: u64,
        leaf_id: u64,
        entry: &IndexEntry,
    ) -> Result<Option<InPlaceDelete>, QueryError> {
        if !self.lazy_write_eligible(leaf_id)? {
            return Ok(None);
        }
        let key = self.page_key(leaf_id);
        let peek_lsn = self.lazy_mutation_lsn();
        if let Some(result) = self
            .buffer_pool
            .try_modify_raw_page(&key, peek_lsn, |bytes| {
                let result = try_delete_raw_leaf(bytes, entry, txn_id)?;
                let modified = matches!(result, Some(InPlaceDelete::Deleted { .. }));
                Ok((modified, result))
            })?
        {
            if result.is_some() {
                return Ok(result);
            }
        }
        self.buffer_pool
            .modify_index_page_fallback(&key, peek_lsn, |page| {
                let IndexPage::Leaf(leaf) = page else {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: expected leaf page",
                    ));
                };
                let Ok(pos) = leaf.entries.binary_search(entry) else {
                    return Ok((false, InPlaceDelete::NotFound));
                };
                // bump_size_for_remove uses current layout — same encoding
                // that `entries[pos]` was contributing — so it's correct
                // even if we discarded the removed entry afterwards.
                let removed_clone = leaf.entries[pos].clone();
                leaf.entries.remove(pos);
                leaf.bump_size_for_remove(&removed_clone);
                // Phase 1b: bypasses `write_page`; stamp inline. See
                // `try_insert_leaf_in_place` for rationale.
                leaf.raise_page_max_trx_id(txn_id);
                Ok((
                    true,
                    InPlaceDelete::Deleted {
                        was_first: pos == 0,
                    },
                ))
            })
    }

    /// Shared-topology delete probe. It performs the same raw/decoded leaf
    /// mutation as `try_delete_leaf_in_place`, but declines *before writing*
    /// if the removal would take a non-root leaf below the minimum fill
    /// threshold. Deleting the first key deliberately keeps the old parent
    /// separator as a conservative lower bound: it is smaller than the new
    /// first key, so probes and inserts in that now-empty gap still route to
    /// the correct child. Underflow/merge rebuilds exact separators through
    /// the structural oracle.
    fn try_delete_leaf_nonstructural_in_place(
        &self,
        txn_id: u64,
        pinned: &PinnedFrame,
        entry: &IndexEntry,
        root_page: u64,
    ) -> Result<Option<InPlaceDelete>, QueryError> {
        let leaf_id = pinned.frame.key.page_id;
        if !self.lazy_write_eligible(leaf_id)? {
            return Ok(None);
        }
        let is_root = leaf_id == root_page;
        let key = self.page_key(leaf_id);
        let peek_lsn = self.lazy_mutation_lsn();
        if let Some(result) =
            self.buffer_pool
                .try_modify_existing_packed_leaf(pinned, peek_lsn, |leaf| {
                    leaf.delete(entry, txn_id, is_root)
                        .map(|result| (matches!(result, InPlaceDelete::Deleted { .. }), result))
                })?
        {
            return Ok(Some(result));
        }
        let result = self
            .buffer_pool
            .modify_pinned_raw_page(pinned, peek_lsn, |bytes| {
                let result = try_delete_raw_leaf_nonstructural(bytes, entry, txn_id, is_root)?;
                Ok((
                    matches!(result, Some(InPlaceDelete::Deleted { .. })),
                    result,
                ))
            })?;
        if result.is_some() {
            return Ok(result);
        }
        self.buffer_pool
            .modify_index_page_fallback(&key, peek_lsn, |page| {
                let IndexPage::Leaf(leaf) = page else {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: expected leaf page",
                    ));
                };
                let Ok(pos) = leaf.entries.binary_search(entry) else {
                    return Ok((false, Some(InPlaceDelete::NotFound)));
                };
                let removed = &leaf.entries[pos];
                let removed_bytes =
                    IndexLeafPage::entry_bytes_in_layout(removed, leaf.key_layout, leaf.layout_aux)
                        .saturating_add(SLOT_OFFSET_BYTES);
                let live_after = encoded_leaf_page_len(leaf).saturating_sub(removed_bytes);
                if !is_root && (leaf.entries.len() <= 1 || live_after < INDEX_MIN_PAGE_BYTES) {
                    return Ok((false, None));
                }
                let removed = leaf.entries.remove(pos);
                leaf.bump_size_for_remove(&removed);
                leaf.raise_page_max_trx_id(txn_id);
                Ok((
                    true,
                    Some(InPlaceDelete::Deleted {
                        was_first: pos == 0,
                    }),
                ))
            })
            .map(Option::flatten)
    }

    #[cfg(test)]
    fn insert_internal(
        &self,
        txn_id: u64,
        entry: IndexEntry,
        flush_strict: bool,
    ) -> Result<(), QueryError> {
        let _structure = self.structure_lock.write();
        let _mutation = self.begin_mutation();
        self.insert_internal_unlocked(txn_id, entry, flush_strict)
    }

    /// Single-entry counterpart of `insert_entries_with_lsn`'s concurrent
    /// leaf path. Route under a shared topology guard, mutate only the target
    /// leaf under its page latch, and take the exclusive structural guard only
    /// when the leaf actually needs a split. COM_QUERY/JDBC OLTP normally
    /// emits one row per INSERT, so leaving this case on `insert_internal`
    /// serialized every writer to an index even though the batch API already
    /// proved that fitting leaves are independent.
    fn insert_entry_concurrent(&self, txn_id: u64, entry: IndexEntry) -> Result<(), QueryError> {
        let completed_without_structure_change = {
            let _structure = self.structure_lock.read();
            let leaf_id = self
                .find_leaf_for_entry_with_candidate(&entry, &entry)?
                .0
                .frame
                .key
                .page_id;
            let (leaf_latch, same_bucket) = self.lock_leaf_latches(leaf_id, leaf_id)?;
            let _mutation = self.begin_mutation();
            let result = self.try_insert_leaf_in_place(txn_id, leaf_id, &entry)?;
            drop(same_bucket);
            drop(leaf_latch);
            match result {
                Some(InPlaceInsert::Inserted) => {
                    self.dirty.store(true, Ordering::Release);
                    true
                }
                Some(InPlaceInsert::Duplicate) => true,
                Some(InPlaceInsert::NeedsSplit) | None => false,
            }
        };
        if completed_without_structure_change {
            return Ok(());
        }
        self.insert_with_prepared_structure(txn_id, entry)
    }

    /// Load and pin the descent plus every page a cascading split can need
    /// before taking the topology write lock. Future allocation candidates
    /// are clean zero frames, not allocated tree pages: reserving cache space
    /// never publishes them in metadata or the leaf chain. A changed allocator
    /// snapshot retries before mutation. Small pools retain the bounded legacy
    /// path rather than pinning their entire working set for an optimization.
    fn prepare_insert_structure(
        &self,
        entry: &IndexEntry,
    ) -> Result<Option<PreparedIndexInsert>, QueryError> {
        if self.durability == DurabilityMode::Memory {
            return Ok(None);
        }
        let _structure = self.structure_lock.read();
        let mut path = Vec::new();
        let leaf = self.find_leaf_for_entry(entry, &mut path)?;
        let allocations = path.len() + 2; // one split per level + new root
        let pin_count = path.len() + 2 + allocations; // ancestors, leaf, metadata
        if pin_count as u64 > self.buffer_pool.capacity_pages.load(Ordering::Relaxed) / 16 {
            return Ok(None);
        }
        let (next, free, root) = {
            let state = self.meta_state.lock();
            (
                state.meta.next_page_id,
                state.meta.free_page_head,
                state.meta.root_page,
            )
        };
        let mut pins = Vec::with_capacity(pin_count);
        for page in path
            .iter()
            .map(|(page, _)| *page)
            .chain([leaf, INDEX_META_PAGE])
        {
            pins.push(self.buffer_pool.pin_page(self.page_key(page))?);
        }
        // Decode and update the leaf outside the topology write lock. The
        // frame epoch is captured under the same byte latch as the snapshot;
        // both the route and the leaf are revalidated before publication.
        let mut route_epochs: Vec<_> = pins[..path.len()]
            .iter()
            .map(|pin| pin.frame.dirty_epoch.load(Ordering::Acquire))
            .collect();
        let (mut leaf_image, leaf_epoch) =
            if let Some(snapshot) = pins[path.len()].packed_leaf_owned_snapshot()? {
                snapshot
            } else {
                pins[path.len()].with_index_raw_bytes(|bytes| {
                    let IndexPage::Leaf(image) = decode_validated_index_tree_page(bytes)? else {
                        return Err(error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "corrupt index: expected leaf page",
                        ));
                    };
                    Ok((
                        image,
                        pins[path.len()].frame.dirty_epoch.load(Ordering::Acquire),
                    ))
                })?
            };
        route_epochs.push(leaf_epoch);
        let duplicate = !Self::insert_into_owned_leaf(&mut leaf_image, entry);
        let allocations = if duplicate || encoded_leaf_page_len(&leaf_image) <= PAGE_SIZE {
            0
        } else {
            allocations
        };
        let mut free_cursor = free;
        let mut next_cursor = next;
        for _ in 0..allocations {
            let page = if let Some(page) = free_cursor {
                free_cursor = match self.read_page(page)?.as_ref() {
                    IndexPage::Free(free) => free.next_free,
                    _ => {
                        return Err(error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "corrupt index: freelist head is not a free page",
                        ))
                    }
                };
                page
            } else {
                let page = next_cursor;
                next_cursor = next_cursor.checked_add(1).ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "index page range overflow",
                    )
                })?;
                page
            };
            pins.push(self.buffer_pool.pin_page(self.page_key(page))?);
        }
        Ok(Some(PreparedIndexInsert {
            pins,
            route_epochs,
            path,
            leaf_id: leaf,
            leaf: leaf_image,
            duplicate,
            next_page: next,
            free_page: free,
            root_page: root,
        }))
    }

    fn insert_with_prepared_structure(
        &self,
        txn_id: u64,
        entry: IndexEntry,
    ) -> Result<(), QueryError> {
        #[cfg(test)]
        if serial_insert_lock_oracle() {
            let _structure = self.structure_lock.write();
            let _mutation = self.begin_mutation();
            return self.insert_internal_unlocked(txn_id, entry, false);
        }
        let _preparation = self.split_prepare.lock();
        for _ in 0..2 {
            // Preparation only reads/pins cache pages. If speculative
            // admission cannot complete, release its pins and let the
            // canonical path perform just the mandatory reads/allocations.
            let prepared = self.prepare_insert_structure(&entry).unwrap_or(None);
            #[cfg(test)]
            INDEX_STRUCTURE_PREPARED_TEST_HOOK.with(|hook| {
                if let Some(hook) = hook.borrow_mut().take() {
                    hook();
                }
            });
            let _structure = self.structure_lock.write();
            if let Some(prepared) = &prepared {
                let current = self.meta_state.lock();
                if current.meta.next_page_id != prepared.next_page
                    || current.meta.free_page_head != prepared.free_page
                    || current.meta.root_page != prepared.root_page
                    || prepared.pins.iter().any(|pin| {
                        !matches!(
                            FrameIoState::from_u8(pin.frame.state.load(Ordering::Acquire)),
                            FrameIoState::Ready | FrameIoState::Flushing
                        )
                    })
                    || prepared
                        .pins
                        .iter()
                        .zip(&prepared.route_epochs)
                        .any(|(pin, epoch)| pin.frame.dirty_epoch.load(Ordering::Acquire) != *epoch)
                {
                    continue;
                }
            }
            let _mutation = self.begin_mutation();
            if let Some(prepared) = prepared {
                if prepared.duplicate {
                    return Ok(());
                }
                if encoded_leaf_page_len(&prepared.leaf) <= PAGE_SIZE {
                    return self.write_page(
                        txn_id,
                        prepared.leaf_id,
                        IndexPage::Leaf(prepared.leaf),
                    );
                }
                return self.split_leaf(txn_id, prepared.leaf_id, prepared.leaf, prepared.path);
            }
            return self.insert_internal_unlocked(txn_id, entry, false);
        }
        // A continuously changing topology must not starve this insertion.
        let _structure = self.structure_lock.write();
        let _mutation = self.begin_mutation();
        self.insert_internal_unlocked(txn_id, entry, false)
    }

    fn insert_into_owned_leaf(leaf: &mut IndexLeafPage, entry: &IndexEntry) -> bool {
        match leaf.entries.binary_search(entry) {
            Ok(_) => return false,
            Err(pos) => leaf.entries.insert(pos, entry.clone()),
        }
        let layout = leaf.key_layout;
        Self::demote_leaf_layout_if_needed(leaf, entry);
        if leaf.key_layout == layout {
            leaf.bump_size_for_insert(entry);
        }
        true
    }

    fn insert_internal_unlocked(
        &self,
        txn_id: u64,
        entry: IndexEntry,
        flush_strict: bool,
    ) -> Result<(), QueryError> {
        let mut path = Vec::new();
        let leaf_id = self.find_leaf_for_entry(&entry, &mut path)?;
        // Try mutating the resident encoded leaf before materializing it.
        match self.try_insert_leaf_in_place(txn_id, leaf_id, &entry)? {
            Some(InPlaceInsert::Inserted) => {
                self.dirty.store(true, Ordering::Release);
                if flush_strict {
                    self.mark_dirty()?;
                }
                return Ok(());
            }
            Some(InPlaceInsert::Duplicate) => return Ok(()),
            Some(InPlaceInsert::NeedsSplit) | None => {}
        }
        let mut leaf = self.read_leaf(leaf_id)?;
        if !Self::insert_into_owned_leaf(&mut leaf, &entry) {
            return Ok(());
        }
        if encoded_leaf_page_len(&leaf) <= PAGE_SIZE {
            self.write_page(txn_id, leaf_id, IndexPage::Leaf(leaf))?;
        } else {
            self.split_leaf(txn_id, leaf_id, leaf, path)?;
        }
        if flush_strict {
            self.mark_dirty()?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn delete(&mut self, key: IndexKey, row_id: RowId) -> Result<(), QueryError> {
        self.delete_internal(txn_id_for_ddl(), key, row_id, true)
    }

    /// Test-only: highest page id the on-disk file is sized for. Used
    /// by Phase 3 internal-page layout tests to iterate every page
    /// without needing a parent-pointer accessor.
    #[cfg(test)]
    pub(crate) fn allocated_page_count_for_tests(&self) -> u64 {
        self.meta_state.lock().meta.next_page_id
    }

    #[cfg(test)]
    pub(crate) fn page_is_free_for_tests(&self, page_id: u64) -> Result<bool, QueryError> {
        Ok(matches!(
            self.read_page(page_id)?.as_ref(),
            IndexPage::Free(_)
        ))
    }

    /// Physical page count for this index file. Mirrors what
    /// `file.set_len(next_page_id * PAGE_SIZE)` writes, so it matches
    /// the on-disk byte length without paying for `fs::metadata`.
    /// Used by `Database::index_page_count` to report real
    /// `INDEX_LENGTH` from `information_schema.tables`.
    pub(crate) fn page_count(&self) -> u64 {
        if let Some(spatial) = &self.spatial {
            return spatial.count_pages();
        }
        self.meta_state.lock().meta.next_page_id
    }

    fn delete_deferred(&self, txn_id: u64, key: IndexKey, row_id: RowId) -> Result<(), QueryError> {
        if let Some(spatial) = &self.spatial {
            return spatial.apply(
                txn_id,
                [IndexBatchEntry {
                    kind: WalIndexOpKind::Delete,
                    key,
                    row_id,
                }],
            );
        }
        let entry = IndexEntry { key, row_id };
        if self.durability != DurabilityMode::Memory
            && txn_wal_buffer_index_enabled()
            && txn_wal_buffering_active(&self.log, txn_id)
            && !btree_logical_wal::is_deferred()
            && !physical_wal_apply_active()
            && self.online_build_log.is_none()
        {
            if let Some(reservation) = self.buffer_pool.reserve_deferred_wal_pages(1) {
                let payload = WalPayload::IndexBatch {
                    path: self.relative_path(),
                    ops: vec![Self::encode_wal_index_op(WalIndexOpKind::Delete, &entry)?],
                };
                let _logical = btree_logical_wal::LsnScope::new(None);
                btree_logical_wal::begin_deferred();
                let applied = self.try_delete_entry_concurrent(txn_id, &entry);
                let keys = btree_logical_wal::take_deferred();
                let (payload, keys, _reservation) = if matches!(applied, Ok(true)) {
                    match push_reserved_txn_wal_record(
                        &self.log,
                        txn_id,
                        payload,
                        keys,
                        Some(reservation),
                    ) {
                        Ok(()) => return Ok(()),
                        Err(owned) => owned,
                    }
                } else {
                    (payload, keys, Some(reservation))
                };
                // Release topology/leaf latches before WAL publication. A
                // structural decline has not changed the leaf; a successful
                // deferred mutation keeps its pin until the eager fallback
                // has assigned a real LSN.
                let appended = crate::wal::LogManager::pre_encode_record(txn_id, payload)
                    .and_then(|pre| crate::wal::LogManager::append_pre_encoded_to(&self.log, pre));
                let lsn = match appended {
                    Ok(lsn) => {
                        for key in &keys {
                            self.buffer_pool.finalize_deferred(key, lsn);
                        }
                        lsn
                    }
                    Err(err) => {
                        for key in &keys {
                            let _ = self.buffer_pool.discard_page(key);
                        }
                        return Err(err);
                    }
                };
                if applied? {
                    return Ok(());
                }
                let _logical = btree_logical_wal::LsnScope::new(Some(lsn));
                return self.delete_entry_concurrent(txn_id, entry);
            }
        }
        // Symmetric with `insert_deferred`: always log via `IndexBatch`
        // so we don't emit a 32 KB per-page before/after image for a
        // single B-tree leaf modification. Recovery replays via
        // `redo_index_batch` → `delete_internal`.
        self.apply_single_index_op_logged(txn_id, WalIndexOpKind::Delete, entry, |index, entry| {
            index.delete_entry_concurrent(txn_id, entry)
        })
    }

    /// Remove a single entry without excluding readers or writers on other
    /// leaves when the removal provably cannot change the B-tree topology.
    /// Underflow remains on `delete_internal_unlocked`, which is the
    /// correctness oracle for separator refresh/borrow/merge. A first-key
    /// removal alone leaves a safe lower-bound separator until a structural
    /// rebuild refreshes it.
    fn delete_entry_concurrent(&self, txn_id: u64, entry: IndexEntry) -> Result<(), QueryError> {
        if self.try_delete_entry_concurrent(txn_id, &entry)? {
            return Ok(());
        }
        let _structure = self.structure_lock.write();
        let _mutation = self.begin_mutation();
        self.delete_internal_unlocked(txn_id, entry.key, entry.row_id, false)
    }

    fn try_delete_entry_concurrent(
        &self,
        txn_id: u64,
        entry: &IndexEntry,
    ) -> Result<bool, QueryError> {
        let completed_without_structure_change = {
            let _structure = self.structure_lock.read();
            let pinned = self.find_leaf_for_entry_with_candidate(entry, entry)?.0;
            let leaf_id = pinned.frame.key.page_id;
            let (leaf_latch, same_bucket) = self.lock_leaf_latches(leaf_id, leaf_id)?;
            let _mutation = self.begin_mutation();
            let root_page = self.root_page.load(Ordering::Acquire);
            let result =
                self.try_delete_leaf_nonstructural_in_place(txn_id, &pinned, entry, root_page)?;
            drop(same_bucket);
            drop(leaf_latch);
            match result {
                Some(InPlaceDelete::Deleted { .. }) => {
                    self.dirty.store(true, Ordering::Release);
                    true
                }
                Some(InPlaceDelete::NotFound) => true,
                None => false,
            }
        };
        Ok(completed_without_structure_change)
    }

    #[cfg(test)]
    fn delete_internal(
        &self,
        txn_id: u64,
        key: IndexKey,
        row_id: RowId,
        flush_strict: bool,
    ) -> Result<(), QueryError> {
        let _structure = self.structure_lock.write();
        let _mutation = self.begin_mutation();
        self.delete_internal_unlocked(txn_id, key, row_id, flush_strict)
    }

    fn delete_internal_unlocked(
        &self,
        txn_id: u64,
        key: IndexKey,
        row_id: RowId,
        flush_strict: bool,
    ) -> Result<(), QueryError> {
        self.with_structural_recovery(|| {
            self.delete_internal_unlocked_guarded(txn_id, key, row_id, flush_strict)
        })
    }

    fn delete_internal_unlocked_guarded(
        &self,
        txn_id: u64,
        key: IndexKey,
        row_id: RowId,
        flush_strict: bool,
    ) -> Result<(), QueryError> {
        let entry = IndexEntry { key, row_id };
        let mut path = Vec::new();
        let leaf_id = self.find_leaf_for_entry(&entry, &mut path)?;
        // Try mutating the resident encoded leaf before materializing it.
        match self.try_delete_leaf_in_place(txn_id, leaf_id, &entry)? {
            Some(InPlaceDelete::Deleted { was_first }) => {
                self.dirty.store(true, Ordering::Release);
                self.rebalance_after_leaf_delete(txn_id, leaf_id, path, was_first)?;
                if flush_strict {
                    self.mark_dirty()?;
                }
                return Ok(());
            }
            Some(InPlaceDelete::NotFound) => return Ok(()),
            None => {}
        }
        let mut leaf = self.read_leaf(leaf_id)?;
        let Ok(pos) = leaf.entries.binary_search(&entry) else {
            return Ok(());
        };
        let was_first = pos == 0;
        let removed_clone = leaf.entries[pos].clone();
        leaf.entries.remove(pos);
        leaf.bump_size_for_remove(&removed_clone);
        self.write_page(txn_id, leaf_id, IndexPage::Leaf(leaf))?;
        self.rebalance_after_leaf_delete(txn_id, leaf_id, path, was_first)?;
        if flush_strict {
            self.mark_dirty()?;
        }
        Ok(())
    }

    fn get(&self, key: &IndexKey, dedup_row_ids: bool) -> Result<Vec<RowId>, QueryError> {
        if let Some(spatial) = &self.spatial {
            return spatial.lookup(key);
        }
        let _structure = self.structure_lock.read_recursive();
        self.ensure_healthy()?;
        let mut rows = Vec::new();
        let mut seen = dedup_row_ids.then(crate::util::fast_hash::ah_hash_set);
        let mut pinned = self.pin_lookup_leaf(key)?;
        let mut leaf_id = pinned.frame.key.page_id;
        let mut chain = IndexPageProgress::new(leaf_id);
        loop {
            let result = match pinned.packed_leaf_lookup(key)? {
                Some((result, _)) => result,
                None => pinned.with_index_raw_bytes(|buf| lookup_leaf_page_bytes(buf, key))?,
            };
            drop(pinned);
            for rid in result.rows {
                push_row_id(&mut rows, &mut seen, rid);
            }
            // Stop unless duplicates may continue on the next leaf.
            if !result.should_continue {
                return Ok(rows);
            }
            let Some(next_leaf) = result.next_leaf else {
                return Ok(rows);
            };
            if next_leaf == leaf_id {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt index: leaf next pointer references itself",
                ));
            }
            chain.advance(next_leaf)?;
            leaf_id = next_leaf;
            pinned = self.buffer_pool.pin_page(self.page_key(leaf_id))?;
        }
    }

    fn get_unique_one(&self, key: &IndexKey) -> Result<Option<RowId>, QueryError> {
        let _structure = self.structure_lock.read_recursive();
        self.ensure_healthy()?;
        let mut pinned = self.pin_lookup_leaf_borrowed(key)?;
        let mut leaf_id = pinned.frame.key.page_id;
        let mut chain = IndexPageProgress::new(leaf_id);
        loop {
            let result = match pinned.packed_leaf_lookup(key)? {
                Some((result, _)) => LeafSingleLookupResult {
                    row: result.rows.into_iter().next(),
                    next_leaf: result.next_leaf,
                    should_continue: result.should_continue,
                },
                None => pinned.with_index_raw_bytes(|buf| lookup_leaf_page_bytes_one(buf, key))?,
            };
            drop(pinned);
            if result.row.is_some() || !result.should_continue {
                return Ok(result.row);
            }
            let Some(next_leaf) = result.next_leaf else {
                return Ok(None);
            };
            chain.advance(next_leaf)?;
            leaf_id = next_leaf;
            pinned = self.buffer_pool.pin_page_borrowed(self.page_key(leaf_id))?;
        }
    }

    fn prefix(&self, prefix: &IndexKey, dedup_row_ids: bool) -> Result<Vec<RowId>, QueryError> {
        let _structure = self.structure_lock.read_recursive();
        self.ensure_healthy()?;
        let mut rows = Vec::new();
        let mut seen = dedup_row_ids.then(crate::util::fast_hash::ah_hash_set);
        let mut pinned = self.pin_lookup_leaf(prefix)?;
        let mut leaf_id = pinned.frame.key.page_id;
        let mut chain = IndexPageProgress::new(leaf_id);
        loop {
            let result = pinned.with_index_raw_bytes(|buf| prefix_leaf_page_bytes(buf, prefix))?;
            drop(pinned);
            for rid in result.rows {
                push_row_id(&mut rows, &mut seen, rid);
            }
            if !result.should_continue {
                return Ok(rows);
            }
            let Some(next_leaf) = result.next_leaf else {
                return Ok(rows);
            };
            chain.advance(next_leaf)?;
            leaf_id = next_leaf;
            pinned = self.buffer_pool.pin_page(self.page_key(leaf_id))?;
        }
    }

    fn get_entries(
        &self,
        key: &IndexKey,
        dedup_row_ids: bool,
    ) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        self.get_entries_with_bound(key, dedup_row_ids)
            .map(|(entries, _page_max_trx_id)| entries)
    }

    fn get_entries_with_bound(
        &self,
        key: &IndexKey,
        dedup_row_ids: bool,
    ) -> Result<(Vec<(IndexKey, RowId)>, u64), QueryError> {
        let _structure = self.structure_lock.read_recursive();
        self.ensure_healthy()?;
        let mut entries = Vec::new();
        let mut seen = dedup_row_ids.then(crate::util::fast_hash::ah_hash_set);
        let pinned = self.pin_lookup_leaf(key)?;
        let page_max_trx_id = self.collect_lookup_entries_from_leaf(
            pinned,
            key,
            0,
            true,
            &mut entries,
            &mut seen,
            &|_, key, row_id| (key, row_id),
        )?;
        Ok((entries, page_max_trx_id))
    }

    fn prefix_entries(
        &self,
        prefix: &IndexKey,
        dedup_row_ids: bool,
    ) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        self.prefix_entries_with_bound(prefix, dedup_row_ids)
            .map(|(entries, _page_max_trx_id)| entries)
    }

    fn prefix_entries_with_bound(
        &self,
        prefix: &IndexKey,
        dedup_row_ids: bool,
    ) -> Result<(Vec<(IndexKey, RowId)>, u64), QueryError> {
        let _structure = self.structure_lock.read_recursive();
        self.ensure_healthy()?;
        let mut entries = Vec::new();
        let mut seen = dedup_row_ids.then(crate::util::fast_hash::ah_hash_set);
        let pinned = self.pin_lookup_leaf(prefix)?;
        let page_max_trx_id = self.collect_lookup_entries_from_leaf(
            pinned,
            prefix,
            0,
            false,
            &mut entries,
            &mut seen,
            &|_, key, row_id| (key, row_id),
        )?;
        Ok((entries, page_max_trx_id))
    }

    /// Fast FULLTEXT posting scan for one exact token.
    ///
    /// Iterates leaves matching the token prefix (`[KeyPart::String(token)]`)
    /// and invokes `visit(row_id, payload_bytes)` for each posting.  The
    /// generic path materialises `Vec<(IndexKey, RowId)>` — one `String`
    /// clone of the (already-known) token and one `Vec<u8>` clone of the
    /// payload bytes per posting; for a `df ≈ 22k` hot query that is
    /// ~44k allocations. This variant hands the `payload` bytes back as a
    /// borrowed slice so the caller can decode inline without cloning the
    /// bytes or the IndexKey.
    ///
    /// The visitor is called in B-tree order (i.e. sorted by payload bytes
    /// within one token, which is *not* row-id order — callers that need a
    /// row-id-sorted stream must sort post-scan).
    pub(crate) fn fulltext_scan_token<F>(&self, token: &str, mut visit: F) -> Result<(), QueryError>
    where
        F: FnMut(RowId, &[u8]) -> Result<(), QueryError>,
    {
        let _structure = self.structure_lock.read_recursive();
        self.ensure_healthy()?;
        let prefix = IndexKey::from(vec![KeyPart::String(token.to_string())]);
        let mut leaf_id = self.find_leaf_for_key_lower_bound(&prefix)?;
        loop {
            let leaf_page = self.read_page(leaf_id)?;
            let leaf = match leaf_page.as_ref() {
                IndexPage::Leaf(leaf) => leaf,
                _ => {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: expected leaf page",
                    ))
                }
            };
            if let Some(next) = leaf.next_leaf {
                if Self::fulltext_prefetch_may_continue(
                    leaf.entries.last().map(|entry| &entry.key),
                    token,
                    None,
                    true,
                ) {
                    self.prefetch_fulltext_leaves(next, token, None, true);
                }
            }
            let start = leaf.entries.partition_point(|entry| entry.key < prefix);
            for entry in &leaf.entries[start..] {
                match entry.key.0.first() {
                    Some(KeyPart::String(t)) if t.as_str() == token => {
                        let payload = match entry.key.0.get(1) {
                            Some(KeyPart::Bytes(b)) => b.as_slice(),
                            _ => &[][..],
                        };
                        visit(entry.row_id, payload)?;
                    }
                    _ => return Ok(()),
                }
            }
            let Some(next_leaf) = leaf.next_leaf else {
                return Ok(());
            };
            leaf_id = next_leaf;
        }
    }

    /// Fast FULLTEXT posting scan for a token prefix (`word*`).
    ///
    /// Iterates leaves matching `[prefix .. prefix_upper)` and invokes
    /// `visit(token_str, row_id, payload_bytes)` — the token is exposed so
    /// prefix expansion can group by concrete token for BM25 scoring.
    /// Same allocation savings vs. `range_entries` as
    /// [`Self::fulltext_scan_token`].
    pub(crate) fn fulltext_scan_prefix<F>(
        &self,
        prefix: &str,
        prefix_upper: Option<&str>,
        mut visit: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(&str, RowId, &[u8]) -> Result<(), QueryError>,
    {
        let _structure = self.structure_lock.read_recursive();
        self.ensure_healthy()?;
        let low_key = IndexKey::from(vec![KeyPart::String(prefix.to_string())]);
        let mut leaf_id = self.find_leaf_for_key_lower_bound(&low_key)?;
        let mut first_leaf = true;
        loop {
            let leaf_page = self.read_page(leaf_id)?;
            let leaf = match leaf_page.as_ref() {
                IndexPage::Leaf(leaf) => leaf,
                _ => {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: expected leaf page",
                    ))
                }
            };
            if let Some(next) = leaf.next_leaf {
                if Self::fulltext_prefetch_may_continue(
                    leaf.entries.last().map(|entry| &entry.key),
                    prefix,
                    prefix_upper,
                    false,
                ) {
                    self.prefetch_fulltext_leaves(next, prefix, prefix_upper, false);
                }
            }
            // Later leaves are already above the routed lower bound.
            let start = if first_leaf {
                first_leaf = false;
                leaf.entries.partition_point(|entry| entry.key < low_key)
            } else {
                0
            };
            for entry in &leaf.entries[start..] {
                let token = match entry.key.0.first() {
                    Some(KeyPart::String(t)) => t.as_str(),
                    _ => continue,
                };
                if token < prefix {
                    continue;
                }
                if !token.starts_with(prefix) {
                    return Ok(());
                }
                if let Some(upper) = prefix_upper {
                    if token >= upper {
                        return Ok(());
                    }
                }
                let payload = match entry.key.0.get(1) {
                    Some(KeyPart::Bytes(b)) => b.as_slice(),
                    _ => &[][..],
                };
                visit(token, entry.row_id, payload)?;
            }
            let Some(next_leaf) = leaf.next_leaf else {
                return Ok(());
            };
            leaf_id = next_leaf;
        }
    }

    fn range_entries(
        &self,
        low: Option<&IndexBound>,
        high: Option<&IndexBound>,
        dedup_row_ids: bool,
    ) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        let _structure = self.structure_lock.read_recursive();
        self.ensure_healthy()?;
        let mut entries = Vec::new();
        let mut seen = dedup_row_ids.then(crate::util::fast_hash::ah_hash_set);
        let mut leaf_id = match low {
            Some(bound) => self.find_leaf_for_key_lower_bound(&bound.key)?,
            None => self.leftmost_leaf()?,
        };
        loop {
            let leaf_page = self.read_page(leaf_id)?;
            let leaf = match leaf_page.as_ref() {
                IndexPage::Leaf(leaf) => leaf,
                _ => {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: expected leaf page",
                    ))
                }
            };
            if let Some(next) = leaf.next_leaf {
                self.prefetch_next_leaves(next);
            }
            for entry in &leaf.entries {
                if index_key_past_high_bound(&entry.key, high) {
                    return Ok(entries);
                }
                if index_key_in_bounds(&entry.key, low, high) {
                    push_index_entry(&mut entries, &mut seen, entry);
                }
            }
            let Some(next_leaf) = leaf.next_leaf else {
                return Ok(entries);
            };
            leaf_id = next_leaf;
        }
    }

    /// Sequentially walk every leaf of the whole index, calling `visit(&entry.key.0[..prefix_len])` on each entry.
    ///
    /// Meant for cases like building the INLJ probe bloom filter that need to "enumerate
    /// all distinct key prefixes". **No visibility filtering**: every persisted entry
    /// in the index is reported, including versions from uncommitted DML — safe for
    /// bloom semantics (a bloom only guarantees no false negatives: every key currently
    /// in the index gets recorded; the rare MVCC-invisible leftover only makes the bloom conservative, never rejecting a valid key).
    ///
    /// - `prefix_len`: key prefix length exposed to the visitor (INLJ's
    ///   `eq_prefix_len`); 0 means the whole key. Values beyond the real key length degrade to the whole key.
    /// - `max_pages`: cap on leaf pages read. When page `max_pages + 1` arrives it
    ///   immediately returns `Ok(false)` without calling the visitor again. 0 means unlimited.
    ///
    /// Returns `Ok(true)` after fully scanning all leaves; `Ok(false)` means early exit at
    /// the cap — the caller must then discard the partially built bloom (it would produce false negatives).
    pub(crate) fn for_each_leaf_key_prefix<F>(
        &self,
        prefix_len: usize,
        max_pages: usize,
        mut visit: F,
    ) -> Result<bool, QueryError>
    where
        F: FnMut(&[KeyPart]),
    {
        let _structure = self.structure_lock.read_recursive();
        self.ensure_healthy()?;
        let mut leaf_id = self.leftmost_leaf()?;
        let mut pages: usize = 0;
        loop {
            if max_pages > 0 && pages >= max_pages {
                return Ok(false);
            }
            let leaf_page = self.read_page(leaf_id)?;
            let leaf = match leaf_page.as_ref() {
                IndexPage::Leaf(leaf) => leaf,
                _ => {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: expected leaf page",
                    ))
                }
            };
            if let Some(next) = leaf.next_leaf {
                self.prefetch_next_leaves(next);
            }
            for entry in &leaf.entries {
                let len = if prefix_len == 0 || prefix_len >= entry.key.0.len() {
                    entry.key.0.len()
                } else {
                    prefix_len
                };
                visit(&entry.key.0[..len]);
            }
            pages += 1;
            let Some(next_leaf) = leaf.next_leaf else {
                return Ok(true);
            };
            leaf_id = next_leaf;
        }
    }

    pub(crate) fn lookup_entries_many_sorted(
        &self,
        keys: &[IndexKey],
        exact: bool,
        dedup_row_ids: bool,
    ) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        debug_assert!(keys.windows(2).all(|pair| pair[0] <= pair[1]));
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let _structure = self.structure_lock.read_recursive();
        self.ensure_healthy()?;
        // Snapshot the process-wide opt-out once. Reading the environment for
        // every probe adds synchronization (and an allocation when it is set).
        static ROUTED_ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        let routed_enabled = *ROUTED_ENABLED.get_or_init(|| {
            match std::env::var("MYDB_INLJ_ENABLE_ROUTED_BATCH_LOOKUP") {
                Ok(value) => {
                    !(value == "0"
                        || value.eq_ignore_ascii_case("false")
                        || value.eq_ignore_ascii_case("off"))
                }
                Err(_) => true,
            }
        });
        // Even small batches benefit from probing raw slots instead of
        // materializing every entry in each visited leaf.
        if keys.len() >= 2 && routed_enabled {
            return self.lookup_entries_many_sorted_routed(
                keys,
                exact,
                dedup_row_ids,
                &|_idx, key, row_id| (key, row_id),
            );
        }
        self.lookup_entries_many_sorted_fallback(keys, exact, dedup_row_ids)
    }

    /// Decoded-page fallback. The caller must hold `structure_lock` for reading.
    fn lookup_entries_many_sorted_fallback(
        &self,
        keys: &[IndexKey],
        exact: bool,
        dedup_row_ids: bool,
    ) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        let mut entries = Vec::new();
        let mut seen = dedup_row_ids.then(crate::util::fast_hash::ah_hash_set);
        let mut leaf_keys: Vec<(u64, usize)> = Vec::with_capacity(keys.len());
        for (idx, key) in keys.iter().enumerate() {
            leaf_keys.push((self.find_leaf_for_key_lower_bound(key)?, idx));
        }

        let mut i = 0usize;
        while i < leaf_keys.len() {
            let leaf_id = leaf_keys[i].0;
            let start_i = i;
            while i < leaf_keys.len() && leaf_keys[i].0 == leaf_id {
                i += 1;
            }
            let leaf_page = self.read_page(leaf_id)?;
            let leaf = match leaf_page.as_ref() {
                IndexPage::Leaf(leaf) => leaf,
                _ => {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: expected leaf page",
                    ))
                }
            };
            if let Some(next) = leaf.next_leaf {
                self.prefetch_next_leaves(next);
            }

            for &(_, key_idx) in &leaf_keys[start_i..i] {
                let key = &keys[key_idx];
                let may_span_next_leaf = leaf.next_leaf.is_some()
                    && leaf.entries.last().is_none_or(|entry| {
                        compare_index_entry_to_lookup_key(&entry.key, key, exact)
                            != std::cmp::Ordering::Greater
                    });
                if may_span_next_leaf {
                    let scalar_entries = if exact {
                        self.get_entries(key, dedup_row_ids)?
                    } else {
                        self.prefix_entries(key, dedup_row_ids)?
                    };
                    for (key, row_id) in scalar_entries {
                        push_index_entry_tuple(&mut entries, &mut seen, key, row_id);
                    }
                    continue;
                }

                let start = leaf.entries.partition_point(|entry| {
                    compare_index_entry_to_lookup_key(&entry.key, key, exact)
                        == std::cmp::Ordering::Less
                });
                for entry in &leaf.entries[start..] {
                    match compare_index_entry_to_lookup_key(&entry.key, key, exact) {
                        std::cmp::Ordering::Less => {}
                        std::cmp::Ordering::Equal => {
                            push_index_entry(&mut entries, &mut seen, entry);
                        }
                        std::cmp::Ordering::Greater => break,
                    }
                }
            }
        }
        Ok(entries)
    }

    fn lookup_entries_many_sorted_routed<E, F: Fn(usize, IndexKey, RowId) -> E>(
        &self,
        keys: &[IndexKey],
        exact: bool,
        dedup_row_ids: bool,
        make: &F,
    ) -> Result<Vec<E>, QueryError> {
        let mut leaf_ranges = Vec::new();
        self.route_lookup_keys_to_leaves(
            self.root_page.load(Ordering::Acquire),
            keys,
            0,
            keys.len(),
            &mut leaf_ranges,
        )?;
        if leaf_ranges.is_empty() {
            return Ok(Vec::new());
        }
        merge_adjacent_leaf_key_ranges(&mut leaf_ranges);

        let mut entries: Vec<E> = Vec::new();
        let mut seen = dedup_row_ids.then(crate::util::fast_hash::ah_hash_set);
        for range in leaf_ranges {
            let pinned = self.buffer_pool.pin_page(self.page_key(range.leaf_id))?;
            let result = pinned.with_index_raw_bytes(|buf| {
                lookup_leaf_entries_many_page_bytes(
                    buf,
                    &keys[range.start..range.end],
                    range.start,
                    exact,
                    &mut seen,
                    make,
                )
            })?;
            drop(pinned);
            entries.extend(result.entries);
            if let Some(next_leaf) = result.next_leaf {
                for offset in result.continue_key_offsets {
                    let _page_max_trx_id = self.collect_lookup_entries_from_leaf(
                        self.buffer_pool.pin_page(self.page_key(next_leaf))?,
                        &keys[range.start + offset],
                        range.start + offset,
                        exact,
                        &mut entries,
                        &mut seen,
                        make,
                    )?;
                }
            }
        }
        Ok(entries)
    }

    /// Anti/Semi short-circuit batched probe. For each input key,
    /// reports whether it has *at least one* index entry whose row
    /// passes `accept` (the caller's visibility / residual check).
    /// Returns `Vec<bool>` parallel to `keys`.
    ///
    /// Differs from `lookup_entries_many_sorted_routed` + caller-side
    /// existence check in that the per-key scan stops at the first
    /// `accept` hit. For Q22-shape (16K outer keys × ~21 matches each)
    /// this drops ~190K entry inspections down to ~12K — what would
    /// otherwise be N×M heap visibility checks becomes O(N).
    ///
    /// Preconditions: `keys` is sorted and distinct.
    pub(crate) fn keys_with_visible_match_routed<V, P>(
        &self,
        keys: &[IndexKey],
        exact: bool,
        mut page_all_visible: V,
        mut accept: P,
    ) -> Result<Vec<bool>, QueryError>
    where
        V: FnMut(u64) -> bool,
        P: FnMut(&IndexKey, RowId) -> Result<bool, QueryError>,
    {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let _structure = self.structure_lock.read_recursive();
        self.ensure_healthy()?;
        let mut leaf_ranges = Vec::new();
        self.route_lookup_keys_to_leaves(
            self.root_page.load(Ordering::Acquire),
            keys,
            0,
            keys.len(),
            &mut leaf_ranges,
        )?;
        let mut matched = vec![false; keys.len()];
        if leaf_ranges.is_empty() {
            return Ok(matched);
        }
        merge_adjacent_leaf_key_ranges(&mut leaf_ranges);
        for range in leaf_ranges {
            let pinned = self.buffer_pool.pin_page(self.page_key(range.leaf_id))?;
            let result = pinned.with_index_raw_bytes(|buf| {
                keys_first_visible_match_leaf_page(
                    buf,
                    &keys[range.start..range.end],
                    range.start,
                    exact,
                    &mut matched,
                    &mut page_all_visible,
                    &mut accept,
                )
            })?;
            drop(pinned);
            if let Some(mut leaf_id) = result.next_leaf {
                for offset in result.continue_key_offsets {
                    let absolute_idx = range.start + offset;
                    if matched[absolute_idx] {
                        continue;
                    }
                    // Follow the next-leaf chain for this single key
                    // until we either hit a visible match or run past
                    // its range (`continue_key_offsets` empty signals
                    // a `Greater` or end-of-chain).
                    let mut leaf = leaf_id;
                    loop {
                        let pinned = self.buffer_pool.pin_page(self.page_key(leaf))?;
                        let inner = pinned.with_index_raw_bytes(|buf| {
                            keys_first_visible_match_leaf_page(
                                buf,
                                std::slice::from_ref(&keys[absolute_idx]),
                                absolute_idx,
                                exact,
                                &mut matched,
                                &mut page_all_visible,
                                &mut accept,
                            )
                        })?;
                        drop(pinned);
                        if matched[absolute_idx]
                            || inner.continue_key_offsets.is_empty()
                            || inner.next_leaf.is_none()
                        {
                            break;
                        }
                        leaf = inner.next_leaf.unwrap();
                    }
                    // Avoid an unused-warning on the captured outer
                    // leaf_id — it's only meaningful when there's at
                    // least one offset that still needs work.
                    let _ = leaf_id;
                    leaf_id = leaf;
                }
            }
        }
        Ok(matched)
    }

    /// Keyed batched probe used by the non-covering INLJ path. Returns
    /// `(probe-key index, row id)` pairs, where `probe-key index` is the
    /// position of the matching key in `keys`. The caller holds `keys`
    /// plus a parallel left-row map, so it never needs the entry's full
    /// `IndexKey` — this avoids materialising one heap-allocated
    /// `IndexKey` per matched entry. `keys` must be sorted and distinct.
    pub(crate) fn lookup_entries_many_sorted_keyed(
        &self,
        keys: &[IndexKey],
        exact: bool,
        dedup_row_ids: bool,
    ) -> Result<Vec<(u32, RowId)>, QueryError> {
        Ok(self
            .lookup_entries_many_sorted_keyed_bounded(keys, exact, dedup_row_ids, usize::MAX)?
            .unwrap_or_default())
    }

    /// Bounded counterpart of [`Self::lookup_entries_many_sorted_keyed`].
    /// Returns `None` as soon as the materialized entry count exceeds
    /// `max_entries`. The check is performed after every pinned leaf page, so
    /// a badly stale selectivity estimate can overshoot by at most one leaf
    /// page rather than expanding an optimization into an unbounded
    /// fact-table-sized allocation.
    pub(crate) fn lookup_entries_many_sorted_keyed_bounded(
        &self,
        keys: &[IndexKey],
        exact: bool,
        dedup_row_ids: bool,
        max_entries: usize,
    ) -> Result<Option<Vec<(u32, RowId)>>, QueryError> {
        if keys.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let _structure = self.structure_lock.read_recursive();
        self.ensure_healthy()?;
        let mut leaf_ranges = Vec::new();
        self.route_lookup_keys_to_leaves(
            self.root_page.load(Ordering::Acquire),
            keys,
            0,
            keys.len(),
            &mut leaf_ranges,
        )?;
        if leaf_ranges.is_empty() {
            return Ok(Some(Vec::new()));
        }
        merge_adjacent_leaf_key_ranges(&mut leaf_ranges);

        let mut entries: Vec<(u32, RowId)> = Vec::new();
        let mut seen = dedup_row_ids.then(crate::util::fast_hash::ah_hash_set);
        for range in leaf_ranges {
            let pinned = self.buffer_pool.pin_page(self.page_key(range.leaf_id))?;
            let native = if exact {
                pinned.packed_leaf_lookup_many_keyed(
                    &keys[range.start..range.end],
                    range.start,
                    &mut seen,
                )?
            } else {
                None
            };
            let result = match native {
                Some(result) => result,
                None => pinned.with_index_raw_bytes(|buf| {
                    lookup_leaf_entries_many_keyed_page_bytes(
                        buf,
                        &keys[range.start..range.end],
                        range.start,
                        exact,
                        &mut seen,
                    )
                })?,
            };
            drop(pinned);
            entries.extend(result.entries);
            if entries.len() > max_entries {
                return Ok(None);
            }
            if let Some(next_leaf) = result.next_leaf {
                for offset in result.continue_key_offsets {
                    if !self.collect_lookup_keyed_entries_from_leaf(
                        next_leaf,
                        &keys[range.start + offset],
                        range.start + offset,
                        exact,
                        &mut entries,
                        &mut seen,
                        max_entries,
                    )? {
                        return Ok(None);
                    }
                }
            }
        }
        Ok(Some(entries))
    }

    fn collect_lookup_entries_from_leaf<E, F: Fn(usize, IndexKey, RowId) -> E>(
        &self,
        mut pinned: PinnedFrame,
        key: &IndexKey,
        key_index: usize,
        exact: bool,
        entries: &mut Vec<E>,
        seen: &mut Option<BtreeDedupSet>,
        make: &F,
    ) -> Result<u64, QueryError> {
        let mut page_max_trx_id = 0;
        let mut chain = IndexPageProgress::new(pinned.frame.key.page_id);
        loop {
            let native = if exact {
                pinned.packed_leaf_lookup(key)?
            } else {
                None
            };
            let result = match native {
                Some((native, page_max_trx_id)) => {
                    let entries = native
                        .rows
                        .into_iter()
                        .filter(|row_id| seen.as_mut().is_none_or(|seen| seen.insert(*row_id)))
                        .map(|row_id| make(key_index, key.clone(), row_id))
                        .collect();
                    LeafEntryLookupResult {
                        entries,
                        next_leaf: native.next_leaf,
                        should_continue: native.should_continue,
                        page_max_trx_id,
                    }
                }
                None => pinned.with_index_raw_bytes(|buf| {
                    lookup_leaf_entry_page_bytes(buf, key, key_index, exact, seen, make)
                })?,
            };
            drop(pinned);
            page_max_trx_id = page_max_trx_id.max(result.page_max_trx_id);
            entries.extend(result.entries);
            if !result.should_continue {
                return Ok(page_max_trx_id);
            }
            let Some(next_leaf) = result.next_leaf else {
                return Ok(page_max_trx_id);
            };
            chain.advance(next_leaf)?;
            pinned = self.buffer_pool.pin_page(self.page_key(next_leaf))?;
        }
    }

    fn collect_lookup_keyed_entries_from_leaf(
        &self,
        mut leaf_id: u64,
        key: &IndexKey,
        key_index: usize,
        exact: bool,
        entries: &mut Vec<(u32, RowId)>,
        seen: &mut Option<BtreeDedupSet>,
        max_entries: usize,
    ) -> Result<bool, QueryError> {
        let mut chain = IndexPageProgress::new(leaf_id);
        loop {
            let pinned = self.buffer_pool.pin_page(self.page_key(leaf_id))?;
            let native = if exact {
                pinned.packed_leaf_lookup_many_keyed(std::slice::from_ref(key), key_index, seen)?
            } else {
                None
            };
            let result = match native {
                Some(result) => result,
                None => pinned.with_index_raw_bytes(|buf| {
                    lookup_leaf_entries_many_keyed_page_bytes(
                        buf,
                        std::slice::from_ref(key),
                        key_index,
                        exact,
                        seen,
                    )
                })?,
            };
            drop(pinned);
            entries.extend(result.entries);
            if entries.len() > max_entries {
                return Ok(false);
            }
            if result.continue_key_offsets.is_empty() {
                return Ok(true);
            }
            let Some(next_leaf) = result.next_leaf else {
                return Ok(true);
            };
            chain.advance(next_leaf)?;
            leaf_id = next_leaf;
        }
    }

    fn route_lookup_keys_to_leaves(
        &self,
        page_id: u64,
        keys: &[IndexKey],
        start: usize,
        end: usize,
        out: &mut Vec<LeafKeyRange>,
    ) -> Result<(), QueryError> {
        if start >= end {
            return Ok(());
        }
        // Route the batch while borrowing the raw internal page.  The old
        // implementation decoded every separator and child into owned Vecs
        // for each internal page visited by a batched INLJ.  Reuse the
        // allocation-free scalar router and coalesce adjacent keys that map
        // to the same child before descending.
        let pinned = self.buffer_pool.pin_page(self.page_key(page_id))?;
        if pinned.frame.has_packed_leaf() {
            out.push(LeafKeyRange {
                leaf_id: page_id,
                start,
                end,
            });
            return Ok(());
        }
        let child_ranges = pinned.with_index_raw_bytes(|bytes| {
            route_index_page_key_ranges_bytes(bytes, keys, start, end, min_lookup_row_id())
        })?;
        drop(pinned);

        match child_ranges {
            None => {
                out.push(LeafKeyRange {
                    leaf_id: page_id,
                    start,
                    end,
                });
                Ok(())
            }
            Some(ranges) => {
                for range in ranges {
                    self.route_lookup_keys_to_leaves(
                        range.leaf_id,
                        keys,
                        range.start,
                        range.end,
                        out,
                    )?;
                }
                Ok(())
            }
        }
    }

    fn range(
        &self,
        low: Option<&IndexBound>,
        high: Option<&IndexBound>,
        dedup_row_ids: bool,
    ) -> Result<Vec<RowId>, QueryError> {
        let _structure = self.structure_lock.read_recursive();
        self.ensure_healthy()?;
        let mut rows = Vec::new();
        let mut seen = dedup_row_ids.then(crate::util::fast_hash::ah_hash_set);
        let mut leaf_id = match low {
            Some(bound) => self.find_leaf_for_key_lower_bound(&bound.key)?,
            None => self.leftmost_leaf()?,
        };
        let mut entry_idx = 0usize;
        let mut positioned = low.is_none();
        let mut last_row_id = None;
        loop {
            let pinned = self
                .buffer_pool
                .pin_page_with_intent(self.page_key(leaf_id), AccessIntent::Scan)?;
            let pulled = pinned.with_index_raw_bytes(|buf| {
                range_leaf_row_ids_page_bytes(
                    buf,
                    low,
                    high,
                    entry_idx,
                    positioned,
                    last_row_id,
                    true,
                    false,
                    usize::MAX,
                )
            })?;
            drop(pinned);
            entry_idx = pulled.next_entry_idx;
            positioned = pulled.positioned;
            last_row_id = pulled.last_row_id;
            for row_id in pulled.rows {
                push_row_id(&mut rows, &mut seen, row_id);
            }
            if pulled.terminal {
                return Ok(rows);
            }
            let Some(next_leaf) = pulled.next_leaf else {
                return Ok(rows);
            };
            if !pulled.exhausted_leaf {
                continue;
            }
            self.prefetch_next_leaves(next_leaf);
            leaf_id = next_leaf;
            entry_idx = 0;
        }
    }

    #[cfg(test)]
    fn range_limited(
        &self,
        low: Option<&IndexBound>,
        high: Option<&IndexBound>,
        offset: usize,
        limit: Option<usize>,
    ) -> Result<Vec<RowId>, QueryError> {
        let _structure = self.structure_lock.read_recursive();
        self.ensure_healthy()?;
        let max_rows = limit.unwrap_or(usize::MAX);
        if max_rows == 0 {
            return Ok(Vec::new());
        }
        let mut rows = Vec::new();
        let mut skipped = 0usize;
        let mut leaf_id = match low {
            Some(bound) => self.find_leaf_for_key_lower_bound(&bound.key)?,
            None => self.leftmost_leaf()?,
        };
        loop {
            let leaf_page = self.read_page(leaf_id)?;
            let leaf = match leaf_page.as_ref() {
                IndexPage::Leaf(leaf) => leaf,
                _ => {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: expected leaf page",
                    ))
                }
            };
            if let Some(next) = leaf.next_leaf {
                self.prefetch_next_leaves(next);
            }
            for entry in &leaf.entries {
                if index_key_past_high_bound(&entry.key, high) {
                    return Ok(rows);
                }
                if !index_key_in_bounds(&entry.key, low, high) {
                    continue;
                }
                if skipped < offset {
                    skipped += 1;
                    continue;
                }
                if rows.last().is_some_and(|last| last == &entry.row_id) {
                    continue;
                }
                rows.push(entry.row_id);
                if rows.len() >= max_rows {
                    return Ok(rows);
                }
            }
            let Some(next_leaf) = leaf.next_leaf else {
                return Ok(rows);
            };
            leaf_id = next_leaf;
        }
    }

    fn range_entries_chunk(
        &self,
        low: Option<&IndexBound>,
        high: Option<&IndexBound>,
        resume_after: Option<&(IndexKey, RowId)>,
        chunk_size: usize,
    ) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        let _structure = self.structure_lock.read_recursive();
        self.ensure_healthy()?;
        if chunk_size == 0 {
            return Ok(Vec::new());
        }
        let mut entries = Vec::with_capacity(chunk_size);
        let start_key = resume_after
            .map(|(key, _)| key)
            .or_else(|| low.map(|bound| &bound.key));
        let mut leaf_id = match start_key {
            Some(key) => self.find_leaf_for_key_lower_bound(key)?,
            None => self.leftmost_leaf()?,
        };
        loop {
            let leaf_page = self.read_page(leaf_id)?;
            let leaf = match leaf_page.as_ref() {
                IndexPage::Leaf(leaf) => leaf,
                _ => {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: expected leaf page",
                    ))
                }
            };
            if let Some(next) = leaf.next_leaf {
                self.prefetch_next_leaves(next);
            }
            // A key-only lower-bound descent can land before the resume
            // leaf, and equal keys can span several leaves. Keep seeking
            // until an entry passes the resume/low bound; only then can
            // subsequent leaves skip these comparisons.
            let start = if entries.is_empty() {
                match (resume_after, low) {
                    (Some((resume_key, resume_row_id)), _) => {
                        leaf.entries.partition_point(|entry| {
                            entry.key < *resume_key
                                || (entry.key == *resume_key && entry.row_id <= *resume_row_id)
                        })
                    }
                    (None, Some(bound)) => leaf
                        .entries
                        .partition_point(|entry| index_key_before_low_bound(&entry.key, bound)),
                    (None, None) => 0,
                }
            } else {
                0
            };
            let slice = &leaf.entries[start..];
            // Whole-leaf accept: when the last (largest) entry is within
            // the high bound, no per-entry key comparison is needed.
            let whole_leaf_in_bounds = slice
                .last()
                .is_some_and(|last| !index_key_past_high_bound(&last.key, high));
            for entry in slice {
                if !whole_leaf_in_bounds && index_key_past_high_bound(&entry.key, high) {
                    return Ok(entries);
                }
                if entries
                    .last()
                    .is_some_and(|(_, row_id)| row_id == &entry.row_id)
                {
                    continue;
                }
                entries.push((entry.key.clone(), entry.row_id));
                if entries.len() >= chunk_size {
                    return Ok(entries);
                }
            }
            let Some(next_leaf) = leaf.next_leaf else {
                return Ok(entries);
            };
            leaf_id = next_leaf;
        }
    }

    /// Collect the leaf page ids covering `[low, high]`, in key order,
    /// by walking the internal levels only: a frontier of same-level
    /// internal nodes is narrowed per level (the leftmost node prunes
    /// children whose subtrees sort entirely before the low bound, the
    /// rightmost prunes those entirely past the high bound) until the
    /// frontier is the leaf level itself. Reads O(internal pages over
    /// the range) instead of the previous leaf-chain walk that read
    /// and decoded every leaf — pages the parallel range-scan workers
    /// were about to read a second time.
    ///
    /// Boundary leaves may still be partially (or, in gap cases,
    /// wholly) out of range; consumers bisect within each leaf, so the
    /// guarantee is only "no leaf with in-range entries is missing and
    /// at most the two boundary leaves are extra".
    fn range_leaf_ids(
        &self,
        low: Option<&IndexBound>,
        high: Option<&IndexBound>,
    ) -> Result<IndexLeafRange, QueryError> {
        let structure = self.structure_lock.read_arc_recursive();
        let ids = self.range_leaf_ids_locked(low, high)?;
        Ok(IndexLeafRange {
            ids,
            lease: Some(Arc::new(IndexRangeLease {
                _structure: structure,
                _handle: None,
            })),
        })
    }

    /// Caller holds either side of `structure_lock` for the complete walk.
    fn range_leaf_ids_locked(
        &self,
        low: Option<&IndexBound>,
        high: Option<&IndexBound>,
    ) -> Result<Vec<u64>, QueryError> {
        let mut frontier: Vec<u64> = vec![self.root_page.load(Ordering::Acquire)];
        loop {
            let Some(&first_id) = frontier.first() else {
                return Ok(Vec::new());
            };
            if matches!(self.read_page(first_id)?.as_ref(), IndexPage::Leaf(_)) {
                return Ok(frontier);
            }
            let mut next = Vec::with_capacity(frontier.len() * 8);
            let last_idx = frontier.len() - 1;
            for (i, &page_id) in frontier.iter().enumerate() {
                let page = self.read_page(page_id)?;
                let IndexPage::Internal(internal) = page.as_ref() else {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: mixed levels in frontier walk",
                    ));
                };
                // keys[j] is the smallest key of children[j + 1]'s
                // subtree, so:
                //  - children[..j+1] sort entirely before keys[j+1];
                //    a separator that is before the low bound proves
                //    every earlier child is too (before-low is
                //    downward monotone).
                //  - a separator past the high bound proves every
                //    later child is too (past-high is upward
                //    monotone).
                // Only the frontier's edge nodes can straddle a
                // bound — interior nodes were kept precisely because
                // their whole range overlaps.
                let mut start = 0usize;
                if i == 0 {
                    if let Some(low) = low {
                        start = internal
                            .keys
                            .partition_point(|k| index_key_before_low_bound(&k.key, low));
                    }
                }
                let mut end = internal.children.len();
                if i == last_idx && high.is_some() {
                    let keep = internal
                        .keys
                        .partition_point(|k| !index_key_past_high_bound(&k.key, high))
                        + 1;
                    end = end.min(keep);
                }
                if start < end {
                    next.extend_from_slice(&internal.children[start..end]);
                }
            }
            frontier = next;
        }
    }

    #[cfg(test)]
    fn range_entries_for_leaf_ids(
        &self,
        leaf_ids: &[u64],
        low: Option<&IndexBound>,
        high: Option<&IndexBound>,
    ) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        let _structure = self.structure_lock.read_recursive();
        self.ensure_healthy()?;
        let mut entries: Vec<(IndexKey, RowId)> = Vec::new();
        self.fill_entries_for_leaf_ids(leaf_ids, low, high, &mut entries)?;
        Ok(entries)
    }

    /// Key-free variant for non-covering parallel range-scan workers:
    /// same walk as `range_entries_for_leaf_ids` but no `IndexKey` is
    /// cloned — the worker only feeds RowIds to the heap lookup.
    fn range_row_ids_for_leaf_ids(
        &self,
        leaf_ids: &[u64],
        low: Option<&IndexBound>,
        high: Option<&IndexBound>,
    ) -> Result<Vec<RowId>, QueryError> {
        let _structure = self.structure_lock.read_recursive();
        self.ensure_healthy()?;
        let mut row_ids = Vec::new();
        let mut last_row_id = None;
        for &leaf_id in leaf_ids {
            let pinned = self.buffer_pool.pin_page(self.page_key(leaf_id))?;
            let (next, terminal) = pinned.with_index_raw_bytes(|bytes| {
                let header = parse_leaf_payload_header(bytes)?;
                let (start, end, terminal) = raw_leaf_in_bounds_span(&header, low, high)?;
                row_ids.reserve(end.saturating_sub(start));
                for slot in start..end {
                    let row_id = decode_raw_leaf_row_id_at(&header, slot)?;
                    if last_row_id != Some(row_id) {
                        row_ids.push(row_id);
                        last_row_id = Some(row_id);
                    }
                }
                Ok((header.next_leaf, terminal))
            })?;
            if terminal {
                break;
            }
            if let Some(next) = next {
                self.prefetch_next_leaves(next);
            }
        }
        Ok(row_ids)
    }

    #[cfg(test)]
    fn fill_entries_for_leaf_ids<S: RangeChunkSink>(
        &self,
        leaf_ids: &[u64],
        low: Option<&IndexBound>,
        high: Option<&IndexBound>,
        sink: &mut S,
    ) -> Result<(), QueryError> {
        let mut last_row_id: Option<RowId> = None;
        for &leaf_id in leaf_ids {
            let leaf_page = self.read_page(leaf_id)?;
            let leaf = match leaf_page.as_ref() {
                IndexPage::Leaf(leaf) => leaf,
                _ => {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: expected leaf page",
                    ))
                }
            };
            if let Some(next) = leaf.next_leaf {
                self.prefetch_next_leaves(next);
            }
            // Per-leaf in-bounds span: interior leaves resolve with
            // two boundary probes and zero per-entry key comparisons;
            // boundary leaves bisect to the cut slots.
            let (start, end, terminal) = leaf_in_bounds_span(&leaf.entries, low, high);
            for entry in &leaf.entries[start..end] {
                if last_row_id == Some(entry.row_id) {
                    continue;
                }
                last_row_id = Some(entry.row_id);
                sink.push(entry);
            }
            if terminal {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Stream a single lossless-integer covering index directly from raw
    /// leaf bytes, avoiding full-page materialization and one `IndexKey`
    /// allocation per entry for consumers that need only the
    /// scalar key and RowId.
    ///
    /// `leaf_ids` is already in key order. Read-ahead therefore queues each
    /// known leaf at most once instead of repeatedly rediscovering the same
    /// sibling window from decoded pages.
    fn visit_lossless_i64_leaf_entry_chunks<F>(
        &self,
        leaf_ids: &[u64],
        seed_leaf_id: Option<u64>,
        low: Option<&IndexBound>,
        high: Option<&IndexBound>,
        unique: bool,
        key_position: usize,
        mut visitor: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(&[(Option<i64>, RowId)], u64) -> Result<(), QueryError>,
    {
        const ENTRY_CHUNK: usize = 512;
        let _structure = self.structure_lock.read_recursive();
        self.ensure_healthy()?;
        let mut last_row_id = None;

        if !unique {
            if let Some(seed_leaf_id) = seed_leaf_id {
                let pinned = self
                    .buffer_pool
                    .pin_page_with_intent(self.page_key(seed_leaf_id), AccessIntent::Scan)?;
                last_row_id = pinned.with_index_raw_bytes(|buf| {
                    let hdr = parse_leaf_payload_header(buf)?;
                    let (start, end, _) = raw_leaf_in_bounds_span(&hdr, low, high)?;
                    if start >= end {
                        return Ok(None);
                    }
                    let mut cursor = hdr.read_slot_offset(end - 1);
                    let _ = hdr.decode_lossless_i64_part_at(&mut cursor, key_position)?;
                    decode_row_id_v7_from(hdr.payload, &mut cursor, hdr.page_table_id).map(Some)
                })?;
            }
        }

        let prefetch_window = self.buffer_pool.effective_prefetch_window();
        for &leaf_id in leaf_ids.iter().take(prefetch_window) {
            self.buffer_pool.prefetch_page(self.page_key(leaf_id));
        }

        for (leaf_idx, &leaf_id) in leaf_ids.iter().enumerate() {
            if let Some(&prefetch_id) = leaf_ids.get(leaf_idx.saturating_add(prefetch_window)) {
                self.buffer_pool.prefetch_page(self.page_key(prefetch_id));
            }
            let pinned = self
                .buffer_pool
                .pin_page_with_intent(self.page_key(leaf_id), AccessIntent::Scan)?;
            let terminal = pinned.with_index_raw_bytes(|buf| {
                let hdr = parse_leaf_payload_header(buf)?;
                let (start, end, terminal) = raw_leaf_in_bounds_span(&hdr, low, high)?;
                let mut entries: Vec<(Option<i64>, RowId)> =
                    Vec::with_capacity(ENTRY_CHUNK.min(end.saturating_sub(start)));
                for entry_idx in start..end {
                    let mut cursor = hdr.read_slot_offset(entry_idx);
                    let value = hdr.decode_lossless_i64_part_at(&mut cursor, key_position)?;
                    let row_id =
                        decode_row_id_v7_from(hdr.payload, &mut cursor, hdr.page_table_id)?;
                    if !unique && last_row_id == Some(row_id) {
                        continue;
                    }
                    last_row_id = Some(row_id);
                    entries.push((value, row_id));
                    if entries.len() >= ENTRY_CHUNK {
                        visitor(&entries, hdr.page_max_trx_id)?;
                        entries.clear();
                    }
                }
                if !entries.is_empty() {
                    visitor(&entries, hdr.page_max_trx_id)?;
                }
                Ok(terminal)
            })?;
            if terminal {
                return Ok(());
            }
        }
        Ok(())
    }

    fn find_leaf_for_reverse_range_start(
        &self,
        low: Option<&IndexBound>,
        high: Option<&IndexBound>,
        resume_before: Option<&(IndexKey, RowId)>,
    ) -> Result<u64, QueryError> {
        let _structure = self.structure_lock.read_recursive();
        self.ensure_healthy()?;
        if let Some((key, row_id)) = resume_before {
            return self.find_leaf_for_entry(
                &IndexEntry {
                    key: key.clone(),
                    row_id: *row_id,
                },
                &mut Vec::new(),
            );
        }

        let Some(high) = high else {
            return self.rightmost_leaf();
        };

        let mut leaf_id = self.find_leaf_for_entry(
            &IndexEntry {
                key: high.key.clone(),
                row_id: max_row_id(),
            },
            &mut Vec::new(),
        )?;

        loop {
            let pinned = self.buffer_pool.pin_page(self.page_key(leaf_id))?;
            let next = pinned
                .with_index_raw_bytes(|bytes| Ok(parse_leaf_payload_header(bytes)?.next_leaf))?;
            drop(pinned);
            let Some(next_leaf_id) = next else {
                return Ok(leaf_id);
            };
            let pinned = self.buffer_pool.pin_page(self.page_key(next_leaf_id))?;
            let overlaps = pinned.with_index_raw_bytes(|bytes| {
                let header = parse_leaf_payload_header(bytes)?;
                let (start, end, _) = raw_leaf_in_bounds_span(&header, low, Some(high))?;
                Ok(start < end)
            })?;
            if !overlaps {
                return Ok(leaf_id);
            }
            leaf_id = next_leaf_id;
        }
    }

    /// Compare only the final encoded slot; uniqueness probes need neither
    /// the other leaf entries nor the duplicate-key RowId collection.
    fn last_key_is_before(&self, candidate: &IndexKey) -> Result<bool, QueryError> {
        let _structure = self.structure_lock.read_recursive();
        self.ensure_healthy()?;
        let leaf_id = self.rightmost_leaf()?;
        let pinned = self.buffer_pool.pin_page(self.page_key(leaf_id))?;
        pinned.with_index_raw_bytes(|bytes| {
            let header = parse_leaf_payload_header(bytes)?;
            if header.count == 0 {
                return Ok(true);
            }
            let slot = header.count - 1;
            let (ordering, _) = match header.fast_scalar_target(candidate) {
                Some(target) => header.cmp_scalar_at(slot, target)?,
                None => header.cmp_key_at(slot, candidate)?,
            };
            Ok(ordering == std::cmp::Ordering::Less)
        })
    }

    fn last_entry(&self) -> Result<Option<(IndexKey, Vec<RowId>)>, QueryError> {
        let _structure = self.structure_lock.read_recursive();
        self.ensure_healthy()?;
        let leaf_id = self.rightmost_leaf()?;
        let leaf_page = self.read_page(leaf_id)?;
        let leaf = match leaf_page.as_ref() {
            IndexPage::Leaf(leaf) => leaf,
            _ => {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt index: expected leaf page",
                ))
            }
        };
        let Some(last) = leaf.entries.last() else {
            return Ok(None);
        };
        let last_key = last.key.clone();
        let mut rows = Vec::new();
        for entry in leaf.entries.iter().rev() {
            if entry.key != last_key {
                break;
            }
            rows.push(entry.row_id);
        }
        rows.sort();
        Ok(Some((last_key, rows)))
    }

    fn append_index_batch_wal(
        &self,
        txn_id: u64,
        kind: WalIndexOpKind,
        entries: &[IndexEntry],
    ) -> Result<Option<u64>, QueryError> {
        if self.durability == DurabilityMode::Memory {
            return Ok(None);
        }
        let path = self.relative_path();
        let fixed_payload_bytes = 4usize
            .checked_add(path.len())
            .and_then(|len| len.checked_add(4))
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                    "WAL index batch is too large",
                )
            })?;
        let mut chunk_payload_bytes = fixed_payload_bytes;
        let mut ops = Vec::new();
        let mut last_lsn = None;

        for entry in entries {
            let mut encoded_key = Vec::with_capacity(encoded_index_key_len(&entry.key)?);
            encode_index_key_into(&entry.key, &mut encoded_key)?;
            // kind:u8 + key:(len:u32 + bytes) + row_id:(page:u32 + slot:u16)
            let op_payload_bytes = 1usize
                .checked_add(4)
                .and_then(|len| len.checked_add(encoded_key.len()))
                .and_then(|len| len.checked_add(6))
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                        "WAL index operation is too large",
                    )
                })?;
            if !ops.is_empty()
                && chunk_payload_bytes.saturating_add(op_payload_bytes)
                    > INDEX_BATCH_WAL_TARGET_BYTES
            {
                let pre = crate::wal::LogManager::pre_encode_record(
                    txn_id,
                    WalPayload::IndexBatch {
                        path: path.clone(),
                        ops: std::mem::take(&mut ops),
                    },
                )?;
                last_lsn = Some(crate::wal::LogManager::append_pre_encoded_to(
                    &self.log, pre,
                )?);
                chunk_payload_bytes = fixed_payload_bytes;
            }
            chunk_payload_bytes = chunk_payload_bytes
                .checked_add(op_payload_bytes)
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                        "WAL index batch is too large",
                    )
                })?;
            ops.push(WalIndexOp {
                kind,
                key: encoded_key,
                row_id: WalRowId {
                    page_id: entry.row_id.page_id,
                    slot_id: entry.row_id.slot_id,
                },
            });
        }
        if !ops.is_empty() {
            let pre = crate::wal::LogManager::pre_encode_record(
                txn_id,
                WalPayload::IndexBatch { path, ops },
            )?;
            last_lsn = Some(crate::wal::LogManager::append_pre_encoded_to(
                &self.log, pre,
            )?);
        }
        Ok(last_lsn)
    }

    fn append_mixed_index_batch_wal(
        &self,
        txn_id: u64,
        entries: &[IndexBatchEntry],
    ) -> Result<Option<u64>, QueryError> {
        let Some(payload) = self.preencode_mixed_index_batch_wal(txn_id, entries)? else {
            return Ok(None);
        };
        let pre = crate::wal::LogManager::pre_encode_record(txn_id, payload)?;
        let lsn = crate::wal::LogManager::append_pre_encoded_to(&self.log, pre)?;
        Ok(Some(lsn))
    }

    pub(super) fn preencode_mixed_index_batch_wal(
        &self,
        txn_id: u64,
        entries: &[IndexBatchEntry],
    ) -> Result<Option<WalPayload>, QueryError> {
        if self.spatial.is_some() {
            return Ok(None);
        }
        if self.durability == DurabilityMode::Memory {
            return Ok(None);
        }
        let ops = entries
            .iter()
            .map(|entry| {
                let mut encoded_key = Vec::with_capacity(encoded_index_key_len(&entry.key)?);
                encode_index_key_into(&entry.key, &mut encoded_key)?;
                Ok(WalIndexOp {
                    kind: entry.kind,
                    key: encoded_key,
                    row_id: WalRowId {
                        page_id: entry.row_id.page_id,
                        slot_id: entry.row_id.slot_id,
                    },
                })
            })
            .collect::<Result<Vec<_>, QueryError>>()?;
        let _ = txn_id;
        Ok(Some(WalPayload::IndexBatch {
            path: self.relative_path(),
            ops,
        }))
    }

    fn apply_single_index_op_logged(
        &self,
        txn_id: u64,
        kind: WalIndexOpKind,
        entry: IndexEntry,
        apply: impl FnOnce(&Self, IndexEntry) -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        self.ensure_healthy()?;
        if let Some(log) = &self.online_build_log {
            log.record(std::iter::once(IndexBatchEntry {
                kind,
                key: entry.key,
                row_id: entry.row_id,
            }));
            return Ok(());
        }
        if self.durability == DurabilityMode::Memory {
            return apply(self, entry);
        }
        // Publish redo before any page can become dirty/evictable. A future
        // peek-LSN is not a reservation: another writer can make it durable.
        let wal_op = Self::encode_wal_index_op(kind, &entry)?;
        let pre = crate::wal::LogManager::pre_encode_record(
            txn_id,
            WalPayload::IndexBatch {
                path: self.relative_path(),
                ops: vec![wal_op],
            },
        )?;
        let lsn = crate::wal::LogManager::append_pre_encoded_to(&self.log, pre)?;
        let previous_lsn = btree_logical_wal::current();
        btree_logical_wal::set(Some(lsn));
        let result = apply(self, entry);
        btree_logical_wal::set(previous_lsn);
        result
    }

    fn encode_wal_index_op(
        kind: WalIndexOpKind,
        entry: &IndexEntry,
    ) -> Result<WalIndexOp, QueryError> {
        let mut encoded_key = Vec::with_capacity(encoded_index_key_len(&entry.key)?);
        encode_index_key_into(&entry.key, &mut encoded_key)?;
        Ok(WalIndexOp {
            kind,
            key: encoded_key,
            row_id: WalRowId {
                page_id: entry.row_id.page_id,
                slot_id: entry.row_id.slot_id,
            },
        })
    }

    fn entries(&self) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        if let Some(spatial) = &self.spatial {
            return spatial.entries();
        }
        let mut entries = Vec::new();
        let mut leaf_id = self.leftmost_leaf()?;
        loop {
            let leaf_page = self.read_page(leaf_id)?;
            let leaf = match leaf_page.as_ref() {
                IndexPage::Leaf(leaf) => leaf,
                _ => {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: expected leaf page",
                    ))
                }
            };
            entries.extend(
                leaf.entries
                    .iter()
                    .map(|entry| (entry.key.clone(), entry.row_id)),
            );
            let Some(next_leaf) = leaf.next_leaf else {
                return Ok(entries);
            };
            leaf_id = next_leaf;
        }
    }

    fn flush(&self) -> Result<(), QueryError> {
        if let Some(spatial) = &self.spatial {
            return spatial.flush();
        }
        #[cfg(test)]
        INDEX_FLUSH_TEST_HOOK.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() {
                hook();
            }
        });
        self.ensure_healthy()?;
        if self.online_build_log.is_some() {
            return Ok(());
        }
        if self.durability == DurabilityMode::Memory {
            self.dirty_meta.store(false, Ordering::Release);
            self.dirty.store(false, Ordering::Release);
            return Ok(());
        }
        if !self.dirty.load(Ordering::Acquire) && self.path.exists() {
            return Ok(());
        }
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(io_err)?;
        }
        let mut file_guard = self.file.lock();
        if file_guard.is_none() {
            *file_guard = Some(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(&self.path)
                    .map_err(io_err)?,
            );
        }
        let next_page_id = self.meta_state.lock().meta.next_page_id;
        self.buffer_pool.flush_file_pages(&self.file_key)?;
        let file = file_guard
            .as_mut()
            .expect("index file should be open before flushing");
        file.set_len(next_page_id * PAGE_SIZE as u64)
            .map_err(io_err)?;
        file.flush().map_err(io_err)?;
        self.buffer_pool.sync_cached_and_direct_file(
            &self.file_key,
            file,
            self.durability == DurabilityMode::Strict,
        )?;
        self.dirty_meta.store(false, Ordering::Release);
        self.dirty.store(false, Ordering::Release);
        Ok(())
    }

    fn discard_without_flush(&mut self) {
        self.spatial = None;
        self.dirty_meta.store(false, Ordering::Release);
        self.dirty.store(false, Ordering::Release);
        let _ = self.buffer_pool.discard_file(&self.file_key);
        self.durability = DurabilityMode::Memory;
    }

    /// Keep shared buffer-pool state untouched when a speculative cache-miss
    /// opener loses to another handle for the same physical index.
    fn retain_buffered_pages_on_drop(&mut self) {
        self.flush_on_drop = false;
    }

    fn mark_dirty(&self) -> Result<(), QueryError> {
        self.dirty.store(true, Ordering::Release);
        Ok(())
    }

    fn pin_tree_page(&self, page_id: u64) -> Result<PinnedFrame, QueryError> {
        let frame = self.pin_tree_frame(page_id)?;
        Ok(PinnedFrame {
            pool: Arc::clone(&self.buffer_pool),
            frame,
            intent: AccessIntent::Normal,
        })
    }

    fn pin_tree_page_borrowed(&self, page_id: u64) -> Result<PinnedFrame<&BufferPool>, QueryError> {
        let frame = self.pin_tree_frame(page_id)?;
        Ok(PinnedFrame {
            pool: self.buffer_pool.as_ref(),
            frame,
            intent: AccessIntent::Normal,
        })
    }

    fn pin_tree_frame(&self, page_id: u64) -> Result<Arc<Frame>, QueryError> {
        if page_id == INDEX_META_PAGE {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "index meta page is not a tree page",
            ));
        }
        if page_id == self.root_page.load(Ordering::Acquire) {
            self.buffer_pool
                .pin_frame_with_hint(self.page_key(page_id), &self.root_frame_hint)
        } else {
            self.buffer_pool.pin_frame_with_candidate(
                self.page_key(page_id),
                AccessIntent::Normal,
                None,
            )
        }
    }

    /// Route one tree level directly through the raw separator slot array.
    fn route_page_for_key(
        &self,
        page_id: u64,
        key: &IndexKey,
        row_id: RowId,
    ) -> Result<RawIndexPageRoute, QueryError> {
        let pinned = self.pin_tree_page(page_id)?;
        if pinned.frame.has_packed_leaf() {
            return Ok(RawIndexPageRoute::Leaf);
        }
        pinned.with_index_raw_bytes(|bytes| route_index_page_bytes(bytes, key, row_id))
    }

    /// Descend to the leaf that would hold the smallest entry with
    /// `key`. Equivalent to `find_leaf_for_entry` probed with
    /// `(key, RowId::zero)`, but borrows the key (the old form cloned
    /// the `IndexKey` and allocated a throwaway `path` Vec per point
    /// lookup) and doesn't record the descent path.
    fn find_leaf_for_key_lower_bound(&self, key: &IndexKey) -> Result<u64, QueryError> {
        Ok(self
            .find_leaf_for_key_lower_bound_pinned(key)?
            .frame
            .key
            .page_id)
    }

    fn pin_lookup_leaf(&self, key: &IndexKey) -> Result<PinnedFrame, QueryError> {
        let pinned = self.find_leaf_for_key_lower_bound_pinned(key)?;
        // Routing and consuming the leaf remain two logical accesses for
        // CLOCK. Reusing the pin must not lower leaf retention under a cold
        // working set merely because the second buffer-map lookup is gone.
        pinned.frame.touch(AccessIntent::Normal);
        Ok(pinned)
    }

    /// Transfer the terminal pin to the consumer instead of releasing and
    /// reacquiring the same leaf. Only one tree page is pinned at a time, so
    /// a cold descent cannot retain ancestors needed by buffer eviction.
    /// Callers retain the structural read lock while consuming the leaf;
    /// the pin protects residency, and each raw visitor takes the page latch.
    fn find_leaf_for_key_lower_bound_pinned(
        &self,
        key: &IndexKey,
    ) -> Result<PinnedFrame, QueryError> {
        self.find_leaf_for_key_lower_bound_with(key, |page| self.pin_tree_page(page))
    }

    fn pin_lookup_leaf_borrowed(
        &self,
        key: &IndexKey,
    ) -> Result<PinnedFrame<&BufferPool>, QueryError> {
        let pinned =
            self.find_leaf_for_key_lower_bound_with(key, |page| self.pin_tree_page_borrowed(page))?;
        pinned.frame.touch(AccessIntent::Normal);
        Ok(pinned)
    }

    fn find_leaf_for_key_lower_bound_with<P, F>(
        &self,
        key: &IndexKey,
        mut pin: F,
    ) -> Result<PinnedFrame<P>, QueryError>
    where
        P: std::ops::Deref<Target = BufferPool>,
        F: FnMut(u64) -> Result<PinnedFrame<P>, QueryError>,
    {
        self.ensure_healthy()?;
        let zero = RowId {
            page_id: 0,
            slot_id: 0,
        };
        let mut page_id = self.root_page.load(Ordering::Acquire);
        let mut progress = IndexPageProgress::new(page_id);
        loop {
            let pinned = pin(page_id)?;
            if pinned.frame.has_packed_leaf() {
                return Ok(pinned);
            }
            let route =
                pinned.with_index_raw_bytes(|bytes| route_index_page_bytes(bytes, key, zero))?;
            match route {
                RawIndexPageRoute::Leaf => return Ok(pinned),
                RawIndexPageRoute::Child { page_id: child, .. } => {
                    progress.advance(child)?;
                    page_id = child;
                }
            }
        }
    }

    #[allow(dead_code)]
    #[allow(dead_code)]
    #[allow(dead_code)]
    fn find_leaf_for_entry(
        &self,
        entry: &IndexEntry,
        path: &mut Vec<(u64, usize)>,
    ) -> Result<u64, QueryError> {
        self.ensure_healthy()?;
        let mut page_id = self.root_page.load(Ordering::Acquire);
        let mut progress = IndexPageProgress::new(page_id);
        loop {
            match self.route_page_for_key(page_id, &entry.key, entry.row_id)? {
                RawIndexPageRoute::Leaf => return Ok(page_id),
                RawIndexPageRoute::Child {
                    page_id: child,
                    child_idx,
                } => {
                    path.push((page_id, child_idx));
                    progress.advance(child)?;
                    page_id = child;
                }
            }
        }
    }

    /// Leaf that owns `(key, row_id)` in the current topology.
    pub(crate) fn leaf_for_entry(&self, key: &IndexKey, row_id: RowId) -> Result<u64, QueryError> {
        let entry = IndexEntry {
            key: key.clone(),
            row_id,
        };
        self.find_leaf_for_entry(&entry, &mut Vec::new())
    }

    /// Root-to-leaf lookup that also reports whether `candidate` is provably
    /// routed to the same terminal leaf. Used by the two-op UPDATE fast path,
    /// which does not need an ancestor path. A candidate between the leaf's
    /// first and last entries is necessarily in the same routing interval; a
    /// candidate above the last entry also stays when this is the rightmost
    /// leaf. Ambiguous boundary cases return false and use the ordinary second
    /// traversal, so this hint can affect performance but never correctness.
    fn find_leaf_for_entry_with_candidate(
        &self,
        entry: &IndexEntry,
        candidate: &IndexEntry,
    ) -> Result<(PinnedFrame, bool), QueryError> {
        self.ensure_healthy()?;
        let mut page_id = self.root_page.load(Ordering::Acquire);
        let mut progress = IndexPageProgress::new(page_id);
        loop {
            let pinned = self.pin_tree_page(page_id)?;
            if pinned.frame.has_packed_leaf() {
                let stays = std::ptr::eq(entry, candidate)
                    || pinned.packed_leaf_candidate_stays(candidate)?;
                return Ok((pinned, stays));
            }
            let (route, candidate_stays) = pinned.with_index_raw_bytes(|bytes| {
                let route = route_index_page_bytes(bytes, &entry.key, entry.row_id)?;
                let candidate_stays = if route == RawIndexPageRoute::Leaf {
                    // INSERT asks only for the routed pin and passes the very
                    // same entry as candidate. Its route is already proven;
                    // decoding both leaf boundaries again adds no information.
                    std::ptr::eq(entry, candidate) || raw_leaf_candidate_stays(bytes, candidate)?
                } else {
                    false
                };
                Ok((route, candidate_stays))
            })?;
            match route {
                RawIndexPageRoute::Leaf => return Ok((pinned, candidate_stays)),
                RawIndexPageRoute::Child { page_id: child, .. } => {
                    drop(pinned);
                    progress.advance(child)?;
                    page_id = child;
                }
            }
        }
    }

    /// Route one sorted interval while holding structure_lock.read(). A parent
    /// proof survives between leaf runs only while its pinned image and epoch
    /// are unchanged. Splits, separator changes and frame replacement force a
    /// fresh descent; ordinary leaf writes do not invalidate their parents.
    fn find_leaf_for_insert_run<'a>(
        &self,
        entries: &[IndexEntry],
        pos: usize,
        mut end: usize,
        path: &'a mut smallvec::SmallVec<[InsertRouteFrame; 4]>,
    ) -> Result<(&'a PinnedFrame, usize), QueryError> {
        self.ensure_healthy()?;
        let mut page_id = self.root_page.load(Ordering::Acquire);
        let mut progress = IndexPageProgress::new(page_id);
        let mut level = 0;
        loop {
            let reusable = path.get(level).is_some_and(|cached| {
                cached.pinned.frame.key.page_id == page_id
                    && matches!(
                        FrameIoState::from_u8(cached.pinned.frame.state.load(Ordering::Acquire)),
                        FrameIoState::Ready | FrameIoState::Flushing
                    )
            });
            if !reusable {
                path.truncate(level);
                let Some(pinned) = self
                    .buffer_pool
                    .try_pin_resident_page(&self.page_key(page_id))?
                else {
                    // Never wait for cold admission with ancestor pins held.
                    // Point navigation proves only the first entry's route;
                    // do not reuse an ancestor's wider interval as a leaf fence.
                    path.clear();
                    let pinned = self
                        .find_leaf_for_entry_with_candidate(&entries[pos], &entries[pos])?
                        .0;
                    path.push(InsertRouteFrame {
                        pinned,
                        epoch: 0,
                        start: pos,
                        end: pos,
                        child: None,
                        search_end: pos,
                        next_slot: 0,
                    });
                    return Ok((&path[0].pinned, pos + 1));
                };
                path.push(InsertRouteFrame {
                    pinned,
                    epoch: 0,
                    start: pos,
                    end: pos,
                    child: None,
                    search_end: pos,
                    next_slot: 0,
                });
            }
            let cached = &path[level];
            let epoch = cached.pinned.frame.dirty_epoch.load(Ordering::Acquire);
            if cached.epoch != epoch || pos < cached.start || pos >= cached.end {
                // A previous child fence is a search floor only inside the same
                // proved sorted interval and unchanged parent image. A new
                // unsorted prefix, reverse probe or changed epoch starts fresh.
                let floor = (cached.epoch == epoch && pos >= cached.end && pos < cached.search_end)
                    .then_some((cached.next_slot, cached.child))
                    .and_then(|(slot, child)| child.map(|child| (slot, child)));
                let (epoch, child, child_end, next_slot) = if cached.pinned.frame.has_packed_leaf()
                {
                    (epoch, None, end, 0)
                } else {
                    cached.pinned.with_index_raw_bytes(|bytes| {
                        let epoch = cached.pinned.frame.dirty_epoch.load(Ordering::Acquire);
                        let Some(page) = parse_raw_internal_route_page(bytes)? else {
                            return Ok((epoch, None, end, 0));
                        };
                        let entry = &entries[pos];
                        let (child, slot) = match floor.filter(|_| cached.epoch == epoch) {
                            Some((slot, child)) => {
                                page.route_near(&entry.key, entry.row_id, slot, child)?
                            }
                            None => {
                                page.route_from(&entry.key, entry.row_id, 0, page.first_child)?
                            }
                        };
                        let child_end = page.insert_run_end(entries, pos, end, slot)?;
                        Ok((epoch, Some(child), child_end, slot))
                    })?
                };
                // A changed parent may keep the same child id with a different
                // fence. Discard every descendant proof in that case too.
                path.truncate(level + 1);
                let cached = &mut path[level];
                cached.epoch = epoch;
                cached.start = pos;
                cached.end = child_end;
                cached.child = child;
                cached.search_end = end;
                cached.next_slot = next_slot;
            }
            let cached = &path[level];
            end = end.min(cached.end);
            match cached.child {
                None => {
                    path.truncate(level + 1);
                    return Ok((&path[level].pinned, end));
                }
                Some(child) => {
                    progress.advance(child)?;
                    page_id = child;
                    level += 1;
                }
            }
        }
    }

    /// Reuse buffer pins, never cached routing decisions. The caller holds
    /// structure_lock.read(): read the current root and every parent again,
    /// and retain only the prefix whose page ids still match that route.
    /// A discarded frame must be re-pinned even if its page id was recycled.
    /// Pins retain governed buffer frames; no page bytes or key vectors are
    /// copied, and the cache is bounded by this tree's current height.
    #[cfg(test)]
    fn find_leaf_for_insert_cached<'a>(
        &self,
        entry: &IndexEntry,
        path: &'a mut smallvec::SmallVec<[PinnedFrame; 4]>,
    ) -> Result<&'a PinnedFrame, QueryError> {
        self.ensure_healthy()?;
        let mut page_id = self.root_page.load(Ordering::Acquire);
        let mut progress = IndexPageProgress::new(page_id);
        let mut level = 0;
        loop {
            let reusable = path.get(level).is_some_and(|pinned| {
                pinned.frame.key.page_id == page_id
                    && matches!(
                        FrameIoState::from_u8(pinned.frame.state.load(Ordering::Acquire)),
                        FrameIoState::Ready | FrameIoState::Flushing
                    )
            });
            if !reusable {
                path.truncate(level);
                let key = self.page_key(page_id);
                if let Some(pinned) = self.buffer_pool.try_pin_resident_page(&key)? {
                    path.push(pinned);
                } else {
                    // This cache is optional: a cold descent must not retain
                    // ancestor pins while waiting to admit another page into
                    // a small/full pool. Release them and use the ordinary
                    // one-page-at-a-time navigator for this run.
                    path.clear();
                    path.push(self.find_leaf_for_entry_with_candidate(entry, entry)?.0);
                    return Ok(&path[0]);
                }
            }
            let route = path[level].with_index_raw_bytes(|bytes| {
                route_index_page_bytes(bytes, &entry.key, entry.row_id)
            })?;
            match route {
                RawIndexPageRoute::Leaf => {
                    path.truncate(level + 1);
                    return Ok(&path[level]);
                }
                RawIndexPageRoute::Child { page_id: child, .. } => {
                    progress.advance(child)?;
                    page_id = child;
                    level += 1;
                }
            }
        }
    }

    fn split_leaf(
        &self,
        txn_id: u64,
        page_id: u64,
        leaf: IndexLeafPage,
        path: Vec<(u64, usize)>,
    ) -> Result<(), QueryError> {
        if leaf.entries.len() < 2 {
            return Err(error(
                crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                "index entry is too large for a leaf page",
            ));
        }
        self.with_structural_recovery(|| self.split_leaf_guarded(txn_id, page_id, leaf, path))
    }

    fn split_leaf_guarded(
        &self,
        txn_id: u64,
        page_id: u64,
        mut leaf: IndexLeafPage,
        path: Vec<(u64, usize)>,
    ) -> Result<(), QueryError> {
        // Keep the split balanced unless the operator explicitly selected a
        // different global fill factor. Treating every current rightmost leaf
        // as an append-only leaf is unsafe under concurrent disjoint key
        // ranges: the identity of the right edge moves between writers and a
        // biased split leaves the page that the other writers soon revisit
        // unnecessarily sparse. That increases buffer churn precisely when
        // the pool is small.
        let split = split_entries_at_pct(&leaf.entries, index_fillfactor_pct());
        let right_entries = leaf.entries.split_off(split);
        let separator = right_entries.first().cloned().ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "index split produced empty right page",
            )
        })?;
        let right_id = self.alloc_page(txn_id)?;
        // Preserve the source page's writer bound onto both split
        // halves before rebuilding them (Phase 1b, see the append-
        // driven split site for the rationale).
        let source_bound = leaf.page_max_trx_id;
        // Each half of a split is a fresh page — give both halves a
        // chance to ride a compact layout (the demote-on-insert path
        // is sticky, so a previously-demoted leaf splits into two
        // halves that may now each fit a compact lane after the
        // narrower span).
        let mut right = IndexLeafPage::new_with_resolved_layout(right_entries, leaf.next_leaf);
        right.set_page_max_trx_id(source_bound);
        let mut left = IndexLeafPage::new_with_resolved_layout(
            std::mem::take(&mut leaf.entries),
            Some(right_id),
        );
        left.set_page_max_trx_id(source_bound);
        // Publish the new right leaf before linking it from the left
        // leaf; concurrent next_leaf scans must never observe an
        // allocated-but-unformatted page.
        self.write_page(txn_id, right_id, IndexPage::Leaf(right))?;
        self.write_page(txn_id, page_id, IndexPage::Leaf(left))?;
        self.insert_separator(txn_id, path, page_id, right_id, separator)
    }

    fn insert_separator(
        &self,
        txn_id: u64,
        mut path: Vec<(u64, usize)>,
        left_id: u64,
        right_id: u64,
        separator: IndexEntry,
    ) -> Result<(), QueryError> {
        let Some((parent_id, child_idx)) = path.pop() else {
            let root_id = self.alloc_page(txn_id)?;
            self.write_page(
                txn_id,
                root_id,
                IndexPage::Internal(IndexInternalPage::new_generic(
                    vec![separator],
                    vec![left_id, right_id],
                )),
            )?;
            self.meta_state.lock().meta.root_page = root_id;
            self.root_page.store(root_id, Ordering::Release);
            self.write_meta_page(txn_id)?;
            return Ok(());
        };
        let mut parent = self.read_internal(parent_id)?;
        if parent.children.get(child_idx).copied() != Some(left_id) {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: split parent mismatch",
            ));
        }
        parent.children.insert(child_idx + 1, right_id);
        parent.keys.insert(child_idx, separator);
        if encoded_internal_page_len(&parent) <= PAGE_SIZE {
            self.write_page(txn_id, parent_id, IndexPage::Internal(parent))?;
        } else {
            self.split_internal(txn_id, parent_id, parent, path)?;
        }
        Ok(())
    }

    fn split_internal(
        &self,
        txn_id: u64,
        page_id: u64,
        mut internal: IndexInternalPage,
        path: Vec<(u64, usize)>,
    ) -> Result<(), QueryError> {
        let split = (internal.children.len() / 2).clamp(1, internal.children.len() - 1);
        let right_children = internal.children.split_off(split);
        let (right, separator) =
            if internal.keys.len() + 1 == internal.children.len() + right_children.len() {
                // `keys[i]` already separates `children[i]` from `children[i + 1]`.
                // Splitting between children `split - 1` and `split` therefore
                // only partitions the existing separators; re-reading every
                // child's first entry would turn a single split into
                // O(fan-out) page pins (cold disk reads under a write guard).
                let right_keys = internal.keys.split_off(split);
                let separator = internal.keys.pop().ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "index internal split has no separator",
                    )
                })?;
                (
                    IndexInternalPage::new_generic(right_keys, right_children),
                    separator,
                )
            } else {
                self.recompute_internal_keys(&mut internal)?;
                let mut right = IndexInternalPage::new_generic(Vec::new(), right_children);
                self.recompute_internal_keys(&mut right)?;
                let separator = self
                    .first_entry_of_page(*right.children.first().ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "index internal split has no child",
                        )
                    })?)?
                    .ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "index internal split has empty child",
                        )
                    })?;
                (right, separator)
            };
        let right_id = self.alloc_page(txn_id)?;
        self.write_page(txn_id, page_id, IndexPage::Internal(internal))?;
        self.write_page(txn_id, right_id, IndexPage::Internal(right))?;
        self.insert_separator(txn_id, path, page_id, right_id, separator)
    }

    fn rebalance_after_leaf_delete(
        &self,
        txn_id: u64,
        page_id: u64,
        path: Vec<(u64, usize)>,
        first_entry_changed: bool,
    ) -> Result<(), QueryError> {
        if page_id == self.root_page.load(Ordering::Acquire) {
            return Ok(());
        }
        // For sysbench-style single-row deletes the leaf is almost
        // always still above the min-fill threshold, so we only need to
        // inspect its size. Only descend into
        // `rebalance_child` (which does take ownership) when the leaf
        // actually under-flowed.
        let (live_size, entry_count) = {
            let pinned = self.buffer_pool.pin_page(self.page_key(page_id))?;
            let result = pinned.with_index_raw_bytes(raw_leaf_live_encoded_len_and_count);
            drop(pinned);
            result?
        };
        // Raw slotted-page deletes deliberately leave record-body holes so
        // the hot path only removes a 4-byte slot. `payload_len` therefore
        // measures the high-water mark, not live occupancy; using it here
        // lets a drained leaf look permanently full. In particular, the old
        // `|| is_empty` return left an empty non-root child in its parent and
        // the next separator recomputation failed with
        // "internal child has no first key".
        if entry_count > 0 && live_size >= INDEX_MIN_PAGE_BYTES {
            // The parent's separator at `child_idx - 1` is the leaf's
            // current first entry. If the deletion didn't change the
            // first entry, the separator is still correct — skip the
            // refresh entirely (which would otherwise re-encode the
            // parent internal page to rewrite the same key it already
            // holds). For sysbench oltp_read_write with ~250 entries
            // per leaf, only ~0.4 % of deletes hit position 0.
            if first_entry_changed {
                self.refresh_parent_separator(txn_id, &path, page_id)?;
            }
            return Ok(());
        }
        self.rebalance_child(txn_id, page_id, path)
    }

    fn rebalance_internal_if_needed(
        &self,
        txn_id: u64,
        page_id: u64,
        path: Vec<(u64, usize)>,
    ) -> Result<(), QueryError> {
        if page_id == self.root_page.load(Ordering::Acquire) {
            let root = self.read_page(page_id)?;
            if let IndexPage::Internal(root) = root.as_ref() {
                if root.children.len() == 1 {
                    let new_root = root.children[0];
                    self.meta_state.lock().meta.root_page = new_root;
                    self.root_page.store(new_root, Ordering::Release);
                    self.write_meta_page(txn_id)?;
                    self.free_page(txn_id, page_id)?;
                }
                // A multi-child root needs no further work: the caller has
                // already written the merged root with consistent separators.
                // Re-deriving them would pin every child of the root (the
                // whole leaf level of a small index) under the write guard.
            }
            return Ok(());
        }
        let internal = self.read_internal(page_id)?;
        if encoded_internal_page_len(&internal) >= INDEX_MIN_PAGE_BYTES
            || internal.children.len() <= 1
        {
            self.refresh_parent_separator(txn_id, &path, page_id)?;
            return Ok(());
        }
        self.rebalance_child(txn_id, page_id, path)
    }

    fn rebalance_child(
        &self,
        txn_id: u64,
        page_id: u64,
        mut path: Vec<(u64, usize)>,
    ) -> Result<(), QueryError> {
        let Some((parent_id, child_idx)) = path.pop() else {
            return Ok(());
        };
        let mut parent = self.read_internal(parent_id)?;
        if parent.children.get(child_idx).copied() != Some(page_id) {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: rebalance parent mismatch",
            ));
        }
        if child_idx > 0 {
            let left_id = parent.children[child_idx - 1];
            if self.try_borrow_from_left(txn_id, left_id, page_id, &mut parent, child_idx)? {
                // Borrowing changes the separator in-place. A variable-width
                // key can grow here (for example TPC-C's nullable
                // o_carrier_id changes from NULL to INT), so a parent that
                // was filled by bulk load may no longer fit. Treat this like
                // every other internal insertion instead of blindly
                // serializing an oversized page.
                if encoded_internal_page_len(&parent) <= PAGE_SIZE {
                    self.write_page(txn_id, parent_id, IndexPage::Internal(parent))?;
                    self.refresh_parent_separator(txn_id, &path, parent_id)?;
                } else {
                    self.split_internal(txn_id, parent_id, parent, path)?;
                }
                return Ok(());
            }
        }
        if child_idx + 1 < parent.children.len() {
            let right_id = parent.children[child_idx + 1];
            if self.try_borrow_from_right(txn_id, page_id, right_id, &mut parent, child_idx)? {
                if encoded_internal_page_len(&parent) <= PAGE_SIZE {
                    self.write_page(txn_id, parent_id, IndexPage::Internal(parent))?;
                    self.refresh_parent_separator(txn_id, &path, parent_id)?;
                } else {
                    self.split_internal(txn_id, parent_id, parent, path)?;
                }
                return Ok(());
            }
        }
        if child_idx > 0 {
            let left_id = parent.children[child_idx - 1];
            let separator = parent.keys.get(child_idx - 1).cloned();
            if self.try_merge_pages(txn_id, left_id, page_id, separator)? {
                // `page` was absorbed into `left`: drop the separator between
                // them. Every other separator still bounds its child.
                parent.children.remove(child_idx);
                parent.keys.remove(child_idx - 1);
                self.write_page(txn_id, parent_id, IndexPage::Internal(parent))?;
                return self.rebalance_internal_if_needed(txn_id, parent_id, path);
            }
        }
        if child_idx + 1 < parent.children.len() {
            let right_id = parent.children[child_idx + 1];
            let separator = parent.keys.get(child_idx).cloned();
            if self.try_merge_pages(txn_id, page_id, right_id, separator)? {
                // `right` was absorbed into `page`: drop the separator between
                // them. The merged page keeps its own lower-bound separator.
                parent.children.remove(child_idx + 1);
                parent.keys.remove(child_idx);
                if child_idx > 0 {
                    // The delete that triggered this rebalance may have removed
                    // the page's first entry; keep its separator exact.
                    if let Some(first) = self.first_entry_of_page(page_id)? {
                        parent.keys[child_idx - 1] = first;
                    }
                }
                self.write_page(txn_id, parent_id, IndexPage::Internal(parent))?;
                return self.rebalance_internal_if_needed(txn_id, parent_id, path);
            }
        }
        self.write_page(txn_id, parent_id, IndexPage::Internal(parent))?;
        self.refresh_parent_separator(txn_id, &path, parent_id)
    }

    fn try_borrow_from_left(
        &self,
        txn_id: u64,
        left_id: u64,
        page_id: u64,
        parent: &mut IndexInternalPage,
        child_idx: usize,
    ) -> Result<bool, QueryError> {
        let left_page = self.read_page(left_id)?;
        let page_page = self.read_page(page_id)?;
        match (left_page.as_ref(), page_page.as_ref()) {
            (IndexPage::Leaf(left), IndexPage::Leaf(page)) => {
                let left = left.clone();
                let page = page.clone();
                if left.entries.len() <= 1 || encoded_leaf_page_len(&left) <= INDEX_MIN_PAGE_BYTES {
                    return Ok(false);
                }
                // Redistribute the complete adjacent run by encoded bytes.
                // Moving one record left the receiver at (or immediately
                // below) the minimum, so a delete stream re-entered this
                // structural path on every row. A balanced redistribution
                // amortizes the same two-leaf + parent rewrite across the
                // following quarter-page of deletes.
                let left_next = left.next_leaf;
                let page_next = page.next_leaf;
                let source_bound = left.page_max_trx_id.max(page.page_max_trx_id);
                let mut combined = left.entries;
                combined.extend(page.entries);
                let split = split_entries_at_pct(&combined, 50);
                let page_entries = combined.split_off(split);
                if combined.is_empty() || page_entries.is_empty() {
                    return Ok(false);
                }
                let mut left = IndexLeafPage::new_with_resolved_layout(combined, left_next);
                let mut page = IndexLeafPage::new_with_resolved_layout(page_entries, page_next);
                if encoded_leaf_page_len(&left) < INDEX_MIN_PAGE_BYTES
                    || encoded_leaf_page_len(&page) < INDEX_MIN_PAGE_BYTES
                    || encoded_leaf_page_len(&left) > PAGE_SIZE
                    || encoded_leaf_page_len(&page) > PAGE_SIZE
                {
                    return Ok(false);
                }
                parent.keys[child_idx - 1] = page.entries.first().cloned().ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "borrow produced empty leaf",
                    )
                })?;
                // Both leaves had entries shift across — re-pick
                // layout so the encoder doesn't trip when the moved
                // entry falls outside the destination's compact window.
                // Preserve the union of both sources' page_max_trx_id
                // onto both halves; write_page raises further to the
                // current writer.
                left.set_page_max_trx_id(source_bound);
                page.set_page_max_trx_id(source_bound);
                self.write_page(txn_id, left_id, IndexPage::Leaf(left))?;
                self.write_page(txn_id, page_id, IndexPage::Leaf(page))?;
                Ok(true)
            }
            (IndexPage::Internal(left), IndexPage::Internal(page)) => {
                let mut left = left.clone();
                let mut page = page.clone();
                if left.children.len() <= 2
                    || encoded_internal_page_len(&left) <= INDEX_MIN_PAGE_BYTES
                {
                    return Ok(false);
                }
                let moved = left
                    .children
                    .pop()
                    .expect("left internal should have child after len check");
                page.children.insert(0, moved);
                self.recompute_internal_keys(&mut left)?;
                self.recompute_internal_keys(&mut page)?;
                parent.keys[child_idx - 1] = self.first_entry_of_page(moved)?.ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "borrow produced empty internal",
                    )
                })?;
                let left = IndexInternalPage::new_generic(left.keys, left.children);
                let page = IndexInternalPage::new_generic(page.keys, page.children);
                self.write_page(txn_id, left_id, IndexPage::Internal(left))?;
                self.write_page(txn_id, page_id, IndexPage::Internal(page))?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn try_borrow_from_right(
        &self,
        txn_id: u64,
        page_id: u64,
        right_id: u64,
        parent: &mut IndexInternalPage,
        child_idx: usize,
    ) -> Result<bool, QueryError> {
        let page_page = self.read_page(page_id)?;
        let right_page = self.read_page(right_id)?;
        match (page_page.as_ref(), right_page.as_ref()) {
            (IndexPage::Leaf(page), IndexPage::Leaf(right)) => {
                let page = page.clone();
                let right = right.clone();
                if right.entries.len() <= 1 || encoded_leaf_page_len(&right) <= INDEX_MIN_PAGE_BYTES
                {
                    return Ok(false);
                }
                let page_next = page.next_leaf;
                let right_next = right.next_leaf;
                let source_bound = page.page_max_trx_id.max(right.page_max_trx_id);
                let mut combined = page.entries;
                combined.extend(right.entries);
                let split = split_entries_at_pct(&combined, 50);
                let right_entries = combined.split_off(split);
                if combined.is_empty() || right_entries.is_empty() {
                    return Ok(false);
                }
                let mut page = IndexLeafPage::new_with_resolved_layout(combined, page_next);
                let mut right = IndexLeafPage::new_with_resolved_layout(right_entries, right_next);
                if encoded_leaf_page_len(&page) < INDEX_MIN_PAGE_BYTES
                    || encoded_leaf_page_len(&right) < INDEX_MIN_PAGE_BYTES
                    || encoded_leaf_page_len(&page) > PAGE_SIZE
                    || encoded_leaf_page_len(&right) > PAGE_SIZE
                {
                    return Ok(false);
                }
                parent.keys[child_idx] = right.entries.first().cloned().ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "borrow produced empty right leaf",
                    )
                })?;
                // Re-pick layout on both halves (see try_borrow_from_left).
                // Preserve max of both sources' page_max_trx_id.
                page.set_page_max_trx_id(source_bound);
                right.set_page_max_trx_id(source_bound);
                self.write_page(txn_id, page_id, IndexPage::Leaf(page))?;
                self.write_page(txn_id, right_id, IndexPage::Leaf(right))?;
                Ok(true)
            }
            (IndexPage::Internal(page), IndexPage::Internal(right)) => {
                let mut page = page.clone();
                let mut right = right.clone();
                if right.children.len() <= 2
                    || encoded_internal_page_len(&right) <= INDEX_MIN_PAGE_BYTES
                {
                    return Ok(false);
                }
                let moved = right.children.remove(0);
                page.children.push(moved);
                self.recompute_internal_keys(&mut page)?;
                self.recompute_internal_keys(&mut right)?;
                parent.keys[child_idx] =
                    self.first_entry_of_page(right.children[0])?
                        .ok_or_else(|| {
                            error(
                                crate::error::catalog::ER_GET_ERRNO,
                                "borrow produced empty right internal",
                            )
                        })?;
                let page = IndexInternalPage::new_generic(page.keys, page.children);
                let right = IndexInternalPage::new_generic(right.keys, right.children);
                self.write_page(txn_id, page_id, IndexPage::Internal(page))?;
                self.write_page(txn_id, right_id, IndexPage::Internal(right))?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn try_merge_pages(
        &self,
        txn_id: u64,
        left_id: u64,
        right_id: u64,
        separator: Option<IndexEntry>,
    ) -> Result<bool, QueryError> {
        let left_page = self.read_page(left_id)?;
        let right_page = self.read_page(right_id)?;
        match (left_page.as_ref(), right_page.as_ref()) {
            (IndexPage::Leaf(left), IndexPage::Leaf(right)) => {
                let mut merged = left.entries.clone();
                merged.extend(right.entries.iter().cloned());
                // Preserve max of both sources' page_max_trx_id onto
                // the merged page so any writer that touched either
                // half is still reflected.
                let source_bound = left.page_max_trx_id.max(right.page_max_trx_id);
                // Merge produces a fresh entry set — re-pick the
                // densest viable layout. Carrying the old `left.key_layout`
                // forward is unsafe: right's entries may sit outside
                // left's base+delta window, and the encoder would
                // error out at write time.
                let mut candidate =
                    IndexLeafPage::new_with_resolved_layout(merged, right.next_leaf);
                candidate.set_page_max_trx_id(source_bound);
                if encoded_leaf_page_len(&candidate) > PAGE_SIZE {
                    return Ok(false);
                }
                self.write_page(txn_id, left_id, IndexPage::Leaf(candidate))?;
                self.free_page(txn_id, right_id)?;
                Ok(true)
            }
            (IndexPage::Internal(left), IndexPage::Internal(right)) => {
                let mut left = left.clone();
                let mut children = left.children.clone();
                children.extend(right.children.iter().copied());
                let mut candidate = IndexInternalPage::new_generic(Vec::new(), children);
                match separator {
                    Some(separator)
                        if left.keys.len() + 1 == left.children.len()
                            && right.keys.len() + 1 == right.children.len() =>
                    {
                        // The parent's separator is the bound between the two
                        // pages; concatenating the three runs yields the merged
                        // page's separators without pinning any child.
                        let mut keys = Vec::with_capacity(left.keys.len() + 1 + right.keys.len());
                        keys.extend(left.keys.iter().cloned());
                        keys.push(separator);
                        keys.extend(right.keys.iter().cloned());
                        candidate.keys = keys;
                    }
                    _ => self.recompute_internal_keys(&mut candidate)?,
                }
                if encoded_internal_page_len(&candidate) > PAGE_SIZE {
                    return Ok(false);
                }
                left.children = candidate.children;
                left.keys = candidate.keys;
                self.write_page(txn_id, left_id, IndexPage::Internal(left))?;
                self.free_page(txn_id, right_id)?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn refresh_parent_separator(
        &self,
        txn_id: u64,
        path: &[(u64, usize)],
        page_id: u64,
    ) -> Result<(), QueryError> {
        let Some((parent_id, child_idx)) = path.last().copied() else {
            return Ok(());
        };
        if child_idx == 0 {
            return Ok(());
        }
        let separator = self.first_entry_of_page(page_id)?;
        let Some(separator) = separator else {
            return Ok(());
        };
        // Refresh the parent separator directly in encoded bytes, avoiding
        // materialization of every separator to overwrite a single key.
        if let Some(fit) =
            self.try_refresh_parent_separator_in_place(parent_id, child_idx, &separator)?
        {
            if fit {
                self.dirty.store(true, Ordering::Release);
                return Ok(());
            }
            // Otherwise the separator overwrite would overflow the
            // internal page — fall through to the slow take/split path.
        }
        let mut parent = self.read_internal(parent_id)?;
        parent.keys[child_idx - 1] = separator;
        if encoded_internal_page_len(&parent) <= PAGE_SIZE {
            self.write_page(txn_id, parent_id, IndexPage::Internal(parent))?;
        } else {
            let ancestor_path = path[..path.len().saturating_sub(1)].to_vec();
            self.split_internal(txn_id, parent_id, parent, ancestor_path)?;
        }
        Ok(())
    }

    /// Fast path for `refresh_parent_separator`: try to update
    /// `parent.keys[child_idx - 1]` in place without cloning the
    /// internal page. Returns `Some(true)` when the new separator
    /// fits in the existing page, `Some(false)` when it doesn't
    /// (caller takes slow split path), and `None` when the frame
    /// isn't eligible (caller falls back to the take/clone path).
    fn try_refresh_parent_separator_in_place(
        &self,
        parent_id: u64,
        child_idx: usize,
        separator: &IndexEntry,
    ) -> Result<Option<bool>, QueryError> {
        if !self.lazy_write_eligible(parent_id)? {
            return Ok(None);
        }
        if child_idx == 0 {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: separator slot out of range",
            ));
        }
        let key = self.page_key(parent_id);
        let peek_lsn = self.lazy_mutation_lsn();
        if let Some(result) = self
            .buffer_pool
            .try_modify_raw_page(&key, peek_lsn, |bytes| {
                let result = try_replace_raw_internal_separator(bytes, child_idx - 1, separator)?;
                Ok((result.is_some(), result))
            })?
        {
            if result.is_some() {
                return Ok(Some(true));
            }
        }
        self.buffer_pool
            .modify_index_page_fallback(&key, peek_lsn, |page| {
                let IndexPage::Internal(internal) = page else {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: expected internal page",
                    ));
                };
                if child_idx == 0 || child_idx > internal.keys.len() {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: separator slot out of range",
                    ));
                }
                let saved = std::mem::replace(&mut internal.keys[child_idx - 1], separator.clone());
                if encoded_internal_page_len(internal) <= PAGE_SIZE {
                    return Ok((true, true));
                }
                // Restore — overflow means caller must split.
                internal.keys[child_idx - 1] = saved;
                Ok((false, false))
            })
    }

    fn recompute_internal_keys(&self, page: &mut IndexInternalPage) -> Result<(), QueryError> {
        let mut keys = Vec::with_capacity(page.children.len().saturating_sub(1));
        for child in page.children.iter().skip(1).copied() {
            keys.push(self.first_entry_of_page(child)?.ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "internal child has no first key",
                )
            })?);
        }
        page.keys = keys;
        Ok(())
    }

    fn first_entry_of_page(&self, page_id: u64) -> Result<Option<IndexEntry>, QueryError> {
        let mut current = page_id;
        let mut progress = IndexPageProgress::new(current);
        loop {
            let pinned = self.buffer_pool.pin_page(self.page_key(current))?;
            let step = pinned.with_index_raw_bytes(raw_first_entry_step)?;
            drop(pinned);
            match step {
                RawFirstEntryStep::Leaf(entry) => return Ok(entry),
                RawFirstEntryStep::Child(child) => {
                    progress.advance(child)?;
                    current = child;
                }
            }
        }
    }

    fn leftmost_leaf(&self) -> Result<u64, QueryError> {
        let mut page_id = self.root_page.load(Ordering::Acquire);
        let mut progress = IndexPageProgress::new(page_id);
        loop {
            let pinned = self.buffer_pool.pin_page(self.page_key(page_id))?;
            let child =
                pinned.with_index_raw_bytes(|bytes| edge_child_index_page_bytes(bytes, false))?;
            drop(pinned);
            match child {
                Some(child) => {
                    progress.advance(child)?;
                    page_id = child;
                }
                None => return Ok(page_id),
            }
        }
    }

    #[allow(dead_code)]
    fn rightmost_leaf(&self) -> Result<u64, QueryError> {
        self.rightmost_leaf_from(self.root_page.load(Ordering::Acquire))
    }

    fn prev_leaf(&self, leaf_id: u64) -> Result<Option<u64>, QueryError> {
        if let Some(path) = self.path_to_leaf_by_first_entry(leaf_id)? {
            for (parent_id, child_idx) in path.into_iter().rev() {
                if child_idx == 0 {
                    continue;
                }
                let parent = self.read_internal(parent_id)?;
                let left_sibling = *parent.children.get(child_idx - 1).ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: previous child is missing",
                    )
                })?;
                return self.rightmost_leaf_from(left_sibling).map(Some);
            }
            return Ok(None);
        }

        let mut current = self.leftmost_leaf()?;
        let mut previous = None;
        loop {
            if current == leaf_id {
                return Ok(previous);
            }
            let leaf_page = self.read_page(current)?;
            let leaf = match leaf_page.as_ref() {
                IndexPage::Leaf(leaf) => leaf,
                _ => {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: expected leaf page",
                    ))
                }
            };
            let Some(next_leaf) = leaf.next_leaf else {
                return Ok(None);
            };
            previous = Some(current);
            current = next_leaf;
        }
    }

    fn path_to_leaf_by_first_entry(
        &self,
        leaf_id: u64,
    ) -> Result<Option<Vec<(u64, usize)>>, QueryError> {
        let pinned = self.buffer_pool.pin_page(self.page_key(leaf_id))?;
        let first = pinned.with_index_raw_bytes(|bytes| {
            let header = parse_leaf_payload_header(bytes)?;
            (header.count > 0)
                .then(|| decode_raw_leaf_entry_at(&header, 0))
                .transpose()
        })?;
        drop(pinned);
        let Some(first) = first else {
            return Ok(None);
        };
        let mut path = Vec::new();
        let found_leaf = self.find_leaf_for_entry(&first, &mut path)?;
        if found_leaf == leaf_id {
            Ok(Some(path))
        } else {
            Ok(None)
        }
    }

    fn rightmost_leaf_from(&self, mut page_id: u64) -> Result<u64, QueryError> {
        let mut progress = IndexPageProgress::new(page_id);
        loop {
            let pinned = self.buffer_pool.pin_page(self.page_key(page_id))?;
            let child =
                pinned.with_index_raw_bytes(|bytes| edge_child_index_page_bytes(bytes, true))?;
            drop(pinned);
            match child {
                Some(child) => {
                    progress.advance(child)?;
                    page_id = child;
                }
                None => return Ok(page_id),
            }
        }
    }

    fn is_empty(&self) -> Result<bool, QueryError> {
        if let Some(spatial) = &self.spatial {
            return spatial.is_empty();
        }
        if self.online_build_log.is_some() {
            // LOAD DATA must use its incremental writer, never replace the
            // private tree being built by CREATE INDEX.
            return Ok(false);
        }
        match self
            .read_page(self.root_page.load(Ordering::Acquire))?
            .as_ref()
        {
            IndexPage::Leaf(page) => Ok(page.entries.is_empty()),
            IndexPage::Internal(_) => Ok(false),
            IndexPage::Free(_) => Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: root is free page",
            )),
        }
    }

    fn alloc_page(&self, txn_id: u64) -> Result<u64, QueryError> {
        // Hold the meta-state lock for the whole observation-and-mutation
        // window so concurrent `alloc_page` calls don't pop the same
        // freelist head twice or race their `next_page_id` watermarks.
        // The cold lock is brief — the actual disk write happens after.
        let page_id = {
            // Peek the freelist head under the lock, drop it before the
            // recursive `read_page` (which itself never touches
            // `meta_state`), then re-acquire to commit the new head.
            // Re-check that the head still matches in case another thread
            // grabbed it between drop and re-acquire.
            let head = self.meta_state.lock().meta.free_page_head;
            if let Some(head) = head {
                let next = match self.read_page(head)?.as_ref() {
                    IndexPage::Free(page) => page.next_free,
                    _ => {
                        return Err(error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "corrupt index: freelist head is not a free page",
                        ))
                    }
                };
                let mut state = self.meta_state.lock();
                if state.meta.free_page_head == Some(head) {
                    state.meta.free_page_head = next;
                    drop(state);
                    self.write_or_defer_meta_page(txn_id)?;
                    return Ok(head);
                }
                // Lost the race — fall through to the extend branch.
            }
            let mut state = self.meta_state.lock();
            self.extend_page_range(&mut state)?
        };
        self.write_or_defer_meta_page(txn_id)?;
        Ok(page_id)
    }

    fn free_page(&self, txn_id: u64, page_id: u64) -> Result<(), QueryError> {
        if page_id == self.root_page.load(Ordering::Acquire) || page_id == INDEX_META_PAGE {
            return Ok(());
        }
        let free = IndexPage::Free(IndexFreePage {
            next_free: self.meta_state.lock().meta.free_page_head,
        });
        // Publish a valid Free image before advertising it in the freelist.
        self.write_page(txn_id, page_id, free)?;
        self.meta_state.lock().meta.free_page_head = Some(page_id);
        self.write_or_defer_meta_page(txn_id)?;
        Ok(())
    }

    fn read_leaf(&self, page_id: u64) -> Result<IndexLeafPage, QueryError> {
        match Arc::unwrap_or_clone(self.read_page(page_id)?) {
            IndexPage::Leaf(page) => Ok(page),
            _ => Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: expected leaf page",
            )),
        }
    }

    fn read_internal(&self, page_id: u64) -> Result<IndexInternalPage, QueryError> {
        match Arc::unwrap_or_clone(self.read_page(page_id)?) {
            IndexPage::Internal(page) => Ok(page),
            _ => Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: expected internal page",
            )),
        }
    }

    fn read_page(&self, page_id: u64) -> Result<Arc<IndexPage>, QueryError> {
        self.read_page_with_intent(page_id, AccessIntent::Normal)
    }

    /// Page reader that accepts an `AccessIntent`. Use `Scan` for
    /// linear leaf-sweep paths (covering-index aggregate, range full
    /// scans) so the buffer pool's midpoint LRU keeps the pages in the
    /// cold sublist instead of promoting them to `young` and evicting
    /// genuinely hot pages. Random point-lookups should keep `Normal`.
    fn read_page_with_intent(
        &self,
        page_id: u64,
        intent: AccessIntent,
    ) -> Result<Arc<IndexPage>, QueryError> {
        self.ensure_healthy()?;
        if page_id == INDEX_META_PAGE {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "index meta page is not a tree page",
            ));
        }
        let pinned = self
            .buffer_pool
            .pin_page_with_intent(self.page_key(page_id), intent)?;
        pinned.index_page()
    }

    /// Count the raw leaf entries in `page_lo..page_hi` while `accept_bound`
    /// proves each leaf all-visible.  Returns `(accepted_rows, resume_page)`;
    /// `resume_page == page_hi` means the whole range was accepted, otherwise
    /// the caller must run its normal MVCC-aware path starting at that page.
    ///
    /// COUNT(*) does not consume index keys or RowIds. Reading the compact
    /// leaf header avoids materialising every `IndexKey` (and its heap-backed
    /// parts) solely to discover `entries.len()`.
    pub(crate) fn count_leaf_entries_page_range_while<F>(
        &self,
        page_lo: u64,
        page_hi: u64,
        mut accept_bound: F,
    ) -> Result<(u64, u64), QueryError>
    where
        F: FnMut(u64) -> bool,
    {
        let _structure = self.structure_lock.read_recursive();
        self.ensure_healthy()?;
        let upper = self.next_page_id();
        let page_lo = page_lo.max(INDEX_EMPTY_ROOT).min(upper);
        let page_hi = page_hi.max(page_lo).min(upper);
        let window = self.buffer_pool.effective_prefetch_window() as u64;
        if window > 0 {
            self.prefetch_page_range(page_lo, page_hi);
        }
        let mut count = 0u64;
        for page_id in page_lo..page_hi {
            if window > 0 {
                let prefetch_at = page_id.saturating_add(window);
                if prefetch_at < page_hi {
                    self.buffer_pool.prefetch_page(self.page_key(prefetch_at));
                }
            }
            let pinned = self
                .buffer_pool
                .pin_page_with_intent(self.page_key(page_id), AccessIntent::Scan)?;
            let summary = pinned.with_index_raw_bytes(raw_leaf_count_and_writer_bound)?;
            drop(pinned);
            let Some((leaf_count, page_max_trx_id)) = summary else {
                continue;
            };
            if !accept_bound(page_max_trx_id) {
                return Ok((count, page_id));
            }
            count = count.saturating_add(leaf_count as u64);
        }
        Ok((count, page_hi))
    }

    /// Project one numeric/temporal key lane from a visible physical prefix.
    /// The caller owns range eligibility and falls back at the returned page.
    pub(crate) fn visit_numeric_leaf_prefix_page_range<F, V>(
        &self,
        page_lo: u64,
        page_hi: u64,
        key_position: usize,
        data_type: &DataTypeKind,
        mut accept_bound: F,
        mut visitor: V,
    ) -> Result<u64, QueryError>
    where
        F: FnMut(u64) -> bool,
        V: FnMut(&[Value]) -> Result<(), QueryError>,
    {
        use crate::engine::executor::mem_budget::{
            ControlledMemoryCategory, ControlledMemoryCharge,
        };
        const CHUNK: usize = 256;
        let _structure = self.structure_lock.read_recursive();
        self.ensure_healthy()?;
        let upper = self.next_page_id();
        let page_lo = page_lo.max(INDEX_EMPTY_ROOT).min(upper);
        let page_hi = page_hi.max(page_lo).min(upper);
        let Some(_charge) = ControlledMemoryCharge::try_new_optional(
            ControlledMemoryCategory::StorageScratch,
            CHUNK * std::mem::size_of::<Value>(),
        ) else {
            return Ok(page_lo);
        };
        let mut values = Vec::with_capacity(CHUNK);
        let window = self.buffer_pool.effective_prefetch_window() as u64;
        if window > 0 {
            self.prefetch_page_range(page_lo, page_hi);
        }
        for page_id in page_lo..page_hi {
            crate::engine::cancel::check_cancelled()?;
            if window > 0 && page_id.saturating_add(window) < page_hi {
                self.buffer_pool
                    .prefetch_page(self.page_key(page_id + window));
            }
            let pinned = self
                .buffer_pool
                .pin_page_with_intent(self.page_key(page_id), AccessIntent::Scan)?;
            let accepted = pinned.with_index_raw_bytes(|buf| {
                let Some((_, bound)) = raw_leaf_count_and_writer_bound(buf)? else {
                    return Ok(true);
                };
                if !accept_bound(bound) {
                    return Ok(false);
                }
                let header = parse_leaf_payload_header(buf)?;
                for slot in 0..header.count {
                    let record = header.read_slot(slot);
                    let mut cursor = record.offset as usize;
                    let end = cursor
                        .checked_add(record.len as usize)
                        .filter(|end| record.len != 0 && *end <= header.slot_array_start)
                        .ok_or_else(|| {
                            error(
                                crate::error::catalog::ER_GET_ERRNO,
                                "corrupt index: slot record bounds",
                            )
                        })?;
                    let key = header.decode_numeric_part_at(&mut cursor, key_position)?;
                    let _ =
                        decode_row_id_v7_from(header.payload, &mut cursor, header.page_table_id)?;
                    if cursor != end {
                        return Err(error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "corrupt index: slot length mismatch",
                        ));
                    }
                    values.push(key.to_value(data_type));
                    if values.len() == CHUNK {
                        visitor(&values)?;
                        values.clear();
                    }
                }
                if !values.is_empty() {
                    visitor(&values)?;
                    values.clear();
                }
                Ok(true)
            })?;
            if !accepted {
                return Ok(page_id);
            }
        }
        Ok(page_hi)
    }

    /// Select one physical entry in `[low, high]` by its zero-based ordinal
    /// after proving that every supplied leaf is visible to the caller's
    /// snapshot.
    ///
    /// The first pass reads compact leaf headers and bisects the requested
    /// range boundaries to obtain the exact row count. `choose_ordinal` runs
    /// only after every visibility bound passed, so a caller may consume
    /// volatile state (notably the exact N draws for `ORDER BY RAND() LIMIT
    /// 1`) without making fallback paths consume it twice. The second pass
    /// decodes only the chosen slot. Both passes hold the index read handle,
    /// excluding a concurrent structural mutation between counting and
    /// selection.
    pub(crate) fn select_range_leaf_entry_by_ordinal_if<F, S>(
        &self,
        low: Option<&IndexBound>,
        high: Option<&IndexBound>,
        accept_bound: F,
        choose_ordinal: S,
    ) -> Result<Option<(Option<(IndexKey, RowId)>, usize)>, QueryError>
    where
        F: FnMut(u64) -> bool,
        S: FnOnce(usize) -> usize,
    {
        self.select_range_leaf_entry_by_filtered_ordinal_if(
            low,
            high,
            &[],
            accept_bound,
            choose_ordinal,
        )
    }

    pub(crate) fn select_range_leaf_entry_by_filtered_ordinal_if<F, S>(
        &self,
        low: Option<&IndexBound>,
        high: Option<&IndexBound>,
        filter_terms: &[(usize, KeyPart)],
        mut accept_bound: F,
        choose_ordinal: S,
    ) -> Result<Option<(Option<(IndexKey, RowId)>, usize)>, QueryError>
    where
        F: FnMut(u64) -> bool,
        S: FnOnce(usize) -> usize,
    {
        // Resolve the cheap internal-level frontier while holding the latch
        // for both the header/count pass and selected-slot read. This is the
        // same stale-page window the streaming cursor defends with its
        // mutation generation, but this bounded two-pass path can exclude it
        // outright without retaining a long-lived guard.
        let _structure = self.structure_lock.read_recursive();
        self.ensure_healthy()?;
        let leaf_ids = self.range_leaf_ids_locked(low, high)?;
        let cache_key = RangeOrderStatsKey {
            low: low.cloned(),
            high: high.cloned(),
            filter_terms: filter_terms.to_vec(),
        };
        let Some(generation) = self.stable_generation() else {
            return Ok(None);
        };
        let cached = self
            .order_stats_cache
            .lock()
            .get(&cache_key)
            .filter(|cached| cached.generation == generation)
            .map(|cached| cached.stats.clone());
        if cached.is_some() {
            ORDER_STATS_CACHE_HITS.fetch_add(1, Ordering::Relaxed);
        } else {
            ORDER_STATS_CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
        }
        let stats = match cached {
            Some(stats) => stats,
            None => {
                let window = self.buffer_pool.effective_prefetch_window();
                let mut leaves = Vec::with_capacity(leaf_ids.len());
                let mut row_count = 0usize;
                for (position, &page_id) in leaf_ids.iter().enumerate() {
                    if window > 0 {
                        if let Some(&prefetch_id) = leaf_ids.get(position.saturating_add(window)) {
                            self.buffer_pool.prefetch_page(self.page_key(prefetch_id));
                        }
                    }
                    let pinned = self
                        .buffer_pool
                        .pin_page_with_intent(self.page_key(page_id), AccessIntent::Scan)?;
                    let summary = pinned.with_index_raw_bytes(|bytes| {
                        let header = parse_leaf_payload_header(bytes)?;
                        let (start, end, _) = raw_leaf_in_bounds_span(&header, low, high)?;
                        let matching = if filter_terms.is_empty() {
                            end.saturating_sub(start)
                        } else {
                            let mut matching = 0usize;
                            for slot_index in start..end {
                                let slot = header.read_slot(slot_index);
                                let mut cursor = slot.offset as usize;
                                let key = header.decode_key_at(&mut cursor)?;
                                if filter_terms.iter().all(|(position, expected)| {
                                    key.0.get(*position).is_some_and(|part| part == expected)
                                }) {
                                    matching = matching.saturating_add(1);
                                }
                            }
                            matching
                        };
                        Ok((start, end, matching, header.page_max_trx_id))
                    })?;
                    drop(pinned);
                    let (start, end, matching, page_max_trx_id) = summary;
                    row_count = row_count.checked_add(matching).ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_OUT_OF_RESOURCES,
                            "index entry count exceeds addressable memory",
                        )
                    })?;
                    leaves.push(RangeOrderLeafSpan {
                        page_id,
                        start,
                        end,
                        prefix_end: row_count,
                        page_max_trx_id,
                    });
                }
                if self.stable_generation() != Some(generation) {
                    ORDER_STATS_CACHE_INVALIDATIONS.fetch_add(1, Ordering::Relaxed);
                    return Ok(None);
                }
                let stats = RangeOrderStats {
                    leaves,
                    total: row_count,
                };
                let cache_bytes = std::mem::size_of::<CachedRangeOrderStats>()
                    .saturating_add(
                        stats
                            .leaves
                            .capacity()
                            .saturating_mul(std::mem::size_of::<RangeOrderLeafSpan>()),
                    )
                    .saturating_add(range_order_bound_capacity_bytes(cache_key.low.as_ref()))
                    .saturating_add(range_order_bound_capacity_bytes(cache_key.high.as_ref()))
                    .saturating_add(
                        cache_key
                            .filter_terms
                            .capacity()
                            .saturating_mul(std::mem::size_of::<(usize, KeyPart)>()),
                    )
                    .saturating_add(cache_key.filter_terms.iter().fold(
                        0usize,
                        |bytes, (_, part)| {
                            bytes.saturating_add(range_order_key_part_capacity_bytes(part))
                        },
                    ))
                    // Conservative hash-table control/bucket allowance for
                    // this bounded cache entry.
                    .saturating_add(256);
                if let Some(charge) =
                    crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                        crate::engine::executor::mem_budget::ControlledMemoryCategory::DerivedCache,
                        cache_bytes,
                    )
                {
                    let mut cache = self.order_stats_cache.lock();
                    if cache.len() >= 256 {
                        if let Some(oldest) = cache.keys().next().cloned() {
                            cache.remove(&oldest);
                        }
                    }
                    cache.insert(
                        cache_key,
                        Arc::new(CachedRangeOrderStats {
                            generation,
                            stats: stats.clone(),
                            _charge: charge,
                        }),
                    );
                }
                stats
            }
        };
        for leaf in &stats.leaves {
            if !accept_bound(leaf.page_max_trx_id) {
                return Ok(None);
            }
        }
        let row_count = stats.total;
        if row_count == 0 {
            return Ok(Some((None, 0)));
        }

        let ordinal = choose_ordinal(row_count);
        if ordinal >= row_count {
            return Err(error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "random index ordinal is outside the visible row count",
            ));
        }
        let leaf_index = stats
            .leaves
            .partition_point(|leaf| leaf.prefix_end <= ordinal);
        let leaf = stats.leaves.get(leaf_index).ok_or_else(|| {
            error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "random index ordinal has no cached leaf",
            )
        })?;
        let prefix_start = leaf_index
            .checked_sub(1)
            .and_then(|index| stats.leaves.get(index))
            .map_or(0, |leaf| leaf.prefix_end);
        let remaining = ordinal.saturating_sub(prefix_start);
        if self.stable_generation() != Some(generation) {
            ORDER_STATS_CACHE_INVALIDATIONS.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
        let pinned = self
            .buffer_pool
            .pin_page_with_intent(self.page_key(leaf.page_id), AccessIntent::Normal)?;
        let selected = pinned.with_index_raw_bytes(|bytes| {
            let header = parse_leaf_payload_header(bytes)?;
            let (start, end, _) = raw_leaf_in_bounds_span(&header, low, high)?;
            if start != leaf.start || end != leaf.end {
                return Ok(None);
            }
            let (slot, key, mut cursor) = if filter_terms.is_empty() {
                let slot = header.read_slot(start.saturating_add(remaining));
                let mut cursor = slot.offset as usize;
                let key = header.decode_key_at(&mut cursor)?;
                (slot, key, cursor)
            } else {
                let mut seen = 0usize;
                let mut found = None;
                for slot_index in start..end {
                    let slot = header.read_slot(slot_index);
                    let mut cursor = slot.offset as usize;
                    let key = header.decode_key_at(&mut cursor)?;
                    if !filter_terms.iter().all(|(position, expected)| {
                        key.0.get(*position).is_some_and(|part| part == expected)
                    }) {
                        continue;
                    }
                    if seen == remaining {
                        found = Some((slot, key, cursor));
                        break;
                    }
                    seen = seen.saturating_add(1);
                }
                found.ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        "cached filtered index ordinal was not found",
                    )
                })?
            };
            let record_start = slot.offset as usize;
            let mut record_end = record_start;
            record_end = record_end.checked_add(slot.len as usize).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt index: selected entry record overflow",
                )
            })?;
            if slot.len == 0 || record_end > header.slot_array_start {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt index: selected entry record bounds",
                ));
            }
            let row_id = decode_row_id_v7_from(header.payload, &mut cursor, header.page_table_id)?;
            if cursor != record_end {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt index: selected entry slot length mismatch",
                ));
            }
            Ok(Some((key, row_id)))
        })?;
        drop(pinned);
        let Some(selected) = selected else {
            ORDER_STATS_CACHE_INVALIDATIONS.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        };
        if self.stable_generation() != Some(generation) {
            ORDER_STATS_CACHE_INVALIDATIONS.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
        Ok(Some((Some(selected), row_count)))
    }

    /// Count an exact snapshot range using leaf headers for proven pages and
    /// an MVCC predicate only for inconclusive pages. Stabilize just the
    /// range's leaf-latch buckets so unrelated index writes cannot force a
    /// full scan, and readers never need to replay volatile evaluation.
    pub(crate) fn select_range_entry_by_mvcc_ordinal<F, V, S>(
        &self,
        low: Option<&IndexBound>,
        high: Option<&IndexBound>,
        filter_terms: &[(usize, KeyPart)],
        mut accept_bound: F,
        mut visible: V,
        choose_ordinal: S,
    ) -> Result<Option<(Option<(IndexKey, RowId)>, usize)>, QueryError>
    where
        F: FnMut(u64) -> bool,
        V: FnMut(&IndexKey, RowId) -> Result<bool, QueryError>,
        S: FnOnce(usize) -> usize,
    {
        let _structure = self.structure_lock.read_recursive();
        self.ensure_healthy()?;
        let mut next_page = Some(match low {
            Some(bound) => self.find_leaf_for_key_lower_bound(&bound.key)?,
            None => self.leftmost_leaf()?,
        });
        // Each exceptional page retains only slot ordinals, never decoded
        // keys or heap rows. All scratch is query-owned and optional: refusal
        // returns to the ordinary spill-capable TopN scan.
        let Ok(mut charge) = crate::engine::executor::mem_budget::ScopedCharge::new_strict(
            0,
            "snapshot index ordinal spans",
        ) else {
            return Ok(None);
        };
        // Topology is fixed by the shared structural guard. First collect
        // the range's latch buckets, then acquire them in the same global
        // order used by two-leaf writers. No latch is held while discovering
        // another, so opposite-direction index updates cannot deadlock us.
        let latch_count = self.page_latches.len();
        if charge
            .add(latch_count.saturating_mul(std::mem::size_of::<bool>()))
            .is_err()
        {
            return Ok(None);
        }
        let mut needed = vec![false; latch_count];
        let first_page = next_page;
        // This fallback may briefly exclude writers in the selected range.
        // Keep large analytical ranges on the streaming MVCC path rather
        // than holding a broad latch set across a long scan/RNG pass.
        const MAX_STABILIZED_LEAVES: usize = 128;
        const MAX_STABILIZED_ENTRIES: usize = 65_536;
        let mut leaf_count = 0usize;
        let mut candidate_count = 0usize;
        while let Some(page_id) = next_page {
            crate::engine::cancel::check_cancelled()?;
            leaf_count += 1;
            if leaf_count > MAX_STABILIZED_LEAVES {
                return Ok(None);
            }
            needed[self.page_latch_index(page_id)] = true;
            let pinned = self
                .buffer_pool
                .pin_page_with_intent(self.page_key(page_id), AccessIntent::Scan)?;
            next_page = pinned.with_index_raw_bytes(|bytes| {
                let header = parse_leaf_payload_header(bytes)?;
                let (start, end, terminal) = raw_leaf_in_bounds_span(&header, low, high)?;
                candidate_count = candidate_count.saturating_add(end - start);
                Ok(if terminal { None } else { header.next_leaf })
            })?;
            if candidate_count > MAX_STABILIZED_ENTRIES {
                return Ok(None);
            }
        }
        let required = needed.iter().filter(|needed| **needed).count();
        if charge
            .add(required.saturating_mul(std::mem::size_of::<MutexGuard<'_, ()>>()))
            .is_err()
        {
            return Ok(None);
        }
        let mut leaf_guards = Vec::with_capacity(required);
        for (bucket, needed) in needed.iter().copied().enumerate() {
            if needed {
                crate::engine::cancel::check_cancelled()?;
                leaf_guards.push(self.page_latches[bucket].lock().map_err(|_| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "index page latch poisoned",
                    )
                })?);
            }
        }
        next_page = first_page;
        let mut spans = Vec::new();
        let mut total = 0usize;
        while let Some(page_id) = next_page {
            crate::engine::cancel::check_cancelled()?;
            // A boundary key may have moved before its latch was acquired.
            // Never extend the second pass into a bucket we did not lock.
            if !needed[self.page_latch_index(page_id)] {
                return Ok(None);
            }
            let pinned = self
                .buffer_pool
                .pin_page_with_intent(self.page_key(page_id), AccessIntent::Scan)?;
            let result = pinned.with_index_raw_bytes(|bytes| {
                let header = parse_leaf_payload_header(bytes)?;
                let (start, end, terminal) = raw_leaf_in_bounds_span(&header, low, high)?;
                next_page = if terminal { None } else { header.next_leaf };
                let all_visible = accept_bound(header.page_max_trx_id);
                if all_visible && filter_terms.is_empty() {
                    return Ok(Some((start, end - start, Vec::new())));
                }
                let count = end - start;
                if charge
                    .add(count.saturating_mul(std::mem::size_of::<usize>()))
                    .is_err()
                {
                    return Ok(None);
                }
                let mut slots = Vec::with_capacity(count);
                for slot_index in start..end {
                    let slot = header.read_slot(slot_index);
                    let mut cursor = slot.offset as usize;
                    let key = header.decode_key_at(&mut cursor)?;
                    if !filter_terms.iter().all(|(position, expected)| {
                        key.0.get(*position).is_some_and(|part| part == expected)
                    }) {
                        continue;
                    }
                    let row_id =
                        decode_row_id_v7_from(header.payload, &mut cursor, header.page_table_id)?;
                    if all_visible || visible(&key, row_id)? {
                        slots.push(slot_index);
                    }
                }
                Ok(Some((start, slots.len(), slots)))
            })?;
            let Some((start, count, slots)) = result else {
                return Ok(None);
            };
            total = total.checked_add(count).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_OUT_OF_RESOURCES,
                    "index entry count exceeds addressable memory",
                )
            })?;
            if count > 0 {
                if spans.len() == spans.capacity() {
                    let extra = spans.capacity().max(16);
                    if charge
                        .add(extra.saturating_mul(std::mem::size_of::<(
                            u64,
                            usize,
                            usize,
                            Vec<usize>,
                        )>()))
                        .is_err()
                    {
                        return Ok(None);
                    }
                    spans.reserve_exact(extra);
                }
                spans.push((page_id, start, total, slots));
            }
        }
        if total == 0 {
            return Ok(Some((None, 0)));
        }
        let ordinal = choose_ordinal(total);
        if ordinal >= total {
            return Err(error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "random index ordinal is outside the visible row count",
            ));
        }
        let position = spans.partition_point(|span| span.2 <= ordinal);
        let (page_id, start, _, slots) = &spans[position];
        let offset = ordinal - position.checked_sub(1).map_or(0, |i| spans[i].2);
        let slot_index = if slots.is_empty() {
            start + offset
        } else {
            slots[offset]
        };
        let pinned = self
            .buffer_pool
            .pin_page_with_intent(self.page_key(*page_id), AccessIntent::Normal)?;
        let selected = pinned.with_index_raw_bytes(|bytes| {
            let header = parse_leaf_payload_header(bytes)?;
            let slot = header.read_slot(slot_index);
            let mut cursor = slot.offset as usize;
            let key = header.decode_key_at(&mut cursor)?;
            let row_id = decode_row_id_v7_from(header.payload, &mut cursor, header.page_table_id)?;
            Ok(Some((key, row_id)))
        })?;
        Ok(selected.map(|entry| (Some(entry), total)))
    }

    fn page_key(&self, page_id: u64) -> PageKey {
        PageKey {
            file: self.file_key.clone(),
            page_id,
        }
    }

    /// Page-id sequential prefetch for `page_lo..page_hi` linear scans
    /// (covering aggregate). Unlike `prefetch_next_leaves` this does
    /// NOT follow the `next_leaf` chain — it walks by file offset —
    /// so it works on the first cold pass without a seed cache page.
    /// Cheap (each call queues `effective_prefetch_window` pages and
    /// returns); intended to be called once per page-range chunk.
    pub(crate) fn prefetch_page_range(&self, start: u64, end_exclusive: u64) {
        let depth = self.buffer_pool.effective_prefetch_window() as u64;
        if depth == 0 || start >= end_exclusive {
            return;
        }
        let upper = end_exclusive.min(start.saturating_add(depth));
        for page_id in start..upper {
            self.buffer_pool.prefetch_page(self.page_key(page_id));
        }
    }

    /// Best-effort sibling-leaf prefetch for sequential scans. Mirrors
    /// `HeapFile::prefetch_after` but follows the `next_leaf` chain
    /// instead of consecutive page IDs (index pages are scattered as
    /// the tree grows, so contiguous range prefetch is meaningless
    /// here). Walks the chain via the buffer-pool's already-cached
    /// pages — when a probe misses, we stop, because we don't have a
    /// pinned version of that page from which to follow `next_leaf`
    /// any further. The async I/O threads pick up each queued page,
    /// and on the next call to this helper (one leaf later) the chain
    /// extends a step deeper, so steady-state we keep
    /// `prefetch_pages` leaves in flight ahead of the cursor.
    fn prefetch_next_leaves(&self, start: u64) {
        self.prefetch_leaves_while(start, |_| Ok(true));
    }

    fn fulltext_prefetch_may_continue(
        last: Option<&IndexKey>,
        prefix: &str,
        upper: Option<&str>,
        exact: bool,
    ) -> bool {
        let Some(KeyPart::String(token)) = last.and_then(|key| key.0.first()) else {
            // Empty or unfamiliar layouts retain conservative read-ahead.
            return true;
        };
        if exact {
            token.as_str() <= prefix
        } else {
            token.as_str() < prefix
                || (token.starts_with(prefix) && upper.is_none_or(|upper| token.as_str() < upper))
        }
    }

    fn prefetch_fulltext_leaves(&self, start: u64, prefix: &str, upper: Option<&str>, exact: bool) {
        self.prefetch_leaves_while(start, |header| {
            if header.count == 0 {
                return Ok(true);
            }
            let mut cursor = header.read_slot_offset(header.count - 1);
            let last = header.decode_key_at(&mut cursor)?;
            Ok(Self::fulltext_prefetch_may_continue(
                Some(&last),
                prefix,
                upper,
                exact,
            ))
        });
    }

    fn prefetch_leaves_while(
        &self,
        mut start: u64,
        mut continues: impl FnMut(&LeafPayloadHeader<'_>) -> Result<bool, QueryError>,
    ) {
        let depth = self.buffer_pool.effective_prefetch_window();
        for _ in 0..depth {
            let key = self.page_key(start);
            // One resident lookup, with no demand-hit promotion. Queue only a
            // missing/loading page, then let asynchronous I/O extend the chain.
            match self.buffer_pool.with_cached_prefetch_bytes(&key, |bytes| {
                let header = parse_leaf_payload_header(bytes)?;
                Ok(if continues(&header)? {
                    header.next_leaf
                } else {
                    None
                })
            }) {
                Ok(Some(Some(next))) => start = next,
                Ok(None) => {
                    self.buffer_pool.prefetch_page(key);
                    return;
                }
                _ => return,
            }
        }
    }

    fn write_page(&self, txn_id: u64, page_id: u64, page: IndexPage) -> Result<(), QueryError> {
        self.ensure_healthy()?;
        #[cfg(test)]
        if INDEX_FAIL_WRITE_AFTER.with(|remaining| match remaining.get() {
            Some(0) => {
                if !INDEX_FAIL_WRITE_PERSIST.with(|persistent| persistent.get()) {
                    remaining.set(None);
                }
                true
            }
            Some(n) => {
                remaining.set(Some(n - 1));
                false
            }
            None => false,
        }) {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "injected index write failure",
            ));
        }
        self.mutation_generation.fetch_add(1, Ordering::AcqRel);
        // Phase 1b: stamp the current writer's txn id onto the leaf
        // page's `page_max_trx_id` field monotonically (max-only).
        // This is the single choke point for every insert / update /
        // delete / split / merge / borrow / rebalance path. Internal
        // and Free pages have no MVCC-reader semantic and skip the
        // stamp (they don't carry the field at all — the leaf
        // payload header owns it exclusively, see
        // `LEAF_OFF_PAGE_MAX_TRX_ID`).
        //
        // The choice of "max only" preserves any historical bound
        // that a split/merge/rebalance preserved on the reused leaf
        // (e.g. source page had page_max_trx_id=300 from a prior
        // writer that raced ahead of the local txn's id assignment).
        let mut page = page;
        if let IndexPage::Leaf(ref mut leaf) = page {
            leaf.raise_page_max_trx_id(txn_id);
        }
        let oracle = Arc::new(page);
        let key = self.page_key(page_id);
        // Physical-replication apply: NEVER append to the local WAL —
        // it mirrors the master's LSN sequence (see PHYSICAL_WAL_APPLY).
        // Stamp pages with the logical apply LSN so later master page
        // images (PageUpdate) still win the page-LSN gate.
        let physical_apply = physical_wal_apply_active()
            || (self.log_atomics.physical_replica() && !crate::wal::promotion_wal_active());
        if physical_apply {
            let lsn = btree_logical_wal::current()
                .unwrap_or_else(|| self.log_atomics.next_lsn().saturating_sub(1));
            self.store_index_page_transient(key, lsn, oracle)?;
            self.dirty.store(true, Ordering::Release);
            return Ok(());
        }
        if btree_logical_wal::is_deferred() {
            // The buffer pool installs deferred pins under its image/shard
            // latch, including when this page must first become resident.
            let peek_lsn = self.log_atomics.next_lsn().max(1);
            self.store_index_page_transient(key, peek_lsn, oracle)?;
            self.dirty.store(true, Ordering::Release);
            return Ok(());
        }
        if btree_logical_wal::current().is_some() {
            // Index changes are durable through compact logical IndexBatch
            // records. The physical index page image is a rebuildable cache
            // over heap/catalog state, so strict mode does not need a 32 KB
            // page image for the first clean-page modification after every
            // checkpoint. If a crash tears the cached index page, the page
            // checksum/tree validator catches it and startup rebuilds the
            // affected index from the heap.
            let peek_lsn = self.log_atomics.next_lsn().max(1);
            self.store_index_page_transient(key, peek_lsn, oracle)?;
            self.dirty.store(true, Ordering::Release);
            return Ok(());
        }
        // FPI / non-WAL fallback — we owe a real byte image right
        // now (either to land in the WAL or to backfill the buffer
        // pool from scratch). Encode eagerly and walk the original
        // path below.
        // Note: `encode_index_tree_page` itself runs the layout
        // safety net (`demote_page_if_unfit`), so we don't repeat it
        // here.
        let mut after = encode_index_tree_page(&oracle).map_err(|e| {
            if std::env::var("MYDB_DEBUG_INDEX_OVERFLOW").is_ok() {
                let summary = match oracle.as_ref() {
                    IndexPage::Leaf(leaf) => format!(
                        "leaf entries={} key_layout={} base_time={} aux={:?} est_size={} path={:?}",
                        leaf.entries.len(),
                        leaf.key_layout,
                        leaf.base_time,
                        leaf.layout_aux,
                        encoded_leaf_page_len(leaf),
                        self.relative_path(),
                    ),
                    IndexPage::Internal(int) => format!(
                        "internal keys={} children={} key_layout={} base_time={} aux={:?} est_size={} path={:?}",
                        int.keys.len(),
                        int.children.len(),
                        int.key_layout,
                        int.base_time,
                        int.layout_aux,
                        encoded_internal_page_len(int),
                        self.relative_path(),
                    ),
                    IndexPage::Free(_) => "free page".to_string(),
                };
                eprintln!("[index-overflow] page_id={} {}", page_id, summary);
            }
            e
        })?;
        stamp_page_owner(&mut after, &self.file_key.owner, page_id);
        refresh_page_checksum(&mut after);
        let lsn = if self.durability == DurabilityMode::Memory {
            0
        } else {
            let before = self.read_raw_page_image(page_id)?;
            let mut log = self.log.lock();
            let lsn = log.reserve_txn_lsn(txn_id)?;
            set_page_lsn_if_formatted(&mut after, lsn);
            let path = self.relative_path();
            let reduced = txn_reduced_redo_active(&self.log, txn_id)
                || (txn_id == txn_id_for_ddl() && !current_redo_enabled());
            let payload = if reduced {
                WalPayload::PageUpdateCompressed {
                    path,
                    page_id,
                    compressed_before: lz4_flex::compress_prepend_size(&before),
                    compressed_after: lz4_flex::compress_prepend_size(&after),
                    undo_lsn: None,
                }
            } else {
                WalPayload::PageUpdate {
                    path,
                    page_id,
                    before,
                    after: after.clone(),
                    undo_lsn: None,
                }
            };
            log.append_reserved(lsn, txn_id, payload)?;
            lsn
        };
        self.buffer_pool.store_page(key, after, lsn)?;
        self.dirty.store(true, Ordering::Release);
        Ok(())
    }

    /// Publish a page-wide operation-local oracle as a raw image.
    fn store_index_page_transient(
        &self,
        key: PageKey,
        lsn: u64,
        oracle: Arc<IndexPage>,
    ) -> Result<(), QueryError> {
        self.buffer_pool
            .store_index_page_transient(key, lsn, oracle)
    }

    fn write_bulk_leaf_direct(
        &self,
        page_id: u64,
        mut page: IndexPage,
        lsn: u64,
        txn_id: u64,
    ) -> Result<(), QueryError> {
        if let IndexPage::Leaf(leaf) = &mut page {
            leaf.raise_page_max_trx_id(txn_id);
        }
        self.write_page_direct(page_id, page, lsn)
    }

    fn write_page_direct(&self, page_id: u64, page: IndexPage, lsn: u64) -> Result<(), QueryError> {
        let mut image = encode_index_tree_page(&page)?;
        stamp_page_owner(&mut image, &self.file_key.owner, page_id);
        refresh_page_checksum(&mut image);
        if lsn != 0 {
            set_page_lsn_if_formatted(&mut image, lsn);
        }
        let mut file_guard = self.file.lock();
        let file = file_guard.as_mut().ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "index file should be open for bulk load",
            )
        })?;
        write_index_page(file, page_id, &image)
    }

    fn write_meta_page(&self, txn_id: u64) -> Result<(), QueryError> {
        let mut state = self.meta_state.lock();
        let mut after = encode_index_meta(&state.meta);
        stamp_page_owner(&mut after, &self.file_key.owner, 0);
        refresh_page_checksum(&mut after);
        // Physical-replication apply: no local WAL appends (see
        // PHYSICAL_WAL_APPLY); the master's own meta-page PageUpdate
        // records arrive in-stream and converge the on-disk image.
        if physical_wal_apply_active()
            || (self.log_atomics.physical_replica() && !crate::wal::promotion_wal_active())
        {
            let lsn = btree_logical_wal::current()
                .unwrap_or_else(|| self.log_atomics.next_lsn().saturating_sub(1));
            if lsn != 0 {
                set_page_lsn_if_formatted(&mut after, lsn);
            }
            state.meta_image = after.clone();
            self.buffer_pool
                .store_page(self.page_key(INDEX_META_PAGE), after, lsn)?;
            self.dirty_meta.store(true, Ordering::Release);
            self.dirty.store(true, Ordering::Release);
            return Ok(());
        }
        let lsn = if btree_logical_wal::current().is_some() {
            let peek_lsn = self.log_atomics.next_lsn().max(1);
            set_page_lsn_if_formatted(&mut after, peek_lsn);
            peek_lsn
        } else if self.durability == DurabilityMode::Memory {
            0
        } else {
            let before = state.meta_image.clone();
            let mut log = self.log.lock();
            let lsn = log.reserve_txn_lsn(txn_id)?;
            set_page_lsn_if_formatted(&mut after, lsn);
            let path = self.relative_path();
            let reduced = txn_reduced_redo_active(&self.log, txn_id)
                || (txn_id == txn_id_for_ddl() && !current_redo_enabled());
            let payload = if reduced {
                WalPayload::PageUpdateCompressed {
                    path,
                    page_id: INDEX_META_PAGE,
                    compressed_before: lz4_flex::compress_prepend_size(&before),
                    compressed_after: lz4_flex::compress_prepend_size(&after),
                    undo_lsn: None,
                }
            } else {
                WalPayload::PageUpdate {
                    path,
                    page_id: INDEX_META_PAGE,
                    before,
                    after: after.clone(),
                    undo_lsn: None,
                }
            };
            log.append_reserved(lsn, txn_id, payload)?;
            lsn
        };
        state.meta_image = after.clone();
        self.buffer_pool
            .store_page(self.page_key(INDEX_META_PAGE), after, lsn)?;
        self.dirty_meta.store(true, Ordering::Release);
        self.dirty.store(true, Ordering::Release);
        Ok(())
    }

    fn write_or_defer_meta_page(&self, txn_id: u64) -> Result<(), QueryError> {
        if btree_logical_wal::current().is_some() {
            return self.mark_meta_dirty();
        }
        self.write_meta_page(txn_id)
    }

    fn mark_meta_dirty(&self) -> Result<(), QueryError> {
        let mut image = encode_index_meta(&self.meta_state.lock().meta);
        stamp_page_owner(&mut image, &self.file_key.owner, 0);
        refresh_page_checksum(&mut image);
        let lsn = btree_logical_wal::current().unwrap_or(0);
        if lsn != 0 {
            set_page_lsn_if_formatted(&mut image, lsn);
        }
        self.meta_state.lock().meta_image = image.clone();
        self.buffer_pool
            .store_page(self.page_key(INDEX_META_PAGE), image, lsn)?;
        self.dirty_meta.store(true, Ordering::Release);
        self.dirty.store(true, Ordering::Release);
        Ok(())
    }

    fn write_meta_page_direct(&self, lsn: u64) -> Result<(), QueryError> {
        let mut image = encode_index_meta(&self.meta_state.lock().meta);
        stamp_page_owner(&mut image, &self.file_key.owner, 0);
        refresh_page_checksum(&mut image);
        if lsn != 0 {
            set_page_lsn_if_formatted(&mut image, lsn);
        }
        decode_index_meta(&image)?;
        let mut file_guard = self.file.lock();
        let file = file_guard.as_mut().ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "index file should be open for bulk load",
            )
        })?;
        write_index_page(file, INDEX_META_PAGE, &image)?;
        self.buffer_pool
            .store_page(self.page_key(INDEX_META_PAGE), image.clone(), lsn)?;
        if let Some(frame) = self
            .buffer_pool
            .lookup_frame(&self.page_key(INDEX_META_PAGE))?
        {
            if frame.dirty.swap(false, Ordering::AcqRel) {
                self.buffer_pool.dirty_pages.fetch_sub(1, Ordering::Relaxed);
            }
            frame.dirty_lsn.store(0, Ordering::Release);
        }
        self.meta_state.lock().meta_image = image;
        Ok(())
    }

    fn read_raw_page_image(&self, page_id: u64) -> Result<Vec<u8>, QueryError> {
        if page_id == INDEX_META_PAGE {
            return Ok(self.meta_state.lock().meta_image.clone());
        }
        if let Some(image) = self.buffer_pool.get_cached_bytes(&self.page_key(page_id))? {
            return Ok(image);
        }
        let mut file_guard = self.file.lock();
        let Some(file) = file_guard.as_mut() else {
            return Ok(vec![0; PAGE_SIZE]);
        };
        let offset = page_id * PAGE_SIZE as u64;
        if file.metadata().map_err(io_err)?.len() < offset + PAGE_SIZE as u64 {
            return Ok(vec![0; PAGE_SIZE]);
        }
        read_index_page(file, page_id)
    }

    fn relative_path(&self) -> String {
        self.rel_path.clone()
    }

    /// Public accessor for the live `next_page_id` watermark. Used by
    /// `Database::index_page_count` and the range-scan upper-bound
    /// clamp; both run under the outer `with_index_read` lock and only
    /// need a momentary snapshot of the cold-state mutex.
    pub(crate) fn next_page_id(&self) -> u64 {
        self.meta_state.lock().meta.next_page_id
    }

    fn compute_relative_path(path: &Path) -> String {
        crate::storage::naming::wal_relative_object_path(path)
    }
}

/// Resolve the true post-append leaf layout and return any overfilled tail to
/// the pending batch. Kept separate from I/O so the compact-layout widening
/// boundary has a fast, exact unit regression.
fn resolve_appended_leaf_layout(leaf: &mut IndexLeafPage, pos: &mut usize, start_pos: usize) {
    let (layout, base_time, layout_aux) =
        resolve_entry_layout_full(KEY_LAYOUT_GENERIC, 0, None, &leaf.entries);
    leaf.key_layout = layout;
    leaf.base_time = base_time;
    leaf.layout_aux = layout_aux;
    leaf.recompute_cached_size();

    // If the honest, resolved size overflows, the append widened the layout
    // past what the estimate predicted. Pop the newly-appended tail back into
    // the pending batch (`pos` rewinds), re-resolving until the leaf fits.
    // Never pop below `start_pos`: the pre-existing on-disk leaf was valid.
    while *pos > start_pos && encoded_leaf_page_len(leaf) > PAGE_SIZE {
        leaf.entries.pop();
        *pos -= 1;
        let (layout, base_time, layout_aux) =
            resolve_entry_layout_full(KEY_LAYOUT_GENERIC, 0, None, &leaf.entries);
        leaf.key_layout = layout;
        leaf.base_time = base_time;
        leaf.layout_aux = layout_aux;
        leaf.recompute_cached_size();
    }
}

fn btree_two_leaf_in_place_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("MYDB_BTREE_TWO_LEAF_IN_PLACE")
            .map(|value| !matches!(value.trim(), "0" | "off" | "false" | "no"))
            .unwrap_or(true)
    })
}

impl Drop for IndexFile {
    fn drop(&mut self) {
        if self.flush_on_drop {
            if let Err(err) = self.flush() {
                eprintln!(
                    "index flush failed for {}: {}",
                    self.path.display(),
                    err.message()
                );
            }
        }
    }
}

pub(super) fn index_key_in_bounds(
    key: &IndexKey,
    low: Option<&IndexBound>,
    high: Option<&IndexBound>,
) -> bool {
    if let Some(low) = low {
        if index_key_before_low_bound(key, low) {
            return false;
        }
    }
    if let Some(high) = high {
        if index_key_past_high_bound(key, Some(high)) {
            return false;
        }
    }
    true
}

fn push_row_id(rows: &mut Vec<RowId>, seen: &mut Option<BtreeDedupSet>, row_id: RowId) {
    if seen.as_mut().is_none_or(|seen| seen.insert(row_id)) {
        rows.push(row_id);
    }
}

fn push_index_entry(
    entries: &mut Vec<(IndexKey, RowId)>,
    seen: &mut Option<BtreeDedupSet>,
    entry: &IndexEntry,
) {
    if seen.as_mut().is_none_or(|seen| seen.insert(entry.row_id)) {
        entries.push((entry.key.clone(), entry.row_id));
    }
}

fn push_index_entry_tuple(
    entries: &mut Vec<(IndexKey, RowId)>,
    seen: &mut Option<BtreeDedupSet>,
    key: IndexKey,
    row_id: RowId,
) {
    if seen.as_mut().is_none_or(|seen| seen.insert(row_id)) {
        entries.push((key, row_id));
    }
}

fn compare_index_entry_to_lookup_key(
    entry_key: &IndexKey,
    lookup_key: &IndexKey,
    exact: bool,
) -> std::cmp::Ordering {
    if exact {
        return entry_key.cmp(lookup_key);
    }
    let prefix_len = lookup_key.0.len();
    entry_key.0[..prefix_len.min(entry_key.0.len())].cmp(&lookup_key.0[..])
}

fn min_lookup_row_id() -> RowId {
    RowId {
        page_id: 0,
        slot_id: 0,
    }
}

fn merge_adjacent_leaf_key_ranges(ranges: &mut Vec<LeafKeyRange>) {
    if ranges.len() <= 1 {
        return;
    }
    let mut write = 0usize;
    for read in 1..ranges.len() {
        if ranges[write].leaf_id == ranges[read].leaf_id && ranges[write].end == ranges[read].start
        {
            ranges[write].end = ranges[read].end;
        } else {
            write += 1;
            if write != read {
                ranges[write] = ranges[read].clone();
            }
        }
    }
    ranges.truncate(write + 1);
}

/// Compute the in-bounds entry span `[start, end)` of a sorted leaf
/// slice with at most two boundary probes (`first` / `last`); only
/// leaves that straddle a bound pay a bisect. `terminal` reports
/// whether the high bound cuts inside this leaf — in key-ordered
/// walks every later leaf is fully out of range.
fn leaf_in_bounds_span(
    entries: &[IndexEntry],
    low: Option<&IndexBound>,
    high: Option<&IndexBound>,
) -> (usize, usize, bool) {
    let start = match low {
        Some(bound)
            if entries
                .first()
                .is_some_and(|e| index_key_before_low_bound(&e.key, bound)) =>
        {
            entries.partition_point(|e| index_key_before_low_bound(&e.key, bound))
        }
        _ => 0,
    };
    let (end, terminal) = if entries
        .last()
        .is_some_and(|e| index_key_past_high_bound(&e.key, high))
    {
        (
            entries.partition_point(|e| !index_key_past_high_bound(&e.key, high)),
            true,
        )
    } else {
        (entries.len(), false)
    };
    (start, end.max(start), terminal)
}

fn index_key_before_low_bound(key: &IndexKey, low: &IndexBound) -> bool {
    if low.key.0.len() < key.0.len() && index_key_starts_with(key, &low.key) {
        return !low.inclusive;
    }
    let ordering = key.cmp(&low.key);
    ordering == std::cmp::Ordering::Less
        || (ordering == std::cmp::Ordering::Equal && !low.inclusive)
}

fn index_key_past_high_bound(key: &IndexKey, high: Option<&IndexBound>) -> bool {
    let Some(high) = high else {
        return false;
    };
    if high.key.0.len() < key.0.len() && index_key_starts_with(key, &high.key) {
        return !high.inclusive;
    }
    let ordering = key.cmp(&high.key);
    ordering == std::cmp::Ordering::Greater
        || (ordering == std::cmp::Ordering::Equal && !high.inclusive)
}

fn encoded_index_entry_len(entry: &IndexEntry) -> usize {
    // v10: 1B u8 part_count + key parts + RowId (varint page_id + u16
    // slot + u16 gen). `page_table_id` lives once in the page header,
    // not per entry.
    1 + entry.key.0.iter().map(encoded_key_part_len).sum::<usize>()
        + row_id_v7_encoded_len(entry.row_id)
}

fn date_index_entry(days: i32, row_id: RowId) -> IndexEntry {
    IndexEntry {
        key: IndexKey::from(vec![KeyPart::Date(days)]),
        row_id,
    }
}

/// Convert a single signed-INT key plus its `RowId` into the generic
/// `IndexEntry` shape. Used by the narrow-int specialization to feed
/// the existing `bulk_load_leaf_level_direct_stream` path lazily,
/// one entry at a time, so peak `Vec<KeyPart>` allocations stay
/// bounded to one leaf page worth (~ thousands).
fn single_int_index_entry(value: i64, row_id: RowId) -> IndexEntry {
    IndexEntry {
        key: IndexKey::from(vec![KeyPart::Int(value)]),
        row_id,
    }
}

fn pair_int_index_entry(v1: i64, v2: i64, row_id: RowId) -> IndexEntry {
    IndexEntry {
        key: IndexKey::from(vec![KeyPart::Int(v1), KeyPart::Int(v2)]),
        row_id,
    }
}

fn encoded_leaf_page_len(page: &IndexLeafPage) -> usize {
    // Fast path: every constructor primes `cached_size` and every
    // insert/delete/demote site keeps it in sync (see the
    // `bump_size_*` / `recompute_cached_size` helpers). Drops a
    // per-call O(N) walk over `page.entries`, which sysbench
    // oltp_update_index profiling identified as the #1 hot function
    // (~2200 samples / 25 s / 12 threads — every UPDATE was re-summing
    // 100+ entries × 2-3 size checks per index op).
    //
    // The pre-cache code also handled the "page claims compact layout
    // but at least one entry no longer fits" edge case by computing
    // against the resolved-down layout on the fly. That branch is no
    // longer needed because every insert routes through
    // `demote_leaf_layout_if_needed` (or a full layout re-resolve via
    // `new_with_resolved_layout`) BEFORE the size check, so by the
    // time we get here the page's claimed layout is the effective one.
    // `debug_assert_eq!` below catches any future mutation site that
    // forgets to refresh the cache.
    debug_assert_eq!(
        page.cached_size,
        IndexLeafPage::compute_encoded_size(&page.entries, page.key_layout, page.layout_aux),
        "encoded_leaf_page_len cache out of sync; some entries mutation forgot to bump cached_size",
    );
    page.cached_size
}

/// Re-validate a leaf page's requested `key_layout` against its
/// current entries. A compact layout is only usable when every entry
/// still encodes under the page `base_time`; an `insert` that landed
/// a value outside the `[base, base + max_delta]` window forces a
/// demote. Returns the *effective* `(layout, base_time)` to commit —
/// either the requested compact layout (unchanged), a freshly
/// re-picked compact layout, or the generic layout as a safe
/// fallback. Pure function: callers (`encode_index_tree_page`,
/// `encoded_leaf_page_len`) stay consistent by both routing through it.
/// True when `page` claims a compact layout but at least one entry
/// no longer fits — i.e. the safety net needs to re-resolve. Cheap:
/// generic pages return false instantly without iterating.
fn needs_layout_demote(page: &IndexPage) -> bool {
    match page {
        IndexPage::Leaf(leaf) => leaf.key_layout != KEY_LAYOUT_GENERIC && !leaf_fits_layout(leaf),
        IndexPage::Internal(int) => {
            int.key_layout != KEY_LAYOUT_GENERIC && !internal_fits_layout(int)
        }
        IndexPage::Free(_) => false,
    }
}

/// Re-resolve a page's layout from scratch, materializing a fresh
/// `IndexPage` (clones the entries Vec). Only called when
/// `needs_layout_demote` returns true.
fn demote_page_if_unfit_owned(page: &IndexPage) -> IndexPage {
    match page {
        IndexPage::Leaf(leaf) => {
            let (key_layout, base_time, layout_aux) =
                resolve_entry_layout_full(KEY_LAYOUT_GENERIC, 0, None, &leaf.entries);
            let entries = leaf.entries.clone();
            let cached_size = IndexLeafPage::compute_encoded_size(&entries, key_layout, layout_aux);
            IndexPage::Leaf(IndexLeafPage {
                entries,
                next_leaf: leaf.next_leaf,
                key_layout,
                base_time,
                layout_aux,
                cached_size,
                // Layout demote never changes visibility semantics: it
                // just re-encodes the same entries under a different
                // key layout. Preserve the source page's writer bound.
                // See `LEAF_OFF_PAGE_MAX_TRX_ID` docs.
                page_max_trx_id: leaf.page_max_trx_id,
            })
        }
        IndexPage::Internal(int) => {
            let (key_layout, base_time, layout_aux) =
                resolve_entry_layout_full(KEY_LAYOUT_GENERIC, 0, None, &int.keys);
            IndexPage::Internal(IndexInternalPage {
                keys: int.keys.clone(),
                children: int.children.clone(),
                key_layout,
                base_time,
                layout_aux,
            })
        }
        IndexPage::Free(free) => IndexPage::Free(free.clone()),
    }
}

fn leaf_fits_layout(leaf: &IndexLeafPage) -> bool {
    leaf.entries
        .iter()
        .all(|e| layout_admits(leaf.key_layout, leaf.base_time, leaf.layout_aux, &e.key))
}

fn internal_fits_layout(int: &IndexInternalPage) -> bool {
    int.keys
        .iter()
        .all(|e| layout_admits(int.key_layout, int.base_time, int.layout_aux, &e.key))
}

fn layout_admits(layout: u8, base: i64, aux: Option<LayoutAux>, key: &IndexKey) -> bool {
    match layout {
        KEY_LAYOUT_GENERIC => true,
        KEY_LAYOUT_COMPACT_TIME_U32 => compact_time_delta(key, base).is_ok(),
        KEY_LAYOUT_COMPACT_DATE_U16 => compact_date_delta(key, base).is_ok(),
        KEY_LAYOUT_COMPACT_DECIMAL_I32 => compact_decimal_magnitude_i32(key, base).is_ok(),
        KEY_LAYOUT_COMPACT_DECIMAL_I64 => compact_decimal_magnitude_i64(key, base).is_ok(),
        KEY_LAYOUT_COMPACT_INT_U16 => compact_int_delta_u16(key, base).is_ok(),
        KEY_LAYOUT_COMPACT_INT_U32 => compact_int_delta_u32(key, base).is_ok(),
        KEY_LAYOUT_COMPACT_UINT_U16 => compact_uint_delta_u16(key, base).is_ok(),
        KEY_LAYOUT_COMPACT_UINT_U32 => compact_uint_delta_u32(key, base).is_ok(),
        KEY_LAYOUT_COMPACT_INT2 => match aux {
            Some(a) => int2_admits(key, base, a.col2_base, a.width1, a.width2),
            None => false,
        },
        _ => false,
    }
}

#[cfg(test)]
/// Backwards-compatible facade: returns only the `(layout_byte, base)`
/// pair. Used by callsites that don't carry/propagate `LayoutAux`.
fn resolve_leaf_layout(requested: u8, base: i64, entries: &[IndexEntry]) -> (u8, i64) {
    let (layout, base, _aux) = resolve_entry_layout_full(requested, base, None, entries);
    (layout, base)
}

/// Resolve the layout *and* `LayoutAux` for a leaf's entry set.
/// `requested_aux` lets callers pass through the existing INT2 aux
/// so the fast-path fits-check can use the right widths/col2_base.
fn resolve_entry_layout_full(
    requested: u8,
    base: i64,
    requested_aux: Option<LayoutAux>,
    entries: &[IndexEntry],
) -> (u8, i64, Option<LayoutAux>) {
    if entries.is_empty() {
        return (KEY_LAYOUT_GENERIC, 0, None);
    }
    if requested != KEY_LAYOUT_GENERIC {
        let fits = entries.iter().all(|entry| match requested {
            KEY_LAYOUT_COMPACT_TIME_U32 => compact_time_delta(&entry.key, base).is_ok(),
            KEY_LAYOUT_COMPACT_DATE_U16 => compact_date_delta(&entry.key, base).is_ok(),
            KEY_LAYOUT_COMPACT_DECIMAL_I32 => {
                compact_decimal_magnitude_i32(&entry.key, base).is_ok()
            }
            KEY_LAYOUT_COMPACT_DECIMAL_I64 => {
                compact_decimal_magnitude_i64(&entry.key, base).is_ok()
            }
            KEY_LAYOUT_COMPACT_INT_U16 => compact_int_delta_u16(&entry.key, base).is_ok(),
            KEY_LAYOUT_COMPACT_INT_U32 => compact_int_delta_u32(&entry.key, base).is_ok(),
            KEY_LAYOUT_COMPACT_UINT_U16 => compact_uint_delta_u16(&entry.key, base).is_ok(),
            KEY_LAYOUT_COMPACT_UINT_U32 => compact_uint_delta_u32(&entry.key, base).is_ok(),
            KEY_LAYOUT_COMPACT_INT2 => match requested_aux {
                Some(aux) => int2_admits(&entry.key, base, aux.col2_base, aux.width1, aux.width2),
                None => false,
            },
            _ => false,
        });
        if fits {
            return (requested, base, requested_aux);
        }
    }
    let mut accum = CompactChunkAccum::new();
    for entry in entries {
        accum.record(entry);
    }
    accum.layout_full()
}

fn encoded_internal_page_len(page: &IndexInternalPage) -> usize {
    // Mirror `encoded_leaf_page_len`: short-circuit when the cached
    // layout still fits, fall back to full resolve only when it
    // doesn't. Keeps the size estimate consistent with what
    // `encode_index_tree_page` actually writes.
    let (effective_layout, effective_aux) =
        if page.key_layout == KEY_LAYOUT_GENERIC || internal_fits_layout(page) {
            (page.key_layout, page.layout_aux)
        } else {
            let (l, _b, a) = resolve_entry_layout_full(KEY_LAYOUT_GENERIC, 0, None, &page.keys);
            (l, a)
        };
    let mut prefix_bytes = 0usize;
    let per_entry_bytes: usize = match effective_layout {
        KEY_LAYOUT_COMPACT_TIME_U32 => page
            .keys
            .iter()
            .map(|e| compact_time_entry_bytes(e.row_id))
            .sum(),
        KEY_LAYOUT_COMPACT_DATE_U16 => page
            .keys
            .iter()
            .map(|e| compact_date_entry_bytes(e.row_id))
            .sum(),
        KEY_LAYOUT_COMPACT_DECIMAL_I32 => page
            .keys
            .iter()
            .map(|e| compact_decimal_i32_entry_bytes(e.row_id))
            .sum(),
        KEY_LAYOUT_COMPACT_DECIMAL_I64 => page
            .keys
            .iter()
            .map(|e| compact_decimal_i64_entry_bytes(e.row_id))
            .sum(),
        KEY_LAYOUT_COMPACT_INT_U16 => page
            .keys
            .iter()
            .map(|e| compact_int_u16_entry_bytes(e.row_id))
            .sum(),
        KEY_LAYOUT_COMPACT_INT_U32 => page
            .keys
            .iter()
            .map(|e| compact_int_u32_entry_bytes(e.row_id))
            .sum(),
        KEY_LAYOUT_COMPACT_UINT_U16 => page
            .keys
            .iter()
            .map(|e| compact_uint_u16_entry_bytes(e.row_id))
            .sum(),
        KEY_LAYOUT_COMPACT_UINT_U32 => page
            .keys
            .iter()
            .map(|e| compact_uint_u32_entry_bytes(e.row_id))
            .sum(),
        KEY_LAYOUT_COMPACT_INT2 => {
            prefix_bytes = COMPACT_INT2_PREFIX_BYTES;
            let (w1, w2) = match effective_aux {
                Some(a) => (a.width1, a.width2),
                None => (4, 4),
            };
            page.keys
                .iter()
                .map(|e| compact_int2_entry_bytes(w1, w2, e.row_id))
                .sum()
        }
        _ => page.keys.iter().map(encoded_index_entry_len).sum(),
    };
    INTERNAL_HEADER_BYTES
        + prefix_bytes
        + per_entry_bytes
        + page.keys.len() * 8
        + page.keys.len() * SLOT_OFFSET_BYTES
}

fn build_bulk_internal_page(
    chunk: &[(u64, IndexEntry)],
) -> Result<(IndexEntry, IndexPage), QueryError> {
    let first = chunk
        .first()
        .map(|(_, first)| first.clone())
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "bulk index internal page is empty",
            )
        })?;
    let children = chunk.iter().map(|(child_id, _)| *child_id).collect();
    let keys = chunk
        .iter()
        .skip(1)
        .map(|(_, first)| first.clone())
        .collect();
    Ok((
        first,
        IndexPage::Internal(IndexInternalPage::new_generic(keys, children)),
    ))
}

fn split_entries_at_pct(entries: &[IndexEntry], pct: u8) -> usize {
    if entries.len() <= 1 {
        return 1;
    }
    let total = entries.iter().map(encoded_index_entry_len).sum::<usize>();
    let target = total.saturating_mul(pct as usize) / 100;
    let mut bytes = 0;
    for split in 1..entries.len() {
        bytes += encoded_index_entry_len(&entries[split - 1]);
        if bytes >= target {
            return split;
        }
    }
    entries.len() / 2
}

/// Process-wide leaf split fill-factor (percent of bytes that stay on
/// the left half after a split). 50 = balanced 50/50 (the existing
/// behaviour). 90 = "right-insert friendly": the right half starts
/// nearly empty, so subsequent right-edge inserts don't re-split as
/// quickly. Clamped to [10, 95]. Configured by `MYDB_INDEX_FILLFACTOR`
/// (read once at first split).
static INDEX_FILLFACTOR_PCT: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);
static INDEX_FILLFACTOR_INIT: std::sync::Once = std::sync::Once::new();

fn index_fillfactor_pct() -> u8 {
    INDEX_FILLFACTOR_INIT.call_once(|| {
        let pct = std::env::var("MYDB_INDEX_FILLFACTOR")
            .ok()
            .and_then(|s| s.parse::<u8>().ok())
            .unwrap_or(50)
            .clamp(10, 95);
        INDEX_FILLFACTOR_PCT.store(pct, std::sync::atomic::Ordering::Relaxed);
    });
    INDEX_FILLFACTOR_PCT.load(std::sync::atomic::Ordering::Relaxed)
}

fn encoded_key_part_len(part: &KeyPart) -> usize {
    match part {
        KeyPart::Null => 1,
        KeyPart::Int(_) | KeyPart::UInt(_) => 9,
        KeyPart::Float32(_) => 5,
        KeyPart::Float64(_) => 9,
        KeyPart::String(v) => 5 + v.len(),
        KeyPart::CollatedString { value, .. } => 7 + value.len(),
        KeyPart::Decimal { .. } => 18,
        KeyPart::DecimalWide { value, .. } => 3 + value.to_signed_bytes_le().len(),
        KeyPart::Date(_) => 5,
        KeyPart::Time(_) | KeyPart::DateTime(_) => 9,
        KeyPart::Year(_) => 2,
        // Param parts are deferred placeholders that must be resolved by
        // the executor before any storage call; reaching here indicates
        // a missed `IndexKey::resolve_params` call upstream.
        KeyPart::Param { .. } => {
            panic!("internal error: KeyPart::Param leaked into btree encoding")
        }
        KeyPart::Bytes(v) => 5 + v.len(),
    }
}

fn index_identity_from_path(path: &Path) -> (u64, String) {
    let stem = path
        .file_stem()
        .map(|stem| stem.to_string_lossy().into_owned())
        .unwrap_or_default();
    let Some((table, name)) = stem.split_once('_') else {
        return (0, stem);
    };
    (table.parse().unwrap_or(0), name.to_string())
}

fn read_index_page(file: &mut File, page_id: u64) -> Result<Vec<u8>, QueryError> {
    let mut buf = vec![0; PAGE_SIZE];
    file.seek(SeekFrom::Start(page_id * PAGE_SIZE as u64))
        .map_err(io_err)?;
    file.read_exact(&mut buf).map_err(io_err)?;
    Ok(buf)
}

fn write_index_page(file: &mut File, page_id: u64, buf: &[u8]) -> Result<(), QueryError> {
    file.seek(SeekFrom::Start(page_id * PAGE_SIZE as u64))
        .map_err(io_err)?;
    file.write_all(buf).map_err(io_err)
}

fn encode_index_meta(meta: &IndexMeta) -> Vec<u8> {
    let mut buf = vec![0; PAGE_SIZE];
    write_u32(&mut buf, 0, INDEX_META_MAGIC);
    write_u16(&mut buf, 4, INDEX_PAGE_VERSION);
    write_u16(&mut buf, 6, 0);
    // Offset 8..16 is the page LSN slot (`set_page_lsn`). Initialize
    // to zero here; `set_page_lsn_if_formatted` overwrites with the
    // real LSN on FPI writes.
    write_u64(&mut buf, 8, 0);
    write_u32(&mut buf, 16, 0);
    write_u32(&mut buf, 20, INDEX_VERSION);
    write_u32(&mut buf, 24, PAGE_SIZE as u32);
    write_u64(&mut buf, 64, meta.root_page);
    write_u64(&mut buf, 72, meta.next_page_id);
    write_u64(&mut buf, 80, meta.free_page_head.unwrap_or(0));
    let checksum = page_checksum(&buf, 16);
    write_u32(&mut buf, 16, checksum);
    buf
}

fn decode_index_meta(buf: &[u8]) -> Result<IndexMeta, QueryError> {
    if buf.len() != PAGE_SIZE {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index meta page length",
        ));
    }
    if read_u32(buf, 0) != INDEX_META_MAGIC {
        if buf.len() == PAGE_SIZE && &buf[..INDEX_MAGIC.len()] == INDEX_MAGIC {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "unsupported legacy index format; remove .idx files or rebuild the data directory",
            ));
        }
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index meta page magic",
        ));
    }
    if read_u16(buf, 4) != INDEX_PAGE_VERSION {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "unsupported index meta page version",
        ));
    }
    let stored_checksum = read_u32(buf, 16);
    let actual_checksum = page_checksum(buf, 16);
    if stored_checksum != actual_checksum {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index meta page checksum",
        ));
    }
    let version = read_u32(buf, 20);
    let page_size = read_u32(buf, 24);
    if version != INDEX_VERSION || page_size != PAGE_SIZE as u32 {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "unsupported index file version",
        ));
    }
    Ok(IndexMeta {
        root_page: read_u64(buf, 64),
        next_page_id: read_u64(buf, 72),
        free_page_head: match read_u64(buf, 80) {
            0 => None,
            page_id => Some(page_id),
        },
    })
}

/// Entry representation is identical in leaf and internal pages. The page
/// writer retains ownership of headers, child pointers and slot offsets.
#[inline]
fn encode_layout_index_entry(
    entry: &IndexEntry,
    key_layout: u8,
    base_time: i64,
    int2_widths: Option<(i64, u8, u8)>,
    buf: &mut [u8],
    offset: &mut usize,
    page_kind: &str,
) -> Result<(), QueryError> {
    match key_layout {
        KEY_LAYOUT_GENERIC => encode_index_entry_into_page(entry, buf, offset)?,
        KEY_LAYOUT_COMPACT_TIME_U32 => {
            encode_compact_time_entry_into_page(entry, base_time, buf, offset)?
        }
        KEY_LAYOUT_COMPACT_DATE_U16 => {
            encode_compact_date_entry_into_page(entry, base_time, buf, offset)?
        }
        KEY_LAYOUT_COMPACT_DECIMAL_I32 => {
            encode_compact_decimal_i32_entry_into_page(entry, base_time, buf, offset)?
        }
        KEY_LAYOUT_COMPACT_DECIMAL_I64 => {
            encode_compact_decimal_i64_entry_into_page(entry, base_time, buf, offset)?
        }
        KEY_LAYOUT_COMPACT_INT_U16 => {
            encode_compact_int_u16_entry_into_page(entry, base_time, buf, offset)?
        }
        KEY_LAYOUT_COMPACT_INT_U32 => {
            encode_compact_int_u32_entry_into_page(entry, base_time, buf, offset)?
        }
        KEY_LAYOUT_COMPACT_UINT_U16 => {
            encode_compact_uint_u16_entry_into_page(entry, base_time, buf, offset)?
        }
        KEY_LAYOUT_COMPACT_UINT_U32 => {
            encode_compact_uint_u32_entry_into_page(entry, base_time, buf, offset)?
        }
        KEY_LAYOUT_COMPACT_INT2 => {
            let (col2_base, w1, w2) =
                int2_widths.expect("INT2 layout requires layout_aux (verified above)");
            encode_compact_int2_entry_into_page(entry, base_time, col2_base, w1, w2, buf, offset)?
        }
        other => {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                &format!("internal error: unknown {page_kind} key_layout {other}"),
            ));
        }
    }
    Ok(())
}

fn encode_index_tree_page(page: &IndexPage) -> Result<Vec<u8>, QueryError> {
    #[cfg(test)]
    INDEX_TREE_PAGE_ENCODINGS.with(|count| count.set(count.get() + 1));
    // v12: the per-page `page_table_id` field was always zero. v19
    // dropped it from the on-disk payload entirely; in-memory
    // `LeafPayloadHeader.page_table_id` is just a 0 placeholder kept
    // so `decode_row_id_v7_from(..., page_table_id)` signatures stay
    // unchanged.

    // v13 layout safety net: if a compact layout no longer fits the
    // current entries (merge/borrow shuffled rows across compact-
    // window boundaries, or an out-of-order insert pushed a value
    // past `base`), re-pick the densest viable layout (may rebase
    // and stay compact, or demote to GENERIC). Hot inserts go
    // through `insert_internal`'s O(1) demote check and almost
    // always short-circuit here via `leaf_fits_layout` — the
    // expensive O(N) `resolve_*_layout` only fires when shape
    // changes invalidate the cached layout.
    let owned;
    let page = if needs_layout_demote(page) {
        owned = demote_page_if_unfit_owned(page);
        &owned
    } else {
        page
    };

    let mut buf = vec![0; PAGE_SIZE];
    let mut offset = INDEX_HEADER_BYTES;
    // Slot offsets are stored *relative to the start of the page
    // payload* (which begins at INDEX_HEADER_BYTES in `buf`). The
    // byte-level lookup operates on the `payload` slice so its
    // index 0 is also the start of the payload — keeping offsets
    // payload-relative makes them directly usable without
    // arithmetic at lookup time.
    let payload_origin = INDEX_HEADER_BYTES;
    match page {
        IndexPage::Leaf(page) => {
            // Trust the page's key_layout — callers normalize at
            // construction or via the O(1) insert demote check.
            let key_layout = page.key_layout;
            let base_time = page.base_time;
            let layout_aux = page.layout_aux;
            write_index_payload_u8(&mut buf, &mut offset, 1)?;
            // v20 header: 1B key_layout + 8B base_time + 8B next_leaf
            //           + 2B entry_count + 8B page_max_trx_id (v13 new).
            write_index_payload_u8(&mut buf, &mut offset, key_layout)?;
            write_index_payload_i64(&mut buf, &mut offset, base_time)?;
            write_index_payload_u64(&mut buf, &mut offset, page.next_leaf.unwrap_or(0))?;
            if page.entries.len() > u16::MAX as usize {
                return Err(error(
                    crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                    "index leaf has too many entries",
                ));
            }
            write_index_payload_u16(&mut buf, &mut offset, page.entries.len() as u16)?;
            // v13: writer bound (`IndexLeafPage::page_max_trx_id`).
            // See LEAF_OFF_PAGE_MAX_TRX_ID docs. `write_page` raised
            // it monotonically before calling us.
            write_index_payload_u64(&mut buf, &mut offset, page.page_max_trx_id)?;
            debug_assert_eq!(offset - INDEX_HEADER_BYTES, LEAF_OFF_ENTRIES);
            // v14: INT2 layout has a 20 B prefix block between the
            // standard header and the first entry, carrying col2_base
            // and per-column delta widths.
            let int2_widths = if key_layout == KEY_LAYOUT_COMPACT_INT2 {
                let aux = layout_aux.ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "INT2 leaf missing layout_aux at encode time",
                    )
                })?;
                encode_int2_prefix(aux.col2_base, aux.width1, aux.width2, &mut buf, &mut offset)?;
                Some((aux.col2_base, aux.width1, aux.width2))
            } else {
                None
            };
            // Record each entry's offset *relative to payload_origin*
            // as we write it. The slot array goes at the end of the
            // payload, after all entry bodies.
            let mut slots: Vec<IndexRawSlot> = Vec::with_capacity(page.entries.len());
            for entry in &page.entries {
                let entry_off = offset - payload_origin;
                debug_assert!(
                    entry_off <= u16::MAX as usize,
                    "leaf entry offset overflows u16"
                );
                encode_layout_index_entry(
                    entry,
                    key_layout,
                    base_time,
                    int2_widths,
                    &mut buf,
                    &mut offset,
                    "leaf",
                )?;
                slots.push(IndexRawSlot {
                    offset: entry_off as u16,
                    len: (offset - payload_origin - entry_off) as u16,
                });
            }
            // Append sorted `(offset,len)` slots.
            for slot in &slots {
                write_index_payload_u16(&mut buf, &mut offset, slot.offset)?;
                write_index_payload_u16(&mut buf, &mut offset, slot.len)?;
            }
        }
        IndexPage::Internal(page) => {
            write_index_payload_u8(&mut buf, &mut offset, 2)?;
            // v13 internal header: 1B key_layout + 8B base_time
            // immediately after page_type. Trust the caller's
            // `key_layout` — `IndexInternalPage::new_generic`
            // normalizes at construction.
            let key_layout = page.key_layout;
            let base_time = page.base_time;
            let layout_aux = page.layout_aux;
            write_index_payload_u8(&mut buf, &mut offset, key_layout)?;
            write_index_payload_i64(&mut buf, &mut offset, base_time)?;
            let first_child = page.children.first().copied().ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "internal index page has no first child",
                )
            })?;
            if page.children.len() != page.keys.len() + 1 {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "invalid internal index fanout",
                ));
            }
            write_index_payload_u64(&mut buf, &mut offset, first_child)?;
            write_index_payload_u32(&mut buf, &mut offset, page.keys.len() as u32)?;
            // v14: INT2 prefix block on internal pages too.
            let int2_widths_int = if key_layout == KEY_LAYOUT_COMPACT_INT2 {
                let aux = layout_aux.ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "INT2 internal page missing layout_aux at encode time",
                    )
                })?;
                encode_int2_prefix(aux.col2_base, aux.width1, aux.width2, &mut buf, &mut offset)?;
                Some((aux.col2_base, aux.width1, aux.width2))
            } else {
                None
            };
            // Internal pages also get a slot array — descend probes
            // a few hundred separators per traversal and benefits
            // equally from O(log fanout) byte-level navigation.
            let mut slots: Vec<IndexRawSlot> = Vec::with_capacity(page.keys.len());
            for (entry_idx, entry) in page.keys.iter().enumerate() {
                let entry_off = offset - payload_origin;
                debug_assert!(
                    entry_off <= u16::MAX as usize,
                    "internal entry offset overflows u16"
                );
                encode_layout_index_entry(
                    entry,
                    key_layout,
                    base_time,
                    int2_widths_int,
                    &mut buf,
                    &mut offset,
                    "internal",
                )?;
                write_index_payload_u64(&mut buf, &mut offset, page.children[entry_idx + 1])?;
                slots.push(IndexRawSlot {
                    offset: entry_off as u16,
                    len: (offset - payload_origin - entry_off) as u16,
                });
            }
            for slot in &slots {
                write_index_payload_u16(&mut buf, &mut offset, slot.offset)?;
                write_index_payload_u16(&mut buf, &mut offset, slot.len)?;
            }
        }
        IndexPage::Free(page) => {
            write_index_payload_u8(&mut buf, &mut offset, 3)?;
            write_index_payload_u64(&mut buf, &mut offset, page.next_free.unwrap_or(0))?;
        }
    }
    let payload_len = offset - INDEX_HEADER_BYTES;
    write_u32(&mut buf, 0, INDEX_PAGE_MAGIC);
    write_u16(&mut buf, 4, INDEX_PAGE_VERSION);
    write_u16(&mut buf, 6, 0);
    // Common-header offset 8..16 is the page LSN slot — leave zero
    // here; `set_page_lsn_if_formatted` stamps the real value on
    // FPI writes. The writer bound lives in the leaf payload at
    // LEAF_OFF_PAGE_MAX_TRX_ID.
    write_u64(&mut buf, 8, 0);
    write_u32(&mut buf, 16, 0);
    write_u32(&mut buf, 20, payload_len as u32);
    let checksum = page_checksum(&buf, 16);
    write_u32(&mut buf, 16, checksum);
    Ok(buf)
}

/// One page of borrowed canonical keys; no per-row KeyPart allocation or
/// speculative compact-layout/length rescans. Used only for generic layouts.
struct EncodedLeafPageBuilder {
    image: Vec<u8>,
    offset: usize,
    slots: Vec<IndexRawSlot>,
    first: Option<IndexEntry>,
}

impl EncodedLeafPageBuilder {
    fn new() -> Self {
        Self {
            image: vec![0; PAGE_SIZE],
            offset: LEAF_HEADER_BYTES,
            slots: Vec::new(),
            first: None,
        }
    }

    fn push(&mut self, key: &[u8], row_id: RowId) -> Result<(), QueryError> {
        if self.slots.is_empty() {
            let mut offset = 0;
            let decoded = decode_index_key_from_leaf_page(key, &mut offset)?;
            if offset != key.len() {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "encoded leaf key has trailing bytes",
                ));
            }
            self.first = Some(IndexEntry {
                key: decoded,
                row_id,
            });
        }
        let start = self.offset;
        write_index_payload_bytes(&mut self.image, &mut self.offset, key)?;
        encode_row_id_v7_into_page(row_id, &mut self.image, &mut self.offset)?;
        self.slots.push(IndexRawSlot {
            offset: (start - INDEX_HEADER_BYTES) as u16,
            len: (self.offset - start) as u16,
        });
        Ok(())
    }

    fn finish(&mut self, next: Option<u64>, lsn: u64) -> Result<Vec<u8>, QueryError> {
        let count = self.slots.len();
        for slot in self.slots.drain(..) {
            write_index_payload_u16(&mut self.image, &mut self.offset, slot.offset)?;
            write_index_payload_u16(&mut self.image, &mut self.offset, slot.len)?;
        }
        let payload_len = self.offset - INDEX_HEADER_BYTES;
        let mut header = INDEX_HEADER_BYTES;
        write_index_payload_u8(&mut self.image, &mut header, 1)?;
        write_index_payload_u8(&mut self.image, &mut header, KEY_LAYOUT_GENERIC)?;
        write_index_payload_i64(&mut self.image, &mut header, 0)?;
        write_index_payload_u64(&mut self.image, &mut header, next.unwrap_or(0))?;
        write_index_payload_u16(&mut self.image, &mut header, count as u16)?;
        write_index_payload_u64(&mut self.image, &mut header, 0)?;
        write_u32(&mut self.image, 0, INDEX_PAGE_MAGIC);
        write_u16(&mut self.image, 4, INDEX_PAGE_VERSION);
        write_u16(&mut self.image, 6, 0);
        write_u64(&mut self.image, 8, lsn);
        write_u32(&mut self.image, 16, 0);
        write_u32(&mut self.image, 20, payload_len as u32);
        let checksum = page_checksum(&self.image, 16);
        write_u32(&mut self.image, 16, checksum);
        self.offset = LEAF_HEADER_BYTES;
        Ok(std::mem::replace(&mut self.image, vec![0; PAGE_SIZE]))
    }
}

fn encoded_materialized_leaf_key_len(payload: &[u8]) -> Result<usize, QueryError> {
    if payload.len() < 4 {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "interned index key payload is truncated",
        ));
    }
    let count = u32::from_le_bytes(payload[..4].try_into().unwrap());
    if count > u8::MAX as u32 {
        return Err(error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "index key has too many parts",
        ));
    }
    Ok(payload.len() - 3)
}

/// Encode generic leaf pages directly from dictionary-owned run-stream key
/// payloads. The stream and page formats differ only in part-count width
/// (u32 vs u8); tagged key parts are copied byte-for-byte.
fn encode_interned_leaf_page(
    keys: &[EncodedInternedKey],
    records: &[u128],
    next_leaf: Option<u64>,
) -> Result<Vec<u8>, QueryError> {
    if records.len() > u16::MAX as usize {
        return Err(error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "index leaf has too many entries",
        ));
    }
    let mut buf = vec![0; PAGE_SIZE];
    let mut offset = INDEX_HEADER_BYTES;
    write_index_payload_u8(&mut buf, &mut offset, 1)?;
    write_index_payload_u8(&mut buf, &mut offset, KEY_LAYOUT_GENERIC)?;
    write_index_payload_i64(&mut buf, &mut offset, 0)?;
    write_index_payload_u64(&mut buf, &mut offset, next_leaf.unwrap_or(0))?;
    write_index_payload_u16(&mut buf, &mut offset, records.len() as u16)?;
    write_index_payload_u64(&mut buf, &mut offset, 0)?;
    debug_assert_eq!(offset - INDEX_HEADER_BYTES, LEAF_OFF_ENTRIES);
    let payload_origin = INDEX_HEADER_BYTES;
    let mut slots = Vec::with_capacity(records.len());
    for record in records.iter().copied() {
        let key = keys
            .get(interned_record_key_id(record) as usize)
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "interned index record references an unknown key",
                )
            })?;
        let payload = key.materialized_key.as_deref().ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "interned index key has no materialized payload",
            )
        })?;
        let count = u32::from_le_bytes(
            payload
                .get(..4)
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "interned index key payload is truncated",
                    )
                })?
                .try_into()
                .unwrap(),
        );
        let count = u8::try_from(count).map_err(|_| {
            error(
                crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                "index key has too many parts",
            )
        })?;
        let entry_start = offset - payload_origin;
        write_index_payload_u8(&mut buf, &mut offset, count)?;
        write_index_payload_bytes(&mut buf, &mut offset, &payload[4..])?;
        encode_row_id_v7_into_page(interned_record_row_id(record), &mut buf, &mut offset)?;
        slots.push(IndexRawSlot {
            offset: u16::try_from(entry_start).map_err(|_| {
                error(
                    crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                    "interned index entry offset exceeds page limit",
                )
            })?,
            len: u16::try_from(offset - payload_origin - entry_start).map_err(|_| {
                error(
                    crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                    "interned index entry exceeds page limit",
                )
            })?,
        });
    }
    for slot in slots {
        write_index_payload_u16(&mut buf, &mut offset, slot.offset)?;
        write_index_payload_u16(&mut buf, &mut offset, slot.len)?;
    }
    if offset > PAGE_SIZE {
        return Err(error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "interned index leaf exceeds page size",
        ));
    }
    let payload_len = offset - INDEX_HEADER_BYTES;
    write_u32(&mut buf, 0, INDEX_PAGE_MAGIC);
    write_u16(&mut buf, 4, INDEX_PAGE_VERSION);
    write_u16(&mut buf, 6, 0);
    write_u64(&mut buf, 8, 0);
    write_u32(&mut buf, 16, 0);
    write_u32(&mut buf, 20, payload_len as u32);
    let checksum = page_checksum(&buf, 16);
    write_u32(&mut buf, 16, checksum);
    Ok(buf)
}

/// A page's entry width is fixed before any row is written. Specializing
/// that width removes repeated offset/error handling from the per-row loop.
fn write_fixed_leaf_rows<T: Copy + Into<i64>, const WIDTH: usize, const TAG: u8>(
    entries: &[(T, RowId)],
    base: i64,
    output: &mut [u8],
) {
    for ((value, row_id), row) in entries.iter().zip(output.chunks_exact_mut(WIDTH + 6)) {
        let value = (*value).into();
        if WIDTH == 2 {
            row[..2].copy_from_slice(&((value - base) as u16).to_le_bytes());
        } else if WIDTH == 4 {
            row[..4].copy_from_slice(&((value - base) as u32).to_le_bytes());
        } else {
            row[0] = 1;
            row[1] = TAG;
            row[2..WIDTH].copy_from_slice(&value.to_le_bytes()[..WIDTH - 2]);
        }
        row[WIDTH..WIDTH + 4].copy_from_slice(&row_id.page_id.to_le_bytes());
        row[WIDTH + 4..].copy_from_slice(&row_id.slot_id.to_le_bytes());
    }
}

fn write_fixed_leaf_slots(output: &mut [u8], entries_start: usize, entry_width: usize) {
    for (index, slot) in output.chunks_exact_mut(SLOT_OFFSET_BYTES).enumerate() {
        let offset = entries_start + index * entry_width;
        slot[..2].copy_from_slice(&(offset as u16).to_le_bytes());
        slot[2..].copy_from_slice(&(entry_width as u16).to_le_bytes());
    }
}

fn encode_date_leaf_page(
    entries: &[(i32, RowId)],
    chunk_min: i64,
    chunk_max: i64,
    next_leaf: Option<u64>,
) -> Result<Vec<u8>, QueryError> {
    // v19: entry_count is u16 — one 16 KB leaf can never hold more
    // than ~5K entries, well under u16 cap.
    if entries.len() > u16::MAX as usize {
        return Err(error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "index page has too many entries",
        ));
    }
    let span = (chunk_max as i128) - (chunk_min as i128);
    let use_compact = !entries.is_empty() && span >= 0 && span <= u16::MAX as i128;
    let (key_layout, base_time) = if use_compact {
        (KEY_LAYOUT_COMPACT_DATE_U16, chunk_min)
    } else {
        (KEY_LAYOUT_GENERIC, 0i64)
    };

    // v19: page_table_id field removed from on-disk payload entirely.
    let mut buf = vec![0; PAGE_SIZE];
    let mut offset = INDEX_HEADER_BYTES;
    write_index_payload_u8(&mut buf, &mut offset, 1)?;
    write_index_payload_u8(&mut buf, &mut offset, key_layout)?;
    write_index_payload_i64(&mut buf, &mut offset, base_time)?;
    write_index_payload_u64(&mut buf, &mut offset, next_leaf.unwrap_or(0))?;
    write_index_payload_u16(&mut buf, &mut offset, entries.len() as u16)?;
    // v13: page_max_trx_id (Phase 1b) — bulk-load leaves have no
    // per-writer bound to stamp yet (DDL bulk-load runs under
    // `txn_id_for_ddl() = 0`), and the Phase 2 read shortcut
    // treats a zero-valued bound as "trivially visible", which is
    // correct for DDL-built indexes: any snapshot new enough to
    // see the new schema is by construction newer than the DDL.
    write_index_payload_u64(&mut buf, &mut offset, 0)?;
    debug_assert_eq!(offset - INDEX_HEADER_BYTES, LEAF_OFF_ENTRIES);
    let entry_width = if use_compact { 2 + 6 } else { 1 + 1 + 4 + 6 };
    let data_end = offset + entries.len() * entry_width;
    let slots_end = data_end + entries.len() * SLOT_OFFSET_BYTES;
    if slots_end > PAGE_SIZE {
        return Err(error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "index page is too large",
        ));
    }
    if use_compact {
        write_fixed_leaf_rows::<_, 2, 9>(entries, base_time, &mut buf[offset..data_end]);
    } else {
        write_fixed_leaf_rows::<_, 6, 9>(entries, base_time, &mut buf[offset..data_end]);
    }
    write_fixed_leaf_slots(&mut buf[data_end..slots_end], LEAF_OFF_ENTRIES, entry_width);
    offset = slots_end;

    let payload_len = offset - INDEX_HEADER_BYTES;
    write_u32(&mut buf, 0, INDEX_PAGE_MAGIC);
    write_u16(&mut buf, 4, INDEX_PAGE_VERSION);
    write_u16(&mut buf, 6, 0);
    // Offset 8..16 is the page LSN (stamped later by
    // `set_page_lsn_if_formatted` on bulk-load writes that need one);
    // page_max_trx_id lives in the leaf payload header above.
    write_u64(&mut buf, 8, 0);
    write_u32(&mut buf, 16, 0);
    write_u32(&mut buf, 20, payload_len as u32);
    let checksum = page_checksum(&buf, 16);
    write_u32(&mut buf, 16, checksum);
    Ok(buf)
}

fn encode_single_int_leaf_page(
    entries: &[(i64, RowId)],
    chunk_min: i64,
    chunk_max: i64,
    next_leaf: Option<u64>,
) -> Result<Vec<u8>, QueryError> {
    if entries.len() > u16::MAX as usize {
        return Err(error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "index page has too many entries",
        ));
    }
    let span = (chunk_max as i128) - (chunk_min as i128);
    let key_layout = if !entries.is_empty() && span >= 0 && span <= u16::MAX as i128 {
        KEY_LAYOUT_COMPACT_INT_U16
    } else if !entries.is_empty() && span >= 0 && span <= u32::MAX as i128 {
        KEY_LAYOUT_COMPACT_INT_U32
    } else {
        KEY_LAYOUT_GENERIC
    };
    let base = if key_layout == KEY_LAYOUT_GENERIC {
        0
    } else {
        chunk_min
    };

    let mut buf = vec![0; PAGE_SIZE];
    let mut offset = INDEX_HEADER_BYTES;
    write_index_payload_u8(&mut buf, &mut offset, 1)?;
    write_index_payload_u8(&mut buf, &mut offset, key_layout)?;
    write_index_payload_i64(&mut buf, &mut offset, base)?;
    write_index_payload_u64(&mut buf, &mut offset, next_leaf.unwrap_or(0))?;
    write_index_payload_u16(&mut buf, &mut offset, entries.len() as u16)?;
    write_index_payload_u64(&mut buf, &mut offset, 0)?;
    debug_assert_eq!(offset - INDEX_HEADER_BYTES, LEAF_OFF_ENTRIES);
    let entry_width = match key_layout {
        KEY_LAYOUT_COMPACT_INT_U16 => 2 + 6,
        KEY_LAYOUT_COMPACT_INT_U32 => 4 + 6,
        _ => 1 + 1 + 8 + 6,
    };
    let data_end = offset + entries.len() * entry_width;
    let slots_end = data_end + entries.len() * SLOT_OFFSET_BYTES;
    if slots_end > PAGE_SIZE {
        return Err(error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "index page is too large",
        ));
    }
    match key_layout {
        KEY_LAYOUT_COMPACT_INT_U16 => {
            write_fixed_leaf_rows::<_, 2, 1>(entries, base, &mut buf[offset..data_end])
        }
        KEY_LAYOUT_COMPACT_INT_U32 => {
            write_fixed_leaf_rows::<_, 4, 1>(entries, base, &mut buf[offset..data_end])
        }
        _ => write_fixed_leaf_rows::<_, 10, 1>(entries, base, &mut buf[offset..data_end]),
    }
    write_fixed_leaf_slots(&mut buf[data_end..slots_end], LEAF_OFF_ENTRIES, entry_width);
    offset = slots_end;

    let payload_len = offset - INDEX_HEADER_BYTES;
    if INDEX_HEADER_BYTES + payload_len > PAGE_SIZE {
        return Err(error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "index page exceeds page size",
        ));
    }
    write_u32(&mut buf, 0, INDEX_PAGE_MAGIC);
    write_u16(&mut buf, 4, INDEX_PAGE_VERSION);
    write_u16(&mut buf, 6, 0);
    write_u64(&mut buf, 8, 0);
    write_u32(&mut buf, 16, 0);
    write_u32(&mut buf, 20, payload_len as u32);
    let checksum = page_checksum(&buf, 16);
    write_u32(&mut buf, 16, checksum);
    Ok(buf)
}

#[cfg(test)]
mod fixed_leaf_stream_encoding_tests {
    use super::*;

    #[test]
    fn fixed_leaf_batches_match_general_page_encoder() {
        for values in [
            vec![],
            vec![-100i64, 0, 300],
            vec![i64::MIN, i64::MIN + 65_535],
            vec![i64::MIN, i64::MIN + u32::MAX as i64],
            vec![i64::MIN, i64::MAX],
        ] {
            let entries = values
                .iter()
                .enumerate()
                .map(|(index, &value)| {
                    (
                        value,
                        RowId {
                            page_id: index as u32 + 7,
                            slot_id: index as u16 * 3,
                        },
                    )
                })
                .collect::<Vec<_>>();
            let min = values.first().copied().unwrap_or(0);
            let max = values.last().copied().unwrap_or(0);
            let span = max as i128 - min as i128;
            let layout = if !entries.is_empty() && span <= u16::MAX as i128 {
                KEY_LAYOUT_COMPACT_INT_U16
            } else if !entries.is_empty() && span <= u32::MAX as i128 {
                KEY_LAYOUT_COMPACT_INT_U32
            } else {
                KEY_LAYOUT_GENERIC
            };
            let expected =
                encode_index_tree_page(&IndexPage::Leaf(IndexLeafPage::new_with_layout_aux(
                    entries
                        .iter()
                        .map(|&(value, id)| single_int_index_entry(value, id))
                        .collect(),
                    Some(42),
                    layout,
                    if layout == KEY_LAYOUT_GENERIC { 0 } else { min },
                    None,
                )))
                .unwrap();
            assert_eq!(
                encode_single_int_leaf_page(&entries, min, max, Some(42)).unwrap(),
                expected
            );
        }
        for values in [
            vec![],
            vec![-100i32, 0, 300],
            vec![0, 70_000],
            vec![i32::MIN, i32::MAX],
        ] {
            let entries = values
                .iter()
                .enumerate()
                .map(|(index, &value)| {
                    (
                        value,
                        RowId {
                            page_id: index as u32 + 7,
                            slot_id: index as u16 * 3,
                        },
                    )
                })
                .collect::<Vec<_>>();
            let min = i64::from(values.first().copied().unwrap_or(0));
            let max = i64::from(values.last().copied().unwrap_or(0));
            let compact = !entries.is_empty() && max - min <= i64::from(u16::MAX);
            let expected =
                encode_index_tree_page(&IndexPage::Leaf(IndexLeafPage::new_with_layout_aux(
                    entries
                        .iter()
                        .map(|&(value, id)| date_index_entry(value, id))
                        .collect(),
                    Some(42),
                    if compact {
                        KEY_LAYOUT_COMPACT_DATE_U16
                    } else {
                        KEY_LAYOUT_GENERIC
                    },
                    if compact { min } else { 0 },
                    None,
                )))
                .unwrap();
            assert_eq!(
                encode_date_leaf_page(&entries, min, max, Some(42)).unwrap(),
                expected
            );
        }
        let overflow = vec![
            (
                0i64,
                RowId {
                    page_id: 1,
                    slot_id: 1
                }
            );
            PAGE_SIZE
        ];
        assert!(encode_single_int_leaf_page(&overflow, 0, 0, None)
            .unwrap_err()
            .is(crate::error::catalog::ER_TOO_BIG_ROWSIZE));
    }
}

fn encode_pair_int_leaf_page<T: PairIntLeafEntry>(
    entries: &[T],
    min1: i64,
    max1: i64,
    min2: i64,
    max2: i64,
    next_leaf: Option<u64>,
) -> Result<Vec<u8>, QueryError> {
    if entries.len() > u16::MAX as usize {
        return Err(error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "index page has too many entries",
        ));
    }
    let widths = int2_width_for_span((max1 as i128) - (min1 as i128))
        .zip(int2_width_for_span((max2 as i128) - (min2 as i128)));
    let key_layout = if widths.is_some() {
        KEY_LAYOUT_COMPACT_INT2
    } else {
        KEY_LAYOUT_GENERIC
    };
    let mut buf = vec![0; PAGE_SIZE];
    let mut offset = INDEX_HEADER_BYTES;
    write_index_payload_u8(&mut buf, &mut offset, 1)?;
    write_index_payload_u8(&mut buf, &mut offset, key_layout)?;
    write_index_payload_i64(
        &mut buf,
        &mut offset,
        if key_layout == KEY_LAYOUT_COMPACT_INT2 {
            min1
        } else {
            0
        },
    )?;
    write_index_payload_u64(&mut buf, &mut offset, next_leaf.unwrap_or(0))?;
    write_index_payload_u16(&mut buf, &mut offset, entries.len() as u16)?;
    write_index_payload_u64(&mut buf, &mut offset, 0)?;
    debug_assert_eq!(offset - INDEX_HEADER_BYTES, LEAF_OFF_ENTRIES);

    let entry_width = if let Some((width1, width2)) = widths {
        encode_int2_prefix(min2, width1, width2, &mut buf, &mut offset)?;
        for entry in entries.iter().copied() {
            let ((value1, value2), row_id) = entry.unpack_pair_int();
            write_int2_delta(&mut buf, &mut offset, (value1 - min1) as u32, width1)?;
            write_int2_delta(&mut buf, &mut offset, (value2 - min2) as u32, width2)?;
            encode_row_id_v7_into_page(row_id, &mut buf, &mut offset)?;
        }
        width1 as usize + width2 as usize + ROW_ID_V7_ENCODED_LEN
    } else {
        for entry in entries.iter().copied() {
            let ((value1, value2), row_id) = entry.unpack_pair_int();
            write_index_payload_u8(&mut buf, &mut offset, 2)?;
            write_index_payload_u8(&mut buf, &mut offset, 1)?;
            write_index_payload_i64(&mut buf, &mut offset, value1)?;
            write_index_payload_u8(&mut buf, &mut offset, 1)?;
            write_index_payload_i64(&mut buf, &mut offset, value2)?;
            encode_row_id_v7_into_page(row_id, &mut buf, &mut offset)?;
        }
        1 + 2 * (1 + 8) + ROW_ID_V7_ENCODED_LEN
    };
    let entries_start = LEAF_OFF_ENTRIES
        + if key_layout == KEY_LAYOUT_COMPACT_INT2 {
            COMPACT_INT2_PREFIX_BYTES
        } else {
            0
        };
    for idx in 0..entries.len() {
        let slot = entries_start + idx * entry_width;
        write_index_payload_u16(&mut buf, &mut offset, slot as u16)?;
        write_index_payload_u16(&mut buf, &mut offset, entry_width as u16)?;
    }
    let payload_len = offset - INDEX_HEADER_BYTES;
    if INDEX_HEADER_BYTES + payload_len > PAGE_SIZE {
        return Err(error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "index page exceeds page size",
        ));
    }
    write_u32(&mut buf, 0, INDEX_PAGE_MAGIC);
    write_u16(&mut buf, 4, INDEX_PAGE_VERSION);
    write_u16(&mut buf, 6, 0);
    write_u64(&mut buf, 8, 0);
    write_u32(&mut buf, 16, 0);
    write_u32(&mut buf, 20, payload_len as u32);
    let checksum = page_checksum(&buf, 16);
    write_u32(&mut buf, 16, checksum);
    Ok(buf)
}

/// Scalar search target for zero-allocation integer-key lookups.
#[derive(Clone, Copy)]
enum FastScalarTarget {
    Int(i64),
    UInt(u64),
    Int2(i64, i64),
    Int3(i64, i64, i64),
    Int4(i64, i64, i64, i64),
}

impl FastScalarTarget {
    fn into_index_key(self) -> IndexKey {
        let parts = match self {
            Self::Int(value) => vec![KeyPart::Int(value)],
            Self::UInt(value) => vec![KeyPart::UInt(value)],
            Self::Int2(a, b) => vec![KeyPart::Int(a), KeyPart::Int(b)],
            Self::Int3(a, b, c) => vec![KeyPart::Int(a), KeyPart::Int(b), KeyPart::Int(c)],
            Self::Int4(a, b, c, d) => vec![
                KeyPart::Int(a),
                KeyPart::Int(b),
                KeyPart::Int(c),
                KeyPart::Int(d),
            ],
        };
        IndexKey::from(parts)
    }
}

/// Parsed leaf payload header shared by every byte-level lookup.
struct LeafPayloadHeader<'a> {
    payload: &'a [u8],
    page_table_id: u64,
    key_layout: u8,
    base_time: i64,
    next_leaf: Option<u64>,
    count: usize,
    slot_array_start: usize,
    layout_aux: Option<LayoutAux>,
    #[allow(dead_code)]
    page_max_trx_id: u64,
}

fn parse_leaf_payload_header(buf: &[u8]) -> Result<LeafPayloadHeader<'_>, QueryError> {
    // The full index page header (size / magic / version / checksum) is
    // validated once per resident page image by the `with_index_raw_bytes`
    // caller boundary; every byte-level leaf lookup routes through it. This
    // hot path only re-derives the structural fields it needs, and still
    // bounds-checks `payload_len` so the payload slice below cannot panic.
    debug_assert_eq!(buf.len(), PAGE_SIZE);
    let payload_len = read_u32(buf, 20) as usize;
    if payload_len < LEAF_PAYLOAD_HEADER_BYTES || INDEX_HEADER_BYTES + payload_len > PAGE_SIZE {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: leaf payload too short",
        ));
    }
    let payload = &buf[INDEX_HEADER_BYTES..INDEX_HEADER_BYTES + payload_len];
    // v19: page_table_id no longer on disk; in-memory header carries 0
    // so `decode_row_id_v7_from(..., page_table_id)` signatures stay
    // unchanged.
    let page_table_id: u64 = 0;
    let page_type = payload[LEAF_OFF_PAGE_TYPE];
    if page_type != 1 {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "byte-level lookup called on non-leaf page",
        ));
    }
    let key_layout = payload[LEAF_OFF_KEY_LAYOUT];
    let base_time = i64::from_le_bytes(
        payload[LEAF_OFF_BASE_TIME..LEAF_OFF_BASE_TIME + 8]
            .try_into()
            .unwrap(),
    );
    let next_leaf_raw = u64::from_le_bytes(
        payload[LEAF_OFF_NEXT_LEAF..LEAF_OFF_NEXT_LEAF + 8]
            .try_into()
            .unwrap(),
    );
    let count = u16::from_le_bytes(
        payload[LEAF_OFF_ENTRY_COUNT..LEAF_OFF_ENTRY_COUNT + 2]
            .try_into()
            .unwrap(),
    ) as usize;
    let next_leaf = (next_leaf_raw != 0).then_some(next_leaf_raw);
    // v14: INT2 layout has a 20 B prefix block right after the
    // standard header (at LEAF_OFF_ENTRIES). Parse it now so
    // `decode_key_at` doesn't have to.
    let layout_aux = if key_layout == KEY_LAYOUT_COMPACT_INT2 {
        let mut int2_off = LEAF_OFF_ENTRIES;
        let (col2_base, width1, width2) = decode_int2_prefix(payload, &mut int2_off)?;
        Some(LayoutAux {
            col2_base,
            width1,
            width2,
        })
    } else {
        None
    };
    let slot_array_start = if count == 0 {
        payload_len
    } else {
        let start = payload_len
            .checked_sub(count * SLOT_OFFSET_BYTES)
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt index: slot array underflow",
                )
            })?;
        if start < LEAF_OFF_ENTRIES {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: slot array overlaps header",
            ));
        }
        start
    };
    // Payload-level writer bound (v13, LEAF_OFF_PAGE_MAX_TRX_ID).
    // `payload` above is the payload slice into `buf`, so the
    // absolute page offset equals `LEAF_OFF_PAGE_MAX_TRX_ID` here.
    let page_max_trx_id = u64::from_le_bytes(
        payload[LEAF_OFF_PAGE_MAX_TRX_ID..LEAF_OFF_PAGE_MAX_TRX_ID + 8]
            .try_into()
            .unwrap(),
    );
    Ok(LeafPayloadHeader {
        payload,
        page_table_id,
        key_layout,
        base_time,
        next_leaf,
        count,
        slot_array_start,
        layout_aux,
        page_max_trx_id,
    })
}

/// Return just the two fields needed by an unbounded, all-visible COUNT(*).
/// `with_index_raw_bytes` has already validated the page header, checksum and
/// slot boundaries, so internal/free pages can be skipped without decoding
/// their keys and leaf pages only need their compact payload header parsed.
fn raw_leaf_count_and_writer_bound(buf: &[u8]) -> Result<Option<(usize, u64)>, QueryError> {
    let payload_len = read_u32(buf, 20) as usize;
    if payload_len == 0 || INDEX_HEADER_BYTES + payload_len > PAGE_SIZE {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index page length",
        ));
    }
    match buf[INDEX_HEADER_BYTES + LEAF_OFF_PAGE_TYPE] {
        0 | 2 | 3 => Ok(None),
        1 => {
            let header = parse_leaf_payload_header(buf)?;
            Ok(Some((header.count, header.page_max_trx_id)))
        }
        _ => Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: unknown page type",
        )),
    }
}

/// Return the compacted encoded size represented by a raw slotted leaf and
/// its live entry count. Raw deletes leave record-body holes, so the page
/// header's `payload_len` is only a physical high-water mark and cannot be
/// used for B-tree minimum-fill decisions.
fn raw_leaf_live_encoded_len_and_count(buf: &[u8]) -> Result<(usize, usize), QueryError> {
    let header = parse_leaf_payload_header(buf)?;
    let records_start = LEAF_OFF_ENTRIES
        + usize::from(header.key_layout == KEY_LAYOUT_COMPACT_INT2) * COMPACT_INT2_PREFIX_BYTES;
    let mut record_bytes = 0usize;
    for slot_idx in 0..header.count {
        let slot = header.read_slot(slot_idx);
        let start = slot.offset as usize;
        let end = start.checked_add(slot.len as usize).ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: slot record overflow",
            )
        })?;
        if slot.len == 0 || start < records_start || end > header.slot_array_start {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: slot record bounds",
            ));
        }
        record_bytes = record_bytes.checked_add(slot.len as usize).ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: live size overflow",
            )
        })?;
    }
    let live_size = INDEX_HEADER_BYTES
        .checked_add(records_start)
        .and_then(|size| size.checked_add(record_bytes))
        .and_then(|size| size.checked_add(header.count * SLOT_OFFSET_BYTES))
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: live size overflow",
            )
        })?;
    Ok((live_size, header.count))
}

impl<'a> LeafPayloadHeader<'a> {
    #[inline(always)]
    fn read_slot(&self, slot: usize) -> IndexRawSlot {
        let off = self.slot_array_start + slot * SLOT_OFFSET_BYTES;
        IndexRawSlot {
            offset: u16::from_le_bytes(self.payload[off..off + 2].try_into().unwrap()),
            len: u16::from_le_bytes(self.payload[off + 2..off + 4].try_into().unwrap()),
        }
    }

    #[inline(always)]
    fn read_slot_offset(&self, slot: usize) -> usize {
        self.read_slot(slot).offset as usize
    }

    /// Generic pages do not carry the index arity in their fixed header, but
    /// every live entry of a page belongs to the same index.  Inspecting the
    /// first record once per lookup lets exact full-key integer probes use the
    /// specialized comparator below without mistaking a shorter prefix bound
    /// for a complete key.
    #[inline]
    fn generic_key_arity_is(&self, expected: usize) -> bool {
        if self.key_layout != KEY_LAYOUT_GENERIC || self.count == 0 {
            return false;
        }
        let slot = self.read_slot(0);
        let offset = slot.offset as usize;
        slot.len != 0
            && offset < self.slot_array_start
            && self.payload.get(offset).copied() == u8::try_from(expected).ok()
    }

    /// Small monomorphic comparator for the overwhelmingly common
    /// auto-increment/INTEGER leaf layout.  Keeping this out of the large
    /// layout dispatch below lets point lookups inline two loads and one
    /// comparison at every binary-search probe.
    #[inline(always)]
    fn cmp_compact_int_u16_at(
        &self,
        slot_idx: usize,
        target: i64,
    ) -> Result<(std::cmp::Ordering, usize), QueryError> {
        let slot = self.read_slot(slot_idx);
        let cursor = slot.offset as usize;
        if slot.len < 2 || cursor + 2 > self.slot_array_start {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: truncated compact int u16 key",
            ));
        }
        let delta = u16::from_le_bytes([self.payload[cursor], self.payload[cursor + 1]]) as i64;
        let value = self.base_time.wrapping_add(delta);
        Ok((value.cmp(&target), cursor + 2))
    }

    #[inline(always)]
    fn lower_bound_compact_int_u16(&self, target: i64) -> Result<usize, QueryError> {
        let mut lo = 0usize;
        let mut hi = self.count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.cmp_compact_int_u16_at(mid, target)?.0 == std::cmp::Ordering::Less {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }

    /// Resolve the two widths once per lookup, rather than dispatching on
    /// layout, target kind and both widths at every binary-search probe.
    #[inline]
    fn lower_bound_scalar(&self, target: FastScalarTarget) -> Result<usize, QueryError> {
        if self.key_layout == KEY_LAYOUT_COMPACT_INT2 {
            if let FastScalarTarget::Int2(first, second) = target {
                let aux = self.layout_aux.ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "INT2 layout missing prefix block at compare time",
                    )
                })?;
                let packed = match (aux.width1, aux.width2) {
                    (1, 1) => self.lower_bound_packed_int2::<1, 1>(first, second, aux.col2_base)?,
                    (1, 2) => self.lower_bound_packed_int2::<1, 2>(first, second, aux.col2_base)?,
                    (1, 4) => self.lower_bound_packed_int2::<1, 4>(first, second, aux.col2_base)?,
                    (2, 1) => self.lower_bound_packed_int2::<2, 1>(first, second, aux.col2_base)?,
                    (2, 2) => self.lower_bound_packed_int2::<2, 2>(first, second, aux.col2_base)?,
                    (2, 4) => self.lower_bound_packed_int2::<2, 4>(first, second, aux.col2_base)?,
                    (4, 1) => self.lower_bound_packed_int2::<4, 1>(first, second, aux.col2_base)?,
                    (4, 2) => self.lower_bound_packed_int2::<4, 2>(first, second, aux.col2_base)?,
                    (4, 4) => self.lower_bound_packed_int2::<4, 4>(first, second, aux.col2_base)?,
                    _ => {
                        return Err(error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "compact int2 delta width must be 1, 2, or 4",
                        ))
                    }
                };
                if let Some(slot) = packed {
                    return Ok(slot);
                }
            }
        }
        if self.key_layout == KEY_LAYOUT_GENERIC {
            match target {
                FastScalarTarget::Int(a) => return self.lower_bound_generic_ints([a], target),
                FastScalarTarget::Int2(a, b) => {
                    return self.lower_bound_generic_ints([a, b], target)
                }
                FastScalarTarget::Int3(a, b, c) => {
                    return self.lower_bound_generic_ints([a, b, c], target)
                }
                FastScalarTarget::Int4(a, b, c, d) => {
                    return self.lower_bound_generic_ints([a, b, c, d], target)
                }
                FastScalarTarget::UInt(_) => {}
            }
        }
        let mut lo = 0usize;
        let mut hi = self.count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.cmp_scalar_at(mid, target)?.0 == std::cmp::Ordering::Less {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }

    #[inline]
    fn lower_bound_generic_ints<const N: usize>(
        &self,
        values: [i64; N],
        target: FastScalarTarget,
    ) -> Result<usize, QueryError> {
        let mut lo = 0usize;
        let mut hi = self.count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let slot = self.read_slot(mid);
            let start = slot.offset as usize;
            let size = 1 + 9 * N;
            let bytes = (usize::from(slot.len) >= size && start + size <= self.slot_array_start)
                .then(|| self.payload.get(start..start + size))
                .flatten()
                .filter(|bytes| {
                    bytes[0] as usize == N && (0..N).all(|column| bytes[1 + 9 * column] == 1)
                });
            let ordering = match bytes {
                Some(bytes) => {
                    let mut ordering = std::cmp::Ordering::Equal;
                    for (column, target) in values.iter().enumerate() {
                        let offset = 2 + 9 * column;
                        let value =
                            i64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
                        ordering = value.cmp(target);
                        if ordering != std::cmp::Ordering::Equal {
                            break;
                        }
                    }
                    ordering
                }
                // NULL and mixed-tag entries retain the canonical comparator;
                // integer target shape alone does not prove stored key shape.
                None => self.cmp_scalar_at(mid, target)?.0,
            };
            if ordering == std::cmp::Ordering::Less {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }

    #[inline]
    fn lower_bound_packed_int2<const FIRST: usize, const SECOND: usize>(
        &self,
        first: i64,
        second: i64,
        second_base: i64,
    ) -> Result<Option<usize>, QueryError> {
        let max_first = (1u64 << (FIRST * 8)) - 1;
        let max_second = (1u64 << (SECOND * 8)) - 1;
        // The legacy comparator uses wrapping addition. Near signed limits,
        // or for a target outside the representable delta range, retain that
        // comparator rather than making an unproved unsigned-order shortcut.
        if self.base_time.checked_add(max_first as i64).is_none()
            || second_base.checked_add(max_second as i64).is_none()
        {
            return Ok(None);
        }
        let a = i128::from(first) - i128::from(self.base_time);
        let b = i128::from(second) - i128::from(second_base);
        if a < 0 || a > i128::from(max_first) || b < 0 || b > i128::from(max_second) {
            return Ok(None);
        }
        let target = ((a as u64) << (SECOND * 8)) | b as u64;
        let mut lo = 0usize;
        let mut hi = self.count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let slot = self.read_slot(mid);
            let start = slot.offset as usize;
            let end = start + FIRST + SECOND;
            if usize::from(slot.len) < FIRST + SECOND || end > self.slot_array_start {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt index: truncated compact int2 key",
                ));
            }
            let bytes = self.payload.get(start..end).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt index: truncated compact int2 key",
                )
            })?;
            let mut left = 0u64;
            let mut right = 0u64;
            // Both loops have compile-time lengths (1/2/4). LLVM folds them
            // into fixed loads, with the slice bounds checked once above.
            for i in 0..FIRST {
                left |= u64::from(bytes[i]) << (8 * i);
            }
            for i in 0..SECOND {
                right |= u64::from(bytes[FIRST + i]) << (8 * i);
            }
            let value = (left << (SECOND * 8)) | right;
            if value < target {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(Some(lo))
    }

    /// P0-2 zero-alloc fast path gate. Returns the scalar target when
    /// every binary-search probe on this leaf can compare a single
    /// integer off the page instead of rebuilding an `IndexKey` (Vec +
    /// enum tag) at each `mid`. Eligible when the target is a
    /// single-column `INT` / `INT UNSIGNED` value and this leaf uses
    /// its matching fixed-shape compact layout. Generic pages are also
    /// eligible for complete one-to-four-part integer keys after the first
    /// record confirms the page arity; shorter lookup prefixes therefore
    /// keep using the general comparator.
    fn fast_scalar_target(&self, target: &IndexKey) -> Option<FastScalarTarget> {
        match target.0.as_slice() {
            [KeyPart::Int(v)] => match self.key_layout {
                KEY_LAYOUT_COMPACT_INT_U16 | KEY_LAYOUT_COMPACT_INT_U32 => {
                    Some(FastScalarTarget::Int(*v))
                }
                KEY_LAYOUT_GENERIC if self.generic_key_arity_is(1) => {
                    Some(FastScalarTarget::Int(*v))
                }
                _ => None,
            },
            [KeyPart::UInt(v)] => match self.key_layout {
                KEY_LAYOUT_COMPACT_UINT_U16 | KEY_LAYOUT_COMPACT_UINT_U32 => {
                    Some(FastScalarTarget::UInt(*v))
                }
                KEY_LAYOUT_GENERIC if self.generic_key_arity_is(1) => {
                    Some(FastScalarTarget::UInt(*v))
                }
                _ => None,
            },
            [KeyPart::Int(a), KeyPart::Int(b)] => match self.key_layout {
                KEY_LAYOUT_COMPACT_INT2 => Some(FastScalarTarget::Int2(*a, *b)),
                KEY_LAYOUT_GENERIC if self.generic_key_arity_is(2) => {
                    Some(FastScalarTarget::Int2(*a, *b))
                }
                _ => None,
            },
            [KeyPart::Int(a), KeyPart::Int(b), KeyPart::Int(c)] if self.generic_key_arity_is(3) => {
                Some(FastScalarTarget::Int3(*a, *b, *c))
            }
            [KeyPart::Int(a), KeyPart::Int(b), KeyPart::Int(c), KeyPart::Int(d)]
                if self.generic_key_arity_is(4) =>
            {
                Some(FastScalarTarget::Int4(*a, *b, *c, *d))
            }
            _ => None,
        }
    }

    /// Compare the entry at `slot_idx` against `target` with no
    /// allocation, returning `(entry.cmp(target), cursor)` where
    /// `cursor` is positioned at the entry's RowId so the caller can
    /// decode it on an `Equal` hit. Caller must have gated via
    /// `fast_scalar_target`, which guarantees the layout/target shape
    /// this matches on. Produces exactly the ordering the slow
    /// `decode_key_at` + `IndexKey::cmp` path would (NULL keys sort
    /// before any non-null integer, matching `key_part_rank`).
    fn cmp_scalar_at(
        &self,
        slot_idx: usize,
        target: FastScalarTarget,
    ) -> Result<(std::cmp::Ordering, usize), QueryError> {
        use std::cmp::Ordering;
        if self.key_layout == KEY_LAYOUT_COMPACT_INT_U16 {
            return match target {
                FastScalarTarget::Int(target) => self.cmp_compact_int_u16_at(slot_idx, target),
                FastScalarTarget::UInt(_)
                | FastScalarTarget::Int2(_, _)
                | FastScalarTarget::Int3(_, _, _)
                | FastScalarTarget::Int4(_, _, _, _) => Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "internal error: signed compact layout with unsigned target",
                )),
            };
        }
        let mut cursor = self.read_slot_offset(slot_idx);
        match self.key_layout {
            KEY_LAYOUT_COMPACT_INT_U32 => {
                let delta = read_u32_checked(self.payload, &mut cursor)? as i64;
                let v = self.base_time.wrapping_add(delta);
                match target {
                    FastScalarTarget::Int(t) => Ok((v.cmp(&t), cursor)),
                    FastScalarTarget::UInt(_)
                    | FastScalarTarget::Int2(_, _)
                    | FastScalarTarget::Int3(_, _, _)
                    | FastScalarTarget::Int4(_, _, _, _) => Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "internal error: signed compact layout with unsigned target",
                    )),
                }
            }
            KEY_LAYOUT_COMPACT_UINT_U16 | KEY_LAYOUT_COMPACT_UINT_U32 => {
                let delta = if self.key_layout == KEY_LAYOUT_COMPACT_UINT_U16 {
                    read_u16_checked(self.payload, &mut cursor)? as u64
                } else {
                    read_u32_checked(self.payload, &mut cursor)? as u64
                };
                let v = ordered_i64_to_uint(self.base_time).wrapping_add(delta);
                match target {
                    FastScalarTarget::UInt(t) => Ok((v.cmp(&t), cursor)),
                    FastScalarTarget::Int(_)
                    | FastScalarTarget::Int2(_, _)
                    | FastScalarTarget::Int3(_, _, _)
                    | FastScalarTarget::Int4(_, _, _, _) => Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "internal error: unsigned compact layout with signed target",
                    )),
                }
            }
            KEY_LAYOUT_COMPACT_INT2 => {
                let aux = self.layout_aux.ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "INT2 layout missing prefix block at compare time",
                    )
                })?;
                let d1 = read_int2_delta(self.payload, &mut cursor, aux.width1)?;
                let d2 = read_int2_delta(self.payload, &mut cursor, aux.width2)?;
                let v1 = self.base_time.wrapping_add(d1 as i64);
                let v2 = aux.col2_base.wrapping_add(d2 as i64);
                match target {
                    FastScalarTarget::Int2(t1, t2) => Ok(((v1, v2).cmp(&(t1, t2)), cursor)),
                    FastScalarTarget::Int(_)
                    | FastScalarTarget::UInt(_)
                    | FastScalarTarget::Int3(_, _, _)
                    | FastScalarTarget::Int4(_, _, _, _) => Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "internal error: INT2 compact layout with scalar target",
                    )),
                }
            }
            KEY_LAYOUT_GENERIC => {
                // On-page generic entry: [u8 part_count][u8 tag][value..].
                // The gate above has verified that this is a complete key,
                // not a prefix bound.  Consume every part even after the
                // ordering is known because internal-page routing needs the
                // returned cursor to point at the separator RowId.
                let record_start = cursor;
                let (signed, signed_len) = match target {
                    FastScalarTarget::Int(a) => ([a, 0, 0, 0], 1),
                    FastScalarTarget::Int2(a, b) => ([a, b, 0, 0], 2),
                    FastScalarTarget::Int3(a, b, c) => ([a, b, c, 0], 3),
                    FastScalarTarget::Int4(a, b, c, d) => ([a, b, c, d], 4),
                    FastScalarTarget::UInt(_) => ([0; 4], 0),
                };
                let part_count = read_u8_checked(self.payload, &mut cursor)?;
                let expected = if signed_len == 0 { 1 } else { signed_len };
                if usize::from(part_count) != expected {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: inconsistent generic key arity",
                    ));
                }
                if let FastScalarTarget::UInt(target) = target {
                    let tag = read_u8_checked(self.payload, &mut cursor)?;
                    return match tag {
                        0 => Ok((Ordering::Less, cursor)),
                        2 => {
                            let value = read_u64_checked(self.payload, &mut cursor)?;
                            Ok((value.cmp(&target), cursor))
                        }
                        _ => {
                            cursor = record_start;
                            let target = FastScalarTarget::UInt(target).into_index_key();
                            let ordering =
                                compare_generic_index_key_from(self.payload, &mut cursor, &target)?;
                            Ok((ordering, cursor))
                        }
                    };
                }

                let mut ordering = Ordering::Equal;
                for target_value in &signed[..signed_len] {
                    let tag = read_u8_checked(self.payload, &mut cursor)?;
                    let part_order = match tag {
                        0 => Ordering::Less,
                        1 => read_i64_checked(self.payload, &mut cursor)?.cmp(target_value),
                        _ => {
                            cursor = record_start;
                            let target = target.into_index_key();
                            let ordering =
                                compare_generic_index_key_from(self.payload, &mut cursor, &target)?;
                            return Ok((ordering, cursor));
                        }
                    };
                    if ordering == Ordering::Equal && part_order != Ordering::Equal {
                        ordering = part_order;
                    }
                }
                Ok((ordering, cursor))
            }
            _ => Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "internal error: cmp_scalar_at on unsupported layout",
            )),
        }
    }

    /// Compare one on-page key against a concrete lookup key and leave the
    /// returned cursor at the RowId. Generic composite keys are compared
    /// directly from their encoded bytes: unlike `decode_key_at`, this path
    /// neither allocates a temporary key container nor copies string/byte
    /// parts at every binary-search probe.
    fn cmp_key_at(
        &self,
        slot_idx: usize,
        target: &IndexKey,
    ) -> Result<(std::cmp::Ordering, usize), QueryError> {
        let mut cursor = self.read_slot_offset(slot_idx);
        if self.key_layout == KEY_LAYOUT_GENERIC {
            let ordering = compare_generic_index_key_from(self.payload, &mut cursor, target)?;
            return Ok((ordering, cursor));
        }
        // These compact scalar layouts do not use the integer fast-target
        // gate. Compare their native value directly instead of constructing
        // an IndexKey at every binary-search probe. Different shapes/scales
        // retain the canonical total-order comparator below.
        let ordering = match (self.key_layout, target.0.as_slice()) {
            (KEY_LAYOUT_COMPACT_DATE_U16, [KeyPart::Date(target)]) => {
                let delta = read_u16_checked(self.payload, &mut cursor)?;
                Some((self.base_time.wrapping_add(delta as i64) as i32).cmp(target))
            }
            (KEY_LAYOUT_COMPACT_TIME_U32, [KeyPart::DateTime(target)]) => {
                let delta = read_u32_checked(self.payload, &mut cursor)?;
                Some(self.base_time.wrapping_add(delta as i64).cmp(target))
            }
            (KEY_LAYOUT_COMPACT_DECIMAL_I32, [KeyPart::Decimal { value, scale }])
                if *scale == self.base_time as u8 =>
            {
                let stored = read_u32_checked(self.payload, &mut cursor)? as i32 as i128;
                Some(stored.cmp(value))
            }
            (KEY_LAYOUT_COMPACT_DECIMAL_I64, [KeyPart::Decimal { value, scale }])
                if *scale == self.base_time as u8 =>
            {
                let stored = read_i64_checked(self.payload, &mut cursor)? as i128;
                Some(stored.cmp(value))
            }
            _ => None,
        };
        if let Some(ordering) = ordering {
            return Ok((ordering, cursor));
        }
        let key = self.decode_key_at(&mut cursor)?;
        Ok((key.cmp(target), cursor))
    }

    fn decode_key_at(&self, cursor: &mut usize) -> Result<IndexKey, QueryError> {
        match self.key_layout {
            KEY_LAYOUT_GENERIC => decode_index_key_from_leaf_page(self.payload, cursor),
            KEY_LAYOUT_COMPACT_TIME_U32 => {
                let delta = read_u32_checked(self.payload, cursor)?;
                let value = self.base_time.wrapping_add(delta as i64);
                Ok(IndexKey::from(smallvec::smallvec![KeyPart::DateTime(
                    value
                )]))
            }
            KEY_LAYOUT_COMPACT_DATE_U16 => {
                let delta = read_u16_checked(self.payload, cursor)?;
                let days = self.base_time.wrapping_add(delta as i64);
                Ok(IndexKey::from(smallvec::smallvec![KeyPart::Date(
                    days as i32
                )]))
            }
            KEY_LAYOUT_COMPACT_DECIMAL_I32 => {
                if *cursor + 4 > self.payload.len() {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "truncated compact decimal i32 key",
                    ));
                }
                let mut mag = [0u8; 4];
                mag.copy_from_slice(&self.payload[*cursor..*cursor + 4]);
                *cursor += 4;
                let value = i32::from_le_bytes(mag) as i128;
                Ok(IndexKey::from(smallvec::smallvec![KeyPart::Decimal {
                    value,
                    scale: self.base_time as u8,
                }]))
            }
            KEY_LAYOUT_COMPACT_DECIMAL_I64 => {
                if *cursor + 8 > self.payload.len() {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "truncated compact decimal i64 key",
                    ));
                }
                let mut mag = [0u8; 8];
                mag.copy_from_slice(&self.payload[*cursor..*cursor + 8]);
                *cursor += 8;
                let value = i64::from_le_bytes(mag) as i128;
                Ok(IndexKey::from(smallvec::smallvec![KeyPart::Decimal {
                    value,
                    scale: self.base_time as u8,
                }]))
            }
            KEY_LAYOUT_COMPACT_INT_U16 => {
                let delta = read_u16_checked(self.payload, cursor)?;
                let value = self.base_time.wrapping_add(delta as i64);
                Ok(IndexKey::from(smallvec::smallvec![KeyPart::Int(value)]))
            }
            KEY_LAYOUT_COMPACT_INT_U32 => {
                let delta = read_u32_checked(self.payload, cursor)?;
                let value = self.base_time.wrapping_add(delta as i64);
                Ok(IndexKey::from(smallvec::smallvec![KeyPart::Int(value)]))
            }
            KEY_LAYOUT_COMPACT_UINT_U16 => {
                let delta = read_u16_checked(self.payload, cursor)?;
                let value = ordered_i64_to_uint(self.base_time).wrapping_add(delta as u64);
                Ok(IndexKey::from(smallvec::smallvec![KeyPart::UInt(value)]))
            }
            KEY_LAYOUT_COMPACT_UINT_U32 => {
                let delta = read_u32_checked(self.payload, cursor)?;
                let value = ordered_i64_to_uint(self.base_time).wrapping_add(delta as u64);
                Ok(IndexKey::from(smallvec::smallvec![KeyPart::UInt(value)]))
            }
            KEY_LAYOUT_COMPACT_INT2 => {
                let aux = self.layout_aux.ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "INT2 layout missing prefix block at decode time",
                    )
                })?;
                let d1 = read_int2_delta(self.payload, cursor, aux.width1)?;
                let d2 = read_int2_delta(self.payload, cursor, aux.width2)?;
                let v1 = self.base_time.wrapping_add(d1 as i64);
                let v2 = aux.col2_base.wrapping_add(d2 as i64);
                Ok(IndexKey::from(smallvec::smallvec![
                    KeyPart::Int(v1),
                    KeyPart::Int(v2)
                ]))
            }
            other => Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                &format!("corrupt index: unknown leaf key_layout {other}"),
            )),
        }
    }

    /// Decode only the leading integer-domain key part. Runtime exact-key
    /// filters use this before materialising the complete covering key. The
    /// index schema has already proved that the leading column belongs to
    /// this domain; any incompatible on-page tag is therefore corruption.
    #[inline]
    fn decode_first_lossless_i128_at(
        &self,
        cursor: &mut usize,
    ) -> Result<Option<i128>, QueryError> {
        let value = match self.key_layout {
            KEY_LAYOUT_GENERIC => {
                let part_count = read_u8_checked(self.payload, cursor)?;
                if part_count == 0 {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "integer runtime filter called on empty index key",
                    ));
                }
                match read_u8_checked(self.payload, cursor)? {
                    0 => return Ok(None),
                    1 => i128::from(read_i64_checked(self.payload, cursor)?),
                    2 => i128::from(read_u64_checked(self.payload, cursor)?),
                    9 => i128::from(read_i32_checked(self.payload, cursor)?),
                    10 | 11 => i128::from(read_i64_checked(self.payload, cursor)?),
                    12 => {
                        let year = read_u8_checked(self.payload, cursor)?;
                        if year == 0 {
                            0
                        } else {
                            1900 + i128::from(year)
                        }
                    }
                    _ => {
                        return Err(error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "integer runtime filter called on incompatible index key",
                        ))
                    }
                }
            }
            KEY_LAYOUT_COMPACT_TIME_U32 | KEY_LAYOUT_COMPACT_INT_U32 => i128::from(
                self.base_time
                    .wrapping_add(read_u32_checked(self.payload, cursor)? as i64),
            ),
            KEY_LAYOUT_COMPACT_DATE_U16 | KEY_LAYOUT_COMPACT_INT_U16 => i128::from(
                self.base_time
                    .wrapping_add(read_u16_checked(self.payload, cursor)? as i64),
            ),
            KEY_LAYOUT_COMPACT_UINT_U16 => i128::from(
                ordered_i64_to_uint(self.base_time)
                    .wrapping_add(read_u16_checked(self.payload, cursor)? as u64),
            ),
            KEY_LAYOUT_COMPACT_UINT_U32 => i128::from(
                ordered_i64_to_uint(self.base_time)
                    .wrapping_add(read_u32_checked(self.payload, cursor)? as u64),
            ),
            KEY_LAYOUT_COMPACT_INT2 => {
                let aux = self.layout_aux.ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "INT2 layout missing prefix block at runtime-filter decode time",
                    )
                })?;
                i128::from(self.base_time.wrapping_add(read_int2_delta(
                    self.payload,
                    cursor,
                    aux.width1,
                )? as i64))
            }
            _ => {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "integer runtime filter called on unsupported compact index layout",
                ))
            }
        };
        Ok(Some(value))
    }

    /// Decode a single-column key into the integer domain used by the
    /// fused COUNT/INNER-JOIN fast path, leaving `cursor` at the RowId.
    ///
    /// Unlike `decode_key_at`, this never constructs `IndexKey` or
    /// `KeyPart`. The caller has already proved from the schema that the
    /// indexed column maps losslessly to `i64`; encountering any other
    /// on-page shape is therefore corruption or an invalid fast-path call.
    #[inline]
    fn decode_lossless_i64_at(&self, cursor: &mut usize) -> Result<Option<i64>, QueryError> {
        match self.key_layout {
            KEY_LAYOUT_GENERIC => {
                let part_count = read_u8_checked(self.payload, cursor)?;
                if part_count != 1 {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "integer leaf stream called on multi-part index key",
                    ));
                }
                let tag = read_u8_checked(self.payload, cursor)?;
                match tag {
                    0 => Ok(None),
                    1 => Ok(Some(read_i64_checked(self.payload, cursor)?)),
                    2 => {
                        let value = read_u64_checked(self.payload, cursor)?;
                        i64::try_from(value).map(Some).map_err(|_| {
                            error(
                                crate::error::catalog::ER_GET_ERRNO,
                                "unsigned index key exceeds integer leaf stream range",
                            )
                        })
                    }
                    9 => Ok(Some(read_i32_checked(self.payload, cursor)? as i64)),
                    10 | 11 => Ok(Some(read_i64_checked(self.payload, cursor)?)),
                    12 => {
                        let value = read_u8_checked(self.payload, cursor)?;
                        Ok(Some(if value == 0 { 0 } else { 1900 + value as i64 }))
                    }
                    _ => Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "integer leaf stream called on unsupported key type",
                    )),
                }
            }
            KEY_LAYOUT_COMPACT_TIME_U32 => {
                let delta = read_u32_checked(self.payload, cursor)?;
                Ok(Some(self.base_time.wrapping_add(delta as i64)))
            }
            KEY_LAYOUT_COMPACT_DATE_U16 | KEY_LAYOUT_COMPACT_INT_U16 => {
                let delta = read_u16_checked(self.payload, cursor)?;
                Ok(Some(self.base_time.wrapping_add(delta as i64)))
            }
            KEY_LAYOUT_COMPACT_INT_U32 => {
                let delta = read_u32_checked(self.payload, cursor)?;
                Ok(Some(self.base_time.wrapping_add(delta as i64)))
            }
            KEY_LAYOUT_COMPACT_UINT_U16 => {
                let delta = read_u16_checked(self.payload, cursor)?;
                let value = ordered_i64_to_uint(self.base_time).wrapping_add(delta as u64);
                i64::try_from(value).map(Some).map_err(|_| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "unsigned index key exceeds integer leaf stream range",
                    )
                })
            }
            KEY_LAYOUT_COMPACT_UINT_U32 => {
                let delta = read_u32_checked(self.payload, cursor)?;
                let value = ordered_i64_to_uint(self.base_time).wrapping_add(delta as u64);
                i64::try_from(value).map(Some).map_err(|_| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "unsigned index key exceeds integer leaf stream range",
                    )
                })
            }
            _ => Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "integer leaf stream called on unsupported leaf layout",
            )),
        }
    }

    fn decode_numeric_part_at(
        &self,
        cursor: &mut usize,
        position: usize,
    ) -> Result<KeyPart, QueryError> {
        if self.key_layout != KEY_LAYOUT_GENERIC {
            let mut key = self.decode_key_at(cursor)?;
            if position >= key.0.len() {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "numeric projection exceeds index key width",
                ));
            }
            return Ok(key.0.remove(position));
        }
        let count = read_u8_checked(self.payload, cursor)? as usize;
        if position >= count {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "numeric projection exceeds index key width",
            ));
        }
        let mut value = KeyPart::Null;
        for part in 0..count {
            let tag = read_u8_checked(self.payload, cursor)?;
            if part != position {
                // Full-key decoding validates strings, including unprojected
                // parts. Keep that corruption check without allocating them.
                if tag == 5 || tag == 13 {
                    if tag == 13 {
                        let _ = read_u16_checked(self.payload, cursor)?;
                    }
                    let bytes = read_index_bytes_ref(self.payload, cursor)?;
                    std::str::from_utf8(bytes).map_err(|err| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            format!("corrupt index string key: {err}"),
                        )
                    })?;
                } else {
                    skip_generic_index_key_part(self.payload, cursor, tag)?;
                }
                continue;
            }
            value = match tag {
                0 => KeyPart::Null,
                1 => KeyPart::Int(read_i64_checked(self.payload, cursor)?),
                2 => KeyPart::UInt(read_u64_checked(self.payload, cursor)?),
                9 => KeyPart::Date(read_i32_checked(self.payload, cursor)?),
                10 => KeyPart::Time(read_i64_checked(self.payload, cursor)?),
                11 => KeyPart::DateTime(read_i64_checked(self.payload, cursor)?),
                12 => KeyPart::Year(read_u8_checked(self.payload, cursor)?),
                _ => {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "numeric projection encountered a non-numeric index key",
                    ))
                }
            };
        }
        Ok(value)
    }

    /// Decode one lossless integer part from a generic composite key while
    /// validating and skipping every other part. This is the projection
    /// counterpart to `decode_lossless_i64_at`: covering scans that need one
    /// integer column from a wide composite index avoid constructing an
    /// `IndexKey` and allocating/cloning every unreferenced `KeyPart`.
    #[inline]
    fn decode_lossless_i64_part_at(
        &self,
        cursor: &mut usize,
        key_position: usize,
    ) -> Result<Option<i64>, QueryError> {
        if self.key_layout == KEY_LAYOUT_COMPACT_INT2 {
            if key_position > 1 {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "integer leaf projection exceeds compact key width",
                ));
            }
            let aux = self.layout_aux.ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "INT2 layout missing prefix block at projection time",
                )
            })?;
            let first =
                self.base_time
                    .wrapping_add(read_int2_delta(self.payload, cursor, aux.width1)? as i64);
            let second =
                aux.col2_base
                    .wrapping_add(read_int2_delta(self.payload, cursor, aux.width2)? as i64);
            return Ok(Some(if key_position == 0 { first } else { second }));
        }
        if self.key_layout != KEY_LAYOUT_GENERIC {
            if key_position != 0 {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "integer leaf projection exceeds compact key width",
                ));
            }
            return self.decode_lossless_i64_at(cursor);
        }
        let part_count = read_u8_checked(self.payload, cursor)? as usize;
        if key_position >= part_count {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "integer leaf projection exceeds composite key width",
            ));
        }
        let mut projected = None;
        for part in 0..part_count {
            let tag = read_u8_checked(self.payload, cursor)?;
            if part == key_position {
                projected = match tag {
                    0 => None,
                    1 => Some(read_i64_checked(self.payload, cursor)?),
                    2 => Some(
                        i64::try_from(read_u64_checked(self.payload, cursor)?).map_err(|_| {
                            error(
                                crate::error::catalog::ER_GET_ERRNO,
                                "unsigned index key exceeds integer leaf projection range",
                            )
                        })?,
                    ),
                    9 => Some(i64::from(read_i32_checked(self.payload, cursor)?)),
                    10 | 11 => Some(read_i64_checked(self.payload, cursor)?),
                    12 => {
                        let value = read_u8_checked(self.payload, cursor)?;
                        Some(if value == 0 {
                            0
                        } else {
                            1900 + i64::from(value)
                        })
                    }
                    _ => {
                        return Err(error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "integer leaf projection called on unsupported key type",
                        ));
                    }
                };
                continue;
            }
            skip_generic_index_key_part(self.payload, cursor, tag)?;
        }
        Ok(projected)
    }
}

/// Compare one raw leaf slot with an index bound and leave the returned
/// cursor at the RowId. Compact scalar layouts avoid constructing an
/// `IndexKey`; generic layouts decode only the slots the range inspects.
fn compare_raw_leaf_slot_to_bound(
    hdr: &LeafPayloadHeader<'_>,
    slot: usize,
    bound: &IndexBound,
) -> Result<(std::cmp::Ordering, usize, bool), QueryError> {
    // Compact scalar layouts guarantee one key part. A generic page may
    // belong to a multi-column index probed by a one-part prefix, so decode
    // it to preserve the prefix-bound rules below.
    if hdr.key_layout != KEY_LAYOUT_GENERIC {
        if let Some(target) = hdr.fast_scalar_target(&bound.key) {
            let (ordering, cursor) = hdr.cmp_scalar_at(slot, target)?;
            return Ok((ordering, cursor, false));
        }
    }
    let mut cursor = hdr.read_slot_offset(slot);
    let key = hdr.decode_key_at(&mut cursor)?;
    let prefix_match = bound.key.0.len() < key.0.len() && index_key_starts_with(&key, &bound.key);
    Ok((key.cmp(&bound.key), cursor, prefix_match))
}

/// Resolve a logical resume key against the very snapshot being consumed.
/// A topology read lock alone does not exclude nonstructural leaf writers.
fn raw_leaf_resume_position(
    header: &LeafPayloadHeader<'_>,
    resume: &IndexEntry,
    forward: bool,
) -> Result<usize, QueryError> {
    let mut lo = 0;
    let mut hi = header.count;
    let target = header.fast_scalar_target(&resume.key);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let cmp = compare_raw_leaf_slot_to_entry_with_target(header, mid, resume, target)?;
        if cmp.is_lt() || (forward && cmp.is_eq()) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

/// One step of a root-to-leaf descent. The raw-page path returns the same
/// child slot as `internal_child_index_by_key` without materialising the
/// complete internal page (and without allocating one `IndexKey` per
/// separator).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RawIndexPageRoute {
    Leaf,
    Child { page_id: u64, child_idx: usize },
}

fn encode_leaf_entry_for_layout(
    entry: &IndexEntry,
    key_layout: u8,
    base_time: i64,
    layout_aux: Option<LayoutAux>,
) -> Result<Option<smallvec::SmallVec<[u8; 64]>>, QueryError> {
    let mut encoded = smallvec::SmallVec::<[u8; 64]>::new();
    Ok(
        encode_leaf_entry_for_layout_into(entry, key_layout, base_time, layout_aux, &mut encoded)?
            .then_some(encoded),
    )
}

/// Reuse capacity across a leaf run, including generic and wide composite keys.
/// Failed layout encodings only touch scratch; callers publish successful bytes.
fn encode_leaf_entry_for_layout_into(
    entry: &IndexEntry,
    key_layout: u8,
    base_time: i64,
    layout_aux: Option<LayoutAux>,
    encoded: &mut smallvec::SmallVec<[u8; 64]>,
) -> Result<bool, QueryError> {
    let capacity = encoded_index_entry_len(entry).saturating_add(32).max(32);
    encoded.clear();
    encoded.resize(capacity, 0);
    let mut offset = 0usize;
    let result = match key_layout {
        KEY_LAYOUT_GENERIC => encode_index_entry_into_page(entry, encoded, &mut offset),
        KEY_LAYOUT_COMPACT_TIME_U32 => {
            encode_compact_time_entry_into_page(entry, base_time, encoded, &mut offset)
        }
        KEY_LAYOUT_COMPACT_DATE_U16 => {
            encode_compact_date_entry_into_page(entry, base_time, encoded, &mut offset)
        }
        KEY_LAYOUT_COMPACT_DECIMAL_I32 => {
            encode_compact_decimal_i32_entry_into_page(entry, base_time, encoded, &mut offset)
        }
        KEY_LAYOUT_COMPACT_DECIMAL_I64 => {
            encode_compact_decimal_i64_entry_into_page(entry, base_time, encoded, &mut offset)
        }
        KEY_LAYOUT_COMPACT_INT_U16 => {
            encode_compact_int_u16_entry_into_page(entry, base_time, encoded, &mut offset)
        }
        KEY_LAYOUT_COMPACT_INT_U32 => {
            encode_compact_int_u32_entry_into_page(entry, base_time, encoded, &mut offset)
        }
        KEY_LAYOUT_COMPACT_UINT_U16 => {
            encode_compact_uint_u16_entry_into_page(entry, base_time, encoded, &mut offset)
        }
        KEY_LAYOUT_COMPACT_UINT_U32 => {
            encode_compact_uint_u32_entry_into_page(entry, base_time, encoded, &mut offset)
        }
        KEY_LAYOUT_COMPACT_INT2 => {
            let Some(aux) = layout_aux else {
                return Ok(false);
            };
            encode_compact_int2_entry_into_page(
                entry,
                base_time,
                aux.col2_base,
                aux.width1,
                aux.width2,
                encoded,
                &mut offset,
            )
        }
        _ => return Ok(false),
    };
    if result.is_err() {
        return Ok(false);
    }
    encoded.truncate(offset);
    Ok(true)
}

/// Replace one internal-page separator without materialising or repacking the
/// page. Internal separator records are a layout-encoded `IndexEntry` followed
/// by their fixed-width right-child id. When the replacement has the exact
/// same encoded length, overwriting the record preserves every slot offset and
/// all page-size/layout invariants. A different length or an entry outside the
/// page's compact-layout domain returns `None` so the decoded oracle can
/// re-layout, repack, or split the page.
fn try_replace_raw_internal_separator(
    buf: &mut [u8],
    separator_idx: usize,
    separator: &IndexEntry,
) -> Result<Option<()>, QueryError> {
    if buf.len() != PAGE_SIZE {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: short page",
        ));
    }
    let payload_len = read_u32(buf, 20) as usize;
    if payload_len < INTERNAL_HEADER_BYTES - INDEX_HEADER_BYTES
        || INDEX_HEADER_BYTES + payload_len > PAGE_SIZE
    {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: invalid internal payload length",
        ));
    }
    let payload = &buf[INDEX_HEADER_BYTES..INDEX_HEADER_BYTES + payload_len];
    if payload[INTERNAL_OFF_PAGE_TYPE] != 2 {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: expected internal page",
        ));
    }
    let key_layout = payload[INTERNAL_OFF_KEY_LAYOUT];
    let base_time = i64::from_le_bytes(
        payload[INTERNAL_OFF_BASE_TIME..INTERNAL_OFF_BASE_TIME + 8]
            .try_into()
            .unwrap(),
    );
    let key_count = u32::from_le_bytes(
        payload[INTERNAL_OFF_KEY_COUNT..INTERNAL_OFF_KEY_COUNT + 4]
            .try_into()
            .unwrap(),
    ) as usize;
    if separator_idx >= key_count {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: separator slot out of range",
        ));
    }
    let mut records_start = INTERNAL_OFF_KEY_COUNT + 4;
    let layout_aux = if key_layout == KEY_LAYOUT_COMPACT_INT2 {
        let (col2_base, width1, width2) = decode_int2_prefix(payload, &mut records_start)?;
        Some(LayoutAux {
            col2_base,
            width1,
            width2,
        })
    } else {
        None
    };
    let slot_bytes = key_count.checked_mul(SLOT_OFFSET_BYTES).ok_or_else(|| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: internal slot count overflow",
        )
    })?;
    let slot_array_start = payload_len.checked_sub(slot_bytes).ok_or_else(|| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: internal slot array underflow",
        )
    })?;
    if slot_array_start < records_start {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: internal slot array overlaps header",
        ));
    }
    let slot_pos = slot_array_start + separator_idx * SLOT_OFFSET_BYTES;
    let record_offset =
        u16::from_le_bytes(payload[slot_pos..slot_pos + 2].try_into().unwrap()) as usize;
    let record_len =
        u16::from_le_bytes(payload[slot_pos + 2..slot_pos + 4].try_into().unwrap()) as usize;
    let record_end = record_offset.checked_add(record_len).ok_or_else(|| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: internal separator record overflow",
        )
    })?;
    if record_len <= 8 || record_offset < records_start || record_end > slot_array_start {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: internal separator record bounds",
        ));
    }

    let Some(encoded) = encode_leaf_entry_for_layout(separator, key_layout, base_time, layout_aux)?
    else {
        return Ok(None);
    };
    if encoded.len().checked_add(8) != Some(record_len) {
        return Ok(None);
    }

    // Preserve the separator-local right child. Only the key/RowId prefix is
    // replaced; the topology remains exactly the same.
    let absolute_start = INDEX_HEADER_BYTES + record_offset;
    let key_end = absolute_start + encoded.len();
    buf[absolute_start..key_end].copy_from_slice(&encoded);
    Ok(Some(()))
}

fn write_raw_index_slot(buf: &mut [u8], offset: usize, slot: IndexRawSlot) {
    write_u16(buf, offset, slot.offset);
    write_u16(buf, offset + 2, slot.len);
}

fn raw_leaf_slots_and_position<'a>(
    buf: &'a [u8],
    entry: &IndexEntry,
) -> Result<(LeafPayloadHeader<'a>, Result<usize, usize>), QueryError> {
    let header = parse_leaf_payload_header(buf)?;
    let position = raw_leaf_position(&header, entry)?;
    Ok((header, position))
}

#[inline]
fn raw_leaf_position(
    header: &LeafPayloadHeader<'_>,
    entry: &IndexEntry,
) -> Result<Result<usize, usize>, QueryError> {
    let target = header.fast_scalar_target(&entry.key);
    let compare_at =
        |slot: usize| compare_raw_leaf_slot_to_entry_with_target(&header, slot, entry, target);
    let mut lo = 0usize;
    let mut hi = header.count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        match compare_at(mid)? {
            std::cmp::Ordering::Less => lo = mid + 1,
            _ => hi = mid,
        }
    }
    let found = if lo < header.count {
        compare_at(lo)? == std::cmp::Ordering::Equal
    } else {
        false
    };
    Ok(if found { Ok(lo) } else { Err(lo) })
}

/// Sparse batch inserts choose their fixed-width comparator once per leaf.
/// Point INSERT keeps the existing kernel. Unrepresentable targets and mixed
/// layouts retain its wrapping, generic-key and corruption semantics.
fn sparse_leaf_slots_and_position<'a>(
    buf: &'a [u8],
    entry: &IndexEntry,
) -> Result<(LeafPayloadHeader<'a>, Result<usize, usize>), QueryError> {
    let header = parse_leaf_payload_header(buf)?;
    let delta = |base: i64, value: i64, width: usize| {
        let max = (1i64 << (width * 8)) - 1;
        base.checked_add(max)?;
        let delta = i128::from(value) - i128::from(base);
        (delta >= 0 && delta <= i128::from(max)).then_some(delta as u64)
    };
    let unsigned_delta = |base: i64, value: u64, width: usize| {
        let base = ordered_i64_to_uint(base);
        let max = (1u64 << (width * 8)) - 1;
        base.checked_add(max)?;
        value.checked_sub(base).filter(|d| *d <= max)
    };
    let scalar = match (header.key_layout, entry.key.0.as_slice()) {
        (KEY_LAYOUT_COMPACT_INT_U16, [KeyPart::Int(value)]) => {
            delta(header.base_time, *value, 2).map(|d| (d, 2))
        }
        (KEY_LAYOUT_COMPACT_INT_U32, [KeyPart::Int(value)]) => {
            delta(header.base_time, *value, 4).map(|d| (d, 4))
        }
        (KEY_LAYOUT_COMPACT_UINT_U16, [KeyPart::UInt(value)]) => {
            unsigned_delta(header.base_time, *value, 2).map(|d| (d, 2))
        }
        (KEY_LAYOUT_COMPACT_UINT_U32, [KeyPart::UInt(value)]) => {
            unsigned_delta(header.base_time, *value, 4).map(|d| (d, 4))
        }
        (KEY_LAYOUT_COMPACT_DATE_U16, [KeyPart::Date(value)])
            if header.base_time >= i64::from(i32::MIN)
                && header.base_time <= i64::from(i32::MAX) - i64::from(u16::MAX) =>
        {
            delta(header.base_time, i64::from(*value), 2).map(|d| (d, 2))
        }
        (KEY_LAYOUT_COMPACT_TIME_U32, [KeyPart::DateTime(value)]) => {
            delta(header.base_time, *value, 4).map(|d| (d, 4))
        }
        _ => None,
    };
    let position = if let Some((target, width)) = scalar {
        match width {
            2 => sparse_compact_position::<2, 0>(&header, target, entry.row_id)?,
            4 => sparse_compact_position::<4, 0>(&header, target, entry.row_id)?,
            _ => unreachable!(),
        }
    } else if let (KEY_LAYOUT_COMPACT_INT2, [KeyPart::Int(a), KeyPart::Int(b)], Some(aux)) =
        (header.key_layout, entry.key.0.as_slice(), header.layout_aux)
    {
        if let (Some(a), Some(b)) = (
            delta(header.base_time, *a, aux.width1 as usize),
            delta(aux.col2_base, *b, aux.width2 as usize),
        ) {
            let target = (a << (aux.width2 * 8)) | b;
            match (aux.width1, aux.width2) {
                (1, 1) => sparse_compact_position::<1, 1>(&header, target, entry.row_id)?,
                (1, 2) => sparse_compact_position::<1, 2>(&header, target, entry.row_id)?,
                (1, 4) => sparse_compact_position::<1, 4>(&header, target, entry.row_id)?,
                (2, 1) => sparse_compact_position::<2, 1>(&header, target, entry.row_id)?,
                (2, 2) => sparse_compact_position::<2, 2>(&header, target, entry.row_id)?,
                (2, 4) => sparse_compact_position::<2, 4>(&header, target, entry.row_id)?,
                (4, 1) => sparse_compact_position::<4, 1>(&header, target, entry.row_id)?,
                (4, 2) => sparse_compact_position::<4, 2>(&header, target, entry.row_id)?,
                (4, 4) => sparse_compact_position::<4, 4>(&header, target, entry.row_id)?,
                _ => {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "invalid compact integer widths",
                    ))
                }
            }
        } else {
            raw_leaf_position(&header, entry)?
        }
    } else {
        raw_leaf_position(&header, entry)?
    };
    Ok((header, position))
}

/// Packed deltas preserve lexicographic order only after the caller proves
/// that neither component wraps. Validate each visited record before loading;
/// decode the RowId only for equal keys and reuse the last lower-bound probe.
#[inline]
fn sparse_compact_position<const FIRST: usize, const SECOND: usize>(
    header: &LeafPayloadHeader<'_>,
    target: u64,
    row_id: RowId,
) -> Result<Result<usize, usize>, QueryError> {
    let mut lo = 0;
    let mut hi = header.count;
    let mut equal = false;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let slot = header.read_slot(mid);
        let start = usize::from(slot.offset);
        let end = start + FIRST + SECOND;
        if usize::from(slot.len) < FIRST + SECOND + ROW_ID_V7_ENCODED_LEN
            || end + ROW_ID_V7_ENCODED_LEN > header.slot_array_start
        {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: truncated compact entry",
            ));
        }
        let bytes = &header.payload[start..end];
        let mut first = 0u64;
        let mut second = 0u64;
        for (i, byte) in bytes[..FIRST].iter().enumerate() {
            first |= u64::from(*byte) << (i * 8);
        }
        for (i, byte) in bytes[FIRST..].iter().enumerate() {
            second |= u64::from(*byte) << (i * 8);
        }
        let key = (first << (SECOND * 8)) | second;
        let ordering = match key.cmp(&target) {
            std::cmp::Ordering::Equal => {
                let mut cursor = end;
                decode_row_id_v7_from(header.payload, &mut cursor, 0)?.cmp(&row_id)
            }
            ordering => ordering,
        };
        if ordering == std::cmp::Ordering::Less {
            lo = mid + 1;
        } else {
            hi = mid;
            equal = ordering == std::cmp::Ordering::Equal;
        }
    }
    Ok(if equal { Ok(lo) } else { Err(lo) })
}

#[inline]
fn compare_raw_leaf_slot_to_entry(
    header: &LeafPayloadHeader<'_>,
    slot: usize,
    entry: &IndexEntry,
) -> Result<std::cmp::Ordering, QueryError> {
    compare_raw_leaf_slot_to_entry_with_target(
        header,
        slot,
        entry,
        header.fast_scalar_target(&entry.key),
    )
}

#[inline]
fn compare_raw_leaf_slot_to_entry_with_target(
    header: &LeafPayloadHeader<'_>,
    slot: usize,
    entry: &IndexEntry,
    target: Option<FastScalarTarget>,
) -> Result<std::cmp::Ordering, QueryError> {
    // Raw INSERT/DELETE positioning used to bypass the same compact-scalar
    // comparator already used by lookups. Every binary-search probe therefore
    // decoded an `IndexKey` and allocated its part container for the dominant
    // one/two-integer layouts. Generic keys already compare directly from the
    // page; compact integer keys can do the same and still leave `cursor`
    // exactly at the encoded RowId for duplicate-key ordering.
    let (key_order, mut cursor) = match target {
        Some(target) => header.cmp_scalar_at(slot, target)?,
        None => header.cmp_key_at(slot, &entry.key)?,
    };
    if key_order != std::cmp::Ordering::Equal {
        return Ok(key_order);
    }
    let row_id = decode_row_id_v7_from(header.payload, &mut cursor, 0)?;
    Ok(row_id.cmp(&entry.row_id))
}

/// Decide the sorted right-edge append gate from the final live slot only.
/// The old path decoded the entire rightmost leaf into `IndexPage` merely to
/// clone its last entry. This preserves the exact `IndexEntry` total order
/// (key then RowId) while allocating nothing for compact integer layouts and
/// decoding at most one generic key.
fn raw_last_leaf_entry_less_than(buf: &[u8], candidate: &IndexEntry) -> Result<bool, QueryError> {
    let header = parse_leaf_payload_header(buf)?;
    if header.count == 0 {
        return Ok(true);
    }
    Ok(
        compare_raw_leaf_slot_to_entry(&header, header.count - 1, candidate)?
            == std::cmp::Ordering::Less,
    )
}

/// Whether `candidate` is provably routed inside this leaf's observed key
/// interval. This is the raw-page equivalent of comparing against the first
/// and last decoded `IndexEntry`: it compares only those two slots and
/// materializes no page-wide key vector.
fn raw_leaf_candidate_stays(buf: &[u8], candidate: &IndexEntry) -> Result<bool, QueryError> {
    let header = parse_leaf_payload_header(buf)?;
    if header.count == 0 {
        return Ok(false);
    }
    let first_order = compare_raw_leaf_slot_to_entry(&header, 0, candidate)?;
    let last_order = compare_raw_leaf_slot_to_entry(&header, header.count - 1, candidate)?;
    let within_observed_range =
        first_order != std::cmp::Ordering::Greater && last_order != std::cmp::Ordering::Less;
    let rightmost_extension =
        header.next_leaf.is_none() && last_order != std::cmp::Ordering::Greater;
    Ok(within_observed_range || rightmost_extension)
}

/// Merge a sorted input prefix into one leaf without moving its slot directory
/// once per record. The caller has routed the first entry and holds the page
/// latch plus the shared topology guard. All fallible work is staged before
/// touching `buf`, so an error cannot bypass the buffer pool's dirty marking.
#[cfg(test)]
fn try_insert_raw_leaf_run(
    buf: &mut [u8],
    run: SortedIndexEntryRun<'_>,
    txn_id: u64,
) -> Result<(bool, Option<usize>), QueryError> {
    try_insert_raw_leaf_run_with_bound::<false>(buf, run, txn_id)
}

/// With ROUTED, the caller proves the entire run belongs to this leaf under
/// the held topology guard. Otherwise only its first entry was routed, and
/// the observed maximum remains the conservative range proof.
// Do not inline batch staging state into the buffer-pool mutation callback:
// single-row writes share that callback and must retain their small frame.
#[inline(never)]
fn try_insert_raw_leaf_run_with_bound<const ROUTED: bool>(
    buf: &mut [u8],
    run: SortedIndexEntryRun<'_>,
    txn_id: u64,
) -> Result<(bool, Option<usize>), QueryError> {
    let entries = run.0;
    let Some(first) = entries.first() else {
        return Ok((false, None));
    };
    let single = |buf: &mut [u8]| {
        let result = try_insert_raw_leaf(buf, first, txn_id)?;
        Ok((
            matches!(result, Some(InPlaceInsert::Inserted)),
            matches!(
                result,
                Some(InPlaceInsert::Inserted | InPlaceInsert::Duplicate)
            )
            .then_some(1),
        ))
    };
    if entries.len() == 1 {
        return single(buf);
    }
    let header = parse_leaf_payload_header(buf)?;
    // A sorted input can only extend the upper bound. The first routed entry
    // may extend the old observed range; its exact duplicates remain safe.
    let stays = |entry: &IndexEntry| -> Result<bool, QueryError> {
        Ok(header.next_leaf.is_none()
            || (entry.row_id == first.row_id && entry.key == first.key)
            || (header.count > 0
                && compare_raw_leaf_slot_to_entry(&header, header.count - 1, entry)?
                    != std::cmp::Ordering::Less))
    };
    if !ROUTED && !stays(&entries[1])? {
        return single(buf);
    }

    let mut records = smallvec::SmallVec::<[u8; 256]>::new();
    // Positions refer to the original slot directory, not to newly inserted
    // records. Equal positions are merged in input order below.
    let mut insertions = smallvec::SmallVec::<[(usize, IndexRawSlot); 16]>::new();
    let mut encoded = smallvec::SmallVec::<[u8; 64]>::new();
    let mut consumed = 0usize;
    let mut lower = 0usize;
    for entry in entries {
        if consumed > 0 {
            // Ordering was validated before taking the leaf latch. Distinct
            // rows cannot be duplicate entries, so avoid redundant key or
            // collation comparisons on the common path.
            let previous = &entries[consumed - 1];
            if entry.row_id == previous.row_id && entry.key == previous.key {
                consumed += 1;
                continue;
            }
        }
        let mut lo = lower;
        let mut hi = header.count;
        let target = header.fast_scalar_target(&entry.key);
        let compare_at =
            |slot| compare_raw_leaf_slot_to_entry_with_target(&header, slot, entry, target);
        // The previous insertion position is a lower bound for this sorted
        // input. Dense generic runs often keep that exact position (e.g. new
        // RowIds for one secondary key, before the next distinct key). One
        // probe then replaces a full suffix search and the duplicate probe.
        // Sparse runs retain bounded binary search after discarding that slot.
        let mut position_order = None;
        if header.key_layout == KEY_LAYOUT_GENERIC && consumed > 0 && lo < hi {
            let order = compare_at(lo)?;
            if order == std::cmp::Ordering::Less {
                lo += 1;
            } else {
                hi = lo;
                position_order = Some(order);
            }
        }
        // Consecutive sorted keys usually land near the previous position.
        // Exponential search preserves logarithmic work for sparse runs while
        // avoiding a full suffix binary search for every dense insertion.
        // Generic keys may perform expensive collation/variable-width work;
        // keep their bounded binary search instead of adding sparse probes.
        if header.key_layout != KEY_LAYOUT_GENERIC && consumed > 0 && lo < hi {
            let base = lo;
            let mut step = 1usize;
            hi = lo;
            while hi < header.count && compare_at(hi)? == std::cmp::Ordering::Less {
                lo = hi + 1;
                hi = base.saturating_add(step).min(header.count);
                step = step.saturating_mul(2);
            }
        }
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if compare_at(mid)? == std::cmp::Ordering::Less {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        // For a non-rightmost leaf, only the routed first key may extend
        // its observed upper bound. Adjacent exact duplicates were handled
        // above. The insertion position already proves the range boundary,
        // avoiding another decode/compare of the final slot for every key.
        if !ROUTED && consumed > 0 && lo == header.count && header.next_leaf.is_some() {
            break;
        }
        lower = lo;
        if lo < header.count
            && match position_order {
                Some(order) => order,
                None => compare_at(lo)?,
            } == std::cmp::Ordering::Equal
        {
            consumed += 1;
            continue;
        }
        if !encode_leaf_entry_for_layout_into(
            entry,
            header.key_layout,
            header.base_time,
            header.layout_aux,
            &mut encoded,
        )? {
            break;
        };
        let payload_len = header.payload.len()
            + records.len()
            + encoded.len()
            + (insertions.len() + 1) * SLOT_OFFSET_BYTES;
        if INDEX_HEADER_BYTES + payload_len > PAGE_SIZE {
            break;
        }
        insertions.push((
            lo,
            IndexRawSlot {
                offset: (header.slot_array_start + records.len()) as u16,
                len: encoded.len() as u16,
            },
        ));
        records.extend_from_slice(&encoded);
        consumed += 1;
    }
    if insertions.is_empty() {
        return Ok((false, (consumed > 0).then_some(consumed)));
    }

    let count = header.count;
    let old_start = INDEX_HEADER_BYTES + header.slot_array_start;
    let new_start = old_start + records.len();
    let new_payload_len =
        header.payload.len() + records.len() + insertions.len() * SLOT_OFFSET_BYTES;
    // Merge backwards directly from the old directory into its final home.
    // Every destination is above its source, so already moved suffixes and
    // newly written slots cannot overwrite the still unread prefix. Avoid
    // first relocating the entire directory only to move each suffix again.
    let mut old_end = count;
    let mut new_end = count + insertions.len();
    for &(position, slot) in insertions.iter().rev() {
        debug_assert!(
            position <= old_end,
            "sorted run insertion positions must be monotonic"
        );
        let run = old_end - position;
        new_end -= run;
        if run > 0 {
            buf.copy_within(
                old_start + position * SLOT_OFFSET_BYTES..old_start + old_end * SLOT_OFFSET_BYTES,
                new_start + new_end * SLOT_OFFSET_BYTES,
            );
        }
        new_end -= 1;
        write_raw_index_slot(buf, new_start + new_end * SLOT_OFFSET_BYTES, slot);
        old_end = position;
    }
    debug_assert_eq!(new_end, old_end);
    if old_end > 0 {
        buf.copy_within(
            old_start..old_start + old_end * SLOT_OFFSET_BYTES,
            new_start,
        );
    }
    buf[old_start..new_start].copy_from_slice(&records);
    write_u16(
        buf,
        INDEX_HEADER_BYTES + LEAF_OFF_ENTRY_COUNT,
        (count + insertions.len()) as u16,
    );
    let writer_offset = INDEX_HEADER_BYTES + LEAF_OFF_PAGE_MAX_TRX_ID;
    write_u64(buf, writer_offset, read_u64(buf, writer_offset).max(txn_id));
    write_u32(buf, 20, new_payload_len as u32);
    Ok((true, Some(consumed)))
}

/// Append a newly encoded record immediately before the trailing slot array
/// and insert its offset/length slot into logical key order. Record bodies do
/// not need to be physically sorted: every reader already addresses them via
/// the slot array.  Deletes leave body holes; the existing decoded/re-encode
/// slow path compacts those holes when contiguous space is exhausted.
#[inline]
fn try_insert_raw_leaf(
    buf: &mut [u8],
    entry: &IndexEntry,
    txn_id: u64,
) -> Result<Option<InPlaceInsert>, QueryError> {
    try_insert_raw_leaf_search::<false>(buf, entry, txn_id)
}

fn try_insert_raw_leaf_search<const SPARSE: bool>(
    buf: &mut [u8],
    entry: &IndexEntry,
    txn_id: u64,
) -> Result<Option<InPlaceInsert>, QueryError> {
    let (key_layout, base_time, layout_aux, count, old_payload_len, old_slot_start, position) = {
        let (header, position) = if SPARSE {
            sparse_leaf_slots_and_position(buf, entry)?
        } else {
            raw_leaf_slots_and_position(buf, entry)?
        };
        (
            header.key_layout,
            header.base_time,
            header.layout_aux,
            header.count,
            read_u32(buf, 20) as usize,
            header.slot_array_start,
            position,
        )
    };
    let insert_pos = match position {
        Ok(_) => return Ok(Some(InPlaceInsert::Duplicate)),
        Err(pos) => pos,
    };
    let Some(encoded) = encode_leaf_entry_for_layout(entry, key_layout, base_time, layout_aux)?
    else {
        return Ok(None);
    };
    let new_payload_len = old_payload_len
        .checked_add(encoded.len() + SLOT_OFFSET_BYTES)
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                "index leaf size overflow",
            )
        })?;
    if INDEX_HEADER_BYTES + new_payload_len > PAGE_SIZE {
        return try_insert_compacted_raw_leaf(buf, &encoded, insert_pos, txn_id);
    }
    let entry_offset = old_slot_start;
    let new_slot_start = old_slot_start + encoded.len();
    let payload_origin = INDEX_HEADER_BYTES;
    let old_slots_abs = payload_origin + old_slot_start;
    let new_slots_abs = payload_origin + new_slot_start;
    let insert_abs = new_slots_abs + insert_pos * SLOT_OFFSET_BYTES;
    // Move the suffix first, then the prefix, directly to their final
    // positions. Moving the whole directory followed by the suffix copies
    // the suffix twice on every single-entry insert into a populated leaf.
    buf.copy_within(
        old_slots_abs + insert_pos * SLOT_OFFSET_BYTES..old_slots_abs + count * SLOT_OFFSET_BYTES,
        insert_abs + SLOT_OFFSET_BYTES,
    );
    buf.copy_within(
        old_slots_abs..old_slots_abs + insert_pos * SLOT_OFFSET_BYTES,
        new_slots_abs,
    );
    write_raw_index_slot(
        buf,
        insert_abs,
        IndexRawSlot {
            offset: entry_offset as u16,
            len: encoded.len() as u16,
        },
    );
    buf[payload_origin + entry_offset..payload_origin + entry_offset + encoded.len()]
        .copy_from_slice(&encoded);
    write_u16(
        buf,
        payload_origin + LEAF_OFF_ENTRY_COUNT,
        (count + 1) as u16,
    );
    write_u64(
        buf,
        payload_origin + LEAF_OFF_PAGE_MAX_TRX_ID,
        read_u64(buf, payload_origin + LEAF_OFF_PAGE_MAX_TRX_ID).max(txn_id),
    );
    write_u32(buf, 20, new_payload_len as u32);
    Ok(Some(InPlaceInsert::Inserted))
}

/// Reclaim deleted record bodies without decoding live keys under the page
/// write latch. Retain every layout and stage the complete image before mutation.
fn try_insert_compacted_raw_leaf(
    buf: &mut [u8],
    encoded: &[u8],
    insert_pos: usize,
    txn_id: u64,
) -> Result<Option<InPlaceInsert>, QueryError> {
    let (live_size, count) = raw_leaf_live_encoded_len_and_count(buf)?;
    let new_size = live_size + encoded.len() + SLOT_OFFSET_BYTES;
    if new_size > PAGE_SIZE {
        return Ok(Some(InPlaceInsert::NeedsSplit));
    }
    // The caller owns the page write latch. Never reclaim storage recursively
    // here; a failed scratch admission leaves the original fallback available.
    let _admission = crate::engine::executor::mem_budget::StorageLatchAdmissionScope::enter();
    let Ok(_scratch) = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
        PAGE_SIZE,
    ) else {
        return Ok(None);
    };
    let header = parse_leaf_payload_header(buf)?;
    let records_start = LEAF_OFF_ENTRIES
        + usize::from(header.key_layout == KEY_LAYOUT_COMPACT_INT2) * COMPACT_INT2_PREFIX_BYTES;
    let directory = new_size - INDEX_HEADER_BYTES - (count + 1) * SLOT_OFFSET_BYTES;
    let mut image = Vec::new();
    if image.try_reserve_exact(PAGE_SIZE).is_err() {
        return Ok(None);
    }
    image.resize(PAGE_SIZE, 0);
    image[..INDEX_HEADER_BYTES + records_start]
        .copy_from_slice(&buf[..INDEX_HEADER_BYTES + records_start]);
    let mut offset = records_start;
    for dest in 0..=count {
        let record = if dest == insert_pos {
            encoded
        } else {
            let slot = header.read_slot(dest - usize::from(dest > insert_pos));
            &header.payload[slot.offset as usize..slot.offset as usize + slot.len as usize]
        };
        image[INDEX_HEADER_BYTES + offset..INDEX_HEADER_BYTES + offset + record.len()]
            .copy_from_slice(record);
        write_raw_index_slot(
            &mut image,
            INDEX_HEADER_BYTES + directory + dest * SLOT_OFFSET_BYTES,
            IndexRawSlot {
                offset: offset as u16,
                len: record.len() as u16,
            },
        );
        offset += record.len();
    }
    debug_assert_eq!(offset, directory);
    write_u16(
        &mut image,
        INDEX_HEADER_BYTES + LEAF_OFF_ENTRY_COUNT,
        (count + 1) as u16,
    );
    write_u64(
        &mut image,
        INDEX_HEADER_BYTES + LEAF_OFF_PAGE_MAX_TRX_ID,
        header.page_max_trx_id.max(txn_id),
    );
    write_u32(&mut image, 20, (new_size - INDEX_HEADER_BYTES) as u32);
    buf.copy_from_slice(&image);
    Ok(Some(InPlaceInsert::Inserted))
}

fn try_delete_raw_leaf(
    buf: &mut [u8],
    entry: &IndexEntry,
    txn_id: u64,
) -> Result<Option<InPlaceDelete>, QueryError> {
    let (count, old_payload_len, slot_start, removed, position) = {
        let (header, position) = raw_leaf_slots_and_position(buf, entry)?;
        let removed = position.ok().map(|pos| header.read_slot(pos));
        (
            header.count,
            read_u32(buf, 20) as usize,
            header.slot_array_start,
            removed,
            position,
        )
    };
    let Ok(pos) = position else {
        return Ok(Some(InPlaceDelete::NotFound));
    };
    let payload_origin = INDEX_HEADER_BYTES;
    let removed = removed.expect("position and slot captured together");
    let removed_start = payload_origin + removed.offset as usize;
    let removed_end = removed_start + removed.len as usize;
    if removed_end > payload_origin + slot_start {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: slot record bounds",
        ));
    }
    delete_raw_leaf_prepared(
        buf,
        count,
        old_payload_len,
        slot_start,
        removed,
        pos,
        txn_id,
    );
    Ok(Some(InPlaceDelete::Deleted {
        was_first: pos == 0,
    }))
}

/// Infallible commit after slot lookup and record-bound validation.
fn delete_raw_leaf_prepared(
    buf: &mut [u8],
    count: usize,
    old_payload_len: usize,
    slot_start: usize,
    removed: IndexRawSlot,
    pos: usize,
    txn_id: u64,
) {
    let payload_origin = INDEX_HEADER_BYTES;
    let removed_start = payload_origin + removed.offset as usize;
    let removed_end = removed_start + removed.len as usize;
    buf[removed_start..removed_end].fill(0);
    let slots_abs = payload_origin + slot_start;
    let remove_abs = slots_abs + pos * SLOT_OFFSET_BYTES;
    buf.copy_within(
        remove_abs + SLOT_OFFSET_BYTES..slots_abs + count * SLOT_OFFSET_BYTES,
        remove_abs,
    );
    let new_payload_len = old_payload_len.saturating_sub(SLOT_OFFSET_BYTES);
    buf[payload_origin + new_payload_len..payload_origin + old_payload_len].fill(0);
    write_u16(
        buf,
        payload_origin + LEAF_OFF_ENTRY_COUNT,
        (count - 1) as u16,
    );
    write_u64(
        buf,
        payload_origin + LEAF_OFF_PAGE_MAX_TRX_ID,
        read_u64(buf, payload_origin + LEAF_OFF_PAGE_MAX_TRX_ID).max(txn_id),
    );
    write_u32(buf, 20, new_payload_len as u32);
}

/// Preflight variant used under a shared topology guard. `None` means the
/// entry exists but deleting it requires separator/underflow maintenance; no
/// byte has been changed in that case. A root leaf has no parent invariant and
/// may become sparse or empty in place.
fn try_delete_raw_leaf_nonstructural(
    buf: &mut [u8],
    entry: &IndexEntry,
    txn_id: u64,
    is_root: bool,
) -> Result<Option<InPlaceDelete>, QueryError> {
    let (header, position) = raw_leaf_slots_and_position(buf, entry)?;
    let Ok(pos) = position else {
        return Ok(Some(InPlaceDelete::NotFound));
    };
    if !is_root {
        let removed = header.read_slot(pos);
        let (live_size, count) = raw_leaf_live_encoded_len_and_count(buf)?;
        let live_after = live_size
            .saturating_sub(removed.len as usize)
            .saturating_sub(SLOT_OFFSET_BYTES);
        if count <= 1 || live_after < INDEX_MIN_PAGE_BYTES {
            return Ok(None);
        }
    }
    try_delete_raw_leaf(buf, entry, txn_id)
}

fn try_reposition_raw_leaf(
    buf: &mut [u8],
    old_entry: &IndexEntry,
    new_entry: &IndexEntry,
    txn_id: u64,
    may_change_first: bool,
) -> Result<Option<bool>, QueryError> {
    let (header, old_position) = raw_leaf_slots_and_position(buf, old_entry)?;
    let old_pos = {
        let Ok(pos) = old_position else {
            return Ok(Some(false));
        };
        pos
    };
    if old_entry == new_entry {
        return Ok(Some(false));
    }
    // Index-key updates are usually small numeric drifts (counter increments,
    // timestamps, state transitions). Search a bounded neighbourhood around
    // the old slot first; if the key moved farther, binary-search only the
    // remaining prefix/suffix. This preserves O(log N) worst-case work while
    // avoiding a second full-page binary search for the common adjacent move.
    const LOCAL_REPOSITION_PROBES: usize = 8;
    let lower_bound = |mut lo: usize, mut hi: usize| -> Result<usize, QueryError> {
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if compare_raw_leaf_slot_to_entry(&header, mid, new_entry)? == std::cmp::Ordering::Less
            {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    };
    let insertion_pos = match new_entry.cmp(old_entry) {
        std::cmp::Ordering::Greater => {
            let mut cursor = old_pos + 1;
            let limit = (cursor + LOCAL_REPOSITION_PROBES).min(header.count);
            while cursor < limit
                && compare_raw_leaf_slot_to_entry(&header, cursor, new_entry)?
                    == std::cmp::Ordering::Less
            {
                cursor += 1;
            }
            if cursor < limit {
                cursor
            } else {
                lower_bound(cursor, header.count)?
            }
        }
        std::cmp::Ordering::Less => {
            let mut cursor = old_pos;
            let mut probes = 0usize;
            while cursor > 0 && probes < LOCAL_REPOSITION_PROBES {
                let candidate = cursor - 1;
                match compare_raw_leaf_slot_to_entry(&header, candidate, new_entry)? {
                    std::cmp::Ordering::Greater => {
                        cursor = candidate;
                        probes += 1;
                    }
                    std::cmp::Ordering::Less | std::cmp::Ordering::Equal => {
                        cursor = candidate;
                        break;
                    }
                }
            }
            if cursor == 0
                || compare_raw_leaf_slot_to_entry(&header, cursor, new_entry)?
                    != std::cmp::Ordering::Greater
            {
                if cursor < old_pos
                    && compare_raw_leaf_slot_to_entry(&header, cursor, new_entry)?
                        == std::cmp::Ordering::Less
                {
                    cursor + 1
                } else {
                    cursor
                }
            } else {
                lower_bound(0, cursor)?
            }
        }
        std::cmp::Ordering::Equal => return Ok(Some(false)),
    };
    if insertion_pos < header.count
        && compare_raw_leaf_slot_to_entry(&header, insertion_pos, new_entry)?
            == std::cmp::Ordering::Equal
    {
        return Ok(Some(false));
    }
    let mut new_pos = insertion_pos;
    if old_pos < new_pos {
        new_pos -= 1;
    }
    if !may_change_first && new_pos == 0 && (old_pos != 0 || new_entry < old_entry) {
        return Ok(None);
    }
    let Some(encoded) = encode_leaf_entry_for_layout(
        new_entry,
        header.key_layout,
        header.base_time,
        header.layout_aux,
    )?
    else {
        return Ok(None);
    };
    let removed = header.read_slot(old_pos);
    let removed_start = removed.offset as usize;
    let removed_end = removed_start
        .checked_add(removed.len as usize)
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: slot record overflow",
            )
        })?;
    if removed_end > header.slot_array_start {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: slot record bounds",
        ));
    }
    let old_slot_array_start = header.slot_array_start;
    let old_payload_len = read_u32(buf, 20) as usize;
    let new_payload_len = old_payload_len
        .checked_sub(removed.len as usize)
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: leaf payload underflow",
            )
        })?
        .checked_add(encoded.len())
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                "index leaf size overflow",
            )
        })?;
    if INDEX_HEADER_BYTES + new_payload_len > PAGE_SIZE {
        return Ok(None);
    }
    let count = header.count;
    // Equal-width replacements do not need to repack the record area. The
    // record's physical offset is independent of its logical ordering (the
    // slot array owns that), so overwrite it in place and move only its
    // four-byte slot to the new sorted position. Integer/date/time updates
    // normally stay in the same compact layout and therefore hit this path.
    // Repacking those updates copied most of a 16 KiB leaf and then adjusted
    // every slot offset even though no offset changed.
    if encoded.len() == removed.len as usize {
        let payload_origin = INDEX_HEADER_BYTES;
        buf[payload_origin + removed_start..payload_origin + removed_end].copy_from_slice(&encoded);

        let slots_abs = payload_origin + old_slot_array_start;
        let remove_abs = slots_abs + old_pos * SLOT_OFFSET_BYTES;
        let insert_abs = slots_abs + new_pos * SLOT_OFFSET_BYTES;
        // Rotate only the slots between the old and new positions. Equal
        // positions need no movement; distant suffixes stay untouched.
        if old_pos < new_pos {
            buf.copy_within(
                remove_abs + SLOT_OFFSET_BYTES..insert_abs + SLOT_OFFSET_BYTES,
                remove_abs,
            );
        } else if new_pos < old_pos {
            buf.copy_within(insert_abs..remove_abs, insert_abs + SLOT_OFFSET_BYTES);
        }
        write_raw_index_slot(buf, insert_abs, removed);
        write_u64(
            buf,
            payload_origin + LEAF_OFF_PAGE_MAX_TRX_ID,
            read_u64(buf, payload_origin + LEAF_OFF_PAGE_MAX_TRX_ID).max(txn_id),
        );
        return Ok(Some(true));
    }
    let entry_offset = old_slot_array_start - removed.len as usize;
    let slot_start = entry_offset + encoded.len();
    let payload_origin = INDEX_HEADER_BYTES;
    let old_slots_abs = payload_origin + header.slot_array_start;
    let new_slots_abs = payload_origin + slot_start;
    // Reclaim the replaced record rather than leaving an unreachable hole in
    // the record area. The previous implementation appended the replacement
    // before the slot array and only removed the old slot, so every UPDATE
    // grew payload_len by the whole new record. Repeated nullable-key updates
    // (e.g. TPC-C o_carrier_id NULL -> INT) eventually raised a spurious 1118
    // even while the logical entries still fit.
    buf.copy_within(
        payload_origin + removed_end..payload_origin + old_slot_array_start,
        payload_origin + removed_start,
    );
    // Compact records first: a shrinking replacement moves the slot array
    // into the old record area. After compaction, both array destinations are
    // above the live records. Move slots before writing a growing replacement.
    buf.copy_within(
        old_slots_abs..old_slots_abs + count * SLOT_OFFSET_BYTES,
        new_slots_abs,
    );
    buf[payload_origin + entry_offset..payload_origin + entry_offset + encoded.len()]
        .copy_from_slice(&encoded);
    for idx in 0..count {
        let slot_abs = new_slots_abs + idx * SLOT_OFFSET_BYTES;
        let mut slot = IndexRawSlot {
            offset: read_u16(buf, slot_abs),
            len: read_u16(buf, slot_abs + 2),
        };
        if slot.offset as usize > removed_start {
            slot.offset = slot.offset.checked_sub(removed.len).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt index: slot offset underflow",
                )
            })?;
        }
        write_raw_index_slot(buf, slot_abs, slot);
    }
    let remove_abs = new_slots_abs + old_pos * SLOT_OFFSET_BYTES;
    buf.copy_within(
        remove_abs + SLOT_OFFSET_BYTES..new_slots_abs + count * SLOT_OFFSET_BYTES,
        remove_abs,
    );
    let insert_abs = new_slots_abs + new_pos * SLOT_OFFSET_BYTES;
    buf.copy_within(
        insert_abs..new_slots_abs + (count - 1) * SLOT_OFFSET_BYTES,
        insert_abs + SLOT_OFFSET_BYTES,
    );
    write_raw_index_slot(
        buf,
        insert_abs,
        IndexRawSlot {
            offset: entry_offset as u16,
            len: encoded.len() as u16,
        },
    );
    if new_payload_len < old_payload_len {
        buf[payload_origin + new_payload_len..payload_origin + old_payload_len].fill(0);
    }
    write_u64(
        buf,
        payload_origin + LEAF_OFF_PAGE_MAX_TRX_ID,
        read_u64(buf, payload_origin + LEAF_OFF_PAGE_MAX_TRX_ID).max(txn_id),
    );
    write_u32(buf, 20, new_payload_len as u32);
    Ok(Some(true))
}

fn try_move_between_raw_leaves(
    old_buf: &mut [u8],
    new_buf: &mut [u8],
    old_entry: &IndexEntry,
    new_entry: &IndexEntry,
    txn_id: u64,
    old_may_change_first: bool,
    new_may_change_first: bool,
) -> Result<Option<bool>, QueryError> {
    let (old_pos, old_count, old_slot_start, removed) = {
        let (header, position) = raw_leaf_slots_and_position(old_buf, old_entry)?;
        let Ok(pos) = position else {
            return Ok(Some(false));
        };
        (
            pos,
            header.count,
            header.slot_array_start,
            header.read_slot(pos),
        )
    };
    // Validate every fallible source operation before touching either page.
    if removed.offset as usize + removed.len as usize > old_slot_start {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: slot record bounds",
        ));
    }
    let old_payload_len = read_u32(old_buf, 20) as usize;
    if !old_may_change_first {
        let (live_size, count) = raw_leaf_live_encoded_len_and_count(old_buf)?;
        if count <= 1
            || live_size.saturating_sub(removed.len as usize + SLOT_OFFSET_BYTES)
                < INDEX_MIN_PAGE_BYTES
        {
            return Ok(None);
        }
    }
    let (new_header, new_position) = raw_leaf_slots_and_position(new_buf, new_entry)?;
    let new_pos = {
        let position = new_position;
        match position {
            Ok(_) => return Ok(Some(false)),
            Err(pos) => pos,
        }
    };
    if !new_may_change_first && new_pos == 0 {
        return Ok(None);
    }
    let Some(encoded) = encode_leaf_entry_for_layout(
        new_entry,
        new_header.key_layout,
        new_header.base_time,
        new_header.layout_aux,
    )?
    else {
        return Ok(None);
    };
    let new_payload_len = (read_u32(new_buf, 20) as usize)
        .checked_add(encoded.len() + SLOT_OFFSET_BYTES)
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                "index leaf size overflow",
            )
        })?;
    if INDEX_HEADER_BYTES + new_payload_len > PAGE_SIZE {
        return Ok(None);
    }
    // Destination insertion fails without modifying bytes. Once it succeeds,
    // the already-prepared source deletion cannot fail; both frames are latched.
    match try_insert_raw_leaf(new_buf, new_entry, txn_id)? {
        Some(InPlaceInsert::Inserted) => {
            delete_raw_leaf_prepared(
                old_buf,
                old_count,
                old_payload_len,
                old_slot_start,
                removed,
                old_pos,
                txn_id,
            );
            Ok(Some(true))
        }
        _ => Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "raw index move preflight diverged while inserting destination entry",
        )),
    }
}

#[cfg(test)]
mod raw_leaf_mutation_tests {
    use super::*;

    #[test]
    fn raw_compaction_recovers_deleted_space_without_changing_rejected_pages() {
        let mut oracle: Vec<_> = (0..1_000).map(|i| entry(i, i as u16)).collect();
        let leaf = IndexLeafPage::new_with_resolved_layout(oracle.clone(), None);
        let mut bytes = encode_index_tree_page(&IndexPage::Leaf(leaf)).unwrap();
        let mut recovered = false;
        for step in 1..=1_000 {
            let victim = oracle.remove(500);
            try_delete_raw_leaf(&mut bytes, &victim, step).unwrap();
            let mut candidate = victim;
            candidate.row_id.page_id += 1;
            let header = parse_leaf_payload_header(&bytes).unwrap();
            let encoded = encode_leaf_entry_for_layout(
                &candidate,
                header.key_layout,
                header.base_time,
                header.layout_aux,
            )
            .unwrap()
            .unwrap();
            let needs_compaction =
                INDEX_HEADER_BYTES + header.payload.len() + encoded.len() + SLOT_OFFSET_BYTES
                    > PAGE_SIZE;
            if needs_compaction {
                let before = bytes.clone();
                assert_eq!(
                    try_insert_compacted_raw_leaf(&mut bytes, &vec![0; PAGE_SIZE], 500, step)
                        .unwrap(),
                    Some(InPlaceInsert::NeedsSplit),
                );
                assert_eq!(bytes, before, "a declined insert must not mutate the page");
                recovered = true;
            }
            assert_eq!(
                try_insert_raw_leaf(&mut bytes, &candidate, step).unwrap(),
                Some(InPlaceInsert::Inserted)
            );
            oracle.insert(500, candidate);
            assert_matches_oracle(&mut bytes, &oracle, step);
            if recovered {
                break;
            }
        }
        assert!(
            recovered,
            "deleted record bodies must eventually exhaust physical space"
        );
    }

    #[test]
    fn raw_compaction_preserves_fragmented_records_across_layouts() {
        for shape in 0..12 {
            let make = |value: i64, row: u16| IndexEntry {
                key: IndexKey::from(match shape {
                    0 => vec![KeyPart::Int(value)],
                    1 => vec![KeyPart::UInt(value as u64)],
                    2 => vec![KeyPart::Int(value), KeyPart::Int(value * 3)],
                    3 => vec![KeyPart::String(format!(
                        "k{value:06}{}",
                        "x".repeat(value as usize % 19)
                    ))],
                    4 => vec![KeyPart::Bytes(vec![value as u8; value as usize % 19 + 1])],
                    5 => vec![KeyPart::Decimal {
                        value: value as i128,
                        scale: 2,
                    }],
                    6 => vec![KeyPart::Date(value as i32)],
                    7 => vec![KeyPart::Time(value)],
                    8 => vec![KeyPart::DateTime(value)],
                    9 => vec![KeyPart::Null, KeyPart::Int(value)],
                    10 => vec![KeyPart::Int(value * 100_000)],
                    _ => vec![KeyPart::UInt(value as u64 * 100_000)],
                }),
                row_id: RowId {
                    page_id: 7,
                    slot_id: row,
                },
            };
            let mut oracle: Vec<_> = (0..80).map(|i| make(i, i as u16)).collect();
            oracle.sort();
            let mut leaf = IndexLeafPage::new_with_resolved_layout(oracle.clone(), Some(42));
            leaf.set_page_max_trx_id(9);
            let mut bytes = encode_index_tree_page(&IndexPage::Leaf(leaf)).unwrap();
            for step in 10..210 {
                let pos = (step * 37) as usize % oracle.len();
                let old = oracle.remove(pos);
                assert!(matches!(
                    try_delete_raw_leaf(&mut bytes, &old, step).unwrap(),
                    Some(InPlaceDelete::Deleted { .. })
                ));
                let candidate = make((step % 80) as i64, (step + 100) as u16);
                let pos = oracle.binary_search(&candidate).unwrap_err();
                let header = parse_leaf_payload_header(&bytes).unwrap();
                let encoded = encode_leaf_entry_for_layout(
                    &candidate,
                    header.key_layout,
                    header.base_time,
                    header.layout_aux,
                )
                .unwrap()
                .unwrap();
                assert_eq!(
                    try_insert_compacted_raw_leaf(&mut bytes, &encoded, pos, step).unwrap(),
                    Some(InPlaceInsert::Inserted)
                );
                oracle.insert(pos, candidate);
                assert_matches_oracle(&mut bytes, &oracle, step);
                assert_eq!(
                    parse_leaf_payload_header(&bytes).unwrap().next_leaf,
                    Some(42)
                );
            }
        }
    }

    #[test]
    fn audit_20260926_cross_leaf_move_declines_source_underflow() {
        let old = entry(20, 2);
        let new = entry(40, 2);
        let mut source = repack(&[entry(10, 1), old.clone()], 0);
        let mut destination = repack(&[entry(30, 3), entry(50, 4)], 0);
        let before_source = source.clone();
        let before_destination = destination.clone();
        assert_eq!(try_move_between_raw_leaves(
            &mut source, &mut destination, &old, &new, 7, false, false,
        ).unwrap(), None);
        assert_eq!(source, before_source);
        assert_eq!(destination, before_destination);
    }

    #[test]
    fn audit_20260926_variable_reposition_matches_oracle() {
        for shape in 0..3 {
            let make = |text: String, row: u16| IndexEntry {
                key: IndexKey::from(match shape {
                    0 => vec![KeyPart::String(text)],
                    1 => vec![KeyPart::Bytes(text.into_bytes())],
                    _ => vec![KeyPart::String(text), KeyPart::Int(row.into())],
                }),
                row_id: RowId {
                    page_id: 7,
                    slot_id: row,
                },
            };
            let mut oracle: Vec<_> = (0..40).map(|row| make("a".repeat(60), row)).collect();
            let mut bytes = repack(&oracle, 0);
            let mut rng = 1234567u64;
            let mut last_mutation = 0;
            for step in 1..=2000 {
                rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                let pos = if step == 1 {
                    0
                } else {
                    (rng >> 32) as usize % oracle.len()
                };
                let old = oracle[pos].clone();
                let text = if step == 1 {
                    "z".into()
                } else {
                    format!(
                        "{}{}",
                        ["a", "z", "中", "\0", "😀"][step as usize % 5],
                        "x".repeat((rng % 100) as usize)
                    )
                };
                let new = make(text, old.row_id.slot_id);
                let expected = old != new;
                assert_eq!(
                    try_reposition_raw_leaf(&mut bytes, &old, &new, step, true).unwrap(),
                    Some(expected)
                );
                oracle[pos] = new;
                oracle.sort();
                if expected {
                    last_mutation = step;
                }
                assert_matches_oracle(&mut bytes, &oracle, last_mutation);
            }
        }
    }

    #[test]
    fn raw_run_stops_before_descending_suffix() {
        for shape in 0..4 {
            let make = |value: i64, row: u16| {
                let mut entry = entry(value, row);
                entry.key = IndexKey::from(match shape {
                    0 => vec![KeyPart::Int(value)],
                    1 => vec![
                        KeyPart::String(format!("key{value:04}")),
                        KeyPart::Int(value),
                    ],
                    2 => vec![KeyPart::UInt(value as u64)],
                    _ => vec![KeyPart::Bytes(vec![value as u8])],
                });
                entry
            };
            let original = vec![make(10, 10), make(20, 20), make(30, 30)];
            for run in [
                vec![make(25, 25), make(1, 1)],
                vec![make(21, 21), make(25, 25), make(1, 1)],
                vec![make(21, 21), make(21, 21), make(25, 25), make(1, 1)],
            ] {
                let mut bytes = repack(&original, 0);
                let (changed, consumed) =
                    try_insert_raw_leaf_run(&mut bytes, SortedIndexEntryRun::prefix(&run), 7)
                        .unwrap();
                assert!(changed);
                assert_eq!(consumed, Some(run.len() - 1));
                let mut expected = original.clone();
                expected.extend_from_slice(&run[..run.len() - 1]);
                expected.sort();
                expected.dedup();
                assert_matches_oracle(&mut bytes, &expected, 7);
            }
        }
    }

    #[test]
    fn raw_cross_leaf_move_preflight_and_commit_are_atomic() {
        let old = entry(10, 1);
        let replacement = entry(30, 1);
        let original = repack(&[old.clone(), entry(20, 2)], 3);
        let destination = repack(&[entry(25, 3), entry(40, 4)], 5);
        let mut source = original.clone();
        let mut target = destination.clone();
        assert_eq!(
            try_move_between_raw_leaves(
                &mut source,
                &mut target,
                &old,
                &replacement,
                9,
                true,
                true
            )
            .unwrap(),
            Some(true)
        );
        assert_matches_oracle(&mut source, &[entry(20, 2)], 9);
        assert_matches_oracle(
            &mut target,
            &[entry(25, 3), replacement.clone(), entry(40, 4)],
            9,
        );
        // Duplicate destination must leave both pages byte-for-byte untouched.
        let source_before = source.clone();
        let target_before = target.clone();
        assert_eq!(
            try_move_between_raw_leaves(
                &mut source,
                &mut target,
                &entry(20, 2),
                &replacement,
                10,
                true,
                true
            )
            .unwrap(),
            Some(false)
        );
        assert_eq!(source, source_before);
        assert_eq!(target, target_before);
        // An oversized destination key must reject before either page changes.
        let mut source = source_before.clone();
        let oversized = IndexEntry {
            key: IndexKey::from(vec![KeyPart::String("x".repeat(PAGE_SIZE))]),
            row_id: old.row_id,
        };
        assert_eq!(
            try_move_between_raw_leaves(
                &mut source,
                &mut target,
                &entry(20, 2),
                &oversized,
                10,
                true,
                true
            )
            .unwrap(),
            None
        );
        assert_eq!(source, source_before);
        assert_eq!(target, target_before);
        // A corrupt source record length is rejected before destination insert.
        let mut source = original;
        let header = parse_leaf_payload_header(&source).unwrap();
        let slot_abs = INDEX_HEADER_BYTES + header.slot_array_start;
        write_u16(&mut source, slot_abs + 2, u16::MAX);
        let source_before = source.clone();
        let mut target = destination.clone();
        assert!(try_move_between_raw_leaves(
            &mut source,
            &mut target,
            &old,
            &replacement,
            10,
            true,
            true
        )
        .is_err());
        assert_eq!(source, source_before);
        assert_eq!(target, destination);
    }

    #[test]
    fn fixed_scalar_sorted_probe_matches_generic_order() {
        let make = |part, page_id, slot_id| IndexEntry {
            key: IndexKey::from(vec![part]),
            row_id: RowId { page_id, slot_id },
        };
        for entries in [
            vec![
                make(KeyPart::Int(-2), 9, 0),
                make(KeyPart::Int(4), 1, 0),
                make(KeyPart::Int(4), 1, 1),
            ],
            vec![
                make(KeyPart::UInt(0), 9, 0),
                make(KeyPart::UInt(u64::MAX), 1, 0),
            ],
            vec![
                make(KeyPart::Date(i32::MIN), 9, 0),
                make(KeyPart::Date(i32::MAX), 1, 0),
            ],
        ] {
            assert!(entries.windows(2).all(|pair| pair[0] < pair[1]));
            assert!(index_entries_are_strictly_sorted(&entries));
        }

        let duplicate = vec![make(KeyPart::Int(4), 1, 1), make(KeyPart::Int(4), 1, 1)];
        assert!(!index_entries_are_strictly_sorted(&duplicate));
        let descending = vec![make(KeyPart::Int(4), 2, 0), make(KeyPart::Int(-2), 1, 0)];
        assert!(!index_entries_are_strictly_sorted(&descending));
    }

    #[test]
    fn direct_pair_int_leaf_encoder_matches_generic_oracle() {
        let compact = (0..500_u16)
            .map(|slot_id| {
                (
                    (10_000 + i64::from(slot_id), i64::from(slot_id) * 3),
                    RowId {
                        page_id: u32::from(slot_id) / 80,
                        slot_id,
                    },
                )
            })
            .collect::<Vec<_>>();
        let bytes =
            encode_pair_int_leaf_page(&compact, 10_000, 10_499, 0, 1_497, Some(77)).unwrap();
        let IndexPage::Leaf(decoded) = decode_index_tree_page(&bytes).unwrap() else {
            panic!("expected leaf");
        };
        assert_eq!(decoded.key_layout, KEY_LAYOUT_COMPACT_INT2);
        assert_eq!(decoded.next_leaf, Some(77));
        assert_eq!(
            decoded.entries,
            compact
                .iter()
                .map(|((left, right), row_id)| pair_int_index_entry(*left, *right, *row_id))
                .collect::<Vec<_>>()
        );

        let generic = vec![
            (
                (i64::MIN + 1, 0),
                RowId {
                    page_id: 1,
                    slot_id: 1,
                },
            ),
            (
                (i64::MAX - 1, 1),
                RowId {
                    page_id: 2,
                    slot_id: 2,
                },
            ),
        ];
        let bytes =
            encode_pair_int_leaf_page(&generic, i64::MIN + 1, i64::MAX - 1, 0, 1, None).unwrap();
        let IndexPage::Leaf(decoded) = decode_index_tree_page(&bytes).unwrap() else {
            panic!("expected leaf");
        };
        assert_eq!(decoded.key_layout, KEY_LAYOUT_GENERIC);
        assert_eq!(
            decoded.entries,
            generic
                .iter()
                .map(|((left, right), row_id)| pair_int_index_entry(*left, *right, *row_id))
                .collect::<Vec<_>>()
        );
    }

    fn entry(key: i64, row: u16) -> IndexEntry {
        IndexEntry {
            key: IndexKey::from(vec![KeyPart::Int(key)]),
            row_id: RowId {
                page_id: 7,
                slot_id: row,
            },
        }
    }

    fn repack(entries: &[IndexEntry], page_max_trx_id: u64) -> Vec<u8> {
        let mut leaf = IndexLeafPage::new(entries.to_vec(), None);
        leaf.set_page_max_trx_id(page_max_trx_id);
        encode_index_tree_page(&IndexPage::Leaf(leaf)).unwrap()
    }

    fn delivery_entry(carrier: Option<i64>, row: u16) -> IndexEntry {
        IndexEntry {
            key: IndexKey::from(vec![
                KeyPart::Int(1),
                KeyPart::Int(2),
                carrier.map_or(KeyPart::Null, KeyPart::Int),
                KeyPart::Int(row as i64),
            ]),
            row_id: RowId {
                page_id: 9,
                slot_id: row,
            },
        }
    }

    #[test]
    fn raw_covering_runtime_filter_skips_full_int2_key_decode() {
        let entries = (0..12)
            .map(|value| IndexEntry {
                key: IndexKey::from(vec![KeyPart::Int(value), KeyPart::Int(value * 10)]),
                row_id: RowId {
                    page_id: 9,
                    slot_id: value as u16,
                },
            })
            .collect::<Vec<_>>();
        let bytes = repack(&entries, 17);
        let pulled = range_leaf_entries_page_bytes_filtered_first_int(
            &bytes,
            None,
            None,
            0,
            true,
            None,
            true,
            usize::MAX,
            |value| matches!(value, Some(2 | 7 | 11)),
        )
        .unwrap();

        assert_eq!(pulled.examined, 12);
        assert_eq!(pulled.rejected, 9);
        assert_eq!(pulled.page_max_trx_id, 17);
        assert_eq!(
            pulled
                .entries
                .iter()
                .map(|entry| entry.key.clone())
                .collect::<Vec<_>>(),
            vec![
                entries[2].key.clone(),
                entries[7].key.clone(),
                entries[11].key.clone()
            ]
        );
    }

    fn assert_matches_oracle(bytes: &mut [u8], oracle: &[IndexEntry], min_txn_id: u64) {
        // Production defers CRC work until flush/full-image exposure. The
        // differential oracle deliberately finalizes on every step so it
        // checks both mutation semantics and checksum correctness.
        refresh_page_checksum(bytes);
        validate_index_page_header(bytes).unwrap();
        let IndexPage::Leaf(leaf) = decode_index_tree_page(bytes).unwrap() else {
            panic!("expected leaf");
        };
        assert_eq!(leaf.entries, oracle);
        assert!(leaf.page_max_trx_id() >= min_txn_id);
        let header = parse_leaf_payload_header(bytes).unwrap();
        for slot in 0..header.count {
            let slot = header.read_slot(slot);
            assert!(slot.offset as usize + slot.len as usize <= header.slot_array_start);
        }
    }

    #[test]
    fn appended_compact_leaf_widening_returns_overflow_tail() {
        // A nearly-full u16 leaf fits before the append. Either gap forces all
        // existing keys to widen (u32 or generic), reproducing the original
        // page-overflow boundary without thousands of SQL/storage writes.
        for gap in [100_000_000_i64, 10_000_000_000_i64] {
            let base_entries: Vec<_> = (0..1_300).map(|i| entry(i as i64, i as u16)).collect();
            let mut leaf = IndexLeafPage::new_with_resolved_layout(base_entries, None);
            assert_eq!(leaf.key_layout, KEY_LAYOUT_COMPACT_INT_U16);
            assert!(encoded_leaf_page_len(&leaf) <= PAGE_SIZE);

            let start_pos = leaf.entries.len();
            leaf.entries
                .extend((1..=16).map(|i| entry(gap + i, (1_300 + i) as u16)));
            let mut pos = leaf.entries.len();
            resolve_appended_leaf_layout(&mut leaf, &mut pos, start_pos);

            assert_eq!(pos, start_pos, "gap={gap}");
            assert_eq!(leaf.entries.len(), start_pos, "gap={gap}");
            assert_eq!(leaf.key_layout, KEY_LAYOUT_COMPACT_INT_U16, "gap={gap}");
            assert!(encoded_leaf_page_len(&leaf) <= PAGE_SIZE, "gap={gap}");
        }
    }

    #[test]
    fn raw_leaf_mutator_matches_oracle_for_2k_steps() {
        let mut bytes = repack(&[], 0);
        let mut oracle = Vec::<IndexEntry>::new();
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let mut last_txn = 0_u64;

        for step in 1..=2_000_u64 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let operation = ((state >> 32) % 3) as u8;
            match operation {
                0 if oracle.len() < 128 => {
                    let candidate = entry(((state >> 8) % 512) as i64, state as u16);
                    let position = oracle.binary_search(&candidate);
                    let mut result = try_insert_raw_leaf(&mut bytes, &candidate, step).unwrap();
                    if matches!(result, None | Some(InPlaceInsert::NeedsSplit)) {
                        bytes = repack(&oracle, step.saturating_sub(1));
                        result = try_insert_raw_leaf(&mut bytes, &candidate, step).unwrap();
                    }
                    match position {
                        Ok(_) => assert_eq!(result, Some(InPlaceInsert::Duplicate)),
                        Err(pos) => {
                            assert_eq!(result, Some(InPlaceInsert::Inserted));
                            oracle.insert(pos, candidate);
                            last_txn = step;
                        }
                    }
                }
                1 if !oracle.is_empty() => {
                    let pos = (state as usize) % oracle.len();
                    let victim = oracle[pos].clone();
                    let result = try_delete_raw_leaf(&mut bytes, &victim, step).unwrap();
                    assert!(matches!(result, Some(InPlaceDelete::Deleted { .. })));
                    oracle.remove(pos);
                    last_txn = step;
                }
                _ if !oracle.is_empty() => {
                    let pos = (state as usize) % oracle.len();
                    let old = oracle[pos].clone();
                    let replacement = entry(((state >> 17) % 512) as i64, old.row_id.slot_id);
                    if oracle.binary_search(&replacement).is_ok() {
                        continue;
                    }
                    let mut result =
                        try_reposition_raw_leaf(&mut bytes, &old, &replacement, step, true)
                            .unwrap();
                    if result.is_none() {
                        bytes = repack(&oracle, step.saturating_sub(1));
                        result =
                            try_reposition_raw_leaf(&mut bytes, &old, &replacement, step, true)
                                .unwrap();
                    }
                    assert_eq!(result, Some(true));
                    oracle.remove(pos);
                    let insert = oracle.binary_search(&replacement).unwrap_err();
                    oracle.insert(insert, replacement);
                    last_txn = step;
                }
                _ => {}
            }
            assert_matches_oracle(&mut bytes, &oracle, last_txn);
        }
    }

    #[test]
    fn raw_composite_reposition_reclaims_replaced_record_bytes() {
        let old = delivery_entry(None, 7);
        let new = delivery_entry(Some(5), 7);
        let mut bytes = repack(std::slice::from_ref(&old), 0);
        let null_payload_len = read_u32(&bytes, 20);

        for txn_id in 1..=2_000 {
            let (from, to) = if txn_id % 2 == 1 {
                (&old, &new)
            } else {
                (&new, &old)
            };
            assert_eq!(
                try_reposition_raw_leaf(&mut bytes, from, to, txn_id, true).unwrap(),
                Some(true)
            );
            refresh_page_checksum(&mut bytes);
            validate_index_page_header(&bytes).unwrap();
            let expected = if txn_id % 2 == 1 {
                std::slice::from_ref(&new)
            } else {
                std::slice::from_ref(&old)
            };
            let IndexPage::Leaf(leaf) = decode_index_tree_page(&bytes).unwrap() else {
                panic!("expected leaf");
            };
            assert_eq!(leaf.entries, expected);
            if txn_id % 2 == 0 {
                assert_eq!(
                    read_u32(&bytes, 20),
                    null_payload_len,
                    "same logical key shape must return to the same payload size"
                );
            }
        }
    }

    #[test]
    fn raw_equal_width_reposition_keeps_record_offset_and_payload_size() {
        let entries = (0..32_u16)
            .map(|value| entry(i64::from(value) * 10, value))
            .collect::<Vec<_>>();
        let old = entries[8].clone();
        let replacement = entry(255, old.row_id.slot_id);
        let mut bytes = repack(&entries, 0);
        let payload_len = read_u32(&bytes, 20);
        let (header, old_position) = raw_leaf_slots_and_position(&bytes, &old).unwrap();
        let old_slot = header.read_slot(old_position.unwrap());

        assert_eq!(
            try_reposition_raw_leaf(&mut bytes, &old, &replacement, 7, true).unwrap(),
            Some(true)
        );

        let (header, new_position) = raw_leaf_slots_and_position(&bytes, &replacement).unwrap();
        let new_slot = header.read_slot(new_position.unwrap());
        assert_eq!(
            new_slot, old_slot,
            "equal-width rewrite must reuse the record"
        );
        assert_eq!(read_u32(&bytes, 20), payload_len);
        let mut oracle = entries;
        oracle.remove(8);
        let insert = oracle.binary_search(&replacement).unwrap_err();
        oracle.insert(insert, replacement);
        assert_matches_oracle(&mut bytes, &oracle, 7);
    }
}

/// Parsed, borrowed view of one internal page used by raw B-tree routing.
///
/// A batched lookup sends many sorted keys through the same internal page.
/// Keeping this header outside the per-key loop avoids re-reading and
/// re-validating the fixed page fields for every key while preserving the
/// existing allocation-free separator comparator.
struct RawInternalRoutePage<'a> {
    header: LeafPayloadHeader<'a>,
    first_child: u64,
    entries_start: usize,
}

impl RawInternalRoutePage<'_> {
    /// Reuse a preceding child interval from this same immutable parent.
    /// Exponential probing keeps adjacent sparse runs cheap without turning
    /// a large jump into a linear separator scan.
    fn route_near(
        &self,
        key: &IndexKey,
        row_id: RowId,
        floor: usize,
        mut child: u64,
    ) -> Result<(u64, usize), QueryError> {
        let target = self.header.fast_scalar_target(key);
        let mut lo = floor;
        let mut hi = floor;
        let mut step = 1usize;
        let mut selected = None;
        while hi < self.header.count {
            let (le, cursor) = self.separator_le_at(hi, key, row_id, target)?;
            if !le {
                break;
            }
            selected = Some(cursor);
            lo = hi + 1;
            hi = floor.saturating_add(step).min(self.header.count);
            step = step.saturating_mul(2);
        }
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let (le, cursor) = self.separator_le_at(mid, key, row_id, target)?;
            if le {
                selected = Some(cursor);
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if let Some(mut cursor) = selected {
            child = read_u64_checked(self.header.payload, &mut cursor)?;
        }
        Ok((child, lo))
    }

    /// Test separator <= probe in full entry order, decoding RowId only for
    /// equal keys. Validate the fixed tail even when its values are not read.
    #[inline]
    fn separator_le_at(
        &self,
        slot_idx: usize,
        key: &IndexKey,
        row_id: RowId,
        target: Option<FastScalarTarget>,
    ) -> Result<(bool, usize), QueryError> {
        let slot = self.header.read_slot(slot_idx);
        if (slot.offset as usize) < self.entries_start
            || slot.offset as usize >= self.header.slot_array_start
        {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: invalid internal slot offset",
            ));
        }
        let (key_order, mut cursor) = match target {
            Some(target) => self.header.cmp_scalar_at(slot_idx, target)?,
            None => self.header.cmp_key_at(slot_idx, key)?,
        };
        let child_cursor = cursor.checked_add(ROW_ID_V7_ENCODED_LEN).ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: row offset overflow",
            )
        })?;
        if child_cursor.checked_add(8).is_none_or(|end| {
            end > slot.offset as usize + slot.len as usize || end > self.header.slot_array_start
        }) {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: truncated internal child pointer",
            ));
        }
        let separator_le = key_order == std::cmp::Ordering::Less
            || (key_order == std::cmp::Ordering::Equal
                && decode_row_id_v7_from(self.header.payload, &mut cursor, 0)? <= row_id);
        Ok((separator_le, child_cursor))
    }

    /// Find the input interval below the next separator. Exponential probing
    /// costs one comparison for sparse batches and logarithmic work for dense
    /// runs. Keys, collations and RowId ties use the same routing comparator.
    fn insert_run_end(
        &self,
        entries: &[IndexEntry],
        start: usize,
        end: usize,
        next_slot: usize,
    ) -> Result<usize, QueryError> {
        if next_slot == self.header.count || start + 1 == end {
            return Ok(end);
        }
        let before_fence = |pos: usize| {
            let entry = &entries[pos];
            self.separator_le_at(
                next_slot,
                &entry.key,
                entry.row_id,
                self.header.fast_scalar_target(&entry.key),
            )
            .map(|(separator_le, _)| !separator_le)
        };
        let mut lo = start + 1;
        let mut hi = lo;
        let mut step = 1usize;
        while hi < end && before_fence(hi)? {
            lo = hi + 1;
            step = step.saturating_mul(2);
            hi = start.saturating_add(step).min(end);
        }
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if before_fence(mid)? {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }

    /// Route one key, starting at a lower-bound separator already established
    /// by the previous (smaller) key. `selected_child` is the child immediately
    /// to the left of `search_floor`.
    fn route_from(
        &self,
        key: &IndexKey,
        row_id: RowId,
        search_floor: usize,
        mut selected_child: u64,
    ) -> Result<(u64, usize), QueryError> {
        let scalar_target = self.header.fast_scalar_target(key);
        let mut lo = search_floor;
        let mut hi = self.header.count;
        let mut selected_cursor = None;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let (separator_le, cursor) = self.separator_le_at(mid, key, row_id, scalar_target)?;
            if separator_le {
                selected_cursor = Some(cursor);
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if let Some(mut cursor) = selected_cursor {
            selected_child = read_u64_checked(self.header.payload, &mut cursor)?;
        }
        Ok((selected_child, lo))
    }
}

/// Parse one validated raw page for routing. `None` denotes a leaf.
fn parse_raw_internal_route_page(
    buf: &[u8],
) -> Result<Option<RawInternalRoutePage<'_>>, QueryError> {
    debug_assert_eq!(buf.len(), PAGE_SIZE);
    let payload_len = read_u32(buf, 20) as usize;
    if payload_len == 0 || INDEX_HEADER_BYTES + payload_len > PAGE_SIZE {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index page length",
        ));
    }
    let payload = &buf[INDEX_HEADER_BYTES..INDEX_HEADER_BYTES + payload_len];
    match payload[INTERNAL_OFF_PAGE_TYPE] {
        1 => return Ok(None),
        2 => {}
        3 => {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: root path hit free page",
            ));
        }
        _ => {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: unknown page type",
            ))
        }
    }
    if payload_len < INTERNAL_HEADER_BYTES - INDEX_HEADER_BYTES {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: internal payload too short",
        ));
    }

    let key_layout = payload[INTERNAL_OFF_KEY_LAYOUT];
    let base_time = i64::from_le_bytes(
        payload[INTERNAL_OFF_BASE_TIME..INTERNAL_OFF_BASE_TIME + 8]
            .try_into()
            .unwrap(),
    );
    let first_child = u64::from_le_bytes(
        payload[INTERNAL_OFF_FIRST_CHILD..INTERNAL_OFF_FIRST_CHILD + 8]
            .try_into()
            .unwrap(),
    );
    let key_count = u32::from_le_bytes(
        payload[INTERNAL_OFF_KEY_COUNT..INTERNAL_OFF_KEY_COUNT + 4]
            .try_into()
            .unwrap(),
    ) as usize;

    let mut entries_start = INTERNAL_OFF_KEY_COUNT + 4;
    let layout_aux = if key_layout == KEY_LAYOUT_COMPACT_INT2 {
        let (col2_base, width1, width2) = decode_int2_prefix(payload, &mut entries_start)?;
        Some(LayoutAux {
            col2_base,
            width1,
            width2,
        })
    } else {
        None
    };
    let slot_bytes = key_count.checked_mul(SLOT_OFFSET_BYTES).ok_or_else(|| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: internal slot count overflow",
        )
    })?;
    let slot_array_start = payload_len.checked_sub(slot_bytes).ok_or_else(|| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: internal slot array underflow",
        )
    })?;
    if slot_array_start < entries_start {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: internal slot array overlaps header",
        ));
    }

    // Reuse the layout-aware leaf key reader: separator key/RowId encodings
    // and the trailing slot format are deliberately identical on leaf and
    // internal pages.
    Ok(Some(RawInternalRoutePage {
        header: LeafPayloadHeader {
            payload,
            page_table_id: 0,
            key_layout,
            base_time,
            next_leaf: None,
            count: key_count,
            slot_array_start,
            layout_aux,
            page_max_trx_id: 0,
        },
        first_child,
        entries_start,
    }))
}

/// Route a `(key, row_id)` probe through one validated raw tree page.
///
/// Internal pages have carried a trailing separator slot array since the v13
/// layout, but the read path historically decoded every separator into
/// `Vec<IndexEntry>` before using it. Under decoded-cache pressure that made
/// every point lookup O(fanout) allocations. This reader uses the slot array
/// for an O(log fanout) binary search. Compact integer layouts compare in
/// place with zero allocation; every other layout decodes only the handful of
/// separators touched by the binary search.
fn route_index_page_bytes(
    buf: &[u8],
    key: &IndexKey,
    row_id: RowId,
) -> Result<RawIndexPageRoute, QueryError> {
    let Some(page) = parse_raw_internal_route_page(buf)? else {
        return Ok(RawIndexPageRoute::Leaf);
    };
    let (page_id, child_idx) = page.route_from(key, row_id, 0, page.first_child)?;
    Ok(RawIndexPageRoute::Child { page_id, child_idx })
}

/// Route a sorted key range through one raw page in a single borrowed pass.
///
/// The separator position for sorted probe keys is monotonic. Carrying the
/// preceding key's child index into the next binary search both shrinks the
/// search suffix and avoids reparsing the page header once per key. Returned
/// ranges use absolute key offsets so recursive descent can consume them
/// without rebuilding an index map. `None` denotes a leaf page.
fn route_index_page_key_ranges_bytes(
    buf: &[u8],
    keys: &[IndexKey],
    start: usize,
    end: usize,
    row_id: RowId,
) -> Result<Option<Vec<LeafKeyRange>>, QueryError> {
    debug_assert!(keys[start..end].windows(2).all(|pair| pair[0] <= pair[1]));
    let Some(page) = parse_raw_internal_route_page(buf)? else {
        return Ok(None);
    };
    let mut ranges: Vec<LeafKeyRange> = Vec::new();
    let mut search_floor = 0usize;
    let mut selected_child = page.first_child;
    for key_index in start..end {
        let (child, child_idx) =
            page.route_from(&keys[key_index], row_id, search_floor, selected_child)?;
        search_floor = child_idx;
        selected_child = child;
        let extends_last = ranges
            .last()
            .is_some_and(|last| last.leaf_id == child && last.end == key_index);
        if extends_last {
            ranges.last_mut().expect("range checked above").end += 1;
        } else {
            ranges.push(LeafKeyRange {
                leaf_id: child,
                start: key_index,
                end: key_index + 1,
            });
        }
    }
    Ok(Some(ranges))
}

/// Return `None` for a leaf, otherwise the left/right edge child of an
/// internal page.  Only the right edge decodes one separator record in order
/// to skip its variable-width key/RowId and reach `right_child`.
fn edge_child_index_page_bytes(buf: &[u8], right: bool) -> Result<Option<u64>, QueryError> {
    let payload_len = read_u32(buf, 20) as usize;
    if payload_len == 0 || INDEX_HEADER_BYTES + payload_len > PAGE_SIZE {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index page length",
        ));
    }
    let payload = &buf[INDEX_HEADER_BYTES..INDEX_HEADER_BYTES + payload_len];
    match payload[INTERNAL_OFF_PAGE_TYPE] {
        1 => return Ok(None),
        2 => {}
        3 => {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: edge walk hit free page",
            ))
        }
        _ => {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: unknown page type",
            ))
        }
    }
    if payload_len < INTERNAL_HEADER_BYTES - INDEX_HEADER_BYTES {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: internal payload too short",
        ));
    }
    let first_child = u64::from_le_bytes(
        payload[INTERNAL_OFF_FIRST_CHILD..INTERNAL_OFF_FIRST_CHILD + 8]
            .try_into()
            .unwrap(),
    );
    if !right {
        return Ok(Some(first_child));
    }
    let key_layout = payload[INTERNAL_OFF_KEY_LAYOUT];
    let base_time = i64::from_le_bytes(
        payload[INTERNAL_OFF_BASE_TIME..INTERNAL_OFF_BASE_TIME + 8]
            .try_into()
            .unwrap(),
    );
    let key_count = u32::from_le_bytes(
        payload[INTERNAL_OFF_KEY_COUNT..INTERNAL_OFF_KEY_COUNT + 4]
            .try_into()
            .unwrap(),
    ) as usize;
    if key_count == 0 {
        return Ok(Some(first_child));
    }
    let mut entries_start = INTERNAL_OFF_KEY_COUNT + 4;
    let layout_aux = if key_layout == KEY_LAYOUT_COMPACT_INT2 {
        let (col2_base, width1, width2) = decode_int2_prefix(payload, &mut entries_start)?;
        Some(LayoutAux {
            col2_base,
            width1,
            width2,
        })
    } else {
        None
    };
    let slot_array_start = payload_len
        .checked_sub(key_count.checked_mul(SLOT_OFFSET_BYTES).ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: slot count overflow",
            )
        })?)
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: slot array underflow",
            )
        })?;
    if slot_array_start < entries_start {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: slot array overlaps header",
        ));
    }
    let header = LeafPayloadHeader {
        payload,
        page_table_id: 0,
        key_layout,
        base_time,
        next_leaf: None,
        count: key_count,
        slot_array_start,
        layout_aux,
        page_max_trx_id: 0,
    };
    let slot = header.read_slot(key_count - 1);
    let mut cursor = slot.offset as usize;
    let _ = header.decode_key_at(&mut cursor)?;
    let _ = decode_row_id_v7_from(payload, &mut cursor, 0)?;
    let child = read_u64_checked(payload, &mut cursor)?;
    if cursor != slot.offset as usize + slot.len as usize {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: internal slot length mismatch",
        ));
    }
    Ok(Some(child))
}

enum RawFirstEntryStep {
    Leaf(Option<IndexEntry>),
    Child(u64),
}

/// Follow one left edge or decode only slot zero of a leaf. The buffer-pool
/// raw-byte boundary has already validated the resident page image; this
/// helper still validates the slot-local record boundary before materialising
/// the single key needed by parent-separator maintenance.
fn raw_first_entry_step(buf: &[u8]) -> Result<RawFirstEntryStep, QueryError> {
    let payload_len = read_u32(buf, 20) as usize;
    if payload_len == 0 || INDEX_HEADER_BYTES + payload_len > PAGE_SIZE {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index page length",
        ));
    }
    match buf[INDEX_HEADER_BYTES + LEAF_OFF_PAGE_TYPE] {
        1 => {
            let header = parse_leaf_payload_header(buf)?;
            if header.count == 0 {
                return Ok(RawFirstEntryStep::Leaf(None));
            }
            let slot = header.read_slot(0);
            let mut cursor = slot.offset as usize;
            let record_end = cursor.checked_add(slot.len as usize).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt index: first entry record overflow",
                )
            })?;
            if slot.len == 0 || record_end > header.slot_array_start {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt index: first entry record bounds",
                ));
            }
            let key = header.decode_key_at(&mut cursor)?;
            let row_id = decode_row_id_v7_from(header.payload, &mut cursor, header.page_table_id)?;
            if cursor != record_end {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt index: first entry slot length mismatch",
                ));
            }
            Ok(RawFirstEntryStep::Leaf(Some(IndexEntry { key, row_id })))
        }
        2 => {
            let child = edge_child_index_page_bytes(buf, false)?.ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt index: internal page has no child",
                )
            })?;
            Ok(RawFirstEntryStep::Child(child))
        }
        3 => Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: first key hit free page",
        )),
        _ => Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: unknown page type",
        )),
    }
}

/// Outcome of a byte-level leaf lookup: equal-key (or prefix-matching)
/// RowIds collected from this page, plus the optional `next_leaf`
/// pointer if the caller may need to follow the sibling chain.
struct LeafLookupResult {
    rows: Vec<RowId>,
    next_leaf: Option<u64>,
    /// Mirrors the v6/v7 control flow: the caller should follow
    /// `next_leaf` *unless* the walk on this page hit a key that
    /// is strictly greater than the target. A key > target proves
    /// every subsequent entry (here and on every later leaf) is
    /// also greater, so the scan can stop. Conversely, if no
    /// greater key was seen — including the `lo == count` case
    /// where the search lower bound falls off the end of this
    /// leaf because of internal-page navigation slack — the
    /// duplicates (or the first matching key) may live on the
    /// next leaf.
    should_continue: bool,
}

struct LeafSingleLookupResult {
    row: Option<RowId>,
    next_leaf: Option<u64>,
    should_continue: bool,
}

struct LeafEntryLookupResult<E> {
    entries: Vec<E>,
    next_leaf: Option<u64>,
    should_continue: bool,
    page_max_trx_id: u64,
}

struct LeafEntriesManyLookupResult<E> {
    entries: Vec<E>,
    next_leaf: Option<u64>,
    continue_key_offsets: Vec<usize>,
}

/// Per-leaf outcome for `keys_first_visible_match_leaf_page` — the
/// "Anti/Semi short-circuit" cousin of `lookup_leaf_entries_many_page_bytes`.
/// `next_leaf` and `continue_key_offsets` mean the same thing as in
/// `LeafEntriesManyLookupResult`: which keys ran off the end of this
/// leaf without seeing a `Greater` and need to continue on the next
/// leaf. The leaf scan writes matched flags directly into the
/// caller's parallel `matched: &mut [bool]`.
struct KeysFirstMatchLeafResult {
    next_leaf: Option<u64>,
    continue_key_offsets: Vec<usize>,
}

/// Byte-level result for the key-free forward range cursor. Keeping this
/// state outside a decoded `IndexPage` lets non-covering scans materialise
/// only the RowIds they return instead of every entry in the leaf.
struct RawLeafRangeRows {
    rows: Vec<RowId>,
    /// Last physical index entry examined, including a duplicate skipped by
    /// the RowId sink. A cursor uses this only after an index mutation to
    /// re-seek from the root without retaining a recyclable leaf page id.
    resume_after: Option<IndexEntry>,
    next_leaf: Option<u64>,
    next_entry_idx: usize,
    last_row_id: Option<RowId>,
    page_max_trx_id: u64,
    positioned: bool,
    exhausted_leaf: bool,
    terminal: bool,
}

/// Per-leaf outcome for forward cursors that need both the encoded key and
/// RowId (covering scans, index aggregates, and resume bookkeeping).  The
/// entries are operation-local: nothing is attached to the frame or retained
/// after the cursor hands the batch to its
/// consumer.
struct RawLeafRangeEntries {
    entries: Vec<IndexEntry>,
    /// Last physical entry examined, including rows rejected by a runtime
    /// filter. See `RawLeafRangeRows::resume_after`.
    resume_after: Option<IndexEntry>,
    examined: usize,
    rejected: usize,
    next_leaf: Option<u64>,
    next_entry_idx: usize,
    last_row_id: Option<RowId>,
    page_max_trx_id: u64,
    positioned: bool,
    exhausted_leaf: bool,
    terminal: bool,
}

/// Read the fixed-width RowId trailer without materializing the key. Slot
/// bounds are checked independently of the key's type or compact layout.
fn decode_raw_leaf_row_id_at(
    hdr: &LeafPayloadHeader<'_>,
    slot_index: usize,
) -> Result<RowId, QueryError> {
    let slot = hdr.read_slot(slot_index);
    let record_start = slot.offset as usize;
    let record_end = record_start.checked_add(slot.len as usize).ok_or_else(|| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: leaf record overflow",
        )
    })?;
    if slot.len as usize <= ROW_ID_V7_ENCODED_LEN
        || record_start < LEAF_OFF_ENTRIES
        || record_end > hdr.slot_array_start
    {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: leaf record cannot contain RowId",
        ));
    }
    let mut cursor = record_end - ROW_ID_V7_ENCODED_LEN;
    let row_id = decode_row_id_v7_from(hdr.payload, &mut cursor, hdr.page_table_id)?;
    debug_assert_eq!(cursor, record_end);
    Ok(row_id)
}

fn decode_raw_leaf_entry_at(
    hdr: &LeafPayloadHeader<'_>,
    slot_index: usize,
) -> Result<IndexEntry, QueryError> {
    let mut cursor = hdr.read_slot_offset(slot_index);
    let key = hdr.decode_key_at(&mut cursor)?;
    let row_id = decode_row_id_v7_from(hdr.payload, &mut cursor, hdr.page_table_id)?;
    Ok(IndexEntry { key, row_id })
}

#[inline]
fn raw_leaf_slot_past_high(
    hdr: &LeafPayloadHeader<'_>,
    slot: usize,
    high: &IndexBound,
) -> Result<bool, QueryError> {
    let (ordering, _, prefix_match) = compare_raw_leaf_slot_to_bound(hdr, slot, high)?;
    Ok(if prefix_match {
        !high.inclusive
    } else {
        ordering == std::cmp::Ordering::Greater
            || (ordering == std::cmp::Ordering::Equal && !high.inclusive)
    })
}

/// Return the exclusive end slot for the high bound.  Interior leaves need
/// one last-slot probe; only the terminal leaf pays a binary search.
fn raw_leaf_high_end(
    hdr: &LeafPayloadHeader<'_>,
    high: Option<&IndexBound>,
) -> Result<(usize, bool), QueryError> {
    let Some(high) = high else {
        return Ok((hdr.count, false));
    };
    if hdr.count == 0 || !raw_leaf_slot_past_high(hdr, hdr.count - 1, high)? {
        return Ok((hdr.count, false));
    }
    let mut lo = 0usize;
    let mut hi = hdr.count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if raw_leaf_slot_past_high(hdr, mid, high)? {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    Ok((lo, true))
}

// These hot readers share source without changing the per-leaf return ABI.
macro_rules! raw_leaf_low_start {
    ($hdr:expr, $low:expr) => {{
        if let Some(low) = $low {
            let mut lo = 0usize;
            let mut hi = $hdr.count;
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                let (ordering, _, prefix_match) = compare_raw_leaf_slot_to_bound($hdr, mid, low)?;
                let before = if prefix_match {
                    !low.inclusive
                } else {
                    ordering == std::cmp::Ordering::Less
                        || (ordering == std::cmp::Ordering::Equal && !low.inclusive)
                };
                if before {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            lo
        } else {
            0
        }
    }};
}

/// Return the in-bounds slot span for a raw leaf. Interior leaves resolve
/// with constant work; only the two range boundaries pay binary searches.
fn raw_leaf_in_bounds_span(
    hdr: &LeafPayloadHeader<'_>,
    low: Option<&IndexBound>,
    high: Option<&IndexBound>,
) -> Result<(usize, usize, bool), QueryError> {
    let start = raw_leaf_low_start!(hdr, low);
    let (end, terminal) = raw_leaf_high_end(hdr, high)?;
    Ok((start.min(end), end, terminal))
}

/// Pull row ids from one raw leaf page for a forward range cursor.
///
/// Consumers that need index keys keep the decoded cursor path. A
/// non-covering scan only needs RowIds, so fully decoding a 16 KiB leaf into
/// hundreds of heap-backed `IndexEntry`/`IndexKey` values for a small bounded
/// slice is avoidable allocation and decoded-cache churn.
fn range_leaf_row_ids_page_bytes(
    buf: &[u8],
    low: Option<&IndexBound>,
    high: Option<&IndexBound>,
    mut entry_idx: usize,
    mut positioned: bool,
    mut last_row_id: Option<RowId>,
    unique: bool,
    capture_resume_key: bool,
    max_rows: usize,
) -> Result<RawLeafRangeRows, QueryError> {
    let hdr = parse_leaf_payload_header(buf)?;

    if !positioned {
        entry_idx = raw_leaf_low_start!(&hdr, low);
        // Match `IndexRangeCursor::leaf_span`: when the low bound falls
        // beyond this leaf, positioning retries in the sibling.
        if entry_idx < hdr.count {
            positioned = true;
        }
    }

    let (end, terminal) = raw_leaf_high_end(&hdr, high)?;
    let examined_start = entry_idx;
    let mut rows = Vec::with_capacity(max_rows.min(end.saturating_sub(entry_idx)));
    while entry_idx < end {
        let row_id = decode_raw_leaf_row_id_at(&hdr, entry_idx)?;
        entry_idx += 1;
        if !unique && last_row_id == Some(row_id) {
            continue;
        }
        last_row_id = Some(row_id);
        rows.push(row_id);
        if rows.len() >= max_rows {
            let resume_after = (capture_resume_key && entry_idx > examined_start)
                .then(|| decode_raw_leaf_entry_at(&hdr, entry_idx - 1))
                .transpose()?;
            return Ok(RawLeafRangeRows {
                rows,
                resume_after,
                next_leaf: hdr.next_leaf,
                next_entry_idx: entry_idx,
                last_row_id,
                page_max_trx_id: hdr.page_max_trx_id,
                positioned,
                exhausted_leaf: false,
                terminal: false,
            });
        }
    }

    let resume_after = (capture_resume_key && entry_idx > examined_start)
        .then(|| decode_raw_leaf_entry_at(&hdr, entry_idx - 1))
        .transpose()?;
    Ok(RawLeafRangeRows {
        rows,
        resume_after,
        next_leaf: hdr.next_leaf,
        next_entry_idx: entry_idx,
        last_row_id,
        page_max_trx_id: hdr.page_max_trx_id,
        positioned,
        exhausted_leaf: !terminal,
        terminal,
    })
}

/// Pull only the requested slice of entries from one raw leaf.  Bounds use
/// the raw comparator; full `IndexKey` values are decoded only for entries
/// that are actually returned to a key-consuming caller.
fn range_leaf_entries_page_bytes(
    buf: &[u8],
    low: Option<&IndexBound>,
    high: Option<&IndexBound>,
    mut entry_idx: usize,
    mut positioned: bool,
    mut last_row_id: Option<RowId>,
    unique: bool,
    max_entries: usize,
) -> Result<RawLeafRangeEntries, QueryError> {
    let hdr = parse_leaf_payload_header(buf)?;

    if !positioned {
        entry_idx = raw_leaf_low_start!(&hdr, low);
        if entry_idx < hdr.count {
            positioned = true;
        }
    }

    let (end, terminal) = raw_leaf_high_end(&hdr, high)?;
    let examined_start = entry_idx;
    let mut entries = Vec::with_capacity(max_entries.min(end.saturating_sub(entry_idx)));
    while entry_idx < end {
        let mut cursor = hdr.read_slot_offset(entry_idx);
        let key = hdr.decode_key_at(&mut cursor)?;
        let row_id = decode_row_id_v7_from(hdr.payload, &mut cursor, hdr.page_table_id)?;
        entry_idx += 1;
        if !unique && last_row_id == Some(row_id) {
            continue;
        }
        last_row_id = Some(row_id);
        entries.push(IndexEntry { key, row_id });
        if entries.len() >= max_entries {
            let resume_after = (entry_idx > examined_start)
                .then(|| decode_raw_leaf_entry_at(&hdr, entry_idx - 1))
                .transpose()?;
            return Ok(RawLeafRangeEntries {
                entries,
                resume_after,
                examined: entry_idx.saturating_sub(examined_start),
                rejected: 0,
                next_leaf: hdr.next_leaf,
                next_entry_idx: entry_idx,
                last_row_id,
                page_max_trx_id: hdr.page_max_trx_id,
                positioned,
                exhausted_leaf: false,
                terminal: false,
            });
        }
    }

    let resume_after = (entry_idx > examined_start)
        .then(|| decode_raw_leaf_entry_at(&hdr, entry_idx - 1))
        .transpose()?;
    Ok(RawLeafRangeEntries {
        entries,
        resume_after,
        examined: entry_idx.saturating_sub(examined_start),
        rejected: 0,
        next_leaf: hdr.next_leaf,
        next_entry_idx: entry_idx,
        last_row_id,
        page_max_trx_id: hdr.page_max_trx_id,
        positioned,
        exhausted_leaf: !terminal,
        terminal,
    })
}

/// Runtime-filtered counterpart of `range_leaf_entries_page_bytes`. It reads
/// the leading integer key and fixed RowId trailer first, and only decodes the
/// complete `IndexKey` for admitted rows. This matters for broad covering
/// scans where a join-built exact set rejects most leaf entries.
fn range_leaf_entries_page_bytes_filtered_first_int(
    buf: &[u8],
    low: Option<&IndexBound>,
    high: Option<&IndexBound>,
    mut entry_idx: usize,
    mut positioned: bool,
    mut last_row_id: Option<RowId>,
    unique: bool,
    max_entries: usize,
    mut admits: impl FnMut(Option<i128>) -> bool,
) -> Result<RawLeafRangeEntries, QueryError> {
    let hdr = parse_leaf_payload_header(buf)?;

    if !positioned {
        entry_idx = raw_leaf_low_start!(&hdr, low);
        if entry_idx < hdr.count {
            positioned = true;
        }
    }

    let (end, terminal) = raw_leaf_high_end(&hdr, high)?;
    let examined_start = entry_idx;
    let mut rejected = 0usize;
    let mut entries = Vec::with_capacity(max_entries.min(end.saturating_sub(entry_idx)));
    while entry_idx < end {
        let slot = hdr.read_slot(entry_idx);
        let record_start = slot.offset as usize;
        let record_end = record_start.checked_add(slot.len as usize).ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: leaf record overflow",
            )
        })?;
        if slot.len as usize <= ROW_ID_V7_ENCODED_LEN
            || record_start < LEAF_OFF_ENTRIES
            || record_end > hdr.slot_array_start
        {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: leaf record cannot contain RowId",
            ));
        }
        let row_id_start = record_end - ROW_ID_V7_ENCODED_LEN;
        let mut row_cursor = row_id_start;
        let row_id = decode_row_id_v7_from(hdr.payload, &mut row_cursor, hdr.page_table_id)?;
        debug_assert_eq!(row_cursor, record_end);
        entry_idx += 1;
        if !unique && last_row_id == Some(row_id) {
            continue;
        }
        last_row_id = Some(row_id);

        let mut first_cursor = record_start;
        let first = hdr.decode_first_lossless_i128_at(&mut first_cursor)?;
        if !admits(first) {
            rejected += 1;
            continue;
        }

        let mut key_cursor = record_start;
        let key = hdr.decode_key_at(&mut key_cursor)?;
        if key_cursor != row_id_start {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: key length does not reach RowId trailer",
            ));
        }
        entries.push(IndexEntry { key, row_id });
        if entries.len() >= max_entries {
            let resume_after = (entry_idx > examined_start)
                .then(|| decode_raw_leaf_entry_at(&hdr, entry_idx - 1))
                .transpose()?;
            return Ok(RawLeafRangeEntries {
                entries,
                resume_after,
                examined: entry_idx.saturating_sub(examined_start),
                rejected,
                next_leaf: hdr.next_leaf,
                next_entry_idx: entry_idx,
                last_row_id,
                page_max_trx_id: hdr.page_max_trx_id,
                positioned,
                exhausted_leaf: false,
                terminal: false,
            });
        }
    }

    let resume_after = (entry_idx > examined_start)
        .then(|| decode_raw_leaf_entry_at(&hdr, entry_idx - 1))
        .transpose()?;
    Ok(RawLeafRangeEntries {
        entries,
        resume_after,
        examined: entry_idx.saturating_sub(examined_start),
        rejected,
        next_leaf: hdr.next_leaf,
        next_entry_idx: entry_idx,
        last_row_id,
        page_max_trx_id: hdr.page_max_trx_id,
        positioned,
        exhausted_leaf: !terminal,
        terminal,
    })
}

/// Per-entry destination for `IndexRangeCursor` chunk pulls. One walker
/// body serves both consumers: owned `(IndexKey, RowId)` tuples for
/// callers that project from the key (covering scans, resume
/// bookkeeping) and bare `RowId`s for callers that immediately fetch
/// heap rows — the latter skips the per-entry `IndexKey` clone (one
/// `Vec<KeyPart>` allocation plus String/Bytes payloads per row) that
/// non-covering scans were paying for keys they never looked at.
trait RangeChunkSink {
    fn push(&mut self, entry: &IndexEntry);
    fn push_owned(&mut self, entry: IndexEntry) {
        self.push(&entry);
    }
    fn push_raw(
        &mut self,
        header: &LeafPayloadHeader<'_>,
        slot: usize,
        row_id: RowId,
    ) -> Result<(), QueryError>;
    fn len(&self) -> usize;
}

impl RangeChunkSink for Vec<(IndexKey, RowId)> {
    #[inline]
    fn push(&mut self, entry: &IndexEntry) {
        Vec::push(self, (entry.key.clone(), entry.row_id));
    }
    #[inline]
    fn push_owned(&mut self, entry: IndexEntry) {
        Vec::push(self, (entry.key, entry.row_id));
    }
    fn push_raw(
        &mut self,
        header: &LeafPayloadHeader<'_>,
        slot: usize,
        row_id: RowId,
    ) -> Result<(), QueryError> {
        let mut cursor = header.read_slot_offset(slot);
        Vec::push(self, (header.decode_key_at(&mut cursor)?, row_id));
        Ok(())
    }
    #[inline]
    fn len(&self) -> usize {
        Vec::len(self)
    }
}

impl RangeChunkSink for Vec<RowId> {
    #[inline]
    fn push(&mut self, entry: &IndexEntry) {
        Vec::push(self, entry.row_id);
    }
    fn push_raw(
        &mut self,
        _header: &LeafPayloadHeader<'_>,
        _slot: usize,
        row_id: RowId,
    ) -> Result<(), QueryError> {
        Vec::push(self, row_id);
        Ok(())
    }
    #[inline]
    fn len(&self) -> usize {
        Vec::len(self)
    }
}

/// Source of the old index keys an MVCC snapshot still needs. A writer
/// registers a retired key before it physically deletes the entry, so a
/// cursor that reads a position after that delete must merge the key back
/// from the repair directory for snapshots older than the writer's commit.
pub(crate) trait IndexRepairFeed: Send {
    /// Whether the index has any retired keys at all (cheap, lock-free).
    fn active(&self) -> bool;
    /// See [`Database::index_repair_rollback_epoch`]. A read whose span this
    /// moved across must be repeated.
    fn rollback_epoch(&self) -> u64;
    /// Snapshot-visible retired keys within the bounds, in ascending order,
    /// or `None` when the directory is unchanged since the previous call.
    fn fetch_if_changed(
        &mut self,
        low: Option<&IndexBound>,
        high: Option<&IndexBound>,
    ) -> Result<Option<Vec<(IndexKey, RowId)>>, QueryError>;
}

/// Repair merging state of one ordered cursor. Once engaged it stays engaged:
/// RowId suppression must outlive the directory entries that caused it.
struct CursorRepairs {
    feed: Box<dyn IndexRepairFeed>,
    engaged: bool,
    /// Retired keys not yet returned, in output order.
    pending: std::collections::VecDeque<(IndexKey, RowId)>,
    /// RowIds with a visible retired key. Their physical entries are newer
    /// versions this snapshot must not return.
    row_ids: std::collections::HashSet<RowId>,
    /// Retired entries already taken. A row updated several times has one
    /// per update; visibility later keeps only the snapshot's own version.
    seen: std::collections::HashSet<(IndexKey, RowId)>,
    /// Repaired rows already returned to a RowId-only consumer, which cannot
    /// tell the snapshot's own key from the row's other retired keys.
    returned_row_ids: std::collections::HashSet<RowId>,
}

impl std::fmt::Debug for CursorRepairs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CursorRepairs")
            .field("engaged", &self.engaged)
            .field("pending", &self.pending.len())
            .field("row_ids", &self.row_ids.len())
            .finish()
    }
}

impl CursorRepairs {
    fn new(feed: Box<dyn IndexRepairFeed>) -> Self {
        Self {
            feed,
            engaged: false,
            pending: std::collections::VecDeque::new(),
            row_ids: std::collections::HashSet::new(),
            seen: std::collections::HashSet::new(),
            returned_row_ids: std::collections::HashSet::new(),
        }
    }

    fn active(&self) -> bool {
        self.engaged || self.feed.active()
    }

    /// Merge one physical read. `before` is the position examined before the
    /// read and `through` the position after it; `finished` flushes every
    /// remaining retired key.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    fn merge(
        &mut self,
        physical: Vec<(IndexKey, RowId)>,
        before: Option<&IndexEntry>,
        through: Option<&IndexEntry>,
        finished: bool,
        reverse: bool,
        low: Option<&IndexBound>,
        high: Option<&IndexBound>,
    ) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        self.absorb_fresh(before, reverse, low, high)?;
        Ok(self.emit(physical, through, finished, reverse))
    }

    /// [`Self::merge`] for a physical read that started at rollback epoch
    /// `epoch_before`. `None` means a rolled-back writer removed retired keys
    /// during the read: the caller rewinds and repeats it, since the read
    /// may have missed a key whose repair the refresh no longer returns.
    #[allow(clippy::too_many_arguments)]
    fn merge_checked(
        &mut self,
        epoch_before: u64,
        physical: Vec<(IndexKey, RowId)>,
        before: Option<&IndexEntry>,
        through: Option<&IndexEntry>,
        finished: bool,
        reverse: bool,
        low: Option<&IndexBound>,
        high: Option<&IndexBound>,
    ) -> Result<Option<Vec<(IndexKey, RowId)>>, QueryError> {
        self.absorb_fresh(before, reverse, low, high)?;
        if self.feed.rollback_epoch() != epoch_before {
            return Ok(None);
        }
        Ok(Some(self.emit(physical, through, finished, reverse)))
    }

    fn rollback_epoch(&self) -> u64 {
        self.feed.rollback_epoch()
    }

    fn order(reverse: bool) -> impl Fn(&(IndexKey, RowId), &IndexKey, RowId) -> std::cmp::Ordering {
        move |left, key, row_id| {
            let order = left.0.cmp(key).then_with(|| left.1.cmp(&row_id));
            if reverse {
                order.reverse()
            } else {
                order
            }
        }
    }

    /// Fold repairs registered since the previous refresh into the pending
    /// queue and the RowId suppression set.
    fn absorb_fresh(
        &mut self,
        before: Option<&IndexEntry>,
        reverse: bool,
        low: Option<&IndexBound>,
        high: Option<&IndexBound>,
    ) -> Result<(), QueryError> {
        self.engaged = true;
        let order = Self::order(reverse);
        if let Some(fresh) = self.feed.fetch_if_changed(low, high)? {
            let mut added = false;
            for entry in fresh {
                // A retired key behind `before` was still physical when this
                // cursor passed it. Its RowId still suppresses newer keys.
                self.row_ids.insert(entry.1);
                if !self.seen.contains(&entry)
                    && before.is_none_or(|pos| order(&entry, &pos.key, pos.row_id).is_gt())
                {
                    self.seen.insert(entry.clone());
                    self.pending.push_back(entry);
                    added = true;
                }
            }
            if added {
                self.pending
                    .make_contiguous()
                    .sort_unstable_by(|left, right| order(left, &right.0, right.1));
            }
        }
        Ok(())
    }

    /// Merge one physical read with the pending repairs in output order.
    fn emit(
        &mut self,
        mut physical: Vec<(IndexKey, RowId)>,
        through: Option<&IndexEntry>,
        finished: bool,
        reverse: bool,
    ) -> Vec<(IndexKey, RowId)> {
        let order = Self::order(reverse);
        physical.retain(|entry| !self.row_ids.contains(&entry.1));
        let mut out = Vec::with_capacity(physical.len());
        let mut physical = physical.into_iter().peekable();
        loop {
            let repair_first = match (self.pending.front(), physical.peek()) {
                (Some(repair), Some(next)) => order(repair, &next.0, next.1).is_lt(),
                (Some(repair), None) => {
                    finished
                        || through.is_some_and(|pos| order(repair, &pos.key, pos.row_id).is_le())
                }
                (None, _) => false,
            };
            if repair_first {
                out.extend(self.pending.pop_front());
            } else if let Some(entry) = physical.next() {
                out.push(entry);
            } else {
                break;
            }
        }
        out
    }
}

/// Entries per pull when a repair-merging cursor serves a visitor.
const VISIT_LEAF_CHUNK: usize = 4096;

/// Drive a borrowed-entry visitor from owned, repair-merged leaf pulls.
fn visit_repairing_leaf_chunks<F>(
    mut next: impl FnMut() -> Result<(Vec<(IndexKey, RowId)>, u64), QueryError>,
    mut visitor: F,
) -> Result<(), QueryError>
where
    F: FnMut(&[(&IndexKey, RowId)], u64) -> Result<(), QueryError>,
{
    loop {
        let (entries, bound) = next()?;
        if entries.is_empty() {
            return Ok(());
        }
        let borrowed: Vec<(&IndexKey, RowId)> =
            entries.iter().map(|(key, row_id)| (key, *row_id)).collect();
        visitor(&borrowed, bound)?;
    }
}

impl CursorRepairs {
    /// RowId view of a merged chunk: a repaired row is returned once.
    fn row_ids(&mut self, entries: Vec<(IndexKey, RowId)>) -> Vec<RowId> {
        entries
            .into_iter()
            .map(|entry| entry.1)
            .filter(|row_id| {
                !self.row_ids.contains(row_id) || self.returned_row_ids.insert(*row_id)
            })
            .collect()
    }
}

/// Leading integer key part as seen by first-int raw filters.
fn first_key_part_as_i128(key: &IndexKey) -> Option<i128> {
    key.0.first().and_then(|part| match part {
        KeyPart::Int(value) => Some(i128::from(*value)),
        KeyPart::UInt(value) => Some(i128::from(*value)),
        KeyPart::Date(value) => Some(i128::from(*value)),
        KeyPart::Time(value) | KeyPart::DateTime(value) => Some(i128::from(*value)),
        KeyPart::Year(value) => Some(if *value == 0 {
            0
        } else {
            1900 + i128::from(*value)
        }),
        _ => None,
    })
}

/// Forward-only range cursor that remembers the current leaf and slot
/// position so repeated chunk pulls keep walking the sibling chain
/// instead of re-descending from the root on every refill.
#[derive(Debug)]
pub(crate) struct IndexRangeCursor {
    index: IndexHandle,
    low: Option<IndexBound>,
    high: Option<IndexBound>,
    leaf_id: Option<u64>,
    entry_idx: usize,
    last_row_id: Option<RowId>,
    /// Last physical entry examined. It is one small cursor-owned resume key,
    /// not a page image; if B-tree mutation invalidates `leaf_id`, re-seeking
    /// strictly after this entry preserves progress across a merge/reuse.
    resume_after: Option<IndexEntry>,
    observed_mutation_generation: u64,
    unique: bool,
    finished: bool,
    repairs: Option<CursorRepairs>,
    /// False until the first in-range slot has been located via
    /// `partition_point` on the low bound. While unpositioned, leaves
    /// whose entries all sort before the low bound are skipped whole
    /// (an exclusive prefix bound can cover several leaves). Once set,
    /// per-entry low-bound checks are dead weight — every later entry
    /// is >= the first in-range one by leaf-chain ordering — so the
    /// walker never runs them again.
    positioned: bool,
}

// Expand preparation with each reader's own closure captures. Holding the
// structure lock and frame latch across the generation check remains explicit.
macro_rules! read_raw_leaf {
    ($cursor:ident, |$buf:ident| $read:expr) => {{
        let index = Arc::clone(&$cursor.index);
        let index_file = index.read();
        let _structure = index_file.structure_lock.read_recursive();
        // A writer may have published its begin epoch but not changed slots
        // yet. Check both the epoch and active writers under the frame latch;
        // a position computed by reseek from an earlier snapshot is not enough.
        // Read active first: if the last writer finishes between the loads,
        // its end epoch must still force repositioning.
        let generation_before = $cursor.observed_mutation_generation;
        $cursor.reseek_after_mutation(&index_file)?;
        let leaf_id = $cursor.leaf_id.ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "index range cursor has no current leaf",
            )
        })?;
        let pinned = index_file
            .buffer_pool
            .pin_page(index_file.page_key(leaf_id))?;
        let result = pinned.with_index_raw_bytes(|$buf| {
            if index_file.active_mutations.load(Ordering::Acquire) != 0
                || generation_before != index_file.mutation_generation.load(Ordering::Acquire)
            {
                if let Some(resume) = &$cursor.resume_after {
                    $cursor.entry_idx =
                        raw_leaf_resume_position(&parse_leaf_payload_header($buf)?, resume, true)?;
                    $cursor.positioned = true;
                }
            }
            $read
        });
        drop(pinned);
        result
    }};
}

impl IndexRangeCursor {
    fn new(
        index: IndexHandle,
        unique: bool,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
    ) -> Result<Self, QueryError> {
        let (leaf_id, observed_mutation_generation) = {
            let index_file = index.read();
            let _structure = index_file.structure_lock.read_recursive();
            index_file.ensure_healthy()?;
            let leaf_id = match low.as_ref() {
                Some(bound) => index_file.find_leaf_for_key_lower_bound(&bound.key)?,
                None => index_file.leftmost_leaf()?,
            };
            (
                leaf_id,
                index_file.mutation_generation.load(Ordering::Acquire),
            )
        };
        let positioned = low.is_none();
        Ok(Self {
            index,
            low,
            high,
            leaf_id: Some(leaf_id),
            entry_idx: 0,
            last_row_id: None,
            resume_after: None,
            observed_mutation_generation,
            unique,
            finished: false,
            repairs: None,
            positioned,
        })
    }

    /// An already-exhausted cursor that yields no rows, built without
    /// touching the index. Used when a deferred index-bound parameter binds
    /// to NULL: a non-null-safe range predicate (`col < ?`, `col BETWEEN ?
    /// AND ?`) is then UNKNOWN for every row, so the scan is empty
    /// (see `Database::resolve_index_bound`).
    pub(crate) fn empty(index: IndexHandle, unique: bool) -> Self {
        Self {
            index,
            low: None,
            high: None,
            leaf_id: None,
            entry_idx: 0,
            last_row_id: None,
            resume_after: None,
            observed_mutation_generation: 0,
            unique,
            finished: true,
            repairs: None,
            positioned: true,
        }
    }

    /// Merge MVCC repairs for `txn`'s snapshot into everything this cursor
    /// returns (see [`IndexRepairFeed`]).
    pub(crate) fn with_repair_feed(mut self, feed: Box<dyn IndexRepairFeed>) -> Self {
        if !self.finished {
            self.repairs = Some(CursorRepairs::new(feed));
        }
        self
    }

    fn repairs_active(&self) -> bool {
        self.repairs.as_ref().is_some_and(CursorRepairs::active)
    }

    /// Run a physical-only read unless repairs must be merged. A key retired
    /// during the read itself is detected afterwards: the read is discarded
    /// and the caller repeats it through [`Self::repaired_chunk`].
    fn physical_read<T>(
        &mut self,
        read: impl FnOnce(&mut Self) -> Result<T, QueryError>,
    ) -> Result<Option<T>, QueryError> {
        if self.repairs.is_none() {
            return read(self).map(Some);
        }
        if self.repairs_active() {
            return Ok(None);
        }
        let before = self.resume_after.clone();
        let epoch = self.repair_rollback_epoch();
        let value = read(self)?;
        if self.repairs_active() || self.repair_rollback_epoch() != epoch {
            self.rewind_to(before);
            return Ok(None);
        }
        Ok(Some(value))
    }

    fn repair_rollback_epoch(&self) -> u64 {
        self.repairs
            .as_ref()
            .map_or(0, CursorRepairs::rollback_epoch)
    }

    fn rewind_to(&mut self, position: Option<IndexEntry>) {
        self.resume_after = position;
        // Force `reseek_after_mutation` on the next read.
        self.observed_mutation_generation = u64::MAX;
        self.finished = false;
        if self.leaf_id.is_none() {
            self.leaf_id = Some(INDEX_EMPTY_ROOT);
        }
    }

    /// Keyed read merged with the snapshot's retired keys. Never returns an
    /// empty chunk before the range is exhausted.
    fn repaired_chunk(&mut self, chunk_size: usize) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        loop {
            let before = self.resume_after.clone();
            let epoch = self.repair_rollback_epoch();
            let mut physical = Vec::with_capacity(chunk_size);
            self.fill_chunk(&mut physical, chunk_size)?;
            let repairs = self
                .repairs
                .as_mut()
                .expect("repair-merging cursor has a feed");
            let Some(out) = repairs.merge_checked(
                epoch,
                physical,
                before.as_ref(),
                self.resume_after.as_ref(),
                self.finished,
                false,
                self.low.as_ref(),
                self.high.as_ref(),
            )?
            else {
                self.rewind_to(before);
                continue;
            };
            if !out.is_empty() || self.finished {
                return Ok(out);
            }
        }
    }

    pub(crate) fn next_chunk(
        &mut self,
        chunk_size: usize,
    ) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        if self.finished || chunk_size == 0 {
            return Ok(Vec::new());
        }
        if let Some(entries) = self.physical_read(|cursor| {
            let mut entries: Vec<(IndexKey, RowId)> = Vec::with_capacity(chunk_size);
            cursor.fill_chunk(&mut entries, chunk_size)?;
            Ok(entries)
        })? {
            return Ok(entries);
        }
        self.repaired_chunk(chunk_size)
    }

    /// Materialize the remaining bounded range through the raw leaf decoder.
    /// Unlike `IndexFile::range_entries`, this decodes only slots inside the
    /// requested bounds and reuses one destination allocation across leaves.
    /// Callers that require a materialized set anyway (notably MVCC repair
    /// merging) use this instead of the streaming `next_chunk` interface.
    pub(crate) fn collect_remaining(mut self) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        const GROW_ROWS: usize = 2048;
        let mut entries = Vec::new();
        if self.repairs.is_some() {
            loop {
                let chunk = self.next_chunk(GROW_ROWS)?;
                if chunk.is_empty() {
                    return Ok(entries);
                }
                entries.extend(chunk);
            }
        }
        while !self.finished {
            let target = entries.len().saturating_add(GROW_ROWS);
            self.fill_chunk(&mut entries, target)?;
        }
        Ok(entries)
    }

    /// Key-free chunk pull for non-covering consumers: same walk as
    /// `next_chunk`, but the sink stores bare `RowId`s so no `IndexKey`
    /// is ever cloned.
    pub(crate) fn next_chunk_row_ids(
        &mut self,
        chunk_size: usize,
    ) -> Result<Vec<RowId>, QueryError> {
        if self.finished || chunk_size == 0 {
            return Ok(Vec::new());
        }
        if let Some(row_ids) = self.physical_read(|cursor| {
            let mut row_ids: Vec<RowId> = Vec::with_capacity(chunk_size);
            cursor.fill_chunk_row_ids_raw(&mut row_ids, chunk_size)?;
            Ok(row_ids)
        })? {
            return Ok(row_ids);
        }
        let entries = self.repaired_chunk(chunk_size)?;
        Ok(self
            .repairs
            .as_mut()
            .expect("repair-merging cursor has a feed")
            .row_ids(entries))
    }

    /// Single-leaf chunk pull: emit up to the current leaf's remaining
    /// span, then stop and return the leaf's `page_max_trx_id`.
    /// Callers combine that bound with
    /// `Database::try_index_page_max_trx_shortcut` to skip the
    /// per-entry visibility check when the whole leaf resolves via the
    /// InnoDB-style shortcut. The next call resumes on the next leaf.
    ///
    /// For a nonzero `soft_cap`, an empty chunk means the scan is exhausted
    /// and its bound is zero. Empty or fully filtered leaves are skipped.
    /// Nonempty chunks contain entries from one physical leaf snapshot and
    /// return that snapshot's `page_max_trx_id`. A cap of zero does no work.
    /// The next call may continue within the same leaf when the cap is reached.
    /// A repair-merged chunk reports `u64::MAX`, which no shortcut accepts.
    pub(crate) fn next_leaf_chunk(
        &mut self,
        soft_cap: usize,
    ) -> Result<(Vec<(IndexKey, RowId)>, u64), QueryError> {
        if self.finished || soft_cap == 0 {
            return Ok((Vec::new(), 0));
        }
        if let Some(chunk) = self.physical_read(|cursor| {
            let mut entries: Vec<(IndexKey, RowId)> = Vec::with_capacity(soft_cap);
            let page_max_trx_id = cursor.fill_one_leaf(&mut entries, soft_cap)?;
            Ok((entries, page_max_trx_id))
        })? {
            return Ok(chunk);
        }
        Ok((self.repaired_chunk(soft_cap)?, u64::MAX))
    }

    /// Leading-integer-filtered covering pull. `examined` includes rejected
    /// leaf entries, while `rejected` counts rows omitted before full key
    /// materialisation. Counters include skipped leaves; the bound belongs
    /// only to the leaf that supplied the returned entries (zero if empty).
    pub(crate) fn next_leaf_chunk_filtered_first_int(
        &mut self,
        soft_cap: usize,
        mut admits: impl FnMut(Option<i128>) -> bool,
    ) -> Result<(Vec<(IndexKey, RowId)>, u64, usize, usize), QueryError> {
        if self.finished || soft_cap == 0 {
            return Ok((Vec::new(), 0, 0, 0));
        }
        if let Some(chunk) = self.physical_read(|cursor| {
            let mut entries: Vec<(IndexKey, RowId)> = Vec::with_capacity(soft_cap);
            let (page_max_trx_id, examined, rejected) =
                cursor.fill_one_leaf_filtered_first_int(&mut entries, soft_cap, &mut admits)?;
            Ok((entries, page_max_trx_id, examined, rejected))
        })? {
            return Ok(chunk);
        }
        let mut entries = self.repaired_chunk(soft_cap)?;
        let examined = entries.len();
        entries.retain(|entry| admits(first_key_part_as_i128(&entry.0)));
        let rejected = examined - entries.len();
        Ok((entries, u64::MAX, examined, rejected))
    }

    /// Row-id-only single-leaf pull: see [`next_leaf_chunk`].
    pub(crate) fn next_leaf_chunk_row_ids(
        &mut self,
        soft_cap: usize,
    ) -> Result<(Vec<RowId>, u64), QueryError> {
        if self.finished || soft_cap == 0 {
            return Ok((Vec::new(), 0));
        }
        if let Some(chunk) = self.physical_read(|cursor| {
            let mut row_ids: Vec<RowId> = Vec::with_capacity(soft_cap);
            let page_max_trx_id = cursor.fill_one_leaf_row_ids_raw(&mut row_ids, soft_cap)?;
            Ok((row_ids, page_max_trx_id))
        })? {
            return Ok(chunk);
        }
        let entries = self.repaired_chunk(soft_cap)?;
        let row_ids = self
            .repairs
            .as_mut()
            .expect("repair-merging cursor has a feed")
            .row_ids(entries);
        Ok((row_ids, u64::MAX))
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.finished
    }

    /// Last physical entry this cursor has examined. Every in-range entry at
    /// or before it, in index order, has already been returned.
    pub(crate) fn resume_position(&self) -> Option<(IndexKey, RowId)> {
        self.resume_after
            .as_ref()
            .map(|entry| (entry.key.clone(), entry.row_id))
    }

    /// Continue strictly after `position` (from the range start when `None`)
    /// on the next read, discarding whatever was read beyond it.
    pub(crate) fn rewind_after(&mut self, position: Option<(IndexKey, RowId)>) {
        self.resume_after = position.map(|(key, row_id)| IndexEntry { key, row_id });
        // Force `reseek_after_mutation` on the next read.
        self.observed_mutation_generation = u64::MAX;
        self.finished = false;
        if self.leaf_id.is_none() {
            self.leaf_id = Some(INDEX_EMPTY_ROOT);
        }
    }

    #[cfg(test)]
    pub(crate) fn current_leaf_id_for_tests(&self) -> Option<u64> {
        self.leaf_id
    }

    /// Re-resolve the cursor's physical position after any page-wide index
    /// mutation. `structure_lock` is held across this check, the root descent,
    /// and the following page pin/read, so a merge cannot turn the freshly
    /// resolved leaf into a freelist page in the hand-off window.
    fn reseek_after_mutation(&mut self, index_file: &IndexFile) -> Result<(), QueryError> {
        let generation = index_file.mutation_generation.load(Ordering::Acquire);
        if generation == self.observed_mutation_generation {
            return Ok(());
        }

        if let Some(resume_after) = self.resume_after.as_ref() {
            let leaf_id = index_file.find_leaf_for_entry(resume_after, &mut Vec::new())?;
            let leaf = index_file.read_leaf(leaf_id)?;
            self.leaf_id = Some(leaf_id);
            self.entry_idx = leaf.entries.partition_point(|entry| entry <= resume_after);
            self.positioned = true;
            self.last_row_id = Some(resume_after.row_id);
        } else {
            let leaf_id = match self.low.as_ref() {
                Some(bound) => index_file.find_leaf_for_key_lower_bound(&bound.key)?,
                None => index_file.leftmost_leaf()?,
            };
            self.leaf_id = Some(leaf_id);
            self.entry_idx = 0;
            self.positioned = self.low.is_none();
            self.last_row_id = None;
        }
        self.finished = false;
        self.observed_mutation_generation = generation;
        Ok(())
    }

    fn read_raw_leaf_row_ids(&mut self, max_rows: usize) -> Result<RawLeafRangeRows, QueryError> {
        read_raw_leaf!(self, |buf| range_leaf_row_ids_page_bytes(
            buf,
            self.low.as_ref(),
            self.high.as_ref(),
            self.entry_idx,
            self.positioned,
            self.last_row_id,
            self.unique,
            true,
            max_rows,
        ))
    }

    fn read_raw_leaf_entries(
        &mut self,
        max_entries: usize,
    ) -> Result<RawLeafRangeEntries, QueryError> {
        read_raw_leaf!(self, |buf| range_leaf_entries_page_bytes(
            buf,
            self.low.as_ref(),
            self.high.as_ref(),
            self.entry_idx,
            self.positioned,
            self.last_row_id,
            self.unique,
            max_entries,
        ))
    }

    fn read_raw_leaf_entries_filtered_first_int(
        &mut self,
        max_entries: usize,
        admits: impl FnMut(Option<i128>) -> bool,
    ) -> Result<RawLeafRangeEntries, QueryError> {
        read_raw_leaf!(
            self,
            |buf| range_leaf_entries_page_bytes_filtered_first_int(
                buf,
                self.low.as_ref(),
                self.high.as_ref(),
                self.entry_idx,
                self.positioned,
                self.last_row_id,
                self.unique,
                max_entries,
                admits,
            )
        )
    }

    /// Queue read-ahead only after the cursor has proved that its range
    /// crosses the current leaf. Bounded probes normally terminate inside
    /// their first leaf; prefetching before `leaf_span` computed `terminal`
    /// decoded and walked unrelated sibling pages on every such query.
    fn prefetch_after_leaf(&self, next_leaf: Option<u64>) {
        let Some(next_leaf) = next_leaf else {
            return;
        };
        let index_file = self.index.read();
        let _structure = index_file.structure_lock.read_recursive();
        index_file.prefetch_next_leaves(next_leaf);
    }

    fn fill_chunk<S: RangeChunkSink>(
        &mut self,
        sink: &mut S,
        chunk_size: usize,
    ) -> Result<(), QueryError> {
        while sink.len() < chunk_size {
            let Some(_leaf_id) = self.leaf_id else {
                self.finished = true;
                break;
            };
            let pulled = self.read_raw_leaf_entries(chunk_size - sink.len())?;
            self.entry_idx = pulled.next_entry_idx;
            self.last_row_id = pulled.last_row_id;
            self.positioned = pulled.positioned;
            if let Some(resume_after) = pulled.resume_after {
                self.resume_after = Some(resume_after);
            }
            for entry in pulled.entries {
                sink.push_owned(entry);
            }
            if pulled.terminal {
                self.finished = true;
                self.leaf_id = None;
                break;
            }
            if !pulled.exhausted_leaf {
                break;
            }
            self.prefetch_after_leaf(pulled.next_leaf);
            self.leaf_id = pulled.next_leaf;
            self.entry_idx = 0;
            if self.leaf_id.is_none() {
                self.finished = true;
                break;
            }
        }
        Ok(())
    }

    fn fill_chunk_row_ids_raw(
        &mut self,
        sink: &mut Vec<RowId>,
        chunk_size: usize,
    ) -> Result<(), QueryError> {
        while sink.len() < chunk_size {
            let Some(_leaf_id) = self.leaf_id else {
                self.finished = true;
                break;
            };
            let pulled = self.read_raw_leaf_row_ids(chunk_size - sink.len())?;
            self.entry_idx = pulled.next_entry_idx;
            self.last_row_id = pulled.last_row_id;
            self.positioned = pulled.positioned;
            if let Some(resume_after) = pulled.resume_after {
                self.resume_after = Some(resume_after);
            }
            sink.extend(pulled.rows);
            if pulled.terminal {
                self.finished = true;
                self.leaf_id = None;
                break;
            }
            if !pulled.exhausted_leaf {
                break;
            }
            self.prefetch_after_leaf(pulled.next_leaf);
            self.leaf_id = pulled.next_leaf;
            self.entry_idx = 0;
            if self.leaf_id.is_none() {
                self.finished = true;
                break;
            }
        }
        Ok(())
    }

    /// Single-leaf pull backing [`next_leaf_chunk`] and
    /// [`next_leaf_chunk_row_ids`]. Same walk as `fill_chunk`, but
    /// stops when transitioning to the next leaf so the caller sees a
    /// batch drawn from exactly one physical leaf and can trust the
    /// returned `page_max_trx_id` bound. Empty leaves (all entries
    /// filtered by low-bound) are skipped transparently; the returned
    /// bound is the leaf we actually emitted from (or 0 if the scan
    /// ended without emitting anything).
    fn fill_one_leaf<S: RangeChunkSink>(
        &mut self,
        sink: &mut S,
        soft_cap: usize,
    ) -> Result<u64, QueryError> {
        loop {
            let Some(_leaf_id) = self.leaf_id else {
                self.finished = true;
                return Ok(0);
            };
            let pulled = self.read_raw_leaf_entries(soft_cap - sink.len())?;
            let page_bound = if pulled.entries.is_empty() {
                0
            } else {
                pulled.page_max_trx_id
            };
            self.entry_idx = pulled.next_entry_idx;
            self.last_row_id = pulled.last_row_id;
            self.positioned = pulled.positioned;
            if let Some(resume_after) = pulled.resume_after {
                self.resume_after = Some(resume_after);
            }
            for entry in pulled.entries {
                sink.push_owned(entry);
            }
            if pulled.terminal {
                self.finished = true;
                self.leaf_id = None;
                return Ok(page_bound);
            }
            if !pulled.exhausted_leaf {
                return Ok(page_bound);
            }
            self.prefetch_after_leaf(pulled.next_leaf);
            self.leaf_id = pulled.next_leaf;
            self.entry_idx = 0;
            if self.leaf_id.is_none() {
                self.finished = true;
            }
            if sink.len() > 0 || self.finished {
                return Ok(page_bound);
            }
        }
    }

    fn fill_one_leaf_filtered_first_int<S: RangeChunkSink>(
        &mut self,
        sink: &mut S,
        soft_cap: usize,
        mut admits: impl FnMut(Option<i128>) -> bool,
    ) -> Result<(u64, usize, usize), QueryError> {
        let mut examined = 0usize;
        let mut rejected = 0usize;
        loop {
            let Some(_leaf_id) = self.leaf_id else {
                self.finished = true;
                return Ok((0, examined, rejected));
            };
            let pulled =
                self.read_raw_leaf_entries_filtered_first_int(soft_cap - sink.len(), &mut admits)?;
            examined = examined.saturating_add(pulled.examined);
            rejected = rejected.saturating_add(pulled.rejected);
            let page_bound = if pulled.entries.is_empty() {
                0
            } else {
                pulled.page_max_trx_id
            };
            self.entry_idx = pulled.next_entry_idx;
            self.last_row_id = pulled.last_row_id;
            self.positioned = pulled.positioned;
            if let Some(resume_after) = pulled.resume_after {
                self.resume_after = Some(resume_after);
            }
            for entry in pulled.entries {
                sink.push_owned(entry);
            }
            if pulled.terminal {
                self.finished = true;
                self.leaf_id = None;
                return Ok((page_bound, examined, rejected));
            }
            if !pulled.exhausted_leaf {
                return Ok((page_bound, examined, rejected));
            }
            self.prefetch_after_leaf(pulled.next_leaf);
            self.leaf_id = pulled.next_leaf;
            self.entry_idx = 0;
            if self.leaf_id.is_none() {
                self.finished = true;
            }
            if sink.len() > 0 || self.finished {
                return Ok((page_bound, examined, rejected));
            }
        }
    }

    fn fill_one_leaf_row_ids_raw(
        &mut self,
        sink: &mut Vec<RowId>,
        soft_cap: usize,
    ) -> Result<u64, QueryError> {
        loop {
            let Some(_leaf_id) = self.leaf_id else {
                self.finished = true;
                return Ok(0);
            };
            let pulled = self.read_raw_leaf_row_ids(soft_cap - sink.len())?;
            let page_bound = if pulled.rows.is_empty() {
                0
            } else {
                pulled.page_max_trx_id
            };
            self.entry_idx = pulled.next_entry_idx;
            self.last_row_id = pulled.last_row_id;
            self.positioned = pulled.positioned;
            if let Some(resume_after) = pulled.resume_after.as_ref() {
                self.resume_after = Some(resume_after.clone());
            }
            sink.extend(pulled.rows);
            if pulled.terminal {
                self.finished = true;
                self.leaf_id = None;
                return Ok(page_bound);
            }
            if !pulled.exhausted_leaf {
                return Ok(page_bound);
            }
            self.prefetch_after_leaf(pulled.next_leaf);
            self.leaf_id = pulled.next_leaf;
            self.entry_idx = 0;
            if self.leaf_id.is_none() {
                self.finished = true;
            }
            if !sink.is_empty() || self.finished {
                return Ok(page_bound);
            }
        }
    }

    /// Walk the remaining range, lending one batch of borrowed entries
    /// per leaf to `visitor`. Covering consumers project rows straight
    /// from the lent `&IndexKey`s, skipping the per-entry clone that
    /// `next_chunk` pays. The borrow is only valid for the duration of
    /// the call (it points into the pinned leaf page).
    pub(crate) fn visit_entries<F>(&mut self, mut visitor: F) -> Result<(), QueryError>
    where
        F: FnMut(&[(&IndexKey, RowId)]) -> Result<(), QueryError>,
    {
        self.visit_entries_with_bound(|batch, _bound| visitor(batch))
    }

    /// Variant of [`visit_entries`] that passes each leaf's
    /// `page_max_trx_id` alongside the borrowed entries. Covering
    /// consumers can hand `bound` to
    /// `Database::try_index_page_max_trx_shortcut` to skip visibility
    /// filtering for that leaf.
    pub(crate) fn visit_entries_with_bound<F>(&mut self, mut visitor: F) -> Result<(), QueryError>
    where
        F: FnMut(&[(&IndexKey, RowId)], u64) -> Result<(), QueryError>,
    {
        if self.repairs.is_some() {
            return visit_repairing_leaf_chunks(|| self.next_leaf_chunk(VISIT_LEAF_CHUNK), visitor);
        }
        while !self.finished {
            let Some(_leaf_id) = self.leaf_id else {
                self.finished = true;
                break;
            };
            let pulled = self.read_raw_leaf_entries(usize::MAX)?;
            let page_bound = pulled.page_max_trx_id;
            self.entry_idx = pulled.next_entry_idx;
            self.last_row_id = pulled.last_row_id;
            self.positioned = pulled.positioned;
            if let Some(resume_after) = pulled.resume_after.as_ref() {
                self.resume_after = Some(resume_after.clone());
            }
            let buf: Vec<(&IndexKey, RowId)> = pulled
                .entries
                .iter()
                .map(|entry| (&entry.key, entry.row_id))
                .collect();
            if !buf.is_empty() {
                visitor(&buf, page_bound)?;
            }
            if pulled.terminal {
                self.finished = true;
                self.leaf_id = None;
                break;
            }
            debug_assert!(pulled.exhausted_leaf);
            self.prefetch_after_leaf(pulled.next_leaf);
            self.leaf_id = pulled.next_leaf;
            self.entry_idx = 0;
            if self.leaf_id.is_none() {
                self.finished = true;
                break;
            }
        }
        Ok(())
    }
}

/// Reverse range cursor: remembers the current leaf and slot so chunk
/// refills keep walking right-to-left without re-descending from the
/// root on every refill. The previous `resume_before` scheme paid a
/// full root descent per (512-row) chunk plus a per-entry resume-key
/// comparison across the first leaf; this cursor pays one descent at
/// construction and one `prev_leaf` descent per leaf transition.
#[derive(Debug)]
pub(crate) struct IndexRangeReverseCursor {
    index: IndexHandle,
    low: Option<IndexBound>,
    high: Option<IndexBound>,
    leaf_id: Option<u64>,
    /// One past the next entry to emit; the walk consumes entries at
    /// `entry_idx - 1` down to 0. Only meaningful while `positioned`
    /// and not `pending_reset`.
    entry_idx: usize,
    /// High-bound positioning done: the first emitted slot was located
    /// via `partition_point`, so no later entry can violate the high
    /// bound (previous leaves hold strictly smaller keys).
    positioned: bool,
    /// Set on a leaf transition: `entry_idx` must be reset to the new
    /// leaf's entry count before consuming from it.
    pending_reset: bool,
    last_row_id: Option<RowId>,
    resume_before: Option<IndexEntry>,
    observed_mutation_generation: u64,
    unique: bool,
    finished: bool,
    repairs: Option<CursorRepairs>,
}

impl IndexRangeReverseCursor {
    fn new(
        index: IndexHandle,
        unique: bool,
        low: Option<IndexBound>,
        high: Option<IndexBound>,
    ) -> Result<Self, QueryError> {
        let (leaf_id, observed_mutation_generation) = {
            let index_file = index.read();
            let _structure = index_file.structure_lock.read_recursive();
            let leaf_id =
                index_file.find_leaf_for_reverse_range_start(low.as_ref(), high.as_ref(), None)?;
            (
                leaf_id,
                index_file.mutation_generation.load(Ordering::Acquire),
            )
        };
        Ok(Self {
            index,
            low,
            high,
            leaf_id: Some(leaf_id),
            entry_idx: 0,
            positioned: false,
            pending_reset: false,
            last_row_id: None,
            resume_before: None,
            observed_mutation_generation,
            unique,
            finished: false,
            repairs: None,
        })
    }

    /// An already-exhausted reverse cursor that yields no rows, built without
    /// touching the index. See [`IndexRangeCursor::empty`].
    pub(crate) fn empty(index: IndexHandle, unique: bool) -> Self {
        Self {
            index,
            low: None,
            high: None,
            leaf_id: None,
            entry_idx: 0,
            positioned: true,
            pending_reset: false,
            last_row_id: None,
            resume_before: None,
            observed_mutation_generation: 0,
            unique,
            finished: true,
            repairs: None,
        }
    }

    /// Reverse counterpart of [`IndexRangeCursor::with_repair_feed`].
    pub(crate) fn with_repair_feed(mut self, feed: Box<dyn IndexRepairFeed>) -> Self {
        if !self.finished {
            self.repairs = Some(CursorRepairs::new(feed));
        }
        self
    }

    fn repairs_active(&self) -> bool {
        self.repairs.as_ref().is_some_and(CursorRepairs::active)
    }

    /// Reverse counterpart of [`IndexRangeCursor::physical_read`].
    fn physical_read<T>(
        &mut self,
        read: impl FnOnce(&mut Self) -> Result<T, QueryError>,
    ) -> Result<Option<T>, QueryError> {
        if self.repairs.is_none() {
            return read(self).map(Some);
        }
        if self.repairs_active() {
            return Ok(None);
        }
        let before = self.resume_before.clone();
        let epoch = self.repair_rollback_epoch();
        let value = read(self)?;
        if self.repairs_active() || self.repair_rollback_epoch() != epoch {
            self.rewind_to(before);
            return Ok(None);
        }
        Ok(Some(value))
    }

    fn repair_rollback_epoch(&self) -> u64 {
        self.repairs
            .as_ref()
            .map_or(0, CursorRepairs::rollback_epoch)
    }

    fn rewind_to(&mut self, position: Option<IndexEntry>) {
        self.resume_before = position;
        // Force `reseek_after_mutation` on the next read.
        self.observed_mutation_generation = u64::MAX;
        self.finished = false;
        if self.leaf_id.is_none() {
            self.leaf_id = Some(INDEX_EMPTY_ROOT);
        }
    }

    fn repaired_chunk(&mut self, chunk_size: usize) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        loop {
            let before = self.resume_before.clone();
            let epoch = self.repair_rollback_epoch();
            let mut physical = Vec::with_capacity(chunk_size);
            self.fill_chunk(&mut physical, chunk_size)?;
            let repairs = self
                .repairs
                .as_mut()
                .expect("repair-merging cursor has a feed");
            let Some(out) = repairs.merge_checked(
                epoch,
                physical,
                before.as_ref(),
                self.resume_before.as_ref(),
                self.finished,
                true,
                self.low.as_ref(),
                self.high.as_ref(),
            )?
            else {
                self.rewind_to(before);
                continue;
            };
            if !out.is_empty() || self.finished {
                return Ok(out);
            }
        }
    }

    pub(crate) fn next_chunk(
        &mut self,
        chunk_size: usize,
    ) -> Result<Vec<(IndexKey, RowId)>, QueryError> {
        if self.finished || chunk_size == 0 {
            return Ok(Vec::new());
        }
        if let Some(entries) = self.physical_read(|cursor| {
            let mut entries: Vec<(IndexKey, RowId)> = Vec::with_capacity(chunk_size);
            cursor.fill_chunk(&mut entries, chunk_size)?;
            Ok(entries)
        })? {
            return Ok(entries);
        }
        self.repaired_chunk(chunk_size)
    }

    /// Key-free chunk pull for non-covering consumers; see
    /// `IndexRangeCursor::next_chunk_row_ids`.
    pub(crate) fn next_chunk_row_ids(
        &mut self,
        chunk_size: usize,
    ) -> Result<Vec<RowId>, QueryError> {
        if self.finished || chunk_size == 0 {
            return Ok(Vec::new());
        }
        if let Some(row_ids) = self.physical_read(|cursor| {
            let mut row_ids: Vec<RowId> = Vec::with_capacity(chunk_size);
            cursor.fill_chunk(&mut row_ids, chunk_size)?;
            Ok(row_ids)
        })? {
            return Ok(row_ids);
        }
        let entries = self.repaired_chunk(chunk_size)?;
        Ok(self
            .repairs
            .as_mut()
            .expect("repair-merging cursor has a feed")
            .row_ids(entries))
    }

    /// Reverse counterpart of [`IndexRangeCursor::next_leaf_chunk`].
    pub(crate) fn next_leaf_chunk(
        &mut self,
        soft_cap: usize,
    ) -> Result<(Vec<(IndexKey, RowId)>, u64), QueryError> {
        if self.finished || soft_cap == 0 {
            return Ok((Vec::new(), 0));
        }
        if let Some(chunk) = self.physical_read(|cursor| {
            let mut entries: Vec<(IndexKey, RowId)> = Vec::with_capacity(soft_cap);
            let page_max_trx_id = cursor.fill_one_leaf(&mut entries, soft_cap)?;
            Ok((entries, page_max_trx_id))
        })? {
            return Ok(chunk);
        }
        Ok((self.repaired_chunk(soft_cap)?, u64::MAX))
    }

    /// Reverse counterpart of [`IndexRangeCursor::next_leaf_chunk_row_ids`].
    #[allow(dead_code)]
    pub(crate) fn next_leaf_chunk_row_ids(
        &mut self,
        soft_cap: usize,
    ) -> Result<(Vec<RowId>, u64), QueryError> {
        if self.finished || soft_cap == 0 {
            return Ok((Vec::new(), 0));
        }
        if let Some(chunk) = self.physical_read(|cursor| {
            let mut row_ids: Vec<RowId> = Vec::with_capacity(soft_cap);
            let page_max_trx_id = cursor.fill_one_leaf(&mut row_ids, soft_cap)?;
            Ok((row_ids, page_max_trx_id))
        })? {
            return Ok(chunk);
        }
        let entries = self.repaired_chunk(soft_cap)?;
        let row_ids = self
            .repairs
            .as_mut()
            .expect("repair-merging cursor has a feed")
            .row_ids(entries);
        Ok((row_ids, u64::MAX))
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.finished
    }

    /// Last physical entry this cursor has examined. Every in-range entry at
    /// or after it, in index order, has already been returned.
    pub(crate) fn resume_position(&self) -> Option<(IndexKey, RowId)> {
        self.resume_before
            .as_ref()
            .map(|entry| (entry.key.clone(), entry.row_id))
    }

    /// Continue strictly before `position` (from the range end when `None`)
    /// on the next read, discarding whatever was read beyond it.
    pub(crate) fn rewind_before(&mut self, position: Option<(IndexKey, RowId)>) {
        self.resume_before = position.map(|(key, row_id)| IndexEntry { key, row_id });
        // Force `reseek_after_mutation` on the next read.
        self.observed_mutation_generation = u64::MAX;
        self.finished = false;
        if self.leaf_id.is_none() {
            self.leaf_id = Some(INDEX_EMPTY_ROOT);
        }
    }

    /// Position within `leaf` (first call bisects the high bound;
    /// post-transition leaves start at their end) and return the
    /// emission stop slot. `terminal` means the low bound cuts inside
    /// this leaf, so the scan finishes once the walk reaches `stop`.
    fn leaf_span(&mut self, leaf: &IndexLeafPage) -> ReverseLeafSpan {
        if !self.positioned {
            // Bisect to one past the last entry within the high bound.
            // A leaf whose entries all sort past the high bound is
            // skipped whole and positioning retries on the previous
            // one.
            let end = match self.high.as_ref() {
                Some(_) => leaf.entries.partition_point(|entry| {
                    !index_key_past_high_bound(&entry.key, self.high.as_ref())
                }),
                None => leaf.entries.len(),
            };
            if end == 0 {
                return ReverseLeafSpan::Skip;
            }
            self.entry_idx = end;
            self.positioned = true;
            self.pending_reset = false;
        } else if self.pending_reset {
            // Fresh leaf after a transition: previous leaves hold
            // strictly smaller keys, so every entry satisfies the high
            // bound and the walk starts from the leaf's end.
            self.entry_idx = leaf.entries.len();
            self.pending_reset = false;
        }
        let straddles_low = match self.low.as_ref() {
            Some(low) => leaf
                .entries
                .first()
                .is_some_and(|first| index_key_before_low_bound(&first.key, low)),
            None => false,
        };
        if straddles_low {
            let stop = self.low.as_ref().map_or(0, |low| {
                leaf.entries
                    .partition_point(|entry| index_key_before_low_bound(&entry.key, low))
            });
            ReverseLeafSpan::Span {
                stop,
                terminal: true,
            }
        } else {
            ReverseLeafSpan::Span {
                stop: 0,
                terminal: false,
            }
        }
    }

    fn reseek_after_mutation(&mut self, index_file: &IndexFile) -> Result<(), QueryError> {
        let generation = index_file.mutation_generation.load(Ordering::Acquire);
        if generation == self.observed_mutation_generation {
            return Ok(());
        }
        if let Some(resume_before) = self.resume_before.as_ref() {
            let leaf_id = index_file.find_leaf_for_reverse_range_start(
                self.low.as_ref(),
                self.high.as_ref(),
                Some(&(resume_before.key.clone(), resume_before.row_id)),
            )?;
            let leaf = index_file.read_leaf(leaf_id)?;
            self.leaf_id = Some(leaf_id);
            self.entry_idx = leaf.entries.partition_point(|entry| entry < resume_before);
            self.positioned = true;
            self.pending_reset = false;
            self.last_row_id = Some(resume_before.row_id);
        } else {
            let leaf_id = index_file.find_leaf_for_reverse_range_start(
                self.low.as_ref(),
                self.high.as_ref(),
                None,
            )?;
            self.leaf_id = Some(leaf_id);
            self.entry_idx = 0;
            self.positioned = false;
            self.pending_reset = false;
            self.last_row_id = None;
        }
        self.finished = false;
        self.observed_mutation_generation = generation;
        Ok(())
    }

    fn read_leaf(&mut self) -> Result<Arc<IndexPage>, QueryError> {
        let index = Arc::clone(&self.index);
        let index_file = index.read();
        let _structure = index_file.structure_lock.read_recursive();
        self.reseek_after_mutation(&index_file)?;
        let leaf_id = self.leaf_id.ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "reverse index range cursor has no current leaf",
            )
        })?;
        let page = index_file.read_page(leaf_id)?;
        if let (Some(resume), IndexPage::Leaf(leaf)) = (&self.resume_before, page.as_ref()) {
            self.entry_idx = leaf.entries.partition_point(|entry| entry < resume);
            self.positioned = true;
            self.pending_reset = false;
        }
        Ok(page)
    }

    fn fill_chunk<S: RangeChunkSink>(
        &mut self,
        sink: &mut S,
        chunk_size: usize,
    ) -> Result<(), QueryError> {
        while sink.len() < chunk_size && !self.finished {
            self.fill_one_leaf(sink, chunk_size)?;
        }
        Ok(())
    }

    /// Pull one leaf directly from encoded slots. RowId consumers decode only
    /// the resume key; covering consumers materialize only the emitted keys.
    fn fill_one_leaf<S: RangeChunkSink>(
        &mut self,
        sink: &mut S,
        soft_cap: usize,
    ) -> Result<u64, QueryError> {
        loop {
            if self.leaf_id.is_none() {
                self.finished = true;
                return Ok(0);
            }
            let initial_len = sink.len();
            let (leaf_id, pulled) = {
                let index = Arc::clone(&self.index);
                let index_file = index.read();
                let _structure = index_file.structure_lock.read_recursive();
                let generation_before = self.observed_mutation_generation;
                self.reseek_after_mutation(&index_file)?;
                let leaf_id = self.leaf_id.ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "reverse index range cursor has no current leaf",
                    )
                })?;
                let pinned = index_file
                    .buffer_pool
                    .pin_page(index_file.page_key(leaf_id))?;
                let pulled = pinned.with_index_raw_bytes(|bytes| {
                    let header = parse_leaf_payload_header(bytes)?;
                    if index_file.active_mutations.load(Ordering::Acquire) != 0
                        || generation_before
                            != index_file.mutation_generation.load(Ordering::Acquire)
                    {
                        if let Some(resume) = &self.resume_before {
                            self.entry_idx = raw_leaf_resume_position(&header, resume, false)?;
                            self.positioned = true;
                            self.pending_reset = false;
                        }
                    }
                    let (stop, end, _) =
                        raw_leaf_in_bounds_span(&header, self.low.as_ref(), self.high.as_ref())?;
                    if !self.positioned {
                        if end == 0 {
                            return Ok(None);
                        }
                        self.entry_idx = end;
                        self.positioned = true;
                        self.pending_reset = false;
                    } else if self.pending_reset {
                        self.entry_idx = header.count;
                        self.pending_reset = false;
                    }
                    let before = self.entry_idx;
                    while self.entry_idx > stop {
                        self.entry_idx -= 1;
                        let row_id = decode_raw_leaf_row_id_at(&header, self.entry_idx)?;
                        if !self.unique && self.last_row_id == Some(row_id) {
                            continue;
                        }
                        sink.push_raw(&header, self.entry_idx, row_id)?;
                        self.last_row_id = Some(row_id);
                        if sink.len() >= soft_cap {
                            break;
                        }
                    }
                    if self.entry_idx < before {
                        self.resume_before =
                            Some(decode_raw_leaf_entry_at(&header, self.entry_idx)?);
                    }
                    // A nonzero lower-bound slot means earlier leaves are
                    // below the range. A full chunk keeps its resume position
                    // even if it ended exactly at this terminal boundary.
                    Ok(Some((
                        header.page_max_trx_id,
                        sink.len() >= soft_cap,
                        stop > 0,
                    )))
                })?;
                (leaf_id, pulled)
            };
            let Some((page_bound, full, terminal)) = pulled else {
                self.step_to_prev_leaf(leaf_id)?;
                continue;
            };
            if full {
                return Ok(page_bound);
            }
            if terminal {
                self.finished = true;
                self.leaf_id = None;
            } else {
                self.step_to_prev_leaf(leaf_id)?;
            }
            if sink.len() > initial_len || self.finished {
                return Ok(if sink.len() > initial_len {
                    page_bound
                } else {
                    0
                });
            }
        }
    }

    /// Reverse counterpart of `IndexRangeCursor::visit_entries`: lends
    /// one batch of borrowed entries per leaf, in descending key order.
    pub(crate) fn visit_entries<F>(&mut self, mut visitor: F) -> Result<(), QueryError>
    where
        F: FnMut(&[(&IndexKey, RowId)]) -> Result<(), QueryError>,
    {
        if self.repairs.is_some() {
            return visit_repairing_leaf_chunks(
                || self.next_leaf_chunk(VISIT_LEAF_CHUNK),
                |entries, _bound| visitor(entries),
            );
        }
        while !self.finished {
            let Some(_) = self.leaf_id else {
                self.finished = true;
                break;
            };
            let leaf_page = self.read_leaf()?;
            let leaf_id = self.leaf_id.expect("read_leaf establishes a current leaf");
            let leaf = match leaf_page.as_ref() {
                IndexPage::Leaf(leaf) => leaf,
                _ => {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: expected leaf page",
                    ));
                }
            };
            let (stop, terminal) = match self.leaf_span(leaf) {
                ReverseLeafSpan::Skip => {
                    self.step_to_prev_leaf(leaf_id)?;
                    continue;
                }
                ReverseLeafSpan::Span { stop, terminal } => (stop, terminal),
            };
            let mut buf: Vec<(&IndexKey, RowId)> =
                Vec::with_capacity(self.entry_idx.saturating_sub(stop));
            let entry_idx_before = self.entry_idx;
            while self.entry_idx > stop {
                let entry = &leaf.entries[self.entry_idx - 1];
                if !self.unique && self.last_row_id == Some(entry.row_id) {
                    self.entry_idx -= 1;
                    continue;
                }
                self.last_row_id = Some(entry.row_id);
                buf.push((&entry.key, entry.row_id));
                self.entry_idx -= 1;
            }
            if self.entry_idx < entry_idx_before {
                self.resume_before = Some(leaf.entries[self.entry_idx].clone());
            }
            if !buf.is_empty() {
                visitor(&buf)?;
            }
            if terminal {
                self.finished = true;
                self.leaf_id = None;
                break;
            }
            self.step_to_prev_leaf(leaf_id)?;
        }
        Ok(())
    }

    fn step_to_prev_leaf(&mut self, leaf_id: u64) -> Result<(), QueryError> {
        let prev = {
            let index = Arc::clone(&self.index);
            let index_file = index.read();
            let _structure = index_file.structure_lock.read_recursive();
            if index_file.mutation_generation.load(Ordering::Acquire)
                != self.observed_mutation_generation
            {
                self.reseek_after_mutation(&index_file)?;
                return Ok(());
            }
            index_file.prev_leaf(leaf_id)?
        };
        self.leaf_id = prev;
        self.pending_reset = true;
        if prev.is_none() {
            self.finished = true;
        }
        Ok(())
    }
}

/// Per-leaf disposition for the reverse range cursor.
enum ReverseLeafSpan {
    /// Every entry sorts past the high bound — skip to the previous
    /// leaf.
    Skip,
    /// Consume entries from the current slot down to (exclusive)
    /// `stop`; `terminal` means the low bound cuts inside this leaf
    /// and the scan finishes there.
    Span { stop: usize, terminal: bool },
}

#[cfg(test)]
mod logged_delete_batch_tests {
    use super::*;

    fn fixture() -> (tempfile::TempDir, Arc<IndexFile>) {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            crate::wal::LogManager::open(&dir.path().join("wal")).unwrap(),
        ));
        let pool = BufferPool::new(
            256,
            log.clone(),
            DurabilityMode::Strict,
            Duration::from_millis(DEFAULT_LOCK_WAIT_MS),
        );
        let file = Arc::new(
            IndexFile::open_with_durability(
                dir.path().join("delete.idx"),
                log,
                DurabilityMode::Strict,
                pool,
            )
            .unwrap(),
        );
        (dir, file)
    }

    #[test]
    fn logged_delete_batch_can_share_unchanged_topology() {
        let (_dir, file) = fixture();
        let entries: Vec<_> = (0..100u16)
            .map(|n| IndexEntry {
                key: IndexKey::from(vec![KeyPart::Int(i64::from(n))]),
                row_id: RowId {
                    page_id: 1,
                    slot_id: n,
                },
            })
            .collect();
        file.bulk_load_sorted(txn_id_for_ddl(), entries.clone())
            .unwrap();
        let deletes = [10usize, 70].map(|n| IndexBatchEntry {
            kind: WalIndexOpKind::Delete,
            key: entries[n].key.clone(),
            row_id: entries[n].row_id,
        });
        let lsn = file
            .append_mixed_index_batch_wal(7, &deletes)
            .unwrap()
            .unwrap();
        let topology = file.structure_lock.read();
        let (tx, rx) = std::sync::mpsc::channel();
        let worker_file = Arc::clone(&file);
        let worker = std::thread::spawn(move || {
            tx.send(worker_file.apply_batch_with_lsn(7, deletes.into(), Some(lsn)))
                .unwrap();
        });
        let result = rx.recv_timeout(Duration::from_millis(200));
        drop(topology);
        worker.join().unwrap();
        result
            .expect("leaf-only deletion must not wait for a topology reader")
            .unwrap();
        for (n, entry) in entries.iter().enumerate() {
            assert_eq!(
                file.get_unique_one(&entry.key).unwrap(),
                (![10, 70].contains(&n)).then_some(entry.row_id)
            );
        }
    }

    #[test]
    fn leaf_update_can_raise_a_nonroot_minimum_without_changing_topology() {
        let (_dir, file) = fixture();
        let entries: Vec<_> = (0..4000u16)
            .map(|n| IndexEntry {
                key: IndexKey::from(vec![KeyPart::Int(i64::from(n) * 4)]),
                row_id: RowId {
                    page_id: 1,
                    slot_id: n,
                },
            })
            .collect();
        file.bulk_load_sorted(txn_id_for_ddl(), entries.clone())
            .unwrap();
        let first = file.read_leaf(file.leftmost_leaf().unwrap()).unwrap();
        let leaf_id = first.next_leaf.unwrap();
        let leaf = file.read_leaf(leaf_id).unwrap();
        let old = leaf.entries[0].clone();
        let [KeyPart::Int(value)] = old.key.0.as_slice() else {
            panic!("integer fixture");
        };
        let new = IndexEntry {
            key: IndexKey::from(vec![KeyPart::Int(value + 1)]),
            row_id: old.row_id,
        };
        let changes = [
            IndexBatchEntry {
                kind: WalIndexOpKind::Delete,
                key: old.key.clone(),
                row_id: old.row_id,
            },
            IndexBatchEntry {
                kind: WalIndexOpKind::Insert,
                key: new.key.clone(),
                row_id: new.row_id,
            },
        ];
        let lsn = file
            .append_mixed_index_batch_wal(7, &changes)
            .unwrap()
            .unwrap();
        assert!(
            file.try_apply_two_op_update_batch_fast(7, &changes, Some(lsn))
                .unwrap(),
            "raising a leaf minimum preserves its parent lower bound"
        );
        file.buffer_pool.flush_and_evict_all().unwrap();
        assert!(file.get(&old.key, false).unwrap().is_empty());
        assert_eq!(file.get(&new.key, false).unwrap(), vec![old.row_id]);
        let replacement_row = RowId {
            page_id: 2,
            slot_id: 1,
        };
        file.insert_with_txn(8, old.key.clone(), replacement_row)
            .unwrap();
        assert_eq!(file.get(&old.key, false).unwrap(), vec![replacement_row]);
        let all = file.entries().unwrap();
        assert_eq!(all.len(), entries.len() + 1);
        assert!(all.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn cross_leaf_nullable_update_can_remove_the_source_minimum() {
        let (_dir, file) = fixture();
        let mut entries: Vec<_> = (0..4u16)
            .flat_map(|group| {
                (0..600u16).map(move |n| IndexEntry {
                    key: IndexKey::from(vec![
                        KeyPart::Int(1),
                        KeyPart::Int(1),
                        if group == 0 {
                            KeyPart::Null
                        } else {
                            KeyPart::Int(i64::from(group))
                        },
                        KeyPart::Int(i64::from(n) * 4),
                    ]),
                    row_id: RowId {
                        page_id: 1,
                        slot_id: group * 600 + n,
                    },
                })
            })
            .collect();
        entries.sort_unstable();
        file.bulk_load_sorted(txn_id_for_ddl(), entries.clone())
            .unwrap();
        let first = file.read_leaf(file.leftmost_leaf().unwrap()).unwrap();
        let source_id = first.next_leaf.unwrap();
        let source = file.read_leaf(source_id).unwrap();
        let old = source.entries[0].clone();
        assert_eq!(old.key.0[2], KeyPart::Null);
        let KeyPart::Int(order) = old.key.0[3] else {
            panic!("integer order");
        };
        let new = IndexEntry {
            key: IndexKey::from(vec![
                KeyPart::Int(1),
                KeyPart::Int(1),
                KeyPart::Int(2),
                KeyPart::Int(order + 1),
            ]),
            row_id: old.row_id,
        };
        let destination_id = file
            .find_leaf_for_entry_with_candidate(&new, &new)
            .unwrap()
            .0
            .frame
            .key
            .page_id;
        assert_ne!(source_id, destination_id);
        // Bulk construction can fill the destination completely. Make room
        // through the ordinary delete path so this tests the separator rule,
        // while leaving overflow/underflow on the structural oracle.
        let destination = file.read_leaf(destination_id).unwrap();
        let removed_destination = destination.entries[destination.entries.len() / 2].clone();
        file.delete_deferred(
            8,
            removed_destination.key.clone(),
            removed_destination.row_id,
        )
        .unwrap();
        assert!(
            encoded_leaf_page_len(&source) > INDEX_MIN_PAGE_BYTES + 128,
            "source fixture must have deletion headroom: {}",
            encoded_leaf_page_len(&source)
        );
        let changes = [
            IndexBatchEntry {
                kind: WalIndexOpKind::Delete,
                key: old.key.clone(),
                row_id: old.row_id,
            },
            IndexBatchEntry {
                kind: WalIndexOpKind::Insert,
                key: new.key.clone(),
                row_id: new.row_id,
            },
        ];
        let lsn = file
            .append_mixed_index_batch_wal(7, &changes)
            .unwrap()
            .unwrap();
        assert!(
            file.try_apply_two_op_update_batch_fast(7, &changes, Some(lsn))
                .unwrap(),
            "a non-underflowing source can retain a conservative separator"
        );
        file.buffer_pool.flush_and_evict_all().unwrap();
        assert!(file.get(&old.key, false).unwrap().is_empty());
        assert_eq!(file.get(&new.key, false).unwrap(), vec![old.row_id]);
        let mut expected: Vec<_> = entries
            .into_iter()
            .filter(|entry| entry != &old && entry != &removed_destination)
            .map(|entry| (entry.key, entry.row_id))
            .collect();
        expected.push((new.key, new.row_id));
        expected.sort_unstable();
        assert_eq!(file.entries().unwrap(), expected);
    }

    #[test]
    fn logged_delete_batch_preserves_composite_keys_through_merges_and_root_collapse() {
        let (_dir, file) = fixture();
        let entries: Vec<_> = (0..1000u16)
            .map(|n| IndexEntry {
                key: IndexKey::from(vec![
                    KeyPart::Int(i64::from(n)),
                    KeyPart::Null,
                    KeyPart::Bytes(vec![n as u8; 20 + usize::from(n % 130)]),
                ]),
                row_id: RowId {
                    page_id: 1,
                    slot_id: n,
                },
            })
            .collect();
        file.bulk_load_sorted(txn_id_for_ddl(), entries.clone())
            .unwrap();
        assert_ne!(
            file.root_page.load(Ordering::Acquire),
            file.leftmost_leaf().unwrap()
        );
        let delete = |entry: &IndexEntry| IndexBatchEntry {
            kind: WalIndexOpKind::Delete,
            key: entry.key.clone(),
            row_id: entry.row_id,
        };
        let mut deletes: Vec<_> = entries
            .iter()
            .enumerate()
            .filter(|(n, _)| n % 137 != 0)
            .map(|(_, entry)| delete(entry))
            .collect();
        // Reapplying an already-deleted entry must remain idempotent after
        // neighboring leaves have merged and their old route has disappeared.
        deletes.push(delete(&entries[1]));
        let lsn = file
            .append_mixed_index_batch_wal(7, &deletes)
            .unwrap()
            .unwrap();
        file.apply_batch_with_lsn(7, deletes, Some(lsn)).unwrap();
        let survivors: Vec<_> = entries.iter().step_by(137).collect();
        assert_eq!(
            file.range(None, None, false).unwrap(),
            survivors
                .iter()
                .map(|entry| entry.row_id)
                .collect::<Vec<_>>()
        );
        let deletes: Vec<_> = survivors.into_iter().map(delete).collect();
        let lsn = file
            .append_mixed_index_batch_wal(8, &deletes)
            .unwrap()
            .unwrap();
        file.apply_batch_with_lsn(8, deletes, Some(lsn)).unwrap();
        assert!(file.range(None, None, false).unwrap().is_empty());
        assert_eq!(file.active_mutations.load(Ordering::Acquire), 0);
        assert_eq!(btree_logical_wal::current(), None);
    }
}

#[cfg(test)]
mod range_cursor_topology_tests {
    use super::*;

    // Model the legal interleaving: writer publishes its begin epoch, reader
    // pulls a chunk, writer changes slots, reader pulls again before writer end.
    #[test]
    fn cursor_active_writer_epoch_repositions_in_consumed_snapshot() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            crate::wal::LogManager::open(&dir.path().join("wal")).unwrap(),
        ));
        let index = IndexFile::open(dir.path().join("epoch.idx"), log).unwrap();
        let entries: Vec<_> = (0..100u16)
            .map(|n| IndexEntry {
                key: IndexKey::from(vec![KeyPart::Int(i64::from(n) * 2)]),
                row_id: RowId {
                    page_id: 1,
                    slot_id: n,
                },
            })
            .collect();
        index
            .bulk_load_sorted(txn_id_for_ddl(), entries.clone())
            .unwrap();
        let index = Arc::new(parking_lot::RwLock::new(index));
        let file = index.read();
        let _topology = file.structure_lock.read_recursive();
        let _writer = file.begin_mutation();
        let mut forward = IndexRangeCursor::new(Arc::clone(&index), true, None, None).unwrap();
        let mut rows = IndexRangeCursor::new(Arc::clone(&index), true, None, None).unwrap();
        let mut filtered = IndexRangeCursor::new(Arc::clone(&index), true, None, None).unwrap();
        let mut reverse =
            IndexRangeReverseCursor::new(Arc::clone(&index), true, None, None).unwrap();
        assert_eq!(forward.next_chunk(17).unwrap().len(), 17);
        assert_eq!(rows.next_chunk_row_ids(17).unwrap().len(), 17);
        assert_eq!(
            filtered
                .next_leaf_chunk_filtered_first_int(17, |_| true)
                .unwrap()
                .0
                .len(),
            17
        );
        assert_eq!(reverse.next_chunk(17).unwrap().len(), 17);
        let leaf = file.leftmost_leaf().unwrap();
        let inserted = IndexEntry {
            key: IndexKey::from(vec![KeyPart::Int(1)]),
            row_id: RowId {
                page_id: 2,
                slot_id: 0,
            },
        };
        file.buffer_pool
            .try_modify_raw_page(&file.page_key(leaf), 1, |bytes| {
                assert!(matches!(
                    try_insert_raw_leaf(bytes, &inserted, 100)?,
                    Some(InPlaceInsert::Inserted)
                ));
                Ok((true, ()))
            })
            .unwrap()
            .expect("cursor has pinned the raw leaf");
        let expected = &entries[17];
        assert_eq!(
            forward.next_chunk(1).unwrap(),
            vec![(expected.key.clone(), expected.row_id)]
        );
        assert_eq!(rows.next_chunk_row_ids(1).unwrap(), vec![expected.row_id]);
        assert_eq!(
            filtered
                .next_leaf_chunk_filtered_first_int(1, |_| true)
                .unwrap()
                .0,
            vec![(expected.key.clone(), expected.row_id)]
        );
        // Insertion before the reverse position would otherwise skip key 164.
        let expected = &entries[82];
        assert_eq!(
            reverse.next_chunk(1).unwrap(),
            vec![(expected.key.clone(), expected.row_id)]
        );
    }

    #[test]
    fn forward_cursor_reseeks_after_remembered_leaf_is_merged() {
        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            crate::wal::LogManager::open(&dir.path().join("wal")).unwrap(),
        ));
        let index = IndexFile::open(dir.path().join("status_ts.idx"), log).unwrap();
        let entries = (0..12_000u32)
            .map(|id| IndexEntry {
                key: IndexKey::from(vec![
                    KeyPart::String("under_review".to_string()),
                    KeyPart::Int(id as i64),
                ]),
                row_id: RowId {
                    page_id: id / 256,
                    slot_id: (id % 256) as u16,
                },
            })
            .collect::<Vec<_>>();
        index
            .bulk_load_sorted(txn_id_for_ddl(), entries.clone())
            .unwrap();
        let index: IndexHandle = Arc::new(parking_lot::RwLock::new(index));

        let rightmost_leaf = index.read().rightmost_leaf().unwrap();
        let first_key = index
            .read()
            .read_leaf(rightmost_leaf)
            .unwrap()
            .entries
            .first()
            .unwrap()
            .key
            .clone();
        let mut cursor = IndexRangeCursor::new(
            Arc::clone(&index),
            true,
            Some(IndexBound {
                key: first_key,
                inclusive: true,
            }),
            None,
        )
        .unwrap();
        let remembered_leaf = cursor.current_leaf_id_for_tests().unwrap();

        // Delete from the high edge down. The remembered rightmost leaf
        // repeatedly borrows from its left neighbour, then merges into that
        // neighbour and becomes a 9-byte freelist page.
        for entry in entries.iter().rev() {
            index
                .write()
                .delete(entry.key.clone(), entry.row_id)
                .unwrap();
            if index
                .read()
                .page_is_free_for_tests(remembered_leaf)
                .unwrap()
            {
                break;
            }
        }
        assert!(
            index
                .read()
                .page_is_free_for_tests(remembered_leaf)
                .unwrap(),
            "descending deletes must merge and free the cursor's leaf"
        );

        assert!(cursor.next_chunk(1).unwrap().is_empty());
        assert_ne!(cursor.current_leaf_id_for_tests(), Some(remembered_leaf));
    }

    #[test]
    fn reverse_cursor_reseeks_after_remembered_leaf_is_merged() {
        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            crate::wal::LogManager::open(&dir.path().join("wal")).unwrap(),
        ));
        let index = IndexFile::open(dir.path().join("reverse.idx"), log).unwrap();
        let entries = (0..12_000u32)
            .map(|id| IndexEntry {
                key: IndexKey::from(vec![KeyPart::Int(id as i64)]),
                row_id: RowId {
                    page_id: id / 256,
                    slot_id: (id % 256) as u16,
                },
            })
            .collect::<Vec<_>>();
        index
            .bulk_load_sorted(txn_id_for_ddl(), entries.clone())
            .unwrap();
        let index: IndexHandle = Arc::new(parking_lot::RwLock::new(index));
        let mut cursor =
            IndexRangeReverseCursor::new(Arc::clone(&index), true, None, None).unwrap();
        let remembered_leaf = cursor.leaf_id.unwrap();

        for entry in entries.iter().rev() {
            index
                .write()
                .delete(entry.key.clone(), entry.row_id)
                .unwrap();
            if index
                .read()
                .page_is_free_for_tests(remembered_leaf)
                .unwrap()
            {
                break;
            }
        }
        assert!(index
            .read()
            .page_is_free_for_tests(remembered_leaf)
            .unwrap());
        let expected = index.read().entries().unwrap().pop().unwrap();
        assert_eq!(cursor.next_chunk(1).unwrap(), vec![expected]);
        assert_ne!(cursor.leaf_id, Some(remembered_leaf));
    }
}

/// Byte-level binary search over a v8 leaf page's raw 16KB buffer.
/// Skips the O(N) `decode_index_tree_page` work by using the
/// trailing slot array (one u16 byte offset per entry, payload-
/// relative, in key-sorted order) for O(log N) random access. Only
/// the O(log N) entries the search probes get key-decoded — and
/// for the canonical "find one row" case, only one RowId is
/// reconstructed.
///
/// Caller pre-validates the page header (or this function does it
/// internally). Returns `LeafLookupResult` describing which RowIds
/// match and whether the caller should walk to `next_leaf`.
fn lookup_leaf_page_bytes_one(
    buf: &[u8],
    target: &IndexKey,
) -> Result<LeafSingleLookupResult, QueryError> {
    let hdr = parse_leaf_payload_header(buf)?;
    if hdr.count == 0 {
        return Ok(LeafSingleLookupResult {
            row: None,
            next_leaf: hdr.next_leaf,
            should_continue: true,
        });
    }

    if hdr.key_layout == KEY_LAYOUT_COMPACT_INT_U16 {
        if let [KeyPart::Int(target)] = target.0.as_slice() {
            // Compact integer pages store `key - base_time`. For dense keys,
            // that delta is also the sorted slot, so an existing primary-key
            // lookup can be verified with a single record probe. Sparse keys,
            // deletes, and any other false guess retain the exact binary-search
            // path below.
            if let Some(delta) = target.checked_sub(hdr.base_time) {
                if let Ok(candidate) = usize::try_from(delta) {
                    if candidate < hdr.count {
                        let (ordering, mut cursor) =
                            hdr.cmp_compact_int_u16_at(candidate, *target)?;
                        if ordering == std::cmp::Ordering::Equal {
                            return Ok(LeafSingleLookupResult {
                                row: Some(decode_row_id_v7_from(
                                    hdr.payload,
                                    &mut cursor,
                                    hdr.page_table_id,
                                )?),
                                next_leaf: hdr.next_leaf,
                                should_continue: false,
                            });
                        }
                    }
                }
            }
            let slot = hdr.lower_bound_compact_int_u16(*target)?;
            if slot >= hdr.count {
                return Ok(LeafSingleLookupResult {
                    row: None,
                    next_leaf: hdr.next_leaf,
                    should_continue: true,
                });
            }
            let (ordering, mut cursor) = hdr.cmp_compact_int_u16_at(slot, *target)?;
            return match ordering {
                std::cmp::Ordering::Equal => Ok(LeafSingleLookupResult {
                    row: Some(decode_row_id_v7_from(
                        hdr.payload,
                        &mut cursor,
                        hdr.page_table_id,
                    )?),
                    next_leaf: hdr.next_leaf,
                    should_continue: false,
                }),
                std::cmp::Ordering::Greater => Ok(LeafSingleLookupResult {
                    row: None,
                    next_leaf: hdr.next_leaf,
                    should_continue: false,
                }),
                std::cmp::Ordering::Less => Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt index: slot array out of order",
                )),
            };
        }
    }

    if let Some(scalar_target) = hdr.fast_scalar_target(target) {
        let lo = hdr.lower_bound_scalar(scalar_target)?;
        if lo >= hdr.count {
            return Ok(LeafSingleLookupResult {
                row: None,
                next_leaf: hdr.next_leaf,
                should_continue: true,
            });
        }
        let (ord, after_key) = hdr.cmp_scalar_at(lo, scalar_target)?;
        return match ord {
            std::cmp::Ordering::Equal => {
                let mut cursor = after_key;
                let row = decode_row_id_v7_from(hdr.payload, &mut cursor, hdr.page_table_id)?;
                Ok(LeafSingleLookupResult {
                    row: Some(row),
                    next_leaf: hdr.next_leaf,
                    should_continue: false,
                })
            }
            std::cmp::Ordering::Greater => Ok(LeafSingleLookupResult {
                row: None,
                next_leaf: hdr.next_leaf,
                should_continue: false,
            }),
            std::cmp::Ordering::Less => Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: slot array out of order",
            )),
        };
    }

    let mut lo = 0usize;
    let mut hi = hdr.count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let (ordering, _) = hdr.cmp_key_at(mid, target)?;
        if ordering == std::cmp::Ordering::Less {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if lo >= hdr.count {
        return Ok(LeafSingleLookupResult {
            row: None,
            next_leaf: hdr.next_leaf,
            should_continue: true,
        });
    }
    let (ordering, mut cursor) = hdr.cmp_key_at(lo, target)?;
    match ordering {
        std::cmp::Ordering::Equal => {
            let row = decode_row_id_v7_from(hdr.payload, &mut cursor, hdr.page_table_id)?;
            Ok(LeafSingleLookupResult {
                row: Some(row),
                next_leaf: hdr.next_leaf,
                should_continue: false,
            })
        }
        std::cmp::Ordering::Greater => Ok(LeafSingleLookupResult {
            row: None,
            next_leaf: hdr.next_leaf,
            should_continue: false,
        }),
        std::cmp::Ordering::Less => Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: slot array out of order",
        )),
    }
}

fn lookup_leaf_page_bytes(buf: &[u8], target: &IndexKey) -> Result<LeafLookupResult, QueryError> {
    let hdr = parse_leaf_payload_header(buf)?;
    if hdr.count == 0 {
        // Empty leaf: target may live on next_leaf (find_leaf
        // navigation can land here when the leaf is the empty
        // root, or via grace-period sibling chain).
        return Ok(LeafLookupResult {
            rows: Vec::new(),
            next_leaf: hdr.next_leaf,
            should_continue: true,
        });
    }

    if hdr.key_layout == KEY_LAYOUT_COMPACT_INT_U16 {
        if let [KeyPart::Int(target)] = target.0.as_slice() {
            let mut slot = hdr.lower_bound_compact_int_u16(*target)?;
            let mut rows = Vec::new();
            let mut saw_greater = false;
            while slot < hdr.count {
                let (ordering, mut cursor) = hdr.cmp_compact_int_u16_at(slot, *target)?;
                match ordering {
                    std::cmp::Ordering::Equal => rows.push(decode_row_id_v7_from(
                        hdr.payload,
                        &mut cursor,
                        hdr.page_table_id,
                    )?),
                    std::cmp::Ordering::Greater => {
                        saw_greater = true;
                        break;
                    }
                    std::cmp::Ordering::Less => {
                        return Err(error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "corrupt index: slot array out of order",
                        ));
                    }
                }
                slot += 1;
            }
            return Ok(LeafLookupResult {
                rows,
                next_leaf: hdr.next_leaf,
                should_continue: !saw_greater,
            });
        }
    }

    // P0-2 fast path — single-column INT / INT UNSIGNED target on a
    // matching compact leaf, or any single-column numeric GENERIC leaf.
    // Skip `IndexKey` reconstruction at every binary-search mid; compare
    // the scalar directly off the page.
    if let Some(scalar_target) = hdr.fast_scalar_target(target) {
        let lo = hdr.lower_bound_scalar(scalar_target)?;
        let mut rows = Vec::new();
        let mut saw_greater = false;
        let mut i = lo;
        while i < hdr.count {
            let (ord, after_key) = hdr.cmp_scalar_at(i, scalar_target)?;
            match ord {
                std::cmp::Ordering::Equal => {
                    let mut cursor = after_key;
                    let rid = decode_row_id_v7_from(hdr.payload, &mut cursor, hdr.page_table_id)?;
                    rows.push(rid);
                }
                std::cmp::Ordering::Greater => {
                    saw_greater = true;
                    break;
                }
                std::cmp::Ordering::Less => {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: slot array out of order",
                    ));
                }
            }
            i += 1;
        }
        return Ok(LeafLookupResult {
            rows,
            next_leaf: hdr.next_leaf,
            should_continue: !saw_greater,
        });
    }

    // Binary search for `partition_point`: smallest slot index
    // whose key is >= target. This is the canonical lower-bound
    // formulation that gracefully handles duplicate keys and
    // missing keys (returns insertion point).
    let mut lo = 0usize;
    let mut hi = hdr.count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let (ordering, _) = hdr.cmp_key_at(mid, target)?;
        if ordering == std::cmp::Ordering::Less {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    // Walk forward from `lo` while key == target, collecting RowIds.
    // Track whether we ever saw a key strictly > target — that's the
    // signal that the scan can stop now (no later leaf could carry
    // an equal key). If we exit the loop without that signal — including
    // the `lo == count` case where the walk body never executes —
    // duplicates may live on `next_leaf`.
    let mut rows = Vec::new();
    let mut saw_greater = false;
    let mut i = lo;
    while i < hdr.count {
        let (ordering, mut cursor) = hdr.cmp_key_at(i, target)?;
        match ordering {
            std::cmp::Ordering::Equal => {
                let rid = decode_row_id_v7_from(hdr.payload, &mut cursor, hdr.page_table_id)?;
                rows.push(rid);
            }
            std::cmp::Ordering::Greater => {
                saw_greater = true;
                break;
            }
            std::cmp::Ordering::Less => {
                // Invariant violation: binary search should not have
                // placed us before a smaller key.
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt index: slot array out of order",
                ));
            }
        }
        i += 1;
    }

    Ok(LeafLookupResult {
        rows,
        next_leaf: hdr.next_leaf,
        should_continue: !saw_greater,
    })
}

#[cfg(test)]
fn internal_child_index_by_key(node: &IndexInternalPage, key: &IndexKey, row_id: RowId) -> usize {
    use std::cmp::Ordering;
    let separator_le = |separator: &IndexEntry| match separator.key.cmp(key) {
        Ordering::Less => true,
        Ordering::Equal => separator.row_id <= row_id,
        Ordering::Greater => false,
    };
    node.keys.partition_point(separator_le)
}

/// Byte-level prefix scan on a v8 leaf page. Returns every RowId
/// whose key starts with `prefix`, plus a flag indicating whether
/// the prefix run may continue on `next_leaf`. Same O(log N) seek
/// + linear walk as `lookup_leaf_page_bytes`, just with a different
/// stop condition.
fn prefix_leaf_page_bytes(buf: &[u8], prefix: &IndexKey) -> Result<LeafLookupResult, QueryError> {
    let hdr = parse_leaf_payload_header(buf)?;
    if hdr.count == 0 {
        return Ok(LeafLookupResult {
            rows: Vec::new(),
            next_leaf: hdr.next_leaf,
            should_continue: true,
        });
    }
    // Lower-bound on prefix.
    let mut lo = 0usize;
    let mut hi = hdr.count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if hdr.cmp_key_at(mid, prefix)?.0 == std::cmp::Ordering::Less {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    // Generic composite indexes dominate multi-column OLTP prefix probes.
    // Compare their encoded parts in place and decode only the RowId on a
    // match; compact layouts retain the layout-specific fallback below.
    if hdr.key_layout == KEY_LAYOUT_GENERIC {
        let mut rows = Vec::new();
        let mut saw_greater = false;
        let mut i = lo;
        while i < hdr.count {
            let mut cursor = hdr.read_slot_offset(i);
            match compare_generic_index_key_prefix_from(hdr.payload, &mut cursor, prefix)? {
                std::cmp::Ordering::Less => {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: slot array out of order",
                    ));
                }
                std::cmp::Ordering::Equal => rows.push(decode_row_id_v7_from(
                    hdr.payload,
                    &mut cursor,
                    hdr.page_table_id,
                )?),
                std::cmp::Ordering::Greater => {
                    saw_greater = true;
                    break;
                }
            }
            i += 1;
        }
        return Ok(LeafLookupResult {
            rows,
            next_leaf: hdr.next_leaf,
            should_continue: !saw_greater,
        });
    }

    let mut rows = Vec::new();
    let mut saw_greater = false;
    let mut i = lo;
    while i < hdr.count {
        let mut cursor = hdr.read_slot_offset(i);
        let entry_key = hdr.decode_key_at(&mut cursor)?;
        if index_key_starts_with(&entry_key, prefix) {
            let rid = decode_row_id_v7_from(hdr.payload, &mut cursor, hdr.page_table_id)?;
            rows.push(rid);
        } else if entry_key > *prefix {
            saw_greater = true;
            break;
        }
        // else: key < prefix → keep scanning (rare; only if prefix
        //       comparison disagrees with index_key_starts_with for
        //       the partial-equal case).
        i += 1;
    }
    Ok(LeafLookupResult {
        rows,
        next_leaf: hdr.next_leaf,
        should_continue: !saw_greater,
    })
}

fn lookup_leaf_entry_page_bytes<E, F: Fn(usize, IndexKey, RowId) -> E>(
    buf: &[u8],
    target: &IndexKey,
    key_index: usize,
    exact: bool,
    seen: &mut Option<BtreeDedupSet>,
    make: &F,
) -> Result<LeafEntryLookupResult<E>, QueryError> {
    let hdr = parse_leaf_payload_header(buf)?;
    if hdr.count == 0 {
        return Ok(LeafEntryLookupResult {
            entries: Vec::new(),
            next_leaf: hdr.next_leaf,
            should_continue: true,
            page_max_trx_id: hdr.page_max_trx_id,
        });
    }

    let mut lo = 0usize;
    let mut hi = hdr.count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        // A full-key comparison has the same lower-bound insertion point as
        // prefix comparison: a stored composite key that starts with
        // `target` sorts immediately after the shorter target. Comparing the
        // encoded key in place removes O(log N) temporary IndexKey allocations;
        // matching entries below are still decoded exactly once for output.
        if hdr.cmp_key_at(mid, target)?.0 == std::cmp::Ordering::Less {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    let mut entries = Vec::new();
    let mut saw_greater = false;
    let mut i = lo;
    while i < hdr.count {
        let mut cursor = hdr.read_slot_offset(i);
        let entry_key = hdr.decode_key_at(&mut cursor)?;
        match compare_index_entry_to_lookup_key(&entry_key, target, exact) {
            std::cmp::Ordering::Less => {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt index: slot array out of order",
                ));
            }
            std::cmp::Ordering::Equal => {
                let row_id = decode_row_id_v7_from(hdr.payload, &mut cursor, hdr.page_table_id)?;
                if seen.as_mut().is_none_or(|seen| seen.insert(row_id)) {
                    entries.push(make(key_index, entry_key, row_id));
                }
            }
            std::cmp::Ordering::Greater => {
                saw_greater = true;
                break;
            }
        }
        i += 1;
    }

    Ok(LeafEntryLookupResult {
        entries,
        next_leaf: hdr.next_leaf,
        should_continue: !saw_greater,
        page_max_trx_id: hdr.page_max_trx_id,
    })
}

fn lookup_leaf_entries_many_page_bytes<E, F: Fn(usize, IndexKey, RowId) -> E>(
    buf: &[u8],
    keys: &[IndexKey],
    base: usize,
    exact: bool,
    seen: &mut Option<BtreeDedupSet>,
    make: &F,
) -> Result<LeafEntriesManyLookupResult<E>, QueryError> {
    let hdr = parse_leaf_payload_header(buf)?;
    if hdr.count == 0 {
        return Ok(LeafEntriesManyLookupResult {
            entries: Vec::new(),
            next_leaf: hdr.next_leaf,
            continue_key_offsets: (0..keys.len()).collect(),
        });
    }

    let mut entries: Vec<E> = Vec::new();
    let mut continue_key_offsets = Vec::new();
    // For dense probes a forward merge costs at most one comparison per
    // skipped slot, instead of K * log2(N) binary-search comparisons. Sparse
    // probes retain logarithmic jumps. Both consume the caller's sorted keys.
    let search_depth = (usize::BITS - hdr.count.leading_zeros()) as usize;
    let dense = keys.len().saturating_mul(search_depth) >= hdr.count;
    let mut search_floor = 0usize;
    for (key_offset, target) in keys.iter().enumerate() {
        let scalar_target = exact.then(|| hdr.fast_scalar_target(target)).flatten();
        let compare_at = |slot| {
            if let Some(target) = scalar_target {
                hdr.cmp_scalar_at(slot, target)
                    .map(|(ordering, _)| ordering)
            } else {
                // Full-key ordering gives the prefix lower bound too, as in
                // scalar entry lookup. Generic parts are compared in place.
                hdr.cmp_key_at(slot, target).map(|(ordering, _)| ordering)
            }
        };
        let mut lo = search_floor;
        if dense && key_offset > 0 {
            while lo < hdr.count && compare_at(lo)? == std::cmp::Ordering::Less {
                lo += 1;
            }
        } else {
            let mut hi = hdr.count;
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                if compare_at(mid)? == std::cmp::Ordering::Less {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
        }
        // Keep the lower bound, not the end of the matching run: repeated
        // keys and overlapping prefixes must still see their matching slots.
        search_floor = lo;

        let mut saw_greater = false;
        let mut i = lo;
        while i < hdr.count {
            let mut cursor = hdr.read_slot_offset(i);
            let entry_key = hdr.decode_key_at(&mut cursor)?;
            match compare_index_entry_to_lookup_key(&entry_key, target, exact) {
                std::cmp::Ordering::Less => {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: slot array out of order",
                    ));
                }
                std::cmp::Ordering::Equal => {
                    let row_id =
                        decode_row_id_v7_from(hdr.payload, &mut cursor, hdr.page_table_id)?;
                    if seen.as_mut().is_none_or(|seen| seen.insert(row_id)) {
                        entries.push(make(base + key_offset, entry_key, row_id));
                    }
                }
                std::cmp::Ordering::Greater => {
                    saw_greater = true;
                    break;
                }
            }
            i += 1;
        }
        if !saw_greater && hdr.next_leaf.is_some() {
            continue_key_offsets.push(key_offset);
        }
    }

    Ok(LeafEntriesManyLookupResult {
        entries,
        next_leaf: hdr.next_leaf,
        continue_key_offsets,
    })
}

fn lookup_leaf_entries_many_keyed_page_bytes(
    buf: &[u8],
    keys: &[IndexKey],
    base: usize,
    exact: bool,
    seen: &mut Option<BtreeDedupSet>,
) -> Result<LeafEntriesManyLookupResult<(u32, RowId)>, QueryError> {
    let hdr = parse_leaf_payload_header(buf)?;
    if hdr.count == 0 {
        return Ok(LeafEntriesManyLookupResult {
            entries: Vec::new(),
            next_leaf: hdr.next_leaf,
            continue_key_offsets: (0..keys.len()).collect(),
        });
    }

    let mut entries: Vec<(u32, RowId)> = Vec::new();
    let mut continue_key_offsets = Vec::new();
    // Both the probe keys and leaf slots are ordered.  Keep the lower
    // bound reached by the previous key instead of restarting every
    // binary search at slot zero.  This turns a page-local batch into
    // an ordered merge with binary skips: each successive lookup only
    // searches the unconsumed suffix of the leaf.
    let mut search_floor = 0usize;
    for (key_offset, target) in keys.iter().enumerate() {
        if exact {
            if let Some(scalar_target) = hdr.fast_scalar_target(target) {
                let mut lo = search_floor;
                let mut hi = hdr.count;
                while lo < hi {
                    let mid = lo + (hi - lo) / 2;
                    let (ord, _) = hdr.cmp_scalar_at(mid, scalar_target)?;
                    if ord == std::cmp::Ordering::Less {
                        lo = mid + 1;
                    } else {
                        hi = mid;
                    }
                }

                let mut saw_greater = false;
                let mut i = lo;
                while i < hdr.count {
                    let (ord, after_key) = hdr.cmp_scalar_at(i, scalar_target)?;
                    match ord {
                        std::cmp::Ordering::Less => {
                            return Err(error(
                                crate::error::catalog::ER_GET_ERRNO,
                                "corrupt index: slot array out of order",
                            ));
                        }
                        std::cmp::Ordering::Equal => {
                            let mut cursor = after_key;
                            let row_id =
                                decode_row_id_v7_from(hdr.payload, &mut cursor, hdr.page_table_id)?;
                            if seen.as_mut().is_none_or(|seen| seen.insert(row_id)) {
                                entries.push(((base + key_offset) as u32, row_id));
                            }
                        }
                        std::cmp::Ordering::Greater => {
                            saw_greater = true;
                            break;
                        }
                    }
                    i += 1;
                }
                search_floor = i;
                if !saw_greater && hdr.next_leaf.is_some() {
                    continue_key_offsets.push(key_offset);
                }
                continue;
            }
        }

        let mut lo = search_floor;
        let mut hi = hdr.count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let mut cursor = hdr.read_slot_offset(mid);
            let mid_key = hdr.decode_key_at(&mut cursor)?;
            if compare_index_entry_to_lookup_key(&mid_key, target, exact)
                == std::cmp::Ordering::Less
            {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        let mut saw_greater = false;
        let mut i = lo;
        while i < hdr.count {
            let mut cursor = hdr.read_slot_offset(i);
            let entry_key = hdr.decode_key_at(&mut cursor)?;
            match compare_index_entry_to_lookup_key(&entry_key, target, exact) {
                std::cmp::Ordering::Less => {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: slot array out of order",
                    ));
                }
                std::cmp::Ordering::Equal => {
                    let row_id =
                        decode_row_id_v7_from(hdr.payload, &mut cursor, hdr.page_table_id)?;
                    if seen.as_mut().is_none_or(|seen| seen.insert(row_id)) {
                        entries.push(((base + key_offset) as u32, row_id));
                    }
                }
                std::cmp::Ordering::Greater => {
                    saw_greater = true;
                    break;
                }
            }
            i += 1;
        }
        search_floor = i;
        if !saw_greater && hdr.next_leaf.is_some() {
            continue_key_offsets.push(key_offset);
        }
    }

    Ok(LeafEntriesManyLookupResult {
        entries,
        next_leaf: hdr.next_leaf,
        continue_key_offsets,
    })
}

/// Mirror of `lookup_leaf_entries_many_page_bytes` for Anti/Semi
/// short-circuit. Same partition-point + scan-until-greater per key,
/// but as soon as `accept(row_id)` returns true we set
/// `matched[base + key_offset]` and break out of that key — we don't
/// need any more matches. Already-matched keys (set by an earlier
/// leaf in the chain) are skipped.
///
/// `accept` runs the caller's exact heap-MVCC visibility check when the leaf
/// writer bound cannot prove all-visible. Errors propagate; on `Ok(false)` we keep scanning so
/// the next inner row for the same key can be tried.
fn keys_first_visible_match_leaf_page<V, P>(
    buf: &[u8],
    keys: &[IndexKey],
    base: usize,
    exact: bool,
    matched: &mut [bool],
    page_all_visible: &mut V,
    accept: &mut P,
) -> Result<KeysFirstMatchLeafResult, QueryError>
where
    V: FnMut(u64) -> bool,
    P: FnMut(&IndexKey, RowId) -> Result<bool, QueryError>,
{
    let hdr = parse_leaf_payload_header(buf)?;
    if hdr.count == 0 {
        let continue_key_offsets = (0..keys.len()).filter(|&i| !matched[base + i]).collect();
        return Ok(KeysFirstMatchLeafResult {
            next_leaf: hdr.next_leaf,
            continue_key_offsets,
        });
    }
    // Existence joins need only prove that one matching entry is visible.
    // When the leaf's monotonic writer bound is below the retained snapshot
    // watermark, every surviving entry on the leaf is visible and decoding
    // its RowId only to perform a random heap-header read is pure overhead.
    // Keep the decision page-local: a prefix can span leaves with different
    // bounds, so a later leaf may still need the ordinary per-row callback.
    let all_visible = page_all_visible(hdr.page_max_trx_id);
    let mut continue_key_offsets = Vec::new();
    for (key_offset, target) in keys.iter().enumerate() {
        if matched[base + key_offset] {
            continue;
        }
        let mut lo = 0usize;
        let mut hi = hdr.count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let mut cursor = hdr.read_slot_offset(mid);
            let mid_key = hdr.decode_key_at(&mut cursor)?;
            if compare_index_entry_to_lookup_key(&mid_key, target, exact)
                == std::cmp::Ordering::Less
            {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let mut saw_greater = false;
        let mut i = lo;
        while i < hdr.count {
            let mut cursor = hdr.read_slot_offset(i);
            let entry_key = hdr.decode_key_at(&mut cursor)?;
            match compare_index_entry_to_lookup_key(&entry_key, target, exact) {
                std::cmp::Ordering::Less => {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: slot array out of order",
                    ));
                }
                std::cmp::Ordering::Equal => {
                    if all_visible {
                        matched[base + key_offset] = true;
                        break;
                    }
                    let row_id =
                        decode_row_id_v7_from(hdr.payload, &mut cursor, hdr.page_table_id)?;
                    if accept(&entry_key, row_id)? {
                        matched[base + key_offset] = true;
                        break;
                    }
                }
                std::cmp::Ordering::Greater => {
                    saw_greater = true;
                    break;
                }
            }
            i += 1;
        }
        if !matched[base + key_offset] && !saw_greater && hdr.next_leaf.is_some() {
            continue_key_offsets.push(key_offset);
        }
    }
    Ok(KeysFirstMatchLeafResult {
        next_leaf: hdr.next_leaf,
        continue_key_offsets,
    })
}

fn validate_index_page_header(buf: &[u8]) -> Result<(), QueryError> {
    validate_index_page_structure(buf)?;
    let stored_checksum = read_u32(buf, 16);
    let actual_checksum = page_checksum(buf, 16);
    if stored_checksum != actual_checksum {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index page checksum",
        ));
    }
    Ok(())
}

fn validate_index_page_structure(buf: &[u8]) -> Result<(), QueryError> {
    if buf.len() != PAGE_SIZE {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: short page",
        ));
    }
    if read_u32(buf, 0) != INDEX_PAGE_MAGIC {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: invalid page magic",
        ));
    }
    if read_u16(buf, 4) != INDEX_PAGE_VERSION {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "unsupported index page version",
        ));
    }
    let payload_len = read_u32(buf, 20) as usize;
    if payload_len == 0 || INDEX_HEADER_BYTES + payload_len > PAGE_SIZE {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index page length",
        ));
    }
    let payload = &buf[INDEX_HEADER_BYTES..INDEX_HEADER_BYTES + payload_len];
    match payload[0] {
        0 | 3 => {}
        1 => {
            let header = parse_leaf_payload_header(buf)?;
            validate_index_slot_bounds(
                header.payload,
                header.count,
                header.slot_array_start,
                LEAF_OFF_ENTRIES,
            )?;
        }
        2 => {
            if payload_len < INTERNAL_HEADER_BYTES - INDEX_HEADER_BYTES {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt index: internal payload too short",
                ));
            }
            let key_layout = payload[INTERNAL_OFF_KEY_LAYOUT];
            let key_count = u32::from_le_bytes(
                payload[INTERNAL_OFF_KEY_COUNT..INTERNAL_OFF_KEY_COUNT + 4]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let mut entries_start = INTERNAL_OFF_KEY_COUNT + 4;
            if key_layout == KEY_LAYOUT_COMPACT_INT2 {
                let _ = decode_int2_prefix(payload, &mut entries_start)?;
            }
            let slot_bytes = key_count.checked_mul(SLOT_OFFSET_BYTES).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt index: internal slot count overflow",
                )
            })?;
            let slot_array_start = payload_len.checked_sub(slot_bytes).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt index: internal slot array underflow",
                )
            })?;
            validate_index_slot_bounds(payload, key_count, slot_array_start, entries_start)?;
        }
        _ => {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: unknown page type",
            ))
        }
    }
    Ok(())
}

fn validate_index_slot_bounds(
    payload: &[u8],
    count: usize,
    slot_array_start: usize,
    records_start: usize,
) -> Result<(), QueryError> {
    let slot_bytes = count.checked_mul(SLOT_OFFSET_BYTES).ok_or_else(|| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: slot count overflow",
        )
    })?;
    if slot_array_start < records_start
        || slot_array_start
            .checked_add(slot_bytes)
            .is_none_or(|end| end > payload.len())
    {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: slot array bounds",
        ));
    }
    for idx in 0..count {
        let slot_off = slot_array_start + idx * SLOT_OFFSET_BYTES;
        let record_off =
            u16::from_le_bytes(payload[slot_off..slot_off + 2].try_into().unwrap()) as usize;
        let record_len =
            u16::from_le_bytes(payload[slot_off + 2..slot_off + 4].try_into().unwrap()) as usize;
        let record_end = record_off.checked_add(record_len).ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: slot record overflow",
            )
        })?;
        if record_len == 0 || record_off < records_start || record_end > slot_array_start {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: slot record bounds",
            ));
        }
    }
    Ok(())
}

fn decode_index_tree_page(buf: &[u8]) -> Result<IndexPage, QueryError> {
    validate_index_tree_page_checksum(buf)?;
    decode_validated_index_tree_page(buf)
}

/// Validate the fixed header and full-page CRC required by the materializing
/// decoder. Unlike byte-view validation, decoding itself walks every record,
/// so a separate structural pre-walk would duplicate the same work.
fn validate_index_tree_page_checksum(buf: &[u8]) -> Result<(), QueryError> {
    if buf.len() != PAGE_SIZE {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: short page",
        ));
    }
    if read_u32(buf, 0) != INDEX_PAGE_MAGIC {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: invalid page magic",
        ));
    }
    if read_u16(buf, 4) != INDEX_PAGE_VERSION {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "unsupported index page version",
        ));
    }
    let stored_checksum = read_u32(buf, 16);
    let actual_checksum = page_checksum(buf, 16);
    if stored_checksum != actual_checksum {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index page checksum",
        ));
    }
    let payload_len = read_u32(buf, 20) as usize;
    if payload_len == 0 || INDEX_HEADER_BYTES + payload_len > PAGE_SIZE {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index page length",
        ));
    }
    Ok(())
}

/// Materialize an index image after its fixed header/checksum has been
/// validated, or after a trusted in-place mutation has preserved its
/// structural invariants and refreshed the checksum. The decoder itself
/// checks record bounds while walking the payload.
fn decode_validated_index_tree_page(buf: &[u8]) -> Result<IndexPage, QueryError> {
    debug_assert_eq!(buf.len(), PAGE_SIZE);
    let payload_len = read_u32(buf, 20) as usize;
    debug_assert!(payload_len > 0 && INDEX_HEADER_BYTES + payload_len <= PAGE_SIZE);
    let buf = &buf[INDEX_HEADER_BYTES..INDEX_HEADER_BYTES + payload_len];
    // v19: page_table_id no longer on disk; pass 0 to existing
    // `decode_*_from(..., page_table_id)` signatures.
    let page_table_id: u64 = 0;
    let mut offset = 0;
    let page_type = buf[offset];
    offset += 1;
    match page_type {
        1 => {
            // v20 leaf payload header: 1B key_layout + 8B base_time
            // + 8B next_leaf + 2B count (u16) + 8B page_max_trx_id.
            let key_layout = read_u8_checked(buf, &mut offset)?;
            let base_time = read_i64_checked(buf, &mut offset)?;
            let next_leaf = read_u64_checked(buf, &mut offset)?;
            let count = read_u16_checked(buf, &mut offset)? as usize;
            let page_max_trx_id = read_u64_checked(buf, &mut offset)?;
            debug_assert_eq!(offset, LEAF_OFF_ENTRIES);
            // v14: INT2 prefix block.
            let layout_aux = if key_layout == KEY_LAYOUT_COMPACT_INT2 {
                let (col2_base, width1, width2) = decode_int2_prefix(buf, &mut offset)?;
                Some(LayoutAux {
                    col2_base,
                    width1,
                    width2,
                })
            } else {
                None
            };
            if !matches!(
                key_layout,
                KEY_LAYOUT_GENERIC
                    | KEY_LAYOUT_COMPACT_TIME_U32
                    | KEY_LAYOUT_COMPACT_DATE_U16
                    | KEY_LAYOUT_COMPACT_DECIMAL_I32
                    | KEY_LAYOUT_COMPACT_DECIMAL_I64
                    | KEY_LAYOUT_COMPACT_INT_U16
                    | KEY_LAYOUT_COMPACT_INT_U32
                    | KEY_LAYOUT_COMPACT_UINT_U16
                    | KEY_LAYOUT_COMPACT_UINT_U32
                    | KEY_LAYOUT_COMPACT_INT2
            ) {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    &format!("corrupt index: unknown leaf key_layout {key_layout}"),
                ));
            }
            let slot_bytes = count.checked_mul(SLOT_OFFSET_BYTES).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt index: leaf slot overflow",
                )
            })?;
            let slot_array_start = buf.len().checked_sub(slot_bytes).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt index: leaf slot underflow",
                )
            })?;
            if slot_array_start < offset {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt index: leaf slot array overlaps header",
                ));
            }
            let header = LeafPayloadHeader {
                payload: buf,
                page_table_id,
                key_layout,
                base_time,
                next_leaf: (next_leaf != 0).then_some(next_leaf),
                count,
                slot_array_start,
                layout_aux,
                page_max_trx_id,
            };
            let mut entries = Vec::with_capacity(count);
            let mut live_record_bytes = 0usize;
            for slot in 0..count {
                let raw_slot = header.read_slot(slot);
                let mut cursor = raw_slot.offset as usize;
                let record_end = cursor.checked_add(raw_slot.len as usize).ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: slot overflow",
                    )
                })?;
                if raw_slot.len == 0 || record_end > slot_array_start {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: slot record bounds",
                    ));
                }
                live_record_bytes = live_record_bytes
                    .checked_add(raw_slot.len as usize)
                    .ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "corrupt index: leaf live size overflow",
                        )
                    })?;
                let key = header.decode_key_at(&mut cursor)?;
                let row_id = decode_row_id_v7_from(buf, &mut cursor, page_table_id)?;
                if cursor != record_end {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: slot length mismatch",
                    ));
                }
                entries.push(IndexEntry { key, row_id });
            }
            let cached_size = LEAF_HEADER_BYTES
                + usize::from(key_layout == KEY_LAYOUT_COMPACT_INT2) * COMPACT_INT2_PREFIX_BYTES
                + live_record_bytes
                + count * SLOT_OFFSET_BYTES;
            let leaf = IndexLeafPage {
                entries,
                next_leaf: (next_leaf != 0).then_some(next_leaf),
                key_layout,
                base_time,
                layout_aux,
                cached_size,
                page_max_trx_id,
            };
            Ok(IndexPage::Leaf(leaf))
        }
        2 => {
            // v13 internal header: 1B key_layout + 8B base_time
            // immediately after page_type, then the v8 layout.
            let key_layout = read_u8_checked(buf, &mut offset)?;
            let base_time = read_i64_checked(buf, &mut offset)?;
            let first_child = read_u64_checked(buf, &mut offset)?;
            let key_count = read_u32_checked(buf, &mut offset)? as usize;
            let mut children = Vec::with_capacity(key_count + 1);
            children.push(first_child);
            // v14: INT2 prefix block on internal pages too.
            let layout_aux = if key_layout == KEY_LAYOUT_COMPACT_INT2 {
                let (col2_base, width1, width2) = decode_int2_prefix(buf, &mut offset)?;
                Some(LayoutAux {
                    col2_base,
                    width1,
                    width2,
                })
            } else {
                None
            };
            let mut keys = Vec::with_capacity(key_count);
            match key_layout {
                KEY_LAYOUT_GENERIC => {
                    for _ in 0..key_count {
                        keys.push(decode_index_entry_from(buf, &mut offset, page_table_id)?);
                        children.push(read_u64_checked(buf, &mut offset)?);
                    }
                }
                KEY_LAYOUT_COMPACT_TIME_U32 => {
                    for _ in 0..key_count {
                        keys.push(decode_compact_time_entry_from(
                            buf,
                            &mut offset,
                            base_time,
                            page_table_id,
                        )?);
                        children.push(read_u64_checked(buf, &mut offset)?);
                    }
                }
                KEY_LAYOUT_COMPACT_DATE_U16 => {
                    for _ in 0..key_count {
                        keys.push(decode_compact_date_entry_from(
                            buf,
                            &mut offset,
                            base_time,
                            page_table_id,
                        )?);
                        children.push(read_u64_checked(buf, &mut offset)?);
                    }
                }
                KEY_LAYOUT_COMPACT_DECIMAL_I32 => {
                    for _ in 0..key_count {
                        keys.push(decode_compact_decimal_i32_entry_from(
                            buf,
                            &mut offset,
                            base_time,
                            page_table_id,
                        )?);
                        children.push(read_u64_checked(buf, &mut offset)?);
                    }
                }
                KEY_LAYOUT_COMPACT_DECIMAL_I64 => {
                    for _ in 0..key_count {
                        keys.push(decode_compact_decimal_i64_entry_from(
                            buf,
                            &mut offset,
                            base_time,
                            page_table_id,
                        )?);
                        children.push(read_u64_checked(buf, &mut offset)?);
                    }
                }
                KEY_LAYOUT_COMPACT_INT_U16 => {
                    for _ in 0..key_count {
                        keys.push(decode_compact_int_u16_entry_from(
                            buf,
                            &mut offset,
                            base_time,
                            page_table_id,
                        )?);
                        children.push(read_u64_checked(buf, &mut offset)?);
                    }
                }
                KEY_LAYOUT_COMPACT_INT_U32 => {
                    for _ in 0..key_count {
                        keys.push(decode_compact_int_u32_entry_from(
                            buf,
                            &mut offset,
                            base_time,
                            page_table_id,
                        )?);
                        children.push(read_u64_checked(buf, &mut offset)?);
                    }
                }
                KEY_LAYOUT_COMPACT_UINT_U16 => {
                    for _ in 0..key_count {
                        keys.push(decode_compact_uint_u16_entry_from(
                            buf,
                            &mut offset,
                            base_time,
                            page_table_id,
                        )?);
                        children.push(read_u64_checked(buf, &mut offset)?);
                    }
                }
                KEY_LAYOUT_COMPACT_UINT_U32 => {
                    for _ in 0..key_count {
                        keys.push(decode_compact_uint_u32_entry_from(
                            buf,
                            &mut offset,
                            base_time,
                            page_table_id,
                        )?);
                        children.push(read_u64_checked(buf, &mut offset)?);
                    }
                }
                KEY_LAYOUT_COMPACT_INT2 => {
                    let aux = layout_aux.ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "INT2 internal layout missing prefix block at decode time",
                        )
                    })?;
                    for _ in 0..key_count {
                        keys.push(decode_compact_int2_entry_from(
                            buf,
                            &mut offset,
                            base_time,
                            aux.col2_base,
                            aux.width1,
                            aux.width2,
                            page_table_id,
                        )?);
                        children.push(read_u64_checked(buf, &mut offset)?);
                    }
                }
                other => {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        &format!("corrupt index: unknown internal key_layout {other}"),
                    ));
                }
            }
            if children.len() != key_count + 1 {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt index: invalid internal fanout",
                ));
            }
            Ok(IndexPage::Internal(IndexInternalPage {
                keys,
                children,
                key_layout,
                base_time,
                layout_aux,
            }))
        }
        3 => {
            let next_free = read_u64_checked(buf, &mut offset)?;
            Ok(IndexPage::Free(IndexFreePage {
                next_free: (next_free != 0).then_some(next_free),
            }))
        }
        _ => Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: unknown page type",
        )),
    }
}

fn validate_index_freelist(file: &mut File, head: Option<u64>) -> Result<(), QueryError> {
    let page_count = file.metadata().map_err(io_err)?.len() / PAGE_SIZE as u64;
    let mut visited = HashSet::new();
    let mut current = head;
    while let Some(page_id) = current {
        if page_id == INDEX_META_PAGE || page_id >= page_count || !visited.insert(page_id) {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: invalid freelist link",
            ));
        }
        current = match decode_index_tree_page(&read_index_page(file, page_id)?)? {
            IndexPage::Free(page) => page.next_free,
            _ => {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt index: freelist contains a live page",
                ))
            }
        };
    }
    Ok(())
}

fn validate_index_page_tree(file: &mut File, root_page: u64) -> Result<(), QueryError> {
    let page_count = file.metadata().map_err(io_err)?.len() / PAGE_SIZE as u64;
    validate_index_tree(root_page, page_count, &mut |page_id| {
        decode_index_tree_page(&read_index_page(file, page_id)?)
    })
}

fn validate_index_tree(
    root_page: u64,
    page_count: u64,
    read_page: &mut impl FnMut(u64) -> Result<IndexPage, QueryError>,
) -> Result<(), QueryError> {
    let mut visited = HashSet::new();
    let mut leaves_via_tree = HashMap::new();
    let mut stack: Vec<(u64, Option<IndexEntry>, Option<IndexEntry>)> =
        vec![(root_page, None, None)];
    while let Some((page_id, low, high)) = stack.pop() {
        if page_id == INDEX_META_PAGE || page_id >= page_count {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: child page out of range",
            ));
        }
        if !visited.insert(page_id) {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: page cycle detected",
            ));
        }
        match read_page(page_id)? {
            IndexPage::Leaf(leaf) => {
                if leaf.entries.windows(2).any(|pair| pair[0] > pair[1])
                    || leaf
                        .entries
                        .first()
                        .is_some_and(|entry| low.as_ref().is_some_and(|low| entry < low))
                    || leaf
                        .entries
                        .last()
                        .is_some_and(|entry| high.as_ref().is_some_and(|high| entry >= high))
                {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: leaf keys violate parent separator bounds",
                    ));
                }
                leaves_via_tree.insert(page_id, leaf.next_leaf);
            }
            IndexPage::Internal(page) => {
                if page.children.is_empty() {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: empty internal page",
                    ));
                }
                if page.keys.windows(2).any(|pair| pair[0] >= pair[1]) {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: unordered separators",
                    ));
                }
                for (ordinal, child) in page.children.into_iter().enumerate() {
                    let child_low = ordinal
                        .checked_sub(1)
                        .and_then(|index| page.keys.get(index))
                        .cloned()
                        .or_else(|| low.clone());
                    let child_high = page.keys.get(ordinal).cloned().or_else(|| high.clone());
                    stack.push((child, child_low, child_high));
                }
            }
            IndexPage::Free(_) => {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt index: root path hit free page",
                ));
            }
        }
    }

    // Phase 6: cross-check the leaf chain (next_leaf links) against the set
    // of leaves reachable via tree descent. After a crash mid-split, the new
    // sibling leaf may be linked into the chain but not yet inserted into
    // its parent — or vice versa. Either case yields a divergent set here
    // and triggers an index rebuild on the next startup pass.
    let mut leaves_via_chain = HashSet::new();
    let mut current = leftmost_leaf_via_descent(read_page, root_page, page_count)?;
    let mut hops: u64 = 0;
    while let Some(page_id) = current {
        if !leaves_via_chain.insert(page_id) {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: leaf chain contains a cycle",
            ));
        }
        hops = hops.saturating_add(1);
        if hops > page_count.saturating_mul(2) {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: leaf chain length exceeds page count",
            ));
        }
        // Every reachable leaf was fully decoded above. Reuse its validated
        // link instead of reading/checksumming/allocating all keys twice.
        current = match leaves_via_tree.get(&page_id) {
            Some(next) => *next,
            None => match read_page(page_id)? {
                IndexPage::Leaf(leaf) => leaf.next_leaf,
                _ => {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: leaf chain reached non-leaf page",
                    ))
                }
            },
        };
        if let Some(next) = current {
            if next >= page_count || next == INDEX_META_PAGE {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt index: next_leaf points outside file",
                ));
            }
        }
    }
    if leaves_via_tree.len() != leaves_via_chain.len()
        || leaves_via_chain
            .iter()
            .any(|id| !leaves_via_tree.contains_key(id))
    {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: leaf chain and tree descent disagree on leaf set",
        ));
    }
    Ok(())
}

/// Walk down the leftmost spine of the tree to find the first leaf. Used by
/// `validate_index_page_tree` to seed the leaf-chain check without depending
/// on a live `IndexFile` instance.
fn leftmost_leaf_via_descent(
    read_page: &mut impl FnMut(u64) -> Result<IndexPage, QueryError>,
    root_page: u64,
    page_count: u64,
) -> Result<Option<u64>, QueryError> {
    let mut page_id = root_page;
    let mut hops: u64 = 0;
    loop {
        if page_id == INDEX_META_PAGE || page_id >= page_count {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: descent hit out-of-range page",
            ));
        }
        hops = hops.saturating_add(1);
        if hops > page_count {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: descent depth exceeds page count",
            ));
        }
        match read_page(page_id)? {
            IndexPage::Leaf(_) => return Ok(Some(page_id)),
            IndexPage::Internal(page) => {
                page_id = *page.children.first().ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: internal page has no child",
                    )
                })?;
            }
            IndexPage::Free(_) => {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt index: descent hit free page",
                ));
            }
        }
    }
}

fn encode_index_entry_into_page(
    entry: &IndexEntry,
    buf: &mut [u8],
    offset: &mut usize,
) -> Result<(), QueryError> {
    // v10: u8 part_count + tagged parts + (page_id varint, slot u16,
    // gen u16). The caller has already written `page_table_id` once
    // at the start of the page.
    encode_index_key_into_leaf_page(&entry.key, buf, offset)?;
    encode_row_id_v7_into_page(entry.row_id, buf, offset)
}

fn decode_index_entry_from(
    buf: &[u8],
    offset: &mut usize,
    page_table_id: u64,
) -> Result<IndexEntry, QueryError> {
    let key = decode_index_key_from_leaf_page(buf, offset)?;
    let row_id = decode_row_id_v7_from(buf, offset, page_table_id)?;
    Ok(IndexEntry { key, row_id })
}

/// Stream-form (non-paginated) entry encoder used by ADD INDEX
/// backfill's external-sort runs. Unlike the paginated form, the
/// stream cannot reference a page-level `table_id` because it is
/// just a flat sequence of length-prefixed records — so the full
/// 6 B RowId is written per entry.
///
/// Stream format is intentionally **independent** of the page-level
/// encoding: it stays on u32 part_count + fixed u32 page_id + u16
/// slot_id (no generation, post rowid-6bytes), so callers (e.g.
/// crash-safe spill cleanup) don't need to know the page-format
/// version.
fn encode_index_entry_for_run_stream(
    entry: &IndexEntry,
    buf: &mut [u8],
    offset: &mut usize,
) -> Result<(), QueryError> {
    encode_index_key_into_page(&entry.key, buf, offset)?;
    write_index_payload_u32(buf, offset, entry.row_id.page_id)?;
    write_index_payload_u16(buf, offset, entry.row_id.slot_id)
}

/// Byte length one entry takes in the run-stream format above.
/// Mirrors `encode_index_entry_for_run_stream` exactly: it sums the
/// u32-part_count key encoding (`encoded_index_key_len`) plus the
/// fixed 6 B RowId (page_id 4 + slot 2). Was 8 B before the
/// rowid-6bytes refactor; was 20 B before per-page `table_id`
/// deduplication.
fn encoded_index_entry_run_stream_len(entry: &IndexEntry) -> Result<usize, QueryError> {
    Ok(encoded_index_key_len(&entry.key)? + 6)
}

fn decode_index_entry_from_run_stream(
    buf: &[u8],
    offset: &mut usize,
) -> Result<IndexEntry, QueryError> {
    let key = decode_index_key_from(buf, offset)?;
    let row_id = RowId {
        page_id: read_u32_checked(buf, offset)?,
        slot_id: read_u16_checked(buf, offset)?,
    };
    Ok(IndexEntry { key, row_id })
}

fn encode_index_key_into(key: &IndexKey, out: &mut Vec<u8>) -> Result<(), QueryError> {
    if key.0.len() > u32::MAX as usize {
        return Err(error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "index key has too many parts",
        ));
    }
    out.extend_from_slice(&(key.0.len() as u32).to_le_bytes());
    for part in &key.0 {
        match part {
            KeyPart::Null => out.push(0),
            KeyPart::Int(v) => {
                out.push(1);
                out.extend_from_slice(&v.to_le_bytes());
            }
            KeyPart::UInt(v) => {
                out.push(2);
                out.extend_from_slice(&v.to_le_bytes());
            }
            KeyPart::Float32(v) => {
                out.push(6);
                out.extend_from_slice(&v.to_le_bytes());
            }
            KeyPart::Float64(v) => {
                out.push(7);
                out.extend_from_slice(&v.to_le_bytes());
            }
            KeyPart::Decimal { value, scale } => {
                out.push(8);
                out.extend_from_slice(&value.to_le_bytes());
                out.push(*scale);
            }
            KeyPart::DecimalWide { value, scale } => {
                let bytes = value.to_signed_bytes_le();
                let len = u8::try_from(bytes.len()).map_err(|_| {
                    error(
                        crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                        "decimal index key is too large",
                    )
                })?;
                out.push(14);
                out.push(len);
                out.extend_from_slice(&bytes);
                out.push(*scale);
            }
            KeyPart::Date(v) => {
                out.push(9);
                out.extend_from_slice(&v.to_le_bytes());
            }
            KeyPart::Time(v) => {
                out.push(10);
                out.extend_from_slice(&v.to_le_bytes());
            }
            KeyPart::DateTime(v) => {
                out.push(11);
                out.extend_from_slice(&v.to_le_bytes());
            }
            KeyPart::Year(v) => {
                out.push(12);
                out.push(*v);
            }
            KeyPart::Bytes(v) => {
                out.push(4);
                encode_bytes_into(v, out)?;
            }
            KeyPart::String(v) => {
                out.push(5);
                encode_bytes_into(v.as_bytes(), out)?;
            }
            KeyPart::CollatedString {
                collation_id,
                value,
            } => {
                out.push(13);
                out.extend_from_slice(&collation_id.to_le_bytes());
                encode_bytes_into(value.as_bytes(), out)?;
            }
            KeyPart::Param { .. } => {
                panic!("internal error: KeyPart::Param leaked into btree encoding")
            }
        }
    }
    Ok(())
}

/// Page-stream key encoder. Writes a u32 part_count followed by the
/// tagged parts. Kept around because `encode_index_entry_for_run_stream`
/// (ADD INDEX spill runs) writes this exact format — spill files live
/// only inside a single process but their encoder/decoder must agree.
/// **Do not** call this for actual .idx page writes: those go through
/// `encode_index_key_into_leaf_page` (u8 part_count) for the 3 B
/// per-entry saving.
fn encode_index_key_into_page(
    key: &IndexKey,
    buf: &mut [u8],
    offset: &mut usize,
) -> Result<(), QueryError> {
    if key.0.len() > u32::MAX as usize {
        return Err(error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "index key has too many parts",
        ));
    }
    write_index_payload_u32(buf, offset, key.0.len() as u32)?;
    encode_index_key_parts_into_page(&key.0, buf, offset)
}

/// v10 .idx-page key encoder. Writes a u8 part_count (capped at 255
/// — well above MySQL's documented 16-column index limit) followed
/// by the tagged parts. The 3 B saving per entry compounds across
/// every leaf and internal page; see the `INDEX_VERSION 9 → 10`
/// note at the top of this module for rationale and limits.
fn encode_index_key_into_leaf_page(
    key: &IndexKey,
    buf: &mut [u8],
    offset: &mut usize,
) -> Result<(), QueryError> {
    if key.0.len() > u8::MAX as usize {
        return Err(error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "index key has too many parts",
        ));
    }
    write_index_payload_u8(buf, offset, key.0.len() as u8)?;
    encode_index_key_parts_into_page(&key.0, buf, offset)
}

fn encode_index_key_parts_into_page(
    parts: &[KeyPart],
    buf: &mut [u8],
    offset: &mut usize,
) -> Result<(), QueryError> {
    for part in parts {
        match part {
            KeyPart::Null => write_index_payload_u8(buf, offset, 0)?,
            KeyPart::Int(v) => {
                write_index_payload_u8(buf, offset, 1)?;
                write_index_payload_i64(buf, offset, *v)?;
            }
            KeyPart::UInt(v) => {
                write_index_payload_u8(buf, offset, 2)?;
                write_index_payload_u64(buf, offset, *v)?;
            }
            KeyPart::Float32(v) => {
                write_index_payload_u8(buf, offset, 6)?;
                write_index_payload_u32(buf, offset, *v)?;
            }
            KeyPart::Float64(v) => {
                write_index_payload_u8(buf, offset, 7)?;
                write_index_payload_u64(buf, offset, *v)?;
            }
            KeyPart::Decimal { value, scale } => {
                write_index_payload_u8(buf, offset, 8)?;
                write_index_payload_bytes(buf, offset, &value.to_le_bytes())?;
                write_index_payload_u8(buf, offset, *scale)?;
            }
            KeyPart::DecimalWide { value, scale } => {
                let bytes = value.to_signed_bytes_le();
                let len = u8::try_from(bytes.len()).map_err(|_| {
                    error(
                        crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                        "decimal index key is too large",
                    )
                })?;
                write_index_payload_u8(buf, offset, 14)?;
                write_index_payload_u8(buf, offset, len)?;
                write_index_payload_bytes(buf, offset, &bytes)?;
                write_index_payload_u8(buf, offset, *scale)?;
            }
            KeyPart::Date(v) => {
                write_index_payload_u8(buf, offset, 9)?;
                write_index_payload_bytes(buf, offset, &v.to_le_bytes())?;
            }
            KeyPart::Time(v) => {
                write_index_payload_u8(buf, offset, 10)?;
                write_index_payload_i64(buf, offset, *v)?;
            }
            KeyPart::DateTime(v) => {
                write_index_payload_u8(buf, offset, 11)?;
                write_index_payload_i64(buf, offset, *v)?;
            }
            KeyPart::Year(v) => {
                write_index_payload_u8(buf, offset, 12)?;
                write_index_payload_u8(buf, offset, *v)?;
            }
            KeyPart::Bytes(v) => {
                write_index_payload_u8(buf, offset, 4)?;
                encode_bytes_into_page(v, buf, offset)?;
            }
            KeyPart::String(v) => {
                write_index_payload_u8(buf, offset, 5)?;
                encode_bytes_into_page(v.as_bytes(), buf, offset)?;
            }
            KeyPart::CollatedString {
                collation_id,
                value,
            } => {
                write_index_payload_u8(buf, offset, 13)?;
                write_index_payload_u16(buf, offset, *collation_id)?;
                encode_bytes_into_page(value.as_bytes(), buf, offset)?;
            }
            KeyPart::Param { .. } => {
                panic!("internal error: KeyPart::Param leaked into btree page encoding")
            }
        }
    }
    Ok(())
}

fn encoded_index_key_len(key: &IndexKey) -> Result<usize, QueryError> {
    if key.0.len() > u32::MAX as usize {
        return Err(error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "index key has too many parts",
        ));
    }
    let mut len = 4usize;
    for part in &key.0 {
        len = len
            .checked_add(match part {
                KeyPart::Null => 1,
                KeyPart::Int(_) | KeyPart::UInt(_) => 1 + 8,
                KeyPart::Float32(_) => 1 + 4,
                KeyPart::Float64(_) => 1 + 8,
                KeyPart::Decimal { .. } => 1 + 16 + 1,
                KeyPart::DecimalWide { value, .. } => 1 + 1 + value.to_signed_bytes_le().len() + 1,
                KeyPart::Date(_) => 1 + 4,
                KeyPart::Time(_) | KeyPart::DateTime(_) => 1 + 8,
                KeyPart::Year(_) => 1 + 1,
                KeyPart::String(v) => checked_index_bytes_len(v.len())?,
                KeyPart::CollatedString { value, .. } => checked_index_bytes_len(value.len())?
                    .checked_add(2)
                    .ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                            "index key is too large",
                        )
                    })?,
                KeyPart::Bytes(v) => checked_index_bytes_len(v.len())?,
                KeyPart::Param { .. } => {
                    panic!("internal error: KeyPart::Param leaked into btree size calculation")
                }
            })
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                    "index key is too large",
                )
            })?;
    }
    Ok(len)
}

fn checked_index_bytes_len(len: usize) -> Result<usize, QueryError> {
    if len > u32::MAX as usize {
        return Err(error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "index key part is too large",
        ));
    }
    (1usize)
        .checked_add(4)
        .and_then(|prefix| prefix.checked_add(len))
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                "index key is too large",
            )
        })
}

/// Page-stream (u32 part_count) key decoder. Pairs with
/// `encode_index_key_into_page`; used by WAL replay
/// (`recovery::decode_wal_index_key`) and the ADD INDEX spill
/// stream (`decode_index_entry_from_run_stream`).
fn decode_index_key_from(buf: &[u8], offset: &mut usize) -> Result<IndexKey, QueryError> {
    let count = read_u32_checked(buf, offset)? as usize;
    decode_index_key_parts_from(buf, offset, count)
}

/// v10 .idx-page (u8 part_count) key decoder. Pairs with
/// `encode_index_key_into_leaf_page`; used by the page-level
/// decoder (`decode_index_entry_from`) and every byte-level
/// fast-path (see `LeafPayloadHeader::decode_key_at`).
fn decode_index_key_from_leaf_page(buf: &[u8], offset: &mut usize) -> Result<IndexKey, QueryError> {
    let count = read_u8_checked(buf, offset)? as usize;
    decode_index_key_parts_from(buf, offset, count)
}

/// Compare a generic-layout on-page key with `target` without materializing
/// `KeyPart`s. The decoder still validates and advances across every encoded
/// part even after the ordering is known, so corruption detection and the
/// returned RowId cursor match the allocating decoder.
fn compare_generic_index_key_from(
    buf: &[u8],
    offset: &mut usize,
    target: &IndexKey,
) -> Result<std::cmp::Ordering, QueryError> {
    compare_generic_index_key_from_mode(buf, offset, target, false)
}

/// Prefix variant of [`compare_generic_index_key_from`]. A stored key whose
/// first `target.len()` parts equal `target` compares `Equal`, while the cursor
/// still advances across the complete stored key to its RowId. This is the
/// byte-level equivalent of `compare_index_entry_to_lookup_key(..., false)`
/// and avoids rebuilding every composite key in a prefix scan.
fn compare_generic_index_key_prefix_from(
    buf: &[u8],
    offset: &mut usize,
    target: &IndexKey,
) -> Result<std::cmp::Ordering, QueryError> {
    compare_generic_index_key_from_mode(buf, offset, target, true)
}

fn compare_generic_index_key_from_mode(
    buf: &[u8],
    offset: &mut usize,
    target: &IndexKey,
    prefix_match: bool,
) -> Result<std::cmp::Ordering, QueryError> {
    use std::cmp::Ordering;

    let count = read_u8_checked(buf, offset)? as usize;
    let mut ordering = Ordering::Equal;
    for index in 0..count {
        let tag = read_u8_checked(buf, offset)?;
        let stored_rank = encoded_key_part_rank(tag)?;
        let target_part = target.0.get(index);
        let part_order = match tag {
            0 => target_part.map_or(Ordering::Greater, |part| {
                stored_rank.cmp(&crate::storage::schema::key_part_rank(part))
            }),
            1 => {
                let value = read_i64_checked(buf, offset)?;
                match target_part {
                    Some(KeyPart::Int(target)) => value.cmp(target),
                    Some(part) => stored_rank.cmp(&crate::storage::schema::key_part_rank(part)),
                    None => Ordering::Greater,
                }
            }
            2 => {
                let value = read_u64_checked(buf, offset)?;
                match target_part {
                    Some(KeyPart::UInt(target)) => value.cmp(target),
                    Some(part) => stored_rank.cmp(&crate::storage::schema::key_part_rank(part)),
                    None => Ordering::Greater,
                }
            }
            4 => {
                let value = read_index_bytes_ref(buf, offset)?;
                match target_part {
                    Some(KeyPart::Bytes(target)) => value.cmp(target.as_slice()),
                    Some(part) => stored_rank.cmp(&crate::storage::schema::key_part_rank(part)),
                    None => Ordering::Greater,
                }
            }
            5 => {
                let bytes = read_index_bytes_ref(buf, offset)?;
                let value = std::str::from_utf8(bytes).map_err(|err| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        format!("corrupt index string key: {err}"),
                    )
                })?;
                match target_part {
                    Some(KeyPart::String(target)) => value.cmp(target.as_str()),
                    Some(part) => stored_rank.cmp(&crate::storage::schema::key_part_rank(part)),
                    None => Ordering::Greater,
                }
            }
            6 => {
                let value = read_u32_checked(buf, offset)?;
                match target_part {
                    Some(KeyPart::Float32(target)) => value.cmp(target),
                    Some(part) => stored_rank.cmp(&crate::storage::schema::key_part_rank(part)),
                    None => Ordering::Greater,
                }
            }
            7 => {
                let value = read_u64_checked(buf, offset)?;
                match target_part {
                    Some(KeyPart::Float64(target)) => value.cmp(target),
                    Some(part) => stored_rank.cmp(&crate::storage::schema::key_part_rank(part)),
                    None => Ordering::Greater,
                }
            }
            8 => {
                let value = read_i128_checked(buf, offset)?;
                let scale = read_u8_checked(buf, offset)?;
                match target_part {
                    Some(KeyPart::Decimal {
                        value: target_value,
                        scale: target_scale,
                    }) => (value, scale).cmp(&(*target_value, *target_scale)),
                    Some(part @ KeyPart::DecimalWide { .. }) => {
                        KeyPart::Decimal { value, scale }.cmp(part)
                    }
                    Some(part) => stored_rank.cmp(&crate::storage::schema::key_part_rank(part)),
                    None => Ordering::Greater,
                }
            }
            14 => {
                let value = read_decimal_wide_key(buf, offset)?;
                let scale = read_u8_checked(buf, offset)?;
                match target_part {
                    Some(KeyPart::DecimalWide {
                        value: target_value,
                        scale: target_scale,
                    }) => (value, scale).cmp(&(target_value.clone(), *target_scale)),
                    Some(part @ KeyPart::Decimal { .. }) => {
                        KeyPart::DecimalWide { value, scale }.cmp(part)
                    }
                    Some(part) => stored_rank.cmp(&crate::storage::schema::key_part_rank(part)),
                    None => Ordering::Greater,
                }
            }
            9 => {
                let value = read_i32_checked(buf, offset)?;
                match target_part {
                    Some(KeyPart::Date(target)) => value.cmp(target),
                    Some(part) => stored_rank.cmp(&crate::storage::schema::key_part_rank(part)),
                    None => Ordering::Greater,
                }
            }
            10 => {
                let value = read_i64_checked(buf, offset)?;
                match target_part {
                    Some(KeyPart::Time(target)) => value.cmp(target),
                    Some(part) => stored_rank.cmp(&crate::storage::schema::key_part_rank(part)),
                    None => Ordering::Greater,
                }
            }
            11 => {
                let value = read_i64_checked(buf, offset)?;
                match target_part {
                    Some(KeyPart::DateTime(target)) => value.cmp(target),
                    Some(part) => stored_rank.cmp(&crate::storage::schema::key_part_rank(part)),
                    None => Ordering::Greater,
                }
            }
            12 => {
                let value = read_u8_checked(buf, offset)?;
                match target_part {
                    Some(KeyPart::Year(target)) => value.cmp(target),
                    Some(part) => stored_rank.cmp(&crate::storage::schema::key_part_rank(part)),
                    None => Ordering::Greater,
                }
            }
            13 => {
                let collation_id = read_u16_checked(buf, offset)?;
                let bytes = read_index_bytes_ref(buf, offset)?;
                let value = std::str::from_utf8(bytes).map_err(|err| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        format!("corrupt index string key: {err}"),
                    )
                })?;
                match target_part {
                    Some(KeyPart::CollatedString {
                        collation_id: target_id,
                        value: target_value,
                    }) => match collation_id.cmp(target_id) {
                        Ordering::Equal => {
                            crate::charset::compare_strings_by_id(collation_id, value, target_value)
                        }
                        order => order,
                    },
                    Some(part) => stored_rank.cmp(&crate::storage::schema::key_part_rank(part)),
                    None => Ordering::Greater,
                }
            }
            _ => unreachable!("encoded_key_part_rank rejects unknown tags"),
        };
        // A prefix comparison ignores stored suffix parts for ordering, but
        // the match above still decodes them so `offset` reaches the RowId and
        // malformed encodings are rejected.
        let part_order = if prefix_match && index >= target.0.len() {
            Ordering::Equal
        } else {
            part_order
        };
        if ordering == Ordering::Equal && part_order != Ordering::Equal {
            ordering = part_order;
        }
    }
    if ordering == Ordering::Equal {
        ordering = if prefix_match && count >= target.0.len() {
            Ordering::Equal
        } else {
            count.cmp(&target.0.len())
        };
    }
    Ok(ordering)
}

#[inline]
fn encoded_key_part_rank(tag: u8) -> Result<u8, QueryError> {
    match tag {
        0 => Ok(0),
        1 => Ok(1),
        2 => Ok(2),
        6 => Ok(3),
        7 => Ok(4),
        8 | 14 => Ok(5),
        4 => Ok(6),
        5 => Ok(7),
        13 => Ok(8),
        9 => Ok(9),
        10 => Ok(10),
        11 => Ok(11),
        12 => Ok(12),
        _ => Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: unknown key part",
        )),
    }
}

#[inline]
fn read_index_bytes_ref<'a>(buf: &'a [u8], offset: &mut usize) -> Result<&'a [u8], QueryError> {
    let len = read_u32_checked(buf, offset)? as usize;
    let end = offset.checked_add(len).ok_or_else(|| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: invalid length",
        )
    })?;
    if end > buf.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: short page",
        ));
    }
    let bytes = &buf[*offset..end];
    *offset = end;
    Ok(bytes)
}

fn read_decimal_wide_key(buf: &[u8], offset: &mut usize) -> Result<num_bigint::BigInt, QueryError> {
    let len = read_u8_checked(buf, offset)? as usize;
    if len > 32 || offset.saturating_add(len) > buf.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt wide decimal index key",
        ));
    }
    let end = *offset + len;
    let value = num_bigint::BigInt::from_signed_bytes_le(&buf[*offset..end]);
    *offset = end;
    Ok(value)
}

#[inline]
fn skip_generic_index_key_part(buf: &[u8], offset: &mut usize, tag: u8) -> Result<(), QueryError> {
    match tag {
        0 => {}
        1 | 2 | 7 | 10 | 11 => {
            let _ = read_u64_checked(buf, offset)?;
        }
        4 | 5 => {
            let _ = read_index_bytes_ref(buf, offset)?;
        }
        6 | 9 => {
            let _ = read_u32_checked(buf, offset)?;
        }
        8 => {
            let _ = read_i128_checked(buf, offset)?;
            let _ = read_u8_checked(buf, offset)?;
        }
        12 => {
            let _ = read_u8_checked(buf, offset)?;
        }
        13 => {
            let _ = read_u16_checked(buf, offset)?;
            let _ = read_index_bytes_ref(buf, offset)?;
        }
        14 => {
            let len = read_u8_checked(buf, offset)? as usize;
            let end = (*offset).checked_add(len).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt wide decimal index key length",
                )
            })?;
            if end > buf.len() {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt wide decimal index key",
                ));
            }
            *offset = end;
            let _ = read_u8_checked(buf, offset)?;
        }
        _ => {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: unknown key part",
            ));
        }
    }
    Ok(())
}

fn decode_index_key_parts_from(
    buf: &[u8],
    offset: &mut usize,
    count: usize,
) -> Result<IndexKey, QueryError> {
    let mut parts = smallvec::SmallVec::<[KeyPart; 4]>::with_capacity(count);
    for _ in 0..count {
        let tag = read_u8_checked(buf, offset)?;
        let part = match tag {
            0 => KeyPart::Null,
            1 => KeyPart::Int(read_i64_checked(buf, offset)?),
            2 => KeyPart::UInt(read_u64_checked(buf, offset)?),
            // Tag 3 was the legacy ASCII-string Float encoding (v5). v6
            // emits Float32 / Float64 (tags 6/7) instead and rejects old
            // .idx files at the meta-page version check, so a tag 3 here
            // is corruption.
            4 => KeyPart::Bytes(read_bytes_from(buf, offset)?),
            5 => KeyPart::String(String::from_utf8(read_bytes_from(buf, offset)?).map_err(
                |err| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        format!("corrupt index string key: {err}"),
                    )
                },
            )?),
            6 => KeyPart::Float32(read_u32_checked(buf, offset)?),
            7 => KeyPart::Float64(read_u64_checked(buf, offset)?),
            8 => {
                let value = read_i128_checked(buf, offset)?;
                let scale = read_u8_checked(buf, offset)?;
                KeyPart::Decimal { value, scale }
            }
            14 => {
                let value = read_decimal_wide_key(buf, offset)?;
                let scale = read_u8_checked(buf, offset)?;
                KeyPart::DecimalWide { value, scale }
            }
            9 => KeyPart::Date(read_i32_checked(buf, offset)?),
            10 => KeyPart::Time(read_i64_checked(buf, offset)?),
            11 => KeyPart::DateTime(read_i64_checked(buf, offset)?),
            12 => KeyPart::Year(read_u8_checked(buf, offset)?),
            13 => {
                let collation_id = read_u16_checked(buf, offset)?;
                let value = String::from_utf8(read_bytes_from(buf, offset)?).map_err(|err| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        format!("corrupt index string key: {err}"),
                    )
                })?;
                KeyPart::CollatedString {
                    collation_id,
                    value,
                }
            }
            _ => {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt index: unknown key part",
                ))
            }
        };
        parts.push(part);
    }
    Ok(IndexKey::from(parts))
}

/// Post rowid-6bytes RowId encoding (3–11 bytes per entry, down
/// from 5–13 B). The page-level `table_id` is written once in the
/// page header; per-entry encoding stores `(page_id u32, slot_id u16)`
/// for `RID_KIND_FULL`. Fixed 6-byte layout (was 8 B in v8 with the
/// trailing 2-byte `generation`). The earlier development build
/// experimented with a LEB128 varint for `page_id` to squeeze small
/// databases further; benchmarks showed the per-lookup varint decode
/// was a measurable ~5 % regression on cache-resident point selects,
/// so the encoding stayed fixed `u32` + `u16`. `RID_KIND_SAME_PAGE`
/// (used when an entry's `page_id` matches the previous entry's, a
/// common pattern under bulk-load or under index-on-monotonic-key
/// inserts) still encodes only `slot_id u16` (2 bytes), so high
/// locality entries still benefit from the per-page run-length
/// compression.
const ROW_ID_V7_ENCODED_LEN: usize = 6;

#[inline]
fn encode_row_id_v7_into_page(
    row_id: RowId,
    buf: &mut [u8],
    offset: &mut usize,
) -> Result<(), QueryError> {
    write_index_payload_u32(buf, offset, row_id.page_id)?;
    write_index_payload_u16(buf, offset, row_id.slot_id)
}

/// Per-entry size of the `RID_KIND_FULL` RowId encoding. Fixed 6 B —
/// 4 B `page_id` + 2 B `slot_id`. Used by the page-fill estimators
/// (`encoded_index_entry_len`, the compact-layout size helpers, the
/// bulk-load split heuristics) so their bytes-on-page projections
/// match what `encode_row_id_v7_into_page` actually writes.
fn row_id_v7_encoded_len(_row_id: RowId) -> usize {
    ROW_ID_V7_ENCODED_LEN
}

/// On-disk encoding for the v9 compact DATETIME / TIME leaf layout.
/// Lays out a single entry as `[u32 delta][RowId(v7)]`, omitting both
/// the 4 B `part_count` and the 1 B KeyPart tag because the page-level
/// `key_layout` already pins them to (1, DateTime|Time). Returns a
/// fall-back error so the caller can demote the page to the generic
/// layout if delta overflows u32 or the key shape mismatches.
fn encode_compact_time_entry_into_page(
    entry: &IndexEntry,
    base_time: i64,
    buf: &mut [u8],
    offset: &mut usize,
) -> Result<(), QueryError> {
    let delta = compact_time_delta(&entry.key, base_time)?;
    write_index_payload_u32(buf, offset, delta)?;
    encode_row_id_v7_into_page(entry.row_id, buf, offset)
}

/// Read one compact-time entry. The caller has already consumed the
/// per-page header (so it knows `base_time`) and points `offset` at
/// the entry's first byte. `KEY_LAYOUT_COMPACT_TIME_U32` pins the
/// KeyPart variant to `DateTime`.
fn decode_compact_time_entry_from(
    buf: &[u8],
    offset: &mut usize,
    base_time: i64,
    page_table_id: u64,
) -> Result<IndexEntry, QueryError> {
    let delta = read_u32_checked(buf, offset)?;
    let value = base_time.wrapping_add(delta as i64);
    let key = IndexKey::from(vec![KeyPart::DateTime(value)]);
    let row_id = decode_row_id_v7_from(buf, offset, page_table_id)?;
    Ok(IndexEntry { key, row_id })
}

/// On-disk encoding for the v9 compact `DATE` leaf layout. Lays out
/// one entry as `[u16 delta][RowId(v7)]`. The page-level `key_layout`
/// pins the (part_count, KeyPart) shape to (1, Date); `base_time`
/// holds the base day count. Returns an error so the caller can
/// demote to the generic layout if the key shape mismatches.
fn encode_compact_date_entry_into_page(
    entry: &IndexEntry,
    base_time: i64,
    buf: &mut [u8],
    offset: &mut usize,
) -> Result<(), QueryError> {
    let delta = compact_date_delta(&entry.key, base_time)?;
    write_index_payload_u16(buf, offset, delta)?;
    encode_row_id_v7_into_page(entry.row_id, buf, offset)
}

/// Read one compact-`DATE` entry. `KEY_LAYOUT_COMPACT_DATE_U16` pins
/// the KeyPart variant to `Date`.
fn decode_compact_date_entry_from(
    buf: &[u8],
    offset: &mut usize,
    base_time: i64,
    page_table_id: u64,
) -> Result<IndexEntry, QueryError> {
    let delta = read_u16_checked(buf, offset)?;
    let days = base_time.wrapping_add(delta as i64);
    let key = IndexKey::from(vec![KeyPart::Date(days as i32)]);
    let row_id = decode_row_id_v7_from(buf, offset, page_table_id)?;
    Ok(IndexEntry { key, row_id })
}

/// Extract the i32 day component of a single-column `DATE` key, then
/// subtract `base_time` to land in u16 range. Returns an error (so
/// the caller may demote to generic) when the key shape is
/// incompatible or the delta overflows.
fn compact_date_delta(key: &IndexKey, base_time: i64) -> Result<u16, QueryError> {
    if key.0.len() != 1 {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "compact date layout requires a single-column key",
        ));
    }
    let days = match &key.0[0] {
        KeyPart::Date(v) => *v as i64,
        KeyPart::Null => {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "compact date layout cannot encode NULL key",
            ));
        }
        _ => {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "compact date layout incompatible with key part",
            ));
        }
    };
    let delta = (days - base_time) as i128;
    if !(0..=(u16::MAX as i128)).contains(&delta) {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "compact date delta out of range for u16 layout",
        ));
    }
    Ok(delta as u16)
}

/// Extract the i64 time component of a single-column DATETIME key,
/// then subtract `base_time` to land in u32 range. Returns an error
/// (so the caller may demote to generic) when the key shape is
/// incompatible or the delta overflows.
fn compact_time_delta(key: &IndexKey, base_time: i64) -> Result<u32, QueryError> {
    if key.0.len() != 1 {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "compact time layout requires a single-column key",
        ));
    }
    let time_value = match &key.0[0] {
        KeyPart::DateTime(v) => *v,
        KeyPart::Null => {
            // NULL deliberately not packed in compact layout — caller
            // demotes the page before reaching here.
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "compact time layout cannot encode NULL key",
            ));
        }
        _ => {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "compact time layout incompatible with key part",
            ));
        }
    };
    let delta = (time_value.wrapping_sub(base_time)) as i128;
    if !(0..=(u32::MAX as i128)).contains(&delta) {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "compact time delta out of range for u32 layout",
        ));
    }
    Ok(delta as u32)
}

/// Byte length of one compact-DATETIME leaf entry (4 B delta + v10
/// RowId). With the v10 varint page_id the RowId varies per entry,
/// so this is no longer a constant — callers pass the RowId in.
fn compact_time_entry_bytes(row_id: RowId) -> usize {
    4 + row_id_v7_encoded_len(row_id)
}
/// Byte length of one compact-DATE leaf entry (2 B delta + v10 RowId).
fn compact_date_entry_bytes(row_id: RowId) -> usize {
    2 + row_id_v7_encoded_len(row_id)
}
/// Byte length of one compact-DECIMAL(P≤9) leaf entry (4 B i32 magnitude
/// + v10 RowId).
fn compact_decimal_i32_entry_bytes(row_id: RowId) -> usize {
    4 + row_id_v7_encoded_len(row_id)
}
/// Byte length of one compact-DECIMAL(P≤18) leaf entry (8 B i64
/// magnitude + v10 RowId).
fn compact_decimal_i64_entry_bytes(row_id: RowId) -> usize {
    8 + row_id_v7_encoded_len(row_id)
}

/// On-disk encoding for the compact DECIMAL(P≤9) layout. Lays out one
/// entry as `[i32 magnitude LE][RowId(v7)]`, omitting the 1 B
/// part_count + 1 B KeyPart tag + 1 B scale that the page-level
/// `key_layout` (and `base_time = scale`) already pins. Returns an
/// error so the caller can demote to the generic layout if the key
/// shape or magnitude range doesn't fit.
fn encode_compact_decimal_i32_entry_into_page(
    entry: &IndexEntry,
    base_scale: i64,
    buf: &mut [u8],
    offset: &mut usize,
) -> Result<(), QueryError> {
    let mag = compact_decimal_magnitude_i32(&entry.key, base_scale)?;
    write_index_payload_bytes(buf, offset, &mag.to_le_bytes())?;
    encode_row_id_v7_into_page(entry.row_id, buf, offset)
}

fn decode_compact_decimal_i32_entry_from(
    buf: &[u8],
    offset: &mut usize,
    base_scale: i64,
    page_table_id: u64,
) -> Result<IndexEntry, QueryError> {
    if *offset + 4 > buf.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "truncated compact decimal i32 entry",
        ));
    }
    let mut mag_buf = [0u8; 4];
    mag_buf.copy_from_slice(&buf[*offset..*offset + 4]);
    *offset += 4;
    let value = i32::from_le_bytes(mag_buf) as i128;
    let key = IndexKey::from(vec![KeyPart::Decimal {
        value,
        scale: base_scale as u8,
    }]);
    let row_id = decode_row_id_v7_from(buf, offset, page_table_id)?;
    Ok(IndexEntry { key, row_id })
}

fn encode_compact_decimal_i64_entry_into_page(
    entry: &IndexEntry,
    base_scale: i64,
    buf: &mut [u8],
    offset: &mut usize,
) -> Result<(), QueryError> {
    let mag = compact_decimal_magnitude_i64(&entry.key, base_scale)?;
    write_index_payload_bytes(buf, offset, &mag.to_le_bytes())?;
    encode_row_id_v7_into_page(entry.row_id, buf, offset)
}

fn decode_compact_decimal_i64_entry_from(
    buf: &[u8],
    offset: &mut usize,
    base_scale: i64,
    page_table_id: u64,
) -> Result<IndexEntry, QueryError> {
    if *offset + 8 > buf.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "truncated compact decimal i64 entry",
        ));
    }
    let mut mag_buf = [0u8; 8];
    mag_buf.copy_from_slice(&buf[*offset..*offset + 8]);
    *offset += 8;
    let value = i64::from_le_bytes(mag_buf) as i128;
    let key = IndexKey::from(vec![KeyPart::Decimal {
        value,
        scale: base_scale as u8,
    }]);
    let row_id = decode_row_id_v7_from(buf, offset, page_table_id)?;
    Ok(IndexEntry { key, row_id })
}

/// Validate that a single-column DECIMAL key fits in i32 at the page's
/// committed scale. Returns the magnitude or an error (caller demotes
/// to generic on mismatch).
fn compact_decimal_magnitude_i32(key: &IndexKey, base_scale: i64) -> Result<i32, QueryError> {
    let value = compact_decimal_value(key, base_scale)?;
    i32::try_from(value).map_err(|_| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            "compact decimal i32 layout: magnitude out of i32 range",
        )
    })
}

fn compact_decimal_magnitude_i64(key: &IndexKey, base_scale: i64) -> Result<i64, QueryError> {
    let value = compact_decimal_value(key, base_scale)?;
    i64::try_from(value).map_err(|_| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            "compact decimal i64 layout: magnitude out of i64 range",
        )
    })
}

fn compact_decimal_value(key: &IndexKey, base_scale: i64) -> Result<i128, QueryError> {
    if key.0.len() != 1 {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "compact decimal layout requires a single-column key",
        ));
    }
    match &key.0[0] {
        KeyPart::Decimal { value, scale } => {
            if *scale as i64 != base_scale {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "compact decimal layout: scale mismatch with page base",
                ));
            }
            Ok(*value)
        }
        KeyPart::Null => Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "compact decimal layout cannot encode NULL key",
        )),
        _ => Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "compact decimal layout incompatible with key part",
        )),
    }
}
/// Byte length of one generic-layout DATE leaf entry: 1 B u8
/// part_count + 1 B tag + 4 B i32 days + v10 RowId.
fn date_generic_entry_bytes(row_id: RowId) -> usize {
    1 + 1 + 4 + row_id_v7_encoded_len(row_id)
}

/// Byte length of one compact-INT (u16 delta) leaf entry.
fn compact_int_u16_entry_bytes(row_id: RowId) -> usize {
    2 + row_id_v7_encoded_len(row_id)
}

/// Byte length of one compact-INT (u32 delta) leaf entry.
fn compact_int_u32_entry_bytes(row_id: RowId) -> usize {
    4 + row_id_v7_encoded_len(row_id)
}

/// Validate a single-column `KeyPart::Int` key against the page base
/// and return the u16 delta. Caller demotes on error.
fn compact_int_delta_u16(key: &IndexKey, base: i64) -> Result<u16, QueryError> {
    let v = compact_int_value(key)?;
    let delta = (v as i128) - (base as i128);
    if !(0..=(u16::MAX as i128)).contains(&delta) {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "compact int delta out of range for u16 layout",
        ));
    }
    Ok(delta as u16)
}

/// Validate a single-column `KeyPart::Int` key against the page base
/// and return the u32 delta. Caller demotes on error.
fn compact_int_delta_u32(key: &IndexKey, base: i64) -> Result<u32, QueryError> {
    let v = compact_int_value(key)?;
    let delta = (v as i128) - (base as i128);
    if !(0..=(u32::MAX as i128)).contains(&delta) {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "compact int delta out of range for u32 layout",
        ));
    }
    Ok(delta as u32)
}

/// Extract the i64 value of a single-column `KeyPart::Int` key, or
/// return an error so the caller can demote to generic.
fn compact_int_value(key: &IndexKey) -> Result<i64, QueryError> {
    if key.0.len() != 1 {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "compact int layout requires a single-column key",
        ));
    }
    match &key.0[0] {
        KeyPart::Int(v) => Ok(*v),
        KeyPart::Null => Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "compact int layout cannot encode NULL key",
        )),
        _ => Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "compact int layout incompatible with key part",
        )),
    }
}

/// Encode one compact-INT (u16 delta) entry as `[u16 delta LE][RowId(v7)]`.
fn encode_compact_int_u16_entry_into_page(
    entry: &IndexEntry,
    base: i64,
    buf: &mut [u8],
    offset: &mut usize,
) -> Result<(), QueryError> {
    let delta = compact_int_delta_u16(&entry.key, base)?;
    write_index_payload_u16(buf, offset, delta)?;
    encode_row_id_v7_into_page(entry.row_id, buf, offset)
}

fn decode_compact_int_u16_entry_from(
    buf: &[u8],
    offset: &mut usize,
    base: i64,
    page_table_id: u64,
) -> Result<IndexEntry, QueryError> {
    let delta = read_u16_checked(buf, offset)?;
    let value = base.wrapping_add(delta as i64);
    let key = IndexKey::from(vec![KeyPart::Int(value)]);
    let row_id = decode_row_id_v7_from(buf, offset, page_table_id)?;
    Ok(IndexEntry { key, row_id })
}

/// Encode one compact-INT (u32 delta) entry as `[u32 delta LE][RowId(v7)]`.
fn encode_compact_int_u32_entry_into_page(
    entry: &IndexEntry,
    base: i64,
    buf: &mut [u8],
    offset: &mut usize,
) -> Result<(), QueryError> {
    let delta = compact_int_delta_u32(&entry.key, base)?;
    write_index_payload_u32(buf, offset, delta)?;
    encode_row_id_v7_into_page(entry.row_id, buf, offset)
}

fn decode_compact_int_u32_entry_from(
    buf: &[u8],
    offset: &mut usize,
    base: i64,
    page_table_id: u64,
) -> Result<IndexEntry, QueryError> {
    let delta = read_u32_checked(buf, offset)?;
    let value = base.wrapping_add(delta as i64);
    let key = IndexKey::from(vec![KeyPart::Int(value)]);
    let row_id = decode_row_id_v7_from(buf, offset, page_table_id)?;
    Ok(IndexEntry { key, row_id })
}

// =================================================================
// v20 COMPACT_UINT (single-column INT UNSIGNED) layout
//
// Mirrors COMPACT_INT but for `KeyPart::UInt`. `base` is stored in the
// page header's i64 `base_time` slot as the bit pattern of the chunk's
// minimum u64 (`min as i64` / `base as u64` round-trips losslessly), so
// values above `i64::MAX` (BIGINT UNSIGNED) encode correctly. Deltas
// are unsigned `value - base`. The byte layout — `[u16|u32 delta][RowId]`
// — is identical to COMPACT_INT, so the entry-byte helpers are reused.
// =================================================================

/// COMPACT_UINT stores its page base (and `CompactChunkAccum` tracks
/// the page-local min/max) as an *order-preserving* `i64`: flipping the
/// high bit maps the full `u64` range onto `i64` so that `u64` ordering
/// is preserved under `i64` comparison. This keeps `CompactChunkAccum`'s
/// i64/i128 span arithmetic correct for unsigned values — including
/// pages that straddle `i64::MAX`, which then compute a huge span and
/// correctly fall back to generic instead of mis-encoding.
const UINT_ORDER_FLIP: u64 = 1u64 << 63;

#[inline]
fn uint_to_ordered_i64(v: u64) -> i64 {
    (v ^ UINT_ORDER_FLIP) as i64
}

#[inline]
fn ordered_i64_to_uint(b: i64) -> u64 {
    (b as u64) ^ UINT_ORDER_FLIP
}

/// Extract the u64 value of a single-column `KeyPart::UInt` key, or
/// return an error so the caller can demote to generic.
fn compact_uint_value(key: &IndexKey) -> Result<u64, QueryError> {
    if key.0.len() != 1 {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "compact uint layout requires a single-column key",
        ));
    }
    match &key.0[0] {
        KeyPart::UInt(v) => Ok(*v),
        KeyPart::Null => Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "compact uint layout cannot encode NULL key",
        )),
        _ => Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "compact uint layout incompatible with key part",
        )),
    }
}

/// Validate a single-column `KeyPart::UInt` key against the page base
/// (carried as the bit pattern of a u64 in `base`) and return the u16
/// delta. Caller demotes on error.
fn compact_uint_delta_u16(key: &IndexKey, base: i64) -> Result<u16, QueryError> {
    let v = compact_uint_value(key)?;
    let base_u = ordered_i64_to_uint(base);
    let delta = (v as u128).checked_sub(base_u as u128);
    match delta {
        Some(d) if d <= u16::MAX as u128 => Ok(d as u16),
        _ => Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "compact uint delta out of range for u16 layout",
        )),
    }
}

/// Validate a single-column `KeyPart::UInt` key against the page base
/// and return the u32 delta. Caller demotes on error.
fn compact_uint_delta_u32(key: &IndexKey, base: i64) -> Result<u32, QueryError> {
    let v = compact_uint_value(key)?;
    let base_u = ordered_i64_to_uint(base);
    let delta = (v as u128).checked_sub(base_u as u128);
    match delta {
        Some(d) if d <= u32::MAX as u128 => Ok(d as u32),
        _ => Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "compact uint delta out of range for u32 layout",
        )),
    }
}

/// Byte length of one compact-UINT (u16 delta) leaf entry.
fn compact_uint_u16_entry_bytes(row_id: RowId) -> usize {
    2 + row_id_v7_encoded_len(row_id)
}

/// Byte length of one compact-UINT (u32 delta) leaf entry.
fn compact_uint_u32_entry_bytes(row_id: RowId) -> usize {
    4 + row_id_v7_encoded_len(row_id)
}

/// Encode one compact-UINT (u16 delta) entry as `[u16 delta LE][RowId(v7)]`.
fn encode_compact_uint_u16_entry_into_page(
    entry: &IndexEntry,
    base: i64,
    buf: &mut [u8],
    offset: &mut usize,
) -> Result<(), QueryError> {
    let delta = compact_uint_delta_u16(&entry.key, base)?;
    write_index_payload_u16(buf, offset, delta)?;
    encode_row_id_v7_into_page(entry.row_id, buf, offset)
}

fn decode_compact_uint_u16_entry_from(
    buf: &[u8],
    offset: &mut usize,
    base: i64,
    page_table_id: u64,
) -> Result<IndexEntry, QueryError> {
    let delta = read_u16_checked(buf, offset)?;
    let value = ordered_i64_to_uint(base).wrapping_add(delta as u64);
    let key = IndexKey::from(vec![KeyPart::UInt(value)]);
    let row_id = decode_row_id_v7_from(buf, offset, page_table_id)?;
    Ok(IndexEntry { key, row_id })
}

/// Encode one compact-UINT (u32 delta) entry as `[u32 delta LE][RowId(v7)]`.
fn encode_compact_uint_u32_entry_into_page(
    entry: &IndexEntry,
    base: i64,
    buf: &mut [u8],
    offset: &mut usize,
) -> Result<(), QueryError> {
    let delta = compact_uint_delta_u32(&entry.key, base)?;
    write_index_payload_u32(buf, offset, delta)?;
    encode_row_id_v7_into_page(entry.row_id, buf, offset)
}

fn decode_compact_uint_u32_entry_from(
    buf: &[u8],
    offset: &mut usize,
    base: i64,
    page_table_id: u64,
) -> Result<IndexEntry, QueryError> {
    let delta = read_u32_checked(buf, offset)?;
    let value = ordered_i64_to_uint(base).wrapping_add(delta as u64);
    let key = IndexKey::from(vec![KeyPart::UInt(value)]);
    let row_id = decode_row_id_v7_from(buf, offset, page_table_id)?;
    Ok(IndexEntry { key, row_id })
}

// =================================================================
// v14 COMPACT_INT2 (two-column INT) layout
// =================================================================

/// Pick the narrowest delta width (1, 2, or 4 bytes) covering a
/// page-local span. Returns `None` if the span exceeds u32 — caller
/// demotes to GENERIC.
fn int2_width_for_span(span_i128: i128) -> Option<u8> {
    if span_i128 < 0 {
        return None;
    }
    if span_i128 <= u8::MAX as i128 {
        Some(1)
    } else if span_i128 <= u16::MAX as i128 {
        Some(2)
    } else if span_i128 <= u32::MAX as i128 {
        Some(4)
    } else {
        None
    }
}

/// Extract `(col1, col2)` from a two-column INT key, or `None` if
/// the key shape doesn't match (caller demotes).
fn int2_values(key: &IndexKey) -> Option<(i64, i64)> {
    match key.0.as_slice() {
        [KeyPart::Int(a), KeyPart::Int(b)] => Some((*a, *b)),
        _ => None,
    }
}

/// Byte length of one INT2 entry given the per-page delta widths.
fn compact_int2_entry_bytes(width1: u8, width2: u8, row_id: RowId) -> usize {
    width1 as usize + width2 as usize + row_id_v7_encoded_len(row_id)
}

/// Write a single delta of `width` bytes (1, 2, or 4) LE.
fn write_int2_delta(
    buf: &mut [u8],
    offset: &mut usize,
    delta: u32,
    width: u8,
) -> Result<(), QueryError> {
    match width {
        1 => {
            if delta > u8::MAX as u32 {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "compact int2 delta overflow for u8 lane",
                ));
            }
            write_index_payload_u8(buf, offset, delta as u8)
        }
        2 => {
            if delta > u16::MAX as u32 {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "compact int2 delta overflow for u16 lane",
                ));
            }
            write_index_payload_u16(buf, offset, delta as u16)
        }
        4 => write_index_payload_u32(buf, offset, delta),
        _ => Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "compact int2 delta width must be 1, 2, or 4",
        )),
    }
}

fn read_int2_delta(buf: &[u8], offset: &mut usize, width: u8) -> Result<u32, QueryError> {
    match width {
        1 => read_u8_checked(buf, offset).map(|v| v as u32),
        2 => read_u16_checked(buf, offset).map(|v| v as u32),
        4 => read_u32_checked(buf, offset),
        _ => Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "compact int2 delta width must be 1, 2, or 4",
        )),
    }
}

/// Encode one INT2 entry: `[delta1][delta2][RowId(v7)]`. Both bases
/// are pinned in the page-level prefix block; widths are also in the
/// prefix block. Caller demotes on width mismatch.
fn encode_compact_int2_entry_into_page(
    entry: &IndexEntry,
    col1_base: i64,
    col2_base: i64,
    width1: u8,
    width2: u8,
    buf: &mut [u8],
    offset: &mut usize,
) -> Result<(), QueryError> {
    let Some((v1, v2)) = int2_values(&entry.key) else {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "compact int2 layout incompatible with key part",
        ));
    };
    let d1 = (v1 as i128) - (col1_base as i128);
    let d2 = (v2 as i128) - (col2_base as i128);
    if !(0..=(u32::MAX as i128)).contains(&d1) || !(0..=(u32::MAX as i128)).contains(&d2) {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "compact int2 delta out of u32 range",
        ));
    }
    write_int2_delta(buf, offset, d1 as u32, width1)?;
    write_int2_delta(buf, offset, d2 as u32, width2)?;
    encode_row_id_v7_into_page(entry.row_id, buf, offset)
}

fn decode_compact_int2_entry_from(
    buf: &[u8],
    offset: &mut usize,
    col1_base: i64,
    col2_base: i64,
    width1: u8,
    width2: u8,
    page_table_id: u64,
) -> Result<IndexEntry, QueryError> {
    let d1 = read_int2_delta(buf, offset, width1)?;
    let d2 = read_int2_delta(buf, offset, width2)?;
    let v1 = col1_base.wrapping_add(d1 as i64);
    let v2 = col2_base.wrapping_add(d2 as i64);
    let key = IndexKey::from(vec![KeyPart::Int(v1), KeyPart::Int(v2)]);
    let row_id = decode_row_id_v7_from(buf, offset, page_table_id)?;
    Ok(IndexEntry { key, row_id })
}

/// Validation helper for `layout_admits` / demote check: returns true
/// iff `key` fits the INT2 page's bases + widths.
fn int2_admits(key: &IndexKey, col1_base: i64, col2_base: i64, width1: u8, width2: u8) -> bool {
    let Some((v1, v2)) = int2_values(key) else {
        return false;
    };
    let d1 = (v1 as i128) - (col1_base as i128);
    let d2 = (v2 as i128) - (col2_base as i128);
    let cap1 = match width1 {
        1 => u8::MAX as i128,
        2 => u16::MAX as i128,
        4 => u32::MAX as i128,
        _ => return false,
    };
    let cap2 = match width2 {
        1 => u8::MAX as i128,
        2 => u16::MAX as i128,
        4 => u32::MAX as i128,
        _ => return false,
    };
    (0..=cap1).contains(&d1) && (0..=cap2).contains(&d2)
}

/// Encode the INT2 per-page prefix block (20 B). `base_time` carries
/// `col1_base`; we write `col2_base` + widths here.
fn encode_int2_prefix(
    col2_base: i64,
    width1: u8,
    width2: u8,
    buf: &mut [u8],
    offset: &mut usize,
) -> Result<(), QueryError> {
    // Note: col1_base already written by the standard leaf header as `base_time`.
    write_index_payload_i64(buf, offset, col2_base)?;
    write_index_payload_u8(buf, offset, width1)?;
    write_index_payload_u8(buf, offset, width2)?;
    // 2 B padding so the block is 20 B / 4-aligned.
    write_index_payload_u16(buf, offset, 0)?;
    // Skip an additional 8 B of reserved padding so the total prefix
    // is exactly COMPACT_INT2_PREFIX_BYTES = 20.
    debug_assert_eq!(8 + 1 + 1 + 2, COMPACT_INT2_PREFIX_BYTES - 8);
    write_index_payload_i64(buf, offset, 0)
}

fn decode_int2_prefix(buf: &[u8], offset: &mut usize) -> Result<(i64, u8, u8), QueryError> {
    let col2_base = read_i64_checked(buf, offset)?;
    let width1 = read_u8_checked(buf, offset)?;
    let width2 = read_u8_checked(buf, offset)?;
    let _padding = read_u16_checked(buf, offset)?;
    let _reserved = read_i64_checked(buf, offset)?;
    Ok((col2_base, width1, width2))
}

/// A single-column key shape eligible for a compact leaf layout. The
/// temporal kinds (`DATETIME` / `DATE`) use a page-local `base + delta`
/// scheme; the decimal kinds (`DecimalI32` / `DecimalI64`) store the
/// magnitude as a fixed-width absolute value, with the page's
/// `base_time` slot repurposed to carry `scale`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompactKind {
    DateTime,
    Date,
    /// DECIMAL(P, S) with |value| ≤ i32::MAX. `scale` propagates from
    /// the first observed entry; subsequent entries must agree.
    DecimalI32 {
        scale: u8,
    },
    /// DECIMAL(P, S) with |value| ≤ i64::MAX (and at least one entry
    /// that didn't fit i32).
    DecimalI64 {
        scale: u8,
    },
    /// Single-column `KeyPart::Int`. Stored layout (U16 vs U32) is
    /// picked at chunk-commit time by `CompactChunkAccum::layout()`
    /// based on the page-local span, so `entry_bytes` returns the
    /// worst-case (U32) width to keep page-fit estimation safe.
    Int,
    /// Single-column `KeyPart::UInt` (`INT UNSIGNED`). Unsigned twin of
    /// `Int`; U16/U32 split decided at commit time from the page-local
    /// span. The page-local `value_min`/`value_max` are tracked as the
    /// bit pattern of the u64 (`v as i64`), so the i128 span arithmetic
    /// stays correct as long as the page doesn't straddle `i64::MAX`
    /// (a straddling page computes a huge span and falls back to
    /// generic — safe).
    Uint,
    /// Two-column INT key (`[KeyPart::Int, KeyPart::Int]`). Both
    /// column widths are decided at commit time from per-column
    /// spans; `entry_bytes` returns worst-case 4+4 to keep page-fit
    /// estimation safe.
    Int2,
}

impl CompactKind {
    /// Inspect a key; return `Some((kind, value_as_i64))` when it is a
    /// single-column shape we can pack, `None` otherwise. For DECIMAL
    /// the i64 fit is checked here so the caller can promote/demote
    /// the chunk-level kind via `merge_with`.
    fn of_entry(entry: &IndexEntry) -> Option<(Self, i64)> {
        match entry.key.0.as_slice() {
            [KeyPart::DateTime(v)] => Some((CompactKind::DateTime, *v)),
            [KeyPart::Date(v)] => Some((CompactKind::Date, *v as i64)),
            [KeyPart::Decimal { value, scale }] => {
                let v64 = i64::try_from(*value).ok()?;
                let kind = if i32::try_from(v64).is_ok() {
                    CompactKind::DecimalI32 { scale: *scale }
                } else {
                    CompactKind::DecimalI64 { scale: *scale }
                };
                Some((kind, v64))
            }
            [KeyPart::Int(v)] => Some((CompactKind::Int, *v)),
            // UInt: track the value as an order-preserving i64 (high bit
            // flipped) so the accumulator's i64/i128 span math matches
            // u64 ordering — see `uint_to_ordered_i64`.
            [KeyPart::UInt(v)] => Some((CompactKind::Uint, uint_to_ordered_i64(*v))),
            // Int2 returns `col1` as the i64 value; col2 tracking is
            // handled by the accumulator separately (see
            // `CompactChunkAccum::record`).
            [KeyPart::Int(v1), KeyPart::Int(_v2)] => Some((CompactKind::Int2, *v1)),
            _ => None,
        }
    }

    /// Combine the current chunk-level kind with a new entry's kind.
    /// Returns the merged kind, or `None` to signal "demote to generic"
    /// (incompatible variants, or a scale mismatch).
    fn merge_with(self, other: CompactKind) -> Option<CompactKind> {
        match (self, other) {
            (CompactKind::DateTime, CompactKind::DateTime) => Some(CompactKind::DateTime),
            (CompactKind::Date, CompactKind::Date) => Some(CompactKind::Date),
            // Decimal: promote i32 -> i64 if either side needs i64, but
            // both sides must share the same scale.
            (CompactKind::DecimalI32 { scale: a }, CompactKind::DecimalI32 { scale: b })
                if a == b =>
            {
                Some(CompactKind::DecimalI32 { scale: a })
            }
            (CompactKind::DecimalI32 { scale: a }, CompactKind::DecimalI64 { scale: b })
            | (CompactKind::DecimalI64 { scale: a }, CompactKind::DecimalI32 { scale: b })
            | (CompactKind::DecimalI64 { scale: a }, CompactKind::DecimalI64 { scale: b })
                if a == b =>
            {
                Some(CompactKind::DecimalI64 { scale: a })
            }
            (CompactKind::Int, CompactKind::Int) => Some(CompactKind::Int),
            (CompactKind::Uint, CompactKind::Uint) => Some(CompactKind::Uint),
            (CompactKind::Int2, CompactKind::Int2) => Some(CompactKind::Int2),
            _ => None,
        }
    }

    fn entry_bytes(self, row_id: RowId) -> usize {
        match self {
            CompactKind::DateTime => compact_time_entry_bytes(row_id),
            CompactKind::Date => compact_date_entry_bytes(row_id),
            CompactKind::DecimalI32 { .. } => compact_decimal_i32_entry_bytes(row_id),
            CompactKind::DecimalI64 { .. } => compact_decimal_i64_entry_bytes(row_id),
            // Worst-case (u32 delta) so page-fit estimation never
            // under-counts. The actual chunk-commit step may pick the
            // narrower u16 layout and write 2 B fewer per entry — that
            // just leaves headroom, never overflows the page.
            CompactKind::Int => compact_int_u32_entry_bytes(row_id),
            // Same worst-case (u32 delta) reasoning as `Int`.
            CompactKind::Uint => compact_uint_u32_entry_bytes(row_id),
            // INT2 worst-case: both columns at u32 width (8 B total).
            // Plus the 20 B per-page prefix block is accounted for in
            // `CompactChunkAccum::new` / `size_with`.
            CompactKind::Int2 => compact_int2_entry_bytes(4, 4, row_id),
        }
    }

    /// Largest page-local span that still fits the delta width. For
    /// decimal kinds there is no delta — the absolute value already
    /// fits the i32/i64 lane, so the span check is unconditional.
    fn max_span(self) -> i128 {
        match self {
            CompactKind::DateTime => u32::MAX as i128,
            CompactKind::Date => u16::MAX as i128,
            CompactKind::DecimalI32 { .. } | CompactKind::DecimalI64 { .. } => i128::MAX,
            // Int can spill into u32 delta layout, so the eligibility
            // cap is u32::MAX. The U16/U32 split is decided by
            // `CompactChunkAccum::layout()` based on the actual span.
            CompactKind::Int => u32::MAX as i128,
            // Uint mirrors Int: u32::MAX delta cap, U16/U32 chosen later.
            CompactKind::Uint => u32::MAX as i128,
            // Int2 has *per-column* spans; we use the same column-1
            // span check here for cheap chunk-level filtering and let
            // `CompactChunkAccum::layout()` re-check col2 separately.
            CompactKind::Int2 => u32::MAX as i128,
        }
    }

    /// Fallback layout when `CompactChunkAccum::layout()` doesn't need
    /// to consider span (decimals + temporal kinds). For `Int` /
    /// `Int2` the accumulator picks between widths based on
    /// per-column span — this returns the widest as a safe default.
    fn layout(self) -> u8 {
        match self {
            CompactKind::DateTime => KEY_LAYOUT_COMPACT_TIME_U32,
            CompactKind::Date => KEY_LAYOUT_COMPACT_DATE_U16,
            CompactKind::DecimalI32 { .. } => KEY_LAYOUT_COMPACT_DECIMAL_I32,
            CompactKind::DecimalI64 { .. } => KEY_LAYOUT_COMPACT_DECIMAL_I64,
            CompactKind::Int => KEY_LAYOUT_COMPACT_INT_U32,
            CompactKind::Uint => KEY_LAYOUT_COMPACT_UINT_U32,
            CompactKind::Int2 => KEY_LAYOUT_COMPACT_INT2,
        }
    }

    /// Value committed to the page's `base_time` header slot. Temporal
    /// kinds emit the actual base time; decimal kinds emit `scale`
    /// (low byte) so the reader can rebuild `KeyPart::Decimal { scale }`.
    fn base_value(self, value_min: i64) -> i64 {
        match self {
            CompactKind::DateTime
            | CompactKind::Date
            | CompactKind::Int
            | CompactKind::Uint
            | CompactKind::Int2 => value_min,
            CompactKind::DecimalI32 { scale } | CompactKind::DecimalI64 { scale } => scale as i64,
        }
    }

    /// True when this kind ignores `value_min` / `value_max` (decimal
    /// kinds don't delta-encode, so the chunk-level range tracking is
    /// irrelevant).
    fn uses_value_range(self) -> bool {
        matches!(
            self,
            CompactKind::DateTime
                | CompactKind::Date
                | CompactKind::Int
                | CompactKind::Uint
                | CompactKind::Int2
        )
    }
}

/// Per-chunk accumulator used by `bulk_load_leaf_level` to decide
/// whether the current chunk can be packed in a v9 compact layout
/// (DATETIME → u32 delta, DATE → u16 delta). Walks entries one by
/// one tracking the value range and whether every entry so far is a
/// single-column temporal key of one consistent kind, then reports
/// the prospective page size (in the cheapest viable layout) and
/// finally hands back the layout / base_time to commit with.
struct CompactChunkAccum {
    count: usize,
    generic_bytes: usize,
    /// Running total of the compact layout's per-entry bytes (delta +
    /// v10 RowId + slot offset). Maintained alongside `generic_bytes`
    /// so `size_with` doesn't need to walk the chunk again.
    compact_bytes: usize,
    /// The temporal kind decided by the first entry; once set, an
    /// entry of any other shape clears `compact_eligible`.
    kind: Option<CompactKind>,
    compact_eligible: bool,
    value_min: i64,
    value_max: i64,
    /// Int2: per-column-2 min/max tracking (col1 lives in
    /// value_min/value_max). Initialised to sentinel; only meaningful
    /// when `kind == Some(CompactKind::Int2)`.
    col2_min: i64,
    col2_max: i64,
}

impl CompactChunkAccum {
    fn new() -> Self {
        Self {
            count: 0,
            generic_bytes: LEAF_HEADER_BYTES,
            compact_bytes: LEAF_HEADER_BYTES,
            kind: None,
            compact_eligible: true,
            value_min: i64::MAX,
            value_max: i64::MIN,
            col2_min: i64::MAX,
            col2_max: i64::MIN,
        }
    }

    fn record(&mut self, entry: &IndexEntry) {
        self.count += 1;
        self.generic_bytes += encoded_index_entry_len(entry) + SLOT_OFFSET_BYTES;
        match CompactKind::of_entry(entry) {
            Some((kind, value)) => {
                match self.kind {
                    None => self.kind = Some(kind),
                    Some(existing) => match existing.merge_with(kind) {
                        Some(merged) => {
                            self.kind = Some(merged);
                            // After merging (e.g. i32 -> i64 for
                            // decimal), recompute compact_bytes for
                            // the prior entries — entry width changes
                            // alongside the kind.
                            if merged != existing {
                                let prior = self.count - 1;
                                let prior_bytes_per = existing.entry_bytes(RowId {
                                    page_id: 0,
                                    slot_id: 0,
                                }) + SLOT_OFFSET_BYTES;
                                let merged_bytes_per = merged.entry_bytes(RowId {
                                    page_id: 0,
                                    slot_id: 0,
                                }) + SLOT_OFFSET_BYTES;
                                // Approximate width correction; the
                                // actual per-entry bytes vary by RowId
                                // varint length, but this estimate is
                                // tight enough for the page-fit
                                // decision (worst case: choose
                                // slightly conservative chunk split).
                                self.compact_bytes = self
                                    .compact_bytes
                                    .saturating_sub(prior_bytes_per * prior)
                                    .saturating_add(merged_bytes_per * prior);
                            }
                        }
                        None => {
                            // Incompatible variants (mixed temporal /
                            // decimal kind, or mismatched scale).
                            self.compact_eligible = false;
                        }
                    },
                }
                let effective_kind = self.kind.unwrap_or(kind);
                self.compact_bytes += effective_kind.entry_bytes(entry.row_id) + SLOT_OFFSET_BYTES;
                if effective_kind.uses_value_range() {
                    if value < self.value_min {
                        self.value_min = value;
                    }
                    if value > self.value_max {
                        self.value_max = value;
                    }
                }
                // INT2: also track col2's span. Pull it from the
                // entry's second KeyPart.
                if matches!(effective_kind, CompactKind::Int2) {
                    if let Some((_, v2)) = int2_values(&entry.key) {
                        if v2 < self.col2_min {
                            self.col2_min = v2;
                        }
                        if v2 > self.col2_max {
                            self.col2_max = v2;
                        }
                    }
                    // First INT2 entry: add the 20 B prefix block on
                    // top of the running compact size estimate.
                    if self.count == 1 {
                        self.compact_bytes += COMPACT_INT2_PREFIX_BYTES;
                    }
                }
            }
            None => {
                self.compact_eligible = false;
            }
        }
    }

    /// Cheapest viable page size if we hypothetically appended `entry`
    /// to this chunk. Returns the smaller of the compact and generic
    /// projections, taking eligibility (consistent kind + range that
    /// fits the layout) into account.
    fn size_with(&self, entry: &IndexEntry) -> usize {
        let entry_generic = encoded_index_entry_len(entry) + SLOT_OFFSET_BYTES;
        let generic = self.generic_bytes + entry_generic;
        if !self.compact_eligible {
            return generic;
        }
        let Some((kind, value)) = CompactKind::of_entry(entry) else {
            return generic;
        };
        // Combine kinds — must be merge-compatible.
        let merged_kind = match self.kind {
            None => kind,
            Some(existing) => match existing.merge_with(kind) {
                Some(k) => k,
                None => return generic,
            },
        };
        // Temporal kinds need the delta-within-range check; decimal
        // kinds are unconditional once `merge_with` succeeded.
        if merged_kind.uses_value_range() {
            let new_min = self.value_min.min(value);
            let new_max = self.value_max.max(value);
            let span = (new_max as i128) - (new_min as i128);
            if !(span >= 0 && span <= merged_kind.max_span()) {
                return generic;
            }
            // INT2: also verify col2 span fits u32. Adding a new
            // entry can blow either column.
            if matches!(merged_kind, CompactKind::Int2) {
                if let Some((_, v2)) = int2_values(&entry.key) {
                    let new_col2_min = self.col2_min.min(v2);
                    let new_col2_max = self.col2_max.max(v2);
                    let col2_span = (new_col2_max as i128) - (new_col2_min as i128);
                    if !(col2_span >= 0 && col2_span <= u32::MAX as i128) {
                        return generic;
                    }
                }
            }
        }
        let mut compact =
            self.compact_bytes + merged_kind.entry_bytes(entry.row_id) + SLOT_OFFSET_BYTES;
        // INT2: first entry in this hypothetical chunk has to also
        // budget the 20 B prefix block (the accumulator hasn't seen
        // it yet because `self.count == 0`).
        if matches!(merged_kind, CompactKind::Int2) && self.count == 0 {
            compact += COMPACT_INT2_PREFIX_BYTES;
        }
        compact.min(generic)
    }

    #[cfg(test)]
    /// Backwards-compatible facade returning only the legacy
    /// `(layout_byte, base_time)` pair. Callers that need the INT2
    /// `LayoutAux` use `layout_full` instead.
    fn layout(&self) -> (u8, i64) {
        let (layout_byte, base, _aux) = self.layout_full();
        (layout_byte, base)
    }

    /// Resolve the final layout for this chunk. Returns
    /// `(layout_byte, col1_base_or_scale, layout_aux)`. For INT2 the
    /// `LayoutAux` carries `col2_base` and the per-column widths
    /// picked from the per-column spans.
    fn layout_full(&self) -> (u8, i64, Option<LayoutAux>) {
        let Some(kind) = self.kind else {
            return (KEY_LAYOUT_GENERIC, 0, None);
        };
        if !self.compact_eligible {
            return (KEY_LAYOUT_GENERIC, 0, None);
        }
        if kind.uses_value_range() {
            if self.value_min == i64::MAX {
                return (KEY_LAYOUT_GENERIC, 0, None);
            }
            let span = (self.value_max as i128) - (self.value_min as i128);
            if !(span >= 0 && span <= kind.max_span()) {
                return (KEY_LAYOUT_GENERIC, 0, None);
            }
            match kind {
                CompactKind::Int => {
                    let layout_byte = if span <= u16::MAX as i128 {
                        KEY_LAYOUT_COMPACT_INT_U16
                    } else {
                        KEY_LAYOUT_COMPACT_INT_U32
                    };
                    (layout_byte, self.value_min, None)
                }
                CompactKind::Uint => {
                    let layout_byte = if span <= u16::MAX as i128 {
                        KEY_LAYOUT_COMPACT_UINT_U16
                    } else {
                        KEY_LAYOUT_COMPACT_UINT_U32
                    };
                    (layout_byte, self.value_min, None)
                }
                CompactKind::Int2 => {
                    // Pick the narrowest delta width per column.
                    let Some(width1) = int2_width_for_span(span) else {
                        return (KEY_LAYOUT_GENERIC, 0, None);
                    };
                    if self.col2_min == i64::MAX {
                        return (KEY_LAYOUT_GENERIC, 0, None);
                    }
                    let col2_span = (self.col2_max as i128) - (self.col2_min as i128);
                    let Some(width2) = int2_width_for_span(col2_span) else {
                        return (KEY_LAYOUT_GENERIC, 0, None);
                    };
                    (
                        KEY_LAYOUT_COMPACT_INT2,
                        self.value_min,
                        Some(LayoutAux {
                            col2_base: self.col2_min,
                            width1,
                            width2,
                        }),
                    )
                }
                _ => (kind.layout(), kind.base_value(self.value_min), None),
            }
        } else {
            (kind.layout(), kind.base_value(0), None)
        }
    }
}

// A row ID consists of two checked fixed-width fields. Keep those checks
// visible to the cursor so validated entry bounds eliminate redundant work.
#[inline(always)]
fn decode_row_id_v7_from(
    buf: &[u8],
    offset: &mut usize,
    _page_table_id: u64,
) -> Result<RowId, QueryError> {
    let page_id = read_u32_checked(buf, offset)?;
    let slot_id = read_u16_checked(buf, offset)?;
    Ok(RowId { page_id, slot_id })
}

fn encode_bytes_into(bytes: &[u8], out: &mut Vec<u8>) -> Result<(), QueryError> {
    if bytes.len() > u32::MAX as usize {
        return Err(error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "index key part is too large",
        ));
    }
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn encode_bytes_into_page(
    bytes: &[u8],
    buf: &mut [u8],
    offset: &mut usize,
) -> Result<(), QueryError> {
    if bytes.len() > u32::MAX as usize {
        return Err(error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "index key part is too large",
        ));
    }
    write_index_payload_u32(buf, offset, bytes.len() as u32)?;
    write_index_payload_bytes(buf, offset, bytes)
}

#[inline]
fn write_index_payload_u8(buf: &mut [u8], offset: &mut usize, value: u8) -> Result<(), QueryError> {
    write_index_payload_bytes(buf, offset, &[value])
}

#[inline]
fn write_index_payload_u16(
    buf: &mut [u8],
    offset: &mut usize,
    value: u16,
) -> Result<(), QueryError> {
    write_index_payload_bytes(buf, offset, &value.to_le_bytes())
}

#[inline]
fn write_index_payload_u32(
    buf: &mut [u8],
    offset: &mut usize,
    value: u32,
) -> Result<(), QueryError> {
    write_index_payload_bytes(buf, offset, &value.to_le_bytes())
}

#[inline]
fn write_index_payload_i64(
    buf: &mut [u8],
    offset: &mut usize,
    value: i64,
) -> Result<(), QueryError> {
    write_index_payload_bytes(buf, offset, &value.to_le_bytes())
}

#[inline]
fn write_index_payload_u64(
    buf: &mut [u8],
    offset: &mut usize,
    value: u64,
) -> Result<(), QueryError> {
    write_index_payload_bytes(buf, offset, &value.to_le_bytes())
}

#[inline(always)]
fn write_index_payload_bytes(
    buf: &mut [u8],
    offset: &mut usize,
    bytes: &[u8],
) -> Result<(), QueryError> {
    let end = offset.checked_add(bytes.len()).ok_or_else(|| {
        error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "index page is too large",
        )
    })?;
    if end > PAGE_SIZE {
        return Err(index_payload_overflow(*offset, bytes.len(), end));
    }
    buf[*offset..end].copy_from_slice(bytes);
    *offset = end;
    Ok(())
}

#[cold]
#[inline(never)]
fn index_payload_overflow(offset: usize, length: usize, end: usize) -> QueryError {
    if std::env::var("MYDB_DEBUG_INDEX_OVERFLOW").is_ok() {
        eprintln!(
            "[index-overflow] offset={} len={} end={} page_size={} backtrace:\n{}",
            offset,
            length,
            end,
            PAGE_SIZE,
            std::backtrace::Backtrace::force_capture(),
        );
    }
    error(
        crate::error::catalog::ER_TOO_BIG_ROWSIZE,
        "index page is too large",
    )
}

#[cfg(test)]
/// Unsigned LEB128 encoder used by the v10 `page_id` field. Each
/// byte carries 7 payload bits in the low bits; the high bit (`0x80`)
/// signals "more bytes follow". A u64 takes at most 10 bytes —
/// realistic page_ids fit in 1–5.
fn write_index_payload_varint_u64(
    buf: &mut [u8],
    offset: &mut usize,
    mut value: u64,
) -> Result<(), QueryError> {
    let mut bytes = [0u8; 10];
    let mut len = 0;
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            bytes[len] = byte;
            len += 1;
            break;
        }
        bytes[len] = byte | 0x80;
        len += 1;
    }
    write_index_payload_bytes(buf, offset, &bytes[..len])
}

#[cfg(test)]
/// Mirror of `write_index_payload_varint_u64`: read up to 10 bytes
/// from `buf` at `offset`, advancing past the encoded varint. Rejects
/// over-long encodings (≥10 bytes with continuation bit set).
fn read_varint_u64_checked(buf: &[u8], offset: &mut usize) -> Result<u64, QueryError> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    for _ in 0..10 {
        if *offset >= buf.len() {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: truncated varint",
            ));
        }
        let byte = buf[*offset];
        *offset += 1;
        value |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 64 {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: varint overflow",
            ));
        }
    }
    Err(error(
        crate::error::catalog::ER_GET_ERRNO,
        "corrupt index: varint too long",
    ))
}

#[cfg(test)]
/// Byte length the LEB128 encoder will emit for `value`. Hot path:
/// every size estimator that previously assumed 8 B for page_id
/// now multiplies by this length per entry.
fn varint_u64_len(value: u64) -> usize {
    let mut len = 1usize;
    let mut v = value >> 7;
    while v != 0 {
        len += 1;
        v >>= 7;
    }
    len
}

fn read_bytes_from(buf: &[u8], offset: &mut usize) -> Result<Vec<u8>, QueryError> {
    let len = read_u32_checked(buf, offset)? as usize;
    read_fixed_bytes_from(buf, offset, len)
}

fn read_fixed_bytes_from(
    buf: &[u8],
    offset: &mut usize,
    len: usize,
) -> Result<Vec<u8>, QueryError> {
    let end = offset.checked_add(len).ok_or_else(|| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: invalid length",
        )
    })?;
    if end > buf.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt index: short page",
        ));
    }
    let bytes = buf[*offset..end].to_vec();
    *offset = end;
    Ok(bytes)
}

fn index_key_starts_with(key: &IndexKey, prefix: &IndexKey) -> bool {
    prefix.0.len() <= key.0.len() && key.0.iter().zip(prefix.0.iter()).all(|(a, b)| a == b)
}

fn max_row_id() -> RowId {
    RowId {
        page_id: u32::MAX,
        slot_id: u16::MAX,
    }
}

#[cfg(test)]
#[path = "btree_audit_20260926_tests.rs"]
mod btree_audit_20260926_tests;

#[cfg(test)]
mod bulk_completion_tests {
    use super::*;

    #[test]
    fn direct_bulk_variants_replace_shrink_and_reopen() {
        for durability in [
            DurabilityMode::Memory,
            DurabilityMode::Relaxed,
            DurabilityMode::Strict,
        ] {
            for variant in 0..3 {
                let dir = tempfile::tempdir().unwrap();
                let log = Arc::new(parking_lot::Mutex::new(
                    LogManager::open(&dir.path().join("wal")).unwrap(),
                ));
                let pool = BufferPool::new(
                    256,
                    log.clone(),
                    durability,
                    Duration::from_millis(DEFAULT_LOCK_WAIT_MS),
                );
                let path = dir.path().join("bulk.idx");
                let mut index = IndexFile::open_with_durability(
                    path.clone(),
                    log.clone(),
                    durability,
                    pool.clone(),
                )
                .unwrap();
                for count in [4000, 17, 0, 257] {
                    let entries: Vec<_> = (0..count)
                        .map(|value| {
                            single_int_index_entry(
                                value,
                                RowId {
                                    page_id: (value / 64) as u32,
                                    slot_id: (value % 64) as u16,
                                },
                            )
                        })
                        .collect();
                    let expected: Vec<_> = entries
                        .iter()
                        .map(|entry| (entry.key.clone(), entry.row_id))
                        .collect();
                    // Vec callers guarantee nonempty input; streaming builders
                    // own empty-tree reset. Keep that existing distinction.
                    if variant == 0 && count == 0 {
                        let error = index
                            .bulk_load_sorted_direct_with_lsn(txn_id_for_ddl(), entries)
                            .unwrap_err();
                        assert_eq!(error.message(), "bulk index build produced no root");
                        continue;
                    }
                    match variant {
                        0 => index
                            .bulk_load_sorted_direct_with_lsn(txn_id_for_ddl(), entries)
                            .unwrap(),
                        1 => index
                            .bulk_load_sorted_direct_stream_with_lsn(
                                txn_id_for_ddl(),
                                entries.into_iter().map(Ok),
                            )
                            .unwrap(),
                        _ => index
                            .bulk_load_sorted_single_int_direct_stream_with_lsn(
                                txn_id_for_ddl(),
                                entries
                                    .into_iter()
                                    .enumerate()
                                    .map(|(value, entry)| Ok((value as i64, entry.row_id))),
                            )
                            .unwrap(),
                    }
                    assert_eq!(index.range_entries(None, None, false).unwrap(), expected);
                    if durability != DurabilityMode::Memory {
                        let next_page = index.meta_state.lock().meta.next_page_id;
                        assert_eq!(
                            fs::metadata(&path).unwrap().len(),
                            next_page * PAGE_SIZE as u64
                        );
                        let file_key = index.file_key.clone();
                        drop(index);
                        pool.discard_file(&file_key).unwrap();
                        index = IndexFile::open_with_durability(
                            path.clone(),
                            log.clone(),
                            durability,
                            pool.clone(),
                        )
                        .unwrap();
                        assert_eq!(index.range_entries(None, None, false).unwrap(), expected);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod round2_tree_tests {
    use super::*;

    #[test]
    fn round2_validator_detects_torn_borrow_separator() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index.raw");
        let entry = |value| IndexEntry {
            key: IndexKey::from(vec![KeyPart::Int(value)]),
            row_id: RowId::new(1, 0),
        };
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .unwrap();
        file.write_all(&vec![0; PAGE_SIZE]).unwrap();
        for page in [
            IndexPage::Internal(IndexInternalPage::new_generic(vec![entry(5)], vec![2, 3])),
            IndexPage::Leaf(IndexLeafPage::new(vec![entry(1), entry(2)], Some(3))),
            IndexPage::Leaf(IndexLeafPage::new(vec![entry(4), entry(5)], None)),
        ] {
            file.write_all(&encode_index_tree_page(&page).unwrap())
                .unwrap();
        }
        assert!(validate_index_page_tree(&mut file, 1).is_err());
        file.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
        file.write_all(
            &encode_index_tree_page(&IndexPage::Internal(IndexInternalPage::new_generic(
                vec![entry(4)],
                vec![2, 3],
            )))
            .unwrap(),
        )
        .unwrap();
        validate_index_page_tree(&mut file, 1).unwrap();
    }
}

#[cfg(test)]
mod cursor_repair_merge_tests {
    use super::*;

    /// Feed that returns a scripted repair directory on each change.
    struct ScriptedFeed {
        directory: Vec<(IndexKey, RowId)>,
        changed: bool,
    }

    impl IndexRepairFeed for ScriptedFeed {
        fn active(&self) -> bool {
            true
        }
        fn rollback_epoch(&self) -> u64 {
            0
        }
        fn fetch_if_changed(
            &mut self,
            _low: Option<&IndexBound>,
            _high: Option<&IndexBound>,
        ) -> Result<Option<Vec<(IndexKey, RowId)>>, QueryError> {
            Ok(std::mem::take(&mut self.changed).then(|| self.directory.clone()))
        }
    }

    fn entry(key: i64, row: u32) -> (IndexKey, RowId) {
        (IndexKey::from(vec![KeyPart::Int(key)]), RowId::new(row, 0))
    }

    fn position(key: i64, row: u32) -> IndexEntry {
        let (key, row_id) = entry(key, row);
        IndexEntry { key, row_id }
    }

    fn merger(directory: Vec<(IndexKey, RowId)>) -> CursorRepairs {
        CursorRepairs::new(Box::new(ScriptedFeed {
            directory,
            changed: true,
        }))
    }

    #[test]
    fn retired_keys_merge_in_order_only_beyond_the_returned_position() {
        // Row 2 moved 20 -> 55 before this read; row 9 moved 5 -> 70 after the
        // cursor passed key 5, so its old key was already returned.
        let mut repairs = merger(vec![entry(20, 2), entry(5, 9)]);
        let out = repairs
            .merge(
                vec![entry(30, 3), entry(55, 2), entry(70, 9)],
                Some(&position(10, 1)),
                Some(&position(70, 9)),
                false,
                false,
                None,
                None,
            )
            .unwrap();
        assert_eq!(out, vec![entry(20, 2), entry(30, 3)]);
    }

    #[test]
    fn retired_keys_beyond_the_read_wait_for_the_range_end() {
        let mut repairs = merger(vec![entry(90, 4)]);
        let first = repairs
            .merge(
                vec![entry(10, 1)],
                None,
                Some(&position(10, 1)),
                false,
                false,
                None,
                None,
            )
            .unwrap();
        assert_eq!(first, vec![entry(10, 1)]);
        let last = repairs
            .merge(
                Vec::new(),
                Some(&position(10, 1)),
                None,
                true,
                false,
                None,
                None,
            )
            .unwrap();
        assert_eq!(last, vec![entry(90, 4)]);
    }

    #[test]
    fn every_retired_key_of_a_row_updated_twice_is_kept_for_visibility() {
        // Row 7 moved 40 -> 10 -> 60 during the scan; only visibility knows
        // which old key belongs to this snapshot, so both are returned.
        let mut repairs = merger(vec![entry(10, 7), entry(40, 7)]);
        let out = repairs
            .merge(
                vec![entry(60, 7)],
                None,
                Some(&position(60, 7)),
                true,
                false,
                None,
                None,
            )
            .unwrap();
        assert_eq!(out, vec![entry(10, 7), entry(40, 7)]);
        assert_eq!(repairs.row_ids(out), vec![RowId::new(7, 0)]);
    }

    #[test]
    fn reverse_merge_uses_descending_positions() {
        let mut repairs = merger(vec![entry(50, 5), entry(95, 6)]);
        let out = repairs
            .merge(
                vec![entry(80, 1), entry(40, 2)],
                Some(&position(90, 3)),
                Some(&position(40, 2)),
                false,
                true,
                None,
                None,
            )
            .unwrap();
        assert_eq!(out, vec![entry(80, 1), entry(50, 5), entry(40, 2)]);
    }
}
