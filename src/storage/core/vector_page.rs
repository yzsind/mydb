// Page-based HNSW vector index storage (`.hvec`).
//
// This is the storage substrate for the paged HNSW backend: it lays the
// graph out in 16 KiB buffer-pool pages so a vector index participates in
// the buffer pool (demand-paged, evicted under the `Vector` soft quota,
// supporting larger-than-memory indexes) and the WAL (crash-safe physical
// redo) exactly like the B-tree (`btree.rs`) — which this module mirrors
// closely on purpose.
//
// ## On-disk layout
//
// One file, several page kinds, all carrying the standard 24-byte page
// header so they flow through the shared LSN/checksum helpers
// (`set_page_lsn_if_formatted`, `page_checksum`) and the generic
// `redo_page_update` recovery path with no HNSW-specific recovery code:
//
// ```text
//   off  0  magic        u32   (page-kind specific, registered in bytes.rs)
//   off  4  page_version u16   VEC_PAGE_VERSION
//   off  6  page_kind    u8    0=meta 1=node 2=free
//   off  7  flags        u8
//   off  8  PAGE_LSN     u64   <-- MUST be offset 8 (page_lsn contract)
//   off 16  checksum     u32   (page_checksum treats these 4 bytes as 0)
//   off 20  reserved     u32
//   off 24  ... payload ...
// ```
//
// - **Meta page** (page 0): index parameters + entry point + page
//   allocators/freelist + counters.
// - **Node page**: slotted records containing the exact f32 row, its persisted
//   SQ8 navigation row, and adjacency. A node's vid is
//   `(page_id << 32) | slot`, mirroring [`RowId`]'s addressing; once
//   written a node never moves (run-time vids are never reused — only a
//   compaction rebuild into a fresh file re-densifies them), because the
//   vid is referenced by many inbound edges with no back-pointers. To keep
//   neighbour add/remove an in-place page update, each node **reserves its
//   maximum per-layer neighbour footprint at first write** (cap `m0` at
//   layer 0, `m` above).
// - **Free page**: freelist link.
//
// Mutable pages use the data buffer. Finalized images can also be navigated
// through a revocable read-only mapping of this same file. A cold build writes
// SQ8 rows with the topology, so reopening needs no O(n) navigation rebuild.

/// File-format version (gates the whole `.hvec` file, like `INDEX_VERSION`).
pub(crate) use crate::storage::format_versions::PAGED_HNSW_INDEX_VERSION as VEC_INDEX_VERSION;
/// Page-format version (per-page, like `INDEX_PAGE_VERSION`).
pub(crate) use crate::storage::format_versions::PAGED_HNSW_PAGE_VERSION as VEC_PAGE_VERSION;
/// Meta page id.
const VEC_META_PAGE: u64 = 0;
const VEC_META_USED_BYTES: usize = 144;

// Meta pages have a short fixed prefix followed by zero padding. CRC extension
// by this fixed suffix is affine over GF(2); cache its basis instead of hashing
// the same 16 KiB of zeros for every commit marker and LSN stamp. The result is
// exactly the ordinary full-page CRC, including for backup and WAL validation.
fn vector_meta_extend_crc(prefix_crc: u32) -> u32 {
    static BASIS: OnceLock<(u32, [u32; 32])> = OnceLock::new();
    let (zero, basis) = BASIS.get_or_init(|| {
        let zeros = [0u8; PAGE_SIZE - VEC_META_USED_BYTES];
        let extend = |initial| {
            let mut hash = crc32fast::Hasher::new_with_initial(initial);
            hash.update(&zeros);
            hash.finalize()
        };
        let zero = extend(0);
        (zero, std::array::from_fn(|bit| extend(1 << bit) ^ zero))
    });
    let mut result = *zero;
    let mut bits = prefix_crc;
    while bits != 0 {
        result ^= basis[bits.trailing_zeros() as usize];
        bits &= bits - 1;
    }
    result
}

fn vector_meta_crc(buf: &[u8], zero_checksum: bool) -> u32 {
    debug_assert_eq!(buf.len(), PAGE_SIZE);
    debug_assert!(buf[VEC_META_USED_BYTES..].iter().all(|byte| *byte == 0));
    let mut hash = crc32fast::Hasher::new();
    if zero_checksum {
        hash.update(&buf[..16]);
        hash.update(&[0; 4]);
        hash.update(&buf[20..VEC_META_USED_BYTES]);
    } else {
        hash.update(&buf[..VEC_META_USED_BYTES]);
    }
    vector_meta_extend_crc(hash.finalize())
}

fn stamp_vector_page(buf: &mut [u8], page_id: u64, lsn: u64) {
    if page_id == VEC_META_PAGE {
        set_page_lsn(buf, lsn);
        let checksum = vector_meta_crc(buf, true);
        write_u32(buf, 16, checksum);
    } else {
        set_page_lsn_if_formatted(buf, lsn);
    }
}

// Page magics — mirror the "M.." (0x4d..) convention of the heap/index/
// overflow magics. EVERY magic defined here must also be registered in the
// two `match` arms of `bytes.rs` (`refresh_page_checksum`,
// `set_page_lsn_if_formatted`); otherwise LSN stamping / checksum refresh
// silently no-op for `.hvec` pages and the recovery LSN gate never sticks.
const VEC_META_MAGIC: u32 = 0x4d_56_4d_31; // "MVM1"
const VEC_NODE_MAGIC: u32 = 0x4d_56_4e_31; // "MVN1"

const VEC_PAGE_KIND_META: u8 = 0;
const VEC_PAGE_KIND_NODE: u8 = 1;

/// Standard page header size; payload starts here.
const VEC_HEADER_BYTES: usize = 24;

/// Number of page latches (power of two). A node-page read-modify-write
/// (append a node / push or prune a neighbour / set a tombstone) holds the
/// latch for `page_id & MASK` across the whole read→mutate→write so
/// concurrent writers to the same page never lose an update. Each RMW takes
/// exactly one latch, so there is no lock-ordering / deadlock concern: a
/// bidirectional edge is wired as two independent single-page RMWs, and the
/// transient half-wired window only affects recall (HNSW tolerates it, the
/// same as the in-memory backend's lock-drop-recompute-relock prune).
const VEC_PAGE_LATCH_COUNT: usize = 1024;
const VEC_PAGE_LATCH_MASK: u64 = (VEC_PAGE_LATCH_COUNT as u64) - 1;

/// `entry_vid` sentinel for "no entry point yet".
const VEC_NO_VID: u64 = u64::MAX;

// Node-page payload header (payload-relative; add `VEC_HEADER_BYTES`):
//   [0..2]  slot_count u16  — number of slots ever allocated on this page
//   [2..4]  heap_top  u16   — absolute page offset where the record region
//                            starts (records grow DOWN from PAGE_SIZE; the
//                            slot directory grows UP from the payload start)
//   [4..]   slot directory: slot_count * u16 absolute record offsets
const NODE_OFF_SLOT_COUNT: usize = VEC_HEADER_BYTES;
const NODE_OFF_HEAP_TOP: usize = VEC_HEADER_BYTES + 2;
const NODE_OFF_DIRECTORY: usize = VEC_HEADER_BYTES + 4;

/// A node id: `(page_id << 32) | slot`, isomorphic to [`RowId`] packing.
pub(crate) type VecId = u64;

/// Compact on-disk navigation id. The high 24 bits address the `.hvec` page
/// and the low 8 bits address its slot, so an edge needs four bytes and can be
/// resolved without a hash table or directory cache.
pub(crate) type NodeId = u32;
const NODE_ID_SLOT_BITS: u32 = 8;
const NODE_ID_SLOTS_PER_PAGE: u16 = 1 << NODE_ID_SLOT_BITS;
const NODE_ID_MAX_PAGE: u64 = (1u64 << (u32::BITS - NODE_ID_SLOT_BITS)) - 1;

#[inline]
fn vid_make(page_id: u64, slot: u16) -> VecId {
    (page_id << 32) | u64::from(slot)
}
#[inline]
fn vid_page(vid: VecId) -> u64 {
    vid >> 32
}
#[inline]
fn vid_slot(vid: VecId) -> u16 {
    vid as u16
}

#[inline]
fn node_id_from_vid(vid: VecId) -> Result<NodeId, QueryError> {
    let page = vid_page(vid);
    let slot = vid_slot(vid);
    if page == VEC_META_PAGE || page > NODE_ID_MAX_PAGE || slot >= NODE_ID_SLOTS_PER_PAGE {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "vector index: node id exceeds v3 navigation address space",
        ));
    }
    Ok(((page as u32) << NODE_ID_SLOT_BITS) | u32::from(slot))
}

#[inline]
fn node_id_to_vid(node_id: NodeId) -> VecId {
    vid_make(
        u64::from(node_id >> NODE_ID_SLOT_BITS),
        (node_id & (u32::from(NODE_ID_SLOTS_PER_PAGE) - 1)) as u16,
    )
}

#[inline]
fn vec_pack_row(r: RowId) -> u64 {
    (u64::from(r.page_id) << 32) | u64::from(r.slot_id)
}
#[inline]
fn vec_unpack_row(v: u64) -> RowId {
    RowId {
        page_id: (v >> 32) as u32,
        slot_id: v as u16,
    }
}

/// Per-layer neighbour capacity: `m0` at layer 0, `m` above.
#[inline]
fn layer_cap(level: usize, m: usize, m0: usize) -> usize {
    if level == 0 {
        m0
    } else {
        m
    }
}

/// Fixed on-disk size of a node record with the given `top_layer`, reserving
/// the maximum neighbour footprint so all edge edits are in-place.
///
///   flags(1) + top_layer(1) + row_id(8) + vector(dim*4) + sq8(dim) + norm(4)
///     + sum_{l=0}^{top_layer} ( count(2) + cap(l)*4-byte NodeId )
fn node_record_size(top_layer: usize, dim: usize, m: usize, m0: usize) -> usize {
    // v3 persists the SQ8 navigation row next to the exact vector.  The page
    // remains the only cached object: both code and adjacency are admitted,
    // prefetched and evicted by the shared data buffer.
    let mut size = 1 + 1 + 8 + dim * 4 + dim + 4;
    for l in 0..=top_layer {
        size += 2 + layer_cap(l, m, m0) * 4;
    }
    size
}

/// Byte offset, within a node record, of layer `level`'s `count` field.
/// The `vid` array follows immediately (`count_off + 2`).
fn layer_count_offset(rec_off: usize, level: usize, dim: usize, m: usize, m0: usize) -> usize {
    let mut off = rec_off + 1 + 1 + 8 + dim * 4 + dim + 4;
    for l in 0..level {
        off += 2 + layer_cap(l, m, m0) * 4;
    }
    off
}

#[cfg(test)]
/// Decoded, owned copy of a node record (the "copy out then unpin" view).
#[derive(Debug, Clone)]
pub(crate) struct NodeView {
    #[allow(dead_code)] // Full-node diagnostics; scans use the header-only path.
    pub row_id: RowId,
    /// Top HNSW layer this node participates in (its adjacency has
    /// `top_layer + 1` levels). Carried for completeness / debugging; the
    /// traversal indexes `neighbors` directly.
    #[allow(dead_code)]
    pub top_layer: usize,
    #[allow(dead_code)] // Part of the complete node view; inspected by storage tests.
    pub deleted: bool,
    /// Full f32 vector. Read by `read_node` for tests / diagnostics; the search
    /// re-rank path scores via the leaner `read_vector_meta_into`.
    #[allow(dead_code)]
    pub vector: Vec<f32>,
    /// `neighbors[level]` — active neighbour vids at that level. The hot
    /// traversal paths now read individual layers via
    /// `read_neighbors_into` (no full decode), so this is populated only for
    /// the complete-decode `read_node` used by tests / diagnostics.
    #[allow(dead_code)]
    pub neighbors: Vec<Vec<VecId>>,
}

/// In-memory image of the meta page.
#[derive(Debug, Clone)]
pub(crate) struct VectorMeta {
    pub dim: u32,
    pub metric: u8,
    pub m: u16,
    pub m0: u16,
    pub ef_construction: u16,
    pub entry_vid: VecId,
    pub max_layer: u32,
    pub node_next_page_id: u64,
    pub node_count: u64,
    pub tombstone_count: u64,
    /// Set once a cold (bulk) build has fully completed and durably emitted
    /// every page to the WAL. A non-empty file whose meta is **not** finalized
    /// is an interrupted build (e.g. crash mid-cold-build under deferred-WAL
    /// mode) and is discarded + rebuilt from the heap rather than trusted —
    /// the heap stays the durable record. Files written before this field
    /// existed read back `false` (zeroed byte) and are rebuilt once, which is
    /// safe (a graph is only ever a best-effort candidate generator).
    pub finalized: bool,
    /// Published only after build snapshot catch-up and commit maintenance.
    pub query_ready: bool,
    /// WAL high-water observed when the last complete build was published.
    /// Incremental DML is WAL-protected independently and deliberately does
    /// not clear this marker. Version-1 files are rejected by the file-version
    /// gate and rebuilt once from the heap.
    pub built_lsn: u64,
    /// Committed heap frontier covered by the completed build/deltas.
    pub coverage_scn: u64,
    /// Global affine SQ8 parameters used by persistent navigation rows.
    /// Invalid for DOT and for an empty index that has not seen its first
    /// vector yet.  Nodes outside the calibrated range carry EXACT_NAV and
    /// use their persisted f32 row without forcing a global rebuild.
    pub nav_quant_valid: bool,
    pub nav_mid: f32,
    pub nav_inv_scale: f32,
    pub nav_dist_mul: f32,
    pub nav_exact_nodes: u64,
}

impl VectorMeta {
    fn fresh(dim: u32, metric: u8, m: u16, m0: u16, ef_construction: u16) -> Self {
        Self {
            dim,
            metric,
            m,
            m0,
            ef_construction,
            entry_vid: VEC_NO_VID,
            max_layer: 0,
            // page 0 is meta; node pages start at 1.
            node_next_page_id: 1,
            node_count: 0,
            tombstone_count: 0,
            finalized: false,
            query_ready: false,
            built_lsn: 0,
            coverage_scn: 0,
            nav_quant_valid: false,
            nav_mid: 0.0,
            nav_inv_scale: 1.0,
            nav_dist_mul: 1.0,
            nav_exact_nodes: 0,
        }
    }
}

fn encode_vector_meta(meta: &VectorMeta) -> Vec<u8> {
    let mut buf = vec![0u8; PAGE_SIZE];
    write_u32(&mut buf, 0, VEC_META_MAGIC);
    write_u16(&mut buf, 4, VEC_PAGE_VERSION);
    buf[6] = VEC_PAGE_KIND_META;
    // [8..16] LSN, [16..20] checksum: stamped by set_page_lsn_if_formatted /
    // refreshed below.
    write_u32(&mut buf, 24, VEC_INDEX_VERSION);
    write_u32(&mut buf, 28, PAGE_SIZE as u32);
    write_u32(&mut buf, 32, meta.dim);
    buf[36] = meta.metric;
    write_u16(&mut buf, 40, meta.m);
    write_u16(&mut buf, 42, meta.m0);
    write_u16(&mut buf, 44, meta.ef_construction);
    write_u64(&mut buf, 48, meta.entry_vid);
    write_u32(&mut buf, 56, meta.max_layer);
    write_u64(&mut buf, 64, meta.node_next_page_id);
    // [72..80] reserved (the abandoned freelist prototype never had a writer).
    write_u64(&mut buf, 80, meta.node_count);
    write_u64(&mut buf, 88, meta.tombstone_count);
    buf[96] = u8::from(meta.finalized);
    buf[97] = u8::from(meta.query_ready);
    write_u64(&mut buf, 104, meta.built_lsn);
    buf[112] = u8::from(meta.nav_quant_valid);
    write_u32(&mut buf, 116, meta.nav_mid.to_bits());
    write_u32(&mut buf, 120, meta.nav_inv_scale.to_bits());
    write_u32(&mut buf, 124, meta.nav_dist_mul.to_bits());
    write_u64(&mut buf, 128, meta.nav_exact_nodes);
    write_u64(&mut buf, 136, meta.coverage_scn);
    let checksum = vector_meta_crc(&buf, true);
    write_u32(&mut buf, 16, checksum);
    buf
}

fn decode_vector_meta(buf: &[u8]) -> Result<VectorMeta, QueryError> {
    if buf.len() != PAGE_SIZE {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt vector index meta page length",
        ));
    }
    if read_u32(buf, 0) != VEC_META_MAGIC {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt vector index meta page magic",
        ));
    }
    if read_u16(buf, 4) != VEC_PAGE_VERSION {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "unsupported vector index meta page version",
        ));
    }
    let stored = read_u32(buf, 16);
    let actual = page_checksum(buf, 16);
    if stored != actual {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt vector index meta page checksum",
        ));
    }
    if read_u32(buf, 24) != VEC_INDEX_VERSION || read_u32(buf, 28) != PAGE_SIZE as u32 {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "unsupported vector index file version",
        ));
    }
    if buf[VEC_META_USED_BYTES..].iter().any(|byte| *byte != 0) {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt vector index meta page padding",
        ));
    }
    Ok(VectorMeta {
        dim: read_u32(buf, 32),
        metric: buf[36],
        m: read_u16(buf, 40),
        m0: read_u16(buf, 42),
        ef_construction: read_u16(buf, 44),
        entry_vid: read_u64(buf, 48),
        max_layer: read_u32(buf, 56),
        node_next_page_id: read_u64(buf, 64),
        node_count: read_u64(buf, 80),
        tombstone_count: read_u64(buf, 88),
        finalized: buf[96] != 0,
        query_ready: buf[97] != 0,
        built_lsn: read_u64(buf, 104),
        coverage_scn: read_u64(buf, 136),
        nav_quant_valid: buf[112] != 0,
        nav_mid: f32::from_bits(read_u32(buf, 116)),
        nav_inv_scale: f32::from_bits(read_u32(buf, 120)),
        nav_dist_mul: f32::from_bits(read_u32(buf, 124)),
        nav_exact_nodes: read_u64(buf, 128),
    })
}

/// Format a fresh, empty node page into `buf`.
fn format_empty_node_page(buf: &mut [u8]) {
    for b in buf.iter_mut() {
        *b = 0;
    }
    write_u32(buf, 0, VEC_NODE_MAGIC);
    write_u16(buf, 4, VEC_PAGE_VERSION);
    buf[6] = VEC_PAGE_KIND_NODE;
    write_u16(buf, NODE_OFF_SLOT_COUNT, 0);
    write_u16(buf, NODE_OFF_HEAP_TOP, PAGE_SIZE as u16);
}

/// Free space (bytes) available for a new record + its directory slot.
fn node_page_free_space(buf: &[u8]) -> usize {
    let slot_count = read_u16(buf, NODE_OFF_SLOT_COUNT) as usize;
    let heap_top = read_u16(buf, NODE_OFF_HEAP_TOP) as usize;
    let dir_end = NODE_OFF_DIRECTORY + slot_count * 2;
    heap_top.saturating_sub(dir_end)
}

/// Flags byte offsets within a record.
const REC_FLAG_INITIALIZED: u8 = 0b0000_0001;
const REC_FLAG_TOMBSTONE: u8 = 0b0000_0010;
const REC_FLAG_EXACT_NAV: u8 = 0b0000_0100;

#[derive(Clone, Copy, Debug)]
struct PersistedNavQuant {
    valid: bool,
    metric: u8,
    mid: f32,
    inv_scale: f32,
}

#[inline]
fn navigation_inv_norm(vector: &[f32], metric: u8) -> f64 {
    if metric != 1 {
        return 1.0;
    }
    let norm = crate::engine::vector::distance::norm(vector);
    if norm > 0.0 {
        norm.recip()
    } else {
        1.0
    }
}

#[inline]
fn persisted_nav_is_exact(vector: &[f32], quant: PersistedNavQuant) -> bool {
    if !quant.valid {
        return true;
    }
    let inv_norm = navigation_inv_norm(vector, quant.metric);
    !vector.iter().all(|x| {
        let value = (*x as f64 * inv_norm) as f32;
        value.is_finite() && ((value - quant.mid) * quant.inv_scale).abs() <= 127.5
    })
}

fn write_persisted_nav_row(
    buf: &mut [u8],
    rec_off: usize,
    dim: usize,
    vector: &[f32],
    quant: PersistedNavQuant,
) {
    let code_off = rec_off + 10 + dim * 4;
    let norm_off = code_off + dim;
    let inv_norm = navigation_inv_norm(vector, quant.metric);
    if persisted_nav_is_exact(vector, quant) {
        buf[rec_off] |= REC_FLAG_EXACT_NAV;
        buf[code_off..code_off + dim].fill(0);
        write_u32(buf, norm_off, 0);
        return;
    }
    buf[rec_off] &= !REC_FLAG_EXACT_NAV;
    let mut norm = 0i32;
    for (dst, &value) in buf[code_off..code_off + dim].iter_mut().zip(vector) {
        let value = (value as f64 * inv_norm) as f32;
        let code = ((value - quant.mid) * quant.inv_scale)
            .round()
            .clamp(-127.0, 127.0) as i8;
        *dst = code as u8;
        norm = norm.saturating_add(i32::from(code) * i32::from(code));
    }
    write_u32(buf, norm_off, norm as u32);
}

/// Append a node record into an already-pinned node page buffer. Returns the
/// new slot, or `None` if it does not fit (caller allocates a fresh page).
/// Reserves the full neighbour footprint; all layers start with `count = 0`.
fn node_page_append(
    buf: &mut [u8],
    row_id: RowId,
    top_layer: usize,
    vector: &[f32],
    dim: usize,
    m: usize,
    m0: usize,
    quant: PersistedNavQuant,
) -> Option<u16> {
    let rec_size = node_record_size(top_layer, dim, m, m0);
    // Need the record plus one new directory slot (2 bytes).
    if node_page_free_space(buf) < rec_size + 2 {
        return None;
    }
    let slot_count = read_u16(buf, NODE_OFF_SLOT_COUNT);
    if slot_count >= NODE_ID_SLOTS_PER_PAGE {
        return None;
    }
    let heap_top = read_u16(buf, NODE_OFF_HEAP_TOP) as usize;
    let rec_off = heap_top - rec_size;
    // Record body.
    buf[rec_off] = REC_FLAG_INITIALIZED;
    buf[rec_off + 1] = top_layer as u8;
    write_u64(buf, rec_off + 2, vec_pack_row(row_id));
    let vec_off = rec_off + 10;
    for (i, v) in vector.iter().take(dim).enumerate() {
        write_u32(buf, vec_off + i * 4, v.to_bits());
    }
    write_persisted_nav_row(buf, rec_off, dim, vector, quant);
    // All layer counts already zero (page was zeroed / region untouched);
    // be explicit in case of slot reuse in a future cold-build path.
    for l in 0..=top_layer {
        let count_off = layer_count_offset(rec_off, l, dim, m, m0);
        write_u16(buf, count_off, 0);
    }
    // Directory entry + watermarks.
    write_u16(
        buf,
        NODE_OFF_DIRECTORY + slot_count as usize * 2,
        rec_off as u16,
    );
    write_u16(buf, NODE_OFF_SLOT_COUNT, slot_count + 1);
    write_u16(buf, NODE_OFF_HEAP_TOP, rec_off as u16);
    Some(slot_count)
}

/// Locate a record offset by slot, validating bounds.
fn node_record_offset(buf: &[u8], slot: u16) -> Result<usize, QueryError> {
    let slot_count = read_u16(buf, NODE_OFF_SLOT_COUNT);
    if slot >= slot_count {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "vector index: slot out of range",
        ));
    }
    let rec_off = read_u16(buf, NODE_OFF_DIRECTORY + slot as usize * 2) as usize;
    if rec_off < NODE_OFF_DIRECTORY || rec_off + 10 > PAGE_SIZE {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "vector index: corrupt record offset",
        ));
    }
    Ok(rec_off)
}

#[cfg(test)]
/// Decode a node record into an owned [`NodeView`]. Returns `None` for a
/// slot that is not initialized (defends against dangling vids that point at
/// a never-written slot after a partially-applied / aborted write).
fn node_page_read(buf: &[u8], slot: u16, dim: usize, m: usize, m0: usize) -> Option<NodeView> {
    let rec_off = node_record_offset(buf, slot).ok()?;
    let flags = buf[rec_off];
    if flags & REC_FLAG_INITIALIZED == 0 {
        return None;
    }
    let top_layer = buf[rec_off + 1] as usize;
    let row_id = vec_unpack_row(read_u64(buf, rec_off + 2));
    let vec_off = rec_off + 10;
    let mut vector = Vec::with_capacity(dim);
    for i in 0..dim {
        vector.push(f32::from_bits(read_u32(buf, vec_off + i * 4)));
    }
    let mut neighbors = Vec::with_capacity(top_layer + 1);
    for l in 0..=top_layer {
        let count_off = layer_count_offset(rec_off, l, dim, m, m0);
        let count = read_u16(buf, count_off) as usize;
        let cap = layer_cap(l, m, m0);
        let n = count.min(cap);
        let vids_off = count_off + 2;
        let mut layer = Vec::with_capacity(n);
        for k in 0..n {
            layer.push(node_id_to_vid(read_u32(buf, vids_off + k * 4)));
        }
        neighbors.push(layer);
    }
    Some(NodeView {
        row_id,
        top_layer,
        deleted: flags & REC_FLAG_TOMBSTONE != 0,
        vector,
        neighbors,
    })
}

/// Decode only the f32 vector of `slot` into `out` (cleared first), avoiding
/// the per-layer neighbour decode + `NodeView` allocation. Returns `false`
/// for a missing / uninitialised slot. The hot path for distance scoring.
fn node_page_read_vector_into(buf: &[u8], slot: u16, dim: usize, out: &mut Vec<f32>) -> bool {
    out.clear();
    let Ok(rec_off) = node_record_offset(buf, slot) else {
        return false;
    };
    if buf[rec_off] & REC_FLAG_INITIALIZED == 0 {
        return false;
    }
    let vec_off = rec_off + 10;
    out.reserve(dim);
    // Bulk-decode the contiguous little-endian f32 run in one bounds-checked
    // slice + a `chunks_exact(4)` loop the optimizer auto-vectorizes, instead of
    // `dim` individually bounds-checked `read_u32`s — the k-NN re-rank hot path
    // decodes `ef` full vectors per query.
    let end = vec_off + dim * 4;
    if end > buf.len() {
        return false;
    }
    out.extend(
        buf[vec_off..end]
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]])),
    );
    true
}

/// Overwrite `slot`'s f32 vector contents in place (same dim, same record
/// size — no relayout), keeping the node's id, level, neighbours and tombstone
/// flag untouched. Returns `false` for a missing / uninitialised slot. The
/// drift-adaptive in-place UPDATE path: an incremental embedding refresh that
/// barely moved keeps its edges and just rewrites the vector bytes.
fn node_page_write_vector_into(
    buf: &mut [u8],
    slot: u16,
    dim: usize,
    vector: &[f32],
    quant: PersistedNavQuant,
) -> (bool, bool, bool) {
    let Ok(rec_off) = node_record_offset(buf, slot) else {
        return (false, false, false);
    };
    if buf[rec_off] & REC_FLAG_INITIALIZED == 0 {
        return (false, false, false);
    }
    let old_exact = buf[rec_off] & REC_FLAG_EXACT_NAV != 0;
    let vec_off = rec_off + 10;
    for (i, v) in vector.iter().take(dim).enumerate() {
        write_u32(buf, vec_off + i * 4, v.to_bits());
    }
    write_persisted_nav_row(buf, rec_off, dim, vector, quant);
    let new_exact = buf[rec_off] & REC_FLAG_EXACT_NAV != 0;
    (true, old_exact, new_exact)
}

/// Score a persisted SQ8 navigation row without copying it out of the data
/// buffer page. `Ok(None)` asks the caller to use the exact f32 row (the node
/// was calibrated outside the index-wide SQ8 range).
fn node_page_navigation_ssd(
    buf: &[u8],
    slot: u16,
    dim: usize,
    qcodes: &[i8],
    qnorm: i32,
) -> Result<Option<i32>, QueryError> {
    if qcodes.len() != dim {
        return Ok(None);
    }
    let rec_off = node_record_offset(buf, slot)?;
    if buf[rec_off] & REC_FLAG_INITIALIZED == 0 || buf[rec_off] & REC_FLAG_EXACT_NAV != 0 {
        return Ok(None);
    }
    let code_off = rec_off + 10 + dim * 4;
    let norm = read_u32(buf, code_off + dim) as i32;
    let code_bytes = &buf[code_off..code_off + dim];
    // Every i8 bit pattern is valid, and the borrowed slice cannot outlive
    // the pinned data-buffer page. Reinterpret in place so persistent-page
    // navigation uses the same NEON/AVX2 dot kernel as the former resident
    // arena instead of a scalar saturating loop per visited node.
    let codes =
        unsafe { std::slice::from_raw_parts(code_bytes.as_ptr().cast::<i8>(), code_bytes.len()) };
    let dot = crate::engine::vector::distance::dot_i8(qcodes, codes);
    Ok(Some(
        qnorm
            .saturating_add(norm)
            .saturating_sub(dot.saturating_mul(2)),
    ))
}

/// Decode only `slot`'s layer-`level` neighbour list into `out` (cleared
/// first), skipping the vector + other layers. Returns `false` for a missing
/// slot or a level above the node's top layer. The hot path for traversal.
fn node_page_read_neighbors_into(
    buf: &[u8],
    slot: u16,
    level: usize,
    dim: usize,
    m: usize,
    m0: usize,
    out: &mut Vec<VecId>,
) -> bool {
    out.clear();
    let Ok(rec_off) = node_record_offset(buf, slot) else {
        return false;
    };
    if buf[rec_off] & REC_FLAG_INITIALIZED == 0 {
        return false;
    }
    let top_layer = buf[rec_off + 1] as usize;
    if level > top_layer {
        return false;
    }
    let count_off = layer_count_offset(rec_off, level, dim, m, m0);
    let count = read_u16(buf, count_off) as usize;
    let n = count.min(layer_cap(level, m, m0));
    let vids_off = count_off + 2;
    out.reserve(n);
    for k in 0..n {
        out.push(node_id_to_vid(read_u32(buf, vids_off + k * 4)));
    }
    true
}

/// Append a neighbour vid to `slot`'s layer `level` in place. Returns the new
/// active count, or `None` if the reserved capacity is already full (caller
/// then prunes). Pure in-page mutation — the node never moves.
fn node_page_push_neighbor(
    buf: &mut [u8],
    slot: u16,
    level: usize,
    nb: VecId,
    dim: usize,
    m: usize,
    m0: usize,
) -> Option<usize> {
    let rec_off = node_record_offset(buf, slot).ok()?;
    let top_layer = buf[rec_off + 1] as usize;
    if level > top_layer {
        return None;
    }
    let count_off = layer_count_offset(rec_off, level, dim, m, m0);
    let count = read_u16(buf, count_off) as usize;
    let cap = layer_cap(level, m, m0);
    if count >= cap {
        return None;
    }
    let vids_off = count_off + 2;
    let node_id = node_id_from_vid(nb).ok()?;
    write_u32(buf, vids_off + count * 4, node_id);
    write_u16(buf, count_off, (count + 1) as u16);
    Some(count + 1)
}

/// Overwrite `slot`'s layer `level` neighbour list with `nbrs` (must be
/// `<= cap`). Used by the prune path. In place.
fn node_page_set_neighbors(
    buf: &mut [u8],
    slot: u16,
    level: usize,
    nbrs: &[VecId],
    dim: usize,
    m: usize,
    m0: usize,
) -> Result<(), QueryError> {
    let rec_off = node_record_offset(buf, slot)?;
    let top_layer = buf[rec_off + 1] as usize;
    if level > top_layer {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "vector index: level out of range",
        ));
    }
    let cap = layer_cap(level, m, m0);
    if nbrs.len() > cap {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "vector index: neighbour overflow",
        ));
    }
    let count_off = layer_count_offset(rec_off, level, dim, m, m0);
    let vids_off = count_off + 2;
    for (k, &nb) in nbrs.iter().enumerate() {
        write_u32(buf, vids_off + k * 4, node_id_from_vid(nb)?);
    }
    write_u16(buf, count_off, nbrs.len() as u16);
    Ok(())
}

/// Set / clear the tombstone flag on `slot`. Returns the previous state.
fn node_page_set_tombstone(buf: &mut [u8], slot: u16, deleted: bool) -> Result<bool, QueryError> {
    let rec_off = node_record_offset(buf, slot)?;
    let prev = buf[rec_off] & REC_FLAG_TOMBSTONE != 0;
    if deleted {
        buf[rec_off] |= REC_FLAG_TOMBSTONE;
    } else {
        buf[rec_off] &= !REC_FLAG_TOMBSTONE;
    }
    Ok(prev)
}

/// The paged HNSW index file. Mirrors [`IndexFile`] but stores the graph
/// instead of a B-tree. Phase 1 is the storage substrate + a single-writer
/// access surface; the per-page latch + structure-lock concurrency comes in
/// Phase 3.
#[derive(Debug)]
pub(crate) struct VectorIndexFile {
    /// Absolute path (kept for diagnostics / `Debug`; page IO addresses the
    /// buffer pool by `file_key` and the WAL by `rel_path`).
    #[allow(dead_code)]
    path: PathBuf,
    file: parking_lot::Mutex<Option<File>>,
    log: Arc<crate::wal::LogMutex>,
    /// Lock-free WAL state mirror, reserved for a future checkpoint-watermark
    /// probe on the paged graph (parity with the B-tree index file).
    #[allow(dead_code)]
    log_atomics: Arc<crate::wal::LogStateAtomics>,
    durability: DurabilityMode,
    file_key: Arc<BufferPageFileKey>,
    buffer_pool: Arc<BufferPool>,
    /// Authoritative meta image + decoded meta + pristine tracker.
    meta_state: parking_lot::Mutex<VectorMetaState>,
    /// In-memory hint: the node page currently being filled (0 = none). Not
    /// persisted — on reopen the first append allocates a fresh page, which
    /// only loses the tail free space of one partially-filled page.
    append_hint: AtomicU64,
    /// Serializes the full-page → allocate-new-page transition. The page latch
    /// handles appends to an existing page; this lock prevents concurrent
    /// losers from each allocating a mostly-empty replacement page.
    append_alloc: parking_lot::Mutex<()>,
    /// Serialize meta snapshot + WAL publication, preventing stale write-back.
    meta_write: parking_lot::Mutex<()>,
    /// Per-page latches (hashed by page id) serialising read-modify-write of
    /// the same page across concurrent writers.
    page_latches: Box<[parking_lot::Mutex<()>]>,
    rel_path: String,
    /// Immutable graph geometry (`dim`, `m`, `m0`), fixed at create time and
    /// validated against the meta page on reopen. Cached here so the hottest
    /// read/write paths (`read_vector_into`, `read_neighbors_into`,
    /// `push_neighbor`, …) never take `meta_state.lock()` just to learn the
    /// record layout — that global mutex otherwise serialises a parallel cold
    /// build (every distance score locked it). `meta_state` remains the
    /// authoritative source for the *mutable* meta (entry point, counts, …).
    dim_fixed: usize,
    m_fixed: usize,
    m0_fixed: usize,
    /// Bulk-build (deferred-WAL) mode. While set, `write_page` mutates pages
    /// in the buffer pool **without** emitting a per-edit `PageUpdate` WAL
    /// record, instead recording the page id in `touched_pages`. A cold build
    /// wires ~`m0` edges per node, each a full-page rewrite, so per-edit WAL
    /// would log O(edges) full pages (gigabytes for a modest index);
    /// `finalize_build` instead emits exactly **one** record per touched page
    /// (O(pages)), recovered by the same generic `redo_page_update` path. Only
    /// the single-writer cold build sets this; incremental DML inserts never
    /// do (they need per-statement WAL durability).
    build_mode: AtomicBool,
    /// Page ids dirtied while `build_mode` is set, flushed to the WAL once by
    /// `finalize_build`.
    touched_pages: parking_lot::Mutex<HashSet<u64>>,
    mapped_navigation: parking_lot::RwLock<Option<MappedVectorNavigation>>,
    mapping_allowed: AtomicBool,
    navigation_writers: AtomicU64,
    navigation_revision: AtomicU64,
}

#[derive(Debug)]
struct VectorMetaState {
    meta: VectorMeta,
    meta_image: Vec<u8>,
    /// Page ids returned by the extend-file branch of `alloc_page` whose
    /// first write can skip the full-page before-image (no torn disk image
    /// exists yet) — same optimisation as B-tree `pristine_alloc_pages`.
    pristine_pages: HashSet<u64>,
}

impl VectorIndexFile {
    /// Flush every dirty frame belonging to this vector file. Atomic rebuild
    /// uses this before publishing the temporary file with `rename(2)`.
    pub(crate) fn flush(&self) -> Result<(), QueryError> {
        self.buffer_pool.flush_file(&self.file_key)
    }

    /// Whether this index has an OS file that can receive the sequential
    /// cold-build image directly. Memory durability keeps using the ordinary
    /// buffer-pool builder as its differential oracle.
    pub(crate) fn supports_direct_bulk_build(&self) -> bool {
        self.durability != DurabilityMode::Memory
    }

    /// Materialize a finalized dense HNSW topology straight into sequential
    /// `.hvec` pages. `top_layer(id)` is called first for every dense node so
    /// all stable vids are known before neighbour ids are translated. `fetch`
    /// then copies one node at a time into reusable scratch buffers.
    ///
    /// Cold-build node pages are derived data until the finalized meta page is
    /// WAL-published. Writing the complete node image directly, syncing it,
    /// and only then logging the meta commit is crash-safe by the same ordering
    /// contract as [`Self::finalize_build`], while avoiding one buffer-frame
    /// flush and one 16 KiB checksum visit per resident page.
    pub(crate) fn bulk_build_direct<FT, FF>(
        &self,
        node_count: usize,
        entry: Option<(u32, usize)>,
        mut top_layer: FT,
        mut fetch: FF,
    ) -> Result<(), QueryError>
    where
        FT: FnMut(usize) -> usize,
        FF: FnMut(usize, &mut Vec<f32>, &mut Vec<Vec<u32>>) -> Result<RowId, QueryError>,
    {
        let _mapping_write = self.navigation_write();
        if !self.supports_direct_bulk_build() {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "direct vector bulk build requires file-backed durability",
            ));
        }

        let (dim, m, m0) = (self.dim_fixed, self.m_fixed, self.m0_fixed);
        let nav_quant = self.persisted_nav_quant();
        let usable = PAGE_SIZE - NODE_OFF_DIRECTORY;
        let mut vids = Vec::with_capacity(node_count);
        let mut levels = Vec::with_capacity(node_count);
        let mut page_id = 1u64;
        let mut slot = 0u16;
        let mut free = usable;
        for id in 0..node_count {
            let level = top_layer(id);
            let record = node_record_size(level, dim, m, m0);
            if record + 2 > usable {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "vector index: node record exceeds page capacity (dimension too large)",
                ));
            }
            if record + 2 > free || slot >= NODE_ID_SLOTS_PER_PAGE {
                page_id += 1;
                if page_id > NODE_ID_MAX_PAGE {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "vector index: v3 navigation page limit exceeded",
                    ));
                }
                slot = 0;
                free = usable;
            }
            vids.push(vid_make(page_id, slot));
            levels.push(level);
            slot = slot.saturating_add(1);
            free -= record + 2;
        }
        let last_page = if node_count == 0 { 0 } else { page_id };

        // No stale buffer frame may race a later cleaner flush over the direct
        // image. The freshly opened meta page is reconstructed below.
        self.build_mode.store(false, Ordering::Release);
        self.touched_pages.lock().clear();
        self.buffer_pool.discard_file(&self.file_key)?;

        if node_count > 0 {
            const WRITE_BATCH_PAGES: usize = 256; // 4 MiB sequential writes
            let mut file_guard = self.file.lock();
            let file = file_guard.as_mut().ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "vector index file is closed during bulk build",
                )
            })?;
            let mut image = vec![0u8; PAGE_SIZE];
            format_empty_node_page(&mut image);
            let mut image_page = 1u64;
            let mut vector = Vec::with_capacity(dim);
            let mut dense_neighbors: Vec<Vec<u32>> = Vec::new();
            let mut mapped = Vec::<VecId>::with_capacity(m0.max(m));
            let mut write_batch = Vec::with_capacity(WRITE_BATCH_PAGES * PAGE_SIZE);
            let mut batch_first_page = 1u64;
            let mut nav_exact_nodes = 0u64;

            let flush_page = |page: &mut Vec<u8>,
                              page_no: u64,
                              batch: &mut Vec<u8>,
                              batch_first: &mut u64,
                              file: &File|
             -> Result<(), QueryError> {
                refresh_page_checksum(page);
                if batch.is_empty() {
                    *batch_first = page_no;
                }
                batch.extend_from_slice(page);
                if batch.len() >= WRITE_BATCH_PAGES * PAGE_SIZE {
                    file.write_all_at(batch, *batch_first * PAGE_SIZE as u64)
                        .map_err(io_err)?;
                    batch.clear();
                }
                Ok(())
            };

            for id in 0..node_count {
                let expected = vids[id];
                let expected_page = vid_page(expected);
                if expected_page != image_page {
                    flush_page(
                        &mut image,
                        image_page,
                        &mut write_batch,
                        &mut batch_first_page,
                        file,
                    )?;
                    image_page = expected_page;
                    format_empty_node_page(&mut image);
                }
                let row_id = fetch(id, &mut vector, &mut dense_neighbors)?;
                nav_exact_nodes += u64::from(persisted_nav_is_exact(&vector, nav_quant));
                let actual_slot = node_page_append(
                    &mut image, row_id, levels[id], &vector, dim, m, m0, nav_quant,
                )
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "vector bulk page packing mismatch",
                    )
                })?;
                if actual_slot != vid_slot(expected) {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "vector bulk vid packing mismatch",
                    ));
                }
                for (level, dense) in dense_neighbors.iter().enumerate() {
                    mapped.clear();
                    mapped.reserve(dense.len());
                    for &neighbor in dense {
                        let mapped_vid = vids.get(neighbor as usize).copied().ok_or_else(|| {
                            error(
                                crate::error::catalog::ER_GET_ERRNO,
                                "vector bulk neighbour id out of range",
                            )
                        })?;
                        mapped.push(mapped_vid);
                    }
                    node_page_set_neighbors(&mut image, actual_slot, level, &mapped, dim, m, m0)?;
                }
            }
            flush_page(
                &mut image,
                image_page,
                &mut write_batch,
                &mut batch_first_page,
                file,
            )?;
            if !write_batch.is_empty() {
                file.write_all_at(&write_batch, batch_first_page * PAGE_SIZE as u64)
                    .map_err(io_err)?;
            }
            file.set_len((last_page + 1) * PAGE_SIZE as u64)
                .map_err(io_err)?;
            file.flush().map_err(io_err)?;
            if self.durability == DurabilityMode::Strict {
                file.sync_data().map_err(io_err)?;
            }
            self.meta_state.lock().meta.nav_exact_nodes = nav_exact_nodes;
        } else if let Some(file) = self.file.lock().as_mut() {
            file.set_len(PAGE_SIZE as u64).map_err(io_err)?;
            file.flush().map_err(io_err)?;
            if self.durability == DurabilityMode::Strict {
                file.sync_data().map_err(io_err)?;
            }
        }

        {
            let mut state = self.meta_state.lock();
            state.meta.entry_vid = entry
                .and_then(|(id, _)| vids.get(id as usize).copied())
                .unwrap_or(VEC_NO_VID);
            state.meta.max_layer = entry.map_or(0, |(_, level)| level as u32);
            state.meta.node_next_page_id = last_page + 1;
            state.meta.node_count = node_count as u64;
            state.meta.tombstone_count = 0;
            state.meta.finalized = true;
            state.meta.built_lsn = self.log_atomics.next_lsn();
            state.pristine_pages.clear();
        }
        self.append_hint.store(last_page, Ordering::Release);
        self.write_meta_page(txn_id_for_ddl())
    }

    /// Open (or create) the `.hvec` file. A new file lays down the meta page
    /// from the supplied parameters; an existing file is validated against
    /// `dim`/`metric`/`m` so a parameter change forces a rebuild upstream.
    #[allow(private_interfaces)] // `BufferPool` is core-private; this ctor is only called inside `core`.
    pub(crate) fn open_with_durability(
        path: PathBuf,
        log: Arc<crate::wal::LogMutex>,
        durability: DurabilityMode,
        buffer_pool: Arc<BufferPool>,
        dim: u32,
        metric: u8,
        m: u16,
        m0: u16,
        ef_construction: u16,
    ) -> Result<Self, QueryError> {
        let mut file = if durability == DurabilityMode::Memory {
            None
        } else {
            Some(crate::storage::image_io::open_data_file(&path).map_err(io_err)?)
        };
        let len = file
            .as_ref()
            .map(|f| f.metadata().map_err(io_err).map(|meta| meta.len()))
            .transpose()?
            .unwrap_or(0);
        if len > 0 && len < PAGE_SIZE as u64 {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt vector index file length",
            ));
        }
        let initialized = len >= PAGE_SIZE as u64;
        let (meta, meta_image) = if initialized {
            let page = read_index_page(
                file.as_mut()
                    .expect("vector index file should be open outside memory mode"),
                VEC_META_PAGE,
            )?;
            let meta = decode_vector_meta(&page)?;
            if meta.dim != dim || meta.metric != metric || meta.m != m {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "vector index parameters changed; rebuild required",
                ));
            }
            (meta, page)
        } else {
            let meta = VectorMeta::fresh(dim, metric, m, m0, ef_construction);
            (meta.clone(), encode_vector_meta(&meta))
        };
        let file_key = Arc::new(BufferPool::vector_index_file_key(path.clone()));
        let rel_path = Self::compute_relative_path(&path);
        let log_atomics = log.lock().state_atomics();
        let dim_fixed = meta.dim as usize;
        let m_fixed = meta.m as usize;
        let m0_fixed = meta.m0 as usize;
        let this = Self {
            path,
            file: parking_lot::Mutex::new(file),
            log,
            log_atomics,
            durability,
            file_key,
            buffer_pool,
            meta_state: parking_lot::Mutex::new(VectorMetaState {
                meta,
                meta_image,
                pristine_pages: HashSet::new(),
            }),
            dim_fixed,
            m_fixed,
            m0_fixed,
            append_hint: AtomicU64::new(0),
            append_alloc: parking_lot::Mutex::new(()),
            meta_write: parking_lot::Mutex::new(()),
            page_latches: (0..VEC_PAGE_LATCH_COUNT)
                .map(|_| parking_lot::Mutex::new(()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            rel_path,
            build_mode: AtomicBool::new(false),
            touched_pages: parking_lot::Mutex::new(HashSet::new()),
            mapped_navigation: parking_lot::RwLock::new(None),
            mapping_allowed: AtomicBool::new(true),
            navigation_writers: AtomicU64::new(0),
            navigation_revision: AtomicU64::new(0),
        };
        if !initialized {
            this.write_meta_page(txn_id_for_ddl())?;
        }
        Ok(this)
    }

    fn compute_relative_path(path: &Path) -> String {
        crate::storage::naming::wal_relative_object_path(path)
    }

    fn relative_path(&self) -> String {
        self.rel_path.clone()
    }

    pub(crate) fn spill_data_dir(&self) -> &Path {
        let relative = Path::new(&self.rel_path);
        let canonical_root = relative.components().next().is_some_and(|part| {
            matches!(
                part.as_os_str().to_str(),
                Some("objects" | "schemas" | "system")
            )
        });
        if canonical_root && !relative.is_absolute() && self.path.ends_with(relative) {
            if let Some(root) = self.path.ancestors().nth(relative.components().count()) {
                return root;
            }
        }
        self.path.parent().unwrap_or_else(|| Path::new("."))
    }

    pub(crate) fn build_adj_temp_path(&self) -> PathBuf {
        let mut path = self.path.as_os_str().to_os_string();
        path.push(".build-adj");
        PathBuf::from(path)
    }

    fn page_key(&self, page_id: u64) -> PageKey {
        PageKey {
            file: self.file_key.clone(),
            page_id,
        }
    }

    /// Acquire the latch guarding read-modify-write of `page_id`.
    fn page_latch(&self, page_id: u64) -> parking_lot::MutexGuard<'_, ()> {
        self.page_latches[(page_id & VEC_PAGE_LATCH_MASK) as usize].lock()
    }

    pub(crate) fn meta(&self) -> VectorMeta {
        self.meta_state.lock().meta.clone()
    }

    pub(crate) fn dim(&self) -> usize {
        self.dim_fixed
    }

    #[inline]
    pub(crate) fn node_id(&self, vid: VecId) -> Result<NodeId, QueryError> {
        node_id_from_vid(vid)
    }

    #[inline]
    pub(crate) fn vid_from_node_id(&self, node_id: NodeId) -> VecId {
        node_id_to_vid(node_id)
    }

    fn persisted_nav_quant(&self) -> PersistedNavQuant {
        let meta = self.meta_state.lock();
        PersistedNavQuant {
            valid: meta.meta.nav_quant_valid,
            metric: meta.meta.metric,
            mid: meta.meta.nav_mid,
            inv_scale: meta.meta.nav_inv_scale,
        }
    }

    pub(crate) fn navigation_quantization(&self) -> Option<(f32, f32, f32)> {
        let meta = self.meta_state.lock();
        meta.meta.nav_quant_valid.then_some((
            meta.meta.nav_mid,
            meta.meta.nav_inv_scale,
            meta.meta.nav_dist_mul,
        ))
    }

    /// Install the one index-wide SQ8 calibration before a cold build starts.
    /// It is persisted in the meta page and reused by incremental writes.
    pub(crate) fn set_navigation_quantization(
        &self,
        params: Option<(f32, f32, f32)>,
    ) -> Result<(), QueryError> {
        {
            let mut state = self.meta_state.lock();
            match params {
                Some((mid, inv_scale, dist_mul))
                    if mid.is_finite()
                        && inv_scale.is_finite()
                        && inv_scale > 0.0
                        && dist_mul.is_finite()
                        && dist_mul > 0.0 =>
                {
                    state.meta.nav_quant_valid = true;
                    state.meta.nav_mid = mid;
                    state.meta.nav_inv_scale = inv_scale;
                    state.meta.nav_dist_mul = dist_mul;
                }
                _ => {
                    state.meta.nav_quant_valid = false;
                    state.meta.nav_mid = 0.0;
                    state.meta.nav_inv_scale = 1.0;
                    state.meta.nav_dist_mul = 1.0;
                }
            }
        }
        self.write_meta_page(txn_id_for_ddl())
    }

    /// Recalibrate persisted SQ8 in two sequential passes with O(dim) scratch.
    /// The caller excludes navigation and vector writers for the whole pass.
    /// Persist an invalid calibration first: interruption then safely selects
    /// exact navigation instead of mixing old parameters with rewritten codes.
    pub(crate) fn recalibrate_navigation(&self, extra: &[f32]) -> Result<(), QueryError> {
        let metric = match self.meta().metric {
            1 => crate::engine::vector::distance::Metric::Cosine,
            2 => return Ok(()),
            _ => crate::engine::vector::distance::Metric::L2,
        };
        let mut stats = crate::engine::vector::hnsw::CompactHnswStats::new(metric);
        if !extra.is_empty() {
            stats.observe(extra);
        }
        let next = self.meta().node_next_page_id;
        let mut vector = Vec::with_capacity(self.dim_fixed);
        for page_id in 1..next {
            self.buffer_pool
                .with_raw_page(self.page_key(page_id), |bytes| {
                    if read_u32(bytes, 0) == VEC_NODE_MAGIC {
                        for slot in 0..read_u16(bytes, NODE_OFF_SLOT_COUNT) {
                            let offset = node_record_offset(bytes, slot)?;
                            if bytes[offset] & REC_FLAG_INITIALIZED == 0 {
                                continue;
                            }
                            vector.clear();
                            for i in 0..self.dim_fixed {
                                vector.push(f32::from_bits(read_u32(bytes, offset + 10 + 4 * i)));
                            }
                            stats.observe(&vector);
                        }
                    }
                    Ok(())
                })?;
        }
        let Some((mid, inv_scale, dist_mul)) = stats.quantization_params() else {
            return Ok(());
        };
        // Headroom reduces repeated fallback for values near the sample edge.
        let params = (mid, inv_scale / 1.04, dist_mul * 1.04 * 1.04);
        self.set_navigation_quantization(None)?;
        let quant = PersistedNavQuant {
            valid: true,
            metric: self.meta().metric,
            mid: params.0,
            inv_scale: params.1,
        };
        let mut exact = 0u64;
        for page_id in 1..next {
            let _latch = self.page_latch(page_id);
            self.rmw_node_page(txn_id_for_ddl(), page_id, |bytes| {
                if read_u32(bytes, 0) != VEC_NODE_MAGIC {
                    return Ok(((), false));
                }
                for slot in 0..read_u16(bytes, NODE_OFF_SLOT_COUNT) {
                    let offset = node_record_offset(bytes, slot)?;
                    if bytes[offset] & REC_FLAG_INITIALIZED == 0 {
                        continue;
                    }
                    vector.clear();
                    for i in 0..self.dim_fixed {
                        vector.push(f32::from_bits(read_u32(bytes, offset + 10 + 4 * i)));
                    }
                    write_persisted_nav_row(bytes, offset, self.dim_fixed, &vector, quant);
                    exact += u64::from(bytes[offset] & REC_FLAG_EXACT_NAV != 0);
                }
                Ok(((), true))
            })?;
        }
        self.meta_state.lock().meta.nav_exact_nodes = exact;
        self.set_navigation_quantization(Some(params))
    }

    /// Current entry point `(vid, max_layer)`, or `None` if empty.
    pub(crate) fn entry_with_level(&self) -> Option<(VecId, u32)> {
        let meta = self.meta_state.lock();
        if meta.meta.entry_vid == VEC_NO_VID {
            None
        } else {
            Some((meta.meta.entry_vid, meta.meta.max_layer))
        }
    }

    pub(crate) fn node_count(&self) -> u64 {
        self.meta_state.lock().meta.node_count
    }

    pub(crate) fn file_size_bytes(&self) -> u64 {
        self.file
            .lock()
            .as_ref()
            .and_then(|file| file.metadata().ok())
            .map_or(0, |meta| meta.len())
    }

    fn read_raw_page_image(&self, page_id: u64) -> Result<Vec<u8>, QueryError> {
        if page_id == VEC_META_PAGE {
            return Ok(self.meta_state.lock().meta_image.clone());
        }
        read_vector_index_page_image(&self.buffer_pool, &self.file, self.page_key(page_id))
    }

    /// Pin `page_id` and hand back a clone of its raw 16 KiB image.
    fn read_page_bytes(&self, page_id: u64) -> Result<Vec<u8>, QueryError> {
        self.buffer_pool.read_raw_page(self.page_key(page_id))
    }

    /// Write a full page image through the buffer pool. System transactions
    /// emit redo-only FullPageImage records; other callers retain PageUpdate
    /// before-images. Both paths preserve WAL ordering and torn-page recovery.
    fn write_page(&self, txn_id: u64, page_id: u64, mut after: Vec<u8>) -> Result<(), QueryError> {
        let _mapping_write = self.navigation_write();
        debug_assert_eq!(after.len(), PAGE_SIZE);
        let key = self.page_key(page_id);
        // Bulk-build mode: skip the per-edit WAL entirely and just dirty the
        // page in the pool, deferring a single WAL record per page to
        // `finalize_build` (which re-scans the node-page range). No per-write
        // `touched_pages` bookkeeping — that global mutex would serialise the
        // parallel build's edge writes.
        if self.build_mode.load(Ordering::Acquire) {
            {
                let mut state = self.meta_state.lock();
                state.pristine_pages.remove(&page_id);
            }
            self.buffer_pool.store_page(key, after, 0)?;
            return Ok(());
        }
        let pristine = {
            let mut state = self.meta_state.lock();
            state.pristine_pages.remove(&page_id)
        };
        if txn_id == 0 && self.durability != DurabilityMode::Memory {
            // System graph maintenance is redo-only. An edit to a resident
            // page whose dirty epoch is already anchored by a full image logs
            // only a dictionary delta: a neighbour-list rewrite touches a few
            // hundred bytes of a 16 KiB page.
            stamp_vector_page(&mut after, page_id, 0);
            let relative = self.relative_path();
            let _publication = self.buffer_pool.page_wal_publish_gate.read();
            let mut pending = Some(after);
            let replaced = self.buffer_pool.try_replace_resident_logged_page(
                &key,
                |before, anchored, _| {
                    let mut after = pending.take().expect("replacement callback runs once");
                    let payload = anchored
                        .then(|| {
                            if page_id == VEC_META_PAGE && read_u32(before, 0) == VEC_META_MAGIC {
                                WalPayload::page_delta_prehashed(
                                    relative.clone(), page_id, crate::wal::page_lsn(before),
                                    before, &after, vector_meta_crc(before, false),
                                    vector_meta_crc(&after, false),
                                )
                            } else {
                                WalPayload::page_delta(
                                    relative.clone(), page_id, crate::wal::page_lsn(before),
                                    before, &after,
                                )
                            }
                        })
                        .flatten()
                        .unwrap_or_else(|| {
                            WalPayload::full_page_image(relative.clone(), page_id, after.clone())
                        });
                    let mut log = self.log.lock();
                    let lsn = log.reserve_lsn()?;
                    if crate::wal::page_lsn(before) >= lsn {
                        return Err(error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "vector page replacement LSN does not follow its base",
                        ));
                    }
                    log.append_reserved(lsn, 0, payload)?;
                    drop(log);
                    stamp_vector_page(&mut after, page_id, lsn);
                    Ok((lsn, after))
                },
            )?;
            if replaced.is_some() {
                return Ok(());
            }
            after = pending.expect("nonresident path keeps the replacement image");
        }
        let lsn = if self.durability == DurabilityMode::Memory {
            0
        } else {
            // System graph maintenance is redo-only. A full after-image
            // provides the same torn-page protection without copying/logging
            // a before-image that recovery never consumes for txn 0.
            let before = if txn_id == 0 {
                None
            } else if pristine {
                Some(vec![0u8; PAGE_SIZE])
            } else {
                Some(self.read_raw_page_image(page_id)?)
            };
            log_vector_index_page_image(
                &self.log,
                txn_id,
                self.relative_path(),
                page_id,
                before,
                &mut after,
            )?
        };
        self.buffer_pool.store_page(key, after, lsn)?;
        Ok(())
    }

    fn write_meta_page(&self, txn_id: u64) -> Result<(), QueryError> {
        let _write = self.meta_write.lock();
        let after = {
            let state = self.meta_state.lock();
            encode_vector_meta(&state.meta)
        };
        // Meta page is page 0 and is never pristine (always has a prior image
        // from open). Reuse the stored meta_image as the before via
        // read_raw_page_image's page-0 branch.
        self.write_page(txn_id, VEC_META_PAGE, after.clone())?;
        let mut state = self.meta_state.lock();
        // after may have been LSN-stamped inside write_page on a clone; re-encode
        // is unnecessary — store the stamped image is not observable to readers
        // (they decode meta fields, not LSN). Keep the unstamped encoding as the
        // before-image source; it round-trips all meta fields identically.
        state.meta_image = after;
        Ok(())
    }

    /// Allocate a node page id by extending the append-only file. The new id is
    /// marked pristine so its first write skips the FPI before-image.
    fn alloc_node_page(&self, txn_id: u64) -> Result<u64, QueryError> {
        let page_id = {
            let mut state = self.meta_state.lock();
            let page_id = state.meta.node_next_page_id;
            if page_id > NODE_ID_MAX_PAGE {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "vector index: v3 navigation page limit exceeded",
                ));
            }
            state.meta.node_next_page_id += 1;
            state.pristine_pages.insert(page_id);
            page_id
        };
        self.write_meta_page(txn_id)?;
        Ok(page_id)
    }

    /// Append a node (with its f32 vector and reserved-but-empty neighbour
    /// footprint) into the graph store, returning its stable vid. Places it
    /// on the current fill page if it fits, else allocates a fresh node page.
    /// The node-page write is WAL-logged and durable; `node_count` is bumped
    /// in memory (not load-bearing for traversal, persisted opportunistically
    /// via `flush_meta`).
    pub(crate) fn append_node(
        &self,
        txn_id: u64,
        row_id: RowId,
        top_layer: usize,
        vector: &[f32],
    ) -> Result<VecId, QueryError> {
        let (dim, m, m0) = (self.dim_fixed, self.m_fixed, self.m0_fixed);
        if vector.len() != dim {
            return Err(error(
                crate::error::catalog::MYDB_ER_INVALID_VECTOR_VALUE,
                format!(
                    "vector index dimension mismatch: expected {dim}, got {}",
                    vector.len()
                ),
            ));
        }

        let nav_quant = self.persisted_nav_quant();
        let nav_exact = persisted_nav_is_exact(vector, nav_quant);
        let rec_size = node_record_size(top_layer, dim, m, m0);
        // A node must fit in an otherwise-empty page (header + directory slot).
        let usable = PAGE_SIZE - NODE_OFF_DIRECTORY;
        if rec_size + 2 > usable {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "vector index: node record exceeds page capacity (dimension too large)",
            ));
        }
        // Fast path: append into the current fill page under its latch (so
        // concurrent appenders to the same page serialise and re-read the
        // committed bytes rather than losing each other's writes).
        let page_id = self.append_hint.load(Ordering::Acquire);
        if page_id != 0 {
            let _latch = self.page_latch(page_id);
            let slot = self.rmw_node_page(txn_id, page_id, |bytes| {
                match node_page_append(bytes, row_id, top_layer, vector, dim, m, m0, nav_quant) {
                    Some(slot) => Ok((Some(slot), true)),
                    None => Ok((None, false)),
                }
            })?;
            if let Some(slot) = slot {
                let mut state = self.meta_state.lock();
                state.meta.node_count += 1;
                state.meta.nav_exact_nodes += u64::from(nav_exact);
                return Ok(vid_make(page_id, slot));
            }
        }
        // Current page is full (or none yet): serialize allocation and retry
        // the hint because another appender may have published a fresh page
        // while we waited.
        let _alloc = self.append_alloc.lock();
        let retry_page = self.append_hint.load(Ordering::Acquire);
        if retry_page != 0 && retry_page != page_id {
            let _latch = self.page_latch(retry_page);
            let slot = self.rmw_node_page(txn_id, retry_page, |bytes| {
                match node_page_append(bytes, row_id, top_layer, vector, dim, m, m0, nav_quant) {
                    Some(slot) => Ok((Some(slot), true)),
                    None => Ok((None, false)),
                }
            })?;
            if let Some(slot) = slot {
                let mut state = self.meta_state.lock();
                state.meta.node_count += 1;
                state.meta.nav_exact_nodes += u64::from(nav_exact);
                return Ok(vid_make(retry_page, slot));
            }
        }
        // Allocate a fresh page and place
        // the node directly into it. The new page id is exclusively ours until
        // we publish it as the hint, so its first write needs no latch.
        let new_page = self.alloc_node_page(txn_id)?;
        let mut bytes = vec![0u8; PAGE_SIZE];
        format_empty_node_page(&mut bytes);
        let slot = node_page_append(&mut bytes, row_id, top_layer, vector, dim, m, m0, nav_quant)
            .expect("a freshly formatted page must fit one node (checked above)");
        self.write_page(txn_id, new_page, bytes)?;
        self.append_hint.store(new_page, Ordering::Release);
        let mut state = self.meta_state.lock();
        state.meta.node_count += 1;
        state.meta.nav_exact_nodes += u64::from(nav_exact);
        Ok(vid_make(new_page, slot))
    }

    #[cfg(test)]
    /// Read a node by vid. Returns `None` for a vid whose slot is missing or
    /// uninitialized (defends traversal against dangling edges). The search
    /// re-rank uses the leaner `read_vector_meta_into`; this complete decode
    /// remains for tests and diagnostics.
    pub(crate) fn read_node(&self, vid: VecId) -> Result<Option<NodeView>, QueryError> {
        let (dim, m, m0) = (self.dim_fixed, self.m_fixed, self.m0_fixed);
        let page_id = vid_page(vid);
        if page_id == VEC_META_PAGE {
            return Ok(None);
        }
        let bytes = self.read_page_bytes(page_id)?;
        if read_u32(&bytes, 0) != VEC_NODE_MAGIC {
            return Ok(None);
        }
        Ok(node_page_read(&bytes, vid_slot(vid), dim, m, m0))
    }

    /// Read only `vid`'s f32 vector into `out`, borrowing the pinned page (no
    /// 16 KiB clone, no `NodeView` allocation). Returns `false` for a missing
    /// node. The hot path for build/search distance scoring.
    /// Check physical headroom at a lock-free linking boundary. Container
    /// reservations cannot predict allocator fragmentation or peer clients;
    /// evict optional raw pages before these consume the sampling margin.
    pub(crate) fn reclaim_build_pressure(&self) -> Result<(), QueryError> {
        use crate::engine::executor::mem_budget::{
            controlled_memory_snapshot, managed_memory_limit_bytes,
        };
        let headroom = (managed_memory_limit_bytes() / 16).min(64 * 1024 * 1024);
        let deficit = crate::engine::memory_reclaimer::process_headroom_reclaim_bytes(
            headroom,
            managed_memory_limit_bytes(),
        );
        if deficit == 0 {
            return Ok(());
        }
        static RECLAIM: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let Ok(_guard) = RECLAIM.try_lock() else {
            return Ok(());
        };
        // Lower this build's temporary admission watermark as well as its
        // residency. Otherwise concurrent prefetch immediately refills pages
        // evicted by the reclaimer and it cannot establish physical headroom.
        // The enclosing build lease restores the original capacity on exit.
        let old_pages = self.buffer_pool.capacity_pages();
        let floor_pages = (16 * 1024 * 1024 / PAGE_SIZE).max(1) as u64;
        let reclaim_pages = deficit.max(headroom).div_ceil(PAGE_SIZE) as u64;
        let target = old_pages
            .saturating_sub(reclaim_pages)
            .max(floor_pages)
            .min(old_pages);
        if target < old_pages {
            self.buffer_pool
                .set_capacity(target, Duration::from_millis(100))?;
        }
        crate::engine::memory_reclaimer::release_allocator_cached_memory();
        if std::env::var("MYDB_HNSW_BUILD_TIMING").is_ok() {
            eprintln!(
                "[paged] pressure reclaim: deficit={} target={} controlled={} private={:?}",
                deficit,
                target,
                controlled_memory_snapshot().0,
                crate::engine::memory_reclaimer::current_private_footprint_bytes()
            );
        }
        Ok(())
    }

    pub(crate) fn read_vector_into(
        &self,
        vid: VecId,
        out: &mut Vec<f32>,
    ) -> Result<bool, QueryError> {
        let dim = self.dim_fixed;
        let page_id = vid_page(vid);
        if page_id == VEC_META_PAGE {
            return Ok(false);
        }
        self.buffer_pool
            .with_raw_page(self.page_key(page_id), |bytes| {
                if read_u32(bytes, 0) != VEC_NODE_MAGIC {
                    return Ok(false);
                }
                Ok(node_page_read_vector_into(bytes, vid_slot(vid), dim, out))
            })
    }

    /// SQ8 navigation distance read directly from the shared data-buffer page.
    /// No code row or long-lived cache is materialised outside BufferPool.
    pub(crate) fn navigation_distance(
        &self,
        vid: VecId,
        qcodes: &[i8],
        qnorm: i32,
    ) -> Result<Option<f64>, QueryError> {
        let Some((_, _, dist_mul)) = self.navigation_quantization() else {
            return Ok(None);
        };
        let page_id = vid_page(vid);
        if page_id == VEC_META_PAGE {
            return Ok(None);
        }
        self.buffer_pool.with_raw_page_intent(
            self.page_key(page_id),
            AccessIntent::Probe,
            |bytes| {
                if read_u32(bytes, 0) != VEC_NODE_MAGIC {
                    return Ok(None);
                }
                Ok(
                    node_page_navigation_ssd(bytes, vid_slot(vid), self.dim_fixed, qcodes, qnorm)?
                        .map(|ssd| ssd as f64 * dist_mul as f64),
                )
            },
        )
    }

    /// Copy the persisted navigation row, preserving the durable EXACT_NAV
    /// fallback. Used by incremental maintenance when no resident code exists.
    pub(crate) fn read_navigation_codes_into(
        &self,
        vid: VecId,
        out: &mut Vec<i8>,
    ) -> Result<Option<i32>, QueryError> {
        if vid_page(vid) == VEC_META_PAGE {
            return Ok(None);
        }
        self.buffer_pool.with_raw_page_intent(
            self.page_key(vid_page(vid)),
            AccessIntent::Probe,
            |bytes| {
                if read_u32(bytes, 0) != VEC_NODE_MAGIC {
                    return Ok(None);
                }
                let rec = node_record_offset(bytes, vid_slot(vid))?;
                if bytes[rec] & REC_FLAG_INITIALIZED == 0 || bytes[rec] & REC_FLAG_EXACT_NAV != 0 {
                    return Ok(None);
                }
                let start = rec + 10 + self.dim_fixed * 4;
                let raw = &bytes[start..start + self.dim_fixed];
                out.clear();
                // SAFETY: all i8 bit patterns are valid; this borrow stays pinned.
                out.extend_from_slice(unsafe {
                    std::slice::from_raw_parts(raw.as_ptr().cast::<i8>(), raw.len())
                });
                Ok(Some(read_u32(bytes, start + self.dim_fixed) as i32))
            },
        )
    }

    /// Read `vid`'s f32 vector into `out` plus its `(row_id, deleted)` metadata,
    /// borrowing the pinned page and skipping the per-layer neighbour decode +
    /// `NodeView` allocation. Returns `None` for a missing / uninitialised slot.
    /// The k-NN re-rank hot path (exact f32 distance + tombstone filter).
    pub(crate) fn read_vector_meta_into(
        &self,
        vid: VecId,
        out: &mut Vec<f32>,
    ) -> Result<Option<(RowId, bool)>, QueryError> {
        let dim = self.dim_fixed;
        let page_id = vid_page(vid);
        if page_id == VEC_META_PAGE {
            return Ok(None);
        }
        self.buffer_pool
            .with_raw_page(self.page_key(page_id), |bytes| {
                if read_u32(bytes, 0) != VEC_NODE_MAGIC {
                    return Ok(None);
                }
                let slot = vid_slot(vid);
                let Ok(rec_off) = node_record_offset(bytes, slot) else {
                    return Ok(None);
                };
                let flags = bytes[rec_off];
                if flags & REC_FLAG_INITIALIZED == 0 {
                    return Ok(None);
                }
                if !node_page_read_vector_into(bytes, slot, dim, out) {
                    return Ok(None);
                }
                let row_id = vec_unpack_row(read_u64(bytes, rec_off + 2));
                let deleted = flags & REC_FLAG_TOMBSTONE != 0;
                Ok(Some((row_id, deleted)))
            })
    }

    /// Score a node's exact vector while its page remains pinned, returning
    /// row metadata with the distance. This is the allocation- and copy-free
    /// paged HNSW re-rank path; malformed/missing slots return `None` exactly
    /// like [`Self::read_vector_meta_into`].
    pub(crate) fn read_vector_meta_distance(
        &self,
        vid: VecId,
        query: &[f32],
        metric: crate::engine::vector::Metric,
    ) -> Result<Option<(RowId, bool, f64)>, QueryError> {
        let dim = self.dim_fixed;
        let page_id = vid_page(vid);
        if page_id == VEC_META_PAGE {
            return Ok(None);
        }
        self.buffer_pool
            .with_raw_page(self.page_key(page_id), |bytes| {
                if read_u32(bytes, 0) != VEC_NODE_MAGIC {
                    return Ok(None);
                }
                let slot = vid_slot(vid);
                let Ok(rec_off) = node_record_offset(bytes, slot) else {
                    return Ok(None);
                };
                let flags = bytes[rec_off];
                if flags & REC_FLAG_INITIALIZED == 0 {
                    return Ok(None);
                }
                let vector_off = rec_off + 10;
                let vector_end = vector_off.saturating_add(dim.saturating_mul(4));
                if vector_end > bytes.len() {
                    return Ok(None);
                }
                let distance = crate::engine::vector::distance::ordering_distance_f32_bytes(
                    query,
                    &bytes[vector_off..vector_end],
                    metric,
                );
                let row_id = vec_unpack_row(read_u64(bytes, rec_off + 2));
                let deleted = flags & REC_FLAG_TOMBSTONE != 0;
                Ok(Some((row_id, deleted, distance)))
            })
    }

    /// Batch SQ8 probes by data-buffer page. `order` and `out` are caller-owned
    /// query scratch so repeated HNSW expansions reuse their allocations.
    pub(crate) fn navigation_distances(
        &self,
        vids: &[VecId],
        qcodes: &[i8],
        qnorm: i32,
        order: &mut Vec<usize>,
        out: &mut Vec<Option<f64>>,
    ) -> Result<(), QueryError> {
        order.clear();
        out.clear();
        out.resize(vids.len(), None);
        let Some((_, _, dist_mul)) = self.navigation_quantization() else {
            return Ok(());
        };

        // HNSW expansion batches are normally one adjacency row (<= m0, 32
        // by default). With four high-dimensional records per page, random
        // graph neighbours almost never share a page, so sorting an index
        // vector on every expansion costs more than it coalesces. Keep the
        // grouped path for genuinely large batches/build helpers, and score
        // ordinary query batches directly in their existing order.
        if vids.len() <= 64 {
            for (index, &vid) in vids.iter().enumerate() {
                let page_id = vid_page(vid);
                if page_id == VEC_META_PAGE {
                    continue;
                }
                self.buffer_pool.with_raw_page_intent(
                    self.page_key(page_id),
                    AccessIntent::Probe,
                    |bytes| {
                        if read_u32(bytes, 0) == VEC_NODE_MAGIC {
                            out[index] = node_page_navigation_ssd(
                                bytes,
                                vid_slot(vid),
                                self.dim_fixed,
                                qcodes,
                                qnorm,
                            )?
                            .map(|ssd| ssd as f64 * dist_mul as f64);
                        }
                        Ok(())
                    },
                )?;
            }
            return Ok(());
        }

        order.extend(0..vids.len());
        order.sort_unstable_by_key(|&index| vid_page(vids[index]));

        let mut begin = 0usize;
        while begin < order.len() {
            let page_id = vid_page(vids[order[begin]]);
            let mut end = begin + 1;
            while end < order.len() && vid_page(vids[order[end]]) == page_id {
                end += 1;
            }
            if page_id != VEC_META_PAGE {
                self.buffer_pool.with_raw_page_intent(
                    self.page_key(page_id),
                    AccessIntent::Probe,
                    |bytes| {
                        if read_u32(bytes, 0) != VEC_NODE_MAGIC {
                            return Ok(());
                        }
                        for &index in &order[begin..end] {
                            out[index] = node_page_navigation_ssd(
                                bytes,
                                vid_slot(vids[index]),
                                self.dim_fixed,
                                qcodes,
                                qnorm,
                            )?
                            .map(|ssd| ssd as f64 * dist_mul as f64);
                        }
                        Ok(())
                    },
                )?;
            }
            begin = end;
        }
        Ok(())
    }

    /// Walk the persistent SQ8 navigation rows and layer-0 adjacency in file
    /// order. The callback borrows the pinned page, so building a derived
    /// read-only query accelerator needs one buffer-pool lookup per node page,
    /// not one lookup per node. Exact-navigation rows are rejected by the
    /// caller before this scan via `meta().nav_exact_nodes`.
    pub(crate) fn for_each_navigation_row(
        &self,
        mut f: impl FnMut(VecId, RowId, bool, &[u8], &[i8], i32, &[NodeId]) -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        let (dim, m, m0) = (self.dim_fixed, self.m_fixed, self.m0_fixed);
        let next = self.meta_state.lock().meta.node_next_page_id;
        let mut neighbors = Vec::<NodeId>::with_capacity(m0);
        for page_id in 1..next {
            self.buffer_pool
                .with_raw_page_intent(self.page_key(page_id), AccessIntent::Probe, |bytes| {
                    if read_u32(bytes, 0) != VEC_NODE_MAGIC {
                        return Ok(());
                    }
                    let slot_count = read_u16(bytes, NODE_OFF_SLOT_COUNT);
                    for slot in 0..slot_count {
                        let rec_off = node_record_offset(bytes, slot)?;
                        let flags = bytes[rec_off];
                        if flags & REC_FLAG_INITIALIZED == 0 {
                            continue;
                        }
                        if flags & REC_FLAG_EXACT_NAV != 0 {
                            return Err(error(
                                crate::error::catalog::ER_GET_ERRNO,
                                "vector index: exact row encountered while building SQ8 navigation accelerator",
                            ));
                        }
                        let code_off = rec_off + 10 + dim * 4;
                        let vector_bytes = &bytes[rec_off + 10..code_off];
                        let row_id = vec_unpack_row(read_u64(bytes, rec_off + 2));
                        let deleted = flags & REC_FLAG_TOMBSTONE != 0;
                        let norm = read_u32(bytes, code_off + dim) as i32;
                        // i8 and u8 have identical layout and alignment. The
                        // slice cannot outlive this pinned-page callback.
                        let codes = unsafe {
                            std::slice::from_raw_parts(bytes[code_off..code_off + dim].as_ptr().cast::<i8>(), dim)
                        };
                        neighbors.clear();
                        let count_off = layer_count_offset(rec_off, 0, dim, m, m0);
                        let count = (read_u16(bytes, count_off) as usize).min(m0);
                        let ids_off = count_off + 2;
                        for index in 0..count {
                            neighbors.push(read_u32(bytes, ids_off + index * 4));
                        }
                        f(
                            vid_make(page_id, slot),
                            row_id,
                            deleted,
                            vector_bytes,
                            codes,
                            norm,
                            &neighbors,
                        )?;
                    }
                    Ok(())
                })?;
        }
        Ok(())
    }

    /// Read only `vid`'s layer-`level` neighbour list into `out`, borrowing the
    /// pinned page. Returns `false` for a missing node / level. The hot path
    /// for graph traversal.
    pub(crate) fn read_neighbors_into(
        &self,
        vid: VecId,
        level: usize,
        out: &mut Vec<VecId>,
    ) -> Result<bool, QueryError> {
        let (dim, m, m0) = (self.dim_fixed, self.m_fixed, self.m0_fixed);
        let page_id = vid_page(vid);
        if page_id == VEC_META_PAGE {
            return Ok(false);
        }
        self.buffer_pool
            .with_raw_page(self.page_key(page_id), |bytes| {
                if read_u32(bytes, 0) != VEC_NODE_MAGIC {
                    return Ok(false);
                }
                Ok(node_page_read_neighbors_into(
                    bytes,
                    vid_slot(vid),
                    level,
                    dim,
                    m,
                    m0,
                    out,
                ))
            })
    }

    /// Submit arbitrary node pages to the buffer pool's asynchronous prefetch
    /// queue. HNSW traversal calls this once for a neighbour batch before
    /// scoring it, overlapping random page reads with SQ8 distance work.
    pub(crate) fn prefetch_nodes(&self, vids: &[VecId]) {
        self.buffer_pool.prefetch_pages(
            vids.iter()
                .copied()
                .map(vid_page)
                .filter(|&page_id| page_id != VEC_META_PAGE)
                .map(|page_id| self.page_key(page_id)),
        );
    }

    /// Read-modify-write one node page. In bulk-build (deferred-WAL) mode the
    /// page is edited **in place** in the buffer pool — no 16 KiB clone, no
    /// `store_page` swap, no WAL — and its id recorded for `finalize_build`.
    /// Otherwise it goes through the WAL'd clone → mutate → `write_page` path.
    /// The closure returns `(result, mutated)`; `mutated == false` skips the
    /// page write entirely (e.g. a `push_neighbor` that found the layer full).
    fn rmw_node_page<T>(
        &self,
        txn_id: u64,
        page_id: u64,
        f: impl FnOnce(&mut [u8]) -> Result<(T, bool), QueryError>,
    ) -> Result<T, QueryError> {
        let _mapping_write = self.navigation_write();
        if self.build_mode.load(Ordering::Acquire) {
            // In-place edit under the frame write lock; `finalize_build` emits
            // the deferred WAL by re-scanning the node-page range, so no
            // per-edit `touched_pages` tracking (and its global mutex) is needed.
            let (result, _mutated) =
                self.buffer_pool
                    .with_raw_page_mut(self.page_key(page_id), 0, f)?;
            Ok(result)
        } else {
            let mut bytes = self.read_page_bytes(page_id)?;
            let (result, mutated) = f(&mut bytes)?;
            if mutated {
                self.write_page(txn_id, page_id, bytes)?;
            }
            Ok(result)
        }
    }

    /// Append a neighbour edge `vid -> nb` at `level`, in place. Returns
    /// `Ok(false)` if the reserved layer capacity is full (caller prunes).
    pub(crate) fn push_neighbor(
        &self,
        txn_id: u64,
        vid: VecId,
        level: usize,
        nb: VecId,
    ) -> Result<bool, QueryError> {
        let (dim, m, m0) = (self.dim_fixed, self.m_fixed, self.m0_fixed);
        let page_id = vid_page(vid);
        let _latch = self.page_latch(page_id);
        self.rmw_node_page(txn_id, page_id, |bytes| {
            let ok = node_page_push_neighbor(bytes, vid_slot(vid), level, nb, dim, m, m0).is_some();
            Ok((ok, ok))
        })
    }

    /// Overwrite `vid`'s layer `level` neighbour list (prune path), in place.
    pub(crate) fn set_neighbors(
        &self,
        txn_id: u64,
        vid: VecId,
        level: usize,
        nbrs: &[VecId],
    ) -> Result<(), QueryError> {
        let (dim, m, m0) = (self.dim_fixed, self.m_fixed, self.m0_fixed);
        let page_id = vid_page(vid);
        let _latch = self.page_latch(page_id);
        self.rmw_node_page(txn_id, page_id, |bytes| {
            node_page_set_neighbors(bytes, vid_slot(vid), level, nbrs, dim, m, m0)?;
            Ok(((), true))
        })
    }

    /// Bulk-build variant: overwrite all adjacency layers of one node under a
    /// single page latch / RMW. Transcription otherwise cloned or re-locked the
    /// same 16 KiB page once per layer.
    #[allow(dead_code)]
    pub(crate) fn set_all_neighbors(
        &self,
        txn_id: u64,
        vid: VecId,
        layers: &[Vec<VecId>],
    ) -> Result<(), QueryError> {
        let (dim, m, m0) = (self.dim_fixed, self.m_fixed, self.m0_fixed);
        let page_id = vid_page(vid);
        let _latch = self.page_latch(page_id);
        self.rmw_node_page(txn_id, page_id, |bytes| {
            for (level, nbrs) in layers.iter().enumerate() {
                node_page_set_neighbors(bytes, vid_slot(vid), level, nbrs, dim, m, m0)?;
            }
            Ok(((), true))
        })
    }

    /// Overwrite `vid`'s f32 vector in place (WAL'd), keeping its id, level,
    /// neighbours and tombstone flag. Returns `Ok(false)` for a missing node.
    /// The drift-adaptive in-place UPDATE path (see
    /// [`PagedHnsw::try_update_in_place`]).
    pub(crate) fn write_vector(
        &self,
        txn_id: u64,
        vid: VecId,
        vector: &[f32],
    ) -> Result<bool, QueryError> {
        let dim = self.dim_fixed;
        if vector.len() != dim {
            return Err(error(
                crate::error::catalog::MYDB_ER_INVALID_VECTOR_VALUE,
                format!(
                    "vector index dimension mismatch: expected {dim}, got {}",
                    vector.len()
                ),
            ));
        }

        let nav_quant = self.persisted_nav_quant();
        let page_id = vid_page(vid);
        let latch = self.page_latch(page_id);
        let (ok, old_exact, new_exact) = self.rmw_node_page(txn_id, page_id, |bytes| {
            let result = node_page_write_vector_into(bytes, vid_slot(vid), dim, vector, nav_quant);
            Ok((result, result.0))
        })?;
        drop(latch);
        if ok && old_exact != new_exact {
            {
                let mut state = self.meta_state.lock();
                if new_exact {
                    state.meta.nav_exact_nodes = state.meta.nav_exact_nodes.saturating_add(1);
                } else {
                    state.meta.nav_exact_nodes = state.meta.nav_exact_nodes.saturating_sub(1);
                }
            }
            self.write_meta_page(txn_id)?;
        }
        Ok(ok)
    }

    /// Set/clear a node's tombstone. Returns the previous tombstone state.
    pub(crate) fn set_tombstone(
        &self,
        txn_id: u64,
        vid: VecId,
        deleted: bool,
    ) -> Result<bool, QueryError> {
        let page_id = vid_page(vid);
        let _latch = self.page_latch(page_id);
        let prev = self.rmw_node_page(txn_id, page_id, |bytes| {
            let prev = node_page_set_tombstone(bytes, vid_slot(vid), deleted)?;
            Ok((prev, true))
        })?;
        if prev != deleted {
            {
                let mut state = self.meta_state.lock();
                if deleted {
                    state.meta.tombstone_count = state.meta.tombstone_count.saturating_add(1);
                } else {
                    state.meta.tombstone_count = state.meta.tombstone_count.saturating_sub(1);
                }
            }
            self.write_meta_page(txn_id)?;
        }
        Ok(prev)
    }

    pub(crate) fn validate_hnsw_layout(dim: usize, m: usize) -> Result<(), QueryError> {
        let maximum = crate::engine::vector::hnsw_paged::VEC_MAX_LAYER;
        if node_record_size(maximum, dim, m, m.saturating_mul(2)).saturating_add(2)
            > PAGE_SIZE - NODE_OFF_DIRECTORY
        {
            return Err(error(crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("paged HNSW VECTOR({dim}) with M={m} exceeds node page capacity at the supported maximum layer; use the memory backend or a smaller dimension/M")));
        }
        Ok(())
    }

    /// Claim an empty entry atomically; return the entry observed by a peer.
    pub(crate) fn claim_entry(
        &self,
        txn_id: u64,
        id: VecId,
        level: u32,
    ) -> Result<Option<(VecId, u32)>, QueryError> {
        {
            let mut state = self.meta_state.lock();
            if state.meta.entry_vid != VEC_NO_VID {
                return Ok(Some((state.meta.entry_vid, state.meta.max_layer)));
            }
            state.meta.entry_vid = id;
            state.meta.max_layer = level;
        }
        self.write_meta_page(txn_id)?;
        Ok(None)
    }

    /// Concurrent higher-level publications can only increase the top layer.
    pub(crate) fn raise_entry(&self, txn_id: u64, id: VecId, level: u32) -> Result<(), QueryError> {
        {
            let mut state = self.meta_state.lock();
            if state.meta.entry_vid != VEC_NO_VID && level <= state.meta.max_layer {
                return Ok(());
            }
            state.meta.entry_vid = id;
            state.meta.max_layer = level;
        }
        self.write_meta_page(txn_id)
    }

    pub(crate) fn advance_coverage(&self, scn: u64) {
        let mut state = self.meta_state.lock();
        state.meta.coverage_scn = state.meta.coverage_scn.max(scn);
    }

    pub(crate) fn mark_delta_finalized(&self, txn_id: u64) -> Result<(), QueryError> {
        {
            let mut state = self.meta_state.lock();
            state.meta.finalized = true;
            state.meta.query_ready = true;
        }
        self.write_meta_page(txn_id)
    }

    /// Publish the entry point + max layer (meta page), persisted via WAL.
    pub(crate) fn set_entry(
        &self,
        txn_id: u64,
        entry_vid: VecId,
        max_layer: u32,
    ) -> Result<(), QueryError> {
        {
            let mut state = self.meta_state.lock();
            state.meta.entry_vid = entry_vid;
            state.meta.max_layer = max_layer;
        }
        self.write_meta_page(txn_id)
    }

    /// Persist the in-memory meta (node_count etc.) to the meta page. Used by
    /// the storage-level tests; the engine path persists meta inline on every
    /// structural change via `write_meta_page`.
    #[allow(dead_code)]
    pub(crate) fn flush_meta(&self, txn_id: u64) -> Result<(), QueryError> {
        self.write_meta_page(txn_id)
    }

    /// Enter bulk-build (deferred-WAL) mode: subsequent `write_page` calls
    /// dirty pages in the pool without per-edit WAL, recording touched pages
    /// for `finalize_build`. Caller MUST pair this with `finalize_build` on
    /// success; on an aborted build the file stays non-finalized and is
    /// rebuilt on next open.
    pub(crate) fn begin_build(&self) {
        self.touched_pages.lock().clear();
        self.build_mode.store(true, Ordering::Release);
    }

    /// Leave bulk-build mode and publish the graph. Node pages are build-local
    /// derived data until the meta page says `finalized = true`, so the fast
    /// path writes them directly to the `.hvec` file (and fsyncs in strict
    /// durability) with refreshed checksums, then WAL-logs only the finalized
    /// meta page as the commit point. A crash before that meta record is
    /// durable leaves `finalized = false` and the index is rebuilt from the
    /// heap; after it is durable, the node pages have already reached the data
    /// file. `MYDB_HNSW_BULK_WAL_FINALIZE=1` restores the older conservative
    /// "one PageUpdate WAL record per node page" path as a differential oracle.
    pub(crate) fn finalize_build(&self, txn_id: u64) -> Result<(), QueryError> {
        let _mapping_write = self.navigation_write();
        let timing = std::env::var("MYDB_HNSW_BUILD_TIMING").is_ok();
        let started = Instant::now();
        self.build_mode.store(false, Ordering::Release);
        let next = self.meta_state.lock().meta.node_next_page_id;
        self.touched_pages.lock().clear();

        let wal_finalize = matches!(
            std::env::var("MYDB_HNSW_BULK_WAL_FINALIZE").as_deref(),
            Ok("1") | Ok("true") | Ok("TRUE") | Ok("on") | Ok("ON")
        );
        if wal_finalize {
            // Legacy oracle: emit one physical redo record per node page. This
            // is correct but expensive for high-dimensional vectors because it
            // duplicates the whole `.hvec` image into WAL during CREATE INDEX.
            for page_id in 1..next {
                let bytes = self.read_page_bytes(page_id)?;
                if read_u32(&bytes, 0) != VEC_NODE_MAGIC {
                    continue; // freelist / uninitialized page — nothing to redo
                }
                // The before-image is irrelevant for these redo-only
                // (`undo_lsn = None`) records, so mark each page pristine to skip
                // the before-image disk read; redo applies `after` by page LSN.
                self.meta_state.lock().pristine_pages.insert(page_id);
                self.write_page(txn_id, page_id, bytes)?;
            }
        } else {
            // Refresh checksums once per allocated node page, then flush the
            // dirty `.hvec` frames straight to the data file. The page LSN stays
            // 0: node pages are made durable by the pre-commit flush, not by
            // redo. The finalized meta page below is the only logged commit
            // record for the bulk-built graph.
            for page_id in 1..next {
                self.buffer_pool
                    .with_raw_page_mut(self.page_key(page_id), 0, |bytes| {
                        if read_u32(bytes, 0) == VEC_NODE_MAGIC {
                            refresh_page_checksum(bytes);
                        }
                        Ok(())
                    })?;
            }
            self.buffer_pool.flush_file(&self.file_key)?;
        }
        {
            let mut state = self.meta_state.lock();
            state.meta.finalized = true;
            state.meta.built_lsn = self.log_atomics.next_lsn();
        }
        self.write_meta_page(txn_id)?;
        if timing {
            eprintln!(
                "[paged] finalize_build: {:?} pages={} mode={}",
                started.elapsed(),
                next.saturating_sub(1),
                if wal_finalize { "wal" } else { "direct" }
            );
        }
        Ok(())
    }

    /// Durably withdraw trust in this graph after a maintenance write failed:
    /// the next open sees a non-finalized file and rebuilds it from the heap
    /// instead of serving a graph that silently lacks committed rows.
    pub(crate) fn invalidate_build(&self, txn_id: u64) -> Result<(), QueryError> {
        {
            let mut state = self.meta_state.lock();
            if !state.meta.finalized {
                return Ok(());
            }
            state.meta.finalized = false;
            state.meta.query_ready = false;
        }
        self.write_meta_page(txn_id)
    }

    /// Whether this file holds a fully-built, durably-finalized graph. A
    /// non-finalized non-empty file is an interrupted build and must be
    /// rebuilt from the heap rather than trusted.
    pub(crate) fn is_finalized(&self) -> bool {
        self.meta_state.lock().meta.finalized
    }

    /// Iterate every initialized node in the file, invoking `f(vid, row_id,
    /// deleted)`. Walks pages `[1, node_next_page_id)` (page 0 is meta),
    /// skipping non-node pages (freelist). Used to (re)build the in-memory
    /// `row_id -> vid` reverse index that the paged backend needs for
    /// DELETE/UPDATE tombstoning — a single sequential pass whose pages are
    /// evicted normally under the buffer pool, so it does not pin the whole
    /// index in memory.
    pub(crate) fn for_each_node(
        &self,
        mut f: impl FnMut(VecId, RowId, bool),
    ) -> Result<(), QueryError> {
        let next = self.meta_state.lock().meta.node_next_page_id;
        for page_id in 1..next {
            // Reverse-index construction needs only the ten-byte header.
            // Borrow one page instead of cloning it and allocating/decoding
            // every vector and every adjacency list in the entire graph.
            self.buffer_pool
                .with_raw_page(self.page_key(page_id), |bytes| {
                    if read_u32(bytes, 0) != VEC_NODE_MAGIC {
                        return Ok(());
                    }
                    let slot_count = read_u16(bytes, NODE_OFF_SLOT_COUNT);
                    for slot in 0..slot_count {
                        let Ok(offset) = node_record_offset(bytes, slot) else {
                            continue;
                        };
                        let flags = bytes[offset];
                        if flags & REC_FLAG_INITIALIZED != 0 {
                            f(
                                vid_make(page_id, slot),
                                vec_unpack_row(read_u64(bytes, offset + 2)),
                                flags & REC_FLAG_TOMBSTONE != 0,
                            );
                        }
                    }
                    Ok(())
                })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod vector_page_tests {
    use super::*;

    fn no_nav_quant() -> PersistedNavQuant {
        PersistedNavQuant {
            valid: false,
            metric: 0,
            mid: 0.0,
            inv_scale: 1.0,
        }
    }

    #[test]
    fn node_record_size_reserves_per_layer_footprint() {
        let (dim, m, m0) = (8usize, 4usize, 8usize);
        // layer-0-only node: header(10) + vec(32) + sq8(8) + norm(4)
        // + count(2) + m0*4-byte NodeId
        assert_eq!(
            node_record_size(0, dim, m, m0),
            10 + 32 + dim + 4 + 2 + 8 * 4
        );
        // two-layer node adds count(2) + m*4 for layer 1
        assert_eq!(
            node_record_size(1, dim, m, m0),
            10 + 32 + dim + 4 + (2 + 8 * 4) + (2 + 4 * 4)
        );
    }

    #[test]
    fn node_page_append_read_round_trips() {
        let (dim, m, m0) = (4usize, 4usize, 8usize);
        let mut page = vec![0u8; PAGE_SIZE];
        format_empty_node_page(&mut page);
        let row = RowId {
            page_id: 7,
            slot_id: 3,
        };
        let vec = vec![1.0f32, 2.0, 3.0, 4.0];
        let slot = node_page_append(&mut page, row, 1, &vec, dim, m, m0, no_nav_quant()).unwrap();
        assert_eq!(slot, 0);
        let view = node_page_read(&page, slot, dim, m, m0).unwrap();
        assert_eq!(view.row_id, row);
        assert_eq!(view.top_layer, 1);
        assert!(!view.deleted);
        assert_eq!(view.vector, vec);
        assert_eq!(view.neighbors.len(), 2);
        assert!(view.neighbors[0].is_empty());
    }

    #[test]
    fn persisted_navigation_row_scores_and_falls_back_exact() {
        let (dim, m, m0) = (2usize, 2usize, 4usize);
        let quant = PersistedNavQuant {
            valid: true,
            metric: 0,
            mid: 0.0,
            inv_scale: 100.0,
        };
        let mut page = vec![0u8; PAGE_SIZE];
        format_empty_node_page(&mut page);
        let in_range = node_page_append(
            &mut page,
            RowId {
                page_id: 1,
                slot_id: 0,
            },
            0,
            &[0.5, -0.5],
            dim,
            m,
            m0,
            quant,
        )
        .unwrap();
        let qcodes = [50i8, -50i8];
        assert_eq!(
            node_page_navigation_ssd(&page, in_range, dim, &qcodes, 5_000).unwrap(),
            Some(0)
        );

        let out_of_range = node_page_append(
            &mut page,
            RowId {
                page_id: 2,
                slot_id: 0,
            },
            0,
            &[2.0, -2.0],
            dim,
            m,
            m0,
            quant,
        )
        .unwrap();
        assert_eq!(
            node_page_navigation_ssd(&page, out_of_range, dim, &qcodes, 5_000).unwrap(),
            None,
            "Nodes outside global SQ8 bounds must use exact navigation"
        );

        assert_eq!(
            node_page_write_vector_into(&mut page, out_of_range, dim, &[0.25, -0.25], quant,),
            (true, true, false),
            "An in-place UPDATE back within quantization bounds must clear exact_nav"
        );
        assert!(
            node_page_navigation_ssd(&page, out_of_range, dim, &[25, -25], 1_250)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn neighbor_push_set_and_tombstone_in_place() {
        let (dim, m, m0) = (2usize, 2usize, 4usize);
        let mut page = vec![0u8; PAGE_SIZE];
        format_empty_node_page(&mut page);
        let slot = node_page_append(
            &mut page,
            RowId {
                page_id: 1,
                slot_id: 1,
            },
            0,
            &[0.5, 0.25],
            dim,
            m,
            m0,
            no_nav_quant(),
        )
        .unwrap();
        // push up to m0 (cap at layer 0)
        for i in 0..4u64 {
            assert!(node_page_push_neighbor(
                &mut page,
                slot,
                0,
                vid_make(10, i as u16),
                dim,
                m,
                m0
            )
            .is_some());
        }
        // capacity reached
        assert!(
            node_page_push_neighbor(&mut page, slot, 0, vid_make(10, 99), dim, m, m0).is_none()
        );
        let view = node_page_read(&page, slot, dim, m, m0).unwrap();
        assert_eq!(view.neighbors[0].len(), 4);
        assert_eq!(view.neighbors[0][0], vid_make(10, 0));
        // prune to 2 via set
        node_page_set_neighbors(
            &mut page,
            slot,
            0,
            &[vid_make(20, 0), vid_make(20, 1)],
            dim,
            m,
            m0,
        )
        .unwrap();
        let view = node_page_read(&page, slot, dim, m, m0).unwrap();
        assert_eq!(view.neighbors[0], vec![vid_make(20, 0), vid_make(20, 1)]);
        // tombstone toggles
        assert!(!node_page_set_tombstone(&mut page, slot, true).unwrap());
        assert!(node_page_read(&page, slot, dim, m, m0).unwrap().deleted);
    }

    #[test]
    fn node_page_append_fills_then_rejects() {
        let (dim, m, m0) = (2usize, 4usize, 8usize);
        let mut page = vec![0u8; PAGE_SIZE];
        format_empty_node_page(&mut page);
        let mut count = 0;
        loop {
            let r = node_page_append(
                &mut page,
                RowId {
                    page_id: 0,
                    slot_id: count as u16,
                },
                0,
                &[1.0, 1.0],
                dim,
                m,
                m0,
                no_nav_quant(),
            );
            if r.is_none() {
                break;
            }
            count += 1;
        }
        assert!(count > 0);
        // every appended slot reads back
        for s in 0..count {
            assert!(node_page_read(&page, s as u16, dim, m, m0).is_some());
        }
    }

    #[test]
    fn vid_packing_round_trips() {
        let vid = vid_make(0x1234_5678, 0xABCD);
        assert_eq!(vid_page(vid), 0x1234_5678);
        assert_eq!(vid_slot(vid), 0xABCD);
    }

    #[test]
    fn compact_node_id_round_trips_page_and_slot() {
        let vid = vid_make(0x00AB_CDEF, 0x7F);
        let node_id = node_id_from_vid(vid).unwrap();
        assert_eq!(node_id_to_vid(node_id), vid);
        assert!(node_id_from_vid(vid_make(NODE_ID_MAX_PAGE + 1, 0)).is_err());
        assert!(node_id_from_vid(vid_make(1, NODE_ID_SLOTS_PER_PAGE)).is_err());
    }

    fn test_pool(log: &Arc<crate::wal::LogMutex>) -> Arc<BufferPool> {
        BufferPool::new_with_options(
            DEFAULT_BUFFER_POOL_PAGES,
            log.clone(),
            DurabilityMode::Strict,
            std::time::Duration::from_millis(DEFAULT_LOCK_WAIT_MS),
            1,
            0,
        )
    }

    #[test]
    fn append_read_persists_through_buffer_pool_and_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("vec__1__idx.hvec");
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let (dim, m, m0) = (4u32, 4u16, 8u16);
        let vids;
        let vecs: Vec<Vec<f32>> = (0..300)
            .map(|i| vec![i as f32, (i * 2) as f32, (i * 3) as f32, (i * 4) as f32])
            .collect();
        {
            let pool = test_pool(&log);
            let f = VectorIndexFile::open_with_durability(
                path.clone(),
                log.clone(),
                DurabilityMode::Strict,
                pool.clone(),
                dim,
                0,
                m,
                m0,
                200,
            )
            .unwrap();
            let mut got = Vec::new();
            for (i, v) in vecs.iter().enumerate() {
                let row = RowId {
                    page_id: i as u32,
                    slot_id: 0,
                };
                let vid = f.append_node(txn_id_for_ddl(), row, 0, v).unwrap();
                got.push(vid);
            }
            // wire a couple of edges + an entry point, exercising in-place ops
            f.push_neighbor(txn_id_for_ddl(), got[0], 0, got[1])
                .unwrap();
            f.push_neighbor(txn_id_for_ddl(), got[0], 0, got[2])
                .unwrap();
            f.set_entry(txn_id_for_ddl(), got[0], 0).unwrap();
            f.flush_meta(txn_id_for_ddl()).unwrap();
            vids = got;
            // Force every dirty page out to disk, then drop the pool.
            pool.flush_all().unwrap();
        }
        // Reopen with a FRESH buffer pool — all reads come from disk.
        {
            let pool = test_pool(&log);
            let f = VectorIndexFile::open_with_durability(
                path.clone(),
                log.clone(),
                DurabilityMode::Strict,
                pool,
                dim,
                0,
                m,
                m0,
                200,
            )
            .unwrap();
            let meta = f.meta();
            assert_eq!(meta.entry_vid, vids[0]);
            // Spans more than one compact v3 node page — proves multi-page IO.
            assert!(meta.node_next_page_id > 2, "expected multiple node pages");
            for (i, &vid) in vids.iter().enumerate() {
                let view = f
                    .read_node(vid)
                    .unwrap()
                    .expect("node present after reopen");
                assert_eq!(view.row_id.page_id, i as u32);
                assert_eq!(view.vector, vecs[i]);
            }
            let head = f.read_node(vids[0]).unwrap().unwrap();
            assert_eq!(head.neighbors[0], vec![vids[1], vids[2]]);
        }
    }

    #[test]
    fn concurrent_appenders_share_the_current_fill_page() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("vec__1__append_race.hvec");
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool = test_pool(&log);
        let file = Arc::new(
            VectorIndexFile::open_with_durability(
                path,
                log,
                DurabilityMode::Strict,
                pool,
                2,
                0,
                4,
                8,
                32,
            )
            .unwrap(),
        );
        let threads = 8usize;
        let per_thread = 80usize;
        let start = Arc::new(std::sync::Barrier::new(threads));
        let mut handles = Vec::new();
        for worker in 0..threads {
            let file = Arc::clone(&file);
            let start = Arc::clone(&start);
            handles.push(std::thread::spawn(move || {
                start.wait();
                for i in 0..per_thread {
                    let ordinal = worker * per_thread + i;
                    file.append_node(
                        txn_id_for_ddl(),
                        RowId {
                            page_id: ordinal as u32,
                            slot_id: 0,
                        },
                        0,
                        &[ordinal as f32, 1.0],
                    )
                    .unwrap();
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }

        let total = threads * per_thread;
        assert_eq!(file.node_count(), total as u64);
        let mut page = vec![0u8; PAGE_SIZE];
        format_empty_node_page(&mut page);
        let mut per_page = 0usize;
        while node_page_append(
            &mut page,
            RowId {
                page_id: per_page as u32,
                slot_id: 0,
            },
            0,
            &[0.0, 1.0],
            2,
            4,
            8,
            no_nav_quant(),
        )
        .is_some()
        {
            per_page += 1;
        }
        let expected_pages = total.div_ceil(per_page);
        assert_eq!(
            file.meta().node_next_page_id as usize - 1,
            expected_pages,
            "the full-page transition must allocate exactly one replacement page"
        );
    }

    fn gen_vectors(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
        let mut s = seed;
        let mut next = || {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((s >> 33) as f32 / (1u32 << 31) as f32) - 0.5
        };
        (0..n).map(|_| (0..dim).map(|_| next()).collect()).collect()
    }

    /// Clustered vectors approximating real embedding workloads: `clusters`
    /// random centroids, each point = a centroid + small gaussian-ish noise, so
    /// the graph has navigable structure and recall is representative (unlike
    /// i.i.d. uniform, which collapses to near-equidistant at high dim). Query
    /// `seed` drift keeps queries near — but not on — centroids.
    #[cfg(test)]
    fn gen_clustered(n: usize, dim: usize, clusters: usize, seed: u64) -> Vec<Vec<f32>> {
        let centroids = gen_vectors(clusters.max(1), dim, seed ^ 0xC0FFEE);
        let mut s = seed;
        let mut next = || {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((s >> 33) as f32 / (1u32 << 31) as f32) - 0.5
        };
        (0..n)
            .map(|i| {
                let c = &centroids[i % centroids.len()];
                // Tight, well-separated clusters → navigable graph structure.
                c.iter().map(|&x| x + next() * 0.1).collect()
            })
            .collect()
    }

    fn brute_knn(
        vectors: &[Vec<f32>],
        query: &[f32],
        k: usize,
        metric: crate::engine::vector::distance::Metric,
    ) -> Vec<usize> {
        let mut scored: Vec<(f64, usize)> = vectors
            .iter()
            .enumerate()
            .map(|(i, v)| {
                (
                    crate::engine::vector::distance::ordering_distance(query, v, metric),
                    i,
                )
            })
            .collect();
        scored.sort_by(|a, b| a.0.total_cmp(&b.0));
        scored.into_iter().take(k).map(|(_, i)| i).collect()
    }

    fn test_navigation_quantization(vectors: &[Vec<f32>], metric: u8) -> Option<(f32, f32, f32)> {
        if vectors.is_empty() || metric == 2 {
            return None;
        }
        let (mut vmin, mut vmax) = (f32::INFINITY, f32::NEG_INFINITY);
        for vector in vectors {
            let inv_norm = navigation_inv_norm(vector, metric);
            for &value in vector {
                let value = (value as f64 * inv_norm) as f32;
                vmin = vmin.min(value);
                vmax = vmax.max(value);
            }
        }
        if !vmin.is_finite() || !vmax.is_finite() || vmax <= vmin {
            return None;
        }
        let mid = (vmin + vmax) * 0.5;
        let inv_scale = 254.0 / (vmax - vmin);
        let scale_sq = inv_scale.recip().powi(2);
        Some((
            mid,
            inv_scale,
            if metric == 1 {
                scale_sq * 0.5
            } else {
                scale_sq
            },
        ))
    }

    fn build_paged(
        dir: &Path,
        name: &str,
        dim: u32,
        metric: u8,
        vecs: &[Vec<f32>],
    ) -> (crate::engine::vector::hnsw_paged::PagedHnsw, Vec<VecId>) {
        build_paged_with_wal_mode(dir, name, dim, metric, vecs, false)
    }

    fn build_paged_with_wal_mode(
        dir: &Path,
        name: &str,
        dim: u32,
        metric: u8,
        vecs: &[Vec<f32>],
        bulk: bool,
    ) -> (crate::engine::vector::hnsw_paged::PagedHnsw, Vec<VecId>) {
        let log = Arc::new(parking_lot::Mutex::new(LogManager::open(dir).unwrap()));
        let pool = BufferPool::new_with_options(
            DEFAULT_BUFFER_POOL_PAGES,
            log.clone(),
            DurabilityMode::Relaxed,
            std::time::Duration::from_millis(DEFAULT_LOCK_WAIT_MS),
            1,
            0,
        );
        let path = dir.join(format!("vec__1__{name}.hvec"));
        let file = Arc::new(
            VectorIndexFile::open_with_durability(
                path,
                log,
                DurabilityMode::Relaxed,
                pool,
                dim,
                metric,
                16,
                32,
                200,
            )
            .unwrap(),
        );
        let g = crate::engine::vector::hnsw_paged::PagedHnsw::new(file);
        g.set_navigation_quantization(test_navigation_quantization(vecs, metric))
            .unwrap();
        if bulk {
            g.begin_bulk_build();
        }
        let mut vids = Vec::new();
        for (i, v) in vecs.iter().enumerate() {
            vids.push(
                g.insert(
                    txn_id_for_ddl(),
                    RowId {
                        page_id: i as u32,
                        slot_id: 0,
                    },
                    v,
                )
                .unwrap(),
            );
        }
        if bulk {
            g.finalize_bulk_build().unwrap();
        }
        (g, vids)
    }

    #[test]
    fn mapped_navigation_matches_paged_and_revokes_on_realtime_dml() {
        for metric in [0, 1, 2] {
            let dir = tempfile::Builder::new()
                .prefix("mapped-nav-")
                .tempdir_in("/tmp/mydb")
                .unwrap();
            let vectors = gen_vectors(96, 16, 1984);
            let (graph, vids) =
                build_paged_with_wal_mode(dir.path(), "mapped", 16, metric, &vectors, true);
            let query = vectors[17].clone();
            let expected = graph.search(&query, 10, 64).unwrap();
            assert!(graph.file().prepare_navigation_mapping().unwrap());
            assert_eq!(graph.search(&query, 10, 64).unwrap(), expected);
            // Exercise an exact-query memo hit before revoking the view.
            assert_eq!(graph.search(&query, 10, 64).unwrap(), expected);
            graph
                .mark_deleted_by_row_id(RowId {
                    page_id: 17,
                    slot_id: 0,
                })
                .unwrap();
            assert!(graph.file().with_navigation_mapping(|_| ()).is_none());
            assert!(!graph
                .search(&query, 96, 128)
                .unwrap()
                .iter()
                .any(|(r, _)| r.page_id == 17));
            graph.file().set_tombstone(0, vids[17], false).unwrap();
            assert!(graph.file().prepare_navigation_mapping().unwrap());
            assert_eq!(graph.search(&query, 10, 64).unwrap(), expected);
            // INSERT must become searchable immediately, without rebuilding a cache.
            let inserted = RowId {
                page_id: 1000,
                slot_id: 0,
            };
            graph.insert(0, inserted, &query).unwrap();
            assert!(graph.file().with_navigation_mapping(|_| ()).is_none());
            assert!(graph
                .search(&query, 97, 128)
                .unwrap()
                .iter()
                .any(|(r, _)| *r == inserted));
            assert!(graph.file().prepare_navigation_mapping().unwrap());
            let replacement = vec![0.125; 16];
            graph
                .file()
                .write_vector(0, vids[17], &replacement)
                .unwrap();
            assert!(graph.file().with_navigation_mapping(|_| ()).is_none());
            let after_update = graph.search(&replacement, 97, 128).unwrap();
            assert!(graph.file().prepare_navigation_mapping().unwrap());
            assert_eq!(graph.search(&replacement, 97, 128).unwrap(), after_update);
            graph.file().disable_navigation_mapping();
            assert!(!graph.file().prepare_navigation_mapping().unwrap());
        }
    }

    #[test]
    fn mapped_navigation_rejects_corrupt_pages_lazily() {
        let dir = tempfile::Builder::new()
            .prefix("mapped-nav-")
            .tempdir_in("/tmp/mydb")
            .unwrap();
        let vectors = gen_vectors(32, 8, 1986);
        let (graph, vids) = build_paged_with_wal_mode(dir.path(), "mapped", 8, 0, &vectors, true);
        graph.file().flush().unwrap();
        // Simulate on-disk damage before first access; never trust unchecked
        // mmap bytes merely because the index meta page was valid at open.
        let file = graph.file().file.lock();
        write_all_at(
            file.as_ref().unwrap(),
            &[0],
            vid_page(vids[0]) * PAGE_SIZE as u64,
        )
        .unwrap();
        drop(file);
        assert!(graph.file().prepare_navigation_mapping().unwrap());
        let result = graph
            .file()
            .with_navigation_mapping(|view| view.page(vids[0]).map(|_| ()));
        assert!(result.unwrap().is_err());
    }

    #[test]
    fn mapped_navigation_writer_drains_readers_and_reopen_needs_no_scan() {
        let dir = tempfile::Builder::new()
            .prefix("mapped-nav-")
            .tempdir_in("/tmp/mydb")
            .unwrap();
        let vectors = gen_vectors(32, 8, 1985);
        let (graph, vids) = build_paged_with_wal_mode(dir.path(), "mapped", 8, 0, &vectors, true);
        assert!(graph.file().prepare_navigation_mapping().unwrap());
        graph
            .file()
            .with_navigation_mapping(|view| {
                assert!(view
                    .verified
                    .iter()
                    .all(|word| word.load(Ordering::Relaxed) == 0));
            })
            .unwrap();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            let read = graph.file().mapped_navigation.read();
            scope.spawn(|| {
                ready_tx.send(()).unwrap();
                graph.file().set_tombstone(0, vids[0], true).unwrap();
                done_tx.send(()).unwrap();
            });
            ready_rx.recv().unwrap();
            assert!(done_rx.recv_timeout(Duration::from_millis(30)).is_err());
            assert!(read.is_some());
            drop(read);
            done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        });
        assert!(graph.file().prepare_navigation_mapping().unwrap());
        let reopened = crate::engine::vector::hnsw_paged::PagedHnsw::new(graph.file().clone());
        assert!(reopened.file().with_navigation_mapping(|_| ()).is_some());
        assert!(!reopened
            .search(&vectors[0], 32, 64)
            .unwrap()
            .iter()
            .any(|(row, _)| row.page_id == 0));
        let tmp = crate::storage::naming::system_tmp_dir(dir.path());
        assert!(std::fs::read_dir(tmp)
            .map(|entries| entries
                .flatten()
                .all(|e| !e.file_name().to_string_lossy().starts_with("vector-nav-")))
            .unwrap_or(true));
    }

    #[test]
    fn paged_insertion_keeps_reverse_edge_headroom() {
        let dir = tempfile::tempdir().unwrap();
        let vectors = gen_vectors(96, 24, 7272);
        let (graph, vids) = build_paged(dir.path(), "fanout", 24, 0, &vectors);
        let node = graph
            .file()
            .read_node(*vids.last().unwrap())
            .unwrap()
            .unwrap();
        assert!(!node.neighbors[0].is_empty() && node.neighbors[0].len() <= 16);
        assert_eq!(graph.file().meta().m0, 32);
    }

    #[test]
    fn paged_recall_matches_brute_force_l2() {
        use crate::engine::vector::distance::Metric;
        let dir = tempfile::tempdir().unwrap();
        let dim = 16;
        let vecs = gen_vectors(96, dim, 12345);
        let (g, _) = build_paged(dir.path(), "l2", dim as u32, 0, &vecs);
        let queries = gen_vectors(3, dim, 999);
        let k = 10;
        let (mut hits, mut total) = (0usize, 0usize);
        for q in &queries {
            let got: Vec<u32> = g
                .search(q, k, 64)
                .unwrap()
                .into_iter()
                .map(|(r, _)| r.page_id)
                .collect();
            for w in brute_knn(&vecs, q, k, Metric::L2) {
                if got.contains(&(w as u32)) {
                    hits += 1;
                }
            }
            total += k;
        }
        let recall = hits as f64 / total as f64;
        assert!(recall >= 0.90, "paged L2 recall too low: {recall}");
    }

    #[test]
    fn navigation_codes_spill_without_changing_results_or_reader_lifetimes() {
        struct Restore(Option<std::ffi::OsString>);
        impl Drop for Restore {
            fn drop(&mut self) {
                if let Some(value) = self.0.take() {
                    std::env::set_var("MYDB_HNSW_QUERY_CACHE_MB", value);
                } else {
                    std::env::remove_var("MYDB_HNSW_QUERY_CACHE_MB");
                }
            }
        }
        let _restore = Restore(std::env::var_os("MYDB_HNSW_QUERY_CACHE_MB"));
        std::env::set_var("MYDB_HNSW_QUERY_CACHE_MB", "5");
        let dir = tempfile::tempdir().unwrap();
        let vectors = gen_vectors(1024, 1024, 721);
        let (graph, _) = build_paged(dir.path(), "spilled-nav", 1024, 0, &[]);
        graph.file().disable_navigation_mapping();
        let quantization = test_navigation_quantization(&vectors, 0);
        graph.set_navigation_quantization(quantization).unwrap();
        graph.begin_bulk_build();
        let mut rows: Vec<_> = vectors
            .iter()
            .enumerate()
            .map(|(i, vector)| {
                (
                    RowId {
                        page_id: i as u32,
                        slot_id: 0,
                    },
                    vector.clone(),
                )
            })
            .collect();
        let mut session = graph
            .build_session(2, 1024 * 1024, quantization, rows.len())
            .unwrap();
        graph
            .insert_bulk_chunk_parallel(&mut rows, 2, &mut session)
            .unwrap();
        graph.materialize_build_adjacency(&session).unwrap();
        graph.finalize_bulk_build().unwrap();
        drop(session);
        graph.refresh_query_navigation().unwrap();
        assert!(graph.has_query_navigation_for_test());
        assert!(!graph.has_spilled_query_navigation_for_test());
        let expected: Vec<_> = vectors[..4]
            .iter()
            .map(|query| graph.search(query, 10, 64).unwrap())
            .collect();
        std::env::set_var("MYDB_HNSW_QUERY_CACHE_MB", "4");
        graph.refresh_query_navigation().unwrap();
        assert!(graph.has_spilled_query_navigation_for_test());
        assert!(graph.has_reverse_seed_for_test());
        assert!(graph
            .has_live_row_id(RowId {
                page_id: 0,
                slot_id: 0
            })
            .unwrap());
        assert!(
            !graph.has_reverse_seed_for_test(),
            "first DML lookup must consume the scanned seed"
        );
        std::thread::scope(|scope| {
            for (query, want) in vectors[..4].iter().zip(&expected) {
                let graph = &graph;
                scope.spawn(move || assert_eq!(graph.search(query, 10, 64).unwrap(), *want));
            }
        });
        graph.clear_query_navigation_for_test();
        assert_eq!(graph.search(&vectors[0], 10, 64).unwrap(), expected[0]);
        assert_eq!(
            std::fs::read_dir(crate::storage::naming::system_tmp_dir(dir.path()))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn compact_query_navigation_matches_paged_oracle() {
        let dir = tempfile::tempdir().unwrap();
        let dim = 24;
        let vecs = gen_clustered(128, dim, 12, 0xA11CE);
        let (g, _) = build_paged(dir.path(), "query_nav", dim as u32, 0, &vecs);
        let queries = gen_clustered(4, dim, 12, 0xB0B);

        g.refresh_query_navigation().unwrap();
        assert!(
            g.has_query_navigation_for_test(),
            "the compact accelerator should be admitted for this small index"
        );
        let accelerated: Vec<Vec<(RowId, f64)>> = queries
            .iter()
            .map(|query| g.search(query, 12, 96).unwrap())
            .collect();

        // The durable page path is the semantic oracle. Compare on the exact
        // same graph so graph-build randomness cannot disguise a recall change.
        g.clear_query_navigation_for_test();
        for (query, expected) in queries.iter().zip(accelerated) {
            let paged = g.search(query, 12, 96).unwrap();
            assert_eq!(paged, expected);
        }

        // Incremental mutations must never leave a stale accelerator visible.
        g.refresh_query_navigation().unwrap();
        assert!(g.has_query_navigation_for_test());
        g.insert(
            txn_id_for_ddl(),
            RowId {
                page_id: vecs.len() as u32,
                slot_id: 0,
            },
            &vec![0.0; dim],
        )
        .unwrap();
        assert!(!g.has_query_navigation_for_test());
        assert!(g.has_query_navigation_codes_for_test());
    }

    #[test]
    fn incremental_navigation_matches_durable_graph() {
        // Odd and SIMD widths, all distance metrics, duplicate vectors and
        // large moves. Cached navigation must preserve the exact same graph
        // as the durable-page path, including all subsequent mutations.
        for metric in [0, 1, 2] {
            for dim in [3, 24, 129] {
                let dir = tempfile::tempdir().unwrap();
                let mut vecs = gen_vectors(64, dim, 9876);
                vecs[1] = vecs[0].clone();
                // Only seed construction defers per-edge WAL. Finalize both
                // graphs before testing ordinary incremental delete/insert;
                // keep the same topology, dimensions, metrics and mutations.
                let (cached, _) = build_paged_with_wal_mode(
                    dir.path(),
                    "cached",
                    dim as u32,
                    metric,
                    &vecs,
                    true,
                );
                let (oracle, _) = build_paged_with_wal_mode(
                    dir.path(),
                    "oracle",
                    dim as u32,
                    metric,
                    &vecs,
                    true,
                );
                cached.refresh_query_navigation().unwrap();
                oracle.refresh_query_navigation().unwrap();
                oracle.clear_query_navigation_for_test();
                if metric != 2 {
                    assert!(cached.has_query_navigation_for_test());
                }
                let query = &vecs[2];
                let before = cached.search(query, 8, 64).unwrap();
                if metric != 2 {
                    assert!(before.iter().any(|(row, _)| row.page_id == 2));
                }
                // Populate the result cache, then delete a cached nearest row.
                assert_eq!(before, cached.search(query, 8, 64).unwrap());
                for i in 2..10 {
                    let row = RowId {
                        page_id: i,
                        slot_id: 0,
                    };
                    cached.mark_deleted_by_row_id(row).unwrap();
                    oracle.mark_deleted_by_row_id(row).unwrap();
                    assert!(!cached
                        .search(query, 8, 64)
                        .unwrap()
                        .iter()
                        .any(|(r, _)| *r == row));
                    let vector = gen_vectors(1, dim, 1234 + i as u64).remove(0);
                    let a = cached.insert(txn_id_for_ddl(), row, &vector).unwrap();
                    let b = oracle.insert(txn_id_for_ddl(), row, &vector).unwrap();
                    assert_eq!(a, b);
                }
                // Reusing immutable codes must not alter a single edge.
                for page in 1..cached.file().meta().node_next_page_id {
                    let a = cached.file().read_page_bytes(page).unwrap();
                    let b = oracle.file().read_page_bytes(page).unwrap();
                    for slot in 0..read_u16(&a, NODE_OFF_SLOT_COUNT) {
                        assert_eq!(
                            node_page_read(&a, slot, dim, 16, 32).unwrap().neighbors,
                            node_page_read(&b, slot, dim, 16, 32).unwrap().neighbors
                        );
                    }
                }
                // Both paths must also produce identical query results.
                assert_eq!(
                    cached.search(query, 8, 72).unwrap(),
                    oracle.search(query, 8, 72).unwrap()
                );
            }
        }
    }

    #[test]
    fn paged_dot_recall_matches_brute_force() {
        use crate::engine::vector::distance::Metric;
        let dir = tempfile::tempdir().unwrap();
        let dim = 16;
        let vecs = gen_vectors(96, dim, 0xD07);
        let (g, _) = build_paged(dir.path(), "dot", dim as u32, Metric::Dot.as_u8(), &vecs);
        let queries = gen_vectors(3, dim, 0xD08);
        let k = 10;
        let (mut hits, mut total) = (0usize, 0usize);
        for q in &queries {
            let got: Vec<u32> = g
                .search(q, k, 64)
                .unwrap()
                .into_iter()
                .map(|(r, _)| r.page_id)
                .collect();
            for wanted in brute_knn(&vecs, q, k, Metric::Dot) {
                hits += usize::from(got.contains(&(wanted as u32)));
            }
            total += k;
        }
        let recall = hits as f64 / total as f64;
        assert!(recall >= 0.90, "paged dot recall too low: {recall}");
        let (_, exact_fallback, covered, _) = g.navigation_stats();
        assert_eq!(covered, 0, "DOT must preserve exact f32 navigation");
        assert_eq!(exact_fallback, g.node_count());
    }

    #[test]
    fn paged_chunked_exact_navigation_connects_across_chunks() {
        use crate::engine::vector::distance::Metric;
        for metric in [Metric::L2, Metric::Dot] {
            let dir = tempfile::tempdir().unwrap();
            let vectors = gen_vectors(320, 16, 7878);
            let log = Arc::new(parking_lot::Mutex::new(
                LogManager::open(dir.path()).unwrap(),
            ));
            let pool = test_pool(&log);
            let file = Arc::new(
                VectorIndexFile::open_with_durability(
                    dir.path().join("exact.hvec"),
                    log,
                    DurabilityMode::Relaxed,
                    pool,
                    16,
                    metric.as_u8(),
                    16,
                    32,
                    128,
                )
                .unwrap(),
            );
            let graph = crate::engine::vector::hnsw_paged::PagedHnsw::new(file);
            graph.begin_bulk_build();
            let mut session = graph.build_session(1, 256, None, vectors.len()).unwrap();
            let mut rows: Vec<_> = vectors
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    (
                        RowId {
                            page_id: i as u32,
                            slot_id: 0,
                        },
                        v.clone(),
                    )
                })
                .collect();
            for chunk in rows.chunks_mut(32) {
                graph
                    .insert_bulk_chunk_parallel(chunk, 1, &mut session)
                    .unwrap();
            }
            graph.finalize_bulk_build().unwrap();
            let mut hits = 0;
            let queries = gen_vectors(10, 16, 8787);
            for query in &queries {
                let found: Vec<_> = graph
                    .search(query, 10, 128)
                    .unwrap()
                    .into_iter()
                    .map(|(r, _)| r.page_id as usize)
                    .collect();
                hits += brute_knn(&vectors, query, 10, metric)
                    .iter()
                    .filter(|i| found.contains(i))
                    .count();
            }
            assert!(
                hits >= 90,
                "exact chunk navigation lost cross-chunk edges: {metric:?}, hits={hits}/100"
            );
        }
    }

    #[test]
    fn audit_bug3_paged_dml_counters_survive_reopen_without_explicit_meta_flush() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let vectors = gen_vectors(257, 8, 9124);
        let expected;
        let path;
        {
            let (graph, _) = build_paged(dir.path(), "counter-tail", 8, 0, &vectors);
            // An out-of-range tail row also verifies the fallback count, not
            // just total rows. No explicit flush_meta/finalize hides DML bugs.
            graph
                .insert_ddl(
                    RowId {
                        page_id: 257,
                        slot_id: 0,
                    },
                    &[50.0; 8],
                )
                .unwrap();
            expected = graph.file().meta();
            assert_eq!(expected.node_count, 258);
            assert!(expected.nav_exact_nodes > 0);
            path = graph.file().path.clone();
            graph.file().flush().unwrap();
        }
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool = test_pool(&log);
        let reopened = VectorIndexFile::open_with_durability(
            path,
            log,
            DurabilityMode::Strict,
            pool,
            8,
            0,
            16,
            32,
            200,
        )
        .unwrap();
        assert_eq!(reopened.meta().node_count, expected.node_count);
        assert_eq!(reopened.meta().nav_exact_nodes, expected.nav_exact_nodes);
    }

    #[test]
    fn audit_bug3_paged_dml_recalibrates_narrow_seed_and_recovers_outliers() {
        use crate::engine::vector::{hnsw_paged::PagedHnsw, Metric};
        for metric in [Metric::L2, Metric::Cosine, Metric::Dot] {
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let log = Arc::new(parking_lot::Mutex::new(
                LogManager::open(dir.path()).unwrap(),
            ));
            let pool = test_pool(&log);
            let file = Arc::new(
                VectorIndexFile::open_with_durability(
                    dir.path().join("audit.hvec"),
                    log,
                    DurabilityMode::Relaxed,
                    pool,
                    8,
                    metric.as_u8(),
                    16,
                    32,
                    128,
                )
                .unwrap(),
            );
            let graph = PagedHnsw::new(Arc::clone(&file));
            let mut vectors = gen_vectors(257, 8, 39852);
            vectors[0] = vec![0.001; 8];
            for (i, vector) in vectors.iter().enumerate() {
                graph
                    .insert_ddl(
                        RowId {
                            page_id: i as u32,
                            slot_id: 0,
                        },
                        vector,
                    )
                    .unwrap();
                assert!(
                    graph.row_lookup_ready(),
                    "DML must maintain the reverse lookup from the first row"
                );
            }
            if metric != Metric::Dot {
                assert!(file.navigation_quantization().is_some());
                assert_eq!(file.meta().nav_exact_nodes, 0);
            } else {
                assert!(file.navigation_quantization().is_none());
            }
            let outlier = vec![100.0, -80.0, 60.0, 0.0, 1.0, 2.0, 3.0, 4.0];
            graph
                .insert_ddl(
                    RowId {
                        page_id: 257,
                        slot_id: 0,
                    },
                    &outlier,
                )
                .unwrap();
            vectors.push(outlier.clone());
            let expected = brute_knn(&vectors, &outlier, 1, metric)[0];
            assert_eq!(
                graph.search(&outlier, 1, 258).unwrap()[0].0.page_id,
                expected as u32
            );
            graph.refresh_query_navigation().unwrap();
            if metric != Metric::Dot {
                assert_eq!(file.meta().nav_exact_nodes, 0);
            }
            assert_eq!(
                graph.search(&outlier, 1, 258).unwrap()[0].0.page_id,
                expected as u32
            );
            let entry = file.entry_with_level().unwrap();
            let row = file.read_node(entry.0).unwrap().unwrap().row_id;
            assert_eq!(graph.mark_deleted_by_row_id(row).unwrap(), 1);
            assert_eq!(file.entry_with_level(), Some(entry));
            assert!(graph
                .search(&outlier, 258, 258)
                .unwrap()
                .iter()
                .all(|(r, _)| *r != row));
        }
    }

    #[test]
    fn paged_small_chunks_preserve_global_recall_for_all_metrics() {
        use crate::engine::vector::distance::Metric;
        for (metric_code, metric) in [(0, Metric::L2), (1, Metric::Cosine), (2, Metric::Dot)] {
            let dir = tempfile::tempdir().unwrap();
            let vectors = gen_vectors(480, 24, 18927);
            let (graph, _) = build_paged(dir.path(), "small-chunks", 24, metric_code, &[]);
            let quantization = test_navigation_quantization(&vectors, metric_code);
            let mut rows: Vec<_> = vectors
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    (
                        RowId {
                            page_id: i as u32,
                            slot_id: 0,
                        },
                        v.clone(),
                    )
                })
                .collect();
            graph.begin_bulk_build();
            // Tiny cache and chunks exercise evictions and old/current pairs.
            let mut session = graph
                .build_session(2, 1024, quantization, rows.len())
                .unwrap();
            for chunk in rows.chunks_mut(24) {
                graph
                    .insert_bulk_chunk_parallel(chunk, 2, &mut session)
                    .unwrap();
                assert!(
                    chunk.iter().all(|(_, vector)| vector.len() == 24),
                    "exact fallback queries must survive SQ8 linking"
                );
            }
            graph.materialize_build_adjacency(&session).unwrap();
            graph.finalize_bulk_build().unwrap();
            drop(session);
            let mut hits = 0;
            for query in gen_vectors(16, 24, 4999) {
                let got = graph.search(&query, 10, 128).unwrap();
                hits += brute_knn(&vectors, &query, 10, metric)
                    .iter()
                    .filter(|&&i| got.iter().any(|(row, _)| row.page_id == i as u32))
                    .count();
            }
            assert!(
                hits >= 144,
                "cross-chunk recall degraded: {metric:?}: {hits}/160"
            );
        }
    }

    #[test]
    fn paged_chunked_global_sq8_cache_reuses_old_nodes() {
        let mut reference = None;
        for (cache_bytes, mapped) in [
            (2 * 1024 * 1024, false),
            (256, false),
            (2 * 1024 * 1024, true),
            (256, true),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let dim = 16usize;
            let vecs = gen_vectors(320, dim, 7878);
            let (mut vmin, mut vmax) = (f32::INFINITY, f32::NEG_INFINITY);
            for &value in vecs.iter().flatten() {
                vmin = vmin.min(value);
                vmax = vmax.max(value);
            }
            let mid = (vmax + vmin) * 0.5;
            let inv_scale = 254.0 / (vmax - vmin);
            let dist_mul = inv_scale.recip().powi(2);
            let log = Arc::new(parking_lot::Mutex::new(
                LogManager::open(dir.path()).unwrap(),
            ));
            let pool = test_pool(&log);
            let file = Arc::new(
                VectorIndexFile::open_with_durability(
                    dir.path().join("vec__1__chunk_sq8.hvec"),
                    log,
                    DurabilityMode::Strict,
                    pool,
                    dim as u32,
                    0,
                    16,
                    32,
                    64,
                )
                .unwrap(),
            );
            let graph = crate::engine::vector::hnsw_paged::PagedHnsw::new(file);
            let mut rows: Vec<(RowId, Vec<f32>)> = vecs
                .iter()
                .enumerate()
                .map(|(i, vector)| {
                    (
                        RowId {
                            page_id: i as u32,
                            slot_id: 0,
                        },
                        vector.clone(),
                    )
                })
                .collect();
            graph.begin_bulk_build();
            let mut session = graph
                .build_session(2, cache_bytes, Some((mid, inv_scale, dist_mul)), rows.len())
                .unwrap();
            if !mapped {
                session.use_positioned_codes_for_test();
            }
            for chunk in rows.chunks_mut(80) {
                graph
                    .insert_bulk_chunk_parallel(chunk, 2, &mut session)
                    .unwrap();
                assert!(
                    chunk.iter().all(|(_, vector)| vector.len() == dim),
                    "SQ8 linking must retain exact fallback vectors"
                );
            }
            graph.materialize_build_adjacency(&session).unwrap();
            graph.finalize_bulk_build().unwrap();
            assert_eq!(
                graph.build_distance_cache_hits_for_test(&vecs[0]),
                1,
                "build navigation must not bypass an already decoded cache entry"
            );
            let (hits, misses) = session.cache_stats();
            let build_adj_path = graph.file().build_adj_temp_path();
            assert!(
                build_adj_path.exists(),
                "temporary adjacency was not created"
            );
            drop(session);
            assert!(
                !build_adj_path.exists(),
                "temporary adjacency was not removed after build"
            );
            if mapped {
                assert_eq!(
                    (hits, misses),
                    (0, 0),
                    "mapped rows must not duplicate the file cache in worker LRUs"
                );
            } else {
                assert!(hits > 0, "old-node SQ8 cache was never reused");
                assert!(misses > 0, "test did not exercise old-node cache fill");
            }
            let got: Vec<u32> = graph
                .search(&vecs[0], 10, 64)
                .unwrap()
                .into_iter()
                .map(|(row, _)| row.page_id)
                .collect();
            assert!(
                got.contains(&0),
                "self-nearest node missing after chunk build"
            );
            assert_eq!(
                &got,
                reference.get_or_insert_with(|| got.clone()),
                "mapped and positioned linking must produce the same graph answers"
            );
        }
    }

    #[test]
    fn paged_cosine_recall_matches_brute_force() {
        // Cosine navigates persisted SQ8 rows via unit-normalized codes
        // (L2² of unit vectors = 2(1−cos), monotone with cosine distance). The
        // f32 re-rank fixes any quantization drift, so recall vs brute cosine
        // must stay high.
        use crate::engine::vector::distance::Metric;
        let dir = tempfile::tempdir().unwrap();
        let dim = 24;
        let vecs = gen_vectors(96, dim, 0x5EED);
        let (g, _) = build_paged(dir.path(), "cos", dim as u32, Metric::Cosine.as_u8(), &vecs);
        let queries = gen_vectors(3, dim, 0xC05);
        let k = 10;
        let (mut hits, mut total) = (0usize, 0usize);
        for q in &queries {
            let got: Vec<u32> = g
                .search(q, k, 64)
                .unwrap()
                .into_iter()
                .map(|(r, _)| r.page_id)
                .collect();
            for w in brute_knn(&vecs, q, k, Metric::Cosine) {
                if got.contains(&(w as u32)) {
                    hits += 1;
                }
            }
            total += k;
        }
        let recall = hits as f64 / total as f64;
        assert!(recall >= 0.85, "paged cosine recall too low: {recall}");
    }

    #[test]
    fn paged_in_place_update_takes_low_drift_rejects_teleport() {
        // The drift-adaptive in-place UPDATE must overwrite a node's vector
        // (keeping its id + edges, no new node, no tombstone) for a small move,
        // and refuse a large move so the caller re-links. After an accepted
        // in-place move the row is still found by a query at its new location.
        let dim = 16usize;
        let dir = tempfile::tempdir().unwrap();
        let vecs = gen_vectors(64, dim, 4242);
        let (g, _) = build_paged(dir.path(), "inplace", dim as u32, 0, &vecs);
        let before = g.node_count();
        let target = RowId {
            page_id: 7,
            slot_id: 0,
        };
        let old = &vecs[7];

        g.refresh_query_navigation().unwrap();
        assert!(g.has_query_navigation_for_test());
        // Tiny move (incremental embedding refresh) → accepted in place.
        let low: Vec<f32> = old.iter().map(|x| x + 0.001).collect();
        assert!(
            g.try_update_in_place(target, &low, 0.5).unwrap(),
            "small move should be taken in place"
        );
        assert!(!g.has_query_navigation_codes_for_test());
        assert_eq!(
            g.node_count(),
            before,
            "in-place update must not add a node"
        );
        assert_eq!(g.stale_count(), 0, "in-place update must not tombstone");
        // The updated row is still navigable at its (barely moved) location.
        let got: Vec<u32> = g
            .search(&low, 10, 64)
            .unwrap()
            .into_iter()
            .map(|(r, _)| r.page_id)
            .collect();
        assert!(got.contains(&7), "updated row must remain searchable");

        // A teleport (unrelated random vector, far from the node's neighbours) is
        // rejected so the caller falls back to a full re-link.
        let far = vec![1000.0f32; dim];
        assert!(
            !g.try_update_in_place(target, &far, 0.5).unwrap(),
            "a large move must be rejected for re-link"
        );
        assert_eq!(g.node_count(), before);
        assert_eq!(g.stale_count(), 0);
    }

    #[test]
    fn paged_search_empty_graph_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let (g, _) = build_paged(dir.path(), "empty", 8, 0, &[]);
        assert!(g.search(&[0.0; 8], 5, 32).unwrap().is_empty());
    }

    #[test]
    fn persistent_navigation_handles_growth_without_rebuild() {
        let dir = tempfile::tempdir().unwrap();
        let dim = 8usize;
        let vecs = gen_vectors(100, dim, 9001);
        let (g, _) = build_paged(dir.path(), "persistent_growth", dim as u32, 0, &vecs);

        let newest = vec![50.0f32; dim];
        g.insert(
            txn_id_for_ddl(),
            RowId {
                page_id: 10_000,
                slot_id: 0,
            },
            &newest,
        )
        .unwrap();
        assert_eq!(
            g.search(&newest, 1, 64).unwrap()[0].0.page_id,
            10_000,
            "exact_nav fallback nodes must be immediately searchable"
        );

        for i in 1..15u32 {
            let v = vec![60.0 + i as f32; dim];
            g.insert(
                txn_id_for_ddl(),
                RowId {
                    page_id: 10_000 + i,
                    slot_id: 0,
                },
                &v,
            )
            .unwrap();
        }
        let (_, exact_fallback, covered, _) = g.navigation_stats();
        assert_eq!(covered + exact_fallback, g.node_count());
        let _ = g.search(&newest, 5, 64).unwrap();
    }

    #[test]
    fn paged_tombstoned_nodes_are_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let dim = 8;
        let vecs = gen_vectors(100, dim, 7);
        let (g, vids) = build_paged(dir.path(), "tomb", dim as u32, 0, &vecs);
        g.file()
            .set_tombstone(txn_id_for_ddl(), vids[0], true)
            .unwrap();
        let got = g.search(&vecs[0], 5, 64).unwrap();
        assert!(got.iter().all(|(r, _)| r.page_id != 0));
    }

    #[test]
    fn paged_survives_reopen_with_fresh_pool() {
        let dir = tempfile::tempdir().unwrap();
        let dim = 8;
        let vecs = gen_vectors(64, dim, 42);
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let path = dir.path().join("vec__1__reopen.hvec");
        let want: Vec<u32>;
        {
            let pool = test_pool(&log);
            let file = Arc::new(
                VectorIndexFile::open_with_durability(
                    path.clone(),
                    log.clone(),
                    DurabilityMode::Strict,
                    pool.clone(),
                    dim as u32,
                    0,
                    16,
                    32,
                    200,
                )
                .unwrap(),
            );
            let g = crate::engine::vector::hnsw_paged::PagedHnsw::new(file);
            for (i, v) in vecs.iter().enumerate() {
                g.insert(
                    txn_id_for_ddl(),
                    RowId {
                        page_id: i as u32,
                        slot_id: 0,
                    },
                    v,
                )
                .unwrap();
            }
            let row = RowId {
                page_id: 0,
                slot_id: 0,
            };
            g.mark_deleted_by_row_id(row).unwrap();
            g.insert(txn_id_for_ddl(), row, &vec![10.0; dim]).unwrap();
            want = g
                .search(&vecs[0], 5, 64)
                .unwrap()
                .into_iter()
                .map(|(r, _)| r.page_id)
                .collect();
            pool.flush_all().unwrap();
        }
        // Reopen with a fresh buffer pool — graph served entirely from disk.
        let pool = test_pool(&log);
        let file = Arc::new(
            VectorIndexFile::open_with_durability(
                path,
                log,
                DurabilityMode::Strict,
                pool,
                dim as u32,
                0,
                16,
                32,
                200,
            )
            .unwrap(),
        );
        let g = crate::engine::vector::hnsw_paged::PagedHnsw::new(file);
        let got: Vec<u32> = g
            .search(&vecs[0], 5, 64)
            .unwrap()
            .into_iter()
            .map(|(r, _)| r.page_id)
            .collect();
        assert_eq!(got, want, "search results must survive reopen from disk");
    }

    #[test]
    fn paged_larger_than_memory_index_is_correct() {
        // The headline capability: QUERY an index whose pages far exceed the
        // buffer pool. Build with a normal pool (fast), flush to disk, then
        // reopen with a tiny pool so every search must demand-page from disk
        // and evict. A search that leaked pins (held many pages at once) could
        // not run in a pool this small, so passing also exercises the
        // O(1)-peak-pin traversal model. (Build durability is exercised by the
        // crash-recovery test; here we use relaxed mode for speed.)
        use crate::engine::vector::distance::Metric;
        let dir = tempfile::tempdir().unwrap();
        let dim = 32usize;
        let n = 160usize;
        let vecs = gen_vectors(n, dim, 555);
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let path = dir.path().join("vec__1__big.hvec");
        // --- build phase: normal pool, fast ---
        {
            let pool = BufferPool::new(
                DEFAULT_BUFFER_POOL_PAGES,
                log.clone(),
                DurabilityMode::Relaxed,
                std::time::Duration::from_millis(DEFAULT_LOCK_WAIT_MS),
            );
            let file = Arc::new(
                VectorIndexFile::open_with_durability(
                    path.clone(),
                    log.clone(),
                    DurabilityMode::Relaxed,
                    pool.clone(),
                    dim as u32,
                    0,
                    16,
                    32,
                    64,
                )
                .unwrap(),
            );
            let g = crate::engine::vector::hnsw_paged::PagedHnsw::new(file.clone());
            for (i, v) in vecs.iter().enumerate() {
                g.insert(
                    txn_id_for_ddl(),
                    RowId {
                        page_id: i as u32,
                        slot_id: 0,
                    },
                    v,
                )
                .unwrap();
            }
            let pages = file.meta().node_next_page_id;
            assert!(pages > 4, "index ({pages} pages) should span many pages");
            pool.flush_all().unwrap();
        }
        // --- query phase: TINY pool, index served from disk ---
        let pool = BufferPool::new(
            4,
            log.clone(),
            DurabilityMode::Relaxed,
            std::time::Duration::from_millis(DEFAULT_LOCK_WAIT_MS),
        );
        let file = Arc::new(
            VectorIndexFile::open_with_durability(
                path,
                log,
                DurabilityMode::Relaxed,
                pool,
                dim as u32,
                0,
                16,
                32,
                64,
            )
            .unwrap(),
        );
        let pages = file.meta().node_next_page_id;
        assert!(
            pages > 4,
            "index ({pages} pages) must exceed the 4-page query pool"
        );
        let g = crate::engine::vector::hnsw_paged::PagedHnsw::new(file);
        let queries = gen_vectors(3, dim, 222);
        let (mut hits, mut total) = (0usize, 0usize);
        let k = 10;
        for q in &queries {
            let got: Vec<u32> = g
                .search(q, k, 64)
                .unwrap()
                .into_iter()
                .map(|(r, _)| r.page_id)
                .collect();
            for w in brute_knn(&vecs, q, k, Metric::L2) {
                if got.contains(&(w as u32)) {
                    hits += 1;
                }
            }
            total += k;
        }
        let recall = hits as f64 / total as f64;
        assert!(
            recall >= 0.85,
            "larger-than-memory recall too low: {recall}"
        );
    }

    #[test]
    fn paged_concurrent_inserts_and_search_no_deadlock_and_recall_holds() {
        use crate::engine::vector::distance::Metric;
        let dir = tempfile::tempdir().unwrap();
        let dim = 12usize;
        let n = 64usize;
        let vecs = gen_vectors(n, dim, 31337);
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool = test_pool(&log);
        let path = dir.path().join("vec__1__conc.hvec");
        let file = Arc::new(
            VectorIndexFile::open_with_durability(
                path,
                log,
                DurabilityMode::Strict,
                pool,
                dim as u32,
                0,
                16,
                32,
                200,
            )
            .unwrap(),
        );
        let g = Arc::new(crate::engine::vector::hnsw_paged::PagedHnsw::new(file));
        let vecs = Arc::new(vecs);
        // 4 concurrent inserter threads over disjoint row-id ranges, plus a
        // searcher hammering the graph as it grows.
        let threads = 4usize;
        let per = n / threads;
        std::thread::scope(|s| {
            for t in 0..threads {
                let g = g.clone();
                let vecs = vecs.clone();
                s.spawn(move || {
                    for i in (t * per)..((t + 1) * per) {
                        g.insert(
                            txn_id_for_ddl(),
                            RowId {
                                page_id: i as u32,
                                slot_id: 0,
                            },
                            &vecs[i],
                        )
                        .unwrap();
                    }
                });
            }
            let g2 = g.clone();
            let vecs2 = vecs.clone();
            s.spawn(move || {
                for _ in 0..10 {
                    let _ = g2.search(&vecs2[0], 5, 32).unwrap();
                }
            });
        });
        assert_eq!(g.node_count(), n as u64, "every concurrent insert landed");
        // Recall over the fully-built concurrent graph.
        let queries = gen_vectors(3, dim, 88);
        let k = 10;
        let (mut hits, mut total) = (0usize, 0usize);
        for q in &queries {
            let got: Vec<u32> = g
                .search(q, k, 64)
                .unwrap()
                .into_iter()
                .map(|(r, _)| r.page_id)
                .collect();
            for w in brute_knn(&vecs, q, k, Metric::L2) {
                if got.contains(&(w as u32)) {
                    hits += 1;
                }
            }
            total += k;
        }
        let recall = hits as f64 / total as f64;
        assert!(recall >= 0.85, "concurrent-build recall too low: {recall}");
    }

    #[test]
    fn paged_recovers_from_wal_after_crash_without_page_flush() {
        // Build a paged graph in strict mode, force the WAL durable, then
        // simulate a crash by dropping the buffer pool WITHOUT flushing its
        // dirty pages — so the only durable copy of the graph is in the WAL.
        // Recovery must reconstruct the `.hvec` purely by replaying the
        // physical full-page records, with zero HNSW-specific redo code.
        let dir = tempfile::tempdir().unwrap();
        let dim = 8usize;
        let vecs = gen_vectors(48, dim, 2026);
        // Place the file under the canonical indexes/ bucket so the WAL
        // relative path round-trips through recovery's path resolution.
        let rel = "schemas/test/indexes/vec__1__crash.hvec";
        let path = dir.path().join(rel);
        let want: Vec<u32>;
        {
            let log = Arc::new(parking_lot::Mutex::new(
                LogManager::open(dir.path()).unwrap(),
            ));
            let pool = test_pool(&log);
            let file = Arc::new(
                VectorIndexFile::open_with_durability(
                    path.clone(),
                    log.clone(),
                    DurabilityMode::Strict,
                    pool.clone(),
                    dim as u32,
                    0,
                    16,
                    32,
                    200,
                )
                .unwrap(),
            );
            let g = crate::engine::vector::hnsw_paged::PagedHnsw::new(file);
            for (i, v) in vecs.iter().enumerate() {
                g.insert(
                    txn_id_for_ddl(),
                    RowId {
                        page_id: i as u32,
                        slot_id: 0,
                    },
                    v,
                )
                .unwrap();
            }
            want = g
                .search(&vecs[0], 5, 64)
                .unwrap()
                .into_iter()
                .map(|(r, _)| r.page_id)
                .collect();
            // WAL durable; buffer-pool pages intentionally NOT flushed.
            log.lock().flush().unwrap();
            drop(g);
            drop(pool);
            drop(log);
        }
        // Replay the WAL: redo_page_update rebuilds the `.hvec` from physical
        // full-page records (generic path — no vector-specific recovery).
        let result = recover_from_wal(dir.path(), DurabilityMode::Strict).unwrap();
        assert!(
            result.summary.redone_records > 0,
            "expected WAL redo to replay vector index pages"
        );
        // Reopen from the recovered on-disk file and confirm the graph is
        // intact and queryable with identical results.
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool = test_pool(&log);
        let file = Arc::new(
            VectorIndexFile::open_with_durability(
                path,
                log,
                DurabilityMode::Strict,
                pool,
                dim as u32,
                0,
                16,
                32,
                200,
            )
            .unwrap(),
        );
        let g = crate::engine::vector::hnsw_paged::PagedHnsw::new(file);
        let got: Vec<u32> = g
            .search(&vecs[0], 5, 64)
            .unwrap()
            .into_iter()
            .map(|(r, _)| r.page_id)
            .collect();
        assert_eq!(got, want, "graph must be identical after WAL recovery");
    }

    /// Build micro-benchmark: 5000 x 8-dim cold build, mirroring the DDL
    /// `insert_ddl` loop. Run with:
    ///   cargo nextest run -- --ignored paged_build_bench_5k_8d --nocapture
    /// (or `cargo test ... -- --nocapture --ignored`).
    #[test]
    #[ignore = "perf benchmark; run explicitly with --ignored --nocapture"]
    fn paged_build_bench_5k_8d() {
        let dir = tempfile::tempdir().unwrap();
        let dim: usize = std::env::var("BENCH_DIM")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8);
        let n: usize = std::env::var("BENCH_N")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5000);
        let efc: u16 = std::env::var("BENCH_EFC")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(200);
        let vecs = gen_vectors(n, dim, 12345);
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool = test_pool(&log);
        let path = dir.path().join("vec__1__bench.hvec");
        let file = Arc::new(
            VectorIndexFile::open_with_durability(
                path,
                log,
                DurabilityMode::Strict,
                pool,
                dim as u32,
                0,
                16,
                32,
                efc,
            )
            .unwrap(),
        );
        let threads: usize = std::env::var("BENCH_THREADS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1);
        let g = crate::engine::vector::hnsw_paged::PagedHnsw::new(file);
        let quantization = test_navigation_quantization(&vecs, 0);
        g.set_navigation_quantization(quantization).unwrap();
        let mut rows: Vec<(RowId, Vec<f32>)> = vecs
            .iter()
            .enumerate()
            .map(|(i, v)| {
                (
                    RowId {
                        page_id: i as u32,
                        slot_id: 0,
                    },
                    v.clone(),
                )
            })
            .collect();
        let t0 = std::time::Instant::now();
        g.begin_bulk_build();
        let mut session = g
            .build_session(threads, 256 * 1024 * 1024, quantization, rows.len())
            .unwrap();
        g.insert_bulk_chunk_parallel(&mut rows, threads, &mut session)
            .unwrap();
        g.materialize_build_adjacency(&session).unwrap();
        g.finalize_bulk_build().unwrap();
        let dt = t0.elapsed();
        eprintln!(
            "PAGED BUILD: n={n} dim={dim} efc={efc} threads={threads} took {:.3}s ({:.0} rows/s)",
            dt.as_secs_f64(),
            n as f64 / dt.as_secs_f64()
        );
        // Optional recall@10 vs brute force over the same data.
        if std::env::var("BENCH_RECALL").is_ok() {
            let q = &vecs[0];
            let mut brute: Vec<(f64, u32)> = vecs
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    (
                        crate::engine::vector::distance::ordering_distance(
                            q,
                            v,
                            crate::engine::vector::distance::Metric::L2,
                        ),
                        i as u32,
                    )
                })
                .collect();
            brute.sort_by(|a, b| a.0.total_cmp(&b.0));
            let truth: std::collections::HashSet<u32> =
                brute.iter().take(10).map(|(_, i)| *i).collect();
            let got: std::collections::HashSet<u32> = g
                .search(q, 10, 64)
                .unwrap()
                .into_iter()
                .map(|(r, _)| r.page_id)
                .collect();
            let hit = truth.intersection(&got).count();
            eprintln!("PAGED RECALL@10 (q=vec0): {hit}/10");
        }
        if std::env::var("BENCH_MEM").is_ok() {
            let mem = crate::engine::vector::hnsw::HnswIndex::new(
                crate::engine::vector::distance::Metric::L2,
                16,
                efc as usize,
            );
            let t1 = std::time::Instant::now();
            for (i, v) in vecs.iter().enumerate() {
                let _ = mem.append_node(
                    RowId {
                        page_id: i as u32,
                        slot_id: 0,
                    },
                    v.clone(),
                );
            }
            mem.link_all(threads.max(1));
            let dt1 = t1.elapsed();
            eprintln!(
                "MEM   BUILD: n={n} dim={dim} efc={efc} threads={threads} took {:.3}s ({:.0} rows/s)",
                dt1.as_secs_f64(),
                n as f64 / dt1.as_secs_f64()
            );
        }
    }

    /// Query micro-benchmark: build an index, then time `BENCH_Q` k-NN
    /// searches. Mirrors the executor's `VectorIndexScan` navigation. Run with:
    ///   BENCH_N=20000 BENCH_DIM=768 cargo test ... -- --ignored paged_query_bench --nocapture
    #[test]
    #[ignore = "perf benchmark; run explicitly with --ignored --nocapture"]
    fn paged_query_bench() {
        let dir = tempfile::tempdir().unwrap();
        let dim: usize = std::env::var("BENCH_DIM")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(768);
        let n: usize = std::env::var("BENCH_N")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(20000);
        let efc: u16 = std::env::var("BENCH_EFC")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(200);
        let ef_search: usize = std::env::var("BENCH_EF")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(64);
        let k: usize = std::env::var("BENCH_K")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10);
        let nq: usize = std::env::var("BENCH_Q")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(500);
        let build_threads: usize = std::env::var("BENCH_THREADS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8);
        // BENCH_CLUSTERS=C generates clustered (realistic) data so recall is
        // representative; default 0 keeps the i.i.d.-uniform worst case.
        let clusters: usize = std::env::var("BENCH_CLUSTERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let (vecs, queries) = if clusters > 0 {
            (
                gen_clustered(n, dim, clusters, 12345),
                gen_clustered(nq, dim, clusters, 54321),
            )
        } else {
            (gen_vectors(n, dim, 12345), gen_vectors(nq, dim, 999))
        };
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool = test_pool(&log);
        let path = dir.path().join("vec__1__qbench.hvec");
        let file = Arc::new(
            VectorIndexFile::open_with_durability(
                path,
                log,
                DurabilityMode::Strict,
                pool,
                dim as u32,
                0,
                16,
                32,
                efc,
            )
            .unwrap(),
        );
        let g = crate::engine::vector::hnsw_paged::PagedHnsw::new(file);
        let quantization = test_navigation_quantization(&vecs, 0);
        g.set_navigation_quantization(quantization).unwrap();
        let mut rows: Vec<(RowId, Vec<f32>)> = vecs
            .iter()
            .enumerate()
            .map(|(i, v)| {
                (
                    RowId {
                        page_id: i as u32,
                        slot_id: 0,
                    },
                    v.clone(),
                )
            })
            .collect();
        g.begin_bulk_build();
        let mut session = g
            .build_session(build_threads, 256 * 1024 * 1024, quantization, rows.len())
            .unwrap();
        g.insert_bulk_chunk_parallel(&mut rows, build_threads, &mut session)
            .unwrap();
        g.materialize_build_adjacency(&session).unwrap();
        g.finalize_bulk_build().unwrap();

        // Measure the true first query. It may fault navigation pages into the
        // data buffer, but must never perform an O(n) arena construction scan.
        let tw = std::time::Instant::now();
        let _ = g.search(&queries[0], k, ef_search).unwrap();
        eprintln!(
            "PAGED FIRST-QUERY (persistent navigation): {:.3} ms",
            tw.elapsed().as_secs_f64() * 1000.0
        );
        let want_recall = std::env::var("BENCH_RECALL").is_ok();
        // Precompute brute-force ground truth once.
        let truth: Vec<Vec<u32>> = if want_recall {
            queries
                .iter()
                .map(|q| {
                    brute_knn(&vecs, q, k, crate::engine::vector::distance::Metric::L2)
                        .into_iter()
                        .map(|w| w as u32)
                        .collect()
                })
                .collect()
        } else {
            Vec::new()
        };
        // Measure the warmed persistent-navigation path over the shared data
        // buffer. There is intentionally no HNSW-specific cache mode toggle.
        let measure = |label: &str| -> f64 {
            // Warm.
            for q in queries.iter().take(20) {
                let _ = g.search(q, k, ef_search).unwrap();
            }
            let t0 = std::time::Instant::now();
            let mut sink = 0u64;
            for q in &queries {
                sink = sink.wrapping_add(g.search(q, k, ef_search).unwrap().len() as u64);
            }
            let per = t0.elapsed().as_secs_f64() * 1000.0 / nq as f64;
            let recall = if want_recall {
                let (mut hits, mut total) = (0usize, 0usize);
                for (q, t) in queries.iter().zip(truth.iter()) {
                    let got: std::collections::HashSet<u32> = g
                        .search(q, k, ef_search)
                        .unwrap()
                        .into_iter()
                        .map(|(r, _)| r.page_id)
                        .collect();
                    hits += t.iter().filter(|w| got.contains(w)).count();
                    total += t.len();
                }
                hits as f64 / total as f64
            } else {
                f64::NAN
            };
            eprintln!(
                "PAGED QUERY [{label}]: n={n} dim={dim} ef={ef_search} k={k} q={nq} → {:.3} ms/query ({:.0} qps) recall@{k}={recall:.4} [sink={sink}]",
                per,
                1000.0 / per,
            );
            per
        };
        let _ = measure("persistent/data-buffer");
    }

    #[test]
    fn paged_bulk_build_finalizes_and_recovers_from_wal() {
        // The deferred-WAL cold-build path: build under `begin_bulk_build` /
        // `finalize_bulk_build` (one WAL record per page at finalize, not per
        // edge), force only the WAL durable (NOT the dirty pages), simulate a
        // crash, and confirm recovery rebuilds the `.hvec` from the per-page
        // records and the graph is finalized + queryable with identical
        // results — i.e. finalize is a correct, crash-safe commit point.
        let dir = tempfile::tempdir().unwrap();
        let dim = 8usize;
        let vecs = gen_vectors(150, dim, 909);
        let rel = "schemas/test/indexes/vec__1__bulk.hvec";
        let path = dir.path().join(rel);
        let want: Vec<u32>;
        {
            let log = Arc::new(parking_lot::Mutex::new(
                LogManager::open(dir.path()).unwrap(),
            ));
            let pool = test_pool(&log);
            let file = Arc::new(
                VectorIndexFile::open_with_durability(
                    path.clone(),
                    log.clone(),
                    DurabilityMode::Strict,
                    pool.clone(),
                    dim as u32,
                    0,
                    16,
                    32,
                    200,
                )
                .unwrap(),
            );
            let g = crate::engine::vector::hnsw_paged::PagedHnsw::new(file.clone());
            assert!(!g.is_finalized(), "fresh file is not finalized");
            g.begin_bulk_build();
            let mut rows: Vec<(RowId, Vec<f32>)> = vecs
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    (
                        RowId {
                            page_id: i as u32,
                            slot_id: 0,
                        },
                        v.clone(),
                    )
                })
                .collect();
            let mut session = g
                .build_session(1, 256 * 1024 * 1024, None, rows.len())
                .unwrap();
            g.insert_bulk_chunk_parallel(&mut rows, 1, &mut session)
                .unwrap();
            g.finalize_bulk_build().unwrap();
            assert!(g.is_finalized(), "finalized after bulk build");
            want = g
                .search(&vecs[0], 5, 64)
                .unwrap()
                .into_iter()
                .map(|(r, _)| r.page_id)
                .collect();
            // WAL durable; buffer-pool pages intentionally NOT flushed.
            log.lock().flush().unwrap();
            drop(g);
            drop(pool);
            drop(log);
        }
        let result = recover_from_wal(dir.path(), DurabilityMode::Strict).unwrap();
        assert!(
            result.summary.redone_records > 0,
            "expected WAL redo to replay the finalized vector index pages"
        );
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool = test_pool(&log);
        let file = Arc::new(
            VectorIndexFile::open_with_durability(
                path,
                log,
                DurabilityMode::Strict,
                pool,
                dim as u32,
                0,
                16,
                32,
                200,
            )
            .unwrap(),
        );
        assert!(file.is_finalized(), "recovered graph must be finalized");
        let g = crate::engine::vector::hnsw_paged::PagedHnsw::new(file);
        let got: Vec<u32> = g
            .search(&vecs[0], 5, 64)
            .unwrap()
            .into_iter()
            .map(|(r, _)| r.page_id)
            .collect();
        assert_eq!(
            got, want,
            "graph identical after deferred-WAL build + recovery"
        );
    }

    #[test]
    fn paged_unfinalized_build_is_not_trusted() {
        // A build that never calls `finalize_bulk_build` (an interrupted cold
        // build) must leave the file non-finalized so the load path rebuilds
        // it from the heap rather than trusting a partial graph. Even after
        // flushing pages to disk + reopening, `is_finalized` stays false.
        let dir = tempfile::tempdir().unwrap();
        let dim = 8usize;
        let vecs = gen_vectors(80, dim, 7);
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let path = dir.path().join("vec__1__partial.hvec");
        {
            let pool = test_pool(&log);
            let file = Arc::new(
                VectorIndexFile::open_with_durability(
                    path.clone(),
                    log.clone(),
                    DurabilityMode::Strict,
                    pool.clone(),
                    dim as u32,
                    0,
                    16,
                    32,
                    200,
                )
                .unwrap(),
            );
            let g = crate::engine::vector::hnsw_paged::PagedHnsw::new(file);
            g.begin_bulk_build();
            let mut rows: Vec<(RowId, Vec<f32>)> = vecs
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    (
                        RowId {
                            page_id: i as u32,
                            slot_id: 0,
                        },
                        v.clone(),
                    )
                })
                .collect();
            let mut session = g
                .build_session(1, 256 * 1024 * 1024, None, rows.len())
                .unwrap();
            g.insert_bulk_chunk_parallel(&mut rows, 1, &mut session)
                .unwrap();
            // NO finalize_bulk_build — simulate an interrupted build.
            pool.flush_all().unwrap();
        }
        let pool = test_pool(&log);
        let file = VectorIndexFile::open_with_durability(
            path,
            log,
            DurabilityMode::Strict,
            pool,
            dim as u32,
            0,
            16,
            32,
            200,
        )
        .unwrap();
        assert!(
            !file.is_finalized(),
            "an un-finalized build must not be trusted (forces heap rebuild)"
        );
    }

    #[test]
    fn meta_prefix_crc_matches_full_page_and_wal_hashes() {
        let mut seed = 0x1bad_f00d_1234_5678u64;
        for _ in 0..256 {
            let mut before = vec![0u8; PAGE_SIZE];
            for byte in &mut before[..VEC_META_USED_BYTES] {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                *byte = seed as u8;
            }
            write_u32(&mut before, 0, VEC_META_MAGIC);
            stamp_vector_page(&mut before, 0, seed.max(1));
            assert_eq!(read_u32(&before, 16), page_checksum(&before, 16));
            assert_eq!(vector_meta_crc(&before, false), crc32fast::hash(&before));
            let mut after = before.clone();
            after[96] ^= 1;
            stamp_vector_page(&mut after, 0, seed.wrapping_add(1));
            let normal = WalPayload::page_delta("test".into(), 0, 1, &before, &after);
            let fast = WalPayload::page_delta_prehashed("test".into(), 0, 1, &before, &after,
                vector_meta_crc(&before, false), vector_meta_crc(&after, false));
            assert_eq!(format!("{normal:?}"), format!("{fast:?}"));
        }
        let mut corrupt = encode_vector_meta(&VectorMeta::fresh(128, 0, 16, 32, 200));
        corrupt[PAGE_SIZE - 1] = 1;
        refresh_page_checksum(&mut corrupt);
        assert!(decode_vector_meta(&corrupt).is_err());
    }

    #[test]
    fn meta_encode_decode_round_trips() {
        let mut meta = VectorMeta::fresh(128, 0, 16, 32, 200);
        meta.entry_vid = vid_make(5, 2);
        meta.max_layer = 3;
        meta.node_next_page_id = 42;
        meta.node_count = 1000;
        meta.tombstone_count = 7;
        meta.nav_quant_valid = true;
        meta.nav_mid = 0.25;
        meta.nav_inv_scale = 123.5;
        meta.nav_dist_mul = 0.000_065;
        meta.nav_exact_nodes = 11;
        let buf = encode_vector_meta(&meta);
        let got = decode_vector_meta(&buf).unwrap();
        assert_eq!(got.dim, 128);
        assert_eq!(got.m, 16);
        assert_eq!(got.m0, 32);
        assert_eq!(got.ef_construction, 200);
        assert_eq!(got.entry_vid, vid_make(5, 2));
        assert_eq!(got.max_layer, 3);
        assert_eq!(got.node_next_page_id, 42);
        assert_eq!(got.node_count, 1000);
        assert_eq!(got.tombstone_count, 7);
        assert!(got.nav_quant_valid);
        assert_eq!(got.nav_mid, 0.25);
        assert_eq!(got.nav_inv_scale, 123.5);
        assert_eq!(got.nav_dist_mul, 0.000_065);
        assert_eq!(got.nav_exact_nodes, 11);
    }

    #[test]
    fn concurrent_entry_claim_is_unique_and_higher_layers_never_regress() {
        let dir = tempfile::tempdir().unwrap();
        let (graph, _) = build_paged(dir.path(), "entry-claim", 3, 0, &[]);
        let first = graph
            .file()
            .append_node(
                0,
                RowId {
                    page_id: 1,
                    slot_id: 0,
                },
                1,
                &[1.0, 0.0, 0.0],
            )
            .unwrap();
        let second = graph
            .file()
            .append_node(
                0,
                RowId {
                    page_id: 2,
                    slot_id: 0,
                },
                2,
                &[2.0, 0.0, 0.0],
            )
            .unwrap();
        let barrier = std::sync::Barrier::new(2);
        let claims = std::sync::atomic::AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for (id, level) in [(first, 1), (second, 2)] {
                let barrier = &barrier;
                let claims = &claims;
                let file = graph.file();
                scope.spawn(move || {
                    barrier.wait();
                    if file.claim_entry(0, id, level).unwrap().is_none() {
                        claims.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
        });
        assert_eq!(claims.load(Ordering::Relaxed), 1);
        graph.file().raise_entry(0, second, 2).unwrap();
        graph.file().raise_entry(0, first, 1).unwrap();
        assert_eq!(graph.file().entry_with_level().unwrap().1, 2);
    }
}

#[cfg(test)]
pub(crate) fn source_audit_vector_file() -> (tempfile::TempDir, Arc<VectorIndexFile>) {
    let dir = tempfile::tempdir().unwrap();
    let log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(dir.path()).unwrap(),
    ));
    let pool = BufferPool::new_with_options(
        128,
        log.clone(),
        DurabilityMode::Strict,
        std::time::Duration::from_millis(DEFAULT_LOCK_WAIT_MS),
        1,
        0,
    );
    let file = VectorIndexFile::open_with_durability(
        dir.path().join("audit.hvec"),
        log,
        DurabilityMode::Strict,
        pool,
        2,
        0,
        4,
        8,
        32,
    )
    .unwrap();
    (dir, Arc::new(file))
}

include!("vector_mapped.rs");
