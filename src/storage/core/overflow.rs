// Off-page storage for large BLOB/TEXT values ("LOB overflow pages").
//
// A large variable-length value that does not fit comfortably inline in
// a heap row is "externalized": its bytes are written to a per-table
// overflow file `tables/<table_id>.lob` as a singly-linked chain of
// 16KB pages, and the heap row stores only a fixed 20-byte `LobRef`
// descriptor pointing at the chain (see `row_codec.rs`).
//
// Page format (16KB; LSN @8 and checksum @16 deliberately match the
// heap/index page convention so `set_page_lsn` / `page_lsn` /
// `page_checksum` / `set_page_lsn_if_formatted` work unchanged):
//
//   off  field         type   notes
//   0    magic         u32    OVFL_MAGIC "MOV1"
//   4    version       u16    OVFL_VERSION
//   6    reserved      u16
//   8    page_lsn      u64    WAL LSN (redo idempotency)
//   16   checksum      u32    CRC32 over the page with this field zeroed
//   20   payload_len   u16    valid data bytes on this page
//   22   flags         u16    bit0 OVFL_FLAG_HEAD (chain head)
//   24   next_page_id  u64    next page in chain; 0 = chain tail
//   32   generation    u64    allocation identity; identical on every page
//   40   payload...           OVFL_PAGE_PAYLOAD bytes
//
// Page 0 is a metadata page: its payload holds `free_head` (head of the
// free-page singly-linked list; 0 = empty), `page_count` (the high-water
// mark, authoritative over the physical file length), and `next_generation`.
// All allocator pages (meta, free-list links, chain data) are written as
// redo-only system full page images (txn 0): the allocator is shared by every
// transaction of the table, so its history is always repeated by recovery and
// never undone per transaction (see `write_page_logged`).
//
// Allocation pops from the free list first, otherwise bumps the
// high-water mark. Freeing a newly allocated contiguous chain counts pages
// from its descriptor in O(1); legacy/non-contiguous chains first validate
// their next pointers. Relinking remains two page writes: the chain tail is
// attached to the current free_head and the chain head becomes free_head.
//
// LOB chains are immutable once written: UPDATE writes a brand-new
// chain and leaves the old one for older MVCC versions / undo; the old
// chain is freed only when its last referencing version is purged (or
// on rollback). See `mvcc.rs` / `undo.rs` for chain ownership.

const OVFL_MAGIC: u32 = 0x4d_4f_56_31; // "MOV1"
use crate::storage::format_versions::OVERFLOW_VERSION as OVFL_VERSION;
const OVFL_HEADER_SIZE: usize = 40;
const OVFL_PAGE_PAYLOAD: usize = PAGE_SIZE - OVFL_HEADER_SIZE; // 16344
const OVFL_FLAG_HEAD: u16 = 1 << 0;
/// M5 (LOB compression) — head page flags bit1: the entire chain's payload is the lz4-compressed
/// bytes. `flags` is an existing 16-bit field; when the chain head
/// carries this bit, `LobRef.total_len` is still the **original (decompressed) logical length**, and the sum of each
/// page's payload_len is the compressed byte count (obtained by collecting along next; no separate compressed length needs to be stored).
const OVFL_FLAG_COMPRESSED: u16 = 1 << 1;
/// The chain occupies the physical interval `[first_page_id,last_page_id]`
/// and every non-tail NEXT points to the following page. Old v2 chains have
/// this bit clear and retain linked-list traversal, so the addition is layout
/// compatible.
const OVFL_FLAG_CONTIGUOUS: u16 = 1 << 2;

fn lob_extent_scan_pages() -> u64 {
    static VALUE: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *VALUE.get_or_init(|| {
        std::env::var("MYDB_LOB_EXTENT_SCAN_PAGES")
            .ok()
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(8192)
    })
}

const OVFL_OFF_MAGIC: usize = 0;
const OVFL_OFF_VERSION: usize = 4;
const OVFL_OFF_PAYLOAD_LEN: usize = 20;
const OVFL_OFF_FLAGS: usize = 22;
const OVFL_OFF_NEXT: usize = 24;
const OVFL_OFF_GENERATION: usize = 32;

// Meta page (page 0) payload fields.
const OVFL_META_FREE_HEAD: usize = OVFL_HEADER_SIZE; // u64
const OVFL_META_PAGE_COUNT: usize = OVFL_HEADER_SIZE + 8; // u64 (high-water mark; >= 1)
const OVFL_META_NEXT_GENERATION: usize = OVFL_HEADER_SIZE + 16; // u64 (next allocation; >= 1)

/// Inline descriptor of an externalized LOB. Stored in the heap row's
/// value section (and in undo records) in place of the value bytes.
///
/// Overflow format v3 keeps three u32 fields and appends a u64 allocation
/// generation. Page ids are per-table and 16 KiB each, so u32 pages still address
/// roughly 64 TiB per table; `total_len` remains bounded by MySQL's
/// LONGTEXT/LONGBLOB 4 GiB column cap.
const LOB_DESCRIPTOR_LEN: usize = 20; // first u32 + last u32 + total_len u32 + generation u64

/// VarString/VarBytes values longer than this many bytes are stored
/// off-page in the overflow file instead of inline in the heap row.
/// M7 — now a **runtime-tunable** sysvar `mydb_lob_inline_max` (takes effect
/// immediately via `SET GLOBAL`); the `MYDB_LOB_INLINE_MAX` env var is the startup default (4096). Delegates to
/// the runtime knob in `dml.rs` (same `core` module, `include!`d into the same namespace).
/// The write path still externalizes a variable-length column additionally when the row cannot fit in a single page.
fn lob_inline_max() -> usize {
    lob_inline_max_runtime()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct LobRef {
    pub(crate) first_page_id: u64,
    pub(crate) last_page_id: u64,
    pub(crate) total_len: u64,
    pub(crate) generation: u64,
}

impl LobRef {
    fn write_to(&self, out: &mut Vec<u8>) -> Result<(), QueryError> {
        let first = u32::try_from(self.first_page_id).map_err(|_| {
            error(
                crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                "LOB first page id exceeds descriptor range",
            )
        })?;
        let last = u32::try_from(self.last_page_id).map_err(|_| {
            error(
                crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                "LOB last page id exceeds descriptor range",
            )
        })?;
        let total = u32::try_from(self.total_len).map_err(|_| {
            error(
                crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                "LOB total length exceeds descriptor range",
            )
        })?;
        out.extend_from_slice(&first.to_le_bytes());
        out.extend_from_slice(&last.to_le_bytes());
        out.extend_from_slice(&total.to_le_bytes());
        out.extend_from_slice(&self.generation.to_le_bytes());
        Ok(())
    }

    fn read_from(buf: &[u8]) -> Result<LobRef, QueryError> {
        if buf.len() < LOB_DESCRIPTOR_LEN {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt LOB descriptor",
            ));
        }
        Ok(LobRef {
            first_page_id: u32::from_le_bytes(buf[0..4].try_into().unwrap()) as u64,
            last_page_id: u32::from_le_bytes(buf[4..8].try_into().unwrap()) as u64,
            total_len: u32::from_le_bytes(buf[8..12].try_into().unwrap()) as u64,
            generation: u64::from_le_bytes(buf[12..20].try_into().unwrap()),
        })
    }
}

/// Build a per-column external mask (parallel to a row of `len`
/// columns) from a `(col_pos, LobRef)` list — used to re-externalize an
/// old version's chains when rollback restores it.
fn external_mask_from(refs: &[(usize, LobRef)], len: usize) -> Vec<Option<LobRef>> {
    let mut mask = vec![None; len];
    for (i, lob) in refs {
        if *i < len {
            mask[*i] = Some(*lob);
        }
    }
    mask
}

fn encode_overflow_page(flags: u16, next: u64, generation: u64, payload: &[u8]) -> Vec<u8> {
    debug_assert!(payload.len() <= OVFL_PAGE_PAYLOAD);
    let mut buf = vec![0u8; PAGE_SIZE];
    write_u32(&mut buf, OVFL_OFF_MAGIC, OVFL_MAGIC);
    write_u16(&mut buf, OVFL_OFF_VERSION, OVFL_VERSION);
    write_u16(&mut buf, OVFL_OFF_PAYLOAD_LEN, payload.len() as u16);
    write_u16(&mut buf, OVFL_OFF_FLAGS, flags);
    write_u64(&mut buf, OVFL_OFF_NEXT, next);
    write_u64(&mut buf, OVFL_OFF_GENERATION, generation);
    buf[OVFL_HEADER_SIZE..OVFL_HEADER_SIZE + payload.len()].copy_from_slice(payload);
    buf
}

fn encode_overflow_meta_page(
    free_head: u64,
    page_count: u64,
    next_generation: u64,
) -> Vec<u8> {
    let mut buf = vec![0u8; PAGE_SIZE];
    write_u32(&mut buf, OVFL_OFF_MAGIC, OVFL_MAGIC);
    write_u16(&mut buf, OVFL_OFF_VERSION, OVFL_VERSION);
    write_u64(&mut buf, OVFL_META_FREE_HEAD, free_head);
    write_u64(&mut buf, OVFL_META_PAGE_COUNT, page_count);
    write_u64(
        &mut buf,
        OVFL_META_NEXT_GENERATION,
        next_generation,
    );
    buf
}

/// Per-table overflow file handle. Reads/writes pages through the shared
/// buffer pool (so eviction / flush / checkpoint cover LOB pages for
/// free) and logs every page mutation as a redo-only full page image. The
/// owning `Database` serializes mutation through `with_overflow_mut`,
/// so `page_count` / `free_head` are a faithful cache of the on-disk
/// meta page while the handle is held.
#[derive(Debug)]
pub(crate) struct OverflowFile {
    path: PathBuf,
    /// Backing file handle, used only to `set_len` on extend. Opened
    /// lazily: a table whose rows never externalize a value never
    /// creates a `.lob` on disk, so installing a read materializer on a
    /// LOB-less scan is free. Page data I/O goes through the buffer
    /// pool's own handle, not this one.
    file: Option<File>,
    #[allow(dead_code)]
    table_id: u64,
    durability: DurabilityMode,
    /// High-water mark (authoritative; persisted in the meta page). 0
    /// means the file has not been initialized with a meta page yet.
    page_count: u64,
    free_head: u64,
    /// Monotone allocation identity. Generation 0 is reserved for metadata
    /// and can never identify a live chain.
    next_generation: u64,
    /// M3 (space shrink) — current page count of the free list. **Not persisted to on-disk meta** (no format
    /// version bump): computed once by walking the free list at open and cached, then maintained at runtime by
    /// `alloc_one_page` (-1) / `free_chain` (+chain length) / `shrink_to_fit`
    /// (-number of tail pages trimmed). Used for shrink decisions and M6 observability.
    free_count: u64,
    file_key: Arc<BufferPageFileKey>,
    buffer_pool: Arc<BufferPool>,
    log: Arc<crate::wal::LogMutex>,
    rel_path: String,
}

impl OverflowFile {
    fn open_with_buffer_pool(
        path: PathBuf,
        table_id: u64,
        log: Arc<crate::wal::LogMutex>,
        durability: DurabilityMode,
        buffer_pool: Arc<BufferPool>,
    ) -> Result<Self, QueryError> {
        // Open without creating: an absent `.lob` means the table has no
        // externalized values yet, and a read-only scan must not create
        // one. The file is created on first write (`ensure_file`).
        // Most tables never allocate overflow pages. Check existence before
        // resolving image capabilities, which walks the path's ancestors.
        let file = match fs::metadata(&path) {
            Ok(_) => match OpenOptions::new()
                .read(true)
                .write(!crate::storage::image_io::immutable_path(&path))
                .open(&path)
            {
                Ok(file) => Some(file),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                Err(e) => return Err(io_err(e)),
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(io_err(e)),
        };
        let len = match &file {
            Some(file) => file.metadata().map_err(io_err)?.len(),
            None => 0,
        };
        let file_key = Arc::new(BufferPool::overflow_file_key(path.clone(), table_id));
        let rel_path = crate::storage::naming::wal_relative_object_path(&path);
        let mut me = Self {
            path,
            file,
            table_id,
            durability,
            page_count: 0,
            free_head: 0,
            next_generation: 1,
            free_count: 0,
            file_key,
            buffer_pool,
            log,
            rel_path,
        };
        if len >= PAGE_SIZE as u64 {
            let meta = me.read_raw_page(0)?;
            if read_u32(&meta, OVFL_OFF_MAGIC) == OVFL_MAGIC {
                let version = read_u16(&meta, OVFL_OFF_VERSION);
                if version != OVFL_VERSION {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        format!(
                            "overflow format version {version} is incompatible with {OVFL_VERSION}; use a new data directory or rebuild data"
                        ),
                    ));
                }
                me.free_head = read_u64(&meta, OVFL_META_FREE_HEAD);
                me.page_count = read_u64(&meta, OVFL_META_PAGE_COUNT);
                me.next_generation = read_u64(&meta, OVFL_META_NEXT_GENERATION);
                if me.next_generation == 0 {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt overflow next generation",
                    ));
                }
                // M3 — free_count is not persisted to disk; recomputed by walking the free list at open.
                me.free_count = me.count_free_list()?;
            }
        }
        Ok(me)
    }

    /// M3 (space shrink) — walks the free list to count the current free pages (recomputes `free_count`
    /// at open; also for test verification). Has cycle detection, bounded by `page_count`.
    fn count_free_list(&self) -> Result<u64, QueryError> {
        let mut n: u64 = 0;
        let mut pid = self.free_head;
        let mut guard: u64 = 0;
        while pid != 0 {
            n += 1;
            let next = self.buffer_pool.with_raw_page(self.page_key(pid), |page| {
                if page.len() != PAGE_SIZE
                    || read_u32(page, OVFL_OFF_MAGIC) != OVFL_MAGIC
                    || read_u16(page, OVFL_OFF_VERSION) != OVFL_VERSION
                {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt overflow page in free list",
                    ));
                }
                Ok(read_u64(page, OVFL_OFF_NEXT))
            })?;
            pid = next;
            guard += 1;
            if guard > self.page_count + 2 {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt overflow free list (cycle)",
                ));
            }
        }
        Ok(n)
    }

    fn page_key(&self, page_id: u64) -> PageKey {
        PageKey {
            file: self.file_key.clone(),
            page_id,
        }
    }

    /// Drop this file's frames from the buffer pool without flushing —
    /// used by DROP/TRUNCATE before the backing `.lob` is deleted, so a
    /// later table reusing the id does not read stale cached pages.
    fn discard_without_flush(&mut self) {
        let _ = self.buffer_pool.discard_file(&self.file_key);
        self.durability = DurabilityMode::Memory;
    }

    fn read_raw_page(&self, page_id: u64) -> Result<Vec<u8>, QueryError> {
        self.buffer_pool.read_raw_page(self.page_key(page_id))
    }

    /// Create the backing file on first write if it was opened lazily.
    fn ensure_file(&mut self) -> Result<&mut File, QueryError> {
        if self.file.is_none() {
            if let Some(parent) = self.path.parent() {
                fs::create_dir_all(parent).map_err(io_err)?;
            }
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&self.path)
                .map_err(io_err)?;
            self.file = Some(file);
        }
        Ok(self.file.as_mut().expect("file just created"))
    }

    fn set_file_len(&mut self, page_count: u64) -> Result<(), QueryError> {
        crate::storage::temp_quota::reserve_extent(&self.path, page_count * PAGE_SIZE as u64)?;
        let file = self.ensure_file()?;
        file.set_len(page_count * PAGE_SIZE as u64).map_err(io_err)
    }

    /// Write `after` (a full 16KB page image) to `page_id` as a redo-only
    /// system write (txn 0, full page image).
    ///
    /// The allocator's pages are shared by every transaction writing LOBs to
    /// this table, so they must never be logged under a user transaction:
    /// recovery skips REDO of aborted transactions and undoes losers with
    /// full-page before-images. Either one rewinds the meta page or a free
    /// list link over writes that other, committed transactions (or a
    /// rollback's own txn-0 free) made afterwards, leaving the free list
    /// pointing at never-written pages or at live chains. Allocation is
    /// therefore always repeated by redo; a chain whose owning row is undone
    /// after a crash is merely unreferenced space, never corruption.
    fn write_page_logged(&mut self, page_id: u64, mut after: Vec<u8>) -> Result<(), QueryError> {
        debug_assert_eq!(after.len(), PAGE_SIZE);
        if self.durability == DurabilityMode::Memory {
            refresh_page_checksum(&mut after);
            self.buffer_pool
                .store_page(self.page_key(page_id), after, 0)?;
            return Ok(());
        }
        let lsn = {
            let mut log = self.log.lock();
            let lsn = log.reserve_lsn()?;
            set_page_lsn(&mut after, lsn);
            refresh_page_checksum(&mut after);
            log.append_reserved(
                lsn,
                0,
                WalPayload::full_page_image(self.rel_path.clone(), page_id, after.clone()),
            )?;
            lsn
        };
        self.buffer_pool
            .store_page(self.page_key(page_id), after, lsn)?;
        Ok(())
    }

    fn persist_meta(&mut self) -> Result<(), QueryError> {
        let after =
            encode_overflow_meta_page(self.free_head, self.page_count, self.next_generation);
        self.write_page_logged(0, after)
    }

    fn ensure_initialized(&mut self) -> Result<(), QueryError> {
        if self.page_count >= 1 {
            return Ok(());
        }
        self.set_file_len(1)?;
        self.page_count = 1;
        self.free_head = 0;
        self.next_generation = 1;
        let meta = encode_overflow_meta_page(self.free_head, self.page_count, self.next_generation);
        self.write_page_logged(0, meta)
    }

    /// Reserve one page for a chain: pop the free list, else extend the file.
    fn alloc_one_page(&mut self) -> Result<u64, QueryError> {
        if self.free_head != 0 {
            let pid = self.free_head;
            let page = self.read_raw_page(pid)?;
            self.free_head = read_u64(&page, OVFL_OFF_NEXT);
            // M3 — pop one page from the free list: free_count -1.
            self.free_count = self.free_count.saturating_sub(1);
            Ok(pid)
        } else {
            let pid = self.page_count;
            let new_count = self.page_count.checked_add(1).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                    "overflow file is full",
                )
            })?;
            self.set_file_len(new_count)?;
            self.page_count = new_count;
            Ok(pid)
        }
    }

    /// M4 (contiguous extent allocation) — reserves pages for a chain of `n_pages` pages.
    ///
    /// When `n_pages >= extent_min` and extents are enabled, first try to reuse a likewise-contiguous
    /// and long-enough run at the free-list head; a typical LOB UPDATE/purge hangs the previous generation's
    /// entire extent back onto the chain head as-is, so space can be reclaimed steadily
    /// without sacrificing sequential layout. Only when the chain head does not qualify does it bump one whole
    /// contiguous run `[page_count, page_count+n_pages)` from the high-water mark. Otherwise it falls back to page-by-page `alloc_one_page`
    /// (free-list-first, preserving fragmentation recovery / small chains).
    ///
    /// Interaction with M3: contiguous reuse pops `free_head` / `free_count` in lockstep; the high-water
    /// path only bumps `page_count`. Both paths preserve the accounting invariants of `shrink_to_fit` /
    /// `count_free_list`.
    fn alloc_pages_for_chain(&mut self, n_pages: usize) -> Result<Vec<u64>, QueryError> {
        let extent_min = lob_extent_min_pages();
        let use_extent = extent_min > 0 && n_pages as u64 >= extent_min;
        if use_extent {
            if let Some(reused) = self.try_alloc_contiguous_free_extent(n_pages)? {
                return Ok(reused);
            }
            // Contiguous run: bump n_pages pages from the high-water mark in one go.
            let start = self.page_count;
            let new_count = start.checked_add(n_pages as u64).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                    "overflow file is full",
                )
            })?;
            self.set_file_len(new_count)?;
            self.page_count = new_count;
            return Ok((start..new_count).collect());
        }
        // Admit the whole non-contiguous allocation before consuming any free
        // pages; a quota rejection must not strand a half-allocated LOB chain.
        let growth = (n_pages as u64).saturating_sub(self.free_count);
        crate::storage::temp_quota::reserve_extent(
            &self.path,
            self.page_count
                .saturating_add(growth)
                .saturating_mul(PAGE_SIZE as u64),
        )?;
        let mut allocated = Vec::with_capacity(n_pages);
        for _ in 0..n_pages {
            allocated.push(self.alloc_one_page()?);
        }
        Ok(allocated)
    }

    /// Pop exactly `n_pages` from the free list when those pages form a
    /// physically ascending extent.  This is intentionally O(n).
    fn try_alloc_contiguous_free_extent(
        &mut self,
        n_pages: usize,
    ) -> Result<Option<Vec<u64>>, QueryError> {
        if n_pages == 0 || self.free_head == 0 || self.free_count < n_pages as u64 {
            return Ok(None);
        }

        let mut pid = self.free_head;
        let mut previous = 0u64;
        let mut run_predecessor = 0u64;
        let mut pages: Vec<u64> = Vec::with_capacity(n_pages);
        let mut scanned = 0u64;
        while pid != 0 && scanned < self.free_count.min(lob_extent_scan_pages()) {
            let page = self.read_raw_page(pid)?;
            if page.len() != PAGE_SIZE || read_u32(&page, OVFL_OFF_MAGIC) != OVFL_MAGIC {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt overflow page in free extent",
                ));
            }
            let next = read_u64(&page, OVFL_OFF_NEXT);
            if pages
                .last()
                .is_some_and(|last| pid != last.saturating_add(1))
            {
                pages.clear();
                run_predecessor = previous;
            }
            pages.push(pid);
            if pages.len() == n_pages {
                if run_predecessor == 0 {
                    self.free_head = next;
                } else {
                    let predecessor_before = self.read_raw_page(run_predecessor)?;
                    if predecessor_before.len() != PAGE_SIZE
                        || read_u32(&predecessor_before, OVFL_OFF_MAGIC) != OVFL_MAGIC
                    {
                        return Err(error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "corrupt overflow free-extent predecessor",
                        ));
                    }
                    let mut predecessor_after = predecessor_before.clone();
                    write_u64(&mut predecessor_after, OVFL_OFF_NEXT, next);
                    self.write_page_logged(run_predecessor, predecessor_after)?;
                }
                self.free_count -= n_pages as u64;
                return Ok(Some(pages));
            }
            previous = pid;
            pid = next;
            scanned += 1;
        }
        Ok(None)
    }

    /// M5 (LOB compression) — writes a new chain; `total_len` is always `data`'s **original logical
    /// length**. When `compressible` and `MYDB_LOB_COMPRESSION=lz4`, the original length is >=
    /// `MYDB_LOB_COMPRESS_MIN_BYTES`, **and lz4 compression really is smaller**, the chain stores
    /// compressed bytes and the head is flagged `OVFL_FLAG_COMPRESSED`; otherwise it is stored as-is (avoiding a net loss).
    /// The chain's page count follows the **actually written (possibly compressed) bytes**, so compressed chains use fewer pages
    /// (consistent with M3 shrink / M4 extent thresholds — both look at actual page counts).
    fn allocate_chain_compressible(
        &mut self,
        data: &[u8],
        compressible: bool,
    ) -> Result<LobRef, QueryError> {
        self.ensure_initialized()?;
        let total_len = data.len() as u64; // original logical length (written into LobRef)

        // Compression decision: only attempted when enabled + column compressible + threshold met; if it does not shrink, store uncompressed.
        let mut compressed_buf: Option<Vec<u8>> = None;
        if compressible && lob_compression_lz4() && data.len() >= lob_compress_min_bytes() {
            let c = lz4_flex::compress(data);
            if c.len() < data.len() {
                compressed_buf = Some(c);
            }
        }
        let (stored, compressed_flag) = match &compressed_buf {
            Some(c) => (c.as_slice(), OVFL_FLAG_COMPRESSED),
            None => (data, 0),
        };
        // Note: the chain is split by `stored.len()` (possibly compressed); empty data still occupies 1 page.
        let n_pages = stored.len().div_ceil(OVFL_PAGE_PAYLOAD).max(1);
        let (generation, allocated) = self.reserve_chain_pages(n_pages)?;
        // The meta page already records the reservation, so a failure while
        // writing the chain below only leaks these pages.
        let contiguous = allocated
            .windows(2)
            .all(|pair| pair[1] == pair[0].saturating_add(1));
        for (i, &pid) in allocated.iter().enumerate() {
            let start = i * OVFL_PAGE_PAYLOAD;
            let end = ((i + 1) * OVFL_PAGE_PAYLOAD).min(stored.len());
            let next = allocated.get(i + 1).copied().unwrap_or(0);
            // The head page carries the HEAD bit + compression bit; subsequent pages get 0.
            let flags = if i == 0 {
                OVFL_FLAG_HEAD
                    | compressed_flag
                    | if contiguous { OVFL_FLAG_CONTIGUOUS } else { 0 }
            } else {
                0
            };
            let after = encode_overflow_page(flags, next, generation, &stored[start..end]);
            self.write_page_logged(pid, after)?;
        }
        Ok(LobRef {
            first_page_id: allocated[0],
            last_page_id: allocated[n_pages - 1],
            total_len, // original length, not the compressed one
            generation,
        })
    }

    /// Take a generation and `n_pages` pages for a new chain and persist the
    /// meta page before any page is overwritten with chain data. Pages popped
    /// from the free list are then no longer reachable from it, whatever
    /// happens to the chain writes. BUG-6 (docs/lob-management-audit-2026-07.md):
    /// a failure before the meta write restores the in-memory allocator, which
    /// still matches the durable meta page.
    fn reserve_chain_pages(&mut self, n_pages: usize) -> Result<(u64, Vec<u64>), QueryError> {
        let saved_free_head = self.free_head;
        let saved_page_count = self.page_count;
        let saved_next_generation = self.next_generation;
        let saved_free_count = self.free_count;
        let result = (|| {
            let generation = self.next_generation;
            self.next_generation = self.next_generation.checked_add(1).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                    "overflow allocation generation is exhausted",
                )
            })?;
            let allocated = self.alloc_pages_for_chain(n_pages)?;
            self.persist_meta()?;
            Ok((generation, allocated))
        })();
        if result.is_err() {
            self.free_head = saved_free_head;
            self.page_count = saved_page_count;
            self.next_generation = saved_next_generation;
            self.free_count = saved_free_count;
        }
        result
    }

    /// Return a freed chain to the free list in two page writes (relink
    /// the chain tail to the current free_head, then point free_head at
    /// the chain head). No-op for a null/empty descriptor.
    fn free_chain(&mut self, lob: LobRef) -> Result<(), QueryError> {
        if lob.first_page_id == 0 {
            return Ok(());
        }
        self.ensure_initialized()?;
        if lob.first_page_id >= self.page_count
            || lob.last_page_id >= self.page_count
        {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt overflow chain descriptor on free",
            ));
        }
        // A purge/rollback retry may present the same immutable descriptor
        // more than once. Relinking an already-free one-page chain used to set
        // `page.next = free_head = page`, creating a persistent self-cycle;
        // longer chains could similarly close a multi-page cycle. Treat an
        // already-listed head as an idempotent completion. The free list is
        // authoritative here: an allocated chain can never legitimately have
        // its head in that list.
        if self.free_list_contains(lob.first_page_id)? {
            return Ok(());
        }
        // M3/M5 — first **walk this chain** along next to count its actual pages (for free_count), and this
        // must happen before the relink: at this point the chain's internal next pointers are intact (head→…→tail,
        // tail.next=0), the walk stops at the tail and never crosses into the free list.
        //
        // Why the page count cannot be derived from `total_len` (a fix to the original M3 approach): a compressed chain's
        // (M5) actual page count follows its **compressed** bytes, while `total_len` is the **original** length,
        // and the two differ → using total_len would overestimate free_count. Walking is the only source that is correct for both compressed
        // and uncompressed chains. The cost is N extra page reads at free time (already in the buffer pool,
        // cheap); the O(1) two-page-write relink is unchanged.
        let head = self.read_raw_page(lob.first_page_id)?;
        if head.len() != PAGE_SIZE
            || read_u32(&head, OVFL_OFF_MAGIC) != OVFL_MAGIC
            || read_u16(&head, OVFL_OFF_VERSION) != OVFL_VERSION
        {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt overflow chain head on free",
            ));
        }
        // The page id may now belong to a newer allocation. A stale undo
        // descriptor is cleanup metadata, not corruption: generation
        // mismatch makes the free an idempotent no-op and is the core ABA
        // safety property of overflow v3.
        if read_u64(&head, OVFL_OFF_GENERATION) != lob.generation {
            return Ok(());
        }
        if read_u16(&head, OVFL_OFF_FLAGS) & OVFL_FLAG_HEAD == 0 {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt overflow chain head on free",
            ));
        }
        let chain_pages = if read_u16(&head, OVFL_OFF_FLAGS) & OVFL_FLAG_CONTIGUOUS != 0 {
            if lob.last_page_id < lob.first_page_id {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt contiguous overflow chain descriptor",
                ));
            }
            lob.last_page_id - lob.first_page_id + 1
        } else {
            self.walk_chain_page_count(lob.first_page_id, lob.last_page_id, lob.generation)?
        };
        let old_free_head = self.free_head;
        let before_last = self.read_raw_page(lob.last_page_id)?;
        if before_last.len() != PAGE_SIZE || read_u32(&before_last, OVFL_OFF_MAGIC) != OVFL_MAGIC {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt overflow page on free",
            ));
        }
        if read_u16(&before_last, OVFL_OFF_VERSION) != OVFL_VERSION
            || read_u64(&before_last, OVFL_OFF_GENERATION) != lob.generation
        {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt overflow chain generation on free",
            ));
        }
        // BUG-5 (docs/lob-management-audit-2026-07.md) — a genuine chain
        // tail has `NEXT == 0`. If `last_page_id` is corrupt and points at
        // some *other* chain's interior page, relinking its NEXT to the
        // free head would sever that live chain (and leak this one's real
        // tail). Refuse to relink a page that is not a true tail rather
        // than corrupt an in-use chain.
        if read_u64(&before_last, OVFL_OFF_NEXT) != 0 {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt overflow chain (last page is not a tail)",
            ));
        }
        let mut after_last = before_last.clone();
        write_u64(&mut after_last, OVFL_OFF_NEXT, old_free_head);
        self.write_page_logged(lob.last_page_id, after_last)?;
        self.free_head = lob.first_page_id;
        self.free_count = self.free_count.saturating_add(chain_pages);
        self.persist_meta()
    }

    fn free_list_contains(&self, target: u64) -> Result<bool, QueryError> {
        let mut pid = self.free_head;
        let mut guard = 0u64;
        while pid != 0 {
            if pid == target {
                return Ok(true);
            }
            if pid >= self.page_count {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt overflow free list page id",
                ));
            }
            pid = self.buffer_pool.with_raw_page(self.page_key(pid), |page| {
                if page.len() != PAGE_SIZE
                    || read_u32(page, OVFL_OFF_MAGIC) != OVFL_MAGIC
                    || read_u16(page, OVFL_OFF_VERSION) != OVFL_VERSION
                {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt overflow page in free list",
                    ));
                }
                Ok(read_u64(page, OVFL_OFF_NEXT))
            })?;
            guard = guard.saturating_add(1);
            if guard > self.page_count.saturating_add(2) {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt overflow free list (cycle)",
                ));
            }
        }
        Ok(false)
    }

    /// Counts a chain's actual pages by walking next, from `first` until **`last` (the chain's known tail
    /// page) is reached and it stops** — rather than walking until next==0. This is the key: `free_chain` relinks the chain tail to
    /// the old free_head, so freed chains are strung head-to-tail; stopping only at next==0 would cross into
    /// the free list and count unrelated trailing pages (an early M3 bug). Stopping at the known `last` counts only this chain.
    /// Has cycle detection. Only called while the chain-internal first→last pointers are intact (before the relink).
    fn walk_chain_page_count(
        &self,
        first: u64,
        last: u64,
        generation: u64,
    ) -> Result<u64, QueryError> {
        let mut n: u64 = 0;
        let mut pid = first;
        let mut guard: u64 = 0;
        let mut reached_last = false;
        while pid != 0 {
            n += 1;
            if pid == last {
                reached_last = true;
                break; // reached this chain's tail page: stop, do not cross into the free list.
            }
            let next = self.buffer_pool.with_raw_page(self.page_key(pid), |page| {
                if page.len() != PAGE_SIZE
                    || read_u32(page, OVFL_OFF_MAGIC) != OVFL_MAGIC
                    || read_u16(page, OVFL_OFF_VERSION) != OVFL_VERSION
                    || read_u64(page, OVFL_OFF_GENERATION) != generation
                {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt overflow page on free",
                    ));
                }
                Ok(read_u64(page, OVFL_OFF_NEXT))
            })?;
            pid = next;
            guard += 1;
            if guard > self.page_count + 2 {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt overflow chain (cycle)",
                ));
            }
        }
        if !reached_last {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt overflow chain (last page is unreachable)",
            ));
        }
        Ok(n)
    }

    /// M3 (space shrink) — returns the contiguous free pages at the tail back to the OS.
    ///
    /// Algorithm:
    ///   1. Walk the free list to build the set of "which pages are free" (O(free_count)).
    ///   2. From `page_count-1` downward, find the longest contiguous free suffix (page 0 is the meta page,
    ///      never shrunk; below the `min_pages` threshold nothing is done, to avoid frequent set_len churn).
    ///   3. Rebuild the free list, skipping the removed tail pages (write surviving pages' NEXT, set the new
    ///      free_head).
    ///   4. Lower page_count / free_count, **persisting meta through WAL first**.
    ///   5. **Then** physically truncate via `set_len`, and drop the buffer frames of truncated pages.
    ///
    /// Crash-safety ordering argument (meta WAL before set_len):
    ///   * **Crash before truncation (before WAL persistence)**: the relink/meta WAL records are not durable,
    ///     after recovery page_count/free_head/free_count are all the old values and the free list still
    ///     contains the tail pages — the state is consistent, just not shrunk; harmless.
    ///   * **meta WAL durable, crash before set_len**: recovery replays relink+meta,
    ///     page_count is already lowered but the physical file is longer — the surplus tail bytes are ignored
    ///     (`page_count` is authoritative over file length) and it can shrink again next time;
    ///     meta can never point at a physically deleted page. Harmless.
    ///   * **Crash after set_len or in any later step beyond meta**: meta already matches the physical length
    ///     (a shorter physical file is also harmless, since page_count is authoritative); the free list no longer
    ///     contains the truncated pages, so they can never be allocated again. Consistent.
    /// Returns the number of pages actually shrunk (returned to disk).
    fn shrink_to_fit(&mut self, min_pages: u64) -> Result<u64, QueryError> {
        if self.page_count <= 1 || self.free_count == 0 {
            return Ok(0);
        }
        // 1) free list → set.
        let mut free_set: std::collections::HashSet<u64> =
            std::collections::HashSet::with_capacity(self.free_count as usize);
        {
            let mut pid = self.free_head;
            let mut guard: u64 = 0;
            while pid != 0 {
                free_set.insert(pid);
                let next = self.buffer_pool.with_raw_page(self.page_key(pid), |page| {
                    if page.len() != PAGE_SIZE || read_u32(page, OVFL_OFF_MAGIC) != OVFL_MAGIC {
                        return Err(error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "corrupt overflow page in free list",
                        ));
                    }
                    Ok(read_u64(page, OVFL_OFF_NEXT))
                })?;
                pid = next;
                guard += 1;
                if guard > self.page_count + 2 {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt overflow free list (cycle)",
                    ));
                }
            }
        }
        // 2) From the tail downward, find the longest contiguous free suffix (page 0 untouched).
        let mut new_page_count = self.page_count;
        while new_page_count > 1 && free_set.contains(&(new_page_count - 1)) {
            new_page_count -= 1;
        }
        let shrink_pages = self.page_count - new_page_count;
        // Opportunistic: skip when the suffix is below the threshold (min_pages>=1).
        if shrink_pages == 0 || shrink_pages < min_pages.max(1) {
            return Ok(0);
        }
        // The set of removed tail pages [new_page_count, page_count).
        let truncated: std::collections::HashSet<u64> = (new_page_count..self.page_count).collect();
        // 3) Rebuild the free list: keep the pages not in truncated, re-string them in the original order.
        //    Collect the surviving pages (order does not matter; correctness only requires the chain to be connected and free of removed pages).
        let survivors: Vec<u64> = {
            let mut out = Vec::new();
            let mut pid = self.free_head;
            let mut guard: u64 = 0;
            while pid != 0 {
                if !truncated.contains(&pid) {
                    out.push(pid);
                }
                let next = self
                    .buffer_pool
                    .with_raw_page(self.page_key(pid), |page| Ok(read_u64(page, OVFL_OFF_NEXT)))?;
                pid = next;
                guard += 1;
                if guard > self.page_count + 2 {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt overflow free list (cycle)",
                    ));
                }
            }
            out
        };
        let old_page_count = self.page_count;
        // Re-string the surviving pages into a chain: survivors[i].next = survivors[i+1], tail 0.
        // Each NEXT rewrite goes through WAL as a full page image. Only the NEXT field is changed;
        // the payload is untouched (these are free pages; their content is meaningless).
        for i in 0..survivors.len() {
            let pid = survivors[i];
            let before = self.read_raw_page(pid)?;
            if before.len() != PAGE_SIZE {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt overflow page during shrink",
                ));
            }
            let next = if i + 1 < survivors.len() {
                survivors[i + 1]
            } else {
                0
            };
            // Write only when NEXT actually changes, saving pointless WAL.
            if read_u64(&before, OVFL_OFF_NEXT) != next {
                let mut after = before.clone();
                write_u64(&mut after, OVFL_OFF_NEXT, next);
                self.write_page_logged(pid, after)?;
            }
        }
        let new_free_head = survivors.first().copied().unwrap_or(0);
        // 4) Update in-memory metadata + persist meta through WAL (must precede set_len).
        self.free_head = new_free_head;
        self.page_count = new_page_count;
        self.free_count = self.free_count.saturating_sub(shrink_pages);
        self.persist_meta()?;
        // Memory mode has no WAL; persist_meta lands directly in the buffer. Under strict/relaxed the
        // meta WAL record is already appended here. To ensure "meta is durable before set_len",
        // flush the WAL once before truncating (a no-op for memory).
        if self.durability != DurabilityMode::Memory {
            self.log.lock().flush()?;
        }
        // 5) Physical truncation + drop the buffer frames of truncated pages (prevents page_id reuse from reading stale frames).
        self.set_file_len(new_page_count)?;
        for pid in new_page_count..old_page_count {
            let _ = self.buffer_pool.discard_page(&self.page_key(pid));
        }
        Ok(shrink_pages)
    }

    /// M6 CHECK TABLE (docs/lob-management-audit-2026-07.md) — structural
    /// fsck of the `.lob` file: the free list is cycle-free, and every
    /// physical page is a well-formed overflow page (valid magic; data
    /// pages carry an in-range `payload_len`). Complements the row-driven
    /// chain walk the caller does by materializing live rows: this catches
    /// free-list / meta corruption that no live `LobRef` points at.
    /// Returns `Ok(())` for a table with no `.lob` yet.
    fn validate_structure(&self) -> Result<(), QueryError> {
        if self.page_count == 0 {
            return Ok(());
        }
        // Free-list walk (cycle-checked inside `count_free_list`).
        let free = self.count_free_list()?;
        if free != self.free_count {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                format!(
                    "overflow free-list count {free} disagrees with cached {}",
                    self.free_count
                ),
            ));
        }
        for pid in 0..self.page_count {
            self.buffer_pool.with_raw_page(self.page_key(pid), |page| {
                if page.len() != PAGE_SIZE
                    || read_u32(page, OVFL_OFF_MAGIC) != OVFL_MAGIC
                    || read_u16(page, OVFL_OFF_VERSION) != OVFL_VERSION
                {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        format!("overflow page {pid} has invalid magic"),
                    ));
                }
                if pid != 0 {
                    let plen = read_u16(page, OVFL_OFF_PAYLOAD_LEN) as usize;
                    if plen > OVFL_PAGE_PAYLOAD {
                        return Err(error(
                            crate::error::catalog::ER_GET_ERRNO,
                            format!("overflow page {pid} payload_len {plen} out of range"),
                        ));
                    }
                }
                Ok(())
            })?;
        }
        Ok(())
    }

    /// M3 — read-only snapshot: current free-list page count (M6 observability / test verification).
    fn free_count(&self) -> u64 {
        self.free_count
    }

    /// M3 — read-only snapshot: high-water-mark page count (including meta page 0).
    fn page_count(&self) -> u64 {
        self.page_count
    }

    /// M4 (in-chain prefetch) — before reading a chain, prefetch its pages into the buffer pool to hide the per-page synchronous
    /// `with_raw_page` access latency. With extent allocation a large chain's page ids are contiguous
    /// `[first, first+n_pages)`, so the buffer pool's contiguous-range prefetch
    /// `prefetch_range` is reused directly (internally it skips lookups of resident pages and throttles under scan pressure,
    /// so it does not pollute the OLTP working set). For a fragmented chain the range may contain pages outside this chain
    /// — but they are all valid overflow pages, merely warming the cache, **not affecting correctness** (the actual read
    /// still follows the next pointers). `MYDB_LOB_PREFETCH_DISABLE` / Memory mode /
    /// a zero prefetch window make the whole thing a no-op.
    fn prefetch_chain(&self, lob: LobRef, flags: u16) {
        if lob.first_page_id == 0 || lob.total_len == 0 || lob_prefetch_disabled() {
            return;
        }
        let n_pages = if flags & OVFL_FLAG_COMPRESSED != 0 {
            if flags & OVFL_FLAG_CONTIGUOUS == 0 {
                return;
            }
            lob.last_page_id
                .saturating_sub(lob.first_page_id)
                .saturating_add(1)
        } else {
            (lob.total_len as usize).div_ceil(OVFL_PAGE_PAYLOAD).max(1) as u64
        };
        if n_pages <= 1 {
            return; // a single-page chain gains nothing from prefetch.
        }
        let start = lob.first_page_id;
        // Clamp to the file's high-water mark to avoid out-of-range page ids.
        let end = start.saturating_add(n_pages).min(self.page_count);
        if end > start {
            self.buffer_pool.prefetch_range(&self.file_key, start, end);
        }
    }

    /// M5 — reads the head page flags to tell whether the chain is lz4-compressed (for read_chain_range's
    /// compressed fallback branch). A null chain returns false.
    fn chain_flags(&self, lob: LobRef) -> Result<u16, QueryError> {
        if lob.first_page_id == 0 {
            return Ok(0);
        }
        self.buffer_pool
            .with_raw_page(self.page_key(lob.first_page_id), |page| {
                if page.len() != PAGE_SIZE
                    || read_u32(page, OVFL_OFF_MAGIC) != OVFL_MAGIC
                    || read_u16(page, OVFL_OFF_VERSION) != OVFL_VERSION
                    || read_u64(page, OVFL_OFF_GENERATION) != lob.generation
                {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt overflow head page or generation",
                    ));
                }
                let flags = read_u16(page, OVFL_OFF_FLAGS);
                if flags & OVFL_FLAG_HEAD == 0 {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt overflow chain (first page is not a head)",
                    ));
                }
                Ok(flags)
            })
    }

    /// Read a full chain back into a contiguous byte buffer. Each page
    /// is borrowed from the buffer pool (`with_raw_page`) rather than
    /// cloned — copying a 1 MiB LOB used to cost 64 extra 16 KiB
    /// alloc+memcpy round trips just to throw each page away after
    /// extracting its payload slice.
    fn read_chain(&self, lob: LobRef) -> Result<Vec<u8>, QueryError> {
        let total = lob.total_len as usize;
        let flags = self.chain_flags(lob)?;
        // M4: batch-prefetch the whole chain (under extents the contiguous run is read ahead in one go).
        self.prefetch_chain(lob, flags);
        // M5: collect the **entire stored stream** along next (the sum of each page's payload_len).
        // Uncompressed chain: the stored stream is the original bytes and its length should == total_len (keeping the old
        // "shorter than descriptor" check). Compressed chain: stored is the lz4 byte stream,
        // its length < total_len, so total_len cannot serve as remaining for an early stop — the full
        // chain must be collected before decompressing. The head page's flags decide whether it is compressed.
        let initial_capacity = if flags & OVFL_FLAG_COMPRESSED != 0 {
            if flags & OVFL_FLAG_CONTIGUOUS != 0 {
                (lob.last_page_id - lob.first_page_id + 1)
                    .saturating_mul(OVFL_PAGE_PAYLOAD as u64)
                    .min(total as u64) as usize
            } else {
                total.min(OVFL_PAGE_PAYLOAD)
            }
        } else {
            total
        };
        let mut scratch_charge =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                initial_capacity,
            )?;
        let mut stored = Vec::new();
        stored.try_reserve_exact(initial_capacity).map_err(|_| {
            error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "out of memory while reading LOB chain",
            )
        })?;
        let mut pid = lob.first_page_id;
        let mut guard: u64 = 0;
        let mut compressed = false;
        let mut first = true;
        while pid != 0 {
            let next = self.buffer_pool.with_raw_page(self.page_key(pid), |page| {
                if page.len() != PAGE_SIZE
                    || read_u32(page, OVFL_OFF_MAGIC) != OVFL_MAGIC
                    || read_u16(page, OVFL_OFF_VERSION) != OVFL_VERSION
                    || read_u64(page, OVFL_OFF_GENERATION) != lob.generation
                {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt overflow page or generation in chain",
                    ));
                }
                if first {
                    compressed = read_u16(page, OVFL_OFF_FLAGS) & OVFL_FLAG_COMPRESSED != 0;
                }
                let plen = read_u16(page, OVFL_OFF_PAYLOAD_LEN) as usize;
                // BUG-3 (docs/lob-management-audit-2026-07.md) — a
                // corrupt page (bit-flip / torn write) that keeps the
                // magic but inflates `payload_len` past the payload
                // region would slice out of bounds and PANIC. Reject
                // it as a 1030 corruption like every other check here,
                // never letting a bad page reach the slice.
                if plen > OVFL_PAGE_PAYLOAD {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt overflow page (payload_len)",
                    ));
                }
                let required = stored.len().saturating_add(plen);
                if required > stored.capacity() {
                    let growth = required - stored.capacity();
                    scratch_charge.try_grow(growth)?;
                    if stored.try_reserve_exact(growth).is_err() {
                        scratch_charge.shrink(growth);
                        return Err(error(
                            crate::error::catalog::ER_OUTOFMEMORY,
                            "out of memory while growing compressed LOB buffer",
                        ));
                    }
                }
                stored.extend_from_slice(&page[OVFL_HEADER_SIZE..OVFL_HEADER_SIZE + plen]);
                Ok(read_u64(page, OVFL_OFF_NEXT))
            })?;
            first = false;
            pid = next;
            guard += 1;
            // Replay can grow the file after this reader was opened. Chains
            // fill every non-tail page and compression only shrinks payloads,
            // so the descriptor bounds traversal without stale allocator state.
            if guard
                > lob
                    .total_len
                    .div_ceil(OVFL_PAGE_PAYLOAD as u64)
                    .saturating_add(2)
            {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt overflow chain (cycle)",
                ));
            }
        }
        if compressed {
            // BUG-4 (docs/lob-management-audit-2026-07.md) — `total` comes
            // from the (possibly corrupt) `LobRef.total_len`. `decompress`
            // pre-allocates `total` bytes, so a corrupt descriptor could
            // force an up-to-4 GiB allocation (DoS/OOM). The lz4 block
            // format expands by at most ~255× (a run compresses to
            // ~len/255 bytes), so a decompressed size far exceeding
            // `stored.len() * 256` cannot be genuine — reject before
            // allocating rather than trusting the length check afterwards.
            let max_plausible = stored.len().saturating_mul(256).saturating_add(1024);
            if total > max_plausible {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt compressed LOB length",
                ));
            }
            // Decompress and verify the length == total_len (the original logical length).
            let _output_admission =
                crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                    total,
                )?;
            let out = lz4_flex::decompress(&stored, total).map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt compressed LOB chain",
                )
            })?;
            if out.len() != total {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "compressed LOB length mismatch",
                ));
            }
            Ok(out)
        } else {
            // Uncompressed: the stored stream length should exactly equal total_len.
            if stored.len() != total {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "overflow chain shorter than descriptor",
                ));
            }
            Ok(stored)
        }
    }

    /// M2 (partial materialization) — reads only the chain's `[offset, offset+len)` byte range, walking the
    /// next pointers but skipping the pages before the range (payload is not extended until the range is entered),
    /// copying only the overlapping part once inside, and stopping early once `len` bytes are gathered. The range is
    /// clamped by `total_len`: `offset >= total_len` returns empty; a short tail returns whatever is actually
    /// readable. Reuses `read_chain`'s zero-copy page borrowing (`with_raw_page`) and
    /// cycle detection. `SUBSTRING/LEFT/RIGHT` push down to this, avoiding pulling in the whole chain.
    //
    // M5 limitation: **compressed chains cannot be partially read** — lz4 compresses the whole chain, so any range
    // requires decompressing the entire chain first. Hence, when the head carries `OVFL_FLAG_COMPRESSED`, this
    // degrades to "read_chain (full-chain read+decompress) + byte slicing"; only uncompressed chains take the page-level
    // partial-read optimization below. Compression and partial materialization trade off: compression saves space/total IO, partial reads save
    // the pages of a single substring fetch — pick one per column workload (compression off by default; the partial-read optimization is retained).
    // Currently driven by `Database::lob_column_substring` and tests; SQL-layer pushdown
    // (planner→scan) is the next wave of work, so lib (non-test) does not call it directly yet.
    #[allow(dead_code)]
    fn read_chain_range(&self, lob: LobRef, offset: u64, len: u64) -> Result<Vec<u8>, QueryError> {
        let total = lob.total_len;
        // Range clamp: offset out of bounds or len==0 → empty.
        if offset >= total || len == 0 {
            return Ok(Vec::new());
        }
        // M5: compressed chains cannot be partially read → full-chain read+decompress, then slice.
        let flags = self.chain_flags(lob)?;
        if flags & OVFL_FLAG_COMPRESSED != 0 {
            let full = self.read_chain(lob)?;
            let s = offset as usize;
            let e = offset.saturating_add(len).min(total) as usize;
            return Ok(full[s..e].to_vec());
        }
        let start = offset as usize;
        // End upper bound (exclusive), clamped by total to guard against offset+len overflow.
        let end = offset.saturating_add(len).min(total) as usize;
        debug_assert!(start < end);
        let want = end - start;
        let mut out = Vec::with_capacity(want);

        // M4: prefetch only the pages covering the range (precise read-ahead for contiguous extent chains; for fragmented chains
        // the page-id range may contain pages outside this chain — mere cache warming, no correctness impact).
        if !lob_prefetch_disabled() && lob.first_page_id != 0 {
            let first_pg = (start / OVFL_PAGE_PAYLOAD) as u64; // ordinal of the page holding the range start
            let last_pg = ((end - 1) / OVFL_PAGE_PAYLOAD) as u64; // ordinal of the page holding the range end
            if last_pg > first_pg {
                let ps = lob.first_page_id.saturating_add(first_pg);
                let pe = lob
                    .first_page_id
                    .saturating_add(last_pg + 1)
                    .min(self.page_count);
                if pe > ps {
                    self.buffer_pool.prefetch_range(&self.file_key, ps, pe);
                }
            }
        }

        let first_page_ordinal = (start / OVFL_PAGE_PAYLOAD) as u64;
        let mut pid = if flags & OVFL_FLAG_CONTIGUOUS != 0 {
            lob.first_page_id.saturating_add(first_page_ordinal)
        } else {
            lob.first_page_id
        };
        // `consumed` = the number of bytes already "logically crossed" (= the sum of the previous pages' payloads).
        let mut consumed: usize = if flags & OVFL_FLAG_CONTIGUOUS != 0 {
            (first_page_ordinal as usize).saturating_mul(OVFL_PAGE_PAYLOAD)
        } else {
            0
        };
        let mut guard: u64 = 0;
        while pid != 0 && out.len() < want {
            let next = self.buffer_pool.with_raw_page(self.page_key(pid), |page| {
                if page.len() != PAGE_SIZE
                    || read_u32(page, OVFL_OFF_MAGIC) != OVFL_MAGIC
                    || read_u16(page, OVFL_OFF_VERSION) != OVFL_VERSION
                    || read_u64(page, OVFL_OFF_GENERATION) != lob.generation
                {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt overflow page or generation in chain",
                    ));
                }
                let plen = read_u16(page, OVFL_OFF_PAYLOAD_LEN) as usize;
                // BUG-3 — same out-of-bounds guard as `read_chain`.
                if plen > OVFL_PAGE_PAYLOAD {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt overflow page (payload_len)",
                    ));
                }
                let page_start = consumed; // logical start of this page's payload
                let page_end = consumed + plen; // logical end of this page's payload (exclusive)
                consumed = page_end;
                // Overlap of this page with the requested range [start, end).
                let lo = page_start.max(start);
                let hi = page_end.min(end);
                if lo < hi {
                    let in_page_off = lo - page_start; // in-page start offset
                    let n = hi - lo;
                    out.extend_from_slice(
                        &page[OVFL_HEADER_SIZE + in_page_off..OVFL_HEADER_SIZE + in_page_off + n],
                    );
                }
                Ok(read_u64(page, OVFL_OFF_NEXT))
            })?;
            pid = next;
            guard += 1;
            // Replay can grow the file after this reader was opened. Chains
            // fill every non-tail page and compression only shrinks payloads,
            // so the descriptor bounds traversal without stale allocator state.
            if guard
                > lob
                    .total_len
                    .div_ceil(OVFL_PAGE_PAYLOAD as u64)
                    .saturating_add(2)
            {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt overflow chain (cycle)",
                ));
            }
        }
        if out.len() != want {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "overflow chain shorter than descriptor",
            ));
        }
        Ok(out)
    }
}

/// `LobMaterializer` that reads externalized columns back from a table's
/// overflow file. Installed via `with_lob_materializer` around a heap
/// read/scan by the `Database` layer; holds the shared `OverflowFile`
/// handle (not a `Database` borrow) so it satisfies the `'static +
/// Send + Sync` thread-local bound.
#[derive(Debug)]
pub(crate) struct OverflowReader {
    overflow: OverflowHandle,
}

impl OverflowReader {
    pub(crate) fn new(overflow: OverflowHandle) -> Self {
        Self { overflow }
    }
}

/// Wraps a span of overflow bytes into a Value per the column kind. VarString goes through UTF-8 validation;
/// the rest hold the bytes directly. Shared by `materialize` and `materialize_range`.
fn overflow_bytes_to_value(kind: &ColumnKind, bytes: Vec<u8>) -> Result<Value, QueryError> {
    Ok(match kind {
        ColumnKind::VarString => Value::String(String::from_utf8(bytes).map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "invalid utf8 in externalized TEXT",
            )
        })?),
        ColumnKind::Json => Value::Json(bytes),
        ColumnKind::Geometry => Value::Geometry(bytes),
        ColumnKind::Vector => Value::Vector(bytes),
        _ => Value::Bytes(bytes),
    })
}

impl LobMaterializer for OverflowReader {
    fn materialize(
        &self,
        _col_pos: usize,
        kind: &ColumnKind,
        lob: LobRef,
    ) -> Result<Value, QueryError> {
        #[cfg(test)]
        LOB_FULL_MATERIALIZATIONS_FOR_TEST.fetch_add(1, Ordering::Relaxed);
        let bytes = self.overflow.read().read_chain(lob)?;
        overflow_bytes_to_value(kind, bytes)
    }

    // `materialize_len` uses the trait default implementation (returns total_len, zero page reads).

    fn materialize_range(
        &self,
        _col_pos: usize,
        kind: &ColumnKind,
        lob: LobRef,
        offset: u64,
        len: u64,
    ) -> Result<Value, QueryError> {
        // M2: read only the pages covering the range (skipping the pages outside it), saving IO.
        let bytes = self.overflow.read().read_chain_range(lob, offset, len)?;
        // Slicing VarString bytes may cut a multi-byte character in half; the sliced-out substring goes
        // through lossy here (the pushdown side is responsible for byte semantics; the oracle goes through slice_value_bytes,
        // likewise lossy, keeping the two consistent).
        Ok(match kind {
            ColumnKind::VarString => Value::String(String::from_utf8_lossy(&bytes).into_owned()),
            ColumnKind::Json => Value::Json(bytes),
            ColumnKind::Geometry => Value::Geometry(bytes),
            ColumnKind::Vector => Value::Vector(bytes),
            _ => Value::Bytes(bytes),
        })
    }
}

/// `LobMaterializer` that does NOT fetch chains: it records each
/// externalized column's `(col_pos, LobRef)` and returns `Value::Null`
/// as a placeholder. Used to capture an old row version's chain
/// descriptors into an undo record (so undo never holds multi-GB
/// bytes) and to learn which chains an inserted row owns when rolling
/// it back.
#[derive(Debug, Default)]
pub(crate) struct LobRefCollector {
    refs: Mutex<Vec<(usize, LobRef)>>,
}

impl LobRefCollector {
    pub(crate) fn new() -> Self {
        Self {
            refs: Mutex::new(Vec::new()),
        }
    }

    /// Drain the collected `(col_pos, LobRef)` pairs.
    pub(crate) fn take(&self) -> Vec<(usize, LobRef)> {
        self.refs
            .lock()
            .map(|mut v| std::mem::take(&mut *v))
            .unwrap_or_default()
    }
}

impl LobMaterializer for LobRefCollector {
    fn materialize(
        &self,
        col_pos: usize,
        _kind: &ColumnKind,
        lob: LobRef,
    ) -> Result<Value, QueryError> {
        if let Ok(mut refs) = self.refs.lock() {
            refs.push((col_pos, lob));
        }
        Ok(Value::Null)
    }
}
#[cfg(test)]
static LOB_FULL_MATERIALIZATIONS_FOR_TEST: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
pub(crate) fn lob_full_materializations_for_test() -> u64 {
    LOB_FULL_MATERIALIZATIONS_FOR_TEST.load(Ordering::Relaxed)
}
