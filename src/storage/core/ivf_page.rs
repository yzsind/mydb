// Page-based IVF_SQ8 vector index storage (`.ivf`).
//
// The storage substrate for the paged IVF_SQ8 backend: it lays the inverted
// file out in 16 KiB buffer-pool pages so an IVF index participates in the
// buffer pool (demand-paged, evicted under the `Vector` soft quota, supporting
// larger-than-memory indexes) and the WAL (crash-safe physical redo) exactly
// like the B-tree (`btree.rs`) and the paged HNSW store (`vector_page.rs`) —
// which this module mirrors closely on purpose.
//
// ## On-disk layout
//
// One file, four page kinds, all carrying the standard 24-byte page header so
// they flow through the shared LSN/checksum helpers (`set_page_lsn_if_formatted`,
// `page_checksum`) and the generic `redo_page_update` recovery path with **no
// IVF-specific recovery code**:
//
// ```text
//   off  0  magic        u32   (page-kind specific, registered in bytes.rs)
//   off  4  page_version u16   IVF_PAGE_VERSION
//   off  6  page_kind    u8    0=meta 1=centroid 2=directory 3=list
//   off  7  flags        u8
//   off  8  PAGE_LSN     u64   <-- MUST be offset 8 (page_lsn contract)
//   off 16  checksum     u32   (page_checksum treats these 4 bytes as 0)
//   off 20  reserved     u32
//   off 24  ... payload ...
// ```
//
// - **Meta page** (page 0): index parameters, global SQ8 quantization params,
//   centroid/directory page ranges, allocator high-water, counters, and the
//   `trained` / `finalized` flags.
// - **Centroid pages**: the `nlist·dim` f32 coarse-quantizer centroids, packed
//   row-major and split across as many pages as needed. Loaded resident on
//   open (nlist is small — `~√N`).
// - **Directory pages**: one entry per inverted list — `(head_page, tail_page,
//   count)` — locating each list's page chain. Loaded resident on open.
// - **List pages**: a per-list singly-linked chain of data pages. Each page
//   packs its entries into four contiguous fixed-capacity regions — codes,
//   SQ8 squared norms, RowIds, tombstone bits — so the codes of one page are
//   one contiguous run,
//   which is exactly the shape the SME batch kernel and the SQ8 int8 SSD loop
//   want. Entry `i`'s SQ8 code (or raw f32 vector, for Dot / SME-L2) lives at
//   `codes[i]`, its RowId at `rids[i]`, its tombstone bit at `tombs[i]`.
//
// ## Correctness contract (identical to HNSW)
//
// The index is a candidate *generator*: `search` returns an approximate
// superset, and the executor re-reads every candidate under MVCC and re-ranks
// by the live heap f32 vector. So a half-applied incremental insert (redo-only
// physical records, `txn_id = 0`) or a skipped corrupt page only ever costs
// recall — never correctness — and the heap stays the durable record. A
// non-`finalized` file is an interrupted cold build and is discarded + rebuilt
// from the heap upstream.

/// File-format version (gates the whole `.ivf` file, like `INDEX_VERSION`).
pub(crate) use crate::storage::format_versions::PAGED_IVF_INDEX_VERSION as IVF_INDEX_VERSION;
/// Page-format version (per-page, like `INDEX_PAGE_VERSION`).
pub(crate) use crate::storage::format_versions::PAGED_IVF_PAGE_VERSION as IVF_PAGE_VERSION;
/// Meta page id.
const IVF_META_PAGE: u64 = 0;

// Page magics — "IV.." (0x49_56..). EVERY magic here must also be registered in
// the two `match` arms of `bytes.rs` (`refresh_page_checksum`,
// `set_page_lsn_if_formatted`); otherwise LSN stamping / checksum refresh
// silently no-op for `.ivf` pages and the recovery LSN gate never sticks.
pub(crate) const IVF_META_MAGIC: u32 = 0x49_56_4d_31; // "IVM1"
pub(crate) const IVF_CENT_MAGIC: u32 = 0x49_56_43_31; // "IVC1"
pub(crate) const IVF_DIR_MAGIC: u32 = 0x49_56_44_31; // "IVD1"
pub(crate) const IVF_LIST_MAGIC: u32 = 0x49_56_4c_31; // "IVL1"

const IVF_PAGE_KIND_META: u8 = 0;
const IVF_PAGE_KIND_CENT: u8 = 1;
const IVF_PAGE_KIND_DIR: u8 = 2;
const IVF_PAGE_KIND_LIST: u8 = 3;

/// Standard page header size; payload starts here.
const IVF_HEADER_BYTES: usize = 24;

// Meta page field offsets (payload starts at 24; laid out u64-aligned).
const IVF_META_OFF_INDEX_VERSION: usize = 24;
const IVF_META_OFF_PAGE_SIZE: usize = 28;
const IVF_META_OFF_DIM: usize = 32;
const IVF_META_OFF_METRIC: usize = 36;
const IVF_META_OFF_STORES_F32: usize = 37;
const IVF_META_OFF_TRAINED: usize = 38;
const IVF_META_OFF_FINALIZED: usize = 39;
const IVF_META_OFF_NLIST: usize = 40;
const IVF_META_OFF_NPROBE: usize = 44;
const IVF_META_OFF_MID: usize = 48;
const IVF_META_OFF_INV_SCALE: usize = 52;
const IVF_META_OFF_DIST_MUL: usize = 56;
const IVF_META_OFF_CENT_FIRST: usize = 64;
const IVF_META_OFF_CENT_COUNT: usize = 72;
const IVF_META_OFF_DIR_FIRST: usize = 80;
const IVF_META_OFF_DIR_COUNT: usize = 88;
const IVF_META_OFF_NEXT_PAGE: usize = 96;
const IVF_META_OFF_TOTAL: usize = 104;
const IVF_META_OFF_LIVE: usize = 112;
const IVF_META_OFF_INSERTS: usize = 120;

// Directory entry: (head_page u64, tail_page u64, count u64).
const IVF_DIR_ENTRY_BYTES: usize = 24;

// List page payload offsets.
const IVF_LIST_OFF_NEXT: usize = 24; // u64 next page in chain (0 = tail)
const IVF_LIST_OFF_COUNT: usize = 32; // u16 entry count on this page
const IVF_LIST_OFF_LISTID: usize = 34; // u32 owning list id (diagnostics / scan)
const IVF_LIST_OFF_DATA: usize = 38; // start of the codes region

#[inline]
fn read_ivf_row(bytes: &[u8], offset: usize) -> RowId {
    RowId {
        page_id: read_u32(bytes, offset),
        slot_id: read_u16(bytes, offset + 4),
    }
}

#[inline]
fn write_ivf_row(bytes: &mut [u8], offset: usize, row: RowId) {
    write_u32(bytes, offset, row.page_id);
    write_u16(bytes, offset + 4, row.slot_id);
}

/// One inverted list's page-chain locator (resident copy of a directory entry).
#[derive(Debug, Clone, Copy, Default)]
struct ListDir {
    head: u64,
    tail: u64,
    count: u64,
}

/// In-memory image of the meta page.
#[derive(Debug, Clone)]
pub(crate) struct IvfMeta {
    pub dim: u32,
    pub metric: u8,
    pub stores_f32: bool,
    pub trained: bool,
    pub finalized: bool,
    pub nlist: u32,
    pub nprobe_default: u32,
    pub mid: f32,
    pub inv_scale: f32,
    pub dist_mul: f32,
    pub cent_first_page: u64,
    pub cent_page_count: u64,
    pub dir_first_page: u64,
    pub dir_page_count: u64,
    /// Allocator high-water: the next page id to hand out.
    pub next_page_id: u64,
    pub total_entries: u64,
    pub live_entries: u64,
    pub inserts_since_train: u64,
}

impl IvfMeta {
    fn fresh(dim: u32, metric: u8, stores_f32: bool) -> Self {
        IvfMeta {
            dim,
            metric,
            stores_f32,
            trained: false,
            finalized: false,
            nlist: 0,
            nprobe_default: 0,
            mid: 0.0,
            inv_scale: 1.0,
            dist_mul: 1.0,
            cent_first_page: 0,
            cent_page_count: 0,
            dir_first_page: 0,
            dir_page_count: 0,
            next_page_id: 1, // page 0 is meta
            total_entries: 0,
            live_entries: 0,
            inserts_since_train: 0,
        }
    }
}

fn encode_ivf_meta(meta: &IvfMeta) -> Vec<u8> {
    let mut buf = vec![0u8; PAGE_SIZE];
    write_u32(&mut buf, 0, IVF_META_MAGIC);
    write_u16(&mut buf, 4, IVF_PAGE_VERSION);
    buf[6] = IVF_PAGE_KIND_META;
    write_u32(&mut buf, IVF_META_OFF_INDEX_VERSION, IVF_INDEX_VERSION);
    write_u32(&mut buf, IVF_META_OFF_PAGE_SIZE, PAGE_SIZE as u32);
    write_u32(&mut buf, IVF_META_OFF_DIM, meta.dim);
    buf[IVF_META_OFF_METRIC] = meta.metric;
    buf[IVF_META_OFF_STORES_F32] = u8::from(meta.stores_f32);
    buf[IVF_META_OFF_TRAINED] = u8::from(meta.trained);
    buf[IVF_META_OFF_FINALIZED] = u8::from(meta.finalized);
    write_u32(&mut buf, IVF_META_OFF_NLIST, meta.nlist);
    write_u32(&mut buf, IVF_META_OFF_NPROBE, meta.nprobe_default);
    write_u32(&mut buf, IVF_META_OFF_MID, meta.mid.to_bits());
    write_u32(&mut buf, IVF_META_OFF_INV_SCALE, meta.inv_scale.to_bits());
    write_u32(&mut buf, IVF_META_OFF_DIST_MUL, meta.dist_mul.to_bits());
    write_u64(&mut buf, IVF_META_OFF_CENT_FIRST, meta.cent_first_page);
    write_u64(&mut buf, IVF_META_OFF_CENT_COUNT, meta.cent_page_count);
    write_u64(&mut buf, IVF_META_OFF_DIR_FIRST, meta.dir_first_page);
    write_u64(&mut buf, IVF_META_OFF_DIR_COUNT, meta.dir_page_count);
    write_u64(&mut buf, IVF_META_OFF_NEXT_PAGE, meta.next_page_id);
    write_u64(&mut buf, IVF_META_OFF_TOTAL, meta.total_entries);
    write_u64(&mut buf, IVF_META_OFF_LIVE, meta.live_entries);
    write_u64(&mut buf, IVF_META_OFF_INSERTS, meta.inserts_since_train);
    let checksum = page_checksum(&buf, 16);
    write_u32(&mut buf, 16, checksum);
    buf
}

fn decode_ivf_meta(buf: &[u8]) -> Result<IvfMeta, QueryError> {
    if buf.len() != PAGE_SIZE {
        return Err(error(crate::error::catalog::ER_GET_ERRNO, "corrupt ivf index meta page length"));
    }
    if read_u32(buf, 0) != IVF_META_MAGIC {
        return Err(error(crate::error::catalog::ER_GET_ERRNO, "corrupt ivf index meta page magic"));
    }
    if read_u16(buf, 4) != IVF_PAGE_VERSION {
        return Err(error(crate::error::catalog::ER_GET_ERRNO, "unsupported ivf index meta page version"));
    }
    let stored = read_u32(buf, 16);
    let actual = page_checksum(buf, 16);
    if stored != actual {
        return Err(error(crate::error::catalog::ER_GET_ERRNO, "corrupt ivf index meta page checksum"));
    }
    if read_u32(buf, IVF_META_OFF_INDEX_VERSION) != IVF_INDEX_VERSION
        || read_u32(buf, IVF_META_OFF_PAGE_SIZE) != PAGE_SIZE as u32
    {
        return Err(error(crate::error::catalog::ER_GET_ERRNO, "unsupported ivf index file version"));
    }
    Ok(IvfMeta {
        dim: read_u32(buf, IVF_META_OFF_DIM),
        metric: buf[IVF_META_OFF_METRIC],
        stores_f32: buf[IVF_META_OFF_STORES_F32] != 0,
        trained: buf[IVF_META_OFF_TRAINED] != 0,
        finalized: buf[IVF_META_OFF_FINALIZED] != 0,
        nlist: read_u32(buf, IVF_META_OFF_NLIST),
        nprobe_default: read_u32(buf, IVF_META_OFF_NPROBE),
        mid: f32::from_bits(read_u32(buf, IVF_META_OFF_MID)),
        inv_scale: f32::from_bits(read_u32(buf, IVF_META_OFF_INV_SCALE)),
        dist_mul: f32::from_bits(read_u32(buf, IVF_META_OFF_DIST_MUL)),
        cent_first_page: read_u64(buf, IVF_META_OFF_CENT_FIRST),
        cent_page_count: read_u64(buf, IVF_META_OFF_CENT_COUNT),
        dir_first_page: read_u64(buf, IVF_META_OFF_DIR_FIRST),
        dir_page_count: read_u64(buf, IVF_META_OFF_DIR_COUNT),
        next_page_id: read_u64(buf, IVF_META_OFF_NEXT_PAGE),
        total_entries: read_u64(buf, IVF_META_OFF_TOTAL),
        live_entries: read_u64(buf, IVF_META_OFF_LIVE),
        inserts_since_train: read_u64(buf, IVF_META_OFF_INSERTS),
    })
}

fn format_page_header(buf: &mut [u8], magic: u32, kind: u8) {
    for b in buf.iter_mut() {
        *b = 0;
    }
    write_u32(buf, 0, magic);
    write_u16(buf, 4, IVF_PAGE_VERSION);
    buf[6] = kind;
}

#[inline]
fn valid_ivf_page(buf: &[u8], magic: u32, kind: u8) -> bool {
    buf.len() == PAGE_SIZE
        && read_u32(buf, 0) == magic
        && read_u16(buf, 4) == IVF_PAGE_VERSION
        && buf[6] == kind
}

/// A borrowed, per-page view of one list page's four contiguous regions,
/// handed to `for_each_list_page`'s callback while the page is pinned. `codes`
/// is `count * stride` bytes of contiguous SQ8 codes (or f32 vectors) — the
/// shape the SME batch / int8 SSD loops want. `rids` / `tombs` are the parallel
/// norm, RowId, and tombstone regions.
pub(crate) struct IvfListPageRef<'a> {
    pub codes: &'a [u8],
    pub norms: &'a [u8],
    pub rids: &'a [u8],
    pub tombs: &'a [u8],
    pub count: usize,
}

impl<'a> IvfListPageRef<'a> {
    #[inline]
    pub(crate) fn rid_at(&self, i: usize) -> RowId {
        read_ivf_row(self.rids, i * 6)
    }
    #[inline]
    pub(crate) fn norm_at(&self, i: usize) -> i32 {
        read_u32(self.norms, i * 4) as i32
    }
    #[inline]
    pub(crate) fn tomb_at(&self, i: usize) -> bool {
        self.tombs[i / 8] & (1 << (i & 7)) != 0
    }
}

/// The paged IVF_SQ8 index file. Mirrors [`VectorIndexFile`] but stores an
/// inverted file instead of a graph.
#[derive(Debug)]
pub(crate) struct IvfIndexFile {
    #[allow(dead_code)] // kept for Debug / diagnostics; IO addresses via file_key/rel_path
    path: PathBuf,
    file: parking_lot::Mutex<Option<File>>,
    log: Arc<crate::wal::LogMutex>,
    durability: DurabilityMode,
    file_key: Arc<BufferPageFileKey>,
    buffer_pool: Arc<BufferPool>,
    rel_path: String,
    meta_state: parking_lot::Mutex<IvfMetaState>,
    /// Resident coarse-quantizer centroids (`nlist·dim` f32), loaded on open.
    centroids: parking_lot::RwLock<Arc<Vec<f32>>>,
    /// Resident directory (one locator per list), loaded on open.
    directory: parking_lot::RwLock<Vec<ListDir>>,
    /// Serialises destructive layout initialization only. Incremental appends
    /// use one latch per inverted list, so unrelated cells scale independently.
    layout_lock: parking_lot::Mutex<()>,
    list_latches: parking_lot::RwLock<Vec<Arc<parking_lot::Mutex<()>>>>,
    dir_page_latches: parking_lot::RwLock<Vec<Arc<parking_lot::Mutex<()>>>>,
    /// Meta is one physical page; serialize its WAL before-image/write sequence
    /// while keeping list-page work outside this short critical section.
    meta_flush_lock: parking_lot::Mutex<()>,
    dim_fixed: usize,
    /// Bytes per stored entry payload: `dim` (SQ8) or `dim*4` (raw f32).
    stride: usize,
    /// Entries per list page (fixed by `stride`).
    cap_per_page: usize,
    stores_f32: bool,
    /// Bulk-build (deferred-WAL) mode, mirroring the HNSW store: page writes
    /// mutate the buffer pool in place without per-edit WAL; `finalize_build`
    /// flushes the dirty frames and logs one meta commit record.
    build_mode: AtomicBool,
}

#[derive(Debug)]
struct IvfMetaState {
    meta: IvfMeta,
    meta_image: Vec<u8>,
    /// Page ids from the extend-file branch of `alloc_page` whose first write
    /// can skip the before-image (no torn disk image exists yet).
    pristine_pages: HashSet<u64>,
}

impl IvfIndexFile {
    /// Entries that fit on one list page for a given payload `stride`. Each
    /// entry costs `stride` code bytes + 4-byte SQ8 norm + the exact 6-byte
    /// RowId; tombstones are a trailing bitset. The compact metadata keeps
    /// 768-d SQ8 at 21 entries/page even after adding norms.
    fn cap_for_stride(stride: usize) -> usize {
        let mut cap = ((PAGE_SIZE - IVF_LIST_OFF_DATA) / (stride + 10)).max(1);
        while cap > 1
            && IVF_LIST_OFF_DATA + cap * (stride + 10) + cap.div_ceil(8) > PAGE_SIZE
        {
            cap -= 1;
        }
        cap.max(1)
    }

    /// Open (or create) the `.ivf` file. A new file lays down an untrained meta
    /// page; an existing file is validated against `dim`/`metric` (a parameter
    /// change forces a rebuild upstream) and its centroids + directory loaded
    /// resident. `stores_f32` is advisory for a *new* file only — an existing
    /// file's persisted `stores_f32` governs its on-disk layout regardless of
    /// the current SME env.
    #[allow(private_interfaces)]
    pub(crate) fn open_with_durability(
        path: PathBuf,
        log: Arc<crate::wal::LogMutex>,
        durability: DurabilityMode,
        buffer_pool: Arc<BufferPool>,
        dim: u32,
        metric: u8,
        stores_f32: bool,
    ) -> Result<Self, QueryError> {
        let mut file = if durability == DurabilityMode::Memory {
            None
        } else {
            Some(
                crate::storage::image_io::open_data_file(&path)
                    .map_err(io_err)?,
            )
        };
        let len = file
            .as_ref()
            .map(|f| f.metadata().map_err(io_err).map(|m| m.len()))
            .transpose()?
            .unwrap_or(0);
        if len > 0 && len < PAGE_SIZE as u64 {
            return Err(error(crate::error::catalog::ER_GET_ERRNO, "corrupt ivf index file length"));
        }
        if len % PAGE_SIZE as u64 != 0 {
            return Err(error(crate::error::catalog::ER_GET_ERRNO,
                "corrupt ivf index file has a truncated trailing page",
            ));
        }
        let initialized = len >= PAGE_SIZE as u64;
        let (mut meta, mut meta_image) = if initialized {
            let page = read_index_page(
                file.as_mut()
                    .expect("ivf index file should be open outside memory mode"),
                IVF_META_PAGE,
            )?;
            let meta = decode_ivf_meta(&page)?;
            if meta.dim != dim || meta.metric != metric {
                return Err(error(crate::error::catalog::ER_GET_ERRNO,
                    "ivf index parameters changed; rebuild required",
                ));
            }
            (meta, page)
        } else {
            let meta = IvfMeta::fresh(dim, metric, stores_f32);
            (meta.clone(), encode_ivf_meta(&meta))
        };
        let file_pages = len / PAGE_SIZE as u64;
        let allocator_corrected = initialized && meta.next_page_id < file_pages;
        if allocator_corrected {
            // A crash between extending a page and flushing the meta high-water
            // must never cause that physical page id to be reused.
            meta.next_page_id = file_pages;
            meta_image = encode_ivf_meta(&meta);
        }
        let file_key = Arc::new(BufferPool::vector_index_file_key(path.clone()));
        let rel_path = Self::compute_relative_path(&path);
        let dim_fixed = meta.dim as usize;
        let effective_stores_f32 = meta.stores_f32;
        let stride = if effective_stores_f32 {
            dim_fixed * 4
        } else {
            dim_fixed
        };
        let cap_per_page = Self::cap_for_stride(stride);
        let initial_nlist = meta.nlist;
        let initial_dir_pages = meta.dir_page_count;
        let this = Self {
            path,
            file: parking_lot::Mutex::new(file),
            log,
            durability,
            file_key,
            buffer_pool,
            rel_path,
            meta_state: parking_lot::Mutex::new(IvfMetaState {
                meta,
                meta_image,
                pristine_pages: HashSet::new(),
            }),
            centroids: parking_lot::RwLock::new(Arc::new(Vec::new())),
            directory: parking_lot::RwLock::new(Vec::new()),
            layout_lock: parking_lot::Mutex::new(()),
            list_latches: parking_lot::RwLock::new(
                (0..initial_nlist)
                    .map(|_| Arc::new(parking_lot::Mutex::new(())))
                    .collect(),
            ),
            dir_page_latches: parking_lot::RwLock::new(
                (0..initial_dir_pages)
                    .map(|_| Arc::new(parking_lot::Mutex::new(())))
                    .collect(),
            ),
            meta_flush_lock: parking_lot::Mutex::new(()),
            dim_fixed,
            stride,
            cap_per_page,
            stores_f32: effective_stores_f32,
            build_mode: AtomicBool::new(false),
        };
        if !initialized {
            this.write_meta_page(txn_id_for_ddl())?;
        } else if this.meta().trained {
            this.load_centroids()?;
            this.load_directory()?;
            this.validate_resident_layout(file_pages)?;
        }
        if allocator_corrected {
            this.write_meta_page(txn_id_for_ddl())?;
        }
        Ok(this)
    }

    fn validate_resident_layout(&self, file_pages: u64) -> Result<(), QueryError> {
        let meta = self.meta();
        let nlist = meta.nlist as usize;
        if nlist == 0 || meta.next_page_id == 0 || meta.next_page_id < file_pages {
            return Err(error(crate::error::catalog::ER_GET_ERRNO, "corrupt ivf meta allocator or nlist"));
        }
        let cent_per_page = (PAGE_SIZE - IVF_HEADER_BYTES) / 4;
        let expected_cent = (nlist.saturating_mul(self.dim_fixed)).div_ceil(cent_per_page) as u64;
        let dir_per_page = (PAGE_SIZE - IVF_HEADER_BYTES) / IVF_DIR_ENTRY_BYTES;
        let expected_dir = nlist.div_ceil(dir_per_page) as u64;
        let range_valid = |first: u64, count: u64, expected: u64| {
            count == expected
                && first >= 1
                && first.checked_add(count).is_some_and(|end| end <= file_pages)
        };
        if !range_valid(meta.cent_first_page, meta.cent_page_count, expected_cent)
            || !range_valid(meta.dir_first_page, meta.dir_page_count, expected_dir)
        {
            return Err(error(crate::error::catalog::ER_GET_ERRNO, "corrupt ivf centroid/directory range"));
        }
        let dir = self.directory.read();
        for entry in dir.iter() {
            let empty = entry.head == 0 && entry.tail == 0 && entry.count == 0;
            if empty {
                continue;
            }
            if entry.head == 0
                || entry.tail == 0
                || entry.count == 0
                || entry.head >= file_pages
                || entry.tail >= file_pages
            {
                return Err(error(crate::error::catalog::ER_GET_ERRNO, "corrupt ivf list directory entry"));
            }
        }
        Ok(())
    }

    fn compute_relative_path(path: &Path) -> String {
        crate::storage::naming::wal_relative_object_path(path)
    }

    pub(crate) fn relative_path(&self) -> String {
        self.rel_path.clone()
    }

    fn page_key(&self, page_id: u64) -> PageKey {
        PageKey {
            file: self.file_key.clone(),
            page_id,
        }
    }

    pub(crate) fn meta(&self) -> IvfMeta {
        self.meta_state.lock().meta.clone()
    }

    pub(crate) fn dim(&self) -> usize {
        self.dim_fixed
    }

    pub(crate) fn stores_f32(&self) -> bool {
        self.stores_f32
    }

    pub(crate) fn is_finalized(&self) -> bool {
        self.meta_state.lock().meta.finalized
    }

    /// Clear the `finalized` flag (durably), so the next open treats this file
    /// as an interrupted build and discards + rebuilds it from the heap. Used
    /// by `OPTIMIZE TABLE` to force a retrain of the coarse quantizer after
    /// centroid drift.
    pub(crate) fn mark_unfinalized(&self, txn_id: u64) -> Result<(), QueryError> {
        {
            let mut state = self.meta_state.lock();
            if !state.meta.finalized {
                return Ok(());
            }
            state.meta.finalized = false;
        }
        self.write_meta_page(txn_id)
    }

    /// Publish completion of a commit-deferred vector delta. The matching
    /// pre-commit `mark_unfinalized(txn_id)` is WAL-transactional; therefore a
    /// crash before this marker leaves `finalized=false` and forces heap rebuild.
    pub(crate) fn mark_finalized(&self, txn_id: u64) -> Result<(), QueryError> {
        {
            let mut state = self.meta_state.lock();
            if state.meta.finalized {
                return Ok(());
            }
            state.meta.finalized = true;
        }
        self.write_meta_page(txn_id)
    }

    pub(crate) fn is_trained(&self) -> bool {
        self.meta_state.lock().meta.trained
    }

    pub(crate) fn nlist(&self) -> usize {
        self.meta_state.lock().meta.nlist as usize
    }

    pub(crate) fn total_entries(&self) -> u64 {
        self.meta_state.lock().meta.total_entries
    }

    pub(crate) fn live_entries(&self) -> u64 {
        self.meta_state.lock().meta.live_entries
    }

    pub(crate) fn stale_entries(&self) -> u64 {
        let m = self.meta_state.lock();
        m.meta.total_entries.saturating_sub(m.meta.live_entries)
    }

    pub(crate) fn inserts_since_train(&self) -> u64 {
        self.meta_state.lock().meta.inserts_since_train
    }

    pub(crate) fn quant_params(&self) -> (f32, f32, f32) {
        let m = self.meta_state.lock();
        (m.meta.mid, m.meta.inv_scale, m.meta.dist_mul)
    }

    /// A cheap clone of the resident centroid array (an `Arc` bump).
    pub(crate) fn centroids(&self) -> Arc<Vec<f32>> {
        self.centroids.read().clone()
    }

    pub(crate) fn entries_per_list_page(&self) -> usize {
        self.cap_per_page
    }

    pub(crate) fn estimated_file_size_bytes(&self) -> u64 {
        self.meta_state
            .lock()
            .meta
            .next_page_id
            .saturating_mul(PAGE_SIZE as u64)
    }

    pub(crate) fn list_length_stats(&self) -> (u64, u64, u64) {
        let mut counts: Vec<u64> = self.directory.read().iter().map(|entry| entry.count).collect();
        if counts.is_empty() {
            return (0, 0, 0);
        }
        counts.sort_unstable();
        let avg = counts.iter().sum::<u64>() / counts.len() as u64;
        let max = *counts.last().unwrap_or(&0);
        let p95_index = ((counts.len() - 1) * 95).div_ceil(100);
        (avg, max, counts[p95_index])
    }

    pub(crate) fn flush(&self) -> Result<(), QueryError> {
        let latches = self.list_latches.read().clone();
        let _guards: Vec<_> = latches.iter().map(|latch| latch.lock()).collect();
        if self.is_trained() {
            self.flush_directory_pages(txn_id_for_ddl())?;
            self.write_meta_page(txn_id_for_ddl())?;
        }
        self.buffer_pool.flush_file(&self.file_key)
    }

    fn read_raw_page_image(&self, page_id: u64) -> Result<Vec<u8>, QueryError> {
        if page_id == IVF_META_PAGE {
            return Ok(self.meta_state.lock().meta_image.clone());
        }
        read_vector_index_page_image(&self.buffer_pool, &self.file, self.page_key(page_id))
    }

    fn read_page_bytes(&self, page_id: u64) -> Result<Vec<u8>, QueryError> {
        self.buffer_pool.read_raw_page(self.page_key(page_id))
    }

    /// Write a full page image through the buffer pool, emitting a physical
    /// `PageUpdate` WAL record (mirrors the HNSW store's `write_page`). In
    /// bulk-build mode the page is dirtied in place with no per-edit WAL.
    fn write_page(&self, txn_id: u64, page_id: u64, mut after: Vec<u8>) -> Result<(), QueryError> {
        debug_assert_eq!(after.len(), PAGE_SIZE);
        let key = self.page_key(page_id);
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
        let lsn = if self.durability == DurabilityMode::Memory {
            0
        } else {
            let before = if pristine {
                vec![0u8; PAGE_SIZE]
            } else {
                self.read_raw_page_image(page_id)?
            };
            log_vector_index_page_image(
                &self.log,
                txn_id,
                self.relative_path(),
                page_id,
                Some(before),
                &mut after,
            )?
        };
        self.buffer_pool.store_page(key, after, lsn)?;
        Ok(())
    }

    fn write_meta_page(&self, txn_id: u64) -> Result<(), QueryError> {
        let _meta_write = self.meta_flush_lock.lock();
        let after = {
            let state = self.meta_state.lock();
            encode_ivf_meta(&state.meta)
        };
        self.write_page(txn_id, IVF_META_PAGE, after.clone())?;
        self.meta_state.lock().meta_image = after;
        Ok(())
    }

    /// Allocate a fresh page id by extending the file (no freelist — compaction
    /// rewrites a fresh file). The new id is marked pristine so its first write
    /// skips the before-image. Does NOT persist the meta page; the caller folds
    /// the `next_page_id` bump into its own end-of-operation meta write.
    fn alloc_page(&self) -> u64 {
        let mut state = self.meta_state.lock();
        let page_id = state.meta.next_page_id;
        state.meta.next_page_id += 1;
        state.pristine_pages.insert(page_id);
        page_id
    }

    // ---- centroids + directory persistence ----------------------------------

    fn load_centroids(&self) -> Result<(), QueryError> {
        let (first, count, nlist) = {
            let m = self.meta_state.lock();
            (
                m.meta.cent_first_page,
                m.meta.cent_page_count,
                m.meta.nlist as usize,
            )
        };
        let total = nlist * self.dim_fixed;
        let mut out: Vec<f32> = Vec::with_capacity(total);
        let per_page = (PAGE_SIZE - IVF_HEADER_BYTES) / 4;
        'pages: for p in 0..count {
            let bytes = self.read_page_bytes(first + p)?;
            if !valid_ivf_page(&bytes, IVF_CENT_MAGIC, IVF_PAGE_KIND_CENT) {
                return Err(error(crate::error::catalog::ER_GET_ERRNO, "corrupt ivf centroid page"));
            }
            for i in 0..per_page {
                if out.len() >= total {
                    break 'pages;
                }
                let off = IVF_HEADER_BYTES + i * 4;
                out.push(f32::from_bits(read_u32(&bytes, off)));
            }
        }
        if out.len() != total {
            return Err(error(crate::error::catalog::ER_GET_ERRNO,
                "truncated ivf centroid page range",
            ));
        }
        *self.centroids.write() = Arc::new(out);
        Ok(())
    }

    fn load_directory(&self) -> Result<(), QueryError> {
        let (first, count, nlist) = {
            let m = self.meta_state.lock();
            (
                m.meta.dir_first_page,
                m.meta.dir_page_count,
                m.meta.nlist as usize,
            )
        };
        let mut dir: Vec<ListDir> = Vec::with_capacity(nlist);
        let per_page = (PAGE_SIZE - IVF_HEADER_BYTES) / IVF_DIR_ENTRY_BYTES;
        'pages: for p in 0..count {
            let bytes = self.read_page_bytes(first + p)?;
            if !valid_ivf_page(&bytes, IVF_DIR_MAGIC, IVF_PAGE_KIND_DIR) {
                return Err(error(crate::error::catalog::ER_GET_ERRNO, "corrupt ivf directory page"));
            }
            for i in 0..per_page {
                if dir.len() >= nlist {
                    break 'pages;
                }
                let off = IVF_HEADER_BYTES + i * IVF_DIR_ENTRY_BYTES;
                dir.push(ListDir {
                    head: read_u64(&bytes, off),
                    tail: read_u64(&bytes, off + 8),
                    count: read_u64(&bytes, off + 16),
                });
            }
        }
        if dir.len() != nlist {
            return Err(error(crate::error::catalog::ER_GET_ERRNO,
                "truncated ivf directory page range",
            ));
        }
        *self.directory.write() = dir;
        Ok(())
    }

    /// Rewrite every directory page from the resident directory (used at
    /// `finalize_build`, which deferred per-entry directory writes).
    fn flush_directory_pages(&self, txn_id: u64) -> Result<(), QueryError> {
        let per_page = (PAGE_SIZE - IVF_HEADER_BYTES) / IVF_DIR_ENTRY_BYTES;
        let (first, count) = {
            let m = self.meta_state.lock();
            (m.meta.dir_first_page, m.meta.dir_page_count)
        };
        let dir = self.directory.read().clone();
        for p in 0..count {
            let mut bytes = vec![0u8; PAGE_SIZE];
            format_page_header(&mut bytes, IVF_DIR_MAGIC, IVF_PAGE_KIND_DIR);
            let base = (p as usize) * per_page;
            for i in 0..per_page {
                let list_id = base + i;
                if list_id >= dir.len() {
                    break;
                }
                let off = IVF_HEADER_BYTES + i * IVF_DIR_ENTRY_BYTES;
                let e = dir[list_id];
                write_u64(&mut bytes, off, e.head);
                write_u64(&mut bytes, off + 8, e.tail);
                write_u64(&mut bytes, off + 16, e.count);
            }
            self.write_page(txn_id, first + p, bytes)?;
        }
        Ok(())
    }

    /// Persist directory entry `list_id` to its page (RMW). Assumes the caller
    /// holds the corresponding list latch and has already updated the resident
    /// directory.
    fn write_dir_entry(&self, txn_id: u64, list_id: usize) -> Result<(), QueryError> {
        let per_page = (PAGE_SIZE - IVF_HEADER_BYTES) / IVF_DIR_ENTRY_BYTES;
        let (first, entry) = {
            let m = self.meta_state.lock();
            (m.meta.dir_first_page, self.directory.read()[list_id])
        };
        let page_id = first + (list_id / per_page) as u64;
        let dir_latch = self
            .dir_page_latches
            .read()
            .get(list_id / per_page)
            .cloned()
            .ok_or_else(|| error(crate::error::catalog::ER_GET_ERRNO, "ivf directory page outside latch table"))?;
        let _dir_write = dir_latch.lock();
        let slot = list_id % per_page;
        let mut bytes = self.read_page_bytes(page_id)?;
        if !valid_ivf_page(&bytes, IVF_DIR_MAGIC, IVF_PAGE_KIND_DIR) {
            return Err(error(crate::error::catalog::ER_GET_ERRNO, "corrupt ivf directory page"));
        }
        let off = IVF_HEADER_BYTES + slot * IVF_DIR_ENTRY_BYTES;
        write_u64(&mut bytes, off, entry.head);
        write_u64(&mut bytes, off + 8, entry.tail);
        write_u64(&mut bytes, off + 16, entry.count);
        self.write_page(txn_id, page_id, bytes)
    }

    // ---- cold build ---------------------------------------------------------

    /// Enter bulk-build mode: page writes skip per-edit WAL. Pair with
    /// `finalize_build`.
    pub(crate) fn begin_build(&self) {
        self.build_mode.store(true, Ordering::Release);
    }

    /// Leave deferred-WAL mode after a failed cold build.  The file remains
    /// explicitly non-finalized so the next open discards and rebuilds it.
    pub(crate) fn abort_build(&self) {
        self.build_mode.store(false, Ordering::Release);
        {
            let mut state = self.meta_state.lock();
            state.meta.finalized = false;
            state.pristine_pages.clear();
        }
        let _ = self.write_meta_page(txn_id_for_ddl());
    }

    /// Lay down the trained coarse quantizer: write the centroid pages and an
    /// empty directory, and record the layout in meta. Resets counters (a fresh
    /// build / retrain). Must be called inside a build.
    pub(crate) fn init_trained(
        &self,
        centroids: &[f32],
        nlist: usize,
        nprobe_default: u32,
        mid: f32,
        inv_scale: f32,
        dist_mul: f32,
    ) -> Result<(), QueryError> {
        let _layout = self.layout_lock.lock();
        let cent_per_page = (PAGE_SIZE - IVF_HEADER_BYTES) / 4;
        let cent_values = nlist * self.dim_fixed;
        let cent_pages = cent_values.div_ceil(cent_per_page.max(1)).max(1) as u64;
        let dir_per_page = (PAGE_SIZE - IVF_HEADER_BYTES) / IVF_DIR_ENTRY_BYTES;
        let dir_pages = nlist.div_ceil(dir_per_page.max(1)).max(1) as u64;

        let cent_first = 1u64;
        let dir_first = cent_first + cent_pages;
        let next_page = dir_first + dir_pages;

        {
            let mut state = self.meta_state.lock();
            state.meta.nlist = nlist as u32;
            state.meta.nprobe_default = nprobe_default;
            state.meta.mid = mid;
            state.meta.inv_scale = inv_scale;
            state.meta.dist_mul = dist_mul;
            state.meta.cent_first_page = cent_first;
            state.meta.cent_page_count = cent_pages;
            state.meta.dir_first_page = dir_first;
            state.meta.dir_page_count = dir_pages;
            state.meta.next_page_id = next_page;
            state.meta.total_entries = 0;
            state.meta.live_entries = 0;
            state.meta.inserts_since_train = 0;
            state.meta.trained = true;
            state.meta.finalized = false;
            // Every page in [1, next_page) is freshly laid down here.
            for pid in 1..next_page {
                state.pristine_pages.insert(pid);
            }
        }

        // Centroid pages.
        let txn = txn_id_for_ddl();
        for p in 0..cent_pages {
            let mut bytes = vec![0u8; PAGE_SIZE];
            format_page_header(&mut bytes, IVF_CENT_MAGIC, IVF_PAGE_KIND_CENT);
            let base = (p as usize) * cent_per_page;
            for i in 0..cent_per_page {
                let idx = base + i;
                if idx >= cent_values {
                    break;
                }
                write_u32(&mut bytes, IVF_HEADER_BYTES + i * 4, centroids[idx].to_bits());
            }
            self.write_page(txn, cent_first + p, bytes)?;
        }
        // Empty directory pages.
        for p in 0..dir_pages {
            let mut bytes = vec![0u8; PAGE_SIZE];
            format_page_header(&mut bytes, IVF_DIR_MAGIC, IVF_PAGE_KIND_DIR);
            self.write_page(txn, dir_first + p, bytes)?;
        }
        self.write_meta_page(txn)?;

        *self.centroids.write() = Arc::new(centroids.to_vec());
        *self.directory.write() = vec![ListDir::default(); nlist];
        *self.list_latches.write() = (0..nlist)
            .map(|_| Arc::new(parking_lot::Mutex::new(())))
            .collect();
        *self.dir_page_latches.write() = (0..dir_pages)
            .map(|_| Arc::new(parking_lot::Mutex::new(())))
            .collect();
        Ok(())
    }

    /// Leave bulk-build mode and publish the index. Node pages are build-local
    /// derived data until the meta says `finalized = true`, so the fast path
    /// refreshes checksums, flushes the dirty `.ivf` frames straight to disk,
    /// then WAL-logs only the finalized meta page as the single commit point.
    /// A crash before that meta record leaves `finalized = false` → rebuilt
    /// from the heap.
    pub(crate) fn finalize_build(&self, txn_id: u64) -> Result<(), QueryError> {
        // Persist the final directory (head/tail/count per list) while still in
        // build mode (store_page, no per-page WAL) — the append loop only
        // mutated the resident copy.
        self.flush_directory_pages(txn_id)?;
        self.build_mode.store(false, Ordering::Release);
        let next = self.meta_state.lock().meta.next_page_id;
        if self.durability != DurabilityMode::Memory {
            for page_id in 1..next {
                self.buffer_pool
                    .with_raw_page_mut(self.page_key(page_id), 0, |bytes| {
                        match read_u32(bytes, 0) {
                            IVF_CENT_MAGIC | IVF_DIR_MAGIC | IVF_LIST_MAGIC => {
                                refresh_page_checksum(bytes);
                            }
                            _ => {}
                        }
                        Ok(())
                    })?;
            }
            self.buffer_pool.flush_file(&self.file_key)?;
        }
        {
            let mut state = self.meta_state.lock();
            state.meta.finalized = true;
            state.pristine_pages.clear();
        }
        self.write_meta_page(txn_id)?;
        Ok(())
    }

    // ---- entry mutation -----------------------------------------------------

    /// Cold-build fast path: materialize one already-sized list page and write
    /// it once. The caller supplies at most `cap_per_page` encoded entries and
    /// visits a list sequentially, so peak scratch is one 16 KiB page rather
    /// than one full list while avoiding per-row page read/modify/write.
    pub(crate) fn append_list_page_bulk(
        &self,
        txn_id: u64,
        list_id: usize,
        entries: &[(RowId, Vec<u8>)],
    ) -> Result<(), QueryError> {
        if entries.is_empty() {
            return Ok(());
        }
        if !self.build_mode.load(Ordering::Acquire)
            || entries.len() > self.cap_per_page
            || entries.iter().any(|(_, payload)| payload.len() != self.stride)
        {
            return Err(error(crate::error::catalog::ER_GET_ERRNO, "invalid ivf bulk list-page append"));
        }
        let list_latch = self
            .list_latches
            .read()
            .get(list_id)
            .cloned()
            .ok_or_else(|| error(crate::error::catalog::ER_GET_ERRNO, "ivf list id outside latch directory"))?;
        let _list_write = list_latch.lock();
        let old = self.directory.read()[list_id];
        let page_id = self.alloc_page();
        let mut page = vec![0u8; PAGE_SIZE];
        format_page_header(&mut page, IVF_LIST_MAGIC, IVF_PAGE_KIND_LIST);
        write_u32(&mut page, IVF_LIST_OFF_LISTID, list_id as u32);
        for (slot, (rid, payload)) in entries.iter().enumerate() {
            self.write_entry_into(&mut page, slot, payload, *rid);
        }
        write_u16(&mut page, IVF_LIST_OFF_COUNT, entries.len() as u16);
        self.write_page(txn_id, page_id, page)?;
        if old.tail != 0 {
            let mut tail = self.read_page_bytes(old.tail)?;
            if !valid_ivf_page(&tail, IVF_LIST_MAGIC, IVF_PAGE_KIND_LIST)
                || read_u64(&tail, IVF_LIST_OFF_NEXT) != 0
            {
                return Err(error(crate::error::catalog::ER_GET_ERRNO, "corrupt ivf bulk-build list tail"));
            }
            write_u64(&mut tail, IVF_LIST_OFF_NEXT, page_id);
            self.write_page(txn_id, old.tail, tail)?;
        }
        {
            let mut directory = self.directory.write();
            let entry = &mut directory[list_id];
            if entry.head == 0 {
                entry.head = page_id;
            }
            entry.tail = page_id;
            entry.count = entry.count.saturating_add(entries.len() as u64);
        }
        {
            let mut state = self.meta_state.lock();
            state.meta.total_entries = state.meta.total_entries.saturating_add(entries.len() as u64);
            state.meta.live_entries = state.meta.live_entries.saturating_add(entries.len() as u64);
        }
        Ok(())
    }

    /// Append one entry (its `stride`-byte payload + RowId) to list `list_id`,
    /// returning its `(page_id, slot)`. Allocates + chains a fresh page when the
    /// tail is full. Writes the list page *before* the directory pointer so a
    /// crash between them only orphans the entry (recall loss, never wrong).
    pub(crate) fn append_entry(
        &self,
        txn_id: u64,
        list_id: usize,
        payload: &[u8],
        rid: RowId,
    ) -> Result<(u64, u16), QueryError> {
        debug_assert_eq!(payload.len(), self.stride);
        let list_latch = self
            .list_latches
            .read()
            .get(list_id)
            .cloned()
            .ok_or_else(|| error(crate::error::catalog::ER_GET_ERRNO, "ivf list id outside latch directory"))?;
        let _list_write = list_latch.lock();
        let dir = self.directory.read()[list_id];
        let (page_id, slot) = if dir.tail == 0 {
            self.new_list_page_with_entry(txn_id, list_id, payload, rid)?
        } else {
            // Try to append into the tail page; if full, chain a new page.
            let mut bytes = self.read_page_bytes(dir.tail)?;
            if valid_ivf_page(&bytes, IVF_LIST_MAGIC, IVF_PAGE_KIND_LIST) {
                let count = read_u16(&bytes, IVF_LIST_OFF_COUNT) as usize;
                if count < self.cap_per_page {
                    self.write_entry_into(&mut bytes, count, payload, rid);
                    write_u16(&mut bytes, IVF_LIST_OFF_COUNT, (count + 1) as u16);
                    self.write_page(txn_id, dir.tail, bytes)?;
                    (dir.tail, count as u16)
                } else {
                    self.chain_new_tail(txn_id, list_id, dir.tail, payload, rid)?
                }
            } else {
                // Starting an unlinked replacement page would make every new
                // entry unreachable from the existing head.  Surface
                // corruption so the upper layer can rebuild/fall back.
                return Err(error(crate::error::catalog::ER_GET_ERRNO, "corrupt ivf list tail page"));
            }
        };
        // Update the resident directory + counts, then persist.
        {
            let mut d = self.directory.write();
            let e = &mut d[list_id];
            if e.head == 0 {
                e.head = page_id;
            }
            e.tail = page_id;
            e.count += 1;
        }
        {
            let mut state = self.meta_state.lock();
            state.meta.total_entries += 1;
            state.meta.live_entries += 1;
            if !self.build_mode.load(Ordering::Acquire) {
                state.meta.inserts_since_train += 1;
            }
        }
        // Cold build defers directory + meta persistence to `finalize_build`
        // (one write per directory page instead of one per entry — O(pages) not
        // O(entries)). Incremental inserts persist immediately for durability.
        if !self.build_mode.load(Ordering::Acquire) {
            if dir.tail == 0 || dir.tail != page_id {
                self.write_dir_entry(txn_id, list_id)?;
            }
            self.write_meta_page(txn_id)?;
        }
        Ok((page_id, slot))
    }

    /// Allocate a fresh list page, write `entry` at slot 0, set it as list
    /// `list_id`'s head+tail (in the resident directory; caller persists).
    fn new_list_page_with_entry(
        &self,
        txn_id: u64,
        list_id: usize,
        payload: &[u8],
        rid: RowId,
    ) -> Result<(u64, u16), QueryError> {
        let page_id = self.alloc_page();
        let mut bytes = vec![0u8; PAGE_SIZE];
        format_page_header(&mut bytes, IVF_LIST_MAGIC, IVF_PAGE_KIND_LIST);
        write_u32(&mut bytes, IVF_LIST_OFF_LISTID, list_id as u32);
        self.write_entry_into(&mut bytes, 0, payload, rid);
        write_u16(&mut bytes, IVF_LIST_OFF_COUNT, 1);
        self.write_page(txn_id, page_id, bytes)?;
        Ok((page_id, 0))
    }

    /// Chain a fresh tail page after `old_tail` and write `entry` at slot 0.
    fn chain_new_tail(
        &self,
        txn_id: u64,
        list_id: usize,
        old_tail: u64,
        payload: &[u8],
        rid: RowId,
    ) -> Result<(u64, u16), QueryError> {
        let (page_id, slot) = self.new_list_page_with_entry(txn_id, list_id, payload, rid)?;
        // Link old tail -> new page (data page written first above).
        let mut bytes = self.read_page_bytes(old_tail)?;
        if !valid_ivf_page(&bytes, IVF_LIST_MAGIC, IVF_PAGE_KIND_LIST) {
            return Err(error(crate::error::catalog::ER_GET_ERRNO, "corrupt ivf list tail page"));
        }
        write_u64(&mut bytes, IVF_LIST_OFF_NEXT, page_id);
        self.write_page(txn_id, old_tail, bytes)?;
        Ok((page_id, slot))
    }

    /// Write one entry's payload + RowId + clear-tombstone into `bytes` at slot
    /// `slot` (regions are fixed-capacity; caller guarantees `slot < cap`).
    fn write_entry_into(&self, bytes: &mut [u8], slot: usize, payload: &[u8], rid: RowId) {
        let cap = self.cap_per_page;
        let codes_off = IVF_LIST_OFF_DATA + slot * self.stride;
        bytes[codes_off..codes_off + self.stride].copy_from_slice(payload);
        let norms_base = IVF_LIST_OFF_DATA + cap * self.stride;
        let norm = if self.stores_f32 {
            0i32
        } else {
            payload
                .iter()
                .map(|&value| {
                    let value = value as i8 as i32;
                    value * value
                })
                .sum()
        };
        write_u32(bytes, norms_base + slot * 4, norm as u32);
        let rids_base = norms_base + cap * 4;
        write_ivf_row(bytes, rids_base + slot * 6, rid);
        let tomb_base = rids_base + cap * 6;
        bytes[tomb_base + slot / 8] &= !(1 << (slot & 7));
    }

    /// Set the tombstone bit at `(page_id, slot)`. Returns the previous state.
    /// Updates the live-entry counter when a live entry is newly tombstoned.
    pub(crate) fn set_tombstone(
        &self,
        txn_id: u64,
        page_id: u64,
        slot: u16,
        deleted: bool,
    ) -> Result<bool, QueryError> {
        let mut bytes = self.read_page_bytes(page_id)?;
        if !valid_ivf_page(&bytes, IVF_LIST_MAGIC, IVF_PAGE_KIND_LIST) {
            return Err(error(crate::error::catalog::ER_GET_ERRNO, "corrupt ivf list page"));
        }
        let cap = self.cap_per_page;
        // A stale reverse-index entry or a corrupt page could hand us a slot
        // past this page's capacity; the index is only a candidate generator,
        // so degrade to a no-op rather than panic on an out-of-bounds write.
        let count = read_u16(&bytes, IVF_LIST_OFF_COUNT) as usize;
        if count > cap {
            return Err(error(crate::error::catalog::ER_GET_ERRNO, "corrupt ivf list slot count"));
        }
        if slot as usize >= count {
            return Ok(false);
        }
        let tomb_base = IVF_LIST_OFF_DATA + cap * (self.stride + 10);
        let tomb_off = tomb_base + slot as usize / 8;
        let tomb_mask = 1 << (slot as usize & 7);
        let prev = bytes[tomb_off] & tomb_mask != 0;
        if deleted {
            bytes[tomb_off] |= tomb_mask;
        } else {
            bytes[tomb_off] &= !tomb_mask;
        }
        if prev != deleted {
            self.write_page(txn_id, page_id, bytes)?;
            let mut state = self.meta_state.lock();
            if deleted {
                state.meta.live_entries = state.meta.live_entries.saturating_sub(1);
            } else {
                state.meta.live_entries += 1;
            }
            drop(state);
            self.write_meta_page(txn_id)?;
        }
        Ok(prev)
    }

    /// Overwrite the payload (SQ8 codes / f32 vector) at `(page_id, slot)` in
    /// place, keeping its RowId and clearing any tombstone. The drift-adaptive
    /// in-place UPDATE path.
    pub(crate) fn overwrite_entry(
        &self,
        txn_id: u64,
        page_id: u64,
        slot: u16,
        payload: &[u8],
    ) -> Result<bool, QueryError> {
        debug_assert_eq!(payload.len(), self.stride);
        let mut bytes = self.read_page_bytes(page_id)?;
        if !valid_ivf_page(&bytes, IVF_LIST_MAGIC, IVF_PAGE_KIND_LIST) {
            return Err(error(crate::error::catalog::ER_GET_ERRNO, "corrupt ivf list page"));
        }
        let s = slot as usize;
        // Bounds-guard against a stale reverse-index slot / corrupt page (see
        // `set_tombstone`): degrade to a no-op rather than panic.
        let count = read_u16(&bytes, IVF_LIST_OFF_COUNT) as usize;
        if count > self.cap_per_page {
            return Err(error(crate::error::catalog::ER_GET_ERRNO, "corrupt ivf list slot count"));
        }
        if s >= count {
            return Ok(false);
        }
        let codes_off = IVF_LIST_OFF_DATA + s * self.stride;
        bytes[codes_off..codes_off + self.stride].copy_from_slice(payload);
        let cap = self.cap_per_page;
        let norms_base = IVF_LIST_OFF_DATA + cap * self.stride;
        let norm = if self.stores_f32 {
            0i32
        } else {
            payload
                .iter()
                .map(|&value| {
                    let value = value as i8 as i32;
                    value * value
                })
                .sum()
        };
        write_u32(&mut bytes, norms_base + s * 4, norm as u32);
        let tomb_base = norms_base + cap * 10;
        let tomb_off = tomb_base + s / 8;
        let tomb_mask = 1 << (s & 7);
        let was_tomb = bytes[tomb_off] & tomb_mask != 0;
        bytes[tomb_off] &= !tomb_mask;
        self.write_page(txn_id, page_id, bytes)?;
        if was_tomb {
            let mut state = self.meta_state.lock();
            state.meta.live_entries += 1;
            drop(state);
            self.write_meta_page(txn_id)?;
        }
        Ok(true)
    }

    // ---- scans --------------------------------------------------------------

    /// Walk list `head`'s page chain, invoking `f` once per page with a
    /// borrowed [`IvfListPageRef`] over that page's contiguous regions. One
    /// page is pinned at a time (O(1) peak pin — larger-than-memory friendly).
    pub(crate) fn for_each_list_page(
        &self,
        list_id: usize,
        _head: u64,
        batch: &mut [u8],
        mut f: impl FnMut(IvfListPageRef) -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        let cap = self.cap_per_page;
        let stride = self.stride;
        let max_pages = self.meta_state.lock().meta.next_page_id.max(1);
        let directory = self.directory.read().get(list_id).copied().ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "ivf list id outside directory",
            )
        })?;
        let head = directory.head;
        let mut page_id = head;
        let contiguous_end = directory.tail.checked_add(1).filter(|&end| {
            directory.head == head
                && head != 0
                && head.checked_add(directory.count.div_ceil(cap as u64)) == Some(end)
                && end <= max_pages
        });
        let mut batch_first = 0u64;
        let mut batch_end = 0u64;
        let mut visited = HashSet::new();
        let mut entries_seen = 0u64;
        while page_id != 0 {
            if (page_id >= max_pages && page_id >= self.meta_state.lock().meta.next_page_id.max(1))
                || !visited.insert(page_id)
            {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt ivf list page chain",
                ));
            }
            let mut visit = |bytes: &[u8]| {
                if !valid_ivf_page(bytes, IVF_LIST_MAGIC, IVF_PAGE_KIND_LIST) {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt ivf list page",
                    ));
                }
                if read_u32(bytes, IVF_LIST_OFF_LISTID) as usize != list_id {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "ivf list chain points to wrong list",
                    ));
                }
                let count = read_u16(bytes, IVF_LIST_OFF_COUNT) as usize;
                if count > cap {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt ivf list slot count",
                    ));
                }
                let codes = &bytes[IVF_LIST_OFF_DATA..IVF_LIST_OFF_DATA + count * stride];
                let norms_base = IVF_LIST_OFF_DATA + cap * stride;
                let norms = &bytes[norms_base..norms_base + count * 4];
                let rids_base = norms_base + cap * 4;
                let rids = &bytes[rids_base..rids_base + count * 6];
                let tomb_base = rids_base + cap * 6;
                let tombs = &bytes[tomb_base..tomb_base + count.div_ceil(8)];
                f(IvfListPageRef {
                    codes,
                    norms,
                    rids,
                    tombs,
                    count,
                })?;
                Ok((read_u64(bytes, IVF_LIST_OFF_NEXT), count as u64))
            };
            let (next, page_entries) =
                if batch.is_empty() || page_id < head || page_id >= contiguous_end.unwrap_or(0) {
                    self.buffer_pool.with_raw_page_intent(
                        self.page_key(page_id),
                        AccessIntent::SmallScan,
                        &mut visit,
                    )?
                } else {
                    if page_id < batch_first || page_id >= batch_end {
                        batch_first = page_id;
                        batch_end = page_id
                            .saturating_add((batch.len() / PAGE_SIZE) as u64)
                            .min(contiguous_end.unwrap());
                        let len = (batch_end - batch_first) as usize * PAGE_SIZE;
                        self.buffer_pool.read_raw_scan_batch(
                            &self.file_key,
                            batch_first,
                            &mut batch[..len],
                        )?;
                    }
                    let offset = (page_id - batch_first) as usize * PAGE_SIZE;
                    visit(&batch[offset..offset + PAGE_SIZE])?
                };
            entries_seen = entries_seen.saturating_add(page_entries);
            page_id = next;
        }
        // Compare against the directory captured BEFORE scanning. Append can
        // extend a tail after its page image was read; that is not corruption.
        if (directory.tail != 0 && !visited.contains(&directory.tail))
            || directory.count > entries_seen
        {
            return Err(error(crate::error::catalog::ER_GET_ERRNO,
                "ivf list directory chain mismatch"));
        }
        Ok(())
    }

    /// One bounded, governed scratch run is reused across every probed list.
    pub(crate) fn scan_scratch_bytes(&self) -> usize {
        if self.use_direct_scan() {
            16 * PAGE_SIZE
        } else {
            0
        }
    }

    fn use_direct_scan(&self) -> bool {
        self.durability != DurabilityMode::Memory
            && self.meta_state.lock().meta.next_page_id > self.buffer_pool.capacity_pages()
    }

    /// Submit physically contiguous selected lists before scoring. Cold build
    /// lays out each list consecutively; incremental-DML fragmentation safely
    /// declines the optional path and uses demand pins below.
    pub(crate) fn prefetch_lists(&self, list_ids: &[usize]) {
        if self.use_direct_scan() {
            return;
        }
        let ranges: Vec<(u64, u64)> = {
            let directory = self.directory.read();
            list_ids
                .iter()
                .filter_map(|&list_id| {
                    let entry = directory.get(list_id)?;
                    if entry.head == 0 || entry.count == 0 {
                        return None;
                    }
                    let pages = entry.count.div_ceil(self.cap_per_page as u64);
                    let end = entry.head.checked_add(pages)?;
                    if pages == 0 || entry.tail.checked_add(1) != Some(end) {
                        return None;
                    }
                    Some((entry.head, end))
                })
                .collect()
        };
        self.buffer_pool
            .prefetch_demand_pages(ranges.into_iter().flat_map(|(start, end)| {
                (start..end).map(|page_id| self.page_key(page_id))
            }));
    }

    /// Head page id for list `list_id` (0 if empty).
    pub(crate) fn list_head(&self, list_id: usize) -> u64 {
        self.directory
            .read()
            .get(list_id)
            .map(|d| d.head)
            .unwrap_or(0)
    }

    /// Visit every live entry across all lists as `(rid, list_id, page_id,
    /// slot)`, for rebuilding the in-memory reverse index. A single pass over
    /// the list-page chains.
    pub(crate) fn for_each_entry(
        &self,
        mut f: impl FnMut(RowId, u32, u64, u16),
    ) -> Result<(), QueryError> {
        let nlist = self.nlist();
        let cap = self.cap_per_page;
        for list_id in 0..nlist {
            let expected = self.directory.read()[list_id];
            let head = expected.head;
            let mut page_id = head;
            let max_pages = self.meta_state.lock().meta.next_page_id.max(1);
            let mut visited = HashSet::new();
            let mut entries_seen = 0u64;
                while page_id != 0 {
                if (page_id >= max_pages && page_id >= self.meta_state.lock().meta.next_page_id.max(1))
                || !visited.insert(page_id)
            {
                    return Err(error(crate::error::catalog::ER_GET_ERRNO, "corrupt ivf list page chain"));
                }
                    let (next, page_entries) = self.buffer_pool.with_raw_page(self.page_key(page_id), |bytes| {
                    if !valid_ivf_page(bytes, IVF_LIST_MAGIC, IVF_PAGE_KIND_LIST) {
                        return Err(error(crate::error::catalog::ER_GET_ERRNO, "corrupt ivf list page"));
                    }
                    if read_u32(bytes, IVF_LIST_OFF_LISTID) as usize != list_id {
                        return Err(error(crate::error::catalog::ER_GET_ERRNO, "ivf list chain points to wrong list"));
                    }
                    let count = read_u16(bytes, IVF_LIST_OFF_COUNT) as usize;
                    if count > cap {
                        return Err(error(crate::error::catalog::ER_GET_ERRNO, "corrupt ivf list slot count"));
                    }
                    let norms_base = IVF_LIST_OFF_DATA + cap * self.stride;
                    let rids_base = norms_base + cap * 4;
                    let tomb_base = rids_base + cap * 6;
                    for i in 0..count {
                        if bytes[tomb_base + i / 8] & (1 << (i & 7)) != 0 {
                            continue;
                        }
                        let rid = read_ivf_row(bytes, rids_base + i * 6);
                        f(rid, list_id as u32, page_id, i as u16);
                    }
                    Ok((read_u64(bytes, IVF_LIST_OFF_NEXT), count as u64))
                })?;
                entries_seen = entries_seen.saturating_add(page_entries);
                    page_id = next;
            }
            if (expected.tail != 0 && !visited.contains(&expected.tail))
                || expected.count > entries_seen
            {
                return Err(error(crate::error::catalog::ER_GET_ERRNO, "ivf list directory chain mismatch"));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod ivf_page_tests {
    use super::*;
    use crate::engine::vector::distance::Metric;
    use crate::engine::vector::ivf_paged::PagedIvf;
    use crate::engine::vector::IvfParams;
    use crate::wal::LogManager;

    fn params(nlist: usize) -> IvfParams {
        IvfParams {
            nlist,
            nprobe_default: 8,
            kmeans_iters: 12,
            train_sample_mult: 256,
        }
    }

    fn gen_vectors(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
        // Deterministic clustered vectors (16 separated centers + noise).
        let mut s = seed;
        let mut next = || {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((s >> 33) as f32 / (1u64 << 31) as f32) - 1.0
        };
        let nc = 16;
        let centers: Vec<Vec<f32>> = (0..nc)
            .map(|_| (0..dim).map(|_| next() * 5.0).collect())
            .collect();
        (0..n)
            .map(|i| {
                let c = &centers[i % nc];
                c.iter().map(|&x| x + next() * 0.3).collect()
            })
            .collect()
    }

    fn brute_knn(vecs: &[Vec<f32>], q: &[f32], k: usize, metric: Metric) -> Vec<u32> {
        let mut scored: Vec<(f32, u32)> = vecs
            .iter()
            .enumerate()
            .map(|(i, v)| {
                (
                    crate::engine::vector::distance::ordering_distance(q, v, metric),
                    i as u32,
                )
            })
            .collect();
        scored.sort_by(|a, b| a.0.total_cmp(&b.0));
        scored.into_iter().take(k).map(|(_, i)| i).collect()
    }

    fn test_pool(log: &Arc<crate::wal::LogMutex>, pages: u64) -> Arc<BufferPool> {
        BufferPool::new(
            pages,
            log.clone(),
            DurabilityMode::Relaxed,
            std::time::Duration::from_millis(DEFAULT_LOCK_WAIT_MS),
        )
    }

    fn open_file(
        path: &Path,
        log: &Arc<crate::wal::LogMutex>,
        pool: &Arc<BufferPool>,
        durability: DurabilityMode,
        dim: u32,
        metric: u8,
    ) -> Arc<IvfIndexFile> {
        let stores_f32 = PagedIvf::stores_f32_for(match metric {
            1 => Metric::Cosine,
            2 => Metric::Dot,
            _ => Metric::L2,
        });
        Arc::new(
            IvfIndexFile::open_with_durability(
                path.to_path_buf(),
                log.clone(),
                durability,
                pool.clone(),
                dim,
                metric,
                stores_f32,
            )
            .unwrap(),
        )
    }

    #[test]
    fn paged_ivf_spilled_training_matches_resident_training() {
        for metric in [0, 1, 2] {
            let dir = tempfile::tempdir().unwrap();
            let log = Arc::new(parking_lot::Mutex::new(
                LogManager::open(dir.path()).unwrap(),
            ));
            let pool = test_pool(&log, 64);
            let vectors = gen_vectors(512, 16, 77);
            let rows: Vec<_> = vectors
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
            let resident = PagedIvf::new(
                open_file(
                    &dir.path().join("resident.ivf"),
                    &log,
                    &pool,
                    DurabilityMode::Relaxed,
                    16,
                    metric,
                ),
                params(16),
            );
            let spilled = PagedIvf::new(
                open_file(
                    &dir.path().join("spilled.ivf"),
                    &log,
                    &pool,
                    DurabilityMode::Relaxed,
                    16,
                    metric,
                ),
                params(16),
            );
            resident.build_from_rows(&rows).unwrap();
            let mut writer =
                crate::engine::vector::spill_rows::VectorRowSpill::new(dir.path(), 16).unwrap();
            for (rid, vector) in &rows {
                writer
                    .push(*rid, &spilled.prepare_build_vector(vector))
                    .unwrap();
            }
            let mapped = writer.finish().unwrap();
            mapped.advise_random();
            spilled
                .build_from_spilled_rows(&mapped.rows(), dir.path())
                .unwrap();
            assert!(spilled.is_finalized());
            assert_eq!(spilled.live_count(), resident.live_count());
            for query in vectors.iter().take(16) {
                assert_eq!(spilled.search(query, 10, 8), resident.search(query, 10, 8));
            }
        }
    }

    #[test]
    fn paged_ivf_v2_keeps_768d_page_density() {
        assert_eq!(IvfIndexFile::cap_for_stride(768), 21);
    }

    #[test]
    fn paged_ivf_reopen_persists_and_is_queryable() {
        let dir = tempfile::tempdir().unwrap();
        let dim = 16usize;
        let n = 800usize;
        let vecs = gen_vectors(n, dim, 99);
        let rows: Vec<(RowId, Vec<f32>)> = vecs
            .iter()
            .enumerate()
            .map(|(i, v)| (RowId { page_id: i as u32, slot_id: 0 }, v.clone()))
            .collect();
        let path = dir.path().join("schemas/test/indexes/v__1__ivf.ivf");
        let log = Arc::new(parking_lot::Mutex::new(LogManager::open(dir.path()).unwrap()));
        // Build phase.
        {
            let pool = test_pool(&log, DEFAULT_BUFFER_POOL_PAGES);
            let file = open_file(&path, &log, &pool, DurabilityMode::Relaxed, dim as u32, 0);
            let ivf = PagedIvf::new(file, params(32));
            ivf.build_from_rows(&rows).unwrap();
            assert!(ivf.is_finalized());
            assert_eq!(ivf.live_count(), n);
            pool.flush_all().unwrap();
        }
        // Reopen phase: index served from disk, no rebuild.
        let pool = test_pool(&log, DEFAULT_BUFFER_POOL_PAGES);
        let file = open_file(&path, &log, &pool, DurabilityMode::Relaxed, dim as u32, 0);
        assert!(file.is_finalized(), "reopened file must be trusted as finalized");
        let ivf = PagedIvf::new(file, params(32));
        assert_eq!(ivf.live_count(), n, "counts survive reopen");
        // Query row 123: itself is its own nearest neighbour.
        let got = ivf.search(&vecs[123], 5, 32);
        assert!(
            got.iter().any(|(r, _)| r.page_id == 123),
            "self should be in its own candidate set after reopen"
        );
    }

    #[test]
    fn paged_ivf_larger_than_memory_recall_holds() {
        // Build with a normal pool, flush, reopen with a tiny pool so every
        // search must demand-page the probed lists from disk and evict.
        let dir = tempfile::tempdir().unwrap();
        let dim = 32usize;
        let n = 800usize;
        let vecs = gen_vectors(n, dim, 4242);
        let rows: Vec<(RowId, Vec<f32>)> = vecs
            .iter()
            .enumerate()
            .map(|(i, v)| (RowId { page_id: i as u32, slot_id: 0 }, v.clone()))
            .collect();
        let path = dir.path().join("schemas/test/indexes/v__1__big.ivf");
        let log = Arc::new(parking_lot::Mutex::new(LogManager::open(dir.path()).unwrap()));
        {
            let pool = test_pool(&log, DEFAULT_BUFFER_POOL_PAGES);
            let file = open_file(&path, &log, &pool, DurabilityMode::Relaxed, dim as u32, 0);
            let ivf = PagedIvf::new(file.clone(), params(32));
            ivf.build_from_rows(&rows).unwrap();
            let pages = file.meta().next_page_id;
            assert!(pages > 8, "index ({pages} pages) should span many pages");
            pool.flush_all().unwrap();
        }
        let pool = test_pool(&log, 8u64); // tiny pool
        let file = open_file(&path, &log, &pool, DurabilityMode::Relaxed, dim as u32, 0);
        assert!(file.meta().next_page_id > 8);
        let ivf = PagedIvf::new(file, params(32));
        let queries = gen_vectors(4, dim, 777);
        let (mut hits, mut total) = (0usize, 0usize);
        let k = 10;
        for q in &queries {
            let got: Vec<u32> = ivf.search(q, k * 4, 24).into_iter().map(|(r, _)| r.page_id).collect();
            for w in brute_knn(&vecs, q, k, Metric::L2) {
                if got.contains(&w) {
                    hits += 1;
                }
            }
            total += k;
        }
        let recall = hits as f64 / total as f64;
        assert!(recall >= 0.85, "larger-than-memory candidate recall too low: {recall}");
    }

    #[test]
    fn paged_ivf_navigator_reports_exact_order_and_dml_invalidates_it() {
        let dir = tempfile::tempdir().unwrap();
        let dim = 24usize;
        let vecs = gen_vectors(600, dim, 0x51a7);
        let rows: Vec<(RowId, Vec<f32>)> = vecs
            .iter()
            .enumerate()
            .map(|(i, vector)| {
                (
                    RowId { page_id: i as u32, slot_id: 0 },
                    vector.clone(),
                )
            })
            .collect();
        let path = dir.path().join("schemas/test/indexes/v__nav.ivf");
        let log = Arc::new(parking_lot::Mutex::new(LogManager::open(dir.path()).unwrap()));
        let pool = test_pool(&log, DEFAULT_BUFFER_POOL_PAGES);
        let file = open_file(&path, &log, &pool, DurabilityMode::Relaxed, dim as u32, 0);
        let ivf = PagedIvf::new(file, params(16));
        ivf.build_from_rows(&rows).unwrap();

        let navigator = Arc::new(crate::engine::vector::HnswIndex::new(Metric::L2, 8, 32));
        for (rid, vector) in &rows {
            navigator.append_node(*rid, vector.clone()).unwrap();
        }
        navigator.link_all(1);
        ivf.install_navigator_for_test(navigator);

        let (_, _, exact_order) = ivf.try_search_with_stats(&vecs[17], 40, 8).unwrap();
        assert!(exact_order, "graph navigator must advertise authoritative f32 order");

        ivf.insert(RowId { page_id: 10_000, slot_id: 0 }, &vecs[18]);
        let (_, _, exact_order) = ivf.try_search_with_stats(&vecs[17], 40, 8).unwrap();
        assert!(!exact_order, "pre-commit DML must invalidate the derived navigator");
    }

    #[test]
    fn paged_ivf_recovers_incremental_inserts_from_wal_after_crash() {
        // Cold-build (finalized, flushed), then incrementally INSERT more rows
        // (WAL'd, NOT flushed), simulate a crash by dropping the pool without
        // flushing, replay the WAL, and confirm the inserted rows survived via
        // the generic redo_page_update path (no IVF-specific recovery code).
        let dir = tempfile::tempdir().unwrap();
        let dim = 8usize;
        let base = gen_vectors(200, dim, 2026);
        let extra = gen_vectors(40, dim, 5150);
        let rel = "schemas/test/indexes/v__1__crash.ivf";
        let path = dir.path().join(rel);
        {
            let log = Arc::new(parking_lot::Mutex::new(LogManager::open(dir.path()).unwrap()));
            // Strict durability so page writes emit WAL records.
            let pool = BufferPool::new(
                DEFAULT_BUFFER_POOL_PAGES,
                log.clone(),
                DurabilityMode::Strict,
                std::time::Duration::from_millis(DEFAULT_LOCK_WAIT_MS),
            );
            let file = open_file(&path, &log, &pool, DurabilityMode::Strict, dim as u32, 0);
            let ivf = PagedIvf::new(file, params(16));
            let rows: Vec<(RowId, Vec<f32>)> = base
                .iter()
                .enumerate()
                .map(|(i, v)| (RowId { page_id: i as u32, slot_id: 0 }, v.clone()))
                .collect();
            ivf.build_from_rows(&rows).unwrap(); // finalize flushes the built pages
            // Incremental inserts: WAL'd, left in the (unflushed) pool.
            for (j, v) in extra.iter().enumerate() {
                ivf.insert(RowId { page_id: (10_000 + j) as u32, slot_id: 0 }, v);
            }
            log.lock().flush().unwrap(); // WAL durable; pool pages NOT flushed
            drop(ivf);
            drop(pool);
            drop(log);
        }
        // Replay the WAL.
        let result = recover_from_wal(dir.path(), DurabilityMode::Strict).unwrap();
        assert!(result.summary.redone_records > 0, "expected WAL redo of ivf pages");
        // Reopen and confirm the incrementally-inserted rows are queryable.
        let log = Arc::new(parking_lot::Mutex::new(LogManager::open(dir.path()).unwrap()));
        let pool = test_pool(&log, DEFAULT_BUFFER_POOL_PAGES);
        let file = open_file(&path, &log, &pool, DurabilityMode::Strict, dim as u32, 0);
        assert!(file.is_finalized());
        let ivf = PagedIvf::new(file, params(16));
        let mut found = 0;
        for (j, v) in extra.iter().enumerate() {
            let rid = RowId { page_id: (10_000 + j) as u32, slot_id: 0 };
            let got = ivf.search(v, 8, 16);
            if got.iter().any(|(r, _)| *r == rid) {
                found += 1;
            }
        }
        assert!(
            found as f64 / extra.len() as f64 > 0.9,
            "recovered index lost incrementally-inserted rows: {found}/{}",
            extra.len()
        );
    }

    #[test]
    fn paged_ivf_direct_scan_matches_pins_with_dirty_and_fragmented_lists() {
        for metric in [0, 1, 2] {
            let dir = tempfile::tempdir().unwrap();
            let log = Arc::new(parking_lot::Mutex::new(
                LogManager::open(dir.path()).unwrap(),
            ));
            let pool = test_pool(&log, 1024);
            let vectors = gen_vectors(600, 65, 31);
            let rows: Vec<_> = vectors
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
            let file = open_file(
                &dir.path().join("direct.ivf"),
                &log,
                &pool,
                DurabilityMode::Relaxed,
                65,
                metric,
            );
            let ivf = PagedIvf::new(file.clone(), params(4));
            ivf.build_from_rows(&rows).unwrap();
            for phase in 0..3 {
                pool.set_capacity(4, std::time::Duration::from_secs(5))
                    .unwrap();
                assert!(file.use_direct_scan());
                if phase == 1 {
                    assert_eq!(ivf.mark_deleted_by_row_id(rows[50].0), 1);
                    let mut moved = vectors[60].clone();
                    moved[0] += 0.001;
                    assert!(ivf.try_update_in_place(rows[60].0, &moved, 1.0));
                } else if phase == 2 {
                    // Interleaving cells extends their tails into fragmented
                    // chains; those lists must retain the demand-pin path.
                    for (i, vector) in vectors.iter().take(160).enumerate() {
                        ivf.insert(
                            RowId {
                                page_id: 1000 + i as u32,
                                slot_id: 0,
                            },
                            vector,
                        );
                    }
                }
                let direct: Vec<_> = vectors
                    .iter()
                    .take(8)
                    .map(|q| ivf.try_search(q, 20, 4).unwrap())
                    .collect();
                pool.set_capacity(1024, std::time::Duration::from_secs(5))
                    .unwrap();
                assert!(!file.use_direct_scan());
                let pinned: Vec<_> = vectors
                    .iter()
                    .take(8)
                    .map(|q| ivf.try_search(q, 20, 4).unwrap())
                    .collect();
                assert_eq!(direct, pinned, "metric={metric} phase={phase}");
                if phase > 0 {
                    assert!(direct
                        .iter()
                        .all(|rs| rs.iter().all(|(rid, _)| *rid != rows[50].0)));
                }
            }
        }
    }

    #[test]
    fn paged_ivf_dml_delete_and_update() {
        let dir = tempfile::tempdir().unwrap();
        let dim = 12usize;
        let vecs = gen_vectors(400, dim, 31);
        let rows: Vec<(RowId, Vec<f32>)> = vecs
            .iter()
            .enumerate()
            .map(|(i, v)| (RowId { page_id: i as u32, slot_id: 0 }, v.clone()))
            .collect();
        let path = dir.path().join("schemas/test/indexes/v__1__dml.ivf");
        let log = Arc::new(parking_lot::Mutex::new(LogManager::open(dir.path()).unwrap()));
        let pool = test_pool(&log, DEFAULT_BUFFER_POOL_PAGES);
        let file = open_file(&path, &log, &pool, DurabilityMode::Relaxed, dim as u32, 0);
        let ivf = PagedIvf::new(file, params(16));
        ivf.build_from_rows(&rows).unwrap();
        // DELETE row 50 → absent from its own neighbourhood.
        let target = RowId { page_id: 50, slot_id: 0 };
        assert_eq!(ivf.mark_deleted_by_row_id(target), 1);
        let got = ivf.search(&vecs[50], 20, 16);
        assert!(!got.iter().any(|(r, _)| *r == target), "deleted row resurfaced");
        assert_eq!(ivf.live_count(), 399);
        // Re-delete is a no-op.
        assert_eq!(ivf.mark_deleted_by_row_id(target), 0);
        // UPDATE row 60 in place (tiny drift keeps it in the same cell).
        let r60 = RowId { page_id: 60, slot_id: 0 };
        let mut moved = vecs[60].clone();
        moved[0] += 0.01;
        let inplace = ivf.try_update_in_place(r60, &moved, 0.2);
        // Either in-place (same cell) or the caller would re-insert; either way
        // the row must still be findable by its (near-unchanged) vector.
        let got = ivf.search(&moved, 5, 16);
        assert!(
            got.iter().any(|(r, _)| *r == r60),
            "updated row not found (inplace={inplace})"
        );
    }

    #[test]
    fn paged_ivf_tombstone_overwrite_out_of_bounds_slot_is_noop_not_panic() {
        // S1: a stale reverse-index entry or corrupt page could hand
        // `set_tombstone` / `overwrite_entry` a slot past the page capacity.
        // The index is only a candidate generator, so these must degrade to a
        // no-op (returning `false`) rather than panic on an OOB write.
        let dir = tempfile::tempdir().unwrap();
        let dim = 8usize;
        let vecs = gen_vectors(100, dim, 77);
        let rows: Vec<(RowId, Vec<f32>)> = vecs
            .iter()
            .enumerate()
            .map(|(i, v)| (RowId { page_id: i as u32, slot_id: 0 }, v.clone()))
            .collect();
        let path = dir.path().join("schemas/test/indexes/v__1__oob.ivf");
        let log = Arc::new(parking_lot::Mutex::new(LogManager::open(dir.path()).unwrap()));
        let pool = test_pool(&log, DEFAULT_BUFFER_POOL_PAGES);
        let file = open_file(&path, &log, &pool, DurabilityMode::Relaxed, dim as u32, 0);
        let ivf = PagedIvf::new(file.clone(), params(8));
        ivf.build_from_rows(&rows).unwrap();
        // Find a real list page to target with a bogus slot.
        let head = file.list_head(0);
        assert!(head != 0, "list 0 should have a head page");
        let huge_slot = u16::MAX; // far past cap_per_page
        // Neither must panic; both must report "nothing changed".
        assert_eq!(
            file.set_tombstone(0, head, huge_slot, true).unwrap(),
            false,
            "OOB tombstone slot must be a no-op"
        );
        let payload = vec![0u8; file.stride];
        assert_eq!(
            file.overwrite_entry(0, head, huge_slot, &payload).unwrap(),
            false,
            "OOB overwrite slot must be a no-op"
        );
        // The index is otherwise intact and still queryable.
        assert_eq!(ivf.live_count(), 100);
    }

    #[test]
    fn paged_ivf_rejects_list_cycle_and_oversized_slot_count() {
        let dir = tempfile::tempdir().unwrap();
        let dim = 8usize;
        let vecs = gen_vectors(32, dim, 0xC1C1E);
        let rows: Vec<(RowId, Vec<f32>)> = vecs
            .iter()
            .enumerate()
            .map(|(i, v)| (RowId { page_id: i as u32, slot_id: 0 }, v.clone()))
            .collect();
        let path = dir.path().join("schemas/test/indexes/v__1__cycle.ivf");
        let log = Arc::new(parking_lot::Mutex::new(LogManager::open(dir.path()).unwrap()));
        let pool = test_pool(&log, DEFAULT_BUFFER_POOL_PAGES);
        let file = open_file(&path, &log, &pool, DurabilityMode::Relaxed, dim as u32, 0);
        let ivf = PagedIvf::new(file.clone(), params(1));
        ivf.build_from_rows(&rows).unwrap();
        let head = file.list_head(0);

        let mut cycle = file.read_page_bytes(head).unwrap();
        write_u64(&mut cycle, IVF_LIST_OFF_NEXT, head);
        file.write_page(0, head, cycle).unwrap();
        assert!(ivf.try_search_with_stats(&vecs[0], 4, 1).is_err());

        // Restore the chain, then corrupt the slot count past physical
        // capacity. Both paths must return a rebuildable error, never clamp or
        // index beyond the page buffer.
        let mut bad_count = file.read_page_bytes(head).unwrap();
        write_u64(&mut bad_count, IVF_LIST_OFF_NEXT, 0);
        write_u16(
            &mut bad_count,
            IVF_LIST_OFF_COUNT,
            (file.cap_per_page + 1) as u16,
        );
        file.write_page(0, head, bad_count).unwrap();
        assert!(file.for_each_entry(|_, _, _, _| {}).is_err());
    }

    /// End-to-end paged IVF_SQ8 benchmark on a large, high-dimensional set
    /// (default 100k x 768). Reports cold-build time, `.ivf` size, per-nprobe
    /// query latency / QPS and recall@10 vs exact brute force, from a fresh
    /// reopen with a memory-bounded pool (so the lists are demand-paged). Run:
    ///   BENCH_N=100000 BENCH_DIM=768 cargo nextest run --lib \
    ///     paged_ivf_bench_100k_768 --run-ignored all --nocapture
    #[test]
    #[ignore = "perf benchmark; run explicitly with --run-ignored all --nocapture"]
    fn paged_ivf_bench_100k_768() {
        let n: usize = std::env::var("BENCH_N").ok().and_then(|v| v.parse().ok()).unwrap_or(100_000);
        let dim: usize = std::env::var("BENCH_DIM").ok().and_then(|v| v.parse().ok()).unwrap_or(768);
        let nlist: usize = std::env::var("BENCH_NLIST")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0); // 0 => auto (√N)
        let nq: usize = std::env::var("BENCH_NQ").ok().and_then(|v| v.parse().ok()).unwrap_or(200);
        let pool_pages: u64 = std::env::var("BENCH_POOL_PAGES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_BUFFER_POOL_PAGES);

        eprintln!("[ivf-bench] generating {n} x {dim} vectors …");
        let vecs = gen_vectors(n, dim, 0xBEEF_1234_5678_9ABC);
        let queries: Vec<Vec<f32>> = (0..nq).map(|i| vecs[(i * 7919) % n].clone()).collect();
        let rows: Vec<(RowId, Vec<f32>)> = vecs
            .iter()
            .enumerate()
            .map(|(i, v)| (RowId { page_id: i as u32, slot_id: 0 }, v.clone()))
            .collect();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("schemas/test/indexes/v__1__bench.ivf");
        let log = Arc::new(parking_lot::Mutex::new(LogManager::open(dir.path()).unwrap()));

        // --- cold build (normal pool, relaxed durability for speed) ---
        {
            let pool = test_pool(&log, pool_pages);
            let file = open_file(&path, &log, &pool, DurabilityMode::Relaxed, dim as u32, 0);
            let t = std::time::Instant::now();
            let ivf = PagedIvf::new(file.clone(), params(nlist));
            ivf.build_from_rows(&rows).unwrap();
            let build = t.elapsed();
            let pages = file.meta().next_page_id;
            eprintln!(
                "[ivf-bench] build: {:?}  nlist={}  pages={}  size={:.1} MiB",
                build,
                file.nlist(),
                pages,
                (pages * PAGE_SIZE as u64) as f64 / (1024.0 * 1024.0)
            );
            pool.flush_all().unwrap();
        }

        // --- query phase: reopen with the bounded pool (demand-paged) ---
        let pool = test_pool(&log, pool_pages);
        let reopen_started = std::time::Instant::now();
        let file = open_file(&path, &log, &pool, DurabilityMode::Relaxed, dim as u32, 0);
        let ivf = PagedIvf::new(file, params(nlist));
        let reopen = reopen_started.elapsed();
        let k = 10;
        // exact ground truth per query (once).
        let truth: Vec<Vec<u32>> = queries
            .iter()
            .map(|q| brute_knn(&vecs, q, k, Metric::L2))
            .collect();

        // Production fetches `k * rerank` SQ8 candidates then re-ranks them by
        // exact heap f32 and truncates to k — so the delivered recall@k is
        // "how many exact top-k landed in the SQ8 candidate pool". Measure that
        // (fetch a rerank-sized pool), which is what the executor delivers, not
        // the raw SQ8-ordered top-k (which understates by the quantization
        // reorder of near-ties).
        let rerank: usize = std::env::var("BENCH_RERANK").ok().and_then(|v| v.parse().ok()).unwrap_or(4);
        let want = k * rerank;
        let cold_started = std::time::Instant::now();
        let cold = ivf.search(&queries[0], want, 8);
        let cold_query = cold_started.elapsed();
        std::hint::black_box(cold);
        eprintln!(
            "[ivf-bench] first-query: reopen={:.3} ms  cold-nprobe8={:.3} ms",
            reopen.as_secs_f64() * 1000.0,
            cold_query.as_secs_f64() * 1000.0,
        );
        for &nprobe in &[1usize, 4, 8, 16, 32] {
            for q in &queries {
                let _ = ivf.search(q, want, nprobe);
            }
            let t = std::time::Instant::now();
            let mut hits = 0usize;
            for (qi, q) in queries.iter().enumerate() {
                let got: Vec<u32> = ivf.search(q, want, nprobe).into_iter().map(|(r, _)| r.page_id).collect();
                for w in &truth[qi] {
                    if got.contains(w) {
                        hits += 1;
                    }
                }
            }
            let el = t.elapsed();
            let per = el.as_secs_f64() / nq as f64;
            eprintln!(
                "[ivf-bench] nprobe={:>3}  {:>7.3} ms/query  {:>8.0} qps  recall@{}={:.3} (pool={})",
                nprobe,
                per * 1000.0,
                1.0 / per,
                k,
                hits as f64 / (nq * k) as f64,
                want,
            );
        }

        // Optional long-running query-only loop for sampling profilers. This
        // starts after build, reopen, ground truth, and the reported benchmark
        // matrix, so an attached profile contains only the IVF query path.
        let profile_iters: usize = std::env::var("BENCH_PROFILE_ITERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        if profile_iters > 0 {
            let profile_nprobe: usize = std::env::var("BENCH_PROFILE_NPROBE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(8);
            eprintln!(
                "[ivf-bench] profile-loop start: iters={profile_iters} nprobe={profile_nprobe}"
            );
            let started = std::time::Instant::now();
            for i in 0..profile_iters {
                std::hint::black_box(ivf.search(
                    &queries[i % queries.len()],
                    want,
                    profile_nprobe,
                ));
            }
            eprintln!(
                "[ivf-bench] profile-loop done: {:?}",
                started.elapsed()
            );
        }
    }

    #[test]
    #[ignore = "100k x 256d recall and warm-load performance gate"]
    fn audit_bug8_default_list_recall_100k_256d() {
        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool = test_pool(&log, 4096);
        let vectors = gen_vectors(100_000, 256, 0xB8);
        let rows: Vec<_> = vectors
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
        let path = dir.path().join("recall.ivf");
        let started = std::time::Instant::now();
        let ivf = PagedIvf::new(
            open_file(&path, &log, &pool, DurabilityMode::Relaxed, 256, 0),
            params(100),
        );
        ivf.build_from_rows(&rows).unwrap();
        assert!(
            !ivf.has_navigator(),
            "default build must skip the high-dimensional navigator"
        );
        eprintln!("100k x 256d build: {:?}", started.elapsed());
        pool.flush_all().unwrap();
        drop(ivf);
        let started = std::time::Instant::now();
        let ivf = PagedIvf::new(
            open_file(&path, &log, &pool, DurabilityMode::Relaxed, 256, 0),
            params(100),
        );
        eprintln!("100k x 256d reopen/reverse map: {:?}", started.elapsed());
        let mut hits = 0;
        let mut query_time = std::time::Duration::ZERO;
        for i in (0..100_000).step_by(10_000) {
            let query = &vectors[i];
            let mut truth: Vec<_> = vectors
                .iter()
                .enumerate()
                .map(|(id, v)| (id as u32, crate::engine::vector::distance::l2_sq(query, v)))
                .collect();
            truth.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
            let started = std::time::Instant::now();
            let (mut got, _, exact) = ivf.try_search_with_stats(query, 40, 8).unwrap();
            assert!(!exact);
            for (rid, score) in &mut got {
                *score = crate::engine::vector::distance::l2_sq(query, &vectors[rid.page_id as usize]);
            }
            got.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.page_id.cmp(&b.0.page_id)));
            query_time += started.elapsed();
            hits += truth[..10]
                .iter()
                .filter(|(id, _)| got.iter().take(10).any(|(rid, _)| rid.page_id == *id))
                .count();
            assert_eq!(
                got[0].0.page_id, i as u32,
                "exact self match must survive candidate generation"
            );
        }
        eprintln!(
            "100k x 256d recall@10={}, mean query={:?}",
            hits as f64 / 100.0,
            query_time / 10
        );
        assert!(hits >= 80, "list recall regressed: {hits}/100");
    }

    #[test]
    fn audit_bug4_overlapping_commit_markers_finalize_only_after_last_delta() {
        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool = test_pool(&log, 64);
        let ivf = PagedIvf::new(
            open_file(
                &dir.path().join("commits.ivf"),
                &log,
                &pool,
                DurabilityMode::Relaxed,
                3,
                0,
            ),
            params(1),
        );
        ivf.build_from_rows(&[(
            RowId {
                page_id: 1,
                slot_id: 0,
            },
            vec![1.0; 3],
        )])
        .unwrap();
        ivf.begin_transaction_delta(100).unwrap();
        ivf.begin_transaction_delta(101).unwrap();
        ivf.finish_committed_delta(100).unwrap();
        assert!(!ivf.is_finalized());
        ivf.finish_committed_delta(101).unwrap();
        assert!(ivf.is_finalized());
    }

    // ---- IVF bug-audit regression tests (doc_old/ivf-sq8-bugs-audit.md) ------

    /// Audit bug 1: a search walks the list page chain with no list latch and
    /// then re-checks the directory snapshot (`expected.count > entries_seen`),
    /// while a concurrent commit-phase `append_entry` bumps the resident
    /// directory count and chains tail pages. The benign race surfaces as
    /// "ivf list directory chain mismatch" / "corrupt ivf list page chain",
    /// and the search error path sets the sticky `needs_rebuild` flag — every
    /// later search on the index then fails until restart/OPTIMIZE. With one
    /// inverted list, appends and scans always collide.
    #[test]
    fn audit_bug1_search_append_race_remains_searchable() {
        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(parking_lot::Mutex::new(LogManager::open(dir.path()).unwrap()));
        let pool = test_pool(&log, 4096);
        // A multi-page list preserves the append/chain-walk race. Bound the
        // work by completed searches instead of spending 20 seconds asleep
        // or repeatedly checking the same invariant.
        let vecs = gen_vectors(4_096, 768, 0xA11CE);
        let rows: Vec<(RowId, Vec<f32>)> = vecs
            .iter()
            .enumerate()
            .map(|(i, v)| (RowId { page_id: i as u32, slot_id: 0 }, v.clone()))
            .collect();
        let file = open_file(
            &dir.path().join("audit1.ivf"),
            &log,
            &pool,
            DurabilityMode::Relaxed,
            768,
            0,
        );
        let ivf = Arc::new(PagedIvf::new(file, params(1)));
        ivf.build_from_rows(&rows).unwrap();
        assert!(ivf.is_finalized());

        let stop = Arc::new(AtomicBool::new(false));
        let first_error: Arc<parking_lot::Mutex<Option<String>>> =
            Arc::new(parking_lot::Mutex::new(None));

        let start = Arc::new(std::sync::Barrier::new(2));
        let writer_start = start.clone();
        let reader_start = start.clone();
        let writer_stop = stop.clone();
        let writer_ivf = Arc::clone(&ivf);
        let writer = std::thread::spawn(move || {
            let mut s = 7u64;
            let mut next = move || {
                s = s.wrapping_mul(6364136223846793005).wrapping_add(1);
                ((s >> 33) as f32) / 4.0
            };
            let mut rid = 1_000_000u32;
            writer_start.wait();
            while !writer_stop.load(Ordering::Relaxed) {
                let v: Vec<f32> = (0..768).map(|_| next()).collect();
                writer_ivf.insert(RowId { page_id: rid, slot_id: 0 }, &v);
                rid += 1;
            }
            rid - 1_000_000
        });
        let reader_error = first_error.clone();
        let reader_ivf = Arc::clone(&ivf);
        let reader_stop = stop.clone();
        let reader = std::thread::spawn(move || {
            let q = vec![0.5f32; 768];
            reader_start.wait();
            for _ in 0..64 {
                match reader_ivf.try_search_with_stats(&q, 10, 1) {
                    Err(e) => {
                        let mut slot = reader_error.lock();
                        if slot.is_none() {
                            *slot = Some(e.message().to_string());
                        }
                        return;
                    }
                    Ok(_) => {}
                }
            }
            reader_stop.store(true, Ordering::Relaxed);
        });
        reader.join().unwrap();
        stop.store(true, Ordering::Relaxed);
        assert!(writer.join().unwrap() > 0, "writer must overlap the search workload");
        let race_error = first_error.lock().clone();
        assert!(race_error.is_none(), "benign append reported corruption: {race_error:?}");
        assert!(ivf.try_search_with_stats(&vec![0.5f32; 768], 10, 1).is_ok());

    }

    /// B3 regression: finalized-file reopen prepares the reverse map before
    /// publishing the handle, so the first foreground DELETE need not scan it.
    #[test]
    fn audit_bug3_reopen_prepares_reverse_map_before_dml() {
        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(parking_lot::Mutex::new(LogManager::open(dir.path()).unwrap()));
        let pool = test_pool(&log, 8192);
        let vecs = gen_vectors(40_000, 768, 0xC0FFEE); // ~1905 list pages
        let rows: Vec<(RowId, Vec<f32>)> = vecs
            .iter()
            .enumerate()
            .map(|(i, v)| (RowId { page_id: i as u32, slot_id: 0 }, v.clone()))
            .collect();
        let path = dir.path().join("audit3.ivf");
        {
            let file = open_file(&path, &log, &pool, DurabilityMode::Relaxed, 768, 0);
            let ivf = PagedIvf::new(file, params(1));
            ivf.build_from_rows(&rows).unwrap();
            assert!(ivf.is_finalized());
            assert!(ivf.has_live_row_id(rows[10].0)); // build publishes the reverse index
        }
        pool.flush_all().unwrap();
        // "Restart": reopen the finalized file in a fresh PagedIvf instance.
        let file = open_file(&path, &log, &pool, DurabilityMode::Relaxed, 768, 0);
        assert!(file.is_finalized(), "finalized file must be trusted");
        let ivf = PagedIvf::new(file, params(1));

        assert!(ivf.row_index_ready_for_test());
        let t_first = std::time::Instant::now();
        assert_eq!(ivf.mark_deleted_by_row_id(rows[10].0), 1);
        let first_delete = t_first.elapsed();

        let t_second = std::time::Instant::now();
        assert_eq!(ivf.mark_deleted_by_row_id(rows[11].0), 1);
        let second_delete = t_second.elapsed();

        eprintln!("warmed delete: first={first_delete:?}, second={second_delete:?}");
        assert!(!ivf.has_live_row_id(rows[10].0));
        assert!(!ivf.has_live_row_id(rows[11].0));

    }

    /// B5 default policy: do not pay to build a navigator which the first
    /// successful DML would discard. The 100k x 256d gate tests the size floor.
    #[test]
    fn audit_bug5_default_build_skips_disposable_navigator() {
        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(parking_lot::Mutex::new(LogManager::open(dir.path()).unwrap()));
        let pool = test_pool(&log, 2048);
        let vecs = gen_vectors(2_000, 32, 0xD00D);
        let rows: Vec<(RowId, Vec<f32>)> = vecs
            .iter()
            .enumerate()
            .map(|(i, v)| (RowId { page_id: i as u32, slot_id: 0 }, v.clone()))
            .collect();
        let ivf = PagedIvf::new(
            open_file(
                &dir.path().join("audit5.ivf"),
                &log,
                &pool,
                DurabilityMode::Relaxed,
                32,
                0,
            ),
            params(1),
        );
        ivf.build_from_rows(&rows).unwrap();
        assert!(!PagedIvf::navigator_enabled());
        assert!(!ivf.has_navigator(), "default build must skip disposable navigation graph");

        ivf.insert(RowId { page_id: 500_000, slot_id: 0 }, &vec![0.5f32; 32]);
        assert!(!ivf.has_navigator(), "one insert dropped the navigator");
        ivf.try_search_with_stats(&vec![0.5f32; 32], 5, 1)
            .expect("search must still work");
        assert!(
            !ivf.has_navigator(),
            "navigator never comes back in-process"
        );
    }
}
