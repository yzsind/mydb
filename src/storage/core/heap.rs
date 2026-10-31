// Differential benchmark oracle; absent from production builds.
#[cfg(test)]
fn serial_insert_lock_oracle() -> bool {
    static LEGACY: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *LEGACY.get_or_init(|| std::env::var_os("MYDB_TEST_SERIAL_INSERT_LOCKS").is_some())
}

#[cfg(test)]
thread_local! {
    static HEAP_BATCH_PUBLISH_TEST_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);
    static HEAP_BATCH_HEADER_SLOTS_READ: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

// Share the forwarding-aware scan without adding a callback layer to the
// per-row decoder. Expansion preserves each caller's concrete visitor type.
macro_rules! scan_mvcc_heap_page {
    ($heap:ident, $pinned:ident, $page_id:ident, $visit:ident) => {{
        $heap.with_heap_scan_page(&$pinned, |page, start_slot, follow| {
            let Some(page) = page else {
                return Ok(HeapScanPageResult::Done(false));
            };
            for slot_id in start_slot..page.slot_count {
                let row_id = RowId {
                    page_id: $page_id as u32,
                    slot_id: slot_id as u16,
                };
                let Some(payload) = page.payload(slot_id)? else {
                    continue;
                };
                let (header, values) = match decode_heap_payload(payload, &$heap.codec)? {
                    HeapPayload::Row(header, values) => (header, values),
                    HeapPayload::Forward(_) => {
                        if !follow {
                            return Ok(HeapScanPageResult::OwnedTail(slot_id));
                        }
                        let Some(row) = $heap.read_mvcc_following(row_id, 0)? else {
                            continue;
                        };
                        (row.header, row.values)
                    }
                    HeapPayload::RowPiece(_, _)
                    | HeapPayload::RowPieceColumns(..)
                    | HeapPayload::RowColumns(..) => continue,
                };
                if $visit(MvccStoredRow {
                    row_id,
                    header,
                    values,
                })? {
                    return Ok(HeapScanPageResult::Done(true));
                }
            }
            Ok(HeapScanPageResult::Done(false))
        })
    }};
}

enum HeapScanPageResult {
    Done(bool),
    OwnedTail(usize),
}

/// Owned by one scan stream, never shared by workers. The admission stays
/// live for the entire buffer lifetime, including time between morsels.
#[derive(Default)]
pub(crate) struct DirectScanScratch {
    buffer: Vec<std::mem::MaybeUninit<u8>>,
    charge: Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
}

impl Clone for DirectScanScratch {
    fn clone(&self) -> Self {
        // Cloning an unopened scan creates an independent worker, not another
        // owner of the original worker's prepaid capacity.
        Self::default()
    }
}

impl DirectScanScratch {
    fn prepare(&mut self, pages: usize) -> Result<(), QueryError> {
        let mut pages = pages.clamp(1, 1024);
        if self.buffer.len() >= pages * PAGE_SIZE {
            return Ok(());
        }
        // Drop the allocation before releasing its charge. On pressure a
        // smaller buffer still scans exactly the requested page range.
        self.buffer = Vec::new();
        self.charge = None;
        let charge = loop {
            let category =
                crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch;
            if let Some(charge) =
                crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                    category,
                    pages * PAGE_SIZE,
                )
            {
                break charge;
            }
            if pages == 1 {
                break crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                    category, PAGE_SIZE,
                )?;
            }
            pages = (pages / 2).max(1);
        };
        self.buffer = vec![std::mem::MaybeUninit::uninit(); pages * PAGE_SIZE];
        self.charge = Some(charge);
        Ok(())
    }
}

/// Per-output-column source selector used by the predicate-first scan
/// path. For every position in the caller's `output_columns`, the
/// scanner either copies the value from the predicate buffer (when the
/// same column was needed by the predicate) or from a secondary
/// "missing" decode buffer (when only the predicate-projected pass
/// already read it). Computed once per scan in the caller (see
/// `database/scan.rs::PredicateFirstReadPlan`) and passed in by slice,
/// rather than rebuilt inside `scan_mvcc_columns_predicate_first_*`
/// on each call.
#[derive(Debug, Clone, Copy)]
pub(crate) enum OutputSource {
    Predicate(usize),
    Missing(usize),
}

/// Result of the visibility + predicate callback used by random-row
/// predicate-first reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PredicateFirstDecision {
    Accept,
    Reject,
    /// The current heap version is not authoritative for the caller's
    /// snapshot.  Resolve the row through the established undo-aware path.
    Defer,
}

/// Process-wide toggle for the Phase 2 in-place UPDATE patch path
/// (`HeapFile::prepare_mvcc_patch` + `UndoKind::UpdateDelta` +
/// `WalPayload::HeapUpdateLogical`). Defaults ON: the path has been
/// exercised end-to-end on sysbench oltp_read_write and the row-by-
/// row caller already has a complete fallback (`try_in_place_update_delta`
/// returning `None` triggers the legacy full-row rewrite +
/// `UndoKind::Update` path with no observable behaviour change).
/// `MYDB_UPDATE_DELTA=0` (or `false`/`off`/`no`) disables it globally
/// for differential testing.
static UPDATE_DELTA_ENABLED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(true);
static UPDATE_DELTA_ENV_READ: std::sync::Once = std::sync::Once::new();

/// Phase 2 patch-path observability counters. Both counters are
/// monotonically increasing across the process lifetime; the delta
/// between two snapshots tells the bench whether the patch path
/// actually fired (`hit`) vs fell back (`miss`) during a workload.
pub(crate) static UPDATE_DELTA_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static UPDATE_DELTA_MISSES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
static PREPARED_POINT_SPARSE_PATCH_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
pub(crate) fn record_prepared_point_sparse_patch_hit_for_test() {
    PREPARED_POINT_SPARSE_PATCH_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) fn prepared_point_sparse_patch_hits_for_test() -> u64 {
    PREPARED_POINT_SPARSE_PATCH_HITS.load(std::sync::atomic::Ordering::Relaxed)
}

type HeapPatchBytes = smallvec::SmallVec<[u8; 16]>;
type HeapColumnPatch = (usize, HeapPatchBytes, HeapPatchBytes);
type HeapColumnPatches = smallvec::SmallVec<[HeapColumnPatch; 4]>;

fn push_heap_column_patch(
    patches: &mut HeapColumnPatches,
    charge: &mut crate::engine::executor::mem_budget::ControlledMemoryCharge,
    patch: HeapColumnPatch,
) -> bool {
    if patches.len() == patches.capacity() {
        let next = patches.capacity().max(4).saturating_mul(2);
        let old_heap_capacity = if patches.spilled() {
            patches.capacity()
        } else {
            0
        };
        if !charge
            .try_grow_optional((next - old_heap_capacity) * std::mem::size_of::<HeapColumnPatch>())
        {
            return false;
        }
        patches.reserve_exact(next - patches.len());
    }
    patches.push(patch);
    true
}

/// Operation-local preparation. No undo or heap mutation has been published.
/// Spilled patch capacity remains charged until application or cancellation.
struct PreparedHeapPatch {
    row_id: RowId,
    old_last_txn_id_bytes: [u8; 8],
    old_undo_ptr_bytes: [u8; 8],
    col_patches: HeapColumnPatches,
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

pub fn update_delta_hits() -> u64 {
    UPDATE_DELTA_HITS.load(std::sync::atomic::Ordering::Relaxed)
}
pub fn update_delta_misses() -> u64 {
    UPDATE_DELTA_MISSES.load(std::sync::atomic::Ordering::Relaxed)
}

/// Phase 4c: process-wide row-migration counter. Bumped every time an
/// UPDATE outgrows its current page and has to chain to a `ROW_PIECE`.
/// Per-heap `HeapFile::row_migrations` tracks the same thing for the
/// local file; this aggregate lets `SHOW ENGINE MYDB STATUS` and the
/// bench oracle see PCTFREE's effect without iterating every table.
pub(crate) static ROW_MIGRATIONS_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

pub fn row_migrations_total() -> u64 {
    ROW_MIGRATIONS_TOTAL.load(std::sync::atomic::Ordering::Relaxed)
}

#[cfg(test)]
pub(crate) fn reset_row_migrations_for_test() {
    ROW_MIGRATIONS_TOTAL.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// Phase 4c: process-wide UPDATE in-place-vs-migration counters. The
/// patch-path counters (`UPDATE_DELTA_HITS/MISSES`) measure the
/// logical/column-level fast path; these measure the *physical*
/// in-place outcome — was the new row body re-encoded inside the same
/// page (`UPDATE_IN_PLACE`), or did it have to migrate (`UPDATE_MIGRATE`)?
/// The patch path always counts as in-place; the row-rewrite path
/// counts here only after `try_update` succeeds.
pub(crate) static UPDATE_IN_PLACE_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static UPDATE_MIGRATE_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

pub fn update_in_place_total() -> u64 {
    UPDATE_IN_PLACE_TOTAL.load(std::sync::atomic::Ordering::Relaxed)
}
pub fn update_migrate_total() -> u64 {
    UPDATE_MIGRATE_TOTAL.load(std::sync::atomic::Ordering::Relaxed)
}

pub(crate) fn update_delta_enabled() -> bool {
    UPDATE_DELTA_ENV_READ.call_once(|| {
        if let Ok(s) = std::env::var("MYDB_UPDATE_DELTA") {
            // Default is ON. Honour explicit overrides in either direction.
            let lowered = s.to_ascii_lowercase();
            match lowered.as_str() {
                "1" | "true" | "on" | "yes" => {
                    UPDATE_DELTA_ENABLED.store(true, std::sync::atomic::Ordering::Relaxed);
                }
                "0" | "false" | "off" | "no" => {
                    UPDATE_DELTA_ENABLED.store(false, std::sync::atomic::Ordering::Relaxed);
                }
                _ => {}
            }
        }
    });
    UPDATE_DELTA_ENABLED.load(std::sync::atomic::Ordering::Relaxed)
}

#[cfg(test)]
pub(crate) fn set_update_delta_enabled_for_test(on: bool) {
    UPDATE_DELTA_ENV_READ.call_once(|| {});
    UPDATE_DELTA_ENABLED.store(on, std::sync::atomic::Ordering::Relaxed);
}

static UNDO_DELTA_REWRITE_ENV_READ: std::sync::Once = std::sync::Once::new();
static UNDO_DELTA_REWRITE_ENABLED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(true);

/// P1/8 (was P1-3 with default OFF) — sparse undo for the full-row
/// UPDATE rewrite path. The undo FORMAT decision is independent of
/// the heap write shape: even when the heap write goes through the
/// full re-encode path (variable-width column changes, NULL transitions,
/// etc.), the undo record only needs the changed columns' before-image
/// to drive MVCC reconstruction (splice onto the newer version) and
/// rollback (splice onto the current row).
///
/// Default is now ON — the P1-3 differential validation against the
/// full-before-image oracle has been carrying it for the past two
/// cycles, and `undo_delta_rewrite_reconstructs_old_versions_and_rolls_back`
/// exercises the NULL transition + variable-width shapes the in-place
/// patch path could never reach. Set `MYDB_UNDO_DELTA_REWRITE=0` to
/// revert to full-before-image records for incident triage.
///
/// Off-page columns remain ineligible: a full Update undo takes
/// ownership of the old version's LOB chains, and a sparse record
/// can't represent that transfer without leaking them.
pub(crate) fn undo_delta_rewrite_enabled() -> bool {
    UNDO_DELTA_REWRITE_ENV_READ.call_once(|| {
        if let Ok(s) = std::env::var("MYDB_UNDO_DELTA_REWRITE") {
            let on = matches!(s.to_ascii_lowercase().as_str(), "1" | "true" | "on" | "yes");
            UNDO_DELTA_REWRITE_ENABLED.store(on, std::sync::atomic::Ordering::Relaxed);
        }
    });
    UNDO_DELTA_REWRITE_ENABLED.load(std::sync::atomic::Ordering::Relaxed)
}

#[cfg(test)]
pub(crate) fn set_undo_delta_rewrite_enabled_for_test(on: bool) {
    UNDO_DELTA_REWRITE_ENV_READ.call_once(|| {});
    UNDO_DELTA_REWRITE_ENABLED.store(on, std::sync::atomic::Ordering::Relaxed);
}

impl VerifiedDirectHeapChecksums {
    fn try_new(page_count: usize) -> Option<Self> {
        let mut values = Vec::with_capacity(page_count);
        let bytes = values
            .capacity()
            .saturating_mul(std::mem::size_of::<std::sync::atomic::AtomicU64>());
        let charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::DerivedCache,
            bytes,
        )?;
        values.resize_with(page_count, || std::sync::atomic::AtomicU64::new(0));
        Some(Self {
            values: values.into_boxed_slice(),
            _charge: charge,
        })
    }

    #[inline]
    fn contains(&self, page_id: u64, checksum: u32) -> bool {
        let Some(value) = usize::try_from(page_id)
            .ok()
            .and_then(|page_id| self.values.get(page_id))
        else {
            return false;
        };
        value.load(std::sync::atomic::Ordering::Acquire) == u64::from(checksum) + 1
    }

    #[inline]
    fn remember(&self, page_id: u64, checksum: u32) {
        if let Some(value) = usize::try_from(page_id)
            .ok()
            .and_then(|page_id| self.values.get(page_id))
        {
            value.store(
                u64::from(checksum) + 1,
                std::sync::atomic::Ordering::Release,
            );
        }
    }
}

// Expand the shared forwarding logic at each decoder's original entry point.
// Projection arguments remain direct parameters on this per-row hot path.
macro_rules! projected_mvcc_reader {
    ($name:ident, $projection:ident: $ty:ty, $decode:path) => {
        fn $name(
            &self,
            row_id: RowId,
            $projection: $ty,
            depth: usize,
        ) -> Result<Option<(RowHeader, Vec<Value>, usize)>, QueryError> {
            if depth > MAX_FORWARDING_DEPTH {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    &format!(
                        "heap row migration chain too deep at row \
                         table={} page={} slot={} (depth={}, max={})",
                        self.table_id, row_id.page_id, row_id.slot_id, depth, MAX_FORWARDING_DEPTH
                    ),
                ));
            }
            if (row_id.page_id as u64) >= self.page_count()? {
                return Ok(None);
            }
            let payload = self.with_page(row_id.page_id as u64, |page| {
                let Some(payload) = page.payload(row_id.slot_id as usize)? else {
                    return Ok(None);
                };
                $decode(payload, $projection, &self.codec).map(Some)
            })?;
            match payload {
                Some(HeapPayload::RowColumns(header, values, stored_count)) => {
                    Ok(Some((header, values, stored_count)))
                }
                Some(HeapPayload::Row(header, values)) => {
                    let stored_count = values.len();
                    Ok(Some((header, values, stored_count)))
                }
                Some(HeapPayload::Forward(target)) => self.$name(target, $projection, depth + 1),
                Some(HeapPayload::RowPieceColumns(header, values, stored_count)) if depth > 0 => {
                    Ok(Some((header, values, stored_count)))
                }
                Some(HeapPayload::RowPiece(header, values)) if depth > 0 => {
                    let stored_count = values.len();
                    Ok(Some((header, values, stored_count)))
                }
                Some(HeapPayload::RowPiece(_, _) | HeapPayload::RowPieceColumns(..)) | None => {
                    Ok(None)
                }
            }
        }
    };
}

impl HeapFile {
    #[cfg(test)]
    fn open(
        path: PathBuf,
        table_id: u64,
        log: Arc<crate::wal::LogMutex>,
    ) -> Result<Self, QueryError> {
        let pool = BufferPool::new(
            DEFAULT_BUFFER_POOL_PAGES,
            log.clone(),
            DurabilityMode::from_env(),
            Duration::from_millis(DEFAULT_LOCK_WAIT_MS),
        );
        Self::open_with_buffer_pool(
            path,
            table_id,
            log,
            DurabilityMode::from_env(),
            DEFAULT_HEAP_UPDATE_RESERVE_BYTES,
            pool,
            test_default_codec(),
        )
    }

    #[cfg(test)]
    fn open_with_durability(
        path: PathBuf,
        table_id: u64,
        log: Arc<crate::wal::LogMutex>,
        durability: DurabilityMode,
        update_reserve_bytes: usize,
    ) -> Result<Self, QueryError> {
        let pool = BufferPool::new(
            DEFAULT_BUFFER_POOL_PAGES,
            log.clone(),
            durability,
            Duration::from_millis(DEFAULT_LOCK_WAIT_MS),
        );
        Self::open_with_buffer_pool(
            path,
            table_id,
            log,
            durability,
            update_reserve_bytes,
            pool,
            test_default_codec(),
        )
    }

    fn open_with_buffer_pool(
        path: PathBuf,
        table_id: u64,
        log: Arc<crate::wal::LogMutex>,
        durability: DurabilityMode,
        update_reserve_bytes: usize,
        buffer_pool: Arc<BufferPool>,
        codec: Arc<RowCodec>,
    ) -> Result<Self, QueryError> {
        let file = if crate::storage::naming::parse_relative_object_path(
            &crate::storage::naming::wal_relative_object_path(&path),
        )
        .is_some()
            && !crate::storage::data_dictionary::is_dd_table_id(table_id)
            && !is_temp_table_id(table_id)
        {
            OpenOptions::new()
                .read(true)
                .write(!crate::storage::image_io::immutable_path(&path))
                .open(&path)
                .map_err(|err| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        format!("open heap '{}': {err}", path.display()),
                    )
                })?
        } else {
            crate::storage::image_io::open_data_file(&path).map_err(io_err)?
        };
        if file.metadata().map_err(io_err)?.len() == 0 {
            let relative = crate::storage::naming::wal_relative_object_path(&path);
            let standalone =
                crate::storage::naming::parse_relative_object_path(&relative).is_none();
            let mut root = path.clone();
            for _ in Path::new(&relative).components() {
                root.pop();
            }
            let initializing_dd = crate::storage::data_dictionary::is_dd_table_id(table_id)
                && crate::storage::control::Control::load(&root)?.is_some_and(|c| !c.initialized);
            if !standalone && !initializing_dd && !is_temp_table_id(table_id) {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "existing heap allocation metadata is missing",
                ));
            }
            let mut meta = encode_heap_meta(&path, 1);
            let lsn = if durability == DurabilityMode::Memory {
                0
            } else {
                log.lock().append(
                    0,
                    WalPayload::full_page_image(
                        crate::storage::naming::wal_relative_object_path(&path),
                        0,
                        meta.clone(),
                    ),
                )?
            };
            set_page_lsn_if_formatted(&mut meta, lsn);
            file.set_len(PAGE_SIZE as u64).map_err(io_err)?;
            buffer_pool.store_page(
                PageKey {
                    file: Arc::new(BufferPool::heap_file_key(path.clone(), table_id)),
                    page_id: 0,
                },
                meta,
                lsn,
            )?;
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            // Direct heap scans bypass the governed raw-page cache. Promoting
            // their one-pass kernel copies can starve anonymous allocations in
            // a cgroup even with ample managed headroom. Match buffer-pool
            // handles' NOREUSE policy while retaining sequential readahead.
            // This is advisory and never discards dirty data or changes sync.
            // SAFETY: the descriptor stays open and no userspace buffer is passed.
            unsafe {
                libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_NOREUSE);
            }
        }
        let len = file.metadata().map_err(io_err)?.len();
        if len % PAGE_SIZE as u64 != 0 {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                format!(
                    "corrupt heap file length: '{}' has {} bytes (page size {})",
                    path.display(),
                    len,
                    PAGE_SIZE
                ),
            ));
        }
        let file_key = Arc::new(BufferPool::heap_file_key(path.clone(), table_id));
        let rel_path = crate::storage::naming::wal_relative_object_path(&path);
        // CachePadded forces 128-byte alignment so adjacent latches don't
        // share a cache line (see HeapFile::page_latches doc). Count is
        // `resolved_page_latch_count()` (env-overridable).
        let (page_latches, page_latch_mask) = new_heap_page_latch_array();
        let meta_bytes = buffer_pool.read_raw_page(PageKey {
            file: file_key.clone(),
            page_id: 0,
        })?;
        let meta_count = decode_heap_meta(&meta_bytes, &file_key.owner)?;
        if file.metadata().map_err(io_err)?.len() / (PAGE_SIZE as u64) < meta_count {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "heap file is shorter than its allocated extent",
            ));
        }
        let page_count = meta_count;
        let verified_direct_checksums = usize::try_from(page_count)
            .ok()
            .and_then(VerifiedDirectHeapChecksums::try_new);
        Ok(Self {
            path,
            file,
            table_id,
            durability: std::sync::atomic::AtomicU8::new(durability_to_u8(durability)),
            update_reserve_bytes: update_reserve_bytes.min(MAX_HEAP_UPDATE_RESERVE_BYTES),
            page_count: std::sync::atomic::AtomicU64::new(page_count),
            file_key,
            buffer_pool,
            row_migrations: std::sync::atomic::AtomicU64::new(0),
            verified_direct_checksums,
            log,
            extend_mutex: Mutex::new(()),
            free_pages: HeapFreePages::default(),
            page_latches,
            page_latch_mask,
            rel_path,
            codec,
            lob_reader: None,
            discard_pages_on_drop: true,
        })
    }

    /// Run `f` with this heap's off-page LOB reader installed as the
    /// thread-local materializer, so any externalized column decoded
    /// inside `f` is rebuilt from the overflow file. Cheap thread-local
    /// save/restore; a no-op when the table can't externalize
    /// (`lob_reader` is `None`).
    fn with_lob<R>(&self, f: impl FnOnce() -> Result<R, QueryError>) -> Result<R, QueryError> {
        with_lob_materializer(self.lob_reader.clone(), f)
    }

    fn validate(path: PathBuf, _table_id: u64, max_lsn: u64) -> Result<usize, QueryError> {
        if !path.exists() {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "published heap file is missing",
            ));
        }
        let owner = page_owner(&path);
        let mut file = OpenOptions::new().read(true).open(path).map_err(io_err)?;
        let len = file.metadata().map_err(io_err)?.len();
        if len % PAGE_SIZE as u64 != 0 {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt heap file length",
            ));
        }
        let mut meta = vec![0; PAGE_SIZE];
        file.read_exact(&mut meta).map_err(io_err)?;
        let page_count = decode_heap_meta(&meta, &owner)?;
        if len / (PAGE_SIZE as u64) < page_count {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "heap allocated extent is truncated",
            ));
        }
        file.seek(SeekFrom::Start(0)).map_err(io_err)?;
        let mut pages_per_read = 64usize;
        let _read_charge = loop {
            let category =
                crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch;
            if let Some(charge) =
                crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                    category,
                    pages_per_read * PAGE_SIZE,
                )
            {
                break charge;
            }
            if pages_per_read == 1 {
                break crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                    category, PAGE_SIZE,
                )?;
            }
            pages_per_read /= 2;
        };
        let mut buffer = vec![0; pages_per_read * PAGE_SIZE];
        let mut checked = 0usize;
        while (checked as u64) < page_count {
            let pages = (page_count - checked as u64).min(pages_per_read as u64) as usize;
            file.read_exact(&mut buffer[..pages * PAGE_SIZE])
                .map_err(io_err)?;
            for buf in buffer[..pages * PAGE_SIZE].chunks_exact(PAGE_SIZE) {
                validate_page_owner(buf, &owner, checked as u64)?;
                if checked > 0 && buf.iter().all(|b| *b == 0) {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "allocated heap page is zero",
                    ));
                }
                if max_lsn > 0 && page_lsn(buf) > max_lsn {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt heap page: page LSN is ahead of WAL",
                    ));
                }
                if validate_heap_page_header(buf)? {
                    // Keep all checksum, slot-boundary and MVCC-header checks,
                    // without allocating an owned page and slot vector merely
                    // to inspect the bytes already in the sequential buffer.
                    let page = HeapPageView::from_validated(buf);
                    for slot_id in 0..page.slot_count {
                        if let Some(payload) = page.payload(slot_id)? {
                            let _ = decode_heap_payload_header(payload)?;
                        }
                    }
                }
                checked += 1;
            }
        }
        Ok(checked)
    }

    fn insert_mvcc(
        &self,
        txn_id: u64,
        header: RowHeader,
        values: &[Value],
    ) -> Result<RowId, QueryError> {
        self.insert_mvcc_with_flush(txn_id, header, values)
    }

    fn insert_mvcc_with_flush(
        &self,
        txn_id: u64,
        header: RowHeader,
        values: &[Value],
    ) -> Result<RowId, QueryError> {
        let payload = encode_mvcc_row(header, values, &self.codec, &[])?;
        if payload.len() + PAGE_HEADER_SIZE + SLOT_SIZE > PAGE_SIZE {
            return Err(error(
                crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                "row size is larger than one heap page",
            ));
        }
        loop {
            let page_count = self.page_count()?;
            let start_page = page_count.saturating_sub(1).max(1);
            for page_id in start_page..page_count {
                let _page_guard = self.page_latch(page_id).lock().map_err(|_| {
                    error(crate::error::catalog::ER_GET_ERRNO, "page latch poisoned")
                })?;
                let plan = self.with_page(page_id, |page| {
                    HeapPageMut::plan_insert(page.bytes, payload.len(), self.update_reserve_bytes)
                })?;
                if let Some(plan) = plan {
                    let row_id = RowId {
                        page_id: page_id as u32,
                        slot_id: plan.slot_id,
                    };
                    self.write_heap_insert_raw_logged(
                        txn_id,
                        row_id,
                        payload.clone(),
                        plan,
                        header.undo_ptr,
                    )?;
                    return Ok(row_id);
                }
            }
            // Tail page didn't fit (or table is empty): serialise file
            // extension and retry. The `extend_mutex` is per heap, so
            // two concurrent extenders queue here — short critical
            // section (just `set_len` + atomic store).
            let extend_guard = self
                .extend_mutex
                .lock()
                .map_err(|_| error(crate::error::catalog::ER_GET_ERRNO, "extend mutex poisoned"))?;
            let cur_count = self.page_count()?;
            if cur_count > page_count {
                drop(extend_guard);
                continue;
            }
            let new_page_id = cur_count;
            let new_count = cur_count
                .checked_add(1)
                .filter(|n| *n <= u64::from(u32::MAX) + 1)
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                        "heap row-id address space exhausted",
                    )
                })?;
            crate::storage::temp_quota::reserve_extent(&self.path, new_count * PAGE_SIZE as u64)?;
            self.file
                .set_len(new_count * PAGE_SIZE as u64)
                .map_err(io_err)?;
            self.set_page_count(new_count)?;
            let _page_guard = self
                .page_latch(new_page_id)
                .lock()
                .map_err(|_| error(crate::error::catalog::ER_GET_ERRNO, "page latch poisoned"))?;
            drop(extend_guard);
            let mut page = Page::new();
            let slot_id = page
                .insert(&payload)
                .expect("fresh page should have enough space");
            let row_id = RowId {
                page_id: new_page_id as u32,
                slot_id,
            };
            self.write_heap_insert_logged(txn_id, row_id, payload, page, header.undo_ptr)?;
            return Ok(row_id);
        }
    }

    /// Place one row into a page whose slots were reclaimed after DELETE /
    /// UPDATE purge instead of appending at the tail. Without this, a table
    /// under steady delete+insert churn grows by every inserted row forever,
    /// spreading key ranges over ever more pages, and every insert serializes
    /// on the tail page. Returns `None` when no advertised page fits, leaving
    /// the caller on the ordinary tail path.
    fn try_insert_into_free_page(
        &self,
        txn_id: u64,
        header: RowHeader,
        values: &[Value],
        external: &[Option<LobRef>],
    ) -> Result<Option<RowId>, QueryError> {
        const MAX_PROBES: usize = 4;
        if self.free_pages.is_empty() {
            return Ok(None);
        }
        let payload = encode_mvcc_row(header, values, &self.codec, external)?;
        if payload.len() + PAGE_HEADER_SIZE + SLOT_SIZE > PAGE_SIZE {
            return Err(error(
                crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                "row size is larger than one heap page",
            ));
        }
        for _ in 0..MAX_PROBES {
            let Some(page_id) = self.free_pages.take() else {
                return Ok(None);
            };
            let page_id = u64::from(page_id);
            if page_id + 1 >= self.page_count()? {
                continue;
            }
            let _page_guard = self
                .page_latch(page_id)
                .lock()
                .map_err(|_| error(crate::error::catalog::ER_GET_ERRNO, "page latch poisoned"))?;
            let plan = self.with_page(page_id, |page| {
                HeapPageMut::plan_insert(page.bytes, payload.len(), self.update_reserve_bytes)
            })?;
            let Some(plan) = plan else {
                // Full again (or never had room for this row width): stop
                // advertising it until another slot is reclaimed there.
                continue;
            };
            let row_id = RowId {
                page_id: page_id as u32,
                slot_id: plan.slot_id,
            };
            self.write_heap_insert_raw_logged(txn_id, row_id, payload, plan, header.undo_ptr)?;
            // More reclaimed slots may remain; a later probe that finds the
            // page full drops it.
            self.free_pages.offer(row_id.page_id);
            return Ok(Some(row_id));
        }
        Ok(None)
    }

    fn insert_mvcc_batch(
        &self,
        txn_id: u64,
        headers: &[RowHeader],
        rows: &[Vec<Value>],
        externals: &[Vec<Option<LobRef>>],
    ) -> Result<Vec<RowId>, QueryError> {
        if headers.len() != rows.len() {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "heap insert batch length mismatch",
            ));
        }
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        if rows.len() == 1 && !crate::storage::data_dictionary::is_dd_table_id(self.table_id) {
            let external = externals.first().map(Vec::as_slice).unwrap_or(&[]);
            if let Some(row_id) =
                self.try_insert_into_free_page(txn_id, headers[0], &rows[0], external)?
            {
                return Ok(vec![row_id]);
            }
        }
        // Encode batches before taking the heap append latch. Single-row
        // inserts keep their allocation-free descriptor path.
        let mut payload_charge = None;
        let mut prepared = None;
        let reduced = (rows.len() >= 16
            || crate::storage::data_dictionary::is_dd_table_id(self.table_id))
            && txn_reduced_redo_active(&self.log, txn_id);
        let prepare_batch = rows.len() >= 16 || reduced;
        #[cfg(test)]
        let prepare_batch = prepare_batch && !serial_insert_lock_oracle();
        if prepare_batch {
            let bytes = rows.iter().fold(
                rows.len().saturating_mul(std::mem::size_of::<Vec<u8>>()),
                |total, row| {
                    total.saturating_add(row.iter().fold(128usize, |size, value| {
                        size.saturating_add(64)
                            .saturating_add(value.heap_capacity())
                    }))
                },
            );
            payload_charge = Some(
                crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                    bytes,
                )?,
            );
            let mut payloads = Vec::with_capacity(rows.len());
            let mut payload_bytes = 0usize;
            for (idx, (header, values)) in headers.iter().copied().zip(rows).enumerate() {
                let external = externals.get(idx).map(Vec::as_slice).unwrap_or(&[]);
                let payload = encode_mvcc_row(header, values, &self.codec, external)?;
                if payload.len() + PAGE_HEADER_SIZE + SLOT_SIZE > PAGE_SIZE {
                    return Err(error(
                        crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                        "row size is larger than one heap page",
                    ));
                }
                payload_bytes = payload_bytes.saturating_add(payload.len() + SLOT_SIZE);
                payloads.push(payload);
            }
            // Bound tail-page waste by choosing private ranges only for
            // substantial byte batches. Tiny inserts continue to fill the
            // existing tail, independent of SQL shape or table/index types.
            if reduced || payload_bytes >= 8 * PAGE_SIZE {
                return self.insert_mvcc_batch_pre_encoded_inner(
                    txn_id,
                    payloads,
                    Some(headers),
                    reduced,
                );
            }
            prepared = Some(payloads.into_iter());
        }
        let _payload_charge = payload_charge;

        // Small batches fill the existing tail under `extend_mutex`.
        // Concurrent SELECTs and single-row UPDATE/DELETE on other
        // pages still run free; what we exclude is a second batch
        // insert and any single-row insert that needs to extend the
        // file.
        let _extend_guard = self
            .extend_mutex
            .lock()
            .map_err(|_| error(crate::error::catalog::ER_GET_ERRNO, "extend mutex poisoned"))?;
        let old_page_count = self.page_count()?;
        let mut page_count = old_page_count;
        let mut page_id = page_count.saturating_sub(1);
        // Acquire the tail page's latch before reading-for-modify so
        // an in-flight single-row insert on the same tail page can't
        // race with us. The latch array is `page_latch_count`-wide and
        // indexed by `page_id & page_latch_mask`; a batch that visits
        // more than that many distinct pages would otherwise
        // re-acquire a latch it already holds and self-deadlock on
        // the second `lock()`. `held_latches` is a MAX_PAGE_LATCH_COUNT-bit
        // bitmap of bucket indices we already own — once we own a bucket, every
        // page mapping to that bucket is implicitly under our control
        // for the rest of the batch.
        let mut latches: Vec<MutexGuard<'_, ()>> = Vec::new();
        let mut held_latches = [0u64; MAX_PAGE_LATCH_COUNT / 64];
        macro_rules! take_latch_for {
            ($pid:expr) => {{
                let idx = ($pid & self.page_latch_mask) as usize;
                let word = idx / 64;
                let bit = 1u64 << (idx % 64);
                if held_latches[word] & bit == 0 {
                    latches.push(self.page_latch($pid).lock().map_err(|_| {
                        error(crate::error::catalog::ER_GET_ERRNO, "page latch poisoned")
                    })?);
                    held_latches[word] |= bit;
                }
            }};
        }
        if page_count > 1 {
            take_latch_for!(page_id);
        }
        let mut append_only_page = page_count <= 1;
        let mut page = if append_only_page {
            page_id = 1;
            page_count = 2;
            // Brand-new page: take its latch too.
            take_latch_for!(page_id);
            Page::new()
        } else {
            self.read_page_for_modify(page_id)?
        };
        let mut touched_pages = BTreeMap::new();
        let mut row_ids = Vec::with_capacity(rows.len());
        let mut wal_inserts = Vec::with_capacity(rows.len());

        for (idx, (header, values)) in headers.iter().copied().zip(rows).enumerate() {
            let external = externals.get(idx).map(|m| m.as_slice()).unwrap_or(&[]);
            let payload = match prepared.as_mut() {
                Some(payloads) => payloads.next().expect("prepared batch length"),
                None => encode_mvcc_row(header, values, &self.codec, external)?,
            };
            if payload.len() + PAGE_HEADER_SIZE + SLOT_SIZE > PAGE_SIZE {
                return Err(error(
                    crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                    "row size is larger than one heap page",
                ));
            }
            let slot_id = match if append_only_page {
                page.insert_append_only_with_reserve(&payload, self.update_reserve_bytes)
            } else {
                page.insert_with_reserve(&payload, self.update_reserve_bytes)
            } {
                Some(slot_id) => slot_id,
                None => {
                    touched_pages.insert(page_id, page);
                    let next_count = page_count
                        .checked_add(1)
                        .filter(|count| *count <= u64::from(u32::MAX) + 1)
                        .ok_or_else(|| {
                            error(
                                crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                                "heap row-id address space exhausted",
                            )
                        })?;
                    page_id = page_count;
                    page_count = next_count;
                    take_latch_for!(page_id);
                    page = Page::new();
                    append_only_page = true;
                    page.insert_append_only_with_reserve(&payload, 0)
                        .expect("fresh page should have enough space")
                }
            };
            let row_id = RowId {
                page_id: page_id as u32,
                slot_id,
            };
            wal_inserts.push(WalHeapInsert {
                page_id: row_id.page_id,
                slot_id,
                payload,
                undo_lsn: header.undo_ptr,
            });
            row_ids.push(row_id);
        }
        touched_pages.insert(page_id, page);

        if page_count > old_page_count {
            crate::storage::temp_quota::reserve_extent(&self.path, page_count * PAGE_SIZE as u64)?;
            self.file
                .set_len(page_count * PAGE_SIZE as u64)
                .map_err(io_err)?;
            self.set_page_count(page_count)?;
        }
        self.write_heap_insert_batch_logged(txn_id, wal_inserts, touched_pages, false, None)?;
        drop(latches);
        Ok(row_ids)
    }

    /// Variant of `insert_mvcc_batch` that takes already-encoded row
    /// payloads — exactly what the parallel LOAD DATA path's worker
    /// threads produce. The writer thread is the bottleneck on parallel
    /// LOAD DATA (it issues every `Page::insert` + WAL append), so
    /// moving per-row encoding off this hot path frees ~25% of writer
    /// cycles on profile-p2-fix1 (4-thread sf=1 lineitem).
    ///
    /// Callers are responsible for: (a) constraint checks (NOT NULL /
    /// CHECK / unique) — done by the worker before encoding; (b) auto-
    /// increment value assignment — incompatible with this fast path,
    /// callers must use `insert_rows_heap_only` instead when the table
    /// has an AUTO_INCREMENT column. The producer must call
    /// `encode_visible_row_payload(values, codec, txn_id)` with the
    /// same `txn_id` passed here so the in-payload MVCC header is
    /// consistent with the row's `last_txn_id`.
    pub(crate) fn insert_mvcc_batch_pre_encoded(
        &self,
        txn_id: u64,
        payloads: Vec<Vec<u8>>,
    ) -> Result<Vec<RowId>, QueryError> {
        self.insert_mvcc_batch_pre_encoded_inner(txn_id, payloads, None, true)
    }

    fn insert_mvcc_batch_pre_encoded_inner(
        &self,
        txn_id: u64,
        payloads: Vec<Vec<u8>>,
        headers: Option<&[RowHeader]>,
        direct_fresh_append: bool,
    ) -> Result<Vec<RowId>, QueryError> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }

        // Preflight the exact number of fresh pages before allocating them.
        // Every page is private to this batch, so workers can build pages in
        // parallel and hold `extend_mutex` only while reserving a disjoint
        // file range. Starting on a fresh page wastes at most one tail page
        // per large LOAD batch, while removing the file-wide critical section
        // from all page construction, WAL preparation, and writeback.
        let mut estimated_pages = 1usize;
        let mut free_space = PAGE_SIZE - PAGE_HEADER_SIZE;
        let mut rows_on_page = 0usize;
        for payload in &payloads {
            if payload.len() + PAGE_HEADER_SIZE + SLOT_SIZE > PAGE_SIZE {
                return Err(error(
                    crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                    "row size is larger than one heap page",
                ));
            }
            let required = payload.len() + SLOT_SIZE;
            let reserve = if rows_on_page == 0 {
                0
            } else {
                self.update_reserve_bytes
            };
            if free_space < required || free_space.saturating_sub(required) < reserve {
                estimated_pages = estimated_pages.saturating_add(1);
                free_space = PAGE_SIZE - PAGE_HEADER_SIZE;
                rows_on_page = 0;
            }
            free_space -= required;
            rows_on_page += 1;
        }
        // `Page::data`, slot directories, row-id output, WAL descriptors and
        // the page map all live until writeback completes. Payload buffers are
        // already owned and charged by LoadDataPreparedBatch; count only their
        // descriptors here so the same bytes are not double-reserved.
        let scratch_bytes = estimated_pages
            .saturating_mul(PAGE_SIZE.saturating_add(std::mem::size_of::<Page>() + 64))
            .saturating_add(estimated_pages.min(128).saturating_mul(PAGE_SIZE))
            .saturating_add(payloads.len().saturating_mul(
                std::mem::size_of::<Slot>()
                    + std::mem::size_of::<RowId>()
                    + std::mem::size_of::<WalHeapInsert>(),
            ));
        let _scratch_charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
            scratch_bytes,
        )?;

        let mut pages = Vec::with_capacity(estimated_pages);
        let mut page = Page::new();
        let mut relative_page_id = 0u64;
        let mut row_ids = Vec::with_capacity(payloads.len());
        let mut wal_inserts = Vec::with_capacity(payloads.len());

        for (ordinal, payload) in payloads.into_iter().enumerate() {
            let reserve = if page.slots.is_empty() {
                0
            } else {
                self.update_reserve_bytes
            };
            let slot_id = match page.insert_append_only_with_reserve(&payload, reserve) {
                Some(slot_id) => slot_id,
                None => {
                    pages.push(page);
                    relative_page_id += 1;
                    page = Page::new();
                    page.insert_append_only_with_reserve(&payload, 0)
                        .expect("fresh page should have enough space")
                }
            };
            let row_id = RowId {
                page_id: relative_page_id as u32,
                slot_id,
            };
            wal_inserts.push(WalHeapInsert {
                page_id: row_id.page_id,
                slot_id,
                payload,
                undo_lsn: headers.and_then(|headers| headers[ordinal].undo_ptr),
            });
            row_ids.push(row_id);
        }
        pages.push(page);
        debug_assert_eq!(pages.len(), estimated_pages);

        let (first_page_id, fresh_extent, _tail_guard) =
            {
                let _extend_guard = self.extend_mutex.lock().map_err(|_| {
                    error(crate::error::catalog::ER_GET_ERRNO, "extend mutex poisoned")
                })?;
                let first_page_id = self.page_count()?;
                let new_page_count = first_page_id
                    .checked_add(pages.len() as u64)
                    .filter(|count| *count <= u64::from(u32::MAX) + 1)
                    .ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                            "heap page id exceeds supported range",
                        )
                    })?;
                let fresh_extent = self
                    .append_reduced_file_extend_wal(txn_id, first_page_id, new_page_count)?
                    .map(|lsn| (first_page_id, lsn));
                crate::storage::temp_quota::reserve_extent(
                    &self.path,
                    new_page_count.saturating_mul(PAGE_SIZE as u64),
                )?;
                self.file
                    .set_len(new_page_count.saturating_mul(PAGE_SIZE as u64))
                    .map_err(io_err)?;
                // Readers may observe reserved zero pages, but a tail writer
                // must wait until this batch has published its final page. Take
                // the latch before advertising the new high-water mark.
                let tail_guard = self.page_latch(new_page_count - 1).lock().map_err(|_| {
                    error(crate::error::catalog::ER_GET_ERRNO, "page latch poisoned")
                })?;
                self.set_page_count_with_extent(new_page_count, fresh_extent.map(|(_, lsn)| lsn))?;
                (first_page_id, fresh_extent, tail_guard)
            };

        for row_id in &mut row_ids {
            row_id.page_id = first_page_id
                .saturating_add(row_id.page_id as u64)
                .try_into()
                .map_err(|_| {
                    error(
                        crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                        "heap page id exceeds supported range",
                    )
                })?;
        }
        for insert in &mut wal_inserts {
            insert.page_id = first_page_id
                .saturating_add(insert.page_id as u64)
                .try_into()
                .map_err(|_| {
                    error(
                        crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                        "heap page id exceeds supported range",
                    )
                })?;
        }
        let touched_pages = pages
            .into_iter()
            .enumerate()
            .map(|(offset, page)| (first_page_id + offset as u64, page))
            .collect();
        self.write_heap_insert_batch_logged(
            txn_id,
            wal_inserts,
            touched_pages,
            direct_fresh_append,
            fresh_extent,
        )?;
        Ok(row_ids)
    }

    /// Append the compact recovery contract before extending a heap file in a
    /// reduced-redo transaction. The record is deliberately eager (never
    /// transaction-buffered): a background flusher may persist a new page as
    /// soon as it is admitted, so recovery must already know the old length.
    fn append_reduced_file_extend_wal(
        &self,
        txn_id: u64,
        old_page_count: u64,
        new_page_count: u64,
    ) -> Result<Option<u64>, QueryError> {
        if self.durability() == DurabilityMode::Memory
            || !txn_reduced_redo_active(&self.log, txn_id)
            || new_page_count <= old_page_count
        {
            return Ok(None);
        }
        let relative_path = self.relative_path();
        let payload = WalPayload::FileExtend {
            path: relative_path.clone(),
            old_len: old_page_count.saturating_mul(PAGE_SIZE as u64),
            new_len: new_page_count.saturating_mul(PAGE_SIZE as u64),
        };
        let pre = crate::wal::LogManager::pre_encode_record(txn_id, payload)?;
        let lsn = crate::wal::LogManager::append_pre_encoded_to(&self.log, pre)?;
        note_reduced_redo_file(&self.log, txn_id, relative_path);
        Ok(Some(lsn))
    }

    /// Append a batch to a private, unpublished shadow heap without going
    /// through transactional DML or the buffer pool.
    ///
    /// ALTER TABLE ... ALGORITHM=COPY owns the shadow storage id exclusively
    /// until the build-then-swap commit point.  Consequently there is no
    /// reader that could observe a partially written page and no recovery
    /// obligation for individual row writes: recovery either removes the
    /// whole shadow generation or finishes the already-fsynced swap.  Writing
    /// complete heap pages here avoids row locks, undo, per-row WAL and dirty
    /// buffer frames while preserving the ordinary on-disk row/page format.
    ///
    /// The caller must serialize calls for this heap and must fsync the file
    /// before publishing it.  Off-page values are intentionally not accepted;
    /// the ALTER COPY orchestrator routes LOB-capable schemas through the
    /// existing compatibility path until an overflow direct builder exists.
    pub(crate) fn shadow_payload_batch_direct_scratch_capacity(payloads: &[&[u8]]) -> usize {
        let estimated_page_bytes = payloads
            .iter()
            .map(|payload| payload.len().saturating_add(SLOT_SIZE))
            .fold(PAGE_SIZE, usize::saturating_add);
        let estimated_pages = estimated_page_bytes.div_ceil(PAGE_SIZE).saturating_add(1);
        estimated_pages
            .saturating_mul(PAGE_SIZE)
            .saturating_add(payloads.len().saturating_mul(std::mem::size_of::<RowId>()))
    }

    pub(crate) fn prepare_shadow_payload_batch_direct(
        build_lsn: u64,
        payloads: &[&[u8]],
    ) -> Result<ShadowPreparedHeapBatch, QueryError> {
        if payloads.is_empty() {
            return Ok(ShadowPreparedHeapBatch {
                encoded_pages: Vec::new(),
                row_ids: Vec::new(),
            });
        }
        let mut page_id = 0_u64;
        let mut page = Page::new();
        let estimated_page_bytes = payloads
            .iter()
            .map(|payload| payload.len().saturating_add(SLOT_SIZE))
            .fold(PAGE_SIZE, usize::saturating_add);
        let estimated_pages = estimated_page_bytes.div_ceil(PAGE_SIZE).saturating_add(1);
        // Every completed page already owns a 16 KiB buffer. Reserve the
        // coalesced write vector once so extending it does not repeatedly
        // copy the earlier pages in this batch.
        let mut encoded_pages = Vec::<u8>::with_capacity(estimated_pages.saturating_mul(PAGE_SIZE));
        let mut row_ids = Vec::with_capacity(payloads.len());

        for &payload in payloads {
            if payload.len() + PAGE_HEADER_SIZE + SLOT_SIZE > PAGE_SIZE {
                return Err(error(
                    crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                    "row size is larger than one heap page",
                ));
            }
            let slot_id = match page.insert(payload) {
                Some(slot_id) => slot_id,
                None => {
                    page.set_lsn(build_lsn);
                    encoded_pages.extend_from_slice(&page.encode_into_owned());
                    page_id = page_id.saturating_add(1);
                    page = Page::new();
                    page.insert(payload)
                        .expect("fresh page should have enough space")
                }
            };
            row_ids.push(RowId {
                page_id: page_id as u32,
                slot_id,
            });
        }
        page.set_lsn(build_lsn);
        encoded_pages.extend_from_slice(&page.encode_into_owned());

        let written_pages = encoded_pages.len() / PAGE_SIZE;
        debug_assert_eq!(encoded_pages.len() % PAGE_SIZE, 0);
        debug_assert_eq!(written_pages, page_id as usize + 1);
        Ok(ShadowPreparedHeapBatch {
            encoded_pages,
            row_ids,
        })
    }

    pub(crate) fn append_prepared_shadow_batch_direct(
        &self,
        mut batch: ShadowPreparedHeapBatch,
    ) -> Result<Vec<RowId>, QueryError> {
        if batch.encoded_pages.is_empty() {
            return Ok(batch.row_ids);
        }
        let first_page_id = self.reserve_prepared_shadow_batch_direct(&mut batch)?;
        self.write_reserved_shadow_batch_direct(batch, first_page_id)
    }

    pub(crate) fn reserve_prepared_shadow_batch_direct(
        &self,
        batch: &mut ShadowPreparedHeapBatch,
    ) -> Result<u64, QueryError> {
        let written_pages = batch.encoded_pages.len() / PAGE_SIZE;
        let first_page_id = {
            let _extend_guard = self
                .extend_mutex
                .lock()
                .map_err(|_| error(crate::error::catalog::ER_GET_ERRNO, "extend mutex poisoned"))?;
            let first_page_id = self.page_count()?;
            let new_page_count = first_page_id
                .checked_add(written_pages as u64)
                .filter(|count| *count <= u64::from(u32::MAX) + 1)
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                        "shadow heap address space exhausted",
                    )
                })?;
            crate::storage::temp_quota::reserve_extent(
                &self.path,
                new_page_count * PAGE_SIZE as u64,
            )?;
            self.file
                .set_len(new_page_count * PAGE_SIZE as u64)
                .map_err(io_err)?;
            self.page_count.store(new_page_count, Ordering::Release);
            first_page_id
        };
        let page_delta = u32::try_from(first_page_id).map_err(|_| {
            error(
                crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                "shadow heap exceeded row-id page range",
            )
        })?;
        for row_id in &mut batch.row_ids {
            row_id.page_id = row_id.page_id.checked_add(page_delta).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                    "shadow heap exceeded row-id page range",
                )
            })?;
        }
        Ok(first_page_id)
    }

    pub(crate) fn write_reserved_shadow_batch_direct(
        &self,
        mut batch: ShadowPreparedHeapBatch,
        first_page_id: u64,
    ) -> Result<Vec<RowId>, QueryError> {
        for (position, bytes) in batch.encoded_pages.chunks_exact_mut(PAGE_SIZE).enumerate() {
            stamp_page_owner(bytes, &self.file_key.owner, first_page_id + position as u64);
            refresh_page_checksum(bytes);
        }
        write_all_at(
            &self.file,
            &batch.encoded_pages,
            first_page_id.saturating_mul(PAGE_SIZE as u64),
        )
        .map_err(io_err)?;
        {
            // Serialize allocation-frontier publication with reservations.
            // The first page's LSN also proves this direct-built generation
            // covers an older FileCreate during crash recovery.
            let _extend_guard = self
                .extend_mutex
                .lock()
                .map_err(|_| error(crate::error::catalog::ER_GET_ERRNO, "extend mutex poisoned"))?;
            let mut meta = encode_heap_meta(&self.path, self.page_count_relaxed());
            set_page_lsn_if_formatted(&mut meta, page_lsn(&batch.encoded_pages[..PAGE_SIZE]));
            write_all_at(&self.file, &meta, 0).map_err(io_err)?;
        }
        if self.durability() != DurabilityMode::Memory {
            // Overlap private-generation writeback with construction; the
            // mandatory sync_shadow_direct barrier still precedes publication.
            crate::util::file::start_file_writeback(
                &self.file,
                first_page_id.saturating_mul(PAGE_SIZE as u64),
                batch.encoded_pages.len(),
            )
            .map_err(io_err)?;
        }
        Ok(batch.row_ids)
    }

    /// Toggle cache bypass for the private shadow-generation file descriptor.
    /// macOS otherwise retains tens of GiB of one-pass bulk output in the
    /// unified cache, competing with the compressed input stream and the
    /// database's governed buffers. The file is unreachable while enabled and
    /// is switched back before publication. Other platforms keep their normal
    /// buffered-I/O behavior.
    pub(crate) fn set_shadow_direct_nocache(&self, _enabled: bool) {
        #[cfg(target_os = "macos")]
        {
            use std::os::fd::AsRawFd;
            unsafe {
                libc::fcntl(self.file.as_raw_fd(), libc::F_NOCACHE, i32::from(_enabled));
            }
        }
    }

    pub(crate) fn append_shadow_payload_batch_direct(
        &self,
        build_lsn: u64,
        payloads: &[&[u8]],
    ) -> Result<Vec<RowId>, QueryError> {
        let prepared = Self::prepare_shadow_payload_batch_direct(build_lsn, payloads)?;
        self.append_prepared_shadow_batch_direct(prepared)
    }

    /// Persist a direct shadow build before its generation is made visible.
    pub(crate) fn sync_shadow_direct(&self) -> Result<(), QueryError> {
        if self.durability() != DurabilityMode::Memory {
            crate::platform::fs::sync_file_at(&self.file, &self.path).map_err(io_err)?;
        }
        Ok(())
    }

    fn read_mvcc(&self, row_id: RowId) -> Result<Option<MvccStoredRow>, QueryError> {
        self.read_mvcc_following(row_id, 0)
    }

    fn read_external_refs(&self, mut row_id: RowId) -> Result<Vec<(usize, LobRef)>, QueryError> {
        if !codec_has_externalizable_columns(&self.codec) {
            return Ok(Vec::new());
        }
        for depth in 0..=MAX_FORWARDING_DEPTH {
            if row_id.page_id as u64 >= self.page_count()? {
                return Ok(Vec::new());
            }
            let (target, refs) = self.with_page(row_id.page_id as u64, |page| {
                let Some(payload) = page.payload(row_id.slot_id as usize)? else {
                    return Ok((None, Vec::new()));
                };
                if let Some(target) = decode_heap_forwarding_stub(payload)? {
                    return Ok((Some(target), Vec::new()));
                }
                let (header, _, _) = read_mvcc_header(payload)?;
                if depth == 0 && header.flags & MVCC_FLAG_ROW_PIECE != 0 {
                    return Ok((None, Vec::new()));
                }
                Ok((None, decode_mvcc_external_refs(payload, &self.codec)?))
            })?;
            match target {
                Some(target) => row_id = target,
                None => return Ok(refs),
            }
        }
        Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "heap row migration chain too deep",
        ))
    }

    fn read_mvcc_following(
        &self,
        row_id: RowId,
        depth: usize,
    ) -> Result<Option<MvccStoredRow>, QueryError> {
        if depth > MAX_FORWARDING_DEPTH {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                &format!(
                    "heap row migration chain too deep at row \
                     table={} page={} slot={} (depth={}, max={})",
                    self.table_id, row_id.page_id, row_id.slot_id, depth, MAX_FORWARDING_DEPTH
                ),
            ));
        }
        if (row_id.page_id as u64) >= self.page_count()? {
            return Ok(None);
        }
        let payload = self.with_page(row_id.page_id as u64, |page| {
            let Some(payload) = page.payload(row_id.slot_id as usize)? else {
                return Ok(None);
            };
            Ok(Some(decode_heap_payload(payload, &self.codec)?))
        })?;
        match payload {
            Some(HeapPayload::Row(header, values)) => Ok(Some(MvccStoredRow {
                row_id,
                header,
                values,
            })),
            Some(HeapPayload::Forward(target)) => {
                self.read_mvcc_following(target, depth + 1).map(|row| {
                    row.map(|mut row| {
                        row.row_id = row_id;
                        row
                    })
                })
            }
            Some(HeapPayload::RowPiece(header, values)) if depth > 0 => Ok(Some(MvccStoredRow {
                row_id,
                header,
                values,
            })),
            Some(
                HeapPayload::RowColumns(header, values, _)
                | HeapPayload::RowPieceColumns(header, values, _),
            ) if depth > 0 => Ok(Some(MvccStoredRow {
                row_id,
                header,
                values,
            })),
            Some(
                HeapPayload::RowPiece(_, _)
                | HeapPayload::RowColumns(..)
                | HeapPayload::RowPieceColumns(..),
            )
            | None => Ok(None),
        }
    }

    /// Header-only counterpart of [`read_mvcc_cached`]. Walks the
    /// cached heap page (no disk I/O) and decodes only the 18-byte
    /// MVCC row header — skipping the per-row value decode that
    /// dominates a covering scan over wide tables. Used by
    /// [`Database::is_row_id_visible_buffered_in_heap`] so an
    /// index-only scan does not pay to materialise values it never
    /// reads from the heap.
    pub(super) fn read_mvcc_header_cached(
        &self,
        row_id: RowId,
    ) -> Result<CachedMvccHeaderRead, QueryError> {
        if (row_id.page_id as u64) >= self.page_count()? {
            return Ok(CachedMvccHeaderRead::NotFound);
        }
        Ok(self
            .buffer_pool
            .with_cached_heap_view(
                &self.page_key(row_id.page_id as u64),
                self.verified_direct_checksums.as_ref(),
                |page| {
                    let Some(page) = page else {
                        return Ok(CachedMvccHeaderRead::NotFound);
                    };
                    let Some(payload) = page.payload(row_id.slot_id as usize)? else {
                        return Ok(CachedMvccHeaderRead::NotFound);
                    };
                    match decode_heap_payload_header(payload)? {
                        HeapPayloadHeader::Row(header) => Ok(CachedMvccHeaderRead::Hit(header)),
                        HeapPayloadHeader::Forward | HeapPayloadHeader::RowPiece => {
                            Ok(CachedMvccHeaderRead::MissingPage)
                        }
                    }
                },
            )?
            .unwrap_or(CachedMvccHeaderRead::MissingPage))
    }

    /// Batch form of [`Self::read_mvcc_header_cached`] for index visibility
    /// probes that touch several slots on the same heap page. It performs one
    /// buffer-pool lookup/pin and then decodes only MVCC headers for every
    /// requested slot, preserving the input order.
    pub(super) fn read_mvcc_headers_cached_same_page_into(
        &self,
        row_ids: &[RowId],
        out: &mut Vec<CachedMvccHeaderRead>,
    ) -> Result<(), QueryError> {
        #[cfg(test)]
        HEAP_BATCH_HEADER_SLOTS_READ.with(|count| count.set(count.get() + row_ids.len()));
        out.clear();
        if row_ids.is_empty() {
            return Ok(());
        }
        let page_id = row_ids[0].page_id;
        debug_assert!(row_ids.iter().all(|row_id| row_id.page_id == page_id));
        if (page_id as u64) >= self.page_count()? {
            out.reserve(row_ids.len());
            for _ in row_ids {
                out.push(CachedMvccHeaderRead::NotFound);
            }
            return Ok(());
        }
        let cached = self.buffer_pool.with_cached_heap_view(
            &self.page_key(page_id as u64),
            self.verified_direct_checksums.as_ref(),
            |page| {
                out.reserve(row_ids.len());
                let Some(page) = page else {
                    for _ in row_ids {
                        out.push(CachedMvccHeaderRead::NotFound);
                    }
                    return Ok(());
                };
                for row_id in row_ids {
                    let Some(payload) = page.payload(row_id.slot_id as usize)? else {
                        out.push(CachedMvccHeaderRead::NotFound);
                        continue;
                    };
                    match decode_heap_payload_header(payload)? {
                        HeapPayloadHeader::Row(header) => {
                            out.push(CachedMvccHeaderRead::Hit(header));
                        }
                        HeapPayloadHeader::Forward | HeapPayloadHeader::RowPiece => {
                            out.push(CachedMvccHeaderRead::MissingPage);
                        }
                    }
                }
                Ok(())
            },
        )?;
        if cached.is_none() {
            out.clear();
            out.reserve(row_ids.len());
            for _ in row_ids {
                out.push(CachedMvccHeaderRead::MissingPage);
            }
        }
        Ok(())
    }

    fn read_mvcc_cached(&self, row_id: RowId) -> Result<CachedMvccRead, QueryError> {
        if (row_id.page_id as u64) >= self.page_count()? {
            return Ok(CachedMvccRead::NotFound);
        }
        Ok(self
            .buffer_pool
            .with_cached_heap_view(
                &self.page_key(row_id.page_id as u64),
                self.verified_direct_checksums.as_ref(),
                |page| {
                    let Some(page) = page else {
                        return Ok(CachedMvccRead::NotFound);
                    };
                    let Some(payload) = page.payload(row_id.slot_id as usize)? else {
                        return Ok(CachedMvccRead::NotFound);
                    };
                    match decode_heap_payload(payload, &self.codec)? {
                        HeapPayload::Row(header, values) => {
                            Ok(CachedMvccRead::Hit(MvccStoredRow {
                                row_id,
                                header,
                                values,
                            }))
                        }
                        HeapPayload::Forward(_) => Ok(CachedMvccRead::MissingPage),
                        HeapPayload::RowPiece(_, _)
                        | HeapPayload::RowPieceColumns(..)
                        | HeapPayload::RowColumns(..) => Ok(CachedMvccRead::NotFound),
                    }
                },
            )?
            .unwrap_or(CachedMvccRead::MissingPage))
    }

    fn read_mvcc_columns(
        &self,
        row_id: RowId,
        columns: &[usize],
    ) -> Result<Option<(RowHeader, Vec<Value>, usize)>, QueryError> {
        self.read_mvcc_columns_following(row_id, columns, 0)
    }

    projected_mvcc_reader!(
        read_mvcc_columns_following,
        columns: &[usize],
        decode_heap_payload_columns
    );
    projected_mvcc_reader!(
        read_mvcc_columns_following_with_plan,
        plan: &RowDecodePlan,
        decode_heap_payload_columns_with_plan
    );

    fn read_mvcc_columns_cached(
        &self,
        row_id: RowId,
        columns: &[usize],
    ) -> Result<CachedProjectedRead, QueryError> {
        let plan = RowDecodePlan::compile_with_codec(columns, &self.codec);
        self.read_mvcc_columns_cached_with_plan(row_id, columns, &plan)
    }

    fn read_mvcc_columns_cached_with_plan(
        &self,
        row_id: RowId,
        _columns: &[usize],
        plan: &RowDecodePlan,
    ) -> Result<CachedProjectedRead, QueryError> {
        if (row_id.page_id as u64) >= self.page_count()? {
            return Ok(CachedProjectedRead::NotFound);
        }
        Ok(self
            .buffer_pool
            .with_cached_heap_view(
                &self.page_key(row_id.page_id as u64),
                self.verified_direct_checksums.as_ref(),
                |page| {
                    let Some(page) = page else {
                        return Ok(CachedProjectedRead::NotFound);
                    };
                    let Some(payload) = page.payload(row_id.slot_id as usize)? else {
                        return Ok(CachedProjectedRead::NotFound);
                    };
                    match decode_heap_payload_columns_with_plan(payload, plan, &self.codec)? {
                        HeapPayload::RowColumns(header, values, stored_count) => {
                            Ok(CachedProjectedRead::Hit(header, values, stored_count))
                        }
                        HeapPayload::Row(header, values) => {
                            let stored_count = values.len();
                            Ok(CachedProjectedRead::Hit(header, values, stored_count))
                        }
                        HeapPayload::Forward(_) => Ok(CachedProjectedRead::MissingPage),
                        HeapPayload::RowPiece(_, _) | HeapPayload::RowPieceColumns(..) => {
                            Ok(CachedProjectedRead::NotFound)
                        }
                    }
                },
            )?
            .unwrap_or(CachedProjectedRead::MissingPage))
    }

    fn read_mvcc_columns_uncached_with_plan(
        &self,
        row_id: RowId,
        plan: &RowDecodePlan,
    ) -> Result<Option<(RowHeader, Vec<Value>, usize)>, QueryError> {
        self.read_mvcc_columns_uncached_with_plan_following(row_id, plan, 0)
    }

    fn read_mvcc_columns_uncached_with_plan_following(
        &self,
        row_id: RowId,
        plan: &RowDecodePlan,
        depth: usize,
    ) -> Result<Option<(RowHeader, Vec<Value>, usize)>, QueryError> {
        if depth > MAX_FORWARDING_DEPTH {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                &format!(
                    "heap row migration chain too deep at row \
                     table={} page={} slot={} (depth={}, max={})",
                    self.table_id, row_id.page_id, row_id.slot_id, depth, MAX_FORWARDING_DEPTH
                ),
            ));
        }
        if (row_id.page_id as u64) >= self.page_count()? {
            return Ok(None);
        }
        let payload =
            self.buffer_pool
                .with_raw_page(self.page_key(row_id.page_id as u64), |page| {
                    let Some(payload) = read_page_payload(page, row_id)? else {
                        return Ok(None);
                    };
                    decode_heap_payload_columns_with_plan(payload, plan, &self.codec).map(Some)
                })?;
        let Some(payload) = payload else {
            return Ok(None);
        };
        match payload {
            HeapPayload::RowColumns(header, values, stored_count) => {
                Ok(Some((header, values, stored_count)))
            }
            HeapPayload::Row(header, values) => {
                let stored_count = values.len();
                Ok(Some((header, values, stored_count)))
            }
            HeapPayload::Forward(target) => {
                self.read_mvcc_columns_uncached_with_plan_following(target, plan, depth + 1)
            }
            HeapPayload::RowPieceColumns(header, values, stored_count) if depth > 0 => {
                Ok(Some((header, values, stored_count)))
            }
            HeapPayload::RowPiece(header, values) if depth > 0 => {
                let stored_count = values.len();
                Ok(Some((header, values, stored_count)))
            }
            HeapPayload::RowPiece(_, _) | HeapPayload::RowPieceColumns(..) => Ok(None),
        }
    }

    pub(crate) fn read_mvcc_columns_probe_page(
        &self,
        row_ids: &[RowId],
        columns: &[usize],
        plan: Option<&RowDecodePlan>,
        out: &mut Vec<(RowId, RowHeader, Vec<Value>, usize)>,
    ) -> Result<(), QueryError> {
        let Some(first) = row_ids.first().copied() else {
            return Ok(());
        };
        debug_assert!(row_ids.iter().all(|row_id| row_id.page_id == first.page_id));
        if first.page_id as u64 >= self.page_count()? {
            return Ok(());
        }
        let owned_plan;
        let plan = match plan {
            Some(plan) => plan,
            None => {
                owned_plan = RowDecodePlan::compile_with_codec(columns, &self.codec);
                &owned_plan
            }
        };
        let mut forwarding = Vec::new();
        let pinned = self
            .buffer_pool
            .pin_page_with_intent(self.page_key(first.page_id as u64), AccessIntent::Probe)?;
        pinned.with_heap_raw_bytes_cached(self.verified_direct_checksums.as_ref(), |page| {
            let Some(page) = page else {
                return Ok(());
            };
            for &row_id in row_ids {
                let Some(payload) = page.payload(row_id.slot_id as usize)? else {
                    continue;
                };
                match decode_heap_payload_columns_with_plan(payload, plan, &self.codec)? {
                    HeapPayload::RowColumns(header, values, stored_count) => {
                        out.push((row_id, header, values, stored_count));
                    }
                    HeapPayload::Row(header, values) => {
                        let stored_count = values.len();
                        out.push((row_id, header, values, stored_count));
                    }
                    HeapPayload::Forward(_) => forwarding.push(row_id),
                    HeapPayload::RowPiece(_, _) | HeapPayload::RowPieceColumns(..) => {}
                }
            }
            Ok(())
        })?;
        // Forwarding and row pieces deliberately stay on the established
        // recursive path, including its depth and LOB safeguards.
        for row_id in forwarding {
            let read = self.read_mvcc_columns_following_with_plan(row_id, plan, 0)?;
            if let Some((header, values, stored_count)) = read {
                out.push((row_id, header, values, stored_count));
            }
        }
        Ok(())
    }

    /// Dense, ordered bulk probes have scan-like reuse: admitting every cold
    /// page evicts useful residents and splits I/O into tiny demand reads.
    /// Reuse the scan reader's dirty-page overlay and bounded sequential I/O,
    /// but decode only the requested slots. Sparse/OLTP batches keep MRR.
    pub(crate) fn try_read_mvcc_columns_dense_probe(
        &self,
        row_ids: &[RowId],
        columns: &[usize],
        plan: Option<&RowDecodePlan>,
        out: &mut Vec<(RowId, RowHeader, Vec<Value>, usize)>,
    ) -> Result<bool, QueryError> {
        if row_ids.len() < 1024 || !dense_probe_streaming_enabled() {
            return Ok(false);
        }
        if !dense_probe_worth_streaming(
            row_ids,
            self.page_count()?,
            self.buffer_pool.capacity_pages(),
        ) {
            return Ok(false);
        }
        const BATCH_PAGES: usize = 64;
        let Some(_charge) =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                BATCH_PAGES * PAGE_SIZE,
            )
        else {
            return Ok(false);
        };
        let mut bytes = vec![0u8; BATCH_PAGES * PAGE_SIZE];
        let owned_plan;
        let plan = match plan {
            Some(plan) => plan,
            None => {
                owned_plan = RowDecodePlan::compile_with_codec(columns, &self.codec);
                &owned_plan
            }
        };
        let mut start = 0;
        while start < row_ids.len() {
            crate::engine::cancel::check_cancelled()?;
            let first = u64::from(row_ids[start].page_id);
            let end = dense_probe_batch_end(row_ids, start);
            let count = (u64::from(row_ids[end - 1].page_id) - first + 1) as usize;
            self.buffer_pool.read_raw_scan_batch(
                &self.file_key,
                first,
                &mut bytes[..count * PAGE_SIZE],
            )?;
            let mut position = start;
            while position < end {
                let page_id = row_ids[position].page_id;
                let offset = (u64::from(page_id) - first) as usize * PAGE_SIZE;
                let page_bytes = &bytes[offset..offset + PAGE_SIZE];
                let valid =
                    self.validate_heap_page_header_for_direct_scan(u64::from(page_id), page_bytes)?;
                let page = HeapPageView::from_validated(page_bytes);
                while position < end && row_ids[position].page_id == page_id {
                    let row_id = row_ids[position];
                    position += 1;
                    if !valid {
                        continue;
                    }
                    let Some(payload) = page.payload(row_id.slot_id as usize)? else {
                        continue;
                    };
                    match decode_heap_payload_columns_with_plan(payload, plan, &self.codec)? {
                        HeapPayload::RowColumns(header, values, stored_count) => {
                            out.push((row_id, header, values, stored_count));
                        }
                        HeapPayload::Row(header, values) => {
                            let stored_count = values.len();
                            out.push((row_id, header, values, stored_count));
                        }
                        HeapPayload::Forward(_) => {
                            if let Some((header, values, stored_count)) =
                                self.read_mvcc_columns_following_with_plan(row_id, plan, 0)?
                            {
                                out.push((row_id, header, values, stored_count));
                            }
                        }
                        HeapPayload::RowPiece(_, _) | HeapPayload::RowPieceColumns(..) => {}
                    }
                }
            }
            start = end;
        }
        Ok(true)
    }

    /// Large-heap sparse probes have little cache reuse. Read bounded windows
    /// through the coherent scan reader instead of cycling raw CLOCK frames.
    pub(crate) fn try_read_mvcc_columns_sparse_probe(
        &self,
        row_ids: &[RowId],
        columns: &[usize],
        plan: Option<&RowDecodePlan>,
        out: &mut Vec<(RowId, RowHeader, Vec<Value>, usize)>,
    ) -> Result<bool, QueryError> {
        if row_ids.len() < 8
            || self.page_count()? <= self.buffer_pool.capacity_pages().saturating_mul(4)
        {
            return Ok(false);
        }
        let base = out.len();
        let owned_plan;
        let plan = match plan {
            Some(plan) => plan,
            None => {
                owned_plan = RowDecodePlan::compile_with_codec(columns, &self.codec);
                &owned_plan
            }
        };
        for rows in row_ids.chunks(64) {
            crate::engine::cancel::check_cancelled()?;
            let mut pages: Vec<_> = rows.iter().map(|row| row.page_id as u64).collect();
            pages.dedup();
            let mut position = 0;
            let complete =
                self.buffer_pool
                    .with_raw_probe_pages(&self.file_key, &pages, |id, bytes| {
                        let valid = self.validate_heap_page_header_for_direct_scan(id, bytes)?;
                        let page = HeapPageView::from_validated(bytes);
                        while position < rows.len() && rows[position].page_id as u64 == id {
                            let row = rows[position];
                            position += 1;
                            if !valid {
                                continue;
                            }
                            let Some(payload) = page.payload(row.slot_id as usize)? else {
                                continue;
                            };
                            match decode_heap_payload_columns_with_plan(payload, plan, &self.codec)?
                            {
                                HeapPayload::RowColumns(header, values, count) => {
                                    out.push((row, header, values, count))
                                }
                                HeapPayload::Row(header, values) => {
                                    let count = values.len();
                                    out.push((row, header, values, count));
                                }
                                HeapPayload::Forward(_) => {
                                    if let Some((header, values, count)) =
                                        self.read_mvcc_columns_following_with_plan(row, plan, 0)?
                                    {
                                        out.push((row, header, values, count));
                                    }
                                }
                                HeapPayload::RowPiece(_, _) | HeapPayload::RowPieceColumns(..) => {}
                            }
                        }
                        Ok(())
                    })?;
            if !complete {
                out.truncate(base);
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Page-batched random read that evaluates a narrow predicate projection
    /// before decoding the output projection.  Accepted rows are decoded from
    /// the same pinned payload, so selective INLJ residuals avoid both a
    /// second heap-page pin and decoding output columns for rejected rows.
    pub(crate) fn read_mvcc_columns_many_predicate_first<P>(
        &self,
        row_ids: &[RowId],
        predicate_columns: &[usize],
        output_columns: &[usize],
        missing_columns: &[usize],
        output_sources: &[OutputSource],
        mut predicate: P,
        out: &mut Vec<(RowId, RowHeader, Vec<Value>, usize)>,
        deferred: &mut Vec<RowId>,
    ) -> Result<(), QueryError>
    where
        P: FnMut(RowId, RowHeader, &[Value], usize) -> Result<PredicateFirstDecision, QueryError>,
    {
        debug_assert_eq!(output_columns.len(), output_sources.len());
        if row_ids.is_empty() {
            return Ok(());
        }
        let page_count = self.page_count()?;
        let predicate_plan = RowDecodePlan::compile_with_codec(predicate_columns, &self.codec);
        let missing_plan = RowDecodePlan::compile_with_codec(missing_columns, &self.codec);
        let mut predicate_values = Vec::with_capacity(predicate_columns.len());
        let mut missing_values = Vec::with_capacity(missing_columns.len());
        // Predicate-first MRR consumes the same sorted RowId stream as the
        // ordinary projected reader.  Keep its cold-page path on the same
        // bounded ordered batch loader: without this window every distinct
        // page falls through to a foreground `pread`, serialising an
        // otherwise parallel non-covering INLJ on storage latency.  Loading
        // only the concrete page ids in the current window preserves the
        // probe's sparse geometry and the buffer-pool governor; demotion
        // keeps the one-pass pages from displacing reusable index pages.
        // This scratch lives for one bounded predicate-first batch and is
        // released after the last page is decoded. Charge by Vec capacity,
        // not logical length, before allocating so concurrent probe workers
        // cannot step outside the process memory target.
        let unique_page_capacity = row_ids.len();
        let _unique_pages_charge =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                unique_page_capacity.saturating_mul(std::mem::size_of::<u64>()),
            )?;
        let mut unique_pages: Vec<u64> = Vec::with_capacity(unique_page_capacity);
        unique_pages.extend(row_ids.iter().map(|row_id| row_id.page_id as u64));
        unique_pages.dedup();
        let ordered_mrr = !crate::util::env::var("MYDB_MRR_SORT")
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "0" | "off" | "false" | "no" | "disable" | "disabled"
                )
            })
            .unwrap_or(false);
        let load_window = self.ordered_probe_window_pages();
        let mut page_group = 0usize;
        let mut loaded_from = 0usize;
        let mut loaded_through = 0usize;
        let mut start = 0usize;
        while start < row_ids.len() {
            let first = row_ids[start];
            let mut end = start + 1;
            while end < row_ids.len() && row_ids[end].page_id == first.page_id {
                end += 1;
            }
            if first.page_id as u64 >= page_count {
                start = end;
                continue;
            }
            let mut consume_page = |page: Option<HeapPageView<'_>>| {
                let Some(page) = page else {
                    return Ok(());
                };
                for &row_id in &row_ids[start..end] {
                    let Some(payload) = page.payload(row_id.slot_id as usize)? else {
                        continue;
                    };
                    let (header, stored_count) = match decode_heap_payload_columns_into_with_plan(
                        payload,
                        &predicate_plan,
                        &self.codec,
                        &mut predicate_values,
                    )? {
                        HeapPayloadOutcome::Row(header, stored_count) => (header, stored_count),
                        HeapPayloadOutcome::Forward(_) => {
                            deferred.push(row_id);
                            continue;
                        }
                        HeapPayloadOutcome::Skip => continue,
                    };
                    match predicate(row_id, header, &predicate_values, stored_count)? {
                        PredicateFirstDecision::Reject => continue,
                        PredicateFirstDecision::Defer => {
                            deferred.push(row_id);
                            continue;
                        }
                        PredicateFirstDecision::Accept => {}
                    }
                    let output_stored_count = if missing_columns.is_empty() {
                        stored_count
                    } else {
                        match decode_heap_payload_columns_into_with_plan(
                            payload,
                            &missing_plan,
                            &self.codec,
                            &mut missing_values,
                        )? {
                            HeapPayloadOutcome::Row(_, stored_count) => stored_count,
                            HeapPayloadOutcome::Forward(_) => {
                                deferred.push(row_id);
                                continue;
                            }
                            HeapPayloadOutcome::Skip => continue,
                        }
                    };
                    let mut values = Vec::with_capacity(output_columns.len());
                    for source in output_sources {
                        values.push(match source {
                            OutputSource::Predicate(index) => predicate_values[*index].clone(),
                            OutputSource::Missing(index) => missing_values[*index].clone(),
                        });
                    }
                    out.push((row_id, header, values, output_stored_count));
                }
                Ok(())
            };
            // Consume resident pages from their authoritative image once.
            // Ordered loading is only useful after a cache miss; applying it
            // to every hot probe repeats page lookup/pin/recency work and
            // demotes pages that concurrent OLTP queries keep reusing.
            let cached = self.buffer_pool.with_cached_heap_view(
                &self.page_key(first.page_id as u64),
                self.verified_direct_checksums.as_ref(),
                &mut consume_page,
            )?;
            if cached.is_none() {
                if page_group >= loaded_through {
                    let target = (page_group + load_window).min(unique_pages.len());
                    if ordered_mrr {
                        loaded_from = page_group;
                        self.load_ordered_probe_pages(&unique_pages[page_group..target])?;
                    } else {
                        self.prefetch_probe_pages(&unique_pages[page_group..target]);
                    }
                    loaded_through = target;
                }
                let pinned = self.buffer_pool.pin_page_with_intent(
                    self.page_key(first.page_id as u64),
                    AccessIntent::Probe,
                )?;
                pinned.with_heap_raw_bytes_cached(
                    self.verified_direct_checksums.as_ref(),
                    consume_page,
                )?;
            }
            start = end;
            page_group += 1;
            if ordered_mrr && page_group == loaded_through {
                self.demote_ordered_probe_pages(&unique_pages[loaded_from..loaded_through])?;
            }
        }
        Ok(())
    }

    fn read_mvcc_columns_many_cached(
        &self,
        row_ids: &[RowId],
        columns: &[usize],
        hits: &mut Vec<(RowId, RowHeader, Vec<Value>, usize)>,
        missing: &mut Vec<RowId>,
    ) -> Result<(), QueryError> {
        // Pre-compile the projection plan once per batch instead of
        // rebuilding it inside `decode_row_schema_columns_into` per
        // row. Saves a `SmallVec<[i16; 32]>` allocation + setup loop
        // per heap row read.
        let plan = RowDecodePlan::compile_with_codec(columns, &self.codec);
        self.read_mvcc_columns_many_cached_with_plan(row_ids, &plan, hits, missing)
    }

    pub(crate) fn read_mvcc_columns_many_cached_with_optional_plan(
        &self,
        row_ids: &[RowId],
        columns: &[usize],
        plan: Option<&RowDecodePlan>,
        hits: &mut Vec<(RowId, RowHeader, Vec<Value>, usize)>,
        missing: &mut Vec<RowId>,
    ) -> Result<(), QueryError> {
        match plan {
            Some(plan) => {
                self.read_mvcc_columns_many_cached_with_plan(row_ids, plan, hits, missing)
            }
            None => self.read_mvcc_columns_many_cached(row_ids, columns, hits, missing),
        }
    }

    fn read_mvcc_columns_many_cached_with_plan(
        &self,
        row_ids: &[RowId],
        plan: &RowDecodePlan,
        hits: &mut Vec<(RowId, RowHeader, Vec<Value>, usize)>,
        missing: &mut Vec<RowId>,
    ) -> Result<(), QueryError> {
        let page_count = self.page_count()?;
        let mut start = 0usize;
        while start < row_ids.len() {
            let first = row_ids[start];
            let mut end = start + 1;
            while end < row_ids.len() && row_ids[end].page_id == first.page_id {
                end += 1;
            }
            let page_rows = &row_ids[start..end];
            if (first.page_id as u64) >= page_count {
                start = end;
                continue;
            }
            let cached = self.buffer_pool.with_cached_heap_view(
                &self.page_key(first.page_id as u64),
                self.verified_direct_checksums.as_ref(),
                |page| {
                    let Some(page) = page else {
                        return Ok(());
                    };
                    for &row_id in page_rows {
                        let Some(payload) = page.payload(row_id.slot_id as usize)? else {
                            continue;
                        };
                        match decode_heap_payload_columns_with_plan(payload, &plan, &self.codec)? {
                            HeapPayload::RowColumns(header, values, stored_count) => {
                                hits.push((row_id, header, values, stored_count));
                            }
                            HeapPayload::Row(header, values) => {
                                let stored_count = values.len();
                                hits.push((row_id, header, values, stored_count));
                            }
                            HeapPayload::Forward(_) => missing.push(row_id),
                            HeapPayload::RowPiece(_, _) | HeapPayload::RowPieceColumns(..) => {}
                        }
                    }
                    Ok(())
                },
            )?;
            if cached.is_none() {
                missing.extend_from_slice(page_rows);
            }
            start = end;
        }
        Ok(())
    }

    /// Visit projected rows straight from resident heap payloads, preserving
    /// the caller's RowId order and avoiding a `Vec<Value>` per row.  The
    /// visitor returns `false` when its snapshot/layout oracle cannot consume
    /// a payload directly.  In that case this method stops and returns
    /// `Ok(false)`; callers must discard any partially-filled sink and retry
    /// the whole window through the ordinary undo-aware row path.
    ///
    /// This deliberately does not admit cold pages or follow forwarding
    /// stubs.  Those uncommon cases use the established general reader, which
    /// keeps this hot path both allocation-free and semantically subordinate
    /// to the row oracle.
    pub(crate) fn visit_mvcc_payloads_many_cached_with_plan<F>(
        &self,
        row_ids: &[RowId],
        columns: &[usize],
        mut visit: F,
    ) -> Result<bool, QueryError>
    where
        F: FnMut(PayloadMvccStoredRow<'_>) -> Result<bool, QueryError>,
    {
        if row_ids.is_empty() {
            return Ok(true);
        }
        let page_count = self.page_count()?;
        let plan = RowDecodePlan::compile_with_codec(columns, &self.codec);
        let mut start = 0usize;
        while start < row_ids.len() {
            let first = row_ids[start];
            let mut end = start + 1;
            while end < row_ids.len() && row_ids[end].page_id == first.page_id {
                end += 1;
            }
            if first.page_id as u64 >= page_count {
                start = end;
                continue;
            }
            let consumed = self.buffer_pool.with_cached_heap_view(
                &self.page_key(first.page_id as u64),
                self.verified_direct_checksums.as_ref(),
                |page| {
                    let Some(page) = page else {
                        return Ok(false);
                    };
                    for &row_id in &row_ids[start..end] {
                        let Some(payload) = page.payload(row_id.slot_id as usize)? else {
                            continue;
                        };
                        if crate::storage::core::decode_heap_forwarding_stub_pub(payload)?.is_some()
                        {
                            return Ok(false);
                        }
                        let Some((header, body_offset, has_external)) =
                            crate::storage::core::peek_mvcc_header(payload)
                        else {
                            continue;
                        };
                        if header.flags & crate::storage::core::MVCC_FLAG_ROW_PIECE_PUB != 0 {
                            return Ok(false);
                        }
                        let mut layout_pos = body_offset;
                        let layout_id =
                            crate::storage::core::read_varint_at(payload, &mut layout_pos)? as u32;
                        if !visit(PayloadMvccStoredRow {
                            row_id,
                            header,
                            payload,
                            body_offset: layout_pos,
                            layout_id,
                            has_external,
                            plan: &plan,
                            codec: &self.codec,
                        })? {
                            return Ok(false);
                        }
                    }
                    Ok(true)
                },
            )?;
            if consumed != Some(true) {
                return Ok(false);
            }
            start = end;
        }
        Ok(true)
    }

    /// Preserve an inline row's body when installing its MVCC tombstone.
    /// External chains retain the full rewrite path: undo takes ownership
    /// of those chains, so a tombstone must not keep materializable refs.
    fn try_write_mvcc_tombstone(
        &self,
        txn_id: u64,
        row_id: RowId,
        header: RowHeader,
    ) -> Result<bool, QueryError> {
        if !header.is_deleted() || header.undo_ptr.is_none() || txn_id == 0 {
            return Ok(false);
        }
        let _page_guard = self
            .page_latch(row_id.page_id as u64)
            .lock()
            .map_err(|_| error(crate::error::catalog::ER_GET_ERRNO, "page latch poisoned"))?;
        let before = self.with_page(row_id.page_id as u64, |page| {
            let Some(payload) = page.payload(row_id.slot_id as usize)? else {
                return Ok(None);
            };
            if payload.first().copied() != Some(MVCC_ROW_VERSION) {
                return Ok(None);
            }
            let (old, width, external) = read_mvcc_header(payload)?;
            if width != 18 || external || old.flags & MVCC_FLAG_ROW_PIECE != 0 {
                return Ok(None);
            }
            Ok(Some(<[u8; 17]>::try_from(&payload[1..18]).unwrap()))
        })?;
        let Some(before) = before else {
            return Ok(false);
        };
        let mut after = before;
        after[0] |= MVCC_FLAG_DELETED;
        after[1..9].copy_from_slice(&header.last_txn_id.to_le_bytes());
        after[9..17].copy_from_slice(&header.undo_ptr.unwrap().to_le_bytes());
        self.maybe_emit_heap_fpi(row_id.page_id)?;
        let key = self.page_key(row_id.page_id as u64);
        let write = |lsn| {
            self.buffer_pool
                .with_raw_page_mut(key.clone(), lsn, |bytes| {
                    patch_heap_page_raw(bytes, row_id.slot_id, &[(1, after.as_slice())])
                })
        };
        if self.durability() == DurabilityMode::Memory {
            write(0)?;
            return Ok(true);
        }
        let payload = WalPayload::HeapUpdateLogical {
            path: self.relative_path(),
            page_id: row_id.page_id,
            slot_id: row_id.slot_id,
            patches: vec![WalColumnPatch {
                offset: 1,
                before: before.to_vec(),
                after: after.to_vec(),
            }],
            undo_lsn: header.undo_ptr,
        };
        match try_buffer_txn_page_write(
            &self.log,
            &self.buffer_pool,
            txn_id,
            payload,
            vec![key.clone()],
            write,
        ) {
            Ok(result) => result?,
            Err((payload, write)) => {
                let pre = crate::wal::LogManager::pre_encode_record(txn_id, payload)?;
                let lsn = crate::wal::LogManager::append_pre_encoded_to(&self.log, pre)?;
                write(lsn)?;
            }
        }
        Ok(true)
    }

    fn write_mvcc(
        &self,
        txn_id: u64,
        row_id: RowId,
        header: RowHeader,
        values: &[Value],
    ) -> Result<(), QueryError> {
        self.write_mvcc_ext(txn_id, row_id, header, values, &[])
    }

    fn write_mvcc_ext(
        &self,
        txn_id: u64,
        row_id: RowId,
        header: RowHeader,
        values: &[Value],
        external: &[Option<LobRef>],
    ) -> Result<(), QueryError> {
        let payload = encode_mvcc_row(header, values, &self.codec, external)?;
        if payload.len() + PAGE_HEADER_SIZE + SLOT_SIZE > PAGE_SIZE {
            return Err(error(
                crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                "row size is larger than one heap page",
            ));
        }
        // Latch the target page for the read-modify-write cycle so two
        // concurrent writers on the same page can't race after the
        // file-wide RwLock was dropped from `with_heap_mut`.
        let _page_guard = self
            .page_latch(row_id.page_id as u64)
            .lock()
            .map_err(|_| error(crate::error::catalog::ER_GET_ERRNO, "page latch poisoned"))?;
        let (before, update_plan) = self.with_page(row_id.page_id as u64, |page| {
            let before = page
                .payload(row_id.slot_id as usize)?
                .ok_or_else(|| error(crate::error::catalog::ER_KEY_NOT_FOUND, "row not found"))?
                .to_vec();
            let plan = HeapPageMut::plan_update(page.bytes, row_id.slot_id, payload.len())?;
            Ok((before, plan))
        })?;
        if let Some(target) = decode_heap_forwarding_stub(&before)? {
            if txn_id == 0 {
                // A redo-only write (rollback restore) reaches the piece through
                // a stub that may belong to the transaction being rolled back.
                // Commit-filtered REDO skips that transaction's records, so the
                // home slot would keep the stale pre-migration row next to the
                // restored piece: later committed writes go to the piece and
                // are lost on recovery. Re-log the stub as part of this write.
                let stub_plan = self.with_page(row_id.page_id as u64, |page| {
                    HeapPageMut::plan_update(page.bytes, row_id.slot_id, before.len())
                })?;
                self.write_heap_update_raw_logged(
                    0,
                    row_id,
                    before.clone(),
                    before,
                    stub_plan,
                    None,
                )?;
            }
            drop(_page_guard);
            return self.write_migrated_row_piece(txn_id, row_id, target, payload, header.undo_ptr);
        }
        if update_plan != HeapUpdatePlan::NoSpace {
            UPDATE_IN_PLACE_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return self.write_heap_update_raw_logged(
                txn_id,
                row_id,
                before,
                payload,
                update_plan,
                header.undo_ptr,
            );
        }
        // Row outgrew its page: drop the page latch before the
        // migration sub-call so the alloc + stub-rewrite path can
        // take its own latches without holding ours (it touches the
        // target page and a freshly-allocated piece page).
        drop(_page_guard);
        let migrated_header = RowHeader {
            flags: header.flags | MVCC_FLAG_ROW_PIECE,
            ..header
        };
        let migrated_payload = encode_mvcc_row(migrated_header, values, &self.codec, external)?;
        let target = self.insert_migrated_row_piece(txn_id, migrated_payload.clone())?;
        let forwarding_stub = encode_heap_forwarding_stub(target);
        let _stub_guard = self
            .page_latch(row_id.page_id as u64)
            .lock()
            .map_err(|_| error(crate::error::catalog::ER_GET_ERRNO, "page latch poisoned"))?;
        let stub_plan = self.with_page(row_id.page_id as u64, |page| {
            HeapPageMut::plan_update(page.bytes, row_id.slot_id, forwarding_stub.len())
        })?;
        if stub_plan == HeapUpdatePlan::NoSpace {
            return Err(error(
                crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                "forwarding stub does not fit in heap page",
            ));
        }
        self.bump_row_migrations();
        ROW_MIGRATIONS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        UPDATE_MIGRATE_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.write_heap_update_raw_logged(
            txn_id,
            row_id,
            before,
            forwarding_stub,
            stub_plan,
            header.undo_ptr,
        )
    }

    fn has_reserved_undo_slot(&self, row_id: RowId) -> Result<bool, QueryError> {
        self.with_page(row_id.page_id as u64, |page| {
            let Some(payload) = page.payload(row_id.slot_id as usize)? else {
                return Ok(false);
            };
            if payload.first().copied() != Some(MVCC_ROW_VERSION) {
                return Ok(false);
            }
            let (header, header_len, external) = read_mvcc_header(payload)?;
            Ok(header_len == 18 && !external && header.flags & MVCC_FLAG_ROW_PIECE == 0)
        })
    }

    /// Prepare an operation-local patch without creating undo or modifying
    /// the heap. Unsupported layouts return None before any write-side work.
    /// The caller keeps its transaction row lock until application, but no
    /// heap handle or page guard survives this method's return.
    fn prepare_mvcc_patch(
        &self,
        row_id: RowId,
        new_values: &[Value],
        changed_cols: &[usize],
    ) -> Result<Option<PreparedHeapPatch>, QueryError> {
        if changed_cols.is_empty() {
            UPDATE_DELTA_MISSES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(None);
        }
        // Own only header and changed-column bytes. Reserve any SmallVec
        // spill before allocation and reconcile to retained capacity below.
        use crate::engine::executor::mem_budget::{
            ControlledMemoryCategory, ControlledMemoryCharge,
        };
        let spill_bytes = if changed_cols.len() > 4 {
            changed_cols
                .len()
                .next_power_of_two()
                .saturating_mul(std::mem::size_of::<HeapColumnPatch>())
        } else {
            0
        };
        let mut charge =
            ControlledMemoryCharge::try_new(ControlledMemoryCategory::StorageScratch, spill_bytes)?;
        let Some((old_last_txn_id_bytes, old_undo_ptr_bytes, mut col_patches)) =
            self.with_page(row_id.page_id as u64, |page| {
                let slot_bytes = page.payload(row_id.slot_id as usize)?.ok_or_else(|| {
                    error(crate::error::catalog::ER_KEY_NOT_FOUND, "row not found")
                })?;
                if decode_heap_forwarding_stub(slot_bytes)?.is_some()
                    || slot_bytes.first().copied() != Some(MVCC_ROW_VERSION)
                {
                    UPDATE_DELTA_MISSES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return Ok(None);
                }

                let (header, header_len, _) = read_mvcc_header(slot_bytes)?;
                if header_len != 18 || header.flags & MVCC_FLAG_ROW_PIECE != 0 {
                    UPDATE_DELTA_MISSES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return Ok(None);
                }

                let old_last_txn_id_bytes: [u8; 8] = slot_bytes
                    .get(2..10)
                    .ok_or_else(|| {
                        error(crate::error::catalog::ER_GET_ERRNO, "corrupt row header")
                    })?
                    .try_into()
                    .expect("eight-byte slice");
                let old_undo_ptr_bytes: [u8; 8] = slot_bytes
                    .get(10..18)
                    .ok_or_else(|| {
                        error(crate::error::catalog::ER_GET_ERRNO, "corrupt row header")
                    })?
                    .try_into()
                    .expect("eight-byte slice");

                let mut col_patches = HeapColumnPatches::with_capacity(changed_cols.len());
                for &col_idx in changed_cols {
                    let new_is_null = matches!(new_values.get(col_idx), Some(Value::Null));
                    let Some((range, null_offset, null_mask)) =
                        column_patch_range(slot_bytes, &self.codec, col_idx)?
                    else {
                        return Ok(None);
                    };
                    // A variable NULL has zero encoded width. Preserve the
                    // canonical shrinking rewrite instead of retaining its
                    // former payload as unused bytes in a sparse patch.
                    if new_is_null && range.width != 0 && fixed_width_for_kind(range.kind).is_none()
                    {
                        return Ok(None);
                    }
                    let old_null_byte = slot_bytes[null_offset];
                    if (old_null_byte & null_mask != 0) != new_is_null {
                        if let Some((_, _, after)) = col_patches
                            .iter_mut()
                            .find(|(offset, _, _)| *offset == null_offset)
                        {
                            if new_is_null {
                                after[0] |= null_mask;
                            } else {
                                after[0] &= !null_mask;
                            }
                        } else {
                            let new_null_byte = if new_is_null {
                                old_null_byte | null_mask
                            } else {
                                old_null_byte & !null_mask
                            };
                            if !push_heap_column_patch(
                                &mut col_patches,
                                &mut charge,
                                (
                                    null_offset,
                                    HeapPatchBytes::from_slice(&[old_null_byte]),
                                    HeapPatchBytes::from_slice(&[new_null_byte]),
                                ),
                            ) {
                                return Err(crate::error::catalog::ER_OUTOFMEMORY
                                    .error("heap patch scratch budget exhausted"));
                            }
                        }
                    }
                    if range.width > 16 {
                        charge.try_grow(range.width.next_power_of_two().saturating_mul(2))?;
                    }
                    let mut after_bytes = HeapPatchBytes::from_elem(0u8, range.width);
                    if !new_is_null
                        && !encode_column_patch_into(
                            range.kind,
                            &new_values[col_idx],
                            &mut after_bytes,
                        )?
                    {
                        return Ok(None);
                    }
                    let end = range
                        .offset_in_payload
                        .checked_add(range.width)
                        .ok_or_else(|| {
                            error(
                                crate::error::catalog::ER_GET_ERRNO,
                                "corrupt heap patch range",
                            )
                        })?;
                    let before_bytes = HeapPatchBytes::from_slice(
                        slot_bytes
                            .get(range.offset_in_payload..end)
                            .ok_or_else(|| {
                                error(
                                    crate::error::catalog::ER_GET_ERRNO,
                                    "corrupt heap patch range",
                                )
                            })?,
                    );
                    if !push_heap_column_patch(
                        &mut col_patches,
                        &mut charge,
                        (range.offset_in_payload, before_bytes, after_bytes),
                    ) {
                        return Err(crate::error::catalog::ER_OUTOFMEMORY
                            .error("heap patch scratch budget exhausted"));
                    }
                }
                Ok(Some((
                    old_last_txn_id_bytes,
                    old_undo_ptr_bytes,
                    col_patches,
                )))
            })?
        else {
            return Ok(None);
        };

        // Adjacent fixed fields form one redo range. Keep their bytes inline
        // when possible rather than allocating before/after Vecs per field.
        col_patches.sort_unstable_by_key(|patch| patch.0);
        let mut i = 0;
        while i + 1 < col_patches.len() {
            let width = col_patches[i].1.len() + col_patches[i + 1].1.len();
            if col_patches[i].0 + col_patches[i].1.len() != col_patches[i + 1].0 {
                i += 1;
                continue;
            }
            let extra = [&col_patches[i].1, &col_patches[i].2]
                .iter()
                .map(|bytes| {
                    if width > bytes.capacity() {
                        width
                            .next_power_of_two()
                            .saturating_sub(if bytes.spilled() { bytes.capacity() } else { 0 })
                    } else {
                        0
                    }
                })
                .sum();
            if !charge.try_grow_optional(extra) {
                i += 1;
                continue;
            }
            let (_, before, after) = col_patches.remove(i + 1);
            col_patches[i].1.reserve_exact(before.len());
            col_patches[i].2.reserve_exact(after.len());
            col_patches[i].1.extend_from_slice(&before);
            col_patches[i].2.extend_from_slice(&after);
        }
        let bytes = if col_patches.spilled() {
            col_patches.capacity() * std::mem::size_of::<HeapColumnPatch>()
        } else {
            0
        } + col_patches
            .iter()
            .map(|(_, before, after)| {
                usize::from(before.spilled()) * before.capacity()
                    + usize::from(after.spilled()) * after.capacity()
            })
            .sum::<usize>();
        debug_assert!(bytes <= charge.bytes());
        charge.shrink(charge.bytes().saturating_sub(bytes));
        Ok(Some(PreparedHeapPatch {
            row_id,
            old_last_txn_id_bytes,
            old_undo_ptr_bytes,
            col_patches,
            _charge: charge,
        }))
    }

    fn apply_prepared_mvcc_patch(
        &self,
        txn_id: u64,
        new_undo_ptr: u64,
        prepared: PreparedHeapPatch,
    ) -> Result<bool, QueryError> {
        let PreparedHeapPatch {
            row_id,
            old_last_txn_id_bytes,
            old_undo_ptr_bytes,
            col_patches,
            _charge,
        } = prepared;
        const MAX_INLINE_COLS: usize = 4;
        // Header overwrites: keep version + flags bytes, replace the
        // last_txn_id and undo_ptr u64 LE fields with new values.
        let mut old_header_bytes = [0u8; 16];
        old_header_bytes[..8].copy_from_slice(&old_last_txn_id_bytes);
        old_header_bytes[8..].copy_from_slice(&old_undo_ptr_bytes);
        let mut new_header_bytes = [0u8; 16];
        new_header_bytes[..8].copy_from_slice(&txn_id.to_le_bytes());
        new_header_bytes[8..].copy_from_slice(&new_undo_ptr.to_le_bytes());

        // Apply patches atomically on the page. The no-capture helper
        // pre-validates every range and does not allocate any
        // before-image `Vec`s — we already have the bytes (in the
        // stack arrays above and in `slot_bytes` for the column
        // portions), and hand them to the WAL builder below directly.
        // Page latch covers the read-modify-store window so two
        // concurrent in-place patchers on the same page can't lose
        // each other's writes.
        let _page_guard = self
            .page_latch(row_id.page_id as u64)
            .lock()
            .map_err(|_| error(crate::error::catalog::ER_GET_ERRNO, "page latch poisoned"))?;
        // Stack-allocated slice descriptor: header (1 entry) + column
        // patches (typically 1). It is used directly by the raw-page
        // mutator after WAL publication; no `Page` decode/encode occurs.
        let descriptor_count = 1 + col_patches.len();
        let _descriptor_charge =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                if descriptor_count > MAX_INLINE_COLS + 2 {
                    descriptor_count * std::mem::size_of::<(usize, &[u8])>()
                } else {
                    0
                },
            )?;
        let mut apply_patches: smallvec::SmallVec<[(usize, &[u8]); MAX_INLINE_COLS + 2]> =
            smallvec::SmallVec::with_capacity(descriptor_count);
        apply_patches.push((2, &new_header_bytes[..]));
        for (off, _, after) in &col_patches {
            apply_patches.push((*off, after.as_slice()));
        }

        // Build the WAL record. WalColumnPatch owns its before/after
        // bytes because the frame is deferred-appended at commit; the
        // patches need to survive the return from this function. This
        // is where the (unavoidable) Vec<u8> allocations land — one
        // pair per patch, mostly 4-8 bytes each.
        let mut wal_patches: Vec<WalColumnPatch> = Vec::with_capacity(1 + col_patches.len());
        wal_patches.push(WalColumnPatch {
            offset: 2,
            before: old_header_bytes.to_vec(),
            after: new_header_bytes.to_vec(),
        });
        for (off, before, after) in &col_patches {
            let offset = u16::try_from(*off).map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "heap patch offset exceeds u16 range",
                )
            })?;
            wal_patches.push(WalColumnPatch {
                offset,
                before: before.to_vec(),
                after: after.to_vec(),
            });
        }

        self.maybe_emit_heap_fpi(row_id.page_id)?;
        let key = self.page_key(row_id.page_id as u64);
        // Txn-buffering fast path: instead of taking the WAL mutex now, pre-
        // encode the record and park it for a single batched append at commit.
        // The page is stored with `dirty_lsn = 0` and pinned un-flushable
        // (`add_deferred_pin`) until commit publishes its real LSN. The pin MUST
        // be taken before the dirtying `store_page*` so a concurrent flush that
        // sees `dirty=true` also sees the pin. Falls back to eager when the
        // frame is not resident (add_deferred_pin == false) — that can only
        // happen on a buffer-pool race, and eager is always correct.
        if self.durability() != DurabilityMode::Memory
            && txn_wal_buffering_active(&self.log, txn_id)
        {
            let payload = WalPayload::HeapUpdateLogical {
                path: self.relative_path(),
                page_id: row_id.page_id,
                slot_id: row_id.slot_id,
                patches: wal_patches,
                undo_lsn: Some(new_undo_ptr),
            };
            if self.buffer_pool.add_deferred_pin(&key) {
                let applied = self.buffer_pool.with_raw_page_mut(key.clone(), 0, |bytes| {
                    patch_heap_page_raw(bytes, row_id.slot_id, &apply_patches)
                });
                if let Err(err) = applied {
                    self.buffer_pool.finalize_deferred(&key, 0);
                    return Err(err);
                }
                if let Err((payload, keys)) =
                    push_txn_wal_record(&self.log, txn_id, payload, vec![key])
                {
                    // The transaction buffer may disappear after the active
                    // probe (rollback/cancellation races with this statement).
                    // The page is already changed and pinned, so preserve the
                    // WAL-before-data contract through an eager append and
                    // balance the pin on both success and failure.
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
            } else {
                // Frame not resident — append eagerly with this record.
                let pre = crate::wal::LogManager::pre_encode_record(txn_id, payload)?;
                let lsn = crate::wal::LogManager::append_pre_encoded_to(&self.log, pre)?;
                self.buffer_pool.with_raw_page_mut(key, lsn, |bytes| {
                    patch_heap_page_raw(bytes, row_id.slot_id, &apply_patches)
                })?;
            }
            UPDATE_DELTA_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            UPDATE_IN_PLACE_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(true);
        }
        let lsn = if self.durability() == DurabilityMode::Memory {
            0
        } else {
            // Pre-encode the WAL record OUTSIDE the LogManager mutex —
            // encode + CRC are CPU-bound and would otherwise serialize
            // every concurrent writer. The mutex then only covers
            // reserve_lsn + header patch + pwrite + bookkeeping.
            let pre = crate::wal::LogManager::pre_encode_record(
                txn_id,
                WalPayload::HeapUpdateLogical {
                    path: self.relative_path(),
                    page_id: row_id.page_id,
                    slot_id: row_id.slot_id,
                    patches: wal_patches,
                    undo_lsn: Some(new_undo_ptr),
                },
            )?;
            crate::wal::LogManager::append_pre_encoded_to(&self.log, pre)?
        };
        self.buffer_pool
            .with_raw_page_mut(self.page_key(row_id.page_id as u64), lsn, |bytes| {
                patch_heap_page_raw(bytes, row_id.slot_id, &apply_patches)
            })?;
        UPDATE_DELTA_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // Patch path is by definition in-place — no slot relocation,
        // no migration. Bump the physical-outcome counter alongside
        // the patch hit/miss pair.
        UPDATE_IN_PLACE_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(true)
    }

    fn write_migrated_row_piece(
        &self,
        txn_id: u64,
        original_row_id: RowId,
        target: RowId,
        payload: Vec<u8>,
        undo_lsn: Option<u64>,
    ) -> Result<(), QueryError> {
        // Migration touches two pages: `target` (the existing row
        // piece) and `original_row_id` (the forwarding stub). Lock
        // both up front in bucket order to keep correctness when two
        // concurrent migrations involve overlapping pages.
        let (_g_target, _g_original) =
            self.lock_two_pages(target.page_id as u64, original_row_id.page_id as u64);
        let mut page = self.read_page_for_modify(target.page_id as u64)?;
        let before = page
            .read(target.slot_id)
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_KEY_NOT_FOUND,
                    "migrated row piece not found",
                )
            })?
            .to_vec();
        let migrated_payload = make_heap_row_piece_payload(&payload, &self.codec)?;
        if page
            .try_update(target.slot_id, &migrated_payload)?
            .is_some()
        {
            return self.write_heap_update_logged(
                txn_id,
                target,
                before,
                migrated_payload,
                page,
                undo_lsn,
            );
        }
        // The migrated piece grew past its current page's free space. Rather
        // than chaining a second forwarding hop, relocate the piece: insert
        // a fresh one elsewhere FIRST, repoint the original page's
        // forwarding stub at the new piece, and only THEN delete the old
        // piece. This ordering is critical for concurrent readers — between
        // any two of these steps the original `row_id` must always resolve
        // to *some* valid piece, otherwise a parallel SELECT can follow the
        // stub to a slot that's been deleted but not yet re-pointed and
        // wrongly report "row not found" (TPCC PAYMENT/NEW_ORDER's
        // "District not found" was reproduced by exactly this race).
        //
        // `insert_migrated_row_piece` may allocate a fresh page, which
        // takes its own latch on the new page. To avoid a deadlock if
        // that page hashes into the same latch bucket as the two we
        // hold here, drop ours first; the row's per-MVCC-chain
        // serialisation (each row has at most one active writer at a
        // time, enforced by `lock_row_for_update`) is what actually
        // prevents two threads from getting to this slow path on the
        // same row concurrently.
        drop(_g_target);
        drop(_g_original);
        drop(page);
        // Relocated pieces must stay hidden from heap scans, just like pieces
        // updated in place. Persist the same flagged payload in HeapInsert WAL.
        let new_target = self.insert_migrated_row_piece(txn_id, migrated_payload)?;
        let new_stub = encode_heap_forwarding_stub(new_target);
        // Re-acquire latches for the original + target pages in
        // bucket order before writing the stub / deleting the old
        // piece.
        let (_g_orig2, _g_target2) =
            self.lock_two_pages(original_row_id.page_id as u64, target.page_id as u64);
        let mut original_page = self.read_page_for_modify(original_row_id.page_id as u64)?;
        let stub_before = original_page
            .read(original_row_id.slot_id)
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_KEY_NOT_FOUND,
                    "forwarding stub not found",
                )
            })?
            .to_vec();
        original_page.update(original_row_id.slot_id, &new_stub)?;
        self.bump_row_migrations();
        ROW_MIGRATIONS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        UPDATE_MIGRATE_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.write_heap_update_logged(
            txn_id,
            original_row_id,
            stub_before,
            new_stub,
            original_page,
            undo_lsn,
        )?;
        // Now that no path can reach the old piece, drop it. Re-read its page
        // because the `page` we held earlier was a stale clone from before
        // the insert/stub-rewrite sequence.
        let mut old_page = self.read_page_for_modify(target.page_id as u64)?;
        if old_page.read(target.slot_id).is_some() {
            old_page.delete(target.slot_id)?;
            self.write_heap_delete_logged(txn_id, target, before, old_page, None)?;
        }
        Ok(())
    }

    fn insert_migrated_row_piece(
        &self,
        txn_id: u64,
        payload: Vec<u8>,
    ) -> Result<RowId, QueryError> {
        // Loop: try a fast insert into one of the tail pages without
        // the extend mutex, falling back to a serialised extend if
        // no existing page has room. A concurrent allocator may have
        // appended pages while we were checking, so re-read
        // `page_count` and retry.
        loop {
            let page_count = self.page_count()?;
            let start_page = page_count.saturating_sub(1).max(1);
            for page_id in start_page..page_count {
                let _page_guard = self.page_latch(page_id).lock().map_err(|_| {
                    error(crate::error::catalog::ER_GET_ERRNO, "page latch poisoned")
                })?;
                let mut page = self.read_page_for_modify(page_id)?;
                if let Some(slot_id) = page.insert(&payload) {
                    let row_id = RowId {
                        page_id: page_id as u32,
                        slot_id,
                    };
                    self.write_heap_insert_logged(txn_id, row_id, payload, page, None)?;
                    return Ok(row_id);
                }
            }
            // Tail page didn't fit; serialise file extension.
            let extend_guard = self
                .extend_mutex
                .lock()
                .map_err(|_| error(crate::error::catalog::ER_GET_ERRNO, "extend mutex poisoned"))?;
            let cur_count = self.page_count()?;
            if cur_count > page_count {
                // Another thread extended; retry the fast path.
                drop(extend_guard);
                continue;
            }
            let new_page_id = cur_count;
            let new_count = cur_count
                .checked_add(1)
                .filter(|n| *n <= u64::from(u32::MAX) + 1)
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                        "heap row-id address space exhausted",
                    )
                })?;
            crate::storage::temp_quota::reserve_extent(&self.path, new_count * PAGE_SIZE as u64)?;
            self.file
                .set_len(new_count * PAGE_SIZE as u64)
                .map_err(io_err)?;
            self.set_page_count(new_count)?;
            // Acquire the new page's latch before dropping the extend
            // mutex so a racing reader pinning the page goes through
            // the buffer pool's normal load path (it's empty on disk;
            // their reads see no data, our store will install bytes).
            let _page_guard = self
                .page_latch(new_page_id)
                .lock()
                .map_err(|_| error(crate::error::catalog::ER_GET_ERRNO, "page latch poisoned"))?;
            drop(extend_guard);
            let mut page = Page::new();
            let slot_id = page
                .insert(&payload)
                .expect("fresh page should have enough space");
            let row_id = RowId {
                page_id: new_page_id as u32,
                slot_id,
            };
            self.write_heap_insert_logged(txn_id, row_id, payload, page, None)?;
            return Ok(row_id);
        }
    }

    fn delete(&self, txn_id: u64, row_id: RowId) -> Result<(), QueryError> {
        let _page_guard = self
            .page_latch(row_id.page_id as u64)
            .lock()
            .map_err(|_| error(crate::error::catalog::ER_GET_ERRNO, "page latch poisoned"))?;
        let before = self.with_page(row_id.page_id as u64, |page| {
            page.payload(row_id.slot_id as usize)?
                .map(|payload| payload.to_vec())
                .ok_or_else(|| error(crate::error::catalog::ER_KEY_NOT_FOUND, "row not found"))
        })?;
        self.write_heap_delete_raw_logged(txn_id, row_id, before, None)
    }

    fn scan_mvcc(&self) -> Result<Vec<MvccStoredRow>, QueryError> {
        self.scan_mvcc_with_policy(ScanCachePolicy::Bulk)
    }

    fn scan_mvcc_with_policy(
        &self,
        policy: ScanCachePolicy,
    ) -> Result<Vec<MvccStoredRow>, QueryError> {
        let mut rows = Vec::new();
        self.scan_mvcc_until_with_policy(policy, |row| {
            rows.push(row);
            Ok(false)
        })?;
        Ok(rows)
    }

    fn scan_mvcc_until<F>(&self, visit: F) -> Result<(), QueryError>
    where
        F: FnMut(MvccStoredRow) -> Result<bool, QueryError>,
    {
        self.scan_mvcc_until_with_policy(ScanCachePolicy::Bulk, visit)
    }

    fn scan_mvcc_until_with_policy<F>(
        &self,
        policy: ScanCachePolicy,
        mut visit: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(MvccStoredRow) -> Result<bool, QueryError>,
    {
        self.scan_mvcc_pages_until_with_policy(None, policy, |row| visit(row))
    }

    fn scan_mvcc_pages_until<F>(&self, page_ids: Option<&[u64]>, visit: F) -> Result<(), QueryError>
    where
        F: FnMut(MvccStoredRow) -> Result<bool, QueryError>,
    {
        self.scan_mvcc_pages_until_with_policy(page_ids, ScanCachePolicy::Bulk, visit)
    }

    /// Normal inline rows retain the zero-copy scan. At the first forwarding
    /// anchor, release the frame read latch and resume from an owned page
    /// image. Following an anchor can revisit this very frame; recursively
    /// acquiring its std RwLock deadlocks behind a queued page writer.
    fn with_heap_scan_page(
        &self,
        pinned: &PinnedFrame,
        mut scan: impl FnMut(
            Option<HeapPageView<'_>>,
            usize,
            bool,
        ) -> Result<HeapScanPageResult, QueryError>,
    ) -> Result<bool, QueryError> {
        let result = pinned
            .with_heap_raw_bytes_cached(self.verified_direct_checksums.as_ref(), |page| {
                scan(page, 0, false)
            })?;
        let HeapScanPageResult::OwnedTail(start) = result else {
            let HeapScanPageResult::Done(stop) = result else {
                unreachable!()
            };
            return Ok(stop);
        };
        // Admission may reclaim buffers, so do it with no page latch held.
        // Keep at most one governed page copy, only for forwarding pages.
        let _charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
            PAGE_SIZE,
        )?;
        let owned = pinned
            .with_heap_raw_bytes_cached(self.verified_direct_checksums.as_ref(), |page| {
                Ok(page.map(|page| page.bytes.to_vec()))
            })?;
        match scan(
            owned.as_deref().map(HeapPageView::from_validated),
            start,
            true,
        )? {
            HeapScanPageResult::Done(stop) => Ok(stop),
            HeapScanPageResult::OwnedTail(_) => {
                unreachable!("owned scan must follow forwarding anchors")
            }
        }
    }

    fn scan_mvcc_pages_until_with_policy<F>(
        &self,
        page_ids: Option<&[u64]>,
        policy: ScanCachePolicy,
        mut visit: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(MvccStoredRow) -> Result<bool, QueryError>,
    {
        let page_count = self.page_count()?;
        let mut prefetch_horizon = 0u64;
        for page_id in HeapPageScan::new(page_count, page_ids) {
            if page_ids.is_none() {
                self.prefetch_after(page_id, page_count, &mut prefetch_horizon);
            }
            let intent = if page_ids.is_none() {
                policy.access_intent()
            } else {
                AccessIntent::Normal
            };
            let pinned = self
                .buffer_pool
                .pin_page_with_intent(self.page_key(page_id), intent)?;
            let stop = scan_mvcc_heap_page!(self, pinned, page_id, visit)?;
            if stop {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Scan a contiguous heap page range `[page_lo, page_hi)`. Used by
    /// parallel sequential scan: each worker is assigned a disjoint
    /// `[lo, hi)` partition computed from `page_count() / degree` so
    /// workers never see the same page. Uses `AccessIntent::Scan`
    /// (same as full scan) so the buffer pool's sequential prefetch
    /// fires within each worker's own range.
    fn scan_mvcc_pages_range_until<F>(
        &self,
        page_lo: u64,
        page_hi: u64,
        visit: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(MvccStoredRow) -> Result<bool, QueryError>,
    {
        self.scan_mvcc_pages_range_until_with_policy(page_lo, page_hi, ScanCachePolicy::Bulk, visit)
    }

    /// ALTER COPY sibling of the regular range scan. Ordinary inline rows
    /// expose their original payload alongside the decoded values so the
    /// shadow codec can byte-copy unchanged columns. Forwarded rows return
    /// `None` and use the full encode fallback.
    fn scan_mvcc_pages_range_with_payload_until<F>(
        &self,
        page_lo: u64,
        page_hi: u64,
        mut visit: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(MvccStoredRow, Option<&[u8]>) -> Result<bool, QueryError>,
    {
        let page_count = self.page_count()?;
        let scan_end = page_hi.min(page_count);
        let mut prefetch_horizon = 0u64;
        for page_id in HeapPageScan::from_range(page_count, page_lo, page_hi) {
            self.prefetch_after(page_id, scan_end, &mut prefetch_horizon);
            let pinned = self
                .buffer_pool
                .pin_page_with_intent(self.page_key(page_id), AccessIntent::Scan)?;
            let stop = self.with_heap_scan_page(&pinned, |page, start_slot, follow| {
                let Some(page) = page else {
                    return Ok(HeapScanPageResult::Done(false));
                };
                for slot_id in start_slot..page.slot_count {
                    let row_id = RowId {
                        page_id: page_id as u32,
                        slot_id: slot_id as u16,
                    };
                    let Some(payload) = page.payload(slot_id)? else {
                        continue;
                    };
                    let (header, values, original) =
                        match decode_heap_payload(payload, &self.codec)? {
                            HeapPayload::Row(header, values) => (header, values, Some(payload)),
                            HeapPayload::Forward(_) => {
                                if !follow {
                                    return Ok(HeapScanPageResult::OwnedTail(slot_id));
                                }
                                let Some(row) = self.read_mvcc_following(row_id, 0)? else {
                                    continue;
                                };
                                (row.header, row.values, None)
                            }
                            HeapPayload::RowPiece(_, _)
                            | HeapPayload::RowPieceColumns(..)
                            | HeapPayload::RowColumns(..) => continue,
                        };
                    if visit(
                        MvccStoredRow {
                            row_id,
                            header,
                            values,
                        },
                        original,
                    )? {
                        return Ok(HeapScanPageResult::Done(true));
                    }
                }
                Ok(HeapScanPageResult::Done(false))
            })?;
            if stop {
                return Ok(());
            }
        }
        Ok(())
    }

    fn scan_alter_copy_raw_pages_range_until<F>(
        &self,
        page_lo: u64,
        page_hi: u64,
        mut visit: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(RowId, RowHeader, Option<&[u8]>, Option<Vec<Value>>) -> Result<bool, QueryError>,
    {
        let page_count = self.page_count()?;
        let scan_end = page_hi.min(page_count);
        let mut prefetch_horizon = 0u64;
        for page_id in HeapPageScan::from_range(page_count, page_lo, page_hi) {
            self.prefetch_after(page_id, scan_end, &mut prefetch_horizon);
            let pinned = self
                .buffer_pool
                .pin_page_with_intent(self.page_key(page_id), AccessIntent::Scan)?;
            let stop = self.with_heap_scan_page(&pinned, |page, start_slot, follow| {
                let Some(page) = page else {
                    return Ok(HeapScanPageResult::Done(false));
                };
                for slot_id in start_slot..page.slot_count {
                    let row_id = RowId {
                        page_id: page_id as u32,
                        slot_id: slot_id as u16,
                    };
                    let Some(payload) = page.payload(slot_id)? else {
                        continue;
                    };
                    match decode_heap_payload_header(payload)? {
                        HeapPayloadHeader::Row(header) => {
                            if visit(row_id, header, Some(payload), None)? {
                                return Ok(HeapScanPageResult::Done(true));
                            }
                        }
                        HeapPayloadHeader::Forward => {
                            if !follow {
                                return Ok(HeapScanPageResult::OwnedTail(slot_id));
                            }
                            let Some(row) = self.read_mvcc_following(row_id, 0)? else {
                                continue;
                            };
                            if visit(row_id, row.header, None, Some(row.values))? {
                                return Ok(HeapScanPageResult::Done(true));
                            }
                        }
                        HeapPayloadHeader::RowPiece => continue,
                    }
                }
                Ok(HeapScanPageResult::Done(false))
            })?;
            if stop {
                return Ok(());
            }
        }
        Ok(())
    }

    fn scan_mvcc_pages_range_until_with_policy<F>(
        &self,
        page_lo: u64,
        page_hi: u64,
        policy: ScanCachePolicy,
        mut visit: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(MvccStoredRow) -> Result<bool, QueryError>,
    {
        let page_count = self.page_count()?;
        let scan_end = page_hi.min(page_count);
        let mut prefetch_horizon = 0u64;
        for page_id in HeapPageScan::from_range(page_count, page_lo, page_hi) {
            self.prefetch_after(page_id, scan_end, &mut prefetch_horizon);
            let pinned = self
                .buffer_pool
                .pin_page_with_intent(self.page_key(page_id), policy.access_intent())?;
            let stop = scan_mvcc_heap_page!(self, pinned, page_id, visit)?;
            if stop {
                return Ok(());
            }
        }
        Ok(())
    }

    fn scan_mvcc_columns_until<F>(&self, columns: &[usize], visit: F) -> Result<(), QueryError>
    where
        F: FnMut(ProjectedMvccStoredRow<'_>) -> Result<bool, QueryError>,
    {
        self.scan_mvcc_columns_until_with_policy(columns, ScanCachePolicy::Bulk, visit)
    }

    fn scan_mvcc_columns_until_with_policy<F>(
        &self,
        columns: &[usize],
        policy: ScanCachePolicy,
        visit: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(ProjectedMvccStoredRow<'_>) -> Result<bool, QueryError>,
    {
        self.scan_mvcc_columns_range_until_with_policy(columns, 0, u64::MAX, policy, visit)
    }

    /// Sequential read-only projected scan for offline index builds.
    ///
    /// The scan reads resident pages from their coherent buffer-pool byte
    /// image and cold pages directly from the heap file without admitting
    /// them.  This is both a correctness requirement for committed dirty
    /// pages and a performance property: callers no longer need to flush an
    /// entire freshly bulk-loaded heap before building an index, while a cold
    /// rebuild still avoids polluting the cache with a one-shot table scan.
    fn scan_mvcc_columns_direct_until<F>(
        &self,
        columns: &[usize],
        visit: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(ProjectedMvccStoredRow<'_>) -> Result<bool, QueryError>,
    {
        self.scan_mvcc_columns_direct_range_impl(columns, 0, self.page_count_relaxed(), visit)
    }

    /// Direct projected scan over one half-open physical page range.  This is
    /// the partitioning primitive used by offline parallel index builds: the
    /// caller assigns disjoint ranges, while this method keeps the ordinary
    /// direct-scan coherence rule (resident dirty image, otherwise positional
    /// file read) and never admits cold pages into the buffer pool.
    fn scan_mvcc_columns_direct_range_impl<F>(
        &self,
        columns: &[usize],
        start_page: u64,
        end_page: u64,
        mut visit: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(ProjectedMvccStoredRow<'_>) -> Result<bool, QueryError>,
    {
        // A direct index-build scan is sequential inside each assigned range.
        // Reading one 16 KiB page per positional syscall leaves fast NVMe
        // devices latency-bound, especially when several workers issue tiny
        // reads concurrently.  Batch up to 1 MiB while preserving the
        // coherence rule: resident buffer-pool images are copied into their
        // batch slots first and only contiguous cold gaps are read from disk.
        // The batch is governed StorageScratch and shrinks under pressure.
        const MAX_DIRECT_SCAN_BATCH_PAGES: usize = 64;
        let page_count = self.page_count_relaxed();
        let start_page = start_page.min(page_count);
        let end_page = end_page.max(start_page).min(page_count);
        if start_page == end_page {
            return Ok(());
        }
        let range_pages = usize::try_from(end_page - start_page).unwrap_or(usize::MAX);
        let mut batch_pages = MAX_DIRECT_SCAN_BATCH_PAGES.min(range_pages).max(1);
        let batch_charge = loop {
            let bytes = batch_pages.saturating_mul(PAGE_SIZE);
            match crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                bytes,
            ) {
                Ok(charge) => break charge,
                Err(_) if batch_pages > 1 => batch_pages = (batch_pages / 2).max(1),
                Err(err) => return Err(err),
            }
        };
        let mut page_buf = vec![0; batch_pages.saturating_mul(PAGE_SIZE)];
        let mut value_buf: Vec<Value> = Vec::with_capacity(columns.len());
        let plan = RowDecodePlan::compile_with_codec(columns, &self.codec);
        let mut scan_page = |page_id: u64, bytes: &[u8]| -> Result<bool, QueryError> {
            validate_page_owner(bytes, &self.file_key.owner, page_id)?;
            if page_id > 0 && bytes.iter().all(|byte| *byte == 0) {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "allocated heap page is zero",
                ));
            }
            let Some(slots) = Page::decode_slots(bytes)? else {
                return Ok(false);
            };
            let _table_id = self.table_id;
            for (slot_id, slot) in slots.iter().enumerate() {
                if slot.flags & ROW_FLAG_DELETED != 0 || slot.len == 0 {
                    continue;
                }
                let offset = slot.offset as usize;
                let end = offset + slot.len as usize;
                if end > bytes.len() {
                    continue;
                }
                let payload: &[u8] = &bytes[offset..end];
                let row_id = RowId {
                    page_id: page_id as u32,
                    slot_id: slot_id as u16,
                };
                let (header, stored_count) = match decode_heap_payload_columns_into_with_plan(
                    payload,
                    &plan,
                    &self.codec,
                    &mut value_buf,
                )? {
                    HeapPayloadOutcome::Row(header, stored_count) => (header, stored_count),
                    HeapPayloadOutcome::Forward(_) => {
                        let Some((header, values, stored_count)) =
                            self.read_mvcc_columns_following(row_id, columns, 0)?
                        else {
                            continue;
                        };
                        value_buf.clear();
                        value_buf.extend(values);
                        (header, stored_count)
                    }
                    HeapPayloadOutcome::Skip => continue,
                };
                if visit(ProjectedMvccStoredRow {
                    row_id,
                    header,
                    values: &value_buf,
                    stored_count,
                })? {
                    return Ok(true);
                }
            }
            Ok(false)
        };
        let mut batch_start = start_page;
        while batch_start < end_page {
            let count = usize::try_from(end_page - batch_start)
                .unwrap_or(usize::MAX)
                .min(batch_pages);
            let bytes_len = count.saturating_mul(PAGE_SIZE);
            let batch = &mut page_buf[..bytes_len];
            let mut cached_mask = 0u64;
            for position in 0..count {
                let page_id = batch_start + position as u64;
                let key = self.page_key(page_id);
                let offset = position * PAGE_SIZE;
                if self
                    .buffer_pool
                    .with_cached_dirty_heap_bytes_for_scan(&key, |bytes| {
                        batch[offset..offset + PAGE_SIZE].copy_from_slice(bytes);
                        Ok(())
                    })?
                    .is_some()
                {
                    cached_mask |= 1u64 << position;
                }
            }

            // Usually the complete batch is cold and this loop issues one
            // large pread.  Cached dirty pages split it into cold gaps; their
            // disk images are never consumed.
            let mut position = 0usize;
            while position < count {
                if cached_mask & (1u64 << position) != 0 {
                    position += 1;
                    continue;
                }
                let run_start = position;
                while position < count && cached_mask & (1u64 << position) == 0 {
                    position += 1;
                }
                let byte_start = run_start * PAGE_SIZE;
                let byte_end = position * PAGE_SIZE;
                read_exact_at_counted(
                    &self.file,
                    &mut batch[byte_start..byte_end],
                    (batch_start + run_start as u64) * PAGE_SIZE as u64,
                )
                .map_err(io_err)?;
            }

            for position in 0..count {
                let page_id = batch_start + position as u64;
                let offset = position * PAGE_SIZE;
                if scan_page(page_id, &batch[offset..offset + PAGE_SIZE])? {
                    return Ok(());
                }
            }
            batch_start += count as u64;
        }
        drop(batch_charge);
        Ok(())
    }

    /// Column-projected variant of `scan_mvcc_pages_range_until`. Used
    /// by the parallel sequential scan's projected path so each worker
    /// only decodes the requested columns from its assigned page range.
    fn scan_mvcc_columns_range_until<F>(
        &self,
        columns: &[usize],
        page_lo: u64,
        page_hi: u64,
        visit: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(ProjectedMvccStoredRow<'_>) -> Result<bool, QueryError>,
    {
        self.scan_mvcc_columns_range_until_with_policy(
            columns,
            page_lo,
            page_hi,
            ScanCachePolicy::Bulk,
            visit,
        )
    }

    fn scan_mvcc_columns_range_until_with_policy<F>(
        &self,
        columns: &[usize],
        page_lo: u64,
        page_hi: u64,
        policy: ScanCachePolicy,
        mut visit: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(ProjectedMvccStoredRow<'_>) -> Result<bool, QueryError>,
    {
        let page_count = self.page_count()?;
        let scan_end = page_hi.min(page_count);
        let mut value_buf: Vec<Value> = Vec::with_capacity(columns.len());
        let plan = RowDecodePlan::compile_with_codec(columns, &self.codec);
        let mut prefetch_horizon = 0u64;
        for page_id in HeapPageScan::from_range(page_count, page_lo, page_hi) {
            self.prefetch_after(page_id, scan_end, &mut prefetch_horizon);
            let _table_id = self.table_id;
            let mut forwarding = Vec::new();
            let stop = self.with_scan_page(page_id, page_count, policy, |page| {
                for slot_id in 0..page.slot_count() {
                    let Some(payload) = page.payload(slot_id)? else {
                        continue;
                    };
                    let row_id = RowId {
                        page_id: page_id as u32,
                        slot_id: slot_id as u16,
                    };
                    let (header, stored_count) = match decode_heap_payload_columns_into_with_plan(
                        payload,
                        &plan,
                        &self.codec,
                        &mut value_buf,
                    )? {
                        HeapPayloadOutcome::Row(header, stored_count) => (header, stored_count),
                        HeapPayloadOutcome::Forward(_) => {
                            forwarding.push(row_id);
                            continue;
                        }
                        HeapPayloadOutcome::Skip => continue,
                    };
                    if visit(ProjectedMvccStoredRow {
                        row_id,
                        header,
                        values: &value_buf,
                        stored_count,
                    })? {
                        return Ok(true);
                    }
                }
                Ok(false)
            })?;
            if stop {
                return Ok(());
            }
            for row_id in forwarding.drain(..) {
                let Some((header, values, stored_count)) =
                    self.read_mvcc_columns_following(row_id, columns, 0)?
                else {
                    continue;
                };
                value_buf.clear();
                value_buf.extend(values);
                if visit(ProjectedMvccStoredRow {
                    row_id,
                    header,
                    values: &value_buf,
                    stored_count,
                })? {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Resolve a migrated row after releasing the scan page latch. Keep the
    /// logical anchor for MVCC undo lookup and range ownership; visiting the
    /// physical piece independently would lose both guarantees.
    fn visit_forwarded_payload<F>(
        &self,
        anchor: RowId,
        plan: &RowDecodePlan,
        visit: &mut F,
    ) -> Result<bool, QueryError>
    where
        F: FnMut(PayloadMvccStoredRow<'_>) -> Result<bool, QueryError>,
    {
        let mut row_id = anchor;
        for depth in 0..=MAX_FORWARDING_DEPTH {
            if u64::from(row_id.page_id) >= self.page_count()? {
                return Ok(false);
            }
            // Copy only the rare migrated payload. No page latch survives a
            // forwarding hop, including hops back to the anchor's page.
            // Rare random targets use cold scan admission, not hot promotion.
            let payload = self.buffer_pool.with_raw_page_intent(
                self.page_key(u64::from(row_id.page_id)),
                AccessIntent::Scan,
                |page| Ok(read_page_payload(page, row_id)?.map(<[u8]>::to_vec)),
            )?;
            let Some(mut payload) = payload else {
                return Ok(false);
            };
            if let Some(target) = decode_heap_forwarding_stub(&payload)? {
                row_id = target;
                continue;
            }
            let (mut header, mut body_offset, has_external) = read_mvcc_header(&payload)?;
            if header.flags & MVCC_FLAG_ROW_PIECE != 0 {
                if depth == 0 {
                    return Ok(false);
                }
                // Expose a logical row to both typed consumers and their
                // projected-decoder fallback. Only this private copy changes.
                payload[1] &= !MVCC_FLAG_ROW_PIECE;
                header.flags &= !MVCC_FLAG_ROW_PIECE;
            }
            let layout_id =
                crate::storage::core::read_varint_at(&payload, &mut body_offset)? as u32;
            return visit(PayloadMvccStoredRow {
                row_id: anchor,
                header,
                payload: &payload,
                body_offset,
                layout_id,
                has_external,
                plan,
                codec: &self.codec,
            });
        }
        Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            &format!(
                "heap row migration chain too deep at row table={} page={} slot={} (max={})",
                self.table_id, anchor.page_id, anchor.slot_id, MAX_FORWARDING_DEPTH
            ),
        ))
    }

    /// Raw-payload variant of [`Self::scan_mvcc_columns_range_until`] for
    /// callers that decode into a typed sink (e.g. the vectorized scan
    /// bridge) rather than a `Vec<Value>`. The visit callback receives
    /// the slot's raw row bytes plus the codec/plan context it needs to
    /// drive [`crate::storage::core::decode_row_into_sink_fast`].
    /// MVCC visibility filtering is the caller's responsibility (this
    /// matches the columns-range API which also hands back the header
    /// for the caller to inspect).
    pub(crate) fn scan_mvcc_payloads_range_until_with_policy<F>(
        &self,
        columns: &[usize],
        page_lo: u64,
        page_hi: u64,
        policy: ScanCachePolicy,
        mut visit: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(PayloadMvccStoredRow<'_>) -> Result<bool, QueryError>,
    {
        let page_count = self.page_count()?;
        let scan_end = page_hi.min(page_count);
        let plan = RowDecodePlan::compile_with_codec(columns, &self.codec);
        let mut prefetch_horizon = 0u64;
        let mut forwarding = Vec::new();
        for page_id in HeapPageScan::from_range(page_count, page_lo, page_hi) {
            self.prefetch_after(page_id, scan_end, &mut prefetch_horizon);
            let stop = self.with_scan_page(page_id, page_count, policy, |page| {
                for slot_id in 0..page.slot_count() {
                    let Some(payload) = page.payload(slot_id)? else {
                        continue;
                    };
                    let row_id = RowId {
                        page_id: page_id as u32,
                        slot_id: slot_id as u16,
                    };
                    // Follow anchors after releasing the page latch. Pieces
                    // remain excluded from physical page iteration below.
                    if crate::storage::core::decode_heap_forwarding_stub_pub(payload)?.is_some() {
                        forwarding.push(row_id);
                        continue;
                    }
                    let Some((header, body_offset, has_external)) =
                        crate::storage::core::peek_mvcc_header(payload)
                    else {
                        continue;
                    };
                    let mut layout_pos = body_offset;
                    let layout_id =
                        crate::storage::core::read_varint_at(payload, &mut layout_pos)? as u32;
                    if header.flags & crate::storage::core::MVCC_FLAG_ROW_PIECE_PUB != 0 {
                        continue;
                    }
                    if visit(PayloadMvccStoredRow {
                        row_id,
                        header,
                        payload,
                        body_offset: layout_pos,
                        layout_id,
                        has_external,
                        plan: &plan,
                        codec: &self.codec,
                    })? {
                        return Ok(true);
                    }
                }
                Ok(false)
            })?;
            if stop {
                return Ok(());
            }
            for row_id in forwarding.drain(..) {
                if self.visit_forwarded_payload(row_id, &plan, &mut visit)? {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Raw-payload range scan that does not admit cold heap pages into the
    /// buffer pool. Resident pages are read from their coherent byte image so
    /// committed dirty rows remain visible; cold pages are read directly from
    /// the heap file. This is intended for one-shot offline consumers such as
    /// ANALYZE, where normal scan admission would evict the freshly loaded
    /// working set without providing reuse.
    pub(crate) fn scan_mvcc_payloads_direct_range_until<F>(
        &self,
        columns: &[usize],
        page_lo: u64,
        page_hi: u64,
        visit: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(PayloadMvccStoredRow<'_>) -> Result<bool, QueryError>,
    {
        let page_count = self.page_count_relaxed();
        let page_lo = page_lo.min(page_count);
        let page_hi = page_hi.min(page_count);
        // Bulk scans must not turn the whole relation into a stream of
        // one-page buffer-pool admissions and evictions. Read contiguous cold
        // gaps in governed 16 MiB morsels, while copying resident (including
        // dirty) pages from their coherent buffer image. The batch size is an
        // operator resource knob only; it does not affect scan semantics or
        // parallel topology.
        const MAX_DIRECT_PAYLOAD_BATCH_PAGES: usize = 1024;
        let range_pages = usize::try_from(page_hi.saturating_sub(page_lo)).unwrap_or(usize::MAX);
        let mut batch_pages = MAX_DIRECT_PAYLOAD_BATCH_PAGES.min(range_pages).max(1);
        let mut batch_charge = loop {
            let bytes = batch_pages.saturating_mul(PAGE_SIZE);
            match crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                bytes,
            ) {
                Ok(charge) => break charge,
                Err(_) if batch_pages > 1 => batch_pages = (batch_pages / 2).max(1),
                Err(err) => return Err(err),
            }
        };
        self.scan_mvcc_payloads_direct_range_with_charge_until(
            columns,
            page_lo,
            page_hi,
            &mut batch_charge,
            visit,
        )
    }

    /// Borrow exclusive prepaid scan capacity from a bulk-operation envelope.
    /// The caller retains the charge through the scan; no competing worker can
    /// consume this capacity between admission and the first page read.
    pub(crate) fn scan_mvcc_payloads_direct_range_with_charge_until<F>(
        &self,
        columns: &[usize],
        page_lo: u64,
        page_hi: u64,
        batch_charge: &mut crate::engine::executor::mem_budget::ControlledMemoryCharge,
        visit: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(PayloadMvccStoredRow<'_>) -> Result<bool, QueryError>,
    {
        self.scan_mvcc_payloads_range_with_charge_until(
            columns,
            page_lo,
            page_hi,
            batch_charge,
            false,
            visit,
        )
    }

    fn scan_mvcc_payloads_range_with_charge_until<F>(
        &self,
        columns: &[usize],
        page_lo: u64,
        page_hi: u64,
        batch_charge: &mut crate::engine::executor::mem_budget::ControlledMemoryCharge,
        coherent: bool,
        visit: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(PayloadMvccStoredRow<'_>) -> Result<bool, QueryError>,
    {
        let batch_pages = (batch_charge.bytes() / PAGE_SIZE).min(1024);
        assert!(batch_pages > 0, "direct scan requires a prepaid page");
        let mut buffer = vec![std::mem::MaybeUninit::<u8>::uninit(); batch_pages * PAGE_SIZE];
        self.scan_mvcc_payloads_direct_range_with_buffer_until(
            columns,
            page_lo,
            page_hi,
            &mut buffer,
            coherent,
            visit,
        )
    }

    pub(crate) fn scan_mvcc_payloads_direct_range_reusing_until<F>(
        &self,
        columns: &[usize],
        page_lo: u64,
        page_hi: u64,
        scratch: &mut DirectScanScratch,
        visit: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(PayloadMvccStoredRow<'_>) -> Result<bool, QueryError>,
    {
        let pages = usize::try_from(page_hi.saturating_sub(page_lo)).unwrap_or(usize::MAX);
        scratch.prepare(pages)?;
        self.scan_mvcc_payloads_direct_range_with_buffer_until(
            columns,
            page_lo,
            page_hi,
            &mut scratch.buffer,
            false,
            visit,
        )
    }

    fn scan_mvcc_payloads_direct_range_with_buffer_until<F>(
        &self,
        columns: &[usize],
        page_lo: u64,
        page_hi: u64,
        page_buf: &mut [std::mem::MaybeUninit<u8>],
        coherent: bool,
        mut visit: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(PayloadMvccStoredRow<'_>) -> Result<bool, QueryError>,
    {
        let page_count = self.page_count_relaxed();
        let page_lo = page_lo.min(page_count);
        let page_hi = page_hi.min(page_count);
        let plan = RowDecodePlan::compile_with_codec(columns, &self.codec);
        let batch_pages = page_buf.len() / PAGE_SIZE;
        let mut scan_page = |page_id: u64, bytes: &[u8]| -> Result<bool, QueryError> {
            if !self.validate_heap_page_header_for_direct_scan(page_id, bytes)? {
                return Ok(false);
            }
            // The direct scan only needs borrowed payload slices. Walking the
            // validated on-page slot directory avoids allocating and filling
            // one `Vec<Slot>` for every 16 KiB page.
            let page = HeapPageView::from_validated(bytes);
            for slot_id in 0..page.slot_count {
                let Some(payload) = page.payload(slot_id)? else {
                    continue;
                };
                if crate::storage::core::decode_heap_forwarding_stub_pub(payload)?.is_some() {
                    // Direct scan bytes are owned scratch, with no page latch
                    // held, so the anchor can be resolved immediately.
                    if self.visit_forwarded_payload(
                        RowId {
                            page_id: page_id as u32,
                            slot_id: slot_id as u16,
                        },
                        &plan,
                        &mut visit,
                    )? {
                        return Ok(true);
                    }
                    continue;
                }
                let Some((header, body_offset, has_external)) =
                    crate::storage::core::peek_mvcc_header(payload)
                else {
                    continue;
                };
                if header.flags & crate::storage::core::MVCC_FLAG_ROW_PIECE_PUB != 0 {
                    continue;
                }
                let mut layout_pos = body_offset;
                let layout_id =
                    crate::storage::core::read_varint_at(payload, &mut layout_pos)? as u32;
                if visit(PayloadMvccStoredRow {
                    row_id: RowId {
                        page_id: page_id as u32,
                        slot_id: slot_id as u16,
                    },
                    header,
                    payload,
                    body_offset: layout_pos,
                    layout_id,
                    has_external,
                    plan: &plan,
                    codec: &self.codec,
                })? {
                    return Ok(true);
                }
            }
            Ok(false)
        };

        let mut batch_start = page_lo;
        while batch_start < page_hi {
            let count = usize::try_from(page_hi - batch_start)
                .unwrap_or(usize::MAX)
                .min(batch_pages);
            let batch = &mut page_buf[..count.saturating_mul(PAGE_SIZE)];
            let mut cached = vec![false; count];
            for (position, is_cached) in cached.iter_mut().enumerate() {
                let page_id = batch_start + position as u64;
                let offset = position * PAGE_SIZE;
                if coherent {
                    // Online scans need a latched image even for a cold page:
                    // a concurrent flusher can otherwise race a direct pread.
                    let pinned = self.buffer_pool.pin_page(self.page_key(page_id))?;
                    self.buffer_pool
                        .with_finalized_frame_bytes(&pinned.frame, |bytes| {
                            assert_eq!(bytes.len(), PAGE_SIZE);
                            // SAFETY: a pinned full page and the prepaid scratch
                            // allocation are disjoint and both cover PAGE_SIZE.
                            unsafe {
                                std::ptr::copy_nonoverlapping(
                                    bytes.as_ptr(),
                                    batch.as_mut_ptr().add(offset).cast::<u8>(),
                                    PAGE_SIZE,
                                );
                            }
                            Ok(())
                        })?;
                    *is_cached = true;
                    continue;
                }
                if self
                    .buffer_pool
                    .with_cached_dirty_heap_bytes_for_scan(&self.page_key(page_id), |bytes| {
                        assert_eq!(bytes.len(), PAGE_SIZE);
                        // SAFETY: source and scratch are disjoint allocations;
                        // the cached page initializes this whole page range.
                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                bytes.as_ptr(),
                                batch.as_mut_ptr().add(offset).cast::<u8>(),
                                PAGE_SIZE,
                            );
                        }
                        Ok(())
                    })?
                    .is_some()
                {
                    *is_cached = true;
                }
            }
            let mut position = 0usize;
            while position < count {
                if cached[position] {
                    position += 1;
                    continue;
                }
                let run_start = position;
                while position < count && !cached[position] {
                    position += 1;
                }
                read_exact_at_uninit(
                    &self.file,
                    &mut batch[run_start * PAGE_SIZE..position * PAGE_SIZE],
                    (batch_start + run_start as u64) * PAGE_SIZE as u64,
                )
                .map_err(io_err)?;
            }
            // SAFETY: cached pages were copied in full and every remaining
            // contiguous run completed read_exact_at_uninit. An EOF/error
            // returned above without exposing any uninitialized bytes.
            let batch =
                unsafe { std::slice::from_raw_parts(batch.as_ptr().cast::<u8>(), batch.len()) };
            for position in 0..count {
                let offset = position * PAGE_SIZE;
                if scan_page(
                    batch_start + position as u64,
                    &batch[offset..offset + PAGE_SIZE],
                )? {
                    return Ok(());
                }
            }
            batch_start += count as u64;
        }
        Ok(())
    }

    // Decode fallback for typed direct-scan consumers. Keeping this beside
    // `decode_heap_payload_columns_into_with_plan` guarantees that a row on a
    // non-active layout follows the same projection/default rules as the
    // established Value-based scan.
    pub(crate) fn decode_direct_payload_projection_into(
        row: &PayloadMvccStoredRow<'_>,
        values: &mut Vec<Value>,
    ) -> Result<bool, QueryError> {
        Ok(matches!(
            decode_heap_payload_columns_into_with_plan(row.payload, row.plan, row.codec, values,)?,
            HeapPayloadOutcome::Row(..)
        ))
    }

    #[inline]
    fn validate_heap_page_header_for_direct_scan(
        &self,
        page_id: u64,
        bytes: &[u8],
    ) -> Result<bool, QueryError> {
        validate_page_owner(bytes, &self.file_key.owner, page_id)?;
        if page_id > 0 && bytes.iter().all(|b| *b == 0) {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "allocated heap page is zero",
            ));
        }
        if !validate_heap_page_structure(bytes)? {
            return Ok(false);
        }
        let stored_checksum = read_u32(bytes, 16);
        if self
            .verified_direct_checksums
            .as_ref()
            .is_some_and(|cache| cache.contains(page_id, stored_checksum))
        {
            return Ok(true);
        }
        let actual_checksum = page_checksum(bytes, 16);
        if stored_checksum != actual_checksum {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt heap page checksum",
            ));
        }
        if let Some(cache) = &self.verified_direct_checksums {
            cache.remember(page_id, stored_checksum);
        }
        Ok(true)
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn scan_mvcc_columns_predicate_first_range_until_with_policy<P, V>(
        &self,
        predicate_columns: &[usize],
        output_columns: &[usize],
        missing_columns: &[usize],
        output_sources: &[OutputSource],
        page_lo: u64,
        page_hi: u64,
        policy: ScanCachePolicy,
        mut predicate: P,
        mut visit: V,
    ) -> Result<(), QueryError>
    where
        P: FnMut(ProjectedMvccStoredRow<'_>) -> Result<bool, QueryError>,
        V: FnMut(ProjectedMvccStoredRow<'_>) -> Result<bool, QueryError>,
    {
        // `missing_columns` and `output_sources` are precomputed by the
        // caller (`database/scan.rs::PredicateFirstReadPlan`) so that
        // parallel partition scans don't each rebuild the same lookup —
        // see the module-level `OutputSource` docstring.
        debug_assert_eq!(output_sources.len(), output_columns.len());

        let page_count = self.page_count()?;
        let scan_end = page_hi.min(page_count);
        let mut pred_buf: Vec<Value> = Vec::with_capacity(predicate_columns.len());
        let mut missing_buf: Vec<Value> = Vec::with_capacity(missing_columns.len());
        let mut output_buf: Vec<Value> = Vec::with_capacity(output_columns.len());
        // Compile each projection plan once per scan; without this the per-row
        // `decode_heap_payload_columns_into` would rebuild them on every row.
        let pred_plan = RowDecodePlan::compile_with_codec(predicate_columns, &self.codec);
        let missing_plan = RowDecodePlan::compile_with_codec(missing_columns, &self.codec);
        let mut prefetch_horizon = 0u64;
        for page_id in HeapPageScan::from_range(page_count, page_lo, page_hi) {
            self.prefetch_after(page_id, scan_end, &mut prefetch_horizon);
            let _table_id = self.table_id;
            let mut forwarding = Vec::new();
            let stop = self.with_scan_page(page_id, page_count, policy, |page| {
                for slot_id in 0..page.slot_count() {
                    let Some(payload) = page.payload(slot_id)? else {
                        continue;
                    };
                    let row_id = RowId {
                        page_id: page_id as u32,
                        slot_id: slot_id as u16,
                    };
                    let (header, stored_count) = match decode_heap_payload_columns_into_with_plan(
                        payload,
                        &pred_plan,
                        &self.codec,
                        &mut pred_buf,
                    )? {
                        HeapPayloadOutcome::Row(header, stored_count) => (header, stored_count),
                        HeapPayloadOutcome::Forward(_) => {
                            forwarding.push(row_id);
                            continue;
                        }
                        HeapPayloadOutcome::Skip => continue,
                    };
                    if !predicate(ProjectedMvccStoredRow {
                        row_id,
                        header,
                        values: &pred_buf,
                        stored_count,
                    })? {
                        continue;
                    }

                    let output_stored_count = if missing_columns.is_empty() {
                        stored_count
                    } else {
                        match decode_heap_payload_columns_into_with_plan(
                            payload,
                            &missing_plan,
                            &self.codec,
                            &mut missing_buf,
                        )? {
                            HeapPayloadOutcome::Row(_, stored_count) => stored_count,
                            HeapPayloadOutcome::Forward(_) | HeapPayloadOutcome::Skip => continue,
                        }
                    };
                    if output_buf.len() != output_columns.len() {
                        output_buf.clear();
                        output_buf.resize_with(output_columns.len(), || Value::Null);
                    }
                    for (out_idx, source) in output_sources.iter().enumerate() {
                        let src = match source {
                            OutputSource::Predicate(pred_idx) => &pred_buf[*pred_idx],
                            OutputSource::Missing(missing_idx) => &missing_buf[*missing_idx],
                        };
                        crate::types::assign_value_reusing(&mut output_buf[out_idx], src);
                    }
                    if visit(ProjectedMvccStoredRow {
                        row_id,
                        header,
                        values: &output_buf,
                        stored_count: output_stored_count,
                    })? {
                        return Ok(true);
                    }
                }
                Ok(false)
            })?;
            if stop {
                return Ok(());
            }
            for row_id in forwarding.drain(..) {
                let Some((header, values, stored_count)) =
                    self.read_mvcc_columns_following(row_id, predicate_columns, 0)?
                else {
                    continue;
                };
                pred_buf.clear();
                pred_buf.extend(values);
                if !predicate(ProjectedMvccStoredRow {
                    row_id,
                    header,
                    values: &pred_buf,
                    stored_count,
                })? {
                    continue;
                }
                let Some((header, values, stored_count)) =
                    self.read_mvcc_columns_following(row_id, output_columns, 0)?
                else {
                    continue;
                };
                output_buf.clear();
                output_buf.extend(values);
                if visit(ProjectedMvccStoredRow {
                    row_id,
                    header,
                    values: &output_buf,
                    stored_count,
                })? {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    fn page_count(&self) -> Result<u64, QueryError> {
        Ok(self.page_count.load(std::sync::atomic::Ordering::Acquire))
    }

    pub(crate) fn page_count_relaxed(&self) -> u64 {
        self.page_count.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn set_page_count(&self, n: u64) -> Result<(), QueryError> {
        self.set_page_count_with_extent(n, None)
    }

    fn set_page_count_with_extent(
        &self,
        n: u64,
        reduced_extent_lsn: Option<u64>,
    ) -> Result<(), QueryError> {
        if n == 0 || n > u64::from(u32::MAX) + 1 {
            return Err(error(
                crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                "heap allocation exceeds row-id address space",
            ));
        }
        let old = self.page_count.load(Ordering::Acquire);
        // Initialize every advertised page. Recovery can repair these pages
        // from their WAL baseline; zero pages below the frontier are corrupt.
        for id in old..n {
            let mut image = Page::new().encode_into_owned();
            stamp_page_owner(&mut image, &self.file_key.owner, id);
            refresh_page_checksum(&mut image);
            let lsn = if let Some(lsn) = reduced_extent_lsn {
                lsn
            } else if self.durability() == DurabilityMode::Memory {
                0
            } else {
                self.log.lock().append(
                    0,
                    WalPayload::full_page_image(self.relative_path(), id, image.clone()),
                )?
            };
            set_page_lsn_if_formatted(&mut image, lsn);
            self.buffer_pool
                .store_page_cold(self.page_key(id), image, lsn)?;
        }
        update_heap_meta_with_key(
            self.page_key(0),
            &self.buffer_pool,
            &self.log,
            self.durability(),
            |meta| {
                write_u64(meta, 24, n);
                Ok(())
            },
        )?;
        self.page_count.store(n, Ordering::Release);
        Ok(())
    }

    fn page_latch(&self, page_id: u64) -> &Mutex<()> {
        &self.page_latches[(page_id & self.page_latch_mask) as usize]
    }

    /// Lock multiple page latches in ascending bucket order to avoid
    /// deadlock when a single operation modifies two pages (migration
    /// piece insert/forwarding stub rewrite).
    fn lock_two_pages(&self, a: u64, b: u64) -> (MutexGuard<'_, ()>, Option<MutexGuard<'_, ()>>) {
        let ai = (a & self.page_latch_mask) as usize;
        let bi = (b & self.page_latch_mask) as usize;
        if ai == bi {
            let g = self.page_latches[ai].lock().expect("page latch poisoned");
            return (g, None);
        }
        let (lo, hi) = if ai < bi { (ai, bi) } else { (bi, ai) };
        let g_lo = self.page_latches[lo].lock().expect("page latch poisoned");
        let g_hi = self.page_latches[hi].lock().expect("page latch poisoned");
        (g_lo, Some(g_hi))
    }

    fn durability(&self) -> DurabilityMode {
        durability_from_u8(self.durability.load(std::sync::atomic::Ordering::Acquire))
    }

    fn set_durability(&self, d: DurabilityMode) {
        self.durability
            .store(durability_to_u8(d), std::sync::atomic::Ordering::Release);
    }

    fn row_migrations_value(&self) -> u64 {
        self.row_migrations
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn bump_row_migrations(&self) {
        self.row_migrations
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn row_migration_count(&self) -> u64 {
        self.row_migrations_value()
    }

    fn with_page<T>(
        &self,
        page_id: u64,
        f: impl FnOnce(HeapPageView<'_>) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        let pinned = self
            .buffer_pool
            .pin_page_with_intent(self.page_key(page_id), AccessIntent::Normal)?;
        pinned.with_heap_raw_bytes_cached(self.verified_direct_checksums.as_ref(), |page| {
            let page = page
                .ok_or_else(|| error(crate::error::catalog::ER_KEY_NOT_FOUND, "row not found"))?;
            f(page)
        })
    }

    fn read_page(&self, page_id: u64) -> Result<Page, QueryError> {
        self.read_page_with_intent(page_id, AccessIntent::Normal)
    }

    /// Compatibility path for low-frequency page-wide transformations.
    /// The returned oracle is operation-local and never retained by a frame.
    fn read_page_for_modify(&self, page_id: u64) -> Result<Page, QueryError> {
        self.read_page(page_id)
    }

    fn read_page_with_intent(
        &self,
        page_id: u64,
        intent: AccessIntent,
    ) -> Result<Page, QueryError> {
        let pinned = self
            .buffer_pool
            .pin_page_with_intent(self.page_key(page_id), intent)?;
        pinned.with_heap_page(|page| Ok(page.clone()))
    }

    fn with_scan_page<T>(
        &self,
        page_id: u64,
        _table_page_count: u64,
        policy: ScanCachePolicy,
        f: impl FnOnce(HeapPageRead<'_>) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        let pinned = self
            .buffer_pool
            .pin_page_with_intent(self.page_key(page_id), policy.access_intent())?;
        pinned.with_heap_raw_bytes_cached(self.verified_direct_checksums.as_ref(), |page| {
            f(HeapPageRead::Raw(page))
        })
    }

    /// Incremental sequential-scan read-ahead. `horizon` is per-scan
    /// state owned by the caller's loop: the first page id beyond
    /// everything already submitted to the prefetcher. The first call
    /// submits the whole read-ahead window; each later call submits
    /// only the page(s) the sliding window newly exposed — usually one
    /// — so an N-page scan does O(N) prefetch bookkeeping instead of
    /// O(N × window). Re-submitting the full adaptive window (16..128
    /// pages) on every page made this the single hottest worker
    /// function (~34% of worker CPU) on fully-cached parallel heap
    /// scans, ahead of row decode.
    ///
    /// Pages the prefetcher sheds (queue cap / scan pressure) are not
    /// re-submitted: the scan reads them synchronously on arrival,
    /// which is the same load-shedding outcome the windowed form
    /// converged to under pressure.
    fn prefetch_after(&self, page_id: u64, page_count: u64, horizon: &mut u64) {
        let Some(start) = page_id.checked_add(1) else {
            return;
        };
        let end = start
            .saturating_add(self.buffer_pool.effective_prefetch_window() as u64)
            .min(page_count);
        let from = start.max(*horizon);
        if from >= end {
            return;
        }
        *horizon = end;
        self.buffer_pool.prefetch_range(&self.file_key, from, end);
    }

    /// Queue a single heap page for asynchronous prefetch. Used by
    /// covering-scan visibility checks to stream upcoming MVCC headers
    /// from disk while the caller is still serving cache hits — same
    /// principle as `prefetch_after` but driven by an arbitrary list
    /// of page IDs rather than a contiguous range.
    pub(crate) fn prefetch_page(&self, page_id: u64) {
        self.buffer_pool.prefetch_page(self.page_key(page_id));
    }

    pub(crate) fn prefetch_probe_pages(&self, page_ids: &[u64]) {
        self.buffer_pool
            .prefetch_pages(page_ids.iter().map(|page_id| self.page_key(*page_id)));
    }

    /// Complete one bounded MRR page window through the buffer pool's
    /// ordered batch-read path before row decode begins.
    pub(crate) fn load_ordered_probe_pages(&self, page_ids: &[u64]) -> Result<(), QueryError> {
        self.buffer_pool
            .load_ordered_probe_pages(&self.file_key, page_ids)
    }

    pub(crate) fn demote_ordered_probe_pages(&self, page_ids: &[u64]) -> Result<(), QueryError> {
        self.buffer_pool
            .demote_ordered_probe_pages(&self.file_key, page_ids)
    }

    pub(crate) fn ordered_probe_window_pages(&self) -> usize {
        (self.buffer_pool.capacity_pages() as usize / 8)
            .max(32)
            .min(4_096)
    }

    /// Number of distinct pages worth queueing ahead of the cursor on
    /// a covering-scan visibility walk. Sized to roughly fill the
    /// buffer pool's internal prefetch queue (`prefetch_pages` per I/O
    /// thread) so all I/O threads stay busy.
    pub(crate) fn prefetch_window_pages(&self) -> usize {
        self.buffer_pool
            .prefetch_base_pages()
            .saturating_mul(self.buffer_pool.io_threads)
            .max(1)
    }

    fn page_key(&self, page_id: u64) -> PageKey {
        PageKey {
            file: self.file_key.clone(),
            page_id,
        }
    }

    /// PostgreSQL-style first-update-after-checkpoint full-page image.
    ///
    /// When a heap page about to receive a logical WAL record was last
    /// stamped before the most recent checkpoint, emit a
    /// `WalPayload::FullPageImage` carrying the current on-disk image
    /// first. Recovery applies WAL records in LSN order, so the FPI
    /// restores any torn-disk image *before* the subsequent
    /// `HeapInsert` / `HeapUpdate` / `HeapDelete` / `HeapInsertBatch` /
    /// `HeapUpdateLogical` / `SlotReclaim` logical record is replayed
    /// onto it — closing the torn-page window for heap files, mirroring
    /// the existing protection on the B-tree side
    /// (see `btree.rs` `logical_wal_lsn` + `pristine_alloc_pages`).
    ///
    /// Skips the FPI when:
    /// - the page's last WAL stamp (its `page_lsn` field) is already
    ///   above the buffer pool's `last_checkpoint_lsn`. Per-checkpoint
    ///   per-page idempotency: once we've emitted the FPI in this
    ///   checkpoint window the page header advances past the
    ///   checkpoint LSN, and we don't re-emit until the next
    ///   checkpoint cycle. Using the dirty bit alone (the original
    ///   prototype) flipped clean every time the background flusher
    ///   cleaned the frame and burned a fresh 16 KiB FPI on every
    ///   subsequent dirty — measured at -36 % tpmC on TPC-C 4w20t/1m.
    /// - the page is *not* resident in the buffer pool — this is the
    ///   "brand-new page just `set_len`-extended" case where the
    ///   on-disk bytes are kernel-zero, so there is nothing to torn;
    /// - durability mode is not `Strict` (Relaxed / Memory accept the
    ///   torn-write risk for higher throughput / no-persistence);
    /// - we haven't completed a checkpoint yet (`last_checkpoint_lsn ==
    ///   0`). Recovery from an empty WAL trivially reconstructs every
    ///   page from the redo records that built it; there is no
    ///   pre-existing on-disk image to be torn.
    fn maybe_emit_heap_fpi(&self, page_id: u32) -> Result<(), QueryError> {
        if self.durability() != DurabilityMode::Strict {
            return Ok(());
        }
        // Emergency disable hatch — keep around as a perf-debugging
        // surface (and so future torn-page work has a known-good
        // "FPI off" baseline to compare against). `getenv` takes the
        // libc environ lock and linearly scans; on the per-page-write
        // hot path that showed up at ~0.13% of TPC-C CPU, so release
        // builds read it once per process. Test builds stay uncached so
        // `util::env::with_vars` scoping keeps working.
        #[cfg(not(test))]
        let fpi_disabled = {
            static DISABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
            *DISABLED.get_or_init(|| std::env::var_os("MYDB_DISABLE_HEAP_FPI").is_some())
        };
        #[cfg(test)]
        let fpi_disabled = crate::util::env::var("MYDB_DISABLE_HEAP_FPI").is_ok();
        if fpi_disabled {
            return Ok(());
        }
        let last_checkpoint_lsn = self.buffer_pool.last_checkpoint_lsn();
        let key = self.page_key(page_id as u64);
        if self
            .buffer_pool
            .cached_heap_redo_state(&key)?
            .is_some_and(|(lsn, reduced)| {
                !reduced && (last_checkpoint_lsn == 0 || lsn > last_checkpoint_lsn)
            })
        {
            return Ok(());
        }
        // Snapshot the page's pre-mutation bytes from the buffer pool.
        // The heap writers (`write_*_logged`) are organised so that
        // this helper runs before the post-mutation raw bytes are
        // installed, so the buffer-pool frame still carries the image
        // FPI's `after` payload.
        let Some(mut after) = self.buffer_pool.read_page_bytes(&key)? else {
            return Ok(());
        };
        // The disk image's `page_lsn` field tells us when this page
        // was last WAL-touched. If that LSN is already above the most
        // recent checkpoint, an earlier record in the same checkpoint
        // window has either emitted the FPI for this page or written
        // some other WAL record covering it — either way the page is
        // recoverable from the WAL tail without another full image.
        let reduced = read_u16(&after, 6) & HEAP_PAGE_REDUCED_REDO != 0;
        if !reduced && (last_checkpoint_lsn == 0 || page_lsn(&after) > last_checkpoint_lsn) {
            return Ok(());
        }
        write_u16(&mut after, 6, 0);
        // Compression, frame encoding and CRC must not serialize unrelated
        // appenders behind a 16 KiB page. Recovery stamps the actual record
        // LSN into an FPI before installation, including its page checksum.
        // The page latch held by our caller keeps this pre-image stable until
        // the following logical mutation; WAL ordering remains FPI first.
        set_page_lsn(&mut after, 0);
        let pre = crate::wal::LogManager::pre_encode_record(
            0, // FPI is a system record, not part of the transaction chain
            WalPayload::full_page_image(self.relative_path(), page_id as u64, after),
        )?;
        let lsn = crate::wal::LogManager::append_pre_encoded_to(&self.log, pre)?;
        // Stamp FPI coverage before any deferred mutation. Otherwise another
        // mutation can emit a system FPI containing an unlogged row version.
        self.buffer_pool.with_raw_page_mut(key, lsn, |bytes| {
            if reduced {
                write_u16(bytes, 6, 0);
            }
            Ok(())
        })?;
        Ok(())
    }

    fn write_heap_insert_logged(
        &self,
        txn_id: u64,
        row_id: RowId,
        payload: Vec<u8>,
        mut page: Page,
        undo_lsn: Option<u64>,
    ) -> Result<(), QueryError> {
        // FPI: emit a full-page image before the logical record so
        // recovery can recover from a torn on-disk page (see
        // `maybe_emit_heap_fpi` doc for the full predicate).
        self.maybe_emit_heap_fpi(row_id.page_id)?;
        let lsn = if self.durability() == DurabilityMode::Memory {
            0
        } else {
            let mut log = self.log.lock();
            let lsn = log.reserve_txn_lsn(txn_id)?;
            log.append_reserved(
                lsn,
                txn_id,
                WalPayload::HeapInsert {
                    path: self.relative_path(),
                    page_id: row_id.page_id,
                    slot_id: row_id.slot_id,
                    payload,
                    undo_lsn,
                },
            )?;
            lsn
        };
        if lsn != 0 {
            page.set_lsn(lsn);
        }
        self.buffer_pool.store_page(
            self.page_key(row_id.page_id as u64),
            page.encode_into_owned(),
            lsn,
        )?;
        Ok(())
    }

    fn write_heap_insert_raw_logged(
        &self,
        txn_id: u64,
        row_id: RowId,
        payload: Vec<u8>,
        plan: HeapInsertPlan,
        undo_lsn: Option<u64>,
    ) -> Result<(), QueryError> {
        let _scratch_charge = if plan.compact {
            Some(
                crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                    PAGE_SIZE,
                )?,
            )
        } else {
            None
        };
        let mut scratch = plan.compact.then(|| vec![0_u8; PAGE_SIZE]);
        self.maybe_emit_heap_fpi(row_id.page_id)?;
        let lsn = if self.durability() == DurabilityMode::Memory {
            0
        } else {
            let mut log = self.log.lock();
            let lsn = log.reserve_txn_lsn(txn_id)?;
            log.append_reserved(
                lsn,
                txn_id,
                WalPayload::HeapInsert {
                    path: self.relative_path(),
                    page_id: row_id.page_id,
                    slot_id: row_id.slot_id,
                    payload: payload.clone(),
                    undo_lsn,
                },
            )?;
            lsn
        };
        self.buffer_pool
            .with_raw_page_mut(self.page_key(row_id.page_id as u64), lsn, |bytes| {
                HeapPageMut::new(bytes)?.insert(&payload, plan, scratch.as_deref_mut())
            })
    }

    fn write_heap_insert_batch_logged(
        &self,
        txn_id: u64,
        mut inserts: Vec<WalHeapInsert>,
        pages: BTreeMap<u64, Page>,
        direct_fresh_append: bool,
        fresh_extent: Option<(u64, u64)>,
    ) -> Result<(), QueryError> {
        #[cfg(test)]
        HEAP_BATCH_PUBLISH_TEST_HOOK.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() {
                hook();
            }
        });
        if inserts.is_empty() {
            return Ok(());
        }
        // Emit FPI per page (each page in the batch may independently
        // be clean-since-last-checkpoint). LOAD DATA / bulk inserts
        // typically target brand-new extended pages where the helper
        // short-circuits via the "not in buffer pool → no torn risk"
        // branch, so the steady-state cost is near zero.
        let reduced_fresh_start = fresh_extent.map(|(start, _)| start);
        for page_id in pages.keys() {
            if !reduced_fresh_start.is_some_and(|start| *page_id >= start) {
                self.maybe_emit_heap_fpi(*page_id as u32)?;
            }
        }
        let skipped_payload_bytes = reduced_fresh_start.map_or(0_u64, |start| {
            inserts
                .iter()
                .filter(|insert| u64::from(insert.page_id) >= start)
                .map(|insert| insert.payload.len() as u64)
                .sum()
        });
        if let Some(start) = reduced_fresh_start {
            inserts.retain(|insert| u64::from(insert.page_id) < start);
            if skipped_payload_bytes > 0 {
                REDUCED_REDO_SKIPPED_RECORDS.fetch_add(1, Ordering::Relaxed);
                REDUCED_REDO_SKIPPED_PAYLOAD_BYTES
                    .fetch_add(skipped_payload_bytes, Ordering::Relaxed);
            }
        }
        let retained_fallback = !inserts.is_empty();
        // Resident ordinary inserts need neither an immediate WAL latch nor
        // direct file writes. Pin every target before queueing redo, and keep
        // the transaction-buffer latch through the image stores so a parallel
        // transaction worker cannot finalize their pins before publication.
        // Fresh/direct reduced-redo extents retain their data-before-commit
        // path; a missing resident frame retains the eager append oracle.
        if self.durability() != DurabilityMode::Memory
            && retained_fallback
            && !direct_fresh_append
            && fresh_extent.is_none()
            && txn_wal_buffering_active(&self.log, txn_id)
        {
            let keys = pages.keys().map(|id| self.page_key(*id)).collect();
            let payload = WalPayload::HeapInsertBatch {
                path: self.relative_path(),
                inserts,
            };
            let write = |lsn| -> Result<(), QueryError> {
                for (page_id, mut page) in pages {
                    if lsn != 0 {
                        page.set_lsn(lsn);
                    }
                    self.buffer_pool.store_page_cold(
                        self.page_key(page_id),
                        page.encode_into_owned(),
                        lsn,
                    )?;
                }
                Ok(())
            };
            return match try_buffer_txn_page_write(
                &self.log,
                &self.buffer_pool,
                txn_id,
                payload,
                keys,
                write,
            ) {
                Ok(result) => result,
                Err((payload, write)) => {
                    let pre = crate::wal::LogManager::pre_encode_record(txn_id, payload)?;
                    let lsn = crate::wal::LogManager::append_pre_encoded_to(&self.log, pre)?;
                    write(lsn)
                }
            };
        }
        let logged_lsn = if self.durability() == DurabilityMode::Memory {
            0
        } else if inserts.is_empty() {
            0
        } else {
            // Large fresh-extent batches are produced concurrently by LOAD
            // workers. Encoding their row images and CRC while holding the
            // global WAL append mutex serialized concurrent workers on memory
            // work even when CPU and storage were idle. Pre-encode into a
            // governed per-thread buffer; the mutex-side append only patches
            // LSN links and copies the finished frame into the WAL stream.
            let pre = crate::wal::LogManager::pre_encode_record(
                txn_id,
                WalPayload::HeapInsertBatch {
                    path: self.relative_path(),
                    inserts,
                },
            )?;
            crate::wal::LogManager::append_pre_encoded_to(&self.log, pre)?
        };
        if txn_reduced_redo_active(&self.log, txn_id) && retained_fallback {
            REDUCED_REDO_FALLBACK_RECORDS.fetch_add(1, Ordering::Relaxed);
        }
        // Heap insert batches grow from the previous tail page through newly
        // allocated pages, so their keys are contiguous. Remember that range
        // before consuming the map; after admission the buffer pool can retain
        // this physical order for coalesced writeback. If this invariant ever
        // changes, skip the optimization and retain ordinary dirty writeback.
        let bulk_page_range = pages
            .first_key_value()
            .zip(pages.last_key_value())
            .and_then(|((first, _), (last, _))| {
                let contiguous_len = last.checked_sub(*first)?.checked_add(1)?;
                (contiguous_len == pages.len() as u64).then_some((*first, *last))
            });

        // Fresh parallel-LOAD extents have no pre-existing page image to
        // preserve. After making their logical WAL durable, write bounded
        // contiguous page runs directly instead of admitting every cold page
        // into the buffer pool only to synchronously flush and evict it. This
        // is the heap analogue of the direct empty-index builder: it remains
        // fully WAL-backed, keeps memory bounded to 128 page images, and uses
        // positioned writes so disjoint worker extents can proceed in
        // parallel.
        if direct_fresh_append {
            let direct_lsn = fresh_extent.map(|(_, lsn)| lsn).unwrap_or(logged_lsn);
            if direct_lsn != 0 {
                crate::wal::LogManager::sync_durable_to(&self.log, direct_lsn)?;
            }
            // Allocation baselines must not race a direct after-image write.
            // discard_page waits for any old writeback before returning.
            if let Some((first, last)) = bulk_page_range {
                for id in first..=last {
                    self.buffer_pool.discard_page(&self.page_key(id))?;
                }
            }
            const DIRECT_RUN_PAGES: usize = 128;
            let mut images = Vec::with_capacity(DIRECT_RUN_PAGES * PAGE_SIZE);
            let mut run_first = None;
            let mut run_pages = 0usize;
            for (page_id, mut page) in pages {
                if direct_lsn != 0 {
                    page.set_lsn(direct_lsn);
                }
                if run_first.is_none() {
                    run_first = Some(page_id);
                }
                debug_assert_eq!(
                    Some(page_id),
                    run_first.map(|first| first + run_pages as u64)
                );
                let flags = if fresh_extent.is_some() {
                    HEAP_PAGE_REDUCED_REDO
                } else {
                    0
                };
                let mut image = page.encode_into_owned_with_flags(flags);
                stamp_page_owner(&mut image, &self.file_key.owner, page_id);
                refresh_page_checksum(&mut image);
                images.extend_from_slice(&image);
                run_pages += 1;
                if run_pages == DIRECT_RUN_PAGES {
                    write_all_at(
                        &self.file,
                        &images,
                        run_first.unwrap().saturating_mul(PAGE_SIZE as u64),
                    )
                    .map_err(io_err)?;
                    images.clear();
                    run_first = None;
                    run_pages = 0;
                }
            }
            if let Some(first_page_id) = run_first {
                write_all_at(
                    &self.file,
                    &images,
                    first_page_id.saturating_mul(PAGE_SIZE as u64),
                )
                .map_err(io_err)?;
            }
            if let Some((first_page_id, last_page_id)) = bulk_page_range {
                // A racing uncommitted scan can have cached an all-zero image
                // after the extent was reserved but before its positioned
                // write completed. Remove only this new range so every later
                // reader reloads the authoritative page bytes from disk.
                for page_id in first_page_id..=last_page_id {
                    self.buffer_pool.discard_page(&self.page_key(page_id))?;
                }
            }
            return Ok(());
        }

        // Consume each `Page` so `encode_into_owned` can re-use its
        // own 16 KB `data` Vec instead of cloning it. Each clone was
        // a fresh 16 KB malloc + memcpy on the writer thread; profile
        // (TPC-H SF=1 lineitem PARALLEL(2)) attributed a meaningful
        // chunk of `_platform_memmove` on the writer to those page
        // clones (~500 pages × ~120 batches = 60K clones per load).
        //
        for (page_id, mut page) in pages.into_iter() {
            let page_lsn = match fresh_extent {
                Some((start, growth_lsn)) if page_id >= start => growth_lsn,
                _ => logged_lsn,
            };
            if page_lsn != 0 {
                page.set_lsn(page_lsn);
            }
            let flags = if reduced_fresh_start.is_some_and(|start| page_id >= start) {
                HEAP_PAGE_REDUCED_REDO
            } else {
                0
            };
            self.buffer_pool.store_page_cold(
                self.page_key(page_id),
                page.encode_into_owned_with_flags(flags),
                page_lsn,
            )?;
        }
        if let Some((first_page_id, last_page_id)) = bulk_page_range {
            self.buffer_pool.schedule_bulk_page_range(
                &self.file_key,
                first_page_id,
                last_page_id,
            )?;
        }
        Ok(())
    }

    fn write_heap_update_logged(
        &self,
        txn_id: u64,
        row_id: RowId,
        before: Vec<u8>,
        payload: Vec<u8>,
        mut page: Page,
        undo_lsn: Option<u64>,
    ) -> Result<(), QueryError> {
        self.maybe_emit_heap_fpi(row_id.page_id)?;
        let lsn = if self.durability() == DurabilityMode::Memory {
            0
        } else {
            let pre = crate::wal::LogManager::pre_encode_record(
                txn_id,
                WalPayload::HeapUpdate {
                    path: self.relative_path(),
                    page_id: row_id.page_id,
                    slot_id: row_id.slot_id,
                    before,
                    after: payload,
                    undo_lsn,
                },
            )?;
            crate::wal::LogManager::append_pre_encoded_to(&self.log, pre)?
        };
        if lsn != 0 {
            page.set_lsn(lsn);
        }
        self.buffer_pool.store_page(
            self.page_key(row_id.page_id as u64),
            page.encode_into_owned(),
            lsn,
        )?;
        Ok(())
    }

    fn write_heap_update_raw_logged(
        &self,
        txn_id: u64,
        row_id: RowId,
        before: Vec<u8>,
        payload: Vec<u8>,
        plan: HeapUpdatePlan,
        undo_lsn: Option<u64>,
    ) -> Result<(), QueryError> {
        let _scratch_charge = if plan == HeapUpdatePlan::Compact {
            Some(
                crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                    PAGE_SIZE,
                )?,
            )
        } else {
            None
        };
        let mut scratch = (plan == HeapUpdatePlan::Compact).then(|| vec![0_u8; PAGE_SIZE]);
        self.maybe_emit_heap_fpi(row_id.page_id)?;
        let mut write = |lsn| {
            self.buffer_pool
                .with_raw_page_mut(self.page_key(row_id.page_id as u64), lsn, |bytes| {
                    HeapPageMut::new(bytes)?.update(
                        row_id.slot_id,
                        &payload,
                        plan,
                        scratch.as_deref_mut(),
                    )
                })
        };
        if self.durability() == DurabilityMode::Memory {
            return write(0);
        }
        let redo = WalPayload::HeapUpdate {
            path: self.relative_path(),
            page_id: row_id.page_id,
            slot_id: row_id.slot_id,
            before,
            after: payload.clone(),
            undo_lsn,
        };
        match try_buffer_txn_page_write(
            &self.log,
            &self.buffer_pool,
            txn_id,
            redo,
            vec![self.page_key(row_id.page_id as u64)],
            write,
        ) {
            Ok(result) => result,
            Err((redo, mut write)) => {
                let pre = crate::wal::LogManager::pre_encode_record(txn_id, redo)?;
                let lsn = crate::wal::LogManager::append_pre_encoded_to(&self.log, pre)?;
                write(lsn)
            }
        }
    }

    fn write_heap_delete_logged(
        &self,
        txn_id: u64,
        row_id: RowId,
        before: Vec<u8>,
        mut page: Page,
        undo_lsn: Option<u64>,
    ) -> Result<(), QueryError> {
        self.maybe_emit_heap_fpi(row_id.page_id)?;
        let lsn = if self.durability() == DurabilityMode::Memory {
            0
        } else {
            let pre = crate::wal::LogManager::pre_encode_record(
                txn_id,
                WalPayload::HeapDelete {
                    path: self.relative_path(),
                    page_id: row_id.page_id,
                    slot_id: row_id.slot_id,
                    before,
                    undo_lsn,
                },
            )?;
            crate::wal::LogManager::append_pre_encoded_to(&self.log, pre)?
        };
        if lsn != 0 {
            page.set_lsn(lsn);
        }
        self.buffer_pool.store_page(
            self.page_key(row_id.page_id as u64),
            page.encode_into_owned(),
            lsn,
        )?;
        Ok(())
    }

    fn write_heap_delete_raw_logged(
        &self,
        txn_id: u64,
        row_id: RowId,
        before: Vec<u8>,
        undo_lsn: Option<u64>,
    ) -> Result<(), QueryError> {
        self.maybe_emit_heap_fpi(row_id.page_id)?;
        let write = |lsn| {
            self.buffer_pool
                .with_raw_page_mut(self.page_key(row_id.page_id as u64), lsn, |bytes| {
                    HeapPageMut::new(bytes)?.delete(row_id.slot_id)
                })
        };
        if self.durability() == DurabilityMode::Memory {
            return write(0);
        }
        let redo = WalPayload::HeapDelete {
            path: self.relative_path(),
            page_id: row_id.page_id,
            slot_id: row_id.slot_id,
            before,
            undo_lsn,
        };
        match try_buffer_txn_page_write(
            &self.log,
            &self.buffer_pool,
            txn_id,
            redo,
            vec![self.page_key(row_id.page_id as u64)],
            write,
        ) {
            Ok(result) => result,
            Err((redo, write)) => {
                let pre = crate::wal::LogManager::pre_encode_record(txn_id, redo)?;
                let lsn = crate::wal::LogManager::append_pre_encoded_to(&self.log, pre)?;
                write(lsn)
            }
        }
    }

    /// rowid-6bytes M2: flip an MVCC-tombstoned slot to `ROW_FLAG_FREE`
    /// so the slot id becomes reusable on the next INSERT. The caller
    /// must have already proved the slot is reclaim-safe:
    ///   * `oldest_active_snapshot_scn > deleted_scn` (no MVCC reader
    ///     can still need to see the tombstone), and
    ///   * every secondary-index entry that previously pointed at this
    ///     `(page_id, slot_id)` has been removed.
    ///
    /// Both invariants are upheld by `Database::reclaim_tombstoned_slots`:
    /// it is driven by the same `purge_except_chains` decision that
    /// drops the Delete undo record (creator_state committed AND
    /// `commit_scn <= oldest_snapshot_scn`), and `delete_indexes_deferred_at`
    /// already moved the secondary-index entries during DELETE commit.
    ///
    /// Note on the M1 slot-flag layout: the user-DML MVCC delete path
    /// (`write_mvcc` with `RowHeader.flags = MVCC_FLAG_DELETED`) writes
    /// the deletion bit into the **row payload's** MVCC header but
    /// leaves the **slot directory's** `flags` byte at 0 — i.e. the
    /// page-level state machine stays "allocated, points at live
    /// bytes" while the MVCC layer above considers the row dead. This
    /// is what gives MVCC readers under an older snapshot enough state
    /// to materialise the pre-delete version. Reclaim therefore peeks
    /// at the row payload's MVCC header to confirm tombstone status,
    /// not at the slot's `flags`.
    ///
    /// Returns `Ok(true)` if the slot transitioned tombstone → FREE,
    /// `Ok(false)` if the slot is already FREE (idempotent re-apply of
    /// a `SlotReclaim` WAL record), out-of-range, points at a live
    /// row, or contains an MVCC row whose header is non-deleted (the
    /// reclaim is then a no-op rather than corruption). Writes a
    /// `WalPayload::SlotReclaim` record before flipping the page.
    pub(crate) fn reclaim_slot_logged(&self, row_id: RowId) -> Result<bool, QueryError> {
        let _page_guard = self
            .page_latch(row_id.page_id as u64)
            .lock()
            .map_err(|_| error(crate::error::catalog::ER_GET_ERRNO, "page latch poisoned"))?;
        let mut page = self.read_page(row_id.page_id as u64)?;
        if !page.is_slot_mvcc_tombstone(row_id.slot_id) {
            return Ok(false);
        }
        self.maybe_emit_heap_fpi(row_id.page_id)?;
        let lsn = if self.durability() == DurabilityMode::Memory {
            0
        } else {
            let mut log = self.log.lock();
            let lsn = log.reserve_lsn()?;
            log.append_reserved(
                lsn,
                0,
                WalPayload::SlotReclaim {
                    path: self.relative_path(),
                    page_id: row_id.page_id,
                    slot_id: row_id.slot_id,
                },
            )?;
            lsn
        };
        page.mark_slot_free(row_id.slot_id)?;
        if lsn != 0 {
            page.set_lsn(lsn);
        }
        self.buffer_pool
            .store_page(self.page_key(row_id.page_id as u64), page.encode(), lsn)?;
        // The tail page is already every insert's target; only older pages
        // need advertising.
        if u64::from(row_id.page_id) + 1 < self.page_count_relaxed() {
            self.free_pages.offer(row_id.page_id);
        }
        Ok(true)
    }

    fn flush_dirty_pages(&self) -> Result<(), QueryError> {
        if self.durability() == DurabilityMode::Memory {
            return Ok(());
        }
        self.buffer_pool.flush_file_pages(&self.file_key)?;
        (&self.file).flush().map_err(io_err)?;
        self.buffer_pool.sync_cached_and_direct_file(
            &self.file_key,
            &self.file,
            self.durability() == DurabilityMode::Strict,
        )?;
        Ok(())
    }

    /// Test-only: report a slot's flag bits (`ROW_FLAG_DELETED` /
    /// `ROW_FLAG_FREE`) so unit tests can assert the M2 reclaim state
    /// machine transitions. Returns `None` for missing / out-of-range
    /// slots. Bypasses the MVCC visibility filter that the public
    /// `read_mvcc` path applies, so tombstones are visible here.
    #[cfg(test)]
    pub(crate) fn slot_flags_for_test(&self, row_id: RowId) -> Result<Option<u8>, QueryError> {
        let page = self.read_page(row_id.page_id as u64)?;
        Ok(page
            .slots
            .get(row_id.slot_id as usize)
            .map(|slot| slot.flags))
    }

    fn discard_without_flush(&mut self) {
        let _ = self.buffer_pool.discard_file(&self.file_key);
        self.set_durability(DurabilityMode::Memory);
        // An explicit retirement already discarded this owner's frames. Its
        // delayed drop must not discard pages installed by a replacement.
        self.discard_pages_on_drop = false;
    }

    fn retain_buffered_pages_on_drop(&mut self) {
        self.discard_pages_on_drop = false;
    }

    fn relative_path(&self) -> String {
        self.rel_path.clone()
    }
}

/// Process-start diagnostic escape hatch, also allowing same-binary A/B
/// measurements without changing memory, worker admission or SQL plans.
fn dense_probe_streaming_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        !crate::util::env::var("MYDB_DENSE_PROBE_READ").is_ok_and(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "0" | "off" | "false" | "no"
            )
        })
    })
}

/// Partition by physical locality, not the span of the whole batch: parallel
/// producers can interleave distant but individually dense page ranges. One
/// unrequested page per gap is allowed, bounding read amplification by two.
fn dense_probe_batch_end(row_ids: &[RowId], start: usize) -> usize {
    let first = u64::from(row_ids[start].page_id);
    let mut end = start + 1;
    while end < row_ids.len()
        && u64::from(row_ids[end].page_id) < first + 64
        && u64::from(row_ids[end].page_id) <= u64::from(row_ids[end - 1].page_id) + 2
    {
        end += 1;
    }
    end
}

fn dense_probe_worth_streaming(row_ids: &[RowId], heap_pages: u64, pool_pages: u64) -> bool {
    if row_ids.len() < 1024 || heap_pages <= pool_pages {
        return false;
    }
    let first = u64::from(row_ids[0].page_id);
    let last = u64::from(row_ids[row_ids.len() - 1].page_id);
    if last >= heap_pages || first > last {
        return false;
    }
    let mut pages = 1u64;
    for pair in row_ids.windows(2) {
        if pair[0] > pair[1] {
            return false;
        }
        pages += u64::from(pair[0].page_id != pair[1].page_id);
    }
    let mut batches = 0u64;
    let mut start = 0;
    while start < row_ids.len() {
        start = dense_probe_batch_end(row_ids, start);
        batches += 1;
    }
    // Demand probes keep their caching behavior unless coalescing saves at
    // least eight page reads per batch on average.
    pages >= 32 && pages >= batches.saturating_mul(8)
}

fn read_page_payload(buf: &[u8], row_id: RowId) -> Result<Option<&[u8]>, QueryError> {
    if !validate_heap_page_header(buf)? {
        return Ok(None);
    }
    read_validated_page_payload(buf, row_id)
}

/// Borrowed, validated heap-page image used by scans that cannot keep the
/// table resident. It deliberately owns neither a 16 KiB copy nor a slot Vec.
#[derive(Debug)]
struct HeapPageView<'a> {
    bytes: &'a [u8],
    slot_count: usize,
}

impl<'a> HeapPageView<'a> {
    #[cfg(test)]
    fn new(bytes: &'a [u8]) -> Result<Option<Self>, QueryError> {
        if !validate_heap_page_header(bytes)? {
            return Ok(None);
        }
        Ok(Some(Self::from_validated(bytes)))
    }

    fn from_validated(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            slot_count: read_u16(bytes, 20) as usize,
        }
    }

    fn payload(&self, slot_id: usize) -> Result<Option<&'a [u8]>, QueryError> {
        if slot_id >= self.slot_count || slot_id > u16::MAX as usize {
            return Ok(None);
        }
        read_validated_page_payload(
            self.bytes,
            RowId {
                page_id: 0,
                slot_id: slot_id as u16,
            },
        )
    }
}

/// Patch one live heap slot directly in the authoritative raw page.
/// Every range is preflighted before the first byte is changed.
fn patch_heap_page_raw(
    bytes: &mut [u8],
    slot_id: u16,
    patches: &[(usize, &[u8])],
) -> Result<(), QueryError> {
    validate_heap_page_structure(bytes)?;
    let slot_count = read_u16(bytes, 20) as usize;
    let idx = slot_id as usize;
    if idx >= slot_count {
        return Err(error(
            crate::error::catalog::ER_KEY_NOT_FOUND,
            "row not found",
        ));
    }
    let base = PAGE_HEADER_SIZE + idx * SLOT_SIZE;
    let offset = read_u16(bytes, base) as usize;
    let len = read_u16(bytes, base + 2) as usize;
    let flags = bytes[base + 4];
    if flags & (ROW_FLAG_DELETED | ROW_FLAG_FREE) != 0 || len == 0 {
        return Err(error(
            crate::error::catalog::ER_KEY_NOT_FOUND,
            "row not found",
        ));
    }
    let slot_end = offset.checked_add(len).ok_or_else(|| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt heap slot bounds",
        )
    })?;
    if slot_end > PAGE_SIZE {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt heap slot bounds",
        ));
    }
    for (patch_offset, patch) in patches {
        let end = patch_offset.checked_add(patch.len()).ok_or_else(|| {
            error(
                crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                "heap patch offset overflow",
            )
        })?;
        if end > len {
            return Err(error(
                crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                "in-place column patch exceeds row payload",
            ));
        }
    }
    for (patch_offset, patch) in patches {
        let start = offset + patch_offset;
        bytes[start..start + patch.len()].copy_from_slice(patch);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HeapUpdatePlan {
    InPlace,
    Append,
    Compact,
    NoSpace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HeapInsertPlan {
    slot_id: u16,
    compact: bool,
}

struct HeapPageMut<'a> {
    bytes: &'a mut [u8],
}

impl<'a> HeapPageMut<'a> {
    fn new(bytes: &'a mut [u8]) -> Result<Self, QueryError> {
        if !validate_heap_page_structure(bytes)? {
            return Err(error(
                crate::error::catalog::ER_KEY_NOT_FOUND,
                "row not found",
            ));
        }
        Ok(Self { bytes })
    }

    fn slot(&self, slot_id: u16) -> Result<Slot, QueryError> {
        let idx = slot_id as usize;
        let slot_count = read_u16(self.bytes, 20) as usize;
        if idx >= slot_count {
            return Err(error(
                crate::error::catalog::ER_KEY_NOT_FOUND,
                "row not found",
            ));
        }
        let base = PAGE_HEADER_SIZE + idx * SLOT_SIZE;
        let slot = Slot {
            offset: read_u16(self.bytes, base),
            len: read_u16(self.bytes, base + 2),
            flags: self.bytes[base + 4],
        };
        if slot.flags & (ROW_FLAG_DELETED | ROW_FLAG_FREE) != 0 || slot.len == 0 {
            return Err(error(
                crate::error::catalog::ER_KEY_NOT_FOUND,
                "row not found",
            ));
        }
        let end = slot.offset as usize + slot.len as usize;
        if end > PAGE_SIZE {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt heap slot bounds",
            ));
        }
        Ok(slot)
    }

    fn plan_update(
        bytes: &[u8],
        slot_id: u16,
        new_len: usize,
    ) -> Result<HeapUpdatePlan, QueryError> {
        if new_len > u16::MAX as usize {
            return Ok(HeapUpdatePlan::NoSpace);
        }
        validate_heap_page_structure(bytes)?;
        let idx = slot_id as usize;
        let count = read_u16(bytes, 20) as usize;
        if idx >= count {
            return Err(error(
                crate::error::catalog::ER_KEY_NOT_FOUND,
                "row not found",
            ));
        }
        let base = PAGE_HEADER_SIZE + idx * SLOT_SIZE;
        let slot = Slot {
            offset: read_u16(bytes, base),
            len: read_u16(bytes, base + 2),
            flags: bytes[base + 4],
        };
        if slot.flags & (ROW_FLAG_DELETED | ROW_FLAG_FREE) != 0 || slot.len == 0 {
            return Err(error(
                crate::error::catalog::ER_KEY_NOT_FOUND,
                "row not found",
            ));
        }
        if new_len <= slot.len as usize {
            return Ok(HeapUpdatePlan::InPlace);
        }
        let free_start = read_u16(bytes, 22) as usize;
        let free_end = read_u16(bytes, 24) as usize;
        if free_end.saturating_sub(free_start) >= new_len {
            return Ok(HeapUpdatePlan::Append);
        }
        let mut live_other = 0usize;
        for slot_idx in 0..count {
            if slot_idx == idx {
                continue;
            }
            let slot_base = PAGE_HEADER_SIZE + slot_idx * SLOT_SIZE;
            let len = read_u16(bytes, slot_base + 2) as usize;
            let offset = read_u16(bytes, slot_base) as usize;
            let flags = bytes[slot_base + 4];
            if flags & (ROW_FLAG_DELETED | ROW_FLAG_FREE) == 0 {
                if offset.checked_add(len).is_none_or(|end| end > PAGE_SIZE) {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt heap slot bounds",
                    ));
                }
                live_other = live_other.checked_add(len).ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt heap live-byte count",
                    )
                })?;
            }
        }
        if free_start
            .checked_add(live_other)
            .and_then(|used| used.checked_add(new_len))
            .is_some_and(|used| used <= PAGE_SIZE)
        {
            Ok(HeapUpdatePlan::Compact)
        } else {
            Ok(HeapUpdatePlan::NoSpace)
        }
    }

    fn plan_insert(
        bytes: &[u8],
        payload_len: usize,
        reserve_bytes: usize,
    ) -> Result<Option<HeapInsertPlan>, QueryError> {
        if payload_len > u16::MAX as usize || !validate_heap_page_structure(bytes)? {
            return Ok(None);
        }
        let count = read_u16(bytes, 20) as usize;
        let free_start = read_u16(bytes, 22) as usize;
        let free_end = read_u16(bytes, 24) as usize;
        let mut reusable = None;
        let mut live_bytes = 0usize;
        for idx in 0..count {
            let base = PAGE_HEADER_SIZE + idx * SLOT_SIZE;
            let offset = read_u16(bytes, base) as usize;
            let len = read_u16(bytes, base + 2) as usize;
            let flags = bytes[base + 4];
            if flags & ROW_FLAG_FREE != 0 && reusable.is_none() {
                reusable = Some(idx);
            }
            if flags & (ROW_FLAG_DELETED | ROW_FLAG_FREE) == 0 {
                if offset.checked_add(len).is_none_or(|end| end > PAGE_SIZE) {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt heap slot bounds",
                    ));
                }
                live_bytes = live_bytes.checked_add(len).ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt heap live-byte count",
                    )
                })?;
            }
        }
        let needs_slot = reusable.is_none();
        let required = payload_len + (needs_slot as usize) * SLOT_SIZE;
        if free_end.saturating_sub(free_start) >= required
            && free_end.saturating_sub(free_start + required) >= reserve_bytes
        {
            return Ok(Some(HeapInsertPlan {
                slot_id: reusable.unwrap_or(count) as u16,
                compact: false,
            }));
        }
        let new_free_start = free_start + (needs_slot as usize) * SLOT_SIZE;
        let used = new_free_start
            .checked_add(live_bytes)
            .and_then(|value| value.checked_add(payload_len));
        if used.is_some_and(|value| value <= PAGE_SIZE.saturating_sub(reserve_bytes)) {
            Ok(Some(HeapInsertPlan {
                slot_id: reusable.unwrap_or(count) as u16,
                compact: true,
            }))
        } else {
            Ok(None)
        }
    }

    fn insert(
        &mut self,
        payload: &[u8],
        plan: HeapInsertPlan,
        scratch: Option<&mut [u8]>,
    ) -> Result<(), QueryError> {
        let count = read_u16(self.bytes, 20) as usize;
        let appending = plan.slot_id as usize == count;
        if plan.slot_id as usize > count {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "heap insert slot preflight diverged",
            ));
        }
        if !appending {
            let base = PAGE_HEADER_SIZE + plan.slot_id as usize * SLOT_SIZE;
            if self.bytes[base + 4] & ROW_FLAG_FREE == 0 {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "heap insert slot preflight diverged",
                ));
            }
        }
        if plan.compact {
            let scratch = scratch.ok_or_else(|| {
                error(
                    crate::error::catalog::ER_OUTOFMEMORY,
                    "heap compaction scratch unavailable",
                )
            })?;
            if scratch.len() != PAGE_SIZE {
                return Err(error(
                    crate::error::catalog::ER_OUTOFMEMORY,
                    "heap compaction scratch has invalid size",
                ));
            }
            scratch.fill(0);
            let old_free_start = read_u16(self.bytes, 22) as usize;
            let new_free_start = old_free_start + (appending as usize) * SLOT_SIZE;
            scratch[..old_free_start].copy_from_slice(&self.bytes[..old_free_start]);
            let mut free_end = PAGE_SIZE;
            for idx in 0..count {
                let base = PAGE_HEADER_SIZE + idx * SLOT_SIZE;
                let old = Slot {
                    offset: read_u16(self.bytes, base),
                    len: read_u16(self.bytes, base + 2),
                    flags: self.bytes[base + 4],
                };
                if old.flags & (ROW_FLAG_DELETED | ROW_FLAG_FREE) != 0 {
                    continue;
                }
                let body = &self.bytes[old.offset as usize..old.offset as usize + old.len as usize];
                free_end -= body.len();
                scratch[free_end..free_end + body.len()].copy_from_slice(body);
                write_u16(scratch, base, free_end as u16);
            }
            free_end -= payload.len();
            scratch[free_end..free_end + payload.len()].copy_from_slice(payload);
            let base = PAGE_HEADER_SIZE + plan.slot_id as usize * SLOT_SIZE;
            write_u16(scratch, base, free_end as u16);
            write_u16(scratch, base + 2, payload.len() as u16);
            scratch[base + 4] = 0;
            if appending {
                write_u16(scratch, 20, (count + 1) as u16);
            }
            write_u16(scratch, 22, new_free_start as u16);
            write_u16(scratch, 24, free_end as u16);
            self.bytes.copy_from_slice(scratch);
        } else {
            let free_end = read_u16(self.bytes, 24) as usize;
            let new_offset = free_end - payload.len();
            self.bytes[new_offset..free_end].copy_from_slice(payload);
            let base = PAGE_HEADER_SIZE + plan.slot_id as usize * SLOT_SIZE;
            write_u16(self.bytes, base, new_offset as u16);
            write_u16(self.bytes, base + 2, payload.len() as u16);
            self.bytes[base + 4] = 0;
            write_u16(self.bytes, 24, new_offset as u16);
            if appending {
                write_u16(self.bytes, 20, (count + 1) as u16);
                write_u16(
                    self.bytes,
                    22,
                    (PAGE_HEADER_SIZE + (count + 1) * SLOT_SIZE) as u16,
                );
            }
        }
        Ok(())
    }

    fn update(
        &mut self,
        slot_id: u16,
        payload: &[u8],
        plan: HeapUpdatePlan,
        scratch: Option<&mut [u8]>,
    ) -> Result<(), QueryError> {
        let slot = self.slot(slot_id)?;
        let slot_base = PAGE_HEADER_SIZE + slot_id as usize * SLOT_SIZE;
        match plan {
            HeapUpdatePlan::InPlace => {
                let start = slot.offset as usize;
                self.bytes[start..start + payload.len()].copy_from_slice(payload);
                self.bytes[start + payload.len()..start + slot.len as usize].fill(0);
                write_u16(self.bytes, slot_base + 2, payload.len() as u16);
            }
            HeapUpdatePlan::Append => {
                let free_end = read_u16(self.bytes, 24) as usize;
                let new_offset = free_end - payload.len();
                self.bytes[new_offset..free_end].copy_from_slice(payload);
                self.bytes[slot.offset as usize..slot.offset as usize + slot.len as usize].fill(0);
                write_u16(self.bytes, slot_base, new_offset as u16);
                write_u16(self.bytes, slot_base + 2, payload.len() as u16);
                write_u16(self.bytes, 24, new_offset as u16);
            }
            HeapUpdatePlan::Compact => {
                let scratch = scratch.ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_OUTOFMEMORY,
                        "heap compaction scratch unavailable",
                    )
                })?;
                if scratch.len() != PAGE_SIZE {
                    return Err(error(
                        crate::error::catalog::ER_OUTOFMEMORY,
                        "heap compaction scratch has invalid size",
                    ));
                }
                scratch.fill(0);
                let free_start = read_u16(self.bytes, 22) as usize;
                scratch[..free_start].copy_from_slice(&self.bytes[..free_start]);
                let count = read_u16(self.bytes, 20) as usize;
                let mut free_end = PAGE_SIZE;
                for idx in 0..count {
                    let base = PAGE_HEADER_SIZE + idx * SLOT_SIZE;
                    let old = Slot {
                        offset: read_u16(self.bytes, base),
                        len: read_u16(self.bytes, base + 2),
                        flags: self.bytes[base + 4],
                    };
                    if old.flags & (ROW_FLAG_DELETED | ROW_FLAG_FREE) != 0 {
                        continue;
                    }
                    let body = if idx == slot_id as usize {
                        payload
                    } else {
                        &self.bytes[old.offset as usize..old.offset as usize + old.len as usize]
                    };
                    free_end -= body.len();
                    scratch[free_end..free_end + body.len()].copy_from_slice(body);
                    write_u16(scratch, base, free_end as u16);
                    write_u16(scratch, base + 2, body.len() as u16);
                }
                write_u16(scratch, 24, free_end as u16);
                self.bytes.copy_from_slice(scratch);
            }
            HeapUpdatePlan::NoSpace => {
                return Err(error(
                    crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                    "updated row does not fit in heap page",
                ));
            }
        }
        Ok(())
    }

    fn delete(&mut self, slot_id: u16) -> Result<(), QueryError> {
        self.slot(slot_id)?;
        let base = PAGE_HEADER_SIZE + slot_id as usize * SLOT_SIZE;
        self.bytes[base + 4] |= ROW_FLAG_DELETED;
        Ok(())
    }
}

enum HeapPageRead<'a> {
    Raw(Option<HeapPageView<'a>>),
}

impl HeapPageRead<'_> {
    fn slot_count(&self) -> usize {
        match self {
            Self::Raw(Some(page)) => page.slot_count,
            Self::Raw(None) => 0,
        }
    }

    fn payload(&self, slot_id: usize) -> Result<Option<&[u8]>, QueryError> {
        match self {
            Self::Raw(Some(page)) => page.payload(slot_id),
            Self::Raw(None) => Ok(None),
        }
    }
}

fn validate_heap_page_header(buf: &[u8]) -> Result<bool, QueryError> {
    validate_heap_page_header_with_cache(buf, 0, None)
}

fn validate_heap_page_header_with_cache(
    buf: &[u8],
    page_id: u64,
    verified_checksums: Option<&VerifiedDirectHeapChecksums>,
) -> Result<bool, QueryError> {
    let present = validate_heap_page_structure(buf)?;
    if !present {
        return Ok(false);
    }
    let stored_checksum = read_u32(buf, 16);
    if verified_checksums.is_some_and(|cache| cache.contains(page_id, stored_checksum)) {
        return Ok(true);
    }
    let actual_checksum = page_checksum(buf, 16);
    if stored_checksum != actual_checksum {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt heap page checksum",
        ));
    }
    if let Some(cache) = verified_checksums {
        cache.remember(page_id, stored_checksum);
    }
    Ok(true)
}

fn validate_heap_page_structure(buf: &[u8]) -> Result<bool, QueryError> {
    if buf.len() != PAGE_SIZE {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt heap page: short page",
        ));
    }
    if buf.iter().all(|byte| *byte == 0) {
        return Ok(false);
    }
    if read_u32(buf, 0) == HEAP_META_MAGIC {
        decode_heap_meta(buf, &buf[40..56].try_into().unwrap())?;
        return Ok(false);
    }
    if read_u32(buf, 0) != HEAP_PAGE_MAGIC {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt heap page: invalid magic",
        ));
    }
    if read_u16(buf, 4) != HEAP_PAGE_VERSION {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "unsupported heap page version",
        ));
    }
    let slot_count = read_u16(buf, 20) as usize;
    let free_start = read_u16(buf, 22);
    let free_end = read_u16(buf, 24);
    if free_start as usize > PAGE_SIZE || free_end as usize > PAGE_SIZE || free_start > free_end {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt heap page header",
        ));
    }
    if PAGE_HEADER_SIZE + slot_count * SLOT_SIZE > free_start as usize {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt heap slot directory",
        ));
    }
    Ok(true)
}

fn read_validated_page_payload(buf: &[u8], row_id: RowId) -> Result<Option<&[u8]>, QueryError> {
    let slot_count = read_u16(buf, 20) as usize;
    let slot_id = row_id.slot_id as usize;
    if slot_id >= slot_count {
        return Ok(None);
    }
    let base = PAGE_HEADER_SIZE + slot_id * SLOT_SIZE;
    let slot = Slot {
        offset: read_u16(buf, base),
        len: read_u16(buf, base + 2),
        flags: buf[base + 4],
    };
    if slot.flags & (ROW_FLAG_DELETED | ROW_FLAG_FREE) != 0 || slot.len == 0 {
        return Ok(None);
    }
    let end = slot.offset as usize + slot.len as usize;
    if end > PAGE_SIZE {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt heap slot bounds",
        ));
    }
    Ok(Some(&buf[slot.offset as usize..end]))
}

impl Drop for HeapFile {
    fn drop(&mut self) {
        // Dropping a handle is not a graceful database shutdown. The
        // engine's explicit shutdown/checkpoint paths flush dirty data
        // after syncing WAL; a plain drop may be a crash simulation
        // with active transactions, so writing cached pages here can
        // create data files whose page LSN is not covered by durable WAL.
        if self.discard_pages_on_drop {
            self.discard_without_flush();
        }
    }
}

#[cfg(test)]
mod raw_page_view_tests {
    use super::*;

    #[test]
    fn verified_direct_checksum_cache_tracks_checksum_changes() {
        let cache = VerifiedDirectHeapChecksums::try_new(2).expect("tiny derived cache admitted");
        assert!(!cache.contains(0, 41));
        cache.remember(0, 41);
        assert!(cache.contains(0, 41));
        assert!(!cache.contains(0, 42));
        assert!(!cache.contains(2, 41));
    }

    #[test]
    fn buffered_heap_validation_reuses_and_refreshes_direct_checksum_cache() {
        let cache = VerifiedDirectHeapChecksums::try_new(1).expect("tiny derived cache admitted");
        let mut first = Page::new();
        first.insert(b"first").unwrap();
        let first = first.encode();
        let first_checksum = read_u32(&first, 16);
        assert!(!cache.contains(0, first_checksum));
        assert!(validate_heap_page_header_with_cache(&first, 0, Some(&cache)).unwrap());
        assert!(cache.contains(0, first_checksum));

        let mut second = Page::new();
        second.insert(b"a different payload").unwrap();
        let second = second.encode();
        let second_checksum = read_u32(&second, 16);
        assert_ne!(first_checksum, second_checksum);
        assert!(validate_heap_page_header_with_cache(&second, 0, Some(&cache)).unwrap());
        assert!(cache.contains(0, second_checksum));
        assert!(!cache.contains(0, first_checksum));
    }

    #[test]
    fn raw_view_matches_decoded_slots_and_payloads() {
        let mut page = Page::new();
        let first = page.insert(b"first-row").unwrap();
        let second = page.insert(b"second-row").unwrap();
        page.delete(second).unwrap();
        let encoded = page.encode();
        let decoded = Page::decode(&encoded).unwrap();
        let view = HeapPageView::new(&encoded).unwrap().unwrap();
        assert_eq!(view.slot_count, decoded.slots.len());
        assert_eq!(view.payload(first as usize).unwrap(), decoded.read(first));
        assert_eq!(view.payload(second as usize).unwrap(), None);
    }

    #[test]
    fn raw_view_accepts_zero_page_and_rejects_bad_checksum() {
        assert!(HeapPageView::new(&vec![0u8; PAGE_SIZE]).unwrap().is_none());
        let mut page = Page::new();
        page.insert(b"row").unwrap();
        let mut encoded = page.encode();
        encoded[PAGE_SIZE - 1] ^= 1;
        let err = HeapPageView::new(&encoded).unwrap_err();
        assert!(err.message().contains("checksum"));
    }
}

enum HeapPageScan<'a> {
    Full(std::ops::Range<u64>),
    Sampled(std::iter::Copied<std::slice::Iter<'a, u64>>),
    /// Contiguous page-id range `[lo, hi)`. Used by parallel sequential
    /// scan to assign disjoint partitions to each worker.
    Range(std::ops::Range<u64>),
}

impl<'a> HeapPageScan<'a> {
    fn new(page_count: u64, page_ids: Option<&'a [u64]>) -> Self {
        match page_ids {
            Some(page_ids) => Self::Sampled(page_ids.iter().copied()),
            None => Self::Full(0..page_count),
        }
    }

    fn from_range(page_count: u64, page_lo: u64, page_hi: u64) -> Self {
        let lo = page_lo.min(page_count);
        let hi = page_hi.min(page_count);
        Self::Range(lo..hi.max(lo))
    }
}

impl Iterator for HeapPageScan<'_> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Full(page_ids) => page_ids.next(),
            Self::Sampled(page_ids) => page_ids.next(),
            Self::Range(page_ids) => page_ids.next(),
        }
    }
}

#[derive(Debug, Clone)]
struct Page {
    free_start: u16,
    free_end: u16,
    slots: Vec<Slot>,
    data: Vec<u8>,
    /// Hint: count of slots whose `ROW_FLAG_DELETED` bit is set. Tracked
    /// inline by `delete_slot` / `insert_with_reserve` so the
    /// append-heavy LOAD DATA hot path can skip an O(n) slot scan on
    /// every insert. On a freshly created page (LOAD DATA's common
    /// case) the counter stays at zero and the hot path skips both the
    /// "find a tombstone to reuse" scan and the "any deleted slot?"
    /// double-check before falling through to a fresh page.
    /// `decode_slots` rebuilds the counter when reading a page from
    /// disk; out-of-sync states would only cost performance (we'd fall
    /// back to a scan), never correctness.
    deleted_count: u16,
}

/// On-disk slot directory entry. 6 bytes after the rowid-6bytes
/// refactor — see the `RowId` doc-comment in `storage/core.rs` for
/// the rationale behind dropping the per-slot `generation` field and
/// the state-machine that replaces it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Slot {
    offset: u16,
    len: u16,
    flags: u8,
}

impl Page {
    fn new() -> Self {
        Self {
            free_start: PAGE_HEADER_SIZE as u16,
            free_end: PAGE_SIZE as u16,
            slots: Vec::new(),
            data: vec![0; PAGE_SIZE],
            deleted_count: 0,
        }
    }

    fn decode_slots(buf: &[u8]) -> Result<Option<Vec<Slot>>, QueryError> {
        if buf.len() == PAGE_SIZE && read_u32(buf, 0) == HEAP_META_MAGIC {
            decode_heap_meta(buf, &buf[40..56].try_into().unwrap())?;
            return Ok(None);
        }
        if buf.iter().all(|byte| *byte == 0) {
            return Ok(None);
        }
        if read_u32(buf, 0) != HEAP_PAGE_MAGIC {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt heap page: invalid magic",
            ));
        }
        if read_u16(buf, 4) != HEAP_PAGE_VERSION {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "unsupported heap page version",
            ));
        }
        let stored_checksum = read_u32(buf, 16);
        let actual_checksum = page_checksum(buf, 16);
        if stored_checksum != actual_checksum {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt heap page checksum",
            ));
        }
        let slot_count = read_u16(buf, 20) as usize;
        let free_start = read_u16(buf, 22);
        let free_end = read_u16(buf, 24);
        if free_start as usize > PAGE_SIZE || free_end as usize > PAGE_SIZE || free_start > free_end
        {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt heap page header",
            ));
        }
        if PAGE_HEADER_SIZE + slot_count * SLOT_SIZE > free_start as usize {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt heap slot directory",
            ));
        }
        let mut slots = Vec::with_capacity(slot_count);
        for idx in 0..slot_count {
            let base = PAGE_HEADER_SIZE + idx * SLOT_SIZE;
            let slot = Slot {
                offset: read_u16(buf, base),
                len: read_u16(buf, base + 2),
                flags: buf[base + 4],
            };
            if slot.flags & (ROW_FLAG_DELETED | ROW_FLAG_FREE) == 0 && slot.len > 0 {
                let end = slot.offset as usize + slot.len as usize;
                if end > PAGE_SIZE {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt heap slot bounds",
                    ));
                }
            }
            slots.push(slot);
        }
        Ok(Some(slots))
    }

    fn decode(buf: &[u8]) -> Result<Self, QueryError> {
        if buf.iter().all(|byte| *byte == 0) {
            return Ok(Self::new());
        }
        if read_u32(buf, 0) != HEAP_PAGE_MAGIC {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt heap page: invalid magic",
            ));
        }
        if read_u16(buf, 4) != HEAP_PAGE_VERSION {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "unsupported heap page version",
            ));
        }
        let stored_checksum = read_u32(buf, 16);
        let actual_checksum = page_checksum(buf, 16);
        if stored_checksum != actual_checksum {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt heap page checksum",
            ));
        }
        let slot_count = read_u16(buf, 20) as usize;
        let free_start = read_u16(buf, 22);
        let free_end = read_u16(buf, 24);
        if free_start as usize > PAGE_SIZE || free_end as usize > PAGE_SIZE || free_start > free_end
        {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt heap page header",
            ));
        }
        if PAGE_HEADER_SIZE + slot_count * SLOT_SIZE > free_start as usize {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt heap slot directory",
            ));
        }
        let mut slots = Vec::new();
        let mut deleted_count: u16 = 0;
        for idx in 0..slot_count {
            let base = PAGE_HEADER_SIZE + idx * SLOT_SIZE;
            let slot = Slot {
                offset: read_u16(buf, base),
                len: read_u16(buf, base + 2),
                flags: buf[base + 4],
            };
            if slot.flags & (ROW_FLAG_DELETED | ROW_FLAG_FREE) == 0 && slot.len > 0 {
                let end = slot.offset as usize + slot.len as usize;
                if end > PAGE_SIZE {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt heap slot bounds",
                    ));
                }
            } else if slot.flags & ROW_FLAG_DELETED != 0 {
                deleted_count = deleted_count.saturating_add(1);
            }
            slots.push(slot);
        }
        Ok(Self {
            free_start,
            free_end,
            slots,
            data: buf.to_vec(),
            deleted_count,
        })
    }

    fn encode(&self) -> Vec<u8> {
        let mut buf = self.data.clone();
        self.write_header_into(&mut buf);
        let checksum = page_checksum(&buf, 16);
        write_u32(&mut buf, 16, checksum);
        buf
    }

    /// Consuming variant: encode by mutating the page's own `data`
    /// buffer (which is already PAGE_SIZE bytes) in place and returning
    /// it. Saves the 16 KB `self.data.clone()` that `encode` does for
    /// every page on the LOAD DATA / bulk-insert hot path. Equivalent
    /// output to `encode`. The page is unusable afterwards because its
    /// `data` Vec has been moved out.
    fn encode_into_owned(self) -> Vec<u8> {
        self.encode_into_owned_with_flags(0)
    }

    fn encode_into_owned_with_flags(mut self, flags: u16) -> Vec<u8> {
        let lsn = page_lsn(&self.data);
        let mut buf = std::mem::take(&mut self.data);
        debug_assert_eq!(buf.len(), PAGE_SIZE);
        self.write_header_into_with_lsn(&mut buf, lsn);
        write_u16(&mut buf, 6, flags);
        let checksum = page_checksum(&buf, 16);
        write_u32(&mut buf, 16, checksum);
        buf
    }

    fn write_header_into(&self, buf: &mut [u8]) {
        self.write_header_into_with_lsn(buf, page_lsn(&self.data));
    }

    fn write_header_into_with_lsn(&self, buf: &mut [u8], lsn: u64) {
        write_u32(buf, 0, HEAP_PAGE_MAGIC);
        write_u16(buf, 4, HEAP_PAGE_VERSION);
        write_u16(buf, 6, 0);
        write_u64(buf, 8, lsn);
        write_u32(buf, 16, 0);
        write_u16(buf, 20, self.slots.len() as u16);
        write_u16(buf, 22, self.free_start);
        write_u16(buf, 24, self.free_end);
        for (idx, slot) in self.slots.iter().enumerate() {
            let base = PAGE_HEADER_SIZE + idx * SLOT_SIZE;
            write_u16(buf, base, slot.offset);
            write_u16(buf, base + 2, slot.len);
            buf[base + 4] = slot.flags;
            // v10: trailing 1 B padding removed (SLOT_SIZE 6 → 5).
        }
    }

    fn set_lsn(&mut self, lsn: u64) {
        write_u64(&mut self.data, 8, lsn);
    }

    fn insert(&mut self, payload: &[u8]) -> Option<u16> {
        self.insert_with_reserve(payload, 0)
    }

    fn insert_with_reserve(&mut self, payload: &[u8], reserve_bytes: usize) -> Option<u16> {
        // Post rowid-6bytes refactor: only slots marked `ROW_FLAG_FREE`
        // are reusable. `ROW_FLAG_DELETED` slots are tombstones that
        // still hold the deleted row payload for MVCC visibility and
        // may have dangling secondary-index entries; reusing them
        // without the index-purge / slot-reclaim pass would let stale
        // index entries mis-point at a new row. M1 never produces
        // FREE slots, so this path always appends fresh — M2 will
        // wire up the reclaim mechanism.
        let reusable = self
            .slots
            .iter()
            .position(|slot| slot.flags & ROW_FLAG_FREE != 0);
        self.insert_with_reusable(payload, reserve_bytes, reusable)
    }

    /// Append into a page that was created by the current batch and therefore
    /// cannot contain reclaimable slots.  The generic insert path must search
    /// the slot directory for `ROW_FLAG_FREE`; repeating that linear scan for
    /// every row makes a fresh-page bulk append quadratic in the number of
    /// slots per page.  Keep the assertion in debug builds so this shortcut
    /// can never silently be used for an existing/recovered page.
    fn insert_append_only_with_reserve(
        &mut self,
        payload: &[u8],
        reserve_bytes: usize,
    ) -> Option<u16> {
        debug_assert!(self
            .slots
            .iter()
            .all(|slot| slot.flags & ROW_FLAG_FREE == 0));
        self.insert_with_reusable(payload, reserve_bytes, None)
    }

    fn insert_with_reusable(
        &mut self,
        payload: &[u8],
        reserve_bytes: usize,
        reusable: Option<usize>,
    ) -> Option<u16> {
        let needs_slot = reusable.is_none();
        let slot_bytes = if needs_slot { SLOT_SIZE } else { 0 };
        let required = payload.len() + slot_bytes;
        if self.free_space() < required
            || self.free_space().saturating_sub(required) < reserve_bytes
        {
            // `compact` reclaims space only when deleted slots have
            // fragmented the page. On an append-heavy path (LOAD DATA,
            // bulk INSERT) every page hits its high-water mark with
            // zero deleted slots, so an unguarded `compact()` call
            // here re-allocates and memcpy's the entire 16 KB page on
            // every page rollover. The samply profile for sf=1
            // lineitem LOAD DATA showed `Page::compact` at 17.7% of
            // writer-thread CPU even though no row had been deleted.
            // Skip compact when there's nothing to reclaim and let the
            // caller open a fresh page instead. A reusable slot is FREE:
            // reclaim left its old bytes behind, so compaction can help.
            if self.deleted_count == 0 && reusable.is_none() {
                return None;
            }
            self.compact();
            if self.free_space() < required
                || self.free_space().saturating_sub(required) < reserve_bytes
            {
                return None;
            }
        }
        self.free_end -= payload.len() as u16;
        let offset = self.free_end;
        self.data[offset as usize..offset as usize + payload.len()].copy_from_slice(payload);
        if let Some(slot_id) = reusable {
            self.slots[slot_id] = Slot {
                offset,
                len: payload.len() as u16,
                flags: 0,
            };
            Some(slot_id as u16)
        } else {
            self.slots.push(Slot {
                offset,
                len: payload.len() as u16,
                flags: 0,
            });
            self.free_start += SLOT_SIZE as u16;
            Some((self.slots.len() - 1) as u16)
        }
    }

    /// Recovery-only: place `payload` at exactly the absolute `slot_id`,
    /// growing the slot directory with `ROW_FLAG_FREE` placeholders to
    /// fill any gap below it. Returns `None` if the row plus the new
    /// directory entries don't fit (after a reclaim-compact attempt).
    ///
    /// WAL REDO is commit-filtered: a crash-loser transaction's inserts
    /// are skipped during redo and rolled back (no-op) in the undo
    /// phase. But a later committed insert to the SAME heap page recorded
    /// its ABSOLUTE `slot_id` at runtime, when the loser's row still
    /// occupied the slot below it — and the committed insert's
    /// secondary-index entries (also redone at absolute slot ids) point
    /// at that exact slot. `insert` (next-free-slot) would shift the row
    /// down into the hole the skipped loser left, breaking index→heap
    /// consistency and tripping the redo slot-equality check. Replaying
    /// at the recorded absolute slot — leaving the loser's slot a FREE
    /// placeholder — keeps every committed row where its index entry
    /// expects it. A FREE (not DELETED) placeholder is correct: the
    /// loser is aborted, it owns no live snapshot, and its index entry
    /// was likewise commit-filtered out, so the slot carries no dangling
    /// reference and may be reused later.
    fn insert_at_slot(&mut self, slot_id: u16, payload: &[u8]) -> Option<()> {
        let target = slot_id as usize;
        // Directory entries we must materialize: everything from the
        // current slot count up to and including `target` that doesn't
        // exist yet. An already-present slot at `target` (FREE/DELETED
        // tombstone) is overwritten in place and costs no new entry.
        let new_dir_slots = (target + 1).saturating_sub(self.slots.len());
        let required = payload.len() + new_dir_slots * SLOT_SIZE;
        if self.free_space() < required {
            // Always try compaction here, unlike the runtime insert path:
            // commit-filtered REDO does not reproduce the runtime page layout.
            // A skipped aborted UPDATE can turn the grow-and-compact its
            // txn-0 rollback did at runtime into an in-place shrink, so bytes
            // a committed insert reused at runtime are still unreclaimed
            // fragments here (as are DELETED/FREE slot bodies).
            self.compact();
            if self.free_space() < required {
                return None;
            }
        }
        // Pad the gap below `target` with FREE placeholders.
        while self.slots.len() < target {
            self.slots.push(Slot {
                offset: 0,
                len: 0,
                flags: ROW_FLAG_FREE,
            });
            self.free_start += SLOT_SIZE as u16;
        }
        self.free_end -= payload.len() as u16;
        let offset = self.free_end;
        self.data[offset as usize..offset as usize + payload.len()].copy_from_slice(payload);
        let new_slot = Slot {
            offset,
            len: payload.len() as u16,
            flags: 0,
        };
        if target < self.slots.len() {
            if self.slots[target].flags & ROW_FLAG_DELETED != 0 {
                self.deleted_count = self.deleted_count.saturating_sub(1);
            }
            self.slots[target] = new_slot;
        } else {
            // target == self.slots.len(): append the final entry.
            self.slots.push(new_slot);
            self.free_start += SLOT_SIZE as u16;
        }
        Some(())
    }

    fn read(&self, slot_id: u16) -> Option<&[u8]> {
        let slot = self.slots.get(slot_id as usize)?;
        if slot.flags & (ROW_FLAG_DELETED | ROW_FLAG_FREE) != 0 {
            return None;
        }
        Some(&self.data[slot.offset as usize..slot.offset as usize + slot.len as usize])
    }

    fn update(&mut self, slot_id: u16, payload: &[u8]) -> Result<(), QueryError> {
        if self.try_update(slot_id, payload)?.is_some() {
            return Ok(());
        }
        Err(error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "updated row does not fit in heap page",
        ))
    }

    fn try_update(&mut self, slot_id: u16, payload: &[u8]) -> Result<Option<()>, QueryError> {
        let idx = slot_id as usize;
        let Some(slot) = self.slots.get(idx).copied() else {
            return Err(error(
                crate::error::catalog::ER_KEY_NOT_FOUND,
                "row not found",
            ));
        };
        if slot.flags & (ROW_FLAG_DELETED | ROW_FLAG_FREE) != 0 {
            return Err(error(
                crate::error::catalog::ER_KEY_NOT_FOUND,
                "row not found",
            ));
        }
        if payload.len() <= slot.len as usize {
            self.data[slot.offset as usize..slot.offset as usize + payload.len()]
                .copy_from_slice(payload);
            self.slots[idx].len = payload.len() as u16;
            return Ok(Some(()));
        }
        let old_payload =
            self.data[slot.offset as usize..slot.offset as usize + slot.len as usize].to_vec();
        self.slots[idx].flags |= ROW_FLAG_DELETED;
        self.deleted_count = self.deleted_count.saturating_add(1);
        self.compact();
        // After compact() the deleted slot's bytes are gone from self.data, so
        // the new payload needs the full payload.len() of free space — not
        // just (payload.len() - slot.len). Using `extra` here used to allow
        // the subsequent `free_end -= payload.len()` to underflow when the
        // page is tightly packed.
        if self.free_space() < payload.len() {
            self.free_end -= old_payload.len() as u16;
            let offset = self.free_end;
            self.data[offset as usize..offset as usize + old_payload.len()]
                .copy_from_slice(&old_payload);
            // Restoring the original (non-deleted) slot means undoing
            // the bump we did above before `compact()`.
            self.slots[idx] = Slot { offset, ..slot };
            self.deleted_count = self.deleted_count.saturating_sub(1);
            return Ok(None);
        }
        self.free_end -= payload.len() as u16;
        let offset = self.free_end;
        self.data[offset as usize..offset as usize + payload.len()].copy_from_slice(payload);
        // The new payload reactivates the slot (flags = 0) — undo the
        // delete bump from the start of this branch.
        self.slots[idx] = Slot {
            offset,
            len: payload.len() as u16,
            flags: 0,
        };
        self.deleted_count = self.deleted_count.saturating_sub(1);
        Ok(Some(()))
    }

    fn delete(&mut self, slot_id: u16) -> Result<(), QueryError> {
        let Some(slot) = self.slots.get_mut(slot_id as usize) else {
            return Err(error(
                crate::error::catalog::ER_KEY_NOT_FOUND,
                "row not found",
            ));
        };
        if slot.flags & ROW_FLAG_FREE != 0 {
            return Err(error(
                crate::error::catalog::ER_KEY_NOT_FOUND,
                "row not found",
            ));
        }
        if slot.flags & ROW_FLAG_DELETED == 0 {
            slot.flags |= ROW_FLAG_DELETED;
            self.deleted_count = self.deleted_count.saturating_add(1);
        }
        Ok(())
    }

    /// rowid-6bytes M2 — idempotent reclaim used by the recovery /
    /// runtime-apply paths. Returns `true` if the slot transitioned
    /// `tombstone → FREE` (caller writes the page back), `false` if
    /// the slot was already FREE, points at a live row, holds a
    /// forwarding stub, or is out-of-range. Hiding the state
    /// inspection behind one helper keeps recovery from having to
    /// know the `ROW_FLAG_*` / `MVCC_FLAG_*` bit values directly.
    fn try_reclaim_deleted_for_recovery(&mut self, slot_id: u16) -> bool {
        if !self.is_slot_mvcc_tombstone(slot_id) {
            // Tolerate the case where the slot was already physically
            // marked DELETED by a non-MVCC path (DDL rollback,
            // recovery) — still reclaim it.
            let already_marked = self
                .slots
                .get(slot_id as usize)
                .is_some_and(|slot| slot.flags & ROW_FLAG_DELETED != 0);
            if !already_marked {
                return false;
            }
        }
        self.mark_slot_free(slot_id).is_ok()
    }

    /// rowid-6bytes M2 — true when slot `slot_id` is allocated,
    /// points at an MVCC row payload (not a forwarding stub), and that
    /// payload's MVCC header has `MVCC_FLAG_DELETED` set. False for
    /// every other slot state (FREE, out-of-range, live row, migrated
    /// stub, payload with version mismatch). Used by reclaim to
    /// decide whether flipping the slot to FREE is safe.
    fn is_slot_mvcc_tombstone(&self, slot_id: u16) -> bool {
        let Some(slot) = self.slots.get(slot_id as usize).copied() else {
            return false;
        };
        if slot.flags & ROW_FLAG_FREE != 0 || slot.len == 0 {
            return false;
        }
        let start = slot.offset as usize;
        let end = start + slot.len as usize;
        if end > self.data.len() || slot.len < 2 {
            return false;
        }
        let payload = &self.data[start..end];
        // First byte is the row format version. Forwarding stubs use
        // `HEAP_FORWARDING_STUB_VERSION` and must not be reclaimed —
        // they point at a migrated piece on another page.
        if payload[0] != MVCC_ROW_VERSION {
            return false;
        }
        let raw_flags = payload[1];
        raw_flags & MVCC_FLAG_DELETED != 0
    }

    /// rowid-6bytes M2 — flip a reclaimable slot to `ROW_FLAG_FREE`.
    /// Zeros the payload pointer so `read`
    /// can't read the dead row body even on a corrupt re-decode. The
    /// caller is responsible for proving reclaim safety (no live
    /// snapshot, no secondary-index entry).
    fn mark_slot_free(&mut self, slot_id: u16) -> Result<(), QueryError> {
        let Some(slot) = self.slots.get_mut(slot_id as usize) else {
            return Err(error(
                crate::error::catalog::ER_KEY_NOT_FOUND,
                "row not found",
            ));
        };
        if slot.flags & ROW_FLAG_FREE != 0 {
            return Ok(());
        }
        let was_deleted = slot.flags & ROW_FLAG_DELETED != 0;
        slot.flags = ROW_FLAG_FREE;
        slot.len = 0;
        if was_deleted {
            self.deleted_count = self.deleted_count.saturating_sub(1);
        }
        Ok(())
    }

    // The earlier `reclaim_deleted` helper required the slot to
    // already carry `ROW_FLAG_DELETED`; M2's MVCC-aware reclaim
    // (`mark_slot_free` above) covers both DELETED-physical slots and
    // MVCC-tombstoned (slot.flags == 0, row header
    // `MVCC_FLAG_DELETED`) slots, so the narrower helper is gone.

    /// In-place patch a subset of byte ranges inside a row's payload.
    /// Used by the Phase 2 column-level UPDATE path: rather than
    /// re-encoding the whole row and going through `try_update`, the
    /// caller pins the slot, computes per-column (offset, new_bytes)
    /// patches via [`fixed_column_byte_range_in_payload`], and asks
    /// the page to overwrite those bytes in place. The slot's `len`
    /// is unchanged — the RowId stays stable, secondary indexes that
    /// point at this row do not need to re-resolve.
    ///
    /// Returns the captured before-image bytes for each patch (same
    /// order as `patches`) so the caller can persist them in an
    /// `UndoColDelta` for rollback / MVCC.
    ///
    /// Errors:
    ///   * `row not found` — slot deleted or freed
    ///   * `42000 updated row does not fit` — patch range exceeds slot.len
    ///     (shouldn't happen if the caller used the row_codec helper)
    fn try_patch_columns(
        &mut self,
        slot_id: u16,
        patches: &[(usize, Vec<u8>)],
    ) -> Result<Vec<Vec<u8>>, QueryError> {
        let idx = slot_id as usize;
        let Some(slot) = self.slots.get(idx).copied() else {
            return Err(error(
                crate::error::catalog::ER_KEY_NOT_FOUND,
                "row not found",
            ));
        };
        if slot.flags & (ROW_FLAG_DELETED | ROW_FLAG_FREE) != 0 {
            return Err(error(
                crate::error::catalog::ER_KEY_NOT_FOUND,
                "row not found",
            ));
        }
        // Pre-validate every patch range; we want all-or-nothing
        // semantics so a half-applied patch can never corrupt a row.
        for (offset, bytes) in patches {
            let end = offset.checked_add(bytes.len()).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                    "patch offset overflow",
                )
            })?;
            if end > slot.len as usize {
                return Err(error(
                    crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                    "in-place column patch exceeds row payload",
                ));
            }
        }
        let mut before_images = Vec::with_capacity(patches.len());
        for (offset, bytes) in patches {
            let start = slot.offset as usize + offset;
            let end = start + bytes.len();
            // Capture old bytes first so undo gets the actual before-image.
            before_images.push(self.data[start..end].to_vec());
            self.data[start..end].copy_from_slice(bytes);
        }
        Ok(before_images)
    }

    #[cfg(test)]
    fn slot(&self, slot_id: u16) -> Slot {
        self.slots[slot_id as usize]
    }

    fn free_space(&self) -> usize {
        self.free_end.saturating_sub(self.free_start) as usize
    }

    fn compact(&mut self) {
        let mut rows = Vec::new();
        for (idx, slot) in self.slots.iter().enumerate() {
            if slot.flags & (ROW_FLAG_DELETED | ROW_FLAG_FREE) == 0 && slot.len > 0 {
                rows.push((
                    idx,
                    *slot,
                    self.data[slot.offset as usize..slot.offset as usize + slot.len as usize]
                        .to_vec(),
                ));
            }
        }
        let mut data = vec![0; PAGE_SIZE];
        // Compaction moves row bodies, not the page's WAL identity. Preserving
        // the header also distinguishes initialized pages from new zero pages.
        data[..PAGE_HEADER_SIZE].copy_from_slice(&self.data[..PAGE_HEADER_SIZE]);
        let mut free_end = PAGE_SIZE as u16;
        for (idx, slot, payload) in rows {
            free_end -= payload.len() as u16;
            data[free_end as usize..free_end as usize + payload.len()].copy_from_slice(&payload);
            self.slots[idx] = Slot {
                offset: free_end,
                ..slot
            };
        }
        self.data = data;
        self.free_start = (PAGE_HEADER_SIZE + self.slots.len() * SLOT_SIZE) as u16;
        self.free_end = free_end;
    }
}

#[cfg(test)]
mod page_patch_tests {
    use super::*;

    fn page_with_payload(payload: &[u8]) -> (Page, u16) {
        let mut page = Page::new();
        let slot_id = page
            .insert_with_reserve(payload, 0)
            .expect("page has room for the test payload");
        (page, slot_id)
    }

    #[test]
    fn append_only_insert_matches_generic_insert_on_fresh_page() {
        let mut generic = Page::new();
        let mut append_only = Page::new();
        for row in 0..120_u16 {
            let payload = vec![(row & 0xff) as u8; 73 + usize::from(row % 19)];
            assert_eq!(
                generic.insert_with_reserve(&payload, 0),
                append_only.insert_append_only_with_reserve(&payload, 0)
            );
        }
        assert_eq!(generic.slots, append_only.slots);
        assert_eq!(generic.free_start, append_only.free_start);
        assert_eq!(generic.free_end, append_only.free_end);
        assert_eq!(generic.encode(), append_only.encode());
    }

    #[test]
    fn try_patch_columns_overwrites_ranges_and_returns_before_image() {
        let payload = b"ABCDEFGHIJ".to_vec();
        let (mut page, slot_id) = page_with_payload(&payload);

        // Patch bytes 0..3 with "xxx" and 7..9 with "yy".
        let patches = vec![(0usize, b"xxx".to_vec()), (7usize, b"yy".to_vec())];
        let before = page.try_patch_columns(slot_id, &patches).expect("patch");
        assert_eq!(before, vec![b"ABC".to_vec(), b"HI".to_vec()]);

        // Page bytes for that slot should now read "xxxDEFGyyJ".
        let slot = page.slot(slot_id);
        let body = &page.data[slot.offset as usize..slot.offset as usize + slot.len as usize];
        assert_eq!(body, b"xxxDEFGyyJ");
        // Slot's len must be unchanged: in-place patch doesn't
        // invalidate the RowId.
        assert_eq!(slot.len as usize, payload.len());
    }

    #[test]
    fn try_patch_columns_rejects_deleted_slot() {
        let (mut page, slot_id) = page_with_payload(b"ABCD");
        page.delete(slot_id).unwrap();
        let bad = page.try_patch_columns(slot_id, &[(0, b"x".to_vec())]);
        assert!(bad.is_err(), "deleted slot must be rejected");
    }

    #[test]
    fn try_patch_columns_rejects_out_of_bounds_patch() {
        let (mut page, slot_id) = page_with_payload(b"ABCD");
        // Patch range 3..7 is past end of 4-byte payload.
        let bad = page.try_patch_columns(slot_id, &[(3, b"OVERFLOW".to_vec())]);
        assert!(bad.is_err(), "out-of-bounds patch must be rejected");
        // Payload must remain untouched (all-or-nothing semantics).
        let slot = page.slot(slot_id);
        let body = &page.data[slot.offset as usize..slot.offset as usize + slot.len as usize];
        assert_eq!(body, b"ABCD");
    }

    #[test]
    fn try_patch_columns_all_or_nothing_on_partial_overlap_validation() {
        // First patch is valid; second is out of bounds. The first
        // must NOT have applied.
        let (mut page, slot_id) = page_with_payload(b"ABCDEFGH");
        let bad =
            page.try_patch_columns(slot_id, &[(0, b"xx".to_vec()), (7, b"OVERFLOW".to_vec())]);
        assert!(bad.is_err());
        let slot = page.slot(slot_id);
        let body = &page.data[slot.offset as usize..slot.offset as usize + slot.len as usize];
        assert_eq!(body, b"ABCDEFGH");
    }
}

#[cfg(test)]
mod raw_heap_mutation_tests {
    use super::*;

    fn payload(seed: u64, len: usize) -> Vec<u8> {
        (0..len)
            .map(|idx| seed.wrapping_add(idx as u64 * 17) as u8)
            .collect()
    }

    fn assert_matches(bytes: &mut [u8], oracle: &Page) {
        refresh_page_checksum(bytes);
        let decoded = Page::decode(bytes).expect("decode raw heap page");
        assert_eq!(decoded.slots.len(), oracle.slots.len());
        for slot_id in 0..oracle.slots.len() {
            assert_eq!(decoded.slots[slot_id].len, oracle.slots[slot_id].len);
            assert_eq!(decoded.slots[slot_id].flags, oracle.slots[slot_id].flags);
            assert_eq!(
                decoded.read(slot_id as u16),
                oracle.read(slot_id as u16),
                "payload mismatch at slot {slot_id}"
            );
        }
        validate_heap_page_header(bytes).expect("valid heap checksum");
    }

    #[test]
    fn raw_heap_mutator_matches_oracle_for_10k_steps() {
        let mut oracle = Page::new();
        let first = payload(1, 64);
        oracle.insert(&first).expect("initial insert");
        let mut bytes = oracle.encode();
        let mut rng = 0xd1b5_4a32_d192_ed03_u64;
        let mut scratch = vec![0_u8; PAGE_SIZE];

        for step in 0..10_000_u64 {
            rng = rng
                .wrapping_mul(2_862_933_555_777_941_757)
                .wrapping_add(3_037_000_493);
            let live: Vec<u16> = oracle
                .slots
                .iter()
                .enumerate()
                .filter(|(_, slot)| slot.flags & (ROW_FLAG_DELETED | ROW_FLAG_FREE) == 0)
                .map(|(idx, _)| idx as u16)
                .collect();
            match (rng >> 61) as u8 {
                0..=2 if oracle.slots.len() < 256 => {
                    let body = payload(rng, 8 + ((rng >> 8) as usize % 160));
                    let mut candidate = oracle.clone();
                    let expected = candidate.insert_with_reserve(&body, 16);
                    let plan = HeapPageMut::plan_insert(&bytes, body.len(), 16).unwrap();
                    match (expected, plan) {
                        (Some(slot), Some(plan)) => {
                            assert_eq!(slot, plan.slot_id);
                            HeapPageMut::new(&mut bytes)
                                .unwrap()
                                .insert(&body, plan, plan.compact.then_some(scratch.as_mut_slice()))
                                .unwrap();
                            oracle = candidate;
                        }
                        (None, None) => {}
                        _ => {
                            // The raw mutator may reclaim FREE-slot
                            // fragmentation more eagerly than the legacy
                            // oracle. Repack to the common semantic state.
                            bytes = oracle.encode();
                        }
                    }
                }
                3..=5 if !live.is_empty() => {
                    let slot = live[(rng as usize) % live.len()];
                    let body = payload(rng ^ step, 4 + ((rng >> 16) as usize % 256));
                    let mut candidate = oracle.clone();
                    let expected = candidate.try_update(slot, &body).unwrap().is_some();
                    let plan = HeapPageMut::plan_update(&bytes, slot, body.len()).unwrap();
                    assert_eq!(expected, plan != HeapUpdatePlan::NoSpace);
                    if expected {
                        HeapPageMut::new(&mut bytes)
                            .unwrap()
                            .update(
                                slot,
                                &body,
                                plan,
                                (plan == HeapUpdatePlan::Compact).then_some(scratch.as_mut_slice()),
                            )
                            .unwrap();
                        oracle = candidate;
                    }
                }
                _ if live.len() > 1 => {
                    let slot = live[(rng as usize) % live.len()];
                    oracle.delete(slot).unwrap();
                    HeapPageMut::new(&mut bytes).unwrap().delete(slot).unwrap();
                }
                _ => {}
            }
            assert_matches(&mut bytes, &oracle);
        }
    }
}
pub(crate) struct ShadowPreparedHeapBatch {
    encoded_pages: Vec<u8>,
    row_ids: Vec<RowId>,
}
