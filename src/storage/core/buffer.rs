/// Q-1 - per-kind soft-quota percentages from the environment.
/// `0` (or unset where the default is 0) disables the bias for that
/// kind. LOB defaults to 30% — overflow chains are read as one-shot
/// streams, so once they exceed a third of the pool they should recycle
/// among themselves instead of evicting the heap / index working set.
fn kind_quota_pct_from_env() -> [AtomicU64; BUFFER_PAGE_KIND_COUNT] {
    fn pct(name: &str, default: u64) -> u64 {
        env::var(name)
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .map(|v| v.min(100))
            .unwrap_or(default)
    }
    [
        AtomicU64::new(pct("MYDB_BUFFER_POOL_QUOTA_HEAP_PCT", 0)),
        AtomicU64::new(pct("MYDB_BUFFER_POOL_QUOTA_INDEX_PCT", 0)),
        AtomicU64::new(pct("MYDB_BUFFER_POOL_QUOTA_LOB_PCT", 30)),
        AtomicU64::new(pct("MYDB_BUFFER_POOL_QUOTA_VECTOR_PCT", 0)),
        AtomicU64::new(pct("MYDB_BUFFER_POOL_QUOTA_ZONE_MAP_PCT", 0)),
    ]
}

/// Return only complete OS pages inside an exclusively owned allocation.
/// Keep its first page resident: allocators use the beginning of a freed
/// block for their free-list link. Boundary pages may contain other objects.
#[cfg(any(test, target_os = "linux"))]
fn unused_raw_page_span(address: usize, len: usize, page_size: usize) -> std::ops::Range<usize> {
    if page_size == 0 {
        return 0..0;
    }
    let header = std::mem::size_of::<usize>().min(len);
    let start = header + (page_size - (address + header) % page_size) % page_size;
    let bytes = len.saturating_sub(start) / page_size * page_size;
    if bytes == 0 {
        0..0
    } else {
        start..start + bytes
    }
}

#[cfg(target_os = "linux")]
fn discard_unused_raw_page_bytes(bytes: &mut [u8]) {
    static OS_PAGE_SIZE: OnceLock<usize> = OnceLock::new();
    let page_size = *OS_PAGE_SIZE.get_or_init(|| {
        // SAFETY: sysconf takes no pointers and has no side effects.
        usize::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) }).unwrap_or(0)
    });
    let span = unused_raw_page_span(bytes.as_ptr() as usize, bytes.len(), page_size);
    if span.is_empty() {
        return;
    }
    // SAFETY: this mutable slice owns every byte in the page-aligned span;
    // no neighboring allocation or allocator header is included. Its data
    // is dead and the owning Vec is dropped immediately after this call.
    // Discard before deallocation, never after returning memory to the heap.
    // Best effort: a failed advice leaves the ordinary deallocation intact.
    unsafe {
        libc::madvise(
            bytes.as_mut_ptr().add(span.start).cast(),
            span.len(),
            libc::MADV_DONTNEED,
        );
    }
}

fn discard_raw_page_lease(lease: RawPageLease) {
    #[cfg(target_os = "linux")]
    {
        let mut lease = lease;
        discard_unused_raw_page_bytes(&mut lease.bytes);
    }
    #[cfg(not(target_os = "linux"))]
    drop(lease);
}

fn bool_env_enabled(name: &str, default: bool) -> bool {
    env::var(name)
        .map(|value| {
            !matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "0" | "off" | "false" | "no"
            )
        })
        .unwrap_or(default)
}

fn batch_read_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| bool_env_enabled("MYDB_BUFFER_POOL_BATCH_READ", true))
}

fn cold_recycle_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| bool_env_enabled("MYDB_BUFFER_POOL_COLD_RECYCLE", true))
}

/// Read once per process; only consulted when opening a cached data file.
/// Buffered I/O reuses the OS page cache warmed by bulk scans and avoids the
/// extra descriptor and staging copies. Direct I/O remains an explicit override.
#[cfg(target_os = "linux")]
fn data_file_direct_io_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| bool_env_enabled("MYDB_DATA_FILE_DIRECT_IO", false))
}

fn adapted_prefetch_window(
    current: usize,
    issued: u64,
    useful: u64,
    wasted: u64,
    drops: u64,
    demand_waits: u64,
    capacity_cap: usize,
) -> usize {
    if issued == 0 {
        return current.min(capacity_cap.max(1));
    }
    let useful_bp = (useful.saturating_mul(10_000) / issued).min(10_000);
    let wasted_bp = (wasted.saturating_mul(10_000) / issued).min(10_000);
    let drop_bp = (drops.saturating_mul(10_000) / issued.saturating_add(drops).max(1)).min(10_000);
    let next = if useful_bp >= 5_000 && wasted_bp <= 1_000 && drop_bp <= 1_000 && demand_waits != 0
    {
        (current.saturating_mul(5) / 4).max(current.saturating_add(1))
    } else if useful_bp < 2_000 || wasted_bp > 3_000 || drop_bp > 2_500 {
        (current / 2).max(MIN_BUFFER_POOL_PREFETCH_PAGES)
    } else {
        current
    };
    next.clamp(
        MIN_BUFFER_POOL_PREFETCH_PAGES,
        MAX_BUFFER_POOL_PREFETCH_PAGES.min(capacity_cap.max(1)),
    )
}

/// N-2 - resolve an integer percent (0..=100) from `name`, falling back to
/// `default`. Cached in a `OnceLock` by the callers below because the
/// background cleaner reads them every ~25ms per IO worker.
fn env_pct(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(|v| v.min(100))
        .unwrap_or(default)
}

/// N-2 - background page-cleaner high-water mark (percent of capacity). The
/// cleaner starts flushing once dirty exceeds this. Default
/// `BUFFER_POOL_DIRTY_HIGH_PCT`; `MYDB_BUFFER_POOL_DIRTY_HIGH_PCT` overrides.
fn dirty_high_pct() -> u64 {
    static CACHED: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        env_pct(
            "MYDB_BUFFER_POOL_DIRTY_HIGH_PCT",
            BUFFER_POOL_DIRTY_HIGH_PCT,
        )
        .max(1)
    })
}

/// N-2 - background page-cleaner target (percent of capacity). The cleaner
/// trickles dirty down to this floor. Default `BUFFER_POOL_DIRTY_TARGET_PCT`;
/// `MYDB_BUFFER_POOL_DIRTY_TARGET_PCT` overrides. Clamped below the high-water
/// so the two never invert.
fn dirty_target_pct() -> u64 {
    static CACHED: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        let high = dirty_high_pct();
        env_pct(
            "MYDB_BUFFER_POOL_DIRTY_TARGET_PCT",
            BUFFER_POOL_DIRTY_TARGET_PCT,
        )
        .clamp(1, high.saturating_sub(1).max(1))
    })
}

/// P0-2 - background-cleaner write-back cooldown in microseconds. A frame
/// that was flushed and re-dirtied inside this window is a hot-write-set
/// member; the pressure cleaners defer it (eviction / checkpoint will
/// carry it out instead) rather than rewriting it every round. Cached
/// because every IO worker consults it every 25 ms. 0 disables.
fn flush_cooldown_us() -> u64 {
    static CACHED: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        env::var("MYDB_BUFFER_POOL_FLUSH_COOLDOWN_MS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(BUFFER_POOL_FLUSH_COOLDOWN_MS)
            .saturating_mul(1_000)
    })
}

/// The count watermark controls backlog size, but not dirty-page age. A stable
/// OLTP working set can remain below that watermark while its oldest rec_lsn
/// pins every sealed WAL segment. Retire a tiny oldest-LSN batch on every
/// cleaner tick so the recovery floor advances continuously. Cached because
/// every IO worker consults it every 25 ms.
fn age_clean_pages_per_tick() -> u64 {
    static CACHED: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        env::var("MYDB_BUFFER_POOL_AGE_CLEAN_PAGES_PER_TICK")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(BUFFER_POOL_AGE_CLEAN_PAGES_PER_TICK)
            .min(BUFFER_POOL_FLUSH_BATCH_PAGES as u64)
    })
}

fn oldest_dirty_queue_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        env::var("MYDB_BUFFER_POOL_OLDEST_DIRTY_QUEUE")
            .map(|value| !matches!(value.trim(), "0" | "off" | "false" | "no"))
            .unwrap_or(true)
    })
}

/// Phase 0 — bypass the macOS unified buffer cache for buffer-pool data
/// files (`.heap`/`.idx`/`.lob` …). mydb already caches these pages in
/// its own buffer pool, so leaving them in the OS page cache double-buffers
/// them AND — the real problem — lets dirty data-page writes pile up in the
/// unified buffer cache until Darwin flushes them in a bursty writeback. That
/// burst shares the global writeback throttle with the WAL `fsync`, so under
/// sustained OLTP write load a commit's `sync_wal_file` periodically stalls
/// for *seconds* (measured `wal_sync_max_us` ratcheting 34 ms → 5.2 s on
/// `oltp_update_index`), which convoys every concurrent committer and shreds
/// throughput. Setting `F_NOCACHE` makes each page write go straight to the
/// device in the cleaner's small trickle instead of accumulating, decoupling
/// data writeback from WAL durability. Buffer-pool reads that miss still work
/// (they just bypass the redundant OS cache — the 99% hit rate makes this a
/// non-event). macOS only: Linux uses the separate
/// `MYDB_DATA_FILE_DIRECT_IO` switch for `O_DIRECT`. Default off; set
/// `MYDB_DATA_FILE_NOCACHE=1` to enable the unbuffered path on macOS.
#[cfg(target_os = "macos")]
fn data_file_nocache_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        env::var("MYDB_DATA_FILE_NOCACHE")
            .map(|value| !matches!(value.trim(), "0" | "off" | "false" | "no"))
            .unwrap_or(false)
    })
}

fn sync_buffer_file_data(file: &File) -> std::io::Result<()> {
    #[cfg(all(test, target_os = "macos"))]
    if let Some(result) = crate::platform::fs::try_fixture_sync_file(file) {
        return result;
    }
    file.sync_data()
}

/// Apply `F_NOCACHE` to a freshly opened buffer-pool data file on macOS.
/// No-op on other platforms and when disabled via `MYDB_DATA_FILE_NOCACHE=0`.
/// Best-effort: a failed `fcntl` only forfeits the optimization, never
/// correctness (the WAL remains the durability source of truth), so the
/// return code is ignored.
fn apply_data_file_nocache(_file: &File) {
    #[cfg(target_os = "macos")]
    {
        if !data_file_nocache_enabled() {
            return;
        }
        use std::os::unix::io::AsRawFd;
        unsafe {
            libc::fcntl(_file.as_raw_fd(), libc::F_NOCACHE, 1);
        }
    }
}

/// Buffer-pool demand reads and its bounded prefetch batches already select
/// the required pages. Linux's shared per-file sequential predictor confuses
/// interleaved point reads with streams, pulling unrelated pages into the
/// same memory cgroup and evicting useful cached data. Disable speculative
/// kernel readahead on these handles; explicit batched reads still transfer
/// their full contiguous range. Direct scan handles outside this cache retain
/// the kernel's normal sequential policy.
fn apply_buffer_file_read_advice(_file: &File) {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: the borrowed File keeps the descriptor live. This advisory
        // call neither changes file contents nor consumes a userspace pointer.
        // Unsupported advice only loses the optimization.
        unsafe { libc::posix_fadvise(_file.as_raw_fd(), 0, 0, libc::POSIX_FADV_RANDOM) };
        // The governed buffer owns reuse after a read. Do not promote its
        // redundant kernel copy over mmap-backed working sets under pressure.
        // Linux before 6.3 treats NOREUSE as a no-op; unsupported advice is
        // likewise harmless. This never discards dirty or cached file data.
        unsafe { libc::posix_fadvise(_file.as_raw_fd(), 0, 0, libc::POSIX_FADV_NOREUSE) };
    }
}

/// B3 - upper bound on the number of open file handles cached by the buffer
/// pool. The cache is never evicted except on DROP/rename, so a long-lived
/// server that touches many files (rolling partitions, many indexes,
/// `.hvec` vector files) accumulates one held-open fd + `CachedFile`
/// per file forever and eventually trips the `nofile` limit (EMFILE). When
/// the cache grows past this cap a second-chance sweep closes idle handles
/// (clock bit clear AND no pending fsync). The default is deliberately finite;
/// `0` explicitly restores the historical unbounded behaviour.
fn file_cache_max() -> usize {
    static CACHED: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        env::var("MYDB_BUFFER_POOL_FILE_CACHE_MAX")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(1024)
    })
}

/// B2 - when the resident set is ≥ `EVICT_DIRTY_PCT_FOR_PRESCAN_SKIP` dirty,
/// the foreground evictor probes a bounded number of clean CLOCK candidates
/// before asking the IO fleet for a clean victim. If none is available, this
/// knob makes that path honour `EVICT_SYNC_GRACE`, then fall back to a
/// synchronous dirty flush. It is enabled by default: mixed OLTP avoids
/// foreground page writes when possible, while all-dirty loads still make
/// progress after the bounded grace.
/// Set `MYDB_BUFFER_POOL_EVICT_DIRTY_GRACE=off` to restore immediate dirty
/// eviction for unusual storage where waiting is consistently slower.
fn evict_dirty_ratio_grace_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        env::var("MYDB_BUFFER_POOL_EVICT_DIRTY_GRACE")
            .map(|value| !matches!(value.trim(), "0" | "off" | "false" | "no"))
            .unwrap_or(true)
    })
}

/// Owns the loader pin until publication or explicit cleanup. In particular,
/// a later reservation failure must not strand earlier Loading frames.
struct PendingFrameLoad<'a> {
    pool: &'a BufferPool,
    frame: Arc<Frame>,
    owns_pin: bool,
}

impl std::ops::Deref for PendingFrameLoad<'_> {
    type Target = Arc<Frame>;
    fn deref(&self) -> &Self::Target {
        &self.frame
    }
}

impl Drop for PendingFrameLoad<'_> {
    fn drop(&mut self) {
        if self.owns_pin {
            self.frame.publish_io_state(FrameIoState::Discarded);
            self.frame.unpin();
            let _ = self.pool.remove_frame_if_same(&self.frame);
            self.pool.notify_frame_waiters();
        }
    }
}

struct OrderedReadWork {
    keys: Vec<PageKey>,
    runs: Vec<(usize, usize)>,
    next: AtomicUsize,
    state: parking_lot::Mutex<(usize, Option<QueryError>)>,
    done: parking_lot::Condvar,
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

struct RawProbeWork {
    file: Arc<BufferPageFileKey>,
    pages: Vec<(u64, usize, parking_lot::Mutex<Vec<u8>>)>,
    next: AtomicUsize,
    state: parking_lot::Mutex<(usize, Option<QueryError>)>,
    done: parking_lot::Condvar,
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

impl BufferPool {
    const EVICTING_PIN: usize = usize::MAX;

    /// Q-1 - register a resident-page count change for `key`'s file kind.
    #[inline]
    fn note_kind_added(&self, key: &PageKey) {
        self.kind_used_pages[key.file.kind.slot()].fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    fn note_kind_removed(&self, key: &PageKey) {
        let slot = key.file.kind.slot();
        let _ =
            self.kind_used_pages[slot].fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            });
    }

    fn note_kind_removed_n(&self, slot: usize, n: u64) {
        if n == 0 {
            return;
        }
        let _ =
            self.kind_used_pages[slot].fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(n))
            });
    }

    /// Q-1 - bitmask of kinds currently above their soft quota. A kind
    /// only joins the mask when other kinds actually hold pages — a pool
    /// that is 100% one kind has nothing to protect, and biasing there
    /// would just degrade that kind's own CLOCK behaviour to FIFO.
    fn over_quota_kind_mask(&self) -> u8 {
        let capacity = self.capacity_pages.load(Ordering::Relaxed).max(1);
        let mut used = [0u64; BUFFER_PAGE_KIND_COUNT];
        let mut total = 0u64;
        for (slot, counter) in self.kind_used_pages.iter().enumerate() {
            used[slot] = counter.load(Ordering::Relaxed);
            total = total.saturating_add(used[slot]);
        }
        let mut mask = 0u8;
        for (slot, used_pages) in used.iter().copied().enumerate() {
            let quota_pct = self.kind_quota_pct[slot].load(Ordering::Relaxed);
            if quota_pct == 0 {
                continue;
            }
            let quota_pages = capacity.saturating_mul(quota_pct) / 100;
            if used_pages > quota_pages && total > used_pages {
                mask |= 1 << slot;
            }
        }
        mask
    }

    /// CFG-1 - current soft-quota percentage for a file kind slot
    /// (0 = heap, 1 = index, 2 = LOB/overflow). 0 disables the bias.
    fn kind_quota_pct(&self, slot: usize) -> u64 {
        self.kind_quota_pct
            .get(slot)
            .map(|cell| cell.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// CFG-1 - update a kind's soft-quota percentage at runtime
    /// (`SET GLOBAL mydb_buffer_pool_quota_*_pct`). Clamped to 0..=100 to
    /// match the `MYDB_BUFFER_POOL_QUOTA_*_PCT` env parsing; takes effect
    /// on the next `find_clock_victim` sweep, which samples the mask once
    /// per search.
    fn set_kind_quota_pct(&self, slot: usize, pct: u64) {
        if let Some(cell) = self.kind_quota_pct.get(slot) {
            cell.store(pct.min(100), Ordering::Relaxed);
        }
    }

    /// CFG-1 - configured base prefetch window in pages (0 disables
    /// read-ahead). Runtime-readable for SHOW ENGINE MYDB STATUS and
    /// `SELECT @@global.mydb_buffer_pool_prefetch_pages`.
    fn prefetch_base_pages(&self) -> usize {
        self.prefetch_pages.load(Ordering::Relaxed)
    }

    /// CFG-1 - set the base prefetch window at runtime
    /// (`SET GLOBAL mydb_buffer_pool_prefetch_pages`). Clamped to
    /// `0..=MAX_BUFFER_POOL_PREFETCH_PAGES`, exactly like the
    /// `MYDB_BUFFER_POOL_PREFETCH_PAGES` env parser. The adaptive
    /// effective window is reseeded to the new base so the change takes
    /// effect on the next scan — this mirrors construction, where
    /// `effective_prefetch_pages` starts equal to the base. Setting 0
    /// disables prefetch immediately: both `prefetch_range` and
    /// `prefetch_pages` early-return on a zero effective window, and
    /// `maybe_adapt_prefetch` early-returns on a zero base, so the
    /// controller will not nudge it back up.
    fn set_prefetch_base_pages(&self, pages: usize) {
        let base = pages.min(MAX_BUFFER_POOL_PREFETCH_PAGES);
        self.prefetch_pages.store(base, Ordering::Relaxed);
        let effective = if base == 0 {
            0
        } else {
            base.min(
                ((self.capacity_pages.load(Ordering::Relaxed) as usize)
                    / (8 * self.io_threads.max(1)))
                .clamp(1, MAX_BUFFER_POOL_PREFETCH_PAGES),
            )
        };
        self.effective_prefetch_pages
            .store(effective, Ordering::Relaxed);
    }

    fn small_scan_pct(&self) -> u64 {
        self.small_scan_pct.load(Ordering::Relaxed)
    }

    fn set_small_scan_pct(&self, pct: u64) {
        self.small_scan_pct.store(pct.min(100), Ordering::Relaxed);
    }

    fn small_scan_threshold_pages(&self) -> u64 {
        let pct = self.small_scan_pct();
        if pct == 0 {
            return 0;
        }
        let capacity = self.capacity_pages.load(Ordering::Relaxed).max(1);
        capacity.min(
            capacity
                .saturating_mul(pct)
                .checked_div(100)
                .unwrap_or(0)
                .max(MIN_BUFFER_POOL_SMALL_SCAN_PAGES),
        )
    }

    fn scan_cache_policy(&self, physical_pages: u64) -> ScanCachePolicy {
        let threshold = self.small_scan_threshold_pages();
        if physical_pages > 0 && threshold > 0 && physical_pages <= threshold {
            ScanCachePolicy::Small
        } else {
            ScanCachePolicy::Bulk
        }
    }

    fn new(
        capacity_pages: u64,
        log: Arc<crate::wal::LogMutex>,
        durability: DurabilityMode,
        lock_wait: Duration,
    ) -> Arc<Self> {
        // Small standalone storage fixtures exercise page/codec semantics.
        // Tests for worker parallelism and prefetch use new_with_options with
        // explicit settings, avoiding speculative I/O races in these fixtures.
        let (io_threads, prefetch_pages) = if cfg!(test) {
            (1, 0)
        } else {
            (
                default_buffer_pool_io_threads(),
                DEFAULT_BUFFER_POOL_PREFETCH_PAGES,
            )
        };
        Self::new_with_options(
            capacity_pages,
            log,
            durability,
            lock_wait,
            io_threads,
            prefetch_pages,
        )
    }

    fn new_with_options(
        capacity_pages: u64,
        log: Arc<crate::wal::LogMutex>,
        durability: DurabilityMode,
        lock_wait: Duration,
        io_threads: usize,
        prefetch_pages: usize,
    ) -> Arc<Self> {
        let shard_count = if cfg!(test) && durability == DurabilityMode::Memory {
            4
        } else {
            thread::available_parallelism()
                .map(NonZeroUsize::get)
                .unwrap_or(4)
                .next_power_of_two()
                .clamp(4, 64)
        };
        // M-6 - per-pool cap is also bounded by shard count. A worker
        // per shard is the natural ceiling; beyond that they fight
        // for the same eviction hand. Picking the larger of half-cpus
        // (the historical default) and the configured request lets
        // operators dial up IO concurrency on heavy-write boxes.
        let effective_max_threads = MAX_BUFFER_POOL_IO_THREADS.min(shard_count);
        let io_threads = io_threads.clamp(1, effective_max_threads.max(1));
        // N-1 - prefetch is now decoupled from BUFFER_POOL_FLUSH_BATCH_PAGES
        // (which governs flush coalescing, a different concern). The
        // base value still clamps to the absolute ceiling so the queue
        // never grows unbounded.
        let prefetch_pages = prefetch_pages.min(MAX_BUFFER_POOL_PREFETCH_PAGES);
        let effective_prefetch_pages = if prefetch_pages == 0 {
            0
        } else {
            prefetch_pages.min(
                ((capacity_pages.max(1) as usize) / (8 * io_threads.max(1)))
                    .clamp(1, MAX_BUFFER_POOL_PREFETCH_PAGES),
            )
        };
        let pool = Arc::new(Self {
            capacity_pages: AtomicU64::new(capacity_pages.max(1)),
            small_scan_pct: AtomicU64::new(
                crate::util::env::var("MYDB_BUFFER_POOL_SMALL_SCAN_PCT")
                    .ok()
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or(DEFAULT_BUFFER_POOL_SMALL_SCAN_PCT)
                    .min(100),
            ),
            used_pages: CachePadded::new(AtomicU64::new(0)),
            evictions: AtomicU64::new(0),
            flushes: AtomicU64::new(0),
            write_requests: AtomicU64::new(0),
            flush_errors: AtomicU64::new(0),
            background_io_errors: AtomicU64::new(0),
            dirty_pages: CachePadded::new(AtomicU64::new(0)),
            waits: AtomicU64::new(0),
            wait_ms: AtomicU64::new(0),
            background_flushes: AtomicU64::new(0),
            last_checkpoint_lsn: AtomicU64::new(0),
            page_wal_publish_gate: parking_lot::RwLock::new(()),
            misses: CachePadded::new(AtomicU64::new(0)),
            read_ios: AtomicU64::new(0),
            kind_read_ios: std::array::from_fn(|_| AtomicU64::new(0)),
            read_syscalls: AtomicU64::new(0),
            batch_read_ops: AtomicU64::new(0),
            batch_read_pages: AtomicU64::new(0),
            write_ios: AtomicU64::new(0),
            write_syscalls: AtomicU64::new(0),
            prefetches: AtomicU64::new(0),
            prefetch_completed: AtomicU64::new(0),
            prefetch_hits: AtomicU64::new(0),
            prefetch_wasted: AtomicU64::new(0),
            prefetch_demand_waits: AtomicU64::new(0),
            eviction_scans: AtomicU64::new(0),
            eviction_hand: AtomicUsize::new(0),
            flush_cursor: AtomicUsize::new(0),
            shards: (0..shard_count)
                .map(|_| BufferPoolShard::default())
                .collect(),
            file_generations: RwLock::new(BufferFileGenerationMap::default()),
            next_file_generation: AtomicU64::new(1),
            file_cache: RwLock::new(BufferFileCacheMap::default()),
            log,
            durability,
            lock_wait,
            io_threads,
            ordered_read_helpers: AtomicUsize::new(0),
            prefetch_pages: AtomicUsize::new(prefetch_pages),
            effective_prefetch_pages: AtomicUsize::new(effective_prefetch_pages),
            prefetch_hit_ema_bp: AtomicU64::new(0),
            prefetch_adapt_last_prefetches: AtomicU64::new(0),
            prefetch_adapt_last_prefetch_hits: AtomicU64::new(0),
            prefetch_adapt_last_wasted: AtomicU64::new(0),
            prefetch_adapt_last_queue_drops: AtomicU64::new(0),
            prefetch_adapt_last_demand_waits: AtomicU64::new(0),
            wait_lock: Mutex::new(()),
            wait_cv: Condvar::new(),
            frame_waiters: AtomicUsize::new(0),
            prefetch_queue: Mutex::new(VecDeque::new()),
            bulk_flush_ranges: Mutex::new(std::array::from_fn(|_| None)),
            bulk_flush_cursor: AtomicUsize::new(0),
            prefetch_queued_or_inflight: (0..shard_count)
                .map(|_| Mutex::new(HashSet::new()))
                .collect(),
            shutdown: AtomicBool::new(false),
            flusher: Mutex::new(Vec::new()),
            read_ios_normal: AtomicU64::new(0),
            read_ios_scan: AtomicU64::new(0),
            read_ios_prefetch: AtomicU64::new(0),
            small_scan_accesses: AtomicU64::new(0),
            small_scan_cold_accesses: AtomicU64::new(0),
            small_scan_promotions: AtomicU64::new(0),
            small_scan_read_ios: AtomicU64::new(0),
            prefetch_queue_drops: AtomicU64::new(0),
            prefetch_duplicate_drops: AtomicU64::new(0),
            prefetch_pressure_drops: AtomicU64::new(0),
            prefetch_queue_full_drops: AtomicU64::new(0),
            prefetch_stale_drops: AtomicU64::new(0),
            prefetch_queue_wait_us_total: AtomicU64::new(0),
            prefetch_queue_drains: AtomicU64::new(0),
            prefetch_lead_us_total: AtomicU64::new(0),
            prefetch_lead_samples: AtomicU64::new(0),
            ensure_capacity_busy_us: AtomicU64::new(0),
            file_handle_wait_ns: AtomicU64::new(0),
            dirty_ratio_histogram: std::array::from_fn(|_| AtomicU64::new(0)),
            dirty_ratio_samples: AtomicU64::new(0),
            scan_origin_count: AtomicU64::new(0),
            prefetch_gate_last_scan_reads: AtomicU64::new(0),
            prefetch_gate_last_normal_reads: AtomicU64::new(0),
            prefetch_gate_mixed_reads: AtomicBool::new(false),
            scan_origin_evictions: AtomicU64::new(0),
            cold_recycle_hits: AtomicU64::new(0),
            cold_recycle_fallbacks: AtomicU64::new(0),
            raw_scan_pages: AtomicU64::new(0),
            raw_page_allocations: AtomicU64::new(0),
            raw_page_reuse_hits: AtomicU64::new(0),
            raw_page_reuse_misses: AtomicU64::new(0),
            recycled_raw_page_count: AtomicU64::new(0),
            file_syncs: AtomicU64::new(0),
            file_sync_skipped: AtomicU64::new(0),
            flush_data_cache_short_circuits: AtomicU64::new(0),
            flush_data_cache_pages: AtomicU64::new(0),
            flush_data_cache_us: AtomicU64::new(0),
            capacity_pressure: AtomicBool::new(false),
            evict_sync_dirty_flushes: AtomicU64::new(0),
            evict_sync_dirty_flush_us: AtomicU64::new(0),
            cooldown_deferred_flushes: AtomicU64::new(0),
            headroom_flushes: AtomicU64::new(0),
            checkpoint_flush_pages: AtomicU64::new(0),
            checkpoint_flush_us: AtomicU64::new(0),
            checkpoint_preflush_pages: AtomicU64::new(0),
            checkpoint_preflush_us: AtomicU64::new(0),
            prefetch_queue_depth_peak: AtomicU64::new(0),
            worker_lock: Mutex::new(()),
            worker_cv: Condvar::new(),
            kind_used_pages: std::array::from_fn(|_| CachePadded::new(AtomicU64::new(0))),
            kind_quota_pct: kind_quota_pct_from_env(),
            quota_evictions: AtomicU64::new(0),
            pool_started: Instant::now(),
        });
        if durability != DurabilityMode::Memory {
            pool.start_workers();
        }
        pool
    }

    fn start_workers(self: &Arc<Self>) {
        let mut handles = Vec::new();
        let stride = self.io_threads.max(1);
        for worker_id in 0..self.io_threads {
            let weak = Arc::downgrade(self);
            let handle = thread::Builder::new()
                .name(format!("mydb-buffer-pool-io-{worker_id}"))
                .spawn(move || {
                    let mut next_clean = Instant::now();
                    loop {
                        let Some(pool) = weak.upgrade() else {
                            break;
                        };
                        if pool.shutdown.load(Ordering::Relaxed) {
                            break;
                        }
                        if let Err(err) = pool.drain_prefetch_queue() {
                            pool.record_background_io_error("prefetch", &err);
                        }
                        let bulk_work = match pool.flush_queued_bulk_range() {
                            Ok(work) => work,
                            Err(err) => {
                                pool.record_background_io_error("bulk flush", &err);
                                false
                            }
                        };
                        // D-2 - a foreground evictor found no clean victim.
                        // Respond with a headroom flush before the regular
                        // watermark check so the evictor's bounded wait ends
                        // with a clean victim instead of a synchronous
                        // foreground page write.
                        if pool.capacity_pressure.swap(false, Ordering::AcqRel) {
                            if let Err(err) = pool.maintain_free_headroom() {
                                pool.record_background_io_error("headroom flush", &err);
                            }
                        }
                        // PERF-4 - each worker flushes only its shard stripe
                        // so the io fleet partitions the dirty backlog instead
                        // of every worker rebuilding + sorting the whole list.
                        if periodic_cleaner_tick_due(&mut next_clean, Instant::now()) {
                            if let Err(err) = pool.background_flush_once(worker_id, stride) {
                                pool.record_background_io_error("page cleaner", &err);
                            }
                            // B1 - bound the oldest-dirty heaps here (no frame lock is
                            // held in the worker loop, unlike the `note_dirty` callers).
                            pool.compact_oversized_dirty_oldest(worker_id, stride);
                        }
                        if bulk_work {
                            continue;
                        }
                        // D-4 - park on the worker condvar; foreground frame
                        // releases no longer wake the IO fleet.
                        let Ok(guard) = pool.worker_lock.lock() else {
                            break;
                        };
                        let _ = pool.worker_cv.wait_timeout(
                            guard,
                            next_clean.saturating_duration_since(Instant::now()),
                        );
                    }
                });
            if let Ok(handle) = handle {
                handles.push(handle);
            }
        }
        if let Ok(mut slot) = self.flusher.lock() {
            slot.extend(handles);
        }
    }

    pub(crate) fn reclaim_for_memory_target(
        &self,
        target_bytes: usize,
        timeout: Duration,
    ) -> Result<(), QueryError> {
        self.reclaim_optional_memory_until(
            target_bytes,
            timeout,
            || crate::engine::executor::mem_budget::controlled_memory_snapshot().0,
            "memory target",
        )
    }

    pub(crate) fn reclaim_for_buffer_limit(
        &self,
        target_bytes: usize,
        timeout: Duration,
    ) -> Result<(), QueryError> {
        self.reclaim_optional_memory_until(
            target_bytes,
            timeout,
            crate::engine::executor::mem_budget::controlled_buffer_used_bytes,
            "buffer pool memory limit",
        )
    }

    /// Best-effort physical-headroom relief. Never wait for a pin, flush a
    /// dirty page or resize the configured pool. A bounded CLOCK window and
    /// time budget keep this safe for foreground admission as well as the
    /// independent pressure worker.
    pub(crate) fn reclaim_clean_for_memory_target(
        &self,
        target_bytes: usize,
        timeout: Duration,
    ) -> Result<(), QueryError> {
        let started = Instant::now();
        while crate::engine::executor::mem_budget::controlled_memory_snapshot().0 > target_bytes
            && started.elapsed() < timeout
        {
            if self.discard_one_recycled_raw_page() {
                continue;
            }
            let start = self.eviction_hand.fetch_add(1, Ordering::Relaxed);
            let Some(victim) = self.find_clean_victim_bounded(start)? else {
                break;
            };
            let scan_origin = victim.2.scan_origin.load(Ordering::Acquire);
            self.consume_victim_with_policy(victim, scan_origin, false)?;
        }
        Ok(())
    }

    fn reclaim_optional_memory_until(
        &self,
        target_bytes: usize,
        timeout: Duration,
        current_used: impl Fn() -> usize,
        target_name: &str,
    ) -> Result<(), QueryError> {
        struct CandidateCleanup<'a>(&'a BufferPool);
        impl Drop for CandidateCleanup<'_> {
            fn drop(&mut self) {
                self.0.trim_cold_candidate_metadata();
            }
        }
        // Weak references retain Arc's allocation after Frame has dropped.
        // Reclaim those control blocks even when raw pages already reached
        // their floor, and again after this pass evicts additional frames.
        self.trim_cold_candidate_metadata();
        let _cleanup = CandidateCleanup(self);
        let started = Instant::now();
        loop {
            let controlled = current_used();
            if controlled <= target_bytes {
                return Ok(());
            }
            self.trim_recycled_raw_pages();
            if current_used() <= target_bytes {
                return Ok(());
            }
            if self.discard_one_recycled_raw_page() {
                continue;
            }
            if !self.evict_one_with_policy(true)? {
                if started.elapsed() >= timeout {
                    return Err(error(
                        crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                        format!(
                            "{target_name} reduction did not converge: controlled={} target={}",
                            current_used(),
                            target_bytes
                        ),
                    ));
                }
                self.wait_for_frame_release(
                    timeout
                        .saturating_sub(started.elapsed())
                        .min(BUFFER_POOL_WAIT_CEILING),
                );
            }
        }
    }

    fn pop_recycled_raw_page(&self, preferred_shard: usize) -> Option<RawPageLease> {
        if self.recycled_raw_page_count.load(Ordering::Acquire) == 0 {
            return None;
        }
        for offset in 0..self.shards.len() {
            let shard_idx = (preferred_shard + offset) % self.shards.len();
            let Ok(mut recycled) = self.shards[shard_idx].recycled_raw_pages.lock() else {
                continue;
            };
            if let Some(lease) = recycled.pop() {
                self.recycled_raw_page_count.fetch_sub(1, Ordering::Relaxed);
                self.raw_page_reuse_hits.fetch_add(1, Ordering::Relaxed);
                return Some(lease);
            }
        }
        None
    }

    fn recycle_raw_page(&self, preferred_shard: usize, mut lease: RawPageLease) {
        if lease.bytes.capacity() < PAGE_SIZE {
            return;
        }
        lease.bytes.resize(PAGE_SIZE, 0);
        let capacity = self.capacity_pages.load(Ordering::Acquire).max(1);
        let used = self.used_pages.load(Ordering::Acquire);
        let mut recycled = self.recycled_raw_page_count.load(Ordering::Acquire);
        loop {
            if used.saturating_add(recycled) >= capacity {
                return;
            }
            match self.recycled_raw_page_count.compare_exchange_weak(
                recycled,
                recycled.saturating_add(1),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => recycled = observed,
            }
        }
        match self.shards[preferred_shard].recycled_raw_pages.lock() {
            Ok(mut pages) => pages.push(lease),
            Err(_) => {
                self.recycled_raw_page_count.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    fn recycle_frame_raw_page(&self, shard_idx: usize, frame: &Frame) {
        // A discard can detach a frame while an existing reader or the
        // single-page miss loader still owns a pin. Moving the Vec out of
        // such a frame would leave that owner observing an empty page image.
        // Detached pinned frames release their charge when the final owner
        // drops; only quiescent/eviction-owned frames are eligible for reuse.
        let pins = frame.pin_count.load(Ordering::Acquire);
        if pins != 0 && pins != Self::EVICTING_PIN {
            return;
        }
        if let Some(lease) = frame.take_raw_page_lease() {
            self.recycle_raw_page(shard_idx, lease);
        }
    }

    fn trim_recycled_raw_pages(&self) {
        let capacity = self.capacity_pages.load(Ordering::Acquire).max(1);
        loop {
            let used = self.used_pages.load(Ordering::Acquire);
            let recycled = self.recycled_raw_page_count.load(Ordering::Acquire);
            if used.saturating_add(recycled) <= capacity || recycled == 0 {
                break;
            }
            let mut removed = false;
            for shard in &self.shards {
                let Ok(mut pages) = shard.recycled_raw_pages.lock() else {
                    continue;
                };
                if let Some(lease) = pages.pop() {
                    discard_raw_page_lease(lease);
                    self.recycled_raw_page_count.fetch_sub(1, Ordering::Relaxed);
                    removed = true;
                    break;
                }
            }
            if !removed {
                break;
            }
        }
    }

    /// Drop one optional recycled raw-page allocation and its governor lease.
    ///
    /// Normal capacity eviction keeps the allocation for immediate reuse, but
    /// a process memory-target reclaim must make the controlled byte total
    /// decrease. Merely moving a victim from a frame into the recycle pool
    /// preserves both its allocation and `BufferRaw` charge, so target reclaim
    /// would otherwise evict every frame and still fail to converge.
    fn discard_one_recycled_raw_page(&self) -> bool {
        for shard in &self.shards {
            let Ok(mut pages) = shard.recycled_raw_pages.lock() else {
                continue;
            };
            if let Some(lease) = pages.pop() {
                // Logical eviction alone can leave most of an allocator's
                // medium pages resident while a few live buffers pin them.
                // Normal replacement still recycles; only pressure/resize
                // discard returns the dead interior pages to the kernel.
                discard_raw_page_lease(lease);
                self.recycled_raw_page_count.fetch_sub(1, Ordering::Relaxed);
                return true;
            }
        }
        false
    }

    /// Release unused preallocated page blocks when the owning database
    /// shuts down. File handles can transiently outlive `Database`, so relying
    /// on the final `Arc<BufferPool>` drop would keep the process-wide raw
    /// reservation charged and prevent a same-process reopen from admitting
    /// its own pool.
    fn release_recycled_raw_pages(&self) {
        let mut released = 0u64;
        for shard in &self.shards {
            if let Ok(mut pages) = shard.recycled_raw_pages.lock() {
                released = released.saturating_add(pages.len() as u64);
                pages.clear();
            }
        }
        let _ = self.recycled_raw_page_count.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |counted| Some(counted.saturating_sub(released)),
        );
    }

    fn try_allocate_raw_page_lease(&self) -> Result<Option<RawPageLease>, QueryError> {
        use crate::engine::executor::mem_budget::{
            ControlledMemoryCategory, ControlledMemoryCharge,
        };
        let Some(mut charge) = ControlledMemoryCharge::try_new_optional(
            ControlledMemoryCategory::BufferRaw,
            PAGE_SIZE,
        ) else {
            return Ok(None);
        };
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(PAGE_SIZE).map_err(|_| {
            error(
                crate::error::catalog::ER_OUTOFMEMORY,
                format!("unable to allocate {PAGE_SIZE} bytes for a buffer-pool page"),
            )
        })?;
        bytes.resize(PAGE_SIZE, 0);
        let extra = bytes.capacity().saturating_sub(PAGE_SIZE);
        if !charge.try_grow_optional(extra) {
            return Ok(None);
        }
        self.raw_page_allocations.fetch_add(1, Ordering::Relaxed);
        Ok(Some(RawPageLease { bytes, charge }))
    }

    fn claim_raw_page_lease(&self, preferred_shard: usize) -> Result<RawPageLease, QueryError> {
        let started = Instant::now();
        loop {
            if let Some(lease) = self.pop_recycled_raw_page(preferred_shard) {
                return Ok(lease);
            }
            self.raw_page_reuse_misses.fetch_add(1, Ordering::Relaxed);
            if let Some(lease) = self.try_allocate_raw_page_lease()? {
                return Ok(lease);
            }

            if self.evict_one_with_policy(true)? {
                continue;
            }
            if started.elapsed() >= self.lock_wait {
                return Err(self.capacity_exhausted_error());
            }
            let remaining = self.lock_wait.saturating_sub(started.elapsed());
            self.waits.fetch_add(1, Ordering::Relaxed);
            self.wait_for_frame_release(remaining.min(BUFFER_POOL_WAIT_CEILING));
        }
    }

    fn claim_raw_page_memory(
        &self,
        allocation_capacity: usize,
    ) -> Result<crate::engine::executor::mem_budget::ControlledMemoryCharge, QueryError> {
        let started = Instant::now();
        loop {
            // Dirty-page callers already own a fully encoded PAGE_SIZE Vec.
            // Reuse the governed reservation from an evicted page even though
            // their existing byte allocation means the old buffer itself can
            // be dropped here.
            if let Some(mut lease) = self.pop_recycled_raw_page(0) {
                let extra = allocation_capacity.saturating_sub(lease.charge.bytes());
                if lease.charge.try_grow_optional(extra) {
                    return Ok(lease.charge);
                }
                self.recycle_raw_page(0, lease);
            }
            if let Some(charge) = try_controlled_raw_page_charge(allocation_capacity) {
                return Ok(charge);
            }
            if self.evict_one_with_policy(true)? {
                continue;
            }
            if started.elapsed() >= self.lock_wait {
                return Err(self.capacity_exhausted_error());
            }
            let remaining = self.lock_wait.saturating_sub(started.elapsed());
            self.waits.fetch_add(1, Ordering::Relaxed);
            self.wait_for_frame_release(remaining.min(BUFFER_POOL_WAIT_CEILING));
        }
    }

    fn heap_page_for_read(
        &self,
        frame: &Arc<Frame>,
        _intent: AccessIntent,
    ) -> Result<Arc<Page>, QueryError> {
        self.with_finalized_frame_bytes(frame, |bytes| Ok(Arc::new(Page::decode(bytes)?)))
    }

    /// Decode an operation-local snapshot; frames retain only encoded bytes.
    fn index_page_for_read(&self, frame: &Arc<Frame>) -> Result<Arc<IndexPage>, QueryError> {
        self.with_finalized_frame_bytes(frame, |bytes| {
            let epoch = frame.dirty_epoch.load(Ordering::Acquire);
            let already_validated =
                frame.index_header_validated_epoch.load(Ordering::Acquire) == epoch;
            // Materialization validates every record. Only check the CRC
            // once per epoch, while retaining validation across mutations.
            if !already_validated {
                validate_index_tree_page_checksum(bytes)?;
            }
            let page = Arc::new(decode_validated_index_tree_page(bytes)?);
            if !already_validated {
                frame
                    .index_header_validated_epoch
                    .store(epoch, Ordering::Release);
            }
            Ok(page)
        })
    }

    /// D-1 - register `key` in its shard's dirty set. Called on every
    /// clean→dirty transition, right where `dirty_pages` is incremented.
    ///
    /// B1 - the `dirty_oldest` push is gated on `oldest_dirty_queue_enabled()`
    /// (the pop side already was; pushing while pops are disabled only grew
    /// the heap) and on a durable backend (memory mode never flushes, so the
    /// queue would only accumulate). The heap is kept bounded by a lazy
    /// compaction that runs in the background IO worker
    /// (`compact_oversized_dirty_oldest`), NOT here: several callers
    /// (`store_page`'s new-frame path and raw mutation variants) invoke
    /// `note_dirty` while already holding this shard's `frames.write()` lock,
    /// so acquiring `frames.read()` for a compaction inline would re-enter the
    /// same `RwLock` and self-deadlock. The background worker holds no frame
    /// locks when it compacts, so it can safely resolve `first_dirty_lsn`.
    fn note_dirty(&self, key: &PageKey, first_lsn: u64) {
        let shard_idx = self.shard_index(key);
        if let Ok(mut set) = self.shards[shard_idx].dirty_set.lock() {
            set.insert(key.clone());
        }
        if !oldest_dirty_queue_enabled() || self.durability == DurabilityMode::Memory {
            return;
        }
        if let Ok(mut oldest) = self.shards[shard_idx].dirty_oldest.lock() {
            oldest.push(Reverse(DirtyCandidate {
                first_lsn,
                key: key.clone(),
            }));
            // Publish the generation while still holding the heap lock. A
            // rebuilder that subsequently acquires the lock must therefore
            // observe either both the candidate and generation, or neither.
            self.shards[shard_idx]
                .dirty_oldest_epoch
                .fetch_add(1, Ordering::Release);
        }
    }

    /// B1 - compact every over-sized `dirty_oldest` heap in this worker's
    /// shard stripe. Called from the background IO worker loop, where no frame
    /// lock is held, so the `frames.read()` inside
    /// [`Self::compact_dirty_oldest`] is safe. A heap is rebuilt once it
    /// exceeds `2 ×` its live dirty-set size (past a small floor so a tiny
    /// working set never thrashes the rebuild). The `len()` probes are cheap
    /// and run every ~25 ms, so between compactions the heap can only grow by
    /// one tick's worth of dirtying — never without bound.
    fn compact_oversized_dirty_oldest(&self, worker_id: usize, stride: usize) {
        if !oldest_dirty_queue_enabled() || self.durability == DurabilityMode::Memory {
            return;
        }
        for shard_idx in 0..self.shards.len() {
            if stride > 1 && shard_idx % stride != worker_id {
                continue;
            }
            let heap_len = self.shards[shard_idx]
                .dirty_oldest
                .lock()
                .map(|heap| heap.len())
                .unwrap_or(0);
            if heap_len == 0 {
                continue;
            }
            let set_len = self.shards[shard_idx]
                .dirty_set
                .lock()
                .map(|set| set.len())
                .unwrap_or(0);
            // Two gates:
            // (a) Idle drain — dirty_set is empty (all pages flushed), so
            //     every heap entry is stale. Compact unconditionally so the
            //     heap converges to 0 rather than getting stuck at the last
            //     "just below the growth threshold" value.
            // (b) Growth threshold — dirty_set has live entries; only compact
            //     when the heap has grown well past `max(2 * set_len, floor)`
            //     so we don't thrash the rebuild on every tick during steady
            //     churn (`floor` amortises the cost of the frames-read scan).
            let should_compact = set_len == 0
                || heap_len
                    > set_len
                        .saturating_mul(2)
                        .max(BUFFER_POOL_DIRTY_OLDEST_MIN_COMPACT);
            if should_compact {
                self.compact_dirty_oldest(shard_idx);
            }
        }
    }

    /// B1 - rebuild a shard's `dirty_oldest` min-heap so it holds exactly
    /// one entry per still-dirty page, each stamped with the frame's
    /// CURRENT `first_dirty_lsn`. This discards every stale entry (pages
    /// flushed since they were enqueued, and old-LSN duplicates of pages
    /// that were re-dirtied) in one pass, keeping the heap bounded by the
    /// live dirty-page count.
    ///
    /// MUST NOT be called while holding this shard's `frames.write()` lock —
    /// it takes `frames.read()`. Only the background worker
    /// (`compact_oversized_dirty_oldest`) calls it, holding no frame locks.
    /// Lock discipline mirrors `collect_dirty_frames_striped` /
    /// `pop_oldest_dirty_frames_striped`: snapshot the dirty-set keys, drop
    /// the set lock, resolve the frames under a short `frames` read lock
    /// (holding no other buffer lock), then swap in the fresh heap under
    /// the `dirty_oldest` lock. It never holds `frames` while taking
    /// `dirty_oldest`, so it cannot deadlock against the discard paths,
    /// which take `frames.write()` before `dirty_oldest`. Pages dirtied in
    /// the tiny window between the set snapshot and the swap are still
    /// tracked in `dirty_set` and are re-enqueued on the next dirtying
    /// event, so nothing is lost for more than one compaction cycle.
    fn compact_dirty_oldest(&self, shard_idx: usize) {
        let observed_epoch = self.shards[shard_idx]
            .dirty_oldest_epoch
            .load(Ordering::Acquire);
        let keys: Vec<PageKey> = {
            let Ok(set) = self.shards[shard_idx].dirty_set.lock() else {
                return;
            };
            set.iter().cloned().collect()
        };
        let mut fresh: Vec<Reverse<DirtyCandidate>> = Vec::with_capacity(keys.len());
        {
            let Ok(frames) = self.shards[shard_idx].frames.read() else {
                return;
            };
            for key in keys {
                if let Some(frame) = frames.get(&key) {
                    if frame.dirty.load(Ordering::Acquire) {
                        let first_lsn = frame.first_dirty_lsn.load(Ordering::Acquire);
                        fresh.push(Reverse(DirtyCandidate { first_lsn, key }));
                    }
                }
            }
        }
        if let Ok(mut oldest) = self.shards[shard_idx].dirty_oldest.lock() {
            // Never blind-swap over a candidate pushed after the authoritative
            // snapshot. `note_dirty` advances the epoch before releasing this
            // same heap lock, closing the old lost-enqueue race completely.
            if self.shards[shard_idx]
                .dirty_oldest_epoch
                .load(Ordering::Acquire)
                == observed_epoch
            {
                *oldest = BinaryHeap::from(fresh);
            }
        }
    }

    /// Txn-buffering (`MYDB_WAL_TXN_BUFFER`): pin `key`'s frame against flush
    /// and eviction because the caller is about to dirty it WITHOUT writing its
    /// WAL record (the record is buffered for a single batched append at
    /// commit). MUST be called BEFORE the dirtying `store_page*` so a concurrent
    /// flush observing `dirty=true` is guaranteed (via the dirty Acquire/Release
    /// edge) to also observe `deferred_pins>0` and skip. Returns false if the
    /// frame is not resident (caller must then fall back to eager append).
    fn add_deferred_pin(self: &Arc<Self>, key: &PageKey) -> bool {
        let Ok(Some(frame)) = self.lookup_frame(key) else {
            return false;
        };
        // Share the ordinary pin CAS with eviction. Merely retaining an Arc
        // does not retain residency: a selected victim may already be owned
        // by an evictor and must take the eager-WAL fallback instead.
        if !self.try_pin_existing(&frame, AccessIntent::Normal) {
            return false;
        }
        frame.deferred_pins.fetch_add(1, Ordering::AcqRel);
        frame.unpin();
        self.notify_frame_waiters();
        true
    }

    /// Txn-buffering: the deferred record for `key` has now been appended at
    /// `lsn`. Publish the real WAL LSN (so the WAL-ahead flush gate syncs up to
    /// it) BEFORE releasing the deferred pin, so the page never becomes
    /// flushable with a stale `dirty_lsn`.
    ///
    /// Also stamp the real LSN into the page image itself: the eager write path
    /// calls `Page::set_lsn(lsn)` right after `append_pre_encoded`, but the
    /// txn-buffer fast path stores the page with `page_lsn = 0` because the LSN
    /// is not known yet. Without this back-patch the heap-page `page_lsn` field
    /// stays 0 forever, and the first-update-after-checkpoint FPI gate
    /// (`maybe_emit_heap_fpi`, which skips when `page_lsn > last_checkpoint_lsn`)
    /// never fires its "already covered this window" shortcut — every UPDATE
    /// emits a fresh 16 KiB full-page image. Measured on sysbench
    /// oltp_update_index (20 threads, 1M rows): fixing this doubles TPS and
    /// cuts WAL bytes ~10×. The raw frame image receives the LSN immediately;
    /// its checksum remains deferred until full-image exposure or flush.
    fn finalize_deferred(self: &Arc<Self>, key: &PageKey, lsn: u64) {
        if let Ok(Some(frame)) = self.lookup_frame(key) {
            if lsn != 0 {
                frame.dirty_lsn.fetch_max(lsn, Ordering::AcqRel);
                frame.note_first_dirty_lsn(lsn);
                if let Ok(mut bytes) = frame.bytes.write() {
                    set_page_lsn(&mut bytes, lsn);
                    frame.checksum_dirty.store(true, Ordering::Release);
                }
            }
            frame.deferred_pins.fetch_sub(1, Ordering::AcqRel);
        }
    }

    /// Remove an evicted/discarded frame's registration. The caller must hold
    /// this shard's frames write lock until removal completes, excluding a
    /// replacement frame registering the same key.
    fn dirty_set_remove(&self, key: &PageKey) {
        let shard_idx = self.shard_index(key);
        if let Ok(mut set) = self.shards[shard_idx].dirty_set.lock() {
            set.remove(key);
        }
    }

    /// A clean observation is only a hint until the registration is removed.
    /// Writers mark dirty before taking dirty_set to register; checking dirty
    /// under that same latch either preserves their registration or removes
    /// the old one before the writer registers again. Hold frames first to
    /// exclude eviction/readmission of the same key. Never acquire a page's
    /// bytes latch here: writers already hold bytes while registering.
    fn prune_clean_dirty_keys(&self, shard_idx: usize, keys: &[PageKey]) -> Result<(), QueryError> {
        #[cfg(test)]
        for key in keys {
            dirty_registry_tests::fire(key);
        }
        let shard = &self.shards[shard_idx];
        let frames = shard.frames.read().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "buffer pool shard lock poisoned",
            )
        })?;
        let mut dirty = shard.dirty_set.lock().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "buffer pool dirty-set lock poisoned",
            )
        })?;
        for key in keys {
            if !frames
                .get(key)
                .is_some_and(|frame| frame.dirty.load(Ordering::Acquire))
            {
                dirty.remove(key);
            }
        }
        Ok(())
    }

    /// D-1 - resolve the dirty sets to live dirty frames, pruning stale
    /// entries (evicted or already-clean pages) as a side effect. Cost
    /// is O(registered dirty keys), not O(pool capacity).
    fn collect_dirty_frames(&self) -> Result<Vec<Arc<Frame>>, QueryError> {
        self.collect_dirty_frames_striped(0, 1)
    }

    /// PERF-4 - dirty-frame collection restricted to the shards a single
    /// background IO worker owns (`shard_idx % stride == worker_id`).
    /// Without this every one of the `io_threads` workers woke together
    /// and each rebuilt the FULL O(dirty) frame list + sort, then mostly
    /// flushed each other's pages (the `flush_lock` made the losers
    /// no-ops). Striping gives each worker a disjoint slice so the
    /// collect + sort work is partitioned instead of duplicated. `stride
    /// = 1` (the default via `collect_dirty_frames`) walks every shard.
    fn collect_dirty_frames_striped(
        &self,
        worker_id: usize,
        stride: usize,
    ) -> Result<Vec<Arc<Frame>>, QueryError> {
        let mut out = Vec::new();
        for (idx, shard) in self.shards.iter().enumerate() {
            if stride > 1 && idx % stride != worker_id {
                continue;
            }
            let keys: Vec<PageKey> = {
                let Ok(set) = shard.dirty_set.lock() else {
                    continue;
                };
                if set.is_empty() {
                    continue;
                }
                set.iter().cloned().collect()
            };
            let mut stale: Vec<PageKey> = Vec::new();
            {
                let frames = shard.frames.read().map_err(|_| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "buffer pool shard lock poisoned",
                    )
                })?;
                for key in keys {
                    match frames.get(&key) {
                        Some(frame) if frame.dirty.load(Ordering::Acquire) => {
                            out.push(frame.clone());
                        }
                        _ => stale.push(key),
                    }
                }
            }
            if !stale.is_empty() {
                self.prune_clean_dirty_keys(idx, &stale)?;
            }
        }
        Ok(out)
    }

    fn pop_oldest_dirty_frames_striped(
        &self,
        worker_id: usize,
        stride: usize,
        max_pages: usize,
    ) -> Result<Vec<Arc<Frame>>, QueryError> {
        if max_pages == 0 {
            return Ok(Vec::new());
        }
        let mut candidates = Vec::new();
        for (idx, shard) in self.shards.iter().enumerate() {
            if stride > 1 && idx % stride != worker_id {
                continue;
            }
            let mut popped = Vec::new();
            if let Ok(mut oldest) = shard.dirty_oldest.lock() {
                // Look past a bounded number of temporarily pinned oldest
                // pages so one busy frame cannot starve the shard, while
                // preserving every still-current candidate for a later tick.
                let probe_limit = max_pages.saturating_mul(8).max(16);
                while popped.len() < probe_limit {
                    let Some(Reverse(candidate)) = oldest.pop() else {
                        break;
                    };
                    popped.push(candidate);
                }
            }
            if popped.is_empty() {
                continue;
            }
            let mut retry = Vec::new();
            {
                let frames = shard.frames.read().map_err(|_| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "buffer pool shard lock poisoned",
                    )
                })?;
                for candidate in popped {
                    let Some(frame) = frames.get(&candidate.key) else {
                        continue;
                    };
                    let current = frame.dirty.load(Ordering::Acquire)
                        && frame.first_dirty_lsn.load(Ordering::Acquire) == candidate.first_lsn;
                    if !current {
                        continue;
                    }
                    if frame.pin_count.load(Ordering::Acquire) == 0 && frame.is_ready() {
                        candidates.push((candidate, Arc::clone(frame)));
                    } else {
                        // Temporary state, not staleness. Dropping this entry
                        // used to make the page invisible to age cleaning
                        // forever once it later became unpinned.
                        retry.push(candidate);
                    }
                }
            }
            if !retry.is_empty() {
                if let Ok(mut oldest) = shard.dirty_oldest.lock() {
                    oldest.extend(retry.into_iter().map(Reverse));
                }
            }
        }
        candidates.sort_unstable_by_key(|(candidate, _)| {
            if candidate.first_lsn == 0 {
                u64::MAX
            } else {
                candidate.first_lsn
            }
        });
        if candidates.len() > max_pages {
            for (candidate, _) in candidates.drain(max_pages..) {
                let shard_idx = self.shard_index(&candidate.key);
                if let Ok(mut oldest) = self.shards[shard_idx].dirty_oldest.lock() {
                    oldest.push(Reverse(candidate));
                }
            }
        }
        let frames = candidates
            .into_iter()
            .map(|(_, frame)| frame)
            .collect::<Vec<_>>();
        Ok(oldest_frames_in_io_order(frames, max_pages))
    }

    fn heap_file_key(path: PathBuf, table_id: u64) -> BufferPageFileKey {
        BufferPageFileKey::new(BufferPageFileKind::Heap { table_id }, path)
    }

    fn index_file_key(path: PathBuf) -> BufferPageFileKey {
        let (table_id, name) = index_identity_from_path(&path);
        BufferPageFileKey::new(
            if path.extension().is_some_and(|e| e == "rtree") {
                BufferPageFileKind::Spatial { table_id, name }
            } else {
                BufferPageFileKind::Index { table_id, name }
            },
            path,
        )
    }

    fn overflow_file_key(path: PathBuf, table_id: u64) -> BufferPageFileKey {
        BufferPageFileKey::new(BufferPageFileKind::Overflow { table_id }, path)
    }

    fn zone_map_file_key(path: PathBuf, table_id: u64, file_generation: u64) -> BufferPageFileKey {
        BufferPageFileKey::new(
            BufferPageFileKind::ZoneMap {
                table_id,
                file_generation,
            },
            path,
        )
    }

    /// Buffer-pool file key for a page-based vector (HNSW) index file
    /// (`.hvec`). Mirrors [`Self::index_file_key`] but tags the pages with
    /// the `Vector` kind so they draw on the dedicated vector soft quota.
    /// `index_identity_from_path` is extension-agnostic, so it parses the
    /// `<table>__<id>__<index>` identity for `.hvec` the same as `.idx`.
    // Wired in Phase 0; first consumer lands with the paged backend (Phase 1).
    #[allow(dead_code)]
    fn vector_index_file_key(path: PathBuf) -> BufferPageFileKey {
        let (table_id, name) = index_identity_from_path(&path);
        BufferPageFileKey::new(BufferPageFileKind::Vector { table_id, name }, path)
    }

    /// Pin `key` and hand back a clone of its raw 16KB page image,
    /// loading it from disk on a cache miss. Used by the overflow (LOB)
    /// store, whose pages are plain byte buffers.
    fn read_raw_page(self: &Arc<Self>, key: PageKey) -> Result<Vec<u8>, QueryError> {
        let pinned = self.pin_page_with_intent(key, AccessIntent::Normal)?;
        self.with_finalized_frame_bytes(&pinned.frame, |bytes| Ok(bytes.to_vec()))
    }

    fn with_raw_page<T>(
        self: &Arc<Self>,
        key: PageKey,
        f: impl FnOnce(&[u8]) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        self.with_raw_page_intent(key, AccessIntent::Normal, f)
    }

    fn with_raw_page_intent<T>(
        self: &Arc<Self>,
        key: PageKey,
        intent: AccessIntent,
        f: impl FnOnce(&[u8]) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        let pinned = self.pin_page_with_intent(key, intent)?;
        let result = self.with_finalized_frame_bytes(&pinned.frame, f);
        if matches!(intent, AccessIntent::Scan | AccessIntent::SmallScan) {
            self.raw_scan_pages.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    pub(crate) fn capacity_pages(&self) -> u64 {
        self.capacity_pages.load(Ordering::Relaxed)
    }

    /// Materialize the deferred LSN/checksum pair in the resident raw image.
    ///
    /// Raw heap/index mutators leave the 16 KiB CRC stale while a frame stays
    /// resident. Borrowed views validate structural bounds; full-image
    /// consumers and flushes pass through this boundary.
    fn with_finalized_frame_bytes<T>(
        &self,
        frame: &Frame,
        f: impl FnOnce(&[u8]) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        // Clean resident pages are overwhelmingly the common case for index
        // probes.  Do not serialize all of their readers behind the frame's
        // write lock merely to discover that there is no deferred checksum to
        // materialize.  The flag is checked *after* acquiring the read lock:
        // a mutator publishes checksum_dirty while holding the write lock, so
        // observing false here guarantees that the borrowed image is already
        // finalized.  Dirty images drop the read guard and retain the original
        // exclusive finalize path below.
        {
            let bytes = frame.bytes.read().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer frame read lock poisoned",
                )
            })?;
            if !frame.checksum_dirty.load(Ordering::Acquire) {
                return f(&bytes);
            }
        }
        let mut bytes = frame.bytes.write().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "buffer frame write lock poisoned",
            )
        })?;
        frame.materialize_packed_leaf(&mut bytes)?;
        if frame.checksum_dirty.load(Ordering::Acquire) {
            let lsn = frame.dirty_lsn.load(Ordering::Acquire);
            set_page_lsn_if_formatted(&mut bytes, lsn);
            frame.checksum_dirty.store(false, Ordering::Release);
        }
        f(&bytes)
    }

    fn validate_frame_mutation_image(
        &self,
        frame: &Frame,
        bytes: &mut [u8],
    ) -> Result<(), QueryError> {
        frame.materialize_packed_leaf(bytes)?;
        if frame.has_packed_leaf() {
            frame.invalidate_packed_leaf();
        }
        let epoch = frame.dirty_epoch.load(Ordering::Acquire);
        let checksum_dirty = frame.checksum_dirty.load(Ordering::Acquire);
        match &frame.key.file.kind {
            BufferPageFileKind::Index { .. } => {
                if frame.index_header_validated_epoch.load(Ordering::Acquire) == epoch {
                    return Ok(());
                }
                if checksum_dirty {
                    validate_index_page_structure(bytes)?;
                } else {
                    validate_index_page_header(bytes)?;
                }
                frame
                    .index_header_validated_epoch
                    .store(epoch, Ordering::Release);
                Ok(())
            }
            BufferPageFileKind::Heap { .. } => {
                if frame.heap_header_validated_epoch.load(Ordering::Acquire) == epoch {
                    return Ok(());
                }
                let present = if checksum_dirty {
                    validate_heap_page_structure(bytes)?
                } else {
                    validate_heap_page_header(bytes)?
                };
                frame.heap_header_present.store(present, Ordering::Relaxed);
                frame
                    .heap_header_validated_epoch
                    .store(epoch, Ordering::Release);
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Raw mutators preflight every fallible condition before publishing and
    /// preserve the page's structural invariants.  `Frame::mark_dirty`
    /// advances the epoch and invalidates the validation marker for generic
    /// page replacement, so restore it for this trusted in-place mutation.
    /// The CRC deliberately remains dirty until a full-image boundary.
    fn mark_raw_structure_validated(&self, frame: &Frame) {
        let epoch = frame.dirty_epoch.load(Ordering::Acquire);
        match &frame.key.file.kind {
            BufferPageFileKind::Index { .. } => frame
                .index_header_validated_epoch
                .store(epoch, Ordering::Release),
            BufferPageFileKind::Heap { .. } => {
                // A trusted heap raw mutator only publishes a validated epoch
                // after it has formatted or updated the page image.
                frame.heap_header_present.store(true, Ordering::Relaxed);
                frame
                    .heap_header_validated_epoch
                    .store(epoch, Ordering::Release);
            }
            _ => {}
        }
    }

    /// Pin `key`, mutate its 16 KiB image **in place** under the frame write
    /// lock (loading from disk on a miss), and mark it dirty at `dirty_lsn`.
    /// Unlike the read-clone + `store_page`-swap round trip, this never
    /// allocates or copies a page image — the closure edits the frame's bytes
    /// directly. Intended for callers that own their own external durability
    /// (WAL) or are explicitly deferring it (the vector index's bulk-build
    /// mode): no WAL record is emitted here, so a plain caller relying on the
    /// pool for crash recovery must NOT use this for logged updates.
    fn with_raw_page_mut<T>(
        self: &Arc<Self>,
        key: PageKey,
        dirty_lsn: u64,
        f: impl FnOnce(&mut [u8]) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        let pinned = self.pin_page_with_intent(key.clone(), AccessIntent::Normal)?;
        let result = {
            let mut bytes = pinned.frame.bytes.write().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer frame write lock poisoned",
                )
            })?;
            self.validate_frame_mutation_image(&pinned.frame, &mut bytes)?;
            let result = f(&mut bytes);
            if result.is_ok() {
                self.write_requests.fetch_add(1, Ordering::Relaxed);
                // Keep the formatted page header current immediately, not
                // only when the background flusher eventually finalizes the
                // checksum. Heap first-update-after-checkpoint protection
                // reads this header through `cached_heap_redo_state`; leaving it
                // stale made every eager raw mutation before the next flush
                // emit another full-page image for the same page. Do not
                // refresh the CRC here: the frame is marked checksum-dirty
                // below, so doing the full 16 KiB checksum on every raw
                // mutation would defeat the deferred-checksum fast path.
                // Deferred transaction writes pass lsn=0 and are
                // back-patched by `finalize_deferred` once their real WAL LSN
                // is known, so they must not erase the prior page LSN.
                if dirty_lsn != 0 {
                    set_page_lsn(&mut bytes, dirty_lsn);
                }
                // Publish checksum/epoch state before releasing the byte
                // write lock. A reader can therefore never observe mutated
                // bytes while still seeing `checksum_dirty=false`.
                pinned.frame.checksum_dirty.store(true, Ordering::Release);
                if pinned.frame.mark_dirty(dirty_lsn) {
                    self.dirty_pages.fetch_add(1, Ordering::Relaxed);
                    self.note_dirty(&pinned.frame.key, dirty_lsn);
                }
                self.mark_raw_structure_validated(&pinned.frame);
            }
            result
        };
        result
    }

    /// The closure must preflight before changing bytes and reports whether it
    /// actually published a raw-page modification.
    fn try_modify_raw_page<F, R>(
        self: &Arc<Self>,
        key: &PageKey,
        dirty_lsn: u64,
        f: F,
    ) -> Result<Option<R>, QueryError>
    where
        F: FnOnce(&mut [u8]) -> Result<(bool, R), QueryError>,
    {
        let Some(frame) = self.lookup_frame(key)? else {
            return Ok(None);
        };
        if !self.try_pin_existing(&frame, AccessIntent::Normal) {
            return Ok(None);
        }
        let result = (|| {
            self.wait_until_ready(&frame)?;
            let mut bytes = frame.bytes.write().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer frame write lock poisoned",
                )
            })?;
            self.validate_frame_mutation_image(&frame, &mut bytes)?;
            let (modified, value) = f(&mut bytes)?;
            if modified {
                self.write_requests.fetch_add(1, Ordering::Relaxed);
                frame.checksum_dirty.store(true, Ordering::Release);
                if btree_logical_wal::is_deferred() {
                    frame.deferred_pins.fetch_add(1, Ordering::AcqRel);
                    btree_logical_wal::push_deferred_key(frame.key.clone());
                }
                if frame.mark_dirty(dirty_lsn) {
                    self.dirty_pages.fetch_add(1, Ordering::Relaxed);
                    self.note_dirty(&frame.key, dirty_lsn);
                }
                self.mark_raw_structure_validated(&frame);
                frame.prefetch.store(false, Ordering::Release);
            }
            Ok(Some(value))
        })();
        frame.unpin();
        self.notify_frame_waiters();
        result
    }

    /// Atomically publish a fully logged replacement of a resident page.
    /// The callback sees whether the current dirty epoch has a nonzero redo
    /// anchor. A caller using dependent redo MUST emit an FPI whenever this
    /// flag is false, and anchor every new dirty epoch with that FPI.
    /// Holding the byte latch until mark_dirty prevents a concurrent flusher
    /// from clearing the anchor between selecting and publishing the WAL.
    /// The caller also holds its checkpoint publication barrier across this
    /// operation so a WAL append cannot precede a checkpoint's dirty snapshot
    /// while the corresponding dirty registration follows that snapshot.
    /// For ZoneMap files the third callback argument reports a validated
    /// input epoch, and the returned image must preserve validated structure
    /// (fresh encoding or a checked, size-preserving payload patch).
    fn try_replace_resident_logged_page(
        self: &Arc<Self>,
        key: &PageKey,
        build: impl FnOnce(&[u8], bool, bool) -> Result<(u64, Vec<u8>), QueryError>,
    ) -> Result<Option<u64>, QueryError> {
        let Some(frame) = self.lookup_frame(key)? else {
            return Ok(None);
        };
        if !self.try_pin_existing(&frame, AccessIntent::Normal) {
            return Ok(None);
        }
        let pinned = PinnedFrame {
            pool: Arc::clone(self),
            frame,
            intent: AccessIntent::Normal,
        };
        let _admission = crate::engine::executor::mem_budget::StorageLatchAdmissionScope::enter();
        self.wait_until_ready(&pinned.frame)?;
        let mut bytes = pinned.frame.bytes.write().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "buffer frame write lock poisoned",
            )
        })?;
        pinned.frame.materialize_packed_leaf(&mut bytes)?;
        if pinned.frame.checksum_dirty.load(Ordering::Acquire) {
            set_page_lsn_if_formatted(&mut bytes, pinned.frame.dirty_lsn.load(Ordering::Acquire));
            pinned.frame.checksum_dirty.store(false, Ordering::Release);
        }
        let anchored = pinned.frame.dirty.load(Ordering::Acquire)
            && pinned.frame.first_dirty_lsn.load(Ordering::Acquire) != 0;
        let zone_page = matches!(key.file.kind, BufferPageFileKind::ZoneMap { .. });
        let validated = zone_page
            && pinned
                .frame
                .index_header_validated_epoch
                .load(Ordering::Acquire)
                == pinned.frame.dirty_epoch.load(Ordering::Acquire);
        let (lsn, after) = build(&bytes, anchored, validated)?;
        if after.len() != PAGE_SIZE {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "logged page replacement has invalid size",
            ));
        }
        // Preserve the governed frame allocation; replacement scratch owns
        // its separate lease and is released after this copy.
        pinned.frame.invalidate_packed_leaf();
        bytes.copy_from_slice(&after);
        self.write_requests.fetch_add(1, Ordering::Relaxed);
        pinned.frame.checksum_dirty.store(false, Ordering::Release);
        if pinned.frame.mark_dirty(lsn) {
            self.dirty_pages.fetch_add(1, Ordering::Relaxed);
            self.note_dirty(&pinned.frame.key, lsn);
        }
        // Zone-map callers return a freshly encoded page or a size-preserving
        // payload patch of a validated page. Publish that structural proof
        // under the same byte latch, just as trusted heap/index mutators do.
        if zone_page {
            pinned.frame.index_header_validated_epoch.store(
                pinned.frame.dirty_epoch.load(Ordering::Acquire),
                Ordering::Release,
            );
        }
        pinned.frame.prefetch.store(false, Ordering::Release);
        Ok(Some(lsn))
    }

    /// Mutate a resident page through a pin already obtained by a preceding
    /// index traversal. The caller holds no byte lock across this call; the
    /// retained pin only prevents eviction, so mutation and dirty accounting
    /// are identical to `try_modify_raw_page` without a second shard lookup,
    /// Arc clone, pin CAS, and matching unpin notification.
    fn modify_pinned_raw_page<F, R>(
        self: &Arc<Self>,
        pinned: &PinnedFrame,
        dirty_lsn: u64,
        f: F,
    ) -> Result<R, QueryError>
    where
        F: FnOnce(&mut [u8]) -> Result<(bool, R), QueryError>,
    {
        debug_assert!(Arc::ptr_eq(self, &pinned.pool));
        self.wait_until_ready(&pinned.frame)?;
        let mut bytes = pinned.frame.bytes.write().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "buffer frame write lock poisoned",
            )
        })?;
        self.validate_frame_mutation_image(&pinned.frame, &mut bytes)?;
        let (modified, value) = f(&mut bytes)?;
        if modified {
            self.write_requests.fetch_add(1, Ordering::Relaxed);
            pinned.frame.checksum_dirty.store(true, Ordering::Release);
            if btree_logical_wal::is_deferred() {
                pinned.frame.deferred_pins.fetch_add(1, Ordering::AcqRel);
                btree_logical_wal::push_deferred_key(pinned.frame.key.clone());
            }
            if pinned.frame.mark_dirty(dirty_lsn) {
                self.dirty_pages.fetch_add(1, Ordering::Relaxed);
                self.note_dirty(&pinned.frame.key, dirty_lsn);
            }
            self.mark_raw_structure_validated(&pinned.frame);
            pinned.frame.prefetch.store(false, Ordering::Release);
        }
        Ok(value)
    }

    fn try_modify_two_raw_pages<F, R>(
        self: &Arc<Self>,
        first_key: &PageKey,
        second_key: &PageKey,
        dirty_lsn: u64,
        f: F,
    ) -> Result<Option<R>, QueryError>
    where
        F: FnOnce(&mut [u8], &mut [u8]) -> Result<(bool, R), QueryError>,
    {
        if first_key == second_key || first_key.file != second_key.file {
            return Ok(None);
        }
        let Some(first_frame) = self.lookup_frame(first_key)? else {
            return Ok(None);
        };
        let Some(second_frame) = self.lookup_frame(second_key)? else {
            return Ok(None);
        };
        let first_before_second = first_key.page_id < second_key.page_id;
        let (low, high) = if first_before_second {
            (&first_frame, &second_frame)
        } else {
            (&second_frame, &first_frame)
        };
        if !self.try_pin_existing(low, AccessIntent::Normal) {
            return Ok(None);
        }
        if !self.try_pin_existing(high, AccessIntent::Normal) {
            low.unpin();
            self.notify_frame_waiters();
            return Ok(None);
        }
        let result = (|| {
            self.wait_until_ready(low)?;
            self.wait_until_ready(high)?;
            let mut low_bytes = low.bytes.write().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer frame write lock poisoned",
                )
            })?;
            let mut high_bytes = high.bytes.write().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer frame write lock poisoned",
                )
            })?;
            self.validate_frame_mutation_image(low, &mut low_bytes)?;
            self.validate_frame_mutation_image(high, &mut high_bytes)?;
            let (modified, value) = if first_before_second {
                f(&mut low_bytes, &mut high_bytes)?
            } else {
                f(&mut high_bytes, &mut low_bytes)?
            };
            if modified {
                self.write_requests.fetch_add(2, Ordering::Relaxed);
                for frame in [&first_frame, &second_frame] {
                    frame.checksum_dirty.store(true, Ordering::Release);
                    if btree_logical_wal::is_deferred() {
                        frame.deferred_pins.fetch_add(1, Ordering::AcqRel);
                        btree_logical_wal::push_deferred_key(frame.key.clone());
                    }
                    if frame.mark_dirty(dirty_lsn) {
                        self.dirty_pages.fetch_add(1, Ordering::Relaxed);
                        self.note_dirty(&frame.key, dirty_lsn);
                    }
                    self.mark_raw_structure_validated(frame);
                    frame.prefetch.store(false, Ordering::Release);
                }
            }
            Ok(Some(value))
        })();
        high.unpin();
        low.unpin();
        self.notify_frame_waiters();
        result
    }

    fn pin_page(self: &Arc<Self>, key: PageKey) -> Result<PinnedFrame, QueryError> {
        self.pin_page_with_intent(key, AccessIntent::Normal)
    }

    /// An optional navigation cache must not hold ancestor pins across page
    /// admission or I/O waits. Claim an already resident, ready frame only;
    /// a racing eviction/load is a cache miss, never a new page allocation.
    fn try_pin_resident_page(
        self: &Arc<Self>,
        key: &PageKey,
    ) -> Result<Option<PinnedFrame>, QueryError> {
        let Some(frame) = self.lookup_frame(key)? else {
            return Ok(None);
        };
        if !self.try_pin_existing(&frame, AccessIntent::Normal) {
            return Ok(None);
        }
        if !matches!(
            FrameIoState::from_u8(frame.state.load(Ordering::Acquire)),
            FrameIoState::Ready | FrameIoState::Flushing
        ) {
            frame.unpin();
            self.notify_frame_waiters();
            return Ok(None);
        }
        let prefetch_hit =
            frame.prefetch.load(Ordering::Relaxed) && frame.prefetch.swap(false, Ordering::Relaxed);
        frame.touch_and_small_scan_prior(AccessIntent::Normal);
        if frame.scan_origin.load(Ordering::Relaxed) {
            self.promote_scan_origin(&frame);
        }
        record_query_buffer_hit();
        self.shards[self.shard_index(key)]
            .hits
            .fetch_add(1, Ordering::Relaxed);
        if prefetch_hit {
            self.prefetch_hits.fetch_add(1, Ordering::Relaxed);
            self.record_prefetch_lead(&frame);
        }
        Ok(Some(PinnedFrame {
            pool: self.clone(),
            frame,
            intent: AccessIntent::Normal,
        }))
    }

    fn pin_page_with_intent(
        self: &Arc<Self>,
        key: PageKey,
        intent: AccessIntent,
    ) -> Result<PinnedFrame, QueryError> {
        self.pin_page_with_candidate(key, intent, None)
    }

    /// A weak hint owns no page bytes and never pins a page between calls.
    /// Discard/eviction publishes a non-ready state before recycling bytes;
    /// the ordinary pin CAS/state checks below close races after this probe.
    #[cfg(test)]
    fn pin_page_with_hint(
        self: &Arc<Self>,
        key: PageKey,
        hint: &parking_lot::RwLock<std::sync::Weak<Frame>>,
    ) -> Result<PinnedFrame, QueryError> {
        let frame = self.pin_frame_with_hint(key, hint)?;
        Ok(PinnedFrame {
            pool: Arc::clone(self),
            frame,
            intent: AccessIntent::Normal,
        })
    }

    fn pin_frame_with_hint(
        self: &Arc<Self>,
        key: PageKey,
        hint: &parking_lot::RwLock<std::sync::Weak<Frame>>,
    ) -> Result<Arc<Frame>, QueryError> {
        let candidate = hint.read().upgrade().filter(|frame| {
            frame.key.page_id == key.page_id
                && (Arc::ptr_eq(&frame.key.file, &key.file) || frame.key.file == key.file)
                && frame.is_ready()
        });
        let prior = candidate.as_ref().map(Arc::as_ptr);
        let pinned = self.pin_frame_with_candidate(key, AccessIntent::Normal, candidate)?;
        if prior != Some(Arc::as_ptr(&pinned)) {
            *hint.write() = Arc::downgrade(&pinned);
        }
        Ok(pinned)
    }

    fn pin_page_with_candidate(
        self: &Arc<Self>,
        key: PageKey,
        intent: AccessIntent,
        candidate: Option<Arc<Frame>>,
    ) -> Result<PinnedFrame, QueryError> {
        let frame = self.pin_frame_with_candidate(key, intent, candidate)?;
        Ok(PinnedFrame {
            pool: Arc::clone(self),
            frame,
            intent,
        })
    }

    /// The caller's borrow keeps the pool alive; only the frame needs shared
    /// ownership. Pin acquisition, replacement policy and wakeups are identical
    /// to the owning guard, including on cold loads and errors.
    fn pin_page_borrowed(self: &Arc<Self>, key: PageKey) -> Result<PinnedFrame<&Self>, QueryError> {
        let frame = self.pin_frame_with_candidate(key, AccessIntent::Normal, None)?;
        Ok(PinnedFrame {
            pool: self.as_ref(),
            frame,
            intent: AccessIntent::Normal,
        })
    }

    fn pin_frame_with_candidate(
        self: &Arc<Self>,
        key: PageKey,
        intent: AccessIntent,
        mut candidate: Option<Arc<Frame>>,
    ) -> Result<Arc<Frame>, QueryError> {
        let shard_idx = self.shard_index(&key);
        loop {
            let frame = match candidate.take() {
                Some(frame) => Some(frame),
                None => self.lookup_frame(&key)?,
            };
            if let Some(frame) = frame {
                if self.try_pin_existing(&frame, intent) {
                    // C7 fast-path - in the SF=1 / hot-cache common case
                    // the frame is already Ready, has no pending prefetch
                    // flag, and isn't scan-origin. Probe those atomics
                    // with Relaxed loads before paying for an AcqRel
                    // swap / RMW. This removes 2-3 unconditional atomic
                    // writes per cache hit on a 99% hit-rate workload.
                    let state = FrameIoState::from_u8(frame.state.load(Ordering::Acquire));
                    match state {
                        FrameIoState::Ready | FrameIoState::Flushing => {}
                        FrameIoState::Discarded | FrameIoState::Evicting => {
                            frame.unpin();
                            self.notify_frame_waiters();
                            return Err(error(
                                crate::error::catalog::ER_GET_ERRNO,
                                "buffer frame was evicted during load",
                            ));
                        }
                        FrameIoState::Loading => {
                            if frame.prefetch.load(Ordering::Relaxed) {
                                self.prefetch_demand_waits.fetch_add(1, Ordering::Relaxed);
                            }
                            if let Err(err) = self.wait_until_ready(&frame) {
                                let invalidated = matches!(
                                    FrameIoState::from_u8(frame.state.load(Ordering::Acquire)),
                                    FrameIoState::Discarded | FrameIoState::Evicting
                                );
                                frame.unpin();
                                self.notify_frame_waiters();
                                if invalidated {
                                    continue;
                                }
                                return Err(err);
                            }
                        }
                    }
                    let prefetch_hit = if frame.prefetch.load(Ordering::Relaxed) {
                        // Only frames materialised by the prefetch worker
                        // ever set this bit; the swap path remains
                        // single-winner via AcqRel.
                        frame.prefetch.swap(false, Ordering::AcqRel)
                    } else {
                        false
                    };
                    let small_scan_prior = frame.touch_and_small_scan_prior(intent);
                    if intent == AccessIntent::SmallScan {
                        self.record_small_scan_touch(&frame, small_scan_prior);
                    } else if matches!(intent, AccessIntent::Normal | AccessIntent::Probe)
                        && frame.scan_origin.load(Ordering::Relaxed)
                    {
                        // N-2 - promote scan/prefetch-origin frames
                        // into the working set as soon as a real
                        // request lands on them. The Relaxed pre-check
                        // skips the function call in the steady state
                        // where the flag is already cleared.
                        self.promote_scan_origin(&frame);
                    }
                    if intent != AccessIntent::Prefetch {
                        record_query_buffer_hit();
                        self.shards[shard_idx].hits.fetch_add(1, Ordering::Relaxed);
                        if prefetch_hit {
                            self.prefetch_hits.fetch_add(1, Ordering::Relaxed);
                            self.record_prefetch_lead(&frame);
                        }
                    }
                    return Ok(frame);
                }
                self.wait_for_frame_release(Duration::from_millis(1));
                continue;
            }

            self.ensure_capacity_for_insert(self.lock_wait)?;
            // One map lookup serves both the pre-load generation stamp
            // and the post-load re-check below.
            let generation_cell = self.file_generation_cell(&key.file)?;
            let raw_page = self.claim_raw_page_lease(shard_idx)?;
            let load_generation = generation_cell.load(Ordering::Acquire);
            let frame = Arc::new(Frame::new_loading(
                key.clone(),
                load_generation,
                intent,
                raw_page,
            ));
            let mut shard = self.shards[shard_idx].frames.write().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer pool shard lock poisoned",
                )
            })?;
            if let Some(existing) = shard.get(&key).cloned() {
                drop(shard);
                self.recycle_frame_raw_page(shard_idx, &frame);
                if self.try_pin_existing(&existing, intent) {
                    if !existing.is_ready() && existing.prefetch.load(Ordering::Relaxed) {
                        self.prefetch_demand_waits.fetch_add(1, Ordering::Relaxed);
                    }
                    if let Err(err) = self.wait_until_ready(&existing) {
                        let invalidated = matches!(
                            FrameIoState::from_u8(existing.state.load(Ordering::Acquire)),
                            FrameIoState::Discarded | FrameIoState::Evicting
                        );
                        existing.unpin();
                        self.notify_frame_waiters();
                        if invalidated {
                            continue;
                        }
                        return Err(err);
                    }
                    let prefetch_hit = existing.prefetch.swap(false, Ordering::AcqRel);
                    let small_scan_prior = existing.touch_and_small_scan_prior(intent);
                    if intent == AccessIntent::SmallScan {
                        self.record_small_scan_touch(&existing, small_scan_prior);
                    } else if matches!(intent, AccessIntent::Normal | AccessIntent::Probe) {
                        self.promote_scan_origin(&existing);
                    }
                    if intent != AccessIntent::Prefetch {
                        record_query_buffer_hit();
                        self.shards[shard_idx].hits.fetch_add(1, Ordering::Relaxed);
                        if prefetch_hit {
                            self.prefetch_hits.fetch_add(1, Ordering::Relaxed);
                            self.record_prefetch_lead(&existing);
                        }
                    }
                    return Ok(existing);
                }
                self.wait_for_frame_release(Duration::from_millis(1));
                continue;
            }
            shard.insert(key.clone(), frame.clone());
            self.used_pages.fetch_add(1, Ordering::Relaxed);
            self.note_kind_added(&key);
            if matches!(
                intent,
                AccessIntent::Scan
                    | AccessIntent::SmallScan
                    | AccessIntent::Prefetch
                    | AccessIntent::Probe
            ) {
                self.observe_new_scan_origin(shard_idx, &frame);
            }
            drop(shard);
            self.push_clock_key(shard_idx, key.clone())?;
            let load_result = {
                let mut frame_bytes = frame.bytes.write().map_err(|_| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "buffer frame write lock poisoned",
                    )
                })?;
                self.load_page_bytes_into(&key, intent, &mut frame_bytes)
            };
            match load_result {
                Ok(()) => {}
                Err(err) => {
                    frame.publish_io_state(FrameIoState::Discarded);
                    frame.unpin();
                    self.remove_frame_if_same(&frame)?;
                    self.notify_frame_waiters();
                    return Err(err);
                }
            }
            // A physical-replication apply, truncate, rename, or DROP may
            // invalidate/remove this frame while pread is in flight. Never
            // publish bytes read under the old generation, and never overwrite
            // a concurrent Discarded state with Ready. The batch-load path has
            // the same generation fence.
            if !self.loaded_frame_is_current(
                shard_idx,
                &key,
                &frame,
                &generation_cell,
                load_generation,
            )? {
                frame.publish_io_state(FrameIoState::Discarded);
                frame.unpin();
                self.remove_frame_if_same(&frame)?;
                self.notify_frame_waiters();
                continue;
            }
            frame.generation.store(load_generation, Ordering::Release);
            frame.publish_io_state(FrameIoState::Ready);
            if intent == AccessIntent::SmallScan {
                self.small_scan_accesses.fetch_add(1, Ordering::Relaxed);
                self.small_scan_cold_accesses
                    .fetch_add(1, Ordering::Relaxed);
            }
            if intent == AccessIntent::Prefetch {
                frame
                    .prefetch_loaded_at_us
                    .store(self.now_us(), Ordering::Release);
                self.prefetch_completed.fetch_add(1, Ordering::Relaxed);
            }
            // `new_loading` seeded the replacement priority for this first
            // access.  Touching again here would count the physical load as a
            // cache hit as well: in particular, a first Probe would be
            // promoted immediately instead of only after a second lookup.
            if intent != AccessIntent::Prefetch {
                self.misses.fetch_add(1, Ordering::Relaxed);
                record_query_buffer_miss();
                self.shards[shard_idx]
                    .misses
                    .fetch_add(1, Ordering::Relaxed);
            }
            self.notify_frame_waiters();
            if let Err(err) = self.ensure_capacity(self.lock_wait) {
                frame.unpin();
                self.remove_frame_if_same(&frame)?;
                self.notify_frame_waiters();
                return Err(err);
            }
            return Ok(frame);
        }
    }

    fn with_cached_bytes<T>(
        &self,
        key: &PageKey,
        f: impl FnOnce(&[u8]) -> Result<T, QueryError>,
    ) -> Result<Option<T>, QueryError> {
        self.with_cached_bytes_with_intent(key, AccessIntent::Normal, f)
    }

    /// A speculative leaf-chain probe must not promote unconsumed pages or
    /// charge a demand hit. Missing/loading pages remain asynchronous hints.
    fn with_cached_prefetch_bytes<T>(
        &self,
        key: &PageKey,
        f: impl FnOnce(&[u8]) -> Result<T, QueryError>,
    ) -> Result<Option<T>, QueryError> {
        self.with_cached_bytes_with_intent(key, AccessIntent::Prefetch, f)
    }

    fn with_cached_bytes_with_intent<T>(
        &self,
        key: &PageKey,
        intent: AccessIntent,
        f: impl FnOnce(&[u8]) -> Result<T, QueryError>,
    ) -> Result<Option<T>, QueryError> {
        let Some(frame) = self.lookup_frame(key)? else {
            return Ok(None);
        };
        if !self.try_pin_existing(&frame, intent) {
            return Ok(None);
        }
        struct UnpinOnDrop<'a>(&'a BufferPool, &'a Frame);
        impl Drop for UnpinOnDrop<'_> {
            fn drop(&mut self) {
                self.1.unpin();
                self.0.notify_frame_waiters();
            }
        }
        let _pin = UnpinOnDrop(self, &frame);
        if !frame.is_ready() {
            return Ok(None);
        }
        if intent == AccessIntent::Normal {
            self.note_normal_access(&frame);
        }
        let result = self.with_finalized_frame_bytes(&frame, f);
        result.map(Some)
    }

    /// Visit an already-resident *dirty* heap page's byte image without
    /// admitting a cache miss. Direct range scans read clean pages from disk
    /// in large contiguous runs and only overlay pages whose current image is
    /// not durable yet. Treating every resident clean page as an overlay used
    /// to fragment a 16 MiB pread around random probe leftovers, turning a
    /// sequential scan into thousands of tiny reads after join-heavy queries.
    ///
    /// Writers publish `dirty` while holding `bytes` for write, and flushers
    /// clear it only after the write completes while holding `bytes` for read.
    /// Checking it under the same read lock therefore makes a clean result a
    /// safe promise that the disk image is current. `AccessIntent::Scan` keeps
    /// the one-shot pass from promoting dirty residents into the hot set.
    fn with_cached_dirty_heap_bytes_for_scan<T>(
        &self,
        key: &PageKey,
        f: impl FnOnce(&[u8]) -> Result<T, QueryError>,
    ) -> Result<Option<T>, QueryError> {
        let Some(frame) = self.lookup_frame(key)? else {
            return Ok(None);
        };
        if !self.try_pin_existing(&frame, AccessIntent::Scan) {
            return Ok(None);
        }
        if !frame.is_ready() {
            frame.unpin();
            self.notify_frame_waiters();
            return Ok(None);
        }
        frame.touch(AccessIntent::Scan);
        let result = {
            let bytes = frame.bytes.read().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer frame read lock poisoned",
                )
            })?;
            if !frame.dirty.load(Ordering::Acquire) {
                None
            } else if !frame.checksum_dirty.load(Ordering::Acquire) {
                Some(f(&bytes))
            } else {
                drop(bytes);
                Some(self.with_finalized_frame_bytes(&frame, f))
            }
        };
        frame.unpin();
        self.notify_frame_waiters();
        match result {
            Some(result) => result.map(Some),
            None => Ok(None),
        }
    }

    /// Bounded sparse one-pass reads reuse the scan reader's dirty overlay.
    /// They do not duplicate cold pages in both raw buffers and the OS cache.
    fn with_raw_probe_pages(
        self: &Arc<Self>,
        file: &Arc<BufferPageFileKey>,
        pages: &[u64],
        mut visit: impl FnMut(u64, &[u8]) -> Result<(), QueryError>,
    ) -> Result<bool, QueryError> {
        if pages.len() < 8
            || pages.len() > 64
            || self.durability == DurabilityMode::Memory
            || crate::engine::executor::mem_budget::critical_memory_reservations_active()
        {
            return Ok(false);
        }
        let Some(charge) =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                pages
                    .len()
                    .saturating_mul(PAGE_SIZE + 4095 + 64)
                    .saturating_add(256),
            )
        else {
            return Ok(false);
        };
        self.open_cached_file(file)?;
        let work = Arc::new(RawProbeWork {
            file: Arc::clone(file),
            pages: pages
                .iter()
                .map(|&id| {
                    let bytes = vec![0u8; PAGE_SIZE + 4095];
                    let start = bytes.as_ptr().align_offset(4096);
                    (id, start, parking_lot::Mutex::new(bytes))
                })
                .collect(),
            next: AtomicUsize::new(0),
            state: parking_lot::Mutex::new((pages.len(), None)),
            done: parking_lot::Condvar::new(),
            _charge: charge,
        });
        let mut active = self.ordered_read_helpers.load(Ordering::Acquire);
        let helpers = loop {
            let count = self
                .io_threads
                .saturating_sub(1)
                .min(pages.len() - 1)
                .min(self.io_threads.saturating_sub(active));
            match self.ordered_read_helpers.compare_exchange_weak(
                active,
                active + count,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break count,
                Err(now) => active = now,
            }
        };
        for _ in 0..helpers {
            let pool = Arc::clone(self);
            let work = Arc::clone(&work);
            drop(crate::util::blocking_pool::spawn(move || {
                struct Release<'a>(&'a AtomicUsize);
                impl Drop for Release<'_> {
                    fn drop(&mut self) {
                        self.0.fetch_sub(1, Ordering::Release);
                    }
                }
                let _slot = Release(&pool.ordered_read_helpers);
                pool.run_raw_probe_work(&work);
            }));
        }
        self.run_raw_probe_work(&work);
        let mut state = work.state.lock();
        while state.0 != 0 {
            work.done.wait(&mut state);
        }
        if let Some(error) = state.1.take() {
            return Err(error);
        }
        drop(state);
        for (id, start, bytes) in &work.pages {
            visit(*id, &bytes.lock()[*start..*start + PAGE_SIZE])?;
        }
        Ok(true)
    }

    fn run_raw_probe_work(&self, work: &RawProbeWork) {
        loop {
            let next = work.next.fetch_add(1, Ordering::Relaxed);
            let Some((id, start, bytes)) = work.pages.get(next) else {
                return;
            };
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.read_raw_scan_batch_inner(
                    &work.file,
                    *id,
                    &mut bytes.lock()[*start..*start + PAGE_SIZE],
                    true,
                )
            }))
            .unwrap_or_else(|_| {
                Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "raw probe worker panicked",
                ))
            });
            let mut state = work.state.lock();
            if state.1.is_none() {
                state.1 = result.err();
            }
            state.0 -= 1;
            if state.0 == 0 {
                work.done.notify_all();
            }
        }
    }

    /// Read a bounded sequential run without admitting cold pages. Copy dirty
    /// residents before reading clean gaps so unflushed WAL-backed images are
    /// never replaced by stale disk data.
    fn read_raw_scan_batch(
        &self,
        file_key: &Arc<BufferPageFileKey>,
        first: u64,
        bytes: &mut [u8],
    ) -> Result<(), QueryError> {
        self.read_raw_scan_batch_inner(file_key, first, bytes, false)
    }

    fn read_raw_scan_batch_inner(
        &self,
        file_key: &Arc<BufferPageFileKey>,
        first: u64,
        bytes: &mut [u8],
        sparse: bool,
    ) -> Result<(), QueryError> {
        assert!(bytes.len().is_multiple_of(PAGE_SIZE));
        let count = bytes.len() / PAGE_SIZE;
        assert!(count <= 64);
        let mut cached = 0u64;
        for (position, page) in bytes.chunks_exact_mut(PAGE_SIZE).enumerate() {
            let key = PageKey {
                file: file_key.clone(),
                page_id: first + position as u64,
            };
            if self
                .with_cached_dirty_heap_bytes_for_scan(&key, |image| {
                    page.copy_from_slice(image);
                    Ok(())
                })?
                .is_some()
            {
                cached |= 1u64 << position;
            }
        }
        let file = self.open_cached_file(file_key)?;
        let mut position = 0;
        while position < count {
            if cached & (1u64 << position) != 0 {
                position += 1;
                continue;
            }
            let start = position;
            while position < count && cached & (1u64 << position) == 0 {
                position += 1;
            }
            let target = &mut bytes[start * PAGE_SIZE..position * PAGE_SIZE];
            let offset = (first + start as u64) * PAGE_SIZE as u64;
            let calls = if sparse {
                read_cached_page_at(&file, target, offset)
            } else {
                read_exact_at_counted(&file.file, target, offset)
            }
            .map_err(io_err)?;
            let pages = (position - start) as u64;
            self.read_ios.fetch_add(pages, Ordering::Relaxed);
            self.kind_read_ios[file_key.kind.slot()].fetch_add(pages, Ordering::Relaxed);
            self.read_ios_scan.fetch_add(pages, Ordering::Relaxed);
            self.read_syscalls.fetch_add(calls, Ordering::Relaxed);
            self.batch_read_pages.fetch_add(pages, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Borrow an already-resident heap page directly from its authoritative
    /// raw image. Header/checksum validation is cached by dirty epoch, so the
    /// steady-state lookup parses only the requested slot metadata and never
    /// allocates a 16 KiB `Page` copy.
    fn with_cached_heap_view<T>(
        &self,
        key: &PageKey,
        verified_checksums: Option<&VerifiedDirectHeapChecksums>,
        f: impl FnOnce(Option<HeapPageView<'_>>) -> Result<T, QueryError>,
    ) -> Result<Option<T>, QueryError> {
        let Some(frame) = self.lookup_frame(key)? else {
            return Ok(None);
        };
        if !self.try_pin_existing(&frame, AccessIntent::Normal) {
            return Ok(None);
        }
        // The page validator and visitor may fail or unwind. Keep pin release
        // independent of every return path, without cloning the pool Arc.
        struct UnpinOnDrop<'a> {
            pool: &'a BufferPool,
            frame: &'a Frame,
        }
        impl Drop for UnpinOnDrop<'_> {
            fn drop(&mut self) {
                self.frame.unpin();
                self.pool.notify_frame_waiters();
            }
        }
        let _pin = UnpinOnDrop {
            pool: self,
            frame: &frame,
        };
        if !frame.is_ready() {
            return Ok(None);
        }
        self.note_normal_access(&frame);
        let result = {
            let bytes = frame.bytes.read().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer frame read lock poisoned",
                )
            })?;
            let present = Self::validated_heap_page_present_with_cache(
                &frame,
                &bytes,
                verified_checksums,
                key.page_id,
            )?;
            f(present.then(|| HeapPageView::from_validated(&bytes)))
        };
        result.map(Some)
    }

    #[inline]
    fn validated_heap_page_present_with_cache(
        frame: &Frame,
        bytes: &[u8],
        verified_checksums: Option<&VerifiedDirectHeapChecksums>,
        page_id: u64,
    ) -> Result<bool, QueryError> {
        let epoch = frame.dirty_epoch.load(Ordering::Acquire);
        if frame.heap_header_validated_epoch.load(Ordering::Acquire) == epoch {
            return Ok(frame.heap_header_present.load(Ordering::Relaxed));
        }
        let present = if frame.checksum_dirty.load(Ordering::Acquire) {
            validate_heap_page_structure(bytes)?
        } else {
            validate_heap_page_header_with_cache(bytes, page_id, verified_checksums)?
        };
        // Publish the payload before the Release epoch marker; readers that
        // acquire the matching epoch can then consume the cached boolean.
        frame.heap_header_present.store(present, Ordering::Relaxed);
        frame
            .heap_header_validated_epoch
            .store(epoch, Ordering::Release);
        Ok(present)
    }

    /// Decode an already-resident index page into an operation-local oracle.
    /// The result is never attached to the frame.
    #[cfg(test)]
    fn cached_index_page(&self, key: &PageKey) -> Result<Option<Arc<IndexPage>>, QueryError> {
        let Some(frame) = self.lookup_frame(key)? else {
            return Ok(None);
        };
        if !self.try_pin_existing(&frame, AccessIntent::Normal) {
            return Ok(None);
        }
        if !frame.is_ready() {
            frame.unpin();
            self.notify_frame_waiters();
            return Ok(None);
        }
        self.note_normal_access(&frame);
        let result = self.index_page_for_read(&frame);
        frame.unpin();
        self.notify_frame_waiters();
        result.map(Some)
    }

    /// Read the LSN of the most recent successful checkpoint as seen
    /// by this buffer pool. Used by the heap-side first-update FPI
    /// predicate; see `last_checkpoint_lsn` field doc for the
    /// rationale.
    pub(crate) fn last_checkpoint_lsn(&self) -> u64 {
        self.last_checkpoint_lsn.load(Ordering::Acquire)
    }

    /// Publish the checkpoint LSN. Called from `Database::checkpoint_locked`
    /// after CheckpointEnd is durable so the heap FPI gate can
    /// retire pages that have already been WAL-touched in the new
    /// checkpoint window.
    pub(crate) fn set_last_checkpoint_lsn(&self, lsn: u64) {
        self.last_checkpoint_lsn.fetch_max(lsn, Ordering::AcqRel);
    }

    /// Return the recovery-relevant dirty-page summary without cloning page
    /// keys, paths, or frame Arcs. Recovery only needs the oldest rec_lsn;
    /// serialising the full DPT made checkpoint memory and WAL bytes grow with
    /// the dirty working set for no semantic benefit.
    ///
    /// Lock order is `frames -> dirty_set`, matching dirty publication and
    /// discard. Other dirty-set readers release that latch before acquiring
    /// `frames`, so this cannot form the inverse edge. A frame with a deferred
    /// WAL LSN is counted as dirty even while its rec_lsn is zero; the active
    /// writer floor captured by Database covers its pre-checkpoint WAL.
    pub(crate) fn checkpoint_dirty_snapshot(&self) -> Result<(Option<u64>, u64), QueryError> {
        let mut oldest = None::<u64>;
        let mut dirty_count = 0_u64;
        for shard in &self.shards {
            let frames = shard.frames.read().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer pool shard lock poisoned",
                )
            })?;
            let dirty = shard.dirty_set.lock().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer pool dirty-set lock poisoned",
                )
            })?;
            for key in dirty.iter() {
                let Some(frame) = frames.get(key) else {
                    continue;
                };
                if !frame.dirty.load(Ordering::Acquire) {
                    continue;
                }
                dirty_count = dirty_count.saturating_add(1);
                let first = frame.first_dirty_lsn.load(Ordering::Acquire);
                let rec_lsn = if first == 0 {
                    frame.dirty_lsn.load(Ordering::Acquire)
                } else {
                    first
                };
                if rec_lsn != 0 {
                    oldest = Some(oldest.map_or(rec_lsn, |current| current.min(rec_lsn)));
                }
            }
        }
        Ok((oldest, dirty_count))
    }

    /// Snapshot the page's currently-cached raw bytes from the buffer
    /// pool without disturbing the frame's pin / dirty state. Used by
    /// the heap-side first-update-after-checkpoint FPI emitter to
    /// produce a "before" image of the page right before the caller
    /// publishes its raw mutation.
    ///
    /// Returns `Ok(None)` when the frame isn't resident — this is the
    /// "brand-new page that was just `set_len`-extended and is not
    /// yet in the buffer pool" case, which has no on-disk image to be
    /// torn by a partial write and therefore does not need an FPI.
    pub(crate) fn read_page_bytes(&self, key: &PageKey) -> Result<Option<Vec<u8>>, QueryError> {
        let Some(frame) = self.lookup_frame(key)? else {
            return Ok(None);
        };
        if !self.try_pin_existing(&frame, AccessIntent::Normal) {
            return Ok(None);
        }
        let result = (|| {
            if !frame.is_ready() {
                return Ok(None);
            }
            let bytes = frame.read_materialized_index_bytes()?;
            Ok(Some(bytes.clone()))
        })();
        frame.unpin();
        self.notify_frame_waiters();
        result
    }

    /// Operation-local compatibility path for page-wide transformations.
    /// Its temporary oracle is charged to `StorageScratch` and never retained.
    pub(crate) fn modify_index_page_fallback<F, R>(
        self: &Arc<Self>,
        key: &PageKey,
        dirty_lsn: u64,
        f: F,
    ) -> Result<Option<R>, QueryError>
    where
        F: FnOnce(&mut IndexPage) -> Result<(bool, R), QueryError>,
    {
        let _scratch = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
            PAGE_SIZE,
        )?;
        self.try_modify_raw_page(key, dirty_lsn, |bytes| {
            // A preceding raw mutation may have deferred the CRC. This rare
            // compatibility path uses the checksum-validating oracle decoder.
            refresh_page_checksum(bytes);
            let mut page = decode_index_tree_page(bytes)?;
            let (modified, value) = f(&mut page)?;
            if modified {
                let mut encoded = encode_index_tree_page(&page)?;
                set_page_lsn_if_formatted(&mut encoded, dirty_lsn);
                bytes.copy_from_slice(&encoded);
            }
            Ok((modified, value))
        })
    }

    /// Operation-local two-page compatibility path. Frames are pinned and
    /// locked in page-id order to prevent AB/BA deadlocks.
    pub(crate) fn modify_two_index_pages_fallback<F, R>(
        self: &Arc<Self>,
        first_key: &PageKey,
        second_key: &PageKey,
        dirty_lsn: u64,
        f: F,
    ) -> Result<Option<R>, QueryError>
    where
        F: FnOnce(&mut IndexPage, &mut IndexPage) -> Result<(bool, R), QueryError>,
    {
        let _scratch = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
            PAGE_SIZE.saturating_mul(2),
        )?;
        self.try_modify_two_raw_pages(
            first_key,
            second_key,
            dirty_lsn,
            |first_bytes, second_bytes| {
                refresh_page_checksum(first_bytes);
                refresh_page_checksum(second_bytes);
                let mut first_page = decode_index_tree_page(first_bytes)?;
                let mut second_page = decode_index_tree_page(second_bytes)?;
                let (modified, value) = f(&mut first_page, &mut second_page)?;
                if modified {
                    let mut first_encoded = encode_index_tree_page(&first_page)?;
                    let mut second_encoded = encode_index_tree_page(&second_page)?;
                    set_page_lsn_if_formatted(&mut first_encoded, dirty_lsn);
                    set_page_lsn_if_formatted(&mut second_encoded, dirty_lsn);
                    first_bytes.copy_from_slice(&first_encoded);
                    second_bytes.copy_from_slice(&second_encoded);
                }
                Ok((modified, value))
            },
        )
    }

    fn get_cached_bytes(&self, key: &PageKey) -> Result<Option<Vec<u8>>, QueryError> {
        self.with_cached_bytes(key, |bytes| Ok(bytes.to_vec()))
    }

    /// Read only the common formatted-page LSN from a resident frame. This is
    /// safe while `checksum_dirty` is set because raw mutators never move the
    /// fixed common header. It lets FPI gates avoid finalizing/copying 16 KiB
    /// when the page is already covered by the checkpoint window.
    #[cfg(test)]
    fn cached_page_lsn(&self, key: &PageKey) -> Result<Option<u64>, QueryError> {
        self.cached_heap_redo_state(key)
            .map(|state| state.map(|(lsn, _)| lsn))
    }

    fn cached_heap_redo_state(&self, key: &PageKey) -> Result<Option<(u64, bool)>, QueryError> {
        let Some(frame) = self.lookup_frame(key)? else {
            return Ok(None);
        };
        if !self.try_pin_existing(&frame, AccessIntent::Normal) {
            return Ok(None);
        }
        if !frame.is_ready() {
            frame.unpin();
            self.notify_frame_waiters();
            return Ok(None);
        }
        let result = {
            let bytes = frame.bytes.read().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer frame read lock poisoned",
                )
            })?;
            Ok((
                page_lsn(&bytes),
                read_u32(&bytes, 0) == HEAP_PAGE_MAGIC
                    && read_u16(&bytes, 6) & HEAP_PAGE_REDUCED_REDO != 0,
            ))
        };
        frame.unpin();
        self.notify_frame_waiters();
        result.map(Some)
    }

    fn store_page(
        self: &Arc<Self>,
        key: PageKey,
        bytes: Vec<u8>,
        dirty_lsn: u64,
    ) -> Result<(), QueryError> {
        self.store_page_with_admission(key, bytes, dirty_lsn, AccessIntent::Normal)
    }

    /// Admit a freshly bulk-built page at the cold end of the replacement
    /// policy. A later demand lookup promotes it exactly like a scan-origin
    /// page. This is only a residency hint: WAL, dirty tracking, checksums,
    /// and the on-disk page image are identical to `store_page`.
    fn store_page_cold(
        self: &Arc<Self>,
        key: PageKey,
        bytes: Vec<u8>,
        dirty_lsn: u64,
    ) -> Result<(), QueryError> {
        self.store_page_with_admission(key, bytes, dirty_lsn, AccessIntent::Scan)
    }

    fn store_page_with_admission(
        self: &Arc<Self>,
        key: PageKey,
        mut bytes: Vec<u8>,
        dirty_lsn: u64,
        admission: AccessIntent,
    ) -> Result<(), QueryError> {
        if bytes.len() != PAGE_SIZE {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "buffer pool page image has invalid size",
            ));
        }
        loop {
            if let Some(frame) = self.lookup_frame(&key)? {
                if !self.try_pin_existing(&frame, AccessIntent::Normal) {
                    self.wait_for_frame_release(Duration::from_millis(1));
                    continue;
                }
                let result = (|| {
                    self.wait_until_ready(&frame)?;
                    let mut frame_bytes = frame.bytes.write().map_err(|_| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "buffer frame write lock poisoned",
                        )
                    })?;
                    frame.invalidate_packed_leaf();
                    std::mem::swap(&mut *frame_bytes, &mut bytes);
                    self.write_requests.fetch_add(1, Ordering::Relaxed);
                    frame.checksum_dirty.store(false, Ordering::Release);
                    if btree_logical_wal::is_deferred() {
                        frame.deferred_pins.fetch_add(1, Ordering::AcqRel);
                        btree_logical_wal::push_deferred_key(frame.key.clone());
                    }
                    if frame.mark_dirty(dirty_lsn) {
                        self.dirty_pages.fetch_add(1, Ordering::Relaxed);
                        self.note_dirty(&frame.key, dirty_lsn);
                    }
                    frame.prefetch.store(false, Ordering::Release);
                    Ok(())
                })();
                frame.unpin();
                self.notify_frame_waiters();
                return result;
            }

            self.ensure_capacity_for_insert(self.lock_wait)?;
            let generation = self.file_generation(&key.file)?;
            let shard_idx = self.shard_index(&key);
            let raw_charge = self.claim_raw_page_memory(bytes.capacity())?;
            let mut shard = self.shards[shard_idx].frames.write().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer pool shard lock poisoned",
                )
            })?;
            if shard.contains_key(&key) {
                drop(shard);
                continue;
            }
            let frame = Arc::new(Frame::new_dirty_with_intent(
                key.clone(),
                bytes,
                dirty_lsn,
                generation,
                raw_charge,
                admission,
            ));
            self.write_requests.fetch_add(1, Ordering::Relaxed);
            if btree_logical_wal::is_deferred() {
                frame.deferred_pins.fetch_add(1, Ordering::AcqRel);
                btree_logical_wal::push_deferred_key(frame.key.clone());
            }
            shard.insert(key.clone(), frame.clone());
            self.used_pages.fetch_add(1, Ordering::Relaxed);
            self.note_kind_added(&key);
            self.dirty_pages.fetch_add(1, Ordering::Relaxed);
            self.note_dirty(&key, dirty_lsn);
            drop(shard);
            if admission == AccessIntent::Scan {
                self.observe_new_scan_origin(shard_idx, &frame);
            }
            self.push_clock_key(shard_idx, key.clone())?;
            if let Err(err) = self.ensure_capacity(self.lock_wait) {
                frame.unpin();
                self.remove_frame_if_same(&frame)?;
                self.notify_frame_waiters();
                return Err(err);
            }
            frame.unpin();
            self.notify_frame_waiters();
            return Ok(());
        }
    }

    /// Encode an operation-local index oracle and publish only its raw image.
    fn store_index_page_transient(
        self: &Arc<Self>,
        key: PageKey,
        dirty_lsn: u64,
        oracle: Arc<IndexPage>,
    ) -> Result<(), QueryError> {
        let mut image = encode_index_tree_page(&oracle)?;
        set_page_lsn_if_formatted(&mut image, dirty_lsn);
        self.store_page(key, image, dirty_lsn)
    }

    /// Resize the pool with atomic configuration semantics. The candidate
    /// capacity is visible while convergence runs so admission/recycling obey
    /// the new bound, but a bounded-time failure restores the old capacity.
    /// Pages already evicted during a failed attempt remain validly evicted;
    /// only the configured limit is rolled back.
    pub(crate) fn set_capacity(
        &self,
        capacity_pages: u64,
        timeout: Duration,
    ) -> Result<(), QueryError> {
        let old_capacity = self.capacity_pages.load(Ordering::Acquire).max(1);
        let new_capacity = capacity_pages.max(1);
        self.capacity_pages.store(new_capacity, Ordering::Release);
        self.trim_recycled_raw_pages();
        let result = self.ensure_capacity(timeout);
        if result.is_err() {
            self.capacity_pages.store(old_capacity, Ordering::Release);
        }
        self.trim_recycled_raw_pages();
        result
    }

    fn flush_all(&self) -> Result<(), QueryError> {
        self.flush_all_inner(true)
    }

    /// Checkpoint takes its dirty snapshot before the single file-sync sweep.
    pub(crate) fn flush_checkpoint_pages(&self) -> Result<(), QueryError> {
        self.flush_all_inner(false)
    }

    fn flush_all_inner(&self, sync_files: bool) -> Result<(), QueryError> {
        if self.durability == DurabilityMode::Memory {
            return Ok(());
        }
        // P0 - skip the dirty walk when nothing is dirty. The per-file
        // fsync sweep (`sync_cached_files`) still runs because a file
        // may have been written to and then re-evicted before this
        // call; the needs_sync bit on each file decides if fsync
        // actually happens.
        let started = Instant::now();
        let mut flushed_pages = 0_u64;
        if self.dirty_pages.load(Ordering::Relaxed) > 0 {
            // D-3 - dirty frames come from the registry (O(dirty)),
            // sorted by (file, page) and grouped per file so each
            // worker writes one file in page order. Batches inside a
            // group coalesce their WAL sync via `flush_frame_batch`.
            let mut frames = self.collect_dirty_frames()?;
            // Group by file (same file ⇒ same precomputed `hash`), then page
            // order, so the batch loop coalesces consecutive pages. Comparing
            // the `u64` file hash instead of the `PathBuf` turns each compare
            // from a `std::path::compare_components` walk (a visible slice of
            // the background flusher's CPU) into one integer compare; a rare
            // hash collision only costs an extra batch break, never a mis-flush.
            frames.sort_unstable_by(|a, b| {
                a.key
                    .file
                    .hash
                    .cmp(&b.key.file.hash)
                    .then_with(|| a.key.page_id.cmp(&b.key.page_id))
            });
            let mut groups: Vec<Vec<Arc<Frame>>> = Vec::new();
            for frame in frames {
                match groups.last_mut() {
                    Some(group)
                        if group
                            .last()
                            .is_some_and(|last| last.key.file == frame.key.file) =>
                    {
                        group.push(frame);
                    }
                    _ => groups.push(vec![frame]),
                }
            }
            flushed_pages = groups.iter().map(|g| g.len() as u64).sum();
            // Schedule bounded, file-ordered chunks rather than assigning one
            // whole file to one worker. A single large heap/index is the
            // common checkpoint case; file-level scheduling left every other
            // configured I/O worker idle. Positioned writes to disjoint pages
            // are independent, and each chunk retains ascending page order so
            // contiguous-run coalescing remains effective.
            let work = groups
                .iter()
                .flat_map(|group| group.chunks(BUFFER_POOL_FLUSH_BATCH_PAGES.max(1)))
                .collect::<Vec<_>>();
            let workers = self.io_threads.clamp(1, work.len().max(1));
            if workers <= 1 || work.len() <= 1 {
                for chunk in &work {
                    if let Err(err) = self.flush_frame_batch(chunk) {
                        self.flush_errors.fetch_add(1, Ordering::Relaxed);
                        return Err(err);
                    }
                }
            } else {
                // Fan chunks out across scoped threads. Frame flush locks and
                // dirty epochs retain the same concurrent re-dirty safety as
                // the single-worker path.
                let next_group = AtomicUsize::new(0);
                let first_error: Mutex<Option<QueryError>> = Mutex::new(None);
                thread::scope(|scope| {
                    for _ in 0..workers {
                        scope.spawn(|| loop {
                            let idx = next_group.fetch_add(1, Ordering::Relaxed);
                            let Some(chunk) = work.get(idx) else {
                                break;
                            };
                            if let Err(err) = self.flush_frame_batch(chunk) {
                                self.flush_errors.fetch_add(1, Ordering::Relaxed);
                                if let Ok(mut slot) = first_error.lock() {
                                    slot.get_or_insert(err);
                                }
                                break;
                            }
                        });
                    }
                });
                if let Some(err) = first_error.lock().ok().and_then(|mut slot| slot.take()) {
                    return Err(err);
                }
            }
        }
        if sync_files {
            self.sync_cached_files()?;
        }
        self.checkpoint_flush_pages
            .store(flushed_pages, Ordering::Relaxed);
        self.checkpoint_flush_us.store(
            started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        Ok(())
    }

    /// Keep bulk writeback contiguous without forcing the inserting thread
    /// through an extra WAL durability barrier before its commit. The fixed
    /// queue owns no pins or page images; normal pressure/checkpoint flushing
    /// still discovers every dirty page independently.
    fn schedule_bulk_page_range(
        &self,
        file: &Arc<BufferPageFileKey>,
        first: u64,
        last: u64,
    ) -> Result<(), QueryError> {
        if last.saturating_sub(first).saturating_add(1) < BUFFER_POOL_FLUSH_BATCH_PAGES as u64 {
            return Ok(());
        }
        if self.durability == DurabilityMode::Memory || self.shutdown.load(Ordering::Acquire) {
            return self.flush_bulk_page_range(file, first, last);
        }
        // Contiguous writeback is a pressure optimization, not a durability
        // requirement. Do not force an otherwise cache-resident bulk write
        // to the device while the ordinary cleaner is below its watermark.
        // Commits remain WAL-durable and the dirty registry remains complete.
        let capacity = self.capacity_pages.load(Ordering::Relaxed).max(1);
        let absolute_high =
            BUFFER_POOL_DIRTY_ABS_HIGH_PAGES.max(capacity / BUFFER_POOL_DIRTY_ABS_SCALE_DIV);
        let high = (capacity * dirty_high_pct() / 100)
            .min(absolute_high)
            .max(1);
        if self.dirty_pages.load(Ordering::Relaxed) < high
            && !self.capacity_pressure.load(Ordering::Acquire)
        {
            return Ok(());
        }
        let generation = self.file_generation(file)?;
        let queued = {
            let mut ranges = self.bulk_flush_ranges.lock().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "bulk flush queue lock poisoned",
                )
            })?;
            let adjacent = ranges.iter_mut().flatten().find(|range| {
                range.generation == generation
                    && range.file == *file
                    && first <= range.last.saturating_add(1)
                    && range.first <= last.saturating_add(1)
            });
            if let Some(range) = adjacent {
                range.first = range.first.min(first);
                range.last = range.last.max(last);
                true
            } else if let Some(slot) = ranges.iter_mut().find(|slot| slot.is_none()) {
                *slot = Some(BulkFlushRange {
                    file: Arc::clone(file),
                    generation,
                    first,
                    last,
                });
                true
            } else {
                false
            }
        };
        if queued {
            // Pair with the worker's condvar gate to avoid a missed wakeup.
            if let Ok(_guard) = self.worker_lock.lock() {}
            self.worker_cv.notify_one();
            Ok(())
        } else {
            // Preserve bounded metadata and foreground backpressure when all
            // slots describe independent files/ranges.
            self.flush_bulk_page_range(file, first, last)
        }
    }

    fn flush_queued_bulk_range(&self) -> Result<bool, QueryError> {
        let start = self.bulk_flush_cursor.fetch_add(1, Ordering::Relaxed);
        let range = {
            let mut ranges = self.bulk_flush_ranges.lock().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "bulk flush queue lock poisoned",
                )
            })?;
            let mut work = None;
            for offset in 0..BUFFER_POOL_BULK_FLUSH_RANGES {
                let slot = &mut ranges[start.wrapping_add(offset) % BUFFER_POOL_BULK_FLUSH_RANGES];
                if let Some(mut range) = slot.take() {
                    let end = range
                        .first
                        .saturating_add(BUFFER_POOL_FLUSH_BATCH_PAGES as u64 - 1)
                        .min(range.last);
                    work = Some(BulkFlushRange {
                        file: Arc::clone(&range.file),
                        generation: range.generation,
                        first: range.first,
                        last: end,
                    });
                    if end < range.last {
                        range.first = end + 1;
                        *slot = Some(range);
                    }
                    break;
                }
            }
            work
        };
        let Some(range) = range else {
            return Ok(false);
        };
        if self.file_generation(&range.file)? == range.generation {
            self.flush_bulk_page_range(&range.file, range.first, range.last)?;
        }
        Ok(true)
    }

    /// Flush a large, freshly built contiguous page range while it is still
    /// resident and ordered. Background cleaners stripe dirty pages by hash,
    /// which is the right concurrency policy for random OLTP writes but loses
    /// the physical adjacency of a bulk heap append. Preserving that adjacency
    /// here lets `flush_contiguous_frame_run` turn up to one batch of 16 KiB
    /// page writes into one positioned write.
    ///
    /// Frames are pinned only for one bounded flush batch. The combined page
    /// image is charged as `StorageScratch` inside
    /// `flush_contiguous_frame_run`; if that optional reservation is denied,
    /// the canonical single-page writer remains the correctness fallback.
    fn flush_bulk_page_range(
        &self,
        file: &Arc<BufferPageFileKey>,
        first_page_id: u64,
        last_page_id: u64,
    ) -> Result<(), QueryError> {
        if last_page_id < first_page_id {
            return Ok(());
        }

        let mut batch_start = first_page_id;
        while batch_start <= last_page_id {
            let batch_end = batch_start
                .saturating_add(BUFFER_POOL_FLUSH_BATCH_PAGES as u64 - 1)
                .min(last_page_id);
            let mut frames = Vec::with_capacity(BUFFER_POOL_FLUSH_BATCH_PAGES);
            for page_id in batch_start..=batch_end {
                let key = PageKey {
                    file: Arc::clone(file),
                    page_id,
                };
                let Some(frame) = self.lookup_frame(&key)? else {
                    continue;
                };
                if self.try_pin_existing(&frame, AccessIntent::Normal) {
                    frames.push(frame);
                }
            }

            let result = self.flush_frame_batch(&frames);
            for frame in &frames {
                frame.unpin();
            }
            self.notify_frame_waiters();
            if let Err(err) = result {
                self.flush_errors.fetch_add(1, Ordering::Relaxed);
                return Err(err);
            }

            if batch_end == u64::MAX {
                break;
            }
            batch_start = batch_end + 1;
        }
        Ok(())
    }

    /// D-2 - capacity-pressure response, run on the IO workers: flush a
    /// bounded batch of dirty pages so the foreground evictor finds a
    /// clean victim without paying a synchronous page write + WAL fsync
    /// itself.
    fn maintain_free_headroom(&self) -> Result<(), QueryError> {
        if self.durability == DurabilityMode::Memory {
            return Ok(());
        }
        let dirty = self.dirty_pages.load(Ordering::Relaxed);
        if dirty == 0 {
            return Ok(());
        }
        self.headroom_flushes.fetch_add(1, Ordering::Relaxed);
        // Claim at most one shard per IO worker. The old all-shard path
        // cloned and sorted every dirty frame just to flush a bounded batch;
        // a full OLTP pool could spend more CPU selecting pages than writing
        // them. Collect bounded CLOCK windows and submit them together so
        // the whole pressure response needs one WAL durability barrier.
        let shard_count = self.shards.len();
        let start = self
            .flush_cursor
            .fetch_add(self.io_threads, Ordering::Relaxed);
        let mut frames = Vec::new();
        for offset in 0..self.io_threads.min(shard_count) {
            let shard_idx = (start + offset) % shard_count;
            let sampled = self.collect_dirty_clock_window(
                shard_idx,
                BUFFER_POOL_FLUSH_BATCH_PAGES * 8,
                true,
            )?;
            frames.extend(cold_frames_in_io_order(
                sampled,
                BUFFER_POOL_FLUSH_BATCH_PAGES,
            ));
        }
        frames.sort_unstable_by(|a, b| {
            a.key
                .file
                .hash
                .cmp(&b.key.file.hash)
                .then_with(|| a.key.page_id.cmp(&b.key.page_id))
        });
        frames.dedup_by(|a, b| a.key == b.key);
        let flushed = self.flush_frame_batch(&frames)?;
        self.background_flushes
            .fetch_add(flushed as u64, Ordering::Relaxed);
        Ok(())
    }

    /// Drop every cached page (across every shard) that maps onto the
    /// given absolute file path, regardless of `kind`. Used by the
    /// physical-replication applier after writing through the recovery
    /// helpers (`redo_heap_*`, `redo_page_update`): those helpers write
    /// raw bytes to disk so any frame still resident in the pool would
    /// serve stale data to concurrent SELECTs on the replica.
    ///
    /// Unlike `discard_file`, this one targets the *path* rather than a
    /// pre-built `BufferPageFileKey`, so callers that only have the WAL
    /// record's `path` string don't need to know whether the file is a
    /// heap, an index, or an overflow chain.
    /// Single-page variant of [`Self::discard_pages_for_path`]: drop one
    /// page from the pool after its bytes were rewritten on disk by the
    /// physical-replication applier's redo helpers. O(1) instead of an
    /// all-shards scan — the applier calls this once per applied WAL
    /// frame, where the whole-file scan dominated apply throughput.
    pub(crate) fn discard_page(&self, key: &PageKey) -> Result<(), QueryError> {
        // Do not bump the file-wide generation for a one-page invalidation:
        // doing so makes every other dirty frame in the file look stale and
        // silently skip its later write-back. Removing this exact frame and
        // publishing Discarded is sufficient; both single and batch loaders
        // validate map identity/state before publishing their bytes.
        let shard_idx = self.shard_index(key);
        let shard = &self.shards[shard_idx];
        let removed_frame = {
            let mut frames = shard.frames.write().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer pool shard lock poisoned",
                )
            })?;
            let removed = if let Some(frame) = frames.remove(key) {
                frame.publish_io_state(FrameIoState::Discarded);
                if frame.dirty.swap(false, Ordering::AcqRel) {
                    self.dirty_pages.fetch_sub(1, Ordering::Relaxed);
                }
                self.note_kind_removed_n(key.file.kind.slot(), 1);
                self.release_scan_origin_flag(&frame);
                self.used_pages.fetch_sub(1, Ordering::Relaxed);
                Some(frame)
            } else {
                None
            };
            if let Ok(mut set) = shard.dirty_set.lock() {
                set.remove(key);
            }
            if let Ok(mut oldest) = shard.dirty_oldest.lock() {
                oldest.retain(|Reverse(candidate)| candidate.key != *key);
            }
            removed
        };
        if let Some(frame) = removed_frame {
            // Serialize with a snapshot already being written. After this
            // returns a redo writer may overwrite disk; no old flush may land
            // afterwards. Other pages retain their file generation.
            let _flush_guard = frame.flush_lock.lock().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer frame flush lock poisoned",
                )
            })?;
            self.recycle_frame_raw_page(shard_idx, &frame);
        }
        self.wait_cv.notify_all();
        Ok(())
    }

    pub(crate) fn discard_pages_for_path(&self, target: &Path) -> Result<(), QueryError> {
        // Remove tombstones after publishing a globally unique replacement
        // generation. A later re-open gets another unique value, so stale
        // in-flight frames can never ABA-match a recreated map entry.
        if let Ok(mut generations) = self.file_generations.write() {
            generations.retain(|key, cell| {
                if key.path != target {
                    return true;
                }
                cell.store(self.allocate_file_generation(), Ordering::Release);
                false
            });
        }
        if let Ok(mut files) = self.file_cache.write() {
            files.retain(|k, _| k.path != target);
        }
        for (shard_idx, shard) in self.shards.iter().enumerate() {
            let mut frames = shard.frames.write().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer pool shard lock poisoned",
                )
            })?;
            let mut removed_dirty = 0_u64;
            let mut removed_by_kind = [0u64; BUFFER_PAGE_KIND_COUNT];
            let mut removed_frames = Vec::new();
            let before = frames.len();
            frames.retain(|key, frame| {
                let keep = key.file.path != target;
                if !keep {
                    frame.publish_io_state(FrameIoState::Discarded);
                    removed_frames.push(Arc::clone(frame));
                    if frame.dirty.swap(false, Ordering::AcqRel) {
                        removed_dirty = removed_dirty.saturating_add(1);
                    }
                    removed_by_kind[key.file.kind.slot()] += 1;
                    self.release_scan_origin_flag(frame);
                }
                keep
            });
            let removed = before.saturating_sub(frames.len()) as u64;
            if removed > 0 {
                self.used_pages.fetch_sub(removed, Ordering::Relaxed);
                for (slot, n) in removed_by_kind.into_iter().enumerate() {
                    self.note_kind_removed_n(slot, n);
                }
            }
            if removed_dirty > 0 {
                self.dirty_pages.fetch_sub(removed_dirty, Ordering::Relaxed);
            }
            // Keep readmission excluded until both dirty registries retire
            // the discarded generation.
            if let Ok(mut set) = shard.dirty_set.lock() {
                set.retain(|key| key.file.path != target);
            }
            if let Ok(mut oldest) = shard.dirty_oldest.lock() {
                oldest.retain(|Reverse(candidate)| candidate.key.file.path != target);
            }
            drop(frames);
            for frame in removed_frames {
                self.recycle_frame_raw_page(shard_idx, &frame);
            }
        }
        self.cancel_prefetch_matching(|key| key.file.path == target);
        self.wait_cv.notify_all();
        Ok(())
    }

    pub(crate) fn flush_and_evict_all(&self) -> Result<(), QueryError> {
        let started = Instant::now();
        let result = (|| -> Result<u64, QueryError> {
            if self.durability == DurabilityMode::Memory {
                return Ok(0);
            }
            // P0 - in-memory state is already empty AND no file in the pool has
            // pending writes since its last sync; nothing to flush, nothing to
            // evict. Skip the per-shard write-lock scan and the per-file fsync
            // walk entirely. Drain the prefetch queue so a re-entry doesn't
            // race a stale prefetch into a freshly-evicted page.
            if self.used_pages.load(Ordering::Relaxed) == 0
                && self.dirty_pages.load(Ordering::Relaxed) == 0
                && !self.any_file_needs_sync()
            {
                self.cancel_all_queued_prefetch();
                self.release_recycled_raw_pages();
                self.flush_data_cache_short_circuits
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(0);
            }
            self.flush_all()?;
            self.cancel_all_queued_prefetch();

            let mut removed_total = 0_u64;
            for (shard_idx, shard) in self.shards.iter().enumerate() {
                let mut removed_frames = Vec::new();
                let mut removed = 0_u64;
                {
                    let mut frames = shard.frames.write().map_err(|_| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "buffer pool shard lock poisoned",
                        )
                    })?;
                    frames.retain(|key, frame| {
                        if !frame.is_ready() || frame.dirty.load(Ordering::Acquire) {
                            return true;
                        }
                        if frame
                            .pin_count
                            .compare_exchange(
                                0,
                                Self::EVICTING_PIN,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_err()
                        {
                            return true;
                        }
                        if frame.dirty.load(Ordering::Acquire) {
                            frame.pin_count.store(0, Ordering::Release);
                            return true;
                        }
                        frame.publish_io_state(FrameIoState::Discarded);
                        self.release_scan_origin_flag(frame);
                        self.note_kind_removed(key);
                        removed_frames.push(Arc::clone(frame));
                        removed = removed.saturating_add(1);
                        false
                    });
                }
                if removed > 0 {
                    removed_total = removed_total.saturating_add(removed);
                    self.used_pages.fetch_sub(removed, Ordering::Relaxed);
                    self.evictions.fetch_add(removed, Ordering::Relaxed);
                    if let Ok(mut clock) = shard.clock.lock() {
                        // Frames are already removed from the shard map. One
                        // live-membership pass compacts every stale clock key
                        // in O(clock_len); retaining once per removed key made
                        // a full-cache flush quadratic in resident pages.
                        self.retain_live_clock_keys(shard_idx, &mut clock);
                    }
                    for frame in removed_frames {
                        self.recycle_frame_raw_page(shard_idx, &frame);
                    }
                }
            }
            // Evicted frames transfer their governed raw pages to the recycle
            // pool. An explicit cache flush promises to release optional cache
            // memory, so do not leave those leases charged until some unrelated
            // future pressure probe happens to trim them.
            self.release_recycled_raw_pages();
            self.notify_frame_waiters();
            Ok(removed_total)
        })();
        self.flush_data_cache_us.fetch_add(
            started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        match result {
            Ok(removed) => {
                self.flush_data_cache_pages
                    .fetch_add(removed, Ordering::Relaxed);
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    fn flush_file_pages(&self, file: &BufferPageFileKey) -> Result<(), QueryError> {
        if self.durability == DurabilityMode::Memory {
            return Ok(());
        }
        let frames = self.frames_for_file(file)?;
        for frame in frames {
            if let Err(err) = self.flush_frame(&frame) {
                self.flush_errors.fetch_add(1, Ordering::Relaxed);
                return Err(err);
            }
        }
        Ok(())
    }

    fn flush_file(&self, file: &BufferPageFileKey) -> Result<(), QueryError> {
        self.flush_file_pages(file)?;
        self.sync_cached_file(file)
    }

    /// Complete a flush after the owner has updated its direct file metadata.
    /// On macOS, identical vnode handles need only one full device barrier.
    fn sync_cached_and_direct_file(
        &self,
        key: &BufferPageFileKey,
        direct: &File,
        sync_direct: bool,
    ) -> Result<(), QueryError> {
        if !sync_direct {
            return self.sync_cached_file(key);
        }
        #[cfg(target_os = "macos")]
        {
            use std::os::unix::fs::MetadataExt;
            let cached = self
                .file_cache
                .read()
                .map_err(|_| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "buffer pool file cache lock poisoned",
                    )
                })?
                .get(key)
                .cloned();
            let _sync = cached.as_ref().map(|file| file.sync_lock.lock());
            let pending = cached.as_ref().is_some_and(|file| file.take_needs_sync());
            // Clear only the pre-barrier publication. Writers can set the
            // flag again while fsync runs; success must preserve that store.
            let result = (|| -> std::io::Result<()> {
                let mut batch = crate::platform::fs::DeviceSyncBatch::new();
                if pending {
                    let cached = cached.as_ref().unwrap();
                    let left = cached.file.metadata()?;
                    let right = direct.metadata()?;
                    if left.dev() != right.dev() || left.ino() != right.ino() {
                        batch.sync_file(cached.file.try_clone()?, &key.path)?;
                    }
                }
                batch.sync_file(direct.try_clone()?, &key.path)?;
                batch.finish()
            })();
            if let Err(error) = result {
                if pending {
                    cached.as_ref().unwrap().mark_needs_sync();
                }
                return Err(io_err(error));
            }
            if pending {
                self.file_syncs.fetch_add(1, Ordering::Relaxed);
            } else if cached.is_some() {
                self.file_sync_skipped.fetch_add(1, Ordering::Relaxed);
            }
            Ok(())
        }
        #[cfg(not(target_os = "macos"))]
        {
            self.sync_cached_file(key)?;
            crate::platform::fs::sync_file_data_at(direct, &key.path).map_err(io_err)
        }
    }

    fn discard_file(&self, file: &BufferPageFileKey) -> Result<(), QueryError> {
        if let Ok(mut generations) = self.file_generations.write() {
            if let Some(cell) = generations.remove(file) {
                cell.store(self.allocate_file_generation(), Ordering::Release);
            }
        }
        if let Ok(mut files) = self.file_cache.write() {
            files.remove(file);
        }
        for (shard_idx, shard) in self.shards.iter().enumerate() {
            let mut frames = shard.frames.write().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer pool shard lock poisoned",
                )
            })?;
            let mut removed_dirty = 0_u64;
            let mut removed_frames = Vec::new();
            let before = frames.len();
            frames.retain(|key, frame| {
                let keep = &*key.file != file;
                if !keep {
                    if frame.dirty.swap(false, Ordering::AcqRel) {
                        removed_dirty = removed_dirty.saturating_add(1);
                    }
                    self.release_scan_origin_flag(frame);
                    frame.publish_io_state(FrameIoState::Discarded);
                    removed_frames.push(Arc::clone(frame));
                }
                keep
            });
            let removed = before.saturating_sub(frames.len()) as u64;
            if removed > 0 {
                self.used_pages.fetch_sub(removed, Ordering::Relaxed);
                self.note_kind_removed_n(file.kind.slot(), removed);
            }
            if removed_dirty > 0 {
                self.dirty_pages.fetch_sub(removed_dirty, Ordering::Relaxed);
            }
            // Keep readmission excluded until both dirty registries retire
            // the discarded generation.
            if let Ok(mut set) = shard.dirty_set.lock() {
                set.retain(|key| &*key.file != file);
            }
            if let Ok(mut oldest) = shard.dirty_oldest.lock() {
                oldest.retain(|Reverse(candidate)| &*candidate.key.file != file);
            }
            drop(frames);
            for frame in removed_frames {
                self.recycle_frame_raw_page(shard_idx, &frame);
            }
        }
        self.cancel_prefetch_matching(|key| &*key.file == file);
        self.wait_cv.notify_all();
        Ok(())
    }

    fn remove_frame_if_same(&self, frame: &Arc<Frame>) -> Result<(), QueryError> {
        let shard_idx = self.shard_index(&frame.key);
        let mut frames = self.shards[shard_idx].frames.write().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "buffer pool shard lock poisoned",
            )
        })?;
        if frames
            .get(&frame.key)
            .is_some_and(|current| Arc::ptr_eq(current, frame))
        {
            frames.remove(&frame.key);
            frame.publish_io_state(FrameIoState::Discarded);
            self.used_pages.fetch_sub(1, Ordering::Relaxed);
            self.note_kind_removed(&frame.key);
            self.release_scan_origin_flag(frame);
            if frame.dirty.swap(false, Ordering::AcqRel) {
                self.dirty_pages.fetch_sub(1, Ordering::Relaxed);
            }
            self.dirty_set_remove(&frame.key);
            drop(frames);
            self.recycle_frame_raw_page(shard_idx, frame);
        }
        Ok(())
    }

    // PERF-5 - the SHOW ENGINE status path now reads per-kind dirty
    // counts straight off the single `collect_dirty_frames` pass inside
    // `snapshot()`. Only the storage test below still asks for an
    // index-only count, so this helper is test-gated.
    #[cfg(test)]
    fn dirty_index_page_count(&self) -> usize {
        self.collect_dirty_frames()
            .map(|frames| {
                frames
                    .iter()
                    .filter(|frame| {
                        matches!(
                            frame.key.file.kind,
                            BufferPageFileKind::Index { .. } | BufferPageFileKind::Spatial { .. }
                        )
                    })
                    .count()
            })
            .unwrap_or(0)
    }

    fn snapshot(&self) -> BufferPoolSnapshot {
        // PERF-5 - walk resident frames in place under each shard read
        // lock instead of `all_frames()`, which cloned every `Arc<Frame>`
        // (O(capacity) refcount traffic + a Vec allocation) just to count
        // them.
        let mut freeable_pages = 0_u64;
        let mut pinned_pages = 0_u64;
        for shard in &self.shards {
            let Ok(frames) = shard.frames.read() else {
                continue;
            };
            for frame in frames.values() {
                let pin_count = frame.pin_count.load(Ordering::Relaxed);
                if pin_count == 0 {
                    if frame.is_ready() && frame.deferred_pins.load(Ordering::Relaxed) == 0 {
                        freeable_pages = freeable_pages.saturating_add(1);
                    }
                } else if pin_count != Self::EVICTING_PIN {
                    pinned_pages = pinned_pages.saturating_add(1);
                }
            }
        }
        // PERF-5 - resolve the dirty registry ONCE for oldest-LSN +
        // per-kind dirty counts. The status assembly previously called
        // `collect_dirty_frames` three times (here, `dirty_heap_page_count`,
        // `dirty_index_page_count`).
        let mut oldest_dirty_lsn = 0_u64;
        let mut dirty_heap_pages = 0_u64;
        let mut dirty_index_pages = 0_u64;
        if let Ok(frames) = self.collect_dirty_frames() {
            for frame in frames {
                let first = frame.first_dirty_lsn.load(Ordering::Acquire);
                let lsn = if first == 0 {
                    frame.dirty_lsn.load(Ordering::Acquire)
                } else {
                    first
                };
                if lsn != 0 && (oldest_dirty_lsn == 0 || lsn < oldest_dirty_lsn) {
                    oldest_dirty_lsn = lsn;
                }
                match frame.key.file.kind {
                    BufferPageFileKind::Heap { .. } => {
                        dirty_heap_pages = dirty_heap_pages.saturating_add(1);
                    }
                    BufferPageFileKind::Index { .. } | BufferPageFileKind::Spatial { .. } => {
                        dirty_index_pages = dirty_index_pages.saturating_add(1);
                    }
                    BufferPageFileKind::Overflow { .. } => {}
                    BufferPageFileKind::Vector { .. } => {}
                    BufferPageFileKind::ZoneMap { .. } => {}
                }
            }
        }
        // P0-1 - per-shard hit/miss imbalance. Reporting min/max +
        // a 0..=100 score lets ops eyeball "one shard does it all" vs
        // "evenly spread" without dumping 16 rows.
        let mut shard_max_hits = 0u64;
        let mut shard_min_hits = u64::MAX;
        for shard in &self.shards {
            let h = shard.hits.load(Ordering::Relaxed);
            shard_max_hits = shard_max_hits.max(h);
            shard_min_hits = shard_min_hits.min(h);
        }
        if self.shards.is_empty() {
            shard_min_hits = 0;
        }
        let shard_imbalance_pct = if shard_max_hits == 0 {
            0
        } else {
            shard_max_hits
                .saturating_sub(shard_min_hits)
                .saturating_mul(100)
                / shard_max_hits
        };
        let mut histogram = [0u64; 8];
        for (i, slot) in self.dirty_ratio_histogram.iter().enumerate() {
            histogram[i] = slot.load(Ordering::Relaxed);
        }
        BufferPoolSnapshot {
            capacity_pages: self.capacity_pages.load(Ordering::Relaxed),
            used_pages: self.used_pages.load(Ordering::Relaxed),
            dirty_pages: self.dirty_pages.load(Ordering::Relaxed),
            freeable_pages,
            evictions: self.evictions.load(Ordering::Relaxed),
            flushes: self.flushes.load(Ordering::Relaxed),
            write_requests: self.write_requests.load(Ordering::Relaxed),
            flush_errors: self.flush_errors.load(Ordering::Relaxed),
            background_io_errors: self.background_io_errors.load(Ordering::Relaxed),
            pinned_pages,
            waits: self.waits.load(Ordering::Relaxed),
            wait_ms: self.wait_ms.load(Ordering::Relaxed),
            background_flushes: self.background_flushes.load(Ordering::Relaxed),
            shards: self.shards.len() as u64,
            read_ios: self.read_ios.load(Ordering::Relaxed),
            kind_read_ios: std::array::from_fn(|slot| {
                self.kind_read_ios[slot].load(Ordering::Relaxed)
            }),
            read_syscalls: self.read_syscalls.load(Ordering::Relaxed),
            batch_read_ops: self.batch_read_ops.load(Ordering::Relaxed),
            batch_read_pages: self.batch_read_pages.load(Ordering::Relaxed),
            write_ios: self.write_ios.load(Ordering::Relaxed),
            write_syscalls: self.write_syscalls.load(Ordering::Relaxed),
            prefetches: self.prefetches.load(Ordering::Relaxed),
            prefetch_hits: self.prefetch_hits.load(Ordering::Relaxed),
            prefetch_wasted: self.prefetch_wasted.load(Ordering::Relaxed),
            prefetch_demand_waits: self.prefetch_demand_waits.load(Ordering::Relaxed),
            eviction_scans: self.eviction_scans.load(Ordering::Relaxed),
            io_threads: self.io_threads as u64,
            prefetch_pages: self.prefetch_pages.load(Ordering::Relaxed) as u64,
            small_scan_pct: self.small_scan_pct(),
            small_scan_threshold_pages: self.small_scan_threshold_pages(),
            small_scan_accesses: self.small_scan_accesses.load(Ordering::Relaxed),
            small_scan_cold_accesses: self.small_scan_cold_accesses.load(Ordering::Relaxed),
            small_scan_promotions: self.small_scan_promotions.load(Ordering::Relaxed),
            small_scan_read_ios: self.small_scan_read_ios.load(Ordering::Relaxed),
            read_ios_normal: self.read_ios_normal.load(Ordering::Relaxed),
            read_ios_scan: self.read_ios_scan.load(Ordering::Relaxed),
            read_ios_prefetch: self.read_ios_prefetch.load(Ordering::Relaxed),
            prefetch_queue_drops: self.prefetch_queue_drops.load(Ordering::Relaxed),
            prefetch_duplicate_drops: self.prefetch_duplicate_drops.load(Ordering::Relaxed),
            prefetch_pressure_drops: self.prefetch_pressure_drops.load(Ordering::Relaxed),
            prefetch_queue_full_drops: self.prefetch_queue_full_drops.load(Ordering::Relaxed),
            prefetch_stale_drops: self.prefetch_stale_drops.load(Ordering::Relaxed),
            raw_page_allocations: self.raw_page_allocations.load(Ordering::Relaxed),
            raw_page_reuse_hits: self.raw_page_reuse_hits.load(Ordering::Relaxed),
            raw_page_reuse_misses: self.raw_page_reuse_misses.load(Ordering::Relaxed),
            recycled_raw_pages: self.recycled_raw_page_count.load(Ordering::Relaxed),
            prefetch_queue_wait_us_total: self.prefetch_queue_wait_us_total.load(Ordering::Relaxed),
            prefetch_queue_drains: self.prefetch_queue_drains.load(Ordering::Relaxed),
            prefetch_lead_us_total: self.prefetch_lead_us_total.load(Ordering::Relaxed),
            prefetch_lead_samples: self.prefetch_lead_samples.load(Ordering::Relaxed),
            ensure_capacity_busy_us: self.ensure_capacity_busy_us.load(Ordering::Relaxed),
            file_handle_wait_ns: self.file_handle_wait_ns.load(Ordering::Relaxed),
            dirty_ratio_histogram: histogram,
            dirty_ratio_samples: self.dirty_ratio_samples.load(Ordering::Relaxed),
            spill_bytes_read: spill_bytes_read(),
            spill_bytes_written: spill_bytes_written(),
            shard_imbalance_pct,
            shard_max_hits,
            shard_min_hits,
            scan_origin_share_pct: {
                let cap = self.capacity_pages.load(Ordering::Relaxed).max(1);
                let so = self.scan_origin_count.load(Ordering::Relaxed);
                (so.saturating_mul(100) / cap).min(100)
            },
            scan_origin_evictions: self.scan_origin_evictions.load(Ordering::Relaxed),
            cold_recycle_hits: self.cold_recycle_hits.load(Ordering::Relaxed),
            cold_recycle_fallbacks: self.cold_recycle_fallbacks.load(Ordering::Relaxed),
            raw_scan_pages: self.raw_scan_pages.load(Ordering::Relaxed),
            kind_used_pages: std::array::from_fn(|slot| {
                self.kind_used_pages[slot].load(Ordering::Relaxed)
            }),
            quota_evictions: self.quota_evictions.load(Ordering::Relaxed),
            file_syncs: self.file_syncs.load(Ordering::Relaxed),
            file_sync_skipped: self.file_sync_skipped.load(Ordering::Relaxed),
            flush_data_cache_short_circuits: self
                .flush_data_cache_short_circuits
                .load(Ordering::Relaxed),
            flush_data_cache_pages: self.flush_data_cache_pages.load(Ordering::Relaxed),
            flush_data_cache_us: self.flush_data_cache_us.load(Ordering::Relaxed),
            evict_sync_dirty_flushes: self.evict_sync_dirty_flushes.load(Ordering::Relaxed),
            evict_sync_dirty_flush_us: self.evict_sync_dirty_flush_us.load(Ordering::Relaxed),
            cooldown_deferred_flushes: self.cooldown_deferred_flushes.load(Ordering::Relaxed),
            headroom_flushes: self.headroom_flushes.load(Ordering::Relaxed),
            checkpoint_flush_pages: self.checkpoint_flush_pages.load(Ordering::Relaxed),
            checkpoint_flush_us: self.checkpoint_flush_us.load(Ordering::Relaxed),
            checkpoint_preflush_pages: self.checkpoint_preflush_pages.load(Ordering::Relaxed),
            checkpoint_preflush_us: self.checkpoint_preflush_us.load(Ordering::Relaxed),
            prefetch_queue_depth_peak: self.prefetch_queue_depth_peak.load(Ordering::Relaxed),
            oldest_dirty_lsn,
            dirty_heap_pages,
            dirty_index_pages,
            dirty_oldest_len: self
                .shards
                .iter()
                .map(|shard| {
                    shard
                        .dirty_oldest
                        .lock()
                        .map(|heap| heap.len() as u64)
                        .unwrap_or(0)
                })
                .sum(),
        }
    }

    fn diagnostic_pages(&self) -> Vec<BufferPageDiagnostic> {
        let mut rows = Vec::with_capacity(self.used_pages.load(Ordering::Relaxed) as usize);
        for shard in &self.shards {
            let Ok(frames) = shard.frames.read() else {
                continue;
            };
            for frame in frames.values() {
                let (storage_id, page_type, index_name) = match &frame.key.file.kind {
                    BufferPageFileKind::Heap { table_id } => (*table_id, "HEAP", None),
                    BufferPageFileKind::Spatial { table_id, name } => {
                        (*table_id, "SPATIAL", Some(name.clone()))
                    }
                    BufferPageFileKind::Index { table_id, name } => {
                        (*table_id, "INDEX", Some(name.clone()))
                    }
                    BufferPageFileKind::Overflow { table_id } => (*table_id, "LOB", None),
                    BufferPageFileKind::Vector { table_id, name } => {
                        (*table_id, "VECTOR", Some(name.clone()))
                    }
                    BufferPageFileKind::ZoneMap { table_id, .. } => (*table_id, "ZONE_MAP", None),
                };
                let state = FrameIoState::from_u8(frame.state.load(Ordering::Acquire));
                let (page_state, io_fix) = match state {
                    FrameIoState::Loading => ("FILE_PAGE", "READ"),
                    FrameIoState::Ready => ("FILE_PAGE", "NONE"),
                    FrameIoState::Flushing => ("FILE_PAGE", "WRITE"),
                    FrameIoState::Evicting => ("REMOVE_HASH", "NONE"),
                    FrameIoState::Discarded => ("NOT_USED", "NONE"),
                };
                let usage = u64::from(frame.usage_count.load(Ordering::Relaxed));
                rows.push(BufferPageDiagnostic {
                    block_id: 0,
                    storage_id,
                    page_number: frame.key.page_id,
                    page_type,
                    index_name,
                    fix_count: frame
                        .pin_count
                        .load(Ordering::Relaxed)
                        .min(u64::MAX as usize) as u64,
                    newest_modification: frame.dirty_lsn.load(Ordering::Acquire),
                    oldest_modification: frame.first_dirty_lsn.load(Ordering::Acquire),
                    access_rank: usage,
                    replacement_position: 0,
                    page_state,
                    io_fix,
                    is_old: frame.scan_origin.load(Ordering::Relaxed) || usage == 0,
                });
            }
        }
        rows.sort_by(|left, right| {
            left.access_rank
                .cmp(&right.access_rank)
                .then_with(|| left.storage_id.cmp(&right.storage_id))
                .then_with(|| left.page_number.cmp(&right.page_number))
        });
        for (position, row) in rows.iter_mut().enumerate() {
            row.block_id = position as u64;
            row.replacement_position = position as u64;
        }
        rows
    }

    fn shutdown_flusher(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.wait_cv.notify_all();
        self.wake_workers();
        let handles = self
            .flusher
            .lock()
            .ok()
            .map(|mut handles| std::mem::take(&mut *handles))
            .unwrap_or_default();
        for handle in handles {
            let _ = handle.join();
        }
        self.release_recycled_raw_pages();
    }

    fn record_background_io_error(&self, operation: &str, err: &QueryError) {
        let count = self
            .background_io_errors
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        if count == 1 || count.is_power_of_two() {
            log_message(format_args!(
                "[mydb] buffer pool background {operation} failed (count={count}, errno={}, sqlstate={}): {}",
                err.code(),
                err.sql_state(),
                err.message(),
            ));
        }
    }

    fn ensure_capacity_for_insert(&self, timeout: Duration) -> Result<(), QueryError> {
        let started = Instant::now();
        while self.used_pages.load(Ordering::Relaxed)
            >= self.capacity_pages.load(Ordering::Relaxed).max(1)
        {
            // D-2 - inside the grace window only clean victims are
            // taken; the IO workers flush on our behalf. Past the
            // window — or when the caller has no wait budget at all
            // (lock_wait of zero means "must make progress inline") —
            // the old synchronous dirty-victim path guarantees
            // progress regardless of worker liveness.
            let allow_dirty_victim = timeout <= BUFFER_POOL_EVICT_SYNC_GRACE
                || started.elapsed() >= BUFFER_POOL_EVICT_SYNC_GRACE;
            match self.evict_one_with_policy(allow_dirty_victim)? {
                true => continue,
                false if started.elapsed() < timeout => {
                    let wait_started = Instant::now();
                    self.waits.fetch_add(1, Ordering::Relaxed);
                    let remaining = timeout.saturating_sub(started.elapsed());
                    // N-3 - cap each individual wait at 50ms (was 2ms)
                    // and rely on `notify_frame_waiters` after every
                    // eviction / flush / pin release for prompt wake.
                    // The cap protects us from a missed notify (e.g.
                    // workers having parked between wake_workers and
                    // their next loop iteration), but should rarely
                    // be reached in practice. Removes 500 wakeups/s
                    // of pure busy waiting on contended OLTP.
                    self.wait_for_frame_release(remaining.min(BUFFER_POOL_WAIT_CEILING));
                    let waited = wait_started.elapsed();
                    self.wait_ms.fetch_add(
                        waited.as_millis().min(u128::from(u64::MAX)) as u64,
                        Ordering::Relaxed,
                    );
                    // P0-1 - microsecond resolution so sub-ms waits
                    // (the P99 tail) stay visible.
                    self.ensure_capacity_busy_us.fetch_add(
                        waited.as_micros().min(u128::from(u64::MAX)) as u64,
                        Ordering::Relaxed,
                    );
                }
                false => return Err(self.capacity_exhausted_error()),
            }
        }
        Ok(())
    }

    fn ensure_capacity(&self, timeout: Duration) -> Result<(), QueryError> {
        let started = Instant::now();
        while self.used_pages.load(Ordering::Relaxed)
            > self.capacity_pages.load(Ordering::Relaxed).max(1)
        {
            let allow_dirty_victim = timeout <= BUFFER_POOL_EVICT_SYNC_GRACE
                || started.elapsed() >= BUFFER_POOL_EVICT_SYNC_GRACE;
            match self.evict_one_with_policy(allow_dirty_victim)? {
                true => continue,
                false if started.elapsed() < timeout => {
                    let wait_started = Instant::now();
                    self.waits.fetch_add(1, Ordering::Relaxed);
                    let remaining = timeout.saturating_sub(started.elapsed());
                    self.wait_for_frame_release(remaining.min(BUFFER_POOL_WAIT_CEILING));
                    let waited = wait_started.elapsed();
                    self.wait_ms.fetch_add(
                        waited.as_millis().min(u128::from(u64::MAX)) as u64,
                        Ordering::Relaxed,
                    );
                    self.ensure_capacity_busy_us.fetch_add(
                        waited.as_micros().min(u128::from(u64::MAX)) as u64,
                        Ordering::Relaxed,
                    );
                }
                false => return Err(self.capacity_exhausted_error()),
            }
        }
        Ok(())
    }

    fn capacity_exhausted_error(&self) -> QueryError {
        let snapshot = self.snapshot();
        error(crate::error::catalog::ER_GET_ERRNO,
            format!(
                "buffer pool capacity exhausted: capacity={}, used={}, recycled={}, pinned={}, dirty={}, freeable={}, governed_raw_bytes={}",
                snapshot.capacity_pages,
                snapshot.used_pages,
                self.recycled_raw_page_count.load(Ordering::Acquire),
                snapshot.pinned_pages,
                snapshot.dirty_pages,
                snapshot.freeable_pages,
                crate::engine::executor::mem_budget::controlled_buffer_used_bytes(),
            ),
        )
    }

    // N-2 (post-regression-fix) - the original implementation ran
    // a *separate* sweep over every shard ahead of the regular
    // clock walk looking for scan-origin victims. Under buffer
    // pressure (lots of evictions) that doubled the per-evict
    // lock cost — 16 shards × 64 frames of shard.frames.lock per
    // miss — and turned a 37s TPC-H run with 6553-page buffer
    // into a 102s one.
    //
    // The fix folds the scan-origin preference into
    // find_clock_victim itself: any scan-origin frame that
    // happens to land on the hand is taken without the ref_bit /
    // usage_count discount. No extra sweep, no extra lock cost,
    // and the working-set protection is preserved (a Normal
    // access has already cleared scan_origin via promote_scan_origin
    // by the time the hand visits the frame).

    /// D-2 - victim selection with a dirty-victim policy switch. The
    /// clean pass always runs; when it comes up empty and
    /// `allow_dirty_victim` is false, the foreground raises the
    /// capacity-pressure flag (the IO workers respond with a headroom
    /// flush) and reports "no victim" so the caller waits instead of
    /// paying a synchronous page write + WAL fsync inline. Callers
    /// flip `allow_dirty_victim` on after a grace window so progress
    /// is always guaranteed even with stalled workers.
    ///
    /// E-1 — skip the clean-only first pass when the pool is nearly
    /// 100% dirty. The clean pass walks up to `4 × pool_pages` frames
    /// per call before giving up, which under a concurrent bulk-load
    /// workload (sysbench `prepare`, 16 inserters into a 1 GiB pool
    /// loading 3 GiB of data) was guaranteed to find nothing and was
    /// burning ~25% of CPU. When the dirty ratio is at or above the
    /// `EVICT_CLEAN_PASS_SKIP_PCT` threshold, jump straight to the
    /// dirty victim path (still gated by `allow_dirty_victim`, so the
    /// grace-window wait-for-background behaviour is preserved when
    /// the caller insists on staying clean-only).
    fn evict_one_with_policy(&self, allow_dirty_victim: bool) -> Result<bool, QueryError> {
        // One ticket belongs to the whole eviction attempt. Advancing once
        // for an empty cold queue and again for CLOCK produces a stride of
        // two, starving half of a power-of-two shard set in normal workloads.
        let start = self.eviction_hand.fetch_add(1, Ordering::Relaxed);
        if cold_recycle_enabled() {
            if self.scan_origin_count.load(Ordering::Relaxed) != 0 {
                if let Some(victim) = self.find_cold_candidate(true, start)? {
                    self.cold_recycle_hits.fetch_add(1, Ordering::Relaxed);
                    return self.consume_victim(victim, true);
                }
            }
            self.cold_recycle_fallbacks.fetch_add(1, Ordering::Relaxed);
        }
        // E-1 — under sustained bulk-load pressure the resident set is
        // nearly all dirty (the background flusher converges dirty to
        // ~50%, but a 1 GiB pool absorbing 3 GiB of fresh inserts
        // never lets that 50% become clean enough to satisfy the
        // foreground pre-scan budget). In that regime the clean-only
        // pre-scan walks up to `4 × pool_pages` frames and finds
        // nothing, and the right move is to skip it — even before
        // the dirty-victim grace window expires — and let the
        // foreground absorb the flush. The grace window's "wait for
        // background" semantics still apply when there *are* clean
        // pages to take.
        let prefer_dirty_first = self.dirty_ratio_high();
        let victim = if prefer_dirty_first {
            // B2 - honour the bounded grace window by default before a
            // synchronous foreground dirty flush (see
            // `evict_dirty_ratio_grace_enabled`). Operators can explicitly
            // disable it for storage where immediate writeback wins.
            if evict_dirty_ratio_grace_enabled() && !allow_dirty_victim {
                // A high dirty fraction still leaves clean pages in a mixed
                // workload.  Probe a small CLOCK window in every shard before
                // waiting for writeback.  A full clean-only sweep here would
                // regress all-dirty bulk loads, while skipping clean pages
                // entirely makes OLTP wait at the 60% threshold.
                self.find_clean_victim_bounded(start)?
            } else {
                self.find_clock_victim_from(false, start)?
            }
        } else {
            match self.find_clock_victim_from(true, start)? {
                Some(victim) => Some(victim),
                None if allow_dirty_victim => self.find_clock_victim_from(false, start)?,
                None => None,
            }
        };
        let victim = match victim {
            Some(victim) => victim,
            None => {
                self.capacity_pressure.store(true, Ordering::Release);
                self.wake_workers();
                return Ok(false);
            }
        };
        let scan_priority = victim.2.scan_origin.load(Ordering::Acquire);
        self.consume_victim(victim, scan_priority)
    }

    /// Pop maintained weak candidates instead of scanning the resident map.
    /// A small stale budget bounds latency; CLOCK remains the progress path.
    fn find_cold_candidate(
        &self,
        clean_only: bool,
        cold_start: usize,
    ) -> Result<Option<(usize, PageKey, Arc<Frame>)>, QueryError> {
        // Ordered MRR can have all candidates in one shard temporarily pinned
        // while sibling probe workers consume other shards. A 32-entry cap
        // fell through to CLOCK about 20k times per Q21/SF20 run; those rare
        // fallbacks then inspected millions of resident keys. Keep the queue
        // walk bounded, but large enough to cross one 64-page I/O run plus
        // stale weak entries left by the preceding run.
        const STALE_BUDGET: usize = 128;
        // Preserve the existing per-kind soft-quota contract. In particular,
        // a heap scan frame is cold, but must not be recycled ahead of an
        // already-over-quota LOB stream; doing so lets a LOB flood gradually
        // replace the whole heap working set even though CLOCK would have
        // selected an over-quota LOB frame. If no matching cold candidate is
        // available within the bounded budget, CLOCK remains the fallback.
        let over_quota_mask = self.over_quota_kind_mask();
        let start = cold_start % self.shards.len();
        let mut inspected = 0usize;
        for offset in 0..self.shards.len() {
            let shard_idx = (start + offset) % self.shards.len();
            // Requeued pinned/dirty candidates must not be inspected again
            // in this pass. Otherwise one unavailable frame can consume the
            // entire budget and hide eligible victims in every later shard.
            let available = self.shards[shard_idx]
                .cold_candidates
                .lock()
                .map_err(|_| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "buffer pool cold queue lock poisoned",
                    )
                })?
                .len()
                .min(STALE_BUDGET.saturating_sub(inspected));
            for _ in 0..available {
                let candidate = self.shards[shard_idx]
                    .cold_candidates
                    .lock()
                    .map_err(|_| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "buffer pool cold queue lock poisoned",
                        )
                    })?
                    .pop_front();
                let Some(candidate) = candidate else { break };
                inspected += 1;
                let Some(frame) = candidate.upgrade() else {
                    if inspected >= STALE_BUDGET {
                        return Ok(None);
                    }
                    continue;
                };
                if !frame.scan_origin.load(Ordering::Acquire) {
                    if inspected >= STALE_BUDGET {
                        return Ok(None);
                    }
                    continue;
                }
                if over_quota_mask != 0 && over_quota_mask & (1 << frame.key.file.kind.slot()) == 0
                {
                    if let Ok(mut queue) = self.shards[shard_idx].cold_candidates.lock() {
                        queue.push_back(Arc::downgrade(&frame));
                    }
                    if inspected >= STALE_BUDGET {
                        return Ok(None);
                    }
                    continue;
                }
                if frame.pin_count.load(Ordering::Acquire) != 0
                    || !frame.is_ready()
                    || frame.deferred_pins.load(Ordering::Acquire) != 0
                    || (clean_only && frame.dirty.load(Ordering::Acquire))
                {
                    if let Ok(mut queue) = self.shards[shard_idx].cold_candidates.lock() {
                        queue.push_back(Arc::downgrade(&frame));
                    }
                    if inspected >= STALE_BUDGET {
                        return Ok(None);
                    }
                    continue;
                }
                return Ok(Some((shard_idx, frame.key.clone(), frame)));
            }
        }
        Ok(None)
    }

    /// True when the dirty fraction of the resident set is at or
    /// above `EVICT_DIRTY_PCT_FOR_PRESCAN_SKIP` — at which point the
    /// clean-only pre-scan is almost certainly going to walk its
    /// full `4 × pool_pages` budget and find nothing. Skipping it
    /// is a pure win: the dirty path's `find_clock_victim(false)`
    /// would have evaluated the same frames and accepted them on
    /// first encounter. The pre-scan is preserved when there is a
    /// reasonable clean reserve so the common mixed-read/write case
    /// keeps its low-cost happy path.
    fn dirty_ratio_high(&self) -> bool {
        let used = self.used_pages.load(Ordering::Relaxed);
        if used == 0 {
            return false;
        }
        let dirty = self.dirty_pages.load(Ordering::Relaxed);
        // dirty / used >= pct / 100  ⟺  100 × dirty >= used × pct.
        dirty.saturating_mul(100) >= used.saturating_mul(EVICT_DIRTY_PCT_FOR_PRESCAN_SKIP)
    }

    fn consume_victim(
        &self,
        victim: (usize, PageKey, Arc<Frame>),
        scan_origin_priority: bool,
    ) -> Result<bool, QueryError> {
        self.consume_victim_with_policy(victim, scan_origin_priority, true)
    }

    fn consume_victim_with_policy(
        &self,
        (shard_idx, key, frame): (usize, PageKey, Arc<Frame>),
        scan_origin_priority: bool,
        allow_dirty: bool,
    ) -> Result<bool, QueryError> {
        {
            let frames = self.shards[shard_idx].frames.read().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer pool shard lock poisoned",
                )
            })?;
            let Some(current) = frames.get(&key) else {
                return Ok(false);
            };
            if !Arc::ptr_eq(current, &frame)
                || frame
                    .pin_count
                    .compare_exchange(0, Self::EVICTING_PIN, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
            {
                return Ok(false);
            }
        }
        // Selection is only a hint. A writer can acquire a deferred WAL pin
        // after selection and release its ordinary pin before our CAS wins.
        // No new deferred pin can now be attached: add_deferred_pin uses the
        // same ordinary pin CAS, which rejects EVICTING_PIN.
        if frame.deferred_pins.load(Ordering::Acquire) != 0
            || (!allow_dirty && frame.dirty.load(Ordering::Acquire))
        {
            frame.pin_count.store(0, Ordering::Release);
            self.notify_frame_waiters();
            return Ok(false);
        }
        frame
            .state
            .store(FrameIoState::Evicting as u8, Ordering::Release);
        // D-2 - measure the synchronous dirty-victim flushes that still
        // happen (grace window expired or a race re-dirtied the frame).
        // This pair is the direct foreground-latency cost of eviction.
        let sync_flush_started = frame.dirty.load(Ordering::Acquire).then(Instant::now);
        if let Err(err) = self.flush_frame(&frame) {
            frame.publish_io_state(FrameIoState::Ready);
            frame.pin_count.store(0, Ordering::Release);
            self.notify_frame_waiters();
            self.flush_errors.fetch_add(1, Ordering::Relaxed);
            return Err(err);
        }
        if frame.dirty.load(Ordering::Acquire) {
            frame.publish_io_state(FrameIoState::Ready);
            frame.pin_count.store(0, Ordering::Release);
            self.notify_frame_waiters();
            return Ok(false);
        }
        if let Some(started) = sync_flush_started {
            self.evict_sync_dirty_flushes
                .fetch_add(1, Ordering::Relaxed);
            self.evict_sync_dirty_flush_us.fetch_add(
                started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
                Ordering::Relaxed,
            );
        }
        let mut frames = self.shards[shard_idx].frames.write().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "buffer pool shard lock poisoned",
            )
        })?;
        if frames
            .get(&key)
            .is_some_and(|current| Arc::ptr_eq(current, &frame))
        {
            if frame.prefetch.swap(false, Ordering::AcqRel) {
                self.prefetch_wasted.fetch_add(1, Ordering::Relaxed);
            }
            frames.remove(&key);
            frame.publish_io_state(FrameIoState::Discarded);
            self.used_pages.fetch_sub(1, Ordering::Relaxed);
            self.note_kind_removed(&key);
            self.evictions.fetch_add(1, Ordering::Relaxed);
            self.shards[shard_idx]
                .evictions
                .fetch_add(1, Ordering::Relaxed);
            let was_scan = self.release_scan_origin_flag(&frame);
            if scan_origin_priority || was_scan {
                self.scan_origin_evictions.fetch_add(1, Ordering::Relaxed);
            }
            if frame.dirty.swap(false, Ordering::AcqRel) {
                self.dirty_pages.fetch_sub(1, Ordering::Relaxed);
            }
            self.dirty_set_remove(&key);
            drop(frames);
            self.recycle_frame_raw_page(shard_idx, &frame);
            // Leave the now-stale `key` in the clock VecDeque rather
            // than running `clock.retain` here (which is O(clock.len)).
            // `find_clock_victim` already tolerates dead keys — the
            // `frames.get(&key)` lookup returns `None` and the hand
            // skips on. `retain_live_clock_keys` then amortises the
            // compaction across many evictions instead of paying it
            // per evict. On a TPC-H Q18 sf=5 heap scan (lineitem >
            // BP capacity) profile showed `remove_clock_key` at ~7%
            // of CPU on the per-evict path; this removes that line
            // from the hot loop entirely.
            self.notify_frame_waiters();
            return Ok(true);
        }
        frame.publish_io_state(FrameIoState::Ready);
        frame.pin_count.store(0, Ordering::Release);
        self.notify_frame_waiters();
        Ok(false)
    }

    #[cfg(test)]
    fn find_clock_victim(
        &self,
        clean_only: bool,
    ) -> Result<Option<(usize, PageKey, Arc<Frame>)>, QueryError> {
        let start = self.eviction_hand.fetch_add(1, Ordering::Relaxed);
        self.find_clock_victim_from(clean_only, start)
    }

    fn find_clock_victim_from(
        &self,
        clean_only: bool,
        start: usize,
    ) -> Result<Option<(usize, PageKey, Arc<Frame>)>, QueryError> {
        // Q-1 - sampled once per victim search: kinds above their soft
        // quota (while other kinds hold pages) get a complete CLOCK pass
        // before under-quota kinds. The old per-shard "lose second chance"
        // preference could still evict the last heap page merely because its
        // shard was visited before a shard containing over-quota LOB pages.
        // A second ordinary CLOCK pass preserves progress when every
        // over-quota frame is pinned, dirty in a clean-only pass, or loading.
        let over_quota_mask = self.over_quota_kind_mask();
        if over_quota_mask != 0 {
            if let Some(victim) =
                self.find_clock_victim_with_mask(clean_only, over_quota_mask, start)?
            {
                return Ok(Some(victim));
            }
        }
        self.find_clock_victim_with_mask(clean_only, 0, start)
    }

    fn find_clean_victim_bounded(
        &self,
        start: usize,
    ) -> Result<Option<(usize, PageKey, Arc<Frame>)>, QueryError> {
        if self.used_pages.load(Ordering::Relaxed) <= self.dirty_pages.load(Ordering::Relaxed) {
            return Ok(None);
        }
        const PROBES_PER_SHARD: usize = 32;
        let over_quota_mask = self.over_quota_kind_mask();
        if over_quota_mask != 0 {
            if let Some(victim) = self.find_clock_victim_with_mask_budget(
                true,
                over_quota_mask,
                start,
                PROBES_PER_SHARD,
            )? {
                return Ok(Some(victim));
            }
        }
        self.find_clock_victim_with_mask_budget(true, 0, start, PROBES_PER_SHARD)
    }

    fn find_clock_victim_with_mask(
        &self,
        clean_only: bool,
        quota_only_mask: u8,
        start: usize,
    ) -> Result<Option<(usize, PageKey, Arc<Frame>)>, QueryError> {
        // Probe every shard before exhaustively sweeping any one of them.
        // Range reads can pin most of a shard while another shard already
        // has an eligible victim. A full first-shard sweep costs O(pool size)
        // for every LOB/cache miss under that pressure. Keep the same kind
        // mask in both passes so soft-quota preference is preserved; the
        // complete fallback still guarantees progress beyond the probe window.
        if let Some(victim) =
            self.find_clock_victim_with_mask_budget(clean_only, quota_only_mask, start, 32)?
        {
            return Ok(Some(victim));
        }
        self.find_clock_victim_with_mask_budget(clean_only, quota_only_mask, start, usize::MAX)
    }

    fn find_clock_victim_with_mask_budget(
        &self,
        clean_only: bool,
        quota_only_mask: u8,
        start: usize,
        probes_per_shard: usize,
    ) -> Result<Option<(usize, PageKey, Arc<Frame>)>, QueryError> {
        let shard_count = self.shards.len();
        let start = start % shard_count;
        // PERF-1 - the old hand walked the clock with `shard.clock` held
        // for the ENTIRE `len*4` sweep AND took a nested `shard.frames`
        // read lock on every probe. That serialised inserts
        // (`push_clock_key` needs the clock lock) and the victim removal
        // (`consume_victim` needs the frames write lock) behind the whole
        // eviction scan — under data-set > pool pressure (1 evict per
        // insert) the shard went effectively single-threaded.
        //
        // Now each round samples a small WINDOW of candidate keys while
        // briefly holding the clock lock (advancing the hand), releases
        // it, and evaluates that window under a single short-lived frames
        // read lock. The ref_bit / usage_count second-chance mutations
        // are frame atomics, so doing them outside the clock lock changes
        // nothing about their concurrency; any victim is re-validated by
        // `consume_victim`'s `pin_count` CAS before removal.
        const VICTIM_SAMPLE_WINDOW: usize = 16;
        for offset in 0..shard_count {
            let shard_idx = (start + offset) % shard_count;
            let shard = &self.shards[shard_idx];
            let max_scans = {
                let clock = shard.clock.lock().map_err(|_| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "buffer pool clock lock poisoned",
                    )
                })?;
                clock.len().saturating_mul(4).max(1).min(probes_per_shard)
            };
            let mut scanned = 0usize;
            while scanned < max_scans {
                // Snapshot a window of consecutive keys starting at the
                // current hand WITHOUT advancing the persistent hand yet —
                // the hand is advanced below by the number of candidates we
                // actually evaluate, so the second-chance sweep (ref_bit
                // clear / usage decrement) stays uniform and the hand never
                // jumps over un-evaluated frames. Two evictors that race on
                // the same base re-evaluate the same window; that is benign
                // (ref_bit/usage ops are idempotent enough and the victim
                // is re-validated by consume_victim's CAS) and still far
                // cheaper than the old whole-sweep clock lock.
                // PERF - the candidate window is a fixed `VICTIM_SAMPLE_WINDOW`
                // slots, so snapshot it into a stack array instead of a fresh
                // heap `Vec<PageKey>` per round. Under data-set > pool pressure
                // this inner block runs ~120 times per eviction and millions of
                // times per second; the old per-window `Vec` allocation showed
                // up directly in the allocator profile of the eviction hot loop.
                let mut candidate_buf: [Option<PageKey>; VICTIM_SAMPLE_WINDOW] = Default::default();
                let candidate_count = {
                    let clock = shard.clock.lock().map_err(|_| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "buffer pool clock lock poisoned",
                        )
                    })?;
                    let len = clock.len();
                    if len == 0 {
                        break;
                    }
                    let base = (shard.hand.load(Ordering::Relaxed)) % len;
                    let take = VICTIM_SAMPLE_WINDOW
                        .min(max_scans.saturating_sub(scanned))
                        .min(len);
                    let mut n = 0usize;
                    for i in 0..take {
                        if let Some(k) = clock.get((base + i) % len) {
                            candidate_buf[n] = Some(k.clone());
                            n += 1;
                        }
                    }
                    n
                };
                if candidate_count == 0 {
                    break;
                }
                let candidates = &candidate_buf[..candidate_count];
                // Evaluate the window under a single frames read lock with
                // the clock lock released.
                let mut consumed = 0u64;
                let mut victim: Option<(PageKey, Arc<Frame>)> = None;
                {
                    let frames = shard.frames.read().map_err(|_| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "buffer pool shard lock poisoned",
                        )
                    })?;
                    for key in candidates.iter().flatten() {
                        scanned += 1;
                        consumed += 1;
                        self.eviction_scans.fetch_add(1, Ordering::Relaxed);
                        let Some(frame) = frames.get(key) else {
                            continue;
                        };
                        if frame.pin_count.load(Ordering::Acquire) != 0 || !frame.is_ready() {
                            continue;
                        }
                        // Txn-buffering: a frame with un-drained deferred writers
                        // cannot be flushed (its WAL isn't written yet), so it must
                        // not be evicted either — discarding it would lose the
                        // change. Treat like a pinned frame. (Always 0 unless
                        // MYDB_WAL_TXN_BUFFER is on.)
                        if frame.deferred_pins.load(Ordering::Acquire) != 0 {
                            continue;
                        }
                        if clean_only && frame.dirty.load(Ordering::Acquire) {
                            // Writeback eligibility must not freeze cache heat.
                            // The cleaner ranks dirty candidates using the same
                            // CLOCK heat as eviction; otherwise cold dirty pages
                            // stay young forever while clean hot pages churn.
                            frame.clock_second_chance();
                            continue;
                        }
                        if quota_only_mask != 0
                            && quota_only_mask & (1 << key.file.kind.slot()) == 0
                        {
                            continue;
                        }
                        // N-2 - scan-origin frames skip the ref_bit /
                        // usage_count discount and become an immediate
                        // victim. A Normal access promotes the frame out
                        // of scan_origin before the hand reaches it.
                        if frame.scan_origin.load(Ordering::Acquire) {
                            victim = Some((key.clone(), frame.clone()));
                            break;
                        }
                        // Q-1 - over-quota kinds recycle among themselves.
                        if quota_only_mask != 0
                            && quota_only_mask & (1 << key.file.kind.slot()) != 0
                        {
                            self.quota_evictions.fetch_add(1, Ordering::Relaxed);
                            victim = Some((key.clone(), frame.clone()));
                            break;
                        }
                        if frame.clock_second_chance() {
                            continue;
                        }
                        victim = Some((key.clone(), frame.clone()));
                        break;
                    }
                }
                // Advance the persistent hand by exactly the frames we
                // evaluated (victim included), so the next round resumes
                // immediately past them.
                shard.hand.fetch_add(consumed as usize, Ordering::Relaxed);
                if let Some((key, frame)) = victim {
                    return Ok(Some((shard_idx, key, frame)));
                }
            }
            // A bounded probe must not turn into a full-shard walk here.
            // The ordinary complete CLOCK pass still amortises stale-key
            // compaction across evictions.
            if probes_per_shard == usize::MAX {
                if let Ok(mut clock) = shard.clock.lock() {
                    self.retain_live_clock_keys(shard_idx, &mut clock);
                }
            }
        }
        Ok(None)
    }

    fn retain_live_clock_keys(&self, shard_idx: usize, clock: &mut VecDeque<PageKey>) {
        if let Ok(frames) = self.shards[shard_idx].frames.read() {
            clock.retain(|key| frames.contains_key(key));
        }
    }

    #[allow(dead_code)]
    fn background_flush_once(&self, worker_id: usize, stride: usize) -> Result<(), QueryError> {
        let capacity = self.capacity_pages.load(Ordering::Relaxed).max(1);
        let dirty = self.dirty_pages.load(Ordering::Relaxed);
        // P0-1 - sample the dirty ratio once per background iteration.
        // 8 buckets at 12.5% width — sufficient to spot whether we
        // spend most of our time idle (bucket 0) vs above the 75%
        // high-watermark (bucket 6/7), which says background flush
        // can't keep up.
        let bucket = ((dirty.saturating_mul(8) / capacity) as usize).min(7);
        self.dirty_ratio_histogram[bucket].fetch_add(1, Ordering::Relaxed);
        self.dirty_ratio_samples.fetch_add(1, Ordering::Relaxed);
        // Absolute cap on the high water — protects the case where the
        // buffer pool is sized far above the working set: 75 % of an
        // 865 K-page pool is ~13 GiB of dirty data, which a TPC-H sf=5
        // load never reaches, so background flush would never run and
        // every dirty page would land on the foreground `flush_all` at
        // the post-LOAD checkpoint. Capping at ABS_HIGH (~768 MiB)
        // forces background flush to start as soon as that much dirty
        // data accumulates, regardless of buffer pool capacity.
        // P0-2 - the absolute caps scale with the pool once it is large: a
        // fixed 768 MiB cap on a multi-GiB pool keeps the cleaner in a
        // permanent high-pressure regime under write-heavy OLTP (the cap is
        // exceeded for the whole run), which defeats cold-first selection
        // and rewrites hot pages continuously. Small pools keep the exact
        // historical arm (`min()` still picks the relative value below
        // ~3 GiB / ~1.5 GiB respectively).
        let abs_high =
            BUFFER_POOL_DIRTY_ABS_HIGH_PAGES.max(capacity / BUFFER_POOL_DIRTY_ABS_SCALE_DIV);
        let high = (capacity * dirty_high_pct() / 100).min(abs_high).max(1);
        let panic = (capacity * BUFFER_POOL_DIRTY_PANIC_PCT / 100).max(1);
        // P0-2 - re-dirtied-soon frames are deferred by the cooldown below,
        // but never past the panic watermark: above it the cleaners must
        // reclaim pages unconditionally so the foreground does not stall.
        let ignore_cooldown = dirty > panic;
        // Recovery progress must not depend on whether the dirty-count
        // watermark happens to be exceeded. Capacity cleaning below prefers
        // cold victims and can otherwise leave hot, old rec_lsns behind.
        if dirty != 0 {
            self.flush_oldest_dirty_striped_limited(
                worker_id,
                stride,
                age_clean_pages_per_tick(),
                ignore_cooldown,
            )?;
        }
        if dirty <= high {
            return Ok(());
        }
        // Every cleaner reaches this branch on the same dirty high-water
        // condition. Do not broadcast from inside the worker loop: with more
        // than one worker, each awakened peer used to broadcast again before
        // parking, creating a self-sustaining notify-all storm while dirty
        // stayed above `high`. The 25 ms periodic wake already brings the
        // complete fleet in promptly. Foreground capacity pressure and newly
        // queued prefetches retain their explicit immediate wakeups.
        // M-6 - three-tier response. Under 90% we aim for the regular
        // target; over 90% we push to a tighter target so the
        // backlog clears before the foreground has to fsync to make
        // room. The periodic fleet distributes shard claims across IO
        // threads without an in-loop broadcast.
        let (target_pct, abs_target) = if ignore_cooldown {
            // Panic mode keeps its own absolute floor at half of the
            // regular cap — when we're truly over-capacity, push hard
            // enough to clear the backlog without thrashing.
            (
                BUFFER_POOL_DIRTY_PANIC_TARGET_PCT,
                BUFFER_POOL_DIRTY_ABS_TARGET_PAGES / 2,
            )
        } else {
            (
                dirty_target_pct(),
                BUFFER_POOL_DIRTY_ABS_TARGET_PAGES
                    .max(capacity / BUFFER_POOL_DIRTY_ABS_TARGET_SCALE_DIV),
            )
        };
        let target = (capacity * target_pct / 100).min(abs_target).max(1);
        // B4 - dynamic shard claiming replaces static `shard_idx % stride`
        // ownership so the IO fleet self-balances under a skewed dirty
        // distribution.
        self.flush_dirty_dynamic(target, ignore_cooldown)
    }

    /// B4 - flush the pool down towards `target_dirty_pages` by pulling
    /// shards from the shared `flush_cursor` instead of statically owning a
    /// `shard_idx % stride` stripe. A worker keeps claiming the next shard
    /// (up to one full sweep of `shard_count` shards) and flushing its dirty
    /// frames down to the per-shard target, stopping early the moment global
    /// dirty drops back under target. Because claims come from one atomic
    /// cursor, concurrent workers process disjoint shards (a fast worker
    /// simply claims more) and no worker idles while a hot shard is drained
    /// by a single peer; a lone worker (the stride-1 headroom path) still
    /// sweeps every shard. The collect stays O(dirty-in-shard) per claim —
    /// there is no whole-pool frame rebuild.
    fn flush_dirty_dynamic(
        &self,
        target_dirty_pages: u64,
        ignore_cooldown: bool,
    ) -> Result<(), QueryError> {
        if self.durability == DurabilityMode::Memory {
            return Ok(());
        }
        let shard_count = self.shards.len().max(1);
        // Per-shard fair share of the global target. Summed over all shards
        // this converges the whole pool to `target_dirty_pages`. Round up so
        // a tiny target (e.g. 3 pages over 16 shards → 0) still lets each
        // shard keep a small residue instead of being driven fully clean,
        // while the global early-out below enforces the real target.
        let per_shard_target = target_dirty_pages / shard_count as u64;
        // One bounded shard claim per worker tick. The former loop made every
        // worker perform a complete shard sweep, so workers overlapped after
        // the first `shard_count` tickets and a hot shard could monopolise a
        // worker for thousands of page writes. Shared round-robin claiming
        // still balances skew, while the fleet collectively advances the
        // cursor and each invocation performs bounded work.
        if self.dirty_pages.load(Ordering::Relaxed) > target_dirty_pages {
            let shard_idx = self.flush_cursor.fetch_add(1, Ordering::Relaxed) % shard_count;
            self.flush_shard_dirty_to(shard_idx, per_shard_target, ignore_cooldown)?;
        }
        Ok(())
    }

    /// B4 - flush one shard's CLOCK-cold dirty frames down to
    /// `per_shard_target` resident dirty pages. Shares the batch /
    /// WAL-coalescing machinery with the striped path.
    fn flush_shard_dirty_to(
        &self,
        shard_idx: usize,
        per_shard_target: u64,
        ignore_cooldown: bool,
    ) -> Result<(), QueryError> {
        let shard = &self.shards[shard_idx];
        let dirty_remaining = shard
            .dirty_set
            .lock()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer pool dirty set lock poisoned",
                )
            })?
            .len() as u64;
        if dirty_remaining <= per_shard_target {
            return Ok(());
        }
        let wanted = (dirty_remaining.saturating_sub(per_shard_target) as usize)
            .min(BUFFER_POOL_FLUSH_BATCH_PAGES);
        // Sampling cost is bounded by the batch size, not the resident dirty
        // set. A separate cursor visits the whole shard over successive ticks
        // without changing CLOCK's eviction heat or second chances.
        let mut frames = self.collect_dirty_clock_window(
            shard_idx,
            BUFFER_POOL_FLUSH_BATCH_PAGES * 8,
            ignore_cooldown,
        )?;
        if !ignore_cooldown {
            // A count watermark is not evidence that a hot page needs
            // writeback. Leave repeated updates on resident hot pages to
            // coalesce. Foreground headroom and panic cleaning bypass this
            // preference; the oldest-LSN budget advances recovery separately.
            frames.retain(|frame| {
                !frame.ref_bit.load(Ordering::Acquire)
                    && frame.usage_count.load(Ordering::Acquire) == 0
            });
        }
        // Capacity-pressure cleaning exists to manufacture the same clean
        // victims CLOCK is about to request. Prefer low-usage, unreferenced
        // dirty frames here instead of blindly flushing the oldest recovery
        // LSN: an old but frequently modified B-tree leaf would otherwise be
        // written, dirtied again, and written a second time before eviction.
        // The below-high-water maintenance path remains oldest-LSN-first, so
        // WAL retention still advances continuously.
        let frames = cold_frames_in_io_order(frames, wanted);
        // PageKey hashes include page_id, so adjacent physical pages normally
        // live in different buffer-map shards. Selecting only this shard
        // therefore guarantees that the later file/page sort sees almost no
        // contiguous runs. Use the cold frames as eviction-oriented seeds and
        // opportunistically add their resident dirty neighbours across shard
        // boundaries. The expansion is bounded to one extra cleaner batch and
        // carries no retained memory: on sparse workloads it simply returns
        // the seeds unchanged.
        let mut frames = self.expand_dirty_neighbors_for_flush(
            frames,
            wanted.saturating_add(BUFFER_POOL_FLUSH_BATCH_PAGES),
        )?;
        // `flush_frame_batch` already separates non-contiguous file/page runs
        // for positioned writes. Keep the whole cleaner selection in one call
        // so its WAL-before-data barrier is advanced once to the selection's
        // high-water mark instead of once per physical run. The old nested
        // grouping could issue several incremental WAL fsyncs for one cleaner
        // tick when cold pages were scattered across heap and index files.
        // Preserve the per-shard target even though neighbour expansion may
        // have found one extra batch of coalescing candidates.
        frames.truncate(wanted);
        let flushed = self.flush_frame_batch(&frames)?;
        self.background_flushes
            .fetch_add(flushed as u64, Ordering::Relaxed);
        Ok(())
    }

    fn collect_dirty_clock_window(
        &self,
        shard_idx: usize,
        max_probes: usize,
        ignore_cooldown: bool,
    ) -> Result<Vec<Arc<Frame>>, QueryError> {
        let shard = &self.shards[shard_idx];
        let keys = {
            let clock = shard.clock.lock().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer pool clock lock poisoned",
                )
            })?;
            let take = max_probes.min(clock.len());
            if take == 0 {
                return Ok(Vec::new());
            }
            let base = shard.cleaner_hand.fetch_add(take, Ordering::Relaxed) % clock.len();
            (0..take)
                .map(|i| clock[(base + i) % clock.len()].clone())
                .collect::<Vec<_>>()
        };
        // Follow the eviction path's lock order: never hold clock while
        // acquiring frames. Concurrent removal/replacement is revalidated here
        // and again by the normal epoch-checked flush protocol.
        let frames = shard.frames.read().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "buffer pool shard lock poisoned",
            )
        })?;
        // P0-2 - defer write-back-cooled frames (flushed and re-dirtied
        // within the window). The panic watermark bypasses this via
        // `ignore_cooldown` so the high-pressure regime can always reclaim.
        let cooldown_us = flush_cooldown_us();
        let now_us = if ignore_cooldown || cooldown_us == 0 {
            0
        } else {
            self.now_us()
        };
        Ok(keys
            .iter()
            .filter_map(|key| frames.get(key))
            .filter(|frame| {
                frame.dirty.load(Ordering::Acquire)
                    && frame.pin_count.load(Ordering::Acquire) == 0
                    && frame.deferred_pins.load(Ordering::Acquire) == 0
                    && frame.is_ready()
            })
            .filter(|frame| {
                if now_us == 0 {
                    return true;
                }
                let flushed_at = frame.flushed_at_us.load(Ordering::Acquire);
                if flushed_at == 0 || now_us.saturating_sub(flushed_at) >= cooldown_us {
                    return true;
                }
                self.cooldown_deferred_flushes
                    .fetch_add(1, Ordering::Relaxed);
                false
            })
            .cloned()
            .collect())
    }

    fn expand_dirty_neighbors_for_flush(
        &self,
        seeds: Vec<Arc<Frame>>,
        limit: usize,
    ) -> Result<Vec<Arc<Frame>>, QueryError> {
        let limit = limit.max(seeds.len());
        if seeds.is_empty() || seeds.len() >= limit {
            return Ok(seeds);
        }
        let mut frames = Vec::with_capacity(limit);
        for seed in seeds {
            if !frames
                .iter()
                .any(|frame: &Arc<Frame>| frame.key == seed.key)
            {
                frames.push(seed);
            }
        }

        // Grow symmetrically so a cold seed in the middle of an extent can
        // still produce one useful run. Four pages on either side is enough
        // to amortise F_NOCACHE/direct-I/O syscall cost without turning a
        // pressure cleaner into a sequential file scan.
        for distance in 1..=4_u64 {
            let seed_count = frames.len();
            for idx in 0..seed_count {
                let seed = Arc::clone(&frames[idx]);
                for page_id in [
                    seed.key.page_id.checked_sub(distance),
                    seed.key.page_id.checked_add(distance),
                ]
                .into_iter()
                .flatten()
                {
                    if frames.len() >= limit {
                        break;
                    }
                    let key = PageKey {
                        file: Arc::clone(&seed.key.file),
                        page_id,
                    };
                    if frames.iter().any(|frame| frame.key == key) {
                        continue;
                    }
                    let Some(frame) = self.lookup_frame(&key)? else {
                        continue;
                    };
                    if frame.pin_count.load(Ordering::Acquire) != 0
                        || !frame.is_ready()
                        || !frame.dirty.load(Ordering::Acquire)
                        || frame.deferred_pins.load(Ordering::Acquire) != 0
                        || frame.ref_bit.load(Ordering::Acquire)
                        || frame.usage_count.load(Ordering::Acquire) != 0
                    {
                        continue;
                    }
                    frames.push(frame);
                }
                if frames.len() >= limit {
                    break;
                }
            }
            if frames.len() >= limit {
                break;
            }
        }
        frames.sort_unstable_by(|a, b| {
            a.key
                .file
                .hash
                .cmp(&b.key.file.hash)
                .then_with(|| a.key.page_id.cmp(&b.key.page_id))
        });
        Ok(frames)
    }

    /// P5 - Best-effort preflush used by the automatic checkpointer before it
    /// takes the checkpoint mutex. It writes a bounded number of dirty pages so
    /// the subsequent strict `flush_all` has less backlog to drain in one
    /// latency spike. This is an optimization only: pinned/deferred/racing pages
    /// are skipped and remain covered by the checkpoint's full flush.
    pub(crate) fn checkpoint_preflush(&self) -> Result<(), QueryError> {
        if self.durability == DurabilityMode::Memory {
            return Ok(());
        }
        let max_ms = env::var("MYDB_CHECKPOINT_PREFLUSH_MS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(CHECKPOINT_PREFLUSH_MS);
        if max_ms == 0 {
            return Ok(());
        }
        let capacity = self.capacity_pages.load(Ordering::Relaxed).max(1);
        let target_pct = env::var("MYDB_CHECKPOINT_PREFLUSH_TARGET_PCT")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .map(|v| v.clamp(1, dirty_high_pct().saturating_sub(1).max(1)))
            .unwrap_or(CHECKPOINT_PREFLUSH_TARGET_PCT);
        let target = (capacity * target_pct / 100)
            .min(BUFFER_POOL_DIRTY_ABS_TARGET_PAGES / 2)
            .max(1);
        let dirty_before = self.dirty_pages.load(Ordering::Relaxed);
        if dirty_before == 0 {
            return Ok(());
        }

        self.wake_workers();
        let started = Instant::now();
        let deadline = Duration::from_millis(max_ms);
        let max_pages_per_round = env::var("MYDB_CHECKPOINT_PREFLUSH_BATCH_PAGES")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(CHECKPOINT_PREFLUSH_BATCH_PAGES)
            .max(1);
        // Even when the pool is already at/below the count target, retire one
        // bounded oldest-LSN batch. Count-only cleaning used to leave the same
        // high-page-id frames dirty forever; their ancient rec_lsn pinned every
        // WAL segment and made later checkpoints rewrite/fsync an ever-growing
        // log. Above target we still converge to the configured target; at or
        // below it this lowers the target by at most one maintenance batch.
        let effective_target = target.min(dirty_before.saturating_sub(max_pages_per_round));
        let mut total_flushed = 0_u64;
        while self.dirty_pages.load(Ordering::Relaxed) > effective_target
            && started.elapsed() < deadline
        {
            let flushed = self.flush_some_dirty_limited(effective_target, max_pages_per_round)?;
            if flushed == 0 {
                break;
            }
            total_flushed = total_flushed.saturating_add(flushed);
        }
        if total_flushed > 0 {
            self.checkpoint_preflush_pages
                .fetch_add(total_flushed, Ordering::Relaxed);
            self.checkpoint_preflush_us.fetch_add(
                started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
                Ordering::Relaxed,
            );
        }
        Ok(())
    }

    fn flush_some_dirty_limited(
        &self,
        target_dirty_pages: u64,
        max_pages: u64,
    ) -> Result<u64, QueryError> {
        if self.durability == DurabilityMode::Memory {
            return Ok(0);
        }
        let frames = self
            .collect_dirty_frames()?
            .into_iter()
            .filter(|frame| frame.pin_count.load(Ordering::Acquire) == 0 && frame.is_ready())
            .collect::<Vec<_>>();
        let dirty_remaining = frames.len() as u64;
        if dirty_remaining <= target_dirty_pages || max_pages == 0 {
            return Ok(0);
        }
        let wanted = dirty_remaining
            .saturating_sub(target_dirty_pages)
            .min(max_pages) as usize;
        let frames = oldest_frames_in_io_order(frames, wanted);
        // One logical cleaner batch gets one WAL durability barrier. Physical
        // run coalescing remains inside `flush_frame_batch`.
        let total_flushed = self.flush_frame_batch(&frames)? as u64;
        self.background_flushes
            .fetch_add(total_flushed, Ordering::Relaxed);
        Ok(total_flushed)
    }

    /// Retire at most `max_pages` from this worker's stripe, ordered by the
    /// first LSN that dirtied each page. Unlike count-watermark cleaning this
    /// remains active for a small stable dirty set, preventing an ancient page
    /// from indefinitely pinning the redo/WAL retention floor.
    fn flush_oldest_dirty_striped_limited(
        &self,
        worker_id: usize,
        stride: usize,
        max_pages: u64,
        ignore_cooldown: bool,
    ) -> Result<(), QueryError> {
        if self.durability == DurabilityMode::Memory || max_pages == 0 {
            return Ok(());
        }
        let frames = if oldest_dirty_queue_enabled() {
            let queued =
                self.pop_oldest_dirty_frames_striped(worker_id, stride, max_pages as usize)?;
            if queued.is_empty() {
                // The heap is an acceleration structure, never the source of
                // truth. A stale/missing candidate must not strand a dirty
                // frame forever and pin the recovery floor. Falling back only
                // after an empty queue keeps the normal tick O(log n).
                oldest_frames_in_io_order(
                    self.collect_dirty_frames_striped(worker_id, stride)?
                        .into_iter()
                        .filter(|frame| {
                            frame.pin_count.load(Ordering::Acquire) == 0 && frame.is_ready()
                        })
                        .collect::<Vec<_>>(),
                    max_pages as usize,
                )
            } else {
                queued
            }
        } else {
            oldest_frames_in_io_order(
                self.collect_dirty_frames_striped(worker_id, stride)?
                    .into_iter()
                    .filter(|frame| {
                        frame.pin_count.load(Ordering::Acquire) == 0 && frame.is_ready()
                    })
                    .collect::<Vec<_>>(),
                max_pages as usize,
            )
        };
        // P0-2 - maintenance write-backs also honour the cooldown: the
        // oldest-LSN arm would otherwise rewrite the same hot pages every
        // tick (they sit at the top of the heap permanently). Deferred
        // frames are requeued with their current LSN so the recovery-floor
        // heap stays complete; the periodic checkpoint preflush (which
        // ignores the cooldown) still advances the floor within one
        // `checkpoint_interval`.
        let cooldown_us = if ignore_cooldown {
            0
        } else {
            flush_cooldown_us()
        };
        let frames = if cooldown_us == 0 || frames.is_empty() {
            frames
        } else {
            let now_us = self.now_us();
            let mut retained = Vec::with_capacity(frames.len());
            for frame in frames {
                let flushed_at = frame.flushed_at_us.load(Ordering::Acquire);
                if flushed_at != 0 && now_us.saturating_sub(flushed_at) < cooldown_us {
                    self.cooldown_deferred_flushes
                        .fetch_add(1, Ordering::Relaxed);
                    self.note_dirty(&frame.key, frame.first_dirty_lsn.load(Ordering::Acquire));
                    continue;
                }
                retained.push(frame);
            }
            retained
        };
        if frames.is_empty() {
            return Ok(());
        }
        let flushed = self.flush_frame_batch(&frames)?;
        // A deferred WAL pin or concurrent re-dirty can make an attempted
        // candidate remain dirty. Requeue its current recovery LSN so it is
        // retried without waiting for another clean→dirty transition.
        for frame in &frames {
            if frame.dirty.load(Ordering::Acquire) {
                self.note_dirty(&frame.key, frame.first_dirty_lsn.load(Ordering::Acquire));
            }
        }
        self.background_flushes
            .fetch_add(flushed as u64, Ordering::Relaxed);
        Ok(())
    }

    fn flush_frame_batch(&self, frames: &[Arc<Frame>]) -> Result<usize, QueryError> {
        let max_dirty_lsn = frames
            .iter()
            .filter(|frame| frame.dirty.load(Ordering::Acquire))
            .map(|frame| frame.dirty_lsn.load(Ordering::Acquire))
            .max()
            .unwrap_or(0);
        if max_dirty_lsn != 0 {
            // Drive durability via the unlocked-fsync entry so the
            // actual `fsync` does not hold the WAL mutex; concurrent
            // commit appenders make progress against the same WAL
            // while we wait on the syscall.
            crate::wal::LogManager::sync_durable_to(&self.log, max_dirty_lsn)?;
        }
        let mut flushed = 0;
        let mut start = 0;
        while start < frames.len() {
            let mut end = start + 1;
            while end < frames.len()
                && frames[end].key.file == frames[start].key.file
                && frames[end].key.page_id == frames[end - 1].key.page_id + 1
            {
                end += 1;
            }
            let run = &frames[start..end];
            if run.len() > 1 {
                if let Some(run_flushed) = self.flush_contiguous_frame_run(run, max_dirty_lsn)? {
                    flushed += run_flushed;
                    start = end;
                    continue;
                }
            }
            for frame in run {
                if self.flush_frame_with_wal(frame, max_dirty_lsn)? {
                    flushed += 1;
                }
            }
            start = end;
        }
        Ok(flushed)
    }

    /// Flush one same-file, page-id-contiguous run with a single positioned
    /// write. The cleaner already restores dirty candidates to file/page
    /// order, but the former batch path still issued one 16 KiB `pwrite` per
    /// page. Under a small buffer pool that turned the page cleaner into the
    /// dominant cost of concurrent indexed INSERTs.
    ///
    /// The combined image is transient `StorageScratch`, charged by Vec
    /// capacity for exactly the duration of this call. If the governor has no
    /// scratch headroom, return `None` and let the caller use the canonical
    /// one-page path; memory pressure can therefore reduce batching without
    /// weakening `MYDB_MEMORY_TARGET`.
    fn flush_contiguous_frame_run(
        &self,
        frames: &[Arc<Frame>],
        synced_lsn: u64,
    ) -> Result<Option<usize>, QueryError> {
        debug_assert!(frames.len() > 1);
        debug_assert!(frames.windows(2).all(|pair| {
            pair[0].key.file == pair[1].key.file && pair[0].key.page_id + 1 == pair[1].key.page_id
        }));

        let bytes_capacity = match frames.len().checked_mul(PAGE_SIZE) {
            Some(bytes) => bytes,
            None => return Ok(None),
        };
        let Some(_scratch) =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                bytes_capacity,
            )
        else {
            return Ok(None);
        };
        let mut images = Vec::new();
        if images.try_reserve_exact(bytes_capacity).is_err() {
            return Ok(None);
        }

        // Every batch acquires frame flush locks in ascending page order.
        // Foreground eviction takes only one such lock, so overlapping
        // cleaner batches cannot form a lock cycle.
        let mut flush_guards = Vec::with_capacity(frames.len());
        for frame in frames {
            let guard = frame.flush_lock.lock().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer frame flush lock poisoned",
                )
            })?;
            flush_guards.push(guard);
        }

        // A deferred writer or an already-clean/discarded frame creates a hole
        // in the physical run. Fall back as a unit so the caller can re-check
        // each page through `flush_frame_with_wal`.
        if frames.iter().any(|frame| {
            !frame.dirty.load(Ordering::Acquire)
                || frame.deferred_pins.load(Ordering::Acquire) != 0
                || frame.state.load(Ordering::Acquire) == FrameIoState::Discarded as u8
        }) {
            drop(flush_guards);
            return Ok(None);
        }

        let generation = frames[0].generation.load(Ordering::Acquire);
        if frames
            .iter()
            .any(|frame| frame.generation.load(Ordering::Acquire) != generation)
        {
            drop(flush_guards);
            return Ok(None);
        }

        struct FlushSnapshot {
            epoch: u64,
            generation: u64,
        }
        let mut snapshots = Vec::with_capacity(frames.len());
        let mut newest_lsn = synced_lsn;
        for frame in frames {
            let mut bytes_guard = frame.bytes.write().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer frame write lock poisoned",
                )
            })?;
            frame.materialize_packed_leaf(&mut bytes_guard)?;
            if frame.deferred_pins.load(Ordering::Acquire) != 0 {
                return Ok(None);
            }
            let dirty_lsn = frame.dirty_lsn.load(Ordering::Acquire);
            newest_lsn = newest_lsn.max(dirty_lsn);
            if frame.checksum_dirty.load(Ordering::Acquire) {
                set_page_lsn_if_formatted(&mut bytes_guard, dirty_lsn);
                frame.checksum_dirty.store(false, Ordering::Release);
            }
            images.extend_from_slice(&bytes_guard);
            snapshots.push(FlushSnapshot {
                epoch: frame.dirty_epoch.load(Ordering::Acquire),
                generation,
            });
        }

        if self.durability != DurabilityMode::Memory && newest_lsn > synced_lsn {
            crate::wal::LogManager::sync_durable_to(&self.log, newest_lsn)?;
        }
        for frame in frames {
            frame.publish_io_state(FrameIoState::Flushing);
        }
        let result = self.write_page_run_bytes_with_generation(
            &frames[0].key,
            generation,
            &images,
            frames.len(),
        );
        for frame in frames {
            frame.publish_io_state(
                if frame.pin_count.load(Ordering::Acquire) == Self::EVICTING_PIN {
                    FrameIoState::Evicting
                } else {
                    FrameIoState::Ready
                },
            );
        }
        result?;

        let mut cleared_count = 0_u64;
        let writeback_us = self.now_us();
        for (frame, snapshot) in frames.iter().zip(&snapshots) {
            let cleared = {
                let _bytes_guard = frame.bytes.read().map_err(|_| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "buffer frame read lock poisoned",
                    )
                })?;
                if frame.dirty_epoch.load(Ordering::Acquire) == snapshot.epoch
                    && frame.generation.load(Ordering::Acquire) == snapshot.generation
                {
                    let was_dirty = frame.dirty.swap(false, Ordering::AcqRel);
                    frame.dirty_lsn.store(0, Ordering::Release);
                    frame.first_dirty_lsn.store(0, Ordering::Release);
                    // P0-2 - write-back stamp for the cleaner cooldown (see
                    // `flush_frame_with_wal`).
                    frame.flushed_at_us.store(writeback_us, Ordering::Release);
                    was_dirty
                } else {
                    false
                }
            };
            if cleared {
                cleared_count += 1;
                self.prune_clean_dirty_keys(
                    self.shard_index(&frame.key),
                    std::slice::from_ref(&frame.key),
                )?;
            }
        }
        if cleared_count > 0 {
            self.dirty_pages.fetch_sub(cleared_count, Ordering::Relaxed);
        }
        self.flushes
            .fetch_add(frames.len() as u64, Ordering::Relaxed);
        drop(flush_guards);
        self.notify_frame_waiters();
        Ok(Some(frames.len()))
    }

    fn flush_frame(&self, frame: &Arc<Frame>) -> Result<bool, QueryError> {
        self.flush_frame_with_wal(frame, 0)
    }

    fn flush_frame_with_wal(
        &self,
        frame: &Arc<Frame>,
        synced_lsn: u64,
    ) -> Result<bool, QueryError> {
        if !frame.dirty.load(Ordering::Acquire) {
            return Ok(false);
        }
        // Transaction-local WAL buffering: a frame with un-drained deferred
        // writers carries page changes whose WAL records are still in a
        // thread-local buffer (not yet appended). Flushing now would put a
        // change on disk with no durable WAL behind it. Skip — it becomes
        // flushable the moment its last writer's commit drains the buffer and
        // decrements the count. (Always 0 unless MYDB_WAL_TXN_BUFFER is on.)
        if frame.deferred_pins.load(Ordering::Acquire) != 0 {
            return Ok(false);
        }
        let _flush_guard = frame.flush_lock.lock().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "buffer frame flush lock poisoned",
            )
        })?;
        if !frame.dirty.load(Ordering::Acquire) || frame.deferred_pins.load(Ordering::Acquire) != 0
        {
            return Ok(false);
        }
        if frame.state.load(Ordering::Acquire) == FrameIoState::Discarded as u8 {
            return Ok(false);
        }
        let (epoch, dirty_lsn, generation, bytes) = {
            let mut bytes_guard = frame.bytes.write().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer frame write lock poisoned",
                )
            })?;
            // Pair the deferred-pin check with the image snapshot. A writer
            // may have published its pin and modified bytes while this flush
            // waited for the byte latch after its optimistic outer check.
            frame.materialize_packed_leaf(&mut bytes_guard)?;
            if frame.deferred_pins.load(Ordering::Acquire) != 0 {
                return Ok(false);
            }
            let dirty_lsn = frame.dirty_lsn.load(Ordering::Acquire);
            if frame.checksum_dirty.load(Ordering::Acquire) {
                set_page_lsn_if_formatted(&mut bytes_guard, dirty_lsn);
                frame.checksum_dirty.store(false, Ordering::Release);
            }
            (
                frame.dirty_epoch.load(Ordering::Acquire),
                dirty_lsn,
                frame.generation.load(Ordering::Acquire),
                bytes_guard.clone(),
            )
        };
        // Memory mode deliberately has no WAL/fsync durability promise, but
        // it still writes a dirty victim to its heap/index file before
        // eviction.  Otherwise enforcing a finite buffer target would
        // silently discard committed rows once the pool fills.
        if self.durability != DurabilityMode::Memory && dirty_lsn > synced_lsn {
            crate::wal::LogManager::sync_durable_to(&self.log, dirty_lsn)?;
        }
        frame.publish_io_state(FrameIoState::Flushing);
        let result = self.write_page_bytes_with_generation(&frame.key, generation, &bytes);
        frame.publish_io_state(
            if frame.pin_count.load(Ordering::Acquire) == Self::EVICTING_PIN {
                FrameIoState::Evicting
            } else {
                FrameIoState::Ready
            },
        );
        result?;
        // Clear the dirty bit under the bytes READ lock so the epoch
        // check and the swap are atomic with respect to writers — every
        // dirtying path swaps bytes / bumps `dirty_epoch` while holding
        // the bytes WRITE lock. Without the guard a writer could land
        // between the epoch check and the swap and have its dirty bit
        // stolen: the frame would sit clean with unflushed bytes,
        // recoverable via WAL redo only until the next checkpoint
        // truncates the log.
        let cleared = {
            let _bytes_guard = frame.bytes.read().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer frame read lock poisoned",
                )
            })?;
            if frame.dirty_epoch.load(Ordering::Acquire) == epoch
                && frame.generation.load(Ordering::Acquire) == generation
            {
                let was_dirty = frame.dirty.swap(false, Ordering::AcqRel);
                frame.dirty_lsn.store(0, Ordering::Release);
                frame.first_dirty_lsn.store(0, Ordering::Release);
                // P0-2 - stamp the completed write-back so the background
                // cleaners can defer this frame if it re-dirties within the
                // cooldown window (hot-write-set detection).
                frame.flushed_at_us.store(self.now_us(), Ordering::Release);
                was_dirty
            } else {
                false
            }
        };
        if cleared {
            self.dirty_pages.fetch_sub(1, Ordering::Relaxed);
            self.prune_clean_dirty_keys(
                self.shard_index(&frame.key),
                std::slice::from_ref(&frame.key),
            )?;
        }
        self.flushes.fetch_add(1, Ordering::Relaxed);
        self.notify_frame_waiters();
        Ok(true)
    }

    fn load_page_bytes_into(
        &self,
        key: &PageKey,
        intent: AccessIntent,
        buf: &mut Vec<u8>,
    ) -> Result<(), QueryError> {
        buf.resize(PAGE_SIZE, 0);
        let offset = key.page_id * PAGE_SIZE as u64;
        let file = self.open_cached_file(&key.file)?;
        let end = offset + PAGE_SIZE as u64;
        let len = if file.len() < end {
            file.refresh_len()?
        } else {
            file.len()
        };
        if len < end {
            buf.fill(0);
            return Ok(());
        }
        let syscalls = read_cached_page_at(&file, buf, offset).map_err(io_err)?;
        self.read_ios.fetch_add(1, Ordering::Relaxed);
        self.kind_read_ios[key.file.kind.slot()].fetch_add(1, Ordering::Relaxed);
        self.read_syscalls.fetch_add(syscalls, Ordering::Relaxed);
        // P0-1 - split read counts by intent so dashboards can spot
        // scan/prefetch IO drowning out a hot OLTP working set.
        match intent {
            AccessIntent::Normal | AccessIntent::Probe => {
                self.read_ios_normal.fetch_add(1, Ordering::Relaxed);
            }
            AccessIntent::Scan => {
                self.read_ios_scan.fetch_add(1, Ordering::Relaxed);
            }
            AccessIntent::SmallScan => {
                self.read_ios_scan.fetch_add(1, Ordering::Relaxed);
                self.small_scan_read_ios.fetch_add(1, Ordering::Relaxed);
            }
            AccessIntent::Prefetch => {
                self.read_ios_prefetch.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    fn write_page_bytes_with_generation(
        &self,
        key: &PageKey,
        generation: u64,
        bytes: &[u8],
    ) -> Result<(), QueryError> {
        if bytes.len() != PAGE_SIZE {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "buffer pool page image has invalid size",
            ));
        }
        // Keep invalidation excluded through the physical write. Checking
        // the generation and releasing the lock before pwrite lets a late
        // cleaner overwrite a replacement bulk-build/truncate image.
        let generations = self.file_generations.read().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "buffer pool generation lock poisoned",
            )
        })?;
        if generations
            .get(&*key.file)
            .map(|cell| cell.load(Ordering::Acquire))
            != Some(generation)
        {
            return Ok(());
        }
        // `open_cached_file` does the one-time mkdir on cache miss; once the
        // handle is cached the parent dir is guaranteed to exist for the
        // lifetime of the BufferPool, so we don't pay the stat+mkdir on the
        // hot write path.
        let file = self.open_cached_file(&key.file)?;
        let offset = key.page_id * PAGE_SIZE as u64;
        write_cached_page_at(&file, bytes, offset).map_err(io_err)?;
        file.update_len_at_least(offset + PAGE_SIZE as u64);
        // P1 - mark the file as needing fsync. The Release pwrite-then-store
        // ordering means any sync that observes the flag will see this data
        // in the kernel page cache.
        file.mark_needs_sync();
        if self.durability != DurabilityMode::Memory {
            file.note_buffered_write(bytes.len());
        }
        self.write_ios.fetch_add(1, Ordering::Relaxed);
        self.write_syscalls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn write_page_run_bytes_with_generation(
        &self,
        first_key: &PageKey,
        generation: u64,
        bytes: &[u8],
        page_count: usize,
    ) -> Result<(), QueryError> {
        if page_count == 0 || bytes.len() != page_count.saturating_mul(PAGE_SIZE) {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "buffer pool page run image has invalid size",
            ));
        }
        let generations = self.file_generations.read().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "buffer pool generation lock poisoned",
            )
        })?;
        if generations
            .get(&*first_key.file)
            .map(|cell| cell.load(Ordering::Acquire))
            != Some(generation)
        {
            return Ok(());
        }
        let file = self.open_cached_file(&first_key.file)?;
        let offset = first_key.page_id * PAGE_SIZE as u64;
        write_all_at(&file.file, bytes, offset).map_err(io_err)?;
        file.update_len_at_least(offset + bytes.len() as u64);
        file.mark_needs_sync();
        if self.durability != DurabilityMode::Memory {
            file.note_buffered_write(bytes.len());
        }
        self.write_ios
            .fetch_add(page_count as u64, Ordering::Relaxed);
        self.write_syscalls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub(crate) fn sync_cached_files(&self) -> Result<(), QueryError> {
        // WAL reclamation requires durable pages even in relaxed mode.
        if self.durability == DurabilityMode::Memory {
            return Ok(());
        }
        let files = self.cached_files()?;
        #[cfg(target_os = "macos")]
        return self.sync_cached_files_by_device(files);
        #[cfg(not(target_os = "macos"))]
        self.sync_cached_files_parallel(files)
    }

    #[cfg(not(target_os = "macos"))]
    fn sync_cached_files_parallel(&self, files: Vec<Arc<CachedFile>>) -> Result<(), QueryError> {
        if files.len() < 8 {
            for file in files {
                self.sync_cached_file_handle(file)?;
            }
            return Ok(());
        }
        let workers = thread::available_parallelism()
            .map(NonZeroUsize::get)
            .unwrap_or(2)
            .min(8)
            .min(files.len());
        let next = AtomicUsize::new(0);
        let first_error = Mutex::new(None);
        thread::scope(|scope| {
            for _ in 0..workers {
                scope.spawn(|| loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    if index >= files.len() {
                        break;
                    }
                    if let Err(err) = self.sync_cached_file_handle(Arc::clone(&files[index])) {
                        if let Ok(mut slot) = first_error.lock() {
                            if slot.is_none() {
                                *slot = Some(err);
                            }
                        }
                    }
                });
            }
        });
        first_error
            .into_inner()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "file sync error lock poisoned",
                )
            })?
            .map_or(Ok(()), Err)
    }

    #[cfg(target_os = "macos")]
    fn sync_cached_files_by_device(
        &self,
        mut files: Vec<Arc<CachedFile>>,
    ) -> Result<(), QueryError> {
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::MetadataExt;

        // Concurrent barriers must acquire overlapping locks in the same order.
        files.sort_unstable_by_key(|file| Arc::as_ptr(file) as usize);
        files.dedup_by(|left, right| Arc::ptr_eq(left, right));
        struct Pending<'a> {
            files: Vec<&'a CachedFile>,
            _locks: Vec<parking_lot::MutexGuard<'a, ()>>,
            complete: bool,
        }
        impl Drop for Pending<'_> {
            fn drop(&mut self) {
                if !self.complete {
                    for file in &self.files {
                        file.mark_needs_sync();
                    }
                }
            }
        }
        let mut pending = Pending {
            files: Vec::new(),
            _locks: Vec::new(),
            complete: false,
        };
        for file in &files {
            let lock = file.sync_lock.lock();
            if file.take_needs_sync() {
                pending.files.push(file);
                pending._locks.push(lock);
            } else {
                self.file_sync_skipped.fetch_add(1, Ordering::Relaxed);
            }
        }
        if pending.files.len() == 1 {
            sync_buffer_file_data(&pending.files[0].file).map_err(io_err)?;
        } else {
            let mut devices = BTreeMap::new();
            for file in &pending.files {
                let device = file.file.metadata().map_err(io_err)?.dev();
                devices.entry(device).or_insert(*file);
                loop {
                    // SAFETY: the cached file owns a live descriptor. fsync
                    // drains this file to its device; F_FULLFSYNC below drains
                    // each device cache after all its file writes are ordered.
                    if unsafe { libc::fsync(file.file.as_raw_fd()) } == 0 {
                        break;
                    }
                    let error = std::io::Error::last_os_error();
                    if error.kind() != std::io::ErrorKind::Interrupted {
                        return Err(io_err(error));
                    }
                }
            }
            // Keep every sync lock until these full barriers complete. A
            // competing single-file sync must not acknowledge our cleared
            // needs_sync bit before the device is durable.
            #[cfg(test)]
            let private_fixture = {
                let mut all_private = true;
                for file in &pending.files {
                    match crate::platform::fs::try_fixture_sync_file(&file.file) {
                        Some(result) => result.map_err(io_err)?,
                        None => all_private = false,
                    }
                }
                all_private
            };
            #[cfg(not(test))]
            let private_fixture = false;
            if !private_fixture {
                for file in devices.values() {
                    file.file.sync_data().map_err(io_err)?;
                }
            }
        }
        self.file_syncs
            .fetch_add(pending.files.len() as u64, Ordering::Relaxed);
        // Writers racing the barrier can set needs_sync again. Never clear
        // that newer publication when committing this batch.
        pending.complete = true;
        Ok(())
    }

    fn sync_cached_file(&self, file_key: &BufferPageFileKey) -> Result<(), QueryError> {
        if self.durability != DurabilityMode::Strict {
            return Ok(());
        }
        let file = {
            self.file_cache
                .read()
                .map_err(|_| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "buffer pool file cache lock poisoned",
                    )
                })?
                .get(file_key)
                .cloned()
        };
        if let Some(file) = file {
            self.sync_cached_file_handle(file)?;
        }
        Ok(())
    }

    fn cached_files(&self) -> Result<Vec<Arc<CachedFile>>, QueryError> {
        Ok(self
            .file_cache
            .read()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer pool file cache lock poisoned",
                )
            })?
            .values()
            .cloned()
            .collect())
    }

    /// P0 - cheap pre-check for `flush_and_evict_all`. Probes each cached
    /// file's `needs_sync` flag without taking any per-file lock; bails
    /// out as soon as one is found. Uses Acquire to pair with the Release
    /// in `mark_needs_sync` so we never miss a flag that a writer set
    /// before this call's logical start.
    fn any_file_needs_sync(&self) -> bool {
        let Ok(files) = self.file_cache.read() else {
            // If the lock is poisoned we can't tell; force the slow path
            // so callers don't silently skip a real sync.
            return true;
        };
        files.values().any(|f| f.needs_sync.load(Ordering::Acquire))
    }

    fn sync_cached_file_handle(&self, file: Arc<CachedFile>) -> Result<(), QueryError> {
        // Wait for an earlier barrier before interpreting its cleared dirty bit.
        let _sync = file.sync_lock.lock();
        // P1 - skip files that nothing has written to since the last sync.
        if !file.take_needs_sync() {
            self.file_sync_skipped.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        let result = sync_buffer_file_data(&file.file);
        match result {
            Ok(()) => {
                self.file_syncs.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(err) => {
                // Restore the flag so the next caller retries — we may have
                // raced with a concurrent writer whose data still needs to
                // hit disk.
                file.mark_needs_sync();
                Err(io_err(err))
            }
        }
    }

    fn open_cached_file(
        &self,
        file_key: &BufferPageFileKey,
    ) -> Result<Arc<CachedFile>, QueryError> {
        // M-2 - hot path: try the read lock first. The cache is
        // write-rarely (only on first touch of a given file_key);
        // every subsequent page IO would otherwise serialise on a
        // process-wide mutex.
        //
        // `file_handle_wait_ns` tracks the read-lock acquisition latency,
        // but issuing `Instant::now()` (a `mach_absolute_time` syscall
        // on macOS) on every fast-path hit is itself the most expensive
        // step in this function. Sample under TPC-C 4W/2T showed clock
        // syscalls as a dominant non-malloc frame. Skip the timing on
        // the fast path — the counter is still updated when we miss
        // and have to take the write lock.
        if let Some(file) = self
            .file_cache
            .read()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer pool file cache lock poisoned",
                )
            })?
            .get(file_key)
            .cloned()
        {
            // B3 - mark the handle as recently used for the second-chance
            // idle sweep. Relaxed + a pre-check to avoid a needless store on
            // the steady-state hot path where the bit is already set.
            if !file.recently_used.load(Ordering::Relaxed) {
                file.recently_used.store(true, Ordering::Relaxed);
            }
            return Ok(file);
        }
        let wait_started = Instant::now();
        // Miss: open the file unlocked so the (potentially blocking)
        // syscall doesn't sit inside the write lock.
        let file = crate::storage::image_io::open_data_file(&file_key.path).map_err(io_err)?;
        // Phase 0: decouple data-page writeback from WAL fsync on macOS by
        // bypassing the OS unified buffer cache for this handle (mydb caches
        // these pages itself). See `apply_data_file_nocache`.
        apply_data_file_nocache(&file);
        apply_buffer_file_read_advice(&file);
        let len = file.metadata().map_err(io_err)?.len();
        let opened = CachedFile::new(file, len);
        #[cfg(target_os = "linux")]
        let opened = {
            use std::os::fd::AsRawFd;
            use std::os::unix::fs::OpenOptionsExt;
            let mut opened = opened;
            // Reopen the same inode, even if a concurrent DDL has renamed
            // its pathname since the buffered handle was opened.
            if data_file_direct_io_enabled() {
                opened.direct_read = OpenOptions::new()
                    .read(true)
                    .write(!crate::storage::image_io::immutable_path(&file_key.path))
                    .custom_flags(libc::O_DIRECT)
                    .open(format!("/proc/self/fd/{}", opened.file.as_raw_fd()))
                    .ok();
            }
            opened
        };
        let opened = Arc::new(opened);
        // Take the write lock and double-check — another thread may
        // have raced us to the open call.
        let mut files = self.file_cache.write().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "buffer pool file cache lock poisoned",
            )
        })?;
        let inserted = files
            .entry(file_key.clone())
            .or_insert_with(|| opened.clone())
            .clone();
        // B3 - bound the handle cache. Only runs on the (rare) open-miss
        // path and only when a cap is configured, so the hot hit path pays
        // nothing. The just-inserted handle is protected by its clock bit.
        let cap = file_cache_max();
        if cap > 0 && files.len() > cap {
            Self::evict_idle_file_handles(
                &mut files,
                file_key,
                cap,
                self.durability != DurabilityMode::Memory,
            );
        }
        self.file_handle_wait_ns.fetch_add(
            wait_started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        Ok(inserted)
    }

    /// B3 - second-chance sweep that closes idle cached file handles so the
    /// pool honours `MYDB_BUFFER_POOL_FILE_CACHE_MAX`. A handle is closed
    /// only when its clock bit is clear (not touched since the last sweep)
    /// AND it has no pending fsync (`needs_sync == false`) — dropping a
    /// handle with unsynced writes would abandon a durability obligation the
    /// checkpoint's `sync_cached_files` sweep relies on. `keep` (the handle
    /// the caller just inserted) is never evicted. Closing a handle only
    /// drops our cached fd; a later access transparently re-opens the file,
    /// and the fresh handle starts with `needs_sync = true` so any lingering
    /// OS-cache writes are still fsync'd. Runs under the file-cache write
    /// lock already held by the caller.
    fn evict_idle_file_handles(
        files: &mut BufferFileCacheMap,
        keep: &BufferPageFileKey,
        cap: usize,
        preserve_unsynced: bool,
    ) {
        let mut over = files.len().saturating_sub(cap);
        if over == 0 {
            return;
        }
        // First pass: reclaim handles whose second chance is already spent.
        // Clear the clock bit on skipped-but-idle handles so a subsequent
        // sweep can reclaim them if they stay untouched.
        let mut victims: Vec<BufferPageFileKey> = Vec::new();
        for (key, file) in files.iter() {
            if over == 0 {
                break;
            }
            if key == keep
                || (preserve_unsynced
                    && (file.needs_sync.load(Ordering::Acquire)
                        || file.sync_lock.is_locked()
                        || Arc::strong_count(file) > 1))
            {
                continue;
            }
            if file.recently_used.swap(false, Ordering::Relaxed) {
                // Second chance granted this round.
                continue;
            }
            victims.push(key.clone());
            over -= 1;
        }
        for key in victims {
            files.remove(&key);
        }
        if over == 0 {
            return;
        }
        // Complete the second-chance cycle in the same open-miss operation
        // instead of waiting for another distinct file to be opened. Dirty
        // handles remain until a checkpoint syncs them; abandoning their
        // `needs_sync` obligation would weaken strict durability.
        let mut victims = Vec::new();
        for (key, file) in files.iter() {
            if over == 0 {
                break;
            }
            if key == keep
                || (preserve_unsynced
                    && (file.needs_sync.load(Ordering::Acquire)
                        || file.sync_lock.is_locked()
                        || Arc::strong_count(file) > 1))
            {
                continue;
            }
            victims.push(key.clone());
            over -= 1;
        }
        for key in victims {
            files.remove(&key);
        }
    }

    fn lookup_frame(&self, key: &PageKey) -> Result<Option<Arc<Frame>>, QueryError> {
        let shard = self.shards[self.shard_index(key)]
            .frames
            .read()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer pool shard lock poisoned",
                )
            })?;
        Ok(shard.get(key).cloned())
    }

    /// Metadata-only per-file residency, without cloning one Arc for every
    /// resident frame. Used by diagnostics and bounded summary costing.
    fn file_residency_stats(&self, file: &BufferPageFileKey) -> Result<(u64, u64), QueryError> {
        let mut resident = 0u64;
        let mut dirty = 0u64;
        for shard in &self.shards {
            let frames = shard.frames.read().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer pool shard lock poisoned",
                )
            })?;
            for (key, frame) in frames.iter() {
                if &*key.file == file {
                    resident += 1;
                    dirty += u64::from(frame.dirty.load(Ordering::Acquire));
                }
            }
        }
        Ok((resident, dirty))
    }

    fn frames_for_file(&self, file: &BufferPageFileKey) -> Result<Vec<Arc<Frame>>, QueryError> {
        let mut frames = Vec::new();
        for shard in &self.shards {
            frames.extend(
                shard
                    .frames
                    .read()
                    .map_err(|_| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "buffer pool shard lock poisoned",
                        )
                    })?
                    .iter()
                    .filter_map(|(key, frame)| (&*key.file == file).then(|| frame.clone())),
            );
        }
        Ok(frames)
    }

    fn try_pin_existing(&self, frame: &Arc<Frame>, _intent: AccessIntent) -> bool {
        // Bounded backoff: cheap CPU hints first, then yield. Without this,
        // many threads CASing the same frame's pin_count cause cache-line
        // bouncing under high contention.
        let mut spins: u32 = 0;
        loop {
            let current = frame.pin_count.load(Ordering::Acquire);
            if current == Self::EVICTING_PIN
                || frame.state.load(Ordering::Acquire) == FrameIoState::Discarded as u8
            {
                return false;
            }
            if frame
                .pin_count
                .compare_exchange(
                    current,
                    current.saturating_add(1),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return true;
            }
            spins = spins.saturating_add(1);
            if spins < 4 {
                std::hint::spin_loop();
            } else if spins < 32 {
                for _ in 0..(1u32 << (spins - 4).min(5)) {
                    std::hint::spin_loop();
                }
            } else {
                std::thread::yield_now();
            }
        }
    }

    fn wait_until_ready(&self, frame: &Arc<Frame>) -> Result<(), QueryError> {
        // Hot path: almost every call observes Ready/Flushing on the
        // first probe. Defer the `Instant::now()` (a `mach_absolute_time`
        // syscall on macOS) until we actually need a wait-timeout — for
        // OLTP workloads that converts a per-page-IO syscall into a
        // syscall paid only on the cold Loading branch.
        let mut started: Option<Instant> = None;
        loop {
            match FrameIoState::from_u8(frame.state.load(Ordering::Acquire)) {
                FrameIoState::Ready | FrameIoState::Flushing => return Ok(()),
                FrameIoState::Discarded | FrameIoState::Evicting => {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "buffer frame was evicted during load",
                    ));
                }
                FrameIoState::Loading => {
                    let deadline_started = *started.get_or_insert_with(Instant::now);
                    if deadline_started.elapsed() > self.lock_wait {
                        return Err(error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "buffer frame load timed out",
                        ));
                    }
                    let guard = frame.io_wait_lock.lock().map_err(|_| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "buffer frame I/O wait lock poisoned",
                        )
                    })?;
                    // The loader may have published between the outer state
                    // probe and acquiring the wait lock.
                    if FrameIoState::from_u8(frame.state.load(Ordering::Acquire))
                        != FrameIoState::Loading
                    {
                        continue;
                    }
                    frame.io_waiters.fetch_add(1, Ordering::Relaxed);
                    let remaining = self.lock_wait.saturating_sub(deadline_started.elapsed());
                    let _ = frame
                        .io_wait_cv
                        .wait_timeout(guard, remaining.min(BUFFER_POOL_WAIT_CEILING));
                    frame.io_waiters.fetch_sub(1, Ordering::Relaxed);
                }
            }
        }
    }

    fn push_clock_key(&self, shard_idx: usize, key: PageKey) -> Result<(), QueryError> {
        let mut clock = self.shards[shard_idx].clock.lock().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "buffer pool clock lock poisoned",
            )
        })?;
        clock.push_back(key);
        // Lazy compaction: `consume_victim` deliberately skips removing
        // its victim's key from the clock (O(n) per evict), so dead
        // keys would accumulate forever without an amortized retain.
        // Cap the slop at 2× the live-frame count: cheap O(n) once
        // per ~n evicts, vs O(n) on every evict. The threshold also
        // bounds `find_clock_victim`'s `max_scans = len*4`.
        let live = {
            let frames = self.shards[shard_idx].frames.read().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer pool shard lock poisoned",
                )
            })?;
            frames.len()
        };
        if live > 0 && clock.len() > live.saturating_mul(2) {
            self.retain_live_clock_keys(shard_idx, &mut clock);
        }
        Ok(())
    }

    /// Shared atomic generation cell for `file`. Fast path is a read
    /// lock + clone of the `Arc`; only the first touch of a file takes
    /// the write lock. Callers on the miss-load path hold the cell so
    /// the pre-load and post-load generation reads cost one map lookup
    /// total instead of two.
    fn file_generation_cell(&self, file: &BufferPageFileKey) -> Result<Arc<AtomicU64>, QueryError> {
        if let Some(cell) = self
            .file_generations
            .read()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer pool generation lock poisoned",
                )
            })?
            .get(file)
            .cloned()
        {
            return Ok(cell);
        }
        let mut generations = self.file_generations.write().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "buffer pool generation lock poisoned",
            )
        })?;
        Ok(generations
            .entry(file.clone())
            .or_insert_with(|| Arc::new(AtomicU64::new(self.allocate_file_generation())))
            .clone())
    }

    fn allocate_file_generation(&self) -> u64 {
        self.next_file_generation
            .fetch_add(1, Ordering::AcqRel)
            .max(1)
    }

    fn file_generation(&self, file: &BufferPageFileKey) -> Result<u64, QueryError> {
        Ok(self.file_generation_cell(file)?.load(Ordering::Acquire))
    }

    fn loaded_frame_is_current(
        &self,
        shard_idx: usize,
        key: &PageKey,
        frame: &Arc<Frame>,
        generation_cell: &AtomicU64,
        load_generation: u64,
    ) -> Result<bool, QueryError> {
        if generation_cell.load(Ordering::Acquire) != load_generation
            || frame.state.load(Ordering::Acquire) == FrameIoState::Discarded as u8
        {
            return Ok(false);
        }
        Ok(self.shards[shard_idx]
            .frames
            .read()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer pool shard lock poisoned",
                )
            })?
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(current, frame)))
    }

    fn buffer_stats(&self) -> BufferStats {
        BufferStats {
            hits: self.shards.iter().fold(0u64, |total, shard| {
                total.wrapping_add(shard.hits.load(Ordering::Relaxed))
            }),
            misses: self.misses.load(Ordering::Relaxed),
        }
    }

    fn prefetch_page(self: &Arc<Self>, key: PageKey) {
        self.prefetch_pages(std::iter::once(key));
    }

    /// Contiguous-range prefetch admission for sequential heap scans.
    ///
    /// The caller (`HeapFile::prefetch_after`) submits each page id
    /// exactly once — the leading edge of its sliding window — so this
    /// path deliberately has no per-candidate skip-window bookkeeping
    /// (see `prefetch_pages`): the steady-state fully-cached cost is a
    /// single `lookup_frame` probe per visited heap page, and a cold
    /// stretch re-engages prefetch on the very next submitted page
    /// instead of after a skip budget drains. Probing mutates one
    /// scratch `PageKey` in place, so a warm scan does one `Arc` clone
    /// per call rather than one per window page (the per-window clones
    /// of a shared `file_key` refcount line were a measurable
    /// cross-worker hot spot on parallel scans).
    fn prefetch_range(&self, file: &Arc<BufferPageFileKey>, start: u64, end: u64) {
        let window = self.effective_prefetch_pages.load(Ordering::Relaxed);
        if window == 0 || self.durability == DurabilityMode::Memory || start >= end {
            return;
        }
        // PERF-2 - admission is gated by `scan_origin_pressure` below, NOT
        // by `used >= capacity`. A full pool is the steady state of any
        // scan whose footprint exceeds the buffer pool — exactly the case
        // prefetch exists to help. Midpoint-LRU parks Scan/Prefetch fill
        // in the cold sublist, so the prefetch worker recycles those cold
        // pages among themselves (`ensure_capacity_for_insert` prefers
        // scan-origin victims) instead of evicting the working set. The
        // old hard `used >= capacity` early-return disabled read-ahead at
        // 100% occupancy and turned large scans into page-at-a-time
        // synchronous reads.
        let mut probe = PageKey {
            file: file.clone(),
            page_id: start,
        };
        let mut to_enqueue: Vec<PageKey> = Vec::new();
        // N-2 - scan/prefetch frames already occupy too much of
        // capacity; throttle further admissions so the OLTP working
        // set keeps its slots. Checked once per batch.
        let mut scan_pressure: Option<bool> = None;
        for page_id in start..end {
            probe.page_id = page_id;
            if self.lookup_frame(&probe).ok().flatten().is_some() {
                continue;
            }
            let pressure = *scan_pressure.get_or_insert_with(|| self.scan_origin_pressure());
            if pressure {
                self.prefetch_queue_drops.fetch_add(1, Ordering::Relaxed);
                self.prefetch_pressure_drops.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            to_enqueue.push(probe.clone());
        }
        self.enqueue_prefetch_batch(to_enqueue, window);
        self.maybe_adapt_prefetch();
    }

    /// Batch prefetch admission for arbitrary page-id lists (covering
    /// visibility walks, B-tree leaf chains, index multi-lookup). These
    /// callers re-submit ids they may have submitted before, so the
    /// per-key filters below run lock-free with a consecutive-hit skip
    /// window, then take the queue lock once for every key that
    /// survived.
    pub(crate) fn prefetch_pages(self: &Arc<Self>, keys: impl IntoIterator<Item = PageKey>) {
        let window = self.effective_prefetch_pages.load(Ordering::Relaxed);
        if window == 0 || self.durability == DurabilityMode::Memory {
            return;
        }
        // PERF-2 - see `prefetch_range`: a full pool no longer hard-blocks
        // prefetch. `scan_origin_pressure` (checked per key below) is the
        // throttle that keeps the working set resident; eviction recycles
        // cold scan-origin pages to make room for read-ahead.
        // C12 - thread-local prefetch skip window. On SF=1 / in-memory
        // analytical workloads, prefetch_hits/prefetches stays around 3%
        // (97% of calls find the page already resident — see
        // BUFFER_OPT_BENCHMARK.md). After observing 64 consecutive
        // lookup-frame hits this thread skips the next 128 prefetch
        // candidates entirely, avoiding the shard RwLock read and the
        // prefetch_queue Mutex enqueue altogether. The skip window
        // re-probes every K candidates so a change in working set still
        // re-engages prefetch promptly.
        const PREFETCH_HIT_WINDOW: u32 = 64;
        const PREFETCH_SKIP_WINDOW: u32 = 128;
        let pool_id = Arc::as_ptr(self) as usize;
        thread_local! {
            // (pool_id, consecutive_hits, remaining_skip_budget)
            static PREFETCH_SKIP_STATE: std::cell::Cell<(usize, u32, u32)> =
                const { std::cell::Cell::new((0, 0, 0)) };
        }
        let (state_pool, mut consec_hits, mut skip_budget) = PREFETCH_SKIP_STATE.with(|c| c.get());
        if state_pool != pool_id {
            consec_hits = 0;
            skip_budget = 0;
        }
        let mut to_enqueue: Vec<PageKey> = Vec::new();
        // N-2 - scan/prefetch frames already occupy too much of
        // capacity; throttle further admissions so the OLTP working
        // set keeps its slots. Checked once per batch.
        let mut scan_pressure: Option<bool> = None;
        for key in keys {
            if skip_budget > 0 {
                skip_budget -= 1;
                continue;
            }
            if self.lookup_frame(&key).ok().flatten().is_some() {
                consec_hits = consec_hits.saturating_add(1);
                if consec_hits >= PREFETCH_HIT_WINDOW {
                    skip_budget = PREFETCH_SKIP_WINDOW;
                    consec_hits = 0;
                }
                continue;
            }
            // Miss — reset hit streak so we don't accidentally skip
            // during a cold range.
            consec_hits = 0;
            let pressure = *scan_pressure.get_or_insert_with(|| self.scan_origin_pressure());
            if pressure {
                self.prefetch_queue_drops.fetch_add(1, Ordering::Relaxed);
                self.prefetch_pressure_drops.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            to_enqueue.push(key);
        }
        PREFETCH_SKIP_STATE.with(|c| c.set((pool_id, consec_hits, skip_budget)));
        self.enqueue_prefetch_batch(to_enqueue, window);
        self.maybe_adapt_prefetch();
    }

    /// Push surviving prefetch candidates onto the queue under a single
    /// lock and wake the IO workers.
    fn enqueue_prefetch_batch(&self, to_enqueue: Vec<PageKey>, window: usize) {
        if to_enqueue.is_empty() {
            return;
        }
        // Keep one-to-two windows per I/O worker, but never let speculative
        // work reserve the whole buffer pool. A queue as deep as the pool
        // lets requests wait until they are stale and then competes with the
        // demand cursor for every frame (at 100 MiB this used to mean 3,840
        // queued pages for a 3,840-page pool). The configured base remains a
        // stable floor, while the hard 1/4-pool budget keeps admission
        // independent from buffer residency and prevents that feedback loop.
        let base_floor = self
            .prefetch_pages
            .load(Ordering::Relaxed)
            .saturating_mul(self.io_threads)
            .saturating_mul(2);
        let queue_budget = (self.capacity_pages.load(Ordering::Relaxed) as usize / 4)
            .max(self.io_threads)
            .max(1);
        let cap = window
            .saturating_mul(self.io_threads)
            .saturating_mul(2)
            .max(base_floor)
            .min(queue_budget)
            .max(1);
        let now = self.now_us();
        let mut enqueued = 0_u64;
        if let Ok(mut queue) = self.prefetch_queue.lock() {
            for key in to_enqueue {
                if queue.len() >= cap {
                    self.prefetch_queue_drops.fetch_add(1, Ordering::Relaxed);
                    self.prefetch_queue_full_drops
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                let set_idx = self.shard_index(&key);
                let Ok(mut admitted) = self.prefetch_queued_or_inflight[set_idx].lock() else {
                    continue;
                };
                if !admitted.insert(key.clone()) {
                    self.prefetch_queue_drops.fetch_add(1, Ordering::Relaxed);
                    self.prefetch_duplicate_drops
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                queue.push_back((key, now));
                enqueued += 1;
            }
            self.prefetch_queue_depth_peak
                .fetch_max(queue.len() as u64, Ordering::Relaxed);
        }
        if enqueued > 0 {
            self.prefetches.fetch_add(enqueued, Ordering::Relaxed);
            self.wake_workers();
        }
    }

    fn complete_prefetch_admission(&self, key: &PageKey) {
        let set_idx = self.shard_index(key);
        if let Ok(mut admitted) = self.prefetch_queued_or_inflight[set_idx].lock() {
            admitted.remove(key);
        }
    }

    fn cancel_prefetch_matching(&self, mut matches: impl FnMut(&PageKey) -> bool) {
        let removed = {
            let Ok(mut queue) = self.prefetch_queue.lock() else {
                return;
            };
            let mut removed = Vec::new();
            queue.retain(|(key, _)| {
                if matches(key) {
                    removed.push(key.clone());
                    false
                } else {
                    true
                }
            });
            removed
        };
        // Release the queue lock before touching the sharded admission sets:
        // cancellation can span many shards and should not widen the queue's
        // critical section or create a future lock-order dependency.
        for key in removed {
            self.complete_prefetch_admission(&key);
        }
    }

    fn cancel_all_queued_prefetch(&self) {
        self.cancel_prefetch_matching(|_| true);
    }

    /// N-1 - public accessor used by hot-path scan callers to size
    /// their issue depth. Reflects the adaptive window, not the
    /// configured base.
    pub(crate) fn effective_prefetch_window(&self) -> usize {
        self.effective_prefetch_pages.load(Ordering::Relaxed)
    }

    /// N-2 - true when scan/prefetch frames already occupy 25% or
    /// more of the configured capacity. Callers can use this to
    /// throttle further scan-class admissions and keep the working
    /// set resident.
    ///
    /// Threshold: 75% of capacity. Earlier this was 25% — defensible
    /// for steady-state OLTP, but pathological for a single large
    /// analytical scan (TPC-H Q18 sf=5 over a 4.5GB lineitem heap with
    /// a 4GB BP showed 98% of prefetches dropped here, leaving the
    /// scan synchronously waiting on every leaf page). Midpoint LRU
    /// already keeps `Scan`-intent pages in the cold sublist so they
    /// don't actually crowd hot working-set pages out — they sit on
    /// top of the clock waiting for the next eviction. The 25% reserve
    /// for non-scan frames keeps the working set protected while
    /// letting prefetch run wide for the bulk scan.
    fn scan_origin_pressure(&self) -> bool {
        // The maintained cold queue makes scan admissions self-recycling:
        // newly prefetched pages replace older cold pages in O(1) without
        // displacing promoted working-set frames. The legacy 75% throttle
        // would otherwise drop nearly every remaining sequential page once
        // a scan fills the pool, defeating batched I/O exactly when memory is
        // tight. Keep it only for the CLOCK-only differential path.
        if cold_recycle_enabled() {
            const MIX_SAMPLE_PAGES: u64 = 1_024;
            let scan_reads = self
                .read_ios_scan
                .load(Ordering::Relaxed)
                .saturating_add(self.read_ios_prefetch.load(Ordering::Relaxed));
            let previous_scan = self.prefetch_gate_last_scan_reads.load(Ordering::Relaxed);
            if scan_reads.saturating_sub(previous_scan) >= MIX_SAMPLE_PAGES
                && self
                    .prefetch_gate_last_scan_reads
                    .compare_exchange(
                        previous_scan,
                        scan_reads,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
            {
                let normal_reads = self.read_ios_normal.load(Ordering::Relaxed);
                let previous_normal = self
                    .prefetch_gate_last_normal_reads
                    .swap(normal_reads, Ordering::AcqRel);
                let normal_delta = normal_reads.saturating_sub(previous_normal);
                let scan_delta = scan_reads.saturating_sub(previous_scan);
                // Random/point reads at 20% or more of recent scan reads mean
                // this is a mixed lookup workload, not a pure sequential
                // stream. Restore the residency throttle so speculative scan
                // I/O cannot starve foreground probes.
                self.prefetch_gate_mixed_reads.store(
                    normal_delta.saturating_mul(5) >= scan_delta,
                    Ordering::Release,
                );
            }
            if !self.prefetch_gate_mixed_reads.load(Ordering::Acquire) {
                return false;
            }
        }
        let cap = self.capacity_pages.load(Ordering::Relaxed).max(1);
        let limit = (cap.saturating_mul(3) / 4).max(1);
        self.scan_origin_count.load(Ordering::Relaxed) >= limit
    }

    /// N-2 - promote a frame out of the scan/prefetch sublist into the
    /// working set. Called when a Normal access lands on a frame that
    /// was previously flagged as scan-origin: clearing `scan_origin`
    /// stops `find_clock_victim` from treating the page as an immediate
    /// victim. The `usage_count` bump that actually keeps the page
    /// resident is done by the caller's preceding `touch(Normal)` /
    /// `note_normal_access` — this method only owns the `scan_origin`
    /// flag and its global count.
    fn promote_scan_origin(&self, frame: &Frame) -> bool {
        if frame.scan_origin.swap(false, Ordering::AcqRel) {
            self.scan_origin_count.fetch_sub(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Return a completed one-pass probe page to the scan-origin cold set.
    ///
    /// Ordered MRR temporarily promotes every concrete page in its active
    /// window so it cannot be recycled before decode. Once the window has
    /// been consumed, merely clearing CLOCK's usage/ref bits is insufficient:
    /// the maintained cold-candidate queue only accepts frames whose
    /// `scan_origin` bit is set. Re-registering the frame here lets sustained
    /// sparse probes recycle those pages in O(1), instead of falling back to
    /// a multi-pass CLOCK search for every admission.
    fn demote_to_scan_origin(&self, shard_idx: usize, frame: &Arc<Frame>) {
        if frame.scan_origin.swap(true, Ordering::AcqRel) {
            return;
        }
        self.scan_origin_count.fetch_add(1, Ordering::Relaxed);
        if cold_recycle_enabled() {
            if let Ok(mut candidates) = self.shards[shard_idx].cold_candidates.lock() {
                let cap = self.cold_candidate_capacity_per_shard();
                while candidates.len() >= cap {
                    candidates.pop_front();
                }
                candidates.push_back(Arc::downgrade(frame));
            }
        }
    }

    fn record_small_scan_touch(&self, frame: &Frame, prior: Option<u8>) {
        self.small_scan_accesses.fetch_add(1, Ordering::Relaxed);
        match prior {
            Some(0) => {
                self.small_scan_cold_accesses
                    .fetch_add(1, Ordering::Relaxed);
            }
            Some(_) => {
                if self.promote_scan_origin(frame) {
                    self.small_scan_promotions.fetch_add(1, Ordering::Relaxed);
                }
            }
            None => {}
        }
    }

    /// BUG-1 - shared Normal-access bookkeeping for the cheap
    /// "page already resident" fast paths (`with_cached_bytes`,
    /// `with_cached_heap_page`, `cached_index_page`). The full
    /// `pin_page_with_intent` hit path performs this inline; before this
    /// helper the cached paths only cleared the prefetch flag and
    /// `touch`ed, silently skipping two things the full path does:
    ///   - **scan-origin promotion**: a page first loaded by a scan
    ///     (`scan_origin = true`) and then made hot through one of these
    ///     paths stayed flagged, so `find_clock_victim` kept treating it
    ///     as an immediate victim (it short-circuits on `scan_origin`
    ///     before the ref_bit / usage_count grace) and evicted a page
    ///     that had become part of the working set. The inflated
    ///     `scan_origin_count` also over-triggered `scan_origin_pressure`
    ///     and dropped legitimate prefetches.
    ///   - **prefetch-hit accounting**: a prefetched page consumed here
    ///     never registered as a prefetch hit, biasing the adaptive
    ///     window controller toward shrinking.
    fn note_normal_access(&self, frame: &Frame) {
        // Resident demand pages normally have no prefetch credit to consume.
        // Avoid an exclusive cache-line transfer for every reader in that
        // case; the exchange still arbitrates the single credited hit.
        if frame.prefetch.load(Ordering::Relaxed) && frame.prefetch.swap(false, Ordering::AcqRel) {
            self.prefetch_hits.fetch_add(1, Ordering::Relaxed);
            self.record_prefetch_lead(frame);
        }
        frame.touch(AccessIntent::Normal);
        if frame.scan_origin.load(Ordering::Relaxed) {
            self.promote_scan_origin(frame);
        }
    }

    /// N-2 - bookkeeping for any path that removes a frame (evict,
    /// discard_file, flush_and_evict_all, remove_frame_if_same).
    /// Keeps `scan_origin_count` in sync with reality.
    fn release_scan_origin_flag(&self, frame: &Frame) -> bool {
        if frame.prefetch_loaded_at_us.load(Ordering::Acquire) != 0
            && frame.prefetch.swap(false, Ordering::AcqRel)
        {
            self.prefetch_wasted.fetch_add(1, Ordering::Relaxed);
        }
        if frame.scan_origin.swap(false, Ordering::AcqRel) {
            self.scan_origin_count.fetch_sub(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// N-2 - bumped at frame creation when intent is Scan/Prefetch.
    fn observe_new_scan_origin(&self, shard_idx: usize, frame: &Arc<Frame>) {
        self.scan_origin_count.fetch_add(1, Ordering::Relaxed);
        if cold_recycle_enabled() {
            if let Ok(mut candidates) = self.shards[shard_idx].cold_candidates.lock() {
                let cap = self.cold_candidate_capacity_per_shard();
                while candidates.len() >= cap {
                    candidates.pop_front();
                }
                candidates.push_back(Arc::downgrade(frame));
            }
        }
    }

    fn cold_candidate_capacity_per_shard(&self) -> usize {
        let capacity =
            usize::try_from(self.capacity_pages.load(Ordering::Relaxed)).unwrap_or(usize::MAX);
        // Divide the pool-wide allowance before applying the shard-local
        // floor; duplicating the entire allowance in every shard retains
        // up to shard_count times as many evicted Frame allocations.
        capacity
            .div_ceil(self.shards.len().max(1))
            .saturating_mul(2)
            .max(64)
    }

    fn trim_cold_candidate_metadata(&self) {
        let cap = self.cold_candidate_capacity_per_shard();
        for shard in &self.shards {
            // Reclamation must not wait behind foreground page admission.
            // A missed busy shard is bounded by its insertion-time cap.
            let Ok(mut candidates) = shard.cold_candidates.try_lock() else {
                continue;
            };
            candidates.retain(|candidate| candidate.strong_count() != 0);
            candidates.truncate(cap);
            candidates.shrink_to_fit();
        }
    }

    /// N-1 - cheap adaptive controller. Once `ADAPT_INTERVAL`
    /// admissions have accumulated since the last update, look at the
    /// hit ratio of the new prefetches and nudge the effective window
    /// up or down inside `[MIN, MAX]`. Skipped if the user pinned the
    /// base window to 0 (prefetch disabled).
    fn maybe_adapt_prefetch(&self) {
        if self.prefetch_pages.load(Ordering::Relaxed) == 0 {
            return;
        }
        // Use admissions, not completions: under queue pressure waiting for
        // 1,024 completed requests delayed the first feedback until after a
        // small pool was already full. 256 admitted pages reacts within a
        // few scan windows while the EMA still smooths transient bursts.
        const ADAPT_INTERVAL: u64 = 256;
        let total = self.prefetches.load(Ordering::Relaxed);
        let baseline = self.prefetch_adapt_last_prefetches.load(Ordering::Relaxed);
        if total.saturating_sub(baseline) < ADAPT_INTERVAL {
            return;
        }
        if self
            .prefetch_adapt_last_prefetches
            .compare_exchange(baseline, total, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            // Lost the race; another thread is updating.
            return;
        }
        let total_hits = self.prefetch_hits.load(Ordering::Relaxed);
        let prev_hits = self
            .prefetch_adapt_last_prefetch_hits
            .swap(total_hits, Ordering::AcqRel);
        let window_prefetches = total.saturating_sub(baseline);
        let window_hits = total_hits.saturating_sub(prev_hits);
        let total_wasted = self.prefetch_wasted.load(Ordering::Relaxed);
        let prev_wasted = self
            .prefetch_adapt_last_wasted
            .swap(total_wasted, Ordering::AcqRel);
        let window_wasted = total_wasted.saturating_sub(prev_wasted);
        let total_drops = self.prefetch_queue_drops.load(Ordering::Relaxed);
        let prev_drops = self
            .prefetch_adapt_last_queue_drops
            .swap(total_drops, Ordering::AcqRel);
        let window_drops = total_drops.saturating_sub(prev_drops);
        let total_demand_waits = self.prefetch_demand_waits.load(Ordering::Relaxed);
        let prev_demand_waits = self
            .prefetch_adapt_last_demand_waits
            .swap(total_demand_waits, Ordering::AcqRel);
        let window_demand_waits = total_demand_waits.saturating_sub(prev_demand_waits);
        if window_prefetches == 0 {
            return;
        }
        let bp = (window_hits.saturating_mul(10_000) / window_prefetches).min(10_000);
        // EMA with alpha = 1/4 — smooth across ~four 1k windows.
        let prev_ema = self.prefetch_hit_ema_bp.load(Ordering::Relaxed);
        let new_ema = ((prev_ema * 3) + bp) / 4;
        self.prefetch_hit_ema_bp.store(new_ema, Ordering::Relaxed);
        let current = self.effective_prefetch_pages.load(Ordering::Relaxed);
        let capacity_cap = (self.capacity_pages.load(Ordering::Relaxed) as usize)
            .checked_div(8usize.saturating_mul(self.io_threads.max(1)))
            .unwrap_or(0)
            .clamp(1, MAX_BUFFER_POOL_PREFETCH_PAGES);
        let clamped = adapted_prefetch_window(
            current,
            window_prefetches,
            window_hits,
            window_wasted,
            window_drops,
            window_demand_waits,
            capacity_cap,
        );
        if clamped != current {
            self.effective_prefetch_pages
                .store(clamped, Ordering::Relaxed);
        }
    }

    fn drain_prefetch_queue(self: &Arc<Self>) -> Result<(), QueryError> {
        let window = self.effective_prefetch_pages.load(Ordering::Relaxed);
        // One queue lock for the whole window instead of one per page.
        let batch: Vec<(PageKey, u64)> = {
            let Ok(mut queue) = self.prefetch_queue.lock() else {
                return Ok(());
            };
            let take = window.max(1).min(queue.len());
            queue.drain(..take).collect()
        };
        if batch.is_empty() {
            return Ok(());
        }
        let now = self.now_us();
        let mut pending = Vec::with_capacity(batch.len());
        for (key, enqueued_us) in batch {
            let wait_us = now.saturating_sub(enqueued_us);
            self.prefetch_queue_wait_us_total
                .fetch_add(wait_us, Ordering::Relaxed);
            self.prefetch_queue_drains.fetch_add(1, Ordering::Relaxed);
            if wait_us >= 500_000 {
                self.prefetch_queue_drops.fetch_add(1, Ordering::Relaxed);
                self.prefetch_stale_drops.fetch_add(1, Ordering::Relaxed);
                self.complete_prefetch_admission(&key);
                continue;
            }
            if self.lookup_frame(&key)?.is_some() {
                self.complete_prefetch_admission(&key);
                continue;
            }
            pending.push(key);
        }
        if !batch_read_enabled() {
            for key in pending {
                if let Ok(pinned) = self.pin_page_with_intent(key.clone(), AccessIntent::Prefetch) {
                    drop(pinned);
                }
                self.complete_prefetch_admission(&key);
            }
            return Ok(());
        }
        pending.sort_unstable_by(|left, right| {
            left.file
                .hash
                .cmp(&right.file.hash)
                .then_with(|| left.file.path.cmp(&right.file.path))
                .then_with(|| left.page_id.cmp(&right.page_id))
        });
        let result = (|| -> Result<(), QueryError> {
            let mut start = 0usize;
            while start < pending.len() {
                let mut end = start + 1;
                while end < pending.len()
                    && *pending[end].file == *pending[start].file
                    && pending[end].page_id == pending[end - 1].page_id.saturating_add(1)
                {
                    end += 1;
                }
                self.load_prefetch_run(&pending[start..end])?;
                start = end;
            }
            Ok(())
        })();
        for key in &pending {
            self.complete_prefetch_admission(key);
        }
        result
    }

    /// Reserve all frames in a contiguous run before issuing one positioned
    /// read. Foreground readers that arrive meanwhile find `Loading` and wait
    /// for this operation instead of issuing a duplicate read.
    fn load_prefetch_run(self: &Arc<Self>, keys: &[PageKey]) -> Result<(), QueryError> {
        self.load_page_run(keys, AccessIntent::Prefetch)
    }

    fn load_page_run(
        self: &Arc<Self>,
        keys: &[PageKey],
        intent: AccessIntent,
    ) -> Result<(), QueryError> {
        if keys.is_empty() {
            return Ok(());
        }
        let file_key = keys[0].file.clone();
        let generation_cell = self.file_generation_cell(&file_key)?;
        let initial_generation = generation_cell.load(Ordering::Acquire);
        let mut reserved = Vec::with_capacity(keys.len());
        for key in keys {
            self.ensure_capacity_for_insert(self.lock_wait)?;
            let shard_idx = self.shard_index(key);
            let raw_page = self.claim_raw_page_lease(shard_idx)?;
            let frame = Arc::new(Frame::new_loading(
                key.clone(),
                initial_generation,
                intent,
                raw_page,
            ));
            let mut frames = self.shards[shard_idx].frames.write().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer pool shard lock poisoned",
                )
            })?;
            if frames.contains_key(key) {
                drop(frames);
                self.recycle_frame_raw_page(shard_idx, &frame);
                continue;
            }
            frames.insert(key.clone(), frame.clone());
            self.used_pages.fetch_add(1, Ordering::Relaxed);
            self.note_kind_added(key);
            self.observe_new_scan_origin(shard_idx, &frame);
            drop(frames);
            reserved.push(PendingFrameLoad {
                pool: self,
                frame,
                owns_pin: true,
            });
            self.push_clock_key(shard_idx, key.clone())?;
        }
        if reserved.is_empty() {
            return Ok(());
        }

        // Races can leave holes in `reserved`. Split winners into contiguous
        // runs and preadv directly into each frame's leased page buffer. This
        // preserves coalesced I/O without allocating a temporary Vec<Vec<u8>>
        // for every prefetch run.
        let io_result = (|| -> Result<(usize, u64, u64), QueryError> {
            let file = self.open_cached_file(&file_key)?;
            let last_page = reserved.last().expect("non-empty").key.page_id;
            let requested_end = last_page.saturating_add(1).saturating_mul(PAGE_SIZE as u64);
            let len = if file.len() < requested_end {
                file.refresh_len()?
            } else {
                file.len()
            };
            let mut readable_total = 0usize;
            let mut syscall_total = 0u64;
            let mut batch_total = 0u64;
            let mut start = 0usize;
            while start < reserved.len() {
                let mut end = start + 1;
                while end < reserved.len()
                    && reserved[end].key.page_id == reserved[end - 1].key.page_id.saturating_add(1)
                {
                    end += 1;
                }
                let first_page = reserved[start].key.page_id;
                let offset = first_page.saturating_mul(PAGE_SIZE as u64);
                let run_len = end - start;
                let readable = if len <= offset {
                    0
                } else {
                    ((len - offset) / PAGE_SIZE as u64).min(run_len as u64) as usize
                };
                let mut guards = Vec::with_capacity(run_len);
                for frame in &reserved[start..end] {
                    let mut guard = frame.bytes.write().map_err(|_| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "buffer frame write lock poisoned",
                        )
                    })?;
                    guard.resize(PAGE_SIZE, 0);
                    guard.fill(0);
                    guards.push(guard);
                }
                if readable != 0 {
                    let mut pages: Vec<&mut [u8]> = guards[..readable]
                        .iter_mut()
                        .map(|guard| guard.as_mut_slice())
                        .collect();
                    syscall_total = syscall_total.saturating_add(
                        read_cached_pages_at(&file, &mut pages, offset).map_err(io_err)?,
                    );
                    readable_total = readable_total.saturating_add(readable);
                    batch_total = batch_total.saturating_add(1);
                }
                start = end;
            }
            Ok((readable_total, syscall_total, batch_total))
        })();
        let (readable_pages, read_syscalls, batch_ops, io_error) = match io_result {
            Ok((readable_pages, syscalls, batches)) => (readable_pages, syscalls, batches, None),
            Err(err) => (0, 0, 0, Some(err)),
        };
        if readable_pages != 0 {
            self.read_ios
                .fetch_add(readable_pages as u64, Ordering::Relaxed);
            self.kind_read_ios[file_key.kind.slot()]
                .fetch_add(readable_pages as u64, Ordering::Relaxed);
            match intent {
                AccessIntent::Prefetch => {
                    self.read_ios_prefetch
                        .fetch_add(readable_pages as u64, Ordering::Relaxed);
                }
                AccessIntent::Normal | AccessIntent::Probe => {
                    self.read_ios_normal
                        .fetch_add(readable_pages as u64, Ordering::Relaxed);
                }
                AccessIntent::Scan => {
                    self.read_ios_scan
                        .fetch_add(readable_pages as u64, Ordering::Relaxed);
                }
                AccessIntent::SmallScan => {
                    self.read_ios_scan
                        .fetch_add(readable_pages as u64, Ordering::Relaxed);
                    self.small_scan_read_ios
                        .fetch_add(readable_pages as u64, Ordering::Relaxed);
                }
            }
            self.read_syscalls
                .fetch_add(read_syscalls, Ordering::Relaxed);
            self.batch_read_ops.fetch_add(batch_ops, Ordering::Relaxed);
            self.batch_read_pages
                .fetch_add(readable_pages as u64, Ordering::Relaxed);
        }
        let generation_ok =
            io_error.is_none() && self.file_generation(&file_key)? == initial_generation;
        for mut pending in reserved {
            let frame = &pending.frame;
            let shard_idx = self.shard_index(&frame.key);
            if !generation_ok
                || !self.loaded_frame_is_current(
                    shard_idx,
                    &frame.key,
                    &frame,
                    &generation_cell,
                    initial_generation,
                )?
            {
                frame.publish_io_state(FrameIoState::Discarded);
                frame.unpin();
                pending.owns_pin = false;
                self.remove_frame_if_same(frame)?;
                continue;
            }
            frame
                .generation
                .store(initial_generation, Ordering::Release);
            if intent == AccessIntent::Prefetch {
                frame
                    .prefetch_loaded_at_us
                    .store(self.now_us(), Ordering::Release);
            }
            frame.publish_io_state(FrameIoState::Ready);
            frame.touch(intent);
            if intent == AccessIntent::Probe {
                // These are confirmed members of the current MRR window, not
                // speculative scan-origin pages. Publish them directly into
                // the working set; otherwise the cold-candidate queue can
                // recycle the beginning of a full-pool batch before decode.
                self.promote_scan_origin(&frame);
            }
            if intent == AccessIntent::Prefetch {
                self.prefetch_completed.fetch_add(1, Ordering::Relaxed);
            }
            frame.unpin();
            pending.owns_pin = false;
        }
        self.notify_frame_waiters();
        match io_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// Synchronously load an ordered set of heap pages with bounded parallel
    /// `preadv` runs. MRR uses this after sorting and deduplicating one page
    /// window: unlike speculative queue admission, every requested missing
    /// page is completed before decode, so queue saturation cannot collapse
    /// the foreground path back to one `pread` per RID/page.
    fn load_ordered_probe_pages(
        self: &Arc<Self>,
        file: &Arc<BufferPageFileKey>,
        page_ids: &[u64],
    ) -> Result<(), QueryError> {
        if page_ids.is_empty() || self.durability == DurabilityMode::Memory {
            return Ok(());
        }
        // Emergency reservations belong to this thread; rollback reads must stay here.
        if crate::engine::executor::mem_budget::critical_memory_reservations_active() {
            for &page_id in page_ids {
                self.load_page_run(
                    &[PageKey {
                        file: Arc::clone(file),
                        page_id,
                    }],
                    AccessIntent::Probe,
                )?;
            }
            return Ok(());
        }
        let mut keys = Vec::with_capacity(page_ids.len());
        for &page_id in page_ids {
            let key = PageKey {
                file: file.clone(),
                page_id,
            };
            if self.lookup_frame(&key)?.is_none() {
                keys.push(key);
            }
        }
        if keys.is_empty() {
            return Ok(());
        }
        keys.sort_unstable_by_key(|key| key.page_id);
        keys.dedup_by_key(|key| key.page_id);

        // Keep every preadv comfortably below platform IOV_MAX (macOS
        // rejects oversized iovec arrays with EINVAL). 64 pages = 1 MiB.
        const MAX_ORDERED_READ_PAGES: usize = 64;
        let mut runs = Vec::new();
        let mut start = 0usize;
        while start < keys.len() {
            let mut end = start + 1;
            while end < keys.len()
                && end - start < MAX_ORDERED_READ_PAGES
                && keys[end].page_id == keys[end - 1].page_id.saturating_add(1)
            {
                end += 1;
            }
            runs.push((start, end));
            start = end;
        }

        let workers = self.io_threads.max(1).min(runs.len());
        // The overwhelmingly common OLTP MRR miss is one sparse page (or one
        // contiguous run). Spawning a scoped OS thread for that single run
        // adds thread creation, stack mapping and join work without exposing
        // any I/O parallelism. Keep the exact same ordered loader on the
        // caller in that case; only a genuinely multi-run window enters the
        // bounded parallel path below.
        if workers == 1 {
            for (from, to) in runs {
                self.load_page_run(&keys[from..to], AccessIntent::Probe)?;
            }
            return Ok(());
        }
        // Reuse the existing blocking workers instead of creating OS threads
        // for each sparse window. Bound queued helpers as well as running ones;
        // a busy pool leaves the read on its caller instead of growing a queue.
        let helper_limit = self.io_threads;
        let mut active = self.ordered_read_helpers.load(Ordering::Acquire);
        let helpers = loop {
            let count = (workers - 1).min(helper_limit.saturating_sub(active));
            if count == 0 {
                for (from, to) in runs {
                    self.load_page_run(&keys[from..to], AccessIntent::Probe)?;
                }
                return Ok(());
            }
            match self.ordered_read_helpers.compare_exchange_weak(
                active,
                active + count,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break count,
                Err(current) => active = current,
            }
        };
        let scratch_bytes = keys
            .capacity()
            .saturating_mul(std::mem::size_of::<PageKey>())
            .saturating_add(
                runs.capacity()
                    .saturating_mul(std::mem::size_of::<(usize, usize)>()),
            )
            .saturating_add(std::mem::size_of::<OrderedReadWork>());
        let Some(charge) =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                scratch_bytes,
            )
        else {
            self.ordered_read_helpers
                .fetch_sub(helpers, Ordering::Release);
            for (from, to) in runs {
                self.load_page_run(&keys[from..to], AccessIntent::Probe)?;
            }
            return Ok(());
        };
        let work = Arc::new(OrderedReadWork {
            state: parking_lot::Mutex::new((runs.len(), None)),
            done: parking_lot::Condvar::new(),
            next: AtomicUsize::new(0),
            keys,
            runs,
            _charge: charge,
        });
        for _ in 0..helpers {
            let pool = Arc::clone(self);
            let work = Arc::clone(&work);
            // Dropping the result handle does not cancel a blocking task.
            // Completion is per claimed I/O run, not per queued helper.
            drop(crate::util::blocking_pool::spawn(move || {
                struct HelperSlot<'a>(&'a AtomicUsize);
                impl Drop for HelperSlot<'_> {
                    fn drop(&mut self) {
                        self.0.fetch_sub(1, Ordering::Release);
                    }
                }
                let _slot = HelperSlot(&pool.ordered_read_helpers);
                pool.run_ordered_read_work(&work);
            }));
        }
        // Caller participation is required for progress: if every blocking
        // worker is itself executing an MRR request, no request waits for a
        // queued helper to start. It finishes all unclaimed runs itself and
        // waits only for helpers that have already claimed real I/O.
        self.run_ordered_read_work(&work);
        let mut state = work.state.lock();
        while state.0 != 0 {
            work.done.wait(&mut state);
        }
        match state.1.take() {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    fn run_ordered_read_work(self: &Arc<Self>, work: &OrderedReadWork) {
        loop {
            let next = work.next.fetch_add(1, Ordering::Relaxed);
            let Some(&(from, to)) = work.runs.get(next) else {
                return;
            };
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.load_page_run(&work.keys[from..to], AccessIntent::Probe)
            }))
            .unwrap_or_else(|_| {
                Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "ordered buffer read worker panicked",
                ))
            });
            let mut state = work.state.lock();
            if state.1.is_none() {
                state.1 = result.err();
            }
            state.0 -= 1;
            if state.0 == 0 {
                work.done.notify_all();
            }
        }
    }

    /// Return completed one-pass MRR pages to the cold end of the ordinary
    /// working set. The loader temporarily promotes the active window so it
    /// cannot recycle pages before decode. Once consumed, giving every page
    /// another CLOCK chance makes a broad sparse probe scan the victim ring
    /// repeatedly and evict reusable index pages before its own one-pass
    /// pages. A genuine later probe hit will promote the page normally.
    fn demote_ordered_probe_pages(
        &self,
        file: &Arc<BufferPageFileKey>,
        page_ids: &[u64],
    ) -> Result<(), QueryError> {
        // With ample free frames there is no competing working set to
        // protect. Avoid a second hash lookup for every page and leave the
        // successfully consumed window resident. This is an execution-only
        // cache policy decision; it cannot affect the chosen access path.
        let capacity = self.capacity_pages.load(Ordering::Relaxed).max(1);
        let used = self.used_pages.load(Ordering::Relaxed);
        if used.saturating_mul(10) <= capacity.saturating_mul(9) {
            return Ok(());
        }
        for &page_id in page_ids {
            let key = PageKey {
                file: file.clone(),
                page_id,
            };
            let Some(frame) = self.lookup_frame(&key)? else {
                continue;
            };
            // A range probe does not own pre-existing hot pages or pages
            // concurrently used by normal DML. Preserve their proven reuse.
            if frame.usage_count.load(Ordering::Acquire) > 2 || frame.dirty.load(Ordering::Acquire)
            {
                continue;
            }
            frame.usage_count.store(0, Ordering::Release);
            frame.ref_bit.store(false, Ordering::Release);
            self.demote_to_scan_origin(self.shard_index(&key), &frame);
        }
        Ok(())
    }

    /// Microseconds since pool start (cheap, monotonic). Used for
    /// prefetch enqueue / load-time bookkeeping where we don't need
    /// the granularity of nanoseconds.
    fn now_us(&self) -> u64 {
        self.pool_started
            .elapsed()
            .as_micros()
            .min(u128::from(u64::MAX)) as u64
    }

    /// Record how long this prefetched frame sat in cache before its
    /// first non-prefetch hit. A large lead means we issued the
    /// prefetch too early (wasted IO bandwidth); a tiny lead means
    /// the prefetch didn't actually hide latency.
    fn record_prefetch_lead(&self, frame: &Frame) {
        let loaded = frame.prefetch_loaded_at_us.load(Ordering::Acquire);
        if loaded == 0 {
            return;
        }
        let lead = self.now_us().saturating_sub(loaded);
        self.prefetch_lead_us_total
            .fetch_add(lead, Ordering::Relaxed);
        self.prefetch_lead_samples.fetch_add(1, Ordering::Relaxed);
    }

    fn wait_for_frame_release(&self, timeout: Duration) {
        if timeout.is_zero() {
            thread::yield_now();
            return;
        }
        if let Ok(guard) = self.wait_lock.lock() {
            self.frame_waiters.fetch_add(1, Ordering::Relaxed);
            let _ = self.wait_cv.wait_timeout(guard, timeout);
            self.frame_waiters.fetch_sub(1, Ordering::Relaxed);
        }
    }

    fn notify_frame_waiters(&self) {
        if self.frame_waiters.load(Ordering::Acquire) == 0 {
            return;
        }
        // Briefly synchronize with `wait_for_frame_release`: a waiter that
        // has incremented `frame_waiters` but has not yet entered
        // `wait_cv.wait` still holds `wait_lock`. Acquiring the lock here
        // blocks until they either complete entry into wait (releasing the
        // lock as part of `wait_timeout`'s atomic park) or back out — closing
        // the lost-wakeup window. Without this, capacity waiters could spin
        // their full 2ms slice before noticing a freed frame.
        if let Ok(_g) = self.wait_lock.lock() {}
        self.wait_cv.notify_all();
    }

    fn wake_workers(&self) {
        // D-4 - workers park on their own condvar now; closing the same
        // race window protects their 25ms timeout from lost wakeups.
        if let Ok(_g) = self.worker_lock.lock() {}
        self.worker_cv.notify_all();
    }

    fn shard_index(&self, key: &PageKey) -> usize {
        let h = key.file.hash ^ key.page_id.wrapping_mul(0x9e3779b97f4a7c15);
        (h as usize) & (self.shards.len() - 1)
    }
}

/// Single-page demand I/O bypasses speculative filesystem readahead. The
/// buffer pool still owns the authoritative dirty image; this function is
/// called only for a loading frame. Buffered writes retain their existing WAL
/// ordering, and Linux synchronizes overlapping cached writes before a direct
/// read. The scratch allocation remains governed and has ordinary Vec
/// ownership: only a suitably aligned subslice is passed to the kernel.
fn read_cached_page_at(file: &CachedFile, buf: &mut [u8], offset: u64) -> std::io::Result<u64> {
    #[cfg(target_os = "linux")]
    if let Some(direct) = file
        .direct_read
        .as_ref()
        .filter(|_| !file.direct_read_disabled.load(Ordering::Relaxed))
    {
        const ALIGNMENT: usize = 4096;
        if buf.len() == PAGE_SIZE && offset.is_multiple_of(PAGE_SIZE as u64) {
            // Recycled fixed-size pages are commonly already sector aligned.
            // Preserve their governed allocation and read into it directly.
            if buf.as_ptr().align_offset(ALIGNMENT) == 0 {
                match read_exact_at_counted(direct, buf, offset) {
                    Ok(calls) => return Ok(calls),
                    Err(err)
                        if matches!(err.raw_os_error(), Some(libc::EINVAL | libc::EOPNOTSUPP)) =>
                    {
                        file.direct_read_disabled.store(true, Ordering::Relaxed);
                        return read_exact_at_counted(&file.file, buf, offset);
                    }
                    Err(err) => return Err(err),
                }
            }
            let scratch_bytes = PAGE_SIZE + ALIGNMENT - 1;
            if let Some(_charge) =
                crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                    scratch_bytes,
                )
            {
                let mut scratch = vec![0u8; scratch_bytes];
                let start = scratch.as_ptr().align_offset(ALIGNMENT);
                let aligned = &mut scratch[start..start + PAGE_SIZE];
                match read_exact_at_counted(direct, aligned, offset) {
                    Ok(calls) => {
                        buf.copy_from_slice(aligned);
                        return Ok(calls);
                    }
                    Err(err)
                        if matches!(err.raw_os_error(), Some(libc::EINVAL | libc::EOPNOTSUPP)) =>
                    {
                        // Some filesystems accept O_DIRECT at open but reject
                        // it at I/O. Fall back once, retaining canonical errors
                        // for real media failures and truncated page images.
                        file.direct_read_disabled.store(true, Ordering::Relaxed);
                    }
                    Err(err) => return Err(err),
                }
            }
        }
    }
    read_exact_at_counted(&file.file, buf, offset)
}

/// Demand batches use the same inode and direct-I/O contract as scalar loads.
/// Keep the optional staging buffer bounded; oversized prefetch runs retain
/// their existing vectored path rather than requesting unbounded scratch.
fn read_cached_pages_at(
    file: &CachedFile,
    pages: &mut [&mut [u8]],
    offset: u64,
) -> std::io::Result<u64> {
    if pages.len() == 1 {
        return read_cached_page_at(file, pages[0], offset);
    }
    #[cfg(target_os = "linux")]
    if let Some(direct) = file
        .direct_read
        .as_ref()
        .filter(|_| !file.direct_read_disabled.load(Ordering::Relaxed))
    {
        const ALIGNMENT: usize = 4096;
        if (2..=64).contains(&pages.len())
            && pages.iter().all(|page| page.len() == PAGE_SIZE)
            && offset.is_multiple_of(PAGE_SIZE as u64)
        {
            if pages
                .iter()
                .all(|page| page.as_ptr().align_offset(ALIGNMENT) == 0)
            {
                match read_pages_batch_at(direct, pages, offset) {
                    Ok(calls) => return Ok(calls),
                    Err(err)
                        if matches!(err.raw_os_error(), Some(libc::EINVAL | libc::EOPNOTSUPP)) =>
                    {
                        file.direct_read_disabled.store(true, Ordering::Relaxed);
                        return read_pages_batch_at(&file.file, pages, offset);
                    }
                    Err(err) => return Err(err),
                }
            }
            let bytes = pages.len() * PAGE_SIZE;
            let scratch_bytes = bytes + ALIGNMENT - 1;
            if let Some(_charge) =
                crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                    scratch_bytes,
                )
            {
                let mut scratch = vec![0u8; scratch_bytes];
                let start = scratch.as_ptr().align_offset(ALIGNMENT);
                let aligned = &mut scratch[start..start + bytes];
                match read_exact_at_counted(direct, aligned, offset) {
                    Ok(calls) => {
                        for (page, image) in pages.iter_mut().zip(aligned.chunks_exact(PAGE_SIZE)) {
                            page.copy_from_slice(image);
                        }
                        return Ok(calls);
                    }
                    Err(err)
                        if matches!(err.raw_os_error(), Some(libc::EINVAL | libc::EOPNOTSUPP)) =>
                    {
                        file.direct_read_disabled.store(true, Ordering::Relaxed);
                    }
                    Err(err) => return Err(err),
                }
            }
        }
    }
    read_pages_batch_at(&file.file, pages, offset)
}

fn write_cached_page_at(file: &CachedFile, bytes: &[u8], offset: u64) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    if let Some(direct) = file
        .direct_read
        .as_ref()
        .filter(|_| !file.direct_read_disabled.load(Ordering::Relaxed))
    {
        const ALIGNMENT: usize = 4096;
        if bytes.len() == PAGE_SIZE && offset.is_multiple_of(PAGE_SIZE as u64) {
            if bytes.as_ptr().align_offset(ALIGNMENT) == 0 {
                match write_all_at(direct, bytes, offset) {
                    Ok(()) => return Ok(()),
                    Err(err)
                        if matches!(err.raw_os_error(), Some(libc::EINVAL | libc::EOPNOTSUPP)) =>
                    {
                        file.direct_read_disabled.store(true, Ordering::Relaxed);
                        return write_all_at(&file.file, bytes, offset);
                    }
                    Err(err) => return Err(err),
                }
            }
            let scratch_bytes = PAGE_SIZE + ALIGNMENT - 1;
            if let Some(_charge) =
                crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                    scratch_bytes,
                )
            {
                let mut scratch = vec![0u8; scratch_bytes];
                let start = scratch.as_ptr().align_offset(ALIGNMENT);
                let aligned = &mut scratch[start..start + PAGE_SIZE];
                aligned.copy_from_slice(bytes);
                match write_all_at(direct, aligned, offset) {
                    Ok(()) => return Ok(()),
                    Err(err)
                        if matches!(err.raw_os_error(), Some(libc::EINVAL | libc::EOPNOTSUPP)) =>
                    {
                        file.direct_read_disabled.store(true, Ordering::Relaxed);
                    }
                    Err(err) => return Err(err),
                }
            }
        }
    }
    write_all_at(&file.file, bytes, offset)
}

#[cfg(unix)]
fn read_exact_at_counted(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<u64> {
    crate::util::file::read_exact_at_counted(file, buf, offset, true, None)
}

/// Fill scratch storage without first writing zeros that the read replaces.
/// Success guarantees every byte is initialized; errors expose no byte slice.
#[cfg(unix)]
fn read_exact_at_uninit(
    file: &File,
    mut buf: &mut [std::mem::MaybeUninit<u8>],
    mut offset: u64,
) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    while !buf.is_empty() {
        let file_offset = libc::off_t::try_from(offset)
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
        // SAFETY: pread only writes to this live, uniquely borrowed allocation.
        // No reference to initialized u8 data is constructed before success.
        let read = unsafe {
            libc::pread(
                file.as_raw_fd(),
                buf.as_mut_ptr().cast(),
                buf.len().min(isize::MAX as usize),
                file_offset,
            )
        };
        if read < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }
        if read == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        offset = offset
            .checked_add(read as u64)
            .ok_or(std::io::ErrorKind::InvalidInput)?;
        buf = &mut buf[read as usize..];
    }
    Ok(())
}

#[cfg(windows)]
fn read_exact_at_uninit(
    file: &File,
    buf: &mut [std::mem::MaybeUninit<u8>],
    offset: u64,
) -> std::io::Result<()> {
    file.read_exact_at_uninit(buf, offset)
}

#[cfg(not(any(unix, windows)))]
fn read_exact_at_uninit(
    file: &File,
    buf: &mut [std::mem::MaybeUninit<u8>],
    offset: u64,
) -> std::io::Result<()> {
    for byte in buf.iter_mut() {
        byte.write(0);
    }
    // SAFETY: every element was initialized above. Portable positioned I/O
    // retains its established implementation on non-Unix targets.
    let bytes = unsafe { std::slice::from_raw_parts_mut(buf.as_mut_ptr().cast(), buf.len()) };
    read_exact_at_counted(file, bytes, offset).map(|_| ())
}

#[cfg(test)]
#[test]
fn direct_scan_uninit_read_checks_eof_and_preserves_position() {
    use std::io::{Seek, Write};
    use std::mem::MaybeUninit;
    let temp = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(temp.path().join("uninit-read"))
        .unwrap();
    file.write_all(b"abcdefghij").unwrap();
    let mut buf = [MaybeUninit::uninit(); 5];
    read_exact_at_uninit(&file, &mut buf, 2).unwrap();
    // SAFETY: the successful read initialized the complete buffer.
    let bytes = unsafe { std::slice::from_raw_parts(buf.as_ptr().cast::<u8>(), buf.len()) };
    assert_eq!(bytes, b"cdefg");
    assert_eq!(file.stream_position().unwrap(), 10);
    assert_eq!(
        read_exact_at_uninit(&file, &mut buf, 8).unwrap_err().kind(),
        std::io::ErrorKind::UnexpectedEof
    );
    read_exact_at_uninit(&file, &mut buf, 0).unwrap();
    let bytes = unsafe { std::slice::from_raw_parts(buf.as_ptr().cast::<u8>(), buf.len()) };
    assert_eq!(bytes, b"abcde");
    read_exact_at_uninit(&file, &mut [], 0).unwrap();
}

/// Read a set of consecutive page buffers. Unix uses one `preadv`; portable
/// targets retain the established per-page positioned-read behavior.
#[cfg(unix)]
fn read_pages_batch_at(file: &File, pages: &mut [&mut [u8]], offset: u64) -> std::io::Result<u64> {
    if pages.is_empty() {
        return Ok(0);
    }
    use std::os::fd::AsRawFd;
    let mut iovecs: Vec<libc::iovec> = pages
        .iter_mut()
        .map(|page| libc::iovec {
            iov_base: page.as_mut_ptr().cast(),
            iov_len: page.len(),
        })
        .collect();
    // SAFETY: every iovec points to a distinct live, writable Vec allocation
    // for the duration of the call; the file descriptor and offset are valid.
    let read = unsafe {
        libc::preadv(
            file.as_raw_fd(),
            iovecs.as_mut_ptr(),
            iovecs.len() as libc::c_int,
            offset as libc::off_t,
        )
    };
    if read < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let expected = pages.len().saturating_mul(PAGE_SIZE);
    if read as usize != expected {
        return Err(std::io::ErrorKind::UnexpectedEof.into());
    }
    Ok(1)
}

#[cfg(not(unix))]
fn read_pages_batch_at(
    file: &File,
    pages: &mut [&mut [u8]],
    mut offset: u64,
) -> std::io::Result<u64> {
    for page in pages.iter_mut() {
        read_exact_at(file, page, offset)?;
        offset = offset.saturating_add(PAGE_SIZE as u64);
    }
    Ok(pages.len() as u64)
}

#[cfg(windows)]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    read_exact_at_counted(file, buf, offset).map(|_| ())
}

#[cfg(windows)]
fn read_exact_at_counted(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<u64> {
    crate::util::file::read_exact_at_counted(file, buf, offset, false, None)
}

#[cfg(not(any(unix, windows)))]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    let mut file = file.try_clone()?;
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(buf)
}

#[cfg(not(any(unix, windows)))]
fn read_exact_at_counted(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<u64> {
    read_exact_at(file, buf, offset)?;
    Ok(1)
}

fn write_bulk_pages_at(
    durability: DurabilityMode,
    file: &File,
    buf: &[u8],
    offset: u64,
) -> std::io::Result<()> {
    write_all_at(file, buf, offset)?;
    if durability != DurabilityMode::Memory {
        crate::util::file::start_file_writeback(file, offset, buf.len())?;
    }
    Ok(())
}

#[cfg(unix)]
fn write_all_at(file: &File, buf: &[u8], offset: u64) -> std::io::Result<()> {
    FileExt::write_all_at(file, buf, offset)
}

#[cfg(windows)]
fn write_all_at(file: &File, mut buf: &[u8], mut offset: u64) -> std::io::Result<()> {
    while !buf.is_empty() {
        let n = file.write_at(buf, offset)?;
        if n == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        offset += n as u64;
        buf = &buf[n..];
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn write_all_at(file: &File, buf: &[u8], offset: u64) -> std::io::Result<()> {
    let mut file = file.try_clone()?;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(buf)
}

/// Pick the `wanted` oldest dirty frames by recovery LSN, then restore
/// file/page order for coalesced physical writes. This separates the recovery
/// policy (advance the redo floor) from the IO policy (keep neighbouring pages
/// together). A zero first-dirty LSN is placed last: it normally means a
/// deferred WAL pin and `flush_frame_with_wal` will reject it anyway.
fn oldest_frames_in_io_order(mut frames: Vec<Arc<Frame>>, wanted: usize) -> Vec<Arc<Frame>> {
    let wanted = wanted.min(frames.len());
    if wanted == 0 {
        return Vec::new();
    }
    let rec_lsn = |frame: &Arc<Frame>| {
        let lsn = frame.first_dirty_lsn.load(Ordering::Acquire);
        if lsn == 0 {
            u64::MAX
        } else {
            lsn
        }
    };
    if wanted < frames.len() {
        frames.select_nth_unstable_by(wanted, |a, b| {
            rec_lsn(a)
                .cmp(&rec_lsn(b))
                .then_with(|| a.key.file.hash.cmp(&b.key.file.hash))
                .then_with(|| a.key.page_id.cmp(&b.key.page_id))
        });
        frames.truncate(wanted);
    }
    frames.sort_unstable_by(|a, b| {
        a.key
            .file
            .hash
            .cmp(&b.key.file.hash)
            .then_with(|| a.key.page_id.cmp(&b.key.page_id))
    });
    frames
}

/// Pick dirty frames most likely to be CLOCK's next victims, then restore
/// file/page order for coalesced writes.
///
/// This is used only while the pool is above its dirty high-water mark. The
/// normal maintenance cleaner remains recovery-LSN ordered. A zero recovery
/// LSN stays last because it denotes a deferred WAL pin that cannot yet be
/// flushed.
fn cold_frames_in_io_order(mut frames: Vec<Arc<Frame>>, wanted: usize) -> Vec<Arc<Frame>> {
    let wanted = wanted.min(frames.len());
    if wanted == 0 {
        return Vec::new();
    }
    let rec_lsn = |frame: &Arc<Frame>| {
        let lsn = frame.first_dirty_lsn.load(Ordering::Acquire);
        if lsn == 0 {
            u64::MAX
        } else {
            lsn
        }
    };
    let cold_order = |a: &Arc<Frame>, b: &Arc<Frame>| {
        // Scan-origin pages are explicitly CLOCK's first-choice victims.
        b.scan_origin
            .load(Ordering::Acquire)
            .cmp(&a.scan_origin.load(Ordering::Acquire))
            // A cleared reference bit means CLOCK has already granted the
            // page its current second chance.
            .then_with(|| {
                a.ref_bit
                    .load(Ordering::Acquire)
                    .cmp(&b.ref_bit.load(Ordering::Acquire))
            })
            .then_with(|| {
                a.usage_count
                    .load(Ordering::Acquire)
                    .cmp(&b.usage_count.load(Ordering::Acquire))
            })
            .then_with(|| rec_lsn(a).cmp(&rec_lsn(b)))
            .then_with(|| a.key.file.hash.cmp(&b.key.file.hash))
            .then_with(|| a.key.page_id.cmp(&b.key.page_id))
    };
    if wanted < frames.len() {
        frames.select_nth_unstable_by(wanted, cold_order);
        frames.truncate(wanted);
    }
    frames.sort_unstable_by(|a, b| {
        a.key
            .file
            .hash
            .cmp(&b.key.file.hash)
            .then_with(|| a.key.page_id.cmp(&b.key.page_id))
    });
    frames
}

#[cfg(test)]
mod oldest_dirty_tests {
    use super::*;

    #[test]
    fn checkpoint_waits_for_in_progress_file_sync() {
        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool = Arc::new(BufferPool::new_with_options(
            8,
            log,
            DurabilityMode::Relaxed,
            Duration::ZERO,
            1,
            0,
        ));
        let path = dir.path().join("barrier.heap");
        std::fs::write(&path, vec![0u8; PAGE_SIZE]).unwrap();
        let file = pool
            .open_cached_file(&BufferPool::heap_file_key(path, 1))
            .unwrap();
        let barrier = file.sync_lock.lock();
        assert!(file.take_needs_sync());
        let (entered, entering) = std::sync::mpsc::channel();
        let (done, completed) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            entered.send(()).unwrap();
            pool.sync_cached_files().unwrap();
            done.send(()).unwrap();
        });
        entering.recv().unwrap();
        assert!(completed.recv_timeout(Duration::from_millis(30)).is_err());
        drop(barrier);
        completed.recv_timeout(Duration::from_secs(5)).unwrap();
        worker.join().unwrap();
    }

    #[test]
    fn relaxed_checkpoint_syncs_clean_cached_files_once() {
        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool =
            BufferPool::new_with_options(8, log, DurabilityMode::Relaxed, Duration::ZERO, 1, 0);
        let path = dir.path().join("checkpoint.heap");
        std::fs::write(&path, vec![0u8; PAGE_SIZE]).unwrap();
        let key = BufferPool::heap_file_key(path, 1);
        let file = pool.open_cached_file(&key).unwrap();
        file.mark_needs_sync();
        assert_eq!(pool.checkpoint_dirty_snapshot().unwrap().1, 0);
        pool.sync_cached_files().unwrap();
        assert_eq!(pool.file_syncs.load(Ordering::Relaxed), 1);
        pool.sync_cached_files().unwrap();
        assert_eq!(pool.file_syncs.load(Ordering::Relaxed), 1);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn device_sync_batch_preserves_new_dirty_flags_and_orders_locks() {
        let dir = tempfile::tempdir().unwrap();
        let pool = BufferPool::new_with_options(
            8,
            Arc::new(parking_lot::Mutex::new(
                LogManager::open(dir.path()).unwrap(),
            )),
            DurabilityMode::Relaxed,
            Duration::ZERO,
            1,
            0,
        );
        let mut files = Vec::new();
        for id in 1..=2 {
            let path = dir.path().join(format!("{id}.heap"));
            std::fs::write(&path, b"data").unwrap();
            files.push(
                pool.open_cached_file(&BufferPool::heap_file_key(path, id))
                    .unwrap(),
            );
        }
        files.sort_unstable_by_key(|file| Arc::as_ptr(file) as usize);
        let blocked = files[1].sync_lock.lock();
        let worker_files = vec![files[1].clone(), files[0].clone(), files[0].clone()];
        let worker_pool = pool.clone();
        let (send, done) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            send.send(worker_pool.sync_cached_files_by_device(worker_files))
                .unwrap();
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        while files[0].needs_sync.load(Ordering::Acquire) {
            assert!(
                Instant::now() < deadline,
                "batch did not acquire the first lock"
            );
            std::thread::yield_now();
        }
        assert!(done.try_recv().is_err());
        files[0].mark_needs_sync();
        drop(blocked);
        done.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
        worker.join().unwrap();
        assert!(files[0].needs_sync.load(Ordering::Acquire));
        assert!(!files[1].needs_sync.load(Ordering::Acquire));
        assert_eq!(pool.file_syncs.load(Ordering::Relaxed), 2);
        pool.sync_cached_files_by_device(files.clone()).unwrap();
        assert!(!files[0].needs_sync.load(Ordering::Acquire));
        assert_eq!(pool.file_syncs.load(Ordering::Relaxed), 3);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn direct_and_cached_sync_covers_replaced_inodes_and_retries_errors() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let pool = BufferPool::new_with_options(
            8,
            Arc::new(parking_lot::Mutex::new(
                LogManager::open(dir.path()).unwrap(),
            )),
            DurabilityMode::Relaxed,
            Duration::ZERO,
            1,
            0,
        );
        let path = dir.path().join("data.heap");
        std::fs::write(&path, b"old").unwrap();
        let key = BufferPool::heap_file_key(path.clone(), 1);
        let cached = pool.open_cached_file(&key).unwrap();
        let direct = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        pool.sync_cached_and_direct_file(&key, &direct, true)
            .unwrap();
        assert!(!cached.needs_sync.load(Ordering::Acquire));
        assert_eq!(pool.file_syncs.load(Ordering::Relaxed), 1);
        // A renamed direct handle and the current cache key can refer to
        // different inodes. Both identities must reach the barrier.
        let retired = dir.path().join("retired.heap");
        std::fs::rename(&path, &retired).unwrap();
        std::fs::write(&path, b"new").unwrap();
        let replacement = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        cached.mark_needs_sync();
        pool.sync_cached_and_direct_file(&key, &replacement, true)
            .unwrap();
        assert!(!cached.needs_sync.load(Ordering::Acquire));
        assert_eq!(std::fs::read(&retired).unwrap(), b"old");
        assert_eq!(std::fs::read(&path).unwrap(), b"new");
        cached.mark_needs_sync();
        let invalid = File::open("/dev/null").unwrap();
        assert!(pool
            .sync_cached_and_direct_file(&key, &invalid, true)
            .is_err());
        assert!(cached.needs_sync.load(Ordering::Acquire));
        pool.sync_cached_and_direct_file(&key, &replacement, true)
            .unwrap();
        assert!(!cached.needs_sync.load(Ordering::Acquire));
        assert_eq!(pool.file_syncs.load(Ordering::Relaxed), 3);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn device_sync_batch_restores_every_dirty_flag_on_error() {
        let dir = tempfile::tempdir().unwrap();
        let pool = BufferPool::new_with_options(
            8,
            Arc::new(parking_lot::Mutex::new(
                LogManager::open(dir.path()).unwrap(),
            )),
            DurabilityMode::Relaxed,
            Duration::ZERO,
            1,
            0,
        );
        let path = dir.path().join("data");
        std::fs::write(&path, b"data").unwrap();
        let good = Arc::new(CachedFile::new(File::open(path).unwrap(), 4));
        let bad = Arc::new(CachedFile::new(File::open("/dev/null").unwrap(), 0));
        assert!(pool
            .sync_cached_files_by_device(vec![good.clone(), bad.clone()])
            .is_err());
        assert!(good.needs_sync.load(Ordering::Acquire));
        assert!(bad.needs_sync.load(Ordering::Acquire));
        assert_eq!(pool.file_syncs.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn index_page_snapshots_do_not_retain_decoded_pages() {
        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool =
            BufferPool::new_with_options(8, log, DurabilityMode::Memory, Duration::ZERO, 1, 0);
        let leaf = IndexLeafPage::new(
            vec![IndexEntry {
                key: IndexKey::from(vec![KeyPart::Int(7)]),
                row_id: RowId {
                    page_id: 1,
                    slot_id: 2,
                },
            }],
            None,
        );
        let bytes = encode_index_tree_page(&IndexPage::Leaf(leaf)).unwrap();
        let frame = Arc::new(Frame::new_loading(
            PageKey {
                file: Arc::new(BufferPool::index_file_key(dir.path().join("cache.idx"))),
                page_id: 1,
            },
            0,
            AccessIntent::Normal,
            RawPageLease {
                bytes,
                charge: controlled_raw_page_charge().unwrap(),
            },
        ));

        let first = pool.index_page_for_read(&frame).unwrap();
        let second = pool.index_page_for_read(&frame).unwrap();
        assert!(!Arc::ptr_eq(&first, &second));
        assert_eq!(Arc::strong_count(&first), 1);
        assert_eq!(Arc::strong_count(&second), 1);

        let replacement = IndexLeafPage::new(
            vec![IndexEntry {
                key: IndexKey::from(vec![KeyPart::Int(9)]),
                row_id: RowId {
                    page_id: 2,
                    slot_id: 3,
                },
            }],
            None,
        );
        *frame.bytes.write().unwrap() =
            encode_index_tree_page(&IndexPage::Leaf(replacement)).unwrap();
        frame.mark_dirty(1);
        let third = pool.index_page_for_read(&frame).unwrap();
        let IndexPage::Leaf(old) = first.as_ref() else {
            panic!("expected leaf")
        };
        let IndexPage::Leaf(new) = third.as_ref() else {
            panic!("expected leaf")
        };
        assert_eq!(old.entries[0].key, IndexKey::from(vec![KeyPart::Int(7)]));
        assert_eq!(new.entries[0].key, IndexKey::from(vec![KeyPart::Int(9)]));
        let weak = Arc::downgrade(&third);
        drop(third);
        assert!(
            weak.upgrade().is_none(),
            "frame must not retain decoded snapshots"
        );
    }

    #[test]
    fn heap_page_presence_is_cached_by_dirty_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let frame = Frame::new_loading(
            PageKey {
                file: Arc::new(BufferPool::heap_file_key(
                    dir.path().join("presence.heap"),
                    7,
                )),
                page_id: 0,
            },
            0,
            AccessIntent::Scan,
            RawPageLease {
                bytes: vec![0; PAGE_SIZE],
                charge: controlled_raw_page_charge().unwrap(),
            },
        );
        let zero = vec![0; PAGE_SIZE];
        assert!(
            !BufferPool::validated_heap_page_present_with_cache(&frame, &zero, None, 0).unwrap()
        );
        assert_eq!(frame.heap_header_validated_epoch.load(Ordering::Acquire), 0);
        assert!(!frame.heap_header_present.load(Ordering::Relaxed));

        // A matching epoch consumes the cached presence bit; it must not
        // inspect a replacement byte slice until mutation advances the epoch.
        let formatted = Page::new().encode();
        assert!(
            !BufferPool::validated_heap_page_present_with_cache(&frame, &formatted, None, 0)
                .unwrap()
        );
        frame.mark_dirty(1);
        assert!(
            BufferPool::validated_heap_page_present_with_cache(&frame, &formatted, None, 0)
                .unwrap()
        );
        assert!(frame.heap_header_present.load(Ordering::Relaxed));
    }

    #[test]
    fn loading_page_completion_wakes_only_its_own_waiters() {
        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool = BufferPool::new_with_options(
            2,
            log,
            DurabilityMode::Strict,
            Duration::from_secs(1),
            1,
            0,
        );
        let file = Arc::new(BufferPool::index_file_key(dir.path().join("wait.idx")));
        let frame = |page_id| {
            Arc::new(Frame::new_loading(
                PageKey {
                    file: Arc::clone(&file),
                    page_id,
                },
                0,
                AccessIntent::Normal,
                RawPageLease {
                    bytes: vec![0; PAGE_SIZE],
                    charge: controlled_raw_page_charge().unwrap(),
                },
            ))
        };
        let first = frame(1);
        let second = frame(2);
        let (tx, rx) = std::sync::mpsc::channel();

        std::thread::scope(|scope| {
            for (id, frame) in [(1, Arc::clone(&first)), (2, Arc::clone(&second))] {
                let pool = Arc::clone(&pool);
                let tx = tx.clone();
                scope.spawn(move || {
                    pool.wait_until_ready(&frame).unwrap();
                    tx.send(id).unwrap();
                });
            }
            let deadline = Instant::now() + Duration::from_secs(1);
            while (first.io_waiters.load(Ordering::Acquire) == 0
                || second.io_waiters.load(Ordering::Acquire) == 0)
                && Instant::now() < deadline
            {
                std::thread::yield_now();
            }
            assert_eq!(first.io_waiters.load(Ordering::Acquire), 1);
            assert_eq!(second.io_waiters.load(Ordering::Acquire), 1);

            first.publish_io_state(FrameIoState::Ready);
            assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), 1);
            assert!(
                rx.recv_timeout(Duration::from_millis(20)).is_err(),
                "publishing one frame must not release an unrelated page waiter"
            );

            second.publish_io_state(FrameIoState::Ready);
            assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), 2);
        });
    }

    #[test]
    fn buffer_pool_kind_read_ios_count_cold_batch_and_bypass_but_not_hits() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let heap_path = dir.path().join("reads.heap");
        let zone_path = dir.path().join("reads.zm");
        for path in [&heap_path, &zone_path] {
            File::create(path)
                .unwrap()
                .set_len(3 * PAGE_SIZE as u64)
                .unwrap();
        }
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool =
            BufferPool::new_with_options(16, log, DurabilityMode::Memory, Duration::ZERO, 1, 0);
        let heap = Arc::new(BufferPool::heap_file_key(heap_path, 1));
        let zone = Arc::new(BufferPool::zone_map_file_key(zone_path, 1, 1));
        for file in [&heap, &zone] {
            let key = |page_id| PageKey {
                file: Arc::clone(file),
                page_id,
            };
            drop(pool.pin_page(key(0)).unwrap());
            assert_eq!(
                pool.kind_read_ios[file.kind.slot()].load(Ordering::Relaxed),
                1
            );
            drop(pool.pin_page(key(0)).unwrap());
            assert_eq!(
                pool.kind_read_ios[file.kind.slot()].load(Ordering::Relaxed),
                1
            );
            pool.load_page_run(&[key(1), key(2)], AccessIntent::Scan)
                .unwrap();
            assert_eq!(
                pool.kind_read_ios[file.kind.slot()].load(Ordering::Relaxed),
                3
            );
            pool.load_page_run(&[key(1), key(2)], AccessIntent::Scan)
                .unwrap();
            assert_eq!(
                pool.kind_read_ios[file.kind.slot()].load(Ordering::Relaxed),
                3
            );
        }
        let mut raw = vec![0; PAGE_SIZE * 2];
        pool.read_raw_scan_batch(&heap, 0, &mut raw).unwrap();
        let snapshot = pool.snapshot();
        assert_eq!(snapshot.kind_read_ios, [5, 0, 0, 0, 3]);
        assert_eq!(
            snapshot.read_ios,
            snapshot.kind_read_ios.iter().sum::<u64>()
        );
    }

    #[test]
    fn failed_batch_reservation_releases_loading_frames() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("partial-read.heap");
        File::create(&path)
            .unwrap()
            .set_len(3 * PAGE_SIZE as u64)
            .unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool =
            BufferPool::new_with_options(2, log, DurabilityMode::Memory, Duration::ZERO, 1, 0);
        let file = Arc::new(BufferPool::heap_file_key(path, 1));
        let key = |page_id| PageKey {
            file: Arc::clone(&file),
            page_id,
        };
        let pinned = pool.pin_page(key(0)).unwrap();
        assert!(pool
            .load_page_run(&[key(1), key(2)], AccessIntent::Probe)
            .is_err());
        assert!(
            pool.lookup_frame(&key(1)).unwrap().is_none(),
            "failed batch stranded a Loading frame"
        );
        assert_eq!(pool.used_pages.load(Ordering::Relaxed), 1);
        drop(pinned);
        pool.load_page_run(&[key(1)], AccessIntent::Probe).unwrap();
        let frame = pool.lookup_frame(&key(1)).unwrap().unwrap();
        assert!(frame.is_ready());
        assert_eq!(frame.pin_count.load(Ordering::Acquire), 0);
    }

    #[test]
    fn clock_checks_other_shards_before_exhausting_pinned_shard() {
        // A range reader can pin a whole shard while LOB misses need space.
        // Searching it four times per miss must not hide a ready victim in
        // another shard. Exercise ordinary and over-quota victim selection.
        for (quota, victim_shard) in [(false, 1), (true, 1), (false, 0), (true, 0)] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("pinned-shard.heap");
            File::create(&path)
                .unwrap()
                .set_len(4096 * PAGE_SIZE as u64)
                .unwrap();
            let log = Arc::new(parking_lot::Mutex::new(
                LogManager::open(dir.path()).unwrap(),
            ));
            let pool = BufferPool::new_with_options(
                512,
                log,
                DurabilityMode::Memory,
                Duration::ZERO,
                1,
                0,
            );
            pool.shutdown_flusher();
            for slot in 0..BUFFER_PAGE_KIND_COUNT {
                pool.set_kind_quota_pct(slot, 0);
            }
            let file = Arc::new(BufferPool::heap_file_key(path, 1));
            let keys = (0..4096).map(|page_id| PageKey {
                file: file.clone(),
                page_id,
            });
            let pinned: Vec<_> = keys
                .clone()
                .filter(|key| pool.shard_index(key) == 0)
                .take(256)
                .map(|key| pool.pin_page(key).unwrap())
                .collect();
            assert_eq!(pinned.len(), 256);
            let victim_key = keys
                .filter(|key| pool.shard_index(key) == victim_shard)
                .nth(if victim_shard == 0 { 256 } else { 0 })
                .unwrap();
            let victim = pool.pin_page(victim_key.clone()).unwrap();
            victim.frame.ref_bit.store(false, Ordering::Release);
            victim.frame.usage_count.store(0, Ordering::Release);
            drop(victim);
            if quota {
                // Keep an under-quota, evictable LOB page in the first shard:
                // the bounded probe must still prefer the heap victim.
                let lob_path = dir.path().join("other.lob");
                File::create(&lob_path)
                    .unwrap()
                    .set_len(64 * PAGE_SIZE as u64)
                    .unwrap();
                let lob = Arc::new(BufferPool::overflow_file_key(lob_path, 1));
                let key = (0..64)
                    .map(|page_id| PageKey {
                        file: lob.clone(),
                        page_id,
                    })
                    .find(|key| pool.shard_index(key) == 0)
                    .unwrap();
                drop(pool.pin_page(key).unwrap());
                pool.set_kind_quota_pct(file.kind.slot(), 1);
            }
            let before = pool.eviction_scans.load(Ordering::Relaxed);
            let (_, key, _) = pool.find_clock_victim_from(true, 0).unwrap().unwrap();
            let probes = pool.eviction_scans.load(Ordering::Relaxed) - before;
            assert_eq!(key, victim_key);
            if victim_shard == 1 {
                assert!(
                    probes <= 64,
                    "quota={quota}: searched {probes} frames before trying another shard"
                );
            } else {
                assert!(
                    probes > 32,
                    "the full fallback must find victims beyond the bounded window"
                );
            }
        }
    }

    #[test]
    fn normal_eviction_visits_every_shard_when_cold_queues_are_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fair-clock.heap");
        let file = File::create(&path).unwrap();
        file.set_len(32 * PAGE_SIZE as u64).unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool =
            BufferPool::new_with_options(32, log, DurabilityMode::Memory, Duration::ZERO, 1, 0);
        let file = Arc::new(BufferPool::heap_file_key(path, 1));
        for page_id in 0..32 {
            let key = PageKey {
                file: Arc::clone(&file),
                page_id,
            };
            let pinned = pool.pin_page(key).unwrap();
            pinned.frame.ref_bit.store(false, Ordering::Release);
            pinned.frame.usage_count.store(0, Ordering::Release);
        }
        assert_eq!(pool.scan_origin_count.load(Ordering::Relaxed), 0);
        for _ in 0..8 {
            assert!(pool.evict_one_with_policy(true).unwrap());
        }
        let evictions = pool
            .shards
            .iter()
            .map(|shard| shard.evictions.load(Ordering::Relaxed))
            .collect::<Vec<_>>();
        assert!(
            evictions.iter().all(|count| *count > 0),
            "eviction skipped half the shards: {evictions:?}"
        );
    }

    #[test]
    fn normal_pressure_uses_one_clock_ticket_when_cold_queues_are_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("normal-pressure.heap");
        let mut file = File::create(&path).unwrap();
        for byte in [10u8, 20] {
            file.write_all(&vec![byte; PAGE_SIZE]).unwrap();
        }
        drop(file);

        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool =
            BufferPool::new_with_options(1, log, DurabilityMode::Strict, Duration::ZERO, 1, 0);
        let file_key = Arc::new(BufferPool::heap_file_key(path, 3));
        let key = |page_id| PageKey {
            file: Arc::clone(&file_key),
            page_id,
        };

        drop(pool.pin_page(key(0)).unwrap());
        let hand_before = pool.eviction_hand.load(Ordering::Relaxed);
        drop(pool.pin_page(key(1)).unwrap());
        let hand_after = pool.eviction_hand.load(Ordering::Relaxed);

        assert_eq!(pool.snapshot().cold_recycle_fallbacks, 1);
        assert_eq!(
            hand_after - hand_before,
            1,
            "all eviction policies must share one round-robin ticket"
        );
    }

    #[test]
    fn ordered_sparse_reads_complete_with_reused_or_busy_helpers() {
        for helpers_busy in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("sparse.heap");
            let bytes = (0..16u8)
                .flat_map(|value| std::iter::repeat_n(value, PAGE_SIZE))
                .collect::<Vec<_>>();
            fs::write(&path, bytes).unwrap();
            let pool = BufferPool::new_with_options(
                32,
                Arc::new(parking_lot::Mutex::new(
                    LogManager::open(dir.path()).unwrap(),
                )),
                DurabilityMode::Strict,
                Duration::from_secs(1),
                4,
                0,
            );
            pool.shutdown_flusher();
            if helpers_busy {
                pool.ordered_read_helpers
                    .store(pool.io_threads, Ordering::Release);
            }
            let file = Arc::new(BufferPool::heap_file_key(path, 1));
            let pages = (0..16u64).step_by(2).collect::<Vec<_>>();
            pool.load_ordered_probe_pages(&file, &pages).unwrap();
            for &page_id in &pages {
                let image = pool
                    .read_raw_page(PageKey {
                        file: file.clone(),
                        page_id,
                    })
                    .unwrap();
                assert!(image.iter().all(|byte| *byte == page_id as u8));
            }
            assert!(pool.ordered_read_helpers.load(Ordering::Acquire) <= pool.io_threads);
        }
    }

    #[test]
    fn raw_sparse_probes_overlay_dirty_pages_and_propagate_io_errors() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let path = dir.path().join("raw-sparse.heap");
        let bytes: Vec<_> = (0..16u8)
            .flat_map(|id| {
                let mut page = Page::new();
                page.data[128] = id;
                page.encode()
            })
            .collect();
        fs::write(&path, bytes).unwrap();
        let pool = BufferPool::new_with_options(
            32,
            Arc::new(parking_lot::Mutex::new(
                LogManager::open(dir.path()).unwrap(),
            )),
            DurabilityMode::Strict,
            Duration::from_secs(1),
            4,
            0,
        );
        pool.shutdown_flusher();
        let file = Arc::new(BufferPool::heap_file_key(path.clone(), 1));
        let dirty = PageKey {
            file: Arc::clone(&file),
            page_id: 2,
        };
        pool.with_raw_page_mut(dirty.clone(), 0, |bytes| {
            bytes[128] = 99;
            Ok(())
        })
        .unwrap();
        let mut visited = Vec::new();
        assert!(pool
            .with_raw_probe_pages(&file, &[0, 2, 4, 6, 8, 10, 12, 14], |id, bytes| {
                assert_eq!(bytes[128], if id == 2 { 99 } else { id as u8 });
                visited.push(id);
                Ok(())
            })
            .unwrap());
        assert_eq!(visited, [0, 2, 4, 6, 8, 10, 12, 14]);
        assert!(pool
            .lookup_frame(&PageKey {
                file: Arc::clone(&file),
                page_id: 14
            })
            .unwrap()
            .is_none());
        assert_eq!(pool.snapshot().pinned_pages, 0);
        OpenOptions::new()
            .write(true)
            .open(path)
            .unwrap()
            .set_len(8 * PAGE_SIZE as u64)
            .unwrap();
        assert!(pool
            .with_raw_probe_pages(&file, &[0, 2, 4, 6, 8, 10, 12, 14], |_, _| Ok(()))
            .is_err());
        assert_eq!(pool.snapshot().pinned_pages, 0);
    }

    #[test]
    fn ordered_sparse_read_error_completes_and_releases_loading_frames() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("truncated-sparse.heap");
        fs::write(&path, vec![17u8; 3 * PAGE_SIZE]).unwrap();
        let pool = BufferPool::new_with_options(
            32,
            Arc::new(parking_lot::Mutex::new(
                LogManager::open(dir.path()).unwrap(),
            )),
            DurabilityMode::Strict,
            Duration::from_secs(1),
            4,
            0,
        );
        pool.shutdown_flusher();
        let file = Arc::new(BufferPool::heap_file_key(path.clone(), 1));
        // Cache the original length, then simulate a truncated data page.
        pool.open_cached_file(&file).unwrap();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len((2 * PAGE_SIZE + 13) as u64)
            .unwrap();
        assert!(pool.load_ordered_probe_pages(&file, &[0, 2]).is_err());
        let key = PageKey { file, page_id: 2 };
        assert!(pool.lookup_frame(&key).unwrap().is_none());
        assert_eq!(pool.snapshot().pinned_pages, 0);
    }

    #[test]
    fn configured_data_file_io_preserves_pages_and_short_read_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("configured-io.heap");
        fs::write(&path, vec![0u8; 2 * PAGE_SIZE]).unwrap();
        let pool = BufferPool::new_with_options(
            8,
            Arc::new(parking_lot::Mutex::new(
                LogManager::open(dir.path()).unwrap(),
            )),
            DurabilityMode::Strict,
            Duration::from_secs(1),
            1,
            0,
        );
        pool.shutdown_flusher();
        let key = BufferPool::heap_file_key(path, 1);
        let cached = pool.open_cached_file(&key).unwrap();
        #[cfg(target_os = "linux")]
        if !data_file_direct_io_enabled() {
            assert!(cached.direct_read.is_none());
        }
        for (page, byte) in [(0, 17), (1, 93)] {
            write_cached_page_at(&cached, &vec![byte; PAGE_SIZE], page * PAGE_SIZE as u64).unwrap();
        }
        let mut first = vec![0u8; PAGE_SIZE];
        let mut second = vec![0u8; PAGE_SIZE];
        read_cached_pages_at(&cached, &mut [&mut first, &mut second], 0).unwrap();
        assert!(first.iter().all(|byte| *byte == 17));
        assert!(second.iter().all(|byte| *byte == 93));
        assert_eq!(
            read_cached_page_at(&cached, &mut first, 2 * PAGE_SIZE as u64)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::UnexpectedEof,
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn direct_io_accepts_aligned_and_unaligned_page_buffers() {
        use std::os::unix::fs::OpenOptionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("aligned.heap");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        let mut cached = CachedFile::new(file, 0);
        cached.direct_read = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(&path)
            .ok();
        for shift in [0, 1] {
            let mut allocation = vec![0u8; PAGE_SIZE * 2 + 4096];
            let start = allocation.as_ptr().align_offset(4096) + shift;
            let (first, rest) = allocation[start..start + 2 * PAGE_SIZE].split_at_mut(PAGE_SIZE);
            let second = &mut rest[..PAGE_SIZE];
            first.fill(31 + shift as u8);
            second.fill(47 + shift as u8);
            write_cached_page_at(&cached, first, 0).unwrap();
            write_cached_page_at(&cached, second, PAGE_SIZE as u64).unwrap();
            first.fill(0);
            second.fill(0);
            read_cached_page_at(&cached, first, 0).unwrap();
            assert!(first.iter().all(|byte| *byte == 31 + shift as u8));
            read_cached_pages_at(&cached, &mut [&mut *first, &mut *second], 0).unwrap();
            assert!(first.iter().all(|byte| *byte == 31 + shift as u8));
            assert!(second.iter().all(|byte| *byte == 47 + shift as u8));
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn direct_batch_reads_preserve_page_boundaries_and_error_contract() {
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::OpenOptionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("direct-batch.heap");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        let mut cached = CachedFile::new(file, 0);
        cached.direct_read = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(format!("/proc/self/fd/{}", cached.file.as_raw_fd()))
            .ok();
        for page in 0..5 {
            write_all_at(
                &cached.file,
                &vec![page as u8 + 10; PAGE_SIZE],
                page * PAGE_SIZE as u64,
            )
            .unwrap();
        }
        let mut buffers = [vec![0; PAGE_SIZE], vec![0; PAGE_SIZE], vec![0; PAGE_SIZE]];
        for fallback in [false, true] {
            cached
                .direct_read_disabled
                .store(fallback, Ordering::Relaxed);
            let mut pages: Vec<&mut [u8]> = buffers.iter_mut().map(Vec::as_mut_slice).collect();
            assert_eq!(
                read_cached_pages_at(&cached, &mut pages, PAGE_SIZE as u64).unwrap(),
                1
            );
            for (i, page) in pages.iter().enumerate() {
                assert!(page.iter().all(|byte| *byte == i as u8 + 11));
            }
        }
        cached.direct_read_disabled.store(false, Ordering::Relaxed);
        cached.file.set_len(3 * PAGE_SIZE as u64 + 17).unwrap();
        let mut pages: Vec<&mut [u8]> = buffers.iter_mut().map(Vec::as_mut_slice).collect();
        assert_eq!(
            read_cached_pages_at(&cached, &mut pages, PAGE_SIZE as u64)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::UnexpectedEof
        );
        assert_eq!(read_cached_pages_at(&cached, &mut [], 0).unwrap(), 0);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn direct_page_writes_invalidate_buffered_reads_and_preserve_inode_identity() {
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::OpenOptionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("direct-write.heap");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        let mut cached = CachedFile::new(file, 0);
        cached.direct_read = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(format!("/proc/self/fd/{}", cached.file.as_raw_fd()))
            .ok();
        write_all_at(&cached.file, &vec![7; PAGE_SIZE], 0).unwrap();
        let mut bytes = vec![0; PAGE_SIZE];
        read_exact_at_counted(&cached.file, &mut bytes, 0).unwrap();
        write_cached_page_at(&cached, &vec![13; PAGE_SIZE], 0).unwrap();
        read_exact_at_counted(&cached.file, &mut bytes, 0).unwrap();
        assert!(bytes.iter().all(|byte| *byte == 13));
        write_cached_page_at(&cached, &vec![29; PAGE_SIZE], PAGE_SIZE as u64).unwrap();
        cached.file.sync_all().unwrap();
        assert_eq!(cached.file.metadata().unwrap().len(), 2 * PAGE_SIZE as u64);
        read_cached_page_at(&cached, &mut bytes, PAGE_SIZE as u64).unwrap();
        assert!(bytes.iter().all(|byte| *byte == 29));
        let renamed = dir.path().join("old.heap");
        fs::rename(&path, &renamed).unwrap();
        fs::write(&path, vec![99; PAGE_SIZE]).unwrap();
        write_cached_page_at(&cached, &vec![41; PAGE_SIZE], 0).unwrap();
        read_exact_at_counted(&File::open(&renamed).unwrap(), &mut bytes, 0).unwrap();
        assert!(bytes.iter().all(|byte| *byte == 41));
        assert_eq!(fs::read(&path).unwrap(), vec![99; PAGE_SIZE]);
        cached.direct_read_disabled.store(true, Ordering::Relaxed);
        write_cached_page_at(&cached, &vec![53; PAGE_SIZE], 0).unwrap();
        read_cached_page_at(&cached, &mut bytes, 0).unwrap();
        assert!(bytes.iter().all(|byte| *byte == 53));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn direct_page_reads_observe_buffered_writes_and_report_truncation() {
        use std::os::unix::fs::OpenOptionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("direct-read.heap");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        let mut cached = CachedFile::new(file, 0);
        cached.direct_read = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(&path)
            .ok();
        let mut bytes = vec![0u8; PAGE_SIZE];
        // Do not fsync here: a loading frame must see a previous buffered
        // cleaner write even if checkpoint has not synchronized the file yet.
        for value in [17u8, 29, 43] {
            write_all_at(&cached.file, &vec![value; PAGE_SIZE], 0).unwrap();
            read_cached_page_at(&cached, &mut bytes, 0).unwrap();
            assert!(bytes.iter().all(|&byte| byte == value));
        }
        write_all_at(&cached.file, &vec![71; PAGE_SIZE], PAGE_SIZE as u64).unwrap();
        read_cached_page_at(&cached, &mut bytes, PAGE_SIZE as u64).unwrap();
        assert!(bytes.iter().all(|&byte| byte == 71));
        cached.file.set_len(PAGE_SIZE as u64 + 17).unwrap();
        assert_eq!(
            read_cached_page_at(&cached, &mut bytes, PAGE_SIZE as u64)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::UnexpectedEof,
        );
        // Explicit fallback follows the same byte/EOF contract.
        cached.direct_read_disabled.store(true, Ordering::Relaxed);
        read_cached_page_at(&cached, &mut bytes, 0).unwrap();
        assert!(bytes.iter().all(|&byte| byte == 43));
    }

    #[test]
    fn clean_clock_pass_ages_dirty_pages_without_selecting_them() {
        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        // No cleaner: isolate CLOCK's aging from asynchronous writeback.
        let pool =
            BufferPool::new_with_options(8, log, DurabilityMode::Memory, Duration::ZERO, 1, 0);
        let key = PageKey {
            file: Arc::new(BufferPool::heap_file_key(
                dir.path().join("dirty-aging.heap"),
                1,
            )),
            page_id: 0,
        };
        pool.store_page(key.clone(), vec![0; PAGE_SIZE], 0).unwrap();
        let frame = pool.lookup_frame(&key).unwrap().unwrap();
        frame.ref_bit.store(true, Ordering::Release);
        frame.usage_count.store(2, Ordering::Release);
        assert!(pool.find_clock_victim(true).unwrap().is_none());
        assert!(frame.dirty.load(Ordering::Acquire));
        assert!(!frame.ref_bit.load(Ordering::Acquire));
        assert_eq!(frame.usage_count.load(Ordering::Acquire), 0);
        assert_eq!(pool.snapshot().write_ios, 0);
        // A subsequent access makes it hot again; aging is not a permanent
        // demotion and must not bypass the pin/deferred-WAL safeguards.
        frame.touch(AccessIntent::Normal);
        frame.pin_count.store(1, Ordering::Release);
        assert!(pool.find_clock_victim(true).unwrap().is_none());
        assert!(frame.ref_bit.load(Ordering::Acquire));
        assert_eq!(frame.usage_count.load(Ordering::Acquire), 1);
        frame.pin_count.store(0, Ordering::Release);
    }

    #[test]
    fn concurrent_clock_hands_do_not_wrap_page_heat() {
        let frame = Frame::new_dirty(
            PageKey {
                file: Arc::new(BufferPool::heap_file_key(
                    PathBuf::from("clock-race.heap"),
                    1,
                )),
                page_id: 0,
            },
            vec![0; PAGE_SIZE],
            0,
            0,
            controlled_raw_page_charge().unwrap(),
        );
        let barrier = std::sync::Barrier::new(9);
        let chances = AtomicUsize::new(0);
        let mut violations = 0;
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..128 {
                        barrier.wait();
                        if frame.clock_second_chance() {
                            chances.fetch_add(1, Ordering::Relaxed);
                        }
                        barrier.wait();
                    }
                });
            }
            for _ in 0..128 {
                frame.ref_bit.store(false, Ordering::Release);
                frame.usage_count.store(1, Ordering::Release);
                chances.store(0, Ordering::Relaxed);
                barrier.wait();
                barrier.wait();
                if chances.load(Ordering::Relaxed) != 1
                    || frame.usage_count.load(Ordering::Acquire) != 0
                {
                    violations += 1;
                }
            }
        });
        // Assert after joining so a regression cannot strand workers at the
        // next barrier and turn a useful failure into a test timeout.
        assert_eq!(violations, 0);
    }

    #[test]
    fn normal_page_heat_saturates_at_bounded_clock_passes() {
        let dir = tempfile::tempdir().unwrap();
        let frame = Frame::new_loading(
            PageKey {
                file: Arc::new(BufferPool::heap_file_key(dir.path().join("heat.heap"), 1)),
                page_id: 0,
            },
            0,
            AccessIntent::Normal,
            RawPageLease {
                bytes: vec![0; PAGE_SIZE],
                charge: controlled_raw_page_charge().unwrap(),
            },
        );
        for _ in 0..64 {
            frame.touch(AccessIntent::Normal);
        }
        assert_eq!(
            frame.usage_count.load(Ordering::Acquire),
            Frame::MAX_NORMAL_USAGE_COUNT
        );
        assert_eq!(Frame::MAX_NORMAL_USAGE_COUNT, 6);
    }

    #[test]
    fn ordered_probe_completion_preserves_existing_hot_pages() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hot-probe.heap");
        File::create(&path)
            .unwrap()
            .write_all(&vec![17; PAGE_SIZE])
            .unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool =
            BufferPool::new_with_options(1, log, DurabilityMode::Strict, Duration::ZERO, 1, 0);
        let file = Arc::new(BufferPool::heap_file_key(path, 1));
        let key = PageKey {
            file: Arc::clone(&file),
            page_id: 0,
        };
        for _ in 0..8 {
            drop(pool.pin_page(key.clone()).unwrap());
        }
        pool.load_ordered_probe_pages(&file, &[0]).unwrap();
        pool.demote_ordered_probe_pages(&file, &[0]).unwrap();
        let frame = pool.lookup_frame(&key).unwrap().unwrap();
        assert!(!frame.scan_origin.load(Ordering::Acquire));
        assert_eq!(
            frame.usage_count.load(Ordering::Acquire),
            Frame::MAX_NORMAL_USAGE_COUNT
        );
    }

    #[test]
    fn consumed_ordered_probe_pages_reenter_cold_candidate_queue() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ordered-probe-cold.heap");
        let mut file = File::create(&path).unwrap();
        for byte in [10u8, 20, 30] {
            file.write_all(&vec![byte; PAGE_SIZE]).unwrap();
        }
        drop(file);

        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool =
            BufferPool::new_with_options(2, log, DurabilityMode::Strict, Duration::ZERO, 1, 0);
        let file_key = Arc::new(BufferPool::heap_file_key(path, 3));
        let key = |page_id| PageKey {
            file: Arc::clone(&file_key),
            page_id,
        };

        pool.load_ordered_probe_pages(&file_key, &[0, 1]).unwrap();
        for page_id in 0..2 {
            let frame = pool.lookup_frame(&key(page_id)).unwrap().unwrap();
            assert!(
                !frame.scan_origin.load(Ordering::Acquire),
                "active ordered-probe window must remain protected until decode"
            );
        }

        pool.demote_ordered_probe_pages(&file_key, &[0, 1]).unwrap();
        for page_id in 0..2 {
            let frame = pool.lookup_frame(&key(page_id)).unwrap().unwrap();
            assert!(frame.scan_origin.load(Ordering::Acquire));
            assert_eq!(frame.usage_count.load(Ordering::Acquire), 0);
            assert!(!frame.ref_bit.load(Ordering::Acquire));
        }

        drop(pool.pin_page(key(2)).unwrap());
        assert_eq!(pool.snapshot().cold_recycle_hits, 1);
        assert_eq!(pool.snapshot().cold_recycle_fallbacks, 0);
    }

    #[test]
    fn cold_candidate_metadata_does_not_accumulate_promoted_evictions() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let path = dir.path().join("bounded-cold-metadata.heap");
        File::create(&path)
            .unwrap()
            .set_len(1024 * PAGE_SIZE as u64)
            .unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool =
            BufferPool::new_with_options(128, log, DurabilityMode::Memory, Duration::ZERO, 1, 0);
        let file = Arc::new(BufferPool::heap_file_key(path, 1));
        for page_id in 0..1024 {
            let pinned = pool
                .pin_page_with_intent(
                    PageKey {
                        file: file.clone(),
                        page_id,
                    },
                    AccessIntent::Normal,
                )
                .unwrap();
            // Demand promotion prevents the cold eviction path from draining
            // the old weak entries; ordinary CLOCK must reclaim these pages.
            pool.demote_to_scan_origin(pool.shard_index(&pinned.frame.key), &pinned.frame);
            pool.promote_scan_origin(&pinned.frame);
        }
        let retained = pool
            .shards
            .iter()
            .map(|shard| shard.cold_candidates.lock().unwrap().len())
            .sum::<usize>();
        assert!(
            retained <= 256,
            "128 resident pages retained {retained} weak candidates"
        );
    }

    #[test]
    fn buffer_pressure_reclaims_expired_cold_candidates_even_at_raw_floor() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let path = dir.path().join("expired-cold-metadata.heap");
        File::create(&path)
            .unwrap()
            .set_len(256 * PAGE_SIZE as u64)
            .unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool =
            BufferPool::new_with_options(16, log, DurabilityMode::Memory, Duration::ZERO, 1, 0);
        let file = Arc::new(BufferPool::heap_file_key(path, 1));
        for page_id in 0..256 {
            let pinned = pool
                .pin_page_with_intent(
                    PageKey {
                        file: file.clone(),
                        page_id,
                    },
                    AccessIntent::Normal,
                )
                .unwrap();
            pool.demote_to_scan_origin(pool.shard_index(&pinned.frame.key), &pinned.frame);
            pool.promote_scan_origin(&pinned.frame);
        }
        pool.reclaim_for_buffer_limit(usize::MAX, Duration::ZERO)
            .unwrap();
        let mut expired = 0;
        for shard in &pool.shards {
            expired += shard
                .cold_candidates
                .lock()
                .unwrap()
                .iter()
                .filter(|candidate| candidate.strong_count() == 0)
                .count();
        }
        assert_eq!(
            expired, 0,
            "pressure reclaim retained expired Frame control blocks"
        );
    }

    #[test]
    fn scheduled_bulk_writeback_is_bounded_and_retains_contiguous_batches() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let path = dir.path().join("scheduled.heap");
        File::create(&path).unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool =
            BufferPool::new_with_options(128, log, DurabilityMode::Strict, Duration::ZERO, 1, 0);
        pool.shutdown_flusher();
        // Exercise deterministic worker steps without a racing IO thread.
        pool.shutdown.store(false, Ordering::Release);
        let file = Arc::new(BufferPool::heap_file_key(path.clone(), 19));
        for page_id in 0..104 {
            pool.store_page_cold(
                PageKey {
                    file: Arc::clone(&file),
                    page_id,
                },
                vec![page_id as u8; PAGE_SIZE],
                0,
            )
            .unwrap();
        }
        pool.schedule_bulk_page_range(&file, 0, 31).unwrap();
        pool.schedule_bulk_page_range(&file, 32, 63).unwrap();
        // A queued run may end with fewer pages than its admission threshold.
        pool.schedule_bulk_page_range(&file, 64, 103).unwrap();
        assert_eq!(pool.snapshot().write_ios, 0);
        assert_eq!(
            pool.bulk_flush_ranges
                .lock()
                .unwrap()
                .iter()
                .flatten()
                .count(),
            1
        );
        for count in [32, 64, 96, 104] {
            assert!(pool.flush_queued_bulk_range().unwrap());
            assert_eq!(pool.snapshot().write_ios, count);
        }
        assert!(!pool.flush_queued_bulk_range().unwrap());
        assert_eq!(pool.snapshot().write_syscalls, 4);
        let bytes = std::fs::read(path).unwrap();
        for page_id in 0..104 {
            assert!(bytes[page_id * PAGE_SIZE..(page_id + 1) * PAGE_SIZE]
                .iter()
                .all(|byte| *byte == page_id as u8));
        }
    }

    #[test]
    fn bulk_writeback_below_pressure_remains_dirty_until_canonical_flush() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let path = dir.path().join("resident-bulk.heap");
        File::create(&path).unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool =
            BufferPool::new_with_options(1024, log, DurabilityMode::Strict, Duration::ZERO, 1, 0);
        pool.shutdown_flusher();
        pool.shutdown.store(false, Ordering::Release);
        let file = Arc::new(BufferPool::heap_file_key(path.clone(), 19));
        for page_id in 0..104 {
            pool.store_page_cold(
                PageKey {
                    file: Arc::clone(&file),
                    page_id,
                },
                vec![page_id as u8; PAGE_SIZE],
                0,
            )
            .unwrap();
        }
        pool.schedule_bulk_page_range(&file, 0, 103).unwrap();
        assert!(!pool.flush_queued_bulk_range().unwrap());
        assert_eq!(pool.snapshot().write_ios, 0);
        assert_eq!(pool.snapshot().dirty_pages, 104);
        pool.flush_all().unwrap();
        assert_eq!(pool.snapshot().dirty_pages, 0);
        let bytes = std::fs::read(path).unwrap();
        for page_id in 0..104 {
            assert!(bytes[page_id * PAGE_SIZE..(page_id + 1) * PAGE_SIZE]
                .iter()
                .all(|byte| *byte == page_id as u8));
        }
    }

    #[test]
    fn scheduled_bulk_writeback_capacity_falls_back_and_rejects_old_generation() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let path = dir.path().join("bounded-scheduled.heap");
        File::create(&path).unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool =
            BufferPool::new_with_options(64, log, DurabilityMode::Strict, Duration::ZERO, 1, 0);
        pool.shutdown_flusher();
        pool.shutdown.store(false, Ordering::Release);
        pool.capacity_pressure.store(true, Ordering::Release);
        let file = Arc::new(BufferPool::heap_file_key(path.clone(), 19));
        for n in 0..BUFFER_POOL_BULK_FLUSH_RANGES {
            let first = n as u64 * 64;
            pool.schedule_bulk_page_range(&file, first, first + 31)
                .unwrap();
        }
        assert_eq!(
            pool.bulk_flush_ranges
                .lock()
                .unwrap()
                .iter()
                .flatten()
                .count(),
            BUFFER_POOL_BULK_FLUSH_RANGES
        );
        pool.schedule_bulk_page_range(&file, 4096, 4127).unwrap();
        assert_eq!(
            pool.bulk_flush_ranges
                .lock()
                .unwrap()
                .iter()
                .flatten()
                .count(),
            BUFFER_POOL_BULK_FLUSH_RANGES
        );
        pool.discard_file(&file).unwrap();
        // Old scheduling hints cannot write a replacement file generation.
        for _ in 0..BUFFER_POOL_BULK_FLUSH_RANGES {
            pool.flush_queued_bulk_range().unwrap();
        }
        assert_eq!(pool.snapshot().write_ios, 0);
    }

    #[test]
    fn bulk_flush_coalesces_new_pages_then_recycles_them_cold() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bulk-cold.heap");
        File::create(&path).unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool =
            BufferPool::new_with_options(32, log, DurabilityMode::Strict, Duration::ZERO, 1, 0);
        // Measure the explicit bulk flush, not a race with the page cleaner
        // draining partial runs while the fixture is still being populated.
        pool.shutdown_flusher();
        let file_key = Arc::new(BufferPool::heap_file_key(path, 9));
        let key = |page_id| PageKey {
            file: Arc::clone(&file_key),
            page_id,
        };

        for page_id in 0..32 {
            pool.store_page_cold(key(page_id), vec![page_id as u8; PAGE_SIZE], 0)
                .unwrap();
        }
        let first = pool.lookup_frame(&key(0)).unwrap().unwrap();
        assert!(first.scan_origin.load(Ordering::Acquire));
        drop(first);

        pool.flush_bulk_page_range(&file_key, 0, 31).unwrap();
        let after_flush = pool.snapshot();
        assert_eq!(after_flush.write_ios, 32);
        assert_eq!(after_flush.write_syscalls, 1);

        pool.store_page_cold(key(32), vec![32; PAGE_SIZE], 0)
            .unwrap();
        assert!(pool.lookup_frame(&key(32)).unwrap().is_some());
        assert_eq!(pool.used_pages.load(Ordering::Relaxed), 32);
        assert_eq!(pool.snapshot().cold_recycle_hits, 1);
    }

    #[test]
    fn oldest_selection_advances_recovery_floor_then_restores_io_order() {
        let file = Arc::new(BufferPool::heap_file_key(PathBuf::from("oldest.heap"), 7));
        let frame = |page_id, rec_lsn| {
            Arc::new(Frame::new_dirty(
                PageKey {
                    file: Arc::clone(&file),
                    page_id,
                },
                vec![0; PAGE_SIZE],
                rec_lsn,
                0,
                controlled_raw_page_charge().unwrap(),
            ))
        };
        let selected = oldest_frames_in_io_order(vec![frame(1, 30), frame(9, 10), frame(2, 20)], 2);
        assert_eq!(
            selected.iter().map(|f| f.key.page_id).collect::<Vec<_>>(),
            vec![2, 9],
            "choose rec_lsn 10/20, then sort chosen pages for IO"
        );
        assert_eq!(
            selected
                .iter()
                .map(|f| f.first_dirty_lsn.load(Ordering::Relaxed))
                .collect::<std::collections::BTreeSet<_>>(),
            [10, 20].into_iter().collect()
        );
    }

    #[test]
    fn concurrent_raw_page_updates_survive_pressure_writeback() {
        const PAGES: usize = 256;
        const WORKERS: usize = 16;
        const ROUNDS: usize = 1024;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("counters.lob");
        fs::write(&path, vec![0; PAGES * PAGE_SIZE]).unwrap();
        let pool = BufferPool::new_with_options(
            32,
            Arc::new(parking_lot::Mutex::new(
                LogManager::open(dir.path()).unwrap(),
            )),
            DurabilityMode::Strict,
            Duration::from_secs(10),
            4,
            0,
        );
        let file = Arc::new(BufferPool::overflow_file_key(path, 7));
        let expected = (0..PAGES).map(|_| AtomicU64::new(0)).collect::<Vec<_>>();
        let barrier = std::sync::Barrier::new(WORKERS);
        std::thread::scope(|scope| {
            for worker in 0..WORKERS {
                let pool = pool.clone();
                let file = file.clone();
                let expected = &expected;
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    let mut random = worker as u64 + 1;
                    for _ in 0..ROUNDS {
                        random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                        let page = ((random >> 32) as usize) % PAGES;
                        pool.with_raw_page_mut(
                            PageKey {
                                file: file.clone(),
                                page_id: page as u64,
                            },
                            0,
                            |bytes| {
                                let n = u64::from_le_bytes(bytes[128..136].try_into().unwrap());
                                bytes[128..136].copy_from_slice(&(n + 1).to_le_bytes());
                                Ok(())
                            },
                        )
                        .unwrap();
                        expected[page].fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
        });
        pool.flush_all().unwrap();
        for (page, expected) in expected.iter().enumerate() {
            let n = pool
                .with_raw_page(
                    PageKey {
                        file: file.clone(),
                        page_id: page as u64,
                    },
                    |bytes| Ok(u64::from_le_bytes(bytes[128..136].try_into().unwrap())),
                )
                .unwrap();
            assert_eq!(n, expected.load(Ordering::Relaxed), "page={page}");
        }
    }

    #[test]
    fn separate_read_write_pins_preserve_formatted_heap_counters() {
        const PAGES: usize = 256;
        const WORKERS: usize = 16;
        const ROUNDS: usize = 1024;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("counters.lob");
        let mut template = Page::new();
        template.insert(&[0u8; 8]).unwrap();
        let image = template.encode();
        fs::write(&path, image.repeat(PAGES)).unwrap();
        let pool = BufferPool::new_with_options(
            32,
            Arc::new(parking_lot::Mutex::new(
                LogManager::open(dir.path()).unwrap(),
            )),
            DurabilityMode::Strict,
            Duration::from_secs(10),
            4,
            0,
        );
        let file = Arc::new(BufferPool::heap_file_key(path, 7));
        let expected = (0..PAGES).map(|_| AtomicU64::new(0)).collect::<Vec<_>>();
        let locks = (0..PAGES).map(|_| Mutex::new(())).collect::<Vec<_>>();
        let barrier = std::sync::Barrier::new(WORKERS);
        std::thread::scope(|scope| {
            for worker in 0..WORKERS {
                let pool = pool.clone();
                let file = file.clone();
                let expected = &expected;
                let locks = &locks;
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    let mut random = worker as u64 + 1;
                    for _ in 0..ROUNDS {
                        random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                        let page = ((random >> 32) as usize) % PAGES;
                        let _row_lock = locks[page].lock().unwrap();
                        let n = pool
                            .with_raw_page(
                                PageKey {
                                    file: file.clone(),
                                    page_id: page as u64,
                                },
                                |bytes| {
                                    Ok(u64::from_le_bytes(
                                        bytes[PAGE_SIZE - 8..PAGE_SIZE].try_into().unwrap(),
                                    ))
                                },
                            )
                            .unwrap();
                        pool.with_raw_page_mut(
                            PageKey {
                                file: file.clone(),
                                page_id: page as u64,
                            },
                            0,
                            |bytes| {
                                bytes[PAGE_SIZE - 8..PAGE_SIZE]
                                    .copy_from_slice(&(n + 1).to_le_bytes());
                                Ok(())
                            },
                        )
                        .unwrap();
                        expected[page].fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
        });
        pool.flush_all().unwrap();
        for (page, expected) in expected.iter().enumerate() {
            let n = pool
                .with_raw_page(
                    PageKey {
                        file: file.clone(),
                        page_id: page as u64,
                    },
                    |bytes| {
                        Ok(u64::from_le_bytes(
                            bytes[PAGE_SIZE - 8..PAGE_SIZE].try_into().unwrap(),
                        ))
                    },
                )
                .unwrap();
            assert_eq!(n, expected.load(Ordering::Relaxed), "page={page}");
        }
    }

    #[test]
    fn flush_rechecks_deferred_pin_under_the_image_latch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("deferred-snapshot.lob");
        fs::write(&path, vec![0; PAGE_SIZE]).unwrap();
        let pool = BufferPool::new_with_options(
            2,
            Arc::new(parking_lot::Mutex::new(
                LogManager::open(dir.path()).unwrap(),
            )),
            DurabilityMode::Memory,
            Duration::ZERO,
            1,
            0,
        );
        let key = PageKey {
            file: Arc::new(BufferPool::overflow_file_key(path.clone(), 7)),
            page_id: 0,
        };
        pool.store_page(key.clone(), vec![1; PAGE_SIZE], 0).unwrap();
        let frame = pool.lookup_frame(&key).unwrap().unwrap();
        let mut bytes = frame.bytes.write().unwrap();
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| pool.flush_frame(&frame));
            let deadline = Instant::now() + Duration::from_secs(2);
            while frame.flush_lock.try_lock().is_ok() {
                assert!(
                    Instant::now() < deadline,
                    "flusher did not reach its image latch"
                );
                std::thread::yield_now();
            }
            assert!(pool.add_deferred_pin(&key));
            bytes[128] = 42;
            frame.mark_dirty(0);
            drop(bytes);
            assert!(!worker.join().unwrap().unwrap());
        });
        assert!(frame.dirty.load(Ordering::Acquire));
        assert_eq!(
            fs::read(&path).unwrap()[128],
            0,
            "unlogged bytes reached disk"
        );
        pool.finalize_deferred(&key, 0);
        assert!(pool.flush_frame(&frame).unwrap());
        assert_eq!(fs::read(path).unwrap()[128], 42);
    }

    #[test]
    fn eviction_rechecks_deferred_wal_pin_after_victim_selection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("deferred-victim.lob");
        fs::write(&path, vec![0; PAGE_SIZE]).unwrap();
        let pool = BufferPool::new_with_options(
            2,
            Arc::new(parking_lot::Mutex::new(
                LogManager::open(dir.path()).unwrap(),
            )),
            DurabilityMode::Memory,
            Duration::ZERO,
            1,
            0,
        );
        let key = PageKey {
            file: Arc::new(BufferPool::overflow_file_key(path, 7)),
            page_id: 0,
        };
        drop(pool.pin_page(key.clone()).unwrap());
        let frame = pool.lookup_frame(&key).unwrap().unwrap();
        // The eviction selector observed a clean, unpinned page. Before it
        // claims ownership, a writer publishes a deferred-WAL mutation.
        let victim = (pool.shard_index(&key), key.clone(), frame.clone());
        assert!(pool.add_deferred_pin(&key));
        pool.with_raw_page_mut(key.clone(), 0, |bytes| {
            bytes[128] = 42;
            Ok(())
        })
        .unwrap();
        assert!(
            !pool.consume_victim(victim, false).unwrap(),
            "deferred page must remain resident until WAL publication"
        );
        assert!(Arc::ptr_eq(
            &pool.lookup_frame(&key).unwrap().unwrap(),
            &frame
        ));
        assert_eq!(frame.pin_count.load(Ordering::Acquire), 0);
        pool.finalize_deferred(&key, 0);
        assert!(pool
            .consume_victim((pool.shard_index(&key), key.clone(), frame), false)
            .unwrap());
        assert_eq!(pool.read_raw_page(key).unwrap()[128], 42);
    }

    #[test]
    fn deferred_wal_pin_cannot_attach_to_an_eviction_owned_frame() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("evicting-pin.lob");
        fs::write(&path, vec![0; PAGE_SIZE]).unwrap();
        let pool = BufferPool::new_with_options(
            2,
            Arc::new(parking_lot::Mutex::new(
                LogManager::open(dir.path()).unwrap(),
            )),
            DurabilityMode::Memory,
            Duration::ZERO,
            1,
            0,
        );
        let key = PageKey {
            file: Arc::new(BufferPool::overflow_file_key(path, 7)),
            page_id: 0,
        };
        drop(pool.pin_page(key.clone()).unwrap());
        let frame = pool.lookup_frame(&key).unwrap().unwrap();
        frame
            .pin_count
            .store(BufferPool::EVICTING_PIN, Ordering::Release);
        assert!(!pool.add_deferred_pin(&key));
        assert_eq!(frame.deferred_pins.load(Ordering::Acquire), 0);
        frame.pin_count.store(0, Ordering::Release);
    }

    #[test]
    fn pinned_cold_candidate_does_not_starve_other_shards() {
        let dir = tempfile::tempdir().unwrap();
        let pool = BufferPool::new_with_options(
            8,
            Arc::new(parking_lot::Mutex::new(
                LogManager::open(dir.path()).unwrap(),
            )),
            DurabilityMode::Memory,
            Duration::ZERO,
            1,
            0,
        );
        let file = Arc::new(BufferPool::heap_file_key(dir.path().join("cold.heap"), 1));
        let frame = |page_id, pins| {
            let frame = Arc::new(Frame::new_loading(
                PageKey {
                    file: file.clone(),
                    page_id,
                },
                0,
                AccessIntent::Scan,
                RawPageLease {
                    bytes: vec![0; PAGE_SIZE],
                    charge: controlled_raw_page_charge().unwrap(),
                },
            ));
            frame
                .state
                .store(FrameIoState::Ready as u8, Ordering::Release);
            frame.pin_count.store(pins, Ordering::Release);
            frame
        };
        let pinned = frame(0, 1);
        let free = frame(1, 0);
        let first = pool.shard_index(&pinned.key);
        let second = pool.shard_index(&free.key);
        assert_ne!(first, second);
        pool.shards[first]
            .cold_candidates
            .lock()
            .unwrap()
            .push_back(Arc::downgrade(&pinned));
        pool.shards[second]
            .cold_candidates
            .lock()
            .unwrap()
            .push_back(Arc::downgrade(&free));
        let (_, key, _) = pool
            .find_cold_candidate(true, first)
            .unwrap()
            .expect("a pinned candidate must not consume the whole search budget repeatedly");
        assert_eq!(key, free.key);
        assert_eq!(pool.shards[first].cold_candidates.lock().unwrap().len(), 1);
    }

    #[test]
    fn pressure_sampling_is_bounded_rotates_and_preserves_cache_heat() {
        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool =
            BufferPool::new_with_options(128, log, DurabilityMode::Memory, Duration::ZERO, 1, 0);
        let file = Arc::new(BufferPool::heap_file_key(dir.path().join("sample.heap"), 7));
        let key = |page_id| PageKey {
            file: file.clone(),
            page_id,
        };
        for page_id in 0..64 {
            pool.store_page(key(page_id), vec![0; PAGE_SIZE], 0)
                .unwrap();
        }
        let shard_idx = pool.shard_index(&key(0));
        let count = pool.shards[shard_idx].clock.lock().unwrap().len();
        assert!(count > 3);
        assert!(pool
            .collect_dirty_clock_window(shard_idx, 0, false)
            .unwrap()
            .is_empty());
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..count.div_ceil(3) {
            let sampled = pool
                .collect_dirty_clock_window(shard_idx, 3, false)
                .unwrap();
            assert!(sampled.len() <= 3);
            for frame in sampled {
                assert!(frame.ref_bit.load(Ordering::Relaxed));
                assert_eq!(
                    frame.usage_count.load(Ordering::Relaxed),
                    initial_usage_count(AccessIntent::Normal)
                );
                seen.insert(frame.key.page_id);
            }
        }
        assert_eq!(seen.len(), count);

        let frame = pool.lookup_frame(&key(0)).unwrap().unwrap();
        frame.deferred_pins.store(1, Ordering::Release);
        assert!(pool
            .collect_dirty_clock_window(shard_idx, 128, false)
            .unwrap()
            .iter()
            .all(|candidate| candidate.key != frame.key));
        frame.deferred_pins.store(0, Ordering::Release);
        frame.pin_count.store(1, Ordering::Release);
        assert!(pool
            .collect_dirty_clock_window(shard_idx, 128, false)
            .unwrap()
            .iter()
            .all(|candidate| candidate.key != frame.key));
        frame.pin_count.store(0, Ordering::Release);
        pool.flush_frame(&frame).unwrap();
        assert!(pool
            .collect_dirty_clock_window(shard_idx, 128, false)
            .unwrap()
            .iter()
            .all(|candidate| candidate.key != frame.key));
        // A stale CLOCK key left by an eviction must not produce a candidate.
        pool.shards[shard_idx]
            .frames
            .write()
            .unwrap()
            .remove(&frame.key);
        assert!(pool
            .collect_dirty_clock_window(shard_idx, 128, false)
            .unwrap()
            .iter()
            .all(|candidate| candidate.key != frame.key));
    }

    #[test]
    fn pressure_selection_prefers_clock_cold_pages_then_restores_io_order() {
        let file = Arc::new(BufferPool::index_file_key(PathBuf::from("pressure.idx")));
        let frame = |page_id, rec_lsn, referenced, usage| {
            let frame = Arc::new(Frame::new_dirty(
                PageKey {
                    file: Arc::clone(&file),
                    page_id,
                },
                vec![0; PAGE_SIZE],
                rec_lsn,
                0,
                controlled_raw_page_charge().unwrap(),
            ));
            frame.ref_bit.store(referenced, Ordering::Relaxed);
            frame.usage_count.store(usage, Ordering::Relaxed);
            frame
        };
        let selected = cold_frames_in_io_order(
            vec![
                frame(9, 10, true, 20),
                frame(3, 30, false, 0),
                frame(1, 20, false, 1),
            ],
            2,
        );
        assert_eq!(
            selected.iter().map(|f| f.key.page_id).collect::<Vec<_>>(),
            vec![1, 3],
            "choose the two CLOCK-cold pages, then sort them for IO"
        );
    }

    #[test]
    fn dirty_pressure_grace_defers_foreground_victim() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dirty-grace.heap");
        File::create(&path).unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        // Memory durability deliberately starts no background workers, making
        // the policy decision deterministic: a clean-only request over an
        // all-dirty pool must signal pressure without evicting the page.
        let pool =
            BufferPool::new_with_options(1, log, DurabilityMode::Memory, Duration::ZERO, 1, 0);
        let key = PageKey {
            file: Arc::new(BufferPool::heap_file_key(path, 1)),
            page_id: 0,
        };
        pool.store_page(key.clone(), vec![7; PAGE_SIZE], 1).unwrap();

        assert!(!pool.evict_one_with_policy(false).unwrap());
        assert!(pool.capacity_pressure.load(Ordering::Acquire));
        assert!(pool.lookup_frame(&key).unwrap().is_some());
        assert_eq!(pool.snapshot().evict_sync_dirty_flushes, 0);
    }

    #[test]
    fn physical_cache_reclaim_releases_clean_bytes_and_preserves_pinned_dirty_pages() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let path = dir.path().join("physical-headroom.heap");
        File::create(&path)
            .unwrap()
            .set_len(4 * PAGE_SIZE as u64)
            .unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool =
            BufferPool::new_with_options(4, log, DurabilityMode::Memory, Duration::ZERO, 1, 0);
        let file = Arc::new(BufferPool::heap_file_key(path, 1));
        let key = |page_id| PageKey {
            file: file.clone(),
            page_id,
        };
        let pinned = pool
            .pin_page_with_intent(key(0), AccessIntent::Normal)
            .unwrap();
        for page in 1..3 {
            let pin = pool
                .pin_page_with_intent(key(page), AccessIntent::Normal)
                .unwrap();
            pin.frame.ref_bit.store(false, Ordering::Relaxed);
            pin.frame.usage_count.store(0, Ordering::Relaxed);
        }
        pool.store_page(key(3), vec![7; PAGE_SIZE], 1).unwrap();
        pool.release_recycled_raw_pages();
        let before = crate::engine::executor::mem_budget::controlled_memory_snapshot();
        pool.reclaim_clean_for_memory_target(before.0 - PAGE_SIZE, Duration::from_secs(1))
            .unwrap();
        let after = crate::engine::executor::mem_budget::controlled_memory_snapshot();
        assert!(after.2[0] <= before.2[0] - PAGE_SIZE);
        assert!(pool.lookup_frame(&key(0)).unwrap().is_some());
        assert!(pinned.frame.pin_count.load(Ordering::Acquire) > 0);
        assert!(pool
            .lookup_frame(&key(3))
            .unwrap()
            .unwrap()
            .dirty
            .load(Ordering::Acquire));
        assert_eq!(pool.snapshot().evict_sync_dirty_flushes, 0);
        let reclaimed = (1..3)
            .find(|page| pool.lookup_frame(&key(*page)).unwrap().is_none())
            .unwrap();
        let reloaded = pool
            .pin_page_with_intent(key(reclaimed), AccessIntent::Normal)
            .unwrap();
        assert!(reloaded
            .frame
            .bytes
            .read()
            .unwrap()
            .iter()
            .all(|byte| *byte == 0));
    }

    #[test]
    fn clean_pressure_eviction_rechecks_a_victim_dirtied_after_selection() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let path = dir.path().join("physical-headroom-race.heap");
        File::create(&path)
            .unwrap()
            .set_len(PAGE_SIZE as u64)
            .unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool =
            BufferPool::new_with_options(1, log, DurabilityMode::Memory, Duration::ZERO, 1, 0);
        let key = PageKey {
            file: Arc::new(BufferPool::heap_file_key(path, 1)),
            page_id: 0,
        };
        let pinned = pool
            .pin_page_with_intent(key.clone(), AccessIntent::Normal)
            .unwrap();
        let frame = pinned.frame.clone();
        drop(pinned);
        // Selection happened while clean; a writer dirtied it before the
        // evictor won the exclusive pin. No synchronous write is permitted.
        pool.store_page(key.clone(), vec![7; PAGE_SIZE], 1).unwrap();
        assert!(!pool
            .consume_victim_with_policy(
                (pool.shard_index(&key), key.clone(), frame.clone()),
                false,
                false
            )
            .unwrap());
        assert_eq!(frame.pin_count.load(Ordering::Acquire), 0);
        assert!(pool.lookup_frame(&key).unwrap().is_some());
        assert_eq!(pool.snapshot().evict_sync_dirty_flushes, 0);
    }

    #[test]
    fn positioned_batch_read_preserves_page_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("batch.heap");
        let mut file = File::create(&path).unwrap();
        for byte in [11u8, 22, 33, 44] {
            file.write_all(&vec![byte; PAGE_SIZE]).unwrap();
        }
        drop(file);
        let file = File::open(path).unwrap();
        let mut pages = vec![vec![0u8; PAGE_SIZE]; 3];
        let syscalls = {
            let mut slices: Vec<&mut [u8]> = pages.iter_mut().map(Vec::as_mut_slice).collect();
            read_pages_batch_at(&file, &mut slices, PAGE_SIZE as u64).unwrap()
        };
        assert_eq!(pages[0][0], 22);
        assert_eq!(pages[1][PAGE_SIZE - 1], 33);
        assert_eq!(pages[2][0], 44);
        #[cfg(unix)]
        assert_eq!(syscalls, 1);
        #[cfg(not(unix))]
        assert_eq!(syscalls, 3);
    }

    #[test]
    fn positioned_batch_read_rejects_short_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("short.heap");
        let mut file = File::create(&path).unwrap();
        file.write_all(&vec![7u8; PAGE_SIZE]).unwrap();
        drop(file);
        let file = File::open(path).unwrap();
        let mut pages = vec![vec![0u8; PAGE_SIZE]; 2];
        let error = {
            let mut slices: Vec<&mut [u8]> = pages.iter_mut().map(Vec::as_mut_slice).collect();
            read_pages_batch_at(&file, &mut slices, 0).unwrap_err()
        };
        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn cached_heap_validation_error_releases_pin() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("invalid.heap");
        // Pin accepts raw bytes; the heap-view validator must reject this
        // malformed page without leaking a pin and preventing eviction.
        std::fs::write(&path, vec![1u8; PAGE_SIZE]).unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool = BufferPool::new_with_options(
            1,
            log,
            DurabilityMode::Memory,
            Duration::from_secs(1),
            1,
            0,
        );
        let key = PageKey {
            file: Arc::new(BufferPool::heap_file_key(path, 1)),
            page_id: 0,
        };
        let pinned = pool.pin_page(key.clone()).unwrap();
        let frame = Arc::clone(&pinned.frame);
        drop(pinned);
        assert_eq!(frame.pin_count.load(Ordering::Acquire), 0);
        assert!(pool.with_cached_heap_view(&key, None, |_| Ok(())).is_err());
        assert_eq!(frame.pin_count.load(Ordering::Acquire), 0);
    }

    #[test]
    fn cached_heap_visitor_error_and_unwind_release_pin() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sparse.heap");
        std::fs::write(&path, vec![0u8; PAGE_SIZE]).unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool = BufferPool::new_with_options(
            1,
            log,
            DurabilityMode::Memory,
            Duration::from_secs(1),
            1,
            0,
        );
        let key = PageKey {
            file: Arc::new(BufferPool::heap_file_key(path, 1)),
            page_id: 0,
        };
        let pinned = pool.pin_page(key.clone()).unwrap();
        let frame = Arc::clone(&pinned.frame);
        drop(pinned);
        let failed: Result<Option<()>, QueryError> = pool.with_cached_heap_view(&key, None, |_| {
            Err(error(crate::error::catalog::ER_GET_ERRNO, "visitor error"))
        });
        assert!(failed.is_err());
        assert_eq!(frame.pin_count.load(Ordering::Acquire), 0);
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _: Result<Option<()>, QueryError> =
                pool.with_cached_heap_view(&key, None, |_| panic!("visitor panic"));
        }));
        assert!(panicked.is_err());
        assert_eq!(frame.pin_count.load(Ordering::Acquire), 0);
    }

    #[test]
    fn weak_frame_hint_reloads_discarded_file_without_retaining_page_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hint.heap");
        std::fs::write(&path, vec![1u8; PAGE_SIZE]).unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool = BufferPool::new_with_options(
            2,
            log,
            DurabilityMode::Memory,
            Duration::from_secs(1),
            1,
            0,
        );
        let file = Arc::new(BufferPool::heap_file_key(path.clone(), 1));
        let key = PageKey {
            file: Arc::clone(&file),
            page_id: 0,
        };
        let hint = parking_lot::RwLock::new(std::sync::Weak::new());
        let first = pool.pin_page_with_hint(key.clone(), &hint).unwrap();
        let old_frame = Arc::clone(&first.frame);
        drop(first);
        let again = pool.pin_page_with_hint(key.clone(), &hint).unwrap();
        assert!(Arc::ptr_eq(&old_frame, &again.frame));
        drop(again);
        pool.discard_file(&file).unwrap();
        assert!(!old_frame.is_ready());
        old_frame.publish_io_state(FrameIoState::Ready);
        assert!(
            !old_frame.is_ready(),
            "late I/O cannot resurrect discarded frames"
        );
        std::fs::write(&path, vec![9u8; PAGE_SIZE]).unwrap();
        let reloaded = pool.pin_page_with_hint(key, &hint).unwrap();
        assert!(!Arc::ptr_eq(&old_frame, &reloaded.frame));
        assert_eq!(reloaded.frame.bytes.read().unwrap()[0], 9);
        drop(old_frame);
        drop(reloaded);
        pool.discard_file(&file).unwrap();
        assert!(hint.read().upgrade().is_none());
    }

    #[test]
    fn borrowed_pin_keeps_residency_and_releases_on_unwind_without_pool_arc_traffic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("borrowed.heap");
        std::fs::write(&path, vec![7u8; PAGE_SIZE * 2]).unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool = BufferPool::new_with_options(
            1,
            log,
            DurabilityMode::Memory,
            Duration::from_millis(20),
            1,
            0,
        );
        let file = Arc::new(BufferPool::heap_file_key(path, 1));
        let key = PageKey {
            file: Arc::clone(&file),
            page_id: 0,
        };
        let owners = Arc::strong_count(&pool);
        let pinned = pool.pin_page_borrowed(key.clone()).unwrap();
        let frame = Arc::clone(&pinned.frame);
        assert_eq!(Arc::strong_count(&pool), owners);
        assert_eq!(frame.pin_count.load(Ordering::Acquire), 1);
        assert_eq!(pinned.frame.bytes.read().unwrap()[0], 7);
        // One pinned page fills this pool. A cold probe must not recycle it.
        assert!(pool
            .pin_page_borrowed(PageKey {
                file: Arc::clone(&file),
                page_id: 1
            })
            .is_err());
        assert_eq!(pinned.frame.bytes.read().unwrap()[0], 7);
        drop(pinned);
        assert_eq!(frame.pin_count.load(Ordering::Acquire), 0);
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = pool.pin_page_borrowed(key.clone()).unwrap();
            panic!("borrowed pin unwind");
        }));
        assert!(unwound.is_err());
        assert_eq!(frame.pin_count.load(Ordering::Acquire), 0);
        let replacement = pool
            .pin_page_borrowed(PageKey { file, page_id: 1 })
            .unwrap();
        assert_eq!(replacement.frame.bytes.read().unwrap()[0], 7);
        drop(replacement);
        assert_eq!(pool.snapshot().pinned_pages, 0);
    }

    #[test]
    fn eviction_reuses_governed_raw_page_allocation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("recycle.heap");
        let mut file = File::create(&path).unwrap();
        file.write_all(&vec![1u8; PAGE_SIZE]).unwrap();
        file.write_all(&vec![2u8; PAGE_SIZE]).unwrap();
        drop(file);

        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool = BufferPool::new_with_options(
            1,
            log,
            DurabilityMode::Memory,
            Duration::from_secs(1),
            1,
            0,
        );
        let file_key = Arc::new(BufferPool::heap_file_key(path, 1));
        let first = pool
            .pin_page(PageKey {
                file: Arc::clone(&file_key),
                page_id: 0,
            })
            .unwrap();
        let first_ptr = first.frame.bytes.read().unwrap().as_ptr();
        drop(first);

        let second = pool
            .pin_page(PageKey {
                file: file_key,
                page_id: 1,
            })
            .unwrap();
        let second_ptr = second.frame.bytes.read().unwrap().as_ptr();
        assert_eq!(first_ptr, second_ptr);
        assert_eq!(second.frame.bytes.read().unwrap()[0], 2);
        assert_eq!(pool.raw_page_allocations.load(Ordering::Relaxed), 1);
        assert!(pool.raw_page_reuse_hits.load(Ordering::Relaxed) >= 1);
    }

    #[test]
    fn raw_page_discard_span_never_reaches_neighbors_or_free_list_header() {
        for page_size in [4096, 16384, 65536] {
            for offset in [0, 1, 8, 16, page_size / 2, page_size - 1] {
                for len in [0, 1, PAGE_SIZE, page_size * 3 + 17] {
                    let address = page_size * 10 + offset;
                    let span = unused_raw_page_span(address, len, page_size);
                    if !span.is_empty() {
                        assert!(span.start >= std::mem::size_of::<usize>());
                        assert!(span.end <= len);
                        assert_eq!((address + span.start) % page_size, 0);
                        assert_eq!((address + span.end) % page_size, 0);
                    }
                }
            }
        }
        assert!(unused_raw_page_span(1, PAGE_SIZE, 0).is_empty());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn raw_page_discard_returns_only_owned_interior_pages() {
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        let mut bytes = vec![0xabu8; page_size * 4 + 17];
        let span = unused_raw_page_span(bytes.as_ptr() as usize, bytes.len(), page_size);
        assert!(!span.is_empty());
        discard_unused_raw_page_bytes(&mut bytes);
        assert!(bytes[..span.start].iter().all(|&byte| byte == 0xab));
        assert!(bytes[span.clone()].iter().all(|&byte| byte == 0));
        assert!(bytes[span.end..].iter().all(|&byte| byte == 0xab));
    }

    #[test]
    fn memory_target_reclaim_releases_recycled_raw_page_reservations() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("memory-target-reclaim.heap");
        let mut file = File::create(&path).unwrap();
        file.write_all(&vec![1u8; PAGE_SIZE]).unwrap();
        file.write_all(&vec![2u8; PAGE_SIZE]).unwrap();
        drop(file);

        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool =
            BufferPool::new_with_options(2, log, DurabilityMode::Strict, Duration::ZERO, 1, 0);
        let file_key = Arc::new(BufferPool::heap_file_key(path, 1));
        for page_id in 0..2 {
            let key = PageKey {
                file: Arc::clone(&file_key),
                page_id,
            };
            drop(pool.pin_page(key.clone()).unwrap());
            let frame = pool.lookup_frame(&key).unwrap().unwrap();
            frame.ref_bit.store(false, Ordering::Release);
            frame.usage_count.store(0, Ordering::Release);
        }

        let before = crate::engine::executor::mem_budget::controlled_memory_snapshot().0;
        let target = before.saturating_sub(PAGE_SIZE);
        pool.reclaim_for_memory_target(target, Duration::from_millis(100))
            .unwrap();

        assert!(
            crate::engine::executor::mem_budget::controlled_memory_snapshot().0 <= target,
            "memory-target reclaim must drop the evicted raw-page lease"
        );
        assert_eq!(
            pool.recycled_raw_page_count.load(Ordering::Relaxed),
            0,
            "a target reclaim must not retain the victim as optional recycle capacity"
        );
    }

    #[test]
    fn buffer_pool_hot_writeback_keeps_recovery_and_pressure_progress() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hot-writeback.heap");
        File::create(&path).unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool =
            BufferPool::new_with_options(100, log, DurabilityMode::Strict, Duration::ZERO, 1, 0);
        pool.shutdown_flusher();
        let file = Arc::new(BufferPool::heap_file_key(path.clone(), 1));
        for page_id in 0..50 {
            pool.store_page(
                PageKey {
                    file: Arc::clone(&file),
                    page_id,
                },
                vec![page_id as u8; PAGE_SIZE],
                1,
            )
            .unwrap();
        }
        // All pages are referenced and above the count watermark. Routine
        // cleaning must advance the oldest-LSN budget without rewriting the
        // rest of the hot working set just to reach a count target.
        pool.background_flush_once(0, 1).unwrap();
        assert_eq!(pool.snapshot().flushes, age_clean_pages_per_tick());
        assert_eq!(pool.snapshot().dirty_pages, 50 - age_clean_pages_per_tick());
        // A real capacity request may write hot pages, and checkpoint must
        // still persist every byte even when CLOCK has never aged the pages.
        let before = pool.snapshot().flushes;
        for _ in 0..pool.shards.len() {
            pool.maintain_free_headroom().unwrap();
        }
        assert!(pool.snapshot().flushes > before);
        pool.flush_all().unwrap();
        let bytes = fs::read(path).unwrap();
        for page_id in 0..50 {
            assert!(bytes[page_id * PAGE_SIZE..(page_id + 1) * PAGE_SIZE]
                .iter()
                .all(|byte| *byte == page_id as u8));
        }
    }

    #[test]
    fn contiguous_dirty_pages_flush_with_one_physical_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coalesced.heap");
        File::create(&path).unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool = BufferPool::new_with_options(
            8,
            log,
            DurabilityMode::Memory,
            Duration::from_secs(1),
            1,
            0,
        );
        let file_key = Arc::new(BufferPool::heap_file_key(path.clone(), 1));
        let mut frames = Vec::new();
        for page_id in 0..3 {
            let key = PageKey {
                file: Arc::clone(&file_key),
                page_id,
            };
            pool.store_page(
                key.clone(),
                vec![(page_id + 1) as u8; PAGE_SIZE],
                page_id + 1,
            )
            .unwrap();
            let frame = pool.lookup_frame(&key).unwrap().unwrap();
            frame.ref_bit.store(false, Ordering::Release);
            frame.usage_count.store(0, Ordering::Release);
            frames.push(frame);
        }

        let expanded = pool
            .expand_dirty_neighbors_for_flush(vec![Arc::clone(&frames[1])], 3)
            .unwrap();
        assert_eq!(
            expanded
                .iter()
                .map(|frame| frame.key.page_id)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(pool.flush_frame_batch(&expanded).unwrap(), 3);
        assert_eq!(pool.write_ios.load(Ordering::Relaxed), 3);
        assert_eq!(pool.write_syscalls.load(Ordering::Relaxed), 1);
        assert_eq!(pool.dirty_pages.load(Ordering::Relaxed), 0);

        let bytes = std::fs::read(path).unwrap();
        assert_eq!(bytes.len(), 3 * PAGE_SIZE);
        for page_id in 0..3 {
            assert!(bytes[page_id * PAGE_SIZE..(page_id + 1) * PAGE_SIZE]
                .iter()
                .all(|byte| *byte == (page_id + 1) as u8));
        }
    }

    #[test]
    fn audit_discard_waits_for_inflight_flush_and_remains_terminal() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let path = dir.path().join("flush.heap");
        std::fs::write(&path, vec![7u8; PAGE_SIZE]).unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool = BufferPool::new_with_options(
            2,
            log,
            DurabilityMode::Memory,
            Duration::from_secs(1),
            1,
            0,
        );
        let key = PageKey {
            file: Arc::new(BufferPool::heap_file_key(path, 1)),
            page_id: 0,
        };
        let pinned = pool.pin_page(key.clone()).unwrap();
        let frame = pinned.frame.clone();
        let flush = frame.flush_lock.lock().unwrap();
        frame.publish_io_state(FrameIoState::Flushing);
        let (done, recv) = std::sync::mpsc::channel();
        let worker_pool = pool.clone();
        let worker_key = key.clone();
        let worker = std::thread::spawn(move || {
            worker_pool.discard_page(&worker_key).unwrap();
            done.send(()).unwrap();
        });
        let deadline = Instant::now() + Duration::from_secs(2);
        while frame.state.load(Ordering::Acquire) != FrameIoState::Discarded as u8 {
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        }
        assert!(recv.try_recv().is_err());
        frame.publish_io_state(FrameIoState::Ready);
        assert_eq!(
            frame.state.load(Ordering::Acquire),
            FrameIoState::Discarded as u8
        );
        assert_eq!(pinned.bytes().unwrap().len(), PAGE_SIZE);
        drop(flush);
        recv.recv_timeout(Duration::from_secs(2)).unwrap();
        worker.join().unwrap();
        assert!(pool.read_page_bytes(&key).unwrap().is_none());
    }

    #[test]
    fn discard_does_not_recycle_a_pinned_frame_and_invalidates_load_publication() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("invalidate.heap");
        let mut file = File::create(&path).unwrap();
        file.write_all(&vec![7u8; PAGE_SIZE]).unwrap();
        drop(file);

        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool = BufferPool::new_with_options(
            1,
            log,
            DurabilityMode::Memory,
            Duration::from_secs(1),
            1,
            0,
        );
        let key = PageKey {
            file: Arc::new(BufferPool::heap_file_key(path, 1)),
            page_id: 0,
        };
        let pinned = pool.pin_page(key.clone()).unwrap();
        let frame = Arc::clone(&pinned.frame);
        let generation = pool.file_generation_cell(&key.file).unwrap();
        let loaded_generation = generation.load(Ordering::Acquire);
        let shard_idx = pool.shard_index(&key);
        assert!(pool
            .loaded_frame_is_current(shard_idx, &key, &frame, &generation, loaded_generation)
            .unwrap());

        pool.discard_page(&key).unwrap();

        assert!(!pool
            .loaded_frame_is_current(shard_idx, &key, &frame, &generation, loaded_generation)
            .unwrap());
        assert_eq!(pinned.bytes().unwrap().len(), PAGE_SIZE);
        assert!(frame.raw_memory_charge.lock().unwrap().is_some());
        assert_eq!(pool.recycled_raw_page_count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn adaptive_prefetch_uses_useful_waste_drop_thresholds_and_cap() {
        assert_eq!(
            adapted_prefetch_window(16, 1_024, 512, 102, 100, 1, 128),
            20
        );
        assert_eq!(
            adapted_prefetch_window(16, 1_024, 512, 102, 100, 0, 128),
            16
        );
        assert_eq!(adapted_prefetch_window(16, 1_024, 205, 0, 0, 0, 128), 16);
        assert_eq!(adapted_prefetch_window(16, 1_024, 204, 0, 0, 0, 128), 8);
        assert_eq!(adapted_prefetch_window(16, 1_024, 800, 308, 0, 0, 128), 8);
        assert_eq!(adapted_prefetch_window(16, 1_024, 800, 0, 342, 0, 128), 8);
        assert_eq!(adapted_prefetch_window(120, 1_024, 800, 0, 0, 1, 64), 64);
        assert_eq!(adapted_prefetch_window(1, 1_024, 0, 0, 0, 0, 128), 1);
    }

    #[test]
    fn cancelling_prefetch_also_releases_duplicate_admission() {
        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool =
            BufferPool::new_with_options(8, log, DurabilityMode::Memory, Duration::ZERO, 1, 4);
        let key = PageKey {
            file: Arc::new(BufferPool::heap_file_key(
                dir.path().join("prefetch.heap"),
                1,
            )),
            page_id: 3,
        };
        let shard_idx = pool.shard_index(&key);
        pool.enqueue_prefetch_batch(vec![key.clone()], 4);
        assert!(pool.prefetch_queued_or_inflight[shard_idx]
            .lock()
            .unwrap()
            .contains(&key));

        pool.cancel_all_queued_prefetch();

        assert!(pool.prefetch_queue.lock().unwrap().is_empty());
        assert!(!pool.prefetch_queued_or_inflight[shard_idx]
            .lock()
            .unwrap()
            .contains(&key));
        pool.enqueue_prefetch_batch(vec![key.clone()], 4);
        assert_eq!(pool.prefetch_queue.lock().unwrap().len(), 1);
    }

    #[test]
    fn file_cache_second_chance_converges_and_preserves_strict_dirty_handles() {
        let dir = tempfile::tempdir().unwrap();
        let mut files = BufferFileCacheMap::default();
        let mut keys = Vec::new();
        for id in 0..3 {
            let key =
                BufferPool::heap_file_key(dir.path().join(format!("file-cache-{id}.heap")), id);
            let file = File::create(&key.path).unwrap();
            let cached = Arc::new(CachedFile::new(file, 0));
            cached.take_needs_sync();
            files.insert(key.clone(), cached);
            keys.push(key);
        }

        BufferPool::evict_idle_file_handles(&mut files, &keys[2], 2, false);
        assert_eq!(files.len(), 2);
        assert!(files.contains_key(&keys[2]));

        for file in files.values() {
            file.mark_needs_sync();
        }
        BufferPool::evict_idle_file_handles(&mut files, &keys[2], 1, true);
        assert_eq!(
            files.len(),
            2,
            "strict mode must retain handles with an outstanding fsync"
        );
    }

    #[test]
    fn background_io_failures_are_observable() {
        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool =
            BufferPool::new_with_options(128, log, DurabilityMode::Memory, Duration::ZERO, 1, 0);
        let before = pool.snapshot().background_io_errors;

        pool.record_background_io_error(
            "test injection",
            &error(
                crate::error::catalog::ER_GET_ERRNO,
                "injected background I/O error",
            ),
        );

        assert_eq!(pool.snapshot().background_io_errors, before + 1);
    }
}

#[cfg(test)]
fn controlled_raw_page_charge(
) -> Result<crate::engine::executor::mem_budget::ControlledMemoryCharge, QueryError> {
    crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::BufferRaw,
        PAGE_SIZE,
    )
}

fn try_controlled_raw_page_charge(
    bytes: usize,
) -> Option<crate::engine::executor::mem_budget::ControlledMemoryCharge> {
    crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::BufferRaw,
        bytes,
    )
}

impl Frame {
    // CLOCK sweeps decrement one point at a time. A three-digit cap lets a
    // once-hot page survive hundreds of replacement passes after its workload
    // has moved on, turning every eviction into a long scan when the working
    // set exceeds the pool. Six, in addition to the independent ref-bit
    // second chance, still distinguishes repeatedly reused pages from
    // scan/probe admissions while bounding stale heat and eviction CPU.
    const MAX_NORMAL_USAGE_COUNT: u8 = 6;

    fn new_loading(
        key: PageKey,
        generation: u64,
        intent: AccessIntent,
        raw_page: RawPageLease,
    ) -> Self {
        Self {
            key,
            raw_memory_charge: Mutex::new(Some(raw_page.charge)),
            bytes: RwLock::new(raw_page.bytes),
            small_scan_accesses: AtomicU8::new(u8::from(intent == AccessIntent::SmallScan)),
            pin_count: AtomicUsize::new(1),
            state: AtomicU8::new(FrameIoState::Loading as u8),
            io_wait_lock: Mutex::new(()),
            io_wait_cv: Condvar::new(),
            io_waiters: AtomicUsize::new(0),
            generation: AtomicU64::new(generation),
            dirty: AtomicBool::new(false),
            dirty_lsn: AtomicU64::new(0),
            first_dirty_lsn: AtomicU64::new(0),
            deferred_pins: AtomicU32::new(0),
            dirty_epoch: AtomicU64::new(0),
            checksum_dirty: AtomicBool::new(false),
            packed_leaf: parking_lot::Mutex::new(None),
            packed_leaf_state: AtomicU8::new(0),
            prefetch_loaded_at_us: AtomicU64::new(0),
            index_header_validated_epoch: AtomicU64::new(u64::MAX),
            heap_header_present: AtomicBool::new(false),
            heap_header_validated_epoch: AtomicU64::new(u64::MAX),
            ref_bit: AtomicBool::new(true),
            usage_count: AtomicU8::new(initial_usage_count(intent)),
            prefetch: AtomicBool::new(intent == AccessIntent::Prefetch),
            scan_origin: AtomicBool::new(matches!(
                intent,
                AccessIntent::Scan
                    | AccessIntent::SmallScan
                    | AccessIntent::Prefetch
                    | AccessIntent::Probe
            )),
            flushed_at_us: AtomicU64::new(0),
            flush_lock: Mutex::new(()),
        }
    }

    #[cfg(test)]
    fn new_dirty(
        key: PageKey,
        bytes: Vec<u8>,
        dirty_lsn: u64,
        generation: u64,
        raw_memory_charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
    ) -> Self {
        Self::new_dirty_with_intent(
            key,
            bytes,
            dirty_lsn,
            generation,
            raw_memory_charge,
            AccessIntent::Normal,
        )
    }

    fn new_dirty_with_intent(
        key: PageKey,
        bytes: Vec<u8>,
        dirty_lsn: u64,
        generation: u64,
        raw_memory_charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
        intent: AccessIntent,
    ) -> Self {
        Self {
            key,
            raw_memory_charge: Mutex::new(Some(raw_memory_charge)),
            bytes: RwLock::new(bytes),
            small_scan_accesses: AtomicU8::new(0),
            pin_count: AtomicUsize::new(1),
            state: AtomicU8::new(FrameIoState::Ready as u8),
            io_wait_lock: Mutex::new(()),
            io_wait_cv: Condvar::new(),
            io_waiters: AtomicUsize::new(0),
            generation: AtomicU64::new(generation),
            dirty: AtomicBool::new(true),
            dirty_lsn: AtomicU64::new(dirty_lsn),
            first_dirty_lsn: AtomicU64::new(dirty_lsn),
            deferred_pins: AtomicU32::new(0),
            dirty_epoch: AtomicU64::new(1),
            checksum_dirty: AtomicBool::new(false),
            packed_leaf: parking_lot::Mutex::new(None),
            packed_leaf_state: AtomicU8::new(0),
            prefetch_loaded_at_us: AtomicU64::new(0),
            index_header_validated_epoch: AtomicU64::new(u64::MAX),
            heap_header_present: AtomicBool::new(false),
            heap_header_validated_epoch: AtomicU64::new(u64::MAX),
            ref_bit: AtomicBool::new(intent != AccessIntent::Prefetch),
            usage_count: AtomicU8::new(initial_usage_count(intent)),
            prefetch: AtomicBool::new(false),
            scan_origin: AtomicBool::new(matches!(
                intent,
                AccessIntent::Scan
                    | AccessIntent::SmallScan
                    | AccessIntent::Prefetch
                    | AccessIntent::Probe
            )),
            flushed_at_us: AtomicU64::new(0),
            flush_lock: Mutex::new(()),
        }
    }

    fn mark_dirty(&self, dirty_lsn: u64) -> bool {
        self.dirty_lsn.fetch_max(dirty_lsn, Ordering::AcqRel);
        let was_clean = !self.dirty.swap(true, Ordering::AcqRel);
        self.note_first_dirty_lsn(dirty_lsn);
        self.dirty_epoch.fetch_add(1, Ordering::AcqRel);
        self.index_header_validated_epoch
            .store(u64::MAX, Ordering::Release);
        self.heap_header_validated_epoch
            .store(u64::MAX, Ordering::Release);
        self.touch(AccessIntent::Normal);
        was_clean
    }

    fn publish_io_state(&self, state: FrameIoState) {
        // Discard is terminal for this Frame allocation. A late loader or
        // flusher must not resurrect it after file invalidation; weak access
        // hints may still refer to the old allocation while a reader drains.
        if state == FrameIoState::Discarded {
            self.state.store(state as u8, Ordering::Release);
        } else if self
            .state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current != FrameIoState::Discarded as u8).then_some(state as u8)
            })
            .is_err()
        {
            return;
        }
        if self.io_waiters.load(Ordering::Acquire) == 0 {
            return;
        }
        // Synchronize with the waiter between its state recheck and atomic
        // Condvar park, closing the standard lost-wakeup window.
        if let Ok(_guard) = self.io_wait_lock.lock() {}
        self.io_wait_cv.notify_all();
    }

    fn note_first_dirty_lsn(&self, dirty_lsn: u64) {
        if dirty_lsn == 0 {
            return;
        }
        let mut current = self.first_dirty_lsn.load(Ordering::Acquire);
        loop {
            if current != 0 && current <= dirty_lsn {
                return;
            }
            match self.first_dirty_lsn.compare_exchange(
                current,
                dirty_lsn,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }

    fn take_raw_page_lease(&self) -> Option<RawPageLease> {
        let mut bytes = self.bytes.write().ok()?;
        let mut charge = self.raw_memory_charge.lock().ok()?;
        let charge = charge.take()?;
        self.invalidate_packed_leaf();
        let mut page = std::mem::take(&mut *bytes);
        if page.capacity() < PAGE_SIZE {
            return None;
        }
        page.resize(PAGE_SIZE, 0);
        Some(RawPageLease {
            bytes: page,
            charge,
        })
    }

    /// Concurrent CLOCK hands may inspect the same frame. Decrement with a
    /// conditional atomic update: load + fetch_sub can wrap zero to 255 when
    /// another hand or scan demotion clears the heat between those operations.
    fn clock_second_chance(&self) -> bool {
        if self.ref_bit.swap(false, Ordering::AcqRel) {
            return true;
        }
        self.usage_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |usage| {
                usage.checked_sub(1)
            })
            .is_ok()
    }

    fn touch(&self, intent: AccessIntent) {
        let _ = self.touch_and_small_scan_prior(intent);
    }

    fn touch_and_small_scan_prior(&self, intent: AccessIntent) -> Option<u8> {
        // M5.1 - Midpoint insertion LRU (InnoDB-style young/old sublist).
        // Scan/Prefetch enters the "old" sublist with low usage. A subsequent
        // Normal access promotes the page into "young" (higher usage). This
        // protects hot pages from full-table-scan pollution.
        if !self.ref_bit.load(Ordering::Relaxed) {
            self.ref_bit.store(true, Ordering::Relaxed);
        }
        let prev = self.usage_count.load(Ordering::Acquire);
        match intent {
            AccessIntent::Normal => {
                // If the page was already accessed (prev >= 1), promote toward young.
                // Cap high enough that repeatedly touched random-lookup pages
                // survive several clock passes under mixed hot/cold pressure.
                let target = prev.saturating_add(1).min(Self::MAX_NORMAL_USAGE_COUNT);
                if target > prev {
                    self.usage_count.fetch_max(target, Ordering::AcqRel);
                }
                None
            }
            AccessIntent::Scan => {
                // Scan accesses keep the page in the cold sublist (usage <= 1).
                if prev < 1 {
                    self.usage_count.fetch_max(1, Ordering::AcqRel);
                }
                None
            }
            AccessIntent::SmallScan => {
                // The logical-access counter, not the cache-hit count, is
                // authoritative. In particular a prefetched frame starts at
                // zero and its first demand remains cold.
                let prior = self
                    .small_scan_accesses
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                        Some(value.saturating_add(1))
                    })
                    .unwrap_or_else(|value| value);
                let target = if prior == 0 {
                    1
                } else {
                    prev.saturating_add(1).min(6)
                };
                if target > prev {
                    self.usage_count.fetch_max(target, Ordering::AcqRel);
                }
                Some(prior)
            }
            AccessIntent::Prefetch => {
                // Prefetch is the lowest priority; do not even ensure ref_bit advanced.
                None
            }
            AccessIntent::Probe => {
                if prev < 1 {
                    self.usage_count.fetch_max(1, Ordering::AcqRel);
                } else {
                    // One probe remains cold; a second hit proves reuse across
                    // rows/chunks and promotes the frame into the working set.
                    self.usage_count.fetch_max(2, Ordering::AcqRel);
                }
                None
            }
        }
    }

    fn is_ready(&self) -> bool {
        matches!(
            FrameIoState::from_u8(self.state.load(Ordering::Acquire)),
            FrameIoState::Ready | FrameIoState::Flushing
        )
    }

    fn unpin(&self) {
        let prev = self.pin_count.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(prev > 0 && prev != BufferPool::EVICTING_PIN);
    }
}

fn initial_usage_count(intent: AccessIntent) -> u8 {
    match intent {
        AccessIntent::Normal => 2,
        AccessIntent::Scan => 1,
        AccessIntent::SmallScan => 1,
        AccessIntent::Prefetch => 0,
        AccessIntent::Probe => 1,
    }
}

impl<P: std::ops::Deref<Target = BufferPool>> PinnedFrame<P> {
    #[cfg(test)]
    fn with_bytes<T>(
        &self,
        f: impl FnOnce(&[u8]) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        let bytes = self.frame.read_materialized_index_bytes()?;
        f(&bytes)
    }

    #[cfg(test)]
    fn bytes(&self) -> Result<Vec<u8>, QueryError> {
        self.with_bytes(|bytes| Ok(bytes.to_vec()))
    }

    fn with_heap_page<T>(
        &self,
        f: impl FnOnce(&Page) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        let page = self.pool.heap_page_for_read(&self.frame, self.intent)?;
        f(&page)
    }

    fn index_page(&self) -> Result<Arc<IndexPage>, QueryError> {
        self.pool.index_page_for_read(&self.frame)
    }

    /// Hand the closure the frame's raw 16KB page bytes under a read lock.
    fn with_index_raw_bytes<R>(
        &self,
        f: impl FnOnce(&[u8]) -> Result<R, QueryError>,
    ) -> Result<R, QueryError> {
        let bytes = self.frame.read_materialized_index_bytes()?;
        // Validate the index page header (magic / version / full-page
        // checksum) at most once per resident page image. The byte-level
        // leaf-lookup closures below assume a structurally sound page, so
        // skipping the recompute is only safe behind this gate. The
        // `bytes` read lock blocks any in-place writer, and a write bumps
        // `dirty_epoch` (resetting the validated marker) under that same
        // lock, so a modified image can never reuse a stale validation.
        let epoch = self.frame.dirty_epoch.load(Ordering::Acquire);
        if self
            .frame
            .index_header_validated_epoch
            .load(Ordering::Acquire)
            != epoch
        {
            if self.frame.checksum_dirty.load(Ordering::Acquire) {
                validate_index_page_structure(&bytes)?;
            } else {
                validate_index_page_header(&bytes)?;
            }
            self.frame
                .index_header_validated_epoch
                .store(epoch, Ordering::Release);
        }
        f(&bytes)
    }

    /// Direct scans and buffer-pool probes share the same best-effort checksum memo,
    /// avoiding another full-page CRC when a verified clean disk image is
    /// admitted (or re-admitted) to the pool. Dirty frames bypass the memo.
    fn with_heap_raw_bytes_cached<R>(
        &self,
        verified_checksums: Option<&VerifiedDirectHeapChecksums>,
        f: impl FnOnce(Option<HeapPageView<'_>>) -> Result<R, QueryError>,
    ) -> Result<R, QueryError> {
        let bytes = self.frame.bytes.read().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "buffer frame read lock poisoned",
            )
        })?;
        let present = BufferPool::validated_heap_page_present_with_cache(
            &self.frame,
            &bytes,
            verified_checksums,
            self.frame.key.page_id,
        )?;
        f(present.then(|| HeapPageView::from_validated(&bytes)))
    }
}

impl<P: std::ops::Deref<Target = BufferPool>> Drop for PinnedFrame<P> {
    fn drop(&mut self) {
        self.frame.unpin();
        self.pool.notify_frame_waiters();
    }
}

// P0-1 - process-global spill IO counters. Hash-join grace, vec
// hash-agg, and external-sort all spill to their own temp files that
// bypass the buffer pool; recording byte volume here lets
// `SHOW ENGINE MYDB STATUS` show how much background IO bandwidth
// queries are consuming outside the page cache. Cleared only on
// process restart, mirroring the other monotonic counters.
static SPILL_BYTES_WRITTEN: AtomicU64 = AtomicU64::new(0);
static SPILL_BYTES_READ: AtomicU64 = AtomicU64::new(0);

pub(crate) fn record_spill_bytes_written(bytes: u64) {
    if bytes == 0 {
        return;
    }
    SPILL_BYTES_WRITTEN.fetch_add(bytes, Ordering::Relaxed);
}

pub(crate) fn record_spill_bytes_read(bytes: u64) {
    if bytes == 0 {
        return;
    }
    SPILL_BYTES_READ.fetch_add(bytes, Ordering::Relaxed);
}

pub(crate) fn spill_bytes_written() -> u64 {
    SPILL_BYTES_WRITTEN.load(Ordering::Relaxed)
}

pub(crate) fn spill_bytes_read() -> u64 {
    SPILL_BYTES_READ.load(Ordering::Relaxed)
}

#[cfg(test)]
mod sparse_run_completion_tests {
    use super::*;
    #[test]
    fn single_io_worker_completes_every_sparse_run() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("single-worker.heap");
        fs::write(&path, vec![19; PAGE_SIZE * 16]).unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool =
            BufferPool::new_with_options(32, log, DurabilityMode::Strict, Duration::ZERO, 1, 0);
        let file = Arc::new(BufferPool::heap_file_key(path, 1));
        pool.load_ordered_probe_pages(&file, &[0, 2, 4, 6]).unwrap();
        for page_id in [0, 2, 4, 6] {
            let key = PageKey {
                file: Arc::clone(&file),
                page_id,
            };
            let frame = pool
                .lookup_frame(&key)
                .unwrap()
                .expect("ordered page loading returned before every run completed");
            assert!(frame.is_ready());
            assert_eq!(frame.pin_count.load(Ordering::Acquire), 0);
        }
    }
}

#[cfg(test)]
#[path = "buffer/dirty_registry_tests.rs"]
mod dirty_registry_tests;

/// Prefetch notifications are demand-driven and can arrive thousands of times
/// per second. They must not turn the periodic writeback budget into a budget
/// per read notification. Capacity-pressure headroom stays immediate.
fn periodic_cleaner_tick_due(next: &mut Instant, now: Instant) -> bool {
    if now < *next {
        return false;
    }
    *next = now + Duration::from_millis(25);
    true
}

#[cfg(test)]
#[test]
fn buffer_pool_prefetch_wakeups_do_not_accelerate_periodic_writeback() {
    let start = Instant::now();
    let mut next = start;
    let ticks = (0..1000)
        .filter(|ms| periodic_cleaner_tick_due(&mut next, start + Duration::from_millis(*ms)))
        .count();
    assert_eq!(ticks, 40);
    // A late worker resumes one bounded batch rather than catching up every
    // missed tick in a burst, and wakes at the same deadline without reads.
    let late = start + Duration::from_secs(10);
    assert!(periodic_cleaner_tick_due(&mut next, late));
    assert!(!periodic_cleaner_tick_due(&mut next, late));
    assert!(periodic_cleaner_tick_due(
        &mut next,
        late + Duration::from_millis(25)
    ));
}

#[cfg(test)]
mod round2_buffer_tests {
    use super::*;
    #[test]
    fn round2_new_frame_is_pinned_until_deferred_wal_finishes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("round2.lob");
        fs::write(&path, vec![0; PAGE_SIZE]).unwrap();
        let pool = BufferPool::new_with_options(
            2,
            Arc::new(parking_lot::Mutex::new(
                LogManager::open(dir.path()).unwrap(),
            )),
            DurabilityMode::Memory,
            Duration::ZERO,
            1,
            0,
        );
        let key = PageKey {
            file: Arc::new(BufferPool::overflow_file_key(path, 7)),
            page_id: 0,
        };
        btree_logical_wal::begin_deferred();
        pool.store_page(key.clone(), vec![0; PAGE_SIZE], 1).unwrap();
        let keys = btree_logical_wal::take_deferred();
        assert_eq!(keys.len(), 1);
        let frame = pool.lookup_frame(&key).unwrap().unwrap();
        assert_eq!(frame.deferred_pins.load(Ordering::Acquire), 1);
        pool.finalize_deferred(&key, 7);
        assert_eq!(frame.deferred_pins.load(Ordering::Acquire), 0);
    }
}
