//! Immutable, quota-accounted byte mappings for vector build scratch.

use crate::engine::executor::select::grace::SpillTmpDir;
use crate::storage::error;
use crate::types::QueryError;

#[cfg(any(target_os = "linux", test))]
use std::sync::atomic::{AtomicU8, Ordering};

/// Approximate residency only schedules hints; demand reads remain authoritative.
#[cfg(any(target_os = "linux", test))]
#[derive(Debug)]
struct PrefetchPages {
    len: usize,
    page: usize,
    state: Vec<AtomicU8>,
}

#[cfg(any(target_os = "linux", test))]
impl PrefetchPages {
    fn observe(&self, resident: &[u8]) {
        for (state, &resident) in self.state.iter().zip(resident) {
            state.store(resident & 1, Ordering::Relaxed);
        }
    }

    fn claim(&self, offset: usize, bytes: usize, mut issue: impl FnMut(usize, usize)) {
        if offset >= self.len || bytes == 0 {
            return;
        }
        let first = offset / self.page;
        let end = offset
            .saturating_add(bytes)
            .min(self.len)
            .div_ceil(self.page);
        let mut run = None;
        for page in first..end {
            // 0 = cold, 1 = observed resident, 2 = already requested. An old
            // positive observation can only omit a hint, never a demand read.
            if self.state[page].load(Ordering::Relaxed) == 0
                && self.state[page]
                    .compare_exchange(0, 2, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
            {
                run.get_or_insert(page);
            } else if let Some(begin) = run.take() {
                issue(begin * self.page, (page - begin) * self.page);
            }
        }
        if let Some(begin) = run {
            issue(begin * self.page, (end - begin) * self.page);
        }
    }
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
pub(crate) struct MappedPrefetch {
    pages: PrefetchPages,
    snapshot: parking_lot::Mutex<Vec<u8>>,
    clock: std::time::Instant,
    next_ms: std::sync::atomic::AtomicU64,
    interval_ms: u64,
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

#[cfg(not(target_os = "linux"))]
#[derive(Debug)]
pub(crate) struct MappedPrefetch;

impl MappedPrefetch {
    pub(crate) fn for_view(
        view: &crate::platform::mapping::FileMapping,
        budget: usize,
    ) -> Option<Self> {
        #[cfg(target_os = "linux")]
        {
            let hints = Self::for_len(
                view.len(),
                budget,
                crate::engine::executor::mem_budget::ControlledMemoryCategory::DerivedCache,
            )?;
            hints.refresh_view(view);
            Some(hints)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (view, budget);
            None
        }
    }

    #[cfg(target_os = "linux")]
    fn for_len(
        len: usize,
        budget: usize,
        category: crate::engine::executor::mem_budget::ControlledMemoryCategory,
    ) -> Option<Self> {
        use crate::engine::executor::mem_budget::ControlledMemoryCharge;
        if len <= crate::engine::executor::mem_budget::effective_memory_limit_bytes() / 4 {
            return None;
        }
        let page = mapping_page_bytes();
        let count = len.div_ceil(page);
        let bytes = count
            .checked_mul(2)?
            .checked_add(std::mem::size_of::<MappedPrefetch>())?;
        // Residency metadata must not displace a significant code share.
        if bytes > budget || bytes > crate::engine::executor::mem_budget::memory_target_bytes() / 64
        {
            return None;
        }
        let charge = ControlledMemoryCharge::try_new_optional(category, bytes)?;
        let mut state = Vec::new();
        state.try_reserve_exact(count).ok()?;
        state.extend((0..count).map(|_| AtomicU8::new(0)));
        let mut snapshot = Vec::new();
        snapshot.try_reserve_exact(count).ok()?;
        snapshot.resize(count, 0);
        let hints = MappedPrefetch {
            pages: PrefetchPages { len, page, state },
            snapshot: parking_lot::Mutex::new(snapshot),
            clock: std::time::Instant::now(),
            next_ms: std::sync::atomic::AtomicU64::new(0),
            // Bound page-table sampling work even for very large maps.
            interval_ms: (count as u64).div_ceil(4000).max(50),
            _charge: charge,
        };
        Some(hints)
    }

    pub(crate) fn refresh_view(&self, view: &crate::platform::mapping::FileMapping) {
        #[cfg(target_os = "linux")]
        self.refresh_region(view.as_ptr().cast(), view.len());
        #[cfg(not(target_os = "linux"))]
        let _ = view;
    }

    pub(crate) fn prefetch_view(
        &self,
        view: &crate::platform::mapping::FileMapping,
        offset: usize,
        bytes: usize,
    ) {
        #[cfg(target_os = "linux")]
        if self.pages.len == view.len() {
            self.pages
                .claim(offset, bytes, |start, len| view.prefetch_range(start, len));
        }
        #[cfg(not(target_os = "linux"))]
        let _ = (view, offset, bytes);
    }

    pub(crate) fn bytes(&self) -> usize {
        #[cfg(target_os = "linux")]
        {
            self._charge.bytes()
        }
        #[cfg(not(target_os = "linux"))]
        {
            0
        }
    }
    pub(crate) fn refresh(&self, mapping: &MappedSpillBytes) {
        #[cfg(target_os = "linux")]
        self.refresh_region(mapping.address, mapping.len);
        #[cfg(not(target_os = "linux"))]
        let _ = mapping;
    }

    #[cfg(target_os = "linux")]
    fn refresh_region(&self, address: *mut libc::c_void, len: usize) {
        if len != self.pages.len {
            return;
        }
        let now = self.clock.elapsed().as_millis().min(u64::MAX as u128) as u64;
        if now < self.next_ms.load(Ordering::Relaxed) {
            return;
        }
        let Some(mut snapshot) = self.snapshot.try_lock() else {
            return;
        };
        if now < self.next_ms.load(Ordering::Relaxed) {
            return;
        }
        self.next_ms
            .store(now.saturating_add(self.interval_ms), Ordering::Relaxed);
        // SAFETY: the address is page-aligned, the immutable view owns
        // len bytes, and the output has one byte for every rounded page.
        if unsafe { libc::mincore(address, len, snapshot.as_mut_ptr()) } == 0 {
            self.pages.observe(&snapshot);
        }
    }

    pub(crate) fn prefetch(&self, mapping: &MappedSpillBytes, offset: usize, bytes: usize) {
        #[cfg(target_os = "linux")]
        if mapping.len == self.pages.len {
            self.pages.claim(offset, bytes, |start, len| {
                mapping.prefetch_range(start, len)
            });
        }
        #[cfg(not(target_os = "linux"))]
        let _ = (mapping, offset, bytes);
    }
}

fn io_error(err: std::io::Error) -> QueryError {
    crate::engine::executor::spill_quota::query_error_from_io(&err).unwrap_or_else(|| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            format!("vector row spill: {err}"),
        )
    })
}

/// Read-only mapping of a completed private spill. Owns the directory and
/// its disk quota through the last reader, including readers on other threads.
pub(crate) struct MappedSpillBytes {
    mapping: Option<crate::platform::mapping::FileMapping>,
    address: *mut libc::c_void,
    len: usize,
    _tmp: SpillTmpDir,
}

// SAFETY: the writer is closed before construction, the private file is never
// modified while mapped, and returned borrows cannot outlive the mapping.
unsafe impl Send for MappedSpillBytes {}
unsafe impl Sync for MappedSpillBytes {}

impl std::fmt::Debug for MappedSpillBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MappedSpillBytes")
            .field("len", &self.len)
            .finish()
    }
}

impl MappedSpillBytes {
    pub(crate) fn prefetcher(&self, budget: usize) -> Option<MappedPrefetch> {
        #[cfg(target_os = "linux")]
        {
            let hints = MappedPrefetch::for_len(
                self.len,
                budget,
                crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
            )?;
            hints.refresh(self);
            Some(hints)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = budget;
            None
        }
    }
    /// Graph probes are random. Suppress mmap readahead so a tiny code
    /// lookup does not evict useful pages by faulting a large adjacent window.
    pub(crate) fn advise_random(&self) {
        if let Some(mapping) = &self.mapping {
            mapping.advise_random();
        }
    }

    pub(crate) fn advise_sequential(&self) {
        #[cfg(unix)]
        if self.len != 0 {
            // SAFETY: this is the live immutable view. Sequential readahead
            // changes cache policy only; random exceptional reads stay valid.
            unsafe {
                libc::madvise(self.address, self.len, libc::MADV_SEQUENTIAL);
            }
        }
    }

    /// Hardware prefetch cannot bring a nonresident mapped file page into
    /// memory. Queue bounded read-ahead only for images larger than the
    /// physical-memory share; small, already resident images need no syscall.
    pub(crate) fn prefetch_range(&self, offset: usize, bytes: usize) {
        #[cfg(target_os = "linux")]
        {
            if self.len <= crate::engine::executor::mem_budget::effective_memory_limit_bytes() / 4
                || offset >= self.len
                || bytes == 0
            {
                return;
            }
            let page = mapping_page_bytes();
            let start = offset / page * page;
            let end = offset.saturating_add(bytes).min(self.len).div_ceil(page) * page;
            // SAFETY: mmap owns the page-rounded extent. This is only an I/O
            // hint over a bounded subrange and never changes the mapped bytes.
            unsafe {
                libc::madvise(
                    self.address.cast::<u8>().add(start).cast(),
                    end - start,
                    libc::MADV_WILLNEED,
                );
            }
        }
        #[cfg(not(target_os = "linux"))]
        let _ = (offset, bytes);
    }

    pub(crate) fn open(
        file: crate::platform::file::File,
        tmp: SpillTmpDir,
    ) -> Result<Self, QueryError> {
        Self::open_with_reuse(file, tmp, true)
    }

    /// One-pass streams should not promote their source pages over random
    /// navigation records. This hint does not discard or alter any bytes.
    pub(crate) fn open_with_reuse(
        file: crate::platform::file::File,
        tmp: SpillTmpDir,
        reuse: bool,
    ) -> Result<Self, QueryError> {
        #[cfg(target_os = "linux")]
        if !reuse {
            use std::os::fd::AsRawFd;
            // SAFETY: the file is live and offset/length zero denotes its
            // full extent. Failure only omits an optional cache policy hint.
            unsafe {
                libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_NOREUSE);
            }
        }
        #[cfg(not(target_os = "linux"))]
        let _ = reuse;
        let len = usize::try_from(file.metadata().map_err(io_error)?.len())
            .ok()
            .filter(|len| *len <= isize::MAX as usize)
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_OUTOFMEMORY,
                    "spill mapping exceeds address space",
                )
            })?;
        let mapping = if len == 0 {
            None
        } else {
            // SAFETY: the completed private file is immutable, and the owned
            // view drops before its backing temporary directory.
            Some(
                unsafe { crate::platform::mapping::FileMapping::open(&file, 0, len, false) }
                    .map_err(io_error)?,
            )
        };
        let address = mapping
            .as_ref()
            .map_or(std::ptr::null_mut(), |m| m.as_ptr().cast());
        Ok(Self {
            mapping,
            address,
            len,
            _tmp: tmp,
        })
    }
}

#[cfg(target_os = "linux")]
fn mapping_page_bytes() -> usize {
    static PAGE_BYTES: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *PAGE_BYTES.get_or_init(|| {
        // SAFETY: sysconf has no pointer arguments.
        usize::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) })
            .ok()
            .filter(|n| n.is_power_of_two())
            .unwrap_or(4096)
    })
}

#[cfg(test)]
mod prefetch_tests {
    use super::*;

    #[test]
    fn resident_and_inflight_pages_are_skipped_and_cold_runs_coalesce() {
        let pages = PrefetchPages {
            len: 19,
            page: 4,
            state: (0..5).map(|_| AtomicU8::new(0)).collect(),
        };
        pages.observe(&[1, 0, 0, 1, 0]);
        let mut runs = Vec::new();
        pages.claim(3, usize::MAX, |start, bytes| runs.push((start, bytes)));
        assert_eq!(runs, [(4, 8), (16, 4)]);
        pages.claim(0, 19, |_, _| panic!("duplicate hint"));
        pages.claim(19, 1, |_, _| panic!("past EOF"));
        pages.claim(0, 0, |_, _| panic!("empty hint"));
        pages.observe(&[1, 1, 0, 1, 1]);
        let mut runs = Vec::new();
        pages.claim(4, 8, |start, bytes| runs.push((start, bytes)));
        assert_eq!(runs, [(8, 4)]);
    }

    #[test]
    fn concurrent_probes_only_claim_each_page_once() {
        let pages = PrefetchPages {
            len: 4096,
            page: 64,
            state: (0..64).map(|_| AtomicU8::new(0)).collect(),
        };
        let issued = std::sync::atomic::AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    pages.claim(0, 4096, |_, bytes| {
                        issued.fetch_add(bytes / 64, Ordering::Relaxed);
                    })
                });
            }
        });
        assert_eq!(issued.load(Ordering::Relaxed), 64);
    }
}

impl std::ops::Deref for MappedSpillBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        if self.len == 0 {
            return &[];
        }
        // SAFETY: construction validates length and the owned mapping remains immutable.
        unsafe { std::slice::from_raw_parts(self.address.cast::<u8>(), self.len) }
    }
}
