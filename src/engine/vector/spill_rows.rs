//! Immutable, quota-accounted byte mappings for vector build scratch.

use crate::engine::executor::select::grace::SpillTmpDir;
use crate::storage::error;
use crate::types::QueryError;

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
    /// Graph probes are random. Suppress mmap readahead so a tiny code
    /// lookup does not evict useful pages by faulting a large adjacent window.
    pub(crate) fn advise_random(&self) {
        if let Some(mapping) = &self.mapping {
            mapping.advise_random();
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
            static PAGE_BYTES: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
            let page = *PAGE_BYTES.get_or_init(|| {
                // SAFETY: sysconf has no pointer arguments.
                let size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
                usize::try_from(size)
                    .ok()
                    .filter(|n| n.is_power_of_two())
                    .unwrap_or(4096)
            });
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
