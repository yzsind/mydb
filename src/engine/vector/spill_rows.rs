//! Immutable, quota-accounted f32 row spill for vector training. Only file
//! pages back the large matrix; row views never copy it into anonymous memory.

use crate::engine::executor::select::grace::SpillTmpDir;
use crate::engine::executor::spill_quota::TrackedSpillWriter;
use crate::storage::{error, RowId};
use crate::types::QueryError;
use std::io::Write;
use std::path::Path;

fn io_error(err: std::io::Error) -> QueryError {
    crate::engine::executor::spill_quota::query_error_from_io(&err).unwrap_or_else(|| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            format!("vector row spill: {err}"),
        )
    })
}

pub(crate) struct VectorRowSpill {
    writer: TrackedSpillWriter,
    tmp: SpillTmpDir,
    dim: usize,
}

impl VectorRowSpill {
    pub(crate) fn new(data_dir: &Path, dim: usize) -> Result<Self, QueryError> {
        if dim
            .checked_mul(4)
            .and_then(|bytes| bytes.checked_add(8))
            .is_none_or(|bytes| bytes > isize::MAX as usize)
        {
            return Err(error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "vector spill dimension exceeds address space",
            ));
        }
        let tmp = SpillTmpDir::create_kind(data_dir, "vector")?;
        let writer =
            TrackedSpillWriter::create_new(&tmp.path().join("rows"), tmp.charger(), 64 * 1024)?;
        Ok(Self { writer, tmp, dim })
    }

    pub(crate) fn push(&mut self, row: RowId, vector: &[f32]) -> Result<(), QueryError> {
        if vector.len() != self.dim {
            return Err(error(
                crate::error::catalog::MYDB_ER_VECTOR_DIMENSION_MISMATCH,
                "vector spill row has inconsistent dimension",
            ));
        }
        let mut header = [0u8; 8];
        header[..4].copy_from_slice(&row.page_id.to_le_bytes());
        header[4..6].copy_from_slice(&row.slot_id.to_le_bytes());
        self.writer.write_all(&header).map_err(io_error)?;
        self.writer
            .write_all(super::ivf::f32_as_bytes(vector))
            .map_err(io_error)
    }

    pub(crate) fn finish(self) -> Result<MappedVectorRows, QueryError> {
        let Self { writer, tmp, dim } = self;
        let len = usize::try_from(writer.finish()?).map_err(|_| {
            error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "vector spill exceeds address space",
            )
        })?;
        if len > isize::MAX as usize || len % (8 + dim * 4) != 0 {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "vector spill has invalid length",
            ));
        }
        let file = crate::platform::file::File::open(tmp.path().join("rows")).map_err(io_error)?;
        let mapping = MappedSpillBytes::open(file, tmp)?;
        Ok(MappedVectorRows { mapping, dim })
    }
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

pub(crate) struct MappedVectorRows {
    mapping: MappedSpillBytes,
    dim: usize,
}

impl MappedVectorRows {
    /// Sampling and list bucketing revisit unrelated rows of the source
    /// matrix. Disable speculative mmap read-ahead for that access pattern;
    /// under a memory cap it otherwise repeatedly evicts useful file pages.
    pub(crate) fn advise_random(&self) {
        self.mapping.advise_random();
    }

    pub(crate) fn rows(&self) -> Vec<(RowId, &[f32])> {
        self.mapping
            .chunks_exact(8 + self.dim * 4)
            .map(|record| {
                let row = RowId {
                    page_id: u32::from_le_bytes(record[..4].try_into().unwrap()),
                    slot_id: u16::from_le_bytes(record[4..6].try_into().unwrap()),
                };
                // SAFETY: the eight-byte header and f32 stride preserve alignment;
                // each completed record contains exactly dim native f32 values.
                let vector = unsafe {
                    std::slice::from_raw_parts(record.as_ptr().add(8).cast::<f32>(), self.dim)
                };
                (row, vector)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spill_preserves_values_and_releases_files_and_quota() {
        let dir = tempfile::tempdir().unwrap();
        let before = crate::engine::executor::spill_quota::global_spilled_bytes();
        let mut writer = VectorRowSpill::new(dir.path(), 3).unwrap();
        let path = writer.tmp.path().to_path_buf();
        let rid = RowId {
            page_id: 123,
            slot_id: 4,
        };
        writer.push(rid, &[1.0, -2.0, 0.5]).unwrap();
        assert!(writer.push(rid, &[1.0]).is_err());
        let mapped = writer.finish().unwrap();
        assert_eq!(mapped.rows(), vec![(rid, &[1.0, -2.0, 0.5][..])]);
        assert!(crate::engine::executor::spill_quota::global_spilled_bytes() > before);
        drop(mapped);
        assert!(!path.exists());
        assert_eq!(
            crate::engine::executor::spill_quota::global_spilled_bytes(),
            before
        );
        let empty = VectorRowSpill::new(dir.path(), 3)
            .unwrap()
            .finish()
            .unwrap();
        assert!(empty.rows().is_empty());
    }
}
