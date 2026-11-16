//! N-4 — small platform shim for telling the OS that a spill file is
//! sequentially-streamed and that, once we're done with it, the page
//! cache doesn't need to hold its bytes.
//!
//! - Linux: `posix_fadvise(SEQUENTIAL | DONTNEED)`.
//! - macOS: `fcntl(F_RDAHEAD)` (analogous to SEQUENTIAL) and
//!   `fcntl(F_NOCACHE)` (analogous to DONTNEED).
//! - Other targets: no-op.
//!
//! Buffer sizing and ownership now live in `spill_quota`; this module keeps
//! only platform hints and shorthand observability wrappers.

use std::fs::File;

/// Start writeback without waiting for durability. Dirty spill pages count
/// against a Linux cgroup until I/O completes; DONTNEED alone cannot discard
/// them. Pipelining writeback keeps spill from exhausting its own headroom.
pub(crate) fn hint_writeback(_file: &File, _offset: u64, _bytes: u64) -> bool {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        let (Ok(offset), Ok(bytes)) = (i64::try_from(_offset), i64::try_from(_bytes)) else {
            return false;
        };
        // SAFETY: the descriptor stays open for this call. WRITE submits I/O
        // without WAIT_BEFORE/WAIT_AFTER. The caller bounds each dirty range.
        unsafe {
            return libc::sync_file_range(
                _file.as_raw_fd(),
                offset,
                bytes,
                libc::SYNC_FILE_RANGE_WRITE,
            ) == 0;
        }
    }
    #[cfg(not(target_os = "linux"))]
    false
}

/// Hint to the OS that this file will be read/written sequentially
/// from start to end. Silent no-op on unsupported platforms.
pub(crate) fn hint_sequential(_file: &File) {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        // Best-effort: errors are not actionable here.
        unsafe {
            libc::posix_fadvise(_file.as_raw_fd(), 0, 0, libc::POSIX_FADV_SEQUENTIAL);
        }
    }
    #[cfg(target_os = "macos")]
    {
        use std::os::unix::io::AsRawFd;
        unsafe {
            libc::fcntl(_file.as_raw_fd(), libc::F_RDAHEAD, 1);
        }
    }
}

/// Hint to the OS that the file's pages can be evicted from the
/// page cache. Called once we're done reading a spill partition so
/// the cold spill bytes don't push the real working set out.
pub(crate) fn hint_dontneed(_file: &File) {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        unsafe {
            libc::posix_fadvise(_file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
        }
    }
    #[cfg(target_os = "macos")]
    {
        use std::os::unix::io::AsRawFd;
        unsafe {
            libc::fcntl(_file.as_raw_fd(), libc::F_NOCACHE, 1);
        }
    }
}

/// Add `bytes` to the process-global spill-write counter. Shorthand
/// over `storage::core::buffer::record_spill_bytes_written` so spill
/// call-sites don't have to know about the buffer module's internals.
pub(crate) fn record_written(bytes: u64) {
    crate::storage::record_spill_bytes_written(bytes);
}

/// Symmetric counterpart of [`record_written`].
#[allow(dead_code)]
pub(crate) fn record_read(bytes: u64) {
    crate::storage::record_spill_bytes_read(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn hint_calls_succeed_on_real_file() {
        // N-4 - the platform-specific hint paths must not error on a
        // well-formed file. We don't probe the kernel's response —
        // just that the FFI call paths don't fault.
        let mut tmp = tempfile::tempfile().expect("tempfile");
        writeln!(tmp, "spill payload").unwrap();
        hint_writeback(&tmp, 0, 14);
        tmp.sync_all().unwrap();
        hint_sequential(&tmp);
        hint_dontneed(&tmp);
    }

    #[test]
    fn record_helpers_accumulate_into_global_counters() {
        // N-4 - validate the shorthand wrappers reach the global
        // counters surfaced via SHOW ENGINE MYDB STATUS. We can't
        // reset the counters from tests (they're process-global),
        // so the test asserts on delta rather than absolute value.
        let before_w = crate::storage::spill_bytes_written();
        let before_r = crate::storage::spill_bytes_read();
        record_written(1_024);
        record_read(512);
        let after_w = crate::storage::spill_bytes_written();
        let after_r = crate::storage::spill_bytes_read();
        assert_eq!(after_w - before_w, 1_024);
        assert_eq!(after_r - before_r, 512);
    }
}
