//! Fixed-size buffer-pool storage with deterministic physical release.
//!
//! Recycling retains hot allocations. A discarded Linux page uses its own
//! mapping so a few live pages cannot pin allocator slabs filled by a scan.

use std::ops::{Deref, DerefMut};

#[derive(Debug, Default)]
pub(super) struct RawPageBytes {
    storage: Storage,
    len: usize,
}

#[derive(Debug, Default)]
enum Storage {
    #[default]
    Empty,
    Heap(Vec<u8>),
    #[cfg(target_os = "linux")]
    Mapped {
        address: std::ptr::NonNull<u8>,
        capacity: usize,
    },
}

// SAFETY: storage is exclusively owned and mutable access requires &mut self.
// Frames additionally protect their bytes with a read/write lock.
unsafe impl Send for RawPageBytes {}
unsafe impl Sync for RawPageBytes {}

impl RawPageBytes {
    pub(super) fn try_zeroed(bytes: usize) -> std::io::Result<Self> {
        if bytes == 0 {
            return Ok(Self::default());
        }
        if bytes > isize::MAX as usize {
            return Err(std::io::ErrorKind::OutOfMemory.into());
        }
        #[cfg(target_os = "linux")]
        {
            static PAGE: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
            let page = *PAGE.get_or_init(|| {
                // SAFETY: sysconf has no pointer arguments.
                usize::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) }).unwrap_or(0)
            });
            // A larger OS page must not multiply ungoverned residency for a
            // smaller database page. Keep the heap fallback for that layout.
            if page != 0 && bytes % page == 0 {
                // SAFETY: an anonymous mapping starts zeroed and has one owner.
                let address = unsafe {
                    libc::mmap(
                        std::ptr::null_mut(),
                        bytes,
                        libc::PROT_READ | libc::PROT_WRITE,
                        libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                        -1,
                        0,
                    )
                };
                if address == libc::MAP_FAILED {
                    return Err(std::io::Error::last_os_error());
                }
                return Ok(Self {
                    storage: Storage::Mapped {
                        address: std::ptr::NonNull::new(address.cast()).expect("non-null mmap"),
                        capacity: bytes,
                    },
                    len: bytes,
                });
            }
        }
        let mut result = Vec::new();
        result
            .try_reserve_exact(bytes)
            .map_err(|_| std::io::ErrorKind::OutOfMemory)?;
        result.resize(bytes, 0);
        Ok(Self::from(result))
    }

    pub(super) fn try_copy(bytes: &[u8]) -> std::io::Result<Self> {
        let mut result = Self::try_zeroed(bytes.len())?;
        result.copy_from_slice(bytes);
        Ok(result)
    }

    pub(super) fn capacity(&self) -> usize {
        match &self.storage {
            Storage::Empty => 0,
            Storage::Heap(bytes) => bytes.capacity(),
            #[cfg(target_os = "linux")]
            Storage::Mapped { capacity, .. } => *capacity,
        }
    }

    pub(super) fn resize(&mut self, len: usize, value: u8) {
        assert!(len <= self.capacity(), "raw page storage cannot grow");
        if let Storage::Heap(bytes) = &mut self.storage {
            bytes.resize(len, value);
            self.len = len;
            return;
        }
        let old = self.len;
        self.len = len;
        if len > old {
            self[old..].fill(value);
        }
    }

    pub(super) fn as_mut_slice(&mut self) -> &mut [u8] {
        self
    }
}

impl From<Vec<u8>> for RawPageBytes {
    fn from(bytes: Vec<u8>) -> Self {
        Self {
            len: bytes.len(),
            storage: Storage::Heap(bytes),
        }
    }
}

impl Deref for RawPageBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match &self.storage {
            Storage::Empty => &[],
            Storage::Heap(bytes) => &bytes[..self.len],
            #[cfg(target_os = "linux")]
            Storage::Mapped { address, .. } => {
                // SAFETY: the mapping is live, initialized and exclusively owned.
                unsafe { std::slice::from_raw_parts(address.as_ptr(), self.len) }
            }
        }
    }
}

impl DerefMut for RawPageBytes {
    fn deref_mut(&mut self) -> &mut [u8] {
        match &mut self.storage {
            Storage::Empty => &mut [],
            Storage::Heap(bytes) => &mut bytes[..self.len],
            #[cfg(target_os = "linux")]
            Storage::Mapped { address, .. } => {
                // SAFETY: &mut self excludes all other borrows of the mapping.
                unsafe { std::slice::from_raw_parts_mut(address.as_ptr(), self.len) }
            }
        }
    }
}

impl Drop for RawPageBytes {
    fn drop(&mut self) {
        #[cfg(target_os = "linux")]
        if let Storage::Mapped { address, capacity } = &self.storage {
            // SAFETY: exactly this mapping is owned and no borrow survives Drop.
            unsafe {
                libc::munmap(address.as_ptr().cast(), *capacity);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_storage_can_be_recycled_and_moved_across_workers() {
        let mut bytes = RawPageBytes::try_zeroed(16384).unwrap();
        assert!(bytes.iter().all(|&v| v == 0));
        let pointer = bytes.as_ptr() as usize;
        bytes[16200] = 91;
        bytes.resize(16000, 0);
        bytes.resize(16384, 17);
        assert_eq!(bytes[16200], 17);
        let bytes = std::thread::spawn(move || {
            assert_eq!(bytes.as_ptr() as usize, pointer);
            bytes[5] = 41;
            bytes
        })
        .join()
        .unwrap();
        assert_eq!(bytes[5], 41);
        assert_eq!(bytes.capacity(), 16384);
    }

    #[test]
    fn importing_bytes_preserves_the_whole_page() {
        let input: Vec<u8> = (0..16384).map(|i| i as u8).collect();
        let output = RawPageBytes::try_copy(&input).unwrap();
        assert_eq!(&*output, input);
        assert!(RawPageBytes::default().is_empty());
        assert!(RawPageBytes::try_zeroed(0).unwrap().is_empty());
    }
}
