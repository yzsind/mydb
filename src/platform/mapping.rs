//! Owned file views. Callers must enforce immutability or synchronize mutation.

use std::fs::File;
use std::io;

#[derive(Debug)]
pub(crate) struct FileMapping {
    address: *mut u8,
    #[cfg(any(not(windows), test))]
    len: usize,
    #[cfg(windows)]
    base: *mut std::ffi::c_void,
    #[cfg(windows)]
    _section: std::os::windows::io::OwnedHandle,
}

// SAFETY: ownership can move between threads. Borrowing or modifying mapped
// bytes still requires the caller's synchronization and lifetime protocol.
unsafe impl Send for FileMapping {}
unsafe impl Sync for FileMapping {}

impl FileMapping {
    /// The file must cover this range and must not be truncated while mapped.
    /// Callers must exclude unsynchronized writers before borrowing bytes.
    pub(crate) unsafe fn open(
        file: &File,
        offset: u64,
        len: usize,
        writable: bool,
    ) -> io::Result<Self> {
        if len == 0 || len > isize::MAX as usize {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let end = offset
            .checked_add(len as u64)
            .ok_or(io::ErrorKind::InvalidInput)?;
        if end > file.metadata()?.len() {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            let offset = libc::off_t::try_from(offset).map_err(|_| io::ErrorKind::InvalidInput)?;
            // SAFETY: the caller guarantees file lifetime and extent validity.
            let address = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    len,
                    libc::PROT_READ | if writable { libc::PROT_WRITE } else { 0 },
                    if writable {
                        libc::MAP_SHARED
                    } else {
                        libc::MAP_PRIVATE
                    },
                    file.as_raw_fd(),
                    offset,
                )
            };
            if address == libc::MAP_FAILED {
                return Err(io::Error::last_os_error());
            }
            Ok(Self {
                address: address.cast(),
                len,
            })
        }
        #[cfg(windows)]
        {
            use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
            use windows_sys::Win32::System::Memory::{
                CreateFileMappingW, MapViewOfFile, FILE_MAP_READ, FILE_MAP_WRITE, PAGE_READONLY,
                PAGE_READWRITE,
            };
            use windows_sys::Win32::System::SystemInformation::{GetSystemInfo, SYSTEM_INFO};
            let mut info: SYSTEM_INFO = unsafe { std::mem::zeroed() };
            // SAFETY: info is a correctly sized output structure.
            unsafe { GetSystemInfo(&mut info) };
            let granularity = u64::from(info.dwAllocationGranularity);
            if granularity == 0 {
                return Err(io::ErrorKind::InvalidData.into());
            }
            let aligned = offset / granularity * granularity;
            let delta =
                usize::try_from(offset - aligned).map_err(|_| io::ErrorKind::InvalidInput)?;
            let mapped_len = len
                .checked_add(delta)
                .filter(|n| *n <= isize::MAX as usize)
                .ok_or(io::ErrorKind::InvalidInput)?;
            // SAFETY: file is live, and its current complete size backs the
            // section. No growth or initialization is requested here.
            let handle = unsafe {
                CreateFileMappingW(
                    file.as_raw_handle(),
                    std::ptr::null(),
                    if writable {
                        PAGE_READWRITE
                    } else {
                        PAGE_READONLY
                    },
                    0,
                    0,
                    std::ptr::null(),
                )
            };
            if handle.is_null() {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: the new section handle has one owner.
            let section = unsafe { OwnedHandle::from_raw_handle(handle) };
            let view = unsafe {
                MapViewOfFile(
                    section.as_raw_handle(),
                    if writable {
                        FILE_MAP_WRITE
                    } else {
                        FILE_MAP_READ
                    },
                    (aligned >> 32) as u32,
                    aligned as u32,
                    mapped_len,
                )
            };
            if view.Value.is_null() {
                return Err(io::Error::last_os_error());
            }
            Ok(Self {
                address: unsafe { view.Value.cast::<u8>().add(delta) },
                #[cfg(test)]
                len,
                base: view.Value,
                _section: section,
            })
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = writable;
            Err(io::ErrorKind::Unsupported.into())
        }
    }

    #[inline]
    pub(crate) fn as_ptr(&self) -> *mut u8 {
        self.address
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Advisory only; callers do not depend on this for correctness.
    pub(crate) fn advise_random(&self) {
        #[cfg(unix)]
        unsafe {
            libc::madvise(self.address.cast(), self.len, libc::MADV_RANDOM);
        }
    }
}

impl Drop for FileMapping {
    fn drop(&mut self) {
        #[cfg(unix)]
        // SAFETY: this is the exact view owned by this object.
        unsafe {
            libc::munmap(self.address.cast(), self.len);
        }
        #[cfg(windows)]
        unsafe {
            use windows_sys::Win32::System::Memory::{UnmapViewOfFile, MEMORY_MAPPED_VIEW_ADDRESS};
            UnmapViewOfFile(MEMORY_MAPPED_VIEW_ADDRESS { Value: self.base });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn mapping_bounds_and_shared_mutations() {
        let root = crate::util::file::test_temp_dir();
        let temp = tempfile::tempdir_in(root).unwrap();
        let mut file = File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(temp.path().join("mapped"))
            .unwrap();
        file.write_all(&vec![7; 131_073]).unwrap();
        // SAFETY: no other reader or writer exists while this view is used.
        let view = unsafe { FileMapping::open(&file, 0, 131_073, true) }.unwrap();
        assert_eq!(view.len(), 131_073);
        unsafe {
            view.as_ptr().add(65_539).write(42);
        }
        drop(view);
        file.sync_all().unwrap();
        let bytes = std::fs::read(temp.path().join("mapped")).unwrap();
        assert_eq!(bytes[65_539], 42);
        assert!(unsafe { FileMapping::open(&file, 0, 0, false) }.is_err());
        assert!(unsafe { FileMapping::open(&file, 131_072, 2, false) }.is_err());
        assert!(unsafe { FileMapping::open(&file, u64::MAX, 2, false) }.is_err());
        #[cfg(windows)]
        {
            // Windows views align down to allocation granularity internally.
            let view = unsafe { FileMapping::open(&file, 65_539, 19, false) }.unwrap();
            assert_eq!(unsafe { *view.as_ptr() }, 42);
        }
    }
}
