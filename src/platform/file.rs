//! Stream files with a separate, concurrently usable positioned-I/O handle.
//!
//! Unix keeps std's zero-cost File/OpenOptions aliases and FileExt syscalls.
//! Windows streams use the ordinary synchronous handle; explicit offsets use
//! a separately reopened overlapped handle, preserving the stream cursor.

#[cfg(not(windows))]
pub(crate) use std::fs::{File, OpenOptions};

#[cfg(windows)]
pub(crate) use windows::{File, OpenOptions};

/// Mark a scratch file as sparse before writing its first high-offset slot.
pub(crate) fn make_sparse(file: &std::fs::File) -> std::io::Result<()> {
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::System::Ioctl::FSCTL_SET_SPARSE;
        use windows_sys::Win32::System::IO::DeviceIoControl;
        let mut returned = 0;
        // SAFETY: the stream handle is synchronous and owns the file; no
        // input/output buffer is required to enable the sparse attribute.
        if unsafe {
            DeviceIoControl(
                file.as_raw_handle(),
                FSCTL_SET_SPARSE,
                std::ptr::null(),
                0,
                std::ptr::null_mut(),
                0,
                &mut returned,
                std::ptr::null_mut(),
            )
        } == 0
        {
            return Err(std::io::Error::last_os_error());
        }
    }
    #[cfg(not(windows))]
    let _ = file;
    Ok(())
}

#[cfg(windows)]
mod windows {
    use std::cell::RefCell;
    use std::fs::File as StdFile;
    use std::io::{self, Read, Seek, SeekFrom, Write};
    use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle};
    use std::path::Path;
    use std::sync::Arc;
    use windows_sys::Win32::Foundation::{
        ERROR_HANDLE_EOF, ERROR_IO_PENDING, GENERIC_READ, GENERIC_WRITE, INVALID_HANDLE_VALUE,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        ReOpenFile, ReadFile, WriteFile, FILE_FLAG_OVERLAPPED, FILE_SHARE_DELETE, FILE_SHARE_READ,
        FILE_SHARE_WRITE,
    };
    use windows_sys::Win32::System::Threading::CreateEventW;
    use windows_sys::Win32::System::IO::{GetOverlappedResult, OVERLAPPED};

    thread_local! {
        // A thread issues one synchronous-completion request at a time. No
        // callbacks or user code execute while this event is borrowed.
        static IO_EVENT: RefCell<Option<OwnedHandle>> = const { RefCell::new(None) };
    }

    #[derive(Debug)]
    pub(crate) struct File {
        stream: StdFile,
        positioned: Arc<StdFile>,
    }

    impl File {
        pub(crate) fn from_std(stream: StdFile, read: bool, write: bool) -> io::Result<Self> {
            let access =
                if read { GENERIC_READ } else { 0 } | if write { GENERIC_WRITE } else { 0 };
            // SAFETY: stream owns a live file handle. The new file description
            // has independent position and non-inheritable overlapped I/O.
            let handle = unsafe {
                ReOpenFile(
                    stream.as_raw_handle(),
                    access,
                    FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                    FILE_FLAG_OVERLAPPED,
                )
            };
            if handle == INVALID_HANDLE_VALUE {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: ReOpenFile returned a new uniquely owned handle.
            let positioned = Arc::new(unsafe { StdFile::from_raw_handle(handle) });
            Ok(Self { stream, positioned })
        }

        pub(crate) fn open(path: impl AsRef<Path>) -> io::Result<Self> {
            Self::from_std(StdFile::open(path)?, true, false)
        }

        pub(crate) fn create(path: impl AsRef<Path>) -> io::Result<Self> {
            Self::from_std(StdFile::create(path)?, false, true)
        }

        pub(crate) fn try_clone(&self) -> io::Result<Self> {
            Ok(Self {
                stream: self.stream.try_clone()?,
                positioned: self.positioned.clone(),
            })
        }

        pub(crate) fn flush(&self) -> io::Result<()> {
            (&self.stream).flush()
        }

        pub(crate) fn sync_all(&self) -> io::Result<()> {
            crate::platform::fs::sync_handle(&self.stream)
        }

        pub(crate) fn sync_data(&self) -> io::Result<()> {
            // Windows uses FlushFileBuffers for both data and metadata.
            self.sync_all()
        }

        pub(crate) fn read_at(&self, output: &mut [u8], offset: u64) -> io::Result<usize> {
            // SAFETY: output remains uniquely borrowed until completion.
            unsafe { self.transfer(output.as_mut_ptr(), output.len(), offset, false) }
        }

        pub(crate) fn write_at(&self, input: &[u8], offset: u64) -> io::Result<usize> {
            // SAFETY: input is not mutated and remains borrowed until completion.
            unsafe { self.transfer(input.as_ptr().cast_mut(), input.len(), offset, true) }
        }

        pub(crate) fn read_exact_at(&self, output: &mut [u8], offset: u64) -> io::Result<()> {
            crate::util::file::read_exact_at_counted(self, output, offset, true, None).map(|_| ())
        }

        pub(crate) fn read_exact_at_uninit(
            &self,
            mut output: &mut [std::mem::MaybeUninit<u8>],
            mut offset: u64,
        ) -> io::Result<()> {
            while !output.is_empty() {
                // SAFETY: transfer writes to the live allocation and completes
                // before return. No initialized-byte reference is created.
                let count = match unsafe {
                    self.transfer(output.as_mut_ptr().cast(), output.len(), offset, false)
                } {
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    result => result?,
                };
                if count == 0 {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
                offset = offset
                    .checked_add(count as u64)
                    .ok_or(io::ErrorKind::InvalidInput)?;
                output = &mut output[count..];
            }
            Ok(())
        }

        pub(crate) fn write_all_at(&self, mut input: &[u8], mut offset: u64) -> io::Result<()> {
            while !input.is_empty() {
                let count = match self.write_at(input, offset) {
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    result => result?,
                };
                if count == 0 {
                    return Err(io::ErrorKind::WriteZero.into());
                }
                offset = offset
                    .checked_add(count as u64)
                    .ok_or(io::ErrorKind::InvalidInput)?;
                input = &input[count..];
            }
            Ok(())
        }

        /// The buffer must stay valid and, for reads, writable until return.
        unsafe fn transfer(
            &self,
            buffer: *mut u8,
            len: usize,
            offset: u64,
            write: bool,
        ) -> io::Result<usize> {
            if len == 0 {
                return Ok(0);
            }
            let bytes = len.min(u32::MAX as usize) as u32;
            offset
                .checked_add(bytes as u64)
                .ok_or(io::ErrorKind::InvalidInput)?;
            IO_EVENT.with(|slot| {
                let mut slot = slot.borrow_mut();
                if slot.is_none() {
                    // SAFETY: null security/name pointers and scalar flags.
                    let event = unsafe { CreateEventW(std::ptr::null(), 1, 0, std::ptr::null()) };
                    if event.is_null() {
                        return Err(io::Error::last_os_error());
                    }
                    // SAFETY: this event is uniquely owned by the thread.
                    *slot = Some(unsafe { OwnedHandle::from_raw_handle(event) });
                }
                let mut request: OVERLAPPED = unsafe { std::mem::zeroed() };
                request.hEvent = slot.as_ref().unwrap().as_raw_handle();
                request.Anonymous.Anonymous.Offset = offset as u32;
                request.Anonymous.Anonymous.OffsetHigh = (offset >> 32) as u32;
                let handle = self.positioned.as_raw_handle();
                // SAFETY: the caller's buffer, request, event and file all
                // outlive this operation. Pending operations are waited below.
                let result = unsafe {
                    if write {
                        WriteFile(handle, buffer, bytes, std::ptr::null_mut(), &mut request)
                    } else {
                        ReadFile(handle, buffer, bytes, std::ptr::null_mut(), &mut request)
                    }
                };
                if result == 0 {
                    let error = io::Error::last_os_error();
                    if !write && error.raw_os_error() == Some(ERROR_HANDLE_EOF as i32) {
                        return Ok(0);
                    }
                    if error.raw_os_error() != Some(ERROR_IO_PENDING as i32) {
                        return Err(error);
                    }
                }
                let mut transferred = 0;
                // SAFETY: wait for completion before releasing the request or
                // caller's buffer, including the immediately completed case.
                if unsafe { GetOverlappedResult(handle, &request, &mut transferred, 1) } == 0 {
                    let error = io::Error::last_os_error();
                    if !write && error.raw_os_error() == Some(ERROR_HANDLE_EOF as i32) {
                        return Ok(0);
                    }
                    return Err(error);
                }
                Ok(transferred as usize)
            })
        }
    }

    impl std::ops::Deref for File {
        type Target = StdFile;
        fn deref(&self) -> &StdFile {
            &self.stream
        }
    }

    impl AsRawHandle for File {
        fn as_raw_handle(&self) -> RawHandle {
            self.stream.as_raw_handle()
        }
    }

    impl Read for File {
        fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
            self.stream.read(output)
        }
    }
    impl Read for &File {
        fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
            (&self.stream).read(output)
        }
    }
    impl Write for File {
        fn write(&mut self, input: &[u8]) -> io::Result<usize> {
            self.stream.write(input)
        }
        fn flush(&mut self) -> io::Result<()> {
            self.stream.flush()
        }
    }
    impl Write for &File {
        fn write(&mut self, input: &[u8]) -> io::Result<usize> {
            (&self.stream).write(input)
        }
        fn flush(&mut self) -> io::Result<()> {
            (&self.stream).flush()
        }
    }
    impl Seek for File {
        fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
            self.stream.seek(position)
        }
    }
    impl Seek for &File {
        fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
            (&self.stream).seek(position)
        }
    }

    impl From<File> for StdFile {
        fn from(file: File) -> Self {
            file.stream
        }
    }

    impl From<File> for std::process::Stdio {
        fn from(file: File) -> Self {
            StdFile::from(file).into()
        }
    }

    #[derive(Debug)]
    pub(crate) struct OpenOptions {
        options: std::fs::OpenOptions,
        read: bool,
        write: bool,
        append: bool,
    }

    impl OpenOptions {
        pub(crate) fn new() -> Self {
            Self {
                options: std::fs::OpenOptions::new(),
                read: false,
                write: false,
                append: false,
            }
        }
        pub(crate) fn read(&mut self, enabled: bool) -> &mut Self {
            self.options.read(enabled);
            self.read = enabled;
            self
        }
        pub(crate) fn write(&mut self, enabled: bool) -> &mut Self {
            self.options.write(enabled);
            self.write = enabled;
            self
        }
        #[cfg(test)]
        pub(crate) fn append(&mut self, enabled: bool) -> &mut Self {
            self.options.append(enabled);
            self.append = enabled;
            self
        }
        pub(crate) fn create(&mut self, enabled: bool) -> &mut Self {
            self.options.create(enabled);
            self
        }
        pub(crate) fn create_new(&mut self, enabled: bool) -> &mut Self {
            self.options.create_new(enabled);
            self
        }
        pub(crate) fn truncate(&mut self, enabled: bool) -> &mut Self {
            self.options.truncate(enabled);
            self
        }
        pub(crate) fn open(&self, path: impl AsRef<Path>) -> io::Result<File> {
            File::from_std(
                self.options.open(path)?,
                self.read,
                self.write || self.append,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom};
    #[cfg(unix)]
    use std::os::unix::fs::FileExt;

    #[test]
    fn parallel_positioned_io_preserves_cloned_stream_position() {
        let temp = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(temp.path().join("parallel"))
            .unwrap();
        file.set_len(32 * 1024).unwrap();
        file.seek(SeekFrom::Start(17)).unwrap();
        std::thread::scope(|scope| {
            for lane in 0..8u64 {
                let file = file.try_clone().unwrap();
                scope.spawn(move || {
                    let expected = [lane as u8 + 1; 4096];
                    for _ in 0..32 {
                        file.write_all_at(&expected, lane * 4096).unwrap();
                        let mut actual = [0; 4096];
                        crate::util::file::read_exact_at_counted(
                            &file,
                            &mut actual,
                            lane * 4096,
                            true,
                            None,
                        )
                        .unwrap();
                        assert_eq!(actual, expected);
                    }
                });
            }
        });
        assert_eq!(file.stream_position().unwrap(), 17);
        let mut tail = [0; 1];
        assert_eq!(
            crate::util::file::read_exact_at_counted(&file, &mut tail, 32 * 1024, true, None)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::UnexpectedEof
        );
        assert_eq!(file.stream_position().unwrap(), 17);
    }

    #[test]
    fn sparse_offsets_above_four_gib_are_not_truncated() {
        let temp = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(temp.path().join("sparse"))
            .unwrap();
        make_sparse(&file).unwrap();
        file.seek(SeekFrom::Start(3)).unwrap();
        let offset = (1u64 << 32) + 127;
        file.write_all_at(b"large-offset", offset).unwrap();
        let mut actual = [0; 12];
        crate::util::file::read_exact_at_counted(&file, &mut actual, offset, true, None).unwrap();
        assert_eq!(&actual, b"large-offset");
        assert_eq!(file.stream_position().unwrap(), 3);
        assert_eq!(file.metadata().unwrap().len(), offset + 12);
    }
}
