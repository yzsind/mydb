//! Nonblocking data-directory locks, owned by the original file handle.

#[cfg(windows)]
pub(crate) fn try_exclusive(file: &std::fs::File) -> std::io::Result<bool> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION;
    use windows_sys::Win32::Storage::FileSystem::{
        LockFileEx, LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY,
    };
    use windows_sys::Win32::System::IO::OVERLAPPED;
    // Lock beyond the PID diagnostic bytes. Windows mandatory byte-range
    // locks would otherwise prevent another handle from reading the PID.
    let mut range: OVERLAPPED = unsafe { std::mem::zeroed() };
    range.Anonymous.Anonymous.Offset = 1024;
    // SAFETY: this is a synchronous, live handle. FAIL_IMMEDIATELY forbids
    // pending lock requests; the kernel releases the lock when it closes.
    if unsafe {
        LockFileEx(
            file.as_raw_handle(),
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
            0,
            1,
            0,
            &mut range,
        )
    } != 0
    {
        return Ok(true);
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(ERROR_LOCK_VIOLATION as i32) {
        Ok(false)
    } else {
        Err(error)
    }
}
