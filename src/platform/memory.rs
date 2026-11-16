//! OS memory measurements. Missing measurements remain absent, not zero.

#[cfg(unix)]
pub(crate) fn peak_resident_bytes() -> Option<u64> {
    // SAFETY: getrusage writes a correctly sized output structure.
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut usage) != 0 {
            return None;
        }
        let raw = usage.ru_maxrss.max(0) as u64;
        Some(if cfg!(target_os = "macos") {
            raw
        } else {
            raw.saturating_mul(1024)
        })
    }
}

#[cfg(windows)]
pub(crate) fn peak_resident_bytes() -> Option<u64> {
    process_memory().map(|memory| memory.peak_working_set)
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn peak_resident_bytes() -> Option<u64> {
    None
}

#[cfg(windows)]
pub(crate) struct ProcessMemory {
    pub working_set: u64,
    pub peak_working_set: u64,
    pub private_bytes: u64,
}

#[cfg(windows)]
pub(crate) fn process_memory() -> Option<ProcessMemory> {
    use windows_sys::Win32::System::ProcessStatus::{
        K32GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS_EX,
    };
    use windows_sys::Win32::System::Threading::GetCurrentProcess;
    // SAFETY: the EX structure begins with the base structure, and cb selects
    // the complete extended layout. GetCurrentProcess is a borrowed pseudo-handle.
    unsafe {
        let mut counters: PROCESS_MEMORY_COUNTERS_EX = std::mem::zeroed();
        counters.cb = std::mem::size_of_val(&counters) as u32;
        if K32GetProcessMemoryInfo(
            GetCurrentProcess(),
            (&mut counters as *mut PROCESS_MEMORY_COUNTERS_EX).cast(),
            counters.cb,
        ) == 0
        {
            return None;
        }
        Some(ProcessMemory {
            working_set: counters.WorkingSetSize as u64,
            peak_working_set: counters.PeakWorkingSetSize as u64,
            private_bytes: counters.PrivateUsage as u64,
        })
    }
}

#[cfg(windows)]
pub(crate) fn host_memory() -> Option<(u64, u64)> {
    use windows_sys::Win32::System::SystemInformation::{GlobalMemoryStatusEx, MEMORYSTATUSEX};
    // SAFETY: dwLength identifies this correctly sized output structure.
    unsafe {
        let mut status: MEMORYSTATUSEX = std::mem::zeroed();
        status.dwLength = std::mem::size_of_val(&status) as u32;
        (GlobalMemoryStatusEx(&mut status) != 0)
            .then_some((status.ullTotalPhys, status.ullAvailPhys))
    }
}
