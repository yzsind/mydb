//! Scheduling hints for background threads that must yield CPU to request
//! workers.

/// Lower the calling thread's scheduling priority. This is a best-effort
/// hint; admission and correctness never depend on the operating system
/// accepting a scheduling preference.
pub(crate) fn lower_current_thread_priority() {
    #[cfg(target_os = "macos")]
    unsafe {
        libc::pthread_set_qos_class_self_np(libc::qos_class_t::QOS_CLASS_UTILITY, 0);
    }
    #[cfg(target_os = "linux")]
    unsafe {
        // Linux niceness is per thread. Use the explicit TID so this cannot
        // lower the priority of the server's foreground request threads.
        let tid = libc::syscall(libc::SYS_gettid) as libc::id_t;
        libc::setpriority(libc::PRIO_PROCESS, tid, 10);
    }
    #[cfg(windows)]
    unsafe {
        use windows_sys::Win32::System::Threading::{
            GetCurrentThread, SetThreadPriority, THREAD_PRIORITY_BELOW_NORMAL,
        };
        SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_BELOW_NORMAL);
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    #[test]
    fn lowers_only_the_calling_thread() {
        let main_tid = unsafe { libc::syscall(libc::SYS_gettid) } as libc::id_t;
        let before = unsafe { libc::getpriority(libc::PRIO_PROCESS, main_tid) };
        let lowered = std::thread::spawn(|| {
            super::lower_current_thread_priority();
            let tid = unsafe { libc::syscall(libc::SYS_gettid) } as libc::id_t;
            unsafe { libc::getpriority(libc::PRIO_PROCESS, tid) }
        })
        .join()
        .unwrap();
        assert!(lowered >= 10);
        assert_eq!(unsafe { libc::getpriority(libc::PRIO_PROCESS, main_tid) }, before);
    }
}
