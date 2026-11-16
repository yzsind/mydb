//! Path-scoped, one-shot fault and scheduling boundaries for backup tests.
use super::*;
use std::sync::OnceLock;

type Hook = Box<dyn FnOnce() -> Result<(), QueryError> + Send>;
type Hooks = BTreeMap<(PathBuf, &'static str), Hook>;
static HOOKS: OnceLock<Mutex<Hooks>> = OnceLock::new();

pub(crate) fn install(
    point: &'static str,
    path: &Path,
    hook: impl FnOnce() -> Result<(), QueryError> + Send + 'static,
) {
    let mut hooks = HOOKS.get_or_init(Default::default).lock().unwrap();
    assert!(hooks
        .insert((path.to_path_buf(), point), Box::new(hook))
        .is_none());
}

pub(crate) fn fail(point: &'static str, path: &Path) {
    install(point, path, move || {
        Err(backup_err(format!("injected {point}")))
    });
}

#[cfg(unix)]
pub(crate) fn crash_process() -> Result<(), QueryError> {
    // kill() queues the signal; another thread can receive it. Never unwind
    // fixture guards in the interval before the kernel terminates this one.
    if unsafe { libc::kill(libc::getpid(), libc::SIGKILL) } != 0 {
        return Err(backup_err(format!(
            "SIGKILL failed: {}",
            std::io::Error::last_os_error()
        )));
    }
    loop {
        std::thread::park();
    }
}

pub(crate) fn hit(point: &'static str, path: &Path) -> Result<(), QueryError> {
    let hook = HOOKS
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .remove(&(path.to_path_buf(), point));
    if let Some(hook) = hook {
        hook()?;
    }
    Ok(())
}
