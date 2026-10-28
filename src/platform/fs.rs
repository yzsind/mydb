//! File-system barriers and file identities used by durable publication.

use std::fs::File;
use std::io;
use std::path::Path;

#[cfg(target_os = "macos")]
mod device_sync;

pub(crate) fn sync_full_file(file: &File) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    return device_sync::sync(file);
    #[cfg(not(target_os = "macos"))]
    file.sync_all()
}

/// Flush a regular file or directory without changing its contents.
/// Windows directory handles require BACKUP_SEMANTICS, and FlushFileBuffers
/// requires write access even when the caller only publishes metadata.
pub(crate) fn sync_path(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref();
    #[cfg(test)]
    SYNC_PATH_ERROR.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.as_ref().is_some_and(|(target, _)| target == path) {
            return Err(io::Error::from_raw_os_error(slot.take().unwrap().1));
        }
        Ok(())
    })?;
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS;
        std::fs::OpenOptions::new()
            .write(true)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(path)?
            .sync_all()
    }
    #[cfg(not(windows))]
    sync_file_at(&File::open(path)?, path)
}

#[cfg(test)]
thread_local! {
    static SYNC_PATH_ERROR: std::cell::RefCell<Option<(std::path::PathBuf, i32)>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(crate) fn inject_path_sync_error_for_test(path: &Path, errno: i32) {
    SYNC_PATH_ERROR.with(|slot| *slot.borrow_mut() = Some((path.to_owned(), errno)));
}

/// Sync file contents. Production retains the platform's full durability
/// barrier; unpublished private trees may coalesce drive-cache flushes.
pub(crate) fn sync_file_at(file: &super::file::File, _path: &Path) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    if let Some(result) = private_sync::sync(file, _path) {
        return result;
    }
    #[cfg(target_os = "macos")]
    return sync_full_file(file);
    #[cfg(not(target_os = "macos"))]
    file.sync_all()
}

pub(crate) fn sync_file_data_at(file: &super::file::File, _path: &Path) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    if let Some(result) = private_sync::sync(file, _path) {
        return result;
    }
    #[cfg(target_os = "macos")]
    return sync_full_file(file);
    #[cfg(not(target_os = "macos"))]
    file.sync_data()
}

/// Standard-library handles also participate in private fixture construction.
/// Outside a private-tree scope these retain their full/data-only barriers.
pub(crate) fn sync_standard_file_at(file: &File, _path: &Path) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    if let Some(result) = private_sync::sync(file, _path) {
        return result;
    }
    sync_full_file(file)
}

#[cfg(any(test, not(target_os = "macos")))]
pub(crate) fn sync_standard_data_at(file: &File, _path: &Path) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    if let Some(result) = private_sync::sync(file, _path) {
        return result;
    }
    #[cfg(target_os = "macos")]
    return sync_full_file(file);
    #[cfg(not(target_os = "macos"))]
    file.sync_data()
}

#[cfg(target_os = "macos")]
pub(crate) fn try_private_sync_file(file: &File) -> Option<io::Result<()>> {
    private_sync::sync_handle(file)
}

pub(crate) fn sync_before_atomic_rename_at(
    file: &super::file::File,
    _path: &Path,
) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    if let Some(result) = private_sync::sync(file, _path) {
        return result;
    }
    sync_before_atomic_rename(file)
}

/// Order staging data before publication. The caller must complete a full
/// parent-directory barrier after rename before acknowledging durability.
pub(crate) fn sync_standard_before_atomic_rename_at(file: &File, _path: &Path) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        if let Some(result) = private_sync::sync(file, _path) {
            return result;
        }
        sync_before_atomic_rename(file)
    }
    #[cfg(not(target_os = "macos"))]
    file.sync_all()
}

/// Drain one path to its device and retain a representative descriptor for
/// the caller's full device-cache barrier. A fixture-owned barrier is returned
/// as None because its construction scope supplies that final flush.
#[cfg(target_os = "macos")]
pub(crate) fn sync_path_to_device(path: &Path) -> io::Result<Option<(u64, File)>> {
    sync_file_to_device(File::open(path)?, path)
}

#[cfg(target_os = "macos")]
fn sync_file_to_device(file: File, _path: &Path) -> io::Result<Option<(u64, File)>> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::MetadataExt;
    if let Some(result) = private_sync::sync(&file, _path) {
        return result.map(|()| None);
    }
    let device = file.metadata()?.dev();
    loop {
        // SAFETY: file owns a live descriptor throughout the syscall.
        if unsafe { libc::fsync(file.as_raw_fd()) } == 0 {
            return Ok(Some((device, file)));
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

/// Persist every path in order, sharing the final device-cache barrier across
/// consecutive paths on one device. Other platforms keep their normal syncs.
pub(crate) fn sync_paths<I, P>(paths: I) -> io::Result<()>
where
    I: IntoIterator<Item = P>,
    P: AsRef<Path>,
{
    #[cfg(target_os = "macos")]
    {
        let mut batch = DeviceSyncBatch::new();
        for path in paths {
            batch.sync_path(path.as_ref())?;
        }
        batch.finish()
    }
    #[cfg(not(target_os = "macos"))]
    {
        for path in paths {
            sync_path(path)?;
        }
        Ok(())
    }
}

#[cfg(target_os = "macos")]
pub(crate) struct DeviceSyncBatch {
    representative: Option<(u64, File)>,
}

#[cfg(target_os = "macos")]
impl DeviceSyncBatch {
    pub(crate) fn new() -> Self {
        Self {
            representative: None,
        }
    }

    pub(crate) fn sync_path(&mut self, path: &Path) -> io::Result<()> {
        self.sync_file(File::open(path)?, path)
    }

    pub(crate) fn sync_file(&mut self, file: File, path: &Path) -> io::Result<()> {
        if let Some(result) = private_sync::sync(&file, path) {
            return result;
        }
        if !device_sync::supports(&file) {
            if let Some((_, previous)) = self.representative.take() {
                sync_full_file(&previous)?;
            }
            return file.sync_all();
        }
        if let Some((device, file)) = sync_file_to_device(file, path)? {
            if let Some((previous, representative)) = &self.representative {
                if *previous != device {
                    sync_full_file(representative)?;
                }
            }
            // Keep only one representative: even huge inventories or mounted
            // subtrees cannot consume an unbounded number of descriptors.
            self.representative = Some((device, file));
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> io::Result<()> {
        if let Some((_, file)) = self.representative {
            sync_full_file(&file)?;
        }
        Ok(())
    }
}

/// Coalesce device-cache flushes while constructing an unpublished private
/// tree. Every file/directory still reaches the device via fsync. The caller
/// must join its writers and finish before publishing any completed state.
/// Existing databases and live transaction work must never enter this scope.
pub(crate) struct PrivateTreeSyncBatch {
    #[cfg(target_os = "macos")]
    inner: private_sync::Batch,
}

impl PrivateTreeSyncBatch {
    pub(crate) fn new(_root: &Path) -> io::Result<Self> {
        Ok(Self {
            #[cfg(target_os = "macos")]
            inner: private_sync::Batch::new(_root)?,
        })
    }

    pub(crate) fn finish(self) -> io::Result<()> {
        #[cfg(target_os = "macos")]
        self.inner.finish()?;
        Ok(())
    }
}

/// Only for private fixture construction; operations under test retain their
/// ordinary barriers unless they use the production private-tree protocol.
#[cfg(test)]
pub(crate) type FixtureSyncBatch = PrivateTreeSyncBatch;

#[cfg(test)]
pub(crate) fn inherit_fixture_sync_batch(_command: &mut std::process::Command, _path: &Path) {
    #[cfg(target_os = "macos")]
    if let Some((root, device)) = private_sync::root_for(_path) {
        _command.env("MYDB_TEST_SYNC_BATCH_ROOT", root);
        _command.env("MYDB_TEST_SYNC_BATCH_DEVICE", device.to_string());
    }
}

#[cfg(all(test, target_os = "macos"))]
pub(crate) fn fixture_sync_epoch() -> Option<u64> {
    private_sync::epoch()
}

#[cfg(target_os = "macos")]
mod private_sync {
    use super::*;
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::MetadataExt;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Mutex, OnceLock};

    static EPOCH: AtomicU64 = AtomicU64::new(0);
    static ACTIVE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

    #[cfg(test)]
    pub(super) fn epoch() -> Option<u64> {
        // Inherited construction scopes last for a preparation child. They
        // must never produce a reusable proof of a full device barrier.
        if inherited().is_some() {
            None
        } else {
            Some(EPOCH.load(Ordering::Acquire))
        }
    }

    struct Root {
        id: u64,
        raw: PathBuf,
        canonical: PathBuf,
        device: u64,
    }

    fn roots() -> &'static Mutex<Vec<Root>> {
        static ROOTS: OnceLock<Mutex<Vec<Root>>> = OnceLock::new();
        ROOTS.get_or_init(Default::default)
    }

    #[cfg(not(test))]
    fn inherited() -> Option<&'static (PathBuf, u64)> {
        None
    }

    #[cfg(test)]
    fn inherited() -> Option<&'static (PathBuf, u64)> {
        static ROOT: OnceLock<Option<(PathBuf, u64)>> = OnceLock::new();
        ROOT.get_or_init(|| {
            Some((
                std::env::var_os("MYDB_TEST_SYNC_BATCH_ROOT")?.into(),
                std::env::var("MYDB_TEST_SYNC_BATCH_DEVICE")
                    .ok()?
                    .parse()
                    .ok()?,
            ))
        })
        .as_ref()
    }

    fn find(roots: &[Root], path: &Path) -> Option<(PathBuf, u64)> {
        roots
            .iter()
            .rev()
            .find(|root| path.starts_with(&root.raw) || path.starts_with(&root.canonical))
            .map(|root| (root.canonical.clone(), root.device))
            .or_else(|| {
                inherited()
                    .filter(|(root, _)| path.starts_with(root))
                    .cloned()
            })
    }

    #[cfg(test)]
    pub(super) fn root_for(path: &Path) -> Option<(PathBuf, u64)> {
        find(&roots().lock().unwrap(), path)
    }

    pub(super) fn sync(file: &File, path: &Path) -> Option<io::Result<()>> {
        if !ACTIVE.load(Ordering::Acquire) && inherited().is_none() {
            return None;
        }
        // Keep registration locked until fsync completes. Finish removes the
        // root under this lock before issuing the final full-device barrier.
        let roots = roots().lock().unwrap();
        let (_, device) = find(&roots, path)?;
        match file.metadata() {
            Ok(metadata) if metadata.dev() == device => {}
            Ok(_) => return None,
            Err(error) => return Some(Err(error)),
        }
        loop {
            // SAFETY: file owns a live descriptor throughout this call.
            if unsafe { libc::fsync(file.as_raw_fd()) } == 0 {
                return Some(Ok(()));
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Some(Err(error));
            }
        }
    }

    pub(super) fn sync_handle(file: &File) -> Option<io::Result<()>> {
        use std::os::unix::ffi::OsStrExt;
        // Most WAL barriers are operations under test, with no fixture scope.
        // Avoid resolving their descriptor paths or changing their barriers.
        if !ACTIVE.load(Ordering::Acquire) && inherited().is_none() {
            return None;
        }
        let mut path = [0u8; libc::PATH_MAX as usize];
        // SAFETY: file owns its descriptor; F_GETPATH writes at most PATH_MAX
        // bytes into the live buffer. An unresolved path uses the normal sync.
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETPATH, path.as_mut_ptr()) } != 0 {
            return None;
        }
        let len = path.iter().position(|byte| *byte == 0)?;
        sync(file, Path::new(std::ffi::OsStr::from_bytes(&path[..len])))
    }

    pub(super) struct Batch {
        id: Option<u64>,
        directory: File,
    }

    impl Batch {
        pub(super) fn new(root: &Path) -> io::Result<Self> {
            static NEXT: AtomicU64 = AtomicU64::new(1);
            let directory = File::open(root)?;
            let id = NEXT.fetch_add(1, Ordering::Relaxed);
            let root = Root {
                id,
                raw: root.to_path_buf(),
                canonical: std::fs::canonicalize(root)?,
                device: directory.metadata()?.dev(),
            };
            let mut registered = roots().lock().unwrap();
            EPOCH.fetch_add(1, Ordering::AcqRel);
            registered.push(root);
            ACTIVE.store(true, Ordering::Release);
            EPOCH.fetch_add(1, Ordering::Release);
            Ok(Self {
                id: Some(id),
                directory,
            })
        }

        fn unregister(&mut self) {
            if let Some(id) = self.id.take() {
                let mut registered = roots().lock().unwrap();
                EPOCH.fetch_add(1, Ordering::AcqRel);
                registered.retain(|root| root.id != id);
                ACTIVE.store(!registered.is_empty(), Ordering::Release);
                EPOCH.fetch_add(1, Ordering::Release);
            }
        }

        pub(super) fn finish(mut self) -> io::Result<()> {
            self.unregister();
            // On Darwin std::File::sync_all issues F_FULLFSYNC. All prior
            // fixture fsyncs have completed, including joined preparation
            // children; this flushes their writes on this same device.
            sync_full_file(&self.directory)
        }
    }

    impl Drop for Batch {
        fn drop(&mut self) {
            self.unregister();
        }
    }
}

/// Order staging contents before a subsequent atomic rename. The caller must
/// still sync the destination directory after rename to complete durability.
pub(crate) fn sync_before_atomic_rename(file: &super::file::File) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        use std::os::fd::AsRawFd;
        loop {
            // SAFETY: the borrowed file owns a live descriptor. This command
            // takes no additional argument and changes no file contents.
            if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_BARRIERFSYNC) } == 0 {
                return Ok(());
            }
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            // Older filesystems may not implement the ordering-only barrier.
            if matches!(error.raw_os_error(), Some(libc::EINVAL | libc::ENOTSUP)) {
                return sync_full_file(file);
            }
            return Err(error);
        }
    }
    #[cfg(not(target_os = "macos"))]
    file.sync_all()
}

#[cfg(windows)]
pub(crate) fn sync_handle(file: &File) -> io::Result<()> {
    use std::os::windows::io::{AsRawHandle, FromRawHandle};
    use windows_sys::Win32::Foundation::{GENERIC_WRITE, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::Storage::FileSystem::{
        ReOpenFile, FILE_FLAG_BACKUP_SEMANTICS, FILE_SHARE_DELETE, FILE_SHARE_READ,
        FILE_SHARE_WRITE,
    };
    match file.sync_all() {
        Ok(()) => return Ok(()),
        Err(error) if error.raw_os_error() == Some(5) => {}
        Err(error) => return Err(error),
    }
    // SAFETY: reopen the same file identity (not its possibly renamed path).
    // Syncing changes no data bytes; both descriptions remain owned here.
    let handle = unsafe {
        ReOpenFile(
            file.as_raw_handle(),
            GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            FILE_FLAG_BACKUP_SEMANTICS,
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    let sync = unsafe { File::from_raw_handle(handle) };
    sync.sync_all()
}

#[cfg(windows)]
pub(crate) fn truncate_and_sync(file: &File, len: u64) -> io::Result<()> {
    use std::os::windows::io::{AsRawHandle, FromRawHandle};
    use windows_sys::Win32::Foundation::{GENERIC_WRITE, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::Storage::FileSystem::{
        ReOpenFile, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    };
    // SAFETY: the caller holds the writer latch. Reopening the same identity
    // grants SetEndOfFile access without changing the append-only description.
    let handle = unsafe {
        ReOpenFile(
            file.as_raw_handle(),
            GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            0,
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    let truncate = unsafe { File::from_raw_handle(handle) };
    truncate.set_len(len)?;
    truncate.sync_all()
}

#[cfg(windows)]
pub(crate) fn link_count(path: &Path) -> io::Result<u64> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
    };
    let file = File::open(path)?;
    // SAFETY: this correctly sized structure is initialized only on success.
    unsafe {
        let mut info: BY_HANDLE_FILE_INFORMATION = std::mem::zeroed();
        if GetFileInformationByHandle(file.as_raw_handle(), &mut info) == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(u64::from(info.nNumberOfLinks))
    }
}

#[cfg(windows)]
pub(crate) fn available_space(path: &Path) -> Option<u64> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;
    let path = path
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect::<Vec<_>>();
    let mut available = 0;
    // SAFETY: the path is nul-terminated and optional outputs are null.
    (unsafe {
        GetDiskFreeSpaceExW(
            path.as_ptr(),
            &mut available,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    } != 0)
        .then_some(available)
}

#[cfg(windows)]
pub(crate) fn wide_path(path: &Path) -> io::Result<Vec<u16>> {
    use std::os::windows::ffi::OsStrExt;
    let path = std::path::absolute(path)?;
    let units = path.as_os_str().encode_wide().collect::<Vec<_>>();
    if units.contains(&0) {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    let mut wide = if units.starts_with(&[92, 92, 63, 92]) {
        units
    } else if units.starts_with(&[92, 92]) {
        r"\\?\UNC\"
            .encode_utf16()
            .chain(units.into_iter().skip(2))
            .collect()
    } else {
        r"\\?\".encode_utf16().chain(units).collect()
    };
    wide.push(0);
    Ok(wide)
}

pub(crate) fn is_link(metadata: &std::fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
        metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
    }
    #[cfg(not(windows))]
    metadata.file_type().is_symlink()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn ordered_staging_sync_and_directory_sync_publish_complete_contents() {
        let temp = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let live = temp.path().join("intent.job");
        let staging = temp.path().join("intent.job.tmp");
        for contents in [b"first intent".as_slice(), b"replacement intent".as_slice()] {
            {
                let mut file = crate::platform::file::File::create(&staging).unwrap();
                file.write_all(contents).unwrap();
                sync_before_atomic_rename(&file).unwrap();
            }
            std::fs::rename(&staging, &live).unwrap();
            sync_path(temp.path()).unwrap();
            assert_eq!(std::fs::read(&live).unwrap(), contents);
            assert!(!staging.exists());
        }
    }

    #[test]
    fn directory_and_read_only_handle_sync_preserve_bytes() {
        let temp = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let path = temp.path().join("durable");
        let mut file = File::create(&path).unwrap();
        file.write_all(b"durable publication").unwrap();
        file.sync_all().unwrap();
        drop(file);
        sync_path(&path).unwrap();
        sync_path(temp.path()).unwrap();
        sync_paths([path.as_path(), temp.path()]).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"durable publication");
        let missing = temp.path().join("missing");
        assert_eq!(
            sync_path(&missing).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(
            sync_paths([&path, &missing]).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        sync_paths([path.as_path(), temp.path()]).unwrap();
        #[cfg(windows)]
        {
            sync_handle(&File::open(&path).unwrap()).unwrap();
            assert_eq!(link_count(&path).unwrap(), 1);
            std::fs::hard_link(&path, temp.path().join("alias")).unwrap();
            assert_eq!(link_count(&path).unwrap(), 2);
        }
    }

    #[cfg(windows)]
    #[test]
    fn interrupted_append_truncates_same_identity_and_preserves_append() {
        let temp = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let path = temp.path().join("relay");
        std::fs::write(&path, b"durablepartial").unwrap();
        let mut append = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        let renamed = temp.path().join("renamed");
        std::fs::rename(&path, &renamed).unwrap();
        std::fs::write(&path, b"different identity").unwrap();
        truncate_and_sync(&append, 7).unwrap();
        append.write_all(b"next").unwrap();
        append.sync_all().unwrap();
        assert_eq!(std::fs::read(&renamed).unwrap(), b"durablenext");
        assert_eq!(std::fs::read(&path).unwrap(), b"different identity");
    }
}
#[cfg(target_os = "macos")]
#[test]
fn fixture_sync_batch_is_private_joined_and_removed_on_unwind() {
    let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let other = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let path = root.path().join("data");
    std::fs::write(&path, b"fixture data").unwrap();
    let batch = FixtureSyncBatch::new(root.path()).unwrap();
    assert!(private_sync::root_for(other.path()).is_none());
    let standard = File::open(&path).unwrap();
    let outside = File::open(other.path()).unwrap();
    assert!(try_private_sync_file(&standard).unwrap().is_ok());
    assert!(try_private_sync_file(&outside).is_none());
    sync_standard_file_at(&standard, &path).unwrap();
    sync_standard_data_at(&standard, &path).unwrap();
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                assert!(private_sync::root_for(&path).is_some());
                sync_path(&path).unwrap();
                sync_path(root.path()).unwrap();
            })
            .join()
            .unwrap();
    });
    let mut child = std::process::Command::new("not-executed");
    inherit_fixture_sync_batch(&mut child, &path);
    assert_eq!(child.get_envs().count(), 2);
    batch.finish().unwrap();
    assert!(private_sync::root_for(&path).is_none());
    assert!(try_private_sync_file(&standard).is_none());
    assert_eq!(std::fs::read(&path).unwrap(), b"fixture data");
    let result = std::panic::catch_unwind(|| {
        let _batch = FixtureSyncBatch::new(root.path()).unwrap();
        panic!("fixture setup failed");
    });
    assert!(result.is_err());
    assert!(private_sync::root_for(&path).is_none());
}
