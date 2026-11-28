//! Credential metadata capabilities rooted at the configured data directory.
//! Unix operations remain relative to opened directories; Windows holds all
//! parent directories without delete sharing so they cannot be redirected.
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Component, Path};
use std::sync::atomic::{AtomicU64, Ordering};

use super::{io_error, PersistedChannel};
use crate::types::QueryError;

const MAX_BYTES: u64 = 64 * 1024 * 1024;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(super) struct ChannelDirectory {
    directory: File,
    #[cfg(not(unix))]
    path: std::path::PathBuf,
    #[cfg(windows)]
    _parents: Vec<File>,
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message)
}

fn leaf(name: &OsStr) -> io::Result<()> {
    let mut parts = Path::new(name).components();
    if !matches!(parts.next(), Some(Component::Normal(part)) if part == name)
        || parts.next().is_some()
    {
        return Err(invalid("invalid replication metadata file name"));
    }
    Ok(())
}

fn check_directory(file: &File, private: bool, secure: bool) -> io::Result<()> {
    let meta = file.metadata()?;
    if !meta.is_dir() {
        return Err(invalid("replication metadata parent must be a directory"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if meta.uid() != rustix::process::geteuid().as_raw() {
            return Err(invalid(
                "replication metadata directory must be owned by the server",
            ));
        }
        if private && secure {
            if meta.mode() & 0o777 != 0o700 {
                file.set_permissions(fs::Permissions::from_mode(0o700))?;
            }
        } else if meta.mode() & (if private { 0o077 } else { 0o022 }) != 0 {
            return Err(invalid(
                "replication metadata directory has unsafe permissions",
            ));
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
        let _ = (private, secure);
        if meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(invalid(
                "replication metadata directory must not be a reparse point",
            ));
        }
    }
    Ok(())
}

fn check_file(file: &File) -> io::Result<()> {
    let meta = file.metadata()?;
    if !meta.is_file() || meta.len() > MAX_BYTES {
        return Err(invalid("invalid replication metadata file"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if meta.mode() & 0o077 != 0
            || meta.uid() != rustix::process::geteuid().as_raw()
            || meta.nlink() > 1
        {
            return Err(invalid(
                "replication metadata must be a private, server-owned file with one link",
            ));
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
        if meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(invalid("replication metadata must not be a reparse point"));
        }
    }
    Ok(())
}

impl ChannelDirectory {
    pub(super) fn prepare_replication(data_dir: &Path) -> io::Result<()> {
        Self::open_path(data_dir, true, false).map(|_| ())
    }

    pub(super) fn open(data_dir: &Path, create: bool) -> io::Result<Self> {
        Self::open_path(data_dir, create, true)
    }

    pub(super) fn replication(data_dir: &Path) -> io::Result<Self> {
        Self::open_path(data_dir, false, false)
    }

    fn open_path(data_dir: &Path, create: bool, channels: bool) -> io::Result<Self> {
        let components = if channels {
            &[("system", false), ("replication", true), ("channels", true)][..]
        } else {
            &[("system", false), ("replication", true)][..]
        };
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            use std::os::unix::fs::OpenOptionsExt;
            // The configured root may itself be a legitimate filesystem alias
            // (e.g. /tmp on macOS). Every child below this root is no-follow.
            let mut parent = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC)
                .open(data_dir)?;
            check_directory(&parent, false, false)?;
            for &(name, private) in components {
                let name = std::ffi::CString::new(name).unwrap();
                let flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC;
                let child = match open_at(&parent, &name, flags, 0) {
                    Ok(child) => child,
                    Err(err) if create && err.kind() == io::ErrorKind::NotFound => {
                        // SAFETY: the parent fd is live and name is NUL-terminated.
                        let result =
                            unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) };
                        if result != 0 {
                            let err = io::Error::last_os_error();
                            if err.kind() != io::ErrorKind::AlreadyExists {
                                return Err(err);
                            }
                        } else {
                            parent.sync_all()?;
                        }
                        open_at(&parent, &name, flags, 0)?
                    }
                    Err(err) => return Err(err),
                };
                check_directory(&child, private, create)?;
                parent = child;
            }
            Ok(Self { directory: parent })
        }
        #[cfg(windows)]
        {
            // Resolve the explicitly trusted root alias once before locking
            // its directory handle; child opens must not revisit that alias.
            let mut path = fs::canonicalize(data_dir)?;
            let mut parents = vec![open_directory(&path, false, false)?];
            for &(name, private) in components {
                path.push(name);
                if create {
                    match fs::create_dir(&path) {
                        Ok(()) => (),
                        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => (),
                        Err(err) => return Err(err),
                    }
                }
                let file = open_directory(&path, true, create)?;
                check_directory(&file, private, create)?;
                parents.push(file);
            }
            let directory = parents.pop().unwrap();
            Ok(Self {
                directory,
                path,
                _parents: parents,
            })
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = (data_dir, create);
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "secure replication metadata is unavailable on this platform",
            ))
        }
    }

    fn open_file(&self, name: &OsStr, create: bool) -> io::Result<File> {
        leaf(name)?;
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            let name = std::ffi::CString::new(name.as_bytes())
                .map_err(|_| invalid("invalid replication metadata file name"))?;
            let access = if create {
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL
            } else {
                libc::O_RDONLY
            };
            open_at(
                &self.directory,
                &name,
                access | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC,
                0o600,
            )
        }
        #[cfg(not(unix))]
        {
            let mut options = OpenOptions::new();
            options.read(!create).write(create).create_new(create);
            #[cfg(windows)]
            {
                use std::os::windows::fs::OpenOptionsExt;
                use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;
                options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
            }
            options.open(self.path.join(name))
        }
    }

    pub(super) fn read(&self, name: &OsStr) -> io::Result<Vec<u8>> {
        let file = self.open_file(name, false)?;
        check_file(&file)?;
        let mut bytes = Vec::new();
        file.take(MAX_BYTES + 1).read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_BYTES {
            return Err(invalid("replication metadata is too large"));
        }
        Ok(bytes)
    }

    pub(super) fn write(
        &self,
        name: &OsStr,
        persisted: &PersistedChannel,
    ) -> Result<(), QueryError> {
        match self.open_file(name, false) {
            Ok(file) => {
                check_file(&file).map_err(|err| io_error("inspect replication metadata", err))?
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => (),
            Err(err) => return Err(io_error("inspect replication metadata", err)),
        }
        let seq = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let mut tmp = name.to_os_string();
        tmp.push(format!(".tmp.{}.{seq}", std::process::id()));
        // Create before installing the cleanup scope: EEXIST must not delete
        // a pre-existing file that this writer never owned.
        let mut file = self
            .open_file(&tmp, true)
            .map_err(|err| io_error("create replication metadata", err))?;
        let result = (|| {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                file.set_permissions(fs::Permissions::from_mode(0o600))
                    .map_err(|err| io_error("secure replication metadata", err))?;
            }
            crate::storage::metadata::write(
                &mut file,
                crate::storage::metadata::CHANNEL,
                crate::storage::format_versions::MYSQL_CHANNEL_VERSION,
                persisted,
            )
            .map_err(|err| {
                crate::storage::error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!("serialize replication metadata failed: {err}"),
                )
            })?;
            file.write_all(b"\n")
                .and_then(|_| file.sync_all())
                .map_err(|err| io_error("sync replication metadata", err))?;
            self.rename(&tmp, name)
                .map_err(|err| io_error("install replication metadata", err))?;
            self.directory
                .sync_all()
                .map_err(|err| io_error("sync replication metadata directory", err))
        })();
        if result.is_err() {
            let _ = self.remove(&tmp);
        }
        result
    }

    fn rename(&self, from: &OsStr, to: &OsStr) -> io::Result<()> {
        leaf(from)?;
        leaf(to)?;
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            use std::os::unix::ffi::OsStrExt;
            let from = std::ffi::CString::new(from.as_bytes())
                .map_err(|_| invalid("invalid metadata name"))?;
            let to = std::ffi::CString::new(to.as_bytes())
                .map_err(|_| invalid("invalid metadata name"))?;
            // SAFETY: both names are valid C strings and the directory fd is live.
            let result = unsafe {
                libc::renameat(
                    self.directory.as_raw_fd(),
                    from.as_ptr(),
                    self.directory.as_raw_fd(),
                    to.as_ptr(),
                )
            };
            if result == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        }
        #[cfg(not(unix))]
        {
            fs::rename(self.path.join(from), self.path.join(to))
        }
    }

    fn remove(&self, name: &OsStr) -> io::Result<()> {
        leaf(name)?;
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            use std::os::unix::ffi::OsStrExt;
            let name = std::ffi::CString::new(name.as_bytes())
                .map_err(|_| invalid("invalid metadata name"))?;
            // SAFETY: the directory fd is live and name is NUL-terminated.
            let result = unsafe { libc::unlinkat(self.directory.as_raw_fd(), name.as_ptr(), 0) };
            if result == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        }
        #[cfg(not(unix))]
        {
            fs::remove_file(self.path.join(name))
        }
    }
}

#[cfg(unix)]
fn open_at(
    parent: &File,
    name: &std::ffi::CStr,
    flags: libc::c_int,
    mode: libc::mode_t,
) -> io::Result<File> {
    use std::os::fd::{AsRawFd, FromRawFd};
    // SAFETY: name is NUL-terminated, parent remains open, and the returned fd
    // is transferred exactly once into an owning File.
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            flags,
            libc::c_uint::from(mode),
        )
    };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

#[cfg(windows)]
fn open_directory(path: &Path, no_follow: bool, write: bool) -> io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ, FILE_SHARE_WRITE,
    };
    let file = OpenOptions::new()
        .read(true)
        .write(write)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(
            FILE_FLAG_BACKUP_SEMANTICS
                | if no_follow {
                    FILE_FLAG_OPEN_REPARSE_POINT
                } else {
                    0
                },
        )
        .open(path)?;
    Ok(file)
}
