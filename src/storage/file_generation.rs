//! Bounded reopening with an inode identity proof and optional owned hard link.
use crate::platform::file::File;
use std::{
    io,
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Identity(pub u64, pub u64);
fn identity(file: &File) -> io::Result<Identity> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = file.metadata()?;
        Ok(Identity(metadata.dev(), metadata.ino()))
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{
            GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
        };
        // SAFETY: the borrowed file handle is live and the output has its exact ABI layout.
        unsafe {
            let mut info: BY_HANDLE_FILE_INFORMATION = std::mem::zeroed();
            if GetFileInformationByHandle(file.as_raw_handle(), &mut info) == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(Identity(
                u64::from(info.dwVolumeSerialNumber),
                (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow),
            ))
        }
    }
}
pub(crate) enum GenerationFile {
    Open(File),
    Reopen {
        path: PathBuf,
        identity: Identity,
        minimum_len: u64,
        _owner: Option<Arc<dyn Send + Sync>>,
    },
}
impl std::fmt::Debug for GenerationFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Open(file) => f.debug_tuple("Open").field(file).finish(),
            Self::Reopen { path, identity, .. } => f
                .debug_struct("Reopen")
                .field("path", path)
                .field("identity", identity)
                .finish(),
        }
    }
}
pub(crate) enum GenerationHandle<'a> {
    Borrowed(&'a File),
    Owned(File),
}
impl Deref for GenerationHandle<'_> {
    type Target = File;
    fn deref(&self) -> &File {
        match self {
            Self::Borrowed(file) => file,
            Self::Owned(file) => file,
        }
    }
}
impl GenerationFile {
    /// Immutable paths are protected by their repository/image lease. Mutable
    /// sources instead use pin(), so a legitimate rename cannot lose the inode.
    pub fn immutable(path: PathBuf, file: File, minimum_len: u64) -> io::Result<Self> {
        Ok(Self::Reopen {
            path,
            identity: identity(&file)?,
            minimum_len,
            _owner: None,
        })
    }
    pub fn pin(
        source: &Path,
        file: File,
        path: PathBuf,
        minimum_len: u64,
        owner: Arc<dyn Send + Sync>,
    ) -> io::Result<Self> {
        let expected = identity(&file)?;
        std::fs::hard_link(source, &path)?;
        let pinned = File::open(&path)?;
        if identity(&pinned)? != expected {
            return Err(io::Error::other("file generation changed while pinning"));
        }
        Ok(Self::Reopen {
            path,
            identity: expected,
            minimum_len,
            _owner: Some(owner),
        })
    }
    pub fn open(&self) -> io::Result<GenerationHandle<'_>> {
        match self {
            Self::Open(file) => Ok(GenerationHandle::Borrowed(file)),
            Self::Reopen {
                path,
                identity: expected,
                minimum_len,
                ..
            } => {
                if std::fs::symlink_metadata(path)?.file_type().is_symlink() {
                    return Err(io::Error::other("symlink generation reference"));
                }
                let file = File::open(path)?;
                if identity(&file)? != *expected || file.metadata()?.len() < *minimum_len {
                    return Err(io::Error::other("file generation/EOF proof changed"));
                }
                Ok(GenerationHandle::Owned(file))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pinned_reopen_keeps_original_inode_after_source_replacement() {
        let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let source = root.path().join("source");
        let pin = root.path().join("pin");
        std::fs::write(&source, b"old").unwrap();
        let file = GenerationFile::pin(&source, File::open(&source).unwrap(), pin, 3, Arc::new(()))
            .unwrap();
        std::fs::remove_file(&source).unwrap();
        std::fs::write(&source, b"new").unwrap();
        let handle = file.open().unwrap();
        let mut bytes = [0; 3];
        crate::util::file::read_exact_at_counted(&handle, &mut bytes, 0, false, None).unwrap();
        assert_eq!(&bytes, b"old");
    }
    #[test]
    fn immutable_reopen_rejects_same_length_replacement_and_truncation() {
        let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let path = root.path().join("object");
        std::fs::write(&path, b"old").unwrap();
        let file = GenerationFile::immutable(path.clone(), File::open(&path).unwrap(), 3).unwrap();
        std::fs::rename(&path, root.path().join("old-inode")).unwrap();
        std::fs::write(&path, b"new").unwrap();
        assert!(file.open().is_err());
        let new = GenerationFile::immutable(path.clone(), File::open(&path).unwrap(), 3).unwrap();
        std::fs::write(path, b"x").unwrap();
        assert!(new.open().is_err());
    }
}
