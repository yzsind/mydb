//! Positioned reads and staging-tree traversal shared by storage and backup.
use crate::platform::file::File;
use std::fs::{self, Metadata};
use std::io;
use std::path::Path;

#[cfg(any(unix, windows))]
#[inline]
pub(crate) fn read_exact_at_counted(
    file: &File,
    mut output: &mut [u8],
    mut offset: u64,
    retry_interrupted: bool,
    eof_message: Option<&'static str>,
) -> io::Result<u64> {
    let mut syscalls = 0;
    while !output.is_empty() {
        #[cfg(unix)]
        let read = std::os::unix::fs::FileExt::read_at(file, output, offset);
        #[cfg(windows)]
        let read = file.read_at(output, offset);
        let read = match read {
            Err(error) if retry_interrupted && error.kind() == io::ErrorKind::Interrupted => {
                continue
            }
            result => result?,
        };
        syscalls += 1;
        if read == 0 {
            return Err(eof_message.map_or_else(
                || io::ErrorKind::UnexpectedEof.into(),
                |message| io::Error::new(io::ErrorKind::UnexpectedEof, message),
            ));
        }
        output = &mut output[read..];
        offset = offset
            .checked_add(read as u64)
            .ok_or(io::ErrorKind::InvalidInput)?;
    }
    Ok(syscalls)
}

/// Begin kernel writeback of a written file range without waiting for
/// durability. Callers must retain their final fsync/publication barrier.
/// A zero `length` covers everything from `offset` to the end of the file.
pub(crate) fn start_file_writeback(file: &File, offset: u64, length: usize) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let offset = libc::off64_t::try_from(offset).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "writeback offset out of range")
        })?;
        let length = libc::off64_t::try_from(length).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "writeback length out of range")
        })?;
        // SAFETY: the descriptor is borrowed from a live File; the checked
        // range names only bytes and does not expose userspace pointers.
        if unsafe {
            libc::sync_file_range(
                file.as_raw_fd(),
                offset,
                length,
                libc::SYNC_FILE_RANGE_WRITE,
            )
        } != 0
        {
            let err = io::Error::last_os_error();
            if !matches!(
                err.raw_os_error(),
                Some(libc::EINVAL) | Some(libc::ENOSYS) | Some(libc::EOPNOTSUPP)
            ) {
                return Err(err);
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (file, offset, length);
    Ok(())
}

/// Write a complete staging image before publishing it by rename. The caller
/// owns the staging name, directory creation and directory-sync policy.
pub(crate) fn write_staged_file(
    path: &Path,
    staging: &Path,
    contents: &[u8],
    sync: bool,
) -> io::Result<()> {
    use std::io::Write;
    {
        let mut file = File::create(staging)?;
        file.write_all(contents)?;
        if sync {
            file.sync_all()?;
        }
    }
    fs::rename(staging, path)
}

/// Visit children before directories, rejecting symlinks before opening them.
/// The visitor decides whether special files are relevant and how to sync.
pub(crate) fn visit_tree_postorder(
    path: &Path,
    symlink_message: &'static str,
    visitor: &mut impl FnMut(&Path, &Metadata) -> io::Result<()>,
) -> io::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if crate::platform::fs::is_link(&metadata) {
        return Err(io::Error::new(io::ErrorKind::InvalidData, symlink_message));
    }
    if metadata.is_dir() {
        for entry in fs::read_dir(path)? {
            visit_tree_postorder(&entry?.path(), symlink_message, visitor)?;
        }
    }
    visitor(path, &metadata)
}

/// All test artifacts stay in a dedicated, platform-explicit root.
#[cfg(test)]
pub(crate) fn test_temp_dir() -> std::path::PathBuf {
    let root = std::env::var_os("MYDB_TEST_TMP_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            #[cfg(windows)]
            {
                std::path::PathBuf::from(r"C:\tmp\mydb")
            }
            #[cfg(not(windows))]
            {
                std::path::PathBuf::from("/tmp/mydb")
            }
        });
    fs::create_dir_all(&root).expect("create mydb test root");
    root
}

/// A filename inside a single-quoted SQL literal. Windows separators must
/// not become MySQL escape sequences, including under NO_BACKSLASH_ESCAPES.
#[cfg(test)]
pub(crate) fn sql_path(path: impl AsRef<Path>) -> String {
    let path = path.as_ref().to_string_lossy();
    #[cfg(windows)]
    let path = if let Some(unc) = path.strip_prefix(r"\\?\UNC\") {
        format!("//{}", unc.replace('\\', "/"))
    } else {
        path.strip_prefix(r"\\?\")
            .unwrap_or(&path)
            .replace('\\', "/")
    };
    #[cfg(not(windows))]
    let path = path.replace('\\', "\\\\");
    path.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom, Write};

    fn temporary_tree() -> tempfile::TempDir {
        tempfile::tempdir_in(test_temp_dir()).unwrap()
    }

    #[test]
    fn staged_publication_preserves_old_contents_until_rename() {
        let temp = temporary_tree();
        let live = temp.path().join("intent");
        let staging = temp.path().join("intent.pending");
        fs::write(&live, b"old").unwrap();
        let missing_parent = temp.path().join("missing/staging");
        assert!(write_staged_file(&live, &missing_parent, b"partial", true).is_err());
        assert_eq!(fs::read(&live).unwrap(), b"old");
        for sync in [false, true] {
            write_staged_file(&live, &staging, b"complete", sync).unwrap();
            assert_eq!(fs::read(&live).unwrap(), b"complete");
            assert!(!staging.exists());
        }
        let directory = temp.path().join("directory");
        fs::create_dir(&directory).unwrap();
        assert!(write_staged_file(&directory, &staging, b"unpublished", true).is_err());
        assert!(directory.is_dir());
        assert_eq!(fs::read(staging).unwrap(), b"unpublished");
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn positioned_reads_preserve_file_position_and_partial_eof() {
        let temp = temporary_tree();
        let mut file = File::create(temp.path().join("input")).unwrap();
        file.write_all(b"abcdef").unwrap();
        let mut file = File::open(temp.path().join("input")).unwrap();
        file.seek(SeekFrom::Start(2)).unwrap();
        let mut output = [0; 3];
        assert_eq!(
            read_exact_at_counted(&file, &mut output, 3, true, None).unwrap(),
            1
        );
        assert_eq!(&output, b"def");
        assert_eq!(file.stream_position().unwrap(), 2);
        let error =
            read_exact_at_counted(&file, &mut output, 5, false, Some("short backup")).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(error.to_string(), "short backup");
        assert_eq!(output[0], b'f');
        assert_eq!(
            read_exact_at_counted(&file, &mut [], 99, false, None).unwrap(),
            0
        );
    }

    #[test]
    fn tree_visit_keeps_directory_publication_after_children() {
        let temp = temporary_tree();
        fs::create_dir(temp.path().join("dir")).unwrap();
        fs::write(temp.path().join("dir/file"), b"payload").unwrap();
        let mut visited = Vec::new();
        visit_tree_postorder(temp.path(), "symlink", &mut |path, _| {
            visited.push(path.to_path_buf());
            Ok(())
        })
        .unwrap();
        assert_eq!(
            visited,
            [
                temp.path().join("dir/file"),
                temp.path().join("dir"),
                temp.path().into()
            ]
        );
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(temp.path().join("dir"), temp.path().join("link")).unwrap();
            let error = visit_tree_postorder(temp.path(), "symlink in staging", &mut |_, _| Ok(()))
                .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert_eq!(error.to_string(), "symlink in staging");
        }
    }
}

/// Copy private fixture contents without sharing mutable file identities.
/// APFS clones share extents until a write; other filesystems use a byte copy.
#[cfg(test)]
pub(crate) fn copy_fixture_file(source: &Path, destination: &Path) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        unsafe extern "C" {
            fn clonefile(
                source: *const libc::c_char,
                destination: *const libc::c_char,
                flags: u32,
            ) -> libc::c_int;
        }
        let source = CString::new(source.as_os_str().as_bytes()).map_err(io::Error::other)?;
        let destination =
            CString::new(destination.as_os_str().as_bytes()).map_err(io::Error::other)?;
        // SAFETY: both C strings remain alive for the entire syscall.
        if unsafe { clonefile(source.as_ptr(), destination.as_ptr(), 0) } == 0 {
            return Ok(());
        }
    }
    fs::copy(source, destination).map(|_| ())
}
