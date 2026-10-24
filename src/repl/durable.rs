//! Crash-safe atomic file writes for replication metadata
//! (`role.json`, `replica_progress.json`, `<uuid>.slot`).
//!
//! The prior implementation used `fs::write(tmp) + fs::rename` which
//! guarantees the target is never *torn* but does NOT guarantee the
//! rename (or the tmp's bytes) reached stable storage before a power
//! loss. A crash could then leave the target holding stale contents, or
//! — worse — a renamed-but-unflushed empty/partial file. For role.json
//! that means a promote can silently roll back to `replica` on reboot
//! (half-promoted); for progress it means replaying a segment that may
//! already be past the master's retention floor (forced re-seed).
//!
//! [`atomic_write_sync`] closes the gap: fsync the tmp before the
//! rename, then fsync the parent directory so the rename entry itself is
//! durable — the standard write-tmp → fsync → rename → fsync-dir dance
//! used by the WAL/undo layers.

use std::fs::{self, OpenOptions};
use std::io::{Result as IoResult, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// Atomically and durably write `bytes` to `path`, staging through
/// a unique file derived from `tmp`. On return, a crash recovers either the
/// complete previous file or the complete new file — never a torn or
/// empty one.
pub fn atomic_write_sync(path: &Path, tmp: &Path, bytes: &[u8]) -> IoResult<()> {
    static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);
    // Concurrent writers must not truncate or rename each other's staging
    // file. create_new also protects against stale files after PID reuse.
    let (staging, mut file) = loop {
        let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let mut name = tmp.as_os_str().to_os_string();
        name.push(format!(".{}.{sequence}.tmp", std::process::id()));
        let staging = PathBuf::from(name);
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staging)
        {
            Ok(file) => break (staging, file),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(err),
        }
    };
    let result = (|| {
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&staging, path)?;
        if let Some(parent) = path.parent() {
            crate::platform::fs::sync_path(parent)?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&staging);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn atomic_write_sync_replaces_contents_and_removes_tmp() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("f.json");
        let tmp = path.with_extension("json.tmp");
        atomic_write_sync(&path, &tmp, b"first").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"first");
        assert!(!tmp.exists(), "tmp must not linger");
        // Overwrite in place.
        atomic_write_sync(&path, &tmp, b"second-longer").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"second-longer");
        assert!(!tmp.exists());
    }
}

#[cfg(test)]
mod audit1022_atomic_tests {
    use super::*;
    #[test]
    fn audit1022_concurrent_metadata_writers_share_only_the_final_path() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let barrier = std::sync::Barrier::new(4);
        std::thread::scope(|scope| {
            for worker in 0..4 {
                let path = dir.path().join("marker.json");
                let tmp = path.with_extension("json.tmp");
                let barrier = &barrier;
                scope.spawn(move || {
                    for iteration in 0..16 {
                        barrier.wait();
                        let bytes=serde_json::to_vec(&serde_json::json!({"writer":worker,"iteration":iteration,"payload":"x".repeat(4096)})).unwrap();
                        atomic_write_sync(&path,&tmp,&bytes).unwrap();
                        let _:serde_json::Value=serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
                    }
                });
            }
        });
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }
}
