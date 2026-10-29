//! Crash-safe progress for the native physical replica.
//!
//! The four watermarks deliberately describe different facts:
//! `received_lsn` reached the protocol decoder, `durable_lsn` reached the
//! local WAL, `applied_lsn` reached the live storage engine, and
//! `durable_applied_lsn` is covered by both apply and WAL durability.  Only
//! the last watermark is safe for a semi-sync acknowledgement or restart.
//! Written
//! atomically (write-tmp + rename) at a throttled checkpoint cadence
//! and forced on orderly shutdown.  A durable local WAL tail must never be
//! treated as applied inside a running process: apply can fail after append.

use std::fs;
use std::io::{Error, ErrorKind, Result as IoResult};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

pub const PROGRESS_FILE: &str = "system/replication/replica_progress.json";

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaProgress {
    #[serde(default)]
    pub received_lsn: u64,
    #[serde(default)]
    pub durable_lsn: u64,
    #[serde(default)]
    pub applied_lsn: u64,
    /// Backward-readable during the development-format transition; new files
    /// are always written with the unambiguous v2 field name.
    #[serde(default, alias = "last_applied_lsn")]
    pub durable_applied_lsn: u64,
    pub last_master_uuid: String,
    pub last_master_epoch: u64,
    #[serde(default)]
    pub cluster_uuid: String,
    #[serde(default)]
    pub timeline_uuid: String,
    #[serde(default)]
    pub source_current_lsn: u64,
    #[serde(default)]
    pub source_durable_lsn: u64,
    #[serde(default)]
    pub source_generation: u64,
    #[serde(default)]
    pub source_heartbeat_ms: u64,
    #[serde(default)]
    pub source_frozen: bool,
    #[serde(default)]
    pub source_fence_lsn: u64,
    #[serde(default)]
    pub source_fence_token_hash: String,
    pub last_update_ms: u64,
}

/// Advance only to records the live engine actually applied and the WAL
/// durably contains. Never infer application from the received WAL tail.
pub(crate) fn reconcile_applied(
    progress: &mut ReplicaProgress,
    db: &crate::storage::Database,
    log: &std::sync::Arc<crate::wal::LogMutex>,
) {
    if progress.last_master_uuid.is_empty() && progress.timeline_uuid.is_empty() {
        return;
    }
    let applied = db.replica_applied_lsn();
    let durable = applied.min(log.lock().durable_lsn());
    if durable > progress.durable_applied_lsn {
        progress.received_lsn = progress.received_lsn.max(durable);
        progress.durable_lsn = progress.durable_lsn.max(durable);
        progress.applied_lsn = progress.applied_lsn.max(applied);
        progress.durable_applied_lsn = durable;
    }
}

pub fn progress_path(data_dir: impl AsRef<Path>) -> PathBuf {
    data_dir.as_ref().join(PROGRESS_FILE)
}

pub fn load_progress(data_dir: impl AsRef<Path>) -> IoResult<Option<ReplicaProgress>> {
    let path = progress_path(data_dir);
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(&path)?;
    let progress: ReplicaProgress = crate::storage::metadata::decode(
        crate::storage::metadata::PROGRESS,
        crate::storage::format_versions::REPLICA_PROGRESS_VERSION,
        &bytes,
    )
    .map_err(|e| {
        Error::new(
            ErrorKind::InvalidData,
            format!("replica_progress.json: {e}"),
        )
    })?;
    Ok(Some(progress))
}

pub fn save_progress(data_dir: impl AsRef<Path>, progress: &ReplicaProgress) -> IoResult<()> {
    let path = progress_path(&data_dir);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("json.tmp");
    let bytes = crate::storage::metadata::encode(
        crate::storage::metadata::PROGRESS,
        crate::storage::format_versions::REPLICA_PROGRESS_VERSION,
        progress,
    )
    .map_err(|e| Error::new(ErrorKind::Other, format!("replica_progress.json: {e}")))?;
    // Crash-safe: fsync tmp + parent dir so the durable progress
    // watermark can't roll back on power loss (a rollback below the
    // master's retention floor would force a full base-backup re-seed).
    crate::repl::durable::atomic_write_sync(&path, &tmp, &bytes)?;
    Ok(())
}

/// Marker file flagging that this replica's WAL position can no longer
/// be served by the master (below retention floor, or diverged
/// timeline). The bootstrap path (`repl::maybe_bootstrap_replica`)
/// re-seeds via base backup on the next process start when present.
pub const RESYNC_MARKER_FILE: &str = "system/replication/resync_required.json";

pub fn resync_marker_path(data_dir: impl AsRef<Path>) -> PathBuf {
    data_dir.as_ref().join(RESYNC_MARKER_FILE)
}

pub fn write_resync_marker(data_dir: impl AsRef<Path>, reason: &str) -> IoResult<()> {
    let path = resync_marker_path(&data_dir);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("json.tmp");
    let body = serde_json::json!({ "reason": reason });
    crate::repl::durable::atomic_write_sync(&path, &tmp, &serde_json::to_vec_pretty(&body)?)?;
    Ok(())
}

pub fn clear_resync_marker(data_dir: impl AsRef<Path>) {
    let _ = fs::remove_file(resync_marker_path(data_dir));
}

pub fn resync_marker_reason(data_dir: impl AsRef<Path>) -> Option<String> {
    let bytes = fs::read(resync_marker_path(data_dir)).ok()?;
    let v: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    Some(
        v.get("reason")
            .and_then(|r| r.as_str())
            .unwrap_or("unknown")
            .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn load_returns_none_when_missing() {
        let dir = TempDir::new().unwrap();
        assert!(load_progress(dir.path()).unwrap().is_none());
    }

    #[test]
    fn save_load_round_trip_preserves_fields() {
        let dir = TempDir::new().unwrap();
        let p = ReplicaProgress {
            received_lsn: 12348,
            durable_lsn: 12347,
            applied_lsn: 12346,
            durable_applied_lsn: 12345,
            last_master_uuid: "master-uuid".into(),
            last_master_epoch: 7,
            cluster_uuid: "cluster".into(),
            timeline_uuid: "timeline".into(),
            source_current_lsn: 12349,
            source_durable_lsn: 12348,
            source_generation: 9,
            source_heartbeat_ms: 1000,
            source_frozen: true,
            source_fence_lsn: 12345,
            source_fence_token_hash: "hash".into(),
            last_update_ms: 1_700_000_000_000,
        };
        save_progress(dir.path(), &p).unwrap();
        let loaded = load_progress(dir.path()).unwrap().expect("present");
        assert_eq!(loaded, p);
    }

    #[test]
    fn resync_marker_round_trip_and_clear() {
        let dir = TempDir::new().unwrap();
        assert!(resync_marker_reason(dir.path()).is_none());
        write_resync_marker(dir.path(), "diverged timeline").unwrap();
        assert_eq!(
            resync_marker_reason(dir.path()).as_deref(),
            Some("diverged timeline")
        );
        clear_resync_marker(dir.path());
        assert!(resync_marker_reason(dir.path()).is_none());
    }

    #[test]
    fn save_is_atomic_via_tmp_rename() {
        let dir = TempDir::new().unwrap();
        let p = ReplicaProgress::default();
        save_progress(dir.path(), &p).unwrap();
        let path = progress_path(dir.path());
        let tmp = path.with_extension("json.tmp");
        assert!(path.exists());
        assert!(
            !tmp.exists(),
            "tmp must not linger after a successful rename"
        );
    }
}
