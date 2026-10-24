//! P0-A4 — disk-full / WAL I/O error → MySQL error code mapping.
//!
//! MySQL distinguishes the "ran out of disk" failure with active error code
//! 3675 ER_DISK_FULL_NOWAIT so operators can wire up dedicated alerts. Before
//! this helper, every mydb subsystem (WAL, binlog, replication metadata
//! …) collapsed `io::Error` straight into 1030 ER_GET_ERRNO, which made
//! ENOSPC indistinguishable from a generic transient I/O fault.
//!
//! The mapping checks `raw_os_error()` against the platform's ENOSPC
//! value (28 on Linux/macOS/BSDs) first, then falls back to a
//! lowercased substring scan ("no space left on device" /
//! "disk full") so synthetic / wrapped errors that lost the raw errno
//! still classify correctly.

use crate::types::QueryError;
use std::io;

/// ENOSPC value across the Unix family mydb targets. macOS / BSD / Linux
/// all share this number; we keep it as a single constant rather than
/// pulling `libc` for one value.
#[cfg(not(windows))]
const ENOSPC: i32 = 28;
#[cfg(windows)]
const ENOSPC: i32 = 112;

/// Classify an `io::Error` as ER_DISK_FULL_NOWAIT (3675) when it looks like a
/// no-space failure, otherwise return a generic 1030 ER_GET_ERRNO with
/// the caller-supplied `action` prefix.
pub fn map_io_error(action: &str, err: io::Error) -> QueryError {
    map_io_error_with_default(action, err, crate::error::catalog::ER_GET_ERRNO)
}

/// Same as [`map_io_error`] but lets the caller pick the fallback code
/// for non-ENOSPC failures. Used by subsystems whose historical error
/// code wasn't 1030 (binlog → 1105) — we don't want to silently change
/// their surface for ordinary I/O hiccups, only special-case the
/// disk-full path so 3675 surfaces correctly.
pub fn map_io_error_with_default(
    action: &str,
    err: io::Error,
    fallback: crate::error::ErrorSpec,
) -> QueryError {
    if is_disk_full(&err) {
        return crate::error::catalog::ER_DISK_FULL_NOWAIT.diagnostic(
            format!("Create table/tablespace '{action}' failed, as disk is full"),
            "MYDB-DIAG-DISK-FULL",
            format!("{action}: {err}"),
        );
    }
    let client_message = if fallback == crate::error::catalog::ER_ERROR_ON_WRITE {
        "Error writing file".to_string()
    } else {
        "Got error from storage engine".to_string()
    };
    fallback.diagnostic(client_message, "MYDB-DIAG-IO", format!("{action}: {err}"))
}

/// `true` when the error reports ENOSPC or carries text MySQL would
/// surface as "disk full". Public so tests in callers can assert the
/// same classification.
pub fn is_disk_full(err: &io::Error) -> bool {
    if err.raw_os_error() == Some(ENOSPC) {
        return true;
    }
    #[cfg(windows)]
    if matches!(err.raw_os_error(), Some(39 | 1295)) {
        return true;
    }
    let msg = err.to_string().to_lowercase();
    msg.contains("no space left on device")
        || msg.contains("disk full")
        || msg.contains("disk quota exceeded")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enospc_maps_to_3675() {
        let err = io::Error::from_raw_os_error(ENOSPC);
        let q = map_io_error("WAL write", err);
        assert_eq!(q.code(), 3675);
        assert_eq!(q.diagnostic_id(), Some("MYDB-DIAG-DISK-FULL"));
    }

    #[test]
    fn generic_io_maps_to_1030() {
        let err = io::Error::new(io::ErrorKind::PermissionDenied, "denied");
        let q = map_io_error("WAL write", err);
        assert_eq!(q.code(), 1030);
        assert_eq!(q.diagnostic_id(), Some("MYDB-DIAG-IO"));
    }

    #[test]
    fn no_space_substring_classifies_as_3675() {
        // Some platforms wrap ENOSPC into io::Error::new(Other, ...);
        // the substring scan keeps those classified correctly.
        let err = io::Error::new(
            io::ErrorKind::Other,
            "No space left on device (os error 28)",
        );
        let q = map_io_error("WAL write", err);
        assert_eq!(q.code(), 3675);
    }

    #[test]
    fn disk_quota_classifies_as_3675() {
        // EDQUOT — soft-quota exceeded; surfaces as disk full to users.
        let err = io::Error::new(io::ErrorKind::Other, "Disk quota exceeded");
        let q = map_io_error("WAL write", err);
        assert_eq!(q.code(), 3675);
    }

    #[test]
    fn empty_action_does_not_lose_message() {
        let err = io::Error::new(io::ErrorKind::InvalidData, "corrupt CRC");
        let q = map_io_error("", err);
        assert_eq!(q.message(), "Got error from storage engine");
        assert!(q.diagnostic_detail().unwrap().contains("corrupt CRC"));
    }
}
