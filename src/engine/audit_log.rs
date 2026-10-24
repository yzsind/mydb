//! P0-A2 phase 1 — Audit Log subsystem.
//!
//! Compliance-grade event log: every authenticated statement
//! attempt is written as a structured JSON line so security teams can
//! audit who did what and when without scraping the general / slow
//! logs. Off by default; turn on with `MYDB_AUDIT_LOG=<path>` (or set
//! `MYDB_AUDIT_LOG=on` to fall back to `<data_dir>/logs/audit.jsonl`).
//!
//! Phase 1 covers the high-leverage events callers actually need to
//! demonstrate compliance with SOC 2 / HIPAA / PCI-DSS log retention:
//!   - connect (handshake success / failure)
//!   - statement (every SQL statement, success / failure / error code)
//!   - disconnect (session close)
//!
//! Phase 2 will add row-level data-access auditing, table-level
//! filtering, and per-rule policy via `AUDIT_LOG_*` system variables.

use crate::engine::api::SessionContext;
use crate::types::QueryError;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};

use serde::Serialize;

/// Global audit log service. `None` when auditing is disabled (the
/// default). Set on engine startup via [`enable_audit_log`] which
/// reads `MYDB_AUDIT_LOG` from the environment.
static ACTIVE: OnceLock<Option<AuditLogService>> = OnceLock::new();

pub struct AuditLogService {
    path: PathBuf,
    file: Mutex<File>,
    enabled: AtomicBool,
}

impl AuditLogService {
    fn open(path: PathBuf) -> Result<Self, QueryError> {
        if crate::storage::image_io::immutable_path(&path) {
            return Err(crate::storage::image_io::read_only_error());
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|err| crate::util::io_error::map_io_error("audit log open", err))?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|err| crate::util::io_error::map_io_error("audit log open", err))?;
        Ok(Self {
            path,
            file: Mutex::new(file),
            enabled: AtomicBool::new(true),
        })
    }

    fn append_line<T: Serialize>(&self, row: &T) {
        if !self.enabled.load(Ordering::Acquire) {
            return;
        }
        let Ok(json) = serde_json::to_string(row) else {
            return;
        };
        if let Ok(mut file) = self.file.lock() {
            let _ = writeln!(file, "{json}");
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Serialize)]
struct AuditConnect<'a> {
    event_time: String,
    event: &'static str,
    user: &'a str,
    host: &'a str,
    connection_id: u32,
    status: &'static str,
    error_code: Option<u16>,
    error_message: Option<&'a str>,
}

#[derive(Debug, Serialize)]
struct AuditStatement<'a> {
    event_time: String,
    event: &'static str,
    user: &'a str,
    host: &'a str,
    connection_id: u32,
    db: &'a str,
    sql: String,
    status: &'static str,
    error_code: Option<u16>,
    error_message: Option<&'a str>,
    elapsed_ms: f64,
}

#[derive(Debug, Serialize)]
struct AuditDisconnect<'a> {
    event_time: String,
    event: &'static str,
    user: &'a str,
    host: &'a str,
    connection_id: u32,
}

/// Initialise the audit log from environment configuration. Called on
/// engine startup; idempotent — repeated calls after the first are
/// no-ops. Returns whether the audit log is now active.
pub fn enable_audit_log(data_dir: &Path) -> bool {
    let path_spec = match std::env::var("MYDB_AUDIT_LOG") {
        Ok(s) => s,
        Err(_) => return false,
    };
    let trimmed = path_spec.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("off") {
        return false;
    }
    let path = if trimmed.eq_ignore_ascii_case("on") {
        data_dir.join("logs").join("audit.jsonl")
    } else {
        PathBuf::from(trimmed)
    };
    let Ok(svc) = AuditLogService::open(path.clone()) else {
        eprintln!("audit log: failed to open {}", path.display());
        return false;
    };
    let _ = ACTIVE.set(Some(svc));
    true
}

fn active() -> Option<&'static AuditLogService> {
    ACTIVE.get().and_then(|opt| opt.as_ref())
}

/// True when the audit log is currently writing events. Cheap;
/// callers may probe this before formatting a full event payload.
pub fn is_active() -> bool {
    active()
        .map(|svc| svc.enabled.load(Ordering::Acquire))
        .unwrap_or(false)
}

fn now_iso() -> String {
    let now = chrono::Local::now();
    now.format("%Y-%m-%dT%H:%M:%S%.6f%:z").to_string()
}

pub fn record_connect(user: &str, host: &str, connection_id: u32, err: Option<&QueryError>) {
    let Some(svc) = active() else { return };
    svc.append_line(&AuditConnect {
        event_time: now_iso(),
        event: "connect",
        user,
        host,
        connection_id,
        status: if err.is_some() { "denied" } else { "ok" },
        error_code: err.map(|e| e.code()),
        error_message: err.map(QueryError::message),
    });
}

pub fn record_disconnect(user: &str, host: &str, connection_id: u32) {
    let Some(svc) = active() else { return };
    svc.append_line(&AuditDisconnect {
        event_time: now_iso(),
        event: "disconnect",
        user,
        host,
        connection_id,
    });
}

pub fn record_statement(
    session: &SessionContext,
    sql: &str,
    elapsed_ms: f64,
    err: Option<&QueryError>,
) {
    let Some(svc) = active() else { return };
    let truncated = crate::engine::logging::redact_sql_for_log(sql);
    svc.append_line(&AuditStatement {
        event_time: now_iso(),
        event: "statement",
        user: &session.user,
        host: &session.host,
        connection_id: session.connection_id,
        db: &session.current_database,
        sql: truncated,
        status: if err.is_some() { "error" } else { "ok" },
        error_code: err.map(|e| e.code()),
        error_message: err.map(QueryError::message),
        elapsed_ms,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn enable_audit_log_off_when_env_unset() {
        crate::util::env::with_vars(&[("MYDB_AUDIT_LOG", None)], || {
            // Don't poll active() — earlier tests may have set ACTIVE.
            // We just confirm enable_audit_log returns false.
            let tmp = tempfile::tempdir().unwrap();
            assert!(!enable_audit_log(tmp.path()));
        });
    }

    #[test]
    fn enable_audit_log_off_when_set_to_off() {
        crate::util::env::with_vars(&[("MYDB_AUDIT_LOG", Some("off"))], || {
            let tmp = tempfile::tempdir().unwrap();
            assert!(!enable_audit_log(tmp.path()));
        });
    }

    #[test]
    fn append_line_round_trip() {
        // Smoke-test the service in isolation (skipping the global
        // ACTIVE singleton so concurrent tests aren't entangled).
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("audit.jsonl");
        let svc = AuditLogService::open(path.clone()).unwrap();
        svc.append_line(&AuditDisconnect {
            event_time: "2026-06-14T00:00:00.000000+00:00".to_string(),
            event: "disconnect",
            user: "alice",
            host: "127.0.0.1",
            connection_id: 42,
        });
        let body = fs::read_to_string(&path).unwrap();
        assert!(body.contains("\"event\":\"disconnect\""));
        assert!(body.contains("\"user\":\"alice\""));
        assert!(body.contains("\"connection_id\":42"));
    }
}
