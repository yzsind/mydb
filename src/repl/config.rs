//! Process-wide mydb-native replication configuration, parsed from
//! environment variables. Held by the global `MydbReplManager` so the
//! sender/receiver subsystems can consult it without re-parsing.

use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplConfig {
    /// Replication mode: `async` (default) or `semi_sync`.
    pub mode: ReplMode,
    /// Master-side TCP bind: `MYDB_REPL_BIND_HOST` + `MYDB_REPL_PORT`.
    pub bind_host: String,
    pub bind_port: u16,
    /// Replica-side target: `MYDB_REPL_MASTER_HOST` + `MYDB_REPL_MASTER_PORT`.
    pub master_host: Option<String>,
    pub master_port: u16,
    /// Semi-sync wait-count threshold (number of replicas that must ack
    /// before a commit returns OK on the master). Default 1.
    pub semi_sync_wait_count: u32,
    /// Timeout for the semi-sync wait before degrading to async.
    pub semi_sync_timeout: Duration,
    /// Heartbeat interval the master sends when no new WAL is available.
    pub heartbeat: Duration,
    /// Replica reconnect backoff after a dropped sender link.
    pub reconnect_backoff: Duration,
    /// Max bytes per `WalChunk` payload.
    pub chunk_bytes: usize,
    /// Sender-side backpressure window: bytes shipped but not yet
    /// acked by the replica before the sender stops reading WAL.
    pub max_inflight_bytes: u64,
    /// Hard upper bound on WAL retained for replication; over this we
    /// force-truncate and invalidate the laggard slot.
    pub max_retained_bytes: u64,
    /// Slot inactivity timeout — silent replica past this is marked
    /// `abandoned` and stops blocking WAL truncation.
    pub slot_timeout: Duration,
    /// Auto-start the replication subsystem on boot when the role file
    /// already says master/replica.
    pub auto_start: bool,
    pub tls_mode: ReplTlsMode,
    pub tls_ca: Option<PathBuf>,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    pub tls_server_name: Option<String>,
    pub max_connections: usize,
    pub max_base_backups: usize,
    pub apply_queue_bytes: usize,
    pub base_backup_disk_reserve_pct: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplMode {
    Async,
    SemiSync,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplTlsMode {
    Auto,
    Disabled,
    VerifyIdentity,
}

impl ReplTlsMode {
    fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "auto" => Some(Self::Auto),
            "disabled" | "off" => Some(Self::Disabled),
            "verify_identity" | "verify-identity" => Some(Self::VerifyIdentity),
            _ => None,
        }
    }
}

impl ReplMode {
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "async" | "asynchronous" => Some(ReplMode::Async),
            "semi_sync" | "semisync" | "semi-sync" => Some(ReplMode::SemiSync),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            ReplMode::Async => "async",
            ReplMode::SemiSync => "semi_sync",
        }
    }
}

impl Default for ReplConfig {
    fn default() -> Self {
        Self {
            mode: ReplMode::Async,
            // Safe-by-default: an unauthenticated/native listener must never
            // be exposed on every interface accidentally. Non-loopback mTLS
            // enforcement is layered on this in protocol v2.
            bind_host: "127.0.0.1".to_string(),
            bind_port: 3316,
            master_host: None,
            master_port: 3316,
            semi_sync_wait_count: 1,
            semi_sync_timeout: Duration::from_millis(10_000),
            heartbeat: Duration::from_millis(1_000),
            reconnect_backoff: Duration::from_millis(1_000),
            chunk_bytes: 1024 * 1024,
            max_inflight_bytes: 32 * 1024 * 1024,
            max_retained_bytes: 16 * 1024 * 1024 * 1024,
            slot_timeout: Duration::from_millis(300_000),
            auto_start: true,
            tls_mode: ReplTlsMode::Auto,
            tls_ca: None,
            tls_cert: None,
            tls_key: None,
            tls_server_name: None,
            max_connections: 16,
            max_base_backups: 1,
            apply_queue_bytes: 16 * 1024 * 1024,
            base_backup_disk_reserve_pct: 10,
        }
    }
}

impl ReplConfig {
    pub fn from_env() -> Self {
        let mut cfg = Self::default();
        if let Ok(v) = crate::util::env::var("MYDB_REPL_MODE") {
            if let Some(mode) = ReplMode::parse(&v) {
                cfg.mode = mode;
            }
        }
        if let Ok(v) = crate::util::env::var("MYDB_REPL_BIND_HOST") {
            if !v.trim().is_empty() {
                cfg.bind_host = v;
            }
        }
        if let Ok(Some(v)) = parse_env_u16("MYDB_REPL_PORT") {
            cfg.bind_port = v;
        }
        if let Ok(v) = crate::util::env::var("MYDB_REPL_MASTER_HOST") {
            let v = v.trim();
            if !v.is_empty() {
                cfg.master_host = Some(v.to_string());
            }
        }
        if let Ok(Some(v)) = parse_env_u16("MYDB_REPL_MASTER_PORT") {
            cfg.master_port = v;
        }
        if let Ok(Some(v)) = parse_env_u32("MYDB_REPL_SEMI_SYNC_WAIT_COUNT") {
            cfg.semi_sync_wait_count = v.max(1);
        }
        if let Ok(Some(v)) = parse_env_u64("MYDB_REPL_SEMI_SYNC_TIMEOUT_MS") {
            cfg.semi_sync_timeout = Duration::from_millis(v.max(1));
        }
        if let Ok(Some(v)) = parse_env_u64("MYDB_REPL_HEARTBEAT_MS") {
            cfg.heartbeat = Duration::from_millis(v.max(1));
        }
        if let Ok(Some(v)) = parse_env_u64("MYDB_REPL_RECONNECT_MS") {
            cfg.reconnect_backoff = Duration::from_millis(v.max(1));
        }
        if let Ok(Some(v)) = parse_env_u64("MYDB_REPL_CHUNK_BYTES") {
            cfg.chunk_bytes = (v as usize).clamp(
                4096,
                crate::repl::protocol::MAX_PAYLOAD_BYTES.saturating_sub(1024),
            );
        }
        if let Ok(Some(v)) = parse_env_u64("MYDB_REPL_MAX_INFLIGHT_BYTES") {
            cfg.max_inflight_bytes = v.max(4096);
        }
        if let Ok(Some(v)) = parse_env_u64("MYDB_REPL_MAX_RETAINED_BYTES") {
            cfg.max_retained_bytes = v;
        }
        if let Ok(Some(v)) = parse_env_u64("MYDB_REPL_SLOT_TIMEOUT_MS") {
            cfg.slot_timeout = Duration::from_millis(v);
        }
        if let Ok(v) = crate::util::env::var("MYDB_REPL_AUTO_START") {
            cfg.auto_start = parse_bool(&v);
        }
        if let Ok(v) = crate::util::env::var("MYDB_REPL_TLS_MODE") {
            if let Some(mode) = ReplTlsMode::parse(&v) {
                cfg.tls_mode = mode;
            }
        }
        cfg.tls_ca = crate::util::env::var("MYDB_REPL_TLS_CA")
            .ok()
            .map(PathBuf::from);
        cfg.tls_cert = crate::util::env::var("MYDB_REPL_TLS_CERT")
            .ok()
            .map(PathBuf::from);
        cfg.tls_key = crate::util::env::var("MYDB_REPL_TLS_KEY")
            .ok()
            .map(PathBuf::from);
        cfg.tls_server_name = crate::util::env::var("MYDB_REPL_TLS_SERVER_NAME").ok();
        if let Ok(Some(v)) = parse_env_u64("MYDB_REPL_MAX_CONNECTIONS") {
            cfg.max_connections = (v as usize).clamp(1, 1024);
        }
        if let Ok(Some(v)) = parse_env_u64("MYDB_REPL_MAX_BASE_BACKUPS") {
            cfg.max_base_backups = (v as usize).clamp(1, 16);
        }
        if let Ok(Some(v)) = parse_env_u64("MYDB_REPL_APPLY_QUEUE_BYTES") {
            cfg.apply_queue_bytes = (v as usize).clamp(64 * 1024, 1024 * 1024 * 1024);
        }
        if let Ok(Some(v)) = parse_env_u64("MYDB_REPL_BASE_BACKUP_DISK_RESERVE_PCT") {
            cfg.base_backup_disk_reserve_pct = (v as u8).min(90);
        }
        // The current IO→blocking-apply handoff is deliberately one chunk
        // deep. Keep that bounded handoff within the configured byte budget.
        cfg.chunk_bytes = cfg.chunk_bytes.min(cfg.apply_queue_bytes).max(4096);
        cfg
    }
}

fn parse_env_u16(name: &str) -> Result<Option<u16>, ()> {
    match crate::util::env::var(name) {
        Ok(v) => v.trim().parse().map(Some).map_err(|_| ()),
        Err(_) => Ok(None),
    }
}

fn parse_env_u32(name: &str) -> Result<Option<u32>, ()> {
    match crate::util::env::var(name) {
        Ok(v) => v.trim().parse().map(Some).map_err(|_| ()),
        Err(_) => Ok(None),
    }
}

fn parse_env_u64(name: &str) -> Result<Option<u64>, ()> {
    match crate::util::env::var(name) {
        Ok(v) => v.trim().parse().map(Some).map_err(|_| ()),
        Err(_) => Ok(None),
    }
}

fn parse_bool(v: &str) -> bool {
    matches!(
        v.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "on" | "yes" | "y"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_design() {
        let cfg = ReplConfig::default();
        assert_eq!(cfg.mode, ReplMode::Async);
        assert_eq!(cfg.bind_host, "127.0.0.1");
        assert_eq!(cfg.bind_port, 3316);
        assert!(cfg.master_host.is_none());
        assert_eq!(cfg.master_port, 3316);
        assert_eq!(cfg.semi_sync_wait_count, 1);
        assert_eq!(cfg.semi_sync_timeout, Duration::from_millis(10_000));
        assert_eq!(cfg.heartbeat, Duration::from_millis(1_000));
        assert_eq!(cfg.reconnect_backoff, Duration::from_millis(1_000));
        assert_eq!(cfg.chunk_bytes, 1024 * 1024);
        assert_eq!(cfg.max_inflight_bytes, 32 * 1024 * 1024);
        assert_eq!(cfg.max_retained_bytes, 16 * 1024 * 1024 * 1024);
        assert_eq!(cfg.slot_timeout, Duration::from_millis(300_000));
        assert!(cfg.auto_start);
        assert_eq!(cfg.tls_mode, ReplTlsMode::Auto);
        assert_eq!(cfg.max_connections, 16);
        assert_eq!(cfg.max_base_backups, 1);
        assert_eq!(cfg.apply_queue_bytes, 16 * 1024 * 1024);
        assert_eq!(cfg.base_backup_disk_reserve_pct, 10);
    }

    #[test]
    fn parse_mode_aliases() {
        assert_eq!(ReplMode::parse("async"), Some(ReplMode::Async));
        assert_eq!(ReplMode::parse("ASYNC"), Some(ReplMode::Async));
        assert_eq!(ReplMode::parse("asynchronous"), Some(ReplMode::Async));
        assert_eq!(ReplMode::parse("semi_sync"), Some(ReplMode::SemiSync));
        assert_eq!(ReplMode::parse("semisync"), Some(ReplMode::SemiSync));
        assert_eq!(ReplMode::parse("semi-sync"), Some(ReplMode::SemiSync));
        assert_eq!(ReplMode::parse("sync"), None);
    }
}

#[cfg(test)]
#[test]
fn audit_zero_semi_sync_timeout_is_clamped() {
    let config = crate::util::env::with_vars(
        &[("MYDB_REPL_SEMI_SYNC_TIMEOUT_MS", Some("0"))],
        ReplConfig::from_env,
    );
    assert_eq!(config.semi_sync_timeout, Duration::from_millis(1));
}
