//! Role file (`<data-dir>/replication/role.json`) and Role enum.
//!
//! Each mydb instance carries a persistent role that survives restart:
//! `standalone` (single node, default), `master` (writable, ships WAL),
//! or `replica` (read-only, receives WAL). The role file is read at
//! startup and rewritten atomically (write-tmp + rename) on promote /
//! demote. The `MYDB_ROLE` env var, if set, overrides what's on disk and
//! the override is persisted with an epoch bump.

use crate::repl::now_ms;
use std::fs;
use std::io::{Error, ErrorKind, Result as IoResult};
use std::path::{Path, PathBuf};

use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const ROLE_FILE: &str = "system/replication/role.json";
pub const REPLICATION_METADATA_VERSION: u32 =
    crate::storage::format_versions::NATIVE_REPLICATION_METADATA_VERSION;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Standalone,
    Master,
    Replica,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Standalone => "standalone",
            Role::Master => "master",
            Role::Replica => "replica",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "standalone" | "auto" => Some(Role::Standalone),
            "master" | "primary" => Some(Role::Master),
            "replica" | "secondary" | "slave" => Some(Role::Replica),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RoleState {
    #[serde(default = "metadata_version")]
    pub metadata_version: u32,
    pub role: Role,
    pub node_uuid: String,
    #[serde(default)]
    pub cluster_uuid: String,
    #[serde(default)]
    pub timeline_uuid: String,
    #[serde(default)]
    pub parent_timeline_uuid: Option<String>,
    #[serde(default)]
    pub fork_lsn: u64,
    #[serde(default)]
    pub generation: u64,
    #[serde(default)]
    pub write_fence: Option<WriteFence>,
    #[serde(default)]
    pub promotion_pending: bool,
    #[serde(default)]
    pub promotion_target_lsn: Option<u64>,
    #[serde(default)]
    pub master_endpoint: Option<String>,
    pub epoch: u64,
    pub promoted_at_ms: u64,
    pub demoted_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WriteFence {
    pub token: String,
    pub fence_lsn: u64,
    pub created_at_ms: u64,
}

const fn metadata_version() -> u32 {
    REPLICATION_METADATA_VERSION
}

impl RoleState {
    pub fn new_standalone() -> Self {
        Self {
            metadata_version: REPLICATION_METADATA_VERSION,
            role: Role::Standalone,
            node_uuid: generate_uuid_string(),
            cluster_uuid: String::new(),
            timeline_uuid: String::new(),
            parent_timeline_uuid: None,
            fork_lsn: 0,
            generation: 0,
            write_fence: None,
            promotion_pending: false,
            promotion_target_lsn: None,
            master_endpoint: None,
            epoch: 0,
            promoted_at_ms: 0,
            demoted_at_ms: 0,
        }
    }

    pub fn promote_to_master_at(&mut self, fork_lsn: u64) {
        if self.cluster_uuid.is_empty() {
            self.cluster_uuid = generate_uuid_string();
        }
        let parent = (!self.timeline_uuid.is_empty()).then(|| self.timeline_uuid.clone());
        self.parent_timeline_uuid = parent;
        self.timeline_uuid = generate_uuid_string();
        self.fork_lsn = fork_lsn;
        self.role = Role::Master;
        self.epoch = self.epoch.saturating_add(1);
        self.generation = self.generation.saturating_add(1);
        self.promoted_at_ms = now_ms();
        self.master_endpoint = None;
        self.write_fence = None;
    }

    pub fn demote_to_replica(&mut self, master_endpoint: String) {
        self.role = Role::Replica;
        self.epoch = self.epoch.saturating_add(1);
        self.generation = self.generation.saturating_add(1);
        self.demoted_at_ms = now_ms();
        self.master_endpoint = Some(master_endpoint);
        self.write_fence = None;
    }
}

pub fn role_path(data_dir: impl AsRef<Path>) -> PathBuf {
    data_dir.as_ref().join(ROLE_FILE)
}

pub fn load(data_dir: impl AsRef<Path>) -> IoResult<Option<RoleState>> {
    let path = role_path(&data_dir);
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(&path)?;
    let state: RoleState = match serde_json::from_slice(&bytes) {
        Ok(state) => state,
        Err(err) => {
            if std::env::var("MYDB_REPAIR_ROLE").as_deref() != Ok("replica") {
                return Err(Error::new(ErrorKind::InvalidData, format!(
                    "{}: {err}; restore role metadata from backup, or explicitly set MYDB_REPAIR_ROLE=replica to quarantine it and start read-only without a source",
                    path.display()
                )));
            }
            let quarantine = path.with_extension(format!("json.corrupt-{}", now_ms()));
            // Keep the original authoritative file in place until the new
            // read-only role is durable; a crash must never look like a fresh
            // standalone installation.
            crate::repl::durable::atomic_write_sync(
                &quarantine,
                &quarantine.with_extension("tmp"),
                &bytes,
            )?;
            let mut state = RoleState::new_standalone();
            state.role = Role::Replica;
            save(&data_dir, &state)?;
            crate::util::logging::log_message(format_args!(
                "role metadata quarantined at {}; starting as unconfigured read-only replica",
                quarantine.display()
            ));
            state
        }
    };
    Ok(Some(state))
}

pub fn save(data_dir: impl AsRef<Path>, state: &RoleState) -> IoResult<()> {
    let path = role_path(&data_dir);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(state)
        .map_err(|e| Error::new(ErrorKind::Other, format!("role.json: {e}")))?;
    // Crash-safe: fsync tmp + parent dir so a promote/demote can't
    // silently roll back on power loss (half-promoted role).
    crate::repl::durable::atomic_write_sync(&path, &tmp, &bytes)?;
    Ok(())
}

/// Resolve the effective role for this boot: read role.json if it exists,
/// optionally apply an explicit override (typically from `MYDB_ROLE`), and
/// persist if either the file was missing or the override changed the role.
pub fn load_or_init_with_override(
    data_dir: impl AsRef<Path>,
    override_role: Option<Role>,
) -> IoResult<RoleState> {
    let state = match load(&data_dir)? {
        Some(state) => state,
        None => {
            let mut fresh = RoleState::new_standalone();
            if let Some(role) = override_role {
                fresh.role = role;
                if role == Role::Master {
                    fresh.cluster_uuid = generate_uuid_string();
                    fresh.timeline_uuid = generate_uuid_string();
                    fresh.generation = 1;
                    fresh.promoted_at_ms = now_ms();
                    fresh.epoch = 1;
                } else if role == Role::Replica {
                    fresh.demoted_at_ms = now_ms();
                    fresh.epoch = 1;
                }
            }
            save(&data_dir, &fresh)?;
            return Ok(fresh);
        }
    };
    if state.metadata_version != REPLICATION_METADATA_VERSION {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!(
                "replication metadata version {} is incompatible with version {}",
                state.metadata_version, REPLICATION_METADATA_VERSION
            ),
        ));
    }
    if let Some(role) = override_role {
        if role != state.role {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "MYDB_ROLE={} conflicts with persisted role={}; use MYDB_ROLE=auto and an explicit replication role-change command",
                    role.as_str(), state.role.as_str()
                ),
            ));
        }
    }
    Ok(state)
}

/// Convenience wrapper: pulls the override from `MYDB_ROLE`.
pub fn load_or_init(data_dir: impl AsRef<Path>) -> IoResult<RoleState> {
    let override_role = crate::util::env::var("MYDB_ROLE").ok().and_then(|v| {
        if v.trim().eq_ignore_ascii_case("auto") {
            None
        } else {
            Role::parse(&v)
        }
    });
    load_or_init_with_override(data_dir, override_role)
}

/// Role probe usable BEFORE the repl manager exists (Database::open /
/// recovery time). `MYDB_ROLE` overrides the on-disk role file, with
/// `auto`/`standalone` falling through to whatever role.json says —
/// the same precedence `load_or_init` applies later.
pub fn physical_replica_on_disk(data_dir: impl AsRef<Path>) -> bool {
    match crate::util::env::var("MYDB_ROLE")
        .ok()
        .and_then(|v| Role::parse(&v))
    {
        Some(Role::Replica) => true,
        Some(Role::Master) => false,
        _ => matches!(load(data_dir), Ok(Some(state)) if state.role == Role::Replica),
    }
}

pub fn generate_uuid_string() -> String {
    let bytes = generate_uuid_bytes();
    uuid_bytes_to_string(&bytes)
}

pub fn fence_token_hash(token: &str) -> String {
    let digest = Sha256::digest(token.as_bytes());
    crate::util::hex::encode_lower(&digest)
}

pub fn generate_uuid_bytes() -> [u8; 16] {
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    bytes
}

pub fn uuid_bytes_to_string(bytes: &[u8; 16]) -> String {
    crate::util::uuid::format(bytes)
}

pub fn uuid_string_to_bytes(value: &str) -> Option<[u8; 16]> {
    crate::util::uuid::parse(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn first_boot_creates_standalone_with_uuid() {
        let dir = TempDir::new().unwrap();
        let state = load_or_init_with_override(dir.path(), None).unwrap();
        assert_eq!(state.role, Role::Standalone);
        assert_eq!(state.epoch, 0);
        assert!(!state.node_uuid.is_empty());
        let again = load(dir.path()).unwrap().unwrap();
        assert_eq!(again, state);
    }

    #[test]
    fn explicit_role_override_initializes_but_cannot_flip_persisted_role() {
        let dir = TempDir::new().unwrap();
        let state1 = load_or_init_with_override(dir.path(), Some(Role::Master)).unwrap();
        assert_eq!(state1.role, Role::Master);
        assert_eq!(state1.epoch, 1);
        assert!(state1.promoted_at_ms > 0);

        let state2 = load_or_init_with_override(dir.path(), Some(Role::Master)).unwrap();
        assert_eq!(state2.role, Role::Master);
        assert_eq!(state2.epoch, 1, "same role override must not bump");

        let err = load_or_init_with_override(dir.path(), Some(Role::Replica)).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert!(err.to_string().contains("conflicts with persisted role"));
    }

    #[test]
    fn no_override_after_first_boot_preserves_state() {
        let dir = TempDir::new().unwrap();
        let s1 = load_or_init_with_override(dir.path(), Some(Role::Master)).unwrap();
        let s2 = load_or_init_with_override(dir.path(), None).unwrap();
        assert_eq!(s1, s2);
    }

    #[test]
    fn role_parse_aliases() {
        assert_eq!(Role::parse("MASTER"), Some(Role::Master));
        assert_eq!(Role::parse("primary"), Some(Role::Master));
        assert_eq!(Role::parse("Replica"), Some(Role::Replica));
        assert_eq!(Role::parse("slave"), Some(Role::Replica));
        assert_eq!(Role::parse("standalone"), Some(Role::Standalone));
        assert_eq!(Role::parse("auto"), Some(Role::Standalone));
        assert_eq!(Role::parse("xxx"), None);
    }

    #[test]
    fn uuids_are_distinct_and_well_formed() {
        let a = generate_uuid_string();
        let b = generate_uuid_string();
        assert_ne!(a, b);
        assert_eq!(a.len(), 36);
        assert_eq!(a.matches('-').count(), 4);
        let nibble = a.as_bytes()[14] as char;
        assert_eq!(nibble, '4');
    }

    #[test]
    fn uuid_byte_string_round_trip() {
        let bytes = generate_uuid_bytes();
        let s = uuid_bytes_to_string(&bytes);
        let parsed = uuid_string_to_bytes(&s).expect("parse");
        assert_eq!(bytes, parsed);
    }

    #[test]
    fn uuid_string_to_bytes_rejects_garbage() {
        assert!(uuid_string_to_bytes("not-a-uuid").is_none());
        assert!(uuid_string_to_bytes("zz").is_none());
        assert!(uuid_string_to_bytes("").is_none());
    }

    #[test]
    fn promote_demote_helpers_update_epoch_and_endpoint() {
        let mut s = RoleState::new_standalone();
        s.promote_to_master_at(42);
        assert_eq!(s.role, Role::Master);
        assert_eq!(s.epoch, 1);
        assert!(!s.cluster_uuid.is_empty());
        assert!(!s.timeline_uuid.is_empty());
        assert_eq!(s.fork_lsn, 42);
        assert!(s.master_endpoint.is_none());

        s.demote_to_replica("127.0.0.1:3316".to_string());
        assert_eq!(s.role, Role::Replica);
        assert_eq!(s.epoch, 2);
        assert_eq!(s.master_endpoint.as_deref(), Some("127.0.0.1:3316"));
    }
}
