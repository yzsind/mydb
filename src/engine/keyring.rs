//! P0-A1 phase 1 — in-process keyring backend.
//!
//! mydb does not yet encrypt page files / WAL at rest, but applications
//! that build on the MySQL Enterprise keyring component
//! (KEYRING_KEY_GENERATE / FETCH / STORE / REMOVE …) need somewhere to
//! park keys. This phase exposes the same SQL surface backed by a
//! process-global in-memory map. Real keys are not yet wired to the
//! storage encryption path — that is phase 2 (page-level AES-CBC +
//! key rotation + on-disk encrypted keyring file).
//!
//! Threat model for phase 1: this gives applications a stable API and
//! deterministic round-trip semantics so they can build / test
//! encryption code paths against mydb. It is NOT a hardened keystore
//! — keys live in process memory and are lost on restart. Documented
//! in docs/encryption-at-rest-roadmap.md once phase 2 lands.

use crate::storage::error;
use crate::types::QueryError;
use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

/// One keyring entry. `key_type` is the MySQL-defined algorithm tag
/// ("AES", "RSA", "DSA", "ECDSA"); only "AES" is exercised by phase 1
/// callers but we accept the others verbatim.
#[derive(Debug, Clone)]
pub struct KeyringEntry {
    pub key_type: String,
    pub key_bytes: Vec<u8>,
}

const MAX_KEYRING_ENTRIES: usize = 1024;
const MAX_KEYRING_BYTES: usize = 16 * 1024 * 1024;

fn quota_error() -> QueryError {
    error(
        crate::error::catalog::ER_KEYRING_UDF_KEYRING_SERVICE_ERROR,
        "keyring memory quota exceeded (1024 keys, 16 MiB total)",
    )
}

#[derive(Default)]
struct KeyringStore {
    entries: HashMap<String, KeyringEntry>,
    bytes: usize,
}

impl KeyringStore {
    fn insert(
        &mut self,
        key_id: &str,
        key_type: &str,
        key_bytes: Vec<u8>,
    ) -> Result<(), QueryError> {
        let bytes = key_id
            .len()
            .saturating_add(key_type.len())
            .saturating_add(key_bytes.len());
        if key_id.len() > 255
            || key_type.len() > 64
            || key_bytes.len() > 64 * 1024
            || self.entries.len() >= MAX_KEYRING_ENTRIES
            || self.bytes.saturating_add(bytes) > MAX_KEYRING_BYTES
        {
            return Err(quota_error());
        }
        self.bytes += bytes;
        self.entries.insert(
            key_id.to_string(),
            KeyringEntry {
                key_type: key_type.to_string(),
                key_bytes,
            },
        );
        Ok(())
    }

    fn remove(&mut self, key_id: &str) -> bool {
        if let Some(entry) = self.entries.remove(key_id) {
            self.bytes = self
                .bytes
                .saturating_sub(key_id.len() + entry.key_type.len() + entry.key_bytes.len());
            true
        } else {
            false
        }
    }
}

fn store() -> &'static RwLock<KeyringStore> {
    static STORE: OnceLock<RwLock<KeyringStore>> = OnceLock::new();
    STORE.get_or_init(|| RwLock::new(KeyringStore::default()))
}

pub fn generate(key_id: &str, key_type: &str, length_bits: u32) -> Result<bool, QueryError> {
    if key_id.is_empty() {
        return Err(error(
            crate::error::catalog::ER_CLIENT_KEYRING_UDF_KEY_INVALID,
            "Function 'keyring_key_generate' failed because key is invalid.",
        ));
    }
    if key_id.len() > 255 || key_type.len() > 64 {
        return Err(quota_error());
    }
    let len_bytes = (length_bits as usize).saturating_div(8);
    if len_bytes == 0 || len_bytes > 64 * 1024 {
        let spec = if len_bytes == 0 {
            crate::error::catalog::ER_CLIENT_KEYRING_UDF_KEY_INVALID
        } else {
            crate::error::catalog::ER_CLIENT_KEYRING_UDF_KEY_TOO_LONG
        };
        return Err(error(
            spec,
            "Function 'keyring_key_generate' failed because key is invalid.",
        ));
    }
    let mut guard = store().write().map_err(|_| {
        error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            "keyring lock poisoned",
        )
    })?;
    if guard.entries.contains_key(key_id) {
        return Err(error(
            crate::error::catalog::ER_KEYRING_UDF_KEYRING_SERVICE_ERROR,
            "Function 'keyring_key_generate' failed because underlying keyring service returned an error. Please check if a keyring is installed and that provided arguments are valid for the keyring you are using.",
        ));
    }
    // Pseudo-random bytes: hash the key_id + length + a process-local
    // counter so generate() is deterministic per-process (callers can
    // rely on stable round trips for tests) but distinct keys still
    // get distinct bytes.
    let mut bytes = Vec::with_capacity(len_bytes);
    let mut counter: u32 = guard.entries.len() as u32;
    while bytes.len() < len_bytes {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(key_id.as_bytes());
        hasher.update(&counter.to_le_bytes());
        let v = hasher.finalize();
        bytes.extend_from_slice(&v.to_le_bytes());
        counter = counter.wrapping_add(1);
    }
    bytes.truncate(len_bytes);
    guard.insert(key_id, key_type, bytes)?;
    Ok(true)
}

pub fn store_key(key_id: &str, key_type: &str, key: &[u8]) -> Result<bool, QueryError> {
    if key_id.is_empty() {
        return Err(error(
            crate::error::catalog::ER_CLIENT_KEYRING_UDF_KEY_INVALID,
            "Function 'keyring_key_store' failed because key is invalid.",
        ));
    }
    if key.is_empty() {
        return Err(error(
            crate::error::catalog::ER_CLIENT_KEYRING_UDF_KEY_INVALID,
            "Function 'keyring_key_store' failed because key is invalid.",
        ));
    }
    if key_id.len() > 255 || key_type.len() > 64 || key.len() > 64 * 1024 {
        return Err(quota_error());
    }
    let mut guard = store().write().map_err(|_| {
        error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            "keyring lock poisoned",
        )
    })?;
    if guard.entries.contains_key(key_id) {
        return Err(error(
            crate::error::catalog::ER_KEYRING_UDF_KEYRING_SERVICE_ERROR,
            "Function 'keyring_key_store' failed because underlying keyring service returned an error. Please check if a keyring is installed and that provided arguments are valid for the keyring you are using.",
        ));
    }
    guard.insert(key_id, key_type, key.to_vec())?;
    Ok(true)
}

pub fn fetch(key_id: &str) -> Option<KeyringEntry> {
    let guard = store().read().ok()?;
    guard.entries.get(key_id).cloned()
}

pub fn remove(key_id: &str) -> Result<bool, QueryError> {
    let mut guard = store().write().map_err(|_| {
        error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            "keyring lock poisoned",
        )
    })?;
    Ok(guard.remove(key_id))
}

pub fn length(key_id: &str) -> Option<u32> {
    fetch(key_id).map(|e| (e.key_bytes.len() * 8) as u32)
}

pub fn key_type(key_id: &str) -> Option<String> {
    fetch(key_id).map(|e| e.key_type)
}

pub(crate) fn snapshot_ids() -> Vec<String> {
    let Ok(guard) = store().read() else {
        return Vec::new();
    };
    let mut ids = guard.entries.keys().cloned().collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

#[cfg(test)]
mod tests {
    use super::*;

    fn purge(prefix: &str) {
        if let Ok(mut g) = store().write() {
            let keys: Vec<_> = g
                .entries
                .keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect();
            for key in keys {
                g.remove(&key);
            }
        }
    }

    #[test]
    fn generate_then_fetch_round_trip() {
        purge("rt_");
        assert!(generate("rt_aes_1", "AES", 256).unwrap());
        let key = fetch("rt_aes_1").unwrap();
        assert_eq!(key.key_type, "AES");
        assert_eq!(key.key_bytes.len(), 32);
        assert_eq!(length("rt_aes_1"), Some(256));
        assert_eq!(key_type("rt_aes_1").as_deref(), Some("AES"));
    }

    #[test]
    fn generate_duplicate_errors() {
        purge("dup_");
        generate("dup_key", "AES", 128).unwrap();
        let err = generate("dup_key", "AES", 128).unwrap_err();
        assert_eq!(err.code(), 3188);
    }

    #[test]
    fn store_then_fetch_round_trip() {
        purge("st_");
        let blob = vec![0x12, 0x34, 0x56, 0x78];
        store_key("st_aes_1", "AES", &blob).unwrap();
        let got = fetch("st_aes_1").unwrap();
        assert_eq!(got.key_bytes, blob);
    }

    #[test]
    fn store_enforces_both_quotas_and_reuses_removed_capacity() {
        assert!(store_key("overlarge-key", "AES", &vec![0; 64 * 1024 + 1]).is_err());
        let mut store = KeyringStore::default();
        for id in 0..MAX_KEYRING_ENTRIES {
            store
                .insert(&format!("key-{id}"), "AES", vec![0; 16])
                .unwrap();
        }
        assert!(store.insert("one-too-many", "AES", vec![0; 16]).is_err());
        assert!(store.remove("key-0"));
        store.insert("replacement", "AES", vec![0; 16]).unwrap();

        let mut store = KeyringStore::default();
        for id in 0..(MAX_KEYRING_BYTES / (64 * 1024)) {
            let result = store.insert(&format!("large-{id}"), "AES", vec![0; 64 * 1024]);
            if result.is_err() {
                assert!(store.bytes <= MAX_KEYRING_BYTES);
                return;
            }
        }
        panic!("the byte quota did not reject a new key");
    }

    #[test]
    fn remove_returns_true_on_first_drop_false_after() {
        purge("rm_");
        store_key("rm_a", "AES", &[0]).unwrap();
        assert!(remove("rm_a").unwrap());
        assert!(!remove("rm_a").unwrap());
    }

    #[test]
    fn invalid_length_rejected() {
        purge("invalid_");
        let err = generate("invalid_zero", "AES", 0).unwrap_err();
        assert_eq!(err.code(), 3930);
    }

    #[test]
    fn empty_key_id_rejected() {
        let err = generate("", "AES", 128).unwrap_err();
        assert_eq!(err.code(), 3930);
        let err = store_key("", "AES", &[1]).unwrap_err();
        assert_eq!(err.code(), 3930);
    }
}
