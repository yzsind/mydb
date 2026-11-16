//! Recoverable credentials use a separate, durable keyring from SQL KEYRING_*.
use super::error;
use crate::types::QueryError;
use aes_gcm::{
    aead::{Aead, KeyInit, Payload},
    Aes256Gcm, Nonce,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use rand::{rngs::OsRng, RngCore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, path::Path};
use zeroize::{Zeroize, Zeroizing};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Envelope {
    pub version: u32,
    pub master_id: String,
    pub wrapped_dek: Vec<u8>,
    pub nonce: Vec<u8>,
    pub ciphertext: Vec<u8>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyFile {
    version: u32,
    active_key_id: String,
    keys: BTreeMap<String, String>,
}
impl Drop for KeyFile {
    fn drop(&mut self) {
        for value in self.keys.values_mut() {
            value.zeroize();
        }
    }
}
pub struct Keys {
    pub active: String,
    pub keys: BTreeMap<String, Zeroizing<Vec<u8>>>,
}
fn id(key: &[u8]) -> String {
    let mut hash = Sha256::new();
    hash.update(b"mydb.rest.ak.kek.v1\0");
    hash.update(key);
    hash.finalize().iter().map(|b| format!("{b:02x}")).collect()
}
fn decode(value: &str) -> Result<Zeroizing<Vec<u8>>, QueryError> {
    let bytes = if let Some(v) = value.strip_prefix("base64:") {
        STANDARD.decode(v).ok()
    } else if let Some(v) = value.strip_prefix("hex:") {
        if v.len() == 64 && v.bytes().all(|b| b.is_ascii_hexdigit()) {
            (0..64)
                .step_by(2)
                .map(|i| u8::from_str_radix(&v[i..i + 2], 16).ok())
                .collect()
        } else {
            None
        }
    } else {
        None
    };
    bytes
        .filter(|v| v.len() == 32)
        .map(Zeroizing::new)
        .ok_or_else(|| {
            error("API key master key must encode 32 random bytes using base64: or hex:")
        })
}
fn read(path: &Path) -> Result<Keys, QueryError> {
    use std::io::Read;
    let mut opts = std::fs::OpenOptions::new();
    opts.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.custom_flags(libc::O_NOFOLLOW);
    }
    #[cfg(not(unix))]
    {
        return Err(error("Use MYDB_REST_ENCRYPT_KEY on this platform"));
    }
    let file = opts
        .open(path)
        .map_err(|_| error("Cannot open API key master keyring"))?;
    let meta = file
        .metadata()
        .map_err(|_| error("Cannot inspect API key master keyring"))?;
    if !meta.is_file() || meta.len() > 1024 * 1024 {
        return Err(error("Invalid API key master keyring file"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if meta.mode() & 0o077 != 0 || meta.uid() != rustix::process::geteuid().as_raw() {
            return Err(error(
                "API key keyring must be owned by the server with mode 0600",
            ));
        }
    }
    let mut data = Zeroizing::new(Vec::new());
    file.take(1024 * 1024 + 1)
        .read_to_end(&mut data)
        .map_err(|_| error("Cannot read API key keyring"))?;
    let mut f: KeyFile =
        serde_json::from_slice(&data).map_err(|_| error("Invalid API key keyring"))?;
    if f.version != 1 {
        return Err(error("Unsupported API key keyring version"));
    }
    let mut keys = BTreeMap::new();
    for (name, value) in std::mem::take(&mut f.keys) {
        let value = Zeroizing::new(value);
        let key = decode(&value)?;
        if name != id(&key) {
            return Err(error("API key master key ID mismatch"));
        }
        keys.insert(name, key);
    }
    if !keys.contains_key(&f.active_key_id) {
        return Err(error("Active API key master key missing"));
    }
    Ok(Keys {
        active: f.active_key_id.clone(),
        keys,
    })
}
pub fn load(dir: &Path, create: bool) -> Result<Keys, QueryError> {
    match std::env::var("MYDB_REST_ENCRYPT_KEY") {
        Ok(value) => {
            let value = Zeroizing::new(value);
            let key = decode(&value)?;
            let active = id(&key);
            let mut keys =
                if let Ok(path) = crate::startup_config::var("MYDB_REST_PREVIOUS_KEYS_FILE") {
                    read(Path::new(&path))?.keys
                } else {
                    BTreeMap::new()
                };
            keys.insert(active.clone(), key);
            return Ok(Keys { active, keys });
        }
        Err(std::env::VarError::NotPresent) => {}
        Err(_) => return Err(error("Invalid API key master key encoding")),
    }
    let path = dir.join("rest-keys.mydb");
    if path.symlink_metadata().is_err() && create {
        #[cfg(not(unix))]
        return Err(error("Use MYDB_REST_ENCRYPT_KEY on this platform"));
        let mut key = Zeroizing::new(vec![0; 32]);
        OsRng
            .try_fill_bytes(&mut key)
            .map_err(|_| error("Secure random source unavailable"))?;
        let name = id(&key);
        let f = KeyFile {
            version: 1,
            active_key_id: name.clone(),
            keys: BTreeMap::from([(name, format!("base64:{}", STANDARD.encode(&key)))]),
        };
        let bytes = Zeroizing::new(
            serde_json::to_vec(&f).map_err(|_| error("Cannot encode API key keyring"))?,
        );
        let tmp = dir.join(format!(".rest-key-{}", super::random_id()?));
        let result = (|| -> std::io::Result<()> {
            use std::io::Write;
            let mut opts = std::fs::OpenOptions::new();
            opts.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                opts.mode(0o600).custom_flags(libc::O_NOFOLLOW);
            }
            let mut file = opts.open(&tmp)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            match std::fs::hard_link(&tmp, &path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(e) => return Err(e),
            }
            std::fs::File::open(dir)?.sync_all()
        })();
        let _ = std::fs::remove_file(tmp);
        result.map_err(|_| error("Cannot durably create API key keyring"))?;
    }
    read(&path)
}
fn aad(key_id: &str) -> Vec<u8> {
    [b"mydb.rest.ak.payload.v1\0".as_slice(), key_id.as_bytes()].concat()
}
pub fn encrypt(keys: &Keys, key_id: &str, value: &[u8]) -> Result<Envelope, QueryError> {
    let mut dek = Zeroizing::new(vec![0; 32]);
    let mut nonce = vec![0; 12];
    OsRng
        .try_fill_bytes(&mut dek)
        .and_then(|_| OsRng.try_fill_bytes(&mut nonce))
        .map_err(|_| error("Secure random source unavailable"))?;
    let cipher = Aes256Gcm::new_from_slice(&dek).map_err(|_| error("Invalid API key data key"))?;
    let ciphertext = cipher
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: value,
                aad: &aad(key_id),
            },
        )
        .map_err(|_| error("Cannot encrypt API key"))?;
    let kek = keys
        .keys
        .get(&keys.active)
        .ok_or_else(|| error("API key master key unavailable"))?;
    Ok(Envelope {
        version: 1,
        master_id: keys.active.clone(),
        wrapped_dek: crate::engine::credential_crypto::wrap(kek, &dek, true)?,
        nonce,
        ciphertext,
    })
}
pub fn decrypt(keys: &Keys, key_id: &str, e: &Envelope) -> Result<Zeroizing<Vec<u8>>, QueryError> {
    if e.version != 1
        || e.nonce.len() != 12
        || e.ciphertext.len() > 512
        || e.wrapped_dek.len() != 40
    {
        return Err(error("Invalid API key envelope"));
    }
    let kek = keys
        .keys
        .get(&e.master_id)
        .ok_or_else(|| error("API key master key unavailable"))?;
    let dek = Zeroizing::new(crate::engine::credential_crypto::wrap(
        kek,
        &e.wrapped_dek,
        false,
    )?);
    let cipher = Aes256Gcm::new_from_slice(&dek).map_err(|_| error("Invalid API key data key"))?;
    cipher
        .decrypt(
            Nonce::from_slice(&e.nonce),
            Payload {
                msg: &e.ciphertext,
                aad: &aad(key_id),
            },
        )
        .map(Zeroizing::new)
        .map_err(|_| error("API key envelope authentication failed"))
}
pub fn rewrap(keys: &Keys, key_id: &str, e: &mut Envelope) -> Result<(), QueryError> {
    decrypt(keys, key_id, e)?;
    let old = keys
        .keys
        .get(&e.master_id)
        .ok_or_else(|| error("Previous API key master key missing"))?;
    let dek = Zeroizing::new(crate::engine::credential_crypto::wrap(
        old,
        &e.wrapped_dek,
        false,
    )?);
    let new = keys
        .keys
        .get(&keys.active)
        .ok_or_else(|| error("Candidate API key master key missing"))?;
    e.wrapped_dek = crate::engine::credential_crypto::wrap(new, &dek, true)?;
    e.master_id = keys.active.clone();
    Ok(())
}
