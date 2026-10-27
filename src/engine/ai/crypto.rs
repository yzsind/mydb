use super::*;
use aes_gcm::{
    aead::{Aead, KeyInit, Payload},
    Aes256Gcm, Nonce,
};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use rand::{rngs::OsRng, RngCore};
use sha2::{Digest, Sha256};
use std::path::Path;
use zeroize::Zeroizing;

pub(crate) struct Keys {
    pub candidate: String,
    pub keys: BTreeMap<String, Zeroizing<Vec<u8>>>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyFile {
    version: u32,
    active_key_id: String,
    keys: BTreeMap<String, String>,
}
fn key_id(key: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(b"mydb.ai.kek.v1\0");
    h.update(key);
    h.finalize().iter().map(|x| format!("{x:02x}")).collect()
}
fn decode_key(s: &str) -> Result<Zeroizing<Vec<u8>>, QueryError> {
    let bytes = if let Some(v) = s.strip_prefix("base64:") {
        STANDARD.decode(v).ok()
    } else if let Some(v) = s.strip_prefix("hex:") {
        if v.len() != 64 || !v.bytes().all(|b| b.is_ascii_hexdigit()) {
            None
        } else {
            (0..64)
                .step_by(2)
                .map(|i| u8::from_str_radix(&v[i..i + 2], 16).ok())
                .collect::<Option<Vec<_>>>()
        }
    } else {
        None
    };
    bytes
        .filter(|v| v.len() == 32)
        .map(Zeroizing::new)
        .ok_or_else(|| {
            error(
                "ER_AI_ENCRYPTION_KEY_INVALID",
                "key must be base64: or hex: encoding of 32 random bytes",
            )
        })
}
fn read_keyfile(path: &Path) -> Result<Keys, QueryError> {
    #[cfg(not(unix))]
    return Err(error("ER_AI_ENCRYPTION_KEY_INVALID","filesystem AI keyrings require Unix permission enforcement on this build; use MYDB_AI_ENCRYPT_KEY"));
    use std::io::Read;
    let mut opts = std::fs::OpenOptions::new();
    opts.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.custom_flags(libc::O_NOFOLLOW);
    }
    let file = opts
        .open(path)
        .map_err(|_| error("ER_AI_ENCRYPTION_KEY_MISSING", "cannot open AI keyring"))?;
    let md = file
        .metadata()
        .map_err(|_| error("ER_AI_ENCRYPTION_KEY_INVALID", "cannot stat AI keyring"))?;
    if !md.is_file() || md.len() > 1024 * 1024 {
        return Err(error(
            "ER_AI_ENCRYPTION_KEY_INVALID",
            "keyring must be a bounded regular file",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if md.mode() & 0o077 != 0 {
            return Err(error(
                "ER_AI_ENCRYPTION_KEY_INVALID",
                "keyring permissions must be 0600",
            ));
        }
        if md.uid() != rustix::process::geteuid().as_raw() {
            return Err(error(
                "ER_AI_ENCRYPTION_KEY_INVALID",
                "keyring must be owned by the server account",
            ));
        }
    }
    let mut data = Zeroizing::new(String::new());
    file.take(1024 * 1024 + 1)
        .read_to_string(&mut data)
        .map_err(|_| error("ER_AI_ENCRYPTION_KEY_INVALID", "invalid keyring"))?;
    let parsed = parse_json(&data, 1024 * 1024)?;
    let f: KeyFile = serde_json::from_value(parsed)
        .map_err(|_| error("ER_AI_ENCRYPTION_KEY_INVALID", "invalid keyring fields"))?;
    if f.version != 1 {
        return Err(error(
            "ER_AI_ENCRYPTION_KEY_INVALID",
            "unsupported keyring version",
        ));
    }
    let mut keys = BTreeMap::new();
    for (id, encoded) in f.keys {
        let k = decode_key(&encoded)?;
        if key_id(&k) != id {
            return Err(error("ER_AI_ENCRYPTION_KEY_INVALID", "key ID mismatch"));
        }
        keys.insert(id, k);
    }
    if !keys.contains_key(&f.active_key_id) {
        return Err(error("ER_AI_ENCRYPTION_KEY_MISSING", "active key missing"));
    }
    Ok(Keys {
        candidate: f.active_key_id,
        keys,
    })
}
pub(crate) fn load_keys(dir: &Path, state: &AiCatalog, create: bool) -> Result<Keys, QueryError> {
    match crate::util::env::var("MYDB_AI_ENCRYPT_KEY") {
        Ok(encoded) => {
            let key = decode_key(&encoded)?;
            let id = key_id(&key);
            let mut keys = BTreeMap::new();
            if let Ok(local) = LocalConfig::load() {
                if let Some(p) = local.encryption.previous_keys_file {
                    keys = read_keyfile(Path::new(&p))?.keys;
                }
            }
            keys.insert(id.clone(), key);
            return Ok(Keys {
                candidate: id,
                keys,
            });
        }
        Err(std::env::VarError::NotPresent) => {}
        Err(_) => {
            return Err(error(
                "ER_AI_ENCRYPTION_KEY_INVALID",
                "master key is not valid UTF-8",
            ))
        }
    }
    #[cfg(not(unix))]
    return Err(error(
        "ER_AI_ENCRYPTION_KEY_INVALID",
        "filesystem keyrings require Unix permission enforcement; configure MYDB_AI_ENCRYPT_KEY",
    ));
    let path = dir.join("apikey.mydb");
    if !path.exists() {
        if !create || !state.encryption_id.is_empty() || !state.secrets.is_empty() {
            return Err(error(
                "ER_AI_ENCRYPTION_KEY_MISSING",
                "AI keyring is missing; restore the original keyring",
            ));
        }
        let mut key = Zeroizing::new(vec![0; 32]);
        OsRng.try_fill_bytes(&mut key).map_err(|_| {
            error(
                "ER_AI_ENCRYPTION_KEY_INVALID",
                "secure random source unavailable",
            )
        })?;
        let id = key_id(&key);
        let f = KeyFile {
            version: 1,
            active_key_id: id.clone(),
            keys: BTreeMap::from([(id, format!("base64:{}", STANDARD.encode(&key)))]),
        };
        let data = Zeroizing::new(
            serde_json::to_vec(&f)
                .map_err(|_| error("ER_AI_ENCRYPTION_KEY_INVALID", "cannot encode keyring"))?,
        );
        let mut random = [0; 16];
        OsRng.try_fill_bytes(&mut random).map_err(|_| {
            error(
                "ER_AI_ENCRYPTION_KEY_INVALID",
                "secure random source unavailable",
            )
        })?;
        let tmp = dir.join(format!(
            ".ai-key-{}",
            STANDARD.encode(random).replace('/', "_")
        ));
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        use std::io::Write;
        let result = (|| {
            let mut file = opts.open(&tmp)?;
            file.write_all(&data)?;
            file.sync_all()?;
            match std::fs::hard_link(&tmp, &path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(e) => return Err(e),
            }
            std::fs::File::open(dir)?.sync_all()
        })();
        let _ = std::fs::remove_file(&tmp);
        result.map_err(|_| {
            error(
                "ER_AI_ENCRYPTION_KEY_INVALID",
                "cannot atomically create AI keyring",
            )
        })?;
    }
    read_keyfile(&path)
}
fn wrap(key: &[u8], input: &[u8], encrypt: bool) -> Result<Zeroizing<Vec<u8>>, QueryError> {
    use openssl::{
        cipher::Cipher,
        cipher_ctx::{CipherCtx, CipherCtxFlags},
    };
    let fail = || {
        error(
            "ER_AI_SECRET_DECRYPT_FAILED",
            "credential key authentication failed",
        )
    };
    let mut ctx = CipherCtx::new().map_err(|_| fail())?;
    ctx.set_flags(CipherCtxFlags::FLAG_WRAP_ALLOW);
    if encrypt {
        ctx.encrypt_init(Some(Cipher::aes_256_wrap_pad()), Some(key), None)
    } else {
        ctx.decrypt_init(Some(Cipher::aes_256_wrap_pad()), Some(key), None)
    }
    .map_err(|_| fail())?;
    let mut out = Zeroizing::new(Vec::new());
    ctx.cipher_update_vec(input, &mut out).map_err(|_| fail())?;
    ctx.cipher_final_vec(&mut out).map_err(|_| fail())?;
    Ok(out)
}
fn aad(state: &AiCatalog, id: u64, generation: u64) -> Vec<u8> {
    let mut a = b"mydb.ai.secret.payload.v1\0".to_vec();
    a.extend_from_slice(state.encryption_id.as_bytes());
    a.push(0);
    a.extend_from_slice(&id.to_be_bytes());
    a.extend_from_slice(&generation.to_be_bytes());
    a
}
pub(crate) fn encrypt_secret(
    state: &AiCatalog,
    keys: &Keys,
    id: u64,
    generation: u64,
    value: &str,
) -> Result<Secret, QueryError> {
    if value.is_empty()
        || value.len() > 16384
        || value.bytes().any(|b| matches!(b, 0 | b'\r' | b'\n'))
    {
        return Err(error(
            "ER_AI_INVALID_ARGUMENT",
            "invalid API key length or characters",
        ));
    }
    if !state.active_key_id.is_empty() && state.active_key_id != keys.candidate {
        return Err(error(
            "ER_AI_KEY_ROTATION_INCOMPLETE",
            "complete master-key rotation before writing Secrets",
        ));
    }
    let active = if state.active_key_id.is_empty() {
        &keys.candidate
    } else {
        &state.active_key_id
    };
    let kek = keys.keys.get(active).ok_or_else(|| {
        error(
            "ER_AI_ENCRYPTION_KEY_MISSING",
            "active master key unavailable",
        )
    })?;
    let mut dek = Zeroizing::new(vec![0; 32]);
    OsRng.try_fill_bytes(&mut dek).map_err(|_| {
        error(
            "ER_AI_ENCRYPTION_KEY_INVALID",
            "secure random source unavailable",
        )
    })?;
    let mut nonce = vec![0; 12];
    OsRng.try_fill_bytes(&mut nonce).map_err(|_| {
        error(
            "ER_AI_ENCRYPTION_KEY_INVALID",
            "secure random source unavailable",
        )
    })?;
    let cipher = Aes256Gcm::new_from_slice(&dek)
        .map_err(|_| error("ER_AI_ENCRYPTION_KEY_INVALID", "invalid DEK"))?;
    let ciphertext = cipher
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: value.as_bytes(),
                aad: &aad(state, id, generation),
            },
        )
        .map_err(|_| {
            error(
                "ER_AI_SECRET_DECRYPT_FAILED",
                "credential encryption failed",
            )
        })?;
    Ok(Secret {
        id,
        generation,
        crypto_generation: 1,
        key_id: active.clone(),
        wrapped_dek: wrap(kek, &dek, true)?.to_vec(),
        nonce,
        ciphertext,
        format_version: 1,
    })
}
pub(crate) fn decrypt_secret(
    state: &AiCatalog,
    keys: &Keys,
    secret: &Secret,
) -> Result<Zeroizing<Vec<u8>>, QueryError> {
    if secret.format_version != 1
        || secret.nonce.len() != 12
        || secret.ciphertext.len() > 16400
        || secret.ciphertext.len() < 17
    {
        return Err(error(
            "ER_AI_SECRET_DECRYPT_FAILED",
            "invalid credential envelope",
        ));
    }
    let key = keys.keys.get(&secret.key_id).ok_or_else(|| {
        error(
            "ER_AI_ENCRYPTION_KEY_MISSING",
            "credential master key unavailable",
        )
    })?;
    let dek = wrap(key, &secret.wrapped_dek, false)?;
    let cipher = Aes256Gcm::new_from_slice(&dek)
        .map_err(|_| error("ER_AI_SECRET_DECRYPT_FAILED", "invalid DEK"))?;
    cipher
        .decrypt(
            Nonce::from_slice(&secret.nonce),
            Payload {
                msg: &secret.ciphertext,
                aad: &aad(state, secret.id, secret.generation),
            },
        )
        .map(Zeroizing::new)
        .map_err(|_| {
            error(
                "ER_AI_SECRET_DECRYPT_FAILED",
                "credential authentication failed",
            )
        })
}
pub(crate) fn rewrap(state: &mut AiCatalog, keys: &Keys) -> Result<(), QueryError> {
    if state.active_key_id == keys.candidate {
        return Err(error(
            "ER_AI_KEY_ROTATION_INCOMPLETE",
            "deploy a different master-key candidate before rotation",
        ));
    }
    let key = keys
        .keys
        .get(&keys.candidate)
        .ok_or_else(|| error("ER_AI_ENCRYPTION_KEY_MISSING", "candidate key missing"))?;
    for secret in state.secrets.values() {
        decrypt_secret(state, keys, secret)?;
    }
    for secret in state.secrets.values_mut() {
        let old = keys
            .keys
            .get(&secret.key_id)
            .ok_or_else(|| error("ER_AI_ENCRYPTION_KEY_MISSING", "old key missing"))?;
        let dek = wrap(old, &secret.wrapped_dek, false)?;
        secret.wrapped_dek = wrap(key, &dek, true)?.to_vec();
        secret.key_id = keys.candidate.clone();
        secret.crypto_generation = secret
            .crypto_generation
            .checked_add(1)
            .ok_or_else(|| error("ER_AI_BUDGET_EXCEEDED", "crypto generation exhausted"))?;
    }
    state.active_key_id = keys.candidate.clone();
    Ok(())
}
