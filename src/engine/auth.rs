mod cow;
mod role_cache;
mod row_index;
pub use cow::AuthCow;
mod grants;
mod schema_pattern;
pub use grants::{AuthRows, GrantStore};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use rand::{rngs::OsRng, RngCore};
use serde::{Deserialize, Serialize};
use sha1::{Digest as Sha1Digest, Sha1};
use sha2::Sha256;

use crate::mysql::errors as mysql_errors;
use crate::types::QueryError;

// One cache generation covers both catalog publication and policy changes.
// Readers capture it before authorization; publication advances it only after
// the new state is visible, so concurrent changes cannot certify stale rights.
static ROLE_POLICY_GENERATION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
static AUTHORIZATION_GENERATION: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(1);
pub fn authorization_generation() -> u64 {
    AUTHORIZATION_GENERATION.load(Ordering::Acquire)
}
pub(crate) fn invalidate_authorization_cache() {
    AUTHORIZATION_GENERATION.fetch_add(1, Ordering::Release);
}

/// L9: `partial_revokes` system variable (MySQL 8.0.16+). When ON, REVOKE
/// against a schema-level scope from an account that holds a higher-scope
/// grant writes a `RestrictionEntry` into the auth catalog (rather than
/// rejecting). Defaults to OFF, matching MySQL.
static PARTIAL_REVOKES: AtomicBool = AtomicBool::new(false);

pub fn partial_revokes_enabled() -> bool {
    PARTIAL_REVOKES.load(Ordering::Relaxed)
}

pub fn set_partial_revokes(value: bool) {
    PARTIAL_REVOKES.store(value, Ordering::Release);
    invalidate_authorization_cache();
}

/// `activate_all_roles_on_login` (MySQL 8.0). When ON the server activates
/// every role granted to an account (plus mandatory roles) at connection
/// time; when OFF only the account's default roles are activated. Defaults
/// to OFF, matching MySQL.
static ACTIVATE_ALL_ROLES_ON_LOGIN: AtomicBool = AtomicBool::new(false);

pub fn activate_all_roles_on_login() -> bool {
    ACTIVATE_ALL_ROLES_ON_LOGIN.load(Ordering::Relaxed)
}

pub fn set_activate_all_roles_on_login(value: bool) {
    ACTIVATE_ALL_ROLES_ON_LOGIN.store(value, Ordering::Release);
    invalidate_authorization_cache();
}

/// `mandatory_roles` (MySQL 8.0). Roles listed here are implicitly granted to
/// every account: they are applicable to everyone, cannot be revoked while
/// mandatory, and are activated at login when `activate_all_roles_on_login`
/// is ON. Stored as the raw sysvar string (round-trips through
/// `SHOW VARIABLES`); parsed on demand into `AccountKey`s.
static MANDATORY_ROLES_RAW: Mutex<String> = Mutex::new(String::new());
static MANDATORY_ROLES_EMPTY: AtomicBool = AtomicBool::new(true);

pub fn mandatory_roles_raw() -> String {
    MANDATORY_ROLES_RAW
        .lock()
        .map(|g| g.clone())
        .unwrap_or_default()
}

pub fn set_mandatory_roles_raw(value: &str) {
    if let Ok(mut g) = MANDATORY_ROLES_RAW.lock() {
        *g = value.to_string();
        MANDATORY_ROLES_EMPTY.store(value.is_empty(), Ordering::Release);
        ROLE_POLICY_GENERATION.fetch_add(1, Ordering::Release);
        invalidate_authorization_cache();
    }
}

/// `default_password_lifetime` (MySQL 8.0). Global default password lifetime
/// in days applied to accounts that don't set their own `PASSWORD EXPIRE
/// INTERVAL`. `0` = passwords never expire (MySQL's default). Stored as u64
/// but only the u16 range is meaningful.
static DEFAULT_PASSWORD_LIFETIME: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

pub fn default_password_lifetime() -> u16 {
    DEFAULT_PASSWORD_LIFETIME
        .load(Ordering::Relaxed)
        .min(u16::MAX as u64) as u16
}

pub fn set_default_password_lifetime(days: u16) {
    DEFAULT_PASSWORD_LIFETIME.store(days as u64, Ordering::Relaxed);
}

/// `password_history` (MySQL 8.0). Global default for the number of previous
/// passwords an account may not reuse. Accounts that set their own
/// `PASSWORD HISTORY n` override this; `PASSWORD HISTORY DEFAULT` (stored as
/// `None`) falls back here. `0` = no count-based reuse restriction.
static GLOBAL_PASSWORD_HISTORY: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub fn global_password_history() -> u16 {
    GLOBAL_PASSWORD_HISTORY
        .load(Ordering::Relaxed)
        .min(u16::MAX as u64) as u16
}

pub fn set_global_password_history(count: u16) {
    GLOBAL_PASSWORD_HISTORY.store(count as u64, Ordering::Relaxed);
}

/// `password_reuse_interval` (MySQL 8.0). Global default for how many days must
/// pass before a password may be reused. Per-account `PASSWORD REUSE INTERVAL`
/// overrides; `DEFAULT` (`None`) falls back here. `0` = no time-based
/// restriction.
static GLOBAL_PASSWORD_REUSE_INTERVAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

pub fn global_password_reuse_interval() -> u16 {
    GLOBAL_PASSWORD_REUSE_INTERVAL
        .load(Ordering::Relaxed)
        .min(u16::MAX as u64) as u16
}

pub fn set_global_password_reuse_interval(days: u16) {
    GLOBAL_PASSWORD_REUSE_INTERVAL.store(days as u64, Ordering::Relaxed);
}

/// `password_require_current` (MySQL 8.0). Global default for whether a password
/// change must verify the current password (via the `REPLACE` clause).
/// Per-account `PASSWORD REQUIRE CURRENT` overrides; `DEFAULT` (`None`) falls
/// back here. OFF by default (MySQL default).
static GLOBAL_PASSWORD_REQUIRE_CURRENT: AtomicBool = AtomicBool::new(false);

pub fn global_password_require_current() -> bool {
    GLOBAL_PASSWORD_REQUIRE_CURRENT.load(Ordering::Relaxed)
}

pub fn set_global_password_require_current(value: bool) {
    GLOBAL_PASSWORD_REQUIRE_CURRENT.store(value, Ordering::Relaxed);
}

/// L7 / P1-2: is the password expired by lifetime? The per-account
/// `password_lifetime_days` takes precedence; when the account didn't set one
/// (`None`) we fall back to the global `default_password_lifetime` sysvar
/// (MySQL semantics). A lifetime of `0` (account or global) means "never
/// expires". Returns false when the last-changed timestamp is unknown.
pub fn password_lifetime_expired(
    account_lifetime_days: Option<u16>,
    last_changed_ms: Option<u64>,
) -> bool {
    let lifetime = account_lifetime_days.unwrap_or_else(default_password_lifetime);
    if lifetime == 0 {
        return false;
    }
    let Some(changed) = last_changed_ms else {
        return false;
    };
    let elapsed_ms = current_time_ms().saturating_sub(changed);
    let lifetime_ms = (lifetime as u64).saturating_mul(24 * 60 * 60 * 1000);
    elapsed_ms >= lifetime_ms
}

/// Parse the `mandatory_roles` string into role account keys. Accepts the
/// MySQL forms `role@host`, `` `role`@`host` ``, and bare `role` (host
/// defaults to `%`), comma-separated. Malformed entries are skipped.
pub fn parse_mandatory_roles(raw: &str) -> Vec<AccountKey> {
    let mut out = Vec::new();
    for part in raw.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (user, host) = match part.split_once('@') {
            Some((u, h)) => (u.trim(), h.trim()),
            None => (part, "%"),
        };
        let user = user.trim_matches(|c| c == '`' || c == '\'' || c == '"');
        let host = host.trim_matches(|c| c == '`' || c == '\'' || c == '"');
        if user.is_empty() {
            continue;
        }
        out.push(AccountKey::new(user, host));
    }
    out
}

/// L8: per-account quota counters. Hour-window questions / updates / new
/// connections, plus instantaneous current_connections. Reset by FLUSH
/// USER_RESOURCES, or implicitly when the window expires. Implementation
/// uses a single mutex over a small HashMap because:
///   - the lock is short (a handful of u32 increments),
///   - per-account contention is bounded by the number of accounts (<<100
///     in typical deployments),
///   - we don't want per-account atomics because a brand-new account with
///     no row yet must allocate on demand.
/// For very-high-concurrency multi-account fleets we can later swap to a
/// shard-of-mutex / DashMap; not on the critical path for the OLTP
/// single-account workload.
mod resource_limits {
    use super::AccountKey;
    use crate::mysql::errors::{self};
    use crate::types::QueryError;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::OnceLock;

    const HOUR_MS: u64 = 60 * 60 * 1000;

    #[derive(Default, Clone, Copy)]
    pub(super) struct AccountUsage {
        pub questions: u32,
        pub updates: u32,
        pub new_conns: u32,
        pub window_start_ms: u64,
        pub current_conns: u32,
    }

    fn table() -> &'static Mutex<HashMap<AccountKey, AccountUsage>> {
        static T: OnceLock<Mutex<HashMap<AccountKey, AccountUsage>>> = OnceLock::new();
        T.get_or_init(|| Mutex::new(HashMap::new()))
    }

    fn roll_window(usage: &mut AccountUsage, now_ms: u64) {
        if usage.window_start_ms == 0 || now_ms.saturating_sub(usage.window_start_ms) >= HOUR_MS {
            usage.window_start_ms = now_ms;
            usage.questions = 0;
            usage.updates = 0;
            usage.new_conns = 0;
        }
    }

    fn err_limit(account: &AccountKey, kind: &str, limit: u32) -> QueryError {
        errors::error(
            errors::ER_USER_LIMIT_REACHED,
            format!(
                "User {} has exceeded the '{kind}' resource (current value: {limit})",
                account.display()
            ),
        )
    }

    pub fn charge_question(account: &AccountKey, limit: u32) -> Result<(), QueryError> {
        if limit == 0 {
            return Ok(());
        }
        let mut map = table().lock().expect("resource_limits lock poisoned");
        let usage = map.entry(account.clone()).or_default();
        roll_window(usage, super::current_time_ms());
        if usage.questions >= limit {
            return Err(err_limit(account, "max_questions", limit));
        }
        usage.questions = usage.questions.saturating_add(1);
        Ok(())
    }

    pub fn charge_update(account: &AccountKey, limit: u32) -> Result<(), QueryError> {
        if limit == 0 {
            return Ok(());
        }
        let mut map = table().lock().expect("resource_limits lock poisoned");
        let usage = map.entry(account.clone()).or_default();
        roll_window(usage, super::current_time_ms());
        if usage.updates >= limit {
            return Err(err_limit(account, "max_updates", limit));
        }
        usage.updates = usage.updates.saturating_add(1);
        Ok(())
    }

    pub fn charge_new_connection(
        account: &AccountKey,
        max_conn: u32,
        max_user_conn: u32,
    ) -> Result<(), QueryError> {
        let mut map = table().lock().expect("resource_limits lock poisoned");
        let usage = map.entry(account.clone()).or_default();
        roll_window(usage, super::current_time_ms());
        if max_conn > 0 && usage.new_conns >= max_conn {
            return Err(err_limit(account, "max_connections_per_hour", max_conn));
        }
        if max_user_conn > 0 && usage.current_conns >= max_user_conn {
            return Err(err_limit(account, "max_user_connections", max_user_conn));
        }
        usage.new_conns = usage.new_conns.saturating_add(1);
        // Count every authenticated connection, including currently unlimited
        // accounts: a later ALTER USER must see sessions that are already live.
        usage.current_conns = usage.current_conns.saturating_add(1);
        Ok(())
    }

    pub fn release_connection(account: &AccountKey) {
        if let Ok(mut map) = table().lock() {
            if let Some(usage) = map.get_mut(account) {
                usage.current_conns = usage.current_conns.saturating_sub(1);
            }
        }
    }

    pub fn clear(account: &AccountKey) {
        if let Ok(mut map) = table().lock() {
            map.remove(account);
            if map.capacity() > 64 && map.len().saturating_mul(4) < map.capacity() {
                let live = map.len();
                map.shrink_to(live.max(64));
            }
        }
    }

    pub fn rename(from: &AccountKey, to: &AccountKey) {
        if let Ok(mut map) = table().lock() {
            if let Some(mut usage) = map.remove(from) {
                // Existing sessions still release the old authenticated key.
                // They must not leave the renamed account's concurrent count
                // permanently elevated.
                usage.current_conns = 0;
                map.insert(to.clone(), usage);
            }
        }
    }

    pub fn flush_all() {
        if let Ok(mut map) = table().lock() {
            for (_, usage) in map.iter_mut() {
                usage.questions = 0;
                usage.updates = 0;
                usage.new_conns = 0;
                // current_conns is NOT reset by FLUSH USER_RESOURCES — it
                // tracks actively-held connections.
            }
        }
    }

    #[cfg(test)]
    #[test]
    fn dropped_and_renamed_accounts_release_resource_rows() {
        let old = AccountKey::new("ml12_old", "localhost");
        let new = AccountKey::new("ml12_new", "localhost");
        charge_new_connection(&old, 10, 1).unwrap();
        assert!(table().lock().unwrap().contains_key(&old));
        rename(&old, &new);
        let map = table().lock().unwrap();
        assert!(!map.contains_key(&old));
        assert_eq!(map.get(&new).unwrap().current_conns, 0);
        drop(map);
        clear(&new);
        assert!(!table().lock().unwrap().contains_key(&new));
    }
}

pub use resource_limits::{
    charge_new_connection, charge_question, charge_update, clear as clear_user_resources,
    flush_all as flush_user_resources, release_connection, rename as rename_user_resources,
};

/// Temporary failed-login lock tracking (the `FAILED_LOGIN_ATTEMPTS` /
/// `PASSWORD_LOCK_TIME` feature). Kept **in memory** — like MySQL, the
/// consecutive-failure counter and the temporary lock are process-local and
/// reset on restart; they are NOT persisted. This matters for two reasons:
///   1. A brute-force attacker no longer forces a full auth-catalog fsync per
///      attempt (the previous design routed every failure through
///      `mutate_auth_catalog`, which serializes the whole catalog to disk).
///   2. A failed login no longer bumps the privilege epoch (it changes no
///      grants), so it can't churn every live session's cached authorization.
/// The permanent `ACCOUNT LOCK` state stays on the persisted `UserAccount`;
/// only the failed-login *counters* live here.
mod failed_login {
    use super::AccountKey;
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};

    #[derive(Default, Clone, Copy)]
    struct LockState {
        failures: u16,
        locked_until_ms: Option<u64>,
    }

    fn table() -> &'static Mutex<HashMap<AccountKey, LockState>> {
        static T: OnceLock<Mutex<HashMap<AccountKey, LockState>>> = OnceLock::new();
        T.get_or_init(|| Mutex::new(HashMap::new()))
    }

    /// Currently locked? Returns the lock expiry (ms) if the account is inside
    /// an active temporary lock window. Expired windows are cleared in place so
    /// the next failed attempt starts a fresh budget (MySQL semantics).
    pub fn active_lock(account: &AccountKey, now_ms: u64) -> Option<u64> {
        let mut map = table().lock().expect("failed_login lock poisoned");
        let Some(state) = map.get_mut(account) else {
            return None;
        };
        match state.locked_until_ms {
            Some(until) if until > now_ms => Some(until),
            Some(_) => {
                // Lock elapsed — reset counter and timer for a fresh window.
                state.failures = 0;
                state.locked_until_ms = None;
                None
            }
            None => None,
        }
    }

    /// Record one failed login. When the running count reaches `max_attempts`
    /// (0 disables the feature) the account is temporarily locked for
    /// `lock_days` days (`u16::MAX` = effectively permanent until unlock/flush).
    pub fn record_failure(
        account: &AccountKey,
        max_attempts: u16,
        lock_days: Option<u16>,
        now_ms: u64,
    ) {
        if max_attempts == 0 || lock_days.unwrap_or(0) == 0 {
            return;
        }
        let mut map = table().lock().expect("failed_login lock poisoned");
        let state = map.entry(account.clone()).or_default();
        state.failures = state.failures.saturating_add(1);
        if state.failures >= max_attempts && state.locked_until_ms.is_none() {
            let days = lock_days.unwrap_or(0);
            state.locked_until_ms = Some(if days == u16::MAX {
                u64::MAX
            } else {
                now_ms.saturating_add(days as u64 * 24 * 60 * 60 * 1000)
            });
        }
    }

    /// Clear the counter and any temporary lock for one account (successful
    /// login, or explicit `ALTER USER ... ACCOUNT UNLOCK`).
    pub fn clear(account: &AccountKey) {
        if let Ok(mut map) = table().lock() {
            map.remove(account);
        }
    }

    pub fn rename(from: &AccountKey, to: &AccountKey) {
        if let Ok(mut map) = table().lock() {
            let prior = map.remove(from);
            map.remove(to);
            if let Some(state) = prior {
                map.insert(to.clone(), state);
            }
        }
    }

    /// `FLUSH HOSTS` — reset every account's failed-login budget.
    pub fn flush_all() {
        if let Ok(mut map) = table().lock() {
            map.clear();
        }
    }
}

pub use failed_login::{
    active_lock as failed_login_active_lock, clear as failed_login_clear,
    flush_all as flush_all_failed_logins, record_failure as failed_login_record,
    rename as failed_login_rename,
};

/// `FLUSH HOSTS` — MySQL clears its `host_cache` (which records per-host
/// handshake failures and blocks hosts after `max_connect_errors` consecutive
/// aborts). mydb does not maintain that cache; the equivalent state is the
/// in-memory failed-login tracker. Resetting it gives FLUSH HOSTS a meaningful
/// effect parallel to MySQL: any account temporarily locked by a run of bad
/// passwords gets a fresh budget.
pub fn flush_all_failed_login_counts(
    _db: &crate::storage::Database,
) -> Result<(), crate::engine::QueryError> {
    flush_all_failed_logins();
    Ok(())
}

/// N1/N2/N6: server-side RSA keypair for `caching_sha2_password` /
/// `sha256_password` full-authentication when the client can't or won't
/// send the password over a TLS link. Loaded from `MYDB_AUTH_RSA_*` env
/// vars if present; otherwise an ephemeral RSA-2048 pair is generated
/// once per process. Public key is exposed as PKCS#1 PEM (the format
/// MySQL clients expect from the `AuthMoreData(public_key)` frame).
pub mod rsa_keys {
    use openssl::encrypt::Decrypter;
    use openssl::error::ErrorStack;
    use openssl::hash::MessageDigest;
    use openssl::pkey::{Id, PKey, Private};
    use openssl::rsa::{Padding, Rsa};
    use std::sync::OnceLock;

    struct RsaKeyset {
        private: PKey<Private>,
        public_pem: String,
    }

    fn decrypt(
        private: &PKey<Private>,
        ciphertext: &[u8],
        padding: Padding,
    ) -> Result<Vec<u8>, ErrorStack> {
        let mut context = Decrypter::new(private)?;
        context.set_rsa_padding(padding)?;
        if padding == Padding::PKCS1_OAEP {
            context.set_rsa_oaep_md(MessageDigest::sha1())?;
            context.set_rsa_mgf1_md(MessageDigest::sha1())?;
        }
        let mut output = vec![0; private.size()];
        let len = context.decrypt(ciphertext, &mut output)?;
        output.truncate(len);
        Ok(output)
    }

    fn load_keyset() -> Result<RsaKeyset, String> {
        // EVP's default provider supplies implicit PKCS#1 v1.5 rejection in
        // OpenSSL 3.2+. Refuse an older externally selected system library.
        if openssl::version::number() < 0x3020_0000 {
            return Err("authentication RSA requires OpenSSL 3.2 or newer".to_string());
        }
        let pem = if let Ok(value) = crate::util::env::var("MYDB_AUTH_RSA_PRIVATE_KEY_INLINE") {
            Some(value)
        } else if let Ok(path) = crate::util::env::var("MYDB_AUTH_RSA_PRIVATE_KEY") {
            Some(
                std::fs::read_to_string(&path)
                    .map_err(|e| format!("MYDB_AUTH_RSA_PRIVATE_KEY '{path}' read failed: {e}"))?,
            )
        } else {
            None
        };
        let private = if let Some(pem) = pem {
            // An explicit passphrase avoids an interactive stdin prompt for
            // encrypted or malformed configuration supplied at startup.
            PKey::private_key_from_pem_passphrase(pem.as_bytes(), b"")
                .map_err(|e| format!("authentication RSA private key parse failed: {e}"))?
        } else {
            Rsa::generate(2048)
                .and_then(PKey::from_rsa)
                .map_err(|e| format!("ephemeral RSA-2048 key generation failed: {e}"))?
        };
        if private.id() != Id::RSA || !(2048..=8192).contains(&private.bits()) {
            return Err(
                "authentication requires an RSA private key of 2048 to 8192 bits".to_string(),
            );
        }
        // A valid-length zero ciphertext has invalid v1.5 padding. A provider
        // without implicit rejection exposes a padding oracle and must fail
        // startup instead of silently weakening the wire protocol.
        decrypt(&private, &vec![0; private.size()], Padding::PKCS1).map_err(|_| {
            "authentication RSA provider lacks implicit PKCS#1 rejection".to_string()
        })?;
        let public_pem = String::from_utf8(
            private
                .public_key_to_pem()
                .map_err(|e| format!("RSA public key SPKI PEM encoding failed: {e}"))?,
        )
        .map_err(|_| "RSA public key PEM is not UTF-8".to_string())?;
        Ok(RsaKeyset {
            private,
            public_pem,
        })
    }

    fn keyset_result() -> &'static Result<RsaKeyset, String> {
        static KEYS: OnceLock<Result<RsaKeyset, String>> = OnceLock::new();
        KEYS.get_or_init(load_keyset)
    }

    fn keyset() -> &'static RsaKeyset {
        keyset_result().as_ref().unwrap_or_else(|err| {
            panic!("authentication RSA keys were not initialized successfully: {err}")
        })
    }

    /// Reject invalid key material or providers before listeners start.
    pub fn initialize() -> Result<(), String> {
        keyset_result().as_ref().map(|_| ()).map_err(Clone::clone)
    }

    /// MySQL's SubjectPublicKeyInfo (BEGIN PUBLIC KEY), not PKCS#1 public PEM.
    pub fn public_key_pem() -> &'static str {
        &keyset().public_pem
    }

    /// EVP supplies implicit rejection for invalid PKCS#1 v1.5 padding.
    /// The synthetic plaintext must still undergo normal password verification.
    pub fn pkcs1v15_decrypt(ciphertext: &[u8]) -> Result<Vec<u8>, ErrorStack> {
        decrypt(&keyset().private, ciphertext, Padding::PKCS1)
    }

    /// MySQL uses OAEP-SHA1 with MGF1-SHA1 and an empty label.
    pub fn oaep_sha1_decrypt(ciphertext: &[u8]) -> Result<Vec<u8>, ErrorStack> {
        decrypt(&keyset().private, ciphertext, Padding::PKCS1_OAEP)
    }

    /// Prefer OAEP because its block can also accidentally fit legacy padding.
    pub fn decrypt_either(ciphertext: &[u8]) -> Result<Vec<u8>, ErrorStack> {
        oaep_sha1_decrypt(ciphertext).or_else(|_| pkcs1v15_decrypt(ciphertext))
    }
}

/// L9-companion: cache of accounts whose primary auth string has been
/// successfully verified at least once. Populated by the
/// `caching_sha2_password` fast-success path (post-full-auth); cleared by
/// FLUSH PRIVILEGES.
#[derive(Clone)]
struct CachedCredential {
    authentication_string: String,
    digest: [u8; 32],
    _charge: Arc<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
}

static AUTH_CACHE: OnceLock<Mutex<BTreeMap<AccountKey, Vec<CachedCredential>>>> = OnceLock::new();

fn auth_cache() -> &'static Mutex<BTreeMap<AccountKey, Vec<CachedCredential>>> {
    AUTH_CACHE.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
/// Compatibility helper used by diagnostics/tests. A real fast-auth entry is
/// installed by `auth_cache_remember_password`; a key-only entry reports cache
/// membership but cannot authenticate a scramble.
pub fn auth_cache_remember(account: &AccountKey) {
    if let Ok(mut cache) = auth_cache().lock() {
        cache.entry(account.clone()).or_default();
    }
}

pub fn auth_cache_remember_password(
    account: &AccountKey,
    password: &str,
    authentication_string: &str,
) {
    let stage1 = Sha256::digest(password.as_bytes());
    let stage2 = Sha256::digest(stage1);
    let mut digest = [0u8; 32];
    digest.copy_from_slice(&stage2);
    use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
    let Some(charge) = ControlledMemoryCharge::try_new_optional(
        ControlledMemoryCategory::SystemMetadata,
        512 + account.user.len() + account.host.len() + authentication_string.len(),
    ) else {
        return;
    };
    if let Ok(mut cache) = auth_cache().lock() {
        // Optional acceleration: bounded eviction only forces a new full-auth.
        if cache.len() >= 4096 && !cache.contains_key(account) {
            cache.pop_first();
        }
        let entries = cache.entry(account.clone()).or_default();
        if !entries.iter().any(|entry| {
            entry.authentication_string == authentication_string
                && constant_time_eq(&entry.digest, &digest)
        }) {
            entries.push(CachedCredential {
                authentication_string: authentication_string.to_string(),
                digest,
                _charge: Arc::new(charge),
            });
            // Primary + retained secondary is the maximum useful set.
            if entries.len() > 2 {
                entries.remove(0);
            }
        }
    }
}

#[cfg(test)]
pub fn auth_cache_contains(account: &AccountKey) -> bool {
    auth_cache()
        .lock()
        .map(|cache| cache.contains_key(account))
        .unwrap_or(false)
}

pub fn auth_cache_forget(account: &AccountKey) {
    if let Ok(mut cache) = auth_cache().lock() {
        cache.remove(account);
    }
}

pub fn verify_cached_caching_sha2(
    account: &AccountKey,
    authentication_string: &str,
    token: &[u8],
    seed: &[u8],
) -> bool {
    if token.len() != 32 {
        return false;
    }
    let Ok(cache) = auth_cache().lock() else {
        return false;
    };
    cache.get(account).is_some_and(|digests| {
        digests
            .iter()
            .filter(|entry| entry.authentication_string == authentication_string)
            .any(|entry| {
                let stage2 = &entry.digest;
                let mut sha = Sha256::new();
                sha.update(stage2);
                sha.update(seed);
                let scramble_digest = sha.finalize();
                let stage1: Vec<u8> = token
                    .iter()
                    .zip(scramble_digest.iter())
                    .map(|(left, right)| left ^ right)
                    .collect();
                constant_time_eq(Sha256::digest(stage1).as_slice(), stage2)
            })
    })
}

/// Auth-cache lookup keyed by *connection identity* (username + peer host).
/// The cache itself stores the `AccountKey` of the matched account (which
/// uses the host pattern, not the peer IP). This helper does the lookup so
/// session.rs doesn't need to peek into the catalog.
pub fn auth_cache_contains_for(catalog: &AuthCatalog, username: &str, peer_host: &str) -> bool {
    let Some(account) = catalog.match_account(username, peer_host) else {
        return false;
    };
    auth_cache()
        .lock()
        .map(|cache| {
            cache.get(&account.key).is_some_and(|entries| {
                entries.iter().any(|e| {
                    e.authentication_string == account.authentication_string
                        || account.secondary_authentication_string.as_ref()
                            == Some(&e.authentication_string)
                })
            })
        })
        .unwrap_or(false)
}

pub fn auth_cache_clear() {
    if let Ok(mut set) = auth_cache().lock() {
        set.clear();
    }
}

/// MySQL caching_sha2_password "RSA-encrypted password XOR seed"
/// transformation. Both the client's encryption and the server's
/// post-decryption recovery use the same XOR with `seed` cycled out
/// to the plaintext length. The trailing nul terminator must be
/// stripped (clients always append it).
pub fn xor_password_with_seed(buf: &mut [u8], seed: &[u8]) {
    if seed.is_empty() {
        return;
    }
    for (i, byte) in buf.iter_mut().enumerate() {
        *byte ^= seed[i % seed.len()];
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AccountKey {
    pub user: String,
    pub host: String,
}

impl AccountKey {
    pub fn new(user: impl Into<String>, host: impl Into<String>) -> Self {
        Self {
            user: user.into(),
            host: host.into().to_ascii_lowercase(),
        }
    }

    pub fn display(&self) -> String {
        format!(
            "'{}'@'{}'",
            self.user.replace('\'', "''"),
            self.host.replace('\'', "''")
        )
    }

    pub fn role_sql_name(&self) -> String {
        format!(
            "`{}`@`{}`",
            self.user.replace('`', "``"),
            self.host.replace('`', "``")
        )
    }

    pub fn current_user_value(&self) -> String {
        format!("{}@{}", self.user, self.host)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserAccount {
    pub key: AccountKey,
    pub plugin: String,
    pub authentication_string: String,
    #[serde(default)]
    pub secondary_authentication_string: Option<String>,
    #[serde(default)]
    pub is_role: bool,
    #[serde(default)]
    pub account_locked: bool,
    #[serde(default)]
    pub password_expired: bool,
    #[serde(default)]
    pub ssl_type: SslRequirement,
    #[serde(default)]
    pub max_questions: u32,
    #[serde(default)]
    pub max_updates: u32,
    #[serde(default)]
    pub max_connections: u32,
    #[serde(default)]
    pub max_user_connections: u32,
    #[serde(default)]
    pub password_last_changed_ms: Option<u64>,
    #[serde(default)]
    pub password_lifetime_days: Option<u16>,
    #[serde(default)]
    pub password_reuse_history: Option<u16>,
    #[serde(default)]
    pub password_reuse_time_days: Option<u16>,
    #[serde(default)]
    pub password_require_current: Option<bool>,
    #[serde(default)]
    pub failed_login_attempts: u16,
    #[serde(default)]
    pub password_lock_time_days: Option<u16>,
    #[serde(default)]
    pub failed_login_count: u16,
    #[serde(default)]
    pub locked_until_ms: Option<u64>,
    #[serde(default)]
    pub user_attributes_json: Option<String>,
    /// L1: stored as SHA-256 hex of past authentication strings, newest
    /// first. Capped at the larger of `password_reuse_history` and 32. Each
    /// entry records when the password was set so the time-window check can
    /// reject reuse within `password_reuse_time_days`.
    #[serde(default)]
    pub password_history: Vec<PasswordHistoryEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PasswordHistoryEntry {
    /// Plugin-formatted authentication string. It is already a one-way hash;
    /// retaining it lets the plugin verify a proposed plaintext password even
    /// when salted hashes differ byte-for-byte.
    pub authentication_string: String,
    pub changed_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum SslRequirement {
    #[default]
    None,
    Ssl,
    X509,
    Specified {
        #[serde(default)]
        cipher: Option<String>,
        #[serde(default)]
        issuer: Option<String>,
        #[serde(default)]
        subject: Option<String>,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceLimits {
    pub max_questions: u32,
    pub max_updates: u32,
    pub max_connections: u32,
    pub max_user_connections: u32,
}

impl UserAccount {
    pub fn new(key: AccountKey, plugin: String, authentication_string: String) -> Self {
        Self {
            key,
            plugin,
            authentication_string,
            secondary_authentication_string: None,
            is_role: false,
            account_locked: false,
            password_expired: false,
            ssl_type: SslRequirement::None,
            max_questions: 0,
            max_updates: 0,
            max_connections: 0,
            max_user_connections: 0,
            password_last_changed_ms: Some(current_time_ms()),
            password_lifetime_days: None,
            password_reuse_history: None,
            password_reuse_time_days: None,
            password_require_current: None,
            failed_login_attempts: 0,
            password_lock_time_days: None,
            failed_login_count: 0,
            locked_until_ms: None,
            user_attributes_json: None,
            password_history: Vec::new(),
        }
    }

    /// Push the current authentication string onto the password history
    /// (newest first), then clamp the history to the larger of
    /// `password_reuse_history` and 32 (we keep up to 32 entries so the
    /// per-account override is meaningful even if the global default is 0).
    pub fn push_password_history(&mut self, hash: &str, now_ms: u64) {
        let entry = PasswordHistoryEntry {
            authentication_string: hash.to_string(),
            changed_ms: now_ms,
        };
        self.password_history.insert(0, entry);
        let count = self
            .password_reuse_history
            .unwrap_or_else(global_password_history) as usize;
        let days = self
            .password_reuse_time_days
            .unwrap_or_else(global_password_reuse_interval);
        let window_ms = u64::from(days).saturating_mul(24 * 60 * 60 * 1000);
        self.password_history = std::mem::take(&mut self.password_history)
            .into_iter()
            .enumerate()
            .filter(|(idx, entry)| {
                *idx < count || (days > 0 && now_ms.saturating_sub(entry.changed_ms) <= window_ms)
            })
            .map(|(_, entry)| entry)
            .collect();
    }

    /// L1: returns true when `candidate` reuses a recent password the
    /// account already used. Reuse limits come from two per-account fields
    /// (`password_reuse_history` count, `password_reuse_time_days` window);
    /// either can independently veto. `None` on both fields = no reuse
    /// restriction.
    pub fn password_auth_string_reused(&self, candidate: &str, now_ms: u64) -> bool {
        // Per-account clause wins; `DEFAULT` (`None`) falls back to the global
        // `password_history` / `password_reuse_interval` sysvars (MySQL 8.0).
        let count = self
            .password_reuse_history
            .unwrap_or_else(global_password_history);
        let days = self
            .password_reuse_time_days
            .unwrap_or_else(global_password_reuse_interval);
        let mut blocked = false;
        if count > 0 {
            for entry in self.password_history.iter().take(count as usize) {
                if constant_time_eq(entry.authentication_string.as_bytes(), candidate.as_bytes()) {
                    blocked = true;
                    break;
                }
            }
        }
        if !blocked && days > 0 {
            let window_ms = (days as u64).saturating_mul(24 * 60 * 60 * 1000);
            for entry in &self.password_history {
                if now_ms.saturating_sub(entry.changed_ms) <= window_ms
                    && constant_time_eq(
                        entry.authentication_string.as_bytes(),
                        candidate.as_bytes(),
                    )
                {
                    blocked = true;
                    break;
                }
            }
        }
        blocked
    }

    pub fn plaintext_password_reused(&self, password: &str, now_ms: u64) -> bool {
        if verify_plaintext_auth_string(&self.plugin, &self.authentication_string, password) {
            return true;
        }
        let count = self
            .password_reuse_history
            .unwrap_or_else(global_password_history) as usize;
        let days = self
            .password_reuse_time_days
            .unwrap_or_else(global_password_reuse_interval);
        let window_ms = u64::from(days).saturating_mul(24 * 60 * 60 * 1000);
        self.password_history
            .iter()
            .enumerate()
            .filter(|(idx, entry)| {
                *idx < count || (days > 0 && now_ms.saturating_sub(entry.changed_ms) <= window_ms)
            })
            .any(|(_, entry)| {
                verify_plaintext_auth_string(&self.plugin, &entry.authentication_string, password)
            })
    }

    pub fn resource_limits(&self) -> ResourceLimits {
        ResourceLimits {
            max_questions: self.max_questions,
            max_updates: self.max_updates,
            max_connections: self.max_connections,
            max_user_connections: self.max_user_connections,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Privilege {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Reload,
    Shutdown,
    Process,
    File,
    GrantOption,
    References,
    Index,
    Alter,
    ShowDb,
    Super,
    CreateTmpTable,
    LockTables,
    Execute,
    ReplSlave,
    ReplClient,
    CreateView,
    ShowView,
    CreateRoutine,
    AlterRoutine,
    CreateUser,
    Event,
    Trigger,
    CreateTablespace,
    CreateRole,
    DropRole,
    Dynamic(String),
}

impl Privilege {
    pub fn parse(input: &str) -> Self {
        let normalized = input
            .trim()
            .trim_matches('`')
            .replace('-', "_")
            .replace(' ', "_")
            .to_ascii_uppercase();
        match normalized.as_str() {
            "SELECT" => Self::Select,
            "INSERT" => Self::Insert,
            "UPDATE" => Self::Update,
            "DELETE" => Self::Delete,
            "CREATE" => Self::Create,
            "DROP" => Self::Drop,
            "RELOAD" => Self::Reload,
            "SHUTDOWN" => Self::Shutdown,
            "PROCESS" => Self::Process,
            "FILE" => Self::File,
            "GRANT_OPTION" => Self::GrantOption,
            "REFERENCES" => Self::References,
            "INDEX" => Self::Index,
            "ALTER" => Self::Alter,
            "SHOW_DATABASES" => Self::ShowDb,
            "SUPER" => Self::Super,
            "CREATE_TEMPORARY_TABLES" => Self::CreateTmpTable,
            "LOCK_TABLES" => Self::LockTables,
            "EXECUTE" => Self::Execute,
            "REPLICATION_SLAVE" | "REPLICATION_REPLICA" => Self::ReplSlave,
            "REPLICATION_CLIENT" => Self::ReplClient,
            "FLUSH_TABLES" => Self::Reload,
            "CREATE_VIEW" => Self::CreateView,
            "SHOW_VIEW" => Self::ShowView,
            "CREATE_ROUTINE" => Self::CreateRoutine,
            "ALTER_ROUTINE" => Self::AlterRoutine,
            "CREATE_USER" => Self::CreateUser,
            "EVENT" => Self::Event,
            "TRIGGER" => Self::Trigger,
            "CREATE_TABLESPACE" => Self::CreateTablespace,
            "CREATE_ROLE" => Self::CreateRole,
            "DROP_ROLE" => Self::DropRole,
            other => Self::Dynamic(other.to_string()),
        }
    }

    pub fn sql_name(&self) -> String {
        match self {
            Self::Select => "SELECT",
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
            Self::Create => "CREATE",
            Self::Drop => "DROP",
            Self::Reload => "RELOAD",
            Self::Shutdown => "SHUTDOWN",
            Self::Process => "PROCESS",
            Self::File => "FILE",
            Self::GrantOption => "GRANT OPTION",
            Self::References => "REFERENCES",
            Self::Index => "INDEX",
            Self::Alter => "ALTER",
            Self::ShowDb => "SHOW DATABASES",
            Self::Super => "SUPER",
            Self::CreateTmpTable => "CREATE TEMPORARY TABLES",
            Self::LockTables => "LOCK TABLES",
            Self::Execute => "EXECUTE",
            Self::ReplSlave => "REPLICATION SLAVE",
            Self::ReplClient => "REPLICATION CLIENT",
            Self::CreateView => "CREATE VIEW",
            Self::ShowView => "SHOW VIEW",
            Self::CreateRoutine => "CREATE ROUTINE",
            Self::AlterRoutine => "ALTER ROUTINE",
            Self::CreateUser => "CREATE USER",
            Self::Event => "EVENT",
            Self::Trigger => "TRIGGER",
            Self::CreateTablespace => "CREATE TABLESPACE",
            Self::CreateRole => "CREATE ROLE",
            Self::DropRole => "DROP ROLE",
            Self::Dynamic(name) => name.as_str(),
        }
        .to_string()
    }

    pub fn mysql_user_column(&self) -> Option<&'static str> {
        match self {
            Self::Select => Some("Select_priv"),
            Self::Insert => Some("Insert_priv"),
            Self::Update => Some("Update_priv"),
            Self::Delete => Some("Delete_priv"),
            Self::Create => Some("Create_priv"),
            Self::Drop => Some("Drop_priv"),
            Self::Reload => Some("Reload_priv"),
            Self::Shutdown => Some("Shutdown_priv"),
            Self::Process => Some("Process_priv"),
            Self::File => Some("File_priv"),
            Self::GrantOption => Some("Grant_priv"),
            Self::References => Some("References_priv"),
            Self::Index => Some("Index_priv"),
            Self::Alter => Some("Alter_priv"),
            Self::ShowDb => Some("Show_db_priv"),
            Self::Super => Some("Super_priv"),
            Self::CreateTmpTable => Some("Create_tmp_table_priv"),
            Self::LockTables => Some("Lock_tables_priv"),
            Self::Execute => Some("Execute_priv"),
            Self::ReplSlave => Some("Repl_slave_priv"),
            Self::ReplClient => Some("Repl_client_priv"),
            Self::CreateView => Some("Create_view_priv"),
            Self::ShowView => Some("Show_view_priv"),
            Self::CreateRoutine => Some("Create_routine_priv"),
            Self::AlterRoutine => Some("Alter_routine_priv"),
            Self::CreateUser => Some("Create_user_priv"),
            Self::Event => Some("Event_priv"),
            Self::Trigger => Some("Trigger_priv"),
            Self::CreateTablespace => Some("Create_tablespace_priv"),
            Self::CreateRole => Some("Create_role_priv"),
            Self::DropRole => Some("Drop_role_priv"),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum PrivilegeScope {
    Global,
    Database {
        database: String,
    },
    Table {
        database: String,
        table: String,
    },
    Routine {
        database: String,
        routine: String,
        kind: String,
    },
    Column {
        database: String,
        table: String,
        column: String,
    },
    Proxy {
        user: String,
        host: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrantEntry {
    #[serde(default)]
    pub updated_ms: u64,
    /// Account that issued the grant. Persisted so ROLE_*_GRANTS can expose
    /// the real MySQL 8.0 GRANTOR instead of fabricating a bootstrap account.
    pub grantor: AccountKey,
    pub grantee: AccountKey,
    pub privilege: Privilege,
    pub scope: PrivilegeScope,
    #[serde(default)]
    pub grant_option: bool,
    #[serde(default)]
    pub origin: GrantOrigin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum GrantOrigin {
    #[default]
    Explicit,
    AutomaticRoutine,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleEdge {
    pub from_role: AccountKey,
    pub to_account: AccountKey,
    #[serde(default)]
    pub admin_option: bool,
}

/// L9: partial revoke entry. Lets `partial_revokes=ON` express "u holds the
/// privilege globally except in this schema". MySQL 8.0 enforces these only
/// at schema (`db.*`) scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestrictionEntry {
    pub grantee: AccountKey,
    pub privilege: Privilege,
    pub database: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequiredPrivilege {
    pub privilege: Privilege,
    pub scope: PrivilegeScope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorizationContext {
    pub authenticated_as: AccountKey,
    pub active_roles: AuthCow<BTreeSet<AccountKey>>,
    pub epoch: u64,
}

#[derive(Debug, Clone)]
pub struct AuthRequest<'a> {
    pub username: &'a str,
    pub host: &'a str,
    pub auth_response: &'a [u8],
    pub auth_plugin: Option<&'a str>,
    /// Per-connection scramble the server sent in the initial handshake (or
    /// AuthSwitchRequest). The password verifiers recompute the expected
    /// scramble against *this* value, so a login captured on one connection
    /// cannot be replayed on the next (each connection gets a fresh nonce).
    pub nonce: &'a [u8],
    pub client_capabilities: u32,
    pub tls_established: bool,
    pub tls_cipher: Option<&'a str>,
    pub tls_version: Option<&'a str>,
    pub x509_subject: Option<&'a str>,
    pub x509_issuer: Option<&'a str>,
}

impl<'a> AuthRequest<'a> {
    pub fn legacy(
        username: &'a str,
        host: &'a str,
        auth_response: &'a [u8],
        auth_plugin: Option<&'a str>,
    ) -> Self {
        Self {
            username,
            host,
            auth_response,
            auth_plugin,
            // Legacy convenience path (tests / internal callers): the fixed
            // nonce matches what the legacy test scrambles are built against.
            nonce: crate::mysql::protocol::auth_plugin_data(),
            client_capabilities: 0,
            tls_established: false,
            tls_cipher: None,
            tls_version: None,
            x509_subject: None,
            x509_issuer: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AuthOutcome {
    pub context: AuthorizationContext,
    pub password_expired: bool,
    pub account_locked: bool,
    pub resource_limits: ResourceLimits,
    pub current_user_is_system: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthCatalog {
    #[serde(default)]
    pub epoch: u64,
    #[serde(default)]
    pub accounts: AuthCow<imbl::OrdMap<String, UserAccount>>,
    #[serde(default)]
    pub grants: GrantStore,
    #[serde(default)]
    pub role_edges: AuthRows<RoleEdge>,
    #[serde(default)]
    pub default_roles: AuthRows<RoleEdge>,
    /// L9: partial-revoke denylist. Each entry says "grantee may not exercise
    /// `privilege` against tables in `database`, even if a higher-scope grant
    /// would have allowed it". Only meaningful while `partial_revokes=ON`.
    #[serde(default)]
    pub restrictions: AuthRows<RestrictionEntry>,
    /// Immutable read index rebuilt once per successful auth mutation. It is
    /// skipped by persistence and Arc-shared by catalog clones, making the
    /// per-statement authorization path proportional to grants held by the
    /// active principals rather than every grant in the server.
    #[serde(skip, default = "empty_auth_indexes")]
    indexes: Arc<AuthIndexes>,
    #[serde(skip, default)]
    store_charge: Option<Arc<crate::engine::executor::mem_budget::ControlledMemoryCharge>>,
    #[serde(skip)]
    account_memory: Option<(u64, usize)>,
}

#[derive(Debug, Default)]
struct AuthIndexes {
    indexed: bool,
    grants_by_grantee: imbl::HashMap<AccountKey, Arc<Vec<(usize, Arc<GrantEntry>)>>>,
    grant_revision: u64,
    grants_by_scope: imbl::HashMap<u64, Arc<Vec<(usize, Arc<GrantEntry>)>>>,
    role_revision: u64,
    default_revision: u64,
    restriction_revision: u64,
    schema_patterns: imbl::HashMap<AccountKey, Arc<Vec<Arc<GrantEntry>>>>,
    roles_by_target: row_index::RowIndex<RoleEdge>,
    defaults_by_target: row_index::RowIndex<RoleEdge>,
    restrictions_by_grantee: row_index::RowIndex<RestrictionEntry>,
    _charge: Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
}

fn empty_auth_indexes() -> Arc<AuthIndexes> {
    Arc::new(AuthIndexes::default())
}

enum GrantIterMode<'a, 'b> {
    Indexed(std::slice::Iter<'a, (usize, Arc<GrantEntry>)>),
    Full {
        account: &'b AccountKey,
        rows: Box<dyn Iterator<Item = &'a GrantEntry> + 'a>,
    },
}

pub(crate) struct GrantIter<'a, 'b> {
    mode: GrantIterMode<'a, 'b>,
}

impl<'a, 'b> Iterator for GrantIter<'a, 'b> {
    type Item = &'a GrantEntry;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.mode {
            GrantIterMode::Indexed(indices) => indices.next().map(|(_, grant)| grant.as_ref()),
            GrantIterMode::Full { account, rows } => rows.find(|grant| &grant.grantee == *account),
        }
    }
}

impl Default for AuthCatalog {
    fn default() -> Self {
        let mut auth = Self::empty();
        auth.ensure_bootstrap_root();
        auth
    }
}

impl AuthCatalog {
    /// Unpopulated auth state for temporary schema catalogs. Real catalog
    /// bootstrap must explicitly install the root account.
    pub(crate) fn empty() -> Self {
        Self {
            epoch: 1,
            accounts: Default::default(),
            grants: GrantStore::new(),
            role_edges: Default::default(),
            default_roles: Default::default(),
            restrictions: Default::default(),
            indexes: empty_auth_indexes(),
            store_charge: None,
            account_memory: None,
        }
    }

    pub fn install_store_charge(
        &mut self,
        charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
    ) {
        self.store_charge = Some(Arc::new(charge));
    }

    pub fn store_is_charged(&self) -> bool {
        self.store_charge.is_some()
    }

    fn account_bytes(id: &str, account: &UserAccount) -> usize {
        std::mem::size_of::<UserAccount>()
            + 256
            + id.len()
            + account.key.user.capacity()
            + account.key.host.capacity()
            + account.plugin.capacity()
            + account.authentication_string.capacity()
            + account
                .secondary_authentication_string
                .as_ref()
                .map_or(0, String::capacity)
            + account
                .user_attributes_json
                .as_ref()
                .map_or(0, String::capacity)
            + account.password_history.capacity() * std::mem::size_of::<PasswordHistoryEntry>()
            + account
                .password_history
                .iter()
                .map(|h| h.authentication_string.capacity())
                .sum::<usize>()
    }
    pub fn clone_capacity_bytes(&self) -> usize {
        let accounts = self
            .account_memory
            .filter(|(revision, _)| *revision == self.accounts.revision())
            .map(|(_, bytes)| bytes)
            .unwrap_or_else(|| {
                self.accounts
                    .iter()
                    .map(|(id, a)| Self::account_bytes(id, a))
                    .sum()
            });
        accounts
            .saturating_add(self.grants.memory_bytes())
            .saturating_add(self.role_edges.memory_bytes())
            .saturating_add(self.default_roles.memory_bytes())
            .saturating_add(self.restrictions.memory_bytes())
    }

    pub fn rebuild_runtime_indexes(&mut self) {
        self.refresh_runtime_indexes(None);
    }

    pub(crate) fn refresh_runtime_indexes(&mut self, before: Option<&AuthCatalog>) {
        if self.account_memory.map(|(r, _)| r) != Some(self.accounts.revision()) {
            let bytes = if let Some((before, (_, mut bytes))) =
                before.and_then(|b| b.account_memory.map(|m| (b, m)))
            {
                for change in before.accounts.diff(&self.accounts) {
                    use imbl::ordmap::DiffItem::*;
                    match change {
                        Add(k, v) => bytes = bytes.saturating_add(Self::account_bytes(k, v)),
                        Remove(k, v) => bytes = bytes.saturating_sub(Self::account_bytes(k, v)),
                        Update {
                            old: (k, v),
                            new: (nk, nv),
                        } => {
                            bytes = bytes
                                .saturating_sub(Self::account_bytes(k, v))
                                .saturating_add(Self::account_bytes(nk, nv))
                        }
                    }
                }
                bytes
            } else {
                self.accounts
                    .iter()
                    .map(|(id, a)| Self::account_bytes(id, a))
                    .sum()
            };
            self.account_memory = Some((self.accounts.revision(), bytes));
        }

        let estimated_bytes = self
            .grants
            .memory_bytes()
            .saturating_mul(2)
            .saturating_add(self.role_edges.memory_bytes())
            .saturating_add(self.default_roles.memory_bytes())
            .saturating_add(self.restrictions.memory_bytes());
        let Some(charge) =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
                estimated_bytes,
            )
        else {
            // The index is an optional acceleration structure. Under memory
            // pressure authorization falls back to the persisted vectors;
            // correctness never depends on an ungoverned allocation.
            self.indexes = empty_auth_indexes();
            return;
        };
        let mut indexes = AuthIndexes::default();
        indexes.indexed = true;
        indexes._charge = Some(charge);
        if let Some(before) =
            before.filter(|b| b.indexes.indexed && b.indexes.grant_revision == b.grants.revision())
        {
            indexes.grants_by_grantee = before.indexes.grants_by_grantee.clone();
            let dirty: BTreeSet<_> = self.grants.changed_slots().collect();
            let mut affected: BTreeMap<AccountKey, Vec<(usize, Arc<GrantEntry>)>> = BTreeMap::new();
            for i in &dirty {
                if let Some(old) = before.grants.get(*i) {
                    affected.entry(old.grantee.clone()).or_default();
                }
                if let Some(new) = self.grants.shared(*i) {
                    affected
                        .entry(new.grantee.clone())
                        .or_default()
                        .push((*i, new));
                }
            }
            for (principal, additions) in affected {
                let mut entries = indexes
                    .grants_by_grantee
                    .get(&principal)
                    .map(|v| v.as_ref().clone())
                    .unwrap_or_default();
                entries.retain(|(i, _)| !dirty.contains(i));
                entries.extend(additions);
                entries.sort_unstable_by(|a, b| {
                    compare_scope_parts(scope_parts(&a.1.scope), scope_parts(&b.1.scope))
                });
                if entries.is_empty() {
                    indexes.grants_by_grantee.remove(&principal);
                } else {
                    indexes
                        .grants_by_grantee
                        .insert(principal, Arc::new(entries));
                }
            }
        } else {
            for (idx, grant) in self.grants.shared_rows() {
                let bucket = indexes
                    .grants_by_grantee
                    .entry(grant.grantee.clone())
                    .or_default();
                Arc::make_mut(bucket).push((idx, grant));
            }
            let keys: Vec<_> = indexes.grants_by_grantee.keys().cloned().collect();
            for key in keys {
                Arc::make_mut(indexes.grants_by_grantee.get_mut(&key).unwrap()).sort_unstable_by(
                    |a, b| compare_scope_parts(scope_parts(&a.1.scope), scope_parts(&b.1.scope)),
                );
            }
        }
        // Pattern schemas are a separate, normally empty candidate set.
        indexes.schema_patterns = before
            .map(|b| b.indexes.schema_patterns.clone())
            .unwrap_or_default();
        let pattern_keys: BTreeSet<_> = if let Some(before) = before.filter(|b| b.indexes.indexed) {
            self.grants
                .changed_slots()
                .flat_map(|i| {
                    [
                        before.grants.get(i).map(|g| g.grantee.clone()),
                        self.grants.get(i).map(|g| g.grantee.clone()),
                    ]
                })
                .flatten()
                .collect()
        } else {
            indexes.grants_by_grantee.keys().cloned().collect()
        };
        for key in pattern_keys {
            let patterns: Vec<_> = indexes.grants_by_grantee.get(&key).into_iter().flat_map(|v| v.iter())
                .filter(|(_, g)| matches!(&g.scope, PrivilegeScope::Database { database } if schema_pattern::has_pattern(database)))
                .map(|(_, g)| g.clone()).collect();
            if patterns.is_empty() {
                indexes.schema_patterns.remove(&key);
            } else {
                indexes.schema_patterns.insert(key, Arc::new(patterns));
            }
        }
        if let Some(before) =
            before.filter(|b| b.indexes.indexed && b.indexes.grant_revision == b.grants.revision())
        {
            indexes.grants_by_scope = before.indexes.grants_by_scope.clone();
            let dirty: BTreeSet<_> = self.grants.changed_slots().collect();
            let mut affected: BTreeMap<u64, Vec<(usize, Arc<GrantEntry>)>> = BTreeMap::new();
            for i in &dirty {
                if let Some(g) = before.grants.get(*i) {
                    affected
                        .entry(scope_fingerprint(scope_parts(&g.scope)))
                        .or_default();
                }
                if let Some(g) = self.grants.shared(*i) {
                    affected
                        .entry(scope_fingerprint(scope_parts(&g.scope)))
                        .or_default()
                        .push((*i, g));
                }
            }
            for (scope, additions) in affected {
                let mut entries = indexes
                    .grants_by_scope
                    .get(&scope)
                    .map(|v| v.as_ref().clone())
                    .unwrap_or_default();
                entries.retain(|(i, _)| !dirty.contains(i));
                entries.extend(additions);
                if entries.is_empty() {
                    indexes.grants_by_scope.remove(&scope);
                } else {
                    indexes.grants_by_scope.insert(scope, Arc::new(entries));
                }
            }
        } else {
            for (i, g) in self.grants.shared_rows() {
                Arc::make_mut(
                    indexes
                        .grants_by_scope
                        .entry(scope_fingerprint(scope_parts(&g.scope)))
                        .or_default(),
                )
                .push((i, g));
            }
        }
        indexes.grant_revision = self.grants.revision();
        indexes.role_revision = self.role_edges.revision();
        indexes.default_revision = self.default_roles.revision();
        indexes.restriction_revision = self.restrictions.revision();
        let previous = before.filter(|b| b.indexes.indexed);
        indexes.roles_by_target = row_index::refresh(
            &self.role_edges,
            previous.map(|b| &b.role_edges),
            previous.map(|b| &b.indexes.roles_by_target),
            |e| &e.to_account,
        );
        indexes.defaults_by_target = row_index::refresh(
            &self.default_roles,
            previous.map(|b| &b.default_roles),
            previous.map(|b| &b.indexes.defaults_by_target),
            |e| &e.to_account,
        );
        indexes.restrictions_by_grantee = row_index::refresh(
            &self.restrictions,
            previous.map(|b| &b.restrictions),
            previous.map(|b| &b.indexes.restrictions_by_grantee),
            |e| &e.grantee,
        );
        self.indexes = Arc::new(indexes);
    }

    pub(crate) fn grants_for<'a, 'b>(&'a self, account: &'b AccountKey) -> GrantIter<'a, 'b> {
        let mode = if self.indexes.indexed && self.indexes.grant_revision == self.grants.revision()
        {
            GrantIterMode::Indexed(
                self.indexes
                    .grants_by_grantee
                    .get(account)
                    .map(|v| v.as_slice())
                    .unwrap_or(&[])
                    .iter(),
            )
        } else {
            GrantIterMode::Full {
                account,
                rows: Box::new(self.grants.iter()),
            }
        };
        GrantIter { mode }
    }

    pub(crate) fn grant_positions_for(&self, account: &AccountKey) -> Vec<usize> {
        if self.indexes.indexed && self.indexes.grant_revision == self.grants.revision() {
            self.indexes
                .grants_by_grantee
                .get(account)
                .map(|v| v.iter().map(|e| e.0).collect())
                .unwrap_or_default()
        } else {
            self.grants
                .iter()
                .enumerate()
                .filter_map(|(i, g)| (&g.grantee == account).then_some(i))
                .collect()
        }
    }

    #[cfg(test)]
    fn indexed_grant_count(&self, account: &AccountKey) -> Option<usize> {
        self.indexes.indexed.then(|| {
            self.indexes
                .grants_by_grantee
                .get(account)
                .map_or(0, |v| v.len())
        })
    }

    pub fn grants_for_proxy(
        &self,
        grantee: &AccountKey,
        proxied: &AccountKey,
        need_grant_option: bool,
    ) -> bool {
        self.grants_for(grantee).any(|grant| {
            matches!(&grant.privilege, Privilege::Dynamic(name) if name == "PROXY")
                && matches!(&grant.scope, PrivilegeScope::Proxy { user, host }
                    if (user.is_empty() && host.is_empty()) || (user == &proxied.user && host.eq_ignore_ascii_case(&proxied.host)))
                && (!need_grant_option || grant.grant_option)
        })
    }

    pub(crate) fn account_has_proxy(
        &self,
        ctx: &AuthorizationContext,
        proxied: &AccountKey,
        grant_option: bool,
    ) -> bool {
        self.grants_for_proxy(&ctx.authenticated_as, proxied, grant_option)
            || role_cache::with_roles(self, ctx, |roles| {
                roles
                    .iter()
                    .any(|r| self.grants_for_proxy(r, proxied, grant_option))
            })
    }

    pub fn ensure_bootstrap_root(&mut self) {
        let password =
            crate::util::env::var("MYDB_ROOT_PASSWORD").unwrap_or_else(|_| "root".into());
        let root = AccountKey::new("root", "localhost");
        self.accounts.entry(account_id(&root)).or_insert_with(|| {
            let plugin = default_auth_plugin();
            UserAccount::new(
                root.clone(),
                plugin.clone(),
                password_auth_string(&plugin, &password),
            )
        });
        // Normalize any legacy `Dynamic("ALL")` rows surviving from older
        // snapshots before we top up bootstrap grants — keeps invariants
        // simple downstream (no ALL wildcard in `self.grants` after init).
        self.normalize_legacy_all_grants();
        for privilege in all_privileges_for_scope(&PrivilegeScope::Global) {
            let already = self.grants.iter().any(|g| {
                g.grantee == root && g.scope == PrivilegeScope::Global && g.privilege == *privilege
            });
            if !already {
                self.grants.push(GrantEntry {
                    updated_ms: crate::storage::naming::current_unix_ms(),
                    grantor: root.clone(),
                    grantee: root.clone(),
                    privilege: privilege.clone(),
                    scope: PrivilegeScope::Global,
                    grant_option: true,
                    origin: GrantOrigin::Explicit,
                });
            }
        }
        // Bootstrap root also gets every default dynamic privilege so it can
        // perform replication, backup, system-variable admin work etc. out of
        // the box. MySQL's mysql_secure_installation runs the equivalent.
        for privilege in default_dynamic_privileges() {
            let already = self.grants.iter().any(|g| {
                g.grantee == root && g.scope == PrivilegeScope::Global && g.privilege == *privilege
            });
            if !already {
                self.grants.push(GrantEntry {
                    updated_ms: crate::storage::naming::current_unix_ms(),
                    grantor: root.clone(),
                    grantee: root.clone(),
                    privilege: privilege.clone(),
                    scope: PrivilegeScope::Global,
                    grant_option: true,
                    origin: GrantOrigin::Explicit,
                });
            }
        }
        for (privilege, scope) in [
            (Privilege::GrantOption, PrivilegeScope::Global),
            (
                Privilege::Dynamic("PROXY".into()),
                PrivilegeScope::Proxy {
                    user: String::new(),
                    host: String::new(),
                },
            ),
        ] {
            if !self
                .grants
                .iter()
                .any(|g| g.grantee == root && g.privilege == privilege && g.scope == scope)
            {
                self.grants.push(GrantEntry {
                    updated_ms: crate::storage::naming::current_unix_ms(),
                    grantor: root.clone(),
                    grantee: root.clone(),
                    privilege,
                    scope,
                    grant_option: true,
                    origin: GrantOrigin::Explicit,
                });
            }
        }
        self.rebuild_runtime_indexes();
    }

    /// Migrate any pre-`auth-mysql80-compat` `Dynamic("ALL")` rows into the
    /// expanded static-privilege representation so `privilege_implies` no
    /// longer needs an ALL wildcard. Idempotent. Called from
    /// `ensure_bootstrap_root` and from any deserialization seam.
    pub fn normalize_legacy_all_grants(&mut self) {
        let mut additions: Vec<GrantEntry> = Vec::new();
        let mut keep: Vec<GrantEntry> = Vec::with_capacity(self.grants.len());
        for grant in std::mem::take(&mut self.grants) {
            let is_legacy_all = matches!(
                &grant.privilege,
                Privilege::Dynamic(name) if name == "ALL" || name == "ALL PRIVILEGES"
            );
            if is_legacy_all {
                for privilege in all_privileges_for_scope(&grant.scope) {
                    additions.push(GrantEntry {
                        updated_ms: crate::storage::naming::current_unix_ms(),
                        grantor: grant.grantor.clone(),
                        grantee: grant.grantee.clone(),
                        privilege: privilege.clone(),
                        scope: grant.scope.clone(),
                        grant_option: grant.grant_option,
                        origin: grant.origin,
                    });
                }
            } else {
                keep.push(grant);
            }
        }
        keep.extend(additions);
        // Deduplicate — legacy snapshots may have had both ALL and an
        // explicit SELECT for the same (grantee, scope). OR the grant_option
        // when collapsing so a strong row beats a weak one.
        keep.sort_by(|a, b| {
            (
                a.grantee.user.as_str(),
                a.grantee.host.as_str(),
                scope_sort_key(&a.scope),
                a.privilege.sql_name(),
            )
                .cmp(&(
                    b.grantee.user.as_str(),
                    b.grantee.host.as_str(),
                    scope_sort_key(&b.scope),
                    b.privilege.sql_name(),
                ))
        });
        let mut deduped: Vec<GrantEntry> = Vec::with_capacity(keep.len());
        for grant in keep {
            if let Some(prev) = deduped.last_mut() {
                if prev.grantee == grant.grantee
                    && prev.scope == grant.scope
                    && prev.privilege == grant.privilege
                {
                    prev.grant_option |= grant.grant_option;
                    continue;
                }
            }
            deduped.push(grant);
        }
        self.grants = deduped.into();
    }

    pub fn bump_epoch(&mut self) {
        self.epoch = self.epoch.saturating_add(1).max(1);
    }

    pub fn match_account(&self, user: &str, host: &str) -> Option<&UserAccount> {
        self.accounts
            .values()
            .filter(|acct| acct.key.user == user || acct.key.user.is_empty())
            .filter(|acct| host_matches(&acct.key.host, host))
            // MySQL sorts matching rows by Host specificity first, then puts
            // a named user before an anonymous user only when Host is equal.
            // Reversing those keys lets a broad `'name'@'%'` shadow the more
            // specific anonymous `''@'localhost'` row and authenticates under
            // the wrong security identity.
            .max_by_key(|acct| (host_specificity(&acct.key.host), !acct.key.user.is_empty()))
    }

    pub fn active_default_roles(&self, account: &AccountKey) -> BTreeSet<AccountKey> {
        if !self.indexes.indexed || self.indexes.default_revision != self.default_roles.revision() {
            return self
                .default_roles
                .iter()
                .filter(|edge| &edge.to_account == account)
                .map(|edge| edge.from_role.clone())
                .collect();
        }
        self.indexes
            .defaults_by_target
            .get(account)
            .into_iter()
            .flat_map(|rows| rows.iter())
            .map(|(_, row)| row.as_ref())
            .map(|edge| edge.from_role.clone())
            .collect()
    }

    /// Roles granted directly to `account` (one hop, not the transitive
    /// closure). This is the set `SET ROLE ALL` may activate.
    pub fn roles_granted_directly(&self, account: &AccountKey) -> BTreeSet<AccountKey> {
        if !self.indexes.indexed || self.indexes.role_revision != self.role_edges.revision() {
            return self
                .role_edges
                .iter()
                .filter(|edge| &edge.to_account == account)
                .map(|edge| edge.from_role.clone())
                .collect();
        }
        self.indexes
            .roles_by_target
            .get(account)
            .into_iter()
            .flat_map(|rows| rows.iter())
            .map(|(_, row)| row.as_ref())
            .map(|e| e.from_role.clone())
            .collect()
    }

    /// Mandatory roles (from the `mandatory_roles` sysvar) that actually exist
    /// as roles in this catalog. MySQL silently ignores mandatory-role names
    /// that don't resolve to a real role.
    pub fn mandatory_role_keys(&self) -> BTreeSet<AccountKey> {
        // The usual empty setting must not serialize all role checks on a
        // process-wide mutex. The setter publishes this flag under its lock.
        if MANDATORY_ROLES_EMPTY.load(Ordering::Acquire) {
            return BTreeSet::new();
        }
        parse_mandatory_roles(&mandatory_roles_raw())
            .into_iter()
            .filter(|key| {
                self.accounts
                    .get(&account_id(key))
                    .map(|_| true)
                    .unwrap_or(false)
            })
            .collect()
    }

    /// Roles applicable to `account`: everything granted to it directly, plus
    /// the mandatory roles that apply to everyone.
    pub fn applicable_roles(&self, account: &AccountKey) -> BTreeSet<AccountKey> {
        let mut out = self.roles_granted_directly(account);
        out.extend(self.mandatory_role_keys());
        out
    }

    /// Roles to activate at login, honouring `activate_all_roles_on_login`
    /// and `mandatory_roles`. When the sysvar is ON we activate every role
    /// granted to the account plus mandatory roles; otherwise only the
    /// account's default roles (MySQL 8.0 semantics — mandatory roles are
    /// applicable but not auto-activated unless the sysvar is ON).
    pub fn login_active_roles(&self, account: &AccountKey) -> BTreeSet<AccountKey> {
        if activate_all_roles_on_login() {
            let mut out = self.roles_granted_directly(account);
            out.extend(self.mandatory_role_keys());
            out
        } else {
            self.active_default_roles(account)
        }
    }

    pub fn role_closure(&self, seed: &BTreeSet<AccountKey>) -> BTreeSet<AccountKey> {
        let mut seen = BTreeSet::new();
        let mut queue = VecDeque::new();
        for role in seed {
            if seen.insert(role.clone()) {
                queue.push_back(role.clone());
            }
        }
        while let Some(role) = queue.pop_front() {
            if !self.indexes.indexed || self.indexes.role_revision != self.role_edges.revision() {
                for edge in self
                    .role_edges
                    .iter()
                    .filter(|edge| edge.to_account == role)
                {
                    if seen.insert(edge.from_role.clone()) {
                        queue.push_back(edge.from_role.clone());
                    }
                }
                continue;
            }
            for edge in self
                .indexes
                .roles_by_target
                .get(&role)
                .into_iter()
                .flat_map(|rows| rows.iter())
                .map(|(_, row)| row.as_ref())
            {
                if seen.insert(edge.from_role.clone()) {
                    queue.push_back(edge.from_role.clone());
                }
            }
        }
        seen
    }

    /// Resolve only roles still applicable in this immutable catalog. Session
    /// role sets can outlive REVOKE; all authorization and metadata consumers
    /// must use this intersection before expanding inherited roles.
    pub fn effective_roles(&self, ctx: &AuthorizationContext) -> BTreeSet<AccountKey> {
        role_cache::with_roles(self, ctx, Clone::clone)
    }

    fn compute_effective_roles(&self, ctx: &AuthorizationContext) -> BTreeSet<AccountKey> {
        if ctx.active_roles.is_empty() {
            return BTreeSet::new();
        }
        let applicable = self.applicable_roles(&ctx.authenticated_as);
        self.role_closure(
            &ctx.active_roles
                .intersection(&applicable)
                .cloned()
                .collect(),
        )
    }

    pub fn account_has_privilege(
        &self,
        ctx: &AuthorizationContext,
        required: &RequiredPrivilege,
        need_grant_option: bool,
    ) -> bool {
        if need_grant_option
            && !matches!(
                required.privilege,
                Privilege::Dynamic(_) | Privilege::GrantOption
            )
            && !self.account_has_privilege(
                ctx,
                &RequiredPrivilege {
                    privilege: Privilege::GrantOption,
                    scope: required.scope.clone(),
                },
                false,
            )
        {
            return false;
        }
        if self.principal_has_privilege(&ctx.authenticated_as, required, need_grant_option) {
            return true;
        }
        role_cache::with_roles(self, ctx, |roles| {
            if roles.len() >= 8
                && !matches!(required.privilege, Privilege::GrantOption)
                && self.indexes.indexed
                && self.indexes.grant_revision == self.grants.revision()
            {
                self.roles_have_privilege(roles, required, need_grant_option)
            } else {
                roles.iter().any(|principal| {
                    self.principal_has_privilege(principal, required, need_grant_option)
                })
            }
        })
    }

    fn roles_have_privilege(
        &self,
        roles: &BTreeSet<AccountKey>,
        required: &RequiredPrivilege,
        need_grant_option: bool,
    ) -> bool {
        let allows = |g: &GrantEntry| {
            grant_satisfies_privilege(g, &required.privilege)
                && (!need_grant_option
                    || !matches!(required.privilege, Privilege::Dynamic(_))
                    || g.grant_option)
                && roles.contains(&g.grantee)
                && (if need_grant_option {
                    scope_allows_delegation(&g.scope, &required.scope)
                } else {
                    if matches!(required.privilege, Privilege::GrantOption) {
                        grant_option_scope_implies(&g.scope, &required.scope)
                    } else {
                        scope_implies(&g.scope, &required.scope)
                    }
                })
                && (!matches!(g.scope, PrivilegeScope::Global)
                    || !self.restrictions_block(&g.grantee, required))
        };
        let key = scope_parts(&required.scope);
        let candidates = [
            (0, "", "", ""),
            key,
            (1, key.1, "", ""),
            (2, key.1, key.2, ""),
        ];
        if candidates.iter().any(|key| {
            self.indexes
                .grants_by_scope
                .get(&scope_fingerprint(*key))
                .is_some_and(|rows| rows.iter().any(|(_, g)| allows(g)))
        }) {
            return true;
        }
        !self.indexes.schema_patterns.is_empty()
            && !partial_revokes_enabled()
            && roles.iter().any(|role| {
                self.indexes
                    .schema_patterns
                    .get(role)
                    .is_some_and(|rows| rows.iter().any(|g| allows(g)))
            })
    }

    /// Information-schema grantability is local to the stored grant level:
    /// database/global GO must not mark a table row grantable. Column rows
    /// share their table's static GO capability; dynamic privileges do not.
    pub(crate) fn metadata_grantable(&self, grant: &GrantEntry) -> bool {
        if grant.grant_option || matches!(grant.privilege, Privilege::Dynamic(_)) {
            return grant.grant_option;
        }
        let level = grant_option_level(&grant.scope);
        let allows = |g: &GrantEntry| {
            !matches!(g.privilege, Privilege::Dynamic(_))
                && (g.grant_option || matches!(g.privilege, Privilege::GrantOption))
                && scope_equal(&grant_option_level(&g.scope), &level)
        };
        if self.indexes.indexed && self.indexes.grant_revision == self.grants.revision() {
            let Some(rows) = self.indexes.grants_by_grantee.get(&grant.grantee) else {
                return false;
            };
            let key = scope_parts(&level);
            let start = rows
                .partition_point(|(_, g)| compare_scope_parts(scope_parts(&g.scope), key).is_lt());
            if rows[start..]
                .iter()
                .take_while(|(_, g)| compare_scope_parts(scope_parts(&g.scope), key).is_eq())
                .any(|(_, g)| allows(g))
            {
                return true;
            }
            if let PrivilegeScope::Table { database, table } = &level {
                let key = (3, database.as_str(), table.as_str(), "");
                let start = rows.partition_point(|(_, g)| {
                    compare_scope_parts(scope_parts(&g.scope), key).is_lt()
                });
                return rows[start..].iter().take_while(|(_,g)|matches!(&g.scope,PrivilegeScope::Column{database:d,table:t,..} if d.eq_ignore_ascii_case(database)&&t.eq_ignore_ascii_case(table))).any(|(_,g)|allows(g));
            }
            return false;
        }
        self.grants_for(&grant.grantee).any(allows)
    }

    fn principal_has_privilege(
        &self,
        principal: &AccountKey,
        required: &RequiredPrivilege,
        need_grant_option: bool,
    ) -> bool {
        let allows = |grant: &GrantEntry| {
            grant_satisfies_privilege(grant, &required.privilege)
                && (if need_grant_option {
                    scope_allows_delegation(&grant.scope, &required.scope)
                } else {
                    if matches!(required.privilege, Privilege::GrantOption) {
                        grant_option_scope_implies(&grant.scope, &required.scope)
                    } else {
                        scope_implies(&grant.scope, &required.scope)
                    }
                })
                && (!need_grant_option
                    || !matches!(required.privilege, Privilege::Dynamic(_))
                    || grant.grant_option)
                && (!matches!(grant.scope, PrivilegeScope::Global)
                    || !self.restrictions_block(principal, required))
        };
        if !self.indexes.indexed || self.indexes.grant_revision != self.grants.revision() {
            return self.grants_for(principal).any(allows);
        }
        if matches!(required.privilege, Privilege::GrantOption) {
            return self.grants_for(principal).any(allows);
        }
        let Some(indices) = self.indexes.grants_by_grantee.get(principal) else {
            return false;
        };
        if indices.len() <= 16 {
            return indices.iter().any(|idx| allows(&idx.1));
        }
        let key = scope_parts(&required.scope);
        let mut candidates = [(0, "", "", ""); 4];
        let mut count = 1;
        if key.0 != 0 {
            candidates[count] = key;
            count += 1;
        }
        if matches!(
            required.scope,
            PrivilegeScope::Table { .. }
                | PrivilegeScope::Column { .. }
                | PrivilegeScope::Routine { .. }
        ) {
            candidates[count] = (1, key.1, "", "");
            count += 1;
        }
        if matches!(required.scope, PrivilegeScope::Column { .. }) {
            candidates[count] = (2, key.1, key.2, "");
            count += 1;
        }
        candidates[..count].iter().any(|candidate| {
            let start = indices.partition_point(|idx| {
                compare_scope_parts(scope_parts(&idx.1.scope), *candidate).is_lt()
            });
            indices[start..]
                .iter()
                .take_while(|idx| {
                    compare_scope_parts(scope_parts(&idx.1.scope), *candidate).is_eq()
                })
                .any(|idx| allows(&idx.1))
        }) || (!self.indexes.schema_patterns.is_empty()
            && !partial_revokes_enabled()
            && self
                .indexes
                .schema_patterns
                .get(principal)
                .is_some_and(|grants| grants.iter().any(|g| allows(g))))
    }

    pub(crate) fn global_restrictions(
        &self,
        principals: &BTreeSet<AccountKey>,
        privilege: &Privilege,
    ) -> BTreeSet<String> {
        let mut result: Option<BTreeSet<String>> = None;
        for principal in principals {
            if !self.grants_for(principal).any(|g| {
                matches!(g.scope, PrivilegeScope::Global) && grant_satisfies_privilege(g, privilege)
            }) {
                continue;
            }
            let rows: Box<dyn Iterator<Item = &RestrictionEntry> + '_> = if self.indexes.indexed
                && self.indexes.restriction_revision == self.restrictions.revision()
            {
                Box::new(
                    self.indexes
                        .restrictions_by_grantee
                        .get(principal)
                        .into_iter()
                        .flat_map(|v| v.iter())
                        .map(|(_, r)| r.as_ref()),
                )
            } else {
                Box::new(self.restrictions.iter().filter(|r| &r.grantee == principal))
            };
            let restrictions: BTreeSet<_> = rows
                .filter(|r| privilege_implies(&r.privilege, privilege))
                .map(|r| r.database.to_ascii_lowercase())
                .collect();
            result = Some(match result {
                None => restrictions,
                Some(old) => old.intersection(&restrictions).cloned().collect(),
            });
            if result.as_ref().is_some_and(BTreeSet::is_empty) {
                break;
            }
        }
        let mut result = result.unwrap_or_default();
        result.retain(|database| !principals.iter().any(|p| self.grants_for(p).any(|g| {
            matches!(&g.scope,PrivilegeScope::Database{database:d} if database_scope_matches(d,database)) && grant_satisfies_privilege(g,privilege)
        })));
        result
    }
    pub(crate) fn delegated_restrictions(
        &self,
        ctx: &AuthorizationContext,
        privilege: &Privilege,
    ) -> BTreeSet<String> {
        let mut principals = self.effective_roles(ctx);
        principals.insert(ctx.authenticated_as.clone());
        let mut result = self.global_restrictions(&principals, privilege);
        if !matches!(privilege, Privilege::Dynamic(_)) {
            result.extend(self.global_restrictions(&principals, &Privilege::GrantOption));
        }
        result
    }

    /// True if `ctx` may GRANT or REVOKE `role` to/from other accounts. MySQL
    /// 8.0 requires one of: the global `ROLE_ADMIN` privilege (SUPER implies
    /// it), or holding the role itself WITH ADMIN OPTION — directly or via an
    /// active role in the session's closure. Without this gate any
    /// authenticated user could grant any existing role (including DBA-level
    /// roles) to itself — a privilege-escalation hole (B1).
    pub fn account_can_administer_role(
        &self,
        ctx: &AuthorizationContext,
        role: &AccountKey,
    ) -> bool {
        let role_admin = RequiredPrivilege {
            privilege: Privilege::Dynamic("ROLE_ADMIN".to_string()),
            scope: PrivilegeScope::Global,
        };
        if self.account_has_privilege(ctx, &role_admin, false) {
            return true;
        }
        // Otherwise the authenticated principal (or any active role in its
        // closure) must hold an ADMIN OPTION edge on this specific role.
        let mut principals = BTreeSet::new();
        principals.insert(ctx.authenticated_as.clone());
        principals.extend(self.effective_roles(ctx));
        self.role_edges.iter().any(|edge| {
            edge.admin_option && &edge.from_role == role && principals.contains(&edge.to_account)
        })
    }

    /// Whether an authorization ID is a MySQL system account. SYSTEM_USER is
    /// a dynamic privilege, not a persisted account flag; it can be held
    /// directly or through an applicable role. Use all applicable roles here
    /// so account-management protection does not depend on the target's
    /// current session activation choices.
    pub fn account_is_system_user(&self, account: &AccountKey) -> bool {
        let ctx = AuthorizationContext {
            authenticated_as: account.clone(),
            active_roles: self.applicable_roles(account).into(),
            epoch: self.epoch,
        };
        self.account_has_privilege(
            &ctx,
            &RequiredPrivilege {
                privilege: Privilege::Dynamic("SYSTEM_USER".to_string()),
                scope: PrivilegeScope::Global,
            },
            false,
        )
    }

    /// Can `ctx`'s account see the object metadata for `schema` (and, when
    /// given, the specific `table`)? True if the account — or any role in its
    /// active closure — holds *any* privilege whose scope covers the object:
    /// a global grant, a grant on the schema, or a grant on the table / one of
    /// its columns (or, for a schema-level query, any routine in the schema).
    /// This backs `information_schema` row-level visibility filtering, matching
    /// MySQL: an account only sees metadata for objects it has some privilege
    /// on. `USAGE`-only accounts (no grants) see nothing.
    pub fn account_can_access_schema(
        &self,
        ctx: &AuthorizationContext,
        schema: &str,
        table: Option<&str>,
    ) -> bool {
        let allows = |principal: &AccountKey| {
            self.metadata_grants_any(principal, schema, table, |grant| {
                // Administrative/routine privileges can expose the schema name,
                // but do not confer visibility of unrelated table objects.
                if table.is_some()
                    && !matches!(
                        grant.privilege,
                        Privilege::Select
                            | Privilege::Insert
                            | Privilege::Update
                            | Privilege::Delete
                            | Privilege::Create
                            | Privilege::Drop
                            | Privilege::References
                            | Privilege::Index
                            | Privilege::Alter
                            | Privilege::CreateView
                            | Privilege::ShowView
                            | Privilege::Trigger
                    )
                {
                    return false;
                }

                if matches!(grant.scope, PrivilegeScope::Global)
                    && self.restrictions_block(
                        principal,
                        &RequiredPrivilege {
                            privilege: grant.privilege.clone(),
                            scope: PrivilegeScope::Database {
                                database: schema.to_string(),
                            },
                        },
                    )
                {
                    return false;
                }
                match &grant.scope {
                    PrivilegeScope::Global => true,
                    PrivilegeScope::Database { database } => {
                        database_scope_matches(database, schema)
                    }
                    PrivilegeScope::Table {
                        database,
                        table: grant_table,
                    } => {
                        database.eq_ignore_ascii_case(schema)
                            && table.is_none_or(|want| grant_table.eq_ignore_ascii_case(want))
                    }
                    PrivilegeScope::Column {
                        database,
                        table: grant_table,
                        ..
                    } => {
                        database.eq_ignore_ascii_case(schema)
                            && table.is_none_or(|want| grant_table.eq_ignore_ascii_case(want))
                    }
                    // A routine grant makes the *schema* visible (there is a
                    // routine in it the account may reach) but says nothing about
                    // a specific table.
                    PrivilegeScope::Routine { database, .. } => {
                        table.is_none() && database.eq_ignore_ascii_case(schema)
                    }
                    PrivilegeScope::Proxy { .. } => false,
                }
            })
        };
        allows(&ctx.authenticated_as)
            || role_cache::with_roles(self, ctx, |roles| roles.iter().any(allows))
    }

    // Metadata probes for a known object need only the same four scope
    // ranges as an ACL lookup; never scan an administrator's entire bucket.
    fn metadata_grants_any(
        &self,
        principal: &AccountKey,
        schema: &str,
        table: Option<&str>,
        mut allows: impl FnMut(&GrantEntry) -> bool,
    ) -> bool {
        let Some(table) = table else {
            return self.grants_for(principal).any(allows);
        };
        if !self.indexes.indexed || self.indexes.grant_revision != self.grants.revision() {
            return self.grants_for(principal).any(allows);
        }
        let Some(rows) = self.indexes.grants_by_grantee.get(principal) else {
            return false;
        };
        for key in [(0, "", "", ""), (1, schema, "", ""), (2, schema, table, "")] {
            let start = rows
                .partition_point(|(_, g)| compare_scope_parts(scope_parts(&g.scope), key).is_lt());
            if rows[start..]
                .iter()
                .take_while(|(_, g)| compare_scope_parts(scope_parts(&g.scope), key).is_eq())
                .any(|(_, g)| allows(g))
            {
                return true;
            }
        }
        let key = (3, schema, table, "");
        let start =
            rows.partition_point(|(_, g)| compare_scope_parts(scope_parts(&g.scope), key).is_lt());
        if rows[start..].iter().take_while(|(_,g)|matches!(&g.scope,PrivilegeScope::Column{database:d,table:t,..} if d.eq_ignore_ascii_case(schema)&&t.eq_ignore_ascii_case(table))).any(|(_,g)|allows(g)) {return true;}
        !partial_revokes_enabled()
            && self
                .indexes
                .schema_patterns
                .get(principal)
                .is_some_and(|patterns| patterns.iter().any(|g| allows(g)))
    }

    pub(crate) fn account_can_use_database(
        &self,
        ctx: &AuthorizationContext,
        database: &str,
    ) -> bool {
        let scope = PrivilegeScope::Database {
            database: database.to_string(),
        };
        let allows = |principal: &AccountKey| {
            self.grants_for(principal).any(|g| {
                if matches!(g.privilege, Privilege::GrantOption | Privilege::Dynamic(_)) {
                    return false;
                }
                match &g.scope {
                    PrivilegeScope::Global => {
                        all_privileges_for_scope(&scope).contains(&g.privilege)
                            && !self.restrictions_block(
                                principal,
                                &RequiredPrivilege {
                                    privilege: g.privilege.clone(),
                                    scope: scope.clone(),
                                },
                            )
                    }
                    PrivilegeScope::Database { database: d } => database_scope_matches(d, database),
                    PrivilegeScope::Table { database: d, .. }
                    | PrivilegeScope::Column { database: d, .. }
                    | PrivilegeScope::Routine { database: d, .. } => {
                        d.eq_ignore_ascii_case(database)
                    }
                    PrivilegeScope::Proxy { .. } => false,
                }
            })
        };
        allows(&ctx.authenticated_as)
            || role_cache::with_roles(self, ctx, |roles| roles.iter().any(allows))
    }

    /// True if any RestrictionEntry vetoes `required` for `grantee`. Schema
    /// (db.*), table (db.t), and column (db.t.c) requireds all match a
    /// schema-scoped restriction (which is the only scope MySQL supports).
    fn restrictions_block(&self, grantee: &AccountKey, required: &RequiredPrivilege) -> bool {
        let target_db = match &required.scope {
            PrivilegeScope::Database { database } => Some(database.as_str()),
            PrivilegeScope::Table { database, .. } => Some(database.as_str()),
            PrivilegeScope::Column { database, .. } => Some(database.as_str()),
            PrivilegeScope::Routine { database, .. } => Some(database.as_str()),
            PrivilegeScope::Global | PrivilegeScope::Proxy { .. } => None,
        };
        let Some(target_db) = target_db else {
            // Global requireds can't be partially-revoked at a schema scope
            // (`REVOKE SHUTDOWN ON db.* FROM u` is meaningless; MySQL itself
            // rejects this at REVOKE time).
            return false;
        };
        if !self.indexes.indexed
            || self.indexes.restriction_revision != self.restrictions.revision()
        {
            return self.restrictions.iter().any(|r| {
                &r.grantee == grantee
                    && privilege_implies(&r.privilege, &required.privilege)
                    && r.database.eq_ignore_ascii_case(target_db)
            });
        }
        self.indexes
            .restrictions_by_grantee
            .get(grantee)
            .into_iter()
            .flat_map(|rows| rows.iter())
            .map(|(_, row)| row.as_ref())
            .any(|r| {
                privilege_implies(&r.privilege, &required.privilege)
                    && r.database.eq_ignore_ascii_case(target_db)
            })
    }
}

pub(crate) fn require_database_access(
    db: &crate::storage::Database,
    session: &crate::engine::api::SessionContext,
    database: &str,
) -> Result<(), QueryError> {
    if session.internal_system_session
        || database.eq_ignore_ascii_case("information_schema")
        || database.eq_ignore_ascii_case("performance_schema")
        || db
            .auth_catalog()
            .account_can_use_database(&session.authorization_context(), database)
    {
        return Ok(());
    }
    Err(mysql_errors::error(
        mysql_errors::ER_DBACCESS_DENIED_ERROR,
        format!(
            "Access denied for user {} to database '{}'",
            session.authenticated_as.display(),
            database
        ),
    ))
}

pub(crate) fn active_login_lock(account: &UserAccount, now_ms: u64) -> Option<u64> {
    if account.failed_login_attempts == 0 || account.password_lock_time_days.unwrap_or(0) == 0 {
        None
    } else {
        failed_login_active_lock(&account.key, now_ms)
    }
}

pub(crate) fn temporary_login_lock_error(account: &UserAccount, until_ms: u64) -> QueryError {
    let remaining = if until_ms == u64::MAX {
        "UNBOUNDED".to_string()
    } else {
        (until_ms
            .saturating_sub(current_time_ms())
            .div_ceil(86_400_000))
        .to_string()
    };
    mysql_errors::error(mysql_errors::ER_USER_ACCESS_DENIED_FOR_USER_ACCOUNT_BLOCKED_BY_PASSWORD_LOCK,
        format!("Access denied for user {}. Account is blocked for {} day(s) ({} day(s) remaining) due to {} consecutive failed logins.",account.key.display(),account.password_lock_time_days.map(|days|if days==u16::MAX {"UNBOUNDED".into()} else {days.to_string()}).unwrap_or_else(||"0".into()),remaining,account.failed_login_attempts))
}

pub fn access_denied(user: &str, host: &str) -> QueryError {
    mysql_errors::error(
        mysql_errors::ER_ACCESS_DENIED_ERROR,
        format!("Access denied for user '{user}'@'{host}'"),
    )
}

pub fn account_id(key: &AccountKey) -> String {
    // Length-prefix the user component. A plain `user@host` concatenation is
    // ambiguous because both legal account components may themselves contain
    // `@` (`'a@b'@'c'` collided with `'a'@'b@c'`).
    format!("{}:{}{}", key.user.len(), key.user, key.host)
}

pub fn privilege_denied(privilege: &Privilege, scope: &PrivilegeScope) -> QueryError {
    let specific_admin = match privilege {
        Privilege::Process => true,
        Privilege::Dynamic(name) => {
            name.eq_ignore_ascii_case("SYSTEM_VARIABLES_ADMIN")
                || name.eq_ignore_ascii_case("PERSIST_RO_VARIABLES_ADMIN")
        }
        _ => false,
    };
    if matches!(scope, PrivilegeScope::Global) && specific_admin {
        return mysql_errors::error(
            mysql_errors::ER_SPECIFIC_ACCESS_DENIED_ERROR,
            format!(
                "Access denied; you need (at least one of) the {} privilege(s) for this operation",
                privilege.sql_name()
            ),
        );
    }
    mysql_errors::error(
        mysql_errors::ER_TABLEACCESS_DENIED_ERROR,
        format!(
            "{} command denied for {}",
            privilege.sql_name(),
            scope_name(scope)
        ),
    )
}

pub fn verify_auth_response(plugin: &str, stored: &str, password: &[u8], seed: &[u8]) -> bool {
    if stored.is_empty() {
        return is_supported_auth_plugin(plugin)
            && (password.is_empty()
                || (matches!(plugin, "caching_sha2_password" | "sha256_password")
                    && password == [0]));
    }
    match plugin {
        "mysql_native_password" => verify_mysql_native(stored, password, seed),
        "caching_sha2_password" => verify_caching_sha2(stored, password, seed),
        "sha256_password" => verify_sha256_password(stored, password),
        _ => false,
    }
}

/// MySQL SHA-2 plugins use MAX_PLAINTEXT_LENGTH = 256 bytes. Bound
/// untrusted inputs before SHA-crypt's quadratic password expansion.
pub(crate) fn validate_password_length(plugin: &str, password: &str) -> Result<(), QueryError> {
    if matches!(plugin, "caching_sha2_password" | "sha256_password") && password.len() > 256 {
        return Err(mysql_errors::error(
            mysql_errors::ER_CANNOT_USER,
            format!("Password exceeds 256 bytes for authentication plugin '{plugin}'"),
        ));
    }
    Ok(())
}
pub(crate) fn checked_password_auth_string(
    plugin: &str,
    password: &str,
) -> Result<String, QueryError> {
    validate_password_length(plugin, password)?;
    Ok(password_auth_string(plugin, password))
}

pub fn password_auth_string(plugin: &str, password: &str) -> String {
    if password.is_empty() {
        return String::new();
    }
    match plugin {
        "caching_sha2_password" => caching_sha2_auth_string(password),
        "sha256_password" => sha256_password_auth_string(password),
        _ => mysql_native_auth_string(password),
    }
}

pub fn verify_plaintext_auth_string(plugin: &str, stored: &str, password: &str) -> bool {
    if stored.is_empty() {
        return is_supported_auth_plugin(plugin) && password.is_empty();
    }
    match plugin {
        "caching_sha2_password" => verify_sha256_crypt_string(stored, password, true),
        "sha256_password" => verify_sha256_crypt_string(stored, password, false),
        _ => constant_time_eq(
            mysql_native_auth_string(password).as_bytes(),
            stored.as_bytes(),
        ),
    }
}

pub fn default_auth_plugin() -> String {
    static PLUGIN: OnceLock<String> = OnceLock::new();
    PLUGIN
        .get_or_init(|| {
            crate::util::env::var("MYDB_DEFAULT_AUTH_PLUGIN")
                .ok()
                .filter(|plugin| is_supported_auth_plugin(plugin))
                .unwrap_or_else(|| "caching_sha2_password".to_string())
        })
        .clone()
}

pub fn is_supported_auth_plugin(plugin: &str) -> bool {
    matches!(
        plugin,
        "caching_sha2_password" | "mysql_native_password" | "sha256_password"
    )
}

pub fn is_valid_authentication_string(plugin: &str, value: &str) -> bool {
    if value.is_empty() {
        return is_supported_auth_plugin(plugin);
    }
    match plugin {
        "mysql_native_password" => {
            value.len() == 41
                && value.starts_with('*')
                && value[1..].bytes().all(|byte| byte.is_ascii_hexdigit())
        }
        "caching_sha2_password" => {
            value.len() == 70
                && value.starts_with("$A$")
                && value.as_bytes().get(6) == Some(&b'$')
                && value[3..6].bytes().all(|byte| byte.is_ascii_digit())
                && value.as_bytes()[7..27]
                    .iter()
                    .all(|byte| *byte != 0 && *byte != b'$' && *byte < 0x80)
                && value.as_bytes()[27..].iter().all(|byte| {
                    b"./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
                        .contains(byte)
                })
        }
        "sha256_password" => value.strip_prefix("$5$").is_some_and(|rest| {
            rest.split_once('$').is_some_and(|(salt, checksum)| {
                salt.as_bytes().len() == 20
                    && salt
                        .as_bytes()
                        .iter()
                        .all(|byte| *byte != 0 && *byte != b'$' && *byte < 0x80)
                    && checksum.len() == 43
                    && checksum.as_bytes().iter().all(|byte| {
                        b"./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
                            .contains(byte)
                    })
            })
        }),
        _ => false,
    }
}

pub fn mysql_native_auth_string(password: &str) -> String {
    let stage1 = Sha1::digest(password.as_bytes());
    let stage2 = Sha1::digest(stage1);
    let mut out = String::with_capacity(1 + stage2.len() * 2);
    out.push('*');
    crate::util::hex::append_upper(&mut out, &stage2);
    out
}

pub fn caching_sha2_auth_string(password: &str) -> String {
    let salt = random_crypt_salt(20);
    format!(
        "$A$005${salt}{}",
        sha256_crypt_checksum(password.as_bytes(), salt.as_bytes(), 5_000)
    )
}

pub fn sha256_password_auth_string(password: &str) -> String {
    let salt = random_crypt_salt(20);
    format!(
        "$5${salt}${}",
        sha256_crypt_checksum(password.as_bytes(), salt.as_bytes(), 5_000)
    )
}

fn random_crypt_salt(len: usize) -> String {
    const ALPHABET: &[u8] = b"./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    let mut random = vec![0u8; len];
    OsRng.fill_bytes(&mut random);
    random
        .into_iter()
        .map(|byte| ALPHABET[usize::from(byte) & 63] as char)
        .collect()
}

fn verify_sha256_crypt_string(stored: &str, password: &str, caching: bool) -> bool {
    if password.len() > 256 {
        return false;
    }
    let (salt, checksum, rounds) = if caching {
        if stored.len() != 7 + 20 + 43 || !stored.starts_with("$A$") {
            return false;
        }
        let Ok(thousands) = stored[3..6].parse::<u32>() else {
            return false;
        };
        if stored.as_bytes().get(6) != Some(&b'$') {
            return false;
        }
        (
            &stored[7..27],
            &stored[27..],
            thousands.saturating_mul(1_000),
        )
    } else {
        let Some(rest) = stored.strip_prefix("$5$") else {
            return false;
        };
        let (salt, checksum) = match rest.split_once('$') {
            Some(parts) => parts,
            None => return false,
        };
        (salt, checksum, 5_000)
    };
    let candidate = sha256_crypt_checksum(password.as_bytes(), salt.as_bytes(), rounds);
    constant_time_eq(candidate.as_bytes(), checksum.as_bytes())
}

/// SHA-256 crypt as used by MySQL caching_sha2_password (`$A$005$`, with a
/// 20-byte salt) and sha256_password (`$5$`). The round function and crypt
/// base64 permutation follow MySQL's `my_crypt_genhash` implementation.
fn sha256_crypt_checksum(password: &[u8], salt: &[u8], rounds: u32) -> String {
    let mut alternate = Sha256::new();
    alternate.update(password);
    alternate.update(salt);
    alternate.update(password);
    let alternate = alternate.finalize();

    let mut digest = Sha256::new();
    digest.update(password);
    digest.update(salt);
    let mut remaining = password.len();
    while remaining > 32 {
        digest.update(alternate);
        remaining -= 32;
    }
    digest.update(&alternate[..remaining]);
    let mut count = password.len();
    while count > 0 {
        if count & 1 != 0 {
            digest.update(alternate);
        } else {
            digest.update(password);
        }
        count >>= 1;
    }
    let mut current = digest.finalize().to_vec();

    let mut p_digest = Sha256::new();
    for _ in 0..password.len() {
        p_digest.update(password);
    }
    let p_digest = p_digest.finalize();
    let p_bytes = (0..password.len())
        .map(|idx| p_digest[idx % 32])
        .collect::<Vec<_>>();

    let mut s_digest = Sha256::new();
    for _ in 0..(16 + usize::from(current[0])) {
        s_digest.update(salt);
    }
    let s_digest = s_digest.finalize();
    let s_bytes = (0..salt.len())
        .map(|idx| s_digest[idx % 32])
        .collect::<Vec<_>>();

    for round in 0..rounds {
        let mut next = Sha256::new();
        if round & 1 != 0 {
            next.update(&p_bytes);
        } else {
            next.update(&current);
        }
        if round % 3 != 0 {
            next.update(&s_bytes);
        }
        if round % 7 != 0 {
            next.update(&p_bytes);
        }
        if round & 1 != 0 {
            next.update(&current);
        } else {
            next.update(&p_bytes);
        }
        current = next.finalize().to_vec();
    }

    let mut encoded = String::with_capacity(43);
    for (b2, b1, b0, count) in [
        (0, 10, 20, 4),
        (21, 1, 11, 4),
        (12, 22, 2, 4),
        (3, 13, 23, 4),
        (24, 4, 14, 4),
        (15, 25, 5, 4),
        (6, 16, 26, 4),
        (27, 7, 17, 4),
        (18, 28, 8, 4),
        (9, 19, 29, 4),
        (0, 31, 30, 3),
    ] {
        let high = if count == 3 && b1 == 31 && b0 == 30 {
            0
        } else {
            current[b2]
        };
        crypt_b64(&mut encoded, high, current[b1], current[b0], count);
    }
    encoded
}

fn crypt_b64(out: &mut String, b2: u8, b1: u8, b0: u8, count: usize) {
    const B64: &[u8] = b"./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    let mut value = (u32::from(b2) << 16) | (u32::from(b1) << 8) | u32::from(b0);
    for _ in 0..count {
        out.push(B64[(value & 0x3f) as usize] as char);
        value >>= 6;
    }
}

pub fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// MySQL 8.0 host-pattern matching, in priority order:
///   1. `'%'` — universal wildcard (fast path).
///   2. Literal IPv4 (no wildcard chars) compared verbatim against the
///      peer's address. We accept either the actual numeric peer or a
///      reverse-DNS hostname; literal compare hits if either matches.
///   3. `'1.2.3.0/255.255.255.0'` netmask: parse pattern as IPv4/mask,
///      apply mask to peer IPv4, compare. Mask must be a valid IPv4.
///   4. IPv6 literal — verbatim string compare (mydb does not currently
///      do canonicalization; if the peer string differs in case/zero
///      compression it will not match).
///   5. SQL LIKE-style `%`/`_` (legacy hostname pattern).
///
/// `skip_name_resolve` only matters when the pattern uses literal-DNS
/// form (i.e. not an IP and not netmask form) — see `match_account_with_options`.
pub fn host_matches(pattern: &str, host: &str) -> bool {
    if pattern == "%" {
        return true;
    }
    // `'user'@'localhost'`. In stock MySQL this account is reached only via the
    // Unix domain socket; mydb is TCP-only, so a local client connects over
    // loopback and presents its numeric peer IP ("127.0.0.1", "::1", …), never
    // the literal string "localhost". Without special-casing this, every
    // `@'localhost'` account — including ones users naturally create — would be
    // impossible to authenticate against (bug: access denied for a valid
    // account). Treat any loopback peer as matching the `localhost` pattern so
    // these accounts remain usable.
    if pattern.eq_ignore_ascii_case("localhost") {
        if host.eq_ignore_ascii_case("localhost") {
            return true;
        }
        return host
            .parse::<std::net::IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(false);
    }
    // Netmask form: 'ip/mask'.
    if let Some((net, mask)) = pattern.split_once('/') {
        if let (Some(net_v4), Some(mask_v4), Some(host_v4)) =
            (parse_ipv4(net), parse_ipv4(mask), parse_ipv4(host))
        {
            let masked = [
                host_v4[0] & mask_v4[0],
                host_v4[1] & mask_v4[1],
                host_v4[2] & mask_v4[2],
                host_v4[3] & mask_v4[3],
            ];
            let net_masked = [
                net_v4[0] & mask_v4[0],
                net_v4[1] & mask_v4[1],
                net_v4[2] & mask_v4[2],
                net_v4[3] & mask_v4[3],
            ];
            return masked == net_masked;
        }
        // Fall through to LIKE — MySQL would treat unparseable masks as a
        // literal pattern and likely never match, but we keep LIKE as the
        // safety net.
    }
    // Both sides literal IP addresses → compare canonically so IPv6 forms that
    // differ only in zero-compression or hex case still match (e.g. `::1` vs
    // `0:0:0:0:0:0:0:1`, `FE80::1` vs `fe80::1`). A verbatim string compare
    // would wrongly reject a valid `@'::1'` account whose peer presented the
    // expanded form.
    if !pattern.contains(['%', '_']) {
        if let (Ok(pat_ip), Ok(host_ip)) = (
            pattern.parse::<std::net::IpAddr>(),
            host.parse::<std::net::IpAddr>(),
        ) {
            return pat_ip == host_ip;
        }
        // No wildcard chars → strict ASCII-insensitive compare (exact
        // hostnames, or an IP pattern against a reverse-DNS peer string).
        return pattern.eq_ignore_ascii_case(host);
    }
    // Wildcard pattern → SQL LIKE.
    like_match(pattern.as_bytes(), host.as_bytes())
}

fn parse_ipv4(s: &str) -> Option<[u8; 4]> {
    let mut out = [0u8; 4];
    let mut parts = s.split('.');
    for slot in out.iter_mut() {
        let part = parts.next()?;
        *slot = part.parse::<u8>().ok()?;
    }
    if parts.next().is_some() {
        return None;
    }
    Some(out)
}

/// Equality of an authorization object follows identifier semantics, while
/// proxy account user names retain their case-sensitive identity.
pub fn scope_equal(a: &PrivilegeScope, b: &PrivilegeScope) -> bool {
    match (a, b) {
        (
            PrivilegeScope::Proxy { user: a, host: ah },
            PrivilegeScope::Proxy { user: b, host: bh },
        ) => a == b && ah.eq_ignore_ascii_case(bh),
        _ => compare_scope_parts(scope_parts(a), scope_parts(b)).is_eq(),
    }
}
pub(crate) fn grant_option_level(scope: &PrivilegeScope) -> PrivilegeScope {
    match scope {
        PrivilegeScope::Column {
            database, table, ..
        } => PrivilegeScope::Table {
            database: database.clone(),
            table: table.clone(),
        },
        _ => scope.clone(),
    }
}

pub fn scope_name(scope: &PrivilegeScope) -> String {
    match scope {
        PrivilegeScope::Global => "*.*".to_string(),
        PrivilegeScope::Database { database } => format!("{database}.*"),
        PrivilegeScope::Table { database, table } => format!("{database}.{table}"),
        PrivilegeScope::Routine {
            database,
            routine,
            kind,
        } => format!("{} {database}.{routine}", kind.to_ascii_uppercase()),
        PrivilegeScope::Column {
            database,
            table,
            column,
        } => format!("{database}.{table}.{column}"),
        PrivilegeScope::Proxy { user, host } => AccountKey::new(user, host).display(),
    }
}

/// Borrowed ordering key; never allocate lowercased identifiers on the hot path.
fn scope_fingerprint(parts: (u8, &str, &str, &str)) -> u64 {
    use std::hash::Hasher;
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    hash.write_u8(parts.0);
    for part in [parts.1, parts.2, parts.3] {
        hash.write_usize(part.len());
        for byte in part.bytes() {
            hash.write_u8(byte.to_ascii_lowercase());
        }
    }
    hash.finish()
}

fn scope_parts(scope: &PrivilegeScope) -> (u8, &str, &str, &str) {
    match scope {
        PrivilegeScope::Global => (0, "", "", ""),
        PrivilegeScope::Database { database } => (1, database, "", ""),
        PrivilegeScope::Table { database, table } => (2, database, table, ""),
        PrivilegeScope::Column {
            database,
            table,
            column,
        } => (3, database, table, column),
        PrivilegeScope::Routine {
            database,
            routine,
            kind,
        } => (4, database, routine, kind),
        PrivilegeScope::Proxy { user, host } => (5, user, host, ""),
    }
}

fn compare_scope_parts(a: (u8, &str, &str, &str), b: (u8, &str, &str, &str)) -> std::cmp::Ordering {
    let folded_cmp = |a: &str, b: &str| {
        a.bytes()
            .map(|c| c.to_ascii_lowercase())
            .cmp(b.bytes().map(|c| c.to_ascii_lowercase()))
    };
    a.0.cmp(&b.0)
        .then_with(|| folded_cmp(a.1, b.1))
        .then_with(|| folded_cmp(a.2, b.2))
        .then_with(|| folded_cmp(a.3, b.3))
}

fn database_scope_matches(granted: &str, required: &str) -> bool {
    if partial_revokes_enabled() {
        granted.eq_ignore_ascii_case(required)
    } else {
        schema_pattern::matches(granted, required)
    }
}
fn scope_allows_delegation(granted: &PrivilegeScope, required: &PrivilegeScope) -> bool {
    if !partial_revokes_enabled() {
        if let (
            PrivilegeScope::Database { database: g },
            PrivilegeScope::Database { database: r },
        ) = (granted, required)
        {
            return schema_pattern::covers(g, r);
        }
    }
    scope_implies(granted, required)
}

pub fn scope_implies(grant: &PrivilegeScope, required: &PrivilegeScope) -> bool {
    match (grant, required) {
        (PrivilegeScope::Global, _) => true,
        (PrivilegeScope::Database { database: g }, PrivilegeScope::Database { database: r }) => {
            database_scope_matches(g, r)
        }
        (PrivilegeScope::Database { database: g }, PrivilegeScope::Table { database: r, .. }) => {
            database_scope_matches(g, r)
        }
        (PrivilegeScope::Database { database: g }, PrivilegeScope::Column { database: r, .. }) => {
            database_scope_matches(g, r)
        }
        (PrivilegeScope::Database { database: g }, PrivilegeScope::Routine { database: r, .. }) => {
            database_scope_matches(g, r)
        }
        (
            PrivilegeScope::Table {
                database: gd,
                table: gt,
            },
            PrivilegeScope::Table {
                database: rd,
                table: rt,
            },
        ) => gd.eq_ignore_ascii_case(rd) && gt.eq_ignore_ascii_case(rt),
        (
            PrivilegeScope::Routine {
                database: gd,
                routine: gr,
                kind: gk,
            },
            PrivilegeScope::Routine {
                database: rd,
                routine: rr,
                kind: rk,
            },
        ) => {
            gd.eq_ignore_ascii_case(rd)
                && gr.eq_ignore_ascii_case(rr)
                && gk.eq_ignore_ascii_case(rk)
        }
        (
            PrivilegeScope::Table {
                database: gd,
                table: gt,
            },
            PrivilegeScope::Column {
                database: rd,
                table: rt,
                ..
            },
        ) => gd.eq_ignore_ascii_case(rd) && gt.eq_ignore_ascii_case(rt),
        (
            PrivilegeScope::Column {
                database: gd,
                table: gt,
                column: gc,
            },
            PrivilegeScope::Column {
                database: rd,
                table: rt,
                column: rc,
            },
        ) => {
            gd.eq_ignore_ascii_case(rd)
                && gt.eq_ignore_ascii_case(rt)
                && gc.eq_ignore_ascii_case(rc)
        }
        (
            PrivilegeScope::Proxy { user: gu, host: gh },
            PrivilegeScope::Proxy { user: ru, host: rh },
        ) => gu == ru && gh.eq_ignore_ascii_case(rh),
        _ => false,
    }
}

fn grant_satisfies_privilege(grant: &GrantEntry, required: &Privilege) -> bool {
    if matches!(required, Privilege::GrantOption) {
        grant.grant_option && !matches!(grant.privilege, Privilege::Dynamic(_))
    } else {
        privilege_implies(&grant.privilege, required)
    }
}
fn grant_option_scope_implies(granted: &PrivilegeScope, required: &PrivilegeScope) -> bool {
    if let PrivilegeScope::Column {
        database, table, ..
    } = granted
    {
        if let PrivilegeScope::Table {
            database: d,
            table: t,
        }
        | PrivilegeScope::Column {
            database: d,
            table: t,
            ..
        } = required
        {
            return database.eq_ignore_ascii_case(d) && table.eq_ignore_ascii_case(t);
        }
    }
    scope_allows_delegation(granted, required)
}

fn privilege_implies(grant: &Privilege, required: &Privilege) -> bool {
    if grant == required {
        return true;
    }
    // SUPER's dynamic-privilege shadow set (L13). MySQL 8.0 stipulates SUPER
    // implies a long list of dynamic admin privileges as a deprecation bridge.
    if let Privilege::Super = grant {
        if let Privilege::Dynamic(name) = required {
            return super_implied_dynamic_privileges()
                .iter()
                .any(|implied| implied.eq_ignore_ascii_case(name));
        }
    }
    false
}

/// Static privilege bundle for `ALL [PRIVILEGES] ON <scope>`. Matches the
/// MySQL 8.0 reference manual (§13.7.1.6 "GRANT Statement") tables for each
/// privilege level. Used both when expanding `GRANT ALL ON ...` and when
/// reverse-folding `SHOW GRANTS` output.
pub fn all_privileges_for_scope(scope: &PrivilegeScope) -> &'static [Privilege] {
    use Privilege::*;
    static GLOBAL_ALL: &[Privilege] = &[
        Select,
        Insert,
        Update,
        Delete,
        Create,
        Drop,
        Reload,
        Shutdown,
        Process,
        File,
        References,
        Index,
        Alter,
        ShowDb,
        Super,
        CreateTmpTable,
        LockTables,
        Execute,
        ReplSlave,
        ReplClient,
        CreateView,
        ShowView,
        CreateRoutine,
        AlterRoutine,
        CreateUser,
        Event,
        Trigger,
        CreateTablespace,
        CreateRole,
        DropRole,
    ];
    static DATABASE_ALL: &[Privilege] = &[
        Select,
        Insert,
        Update,
        Delete,
        Create,
        Drop,
        References,
        Index,
        Alter,
        CreateTmpTable,
        LockTables,
        Execute,
        CreateView,
        ShowView,
        CreateRoutine,
        AlterRoutine,
        Event,
        Trigger,
    ];
    static TABLE_ALL: &[Privilege] = &[
        Select, Insert, Update, Delete, Create, Drop, References, Index, Alter, CreateView,
        ShowView, Trigger,
    ];
    static COLUMN_ALL: &[Privilege] = &[Select, Insert, Update, References];
    static ROUTINE_ALL: &[Privilege] = &[Execute, AlterRoutine];
    match scope {
        PrivilegeScope::Global => GLOBAL_ALL,
        PrivilegeScope::Database { .. } => DATABASE_ALL,
        PrivilegeScope::Table { .. } => TABLE_ALL,
        PrivilegeScope::Column { .. } => COLUMN_ALL,
        PrivilegeScope::Routine { .. } => ROUTINE_ALL,
        PrivilegeScope::Proxy { .. } => &[],
    }
}

/// Default set of dynamic privileges shipped at bootstrap. Maps to MySQL 8.0's
/// builtin dynamic privilege table — `mysql.global_grants` is seeded with
/// these for `root@localhost` so a fresh install can perform replication / backup /
/// sysvar admin / role admin work without re-granting. New dynamic privileges
/// granted by users (via `GRANT custom_admin ON *.* TO u`) are also valid;
/// this list only governs what `root` automatically holds and what `SUPER`
/// implies (see `super_implied_dynamic_privileges`).
pub fn default_dynamic_privileges() -> &'static [Privilege] {
    static SET: std::sync::OnceLock<Vec<Privilege>> = std::sync::OnceLock::new();
    SET.get_or_init(|| {
        [
            "APPLICATION_PASSWORD_ADMIN",
            "AUDIT_ABORT_EXEMPT",
            "AUDIT_ADMIN",
            "AUTHENTICATION_POLICY_ADMIN",
            "BACKUP_ADMIN",
            "BINLOG_ADMIN",
            "BINLOG_ENCRYPTION_ADMIN",
            "CLONE_ADMIN",
            "CONNECTION_ADMIN",
            "ENCRYPTION_KEY_ADMIN",
            "FIREWALL_EXEMPT",
            "FLUSH_OPTIMIZER_COSTS",
            "FLUSH_STATUS",
            "FLUSH_TABLES",
            "FLUSH_USER_RESOURCES",
            "GROUP_REPLICATION_ADMIN",
            "GROUP_REPLICATION_STREAM",
            "INNODB_REDO_LOG_ARCHIVE",
            "INNODB_REDO_LOG_ENABLE",
            "PASSWORDLESS_USER_ADMIN",
            "PERSIST_RO_VARIABLES_ADMIN",
            "REPLICATION_APPLIER",
            "REPLICATION_SLAVE_ADMIN",
            "RESOURCE_GROUP_ADMIN",
            "RESOURCE_GROUP_USER",
            "ROLE_ADMIN",
            "SENSITIVE_VARIABLES_OBSERVER",
            "SERVICE_CONNECTION_ADMIN",
            "SESSION_VARIABLES_ADMIN",
            "SET_USER_ID",
            "SHOW_ROUTINE",
            "SYSTEM_USER",
            "SYSTEM_VARIABLES_ADMIN",
            "TABLE_ENCRYPTION_ADMIN",
            "TELEMETRY_LOG_ADMIN",
            "XA_RECOVER_ADMIN",
        ]
        .iter()
        .map(|s| Privilege::Dynamic((*s).to_string()))
        .collect()
    })
}

/// True when a dynamic privilege is currently registered by the built-in
/// server/component set. GRANT/REVOKE must reject arbitrary identifiers;
/// accepting every unknown name as a dynamic privilege makes typos look like
/// successful security configuration.
pub fn is_registered_dynamic_privilege(name: &str) -> bool {
    default_dynamic_privileges().iter().any(|privilege| {
        matches!(privilege, Privilege::Dynamic(registered) if registered.eq_ignore_ascii_case(name))
    })
}

/// Subset of dynamic privileges SUPER implies. From MySQL 8.0 reference
/// manual §6.2.10 "Migrating Accounts from SUPER to Dynamic Privileges".
pub fn super_implied_dynamic_privileges() -> &'static [&'static str] {
    &[
        "SET_USER_ID",
        "CONNECTION_ADMIN",
        "BINLOG_ADMIN",
        "GROUP_REPLICATION_ADMIN",
        "REPLICATION_SLAVE_ADMIN",
        "SYSTEM_VARIABLES_ADMIN",
        "SESSION_VARIABLES_ADMIN",
        "BACKUP_ADMIN",
        "CLONE_ADMIN",
        "AUDIT_ADMIN",
        "ENCRYPTION_KEY_ADMIN",
        "RESOURCE_GROUP_ADMIN",
        "RESOURCE_GROUP_USER",
        "BINLOG_ENCRYPTION_ADMIN",
        "XA_RECOVER_ADMIN",
        "SERVICE_CONNECTION_ADMIN",
        "FLUSH_OPTIMIZER_COSTS",
        "FLUSH_STATUS",
        "FLUSH_TABLES",
        "FLUSH_USER_RESOURCES",
        "FIREWALL_EXEMPT",
        "INNODB_REDO_LOG_ARCHIVE",
        "PERSIST_RO_VARIABLES_ADMIN",
        "TABLE_ENCRYPTION_ADMIN",
        "ROLE_ADMIN",
        "SHOW_ROUTINE",
    ]
}

/// Stable sort key used when dedupe-merging grants. Returns the scope kind
/// followed by its identifying names lower-cased; lexicographic comparison
/// on the result gives MySQL-style ordering (Global → Database → Table →
/// Column → Routine).
fn scope_sort_key(scope: &PrivilegeScope) -> (u8, String, String, String) {
    match scope {
        PrivilegeScope::Global => (0, String::new(), String::new(), String::new()),
        PrivilegeScope::Database { database } => (
            1,
            database.to_ascii_lowercase(),
            String::new(),
            String::new(),
        ),
        PrivilegeScope::Table { database, table } => (
            2,
            database.to_ascii_lowercase(),
            table.to_ascii_lowercase(),
            String::new(),
        ),
        PrivilegeScope::Column {
            database,
            table,
            column,
        } => (
            4,
            database.to_ascii_lowercase(),
            table.to_ascii_lowercase(),
            column.to_ascii_lowercase(),
        ),
        PrivilegeScope::Routine {
            database,
            routine,
            kind,
        } => (
            3,
            database.to_ascii_lowercase(),
            routine.to_ascii_lowercase(),
            kind.to_ascii_lowercase(),
        ),
        PrivilegeScope::Proxy { user, host } => {
            (5, user.clone(), host.to_ascii_lowercase(), String::new())
        }
    }
}

fn verify_mysql_native(stored: &str, token: &[u8], seed: &[u8]) -> bool {
    let stored = stored.trim_start_matches('*');
    if token.len() != 20 || stored.len() != 40 {
        return false;
    }
    let Some(stage2) = parse_hex(stored) else {
        return false;
    };
    let mut sha = Sha1::new();
    sha.update(seed);
    sha.update(&stage2);
    let digest = sha.finalize();
    let stage1: Vec<u8> = token
        .iter()
        .zip(digest.iter())
        .map(|(a, b)| a ^ b)
        .collect();
    let check = Sha1::digest(&stage1);
    constant_time_eq(check.as_slice(), stage2.as_slice())
}

fn verify_caching_sha2(stored: &str, token: &[u8], seed: &[u8]) -> bool {
    let stored = stored.trim_start_matches("$A$");
    if token.len() != 32 || stored.len() != 64 {
        return false;
    }
    let Some(stage2) = parse_hex(stored) else {
        return false;
    };
    let mut sha = Sha256::new();
    sha.update(&stage2);
    sha.update(seed);
    let digest = sha.finalize();
    let stage1: Vec<u8> = token
        .iter()
        .zip(digest.iter())
        .map(|(a, b)| a ^ b)
        .collect();
    let check = Sha256::digest(&stage1);
    constant_time_eq(check.as_slice(), stage2.as_slice())
}

fn verify_sha256_password(stored: &str, token: &[u8]) -> bool {
    if token.len() > 257 {
        return false;
    }
    let plain = token.strip_suffix(&[0]).unwrap_or(token);
    let Ok(password) = std::str::from_utf8(plain) else {
        return false;
    };
    verify_plaintext_auth_string("sha256_password", stored, password)
}

fn parse_hex(input: &str) -> Option<Vec<u8>> {
    crate::util::hex::decode(input.as_bytes()).ok()
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut diff = left.len() ^ right.len();
    for i in 0..left.len().max(right.len()) {
        let a = left.get(i).copied().unwrap_or(0);
        let b = right.get(i).copied().unwrap_or(0);
        diff |= (a ^ b) as usize;
    }
    diff == 0
}

fn host_specificity(pattern: &str) -> (u8, u16) {
    if pattern.is_empty() {
        return (0, 0); // sorts after `%`
    }
    if pattern == "%" {
        return (1, 0);
    }
    if pattern.contains(['%', '_']) {
        return (
            2,
            pattern
                .bytes()
                .filter(|byte| *byte != b'%' && *byte != b'_')
                .count()
                .min(u16::MAX as usize) as u16,
        );
    }
    if let Some((_, suffix)) = pattern.split_once('/') {
        if let Ok(prefix) = suffix.parse::<u8>() {
            return (4, u16::from(prefix.min(128))); // CIDR
        }
        let bits = parse_ipv4(suffix)
            .map(|mask| mask.into_iter().map(u8::count_ones).sum::<u32>())
            .unwrap_or(0);
        return (3, bits as u16);
    }
    (5, u16::MAX) // literal IP or host name
}

/// GRANT `user@host` pattern matcher. Host patterns are ASCII in
/// practice; `_` matches any single byte, `%` matches any run.
///
/// B6: delegates to the shared LIKE kernel so pattern semantics stay
/// consistent with SQL LIKE. The previous recursive matcher folded
/// bytes case-insensitively verbatim (host `%` grants match any host
/// spelling); we preserve that with a case-insensitive collation info
/// pinned to `utf8mb4_general_ci`, but restrict to ASCII pairs — host
/// bytes containing a stray non-ASCII byte fall through to byte
/// equality just like the old recursive version.
fn like_match(pattern: &[u8], value: &[u8]) -> bool {
    // The recursive matcher used `eq_ignore_ascii_case`; the shared
    // kernel's byte fast path is byte-exact, so we fold to lowercase
    // through the same ASCII path used by `default` and preserve the
    // "hostnames compare CI" property.
    let (Ok(pat_str), Ok(val_str)) = (std::str::from_utf8(pattern), std::str::from_utf8(value))
    else {
        return false;
    };
    // A case-insensitive ASCII collation for host wildcards.
    // `utf8mb4_general_ci` is the closest match: CI + accent-sensitive
    // matches the historical `eq_ignore_ascii_case` on the ASCII slice.
    let info = crate::charset::resolve_collation("utf8mb4_general_ci")
        .unwrap_or_else(|_| crate::charset::default_collation_info());
    crate::engine::expr::match_like_pattern_with_info(val_str, pat_str, None, info)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Predeterministic RSA-2048 PEM injected via `MYDB_AUTH_RSA_PRIVATE_KEY_INLINE`
    /// so the per-process keyset() skips ~12s of OsRng RSA-2048 keygen. The
    /// PEM was generated once via `openssl genpkey` and is exclusively for
    /// unit tests; production paths still generate ephemeral keys.
    const TEST_RSA_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
        MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC2Ck2urPw6nqTT\n\
        JSfXnfknLC4HmEIGoiM0Fvs3ApB6XQpwFk+CN6H7CHVPS/zUPiMTZXVQgFFtixcg\n\
        dWMsfAcJcCIZsAmDbLJ279gJ82chtF7vXF1ab+q8sAbqM/BEBYjxgNaZBX43j9Rk\n\
        uzjg1GS/xSHQ0FcX2uHjTQ5Ixcy7JXhqEO31agfA9XYuOQysXEOPEeC8mdt/hm79\n\
        xMzf3Q78eIsm4HWly+EOE2IaVzZruk1pU7i6HFBsiY/uvqRRTnasjI3WCrUd/1wD\n\
        uS1Sypw1j/WGKPQJczNhidhOSFy1HnjR31BUPDhpdDR3Vm+lkSF8+6AhtPhrizrz\n\
        PpAgzs0JAgMBAAECggEAAdKmFvzv9KVQiLW7SacxDsGcP2K4rIjUmemt/83EhRkn\n\
        6NnsV2U3t8QMqqvb3wM1RjUyoMj8J8DoDSDnypL+XguTp9axrbF5swOxUd4wVu/P\n\
        sI1n+OccfmGz3AYDnqbAiYyM+ZDKxAMqcQzH48Z0sM/oSAdCDADa2LQVk8h5RZl0\n\
        4WylTqbwHCMM7aY1fKn6ZfNWdx0E5u/6VPI6mLTiLUPvViS7Q28mRCnb0XhBge18\n\
        RH4aYRATbA3Kxx6DFrOlubLBCoekXzqfMIwXoYkOqxvFgDTi9xqxr7Tq72R4x74d\n\
        sA2+6YtwsabR9HpkcN0KmBE9GY7LvVd9EnS2baNfoQKBgQDcpEKB0vff+I9jwXKb\n\
        t877jJ9uCzFsxUvyKIeVGTpcTNL1QIzmc3zYjF+hU1z31n+cXb1ePH62z/FsuDzI\n\
        UU8eha1EHuVvmpG2bmDYElKh6eL3t19RLe5s+qcQHvD4pKBNegjAiA78/G6uMtwU\n\
        SrKJGCyzw4FtScKBP87tR065mQKBgQDTNm+i7J1akEKMQHIgpS5dyQLQ64nHlq3Z\n\
        zYLVTVLfGVGs4n8XX2YRdErsDBfNlCjEwO8/aXIcPjSIrBr8+ibFuzbFXMRfBHRo\n\
        XrghH9J/cs5mQ6pDlI6C9EE6KrJQLvX0qeGxVHmZA0zezOA0DPR1DDlznYj0slKo\n\
        XQQ1x1c08QKBgFiY7MvMtxQMbyeTaj31FPwVf39kpzgSEWjvtWG9e53Mi3lHrkxr\n\
        W1ppI41YOCCc1Skoyv2oNQmbfko1wNJ/M9IBw41zCpZB7vjeYazhzqSNkkH+NBwY\n\
        4YCJNcRXaFOvZ180/YayG7Kiep8gdRcA16gNOQYGH1aPI7das/C7jXUhAoGAYVdp\n\
        Q/FA5AgwY2sgLiNypw6JGbg0G6Q6I3MvpRap5uBIYCFS6XayF8E89mTsY3axA1zX\n\
        5qW2TYXtuD95gLXZQK/zoioK5q+q5p8gxUNhRBOpREBTLCo/M+NWXHRpEcNS35ub\n\
        xJpmIDsBJn3Npq+ByqADqmEkSYIVzjpwQkjIzEECgYAQsF36WxLCKyinfDdY1+64\n\
        PJOnOLoBOWUyzgJA4VtcaQTxUQH4GvMr3jMNlkq0sc7Jp3i3vdgxJZTUhEgoODnw\n\
        OwF5jkalsxod1kSzB95sJHPp4hrZMD8xrUKYJ8XZEGvKVzwoP+Zwe/Jzi8+AIbOz\n\
        GCE9D+4ujERcESSAvMhHiQ==\n\
        -----END PRIVATE KEY-----\n";

    fn inject_test_rsa_key_once() {
        use std::sync::Once;
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            // Strip the line-leading whitespace that comes from the source-literal
            // continuation lines; pkcs8 PEM parsers tolerate \r\n but not stray
            // spaces inside the base64 body.
            let cleaned: String = TEST_RSA_PEM
                .lines()
                .map(|l| l.trim())
                .collect::<Vec<_>>()
                .join("\n");
            // SAFETY: set before any code in this test process touches
            // `rsa_keys::keyset()`. No other thread is reading env here.
            unsafe { std::env::set_var("MYDB_AUTH_RSA_PRIVATE_KEY_INLINE", cleaned) };
        });
    }

    #[test]
    fn rsa_keypair_pem_round_trip_decrypts_client_blob() {
        // Simulates what a MySQL client does: parse the server's public-key
        // PEM, RSA-encrypt the password XOR seed, send to server, server
        // decrypts and recovers plaintext.
        use openssl::encrypt::Encrypter;
        use openssl::pkey::PKey;
        use openssl::rsa::Padding;

        inject_test_rsa_key_once();
        let pem = rsa_keys::public_key_pem();
        // Server emits SubjectPublicKeyInfo PEM (BEGIN PUBLIC KEY), which
        // is what MySQL 8.0 ships and what every connector expects.
        let pubkey = PKey::public_key_from_pem(pem.as_bytes()).unwrap();
        let plain = b"my_password\0"; // MySQL clients append the nul.
        let seed = b"caching-sha2-salt-12345abcdef000";
        let mut xored = plain.to_vec();
        xor_password_with_seed(&mut xored, seed);
        let mut encrypt = Encrypter::new(&pubkey).unwrap();
        encrypt.set_rsa_padding(Padding::PKCS1).unwrap();
        let mut ct = vec![0; pubkey.size()];
        let len = encrypt.encrypt(&xored, &mut ct).unwrap();
        ct.truncate(len);
        let decrypted = rsa_keys::decrypt_either(&ct).unwrap();
        assert_eq!(decrypted.len(), plain.len());
        // Reverse the XOR — same operation, symmetric.
        let mut recovered = decrypted;
        xor_password_with_seed(&mut recovered, seed);
        assert_eq!(&recovered, plain);
    }

    #[test]
    fn audit13_rsa_oaep_takes_precedence_over_ambiguous_legacy_padding() {
        use openssl::bn::{BigNum, BigNumContext};
        use openssl::rsa::Rsa;
        fn mgf1(seed: &[u8], len: usize) -> Vec<u8> {
            let mut mask = Vec::new();
            for counter in 0..len.div_ceil(20) {
                let mut hash = Sha1::new();
                hash.update(seed);
                hash.update((counter as u32).to_be_bytes());
                mask.extend_from_slice(&hash.finalize());
            }
            mask.truncate(len);
            mask
        }
        inject_test_rsa_key_once();
        let public = Rsa::public_key_from_pem(rsa_keys::public_key_pem().as_bytes()).unwrap();
        let size = public.size() as usize;
        let plain = b"root\0";
        let mut db = Sha1::digest(b"").to_vec();
        db.resize(size - plain.len() - 22, 0);
        db.push(1);
        db.extend_from_slice(plain);
        // Construct a valid deterministic OAEP block whose encoded bytes also
        // meet v1.5's 00 02 / nonzero padding / zero separator constraints.
        let encoded = (0u32..100_000)
            .find_map(|counter| {
                let mut seed = [0u8; 20];
                seed[16..].copy_from_slice(&counter.to_be_bytes());
                let masked_db: Vec<u8> = db
                    .iter()
                    .zip(mgf1(&seed, size - 21))
                    .map(|(a, b)| a ^ b)
                    .collect();
                let mut encoded = vec![0];
                encoded.extend(seed.iter().zip(mgf1(&masked_db, 20)).map(|(a, b)| a ^ b));
                encoded.extend(masked_db);
                (encoded[1] == 2 && !encoded[2..10].contains(&0) && encoded[10..].contains(&0))
                    .then_some(encoded)
            })
            .expect("deterministic ambiguous OAEP block");
        let mut encrypted = BigNum::new().unwrap();
        encrypted
            .mod_exp(
                &BigNum::from_slice(&encoded).unwrap(),
                public.e(),
                public.n(),
                &mut BigNumContext::new().unwrap(),
            )
            .unwrap();
        let ciphertext = encrypted.to_vec_padded(size as i32).unwrap();
        assert_ne!(
            rsa_keys::pkcs1v15_decrypt(&ciphertext).unwrap().as_slice(),
            plain
        );
        assert_eq!(
            rsa_keys::oaep_sha1_decrypt(&ciphertext).unwrap().as_slice(),
            plain
        );
        assert_eq!(
            rsa_keys::decrypt_either(&ciphertext).unwrap().as_slice(),
            plain
        );
    }

    #[test]
    fn rsa_invalid_pkcs1_padding_uses_provider_implicit_rejection() {
        inject_test_rsa_key_once();
        let key = openssl::pkey::PKey::public_key_from_pem(rsa_keys::public_key_pem().as_bytes())
            .unwrap();
        let invalid = vec![0; key.size()];
        let first = rsa_keys::pkcs1v15_decrypt(&invalid)
            .expect("implicit rejection must produce synthetic plaintext");
        let second = rsa_keys::pkcs1v15_decrypt(&invalid).unwrap();
        assert_eq!(
            first, second,
            "implicit rejection is tied to the key and ciphertext"
        );
        assert!(rsa_keys::oaep_sha1_decrypt(&invalid).is_err());
        assert!(openssl::version::number() >= 0x3020_0000);
        eprintln!("authentication provider: {}", openssl::version::version());
    }

    #[test]
    fn auth_cache_remember_and_lookup() {
        let key = AccountKey::new("cache_test_user", "%");
        assert!(!auth_cache_contains(&key));
        auth_cache_remember(&key);
        assert!(auth_cache_contains(&key));
        auth_cache_clear();
        assert!(!auth_cache_contains(&key));
    }

    #[test]
    fn mysql_native_scramble_verifies_password() {
        let seed = b"mydb-auth-salt-12345";
        let password = "root";
        let stage1 = Sha1::digest(password.as_bytes());
        let stage2 = Sha1::digest(stage1);
        let mut sha = Sha1::new();
        sha.update(seed);
        sha.update(stage2);
        let digest = sha.finalize();
        let token: Vec<u8> = stage1
            .iter()
            .zip(digest.iter())
            .map(|(a, b)| a ^ b)
            .collect();
        assert!(verify_auth_response(
            "mysql_native_password",
            &mysql_native_auth_string(password),
            &token,
            seed
        ));
        assert!(!verify_auth_response(
            "mysql_native_password",
            &mysql_native_auth_string("wrong"),
            &token,
            seed
        ));
    }

    #[test]
    fn host_matching_prefers_specific_accounts() {
        let mut auth = AuthCatalog::default();
        auth.accounts.insert(
            account_id(&AccountKey::new("alice", "127.0.0.1")),
            UserAccount::new(
                AccountKey::new("alice", "127.0.0.1"),
                "mysql_native_password".to_string(),
                mysql_native_auth_string("pw"),
            ),
        );
        auth.accounts.insert(
            account_id(&AccountKey::new("alice", "%")),
            UserAccount::new(
                AccountKey::new("alice", "%"),
                "mysql_native_password".to_string(),
                mysql_native_auth_string("pw"),
            ),
        );
        assert_eq!(
            auth.match_account("alice", "127.0.0.1").unwrap().key.host,
            "127.0.0.1"
        );
    }

    #[test]
    fn localhost_pattern_matches_loopback_peers() {
        // mydb is TCP-only: a local client presents a numeric loopback IP, so a
        // `@'localhost'` account must match those peers or it is unusable.
        assert!(host_matches("localhost", "127.0.0.1"));
        assert!(host_matches("localhost", "127.0.0.53"));
        assert!(host_matches("LOCALHOST", "127.0.0.1"));
        assert!(host_matches("localhost", "::1"));
        assert!(host_matches("localhost", "localhost"));
        // Non-loopback peers must not match a localhost account.
        assert!(!host_matches("localhost", "192.168.1.10"));
        assert!(!host_matches("localhost", "10.0.0.1"));
    }

    #[test]
    fn host_matches_canonicalizes_ipv6_literals() {
        // Zero-compression and hex-case differences must still match (M8).
        assert!(host_matches("::1", "0:0:0:0:0:0:0:1"));
        assert!(host_matches("0:0:0:0:0:0:0:1", "::1"));
        assert!(host_matches("FE80::1", "fe80::1"));
        assert!(host_matches(
            "2001:db8::1",
            "2001:0db8:0000:0000:0000:0000:0000:0001"
        ));
        // Distinct addresses must not match.
        assert!(!host_matches("::1", "::2"));
        assert!(!host_matches("2001:db8::1", "2001:db8::2"));
        // IPv4 literals still compare correctly through the same path.
        assert!(host_matches("127.0.0.1", "127.0.0.1"));
        assert!(!host_matches("127.0.0.1", "127.0.0.2"));
    }

    #[test]
    fn match_account_localhost_from_tcp_loopback() {
        let mut auth = AuthCatalog::default();
        auth.accounts.insert(
            account_id(&AccountKey::new("Teacher", "localhost")),
            UserAccount::new(
                AccountKey::new("Teacher", "localhost"),
                "mysql_native_password".to_string(),
                mysql_native_auth_string("T99999"),
            ),
        );
        assert_eq!(
            auth.match_account("Teacher", "127.0.0.1").unwrap().key.host,
            "localhost"
        );
    }

    #[test]
    fn account_matching_orders_host_before_named_user() {
        let mut auth = AuthCatalog::default();
        let named = UserAccount::new(
            AccountKey::new("shadow", "%"),
            "mysql_native_password".to_string(),
            mysql_native_auth_string("named"),
        );
        let anonymous = UserAccount::new(
            AccountKey::new("", "localhost"),
            "mysql_native_password".to_string(),
            mysql_native_auth_string("anonymous"),
        );
        auth.accounts.insert(account_id(&named.key), named);
        auth.accounts.insert(account_id(&anonymous.key), anonymous);
        let matched = auth.match_account("shadow", "127.0.0.1").unwrap();
        assert_eq!(matched.key, AccountKey::new("", "localhost"));
    }

    #[test]
    fn caching_sha2_hash_matches_mysql_8046_oracle() {
        // Generated by mysql:8.0.46 for plaintext `Secret-123!`.
        let mysql_hash =
            "$A$005$aD\u{1f}\tWINjzF)\u{18}p_\u{6}n3{X647c2y5ryJYqtQhuvj029pqChKOXi/khPaioTv0emtTD";
        assert_eq!(mysql_hash.as_bytes().len(), 70);
        assert_eq!(
            sha256_crypt_checksum(
                b"Secret-123!",
                "aD\u{1f}\tWINjzF)\u{18}p_\u{6}n3{X6".as_bytes(),
                5_000,
            ),
            "47c2y5ryJYqtQhuvj029pqChKOXi/khPaioTv0emtTD"
        );
        assert!(verify_plaintext_auth_string(
            "caching_sha2_password",
            mysql_hash,
            "Secret-123!"
        ));
        assert!(!verify_plaintext_auth_string(
            "caching_sha2_password",
            mysql_hash,
            "wrong"
        ));

        let generated = caching_sha2_auth_string("Secret-123!");
        assert_eq!(generated.as_bytes().len(), 70);
        assert!(is_valid_authentication_string(
            "caching_sha2_password",
            &generated
        ));
        assert!(verify_plaintext_auth_string(
            "caching_sha2_password",
            &generated,
            "Secret-123!"
        ));
    }

    #[test]
    fn sha256_password_hash_matches_mysql_8046_oracle() {
        let mysql_hash = "$5$\\z\u{16}8\u{13}*#\u{f}jn}\u{1f}gb\u{1a}k%E\"e$UFyivpK2sYM92UMqeX25gaZWgQgam5ctkUmBKPBNzw4";
        assert_eq!(mysql_hash.as_bytes().len(), 67);
        assert!(verify_plaintext_auth_string(
            "sha256_password",
            mysql_hash,
            "Secret-123!"
        ));
        assert!(!verify_plaintext_auth_string(
            "sha256_password",
            mysql_hash,
            "wrong"
        ));
        let generated = sha256_password_auth_string("Secret-123!");
        assert_eq!(generated.as_bytes().len(), 67);
        assert!(verify_plaintext_auth_string(
            "sha256_password",
            &generated,
            "Secret-123!"
        ));
    }

    #[test]
    fn privilege_index_bounds_hot_lookup_at_one_hundred_thousand_grants() {
        let mut auth = AuthCatalog::default();
        let grantor = AccountKey::new("root", "localhost");
        for principal in 0..10_000 {
            let grantee = AccountKey::new(format!("bench_{principal}"), "%");
            for table in 0..10 {
                auth.grants.push(GrantEntry {
                    updated_ms: crate::storage::naming::current_unix_ms(),
                    grantor: grantor.clone(),
                    grantee: grantee.clone(),
                    privilege: Privilege::Select,
                    scope: PrivilegeScope::Table {
                        database: "bench".to_string(),
                        table: format!("t{table}"),
                    },
                    grant_option: false,
                    origin: GrantOrigin::Explicit,
                });
            }
        }
        auth.rebuild_runtime_indexes();
        let target = AccountKey::new("bench_9999", "%");
        assert_eq!(auth.indexed_grant_count(&target), Some(10));
        let ctx = AuthorizationContext {
            authenticated_as: target,
            active_roles: Default::default(),
            epoch: auth.epoch,
        };
        assert!(auth.account_has_privilege(
            &ctx,
            &RequiredPrivilege {
                privilege: Privilege::Select,
                scope: PrivilegeScope::Table {
                    database: "bench".to_string(),
                    table: "t9".to_string(),
                },
            },
            false,
        ));
    }
    #[test]
    fn sorted_scope_index_matches_scan_for_all_scopes_and_restrictions() {
        let mut auth = AuthCatalog::empty();
        let principal = AccountKey::new("indexed", "%");
        let ctx = AuthorizationContext {
            authenticated_as: principal.clone(),
            active_roles: Default::default(),
            epoch: 1,
        };
        let mut scopes = vec![
            PrivilegeScope::Global,
            PrivilegeScope::Database {
                database: "Db".into(),
            },
        ];
        for n in 0..100 {
            scopes.extend([
                PrivilegeScope::Table {
                    database: "Db".into(),
                    table: format!("T{n}"),
                },
                PrivilegeScope::Column {
                    database: "Db".into(),
                    table: format!("T{n}"),
                    column: "C".into(),
                },
                PrivilegeScope::Routine {
                    database: "Db".into(),
                    routine: format!("R{n}"),
                    kind: "function".into(),
                },
                PrivilegeScope::Proxy {
                    user: format!("P{n}"),
                    host: "LOCALHOST".into(),
                },
            ]);
        }
        for (n, scope) in scopes.iter().enumerate() {
            auth.grants.push(GrantEntry {
                updated_ms: crate::storage::naming::current_unix_ms(),
                grantor: principal.clone(),
                grantee: principal.clone(),
                privilege: if n % 3 == 0 {
                    Privilege::Select
                } else {
                    Privilege::Execute
                },
                scope: scope.clone(),
                grant_option: n % 2 == 0,
                origin: GrantOrigin::Explicit,
            });
        }
        auth.restrictions.push(RestrictionEntry {
            grantee: principal.clone(),
            privilege: Privilege::Select,
            database: "db".into(),
        });
        let scan = auth.clone();
        auth.rebuild_runtime_indexes();
        assert!(auth.indexes.indexed);
        for table in [None, Some("T0"), Some("t7"), Some("T99"), Some("absent")] {
            for schema in ["Db", "DB", "absent"] {
                assert_eq!(
                    auth.account_can_access_schema(&ctx, schema, table),
                    scan.account_can_access_schema(&ctx, schema, table)
                );
            }
        }
        for grant in auth.grants.iter() {
            assert_eq!(
                auth.metadata_grantable(grant),
                scan.metadata_grantable(grant)
            );
        }
        scopes.extend([
            PrivilegeScope::Table {
                database: "db".into(),
                table: "t42".into(),
            },
            PrivilegeScope::Column {
                database: "DB".into(),
                table: "t7".into(),
                column: "c".into(),
            },
            PrivilegeScope::Table {
                database: "absent".into(),
                table: "no".into(),
            },
        ]);
        for scope in scopes {
            for privilege in [Privilege::Select, Privilege::Execute, Privilege::Update] {
                for grant_option in [false, true] {
                    let required = RequiredPrivilege {
                        privilege: privilege.clone(),
                        scope: scope.clone(),
                    };
                    assert_eq!(
                        auth.account_has_privilege(&ctx, &required, grant_option),
                        scan.account_has_privilege(&ctx, &required, grant_option),
                        "{required:?} grant_option={grant_option}"
                    );
                }
            }
        }
    }

    #[test]
    fn phase2_deep_dense_role_graph_and_incremental_indexes_match_fallback() {
        let mut auth = AuthCatalog::empty();
        let user = AccountKey::new("graph_user", "%");
        auth.accounts.insert(
            account_id(&user),
            UserAccount::new(user.clone(), "mysql_native_password".into(), String::new()),
        );
        // A 256-level chain and a dense diamond prefix exercise closure deduplication
        // and incremental revoke without repeating the scan oracle hundreds of times.
        let roles: Vec<_> = (0..256)
            .map(|n| AccountKey::new(format!("graph_role_{n:04}"), "%"))
            .collect();
        for role in &roles {
            auth.accounts.insert(
                account_id(role),
                UserAccount::new(role.clone(), "mysql_native_password".into(), String::new()),
            );
        }
        auth.role_edges.push(RoleEdge {
            from_role: roles[0].clone(),
            to_account: user.clone(),
            admin_option: false,
        });
        for n in 1..256 {
            auth.role_edges.push(RoleEdge {
                from_role: roles[n].clone(),
                to_account: roles[n - 1].clone(),
                admin_option: false,
            });
        }
        for target in 0..24 {
            for source in target + 2..24 {
                auth.role_edges.push(RoleEdge {
                    from_role: roles[source].clone(),
                    to_account: roles[target].clone(),
                    admin_option: false,
                });
            }
        }
        for (n, role) in roles.iter().enumerate() {
            auth.grants.push(GrantEntry {
                updated_ms: crate::storage::naming::current_unix_ms(),
                grantor: user.clone(),
                grantee: role.clone(),
                privilege: Privilege::Select,
                scope: PrivilegeScope::Table {
                    database: "graph".into(),
                    table: format!("t{n}"),
                },
                grant_option: n % 2 == 0,
                origin: GrantOrigin::Explicit,
            });
        }
        auth.rebuild_runtime_indexes();
        let ctx = AuthorizationContext {
            authenticated_as: user,
            active_roles: BTreeSet::from([roles[0].clone()]).into(),
            epoch: 1,
        };
        assert_eq!(auth.effective_roles(&ctx).len(), 256);
        for step in 0..8 {
            let before = auth.clone();
            auth.grants.begin_mutation();
            auth.grants.swap_remove(step * 7);
            auth.refresh_runtime_indexes(Some(&before));
            let mut scan = auth.clone();
            scan.indexes = empty_auth_indexes();
            for object in [0, 7, 23, 255, 257] {
                for privilege in [Privilege::Select, Privilege::Delete] {
                    let req = RequiredPrivilege {
                        privilege,
                        scope: PrivilegeScope::Table {
                            database: "graph".into(),
                            table: format!("t{object}"),
                        },
                    };
                    assert_eq!(
                        auth.account_has_privilege(&ctx, &req, false),
                        scan.account_has_privilege(&ctx, &req, false)
                    );
                }
            }
        }
        let before = auth.clone();
        auth.role_edges.begin_mutation();
        auth.role_edges.swap_remove(0);
        auth.refresh_runtime_indexes(Some(&before));
        assert!(
            auth.effective_roles(&ctx).is_empty(),
            "cached role closure survived revoke"
        );
    }
}

#[cfg(test)]
pub(crate) mod test_faults {
    use crate::types::QueryError;
    thread_local! { static FAIL: std::cell::Cell<&'static str> = const { std::cell::Cell::new("") }; }
    pub fn with_failure<T>(stage: &'static str, f: impl FnOnce() -> T) -> T {
        struct Reset(&'static str);
        impl Drop for Reset {
            fn drop(&mut self) {
                FAIL.with(|p| p.set(self.0));
            }
        }
        let _reset = Reset(FAIL.with(|p| p.replace(stage)));
        f()
    }
    pub fn hit(stage: &str) -> Result<(), QueryError> {
        if std::env::var("MYDB_AUTH_TEST_CRASH_STAGE").ok().as_deref() == Some(stage) {
            std::process::exit(73);
        }
        if FAIL.with(|p| p.get() == stage) {
            assert!(
                matches!(stage, "before_dd" | "after_dd_rows"),
                "post-commit hooks support crash only"
            );
            return Err(crate::storage::error(
                crate::error::catalog::ER_GET_ERRNO,
                format!("injected auth failure: {stage}"),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
#[test]
fn malformed_hex_credentials_are_rejected_without_panicking() {
    for value in ["€a", "🙂", "+A", "-1", " 1", "1 ", "0", "GG"] {
        assert_eq!(parse_hex(value), None, "{value:?}");
    }
    assert_eq!(parse_hex("00fF"), Some(vec![0, 255]));
    assert!(!verify_mysql_native(
        &format!("*{}", "🙂".repeat(10)),
        &[0; 20],
        &[0; 20],
    ));
    assert!(!verify_caching_sha2(
        &format!("$A${}", "🙂".repeat(16)),
        &[0; 32],
        &[0; 20],
    ));
}
