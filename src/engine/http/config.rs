//! Node-local HTTP policy and validated resource ceilings.
use super::{error, json::parse_json};
use crate::types::QueryError;
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    net::IpAddr,
    sync::{
        atomic::{AtomicI8, AtomicU64, Ordering},
        Arc, OnceLock, RwLock,
    },
};

pub(crate) fn limit(name: &str, default: usize, max: usize) -> Result<usize, QueryError> {
    match crate::util::env::var(name) {
        Err(std::env::VarError::NotPresent) => Ok(default),
        Ok(v) => v
            .parse::<usize>()
            .ok()
            .filter(|v| *v > 0 && *v <= max)
            .ok_or_else(|| error("ER_HTTP_INVALID_ARGUMENT", "invalid HTTP resource setting")),
        Err(_) => Err(error(
            "ER_HTTP_INVALID_ARGUMENT",
            "invalid HTTP resource setting",
        )),
    }
}
#[derive(Clone, Debug)]
pub(crate) struct Limits {
    pub inflight: usize,
    pub queue: usize,
    pub memory: usize,
    pub request: usize,
    pub response: usize,
    pub headers: usize,
    pub calls: usize,
}
impl Limits {
    pub fn load() -> Result<Self, QueryError> {
        Ok(Self {
            inflight: limit("MYDB_HTTP_MAX_INFLIGHT", 32, 256)?,
            queue: limit("MYDB_HTTP_MAX_QUEUE", 128, 4096)?,
            memory: limit("MYDB_HTTP_MEMORY_LIMIT_MB", 128, 65536)?
                .checked_mul(1024 * 1024)
                .ok_or_else(|| error("ER_HTTP_INVALID_ARGUMENT", "memory limit overflow"))?,
            request: limit(
                "MYDB_HTTP_MAX_REQUEST_BYTES",
                4 * 1024 * 1024,
                64 * 1024 * 1024,
            )?,
            response: limit(
                "MYDB_HTTP_MAX_RESPONSE_BYTES",
                4 * 1024 * 1024,
                64 * 1024 * 1024,
            )?,
            headers: limit("MYDB_HTTP_MAX_HEADER_BYTES", 64 * 1024, 1024 * 1024)?,
            calls: limit("MYDB_HTTP_MAX_CALLS_PER_STATEMENT", 1024, 1_000_000)?,
        })
    }
}
#[derive(Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Egress {
    pub allowed_origins: Vec<String>,
    #[serde(default)]
    pub private_destinations: Vec<ipnet::IpNet>,
    #[serde(default)]
    pub allow_plaintext_origins: Vec<String>,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UserLimit {
    pub user: String,
    pub host: String,
    pub max_inflight: Option<usize>,
    pub requests_per_minute: Option<u64>,
    pub max_http_attempts_per_statement: Option<usize>,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Policy {
    pub version: u32,
    #[serde(default)]
    pub egress: Option<Egress>,
    /// Canonical origins mapped to administrator-owned PEM CA bundle paths.
    #[serde(default)]
    pub ca_bindings: BTreeMap<String, String>,
    #[serde(default)]
    pub user_limits: Vec<UserLimit>,
    #[serde(skip)]
    pub generation: u64,
    #[serde(skip)]
    pub source: String,
    #[serde(skip)]
    pub certificates: BTreeMap<String, Vec<reqwest::Certificate>>,
    #[serde(skip)]
    _charge: Option<Arc<crate::engine::executor::mem_budget::ControlledMemoryCharge>>,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            version: 1,
            egress: None,
            ca_bindings: BTreeMap::new(),
            user_limits: vec![],
            generation: 0,
            source: String::new(),
            certificates: BTreeMap::new(),
            _charge: None,
        }
    }
}
static POLICIES: OnceLock<RwLock<BTreeMap<String, Arc<Policy>>>> = OnceLock::new();
static GENERATION: AtomicU64 = AtomicU64::new(1);
static ENABLED: AtomicI8 = AtomicI8::new(-1);
fn policies() -> &'static RwLock<BTreeMap<String, Arc<Policy>>> {
    POLICIES.get_or_init(Default::default)
}
fn source() -> Result<String, QueryError> {
    match crate::util::env::var("MYDB_HTTP_LOCAL_CONFIG") {
        Err(std::env::VarError::NotPresent) => Ok(String::new()),
        Ok(v) if !v.is_empty() => Ok(v),
        _ => Err(error(
            "ER_HTTP_INVALID_ARGUMENT",
            "invalid http_local_config path",
        )),
    }
}
pub(crate) fn enabled() -> bool {
    match ENABLED.load(Ordering::Acquire) {
        0 => false,
        1 => true,
        _ => crate::util::env::flag("MYDB_HTTP_ENABLED", true),
    }
}
pub(crate) fn set_enabled(value: bool) -> Result<(), QueryError> {
    if value {
        load()?;
        Limits::load()?;
    }
    ENABLED.store(i8::from(value), Ordering::Release);
    // A generation change also invalidates calls when OFF is immediately followed by ON.
    GENERATION.fetch_add(1, Ordering::AcqRel);
    Ok(())
}
pub(crate) fn switch_generation() -> u64 {
    GENERATION.load(Ordering::Acquire)
}
pub(crate) fn load() -> Result<Arc<Policy>, QueryError> {
    let path = source()?;
    if let Some(p) = policies()
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(&path)
    {
        return Ok(p.clone());
    }
    reload()
}
pub(crate) fn reload() -> Result<Arc<Policy>, QueryError> {
    let path = source()?;
    let mut charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
        1024,
    )?;
    let mut p = if path.is_empty() {
        Policy::default()
    } else {
        let bytes = read_bounded(&path, 1024 * 1024)?;
        charge.try_grow(bytes.len().saturating_mul(32))?;
        let text = std::str::from_utf8(&bytes)
            .map_err(|_| error("ER_HTTP_INVALID_ARGUMENT", "policy must be UTF-8"))?;
        serde_json::from_value::<Policy>(parse_json(text, 1024 * 1024)?)
            .map_err(|_| error("ER_HTTP_INVALID_ARGUMENT", "invalid HTTP policy fields"))?
    };
    if p.version != 1 {
        return Err(error(
            "ER_HTTP_INVALID_ARGUMENT",
            "unsupported HTTP policy version",
        ));
    }
    if let Some(e) = &p.egress {
        for origin in e.allowed_origins.iter().chain(&e.allow_plaintext_origins) {
            validate_origin(origin)?;
        }
    }
    if p.ca_bindings.len() > 128 || p.user_limits.len() > 1024 {
        return Err(error(
            "ER_HTTP_BUDGET_EXCEEDED",
            "HTTP policy has too many entries",
        ));
    }
    let mut ca_bytes = 0usize;
    for (origin, path) in &p.ca_bindings {
        validate_origin(origin)?;
        let bytes = read_bounded(path, 1024 * 1024)?;
        ca_bytes = ca_bytes.saturating_add(bytes.len());
        if ca_bytes > 4 * 1024 * 1024 {
            return Err(error(
                "ER_HTTP_BUDGET_EXCEEDED",
                "HTTP CA bundles exceed 4 MiB",
            ));
        }
        charge.try_grow(bytes.len().saturating_mul(8))?;
        let certs = reqwest::Certificate::from_pem_bundle(&bytes)
            .map_err(|_| error("ER_HTTP_INVALID_ARGUMENT", "invalid HTTP CA bundle"))?;
        if certs.is_empty() {
            return Err(error("ER_HTTP_INVALID_ARGUMENT", "empty HTTP CA bundle"));
        }
        p.certificates.insert(origin.clone(), certs);
    }
    let mut users = std::collections::BTreeSet::new();
    for u in &p.user_limits {
        if !users.insert((&u.user, &u.host))
            || u.max_inflight.is_some_and(|v| v == 0 || v > 256)
            || u.requests_per_minute == Some(0)
            || u.max_http_attempts_per_statement == Some(0)
        {
            return Err(error(
                "ER_HTTP_INVALID_ARGUMENT",
                "invalid or duplicate HTTP account limits",
            ));
        }
    }
    p.source = path.clone();
    p._charge = Some(Arc::new(charge));
    let mut cache = policies().write().unwrap_or_else(|e| e.into_inner());
    if cache.len() >= 64 && !cache.contains_key(&path) {
        return Err(error(
            "ER_HTTP_BUDGET_EXCEEDED",
            "too many HTTP policy sources",
        ));
    }
    p.generation = GENERATION.fetch_add(1, Ordering::AcqRel) + 1;
    let p = Arc::new(p);
    cache.insert(path, p.clone());
    Ok(p)
}
pub(crate) fn read_bounded(path: &str, limit: usize) -> Result<Vec<u8>, QueryError> {
    use std::io::Read;
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK);
    }
    let mut file = options
        .open(path)
        .map_err(|_| error("ER_HTTP_INVALID_ARGUMENT", "cannot open HTTP policy or CA"))?;
    if !file
        .metadata()
        .map_err(|_| error("ER_HTTP_INVALID_ARGUMENT", "cannot stat HTTP policy or CA"))?
        .is_file()
    {
        return Err(error(
            "ER_HTTP_INVALID_ARGUMENT",
            "HTTP policy or CA must be a regular file",
        ));
    }
    let mut bytes = Vec::new();
    file.by_ref()
        .take((limit + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| error("ER_HTTP_INVALID_ARGUMENT", "cannot read HTTP policy or CA"))?;
    if bytes.len() > limit {
        return Err(error(
            "ER_HTTP_INVALID_ARGUMENT",
            "HTTP policy or CA exceeds limit",
        ));
    }
    Ok(bytes)
}
fn validate_origin(s: &str) -> Result<(), QueryError> {
    let u = parse_url(s)?;
    if u.path() != "/" || u.query().is_some() || u.origin().ascii_serialization() != s {
        return Err(error(
            "ER_HTTP_INVALID_ARGUMENT",
            "policy requires canonical origins",
        ));
    }
    Ok(())
}
pub(crate) fn parse_url(s: &str) -> Result<reqwest::Url, QueryError> {
    if s.len() > 16384 || s.bytes().any(|b| b < 32 || b == 127) {
        return Err(error(
            "ER_HTTP_INVALID_ARGUMENT",
            "invalid or oversized URL",
        ));
    }
    let u = reqwest::Url::parse(s)
        .map_err(|_| error("ER_HTTP_INVALID_ARGUMENT", "invalid HTTP URL"))?;
    if !matches!(u.scheme(), "http" | "https")
        || u.host_str().is_none()
        || !u.username().is_empty()
        || u.password().is_some()
        || u.fragment().is_some()
    {
        return Err(error(
            "ER_HTTP_INVALID_ARGUMENT",
            "HTTP URL must have a host and no userinfo or fragment",
        ));
    }
    Ok(u)
}
impl Policy {
    pub fn current(&self) -> bool {
        policies()
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&self.source)
            .is_some_and(|p| p.generation == self.generation)
    }
    pub fn check_url(&self, u: &reqwest::Url) -> Result<(), QueryError> {
        if let Some(e) = &self.egress {
            let origin = u.origin().ascii_serialization();
            if !e.allowed_origins.contains(&origin)
                || (u.scheme() == "http" && !e.allow_plaintext_origins.contains(&origin))
            {
                return Err(error(
                    "ER_HTTP_EGRESS_DENIED",
                    "HTTP origin denied by node policy",
                ));
            }
        }
        Ok(())
    }
    pub fn check_ip(&self, ip: IpAddr) -> Result<(), QueryError> {
        let Some(e) = &self.egress else {
            return Ok(());
        };
        let ip = match ip {
            IpAddr::V6(v) => v.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(ip),
            _ => ip,
        };
        let restricted = match ip {
            IpAddr::V4(v) => {
                v.is_private()
                    || v.is_loopback()
                    || v.is_link_local()
                    || v.is_unspecified()
                    || v.is_multicast()
                    || v.is_broadcast()
                    || v.octets()[0] == 0
                    || v.octets()[0] >= 240
                    || (v.octets()[0] == 100 && (64..=127).contains(&v.octets()[1]))
                    || (v.octets()[0] == 198 && (18..=19).contains(&v.octets()[1]))
                    || (v.octets()[0] == 192 && v.octets()[1] == 0)
            }
            IpAddr::V6(v) => {
                v.is_loopback()
                    || v.is_unspecified()
                    || v.is_multicast()
                    || v.is_unique_local()
                    || v.is_unicast_link_local()
                    || (v.segments()[0] & 0xffc0) == 0xfec0
                    || (v.segments()[0] == 0x2001 && v.segments()[1] == 0xdb8)
                    || v.segments()[0] == 0x2002
                    || v.segments()[..6] == [0x64, 0xff9b, 0, 0, 0, 0]
                    || v.segments()[..6] == [0, 0, 0, 0, 0, 0]
            }
        };
        if restricted && !e.private_destinations.iter().any(|n| n.contains(&ip)) {
            return Err(error(
                "ER_HTTP_EGRESS_DENIED",
                "HTTP destination IP denied by node policy",
            ));
        }
        Ok(())
    }
}
