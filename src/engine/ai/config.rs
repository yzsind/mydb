use super::*;
use serde::de::{MapAccess, SeqAccess, Visitor};
use serde_json::Value as Json;
use std::fmt;
use std::net::IpAddr;
use std::path::Path;

/// Enforce duplicate-key and nesting limits before creating a JSON DOM.
pub(crate) fn parse_json(input: &str, max: usize) -> Result<Json, QueryError> {
    if input.len() > max {
        return Err(error("ER_AI_INVALID_ARGUMENT", "JSON exceeds byte limit"));
    }
    struct Strict(Json);
    impl<'de> Deserialize<'de> for Strict {
        fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
            struct V;
            impl<'de> Visitor<'de> for V {
                type Value = Strict;
                fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    f.write_str("bounded JSON")
                }
                fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<Strict, E> {
                    Ok(Strict(v.into()))
                }
                fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Strict, E> {
                    Ok(Strict(v.into()))
                }
                fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Strict, E> {
                    Ok(Strict(v.into()))
                }
                fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Strict, E> {
                    serde_json::Number::from_f64(v)
                        .map(|n| Strict(Json::Number(n)))
                        .ok_or_else(|| E::custom("non-finite number"))
                }
                fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Strict, E> {
                    Ok(Strict(v.into()))
                }
                fn visit_string<E: serde::de::Error>(self, v: String) -> Result<Strict, E> {
                    Ok(Strict(v.into()))
                }
                fn visit_unit<E: serde::de::Error>(self) -> Result<Strict, E> {
                    Ok(Strict(Json::Null))
                }
                fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<Strict, A::Error> {
                    let mut v = Vec::new();
                    while let Some(x) = a.next_element::<Strict>()? {
                        v.push(x.0);
                    }
                    Ok(Strict(Json::Array(v)))
                }
                fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Strict, A::Error> {
                    let mut m = serde_json::Map::new();
                    while let Some(k) = a.next_key::<String>()? {
                        if m.contains_key(&k) {
                            return Err(serde::de::Error::custom("duplicate JSON key"));
                        }
                        let v = a.next_value::<Strict>()?;
                        m.insert(k, v.0);
                    }
                    Ok(Strict(Json::Object(m)))
                }
            }
            de.deserialize_any(V)
        }
    }
    // Count lexical nesting before recursively invoking serde.
    let (mut depth, mut quoted, mut escaped) = (0usize, false, false);
    for c in input.bytes() {
        if quoted {
            if escaped {
                escaped = false;
            } else if c == b'\\' {
                escaped = true;
            } else if c == b'"' {
                quoted = false;
            }
        } else {
            match c {
                b'"' => quoted = true,
                b'[' | b'{' => {
                    depth += 1;
                    if depth > 32 {
                        return Err(error("ER_AI_INVALID_ARGUMENT", "JSON nesting exceeds 32"));
                    }
                }
                b']' | b'}' => depth = depth.saturating_sub(1),
                _ => {}
            }
        }
    }
    let mut de = serde_json::Deserializer::from_str(input);
    let value = Strict::deserialize(&mut de)
        .map_err(|_| {
            error(
                "ER_AI_INVALID_ARGUMENT",
                "invalid JSON, duplicate key or unsupported value",
            )
        })?
        .0;
    de.end()
        .map_err(|_| error("ER_AI_INVALID_ARGUMENT", "trailing JSON data"))?;
    Ok(value)
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LocalConfig {
    #[serde(skip)]
    pub generation: u64,
    #[serde(skip)]
    pub source_path: String,
    #[serde(skip)]
    pub raw_bytes: usize,
    pub version: u32,
    #[serde(default)]
    pub encryption: Encryption,
    #[serde(default)]
    pub ca_bindings: BTreeMap<String, String>,
    #[serde(default)]
    pub user_limits: Vec<UserLimit>,
    pub egress: Egress,
}
#[derive(Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Encryption {
    pub previous_keys_file: Option<String>,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UserLimit {
    pub user: String,
    pub host: String,
    pub max_inflight: Option<usize>,
    pub requests_per_minute: Option<u64>,
    pub tokens_per_minute: Option<u64>,
    pub max_call_items_per_statement: Option<usize>,
    pub max_http_attempts_per_statement: Option<usize>,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Egress {
    pub allowed_origins: Vec<String>,
    #[serde(default)]
    pub private_destinations: Vec<ipnet::IpNet>,
    #[serde(default)]
    pub allow_plaintext_origins: Vec<String>,
}
impl LocalConfig {
    pub fn load() -> Result<Self, QueryError> {
        let path = crate::util::env::var("MYDB_AI_LOCAL_CONFIG")
            .map_err(|_| error("ER_AI_DISABLED", "ai_local_config is required"))?;
        if let Some(entry) = config_cache()
            .lock()
            .map_err(|_| error("ER_AI_DISABLED", "config cache poisoned"))?
            .get(&path)
        {
            return Ok(entry.config.clone());
        }
        Self::reload()
    }
    /// Cancellation polling needs only the policy version, never a deep clone.
    pub fn generation_is_current(&self) -> Result<bool, QueryError> {
        Ok(config_cache()
            .lock()
            .map_err(|_| error("ER_AI_DISABLED", "config cache poisoned"))?
            .get(&self.source_path)
            .is_none_or(|entry| entry.config.generation == self.generation))
    }
    pub fn current(&self) -> Result<Self, QueryError> {
        Ok(config_cache()
            .lock()
            .map_err(|_| error("ER_AI_DISABLED", "config cache poisoned"))?
            .get(&self.source_path)
            .map(|e| e.config.clone())
            .unwrap_or_else(|| self.clone()))
    }
    pub fn reload() -> Result<Self, QueryError> {
        let mut candidate = Self::read()?;
        static GENERATION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        candidate.generation = GENERATION.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
            candidate.raw_bytes.saturating_mul(32),
        )?;
        let mut cache = config_cache()
            .lock()
            .map_err(|_| error("ER_AI_DISABLED", "config cache poisoned"))?;
        if cache.len() >= 64 && !cache.contains_key(&candidate.source_path) {
            return Err(error(
                "ER_AI_BUDGET_EXCEEDED",
                "too many local config sources",
            ));
        }
        cache.insert(
            candidate.source_path.clone(),
            ConfigEntry {
                config: candidate.clone(),
                _charge: charge,
            },
        );
        Ok(candidate)
    }
    fn read() -> Result<Self, QueryError> {
        let path = crate::util::env::var("MYDB_AI_LOCAL_CONFIG")
            .map_err(|_| error("ER_AI_DISABLED", "ai_local_config is required"))?;
        let data = read_bounded(Path::new(&path), 1024 * 1024)
            .map_err(|_| error("ER_AI_INVALID_ARGUMENT", "cannot read ai_local_config"))?;
        let text = std::str::from_utf8(&data)
            .map_err(|_| error("ER_AI_INVALID_ARGUMENT", "local config must be UTF-8"))?;
        let value = parse_json(text, 1024 * 1024)?;
        let mut cfg: Self = serde_json::from_value(value)
            .map_err(|_| error("ER_AI_INVALID_ARGUMENT", "invalid local AI config fields"))?;
        if cfg.version != 1 {
            return Err(error(
                "ER_AI_INVALID_ARGUMENT",
                "unsupported local config version",
            ));
        }
        cfg.source_path = path;
        cfg.raw_bytes = data.len();
        for cert in cfg.ca_bindings.values() {
            let bytes = read_bounded(Path::new(cert), 1024 * 1024)?;
            reqwest::Certificate::from_pem_bundle(&bytes)
                .map_err(|_| error("ER_AI_INVALID_ARGUMENT", "invalid private CA bundle"))?;
        }
        let mut identities = std::collections::BTreeSet::new();
        for u in &cfg.user_limits {
            if !identities.insert((&u.user, &u.host))
                || [
                    u.max_inflight,
                    u.max_call_items_per_statement,
                    u.max_http_attempts_per_statement,
                ]
                .into_iter()
                .flatten()
                .any(|x| x == 0)
                || u.requests_per_minute == Some(0)
            {
                return Err(error(
                    "ER_AI_INVALID_ARGUMENT",
                    "duplicate account or invalid quota",
                ));
            }
            if u.max_inflight.is_some_and(|n| n > 256)
                || u.requests_per_minute.is_some_and(|n| n > 1_000_000_000)
            {
                return Err(error(
                    "ER_AI_INVALID_ARGUMENT",
                    "principal limit exceeds ceiling",
                ));
            }
            if u.tokens_per_minute.is_some() {
                return Err(error(
                    "ER_AI_INVALID_ARGUMENT",
                    "hard TPM requires a tokenizer or gateway",
                ));
            }
        }
        Ok(cfg)
    }
    pub fn check_url(&self, base: &str) -> Result<reqwest::Url, QueryError> {
        let u = reqwest::Url::parse(base)
            .map_err(|_| error("ER_AI_INVALID_ARGUMENT", "invalid base_url"))?;
        if !matches!(u.scheme(), "http" | "https")
            || !u.username().is_empty()
            || u.password().is_some()
            || u.query().is_some()
            || u.fragment().is_some()
            || u.host_str().is_none()
        {
            return Err(error("ER_AI_EGRESS_DENIED", "invalid URL components"));
        }
        let origin = u.origin().ascii_serialization();
        let same = |s: &String| {
            reqwest::Url::parse(s)
                .ok()
                .is_some_and(|x| x.origin().ascii_serialization() == origin)
        };
        if !self.egress.allowed_origins.iter().any(same)
            || (u.scheme() == "http" && !self.egress.allow_plaintext_origins.iter().any(same))
        {
            return Err(error("ER_AI_EGRESS_DENIED", "origin is not allowed"));
        }
        Ok(u)
    }
    pub fn check_ip(&self, ip: IpAddr) -> Result<(), QueryError> {
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
                    || (v.segments()[0] == 0x2001 && v.segments()[1] == 0x0db8)
                    || v.segments()[0] == 0x2002
                    || v.segments()[..6] == [0x64, 0xff9b, 0, 0, 0, 0]
                    || v.segments()[..6] == [0, 0, 0, 0, 0, 0]
            }
        };
        if restricted
            && !self
                .egress
                .private_destinations
                .iter()
                .any(|n| n.contains(&ip))
        {
            return Err(error(
                "ER_AI_EGRESS_DENIED",
                "destination IP is not allowed",
            ));
        }
        Ok(())
    }
}
static ENABLED: std::sync::atomic::AtomicI8 = std::sync::atomic::AtomicI8::new(-1);
pub(crate) fn enabled() -> bool {
    match ENABLED.load(std::sync::atomic::Ordering::Acquire) {
        0 => false,
        1 => true,
        _ => crate::util::env::flag("MYDB_AI_ENABLED", false),
    }
}
pub(crate) fn dispatch_enabled() -> bool {
    ENABLED.load(std::sync::atomic::Ordering::Acquire) != 0
}
pub(crate) fn set_enabled(value: bool) -> Result<(), QueryError> {
    if value {
        LocalConfig::load()?;
    }
    ENABLED.store(i8::from(value), std::sync::atomic::Ordering::Release);
    Ok(())
}
pub(crate) fn env_limit(name: &str, default: usize) -> usize {
    crate::util::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}
pub(crate) fn validate_runtime_limits() -> Result<(), QueryError> {
    for (name, maximum) in [
        ("MYDB_AI_MAX_INFLIGHT", 256),
        ("MYDB_AI_MAX_QUEUE", 4096),
        ("MYDB_AI_MEMORY_LIMIT_MB", 65536),
    ] {
        match crate::util::env::var(name) {
            Ok(value)
                if value
                    .parse::<usize>()
                    .ok()
                    .is_some_and(|n| n > 0 && n <= maximum) => {}
            Err(std::env::VarError::NotPresent) => {}
            _ => return Err(error("ER_AI_INVALID_ARGUMENT", format!("invalid {name}"))),
        }
    }
    Ok(())
}
fn transport_owned_header(name: &str) -> bool {
    matches!(
        name,
        "host"
            | "content-length"
            | "content-type"
            | "content-encoding"
            | "transfer-encoding"
            | "connection"
            | "keep-alive"
            | "proxy-connection"
            | "te"
            | "trailer"
            | "upgrade"
            | "expect"
            | "cookie"
            | "proxy-authorization"
    )
}
pub(crate) fn validate_connection(c: &Connection) -> Result<(), QueryError> {
    if !matches!(
        c.protocol.as_str(),
        "ollama" | "openai_chat" | "openai_responses" | "openai_embeddings"
    ) {
        return Err(error("ER_AI_INVALID_ARGUMENT", "unsupported protocol"));
    }
    let local = LocalConfig::load()?;
    local.check_url(&c.base_url)?;
    if c.base_url.len() > 2048
        || c.transport
            .connect_timeout_ms
            .is_some_and(|v| v == 0 || v > 60000)
        || c.transport
            .ca_file
            .as_ref()
            .is_some_and(|name| !local.ca_bindings.contains_key(name))
    {
        return Err(error(
            "ER_AI_INVALID_ARGUMENT",
            "invalid transport configuration",
        ));
    }
    if c.profile.as_deref().is_some_and(|p| {
        p != "default" && !(p == "openai_chat_legacy" && c.protocol == "openai_chat")
    }) {
        return Err(error(
            "ER_AI_INVALID_ARGUMENT",
            "unsupported adapter profile",
        ));
    }
    if c.limits.max_input_bytes.is_some()
        || c.limits.max_input_tokens.is_some()
        || c.limits.max_output_tokens.is_some()
        || c.limits.max_batch_items.is_some()
        || c.limits.max_retries.is_some()
        || c.limits.allow_retry_ambiguous
    {
        return Err(error(
            "ER_AI_INVALID_ARGUMENT",
            "model limits cannot be configured on a connection",
        ));
    }
    let auth_header = match &c.auth {
        Auth::None => None,
        Auth::Bearer { .. } => Some("authorization"),
        Auth::Header { header_name, .. } => Some(header_name.as_str()),
    };
    let mut header_names = std::collections::BTreeSet::new();
    for (name, value) in &c.headers {
        let lower = name.to_ascii_lowercase();
        if transport_owned_header(&lower)
            || matches!(lower.as_str(), "authorization" | "api-key" | "x-api-key")
            || auth_header.is_some_and(|auth| name.eq_ignore_ascii_case(auth))
            || !header_names.insert(lower)
            || reqwest::header::HeaderName::from_bytes(name.as_bytes()).is_err()
            || reqwest::header::HeaderValue::from_str(value).is_err()
        {
            return Err(error(
                "ER_AI_INVALID_ARGUMENT",
                "duplicate, transport-owned or sensitive fixed header",
            ));
        }
    }
    if let Auth::Header { header_name, .. } = &c.auth {
        if reqwest::header::HeaderName::from_bytes(header_name.as_bytes()).is_err()
            || transport_owned_header(&header_name.to_ascii_lowercase())
        {
            return Err(error(
                "ER_AI_INVALID_ARGUMENT",
                "invalid or transport-owned auth header",
            ));
        }
    }
    validate_limits(&c.limits)
}
pub(crate) fn validate_limits(l: &Limits) -> Result<(), QueryError> {
    if l.max_inflight.is_some_and(|n| n > 256)
        || l.requests_per_minute.is_some_and(|n| n > 1_000_000_000)
        || l.max_retries.is_some_and(|n| n > 8)
    {
        return Err(error(
            "ER_AI_INVALID_ARGUMENT",
            "limit exceeds implementation ceiling",
        ));
    }
    if [
        l.max_inflight,
        l.max_input_bytes,
        l.max_input_tokens,
        l.max_batch_items,
    ]
    .into_iter()
    .flatten()
    .any(|x| x == 0)
        || [
            l.requests_per_minute,
            l.tokens_per_minute,
            l.max_output_tokens,
        ]
        .into_iter()
        .flatten()
        .any(|x| x == 0)
    {
        return Err(error("ER_AI_INVALID_ARGUMENT", "limits must be positive"));
    }
    if l.tokens_per_minute.is_some() || l.max_input_tokens.is_some() {
        return Err(error(
            "ER_AI_INVALID_ARGUMENT",
            "token limits require a configured trusted tokenizer or gateway",
        ));
    }
    Ok(())
}
pub(crate) fn validate_model(m: &Model, c: &Connection) -> Result<(), QueryError> {
    if m.remote_model.is_empty() || m.model_revision.is_empty() {
        return Err(error(
            "ER_AI_INVALID_ARGUMENT",
            "model ID and revision are required",
        ));
    }
    match m.kind.as_str() {
        "embedding" => {
            let e = m
                .embedding
                .as_ref()
                .ok_or_else(|| error("ER_AI_INVALID_ARGUMENT", "embedding config required"))?;
            if !(1..=16383).contains(&e.dimensions)
                || !matches!(e.normalize.as_str(), "none" | "l2")
            {
                return Err(error(
                    "ER_AI_INVALID_ARGUMENT",
                    "invalid dimensions or normalization",
                ));
            }
            if !e.space_options.is_empty() {
                return Err(error(
                    "ER_AI_INVALID_ARGUMENT",
                    "embedding space_options are not supported by these adapters",
                ));
            }
            if !matches!(c.protocol.as_str(), "ollama" | "openai_embeddings") {
                return Err(error(
                    "ER_AI_INVALID_ARGUMENT",
                    "connection does not support embedding",
                ));
            }
        }
        "text_generation" => {
            if m.embedding.is_some() || c.protocol == "openai_embeddings" {
                return Err(error(
                    "ER_AI_INVALID_ARGUMENT",
                    "model kind and connection disagree",
                ));
            }
        }
        _ => return Err(error("ER_AI_INVALID_ARGUMENT", "unknown model kind")),
    }
    for k in m.capabilities.keys() {
        if !matches!(
            k.as_str(),
            "messages" | "json_schema" | "temperature" | "top_p" | "stop" | "seed"
        ) {
            return Err(error("ER_AI_INVALID_ARGUMENT", "unknown capability"));
        }
    }
    if m.kind == "embedding" && m.capabilities.values().any(|v| *v) {
        return Err(error(
            "ER_AI_INVALID_ARGUMENT",
            "generation capabilities do not apply to embedding models",
        ));
    }
    if c.protocol == "openai_responses"
        && ["seed", "stop"]
            .iter()
            .any(|name| m.capabilities.get(*name) == Some(&true))
    {
        return Err(error(
            "ER_AI_INVALID_ARGUMENT",
            "Responses adapter does not support seed or stop",
        ));
    }
    if m.remote_model.len() > 256
        || m.model_revision.len() > 128
        || m.limits.max_batch_items.is_some_and(|v| v > 64)
        || m.limits.max_output_tokens.is_some_and(|v| v > 4096)
        || m.limits.max_input_bytes.is_some_and(|v| v > 262144)
    {
        return Err(error(
            "ER_AI_INVALID_ARGUMENT",
            "model limit exceeds implementation ceiling",
        ));
    }
    validate_limits(&m.limits)?;
    super::runtime::validate_defaults(m)
}
pub(crate) fn read_bounded(path: &Path, max: usize) -> Result<Vec<u8>, QueryError> {
    use std::io::Read;
    let file = std::fs::File::open(path)
        .map_err(|_| error("ER_AI_INVALID_ARGUMENT", "cannot open AI file"))?;
    let mut v = Vec::new();
    file.take(max as u64 + 1)
        .read_to_end(&mut v)
        .map_err(|_| error("ER_AI_INVALID_ARGUMENT", "cannot read AI file"))?;
    if v.len() > max {
        return Err(error(
            "ER_AI_INVALID_ARGUMENT",
            "AI file exceeds size limit",
        ));
    }
    Ok(v)
}

struct ConfigEntry {
    config: LocalConfig,
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}
fn config_cache() -> &'static std::sync::Mutex<std::collections::HashMap<String, ConfigEntry>> {
    static CACHE: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<String, ConfigEntry>>,
    > = std::sync::OnceLock::new();
    CACHE.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}
