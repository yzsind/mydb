use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    time::Duration,
};
#[derive(Debug, Clone)]
pub struct RestConfig {
    pub enabled: bool,
    pub addr: SocketAddr,
    pub tls: bool,
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
    pub max_connections: usize,
    pub max_sessions: usize,
    pub max_inflight: usize,
    pub max_request_bytes: usize,
    pub max_header_bytes: usize,
    pub max_json_response_bytes: usize,
    pub memory_limit: usize,
    pub default_timeout_ms: u64,
    pub max_timeout_ms: u64,
    pub session_idle: Duration,
    pub transaction_idle: Duration,
}
impl RestConfig {
    pub fn from_startup() -> std::io::Result<Self> {
        let get = |name: &str, default: &str| {
            crate::startup_config::var(&format!("MYDB_REST_{name}"))
                .unwrap_or_else(|_| default.into())
        };
        let n = |name: &str, default: &str| -> std::io::Result<u64> {
            get(name, default)
                .parse::<u64>()
                .ok()
                .filter(|v| *v > 0)
                .ok_or_else(|| {
                    std::io::Error::other(format!("Invalid rest_{}", name.to_ascii_lowercase()))
                })
        };
        let enabled = matches!(
            get("ENABLED", "OFF").to_ascii_lowercase().as_str(),
            "on" | "true" | "1"
        );
        let host = get("HOST", "127.0.0.1")
            .parse::<IpAddr>()
            .map_err(|_| std::io::Error::other("Invalid REST host"))?;
        let port = get("PORT", "8739")
            .parse::<u16>()
            .map_err(|_| std::io::Error::other("Invalid REST port"))?;
        let tls = match get("TLS_MODE", "required").as_str() {
            "required" => true,
            "disabled" => false,
            _ => {
                return Err(std::io::Error::other(
                    "REST TLS mode must be required or disabled",
                ))
            }
        };
        let cert = crate::startup_config::var_os("MYDB_REST_SSL_CERT").map(PathBuf::from);
        let key = crate::startup_config::var_os("MYDB_REST_SSL_KEY").map(PathBuf::from);
        if cert.is_some() != key.is_some() || (!tls && (cert.is_some() || !host.is_loopback())) {
            return Err(std::io::Error::other(
                "REST TLS requires a certificate/key pair; plaintext requires a loopback listener",
            ));
        }
        if n("MAX_JSON_RESPONSE_BYTES", "16777216")? < 1024 {
            return Err(std::io::Error::other(
                "REST JSON response limit must be at least 1024 bytes",
            ));
        }
        let default_timeout_ms = n("DEFAULT_TIMEOUT_MS", "30000")?;
        let max_timeout_ms = n("MAX_TIMEOUT_MS", "300000")?;
        if default_timeout_ms > max_timeout_ms {
            return Err(std::io::Error::other(
                "REST default timeout exceeds maximum",
            ));
        }
        Ok(Self {
            enabled,
            addr: SocketAddr::new(host, port),
            tls,
            cert,
            key,
            max_connections: crate::mysql::connection_manager::configure_open_files_limit()
                .effective_max_connections(n("MAX_CONNECTIONS", "512")? as usize),
            max_sessions: n("MAX_SESSIONS", "256")? as usize,
            max_inflight: n("MAX_INFLIGHT", "64")? as usize,
            max_request_bytes: n("MAX_REQUEST_BYTES", "8388608")? as usize,
            max_header_bytes: n("MAX_HEADER_BYTES", "16384")? as usize,
            max_json_response_bytes: n("MAX_JSON_RESPONSE_BYTES", "16777216")? as usize,
            memory_limit: (n("MEMORY_LIMIT_MB", "128")? as usize)
                .checked_mul(1024 * 1024)
                .ok_or_else(|| std::io::Error::other("REST memory limit overflow"))?,
            default_timeout_ms,
            max_timeout_ms,
            session_idle: Duration::from_millis(n("SESSION_IDLE_TIMEOUT_MS", "300000")?),
            transaction_idle: Duration::from_millis(n("TRANSACTION_IDLE_TIMEOUT_MS", "60000")?),
        })
    }
}
