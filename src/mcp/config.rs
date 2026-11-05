use std::io;

/// MCP limits supplement the shared HTTP/SQL service limits.
#[derive(Clone, Debug)]
pub struct McpConfig {
    pub enabled: bool,
    pub max_inflight: usize,
    pub max_result_rows: usize,
    pub max_result_bytes: usize,
    pub allowed_origins: Vec<String>,
}
impl Default for McpConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_inflight: 16,
            max_result_rows: 1000,
            max_result_bytes: 1048576,
            allowed_origins: Vec::new(),
        }
    }
}
impl McpConfig {
    pub fn from_startup() -> io::Result<Self> {
        Self::read(|name| crate::startup_config::var(name).ok())
    }
    fn read(get: impl Fn(&str) -> Option<String>) -> io::Result<Self> {
        let positive = |name: &str, default: usize| -> io::Result<usize> {
            match get(name) {
                None => Ok(default),
                Some(v) => v
                    .parse::<usize>()
                    .ok()
                    .filter(|n| *n > 0 && *n <= 1073741824)
                    .ok_or_else(|| io::Error::other(format!("Invalid {name}"))),
            }
        };
        let enabled = match get("MYDB_MCP_ENABLED")
            .as_deref()
            .unwrap_or("OFF")
            .to_ascii_lowercase()
            .as_str()
        {
            "on" | "true" | "1" => true,
            "off" | "false" | "0" => false,
            _ => return Err(io::Error::other("Invalid mcp_enabled")),
        };
        let max_result_bytes = positive("MYDB_MCP_MAX_RESULT_BYTES", 1048576)?;
        if max_result_bytes < 8192 {
            return Err(io::Error::other(
                "mcp_max_result_bytes must be at least 8192",
            ));
        }
        let allowed_origins: Vec<String> =
            serde_json::from_str(&get("MYDB_MCP_ALLOWED_ORIGINS").unwrap_or_else(|| "[]".into()))
                .map_err(|_| {
                io::Error::other("mcp_allowed_origins must be a JSON array of HTTP origins")
            })?;
        if allowed_origins
            .iter()
            .any(|origin| parse_origin(origin).is_none())
        {
            return Err(io::Error::other("MCP origins require a valid HTTP(S) scheme, host and optional numeric port, without credentials, fragments, queries, paths or wildcards"));
        }
        Ok(Self {
            enabled,
            max_inflight: positive("MYDB_MCP_MAX_INFLIGHT", 16)?,
            max_result_rows: positive("MYDB_MCP_MAX_RESULT_ROWS", 1000)?,
            max_result_bytes,
            allowed_origins,
        })
    }
}
/// Use the same strict parser for configuration and incoming Origin headers.
/// http::Uri permits malformed/out-of-range port text, so a missing port_u16
/// must not silently turn an explicit invalid port into the scheme default.
pub(super) fn parse_origin(value: &str) -> Option<(String, String, u16)> {
    if value.contains('#') {
        return None;
    }
    let uri: axum::http::Uri = value.parse().ok()?;
    let scheme = uri.scheme_str()?.to_ascii_lowercase();
    let authority = uri.authority()?;
    if !matches!(scheme.as_str(), "http" | "https")
        || authority.as_str().contains(['@', '*'])
        || uri.path_and_query().is_some_and(|p| p.as_str() != "/")
    {
        return None;
    }
    let raw_host = authority.host();
    if raw_host.is_empty() {
        return None;
    }
    let host = if raw_host.starts_with('[') {
        format!(
            "[{}]",
            raw_host
                .strip_prefix('[')?
                .strip_suffix(']')?
                .parse::<std::net::Ipv6Addr>()
                .ok()?
        )
    } else {
        raw_host.to_ascii_lowercase()
    };
    let suffix = &authority.as_str()[raw_host.len()..];
    let port = if suffix.is_empty() {
        if scheme == "https" {
            443
        } else {
            80
        }
    } else {
        let port = suffix.strip_prefix(':')?;
        if port.is_empty() || !port.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        port.parse::<u16>().ok()?
    };
    Some((scheme, host, port))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validate_configuration() {
        assert!(!McpConfig::read(|_| None).unwrap().enabled);
        for (name, value) in [
            ("MYDB_MCP_ENABLED", "maybe"),
            ("MYDB_MCP_MAX_RESULT_BYTES", "8191"),
            ("MYDB_MCP_MAX_RESULT_ROWS", "0"),
            ("MYDB_MCP_ALLOWED_ORIGINS", "[\"*\"]"),
            ("MYDB_MCP_ALLOWED_ORIGINS", "[\"https://app.example/path\"]"),
        ] {
            assert!(
                McpConfig::read(|n| (n == name).then(|| value.into())).is_err(),
                "{name}"
            );
        }
        assert!(McpConfig::read(
            |n| (n == "MYDB_MCP_ALLOWED_ORIGINS").then(|| "[\"https://app.example:443\"]".into())
        )
        .is_ok());
    }
}
