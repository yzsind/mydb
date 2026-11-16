use clap::{ArgAction, Parser, ValueEnum};
use mysql::{consts::CapabilityFlags, ClientIdentity, LocalInfileHandler, OptsBuilder, SslOpts};
use std::{ffi::OsString, io, path::PathBuf, time::Duration};

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SslMode {
    Disabled,
    Required,
    VerifyCa,
    VerifyIdentity,
}

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Interactive and batch SQL client for MyDB",
    disable_help_flag = true,
    args_override_self = true
)]
pub struct Args {
    #[arg(long, short = '?', action = ArgAction::Help)]
    help: Option<bool>,
    #[arg(short = 'h', long, default_value = "127.0.0.1")]
    pub host: String,
    #[arg(short = 'P', long, default_value = "8738", value_parser = clap::value_parser!(u16).range(1..))]
    pub port: u16,
    #[arg(short = 'u', long, default_value = "root")]
    pub user: String,
    #[arg(short = 'D', long, conflicts_with = "database_name")]
    pub database: Option<String>,
    #[arg(value_name = "DATABASE")]
    pub database_name: Option<String>,
    #[arg(long, num_args = 0..=1, require_equals = true)]
    pub password: Option<Option<String>>,
    #[arg(short = 'e', long, action = ArgAction::Append)]
    pub execute: Vec<String>,
    #[arg(short = 'B', long, action = ArgAction::Set, num_args = 0..=1, require_equals = true, default_value = "false", default_missing_value = "true")]
    pub batch: bool,
    #[arg(short = 'N', long, action = ArgAction::Set, num_args = 0..=1, require_equals = true, default_value = "false", default_missing_value = "true")]
    pub skip_column_names: bool,
    #[arg(short = 'r', long, action = ArgAction::Set, num_args = 0..=1, require_equals = true, default_value = "false", default_missing_value = "true")]
    pub raw: bool,
    #[arg(short = 't', long, conflicts_with = "vertical", action = ArgAction::Set, num_args = 0..=1, require_equals = true, default_value = "false", default_missing_value = "true")]
    pub table: bool,
    #[arg(short = 'E', long, action = ArgAction::Set, num_args = 0..=1, require_equals = true, default_value = "false", default_missing_value = "true")]
    pub vertical: bool,
    #[arg(short = 'q', long, action = ArgAction::Set, num_args = 0..=1, require_equals = true, default_value = "false", default_missing_value = "true")]
    pub quick: bool,
    #[arg(long, action = ArgAction::Set, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    pub binary_as_hex: Option<bool>,
    #[arg(short = 'f', long, action = ArgAction::Set, num_args = 0..=1, require_equals = true, default_value = "false", default_missing_value = "true")]
    pub force: bool,
    #[arg(long, default_value = ";")]
    pub delimiter: String,
    #[arg(long, action = ArgAction::Set, num_args = 0..=1, require_equals = true, default_value = "false", default_missing_value = "true")]
    pub show_warnings: bool,
    #[arg(long, default_value = "10", value_parser = clap::value_parser!(u64).range(1..))]
    pub connect_timeout: u64,
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    pub read_timeout: Option<u64>,
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    pub write_timeout: Option<u64>,
    #[arg(long, value_enum, default_value = "REQUIRED")]
    pub ssl_mode: SslMode,
    #[arg(long)]
    pub ssl_ca: Option<PathBuf>,
    #[arg(long, requires = "ssl_key")]
    pub ssl_cert: Option<PathBuf>,
    #[arg(long, requires = "ssl_cert")]
    pub ssl_key: Option<PathBuf>,
    #[arg(long, default_value = "0", value_parser = clap::value_parser!(u8).range(0..=1))]
    pub local_infile: u8,
    #[arg(long, action = ArgAction::Set, num_args = 0..=1, require_equals = true, default_value = "false", default_missing_value = "true")]
    pub no_history: bool,
    #[arg(long)]
    pub history_file: Option<PathBuf>,
    #[arg(long, action = ArgAction::Set, num_args = 0..=1, require_equals = true, default_value = "false", default_missing_value = "true")]
    pub skip_auto_rehash: bool,
    #[arg(long, conflicts_with = "no_config")]
    pub config: Option<PathBuf>,
    #[arg(long)]
    pub no_config: bool,
}

const KEYS: &[&str] = &[
    "host",
    "port",
    "user",
    "database",
    "password",
    "batch",
    "skip-column-names",
    "raw",
    "table",
    "vertical",
    "quick",
    "binary-as-hex",
    "force",
    "delimiter",
    "show-warnings",
    "connect-timeout",
    "read-timeout",
    "write-timeout",
    "ssl-mode",
    "ssl-ca",
    "ssl-cert",
    "ssl-key",
    "local-infile",
    "no-history",
    "history-file",
    "skip-auto-rehash",
];

pub fn config_tokens(text: &str) -> Result<Vec<OsString>, String> {
    let mut tokens = Vec::new();
    let mut section = false;
    let mut seen = std::collections::HashSet::new();
    for (line_no, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if line.starts_with('[') {
            if line != "[mydb-cli]" {
                return Err(format!("config line {}: expected [mydb-cli]", line_no + 1));
            }
            section = true;
            continue;
        }
        if !section {
            return Err(format!(
                "config line {}: key outside [mydb-cli]",
                line_no + 1
            ));
        }
        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| format!("config line {}: expected key=value", line_no + 1))?;
        let key = key.trim();
        if !KEYS.contains(&key) || !seen.insert(key.to_owned()) {
            return Err(format!(
                "config line {}: unknown or duplicate key {key:?}",
                line_no + 1
            ));
        }
        let value = value.trim();
        let value = if value.starts_with(['\'', '"']) {
            let q = value.as_bytes()[0] as char;
            if value.len() < 2 || !value.ends_with(q) {
                return Err(format!("config line {}: unclosed value", line_no + 1));
            }
            &value[1..value.len() - 1]
        } else {
            value
        };
        tokens.push(format!("--{key}={value}").into());
    }
    Ok(tokens)
}

fn home() -> Option<PathBuf> {
    std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" }).map(PathBuf::from)
}

pub fn default_config() -> Option<PathBuf> {
    if cfg!(windows) {
        std::env::var_os("APPDATA").map(|p| PathBuf::from(p).join("mydb-cli/config.ini"))
    } else {
        home().map(|p| p.join(".mydb-cli.cnf"))
    }
}

pub fn default_history() -> Option<PathBuf> {
    home().map(|p| p.join(".mydb-cli_history"))
}

fn normalize_password(args: Vec<OsString>) -> Vec<OsString> {
    let mut positional = false;
    args.into_iter()
        .map(|arg| {
            if arg == "--" {
                positional = true;
            }
            if !positional {
                if let Some(s) = arg.to_str() {
                    if s == "-p" {
                        return "--password".into();
                    }
                    if let Some(value) = s.strip_prefix("-p") {
                        return format!("--password={value}").into();
                    }
                }
            }
            arg
        })
        .collect()
}

impl Args {
    pub fn load() -> Result<Self, String> {
        let raw: Vec<OsString> = std::env::args_os().collect();
        // Help/version must also work when a user's config is broken.
        if raw
            .iter()
            .any(|a| matches!(a.to_str(), Some("--help" | "-?" | "--version" | "-V")))
        {
            return Ok(Self::parse_from(normalize_password(raw)));
        }
        let mut explicit = None;
        let mut no_config = false;
        let mut i = 1;
        while i < raw.len() {
            if raw[i] == "--" {
                break;
            }
            if raw[i] == "--no-config" {
                no_config = true;
            }
            if raw[i] == "--config" {
                i += 1;
                explicit = Some(PathBuf::from(raw.get(i).ok_or("--config requires a path")?));
            } else if let Some(s) = raw[i].to_str().and_then(|s| s.strip_prefix("--config=")) {
                explicit = Some(PathBuf::from(s));
            }
            i += 1;
        }
        if no_config && explicit.is_some() {
            return Err("--config conflicts with --no-config".into());
        }
        let mut merged = vec![raw[0].clone()];
        if !no_config {
            if let Some(path) = explicit.as_ref().cloned().or_else(default_config) {
                match std::fs::read_to_string(&path) {
                    Ok(text) => merged.extend(
                        config_tokens(&text).map_err(|e| format!("{}: {e}", path.display()))?,
                    ),
                    Err(e) if explicit.is_none() && e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(format!("{}: {e}", path.display())),
                }
            }
        }
        for key in KEYS {
            let name = format!("MYDB_CLI_{}", key.replace('-', "_").to_uppercase());
            if let Some(value) = std::env::var_os(&name) {
                let mut token = OsString::from(format!("--{key}="));
                token.push(value);
                merged.push(token);
            }
        }
        merged.extend(raw.into_iter().skip(1));
        let args = Self::try_parse_from(normalize_password(merged)).map_err(|e| e.to_string())?;
        args.validate()?;
        Ok(args)
    }

    pub fn validate(&self) -> Result<(), String> {
        crate::input::validate_delimiter(&self.delimiter)?;
        if self.ssl_mode == SslMode::Disabled && (self.ssl_ca.is_some() || self.ssl_cert.is_some())
        {
            return Err("TLS certificate options conflict with --ssl-mode=DISABLED".into());
        }
        if matches!(self.ssl_mode, SslMode::VerifyCa | SslMode::VerifyIdentity)
            && self.ssl_ca.is_none()
        {
            return Err("VERIFY_CA and VERIFY_IDENTITY require --ssl-ca".into());
        }
        if self.ssl_mode == SslMode::Required && self.ssl_ca.is_some() {
            return Err("--ssl-ca requires VERIFY_CA or VERIFY_IDENTITY".into());
        }
        for path in [&self.ssl_ca, &self.ssl_cert, &self.ssl_key]
            .into_iter()
            .flatten()
        {
            std::fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
        }
        Ok(())
    }

    pub fn database(&self) -> Option<&str> {
        self.database.as_deref().or(self.database_name.as_deref())
    }

    pub fn opts(&self, password: &str, control: bool) -> OptsBuilder {
        let mut opts = OptsBuilder::new()
            .ip_or_hostname(Some(self.host.clone()))
            .tcp_port(self.port)
            .user(Some(self.user.clone()))
            .pass(Some(password.to_owned()))
            .db_name(if control {
                None
            } else {
                self.database().map(str::to_owned)
            })
            .prefer_socket(false)
            .stmt_cache_size(0)
            .tcp_connect_timeout(Some(Duration::from_secs(if control {
                3
            } else {
                self.connect_timeout
            })))
            .read_timeout(if control {
                Some(Duration::from_secs(3))
            } else {
                self.read_timeout.map(Duration::from_secs)
            })
            .write_timeout(if control {
                Some(Duration::from_secs(3))
            } else {
                self.write_timeout.map(Duration::from_secs)
            })
            .additional_capabilities(
                CapabilityFlags::CLIENT_SESSION_TRACK | CapabilityFlags::CLIENT_DEPRECATE_EOF,
            );
        if self.ssl_mode != SslMode::Disabled {
            let mut ssl = SslOpts::default()
                .with_danger_accept_invalid_certs(self.ssl_mode == SslMode::Required)
                .with_danger_skip_domain_validation(self.ssl_mode != SslMode::VerifyIdentity)
                .with_root_cert_path(self.ssl_ca.clone());
            if let (Some(cert), Some(key)) = (&self.ssl_cert, &self.ssl_key) {
                ssl =
                    ssl.with_client_identity(Some(ClientIdentity::new(cert.clone(), key.clone())));
            }
            opts = opts.ssl_opts(Some(ssl));
        }
        let enabled = self.local_infile == 1 && !control;
        opts.local_infile_handler(Some(LocalInfileHandler::new(move |name, out| {
            if !enabled {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "LOCAL INFILE is disabled; use --local-infile=1",
                ));
            }
            #[cfg(unix)]
            let path = {
                use std::os::unix::ffi::OsStrExt;
                std::path::Path::new(std::ffi::OsStr::from_bytes(name))
            };
            #[cfg(not(unix))]
            let path = std::path::Path::new(
                std::str::from_utf8(name)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
            );
            let mut file = std::fs::File::open(path)?;
            io::copy(&mut file, out)?;
            Ok(())
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn password_does_not_consume_database() {
        for raw in [vec!["cli", "-p", "test"], vec!["cli", "--password", "test"]] {
            let args = Args::try_parse_from(normalize_password(
                raw.into_iter().map(Into::into).collect(),
            ))
            .unwrap();
            assert_eq!(args.password, Some(None));
            assert_eq!(args.database(), Some("test"));
        }
        let args = Args::try_parse_from(normalize_password(vec![
            "cli".into(),
            "-proot".into(),
            "test".into(),
        ]))
        .unwrap();
        assert_eq!(args.password, Some(Some("root".into())));
        assert!(Args::try_parse_from(["cli", "--password=", "test"])
            .unwrap()
            .password
            .unwrap()
            .unwrap()
            .is_empty());
    }
    #[test]
    fn config_is_strict_and_preserves_password_punctuation() {
        assert_eq!(
            config_tokens("[mydb-cli]\npassword=\"a#b;c=1\"\nport=8738").unwrap(),
            vec![
                OsString::from("--password=a#b;c=1"),
                OsString::from("--port=8738")
            ]
        );
        for text in [
            "host=x",
            "[client]",
            "[mydb-cli]\nunknown=1",
            "[mydb-cli]\nport=1\nport=2",
            "[mydb-cli]\nuser='x",
        ] {
            assert!(config_tokens(text).is_err(), "{text}");
        }
    }
    #[test]
    fn repeated_values_and_execute_order() {
        let args = Args::try_parse_from([
            "cli",
            "--port=1234",
            "--port=8738",
            "-e",
            "SELECT 1",
            "-e",
            "SELECT 2",
            "-BN",
        ])
        .unwrap();
        assert_eq!(args.port, 8738);
        assert_eq!(args.execute, ["SELECT 1", "SELECT 2"]);
        assert!(args.batch && args.skip_column_names);
        assert!(Args::try_parse_from(["cli", "--socket=/tmp/a"]).is_err());
    }
}
