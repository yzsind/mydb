//! Server identity & build metadata variables. Mostly read-only:
//! version strings, hostname, port, datadir.

use std::sync::{Arc, OnceLock};

use crate::engine::sysvar::backend::SysVar;
use crate::engine::sysvar::meta::{Access, DataType, Scope, SysVarDef};
use crate::engine::sysvar::registry::Registry;
use crate::engine::sysvar::value::SysVarValue;

const VERSION_COMPILE_OS: &str = env!("MYDB_VERSION_COMPILE_OS");
const VERSION_COMPILE_MACHINE: &str = env!("MYDB_VERSION_COMPILE_MACHINE");

/// mydb version string shown by `SELECT @@version`. Includes the MySQL
/// compatibility prefix so clients that string-match the major.minor
/// (`^[58]\.[0-9]+`) treat us as a MySQL 8.0 server.
pub const MYDB_VERSION_STRING: &str = crate::server_version::MYSQL_COMPAT_VERSION;

pub(crate) fn register(reg: &mut Registry) {
    reg.register(SysVar::const_read_only(
        SysVarDef::new("version", Scope::Global, DataType::String { max_len: None })
            .with_doc("Server version string."),
        SysVarValue::String(MYDB_VERSION_STRING.to_string()),
    ));
    reg.register(SysVar::const_read_only(
        SysVarDef::new(
            "version_comment",
            Scope::Global,
            DataType::String { max_len: None },
        )
        .with_doc("Server build comment."),
        SysVarValue::String("mydb".to_string()),
    ));
    reg.register(SysVar::const_read_only(
        SysVarDef::new(
            "version_compile_os",
            Scope::Global,
            DataType::String { max_len: None },
        ),
        SysVarValue::String(VERSION_COMPILE_OS.to_string()),
    ));
    reg.register(SysVar::const_read_only(
        SysVarDef::new(
            "version_compile_machine",
            Scope::Global,
            DataType::String { max_len: None },
        ),
        SysVarValue::String(VERSION_COMPILE_MACHINE.to_string()),
    ));
    reg.register(SysVar::const_read_only(
        SysVarDef::new(
            "version_compile_zlib",
            Scope::Global,
            DataType::String { max_len: None },
        ),
        SysVarValue::String("1.2.13".to_string()),
    ));
    reg.register(SysVar::const_read_only(
        SysVarDef::new(
            "protocol_version",
            Scope::Global,
            DataType::Int { min: 0, max: 255 },
        )
        .with_doc("MySQL wire-protocol version."),
        SysVarValue::Int(10),
    ));
    reg.register(SysVar::const_read_only(
        SysVarDef::new("license", Scope::Global, DataType::String { max_len: None }),
        SysVarValue::String("MIT".to_string()),
    ));
    reg.register(SysVar::runtime_read_only(
        SysVarDef::new(
            "hostname",
            Scope::Global,
            DataType::String { max_len: None },
        )
        .with_doc("Host name of the server machine."),
        || SysVarValue::String(detect_hostname()),
    ));
    reg.register(SysVar::runtime_read_only(
        SysVarDef::new("port", Scope::Global, DataType::UInt { min: 0, max: 65535 })
            .with_doc("TCP port the server listens on."),
        || SysVarValue::UInt(detect_port() as u64),
    ));
    reg.register(SysVar::runtime_read_only(
        SysVarDef::new("datadir", Scope::Global, DataType::String { max_len: None })
            .with_doc("Server data directory."),
        || SysVarValue::String(detect_datadir()),
    ));
    reg.register(SysVar {
        validate: None,
        compiled_default: SysVarValue::String(detect_tmpdir()),
        inherit: None,
        default_provider: None,
        def: SysVarDef::new("tmpdir", Scope::Global, DataType::String { max_len: None })
            .with_access(Access::ReadOnly)
            .with_doc("Server temp directory."),
        read: Arc::new(|ctx| {
            let directory = ctx
                .db
                .filter(|db| db.is_snapshot_read_only())
                .map(|db| {
                    crate::storage::naming::system_tmp_dir(db.data_dir())
                        .display()
                        .to_string()
                })
                .unwrap_or_else(detect_tmpdir);
            SysVarValue::String(directory)
        }),
        write: None,
    });
    reg.register(SysVar::const_read_only(
        SysVarDef::new("basedir", Scope::Global, DataType::String { max_len: None }),
        SysVarValue::String("/".to_string()),
    ));
    reg.register(SysVar::const_read_only(
        SysVarDef::new(
            "pid_file",
            Scope::Global,
            DataType::String { max_len: None },
        ),
        SysVarValue::String(String::new()),
    ));
    reg.register(SysVar::const_read_only(
        SysVarDef::new("socket", Scope::Global, DataType::String { max_len: None }),
        SysVarValue::String(String::new()),
    ));
    reg.register(SysVar::runtime_read_only(
        SysVarDef::new("performance_schema", Scope::Global, DataType::Bool)
            .with_doc("Whether the Performance Schema is enabled."),
        || SysVarValue::Bool(crate::engine::perf_setup::enabled()),
    ));
    reg.register(SysVar::const_read_only(
        SysVarDef::new(
            "lower_case_table_names",
            Scope::Global,
            DataType::UInt { min: 0, max: 2 },
        )
        .with_doc("Identifier case-folding policy."),
        SysVarValue::UInt(1),
    ));
    reg.register(SysVar::const_read_only(
        SysVarDef::new("lower_case_file_system", Scope::Global, DataType::Bool)
            .with_doc("Whether the underlying filesystem is case-insensitive."),
        SysVarValue::Bool(cfg!(target_os = "macos") || cfg!(target_os = "windows")),
    ));
    // `have_*` family — used by ORMs / mysqldump to probe feature flags.
    for (name, value) in &[
        ("have_query_cache", "NO"),
        ("have_openssl", "YES"),
        ("have_ssl", "YES"),
        ("have_symlink", "DISABLED"),
        ("have_statement_timeout", "YES"),
        ("have_compress", "NO"),
        ("have_dynamic_loading", "NO"),
        ("have_geometry", "YES"),
        ("have_profiling", "NO"),
        ("have_rtree_keys", "YES"),
    ] {
        let val: &'static str = *value;
        reg.register(SysVar::const_read_only(
            SysVarDef::new(name, Scope::Global, DataType::String { max_len: None }),
            SysVarValue::String(val.to_string()),
        ));
    }
}

// ---------------------------------------------------------------------------
// Runtime probes
// ---------------------------------------------------------------------------

static HOSTNAME: OnceLock<String> = OnceLock::new();
static TMPDIR: OnceLock<String> = OnceLock::new();

fn detect_hostname() -> String {
    HOSTNAME
        .get_or_init(|| hostname_from_probe(os_hostname))
        .clone()
}

fn hostname_from_probe(probe: impl FnOnce() -> std::io::Result<String>) -> String {
    probe()
        .ok()
        .filter(|hostname| !hostname.is_empty())
        .unwrap_or_else(|| "localhost".to_owned())
}

#[cfg(unix)]
fn os_hostname() -> std::io::Result<String> {
    // MySQL limits @@hostname to 255 characters and obtains it with the same
    // operating-system call during server startup.
    let mut buffer = [0_u8; 256];
    // SAFETY: `buffer` is writable for the supplied length, and it remains
    // alive until gethostname returns.
    let result =
        unsafe { libc::gethostname(buffer.as_mut_ptr().cast::<libc::c_char>(), buffer.len()) };
    if result != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let len = buffer
        .iter()
        .position(|&byte| byte == 0)
        .unwrap_or(buffer.len());
    Ok(String::from_utf8_lossy(&buffer[..len]).into_owned())
}

#[cfg(windows)]
fn os_hostname() -> std::io::Result<String> {
    #[link(name = "kernel32")]
    extern "system" {
        fn GetComputerNameW(buffer: *mut u16, size: *mut u32) -> i32;
    }

    let mut buffer = [0_u16; 256];
    let mut len = buffer.len() as u32;
    // SAFETY: `buffer` is writable for `len` UTF-16 code units and both
    // pointers remain valid until GetComputerNameW returns.
    if unsafe { GetComputerNameW(buffer.as_mut_ptr(), &mut len) } == 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(String::from_utf16_lossy(&buffer[..len as usize]))
}

#[cfg(not(any(unix, windows)))]
fn os_hostname() -> std::io::Result<String> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "host-name lookup is unsupported on this platform",
    ))
}

fn detect_port() -> u16 {
    crate::util::env::var("MYDB_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(crate::startup_config::DEFAULT_PORT)
}

fn detect_datadir() -> String {
    crate::util::env::var("MYDB_DATA_DIR")
        .ok()
        .unwrap_or_else(|| "./data".to_string())
}

fn detect_tmpdir() -> String {
    TMPDIR
        .get_or_init(|| format!("{}/system/tmp", detect_datadir()))
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hostname_probe_value_is_used_without_environment_lookup() {
        assert_eq!(
            hostname_from_probe(|| Ok("db-host.example".to_owned())),
            "db-host.example"
        );
    }

    #[test]
    fn hostname_probe_failure_and_empty_value_fall_back_to_localhost() {
        assert_eq!(
            hostname_from_probe(|| Err(std::io::Error::other("probe failed"))),
            "localhost"
        );
        assert_eq!(hostname_from_probe(|| Ok(String::new())), "localhost");
    }

    #[test]
    fn operating_system_hostname_is_non_empty_and_bounded() {
        let hostname = os_hostname().expect("operating system should provide a host name");
        assert!(!hostname.is_empty());
        assert!(hostname.len() <= 255);
    }

    #[test]
    fn compile_platform_values_use_mysql_names() {
        if cfg!(target_os = "macos") {
            assert!(VERSION_COMPILE_OS.starts_with("macos"));
            if cfg!(target_arch = "aarch64") {
                assert_eq!(VERSION_COMPILE_MACHINE, "arm64");
            }
        } else if cfg!(target_os = "linux") {
            assert_eq!(VERSION_COMPILE_OS, "Linux");
        } else if cfg!(all(target_os = "windows", target_pointer_width = "64")) {
            assert_eq!(VERSION_COMPILE_OS, "Win64");
        }
    }
}
