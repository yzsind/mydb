use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use mydb::binlog::BinlogConfig;
use mydb::engine::{QueryLogConfig, SessionRegistry, StorageQueryEngine};
use mydb::mysql::server::{configure_open_files_limit, load_or_generate_tls_acceptor, MysqlServer};
use mydb::startup_config::{self, StartupConfig};
use mydb::util::logging::log_message;

// Default fast global allocator. The macOS/system allocator spends ~10% of
// query-thread CPU in malloc/free on the high-churn OLTP path; mimalloc's
// per-thread sharded free lists cut that sharply. The `fast-alloc` feature can
// be disabled with --no-default-features to use the platform fallback allocator.
#[cfg(feature = "fast-alloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(all(target_os = "linux", not(feature = "fast-alloc")))]
#[global_allocator]
static GLOBAL: mydb::util::mapped_alloc::MappedSystem = mydb::util::mapped_alloc::MappedSystem;

/// ASCII banner printed to the log once the engine is open and the server is
/// about to start accepting connections.
const MYDB_BANNER: &str = r#"
   ███╗   ███╗ ██╗   ██╗ ██████╗ ██████╗
   ████╗ ████║ ╚██╗ ██╔╝ ██╔══██╗ ██╔══██╗
   ██╔████╔██║  ╚████╔╝  ██║  ██║ ██████╔╝
   ██║╚██╔╝██║   ╚██╔╝   ██║  ██║ ██╔══██╗
   ██║ ╚═╝ ██║    ██║    ██████╔╝ ██████╔╝
   ╚═╝     ╚═╝    ╚═╝    ╚═════╝ ╚═════╝
   high-performance database.
"#;

fn main() -> std::io::Result<()> {
    mydb::util::system_memory::configure_allocator_page_policy();
    mydb::engine::initialize_server_status_clock();
    // Snapshot argv / exe path up front so a later `RESTART` can re-exec
    // the exact same invocation.
    mydb::mysql::shutdown::cache_launch_args();
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.first().is_some_and(|arg| arg == "--seal-worker") {
        if args.len() != 4 || args[2] != "--operation" {
            return Err(std::io::Error::other(
                "--seal-worker requires a directory and --operation ID",
            ));
        }
        return mydb::backup::native::prepare_directory_checked(
            std::path::Path::new(&args[1]),
            &args[3],
        )
        .map_err(startup_error);
    }
    let startup = match startup_config::bootstrap(&args) {
        Ok(config) => config,
        Err(err) => {
            eprintln!("mydb: configuration error: {err}");
            std::process::exit(2);
        }
    };
    if startup.help {
        print_help();
        return Ok(());
    }
    for warning in &startup.warnings {
        eprintln!("mydb: warning: {warning}");
    }
    if let Some(path) = &startup.config_path {
        eprintln!("mydb: using configuration file {}", path.display());
    }
    mydb::engine::auth::rsa_keys::initialize().map_err(|err| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("authentication RSA initialization failed: {err}"),
        )
    })?;
    // macOS schedules threads without an explicit QoS class mostly onto
    // efficiency cores under bursty load; a server's request threads
    // belong on performance cores. No-op on other platforms.
    set_thread_qos();
    // Keep each connection on a fixed request worker across socket wakes.
    // The control reactor runs on the existing main thread; request workers
    // retain the same configured count and SQL blocking-pool limits.
    let worker_threads = startup_config::var("MYDB_TOKIO_WORKERS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| {
            let n = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4);
            n.saturating_mul(4).max(16)
        });
    #[cfg(unix)]
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    // IOCP associations cannot migrate an accepted socket to another port.
    // Keep the shared reactor on platforms without Unix re-registration.
    #[cfg(not(unix))]
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .on_thread_start(set_thread_qos)
        .build()?;
    #[cfg(unix)]
    mydb::util::connection_dispatch::initialize(worker_threads, set_thread_qos)?;
    let result = runtime.block_on(async_main(startup));
    #[cfg(unix)]
    mydb::util::connection_dispatch::shutdown();
    result
}

#[cfg(target_os = "macos")]
fn set_thread_qos() {
    // Opt-in only: measured neutral-to-negative for OLTP throughput on
    // M-series (all workers then contend for the few P cores), so the
    // default leaves the scheduler alone.
    let class = match startup_config::var("MYDB_THREAD_QOS").as_deref() {
        Ok("interactive") => libc::qos_class_t::QOS_CLASS_USER_INTERACTIVE,
        Ok("initiated") => libc::qos_class_t::QOS_CLASS_USER_INITIATED,
        _ => return,
    };
    unsafe {
        libc::pthread_set_qos_class_self_np(class, 0);
    }
}

#[cfg(not(target_os = "macos"))]
fn set_thread_qos() {}

fn startup_error(err: mydb::error::QueryError) -> std::io::Error {
    let message = match err.diagnostic_detail() {
        Some(detail) => format!("{}: {detail}", err.message()),
        None => err.message().to_string(),
    };
    std::io::Error::other(message)
}

async fn async_main(startup: StartupConfig) -> std::io::Result<()> {
    let config = Config::from_startup();
    configure_open_files_limit();
    let addr = SocketAddr::new(config.host, config.port);
    let log_config = QueryLogConfig::with_slow_sql_threshold_ms(config.slow_sql_ms);
    let registry = Arc::new(SessionRegistry::new());
    let repository = startup_config::var_os("MYDB_BACKUP_REPOSITORY").map(PathBuf::from);
    let base = startup_config::var("MYDB_BACKUP_BASE").ok();
    let runtime = startup_config::var_os("MYDB_BACKUP_RUNTIME_DIR").map(PathBuf::from);
    let direct = repository.is_some() || base.is_some() || runtime.is_some();
    let reader = if direct {
        let repository = repository.as_ref().ok_or_else(|| {
            std::io::Error::other("--backup-repository is required with --base/--runtime-dir")
        })?;
        let base = base.as_ref().ok_or_else(|| {
            std::io::Error::other("direct image query requires an explicit --base")
        })?;
        let runtime = runtime.as_ref().ok_or_else(|| {
            std::io::Error::other(
                "direct image query requires --runtime-dir outside the repository",
            )
        })?;
        if startup_config::var_os("MYDB_DATA_DIR").is_some() {
            return Err(std::io::Error::other(
                "--data-dir cannot be combined with direct image query",
            ));
        }
        Some(mydb::backup::open_base(repository, base, runtime).map_err(startup_error)?)
    } else {
        None
    };
    let data_dir = reader
        .as_ref()
        .map(|reader| reader.data_dir.clone())
        .unwrap_or_else(|| config.data_dir.clone());
    // Physical-replica bootstrap: a fresh (or resync-flagged) replica
    // pulls a streaming base backup from its master BEFORE the engine
    // opens, so recovery starts from a consistent snapshot.
    let snapshot = mydb::backup::native::is_snapshot(&data_dir).map_err(startup_error)?;
    let _startup_retention_guard = if !snapshot {
        // Reject corrupt role metadata before recovery can mistake a physical
        // replica for a standalone database and undo its in-flight source work.
        mydb::repl::role::load(&data_dir)?;
        mydb::repl::maybe_bootstrap_replica(&data_dir)
            .await
            .map_err(startup_error)?;
        Some(mydb::backup::prepare_archive_retention(&data_dir).map_err(startup_error)?)
    } else {
        None
    };
    let engine = if let Some(runtime) = runtime {
        StorageQueryEngine::open_backup_image(
            data_dir.clone(),
            runtime,
            log_config,
            Arc::clone(&registry),
            startup.sysvars,
        )
    } else {
        StorageQueryEngine::open_full_with_startup_options(
            data_dir.clone(),
            log_config,
            Arc::clone(&registry),
            startup.sysvars,
        )
    }
    .map_err(startup_error)?;
    let engine = Arc::new(engine);
    if !engine.db_handle().is_snapshot_read_only() {
        mydb::backup::init_background_archiver(Arc::clone(engine.db_handle()))
            .map_err(startup_error)?;
        let binlog_started = std::time::Instant::now();
        mydb::binlog::init_global(data_dir.clone(), config.binlog.clone())
            .map_err(startup_error)?;
        log_message(format_args!(
            "mydb startup: binlog_init={}ms",
            binlog_started.elapsed().as_millis()
        ));
        let repl = mydb::replication::init_global(data_dir.clone(), Arc::clone(&engine));
        mydb::replication::maybe_start_from_env(&repl).map_err(startup_error)?;
        let _mydb_repl =
            mydb::repl::init_global(data_dir, Arc::clone(&engine)).map_err(startup_error)?;
    }
    let tls_acceptor = match (config.ssl_mode, &config.ssl_cert, &config.ssl_key) {
        (SslMode::Disabled, Some(_), _) | (SslMode::Disabled, _, Some(_)) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "--ssl-mode disabled cannot be combined with --ssl-cert/--ssl-key",
            ))
        }
        (SslMode::Disabled, None, None) => None,
        (SslMode::Auto, Some(cert), Some(key)) => load_or_generate_tls_acceptor(Some((cert, key)))?,
        (SslMode::Auto, Some(_), None) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "--ssl-cert requires --ssl-key or MYDB_SSL_KEY",
            ))
        }
        (SslMode::Auto, None, Some(_)) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "--ssl-key requires --ssl-cert or MYDB_SSL_CERT",
            ))
        }
        (SslMode::Auto, None, None) => load_or_generate_tls_acceptor(None)?,
    };
    let ssl_status = match (config.ssl_mode, config.ssl_cert.is_some()) {
        (SslMode::Disabled, _) => "disabled",
        (SslMode::Auto, true) => "enabled (configured certificate)",
        (SslMode::Auto, false) => "enabled (ephemeral self-signed certificate)",
    };
    let server = MysqlServer::with_tls(addr, engine, tls_acceptor, registry);

    log_message(format_args!("\n{MYDB_BANNER}"));
    log_message(format_args!(
        "mydb listening on {addr}; slow SQL threshold={}ms; ssl={ssl_status}; log_bin={}",
        config.slow_sql_ms,
        if config.binlog.enabled { "ON" } else { "OFF" },
    ));
    server.run().await
}

#[derive(Debug, Clone)]
struct Config {
    host: IpAddr,
    port: u16,
    data_dir: PathBuf,
    slow_sql_ms: u64,
    ssl_cert: Option<PathBuf>,
    ssl_key: Option<PathBuf>,
    ssl_mode: SslMode,
    binlog: BinlogConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SslMode {
    Auto,
    Disabled,
}

impl SslMode {
    fn parse(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "auto" | "on" | "enabled" | "enable" => Self::Auto,
            "disabled" | "disable" | "off" | "0" | "false" => Self::Disabled,
            other => panic!("invalid ssl mode: {other}"),
        }
    }
}

impl Config {
    fn from_startup() -> Self {
        let host = startup_config::var("MYDB_HOST")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        let port = startup_config::var("MYDB_PORT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(startup_config::DEFAULT_PORT);
        let data_dir = startup_config::var("MYDB_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./data"));
        let slow_sql_ms = startup_config::var("MYDB_SLOW_SQL_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(QueryLogConfig::DEFAULT_SLOW_SQL_MS);
        let ssl_cert = startup_config::var("MYDB_SSL_CERT").ok().map(PathBuf::from);
        let ssl_key = startup_config::var("MYDB_SSL_KEY").ok().map(PathBuf::from);
        let ssl_mode = startup_config::var("MYDB_SSL_MODE")
            .ok()
            .map(|value| SslMode::parse(&value))
            .unwrap_or(SslMode::Auto);
        let binlog = mydb::binlog::config_from_env_defaults();

        Self {
            host,
            port,
            data_dir,
            slow_sql_ms,
            ssl_cert,
            ssl_key,
            ssl_mode,
            binlog: binlog.normalize(),
        }
    }
}

fn print_help() {
    println!(
        "Usage: mydb [OPTIONS]\n\
         \nStartup configuration:\n\
         \x20 --config PATH                  Read one explicit MySQL-style option file\n\
         \x20 --host ADDRESS                 Listen address (default 0.0.0.0)\n\
         \x20 --port PORT                    MySQL protocol port (default {default_port})\n\
         \x20 --data-dir PATH                Data directory (default ./data)\n\
         \x20 --init-connect SQL             SQL initialization for new client connections\n\
         \x20 --slow-sql-ms MS               Legacy slow SQL threshold\n\
         \x20 --ssl-mode auto|disabled       TLS advertisement mode\n\
         \x20 --ssl-cert PATH --ssl-key PATH Configured TLS certificate and key\n\
         \nHTTP SQL calls:\n\
         \x20 --http-enabled ON|OFF          Enable HTTP SQL calls (default ON)\n\
         \x20 --http-local-config PATH       Optional node egress/CA/account policy\n\
         \x20 --http-max-inflight N          Maximum concurrent requests (default 32)\n\
         \x20 --http-max-queue N             Maximum queued requests (default 128)\n\
         \x20 --http-memory-limit-mb N       HTTP memory ceiling (default 128 MiB)\n\
         \nImmutable backup query:\n\
         \x20 --backup-repository PATH       Unified local repository (requires --base)\n\
         \x20 --base ID --runtime-dir PATH   Query a sealed baseline; runtime stays external\n\
         \nLogging and binlog:\n\
         \x20 --general-log --slow-query-log Enable query logs\n\
         \x20 --log-output VALUE             FILE|TABLE|FILE,TABLE|NONE\n\
         \x20 --log-bin --server-id N        Enable row binlog source\n\
         \x20 --binlog-checksum CRC32|NONE   Binlog checksum\n\
         \x20 --sync-binlog N                Binlog fsync interval\n\
         \nOptions accept both --name VALUE and --name=VALUE. Configuration files are\n\
         read only when --config is specified; [mydb] overrides [mysqld].\n\
         \x20 -h, --help                      Print this help",
        default_port = startup_config::DEFAULT_PORT,
    );
}
