//! Connection-level variables — timeouts, buffer sizes, max_connections.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::OnceLock;

use crate::engine::api::SessionContext;
use crate::engine::sysvar::backend::SysVar;
use crate::engine::sysvar::ctx::WriteCtx;
use crate::engine::sysvar::meta::{Access, DataType, Scope, SysVarDef};
use crate::engine::sysvar::registry::Registry;
use crate::engine::sysvar::value::SysVarValue;
use crate::types::QueryError;

// ---------------------------------------------------------------------------
// Atomics backing the per-process globals. Each is initialised from its
// MYDB_*-named env var on first read so the values stay in sync with the
// process configuration.
// ---------------------------------------------------------------------------

static WAIT_TIMEOUT: AtomicU64 = AtomicU64::new(28800);
static INTERACTIVE_TIMEOUT: AtomicU64 = AtomicU64::new(28800);
static NET_READ_TIMEOUT: AtomicU64 = AtomicU64::new(30);
static NET_WRITE_TIMEOUT: AtomicU64 = AtomicU64::new(60);
static CONNECT_TIMEOUT: AtomicU64 = AtomicU64::new(10);
const DEFAULT_MAX_ALLOWED_PACKET: u64 = 67_108_864;
static MAX_ALLOWED_PACKET: AtomicU64 = AtomicU64::new(DEFAULT_MAX_ALLOWED_PACKET);
static NET_BUFFER_LENGTH: AtomicU64 = AtomicU64::new(16_384);
static MAX_CONNECTIONS: AtomicU64 = AtomicU64::new(1000);
static BACK_LOG: AtomicU64 = AtomicU64::new(4096);
static THREAD_STACK: AtomicU64 = AtomicU64::new(286_720);
static THREAD_CACHE_SIZE: AtomicU64 = AtomicU64::new(9);
static OPEN_FILES_LIMIT: AtomicU64 = AtomicU64::new(65535);
static TABLE_OPEN_CACHE: AtomicU64 = AtomicU64::new(4000);
static TABLE_DEFINITION_CACHE: AtomicU64 = AtomicU64::new(2000);
static MAX_USER_CONNECTIONS: AtomicU64 = AtomicU64::new(0);
static SESSION_TRACK_SCHEMA: AtomicBool = AtomicBool::new(true);
static SESSION_TRACK_STATE_CHANGE: AtomicBool = AtomicBool::new(false);

/// Seed the per-process global atomics from their `MYDB_*` env vars
/// exactly once, before any layer reads them. Historically each
/// connection layer read its own `MYDB_*` env via a private `OnceLock`,
/// so the sysvar atomics (and therefore `SHOW GLOBAL VARIABLES` /
/// `@@global.x`) drifted from the values the server actually enforced.
/// Routing both the enforcement layers and the registry through these
/// atomics — seeded here — collapses that into a single source of truth
/// while keeping `SET GLOBAL x = N` live (it stores straight into the
/// same atomic).
fn ensure_env_seeded() {
    static SEEDED: OnceLock<()> = OnceLock::new();
    SEEDED.get_or_init(|| {
        seed_atomic_from_env(&CONNECT_TIMEOUT, "MYDB_HANDSHAKE_TIMEOUT", 0, 31_536_000);
        seed_atomic_from_env(&WAIT_TIMEOUT, "MYDB_WAIT_TIMEOUT", 1, 31_536_000);
        seed_atomic_from_env(
            &INTERACTIVE_TIMEOUT,
            "MYDB_INTERACTIVE_TIMEOUT",
            1,
            31_536_000,
        );
        seed_atomic_from_env(&NET_READ_TIMEOUT, "MYDB_NET_READ_TIMEOUT", 1, 31_536_000);
        seed_atomic_from_env(&NET_WRITE_TIMEOUT, "MYDB_NET_WRITE_TIMEOUT", 0, 31_536_000);
        seed_atomic_from_env(&MAX_CONNECTIONS, "MYDB_MAX_CONNECTIONS", 1, 1_000_000);
        // The wire codec (`mysql::packet`) enforces this exact atomic, so
        // seeding it from `MYDB_MAX_ALLOWED_PACKET` here is what keeps the
        // startup env, `SHOW VARIABLES`, and the enforced inbound limit in
        // agreement. Clamp bounds match the registered sysvar [1 KiB, 1 GiB].
        seed_atomic_from_env(
            &MAX_ALLOWED_PACKET,
            "MYDB_MAX_ALLOWED_PACKET",
            1024,
            1024 * 1024 * 1024,
        );
        if let Ok(sql) = crate::util::env::var("MYDB_INIT_CONNECT") {
            init_connect_set(sql);
        }
    });
}

fn seed_atomic_from_env(cell: &AtomicU64, var: &str, min: u64, max: u64) {
    if let Ok(raw) = crate::util::env::var(var) {
        if let Ok(parsed) = raw.trim().parse::<u64>() {
            // `net_write_timeout` accepts 0 (disabled); the rest clamp up
            // to their documented minimum so a bogus env can't push a
            // sub-second idle/connection timeout.
            let clamped = parsed.clamp(min, max);
            cell.store(clamped, Ordering::Relaxed);
        }
    }
}

/// Public accessors so the connection-handling layers can honour these
/// settings without going through the registry every request. Each
/// triggers the one-time env seed so the value reflects `MYDB_*` config
/// regardless of which layer reads first at startup.
pub fn connect_timeout_secs() -> u64 {
    ensure_env_seeded();
    CONNECT_TIMEOUT.load(Ordering::Relaxed)
}

pub fn wait_timeout_secs() -> u64 {
    ensure_env_seeded();
    WAIT_TIMEOUT.load(Ordering::Relaxed)
}
pub fn interactive_timeout_secs() -> u64 {
    ensure_env_seeded();
    INTERACTIVE_TIMEOUT.load(Ordering::Relaxed)
}
pub fn net_read_timeout_secs() -> u64 {
    ensure_env_seeded();
    NET_READ_TIMEOUT.load(Ordering::Relaxed)
}
pub fn net_write_timeout_secs() -> u64 {
    ensure_env_seeded();
    NET_WRITE_TIMEOUT.load(Ordering::Relaxed)
}
pub fn max_allowed_packet_bytes() -> u64 {
    ensure_env_seeded();
    MAX_ALLOWED_PACKET.load(Ordering::Relaxed)
}

#[cfg(test)]
pub(crate) fn reset_max_allowed_packet_for_mtr() {
    ensure_env_seeded();
    MAX_ALLOWED_PACKET.store(DEFAULT_MAX_ALLOWED_PACKET, Ordering::Relaxed);
}
pub fn max_connections() -> u64 {
    ensure_env_seeded();
    MAX_CONNECTIONS.load(Ordering::Relaxed)
}

pub(crate) fn register(reg: &mut Registry) {
    // Seed the globals from `MYDB_*` env before the first read so
    // `SHOW GLOBAL VARIABLES` and the enforcement layers agree.
    ensure_env_seeded();
    // `wait_timeout` / `interactive_timeout` are genuinely session-scoped:
    // the idle-wait enforcement reads the issuing session's value, so a
    // `SET SESSION wait_timeout = N` only shortens that one connection's
    // next idle slice (matching MySQL). The session field is initialised
    // from the appropriate global at connect (interactive clients copy
    // `interactive_timeout`).
    register_session_atomic_uint(
        reg,
        "wait_timeout",
        &WAIT_TIMEOUT,
        28800,
        1,
        31_536_000,
        "Idle connection timeout in seconds.",
        |s| s.sysvars.wait_timeout,
        |s, n| s.sysvars.wait_timeout = n,
    );
    register_session_atomic_uint(
        reg,
        "interactive_timeout",
        &INTERACTIVE_TIMEOUT,
        28800,
        1,
        31_536_000,
        "Idle timeout in seconds for interactive clients.",
        |s| s.sysvars.interactive_timeout,
        |s, n| s.sysvars.interactive_timeout = n,
    );
    // `net_read_timeout` / `net_write_timeout`: the SESSION value is
    // stored and faithfully echoed by `@@session.x` (so `SET SESSION`
    // no longer clobbers the global the way the old shared-atomic
    // backend did), but on-wire enforcement currently honours the
    // GLOBAL value — per-session packet timeouts are advisory, in line
    // with mydb's other advisory connection knobs. `SET GLOBAL` is live.
    register_session_atomic_uint(
        reg,
        "net_read_timeout",
        &NET_READ_TIMEOUT,
        30,
        1,
        31_536_000,
        "Read timeout (seconds) for a single network packet (GLOBAL enforced).",
        |s| s.sysvars.net_read_timeout,
        |s, n| s.sysvars.net_read_timeout = n,
    );
    register_session_atomic_uint(
        reg,
        "net_write_timeout",
        &NET_WRITE_TIMEOUT,
        60,
        0,
        31_536_000,
        "Write timeout (seconds) for a single network packet (GLOBAL enforced).",
        |s| s.sysvars.net_write_timeout,
        |s, n| s.sysvars.net_write_timeout = n,
    );
    register_atomic_uint(
        reg,
        "connect_timeout",
        Scope::Global,
        &CONNECT_TIMEOUT,
        2,
        31_536_000,
        "Seconds the server waits for a handshake packet.",
    );
    reg.register(SysVar::settable(
        SysVarDef::new(
            "max_allowed_packet",
            Scope::Both,
            DataType::UInt {
                min: 1024,
                max: 1024 * 1024 * 1024,
            },
        )
        .with_access(Access::ReadWrite)
        .with_scope_access(Access::ReadOnly, Access::ReadWrite)
        .with_doc("Maximum size of one packet (compressed)."),
        |ctx| {
            SysVarValue::UInt(match (ctx.scope, ctx.session) {
                (Scope::Session, Some(s)) => s.sysvars.max_allowed_packet,
                _ => MAX_ALLOWED_PACKET.load(Ordering::Relaxed),
            })
        },
        |_ctx: &mut WriteCtx<'_>, value: SysVarValue| -> Result<(), QueryError> {
            let bytes = value.as_u64().unwrap_or(0);
            MAX_ALLOWED_PACKET.store(bytes, Ordering::Relaxed);
            if bytes < NET_BUFFER_LENGTH.load(Ordering::Relaxed) {
                crate::engine::api::push_statement_warning(
                    crate::error::ConditionLevel::Warning,
                    crate::error::catalog::WARN_OPTION_BELOW_LIMIT,
                    "The value of 'max_allowed_packet' should be no less than the value of 'net_buffer_length'",
                );
            }
            Ok(())
        },
    ).with_default(SysVarValue::UInt(DEFAULT_MAX_ALLOWED_PACKET)));
    register_atomic_uint(
        reg,
        "net_buffer_length",
        Scope::Both,
        &NET_BUFFER_LENGTH,
        1024,
        16_777_216,
        "Initial packet buffer size — also a hint to client batchers.",
    );
    register_atomic_uint(
        reg,
        "max_connections",
        Scope::Global,
        &MAX_CONNECTIONS,
        1,
        1_000_000,
        "Maximum number of concurrent ordinary client connections; ten additional slots are reserved for CONNECTION_ADMIN accounts.",
    );
    register_atomic_uint(
        reg,
        "back_log",
        Scope::Global,
        &BACK_LOG,
        1,
        65535,
        "Listener backlog passed to the OS.",
    );
    register_atomic_uint(
        reg,
        "thread_stack",
        Scope::Global,
        &THREAD_STACK,
        131_072,
        1_073_741_824,
        "Stack size per server thread (advisory in mydb).",
    );
    register_atomic_uint(
        reg,
        "thread_cache_size",
        Scope::Global,
        &THREAD_CACHE_SIZE,
        0,
        16_384,
        "Threads kept warm between client connections.",
    );
    register_atomic_uint(
        reg,
        "open_files_limit",
        Scope::Global,
        &OPEN_FILES_LIMIT,
        0,
        4_294_967_295,
        "Maximum file descriptors the server may open.",
    );
    register_atomic_uint(
        reg,
        "table_open_cache",
        Scope::Global,
        &TABLE_OPEN_CACHE,
        1,
        524_288,
        "Open table-handle cache (advisory in mydb).",
    );
    register_atomic_uint(
        reg,
        "table_definition_cache",
        Scope::Global,
        &TABLE_DEFINITION_CACHE,
        400,
        524_288,
        "Table definition cache (advisory in mydb).",
    );
    register_atomic_uint(
        reg,
        "max_user_connections",
        Scope::Both,
        &MAX_USER_CONNECTIONS,
        0,
        100_000,
        "Per-user connection cap; 0 = unlimited.",
    );

    // `session_track_system_variables` — comma-separated subscription
    // list (or `*`) of variables whose changes are reported back to the
    // client via the OK packet's SESSION_TRACK_SYSTEM_VARIABLES block.
    // GLOBAL is the default for new connections; SESSION overrides it for
    // the issuing connection. Default matches MySQL 8.0.45/8.0.46.
    reg.register(SysVar::settable(
        SysVarDef::new(
            "session_track_system_variables",
            Scope::Both,
            DataType::String { max_len: None },
        )
        .with_doc("Variables whose changes are tracked in the OK packet."),
        |ctx| match (ctx.scope, ctx.session) {
            (Scope::Session, Some(s)) => {
                SysVarValue::String(s.session_track_system_variables.clone())
            }
            _ => SysVarValue::String(session_track_system_variables_global()),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let value = v.to_display_string();
            if ctx.is_global() {
                set_session_track_system_variables_global(value);
            } else {
                ctx.session.session_track_system_variables = value;
            }
            Ok(())
        },
    ));

    type SessionBoolGetter = fn(&SessionContext) -> bool;
    let tracking_bools: [(&'static str, &'static AtomicBool, SessionBoolGetter); 2] = [
        (
            "session_track_schema",
            &SESSION_TRACK_SCHEMA,
            |session: &SessionContext| session.session_track_schema,
        ),
        (
            "session_track_state_change",
            &SESSION_TRACK_STATE_CHANGE,
            |session: &SessionContext| session.session_track_state_change,
        ),
    ];
    for (name, global, field) in tracking_bools {
        reg.register(SysVar::settable(
            SysVarDef::new(name, Scope::Both, DataType::Bool)
                .with_doc("Controls CLIENT_SESSION_TRACK state notifications."),
            move |ctx| {
                SysVarValue::Bool(match (ctx.scope, ctx.session) {
                    (Scope::Session, Some(session)) => field(session),
                    _ => global.load(Ordering::Relaxed),
                })
            },
            move |ctx: &mut WriteCtx<'_>, value: SysVarValue| -> Result<(), QueryError> {
                let enabled = value.as_bool().unwrap_or(false);
                if ctx.is_global() {
                    global.store(enabled, Ordering::Relaxed);
                } else if name == "session_track_schema" {
                    ctx.session.session_track_schema = enabled;
                } else {
                    ctx.session.session_track_state_change = enabled;
                }
                Ok(())
            },
        ));
    }

    for name in ["session_track_gtids", "session_track_transaction_info"] {
        reg.register(SysVar::settable(
            SysVarDef::new(name, Scope::Session, DataType::Enum { values: &["OFF"] })
                .with_doc("Only OFF is supported until the corresponding producer is implemented."),
            |_ctx| SysVarValue::String("OFF".to_string()),
            |_ctx: &mut WriteCtx<'_>, _value: SysVarValue| -> Result<(), QueryError> { Ok(()) },
        ));
    }

    reg.register(SysVar::settable(
        SysVarDef::new(
            "init_connect",
            Scope::Global,
            DataType::StrictString { max_len: None, null_as_empty: true },
        )
        .with_doc(
            "SQL executed for new client connections, except administrators and expired-password accounts.",
        ),
        |_ctx| SysVarValue::String(init_connect_get()),
        |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            init_connect_set(match v {
                SysVarValue::Null => String::new(),
                value => value.to_display_string(),
            });
            Ok(())
        },
    ).with_default(SysVarValue::String(String::new())));
    reg.register(SysVar::const_read_only(
        SysVarDef::new(
            "init_file",
            Scope::Global,
            DataType::String { max_len: None },
        ),
        SysVarValue::String(String::new()),
    ));
}

// ---------------------------------------------------------------------------
// init_connect storage — separate from the AtomicU64 family.
// ---------------------------------------------------------------------------

use std::sync::RwLock;
// None is the disabled fast path. A connection keeps an immutable snapshot
// without holding a lock across SQL execution (which may change this variable).
static INIT_CONNECT: arc_swap::ArcSwapOption<String> = arc_swap::ArcSwapOption::const_empty();
static SESSION_TRACK_SYSTEM_VARIABLES: RwLock<Option<String>> = RwLock::new(None);
const DEFAULT_SESSION_TRACK_SYSTEM_VARIABLES: &str =
    "time_zone,autocommit,character_set_client,character_set_results,character_set_connection";

fn session_track_system_variables_global() -> String {
    SESSION_TRACK_SYSTEM_VARIABLES
        .read()
        .ok()
        .and_then(|s| s.clone())
        .unwrap_or_else(|| DEFAULT_SESSION_TRACK_SYSTEM_VARIABLES.to_string())
}

fn set_session_track_system_variables_global(value: String) {
    if let Ok(mut guard) = SESSION_TRACK_SYSTEM_VARIABLES.write() {
        *guard = Some(value);
    }
}

/// The global default copied into a new session's subscription at
/// connect.
pub fn session_track_system_variables_default() -> String {
    session_track_system_variables_global()
}

pub fn session_track_schema_default() -> bool {
    SESSION_TRACK_SCHEMA.load(Ordering::Relaxed)
}

pub fn session_track_state_change_default() -> bool {
    SESSION_TRACK_STATE_CHANGE.load(Ordering::Relaxed)
}

fn init_connect_get() -> String {
    init_connect_sql()
        .map(|sql| (*sql).clone())
        .unwrap_or_default()
}

fn init_connect_set(value: String) {
    INIT_CONNECT.store((!value.is_empty()).then(|| std::sync::Arc::new(value)));
}

pub(crate) fn init_connect_sql() -> Option<std::sync::Arc<String>> {
    ensure_env_seeded();
    INIT_CONNECT.load_full()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A `Scope::Both` unsigned variable whose GLOBAL value lives in a
/// process atomic and whose SESSION value lives in a `SessionContext`
/// field. Reads/writes honour `ctx.scope` so `SET SESSION x` no longer
/// leaks into the global atomic, and `@@session.x` returns the
/// per-session value.
fn register_session_atomic_uint(
    reg: &mut Registry,
    name: &'static str,
    cell: &'static AtomicU64,
    default: u64,
    min: u64,
    max: u64,
    doc: &'static str,
    read_session: fn(&SessionContext) -> u64,
    write_session: fn(&mut SessionContext, u64),
) {
    reg.register(
        SysVar::settable(
            SysVarDef::new(name, Scope::Both, DataType::UInt { min, max }).with_doc(doc),
            move |ctx| match (ctx.scope, ctx.session) {
                (Scope::Session, Some(s)) => SysVarValue::UInt(read_session(s)),
                _ => SysVarValue::UInt(cell.load(Ordering::Relaxed)),
            },
            move |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
                let n = v.as_u64().unwrap_or(0);
                // The packet layer still accepts zero from its startup
                // environment as a disabled timeout, but SQL follows the
                // MySQL range [1, ...]. Registry-only configuration probes
                // without a database context retain the underlying zero
                // value for compatibility with that environment contract.
                if name == "net_write_timeout" && n == 0 && ctx.db.is_some() {
                    return Err(crate::engine::sysvar::errors::wrong_value_for_var(
                        "net_write_timeout",
                        "0",
                    ));
                }
                // GLOBAL write updates the process default for future
                // connections (and the value `SHOW GLOBAL VARIABLES` reports);
                // existing sessions keep their copy, matching MySQL. SESSION
                // write touches only the issuing session's field.
                if ctx.is_global() {
                    cell.store(n, Ordering::Relaxed);
                } else {
                    write_session(ctx.session, n);
                }
                Ok(())
            },
        )
        .with_validation(move |ctx, value| {
            if name == "net_write_timeout" && value.as_u64() == Some(0) && ctx.db.is_some() {
                return Err(crate::engine::sysvar::errors::wrong_value_for_var(
                    name, "0",
                ));
            }
            Ok(())
        })
        .with_default(SysVarValue::UInt(default)),
    );
}

fn register_atomic_uint(
    reg: &mut Registry,
    name: &'static str,
    scope: Scope,
    cell: &'static AtomicU64,
    min: u64,
    max: u64,
    doc: &'static str,
) {
    reg.register(
        SysVar::settable(
            SysVarDef::new(name, scope, DataType::UInt { min, max })
                .with_scope_access(
                    if scope == Scope::Both {
                        Access::ReadOnly
                    } else {
                        Access::ReadWrite
                    },
                    Access::ReadWrite,
                )
                .with_doc(doc),
            move |ctx| {
                SysVarValue::UInt(match (name, ctx.scope, ctx.session) {
                    ("net_buffer_length", Scope::Session, Some(s)) => s.sysvars.net_buffer_length,
                    ("max_user_connections", Scope::Session, Some(s)) => {
                        s.sysvars.max_user_connections
                    }
                    _ => cell.load(Ordering::Relaxed),
                })
            },
            move |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
                let n = v.as_u64().unwrap_or(0);
                cell.store(n, Ordering::Relaxed);
                Ok(())
            },
        )
        .with_default(SysVarValue::UInt(if name == "max_connections" {
            1000
        } else {
            cell.load(Ordering::Relaxed)
        })),
    );
}

pub(crate) fn net_buffer_length() -> u64 {
    NET_BUFFER_LENGTH.load(Ordering::Relaxed)
}
pub(crate) fn max_user_connections() -> u64 {
    MAX_USER_CONNECTIONS.load(Ordering::Relaxed)
}
