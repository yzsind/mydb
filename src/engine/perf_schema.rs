//! `performance_schema` table registry, column definitions, and row producers.
//!
//! performance_schema reuses the system-table machinery in
//! [`crate::engine::information_schema`] — the shared [`SystemColumn`] /
//! [`SystemTableDef`] types and the SELECT path that reads
//! `columns_for_table_name` / `rows_for_table_name`. This module owns
//! everything PERFORMANCE_SCHEMA-specific so the (already large)
//! information_schema module does not balloon: column definitions, the
//! generated MySQL 8.0.46 table list, per-table row producers, and (in
//! later phases) writable setup state, DML / TRUNCATE handlers, and the
//! statement-digest collector.
//!
//! Honesty rules (per CLAUDE.md): a table backed by a real data source is
//! populated from it; every other *registered* table returns an empty result
//! set rather than fabricated rows. Unsupported operations error explicitly.

use crate::engine::information_schema::{SystemColumn, SystemTableDef};
use crate::engine::session_registry::SessionRegistry;
use crate::storage::{error, Database};
use crate::types::{ColumnType, QueryError, Value};

// Static column definitions for optional extensions only.

macro_rules! i64c {
    ($name:literal) => {
        SystemColumn::new($name, ColumnType::LongLong, "bigint", "bigint", true)
            .numeric(Some(19), Some(0))
    };
}

macro_rules! text {
    ($name:literal) => {
        SystemColumn::new($name, ColumnType::VarString, "text", "text", true)
            .character_lengths(Some(65535), Some(262140))
    };
}

macro_rules! f64c {
    ($name:literal) => {
        SystemColumn::new($name, ColumnType::Double, "double", "double", true)
            .numeric(Some(22), None)
    };
}

macro_rules! u64c {
    ($name:literal) => {
        SystemColumn::new(
            $name,
            ColumnType::LongLong,
            "bigint",
            "bigint unsigned",
            false,
        )
        .numeric(Some(20), Some(0))
    };
}

macro_rules! u64cn {
    ($name:literal) => {
        SystemColumn {
            nullable: true,
            ..u64c!($name)
        }
    };
}

macro_rules! i32sc {
    ($name:literal) => {
        SystemColumn::new($name, ColumnType::Long, "int", "int", false).numeric(Some(10), Some(0))
    };
}

macro_rules! charn {
    ($name:literal, $len:literal) => {
        SystemColumn::new(
            $name,
            ColumnType::VarString,
            "varchar",
            concat!("varchar(", stringify!($len), ")"),
            false,
        )
        .character_lengths(Some($len), Some($len * 4))
    };
}

macro_rules! charnn {
    ($name:literal, $len:literal) => {
        SystemColumn {
            nullable: true,
            ..charn!($name, $len)
        }
    };
}

macro_rules! enm {
    ($name:literal, $sql:literal) => {
        SystemColumn::new($name, ColumnType::VarString, "enum", $sql, false)
            .character_lengths(Some(64), Some(256))
    };
}

macro_rules! enmn {
    ($name:literal, $sql:literal) => {
        SystemColumn {
            nullable: true,
            ..enm!($name, $sql)
        }
    };
}

macro_rules! ltext {
    ($name:literal) => {
        SystemColumn::new($name, ColumnType::VarString, "longtext", "longtext", true)
            .character_lengths(Some(4294967295), Some(4294967295))
    };
}

const fn perf(name: &'static str, columns: &'static [SystemColumn]) -> SystemTableDef {
    SystemTableDef {
        schema: "performance_schema",
        name,
        table_type: "BASE TABLE",
        columns,
        engine: "PERFORMANCE_SCHEMA",
        indexes: &[],
        create_sql: None,
        hidden: false,
    }
}

const REPLICATION_GROUP_MEMBER_ACTIONS_COLUMNS: &[SystemColumn] = &[
    charn!("NAME", 64),
    charn!("EVENT", 64),
    {
        SystemColumn {
            column_type_sql: "int unsigned",
            ..i32sc!("ENABLED")
        }
    },
    charn!("TYPE", 64),
    {
        SystemColumn {
            column_type_sql: "int unsigned",
            ..i32sc!("PRIORITY")
        }
    },
    charn!("ERROR_HANDLING", 64),
];

const REPLICATION_GROUP_CONFIGURATION_VERSION_COLUMNS: &[SystemColumn] =
    &[charn!("NAME", 64), u64c!("VERSION")];

const REPLICATION_GROUP_COMMUNICATION_INFORMATION_COLUMNS: &[SystemColumn] = &[
    u64c!("WRITE_CONCURRENCY"),
    charn!("PROTOCOL_VERSION", 64),
    text!("WRITE_CONSENSUS_LEADERS_PREFERRED"),
    text!("WRITE_CONSENSUS_LEADERS_ACTUAL"),
    enm!("WRITE_CONSENSUS_SINGLE_LEADER_CAPABLE", "enum('1','0')"),
];

const CLONE_STATUS_COLUMNS: &[SystemColumn] = &[
    i32sc!("ID"),
    i32sc!("PID"),
    charn!("STATE", 16),
    charn!("BEGIN_TIME", 32),
    charn!("END_TIME", 32),
    charn!("SOURCE", 256),
    charn!("DESTINATION", 256),
    i32sc!("ERROR_NO"),
    charn!("ERROR_MESSAGE", 512),
    charn!("BINLOG_FILE", 512),
    u64c!("BINLOG_POSITION"),
    charn!("GTID_EXECUTED", 4096),
];

const CLONE_PROGRESS_COLUMNS: &[SystemColumn] = &[
    i32sc!("ID"),
    charn!("STAGE", 32),
    charn!("STATE", 16),
    charn!("BEGIN_TIME", 32),
    charn!("END_TIME", 32),
    f64c!("THREADS"),
    f64c!("ESTIMATE"),
    f64c!("DATA"),
    f64c!("NETWORK"),
    i32sc!("DATA_SPEED"),
    i32sc!("NETWORK_SPEED"),
];

const COMPONENT_SCHEDULER_TASKS_COLUMNS: &[SystemColumn] = &[
    charn!("NAME", 64),
    charnn!("STATUS", 64),
    charnn!("COMMENT", 1024),
    i64c!("NEXT_RUN_SECONDS"),
];

const PROCESSLIST_PFS_FIREWALL_GROUPS_COLUMNS: &[SystemColumn] =
    &[charnn!("NAME", 288), charnn!("MODE", 12)];

const FIREWALL_GROUP_ALLOWLIST_COLUMNS: &[SystemColumn] = &[charnn!("NAME", 288), ltext!("RULE")];

const FIREWALL_MEMBERSHIP_COLUMNS: &[SystemColumn] =
    &[charnn!("GROUP_ID", 288), charnn!("MEMBER_ID", 288)];

const TP_THREAD_GROUP_STATE_COLUMNS: &[SystemColumn] = &[
    u64c!("TP_GROUP_ID"),
    u64c!("CONSUMER_THREADS"),
    u64c!("RESERVE_THREADS"),
    u64c!("CONNECT_THREAD_COUNT"),
    u64c!("CONNECTION_COUNT"),
    u64c!("QUEUED_QUERIES"),
    u64c!("QUEUED_TRANSACTIONS"),
    u64cn!("STALL_LIMIT"),
    u64cn!("PRIO_KICKUP_TIMER"),
    enmn!("ALGORITHM", "enum('0','1')"),
    u64cn!("THREAD_CREATION_PER_MIN_MAX"),
];

const TP_THREAD_GROUP_STATS_COLUMNS: &[SystemColumn] = &[
    u64c!("TP_GROUP_ID"),
    u64c!("CONNECTIONS_STARTED"),
    u64c!("CONNECTIONS_CLOSED"),
    u64c!("QUERIES_EXECUTED"),
    u64c!("QUERIES_QUEUED"),
    u64c!("THREADS_STARTED"),
    u64c!("PRIO_KICKUPS"),
    u64c!("STALLED_QUERIES_EXECUTED"),
    u64c!("BECOME_CONSUMER_THREAD"),
    u64c!("BECOME_RESERVE_THREAD"),
    u64c!("BECOME_WAITING_THREAD"),
    u64c!("WAKE_THREAD_STALL_CHECKER"),
    u64c!("SLEEP_WAITS"),
    u64c!("THROTTLES"),
    u64c!("STALLS"),
    u64c!("POLLS_BY_LISTENER"),
    u64c!("POLLS_BY_WORKER"),
    u64c!("TIMER_POLLS"),
    u64c!("DEQUEUE_QUERIES_TO_GROUP_QUEUE"),
];

const TP_THREAD_STATE_COLUMNS: &[SystemColumn] = &[
    u64c!("TP_GROUP_ID"),
    u64c!("TP_THREAD_NUMBER"),
    enmn!("PROCESS_COUNT", "enum('0','1')"),
    charnn!("WAIT_TYPE", 64),
];

const NDB_SYNC_PENDING_OBJECTS_COLUMNS: &[SystemColumn] = &[
    charn!("SCHEMA_NAME", 64),
    charn!("NAME", 64),
    enm!(
        "TYPE",
        "enum('LOGFILE GROUP','TABLESPACE','SCHEMA','TABLE')"
    ),
];

const NDB_SYNC_EXCLUDED_OBJECTS_COLUMNS: &[SystemColumn] = &[
    charn!("SCHEMA_NAME", 64),
    charn!("NAME", 64),
    enm!(
        "TYPE",
        "enum('LOGFILE GROUP','TABLESPACE','SCHEMA','TABLE')"
    ),
    charnn!("REASON", 256),
];

/// Optional extension definitions. Consumed by
/// `information_schema::definition_groups`, so these tables surface in
/// `SHOW TABLES`, `information_schema.tables`/`.columns`, etc.
// Only optional extensions belong here. Community definitions have a single
// source of truth in the generated MySQL 8.0.46 fixture.
const EXTENSION_PERFORMANCE_SCHEMA_DEFS: &[SystemTableDef] = &[
    perf(
        "replication_group_member_actions",
        REPLICATION_GROUP_MEMBER_ACTIONS_COLUMNS,
    ),
    perf(
        "replication_group_configuration_version",
        REPLICATION_GROUP_CONFIGURATION_VERSION_COLUMNS,
    ),
    perf(
        "replication_group_communication_information",
        REPLICATION_GROUP_COMMUNICATION_INFORMATION_COLUMNS,
    ),
    perf("clone_status", CLONE_STATUS_COLUMNS),
    perf("clone_progress", CLONE_PROGRESS_COLUMNS),
    perf(
        "component_scheduler_tasks",
        COMPONENT_SCHEDULER_TASKS_COLUMNS,
    ),
    perf("firewall_groups", PROCESSLIST_PFS_FIREWALL_GROUPS_COLUMNS),
    perf("firewall_group_allowlist", FIREWALL_GROUP_ALLOWLIST_COLUMNS),
    perf("firewall_membership", FIREWALL_MEMBERSHIP_COLUMNS),
    perf("tp_thread_group_state", TP_THREAD_GROUP_STATE_COLUMNS),
    perf("tp_thread_group_stats", TP_THREAD_GROUP_STATS_COLUMNS),
    perf("tp_thread_state", TP_THREAD_STATE_COLUMNS),
    perf("ndb_sync_pending_objects", NDB_SYNC_PENDING_OBJECTS_COLUMNS),
    perf(
        "ndb_sync_excluded_objects",
        NDB_SYNC_EXCLUDED_OBJECTS_COLUMNS,
    ),
];

/// All visible Performance Schema definitions. The first portion is the exact
/// Oracle MySQL 8.0.46 Community catalog; the tail contains the 14
/// explicitly retained optional extension tables.
pub(crate) fn definitions() -> impl Iterator<Item = &'static SystemTableDef> {
    let community = crate::engine::performance_schema_mysql80_generated::COMMUNITY_DEFS;
    community
        .iter()
        .chain(EXTENSION_PERFORMANCE_SCHEMA_DEFS.iter())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PfsCompatibility {
    Mysql80Community,
    OptionalExtension,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PfsProducer {
    Real,
    HonestEmpty(&'static str),
}

pub(crate) fn compatibility(table: &str) -> Option<PfsCompatibility> {
    if crate::engine::performance_schema_mysql80_generated::COMMUNITY_DEFS
        .iter()
        .any(|definition| definition.name.eq_ignore_ascii_case(table))
    {
        Some(PfsCompatibility::Mysql80Community)
    } else if EXTENSION_PERFORMANCE_SCHEMA_DEFS
        .iter()
        .any(|definition| definition.name.eq_ignore_ascii_case(table))
    {
        Some(PfsCompatibility::OptionalExtension)
    } else {
        None
    }
}

pub(crate) fn producer(table: &str) -> Option<PfsProducer> {
    if !registered(table) {
        return None;
    }
    let real = matches!(
        table.to_ascii_lowercase().as_str(),
        "global_variables"
            | "session_variables"
            | "threads"
            | "metadata_locks"
            | "setup_consumers"
            | "setup_instruments"
            | "setup_actors"
            | "setup_objects"
            | "setup_threads"
            | "events_waits_current"
            | "events_waits_history"
            | "events_waits_history_long"
            | "events_statements_current"
            | "events_statements_history"
            | "events_statements_history_long"
            | "events_transactions_current"
            | "prepared_statements_instances"
            | "accounts"
            | "hosts"
            | "users"
            | "processlist"
            | "session_connect_attrs"
            | "session_account_connect_attrs"
            | "events_waits_summary_global_by_event_name"
            | "events_waits_summary_by_account_by_event_name"
            | "events_waits_summary_by_host_by_event_name"
            | "events_waits_summary_by_thread_by_event_name"
            | "events_waits_summary_by_user_by_event_name"
            | "events_waits_summary_by_instance"
            | "events_statements_summary_global_by_event_name"
            | "events_statements_summary_by_account_by_event_name"
            | "events_statements_summary_by_host_by_event_name"
            | "events_statements_summary_by_thread_by_event_name"
            | "events_statements_summary_by_user_by_event_name"
            | "events_statements_summary_by_digest"
            | "events_statements_histogram_by_digest"
            | "events_statements_histogram_global"
            | "events_errors_summary_global_by_error"
            | "events_errors_summary_by_account_by_error"
            | "events_errors_summary_by_host_by_error"
            | "events_errors_summary_by_thread_by_error"
            | "events_errors_summary_by_user_by_error"
            | "data_locks"
            | "data_lock_waits"
            | "global_status"
            | "session_status"
            | "status_by_account"
            | "status_by_host"
            | "status_by_thread"
            | "status_by_user"
            | "variables_by_thread"
            | "user_variables_by_thread"
            | "persisted_variables"
            | "variables_info"
            | "performance_timers"
            | "memory_summary_global_by_event_name"
            | "keyring_keys"
            | "keyring_component_status"
            | "error_log"
            | "replication_connection_configuration"
            | "replication_connection_status"
            | "replication_applier_configuration"
            | "replication_applier_status"
    );
    if real {
        Some(PfsProducer::Real)
    } else if compatibility(table) == Some(PfsCompatibility::OptionalExtension) {
        Some(PfsProducer::HonestEmpty(
            "optional MySQL plugin/enterprise subsystem is not installed in mydb",
        ))
    } else {
        Some(PfsProducer::HonestEmpty(
            "mydb has no equivalent instrumented subsystem yet",
        ))
    }
}

fn registered(table: &str) -> bool {
    definitions().any(|definition| definition.name.eq_ignore_ascii_case(table))
}

// ---------------------------------------------------------------------------
// Row dispatch
// ---------------------------------------------------------------------------

/// Produce rows for `performance_schema.<table>`. Tables with a real data
/// source are handled explicitly; any other *registered* table returns empty
/// (honest "no data" rather than fabricated rows); an unregistered name is a
/// hard "table does not exist" error.
pub(crate) fn rows(
    db: &Database,
    table: &str,
    registry: &SessionRegistry,
    request: &crate::engine::information_schema::SystemScanRequest,
) -> Result<Vec<Vec<Value>>, QueryError> {
    if !crate::engine::perf_setup::enabled() {
        return if registered(table) {
            Ok(Vec::new())
        } else {
            Err(error(
                crate::error::catalog::ER_NO_SUCH_TABLE,
                format!("Table 'performance_schema.{table}' doesn't exist"),
            ))
        };
    }
    match table {
        "threads" => Ok(threads_rows(db, registry)),
        "processlist" => Ok(processlist_rows(db, registry)),
        "accounts" => Ok(accounts_rows(registry)),
        "hosts" => Ok(hosts_rows(registry)),
        "users" => Ok(users_rows(registry)),
        "setup_consumers" => Ok(crate::engine::perf_setup::consumers_rows()),
        "setup_instruments" => Ok(crate::engine::perf_setup::instruments_rows()),
        "setup_actors" => Ok(crate::engine::perf_setup::actors_rows()),
        "setup_objects" => Ok(crate::engine::perf_setup::objects_rows()),
        "setup_threads" => Ok(crate::engine::perf_setup::thread_classes_rows()),
        "performance_timers" => Ok(performance_timers_rows()),
        "memory_summary_global_by_event_name" => Ok(memory_summary_global_rows()),
        "keyring_keys" => Ok(keyring_keys_rows()),
        "keyring_component_status" => Ok(keyring_component_status_rows()),
        "variables_info" => Ok(variables_info_rows(db)),
        "events_transactions_current" => {
            if crate::engine::perf_setup::consumer_enabled("events_transactions_current") {
                Ok(events_transactions_current_rows(db, registry))
            } else {
                Ok(Vec::new())
            }
        }
        "prepared_statements_instances" => Ok(prepared_statements_instances_rows(registry)),
        "events_waits_current" => Ok(crate::engine::perf_events::wait_current_rows()),
        "events_waits_history" => Ok(crate::engine::perf_events::wait_history_rows()),
        "events_waits_history_long" => Ok(crate::engine::perf_events::wait_history_long_rows()),
        "events_statements_current" => Ok(crate::engine::perf_events::statement_current_rows()),
        "events_statements_history" => Ok(crate::engine::perf_events::statement_history_rows()),
        "events_statements_history_long" => {
            Ok(crate::engine::perf_events::statement_history_long_rows())
        }
        "events_waits_summary_global_by_event_name" => {
            let mut rows = waits_summary_global_rows(db);
            rows.extend(crate::engine::perf_events::wait_summary_global_rows());
            Ok(rows)
        }
        "events_waits_summary_by_thread_by_event_name" => {
            Ok(crate::engine::perf_events::wait_summary_by_thread_rows())
        }
        "events_waits_summary_by_user_by_event_name" => {
            Ok(crate::engine::perf_events::wait_summary_by_user_rows())
        }
        "events_waits_summary_by_host_by_event_name" => {
            Ok(crate::engine::perf_events::wait_summary_by_host_rows())
        }
        "events_waits_summary_by_account_by_event_name" => {
            Ok(crate::engine::perf_events::wait_summary_by_account_rows())
        }
        "events_waits_summary_by_instance" => {
            Ok(crate::engine::perf_events::wait_summary_by_instance_rows())
        }
        "events_statements_summary_global_by_event_name" => {
            Ok(crate::engine::perf_events::statement_summary_global_rows())
        }
        "events_statements_summary_by_thread_by_event_name" => {
            Ok(crate::engine::perf_events::statement_summary_by_thread_rows())
        }
        "events_statements_summary_by_user_by_event_name" => {
            Ok(crate::engine::perf_events::statement_summary_by_user_rows())
        }
        "events_statements_summary_by_host_by_event_name" => {
            Ok(crate::engine::perf_events::statement_summary_by_host_rows())
        }
        "events_statements_summary_by_account_by_event_name" => {
            Ok(crate::engine::perf_events::statement_summary_by_account_rows())
        }
        "events_statements_summary_by_digest" => Ok(crate::engine::perf_digest::rows()),
        "events_statements_histogram_by_digest" => {
            Ok(crate::engine::perf_digest::histogram_by_digest_rows())
        }
        "events_statements_histogram_global" => {
            Ok(crate::engine::perf_digest::histogram_global_rows())
        }
        "events_errors_summary_global_by_error" => Ok(
            crate::engine::perf_errors::global_rows_filtered(equality_u16(request, "error_number")),
        ),
        "events_errors_summary_by_account_by_error" => {
            Ok(crate::engine::perf_errors::account_rows())
        }
        "events_errors_summary_by_host_by_error" => Ok(crate::engine::perf_errors::host_rows()),
        "events_errors_summary_by_thread_by_error" => Ok(crate::engine::perf_errors::thread_rows()),
        "events_errors_summary_by_user_by_error" => Ok(crate::engine::perf_errors::user_rows()),
        "metadata_locks" => Ok(metadata_locks_rows(db)),
        "data_locks" => Ok(data_locks_rows(db, registry)),
        "data_lock_waits" => Ok(data_lock_waits_rows(db, registry)),
        "global_status" => Ok(global_status_rows(db, registry)),
        "session_status" => Ok(session_status_rows(db, registry)),
        "status_by_thread" => Ok(status_by_thread_rows(registry)),
        "status_by_account" => Ok(status_by_account_rows(registry)),
        "status_by_host" => Ok(status_by_host_rows(registry)),
        "status_by_user" => Ok(status_by_user_rows(registry)),
        "global_variables" => Ok(
            crate::engine::information_schema::system_variables_rows_scoped(
                db,
                crate::engine::sysvar::Scope::Global,
            ),
        ),
        "session_variables" => Ok(
            crate::engine::information_schema::system_variables_rows_scoped(
                db,
                crate::engine::sysvar::Scope::Session,
            ),
        ),
        "persisted_variables" => Ok(persisted_variables_rows(db)),
        "variables_by_thread" => Ok(variables_by_thread_rows(db, registry, request)),
        "user_variables_by_thread" => Ok(user_variables_by_thread_rows(registry, request)),
        "error_log" => Ok(crate::engine::logging::error_log_rows()),
        "session_connect_attrs" => Ok(session_connect_attrs_rows(registry, false)),
        "session_account_connect_attrs" => Ok(session_connect_attrs_rows(registry, true)),
        "replication_connection_configuration" => Ok(replication_connection_configuration_rows()),
        "replication_connection_status" => Ok(replication_connection_status_rows()),
        "replication_applier_configuration" => Ok(replication_applier_configuration_rows()),
        "replication_applier_status" => Ok(replication_applier_status_rows()),
        other => match producer(other) {
            Some(PfsProducer::HonestEmpty(_)) => Ok(Vec::new()),
            Some(PfsProducer::Real) => Err(error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                format!("performance_schema.{other} is marked Real but has no row dispatch"),
            )),
            None => Err(error(
                crate::error::catalog::ER_NO_SUCH_TABLE,
                format!("Table 'performance_schema.{other}' doesn't exist"),
            )),
        },
    }
}

fn equality_value<'a>(
    request: &'a crate::engine::information_schema::SystemScanRequest,
    column: &str,
) -> Option<&'a Value> {
    request
        .equalities
        .iter()
        .find_map(|(name, value)| name.eq_ignore_ascii_case(column).then_some(value))
}

fn equality_u64(
    request: &crate::engine::information_schema::SystemScanRequest,
    column: &str,
) -> Option<u64> {
    match equality_value(request, column)? {
        Value::UInt(value) => Some(*value),
        Value::Int(value) if *value >= 0 => Some(*value as u64),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}

fn equality_u16(
    request: &crate::engine::information_schema::SystemScanRequest,
    column: &str,
) -> Option<u16> {
    equality_u64(request, column).and_then(|value| u16::try_from(value).ok())
}

// ---------------------------------------------------------------------------
// Row producers
// ---------------------------------------------------------------------------

/// Shared PROCESS-privilege scoping for every connection-list surface
/// (`SHOW PROCESSLIST`, `information_schema.processlist`,
/// `performance_schema.{processlist,threads}`, `sys.processlist`). Returns
/// `Some(login_user)` when the current session lacks `PROCESS` (nor `SUPER`)
/// and must therefore be restricted to its own threads; `None` means "see all"
/// (has the privilege, a system/internal session, or no borrowed session
/// context on this thread). Matches MySQL's per-account scoping.
pub(crate) fn processlist_user_restriction(db: &Database) -> Option<String> {
    match crate::engine::expr::current_authorization_context() {
        Some((ctx, false)) => {
            let auth = db.auth_catalog();
            let required = crate::engine::auth::RequiredPrivilege {
                privilege: crate::engine::auth::Privilege::Process,
                scope: crate::engine::auth::PrivilegeScope::Global,
            };
            if auth.account_has_privilege(&ctx, &required, false) {
                None
            } else {
                Some(ctx.authenticated_as.user.clone())
            }
        }
        // System session or no borrowed context → unrestricted.
        _ => None,
    }
}

/// Apply `processlist_user_restriction` to a snapshot in place. Shared by the
/// `information_schema` / `sys` connection-list producers so every surface
/// enforces the same PROCESS-privilege scoping.
pub(crate) fn scope_processlist_to_account(
    db: &Database,
    processes: &mut Vec<crate::engine::session_registry::ProcessInfo>,
) {
    if let Some(user) = processlist_user_restriction(db) {
        processes.retain(|p| {
            p.kind == crate::engine::session_registry::ProcessKind::Foreground && p.user == user
        });
    }
}

/// `performance_schema.threads` — one row per live connection, exposing the
/// PROCESSLIST_* columns plus PS thread metadata. Backed by the same session
/// registry as SHOW PROCESSLIST / information_schema.processlist; user
/// connections are reported as instrumented FOREGROUND threads (MySQL's
/// `thread/sql/one_connection`); server services use BACKGROUND.
fn threads_rows(db: &Database, registry: &SessionRegistry) -> Vec<Vec<Value>> {
    let mut processes = registry.snapshot();
    scope_processlist_to_account(db, &mut processes);
    processes.sort_by_key(|p| p.id);
    processes
        .into_iter()
        .map(|p| {
            let background = p.kind == crate::engine::session_registry::ProcessKind::Background;
            vec![
                uval(p.id as u64), // THREAD_ID
                s(if background {
                    "thread/sql/background"
                } else {
                    "thread/sql/one_connection"
                }), // NAME
                s(if background {
                    "BACKGROUND"
                } else {
                    "FOREGROUND"
                }), // TYPE
                if background {
                    Value::Null
                } else {
                    uval(p.id as u64)
                }, // PROCESSLIST_ID
                s(&p.user),        // PROCESSLIST_USER
                s(&p.host),        // PROCESSLIST_HOST
                p.database.map(Value::String).unwrap_or(Value::Null), // PROCESSLIST_DB
                s(&p.command),     // PROCESSLIST_COMMAND
                ival(p.time_secs as i64), // PROCESSLIST_TIME
                p.state.map(Value::String).unwrap_or(Value::Null), // PROCESSLIST_STATE
                p.info.map(Value::String).unwrap_or(Value::Null), // PROCESSLIST_INFO
                Value::Null,       // PARENT_THREAD_ID
                Value::Null,       // ROLE
                s(
                    if !background
                        && crate::engine::perf_setup::thread_enabled("thread/sql/one_connection")
                        && crate::engine::perf_setup::actor_enabled(&p.user, &p.host)
                    {
                        "YES"
                    } else {
                        "NO"
                    },
                ), // INSTRUMENTED
                s(
                    if !background
                        && crate::engine::perf_setup::thread_history_enabled(
                            "thread/sql/one_connection",
                        )
                        && crate::engine::perf_setup::actor_history_enabled(&p.user, &p.host)
                    {
                        "YES"
                    } else {
                        "NO"
                    },
                ), // HISTORY
                if background { Value::Null } else { s("TCP/IP") }, // CONNECTION_TYPE
                Value::Null,       // THREAD_OS_ID is not a connection id
                s(if background {
                    "SYS_default"
                } else {
                    "USR_default"
                }),
                s("PRIMARY"), // EXECUTION_ENGINE
                uval(p.exec_current_bytes),
                uval(p.query_peak_bytes.unwrap_or(p.session_peak_bytes)),
                uval(p.exec_current_bytes.saturating_add(p.retained_cache_bytes)),
                uval(p.session_peak_bytes.saturating_add(p.retained_cache_bytes)),
                s("NO"), // TELEMETRY_ACTIVE: mydb has no telemetry trace consumer
            ]
        })
        .collect()
}

/// `performance_schema.processlist` — PFS variant of the connection list (the
/// 9-column shape: ID, USER, HOST, DB, COMMAND, TIME, STATE, INFO,
/// EXECUTION_ENGINE). Same data source as `threads` / SHOW PROCESSLIST.
fn processlist_rows(db: &Database, registry: &SessionRegistry) -> Vec<Vec<Value>> {
    let mut processes = registry.snapshot();
    scope_processlist_to_account(db, &mut processes);
    processes.sort_by_key(|p| p.id);
    processes
        .into_iter()
        .map(|p| {
            vec![
                uval(p.id as u64),                                    // ID
                s(&p.user),                                           // USER
                s(&p.host),                                           // HOST
                p.database.map(Value::String).unwrap_or(Value::Null), // DB
                s(&p.command),                                        // COMMAND
                ival(p.time_secs as i64),                             // TIME
                p.state.map(Value::String).unwrap_or(Value::Null),    // STATE
                p.info.map(Value::String).unwrap_or(Value::Null),     // INFO
                s("PRIMARY"),                                         // EXECUTION_ENGINE
            ]
        })
        .collect()
}

/// `performance_schema.accounts` — one row per distinct (user, host) with a
/// live connection. CURRENT_CONNECTIONS counts live sessions in the group.
/// mydb keeps no historical connection accounting, so TOTAL_CONNECTIONS is
/// reported equal to CURRENT_CONNECTIONS (honest: never undercounted, but not
/// a lifetime total). The MAX_SESSION_* memory water-marks are NULL — mydb has
/// no per-session memory instrumentation.
fn accounts_rows(registry: &SessionRegistry) -> Vec<Vec<Value>> {
    let mut counts: std::collections::BTreeMap<(String, String), (i64, u64, u64, u64)> =
        std::collections::BTreeMap::new();
    for retired in registry.retired_connection_snapshots() {
        counts.insert(
            (retired.user, retired.host),
            (
                0,
                retired.total_connections,
                retired.max_controlled_memory,
                retired.max_total_memory,
            ),
        );
    }
    for p in registry.snapshot() {
        if p.kind != crate::engine::session_registry::ProcessKind::Foreground {
            continue;
        }
        let entry = counts.entry((p.user, p.host)).or_insert((0, 0, 0, 0));
        entry.0 += 1;
        entry.1 = entry.1.saturating_add(1);
        entry.2 = entry.2.max(p.session_peak_bytes);
        entry.3 = entry
            .3
            .max(p.session_peak_bytes.saturating_add(p.retained_cache_bytes));
    }
    counts
        .into_iter()
        .map(
            |((user, host), (n, total_connections, controlled, total))| {
                vec![
                    s(&user), // USER
                    s(&host), // HOST
                    ival(n),  // CURRENT_CONNECTIONS
                    ival(i64::try_from(total_connections).unwrap_or(i64::MAX)),
                    uval(controlled),
                    uval(total),
                ]
            },
        )
        .collect()
}

/// `performance_schema.hosts` — accounts grouped by HOST only.
fn hosts_rows(registry: &SessionRegistry) -> Vec<Vec<Value>> {
    connection_summary_rows::<true>(registry)
}

fn connection_summary_rows<const BY_HOST: bool>(registry: &SessionRegistry) -> Vec<Vec<Value>> {
    let mut counts: std::collections::BTreeMap<String, (i64, u64, u64, u64)> =
        std::collections::BTreeMap::new();
    for retired in registry.retired_connection_snapshots() {
        let entry = counts
            .entry(if BY_HOST { retired.host } else { retired.user })
            .or_insert((0, 0, 0, 0));
        entry.1 = entry.1.saturating_add(retired.total_connections);
        entry.2 = entry.2.max(retired.max_controlled_memory);
        entry.3 = entry.3.max(retired.max_total_memory);
    }
    for p in registry.snapshot() {
        if p.kind != crate::engine::session_registry::ProcessKind::Foreground {
            continue;
        }
        let entry = counts
            .entry(if BY_HOST { p.host } else { p.user })
            .or_insert((0, 0, 0, 0));
        entry.0 += 1;
        entry.1 = entry.1.saturating_add(1);
        entry.2 = entry.2.max(p.session_peak_bytes);
        entry.3 = entry
            .3
            .max(p.session_peak_bytes.saturating_add(p.retained_cache_bytes));
    }
    counts
        .into_iter()
        .map(|(host, (n, total_connections, controlled, total))| {
            vec![
                s(&host), // HOST
                ival(n),  // CURRENT_CONNECTIONS
                ival(i64::try_from(total_connections).unwrap_or(i64::MAX)),
                uval(controlled),
                uval(total),
            ]
        })
        .collect()
}

fn users_rows(registry: &SessionRegistry) -> Vec<Vec<Value>> {
    connection_summary_rows::<false>(registry)
}

/// `performance_schema.performance_timers` — the available wall-clock timers.
/// mydb measures wait events in microseconds (see `wait_event_summary`), so it
/// advertises the wall-clock timers whose frequency it can state honestly.
/// CYCLE / THREAD_CPU are not exposed by mydb's instrumentation, so their
/// frequency is NULL (MySQL reports NULL frequency for timers the platform
/// cannot provide). TIMER_OVERHEAD is unknown and left NULL.
fn performance_timers_rows() -> Vec<Vec<Value>> {
    // (TIMER_NAME, TIMER_FREQUENCY, TIMER_RESOLUTION)
    let timers: &[(&str, Option<i64>, i64)] = &[
        ("CYCLE", None, 1),
        ("NANOSECOND", Some(1_000_000_000), 1),
        ("MICROSECOND", Some(1_000_000), 1),
        ("MILLISECOND", Some(1_000), 1),
        ("THREAD_CPU", None, 1),
    ];
    timers
        .iter()
        .map(|&(name, freq, res)| {
            vec![
                s(name),                                     // TIMER_NAME
                freq.map(Value::Int).unwrap_or(Value::Null), // TIMER_FREQUENCY
                ival(res),                                   // TIMER_RESOLUTION
                Value::Null,                                 // TIMER_OVERHEAD
            ]
        })
        .collect()
}

fn memory_summary_global_rows() -> Vec<Vec<Value>> {
    const NAMES: [&str; 16] = [
        "memory/mydb/buffer_raw",
        "memory/mydb/storage_scratch",
        "memory/mydb/executor",
        "memory/mydb/derived_cache",
        "memory/mydb/partition_dml_batch",
        "memory/mydb/partition_ddl",
        "memory/mydb/partition_stats",
        "memory/mydb/ddl_copy",
        "memory/mydb/system_metadata",
        "memory/mydb/temporal_history",
        "memory/mydb/protocol_transient",
        "memory/mydb/binlog_transient",
        "memory/mydb/parser_transient",
        "memory/mydb/native_replication_transient",
        "memory/mydb/performance_schema",
        "memory/mydb/ddl_online_log",
    ];
    crate::engine::executor::mem_budget::pfs_memory_category_snapshots()
        .into_iter()
        .enumerate()
        .map(|(slot, snapshot)| {
            let current_count = snapshot.alloc_count.saturating_sub(snapshot.free_count);
            vec![
                s(NAMES[slot]),
                uval(snapshot.alloc_count),
                uval(snapshot.free_count),
                uval(snapshot.alloc_bytes),
                uval(snapshot.free_bytes),
                ival(0),
                ival(i64::try_from(current_count).unwrap_or(i64::MAX)),
                ival(i64::try_from(snapshot.peak_count).unwrap_or(i64::MAX)),
                ival(0),
                ival(i64::try_from(snapshot.current_bytes).unwrap_or(i64::MAX)),
                ival(i64::try_from(snapshot.peak_bytes).unwrap_or(i64::MAX)),
            ]
        })
        .collect()
}

fn keyring_keys_rows() -> Vec<Vec<Value>> {
    crate::engine::keyring::snapshot_ids()
        .into_iter()
        .map(|key_id| vec![s(&key_id), s("mydb"), s(&key_id)])
        .collect()
}

fn keyring_component_status_rows() -> Vec<Vec<Value>> {
    vec![
        vec![s("Component_name"), s("component_keyring_mydb")],
        vec![s("Author"), s("mydb")],
        vec![s("Implementation_name"), s("in_memory")],
        vec![
            s("Keys"),
            s(&crate::engine::keyring::snapshot_ids().len().to_string()),
        ],
    ]
}

/// System-variable provenance. Runtime SETs and registry-applied startup
/// settings carry their actual source; untouched defaults have no audit trail.
fn variables_info_rows(db: &Database) -> Vec<Vec<Value>> {
    crate::engine::sysvar::snapshot(crate::engine::sysvar::Scope::Global, None, Some(db))
        .into_iter()
        .map(|row| {
            let name = row.name;
            let provenance = crate::engine::sysvar::registry().provenance(name);
            vec![
                s(name),                                                 // VARIABLE_NAME
                s(provenance.as_ref().map_or("COMPILED", |p| p.source)), // VARIABLE_SOURCE
                provenance
                    .as_ref()
                    .and_then(|p| p.path.as_deref())
                    .map(s)
                    .unwrap_or(Value::Null), // VARIABLE_PATH
                Value::Null,                                             // MIN_VALUE unavailable
                Value::Null,                                             // MAX_VALUE unavailable
                provenance
                    .as_ref()
                    .and_then(|p| p.set_time.as_deref())
                    .map(s)
                    .unwrap_or(Value::Null), // SET_TIME
                provenance
                    .as_ref()
                    .and_then(|p| p.user.as_deref())
                    .map(s)
                    .unwrap_or(Value::Null), // SET_USER
                provenance
                    .as_ref()
                    .and_then(|p| p.host.as_deref())
                    .map(s)
                    .unwrap_or(Value::Null), // SET_HOST
            ]
        })
        .collect()
}

/// `performance_schema.events_waits_summary_global_by_event_name` — aggregated
/// wait-event counters from the storage engine's lock-wait instrumentation
/// (`StorageStatus::wait_event_summary`: `(event, count, total_us, max_us)`).
/// EVENT_NAME is mydb's own event string (not remapped onto MySQL's
/// `wait/...` taxonomy — honest about provenance). Timer columns are converted
/// from microseconds to picoseconds to match MySQL's `*_TIMER_WAIT` units.
/// Only events that actually accumulated data are emitted; the rest of MySQL's
/// fixed instrument set is not fabricated. MIN_TIMER_WAIT is not tracked by the
/// engine, so it is reported as 0.
/// `performance_schema.variables_by_thread` — emit `(THREAD_ID,
/// VARIABLE_NAME, VARIABLE_VALUE)` for every live connection.
///
/// We currently emit the same row set per thread, taken at the global
/// scope, plus a session-overlay row for the **active** thread (the one
/// running the SELECT). True per-thread tracking would require every
/// connection to publish a periodic snapshot of its session sysvars into
/// the registry; that's a future enhancement. The cardinality matches
/// MySQL — N_threads × N_session_variables — so monitoring queries get
/// the row count they expect.
fn variables_by_thread_rows(
    db: &Database,
    registry: &crate::engine::session_registry::SessionRegistry,
    request: &crate::engine::information_schema::SystemScanRequest,
) -> Vec<Vec<Value>> {
    let wanted_thread = equality_u64(request, "thread_id");
    let wanted_variable = equality_value(request, "variable_name").map(Value::to_display_string);
    let active_thread =
        crate::engine::expr::with_active_session(|session| session.map(|s| s.connection_id));
    let active_overlay: Vec<crate::engine::sysvar::VariableRow> =
        crate::engine::expr::with_active_session(|session| {
            crate::engine::sysvar::snapshot(
                crate::engine::sysvar::Scope::Session,
                session,
                Some(db),
            )
        });
    let processes = registry.snapshot();
    let mut out = Vec::new();
    // Always emit a row set for the currently-executing thread (the one
    // running this SELECT) — even when it isn't tracked in the registry
    // (e.g. embedded `execute_with_session` calls in test harnesses).
    let mut seen_active = false;
    for process in &processes {
        if wanted_thread.is_some_and(|thread| thread != process.id as u64) {
            continue;
        }
        let is_active = Some(process.id) == active_thread;
        seen_active |= is_active;
        if is_active {
            for row in &active_overlay {
                if wanted_variable
                    .as_deref()
                    .is_some_and(|wanted| !wanted.eq_ignore_ascii_case(row.name))
                {
                    continue;
                }
                out.push(vec![uval(process.id as u64), s(row.name), s(&row.value)]);
            }
        } else {
            for (name, value) in &process.session_variables {
                if wanted_variable
                    .as_deref()
                    .is_some_and(|wanted| !wanted.eq_ignore_ascii_case(name))
                {
                    continue;
                }
                out.push(vec![uval(process.id as u64), s(name), s(value)]);
            }
        }
    }
    if !seen_active {
        if let Some(thread_id) = active_thread {
            for row in &active_overlay {
                if wanted_variable
                    .as_deref()
                    .is_some_and(|wanted| !wanted.eq_ignore_ascii_case(row.name))
                {
                    continue;
                }
                out.push(vec![uval(thread_id as u64), s(row.name), s(&row.value)]);
            }
        }
    }
    out
}

fn user_variables_by_thread_rows(
    registry: &SessionRegistry,
    request: &crate::engine::information_schema::SystemScanRequest,
) -> Vec<Vec<Value>> {
    let wanted_thread = equality_u64(request, "thread_id");
    let wanted_variable = equality_value(request, "variable_name").map(Value::to_display_string);
    let mut rows = Vec::new();
    for process in registry.snapshot() {
        if wanted_thread.is_some_and(|thread| thread != process.id as u64) {
            continue;
        }
        for (name, value) in process.user_variables {
            if wanted_variable
                .as_deref()
                .is_some_and(|wanted| !wanted.eq_ignore_ascii_case(&name))
            {
                continue;
            }
            let bytes = match value {
                Value::Bytes(bytes) => bytes,
                Value::String(value) | Value::Decimal(value) => value.into_bytes(),
                other => other.to_display_string().into_bytes(),
            };
            rows.push(vec![uval(process.id as u64), s(&name), Value::Bytes(bytes)]);
        }
    }
    rows
}

fn waits_summary_global_rows(db: &Database) -> Vec<Vec<Value>> {
    const US_TO_PS: u64 = 1_000_000;
    db.storage_status()
        .wait_event_summary
        .into_iter()
        .map(|(event, count, total_us, max_us)| {
            let sum_ps = total_us.saturating_mul(US_TO_PS);
            let max_ps = max_us.saturating_mul(US_TO_PS);
            let avg_ps = if count > 0 { sum_ps / count } else { 0 };
            vec![
                s(&event),    // EVENT_NAME
                uval(count),  // COUNT_STAR
                uval(sum_ps), // SUM_TIMER_WAIT
                uval(0),      // MIN_TIMER_WAIT (not tracked)
                uval(avg_ps), // AVG_TIMER_WAIT
                uval(max_ps), // MAX_TIMER_WAIT
            ]
        })
        .collect()
}

/// Map each live connection's owned storage transaction id to its THREAD_ID
/// (mydb reports a connection's THREAD_ID as its connection id). Built from the
/// session registry so `data_locks` / `data_lock_waits` can attribute a lock to
/// the holding connection. A transaction with no live owner (e.g. an internal
/// background transaction) is simply absent, yielding a NULL THREAD_ID.
fn txn_thread_map(registry: &SessionRegistry) -> std::collections::HashMap<u64, u64> {
    registry
        .snapshot()
        .into_iter()
        .filter_map(|p| p.current_txn.map(|txn| (txn, p.id as u64)))
        .collect()
}

/// `performance_schema.events_transactions_current` — one row per active
/// transaction, produced live from [`Database::active_transaction_snapshot`]
/// and joined to a THREAD_ID via the session registry (M6). Columns mydb does
/// not yet track per transaction (GTID, XA state for non-XA txns, timers,
/// savepoint counters) are reported as NULL / defaults rather than fabricated.
fn events_transactions_current_rows(db: &Database, registry: &SessionRegistry) -> Vec<Vec<Value>> {
    let process_by_txn: std::collections::HashMap<
        u64,
        crate::engine::session_registry::ProcessInfo,
    > = registry
        .snapshot()
        .into_iter()
        .filter_map(|process| process.current_txn.map(|txn| (txn, process)))
        .collect();
    let active_session = crate::engine::expr::with_active_session(|session| {
        session.and_then(|session| {
            session.current_txn.map(|txn| {
                (
                    txn,
                    session.connection_id,
                    session.isolation.as_mysql_name().replace('-', " "),
                    session.autocommit,
                )
            })
        })
    });
    db.active_transaction_snapshot()
        .into_iter()
        .map(|trx| {
            let process = process_by_txn.get(&trx.transaction_id);
            let active = active_session
                .as_ref()
                .filter(|(txn, _, _, _)| *txn == trx.transaction_id);
            let access_mode = if trx.read_only {
                "READ ONLY"
            } else {
                "READ WRITE"
            };
            vec![
                process
                    .map(|process| uval(process.id as u64))
                    .or_else(|| active.map(|(_, connection_id, _, _)| uval(*connection_id as u64)))
                    .unwrap_or(Value::Null), // THREAD_ID
                uval(trx.transaction_id),           // EVENT_ID
                Value::Null,                        // END_EVENT_ID (active)
                s("transaction"),                   // EVENT_NAME
                s("ACTIVE"),                        // STATE
                s(&trx.transaction_id.to_string()), // TRX_ID
                Value::Null,                        // GTID (not tracked)
                Value::Null,                        // XID_FORMAT_ID
                Value::Null,                        // XID_GTRID
                Value::Null,                        // XID_BQUAL
                Value::Null,                        // XA_STATE (non-XA)
                Value::Null,                        // SOURCE
                Value::Null,                        // TIMER_START
                Value::Null,                        // TIMER_END
                Value::Null,                        // TIMER_WAIT
                s(access_mode),                     // ACCESS_MODE
                process
                    .map(|process| s(&process.isolation))
                    .or_else(|| active.map(|(_, _, isolation, _)| s(isolation)))
                    .unwrap_or(Value::Null), // ISOLATION_LEVEL
                process
                    .map(|process| s(if process.autocommit { "YES" } else { "NO" }))
                    .or_else(|| {
                        active
                            .map(|(_, _, _, autocommit)| s(if *autocommit { "YES" } else { "NO" }))
                    })
                    .unwrap_or(Value::Null), // AUTOCOMMIT
                Value::UInt(0),                     // NUMBER_OF_SAVEPOINTS
                Value::UInt(0),                     // NUMBER_OF_ROLLBACK_TO_SAVEPOINT
                Value::UInt(0),                     // NUMBER_OF_RELEASE_SAVEPOINT
                Value::Null,                        // OBJECT_INSTANCE_BEGIN
                Value::Null,                        // NESTING_EVENT_ID
                Value::Null,                        // NESTING_EVENT_TYPE
            ]
        })
        .collect()
}

fn prepared_statements_instances_rows(registry: &SessionRegistry) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for process in registry.snapshot() {
        for prepared in process.prepared_statements {
            let avg = if prepared.count_execute == 0 {
                0
            } else {
                prepared.sum_timer_execute / prepared.count_execute
            };
            rows.push(vec![
                uval(prepared.object_instance),
                uval(prepared.statement_id),
                prepared
                    .statement_name
                    .as_deref()
                    .map(s)
                    .unwrap_or(Value::Null),
                s(&prepared.sql_text),
                uval(prepared.owner_thread_id),
                uval(prepared.statement_id),
                Value::Null,
                Value::Null,
                Value::Null,
                s("PRIMARY"),
                uval(prepared.timer_prepare),
                uval(0),
                uval(prepared.count_execute),
                uval(prepared.sum_timer_execute),
                uval(prepared.min_timer_execute),
                uval(avg),
                uval(prepared.max_timer_execute),
                uval(0),
                uval(prepared.sum_errors),
                uval(0),
                uval(0),
                uval(0),
                uval(0),
                uval(0),
                uval(0),
                uval(0),
                uval(0),
                uval(0),
                uval(0),
                uval(0),
                uval(0),
                uval(0),
                uval(0),
                uval(0),
                uval(0),
                uval(0),
                uval(0),
                uval(prepared.memory_bytes),
                uval(prepared.memory_bytes),
                uval(0),
            ]);
        }
    }
    rows
}

fn metadata_locks_rows(db: &Database) -> Vec<Vec<Value>> {
    if !crate::engine::perf_setup::instrument_enabled("wait/lock/metadata/sql/mdl") {
        return Vec::new();
    }
    db.metadata_lock_snapshot()
        .into_iter()
        .map(|lock| {
            vec![
                s(lock.object_type),
                opt_s(lock.object_schema),
                opt_s(lock.object_name),
                opt_s(lock.column_name),
                uval(lock.object_instance),
                s(lock.lock_type),
                s(lock.lock_duration),
                s(lock.lock_status),
                s(&lock.source),
                lock.owner_thread_id.map(uval).unwrap_or(Value::Null),
                lock.owner_event_id.map(uval).unwrap_or(Value::Null),
            ]
        })
        .collect()
}

/// `performance_schema.data_locks` — every lock the transaction manager
/// currently holds or is waiting on, from [`Database::data_lock_snapshot`].
/// ENGINE is `MYDB`; THREAD_ID is resolved from the session registry (NULL when
/// the owning transaction has no live connection).
fn data_locks_rows(db: &Database, registry: &SessionRegistry) -> Vec<Vec<Value>> {
    let txn_to_thread = txn_thread_map(registry);
    db.data_lock_snapshot()
        .into_iter()
        .map(|lock| {
            let thread = txn_to_thread.get(&lock.transaction_id).copied();
            vec![
                s("MYDB"),                               // ENGINE
                s(&lock.engine_lock_id),                 // ENGINE_LOCK_ID
                uval(lock.transaction_id),               // ENGINE_TRANSACTION_ID
                thread.map(uval).unwrap_or(Value::Null), // THREAD_ID
                Value::Null,                             // EVENT_ID (not tracked)
                opt_s(lock.object_schema),               // OBJECT_SCHEMA
                opt_s(lock.object_name),                 // OBJECT_NAME
                Value::Null,                             // PARTITION_NAME
                Value::Null,                             // SUBPARTITION_NAME
                opt_s(lock.index_name),                  // INDEX_NAME
                uval(lock.object_instance),              // OBJECT_INSTANCE_BEGIN
                s(lock.lock_type),                       // LOCK_TYPE
                s(&lock.lock_mode),                      // LOCK_MODE
                s(lock.lock_status),                     // LOCK_STATUS
                opt_s(lock.lock_data),                   // LOCK_DATA
            ]
        })
        .collect()
}

/// `performance_schema.data_lock_waits` — the transaction wait-for graph as
/// `(requesting, blocking)` lock pairs, from
/// [`Database::data_lock_wait_snapshot`]. The lock ids join back to the
/// corresponding WAITING / GRANTED rows in `data_locks`.
fn data_lock_waits_rows(db: &Database, registry: &SessionRegistry) -> Vec<Vec<Value>> {
    let txn_to_thread = txn_thread_map(registry);
    let thread = |txn: u64| {
        txn_to_thread
            .get(&txn)
            .copied()
            .map(uval)
            .unwrap_or(Value::Null)
    };
    db.data_lock_wait_snapshot()
        .into_iter()
        .map(|wait| {
            vec![
                s("MYDB"),                              // ENGINE
                s(&wait.requesting_lock_id),            // REQUESTING_ENGINE_LOCK_ID
                uval(wait.requesting_transaction_id),   // REQUESTING_ENGINE_TRANSACTION_ID
                thread(wait.requesting_transaction_id), // REQUESTING_THREAD_ID
                Value::Null,                            // REQUESTING_EVENT_ID
                uval(wait.requesting_object_instance),  // REQUESTING_OBJECT_INSTANCE_BEGIN
                s(&wait.blocking_lock_id),              // BLOCKING_ENGINE_LOCK_ID
                uval(wait.blocking_transaction_id),     // BLOCKING_ENGINE_TRANSACTION_ID
                thread(wait.blocking_transaction_id),   // BLOCKING_THREAD_ID
                Value::Null,                            // BLOCKING_EVENT_ID
                uval(wait.blocking_object_instance),    // BLOCKING_OBJECT_INSTANCE_BEGIN
            ]
        })
        .collect()
}

/// Real server status variables, mapped from the storage engine's
/// `StorageStatus`, the process-global connection metrics, and process uptime.
/// Only variables with a genuine data source are emitted — mydb does not
/// fabricate zero-valued counters for instrumentation it lacks. Counters are
/// process-global (mydb has no per-session status accounting), so
/// `global_status` and `session_status` report the same set. Shared with
/// `SHOW [GLOBAL] STATUS` via `global_status_pairs`.
pub(crate) fn global_status_pairs(
    db: &Database,
    registry: Option<&SessionRegistry>,
) -> Vec<(String, String)> {
    let st = db.mysql_status();
    let conn = crate::mysql::connection_manager::connection_metrics();
    let uptime = crate::engine::status_counters::uptime_seconds();

    let mut out: Vec<(String, String)> = Vec::new();
    let mut push = |name: &str, value: u64| out.push((name.to_string(), value.to_string()));

    // InnoDB-style buffer pool / IO counters (mapped from the mydb buffer pool).
    push(
        "Innodb_buffer_pool_read_requests",
        st.buffer_hits.saturating_add(st.buffer_misses),
    );
    push("Innodb_buffer_pool_reads", st.buffer_misses);
    push("Innodb_buffer_pool_pages_dirty", st.buffer_pool_dirty_pages);
    push("Innodb_buffer_pool_pages_total", st.buffer_pool_pages);
    push("Innodb_buffer_pool_pages_data", st.buffer_pool_used_pages);
    push(
        "Innodb_buffer_pool_pages_free",
        st.buffer_pool_pages
            .saturating_sub(st.buffer_pool_used_pages),
    );
    push("Innodb_buffer_pool_wait_free", st.buffer_pool_waits);
    push(
        "Innodb_buffer_pool_write_requests",
        st.buffer_pool_write_requests,
    );
    push("Innodb_pages_read", st.buffer_pool_read_ios);
    push("Innodb_pages_written", st.buffer_pool_write_ios);
    push("Innodb_data_reads", st.buffer_pool_read_syscalls);
    push("Innodb_data_writes", st.buffer_pool_write_syscalls);

    // Row-lock / deadlock counters.
    push("Innodb_row_lock_waits", st.lock_waits);
    push("Innodb_row_lock_current_waits", st.active_lock_waits as u64);
    push("Innodb_row_lock_time", st.lock_wait_time_ms);
    push("Innodb_row_lock_time_avg", st.lock_wait_time_avg_ms);
    push("Innodb_row_lock_time_max", st.lock_wait_time_max_ms);
    push("Innodb_deadlocks", st.deadlocks);

    // WAL / log counters (mapped from mydb's WAL stats — Innodb_log_*
    // names so wire-level monitoring stays MySQL-shaped).
    push("Innodb_log_writes", st.wal_physical_write_calls);
    push("Innodb_log_write_requests", st.wal_logical_records);
    push("Innodb_os_log_written", st.wal_physical_write_bytes);
    push("Innodb_os_log_fsyncs", st.wal_syncs);

    // mydb-native counters surfaced via the same SHOW STATUS table so
    // dashboards can graph them alongside their MySQL counterparts.
    push("Mydb_active_transactions", st.active_transactions as u64);
    push("Mydb_undo_bytes", st.undo_bytes);
    push("Mydb_undo_records", st.undo_records as u64);
    for (name, value) in db.sequence_status_pairs() {
        push(name, value);
    }
    push("Mydb_wal_bytes", st.wal_bytes);
    push("Mydb_wal_syncs", st.wal_syncs);
    push("Mydb_checkpoints_total", st.checkpoints_total);
    push("Mydb_buffer_pool_evictions", st.buffer_pool_evictions);
    push("Mydb_buffer_pool_prefetches", st.buffer_pool_prefetches);
    push(
        "Mydb_buffer_pool_prefetch_hits",
        st.buffer_pool_prefetch_hits,
    );
    push("Mydb_plan_cache_hits", st.plan_cache_hits);
    push("Mydb_plan_cache_misses", st.plan_cache_misses);
    push("Mydb_select_parse_cache_hits", st.select_parse_cache_hits);
    push(
        "Mydb_select_compiled_plan_hits",
        st.select_compiled_plan_hits,
    );
    push("Mydb_select_operator_compiles", st.select_operator_compiles);
    push("Mydb_prepared_plan_reuses", st.prepared_plan_reuses);
    push("Mydb_prepared_plan_compiles", st.prepared_plan_compiles);
    push("Mydb_snapshot_too_old", st.snapshot_too_old_count);
    push("Mydb_undo_peak_bytes", st.undo_peak_bytes);
    push(
        "Mydb_setop_spill_count",
        crate::engine::executor::SETOP_SPILL_COUNT.load(std::sync::atomic::Ordering::Relaxed),
    );
    push(
        "Mydb_setop_spill_bytes_written",
        crate::engine::executor::SETOP_SPILL_BYTES_WRITTEN
            .load(std::sync::atomic::Ordering::Relaxed),
    );
    push(
        "Mydb_setop_spill_bytes_read",
        crate::engine::executor::SETOP_SPILL_BYTES_READ.load(std::sync::atomic::Ordering::Relaxed),
    );
    push(
        "Mydb_setop_spill_partitions",
        crate::engine::executor::SETOP_SPILL_PARTITIONS.load(std::sync::atomic::Ordering::Relaxed),
    );
    push(
        "Mydb_setop_spill_repartitions",
        crate::engine::executor::SETOP_SPILL_REPARTITIONS
            .load(std::sync::atomic::Ordering::Relaxed),
    );
    push(
        "Mydb_setop_spill_max_depth",
        crate::engine::executor::SETOP_SPILL_MAX_DEPTH.load(std::sync::atomic::Ordering::Relaxed),
    );
    push(
        "Mydb_setop_spill_file_opens",
        crate::engine::executor::SETOP_SPILL_FILE_OPENS.load(std::sync::atomic::Ordering::Relaxed),
    );
    push(
        "Mydb_setop_spill_merge_passes",
        crate::engine::executor::SETOP_SPILL_MERGE_PASSES
            .load(std::sync::atomic::Ordering::Relaxed),
    );
    push(
        "Mydb_status_aggregation_dropped",
        crate::engine::status_counters::status_aggregation_dropped(),
    );
    let exec_permit = crate::util::exec_permit::status_snapshot();
    push("Mydb_txn_admission_waits", exec_permit.txn_admission_waits);
    push(
        "Mydb_txn_admission_wait_us",
        exec_permit.txn_admission_wait_us,
    );
    push(
        "Mydb_mixed_admission_waits",
        exec_permit.mixed_admission_waits,
    );
    push(
        "Mydb_mixed_admission_wait_us",
        exec_permit.mixed_admission_wait_us,
    );
    push(
        "Mydb_parallel_read_pressure_entries",
        exec_permit.parallel_read_pressure_entries,
    );
    push(
        "Mydb_parallel_read_pressure_suppressed_rw_txn",
        exec_permit.parallel_read_pressure_suppressed_rw_txn,
    );
    push(
        "Mydb_parallel_read_pressure_active",
        exec_permit.parallel_read_pressure_active as u64,
    );
    push(
        "Mydb_parallel_read_gate_waits",
        exec_permit.parallel_read_gate_waits,
    );
    push(
        "Mydb_parallel_read_gate_wait_us",
        exec_permit.parallel_read_gate_wait_us,
    );

    // Connection counters (process-global accounting). Counter names
    // align with MySQL 8.0's `SHOW GLOBAL STATUS` output so DBAs can
    // point their existing monitoring scripts at mydb without
    // remapping fields. Every value here has a genuine data source —
    // we do not fabricate zero-valued counters for instrumentation
    // mydb hasn't built yet (`Connection_errors_*` family, etc.).
    push("Threads_connected", conn.active);
    push("Connections", conn.accepted);
    push("Aborted_connects", conn.handshake_failures);
    push("Aborted_clients", conn.aborted_clients);
    push("Threads_created", conn.threads_created);
    // mydb has no separate "running" vs "connected" distinction (every
    // session is its own task), so they coincide. MySQL reports the
    // same when no query is in flight.
    push(
        "Threads_running",
        registry
            .map(SessionRegistry::running_foreground_count)
            .unwrap_or(conn.active),
    );
    // mydb spawns a task per connection and never pools idle threads, so the
    // thread cache is genuinely empty (an accurate 0, not a stub).
    push("Threads_cached", 0);
    // Client statements executed — QPS graphs (`rate(mysql_global_status_
    // queries[1m])`) are the single most-used exporter metric. mydb counts
    // sub-statements the same as top-level ones, so Queries mirrors Questions.
    let aggregated_session = registry.map(SessionRegistry::global_session_status);
    for (name, counter) in [
        (
            "Mydb_sequence_values",
            crate::engine::status_counters::SessionCounter::SequenceValues,
        ),
        (
            "Mydb_sequence_cache_hits",
            crate::engine::status_counters::SessionCounter::SequenceCacheHits,
        ),
        (
            "Mydb_sequence_refills",
            crate::engine::status_counters::SessionCounter::SequenceRefills,
        ),
    ] {
        push(
            name,
            aggregated_session
                .as_ref()
                .map(|status| status.value(counter))
                .unwrap_or(0),
        );
    }
    push(
        "Questions",
        aggregated_session
            .as_ref()
            .map(|status| status.value(crate::engine::status_counters::SessionCounter::Questions))
            .unwrap_or(0),
    );
    push("Queries", conn.queries_executed);
    // Real peak gauge (monotonic max of concurrent connections) rather
    // than the former handshake-count approximation.
    push("Max_used_connections", conn.max_used_connections);
    // Connections refused because max_connections was reached.
    push(
        "Connection_errors_max_connections",
        conn.connection_errors_max_connections,
    );

    // --- Network throughput (client wire bytes) ------------------------
    // The two most-scraped exporter gauges for network saturation.
    push(
        "Bytes_received",
        crate::engine::status_counters::bytes_received(),
    );
    push("Bytes_sent", crate::engine::status_counters::bytes_sent());

    // --- Command counters (Com_xxx) ------------------------------------
    // Per-command rates (`rate(mysql_global_status_commands_total[1m])`) are
    // the backbone of every MySQL dashboard. Statement-level kinds are
    // keyword-classified at the COM_QUERY boundary; protocol-command kinds
    // are counted at COM dispatch. Only kinds with a real event site are
    // emitted — we do not pad out MySQL's full ~150-name Com_ space.
    if let Some(status) = &aggregated_session {
        for (name, value) in status.com_counters() {
            push(name, value);
        }
    } else {
        for (name, value) in crate::engine::status_counters::com_counters() {
            push(name, value);
        }
    }
    for (name, value) in crate::engine::status_counters::like_access_counters() {
        push(name, value);
    }

    // --- Handler / row-operation counters ------------------------------
    // `Handler_write/update/delete` and the `Innodb_rows_*` family map onto
    // mydb's DML row counters; `Handler_read_rnd_next` counts sequential
    // scan rows while `Innodb_rows_read` includes index accesses. `Handler_commit` /
    // `Handler_rollback` come from the storage commit/rollback funnels.
    push("Handler_write", st.dml_insert_rows);
    push("Handler_update", st.dml_update_rows);
    push("Handler_delete", st.dml_delete_rows);
    push(
        "Handler_commit",
        crate::engine::status_counters::handler_commit(),
    );
    push(
        "Handler_rollback",
        crate::engine::status_counters::handler_rollback(),
    );
    push(
        "Handler_read_rnd_next",
        crate::engine::status_counters::handler_read_rnd_next(),
    );
    push("Innodb_rows_inserted", st.dml_insert_rows);
    push("Innodb_rows_updated", st.dml_update_rows);
    push("Innodb_rows_deleted", st.dml_delete_rows);
    push(
        "Innodb_rows_read",
        crate::engine::status_counters::rows_read(),
    );

    // --- Query-execution counters --------------------------------------
    push(
        "Slow_queries",
        crate::engine::status_counters::slow_queries(),
    );
    // Sort_rows is intentionally not surfaced yet.
    // mydb has no single sort funnel — sorts happen in the row-pipeline
    // external sort, several in-memory `sort_plan_rows_by_order` call sites
    // (some invoked per parallel chunk / merge pass), and the default
    // vectorized executor's own operators — so no instrumentation point
    // counts each query's input rows exactly once. External/vector spill sorts
    // do share a run hook, so Sort_merge_passes and Created_tmp_files below
    // are real for that implemented path.
    // Table-level lock acquisitions granted immediately (mydb takes no
    // table-lock waits on the common path, so this mirrors the acquire
    // count; a dedicated waited counter is deferred until table-lock
    // contention accounting lands).
    push("Table_locks_immediate", st.table_lock_acquires);
    push("Table_locks_waited", st.table_lock_waits);
    push(
        "Prepared_stmt_count",
        crate::engine::status_counters::prepared_stmt_count(),
    );
    push(
        "Performance_schema_digest_lost",
        crate::engine::perf_digest::lost(),
    );
    push(
        "Created_tmp_tables",
        crate::engine::status_counters::created_tmp_tables(),
    );
    push(
        "Created_tmp_disk_tables",
        crate::engine::status_counters::created_tmp_disk_tables(),
    );
    push(
        "Created_tmp_files",
        crate::engine::status_counters::created_tmp_files(),
    );
    push(
        "Sort_merge_passes",
        crate::engine::status_counters::sort_merge_passes(),
    );

    // Replica health counters — surface whether mydb is replicating
    // and how the channel is doing without making the user open
    // SHOW REPLICA STATUS.
    let (mut running, mut total) = (0u64, 0u64);
    if let Some(mgr) = crate::replication::global_manager() {
        for (_cfg, status) in mgr.snapshot_all_channels() {
            total += 1;
            if status.running {
                running += 1;
            }
        }
    }
    push("Mydb_replica_channels_running", running);
    push("Mydb_replica_channels_total", total);

    push("Uptime", uptime);
    push(
        "Uptime_since_flush_status",
        crate::engine::status_counters::uptime_since_flush_status_seconds(),
    );
    // MySQL semi-sync status. Master side (Rpl_semi_sync_source_* and the
    // legacy _master_ aliases) is sourced from the binlog manager; the
    // replica-side status is always OFF because mydb-as-replica connects to a
    // MySQL source as an async replica (phase 3 boundary).
    let mut semi_sync = crate::binlog::semi_sync_status_pairs();
    if semi_sync.is_empty() {
        for prefix in ["Rpl_semi_sync_source", "Rpl_semi_sync_master"] {
            for (suffix, value) in [
                ("status", "OFF"),
                ("clients", "0"),
                ("yes_tx", "0"),
                ("no_tx", "0"),
                ("wait_sessions", "0"),
            ] {
                semi_sync.push((format!("{prefix}_{suffix}"), value.to_string()));
            }
        }
        for (name, value) in [
            ("Mydb_binlog_group_sync_requests", "0"),
            ("Mydb_binlog_group_sync_batches", "0"),
            ("Mydb_binlog_group_sync_avg_batch_size", "0.00"),
            ("Mydb_binlog_group_sync_batch_max", "0"),
            ("Mydb_binlog_cached_events", "0"),
            ("Mydb_binlog_cached_payload_bytes", "0"),
            ("Mydb_binlog_file_opens", "0"),
            ("Mydb_binlog_write_calls", "0"),
            ("Mydb_binlog_write_bytes", "0"),
            ("Mydb_binlog_sync_total_us", "0"),
            ("Mydb_binlog_sync_max_us", "0"),
        ] {
            semi_sync.push((name.to_string(), value.to_string()));
        }
    }
    for (name, value) in semi_sync {
        out.push((name, value));
    }
    out.push((
        "Rpl_semi_sync_replica_status".to_string(),
        "OFF".to_string(),
    ));
    out.push(("Rpl_semi_sync_slave_status".to_string(), "OFF".to_string()));

    out.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    out
}

fn global_status_rows(db: &Database, registry: &SessionRegistry) -> Vec<Vec<Value>> {
    let mut pairs = crate::engine::expr::with_active_session(|session| {
        session
            .map(|session| {
                crate::engine::system::status_pairs(
                    session,
                    Some(db),
                    Some(registry),
                    crate::parser::ShowScope::Global,
                )
            })
            .unwrap_or_else(|| global_status_pairs(db, Some(registry)))
    });
    pairs.retain(|(name, _)| {
        crate::engine::status_counters::status_var_def(name)
            .is_some_and(|definition| definition.pfs_visible)
    });
    pairs.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    pairs
        .into_iter()
        .map(|(name, value)| vec![s(&name), s(&value)])
        .collect()
}

fn session_status_rows(db: &Database, registry: &SessionRegistry) -> Vec<Vec<Value>> {
    let mut pairs = crate::engine::expr::with_active_session(|session| {
        session
            .map(|session| {
                crate::engine::system::status_pairs(
                    session,
                    Some(db),
                    Some(registry),
                    crate::parser::ShowScope::Session,
                )
            })
            .unwrap_or_else(|| global_status_pairs(db, Some(registry)))
    });
    // MySQL Performance Schema deliberately excludes Com_xxx from all status
    // variable tables; statement summaries are the PFS source for them.
    pairs.retain(|(name, _)| {
        crate::engine::status_counters::status_var_def(name)
            .is_some_and(|definition| definition.pfs_visible)
    });
    pairs.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    pairs
        .into_iter()
        .map(|(name, value)| vec![s(&name), s(&value)])
        .collect()
}

fn status_by_thread_rows(registry: &SessionRegistry) -> Vec<Vec<Value>> {
    let mut snapshots = registry.thread_status_snapshots();
    snapshots.sort_unstable_by_key(|snapshot| snapshot.thread_id);
    let mut rows = Vec::new();
    for snapshot in snapshots {
        for (name, value) in snapshot.status.session_pfs_counters() {
            rows.push(vec![
                uval(snapshot.thread_id as u64),
                s(name),
                s(&value.to_string()),
            ]);
        }
    }
    rows
}

fn status_by_account_rows(registry: &SessionRegistry) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for snapshot in registry.account_status_snapshots() {
        for (name, value) in snapshot.status.session_pfs_counters() {
            rows.push(vec![
                snapshot.user.as_deref().map(s).unwrap_or(Value::Null),
                snapshot.host.as_deref().map(s).unwrap_or(Value::Null),
                s(name),
                s(&value.to_string()),
            ]);
        }
    }
    rows
}

fn status_by_host_rows(registry: &SessionRegistry) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for snapshot in registry.host_status_snapshots() {
        for (name, value) in snapshot.status.session_pfs_counters() {
            rows.push(vec![
                snapshot.host.as_deref().map(s).unwrap_or(Value::Null),
                s(name),
                s(&value.to_string()),
            ]);
        }
    }
    rows
}

fn status_by_user_rows(registry: &SessionRegistry) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for snapshot in registry.user_status_snapshots() {
        for (name, value) in snapshot.status.session_pfs_counters() {
            rows.push(vec![
                snapshot.user.as_deref().map(s).unwrap_or(Value::Null),
                s(name),
                s(&value.to_string()),
            ]);
        }
    }
    rows
}

/// performance_schema.persisted_variables — the in-memory shadow of
/// `<data_dir>/system/mysqld-auto.cnf`. Two columns to match MySQL 8.0's
/// view (`VARIABLE_NAME`, `VARIABLE_VALUE`); the file's metadata fields
/// (timestamp/user/host) are not currently surfaced.
fn persisted_variables_rows(db: &Database) -> Vec<Vec<Value>> {
    db.persisted_variables()
        .snapshot()
        .into_iter()
        .map(|entry| vec![s(&entry.name), s(&entry.value)])
        .collect()
}

// setup_consumers / setup_instruments / setup_actors / setup_objects /
// setup_threads row producers and their writable runtime state live in
// `crate::engine::perf_setup`.

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// performance_schema.session_connect_attrs / session_account_connect_attrs —
/// one row per (connection, attribute) pair from the CLIENT_CONNECT_ATTRS the
/// client sent at handshake. mydb returns attributes for all live connections;
/// it does not scope session_account_connect_attrs to the current account (the
/// row path has no session handle), so the two tables share this producer.
fn session_connect_attrs_rows(
    registry: &SessionRegistry,
    current_account_only: bool,
) -> Vec<Vec<Value>> {
    let current_user = current_account_only
        .then(|| {
            crate::engine::expr::with_active_session(|session| {
                session.map(|session| session.login_user.clone())
            })
        })
        .flatten();
    let mut processes = registry.snapshot();
    if let Some(current_user) = current_user {
        processes.retain(|process| process.user == current_user);
    }
    processes.sort_by_key(|p| p.id);
    let mut rows = Vec::new();
    for p in processes {
        for (ordinal, (name, value)) in p.connection_attrs.iter().enumerate() {
            rows.push(vec![
                Value::UInt(p.id as u64),         // PROCESSLIST_ID
                s(name),                          // ATTR_NAME
                s(value),                         // ATTR_VALUE
                Value::Int((ordinal + 1) as i64), // ORDINAL_POSITION
            ]);
        }
    }
    rows
}

fn s(value: &str) -> Value {
    Value::String(value.to_string())
}

/// performance_schema.replication_connection_configuration: one row per
/// channel exposing the negotiated `CHANGE REPLICATION SOURCE` parameters.
/// Column order MUST match the generated Community connection configuration definition
/// (27 columns, MySQL 8.0). The previous implementation only pushed 26
/// values in an out-of-order sequence — the row was self-consistent-looking
/// but every column past the 5th read a value belonging to another column
/// (`AUTO_POSITION` returned a heartbeat placeholder, `SSL_ALLOWED` returned
/// a small integer, etc.), so orchestrator's checks would misfire.
fn replication_connection_configuration_rows() -> Vec<Vec<Value>> {
    let Some(mgr) = crate::replication::global_manager() else {
        return Vec::new();
    };
    let mut rows = Vec::new();
    for (cfg, _status) in mgr.snapshot_all_channels() {
        let ssl_allowed = if matches!(
            cfg.ssl_mode,
            crate::replication::ReplicationSslMode::Disabled
        ) {
            "NO"
        } else {
            "YES"
        };
        let ssl_verify = if matches!(
            cfg.ssl_mode,
            crate::replication::ReplicationSslMode::VerifyIdentity
                | crate::replication::ReplicationSslMode::VerifyCa
        ) {
            "YES"
        } else {
            "NO"
        };
        let auto_position = if cfg.auto_position { "1" } else { "0" };
        rows.push(vec![
            s(&cfg.channel),                                         // CHANNEL_NAME
            s(&cfg.source_host),                                     // HOST
            Value::Int(cfg.source_port as i64),                      // PORT
            s(&cfg.source_user),                                     // USER
            s(""),                                                   // NETWORK_INTERFACE
            s(auto_position),                                        // AUTO_POSITION
            s(ssl_allowed),                                          // SSL_ALLOWED
            s(&cfg.ssl_ca.clone().unwrap_or_default()),              // SSL_CA_FILE
            s(""),                                                   // SSL_CA_PATH
            s(&cfg.ssl_cert.clone().unwrap_or_default()),            // SSL_CERTIFICATE
            s(&cfg.ssl_cipher.clone().unwrap_or_default()),          // SSL_CIPHER
            s(&cfg.ssl_key.clone().unwrap_or_default()),             // SSL_KEY
            s(ssl_verify), // SSL_VERIFY_SERVER_CERTIFICATE
            s(""),         // SSL_CRL_FILE
            s(""),         // SSL_CRL_PATH
            Value::Int((cfg.connect_retry_ms / 1000).max(1) as i64), // CONNECTION_RETRY_INTERVAL
            Value::UInt(86400), // CONNECTION_RETRY_COUNT (advisory)
            Value::Double(1.0), // HEARTBEAT_INTERVAL (seconds)
            s(""),         // TLS_VERSION
            s(""),         // PUBLIC_KEY_PATH
            s("NO"),       // GET_PUBLIC_KEY
            s(""),         // NETWORK_NAMESPACE
            s(""),         // COMPRESSION_ALGORITHM
            Value::UInt(3), // ZSTD_COMPRESSION_LEVEL
            Value::Null,   // TLS_CIPHERSUITES
            s("0"),        // SOURCE_CONNECTION_AUTO_FAILOVER
            s("0"),        // GTID_ONLY
        ]);
    }
    rows
}

/// performance_schema.replication_connection_status: live IO thread state.
/// C2/C3: LAST_ERROR_* now reflects the structured `st.last_io_error`
/// (falling back to the SQL error when IO is clean but a SQL error
/// preceded the disconnect) instead of a hard-coded 1593; SOURCE_UUID
/// carries the connect-time probed value; heartbeat counters/timestamps
/// come from the newly-wired `HeartbeatEvent` handler.
fn replication_connection_status_rows() -> Vec<Vec<Value>> {
    let Some(mgr) = crate::replication::global_manager() else {
        return Vec::new();
    };
    let mut rows = Vec::new();
    for (_cfg, st) in mgr.snapshot_all_channels() {
        let service_state = if st.running {
            "ON"
        } else if st.configured {
            "CONNECTING"
        } else {
            "OFF"
        };
        let effective_error = st.last_io_error.as_ref().or(st.last_sql_error.as_ref());
        let (err_no, err_msg, err_ts) = match effective_error {
            Some(err) => (
                Value::Int(err.code as i64),
                s(&err.message),
                format_unix_secs_as_timestamp6(err.at_unix_secs),
            ),
            None => (Value::Int(0), s(""), format_unix_secs_as_timestamp6(0)),
        };
        let heartbeat_ts = if st.last_heartbeat_at_unix_secs > 0 {
            format_unix_secs_as_timestamp6(st.last_heartbeat_at_unix_secs)
        } else {
            format_unix_secs_as_timestamp6(0)
        };
        rows.push(vec![
            s(&st.channel),                            // CHANNEL_NAME
            s(""),                                     // GROUP_NAME
            s(&st.source_uuid),                        // SOURCE_UUID
            Value::Null,                               // THREAD_ID
            s(service_state),                          // SERVICE_STATE
            Value::UInt(st.count_received_heartbeats), // COUNT_RECEIVED_HEARTBEATS
            heartbeat_ts,                              // LAST_HEARTBEAT_TIMESTAMP
            s(&st.retrieved_gtid_set),                 // RECEIVED_TRANSACTION_SET
            err_no,                                    // LAST_ERROR_NUMBER
            err_msg,                                   // LAST_ERROR_MESSAGE
            err_ts,                                    // LAST_ERROR_TIMESTAMP
            s(""),                                     // LAST_QUEUED_TRANSACTION
            Value::Null, // LAST_QUEUED_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP
            Value::Null, // ..._IMMEDIATE_COMMIT_TIMESTAMP
            Value::Null, // ..._START_QUEUE_TIMESTAMP
            Value::Null, // ..._END_QUEUE_TIMESTAMP
            s(""),       // QUEUEING_TRANSACTION
            Value::Null, // QUEUEING_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP
            Value::Null, // ..._IMMEDIATE_COMMIT_TIMESTAMP
            Value::Null, // ..._START_QUEUE_TIMESTAMP
        ]);
    }
    rows
}

/// Render a Unix-seconds timestamp as MySQL's `TIMESTAMP(6)`
/// (`YYYY-MM-DD HH:MM:SS.000000`). Zero denotes an event not observed yet.
fn format_unix_secs_as_timestamp6(unix_secs: u64) -> Value {
    if unix_secs == 0 {
        return s("0000-00-00 00:00:00.000000");
    }
    use chrono::{Local, TimeZone};
    match Local.timestamp_opt(unix_secs as i64, 0).single() {
        Some(dt) => Value::String(dt.format("%Y-%m-%d %H:%M:%S.000000").to_string()),
        None => Value::Null,
    }
}

/// performance_schema.replication_applier_configuration: per-channel apply
/// settings. mydb has no MTS workers, so DESIRED_DELAY reflects MASTER_DELAY.
fn replication_applier_configuration_rows() -> Vec<Vec<Value>> {
    let Some(mgr) = crate::replication::global_manager() else {
        return Vec::new();
    };
    let mut rows = Vec::new();
    for (cfg, _st) in mgr.snapshot_all_channels() {
        rows.push(vec![
            s(&cfg.channel),
            Value::UInt(cfg.source_delay_secs),
            s("NO"),     // PRIVILEGE_CHECKS_USER
            s("YES"),    // REQUIRE_ROW_FORMAT
            s("STREAM"), // REQUIRE_TABLE_PRIMARY_KEY_CHECK
            Value::Null, // ASSIGN_GTIDS_TO_ANONYMOUS_TRANSACTIONS_TYPE
            s(""),       // ASSIGN_GTIDS_TO_ANONYMOUS_TRANSACTIONS_VALUE
        ]);
    }
    rows
}

/// performance_schema.replication_applier_status: per-channel SQL state.
fn replication_applier_status_rows() -> Vec<Vec<Value>> {
    let Some(mgr) = crate::replication::global_manager() else {
        return Vec::new();
    };
    let mut rows = Vec::new();
    for (_cfg, st) in mgr.snapshot_all_channels() {
        let service_state = if st.running { "ON" } else { "OFF" };
        rows.push(vec![
            s(&st.channel),
            s(service_state),
            Value::Int(0),  // REMAINING_DELAY
            Value::UInt(0), // COUNT_TRANSACTIONS_RETRIES
        ]);
    }
    rows
}

/// `Option<String>` → nullable string column value.
fn opt_s(value: Option<String>) -> Value {
    value.map(Value::String).unwrap_or(Value::Null)
}

/// `bigint unsigned` value (PFS counters / ids).
fn uval(value: u64) -> Value {
    Value::UInt(value)
}

/// `bigint` (signed) value.
fn ival(value: i64) -> Value {
    Value::Int(value)
}

/// Handle `TRUNCATE TABLE performance_schema.<table>` with MySQL semantics:
/// `setup_actors` / `setup_objects` have their rows removed; fixed-row config
/// tables and read-only state tables are not truncatable
/// (`ER_WRONG_PERFSCHEMA_USAGE`, 1683); event / summary tables clear collected
/// data — `events_statements_summary_by_digest` resets the digest accumulator,
/// and the remaining (still honest-empty) event / summary tables succeed as a
/// no-op until their instrumentation lands.
pub(crate) fn truncate(
    table: &str,
    registry: &SessionRegistry,
    user: &str,
    host: &str,
) -> Result<(), QueryError> {
    let lower = table.to_ascii_lowercase();
    if !registered(&lower) {
        return Err(error(
            crate::error::catalog::ER_NO_SUCH_TABLE,
            format!("Table 'performance_schema.{table}' doesn't exist"),
        ));
    }
    if truncate_command_denied(&lower) {
        return Err(error(
            crate::error::catalog::ER_TABLEACCESS_DENIED_ERROR,
            format!("TRUNCATE command denied to user '{user}'@'{host}' for table '{table}'"),
        ));
    }
    match lower.as_str() {
        "setup_actors" => crate::engine::perf_setup::truncate_actors(),
        "setup_objects" => crate::engine::perf_setup::truncate_objects(),
        "events_statements_summary_by_digest" => crate::engine::perf_digest::reset(),
        "events_statements_histogram_by_digest" | "events_statements_histogram_global" => {
            crate::engine::perf_digest::reset_histograms()
        }
        "events_errors_summary_global_by_error"
        | "events_errors_summary_by_account_by_error"
        | "events_errors_summary_by_host_by_error"
        | "events_errors_summary_by_thread_by_error"
        | "events_errors_summary_by_user_by_error" => crate::engine::perf_errors::truncate(&lower),
        "global_status" => {
            registry.flush_session_status();
            crate::mysql::connection_manager::reset_status_peaks();
            crate::engine::status_counters::record_flush_status();
        }
        "status_by_thread" => registry.truncate_thread_status(),
        "status_by_account" => registry.reset_account_status(),
        "status_by_host" => registry.reset_host_status(),
        "status_by_user" => registry.reset_user_status(),
        _ if crate::engine::perf_events::truncate(&lower) => {}
        "setup_consumers"
        | "setup_instruments"
        | "setup_threads"
        | "threads"
        | "processlist"
        | "global_variables"
        | "session_variables"
        | "variables_info"
        | "persisted_variables"
        | "variables_by_thread"
        | "user_variables_by_thread"
        | "session_status"
        | "data_locks"
        | "data_lock_waits"
        | "metadata_locks"
        | "table_handles"
        | "performance_timers" => {
            return Err(error(
                crate::error::catalog::ER_WRONG_PERFSCHEMA_USAGE,
                "Invalid performance_schema usage.".to_string(),
            ));
        }
        _ => {}
    }
    Ok(())
}

/// Tables whose MySQL 8.0.46 PFS ACL does not expose delete_all_rows. MySQL
/// rejects TRUNCATE with ER_TABLEACCESS_DENIED_ERROR (1142), before reaching
/// the storage-engine command handler.
fn truncate_command_denied(table: &str) -> bool {
    matches!(
        table,
        "cond_instances"
            | "data_lock_waits"
            | "data_locks"
            | "error_log"
            | "file_instances"
            | "global_variables"
            | "innodb_redo_log_files"
            | "keyring_component_status"
            | "keyring_keys"
            | "log_status"
            | "metadata_locks"
            | "mutex_instances"
            | "performance_timers"
            | "persisted_variables"
            | "processlist"
            | "replication_applier_configuration"
            | "replication_applier_filters"
            | "replication_applier_global_filters"
            | "replication_applier_status"
            | "replication_applier_status_by_coordinator"
            | "replication_applier_status_by_worker"
            | "replication_asynchronous_connection_failover"
            | "replication_asynchronous_connection_failover_managed"
            | "replication_connection_configuration"
            | "replication_connection_status"
            | "replication_group_member_stats"
            | "replication_group_members"
            | "rwlock_instances"
            | "session_account_connect_attrs"
            | "session_connect_attrs"
            | "session_status"
            | "session_variables"
            | "setup_consumers"
            | "setup_instruments"
            | "setup_threads"
            | "socket_instances"
            | "table_handles"
            | "threads"
            | "tls_channel_status"
            | "user_defined_functions"
            | "user_variables_by_thread"
            | "variables_by_thread"
            | "variables_info"
    )
}

#[cfg(test)]
mod catalog_tests {
    use super::*;

    #[test]
    fn generated_8046_catalog_and_extensions_are_fully_classified() {
        let definitions = definitions().collect::<Vec<_>>();
        assert_eq!(
            crate::engine::performance_schema_mysql80_generated::COMMUNITY_DEFS.len(),
            111
        );
        assert_eq!(definitions.len(), 125);
        assert_eq!(
            definitions
                .iter()
                .filter(|definition| {
                    compatibility(definition.name) == Some(PfsCompatibility::OptionalExtension)
                })
                .count(),
            14
        );
        let names = definitions
            .iter()
            .map(|definition| definition.name)
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(
            names.len(),
            definitions.len(),
            "system tables must have one definition"
        );
        for (actual, expected) in definitions
            .iter()
            .zip(crate::engine::performance_schema_mysql80_generated::COMMUNITY_DEFS)
        {
            assert_eq!(
                (actual.schema, actual.name, actual.engine, actual.create_sql),
                (
                    expected.schema,
                    expected.name,
                    expected.engine,
                    expected.create_sql
                )
            );
            let signature = |column: &SystemColumn| {
                (
                    column.name,
                    column.result_type,
                    column.data_type,
                    column.column_type_sql,
                    column.nullable,
                    column.char_len,
                    column.octet_len,
                    column.numeric_precision,
                    column.numeric_scale,
                    column.datetime_precision,
                )
            };
            assert_eq!(
                actual.columns.iter().map(signature).collect::<Vec<_>>(),
                expected.columns.iter().map(signature).collect::<Vec<_>>()
            );
        }
        for definition in definitions {
            assert!(
                producer(definition.name).is_some(),
                "{} must declare Real or HonestEmpty",
                definition.name
            );
        }
    }

    #[test]
    fn phase2_account_connect_attributes_preserve_username_case() {
        let registry = SessionRegistry::default();
        let upper = registry.register(9001, "p2Case".into(), "localhost".into(), "test".into());
        let lower = registry.register(9002, "p2case".into(), "localhost".into(), "test".into());
        for handle in [&upper, &lower] {
            handle.set_connection_attrs(std::collections::BTreeMap::from([(
                "client".into(),
                "gate".into(),
            )]));
        }
        let mut session = crate::engine::SessionContext::default();
        session.login_user = "p2case".into();
        let rows = crate::engine::expr::with_session_expr_info(&session, || {
            session_connect_attrs_rows(&registry, true)
        });
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::UInt(9002));
    }
}
