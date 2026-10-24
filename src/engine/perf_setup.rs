//! Writable `performance_schema` setup state and its DML handlers.
//!
//! MySQL lets a handful of setup tables be reconfigured at runtime to control
//! what the instrumentation collects:
//!
//! | table              | UPDATE columns      | INSERT / DELETE rows |
//! |--------------------|---------------------|----------------------|
//! | `setup_consumers`  | `ENABLED`           | no                   |
//! | `setup_instruments`| `ENABLED`, `TIMED`  | no                   |
//! | `setup_threads`    | `ENABLED`, `HISTORY`| no                   |
//! | `setup_actors`     | `ENABLED`, `HISTORY`| yes                  |
//! | `setup_objects`    | `ENABLED`, `TIMED`  | yes                  |
//!
//! Every other performance_schema table is read-only; DML against it raises
//! `ER_WRONG_PERFSCHEMA_USAGE` (1683), matching MySQL.
//!
//! The state is process-global and resets to defaults on restart — the same
//! lifetime as MySQL's in-memory performance_schema configuration, and
//! consistent with CLAUDE.md (runtime changes affect only this process).
//! `row_producers` in `perf_schema.rs` read this state so `SELECT`s reflect
//! live configuration.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU8, Ordering};
use std::sync::OnceLock;

use parking_lot::RwLock;

use crate::parser::{Assignment, BinaryOp, Expr, Literal};
use crate::storage::error;
use crate::types::{QueryError, Value};

/// Fast-path mirror of the `statements_digest` consumer's `ENABLED` flag.
/// `log_query` checks this single relaxed atomic on every statement, so a
/// server that never turns the consumer on pays one load and nothing else —
/// it never touches the consumer `RwLock` or the digest table. Kept in sync by
/// [`update_consumers`]; the initial value matches `default_consumers()`
/// (digest collection on, aligned with MySQL 8.0's factory default so that
/// `events_statements_summary_by_digest` and the `sys` views built on it are
/// non-empty on first query without an explicit `UPDATE setup_consumers`).
static STATEMENTS_DIGEST_ON: AtomicBool = AtomicBool::new(true);
static GLOBAL_INSTRUMENTATION_ON: AtomicBool = AtomicBool::new(true);
static THREAD_INSTRUMENTATION_ON: AtomicBool = AtomicBool::new(true);

pub(crate) const CONSUMER_STATEMENTS_CURRENT: u32 = 1 << 0;
pub(crate) const CONSUMER_STATEMENTS_HISTORY: u32 = 1 << 1;
pub(crate) const CONSUMER_STATEMENTS_HISTORY_LONG: u32 = 1 << 2;
pub(crate) const CONSUMER_WAITS_CURRENT: u32 = 1 << 3;
pub(crate) const CONSUMER_WAITS_HISTORY: u32 = 1 << 4;
pub(crate) const CONSUMER_WAITS_HISTORY_LONG: u32 = 1 << 5;

/// Fast-path mirror of the event consumers. A disabled default server pays one
/// relaxed load before skipping the PFS event collector. The initial value
/// matches `default_consumers()`: `events_statements_current` /
/// `events_statements_history` on (MySQL 8.0 factory default), everything else
/// off until its instrumentation lands.
static CONSUMER_MASK: AtomicU32 =
    AtomicU32::new(CONSUMER_STATEMENTS_CURRENT | CONSUMER_STATEMENTS_HISTORY);

// Bit 2 marks the common factory-default setup_actors state as valid;
// bits 0/1 are ENABLED/HISTORY. This avoids taking the actors RwLock and
// running two LIKE matches for every statement until the table is changed.
static ACTOR_DEFAULT_FAST: AtomicU8 = AtomicU8::new(0b111);

pub(crate) fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("MYDB_PERFORMANCE_SCHEMA")
            .ok()
            .as_deref()
            .map(|value| {
                !matches!(
                    value.to_ascii_lowercase().as_str(),
                    "0" | "off" | "false" | "no"
                )
            })
            .unwrap_or(true)
    })
}

pub(crate) fn configured_i64(name: &str, default: i64) -> i64 {
    let env_name = format!("MYDB_{}", name.to_ascii_uppercase());
    std::env::var(env_name)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(default)
}

pub(crate) fn configured_usize(name: &str, default: usize) -> usize {
    usize::try_from(configured_i64(name, default as i64)).unwrap_or(default)
}

/// True when the `statements_digest` consumer is enabled. The hot statement
/// path reads this single atomic instead of scanning the consumer `RwLock` on
/// every query.
pub(crate) fn statements_digest_enabled() -> bool {
    STATEMENTS_DIGEST_ON.load(Ordering::Relaxed)
}

pub(crate) fn consumer_enabled(name: &str) -> bool {
    state()
        .consumers
        .read()
        .iter()
        .find(|consumer| consumer.name.eq_ignore_ascii_case(name))
        .is_some_and(|consumer| consumer.enabled)
}

pub(crate) fn statement_consumer_mask() -> u32 {
    CONSUMER_MASK.load(Ordering::Relaxed)
        & (CONSUMER_STATEMENTS_CURRENT
            | CONSUMER_STATEMENTS_HISTORY
            | CONSUMER_STATEMENTS_HISTORY_LONG)
}

pub(crate) fn wait_consumer_mask() -> u32 {
    CONSUMER_MASK.load(Ordering::Relaxed)
        & (CONSUMER_WAITS_CURRENT | CONSUMER_WAITS_HISTORY | CONSUMER_WAITS_HISTORY_LONG)
}

pub(crate) fn global_instrumentation_enabled() -> bool {
    GLOBAL_INSTRUMENTATION_ON.load(Ordering::Relaxed)
}

pub(crate) fn thread_instrumentation_enabled() -> bool {
    THREAD_INSTRUMENTATION_ON.load(Ordering::Relaxed)
}

/// Refresh fast-path mirrors from the current consumer rows. Called after any
/// `setup_consumers` mutation.
fn refresh_consumer_flags(consumers: &[ConsumerRow]) {
    let enabled = |name: &str| {
        consumers
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
            .map(|c| c.enabled)
            .unwrap_or(false)
    };
    let digest_on = consumers
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case("statements_digest"))
        .map(|c| c.enabled)
        .unwrap_or(false);
    STATEMENTS_DIGEST_ON.store(digest_on, Ordering::Relaxed);
    GLOBAL_INSTRUMENTATION_ON.store(enabled("global_instrumentation"), Ordering::Relaxed);
    THREAD_INSTRUMENTATION_ON.store(enabled("thread_instrumentation"), Ordering::Relaxed);

    let mut mask = 0;
    if enabled("events_statements_current") {
        mask |= CONSUMER_STATEMENTS_CURRENT;
    }
    if enabled("events_statements_history") {
        mask |= CONSUMER_STATEMENTS_HISTORY;
    }
    if enabled("events_statements_history_long") {
        mask |= CONSUMER_STATEMENTS_HISTORY_LONG;
    }
    if enabled("events_waits_current") {
        mask |= CONSUMER_WAITS_CURRENT;
    }
    if enabled("events_waits_history") {
        mask |= CONSUMER_WAITS_HISTORY;
    }
    if enabled("events_waits_history_long") {
        mask |= CONSUMER_WAITS_HISTORY_LONG;
    }
    CONSUMER_MASK.store(mask, Ordering::Relaxed);
}

fn refresh_actor_fast(actors: &[ActorRow]) {
    let bits = match actors {
        [row] if row.host == "%" && row.user == "%" && row.role == "%" => {
            0b100 | u8::from(row.enabled) | (u8::from(row.history) << 1)
        }
        _ => 0,
    };
    ACTOR_DEFAULT_FAST.store(bits, Ordering::Release);
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct ConsumerRow {
    name: &'static str,
    enabled: bool,
}

#[derive(Clone)]
struct InstrumentRow {
    name: &'static str,
    enabled: bool,
    timed: Option<bool>,
    properties: &'static str,
    flags: Option<&'static str>,
    volatility: i32,
    documentation: Option<&'static str>,
}

#[derive(Clone)]
struct ActorRow {
    host: String,
    user: String,
    role: String,
    enabled: bool,
    history: bool,
}

#[derive(Clone)]
struct ObjectRow {
    object_type: String,
    schema: String,
    name: String,
    enabled: bool,
    timed: bool,
}

#[derive(Clone)]
struct ThreadClassRow {
    name: &'static str,
    enabled: bool,
    history: bool,
    properties: &'static str,
    volatility: i32,
    documentation: Option<&'static str>,
}

struct SetupState {
    consumers: RwLock<Vec<ConsumerRow>>,
    instruments: RwLock<Vec<InstrumentRow>>,
    actors: RwLock<Vec<ActorRow>>,
    objects: RwLock<Vec<ObjectRow>>,
    thread_classes: RwLock<Vec<ThreadClassRow>>,
    /// Dense lock-free mirrors indexed by the generated 8.0.46 class
    /// catalogs. Bit 0=enabled, bit 1=timed/history.
    instrument_flags: Vec<AtomicU8>,
    thread_flags: Vec<AtomicU8>,
    _memory_charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

fn state() -> &'static SetupState {
    static STATE: OnceLock<SetupState> = OnceLock::new();
    STATE.get_or_init(|| {
        let bytes = setup_capacity_bytes();
        let Some(memory_charge) =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::PerformanceSchema,
                bytes,
            )
        else {
            crate::util::logging::log_message(format_args!(
                "performance_schema: setup catalog disabled because {bytes} bytes could not be reserved"
            ));
            refresh_consumer_flags(&[]);
            refresh_actor_fast(&[]);
            return SetupState {
                consumers: RwLock::new(Vec::new()),
                instruments: RwLock::new(Vec::new()),
                actors: RwLock::new(Vec::new()),
                objects: RwLock::new(Vec::new()),
                thread_classes: RwLock::new(Vec::new()),
                instrument_flags: Vec::new(),
                thread_flags: Vec::new(),
                _memory_charge: crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::PerformanceSchema,
                    0,
                )
                .expect("zero-byte PFS reservation cannot fail"),
            };
        };
        let consumers = default_consumers();
        // Keep the lock-free fast-path mirrors in sync with the defaults so a
        // process that never mutates setup_consumers still gates collection
        // correctly (the static initializers above are the belt; this is the
        // suspenders should the defaults ever change).
        refresh_consumer_flags(&consumers);
        let instruments = default_instruments();
        let instrument_flags = instruments
            .iter()
            .map(|row| {
                AtomicU8::new(
                    u8::from(row.enabled)
                        | (u8::from(row.timed.unwrap_or(false)) << 1),
                )
            })
            .collect();
        let thread_classes = default_thread_classes();
        let thread_flags = thread_classes
            .iter()
            .map(|row| AtomicU8::new(u8::from(row.enabled) | (u8::from(row.history) << 1)))
            .collect();
        SetupState {
            consumers: RwLock::new(consumers),
            instruments: RwLock::new(instruments),
            actors: RwLock::new(default_actors()),
            objects: RwLock::new(default_objects()),
            thread_classes: RwLock::new(thread_classes),
            instrument_flags,
            thread_flags,
            _memory_charge: memory_charge,
        }
    })
}

fn setup_capacity_bytes() -> usize {
    use crate::engine::performance_schema_mysql80_generated::{
        SETUP_CONSUMERS_DEFAULTS, SETUP_INSTRUMENT_DEFAULTS, SETUP_THREAD_DEFAULTS,
    };
    let mut bytes = std::mem::size_of::<SetupState>()
        .saturating_add(
            SETUP_CONSUMERS_DEFAULTS
                .len()
                .saturating_mul(std::mem::size_of::<ConsumerRow>()),
        )
        .saturating_add(
            SETUP_INSTRUMENT_DEFAULTS
                .len()
                .saturating_mul(std::mem::size_of::<InstrumentRow>()),
        )
        .saturating_add(
            SETUP_THREAD_DEFAULTS
                .len()
                .saturating_mul(std::mem::size_of::<ThreadClassRow>()),
        )
        .saturating_add(
            SETUP_INSTRUMENT_DEFAULTS
                .len()
                .saturating_mul(std::mem::size_of::<AtomicU8>()),
        )
        .saturating_add(
            SETUP_THREAD_DEFAULTS
                .len()
                .saturating_mul(std::mem::size_of::<AtomicU8>()),
        )
        .saturating_add(std::mem::size_of::<ActorRow>() + 3);
    // setup_objects has exactly 5 object kinds × 4 schema patterns and is
    // constructed with matching capacity below.
    const OBJECT_KINDS: [&str; 5] = ["EVENT", "FUNCTION", "PROCEDURE", "TABLE", "TRIGGER"];
    const SCHEMAS: [&str; 4] = ["mysql", "performance_schema", "information_schema", "%"];
    bytes = bytes.saturating_add(
        20usize
            .saturating_mul(std::mem::size_of::<ObjectRow>())
            .saturating_add(
                OBJECT_KINDS
                    .iter()
                    .map(|kind| kind.len().saturating_mul(SCHEMAS.len()))
                    .sum::<usize>(),
            )
            .saturating_add(
                SCHEMAS
                    .iter()
                    .map(|schema| schema.len().saturating_mul(OBJECT_KINDS.len()))
                    .sum::<usize>(),
            )
            .saturating_add(20),
    );
    bytes
}

// ---------------------------------------------------------------------------
// Defaults — mirror MySQL 8.0's initial performance_schema configuration.
// ENABLED values reflect what mydb actually collects today (global/thread
// instrumentation on; event/digest consumers off until instrumentation lands),
// rather than overclaiming coverage.
// ---------------------------------------------------------------------------

fn default_consumers() -> Vec<ConsumerRow> {
    crate::engine::performance_schema_mysql80_generated::SETUP_CONSUMERS_DEFAULTS
        .iter()
        .map(|&(name, enabled)| ConsumerRow { name, enabled })
        .collect()
}

fn default_instruments() -> Vec<InstrumentRow> {
    crate::engine::performance_schema_mysql80_generated::SETUP_INSTRUMENT_DEFAULTS
        .iter()
        .map(|source| InstrumentRow {
            name: source.name,
            enabled: source.enabled,
            timed: source.timed,
            properties: source.properties,
            flags: source.flags,
            volatility: source.volatility,
            documentation: source.documentation,
        })
        .collect()
}

fn default_actors() -> Vec<ActorRow> {
    vec![ActorRow {
        host: "%".to_string(),
        user: "%".to_string(),
        role: "%".to_string(),
        enabled: true,
        history: true,
    }]
}

fn default_objects() -> Vec<ObjectRow> {
    let mut rows = Vec::with_capacity(20);
    for ot in ["EVENT", "FUNCTION", "PROCEDURE", "TABLE", "TRIGGER"] {
        for schema in ["mysql", "performance_schema", "information_schema"] {
            rows.push(ObjectRow {
                object_type: ot.to_string(),
                schema: schema.to_string(),
                name: "%".to_string(),
                enabled: false,
                timed: false,
            });
        }
        rows.push(ObjectRow {
            object_type: ot.to_string(),
            schema: "%".to_string(),
            name: "%".to_string(),
            enabled: true,
            timed: true,
        });
    }
    rows
}

fn default_thread_classes() -> Vec<ThreadClassRow> {
    crate::engine::performance_schema_mysql80_generated::SETUP_THREAD_DEFAULTS
        .iter()
        .map(|source| ThreadClassRow {
            name: source.name,
            enabled: source.enabled,
            history: source.history,
            properties: source.properties,
            volatility: source.volatility,
            documentation: source.documentation,
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Row producers (read by `perf_schema::rows`)
// ---------------------------------------------------------------------------

fn s(value: &str) -> Value {
    Value::String(value.to_string())
}

fn yn(value: bool) -> &'static str {
    if value {
        "YES"
    } else {
        "NO"
    }
}

fn yes_no(value: bool) -> Value {
    s(yn(value))
}

pub(crate) fn consumers_rows() -> Vec<Vec<Value>> {
    state()
        .consumers
        .read()
        .iter()
        .map(|c| vec![s(c.name), yes_no(c.enabled)])
        .collect()
}

pub(crate) fn instruments_rows() -> Vec<Vec<Value>> {
    state()
        .instruments
        .read()
        .iter()
        .map(|i| {
            vec![
                s(i.name),
                yes_no(i.enabled),
                i.timed.map(yes_no).unwrap_or(Value::Null),
                s(i.properties),
                i.flags.map(s).unwrap_or(Value::Null),
                Value::Int(i.volatility as i64),
                i.documentation.map(s).unwrap_or(Value::Null),
            ]
        })
        .collect()
}

pub(crate) fn instrument_enabled(name: &str) -> bool {
    instrument_index(name).is_some_and(instrument_enabled_at)
}

/// Resolve an immutable generated instrument name once at a caller-owned
/// cache boundary. The returned index remains stable for the process lifetime;
/// only its atomic flag is mutable through setup_instruments.
pub(crate) fn instrument_index(name: &str) -> Option<usize> {
    crate::engine::performance_schema_mysql80_generated::instrument_index(name)
}

pub(crate) fn instrument_enabled_at(index: usize) -> bool {
    state()
        .instrument_flags
        .get(index)
        .is_some_and(|flag| flag.load(Ordering::Relaxed) & 1 != 0)
}

pub(crate) fn thread_enabled(name: &str) -> bool {
    crate::engine::performance_schema_mysql80_generated::thread_class_index(name)
        .and_then(|index| state().thread_flags.get(index))
        .is_some_and(|flag| flag.load(Ordering::Relaxed) & 1 != 0)
}

pub(crate) fn thread_history_enabled(name: &str) -> bool {
    crate::engine::performance_schema_mysql80_generated::thread_class_index(name)
        .and_then(|index| state().thread_flags.get(index))
        .is_some_and(|flag| flag.load(Ordering::Relaxed) & 2 != 0)
}

pub(crate) fn actor_enabled(user: &str, host: &str) -> bool {
    let fast = ACTOR_DEFAULT_FAST.load(Ordering::Acquire);
    if fast & 0b100 != 0 {
        return fast & 1 != 0;
    }
    actor_flag(user, host, |row| row.enabled)
}

pub(crate) fn actor_history_enabled(user: &str, host: &str) -> bool {
    let fast = ACTOR_DEFAULT_FAST.load(Ordering::Acquire);
    if fast & 0b100 != 0 {
        return fast & 2 != 0;
    }
    actor_flag(user, host, |row| row.history)
}

fn actor_flag(user: &str, host: &str, flag: impl Fn(&ActorRow) -> bool) -> bool {
    state()
        .actors
        .read()
        .iter()
        .filter(|row| {
            like_match(user, &row.user) && like_match(host, &row.host) && like_match("%", &row.role)
        })
        .max_by_key(|row| {
            let literal_weight = |pattern: &str| {
                pattern
                    .bytes()
                    .filter(|byte| !matches!(byte, b'%' | b'_'))
                    .count()
            };
            (
                usize::from(row.user != "%")
                    + usize::from(row.host != "%")
                    + usize::from(row.role != "%"),
                literal_weight(&row.user) + literal_weight(&row.host) + literal_weight(&row.role),
            )
        })
        .map(flag)
        .unwrap_or(false)
}

pub(crate) fn actors_rows() -> Vec<Vec<Value>> {
    state()
        .actors
        .read()
        .iter()
        .map(|a| {
            vec![
                s(&a.host),
                s(&a.user),
                s(&a.role),
                yes_no(a.enabled),
                yes_no(a.history),
            ]
        })
        .collect()
}

pub(crate) fn objects_rows() -> Vec<Vec<Value>> {
    state()
        .objects
        .read()
        .iter()
        .map(|o| {
            vec![
                s(&o.object_type),
                s(&o.schema),
                s(&o.name),
                yes_no(o.enabled),
                yes_no(o.timed),
            ]
        })
        .collect()
}

pub(crate) fn thread_classes_rows() -> Vec<Vec<Value>> {
    state()
        .thread_classes
        .read()
        .iter()
        .map(|t| {
            vec![
                s(t.name),
                yes_no(t.enabled),
                yes_no(t.history),
                s(t.properties),
                Value::Int(t.volatility as i64),
                t.documentation.map(s).unwrap_or(Value::Null),
            ]
        })
        .collect()
}

// ---------------------------------------------------------------------------
// TRUNCATE support: setup_actors / setup_objects reset to defaults.
// ---------------------------------------------------------------------------

/// `TRUNCATE TABLE performance_schema.setup_actors` — MySQL removes all rows.
pub(crate) fn truncate_actors() {
    let mut actors = state().actors.write();
    actors.clear();
    refresh_actor_fast(&actors);
}

/// `TRUNCATE TABLE performance_schema.setup_objects` — MySQL removes all rows.
pub(crate) fn truncate_objects() {
    state().objects.write().clear();
}

// ---------------------------------------------------------------------------
// DML classification
// ---------------------------------------------------------------------------

pub(crate) enum Disposition {
    /// Table accepts DML (a writable setup table).
    Writable,
    /// Read-only performance_schema table: DML raises 1683.
    ReadOnly,
}

const WRITABLE: &[&str] = &[
    "setup_consumers",
    "setup_instruments",
    "setup_actors",
    "setup_objects",
    "setup_threads",
];

/// Classify a `performance_schema.<table>` for DML.
pub(crate) fn disposition(table: &str) -> Disposition {
    if WRITABLE.iter().any(|t| table.eq_ignore_ascii_case(t)) {
        Disposition::Writable
    } else {
        Disposition::ReadOnly
    }
}

fn invalid_usage() -> QueryError {
    error(
        crate::error::catalog::ER_WRONG_PERFSCHEMA_USAGE,
        "Invalid performance_schema usage.".to_string(),
    )
}

fn unsupported_where() -> QueryError {
    error(
        crate::error::catalog::ER_WRONG_PERFSCHEMA_USAGE,
        "Unsupported WHERE clause for performance_schema DML; only equality / IN / LIKE on \
         configuration columns combined with AND/OR is supported"
            .to_string(),
    )
}

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

/// Execute `UPDATE performance_schema.<table> SET ... [WHERE ...]`. Returns the
/// number of matched rows. The caller has already classified `table` as
/// writable.
pub(crate) fn update(
    table: &str,
    assignments: &[Assignment],
    selection: Option<&Expr>,
) -> Result<u64, QueryError> {
    validate_selection(table, selection)?;
    match table.to_ascii_lowercase().as_str() {
        "setup_consumers" => update_consumers(assignments, selection),
        "setup_instruments" => update_instruments(assignments, selection),
        "setup_actors" => update_actors(assignments, selection),
        "setup_objects" => update_objects(assignments, selection),
        "setup_threads" => update_thread_classes(assignments, selection),
        _ => Err(invalid_usage()),
    }
}

fn update_consumers(
    assignments: &[Assignment],
    selection: Option<&Expr>,
) -> Result<u64, QueryError> {
    let mut new_enabled = None;
    for a in assignments {
        match a.column.to_ascii_uppercase().as_str() {
            "ENABLED" => new_enabled = Some(parse_bool_assignment(&a.value)?),
            other => return Err(immutable_column(other)),
        }
    }
    let mut consumers = state().consumers.write();
    let mut affected = 0;
    for row in consumers.iter_mut() {
        let matched = {
            let logical = [("NAME", row.name), ("ENABLED", yn(row.enabled))];
            eval_predicate(selection, &logical)?
        };
        if matched {
            let before = row.enabled;
            if let Some(e) = new_enabled {
                row.enabled = e;
            }
            affected += u64::from(before != row.enabled);
        }
    }
    refresh_consumer_flags(&consumers);
    Ok(affected)
}

fn update_instruments(
    assignments: &[Assignment],
    selection: Option<&Expr>,
) -> Result<u64, QueryError> {
    let (mut new_enabled, mut new_timed) = (None, None);
    for a in assignments {
        match a.column.to_ascii_uppercase().as_str() {
            "ENABLED" => new_enabled = Some(parse_bool_assignment(&a.value)?),
            "TIMED" => new_timed = Some(parse_bool_assignment(&a.value)?),
            other => return Err(immutable_column(other)),
        }
    }
    let mut instruments = state().instruments.write();
    let mut affected = 0;
    for (index, row) in instruments.iter_mut().enumerate() {
        let matched = {
            let logical = [
                ("NAME", row.name),
                ("ENABLED", yn(row.enabled)),
                ("TIMED", row.timed.map(yn).unwrap_or("")),
            ];
            eval_predicate(selection, &logical)?
        };
        if matched {
            let before = (row.enabled, row.timed);
            if let Some(e) = new_enabled {
                row.enabled = e;
            }
            if let Some(t) = new_timed {
                if row.timed.is_some() {
                    row.timed = Some(t);
                }
            }
            affected += u64::from(before != (row.enabled, row.timed));
        }
        let flags = u8::from(row.enabled) | (u8::from(row.timed.unwrap_or(false)) << 1);
        state().instrument_flags[index].store(flags, Ordering::Relaxed);
    }
    Ok(affected)
}

fn update_actors(assignments: &[Assignment], selection: Option<&Expr>) -> Result<u64, QueryError> {
    let (mut new_enabled, mut new_history) = (None, None);
    for a in assignments {
        match a.column.to_ascii_uppercase().as_str() {
            "ENABLED" => new_enabled = Some(parse_bool_assignment(&a.value)?),
            "HISTORY" => new_history = Some(parse_bool_assignment(&a.value)?),
            other => return Err(immutable_column(other)),
        }
    }
    let mut actors = state().actors.write();
    let mut affected = 0;
    for row in actors.iter_mut() {
        let matched = {
            let logical = [
                ("HOST", row.host.as_str()),
                ("USER", row.user.as_str()),
                ("ROLE", row.role.as_str()),
                ("ENABLED", yn(row.enabled)),
                ("HISTORY", yn(row.history)),
            ];
            eval_predicate(selection, &logical)?
        };
        if matched {
            let before = (row.enabled, row.history);
            if let Some(e) = new_enabled {
                row.enabled = e;
            }
            if let Some(h) = new_history {
                row.history = h;
            }
            affected += u64::from(before != (row.enabled, row.history));
        }
    }
    refresh_actor_fast(&actors);
    Ok(affected)
}

fn update_objects(assignments: &[Assignment], selection: Option<&Expr>) -> Result<u64, QueryError> {
    let (mut new_enabled, mut new_timed) = (None, None);
    for a in assignments {
        match a.column.to_ascii_uppercase().as_str() {
            "ENABLED" => new_enabled = Some(parse_bool_assignment(&a.value)?),
            "TIMED" => new_timed = Some(parse_bool_assignment(&a.value)?),
            other => return Err(immutable_column(other)),
        }
    }
    let mut objects = state().objects.write();
    let mut affected = 0;
    for row in objects.iter_mut() {
        let matched = {
            let logical = [
                ("OBJECT_TYPE", row.object_type.as_str()),
                ("OBJECT_SCHEMA", row.schema.as_str()),
                ("OBJECT_NAME", row.name.as_str()),
                ("ENABLED", yn(row.enabled)),
                ("TIMED", yn(row.timed)),
            ];
            eval_predicate(selection, &logical)?
        };
        if matched {
            let before = (row.enabled, row.timed);
            if let Some(e) = new_enabled {
                row.enabled = e;
            }
            if let Some(t) = new_timed {
                row.timed = t;
            }
            affected += u64::from(before != (row.enabled, row.timed));
        }
    }
    Ok(affected)
}

fn update_thread_classes(
    assignments: &[Assignment],
    selection: Option<&Expr>,
) -> Result<u64, QueryError> {
    let (mut new_enabled, mut new_history) = (None, None);
    for a in assignments {
        match a.column.to_ascii_uppercase().as_str() {
            "ENABLED" => new_enabled = Some(parse_bool_assignment(&a.value)?),
            "HISTORY" => new_history = Some(parse_bool_assignment(&a.value)?),
            other => return Err(immutable_column(other)),
        }
    }
    let mut classes = state().thread_classes.write();
    let mut affected = 0;
    for (index, row) in classes.iter_mut().enumerate() {
        let matched = {
            let logical = [
                ("NAME", row.name),
                ("ENABLED", yn(row.enabled)),
                ("HISTORY", yn(row.history)),
            ];
            eval_predicate(selection, &logical)?
        };
        if matched {
            let before = (row.enabled, row.history);
            if let Some(e) = new_enabled {
                row.enabled = e;
            }
            if let Some(h) = new_history {
                row.history = h;
            }
            affected += u64::from(before != (row.enabled, row.history));
        }
        let flags = u8::from(row.enabled) | (u8::from(row.history) << 1);
        state().thread_flags[index].store(flags, Ordering::Relaxed);
    }
    Ok(affected)
}

// ---------------------------------------------------------------------------
// INSERT (setup_actors / setup_objects only)
// ---------------------------------------------------------------------------

pub(crate) fn insert(
    table: &str,
    columns: Option<&[String]>,
    rows: &[Vec<Expr>],
) -> Result<u64, QueryError> {
    match table.to_ascii_lowercase().as_str() {
        "setup_actors" => insert_actors(columns, rows),
        "setup_objects" => insert_objects(columns, rows),
        // setup_consumers / setup_instruments / setup_threads have a fixed row
        // set in MySQL; INSERT is rejected.
        _ => Err(invalid_usage()),
    }
}

fn column_index(columns: Option<&[String]>, name: &str, default: usize) -> Option<usize> {
    match columns {
        Some(cols) => cols.iter().position(|c| c.eq_ignore_ascii_case(name)),
        None => Some(default),
    }
}

fn insert_actors(columns: Option<&[String]>, rows: &[Vec<Expr>]) -> Result<u64, QueryError> {
    let mut actors = state().actors.write();
    let mut inserted = 0;
    for values in rows {
        let get = |name: &str, default: usize, fallback: &str| -> Result<String, QueryError> {
            match column_index(columns, name, default) {
                Some(idx) => values
                    .get(idx)
                    .map(literal_string)
                    .transpose()?
                    .ok_or_else(invalid_usage),
                None => Ok(fallback.to_string()),
            }
        };
        let host = get("HOST", 0, "%")?;
        let user = get("USER", 1, "%")?;
        let role = get("ROLE", 2, "%")?;
        let enabled = match column_index(columns, "ENABLED", 3) {
            Some(idx) => values.get(idx).map(parse_bool).transpose()?.unwrap_or(true),
            None => true,
        };
        let history = match column_index(columns, "HISTORY", 4) {
            Some(idx) => values.get(idx).map(parse_bool).transpose()?.unwrap_or(true),
            None => true,
        };
        if actors.iter().any(|row| {
            row.host.eq_ignore_ascii_case(&host)
                && row.user.eq_ignore_ascii_case(&user)
                && row.role.eq_ignore_ascii_case(&role)
        }) {
            return Err(crate::error::messages::duplicate_entry(
                &format!("{host}-{user}-{role}"),
                "PRIMARY",
            ));
        }
        actors.push(ActorRow {
            host,
            user,
            role,
            enabled,
            history,
        });
        inserted += 1;
    }
    refresh_actor_fast(&actors);
    Ok(inserted)
}

fn insert_objects(columns: Option<&[String]>, rows: &[Vec<Expr>]) -> Result<u64, QueryError> {
    let mut objects = state().objects.write();
    let mut inserted = 0;
    for values in rows {
        let object_type = match column_index(columns, "OBJECT_TYPE", 0) {
            Some(idx) => values
                .get(idx)
                .map(literal_string)
                .transpose()?
                .unwrap_or_else(|| "TABLE".to_string()),
            None => "TABLE".to_string(),
        };
        let schema = match column_index(columns, "OBJECT_SCHEMA", 1) {
            Some(idx) => values
                .get(idx)
                .map(literal_string)
                .transpose()?
                .unwrap_or_else(|| "%".to_string()),
            None => "%".to_string(),
        };
        let name = match column_index(columns, "OBJECT_NAME", 2) {
            Some(idx) => values
                .get(idx)
                .map(literal_string)
                .transpose()?
                .unwrap_or_else(|| "%".to_string()),
            None => "%".to_string(),
        };
        let enabled = match column_index(columns, "ENABLED", 3) {
            Some(idx) => values.get(idx).map(parse_bool).transpose()?.unwrap_or(true),
            None => true,
        };
        let timed = match column_index(columns, "TIMED", 4) {
            Some(idx) => values.get(idx).map(parse_bool).transpose()?.unwrap_or(true),
            None => true,
        };
        if objects.iter().any(|row| {
            row.object_type.eq_ignore_ascii_case(&object_type)
                && row.schema.eq_ignore_ascii_case(&schema)
                && row.name.eq_ignore_ascii_case(&name)
        }) {
            return Err(crate::error::messages::duplicate_entry(
                &format!("{object_type}-{schema}-{name}"),
                "PRIMARY",
            ));
        }
        objects.push(ObjectRow {
            object_type,
            schema,
            name,
            enabled,
            timed,
        });
        inserted += 1;
    }
    Ok(inserted)
}

// ---------------------------------------------------------------------------
// DELETE (setup_actors / setup_objects only)
// ---------------------------------------------------------------------------

pub(crate) fn delete(table: &str, selection: Option<&Expr>) -> Result<u64, QueryError> {
    validate_selection(table, selection)?;
    match table.to_ascii_lowercase().as_str() {
        "setup_actors" => {
            let mut actors = state().actors.write();
            let removed = delete_retaining(&mut actors, selection, |row| {
                [
                    ("HOST", row.host.clone()),
                    ("USER", row.user.clone()),
                    ("ROLE", row.role.clone()),
                    ("ENABLED", yn(row.enabled).to_string()),
                    ("HISTORY", yn(row.history).to_string()),
                ]
            })?;
            refresh_actor_fast(&actors);
            Ok(removed)
        }
        "setup_objects" => {
            let mut objects = state().objects.write();
            delete_retaining(&mut objects, selection, |row| {
                [
                    ("OBJECT_TYPE", row.object_type.clone()),
                    ("OBJECT_SCHEMA", row.schema.clone()),
                    ("OBJECT_NAME", row.name.clone()),
                    ("ENABLED", yn(row.enabled).to_string()),
                    ("TIMED", yn(row.timed).to_string()),
                ]
            })
        }
        _ => Err(invalid_usage()),
    }
}

/// Remove rows from `rows` that match `selection`, returning the count removed.
/// `to_logical` maps a row to its `(column, value)` pairs for predicate eval.
fn delete_retaining<T, const N: usize>(
    rows: &mut Vec<T>,
    selection: Option<&Expr>,
    to_logical: impl Fn(&T) -> [(&'static str, String); N],
) -> Result<u64, QueryError> {
    let mut removed = 0u64;
    let mut err: Option<QueryError> = None;
    rows.retain(|row| {
        if err.is_some() {
            return true;
        }
        let logical = to_logical(row);
        let pairs: Vec<(&str, &str)> = logical.iter().map(|(c, v)| (*c, v.as_str())).collect();
        match eval_predicate(selection, &pairs) {
            Ok(true) => {
                removed += 1;
                false // drop
            }
            Ok(false) => true, // keep
            Err(e) => {
                err = Some(e);
                true // keep on error
            }
        }
    });
    match err {
        Some(e) => Err(e),
        None => Ok(removed),
    }
}

// ---------------------------------------------------------------------------
// Restricted predicate evaluation for setup-table WHERE clauses.
// Supports `col = 'lit'`, `col != 'lit'`, `col LIKE 'pat'`, `col IN (..)`,
// `a AND b`, `a OR b`, and an absent predicate (matches all). All comparisons
// are case-insensitive over the configuration columns' string values.
// Unsupported shapes error explicitly (never a silent mismatch).
// ---------------------------------------------------------------------------

fn validate_selection(table: &str, selection: Option<&Expr>) -> Result<(), QueryError> {
    let columns: &[&str] = match table.to_ascii_lowercase().as_str() {
        "setup_consumers" => &["NAME", "ENABLED"],
        "setup_instruments" => &["NAME", "ENABLED", "TIMED"],
        "setup_threads" => &["NAME", "ENABLED", "HISTORY"],
        "setup_actors" => &["HOST", "USER", "ROLE", "ENABLED", "HISTORY"],
        "setup_objects" => &[
            "OBJECT_TYPE",
            "OBJECT_SCHEMA",
            "OBJECT_NAME",
            "ENABLED",
            "TIMED",
        ],
        _ => return Err(invalid_usage()),
    };
    fn validate(expr: &Expr, columns: &[&str]) -> Result<(), QueryError> {
        if let Some(name) = column_name(expr) {
            if !columns.iter().any(|col| col.eq_ignore_ascii_case(&name)) {
                return Err(crate::error::catalog::ER_BAD_FIELD_ERROR
                    .error(format!("Unknown column '{name}' in 'where clause'")));
            }
        }
        match expr {
            Expr::Binary { left, right, .. } => {
                validate(left, columns)?;
                validate(right, columns)?;
            }
            Expr::Like { expr, pattern, .. } => {
                validate(expr, columns)?;
                validate(pattern, columns)?;
            }
            Expr::InList { expr, list, .. } => {
                validate(expr, columns)?;
                for item in list {
                    validate(item, columns)?;
                }
            }
            Expr::Identifier(_)
            | Expr::CompoundIdentifier(_)
            | Expr::Literal(_)
            | Expr::BoundValue(_) => {}
            _ => return Err(unsupported_where()),
        }
        Ok(())
    }
    if let Some(expr) = selection {
        validate(expr, columns)?;
        let row: Vec<_> = columns.iter().map(|col| (*col, "")).collect();
        eval_expr(expr, &row)?;
    }
    Ok(())
}

fn eval_predicate(pred: Option<&Expr>, row: &[(&str, &str)]) -> Result<bool, QueryError> {
    match pred {
        None => Ok(true),
        Some(expr) => eval_expr(expr, row),
    }
}

fn eval_expr(expr: &Expr, row: &[(&str, &str)]) -> Result<bool, QueryError> {
    match expr {
        Expr::Binary { left, op, right } => match op {
            BinaryOp::And => Ok(eval_expr(left, row)? & eval_expr(right, row)?),
            BinaryOp::Or => Ok(eval_expr(left, row)? | eval_expr(right, row)?),
            BinaryOp::Eq | BinaryOp::NullSafeEq | BinaryOp::NotEq => {
                let (col, lit) = column_and_literal(left, right)?;
                let current = column_value(row, &col).unwrap_or("");
                let eq = current.eq_ignore_ascii_case(&lit);
                Ok(if matches!(op, BinaryOp::NotEq) {
                    !eq
                } else {
                    eq
                })
            }
            _ => Err(unsupported_where()),
        },
        Expr::Like {
            expr,
            pattern,
            negated,
            ..
        } => {
            let col = column_name(expr).ok_or_else(unsupported_where)?;
            let pat = literal_string(pattern)?;
            let current = column_value(row, &col).unwrap_or("");
            let matched = like_match(current, &pat);
            Ok(matched != *negated)
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let col = column_name(expr).ok_or_else(unsupported_where)?;
            let current = column_value(row, &col).unwrap_or("");
            let mut found = false;
            for item in list {
                found |= current.eq_ignore_ascii_case(&literal_string(item)?);
            }
            Ok(found != *negated)
        }
        _ => Err(unsupported_where()),
    }
}

fn column_value<'a>(row: &'a [(&str, &str)], name: &str) -> Option<&'a str> {
    row.iter()
        .find(|(c, _)| c.eq_ignore_ascii_case(name))
        .map(|(_, v)| *v)
}

fn column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(name) => Some(name.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().cloned(),
        _ => None,
    }
}

/// Extract `(column, literal)` from a comparison whichever side is which.
fn column_and_literal(left: &Expr, right: &Expr) -> Result<(String, String), QueryError> {
    if let Some(col) = column_name(left) {
        if let Ok(lit) = literal_string(right) {
            return Ok((col, lit));
        }
    }
    if let Some(col) = column_name(right) {
        if let Ok(lit) = literal_string(left) {
            return Ok((col, lit));
        }
    }
    Err(unsupported_where())
}

fn literal_string(expr: &Expr) -> Result<String, QueryError> {
    match expr {
        Expr::Literal(Literal::String(s)) => Ok(s.clone()),
        Expr::Literal(Literal::Int(i)) => Ok(i.to_string()),
        Expr::Literal(Literal::UInt(u)) => Ok(u.to_string()),
        Expr::Literal(Literal::Bool(b)) => Ok(if *b { "1" } else { "0" }.to_string()),
        Expr::Literal(Literal::Decimal(d)) => Ok(d.clone()),
        Expr::BoundValue(v) => Ok(v.to_display_string()),
        _ => Err(unsupported_where()),
    }
}

fn parse_bool(expr: &Expr) -> Result<bool, QueryError> {
    parse_bool_str(&literal_string(expr)?)
}

fn parse_bool_assignment(expr: &Expr) -> Result<bool, QueryError> {
    let raw = literal_string(expr).map_err(|_| invalid_usage())?;
    parse_bool_str(&raw)
}

fn parse_bool_str(raw: &str) -> Result<bool, QueryError> {
    match raw.trim().to_ascii_uppercase().as_str() {
        "YES" | "1" | "ON" | "TRUE" | "Y" => Ok(true),
        "NO" | "0" | "OFF" | "FALSE" | "N" => Ok(false),
        other => Err(error(
            crate::error::catalog::ER_WRONG_VALUE,
            format!("Incorrect value '{other}' (expected YES or NO)"),
        )),
    }
}

fn immutable_column(name: &str) -> QueryError {
    error(
        crate::error::catalog::ER_WRONG_PERFSCHEMA_USAGE,
        format!("column '{name}' of this performance_schema table is not updatable"),
    )
}

/// Case-insensitive SQL `LIKE`: `%` matches any sequence, `_` any single char.
///
/// B6: routes through the shared iterative kernel so
/// `performance_schema.setup_actors WHERE user LIKE '%%%%…X'` does not
/// blow the stack with O(2^k) backtracking. The old recursive matcher
/// was reachable from any authenticated client and was the DoS vector
/// flagged in the audit.
fn like_match(value: &str, pattern: &str) -> bool {
    // performance_schema names are ASCII by contract; CI matching
    // against `utf8mb4_general_ci` is behaviourally identical to the
    // old `to_ascii_lowercase` fold and avoids allocating twice.
    let info = crate::charset::resolve_collation("utf8mb4_general_ci")
        .unwrap_or_else(|_| crate::charset::default_collation_info());
    crate::engine::expr::match_like_pattern_with_info(value, pattern, None, info)
}

#[cfg(test)]
mod audit_tests {
    use super::*;

    #[test]
    fn setup_predicates_accept_bound_values() {
        let name = || Box::new(Expr::Identifier("NAME".into()));
        let bound = |value: &str| Expr::BoundValue(Value::String(value.into()));
        for predicate in [
            Expr::Binary {
                left: name(),
                op: BinaryOp::Eq,
                right: Box::new(bound("events_statements_history")),
            },
            Expr::Like {
                expr: name(),
                pattern: Box::new(bound("%statements_history%")),
                negated: false,
                escape_char: None,
            },
            Expr::InList {
                expr: name(),
                list: vec![bound("events_statements_history")],
                negated: false,
            },
        ] {
            validate_selection("setup_consumers", Some(&predicate)).unwrap();
            assert!(
                eval_predicate(Some(&predicate), &[("NAME", "events_statements_history")]).unwrap()
            );
            assert!(
                !eval_predicate(Some(&predicate), &[("NAME", "thread_instrumentation")]).unwrap()
            );
        }
    }

    #[test]
    fn pfs_audit_unknown_columns_fail_before_mutation_even_on_empty_tables() {
        for sql in [
            "UPDATE performance_schema.setup_consumers SET ENABLED='NO' WHERE MISSING='x'",
            "UPDATE performance_schema.setup_consumers SET ENABLED='NO' WHERE NAME='x' AND MISSING='x'",
            "UPDATE performance_schema.setup_consumers SET ENABLED='NO' WHERE NAME LIKE '%' OR MISSING IN ('x')",
        ] {
            let crate::parser::Statement::Update(update_stmt) = crate::parser::parse_statement(sql).unwrap() else { panic!("update"); };
            let before = consumers_rows();
            let err = update("setup_consumers", &update_stmt.assignments, update_stmt.selection.as_ref()).unwrap_err();
            assert_eq!(err.code(), 1054);
            assert_eq!(consumers_rows(), before);
        }
        truncate_actors();
        assert!(!actor_enabled("root", "localhost"));
        assert!(!actor_history_enabled("root", "localhost"));
        let missing = Expr::Binary {
            left: Box::new(Expr::Identifier("MISSING".into())),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::String("x".into()))),
        };
        assert_eq!(
            delete("setup_actors", Some(&missing)).unwrap_err().code(),
            1054
        );
        assert_eq!(
            update("setup_actors", &[], Some(&missing))
                .unwrap_err()
                .code(),
            1054
        );
    }
}
