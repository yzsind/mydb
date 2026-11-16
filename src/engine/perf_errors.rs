//! Process-local error summary collector backing
//! `performance_schema.events_errors_summary_*`.
//!
//! Error recording is intentionally attached only to error paths and handled
//! stored-program conditions. Successful statements do not touch this module.

use std::collections::{btree_map::Entry, BTreeMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

use parking_lot::RwLock;

use crate::engine::api::SessionContext;
use crate::types::{QueryError, Value};

#[derive(Debug, Clone)]
struct ErrorStat {
    code: u16,
    name: &'static str,
    sql_state: String,
    raised: u64,
    handled: u64,
    first_seen: String,
    last_seen: String,
}

struct ErrorSample<'a> {
    error: &'a QueryError,
    raised: bool,
    handled: bool,
    now: &'a str,
}

impl ErrorStat {
    fn new(sample: &ErrorSample<'_>) -> Self {
        Self {
            code: sample.error.code(),
            name: sample
                .error
                .symbol()
                .unwrap_or("MYDB_ER_USER_DEFINED_CONDITION"),
            sql_state: sample.error.sql_state().to_string(),
            raised: u64::from(sample.raised),
            handled: u64::from(sample.handled),
            first_seen: sample.now.to_string(),
            last_seen: sample.now.to_string(),
        }
    }

    fn record(&mut self, sample: &ErrorSample<'_>) {
        if sample.raised {
            self.raised = self.raised.saturating_add(1);
        }
        if sample.handled {
            self.handled = self.handled.saturating_add(1);
        }
        sample.now.clone_into(&mut self.last_seen);
    }
}

struct ErrorSummary {
    global: BTreeMap<u16, ErrorStat>,
    by_account: BTreeMap<(String, String, u16), ErrorStat>,
    by_host: BTreeMap<(String, u16), ErrorStat>,
    by_thread: BTreeMap<(u64, u16), ErrorStat>,
    by_user: BTreeMap<(String, u16), ErrorStat>,
    total_keys: usize,
    max_keys: usize,
    _memory_charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

const MAX_ERROR_SUMMARY_KEYS: usize = 16_384;
static ERROR_SUMMARY_LOST: AtomicU64 = AtomicU64::new(0);

impl Default for ErrorSummary {
    fn default() -> Self {
        use crate::engine::executor::mem_budget::{
            ControlledMemoryCategory, ControlledMemoryCharge,
        };
        let desired = MAX_ERROR_SUMMARY_KEYS
            .saturating_mul(256)
            .saturating_add(std::mem::size_of::<Self>());
        let admitted = ControlledMemoryCharge::try_new_optional(
            ControlledMemoryCategory::PerformanceSchema,
            desired,
        );
        let (max_keys, charge) = if let Some(charge) = admitted {
            (MAX_ERROR_SUMMARY_KEYS, charge)
        } else {
            (
                0,
                ControlledMemoryCharge::try_new_optional(
                    ControlledMemoryCategory::PerformanceSchema,
                    0,
                )
                .expect("zero-byte PFS reservation cannot fail"),
            )
        };
        Self {
            global: BTreeMap::new(),
            by_account: BTreeMap::new(),
            by_host: BTreeMap::new(),
            by_thread: BTreeMap::new(),
            by_user: BTreeMap::new(),
            total_keys: 0,
            max_keys,
            _memory_charge: charge,
        }
    }
}

static SUMMARY: OnceLock<RwLock<ErrorSummary>> = OnceLock::new();

fn summary() -> &'static RwLock<ErrorSummary> {
    SUMMARY.get_or_init(|| RwLock::new(ErrorSummary::default()))
}

pub(crate) fn record_raised(session: &SessionContext, err: &QueryError) {
    record(session, err, true, false);
}

pub(crate) fn record_handled(session: &SessionContext, err: &QueryError) {
    // A handled condition was still raised; MySQL exposes both counters.
    record(session, err, true, true);
}

fn record(session: &SessionContext, err: &QueryError, raised: bool, handled: bool) {
    let now = now_ts6();
    summary().write().record(
        session,
        &ErrorSample {
            error: err,
            raised,
            handled,
            now: &now,
        },
    );
}

impl ErrorSummary {
    fn record(&mut self, session: &SessionContext, sample: &ErrorSample<'_>) {
        let code = sample.error.code();
        let Self {
            global,
            by_account,
            by_host,
            by_thread,
            by_user,
            total_keys,
            max_keys,
            ..
        } = self;
        // Preserve admission priority under the shared key cap. Existing keys
        // remain writable even when later dimensions cannot admit a new key.
        record_one(global, code, sample, total_keys, *max_keys);
        record_one(
            by_account,
            (session.user.clone(), session.host.clone(), code),
            sample,
            total_keys,
            *max_keys,
        );
        record_one(
            by_host,
            (session.host.clone(), code),
            sample,
            total_keys,
            *max_keys,
        );
        record_one(
            by_thread,
            (u64::from(session.connection_id), code),
            sample,
            total_keys,
            *max_keys,
        );
        record_one(
            by_user,
            (session.user.clone(), code),
            sample,
            total_keys,
            *max_keys,
        );
    }
}

fn record_one<K: Ord>(
    map: &mut BTreeMap<K, ErrorStat>,
    key: K,
    sample: &ErrorSample<'_>,
    total_keys: &mut usize,
    max_keys: usize,
) {
    match map.entry(key) {
        Entry::Occupied(mut entry) => entry.get_mut().record(sample),
        Entry::Vacant(entry) if *total_keys < max_keys => {
            entry.insert(ErrorStat::new(sample));
            *total_keys += 1;
        }
        Entry::Vacant(_) => {
            ERROR_SUMMARY_LOST.fetch_add(1, Ordering::Relaxed);
        }
    }
}

// Expand the complete row into vec!: each table gets one exact allocation,
// including the prefix-free case, without a temporary suffix vector or array.
macro_rules! stat_row {
    ($stat:ident $(, $prefix:expr)* $(,)?) => {
        vec![
            $($prefix,)*
            Value::Int($stat.code as i64),
            Value::String($stat.name.to_string()),
            Value::String($stat.sql_state.clone()),
            Value::UInt($stat.raised),
            Value::UInt($stat.handled),
            Value::String($stat.first_seen.clone()),
            Value::String($stat.last_seen.clone()),
        ]
    };
}

pub(crate) fn global_rows_filtered(error_number: Option<u16>) -> Vec<Vec<Value>> {
    let guard = summary().read();
    let catalog = crate::engine::performance_schema_mysql80_generated::ERROR_CATALOG;
    let mut rows = Vec::with_capacity(catalog.len().saturating_add(guard.global.len()));
    for &(number, name, sql_state) in catalog {
        if error_number.is_some_and(|wanted| number != Some(wanted)) {
            continue;
        }
        let stat = number.and_then(|number| guard.global.get(&number));
        rows.push(vec![
            number
                .map(|number| Value::Int(number as i64))
                .unwrap_or(Value::Null),
            name.map(|name| Value::String(name.to_string()))
                .unwrap_or(Value::Null),
            sql_state
                .map(|state| Value::String(state.to_string()))
                .unwrap_or(Value::Null),
            Value::UInt(stat.map_or(0, |stat| stat.raised)),
            Value::UInt(stat.map_or(0, |stat| stat.handled)),
            stat.map(|stat| Value::String(stat.first_seen.clone()))
                .unwrap_or(Value::Null),
            stat.map(|stat| Value::String(stat.last_seen.clone()))
                .unwrap_or(Value::Null),
        ]);
    }
    for (&number, stat) in &guard.global {
        if error_number.is_some_and(|wanted| number != wanted) {
            continue;
        }
        if !catalog
            .iter()
            .any(|(candidate, _, _)| *candidate == Some(number))
        {
            rows.push(stat_row!(stat));
        }
    }
    rows
}

pub(crate) fn account_rows() -> Vec<Vec<Value>> {
    summary()
        .read()
        .by_account
        .iter()
        .map(|((user, host, _), stat)| stat_row!(stat, s(user), s(host)))
        .collect()
}

pub(crate) fn host_rows() -> Vec<Vec<Value>> {
    summary()
        .read()
        .by_host
        .iter()
        .map(|((host, _), stat)| stat_row!(stat, s(host)))
        .collect()
}

pub(crate) fn thread_rows() -> Vec<Vec<Value>> {
    summary()
        .read()
        .by_thread
        .iter()
        .map(|((thread_id, _), stat)| stat_row!(stat, Value::UInt(*thread_id)))
        .collect()
}

pub(crate) fn user_rows() -> Vec<Vec<Value>> {
    summary()
        .read()
        .by_user
        .iter()
        .map(|((user, _), stat)| stat_row!(stat, s(user)))
        .collect()
}

pub(crate) fn truncate(table: &str) {
    let mut guard = summary().write();
    match table {
        "events_errors_summary_global_by_error" => {
            // Truncating the global summary also resets every dependent
            // dimension; truncating an individual dimension stays local.
            guard.global.clear();
            guard.by_account.clear();
            guard.by_host.clear();
            guard.by_thread.clear();
            guard.by_user.clear();
        }
        "events_errors_summary_by_account_by_error" => guard.by_account.clear(),
        "events_errors_summary_by_host_by_error" => guard.by_host.clear(),
        "events_errors_summary_by_thread_by_error" => guard.by_thread.clear(),
        "events_errors_summary_by_user_by_error" => guard.by_user.clear(),
        _ => unreachable!("validated error summary table"),
    }
    guard.total_keys = guard.global.len()
        + guard.by_account.len()
        + guard.by_host.len()
        + guard.by_thread.len()
        + guard.by_user.len();
}

#[cfg(test)]
pub(crate) fn reset() {
    *summary().write() = ErrorSummary::default();
    ERROR_SUMMARY_LOST.store(0, Ordering::Relaxed);
}

pub(crate) fn unregister_thread(thread_id: u64) {
    // Successful-only sessions must not initialize the collector/reservation.
    let Some(summary) = SUMMARY.get() else { return };
    let mut guard = summary.write();
    let removed = remove_thread_entries(&mut guard.by_thread, thread_id);
    guard.total_keys = guard.total_keys.saturating_sub(removed);
}

/// Error numbers form the suffix of each thread's ordered key range. Prefer
/// bounded point removal to scanning unrelated threads in larger maps.
fn remove_thread_entries<V>(entries: &mut BTreeMap<(u64, u16), V>, thread_id: u64) -> usize {
    const POINT_LIMIT: usize = 16;
    let before = entries.len();
    // Tiny maps need no range probe. For larger maps, retain's linear walk
    // remains preferable when one connection owns many error codes.
    if before > POINT_LIMIT * 4 {
        if entries
            .first_key_value()
            .is_some_and(|(key, _)| key.0 == thread_id)
            && entries
                .last_key_value()
                .is_some_and(|(key, _)| key.0 == thread_id)
        {
            entries.clear();
            return before;
        }
        let mut keys = [(0, 0); POINT_LIMIT];
        let mut count = 0;
        let mut dense = false;
        for (key, _) in entries.range((thread_id, 0)..=(thread_id, u16::MAX)) {
            if count == POINT_LIMIT {
                dense = true;
                break;
            }
            keys[count] = *key;
            count += 1;
        }
        if !dense {
            for key in &keys[..count] {
                entries.remove(key);
            }
            return count;
        }
    }
    entries.retain(|(candidate, _), _| *candidate != thread_id);
    before - entries.len()
}

fn s(value: &str) -> Value {
    Value::String(value.to_string())
}

fn now_ts6() -> String {
    chrono::Local::now()
        .format("%Y-%m-%d %H:%M:%S%.6f")
        .to_string()
}

#[cfg(test)]
#[path = "perf_errors/tests.rs"]
mod audit_tests;
