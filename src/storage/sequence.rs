//! PostgreSQL-style SEQUENCE objects.
//!
//! Definitions and checkpointed recovery states live in catalog tables.
//! Nontransactional WAL reservations cover every value before it is returned.
//! Session CACHE blocks and per-sequence allocation avoid catalog writes on
//! the value path; checkpoints merge the latest logged state into the DD.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::{Condvar, Mutex, MutexGuard, RwLock};
use serde::{Deserialize, Serialize};

use super::core::error;
use crate::types::QueryError;

/// Additional WAL look-ahead steps. Session caches can add further gaps on
/// disconnect or restart; this is not a bound on total lost cached values.
pub(crate) fn is_sequence_function(name: &str) -> bool {
    ["nextval", "currval", "lastval", "setval"]
        .iter()
        .any(|candidate| name.eq_ignore_ascii_case(candidate))
}

pub fn sequence_wal_batch() -> Result<i64, QueryError> {
    match crate::util::env::var("MYDB_SEQUENCE_WAL_BATCH") {
        Ok(value) => value.parse::<i64>().ok().filter(|v| *v > 0).ok_or_else(|| {
            err_invalid("MYDB_SEQUENCE_WAL_BATCH must be an integer in 1..=9223372036854775807")
        }),
        Err(std::env::VarError::NotPresent) => Ok(32),
        Err(_) => Err(err_invalid("MYDB_SEQUENCE_WAL_BATCH is not valid UTF-8")),
    }
}

// ===========================================================================
// Definition
// ===========================================================================

/// Backing integer type of a sequence (`AS smallint|integer|bigint`), which
/// fixes the default MIN (ascending) / MAX (descending) and bounds explicit
/// MINVALUE / MAXVALUE.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SeqType {
    SmallInt,
    Integer,
    BigInt,
}

impl Default for SeqType {
    fn default() -> Self {
        SeqType::BigInt
    }
}

impl SeqType {
    pub fn min(self) -> i64 {
        match self {
            SeqType::SmallInt => i16::MIN as i64,
            SeqType::Integer => i32::MIN as i64,
            SeqType::BigInt => i64::MIN,
        }
    }

    pub fn max(self) -> i64 {
        match self {
            SeqType::SmallInt => i16::MAX as i64,
            SeqType::Integer => i32::MAX as i64,
            SeqType::BigInt => i64::MAX,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            SeqType::SmallInt => "smallint",
            SeqType::Integer => "integer",
            SeqType::BigInt => "bigint",
        }
    }

    /// Parse a PostgreSQL/MySQL type name (and common aliases) into a sequence
    /// backing type. Returns `None` for anything that is not a valid sequence
    /// data type.
    pub fn parse(name: &str) -> Option<SeqType> {
        match name.trim().to_ascii_lowercase().as_str() {
            "smallint" | "int2" => Some(SeqType::SmallInt),
            "integer" | "int" | "int4" => Some(SeqType::Integer),
            "bigint" | "int8" => Some(SeqType::BigInt),
            _ => None,
        }
    }
}

/// Persisted definition of a sequence — the catalog half. Mutated only by
/// CREATE / ALTER SEQUENCE; the live counter lives in [`SequenceManager`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceSchema {
    pub id: u64,
    /// Physical counter identity; RESTART replaces it without changing currval identity.
    pub counter_id: u64,
    pub database: String,
    pub name: String,
    pub as_type: SeqType,
    pub increment: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub start_value: i64,
    pub cache: i64,
    pub cycle: bool,
    /// `OWNED BY table.column`, if any (lifecycle linkage — M5).
    pub owned_table: Option<String>,
    pub owned_column: Option<String>,
    pub owned_table_id: Option<u64>,
    pub owned_column_id: Option<u32>,
    pub created_ms: u64,
    pub updated_ms: u64,
}

/// One option as parsed from CREATE/ALTER SEQUENCE. `Unset` means the clause
/// was absent; `NoBound` is the explicit `NO MINVALUE` / `NO MAXVALUE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BoundOpt {
    Unset,
    NoBound,
    Value(i64),
}

impl Default for BoundOpt {
    fn default() -> Self {
        BoundOpt::Unset
    }
}

/// Normalised CREATE/ALTER SEQUENCE option bag handed from the parser.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SeqOptions {
    pub as_type: Option<SeqType>,
    pub increment: Option<i64>,
    pub min_value: BoundOpt,
    pub max_value: BoundOpt,
    pub start: Option<i64>,
    pub cache: Option<i64>,
    pub cycle: Option<bool>,
    pub owned_table: Option<String>,
    pub owned_column: Option<String>,
    /// `OWNED BY NONE` was specified.
    pub owned_none: bool,
}

/// Resolve a CREATE (`base = None`) or ALTER (`base = Some(existing)`) option
/// bag into a fully-defaulted, validated [`SequenceSchema`]. Unspecified
/// options keep the base value on ALTER (PostgreSQL semantics) or fall back to
/// the type/direction default on CREATE.
#[allow(clippy::too_many_arguments)]
pub fn build_sequence_schema(
    id: u64,
    database: &str,
    name: &str,
    base: Option<&SequenceSchema>,
    opts: &SeqOptions,
    created_ms: u64,
    updated_ms: u64,
) -> Result<SequenceSchema, QueryError> {
    let as_type = opts
        .as_type
        .or_else(|| base.map(|b| b.as_type))
        .unwrap_or_default();
    let increment = opts
        .increment
        .or_else(|| base.map(|b| b.increment))
        .unwrap_or(1);
    if increment == 0 {
        return Err(err_invalid("INCREMENT must not be zero"));
    }
    let ascending = increment > 0;

    let min_value = match opts.min_value {
        BoundOpt::Value(v) => v,
        BoundOpt::NoBound => default_min(ascending, as_type),
        BoundOpt::Unset => base
            .map(|b| b.min_value)
            .unwrap_or_else(|| default_min(ascending, as_type)),
    };
    let max_value = match opts.max_value {
        BoundOpt::Value(v) => v,
        BoundOpt::NoBound => default_max(ascending, as_type),
        BoundOpt::Unset => base
            .map(|b| b.max_value)
            .unwrap_or_else(|| default_max(ascending, as_type)),
    };

    if min_value < as_type.min() || min_value > as_type.max() {
        return Err(err_invalid(format!(
            "MINVALUE ({min_value}) is out of range for sequence data type {}",
            as_type.as_str()
        )));
    }
    if max_value < as_type.min() || max_value > as_type.max() {
        return Err(err_invalid(format!(
            "MAXVALUE ({max_value}) is out of range for sequence data type {}",
            as_type.as_str()
        )));
    }
    if min_value > max_value {
        return Err(err_invalid(format!(
            "MINVALUE ({min_value}) must be less than MAXVALUE ({max_value})"
        )));
    }

    let start_value = match opts.start {
        Some(v) => v,
        None => {
            base.map(|b| b.start_value)
                .unwrap_or(if ascending { min_value } else { max_value })
        }
    };
    if start_value < min_value || start_value > max_value {
        return Err(err_invalid(format!(
            "START value ({start_value}) cannot be less than MINVALUE ({min_value}) \
             or greater than MAXVALUE ({max_value})"
        )));
    }

    let cache = opts.cache.or_else(|| base.map(|b| b.cache)).unwrap_or(1);
    if cache < 1 {
        return Err(err_invalid(format!(
            "CACHE ({cache}) must be greater than zero"
        )));
    }
    let cycle = opts
        .cycle
        .or_else(|| base.map(|b| b.cycle))
        .unwrap_or(false);

    let (owned_table, owned_column) = if opts.owned_none {
        (None, None)
    } else if opts.owned_table.is_some() {
        (opts.owned_table.clone(), opts.owned_column.clone())
    } else {
        (
            base.and_then(|b| b.owned_table.clone()),
            base.and_then(|b| b.owned_column.clone()),
        )
    };

    Ok(SequenceSchema {
        id,
        counter_id: base.map_or(id, |definition| definition.counter_id),
        database: database.to_string(),
        name: name.to_string(),
        as_type,
        increment,
        min_value,
        max_value,
        start_value,
        cache,
        cycle,
        owned_table,
        owned_column,
        owned_table_id: base.and_then(|b| b.owned_table_id),
        owned_column_id: base.and_then(|b| b.owned_column_id),
        created_ms,
        updated_ms,
    })
}

fn default_min(ascending: bool, as_type: SeqType) -> i64 {
    if ascending {
        1
    } else {
        as_type.min()
    }
}

fn default_max(ascending: bool, as_type: SeqType) -> i64 {
    if ascending {
        as_type.max()
    } else {
        -1
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SequenceValue {
    pub sequence_id: u64,
    pub counter_id: u64,
    pub resume_value: i64,
    pub is_called: bool,
    pub state_lsn: u64,
}

impl SequenceValue {
    pub(crate) fn to_row(self) -> Vec<crate::types::Value> {
        use crate::types::Value;
        vec![
            Value::UInt(self.counter_id),
            Value::UInt(self.sequence_id),
            Value::Int(self.resume_value),
            Value::Int(i64::from(self.is_called)),
            Value::UInt(self.state_lsn),
        ]
    }

    pub(crate) fn from_row(row: &[crate::types::Value]) -> Result<Self, QueryError> {
        use crate::types::Value;
        let invalid = || err_invalid("invalid sequence catalog state");
        if row.len() != 5 {
            return Err(invalid());
        }
        let unsigned = |index| match row.get(index) {
            Some(Value::UInt(v)) => Ok(*v),
            Some(Value::Int(v)) if *v >= 0 => Ok(*v as u64),
            _ => Err(invalid()),
        };
        let counter_id = unsigned(0)?;
        let sequence_id = unsigned(1)?;
        let resume_value = match row[2] {
            Value::Int(v) => v,
            Value::UInt(v) if v <= i64::MAX as u64 => v as i64,
            _ => return Err(invalid()),
        };
        let called = unsigned(3)?;
        if counter_id == 0 || sequence_id == 0 || called > 1 {
            return Err(invalid());
        }
        Ok(Self {
            counter_id,
            sequence_id,
            resume_value,
            is_called: called != 0,
            state_lsn: unsigned(4)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SequenceSyncConfig {
    pub remote_apply: bool,
    pub replicas: Vec<String>,
    pub epoch: u64,
}

impl Default for SequenceSyncConfig {
    fn default() -> Self {
        Self {
            remote_apply: false,
            replicas: Vec::new(),
            epoch: 1,
        }
    }
}

impl SequenceSyncConfig {
    pub(crate) fn from_env() -> Result<Option<Self>, QueryError> {
        let policy = crate::util::env::var("MYDB_SEQUENCE_SYNC_POLICY").ok();
        let members = crate::util::env::var("MYDB_SEQUENCE_SYNC_REPLICAS").ok();
        if policy.is_none() && members.is_none() {
            return Ok(None);
        }
        let remote_apply = match policy.as_deref().unwrap_or("local") {
            "local" => false,
            "remote_apply" => true,
            _ => {
                return Err(err_invalid(
                    "MYDB_SEQUENCE_SYNC_POLICY must be local or remote_apply",
                ))
            }
        };
        let mut replicas: Vec<_> = members
            .unwrap_or_default()
            .split(',')
            .filter(|s| !s.trim().is_empty())
            .map(|s| s.trim().to_ascii_lowercase())
            .collect();
        replicas.sort();
        let result = Self {
            remote_apply,
            replicas,
            epoch: 1,
        };
        result.validate()?;
        Ok(Some(result))
    }

    pub(crate) fn validate(&self) -> Result<(), QueryError> {
        if self.epoch == 0
            || self.replicas.len() > 64
            || self.replicas.windows(2).any(|w| w[0] >= w[1])
            || self
                .replicas
                .iter()
                .any(|id| crate::repl::role::uuid_string_to_bytes(id).is_none())
            || (self.remote_apply && self.replicas.is_empty())
            || (!self.remote_apply && !self.replicas.is_empty())
        {
            return Err(err_invalid(
                "invalid sequence sync policy, epoch or replica UUID set",
            ));
        }
        Ok(())
    }
}

impl SequenceRuntime {
    pub(crate) fn status(&self) -> (Option<i64>, bool) {
        let g = self.inner.lock();
        (
            g.is_called.then_some(g.current),
            g.working || g.pending.is_some(),
        )
    }

    pub(crate) fn definition(&self) -> Arc<SequenceSchema> {
        self.inner.lock().def.clone()
    }

    pub(crate) fn reset_to_logged(&self) {
        let mut inner = self.inner.lock();
        let value = *self.logged.lock();
        inner.current = value.resume_value;
        inner.durable = value.resume_value;
        inner.is_called = value.is_called;
        inner.pending = None;
    }
    pub(crate) fn apply_value(&self, value: SequenceValue) -> Result<(), QueryError> {
        let mut inner = self.inner.lock();
        let mut logged = self.logged.lock();
        if value.sequence_id != inner.def.id || value.counter_id != self.counter_id {
            return Err(err_invalid("sequence generation mismatch"));
        }
        if value.state_lsn < logged.state_lsn {
            return Ok(());
        }
        if value.state_lsn == logged.state_lsn {
            if value != *logged {
                return Err(err_invalid("conflicting sequence state at the same LSN"));
            }
            return Ok(());
        }
        if value.resume_value < inner.def.min_value || value.resume_value > inner.def.max_value {
            return Err(err_invalid(
                "replicated sequence state is outside its bounds",
            ));
        }
        inner.current = value.resume_value;
        inner.durable = value.resume_value;
        inner.is_called = value.is_called;
        inner.pending = None;
        *logged = value;
        Ok(())
    }
}

// ===========================================================================
// Runtime counter
// ===========================================================================

/// Outcome of a reservation: a contiguous arithmetic block `start, start+inc,
/// … (count values)`. `count` may be shorter than requested when a cycle wrap
/// or a NO CYCLE boundary is reached mid-block (so a session cache stays a
/// clean arithmetic progression).
#[derive(Debug, Clone, Copy)]
pub struct SeqAlloc {
    pub start: i64,
    pub count: i64,
    pub increment: i64,
}

impl SeqAlloc {
    #[cfg(test)]
    /// Last value in the block (the new `currval` source).
    pub fn last(&self) -> i64 {
        (i128::from(self.start) + i128::from(self.count - 1) * i128::from(self.increment)) as i64
    }
}

#[derive(Debug)]
struct SeqInner {
    def: Arc<SequenceSchema>,
    /// When `is_called`, the last value handed out; otherwise the value the
    /// next `nextval` will return (i.e. `start` immediately after CREATE).
    current: i64,
    /// Recovery reservation already written to durable WAL. Always covers
    /// `current` along the increment direction.
    durable: i64,
    is_called: bool,
    pending: Option<(i64, bool)>,
    working: bool,
}

/// Per-sequence live counter, guarded by its own lock so distinct sequences
/// never contend.
#[derive(Debug)]
pub(crate) struct SequenceRuntime {
    inner: Mutex<SeqInner>,
    changed: Condvar,
    pub(crate) logged: Mutex<SequenceValue>,
    pub(crate) queued_epoch: AtomicU64,
    pub(crate) retired: AtomicBool,
    pub(crate) counter_id: u64,
    memory_charge: Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
}

enum Step {
    Value(i64),
    Boundary,
}

/// Compute the value after `current` for one step, honouring bounds and CYCLE.
fn step(current: i64, inc: i64, min: i64, max: i64, cycle: bool) -> Step {
    if inc > 0 {
        match current.checked_add(inc) {
            Some(n) if n <= max => Step::Value(n),
            _ if cycle => Step::Value(min),
            _ => Step::Boundary,
        }
    } else {
        match current.checked_add(inc) {
            Some(n) if n >= min => Step::Value(n),
            _ if cycle => Step::Value(max),
            _ => Step::Boundary,
        }
    }
}

impl SequenceRuntime {
    fn new(def: Arc<SequenceSchema>, current: i64, durable: i64, is_called: bool) -> Arc<Self> {
        Arc::new(SequenceRuntime {
            counter_id: def.counter_id,
            changed: Condvar::new(),
            memory_charge: None,
            logged: Mutex::new(SequenceValue {
                sequence_id: def.id,
                counter_id: def.counter_id,
                resume_value: durable,
                is_called,
                state_lsn: 0,
            }),
            queued_epoch: AtomicU64::new(0),
            retired: AtomicBool::new(false),
            inner: Mutex::new(SeqInner {
                def,
                current,
                durable,
                is_called,
                pending: None,
                working: false,
            }),
        })
    }

    fn persist_unlocked(
        &self,
        g: &mut MutexGuard<'_, SeqInner>,
        value: i64,
        called: bool,
        persist: &mut impl FnMut(i64, bool) -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        g.working = true;
        let result = MutexGuard::unlocked(g, || persist(value, called));
        g.working = false;
        self.changed.notify_all();
        result
    }

    /// Reserve a contiguous arithmetic block. Only its short publication is
    /// locked; durability waits leave definition and checkpoint readers free.
    pub(crate) fn alloc(
        &self,
        want: i64,
        batch: i64,
        mut persist: impl FnMut(i64, bool) -> Result<(), QueryError>,
    ) -> Result<SeqAlloc, QueryError> {
        let want = want.max(1);
        let mut g = self.inner.lock();
        while g.working {
            self.changed.wait(&mut g);
        }
        if let Some((value, called)) = g.pending {
            self.persist_unlocked(&mut g, value, called, &mut persist)?;
            g.current = value;
            g.durable = value;
            g.is_called = called;
            g.pending = None;
        }
        let def = g.def.clone();
        let inc = def.increment;
        let ascending = inc > 0;

        // Stage the allocation until its durable reservation succeeds. An IO
        // failure must not make a subsequent call use an unpersisted window.
        let was_called = g.is_called;
        let first = if !was_called {
            g.current
        } else {
            match step(g.current, inc, def.min_value, def.max_value, def.cycle) {
                Step::Value(v) => v,
                Step::Boundary => return Err(err_reached(&def, ascending)),
            }
        };
        let wrapped = was_called && Some(first) != g.current.checked_add(inc);
        let bound = if ascending {
            def.max_value
        } else {
            def.min_value
        };
        let available = (i128::from(bound) - i128::from(first)).abs() / i128::from(inc).abs() + 1;
        let count = i128::from(want).min(available) as i64;
        let current = (i128::from(first) + i128::from(count - 1) * i128::from(inc)) as i64;

        // A wrap starts a new reservation interval; numeric comparison with
        // the previous cycle's highwater is no longer meaningful.
        let need_persist = !was_called
            || wrapped
            || if ascending {
                current > g.durable
            } else {
                current < g.durable
            };
        if need_persist {
            let extra = i128::from(batch.max(1)).min(available - i128::from(count));
            let new_durable = (i128::from(current) + extra * i128::from(inc)) as i64;
            let before_lsn = self.logged.lock().state_lsn;
            if let Err(err) = self.persist_unlocked(&mut g, new_durable, true, &mut persist) {
                if self.logged.lock().state_lsn > before_lsn {
                    g.pending = Some((new_durable, true));
                }
                return Err(err);
            }
            g.durable = new_durable;
        }
        g.current = current;
        g.is_called = true;

        Ok(SeqAlloc {
            start: first,
            count,
            increment: inc,
        })
    }

    /// `setval(value, is_called)` — non-transactional reset. Persists the exact
    /// value (no look-ahead) so `currval` and recovery see it precisely.
    pub(crate) fn setval(
        &self,
        value: i64,
        is_called: bool,
        mut persist: impl FnMut(i64, bool) -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        let mut g = self.inner.lock();
        while g.working {
            self.changed.wait(&mut g);
        }
        if value < g.def.min_value || value > g.def.max_value {
            return Err(err_invalid(format!(
                "setval: value {value} is out of bounds for sequence \"{}\" ({}..{})",
                g.def.name, g.def.min_value, g.def.max_value
            )));
        }
        let before_lsn = self.logged.lock().state_lsn;
        if let Err(err) = self.persist_unlocked(&mut g, value, is_called, &mut persist) {
            if self.logged.lock().state_lsn > before_lsn {
                g.pending = Some((value, is_called));
            }
            return Err(err);
        }
        g.pending = None;
        g.current = value;
        g.is_called = is_called;
        g.durable = value;
        Ok(())
    }

    pub(crate) fn ddl_value(&self) -> (i64, bool) {
        let state = self.inner.lock();
        (state.current, state.is_called)
    }

    /// Publish metadata-only changes without touching allocated values.
    /// Generator changes install a new generation after DD commit.
    pub(crate) fn set_def(&self, def: Arc<SequenceSchema>) {
        let mut g = self.inner.lock();
        while g.working {
            self.changed.wait(&mut g);
        }
        g.def = def;
    }

    #[cfg(test)]
    /// `last_value` for introspection (`pg_sequences.last_value`), `None` until
    /// the sequence has been read from.
    pub(crate) fn last_value(&self) -> Option<i64> {
        let g = self.inner.lock();
        if g.is_called {
            Some(g.current)
        } else {
            None
        }
    }
}

/// Process-wide registry of live sequence counters, keyed by sequence id.
#[derive(Default, Debug)]
pub(crate) struct SequenceManager {
    map: RwLock<HashMap<u64, Arc<SequenceRuntime>>>,
    owners: RwLock<HashMap<u64, std::collections::HashSet<u64>>>,
    pub(crate) role_epoch: AtomicU64,
    pub(crate) active_writes: AtomicUsize,
}

impl SequenceManager {
    fn track_owner(&self, id: u64, old: Option<u64>, new: Option<u64>) {
        if old == new {
            return;
        }
        let mut owners = self.owners.write();
        if let Some(old) = old {
            if let Some(ids) = owners.get_mut(&old) {
                ids.remove(&id);
                if ids.is_empty() {
                    owners.remove(&old);
                }
            }
        }
        if let Some(new) = new {
            owners.entry(new).or_default().insert(id);
        }
    }

    pub(crate) fn has_owned_sequences(&self) -> bool {
        !self.owners.read().is_empty()
    }

    pub(crate) fn owned_definitions(&self, tables: &[u64]) -> Vec<Arc<SequenceSchema>> {
        let ids: std::collections::BTreeSet<_> = {
            let owners = self.owners.read();
            tables
                .iter()
                .filter_map(|id| owners.get(id))
                .flatten()
                .copied()
                .collect()
        };
        ids.into_iter()
            .filter_map(|id| self.get(id))
            .map(|rt| rt.definition())
            .collect()
    }

    pub(crate) fn reset_for_promotion(&self) {
        for runtime in self.map.read().values() {
            runtime.reset_to_logged();
        }
    }

    pub(crate) fn retain(&self, valid: &std::collections::HashSet<u64>) {
        self.map.write().retain(|id, rt| {
            if valid.contains(id) {
                true
            } else {
                self.track_owner(*id, rt.definition().owned_table_id, None);
                rt.retired.store(true, Ordering::Release);
                false
            }
        });
    }
    /// Install (or replace) the runtime counter for a freshly created or
    /// loaded sequence.
    pub(crate) fn install(
        &self,
        def: Arc<SequenceSchema>,
        current: i64,
        durable: i64,
        is_called: bool,
    ) -> Result<Arc<SequenceRuntime>, QueryError> {
        if let Some(rt) = self.get(def.id) {
            if rt.counter_id == def.counter_id {
                return Ok(rt);
            }
        }
        let prepared = self.prepare(def, current, durable, is_called)?;
        Ok(self.publish_prepared(prepared))
    }

    pub(crate) fn prepare(
        &self,
        def: Arc<SequenceSchema>,
        current: i64,
        durable: i64,
        is_called: bool,
    ) -> Result<Arc<SequenceRuntime>, QueryError> {
        let charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
            1024 + def.database.capacity() + def.name.capacity(),
        )?;
        let mut rt = SequenceRuntime::new(def, current, durable, is_called);
        Arc::get_mut(&mut rt)
            .expect("unpublished sequence")
            .memory_charge = Some(charge);
        self.map.write().try_reserve(1).map_err(|_| {
            error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "sequence registry allocation failed",
            )
        })?;
        Ok(rt)
    }

    pub(crate) fn publish_prepared(&self, rt: Arc<SequenceRuntime>) -> Arc<SequenceRuntime> {
        let id = rt.definition().id;
        let mut map = self.map.write();
        if let Some(existing) = map.get(&id) {
            if existing.counter_id == rt.counter_id {
                return existing.clone();
            }
        }
        let old = map.insert(id, rt.clone());
        self.track_owner(
            id,
            old.as_ref().and_then(|rt| rt.definition().owned_table_id),
            rt.definition().owned_table_id,
        );
        if let Some(old) = old {
            old.retired.store(true, Ordering::Release);
        }
        rt
    }

    pub(crate) fn get(&self, id: u64) -> Option<Arc<SequenceRuntime>> {
        self.map.read().get(&id).cloned()
    }

    pub(crate) fn remove(&self, id: u64) {
        if let Some(old) = self.map.write().remove(&id) {
            self.track_owner(id, old.definition().owned_table_id, None);
            old.retired.store(true, Ordering::Release);
        }
    }

    /// Replace the cached definition (after ALTER) without disturbing the live
    /// counter.
    pub(crate) fn update_def(&self, def: Arc<SequenceSchema>) {
        if let Some(rt) = self.get(def.id) {
            let previous = rt.definition().owned_table_id;
            self.track_owner(def.id, previous, def.owned_table_id);
            rt.set_def(def);
        }
    }
}

// ===========================================================================
// Per-session state (currval / lastval / CACHE blocks)
// ===========================================================================

/// A session's reserved CACHE block for one sequence (a clean arithmetic
/// progression).
#[derive(Debug, Clone, Copy)]
pub struct SessionSeqCache {
    pub counter_id: u64,
    pub role_epoch: u64,
    pub next: i64,
    pub remaining: i64,
    pub increment: i64,
}

mod session;
pub use session::SessionSeqState;

// ===========================================================================
// Errors (MySQL code + PostgreSQL-aligned SQLSTATE / wording)
// ===========================================================================

pub fn err_seq_exists(name: &str) -> QueryError {
    error(
        crate::error::catalog::MYDB_ER_SEQUENCE_EXISTS,
        format!("relation \"{name}\" already exists"),
    )
}

pub fn err_unknown_seq(name: &str) -> QueryError {
    error(
        crate::error::catalog::MYDB_ER_SEQUENCE_NOT_FOUND,
        format!("relation \"{name}\" does not exist"),
    )
}

fn err_reached(def: &SequenceSchema, ascending: bool) -> QueryError {
    if ascending {
        error(
            crate::error::catalog::MYDB_ER_SEQUENCE_LIMIT_REACHED,
            format!(
                "nextval: reached maximum value of sequence \"{}\" ({})",
                def.name, def.max_value
            ),
        )
    } else {
        error(
            crate::error::catalog::MYDB_ER_SEQUENCE_LIMIT_REACHED,
            format!(
                "nextval: reached minimum value of sequence \"{}\" ({})",
                def.name, def.min_value
            ),
        )
    }
}

pub fn err_currval_undefined(name: &str) -> QueryError {
    error(
        crate::error::catalog::MYDB_ER_SEQUENCE_VALUE_UNDEFINED,
        format!("currval of sequence \"{name}\" is not yet defined in this session"),
    )
}

pub fn err_lastval_undefined() -> QueryError {
    error(
        crate::error::catalog::MYDB_ER_SEQUENCE_VALUE_UNDEFINED,
        "lastval is not yet defined in this session".to_string(),
    )
}

fn err_invalid(msg: impl Into<String>) -> QueryError {
    error(crate::error::catalog::MYDB_ER_INVALID_SEQUENCE, msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema(
        increment: i64,
        min: i64,
        max: i64,
        start: i64,
        cache: i64,
        cycle: bool,
    ) -> Arc<SequenceSchema> {
        Arc::new(SequenceSchema {
            counter_id: 1,
            id: 1,
            database: "test".into(),
            name: "s".into(),
            as_type: SeqType::BigInt,
            increment,
            min_value: min,
            max_value: max,
            start_value: start,
            cache,
            cycle,
            owned_table: None,
            owned_column: None,
            owned_table_id: None,
            owned_column_id: None,
            created_ms: 0,
            updated_ms: 0,
        })
    }

    fn drain(rt: &SequenceRuntime, n: usize) -> Vec<i64> {
        let mut out = Vec::new();
        let mut pending: Vec<i64> = Vec::new();
        while out.len() < n {
            if pending.is_empty() {
                let a = rt.alloc(1, 32, |_, _| Ok(())).unwrap();
                let mut v = a.start;
                for _ in 0..a.count {
                    pending.push(v);
                    v += a.increment;
                }
            }
            out.push(pending.remove(0));
        }
        out
    }

    #[test]
    fn audit_cycle_reserves_each_wrap_and_retries_failed_persist() {
        for inc in [1, -1] {
            let start = if inc > 0 { 1 } else { 3 };
            let rt = SequenceRuntime::new(schema(inc, 1, 3, start, 1, true), start, start, false);
            let mut writes = 0;
            for _ in 0..7 {
                rt.alloc(1, 32, |_, called| {
                    assert!(called);
                    writes += 1;
                    Ok(())
                })
                .unwrap();
            }
            assert_eq!(writes, 3, "initial reservation plus two wraps");
        }
        let rt = SequenceRuntime::new(schema(1, 1, 100, 1, 1, false), 1, 1, false);
        assert!(rt
            .alloc(1, 32, |_, _| Err(err_invalid("injected IO failure")))
            .is_err());
        assert_eq!(rt.last_value(), None);
        assert_eq!(rt.alloc(1, 32, |_, _| Ok(())).unwrap().start, 1);
    }

    #[test]
    fn sequence_reservation_preserves_increment_at_unaligned_bounds() {
        for (inc, min, max, start, expected) in [(3, 1, 11, 1, 10), (-3, -11, -1, -1, -10)] {
            let rt =
                SequenceRuntime::new(schema(inc, min, max, start, 1, false), start, start, false);
            let mut reserved = None;
            rt.alloc(1, 32, |value, _| {
                reserved = Some(value);
                Ok(())
            })
            .unwrap();
            assert_eq!(reserved, Some(expected));
        }
    }

    #[test]
    fn sequence_huge_cache_and_extreme_steps_are_constant_time_and_exact() {
        for (increment, start, expected_count, expected_last) in [
            (1, 1, i64::MAX, i64::MAX),
            (i64::MIN, i64::MAX, 2, -1),
            (i64::MAX, i64::MIN, 3, i64::MAX - 1),
        ] {
            let rt = SequenceRuntime::new(
                schema(increment, i64::MIN, i64::MAX, start, i64::MAX, false),
                start,
                start,
                false,
            );
            let alloc = rt.alloc(i64::MAX, i64::MAX, |_, _| Ok(())).unwrap();
            assert_eq!(alloc.count, expected_count);
            assert_eq!(alloc.last(), expected_last);
            assert!(rt.alloc(1, 32, |_, _| Ok(())).is_err());
        }
    }

    #[test]
    fn sequence_uncertain_wal_failure_burns_pending_window_before_retry() {
        let rt = SequenceRuntime::new(schema(1, 1, 1000, 1, 1, false), 1, 1, false);
        assert!(rt
            .alloc(1, 32, |value, called| {
                *rt.logged.lock() = SequenceValue {
                    sequence_id: 1,
                    counter_id: 1,
                    resume_value: value,
                    is_called: called,
                    state_lsn: 8,
                };
                Err(err_invalid("injected durability failure after WAL append"))
            })
            .is_err());
        assert_eq!(rt.last_value(), None);
        assert_eq!(rt.alloc(1, 32, |_, _| Ok(())).unwrap().start, 34);
    }

    #[test]
    fn ascending_basic() {
        let def = schema(1, 1, i64::MAX, 1, 1, false);
        let rt = SequenceRuntime::new(def, 1, 1, false);
        assert_eq!(drain(&rt, 5), vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn increment_by_three() {
        let def = schema(3, 1, i64::MAX, 1, 1, false);
        let rt = SequenceRuntime::new(def, 1, 1, false);
        assert_eq!(drain(&rt, 4), vec![1, 4, 7, 10]);
    }

    #[test]
    fn no_cycle_boundary_errors() {
        let def = schema(1, 1, 3, 1, 1, false);
        let rt = SequenceRuntime::new(def, 1, 1, false);
        assert_eq!(drain(&rt, 3), vec![1, 2, 3]);
        assert!(rt.alloc(1, 32, |_, _| Ok(())).is_err());
    }

    #[test]
    fn cycle_wraps() {
        let def = schema(1, 1, 3, 1, 1, true);
        let rt = SequenceRuntime::new(def, 1, 1, false);
        assert_eq!(drain(&rt, 7), vec![1, 2, 3, 1, 2, 3, 1]);
    }

    #[test]
    fn descending_basic() {
        let def = schema(-1, 1, 10, 10, 1, false);
        let rt = SequenceRuntime::new(def, 10, 10, false);
        assert_eq!(drain(&rt, 4), vec![10, 9, 8, 7]);
    }

    #[test]
    fn cache_block_is_contiguous() {
        let def = schema(1, 1, i64::MAX, 1, 10, false);
        let rt = SequenceRuntime::new(def, 1, 1, false);
        let a = rt.alloc(10, 32, |_, _| Ok(())).unwrap();
        assert_eq!(a.start, 1);
        assert_eq!(a.count, 10);
        assert_eq!(a.last(), 10);
        let b = rt.alloc(10, 32, |_, _| Ok(())).unwrap();
        assert_eq!(b.start, 11);
    }

    #[test]
    fn setval_then_next() {
        let def = schema(1, 1, i64::MAX, 1, 1, false);
        let rt = SequenceRuntime::new(def, 1, 1, false);
        rt.setval(42, true, |_, _| Ok(())).unwrap();
        assert_eq!(drain(&rt, 1), vec![43]);
        rt.setval(100, false, |_, _| Ok(())).unwrap();
        assert_eq!(drain(&rt, 1), vec![100]);
    }

    #[test]
    fn build_defaults_ascending() {
        let s = build_sequence_schema(1, "test", "s", None, &SeqOptions::default(), 0, 0).unwrap();
        assert_eq!(s.increment, 1);
        assert_eq!(s.min_value, 1);
        assert_eq!(s.max_value, i64::MAX);
        assert_eq!(s.start_value, 1);
        assert_eq!(s.cache, 1);
        assert!(!s.cycle);
    }

    #[test]
    fn build_defaults_descending() {
        let opts = SeqOptions {
            increment: Some(-1),
            ..Default::default()
        };
        let s = build_sequence_schema(1, "test", "s", None, &opts, 0, 0).unwrap();
        assert_eq!(s.min_value, i64::MIN);
        assert_eq!(s.max_value, -1);
        assert_eq!(s.start_value, -1);
    }

    #[test]
    fn build_rejects_zero_increment() {
        let opts = SeqOptions {
            increment: Some(0),
            ..Default::default()
        };
        assert!(build_sequence_schema(1, "test", "s", None, &opts, 0, 0).is_err());
    }

    #[test]
    fn build_smallint_range() {
        let opts = SeqOptions {
            as_type: Some(SeqType::SmallInt),
            ..Default::default()
        };
        let s = build_sequence_schema(1, "test", "s", None, &opts, 0, 0).unwrap();
        assert_eq!(s.max_value, i16::MAX as i64);
    }
}
