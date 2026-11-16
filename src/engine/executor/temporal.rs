use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use crate::parser::{Select, Statement, SystemTimeAsOf, TableRef};
use crate::planner::PlanTable;
use crate::storage::{error, Database, SnapshotGuard, TxnView};
use crate::types::{QueryError, Value};

#[derive(Debug)]
pub(crate) struct TemporalReadContext {
    views: HashMap<u32, TxnView>,
    #[allow(dead_code)]
    guards: Vec<SnapshotGuard>,
    /// Keeps guards registered by an outer query graph alive when a nested
    /// execution adds bindings of its own and a streaming worker outlives the
    /// caller's thread-local scope.
    #[allow(dead_code)]
    parent: Option<Arc<TemporalReadContext>>,
}

thread_local! {
    static CURRENT: RefCell<Option<Arc<TemporalReadContext>>> = const { RefCell::new(None) };
}

pub(crate) struct TemporalScope(Option<Arc<TemporalReadContext>>);

impl Drop for TemporalScope {
    fn drop(&mut self) {
        CURRENT.with(|slot| *slot.borrow_mut() = self.0.take());
    }
}

pub(crate) fn capture() -> Option<Arc<TemporalReadContext>> {
    CURRENT.with(|slot| slot.borrow().clone())
}

pub(crate) fn enter(context: Option<Arc<TemporalReadContext>>) -> TemporalScope {
    let previous = CURRENT.with(|slot| std::mem::replace(&mut *slot.borrow_mut(), context));
    TemporalScope(previous)
}

pub(crate) fn txn_for_table(default: TxnView, table: &PlanTable) -> TxnView {
    let Some(binding) = table.system_time.as_ref().map(|binding| binding.binding_id) else {
        return default;
    };
    CURRENT.with(|slot| {
        slot.borrow()
            .as_ref()
            .and_then(|context| context.views.get(&binding).copied())
            .unwrap_or(default)
    })
}

pub(crate) fn resolved_scn(binding_id: u32) -> Option<u64> {
    CURRENT.with(|slot| {
        slot.borrow()
            .as_ref()
            .and_then(|context| context.views.get(&binding_id))
            .map(|view| view.snapshot_scn)
    })
}

pub(crate) fn preflight(
    db: &Arc<Database>,
    select: &Select,
) -> Result<Option<Arc<TemporalReadContext>>, QueryError> {
    // The session dispatcher preflights the complete statement graph. Nested
    // execution must reuse those guards instead of cloning and walking each
    // SELECT again. For the session-free entry point, only a direct temporal
    // table or a derived query can introduce a binding; an ordinary SELECT
    // stays on a zero-allocation fast path.
    if capture().is_some()
        || (select.from.iter().all(|table| table.system_time.is_none())
            && select
                .joins
                .iter()
                .all(|join| join.table.system_time.is_none())
            && select.derived.is_none()
            && select.joins.iter().all(|join| join.derived.is_none()))
    {
        return Ok(None);
    }
    preflight_statement(db, &Statement::Select(select.clone()))
}

pub(crate) fn preflight_statement(
    db: &Arc<Database>,
    statement: &Statement,
) -> Result<Option<Arc<TemporalReadContext>>, QueryError> {
    let bindings = crate::parser::collect_system_time_bindings(statement);
    preflight_bindings(db, bindings)
}

fn preflight_bindings(
    db: &Arc<Database>,
    bindings: Vec<(TableRef, SystemTimeAsOf, bool)>,
) -> Result<Option<Arc<TemporalReadContext>>, QueryError> {
    if bindings.is_empty() {
        return Ok(None);
    }
    if bindings.iter().any(|(_, _, locking)| *locking) {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "locking reads cannot use FOR SYSTEM_TIME AS OF",
        ));
    }
    let parent = capture();
    let mut views = parent
        .as_ref()
        .map(|context| context.views.clone())
        .unwrap_or_else(|| HashMap::with_capacity(bindings.len()));
    let bindings = bindings
        .into_iter()
        .filter(|(_, binding, _)| !views.contains_key(&binding.binding_id))
        .collect::<Vec<_>>();
    if bindings.is_empty() {
        return Ok(None);
    }
    db.record_temporal_query();
    let statement_start_unix_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .min(u128::from(u64::MAX)) as u64;
    for (table_ref, binding, _) in bindings {
        let schema = db.table(&table_ref.name).map_err(|err| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!(
                    "FOR SYSTEM_TIME AS OF requires a persistent user base table: {} ({})",
                    table_ref.name,
                    err.message()
                ),
            )
        })?;
        if schema.system || crate::storage::is_temp_table_id(schema.id) {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "FOR SYSTEM_TIME AS OF cannot qualify a system table",
            ));
        }
        let target = eval_as_of(&binding)?;
        let view = db.historical_view_as_of(target, statement_start_unix_us)?;
        if target < schema.system_time_epoch_us {
            return Err(error(
                crate::error::catalog::MYDB_ER_SYSTEM_TIME_SCHEMA_CHANGED,
                format!(
                    "FOR SYSTEM_TIME AS OF target {target}us predates the current structural epoch {}us of table '{}'",
                    schema.system_time_epoch_us,
                    table_ref.name,
                ),
            ));
        }
        views.insert(binding.binding_id, view);
    }
    let mut distinct_scns = views
        .values()
        .map(|view| view.snapshot_scn)
        .collect::<Vec<_>>();
    distinct_scns.sort_unstable();
    distinct_scns.dedup();
    let guards = db.register_statement_snapshots_atomic(&distinct_scns);
    Ok(Some(Arc::new(TemporalReadContext {
        views,
        guards,
        parent,
    })))
}

fn eval_as_of(binding: &SystemTimeAsOf) -> Result<u64, QueryError> {
    if !crate::parser::system_time_expr_is_statement_constant(&binding.expr) {
        return Err(invalid_system_time(
            "AS OF expression must be a statement-constant temporal scalar",
        ));
    }
    let value = crate::engine::expr::eval_const_runtime(&binding.expr)
        .map_err(|err| invalid_system_time(err.message()))?;
    let micros = match value {
        Value::Timestamp(micros) => micros,
        Value::DateTime(micros) => {
            crate::engine::expr::session_local_datetime_to_utc_timestamp_micros(micros)
                .map_err(|err| invalid_system_time(err.message()))?
        }
        Value::Date(_) | Value::String(_) | Value::SmallString(_) | Value::Bytes(_) => {
            let local = crate::engine::expr::parse_datetime(&value)
                .map_err(|err| invalid_system_time(err.message()))?
                .and_utc()
                .timestamp_micros();
            crate::engine::expr::session_local_datetime_to_utc_timestamp_micros(local)
                .map_err(|err| invalid_system_time(err.message()))?
        }
        Value::Null => return Err(invalid_system_time("AS OF expression evaluated to NULL")),
        _ => {
            return Err(invalid_system_time(
                "AS OF expression is not a datetime value",
            ))
        }
    };
    u64::try_from(micros).map_err(|_| invalid_system_time("AS OF time is out of range"))
}

fn invalid_system_time(message: impl Into<String>) -> QueryError {
    error(crate::error::catalog::MYDB_ER_INVALID_SYSTEM_TIME, message)
}
