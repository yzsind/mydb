//! Bounded per-session optimizer trace production.

use crate::planner::{estimated_rows, plan_cost, PhysicalPlan};
use crate::types::Value;
use serde_json::{json, Value as JsonValue};
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

const MAX_EVENTS: usize = 256;
const MAX_TRACE_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
struct TraceRow {
    query: String,
    trace: String,
    missing_bytes: usize,
}

#[derive(Default)]
struct WorkingTrace {
    query: String,
    events: Vec<JsonValue>,
}

#[derive(Default)]
struct TraceStore {
    working: HashMap<u32, WorkingTrace>,
    last: HashMap<u32, TraceRow>,
}

fn store() -> &'static Mutex<TraceStore> {
    static STORE: OnceLock<Mutex<TraceStore>> = OnceLock::new();
    STORE.get_or_init(|| Mutex::new(TraceStore::default()))
}

thread_local! {
    static ACTIVE_CONNECTION: Cell<Option<u32>> = const { Cell::new(None) };
}

pub(crate) struct TraceSessionGuard(Option<u32>);

impl Drop for TraceSessionGuard {
    fn drop(&mut self) {
        ACTIVE_CONNECTION.with(|active| active.set(self.0));
    }
}

pub(crate) fn activate(connection_id: u32, enabled: bool) -> TraceSessionGuard {
    let next = enabled.then_some(connection_id);
    let previous = ACTIVE_CONNECTION.with(|active| active.replace(next));
    TraceSessionGuard(previous)
}

fn enabled_connection() -> Option<u32> {
    crate::engine::expr::current_optimizer_trace_context()
        .and_then(|(connection_id, enabled)| enabled.then_some(connection_id))
        .or_else(|| ACTIVE_CONNECTION.with(Cell::get))
}

fn event_connection() -> Option<u32> {
    enabled_connection()
}

pub(crate) fn begin(query: &str) {
    let Some(connection_id) = enabled_connection() else {
        return;
    };
    let mut traces = store().lock().unwrap_or_else(|poison| poison.into_inner());
    // Reading the trace table must expose the preceding statement and must
    // not replace it with the plan of the inspection query itself. This was
    // previously implicit in the dedicated system-table dispatcher; make it
    // an explicit trace-store invariant now that SystemScan is an ordinary
    // physical BatchOperator.
    if query
        .to_ascii_lowercase()
        .contains("information_schema.optimizer_trace")
    {
        traces.working.remove(&connection_id);
        return;
    }
    traces.working.insert(
        connection_id,
        WorkingTrace {
            query: if crate::engine::ai::sql::sql_may_have_ai(query) {
                "[redacted AI statement]".into()
            } else {
                query.to_owned()
            },
            events: vec![json!({
                "stage": "planning_started",
                "cbo_epoch": crate::engine::cbo::cbo_epoch(),
                "cbo_model": crate::engine::cbo::cbo_model(),
            })],
        },
    );
}

pub(crate) fn event(stage: &'static str, detail: JsonValue) {
    let Some(connection_id) = event_connection() else {
        return;
    };
    let mut traces = store().lock().unwrap_or_else(|poison| poison.into_inner());
    let Some(trace) = traces.working.get_mut(&connection_id) else {
        return;
    };
    if trace.events.len() < MAX_EVENTS {
        trace.events.push(json!({"stage": stage, "detail": detail}));
    }
}

pub(crate) fn finish(plan: &PhysicalPlan) {
    let Some(connection_id) = event_connection() else {
        return;
    };
    let estimate = crate::planner::plan_rel_estimate(plan);
    let properties = crate::planner::plan_physical_properties(plan);
    let local = crate::planner::plan_local_cost(plan);
    event(
        "plan_selected",
        json!({
            "rows": estimated_rows(plan),
            "width": plan.output_width(),
            "estimate_source": estimate.source.as_str(),
            "estimate_confidence": estimate.confidence,
            "physical_properties": {
                "ordering": properties.ordering,
                "execution_mode": format!("{:?}", properties.execution_mode),
                "parallel_degree": properties.parallel_degree,
                "parameterized_by": properties.parameterized_by,
                "rewindable": properties.rewindable,
            },
            "local_cost": {
                "startup": local.startup,
                "total": local.total,
                "seq_pages": local.io_seq_pages,
                "random_pages": local.io_rnd_pages,
                "cpu_units": local.cpu_units,
                "memory_bytes": local.mem_bytes,
                "spill_bytes": local.spill_bytes,
            },
            "cost": {
                "startup": plan_cost(plan).startup,
                "total": plan_cost(plan).total,
                "seq_pages": plan_cost(plan).io_seq_pages,
                "random_pages": plan_cost(plan).io_rnd_pages,
                "cpu_units": plan_cost(plan).cpu_units,
                "peak_memory_bytes": plan_cost(plan).mem_bytes,
                "spill_bytes": plan_cost(plan).spill_bytes,
            },
            "valid": plan_cost(plan).is_valid(),
        }),
    );
    finish_working(connection_id);
}

/// Complete a trace when execution used a cached plan or a specialized fast
/// path that did not call the physical planner in this statement.
pub(crate) fn complete_statement() {
    let Some(connection_id) = event_connection() else {
        return;
    };
    {
        let traces = store().lock().unwrap_or_else(|poison| poison.into_inner());
        let Some(working) = traces.working.get(&connection_id) else {
            return;
        };
        if working
            .events
            .iter()
            .any(|event| event.get("stage").and_then(JsonValue::as_str) == Some("plan_selected"))
        {
            return;
        }
    }
    event(
        "plan_selected",
        json!({"source": "cached_or_specialized_path", "cbo_epoch": crate::engine::cbo::cbo_epoch()}),
    );
    finish_working(connection_id);
}

fn finish_working(connection_id: u32) {
    let mut traces = store().lock().unwrap_or_else(|poison| poison.into_inner());
    let Some(working) = traces.working.remove(&connection_id) else {
        return;
    };
    let full = json!({
        "model": crate::engine::cbo::cbo_model(),
        "events": working.events,
    })
    .to_string();
    let missing_bytes = full.len().saturating_sub(MAX_TRACE_BYTES);
    let trace = if missing_bytes == 0 {
        full
    } else {
        json!({
            "model": crate::engine::cbo::cbo_model(),
            "truncated": true,
            "missing_bytes_beyond_max_mem_size": missing_bytes,
        })
        .to_string()
    };
    traces.last.insert(
        connection_id,
        TraceRow {
            query: working.query,
            trace,
            missing_bytes,
        },
    );
}

/// Session registry teardown calls this even after a protocol task panics.
pub(crate) fn clear_connection(connection_id: u32) {
    let mut traces = store().lock().unwrap_or_else(|poison| poison.into_inner());
    traces.working.remove(&connection_id);
    traces.last.remove(&connection_id);
    if traces.working.capacity() > 1024
        && traces.working.len().saturating_mul(4) < traces.working.capacity()
    {
        traces.working.shrink_to_fit();
    }
    if traces.last.capacity() > 1024 && traces.last.len().saturating_mul(4) < traces.last.capacity()
    {
        traces.last.shrink_to_fit();
    }
}

pub(crate) fn rows() -> Vec<Vec<Value>> {
    let Some(connection_id) = enabled_connection() else {
        return Vec::new();
    };
    let traces = store().lock().unwrap_or_else(|poison| poison.into_inner());
    traces
        .last
        .get(&connection_id)
        .map(|trace| {
            let json_value = serde_json::from_str(&trace.trace).unwrap_or_else(|_| json!({}));
            vec![vec![
                Value::String(trace.query.clone()),
                Value::Json(crate::engine::json::from_serde(&json_value)),
                Value::UInt(trace.missing_bytes as u64),
                Value::UInt(0),
            ]]
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod release_audit_tests {
    use super::*;

    #[test]
    fn release_audit_trace_never_falls_back_to_another_session() {
        let victim = u32::MAX - 100;
        let reader = u32::MAX - 101;
        clear_connection(victim);
        clear_connection(reader);
        {
            let _guard = activate(victim, true);
            begin("SELECT 'private literal'");
            complete_statement();
            assert_eq!(rows().len(), 1);
        }
        {
            let _guard = activate(reader, true);
            assert!(rows().is_empty());
        }
        {
            let _guard = activate(victim, true);
            begin("SELECT 'working private literal'");
        }
        assert_eq!(event_connection(), None);
        clear_connection(victim);
        clear_connection(reader);
    }
}
