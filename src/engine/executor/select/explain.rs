// Keep formatting helpers in this module while separating their source.
include!("explain/node.rs");

use super::super::*;
use super::analyze_stats::{self, NodeIoStats, NodeStats, NodeStatsMap};
use super::*;
use crate::engine::cbo::Cost;
use crate::engine::session_registry::SessionRegistry;
use crate::parser::{ExplainFormat, SetOp};

/// Additional UNION-ALL branches for a CTE whose body is a non-recursive
/// `... UNION ALL ...` set operation. Indexed by lower-cased CTE name. The
/// LEFT branch's plan still lives in the primary `cte_plans` map; entries
/// here carry branches 1, 2, ... in source order, so the display tree can
/// render every branch and EXPLAIN ANALYZE can execute every branch under
/// the stats collector. Absent / empty for plain-SELECT CTEs.
type CteExtraBranches = std::collections::HashMap<String, Vec<planner::PhysicalPlan>>;

fn is_source_free_select(select: &Select) -> bool {
    select.table.is_none()
        && select.from.is_empty()
        && select.values.is_none()
        && select.json_table.is_none()
        && select.derived.is_none()
        && select.joins.is_empty()
}

fn explain_no_tables_used_result() -> QueryResponse {
    QueryResponse::ResultSet(ResultSet {
        columns: planner::explain_result_columns(),
        rows: vec![vec![
            Value::Int(1),
            Value::String("SIMPLE".to_string()),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::String("No tables used".to_string()),
        ]],
    })
}

/// Left-deep flatten a chain of `UNION ALL` set operations into the
/// source-order list of branch SELECTs. Returns `None` if any node in the
/// chain isn't `UNION ALL`, or if any leaf isn't a plain SELECT.
fn flatten_union_all_branches<'q>(
    query: impl Into<crate::parser::QueryRef<'q>>,
) -> Option<Vec<Select>> {
    let query = query.into();
    fn walk<'q>(stmt: impl Into<crate::parser::QueryRef<'q>>, out: &mut Vec<Select>) -> bool {
        let stmt = stmt.into();
        match stmt {
            crate::parser::QueryRef::Select(select) => {
                out.push(select.clone());
                true
            }
            crate::parser::QueryRef::SetOperation(set) => {
                if set.op != SetOp::Union || !set.all {
                    return false;
                }
                walk(&set.left, out) && walk(&set.right, out)
            }
            _ => false,
        }
    }
    let mut branches = Vec::new();
    if walk(query, &mut branches) && branches.len() >= 2 {
        Some(branches)
    } else {
        None
    }
}

pub(in crate::engine::executor) fn execute_explain(
    db: &Arc<Database>,
    session: &mut SessionContext,
    target: Statement,
    format: ExplainFormat,
    analyze: bool,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    let target = match target {
        Statement::Select(select)
            if session.effective_isolation() != crate::storage::IsolationLevel::ReadUncommitted =>
        {
            match super::shared_aggregate::factor(db, &select) {
                Some(shared) => Statement::WithCte(crate::parser::WithCte {
                    ctes: shared
                        .definitions
                        .into_iter()
                        .map(|(definition, _)| definition)
                        .collect(),
                    recursive: false,
                    body: Box::new(Statement::Select(shared.body)),
                }),
                None => Statement::Select(select),
            }
        }
        other => other,
    };
    if let Statement::WithCte(mut with) = target {
        if matches!(
            with.body.as_ref(),
            Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
        ) {
            return explain_dml_with_cte(db, session, &mut with, format, analyze, registry);
        }
        if analyze {
            return explain_analyze_with_cte(db, session, with, format, registry);
        }
        // NOTE: EXPLAIN displays the materialised-CTE plan (CteScan nodes).
        // Execution additionally applies P1-1 inlining / P1-2 / P1-3 pushdown
        // (see dispatch::execute_with_cte), proven result-preserving by the
        // executor tests; the EXPLAIN derived-table renderer is not wired for
        // the synthetic inlined shape, so the plan tree intentionally shows the
        // pre-inline form here.
        return explain_with_cte(db, &session.current_database, with, format);
    }
    match &target {
        Statement::Update(_) | Statement::Delete(_) | Statement::Insert(_) => {
            return explain_dml(db, session, target, format, analyze, registry);
        }
        _ => {}
    }
    if let Statement::SetOperation(mut set) = target {
        fn prepare_query<'q>(
            db: &Arc<Database>,
            statement: impl Into<crate::parser::QueryMut<'q>>,
        ) -> Result<(), QueryError> {
            let statement = statement.into();
            match statement {
                crate::parser::QueryMut::Select(select) => prepare_select_for_planning(db, select),
                crate::parser::QueryMut::Rrf(rrf) => {
                    for branch in &mut rrf.branches {
                        prepare_query(db, &mut branch.query)?;
                    }
                    Ok(())
                }
                crate::parser::QueryMut::SetOperation(set) => {
                    prepare_query(db, &mut set.left)?;
                    prepare_query(db, &mut set.right)
                }
                _ => Ok(()),
            }
        }

        let mut statement = Statement::SetOperation(set.clone());
        prepare_query(db, &mut statement)?;
        let Statement::SetOperation(prepared) = statement else {
            unreachable!()
        };
        set = prepared;
        if matches!(format, ExplainFormat::Tree | ExplainFormat::Json) {
            let planned_statement = Statement::SetOperation(set.clone());
            let plan = planner::plan_query_physical(db, &planned_statement)?.ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "set operation could not be lowered to a physical graph",
                )
            })?;
            let analyze_data = if analyze {
                let (mut stats, result) = analyze_stats::with_collector(|| {
                    let started = std::time::Instant::now();
                    let response = super::super::dispatch::execute_set_operation_dispatch(
                        db,
                        session,
                        set.clone(),
                        registry,
                    )?
                    .materialize()?;
                    let rows = match response {
                        QueryResponse::ResultSet(result) => result.rows.len() as u64,
                        _ => 0,
                    };
                    Ok::<_, QueryError>((rows, started.elapsed().as_nanos() as f64 / 1_000_000.0))
                });
                let (rows, elapsed_ms) = result?;
                let executed_plan = stats
                    .take_executed_plan()
                    .unwrap_or_else(|| Box::new(plan.clone()));
                complete_fused_trace(
                    &executed_plan,
                    &mut stats,
                    Some(NodeStats {
                        first_row_ns: (elapsed_ms * 1_000_000.0).round() as u128,
                        time_ns: (elapsed_ms * 1_000_000.0).round() as u128,
                        rows,
                        loops: 1,
                        ..NodeStats::default()
                    }),
                );
                Some((stats, rows, elapsed_ms, executed_plan))
            } else {
                None
            };
            let text = match format {
                ExplainFormat::Tree => format_plan_tree(
                    analyze_data
                        .as_ref()
                        .map(|(_, _, _, executed)| executed.as_ref())
                        .unwrap_or(&plan),
                    0,
                    analyze_data.as_ref().map(|(stats, _, _, _)| stats),
                    None,
                    None,
                ),
                ExplainFormat::Json => format_plan_json_with_ctes(
                    analyze_data
                        .as_ref()
                        .map(|(_, _, _, executed)| executed.as_ref())
                        .unwrap_or(&plan),
                    analyze_data.as_ref().map(|(stats, rows, elapsed_ms, _)| {
                        (stats, *rows, *elapsed_ms, stats.query_io())
                    }),
                    None,
                    None,
                ),
                ExplainFormat::Traditional => unreachable!(),
            };
            return Ok(QueryResponse::ResultSet(ResultSet {
                columns: vec![Column {
                    source: Default::default(),
                    name: "EXPLAIN".to_string(),
                    column_type: ColumnType::VarString,
                }],
                rows: vec![vec![Value::String(text)]],
            }));
        }
        if analyze {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "EXPLAIN ANALYZE for set operations requires FORMAT=TREE or FORMAT=JSON",
            ));
        }
        let mut rows = explain_statement_body(db, &Statement::SetOperation(set))?;
        renumber_explain_rows(&mut rows);
        return Ok(QueryResponse::ResultSet(ResultSet {
            columns: planner::explain_result_columns(),
            rows: planner::explain_rows_to_values(rows),
        }));
    }
    let Statement::Select(mut select) = target else {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "EXPLAIN currently supports SELECT, WITH, INSERT, UPDATE, DELETE",
        ));
    };
    prepare_select_for_planning(db, &mut select)?;
    if matches!(format, ExplainFormat::Tree | ExplainFormat::Json) {
        return execute_explain_formatted(db, session, registry, &select, format, analyze);
    }
    if select_has_window_in_projection(&select) {
        return explain_select_with_window(db, &select);
    }
    if needs_multi_source_explain(&select) {
        if analyze {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "EXPLAIN ANALYZE is not yet supported for multi-source derived/CTE joins",
            ));
        }
        let mut rows = explain_multi_source_select(db, &select)?;
        tag_view_rows(db, &mut rows);
        renumber_explain_rows(&mut rows);
        return Ok(QueryResponse::ResultSet(ResultSet {
            columns: planner::explain_result_columns(),
            rows: planner::explain_rows_to_values(rows),
        }));
    }
    if select.derived.is_some() {
        return execute_derived_explain(db, &select);
    }
    if is_source_free_select(&select) {
        return Ok(explain_no_tables_used_result());
    }
    // EXPLAIN and EXPLAIN ANALYZE must be planned from the same statistics
    // snapshot.  Refresh before planning in both modes; the ANALYZE timer is
    // installed below, so this maintenance work is deliberately excluded
    // from actual execution time.
    refresh_stale_stats_for_select(db, &select)?;
    // Keep the prepared forest at a stable address across execution and
    // formatting. Runtime observations are being migrated to OperatorId, but
    // legacy executor hooks still resolve their id from the node address.
    let planned = Box::new(planner::plan_select(db, &select)?);
    let mut rows = planner::explain_rows(&planned.physical);
    let before_subquery_count = rows.len();
    let mut normalized_select = select.clone();
    planner::normalize_select_predicates(db, &mut normalized_select);
    append_select_subquery_explain_rows(db, &normalized_select, &mut rows)?;
    if rows.len() > before_subquery_count {
        for row in rows.iter_mut().take(before_subquery_count) {
            if row.select_type == "SIMPLE" {
                row.select_type = "PRIMARY".to_string();
            }
        }
    }
    tag_view_rows(db, &mut rows);
    renumber_explain_rows(&mut rows);
    attach_hint_warnings(&mut rows, &planned.hint_warnings);

    if !analyze {
        return Ok(QueryResponse::ResultSet(ResultSet {
            columns: planner::explain_result_columns(),
            rows: planner::explain_rows_to_values(rows),
        }));
    }
    let analyze_compiled = crate::engine::cache::CompiledSelectPlan::new(
        Arc::new(select.clone()),
        Arc::new(planned.physical.clone()),
        smallvec::SmallVec::new(),
        smallvec::SmallVec::new(),
        0,
    );
    // EXPLAIN ANALYZE: run the query under a thread-local collector so
    // each `execute_plan_rows` call records (time, rows, loops) keyed
    // by its plan node pointer. Per-leaf stats then surface in the
    // tabular output via `ExplainRow::plan_id`. Whole-query metrics
    // (total wall time, total result rows) still go on row[0] for
    // operators that don't have a leaf row of their own.
    let (mut stats, exec_result) = analyze_stats::with_collector(|| {
        let exec_start = std::time::Instant::now();
        let rows =
            run_select_for_analyze_with_plan(db, session, &analyze_compiled, &select, registry)?;
        let elapsed_ms = exec_start.elapsed().as_nanos() as f64 / 1_000_000.0;
        Ok::<_, QueryError>((rows, elapsed_ms))
    });
    let (exec_rows, elapsed_ms) = exec_result?;
    let executed_plan = stats
        .take_executed_plan()
        .unwrap_or_else(|| Box::new(planned.physical.clone()));
    complete_fused_trace(
        &executed_plan,
        &mut stats,
        Some(NodeStats {
            first_row_ns: (elapsed_ms * 1_000_000.0).round() as u128,
            time_ns: (elapsed_ms * 1_000_000.0).round() as u128,
            rows: exec_rows,
            loops: 1,
            ..NodeStats::default()
        }),
    );
    // Generate the metric-bearing rows after execution from the same pinned
    // forest. Pre-execution ExplainRows must not carry address-based identity
    // across the execution phase.
    let mut analyzed_rows = planner::explain_rows(&executed_plan);
    tag_view_rows(db, &mut analyzed_rows);
    renumber_explain_rows(&mut analyzed_rows);
    attach_hint_warnings(&mut analyzed_rows, &planned.hint_warnings);
    Ok(QueryResponse::ResultSet(ResultSet {
        columns: explain_analyze_result_columns(),
        rows: explain_rows_to_analyze_values(analyzed_rows, exec_rows, elapsed_ms, &stats),
    }))
}

/// EXPLAIN ANALYZE for `WITH ... body` statements.
///
/// Approach: build the structural explain rows under a stub CTE registry
/// (so the body planner recognises CTE references), then actually execute
/// the WITH — materialising each CTE in turn while timing it, then running
/// the body with the populated registry while timing it. Finally stamp the
/// per-CTE and body timings onto the first row of each block.
///
/// Per-plan-node stats can't be keyed correctly here because the executed
/// plan trees are built fresh inside `materialize_single_cte` / the body
/// dispatch and don't share node pointers with the trees the formatter
/// walked in Phase 1. Per-block totals are still meaningful and match
/// MySQL's coarse-grained WITH-CTE timing.
fn explain_analyze_with_cte(
    db: &Arc<Database>,
    session: &mut SessionContext,
    mut with: crate::parser::WithCte,
    format: ExplainFormat,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    // FORMAT=TREE/JSON: execute the *display* plan trees themselves under a
    // single stats collector, so per-operator `actual time/rows/loops`
    // attach to the clauses inside each CTE body (not just per-CTE totals).
    // Inline-derived sources in the body (TPC-DS Q1-style `FROM (SELECT ...)
    // y, (SELECT ...) z`) are pre-materialised in Phase 2 so the body's
    // display plan — whose corresponding leaves are stub CteScans — can run.
    // The pre-materialisation rewrites (incl. single-use / IN-subquery-only
    // CTE inlining) run here so the displayed trees match what executes.
    if matches!(format, ExplainFormat::Tree | ExplainFormat::Json) {
        apply_with_cte_explain_rewrites(db, &session.current_database, &mut with);
        return explain_analyze_with_cte_formatted(db, session, with, format, registry);
    }

    // FORMAT=TRADITIONAL keeps the coarse per-block timing path below.
    // Unlike the formatted path it must NOT inline single-use / IN-subquery-
    // only CTEs: Phase 1 builds rows from stub CTE registry entries via
    // `explain_statement_body`, and an inlined-derived CTE reference that sits
    // inside a WHERE-subquery join cannot be planned in isolation (it fails
    // column resolution with 1054). Plain `EXPLAIN WITH` (traditional) likewise
    // skips inlining — see `explain_with_cte` — so leaving the rewrites off
    // here keeps the analyzed and un-analyzed traditional output consistent.
    // Phase 1: build the Traditional explain rows up front (under a stub
    // CTE registry). We remember the row index where each CTE's block and
    // the body's block start so we can stamp timings later.
    let mut traditional_rows: Vec<planner::ExplainRow> = Vec::new();
    let mut cte_row_starts: Vec<usize> = Vec::with_capacity(with.ctes.len());
    let mut body_row_start: usize = 0;
    if matches!(format, ExplainFormat::Traditional) {
        let mut stubs: std::collections::HashMap<
            String,
            std::sync::Arc<crate::engine::api::CteResult>,
        > = std::collections::HashMap::new();
        for cte in &with.ctes {
            // Infer each stub's columns from the CTE body's projection (not the
            // usually-absent explicit column list) so references like
            // `ws_wh.ws_order_number` inside the body / its WHERE-subqueries
            // resolve. Built incrementally so a CTE that references an earlier
            // one sees its columns. Mirrors plain `explain_with_cte`.
            let columns = cte_stub_columns(db, cte, &stubs);
            stubs.insert(
                cte.name.to_ascii_lowercase(),
                std::sync::Arc::new(crate::engine::api::CteResult::stub_for_explain(
                    columns, 0.0,
                )),
            );
        }
        let body_rows = super::cte::with_cte_registry(
            stubs,
            || -> Result<Vec<planner::ExplainRow>, QueryError> {
                for cte in &with.ctes {
                    cte_row_starts.push(traditional_rows.len());
                    let cte_rows = explain_cte_body(db, cte)?;
                    traditional_rows.extend(cte_rows);
                }
                explain_statement_body(db, &with.body)
            },
        )?;
        body_row_start = traditional_rows.len();
        traditional_rows.extend(body_rows);
        for row in traditional_rows.iter_mut().skip(body_row_start) {
            if row.select_type == "SIMPLE" || row.select_type == "DERIVED" {
                row.select_type = "PRIMARY".to_string();
            }
        }
        renumber_explain_rows(&mut traditional_rows);
    }

    // Phase 2: execute the WITH statement, timing each piece. Mirrors
    // `dispatch::execute_with_cte` so the per-query memory budget covers
    // the whole WITH execution like the un-analyzed path does.
    let crate::parser::WithCte { ctes, body, .. } = with;
    let body_stmt: Statement = *body;

    let exec_fn = || -> Result<(Vec<(String, u64, f64)>, u64, f64), QueryError> {
        let mut visible: std::collections::HashMap<
            String,
            std::sync::Arc<crate::engine::api::CteResult>,
        > = std::collections::HashMap::new();
        let mut timings: Vec<(String, u64, f64)> = Vec::with_capacity(ctes.len());
        for cte in &ctes {
            let snapshot = visible.clone();
            let start = std::time::Instant::now();
            let result = super::cte::materialize_single_cte(db, session, cte, snapshot, registry)?;
            let elapsed_ms = start.elapsed().as_nanos() as f64 / 1_000_000.0;
            let rows_count = result.nrows() as u64;
            timings.push((cte.name.clone(), rows_count, elapsed_ms));
            visible.insert(cte.name.to_ascii_lowercase(), std::sync::Arc::new(result));
        }
        let body_start_t = std::time::Instant::now();
        let body_response = super::cte::with_cte_registry(visible, || {
            super::super::dispatch::execute_statement(db, session, body_stmt, registry)
        })?;
        let body_ms = body_start_t.elapsed().as_nanos() as f64 / 1_000_000.0;
        let body_rows_count = match body_response {
            QueryResponse::ResultSet(rs) => rs.rows.len() as u64,
            QueryResponse::StreamingResultSet(mut srs) => {
                let mut count = 0u64;
                while srs.cursor.next_row()?.is_some() {
                    count += 1;
                }
                count
            }
            _ => 0,
        };
        Ok((timings, body_rows_count, body_ms))
    };

    let (cte_timings, body_rows_count, body_ms) =
        if crate::engine::executor::mem_budget::current_budget().is_some() {
            exec_fn()?
        } else {
            let budget =
                std::sync::Arc::new(crate::engine::executor::mem_budget::MemBudget::from_env());
            crate::engine::executor::mem_budget::with_query_budget(budget, exec_fn)?
        };

    // Phase 3: combine Phase 1's structure with Phase 2's timings.
    // TREE/JSON were routed to `explain_analyze_with_cte_formatted` above;
    // only the TRADITIONAL tabular output is assembled here.
    let mut out = planner::explain_rows_to_values(traditional_rows);
    for (i, (_name, rows_count, ms)) in cte_timings.iter().enumerate() {
        if let Some(&start) = cte_row_starts.get(i) {
            if let Some(row) = out.get_mut(start) {
                row.push(Value::Double(*rows_count as f64));
                row.push(Value::Int(1));
                row.push(Value::Double(*ms));
                row.push(Value::Double(*ms));
                row.push(Value::Int(*rows_count as i64));
            }
        }
    }
    if let Some(row) = out.get_mut(body_row_start) {
        row.push(Value::Double(body_rows_count as f64));
        row.push(Value::Int(1));
        row.push(Value::Double(body_ms));
        row.push(Value::Double(body_ms));
        row.push(Value::Int(body_rows_count as i64));
    }
    let expected = explain_analyze_result_columns().len();
    for row in out.iter_mut() {
        while row.len() < expected {
            row.push(Value::Null);
        }
    }
    Ok(QueryResponse::ResultSet(ResultSet {
        columns: explain_analyze_result_columns(),
        rows: out,
    }))
}

/// EXPLAIN ANALYZE FORMAT=TREE/JSON for `WITH ... body`.
///
/// Unlike the coarse TRADITIONAL path, this plans the body + each CTE body
/// into display trees once, then executes *those exact trees* under a
/// single per-operator stats collector. Because the formatter walks the
/// same trees, `actual time/rows/loops` line up by node pointer for the
/// clauses *inside* every (non-recursive, plain-SELECT) CTE, and the
/// `CteScan` consumer nodes pick up their real scan loops/rows. Recursive
/// and set-operation CTE bodies fall back to the re-planning materialiser
/// (correct results, per-CTE totals only) until P2.
fn explain_analyze_with_cte_formatted(
    db: &Arc<Database>,
    session: &mut SessionContext,
    with: crate::parser::WithCte,
    format: ExplainFormat,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    use std::collections::HashMap;

    // Phase 1: plan the display trees once.
    let (
        body_plan,
        cte_plans,
        cte_extras,
        hint_warnings,
        body_executable,
        per_cte_inline,
        lossy_ctes,
        body_inline,
        body_lossy,
    ) = plan_with_cte_for_explain(db, &with)?;

    // Phase 2: execute the display trees under one stats collector and a
    // shared read-only snapshot, mirroring `dispatch::execute_with_cte`'s
    // per-query memory budget so the whole WITH stays inside one budget.
    type Phase2 = (
        analyze_stats::NodeStatsMap,
        Vec<(String, u64, f64)>,
        u64,
        f64,
        f64,
    );
    let mut exec_fn = || -> Result<Phase2, QueryError> {
        let (stats, inner) = analyze_stats::with_collector(|| -> Result<_, QueryError> {
            let (txn, _snapshot_guard) = read_view_for_analyze(db, session)?;

            let query_start = std::time::Instant::now();

            let mut visible: HashMap<String, std::sync::Arc<crate::engine::api::CteResult>> =
                HashMap::new();
            let mut timings: Vec<(String, u64, f64)> = Vec::with_capacity(with.ctes.len());
            for cte in &with.ctes {
                let key = cte.name.to_ascii_lowercase();
                let start = std::time::Instant::now();
                let result = if lossy_ctes.contains(&key) {
                    // The CTE body's display plan was built via the
                    // inline-derived rewriter and a SetOp inner only
                    // contributed its leftmost branch. Re-plan the body
                    // through the legacy dispatch path so the materialised
                    // row set is complete (per-node stats inside this CTE
                    // are forfeited as a result).
                    super::cte::materialize_single_cte(db, session, cte, visible.clone(), registry)?
                } else {
                    // Materialise any inline-derived synthetic CTEs the
                    // owner's display plan references — must be visible
                    // before the owner's plan executes so its CteScan
                    // leaves can resolve. Order is innermost-first
                    // (registration order from the rewriter).
                    if let Some(inline_list) = per_cte_inline.get(&key) {
                        for (inl_key, inl_select) in inline_list {
                            let Some(inl_plan) = cte_plans.get(inl_key) else {
                                continue;
                            };
                            let inl_result =
                                super::cte::with_cte_registry(visible.clone(), || {
                                    materialize_analyzed_inline(
                                        db, txn, inl_key, inl_select, inl_plan, registry,
                                    )
                                })?;
                            visible.insert(inl_key.clone(), std::sync::Arc::new(inl_result));
                        }
                    }
                    super::cte::analyze_materialize_cte(
                        db,
                        session,
                        cte,
                        &cte_plans,
                        cte_extras.get(&key).map(|v| v.as_slice()),
                        &key,
                        txn,
                        visible.clone(),
                        registry,
                    )?
                };
                let elapsed_ms = start.elapsed().as_nanos() as f64 / 1_000_000.0;
                // A spilled CTE reports rows.len() == 0 (rows live on disk);
                // `estimated_rows` then carries the real spilled row count
                // (for a materialised in-memory result it equals rows.len()).
                let rows_count = if result.nrows() == 0 {
                    result.estimated_rows as u64
                } else {
                    result.nrows() as u64
                };
                timings.push((cte.name.clone(), rows_count, elapsed_ms));
                visible.insert(key, std::sync::Arc::new(result));
            }

            // Pre-materialise body-level inline-derived stubs the same way
            // (only when the body's display plan is executable and not
            // lossy — the legacy dispatch fallback handles them natively).
            let body_runnable = body_executable && !body_lossy;
            if body_runnable {
                for (inl_key, inl_select) in &body_inline {
                    let Some(inl_plan) = cte_plans.get(inl_key) else {
                        continue;
                    };
                    let inl_result = super::cte::with_cte_registry(visible.clone(), || {
                        materialize_analyzed_inline(
                            db, txn, inl_key, inl_select, inl_plan, registry,
                        )
                    })?;
                    visible.insert(inl_key.clone(), std::sync::Arc::new(inl_result));
                }
            }

            let body_start = std::time::Instant::now();
            let body_rows = run_with_body_for_analyze(
                db,
                session,
                &with.body,
                &body_plan,
                body_runnable,
                txn,
                &visible,
                registry,
            )?;
            let body_ms = body_start.elapsed().as_nanos() as f64 / 1_000_000.0;
            let total_ms = query_start.elapsed().as_nanos() as f64 / 1_000_000.0;
            Ok((timings, body_rows, body_ms, total_ms))
        });
        let (timings, body_rows, body_ms, total_ms) = inner?;
        Ok((stats, timings, body_rows, body_ms, total_ms))
    };

    let (mut stats, cte_timings, body_rows_count, body_ms, total_ms) =
        if crate::engine::executor::mem_budget::current_budget().is_some() {
            exec_fn()?
        } else {
            let budget =
                std::sync::Arc::new(crate::engine::executor::mem_budget::MemBudget::from_env());
            crate::engine::executor::mem_budget::with_query_budget(budget, exec_fn)?
        };
    complete_fused_trace(
        &body_plan,
        &mut stats,
        Some(NodeStats {
            first_row_ns: (body_ms * 1_000_000.0).round() as u128,
            time_ns: (body_ms * 1_000_000.0).round() as u128,
            rows: body_rows_count,
            loops: 1,
            ..NodeStats::default()
        }),
    );
    // The root owns prerequisite materialization as well as its final pull.
    // Inline-derived spools (including blocking windows) are prepared before
    // body_start, so summing named CTE times and body_ms loses that work.
    if let Some(mut root) = stats.get_for_plan(&body_plan).copied() {
        root.first_row_ns = root
            .first_row_ns
            .saturating_add(((total_ms - body_ms).max(0.0) * 1_000_000.0) as u128);
        root.time_ns = (total_ms * 1_000_000.0).round() as u128;
        stats.insert_for_plan(&body_plan, root);
    }
    for (name, rows, elapsed_ms) in &cte_timings {
        let key = name.to_ascii_lowercase();
        let inherited = NodeStats {
            first_row_ns: (*elapsed_ms * 1_000_000.0).round() as u128,
            time_ns: (*elapsed_ms * 1_000_000.0).round() as u128,
            rows: *rows,
            loops: 1,
            ..NodeStats::default()
        };
        if let Some(plan) = cte_plans.get(&key) {
            complete_fused_trace(plan, &mut stats, Some(inherited));
            let mut visited = std::collections::HashSet::from([key.clone()]);
            complete_referenced_cte_traces(plan, &cte_plans, &mut stats, inherited, &mut visited);
        }
        if let Some(extras) = cte_extras.get(&key) {
            for plan in extras {
                complete_fused_trace(plan, &mut stats, Some(inherited));
            }
        }
    }
    let query_io = stats.query_io();

    // Phase 3: format the same trees with the collected stats.  ANALYZE must
    // not manufacture CTE/body summary nodes or comment headers: every
    // runtime observation belongs to a node already visible in plain
    // EXPLAIN.
    let combined = match format {
        ExplainFormat::Tree => {
            let tree = format_plan_tree(
                &body_plan,
                0,
                Some(&stats),
                Some(&cte_plans),
                Some(&cte_extras),
            );
            prepend_hint_warnings_tree(tree, &hint_warnings)
        }
        ExplainFormat::Json => {
            let json = format_plan_json_with_ctes(
                &body_plan,
                Some((&stats, body_rows_count, total_ms, query_io)),
                Some(&cte_plans),
                Some(&cte_extras),
            );
            inject_hint_warnings_json(json, &hint_warnings)
        }
        ExplainFormat::Traditional => unreachable!("traditional routed away"),
    };
    Ok(QueryResponse::ResultSet(ResultSet {
        columns: vec![Column {
            source: Default::default(),
            name: "EXPLAIN".to_string(),
            column_type: ColumnType::VarString,
        }],
        rows: vec![vec![Value::String(combined)]],
    }))
}

/// Execute the WITH body for EXPLAIN ANALYZE FORMAT=TREE/JSON. When the
/// body plan came from real planning (`executable`), run that exact display
/// tree so the body's operators — including `CteScan` consumers — collect
/// per-node stats. Otherwise (synthetic CTE-in-JOIN plan) fall back to
/// re-dispatching the body for a correct row count without body node stats.
/// Returns the body's actual row count.
#[allow(clippy::too_many_arguments)]
fn run_with_body_for_analyze<'q>(
    db: &Arc<Database>,
    session: &mut SessionContext,
    body: impl Into<crate::parser::QueryRef<'q>>,
    body_plan: &planner::PhysicalPlan,
    executable: bool,
    txn: crate::storage::TxnView,
    visible: &std::collections::HashMap<String, std::sync::Arc<crate::engine::api::CteResult>>,
    registry: &SessionRegistry,
) -> Result<u64, QueryError> {
    let body = body.into();
    let response = if let (true, crate::parser::QueryRef::Select(select)) = (executable, body) {
        // A top-level windowed body has the same plan/dispatch split as a
        // windowed CTE / inline-derived: `planner::plan_select` omits the
        // `WindowAgg` node, so running `body_plan` directly would re-apply the
        // window projection in the legacy evaluator (error 1235). Route through
        // the window-aware path instead (forfeiting body node stats).
        if super::super::dispatch::select_has_window_functions(select) {
            super::cte::with_cte_registry(visible.clone(), || {
                super::super::dispatch::execute_windowed_select(db, txn, select.clone(), registry)
            })?
        } else {
            super::cte::with_cte_registry(visible.clone(), || {
                super::plan::execute_physical_select(db, txn, select, body_plan)
            })?
        }
    } else {
        let body_stmt = match body {
            crate::parser::QueryRef::Other(statement) => statement.clone(),
            query => query
                .to_owned_query()
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        "expected query body",
                    )
                })?
                .into_statement(),
        };
        super::cte::with_cte_registry(visible.clone(), || {
            super::super::dispatch::execute_statement(db, session, body_stmt, registry)
        })?
    };
    count_response_rows(response)
}

/// Drain a `QueryResponse` to its row count, consuming a streaming cursor
/// fully so its operators finish (and record their stats) under ANALYZE.
fn count_response_rows(response: QueryResponse) -> Result<u64, QueryError> {
    Ok(match response {
        QueryResponse::ResultSet(rs) => rs.rows.len() as u64,
        QueryResponse::StreamingResultSet(mut srs) => {
            let mut count = 0u64;
            while srs.cursor.next_row()?.is_some() {
                count += 1;
            }
            count
        }
        _ => 0,
    })
}

/// Build the same consistent-read view used by an ordinary SELECT.
///
/// EXPLAIN ANALYZE executes the target statement, so it must see the current
/// transaction's own writes and reuse a pinned RR snapshot. Creating an
/// unrelated read-only view would make every collected metric describe a
/// different data set from the query being explained.
fn read_view_for_analyze(
    db: &Arc<Database>,
    session: &mut SessionContext,
) -> Result<(TxnView, Option<crate::storage::SnapshotGuard>), QueryError> {
    if session.current_txn.is_some() {
        return super::super::tx::current_transaction_read_select_view(db, session);
    }

    if session.explicit_txn || !session.autocommit {
        let isolation = session.start_transaction_isolation();
        let pinned = isolation
            .uses_repeatable_snapshot()
            .then_some(session.txn_snapshot_scn)
            .flatten();
        if let Some(snapshot_scn) = pinned {
            return Ok((
                db.read_only_statement_view(isolation, Some(snapshot_scn)),
                None,
            ));
        }
        let (txn, guard) = db.read_only_statement_view_with_guard(isolation, None);
        if isolation.uses_repeatable_snapshot() {
            session.pin_transaction_snapshot(db, txn.snapshot_scn);
        } else {
            session.txn_snapshot_scn = Some(txn.snapshot_scn);
        }
        return Ok((txn, Some(guard)));
    }

    let isolation = session.autocommit_statement_isolation();
    let (txn, guard) = db.read_only_statement_view_with_guard(isolation, None);
    Ok((txn, Some(guard)))
}

/// Execute a planned SELECT in place, reusing the supplied
/// `PhysicalPlan` so the per-operator stats collector's pointer keys
/// match the plan tree the formatter is about to walk. Used by EXPLAIN
/// ANALYZE FORMAT=TREE/JSON under the session's real consistent-read view.
fn run_select_for_analyze_with_plan(
    db: &Arc<Database>,
    session: &mut SessionContext,
    compiled: &crate::engine::cache::CompiledSelectPlan,
    select: &Select,
    registry: &SessionRegistry,
) -> Result<u64, QueryError> {
    let (txn, _snapshot_guard) = read_view_for_analyze(db, session)?;
    let started = std::time::Instant::now();
    let io_start = db.buffer_stats_counts();
    // Window functions are orchestrated by `dispatch::execute_windowed_select`
    // (wrap a `WindowAgg`, then post-project `__window__`), not by
    // `planner::plan_select` — so running `plan` directly would re-evaluate the
    // window projection through the legacy evaluator and raise error 1235.
    // Route windowed selects through the window-aware path (forfeiting display
    // node stats, since it re-plans internally; the whole-query total is still
    // measured by the caller's collector scope).
    // Ordinary SELECT enters this scope in `execute_select`. EXPLAIN ANALYZE
    // executes the already-built physical tree directly, so it must install
    // the same statement registry explicitly; SystemScan uses it for
    // PROCESSLIST/session-local producers and must never fall back to a
    // process-global registry.
    let response = super::with_session_registry(registry.clone(), || {
        if super::super::dispatch::select_has_window_functions(select) {
            super::super::dispatch::execute_windowed_select(db, txn, select.clone(), registry)
        } else {
            // Enter the same prepared/compiled execution path as an ordinary
            // SELECT. The collector captures the finalized plan actually
            // opened by this path for post-execution formatting.
            super::execute_compiled_select(db, txn, select, compiled)
        }
    })?;
    let rows = count_response_rows(response)?;
    analyze_stats::record_if_absent_with_io_snapshot(
        db,
        &compiled.physical,
        started.elapsed().as_nanos(),
        rows,
        io_start,
    );
    Ok(rows)
}

fn explain_analyze_window_formatted(
    db: &Arc<Database>,
    session: &mut SessionContext,
    select: &Select,
    format: ExplainFormat,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    let (txn, _snapshot_guard) = read_view_for_analyze(db, session)?;
    let (stats, exec_result) = analyze_stats::with_collector(|| {
        let exec_start = std::time::Instant::now();
        let analyzed = super::super::dispatch::execute_windowed_select_for_analyze(
            db,
            txn,
            select.clone(),
            registry,
        )?;
        let elapsed_ms = exec_start.elapsed().as_nanos() as f64 / 1_000_000.0;
        Ok::<_, QueryError>((analyzed, elapsed_ms))
    });
    let (analyzed, elapsed_ms) = exec_result?;
    let io = stats.query_io();
    let text = match format {
        ExplainFormat::Tree => format_plan_tree(
            &analyzed.plan,
            0,
            Some(&stats),
            Some(&analyzed.cte_plans),
            None,
        ),
        ExplainFormat::Json => format_plan_json_with_ctes(
            &analyzed.plan,
            Some((&stats, analyzed.rows_count, elapsed_ms, io)),
            Some(&analyzed.cte_plans),
            None,
        ),
        ExplainFormat::Traditional => unreachable!("traditional routed away"),
    };
    Ok(QueryResponse::ResultSet(ResultSet {
        columns: vec![Column {
            source: Default::default(),
            name: "EXPLAIN".to_string(),
            column_type: ColumnType::VarString,
        }],
        rows: vec![vec![Value::String(text)]],
    }))
}

/// Surface `PlannedSelect::hint_warnings` to the user by appending them
/// to the first explain row's `Extra` column, prefixed with
/// `hint warning: `. Multiple warnings are joined with `; ` so the
/// existing single-line Extra format is preserved.
fn attach_hint_warnings(rows: &mut [planner::ExplainRow], warnings: &[String]) {
    if warnings.is_empty() || rows.is_empty() {
        return;
    }
    let note = format!("hint warning: {}", warnings.join("; "));
    let first = &mut rows[0];
    if first.extra.is_empty() {
        first.extra = note;
    } else {
        first.extra.push_str("; ");
        first.extra.push_str(&note);
    }
}

/// Prepend a `hint warning:` line to a TREE-format explain string so
/// users see hint diagnostics above the plan tree. No-op when there are
/// no warnings.
fn prepend_hint_warnings_tree(text: String, warnings: &[String]) -> String {
    if warnings.is_empty() {
        return text;
    }
    let mut out = String::new();
    for w in warnings {
        out.push_str("hint warning: ");
        out.push_str(w);
        out.push('\n');
    }
    out.push_str(&text);
    out
}

/// Inject a `"hint_warnings"` array into the JSON explain output. The
/// generator emits `"select_id": 1,\n` as the first field of
/// `query_block`; we splice in the warnings field right after it so
/// downstream JSON parsers see it as a sibling of `select_id`.
fn inject_hint_warnings_json(text: String, warnings: &[String]) -> String {
    if warnings.is_empty() {
        return text;
    }
    let anchor = "\"select_id\": 1,\n";
    let Some(idx) = text.find(anchor) else {
        return text;
    };
    let cut = idx + anchor.len();
    let escaped: Vec<String> = warnings
        .iter()
        .map(|w| format!("\"{}\"", w.replace('\\', "\\\\").replace('"', "\\\"")))
        .collect();
    let inject = format!("    \"hint_warnings\": [{}],\n", escaped.join(", "));
    let mut out = String::with_capacity(text.len() + inject.len());
    out.push_str(&text[..cut]);
    out.push_str(&inject);
    out.push_str(&text[cut..]);
    out
}

pub(super) fn explain_analyze_result_columns() -> Vec<Column> {
    let mut cols = planner::explain_result_columns();
    cols.push(Column {
        source: Default::default(),
        name: "actual_rows".to_string(),
        column_type: ColumnType::Double,
    });
    cols.push(Column {
        source: Default::default(),
        name: "actual_loops".to_string(),
        column_type: ColumnType::LongLong,
    });
    cols.push(Column {
        source: Default::default(),
        name: "actual_first_row_ms".to_string(),
        column_type: ColumnType::Double,
    });
    cols.push(Column {
        source: Default::default(),
        name: "actual_last_row_ms".to_string(),
        column_type: ColumnType::Double,
    });
    cols.push(Column {
        source: Default::default(),
        name: "actual_rows_total".to_string(),
        column_type: ColumnType::LongLong,
    });
    cols
}

fn explain_rows_to_analyze_values(
    rows: Vec<planner::ExplainRow>,
    _total_rows: u64,
    _total_ms: f64,
    stats: &NodeStatsMap,
) -> Vec<Vec<Value>> {
    // Capture each row's originating stable plan-node ID (set by
    // `planner::collect_explain_rows`) before the rows are flattened
    // into raw `Value` vectors.
    let plan_ids: Vec<Option<planner::PlanNodeId>> = rows.iter().map(|r| r.plan_id).collect();
    let mut out = planner::explain_rows_to_values(rows);
    for (idx, row) in out.iter_mut().enumerate() {
        let stat = plan_ids
            .get(idx)
            .and_then(|p| p.as_ref().and_then(|key| stats.get_by_id(*key)))
            .copied();
        if let Some(s) = stat {
            if s.loops == 0 || s.source == analyze_stats::StatsSource::Included {
                row.extend(std::iter::repeat_n(Value::Null, 5));
            } else {
                let loops = s.loops as f64;
                let first = if s.first_row_ns == 0 {
                    s.time_ns
                } else {
                    s.first_row_ns
                };
                row.push(Value::Double(s.rows as f64 / loops));
                row.push(Value::Int(s.loops as i64));
                row.push(Value::Double(first as f64 / loops / 1_000_000.0));
                row.push(Value::Double(s.time_ns as f64 / loops / 1_000_000.0));
                row.push(Value::Int(s.rows as i64));
            }
        } else {
            row.extend(std::iter::repeat_n(Value::Null, 5));
        }
    }
    out
}

fn complete_fused_trace(
    plan: &planner::PhysicalPlan,
    stats: &mut NodeStatsMap,
    inherited: Option<NodeStats>,
) {
    stats.ensure_tree(plan);
    fn visit(
        plan: &planner::PhysicalPlan,
        stats: &mut NodeStatsMap,
        owner: Option<planner::PlanNodeId>,
        root_actual: Option<NodeStats>,
        is_root: bool,
    ) {
        let id = stats.id_for_plan(plan);
        let mut actual = stats.get_for_plan(plan).copied();
        if is_root && actual.as_ref().is_none_or(|value| value.loops == 0) {
            if let Some(mut measured) = root_actual.filter(|value| value.loops > 0) {
                measured.source = analyze_stats::StatsSource::Measured;
                measured.first_row_ns = measured.time_ns;
                stats.insert_for_plan(plan, measured);
                actual = Some(measured);
            }
        } else if actual.is_none() {
            if let Some(owner) = owner {
                let included = NodeStats {
                    source: analyze_stats::StatsSource::Included,
                    included_in: Some(owner),
                    ..NodeStats::default()
                };
                stats.insert_for_plan(plan, included);
                actual = Some(included);
            }
        } else if let (Some(mut details), Some(owner)) = (actual, owner) {
            if details.loops == 0 && details.source != analyze_stats::StatsSource::Included {
                details.source = analyze_stats::StatsSource::Included;
                details.included_in = Some(owner);
                stats.insert_for_plan(plan, details);
                actual = Some(details);
            }
        }

        let short_circuits_input =
            matches!(plan, planner::PhysicalPlan::Limit { limit: Some(0), .. });
        let child_owner = actual.and_then(|value| {
            if short_circuits_input {
                None
            } else if value.source == analyze_stats::StatsSource::Included {
                value.included_in
            } else if value.loops == 0 || value.rows == 0 {
                None
            } else {
                id
            }
        });
        let children = super::plan::plan_stats_children(plan);
        for child in children.iter().copied() {
            visit(child, stats, child_owner, None, false);
        }
        if let Some(mut measured) = stats.get_for_plan(plan).copied() {
            if measured.loops > 0 && measured.self_time_ns == 0 {
                let child_time = children
                    .iter()
                    .filter_map(|child| stats.get_for_plan(child))
                    .filter(|child| child.loops > 0)
                    .fold(0u128, |sum, child| sum.saturating_add(child.time_ns));
                measured.self_time_ns = measured.time_ns.saturating_sub(child_time);
                stats.insert_for_plan(plan, measured);
            }
        }
    }

    visit(plan, stats, None, inherited, true);
}

fn complete_referenced_cte_traces(
    owner: &planner::PhysicalPlan,
    cte_plans: &std::collections::HashMap<String, planner::PhysicalPlan>,
    stats: &mut NodeStatsMap,
    inherited: NodeStats,
    visited: &mut std::collections::HashSet<String>,
) {
    fn first_setop(plan: &planner::PhysicalPlan) -> Option<&planner::PhysicalPlan> {
        if matches!(plan, planner::PhysicalPlan::SetOp { .. }) {
            return Some(plan);
        }
        for child in super::plan::plan_stats_children(plan) {
            if let Some(found) = first_setop(child) {
                return Some(found);
            }
        }
        None
    }

    let mut names = Vec::new();
    owner.walk(|node| {
        if let planner::PhysicalPlan::CteScan { name, .. } = node {
            names.push(name.to_ascii_lowercase());
        }
    });
    for name in names {
        if !visited.insert(name.clone()) {
            continue;
        }
        let Some(referenced) = cte_plans.get(&name) else {
            continue;
        };
        stats.ensure_tree(referenced);
        if let Some(setop) = first_setop(referenced) {
            if let Some(actual) = stats.aggregate_setop_stats(setop) {
                stats.insert_for_plan(setop, actual);
            }
        }
        complete_fused_trace(referenced, stats, Some(inherited));
        complete_referenced_cte_traces(referenced, cte_plans, stats, inherited, visited);
    }
}

/// Generate EXPLAIN rows for a `WITH ... body` statement. Each CTE produces
/// its own block of rows tagged with `select_type` "CTE" (non-recursive) or
/// "RECURSIVE_CTE", followed by the body's rows tagged "PRIMARY".
fn explain_with_cte(
    db: &Arc<Database>,
    current_db: &str,
    with: crate::parser::WithCte,
    format: ExplainFormat,
) -> Result<QueryResponse, QueryError> {
    if matches!(format, ExplainFormat::Tree | ExplainFormat::Json) {
        return explain_with_cte_formatted(db, current_db, with, format);
    }
    let mut rows: Vec<planner::ExplainRow> = Vec::new();
    // Stub each CTE name in the thread-local registry so EXPLAIN of the body
    // recognises them as CTE references rather than missing tables. Columns are
    // computed from each CTE body's projection (not just the declared column
    // list, which is usually absent in TPC-DS) so references like
    // `cross_items.ss_item_sk` resolve. Built incrementally so a CTE that
    // references an earlier one sees its columns.
    let mut stubs: std::collections::HashMap<
        String,
        std::sync::Arc<crate::engine::api::CteResult>,
    > = std::collections::HashMap::new();
    for cte in &with.ctes {
        let columns = cte_stub_columns(db, cte, &stubs);
        stubs.insert(
            cte.name.to_ascii_lowercase(),
            std::sync::Arc::new(crate::engine::api::CteResult::stub_for_explain(
                columns, 0.0,
            )),
        );
    }
    let body_rows = super::cte::with_cte_registry(
        stubs,
        || -> Result<Vec<planner::ExplainRow>, QueryError> {
            for cte in &with.ctes {
                let cte_rows = explain_cte_body(db, cte)?;
                rows.extend(cte_rows);
            }
            explain_statement_body(db, &with.body)
        },
    )?;
    let body_start = rows.len();
    rows.extend(body_rows);
    for row in rows.iter_mut().skip(body_start) {
        if row.select_type == "SIMPLE" || row.select_type == "DERIVED" {
            row.select_type = "PRIMARY".to_string();
        }
    }
    renumber_explain_rows(&mut rows);
    Ok(QueryResponse::ResultSet(ResultSet {
        columns: planner::explain_result_columns(),
        rows: planner::explain_rows_to_values(rows),
    }))
}

/// EXPLAIN FORMAT=TREE/JSON for WITH-CTE queries. Plans each CTE's inner
/// query and the body query, then formats the combined tree with CTE sub-plans
/// shown as children of their CteScan nodes.
/// Plan the display trees for a `WITH` statement: the body plan plus each
/// CTE body's plan, keyed by lowercased CTE name. CTE bodies are planned
/// under a stub registry (empty rows, but a real estimated-row count so
/// downstream `CteScan` references get a sensible CBO estimate). Shared by
/// the plain EXPLAIN FORMAT=TREE/JSON path and EXPLAIN ANALYZE.
///
/// Returns `(body_plan, cte_plans, cte_extras, hint_warnings, body_executable,
/// per_cte_inline, lossy_ctes, body_inline, body_lossy)`. The last four are
/// only consumed by EXPLAIN ANALYZE Phase 2 — plain EXPLAIN discards them.
///
/// `per_cte_inline[k]` lists the (alias, prepared inner SELECT) pairs that
/// `plan_cte_inner_for_explain` registered while planning CTE `k`'s body, in
/// innermost-first order. Phase 2 materialises each one (via the prepared
/// plan in `cte_plans[alias]`) before materialising the owning CTE so the
/// owner's display plan — which leaves CteScan stubs in their place — can
/// resolve them.
///
/// `lossy_ctes` (and `body_lossy`) collect CTE keys whose display plan is a
/// strict subset of the real query — currently only when an inline-derived
/// source's inner is a set operation, since the display planner inlines its
/// leftmost branch only. Phase 2 falls back to legacy dispatch
/// materialisation for those owners so the returned row counts stay correct
/// (at the cost of per-operator stats inside them).
type WithCteExplainPlan = (
    planner::PhysicalPlan,
    std::collections::HashMap<String, planner::PhysicalPlan>,
    CteExtraBranches,
    Vec<String>,
    bool,
    std::collections::HashMap<String, Vec<(String, InlineDerivedSelect)>>,
    std::collections::HashSet<String>,
    Vec<(String, InlineDerivedSelect)>,
    bool,
);

fn plan_with_cte_for_explain(
    db: &Arc<Database>,
    with: &crate::parser::WithCte,
) -> Result<WithCteExplainPlan, QueryError> {
    use std::collections::{HashMap, HashSet};

    let mut stubs: HashMap<String, std::sync::Arc<crate::engine::api::CteResult>> = HashMap::new();
    for cte in &with.ctes {
        let columns = cte_stub_columns(db, cte, &stubs);
        stubs.insert(
            cte.name.to_ascii_lowercase(),
            std::sync::Arc::new(crate::engine::api::CteResult::stub_for_explain(
                columns, 0.0,
            )),
        );
    }

    super::cte::with_cte_registry(stubs, || {
        let mut cte_plans: HashMap<String, planner::PhysicalPlan> = HashMap::new();
        let mut cte_extras: CteExtraBranches = HashMap::new();
        let mut warnings: Vec<String> = Vec::new();
        let mut per_cte_inline: HashMap<String, Vec<(String, InlineDerivedSelect)>> =
            HashMap::new();
        let mut lossy_ctes: HashSet<String> = HashSet::new();
        for cte in &with.ctes {
            let mut local_inline: Vec<(String, InlineDerivedSelect)> = Vec::new();
            let mut local_lossy = false;
            if let Some((plan, extra_plans, mut cte_warnings, columns)) = plan_cte_inner_for_explain(
                db,
                cte,
                &mut cte_plans,
                &mut local_inline,
                &mut local_lossy,
            ) {
                // No materialisation happens during EXPLAIN, so the stub's
                // `rows` is empty. Carry the CTE body plan's estimated row
                // count into the registry stub so that `CteScan` references
                // in later CTEs and in the body get a real CBO estimate
                // instead of collapsing to rows=0. For UNION ALL bodies,
                // sum across branches.
                let mut est = planner::estimated_rows(&plan);
                for extra in &extra_plans {
                    est += planner::estimated_rows(extra);
                }
                if super::cte::lookup_cte(&cte.name).is_some() {
                    super::cte::set_cte_result(
                        &cte.name,
                        std::sync::Arc::new(crate::engine::api::CteResult::stub_for_explain(
                            columns, est,
                        )),
                    );
                }
                let key = cte.name.to_ascii_lowercase();
                cte_plans.insert(key.clone(), plan);
                if !extra_plans.is_empty() {
                    cte_extras.insert(key.clone(), extra_plans);
                }
                if !local_inline.is_empty() {
                    per_cte_inline.insert(key.clone(), local_inline);
                }
                if local_lossy {
                    lossy_ctes.insert(key);
                }
                warnings.append(&mut cte_warnings);
            }
        }
        let mut body_inline: Vec<(String, InlineDerivedSelect)> = Vec::new();
        let mut body_lossy = false;
        let (body_plan, mut body_warnings, executable) = plan_body_for_explain(
            db,
            &with.body,
            &mut cte_plans,
            &mut body_inline,
            &mut body_lossy,
        )?;
        warnings.append(&mut body_warnings);
        Ok((
            body_plan,
            cte_plans,
            cte_extras,
            warnings,
            executable,
            per_cte_inline,
            lossy_ctes,
            body_inline,
            body_lossy,
        ))
    })
}

fn explain_with_cte_formatted(
    db: &Arc<Database>,
    current_db: &str,
    mut with: crate::parser::WithCte,
    format: ExplainFormat,
) -> Result<QueryResponse, QueryError> {
    apply_with_cte_explain_rewrites(db, current_db, &mut with);
    let (
        body_plan,
        cte_plans,
        cte_extras,
        hint_warnings,
        _executable,
        _per_cte_inline,
        _lossy_ctes,
        _body_inline,
        _body_lossy,
    ) = plan_with_cte_for_explain(db, &with)?;

    let text = match format {
        ExplainFormat::Tree => {
            let tree = format_plan_tree(&body_plan, 0, None, Some(&cte_plans), Some(&cte_extras));
            prepend_hint_warnings_tree(tree, &hint_warnings)
        }
        ExplainFormat::Json => {
            let json =
                format_plan_json_with_ctes(&body_plan, None, Some(&cte_plans), Some(&cte_extras));
            inject_hint_warnings_json(json, &hint_warnings)
        }
        ExplainFormat::Traditional => unreachable!(),
    };
    Ok(QueryResponse::ResultSet(ResultSet {
        columns: vec![Column {
            source: Default::default(),
            name: "EXPLAIN".to_string(),
            column_type: ColumnType::VarString,
        }],
        rows: vec![vec![Value::String(text)]],
    }))
}

fn apply_with_cte_explain_rewrites(
    db: &Arc<Database>,
    current_db: &str,
    with: &mut crate::parser::WithCte,
) {
    // Run the exact same rewrite sequence as execution (including single-use /
    // IN-subquery-only CTE inlining) so the displayed plan reflects what runs.
    super::cte::apply_pre_materialization_rewrites(
        db,
        current_db,
        &mut with.ctes,
        with.body.as_mut(),
        true,
    );
}

/// Plan a CTE's inner query for EXPLAIN display. Returns the physical
/// plan plus any hint warnings the planner produced for this CTE.
/// Returns None when planning fails (callers tolerate missing CTEs).
fn plan_cte_inner_for_explain(
    db: &Arc<Database>,
    cte: &crate::parser::CteDef,
    cte_plans: &mut std::collections::HashMap<String, planner::PhysicalPlan>,
    inline_derived: &mut Vec<(String, InlineDerivedSelect)>,
    is_lossy: &mut bool,
) -> Option<(
    planner::PhysicalPlan,
    Vec<planner::PhysicalPlan>,
    Vec<String>,
    Vec<Column>,
)> {
    fn plan_one(
        db: &Arc<Database>,
        select: &Select,
        cte_plans: &mut std::collections::HashMap<String, planner::PhysicalPlan>,
        inline_derived: &mut Vec<(String, InlineDerivedSelect)>,
        is_lossy: &mut bool,
    ) -> Option<(planner::PhysicalPlan, Vec<String>)> {
        refresh_stale_stats_for_select(db, select).ok()?;
        if let Ok(planned) = planner::plan_select(db, select) {
            return Some((planned.physical, planned.hint_warnings));
        }
        // The CTE body itself carries inline derived sources (e.g. q02's
        // `wscs` body wraps `(SELECT ... UNION ALL SELECT ...) WS_FROM`).
        // Stub them as nested CTEs and re-plan.
        let has_inline_derived =
            select.derived.is_some() || select.joins.iter().any(|j| j.derived.is_some());
        if has_inline_derived
            && !select
                .joins
                .iter()
                .any(|j| j.derived.is_some() && j.lateral)
        {
            let mut rewritten = select.clone();
            let mut warnings: Vec<String> = Vec::new();
            if let Ok(plan) = plan_select_with_inline_derived_for_explain(
                db,
                &mut rewritten,
                cte_plans,
                &mut warnings,
                inline_derived,
                is_lossy,
            ) {
                return Some((plan, warnings));
            }
        }
        None
    }

    match &cte.query {
        crate::parser::QueryExpr::Select(select) => {
            let (plan, warnings) = plan_one(db, select, cte_plans, inline_derived, is_lossy)?;
            let columns = cte_columns_for_explain(db, cte, select)?;
            Some((
                planner::materialize_plan(plan, cte.name.clone()),
                Vec::new(),
                warnings,
                columns,
            ))
        }
        crate::parser::QueryExpr::SetOp(set) => {
            if cte.is_recursive
                && super::super::cte::statement_references_cte(&cte.query, &cte.name)
            {
                let (anchor_statement, branches, all) =
                    super::super::cte::recursive_parts(cte).ok()?;
                let anchor = planner::plan_query_physical(db, &anchor_statement).ok()??;
                let mut statements = branches.into_iter().map(crate::parser::QueryExpr::from);
                let mut recursive_statement = statements.next()?;
                for branch in statements {
                    recursive_statement =
                        crate::parser::QueryExpr::from(crate::parser::SetOperation {
                            op: crate::parser::SetOp::Union,
                            all: true,
                            left: recursive_statement,
                            right: branch,
                            order_by: Vec::new(),
                            limit: None,
                            offset: None,
                        });
                }
                let recursive = planner::plan_query_physical(db, &recursive_statement).ok()??;
                let mut columns = super::describe_subquery_columns(db, &anchor_statement).ok()?;
                for (column, alias) in columns.iter_mut().zip(&cte.columns) {
                    column.name = alias.clone();
                }
                let depth = crate::engine::expr::current_session_context_snapshot()
                    .map(|session| session.cte_max_recursion_depth)
                    .unwrap_or(1000);
                let recursive_union =
                    planner::recursive_union_plan(anchor, recursive, cte.name.clone(), all, depth);
                return Some((
                    planner::materialize_plan(recursive_union, cte.name.clone()),
                    Vec::new(),
                    Vec::new(),
                    columns,
                ));
            }
            // Non-recursive `UNION ALL` CTE bodies: plan every branch so the
            // display tree renders the full union and EXPLAIN ANALYZE can
            // drive each branch through its own physical plan (per-operator
            // stats then line up by node pointer for *both* sides, instead
            // of the LEFT-only fallback below).
            if !cte.is_recursive
                && !super::super::cte::statement_references_cte(&cte.query, &cte.name)
            {
                if let Some(branches) = flatten_union_all_branches(&cte.query) {
                    let mut plans = Vec::with_capacity(branches.len());
                    let mut all_warnings: Vec<String> = Vec::new();
                    let mut columns: Option<Vec<Column>> = None;
                    for select in &branches {
                        let (plan, warnings) =
                            plan_one(db, select, cte_plans, inline_derived, is_lossy)?;
                        if columns.is_none() {
                            columns = cte_columns_for_explain(db, cte, select);
                        }
                        plans.push(plan);
                        all_warnings.extend(warnings);
                    }
                    let columns = columns?;
                    let mut iter = plans.into_iter();
                    let head = iter.next()?;
                    let tail: Vec<_> = iter.collect();
                    return Some((head, tail, all_warnings, columns));
                }
            }
            // Recursive CTEs (or non-plain set ops): plan the anchor (left)
            // side only, mirroring the historical display behaviour.
            if let crate::parser::QueryExpr::Select(select) = &set.left {
                let (plan, warnings) = plan_one(db, select, cte_plans, inline_derived, is_lossy)?;
                let columns = cte_columns_for_explain(db, cte, select)?;
                Some((plan, Vec::new(), warnings, columns))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Best-effort output columns for a CTE stub used during EXPLAIN. Computes them
/// from the CTE body's projection (so an undeclared column list still resolves
/// references), with `prior` holding stubs for CTEs declared earlier in the
/// same WITH. Falls back to the declared names (or empty `VarString` stubs)
/// when the body can't be described (e.g. a set-operation body).
fn cte_stub_columns(
    db: &Arc<Database>,
    cte: &crate::parser::CteDef,
    prior: &std::collections::HashMap<String, std::sync::Arc<crate::engine::api::CteResult>>,
) -> Vec<Column> {
    if cte.is_recursive && super::super::cte::statement_references_cte(&cte.query, &cte.name) {
        if let Ok((anchor, _, _)) = super::super::cte::recursive_parts(cte) {
            if let Ok(mut columns) = super::cte::with_cte_registry_extended(prior.clone(), || {
                super::describe_subquery_columns(db, &anchor)
            }) {
                for (column, alias) in columns.iter_mut().zip(&cte.columns) {
                    column.name = alias.clone();
                }
                return columns;
            }
        }
    }
    if let crate::parser::QueryExpr::Select(select) = &cte.query {
        let computed = super::cte::with_cte_registry_extended(prior.clone(), || {
            cte_columns_for_explain(db, cte, select)
        });
        if let Some(columns) = computed {
            return columns;
        }
        // `describe_select_columns` can't resolve a multi-source body (a
        // derived FROM comma-joined with base tables, e.g. q05's `ssr`). Fall
        // back to the projection's output *names* — enough for EXPLAIN of the
        // body to resolve `cte.col` references even without column types.
        if let Some(names) = projection_output_names(select) {
            let names = if !cte.columns.is_empty() && cte.columns.len() == names.len() {
                cte.columns.clone()
            } else {
                names
            };
            return names
                .into_iter()
                .map(|name| Column {
                    source: Default::default(),
                    name,
                    column_type: ColumnType::VarString,
                })
                .collect();
        }
    }
    cte.columns
        .iter()
        .map(|n| Column {
            source: Default::default(),
            name: n.clone(),
            column_type: ColumnType::VarString,
        })
        .collect()
}

/// Output column names of a SELECT derived purely from its projection list:
/// the alias when present, otherwise a bare/compound identifier's final
/// component. Returns `None` if any item is a wildcard or an unaliased complex
/// expression (whose name can't be inferred without resolving the FROM scope).
fn projection_output_names(select: &Select) -> Option<Vec<String>> {
    let mut names = Vec::with_capacity(select.projection.len());
    for item in &select.projection {
        match item {
            SelectItem::Expr {
                alias: Some(alias), ..
            } => names.push(alias.clone()),
            SelectItem::Expr {
                expr: Expr::Identifier(name),
                alias: None,
            } => names.push(name.clone()),
            SelectItem::Expr {
                expr: Expr::CompoundIdentifier(parts),
                alias: None,
            } => names.push(parts.last()?.clone()),
            _ => return None,
        }
    }
    Some(names)
}

fn cte_columns_for_explain(
    db: &Arc<Database>,
    cte: &crate::parser::CteDef,
    select: &Select,
) -> Option<Vec<Column>> {
    let mut columns = super::describe_select_columns(db, select).ok()?;
    if !cte.columns.is_empty() {
        if columns.len() != cte.columns.len() {
            return None;
        }
        for (column, alias) in columns.iter_mut().zip(cte.columns.iter()) {
            column.name = alias.clone();
        }
    }
    Some(columns)
}

/// Plan the body of a WITH statement for EXPLAIN FORMAT=TREE/JSON.
/// Tries normal plan_select first; on failure (e.g. CTE in JOIN), builds
/// a synthetic plan using CteScan for CTE references.
/// Returns `(plan, hint_warnings, executable)`. `executable` is true when
/// the plan came from the real `plan_select` path (so EXPLAIN ANALYZE may
/// run that exact tree to collect per-operator stats) and false for the
/// synthetic CTE-in-JOIN fallback (which is display-only and must not be
/// executed directly).
fn plan_body_for_explain<'q>(
    db: &Arc<Database>,
    body: impl Into<crate::parser::QueryRef<'q>>,
    cte_plans: &mut std::collections::HashMap<String, planner::PhysicalPlan>,
    body_inline: &mut Vec<(String, InlineDerivedSelect)>,
    body_is_lossy: &mut bool,
) -> Result<(planner::PhysicalPlan, Vec<String>, bool), QueryError> {
    let body = body.into();
    let crate::parser::QueryRef::Select(select) = body else {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "EXPLAIN FORMAT=TREE for WITH body requires SELECT",
        ));
    };
    if select.table.is_none() {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "constant SELECT in WITH body does not produce a plan tree",
        ));
    }
    refresh_stale_stats_for_select(db, select).ok();
    // Try normal planning first — works when CTE is only in FROM (no joins)
    // or when all join tables are real catalog tables.
    if let Ok(planned) = planner::plan_select(db, select) {
        return Ok((planned.physical, planned.hint_warnings, true));
    }
    // Normal planning failed. If the body has inline derived sources
    // (e.g. q02-style `FROM (SELECT ...) y, (SELECT ...) z`), stub each one
    // as a CTE and plan recursively, contributing sub-plans to `cte_plans`.
    let has_inline_derived =
        select.derived.is_some() || select.joins.iter().any(|j| j.derived.is_some());
    if has_inline_derived
        && !select
            .joins
            .iter()
            .any(|j| j.derived.is_some() && j.lateral)
    {
        let mut rewritten = select.clone();
        let mut warnings: Vec<String> = Vec::new();
        if let Ok(plan) = plan_select_with_inline_derived_for_explain(
            db,
            &mut rewritten,
            cte_plans,
            &mut warnings,
            body_inline,
            body_is_lossy,
        ) {
            return Ok((plan, warnings, true));
        }
    }
    // Final fallback: synthetic plan (CTE-in-JOIN with no inline derived).
    // The synthetic path bypasses `plan_select`, so no hint warnings.
    build_synthetic_body_plan(db, select).map(|plan| (plan, Vec::new(), false))
}

/// Build a synthetic plan tree for a body SELECT that has CTEs in JOINs.
fn build_synthetic_body_plan(
    db: &Arc<Database>,
    select: &Select,
) -> Result<planner::PhysicalPlan, QueryError> {
    let mut leaves: Vec<planner::PhysicalPlan> = Vec::new();
    if let Some(base) = select.from.first() {
        leaves.push(plan_leaf_for_explain(db, base)?);
    }
    for join in &select.joins {
        leaves.push(plan_leaf_for_explain(db, &join.table)?);
    }
    if leaves.is_empty() {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "empty FROM in WITH body",
        ));
    }
    let mut plan = leaves.remove(0);
    for (right, join) in leaves.into_iter().zip(select.joins.iter()) {
        let left_rows = planner::estimated_rows(&plan);
        let right_rows = planner::estimated_rows(&right);
        let est = (left_rows * right_rows * 0.1).max(1.0);
        let cost_val = planner::plan_cost(&plan).total + planner::plan_cost(&right).total + est;
        plan = planner::PhysicalPlan::NestedLoopJoin {
            left: Box::new(plan),
            right: Box::new(right),
            // Preserve the source join type (FULL/LEFT/…) so the synthetic
            // display tree reflects the real shape rather than always INNER.
            join_type: join.join_type.clone(),
            condition: None,
            estimated_rows: est,
            cost: Cost::scalar(cost_val),
        };
    }
    // Wrap with aggregate if needed
    if !select.group_by.is_empty() || select_projection_has_aggregate(&select.projection) {
        let input_rows = planner::estimated_rows(&plan);
        let group_count = if select.group_by.is_empty() {
            1.0
        } else {
            (input_rows * 0.25).max(1.0)
        };
        let cost_val = planner::plan_cost(&plan).total + input_rows * 0.01;
        plan = planner::PhysicalPlan::HashAggregate {
            input: Box::new(plan),
            group_by: select.group_by.clone(),
            having: select.having.clone(),
            aggregates: Vec::new(),
            estimated_rows: group_count,
            cost: Cost::scalar(cost_val),
        };
    }
    // Wrap with sort if ORDER BY present
    if !select.order_by.is_empty() {
        let est = planner::estimated_rows(&plan);
        let cost_val = planner::plan_cost(&plan).total + est * (est.max(1.0).ln());
        plan = planner::PhysicalPlan::Sort {
            input: Box::new(plan),
            order_by: select.order_by.clone(),
            estimated_rows: est,
            cost: Cost::scalar(cost_val),
        };
    }
    // Wrap with limit if present
    if select.limit.is_some() || select.offset.is_some() {
        let limit_val = select.limit.as_ref().and_then(|l| l.as_usize());
        let offset_val = select.offset.as_ref().and_then(|o| o.as_usize());
        let est = limit_val
            .map(|l| (l as f64).min(planner::estimated_rows(&plan)))
            .unwrap_or_else(|| planner::estimated_rows(&plan));
        let cost = planner::plan_cost(&plan);
        plan = planner::PhysicalPlan::Limit {
            input: Box::new(plan),
            limit: limit_val,
            offset: offset_val,
            binding: planner::LimitBinding::default(),
            estimated_rows: est,
            cost,
        };
    }
    Ok(plan)
}

/// Plan a single leaf table for the synthetic body plan.
fn plan_leaf_for_explain(
    db: &Arc<Database>,
    table: &crate::parser::TableRef,
) -> Result<planner::PhysicalPlan, QueryError> {
    // Try CTE first
    if let Some(scan) = planner::try_plan_cte_scan(table) {
        return Ok(scan);
    }
    // Plan as a real table via a minimal single-table SELECT
    let minimal = Select {
        projection: vec![crate::parser::SelectItem::Wildcard],
        from: vec![table.clone()],
        table: Some(table.name.clone()),
        ..Select::default()
    };
    refresh_stale_stats_for_select(db, &minimal).ok();
    let planned = planner::plan_select(db, &minimal)?;
    Ok(planned.physical)
}

fn explain_cte_body(
    db: &Arc<Database>,
    cte: &crate::parser::CteDef,
) -> Result<Vec<planner::ExplainRow>, QueryError> {
    let label = if cte.is_recursive {
        "RECURSIVE_CTE"
    } else {
        "CTE"
    };
    let body_rows = explain_statement_body(db, &cte.query)?;
    if body_rows.is_empty() {
        return Ok(vec![planner::ExplainRow {
            id: 0,
            select_type: label.to_string(),
            table: format!("<{}>", cte.name),
            access_type: "ALL".to_string(),
            partitions: "NULL".to_string(),
            possible_keys: String::new(),
            key: String::new(),
            key_len: String::new(),
            ref_: String::new(),
            rows: 0,
            filtered: 0.0,
            extra: format!("CTE materialised: {}", cte.name),
            plan_id: None,
        }]);
    }
    let mut rows = body_rows;
    for row in rows.iter_mut() {
        row.select_type = label.to_string();
        append_explain_extra(&mut row.extra, &format!("CTE: {}", cte.name));
    }
    Ok(rows)
}

fn explain_statement_body<'q>(
    db: &Arc<Database>,
    stmt: impl Into<crate::parser::QueryRef<'q>>,
) -> Result<Vec<planner::ExplainRow>, QueryError> {
    let stmt = stmt.into();
    match stmt {
        crate::parser::QueryRef::Select(select) => {
            // If the body references a CTE in FROM/JOIN, emit synthetic rows
            // (the planner doesn't know about CTEs; running plan_select would
            // fail with "table not found").
            if super::cte::has_cte(select.from.first().map(|t| t.name.as_str()).unwrap_or(""))
                || select
                    .joins
                    .iter()
                    .any(|j| super::cte::has_cte(&j.table.name))
            {
                return Ok(synth_cte_referencing_explain(select));
            }
            if needs_multi_source_explain(select) {
                return explain_multi_source_select(db, select);
            }
            if select.derived.is_some() {
                let derived_rows = match execute_derived_explain(db, select)? {
                    QueryResponse::ResultSet(rs) => rs,
                    _ => unreachable!(),
                };
                return Ok(decode_explain_rs(derived_rows));
            }
            if select_has_window_in_projection(select) {
                let with_window = match explain_select_with_window(db, select)? {
                    QueryResponse::ResultSet(rs) => rs,
                    _ => unreachable!(),
                };
                return Ok(decode_explain_rs(with_window));
            }
            if select.table.is_none() {
                return Ok(Vec::new());
            }
            refresh_stale_stats_for_select(db, select)?;
            let planned = planner::plan_select(db, select)?;
            let mut rows = planner::explain_rows(&planned.physical);
            append_select_subquery_explain_rows(db, select, &mut rows)?;
            Ok(rows)
        }
        crate::parser::QueryRef::SetOperation(set) => {
            let mut rows = explain_statement_body(db, &set.left)?;
            rows.extend(explain_statement_body(db, &set.right)?);
            for row in rows.iter_mut() {
                if row.select_type == "SIMPLE" {
                    row.select_type = "UNION".to_string();
                }
            }
            Ok(rows)
        }
        crate::parser::QueryRef::WithCte(_)
        | crate::parser::QueryRef::Other(Statement::Insert(_))
        | crate::parser::QueryRef::Other(Statement::Update(_))
        | crate::parser::QueryRef::Other(Statement::Delete(_)) => Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "EXPLAIN of nested DML/WITH bodies is not yet supported",
        )),
        _ => Ok(Vec::new()),
    }
}

/// Convert a result-set whose rows match the explain schema back into typed
/// ExplainRow structs. Used to feed `execute_derived_explain` output back
/// into the WITH-CTE explain pipeline.
fn decode_explain_rs(rs: ResultSet) -> Vec<planner::ExplainRow> {
    rs.rows
        .into_iter()
        .map(|row| {
            let s = |i: usize| -> String {
                row.get(i)
                    .map(|v| match v {
                        Value::String(s) => s.clone(),
                        Value::Null => String::new(),
                        Value::Int(i) => i.to_string(),
                        Value::UInt(u) => u.to_string(),
                        Value::Double(f) => f.to_string(),
                        _ => String::new(),
                    })
                    .unwrap_or_default()
            };
            let int_at = |i: usize| -> usize {
                match row.get(i) {
                    Some(Value::Int(v)) => *v as usize,
                    Some(Value::UInt(v)) => *v as usize,
                    _ => 0,
                }
            };
            let float_at = |i: usize| -> f64 {
                match row.get(i) {
                    Some(Value::Double(v)) => *v,
                    Some(Value::Int(v)) => *v as f64,
                    _ => 0.0,
                }
            };
            planner::ExplainRow {
                id: int_at(0),
                select_type: s(1),
                table: s(2),
                partitions: s(3),
                access_type: s(4),
                possible_keys: s(5),
                key: s(6),
                key_len: s(7),
                ref_: s(8),
                rows: int_at(9),
                filtered: float_at(10),
                extra: s(11),
                plan_id: None,
            }
        })
        .collect()
}

/// Emit synthetic EXPLAIN rows for a SELECT whose FROM/JOIN targets are CTE
/// references. The planner doesn't know about CTEs, so we describe each
/// reference as an `ALL` scan of the materialised result.
fn synth_cte_referencing_explain(select: &Select) -> Vec<planner::ExplainRow> {
    let mut rows = Vec::new();
    if let Some(table) = select.from.first() {
        rows.push(planner::ExplainRow {
            id: 0,
            select_type: "SIMPLE".to_string(),
            table: table.alias.clone().unwrap_or_else(|| table.name.clone()),
            partitions: "NULL".to_string(),
            access_type: "ALL".to_string(),
            possible_keys: String::new(),
            key: String::new(),
            key_len: String::new(),
            ref_: String::new(),
            rows: 0,
            filtered: 100.0,
            extra: format!("CTE scan: {}", table.name),
            plan_id: None,
        });
    }
    for join in &select.joins {
        rows.push(planner::ExplainRow {
            id: 0,
            select_type: "SIMPLE".to_string(),
            table: join
                .table
                .alias
                .clone()
                .unwrap_or_else(|| join.table.name.clone()),
            partitions: "NULL".to_string(),
            access_type: format!("{:?} join", join.join_type),
            possible_keys: String::new(),
            key: String::new(),
            key_len: String::new(),
            ref_: String::new(),
            rows: 0,
            filtered: 100.0,
            extra: if super::cte::has_cte(&join.table.name) {
                format!("CTE scan: {}", join.table.name)
            } else {
                String::new()
            },
            plan_id: None,
        });
    }
    rows
}

fn select_has_window_in_projection(select: &Select) -> bool {
    select.projection.iter().any(|item| match item {
        SelectItem::Expr { expr, .. } => crate::parser::visit::has_window_expression(expr),
        _ => false,
    })
}

/// Generate EXPLAIN rows for a SELECT whose projection contains window
/// functions: explain the inner (windowless) plan, then append a synthetic
/// row describing the window step.
fn explain_select_with_window(
    db: &Arc<Database>,
    select: &Select,
) -> Result<QueryResponse, QueryError> {
    // Build a stripped Select (no projection windows) so the regular planner
    // can describe the underlying scan.
    let mut inner = select.clone();
    inner.projection = vec![SelectItem::Wildcard];
    inner.order_by.clear();
    inner.limit = None;
    inner.offset = None;
    let mut rows = if inner.table.is_some() && inner.derived.is_none() {
        refresh_stale_stats_for_select(db, &inner)?;
        let planned = planner::plan_select(db, &inner)?;
        planner::explain_rows(&planned.physical)
    } else if inner.derived.is_some() {
        let response = execute_derived_explain(db, &inner)?;
        match response {
            QueryResponse::ResultSet(rs) => decode_explain_rs(rs),
            _ => Vec::new(),
        }
    } else {
        Vec::new()
    };
    // Collect window-function descriptions.
    let mut win_descs: Vec<String> = Vec::new();
    for item in &select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            collect_window_descriptions(expr, &mut win_descs);
        }
    }
    if !win_descs.is_empty() {
        rows.push(planner::ExplainRow {
            id: 0,
            select_type: "WINDOW".to_string(),
            table: "<window>".to_string(),
            partitions: "NULL".to_string(),
            access_type: "ALL".to_string(),
            possible_keys: String::new(),
            key: String::new(),
            key_len: String::new(),
            ref_: String::new(),
            rows: 0,
            filtered: 0.0,
            extra: format!("Window functions: {}", win_descs.join(", ")),
            plan_id: None,
        });
    }
    renumber_explain_rows(&mut rows);
    Ok(QueryResponse::ResultSet(ResultSet {
        columns: planner::explain_result_columns(),
        rows: planner::explain_rows_to_values(rows),
    }))
}

fn collect_window_descriptions(expr: &Expr, out: &mut Vec<String>) {
    crate::parser::visit::inspect_value_expr(
        expr,
        |node| {
            if let Expr::WindowFunction { name, spec, .. } = node {
                let crate::parser::WindowSpec {
                    partition_by,
                    order_by,
                    frame,
                    ..
                } = spec.as_ref();
                let partition_part = if partition_by.is_empty() {
                    String::new()
                } else {
                    format!(" PARTITION BY {}", partition_by.len())
                };

                let order_part = if order_by.is_empty() {
                    String::new()
                } else {
                    format!(" ORDER BY {}", order_by.len())
                };
                let frame_part = match frame {
                    Some(f) => format!(" {:?}", f.units),
                    None => String::new(),
                };
                out.push(format!(
                    "{}(){}{}{}",
                    name.to_uppercase(),
                    partition_part,
                    order_part,
                    frame_part
                ));
            }
            matches!(
                node,
                Expr::Unary { .. }
                    | Expr::IsNull(_)
                    | Expr::IsNotNull(_)
                    | Expr::Binary { .. }
                    | Expr::Function { .. }
            )
        },
        |_| {},
    );
}

pub(super) fn execute_derived_explain(
    db: &Arc<Database>,
    select: &Select,
) -> Result<QueryResponse, QueryError> {
    let Some(sub) = &select.derived else {
        unreachable!();
    };
    // For a set-operation derived table, explain its leftmost SELECT as a
    // representative of the DERIVED subplan (a full per-branch breakdown is a
    // later refinement).
    let derived = sub.leftmost_select().ok_or_else(|| {
        error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "cannot explain derived set operation",
        )
    })?;
    // A derived table whose body is itself a derived table / multi-source CTE
    // join can't be handed straight to `plan_select` (it would fail to resolve
    // the inner alias, e.g. TPC-DS q67's `(… ) dw1) dw2`). Recurse so EXPLAIN
    // follows the nesting down; otherwise take the plain single-plan path,
    // unchanged.
    let recursed = derived.derived.is_some() || needs_multi_source_explain(derived);
    let mut rows = if let crate::parser::QueryExpr::Rrf(rrf) = sub {
        planner::explain_rows(&super::hybrid::plan_for_explain(db, rrf)?)
    } else if recursed {
        explain_select_node(db, derived)?
    } else {
        refresh_stale_stats_for_select(db, derived)?;
        let planned = planner::plan_select(db, derived)?;
        planner::explain_rows(&planned.physical)
    };
    for row in &mut rows {
        if row.select_type == "SIMPLE" {
            row.select_type = "DERIVED".to_string();
        }
    }
    if let Some(row) = rows.last_mut() {
        if select_projection_has_aggregate(&select.projection) {
            append_explain_extra(&mut row.extra, "Using temporary");
        }
        append_explain_extra(&mut row.extra, "Derived");
    }
    let before = rows.len();
    if !recursed {
        // The recursive path already appended the derived body's own
        // subqueries; only add them here on the plain single-plan path.
        append_select_subquery_explain_rows(db, derived, &mut rows)?;
    }
    append_select_subquery_explain_rows(db, select, &mut rows)?;
    if rows.len() > before {
        for row in rows.iter_mut().take(before) {
            if row.select_type == "DERIVED" {
                row.select_type = "PRIMARY".to_string();
            }
        }
    }
    tag_view_rows(db, &mut rows);
    // Re-stamp the outer derived alias too: when the user wrote
    // `SELECT ... FROM v` and v was inlined as Select.derived, the very
    // first row above does not carry the view name in its `table` field
    // (planner emits the alias from `from[0]`, which `expand_views` set
    // to the alias string). Look at the SELECT's primary FROM directly.
    if let Some(t) = select.from.first() {
        if let Some(view) = db.view_definition(crate::storage::DEFAULT_DATABASE, &t.name) {
            if let Some(row) = rows.first_mut() {
                if row.select_type == "DERIVED" || row.select_type == "PRIMARY" {
                    row.select_type = "VIEW".to_string();
                    append_explain_extra(&mut row.extra, &format!("view: {}", view.name));
                }
            }
        }
    }
    renumber_explain_rows(&mut rows);
    Ok(QueryResponse::ResultSet(ResultSet {
        columns: planner::explain_result_columns(),
        rows: planner::explain_rows_to_values(rows),
    }))
}

/// Does this SELECT join derived tables / CTE references across multiple FROM
/// positions, the way the executor routes to its in-memory CTE-join path
/// (`execute_select_with_cte_joins` / a derived primary plus comma/JOIN
/// sources)? The planner can't fold such a query into a single physical plan,
/// so EXPLAIN describes it with a synthetic materialise-then-scan shape rather
/// than failing with `1146 table 'X' does not exist` for a query that actually
/// executes.
fn needs_multi_source_explain(select: &Select) -> bool {
    if select.joins.is_empty() {
        return false;
    }
    super::cte::select_uses_cte_in_joins(select)
        || super::cte::select_has_join_derived(select)
        || super::cte::has_cte(select.from.first().map(|t| t.name.as_str()).unwrap_or(""))
        || select.derived.is_some()
}

/// Recursive EXPLAIN core for an arbitrary SELECT node. Routes window
/// projections, multi-source derived/CTE joins, single derived tables
/// (including nested ones), and plain single-source selects to the right
/// explainer, returning typed rows the caller wraps into a `ResultSet`. This
/// is what lets EXPLAIN follow a derived table down as many levels as the
/// executor would.
fn explain_select_node(
    db: &Arc<Database>,
    select: &Select,
) -> Result<Vec<planner::ExplainRow>, QueryError> {
    if select_has_window_in_projection(select) {
        let QueryResponse::ResultSet(rs) = explain_select_with_window(db, select)? else {
            unreachable!();
        };
        return Ok(decode_explain_rs(rs));
    }
    if needs_multi_source_explain(select) {
        return explain_multi_source_select(db, select);
    }
    if select.derived.is_some() {
        let QueryResponse::ResultSet(rs) = execute_derived_explain(db, select)? else {
            unreachable!();
        };
        return Ok(decode_explain_rs(rs));
    }
    if select.table.is_none() && select.from.first().is_none() {
        return Ok(Vec::new());
    }
    refresh_stale_stats_for_select(db, select)?;
    // The plan is executed and formatted in two phases; pin its owner so the
    // legacy address-to-OperatorId bridge cannot be invalidated by a move.
    let planned = Box::new(planner::plan_select(db, select)?);
    let mut rows = planner::explain_rows(&planned.physical);
    append_select_subquery_explain_rows(db, select, &mut rows)?;
    Ok(rows)
}

/// EXPLAIN rows for a SELECT whose FROM joins derived tables / CTE references
/// the planner can't fold into one physical plan. Each derived source is
/// recursively explained as a `DERIVED` sub-plan; each top-level reference
/// becomes a coarse scan row (mirroring MySQL's `<derivedN>` materialise-then
/// -scan shape). Embedded WHERE/HAVING subqueries are appended as usual.
fn explain_multi_source_select(
    db: &Arc<Database>,
    select: &Select,
) -> Result<Vec<planner::ExplainRow>, QueryError> {
    let mut top: Vec<planner::ExplainRow> = Vec::new();
    let mut subplans: Vec<planner::ExplainRow> = Vec::new();

    if let Some(table) = select.from.first() {
        explain_one_source(
            db,
            table,
            select.derived.as_ref(),
            None,
            &mut top,
            &mut subplans,
        )?;
    }
    for join in &select.joins {
        explain_one_source(
            db,
            &join.table,
            join.derived.as_ref(),
            Some(&join.join_type),
            &mut top,
            &mut subplans,
        )?;
    }

    // IN / scalar / EXISTS subqueries living in WHERE / HAVING.
    let mut normalized = select.clone();
    planner::normalize_select_predicates(db, &mut normalized);
    append_select_subquery_explain_rows(db, &normalized, &mut top)?;

    top.extend(subplans);
    Ok(top)
}

/// Append the top-level scan row (and, for a derived source, its recursively
/// explained `DERIVED` sub-plan) for one FROM/JOIN source.
fn explain_one_source(
    db: &Arc<Database>,
    table: &TableRef,
    derived: Option<&SubQuery>,
    join_type: Option<&crate::parser::JoinType>,
    top: &mut Vec<planner::ExplainRow>,
    subplans: &mut Vec<planner::ExplainRow>,
) -> Result<(), QueryError> {
    use crate::parser::JoinType;
    let alias = table.alias.clone().unwrap_or_else(|| table.name.clone());
    let access = match join_type {
        None => "ALL",
        Some(JoinType::Left) => "LEFT",
        Some(JoinType::Right) => "RIGHT",
        Some(JoinType::Full) => "FULL",
        Some(_) => "ALL",
    }
    .to_string();
    let mut extra = String::new();
    if let Some(sub) = derived {
        let inner = sub.leftmost_select().ok_or_else(|| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "cannot explain derived set operation",
            )
        })?;
        let mut sub_rows = explain_select_node(db, inner)?;
        for row in &mut sub_rows {
            if row.select_type == "SIMPLE" || row.select_type == "PRIMARY" {
                row.select_type = "DERIVED".to_string();
            }
        }
        subplans.extend(sub_rows);
        extra = format!("Derived: {alias}");
    } else if super::cte::has_cte(&table.name) {
        extra = format!("CTE scan: {}", table.name);
    }
    top.push(planner::ExplainRow {
        id: 0,
        select_type: "PRIMARY".to_string(),
        table: alias,
        partitions: "NULL".to_string(),
        access_type: access,
        possible_keys: String::new(),
        key: String::new(),
        key_len: String::new(),
        ref_: String::new(),
        rows: 0,
        filtered: 100.0,
        extra,
        plan_id: None,
    });
    Ok(())
}

/// Re-tag rows whose source table is actually a view. After
/// `expand_views` rewrites a view reference into a derived subquery, the
/// derived row's `table` field still carries the alias the user wrote
/// (which defaults to the view name). Cross-checking against the
/// catalog's view list lets EXPLAIN distinguish `VIEW` (a derived from
/// a stored view) from a user-written `(SELECT ...)` derived — useful
/// when reading plans for view-heavy queries.
pub(super) fn tag_view_rows(db: &Database, rows: &mut [planner::ExplainRow]) {
    for row in rows.iter_mut() {
        if row.table.is_empty() {
            continue;
        }
        // Try the bare alias and any qualified `db.alias` form.
        if let Some(view) = db.view_definition(crate::storage::DEFAULT_DATABASE, &row.table) {
            if row.select_type == "DERIVED" || row.select_type == "SIMPLE" {
                row.select_type = "VIEW".to_string();
            }
            append_explain_extra(&mut row.extra, &format!("view: {}", view.name));
        }
    }
}

pub(super) fn append_explain_extra(extra: &mut String, value: &str) {
    if extra.is_empty() {
        extra.push_str(value);
    } else if !extra.split(';').map(str::trim).any(|part| part == value) {
        extra.push_str("; ");
        extra.push_str(value);
    }
}

pub(super) fn renumber_explain_rows(rows: &mut [planner::ExplainRow]) {
    for (idx, row) in rows.iter_mut().enumerate() {
        row.id = idx + 1;
    }
}

pub(super) fn append_select_subquery_explain_rows(
    db: &Arc<Database>,
    select: &Select,
    rows: &mut Vec<planner::ExplainRow>,
) -> Result<(), QueryError> {
    for item in &select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            append_expr_subquery_explain_rows(db, expr, rows)?;
        }
    }
    if let Some(selection) = &select.selection {
        append_expr_subquery_explain_rows(db, selection, rows)?;
    }
    for join in &select.joins {
        if let Some(condition) = &join.condition {
            append_expr_subquery_explain_rows(db, condition, rows)?;
        }
    }
    for expr in &select.group_by {
        append_expr_subquery_explain_rows(db, expr, rows)?;
    }
    if let Some(having) = &select.having {
        append_expr_subquery_explain_rows(db, having, rows)?;
    }
    for order_by in &select.order_by {
        append_expr_subquery_explain_rows(db, &order_by.expr, rows)?;
    }
    Ok(())
}

pub(super) fn append_expr_subquery_explain_rows(
    db: &Arc<Database>,
    expr: &Expr,
    rows: &mut Vec<planner::ExplainRow>,
) -> Result<(), QueryError> {
    crate::parser::visit::try_inspect_value_expr(expr, |node| {
        // Process the value operand before its subquery, preserving row IDs
        // and first-error ordering. The subquery planner owns the inner scope.
        let nested = match node {
            Expr::InSubquery { expr, subquery, .. } => {
                append_expr_subquery_explain_rows(db, expr, rows)?;
                Some((subquery.as_ref(), "IN subquery HashSet"))
            }
            Expr::QuantifiedComparison {
                left,
                right: QuantifiedComparisonRight::Subquery(subquery),
                ..
            } => {
                append_expr_subquery_explain_rows(db, left, rows)?;
                Some((subquery.as_ref(), "ANY/ALL/SOME subquery"))
            }
            Expr::Subquery(subquery) => Some((subquery.as_ref(), "Scalar subquery")),
            Expr::Exists { subquery, .. } => Some((subquery.as_ref(), "EXISTS subquery")),
            _ => None,
        };
        if let Some((subquery, tag)) = nested {
            append_subquery_explain_rows(db, subquery, tag, rows)?;
            return Ok(false);
        }
        Ok(crate::parser::visit::scalar_query_operand_children(node))
    })
}

/// Heuristic: does `subquery` mention a qualified column whose qualifier
/// isn't one of the subquery's own FROM / JOIN tables? That qualifier
/// must come from the outer scope, which means the subquery is
/// correlated. The check is intentionally surface-syntactic — false
/// positives only cause EXPLAIN to print a structural placeholder
/// instead of a fully-planned subquery, and false negatives can't
/// happen for the well-formed inputs `plan_select` receives.
fn subquery_references_outer_qualifier(subquery: &Select) -> bool {
    use std::collections::HashSet;
    let mut inner_qualifiers: HashSet<String> = HashSet::new();
    let mut enroll = |name: &str, alias: Option<&str>| {
        inner_qualifiers.insert(name.to_ascii_lowercase());
        if let Some(suffix) = name.rsplit('.').next() {
            if suffix != name {
                inner_qualifiers.insert(suffix.to_ascii_lowercase());
            }
        }
        if let Some(a) = alias {
            inner_qualifiers.insert(a.to_ascii_lowercase());
        }
    };
    for t in &subquery.from {
        enroll(&t.name, t.alias.as_deref());
    }
    for j in &subquery.joins {
        enroll(&j.table.name, j.table.alias.as_deref());
    }
    let mut found = false;
    walk_expr_for_outer_qualifier(&subquery.projection, &inner_qualifiers, &mut found);
    if let Some(s) = &subquery.selection {
        walk_one_expr_for_outer_qualifier(s, &inner_qualifiers, &mut found);
    }
    for j in &subquery.joins {
        if let Some(c) = &j.condition {
            walk_one_expr_for_outer_qualifier(c, &inner_qualifiers, &mut found);
        }
    }
    if let Some(h) = &subquery.having {
        walk_one_expr_for_outer_qualifier(h, &inner_qualifiers, &mut found);
    }
    for ob in &subquery.order_by {
        walk_one_expr_for_outer_qualifier(&ob.expr, &inner_qualifiers, &mut found);
    }
    for e in &subquery.group_by {
        walk_one_expr_for_outer_qualifier(e, &inner_qualifiers, &mut found);
    }
    found
}

fn walk_expr_for_outer_qualifier(
    items: &[SelectItem],
    inner: &std::collections::HashSet<String>,
    found: &mut bool,
) {
    for item in items {
        if let SelectItem::Expr { expr, .. } = item {
            walk_one_expr_for_outer_qualifier(expr, inner, found);
        }
    }
}

fn walk_one_expr_for_outer_qualifier(
    expr: &Expr,
    inner: &std::collections::HashSet<String>,
    found: &mut bool,
) {
    if *found {
        return;
    }
    *found = crate::parser::visit::any_expr_matching(
        expr,
        |node| match node {
            Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                !inner.contains(&parts[parts.len() - 2].to_ascii_lowercase())
            }
            Expr::WindowFunction { args, .. } => args.iter().any(|arg| {
                let mut found = false;
                walk_one_expr_for_outer_qualifier(arg, inner, &mut found);
                found
            }),
            Expr::Subquery(s) | Expr::Exists { subquery: s, .. } => {
                subquery_references_outer_qualifier(s)
            }
            Expr::InSubquery { expr, subquery, .. } => {
                let mut found = false;
                walk_one_expr_for_outer_qualifier(expr, inner, &mut found);
                found || subquery_references_outer_qualifier(subquery)
            }
            Expr::QuantifiedComparison {
                left,
                right: QuantifiedComparisonRight::Subquery(s),
                ..
            } => {
                let mut found = false;
                walk_one_expr_for_outer_qualifier(left, inner, &mut found);
                found || subquery_references_outer_qualifier(s)
            }
            _ => false,
        },
        |node| {
            crate::parser::visit::scalar_query_operand_children(node)
                && !matches!(
                    node,
                    Expr::InSubquery { .. }
                        | Expr::QuantifiedComparison {
                            right: QuantifiedComparisonRight::Subquery(_),
                            ..
                        }
                )
        },
    );
}

/// B1: pick the MySQL `select_type` for a subquery block.
///
/// - `Derived` → `DERIVED` (FROM-clause subquery).
/// - non-deterministic body (`RAND()`, user-variable assignment, etc.) →
///   `UNCACHEABLE SUBQUERY` (its result can't be cached and is recomputed).
/// - correlated body → `DEPENDENT SUBQUERY`.
/// - non-correlated `IN (...)` subquery → `MATERIALIZED` (mydb materialises it
///   into a hash set, matching MySQL's semijoin/materialization label).
/// - otherwise → `SUBQUERY`.
fn subquery_select_type(tag: &str, subquery: &Select, dependent: bool) -> &'static str {
    if tag == "Derived" {
        return "DERIVED";
    }
    if select_is_uncacheable(subquery) {
        return "UNCACHEABLE SUBQUERY";
    }
    if dependent {
        return "DEPENDENT SUBQUERY";
    }
    if tag == "IN subquery HashSet" {
        return "MATERIALIZED";
    }
    "SUBQUERY"
}

/// A subquery is uncacheable when its result may differ between evaluations for
/// the same inputs — i.e. it references a non-deterministic construct such as
/// `RAND()` or reads/writes a user variable. MySQL labels these `UNCACHEABLE
/// SUBQUERY` because it cannot memoise a single result.
pub(super) fn select_is_uncacheable(select: &Select) -> bool {
    if select.for_update || select.lock_clause.is_some() {
        return true;
    }
    let mut found = false;
    for item in &select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            if expr_is_nondeterministic(expr) {
                found = true;
            }
        }
    }
    if let Some(selection) = &select.selection {
        found |= expr_is_nondeterministic(selection);
    }
    if let Some(having) = &select.having {
        found |= expr_is_nondeterministic(having);
    }
    found |= select
        .order_by
        .iter()
        .any(|o| expr_is_nondeterministic(&o.expr));
    found |= select.group_by.iter().any(expr_is_nondeterministic);
    found |= select.joins.iter().any(|j| {
        j.condition.as_ref().is_some_and(expr_is_nondeterministic)
            || j.derived.as_ref().is_some_and(subquery_is_uncacheable)
    });
    found |= select.derived.as_ref().is_some_and(subquery_is_uncacheable);
    found |= select
        .values
        .as_ref()
        .is_some_and(|v| v.rows.iter().flatten().any(expr_is_nondeterministic));
    found |= select
        .json_table
        .as_ref()
        .is_some_and(|j| expr_is_nondeterministic(&j.expr));
    found |= select.named_windows.iter().any(|w| {
        w.partition_by.iter().any(expr_is_nondeterministic)
            || w.order_by.iter().any(|o| expr_is_nondeterministic(&o.expr))
    });
    found
}

fn subquery_is_uncacheable(query: &SubQuery) -> bool {
    fn statement<'q>(stmt: impl Into<crate::parser::QueryRef<'q>>) -> bool {
        let stmt = stmt.into();
        match stmt {
            crate::parser::QueryRef::Select(s) => select_is_uncacheable(s),
            crate::parser::QueryRef::SetOperation(s) => statement(&s.left) || statement(&s.right),
            // Conservatively avoid caching unfamiliar statement wrappers.
            _ => true,
        }
    }
    match query {
        SubQuery::Select(s) => select_is_uncacheable(s),
        s @ (SubQuery::SetOp(_) | SubQuery::Rrf(_) | SubQuery::WithCte(_)) => statement(s),
    }
}

/// Surface-syntactic non-determinism check: `RAND`/`UUID`/`SYSDATE`/`NOW`-style
/// volatile functions or any user-variable reference (`@x`). Intentionally
/// conservative — shared by execution memoization and EXPLAIN.
fn expr_is_nondeterministic(expr: &Expr) -> bool {
    fn walk(expr: &Expr) -> bool {
        match expr {
            Expr::Function { name, args, .. } => {
                let n = name.to_ascii_uppercase();
                let volatile = matches!(
                    n.as_str(),
                    "RAND"
                        | "UUID"
                        | "UUID_SHORT"
                        | "SYSDATE"
                        | "RANDOM"
                        | "RANDOM_BYTES"
                        | "LOAD_FILE"
                        | "__SET_USER_VAR"
                );
                volatile
                    || crate::engine::expr::function_has_external_effect(name)
                    || crate::engine::expr_type::builtin_spec(name).is_some_and(|spec| {
                        spec.volatility == crate::engine::expr_type::Volatility::Volatile
                    })
                    || args.iter().any(walk)
            }
            Expr::UserVariable(_) => true,
            Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => walk(expr),
            Expr::Binary { left, right, .. } => walk(left) || walk(right),
            Expr::Like { expr, pattern, .. } => walk(expr) || walk(pattern),
            Expr::Between {
                expr, low, high, ..
            } => walk(expr) || walk(low) || walk(high),
            Expr::InList { expr, list, .. } => walk(expr) || list.iter().any(walk),
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                operand.as_deref().map(walk).unwrap_or(false)
                    || conditions.iter().any(walk)
                    || results.iter().any(walk)
                    || else_result.as_deref().map(walk).unwrap_or(false)
            }
            Expr::Subquery(s) | Expr::Exists { subquery: s, .. } => select_is_uncacheable(s),
            Expr::InSubquery { expr, subquery, .. } => {
                walk(expr) || select_is_uncacheable(subquery)
            }
            Expr::QuantifiedComparison { left, right, .. } => {
                walk(left)
                    || match right {
                        crate::parser::QuantifiedComparisonRight::Subquery(s) => {
                            select_is_uncacheable(s)
                        }
                        crate::parser::QuantifiedComparisonRight::List(items) => {
                            items.iter().any(walk)
                        }
                    }
            }
            Expr::Tuple(items) => items.iter().any(walk),
            Expr::GroupKeyRef { expr, .. } => walk(expr),
            Expr::WindowFunction { args, spec, .. } => {
                let crate::parser::WindowSpec {
                    partition_by,
                    order_by,
                    ..
                } = spec.as_ref();

                args.iter().chain(partition_by).any(walk) || order_by.iter().any(|o| walk(&o.expr))
            }
            _ => false,
        }
    }
    walk(expr)
}

pub(super) fn append_subquery_explain_rows(
    db: &Arc<Database>,
    subquery: &Select,
    tag: &str,
    rows: &mut Vec<planner::ExplainRow>,
) -> Result<(), QueryError> {
    // Track whether the subquery is correlated with the outer scope so we can
    // label its block `DEPENDENT SUBQUERY` (MySQL) rather than `SUBQUERY`.
    let mut dependent = false;
    let mut subquery_rows = if subquery.derived.is_some() {
        derived_explain_rows(db, subquery)?
    } else if subquery.table.is_none() {
        Vec::new()
    } else if subquery_references_outer_qualifier(subquery) {
        // Correlated subquery via a qualified outer reference (`WHERE x =
        // outer.k`): `plan_select` only knows the subquery's own scope, so it
        // would fail column validation in isolation. Emit a structural
        // placeholder row instead.
        dependent = true;
        correlated_subquery_placeholder(subquery)
    } else {
        refresh_stale_stats_for_select(db, subquery)?;
        match planner::plan_select(db, subquery) {
            Ok(planned) => {
                let mut subquery_rows = planner::explain_rows(&planned.physical);
                append_select_subquery_explain_rows(db, subquery, &mut subquery_rows)?;
                subquery_rows
            }
            // A *bare-column* correlation (`WHERE cs_item_sk = i_item_sk`,
            // where `i_item_sk` belongs to an outer table) isn't caught by the
            // qualifier check above, so planning the subquery alone reports the
            // outer column as unknown (1054). The main query plan already
            // decorrelated it successfully; fall back to the same correlated
            // placeholder rather than failing the whole EXPLAIN.
            Err(e) if e.is(crate::error::catalog::ER_BAD_FIELD_ERROR) => {
                dependent = true;
                correlated_subquery_placeholder(subquery)
            }
            Err(e) => return Err(e),
        }
    };
    // B1 (explain-mysql80-compat): derive the MySQL `select_type` for this
    // block from its kind + correlation. A correlated body is `DEPENDENT
    // SUBQUERY`; a non-correlated IN-subquery is materialised into a hash set
    // (`MATERIALIZED`); a body that is non-deterministic / references the outer
    // scope non-cacheably is `UNCACHEABLE SUBQUERY`; everything else is a plain
    // `SUBQUERY`. `Derived` keeps `DERIVED`.
    let select_type = subquery_select_type(tag, subquery, dependent);
    for row in &mut subquery_rows {
        if row.select_type == "SIMPLE" {
            row.select_type = select_type.to_string();
        }
    }
    if let Some(row) = subquery_rows.first_mut() {
        append_explain_extra(&mut row.extra, tag);
    }
    rows.extend(subquery_rows);
    Ok(())
}

/// Structural placeholder row for a correlated subquery whose body can only be
/// resolved against the outer scope. EXPLAIN still names the table the subquery
/// scans, just without a standalone access path.
fn correlated_subquery_placeholder(subquery: &Select) -> Vec<planner::ExplainRow> {
    let table_name = subquery.table.clone().unwrap_or_default();
    let table_label = subquery
        .from
        .first()
        .and_then(|t| t.alias.clone())
        .unwrap_or_else(|| table_name.clone());
    vec![planner::ExplainRow {
        id: 1,
        select_type: "SIMPLE".to_string(),
        table: table_label,
        partitions: "NULL".to_string(),
        access_type: "ALL".to_string(),
        possible_keys: String::new(),
        key: String::new(),
        key_len: String::new(),
        ref_: String::new(),
        rows: 0,
        filtered: 100.0,
        extra: "Correlated; evaluated per outer row".to_string(),
        plan_id: None,
    }]
}

pub(super) fn derived_explain_rows(
    db: &Arc<Database>,
    select: &Select,
) -> Result<Vec<planner::ExplainRow>, QueryError> {
    let Some(sub) = &select.derived else {
        return Ok(Vec::new());
    };
    let derived = sub.leftmost_select().ok_or_else(|| {
        error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "cannot explain derived set operation",
        )
    })?;
    refresh_stale_stats_for_select(db, derived)?;
    let planned = planner::plan_select(db, derived)?;
    let mut rows = planner::explain_rows(&planned.physical);
    if let Some(row) = rows.last_mut() {
        if select_projection_has_aggregate(&select.projection) {
            append_explain_extra(&mut row.extra, "Using temporary");
        }
        append_explain_extra(&mut row.extra, "Derived");
    }
    append_select_subquery_explain_rows(db, derived, &mut rows)?;
    append_select_subquery_explain_rows(db, select, &mut rows)?;
    Ok(rows)
}

fn execute_explain_formatted(
    db: &Arc<Database>,
    session: &mut SessionContext,
    registry: &SessionRegistry,
    select: &Select,
    format: ExplainFormat,
    analyze: bool,
) -> Result<QueryResponse, QueryError> {
    // Inline derived tables (anywhere in FROM/JOIN, possibly nested) can't be
    // handed straight to `plan_select` — it would resolve each alias against
    // the catalog and fail with 1146 ("table '<alias>' does not exist").
    // Register every one as a stub CTE (same mechanism as
    // `explain_with_cte_formatted`) so the body planner lowers them to
    // `CteScan` leaves. Skipped only for LATERAL shapes that require Apply.
    // ANALYZE is unsupported here (nothing is materialised
    // during EXPLAIN), matching the traditional multi-source path.
    let has_inline_derived =
        select.derived.is_some() || select.joins.iter().any(|j| j.derived.is_some());
    if has_inline_derived
        && !select
            .joins
            .iter()
            .any(|j| j.derived.is_some() && j.lateral)
    {
        if analyze {
            return explain_analyze_join_derived_formatted(db, session, select, format, registry);
        }
        return explain_join_derived_formatted(db, select, format);
    }
    let prepared_derived;
    let select = if let Some(sub) = &select.derived {
        // LATERAL / FULL-join case fell through: plan its leftmost SELECT as
        // a representative subplan.
        let mut prepared = sub
            .leftmost_select()
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "cannot explain derived set operation",
                )
            })?
            .clone();
        prepare_select_for_planning(db, &mut prepared)?;
        prepared_derived = prepared;
        &prepared_derived
    } else {
        select
    };
    if is_source_free_select(select) {
        let text = match format {
            ExplainFormat::Tree => {
                "-> Rows fetched before execution (cost=0.00 rows=1)".to_string()
            }
            ExplainFormat::Json => {
                r#"{"query_block":{"select_id":1,"message":"No tables used"}}"#.to_string()
            }
            ExplainFormat::Traditional => unreachable!("traditional routed away"),
        };
        return Ok(QueryResponse::ResultSet(ResultSet {
            columns: vec![Column {
                source: Default::default(),
                name: "EXPLAIN".to_string(),
                column_type: ColumnType::VarString,
            }],
            rows: vec![vec![Value::String(text)]],
        }));
    }
    if select_has_window_in_projection(select) {
        if analyze {
            return explain_analyze_window_formatted(db, session, select, format, registry);
        }
        refresh_stale_stats_for_select(db, select)?;
        let planned = super::super::dispatch::plan_windowed_select_for_explain(db, select.clone())?;
        let cte_plans = (!planned.cte_plans.is_empty()).then_some(&planned.cte_plans);
        let text = match format {
            ExplainFormat::Tree => prepend_hint_warnings_tree(
                format_plan_tree(&planned.plan, 0, None, cte_plans, None),
                &planned.hint_warnings,
            ),
            ExplainFormat::Json => inject_hint_warnings_json(
                format_plan_json_with_ctes(&planned.plan, None, cte_plans, None),
                &planned.hint_warnings,
            ),
            ExplainFormat::Traditional => unreachable!("traditional routed away"),
        };
        return Ok(QueryResponse::ResultSet(ResultSet {
            columns: vec![Column {
                source: Default::default(),
                name: "EXPLAIN".to_string(),
                column_type: ColumnType::VarString,
            }],
            rows: vec![vec![Value::String(text)]],
        }));
    }
    // Keep the static plan contract identical to ordinary EXPLAIN.  This is
    // outside the measured execution interval below.
    refresh_stale_stats_for_select(db, select)?;
    let planned = Box::new(planner::plan_select(db, select)?);
    let analyze_compiled = analyze.then(|| {
        crate::engine::cache::CompiledSelectPlan::new(
            Arc::new(select.clone()),
            Arc::new(planned.physical.clone()),
            smallvec::SmallVec::new(),
            smallvec::SmallVec::new(),
            0,
        )
    });

    // For ANALYZE: run the query under a thread-local stats collector,
    // executing the *same* PhysicalPlan instance the formatter will
    // walk so per-operator stats can be looked up by node pointer.
    let analyze_data = if analyze {
        let (mut stats, exec_result) = analyze_stats::with_collector(|| {
            let exec_start = std::time::Instant::now();
            let rows = run_select_for_analyze_with_plan(
                db,
                session,
                analyze_compiled
                    .as_ref()
                    .expect("ANALYZE compiled plan missing"),
                select,
                registry,
            )?;
            let elapsed_ms = exec_start.elapsed().as_nanos() as f64 / 1_000_000.0;
            Ok::<_, QueryError>((rows, elapsed_ms))
        });
        let (rows_count, elapsed_ms) = exec_result?;
        let executed_plan = stats
            .take_executed_plan()
            .unwrap_or_else(|| Box::new(planned.physical.clone()));
        complete_fused_trace(
            &executed_plan,
            &mut stats,
            Some(NodeStats {
                first_row_ns: (elapsed_ms * 1_000_000.0).round() as u128,
                time_ns: (elapsed_ms * 1_000_000.0).round() as u128,
                rows: rows_count,
                loops: 1,
                ..NodeStats::default()
            }),
        );
        let io = stats.query_io();
        Some((stats, rows_count, elapsed_ms, io, executed_plan))
    } else {
        None
    };

    let text = match format {
        ExplainFormat::Tree => {
            let stats_ref = analyze_data.as_ref().map(|(s, _, _, _, _)| s);
            let display_plan = analyze_data
                .as_ref()
                .map(|(_, _, _, _, plan)| plan.as_ref())
                .unwrap_or(&planned.physical);
            let tree = format_plan_tree(display_plan, 0, stats_ref, None, None);
            prepend_hint_warnings_tree(tree, &planned.hint_warnings)
        }
        ExplainFormat::Json => {
            let stats_ref = analyze_data
                .as_ref()
                .map(|(s, rows, ms, io, _)| (s, *rows, *ms, *io));
            let display_plan = analyze_data
                .as_ref()
                .map(|(_, _, _, _, plan)| plan.as_ref())
                .unwrap_or(&planned.physical);
            let json = format_plan_json_with_ctes(display_plan, stats_ref, None, None);
            inject_hint_warnings_json(json, &planned.hint_warnings)
        }
        ExplainFormat::Traditional => unreachable!(),
    };
    Ok(QueryResponse::ResultSet(ResultSet {
        columns: vec![Column {
            source: Default::default(),
            name: "EXPLAIN".to_string(),
            column_type: ColumnType::VarString,
        }],
        rows: vec![vec![Value::String(text)]],
    }))
}

/// Best-effort output columns for a derived-table stub used during formatted
/// EXPLAIN. Resolves the inner body's column names/types (with `prior` stubs
/// for already-registered siblings in scope) and falls back to the bare
/// projection names — enough for the body planner to bind `alias.col`.
fn derived_stub_columns_for_explain(
    db: &Arc<Database>,
    inner: &Select,
    prior: &std::collections::HashMap<String, std::sync::Arc<crate::engine::api::CteResult>>,
) -> Vec<Column> {
    let computed = super::cte::with_cte_registry_extended(prior.clone(), || {
        super::describe_select_columns(db, inner).ok()
    });
    if let Some(columns) = computed {
        return columns;
    }
    projection_output_names(inner)
        .unwrap_or_default()
        .into_iter()
        .map(|name| Column {
            source: Default::default(),
            name,
            column_type: ColumnType::VarString,
        })
        .collect()
}

/// Keep consumer bounds with the display-plan input. Planning detaches derived
/// sources, so thread-local hints alone cannot survive until materialization.
struct InlineDerivedSelect {
    select: Select,
    fusion: Option<std::sync::Arc<crate::parser::RrfFusion>>,
    window_topk: Option<(String, usize)>,
    rollup_topk: Option<super::plan::RollupWindowTopKHint>,
}

fn materialize_analyzed_inline(
    db: &Arc<Database>,
    txn: crate::storage::TxnView,
    alias: &str,
    input: &InlineDerivedSelect,
    plan: &planner::PhysicalPlan,
    registry: &SessionRegistry,
) -> Result<crate::engine::api::CteResult, QueryError> {
    let _window = super::super::dispatch::set_window_topk_hint(input.window_topk.clone());
    let _rollup = super::plan::set_rollup_window_topk_hint(input.rollup_topk.clone());
    let run = || {
        super::cte::materialize_inline_derived_for_analyze(
            db,
            txn,
            alias,
            &input.select,
            plan,
            registry,
        )
    };
    if let Some(fusion) = &input.fusion {
        super::hybrid::with_analyzed_sources(db, txn, fusion, plan, registry, run)
    } else {
        run()
    }
}

/// Recursive worker for formatted EXPLAIN of a SELECT that may carry inline
/// derived tables anywhere in its tree. Strips every FROM[0]/JOIN-position
/// derived (descending into nested derived sources too), stubs each one as a
/// CTE entry in the active registry under its alias (or "derived" when the
/// parser had no alias to give), recursively plans the inners into
/// `cte_plans`, then plans the now-clean outer via `plan_select`. The active
/// registry must already be installed by the caller via
/// `with_cte_registry[_extended]` so the stub mutations stay scoped.
fn plan_select_with_inline_derived_for_explain(
    db: &Arc<Database>,
    select: &mut Select,
    cte_plans: &mut std::collections::HashMap<String, planner::PhysicalPlan>,
    warnings: &mut Vec<String>,
    inline_derived: &mut Vec<(String, InlineDerivedSelect)>,
    is_lossy: &mut bool,
) -> Result<planner::PhysicalPlan, QueryError> {
    let _rrf_window = super::hybrid::window_for_select(db, select)?;
    use crate::engine::api::CteResult;
    use std::sync::Arc;

    crate::planner_decorrelate::derived::pushdown_derived_predicates_with_db(db, select);

    let stub_inner = |alias: String,
                      sub: &crate::parser::SubQuery|
     -> Result<
        (
            String,
            Select,
            Option<planner::PhysicalPlan>,
            Option<std::sync::Arc<crate::parser::RrfFusion>>,
        ),
        QueryError,
    > {
        let inner = sub
            .leftmost_select()
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "cannot explain derived set operation",
                )
            })?
            .clone();
        let snapshot = super::cte::current_cte_registry();
        let columns = if matches!(sub, crate::parser::QueryExpr::Rrf(_)) {
            super::describe_subquery_columns(db, sub)?
        } else {
            derived_stub_columns_for_explain(db, &inner, &snapshot)
        };
        let key = super::cte::next_inline_derived_name();
        super::cte::set_cte_result(&key, Arc::new(CteResult::stub_for_explain(columns, 0.0)));
        let complete_plan = match sub {
            crate::parser::SubQuery::Rrf(rrf) => Some(planner::materialize_plan(
                super::hybrid::plan_for_explain(db, rrf)?,
                format!("derived:{alias}"),
            )),
            query @ (crate::parser::SubQuery::SetOp(_) | crate::parser::SubQuery::WithCte(_)) => {
                planner::plan_query_physical(db, query)?
                    .map(|plan| planner::materialize_plan(plan, format!("derived:{alias}")))
            }
            crate::parser::SubQuery::Select(_) => None,
        };
        let fusion = if let crate::parser::QueryExpr::Rrf(fusion) = sub {
            Some(std::sync::Arc::new((**fusion).clone()))
        } else {
            None
        };
        Ok((key, inner, complete_plan, fusion))
    };

    let derived_window_topk = select.derived.as_ref().and_then(|source| {
        let crate::parser::SubQuery::Select(inner) = source else {
            return None;
        };
        (select.joins.is_empty()
            && select.from.len() == 1
            && inner.limit.is_none()
            && inner.offset.is_none()
            && super::super::dispatch::select_has_window_functions(inner))
        .then(|| {
            select
                .selection
                .as_ref()
                .and_then(super::extract_window_topk_filter)
        })
        .flatten()
    });
    let derived_rollup_topk = select.derived.as_ref().and_then(|source| {
        super::super::dispatch::rollup_window_topk_hint_for_window_derived(select, source)
    });
    let mut local_inners = Vec::new();
    if let Some(sub) = select.derived.take() {
        let alias = select
            .from
            .first()
            .map(|t| t.alias.clone().unwrap_or_else(|| t.name.clone()))
            .unwrap_or_else(|| "derived".to_string());
        // A set-op inline derived (typically UNION ALL) only contributes its
        // leftmost branch to the display plan — the synthetic CteScan stub
        // therefore covers a strict subset of the real rows. Mark the
        // enclosing SELECT as "lossy" so EXPLAIN ANALYZE materialises this
        // owner via the legacy dispatch path instead of the partial plan.
        if sub.as_select().is_none() && !matches!(sub, crate::parser::QueryExpr::Rrf(_)) {
            *is_lossy = true;
        }
        let (key, inner, complete_plan, fusion) = stub_inner(alias.clone(), &sub)?;
        if let Some(t) = select.from.get_mut(0) {
            t.name = key.clone();
            t.alias = Some(alias);
        }
        local_inners.push((
            key,
            inner,
            complete_plan,
            fusion,
            derived_window_topk,
            derived_rollup_topk,
        ));
    }
    for join in select.joins.iter_mut() {
        let Some(sub) = join.derived.take() else {
            continue;
        };
        let alias = join
            .table
            .alias
            .clone()
            .unwrap_or_else(|| join.table.name.clone());
        if sub.as_select().is_none() && !matches!(sub, crate::parser::QueryExpr::Rrf(_)) {
            *is_lossy = true;
        }
        let (key, inner, complete_plan, fusion) = stub_inner(alias.clone(), &sub)?;
        join.table.name = key.clone();
        join.table.alias = Some(alias);
        local_inners.push((key, inner, complete_plan, fusion, None, None));
    }

    for (key, inner, complete_plan, fusion, window_topk, rollup_topk) in local_inners {
        let _window_hint = super::super::dispatch::set_window_topk_hint(window_topk.clone());
        if let Some(inner_plan) = complete_plan {
            let est = planner::estimated_rows(&inner_plan);
            if let Some(existing) = super::cte::lookup_cte(&key) {
                super::cte::set_cte_result(
                    &key,
                    Arc::new(CteResult::stub_for_explain(existing.columns.clone(), est)),
                );
            }
            if fusion.is_some() {
                inline_derived.push((
                    key.clone(),
                    InlineDerivedSelect {
                        select: Select {
                            projection: vec![SelectItem::Wildcard],
                            ..Select::default()
                        },
                        fusion,
                        window_topk,
                        rollup_topk,
                    },
                ));
            }
            cte_plans.insert(key, inner_plan);
            continue;
        }
        let mut prepared = inner;
        if prepare_select_for_planning(db, &mut prepared).is_err() {
            continue;
        }
        if prepared.table.is_none() {
            continue;
        }
        let inner_plan = match plan_select_with_inline_derived_for_explain(
            db,
            &mut prepared,
            cte_plans,
            warnings,
            inline_derived,
            is_lossy,
        ) {
            Ok(plan) => plan,
            Err(_) => continue,
        };
        let est = planner::estimated_rows(&inner_plan);
        if let Some(existing) = super::cte::lookup_cte(&key) {
            super::cte::set_cte_result(
                &key,
                Arc::new(CteResult::stub_for_explain(existing.columns.clone(), est)),
            );
        }
        cte_plans.insert(key.clone(), inner_plan);
        // Capture the prepared inner SELECT alongside its plan so EXPLAIN
        // ANALYZE Phase 2 can materialise this synthetic CTE before its
        // consumer runs. Order is innermost-first (we register after the
        // recursive call returns), matching execution dependency order.
        inline_derived.push((
            key,
            InlineDerivedSelect {
                select: prepared,
                fusion: None,
                window_topk,
                rollup_topk,
            },
        ));
    }

    refresh_stale_stats_for_select(db, select).ok();
    let planned = planner::plan_select(db, select)?;
    warnings.extend(planned.hint_warnings.clone());
    Ok(planned.physical)
}

/// EXPLAIN ANALYZE FORMAT=TREE/JSON for a SELECT whose FROM/JOIN list carries
/// inline derived tables. The ANALYZE counterpart of
/// [`explain_join_derived_formatted`], built on the same Phase-1 plan forest
/// (`plan_select_with_inline_derived_for_explain`) and mirroring
/// [`explain_analyze_with_cte_formatted`]'s execute-the-display-tree approach:
///
/// 1. Plan the body + each inline-derived sub-plan once (the same instances the
///    formatter will walk, so per-node stats key by pointer).
/// 2. Under one stats collector + read-only snapshot + query memory budget,
///    materialise each inline-derived synthetic CTE into the registry (innermost
///    first) so the body's `CteScan` leaves resolve, then run the body plan.
/// 3. Format the same trees with the collected stats.
///
/// Set-op inline derived (`is_lossy`) and windowed bodies forfeit per-node body
/// stats (re-planned via the legacy dispatch / window path) but still produce a
/// correct row count and total — matching the WITH-CTE path's degradation.
fn explain_analyze_join_derived_formatted(
    db: &Arc<Database>,
    session: &mut SessionContext,
    select: &Select,
    format: ExplainFormat,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    use std::collections::HashMap;

    // Phase 1: build the display plan forest once.
    let mut rewritten = select.clone();
    let mut cte_plans: HashMap<String, planner::PhysicalPlan> = HashMap::new();
    let mut hint_warnings: Vec<String> = Vec::new();
    let mut inline_derived: Vec<(String, InlineDerivedSelect)> = Vec::new();
    let mut is_lossy = false;
    let body_plan = super::cte::with_cte_registry_extended(HashMap::new(), || {
        plan_select_with_inline_derived_for_explain(
            db,
            &mut rewritten,
            &mut cte_plans,
            &mut hint_warnings,
            &mut inline_derived,
            &mut is_lossy,
        )
    })?;

    // Phase 2: execute the display trees under one collector + snapshot,
    // mirroring `dispatch`'s per-query memory budget.
    type Phase2 = (analyze_stats::NodeStatsMap, u64, f64);
    let body_runnable = !is_lossy;
    let mut exec_fn = || -> Result<Phase2, QueryError> {
        let (stats, inner) = analyze_stats::with_collector(|| -> Result<_, QueryError> {
            let (txn, _snapshot_guard) = read_view_for_analyze(db, session)?;

            let query_start = std::time::Instant::now();
            let mut visible: HashMap<String, std::sync::Arc<crate::engine::api::CteResult>> =
                HashMap::new();
            // Materialise inline-derived synthetic CTEs innermost-first (the
            // order `plan_select_with_inline_derived_for_explain` pushed them)
            // so each consumer sees its dependencies in the registry.
            for (key, inl_select) in &inline_derived {
                let Some(inl_plan) = cte_plans.get(key) else {
                    continue;
                };
                let inl_result = super::cte::with_cte_registry(visible.clone(), || {
                    materialize_analyzed_inline(db, txn, key, inl_select, inl_plan, registry)
                })?;
                visible.insert(key.clone(), std::sync::Arc::new(inl_result));
            }

            let body_stmt = Statement::Select(rewritten.clone());
            let body_rows = run_with_body_for_analyze(
                db,
                session,
                &body_stmt,
                &body_plan,
                body_runnable,
                txn,
                &visible,
                registry,
            )?;
            let body_ms = query_start.elapsed().as_nanos() as f64 / 1_000_000.0;
            Ok((body_rows, body_ms))
        });
        let (body_rows, body_ms) = inner?;
        Ok((stats, body_rows, body_ms))
    };

    let (mut stats, body_rows_count, body_ms) =
        if crate::engine::executor::mem_budget::current_budget().is_some() {
            exec_fn()?
        } else {
            let budget =
                std::sync::Arc::new(crate::engine::executor::mem_budget::MemBudget::from_env());
            crate::engine::executor::mem_budget::with_query_budget(budget, exec_fn)?
        };
    complete_fused_trace(&body_plan, &mut stats, None);
    if let Some(mut root) = stats.get_for_plan(&body_plan).copied() {
        root.first_row_ns = (body_ms * 1_000_000.0).round() as u128;
        root.time_ns = root.first_row_ns;
        stats.insert_for_plan(&body_plan, root);
    }
    let query_io = stats.query_io();

    // Phase 3: format the same trees with the collected stats.
    let text = match format {
        ExplainFormat::Tree => {
            let tree = format_plan_tree(&body_plan, 0, Some(&stats), Some(&cte_plans), None);
            prepend_hint_warnings_tree(tree, &hint_warnings)
        }
        ExplainFormat::Json => {
            let json = format_plan_json_with_ctes(
                &body_plan,
                Some((&stats, body_rows_count, body_ms, query_io)),
                Some(&cte_plans),
                None,
            );
            inject_hint_warnings_json(json, &hint_warnings)
        }
        ExplainFormat::Traditional => unreachable!("traditional routed away"),
    };
    Ok(QueryResponse::ResultSet(ResultSet {
        columns: vec![Column {
            source: Default::default(),
            name: "EXPLAIN".to_string(),
            column_type: ColumnType::VarString,
        }],
        rows: vec![vec![Value::String(text)]],
    }))
}

/// EXPLAIN FORMAT=TREE/JSON for a SELECT whose FROM/JOIN list carries inline
/// derived tables. Mirrors [`explain_with_cte_formatted`]: stub each derived
/// alias in the thread-local CTE registry (so the body planner resolves it to
/// a `CteScan` leaf and applies CBO join ordering), plan each derived body for
/// its sub-tree, then format the body plan with the derived sub-plans attached
/// under their scans. This is the formatted-output counterpart of the runtime
/// `bridge_join_derived_to_ctes` bridge.
fn explain_join_derived_formatted(
    db: &Arc<Database>,
    select: &Select,
    format: ExplainFormat,
) -> Result<QueryResponse, QueryError> {
    use std::collections::HashMap;

    let mut rewritten = select.clone();
    let mut cte_plans: HashMap<String, planner::PhysicalPlan> = HashMap::new();
    let mut hint_warnings: Vec<String> = Vec::new();
    let mut inline_derived: Vec<(String, InlineDerivedSelect)> = Vec::new();
    let mut is_lossy = false;
    // Use `with_cte_registry_extended` so any enclosing WITH-CTE stubs stay
    // visible while the inline-derived stubs we add only live for this call.
    let body_plan = super::cte::with_cte_registry_extended(HashMap::new(), || {
        plan_select_with_inline_derived_for_explain(
            db,
            &mut rewritten,
            &mut cte_plans,
            &mut hint_warnings,
            &mut inline_derived,
            &mut is_lossy,
        )
    })?;
    let _ = (inline_derived, is_lossy);

    let text = match format {
        ExplainFormat::Tree => {
            let tree = format_plan_tree(&body_plan, 0, None, Some(&cte_plans), None);
            prepend_hint_warnings_tree(tree, &hint_warnings)
        }
        ExplainFormat::Json => {
            let json = format_plan_json_with_ctes(&body_plan, None, Some(&cte_plans), None);
            inject_hint_warnings_json(json, &hint_warnings)
        }
        ExplainFormat::Traditional => unreachable!(),
    };
    Ok(QueryResponse::ResultSet(ResultSet {
        columns: vec![Column {
            source: Default::default(),
            name: "EXPLAIN".to_string(),
            column_type: ColumnType::VarString,
        }],
        rows: vec![vec![Value::String(text)]],
    }))
}

#[derive(Clone, Copy)]
struct PipelineDisplayInfo {
    pipeline: u32,
    stage: u32,
    blocking: bool,
    exchange: bool,
}

thread_local! {
    static PIPELINE_DISPLAY: std::cell::RefCell<Option<std::collections::HashMap<usize, PipelineDisplayInfo>>> =
        const { std::cell::RefCell::new(None) };
}

struct PipelineDisplayScope(Option<std::collections::HashMap<usize, PipelineDisplayInfo>>);

impl PipelineDisplayScope {
    fn enter(root: &planner::PhysicalPlan) -> Self {
        fn is_exchange(plan: &planner::PhysicalPlan) -> bool {
            matches!(plan, planner::PhysicalPlan::Exchange { .. })
        }
        fn is_blocking(plan: &planner::PhysicalPlan) -> bool {
            matches!(
                plan,
                planner::PhysicalPlan::Sort { .. }
                    | planner::PhysicalPlan::IncrementalSort { .. }
                    | planner::PhysicalPlan::ParallelSort { .. }
                    | planner::PhysicalPlan::TopK { .. }
                    | planner::PhysicalPlan::ParallelTopK { .. }
                    | planner::PhysicalPlan::TopWindow { .. }
                    | planner::PhysicalPlan::HashAggregate { .. }
                    | planner::PhysicalPlan::HashRollupAggregate { .. }
                    | planner::PhysicalPlan::HashGroupingSetsAggregate { .. }
                    | planner::PhysicalPlan::ParallelHashAggregate { .. }
                    | planner::PhysicalPlan::ParallelRollupAggregate { .. }
                    | planner::PhysicalPlan::ParallelGroupingSetsAggregate { .. }
                    | planner::PhysicalPlan::WindowAgg { .. }
                    | planner::PhysicalPlan::StreamingDistinct { .. }
                    | planner::PhysicalPlan::RrfFusion { .. }
                    | planner::PhysicalPlan::SetOp { all: false, .. }
                    | planner::PhysicalPlan::SetOp {
                        op: SetOp::Intersect | SetOp::Except,
                        ..
                    }
                    | planner::PhysicalPlan::Exchange { .. }
            )
        }
        fn assign(
            plan: &planner::PhysicalPlan,
            pipeline: u32,
            stage: u32,
            next_pipeline: &mut u32,
            map: &mut std::collections::HashMap<usize, PipelineDisplayInfo>,
        ) {
            let blocking = is_blocking(plan);
            let exchange = is_exchange(plan);
            map.insert(
                std::ptr::from_ref(plan).addr(),
                PipelineDisplayInfo {
                    pipeline,
                    stage,
                    blocking,
                    exchange,
                },
            );
            let children = planner::plan_children(plan);
            let split_binary = children.len() > 1
                && !matches!(
                    plan,
                    planner::PhysicalPlan::SetOp {
                        op: SetOp::Union,
                        all: true,
                        ..
                    }
                );
            for child in children {
                let (child_pipeline, child_stage) = if blocking || split_binary {
                    let id = *next_pipeline;
                    *next_pipeline = next_pipeline.saturating_add(1);
                    (id, stage.saturating_add(1))
                } else {
                    (pipeline, stage)
                };
                assign(child, child_pipeline, child_stage, next_pipeline, map);
            }
        }
        let mut map = std::collections::HashMap::new();
        let mut next_pipeline = 1;
        assign(root, 0, 0, &mut next_pipeline, &mut map);
        let previous = PIPELINE_DISPLAY.with(|slot| slot.replace(Some(map)));
        Self(previous)
    }
}

impl Drop for PipelineDisplayScope {
    fn drop(&mut self) {
        PIPELINE_DISPLAY.with(|slot| {
            slot.replace(self.0.take());
        });
    }
}

fn pipeline_display_info(plan: &planner::PhysicalPlan) -> Option<PipelineDisplayInfo> {
    PIPELINE_DISPLAY.with(|slot| {
        slot.borrow()
            .as_ref()
            .and_then(|map| map.get(&std::ptr::from_ref(plan).addr()).copied())
    })
}

fn pipeline_tree_suffix(plan: &planner::PhysicalPlan, depth: usize) -> String {
    let info = pipeline_display_info(plan).unwrap_or(PipelineDisplayInfo {
        pipeline: depth as u32,
        stage: depth as u32,
        blocking: false,
        exchange: false,
    });
    let cost = planner::plan_cost(plan);
    let kernel = if matches!(
        crate::engine::executor::vec::config::effective_mode(),
        crate::engine::executor::vec::config::VecMode::Off
    ) {
        "scalar"
    } else {
        "vector"
    };
    // TREE is primarily a human-readable plan. Keep exceptional/resource
    // attributes, but do not repeat their defaults on every node.
    let mut attributes = Vec::with_capacity(7);
    if info.pipeline != 0 {
        attributes.push(format!("pipeline={}", info.pipeline));
    }
    if info.stage != 0 {
        attributes.push(format!("stage={}", info.stage));
    }
    if info.blocking {
        attributes.push("blocking=true".to_string());
    }
    if info.exchange {
        attributes.push("exchange=true".to_string());
    }
    if kernel != "vector" {
        attributes.push(format!("kernel={kernel}"));
    }
    if cost.mem_bytes > 0.0 {
        attributes.push(format!("est_mem={:.0}", cost.mem_bytes));
    }
    if cost.spill_bytes > 0.0 {
        attributes.push(format!("est_spill={:.0}", cost.spill_bytes));
    }
    if attributes.is_empty() {
        String::new()
    } else {
        format!(" [{}]", attributes.join(" "))
    }
}

fn format_plan_tree(
    plan: &planner::PhysicalPlan,
    depth: usize,
    stats: Option<&NodeStatsMap>,
    cte_plans: Option<&std::collections::HashMap<String, planner::PhysicalPlan>>,
    cte_extras: Option<&CteExtraBranches>,
) -> String {
    let tree = format_plan_tree_nodes(plan, depth, stats, cte_plans, cte_extras);
    if depth != 0 {
        return tree;
    }
    let Some(stats) = stats else {
        return tree;
    };
    let total = stats.query_io();
    let unassigned = stats.unassigned_io();
    let reads = total.cache_reads.saturating_add(total.physical_reads);
    let hit_rate = if reads == 0 {
        0.0
    } else {
        total.cache_reads as f64 * 100.0 / reads as f64
    };
    let mut out = format!(
        "Query I/O: physical_reads={} cache_reads={} cache_hit_rate={hit_rate:.2}%",
        total.physical_reads, total.cache_reads,
    );
    if unassigned.cache_reads > 0 || unassigned.physical_reads > 0 {
        out.push_str(&format!(
            " unassigned_physical_reads={} unassigned_cache_reads={}",
            unassigned.physical_reads, unassigned.cache_reads,
        ));
    }
    out.push('\n');
    out.push_str(&tree);
    out
}

fn push_scan_filter_and_hint(
    details: &mut Vec<(&'static str, String)>,
    filter: Option<&Expr>,
    hint: Option<&str>,
) {
    if let Some(filter) = filter {
        details.push(("filter", explain_expr(filter)));
    }
    if let Some(hint) = hint {
        details.push(("hint", hint.to_owned()));
    }
}

fn format_plan_tree_nodes(
    plan: &planner::PhysicalPlan,
    depth: usize,
    stats: Option<&NodeStatsMap>,
    cte_plans: Option<&std::collections::HashMap<String, planner::PhysicalPlan>>,
    cte_extras: Option<&CteExtraBranches>,
) -> String {
    let estimated_rows = &planner::estimated_rows(plan);
    let cost = &planner::plan_cost(plan).total;
    let _pipeline_scope = (depth == 0).then(|| PipelineDisplayScope::enter(plan));
    let indent = "    ".repeat(depth);
    let arrow = if depth > 0 { "-> " } else { "-> " };
    let suffix = |node: &planner::PhysicalPlan| -> String {
        let actual = stats
            .map(|s| {
                analyze_stats::format_tree_suffix(display_stats(s, node, cte_plans, cte_extras))
            })
            .unwrap_or_default();
        format!("{}{}", pipeline_tree_suffix(node, depth), actual)
    };
    let append_child =
        |out: &mut String,
         child: &planner::PhysicalPlan,
         child_ctes: Option<&std::collections::HashMap<String, planner::PhysicalPlan>>| {
            out.push('\n');
            out.push_str(&format_plan_tree_nodes(
                child,
                depth + 1,
                stats,
                child_ctes,
                cte_extras,
            ));
        };
    if let Some(node) = UnaryExplain::from_plan(plan) {
        if matches!(
            plan,
            planner::PhysicalPlan::Projection {
                distinct: false,
                ..
            }
        ) {
            // Transparent projection re-roots both indentation and actual statistics.
            return format_plan_tree_nodes(node.input, depth, stats, cte_plans, cte_extras);
        }
        let mut out = node.tree_header(&indent, arrow);
        out.push_str(&suffix(plan));
        append_child(&mut out, node.input, cte_plans);
        return out;
    }
    match plan {
        planner::PhysicalPlan::RrfFusion {
            inputs,
            spec,
            columns,
            output_window,
            ..
        } => {
            let weights = spec
                .weights
                .iter()
                .map(crate::parser::expr_to_sql)
                .collect::<Vec<_>>()
                .join(",");
            let mut out = format!("{indent}{arrow}RRF fusion (rank_constant={}, weights=[{weights}], cost={cost:.2} rows={estimated_rows:.0})", crate::parser::expr_to_sql(&spec.rank_constant));
            out.push_str(&format!(
                " output_window={}",
                super::hybrid::window_json(output_window)
            ));
            out.push_str(&format!(
                " metadata={}",
                super::hybrid::plan_metadata(spec, columns)
            ));
            out.push_str(&suffix(plan));
            for input in inputs {
                append_child(&mut out, input, cte_plans);
            }
            out
        }
        planner::PhysicalPlan::SetOp {
            left,
            right,
            op,
            all,
            ..
        } => {
            let qualifier = if *all { " all" } else { " distinct" };
            let mut out = format!(
                "{indent}{arrow}{op:?}{qualifier} (cost={cost:.2} rows={estimated_rows:.0})"
            );
            out.push_str(&suffix(plan));
            append_child(&mut out, left, cte_plans);
            append_child(&mut out, right, cte_plans);
            out
        }
        planner::PhysicalPlan::Materialize {
            input, reuse_key, ..
        } => {
            let mut out = format!(
                "{indent}{arrow}Materialize [{reuse_key}] (cost={cost:.2} rows={estimated_rows:.0})"
            );
            out.push_str(&suffix(plan));
            append_child(&mut out, input, cte_plans);
            out
        }
        planner::PhysicalPlan::RecursiveUnion {
            anchor,
            recursive,
            name,
            union_all,
            max_depth,
            ..
        } => {
            let qualifier = if *union_all { "all" } else { "distinct" };
            let mut out = format!(
                "{indent}{arrow}Recursive union [{name}] ({qualifier}, max_depth={max_depth}, cost={cost:.2} rows={estimated_rows:.0})"
            );
            out.push_str(&suffix(plan));
            append_child(&mut out, anchor, None);
            append_child(&mut out, recursive, None);
            out
        }
        planner::PhysicalPlan::ValuesScan { filter, .. }
        | planner::PhysicalPlan::JsonTableScan { filter, .. }
        | planner::PhysicalPlan::SystemScan { filter, .. } => {
            let label = match plan {
                planner::PhysicalPlan::ValuesScan { .. } => "Values scan",
                planner::PhysicalPlan::JsonTableScan { .. } => "JSON_TABLE scan",
                _ => "System table scan",
            };
            let mut line =
                format!("{indent}{arrow}{label} (cost={cost:.2} rows={estimated_rows:.0})");
            if let Some(filter) = filter {
                append_tree_details(&mut line, vec![("filter", explain_expr(filter))]);
            }
            line.push_str(&suffix(plan));
            line
        }
        planner::PhysicalPlan::SeqScan {
            table,
            filter,
            hint_note,
            ..
        } => {
            let name = table_display_name(table);
            let scan_kind = "Table scan";
            let mut line = format!(
                "{indent}{arrow}{scan_kind} on {name} (cost={cost:.2} rows={estimated_rows:.0})"
            );
            let mut details = Vec::new();
            push_partition_tree_detail(&mut details, table, filter.as_ref());
            push_scan_filter_and_hint(&mut details, filter.as_ref(), hint_note.as_deref());
            append_tree_details(&mut line, details);
            line.push_str(&suffix(plan));
            line
        }
        planner::PhysicalPlan::ZoneMapScan {
            table,
            filter,
            hint_note,
            zone_map,
            ..
        } => {
            let name = table_display_name(table);
            let mut line = format!(
                "{indent}{arrow}Zone map scan on {name} (cost={cost:.2} rows={estimated_rows:.0})"
            );
            let mut details = Vec::new();
            push_partition_tree_detail(&mut details, table, filter.as_ref());
            if let Some(filter) = filter {
                details.push(("filter", explain_expr(filter)));
            }
            details.push((
                "zone map",
                format!(
                    "pages={}/{} ranges={} zones={} metadata_bytes={} cache_misses={} storages={} estimate={}",
                    zone_map.surviving_pages,
                    zone_map.total_pages,
                    zone_map.ranges,
                    zone_map.zones_examined,
                    zone_map.metadata_read_bytes,
                    zone_map.metadata_cache_misses,
                    zone_map.storage_objects,
                    if zone_map.estimate_exact { "exact" } else { "sampled" }
                ),
            ));
            if let Some(note) = hint_note {
                details.push(("hint", note.clone()));
            }
            append_tree_details(&mut line, details);
            line.push_str(&suffix(plan));
            line
        }
        planner::PhysicalPlan::IndexMerge {
            table,
            root,
            filter,
            hint_note,
            ..
        } => {
            let name = table_display_name(table);
            let merge = IndexMergeInfo::new(root);
            let algorithm = merge.tree_algorithm;
            let mut line = format!(
                "{indent}{arrow}Index merge {algorithm} on {name} using {} (cost={cost:.2} rows={estimated_rows:.0})",
                merge.keys
            );
            let mut details = Vec::new();
            push_partition_tree_detail(&mut details, table, filter.as_ref());
            push_scan_filter_and_hint(&mut details, filter.as_ref(), hint_note.as_deref());
            append_tree_details(&mut line, details);
            line.push_str(&suffix(plan));
            line
        }
        planner::PhysicalPlan::IndexLookup { .. }
        | planner::PhysicalPlan::IndexMultiLookup { .. }
        | planner::PhysicalPlan::IndexRangeScan { .. }
        | planner::PhysicalPlan::IndexFastScan { .. } => {
            let access = IndexAccess::from_plan(plan).expect("matched index access");
            let name = table_display_name(access.table);
            let label = access.tree_label();
            let mut line = format!(
                "{indent}{arrow}{label} on {name} using {idx} (cost={cost:.2} rows={estimated_rows:.0})",
                idx = access.index.name
            );
            let mut details = Vec::new();
            if access.uses_mrr() {
                details.push(("heap fetch", "mrr".to_string()));
                details.push((
                    "planning mrr window",
                    (crate::engine::cbo::PLANNING_MRR_WINDOW_ROWS as u64).to_string(),
                ));
            }
            push_partition_tree_detail(&mut details, access.table, access.filter.as_ref());
            if let Some(condition) = access.condition() {
                details.push(("index condition", condition));
            }
            if let Some(like_details) = access.like_details() {
                details.extend(like_details);
            }
            push_scan_filter_and_hint(&mut details, access.filter.as_ref(), access.hint);
            append_tree_details(&mut line, details);
            line.push_str(&suffix(plan));
            line
        }
        planner::PhysicalPlan::FulltextScan {
            table,
            index,
            query,
            query_param,
            filter,
            hint_note,
            additional_matches,
            like_prefilter,
            ..
        } => {
            let name = table_display_name(table);
            let mut line = if like_prefilter.is_some() {
                format!(
                    "{indent}{arrow}Full-text LIKE ngram prefilter on {name} using {idx} (cost={cost:.2} rows={estimated_rows:.0})",
                    idx = index.name
                )
            } else {
                format!(
                    "{indent}{arrow}Full-text search on {name} using {idx} (cost={cost:.2} rows={estimated_rows:.0})",
                    idx = index.name
                )
            };
            let mut details = if let Some(prefilter) = like_prefilter {
                vec![
                    (
                        "like shape",
                        if prefilter.pattern_param.is_some() {
                            "adaptive ?".to_string()
                        } else {
                            "unanchored".to_string()
                        },
                    ),
                    ("ngram tokens", prefilter.tokens.len().to_string()),
                    ("literal segments", prefilter.literal_segments.to_string()),
                ]
            } else {
                vec![
                    (
                        "match",
                        query_param
                            .map(|idx| format!("AGAINST ?{idx}"))
                            .unwrap_or_else(|| format!("AGAINST '{query}'")),
                    ),
                    (
                        "match predicates",
                        (additional_matches.len() + 1).to_string(),
                    ),
                ]
            };
            push_partition_tree_detail(&mut details, table, filter.as_ref());
            push_scan_filter_and_hint(&mut details, filter.as_ref(), hint_note.as_deref());
            append_tree_details(&mut line, details);
            line.push_str(&suffix(plan));
            line
        }
        planner::PhysicalPlan::SpatialScan {
            table,
            index,
            query_probe,
            filter,
            hint_note,
            ..
        } => {
            let name = table_display_name(table);
            let mut line = format!(
                "{indent}{arrow}Spatial index scan on {name} using {idx} (cost={cost:.2} rows={estimated_rows:.0})",
                idx = index.name
            );
            let window = match query_probe {
                planner::SpatialProbe::Constant(query_mbr) => format!(
                    "({:.6} {:.6}, {:.6} {:.6})",
                    query_mbr[0], query_mbr[1], query_mbr[2], query_mbr[3]
                ),
                planner::SpatialProbe::Parameter(expr) => {
                    format!("parameter {}", explain_expr(expr))
                }
            };
            let mut details = vec![("window", window)];
            push_partition_tree_detail(&mut details, table, filter.as_ref());
            push_scan_filter_and_hint(&mut details, filter.as_ref(), hint_note.as_deref());
            append_tree_details(&mut line, details);
            line.push_str(&suffix(plan));
            line
        }
        planner::PhysicalPlan::VectorIndexScan {
            table,
            index,
            metric,
            k,
            ef_search,
            filter,
            hint_note,
            ..
        } => {
            let name = table_display_name(table);
            let (algo, effort_key) = vector_access_labels(index);
            let mut line = format!(
                "{indent}{arrow}Vector index ({algo}) scan on {name} using {idx} (cost={cost:.2} rows={estimated_rows:.0})",
                idx = index.name
            );
            let mut details = vec![
                ("metric", metric.as_str().to_string()),
                ("k", k.to_string()),
                (effort_key, ef_search.to_string()),
            ];
            details.push((
                "quantization",
                index
                    .vector
                    .as_ref()
                    .map_or("none", |v| v.quantization.name())
                    .to_string(),
            ));
            details.push((
                "rerank_pool",
                crate::engine::vector::session::explained_rerank_pool(index, *k, *ef_search)
                    .to_string(),
            ));
            push_partition_tree_detail(&mut details, table, filter.as_ref());
            push_scan_filter_and_hint(&mut details, filter.as_ref(), hint_note.as_deref());
            append_tree_details(&mut line, details);
            line.push_str(&suffix(plan));
            line
        }
        planner::PhysicalPlan::VectorBruteScan {
            table, metric, k, ..
        } => {
            let name = table_display_name(table);
            let mut line = format!(
                "{indent}{arrow}Vector brute-force scan on {name} (cost={cost:.2} rows={estimated_rows:.0})"
            );
            let mut details = vec![("metric", metric.clone()), ("k", k.to_string())];
            push_partition_tree_detail(&mut details, table, None);
            append_tree_details(&mut line, details);
            line.push_str(&suffix(plan));
            line
        }
        planner::PhysicalPlan::IndexInLookup {
            table,
            index,
            key_prefix,
            in_column,
            subquery_plan,
            filter,
            hint_note,
            ..
        } => {
            let name = table_display_name(table);
            let mut line = format!(
                "{indent}{arrow}Index lookup on {name} using {idx} with IN subquery (cost={cost:.2} rows={estimated_rows:.0})",
                idx = index.name
            );
            let mut details = vec![(
                "index condition",
                index_in_lookup_condition(index, key_prefix.0.len(), in_column),
            )];
            push_partition_tree_detail(&mut details, table, filter.as_ref());
            push_scan_filter_and_hint(&mut details, filter.as_ref(), hint_note.as_deref());
            append_tree_details(&mut line, details);
            line.push_str(&suffix(plan));
            append_child(&mut line, subquery_plan, cte_plans);
            line
        }
        planner::PhysicalPlan::NestedLoopJoin {
            left,
            right,
            join_type,
            condition,
            ..
        } => {
            let label = join_tree_label("Nested loop", *join_type);
            let mut out =
                format!("{indent}{arrow}{label} (cost={cost:.2} rows={estimated_rows:.0})");
            if let Some(condition) = condition {
                append_tree_details(&mut out, vec![("condition", explain_expr(condition))]);
            }
            out.push_str(&suffix(plan));
            append_child(&mut out, left, cte_plans);
            append_child(&mut out, right, cte_plans);
            out
        }
        planner::PhysicalPlan::IndexNestedLoopJoin {
            left,
            right_table,
            right_index,
            join_type,
            condition,
            right_covering,
            ..
        } => {
            let name = table_display_name(right_table);
            let label = join_tree_label("Index nested loop", *join_type);
            let mut out =
                format!("{indent}{arrow}{label} (cost={cost:.2} rows={estimated_rows:.0})");
            let batch = super::plan::index_nested_loop_batch_eligibility(
                condition.as_ref(),
                right_table,
                right_index,
            );
            let mut details = Vec::with_capacity(3);
            if batch != "eligible" {
                details.push(("batch", batch.to_string()));
            }
            if !right_covering {
                details.push(("covering", "false".to_string()));
            }
            if let Some(condition) = condition {
                details.push(("condition", explain_expr(condition)));
            }
            append_tree_details(&mut out, details);
            out.push_str(&suffix(plan));
            append_child(&mut out, left, cte_plans);
            out.push('\n');
            let inner_indent = "    ".repeat(depth + 1);
            out.push_str(&format!(
                "{inner_indent}-> Index lookup on {name} using {idx}",
                idx = right_index.name
            ));
            out.push_str(" (actual stats included in parent join)");
            out
        }
        planner::PhysicalPlan::HashJoin {
            left,
            right,
            join_type,
            build_side,
            left_keys,
            right_keys,
            null_safe,
            residual,
            ..
        } => {
            let side = match build_side {
                planner::BuildSide::Left => "left",
                planner::BuildSide::Right => "right",
            };
            let label = join_tree_label("Hash", *join_type);
            let mut out = format!(
                "{indent}{arrow}{label} (build={side}, cost={cost:.2} rows={estimated_rows:.0})"
            );
            let mut details = vec![(
                "keys",
                hash_join_keys_text(left_keys, right_keys, null_safe),
            )];
            let key_collations =
                super::plan::hash_join_key_collation_names(left, left_keys, right, right_keys);
            if key_collations.iter().any(|name| name != "non_text") {
                details.push(("key_collations", key_collations.join(",")));
            }
            if let Some(residual) = residual {
                details.push(("residual", explain_expr(residual)));
            }
            if super::plan::partition_wise_hash_join_leaf_pairs(left, right, left_keys, right_keys)
                .is_some()
            {
                details.push(("partition_wise", "true".to_string()));
            }
            append_tree_details(&mut out, details);
            out.push_str(&suffix(plan));
            append_child(&mut out, left, cte_plans);
            append_child(&mut out, right, cte_plans);
            out
        }
        planner::PhysicalPlan::AdaptiveJoin {
            left,
            right,
            join_type,
            candidates,
            preferred,
            ..
        } => {
            let label = join_tree_label("Adaptive", *join_type);
            let candidates_text = candidates
                .iter()
                .map(
                    |candidate| match planner::adaptive_candidate_choice(candidate) {
                        planner::AdaptiveJoinChoice::Hash => "Hash join",
                        planner::AdaptiveJoinChoice::IndexNestedLoop => "Index nested loop",
                        planner::AdaptiveJoinChoice::NestedLoop => "Nested loop",
                    },
                )
                .collect::<Vec<_>>()
                .join(",");
            let mut out = format!(
                "{indent}{arrow}{label} (preferred={}, cost={cost:.2} rows={estimated_rows:.0})",
                planner::adaptive_choice_name(*preferred)
            );
            append_tree_details(&mut out, vec![("candidates", candidates_text)]);
            out.push_str(&suffix(plan));
            append_child(&mut out, left, cte_plans);
            out.push('\n');
            if *preferred == planner::AdaptiveJoinChoice::IndexNestedLoop {
                if let Some(planner::AdaptiveJoinCandidate::IndexNestedLoop {
                    right_table,
                    right_index,
                    ..
                }) = candidates
                    .iter()
                    .find(|candidate| planner::adaptive_candidate_choice(candidate) == *preferred)
                {
                    let inner_indent = "    ".repeat(depth + 1);
                    let name = table_display_name(right_table);
                    out.push_str(&format!(
                        "{inner_indent}-> Index lookup on {name} using {idx}",
                        idx = right_index.name
                    ));
                    return out;
                }
            }
            out.push_str(&format_plan_tree_nodes(
                right,
                depth + 1,
                stats,
                cte_plans,
                cte_extras,
            ));
            out
        }
        planner::PhysicalPlan::CteScan { name, alias, .. } => {
            let display = alias.as_deref().unwrap_or(name);
            let logical_name = if name.starts_with('\0') {
                display
            } else {
                name.as_str()
            };
            let mut line = format!(
                "{indent}{arrow}CTE scan on {display} <{logical_name}> (cost={cost:.2} rows={estimated_rows:.0})"
            );
            let cte_stats = cte_scan_stats(stats, plan, name, cte_plans, cte_extras);
            line.push_str(&pipeline_tree_suffix(plan, depth));
            line.push_str(&match cte_stats {
                Some(actual) => analyze_stats::format_tree_suffix(Some(actual)),
                None => stats
                    .map(|map| {
                        analyze_stats::format_tree_suffix(display_stats(
                            map, plan, cte_plans, cte_extras,
                        ))
                    })
                    .unwrap_or_default(),
            });
            if let Some(cte_map) = cte_plans {
                let key = name.to_ascii_lowercase();
                if let Some(cte_plan) = cte_map.get(&key) {
                    // Surface UNION ALL branches as siblings: prefix the
                    // primary plan with `Union all branch 1` and each extra
                    // with `Union all branch N`, so the user can see the
                    // full materialised CTE shape — not just the LEFT side.
                    let extras_for = cte_extras.and_then(|m| m.get(&key));
                    let has_extras = extras_for.map(|v| !v.is_empty()).unwrap_or(false);
                    line.push('\n');
                    if has_extras {
                        let child_indent = "    ".repeat(depth + 1);
                        line.push_str(&format!("{child_indent}-> Union all branch 1\n"));
                        line.push_str(&format_plan_tree_nodes(
                            cte_plan,
                            depth + 2,
                            stats,
                            cte_plans,
                            cte_extras,
                        ));
                        for (i, extra) in extras_for.unwrap().iter().enumerate() {
                            line.push('\n');
                            line.push_str(&format!(
                                "{child_indent}-> Union all branch {n}\n",
                                n = i + 2
                            ));
                            line.push_str(&format_plan_tree_nodes(
                                extra,
                                depth + 2,
                                stats,
                                cte_plans,
                                cte_extras,
                            ));
                        }
                    } else {
                        line.push_str(&format_plan_tree_nodes(
                            cte_plan,
                            depth + 1,
                            stats,
                            cte_plans,
                            cte_extras,
                        ));
                    }
                }
            }
            line
        }
        planner::PhysicalPlan::GenerateSeries { relation, .. } => {
            let mut line = format!(
                "{indent}{arrow}Function scan on {relation} <generate_series> (cost={cost:.2} rows={estimated_rows:.0})"
            );
            line.push_str(&suffix(plan));
            line
        }
        planner::PhysicalPlan::IndexAggregate {
            table,
            index,
            low,
            high,
            filter,
            access_rows,
            ..
        } => {
            let name = table_display_name(table);
            let mut out = format!(
                "{indent}{arrow}Index aggregate on {name} using {idx} (cost={cost:.2} rows={estimated_rows:.0}, workers=1)",
                idx = index.name
            );
            if let Some(condition) = index_range_condition(index, low, high) {
                append_tree_details(&mut out, vec![("index condition", condition)]);
            }
            if let Some(filter) = filter {
                append_tree_details(&mut out, vec![("filter", explain_expr(filter))]);
            }
            out.push_str(&suffix(plan));
            out.push('\n');
            let child_indent = "    ".repeat(depth + 1);
            let mut child = format!(
                "{child_indent}-> {scan_kind} on {name} using {idx} (rows={access_rows:.0}, workers=1)",
                scan_kind = if filter.is_some() { "Index scan" } else { "Index-only scan" },
                idx = index.name
            );
            if let Some(condition) = index_range_condition(index, low, high) {
                append_tree_details(&mut child, vec![("index condition", condition)]);
            }
            if filter.is_some() {
                append_tree_details(&mut child, vec![("heap fetch", "ordered".to_string())]);
            }
            out.push_str(&child);
            out
        }
        planner::PhysicalPlan::CoveringIndexAggregate {
            table,
            index,
            low,
            high,
            access_rows,
            ..
        } => {
            let name = table_display_name(table);
            let mut out = format!(
                "{indent}{arrow}Index aggregate on {name} using {idx} (cost={cost:.2} rows={estimated_rows:.0}, workers=1)",
                idx = index.name
            );
            if let Some(condition) = index_range_condition(index, low, high) {
                append_tree_details(&mut out, vec![("index condition", condition)]);
            }
            out.push_str(&suffix(plan));
            out.push('\n');
            let child_indent = "    ".repeat(depth + 1);
            let mut child = format!(
                "{child_indent}-> Index-only scan on {name} using {idx} (rows={access_rows:.0}, workers=1)",
                idx = index.name
            );
            if let Some(condition) = index_range_condition(index, low, high) {
                append_tree_details(&mut child, vec![("index condition", condition)]);
            }
            out.push_str(&child);
            out
        }
        planner::PhysicalPlan::ParallelSeqScan {
            table,
            filter,
            degree,
            hint_note,
            ..
        } => {
            let name = table_display_name(table);
            let scan_kind = "Parallel table scan";
            let mut line = format!(
                "{indent}{arrow}{scan_kind} on {name} (workers={degree}, cost={cost:.2} rows={estimated_rows:.0})"
            );
            let mut details = Vec::new();
            push_partition_tree_detail(&mut details, table, filter.as_ref());
            push_scan_filter_and_hint(&mut details, filter.as_ref(), hint_note.as_deref());
            append_tree_details(&mut line, details);
            line.push_str(&suffix(plan));
            line
        }
        planner::PhysicalPlan::ParallelZoneMapScan {
            table,
            filter,
            degree,
            hint_note,
            zone_map,
            ..
        } => {
            let name = table_display_name(table);
            let mut line = format!(
                "{indent}{arrow}Parallel zone map scan on {name} (workers={degree}, cost={cost:.2} rows={estimated_rows:.0})"
            );
            let mut details = Vec::new();
            push_partition_tree_detail(&mut details, table, filter.as_ref());
            if let Some(filter) = filter {
                details.push(("filter", explain_expr(filter)));
            }
            details.push((
                "zone map",
                format!(
                    "pages={}/{} ranges={} zones={} metadata_bytes={} cache_misses={} storages={}",
                    zone_map.surviving_pages,
                    zone_map.total_pages,
                    zone_map.ranges,
                    zone_map.zones_examined,
                    zone_map.metadata_read_bytes,
                    zone_map.metadata_cache_misses,
                    zone_map.storage_objects
                ),
            ));
            if let Some(note) = hint_note {
                details.push(("hint", note.clone()));
            }
            append_tree_details(&mut line, details);
            line.push_str(&suffix(plan));
            line
        }
        planner::PhysicalPlan::ParallelIndexFastScan {
            table,
            index,
            low,
            high,
            filter,
            degree,
            hint_note,
            ..
        } => {
            let name = table_display_name(table);
            let mut line = format!(
                "{indent}{arrow}Parallel index-only scan on {name} using {idx} (workers={degree}, cost={cost:.2} rows={estimated_rows:.0})",
                idx = index.name
            );
            let mut details = Vec::new();
            push_partition_tree_detail(&mut details, table, filter.as_ref());
            if let Some(condition) = index_range_condition(index, low, high) {
                details.push(("index condition", condition));
            }
            push_scan_filter_and_hint(&mut details, filter.as_ref(), hint_note.as_deref());
            append_tree_details(&mut line, details);
            line.push_str(&suffix(plan));
            line
        }
        planner::PhysicalPlan::ParallelIndexRangeScan {
            table,
            index,
            low,
            high,
            filter,
            degree,
            hint_note,
            ..
        } => {
            let name = table_display_name(table);
            let mut line = format!(
                "{indent}{arrow}Parallel index range scan on {name} using {idx} (workers={degree}, cost={cost:.2} rows={estimated_rows:.0})",
                idx = index.name
            );
            let mut details = Vec::new();
            push_partition_tree_detail(&mut details, table, filter.as_ref());
            if let Some(condition) = index_range_condition(index, low, high) {
                details.push(("index condition", condition));
            }
            push_scan_filter_and_hint(&mut details, filter.as_ref(), hint_note.as_deref());
            append_tree_details(&mut line, details);
            line.push_str(&suffix(plan));
            line
        }
        planner::PhysicalPlan::ParallelIndexAggregate {
            table,
            index,
            low,
            high,
            access_rows,
            degree,
            ..
        } => {
            let name = table_display_name(table);
            let mut out = format!(
                "{indent}{arrow}Parallel index aggregate on {name} using {idx} (cost={cost:.2} rows={estimated_rows:.0}, workers={degree})",
                idx = index.name
            );
            if let Some(condition) = index_range_condition(index, low, high) {
                append_tree_details(&mut out, vec![("index condition", condition)]);
            }
            out.push_str(&suffix(plan));
            out.push('\n');
            let child_indent = "    ".repeat(depth + 1);
            let mut child = format!(
                "{child_indent}-> Parallel index-only scan on {name} using {idx} (rows={access_rows:.0}, workers={degree})",
                idx = index.name
            );
            if let Some(condition) = index_range_condition(index, low, high) {
                append_tree_details(&mut child, vec![("index condition", condition)]);
            }
            out.push_str(&child);
            out
        }
        planner::PhysicalPlan::Exchange {
            input,
            degree,
            output_order,
            ..
        } => {
            let label = match output_order {
                planner::ExchangeOrder::Unordered => "Exchange",
                planner::ExchangeOrder::Sorted => "Exchange Merge",
            };
            let mut out = format!("{indent}{arrow}{label} (workers={degree}, cost={cost:.2})");
            out.push_str(&suffix(plan));
            append_child(&mut out, input, cte_plans);
            out
        }

        planner::PhysicalPlan::NullExtend { .. }
        | planner::PhysicalPlan::Sort { .. }
        | planner::PhysicalPlan::IncrementalSort { .. }
        | planner::PhysicalPlan::TopK { .. }
        | planner::PhysicalPlan::TopWindow { .. }
        | planner::PhysicalPlan::ParallelTopK { .. }
        | planner::PhysicalPlan::HashAggregate { .. }
        | planner::PhysicalPlan::HashRollupAggregate { .. }
        | planner::PhysicalPlan::HashGroupingSetsAggregate { .. }
        | planner::PhysicalPlan::Limit { .. }
        | planner::PhysicalPlan::Projection { .. }
        | planner::PhysicalPlan::Filter { .. }
        | planner::PhysicalPlan::Apply { .. }
        | planner::PhysicalPlan::WindowAgg { .. }
        | planner::PhysicalPlan::StreamingAggregate { .. }
        | planner::PhysicalPlan::StreamingRollupAggregate { .. }
        | planner::PhysicalPlan::StreamingGroupingSetsAggregate { .. }
        | planner::PhysicalPlan::StreamingDistinct { .. }
        | planner::PhysicalPlan::ParallelHashAggregate { .. }
        | planner::PhysicalPlan::ParallelRollupAggregate { .. }
        | planner::PhysicalPlan::ParallelGroupingSetsAggregate { .. }
        | planner::PhysicalPlan::ParallelSort { .. } => {
            unreachable!("unary operators handled before rendering")
        }
    }
}

fn join_tree_label(base: &'static str, join_type: crate::parser::JoinType) -> String {
    match join_type {
        crate::parser::JoinType::Left => format!("{base} left join"),
        crate::parser::JoinType::Right => format!("{base} right join"),
        crate::parser::JoinType::Full => format!("{base} full join"),
        crate::parser::JoinType::Semi => format!("{base} semi join"),
        crate::parser::JoinType::Anti => format!("{base} anti join"),
        // Inner and Cross keep the bare "<base> join" label, matching the
        // long-standing TREE output (and the substrings existing tests assert).
        crate::parser::JoinType::Inner | crate::parser::JoinType::Cross => {
            format!("{base} join")
        }
    }
}

fn join_type_json(join_type: crate::parser::JoinType) -> &'static str {
    match join_type {
        crate::parser::JoinType::Inner => "inner",
        crate::parser::JoinType::Left => "left",
        crate::parser::JoinType::Right => "right",
        crate::parser::JoinType::Full => "full",
        crate::parser::JoinType::Cross => "cross",
        crate::parser::JoinType::Semi => "semi",
        crate::parser::JoinType::Anti => "anti",
    }
}

fn explain_expr(expr: &crate::parser::Expr) -> String {
    planner::expr_to_explain_string(expr)
}

fn hash_join_keys_text(
    left_keys: &[crate::parser::Expr],
    right_keys: &[crate::parser::Expr],
    null_safe: &[bool],
) -> String {
    left_keys
        .iter()
        .zip(right_keys.iter())
        .zip(null_safe.iter())
        .map(|((left, right), null_safe)| {
            let op = if *null_safe { "<=>" } else { "=" };
            format!("{}{}{}", explain_expr(left), op, explain_expr(right))
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn append_tree_details(line: &mut String, details: Vec<(&'static str, String)>) {
    if details.is_empty() {
        return;
    }
    line.push_str("  (");
    line.push_str(
        &details
            .into_iter()
            .map(|(label, value)| format!("{label}: {value}"))
            .collect::<Vec<_>>()
            .join("; "),
    );
    line.push(')');
}

/// Human-readable label for index key part `pos` used by EXPLAIN.
/// Plain column parts show the column name; functional parts wrap the
/// expression SQL in parens so the output line reads
/// `(LOWER(email))=const` instead of the bare placeholder.
fn index_part_label(index: &crate::storage::IndexSchema, pos: usize) -> String {
    match index.key_part(pos) {
        Some(crate::storage::IndexKeyPart::Expression(expr)) => format!("({})", expr.expr_sql),
        Some(crate::storage::IndexKeyPart::Column(name)) => name.to_string(),
        None => index.columns.get(pos).cloned().unwrap_or_default(),
    }
}

fn index_lookup_condition(
    index: &crate::storage::IndexSchema,
    prefix_len: usize,
) -> Option<String> {
    let parts = (0..prefix_len)
        .map(|pos| format!("{}=const", index_part_label(index, pos)))
        .collect::<Vec<_>>();
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" AND "))
    }
}

fn index_in_lookup_condition(
    index: &crate::storage::IndexSchema,
    key_prefix_len: usize,
    in_column: &str,
) -> String {
    let mut parts = index_lookup_condition(index, key_prefix_len)
        .map(|condition| vec![condition])
        .unwrap_or_default();
    parts.push(format!("{in_column} IN (subquery)"));
    parts.join(" AND ")
}

fn index_range_condition(
    index: &crate::storage::IndexSchema,
    low: &Option<crate::storage::IndexBound>,
    high: &Option<crate::storage::IndexBound>,
) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(bound) = low {
        if let Some(condition) = index_bound_condition(index, bound, true) {
            parts.push(condition);
        }
    }
    if let Some(bound) = high {
        if let Some(condition) = index_bound_condition(index, bound, false) {
            parts.push(condition);
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" AND "))
    }
}

fn like_access_diagnostics(
    table: &crate::planner::PlanTable,
    filter: &crate::parser::Expr,
) -> Option<Vec<(&'static str, String)>> {
    use crate::engine::expr::{analyze_like_access, LikeAccessShape};
    use crate::parser::Expr;

    let mut stack = vec![filter];
    while let Some(expr) = stack.pop() {
        match expr {
            Expr::Like {
                expr,
                pattern,
                negated: false,
                escape_char,
            } => {
                let column_name = match expr.as_ref() {
                    Expr::Identifier(name) => name.as_str(),
                    Expr::CompoundIdentifier(parts) => parts.last()?.as_str(),
                    _ => continue,
                };
                let column = table
                    .schema
                    .columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(column_name))?;
                let collation = column.collation.as_deref().unwrap_or("binary");
                let pattern = match pattern.as_ref() {
                    Expr::Placeholder(index) => {
                        return Some(vec![
                            ("like shape", format!("adaptive ?{index}")),
                            ("collation", collation.to_string()),
                            ("sort-key range", "bound at execution".to_string()),
                            ("residual LIKE", "yes".to_string()),
                        ]);
                    }
                    Expr::Literal(crate::parser::Literal::String(value)) => value.clone(),
                    Expr::BoundValue(crate::types::Value::String(value)) => value.clone(),
                    _ => continue,
                };
                let analysis =
                    analyze_like_access(&pattern, *escape_char, false, column.collation.as_deref());
                let (shape, range) = match analysis.shape {
                    LikeAccessShape::Literal(value) => {
                        let key = crate::charset::collation_key(collation, &value);
                        (
                            "literal",
                            format!("[{}, {}]", hex_bytes(&key), hex_bytes(&key)),
                        )
                    }
                    LikeAccessShape::Prefix(value) => {
                        let low = crate::charset::collation_key(collation, &value);
                        let high = byte_prefix_successor(&low);
                        (
                            "prefix",
                            format!(
                                "[{}, {})",
                                hex_bytes(&low),
                                high.as_deref()
                                    .map(hex_bytes)
                                    .unwrap_or_else(|| "+inf".to_string())
                            ),
                        )
                    }
                    LikeAccessShape::Unanchored => ("unanchored", "none".to_string()),
                    LikeAccessShape::Unsargable => ("unsargable", "none".to_string()),
                };
                return Some(vec![
                    ("like shape", shape.to_string()),
                    ("collation", collation.to_string()),
                    ("sort-key range", range),
                    ("residual LIKE", "yes".to_string()),
                ]);
            }
            Expr::Binary { left, right, .. } => {
                stack.push(right);
                stack.push(left);
            }
            Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
                stack.push(expr)
            }
            _ => {}
        }
    }
    None
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2 + 2);
    out.push_str("0x");
    crate::util::hex::append_lower(&mut out, bytes);
    out
}

fn byte_prefix_successor(bytes: &[u8]) -> Option<Vec<u8>> {
    let mut out = bytes.to_vec();
    while let Some(last) = out.last_mut() {
        if *last != u8::MAX {
            *last += 1;
            return Some(out);
        }
        out.pop();
    }
    None
}

fn index_bound_condition(
    index: &crate::storage::IndexSchema,
    bound: &crate::storage::IndexBound,
    lower: bool,
) -> Option<String> {
    let prefix_len = bound.key.0.len();
    if prefix_len == 0 {
        return None;
    }
    let op = match (lower, bound.inclusive) {
        (true, true) => ">=",
        (true, false) => ">",
        (false, true) => "<=",
        (false, false) => "<",
    };
    if prefix_len == 1 {
        return Some(format!("{}{op}const", index_part_label(index, 0)));
    }
    let columns = (0..prefix_len)
        .map(|pos| index_part_label(index, pos))
        .collect::<Vec<_>>()
        .join(",");
    let consts = std::iter::repeat("const")
        .take(prefix_len)
        .collect::<Vec<_>>()
        .join(",");
    Some(format!("({columns}) {op} ({consts})"))
}

fn json_string(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"<invalid>\"".to_string())
}

fn json_string_field(out: &mut String, indent: &str, name: &str, value: &str) {
    out.push_str(&format!("{indent}  \"{name}\": {},\n", json_string(value)));
}

fn json_partitions_field(
    out: &mut String,
    indent: &str,
    table: &planner::PlanTable,
    filter: Option<&crate::parser::Expr>,
) {
    let Some(names) = partition_names_for_filter(table, filter) else {
        return;
    };
    let values = names
        .iter()
        .map(|name| json_string(name))
        .collect::<Vec<_>>()
        .join(", ");
    out.push_str(&format!("{indent}  \"partitions\": [{values}],\n"));
}

fn push_partition_tree_detail(
    details: &mut Vec<(&'static str, String)>,
    table: &planner::PlanTable,
    filter: Option<&crate::parser::Expr>,
) {
    if let Some(names) = partition_names_for_filter(table, filter) {
        details.push(("partitions", names.join(",")));
    }
}

fn partition_names_for_filter(
    table: &planner::PlanTable,
    filter: Option<&crate::parser::Expr>,
) -> Option<Vec<String>> {
    let part = table.schema.partitioning.as_ref()?;
    let storage_ids = table
        .resolved_partition_storage_ids(filter)
        .unwrap_or_else(|| part.storage_ids());
    let mut names = Vec::new();
    for storage_id in storage_ids {
        for partition in &part.partitions {
            if partition.subpartitions.is_empty() && partition.storage_id == storage_id {
                names.push(partition.name.clone());
                break;
            }
            if let Some(subpartition) = partition
                .subpartitions
                .iter()
                .find(|subpartition| subpartition.storage_id == storage_id)
            {
                names.push(subpartition.name.clone());
                break;
            }
        }
    }
    Some(names)
}

/// Phase 2b: emit `"rows_estimation_source"` for a table-access node whose
/// row estimate is driven by an attached predicate. The value tells the
/// reader whether the planner's selectivity came from a histogram, NDV/
/// min-max stats, or a fallback default constant (`cbo_default_*`). Skipped
/// when the node has no residual filter (the estimate is then the raw
/// access cardinality, with no selectivity to attribute).
fn json_rows_estimation_source(
    out: &mut String,
    indent: &str,
    table: &planner::PlanTable,
    filter: &Option<crate::parser::Expr>,
) {
    let Some(filter) = filter else { return };
    let label = match planner::filter_rows_source(&table.schema, filter) {
        crate::engine::cbo::SelectivitySource::Histogram => "histogram",
        crate::engine::cbo::SelectivitySource::Ndv => "ndv",
        crate::engine::cbo::SelectivitySource::Default => "default",
    };
    out.push_str(&format!(
        "{indent}  \"rows_estimation_source\": \"{label}\",\n"
    ));
}

fn format_plan_json_with_ctes(
    plan: &planner::PhysicalPlan,
    actual: Option<(&NodeStatsMap, u64, f64, NodeIoStats)>,
    cte_plans: Option<&std::collections::HashMap<String, planner::PhysicalPlan>>,
    cte_extras: Option<&CteExtraBranches>,
) -> String {
    let _pipeline_scope = PipelineDisplayScope::enter(plan);
    let mut out = String::from("{\n  \"query_block\": {\n    \"select_id\": 1,\n");
    // D1: MySQL 8.0 emits the whole-query estimated cost at the query_block
    // level. Use the root operator's cumulative total cost.
    out.push_str(&format!(
        "    \"cost_info\": {{ \"query_cost\": \"{:.2}\" }},\n",
        planner::plan_cost(plan).total.max(0.0)
    ));
    if let Some((stats, rows, ms, io)) = actual {
        let unassigned = stats.unassigned_io();
        out.push_str(&format!(
            "    \"execution\": {{ \"rows_total\": {rows}, \"wall_time_ms\": {ms:.3}, \"physical_reads\": {physical_reads}, \"cache_reads\": {cache_reads}, \"unassigned_physical_reads\": {unassigned_physical}, \"unassigned_cache_reads\": {unassigned_cache}, \"cache_hit_rate\": {hit_rate:.2}, \"io_scope\": \"query\" }},\n",
            physical_reads = io.physical_reads,
            cache_reads = io.cache_reads,
            unassigned_physical = unassigned.physical_reads,
            unassigned_cache = unassigned.cache_reads,
            hit_rate = if io.cache_reads + io.physical_reads == 0 {
                0.0
            } else {
                io.cache_reads as f64 * 100.0 / (io.cache_reads + io.physical_reads) as f64
            },
        ));
    }
    let stats = actual.map(|(s, _, _, _)| s);
    format_plan_json_node(plan, &mut out, 2, stats, cte_plans, cte_extras);
    out.push_str("  }\n}");
    out
}

/// Close out a JSON node object: append the per-node `"actual"` field
/// (preceded by `,\n` to slot after the existing last field) when
/// stats are present, or just a newline otherwise, then the closing
/// `}` and trailing newline.
///
/// Caller pattern: emit the last regular field of a node WITHOUT a
/// trailing newline, then call this helper.
fn json_close_node(
    out: &mut String,
    indent: &str,
    stats: Option<&NodeStatsMap>,
    plan: &planner::PhysicalPlan,
) {
    if let Some(info) = pipeline_display_info(plan) {
        let kernel = if matches!(
            crate::engine::executor::vec::config::effective_mode(),
            crate::engine::executor::vec::config::VecMode::Off
        ) {
            "scalar"
        } else {
            "vector"
        };
        out.push_str(&format!(
            ",\n{indent}  \"pipeline_id\": {}, \"stage_id\": {}, \"blocking\": {}, \"exchange\": {}, \"kernel\": \"{}\"",
            info.pipeline, info.stage, info.blocking, info.exchange, kernel
        ));
    }
    let actual = stats.and_then(|map| display_stats(map, plan, None, None));
    json_close_node_with_actual(out, indent, actual);
}

fn json_close_node_with_actual(out: &mut String, indent: &str, actual: Option<NodeStats>) {
    if let Some(s) = actual {
        if s.source == analyze_stats::StatsSource::Included {
            let owner = s
                .included_in
                .map(|id| id.0.to_string())
                .unwrap_or_else(|| "null".to_string());
            out.push_str(&format!(
                ",\n{indent}  \"actual\": {{ \"executed\": false, \"stats_status\": \"included\", \"included_in_operator\": {owner} }}\n"
            ));
            out.push_str(&format!("{indent}}}\n"));
            return;
        }
        if s.loops == 0 {
            out.push_str(&format!(
                ",\n{indent}  \"actual\": {{ \"executed\": false, \"stats_status\": \"never_executed\", \"stats_source\": \"{}\" }}\n",
                s.source.as_str()
            ));
            out.push_str(&format!("{indent}}}\n"));
            return;
        }
        let loops = s.loops.max(1) as f64;
        let active_time_total_ms = s.time_ns as f64 / 1_000_000.0;
        let last_row_ms = active_time_total_ms / loops;
        let first_total = if s.first_row_ns == 0 {
            s.time_ns
        } else {
            s.first_row_ns
        };
        let first_row_ms = first_total as f64 / 1_000_000.0 / loops;
        let self_time_total_ms = s.self_time_ns as f64 / 1_000_000.0;
        let rows_per_loop = s.rows as f64 / loops;
        let total_reads = s.cache_reads.saturating_add(s.physical_reads);
        let io_scope = if s.io_is_self {
            "operator_self"
        } else {
            "operator_interval_inclusive"
        };
        let hit_rate = if total_reads == 0 {
            0.0
        } else {
            s.cache_reads as f64 * 100.0 / total_reads as f64
        };
        let mut extra = String::new();
        if s.rrf_branches > 0 {
            let n = usize::from(s.rrf_branches);
            let work = serde_json::json!({"input_rows": &s.rrf_input_rows[..n], "unique_rows": &s.rrf_unique_rows[..n], "union_rows": s.rrf_union_rows, "overlap_rows": s.rrf_overlap_rows, "fusion_ms": s.rrf_fusion_ns as f64 / 1_000_000.0});
            extra.push_str(&format!(", \"rrf\": {work}"));
        }
        if s.peak_memory_bytes > 0 {
            extra.push_str(&format!(", \"peak_mem\": {}", s.peak_memory_bytes));
            extra.push_str(&format!(", \"peak_memory_bytes\": {}", s.peak_memory_bytes));
        }
        if s.mrr_windows > 0 || s.mrr_workarea_flushes > 0 {
            extra.push_str(&format!(", \"mrr_windows\": {}", s.mrr_windows));
            extra.push_str(&format!(", \"mrr_rids\": {}", s.mrr_rids));
            extra.push_str(&format!(", \"mrr_unique_pages\": {}", s.mrr_unique_pages));
            extra.push_str(&format!(", \"mrr_page_reuses\": {}", s.mrr_page_reuses));
            extra.push_str(&format!(
                ", \"mrr_workarea_flushes\": {}",
                s.mrr_workarea_flushes
            ));
            extra.push_str(&format!(", \"mrr_peak_bytes\": {}", s.mrr_peak_bytes));
        }
        if s.row_goal > 0 {
            extra.push_str(&format!(", \"row_goal\": {}", s.row_goal));
            extra.push_str(&format!(", \"candidate_batches\": {}", s.candidate_batches));
            extra.push_str(&format!(
                ", \"candidate_window_peak\": {}",
                s.candidate_window_peak
            ));
        }
        if s.spill_bytes > 0 {
            extra.push_str(&format!(", \"spill_bytes\": {}", s.spill_bytes));
            extra.push_str(&format!(", \"spill_bytes_written\": {}", s.spill_bytes));
        }
        if s.spill_bytes_read > 0 {
            extra.push_str(&format!(", \"spill_bytes_read\": {}", s.spill_bytes_read));
        }
        if s.spill_runs > 0 {
            extra.push_str(&format!(", \"spill_runs\": {}", s.spill_runs));
        }
        if s.spill_passes > 0 {
            extra.push_str(&format!(", \"spill_passes\": {}", s.spill_passes));
        }
        if s.spill_partitions > 0 {
            extra.push_str(&format!(", \"spill_partitions\": {}", s.spill_partitions));
        }
        if s.spill_file_opens > 0 {
            extra.push_str(&format!(", \"spill_file_opens\": {}", s.spill_file_opens));
        }
        if s.spill_repartitions > 0 {
            extra.push_str(&format!(
                ", \"spill_repartitions\": {}",
                s.spill_repartitions
            ));
        }
        if s.spill_max_depth > 0 {
            extra.push_str(&format!(", \"spill_max_depth\": {}", s.spill_max_depth));
        }
        if let Some(reason) = s.spill_reason {
            extra.push_str(&format!(", \"spill_mode\": \"{reason}\""));
            extra.push_str(&format!(", \"spill_reason\": \"{reason}\""));
        }
        if s.spill_input_rows > 0 {
            extra.push_str(&format!(", \"spill_input_rows\": {}", s.spill_input_rows));
        }
        if s.spill_partial_rows > 0 {
            extra.push_str(&format!(
                ", \"spill_partial_rows\": {}",
                s.spill_partial_rows
            ));
        }
        if s.partitions > 0 {
            extra.push_str(&format!(", \"partitions\": {}", s.partitions));
        }
        if s.index_entries_examined > 0 || s.heap_rows_fetched > 0 || s.heap_page_probes > 0 {
            extra.push_str(&format!(
                ", \"index_entries_examined\": {}, \"heap_rows_fetched\": {}, \"heap_page_probes\": {}",
                s.index_entries_examined, s.heap_rows_fetched, s.heap_page_probes
            ));
        }
        if s.index_merge_branches > 0 {
            extra.push_str(&format!(
                ", \"index_merge_branches\": {}, \"candidate_rowids\": {}, \"duplicates_removed\": {}",
                s.index_merge_branches,
                s.index_merge_candidate_rowids,
                s.index_merge_duplicates_removed
            ));
        }
        if s.runtime_filter_rejected > 0 {
            extra.push_str(&format!(
                ", \"runtime_filter_rejected\": {}",
                s.runtime_filter_rejected
            ));
        }
        if s.build_rows > 0 || s.probe_rows > 0 || s.match_rows > 0 {
            extra.push_str(&format!(
                ", \"build_rows\": {}, \"probe_rows\": {}, \"match_rows\": {}",
                s.build_rows, s.probe_rows, s.match_rows
            ));
        }
        if s.zone_candidate_ns > 0 || s.zone_tasks > 0 || s.zone_fallbacks > 0 {
            extra.push_str(&format!(
                ", \"zone_metadata_ms\": {:.3}, \"zone_candidate_ms\": {:.3}, \"zone_heap_ms\": {:.3}, \"zone_tasks\": {}, \"zone_pages\": {}, \"zone_skipped\": {}, \"zone_fallbacks\": {}, \"zone_workers\": {}",
                s.zone_metadata_ns as f64 / 1_000_000.0,
                s.zone_candidate_ns as f64 / 1_000_000.0,
                s.zone_heap_scan_ns as f64 / 1_000_000.0,
                s.zone_tasks,
                s.zone_pages_scanned,
                s.zone_pages_skipped,
                s.zone_fallbacks,
                s.zone_workers,
            ));
        }
        if s.is_streaming {
            extra.push_str(", \"streaming\": true");
        }
        if s.is_vectorized {
            let scalar_rows = s
                .fallback_expression_rows
                .saturating_add(s.fallback_aggregate_rows);
            extra.push_str(&format!(
                ", \"execution_mode\": \"vector\", \"batches\": {}, \"conversion_rows\": {}, \"scalar_rows\": {}, \"workers\": {}",
                s.batches, s.conversion_rows, scalar_rows, s.workers
            ));
        }
        if let Some(mode) = s.expression_mode {
            extra.push_str(&format!(
                ", \"expression_mode\": \"{}\", \"native_expression_rows\": {}, \"fallback_expression_rows\": {}",
                mode, s.native_expression_rows, s.fallback_expression_rows
            ));
        }
        if let Some(mode) = s.aggregate_mode {
            extra.push_str(&format!(
                ", \"aggregate_mode\": \"{}\", \"native_aggregate_rows\": {}, \"fallback_aggregate_rows\": {}, \"distinct_spill_bytes\": {}",
                mode, s.native_aggregate_rows, s.fallback_aggregate_rows, s.distinct_spill_bytes
            ));
        }
        if let Some(mode) = s.window_mode {
            extra.push_str(&format!(
                ", \"window_mode\": \"{}\", \"window_spill_bytes\": {}, \"window_peak_partition_rows\": {}",
                mode, s.window_spill_bytes, s.window_peak_partition_rows
            ));
        }
        out.push_str(&format!(
            ",\n{indent}  \"actual\": {{ \"executed\": true, \"loops\": {loops_count}, \"rows\": {rows_per_loop:.3}, \"rows_total\": {rows_total}, \"first_row_ms\": {first_row_ms:.3}, \"last_row_ms\": {last_row_ms:.3}, \"active_time_total_ms\": {active_time_total_ms:.3}, \"self_time_total_ms\": {self_time_total_ms:.3}, \"time_scope\": \"inclusive_active\", \"stats_source\": \"{source}\", \"physical_reads\": {physical_reads}, \"cache_reads\": {cache_reads}, \"cache_hit_rate\": {hit_rate:.2}, \"io_scope\": \"{io_scope}\"{extra} }}\n",
            loops_count = s.loops,
            rows_total = s.rows,
            source = s.source.as_str(),
            physical_reads = s.physical_reads,
            cache_reads = s.cache_reads,
        ));
    } else {
        out.push('\n');
    }
    out.push_str(&format!("{indent}}}\n"));
}

fn cte_scan_stats(
    stats: Option<&NodeStatsMap>,
    scan_plan: &planner::PhysicalPlan,
    name: &str,
    cte_plans: Option<&std::collections::HashMap<String, planner::PhysicalPlan>>,
    cte_extras: Option<&CteExtraBranches>,
) -> Option<NodeStats> {
    let map = stats?;
    let _ = (name, cte_plans, cte_extras);
    analyze_stats::lookup(map, scan_plan)
}

fn display_stats(
    stats: &NodeStatsMap,
    plan: &planner::PhysicalPlan,
    cte_plans: Option<&std::collections::HashMap<String, planner::PhysicalPlan>>,
    cte_extras: Option<&CteExtraBranches>,
) -> Option<NodeStats> {
    let _ = (cte_plans, cte_extras);
    analyze_stats::lookup(stats, plan)
}

/// D1 (explain-mysql80-compat): emit a MySQL 8.0-shaped `cost_info` object for
/// a table/operator node. mydb's cost model gives `total = io + cpu`, so
/// `eval_cost` is the CPU/evaluation portion (`cpu_units`), `read_cost` is the
/// I/O remainder, `prefix_cost` is the node's cumulative cost (== `total`), and
/// `data_read_per_join` estimates the bytes produced (`width * rows`). Values
/// are in mydb cost units (its cost model differs from InnoDB's); the field
/// *shape* is what tools consuming `FORMAT=JSON` rely on. Emitted WITHOUT a
/// trailing newline so `json_close_node` can slot `actual` after it.
fn json_cost_info(out: &mut String, indent: &str, plan: &planner::PhysicalPlan, total: f64) {
    let total = total.max(0.0);
    let components = planner::plan_cost(plan);
    let local = planner::plan_local_cost(plan);
    let estimate = planner::plan_rel_estimate(plan);
    let properties = planner::plan_physical_properties(plan);
    let eval = components.cpu_units.max(0.0).min(total);
    let read = (total - eval).max(0.0);
    // Cardinality and width come from the final physical node, not the legacy
    // mirrors inside Cost: pruning/filter rewrites may legitimately change
    // the former before all old callers have migrated.
    let data = (plan.output_width() * planner::estimated_rows(plan)).max(0.0);
    out.push_str(&format!(
        "{indent}  \"cost_info\": {{ \"read_cost\": \"{read:.2}\", \
         \"eval_cost\": \"{eval:.2}\", \"prefix_cost\": \"{total:.2}\", \
         \"data_read_per_join\": \"{data:.0}\", \
         \"mydb_startup_cost\": \"{:.2}\", \
         \"mydb_local_total_cost\": \"{:.2}\", \"mydb_local_cpu_units\": \"{:.2}\", \
         \"mydb_local_seq_pages\": \"{:.2}\", \"mydb_local_random_pages\": \"{:.2}\", \
         \"mydb_cumulative_seq_pages\": \"{:.2}\", \"mydb_cumulative_random_pages\": \"{:.2}\", \
         \"mydb_cumulative_cpu_units\": \"{:.2}\", \
         \"mydb_peak_memory_bytes\": \"{:.0}\", \"mydb_spill_bytes\": \"{:.0}\", \
         \"mydb_estimate_source\": \"{}\", \"mydb_estimate_confidence\": \"{:.3}\", \
         \"mydb_execution_mode\": \"{:?}\", \"mydb_parallel_degree\": {}, \
         \"mydb_output_ordering\": {:?}, \"mydb_cost_valid\": {}, \
         \"mydb_cbo_model\": \"{}\", \"mydb_cbo_epoch\": {} }}",
        components.startup,
        local.total,
        local.cpu_units,
        local.io_seq_pages,
        local.io_rnd_pages,
        components.io_seq_pages,
        components.io_rnd_pages,
        components.cpu_units,
        components.mem_bytes,
        components.spill_bytes,
        estimate.source.as_str(),
        estimate.confidence,
        properties.execution_mode,
        properties.parallel_degree,
        properties.ordering,
        components.is_valid(),
        crate::engine::cbo::cbo_model(),
        crate::engine::cbo::cbo_epoch(),
    ));
}

/// Shared scan estimates; node-specific access and diagnostics stay with the
/// node renderer so all existing JSON field ordering is retained.
fn scan_filtered_percent(access_rows: f64, estimated_rows: f64) -> f64 {
    if access_rows > 0.0 {
        (estimated_rows / access_rows * 100.0).clamp(0.01, 100.0)
    } else {
        100.0
    }
}

fn json_scan_estimates(
    out: &mut String,
    indent: &str,
    table: &planner::PlanTable,
    filter: &Option<Expr>,
    access_rows: f64,
    estimated_rows: f64,
) {
    let filtered = scan_filtered_percent(access_rows, estimated_rows);
    json_scan_rows(out, indent, access_rows, estimated_rows);
    out.push_str(&format!("{indent}  \"filtered\": \"{filtered:.2}\",\n"));
    json_rows_estimation_source(out, indent, table, filter);
}

fn json_scan_rows(out: &mut String, indent: &str, access_rows: f64, estimated_rows: f64) {
    out.push_str(&format!(
        "{indent}  \"rows_examined_per_scan\": {rows},\n",
        rows = access_rows.ceil() as u64
    ));
    out.push_str(&format!(
        "{indent}  \"rows_produced_per_join\": {rows},\n",
        rows = estimated_rows.ceil() as u64
    ));
}

fn json_attached_condition(out: &mut String, indent: &str, filter: Option<&Expr>) {
    if let Some(filter) = filter {
        json_string_field(out, indent, "attached_condition", &explain_expr(filter));
    }
}

fn format_plan_json_node(
    plan: &planner::PhysicalPlan,
    out: &mut String,
    depth: usize,
    stats: Option<&NodeStatsMap>,
    cte_plans: Option<&std::collections::HashMap<String, planner::PhysicalPlan>>,
    cte_extras: Option<&CteExtraBranches>,
) {
    let estimated_rows = &planner::estimated_rows(plan);
    let cost = &planner::plan_cost(plan).total;
    let indent = "  ".repeat(depth);
    let append_child =
        |out: &mut String,
         field: &str,
         child: &planner::PhysicalPlan,
         child_ctes: Option<&std::collections::HashMap<String, planner::PhysicalPlan>>,
         ending: &str| {
            out.push_str(&format!("{indent}  \"{field}\": {{\n"));
            format_plan_json_node(child, out, depth + 2, stats, child_ctes, cte_extras);
            out.push_str(&format!("{indent}  }}{ending}"));
        };
    if let Some(node) = UnaryExplain::from_plan(plan) {
        let kind = node.json_kind;
        out.push_str(&format!("{indent}\"{kind}\": {{\n"));
        out.push_str(&format!(
            "{indent}  \"estimated_rows\": {},\n",
            estimated_rows.ceil() as u64
        ));
        out.push_str(&format!(
            "{indent}  \"cost_info\": {{ \"total_cost\": \"{:.2}\" }},\n",
            cost.max(0.0)
        ));
        append_child(out, "input", node.input, cte_plans, "");
        json_close_node(out, &indent, stats, plan);
        return;
    }
    match plan {
        planner::PhysicalPlan::RrfFusion {
            inputs,
            spec,
            columns,
            output_window,
            ..
        } => {
            out.push_str(&format!("{indent}\"rrf_fusion\": {{\n"));
            out.push_str(&format!(
                "{indent}  \"metadata\": {},\n",
                super::hybrid::plan_metadata(spec, columns)
            ));
            out.push_str(&format!(
                "{indent}  \"output_window\": {},\n",
                super::hybrid::window_json(output_window)
            ));
            json_string_field(
                out,
                &indent,
                "rank_constant",
                &crate::parser::expr_to_sql(&spec.rank_constant),
            );
            json_string_field(
                out,
                &indent,
                "weights",
                &spec
                    .weights
                    .iter()
                    .map(crate::parser::expr_to_sql)
                    .collect::<Vec<_>>()
                    .join(","),
            );
            out.push_str(&format!(
                "{indent}  \"rows_produced_per_join\": {},\n",
                estimated_rows.ceil() as u64
            ));
            for (i, input) in inputs.iter().enumerate() {
                append_child(
                    out,
                    &format!("branch_{}", i + 1),
                    input,
                    cte_plans,
                    if i + 1 < inputs.len() { ",\n" } else { "" },
                );
            }
            json_close_node(out, &indent, stats, plan);
        }
        planner::PhysicalPlan::SetOp {
            left,
            right,
            op,
            all,
            ..
        } => {
            out.push_str(&format!("{indent}\"set_operation\": {{\n"));
            json_string_field(out, &indent, "operation", &format!("{op:?}"));
            out.push_str(&format!("{indent}  \"all\": {all},\n"));
            out.push_str(&format!(
                "{indent}  \"rows_produced_per_join\": {},\n",
                estimated_rows.ceil() as u64
            ));
            out.push_str(&format!(
                "{indent}  \"cost_info\": {{ \"total_cost\": \"{cost:.2}\" }},\n"
            ));
            append_child(out, "left", left, cte_plans, ",\n");
            append_child(out, "right", right, cte_plans, "");
            json_close_node(out, &indent, stats, plan);
        }
        planner::PhysicalPlan::Materialize {
            input, reuse_key, ..
        } => {
            out.push_str(&format!("{indent}\"materialize\": {{\n"));
            json_string_field(out, &indent, "reuse_key", reuse_key);
            out.push_str(&format!(
                "{indent}  \"rows_produced_per_join\": {},\n",
                estimated_rows.ceil() as u64
            ));
            out.push_str(&format!(
                "{indent}  \"cost_info\": {{ \"total_cost\": \"{cost:.2}\" }},\n"
            ));
            append_child(out, "input", input, cte_plans, "");
            json_close_node(out, &indent, stats, plan);
        }
        planner::PhysicalPlan::RecursiveUnion {
            anchor,
            recursive,
            name,
            union_all,
            max_depth,
            ..
        } => {
            out.push_str(&format!("{indent}\"recursive_union\": {{\n"));
            json_string_field(out, &indent, "name", name);
            out.push_str(&format!("{indent}  \"all\": {union_all},\n"));
            out.push_str(&format!("{indent}  \"max_depth\": {max_depth},\n"));
            out.push_str(&format!(
                "{indent}  \"rows_produced_per_join\": {},\n",
                estimated_rows.ceil() as u64
            ));
            out.push_str(&format!(
                "{indent}  \"cost_info\": {{ \"total_cost\": \"{cost:.2}\" }},\n"
            ));
            append_child(out, "anchor", anchor, None, ",\n");
            append_child(out, "recursive", recursive, None, "");
            json_close_node(out, &indent, stats, plan);
        }
        planner::PhysicalPlan::ValuesScan { filter, .. }
        | planner::PhysicalPlan::JsonTableScan { filter, .. }
        | planner::PhysicalPlan::SystemScan { filter, .. } => {
            let kind = match plan {
                planner::PhysicalPlan::ValuesScan { .. } => "values_scan",
                planner::PhysicalPlan::JsonTableScan { .. } => "json_table_scan",
                _ => "system_scan",
            };
            out.push_str(&format!("{indent}\"{kind}\": {{\n"));
            out.push_str(&format!(
                "{indent}  \"rows_produced_per_join\": {},\n",
                estimated_rows.ceil() as u64
            ));
            json_scan_footer(out, &indent, plan, filter.as_ref(), stats);
        }
        planner::PhysicalPlan::SeqScan {
            table,
            filter,
            access_rows,
            possible_keys,
            ..
        } => {
            let name = table_display_name(table);
            json_table_header(out, &indent, "table", &name, table, filter.as_ref(), "ALL");
            json_possible_keys(out, possible_keys, &indent);
            json_scan_estimates(out, &indent, table, filter, *access_rows, *estimated_rows);
            json_scan_footer(out, &indent, plan, filter.as_ref(), stats);
        }
        planner::PhysicalPlan::ZoneMapScan {
            table,
            filter,
            access_rows,
            possible_keys,
            zone_map,
            ..
        } => {
            let name = table_display_name(table);
            let filtered = scan_filtered_percent(*access_rows, *estimated_rows);
            json_table_header(
                out,
                &indent,
                "zone_map",
                &name,
                table,
                filter.as_ref(),
                "zone",
            );
            json_possible_keys(out, possible_keys, &indent);
            json_scan_rows(out, &indent, *access_rows, *estimated_rows);
            out.push_str(&format!("{indent}  \"filtered\": \"{filtered:.2}\",\n"));
            out.push_str(&format!(
                "{indent}  \"zone_pages\": {},\n{indent}  \"zone_total_pages\": {},\n{indent}  \"zone_ranges\": {},\n{indent}  \"zone_metadata_bytes\": {},\n{indent}  \"zone_cache_misses\": {},\n{indent}  \"zone_storage_objects\": {},\n{indent}  \"zone_estimate\": \"{}\",\n",
                zone_map.surviving_pages,
                zone_map.total_pages,
                zone_map.ranges,
                zone_map.metadata_read_bytes,
                zone_map.metadata_cache_misses,
                zone_map.storage_objects,
                if zone_map.estimate_exact { "exact" } else { "sampled" }
            ));
            json_rows_estimation_source(out, &indent, table, filter);
            json_scan_footer(out, &indent, plan, filter.as_ref(), stats);
        }
        planner::PhysicalPlan::FulltextScan {
            table,
            index,
            filter,
            access_rows,
            possible_keys,
            additional_matches,
            like_prefilter,
            ..
        } => {
            let name = table_display_name(table);
            json_table_header(
                out,
                &indent,
                "table",
                &name,
                table,
                filter.as_ref(),
                "fulltext",
            );
            out.push_str(&format!("{indent}  \"key\": \"{}\",\n", index.name));
            if let Some(prefilter) = like_prefilter {
                out.push_str(&format!(
                    "{indent}  \"like_shape\": \"{}\",\n{indent}  \"ngram_token_count\": {},\n{indent}  \"literal_segment_count\": {},\n",
                    if prefilter.pattern_param.is_some() { "adaptive" } else { "unanchored" },
                    prefilter.tokens.len(), prefilter.literal_segments
                ));
            } else {
                out.push_str(&format!(
                    "{indent}  \"match_predicate_count\": {},\n",
                    additional_matches.len() + 1
                ));
            }
            json_possible_keys(out, possible_keys, &indent);
            json_scan_estimates(out, &indent, table, filter, *access_rows, *estimated_rows);
            json_scan_footer(out, &indent, plan, filter.as_ref(), stats);
        }
        planner::PhysicalPlan::SpatialScan {
            table,
            index,
            filter,
            access_rows,
            possible_keys,
            ..
        } => {
            let name = table_display_name(table);
            json_table_header(
                out,
                &indent,
                "table",
                &name,
                table,
                filter.as_ref(),
                "range",
            );
            out.push_str(&format!("{indent}  \"key\": \"{}\",\n", index.name));
            json_possible_keys(out, possible_keys, &indent);
            json_scan_estimates(out, &indent, table, filter, *access_rows, *estimated_rows);
            json_scan_footer(out, &indent, plan, filter.as_ref(), stats);
        }
        planner::PhysicalPlan::VectorIndexScan {
            table,
            index,
            filter,
            access_rows,
            possible_keys,
            metric,
            k,
            ef_search,
            ..
        } => {
            let name = table_display_name(table);
            json_table_header(out, &indent, "table", &name, table, filter.as_ref(), "ref");
            out.push_str(&format!("{indent}  \"key\": \"{}\",\n", index.name));
            json_possible_keys(out, possible_keys, &indent);
            json_scan_estimates(out, &indent, table, filter, *access_rows, *estimated_rows);
            json_attached_condition(out, &indent, filter.as_ref());
            let (algo, effort_key) = vector_access_labels(index);
            let quant = index
                .vector
                .as_ref()
                .map_or("none", |v| v.quantization.name());
            let rerank =
                crate::engine::vector::session::explained_rerank_pool(index, *k, *ef_search);
            out.push_str(&format!(
                "{indent}  \"vector_scan\": {{\"algo\": \"{algo}\", \"metric\": \"{metric}\", \"k\": {k}, \"{effort_key}\": {ef_search}, \"quantization\": \"{quant}\", \"rerank_pool\": {rerank}}},\n"
            ));
            json_cost_info(out, &indent, plan, *cost);
            json_close_node(out, &indent, stats, plan);
        }
        planner::PhysicalPlan::VectorBruteScan {
            table,
            access_rows,
            possible_keys,
            metric,
            k,
            ..
        } => {
            let name = table_display_name(table);
            json_table_header(out, &indent, "table", &name, table, None, "ALL");
            json_possible_keys(out, possible_keys, &indent);
            json_scan_rows(out, &indent, *access_rows, *estimated_rows);
            json_string_field(out, &indent, "attached_condition", "brute-force vector KNN");
            out.push_str(&format!(
                "{indent}  \"vector_scan\": {{\"algo\": \"BRUTE\", \"metric\": \"{metric}\", \"k\": {k}}},\n"
            ));
            json_cost_info(out, &indent, plan, *cost);
            json_close_node(out, &indent, stats, plan);
        }
        planner::PhysicalPlan::CoveringIndexAggregate {
            table,
            index,
            low,
            high,
            access_rows,
            ..
        } => {
            let name = table_display_name(table);
            out.push_str(&format!("{indent}\"index_aggregate\": {{\n"));
            out.push_str(&format!("{indent}  \"table_name\": \"{name}\",\n"));
            out.push_str(&format!("{indent}  \"access_type\": \"index\",\n"));
            out.push_str(&format!("{indent}  \"key\": \"{}\",\n", index.name));
            json_scan_rows(out, &indent, *access_rows, *estimated_rows);
            if let Some(condition) = index_range_condition(index, low, high) {
                json_string_field(out, &indent, "index_condition", &condition);
            }
            out.push_str(&format!("{indent}  \"using_index\": true,\n"));
            out.push_str(&format!("{indent}  \"workers\": 1,\n"));
            json_cost_info(out, &indent, plan, *cost);
            json_close_node(out, &indent, stats, plan);
        }
        planner::PhysicalPlan::IndexMerge {
            table,
            root,
            filter,
            ..
        } => {
            let merge = IndexMergeInfo::new(root);
            let algorithm = merge.json_algorithm;
            out.push_str(&format!("{indent}\"index_merge\": {{\n"));
            json_string_field(out, &indent, "table_name", &table_display_name(table));
            json_string_field(out, &indent, "algorithm", algorithm);
            json_string_field(out, &indent, "key", &merge.keys);
            out.push_str(&format!(
                "{indent}  \"rows_examined_per_scan\": {},\n",
                estimated_rows.ceil() as u64
            ));
            json_scan_footer(out, &indent, plan, filter.as_ref(), stats);
        }
        planner::PhysicalPlan::IndexLookup { .. }
        | planner::PhysicalPlan::IndexMultiLookup { .. }
        | planner::PhysicalPlan::IndexRangeScan { .. }
        | planner::PhysicalPlan::IndexFastScan { .. } => {
            let access = IndexAccess::from_plan(plan).expect("matched index access");
            let name = table_display_name(access.table);
            json_table_header(
                out,
                &indent,
                "table",
                &name,
                access.table,
                access.filter.as_ref(),
                access.access_type(),
            );
            json_possible_keys(out, access.possible_keys, &indent);
            out.push_str(&format!("{indent}  \"key\": \"{}\",\n", access.index.name));
            out.push_str(&format!(
                "{indent}  \"key_length\": \"{}\",\n",
                access.key_length()
            ));
            // RANGE puts its MRR fields before the estimation source. Keep
            // the existing field order as well as the values byte-for-byte.
            if matches!(access.kind, IndexAccessKind::Range { .. }) {
                json_scan_rows(out, &indent, access.access_rows, *estimated_rows);
                let filtered = scan_filtered_percent(access.access_rows, *estimated_rows);
                out.push_str(&format!("{indent}  \"filtered\": \"{filtered:.2}\",\n"));
                if access.uses_mrr() {
                    out.push_str(&format!("{indent}  \"heap_fetch\": \"mrr\",\n"));
                    out.push_str(&format!(
                        "{indent}  \"planning_mrr_window_rows\": {},\n",
                        crate::engine::cbo::PLANNING_MRR_WINDOW_ROWS as u64
                    ));
                }
                json_rows_estimation_source(out, &indent, access.table, access.filter);
            } else {
                json_scan_estimates(
                    out,
                    &indent,
                    access.table,
                    access.filter,
                    access.access_rows,
                    *estimated_rows,
                );
                if matches!(access.kind, IndexAccessKind::Fast { .. }) {
                    out.push_str(&format!("{indent}  \"using_index\": true,\n"));
                }
            }
            if let Some(condition) = access.condition() {
                json_string_field(out, &indent, "index_condition", &condition);
            }
            if let Some(details) = access.like_details() {
                for (name, value) in details {
                    json_string_field(out, &indent, &name.replace(' ', "_"), &value);
                }
            }
            json_scan_footer(out, &indent, plan, access.filter.as_ref(), stats);
        }
        planner::PhysicalPlan::HashJoin {
            left,
            right,
            join_type,
            left_keys,
            right_keys,
            null_safe,
            build_side,
            residual,
            ..
        } => {
            out.push_str(&format!("{indent}\"hash_join\": {{\n"));
            out.push_str(&format!(
                "{indent}  \"join_type\": \"{}\",\n",
                join_type_json(*join_type)
            ));
            let side = match build_side {
                planner::BuildSide::Left => "left",
                planner::BuildSide::Right => "right",
            };
            out.push_str(&format!("{indent}  \"build_side\": \"{side}\",\n"));
            out.push_str(&format!("{indent}  \"join_keys\": ["));
            for (idx, ((left_key, right_key), null_safe)) in left_keys
                .iter()
                .zip(right_keys.iter())
                .zip(null_safe.iter())
                .enumerate()
            {
                if idx > 0 {
                    out.push_str(", ");
                }
                let op = if *null_safe { "<=>" } else { "=" };
                out.push_str(&format!(
                    "{{ \"left\": {}, \"right\": {}, \"operator\": {} }}",
                    json_string(&explain_expr(left_key)),
                    json_string(&explain_expr(right_key)),
                    json_string(op)
                ));
            }
            out.push_str("],\n");
            let key_collations =
                super::plan::hash_join_key_collation_names(left, left_keys, right, right_keys);
            if !key_collations.is_empty() {
                out.push_str(&format!("{indent}  \"key_collations\": ["));
                for (idx, collation) in key_collations.iter().enumerate() {
                    if idx > 0 {
                        out.push_str(", ");
                    }
                    out.push_str(&json_string(collation));
                }
                out.push_str("],\n");
            }
            if let Some(residual) = residual {
                json_string_field(out, &indent, "attached_condition", &explain_expr(residual));
            }
            out.push_str(&format!(
                "{indent}  \"partition_wise\": {},\n",
                super::plan::partition_wise_hash_join_leaf_pairs(
                    left, right, left_keys, right_keys,
                )
                .is_some()
            ));
            out.push_str(&format!(
                "{indent}  \"estimated_rows\": {rows},\n",
                rows = estimated_rows.ceil() as u64
            ));
            out.push_str(&format!("{indent}  \"cost\": \"{cost:.2}\",\n"));
            out.push_str(&format!("{indent}  \"build\": {{\n"));
            let (build, probe) = match build_side {
                planner::BuildSide::Left => (left.as_ref(), right.as_ref()),
                planner::BuildSide::Right => (right.as_ref(), left.as_ref()),
            };
            format_plan_json_node(build, out, depth + 2, stats, cte_plans, cte_extras);
            out.push_str(&format!("{indent}  }},\n"));
            append_child(out, "probe", probe, cte_plans, "");
            json_close_node(out, &indent, stats, plan);
        }
        planner::PhysicalPlan::AdaptiveJoin {
            left,
            right,
            join_type,
            candidates,
            preferred,
            ..
        } => {
            out.push_str(&format!("{indent}\"adaptive_join\": {{\n"));
            out.push_str(&format!(
                "{indent}  \"join_type\": \"{}\",\n",
                join_type_json(*join_type)
            ));
            out.push_str(&format!(
                "{indent}  \"preferred\": \"{}\",\n",
                planner::adaptive_choice_name(*preferred)
            ));
            out.push_str(&format!("{indent}  \"candidates\": ["));
            for (idx, candidate) in candidates.iter().enumerate() {
                if idx > 0 {
                    out.push_str(", ");
                }
                out.push_str(&json_string(planner::adaptive_choice_name(
                    planner::adaptive_candidate_choice(candidate),
                )));
            }
            out.push_str("],\n");
            out.push_str(&format!(
                "{indent}  \"estimated_rows\": {rows},\n",
                rows = estimated_rows.ceil() as u64
            ));
            out.push_str(&format!("{indent}  \"cost\": \"{cost:.2}\",\n"));
            append_child(out, "left", left, cte_plans, ",\n");
            append_child(out, "right", right, cte_plans, "");
            json_close_node(out, &indent, stats, plan);
        }
        planner::PhysicalPlan::NestedLoopJoin {
            left,
            right,
            join_type,
            condition,
            ..
        } => {
            out.push_str(&format!("{indent}\"nested_loop\": {{\n"));
            out.push_str(&format!(
                "{indent}  \"join_type\": \"{}\",\n",
                join_type_json(*join_type)
            ));
            if let Some(condition) = condition {
                json_string_field(out, &indent, "join_condition", &explain_expr(condition));
            }
            out.push_str(&format!(
                "{indent}  \"estimated_rows\": {rows},\n",
                rows = estimated_rows.ceil() as u64
            ));
            out.push_str(&format!("{indent}  \"cost\": \"{cost:.2}\",\n"));
            append_child(out, "outer", left, cte_plans, ",\n");
            append_child(out, "inner", right, cte_plans, "");
            json_close_node(out, &indent, stats, plan);
        }
        planner::PhysicalPlan::IndexNestedLoopJoin {
            left,
            right_table,
            right_index,
            join_type,
            condition,
            right_covering,
            right_projected_columns,
            ..
        } => {
            let name = table_display_name(right_table);
            let (requested_degree, outer_threshold, probe_task_rows) =
                super::plan::inlj_parallel_policy_for_explain();
            out.push_str(&format!("{indent}\"nested_loop\": {{\n"));
            out.push_str(&format!(
                "{indent}  \"join_type\": \"{}\",\n",
                join_type_json(*join_type)
            ));
            if let Some(condition) = condition {
                json_string_field(out, &indent, "join_condition", &explain_expr(condition));
            }
            out.push_str(&format!(
                "{indent}  \"batch\": \"{}\",\n",
                super::plan::index_nested_loop_batch_eligibility(
                    condition.as_ref(),
                    right_table,
                    right_index,
                )
            ));
            out.push_str(&format!("{indent}  \"covering\": {},\n", right_covering));
            out.push_str(&format!(
                "{indent}  \"parallel_frontier\": \"runtime_actual_work\",\n"
            ));
            out.push_str(&format!(
                "{indent}  \"requested_degree\": {requested_degree},\n"
            ));
            out.push_str(&format!(
                "{indent}  \"outer_threshold\": {outer_threshold},\n"
            ));
            out.push_str(&format!(
                "{indent}  \"probe_task_rows\": {probe_task_rows},\n"
            ));
            out.push_str(&format!(
                "{indent}  \"parallel_statistics\": \"not_used\",\n"
            ));
            out.push_str(&format!(
                "{indent}  \"right_projection\": \"{}\",\n",
                right_projected_columns
                    .as_ref()
                    .map(|cols| cols.len().to_string())
                    .unwrap_or_else(|| "all".to_string())
            ));
            out.push_str(&format!(
                "{indent}  \"estimated_rows\": {rows},\n",
                rows = estimated_rows.ceil() as u64
            ));
            out.push_str(&format!("{indent}  \"cost\": \"{cost:.2}\",\n"));
            append_child(out, "outer", left, cte_plans, ",\n");
            out.push_str(&format!("{indent}  \"table\": {{\n"));
            out.push_str(&format!("{indent}    \"table_name\": \"{name}\",\n"));
            out.push_str(&format!("{indent}    \"access_type\": \"ref\",\n"));
            out.push_str(&format!(
                "{indent}    \"key\": \"{key}\",\n",
                key = right_index.name
            ));
            out.push_str(&format!(
                "{indent}    \"actual\": {{ \"executed\": false, \"stats_status\": \"included\", \"included_in_operator\": {} }}\n",
                stats
                    .and_then(|map| map.id_for_plan(plan))
                    .map(|id| id.0.to_string())
                    .unwrap_or_else(|| "null".to_string())
            ));
            out.push_str(&format!("{indent}  }}"));
            json_close_node(out, &indent, stats, plan);
        }

        planner::PhysicalPlan::NullExtend { .. }
        | planner::PhysicalPlan::Sort { .. }
        | planner::PhysicalPlan::IncrementalSort { .. }
        | planner::PhysicalPlan::TopK { .. }
        | planner::PhysicalPlan::TopWindow { .. }
        | planner::PhysicalPlan::ParallelTopK { .. }
        | planner::PhysicalPlan::HashAggregate { .. }
        | planner::PhysicalPlan::HashRollupAggregate { .. }
        | planner::PhysicalPlan::HashGroupingSetsAggregate { .. }
        | planner::PhysicalPlan::Limit { .. }
        | planner::PhysicalPlan::Projection { .. }
        | planner::PhysicalPlan::Filter { .. }
        | planner::PhysicalPlan::Apply { .. }
        | planner::PhysicalPlan::WindowAgg { .. }
        | planner::PhysicalPlan::StreamingAggregate { .. }
        | planner::PhysicalPlan::StreamingRollupAggregate { .. }
        | planner::PhysicalPlan::StreamingGroupingSetsAggregate { .. }
        | planner::PhysicalPlan::StreamingDistinct { .. }
        | planner::PhysicalPlan::ParallelHashAggregate { .. }
        | planner::PhysicalPlan::ParallelRollupAggregate { .. }
        | planner::PhysicalPlan::ParallelGroupingSetsAggregate { .. }
        | planner::PhysicalPlan::ParallelSort { .. } => {
            unreachable!("unary operators handled before rendering")
        }
        planner::PhysicalPlan::CteScan { name, alias, .. } => {
            let display = alias.as_deref().unwrap_or(name);
            let indent = "  ".repeat(depth);
            let cte_actual = cte_scan_stats(stats, plan, name, cte_plans, cte_extras);
            out.push_str(&format!("{indent}\"cte_scan\": {{\n"));
            json_string_field(out, &indent, "alias", display);
            let logical_name = if name.starts_with('\0') {
                display
            } else {
                name.as_str()
            };
            out.push_str(&format!("{indent}  \"cte\": {}", json_string(logical_name)));
            if let Some(cte_map) = cte_plans {
                let key = name.to_ascii_lowercase();
                if let Some(cte_plan) = cte_map.get(&key) {
                    let extras_for = cte_extras.and_then(|m| m.get(&key));
                    let has_extras = extras_for.map(|v| !v.is_empty()).unwrap_or(false);
                    if has_extras {
                        // UNION ALL CTE body: emit a `union_all_branches`
                        // array with one entry per branch so per-operator
                        // stats are visible for every side.
                        out.push_str(&format!(",\n{indent}  \"union_all_branches\": [\n"));
                        out.push_str(&format!("{indent}    {{\n"));
                        format_plan_json_node(
                            cte_plan,
                            out,
                            depth + 3,
                            stats,
                            cte_plans,
                            cte_extras,
                        );
                        out.push_str(&format!("{indent}    }}"));
                        for extra in extras_for.unwrap() {
                            out.push_str(",\n");
                            out.push_str(&format!("{indent}    {{\n"));
                            format_plan_json_node(
                                extra,
                                out,
                                depth + 3,
                                stats,
                                cte_plans,
                                cte_extras,
                            );
                            out.push_str(&format!("{indent}    }}"));
                        }
                        out.push_str(&format!("\n{indent}  ]"));
                    } else {
                        out.push_str(&format!(",\n{indent}  \"materialized_plan\": {{\n"));
                        format_plan_json_node(
                            cte_plan,
                            out,
                            depth + 2,
                            stats,
                            cte_plans,
                            cte_extras,
                        );
                        out.push_str(&format!("{indent}  }}"));
                    }
                }
            }
            json_close_node_with_actual(out, &indent, cte_actual);
        }
        planner::PhysicalPlan::GenerateSeries { relation, .. } => {
            let indent = "  ".repeat(depth);
            out.push_str(&format!("{indent}\"function_scan\": {{\n"));
            out.push_str(&format!("{indent}  \"function\": \"generate_series\",\n"));
            out.push_str(&format!("{indent}  \"alias\": \"{relation}\""));
            json_close_node(out, &indent, stats, plan);
        }
        planner::PhysicalPlan::IndexInLookup {
            table,
            index,
            key_prefix,
            in_column,
            subquery_plan,
            filter,
            possible_keys,
            ..
        } => {
            let name = table_display_name(table);
            json_table_header(out, &indent, "table", &name, table, filter.as_ref(), "ref");
            json_possible_keys(out, possible_keys, &indent);
            out.push_str(&format!(
                "{indent}  \"key\": \"{key_name}\",\n",
                key_name = index.name
            ));
            out.push_str(&format!(
                "{indent}  \"rows_examined_per_scan\": {rows},\n",
                rows = estimated_rows.ceil() as u64
            ));
            let condition = index_in_lookup_condition(index, key_prefix.0.len(), in_column);
            json_string_field(out, &indent, "index_condition", &condition);
            json_attached_condition(out, &indent, filter.as_ref());
            json_cost_info(out, &indent, plan, *cost);
            out.push_str(",\n");
            append_child(out, "subquery", subquery_plan, cte_plans, "");
            json_close_node(out, &indent, stats, plan);
        }
        planner::PhysicalPlan::IndexAggregate {
            table,
            index,
            low,
            high,
            filter,
            access_rows,
            ..
        } => {
            let name = table_display_name(table);
            out.push_str(&format!("{indent}\"index_aggregate\": {{\n"));
            out.push_str(&format!("{indent}  \"table_name\": \"{name}\",\n"));
            out.push_str(&format!("{indent}  \"access_type\": \"index\",\n"));
            out.push_str(&format!(
                "{indent}  \"key\": \"{key_name}\",\n",
                key_name = index.name
            ));
            json_scan_rows(out, &indent, *access_rows, *estimated_rows);
            if let Some(condition) = index_range_condition(index, low, high) {
                json_string_field(out, &indent, "index_condition", &condition);
            }
            json_attached_condition(out, &indent, filter.as_ref());
            out.push_str(&format!(
                "{indent}  \"using_index\": {},\n",
                filter.is_none()
            ));
            out.push_str(&format!("{indent}  \"workers\": 1,\n"));
            json_cost_info(out, &indent, plan, *cost);
            json_close_node(out, &indent, stats, plan);
        }
        planner::PhysicalPlan::ParallelSeqScan {
            table,
            filter,
            possible_keys,
            degree,
            ..
        } => {
            let name = table_display_name(table);
            json_table_header(
                out,
                &indent,
                "parallel_table",
                &name,
                table,
                filter.as_ref(),
                "ALL",
            );
            out.push_str(&format!("{indent}  \"workers\": {degree},\n"));
            json_possible_keys(out, possible_keys, &indent);
            out.push_str(&format!(
                "{indent}  \"rows_examined_per_scan\": {rows},\n",
                rows = estimated_rows.ceil() as u64
            ));
            json_scan_footer(out, &indent, plan, filter.as_ref(), stats);
        }
        planner::PhysicalPlan::ParallelZoneMapScan {
            table,
            filter,
            possible_keys,
            degree,
            zone_map,
            ..
        } => {
            let name = table_display_name(table);
            json_table_header(
                out,
                &indent,
                "parallel_zone_map",
                &name,
                table,
                filter.as_ref(),
                "zone",
            );
            out.push_str(&format!("{indent}  \"workers\": {degree},\n"));
            json_possible_keys(out, possible_keys, &indent);
            out.push_str(&format!(
                "{indent}  \"rows_examined_per_scan\": {rows},\n",
                rows = estimated_rows.ceil() as u64
            ));
            out.push_str(&format!(
                "{indent}  \"zone_pages\": {},\n{indent}  \"zone_total_pages\": {},\n{indent}  \"zone_ranges\": {},\n{indent}  \"zone_metadata_bytes\": {},\n{indent}  \"zone_cache_misses\": {},\n{indent}  \"zone_storage_objects\": {},\n{indent}  \"zone_estimate\": \"{}\",\n",
                zone_map.surviving_pages,
                zone_map.total_pages,
                zone_map.ranges,
                zone_map.metadata_read_bytes,
                zone_map.metadata_cache_misses,
                zone_map.storage_objects,
                if zone_map.estimate_exact { "exact" } else { "sampled" }
            ));
            json_scan_footer(out, &indent, plan, filter.as_ref(), stats);
        }
        planner::PhysicalPlan::ParallelIndexFastScan {
            table,
            index,
            low,
            high,
            filter,
            access_rows,
            possible_keys,
            degree,
            ..
        } => {
            let name = table_display_name(table);
            let prefix_len = crate::planner::explain_access::range_prefix_len(index, low, high);
            let kl = table.schema.index_key_len(index, prefix_len);
            json_table_header(
                out,
                &indent,
                "parallel_index",
                &name,
                table,
                filter.as_ref(),
                "index",
            );
            out.push_str(&format!("{indent}  \"workers\": {degree},\n"));
            json_possible_keys(out, possible_keys, &indent);
            out.push_str(&format!(
                "{indent}  \"key\": \"{key_name}\",\n",
                key_name = index.name
            ));
            out.push_str(&format!("{indent}  \"key_length\": \"{kl}\",\n"));
            json_scan_estimates(out, &indent, table, filter, *access_rows, *estimated_rows);
            out.push_str(&format!("{indent}  \"using_index\": true,\n"));
            if let Some(condition) = index_range_condition(index, low, high) {
                json_string_field(out, &indent, "index_condition", &condition);
            }
            json_scan_footer(out, &indent, plan, filter.as_ref(), stats);
        }
        planner::PhysicalPlan::ParallelIndexRangeScan {
            table,
            index,
            low,
            high,
            filter,
            access_rows,
            possible_keys,
            degree,
            ..
        } => {
            let name = table_display_name(table);
            let prefix_len = crate::planner::explain_access::range_prefix_len(index, low, high);
            let kl = table.schema.index_key_len(index, prefix_len);
            json_table_header(
                out,
                &indent,
                "parallel_range",
                &name,
                table,
                filter.as_ref(),
                "range",
            );
            out.push_str(&format!("{indent}  \"workers\": {degree},\n"));
            json_possible_keys(out, possible_keys, &indent);
            out.push_str(&format!(
                "{indent}  \"key\": \"{key_name}\",\n",
                key_name = index.name
            ));
            out.push_str(&format!("{indent}  \"key_length\": \"{kl}\",\n"));
            json_scan_estimates(out, &indent, table, filter, *access_rows, *estimated_rows);
            if let Some(condition) = index_range_condition(index, low, high) {
                json_string_field(out, &indent, "index_condition", &condition);
            }
            json_scan_footer(out, &indent, plan, filter.as_ref(), stats);
        }
        planner::PhysicalPlan::ParallelIndexAggregate {
            table,
            index,
            low,
            high,
            access_rows,
            degree,
            ..
        } => {
            let name = table_display_name(table);
            out.push_str(&format!("{indent}\"parallel_index_aggregate\": {{\n"));
            out.push_str(&format!("{indent}  \"table_name\": \"{name}\",\n"));
            out.push_str(&format!("{indent}  \"access_type\": \"index\",\n"));
            out.push_str(&format!("{indent}  \"workers\": {degree},\n"));
            out.push_str(&format!("{indent}  \"key\": \"{}\",\n", index.name));
            json_scan_rows(out, &indent, *access_rows, *estimated_rows);
            if let Some(condition) = index_range_condition(index, low, high) {
                json_string_field(out, &indent, "index_condition", &condition);
            }
            out.push_str(&format!("{indent}  \"using_index\": true,\n"));
            json_cost_info(out, &indent, plan, *cost);
            json_close_node(out, &indent, stats, plan);
        }
        planner::PhysicalPlan::Exchange {
            input,
            degree,
            output_order,
            ..
        } => {
            let kind = match output_order {
                planner::ExchangeOrder::Unordered => "exchange",
                planner::ExchangeOrder::Sorted => "exchange_merge",
            };
            out.push_str(&format!("{indent}\"{kind}\": {{\n"));
            out.push_str(&format!("{indent}  \"workers\": {degree},\n"));
            out.push_str(&format!(
                "{indent}  \"cost_info\": {{ \"total_cost\": \"{cost:.2}\" }},\n"
            ));
            append_child(out, "input", input, cte_plans, "");
            json_close_node(out, &indent, stats, plan);
        }
    }
}

fn json_possible_keys(out: &mut String, keys: &[String], indent: &str) {
    if !keys.is_empty() {
        out.push_str(&format!(
            "{indent}  \"possible_keys\": [{}],\n",
            keys.iter()
                .map(|k| format!("\"{k}\""))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
}

fn table_display_name(table: &planner::PlanTable) -> String {
    table
        .alias
        .clone()
        .unwrap_or_else(|| table.schema.name.clone())
}

enum DmlExplainInput {
    Values(usize),
    Plan(Box<planner::PhysicalPlan>),
    PartitionCandidates(PartitionDmlExplain),
    /// The multi-table writer currently materializes full sources and enumerates nested loops.
    MultiTable(Select),
}

struct PartitionDmlExplain {
    access: &'static str,
    key: Option<String>,
    partitions: Vec<String>,
    filter: Option<String>,
    ordered: bool,
    limit: Option<usize>,
}

fn partition_dml_explain_input(
    db: &Arc<Database>,
    table_name: &str,
    selection: Option<&Expr>,
    explicit: &[String],
    order_by: &[OrderBy],
    limit: Option<&LimitValue>,
    update: bool,
    single_table: bool,
    fallback: planner::PhysicalPlan,
) -> Result<DmlExplainInput, QueryError> {
    let table = db.table(table_name)?;
    let Some(part) = table.partitioning.as_ref().filter(|_| single_table) else {
        return Ok(DmlExplainInput::Plan(Box::new(fallback)));
    };
    // A correlated/subquery predicate is materialized by execution. Keep
    // that complex input in the existing planner instead of guessing it here.
    if selection.is_some_and(super::super::expr_has_subquery) {
        return Ok(DmlExplainInput::Plan(Box::new(fallback)));
    }
    // Unlimited heap DML retains the batch storage path. It does not install
    // a predicate scope across triggers/expressions, so report all leaves
    // unless an explicit PARTITION clause restricts that path.
    let unlimited_heap = order_by.is_empty()
        && limit.is_none()
        && selection
            .and_then(|expr| super::super::indexes::plan_write_candidates(&table, expr))
            .is_none();
    let mut ids = if unlimited_heap {
        part.storage_ids()
    } else {
        super::super::partition_router::pruned_storage_ids_from_filter(&table, selection)
            .unwrap_or_else(|| part.storage_ids())
    };
    if let Some(explicit) =
        super::super::partition_clause::resolve_partition_clause(db, table_name, explicit)?
    {
        ids.retain(|id| explicit.contains(id));
    }
    let partitions = ids
        .iter()
        .filter_map(|id| {
            let (_, parent) = part.find_by_storage_id(*id)?;
            Some(
                parent
                    .subpartitions
                    .iter()
                    .find(|leaf| leaf.storage_id == *id)
                    .map(|leaf| leaf.name.clone())
                    .unwrap_or_else(|| parent.name.clone()),
            )
        })
        .collect();
    use super::super::indexes::{plan_write_candidates, EngineIndexAccess, WriteCandidateAccess};
    let ordered_index = (update && !db.has_active_write_txns())
        .then(|| super::super::dml::plan_ordered_dml_index(&table, selection, order_by))
        .flatten();
    let (access, key) = if let Some((index, ..)) = ordered_index {
        ("index", Some(index.name.clone()))
    } else {
        match selection.and_then(|expr| plan_write_candidates(&table, expr)) {
            Some(WriteCandidateAccess::Single(index, access)) => (
                match access {
                    EngineIndexAccess::Lookup { exact: true, .. } if index.unique => "const",
                    EngineIndexAccess::Lookup { .. } => "ref",
                    EngineIndexAccess::Range { .. } => "range",
                },
                Some(index.name.clone()),
            ),
            Some(WriteCandidateAccess::Multi(access)) => ("range", Some(access.index.name)),
            Some(WriteCandidateAccess::Merge(root)) => {
                let mut names = Vec::new();
                root.collect_index_names(&mut names);
                ("index_merge", Some(names.join(",")))
            }
            None => ("ALL", None),
        }
    };
    Ok(DmlExplainInput::PartitionCandidates(PartitionDmlExplain {
        access,
        key,
        partitions,
        filter: selection.map(planner::expr_to_explain_string),
        ordered: !order_by.is_empty(),
        limit: limit.and_then(LimitValue::as_usize),
    }))
}

struct DmlExplainPlan {
    operation: &'static str,
    target_table: String,
    target_names: std::collections::HashSet<String>,
    partitions: Option<String>,
    input: DmlExplainInput,
    extra: String,
    hint_warnings: Vec<String>,
}

struct DmlAnalyzeStats {
    affected_rows: u64,
    matched_rows: Option<u64>,
    candidate_rows: u64,
    elapsed_ms: f64,
    io: NodeIoStats,
    trace: NodeStatsMap,
    input_plan: Option<Box<planner::PhysicalPlan>>,
}

fn explain_dml(
    db: &Arc<Database>,
    session: &mut SessionContext,
    target: Statement,
    format: ExplainFormat,
    analyze: bool,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    // Planning and all catalog/partition validation happen before ANALYZE
    // starts the mutation. Rendering below is deliberately infallible, so a
    // successfully committed DML can never be followed by a formatting error.
    let plan = DmlExplainPlan::prepare(db, session, &target)?;
    let stats = if analyze {
        let rows_start = crate::engine::perf_events::rows_examined();
        crate::engine::api::begin_statement_dml_candidate_count();
        let started = std::time::Instant::now();
        let (mut io_stats, response) = analyze_stats::with_collector(|| {
            super::super::dispatch::execute_qualified_statement(db, session, target, registry)
        });
        let response = response?;
        let affected_rows = response_affected_rows(response)?;
        let matched_rows = crate::engine::api::statement_matched_rows();
        let candidate_rows = plan.analyzed_candidate_rows(
            crate::engine::api::take_statement_dml_candidate_rows().unwrap_or_else(|| {
                crate::engine::perf_events::rows_examined().saturating_sub(rows_start)
            }),
        );
        let elapsed_ms = started.elapsed().as_nanos() as f64 / 1_000_000.0;
        let input_plan = io_stats.take_executed_plan();
        if let Some(input) = input_plan.as_deref() {
            complete_fused_trace(
                input,
                &mut io_stats,
                Some(NodeStats {
                    first_row_ns: (elapsed_ms * 1_000_000.0).round() as u128,
                    time_ns: (elapsed_ms * 1_000_000.0).round() as u128,
                    rows: candidate_rows,
                    loops: 1,
                    ..NodeStats::default()
                }),
            );
        }
        let io = io_stats.query_io();
        Some(DmlAnalyzeStats {
            affected_rows,
            matched_rows,
            candidate_rows,
            elapsed_ms,
            io,
            trace: io_stats,
            input_plan,
        })
    } else {
        None
    };

    match format {
        ExplainFormat::Traditional => Ok(QueryResponse::ResultSet(ResultSet {
            columns: if analyze {
                explain_analyze_result_columns()
            } else {
                planner::explain_result_columns()
            },
            rows: plan.traditional_rows(stats.as_ref()),
        })),
        ExplainFormat::Tree | ExplainFormat::Json => Ok(QueryResponse::ResultSet(ResultSet {
            columns: vec![Column {
                source: Default::default(),
                name: "EXPLAIN".to_string(),
                column_type: ColumnType::VarString,
            }],
            rows: vec![vec![Value::String(match format {
                ExplainFormat::Tree => plan.tree(stats.as_ref()),
                ExplainFormat::Json => plan.json(stats.as_ref()),
                ExplainFormat::Traditional => unreachable!(),
            })]],
        })),
    }
}

fn explain_dml_with_cte(
    db: &Arc<Database>,
    session: &mut SessionContext,
    with: &mut crate::parser::WithCte,
    format: ExplainFormat,
    analyze: bool,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    super::cte::apply_pre_materialization_rewrites(
        db,
        &session.current_database,
        &mut with.ctes,
        with.body.as_mut(),
        true,
    );
    let mut stubs = std::collections::HashMap::new();
    for cte in &with.ctes {
        let columns = cte_stub_columns(db, cte, &stubs);
        stubs.insert(
            cte.name.to_ascii_lowercase(),
            std::sync::Arc::new(crate::engine::api::CteResult::stub_for_explain(
                columns, 0.0,
            )),
        );
    }
    let (plan, cte_rows) = super::cte::with_cte_registry(stubs, || {
        let mut cte_rows = Vec::new();
        for cte in &with.ctes {
            cte_rows.extend(explain_cte_body(db, cte)?);
        }
        Ok::<_, QueryError>((
            DmlExplainPlan::prepare(db, session, with.body.as_ref())?,
            cte_rows,
        ))
    })?;

    let stats = if analyze {
        let rows_start = crate::engine::perf_events::rows_examined();
        crate::engine::api::begin_statement_dml_candidate_count();
        let started = std::time::Instant::now();
        let (mut io_stats, response) = analyze_stats::with_collector(|| {
            let cte_map = super::cte::materialize_ctes(
                db,
                session,
                &with.ctes,
                with.body.as_ref(),
                registry,
            )?;
            super::cte::with_cte_registry(cte_map, || {
                super::super::dispatch::execute_qualified_statement(
                    db,
                    session,
                    (*with.body).clone(),
                    registry,
                )
            })
        });
        let response = response?;
        let affected_rows = response_affected_rows(response)?;
        let matched_rows = crate::engine::api::statement_matched_rows();
        let candidate_rows = plan.analyzed_candidate_rows(
            crate::engine::api::take_statement_dml_candidate_rows().unwrap_or_else(|| {
                crate::engine::perf_events::rows_examined().saturating_sub(rows_start)
            }),
        );
        let elapsed_ms = started.elapsed().as_nanos() as f64 / 1_000_000.0;
        let input_plan = io_stats.take_executed_plan();
        let io = io_stats.query_io();
        Some(DmlAnalyzeStats {
            affected_rows,
            matched_rows,
            candidate_rows,
            elapsed_ms,
            io,
            trace: io_stats,
            input_plan,
        })
    } else {
        None
    };

    match format {
        ExplainFormat::Traditional => {
            let mut rows = plan.traditional_rows(stats.as_ref());
            let mut values = planner::explain_rows_to_values(cte_rows);
            if analyze {
                for row in &mut values {
                    row.push(Value::Null);
                    row.push(Value::Null);
                }
            }
            rows.extend(values);
            Ok(QueryResponse::ResultSet(ResultSet {
                columns: if analyze {
                    explain_analyze_result_columns()
                } else {
                    planner::explain_result_columns()
                },
                rows,
            }))
        }
        ExplainFormat::Tree | ExplainFormat::Json => {
            let mut text = match format {
                ExplainFormat::Tree => plan.tree(stats.as_ref()),
                ExplainFormat::Json => plan.json(stats.as_ref()),
                ExplainFormat::Traditional => unreachable!(),
            };
            if matches!(format, ExplainFormat::Tree) && !with.ctes.is_empty() {
                for cte in &with.ctes {
                    text.push_str(&format!("\n    CTE {} (materialized)", cte.name));
                }
            }
            Ok(QueryResponse::ResultSet(ResultSet {
                columns: vec![Column {
                    source: Default::default(),
                    name: "EXPLAIN".to_string(),
                    column_type: ColumnType::VarString,
                }],
                rows: vec![vec![Value::String(text)]],
            }))
        }
    }
}

fn response_affected_rows(response: QueryResponse) -> Result<u64, QueryError> {
    match response {
        QueryResponse::OkAffected { affected_rows } => Ok(affected_rows),
        QueryResponse::Ok => Ok(0),
        QueryResponse::ResultSet(rs) => Ok(rs.rows.len() as u64),
        QueryResponse::SharedResultSet(rs) => Ok(rs.rows.len() as u64),
        QueryResponse::StreamingResultSet(mut rs) => {
            let mut rows = 0;
            while rs.cursor.next_row()?.is_some() {
                rows += 1;
            }
            Ok(rows)
        }
        QueryResponse::MultiResult(results) => results
            .into_iter()
            .try_fold(0, |sum, item| Ok(sum + response_affected_rows(item)?)),
    }
}

impl DmlExplainPlan {
    fn analyzed_candidate_rows(&self, examined: u64) -> u64 {
        match &self.input {
            DmlExplainInput::Values(rows) => *rows as u64,
            DmlExplainInput::Plan(_)
            | DmlExplainInput::PartitionCandidates(_)
            | DmlExplainInput::MultiTable(_) => examined,
        }
    }

    fn prepare(
        db: &Arc<Database>,
        session: &SessionContext,
        target: &Statement,
    ) -> Result<Self, QueryError> {
        match target {
            Statement::Update(stmt) => {
                super::super::dispatch::reject_system_catalog_dml(db, session, &stmt.table)?;
                let display_primary = stmt.primary_alias.clone().unwrap_or_else(|| {
                    stmt.table
                        .rsplit('.')
                        .next()
                        .unwrap_or(&stmt.table)
                        .to_string()
                });
                let mut display_targets = std::collections::BTreeSet::new();
                let primary_is_target = stmt.assignments.iter().any(|assignment| {
                    assignment.table_qualifier.as_ref().is_none_or(|qualifier| {
                        qualifier.eq_ignore_ascii_case(&display_primary)
                            || qualifier.eq_ignore_ascii_case(&stmt.table)
                    })
                });
                for assignment in &stmt.assignments {
                    display_targets.insert(
                        assignment
                            .table_qualifier
                            .clone()
                            .unwrap_or_else(|| display_primary.clone()),
                    );
                }
                let display_table = display_targets.into_iter().collect::<Vec<_>>().join(",");
                let mut stmt =
                    super::super::dispatch::rewrite_update_through_view(db, session, stmt.clone())?;
                if let Some(internal) = session.resolve_temp_table_name(&stmt.table) {
                    stmt.table = internal;
                }
                if session.sysvars.sql_safe_updates
                    && stmt.selection.is_none()
                    && stmt.limit.is_none()
                {
                    return Err(error(crate::error::catalog::ER_UPDATE_WITHOUT_KEY_IN_SAFE_MODE,
                        "You are using safe update mode and you tried to update a table without a WHERE that uses a KEY column.",
                    ));
                }
                let target_schema = db.table(&stmt.table)?;
                for assignment in &stmt.assignments {
                    if assignment.table_qualifier.as_ref().is_none_or(|qualifier| {
                        qualifier.eq_ignore_ascii_case(&stmt.table)
                            || qualifier.eq_ignore_ascii_case(
                                stmt.table.rsplit('.').next().unwrap_or(&stmt.table),
                            )
                            || stmt
                                .primary_alias
                                .as_ref()
                                .is_some_and(|alias| alias.eq_ignore_ascii_case(qualifier))
                    }) {
                        target_schema.column_index(&assignment.column)?;
                    } else if let Some(qualifier) = &assignment.table_qualifier {
                        let join = stmt
                            .joins
                            .iter()
                            .find(|join| {
                                join.table
                                    .alias
                                    .as_deref()
                                    .unwrap_or_else(|| {
                                        join.table
                                            .name
                                            .rsplit('.')
                                            .next()
                                            .unwrap_or(&join.table.name)
                                    })
                                    .eq_ignore_ascii_case(qualifier)
                            })
                            .ok_or_else(|| {
                                error(
                                    crate::error::catalog::ER_BAD_FIELD_ERROR,
                                    format!("Unknown column '{}.{}'", qualifier, assignment.column),
                                )
                            })?;
                        let join_table = session
                            .resolve_temp_table_name(&join.table.name)
                            .unwrap_or_else(|| join.table.name.clone());
                        db.table(&join_table)?.column_index(&assignment.column)?;
                    }
                }
                super::super::multi_table::flip_lone_right_join(&mut stmt);
                let mut select = update_to_explain_select(&stmt);
                rewrite_temp_tables_for_explain(session, &mut select);
                prepare_select_for_planning(db, &mut select)?;
                let mut validation = select.clone();
                validation.projection = stmt
                    .assignments
                    .iter()
                    .filter(|assignment| {
                        !matches!(
                            assignment.value,
                            Expr::Default(_) | Expr::DefaultQualified(_)
                        )
                    })
                    .map(|assignment| SelectItem::Expr {
                        expr: assignment.value.clone(),
                        alias: None,
                    })
                    .collect();
                if validation.projection.is_empty() {
                    validation.projection.push(SelectItem::Wildcard);
                }
                planner::plan_select(db, &validation)?;
                let planned = planner::plan_select(db, &select)?;
                let mut targets = std::collections::HashSet::new();
                if primary_is_target {
                    targets.insert(stmt.table.to_ascii_lowercase());
                    targets.insert(
                        stmt.table
                            .rsplit('.')
                            .next()
                            .unwrap_or(&stmt.table)
                            .to_ascii_lowercase(),
                    );
                    if let Some(alias) = &stmt.primary_alias {
                        targets.insert(alias.to_ascii_lowercase());
                    }
                }
                for assignment in &stmt.assignments {
                    if let Some(qualifier) = &assignment.table_qualifier {
                        targets.insert(qualifier.to_ascii_lowercase());
                    }
                }
                Ok(Self {
                    operation: "UPDATE",
                    target_table: display_table,
                    target_names: targets,
                    partitions: if stmt.partitions.is_empty() {
                        None
                    } else {
                        dml_partition_names(db, &stmt.table, &stmt.partitions)?
                    },
                    input: if super::super::multi_table::is_multi_table_update(&stmt) {
                        DmlExplainInput::MultiTable(select.clone())
                    } else {
                        partition_dml_explain_input(
                            db,
                            &stmt.table,
                            stmt.selection.as_ref(),
                            &stmt.partitions,
                            &stmt.order_by,
                            stmt.limit.as_ref(),
                            true,
                            !super::super::multi_table::is_multi_table_update(&stmt),
                            planned.physical,
                        )?
                    },
                    extra: if stmt.ignore {
                        "IGNORE".to_string()
                    } else {
                        String::new()
                    },
                    hint_warnings: planned.hint_warnings,
                })
            }
            Statement::Delete(stmt) => {
                super::super::dispatch::reject_system_catalog_dml(db, session, &stmt.table)?;
                let display_table = if stmt.targets.is_empty() {
                    stmt.primary_alias.clone().unwrap_or_else(|| {
                        stmt.table
                            .rsplit('.')
                            .next()
                            .unwrap_or(&stmt.table)
                            .to_string()
                    })
                } else {
                    stmt.targets.join(",")
                };
                let mut stmt =
                    super::super::dispatch::rewrite_delete_through_view(db, session, stmt.clone())?;
                if let Some(internal) = session.resolve_temp_table_name(&stmt.table) {
                    stmt.table = internal;
                }
                if session.sysvars.sql_safe_updates
                    && stmt.selection.is_none()
                    && stmt.limit.is_none()
                {
                    return Err(error(crate::error::catalog::ER_UPDATE_WITHOUT_KEY_IN_SAFE_MODE,
                        "You are using safe update mode and you tried to delete from a table without a WHERE that uses a KEY column.",
                    ));
                }
                super::super::multi_table::flip_lone_right_join_delete(&mut stmt);
                let mut select = delete_to_explain_select(&stmt);
                rewrite_temp_tables_for_explain(session, &mut select);
                prepare_select_for_planning(db, &mut select)?;
                let planned = planner::plan_select(db, &select)?;
                let mut targets = std::collections::HashSet::new();
                if stmt.targets.is_empty() {
                    targets.insert(stmt.table.to_ascii_lowercase());
                    targets.insert(
                        stmt.table
                            .rsplit('.')
                            .next()
                            .unwrap_or(&stmt.table)
                            .to_ascii_lowercase(),
                    );
                    if let Some(alias) = &stmt.primary_alias {
                        targets.insert(alias.to_ascii_lowercase());
                    }
                } else {
                    targets.extend(stmt.targets.iter().map(|name| name.to_ascii_lowercase()));
                }
                Ok(Self {
                    operation: "DELETE",
                    target_table: display_table,
                    target_names: targets,
                    partitions: if stmt.partitions.is_empty() {
                        None
                    } else {
                        dml_partition_names(db, &stmt.table, &stmt.partitions)?
                    },
                    input: if super::super::multi_table::is_multi_table_delete(&stmt) {
                        DmlExplainInput::MultiTable(select.clone())
                    } else {
                        partition_dml_explain_input(
                            db,
                            &stmt.table,
                            stmt.selection.as_ref(),
                            &stmt.partitions,
                            &stmt.order_by,
                            stmt.limit.as_ref(),
                            false,
                            !super::super::multi_table::is_multi_table_delete(&stmt),
                            planned.physical,
                        )?
                    },
                    extra: if stmt.ignore {
                        "IGNORE".to_string()
                    } else {
                        String::new()
                    },
                    hint_warnings: planned.hint_warnings,
                })
            }
            Statement::Insert(stmt) => {
                super::super::dispatch::reject_system_catalog_dml(db, session, &stmt.table)?;
                let display_table = stmt.target_alias.clone().unwrap_or_else(|| {
                    stmt.table
                        .rsplit('.')
                        .next()
                        .unwrap_or(&stmt.table)
                        .to_string()
                });
                let mut stmt =
                    super::super::dispatch::rewrite_insert_through_view(db, session, stmt.clone())?;
                if let Some(internal) = session.resolve_temp_table_name(&stmt.table) {
                    stmt.table = internal;
                }
                let target_schema = db.table(&stmt.table)?;
                if let Some(columns) = &stmt.columns {
                    let mut seen = std::collections::HashSet::new();
                    for column in columns {
                        target_schema.column_index(column)?;
                        if !seen.insert(column.to_ascii_lowercase()) {
                            return Err(error(
                                crate::error::catalog::ER_FIELD_SPECIFIED_TWICE,
                                format!("Column '{column}' specified twice"),
                            ));
                        }
                    }
                }
                let operation = if stmt.replace { "REPLACE" } else { "INSERT" };
                let target_table = display_table;
                let (input, source_warnings) = match &stmt.source {
                    crate::parser::InsertSource::Values(rows) => {
                        let expected = stmt
                            .columns
                            .as_ref()
                            .map_or(target_schema.columns.len(), Vec::len);
                        if rows.iter().any(|row| row.len() != expected) {
                            return Err(error(
                                crate::error::catalog::ER_WRONG_VALUE_COUNT_ON_ROW,
                                "Column count doesn't match value count at row 1",
                            ));
                        }
                        (DmlExplainInput::Values(rows.len()), Vec::new())
                    }
                    crate::parser::InsertSource::Query(query) => {
                        let mut query = query.clone();
                        fn prepare_source(
                            db: &Arc<Database>,
                            session: &SessionContext,
                            query: &mut crate::parser::QueryExpr,
                        ) -> Result<(), QueryError> {
                            match query {
                                crate::parser::QueryExpr::Select(select) => {
                                    rewrite_temp_tables_for_explain(session, select);
                                    prepare_select_for_planning(db, select)?;
                                    Ok(())
                                }
                                crate::parser::QueryExpr::Rrf(rrf) => {
                                    for branch in &mut rrf.branches {
                                        prepare_source(db, session, &mut branch.query)?;
                                    }
                                    Ok(())
                                }
                                crate::parser::QueryExpr::SetOp(set) => {
                                    prepare_source(db, session, &mut set.left)?;
                                    prepare_source(db, session, &mut set.right)
                                }
                                crate::parser::QueryExpr::WithCte(_) => Err(error(
                                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                                    "EXPLAIN of a nested WITH source is not supported",
                                )),
                            }
                        }
                        let mut warnings = Vec::new();
                        prepare_source(db, session, &mut query)?;
                        let plan = match &query {
                            crate::parser::QueryExpr::Select(select) => {
                                let planned = planner::plan_select(db, select)?;
                                warnings.extend(planned.hint_warnings);
                                planned.physical
                            }
                            _ => planner::plan_query_physical(db, &query)?.ok_or_else(|| {
                                error(
                                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                                    "INSERT query source could not be lowered to a physical graph",
                                )
                            })?,
                        };
                        (DmlExplainInput::Plan(Box::new(plan)), warnings)
                    }
                };
                let mut target_names = std::collections::HashSet::new();
                target_names.insert(stmt.table.to_ascii_lowercase());
                target_names.insert(
                    stmt.table
                        .rsplit('.')
                        .next()
                        .unwrap_or(&stmt.table)
                        .to_ascii_lowercase(),
                );
                target_names.insert(target_table.to_ascii_lowercase());
                let mut hint_warnings = stmt.hint_warnings.clone();
                hint_warnings.extend(source_warnings);
                Ok(Self {
                    operation,
                    target_table,
                    target_names,
                    partitions: insert_partition_names(db, &stmt)?,
                    input,
                    extra: dml_extra(&stmt),
                    hint_warnings,
                })
            }
            _ => unreachable!("DmlExplainPlan::prepare called for non-DML"),
        }
    }

    fn traditional_rows(&self, stats: Option<&DmlAnalyzeStats>) -> Vec<Vec<Value>> {
        let mut rows = match &self.input {
            DmlExplainInput::MultiTable(select) => select
                .from
                .iter()
                .take(1)
                .chain(select.joins.iter().map(|join| &join.table))
                .enumerate()
                .map(|(index, source)| {
                    let mut row = self.target_row();
                    let name = source
                        .alias
                        .as_deref()
                        .unwrap_or_else(|| source.name.rsplit('.').next().unwrap_or(&source.name));
                    row[1] = Value::String(
                        if self.target_names.contains(&name.to_ascii_lowercase()) {
                            self.operation
                        } else {
                            "SIMPLE"
                        }
                        .into(),
                    );
                    row[2] = Value::String(name.into());
                    row[3] = if source.partitions.is_empty() {
                        Value::Null
                    } else {
                        Value::String(source.partitions.join(","))
                    };
                    let condition = index
                        .checked_sub(1)
                        .and_then(|i| select.joins.get(i))
                        .and_then(|join| join.condition.as_ref());
                    let mut extra =
                        "Materialized full scan; nested loop multi-table DML".to_string();
                    if let Some(condition) = condition {
                        extra.push_str(&format!("; ON {}", explain_expr(condition)));
                    }
                    if index == 0 {
                        if let Some(filter) = &select.selection {
                            extra.push_str(&format!("; WHERE {}", explain_expr(filter)));
                        }
                    }
                    row[11] = Value::String(extra);
                    row
                })
                .collect(),
            DmlExplainInput::PartitionCandidates(input) => {
                let mut row = self.target_row();
                row[3] = if input.partitions.is_empty() {
                    Value::Null
                } else {
                    Value::String(input.partitions.join(","))
                };
                row[4] = Value::String(input.access.into());
                row[5] = input.key.clone().map(Value::String).unwrap_or(Value::Null);
                row[6] = row[5].clone();
                row[11] =
                    Value::String("Partition DML candidates; current-row predicate recheck".into());
                vec![row]
            }
            DmlExplainInput::Values(_) => vec![self.target_row()],
            DmlExplainInput::Plan(plan) if matches!(self.operation, "INSERT" | "REPLACE") => {
                let mut rows = vec![self.target_row()];
                rows.extend(planner::explain_rows_to_values(planner::explain_rows(plan)));
                rows
            }
            DmlExplainInput::Plan(plan) => {
                let mut explain = planner::explain_rows(plan);
                for row in &mut explain {
                    if self.target_names.contains(&row.table.to_ascii_lowercase()) {
                        row.select_type = self.operation.to_string();
                        row.table = self.target_table.clone();
                        if let Some(partitions) = &self.partitions {
                            row.partitions = partitions.clone();
                        }
                    }
                }
                planner::explain_rows_to_values(explain)
            }
        };
        if let Some(actual) = stats {
            for (idx, row) in rows.iter_mut().enumerate() {
                if idx == 0 {
                    row.push(Value::Double(actual.affected_rows as f64));
                    row.push(Value::Int(1));
                    row.push(Value::Double(actual.elapsed_ms));
                    row.push(Value::Double(actual.elapsed_ms));
                    row.push(Value::Int(actual.affected_rows as i64));
                } else {
                    row.extend(std::iter::repeat_n(Value::Null, 5));
                }
            }
        }
        rows
    }

    fn target_row(&self) -> Vec<Value> {
        vec![
            Value::Int(1),
            Value::String(self.operation.to_string()),
            Value::String(self.target_table.clone()),
            self.partitions
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null),
            Value::String("ALL".to_string()),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            if self.extra.is_empty() {
                Value::Null
            } else {
                Value::String(self.extra.clone())
            },
        ]
    }

    fn tree(&self, stats: Option<&DmlAnalyzeStats>) -> String {
        let actual = stats
            .map(|s| {
                let mut text = format!(
                    " (actual time={:.3}..{:.3} rows={} loops=1)",
                    s.elapsed_ms, s.elapsed_ms, s.affected_rows
                );
                if let Some(matched) = s.matched_rows {
                    text.push_str(&format!(" (matched={matched} changed={})", s.affected_rows));
                }
                text.push_str(&format!(" (candidates={})", s.candidate_rows));
                text
            })
            .unwrap_or_default();
        let query_io = stats
            .map(|s| {
                let reads = s.io.cache_reads.saturating_add(s.io.physical_reads);
                let hit_rate = if reads == 0 {
                    0.0
                } else {
                    s.io.cache_reads as f64 * 100.0 / reads as f64
                };
                format!(
                    "Query I/O: physical_reads={} cache_reads={} cache_hit_rate={hit_rate:.2}%\n",
                    s.io.physical_reads, s.io.cache_reads,
                )
            })
            .unwrap_or_default();
        let mut out = format!(
            "{query_io}-> {} on {}{}",
            title_case_operation(self.operation),
            self.target_table,
            actual
        );
        if let Some(partitions) = &self.partitions {
            out.push_str(&format!(" (partitions={partitions})"));
        }
        if !self.extra.is_empty() {
            out.push_str(&format!(" ({})", self.extra));
        }
        out.push('\n');
        match &self.input {
            DmlExplainInput::MultiTable(select) => {
                out.push_str("    -> Nested loop multi-table DML (materialize sources; deduplicate targets by storage_id,row_id)\n");
                for source in select
                    .from
                    .iter()
                    .take(1)
                    .chain(select.joins.iter().map(|join| &join.table))
                {
                    out.push_str(&format!(
                        "        -> Full scan {} (partitions={})\n",
                        source.alias.as_deref().unwrap_or(&source.name),
                        source.partitions.join(",")
                    ));
                }
                for join in &select.joins {
                    out.push_str(&format!(
                        "        Join {:?}: {}\n",
                        join.join_type,
                        join.condition
                            .as_ref()
                            .map(explain_expr)
                            .unwrap_or_else(|| "TRUE".into())
                    ));
                }
                if let Some(filter) = &select.selection {
                    out.push_str(&format!("        Filter: {}\n", explain_expr(filter)));
                }
            }
            DmlExplainInput::PartitionCandidates(input) => {
                let mut indent = "    ".to_string();
                if let Some(limit) = input.limit {
                    out.push_str(&format!("{indent}-> Limit {limit}\n"));
                    indent.push_str("    ");
                }
                if input.ordered {
                    out.push_str(&format!(
                        "{indent}-> Global order of partition candidates\n"
                    ));
                    indent.push_str("    ");
                }
                out.push_str(&format!(
                    "{indent}-> Partition DML {}{} (partitions: {}; row locator: storage_id,row_id)",
                    input.access,
                    input
                        .key
                        .as_ref()
                        .map(|key| format!(" using {key}"))
                        .unwrap_or_default(),
                    if input.partitions.is_empty() {
                        "none".into()
                    } else {
                        input.partitions.join(",")
                    }
                ));
                if let Some(filter) = &input.filter {
                    out.push_str(&format!(" (filter: {filter}; recheck before write)"));
                }
            }
            DmlExplainInput::Values(rows) => {
                out.push_str(&format!("    -> Values rows={rows}"));
            }
            DmlExplainInput::Plan(plan) => {
                let display = stats
                    .and_then(|actual| actual.input_plan.as_deref())
                    .unwrap_or(plan);
                out.push_str(&format_plan_tree(
                    display,
                    1,
                    stats.map(|actual| &actual.trace),
                    None,
                    None,
                ));
            }
        }
        out
    }

    fn json(&self, stats: Option<&DmlAnalyzeStats>) -> String {
        let input = match &self.input {
            DmlExplainInput::MultiTable(select) => serde_json::json!({
                "multi_table_dml": {
                    "algorithm": "nested_loop", "materialize_sources": true,
                    "target_identity": ["storage_id", "row_id"],
                    "sources": select.from.iter().take(1).chain(select.joins.iter().map(|join| &join.table)).map(|source| serde_json::json!({
                        "table": source.name, "alias": source.alias, "access_type": "ALL", "partitions": source.partitions,
                    })).collect::<Vec<_>>(),
                    "joins": select.joins.iter().map(|join| serde_json::json!({"join_type": format!("{:?}", join.join_type), "condition": join.condition.as_ref().map(explain_expr)})).collect::<Vec<_>>(),
                    "attached_condition": select.selection.as_ref().map(explain_expr),
                }
            }),
            DmlExplainInput::PartitionCandidates(input) => serde_json::json!({
                "partition_dml_candidates": {
                    "access_type": input.access, "key": input.key, "partitions": input.partitions,
                    "attached_condition": input.filter, "global_order": input.ordered, "limit": input.limit,
                    "row_locator": ["storage_id", "row_id"], "predicate_recheck": true,
                }
            }),
            DmlExplainInput::Values(rows) => serde_json::json!({
                "values": { "rows": rows }
            }),
            DmlExplainInput::Plan(plan) => {
                let display = stats
                    .and_then(|actual| actual.input_plan.as_deref())
                    .unwrap_or(plan);
                serde_json::from_str::<serde_json::Value>(&format_plan_json_with_ctes(
                    display,
                    stats.map(|actual| {
                        (
                            &actual.trace,
                            actual.candidate_rows,
                            actual.elapsed_ms,
                            actual.io,
                        )
                    }),
                    None,
                    None,
                ))
                .unwrap_or_else(|_| serde_json::json!({ "query_block": {} }))
            }
        };
        let mut mutation = serde_json::json!({
            "operation": self.operation.to_ascii_lowercase(),
            "table": self.target_table,
            "input": input,
        });
        if let Some(partitions) = &self.partitions {
            mutation["partitions"] = serde_json::Value::Array(
                partitions
                    .split(',')
                    .map(|name| serde_json::Value::String(name.to_string()))
                    .collect(),
            );
        }
        if !self.extra.is_empty() {
            mutation["extra"] = serde_json::Value::String(self.extra.clone());
        }
        if !self.hint_warnings.is_empty() {
            mutation["hint_warnings"] = serde_json::Value::Array(
                self.hint_warnings
                    .iter()
                    .cloned()
                    .map(serde_json::Value::String)
                    .collect(),
            );
        }
        if let Some(actual) = stats {
            mutation["actual"] = serde_json::json!({
                "executed": true,
                "loops": 1,
                "rows": actual.affected_rows as f64,
                "rows_total": actual.affected_rows,
                "first_row_ms": actual.elapsed_ms,
                "last_row_ms": actual.elapsed_ms,
                "active_time_total_ms": actual.elapsed_ms,
                "self_time_total_ms": 0.0,
                "time_scope": "statement_wall",
                "affected_rows": actual.affected_rows,
                "physical_reads": actual.io.physical_reads,
                "cache_reads": actual.io.cache_reads,
                "io_scope": "query",
                "candidate_rows": actual.candidate_rows,
            });
            match self.operation {
                "INSERT" | "REPLACE" => {
                    // Affected-row semantics can count conflict updates or a
                    // REPLACE delete+insert as two. Input rows are exact;
                    // do not mislabel affected_rows as inserted/replaced.
                    mutation["actual"]["input_rows"] = serde_json::json!(actual.candidate_rows);
                }
                "UPDATE" => {
                    mutation["actual"]["changed"] = serde_json::json!(actual.affected_rows);
                }
                "DELETE" => {
                    mutation["actual"]["deleted"] = serde_json::json!(actual.affected_rows);
                }
                _ => {}
            }
            if let Some(matched) = actual.matched_rows {
                mutation["actual"]["matched"] = serde_json::json!(matched);
            }
        }
        serde_json::to_string_pretty(&serde_json::json!({ "mutation": mutation }))
            .expect("serializing DML EXPLAIN JSON cannot fail")
    }
}

fn rewrite_temp_tables_for_explain(session: &SessionContext, select: &mut Select) {
    for table in &mut select.from {
        if let Some(internal) = session.resolve_temp_table_name(&table.name) {
            table.name = internal;
        }
    }
    if let Some(table) = &mut select.table {
        if let Some(internal) = session.resolve_temp_table_name(table) {
            *table = internal;
        }
    }
    for join in &mut select.joins {
        if let Some(internal) = session.resolve_temp_table_name(&join.table.name) {
            join.table.name = internal;
        }
        if let Some(crate::parser::SubQuery::Select(inner)) = &mut join.derived {
            rewrite_temp_tables_for_explain(session, inner);
        }
    }
    if let Some(crate::parser::SubQuery::Select(inner)) = &mut select.derived {
        rewrite_temp_tables_for_explain(session, inner);
    }
}

fn title_case_operation(operation: &str) -> &'static str {
    match operation {
        "INSERT" => "Insert",
        "REPLACE" => "Replace",
        "UPDATE" => "Update",
        "DELETE" => "Delete",
        _ => "Mutation",
    }
}

fn dml_partition_names(
    db: &Arc<Database>,
    table: &str,
    explicit: &[String],
) -> Result<Option<String>, QueryError> {
    let selected =
        crate::engine::executor::partition_clause::resolve_partition_clause(db, table, explicit)?;
    let schema = db.table(table)?;
    let Some(info) = schema.partitioning.as_ref() else {
        return Ok(None);
    };
    let storage_ids = selected.unwrap_or_else(|| info.storage_ids());
    let mut names = Vec::new();
    for storage_id in storage_ids {
        if let Some((_, partition)) = info.find_by_storage_id(storage_id) {
            let name = partition
                .subpartitions
                .iter()
                .find(|sub| sub.storage_id == storage_id)
                .map(|sub| sub.name.clone())
                .unwrap_or_else(|| partition.name.clone());
            if !names.contains(&name) {
                names.push(name);
            }
        }
    }
    Ok((!names.is_empty()).then(|| names.join(",")))
}

fn insert_partition_names(
    db: &Arc<Database>,
    stmt: &crate::parser::Insert,
) -> Result<Option<String>, QueryError> {
    if !stmt.partitions.is_empty() {
        return dml_partition_names(db, &stmt.table, &stmt.partitions);
    }
    let schema = db.table(&stmt.table)?;
    let Some(partitioning) = schema.partitioning.as_ref() else {
        return Ok(None);
    };
    if let crate::parser::InsertSource::Values(rows) = &stmt.source {
        if stmt.columns.is_none() {
            let mut names = Vec::new();
            let mut all_constant = true;
            for expressions in rows {
                let mut values = Vec::with_capacity(expressions.len());
                for expression in expressions {
                    if let Some(value) = crate::engine::expr::expr_literal_value(expression) {
                        values.push(value);
                    } else {
                        all_constant = false;
                        break;
                    }
                }
                if !all_constant || values.len() != schema.columns.len() {
                    break;
                }
                let Ok(routed) = schema.route_row_to_partition(&values) else {
                    all_constant = false;
                    break;
                };
                if let Some((_, partition)) = partitioning.find_by_storage_id(routed.storage_id) {
                    let name = partition
                        .subpartitions
                        .iter()
                        .find(|sub| sub.storage_id == routed.storage_id)
                        .map(|sub| sub.name.clone())
                        .unwrap_or_else(|| partition.name.clone());
                    if !names.contains(&name) {
                        names.push(name);
                    }
                }
            }
            if all_constant && !names.is_empty() {
                return Ok(Some(names.join(",")));
            }
        }
    }
    dml_partition_names(db, &stmt.table, &[])
}

/// Build a `SELECT *` that has the same FROM / joins / WHERE as `stmt`.
/// Mirrors what a single-table or multi-table UPDATE would scan.
fn update_to_explain_select(stmt: &crate::parser::Update) -> Select {
    dml_to_explain_select(
        &stmt.table,
        &stmt.primary_alias,
        &stmt.joins,
        &stmt.selection,
        &stmt.order_by,
        &stmt.limit,
        &stmt.hint_warnings,
    )
}

fn delete_to_explain_select(stmt: &crate::parser::Delete) -> Select {
    dml_to_explain_select(
        &stmt.table,
        &stmt.primary_alias,
        &stmt.joins,
        &stmt.selection,
        &stmt.order_by,
        &stmt.limit,
        &stmt.hint_warnings,
    )
}

fn force_multi_dml_join_shape(select: &mut Select) {
    if select.joins.is_empty() {
        return;
    }
    let mut tables = select
        .from
        .iter()
        .map(|table| table.alias.clone().unwrap_or_else(|| table.name.clone()))
        .collect::<Vec<_>>();
    tables.extend(select.joins.iter().map(|join| {
        join.table
            .alias
            .clone()
            .unwrap_or_else(|| join.table.name.clone())
    }));
    select.query_hints.push(crate::parser::QueryHint::Ordered);
    select
        .query_hints
        .push(crate::parser::QueryHint::NlJoin { tables });
}

fn dml_extra(stmt: &crate::parser::Insert) -> String {
    let mut parts: Vec<&'static str> = Vec::new();
    if stmt.ignore {
        parts.push("IGNORE");
    }
    if stmt.on_duplicate_key_update.is_some() {
        parts.push("ON DUPLICATE KEY UPDATE");
    }
    parts.join(", ")
}

fn json_table_header(
    out: &mut String,
    indent: &str,
    kind: &str,
    name: &str,
    table: &planner::PlanTable,
    filter: Option<&Expr>,
    access_type: &str,
) {
    out.push_str(&format!("{indent}\"{kind}\": {{\n"));
    out.push_str(&format!("{indent}  \"table_name\": \"{name}\",\n"));
    json_partitions_field(out, indent, table, filter);
    out.push_str(&format!("{indent}  \"access_type\": \"{access_type}\",\n"));
}

fn json_scan_footer(
    out: &mut String,
    indent: &str,
    plan: &planner::PhysicalPlan,
    filter: Option<&Expr>,
    stats: Option<&NodeStatsMap>,
) {
    json_attached_condition(out, indent, filter);
    json_cost_info(out, indent, plan, planner::plan_cost(plan).total);
    json_close_node(out, indent, stats, plan);
}

fn dml_to_explain_select(
    table: &str,
    primary_alias: &Option<String>,
    joins: &[crate::parser::Join],
    selection: &Option<Expr>,
    order_by: &[OrderBy],
    limit: &Option<LimitValue>,
    hint_warnings: &[String],
) -> Select {
    let primary = TableRef {
        name: table.to_owned(),
        alias: primary_alias.to_owned(),
        system_time: None,
        index_hints: Vec::new(),
        partitions: Vec::new(),
        column_aliases: Vec::new(),
        generate_series: None,
    };
    let mut select = Select {
        projection: vec![SelectItem::Wildcard],
        table: Some(table.to_owned()),
        from: vec![primary],
        joins: joins.to_owned(),
        selection: selection.to_owned(),
        order_by: order_by.to_owned(),
        limit: limit.to_owned(),
        hint_warnings: hint_warnings.to_owned(),
        ..Select::default()
    };
    force_multi_dml_join_shape(&mut select);
    select
}

#[cfg(test)]
mod correlation_scope_tests {
    use super::*;

    #[test]
    fn explain_correlation_keeps_query_and_window_scopes() {
        let inner = ["local".to_string()].into_iter().collect();
        for (sql, expected) in [
            ("local.a + outer_t.b", true),
            (
                "SUM(local.a) OVER (PARTITION BY outer_t.b ORDER BY outer_t.c)",
                false,
            ),
            ("SUM(outer_t.a) OVER (PARTITION BY local.b)", true),
            (
                "local.a IN (SELECT x FROM inner_t WHERE inner_t.x=outer_t.x)",
                true,
            ),
            ("local.a IN (SELECT inner_t.x FROM inner_t)", false),
            ("CASE WHEN local.a>0 THEN outer_t.b ELSE local.c END", true),
        ] {
            let expr = crate::parser::parse_expr_fragment(sql).unwrap();
            let mut found = false;
            walk_one_expr_for_outer_qualifier(&expr, &inner, &mut found);
            assert_eq!(found, expected, "{sql}");
        }
    }
}
