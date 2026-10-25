//! Immutable query execution context inherited by every operator worker.

use std::collections::HashMap;
use std::sync::Arc;

use super::analyze_stats::CollectorHandle;
use crate::engine::api::CteResult;
use crate::engine::cancel::CancellationSnapshot;
use crate::engine::executor::mem_budget::MemBudget;
use crate::engine::expr::SessionExprInfo;
use crate::storage::{Database, TxnView};

#[derive(Clone)]
pub(in crate::engine::executor) struct ExecutionContext {
    txn: TxnView,
    fulltext_scoring: Option<super::fulltext_context::Context>,
    #[cfg(test)]
    rrf_branch_hook: Option<super::hybrid::BranchHook>,
    temporal: Option<Arc<crate::engine::executor::temporal::TemporalReadContext>>,
    cancellation: CancellationSnapshot,
    params: Option<crate::engine::params::ParamContextSnapshot>,
    budget: Arc<MemBudget>,
    session_expr_info: Option<SessionExprInfo>,
    // Owned per-context clone: SessionContext is Send but intentionally not
    // Sync (it contains connection-local RefCells). ExecutionContext clones
    // give each worker an independent immutable view instead of sharing it.
    session_context: Option<crate::engine::api::SessionContext>,
    zone_map_optimizer_enabled: bool,
    dml_expression: bool,
    field_numeric_materialization: bool,
    optimizer_trace_context: Option<(u32, bool)>,
    ctes: HashMap<String, Arc<CteResult>>,
    materialized_ctes: HashMap<String, Arc<CteResult>>,
    cte_materialization_active: bool,
    session_registry: Option<crate::engine::session_registry::SessionRegistry>,
    pruned_partitions: Option<Vec<u64>>,
    partition_visit_collector: Option<Arc<std::sync::atomic::AtomicU64>>,
    stats_collector: Option<CollectorHandle>,
    env_snapshot: crate::util::env::OverrideSnapshot,
    tuning: crate::engine::executor::tuning::ExecutorTuningSnapshot,
    kernel_mode: crate::engine::executor::vec::config::VecMode,
    rollup_window_topk_hint: Option<super::plan::RollupWindowTopKHint>,
    group_concat_max_len: usize,
    warning_sink: crate::engine::api::StatementWarningSink,
    rows_examined: Option<Arc<crate::engine::perf_events::RowsExaminedCounters>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zone_map_audit_worker_restores_optimizer_switch() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let db = Arc::new(Database::open(dir.path()).unwrap());
        for enabled in [false, true] {
            let context = {
                let _scope = crate::storage::schema::ZoneMapOptimizerScope::install(enabled);
                ExecutionContext::capture(TxnView::read_only(
                    0,
                    crate::storage::IsolationLevel::ReadCommitted,
                ))
            };
            let db = db.clone();
            std::thread::spawn(move || {
                let _scope = crate::storage::schema::ZoneMapOptimizerScope::install(!enabled);
                context.run(&db, || {
                    assert_eq!(
                        crate::storage::schema::zone_map_optimizer_enabled(),
                        enabled
                    )
                });
                assert_eq!(
                    crate::storage::schema::zone_map_optimizer_enabled(),
                    !enabled
                );
            })
            .join()
            .unwrap();
        }
    }

    #[test]
    fn parallel_read_admission_uses_captured_cancellation_and_oom_without_tls() {
        let budget = Arc::new(MemBudget::new(1024));
        let context = ExecutionContext::capture_with_budget(
            TxnView::read_only(0, crate::storage::IsolationLevel::ReadCommitted),
            budget.clone(),
        )
        .fork_worker_cancellation();
        context.cancellation().cancel_local();
        let cancelled = context.clone();
        let code = std::thread::spawn(move || {
            assert!(crate::engine::cancel::current_token().is_none());
            cancelled
                .enter_parallel_read_pressure()
                .err()
                .unwrap()
                .code()
        })
        .join()
        .unwrap();
        assert_eq!(code, 1317);
        budget.latch_terminal_oom(crate::error::catalog::ER_OUTOFMEMORY.error("test OOM"));
        let code = std::thread::spawn(move || {
            context.enter_parallel_read_pressure().err().unwrap().code()
        })
        .join()
        .unwrap();
        assert_eq!(
            code, 1037,
            "terminal OOM must retain precedence over cancellation"
        );
    }
}

impl ExecutionContext {
    pub(in crate::engine::executor) fn capture(txn: TxnView) -> Self {
        let budget = crate::engine::executor::mem_budget::current_budget()
            .unwrap_or_else(|| Arc::new(MemBudget::from_env()));
        Self::capture_with_budget(txn, budget)
    }

    pub(in crate::engine::executor) fn capture_with_budget(
        txn: TxnView,
        budget: Arc<MemBudget>,
    ) -> Self {
        Self {
            txn,
            fulltext_scoring: super::fulltext_context::capture(),
            #[cfg(test)]
            rrf_branch_hook: super::hybrid::branch_hook(),
            temporal: crate::engine::executor::temporal::capture(),
            cancellation: CancellationSnapshot::capture(),
            params: crate::engine::params::snapshot_current_context(),
            budget,
            session_expr_info: crate::engine::expr::current_session_expr_info(),
            session_context: crate::engine::expr::current_session_context_snapshot(),
            zone_map_optimizer_enabled: crate::storage::schema::zone_map_optimizer_enabled(),
            dml_expression: crate::engine::expr::strict_dml_expression(),
            field_numeric_materialization:
                crate::engine::expr::field_numeric_materialization_active(),
            optimizer_trace_context: crate::engine::expr::current_optimizer_trace_context(),
            ctes: super::eval::capture_subquery_ctes(),
            materialized_ctes: crate::engine::executor::cte::current_cte_registry(),
            cte_materialization_active: crate::engine::executor::cte::cte_materialization_active(),
            session_registry: super::current_session_registry(),
            pruned_partitions: crate::engine::executor::partition_router::current_pruned_partitions(
            ),
            partition_visit_collector:
                crate::engine::executor::partition_router::current_partition_visit_collector(),
            stats_collector: super::analyze_stats::current_collector_handle(),
            env_snapshot: crate::util::env::override_snapshot(),
            tuning: crate::engine::executor::tuning::current_or_capture(),
            kernel_mode: crate::engine::executor::vec::config::effective_mode(),
            rollup_window_topk_hint: super::plan::current_rollup_window_topk_hint(),
            group_concat_max_len: super::aggregate::group_concat_max_len(),
            warning_sink: crate::engine::api::current_statement_warning_sink()
                .unwrap_or_else(|| Arc::new(parking_lot::Mutex::new(Default::default()))),
            rows_examined: crate::engine::perf_events::current_rows_examined_counter(),
        }
    }

    pub(super) fn with_pruned_partitions(mut self, partitions: Option<Vec<u64>>) -> Self {
        self.pruned_partitions = partitions;
        self
    }

    /// Cap nested executor workers when the caller already supplies an outer
    /// worker pool. This keeps total statement parallelism within the captured
    /// scheduler degree instead of multiplying two independent pools.
    pub(super) fn with_scheduler_parallelism(mut self, parallelism: usize) -> Self {
        self.tuning.scheduler_parallelism = parallelism.max(1);
        self
    }

    /// Give a channel-backed stream its own cancellation branch. Early
    /// consumer Drop stops that producer tree but leaves the owning statement
    /// token usable for normal LIMIT/TopN completion and sibling operators.
    pub(super) fn fork_worker_cancellation(mut self) -> Self {
        self.cancellation = self.cancellation.fork_local();
        self
    }

    pub(super) fn budget(&self) -> &Arc<MemBudget> {
        &self.budget
    }

    pub(super) fn cancellation(&self) -> &CancellationSnapshot {
        &self.cancellation
    }

    pub(super) fn terminal_error(
        &self,
        error: crate::types::QueryError,
    ) -> crate::types::QueryError {
        if error.is(crate::error::catalog::ER_OUTOFMEMORY) {
            let error = self.budget.latch_terminal_oom(error);
            self.cancellation.cancel_local();
            error
        } else {
            error
        }
    }

    pub(super) fn txn(&self) -> TxnView {
        self.txn
    }

    pub(super) fn warning_sink(&self) -> &crate::engine::api::StatementWarningSink {
        &self.warning_sink
    }

    /// Whether a parallel read opened by this statement should shape writers
    /// in other sessions. The complete SessionContext snapshot was captured at
    /// statement start, so this remains fixed for every worker and cannot make
    /// plan topology depend on runtime pressure.
    pub(super) fn parallel_read_pressure_eligible(&self) -> bool {
        self.session_context.as_ref().map_or(true, |session| {
            crate::util::exec_permit::parallel_read_pressure_eligible(
                session.read_only_txn,
                session.write_intent_seen,
            )
        })
    }

    pub(super) fn enter_parallel_read_pressure(
        &self,
    ) -> Result<Option<crate::util::exec_permit::ParallelReadPressureGuard>, crate::types::QueryError>
    {
        crate::util::exec_permit::enter_parallel_read_pressure(
            self.budget.query_id(),
            self.parallel_read_pressure_eligible(),
            || {
                // Use the captured context: a cursor can be opened on a
                // different thread or after its caller's TLS scope ended.
                if let Some(error) = self.budget.terminal_oom() {
                    return Err(error);
                }
                self.cancellation.check()
            },
        )
    }

    pub(in crate::engine::executor) fn run<R>(
        &self,
        db: &Arc<Database>,
        f: impl FnOnce() -> R,
    ) -> R {
        let txn = self.txn;
        let _fulltext_scoring = super::fulltext_context::enter(self.fulltext_scoring.clone());
        #[cfg(test)]
        let _rrf_branch_hook = super::hybrid::enter_branch_hook(self.rrf_branch_hook.clone());
        let _read_schema = crate::storage::read_schema::ReadSchemaScope::enter(
            (!txn.dirty_read).then_some(txn.snapshot_scn),
        );
        let _zone_map_scope =
            crate::storage::schema::ZoneMapOptimizerScope::install(self.zone_map_optimizer_enabled);
        let _temporal_scope = crate::engine::executor::temporal::enter(self.temporal.clone());
        let _cancel_scope = self.cancellation.enter();
        let _rows_examined_scope =
            crate::engine::perf_events::enter_rows_examined_counter(self.rows_examined.clone());
        let _tuning_scope = crate::engine::executor::tuning::enter(self.tuning.clone());
        let _kernel_scope = crate::engine::executor::vec::config::enter_mode(self.kernel_mode);
        // The physical SELECT may move to a channel-backed streaming worker.
        // Preserve the surrounding window-to-ROLLUP Top-K contract across
        // that boundary just like the budget, CTE registry, and cancellation
        // context. Without this, wide ROLLUP executes with no hint and
        // materializes every group before the window discards almost all of
        // them.
        let _rollup_topk_scope =
            super::plan::set_rollup_window_topk_hint(self.rollup_window_topk_hint.clone());
        let _dml_expression_scope = self
            .dml_expression
            .then(crate::engine::expr::DmlExpressionGuard::new);
        let _optimizer_trace_scope =
            self.optimizer_trace_context
                .map(|(connection_id, enabled)| {
                    crate::engine::cbo::trace::activate(connection_id, enabled)
                });
        let mut operation = Some(f);
        let mut run = || {
            crate::engine::executor::cte::with_cte_registry(self.materialized_ctes.clone(), || {
                crate::engine::executor::cte::with_cte_materialization_active(
                    self.cte_materialization_active,
                    || {
                        super::aggregate::with_group_concat_max_len(
                            self.group_concat_max_len as u64,
                            || {
                                crate::engine::api::with_statement_warning_sink(
                                    self.warning_sink.clone(),
                                    || {
                                        crate::util::env::with_override_snapshot(
                                            self.env_snapshot.clone(),
                                            || {
                                                super::analyze_stats::with_collector_handle_if_present(
                                                self.stats_collector.clone(),
                                                || {
                                                    crate::engine::executor::partition_router::with_partition_visit_collector(
                            self.partition_visit_collector.clone(),
                            || {
                                crate::engine::executor::mem_budget::with_query_budget(
                                    Arc::clone(&self.budget),
                                    || {
                                        let run_with_session = || {
                                                let mut run_with_ctes = || {
                                                    super::eval::with_subquery_context_ctes(
                                                        db,
                                                        txn,
                                                        self.ctes.clone(),
                                                        operation
                                                            .take()
                                                            .expect("statement context runs once"),
                                                    )
                                                };
                                                let run_with_params = || match self.params.as_ref() {
                                                    Some(params) => params.run(run_with_ctes),
                                                    None => run_with_ctes(),
                                                };
                                                match self.pruned_partitions.as_ref() {
                                                Some(partitions) => crate::engine::executor::partition_router::with_pruned_partitions(
                                                    partitions.clone(),
                                                    run_with_params,
                                                ),
                                                None => run_with_params(),
                                            }
                                        };
                                        match self.session_context.as_ref() {
                                            Some(session) => crate::engine::expr::with_session_expr_info(
                                                session,
                                                run_with_session,
                                            ),
                                            None => crate::engine::expr::with_session_expr_info_option(
                                                &self.session_expr_info,
                                                run_with_session,
                                            ),
                                        }
                                    },
                                )
                            },
                        )
                                                },
                                            )
                                            },
                                        )
                                    },
                                )
                            },
                        )
                    },
                )
            })
        };
        let run_with_registry = || match self.session_registry.clone() {
            Some(registry) => super::with_session_registry(registry, run),
            None => run(),
        };
        let result = if self.field_numeric_materialization {
            crate::engine::expr::with_strict_numeric_materialization(true, run_with_registry)
        } else {
            run_with_registry()
        };
        crate::engine::memory_reclaimer::reclaim_worker_memory_after_phase();
        result
    }
}
