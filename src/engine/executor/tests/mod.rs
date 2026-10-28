mod ai;
mod critical_audit;
mod ddl_lock_contract;
mod fresh_release_audit;
mod github_release_audit;
mod http;
mod release_audit_1013;
mod release_audit_1014;
mod release_audit_1022;
mod sql_execution_audit_1015;
mod sql_execution_audit_1027;
mod sql_execution_fixes;
mod support;
mod table_ddl_audit_1012;
mod table_ddl_audit_1027;

use super::*;
use crate::parser::Assignment;

pub(super) fn memory_test_storage_config() -> StorageConfig {
    StorageConfig {
        durability: crate::storage::DurabilityMode::Memory,
        checkpoint_interval: Duration::from_millis(60_000),
        checkpoint_wal_bytes: 64 * 1024 * 1024,
        lock_wait: Duration::from_millis(1_000),
        group_commit_window: Duration::from_micros(0),
        wal_segment_bytes: 64 * 1024 * 1024,
        buffer_pool_pages: 512,
        buffer_pool_io_threads: 1,
        buffer_pool_prefetch_pages: 0,
        undo_retention: Duration::from_millis(60_000),
        heap_update_reserve_bytes: 2048,
        startup_validation: crate::storage::StartupValidationMode::Off,
        startup_validate_indexes: false,
    }
}

fn open_test_engine(dir: &std::path::Path) -> StorageQueryEngine {
    let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir,
        memory_test_storage_config(),
    )
    .unwrap();
    crate::engine::sysvar::builtin::query::allow_test_server_files();
    engine
}

/// Size large query inputs independently of the operator budgets under test.
/// Small-cache eviction fixtures continue to select their own storage config.
fn open_test_engine_with_buffer_pool_pages(
    dir: &std::path::Path,
    pages: u64,
) -> StorageQueryEngine {
    let mut config = memory_test_storage_config();
    config.buffer_pool_pages = pages;
    let engine =
        StorageQueryEngine::open_with_storage_config_without_background_workers(dir, config)
            .unwrap();
    crate::engine::sysvar::builtin::query::allow_test_server_files();
    engine
}

// Keep disk pages and WAL for online DDL's physical COPY path, while live
// semantic fixtures need no commit fsync. Recovery fixtures remain strict.
fn open_relaxed_test_engine(dir: &std::path::Path) -> StorageQueryEngine {
    let mut config = memory_test_storage_config();
    config.durability = crate::storage::DurabilityMode::Relaxed;
    let engine =
        StorageQueryEngine::open_with_storage_config_without_background_workers(dir, config)
            .unwrap();
    crate::engine::sysvar::builtin::query::allow_test_server_files();
    engine
}

fn open_background_test_engine(dir: &std::path::Path) -> StorageQueryEngine {
    let engine =
        StorageQueryEngine::open_with_storage_config(dir, memory_test_storage_config()).unwrap();
    crate::engine::sysvar::builtin::query::allow_test_server_files();
    engine
}

/// Query-level proof that execution reached the native `BatchOperator`
/// result boundary. Tests use this general pipeline metric instead of the
/// removed shape-specific fused/fast-path counters.
fn native_batch_hits() -> u64 {
    crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed)
}

fn explain_analyze_tree(engine: &StorageQueryEngine, sql: &str) -> String {
    let QueryResponse::ResultSet(result) = engine
        .execute(&format!("EXPLAIN ANALYZE FORMAT=TREE {sql}"))
        .expect("EXPLAIN ANALYZE should succeed")
    else {
        panic!("expected EXPLAIN ANALYZE result set");
    };
    result
        .rows
        .first()
        .and_then(|row| row.first())
        .map(Value::to_display_string)
        .expect("EXPLAIN ANALYZE should return a plan")
}

fn open_strict_test_engine(dir: &std::path::Path) -> StorageQueryEngine {
    let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir,
        StorageConfig {
            durability: crate::storage::DurabilityMode::Strict,
            checkpoint_interval: Duration::from_millis(60_000),
            checkpoint_wal_bytes: 64 * 1024 * 1024,
            lock_wait: Duration::from_millis(1_000),
            group_commit_window: Duration::from_micros(0),
            wal_segment_bytes: 64 * 1024 * 1024,
            buffer_pool_pages: 2_048,
            buffer_pool_io_threads: 1,
            buffer_pool_prefetch_pages: 2,
            undo_retention: Duration::from_millis(60_000),
            heap_update_reserve_bytes: 2048,
            startup_validation: crate::storage::StartupValidationMode::Auto,
            startup_validate_indexes: true,
        },
    )
    .unwrap();
    crate::engine::sysvar::builtin::query::allow_test_server_files();
    engine
}

/// Durable fixture that deliberately leaves index files cold after reopen.
/// Metadata regressions use this to prove that physical sizes do not depend
/// on startup validation or buffer-cache residency.
fn open_strict_cold_metadata_engine(dir: &std::path::Path) -> StorageQueryEngine {
    let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir,
        StorageConfig {
            durability: crate::storage::DurabilityMode::Strict,
            checkpoint_interval: Duration::from_millis(60_000),
            checkpoint_wal_bytes: 64 * 1024 * 1024,
            lock_wait: Duration::from_millis(1_000),
            group_commit_window: Duration::from_micros(0),
            wal_segment_bytes: 64 * 1024 * 1024,
            buffer_pool_pages: 2_048,
            buffer_pool_io_threads: 1,
            buffer_pool_prefetch_pages: 0,
            undo_retention: Duration::from_millis(60_000),
            heap_update_reserve_bytes: 2048,
            startup_validation: crate::storage::StartupValidationMode::Off,
            startup_validate_indexes: false,
        },
    )
    .unwrap();
    crate::engine::sysvar::builtin::query::allow_test_server_files();
    engine
}

/// Populate query fixtures through the batch storage API. This retains the
/// real row/index cardinality while avoiding per-row SQL expression work.
fn insert_fixture_rows(
    engine: &StorageQueryEngine,
    table: &str,
    rows: impl IntoIterator<Item = Vec<Value>>,
) {
    let db = engine.database();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(txn, table, rows.into_iter().collect())
        .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
}

/// SQL installs session timeouts over the storage fixture's millisecond timeout.
/// Use MySQL's smallest supported timeout when a regression deliberately keeps
/// a conflicting lock until the statement fails (row locks or metadata locks).
fn short_lock_wait_session() -> SessionContext {
    let mut session = SessionContext::default();
    session.sysvars.innodb_lock_wait_timeout = Some(1);
    session.sysvars.lock_wait_timeout = Some(1);
    session
}

// These fixtures verify live lock/snapshot semantics; recovery cases use
// open_strict_test_engine. Avoid fsync and speculative I/O in lock fixtures.
fn engine_with_lock_wait_ms(dir: &std::path::Path, lock_wait_ms: u64) -> StorageQueryEngine {
    StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir,
        StorageConfig {
            durability: crate::storage::DurabilityMode::Memory,
            checkpoint_interval: Duration::from_millis(60_000),
            checkpoint_wal_bytes: 64 * 1024 * 1024,
            lock_wait: Duration::from_millis(lock_wait_ms),
            group_commit_window: Duration::from_micros(0),
            wal_segment_bytes: 256 * 1024 * 1024,
            // Lock-wait tests exercise concurrency semantics, not a 1 GiB
            // cache.  Keep the fixture inside the default governed 60/30/10
            // split so startup validation can remain strict.
            buffer_pool_pages: 2_048,
            buffer_pool_io_threads: 1,
            buffer_pool_prefetch_pages: 0,
            undo_retention: Duration::from_millis(60_000),
            heap_update_reserve_bytes: 2048,
            startup_validation: crate::storage::StartupValidationMode::Auto,
            startup_validate_indexes: true,
        },
    )
    .unwrap()
}

fn number_statement_placeholders_for_test<'q>(
    statement: impl Into<crate::parser::QueryMut<'q>>,
    next: &mut usize,
) {
    let statement = statement.into();
    match statement {
        crate::parser::QueryMut::Select(stmt) => {
            if let Some(selection) = &mut stmt.selection {
                number_expr_placeholders_for_test(selection, next);
            }
        }
        _ => {}
    }
}

fn number_expr_placeholders_for_test(expr: &mut Expr, next: &mut usize) {
    match expr {
        Expr::Placeholder(idx) => {
            *idx = *next;
            *next += 1;
        }
        Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            number_expr_placeholders_for_test(expr, next)
        }
        Expr::Binary { left, right, .. } => {
            number_expr_placeholders_for_test(left, next);
            number_expr_placeholders_for_test(right, next);
        }
        Expr::InList { expr, list, .. } => {
            number_expr_placeholders_for_test(expr, next);
            for item in list {
                number_expr_placeholders_for_test(item, next);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            number_expr_placeholders_for_test(expr, next);
            number_expr_placeholders_for_test(low, next);
            number_expr_placeholders_for_test(high, next);
        }
        _ => {}
    }
}

fn test_env_lock() -> std::sync::MutexGuard<'static, ()> {
    use std::sync::{Mutex, OnceLock};
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

fn with_test_env<R>(vars: &[(&'static str, Option<&str>)], f: impl FnOnce() -> R) -> R {
    crate::util::env::with_vars(vars, f)
}

mod a1_vec_agg_bit_variance;
mod a2_vec_agg_distinct;
mod agg_projection_fast_path;
mod aggregate_in_scalar_fn;
mod api;
mod auto_analyze;
mod c_vec_rollup;
mod cast_column_name;
mod catalog_startup;
mod charset_collation;
mod check_constraints;
mod column_position;
mod column_source_flags;
mod compiled_select;
mod covering;
mod ctas_streaming;
mod cte_audit_regressions;
mod cte_join_agg_stream_fuse;
mod cte_mdl;
mod cte_predicate_pushdown;
mod cte_result_cache_invalidation;
mod cte_setop_projection_pruning;
mod cte_share;
mod cte_vec;
mod d2_load_data_progress;
mod d3_explain_dml;
mod ddl;
mod ddl_audit_1014;
mod ddl_hardening;
mod decorrelate_perf;
mod dml;
mod dml_returning;
mod events;
mod explain;
mod fd_agg_key_lowering;
mod float_double_md;
mod foreign_keys;
mod fulltext;
mod function_compat;
mod function_name_case;
mod functional_index;
mod generate_series;
mod geometry_type;
mod github_issue_fixes;
mod group_by_alias;
mod group_by_hidden_column;
mod group_by_ordinal;
mod hash_join_agg_fused_outer;
mod hash_join_build_preserved_parallel;
mod hash_join_int_pair_key;
mod hash_join_int_triple_key;
mod hash_join_mem_budget;
mod hash_join_null_scatter;
mod hash_join_prefilter_outer;
mod hash_join_streaming_residual;
mod hash_join_work_area;
mod hybrid_search;
mod in_subquery_semi;
mod index_merge;
mod index_range_fetch_optimization;
mod inlj_probe_bloom;
mod insert;
mod insert_select_review;
mod join_derived_bridge;
mod join_projection_fused;
mod join_stream;
mod json_type;
mod left_join_is_null;
mod m1_dml;
mod m3_dml;
mod m4_dml;
mod m5_dml;
mod memory_status;
mod mtr;
mod mysql80_compat_auth;
mod mysql80_compat_binlog_multi;
mod mysql80_compat_checksum;
mod mysql80_compat_comments;
mod mysql80_compat_data_masking;
mod mysql80_compat_dml;
mod mysql80_compat_enum;
mod mysql80_compat_field_list;
mod mysql80_compat_flush;
mod mysql80_compat_functional_index_json;
mod mysql80_compat_geohash;
mod mysql80_compat_index;
mod mysql80_compat_json_errors;
mod mysql80_compat_json_schema;
mod mysql80_compat_keyring;
mod mysql80_compat_like;
mod mysql80_compat_literals;
mod mysql80_compat_load_file;
mod mysql80_compat_metadata;
mod mysql80_compat_mts;
mod mysql80_compat_multi_dml;
mod mysql80_compat_numeric;
mod mysql80_compat_p0p1;
mod mysql80_compat_relaylog;
mod mysql80_compat_rename_table;
mod mysql80_compat_replication_status;
mod mysql80_compat_resource_group;
mod mysql80_compat_row_constructor;
mod mysql80_compat_select_into_file;
mod mysql80_compat_select_modifiers;
mod mysql80_compat_select_p2;
mod mysql80_compat_session_track;
mod mysql80_compat_set;
mod mysql80_compat_show;
mod mysql80_compat_shutdown;
mod mysql80_compat_slow_log;
mod mysql80_compat_table_ddl;
mod mysql80_compat_table_maintenance;
mod mysql80_compat_tablespace;
mod mysql80_compat_temporal;
mod mysql80_compat_txn;
mod mysql80_compat_user_conn_limit;
mod mysql80_compat_view;
mod mysql80_compat_view_check;
mod mysql80_compat_window_params;
mod mysql80_functions;
mod nlj_fast_path;
mod nlj_n3_small_batch;
mod nlj_n4_range_promote;
mod nlj_n5_mrr_chunk_shared;
mod nlj_n6_left_fused_agg;
mod nlj_n7_grouped_direct_agg;
mod not_in_anti;
mod online_ddl;
mod operators;
mod order_by_nulls;
mod order_by_ordinal;
mod order_by_semantics;
mod p0_cte_spill_and_union_streaming;
mod p0_inline_derived_nested;
mod p0_semi_anti_right_join;
mod p1_parallel_covering_inlj;
mod p1_vec_window;
mod parallel;
mod partition_order_by_key;
mod partitioning;
mod pctfree;
mod plan_cache;
mod prefix_descending_index;
mod prepared_dml;
mod privilege_audit;
mod q20_repro;
mod rc_concurrency;
mod read_compatible_ddl;
mod rollup;
mod routines;
mod scalar_decorrelate;
mod select;
mod select_audit;
mod sequence;
mod show_memory;
mod stats_estimation;
mod stats_surface;
mod streaming_agg;
mod string_binary_kernel;
mod string_collation;
mod string_func_edge;
mod string_length_semantics;
mod sysvar;
mod table_ddl_fixes_1015;
mod temporal_fsp;
mod temporary_tables;
mod time_travel;
mod timestamp_9999;
mod transaction_audit_1014;
mod triggers;
mod tx;
mod union_derived_pushdown;
mod update_delta;
mod vec_routing;
mod vector_bench;
mod vector_datatypes;
mod vector_quantization;
mod vector_type;
mod work_area;
mod xa;
mod zone_map;

mod dml_audit_1015;
mod dml_bugs_audit;
mod dml_in_lookup;
mod dml_path_contract;

mod tuple_index;

mod setop_derived_scope;

mod optimize_online;

mod statement_separator_scan;

mod verified_100;

mod expr_audit_regression;

mod executor_audit_regression;

mod parser_audit;
mod parser_quality;
mod sql_processing_audit;

mod planner_audit;

mod storage_audit;

mod metadata_audit;

mod schema_show_audit;

mod mysql_schema_audit;
mod sys_schema_audit;
mod view_audit_20261028;
mod view_source_audit;

mod table_ddl_audit;

mod session_management_audit;

mod constraints_audit;

mod expr_audit_20260928;

mod json_audit_20260928;

mod expression_stack;

mod window_audit_regressions;

mod ctas_insert_audit;

mod view_bugs_20260929;

mod query_engine_bugs_20260930;

/// Fixtures for MySQL's explicitly permissive GROUP BY mode.
fn execute_permissive(engine: &StorageQueryEngine, sql: &str) -> Result<QueryResponse, QueryError> {
    let mut session = SessionContext::default();
    engine.execute_with_session(&mut session, "SET sql_mode=''")?;
    engine.execute_with_session(&mut session, sql)
}

mod binlog_admin_metadata;

mod replication_admin_metadata;

mod compiled_range_recipe;

mod release_audit;

mod release_audit_1012;

mod parser_audit_1014;

mod release_audit_1023;
mod release_audit_1024;
mod release_audit_1025;

mod expression_functions_1028;
