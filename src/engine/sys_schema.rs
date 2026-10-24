//! MySQL 8.0-compatible `sys` schema registry.
//!
//! The sys schema is a compatibility/observability surface built on top of
//! information_schema and performance_schema. mydb exposes the same object
//! surface, but it does not fabricate missing performance instrumentation:
//! views backed by unsupported PFS producers return empty rows.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use chrono::{Local, TimeZone};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};

use crate::engine::api::{QueryResponse, SessionContext};
use crate::engine::information_schema::{SystemColumn, SystemTableDef, SystemTableIndex};
use crate::engine::session_registry::SessionRegistry;
use crate::parser::{Assignment, Delete, Expr, Insert, InsertSource, RoutineCall, Update};
use crate::storage::{
    error, split_table_name, DataTypeKind, Database, RoutineKindSchema, RoutineParamModeSchema,
    RoutineParamSchema, RoutineSchema, RoutineSecuritySchema, RoutineSqlDataAccessSchema,
    TriggerEventSchema, TriggerSchema, TriggerTimingSchema,
};
use crate::types::{ColumnType, QueryError, Value};

const SYS_VERSION: &str = "2.1.3";
const SYS_DEFINER_USER: &str = "mysql.sys";
const SYS_DEFINER_HOST: &str = "localhost";
const SYS_ROUTINE_SQL_MODE: &str = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION";

const SYS_CONFIG_INDEX_COLUMNS: &[&str] = &["variable"];
const SYS_CONFIG_CREATE_SQL: &str = "CREATE TABLE `sys_config` (\n  `variable` varchar(128) NOT NULL,\n  `value` varchar(128),\n  `set_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n  `set_by` varchar(128),\n  PRIMARY KEY (`variable`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

const INNODB_BUFFER_STATS_BY_SCHEMA_VIEW_COLUMNS: &[&str] = &[
    "object_schema",
    "allocated",
    "data",
    "pages",
    "pages_hashed",
    "pages_old",
    "rows_cached",
];
const INNODB_BUFFER_STATS_BY_TABLE_VIEW_COLUMNS: &[&str] = &[
    "object_schema",
    "object_name",
    "allocated",
    "data",
    "pages",
    "pages_hashed",
    "pages_old",
    "rows_cached",
];
const INNODB_LOCK_WAITS_VIEW_COLUMNS: &[&str] = &[
    "wait_started",
    "wait_age",
    "wait_age_secs",
    "locked_table",
    "locked_table_schema",
    "locked_table_name",
    "locked_table_partition",
    "locked_table_subpartition",
    "locked_index",
    "locked_type",
    "waiting_trx_id",
    "waiting_trx_started",
    "waiting_trx_age",
    "waiting_trx_rows_locked",
    "waiting_trx_rows_modified",
    "waiting_pid",
    "waiting_query",
    "waiting_lock_id",
    "waiting_lock_mode",
    "blocking_trx_id",
    "blocking_pid",
    "blocking_query",
    "blocking_lock_id",
    "blocking_lock_mode",
    "blocking_trx_started",
    "blocking_trx_age",
    "blocking_trx_rows_locked",
    "blocking_trx_rows_modified",
    "sql_kill_blocking_query",
    "sql_kill_blocking_connection",
];
const LATEST_FILE_IO_VIEW_COLUMNS: &[&str] =
    &["thread", "file", "latency", "operation", "requested"];
const IO_BY_THREAD_BY_LATENCY_VIEW_COLUMNS: &[&str] = &[
    "user",
    "total",
    "total_latency",
    "min_latency",
    "avg_latency",
    "max_latency",
    "thread_id",
    "processlist_id",
];
const IO_GLOBAL_BY_FILE_BY_BYTES_VIEW_COLUMNS: &[&str] = &[
    "file",
    "count_read",
    "total_read",
    "avg_read",
    "count_write",
    "total_written",
    "avg_write",
    "total",
    "write_pct",
];
const IO_GLOBAL_BY_FILE_BY_LATENCY_VIEW_COLUMNS: &[&str] = &[
    "file",
    "total",
    "total_latency",
    "count_read",
    "read_latency",
    "count_write",
    "write_latency",
    "count_misc",
    "misc_latency",
];
const IO_GLOBAL_BY_WAIT_BY_BYTES_VIEW_COLUMNS: &[&str] = &[
    "event_name",
    "total",
    "total_latency",
    "min_latency",
    "avg_latency",
    "max_latency",
    "count_read",
    "total_read",
    "avg_read",
    "count_write",
    "total_written",
    "avg_written",
    "total_requested",
];
const IO_GLOBAL_BY_WAIT_BY_LATENCY_VIEW_COLUMNS: &[&str] = &[
    "event_name",
    "total",
    "total_latency",
    "avg_latency",
    "max_latency",
    "read_latency",
    "write_latency",
    "misc_latency",
    "count_read",
    "total_read",
    "avg_read",
    "count_write",
    "total_written",
    "avg_written",
];
const MEMORY_BY_USER_BY_CURRENT_BYTES_VIEW_COLUMNS: &[&str] = &[
    "user",
    "current_count_used",
    "current_allocated",
    "current_avg_alloc",
    "current_max_alloc",
    "total_allocated",
];
const MEMORY_BY_HOST_BY_CURRENT_BYTES_VIEW_COLUMNS: &[&str] = &[
    "host",
    "current_count_used",
    "current_allocated",
    "current_avg_alloc",
    "current_max_alloc",
    "total_allocated",
];
const MEMORY_BY_THREAD_BY_CURRENT_BYTES_VIEW_COLUMNS: &[&str] = &[
    "thread_id",
    "user",
    "current_count_used",
    "current_allocated",
    "current_avg_alloc",
    "current_max_alloc",
    "total_allocated",
];
const MEMORY_GLOBAL_BY_CURRENT_BYTES_VIEW_COLUMNS: &[&str] = &[
    "event_name",
    "current_count",
    "current_alloc",
    "current_avg_alloc",
    "high_count",
    "high_alloc",
    "high_avg_alloc",
];
const SCHEMA_INDEX_STATISTICS_VIEW_COLUMNS: &[&str] = &[
    "table_schema",
    "table_name",
    "index_name",
    "rows_selected",
    "select_latency",
    "rows_inserted",
    "insert_latency",
    "rows_updated",
    "update_latency",
    "rows_deleted",
    "delete_latency",
];
const SCHEMA_TABLE_STATISTICS_VIEW_COLUMNS: &[&str] = &[
    "table_schema",
    "table_name",
    "total_latency",
    "rows_fetched",
    "fetch_latency",
    "rows_inserted",
    "insert_latency",
    "rows_updated",
    "update_latency",
    "rows_deleted",
    "delete_latency",
    "io_read_requests",
    "io_read",
    "io_read_latency",
    "io_write_requests",
    "io_write",
    "io_write_latency",
    "io_misc_requests",
    "io_misc_latency",
];
const SCHEMA_TABLE_STATISTICS_WITH_BUFFER_VIEW_COLUMNS: &[&str] = &[
    "table_schema",
    "table_name",
    "rows_fetched",
    "fetch_latency",
    "rows_inserted",
    "insert_latency",
    "rows_updated",
    "update_latency",
    "rows_deleted",
    "delete_latency",
    "io_read_requests",
    "io_read",
    "io_read_latency",
    "io_write_requests",
    "io_write",
    "io_write_latency",
    "io_misc_requests",
    "io_misc_latency",
    "innodb_buffer_allocated",
    "innodb_buffer_data",
    "innodb_buffer_free",
    "innodb_buffer_pages",
    "innodb_buffer_pages_hashed",
    "innodb_buffer_pages_old",
    "innodb_buffer_rows_cached",
];
const SCHEMA_TABLES_WITH_FULL_TABLE_SCANS_VIEW_COLUMNS: &[&str] = &[
    "object_schema",
    "object_name",
    "rows_full_scanned",
    "latency",
];
const SCHEMA_TABLE_LOCK_WAITS_VIEW_COLUMNS: &[&str] = &[
    "object_schema",
    "object_name",
    "waiting_thread_id",
    "waiting_pid",
    "waiting_account",
    "waiting_lock_type",
    "waiting_lock_duration",
    "waiting_query",
    "waiting_query_secs",
    "waiting_query_rows_affected",
    "waiting_query_rows_examined",
    "blocking_thread_id",
    "blocking_pid",
    "blocking_account",
    "blocking_lock_type",
    "blocking_lock_duration",
    "sql_kill_blocking_query",
    "sql_kill_blocking_connection",
];
const STATEMENTS_WITH_ERRORS_OR_WARNINGS_VIEW_COLUMNS: &[&str] = &[
    "query",
    "db",
    "exec_count",
    "errors",
    "error_pct",
    "warnings",
    "warning_pct",
    "first_seen",
    "last_seen",
    "digest",
];
const STATEMENTS_WITH_FULL_TABLE_SCANS_VIEW_COLUMNS: &[&str] = &[
    "query",
    "db",
    "exec_count",
    "total_latency",
    "no_index_used_count",
    "no_good_index_used_count",
    "no_index_used_pct",
    "rows_sent",
    "rows_examined",
    "rows_sent_avg",
    "rows_examined_avg",
    "first_seen",
    "last_seen",
    "digest",
];
const STATEMENTS_WITH_RUNTIMES_IN_95TH_PERCENTILE_VIEW_COLUMNS: &[&str] = &[
    "query",
    "db",
    "full_scan",
    "exec_count",
    "err_count",
    "warn_count",
    "total_latency",
    "max_latency",
    "avg_latency",
    "rows_sent",
    "rows_sent_avg",
    "rows_examined",
    "rows_examined_avg",
    "first_seen",
    "last_seen",
    "digest",
];
const STATEMENTS_WITH_SORTING_VIEW_COLUMNS: &[&str] = &[
    "query",
    "db",
    "exec_count",
    "total_latency",
    "sort_merge_passes",
    "avg_sort_merges",
    "sorts_using_scans",
    "sort_using_range",
    "rows_sorted",
    "avg_rows_sorted",
    "first_seen",
    "last_seen",
    "digest",
];
const STATEMENTS_WITH_TEMP_TABLES_VIEW_COLUMNS: &[&str] = &[
    "query",
    "db",
    "exec_count",
    "total_latency",
    "memory_tmp_tables",
    "disk_tmp_tables",
    "avg_tmp_tables_per_query",
    "tmp_tables_to_disk_pct",
    "first_seen",
    "last_seen",
    "digest",
];
const USER_SUMMARY_BY_FILE_IO_TYPE_VIEW_COLUMNS: &[&str] =
    &["user", "event_name", "total", "latency", "max_latency"];
const USER_SUMMARY_BY_STATEMENT_TYPE_VIEW_COLUMNS: &[&str] = &[
    "user",
    "statement",
    "total",
    "total_latency",
    "max_latency",
    "lock_latency",
    "cpu_latency",
    "rows_sent",
    "rows_examined",
    "rows_affected",
    "full_scans",
];
const USER_SUMMARY_BY_STATEMENT_LATENCY_VIEW_COLUMNS: &[&str] = &[
    "user",
    "total",
    "total_latency",
    "max_latency",
    "lock_latency",
    "cpu_latency",
    "rows_sent",
    "rows_examined",
    "rows_affected",
    "full_scans",
];
const USER_SUMMARY_BY_STAGES_VIEW_COLUMNS: &[&str] = &[
    "user",
    "event_name",
    "total",
    "total_latency",
    "avg_latency",
];
const USER_SUMMARY_VIEW_COLUMNS: &[&str] = &[
    "user",
    "statements",
    "statement_latency",
    "statement_avg_latency",
    "table_scans",
    "file_ios",
    "file_io_latency",
    "current_connections",
    "total_connections",
    "unique_hosts",
    "current_memory",
    "total_memory_allocated",
];
const HOST_SUMMARY_BY_FILE_IO_TYPE_VIEW_COLUMNS: &[&str] = &[
    "host",
    "event_name",
    "total",
    "total_latency",
    "max_latency",
];
const HOST_SUMMARY_BY_STATEMENT_TYPE_VIEW_COLUMNS: &[&str] = &[
    "host",
    "statement",
    "total",
    "total_latency",
    "max_latency",
    "lock_latency",
    "cpu_latency",
    "rows_sent",
    "rows_examined",
    "rows_affected",
    "full_scans",
];
const HOST_SUMMARY_BY_STATEMENT_LATENCY_VIEW_COLUMNS: &[&str] = &[
    "host",
    "total",
    "total_latency",
    "max_latency",
    "lock_latency",
    "cpu_latency",
    "rows_sent",
    "rows_examined",
    "rows_affected",
    "full_scans",
];
const HOST_SUMMARY_BY_STAGES_VIEW_COLUMNS: &[&str] = &[
    "host",
    "event_name",
    "total",
    "total_latency",
    "avg_latency",
];
const HOST_SUMMARY_VIEW_COLUMNS: &[&str] = &[
    "host",
    "statements",
    "statement_latency",
    "statement_avg_latency",
    "table_scans",
    "file_ios",
    "file_io_latency",
    "current_connections",
    "total_connections",
    "unique_users",
    "current_memory",
    "total_memory_allocated",
];
const WAIT_CLASSES_GLOBAL_BY_AVG_LATENCY_VIEW_COLUMNS: &[&str] = &[
    "event_class",
    "total",
    "total_latency",
    "min_latency",
    "avg_latency",
    "max_latency",
];
const WAITS_BY_USER_BY_LATENCY_VIEW_COLUMNS: &[&str] = &[
    "user",
    "event",
    "total",
    "total_latency",
    "avg_latency",
    "max_latency",
];
const WAITS_BY_HOST_BY_LATENCY_VIEW_COLUMNS: &[&str] = &[
    "host",
    "event",
    "total",
    "total_latency",
    "avg_latency",
    "max_latency",
];
const WAITS_GLOBAL_BY_LATENCY_VIEW_COLUMNS: &[&str] = &[
    "events",
    "total",
    "total_latency",
    "avg_latency",
    "max_latency",
];
const PROCESSLIST_VIEW_COLUMNS: &[&str] = &[
    "thd_id",
    "conn_id",
    "user",
    "db",
    "command",
    "state",
    "time",
    "current_statement",
    "execution_engine",
    "statement_latency",
    "progress",
    "lock_latency",
    "cpu_latency",
    "rows_examined",
    "rows_sent",
    "rows_affected",
    "tmp_tables",
    "tmp_disk_tables",
    "full_scan",
    "last_statement",
    "last_statement_latency",
    "current_memory",
    "last_wait",
    "last_wait_latency",
    "source",
    "trx_latency",
    "trx_state",
    "trx_autocommit",
    "pid",
    "program_name",
];

const VIEW_COLUMNS: &[(&str, &[&str])] = &[
    ("version", &["sys_version", "mysql_version"]),
    (
        "innodb_buffer_stats_by_schema",
        INNODB_BUFFER_STATS_BY_SCHEMA_VIEW_COLUMNS,
    ),
    (
        "x$innodb_buffer_stats_by_schema",
        INNODB_BUFFER_STATS_BY_SCHEMA_VIEW_COLUMNS,
    ),
    (
        "innodb_buffer_stats_by_table",
        INNODB_BUFFER_STATS_BY_TABLE_VIEW_COLUMNS,
    ),
    (
        "x$innodb_buffer_stats_by_table",
        INNODB_BUFFER_STATS_BY_TABLE_VIEW_COLUMNS,
    ),
    ("innodb_lock_waits", INNODB_LOCK_WAITS_VIEW_COLUMNS),
    ("x$innodb_lock_waits", INNODB_LOCK_WAITS_VIEW_COLUMNS),
    ("schema_object_overview", &["db", "object_type", "count"]),
    (
        "schema_auto_increment_columns",
        &[
            "table_schema",
            "table_name",
            "column_name",
            "data_type",
            "column_type",
            "is_signed",
            "is_unsigned",
            "max_value",
            "auto_increment",
            "auto_increment_ratio",
        ],
    ),
    (
        "x$schema_flattened_keys",
        &[
            "table_schema",
            "table_name",
            "index_name",
            "non_unique",
            "subpart_exists",
            "index_columns",
        ],
    ),
    (
        "schema_redundant_indexes",
        &[
            "table_schema",
            "table_name",
            "redundant_index_name",
            "redundant_index_columns",
            "redundant_index_non_unique",
            "dominant_index_name",
            "dominant_index_columns",
            "dominant_index_non_unique",
            "subpart_exists",
            "sql_drop_index",
        ],
    ),
    (
        "ps_check_lost_instrumentation",
        &["variable_name", "variable_value"],
    ),
    ("latest_file_io", LATEST_FILE_IO_VIEW_COLUMNS),
    ("x$latest_file_io", LATEST_FILE_IO_VIEW_COLUMNS),
    (
        "io_by_thread_by_latency",
        IO_BY_THREAD_BY_LATENCY_VIEW_COLUMNS,
    ),
    (
        "x$io_by_thread_by_latency",
        IO_BY_THREAD_BY_LATENCY_VIEW_COLUMNS,
    ),
    (
        "io_global_by_file_by_bytes",
        IO_GLOBAL_BY_FILE_BY_BYTES_VIEW_COLUMNS,
    ),
    (
        "x$io_global_by_file_by_bytes",
        IO_GLOBAL_BY_FILE_BY_BYTES_VIEW_COLUMNS,
    ),
    (
        "io_global_by_file_by_latency",
        IO_GLOBAL_BY_FILE_BY_LATENCY_VIEW_COLUMNS,
    ),
    (
        "x$io_global_by_file_by_latency",
        IO_GLOBAL_BY_FILE_BY_LATENCY_VIEW_COLUMNS,
    ),
    (
        "io_global_by_wait_by_bytes",
        IO_GLOBAL_BY_WAIT_BY_BYTES_VIEW_COLUMNS,
    ),
    (
        "x$io_global_by_wait_by_bytes",
        IO_GLOBAL_BY_WAIT_BY_BYTES_VIEW_COLUMNS,
    ),
    (
        "io_global_by_wait_by_latency",
        IO_GLOBAL_BY_WAIT_BY_LATENCY_VIEW_COLUMNS,
    ),
    (
        "x$io_global_by_wait_by_latency",
        IO_GLOBAL_BY_WAIT_BY_LATENCY_VIEW_COLUMNS,
    ),
    (
        "memory_by_user_by_current_bytes",
        MEMORY_BY_USER_BY_CURRENT_BYTES_VIEW_COLUMNS,
    ),
    (
        "x$memory_by_user_by_current_bytes",
        MEMORY_BY_USER_BY_CURRENT_BYTES_VIEW_COLUMNS,
    ),
    (
        "memory_by_host_by_current_bytes",
        MEMORY_BY_HOST_BY_CURRENT_BYTES_VIEW_COLUMNS,
    ),
    (
        "x$memory_by_host_by_current_bytes",
        MEMORY_BY_HOST_BY_CURRENT_BYTES_VIEW_COLUMNS,
    ),
    (
        "memory_by_thread_by_current_bytes",
        MEMORY_BY_THREAD_BY_CURRENT_BYTES_VIEW_COLUMNS,
    ),
    (
        "x$memory_by_thread_by_current_bytes",
        MEMORY_BY_THREAD_BY_CURRENT_BYTES_VIEW_COLUMNS,
    ),
    (
        "memory_global_by_current_bytes",
        MEMORY_GLOBAL_BY_CURRENT_BYTES_VIEW_COLUMNS,
    ),
    (
        "x$memory_global_by_current_bytes",
        MEMORY_GLOBAL_BY_CURRENT_BYTES_VIEW_COLUMNS,
    ),
    ("memory_global_total", &["total_allocated"]),
    ("x$memory_global_total", &["total_allocated"]),
    (
        "schema_index_statistics",
        SCHEMA_INDEX_STATISTICS_VIEW_COLUMNS,
    ),
    (
        "x$schema_index_statistics",
        SCHEMA_INDEX_STATISTICS_VIEW_COLUMNS,
    ),
    (
        "x$ps_schema_table_statistics_io",
        &[
            "table_schema",
            "table_name",
            "count_read",
            "sum_number_of_bytes_read",
            "sum_timer_read",
            "count_write",
            "sum_number_of_bytes_write",
            "sum_timer_write",
            "count_misc",
            "sum_timer_misc",
        ],
    ),
    (
        "schema_table_statistics",
        SCHEMA_TABLE_STATISTICS_VIEW_COLUMNS,
    ),
    (
        "x$schema_table_statistics",
        SCHEMA_TABLE_STATISTICS_VIEW_COLUMNS,
    ),
    (
        "schema_table_statistics_with_buffer",
        SCHEMA_TABLE_STATISTICS_WITH_BUFFER_VIEW_COLUMNS,
    ),
    (
        "x$schema_table_statistics_with_buffer",
        SCHEMA_TABLE_STATISTICS_WITH_BUFFER_VIEW_COLUMNS,
    ),
    (
        "schema_tables_with_full_table_scans",
        SCHEMA_TABLES_WITH_FULL_TABLE_SCANS_VIEW_COLUMNS,
    ),
    (
        "x$schema_tables_with_full_table_scans",
        SCHEMA_TABLES_WITH_FULL_TABLE_SCANS_VIEW_COLUMNS,
    ),
    (
        "schema_unused_indexes",
        &["object_schema", "object_name", "index_name"],
    ),
    (
        "schema_table_lock_waits",
        SCHEMA_TABLE_LOCK_WAITS_VIEW_COLUMNS,
    ),
    (
        "x$schema_table_lock_waits",
        SCHEMA_TABLE_LOCK_WAITS_VIEW_COLUMNS,
    ),
    (
        "statement_analysis",
        &[
            "query",
            "db",
            "full_scan",
            "exec_count",
            "err_count",
            "warn_count",
            "total_latency",
            "max_latency",
            "avg_latency",
            "lock_latency",
            "cpu_latency",
            "rows_sent",
            "rows_sent_avg",
            "rows_examined",
            "rows_examined_avg",
            "rows_affected",
            "rows_affected_avg",
            "tmp_tables",
            "tmp_disk_tables",
            "rows_sorted",
            "sort_merge_passes",
            "max_controlled_memory",
            "max_total_memory",
            "digest",
            "first_seen",
            "last_seen",
        ],
    ),
    (
        "x$statement_analysis",
        &[
            "query",
            "db",
            "full_scan",
            "exec_count",
            "exec_secondary_count",
            "err_count",
            "warn_count",
            "total_latency",
            "max_latency",
            "avg_latency",
            "lock_latency",
            "cpu_latency",
            "rows_sent",
            "rows_sent_avg",
            "rows_examined",
            "rows_examined_avg",
            "rows_affected",
            "rows_affected_avg",
            "tmp_tables",
            "tmp_disk_tables",
            "rows_sorted",
            "sort_merge_passes",
            "max_controlled_memory",
            "max_total_memory",
            "digest",
            "first_seen",
            "last_seen",
        ],
    ),
    (
        "statements_with_errors_or_warnings",
        STATEMENTS_WITH_ERRORS_OR_WARNINGS_VIEW_COLUMNS,
    ),
    (
        "x$statements_with_errors_or_warnings",
        STATEMENTS_WITH_ERRORS_OR_WARNINGS_VIEW_COLUMNS,
    ),
    (
        "statements_with_full_table_scans",
        STATEMENTS_WITH_FULL_TABLE_SCANS_VIEW_COLUMNS,
    ),
    (
        "x$statements_with_full_table_scans",
        STATEMENTS_WITH_FULL_TABLE_SCANS_VIEW_COLUMNS,
    ),
    ("x$ps_digest_avg_latency_distribution", &["cnt", "avg_us"]),
    (
        "x$ps_digest_95th_percentile_by_avg_us",
        &["avg_us", "percentile"],
    ),
    (
        "statements_with_runtimes_in_95th_percentile",
        STATEMENTS_WITH_RUNTIMES_IN_95TH_PERCENTILE_VIEW_COLUMNS,
    ),
    (
        "x$statements_with_runtimes_in_95th_percentile",
        STATEMENTS_WITH_RUNTIMES_IN_95TH_PERCENTILE_VIEW_COLUMNS,
    ),
    (
        "statements_with_sorting",
        STATEMENTS_WITH_SORTING_VIEW_COLUMNS,
    ),
    (
        "x$statements_with_sorting",
        STATEMENTS_WITH_SORTING_VIEW_COLUMNS,
    ),
    (
        "statements_with_temp_tables",
        STATEMENTS_WITH_TEMP_TABLES_VIEW_COLUMNS,
    ),
    (
        "x$statements_with_temp_tables",
        STATEMENTS_WITH_TEMP_TABLES_VIEW_COLUMNS,
    ),
    (
        "user_summary_by_file_io_type",
        USER_SUMMARY_BY_FILE_IO_TYPE_VIEW_COLUMNS,
    ),
    (
        "x$user_summary_by_file_io_type",
        USER_SUMMARY_BY_FILE_IO_TYPE_VIEW_COLUMNS,
    ),
    ("user_summary_by_file_io", &["user", "ios", "io_latency"]),
    ("x$user_summary_by_file_io", &["user", "ios", "io_latency"]),
    (
        "user_summary_by_statement_type",
        USER_SUMMARY_BY_STATEMENT_TYPE_VIEW_COLUMNS,
    ),
    (
        "x$user_summary_by_statement_type",
        USER_SUMMARY_BY_STATEMENT_TYPE_VIEW_COLUMNS,
    ),
    (
        "user_summary_by_statement_latency",
        USER_SUMMARY_BY_STATEMENT_LATENCY_VIEW_COLUMNS,
    ),
    (
        "x$user_summary_by_statement_latency",
        USER_SUMMARY_BY_STATEMENT_LATENCY_VIEW_COLUMNS,
    ),
    (
        "user_summary_by_stages",
        USER_SUMMARY_BY_STAGES_VIEW_COLUMNS,
    ),
    (
        "x$user_summary_by_stages",
        USER_SUMMARY_BY_STAGES_VIEW_COLUMNS,
    ),
    ("user_summary", USER_SUMMARY_VIEW_COLUMNS),
    ("x$user_summary", USER_SUMMARY_VIEW_COLUMNS),
    (
        "host_summary_by_file_io_type",
        HOST_SUMMARY_BY_FILE_IO_TYPE_VIEW_COLUMNS,
    ),
    (
        "x$host_summary_by_file_io_type",
        HOST_SUMMARY_BY_FILE_IO_TYPE_VIEW_COLUMNS,
    ),
    ("host_summary_by_file_io", &["host", "ios", "io_latency"]),
    ("x$host_summary_by_file_io", &["host", "ios", "io_latency"]),
    (
        "host_summary_by_statement_type",
        HOST_SUMMARY_BY_STATEMENT_TYPE_VIEW_COLUMNS,
    ),
    (
        "x$host_summary_by_statement_type",
        HOST_SUMMARY_BY_STATEMENT_TYPE_VIEW_COLUMNS,
    ),
    (
        "host_summary_by_statement_latency",
        HOST_SUMMARY_BY_STATEMENT_LATENCY_VIEW_COLUMNS,
    ),
    (
        "x$host_summary_by_statement_latency",
        HOST_SUMMARY_BY_STATEMENT_LATENCY_VIEW_COLUMNS,
    ),
    (
        "host_summary_by_stages",
        HOST_SUMMARY_BY_STAGES_VIEW_COLUMNS,
    ),
    (
        "x$host_summary_by_stages",
        HOST_SUMMARY_BY_STAGES_VIEW_COLUMNS,
    ),
    ("host_summary", HOST_SUMMARY_VIEW_COLUMNS),
    ("x$host_summary", HOST_SUMMARY_VIEW_COLUMNS),
    (
        "wait_classes_global_by_avg_latency",
        WAIT_CLASSES_GLOBAL_BY_AVG_LATENCY_VIEW_COLUMNS,
    ),
    (
        "x$wait_classes_global_by_avg_latency",
        WAIT_CLASSES_GLOBAL_BY_AVG_LATENCY_VIEW_COLUMNS,
    ),
    (
        "wait_classes_global_by_latency",
        WAIT_CLASSES_GLOBAL_BY_AVG_LATENCY_VIEW_COLUMNS,
    ),
    (
        "x$wait_classes_global_by_latency",
        WAIT_CLASSES_GLOBAL_BY_AVG_LATENCY_VIEW_COLUMNS,
    ),
    (
        "waits_by_user_by_latency",
        WAITS_BY_USER_BY_LATENCY_VIEW_COLUMNS,
    ),
    (
        "x$waits_by_user_by_latency",
        WAITS_BY_USER_BY_LATENCY_VIEW_COLUMNS,
    ),
    (
        "waits_by_host_by_latency",
        WAITS_BY_HOST_BY_LATENCY_VIEW_COLUMNS,
    ),
    (
        "x$waits_by_host_by_latency",
        WAITS_BY_HOST_BY_LATENCY_VIEW_COLUMNS,
    ),
    (
        "waits_global_by_latency",
        WAITS_GLOBAL_BY_LATENCY_VIEW_COLUMNS,
    ),
    (
        "x$waits_global_by_latency",
        WAITS_GLOBAL_BY_LATENCY_VIEW_COLUMNS,
    ),
    (
        "metrics",
        &["Variable_name", "Variable_value", "Type", "Enabled"],
    ),
    ("processlist", PROCESSLIST_VIEW_COLUMNS),
    ("x$processlist", PROCESSLIST_VIEW_COLUMNS),
    ("session", PROCESSLIST_VIEW_COLUMNS),
    ("x$session", PROCESSLIST_VIEW_COLUMNS),
    (
        "session_ssl_status",
        &[
            "thread_id",
            "ssl_version",
            "ssl_cipher",
            "ssl_sessions_reused",
        ],
    ),
];

const FUNCTION_NAMES: &[&str] = &[
    "extract_schema_from_file_name",
    "extract_table_from_file_name",
    "format_bytes",
    "format_path",
    "format_statement",
    "format_time",
    "list_add",
    "list_drop",
    "ps_is_account_enabled",
    "ps_is_consumer_enabled",
    "ps_is_instrument_default_enabled",
    "ps_is_instrument_default_timed",
    "ps_is_thread_instrumented",
    "ps_thread_id",
    "ps_thread_account",
    "ps_thread_stack",
    "ps_thread_trx_info",
    "quote_identifier",
    "sys_get_config",
    "version_major",
    "version_minor",
    "version_patch",
];

const PROCEDURE_NAMES: &[&str] = &[
    "create_synonym_db",
    "execute_prepared_stmt",
    "diagnostics",
    "ps_statement_avg_latency_histogram",
    "ps_trace_statement_digest",
    "ps_trace_thread",
    "ps_setup_disable_background_threads",
    "ps_setup_disable_consumer",
    "ps_setup_disable_instrument",
    "ps_setup_disable_thread",
    "ps_setup_enable_background_threads",
    "ps_setup_enable_consumer",
    "ps_setup_enable_instrument",
    "ps_setup_enable_thread",
    "ps_setup_reload_saved",
    "ps_setup_reset_to_default",
    "ps_setup_save",
    "ps_setup_show_disabled",
    "ps_setup_show_disabled_consumers",
    "ps_setup_show_disabled_instruments",
    "ps_setup_show_enabled",
    "ps_setup_show_enabled_consumers",
    "ps_setup_show_enabled_instruments",
    "ps_truncate_all_tables",
    "statement_performance_analyzer",
    "table_exists",
];

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ConfigRow {
    variable: String,
    value: Option<String>,
    set_time_ms: u64,
    set_by: Option<String>,
}

fn default_config() -> BTreeMap<String, ConfigRow> {
    let now = now_ms();
    let mut rows = BTreeMap::new();
    for (variable, value) in [
        ("statement_truncate_len", Some("64")),
        ("statement_performance_analyzer.limit", Some("100")),
        ("statement_performance_analyzer.view", None),
        ("diagnostics.allow_i_s_tables", Some("OFF")),
        ("diagnostics.include_raw", Some("OFF")),
        ("ps_thread_trx_info.max_length", Some("65535")),
    ] {
        rows.insert(
            variable.to_string(),
            ConfigRow {
                variable: variable.to_string(),
                value: value.map(str::to_string),
                set_time_ms: now,
                set_by: None,
            },
        );
    }
    rows
}

static CONFIG: LazyLock<RwLock<BTreeMap<PathBuf, BTreeMap<String, ConfigRow>>>> =
    LazyLock::new(|| RwLock::new(BTreeMap::new()));
static CONFIG_WRITE: Mutex<()> = Mutex::new(());

fn config_path(db: &Database) -> PathBuf {
    db.data_dir().join("system/sys_config.json")
}

fn config_io_error(action: &str, err: impl std::fmt::Display) -> QueryError {
    error(
        crate::error::catalog::ER_UNKNOWN_ERROR,
        format!("sys.sys_config {action} failed: {err}"),
    )
}

fn load_config(path: &Path) -> Result<BTreeMap<String, ConfigRow>, QueryError> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map_err(|err| config_io_error("load", err)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(default_config()),
        Err(err) => Err(config_io_error("load", err)),
    }
}

fn with_config<R>(
    db: &Database,
    f: impl FnOnce(&BTreeMap<String, ConfigRow>) -> R,
) -> Result<R, QueryError> {
    if let Some(rows) = CONFIG.read().get(db.data_dir()) {
        return Ok(f(rows));
    }
    let key = db.data_dir().to_path_buf();
    let mut all = CONFIG.write();
    if !all.contains_key(&key) {
        all.insert(key.clone(), load_config(&config_path(db))?);
    }
    Ok(f(all.get(&key).expect("configuration was loaded")))
}

fn mutate_config<R>(
    db: &Database,
    f: impl FnOnce(&mut BTreeMap<String, ConfigRow>) -> Result<R, QueryError>,
) -> Result<R, QueryError> {
    let _writer = CONFIG_WRITE.lock();
    let path = config_path(db);
    let mut next = with_config(db, Clone::clone)?;
    let result = f(&mut next)?;
    persist_config(&path, &next)?;
    CONFIG.write().insert(db.data_dir().to_path_buf(), next);
    Ok(result)
}

fn persist_config(path: &Path, rows: &BTreeMap<String, ConfigRow>) -> Result<(), QueryError> {
    let bytes = serde_json::to_vec(rows).map_err(|err| config_io_error("serialize", err))?;
    let parent = path.parent().expect("configuration path has parent");
    fs::create_dir_all(parent).map_err(|err| config_io_error("write", err))?;
    let tmp = path.with_extension("json.tmp");
    {
        let mut file = fs::File::create(&tmp).map_err(|err| config_io_error("write", err))?;
        file.write_all(&bytes)
            .map_err(|err| config_io_error("write", err))?;
        file.sync_all()
            .map_err(|err| config_io_error("sync", err))?;
    }
    fs::rename(&tmp, path).map_err(|err| config_io_error("rename", err))?;
    crate::platform::fs::sync_path(parent).map_err(|err| config_io_error("sync", err))?;
    Ok(())
}

static DEFINITIONS: LazyLock<Vec<SystemTableDef>> = LazyLock::new(|| {
    let mut defs = Vec::with_capacity(VIEW_COLUMNS.len() + 1);
    let config_columns = Box::leak(
        vec![
            varchar_col("variable", 128, false),
            varchar_col("value", 128, true),
            timestamp_col("set_time"),
            varchar_col("set_by", 128, true),
        ]
        .into_boxed_slice(),
    );
    let config_indexes = Box::leak(
        vec![SystemTableIndex {
            name: "PRIMARY",
            unique: true,
            columns: SYS_CONFIG_INDEX_COLUMNS,
            index_type: "BTREE",
            visible: true,
        }]
        .into_boxed_slice(),
    );
    defs.push(SystemTableDef {
        schema: "sys",
        name: "sys_config",
        table_type: "BASE TABLE",
        columns: config_columns,
        engine: "InnoDB",
        indexes: config_indexes,
        create_sql: Some(SYS_CONFIG_CREATE_SQL),
        hidden: false,
    });
    for (name, columns) in VIEW_COLUMNS {
        let columns = Box::leak(
            columns
                .iter()
                .map(|column| sys_view_column(name, column))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        );
        defs.push(SystemTableDef {
            schema: "sys",
            name,
            table_type: "VIEW",
            columns,
            engine: "",
            indexes: &[],
            create_sql: None,
            hidden: false,
        });
    }
    defs
});

pub(crate) fn definitions() -> &'static [SystemTableDef] {
    &DEFINITIONS
}

pub(crate) fn is_sys_config(schema: &str, table: &str) -> bool {
    schema.eq_ignore_ascii_case("sys") && table.eq_ignore_ascii_case("sys_config")
}

pub(crate) fn rows(
    db: &Database,
    table: &str,
    registry: &SessionRegistry,
) -> Result<Vec<Vec<Value>>, QueryError> {
    let lower = table.to_ascii_lowercase();
    match lower.as_str() {
        "sys_config" => sys_config_rows(db),
        "version" => Ok(vec![vec![
            s(SYS_VERSION),
            s(crate::server_version::MYSQL_COMPAT_VERSION),
        ]]),
        "schema_object_overview" => Ok(schema_object_overview_rows(db)),
        "metrics" => Ok(metrics_rows(db, registry)),
        "processlist" | "x$processlist" => Ok(processlist_rows(db, registry, false)),
        "session" | "x$session" => Ok(processlist_rows(db, registry, true)),
        other
            if VIEW_COLUMNS
                .iter()
                .any(|(name, _)| name.eq_ignore_ascii_case(other)) =>
        {
            Ok(Vec::new())
        }
        other => Err(error(
            crate::error::catalog::ER_NO_SUCH_TABLE,
            format!("Table 'sys.{other}' doesn't exist"),
        )),
    }
}

pub(crate) fn view_information_schema_rows() -> Vec<Vec<Value>> {
    VIEW_COLUMNS
        .iter()
        .map(|(name, _)| {
            vec![
                s("def"),
                s("sys"),
                s(name),
                s(&view_select_sql(name)),
                s("NONE"),
                s("NO"),
                s("mysql.sys@localhost"),
                s("INVOKER"),
                s(crate::charset::DEFAULT_CHARSET),
                s(crate::charset::DEFAULT_COLLATION),
            ]
        })
        .collect()
}

pub(crate) fn create_view_sql(name: &str, columns: &[SystemColumn]) -> String {
    let cols = columns
        .iter()
        .map(|col| format!("`{}`", col.name))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "CREATE ALGORITHM=UNDEFINED DEFINER=`mysql.sys`@`localhost` SQL SECURITY INVOKER VIEW `{name}` ({cols}) AS {}",
        view_select_sql(name)
    )
}

pub(crate) fn routine_definition(
    current_database: &str,
    kind: RoutineKindSchema,
    name: &str,
) -> Option<RoutineSchema> {
    let (database, routine_name) = split_table_name(current_database, name);
    if !database.eq_ignore_ascii_case("sys") {
        return None;
    }
    match kind {
        RoutineKindSchema::Function => FUNCTION_NAMES
            .iter()
            .any(|n| n.eq_ignore_ascii_case(&routine_name))
            .then(|| routine_schema(&routine_name, RoutineKindSchema::Function)),
        RoutineKindSchema::Procedure => PROCEDURE_NAMES
            .iter()
            .any(|n| n.eq_ignore_ascii_case(&routine_name))
            .then(|| routine_schema(&routine_name, RoutineKindSchema::Procedure)),
    }
}

pub(crate) fn routine_schemas() -> Vec<RoutineSchema> {
    FUNCTION_NAMES
        .iter()
        .map(|name| routine_schema(name, RoutineKindSchema::Function))
        .chain(
            PROCEDURE_NAMES
                .iter()
                .map(|name| routine_schema(name, RoutineKindSchema::Procedure)),
        )
        .collect()
}

pub(crate) fn trigger_schemas() -> Vec<TriggerSchema> {
    vec![
        sys_config_trigger("sys_config_insert_set_user", TriggerEventSchema::Insert),
        sys_config_trigger("sys_config_update_set_user", TriggerEventSchema::Update),
    ]
}

pub(crate) fn execute_call(
    db: &Database,
    session: &mut SessionContext,
    call: &RoutineCall,
) -> Option<Result<QueryResponse, QueryError>> {
    let (database, proc_name) = split_table_name(&session.current_database, &call.name);
    if !database.eq_ignore_ascii_case("sys")
        || !PROCEDURE_NAMES
            .iter()
            .any(|name| name.eq_ignore_ascii_case(&proc_name))
    {
        return None;
    }
    Some(match proc_name.to_ascii_lowercase().as_str() {
        "table_exists" => execute_table_exists(db, session, &call.args),
        "ps_setup_enable_consumer" => setup_toggle("setup_consumers", &call.args, "ENABLED", true)
            .map(|_| QueryResponse::MultiResult(vec![QueryResponse::Ok])),
        "ps_setup_disable_consumer" => {
            setup_toggle("setup_consumers", &call.args, "ENABLED", false)
                .map(|_| QueryResponse::MultiResult(vec![QueryResponse::Ok]))
        }
        "ps_setup_enable_instrument" => setup_toggle_instrument(&call.args, true)
            .map(|_| QueryResponse::MultiResult(vec![QueryResponse::Ok])),
        "ps_setup_disable_instrument" => setup_toggle_instrument(&call.args, false)
            .map(|_| QueryResponse::MultiResult(vec![QueryResponse::Ok])),
        _ => Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            format!("PROCEDURE sys.{proc_name} is not implemented"),
        )),
    })
}

pub(crate) fn config_value(
    variable: &str,
    default_value: Option<String>,
) -> Result<Option<String>, QueryError> {
    let db = crate::engine::executor::mem_budget::current_reclaim_database()
        .ok_or_else(|| config_io_error("read", "no active database"))?;
    with_config(&db, |rows| {
        rows.get(&variable.to_ascii_lowercase())
            .and_then(|row| row.value.clone())
            .or(default_value)
    })
}

pub(crate) fn insert_sys_config(
    db: &Database,
    session: &SessionContext,
    stmt: &Insert,
) -> Result<u64, QueryError> {
    let rows = match &stmt.source {
        InsertSource::Values(rows) => rows.as_slice(),
        InsertSource::Query(_) => {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "INSERT ... SELECT into sys.sys_config is not supported",
            ))
        }
    };
    let columns = stmt.columns.clone().unwrap_or_else(|| {
        vec![
            "variable".to_string(),
            "value".to_string(),
            "set_time".to_string(),
            "set_by".to_string(),
        ]
    });
    mutate_config(db, |next| {
        let mut affected = 0;
        for values in rows {
            if values.len() != columns.len() {
                return Err(error(
                    crate::error::catalog::ER_WRONG_VALUE_COUNT_ON_ROW,
                    "Column count doesn't match value count at row 1",
                ));
            }
            let mut variable = None;
            let mut value = None;
            let mut set_by = None;
            let mut set_time_ms = None;
            for (column, expr) in columns.iter().zip(values.iter()) {
                let v = crate::engine::expr::eval_const_runtime(expr)?;
                match column.to_ascii_lowercase().as_str() {
                    "variable" => variable = Some(value_string_required(v, "variable")?),
                    "value" => value = nullable_string(v),
                    "set_by" => set_by = nullable_string(v),
                    "set_time" => set_time_ms = Some(config_time_ms(&v)?),
                    other => return Err(unknown_sys_config_column(other)),
                }
            }
            let variable = variable.ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NO_DEFAULT_FOR_FIELD,
                    "Field 'variable' doesn't have a default value",
                )
            })?;
            let key = variable.to_ascii_lowercase();
            let candidate = ConfigRow {
                variable,
                value,
                set_time_ms: set_time_ms.unwrap_or_else(now_ms),
                set_by,
            };
            if next.contains_key(&key) && stmt.ignore && stmt.on_duplicate_key_update.is_none() {
                continue;
            }
            if let Some(assignments) = &stmt.on_duplicate_key_update {
                if let Some(current) = next.get(&key).cloned() {
                    let mut updated = current.clone();
                    apply_config_assignments(&mut updated, assignments, Some(&candidate))?;
                    apply_sys_config_trigger(session, &mut updated);
                    let new_key = updated.variable.to_ascii_lowercase();
                    if new_key != key && next.contains_key(&new_key) {
                        return Err(error(
                            crate::error::catalog::ER_DUP_ENTRY,
                            format!("Duplicate entry '{}' for key 'PRIMARY'", updated.variable),
                        ));
                    }
                    let changed = updated != current;
                    next.remove(&key);
                    next.insert(new_key, updated);
                    affected += if changed { 2 } else { 0 };
                    continue;
                }
            }
            if next.contains_key(&key) && !stmt.replace {
                return Err(error(
                    crate::error::catalog::ER_DUP_ENTRY,
                    format!("Duplicate entry '{}' for key 'PRIMARY'", candidate.variable),
                ));
            }
            let mut row = candidate;
            apply_sys_config_trigger(session, &mut row);
            let replaced = next.insert(key, row).is_some();
            affected += if replaced { 2 } else { 1 };
        }
        Ok(affected)
    })
}

pub(crate) fn update_sys_config(
    db: &Database,
    session: &SessionContext,
    stmt: &Update,
) -> Result<u64, QueryError> {
    mutate_config(db, |next| {
        let mut affected = 0;
        let keys = next.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            let Some(current) = next.get(&key).cloned() else {
                continue;
            };
            if !matches_config_predicate(stmt.selection.as_ref(), &current)? {
                continue;
            }
            let mut row = current;
            let old_key = key;
            apply_config_assignments(&mut row, &stmt.assignments, None)?;
            if !stmt
                .assignments
                .iter()
                .any(|a| a.column.eq_ignore_ascii_case("set_time"))
            {
                row.set_time_ms = now_ms();
            }
            apply_sys_config_trigger(session, &mut row);
            let new_key = row.variable.to_ascii_lowercase();
            if new_key != old_key && next.contains_key(&new_key) {
                return Err(error(
                    crate::error::catalog::ER_DUP_ENTRY,
                    format!("Duplicate entry '{}' for key 'PRIMARY'", row.variable),
                ));
            }
            next.remove(&old_key);
            next.insert(new_key, row);
            affected += 1;
        }
        Ok(affected)
    })
}

pub(crate) fn delete_sys_config(db: &Database, stmt: &Delete) -> Result<u64, QueryError> {
    mutate_config(db, |config| {
        let mut keys = Vec::new();
        for (key, row) in config.iter() {
            if matches_config_predicate(stmt.selection.as_ref(), row)? {
                keys.push(key.clone());
            }
        }
        let affected = keys.len() as u64;
        for key in keys.drain(..) {
            config.remove(&key);
        }
        Ok(affected)
    })
}

pub(crate) fn truncate_sys_config(db: &Database) -> Result<(), QueryError> {
    mutate_config(db, |config| {
        config.clear();
        Ok(())
    })
}

#[cfg(test)]
pub(crate) fn normalize_sys_function_name(name: &str) -> String {
    if name
        .as_bytes()
        .get(..4)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"sys."))
    {
        name[4..].to_ascii_uppercase()
    } else {
        name.to_ascii_uppercase()
    }
}

/// Zero-allocation variant of [`normalize_sys_function_name`] for per-row
/// dispatch: uppercases the (possibly `sys.`-prefixed) name into an inline
/// buffer. Names longer than the buffer (no built-in comes close) fall back
/// to one heap allocation, preserving behaviour.
pub(crate) struct NormalizedFuncName {
    buf: [u8; 64],
    len: usize,
    heap: Option<String>,
}

impl NormalizedFuncName {
    pub(crate) fn new(name: &str) -> Self {
        let stripped = if name
            .as_bytes()
            .get(..4)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"sys."))
        {
            &name[4..]
        } else {
            name
        };
        Self::uppercase(stripped)
    }

    /// ASCII case normalization without changing the function's namespace.
    pub(crate) fn uppercase(stripped: &str) -> Self {
        if stripped.len() <= 64 {
            let mut buf = [0u8; 64];
            for (dst, src) in buf.iter_mut().zip(stripped.bytes()) {
                *dst = src.to_ascii_uppercase();
            }
            NormalizedFuncName {
                buf,
                len: stripped.len(),
                heap: None,
            }
        } else {
            NormalizedFuncName {
                buf: [0u8; 64],
                len: 0,
                heap: Some(stripped.to_ascii_uppercase()),
            }
        }
    }

    pub(crate) fn as_str(&self) -> &str {
        match &self.heap {
            Some(s) => s,
            // ASCII-uppercasing the bytes of a valid UTF-8 string cannot
            // break UTF-8: only bytes < 0x80 are rewritten.
            None => {
                std::str::from_utf8(&self.buf[..self.len]).expect("ascii uppercase preserves utf8")
            }
        }
    }
}

fn varchar_col(name: &'static str, len: i64, nullable: bool) -> SystemColumn {
    SystemColumn {
        name,
        result_type: ColumnType::VarString,
        data_type: "varchar",
        column_type_sql: Box::leak(format!("varchar({len})").into_boxed_str()),
        nullable,
        char_len: Some(len),
        octet_len: Some(len * 4),
        numeric_precision: None,
        numeric_scale: None,
        datetime_precision: None,
    }
}

fn sys_view_column(view: &str, column: &'static str) -> SystemColumn {
    mysql80_sys_view_columns()
        .get(&(view, column))
        .copied()
        .unwrap_or_else(|| varchar_col(column, 255, true))
}

/// Metadata captured from MySQL 8.0.46's sys 2.1.3 schema. The source file
/// covers every registered sys view column, including views whose PFS producer
/// mydb does not yet implement.
fn mysql80_sys_view_columns() -> &'static HashMap<(&'static str, &'static str), SystemColumn> {
    static COLUMNS: LazyLock<HashMap<(&'static str, &'static str), SystemColumn>> =
        LazyLock::new(|| {
            let mut columns = HashMap::new();
            for line in include_str!("sys_schema_mysql80_columns.tsv").lines() {
                let mut fields = line.split('\t');
                let view = fields.next().expect("sys view name");
                let name = fields.next().expect("sys column name");
                let data_type = fields.next().expect("sys data type");
                let column_type_sql = fields.next().expect("sys column type");
                let nullable = fields.next().expect("sys nullability") == "YES";
                let parse_optional = |text: &str| text.parse::<i64>().ok();
                let char_len = fields.next().and_then(parse_optional);
                let octet_len = fields.next().and_then(parse_optional);
                let numeric_precision = fields.next().and_then(parse_optional);
                let numeric_scale = fields.next().and_then(parse_optional);
                let datetime_precision = fields.next().and_then(parse_optional);
                let result_type = match data_type {
                    "bigint" => ColumnType::LongLong,
                    "int" => ColumnType::Long,
                    "decimal" => ColumnType::Decimal {
                        precision: numeric_precision
                            .and_then(|n| u8::try_from(n).ok())
                            .unwrap_or(65),
                        scale: numeric_scale
                            .and_then(|n| u8::try_from(n).ok())
                            .unwrap_or(0),
                    },
                    "datetime" => ColumnType::DateTime,
                    "timestamp" => ColumnType::Timestamp,
                    "time" => ColumnType::Time,
                    _ => ColumnType::VarString,
                };
                columns.insert(
                    (view, name),
                    SystemColumn {
                        name,
                        result_type,
                        data_type,
                        column_type_sql,
                        nullable,
                        char_len,
                        octet_len,
                        numeric_precision,
                        numeric_scale,
                        datetime_precision,
                    },
                );
            }
            columns
        });
    &COLUMNS
}

fn timestamp_col(name: &'static str) -> SystemColumn {
    SystemColumn {
        name,
        result_type: ColumnType::DateTime,
        data_type: "timestamp",
        column_type_sql: "timestamp",
        nullable: false,
        char_len: None,
        octet_len: None,
        numeric_precision: None,
        numeric_scale: None,
        datetime_precision: Some(0),
    }
}

struct RoutineReference {
    params: Vec<RoutineParamSchema>,
    return_type: Option<DataTypeKind>,
    return_type_sql: Option<String>,
    return_unsigned: bool,
    deterministic: bool,
    sql_access: RoutineSqlDataAccessSchema,
}

fn mysql80_sys_routines() -> &'static HashMap<&'static str, RoutineReference> {
    static ROUTINES: LazyLock<HashMap<&'static str, RoutineReference>> = LazyLock::new(|| {
        let mut routines = HashMap::new();
        for line in include_str!("sys_schema_mysql80_routines.tsv").lines() {
            let mut fields = line.split('\t');
            let name = fields.next().expect("sys routine name");
            let kind = fields.next().expect("sys routine kind");
            let data_type = fields.next().expect("sys return data type");
            let dtd = fields.next().expect("sys return SQL type");
            let deterministic = fields.next().expect("sys determinism") == "YES";
            let sql_access = match fields.next().expect("sys SQL access") {
                "NO SQL" => RoutineSqlDataAccessSchema::NoSql,
                "READS SQL DATA" => RoutineSqlDataAccessSchema::ReadsSqlData,
                "MODIFIES SQL DATA" => RoutineSqlDataAccessSchema::ModifiesSqlData,
                "CONTAINS SQL" => RoutineSqlDataAccessSchema::ContainsSql,
                other => panic!("unknown sys SQL access: {other}"),
            };
            routines.insert(
                name,
                RoutineReference {
                    params: Vec::new(),
                    return_type: (kind == "FUNCTION")
                        .then(|| mysql80_routine_data_type(data_type, dtd)),
                    return_type_sql: (kind == "FUNCTION").then(|| dtd.to_string()),
                    return_unsigned: dtd.contains("unsigned"),
                    deterministic,
                    sql_access,
                },
            );
        }
        for line in include_str!("sys_schema_mysql80_parameters.tsv").lines() {
            let mut fields = line.split('\t');
            let name = fields.next().expect("sys parameter routine");
            let ordinal = fields.next().expect("sys parameter ordinal");
            if ordinal == "0" {
                continue;
            }
            let mode = match fields.next().expect("sys parameter mode") {
                "IN" => RoutineParamModeSchema::In,
                "OUT" => RoutineParamModeSchema::Out,
                "INOUT" => RoutineParamModeSchema::InOut,
                other => panic!("unknown sys parameter mode: {other}"),
            };
            let param_name = fields.next().expect("sys parameter name");
            let data_type = fields.next().expect("sys parameter type");
            let dtd = fields.next().expect("sys parameter SQL type");
            routines
                .get_mut(name)
                .expect("parameter routine exists")
                .params
                .push(RoutineParamSchema {
                    name: param_name.to_string(),
                    mode,
                    data_type: mysql80_routine_data_type(data_type, dtd),
                    unsigned: dtd.contains("unsigned"),
                    data_type_sql: dtd.to_string(),
                });
        }
        routines
    });
    &ROUTINES
}

fn mysql80_routine_data_type(data_type: &str, dtd: &str) -> DataTypeKind {
    let args = dtd
        .split_once('(')
        .and_then(|(_, rest)| rest.split_once(')'))
        .map(|(args, _)| args);
    match data_type {
        "varchar" => DataTypeKind::VarChar {
            len: args.and_then(|s| s.parse().ok()),
        },
        "text" => DataTypeKind::Text { max_len: 65_535 },
        "mediumtext" => DataTypeKind::Text {
            max_len: 16_777_215,
        },
        "longtext" => DataTypeKind::Text { max_len: u32::MAX },
        "tinyint" => DataTypeKind::TinyInt,
        "int" => DataTypeKind::Int,
        "bigint" => DataTypeKind::BigInt,
        "decimal" => {
            let mut numbers = args.unwrap_or("").split(',');
            DataTypeKind::Decimal {
                precision: numbers.next().and_then(|s| s.parse().ok()),
                scale: numbers.next().and_then(|s| s.parse().ok()),
            }
        }
        "enum" | "set" => {
            let values = args
                .unwrap_or("")
                .split(',')
                .map(|value| value.trim().trim_matches('\'').to_string())
                .collect();
            if data_type == "enum" {
                DataTypeKind::Enum { values }
            } else {
                DataTypeKind::Set { values }
            }
        }
        other => panic!("unknown sys routine data type: {other}"),
    }
}

fn routine_schema(name: &str, kind: RoutineKindSchema) -> RoutineSchema {
    let canonical_name = name.to_ascii_lowercase();
    let reference = mysql80_sys_routines()
        .get(canonical_name.as_str())
        .expect("registered sys routine has MySQL 8.0 metadata");
    RoutineSchema {
        database: "sys".to_string(),
        name: canonical_name,
        kind,
        definer_user: SYS_DEFINER_USER.to_string(),
        definer_host: SYS_DEFINER_HOST.to_string(),
        security_type: RoutineSecuritySchema::Invoker,
        params: reference.params.clone(),
        return_type: reference.return_type.clone(),
        return_type_sql: reference.return_type_sql.clone(),
        return_unsigned: reference.return_unsigned,
        deterministic: reference.deterministic,
        sql_data_access: reference.sql_access.clone(),
        comment: String::new(),
        sql_mode: SYS_ROUTINE_SQL_MODE.to_string(),
        body_sql: "BEGIN SIGNAL SQLSTATE '0A000' SET MESSAGE_TEXT = 'Native mydb sys routine source is not portable'; END".to_string(),
        schema_version: 1,
        created_ms: 0,
        updated_ms: 0,
        character_set_client: crate::charset::DEFAULT_CHARSET.to_string(),
        collation_connection: crate::charset::DEFAULT_COLLATION.to_string(),
        database_collation: crate::charset::DEFAULT_COLLATION.to_string(),
    }
}

fn sys_config_trigger(name: &str, event: TriggerEventSchema) -> TriggerSchema {
    TriggerSchema {
        database: "sys".to_string(),
        name: name.to_string(),
        table_database: "sys".to_string(),
        table_name: "sys_config".to_string(),
        timing: TriggerTimingSchema::Before,
        event,
        definer_user: SYS_DEFINER_USER.to_string(),
        definer_host: SYS_DEFINER_HOST.to_string(),
        body_sql: "IF @sys.ignore_sys_config_triggers != true AND NEW.set_by IS NULL THEN SET NEW.set_by = USER(); END IF".to_string(),
        create_sql: String::new(),
        action_order: 1,
        schema_version: 1,
        created_ms: 0,
        sql_mode: String::new(),
        character_set_client: crate::charset::DEFAULT_CHARSET.to_string(),
        collation_connection: crate::charset::DEFAULT_COLLATION.to_string(),
        database_collation: crate::charset::DEFAULT_COLLATION.to_string(),
    }
}

fn sys_config_rows(db: &Database) -> Result<Vec<Vec<Value>>, QueryError> {
    with_config(db, |config| {
        config.values().map(config_row_values).collect()
    })
}

fn schema_object_overview_rows(db: &Database) -> Vec<Vec<Value>> {
    let mut counts: BTreeMap<(String, String), i64> = BTreeMap::new();
    let catalog = db.catalog();
    for table in catalog.tables.values() {
        *counts
            .entry((table.database.clone(), table.table_type.clone()))
            .or_default() += 1;
    }
    for view in catalog.views.values() {
        *counts
            .entry((view.database.clone(), "VIEW".to_string()))
            .or_default() += 1;
    }
    for routine in catalog.routines.values() {
        *counts
            .entry((routine.database.clone(), routine.kind.as_str().to_string()))
            .or_default() += 1;
    }
    for trigger in catalog.triggers.values() {
        *counts
            .entry((trigger.database.clone(), "TRIGGER".to_string()))
            .or_default() += 1;
    }
    for event in db.all_events() {
        *counts
            .entry((event.database, "EVENT".to_string()))
            .or_default() += 1;
    }
    for def in definitions() {
        if def.hidden {
            continue;
        }
        *counts
            .entry((def.schema.to_string(), def.table_type.to_string()))
            .or_default() += 1;
    }
    counts
        .into_iter()
        .map(|((schema, object_type), count)| vec![s(&schema), s(&object_type), Value::Int(count)])
        .collect()
}

fn metrics_rows(db: &Database, registry: &SessionRegistry) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    let mut status = crate::engine::expr::with_active_session(|session| {
        session
            .map(|session| {
                crate::engine::system::status_pairs(
                    session,
                    Some(db),
                    Some(registry),
                    crate::parser::ShowScope::Global,
                )
            })
            .unwrap_or_else(|| crate::engine::perf_schema::global_status_pairs(db, Some(registry)))
    });
    status.retain(|(name, _)| {
        crate::engine::status_counters::status_var_def(name)
            .is_some_and(|definition| definition.pfs_visible)
    });
    status.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    for (name, value) in status {
        rows.push(vec![s(&name), s(&value), s("Global Status"), s("YES")]);
    }
    for row in crate::engine::sysvar::snapshot(crate::engine::sysvar::Scope::Global, None, Some(db))
    {
        rows.push(vec![
            s(row.name),
            s(&row.value),
            s("Global Variable"),
            s(""),
        ]);
    }
    rows
}

fn processlist_rows(
    db: &Database,
    registry: &SessionRegistry,
    sessions_only: bool,
) -> Vec<Vec<Value>> {
    let mut processes = registry.snapshot();
    crate::engine::perf_schema::scope_processlist_to_account(db, &mut processes);
    if sessions_only {
        processes.retain(|process| process.command != "Daemon");
    }
    processes.sort_by_key(|p| p.id);
    processes
        .into_iter()
        .map(|p| {
            let info = p.info.unwrap_or_default();
            vec![
                Value::UInt(p.id as u64),
                Value::UInt(p.id as u64),
                s(&sys_process_user(&p.user, &p.host, &p.command)),
                p.database.map(Value::String).unwrap_or(Value::Null),
                s(&p.command),
                p.state.map(Value::String).unwrap_or(Value::Null),
                Value::Int(p.time_secs as i64),
                if info.is_empty() {
                    Value::Null
                } else {
                    s(&info)
                },
                s("PRIMARY"),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
                s("NO"),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                s(if p.current_txn.is_some() { "NO" } else { "YES" }),
                Value::Null,
                Value::Null,
            ]
        })
        .collect()
}

fn sys_process_user(user: &str, host: &str, command: &str) -> String {
    if command == "Daemon" {
        return if user == "event_scheduler" {
            "sql/event_scheduler".to_string()
        } else {
            user.to_string()
        };
    }
    let host_without_port = host
        .rsplit_once(':')
        .filter(|(_, port)| port.parse::<u16>().is_ok())
        .map(|(address, _)| address.trim_start_matches('[').trim_end_matches(']'))
        .unwrap_or(host);
    format!("{user}@{host_without_port}")
}

fn view_select_sql(name: &str) -> String {
    if name.eq_ignore_ascii_case("version") {
        return "SELECT '2.1.3' AS `sys_version`, VERSION() AS `mysql_version`".to_string();
    }
    if name.eq_ignore_ascii_case("session") {
        return "SELECT * FROM `sys`.`processlist` WHERE `conn_id` IS NOT NULL AND `command` != 'Daemon'".to_string();
    }
    if name.eq_ignore_ascii_case("x$session") {
        return "SELECT * FROM `sys`.`x$processlist` WHERE `conn_id` IS NOT NULL AND `command` != 'Daemon'".to_string();
    }
    let columns = VIEW_COLUMNS
        .iter()
        .find(|(view, _)| view.eq_ignore_ascii_case(name))
        .map(|(_, cols)| *cols)
        .unwrap_or(&[]);
    let projection = columns
        .iter()
        .map(|col| format!("NULL AS `{col}`"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT {projection} WHERE FALSE")
}

fn execute_table_exists(
    db: &Database,
    session: &mut SessionContext,
    args: &[Expr],
) -> Result<QueryResponse, QueryError> {
    if args.len() != 3 {
        return Err(error(
            crate::error::catalog::ER_SP_WRONG_NO_OF_ARGS,
            "Incorrect number of arguments for PROCEDURE table_exists",
        ));
    }
    let schema = value_to_string(crate::engine::expr::eval_const_runtime(&args[0])?);
    let table = value_to_string(crate::engine::expr::eval_const_runtime(&args[1])?);
    let exists = if session.has_temp_table(&format!("{schema}.{table}")) {
        "TEMPORARY"
    } else if let Some(def) = crate::engine::information_schema::find_system_table(&schema, &table)
    {
        def.table_type
    } else if db.table_in_database(&schema, &table).is_ok() {
        "BASE TABLE"
    } else if db.view_definition(&schema, &table).is_some() {
        "VIEW"
    } else {
        ""
    };
    match &args[2] {
        Expr::UserVariable(name) => {
            session.set_user_variable(name, Value::String(exists.to_string()))
        }
        _ => {
            return Err(error(
                crate::error::catalog::ER_SP_NOT_VAR_ARG,
                "OUT or INOUT argument must be a variable",
            ))
        }
    }
    Ok(QueryResponse::MultiResult(vec![QueryResponse::Ok]))
}

fn setup_toggle(
    table: &str,
    args: &[Expr],
    column: &str,
    enabled: bool,
) -> Result<u64, QueryError> {
    if args.len() != 1 {
        return Err(error(
            crate::error::catalog::ER_SP_WRONG_NO_OF_ARGS,
            format!(
                "Incorrect number of arguments for PROCEDURE sys.ps_setup_{}_{}",
                if enabled { "enable" } else { "disable" },
                if table == "setup_consumers" {
                    "consumer"
                } else {
                    "instrument"
                }
            ),
        ));
    }
    let pattern_value = crate::engine::expr::eval_const_runtime(&args[0])?;
    if matches!(pattern_value, Value::Null) {
        return Ok(0);
    }
    let pattern = format!("%{}%", value_to_string(pattern_value));
    crate::engine::perf_setup::update(
        table,
        &[Assignment {
            column: column.to_string(),
            value: Expr::BoundValue(Value::String(
                if enabled { "YES" } else { "NO" }.to_string(),
            )),
            table_qualifier: None,
        }],
        Some(&Expr::Like {
            expr: Box::new(Expr::Identifier("NAME".to_string())),
            pattern: Box::new(Expr::BoundValue(Value::String(pattern))),
            negated: false,
            escape_char: Some('\\'),
        }),
    )
}

fn setup_toggle_instrument(args: &[Expr], enabled: bool) -> Result<u64, QueryError> {
    setup_toggle("setup_instruments", args, "ENABLED", enabled)
}

fn matches_config_predicate(selection: Option<&Expr>, row: &ConfigRow) -> Result<bool, QueryError> {
    let Some(expr) = selection else {
        return Ok(true);
    };
    let values = config_row_values(row);
    Ok(crate::engine::expr::truthy(
        &crate::engine::expr::eval_expr(expr, config_schema(), &values)?,
    ))
}

fn config_schema() -> &'static crate::storage::TableSchema {
    static CONFIG_SCHEMA: LazyLock<crate::storage::TableSchema> = LazyLock::new(|| {
        let columns = crate::engine::information_schema::columns_for_table_name("sys.sys_config")
            .expect("sys_config columns are registered");
        crate::storage::TableSchema::synthetic_for_planning("sys.sys_config", &columns)
    });
    &CONFIG_SCHEMA
}

fn config_row_values(row: &ConfigRow) -> Vec<Value> {
    vec![
        s(&row.variable),
        row.value.clone().map(Value::String).unwrap_or(Value::Null),
        mysql_time_value(row.set_time_ms),
        row.set_by.clone().map(Value::String).unwrap_or(Value::Null),
    ]
}

fn apply_config_assignments(
    row: &mut ConfigRow,
    assignments: &[Assignment],
    inserted: Option<&ConfigRow>,
) -> Result<(), QueryError> {
    for assignment in assignments {
        let value = if let (Some(inserted), Expr::Function { name, args, .. }) =
            (inserted, &assignment.value)
        {
            if name.eq_ignore_ascii_case("VALUES") && args.len() == 1 {
                let name = match &args[0] {
                    Expr::Identifier(name) => name.as_str(),
                    Expr::CompoundIdentifier(parts) => {
                        parts.last().map(String::as_str).unwrap_or("")
                    }
                    _ => "",
                };
                let index = config_schema().column_index(name)?;
                config_row_values(inserted)[index].clone()
            } else {
                crate::engine::expr::eval_expr(
                    &assignment.value,
                    config_schema(),
                    &config_row_values(row),
                )?
            }
        } else {
            crate::engine::expr::eval_expr(
                &assignment.value,
                config_schema(),
                &config_row_values(row),
            )?
        };
        match assignment.column.to_ascii_lowercase().as_str() {
            "variable" => row.variable = value_string_required(value, "variable")?,
            "value" => row.value = nullable_string(value),
            "set_by" => row.set_by = nullable_string(value),
            "set_time" => row.set_time_ms = config_time_ms(&value)?,
            other => return Err(unknown_sys_config_column(other)),
        }
    }
    Ok(())
}

fn config_time_ms(value: &Value) -> Result<u64, QueryError> {
    let dt = crate::engine::expr::parse_datetime(value)?;
    let timestamp = Local
        .from_local_datetime(&dt)
        .single()
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_TRUNCATED_WRONG_VALUE,
                "Invalid sys_config.set_time",
            )
        })?
        .timestamp_millis();
    u64::try_from(timestamp).map_err(|_| {
        error(
            crate::error::catalog::ER_TRUNCATED_WRONG_VALUE,
            "Invalid sys_config.set_time",
        )
    })
}

fn apply_sys_config_trigger(session: &SessionContext, row: &mut ConfigRow) {
    let ignore = session
        .user_variable("sys.ignore_sys_config_triggers")
        .is_some_and(crate::engine::expr::truthy);
    if !ignore && row.set_by.is_none() {
        row.set_by = Some(format!("{}@{}", session.login_user, session.host));
    }
}

fn value_string_required(value: Value, column: &str) -> Result<String, QueryError> {
    match value {
        Value::Null => Err(error(
            crate::error::catalog::ER_BAD_NULL_ERROR,
            format!("Column '{column}' cannot be null"),
        )),
        value => Ok(value_to_string(value)),
    }
}

fn nullable_string(value: Value) -> Option<String> {
    match value {
        Value::Null => None,
        value => Some(value_to_string(value)),
    }
}

fn unknown_sys_config_column(column: &str) -> QueryError {
    error(
        crate::error::catalog::ER_BAD_FIELD_ERROR,
        format!("Unknown column '{column}' in 'sys_config'"),
    )
}

fn value_to_string(value: Value) -> String {
    crate::storage::value_to_string(&value)
}

fn mysql_time_value(unix_ms: u64) -> Value {
    if unix_ms == 0 {
        return Value::Null;
    }
    match Local.timestamp_millis_opt(unix_ms as i64).single() {
        Some(dt) => Value::String(dt.format("%Y-%m-%d %H:%M:%S").to_string()),
        None => Value::Null,
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn s(value: &str) -> Value {
    Value::String(value.to_string())
}

#[cfg(test)]
mod func_name_tests {
    use super::{
        mysql80_sys_routines, mysql80_sys_view_columns, normalize_sys_function_name,
        sys_process_user, NormalizedFuncName, FUNCTION_NAMES, PROCEDURE_NAMES, VIEW_COLUMNS,
    };

    #[test]
    fn process_user_omits_client_port() {
        assert_eq!(
            sys_process_user("root", "localhost:50123", "Query"),
            "root@localhost"
        );
        assert_eq!(sys_process_user("root", "[::1]:50123", "Query"), "root@::1");
        assert_eq!(
            sys_process_user("event_scheduler", "localhost", "Daemon"),
            "sql/event_scheduler"
        );
    }

    #[test]
    fn registered_sys_routines_match_mysql80() {
        let reference = mysql80_sys_routines();
        assert_eq!(
            FUNCTION_NAMES.len() + PROCEDURE_NAMES.len(),
            reference.len()
        );
        for name in FUNCTION_NAMES.iter().chain(PROCEDURE_NAMES) {
            assert!(
                reference.contains_key(name),
                "missing sys routine metadata for {name}"
            );
        }
    }

    #[test]
    fn every_sys_view_column_has_mysql80_metadata() {
        let reference = mysql80_sys_view_columns();
        for (view, columns) in VIEW_COLUMNS {
            for column in *columns {
                assert!(
                    reference.contains_key(&(*view, *column)),
                    "missing {view}.{column}"
                );
            }
        }
    }

    #[test]
    fn normalized_func_name_matches_allocating_path() {
        let cases = [
            "concat",
            "CONCAT",
            "sys.format_bytes",
            "SYS.VERSION_MAJOR",
            "st_x",
            "ab系统",
            "über_func",
            "",
            "sys.",
            "a_very_long_function_name_that_certainly_exceeds_the_inline_buffer_capacity_0123456789",
        ];
        for name in cases {
            let fast = NormalizedFuncName::new(name);
            assert_eq!(
                fast.as_str(),
                normalize_sys_function_name(name),
                "mismatch for {name:?}"
            );
        }
    }
}
