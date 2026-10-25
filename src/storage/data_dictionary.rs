//! Compiled-in data-dictionary (system catalog) table definitions.
//!
//! mydb stores its catalog the way MySQL 8.0 does: as rows in regular
//! transactional heap tables (`mysql.tables`, `mysql.columns`, ...).
//! Decoding any heap row requires a [`RowCodec`], which is derived from a
//! [`TableSchema`] — so the schemas of the dictionary tables themselves
//! cannot be read from disk without already knowing them. We break that
//! chicken-and-egg by defining the dictionary tables' schemas here, in
//! code: their `RowCodec` is built from these compiled-in definitions,
//! independent of any on-disk catalog (see [`TableSchema::system_table`]).
//!
//! Dictionary tables occupy a reserved high id range (`DD_TABLE_ID_BASE`+),
//! kept well clear of user table ids (which start at 1 and increment
//! monotonically) so the two never collide in the shared
//! `tables/<id>.heap` namespace. User table id allocation is unchanged
//! (see `crate::storage::catalog`).
//!
//! ## Current scope
//!
//! The catalog mirror now covers the full in-memory catalog surface:
//! databases, base tables, columns, indexes, checks, foreign keys, views,
//! routines, triggers, events, auth, and sequences. A few
//! structurally-complex fields — the full column type (with ENUM/SET
//! members), DEFAULT values, partition definitions, routine parameters,
//! and table statistics — are still carried as JSON text columns for now;
//! later phases can split these into dedicated normalized tables (e.g.
//! MySQL's `column_type_elements`, `table_partitions`) without changing the
//! rule that DD heap rows are the catalog authority.
//!
//! [`RowCodec`]: crate::storage::core::HeapFile

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};

use crate::engine::auth::{account_id, AuthCatalog, GrantEntry, RoleEdge, UserAccount};
use crate::storage::partitioning::PartitionInfo;
use crate::types::Value;

use super::naming::{event_key, normalize_name, routine_key, table_key, trigger_key};
use super::schema::EventOnCompletionSchema;
use super::schema::{
    Catalog, CheckConstraintSchema, ColumnSchema, ColumnState, DataTypeKind, DatabaseSchema,
    EventScheduleSchema, EventSchema, EventStatusSchema, ForeignKeySchema, IndexColumnDirection,
    IndexExpression, IndexSchema, IndexState, ReferentialAction, RoutineKindSchema,
    RoutineParamSchema, RoutineSchema, RoutineSecuritySchema, RoutineSqlDataAccessSchema,
    TableSchema, TriggerEventSchema, TriggerSchema, TriggerTimingSchema, ViewColumnDef,
    ViewDependency, ViewRoutineDependency, ViewSchema, CATALOG_VERSION,
};
use super::sequence::{SeqType, SequenceSchema};
use super::stats::TableStats;

/// Persisted payload ceiling used by `mysql.table_stats.stats_json`.
///
/// Dictionary writes use the same governed LOB externalization path as user
/// tables, so the payload is not constrained by the 16 KiB heap-page size.
/// Keeping a finite ceiling still bounds catalog recovery and descriptor-cache
/// materialization for pathologically wide schemas.
pub(crate) const DD_STATS_JSON_CAP_BYTES: usize = 1024 * 1024;
const DD_STATS_JSON_CAP_BYTES_U32: u32 = DD_STATS_JSON_CAP_BYTES as u32;

#[cfg(test)]
/// Database that owns the dictionary tables. MySQL keeps them in the
/// `mysql` schema (hidden from ordinary `SHOW TABLES` / user queries).
pub(crate) const DD_DATABASE: &str = "mysql";

// ---------------------------------------------------------------------------
// Reserved dictionary table ids.
//
// Dictionary tables share the `tables/<id>.heap` namespace with user
// tables, so their ids must never collide with a user `next_table_id`
// (which starts at 1 and only increases). We place them at a fixed,
// unreachable-high base instead of reversing the user id space — that
// keeps the long-standing "first user table = tables/1.heap" invariant
// intact (much of the storage test suite relies on it). These ids are
// stable across releases: changing one is an on-disk format break.
// ---------------------------------------------------------------------------

/// Base of the reserved dictionary-table id range. `1 << 48` is far above
/// any reachable user table id, so dictionary heaps never collide with
/// user tables in `tables/<id>.heap`.
pub(crate) const DD_TABLE_ID_BASE: u64 = 1 << 48;

/// `mysql.schemata` — one row per database/schema.
pub(crate) const DD_SCHEMATA: u64 = DD_TABLE_ID_BASE + 1;
/// `mysql.tables` — table-level metadata (one row per base table/view stub).
pub(crate) const DD_TABLES: u64 = DD_TABLE_ID_BASE + 2;
/// `mysql.columns` — one row per column.
pub(crate) const DD_COLUMNS: u64 = DD_TABLE_ID_BASE + 3;
/// `mysql.indexes` — index-level metadata (one row per index).
pub(crate) const DD_INDEXES: u64 = DD_TABLE_ID_BASE + 4;
/// `mysql.index_column_usage` — one row per index key part.
pub(crate) const DD_INDEX_COLUMN_USAGE: u64 = DD_TABLE_ID_BASE + 5;
/// `mysql.check_constraints` — one row per CHECK constraint.
pub(crate) const DD_CHECK_CONSTRAINTS: u64 = DD_TABLE_ID_BASE + 6;

/// `mysql.views` — view definitions.
pub(crate) const DD_VIEWS: u64 = DD_TABLE_ID_BASE + 7;
/// `mysql.view_table_usage` — view → referenced-object edges.
#[allow(dead_code)]
pub(crate) const DD_VIEW_TABLE_USAGE: u64 = DD_TABLE_ID_BASE + 8;
/// `mysql.routines` — stored procedures / functions.
pub(crate) const DD_ROUTINES: u64 = DD_TABLE_ID_BASE + 9;
/// `mysql.parameters` — routine parameters.
#[allow(dead_code)]
pub(crate) const DD_PARAMETERS: u64 = DD_TABLE_ID_BASE + 10;
/// `mysql.triggers` — row triggers.
pub(crate) const DD_TRIGGERS: u64 = DD_TABLE_ID_BASE + 11;
/// `mysql.events` — scheduled events.
pub(crate) const DD_EVENTS: u64 = DD_TABLE_ID_BASE + 12;
/// `mysql.table_partitions` — partition definitions.
#[allow(dead_code)]
pub(crate) const DD_TABLE_PARTITIONS: u64 = DD_TABLE_ID_BASE + 13;
/// `mysql.column_type_elements` — ENUM/SET members.
#[allow(dead_code)]
pub(crate) const DD_COLUMN_TYPE_ELEMENTS: u64 = DD_TABLE_ID_BASE + 14;
/// `mysql.foreign_keys` — FK definitions.
pub(crate) const DD_FOREIGN_KEYS: u64 = DD_TABLE_ID_BASE + 15;
/// `mysql.foreign_key_column_usage` — FK key parts.
pub(crate) const DD_FOREIGN_KEY_COLUMN_USAGE: u64 = DD_TABLE_ID_BASE + 16;
/// `mysql.dd_properties` — dictionary metadata (format version, etc.).
pub(crate) const DD_DD_PROPERTIES: u64 = DD_TABLE_ID_BASE + 17;
/// Auth catalog tables.
pub(crate) const DD_USERS: u64 = DD_TABLE_ID_BASE + 18;
pub(crate) const DD_ROLE_EDGES: u64 = DD_TABLE_ID_BASE + 19;
pub(crate) const DD_GLOBAL_PRIVILEGES: u64 = DD_TABLE_ID_BASE + 20;
/// `mysql.sequences` — sequence definitions; values live in `sequence_values`.
pub(crate) const DD_SEQUENCES: u64 = DD_TABLE_ID_BASE + 21;
/// `mysql.restrictions` — partial-revoke denylist entries.
pub(crate) const DD_RESTRICTIONS: u64 = DD_TABLE_ID_BASE + 22;
/// `mysql.table_stats` — independently versioned optimizer statistics.
pub(crate) const DD_TABLE_STATS: u64 = DD_TABLE_ID_BASE + 23;
/// `mysql.innodb_dynamic_metadata` — checkpointed engine-owned table state.
pub(crate) const DD_INNODB_DYNAMIC_METADATA: u64 = DD_TABLE_ID_BASE + 24;
/// Engine-owned retirement manifests, committed with the defining DDL.
pub(crate) const DD_DDL_RETIREMENTS: u64 = DD_TABLE_ID_BASE + 25;
pub(crate) const DD_SEQUENCE_VALUES: u64 = DD_TABLE_ID_BASE + 26;
pub(crate) const DD_SEQUENCE_RETIREMENTS: u64 = DD_TABLE_ID_BASE + 27;

/// `true` if `id` falls in the reserved dictionary-table id range.
#[inline]
#[allow(dead_code)]
pub(crate) fn is_dd_table_id(id: u64) -> bool {
    id >= DD_TABLE_ID_BASE
}

// ---------------------------------------------------------------------------
// Column-definition helpers.
//
// Identity fields (`column_id` / `physical_pos` / `logical_pos` /
// `ordinal_position`) are left zeroed; `TableSchema::system_table` runs
// `build_column_index`, which fills them in declaration order.
// ---------------------------------------------------------------------------

fn col(name: &str, data_type: DataTypeKind, nullable: bool) -> ColumnSchema {
    ColumnSchema {
        not_null_from_scn: 0,
        name: name.to_string(),
        data_type,
        nullable,
        unsigned: false,
        zerofill: false,
        invisible: false,
        auto_increment: false,
        default: None,
        read_backfill_default: None,
        default_expr: None,
        on_update_expr: None,
        column_id: 0,
        state: ColumnState::Public,
        physical_pos: 0,
        logical_pos: 0,
        ordinal_position: 0,
        charset: None,
        collation: None,
        comment: String::new(),
        generated_expr: None,
        generated_stored: false,
        generated_column_names: Vec::new(),
        generated_expr_ast: None,
        generated_program: None,
    }
}

/// Unsigned integer column.
fn ucol(name: &str, data_type: DataTypeKind, nullable: bool) -> ColumnSchema {
    let mut c = col(name, data_type, nullable);
    c.unsigned = true;
    c
}

#[inline]
fn varchar(len: usize) -> DataTypeKind {
    DataTypeKind::VarChar { len: Some(len) }
}

// ---------------------------------------------------------------------------
// Dictionary table definitions.
// ---------------------------------------------------------------------------

/// `mysql.schemata` ← [`super::schema::DatabaseSchema`].
fn schemata_table() -> TableSchema {
    TableSchema::system_table(
        DD_SCHEMATA,
        "schemata",
        vec![
            col("name", varchar(64), false),
            col("default_charset", varchar(64), false),
            col("default_collation", varchar(64), false),
            col("is_system", DataTypeKind::Bool, false),
            ucol("created_ms", DataTypeKind::BigInt, false),
            ucol("updated_ms", DataTypeKind::BigInt, false),
        ],
    )
}

/// `mysql.tables` ← table-level fields of [`super::schema::TableSchema`].
/// `primary_key` and `partitioning` are carried as JSON text
/// for now (see module-level Phase 0 note).
fn tables_table() -> TableSchema {
    TableSchema::system_table(
        DD_TABLES,
        "tables",
        vec![
            ucol("id", DataTypeKind::BigInt, false),
            ucol("object_id", DataTypeKind::BigInt, false),
            col("schema_name", varchar(64), false),
            col("name", varchar(64), false),
            col("engine", varchar(64), false),
            col("charset", varchar(64), false),
            col("collation", varchar(64), false),
            col("comment", DataTypeKind::Text { max_len: 65_535 }, false),
            col("table_type", varchar(64), false),
            col("is_system", DataTypeKind::Bool, false),
            ucol("schema_version", DataTypeKind::BigInt, false),
            ucol("next_column_id", DataTypeKind::Int, false),
            ucol("active_layout_id", DataTypeKind::Int, false),
            col(
                "row_layouts_json",
                DataTypeKind::Text { max_len: 65_535 },
                false,
            ),
            col(
                "primary_key_json",
                DataTypeKind::Text { max_len: 65_535 },
                false,
            ),
            col(
                "partitioning_json",
                DataTypeKind::Text { max_len: 65_535 },
                true,
            ),
            ucol("created_ms", DataTypeKind::BigInt, false),
            ucol("updated_ms", DataTypeKind::BigInt, false),
            // v34: per-table physical-storage knobs (PCTFREE / PCTUSED).
            // Nullable: NULL = follow global. Format: `TableOptions` JSON.
            col(
                "table_options_json",
                DataTypeKind::Text { max_len: 65_535 },
                true,
            ),
            ucol("active_generation", DataTypeKind::BigInt, false),
            col(
                "pending_primary_key_json",
                DataTypeKind::Text { max_len: 65_535 },
                true,
            ),
            ucol("primary_definition_scn", DataTypeKind::BigInt, false),
            ucol("next_index_id", DataTypeKind::BigInt, false),
            ucol("primary_publication_job_id", DataTypeKind::BigInt, false),
        ],
    )
}

fn table_stats_table() -> TableSchema {
    TableSchema::system_table(
        DD_TABLE_STATS,
        "table_stats",
        vec![
            ucol("table_id", DataTypeKind::BigInt, false),
            ucol("stats_version", DataTypeKind::BigInt, false),
            col(
                "stats_json",
                DataTypeKind::Text {
                    max_len: DD_STATS_JSON_CAP_BYTES_U32,
                },
                false,
            ),
            ucol("updated_ms", DataTypeKind::BigInt, false),
        ],
    )
}

fn innodb_dynamic_metadata_table() -> TableSchema {
    TableSchema::system_table(
        DD_INNODB_DYNAMIC_METADATA,
        "innodb_dynamic_metadata",
        vec![
            ucol("table_id", DataTypeKind::BigInt, false),
            ucol("version", DataTypeKind::BigInt, false),
            ucol("auto_increment_max", DataTypeKind::BigInt, false),
            ucol("covered_lsn", DataTypeKind::BigInt, false),
        ],
    )
}

/// `mysql.columns` ← [`super::schema::ColumnSchema`]. The full column
/// type (including ENUM/SET members, length, precision/scale) and the
/// DEFAULT value are JSON-encoded for now.
fn columns_table() -> TableSchema {
    TableSchema::system_table(
        DD_COLUMNS,
        "columns",
        vec![
            ucol("table_id", DataTypeKind::BigInt, false),
            ucol("column_id", DataTypeKind::Int, false),
            col("name", varchar(64), false),
            ucol("ordinal_position", DataTypeKind::Int, false),
            ucol("physical_pos", DataTypeKind::Int, false),
            ucol("logical_pos", DataTypeKind::Int, false),
            col(
                "column_type_json",
                DataTypeKind::Text { max_len: 65_535 },
                false,
            ),
            col("is_nullable", DataTypeKind::Bool, false),
            col("is_unsigned", DataTypeKind::Bool, false),
            col("is_auto_increment", DataTypeKind::Bool, false),
            col(
                "column_default_json",
                DataTypeKind::Text { max_len: 65_535 },
                true,
            ),
            col("default_expr", DataTypeKind::Text { max_len: 65_535 }, true),
            col(
                "on_update_expr",
                DataTypeKind::Text { max_len: 65_535 },
                true,
            ),
            col("state", varchar(16), false),
            col("charset", varchar(64), true),
            col("collation", varchar(64), true),
            col("comment", DataTypeKind::Text { max_len: 65_535 }, false),
            col(
                "generated_expr",
                DataTypeKind::Text { max_len: 65_535 },
                true,
            ),
            col("generated_stored", DataTypeKind::Bool, false),
            col(
                "generated_column_names_json",
                DataTypeKind::Text { max_len: 65_535 },
                false,
            ),
            // v36 additions: readers fall back to false when the row
            // pre-dates the column (same pattern as table_options_json).
            col("is_zerofill", DataTypeKind::Bool, true),
            col("is_invisible", DataTypeKind::Bool, true),
            col(
                "read_backfill_default_json",
                DataTypeKind::Text { max_len: 65_535 },
                true,
            ),
            ucol("not_null_from_scn", DataTypeKind::BigInt, false),
        ],
    )
}

/// `mysql.indexes` ← index-level fields of [`super::schema::IndexSchema`].
/// Key parts live in `index_column_usage`. `ordinal_position` preserves
/// the order of `TableSchema::indexes` so the Vec rebuilds identically.
fn indexes_table() -> TableSchema {
    TableSchema::system_table(
        DD_INDEXES,
        "indexes",
        vec![
            ucol("table_id", DataTypeKind::BigInt, false),
            ucol("ordinal_position", DataTypeKind::Int, false),
            col("name", varchar(64), false),
            col("index_type", varchar(32), false),
            col("is_unique", DataTypeKind::Bool, false),
            col("is_primary", DataTypeKind::Bool, false),
            col("is_visible", DataTypeKind::Bool, false),
            col("state", varchar(16), false),
            col("comment", DataTypeKind::Text { max_len: 65_535 }, false),
            // FULLTEXT `WITH PARSER` name ("" = default whitespace parser,
            // "ngram" = CJK). Empty for non-FULLTEXT indexes. Persisting it
            // is required so a CJK ngram index rebuilds the *same* tokenizer
            // after restart instead of silently reverting to the default.
            col("fulltext_parser", varchar(32), true),
            ucol("index_id", DataTypeKind::BigInt, false),
            col(
                "fulltext_config",
                DataTypeKind::Text { max_len: 65_535 },
                true,
            ),
            ucol("readable_from_scn", DataTypeKind::BigInt, false),
            col(
                "vector_config",
                DataTypeKind::Text { max_len: 65_535 },
                true,
            ),
        ],
    )
}

/// `mysql.index_column_usage` ← per-key-part fields of
/// [`super::schema::IndexSchema`] (columns, prefix lengths, directions,
/// INCLUDE columns, and functional-index expressions).
fn index_column_usage_table() -> TableSchema {
    TableSchema::system_table(
        DD_INDEX_COLUMN_USAGE,
        "index_column_usage",
        vec![
            ucol("table_id", DataTypeKind::BigInt, false),
            col("index_name", varchar(64), false),
            ucol("ordinal_position", DataTypeKind::Int, false),
            col("column_name", varchar(64), true),
            ucol("prefix_length", DataTypeKind::Int, true),
            col("direction", varchar(8), false),
            col("is_included", DataTypeKind::Bool, false),
            col("expression", DataTypeKind::Text { max_len: 65_535 }, true),
            col(
                "expr_column_type_json",
                DataTypeKind::Text { max_len: 65_535 },
                true,
            ),
            col("expr_nullable", DataTypeKind::Bool, false),
            col(
                "expr_referenced_columns_json",
                DataTypeKind::Text { max_len: 65_535 },
                true,
            ),
            col("expr_multi_valued", DataTypeKind::Bool, false),
        ],
    )
}

/// `mysql.check_constraints` ← [`super::schema::CheckConstraintSchema`].
fn check_constraints_table() -> TableSchema {
    TableSchema::system_table(
        DD_CHECK_CONSTRAINTS,
        "check_constraints",
        vec![
            ucol("table_id", DataTypeKind::BigInt, false),
            col("name", varchar(64), false),
            col(
                "check_clause",
                DataTypeKind::Text { max_len: 65_535 },
                false,
            ),
            col("enforced", DataTypeKind::Bool, false),
            col(
                "column_names_json",
                DataTypeKind::Text { max_len: 65_535 },
                false,
            ),
        ],
    )
}

/// `mysql.foreign_keys` ← [`super::schema::ForeignKeySchema`]. One row
/// per foreign key declared on the child (`table_id`) side; the
/// participating column pairs live in `foreign_key_column_usage`,
/// mirroring how indexes split into `index_column_usage`.
fn foreign_keys_table() -> TableSchema {
    TableSchema::system_table(
        DD_FOREIGN_KEYS,
        "foreign_keys",
        vec![
            ucol("table_id", DataTypeKind::BigInt, false),
            col("name", varchar(64), false),
            col("referenced_schema", varchar(64), false),
            col("referenced_table", varchar(64), false),
            col("update_rule", varchar(16), false),
            col("delete_rule", varchar(16), false),
            col("backing_index", varchar(64), false),
            ucol("backing_index_id", DataTypeKind::BigInt, false),
        ],
    )
}

/// `mysql.foreign_key_column_usage` ← the `(columns, referenced_columns)`
/// pairs of each [`super::schema::ForeignKeySchema`], one row per pair.
fn foreign_key_column_usage_table() -> TableSchema {
    TableSchema::system_table(
        DD_FOREIGN_KEY_COLUMN_USAGE,
        "foreign_key_column_usage",
        vec![
            ucol("table_id", DataTypeKind::BigInt, false),
            col("fk_name", varchar(64), false),
            ucol("ordinal_position", DataTypeKind::Int, false),
            col("column_name", varchar(64), false),
            col("referenced_column_name", varchar(64), false),
        ],
    )
}

/// `mysql.views` ← [`super::schema::ViewSchema`]. Output columns and
/// referenced-object dependencies are carried as JSON for now (MySQL
/// splits dependencies into `view_table_usage`).
fn views_table() -> TableSchema {
    TableSchema::system_table(
        DD_VIEWS,
        "views",
        vec![
            col("schema_name", varchar(64), false),
            col("name", varchar(64), false),
            col(
                "view_definition",
                DataTypeKind::Text { max_len: 65_535 },
                false,
            ),
            col(
                "columns_json",
                DataTypeKind::Text { max_len: 65_535 },
                false,
            ),
            col(
                "dependencies_json",
                DataTypeKind::Text { max_len: 65_535 },
                false,
            ),
            col("algorithm", varchar(16), false),
            col("definer_user", varchar(64), false),
            col("definer_host", varchar(255), false),
            col("security_type", varchar(16), false),
            col(
                "routine_dependencies_json",
                DataTypeKind::Text { max_len: 65_535 },
                false,
            ),
            col("check_option", varchar(16), true),
            col("is_updatable", DataTypeKind::Bool, false),
            col("character_set_client", varchar(64), false),
            col("collation_connection", varchar(64), false),
            col("is_invalid", DataTypeKind::Bool, false),
            ucol("schema_version", DataTypeKind::BigInt, false),
            // P0 — MySQL-style canonical view body for SHOW CREATE VIEW /
            // information_schema.views. Nullable so DD heaps written before
            // this column reconstruct with an empty string (→ raw-text echo).
            col(
                "view_definition_normalized",
                DataTypeKind::Text { max_len: 65_535 },
                true,
            ),
            col("query_json", DataTypeKind::Text { max_len: u32::MAX }, true),
        ],
    )
}

/// `mysql.routines` ← [`super::schema::RoutineSchema`]. Parameters are
/// carried as JSON for now (MySQL splits them into `parameters`).
fn routines_table() -> TableSchema {
    TableSchema::system_table(
        DD_ROUTINES,
        "routines",
        vec![
            col("schema_name", varchar(64), false),
            col("name", varchar(64), false),
            col("routine_type", varchar(16), false),
            col("definer_user", varchar(64), false),
            col("definer_host", varchar(255), false),
            col("security_type", varchar(16), false),
            col(
                "return_type_json",
                DataTypeKind::Text { max_len: 65_535 },
                true,
            ),
            col(
                "return_type_sql",
                DataTypeKind::Text { max_len: 65_535 },
                true,
            ),
            col("return_unsigned", DataTypeKind::Bool, false),
            col("is_deterministic", DataTypeKind::Bool, false),
            col("sql_data_access", varchar(32), false),
            col("comment", DataTypeKind::Text { max_len: 65_535 }, false),
            col("body", DataTypeKind::Text { max_len: 65_535 }, false),
            col("params_json", DataTypeKind::Text { max_len: 65_535 }, false),
            ucol("schema_version", DataTypeKind::BigInt, false),
            ucol("created_ms", DataTypeKind::BigInt, false),
            ucol("updated_ms", DataTypeKind::BigInt, false),
            col("character_set_client", varchar(64), false),
            col("collation_connection", varchar(64), false),
            col("database_collation", varchar(64), false),
            // Appended after the original 21 columns; nullable so pre-existing
            // rows (which lack it) reconstruct to an empty sql_mode without a
            // CATALOG_VERSION bump.
            col("sql_mode", DataTypeKind::Text { max_len: 65_535 }, true),
        ],
    )
}

/// `mysql.triggers` ← [`super::schema::TriggerSchema`].
fn triggers_table() -> TableSchema {
    TableSchema::system_table(
        DD_TRIGGERS,
        "triggers",
        vec![
            col("schema_name", varchar(64), false),
            col("name", varchar(64), false),
            col("table_schema", varchar(64), false),
            col("table_name", varchar(64), false),
            col("action_timing", varchar(8), false),
            col("event_type", varchar(8), false),
            col("definer_user", varchar(64), false),
            col("definer_host", varchar(255), false),
            col(
                "action_statement",
                DataTypeKind::Text { max_len: 65_535 },
                false,
            ),
            col("create_sql", DataTypeKind::Text { max_len: 65_535 }, false),
            ucol("action_order", DataTypeKind::Int, false),
            ucol("schema_version", DataTypeKind::BigInt, false),
            ucol("created_ms", DataTypeKind::BigInt, false),
            col("sql_mode", DataTypeKind::Text { max_len: 65_535 }, false),
            col("character_set_client", varchar(64), false),
            col("collation_connection", varchar(64), false),
            col("database_collation", varchar(64), false),
        ],
    )
}

/// `mysql.events` ← [`super::schema::EventSchema`]. The schedule
/// (one-shot vs recurring + interval) is carried as JSON.
fn events_table() -> TableSchema {
    TableSchema::system_table(
        DD_EVENTS,
        "events",
        vec![
            col("schema_name", varchar(64), false),
            col("name", varchar(64), false),
            col("definer_user", varchar(64), false),
            col("definer_host", varchar(255), false),
            col(
                "schedule_json",
                DataTypeKind::Text { max_len: 65_535 },
                false,
            ),
            col("on_completion", varchar(16), false),
            col("status", varchar(24), false),
            col("comment", DataTypeKind::Text { max_len: 65_535 }, false),
            col("body", DataTypeKind::Text { max_len: 65_535 }, false),
            col("create_sql", DataTypeKind::Text { max_len: 65_535 }, false),
            ucol("created_ms", DataTypeKind::BigInt, false),
            ucol("last_altered_ms", DataTypeKind::BigInt, false),
            ucol("last_executed_ms", DataTypeKind::BigInt, false),
            ucol("schema_version", DataTypeKind::BigInt, false),
            col("sql_mode", DataTypeKind::Text { max_len: 65_535 }, false),
            col("time_zone", varchar(64), false),
            col("character_set_client", varchar(64), false),
            col("collation_connection", varchar(64), false),
            col("database_collation", varchar(64), false),
        ],
    )
}

/// `mysql.users` ← [`crate::engine::auth::UserAccount`]. Plugin / auth
/// string / resource limits are carried as JSON; user+host are typed for
/// lookup and information_schema.
fn users_table() -> TableSchema {
    TableSchema::system_table(
        DD_USERS,
        "users",
        vec![
            col("user", varchar(64), false),
            col("host", varchar(255), false),
            col(
                "account_json",
                DataTypeKind::Text { max_len: 65_535 },
                false,
            ),
        ],
    )
}

/// `mysql.grants` ← [`crate::engine::auth::GrantEntry`], ordered by
/// `ordinal`.
fn grants_table() -> TableSchema {
    TableSchema::system_table(
        DD_GLOBAL_PRIVILEGES,
        "grants",
        vec![
            col("grant_key", varchar(1024), false),
            col("grant_json", DataTypeKind::Text { max_len: 65_535 }, false),
        ],
    )
}

/// `mysql.role_edges` ← [`crate::engine::auth::RoleEdge`]. `is_default`
/// separates granted role edges from default-role assignments.
fn role_edges_table() -> TableSchema {
    TableSchema::system_table(
        DD_ROLE_EDGES,
        "role_edges",
        vec![
            col("edge_key", varchar(1024), false),
            col("is_default", DataTypeKind::Bool, false),
            col("edge_json", DataTypeKind::Text { max_len: 65_535 }, false),
        ],
    )
}

/// `mysql.dd_properties` — key/value dictionary metadata (auth epoch,
/// dictionary format version, ...).
fn dd_properties_table() -> TableSchema {
    TableSchema::system_table(
        DD_DD_PROPERTIES,
        "dd_properties",
        vec![
            col("name", varchar(64), false),
            col("value", DataTypeKind::Text { max_len: 65_535 }, false),
        ],
    )
}

/// Sequence definitions, separate from checkpointed dynamic values.
fn sequences_table() -> TableSchema {
    TableSchema::system_table(
        DD_SEQUENCES,
        "sequences",
        vec![
            ucol("id", DataTypeKind::BigInt, false),
            col("schema_name", varchar(64), false),
            col("name", varchar(64), false),
            col("as_type", varchar(16), false),
            col("increment", DataTypeKind::BigInt, false),
            col("min_value", DataTypeKind::BigInt, false),
            col("max_value", DataTypeKind::BigInt, false),
            col("start_value", DataTypeKind::BigInt, false),
            col("cache", DataTypeKind::BigInt, false),
            col("cycle", DataTypeKind::Bool, false),
            col("owned_table", varchar(64), true),
            col("owned_column", varchar(64), true),
            ucol("created_ms", DataTypeKind::BigInt, false),
            ucol("updated_ms", DataTypeKind::BigInt, false),
            ucol("counter_id", DataTypeKind::BigInt, false),
            ucol("owned_table_id", DataTypeKind::BigInt, true),
            ucol("owned_column_id", DataTypeKind::Int, true),
        ],
    )
}

fn restrictions_table() -> TableSchema {
    TableSchema::system_table(
        DD_RESTRICTIONS,
        "restrictions",
        vec![
            col("restriction_key", varchar(1024), false),
            col(
                "restriction_json",
                DataTypeKind::Text { max_len: 65_535 },
                false,
            ),
        ],
    )
}

fn sequence_values_table() -> TableSchema {
    TableSchema::system_table(
        DD_SEQUENCE_VALUES,
        "sequence_values",
        vec![
            ucol("counter_id", DataTypeKind::BigInt, false),
            ucol("sequence_id", DataTypeKind::BigInt, false),
            col("resume_value", DataTypeKind::BigInt, false),
            col("is_called", DataTypeKind::Bool, false),
            ucol("state_lsn", DataTypeKind::BigInt, false),
        ],
    )
}

fn build_dd_tables() -> Vec<Arc<TableSchema>> {
    vec![
        Arc::new(schemata_table()),
        Arc::new(tables_table()),
        Arc::new(columns_table()),
        Arc::new(indexes_table()),
        Arc::new(index_column_usage_table()),
        Arc::new(check_constraints_table()),
        Arc::new(foreign_keys_table()),
        Arc::new(foreign_key_column_usage_table()),
        Arc::new(views_table()),
        Arc::new(routines_table()),
        Arc::new(triggers_table()),
        Arc::new(events_table()),
        Arc::new(users_table()),
        Arc::new(grants_table()),
        Arc::new(role_edges_table()),
        Arc::new(dd_properties_table()),
        Arc::new(sequences_table()),
        Arc::new(restrictions_table()),
        Arc::new(table_stats_table()),
        Arc::new(innodb_dynamic_metadata_table()),
        Arc::new(TableSchema::system_table(
            DD_DDL_RETIREMENTS,
            "mydb_ddl_retirements",
            vec![
                ucol("table_id", DataTypeKind::BigInt, false),
                ucol("generation", DataTypeKind::BigInt, false),
                ucol("index_id", DataTypeKind::BigInt, false),
                col(
                    "manifest",
                    DataTypeKind::Text {
                        max_len: 16 * 1024 * 1024,
                    },
                    false,
                ),
            ],
        )),
        Arc::new(sequence_values_table()),
        Arc::new(TableSchema::system_table(
            DD_SEQUENCE_RETIREMENTS,
            "sequence_retirements",
            vec![
                ucol("counter_id", DataTypeKind::BigInt, false),
                ucol("sequence_id", DataTypeKind::BigInt, false),
                ucol("retired_lsn", DataTypeKind::BigInt, false),
            ],
        )),
    ]
}

/// All compiled-in dictionary table schemas, in ascending id order.
/// Built once and cached for the process lifetime.
pub(crate) fn dd_tables() -> &'static [Arc<TableSchema>] {
    static TABLES: OnceLock<Vec<Arc<TableSchema>>> = OnceLock::new();
    TABLES.get_or_init(build_dd_tables)
}

/// The compiled-in schema for dictionary table `id`, if `id` names one.
pub(crate) fn dd_table_schema(id: u64) -> Option<Arc<TableSchema>> {
    dd_tables().iter().find(|t| t.id == id).cloned()
}

// ===========================================================================
// Catalog <-> dictionary-row mapping.
//
// The mirror covers every durable catalog family represented by `DdRowSet`.
// Structurally-complex fields (full column type, DEFAULT value, partition
// definition, table statistics, routine parameters, auth account payloads)
// are carried as JSON text and rebuilt on read by the relevant schema
// constructors. `TableSchema::build_column_index` re-derives every
// `#[serde(skip)]` runtime field (row codec, compiled expressions, partition
// program) from the persisted text.
//
// Row column order MUST match the table's column definitions above — the
// `roundtrip_*` tests guard this.
// ===========================================================================

/// Rows for each mirrored dictionary table, each row in column-definition
/// order. Produced by [`catalog_to_dd_rows`]; consumed when writing heaps
/// and when rebuilding a [`Catalog`] via [`catalog_from_dd_rows`].
#[derive(Debug, Default, Clone)]
pub(crate) struct DdRowSet {
    pub schemata: Vec<Vec<Value>>,
    pub tables: Vec<Vec<Value>>,
    pub columns: Vec<Vec<Value>>,
    pub indexes: Vec<Vec<Value>>,
    pub index_column_usage: Vec<Vec<Value>>,
    pub check_constraints: Vec<Vec<Value>>,
    pub foreign_keys: Vec<Vec<Value>>,
    pub foreign_key_column_usage: Vec<Vec<Value>>,
    pub views: Vec<Vec<Value>>,
    pub routines: Vec<Vec<Value>>,
    pub triggers: Vec<Vec<Value>>,
    pub events: Vec<Vec<Value>>,
    pub users: Vec<Vec<Value>>,
    pub grants: Vec<Vec<Value>>,
    pub role_edges: Vec<Vec<Value>>,
    pub dd_properties: Vec<Vec<Value>>,
    pub sequences: Vec<Vec<Value>>,
    pub restrictions: Vec<Vec<Value>>,
    pub table_stats: Vec<Vec<Value>>,
    pub innodb_dynamic_metadata: Vec<Vec<Value>>,
}

impl DdRowSet {
    /// `(dictionary table id, rows)` pairs, for writing into heap files.
    pub(crate) fn per_table(&self) -> [(u64, &Vec<Vec<Value>>); 20] {
        [
            (DD_SCHEMATA, &self.schemata),
            (DD_TABLES, &self.tables),
            (DD_COLUMNS, &self.columns),
            (DD_INDEXES, &self.indexes),
            (DD_INDEX_COLUMN_USAGE, &self.index_column_usage),
            (DD_CHECK_CONSTRAINTS, &self.check_constraints),
            (DD_FOREIGN_KEYS, &self.foreign_keys),
            (DD_FOREIGN_KEY_COLUMN_USAGE, &self.foreign_key_column_usage),
            (DD_VIEWS, &self.views),
            (DD_ROUTINES, &self.routines),
            (DD_TRIGGERS, &self.triggers),
            (DD_EVENTS, &self.events),
            (DD_USERS, &self.users),
            (DD_GLOBAL_PRIVILEGES, &self.grants),
            (DD_ROLE_EDGES, &self.role_edges),
            (DD_DD_PROPERTIES, &self.dd_properties),
            (DD_SEQUENCES, &self.sequences),
            (DD_RESTRICTIONS, &self.restrictions),
            (DD_TABLE_STATS, &self.table_stats),
            (DD_INNODB_DYNAMIC_METADATA, &self.innodb_dynamic_metadata),
        ]
    }
}

// ---- build helpers (Catalog field -> column Value) ----
#[inline]
fn sv(v: impl Into<String>) -> Value {
    Value::String(v.into())
}
#[inline]
fn uv(v: u64) -> Value {
    Value::UInt(v)
}
#[inline]
fn bv(v: bool) -> Value {
    Value::Int(v as i64)
}
#[inline]
fn opt_sv(v: Option<&str>) -> Value {
    v.map_or(Value::Null, |s| Value::String(s.to_string()))
}
#[inline]
fn opt_uv(v: Option<u64>) -> Value {
    v.map_or(Value::Null, Value::UInt)
}
/// Serialize a structurally-complex field to a JSON text column.
fn jsonv<T: serde::Serialize>(v: &T) -> Value {
    Value::String(serde_json::to_string(v).unwrap_or_else(|_| "null".to_string()))
}

/// Serialize table statistics for the `mysql.table_stats.stats_json` column
/// while keeping the encoded text within the 1 MiB catalog payload limit.
///
/// A full [`TableStats`] for a wide table — per-column top-value sketches
/// plus equi-depth histograms — can exceed the bounded catalog payload. When
/// that happens we persist a progressively trimmed copy that keeps the cheap
/// scalar estimates (row / distinct / null counts, min, max), drops the large
/// sketches, and preserves compact conditional key-domain stats when possible.
/// The value is externalized through the normal DD LOB path whenever it does
/// not fit in a heap page. A final guard falls back to empty stats for
/// pathologically wide tables. Statistics are advisory, so this
/// lossy-on-overflow persistence is sound — correctness never depends on the
/// sketches surviving a restart.
fn stats_json_bounded(table: &TableSchema) -> Value {
    let stats = &table.stats;
    let full = serde_json::to_string(stats).unwrap_or_else(|_| "null".to_string());
    if full.len() <= DD_STATS_JSON_CAP_BYTES {
        return Value::String(full);
    }
    let mut trimmed = stats.clone();
    for col in trimmed.columns.values_mut() {
        col.top_values = Vec::new();
        col.frequency = Default::default();
        col.histogram = Vec::new();
    }
    let trimmed_json = serde_json::to_string(&trimmed).unwrap_or_else(|_| "null".to_string());
    if trimmed_json.len() <= DD_STATS_JSON_CAP_BYTES {
        return Value::String(trimmed_json);
    }
    trimmed.conditional_key_domains.retain(|domain| {
        domain.filter_columns.len() == 1
            && domain.key_columns.len() == 1
            && domain.entries.len() <= 1024
    });
    trimmed
        .conditional_key_domains
        .sort_by_key(|domain| domain.entries.len());
    let trimmed_json = serde_json::to_string(&trimmed).unwrap_or_else(|_| "null".to_string());
    if trimmed_json.len() <= DD_STATS_JSON_CAP_BYTES {
        return Value::String(trimmed_json);
    }
    let keep_columns = stats_priority_columns(table, &trimmed);
    trimmed
        .columns
        .retain(|column, _| keep_columns.contains(&normalize_name(column)));
    let trimmed_json = serde_json::to_string(&trimmed).unwrap_or_else(|_| "null".to_string());
    if trimmed_json.len() <= DD_STATS_JSON_CAP_BYTES {
        return Value::String(trimmed_json);
    }
    for entry_limit in [256_usize, 128, 64, 16, 4, 1] {
        for domain in &mut trimmed.conditional_key_domains {
            if domain.entries.len() > entry_limit {
                domain
                    .entries
                    .sort_by_key(|entry| std::cmp::Reverse(entry.row_count));
                domain.entries.truncate(entry_limit);
            }
        }
        let trimmed_json = serde_json::to_string(&trimmed).unwrap_or_else(|_| "null".to_string());
        if trimmed_json.len() <= DD_STATS_JSON_CAP_BYTES {
            return Value::String(trimmed_json);
        }
    }
    while !trimmed.conditional_key_domains.is_empty() {
        trimmed.conditional_key_domains.pop();
        let trimmed_json = serde_json::to_string(&trimmed).unwrap_or_else(|_| "null".to_string());
        if trimmed_json.len() <= DD_STATS_JSON_CAP_BYTES {
            return Value::String(trimmed_json);
        }
    }
    let mut fallback = TableStats::empty();
    // Advisory CBO sketches may be trimmed; explicitly requested SQL
    // histograms are user-visible metadata and must survive a restart.
    fallback.explicit_histograms = stats.explicit_histograms.clone();
    jsonv(&fallback)
}

fn stats_priority_columns(
    table: &TableSchema,
    stats: &TableStats,
) -> std::collections::BTreeSet<String> {
    let mut keep = std::collections::BTreeSet::new();
    for column in &table.primary_key {
        keep.insert(normalize_name(column));
    }
    for index in &table.indexes {
        for column in &index.columns {
            keep.insert(normalize_name(column));
        }
    }
    for domain in &stats.conditional_key_domains {
        for column in domain
            .filter_columns
            .iter()
            .chain(domain.key_columns.iter())
        {
            keep.insert(normalize_name(column));
        }
    }
    keep
}

// ---- read helpers (column Value -> Catalog field) ----
fn r_str(row: &[Value], i: usize) -> String {
    match row.get(i) {
        Some(Value::String(s)) => s.clone(),
        Some(Value::SmallString(b)) => unsafe { std::str::from_utf8_unchecked(b) }.to_string(),
        Some(Value::Bytes(b)) => String::from_utf8_lossy(b).into_owned(),
        _ => String::new(),
    }
}
fn r_str_default(row: &[Value], i: usize, default: &str) -> String {
    let value = r_str(row, i);
    if value.is_empty() {
        default.to_string()
    } else {
        value
    }
}
fn r_u64(row: &[Value], i: usize) -> u64 {
    match row.get(i) {
        Some(Value::UInt(x)) => *x,
        Some(Value::Int(x)) => *x as u64,
        _ => 0,
    }
}
fn r_i64(row: &[Value], i: usize) -> i64 {
    match row.get(i) {
        Some(Value::Int(x)) => *x,
        Some(Value::UInt(x)) => *x as i64,
        _ => 0,
    }
}
fn r_bool(row: &[Value], i: usize) -> bool {
    r_u64(row, i) != 0
}
fn r_opt_str(row: &[Value], i: usize) -> Option<String> {
    match row.get(i) {
        Some(Value::String(s)) => Some(s.clone()),
        Some(Value::SmallString(b)) => {
            Some(unsafe { std::str::from_utf8_unchecked(b) }.to_string())
        }
        Some(Value::Bytes(b)) => Some(String::from_utf8_lossy(b).into_owned()),
        _ => None,
    }
}
fn r_opt_usize(row: &[Value], i: usize) -> Option<usize> {
    match row.get(i) {
        Some(Value::UInt(x)) => Some(*x as usize),
        Some(Value::Int(x)) => Some(*x as usize),
        _ => None,
    }
}
/// Deserialize a JSON text column to a `Default`-able type.
fn r_json<T: serde::de::DeserializeOwned + Default>(row: &[Value], i: usize) -> T {
    match row.get(i) {
        Some(Value::String(s)) => serde_json::from_str(s).unwrap_or_default(),
        Some(Value::SmallString(b)) => {
            serde_json::from_str(unsafe { std::str::from_utf8_unchecked(b) }).unwrap_or_default()
        }
        _ => T::default(),
    }
}
/// DataTypeKind has no `Default`; fall back to `Text` on parse failure
/// (should never happen for rows we wrote).
fn r_data_type(row: &[Value], i: usize) -> DataTypeKind {
    match row.get(i) {
        Some(Value::String(s)) => {
            serde_json::from_str(s).unwrap_or(DataTypeKind::Text { max_len: 65_535 })
        }
        Some(Value::SmallString(b)) => {
            serde_json::from_str(unsafe { std::str::from_utf8_unchecked(b) })
                .unwrap_or(DataTypeKind::Text { max_len: 65_535 })
        }
        _ => DataTypeKind::Text { max_len: 65_535 },
    }
}

fn col_state_str(s: ColumnState) -> &'static str {
    match s {
        ColumnState::Public => "PUBLIC",
        ColumnState::WriteOnly => "WRITE_ONLY",
        ColumnState::DeleteOnly => "DELETE_ONLY",
        ColumnState::Dropped => "DROPPED",
    }
}
fn col_state_from(s: &str) -> ColumnState {
    match s {
        "WRITE_ONLY" => ColumnState::WriteOnly,
        "DELETE_ONLY" => ColumnState::DeleteOnly,
        "DROPPED" => ColumnState::Dropped,
        _ => ColumnState::Public,
    }
}
fn idx_state_str(s: IndexState) -> &'static str {
    match s {
        IndexState::Public => "PUBLIC",
        IndexState::Building => "BUILDING",
    }
}
fn idx_state_from(s: &str) -> IndexState {
    match s {
        "BUILDING" => IndexState::Building,
        _ => IndexState::Public,
    }
}
fn dir_str(d: IndexColumnDirection) -> &'static str {
    match d {
        IndexColumnDirection::Asc => "ASC",
        IndexColumnDirection::Desc => "DESC",
    }
}
fn dir_from(s: &str) -> IndexColumnDirection {
    match s {
        "DESC" => IndexColumnDirection::Desc,
        _ => IndexColumnDirection::Asc,
    }
}

/// Flatten the database + base-table portion of `catalog` into dictionary
/// rows. (Views / routines / triggers / events / auth are mirrored in a
/// later phase.)
pub(crate) fn catalog_to_dd_rows(catalog: &Catalog) -> DdRowSet {
    let mut set = DdRowSet::default();
    for db in catalog.databases.values() {
        set.schemata.push(vec![
            sv(&db.name),
            sv(&db.charset),
            sv(&db.collation),
            bv(db.system),
            uv(db.created_ms),
            uv(db.updated_ms),
        ]);
    }
    for table in catalog.tables.values() {
        push_table_rows(&mut set, table);
    }
    for view in catalog.views.values() {
        set.views.push(view_row(view));
    }
    for routine in catalog.routines.values() {
        set.routines.push(routine_row(routine));
    }
    for trigger in catalog.triggers.values() {
        set.triggers.push(trigger_row(trigger));
    }
    for event in catalog.events.values() {
        set.events.push(event_row(event));
    }
    for seq in catalog.sequences.values() {
        set.sequences.push(sequence_row(seq));
    }
    for account in catalog.auth.accounts.values() {
        set.users.push(vec![
            sv(&account.key.user),
            sv(&account.key.host),
            jsonv(account),
        ]);
    }
    for grant in &catalog.auth.grants {
        set.grants
            .push(vec![sv(&auth_grant_key(grant)), jsonv(grant)]);
    }
    for edge in &catalog.auth.role_edges {
        set.role_edges.push(vec![
            sv(&auth_role_edge_key(edge, false)),
            bv(false),
            jsonv(edge),
        ]);
    }
    for edge in &catalog.auth.default_roles {
        set.role_edges.push(vec![
            sv(&auth_role_edge_key(edge, true)),
            bv(true),
            jsonv(edge),
        ]);
    }
    for restriction in &catalog.auth.restrictions {
        set.restrictions.push(vec![
            sv(&auth_restriction_key(restriction)),
            jsonv(restriction),
        ]);
    }
    set.dd_properties
        .push(vec![sv("auth_epoch"), sv(catalog.auth.epoch.to_string())]);
    set.dd_properties
        .push(vec![sv("catalog_version"), sv(CATALOG_VERSION.to_string())]);
    set.dd_properties.push(vec![
        sv("next_table_id"),
        sv(catalog.next_table_id.to_string()),
    ]);
    set.dd_properties.push(vec![
        sv("next_sequence_id"),
        sv(catalog.next_sequence_id.to_string()),
    ]);
    set
}

fn push_table_rows(set: &mut DdRowSet, table: &TableSchema) {
    let table_options = crate::storage::schema::TableOptions::from_schema(table);
    let table_options_json = if table_options.is_default() {
        Value::Null
    } else {
        jsonv(&table_options)
    };
    set.tables.push(vec![
        uv(table.id),
        uv(table.object_id),
        sv(&table.database),
        sv(&table.name),
        sv(&table.engine),
        sv(&table.charset),
        sv(&table.collation),
        sv(&table.comment),
        sv(&table.table_type),
        bv(table.system),
        uv(table.schema_version),
        uv(u64::from(table.next_column_id)),
        uv(u64::from(table.active_layout_id)),
        jsonv(&table.row_layouts),
        jsonv(&table.primary_key),
        jsonv(&table.partitioning),
        uv(table.created_ms),
        uv(table.updated_ms),
        table_options_json,
        uv(table.active_generation),
        jsonv(&table.pending_primary_key),
        uv(table.primary_definition_scn),
        uv(table.next_index_id),
        uv(table.primary_publication_job_id),
    ]);
    set.table_stats.push(vec![
        uv(table.id),
        uv(table.schema_version),
        stats_json_bounded(table),
        uv(table.updated_ms),
    ]);
    for col in &table.columns {
        set.columns.push(vec![
            uv(table.id),
            uv(u64::from(col.column_id)),
            sv(&col.name),
            uv(u64::from(col.ordinal_position)),
            uv(u64::from(col.physical_pos)),
            uv(u64::from(col.logical_pos)),
            jsonv(&col.data_type),
            bv(col.nullable),
            bv(col.unsigned),
            bv(col.auto_increment),
            jsonv(&col.default),
            opt_sv(col.default_expr.as_deref()),
            opt_sv(col.on_update_expr.as_deref()),
            sv(col_state_str(col.state)),
            opt_sv(col.charset.as_deref()),
            opt_sv(col.collation.as_deref()),
            sv(&col.comment),
            opt_sv(col.generated_expr.as_deref()),
            bv(col.generated_stored),
            jsonv(&col.generated_column_names),
            bv(col.zerofill),
            bv(col.invisible),
            jsonv(&col.read_backfill_default),
            uv(col.not_null_from_scn),
        ]);
    }
    for (idx_pos, index) in table.indexes.iter().enumerate() {
        set.indexes.push(vec![
            uv(table.id),
            uv(idx_pos as u64),
            sv(&index.name),
            sv(&index.index_type),
            bv(index.unique),
            bv(index.primary),
            bv(index.visible),
            sv(idx_state_str(index.state)),
            sv(&index.comment),
            sv(index
                .fulltext
                .as_ref()
                .map(|m| m.parser.as_str())
                .unwrap_or("")),
            uv(index.index_id),
            jsonv(&index.fulltext),
            uv(index.readable_from_scn),
            jsonv(&index.vector),
        ]);
        let mut ord = 0u64;
        for pos in 0..index.columns.len() {
            let expr = index.key_expression(pos);
            set.index_column_usage.push(vec![
                uv(table.id),
                sv(&index.name),
                uv(ord),
                opt_sv(if expr.is_some() {
                    None
                } else {
                    index.columns.get(pos).map(String::as_str)
                }),
                opt_uv(index.column_prefix_len(pos).map(|x| x as u64)),
                sv(dir_str(index.column_direction(pos))),
                bv(false),
                opt_sv(expr.map(|e| e.expr_sql.as_str())),
                expr.map_or(Value::Null, |e| jsonv(&e.data_type)),
                bv(expr.is_some_and(|e| e.nullable)),
                expr.map_or(Value::Null, |e| jsonv(&e.referenced_columns)),
                bv(expr.is_some_and(|e| e.multi_valued)),
            ]);
            ord += 1;
        }
        for inc in &index.included_columns {
            set.index_column_usage.push(vec![
                uv(table.id),
                sv(&index.name),
                uv(ord),
                sv(inc),
                Value::Null,
                sv("ASC"),
                bv(true),
                Value::Null,
                Value::Null,
                bv(false),
                Value::Null,
                bv(false),
            ]);
            ord += 1;
        }
    }
    for chk in &table.check_constraints {
        set.check_constraints.push(vec![
            uv(table.id),
            sv(&chk.name),
            sv(&chk.expr),
            bv(chk.enforced),
            jsonv(&chk.column_names),
        ]);
    }
    for fk in &table.foreign_keys {
        set.foreign_keys.push(vec![
            uv(table.id),
            sv(&fk.name),
            sv(&fk.referenced_database),
            sv(&fk.referenced_table),
            sv(fk.on_update.as_sql()),
            sv(fk.on_delete.as_sql()),
            sv(&fk.backing_index),
            uv(fk.backing_index_id),
        ]);
        for (ord, (child, parent)) in fk
            .columns
            .iter()
            .zip(fk.referenced_columns.iter())
            .enumerate()
        {
            set.foreign_key_column_usage.push(vec![
                uv(table.id),
                sv(&fk.name),
                uv(ord as u64),
                sv(child),
                sv(parent),
            ]);
        }
    }
}

/// Encode exactly one table and its owned child metadata. Incremental catalog
/// transactions use this instead of walking the process-wide catalog.
pub(crate) fn dd_rows_for_table(table: &TableSchema) -> DdRowSet {
    let mut set = DdRowSet::default();
    push_table_rows(&mut set, table);
    set
}

pub(crate) fn dd_row_for_table_stats(table: &TableSchema) -> Vec<Value> {
    vec![
        uv(table.id),
        uv(table.schema_version),
        stats_json_bounded(table),
        uv(table.updated_ms),
    ]
}

/// Decode only the startup-resident relation header. Large row-layout,
/// primary-key and statistics JSON fields are intentionally skipped; they are
/// decoded with the rest of the descriptor on first object access.
pub(crate) fn table_header_from_dd_row(trow: &[Value]) -> Arc<TableSchema> {
    let table_options: crate::storage::schema::TableOptions = match trow.get(18) {
        Some(Value::String(s)) if !s.is_empty() => serde_json::from_str(s).unwrap_or_default(),
        Some(Value::SmallString(b)) if !b.is_empty() => {
            serde_json::from_str(unsafe { std::str::from_utf8_unchecked(b) }).unwrap_or_default()
        }
        _ => crate::storage::schema::TableOptions::default(),
    };
    Arc::new(TableSchema {
        id: r_u64(trow, 0),
        object_id: r_u64(trow, 1),
        active_generation: r_u64(trow, 19).max(1),
        database: r_str(trow, 2),
        name: r_str(trow, 3),
        engine: r_str(trow, 4),
        charset: r_str(trow, 5),
        collation: r_str(trow, 6),
        comment: r_str(trow, 7),
        created_ms: r_u64(trow, 16),
        updated_ms: r_u64(trow, 17),
        table_type: r_str(trow, 8),
        columns: Vec::new(),
        primary_key: Vec::new(),
        indexes: Vec::new(),
        check_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        fk_referenced_by: Vec::new(),
        stats: TableStats::empty(),
        system: r_bool(trow, 9),
        schema_version: r_u64(trow, 10),
        system_time_epoch_us: table_options.system_time_epoch_us,
        row_generation_txn_id: table_options.row_generation_txn_id,
        next_column_id: r_u64(trow, 11) as u32,
        active_layout_id: r_u64(trow, 12) as u32,
        row_layouts: Vec::new(),
        pending_primary_key: r_json(trow, 20),
        primary_definition_scn: r_u64(trow, 21),
        next_index_id: r_u64(trow, 22),
        primary_publication_job_id: r_u64(trow, 23),
        partitioning: r_json::<Option<PartitionInfo>>(trow, 15),
        pctfree: table_options.pctfree,
        pctused: table_options.pctused,
        mysql_table_options: table_options.mysql_options,
        column_name_map: Default::default(),
        column_id_map: HashMap::new(),
        row_codec: Arc::new(crate::storage::core::RowCodec::empty()),
    })
}

pub(crate) fn dd_row_for_schema(schema: &DatabaseSchema) -> Vec<Value> {
    vec![
        sv(&schema.name),
        sv(&schema.charset),
        sv(&schema.collation),
        bv(schema.system),
        uv(schema.created_ms),
        uv(schema.updated_ms),
    ]
}

pub(crate) fn dd_row_for_view(view: &ViewSchema) -> Vec<Value> {
    view_row(view)
}

pub(crate) fn dd_row_for_routine(routine: &RoutineSchema) -> Vec<Value> {
    routine_row(routine)
}

pub(crate) fn dd_row_for_trigger(trigger: &TriggerSchema) -> Vec<Value> {
    trigger_row(trigger)
}

pub(crate) fn dd_row_for_event(event: &EventSchema) -> Vec<Value> {
    event_row(event)
}

pub(crate) fn dd_row_for_sequence(sequence: &SequenceSchema) -> Vec<Value> {
    sequence_row(sequence)
}

/// Auth mutations are independent of table count. Until the authorization
/// layer moves to object-scoped CatalogTxn calls, replace only the bounded
/// auth DD families rather than the whole database catalog.
pub(crate) fn dd_rows_for_auth(auth: &AuthCatalog) -> DdRowSet {
    let mut catalog = super::catalog::empty_catalog();
    catalog.databases.clear();
    catalog.auth = std::sync::Arc::new(auth.clone());
    let mut set = catalog_to_dd_rows(&catalog);
    set.dd_properties
        .retain(|row| row.first().and_then(crate::types::value_str) == Some("auth_epoch"));
    set
}

pub(crate) fn auth_grant_key(grant: &GrantEntry) -> String {
    format!(
        "{}|{}|{}",
        account_id(&grant.grantee),
        serde_json::to_string(&grant.scope).unwrap_or_default(),
        grant.privilege.sql_name()
    )
}

pub(crate) fn auth_role_edge_key(edge: &RoleEdge, is_default: bool) -> String {
    format!(
        "{}|{}|{}",
        u8::from(is_default),
        account_id(&edge.from_role),
        account_id(&edge.to_account)
    )
}

pub(crate) fn auth_restriction_key(restriction: &crate::engine::auth::RestrictionEntry) -> String {
    format!(
        "{}|{}|{}",
        account_id(&restriction.grantee),
        restriction.database,
        restriction.privilege.sql_name()
    )
}

/// Parse a persisted `update_rule` / `delete_rule` string back into a
/// [`ReferentialAction`]. Unknown values fall back to `NO ACTION` (the
/// MySQL default and the safest enforcement).
fn parse_referential_action(s: &str) -> ReferentialAction {
    match s.to_ascii_uppercase().as_str() {
        "CASCADE" => ReferentialAction::Cascade,
        "SET NULL" => ReferentialAction::SetNull,
        "RESTRICT" => ReferentialAction::Restrict,
        _ => ReferentialAction::NoAction,
    }
}

/// Rebuild the database + base-table portion of a [`Catalog`] from
/// dictionary rows. The reverse of [`catalog_to_dd_rows`]. Runtime-only
/// fields are re-derived by `build_column_index`.
pub(crate) fn catalog_from_dd_rows(set: &DdRowSet) -> Result<Catalog, crate::types::QueryError> {
    catalog_from_dd_rows_with_auth(set, reconstruct_auth(set))
}

/// Decode schema metadata for a temporary descriptor graph without creating
/// bootstrap credentials. Authorization is loaded separately by the live catalog.
pub(crate) fn catalog_schema_from_dd_rows(
    set: &DdRowSet,
) -> Result<Catalog, crate::types::QueryError> {
    catalog_from_dd_rows_with_auth(set, AuthCatalog::empty())
}

fn catalog_from_dd_rows_with_auth(
    set: &DdRowSet,
    auth: AuthCatalog,
) -> Result<Catalog, crate::types::QueryError> {
    // Partial descriptor snapshots omit properties. A persisted authority must
    // never be silently relabelled with the current binary's format version.
    for row in &set.dd_properties {
        if r_str(row, 0) == "catalog_version" {
            let persisted = r_str(row, 1);
            if persisted.parse::<u32>().ok() != Some(CATALOG_VERSION) {
                return Err(crate::storage::error(crate::error::catalog::ER_GET_ERRNO,
                    format!("catalog version '{persisted}' is incompatible with mydb development storage format version {CATALOG_VERSION}; use a new data directory or rebuild data")));
            }
        }
    }
    let mut databases = BTreeMap::new();
    for row in &set.schemata {
        let name = r_str(row, 0);
        databases.insert(
            name.clone(),
            DatabaseSchema {
                name,
                system: r_bool(row, 3),
                charset: r_str(row, 1),
                collation: r_str(row, 2),
                created_ms: r_u64(row, 4),
                updated_ms: r_u64(row, 5),
            },
        );
    }

    let mut cols_by_table: HashMap<u64, Vec<&Vec<Value>>> = HashMap::new();
    for row in &set.columns {
        cols_by_table.entry(r_u64(row, 0)).or_default().push(row);
    }
    let mut idx_by_table: HashMap<u64, Vec<&Vec<Value>>> = HashMap::new();
    for row in &set.indexes {
        idx_by_table.entry(r_u64(row, 0)).or_default().push(row);
    }
    let mut icu_by_index: HashMap<(u64, String), Vec<&Vec<Value>>> = HashMap::new();
    for row in &set.index_column_usage {
        icu_by_index
            .entry((r_u64(row, 0), r_str(row, 1)))
            .or_default()
            .push(row);
    }
    let mut stats_by_table = set
        .table_stats
        .iter()
        .map(|row| (r_u64(row, 0), row))
        .collect::<HashMap<_, _>>();
    let mut chk_by_table: HashMap<u64, Vec<&Vec<Value>>> = HashMap::new();
    for row in &set.check_constraints {
        chk_by_table.entry(r_u64(row, 0)).or_default().push(row);
    }
    let mut fk_by_table: HashMap<u64, Vec<&Vec<Value>>> = HashMap::new();
    for row in &set.foreign_keys {
        fk_by_table.entry(r_u64(row, 0)).or_default().push(row);
    }
    let mut fkcu_by_fk: HashMap<(u64, String), Vec<&Vec<Value>>> = HashMap::new();
    for row in &set.foreign_key_column_usage {
        fkcu_by_fk
            .entry((r_u64(row, 0), r_str(row, 1)))
            .or_default()
            .push(row);
    }

    let mut tables = BTreeMap::new();
    let mut max_id = 0u64;
    for trow in &set.tables {
        let id = r_u64(trow, 0);
        max_id = max_id.max(id);

        let mut col_rows = cols_by_table.remove(&id).unwrap_or_default();
        col_rows.sort_by_key(|r| r_u64(r, 4)); // physical_pos
        let columns: Vec<ColumnSchema> = col_rows.iter().map(|r| reconstruct_column(r)).collect();

        let mut idx_rows = idx_by_table.remove(&id).unwrap_or_default();
        idx_rows.sort_by_key(|r| r_u64(r, 1)); // ordinal_position
        let indexes = idx_rows
            .iter()
            .map(|r| {
                let parts = icu_by_index.remove(&(id, r_str(r, 2))).unwrap_or_default();
                let mut index = reconstruct_index(r, parts);
                if index.is_vector() {
                    let raw = r.get(13).and_then(crate::types::value_str).ok_or_else(|| {
                        crate::storage::error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "missing vector_config in data dictionary",
                        )
                    })?;
                    index.vector = serde_json::from_str(raw).map_err(|e| {
                        crate::storage::error(
                            crate::error::catalog::ER_GET_ERRNO,
                            format!("corrupt vector_config: {e}"),
                        )
                    })?;
                    let algo = index.vector_algo();
                    let config = index.vector.as_mut().ok_or_else(|| {
                        crate::storage::error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "missing vector index configuration",
                        )
                    })?;
                    let column = index
                        .columns
                        .first()
                        .and_then(|name| columns.iter().find(|c| c.name.eq_ignore_ascii_case(name)))
                        .ok_or_else(|| {
                            crate::storage::error(
                                crate::error::catalog::ER_GET_ERRNO,
                                "missing vector index column",
                            )
                        })?;
                    if let DataTypeKind::Vector {
                        datatype,
                        dim: Some(dim),
                    } = column.data_type
                    {
                        config.resolve(datatype, dim, algo)?;
                    } else {
                        return Err(crate::storage::error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "invalid vector index column type",
                        ));
                    }
                }
                Ok(index)
            })
            .collect::<Result<Vec<_>, crate::types::QueryError>>()?;

        let mut chk_rows = chk_by_table.remove(&id).unwrap_or_default();
        chk_rows.sort_by(|a, b| r_str(a, 1).cmp(&r_str(b, 1)));
        let check_constraints = chk_rows
            .iter()
            .map(|r| CheckConstraintSchema {
                name: r_str(r, 1),
                expr: r_str(r, 2),
                enforced: r_bool(r, 3),
                column_names: r_json::<Vec<String>>(r, 4),
                expr_ast: None,
                program: None,
            })
            .collect();

        let mut fk_rows = fk_by_table.remove(&id).unwrap_or_default();
        fk_rows.sort_by(|a, b| r_str(a, 1).cmp(&r_str(b, 1)));
        let foreign_keys = fk_rows
            .iter()
            .map(|r| {
                let fk_name = r_str(r, 1);
                let mut parts = fkcu_by_fk
                    .remove(&(id, fk_name.clone()))
                    .unwrap_or_default();
                parts.sort_by_key(|p| r_u64(p, 2)); // ordinal_position
                ForeignKeySchema {
                    name: fk_name,
                    columns: parts.iter().map(|p| r_str(p, 3)).collect(),
                    referenced_database: r_str(r, 2),
                    referenced_table: r_str(r, 3),
                    referenced_columns: parts.iter().map(|p| r_str(p, 4)).collect(),
                    on_update: parse_referential_action(&r_str(r, 4)),
                    on_delete: parse_referential_action(&r_str(r, 5)),
                    backing_index: r_str(r, 6),
                    backing_index_id: r_u64(r, 7),
                    child_col_idx: Vec::new(),
                }
            })
            .collect();

        let database = r_str(trow, 2);
        let name = r_str(trow, 3);
        let stats = match stats_by_table.remove(&id).and_then(|row| row.get(2)) {
            Some(Value::String(s)) => {
                serde_json::from_str(s).unwrap_or_else(|_| TableStats::empty())
            }
            Some(Value::SmallString(b)) => {
                serde_json::from_str(unsafe { std::str::from_utf8_unchecked(b) })
                    .unwrap_or_else(|_| TableStats::empty())
            }
            _ => TableStats::empty(),
        };
        let table_options: crate::storage::schema::TableOptions = match trow.get(18) {
            Some(Value::String(s)) if !s.is_empty() => serde_json::from_str(s).unwrap_or_default(),
            Some(Value::SmallString(b)) if !b.is_empty() => {
                serde_json::from_str(unsafe { std::str::from_utf8_unchecked(b) })
                    .unwrap_or_default()
            }
            _ => crate::storage::schema::TableOptions::default(),
        };
        let mut table = TableSchema {
            id,
            object_id: r_u64(trow, 1),
            active_generation: r_u64(trow, 19).max(1),
            database: database.clone(),
            name: name.clone(),
            engine: r_str(trow, 4),
            charset: r_str(trow, 5),
            collation: r_str(trow, 6),
            comment: r_str(trow, 7),
            created_ms: r_u64(trow, 16),
            updated_ms: r_u64(trow, 17),
            table_type: r_str(trow, 8),
            columns,
            primary_key: r_json::<Vec<String>>(trow, 14),
            indexes,
            check_constraints,
            foreign_keys,
            fk_referenced_by: Vec::new(),
            stats,
            system: r_bool(trow, 9),
            schema_version: r_u64(trow, 10),
            system_time_epoch_us: table_options.system_time_epoch_us,
            row_generation_txn_id: table_options.row_generation_txn_id,
            next_column_id: r_u64(trow, 11) as u32,
            active_layout_id: r_u64(trow, 12) as u32,
            row_layouts: r_json(trow, 13),
            pending_primary_key: r_json(trow, 20),
            primary_definition_scn: r_u64(trow, 21),
            next_index_id: r_u64(trow, 22),
            primary_publication_job_id: r_u64(trow, 23),
            partitioning: r_json::<Option<PartitionInfo>>(trow, 15),
            pctfree: table_options.pctfree,
            pctused: table_options.pctused,
            mysql_table_options: table_options.mysql_options,
            column_name_map: Default::default(),
            column_id_map: HashMap::new(),
            row_codec: Arc::new(crate::storage::core::RowCodec::empty()),
        };
        table.build_column_index();
        tables.insert(table_key(&database, &name), Arc::new(table));
    }

    let mut views = BTreeMap::new();
    for row in &set.views {
        let v = reconstruct_view(row)?;
        views.insert(table_key(&v.database, &v.name), v);
    }
    let mut routines = BTreeMap::new();
    for row in &set.routines {
        let r = reconstruct_routine(row);
        routines.insert(
            routine_key(&r.database, r.kind.lower(), &r.name),
            std::sync::Arc::new(r),
        );
    }
    let mut triggers = BTreeMap::new();
    for row in &set.triggers {
        let t = reconstruct_trigger(row);
        triggers.insert(trigger_key(&t.database, &t.name), t);
    }
    let mut events = BTreeMap::new();
    for row in &set.events {
        let e = reconstruct_event(row);
        events.insert(event_key(&e.database, &e.name), e);
    }
    let mut sequences = BTreeMap::new();
    let mut max_seq_id = 0u64;
    for row in &set.sequences {
        let s = reconstruct_sequence(row)?;
        max_seq_id = max_seq_id.max(s.id).max(s.counter_id);
        sequences.insert(table_key(&s.database, &s.name), Arc::new(s));
    }

    let property_u64 = |name: &str| {
        set.dd_properties.iter().find_map(|row| {
            (r_str(row, 0) == name)
                .then(|| r_str(row, 1).parse::<u64>().ok())
                .flatten()
        })
    };

    Ok(Catalog {
        version: CATALOG_VERSION,
        next_table_id: property_u64("next_table_id").unwrap_or(max_id + 1),
        next_sequence_id: property_u64("next_sequence_id").unwrap_or(max_seq_id + 1),
        databases,
        tables,
        views,
        routines,
        triggers,
        events,
        sequences,
        auth: std::sync::Arc::new(auth),
    })
}

fn reconstruct_auth(set: &DdRowSet) -> AuthCatalog {
    // A persisted authority is authoritative, including deleted bootstrap accounts.
    let mut auth = AuthCatalog::empty();
    for row in &set.users {
        if let Some(s) = row.get(2).and_then(crate::types::value_str) {
            if let Ok(account) = serde_json::from_str::<UserAccount>(s) {
                auth.accounts.insert(account_id(&account.key), account);
            }
        }
    }
    let mut grant_rows: Vec<(String, GrantEntry)> = set
        .grants
        .iter()
        .filter_map(|row| {
            let s = row.get(1).and_then(crate::types::value_str)?;
            serde_json::from_str::<GrantEntry>(s)
                .ok()
                .map(|g| (r_str(row, 0), g))
        })
        .collect();
    grant_rows.sort_by(|left, right| left.0.cmp(&right.0));
    auth.grants = grant_rows.into_iter().map(|(_, g)| g).collect();

    let mut edges: Vec<(String, RoleEdge)> = Vec::new();
    let mut defaults: Vec<(String, RoleEdge)> = Vec::new();
    for row in &set.role_edges {
        if let Some(s) = row.get(2).and_then(crate::types::value_str) {
            if let Ok(edge) = serde_json::from_str::<RoleEdge>(s) {
                if r_bool(row, 1) {
                    defaults.push((r_str(row, 0), edge));
                } else {
                    edges.push((r_str(row, 0), edge));
                }
            }
        }
    }
    edges.sort_by(|left, right| left.0.cmp(&right.0));
    defaults.sort_by(|left, right| left.0.cmp(&right.0));
    auth.role_edges = edges.into_iter().map(|(_, e)| e).collect();
    auth.default_roles = defaults.into_iter().map(|(_, e)| e).collect();

    let mut restrictions = set
        .restrictions
        .iter()
        .filter_map(|row| {
            let json = row.get(1).and_then(crate::types::value_str)?;
            serde_json::from_str::<crate::engine::auth::RestrictionEntry>(json)
                .ok()
                .map(|restriction| (r_str(row, 0), restriction))
        })
        .collect::<Vec<_>>();
    restrictions.sort_by(|left, right| left.0.cmp(&right.0));
    auth.restrictions = restrictions
        .into_iter()
        .map(|(_, restriction)| restriction)
        .collect();

    for row in &set.dd_properties {
        if r_str(row, 0) == "auth_epoch" {
            auth.epoch = r_str(row, 1).parse().unwrap_or(0);
        }
    }
    auth.rebuild_runtime_indexes();
    auth
}

fn reconstruct_column(r: &[Value]) -> ColumnSchema {
    ColumnSchema {
        not_null_from_scn: r_u64(r, 23),
        name: r_str(r, 2),
        data_type: r_data_type(r, 6),
        nullable: r_bool(r, 7),
        unsigned: r_bool(r, 8),
        auto_increment: r_bool(r, 9),
        default: r_json::<Option<Value>>(r, 10),
        read_backfill_default: r_json::<Option<Value>>(r, 22),
        default_expr: r_opt_str(r, 11),
        on_update_expr: r_opt_str(r, 12),
        column_id: r_u64(r, 1) as u32,
        state: col_state_from(&r_str(r, 13)),
        physical_pos: r_u64(r, 4) as u32,
        logical_pos: r_u64(r, 5) as u32,
        ordinal_position: r_u64(r, 3) as u32,
        charset: r_opt_str(r, 14),
        collation: r_opt_str(r, 15),
        comment: r_str(r, 16),
        generated_expr: r_opt_str(r, 17),
        generated_stored: r_bool(r, 18),
        generated_column_names: r_json::<Vec<String>>(r, 19),
        zerofill: r_bool(r, 20),
        invisible: r_bool(r, 21),
        generated_expr_ast: None,
        generated_program: None,
    }
}

/// Physical index identity for startup validation. Key definitions are only
/// needed if validation discovers a file that must be rebuilt.
pub(crate) fn index_is_fulltext(row: &[Value]) -> bool {
    r_str(row, 3).eq_ignore_ascii_case("FULLTEXT")
}

pub(crate) fn index_storage_identity(row: &[Value]) -> Option<(u64, u64)> {
    (idx_state_from(&r_str(row, 7)) != IndexState::Building)
        .then(|| (r_u64(row, 0), r_u64(row, 10)))
}

fn reconstruct_index(irow: &[Value], mut parts: Vec<&Vec<Value>>) -> IndexSchema {
    parts.sort_by_key(|r| r_u64(r, 2)); // ordinal_position
    let mut columns = Vec::new();
    let mut prefix_lengths = Vec::new();
    let mut directions = Vec::new();
    let mut key_expressions: Vec<Option<IndexExpression>> = Vec::new();
    let mut included_columns = Vec::new();
    for p in &parts {
        if r_bool(p, 6) {
            included_columns.push(r_str(p, 3));
            continue;
        }
        match r_opt_str(p, 7) {
            Some(expr_sql) => {
                // Expression key part: `columns[pos]` carries the canonical
                // SQL text; the real key shape lives in `key_expressions`.
                columns.push(expr_sql.clone());
                key_expressions.push(Some(IndexExpression {
                    expr_sql,
                    data_type: r_data_type(p, 8),
                    nullable: r_bool(p, 9),
                    referenced_columns: r_json::<Vec<String>>(p, 10),
                    multi_valued: r_bool(p, 11),
                    ast: None,
                    program: None,
                }));
            }
            None => {
                columns.push(r_str(p, 3));
                key_expressions.push(None);
            }
        }
        prefix_lengths.push(r_opt_usize(p, 4));
        directions.push(dir_from(&r_str(p, 5)));
    }
    // Canonicalize to the empty-vec form used on the hot path when a
    // parallel array carries no information.
    if key_expressions.iter().all(Option::is_none) {
        key_expressions.clear();
    }
    if prefix_lengths.iter().all(Option::is_none) {
        prefix_lengths.clear();
    }
    if directions.iter().all(|d| *d == IndexColumnDirection::Asc) {
        directions.clear();
    }
    let index_type = r_str(irow, 3);
    // Persist the complete per-index snapshot: process environment changes
    // must never change the tokenizer of an existing physical index.
    let fulltext = r_json::<Option<crate::storage::schema::FulltextMeta>>(irow, 11);
    IndexSchema {
        readable_from_scn: r_u64(irow, 12),
        index_id: r_u64(irow, 10),
        name: r_str(irow, 2),
        columns,
        column_prefix_lengths: prefix_lengths,
        column_directions: directions,
        key_expressions,
        unique: r_bool(irow, 4),
        primary: r_bool(irow, 5),
        included_columns,
        state: idx_state_from(&r_str(irow, 7)),
        index_type,
        visible: r_bool(irow, 6),
        comment: r_str(irow, 8),
        fulltext,
        vector: r_json(irow, 13),
    }
}

// ---- views ----
fn view_row(v: &ViewSchema) -> Vec<Value> {
    vec![
        sv(&v.database),
        sv(&v.name),
        sv(&v.sql),
        jsonv(&v.columns),
        jsonv(&v.dependencies),
        sv(&v.algorithm),
        sv(&v.definer_user),
        sv(&v.definer_host),
        sv(v.security_type.as_str()),
        jsonv(&v.routine_dependencies),
        opt_sv(v.check_option.as_deref()),
        bv(v.is_updatable),
        sv(&v.character_set_client),
        sv(&v.collation_connection),
        bv(v.invalid),
        uv(v.schema_version),
        sv(&v.definition_normalized),
        v.query
            .as_ref()
            .map(|query| sv(query.encoded()))
            .unwrap_or(Value::Null),
    ]
}
fn reconstruct_view(r: &[Value]) -> Result<ViewSchema, crate::types::QueryError> {
    let encoded = match r.get(17) {
        None | Some(Value::Null) => None,
        Some(value) => Some(crate::types::value_str(value).ok_or_else(|| {
            crate::storage::error(
                crate::error::catalog::ER_VIEW_INVALID,
                format!(
                    "View '{}.{}': semantic query metadata must be JSON text",
                    r_str(r, 0),
                    r_str(r, 1)
                ),
            )
        })?),
    };
    let query = encoded
        .filter(|source| source.trim() != "null")
        .map(crate::storage::schema::ViewQuery::decode)
        .transpose()
        .map_err(|decode| {
            crate::storage::error(
                decode
                    .spec()
                    .unwrap_or(crate::error::catalog::ER_VIEW_INVALID),
                format!(
                    "View '{}.{}': {}",
                    r_str(r, 0),
                    r_str(r, 1),
                    decode.message()
                ),
            )
        })?;
    Ok(ViewSchema {
        database: r_str(r, 0),
        name: r_str(r, 1),
        sql: r_str(r, 2),
        columns: r_json::<Vec<ViewColumnDef>>(r, 3),
        dependencies: r_json::<Vec<ViewDependency>>(r, 4),
        algorithm: r_str_default(r, 5, "UNDEFINED"),
        definer_user: r_str_default(r, 6, "root"),
        definer_host: r_str_default(r, 7, "%"),
        security_type: match r_str_default(r, 8, "DEFINER").to_ascii_uppercase().as_str() {
            "INVOKER" => RoutineSecuritySchema::Invoker,
            _ => RoutineSecuritySchema::Definer,
        },
        routine_dependencies: r_json::<Vec<ViewRoutineDependency>>(r, 9),
        check_option: r_opt_str(r, 10),
        is_updatable: r_bool(r, 11),
        character_set_client: r_str_default(r, 12, crate::charset::DEFAULT_CHARSET),
        collation_connection: r_str_default(r, 13, crate::charset::DEFAULT_COLLATION),
        invalid: r_bool(r, 14),
        schema_version: r_u64(r, 15),
        // Missing in DD heaps written before this column → empty (raw echo).
        definition_normalized: r_str(r, 16),
        query,
    })
}

// ---- sequences ----
fn sequence_row(s: &SequenceSchema) -> Vec<Value> {
    vec![
        uv(s.id),
        sv(&s.database),
        sv(&s.name),
        sv(s.as_type.as_str()),
        Value::Int(s.increment),
        Value::Int(s.min_value),
        Value::Int(s.max_value),
        Value::Int(s.start_value),
        Value::Int(s.cache),
        bv(s.cycle),
        opt_sv(s.owned_table.as_deref()),
        opt_sv(s.owned_column.as_deref()),
        uv(s.created_ms),
        uv(s.updated_ms),
        uv(s.counter_id),
        opt_uv(s.owned_table_id),
        opt_uv(s.owned_column_id.map(u64::from)),
    ]
}
fn reconstruct_sequence(r: &[Value]) -> Result<SequenceSchema, crate::types::QueryError> {
    let corrupt = || {
        super::core::error(
            crate::error::catalog::ER_GET_ERRNO,
            "invalid sequence definition in catalog",
        )
    };
    if r.len() != 17 {
        return Err(corrupt());
    }
    let def = SequenceSchema {
        id: r_u64(r, 0),
        counter_id: r_u64(r, 14),
        owned_table_id: r_opt_usize(r, 15).map(|id| id as u64),
        owned_column_id: r_opt_usize(r, 16).map(|id| id as u32),
        database: r_str(r, 1),
        name: r_str(r, 2),
        as_type: SeqType::parse(&r_str(r, 3)).ok_or_else(corrupt)?,
        increment: r_i64(r, 4),
        min_value: r_i64(r, 5),
        max_value: r_i64(r, 6),
        start_value: r_i64(r, 7),
        cache: r_i64(r, 8),
        cycle: r_bool(r, 9),
        owned_table: r_opt_str(r, 10),
        owned_column: r_opt_str(r, 11),
        created_ms: r_u64(r, 12),
        updated_ms: r_u64(r, 13),
    };
    if def.id == 0
        || def.counter_id == 0
        || def.increment == 0
        || def.cache < 1
        || def.min_value < def.as_type.min()
        || def.max_value > def.as_type.max()
        || def.min_value > def.max_value
        || def.start_value < def.min_value
        || def.start_value > def.max_value
        || def.database.is_empty()
        || def.name.is_empty()
        || def.owned_table_id.is_some() != def.owned_column_id.is_some()
        || def.owned_table_id.is_some() != def.owned_table.is_some()
        || def.owned_table.is_some() != def.owned_column.is_some()
        || r_opt_usize(r, 16).is_some_and(|id| id == 0 || id > u32::MAX as usize)
    {
        return Err(corrupt());
    }
    Ok(def)
}

// ---- routines ----
fn routine_row(r: &RoutineSchema) -> Vec<Value> {
    vec![
        sv(&r.database),
        sv(&r.name),
        sv(r.kind.as_str()),
        sv(&r.definer_user),
        sv(&r.definer_host),
        sv(r.security_type.as_str()),
        r.return_type.as_ref().map_or(Value::Null, jsonv),
        opt_sv(r.return_type_sql.as_deref()),
        bv(r.return_unsigned),
        bv(r.deterministic),
        sv(r.sql_data_access.as_str()),
        sv(&r.comment),
        sv(&r.body_sql),
        jsonv(&r.params),
        uv(r.schema_version),
        uv(r.created_ms),
        uv(r.updated_ms),
        sv(&r.character_set_client),
        sv(&r.collation_connection),
        sv(&r.database_collation),
        sv(&r.sql_mode),
    ]
}
fn reconstruct_routine(r: &[Value]) -> RoutineSchema {
    RoutineSchema {
        database: r_str(r, 0),
        name: r_str(r, 1),
        kind: routine_kind_from(&r_str(r, 2)),
        definer_user: r_str(r, 3),
        definer_host: r_str(r, 4),
        security_type: routine_security_from(&r_str(r, 5)),
        return_type: r
            .get(6)
            .and_then(crate::types::value_str)
            .and_then(|s| serde_json::from_str(s).ok()),
        return_type_sql: r_opt_str(r, 7),
        return_unsigned: r_bool(r, 8),
        deterministic: r_bool(r, 9),
        sql_data_access: routine_sda_from(&r_str(r, 10)),
        comment: r_str(r, 11),
        body_sql: r_str(r, 12),
        params: r_json::<Vec<RoutineParamSchema>>(r, 13),
        schema_version: r_u64(r, 14),
        created_ms: r_u64(r, 15),
        updated_ms: r_u64(r, 16),
        character_set_client: r_str_default(r, 17, crate::charset::DEFAULT_CHARSET),
        collation_connection: r_str_default(r, 18, crate::charset::DEFAULT_COLLATION),
        database_collation: r_str_default(r, 19, crate::charset::DEFAULT_COLLATION),
        sql_mode: r_str(r, 20),
    }
}
fn routine_kind_from(s: &str) -> RoutineKindSchema {
    if s.eq_ignore_ascii_case("FUNCTION") {
        RoutineKindSchema::Function
    } else {
        RoutineKindSchema::Procedure
    }
}
fn routine_security_from(s: &str) -> RoutineSecuritySchema {
    if s.eq_ignore_ascii_case("INVOKER") {
        RoutineSecuritySchema::Invoker
    } else {
        RoutineSecuritySchema::Definer
    }
}
fn routine_sda_from(s: &str) -> RoutineSqlDataAccessSchema {
    match s.to_ascii_uppercase().as_str() {
        "NO SQL" => RoutineSqlDataAccessSchema::NoSql,
        "READS SQL DATA" => RoutineSqlDataAccessSchema::ReadsSqlData,
        "MODIFIES SQL DATA" => RoutineSqlDataAccessSchema::ModifiesSqlData,
        _ => RoutineSqlDataAccessSchema::ContainsSql,
    }
}

// ---- triggers ----
fn trigger_row(t: &TriggerSchema) -> Vec<Value> {
    vec![
        sv(&t.database),
        sv(&t.name),
        sv(&t.table_database),
        sv(&t.table_name),
        sv(t.timing.as_str()),
        sv(t.event.as_str()),
        sv(&t.definer_user),
        sv(&t.definer_host),
        sv(&t.body_sql),
        sv(&t.create_sql),
        uv(u64::from(t.action_order)),
        uv(t.schema_version),
        uv(t.created_ms),
        sv(&t.sql_mode),
        sv(&t.character_set_client),
        sv(&t.collation_connection),
        sv(&t.database_collation),
    ]
}
fn reconstruct_trigger(r: &[Value]) -> TriggerSchema {
    TriggerSchema {
        database: r_str(r, 0),
        name: r_str(r, 1),
        table_database: r_str(r, 2),
        table_name: r_str(r, 3),
        timing: if r_str(r, 4).eq_ignore_ascii_case("AFTER") {
            TriggerTimingSchema::After
        } else {
            TriggerTimingSchema::Before
        },
        event: trigger_event_from(&r_str(r, 5)),
        definer_user: r_str(r, 6),
        definer_host: r_str(r, 7),
        body_sql: r_str(r, 8),
        create_sql: r_str(r, 9),
        action_order: r_u64(r, 10) as u32,
        schema_version: r_u64(r, 11),
        created_ms: r_u64(r, 12),
        sql_mode: r_str(r, 13),
        character_set_client: r_str(r, 14),
        collation_connection: r_str(r, 15),
        database_collation: r_str(r, 16),
    }
}
fn trigger_event_from(s: &str) -> TriggerEventSchema {
    match s.to_ascii_uppercase().as_str() {
        "UPDATE" => TriggerEventSchema::Update,
        "DELETE" => TriggerEventSchema::Delete,
        _ => TriggerEventSchema::Insert,
    }
}

// ---- events ----
fn event_row(e: &EventSchema) -> Vec<Value> {
    vec![
        sv(&e.database),
        sv(&e.name),
        sv(&e.definer_user),
        sv(&e.definer_host),
        jsonv(&e.schedule),
        sv(e.on_completion.as_str()),
        sv(e.status.as_str()),
        sv(&e.comment),
        sv(&e.body_sql),
        sv(&e.create_sql),
        uv(e.created_ms),
        uv(e.last_altered_ms),
        uv(e.last_executed_ms),
        uv(e.schema_version),
        sv(&e.sql_mode),
        sv(&e.time_zone),
        sv(&e.character_set_client),
        sv(&e.collation_connection),
        sv(&e.database_collation),
    ]
}
fn reconstruct_event(r: &[Value]) -> EventSchema {
    EventSchema {
        database: r_str(r, 0),
        name: r_str(r, 1),
        definer_user: r_str(r, 2),
        definer_host: r_str(r, 3),
        schedule: r
            .get(4)
            .and_then(crate::types::value_str)
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or(EventScheduleSchema::OneShot { execute_at_ms: 0 }),
        on_completion: if r_str(r, 5).eq_ignore_ascii_case("PRESERVE") {
            EventOnCompletionSchema::Preserve
        } else {
            EventOnCompletionSchema::NotPreserve
        },
        status: event_status_from(&r_str(r, 6)),
        comment: r_str(r, 7),
        body_sql: r_str(r, 8),
        create_sql: r_str(r, 9),
        created_ms: r_u64(r, 10),
        last_altered_ms: r_u64(r, 11),
        last_executed_ms: r_u64(r, 12),
        schema_version: r_u64(r, 13),
        sql_mode: r_str(r, 14),
        time_zone: {
            let tz = r_str(r, 15);
            if tz.is_empty() {
                "SYSTEM".to_string()
            } else {
                tz
            }
        },
        character_set_client: r_str(r, 16),
        collation_connection: r_str(r, 17),
        database_collation: r_str(r, 18),
    }
}
fn event_status_from(s: &str) -> EventStatusSchema {
    match s.to_ascii_uppercase().as_str() {
        "DISABLED" => EventStatusSchema::Disabled,
        "SLAVESIDE_DISABLED" => EventStatusSchema::DisabledOnSlave,
        _ => EventStatusSchema::Enabled,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_format_rejects_stale_persisted_catalog_version() {
        for version in [
            (CATALOG_VERSION - 1).to_string(),
            (CATALOG_VERSION + 1).to_string(),
            "invalid".into(),
        ] {
            let mut set = DdRowSet::default();
            set.dd_properties
                .push(vec![sv("catalog_version"), sv(&version)]);
            let error = catalog_from_dd_rows(&set).unwrap_err();
            assert!(error.message().contains("new data directory"), "{error}");
        }
        let mut set = DdRowSet::default();
        set.dd_properties
            .push(vec![sv("catalog_version"), sv(CATALOG_VERSION.to_string())]);
        assert!(catalog_from_dd_rows(&set).is_ok());
        assert!(catalog_schema_from_dd_rows(&DdRowSet::default()).is_ok());
    }

    #[test]
    fn audit_fulltext_config_roundtrip() {
        let mut catalog = sample_catalog();
        let table = std::sync::Arc::make_mut(catalog.tables.get_mut("test.t").unwrap());
        let idx = &mut table.indexes[1];
        idx.index_type = "FULLTEXT".into();
        idx.fulltext = Some(crate::storage::schema::FulltextMeta {
            parser: "ngram".into(),
            ngram_size: Some(2),
            min_token_size: Some(4),
            max_token_size: Some(60),
            use_stopwords: Some(false),
            positions: Some(false),
            position_gap: Some(7),
        });
        let expected = serde_json::to_value(&idx.fulltext).unwrap();
        let rebuilt = catalog_from_dd_rows(&catalog_to_dd_rows(&catalog)).unwrap();
        assert_eq!(
            serde_json::to_value(&rebuilt.tables["test.t"].indexes[1].fulltext).unwrap(),
            expected
        );
    }

    #[test]
    fn dd_tables_have_unique_reserved_ids_and_codecs() {
        let tables = dd_tables();
        assert!(!tables.is_empty());
        let mut seen = std::collections::HashSet::new();
        for table in tables {
            // Ids are unique and inside the reserved high range.
            assert!(seen.insert(table.id), "duplicate dd table id {}", table.id);
            assert!(is_dd_table_id(table.id), "dd id {} out of range", table.id);
            assert_eq!(table.database, DD_DATABASE);
            assert!(table.system);
            // build_column_index ran: every column has a stable id and a
            // monotonically increasing ordinal, and the row codec exists.
            assert!(table.next_column_id > table.columns.len() as u32 - 1);
            for (i, c) in table.columns.iter().enumerate() {
                assert_ne!(
                    c.column_id, 0,
                    "{}.{} unassigned column id",
                    table.name, c.name
                );
                assert_eq!(
                    c.physical_pos as usize, i,
                    "{}.{} phys pos",
                    table.name, c.name
                );
            }
            assert!(table.column_name_map.len() == table.columns.len());
        }
    }

    #[test]
    fn dd_table_schema_lookup() {
        assert_eq!(dd_table_schema(DD_TABLES).unwrap().name, "tables");
        assert_eq!(dd_table_schema(DD_COLUMNS).unwrap().name, "columns");
        assert!(dd_table_schema(9_999).is_none());
    }

    #[test]
    fn dd_tables_are_hidden_or_explicit_mysql_compat_projections() {
        for table in dd_tables() {
            let hidden = crate::engine::mysql_schema::is_hidden_dd_table(DD_DATABASE, &table.name);
            let mysql_projection =
                crate::engine::information_schema::find_system_table(DD_DATABASE, &table.name)
                    .is_some();
            match table.name.as_str() {
                // MySQL 8.0 exposes mysql.role_edges as a compatibility system
                // table. mydb also persists auth role edges in an internal DD
                // heap with the same logical name, so direct SELECTs must route
                // to the public projection, not the raw DD rows.
                "role_edges" => assert!(
                    mysql_projection && !hidden,
                    "mysql.role_edges must stay a public compatibility projection"
                ),
                _ => assert!(
                    hidden,
                    "raw DD table mysql.{} must be hidden or explicitly classified",
                    table.name
                ),
            }
        }

        let err = crate::engine::mysql_schema::inaccessible_table_error("tables");
        assert_eq!(err.code(), 3554);
        assert_eq!(err.sql_state(), "HY000");
    }

    fn sample_catalog() -> Catalog {
        let mut id_col = col("id", DataTypeKind::Int, false);
        id_col.auto_increment = true;
        let name_col = col("name", varchar(64), true);
        let mut table = TableSchema {
            id: 1,
            object_id: 1,
            active_generation: 1,
            database: "test".to_string(),
            name: "t".to_string(),
            engine: "mydb".to_string(),
            charset: crate::charset::DEFAULT_CHARSET.to_string(),
            collation: crate::charset::DEFAULT_COLLATION.to_string(),
            comment: "hi".to_string(),
            created_ms: 10,
            updated_ms: 20,
            table_type: "BASE TABLE".to_string(),
            columns: vec![id_col, name_col],
            foreign_keys: Vec::new(),
            fk_referenced_by: Vec::new(),
            primary_key: vec!["id".to_string()],
            indexes: vec![
                IndexSchema {
                    readable_from_scn: 0,
                    index_id: 0,
                    name: "PRIMARY".to_string(),
                    columns: vec!["id".to_string()],
                    column_prefix_lengths: Vec::new(),
                    column_directions: Vec::new(),
                    key_expressions: Vec::new(),
                    unique: true,
                    primary: true,
                    included_columns: Vec::new(),
                    state: IndexState::Public,
                    index_type: "BTREE".to_string(),
                    visible: true,
                    comment: String::new(),
                    vector: None,
                    fulltext: None,
                },
                IndexSchema {
                    readable_from_scn: 0,
                    index_id: 0,
                    name: "idx_name".to_string(),
                    columns: vec!["name".to_string()],
                    column_prefix_lengths: vec![Some(10)],
                    column_directions: vec![IndexColumnDirection::Desc],
                    key_expressions: Vec::new(),
                    unique: false,
                    primary: false,
                    included_columns: Vec::new(),
                    state: IndexState::Public,
                    index_type: "BTREE".to_string(),
                    visible: true,
                    comment: String::new(),
                    vector: None,
                    fulltext: None,
                },
            ],
            check_constraints: vec![CheckConstraintSchema {
                name: "c_pos".to_string(),
                expr: "id > 0".to_string(),
                enforced: true,
                column_names: vec!["id".to_string()],
                expr_ast: None,
                program: None,
            }],
            stats: TableStats::empty(),
            system: false,
            schema_version: 3,
            system_time_epoch_us: 0,
            row_generation_txn_id: 0,
            next_column_id: 0,
            active_layout_id: 1,
            row_layouts: Vec::new(),
            pending_primary_key: None,
            primary_definition_scn: 0,
            next_index_id: 0,
            primary_publication_job_id: 0,
            partitioning: None,
            pctfree: 0,
            pctused: 0,
            mysql_table_options: Vec::new(),
            column_name_map: Default::default(),
            column_id_map: HashMap::new(),
            row_codec: Arc::new(crate::storage::core::RowCodec::empty()),
        };
        table.build_column_index();

        let mut databases = BTreeMap::new();
        databases.insert(
            "test".to_string(),
            DatabaseSchema {
                name: "test".to_string(),
                system: false,
                charset: crate::charset::DEFAULT_CHARSET.to_string(),
                collation: crate::charset::DEFAULT_COLLATION.to_string(),
                created_ms: 1,
                updated_ms: 2,
            },
        );
        let mut tables = BTreeMap::new();
        tables.insert(table_key("test", "t"), Arc::new(table));
        Catalog {
            version: CATALOG_VERSION,
            next_table_id: 2,
            next_sequence_id: 1,
            databases,
            tables,
            views: BTreeMap::new(),
            routines: BTreeMap::new(),
            triggers: BTreeMap::new(),
            events: BTreeMap::new(),
            sequences: BTreeMap::new(),
            auth: std::sync::Arc::new(AuthCatalog::default()),
        }
    }

    #[test]
    fn dd_rows_match_column_counts() {
        let set = catalog_to_dd_rows(&sample_catalog());
        for (id, rows) in set.per_table() {
            let ncols = dd_table_schema(id).unwrap().columns.len();
            for row in rows {
                assert_eq!(row.len(), ncols, "dd table {id} row arity");
            }
        }
        // schemata(1) + one user table's rows exist.
        assert_eq!(set.schemata.len(), 1);
        assert_eq!(set.tables.len(), 1);
        assert_eq!(set.columns.len(), 2);
        assert_eq!(set.indexes.len(), 2);
        assert_eq!(set.check_constraints.len(), 1);
    }

    #[test]
    fn schema_decode_preserves_metadata_without_bootstrap_auth() {
        let set = catalog_to_dd_rows(&sample_catalog());
        let full = catalog_from_dd_rows(&set).unwrap();
        let schema = catalog_schema_from_dd_rows(&set).unwrap();
        assert!(!full.auth.accounts.is_empty());
        assert!(schema.auth.accounts.is_empty());
        assert!(schema.auth.grants.is_empty());
        // The descriptor path must share the complete schema decoder, including
        // column types, indexes and constraints, while omitting only auth.
        let mut expected = full;
        expected.auth = schema.auth.clone();
        assert_eq!(
            serde_json::to_value(&schema).unwrap(),
            serde_json::to_value(&expected).unwrap()
        );
    }

    #[test]
    fn empty_catalog_bootstraps_auth_only_when_migrated() {
        let empty = super::super::catalog::empty_catalog();
        assert!(empty.auth.accounts.is_empty());
        assert!(empty.auth.grants.is_empty());
        let catalog = crate::storage::naming::migrate_catalog(empty);
        let defaults = AuthCatalog::default();
        assert_eq!(catalog.auth.accounts.len(), defaults.accounts.len());
        let root_id = account_id(&crate::engine::auth::AccountKey::new("root", "localhost"));
        let root = catalog.auth.accounts.get(&root_id).expect("bootstrap root");
        let expected = defaults.accounts.get(&root_id).unwrap();
        assert_eq!(root.plugin, expected.plugin);
        let password =
            crate::util::env::var("MYDB_ROOT_PASSWORD").unwrap_or_else(|_| "root".into());
        assert!(crate::engine::auth::verify_plaintext_auth_string(
            &root.plugin,
            &root.authentication_string,
            &password,
        ));
        assert_eq!(catalog.auth.grants.len(), defaults.grants.len());
    }

    #[test]
    fn catalog_roundtrips_through_dd_rows() {
        let original = sample_catalog();
        let set = catalog_to_dd_rows(&original);
        let rebuilt = catalog_from_dd_rows(&set).unwrap();

        let db = rebuilt.databases.get("test").expect("test db");
        assert_eq!(db.charset, "utf8mb4");
        assert!(!db.system);

        let orig_t = original.tables.get("test.t").unwrap();
        let t = rebuilt.tables.get("test.t").expect("test.t");
        assert_eq!(t.id, orig_t.id);
        assert_eq!(t.name, "t");
        assert_eq!(t.comment, "hi");
        assert_eq!(t.schema_version, 3);
        assert_eq!(t.next_column_id, orig_t.next_column_id);
        assert_eq!(t.primary_key, vec!["id".to_string()]);

        assert_eq!(t.columns.len(), 2);
        assert_eq!(t.columns[0].name, "id");
        assert!(t.columns[0].auto_increment);
        assert!(!t.columns[0].nullable);
        assert_eq!(t.columns[0].data_type, DataTypeKind::Int);
        assert_eq!(t.columns[0].column_id, orig_t.columns[0].column_id);
        assert_eq!(t.columns[0].physical_pos, orig_t.columns[0].physical_pos);
        assert_eq!(t.columns[1].name, "name");
        assert!(t.columns[1].nullable);

        assert_eq!(t.indexes.len(), 2);
        let pk = t.indexes.iter().find(|i| i.name == "PRIMARY").unwrap();
        assert!(pk.primary && pk.unique);
        assert_eq!(pk.columns, vec!["id".to_string()]);
        let sec = t.indexes.iter().find(|i| i.name == "idx_name").unwrap();
        assert!(!sec.unique);
        assert_eq!(sec.columns, vec!["name".to_string()]);
        assert_eq!(sec.column_prefix_len(0), Some(10));
        assert_eq!(sec.column_direction(0), IndexColumnDirection::Desc);

        assert_eq!(t.check_constraints.len(), 1);
        assert_eq!(t.check_constraints[0].expr, "id > 0");
        assert!(t.check_constraints[0].enforced);

        // build_column_index re-derived the lookup maps / codec.
        assert_eq!(t.column_name_map.len(), 2);
    }

    #[test]
    fn stats_json_bounded_preserves_payloads_between_128k_and_1m() {
        use super::super::stats::{ColumnStats, HistogramBucket, ValueFrequency};

        let mut table = (**sample_catalog()
            .tables
            .get(&table_key("test", "t"))
            .unwrap())
        .clone();
        table.stats.row_count = 1_000;
        table.stats.columns.insert(
            "payload".to_string(),
            ColumnStats {
                null_count: 0,
                distinct_count: 1_000,
                min: Some(Value::String("a".to_string())),
                max: Some(Value::String("z".to_string())),
                top_values: Vec::new(),
                average_width_bytes: Some(256.0),
                histogram: Vec::new(),
                frequency: Default::default(),
            },
        );

        let col = table.stats.columns.get_mut("payload").unwrap();
        for i in 0..20 {
            let value = Value::String(format!("{i:02}-{}", "x".repeat(4096)));
            col.top_values.push(ValueFrequency {
                value: value.clone(),
                count: i + 1,
            });
            col.histogram.push(HistogramBucket {
                upper: value,
                cumulative_count: (i + 1) * 50,
            });
        }

        let full = serde_json::to_string(&table.stats).unwrap();
        assert!(
            full.len() > 128 * 1024,
            "test payload should exceed the superseded 128 KiB stats budget; got {}",
            full.len()
        );
        assert!(
            full.len() <= DD_STATS_JSON_CAP_BYTES,
            "test payload should fit the 1 MiB stats_json cap; got {}",
            full.len()
        );

        let Value::String(encoded) = stats_json_bounded(&table) else {
            panic!("stats_json_bounded should return JSON text");
        };
        assert_eq!(encoded, full);
        let decoded: TableStats = serde_json::from_str(&encoded).unwrap();
        let decoded_col = decoded.columns.get("payload").unwrap();
        assert_eq!(decoded_col.top_values.len(), 20);
        assert_eq!(decoded_col.histogram.len(), 20);
    }

    #[test]
    fn stats_json_bounded_preserves_small_conditional_domains_when_trimming_sketches() {
        use super::super::stats::{
            ColumnStats, ConditionalKeyDomainEntry, ConditionalKeyDomainStats, HistogramBucket,
            ValueFrequency,
        };

        let mut table = (**sample_catalog()
            .tables
            .get(&table_key("test", "t"))
            .unwrap())
        .clone();
        table.primary_key = vec!["d_date_sk".to_string()];
        table.indexes = vec![
            IndexSchema {
                readable_from_scn: 0,
                index_id: 0,
                name: "PRIMARY".to_string(),
                columns: vec!["d_date_sk".to_string()],
                column_prefix_lengths: Vec::new(),
                column_directions: Vec::new(),
                key_expressions: Vec::new(),
                unique: true,
                primary: true,
                included_columns: Vec::new(),
                state: IndexState::Public,
                index_type: "BTREE".to_string(),
                visible: true,
                comment: String::new(),
                vector: None,
                fulltext: None,
            },
            IndexSchema {
                readable_from_scn: 0,
                index_id: 0,
                name: "idx_year".to_string(),
                columns: vec!["d_year".to_string()],
                column_prefix_lengths: Vec::new(),
                column_directions: Vec::new(),
                key_expressions: Vec::new(),
                unique: false,
                primary: false,
                included_columns: Vec::new(),
                state: IndexState::Public,
                index_type: "BTREE".to_string(),
                visible: true,
                comment: String::new(),
                vector: None,
                fulltext: None,
            },
        ];
        table.stats = TableStats::empty();
        table.stats.row_count = 100_000;
        for col_idx in 0..80 {
            let name = format!("payload_{col_idx}");
            let mut col = ColumnStats {
                null_count: 0,
                distinct_count: 10_000,
                min: Some(Value::String("a".to_string())),
                max: Some(Value::String("z".to_string())),
                top_values: Vec::new(),
                average_width_bytes: Some(64.0),
                histogram: Vec::new(),
                frequency: Default::default(),
            };
            col.frequency.complete = true;
            col.frequency.source = crate::storage::FrequencySource::Exact;
            for bucket in 0..10 {
                let value = Value::String(format!("{col_idx:02}-{bucket:02}-{}", "x".repeat(2048)));
                col.top_values.push(ValueFrequency {
                    value: value.clone(),
                    count: bucket + 1,
                });
                col.histogram.push(HistogramBucket {
                    upper: value,
                    cumulative_count: (bucket + 1) * 100,
                });
            }
            table.stats.columns.insert(name, col);
        }
        for name in ["d_date_sk", "d_year"] {
            table.stats.columns.insert(
                name.to_string(),
                ColumnStats {
                    null_count: 0,
                    distinct_count: 1_000,
                    min: Some(Value::Int(1)),
                    max: Some(Value::Int(1_000)),
                    top_values: Vec::new(),
                    average_width_bytes: Some(8.0),
                    histogram: Vec::new(),
                    frequency: Default::default(),
                },
            );
        }
        table
            .stats
            .conditional_key_domains
            .push(ConditionalKeyDomainStats {
                filter_columns: vec!["d_year".to_string()],
                key_columns: vec!["d_date_sk".to_string()],
                entries: vec![ConditionalKeyDomainEntry {
                    filter_values: vec![Value::Int(2000)],
                    row_count: 366,
                    key_distinct_count: 366,
                    key_min: Some(Value::Int(2_451_545)),
                    key_max: Some(Value::Int(2_451_910)),
                }],
            });

        assert!(
            serde_json::to_string(&table.stats).unwrap().len() > DD_STATS_JSON_CAP_BYTES,
            "test payload should force stats_json trimming"
        );

        let Value::String(encoded) = stats_json_bounded(&table) else {
            panic!("stats_json_bounded should return JSON text");
        };
        assert!(encoded.len() <= DD_STATS_JSON_CAP_BYTES);
        let decoded: TableStats = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.conditional_key_domains.len(), 1);
        assert_eq!(
            decoded.conditional_key_domains[0].filter_columns,
            ["d_year".to_string()]
        );
        assert!(decoded.columns.contains_key("d_date_sk"));
        assert!(decoded.columns.contains_key("d_year"));
        assert!(
            decoded.columns.values().all(|col| col.top_values.is_empty()
                && col.histogram.is_empty()
                && !col.frequency.complete),
            "large per-column sketches should be trimmed before conditional domains"
        );
        assert_eq!(
            decoded.columns.len(),
            82,
            "dropping sketches should retain scalar statistics for every column"
        );
    }

    #[test]
    fn long_tail_objects_roundtrip() {
        use super::super::schema::{
            EventScheduleSchema, RoutineKindSchema, RoutineSecuritySchema,
            RoutineSqlDataAccessSchema, TriggerEventSchema, TriggerTimingSchema,
        };

        let mut catalog = sample_catalog();
        catalog.views.insert(
            table_key("test", "v"),
            ViewSchema {
                database: "test".into(),
                name: "v".into(),
                sql: "SELECT 1".into(),
                query: Some(
                    crate::storage::schema::ViewQuery::new(
                        match crate::parser::parse_statement("SELECT 1").unwrap() {
                            crate::parser::Statement::Select(select) => select,
                            _ => unreachable!(),
                        },
                    )
                    .unwrap(),
                ),
                algorithm: "UNDEFINED".into(),
                definer_user: "root".into(),
                definer_host: "%".into(),
                security_type: RoutineSecuritySchema::Definer,
                columns: Vec::new(),
                dependencies: Vec::new(),
                routine_dependencies: Vec::new(),
                invalid: false,
                schema_version: 1,
                check_option: None,
                is_updatable: false,
                character_set_client: crate::charset::DEFAULT_CHARSET.to_string(),
                collation_connection: crate::charset::DEFAULT_COLLATION.to_string(),
                definition_normalized: String::new(),
            },
        );
        catalog.routines.insert(
            routine_key("test", "procedure", "p"),
            std::sync::Arc::new(RoutineSchema {
                database: "test".into(),
                name: "p".into(),
                kind: RoutineKindSchema::Procedure,
                definer_user: "root".into(),
                definer_host: "%".into(),
                security_type: RoutineSecuritySchema::Definer,
                params: Vec::new(),
                return_type: None,
                return_type_sql: None,
                return_unsigned: false,
                deterministic: false,
                sql_data_access: RoutineSqlDataAccessSchema::ReadsSqlData,
                comment: String::new(),
                sql_mode: String::new(),
                body_sql: "BEGIN END".into(),
                schema_version: 1,
                created_ms: 0,
                updated_ms: 0,
                character_set_client: crate::charset::DEFAULT_CHARSET.into(),
                collation_connection: crate::charset::DEFAULT_COLLATION.into(),
                database_collation: crate::charset::DEFAULT_COLLATION.into(),
            }),
        );
        catalog.triggers.insert(
            trigger_key("test", "trg"),
            TriggerSchema {
                database: "test".into(),
                name: "trg".into(),
                table_database: "test".into(),
                table_name: "t".into(),
                timing: TriggerTimingSchema::Before,
                event: TriggerEventSchema::Insert,
                definer_user: "root".into(),
                definer_host: "%".into(),
                body_sql: "SET @x = 1".into(),
                create_sql: "CREATE TRIGGER trg ...".into(),
                action_order: 0,
                schema_version: 1,
                created_ms: 0,
                sql_mode: String::new(),
                character_set_client: crate::charset::DEFAULT_CHARSET.into(),
                collation_connection: crate::charset::DEFAULT_COLLATION.into(),
                database_collation: crate::charset::DEFAULT_COLLATION.into(),
            },
        );
        catalog.events.insert(
            event_key("test", "e"),
            EventSchema {
                database: "test".into(),
                name: "e".into(),
                definer_user: "root".into(),
                definer_host: "%".into(),
                schedule: EventScheduleSchema::OneShot {
                    execute_at_ms: 1000,
                },
                on_completion: EventOnCompletionSchema::NotPreserve,
                status: EventStatusSchema::Enabled,
                comment: String::new(),
                body_sql: "DO 1".into(),
                create_sql: "CREATE EVENT e ...".into(),
                created_ms: 0,
                last_altered_ms: 0,
                last_executed_ms: 0,
                schema_version: 1,
                sql_mode: String::new(),
                time_zone: "SYSTEM".into(),
                character_set_client: crate::charset::DEFAULT_CHARSET.into(),
                collation_connection: crate::charset::DEFAULT_COLLATION.into(),
                database_collation: crate::charset::DEFAULT_COLLATION.into(),
            },
        );

        let rebuilt = catalog_from_dd_rows(&catalog_to_dd_rows(&catalog)).unwrap();

        let v = rebuilt.views.get(&table_key("test", "v")).unwrap();
        assert_eq!(v.sql, "SELECT 1");
        assert!(!v.invalid);

        let p = rebuilt
            .routines
            .get(&routine_key("test", "procedure", "p"))
            .unwrap();
        assert_eq!(p.kind.as_str(), "PROCEDURE");
        assert_eq!(p.body_sql, "BEGIN END");
        assert_eq!(p.sql_data_access.as_str(), "READS SQL DATA");
        assert_eq!(p.security_type.as_str(), "DEFINER");

        let trg = rebuilt.triggers.get(&trigger_key("test", "trg")).unwrap();
        assert_eq!(trg.timing.as_str(), "BEFORE");
        assert_eq!(trg.event.as_str(), "INSERT");
        assert_eq!(trg.table_name, "t");

        let e = rebuilt.events.get(&event_key("test", "e")).unwrap();
        assert_eq!(e.status.as_str(), "ENABLED");
        match e.schedule {
            EventScheduleSchema::OneShot { execute_at_ms } => assert_eq!(execute_at_ms, 1000),
            _ => panic!("event schedule should be one-shot"),
        }
    }
    #[test]
    fn semantic_view_decode_errors_are_not_replaced_with_empty_queries() {
        let query = super::super::schema::ViewQuery::new(
            match crate::parser::parse_statement("SELECT 1 n").unwrap() {
                crate::parser::Statement::Select(select) => select,
                _ => unreachable!(),
            },
        )
        .unwrap();
        let bad_version =
            query
                .encoded()
                .replacen("\"format_version\":2", "\"format_version\":999", 1);
        for payload in [
            Value::String("{}".into()),
            Value::String(bad_version),
            Value::Int(42),
        ] {
            let mut row = vec![Value::Null; 18];
            row[0] = Value::String("test".into());
            row[1] = Value::String("corrupt".into());
            row[17] = payload;
            let set = DdRowSet {
                views: vec![row],
                ..DdRowSet::default()
            };
            for decoded in [
                catalog_schema_from_dd_rows(&set),
                catalog_from_dd_rows(&set),
            ] {
                let error = decoded.unwrap_err();
                assert_eq!(error.code(), 1356);
                assert!(error.message().contains("View 'test.corrupt'"));
            }
        }
    }
}
