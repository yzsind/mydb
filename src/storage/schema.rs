use crate::util::decimal::wide_to_f64 as v_to_f64_decimal;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::parser::{DataType, Expr};
use crate::types::{ColumnType, QueryError, Value};

use super::core::{error, RowId};
use super::naming::{catalog_version, default_database_name, normalize_name, table_key};
use super::sequence::SequenceSchema;
use super::stats::TableStats;
use crate::engine::auth::AuthCatalog;

pub const DEFAULT_DATABASE: &str = "test";
// v22: the catalog is no longer persisted as `catalog/data_dictionary.json`.
// It lives solely in the data-dictionary heap tables (`tables/<id>.heap`).
// v46 stores transactional current rows and updates only affected objects. Data
// dirs from earlier formats (including the v21 JSON catalog) are rejected.
// v24: `mysql.indexes` gained a `fulltext_parser` column so a FULLTEXT
// index's `WITH PARSER` choice survives restart. Older dictionary heaps lack
// the column and are rejected rather than silently misread.
// v25: added `mysql.foreign_keys` / `mysql.foreign_key_column_usage`
// dictionary tables for FOREIGN KEY support. Older catalogs lack these
// heaps and are rejected (rebuild the data directory).
// v26: JSON columns now persist as binary `jsonb` (engine::json) via the
// new `ColumnKind::Json` instead of VarString text. Catalogs predating
// this would have their JSON columns misread, so they are rejected.
// v27: added the GEOMETRY family (DataTypeKind::Geometry / ColumnKind::Geometry)
// for MySQL 8.0 spatial support, stored as internal `SRID + WKB` bytes, plus
// the SPATIAL index type. Older catalogs lack these and are rejected.
// v28: added the VECTOR family (DataTypeKind::Vector / ColumnKind::Vector) for
// similarity search, stored as a bare little-endian `f32` array, plus the HNSW
// vector index type. Older catalogs lack these and are rejected.
// v29: charset/collation defaults and persisted database/table/column metadata
// became semantically meaningful. Older development catalogs used
// utf8mb4_general_ci placeholders, so they are rejected rather than migrated.
// v30: TIME/DATETIME/TIMESTAMP now carry an `fsp: u8` field (fractional
// seconds precision, 0..=6 per MySQL 8.0). `ColumnKind::Time`/`DateTime`
// carry the same field so the codec can truncate the i64-microsecond
// payload on write. Older v29 catalogs serialise these as `"Time"`
// (unit) rather than `"Time": {"fsp": 0}` and are rejected.
// v31: `mysql.views` now stores MySQL 8.0 view execution metadata
// (ALGORITHM, DEFINER, SQL SECURITY, CHECK OPTION, charset/collation,
// updatability, and routine dependencies). Older view dictionary rows
// lack these columns and are rejected rather than guessed.
// v32: heap rows switch to MVCC row v5. Table schemas persist row layout
// metadata (`active_layout_id` + `row_layouts`) so online DDL can keep
// pre- and post-DDL layouts readable while new writes use the active one.
// This is an intentionally incompatible development-format break.
// v33: on-disk directory layout reshuffled — global service files move
// under `system/{wal,undo,replication,tmp}/` and table/index files move
// to per-database directories `schemas/<db>/{tables,indexes}/`, with
// every object file named `<table_name>__<table_id>__<part>.<ext>`
// (`part = "data"` for the table heap/lob/ai sidecars and the index
// name for `.idx`/`.hnsw`). RENAME TABLE now physically renames files.
// v34: `mysql.tables` gains a nullable `table_options_json` column that
// persists per-table physical-storage knobs (Oracle-style PCTFREE/
// PCTUSED today; future room for ITL slot count / async-merge cap).
// v35: functional index expression metadata gains `multi_valued` so MySQL
// 8.0 JSON multi-valued indexes can persist one logical row as multiple
// B-tree entries.
// Older dictionary rows lack the column and are rejected rather than treating
// a multi-valued key as an ordinary functional scalar.
// v36: rowid-6bytes refactor. Heap pages, B-tree pages, WAL records and
// undo records all encode RowId without the 2-byte `generation` field
// (RID_KIND_FULL: 8 → 6 B, RID_KIND_SAME_PAGE: 4 → 2 B; per-record
// HeapInsert/Update/Delete saves 2 B). Bumping the catalog version
// keeps old data dirs (which mixed v35 catalog metadata with the wider
// row-id encoding) from being opened by a v36 binary; matching the
// "rebuild data dir on version mismatch" policy in CLAUDE.md.
// v37: PostgreSQL-style SEQUENCE objects. The catalog gains a `sequences`
// map + `next_sequence_id`, persisted in the new `mysql.sequences`
// dictionary heap. Older catalogs lack the heap (which would shift nothing
// for existing tables but leaves the dictionary one table short), so they
// are rejected rather than opened without sequence support.
// v38: ANALYZE persists conditional key-domain statistics in table stats JSON
// for CBO star-join cardinality. Older catalogs are rejected so plans do not
// silently run with a stats shape calibrated for the previous estimator.
// v40: TIMESTAMP values are stored as absolute UTC instants and converted at
// the session boundary; DATETIME remains a timezone-free wall-clock value.
// The physical slot width is unchanged, but opening an older directory would
// reinterpret its naive TIMESTAMP payloads, so the format break is explicit.
// v41: spatial column SRID constraints and the MySQL 8.0.46 SRS
// data-dictionary contract become authoritative catalog semantics. This is
// the first product version, so older development directories must rebuild.
pub use crate::storage::format_versions::CATALOG_VERSION;

/// MySQL 8.0 COMMENT length caps. These are hard limits enforced at DDL
/// time regardless of `sql_mode`, measured in *characters* (not bytes,
/// not the underlying storage-column byte cap). Kept here as the single
/// source of truth so CREATE TABLE and every ALTER path share one rule.
pub const MAX_TABLE_COMMENT_CHARS: usize = 2048;
pub const MAX_COLUMN_COMMENT_CHARS: usize = 1024;
pub const MAX_INDEX_COMMENT_CHARS: usize = 1024;
pub const MAX_PARTITION_COMMENT_CHARS: usize = 1024;

/// The kind of schema object a COMMENT is attached to. Selects the cap and
/// the matching MySQL error (errno + message wording).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommentTarget {
    Table,
    Field,
    Index,
    Partition,
}

/// Reject an over-long COMMENT with the MySQL 8.0 error for its object kind.
/// `name` is the object's identifier (table/column/index/partition name),
/// interpolated into the message exactly as MySQL does. Length is counted in
/// Unicode scalar values (`chars().count()`), matching MySQL's per-character
/// cap. This is the single enforcement point used by all DDL entries.
pub fn check_comment_len(
    target: CommentTarget,
    name: &str,
    comment: &str,
) -> Result<(), QueryError> {
    let (max, spec, obj) = match target {
        CommentTarget::Table => (
            MAX_TABLE_COMMENT_CHARS,
            crate::mysql::errors::ER_TOO_LONG_TABLE_COMMENT,
            "table",
        ),
        CommentTarget::Field => (
            MAX_COLUMN_COMMENT_CHARS,
            crate::mysql::errors::ER_TOO_LONG_FIELD_COMMENT,
            "field",
        ),
        CommentTarget::Index => (
            MAX_INDEX_COMMENT_CHARS,
            crate::mysql::errors::ER_TOO_LONG_INDEX_COMMENT,
            "index",
        ),
        CommentTarget::Partition => (
            MAX_PARTITION_COMMENT_CHARS,
            crate::mysql::errors::ER_TOO_LONG_TABLE_PARTITION_COMMENT,
            "table partition",
        ),
    };
    if comment.chars().count() > max {
        return Err(spec.error(format!(
            "Comment for {obj} '{name}' is too long (max = {max})"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Catalog {
    #[serde(default = "catalog_version")]
    pub version: u32,
    pub next_table_id: u64,
    #[serde(default)]
    pub next_sequence_id: u64,
    #[serde(default)]
    pub databases: BTreeMap<String, DatabaseSchema>,
    // Arc-shared so per-statement reads bump a refcount instead of cloning the
    // (potentially large) TableSchema and all its column statistics. Catalog
    // mutations replace the Arc atomically.
    pub tables: BTreeMap<String, Arc<TableSchema>>,
    #[serde(default)]
    pub views: BTreeMap<String, ViewSchema>,
    #[serde(default)]
    pub routines: BTreeMap<String, Arc<RoutineSchema>>,
    #[serde(default)]
    pub triggers: BTreeMap<String, TriggerSchema>,
    #[serde(default)]
    pub events: BTreeMap<String, EventSchema>,
    #[serde(default)]
    pub sequences: BTreeMap<String, Arc<SequenceSchema>>,
    #[serde(default)]
    pub auth: Arc<AuthCatalog>,
    #[serde(default)]
    pub(crate) ai: Arc<crate::engine::ai::AiCatalog>,
}

/// Rebuild every table's foreign-key runtime caches from the persisted
/// `foreign_keys` of all tables: resolves each FK's `child_col_idx`, and
/// denormalizes the reverse index (`fk_referenced_by`) onto each parent
/// (referenced) table. Idempotent; called after catalog load
/// (`migrate_catalog`) and after every FK-related DDL, before the
/// catalog is published. Foreign keys whose parent table or columns no
/// longer resolve are skipped for the reverse index but kept in the
/// child's `foreign_keys` (a dangling FK can only arise from corruption;
/// enforcement simply finds no parent index and the child-side check
/// short-circuits on unresolved indices).
pub fn build_fk_runtime(catalog: &mut Catalog) {
    // Read pass: resolve indices and collect inbound edges without
    // holding a mutable borrow.
    let mut inbound: HashMap<String, Vec<InboundFk>> = HashMap::new();
    // child table_key -> per-FK resolved child column indices.
    let mut child_idx: HashMap<String, Vec<Vec<usize>>> = HashMap::new();
    for (child_key, child) in &catalog.tables {
        if child.foreign_keys.is_empty() {
            continue;
        }
        let mut per_fk = Vec::with_capacity(child.foreign_keys.len());
        for fk in &child.foreign_keys {
            let cidx: Vec<usize> = fk
                .columns
                .iter()
                .filter_map(|c| child.column_index(c).ok())
                .collect();
            per_fk.push(cidx.clone());
            let parent_key = table_key(&fk.referenced_database, &fk.referenced_table);
            let Some(parent) = catalog.tables.get(&parent_key) else {
                continue;
            };
            let ridx: Vec<usize> = fk
                .referenced_columns
                .iter()
                .filter_map(|c| parent.column_index(c).ok())
                .collect();
            inbound.entry(parent_key).or_default().push(InboundFk {
                child_table_id: child.id,
                child_table_key: child_key.clone(),
                fk_name: fk.name.clone(),
                on_delete: fk.on_delete,
                on_update: fk.on_update,
                referenced_col_idx: ridx,
                child_col_idx: cidx,
                child_backing_index: fk.backing_index.clone(),
                child_backing_index_id: fk.backing_index_id,
            });
        }
        child_idx.insert(child_key.clone(), per_fk);
    }
    // Mutation pass.
    for (key, table) in &mut catalog.tables {
        let needs_child = child_idx.contains_key(key);
        let needs_inbound = inbound.contains_key(key);
        if !needs_child && !needs_inbound && table.fk_referenced_by.is_empty() {
            // Nothing changes for this table — avoid an Arc clone on the
            // DML hot-path tables that have no FK relationship at all.
            continue;
        }
        let t = Arc::make_mut(table);
        if let Some(per_fk) = child_idx.get(key) {
            for (fk, idx) in t.foreign_keys.iter_mut().zip(per_fk.iter()) {
                fk.child_col_idx = idx.clone();
            }
        }
        let referenced_by = inbound.remove(key).unwrap_or_default();
        if t.fk_referenced_by != referenced_by {
            // Prepared DML embeds this runtime schema, including referential
            // actions and the child incarnation. Invalidate it on FK changes.
            t.bump_schema_version();
            t.fk_referenced_by = referenced_by;
        }
    }
}

pub use super::view_query::ViewQuery;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewSchema {
    /// Versioned semantic query. SQL echo is not an execution format.
    #[serde(default)]
    pub query: Option<ViewQuery>,
    pub database: String,
    pub name: String,
    pub sql: String,
    #[serde(default = "default_view_algorithm")]
    pub algorithm: String,
    #[serde(default = "default_definer_user")]
    pub definer_user: String,
    #[serde(default = "default_definer_host")]
    pub definer_host: String,
    #[serde(default = "default_view_security")]
    pub security_type: RoutineSecuritySchema,
    /// Output column metadata, captured at CREATE/ALTER time so SHOW
    /// COLUMNS / DESCRIBE / information_schema.columns can answer
    /// without re-expanding the view body. Empty for catalogs written
    /// before column tracking was added — those are repopulated on the
    /// next ALTER VIEW.
    #[serde(default)]
    pub columns: Vec<ViewColumnDef>,
    /// Tables and views referenced by the body, in `(database, name)` form,
    /// collected during CREATE/ALTER validation. Used for reverse-lookup on
    /// DROP TABLE / ALTER TABLE / DROP VIEW so dependent views can be
    /// invalidated rather than silently turning into ghost views.
    #[serde(default)]
    pub dependencies: Vec<ViewDependency>,
    #[serde(default)]
    pub routine_dependencies: Vec<ViewRoutineDependency>,
    /// Set true when a referenced base table/view has been dropped or
    /// altered incompatibly; the next SELECT against this view will
    /// raise MySQL error 1356. CREATE OR REPLACE / ALTER VIEW clears it.
    #[serde(default)]
    pub invalid: bool,
    /// Bumped on every CREATE OR REPLACE / ALTER VIEW. Keys the parsed-AST
    /// cache so an in-flight stale entry is not reused after redefinition.
    #[serde(default)]
    pub schema_version: u64,
    /// `WITH CHECK OPTION` clause as parsed (None / "LOCAL" / "CASCADED").
    /// Stored for SHOW CREATE VIEW / information_schema.views fidelity.
    /// Enforcement (P1-A4): `resolve_updatable_view_schema` lifts the
    /// view's WHERE into `UpdatablePlan.check_predicate` (LOCAL = only
    /// this layer, CASCADED = AND-of-all-layers); `dispatch_insert_chunk`
    /// and `dml::validate_view_check` evaluate it against the
    /// post-image row and surface 1369 ER_VIEW_CHECK_FAILED on miss.
    #[serde(default)]
    pub check_option: Option<String>,
    #[serde(default)]
    pub is_updatable: bool,
    #[serde(default = "default_view_charset")]
    pub character_set_client: String,
    #[serde(default = "default_view_collation")]
    pub collation_connection: String,
    /// MySQL-style canonical view body (identifiers backtick-quoted, table
    /// names qualified, `*` expanded, projections aliased), generated at
    /// CREATE/ALTER time. `SHOW CREATE VIEW` and `information_schema.views`
    /// echo this instead of the user's raw `sql`. Empty for views written
    /// before normalization existed — callers lazily fall back to `sql`.
    #[serde(default)]
    pub definition_normalized: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ViewColumnDef {
    pub name: String,
    /// Source column type if the projection is a direct column reference;
    /// `None` for arbitrary expressions (their type is derived at execute
    /// time, matching MySQL's behaviour for expression columns in views).
    #[serde(default)]
    pub data_type: Option<DataTypeKind>,
    #[serde(default = "default_nullable")]
    pub nullable: bool,
    #[serde(default)]
    pub charset: Option<String>,
    #[serde(default)]
    pub collation: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ViewDependency {
    pub database: String,
    pub name: String,
    /// True if the referenced object is itself a view (vs a base table).
    #[serde(default)]
    pub is_view: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ViewRoutineDependency {
    pub database: String,
    pub name: String,
}

fn default_view_algorithm() -> String {
    "UNDEFINED".to_string()
}

fn default_definer_user() -> String {
    "root".to_string()
}

fn default_definer_host() -> String {
    "%".to_string()
}

fn default_view_security() -> RoutineSecuritySchema {
    RoutineSecuritySchema::Definer
}

fn default_view_charset() -> String {
    crate::charset::DEFAULT_CHARSET.to_string()
}

fn default_view_collation() -> String {
    crate::charset::DEFAULT_COLLATION.to_string()
}

impl ViewSchema {
    pub fn bump_schema_version(&mut self) {
        self.schema_version = self.schema_version.saturating_add(1);
    }

    /// In-memory identity used by compiled-plan dependency validation.
    /// `schema_version` alone is insufficient across DROP + CREATE because a
    /// newly-created view starts at version 1 again. The plan cache itself is
    /// not persistent, so a stable fingerprint of the complete semantic view
    /// definition gives it object-identity precision without changing the
    /// data-dictionary format.
    pub fn plan_cache_identity(&self) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.database.hash(&mut hasher);
        self.name.hash(&mut hasher);
        self.sql.hash(&mut hasher);
        // The same SQL/metadata can describe different semantics under creation
        // SQL modes. The codec captures that immutable AST identity once.
        self.query
            .as_ref()
            .map(ViewQuery::identity)
            .hash(&mut hasher);
        self.algorithm.hash(&mut hasher);
        self.definer_user.hash(&mut hasher);
        self.definer_host.hash(&mut hasher);
        self.security_type.as_str().hash(&mut hasher);
        self.invalid.hash(&mut hasher);
        self.schema_version.hash(&mut hasher);
        self.check_option.hash(&mut hasher);
        self.is_updatable.hash(&mut hasher);
        self.character_set_client.hash(&mut hasher);
        self.collation_connection.hash(&mut hasher);
        self.definition_normalized.hash(&mut hasher);
        // These metadata vectors do not derive Hash because some nested SQL
        // types deliberately do not. Their catalog JSON representation is
        // canonical for struct fields and is only computed during view-plan
        // compilation/validation, never on row execution.
        serde_json::to_string(&self.columns)
            .unwrap_or_default()
            .hash(&mut hasher);
        serde_json::to_string(&self.dependencies)
            .unwrap_or_default()
            .hash(&mut hasher);
        serde_json::to_string(&self.routine_dependencies)
            .unwrap_or_default()
            .hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RoutineKindSchema {
    Procedure,
    Function,
}

impl RoutineKindSchema {
    pub fn as_str(&self) -> &'static str {
        match self {
            RoutineKindSchema::Procedure => "PROCEDURE",
            RoutineKindSchema::Function => "FUNCTION",
        }
    }

    pub fn lower(&self) -> &'static str {
        match self {
            RoutineKindSchema::Procedure => "procedure",
            RoutineKindSchema::Function => "function",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RoutineParamModeSchema {
    In,
    Out,
    InOut,
}

impl RoutineParamModeSchema {
    pub fn as_str(&self) -> &'static str {
        match self {
            RoutineParamModeSchema::In => "IN",
            RoutineParamModeSchema::Out => "OUT",
            RoutineParamModeSchema::InOut => "INOUT",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutineSchema {
    pub database: String,
    pub name: String,
    pub kind: RoutineKindSchema,
    pub definer_user: String,
    pub definer_host: String,
    pub security_type: RoutineSecuritySchema,
    pub params: Vec<RoutineParamSchema>,
    #[serde(default)]
    pub return_type: Option<DataTypeKind>,
    #[serde(default)]
    pub return_type_sql: Option<String>,
    #[serde(default)]
    pub return_unsigned: bool,
    pub deterministic: bool,
    pub sql_data_access: RoutineSqlDataAccessSchema,
    #[serde(default)]
    pub comment: String,
    /// `sql_mode` captured at CREATE time. Replayed while the routine body
    /// executes so its behavior (division-by-zero, `NO_ZERO_DATE`,
    /// `PIPES_AS_CONCAT`, …) is independent of the caller's session mode.
    /// Empty for routines created before this field existed (`serde(default)`
    /// → old `mysql.routines` rows deserialize to empty, no `CATALOG_VERSION`
    /// bump).
    #[serde(default)]
    pub sql_mode: String,
    pub body_sql: String,
    #[serde(default)]
    pub schema_version: u64,
    #[serde(default)]
    pub created_ms: u64,
    #[serde(default)]
    pub updated_ms: u64,
    #[serde(default = "default_charset_client")]
    pub character_set_client: String,
    #[serde(default = "default_collation")]
    pub collation_connection: String,
    #[serde(default = "default_collation")]
    pub database_collation: String,
}

impl RoutineSchema {
    pub fn bump_schema_version(&mut self) {
        self.schema_version = self.schema_version.saturating_add(1);
    }

    pub fn definer(&self) -> String {
        format!("{}@{}", self.definer_user, self.definer_host)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TriggerTimingSchema {
    Before,
    After,
}

impl TriggerTimingSchema {
    pub fn as_str(&self) -> &'static str {
        match self {
            TriggerTimingSchema::Before => "BEFORE",
            TriggerTimingSchema::After => "AFTER",
        }
    }

    pub fn lower(&self) -> &'static str {
        match self {
            TriggerTimingSchema::Before => "before",
            TriggerTimingSchema::After => "after",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TriggerEventSchema {
    Insert,
    Update,
    Delete,
}

impl TriggerEventSchema {
    pub fn as_str(&self) -> &'static str {
        match self {
            TriggerEventSchema::Insert => "INSERT",
            TriggerEventSchema::Update => "UPDATE",
            TriggerEventSchema::Delete => "DELETE",
        }
    }

    pub fn lower(&self) -> &'static str {
        match self {
            TriggerEventSchema::Insert => "insert",
            TriggerEventSchema::Update => "update",
            TriggerEventSchema::Delete => "delete",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TriggerOrderKindSchema {
    Follows,
    Precedes,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TriggerOrderSchema {
    pub kind: TriggerOrderKindSchema,
    pub other_trigger: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerSchema {
    pub database: String,
    pub name: String,
    pub table_database: String,
    pub table_name: String,
    pub timing: TriggerTimingSchema,
    pub event: TriggerEventSchema,
    pub definer_user: String,
    pub definer_host: String,
    pub body_sql: String,
    #[serde(default)]
    pub create_sql: String,
    #[serde(default)]
    pub action_order: u32,
    #[serde(default)]
    pub schema_version: u64,
    #[serde(default)]
    pub created_ms: u64,
    #[serde(default)]
    pub sql_mode: String,
    #[serde(default = "default_charset_client")]
    pub character_set_client: String,
    #[serde(default = "default_collation")]
    pub collation_connection: String,
    #[serde(default = "default_collation")]
    pub database_collation: String,
}

impl TriggerSchema {
    pub fn bump_schema_version(&mut self) {
        self.schema_version = self.schema_version.saturating_add(1);
    }

    pub fn definer(&self) -> String {
        format!("{}@{}", self.definer_user, self.definer_host)
    }

    pub fn table_qualified_name(&self) -> String {
        format!("{}.{}", self.table_database, self.table_name)
    }
}

fn default_charset_client() -> String {
    crate::charset::DEFAULT_CHARSET.to_string()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventStatusSchema {
    Enabled,
    Disabled,
    DisabledOnSlave,
}

impl EventStatusSchema {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventStatusSchema::Enabled => "ENABLED",
            EventStatusSchema::Disabled => "DISABLED",
            EventStatusSchema::DisabledOnSlave => "SLAVESIDE_DISABLED",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventOnCompletionSchema {
    /// `ON COMPLETION NOT PRESERVE` (default) — drop after the last
    /// execution.
    NotPreserve,
    /// `ON COMPLETION PRESERVE` — keep the event in the catalog with
    /// status DISABLED once exhausted.
    Preserve,
}

impl EventOnCompletionSchema {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventOnCompletionSchema::NotPreserve => "NOT PRESERVE",
            EventOnCompletionSchema::Preserve => "PRESERVE",
        }
    }
}

/// `INTERVAL <expr> <unit>` field. MySQL accepts a wide range of units
/// (YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND,
/// MICROSECOND, plus compounds like YEAR_MONTH). We persist them as
/// strings and resolve at execution time so the parser does not need
/// to enumerate every variant; unsupported units fail loudly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventIntervalSchema {
    /// Stored as a number (`5`, `30`) or compound literal (`'1:30'`).
    /// We keep it as the raw text so SHOW CREATE EVENT reproduces the
    /// user's source.
    pub value: String,
    /// Upper-case unit name, e.g. `SECOND`, `MINUTE`, `HOUR`, `DAY`,
    /// `WEEK`, `MONTH`, `QUARTER`, `YEAR`.
    pub unit: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventScheduleSchema {
    /// `AT <ts>` — fires once.
    OneShot {
        /// MySQL-formatted timestamp string ("YYYY-MM-DD HH:MM:SS")
        /// in UTC.
        execute_at_ms: u64,
    },
    /// `EVERY <interval> [STARTS <ts>] [ENDS <ts>]` — recurring.
    Recurring {
        interval: EventIntervalSchema,
        starts_ms: u64,
        ends_ms: Option<u64>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventSchema {
    pub database: String,
    pub name: String,
    pub definer_user: String,
    pub definer_host: String,
    pub schedule: EventScheduleSchema,
    pub on_completion: EventOnCompletionSchema,
    pub status: EventStatusSchema,
    #[serde(default)]
    pub comment: String,
    pub body_sql: String,
    #[serde(default)]
    pub create_sql: String,
    #[serde(default)]
    pub created_ms: u64,
    #[serde(default)]
    pub last_altered_ms: u64,
    #[serde(default)]
    pub last_executed_ms: u64,
    /// Bumped on every `CREATE [OR REPLACE]` / `ALTER EVENT`.  The
    /// scheduler keys its in-memory state on `(name, schema_version)`
    /// so a redefinition under a running tick wakes up against the
    /// fresh definition instead of re-running stale state.
    #[serde(default)]
    pub schema_version: u64,
    #[serde(default)]
    pub sql_mode: String,
    /// IANA time-zone name (or `SYSTEM`) captured from the creating
    /// session. The schedule timestamps are stored as UTC ms; this name
    /// is surfaced on `information_schema.events.TIME_ZONE` / SHOW and is
    /// the calendar reference for recurring stepping.
    #[serde(default = "default_event_time_zone")]
    pub time_zone: String,
    #[serde(default = "default_charset_client")]
    pub character_set_client: String,
    #[serde(default = "default_collation")]
    pub collation_connection: String,
    #[serde(default = "default_collation")]
    pub database_collation: String,
}

fn default_event_time_zone() -> String {
    "SYSTEM".to_string()
}

impl EventSchema {
    pub fn definer(&self) -> String {
        format!("{}@{}", self.definer_user, self.definer_host)
    }

    pub fn bump_schema_version(&mut self) {
        self.schema_version = self.schema_version.saturating_add(1);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutineParamSchema {
    pub name: String,
    pub mode: RoutineParamModeSchema,
    pub data_type: DataTypeKind,
    pub unsigned: bool,
    pub data_type_sql: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RoutineSecuritySchema {
    Definer,
    Invoker,
}

impl RoutineSecuritySchema {
    pub fn as_str(&self) -> &'static str {
        match self {
            RoutineSecuritySchema::Definer => "DEFINER",
            RoutineSecuritySchema::Invoker => "INVOKER",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RoutineSqlDataAccessSchema {
    ContainsSql,
    NoSql,
    ReadsSqlData,
    ModifiesSqlData,
}

impl RoutineSqlDataAccessSchema {
    pub fn as_str(&self) -> &'static str {
        match self {
            RoutineSqlDataAccessSchema::ContainsSql => "CONTAINS SQL",
            RoutineSqlDataAccessSchema::NoSql => "NO SQL",
            RoutineSqlDataAccessSchema::ReadsSqlData => "READS SQL DATA",
            RoutineSqlDataAccessSchema::ModifiesSqlData => "MODIFIES SQL DATA",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseSchema {
    pub name: String,
    #[serde(default)]
    pub system: bool,
    #[serde(default = "default_charset")]
    pub charset: String,
    #[serde(default = "default_collation")]
    pub collation: String,
    #[serde(default)]
    pub created_ms: u64,
    #[serde(default)]
    pub updated_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub id: u64,
    #[serde(default)]
    pub object_id: u64,
    #[serde(default = "default_object_generation")]
    pub active_generation: u64,
    #[serde(default = "default_database_name")]
    pub database: String,
    pub name: String,
    #[serde(default = "default_engine")]
    pub engine: String,
    #[serde(default = "default_charset")]
    pub charset: String,
    #[serde(default = "default_collation")]
    pub collation: String,
    #[serde(default)]
    pub comment: String,
    #[serde(default)]
    pub created_ms: u64,
    #[serde(default)]
    pub updated_ms: u64,
    #[serde(default = "default_table_type")]
    pub table_type: String,
    pub columns: Vec<ColumnSchema>,
    pub primary_key: Vec<String>,
    #[serde(default)]
    pub pending_primary_key: Option<PendingPrimaryKey>,
    /// Old read views must not use a primary definition built from newer rows.
    #[serde(default)]
    pub primary_definition_scn: u64,
    #[serde(default)]
    pub next_index_id: u64,
    #[serde(default)]
    pub primary_publication_job_id: u64,
    pub indexes: Vec<IndexSchema>,
    #[serde(default)]
    pub check_constraints: Vec<CheckConstraintSchema>,
    /// Foreign keys declared *on this table* (the child / referencing
    /// side). Persisted via the data dictionary. The hot DML path gates
    /// child-side existence checks on `foreign_keys.is_empty()` — a plain
    /// `Vec` length check, as cheap as a bool, so tables without foreign
    /// keys pay nothing.
    #[serde(default)]
    pub foreign_keys: Vec<ForeignKeySchema>,
    pub stats: TableStats,
    #[serde(default)]
    pub system: bool,
    /// Monotonically increasing version bumped on any schema-changing DDL
    /// against this table (column add/drop, index add/drop, rename,
    /// truncate). Plan-cache entries record the version they were built
    /// against; on lookup, a mismatch invalidates the entry.
    ///
    /// `#[serde(default)]` keeps existing on-disk catalogs backward
    /// compatible — older entries deserialize as version 0, and the next
    /// DDL bump moves them forward.
    #[serde(default)]
    pub schema_version: u64,
    /// UTC microsecond boundary of the current structural epoch. Historical
    /// reads may not cross it because catalog history is intentionally not
    /// versioned in the first product format.
    #[serde(default)]
    pub system_time_epoch_us: u64,
    /// Transaction that published the current rewritten row generation.
    #[serde(default)]
    pub row_generation_txn_id: u64,
    /// Allocator counter for [`ColumnSchema::column_id`]. Starts at 1; 0 is
    /// reserved for "unassigned". Migrated on load for legacy catalogs that
    /// pre-date stable column identity (see `build_column_index`).
    #[serde(default)]
    pub next_column_id: u32,
    /// Active heap-row layout id for new writes. Layout id 1 is the
    /// initial layout; online rewrites allocate higher ids so rows written
    /// before and after DDL can coexist while backfill catches up.
    #[serde(default = "default_row_layout_id")]
    pub active_layout_id: u32,
    /// Persisted history of heap-row layouts that remain readable. The
    /// runtime `RowCodec` is compiled from this plus `columns`; if the
    /// vector is empty (fresh in-memory tests), `build_column_index`
    /// synthesizes the active layout from the current public schema.
    #[serde(default)]
    pub row_layouts: Vec<RowLayoutSchema>,
    /// `Some(_)` when this table is partitioned with `PARTITION BY ...`.
    /// Persisted across catalog reloads; the partition expression's
    /// AST/program are rebuilt at load time the same way generated
    /// columns / functional indexes are.
    #[serde(default)]
    pub partitioning: Option<crate::storage::partitioning::PartitionInfo>,
    /// Oracle-style `PCTFREE` per-table override: percentage of each
    /// heap page reserved for in-place UPDATE growth. `0` (the default
    /// here, not 10 — we keep the historical default opt-in via
    /// `MYDB_HEAP_UPDATE_RESERVE_BYTES`/`MYDB_PCTFREE`) means "follow the
    /// global setting". When set via `CREATE TABLE … PCTFREE=N` /
    /// `ALTER TABLE … PCTFREE=N`, it pins the per-page reserve to
    /// `N% × PAGE_SIZE`, clamped to `[0, 50]`.
    #[serde(default)]
    pub pctfree: u8,
    /// Oracle-style `PCTUSED` per-table override: the low-water mark
    /// (as a percentage of `PAGE_SIZE`) below which an underfull page
    /// is re-marked as an INSERT target. Bench-only knob today —
    /// recorded for SHOW CREATE TABLE round-trip and future heap free-list
    /// behaviour. `0` means "follow the global setting".
    #[serde(default)]
    pub pctused: u8,
    /// MySQL table options (ROW_FORMAT, KEY_BLOCK_SIZE, STATS_*, MAX_ROWS,
    /// ZONE_MAP, ZONE_SIZE, …): persisted and echoed by SHOW CREATE TABLE /
    /// information_schema. Most have no physical behaviour; zone-map options
    /// are storage-engine settings. Canonical
    /// uppercase keys, declaration order preserved.
    #[serde(default)]
    pub mysql_table_options: Vec<(String, String)>,
    #[serde(skip)]
    pub column_name_map: HashMap<String, usize, ahash::RandomState>,
    /// Reverse lookup from [`ColumnSchema::column_id`] to its index in
    /// `columns`. Built alongside `column_name_map`; not persisted.
    #[serde(skip)]
    pub column_id_map: HashMap<u32, usize>,
    /// Cached encode/decode plan for this table's heap rows, derived
    /// from the column list in `physical_pos` order. Rebuilt by
    /// `build_column_index` so every code path that consults the codec
    /// (HeapFile encoding/decoding) observes the same layout as DDL.
    /// Not persisted: it is fully derivable from `columns`.
    #[serde(skip)]
    pub row_codec: Arc<crate::storage::core::RowCodec>,
    /// Reverse foreign-key index: every foreign key *pointing at* this
    /// table (i.e. this is the parent / referenced side). Denormalized
    /// onto the parent's runtime schema so the DELETE/UPDATE hot path
    /// reads one already-held `Arc<TableSchema>` field instead of
    /// consulting a separate, separately-locked catalog map. Rebuilt
    /// from the persisted `foreign_keys` of every table by
    /// [`crate::storage::naming::build_fk_runtime`] on catalog load and
    /// after every FK-related DDL; never serialized.
    #[serde(skip)]
    pub fk_referenced_by: Vec<InboundFk>,
}

fn default_row_layout_id() -> u32 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingPrimaryKey {
    pub job_id: u64,
    pub source_generation: u64,
    pub column_ids: Vec<u32>,
    pub capture_name: String,
    pub final_index: IndexSchema,
}

fn default_object_generation() -> u64 {
    1
}

/// Per-table physical-storage knobs persisted as JSON in
/// `mysql.tables.table_options_json`. Today carries Oracle-style
/// `PCTFREE`/`PCTUSED`; future fields (e.g. ITL slot count, async-merge
/// cap) plug in without bumping `CATALOG_VERSION` again — unknown JSON
/// keys are ignored, missing keys default to zero (= "follow global").
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableOptions {
    /// `PCTFREE = N` percentage. `0` means "follow `MYDB_PCTFREE` or
    /// `MYDB_HEAP_UPDATE_RESERVE_BYTES`".
    #[serde(default)]
    pub pctfree: u8,
    /// `PCTUSED = N` percentage. `0` means "follow global".
    #[serde(default)]
    pub pctused: u8,
    /// Accepted metadata-only MySQL table options (see
    /// [`TableSchema::mysql_table_options`]). Empty = none set.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mysql_options: Vec<(String, String)>,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub system_time_epoch_us: u64,
    /// Transaction that published the current rewritten row generation.
    #[serde(default)]
    pub row_generation_txn_id: u64,
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

impl TableOptions {
    /// Project the runtime per-table options off a `TableSchema`. Used
    /// when emitting JSON for the dictionary row.
    pub fn from_schema(t: &TableSchema) -> Self {
        Self {
            pctfree: t.pctfree,
            pctused: t.pctused,
            mysql_options: t.mysql_table_options.clone(),
            system_time_epoch_us: t.system_time_epoch_us,
            row_generation_txn_id: t.row_generation_txn_id,
        }
    }

    /// True when no knob is set — the dictionary writer skips the
    /// JSON column (stores NULL) so older rows are byte-identical to new
    /// rows in the common case.
    pub fn is_default(&self) -> bool {
        self.pctfree == 0
            && self.pctused == 0
            && self.mysql_options.is_empty()
            && self.system_time_epoch_us == 0
            && self.row_generation_txn_id == 0
    }
}

/// Reject physical storage promises before any DDL side effect. Shared by SQL
/// dispatch (including temporary/IF NOT EXISTS paths) and schema preparation.
pub(crate) fn validate_supported_table_options(
    options: &[(String, String)],
) -> Result<(), QueryError> {
    if options
        .iter()
        .any(|(key, _)| key.eq_ignore_ascii_case("TABLESPACE"))
    {
        return Err(crate::storage::error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "TABLESPACE is not supported",
        ));
    }
    Ok(())
}

/// Merge accepted metadata-only table options into an existing option
/// list. Value `DEFAULT` resets the key back to "not set" (so SHOW
/// CREATE stops echoing it — MySQL behaviour); anything else replaces
/// the previous value in place, appending in declaration order.
pub fn apply_generic_table_options(
    target: &mut Vec<(String, String)>,
    updates: Vec<(String, String)>,
) {
    for (key, value) in updates {
        target.retain(|(k, _)| k != &key);
        if !value.eq_ignore_ascii_case("DEFAULT") {
            target.push((key, value));
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum RowLayoutState {
    /// Layout used for newly-written heap rows.
    #[default]
    Active,
    /// Old layout still readable while online DDL/backfill catches up.
    Readable,
    /// Backfill is complete; retained until physical cleanup/compaction.
    Retiring,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowLayoutColumnSchema {
    pub column_id: u32,
    pub physical_pos: u32,
    pub codec_kind: String,
    #[serde(default)]
    pub fixed_offset: Option<u32>,
    #[serde(default)]
    pub nullable_ord: Option<u32>,
    #[serde(default)]
    pub var_ord: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowLayoutSchema {
    pub layout_id: u32,
    #[serde(default)]
    pub state: RowLayoutState,
    pub columns: Vec<RowLayoutColumnSchema>,
}

impl TableSchema {
    /// Build a compiled-in *system* (data-dictionary) table definition.
    ///
    /// Dictionary tables (`mysql.tables`, `mysql.columns`, ...) store the
    /// catalog itself. Decoding their heap rows requires a `RowCodec`,
    /// which is derived from a `TableSchema` — so their schemas must be
    /// known without first reading any on-disk catalog. This constructor
    /// builds that schema from compiled-in column definitions and runs
    /// `build_column_index`, which assigns `column_id` / `physical_pos`
    /// and the `RowCodec`, breaking the bootstrap cycle. The columns are
    /// expected to arrive with zeroed identity fields; `build_column_index`
    /// fills them in declaration order.
    pub(crate) fn system_table(id: u64, name: &str, columns: Vec<ColumnSchema>) -> TableSchema {
        // v46 stores one current MVCC row per dictionary object.  Every
        // dictionary heap has a compiled-in primary key, so catalog DDL can
        // update just the affected object in one ordinary transaction.  The
        // old trailing generation column and full-snapshot publication model
        // are deliberately gone.
        let primary_key = match name {
            "schemata" => vec!["name"],
            "tables" => vec!["id"],
            "table_stats" => vec!["table_id"],
            "innodb_dynamic_metadata" => vec!["table_id"],
            "mydb_ddl_retirements" => vec!["table_id", "generation", "index_id"],
            "columns" => vec!["table_id", "column_id"],
            "indexes" => vec!["table_id", "name"],
            "index_column_usage" => vec!["table_id", "index_name", "ordinal_position"],
            "check_constraints" => vec!["table_id", "name"],
            "foreign_keys" => vec!["table_id", "name"],
            "foreign_key_column_usage" => {
                vec!["table_id", "fk_name", "ordinal_position"]
            }
            "views" | "triggers" | "events" => vec!["schema_name", "name"],
            "routines" => vec!["schema_name", "routine_type", "name"],
            "users" => vec!["user", "host"],
            "grants" => vec!["grant_key"],
            "role_edges" => vec!["edge_key"],
            "dd_properties" => vec!["name"],
            "sequences" => vec!["id"],
            "sequence_values" | "sequence_retirements" => vec!["counter_id"],
            "restrictions" => vec!["restriction_key"],
            _ => Vec::new(),
        }
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
        let make_index = |name: &str, columns: Vec<String>, primary: bool| IndexSchema {
            readable_from_scn: 0,
            index_id: 0,
            name: name.to_string(),
            columns,
            column_prefix_lengths: Vec::new(),
            column_directions: Vec::new(),
            key_expressions: Vec::new(),
            unique: true,
            primary,
            included_columns: Vec::new(),
            state: IndexState::Public,
            index_type: "BTREE".to_string(),
            visible: true,
            comment: String::new(),
            vector: None,
            fulltext: None,
        };
        let mut indexes = Vec::new();
        if !primary_key.is_empty() {
            indexes.push(make_index("PRIMARY", primary_key.clone(), true));
        }
        if matches!(name, "tables" | "sequences") {
            indexes.push(make_index(
                "UNIQUE_SCHEMA_NAME",
                vec!["schema_name".to_string(), "name".to_string()],
                false,
            ));
        }
        if name == "foreign_keys" {
            let mut reverse = make_index(
                "IDX_REFERENCED_TABLE",
                vec![
                    "referenced_schema".to_string(),
                    "referenced_table".to_string(),
                ],
                false,
            );
            reverse.unique = false;
            indexes.push(reverse);
        }
        let mut table = TableSchema {
            id,
            object_id: id,
            active_generation: 1,
            database: "mysql".to_string(),
            name: name.to_string(),
            engine: default_engine(),
            charset: default_charset(),
            collation: default_collation(),
            comment: String::new(),
            created_ms: 0,
            updated_ms: 0,
            table_type: default_table_type(),
            columns,
            primary_key,
            indexes,
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            fk_referenced_by: Vec::new(),
            stats: TableStats::empty(),
            system: true,
            schema_version: 0,
            system_time_epoch_us: 0,
            row_generation_txn_id: 0,
            next_column_id: 0,
            active_layout_id: default_row_layout_id(),
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
        table
    }

    /// Build a minimal in-memory `TableSchema` for a *derived* source (CTE,
    /// FROM-subquery, table function) so the join planner can resolve which
    /// side of a join a qualified column belongs to. There is no storage
    /// identity (`id = 0`, no `RowCodec`/index maps), but result types remain
    /// semantically meaningful: expression evaluation also uses this schema
    /// to distinguish BIT/binary/string and temporal operands. Never register
    /// it in a catalog or decode rows with it.
    ///
    /// Without this, `collect_physical_plan_tables` reports no tables for a
    /// `CteScan`, so an equi-join predicate referencing the CTE (e.g.
    /// `t.k = base.k`) cannot be matched to the CTE side and degrades to a
    /// cartesian product — a single such join over a non-trivial CTE explodes
    /// the intermediate and OOM-kills the server.
    pub(crate) fn synthetic_for_planning(
        name: &str,
        columns: &[crate::types::Column],
    ) -> TableSchema {
        let columns = columns
            .iter()
            .map(|c| ColumnSchema {
                not_null_from_scn: 0,
                name: c.name.clone(),
                data_type: data_type_kind_for_result_column(c),
                nullable: c.source.flags & crate::mysql::protocol::NOT_NULL_FLAG == 0,
                unsigned: c.source.flags & crate::mysql::protocol::UNSIGNED_FLAG != 0,
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
                charset: c
                    .source
                    .collation_id
                    .and_then(crate::charset::resolve_collation_id)
                    .map(|info| info.charset.to_string()),
                collation: c
                    .source
                    .collation_id
                    .and_then(crate::charset::resolve_collation_id)
                    .map(|info| info.name.to_string()),
                comment: String::new(),
                generated_expr: None,
                generated_stored: false,
                generated_column_names: Vec::new(),
                generated_expr_ast: None,
                generated_program: None,
            })
            .collect();
        TableSchema {
            id: 0,
            object_id: 0,
            active_generation: 1,
            database: String::new(),
            name: name.to_string(),
            engine: default_engine(),
            charset: default_charset(),
            collation: default_collation(),
            comment: String::new(),
            created_ms: 0,
            updated_ms: 0,
            table_type: default_table_type(),
            columns,
            primary_key: Vec::new(),
            indexes: Vec::new(),
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            fk_referenced_by: Vec::new(),
            stats: TableStats::empty(),
            system: false,
            schema_version: 0,
            system_time_epoch_us: 0,
            row_generation_txn_id: 0,
            next_column_id: 0,
            active_layout_id: default_row_layout_id(),
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
        }
    }

    /// Rebuild the per-table lookup maps and migrate legacy column metadata
    /// (`column_id` / `physical_pos` / `logical_pos` / `next_column_id`)
    /// for catalogs written before stable column identity was introduced.
    /// Idempotent: subsequent calls observe the already-populated fields
    /// and only refresh the lookup maps.
    pub fn build_column_index(&mut self) {
        if self.object_id == 0 {
            self.object_id = self.id;
        }
        if self.active_generation == 0 {
            self.active_generation = 1;
        }
        if !self.system && self.id != 0 && self.system_time_epoch_us == 0 {
            self.system_time_epoch_us = self
                .created_ms
                .max(crate::storage::naming::current_unix_ms())
                .saturating_mul(1_000);
        }
        if self.engine.is_empty() {
            self.engine = default_engine();
        }
        if self.charset.is_empty() {
            self.charset = default_charset();
        }
        if self.collation.is_empty() {
            self.collation = default_collation();
        }
        if self.table_type.is_empty() {
            self.table_type = default_table_type();
        }
        // Migrate legacy entries: any zero-id column predates stable
        // identity. Assign ids in declaration order so that `column_id`
        // happens to equal `array index + 1` for unmigrated tables — that
        // makes the migration observable but harmless.
        let needs_migration =
            self.next_column_id == 0 || self.columns.iter().any(|c| c.column_id == 0);
        if needs_migration {
            let mut next_id = self.next_column_id.max(1);
            // First pass: collect already-used ids so we never collide.
            for col in &self.columns {
                if col.column_id >= next_id {
                    next_id = col.column_id.saturating_add(1);
                }
            }
            // Second pass: assign ids/positions to columns missing them.
            // physical_pos / logical_pos default to the array index for
            // legacy entries — preserves the old "positional" layout.
            for (idx, col) in self.columns.iter_mut().enumerate() {
                if col.column_id == 0 {
                    col.column_id = next_id;
                    next_id = next_id.saturating_add(1);
                }
                if col.physical_pos == 0 && idx != 0 {
                    col.physical_pos = idx as u32;
                }
                if col.logical_pos == 0 && idx != 0 {
                    col.logical_pos = idx as u32;
                }
                if col.ordinal_position == 0 {
                    col.ordinal_position = col.logical_pos.saturating_add(1);
                }
            }
            self.next_column_id = next_id;
        }
        // Allocate monotonically increasing per-table physical index ids.
        // Format v49 rejects older directories, but zero is also convenient
        // in compiled/test constructors, so normalization remains idempotent.
        let mut next_index_id = self
            .indexes
            .iter()
            .map(|index| index.index_id)
            .max()
            .unwrap_or(0)
            .saturating_add(1)
            .max(self.next_index_id)
            .max(1);
        let mut used_index_ids = std::collections::HashSet::new();
        for index in &mut self.indexes {
            if index.index_id == 0 || !used_index_ids.insert(index.index_id) {
                while used_index_ids.contains(&next_index_id) {
                    next_index_id = next_index_id.saturating_add(1).max(1);
                }
                index.index_id = next_index_id;
                used_index_ids.insert(next_index_id);
                next_index_id = next_index_id.saturating_add(1).max(1);
            }
        }
        self.next_index_id = next_index_id;
        for fk in &mut self.foreign_keys {
            if let Some(index) = self
                .indexes
                .iter()
                .find(|index| index.name.eq_ignore_ascii_case(&fk.backing_index))
            {
                fk.backing_index_id = index.index_id;
            }
        }
        let mut visible_order = self
            .columns
            .iter()
            .enumerate()
            .filter(|(_, col)| col.state != ColumnState::Dropped)
            .map(|(idx, col)| (idx, col.logical_pos))
            .collect::<Vec<_>>();
        visible_order.sort_by_key(|(idx, logical)| (*logical, *idx));
        for (visible_ordinal, (idx, _)) in visible_order.into_iter().enumerate() {
            self.columns[idx].ordinal_position = (visible_ordinal as u32).saturating_add(1);
        }
        // Name lookup intentionally excludes tombstone (Dropped)
        // columns: a query that names a logically-removed column must
        // fail with "unknown column", same as if the DDL had physically
        // dropped the slot. column_id_map keeps tombstones so internal
        // physical-pos-keyed paths (heap row encoding, undo records)
        // continue to work.
        self.column_name_map = self
            .columns
            .iter()
            .enumerate()
            .filter(|(_, col)| col.state != ColumnState::Dropped)
            .map(|(i, col)| (col.name.to_ascii_lowercase(), i))
            .collect();
        self.column_id_map = self
            .columns
            .iter()
            .enumerate()
            .map(|(i, col)| (col.column_id, i))
            .collect();
        if self.active_layout_id == 0 {
            self.active_layout_id = default_row_layout_id();
        }
        if self.row_layouts.is_empty()
            || !self
                .row_layouts
                .iter()
                .any(|layout| layout.layout_id == self.active_layout_id)
        {
            self.row_layouts
                .retain(|layout| layout.state != RowLayoutState::Active);
            self.row_layouts.push(build_row_layout_schema(
                self.active_layout_id,
                RowLayoutState::Active,
                &self.columns,
            ));
        }
        // A logical NOT NULL transition cannot erase the NULL decoding
        // capability of rows/tombstones written by an older definition.
        for layout in &mut self.row_layouts {
            let mut next = layout
                .columns
                .iter()
                .filter_map(|c| c.nullable_ord)
                .max()
                .map_or(0, |n| n + 1);
            for column in &mut layout.columns {
                if self
                    .column_id_map
                    .get(&column.column_id)
                    .is_some_and(|&i| self.columns[i].nullable)
                    && column.nullable_ord.is_none()
                {
                    column.nullable_ord = Some(next);
                    next += 1;
                }
            }
        }
        // Rebuild the row codec so HeapFile encode/decode and any
        // schema-derived cost models see the new column layout.
        // build_column_index runs on catalog load AND after every DDL
        // bump_schema_version path, so the codec stays in lockstep with
        // the column list.
        self.row_codec = Arc::new(crate::storage::core::RowCodec::from_schema(self));
        self.rebuild_generated_column_runtime();
        self.rebuild_check_constraint_runtime();
        self.rebuild_index_expression_runtime();
        self.rebuild_partition_expression_runtime();
    }

    /// Publish a heap-row decoder for subsequent writes, reusing an identical
    /// retained format when possible. Existing formats remain readable.
    pub fn publish_new_row_layout(&mut self) {
        let mut candidate = build_row_layout_schema(0, RowLayoutState::Active, &self.columns);
        // Physical NULL capacity may outlive logical nullability. Reserving
        // its existing bits keeps the same decoder usable for old snapshots,
        // and avoids an NN/NULL cycle manufacturing a new equivalent layout.
        let nullable_ids = self
            .row_layouts
            .iter()
            .flat_map(|layout| &layout.columns)
            .filter(|column| column.nullable_ord.is_some())
            .map(|column| column.column_id)
            .collect::<std::collections::BTreeSet<_>>();
        let mut nullable_ord = 0;
        for column in &mut candidate.columns {
            if column.nullable_ord.is_some() || nullable_ids.contains(&column.column_id) {
                column.nullable_ord = Some(nullable_ord);
                nullable_ord += 1;
            }
        }
        // A layout describes stable column identities and byte decoding, not
        // DDL order. Reuse an identical retained decoder (e.g. NULL toggles)
        // without discarding any format still referenced by rows or undo.
        if let Some(position) = self.row_layouts.iter().position(|layout| {
            layout.state != RowLayoutState::Retiring && layout.columns == candidate.columns
        }) {
            self.active_layout_id = self.row_layouts[position].layout_id;
            for (index, layout) in self.row_layouts.iter_mut().enumerate() {
                if index == position {
                    layout.state = RowLayoutState::Active;
                } else if layout.state == RowLayoutState::Active {
                    layout.state = RowLayoutState::Readable;
                }
            }
            return;
        }
        // Reactivating an older ID must not allow the next allocation to
        // collide with a newer retained layout that has a different decoder.
        self.active_layout_id = self
            .row_layouts
            .iter()
            .map(|layout| layout.layout_id)
            .max()
            .unwrap_or(self.active_layout_id)
            .max(self.active_layout_id)
            .saturating_add(1)
            .max(default_row_layout_id());
        for layout in &mut self.row_layouts {
            if layout.state == RowLayoutState::Active {
                layout.state = RowLayoutState::Readable;
            }
        }
        candidate.layout_id = self.active_layout_id;
        self.row_layouts.push(candidate);
    }

    /// Collapse `row_layouts` down to just the current Active layout,
    /// discarding every prior (Readable) layout.
    ///
    /// V4 — only valid when every heap row is about to be, or has just
    /// been, physically rewritten under the current active layout (an
    /// ALTER … COPY rewrite: the heap is truncated and re-inserted, so no
    /// surviving row references any old layout). In that case the
    /// accumulated Readable layouts are provably dead, and keeping them is
    /// pure waste — each INSTANT ADD/DROP COLUMN before the rewrite left
    /// one behind, growing the persisted catalog without bound. MUST NOT
    /// be called on the INSTANT path, where pre-existing short rows still
    /// decode through their original (now-Readable) layout.
    ///
    /// Keeps `active_layout_id` unchanged and retains the layout it names
    /// (built from the post-ALTER columns by the caller's earlier
    /// `publish_new_row_layout`): the candidate's row codec and the
    /// rewrite's insert path already encode under it, so bumping the id
    /// would strand every rewritten row on a layout the schema no longer
    /// lists.
    pub fn reset_row_layouts_for_rewrite(&mut self) {
        let active = self.active_layout_id;
        self.row_layouts
            .retain(|l| l.layout_id == active && l.state == RowLayoutState::Active);
        if self.row_layouts.is_empty() {
            // Defensive: guarantee exactly one Active layout for the
            // current columns even if the caller had not published one.
            let id = active.max(default_row_layout_id());
            self.active_layout_id = id;
            self.row_layouts.push(build_row_layout_schema(
                id,
                RowLayoutState::Active,
                &self.columns,
            ));
        }
    }

    /// Re-parse and recompile the partition expression after a catalog
    /// load. Mirrors [`Self::rebuild_index_expression_runtime`]; the
    /// `expr_sql` text is the persisted form, the `expr_ast` and
    /// `expr_program` fields are skipped on disk because the
    /// [`crate::engine::expr_program::CompiledTableExpr`] tree is not
    /// stable across releases.
    pub(crate) fn rebuild_partition_expression_runtime(&mut self) {
        let expr_sql = self
            .partitioning
            .as_ref()
            .map(|p| p.expr_sql.clone())
            .filter(|s| !s.is_empty());
        let sub_expr_sql = self
            .partitioning
            .as_ref()
            .and_then(|p| p.subpartitioning.as_ref())
            .map(|sub| sub.expr_sql.clone())
            .filter(|s| !s.is_empty());
        let ast = expr_sql
            .as_deref()
            .and_then(|sql| crate::parser::parse_expr_fragment(sql).ok());
        let program = ast.as_ref().map(|ast| {
            Arc::new(crate::engine::expr_program::CompiledTableExpr::compile_once(ast, self))
        });
        let sub_ast = sub_expr_sql
            .as_deref()
            .and_then(|sql| crate::parser::parse_expr_fragment(sql).ok());
        let sub_program = sub_ast.as_ref().map(|ast| {
            Arc::new(crate::engine::expr_program::CompiledTableExpr::compile_once(ast, self))
        });
        if let Some(part) = self.partitioning.as_mut() {
            part.expr_ast = ast;
            part.expr_program = program;
            part.sub_expr_ast = sub_ast;
            part.sub_expr_program = sub_program;
            // Any schema/partition DDL may have cloned a populated runtime
            // from the previous schema snapshot. Never publish that stale
            // LIST lookup under the new schema generation.
            part.runtime = std::sync::OnceLock::new();
            part.metadata_runtime = std::sync::OnceLock::new();
        }
    }

    fn rebuild_index_expression_runtime(&mut self) {
        if !self
            .indexes
            .iter()
            .any(|index| index.has_expression_parts())
        {
            return;
        }
        // Best-effort rebuild. Persisted catalogs have already passed
        // through `compile_pending_index_expressions` at DDL time, so a
        // failure here just leaves `program = None`; the DML path
        // surfaces that as a clear error. CREATE TABLE / ADD INDEX
        // therefore explicitly call `compile_pending_index_expressions`
        // *after* `build_column_index` to get a fail-fast error path.
        let runtime: Vec<
            Vec<(
                Option<Expr>,
                Option<crate::engine::expr_program::CompiledTableExpr>,
                Vec<String>,
                Option<u16>,
            )>,
        > = self
            .indexes
            .iter()
            .map(|index| {
                index
                    .key_expressions
                    .iter()
                    .map(|slot| match slot {
                        Some(expr_part) => {
                            let ast = crate::parser::parse_catalog_expr(&expr_part.expr_sql).ok();
                            let program = ast.as_ref().map(|ast| {
                                crate::engine::expr_program::CompiledTableExpr::compile_once(
                                    ast, self,
                                )
                            });
                            let refs = if expr_part.referenced_columns.is_empty() {
                                ast.as_ref()
                                    .and_then(|ast| {
                                        collect_index_expression_columns(ast, self).ok()
                                    })
                                    .unwrap_or_default()
                            } else {
                                expr_part.referenced_columns.clone()
                            };
                            let collation = expr_part.collation_id.or_else(|| {
                                ast.as_ref()
                                    .and_then(|ast| crate::engine::expr::expr_collation(ast, self))
                                    .map(|c| c.collation_id)
                            });
                            (ast, program, refs, collation)
                        }
                        None => (None, None, Vec::new(), None),
                    })
                    .collect()
            })
            .collect();
        for (index, slots) in self.indexes.iter_mut().zip(runtime) {
            for (slot, (ast, program, refs, collation_id)) in
                index.key_expressions.iter_mut().zip(slots)
            {
                if let Some(expr_part) = slot.as_mut() {
                    expr_part.ast = ast;
                    expr_part.collation_id = collation_id;
                    expr_part.program = program;
                    if expr_part.referenced_columns.is_empty() {
                        expr_part.referenced_columns = refs;
                    }
                }
            }
        }
    }

    /// Failing variant of [`Self::rebuild_index_expression_runtime`].
    /// Used by CREATE TABLE / ADD INDEX to surface invalid expressions
    /// (parse error, nondeterministic function, missing column
    /// reference) to the caller. Idempotent: re-running it after a
    /// successful compilation is a no-op besides the recompile cost.
    pub fn compile_pending_index_expressions(&mut self) -> Result<(), QueryError> {
        if !self
            .indexes
            .iter()
            .any(|index| index.has_expression_parts())
        {
            return Ok(());
        }
        let snapshots: Vec<(Vec<String>, Vec<(Option<String>, bool)>)> = self
            .indexes
            .iter()
            .map(|index| {
                let exprs = index
                    .key_expressions
                    .iter()
                    .map(|slot| {
                        slot.as_ref()
                            .map(|e| (Some(e.expr_sql.clone()), e.multi_valued))
                            .unwrap_or((None, false))
                    })
                    .collect();
                (index.columns.clone(), exprs)
            })
            .collect();
        let compiled: Vec<Vec<Option<IndexExpression>>> = snapshots
            .iter()
            .map(|(cols, slots)| compile_index_key_expressions(self, cols, slots))
            .collect::<Result<Vec<_>, _>>()?;
        for (index, mut new_slots) in self.indexes.iter_mut().zip(compiled) {
            if !new_slots.is_empty() {
                for (old, new) in index.key_expressions.iter().zip(&mut new_slots) {
                    if let (Some(old), Some(new)) = (old, new) {
                        new.collation_id = old.collation_id.or(new.collation_id);
                    }
                }
                index.key_expressions = new_slots;
            }
            index.validate_multi_valued_definition()?;
        }
        Ok(())
    }

    fn rebuild_generated_column_runtime(&mut self) {
        if !self
            .columns
            .iter()
            .any(|column| column.generated_expr.is_some())
        {
            return;
        }
        let input_schema = crate::engine::expr_program::InputSchema::from_table(self);
        let runtime = self
            .columns
            .iter()
            .map(|column| {
                let expr = column
                    .generated_expr
                    .as_deref()
                    .and_then(|sql| crate::parser::parse_catalog_expr(sql).ok());
                let program = expr.as_ref().map(|expr| {
                    if let Some(idx) = crate::engine::expr::expr_column_index(expr, self) {
                        crate::engine::expr_program::CompiledTableExpr::SimpleColumn(idx)
                    } else {
                        match crate::engine::expr_program::ExprCompiler::compile(
                            expr,
                            &input_schema,
                        ) {
                            Ok(program) => crate::engine::expr_program::CompiledTableExpr::Program(
                                std::sync::Arc::new(program),
                            ),
                            Err(_) => {
                                crate::engine::expr_program::CompiledTableExpr::Legacy(expr.clone())
                            }
                        }
                    }
                });
                (expr, program)
            })
            .collect::<Vec<_>>();
        for (column, (expr, program)) in self.columns.iter_mut().zip(runtime) {
            column.generated_expr_ast = expr;
            column.generated_program = program;
        }
    }

    fn rebuild_check_constraint_runtime(&mut self) {
        if self.check_constraints.is_empty() {
            return;
        }
        let input_schema = crate::engine::expr_program::InputSchema::from_table(self);
        let runtime = self
            .check_constraints
            .iter()
            .map(|check| {
                let expr = crate::parser::parse_catalog_expr(&check.expr).ok();
                let program = expr.as_ref().map(|expr| {
                    if let Some(idx) = crate::engine::expr::expr_column_index(expr, self) {
                        crate::engine::expr_program::CompiledTableExpr::SimpleColumn(idx)
                    } else {
                        match crate::engine::expr_program::ExprCompiler::compile(
                            expr,
                            &input_schema,
                        ) {
                            Ok(program) => crate::engine::expr_program::CompiledTableExpr::Program(
                                std::sync::Arc::new(program),
                            ),
                            Err(_) => {
                                crate::engine::expr_program::CompiledTableExpr::Legacy(expr.clone())
                            }
                        }
                    }
                });
                (expr, program)
            })
            .collect::<Vec<_>>();
        for (check, (expr, program)) in self.check_constraints.iter_mut().zip(runtime) {
            check.expr_ast = expr;
            check.program = program;
        }
    }

    /// Allocate a fresh [`ColumnSchema::column_id`]. Caller is responsible
    /// for ensuring `build_column_index` has been run at least once so
    /// `next_column_id` is non-zero.
    pub fn allocate_column_id(&mut self) -> u32 {
        if self.next_column_id == 0 {
            self.next_column_id = 1;
        }
        let id = self.next_column_id;
        self.next_column_id = self.next_column_id.saturating_add(1);
        id
    }

    /// Next free physical slot for a newly added column. Equals
    /// `max(physical_pos) + 1` over all existing columns, or 0 when the
    /// table is empty. New columns are appended at the tail of the row
    /// payload — never inserted in the middle — so existing rows keep
    /// their layout and `ADD COLUMN` stays INSTANT.
    pub fn next_physical_pos(&self) -> u32 {
        self.columns
            .iter()
            .map(|c| c.physical_pos)
            .max()
            .map_or(0, |m| m.saturating_add(1))
    }

    /// Bump `schema_version` by one. DDL that mutates this table's structure
    /// (column add/drop, index add/drop, rename, modify, truncate) must call
    /// this so plan-cache entries dependent on the prior version can be
    /// invalidated on next lookup. Statistics refreshes also invalidate plan
    /// dependencies, but use `bump_schema_version_without_system_time_epoch`
    /// to preserve the structural boundary for historical reads.
    pub fn bump_schema_version(&mut self) {
        self.schema_version = self.schema_version.saturating_add(1);
        self.system_time_epoch_us = crate::storage::naming::current_unix_ms().saturating_mul(1_000);
        // Structural DDL may replace a column while retaining its name.
        // Never publish a histogram sampled from the previous definition.
        self.stats.explicit_histograms.clear();
    }

    pub fn bump_schema_version_without_system_time_epoch(&mut self) {
        self.schema_version = self.schema_version.saturating_add(1);
    }

    /// `true` when this table has a `PARTITION BY` clause.
    pub fn is_partitioned(&self) -> bool {
        self.partitioning.is_some()
    }

    /// Every physical storage id this table's data lives in. For an
    /// unpartitioned table that's just `[self.id]`; for a partitioned
    /// table the storage id of each physical leaf in declaration order.
    /// With subpartitioning, logical parents contribute no id and their
    /// subpartitions are flattened parent-by-parent. The order is exactly
    /// [`crate::storage::partitioning::PartitionInfo::storage_ids`].
    pub fn storage_ids(&self) -> Vec<u64> {
        match &self.partitioning {
            Some(partitioning) => partitioning.storage_ids(),
            None => vec![self.id],
        }
    }

    /// Compute the partition this row should land in, evaluating the
    /// partition expression (RANGE/LIST/HASH) or extracting the column
    /// tuple (RANGE COLUMNS / LIST COLUMNS / KEY).
    ///
    /// Returns `Ok(RoutedPartition)` on success; `Err` for:
    /// - RANGE: no `VALUES LESS THAN` bucket large enough (no MAXVALUE)
    /// - LIST: value isn't a member of any partition's list
    /// - Expression evaluation failure (cancelled query, NULL where the
    ///   partition function rejects NULL, etc.)
    pub fn route_row_to_partition(
        &self,
        row: &[Value],
    ) -> Result<crate::storage::partitioning::RoutedPartition, QueryError> {
        let part = self
            .partitioning
            .as_ref()
            .expect("route_row_to_partition called on unpartitioned table");
        crate::engine::executor::partition_router::route_row(self, part, row)
    }
}

fn build_row_layout_schema(
    layout_id: u32,
    state: RowLayoutState,
    columns: &[ColumnSchema],
) -> RowLayoutSchema {
    let mut ordered = columns
        .iter()
        .filter(|column| column.state != ColumnState::Dropped)
        .collect::<Vec<_>>();
    ordered.sort_by_key(|column| column.physical_pos);

    let mut fixed_offset = 0u32;
    let mut nullable_ord = 0u32;
    let mut var_ord = 0u32;
    let mut layout_columns = Vec::with_capacity(ordered.len());
    for column in ordered {
        let fixed_len = row_layout_fixed_len(&column.data_type);
        let fixed = fixed_len.map(|len| {
            let off = fixed_offset;
            fixed_offset = fixed_offset.saturating_add(len as u32);
            off
        });
        let var = if fixed_len.is_none() {
            let ord = var_ord;
            var_ord = var_ord.saturating_add(1);
            Some(ord)
        } else {
            None
        };
        let nullable = if column.nullable {
            let ord = nullable_ord;
            nullable_ord = nullable_ord.saturating_add(1);
            Some(ord)
        } else {
            None
        };
        layout_columns.push(RowLayoutColumnSchema {
            column_id: column.column_id,
            physical_pos: column.physical_pos,
            codec_kind: row_layout_codec_kind(&column.data_type, column.unsigned),
            fixed_offset: fixed,
            nullable_ord: nullable,
            var_ord: var,
        });
    }
    RowLayoutSchema {
        layout_id,
        state,
        columns: layout_columns,
    }
}

fn row_layout_codec_kind(data_type: &DataTypeKind, unsigned: bool) -> String {
    format!("{:?}:{}", data_type, if unsigned { "u" } else { "s" })
}

fn row_layout_fixed_len(data_type: &DataTypeKind) -> Option<usize> {
    Some(match data_type {
        DataTypeKind::TinyInt | DataTypeKind::Bool | DataTypeKind::Year => 1,
        DataTypeKind::SmallInt => 2,
        DataTypeKind::MediumInt | DataTypeKind::Date => 3,
        DataTypeKind::Int | DataTypeKind::Float { .. } => 4,
        DataTypeKind::BigInt | DataTypeKind::Double { .. } => 8,
        DataTypeKind::Decimal { precision, .. } => match precision {
            Some(p) if *p <= 9 => 4,
            Some(p) if *p <= 18 => 8,
            Some(p) if *p <= 38 => 16,
            _ => return None,
        },
        DataTypeKind::Time { fsp } => temporal_fsp_width(*fsp, 3),
        DataTypeKind::DateTime { fsp } | DataTypeKind::Timestamp { fsp } => {
            temporal_fsp_width(*fsp, 5)
        }
        DataTypeKind::Binary { len } => len.unwrap_or(1),
        DataTypeKind::Bit { len } => len.unwrap_or(1).div_ceil(8),
        DataTypeKind::Vector {
            dim: Some(dim),
            datatype,
        } if *datatype != crate::vector::VectorDataType::Float32
            && datatype.payload_len(*dim) > 4096 =>
        {
            return None
        }
        DataTypeKind::Vector {
            dim: Some(dim),
            datatype,
        } => datatype.payload_len(*dim),
        DataTypeKind::Enum { values } => crate::storage::enum_storage_width(values.len()) as usize,
        DataTypeKind::Set { values } => crate::storage::set_storage_width(values.len()) as usize,
        DataTypeKind::Char { .. }
        | DataTypeKind::VarChar { .. }
        | DataTypeKind::VarBinary { .. }
        | DataTypeKind::Text { .. }
        | DataTypeKind::Blob { .. }
        | DataTypeKind::Json
        | DataTypeKind::Geometry { .. }
        | DataTypeKind::Vector { dim: None, .. } => return None,
    })
}

#[inline]
pub(crate) fn temporal_fsp_width(fsp: u8, base: usize) -> usize {
    match fsp {
        0 => base,
        1 | 2 => base + 1,
        3 | 4 => base + 2,
        _ => base + 3,
    }
}

/// Lifecycle state of a column under online DDL. Only `Public` is
/// queryable; other states gate read/write visibility during a
/// multi-phase ALTER. New columns added via INSTANT ADD COLUMN go
/// directly to `Public` since the existing variable-length row encoding
/// already tolerates "row was written before this column existed".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ColumnState {
    /// Visible to readers; writers must produce a value.
    #[default]
    Public,
    /// Writers must produce a value (so the column is consistent across
    /// the heap and any concurrent index build), but readers ignore it.
    /// Used as the intermediate state before an ONLINE column rewrite
    /// promotes the shadow column to `Public`.
    WriteOnly,
    /// Writers may DELETE rows that reference this column but neither
    /// INSERT nor UPDATE produces values for it. Used as the first step
    /// of column removal so any in-flight transaction sees a coherent
    /// "column is going away" view before WriteOnly inserts cease.
    DeleteOnly,
    /// Logically dropped tombstone retained until VACUUM physically
    /// reclaims the slot. Readers and writers both skip the column;
    /// existing rows keep their stale value in the slot until naturally
    /// rewritten by a future UPDATE.
    Dropped,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchema {
    /// NOT NULL is an optimizer fact only for snapshots at/after validation.
    /// Older snapshots can still see NULL before-images in undo.
    #[serde(default)]
    pub not_null_from_scn: u64,
    pub name: String,
    pub data_type: DataTypeKind,
    #[serde(default = "default_nullable")]
    pub nullable: bool,
    #[serde(default)]
    pub unsigned: bool,
    /// MySQL display-only `ZEROFILL` attribute (implies `unsigned`).
    /// Echoed by SHOW CREATE / SHOW COLUMNS and used by the text
    /// protocol to zero-pad numeric output to the type's default
    /// display width. No storage-format impact.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub zerofill: bool,
    /// MySQL 8.0.23+ `INVISIBLE` column attribute: excluded from
    /// `SELECT *` and from INSERT column-list-less arity, still fully
    /// addressable by name. SHOW COLUMNS reports `INVISIBLE` in Extra.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub invisible: bool,
    #[serde(default)]
    pub auto_increment: bool,
    #[serde(default)]
    pub default: Option<Value>,
    #[serde(default)]
    pub default_expr: Option<String>,
    /// Immutable value for physical slots missing from pre-INSTANT rows.
    /// SQL DEFAULT continues to govern new inserts independently.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_backfill_default: Option<Value>,
    #[serde(default)]
    pub on_update_expr: Option<String>,
    /// Stable identity for this column, assigned at creation and never
    /// reused even after DROP. 0 indicates "not yet assigned" — only
    /// observed transiently on legacy catalogs read from disk;
    /// [`TableSchema::build_column_index`] migrates them on load.
    #[serde(default)]
    pub column_id: u32,
    /// Online-DDL lifecycle state. See [`ColumnState`].
    #[serde(default)]
    pub state: ColumnState,
    /// Slot index in the heap row's value array. Pinned at column
    /// creation time and never mutated thereafter; ALTER ADD COLUMN
    /// always allocates a new slot at `TableSchema::next_physical_pos()`
    /// so existing rows do not need to be rewritten. Reading a column
    /// at `physical_pos >= row.values.len()` yields the column's
    /// DEFAULT (the row predates the ADD COLUMN — INSTANT semantics).
    #[serde(default)]
    pub physical_pos: u32,
    /// Display position for SHOW COLUMNS / DESCRIBE / SELECT *. Decoupled
    /// from `physical_pos` so `ALTER ... ADD COLUMN AFTER x` and
    /// `ALTER ... CHANGE` can renumber presentation without touching
    /// rows.
    #[serde(default)]
    pub logical_pos: u32,
    #[serde(default)]
    pub ordinal_position: u32,
    #[serde(default)]
    pub charset: Option<String>,
    #[serde(default)]
    pub collation: Option<String>,
    #[serde(default)]
    pub comment: String,
    #[serde(default)]
    pub generated_expr: Option<String>,
    #[serde(default)]
    pub generated_stored: bool,
    #[serde(default)]
    pub generated_column_names: Vec<String>,
    #[serde(skip)]
    pub(crate) generated_expr_ast: Option<Expr>,
    #[serde(skip)]
    pub(crate) generated_program: Option<crate::engine::expr_program::CompiledTableExpr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexSchema {
    /// Earliest snapshot whose complete key set is represented by this index.
    /// Online builds do not backfill keys deleted before their build began.
    #[serde(default)]
    pub readable_from_scn: u64,
    /// Stable physical identity within the owning table. Names are mutable
    /// display metadata; physical paths use this id so RENAME INDEX is a
    /// catalog-only operation.
    #[serde(default)]
    pub index_id: u64,
    pub name: String,
    /// For pure column key parts, the column name. For expression key
    /// parts (MySQL 8.0 functional indexes — `INDEX i ((LOWER(c)))`), the
    /// canonical SQL text of the expression; the real key shape is
    /// described by `key_expressions[pos]`. This dual usage keeps every
    /// `(prefix_lengths / directions / cardinality)` parallel array
    /// indexable by the same `pos`, so the existing code paths that
    /// iterate over `columns` keep working for non-expression indexes.
    pub columns: Vec<String>,
    /// Optional MySQL-style prefix length per declared key column, as in
    /// `CREATE INDEX idx ON t (name(10))`. Length is interpreted as
    /// characters for string key parts and bytes for binary key parts.
    #[serde(default)]
    pub column_prefix_lengths: Vec<Option<usize>>,
    /// Sort direction per declared key column. Missing entries default to
    /// ascending for catalogs written before direction metadata existed.
    #[serde(default)]
    pub column_directions: Vec<IndexColumnDirection>,
    /// Parallel to `columns`. `Some(expr)` marks a functional key part:
    /// the index value at that position is computed from the row by
    /// evaluating the stored expression rather than reading a column
    /// directly. Empty Vec (or all `None`) means a plain column index;
    /// `#[serde(default)]` lets pre-v20 catalogs deserialize cleanly.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub key_expressions: Vec<Option<IndexExpression>>,
    pub unique: bool,
    pub primary: bool,
    /// Columns appended to the key purely for covering. Standard SQL
    /// spelling: `CREATE INDEX ... INCLUDE (col1, col2)`. The execution
    /// path treats them as additional key parts at the end of the key,
    /// so the binary layout stays a single sorted run; the only
    /// observable difference from a composite index is that we forbid
    /// `INCLUDE` on `UNIQUE` indexes (the unique constraint must apply
    /// to the user-declared key columns alone, not to the included
    /// suffix). Default empty for old catalogs without this field.
    #[serde(default)]
    pub included_columns: Vec<String>,
    /// Lifecycle state. `Public` is the steady state (and the default
    /// for catalogs written before online DDL — see `#[serde(default)]`).
    /// `Building` marks an index whose ADD INDEX is still in flight:
    /// concurrent DML maintains it, but the planner does NOT consider
    /// it for read paths because the heap scan that populates pre-DDL
    /// rows has not yet completed. A catalog containing a `Building`
    /// entry on startup means the previous process crashed mid-build;
    /// recovery removes such entries so the user can re-issue ADD INDEX.
    #[serde(default)]
    pub state: IndexState,
    #[serde(default = "default_index_type")]
    pub index_type: String,
    #[serde(default = "default_index_visible")]
    pub visible: bool,
    #[serde(default)]
    pub comment: String,
    /// Complete vector index configuration, independent from the user comment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector: Option<crate::storage::vector_config::VectorIndexConfig>,
    /// Persisted FULLTEXT tokenizer settings, independent from vector options.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fulltext: Option<FulltextMeta>,
}

/// Persisted metadata for a FULLTEXT index.
///
/// **Tokenizer-rule consistency guard**: the tokenizer config (ngram_size / min/max token size /
/// stopword toggle) is snapshotted at index build time from the global `MYDB_FT_*` env vars into
/// this on-disk structure; queries read the index's own config first — otherwise env changes
/// across a restart fork the build/query rules into **silent false negatives** (e.g. ngram_size
/// 2->3: historical two-character postings cannot match a three-character single-token
/// query). All fields are `Option` with `#[serde(default)]` — old indexes deserialize to `None` and use global defaults (old behavior preserved); new indexes always record their rules.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct FulltextMeta {
    /// MySQL `WITH PARSER` name: empty = default whitespace/punctuation tokenizer, `"ngram"` = the ngram tokenizer.
    #[serde(default)]
    pub parser: String,
    /// ngram window size (only meaningful when `parser == "ngram"`). `None` uses the global default (legacy-index compat).
    #[serde(default)]
    pub ngram_size: Option<u32>,
    /// Shortest token length (in characters) kept by the default tokenizer. `None` uses the global default (legacy-index compat).
    #[serde(default)]
    pub min_token_size: Option<u32>,
    /// Longest token length kept by the default tokenizer. `None` uses the global default (legacy-index compat).
    #[serde(default)]
    pub max_token_size: Option<u32>,
    /// Whether default stopword filtering is enabled. `None` uses the global default (legacy-index compat).
    #[serde(default)]
    pub use_stopwords: Option<bool>,
    /// Whether posting keys contain positions. Persisted so key reconstruction
    /// remains byte-identical after process defaults change.
    #[serde(default)]
    pub positions: Option<bool>,
    /// Nonzero position gap between indexed columns. Part of the key format.
    #[serde(default)]
    pub position_gap: Option<u32>,
}

/// The concrete nearest-neighbor algorithm of a vector index. A type-safe view over the string `index_type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorAlgo {
    /// HNSW graph (in-memory `HnswIndex` / paged `PagedHnsw`).
    Hnsw,
    /// Native inverted-file index.
    Ivf,
}

/// Resolved build/planner view of the persisted typed VectorIndexConfig.
/// SQL comments are independent descriptive text. Algorithm-specific options
/// are validated before this common read-only view is constructed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorIndexMeta {
    /// Distance metric name: "L2" | "COSINE" | "DOT".
    pub metric: String,
    /// Maximum neighbors per node (above level 0).
    pub m: u32,
    /// Dynamic candidate-list size during construction.
    pub ef_construction: u32,
    /// Per-index default query-time `ef_search` (pgvector-style). `None` means
    /// "use the session default". Priority is hint > explicit session > index > default.
    pub default_ef_search: Option<u32>,
    /// IVF: number of inverted-list cells (k-means centroids). `None`/`0` = auto √N at build time.
    /// Resolved from WITH nlist at index creation.
    pub nlist: Option<u32>,
    /// IVF: per-index default number of probe cells per query. `None` = session / GUC default.
    /// Priority is hint > explicit session > index default > system default.
    pub default_nprobe: Option<u32>,
    /// IVF: k-means iteration rounds (`None` = GUC default). Captured at index creation.
    pub kmeans_iters: Option<u32>,
    /// IVF: training sample multiplier — samples `min(N, mult·nlist)` rows for training (`None` = GUC default).
    /// Captured at index creation.
    pub train_sample_mult: Option<u32>,
}

impl Default for VectorIndexMeta {
    fn default() -> Self {
        VectorIndexMeta {
            metric: "L2".to_string(),
            m: crate::engine::vector::DEFAULT_M as u32,
            ef_construction: crate::engine::vector::DEFAULT_EF_CONSTRUCTION as u32,
            default_ef_search: None,
            nlist: None,
            default_nprobe: None,
            kmeans_iters: None,
            train_sample_mult: None,
        }
    }
}

impl VectorIndexMeta {
    /// Resolve the metric name to the [`crate::engine::vector::Metric`] enum.
    /// `pub(crate)` (not `pub`) so this public struct doesn't leak the
    /// crate-internal `Metric` type through its signature.
    pub(crate) fn metric_enum(&self) -> crate::engine::vector::Metric {
        crate::engine::vector::Metric::parse(&self.metric).unwrap_or_default()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum IndexColumnDirection {
    #[default]
    Asc,
    Desc,
}

/// Persisted descriptor for a single MySQL 8.0 functional index key part
/// — `INDEX i ((LOWER(c)))`. Captures only the persistent state on disk:
/// the canonical SQL text and the declared result type that drives key
/// encoding. The `ast` and `program` fields are rebuilt on catalog load
/// (the same way `ColumnSchema::generated_program` is) so we never have
/// to serialize an `Expr` value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexExpression {
    /// Canonical SQL text of the expression, used both for persistence
    /// and for matching equivalent expressions in WHERE/ORDER BY.
    pub expr_sql: String,
    /// Result type used to encode this key part. Fixed at CREATE time so
    /// the on-disk index file's layout is stable even if functions later
    /// gain wider/narrower return types.
    pub data_type: DataTypeKind,
    /// Whether the expression result may be NULL. Most functions return
    /// NULL on any NULL input, so this is true by default unless the
    /// caller can prove otherwise.
    #[serde(default = "default_nullable")]
    pub nullable: bool,
    /// Lowercased names of base columns the expression references. DML
    /// uses this set to decide whether a row update touched the
    /// functional index (analogous to `index_may_be_affected_by_columns`
    /// for plain column indexes).
    #[serde(default)]
    pub referenced_columns: Vec<String>,
    /// MySQL multi-valued index part: `CAST(json_path AS <scalar> ARRAY)`.
    /// The expression evaluates to a JSON array and each element becomes
    /// one key for the row.
    #[serde(default)]
    pub multi_valued: bool,
    /// Parsed AST, rebuilt from `expr_sql` on catalog load. Skipped on
    /// disk because Expr is not stable across releases.
    #[serde(skip)]
    pub(crate) ast: Option<Expr>,
    /// Result collation fixed at DDL time, shared by writes and probes.
    #[serde(default)]
    pub(crate) collation_id: Option<u16>,
    /// Compiled program over the table row, rebuilt alongside `ast`.
    #[serde(skip)]
    pub(crate) program: Option<crate::engine::expr_program::CompiledTableExpr>,
}

/// View of an index key part exposing whether it refers to a base column
/// or a functional expression. Returned by [`IndexSchema::key_part`].
#[derive(Debug, Clone, Copy)]
pub enum IndexKeyPart<'a> {
    Column(&'a str),
    Expression(&'a IndexExpression),
}

impl<'a> IndexKeyPart<'a> {
    pub fn is_expression(&self) -> bool {
        matches!(self, IndexKeyPart::Expression(_))
    }
}

/// Canonical persisted/probe key shape for a functional expression.
/// CAST signedness is semantic but is not represented by `DataTypeKind`, so
/// recover it from the rebuilt AST exactly as the planner does for bounds.
pub(crate) fn index_expression_param_target(expr: &IndexExpression) -> ParamTarget {
    if let Some(Expr::Function { name, args, .. }) = expr.ast.as_ref() {
        if (name.eq_ignore_ascii_case("CAST") || name.eq_ignore_ascii_case("CONVERT"))
            && args.len() >= 2
        {
            if let Some(Expr::Literal(crate::parser::Literal::String(target))) = args.last() {
                let target = target.trim().to_ascii_uppercase();
                if target.starts_with("UNSIGNED") {
                    return ParamTarget::UInt;
                }
                if target.starts_with("SIGNED") {
                    return ParamTarget::Int;
                }
                if target.starts_with("JSON") {
                    return ParamTarget::JsonScalar;
                }
            }
        }
    }
    match (
        ParamTarget::for_column(&expr.data_type, false),
        expr.collation_id,
    ) {
        (ParamTarget::String, Some(collation_id)) => ParamTarget::CollatedString { collation_id },
        (ParamTarget::CharString, Some(collation_id)) => {
            ParamTarget::CollatedCharString { collation_id }
        }
        (target, _) => target,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckConstraintSchema {
    pub name: String,
    /// Canonical SQL text for the expression inside `CHECK (...)`.
    pub expr: String,
    #[serde(default = "default_check_enforced")]
    pub enforced: bool,
    #[serde(default)]
    pub column_names: Vec<String>,
    #[serde(skip)]
    pub(crate) expr_ast: Option<Expr>,
    #[serde(skip)]
    pub(crate) program: Option<crate::engine::expr_program::CompiledTableExpr>,
}

fn default_check_enforced() -> bool {
    true
}

impl CheckConstraintSchema {
    pub fn references_column(&self, column: &str) -> bool {
        self.column_names
            .iter()
            .any(|name| name.eq_ignore_ascii_case(column))
    }

    pub fn references_any_column(&self, columns: &[usize], table: &TableSchema) -> bool {
        columns.iter().any(|idx| {
            table
                .columns
                .get(*idx)
                .is_some_and(|column| self.references_column(&column.name))
        })
    }
}

/// Referential action for a foreign key's `ON DELETE` / `ON UPDATE`
/// clause. `SET DEFAULT` is intentionally absent: like InnoDB, mydb
/// parses it but rejects it at DDL time (error 1215), so it is never
/// persisted. `NO ACTION` is stored distinctly from `RESTRICT` so
/// `SHOW CREATE TABLE` / `information_schema` round-trip the user's
/// spelling, but the two are enforced identically (immediate, per-row).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ReferentialAction {
    #[default]
    NoAction,
    Restrict,
    Cascade,
    SetNull,
}

impl ReferentialAction {
    /// Canonical MySQL spelling for `SHOW CREATE TABLE` /
    /// `information_schema.referential_constraints`.
    pub fn as_sql(self) -> &'static str {
        match self {
            ReferentialAction::NoAction => "NO ACTION",
            ReferentialAction::Restrict => "RESTRICT",
            ReferentialAction::Cascade => "CASCADE",
            ReferentialAction::SetNull => "SET NULL",
        }
    }

    /// `NO ACTION` and `RESTRICT` both forbid the parent change when a
    /// child row references it (mydb enforces them immediately, never
    /// deferred — matching InnoDB).
    pub fn is_blocking(self) -> bool {
        matches!(
            self,
            ReferentialAction::NoAction | ReferentialAction::Restrict
        )
    }
}

/// A foreign key declared on the child (referencing) table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeySchema {
    /// Constraint symbol. Generated as `<table>_ibfk_<n>` when the user
    /// omits it, matching InnoDB's naming.
    pub name: String,
    /// Child-side columns participating in the key, in declared order.
    pub columns: Vec<String>,
    #[serde(default = "default_database_name")]
    pub referenced_database: String,
    pub referenced_table: String,
    /// Parent-side referenced columns, parallel to `columns`.
    pub referenced_columns: Vec<String>,
    pub on_delete: ReferentialAction,
    pub on_update: ReferentialAction,
    /// Child-side index used to enforce the key (looked up on parent
    /// DELETE/UPDATE to find referencing rows, and to satisfy MySQL's
    /// rule that every FK has a backing index). May have been created
    /// automatically when no suitable index already existed.
    pub backing_index: String,
    #[serde(default)]
    pub backing_index_id: u64,
    /// Runtime-resolved child column indices (parallel to `columns`).
    /// Rebuilt by [`crate::storage::naming::build_fk_runtime`].
    #[serde(skip)]
    pub child_col_idx: Vec<usize>,
}

impl ForeignKeySchema {
    pub fn references_column(&self, column: &str) -> bool {
        self.columns
            .iter()
            .any(|name| name.eq_ignore_ascii_case(column))
    }

    /// True if any of `changed` (column indices into the child table)
    /// is one of this FK's child columns — used to gate UPDATE-side
    /// existence re-checks to the columns that actually matter.
    pub fn affected_by_changed(&self, changed: &[usize]) -> bool {
        changed.iter().any(|c| self.child_col_idx.contains(c))
    }
}

/// One inbound foreign key, denormalized onto the parent (referenced)
/// table's runtime [`TableSchema::fk_referenced_by`]. Carries everything
/// the parent-side DELETE/UPDATE enforcement needs without re-reading the
/// child's schema: the child table to act on, the action, the parent
/// columns whose values form the lookup key, and the child index to
/// probe for referencing rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundFk {
    pub child_table_id: u64,
    pub child_table_key: String,
    pub fk_name: String,
    pub on_delete: ReferentialAction,
    pub on_update: ReferentialAction,
    /// Parent-side referenced column indices (into the parent table's
    /// `columns`), parallel to `child_col_idx`.
    pub referenced_col_idx: Vec<usize>,
    /// Child-side FK column indices (into the child table's `columns`),
    /// parallel to `referenced_col_idx`.
    pub child_col_idx: Vec<usize>,
    pub child_backing_index: String,
    pub child_backing_index_id: u64,
}

/// Lifecycle state for an [`IndexSchema`] under online DDL. See the
/// field docs on `IndexSchema::state` for the full state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum IndexState {
    /// Steady-state — readable and writable. All catalogs written
    /// before online ADD INDEX deserialize to this variant via
    /// `#[serde(default)]`.
    #[default]
    Public,
    /// ADD INDEX is in progress. DML maintains entries, but the
    /// planner must not pick this index for read paths because the
    /// backfill of pre-DDL rows is incomplete.
    Building,
}

impl IndexSchema {
    /// Candidate unique indexes are validated by DDL before publication;
    /// they must not impose a new constraint on concurrent business DML.
    pub fn enforces_uniqueness(&self) -> bool {
        self.unique && self.state == IndexState::Public
    }

    /// Whether this is a MySQL FULLTEXT index. A fulltext index reuses the B-tree underneath,
    /// but its keys come from text tokenization (multiple tokens per row); DML / backfill / queries take dedicated branches.
    pub fn is_fulltext(&self) -> bool {
        self.index_type.eq_ignore_ascii_case("FULLTEXT")
    }

    /// Whether this index uses the persistent page-backed spatial R-tree.
    pub fn is_spatial(&self) -> bool {
        self.index_type.eq_ignore_ascii_case("SPATIAL")
    }

    /// Whether this is a vector index (HNSW graph or IVF inverted table). A vector index keeps a
    /// degenerate B-tree (constant key → row_id) as a row_id registry to reuse the DML/recovery
    /// infrastructure; the real ANN structure is a derived candidate generator (`engine::vector`:
    /// lazily built from heap scans, incrementally maintained, and authoritative for queries).
    /// The ~30 DML/catalog/recovery call sites dispatch uniformly on this predicate; the few sites that must branch by algorithm use [`Self::vector_algo`].
    pub fn is_vector(&self) -> bool {
        self.index_type.eq_ignore_ascii_case("HNSW")
            || self.index_type.eq_ignore_ascii_case("VECTOR")
            || self.index_type.eq_ignore_ascii_case("IVF")
    }

    /// The vector index's concrete algorithm (HNSW graph vs IVF inverted table). Called only
    /// where truly branching by algorithm (build/query routing, parameter parsing); elsewhere share [`Self::is_vector`].
    /// Returns `Hnsw` for non-vector indexes (conventional default; callers should filter via `is_vector()` first).
    pub fn vector_algo(&self) -> VectorAlgo {
        if self.index_type.eq_ignore_ascii_case("IVF") {
            VectorAlgo::Ivf
        } else {
            VectorAlgo::Hnsw
        }
    }

    /// Resolve the persisted configuration against a column's native datatype.
    pub(crate) fn vector_meta_for_table(&self, table: &TableSchema) -> VectorIndexMeta {
        let mut meta = self.vector_meta();
        let explicit = self.vector.as_ref().is_some_and(|v| v.metric.is_some());
        let binary = self
            .columns
            .first()
            .and_then(|name| {
                table
                    .columns
                    .iter()
                    .find(|c| c.name.eq_ignore_ascii_case(name))
            })
            .is_some_and(|c| {
                matches!(
                    c.data_type,
                    DataTypeKind::Vector {
                        datatype: crate::vector::VectorDataType::Binary,
                        ..
                    }
                )
            });
        if binary && !explicit {
            meta.metric = "HAMMING".into();
        }
        meta
    }

    pub fn vector_meta(&self) -> VectorIndexMeta {
        self.vector.as_ref().map(|v| v.meta()).unwrap_or_default()
    }

    /// All columns physically encoded into each index entry's key, in
    /// order: declared key columns followed by `INCLUDE` columns. Used
    /// by `TableSchema::index_key` for writes and by covering checks
    /// for reads.
    pub fn effective_columns(&self) -> Vec<&str> {
        self.columns
            .iter()
            .chain(self.included_columns.iter())
            .map(String::as_str)
            .collect()
    }

    /// View of declared key part at position `pos`. Expression parts
    /// (`((LOWER(c)))`) live in `key_expressions`; everything else is a
    /// plain column reference.
    #[inline]
    pub fn key_part(&self, pos: usize) -> Option<IndexKeyPart<'_>> {
        let column = self.columns.get(pos)?;
        if let Some(Some(expr)) = self.key_expressions.get(pos) {
            return Some(IndexKeyPart::Expression(expr));
        }
        Some(IndexKeyPart::Column(column.as_str()))
    }

    /// Borrow the persisted expression for key part `pos`, if any.
    #[inline]
    pub fn key_expression(&self, pos: usize) -> Option<&IndexExpression> {
        self.key_expressions.get(pos).and_then(Option::as_ref)
    }

    /// True iff at least one declared key part is a functional
    /// expression. INCLUDE columns are never expressions.
    #[inline]
    pub fn has_expression_parts(&self) -> bool {
        self.key_expressions.iter().any(Option::is_some)
    }

    #[inline]
    pub fn has_multi_valued_parts(&self) -> bool {
        self.key_expressions
            .iter()
            .flatten()
            .any(|expr| expr.multi_valued)
    }

    pub fn validate_multi_valued_definition(&self) -> Result<(), QueryError> {
        if !self.has_multi_valued_parts() {
            return Ok(());
        }
        if self.primary || self.unique {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "UNIQUE or PRIMARY multi-valued indexes are not supported",
            ));
        }
        if !self.index_type.eq_ignore_ascii_case("BTREE") {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "multi-valued indexes require BTREE index type",
            ));
        }
        if !self.included_columns.is_empty() {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "multi-valued indexes with INCLUDE columns are not supported",
            ));
        }
        let mut multi_valued_parts =
            self.key_expressions
                .iter()
                .enumerate()
                .filter_map(|(pos, expr)| {
                    expr.as_ref()
                        .filter(|expr| expr.multi_valued)
                        .map(|e| (pos, e))
                });
        let Some((pos, _)) = multi_valued_parts.next() else {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "multi-valued index requires an ARRAY expression key part",
            ));
        };
        if multi_valued_parts.next().is_some() {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "only one multi-valued index key part is supported",
            ));
        }
        if self.column_prefix_len(pos).is_some() {
            return Err(error(
                crate::error::catalog::ER_WRONG_SUB_KEY,
                "prefix length is not allowed for multi-valued index key part",
            ));
        }
        if self
            .column_directions
            .get(pos)
            .is_some_and(|direction| !matches!(direction, IndexColumnDirection::Asc))
        {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "ASC or DESC is not allowed for a multi-valued index key part",
            ));
        }
        Ok(())
    }

    #[inline]
    pub fn column_prefix_len(&self, pos: usize) -> Option<usize> {
        // Hot path: typical OLTP catalogs have no prefix lengths declared,
        // so skip every per-call `Vec::get` chain.
        if self.column_prefix_lengths.is_empty() {
            return None;
        }
        self.column_prefix_lengths
            .get(pos)
            .copied()
            .flatten()
            .filter(|len| *len > 0)
    }

    #[inline]
    pub fn column_direction(&self, pos: usize) -> IndexColumnDirection {
        if self.column_directions.is_empty() {
            return IndexColumnDirection::Asc;
        }
        self.column_directions.get(pos).copied().unwrap_or_default()
    }

    #[inline]
    pub fn has_prefix_columns(&self) -> bool {
        if self.column_prefix_lengths.is_empty() {
            return false;
        }
        self.column_prefix_lengths
            .iter()
            .any(|len| len.is_some_and(|len| len > 0))
    }

    #[inline]
    pub fn key_parts_are_exact(&self, len: usize) -> bool {
        if !self.included_columns.is_empty() {
            return false;
        }
        if self.column_prefix_lengths.is_empty() {
            return true;
        }
        (0..len.min(self.columns.len())).all(|pos| self.column_prefix_len(pos).is_none())
    }

    #[inline]
    pub fn apply_prefix_to_key_part(&self, pos: usize, part: KeyPart) -> KeyPart {
        // Hot path: no prefix metadata, so return the part unchanged
        // without touching `column_prefix_lengths` at all.
        if self.column_prefix_lengths.is_empty() {
            return part;
        }
        apply_key_part_prefix(part, self.column_prefix_len(pos))
    }
}

/// MySQL per-type maximum byte lengths for the TEXT / BLOB families.
/// A value longer than the column's cap is rejected with error 1406.
pub const TINY_LOB_MAX: u32 = 255;
pub const DEFAULT_LOB_MAX: u32 = 65_535; // plain TEXT / BLOB
pub const MEDIUM_LOB_MAX: u32 = 16_777_215; // 2^24 - 1
pub const LONG_LOB_MAX: u32 = 4_294_967_295; // 2^32 - 1

/// The MySQL geometry subtype a `GEOMETRY` column is declared as. `Geometry`
/// is the permissive base (accepts any geometry); the others constrain stored
/// values to a single WKB type. Carried in the catalog so `DESCRIBE` /
/// `SHOW COLUMNS` echo the declared name and writes can be type-checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GeometrySubtype {
    Geometry,
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
    GeometryCollection,
}

impl GeometrySubtype {
    /// Lower-case MySQL type name shown by `DESCRIBE` / `information_schema`.
    pub fn type_name(self) -> &'static str {
        match self {
            GeometrySubtype::Geometry => "geometry",
            GeometrySubtype::Point => "point",
            GeometrySubtype::LineString => "linestring",
            GeometrySubtype::Polygon => "polygon",
            GeometrySubtype::MultiPoint => "multipoint",
            GeometrySubtype::MultiLineString => "multilinestring",
            GeometrySubtype::MultiPolygon => "multipolygon",
            GeometrySubtype::GeometryCollection => "geometrycollection",
        }
    }

    /// The WKB type code (1..=7) this subtype constrains to, or `None` for the
    /// permissive base `GEOMETRY`.
    pub fn wkb_type_code(self) -> Option<u32> {
        Some(match self {
            GeometrySubtype::Geometry => return None,
            GeometrySubtype::Point => 1,
            GeometrySubtype::LineString => 2,
            GeometrySubtype::Polygon => 3,
            GeometrySubtype::MultiPoint => 4,
            GeometrySubtype::MultiLineString => 5,
            GeometrySubtype::MultiPolygon => 6,
            GeometrySubtype::GeometryCollection => 7,
        })
    }

    /// True when a geometry with the given WKB type code may be stored in a
    /// column of this subtype.
    pub fn accepts(self, wkb_type_code: u32) -> bool {
        match self.wkb_type_code() {
            None => true,
            Some(code) => code == wkb_type_code,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataTypeKind {
    TinyInt,
    SmallInt,
    MediumInt,
    Int,
    BigInt,
    Bool,
    /// `FLOAT[(M[,D])]` — the optional `(precision, scale)` is the
    /// declared display width / decimals carried for SHOW COLUMNS and
    /// information_schema. Storage is still 4-byte IEEE-754 regardless.
    Float {
        #[serde(default)]
        precision: Option<u8>,
        #[serde(default)]
        scale: Option<u8>,
    },
    /// `DOUBLE[(M[,D])]` — see [`DataTypeKind::Float`]. 8-byte storage.
    Double {
        #[serde(default)]
        precision: Option<u8>,
        #[serde(default)]
        scale: Option<u8>,
    },
    Decimal {
        precision: Option<u8>,
        scale: Option<u8>,
    },
    Char {
        len: Option<usize>,
    },
    VarChar {
        len: Option<usize>,
    },
    Binary {
        len: Option<usize>,
    },
    VarBinary {
        len: Option<usize>,
    },
    /// TEXT family (character). `max_len` is the byte cap per the
    /// declared size (TINYTEXT=255, TEXT=65535, MEDIUMTEXT=16M-1,
    /// LONGTEXT=4G-1).
    Text {
        max_len: u32,
    },
    /// BLOB family (binary). `max_len` is the byte cap per the declared
    /// size (TINYBLOB=255 … LONGBLOB=4G-1).
    Blob {
        max_len: u32,
    },
    Date,
    /// `TIME[(fsp)]` — `fsp` is the declared fractional-seconds precision
    /// (0..=6). Writers round the i64-microsecond payload half-away-from-zero
    /// to the corresponding multiple of `10^(6-fsp)` before storing (MySQL
    /// 5.6.4+ default; `TIME_TRUNCATE_FRACTIONAL` switches to truncation), so a
    /// `TIME(0)` column never round-trips a non-zero microsecond fragment.
    Time {
        fsp: u8,
    },
    /// `DATETIME[(fsp)]` — see [`DataTypeKind::Time { .. }`].
    DateTime {
        fsp: u8,
    },
    /// `TIMESTAMP[(fsp)]` — see [`DataTypeKind::Time { .. }`].
    Timestamp {
        fsp: u8,
    },
    Year,
    Json,
    /// `GEOMETRY` / `POINT` / `POLYGON` / … — stored as internal `SRID + WKB`
    /// bytes (`ColumnKind::Geometry`). `srid` is the optional column SRID
    /// constraint (MySQL 8.0 `... SRID n`); `None` means unconstrained.
    Geometry {
        subtype: GeometrySubtype,
        srid: Option<u32>,
    },
    /// `VECTOR(n)` — a fixed-width `f32` array for similarity search, stored as
    /// internal little-endian bytes (`ColumnKind::Vector`). `dim` is the
    /// declared dimension constraint; `None` means unconstrained (any
    /// dimension accepted).
    Vector {
        dim: Option<u32>,
        datatype: crate::vector::VectorDataType,
    },
    Enum {
        values: Vec<String>,
    },
    Set {
        values: Vec<String>,
    },
    Bit {
        len: Option<usize>,
    },
}

/// Convert result-set metadata back into the in-memory type descriptor used by
/// expression evaluation over derived/CTE/projected rows. Unlike a storage
/// column, a result column has no catalog `DataType`, so declared wire
/// metadata supplies widths and temporal fractional precision.
pub(crate) fn data_type_kind_for_result_column(column: &crate::types::Column) -> DataTypeKind {
    data_type_kind_for_column_type(column.column_type, &column.source)
}

fn data_type_kind_for_column_type(
    column_type: ColumnType,
    source: &crate::types::ColumnSource,
) -> DataTypeKind {
    let length = source.length;
    let decimals = source.decimals.unwrap_or(0).min(6);
    match column_type {
        ColumnType::Tiny => DataTypeKind::TinyInt,
        ColumnType::Short => DataTypeKind::SmallInt,
        ColumnType::MediumInt => DataTypeKind::MediumInt,
        ColumnType::Long => DataTypeKind::Int,
        ColumnType::LongLong => DataTypeKind::BigInt,
        ColumnType::Year => DataTypeKind::Year,
        ColumnType::Float => DataTypeKind::Float {
            precision: source.numeric_precision,
            scale: source.decimals,
        },
        ColumnType::Double => DataTypeKind::Double {
            precision: source.numeric_precision,
            scale: source.decimals,
        },
        ColumnType::Decimal { precision, scale } => DataTypeKind::Decimal {
            precision: Some(precision),
            scale: Some(scale),
        },
        ColumnType::Date => DataTypeKind::Date,
        ColumnType::Time => DataTypeKind::Time { fsp: decimals },
        ColumnType::DateTime => DataTypeKind::DateTime { fsp: decimals },
        ColumnType::Timestamp => DataTypeKind::Timestamp { fsp: decimals },
        ColumnType::Blob => DataTypeKind::Blob {
            max_len: length.unwrap_or(DEFAULT_LOB_MAX),
        },
        ColumnType::Json => DataTypeKind::Json,
        ColumnType::Geometry => DataTypeKind::Geometry {
            subtype: GeometrySubtype::Geometry,
            srid: None,
        },
        ColumnType::Vector => {
            let (datatype, dim) = source.vector_type.unwrap_or_default();
            DataTypeKind::Vector { dim, datatype }
        }
        ColumnType::Bit { bits } => DataTypeKind::Bit {
            len: Some(bits as usize),
        },
        ColumnType::VarString => DataTypeKind::Text {
            max_len: length.unwrap_or(DEFAULT_LOB_MAX),
        },
    }
}

/// Target storage shape for a deferred (placeholder) `KeyPart`. The
/// planner picks this once based on the matching index column's column
/// type so resolution at execute time can produce a `KeyPart` that
/// compares against persisted index entries with matching enum variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ParamTarget {
    Int,
    UInt,
    Float32,
    Float64,
    Decimal {
        precision: u8,
        scale: u8,
    },
    Bytes,
    /// SQL scalar coerced into the canonical binary-JSON representation used
    /// by functional indexes such as `JSON_EXTRACT(doc, '$.x')`.
    JsonScalar,
    CharString,
    CollatedCharString {
        collation_id: u16,
    },
    String,
    CollatedString {
        collation_id: u16,
    },
    Date,
    Time,
    DateTime,
    Timestamp,
    Year,
    /// Search-only deferred LIKE bound. It is resolved by the storage range
    /// boundary (which may remove the bound entirely for an unanchored
    /// pattern), never persisted as an index key part.
    LikePattern {
        collation_id: Option<u16>,
        binary: bool,
        char_semantics: bool,
        upper: bool,
        escape: Option<char>,
        prefix_len: Option<u32>,
    },
}

/// Logical key part used for index lookups and persisted index entries.
///
/// Variants are kept in one-to-one correspondence with `ColumnKind` so a
/// `KeyPart` round-trips between catalog-declared column types and
/// stored index entries without ASCII detours. The derived `Ord` ranks
/// variants by declaration order, which is fine because an index column
/// has a fixed type and only ever compares parts of the same variant.
///
/// Float values are stored in order-preserving unsigned bit form
/// (`f32_to_order_u32` / `f64_to_order_u64`) so the derived comparison
/// reflects real numeric order, including for negative values — the
/// pre-v6 index encoded floats as `format!("{:020.8}")` ASCII strings,
/// which mis-ordered any value with a `-` prefix.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KeyPart {
    Null,
    Int(i64),
    UInt(u64),
    /// f32 stored in order-preserving u32 form. Use
    /// `row_codec::f32_from_order_u32` to recover the original f32.
    Float32(u32),
    /// f64 stored in order-preserving u64 form.
    Float64(u64),
    /// Signed scaled magnitude (i.e. `value × 10^scale`). i128 covers
    /// every legal MySQL DECIMAL(38,*) magnitude. Same-scale comparison
    /// is what matters in practice because a single index column has a
    /// fixed scale; cross-scale comparison falls back to the derived
    /// field-by-field ordering on `(value, scale)`.
    Decimal {
        value: i128,
        scale: u8,
    },
    /// Full-width DECIMAL coefficient for declared precision 39..65. A
    /// column uses either this lane or `Decimal` consistently according to
    /// its schema, so B-tree comparisons never depend on a row's value size.
    DecimalWide {
        value: num_bigint::BigInt,
        scale: u8,
    },
    Bytes(Vec<u8>),
    String(String),
    CollatedString {
        collation_id: u16,
        value: String,
    },
    /// Days since 1970-01-01; matches `Value::Date`.
    Date(i32),
    /// Microseconds; matches `Value::Time`.
    Time(i64),
    /// Microseconds since 1970-01-01 UTC; matches `Value::DateTime`.
    DateTime(i64),
    /// `year - 1900`; matches `Value::Year`.
    Year(u8),
    /// Phase 2.5 deferred index-key part. Carries a parameter index and
    /// the target storage shape determined at planning time; resolved
    /// against the thread-local parameter context at the storage
    /// boundary (see `IndexKey::resolve_params`). Lives only in plan
    /// trees that reference a placeholder; serializing one would
    /// indicate a leak from the planner into persisted index pages, so
    /// `#[serde(skip)]` rejects the write at the serialization layer.
    #[serde(skip)]
    Param {
        idx: usize,
        target: ParamTarget,
    },
}

impl PartialEq for KeyPart {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for KeyPart {}

impl PartialOrd for KeyPart {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KeyPart {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        key_part_cmp_fast(self, other)
    }
}

#[inline]
fn key_part_cmp_fast(left: &KeyPart, right: &KeyPart) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (left, right) {
        (KeyPart::Null, KeyPart::Null) => Ordering::Equal,
        (KeyPart::Int(a), KeyPart::Int(b)) => a.cmp(b),
        (KeyPart::UInt(a), KeyPart::UInt(b)) => a.cmp(b),
        (KeyPart::Float32(a), KeyPart::Float32(b)) => a.cmp(b),
        (KeyPart::Float64(a), KeyPart::Float64(b)) => a.cmp(b),
        (
            KeyPart::Decimal {
                value: av,
                scale: as_,
            },
            KeyPart::Decimal {
                value: bv,
                scale: bs,
            },
        ) => (av, as_).cmp(&(bv, bs)),
        (
            KeyPart::DecimalWide {
                value: av,
                scale: as_,
            },
            KeyPart::DecimalWide {
                value: bv,
                scale: bs,
            },
        ) => (av, as_).cmp(&(bv, bs)),
        (
            KeyPart::Decimal { value, scale },
            KeyPart::DecimalWide {
                value: wide,
                scale: wide_scale,
            },
        ) => decimal_key_cmp_big(&num_bigint::BigInt::from(*value), *scale, wide, *wide_scale),
        (
            KeyPart::DecimalWide {
                value: wide,
                scale: wide_scale,
            },
            KeyPart::Decimal { value, scale },
        ) => decimal_key_cmp_big(wide, *wide_scale, &num_bigint::BigInt::from(*value), *scale),
        (KeyPart::Bytes(a), KeyPart::Bytes(b)) => a.cmp(b),
        (KeyPart::String(a), KeyPart::String(b)) => a.cmp(b),
        (
            KeyPart::CollatedString {
                collation_id: aid,
                value: av,
            },
            KeyPart::CollatedString {
                collation_id: bid,
                value: bv,
            },
        ) => match aid.cmp(bid) {
            Ordering::Equal => crate::charset::compare_strings_by_id(*aid, av, bv),
            ord => ord,
        },
        (KeyPart::Date(a), KeyPart::Date(b)) => a.cmp(b),
        (KeyPart::Time(a), KeyPart::Time(b)) => a.cmp(b),
        (KeyPart::DateTime(a), KeyPart::DateTime(b)) => a.cmp(b),
        (KeyPart::Year(a), KeyPart::Year(b)) => a.cmp(b),
        (
            KeyPart::Param {
                idx: ai,
                target: at,
            },
            KeyPart::Param {
                idx: bi,
                target: bt,
            },
        ) => (ai, at).cmp(&(bi, bt)),
        _ => key_part_rank(left).cmp(&key_part_rank(right)),
    }
}

fn decimal_key_cmp_big(
    left: &num_bigint::BigInt,
    left_scale: u8,
    right: &num_bigint::BigInt,
    right_scale: u8,
) -> std::cmp::Ordering {
    match left_scale.cmp(&right_scale) {
        std::cmp::Ordering::Equal => left.cmp(right),
        std::cmp::Ordering::Less => (left
            * num_bigint::BigInt::from(10u8).pow(u32::from(right_scale - left_scale)))
        .cmp(right),
        std::cmp::Ordering::Greater => left.cmp(
            &(right * num_bigint::BigInt::from(10u8).pow(u32::from(left_scale - right_scale))),
        ),
    }
}

impl Hash for KeyPart {
    fn hash<H: Hasher>(&self, state: &mut H) {
        key_part_rank(self).hash(state);
        match self {
            KeyPart::Null => {}
            KeyPart::Int(v) => v.hash(state),
            KeyPart::UInt(v) => v.hash(state),
            KeyPart::Float32(v) => v.hash(state),
            KeyPart::Float64(v) => v.hash(state),
            KeyPart::Decimal { value, scale } => {
                num_bigint::BigInt::from(*value).hash(state);
                scale.hash(state);
            }
            KeyPart::DecimalWide { value, scale } => {
                value.hash(state);
                scale.hash(state);
            }
            KeyPart::Bytes(v) => v.hash(state),
            KeyPart::String(v) => v.hash(state),
            KeyPart::CollatedString {
                collation_id,
                value,
            } => {
                collation_id.hash(state);
                // Zero-allocation collation key digest: the previous
                // form allocated one throwaway `Vec<u8>` per `KeyPart`
                // hashed (B-tree dedup, hashable IndexKey shape, etc).
                crate::charset::with_collation_key_by_id(*collation_id, value, |bytes| {
                    bytes.hash(state)
                });
            }
            KeyPart::Date(v) => v.hash(state),
            KeyPart::Time(v) => v.hash(state),
            KeyPart::DateTime(v) => v.hash(state),
            KeyPart::Year(v) => v.hash(state),
            KeyPart::Param { idx, target } => {
                idx.hash(state);
                target.hash(state);
            }
        }
    }
}

#[inline]
pub(crate) fn key_part_rank(part: &KeyPart) -> u8 {
    match part {
        KeyPart::Null => 0,
        KeyPart::Int(_) => 1,
        KeyPart::UInt(_) => 2,
        KeyPart::Float32(_) => 3,
        KeyPart::Float64(_) => 4,
        KeyPart::Decimal { .. } | KeyPart::DecimalWide { .. } => 5,
        KeyPart::Bytes(_) => 6,
        KeyPart::String(_) => 7,
        KeyPart::CollatedString { .. } => 8,
        KeyPart::Date(_) => 9,
        KeyPart::Time(_) => 10,
        KeyPart::DateTime(_) => 11,
        KeyPart::Year(_) => 12,
        KeyPart::Param { .. } => 13,
    }
}

impl ParamTarget {
    /// Pick the storage shape a deferred index key part should resolve
    /// to, based on the column the placeholder is being compared
    /// against. Mirrors the variant selection that `coerce_value_*`
    /// would produce when an INSERT writes the row that lands on the
    /// matching index entry — so a resolved Param compares equal to
    /// the stored KeyPart by enum variant.
    pub fn for_column(data_type: &DataTypeKind, unsigned: bool) -> Self {
        Self::for_column_with_collation(data_type, unsigned, None)
    }

    pub fn for_column_with_collation(
        data_type: &DataTypeKind,
        unsigned: bool,
        collation: Option<&str>,
    ) -> Self {
        match data_type {
            DataTypeKind::TinyInt
            | DataTypeKind::SmallInt
            | DataTypeKind::MediumInt
            | DataTypeKind::Int
            | DataTypeKind::BigInt
            | DataTypeKind::Bool => {
                if unsigned {
                    ParamTarget::UInt
                } else {
                    ParamTarget::Int
                }
            }
            DataTypeKind::Year => ParamTarget::Year,
            DataTypeKind::Float { .. } => ParamTarget::Float32,
            DataTypeKind::Double { .. } => ParamTarget::Float64,
            DataTypeKind::Decimal { precision, scale } => ParamTarget::Decimal {
                precision: precision.unwrap_or(10),
                scale: scale.unwrap_or(0),
            },
            DataTypeKind::Date => ParamTarget::Date,
            DataTypeKind::Time { .. } => ParamTarget::Time,
            DataTypeKind::DateTime { .. } => ParamTarget::DateTime,
            DataTypeKind::Timestamp { .. } => ParamTarget::Timestamp,
            DataTypeKind::Char { .. } => collation
                .map(crate::charset::collation_id)
                .map(|collation_id| ParamTarget::CollatedCharString { collation_id })
                .unwrap_or(ParamTarget::CharString),
            DataTypeKind::VarChar { .. }
            | DataTypeKind::Text { .. }
            | DataTypeKind::Enum { .. }
            | DataTypeKind::Set { .. } => collation
                .map(crate::charset::collation_id)
                .map(|collation_id| ParamTarget::CollatedString { collation_id })
                .unwrap_or(ParamTarget::String),
            DataTypeKind::Json => ParamTarget::JsonScalar,
            DataTypeKind::Binary { .. }
            | DataTypeKind::VarBinary { .. }
            | DataTypeKind::Blob { .. }
            | DataTypeKind::Geometry { .. }
            | DataTypeKind::Vector { .. } => ParamTarget::Bytes,
            // BIT is decoded off the heap as `Value::UInt` and its stored
            // index key is `KeyPart::UInt` (via `KeyPart::from_value`), so a
            // prepared-statement probe must build the same variant.
            // `ParamTarget::Bytes` would have produced a `KeyPart::Bytes`
            // (the ASCII text of the number), never matching the stored key.
            DataTypeKind::Bit { .. } => ParamTarget::UInt,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IndexKey(pub SmallVec<[KeyPart; 4]>);

impl From<Vec<KeyPart>> for IndexKey {
    #[inline]
    fn from(parts: Vec<KeyPart>) -> Self {
        Self(SmallVec::from_vec(parts))
    }
}

impl From<SmallVec<[KeyPart; 4]>> for IndexKey {
    #[inline]
    fn from(parts: SmallVec<[KeyPart; 4]>) -> Self {
        Self(parts)
    }
}

impl PartialOrd for IndexKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IndexKey {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        if let Some(ord) = cmp_uniform_numeric_key_parts(&self.0, &other.0) {
            return ord;
        }
        let len = self.0.len().min(other.0.len());
        for idx in 0..len {
            match key_part_cmp_fast(&self.0[idx], &other.0[idx]) {
                Ordering::Equal => {}
                ord => return ord,
            }
        }
        self.0.len().cmp(&other.0.len())
    }
}

#[inline]
fn cmp_uniform_numeric_key_parts(
    left: &[KeyPart],
    right: &[KeyPart],
) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;
    if left.is_empty() || right.is_empty() {
        return None;
    }
    match (&left[0], &right[0]) {
        (KeyPart::Int(_), KeyPart::Int(_)) => {
            for (a, b) in left.iter().zip(right.iter()) {
                let (KeyPart::Int(a), KeyPart::Int(b)) = (a, b) else {
                    return None;
                };
                match a.cmp(b) {
                    Ordering::Equal => {}
                    ord => return Some(ord),
                }
            }
            Some(left.len().cmp(&right.len()))
        }
        (KeyPart::UInt(_), KeyPart::UInt(_)) => {
            for (a, b) in left.iter().zip(right.iter()) {
                let (KeyPart::UInt(a), KeyPart::UInt(b)) = (a, b) else {
                    return None;
                };
                match a.cmp(b) {
                    Ordering::Equal => {}
                    ord => return Some(ord),
                }
            }
            Some(left.len().cmp(&right.len()))
        }
        _ => None,
    }
}

impl IndexKey {
    /// Ordering equality can hide a changed covering-index payload (case,
    /// accents or PAD SPACE). Maintenance must preserve the original text.
    #[inline]
    pub(crate) fn same_stored_value(&self, other: &Self) -> bool {
        self == other
            && self.0.iter().zip(&other.0).all(|(a, b)| match (a, b) {
                (KeyPart::String(a), KeyPart::String(b)) => a == b,
                (
                    KeyPart::CollatedString { value: a, .. },
                    KeyPart::CollatedString { value: b, .. },
                ) => a == b,
                _ => true,
            })
    }

    /// Whether this key carries a concrete NULL part (`col IS NULL`). UNIQUE
    /// permits any number of NULL keys, so such a key is never a point
    /// lookup that resolves to at most one row.
    pub fn contains_null(&self) -> bool {
        self.0.iter().any(|p| matches!(p, KeyPart::Null))
    }

    /// Whether this key carries any deferred `KeyPart::Param` part. Used
    /// as a fast-path check to avoid cloning when no resolution is
    /// needed.
    pub fn contains_param(&self) -> bool {
        self.0.iter().any(|p| matches!(p, KeyPart::Param { .. }))
    }

    /// Resolve every `KeyPart::Param` against the thread-local parameter
    /// context. Concrete parts are passed through unchanged. Called at
    /// storage boundaries (`lookup_index_visible` / `prefix_index_visible`
    /// / `range_index_bounds_visible` etc.) right before the actual
    /// index access, so persisted code never sees a Param.
    pub fn resolve_params(&self) -> Result<IndexKey, QueryError> {
        if !self.contains_param() {
            return Ok(self.clone());
        }
        let mut parts = SmallVec::<[KeyPart; 4]>::with_capacity(self.0.len());
        for part in &self.0 {
            parts.push(part.resolve_param()?);
        }
        Ok(IndexKey::from(parts))
    }
}

impl KeyPart {
    /// Resolve a single `KeyPart::Param` against the thread-local
    /// parameter context, coercing the bound value into the storage
    /// shape recorded at planning time. Concrete variants are returned
    /// as-is.
    pub fn resolve_param(&self) -> Result<KeyPart, QueryError> {
        let KeyPart::Param { idx, target } = self else {
            return Ok(self.clone());
        };
        let value = crate::engine::params::current_param(*idx).ok_or_else(|| {
            error(
                crate::error::catalog::ER_WRONG_ARGUMENTS,
                "parameter is not bound",
            )
        })?;
        // Out-of-domain lookup values are valid predicates, not assignments.
        // Signed negatives sort below every unsigned key and cannot match one.
        if matches!(target, ParamTarget::UInt) {
            if let Value::Int(value) = &value {
                if *value < 0 {
                    return Ok(KeyPart::Int(*value));
                }
            }
        }
        let result = KeyPart::for_target(value, *target);
        if result.is_err() {
            let kind = match target {
                ParamTarget::Date => Some("DATE"),
                ParamTarget::DateTime | ParamTarget::Timestamp => Some("DATETIME"),
                _ => None,
            };
            if let Some(kind) = kind {
                // A failed prepared temporal conversion is SQL UNKNOWN, not
                // a probe for the zero date. Storage treats a deferred NULL
                // key as unsatisfiable for these non-null-safe predicates.
                let value = crate::engine::params::current_param(*idx)
                    .expect("parameter was resolved above");
                crate::engine::expr::report_invalid_date_comparison(kind, &value)?;
                return Ok(KeyPart::Null);
            }
        }
        result
    }

    /// Convert a runtime `Value` into the `KeyPart` shape an index entry
    /// at this column type was stored with. Both the planner (when
    /// building literal bounds for a WHERE clause) and `resolve_param`
    /// (when materialising a prepared-statement parameter at execute
    /// time) go through this helper so the resulting `KeyPart` lands in
    /// the same enum variant as the stored entries — otherwise the
    /// derived `Ord` would silently miscompare across variants.
    pub fn for_target(value: Value, target: ParamTarget) -> Result<KeyPart, QueryError> {
        if matches!(value, Value::Null) {
            return Ok(KeyPart::Null);
        }
        let temporal_kind = match target {
            ParamTarget::Date => Some(DataTypeKind::Date),
            ParamTarget::Time => Some(DataTypeKind::Time { fsp: 6 }),
            ParamTarget::DateTime => Some(DataTypeKind::DateTime { fsp: 6 }),
            ParamTarget::Timestamp => Some(DataTypeKind::Timestamp { fsp: 6 }),
            _ => None,
        };
        let value = if let Some(kind) = temporal_kind.as_ref().filter(|_| {
            matches!(
                value,
                Value::Int(_)
                    | Value::UInt(_)
                    | Value::Float(_)
                    | Value::Double(_)
                    | Value::Decimal(_)
                    | Value::DecimalCompact(..)
                    | Value::Decimal128(..)
                    | Value::DecimalWide(_)
            )
        }) {
            let text = Value::Decimal(value.to_display_string());
            crate::engine::expr::coerce_comparison_constant_for_type(&text, &kind)
                .filter(|converted| {
                    crate::engine::expr::compare_values(&value, converted).ok()
                        == Some(std::cmp::Ordering::Equal)
                })
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_WRONG_VALUE,
                        "temporal index bound cannot preserve numeric comparison",
                    )
                })?
        } else if let Some(kind) = temporal_kind.as_ref().filter(|_| {
            matches!(
                value,
                Value::String(_) | Value::SmallString(_) | Value::Bytes(_)
            )
        }) {
            if let Some(converted) =
                crate::engine::expr::coerce_comparison_constant_for_type(&value, kind)
            {
                converted
            } else if matches!(kind, DataTypeKind::Date)
                && crate::engine::expr::coerce_comparison_constant_for_type(
                    &value,
                    &DataTypeKind::DateTime { fsp: 6 },
                )
                .is_some()
            {
                // DATE ranges may widen a valid non-midnight boundary to
                // its day, provided the original residual remains. Validate
                // the full timestamp before the date-only storage parser.
                value
            } else {
                return Err(error(
                    crate::error::catalog::ER_WRONG_VALUE,
                    "temporal index bound cannot preserve text comparison",
                ));
            }
        } else {
            value
        };
        Ok(match target {
            ParamTarget::Int => KeyPart::Int(value_to_int_lossy(&value)),
            ParamTarget::UInt => match &value {
                Value::Int(v) if *v < 0 => {
                    return Err(error(
                        crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                        "negative value in unsigned column",
                    ))
                }
                _ => KeyPart::UInt(value_to_uint_lossy(&value)),
            },
            ParamTarget::Float32 => KeyPart::Float32(crate::storage::core::f32_to_order_u32(
                value_to_f64_lossy(&value) as f32,
            )),
            ParamTarget::Float64 => KeyPart::Float64(crate::storage::core::f64_to_order_u64(
                value_to_f64_lossy(&value),
            )),
            ParamTarget::Decimal { precision, scale } => {
                decimal_keypart_for_value(&value, precision, scale)?
            }
            ParamTarget::CharString => KeyPart::String(value_to_key_string(value, true)),
            ParamTarget::CollatedCharString { collation_id } => KeyPart::CollatedString {
                collation_id,
                value: value_to_key_string(value, true),
            },
            ParamTarget::String => match value {
                Value::String(s) | Value::Decimal(s) => KeyPart::String(s),
                Value::Bytes(b) => KeyPart::String(String::from_utf8_lossy(&b).into_owned()),
                other => KeyPart::String(other.to_display_string()),
            },
            ParamTarget::CollatedString { collation_id } => KeyPart::CollatedString {
                collation_id,
                value: value_to_key_string(value, false),
            },
            ParamTarget::Bytes => match value {
                Value::Bytes(b) => KeyPart::Bytes(b),
                value @ (Value::String(_) | Value::SmallString(_)) => {
                    let Value::Bytes(bytes) = crate::engine::expr::encode_binary_assignment_value(
                        value,
                        crate::engine::expr::connection_charset(),
                    )?
                    else {
                        unreachable!("character value encodes as bytes")
                    };
                    KeyPart::Bytes(bytes)
                }
                Value::Decimal(s) => KeyPart::Bytes(s.into_bytes()),
                other => KeyPart::Bytes(other.to_display_string().into_bytes()),
            },
            ParamTarget::JsonScalar => match value {
                Value::Json(bytes) => KeyPart::Bytes(bytes),
                other => KeyPart::Bytes(crate::engine::json::scalar_from_value(&other)),
            },
            ParamTarget::Date => KeyPart::Date(value_to_date_days(&value)?),
            ParamTarget::Time => KeyPart::Time(value_to_time_micros(&value)),
            ParamTarget::DateTime => KeyPart::DateTime(value_to_datetime_micros(&value)?),
            ParamTarget::Timestamp => {
                let micros = match &value {
                    Value::Timestamp(micros) => *micros,
                    _ => crate::engine::expr::session_local_datetime_to_utc_timestamp_micros(
                        value_to_datetime_micros(&value)?,
                    )?,
                };
                KeyPart::DateTime(micros)
            }
            ParamTarget::Year => KeyPart::Year(value_to_year_byte(&value)),
            ParamTarget::LikePattern { .. } => {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "LIKE pattern bound must be resolved at the range boundary",
                ))
            }
        })
    }
}

fn value_to_key_string(value: Value, compact_char: bool) -> String {
    let text = match value {
        Value::String(s) | Value::Decimal(s) => s,
        Value::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        other => other.to_display_string(),
    };
    if compact_char {
        compact_char_storage_str(&text).to_string()
    } else {
        text
    }
}

fn value_to_int_lossy(v: &Value) -> i64 {
    match v {
        Value::Int(x) => *x,
        Value::UInt(x) => *x as i64,
        Value::Float(x) => *x as i64,
        Value::Double(x) => *x as i64,
        Value::DecimalCompact(mag, scale) => {
            crate::util::decimal::rescale_trunc_i128(*mag as i128, *scale, 0).unwrap_or(0) as i64
        }
        Value::Decimal128(mag, scale) => crate::util::decimal::rescale_trunc_i128(*mag, *scale, 0)
            .and_then(|v| i64::try_from(v).ok())
            .unwrap_or(if *mag < 0 { i64::MIN } else { i64::MAX }),
        Value::DecimalWide(value) => v_to_f64_decimal(value).trunc() as i64,
        Value::Decimal(s) | Value::String(s) => s
            .trim()
            .parse::<i64>()
            .unwrap_or_else(|_| crate::util::decimal::parse_lenient_f64(s).value as i64),
        Value::SmallString(b) => {
            let text = unsafe { std::str::from_utf8_unchecked(b) };
            text.trim()
                .parse::<i64>()
                .unwrap_or_else(|_| crate::util::decimal::parse_lenient_f64(text).value as i64)
        }
        Value::Bytes(b) => {
            let text = String::from_utf8_lossy(b);
            text.trim()
                .parse::<i64>()
                .unwrap_or_else(|_| crate::util::decimal::parse_lenient_f64(&text).value as i64)
        }
        Value::Date(_)
        | Value::Time(_)
        | Value::DateTime(_)
        | Value::Timestamp(_)
        | Value::Year(_) => crate::types::temporal_to_mysql_number(v)
            .expect("temporal variant")
            .to_i64(),
        Value::Json(b) => crate::engine::json::coerce_i64(b),
        Value::Geometry(_) => 0,
        Value::Vector(_) => 0,
        Value::Null => 0,
    }
}

fn value_to_uint_lossy(v: &Value) -> u64 {
    match v {
        Value::UInt(x) => *x,
        Value::Int(x) => *x.max(&0) as u64,
        Value::Float(x) => x.max(0.0) as u64,
        Value::Double(x) => x.max(0.0) as u64,
        Value::DecimalCompact(mag, scale) => {
            crate::util::decimal::rescale_trunc_i128((*mag).max(0) as i128, *scale, 0).unwrap_or(0)
                as u64
        }
        Value::Decimal128(mag, scale) => crate::util::decimal::rescale_trunc_i128(*mag, *scale, 0)
            .and_then(|v| u64::try_from(v).ok())
            .unwrap_or(if *mag < 0 { 0 } else { u64::MAX }),
        Value::DecimalWide(value) => v_to_f64_decimal(value).max(0.0).trunc() as u64,
        Value::Decimal(s) | Value::String(s) => s
            .trim()
            .parse::<u64>()
            .unwrap_or_else(|_| crate::util::decimal::parse_lenient_f64(s).value as u64),
        Value::SmallString(b) => {
            let text = unsafe { std::str::from_utf8_unchecked(b) };
            text.trim()
                .parse::<u64>()
                .unwrap_or_else(|_| crate::util::decimal::parse_lenient_f64(text).value as u64)
        }
        Value::Bytes(b) => {
            let text = String::from_utf8_lossy(b);
            text.trim()
                .parse::<u64>()
                .unwrap_or_else(|_| crate::util::decimal::parse_lenient_f64(&text).value as u64)
        }
        Value::Date(_)
        | Value::Time(_)
        | Value::DateTime(_)
        | Value::Timestamp(_)
        | Value::Year(_) => crate::types::temporal_to_mysql_number(v)
            .expect("temporal variant")
            .to_i64()
            .max(0) as u64,
        Value::Json(b) => crate::engine::json::coerce_i64(b).max(0) as u64,
        Value::Geometry(_) => 0,
        Value::Vector(_) => 0,
        Value::Null => 0,
    }
}

fn value_to_f64_lossy(v: &Value) -> f64 {
    match v {
        Value::Float(x) => *x as f64,
        Value::Double(x) => *x,
        Value::Int(x) => *x as f64,
        Value::UInt(x) => *x as f64,
        Value::DecimalCompact(mag, scale) => (*mag as f64) / 10f64.powi(*scale as i32),
        Value::Decimal128(mag, scale) => (*mag as f64) / 10f64.powi(*scale as i32),
        Value::DecimalWide(value) => v_to_f64_decimal(value),
        Value::Decimal(s) | Value::String(s) => s.trim().parse::<f64>().unwrap_or(0.0),
        Value::SmallString(b) => unsafe { std::str::from_utf8_unchecked(b) }
            .trim()
            .parse::<f64>()
            .unwrap_or(0.0),
        Value::Bytes(b) => String::from_utf8_lossy(b)
            .trim()
            .parse::<f64>()
            .unwrap_or(0.0),
        Value::Date(_)
        | Value::Time(_)
        | Value::DateTime(_)
        | Value::Timestamp(_)
        | Value::Year(_) => crate::types::temporal_to_mysql_number(v)
            .expect("temporal variant")
            .to_f64(),
        Value::Json(b) => crate::engine::json::coerce_f64(b),
        Value::Geometry(_) => 0.0,
        Value::Vector(_) => 0.0,
        Value::Null => 0.0,
    }
}

/// Parse an ASCII decimal literal (e.g. `"-12.34"` or `"100"`) into a
/// signed scaled magnitude at `scale`. Returns (value × 10^scale).
/// Used both by `KeyPart::resolve_param` for Decimal parameters and by
/// `KeyPart::from_value` for `Value::Decimal(String)` round-tripping.
fn parse_decimal_string_to_i128(s: &str, scale: u8) -> i128 {
    let s = s.trim();
    let negative = s.starts_with('-');
    let body = s.trim_start_matches(['+', '-']);
    let (int_part, frac_part) = match body.split_once('.') {
        Some((i, f)) => (i, f),
        None => (body, ""),
    };
    let mut mag: i128 = 0;
    for ch in int_part.bytes() {
        if !ch.is_ascii_digit() {
            return 0;
        }
        mag = mag
            .checked_mul(10)
            .and_then(|m| m.checked_add((ch - b'0') as i128))
            .unwrap_or(i128::MAX);
    }
    // Truncate / right-pad fractional part to exactly `scale` digits to
    // match column-defined scale (mirrors the heap codec's behaviour).
    let scale = scale as usize;
    let frac_digits: i128 = {
        let take = frac_part.len().min(scale);
        let mut v: i128 = 0;
        for ch in frac_part.bytes().take(take) {
            if !ch.is_ascii_digit() {
                return 0;
            }
            v = v.saturating_mul(10) + (ch - b'0') as i128;
        }
        // Right-pad with zeros so the magnitude lines up with `scale`.
        for _ in take..scale {
            v = v.saturating_mul(10);
        }
        // Multiply integer part up to scale's exponent.
        let mut int_mag = mag;
        for _ in 0..scale {
            int_mag = int_mag.saturating_mul(10);
        }
        int_mag.saturating_add(v)
    };
    if negative {
        -frac_digits
    } else {
        frac_digits
    }
}

fn decimal_keypart_for_value(v: &Value, precision: u8, scale: u8) -> Result<KeyPart, QueryError> {
    let exact = v.exact_decimal().or_else(|| match v {
        Value::String(s) | Value::Decimal(s) => crate::util::decimal::parse_exact_decimal_big(s)
            .map(|(magnitude, source_scale)| {
                crate::util::decimal::ExactDecimal::from_big(magnitude, source_scale)
            }),
        Value::SmallString(bytes) => crate::util::decimal::parse_exact_decimal_big(unsafe {
            std::str::from_utf8_unchecked(bytes)
        })
        .map(|(magnitude, source_scale)| {
            crate::util::decimal::ExactDecimal::from_big(magnitude, source_scale)
        }),
        Value::Bytes(bytes) => std::str::from_utf8(bytes)
            .ok()
            .and_then(crate::util::decimal::parse_exact_decimal_big)
            .map(|(magnitude, source_scale)| {
                crate::util::decimal::ExactDecimal::from_big(magnitude, source_scale)
            }),
        _ => None,
    });
    if let Some(exact) = exact {
        let (magnitude, _) =
            crate::util::decimal::rescale_exact(exact, scale, true).into_big_parts();
        if precision > 38 {
            return Ok(KeyPart::DecimalWide {
                value: magnitude,
                scale,
            });
        }
        return magnitude
            .try_into()
            .map(|value| KeyPart::Decimal { value, scale })
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                    "decimal index key exceeds declared precision",
                )
            });
    }
    let value: i128 = match v {
        Value::DecimalCompact(mag, src_scale) => {
            // Re-scale to the target scale if they differ; mismatches
            // shouldn't happen in steady state but we normalise so the
            // resulting KeyPart compares correctly against persisted
            // entries written at the column's declared scale.
            let mut mag = *mag as i128;
            match scale.cmp(src_scale) {
                std::cmp::Ordering::Equal => {}
                std::cmp::Ordering::Greater => {
                    for _ in 0..(scale - src_scale) {
                        mag = mag.saturating_mul(10);
                    }
                }
                std::cmp::Ordering::Less => {
                    for _ in 0..(src_scale - scale) {
                        mag /= 10;
                    }
                }
            }
            mag
        }
        Value::Decimal128(mag, src_scale) => crate::util::decimal::rescale_exact(
            crate::util::decimal::ExactDecimal::I128(*mag, *src_scale),
            scale,
            true,
        )
        .into_big_parts()
        .0
        .try_into()
        .map_err(|_| {
            error(
                crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                "decimal index key exceeds i128",
            )
        })?,
        Value::DecimalWide(_) => unreachable!("handled by exact_decimal"),
        Value::Decimal(s) | Value::String(s) => parse_decimal_string_to_i128(s, scale),
        Value::SmallString(b) => {
            parse_decimal_string_to_i128(unsafe { std::str::from_utf8_unchecked(b) }, scale)
        }
        Value::Bytes(b) => parse_decimal_string_to_i128(&String::from_utf8_lossy(b), scale),
        Value::Int(x) => {
            let mut mag = *x as i128;
            for _ in 0..scale {
                mag = mag.saturating_mul(10);
            }
            mag
        }
        Value::UInt(x) => {
            let mut mag = *x as i128;
            for _ in 0..scale {
                mag = mag.saturating_mul(10);
            }
            mag
        }
        Value::Float(x) => {
            let scaled = (*x as f64) * 10f64.powi(scale as i32);
            scaled.round() as i128
        }
        Value::Double(x) => {
            let scaled = *x * 10f64.powi(scale as i32);
            scaled.round() as i128
        }
        Value::Null => return Ok(KeyPart::Null),
        // JSON doesn't legitimately key a DECIMAL column, but coerce via
        // its unquoted text so the result stays deterministic.
        Value::Json(b) => {
            parse_decimal_string_to_i128(&crate::engine::json::unquote_text(b), scale)
        }
        // Temporal / geometry values don't legitimately appear in a DECIMAL
        // column; fall through to a deterministic zero to keep the codec total.
        Value::Date(_)
        | Value::Time(_)
        | Value::DateTime(_)
        | Value::Timestamp(_)
        | Value::Year(_)
        | Value::Geometry(_) => 0,
        Value::Vector(_) => 0,
    };
    if precision > 38 {
        Ok(KeyPart::DecimalWide {
            value: num_bigint::BigInt::from(value),
            scale,
        })
    } else {
        Ok(KeyPart::Decimal { value, scale })
    }
}

fn value_to_date_days(v: &Value) -> Result<i32, QueryError> {
    Ok(match v {
        Value::Date(d) => *d,
        Value::Int(d) => *d as i32,
        Value::UInt(d) => *d as i32,
        Value::String(s) | Value::Decimal(s) => crate::storage::core::parse_date_to_days(s)?,
        Value::SmallString(s) => {
            crate::storage::core::parse_date_to_days(&String::from_utf8_lossy(s))?
        }
        Value::Bytes(b) => crate::storage::core::parse_date_to_days(&String::from_utf8_lossy(b))?,
        Value::DateTime(t) => {
            // truncate microseconds to days
            (*t / 86_400_000_000) as i32
        }
        _ => {
            return Err(error(
                crate::error::catalog::ER_WRONG_VALUE,
                "invalid DATE index bound",
            ))
        }
    })
}

fn value_to_time_micros(v: &Value) -> i64 {
    match v {
        Value::Time(t) => *t,
        Value::Int(t) => *t,
        Value::UInt(t) => *t as i64,
        _ => crate::engine::expr::parse_time_value_components(v)
            .map(|(negative, hour, minute, second, micros)| {
                let magnitude =
                    ((i64::from(hour) * 3_600 + i64::from(minute) * 60 + i64::from(second))
                        * 1_000_000)
                        + i64::from(micros);
                if negative {
                    -magnitude
                } else {
                    magnitude
                }
            })
            .unwrap_or(0),
    }
}

fn value_to_datetime_micros(v: &Value) -> Result<i64, QueryError> {
    Ok(match v {
        Value::DateTime(t) => *t,
        Value::Int(t) => *t,
        Value::UInt(t) => *t as i64,
        Value::String(s) | Value::Decimal(s) => crate::storage::core::parse_datetime_to_micros(s)?,
        Value::SmallString(s) => {
            crate::storage::core::parse_datetime_to_micros(&String::from_utf8_lossy(s))?
        }
        Value::Bytes(b) => {
            crate::storage::core::parse_datetime_to_micros(&String::from_utf8_lossy(b))?
        }
        Value::Date(d) => *d as i64 * 86_400_000_000,
        _ => {
            return Err(error(
                crate::error::catalog::ER_WRONG_VALUE,
                "invalid DATETIME index bound",
            ))
        }
    })
}

fn value_to_year_byte(v: &Value) -> u8 {
    match v {
        Value::Year(y) => *y,
        Value::Int(v) => {
            let v = *v;
            if (1..=69).contains(&v) {
                (v + 100) as u8
            } else if (70..=99).contains(&v) {
                (v - 70 + 70) as u8
            } else if (1901..=2155).contains(&v) {
                (v - 1900) as u8
            } else {
                0
            }
        }
        Value::UInt(v) => value_to_year_byte(&Value::Int(*v as i64)),
        // A string bound (`WHERE year_col = '2020'`) must parse to the same
        // YEAR byte as the integer form, not fall through to 0 (= year 0000),
        // which made the index probe miss every real row.
        Value::String(s) | Value::Decimal(s) => s
            .trim()
            .parse::<i64>()
            .map(|n| value_to_year_byte(&Value::Int(n)))
            .unwrap_or(0),
        Value::Bytes(b) => String::from_utf8_lossy(b)
            .trim()
            .parse::<i64>()
            .map(|n| value_to_year_byte(&Value::Int(n)))
            .unwrap_or(0),
        _ => 0,
    }
}

impl IndexBound {
    pub fn resolve_params(&self) -> Result<IndexBound, QueryError> {
        Ok(IndexBound {
            key: self.key.resolve_params()?,
            inclusive: self.inclusive,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct IndexBound {
    pub key: IndexKey,
    pub inclusive: bool,
}

#[derive(Debug, Clone)]
pub struct StoredRow {
    pub row_id: RowId,
    pub values: Vec<Value>,
}

/// Stable identity of a row selected for UPDATE/DELETE.
///
/// `RowId` is allocated independently by every partition heap, so it is not
/// sufficient to identify a row of a partitioned table. DML candidate
/// selection must retain the originating storage all the way to the write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DmlRowRef {
    pub storage_id: u64,
    pub row_id: RowId,
}

#[derive(Debug, Clone)]
pub struct DmlStoredRow {
    pub row_ref: DmlRowRef,
    pub values: Vec<Value>,
}

thread_local! {
    /// Whether the statement currently executing on this thread runs
    /// with the session `optimizer_switch` flag `use_invisible_indexes=on`.
    /// Installed by the executor entry point through
    /// [`InvisibleIndexScope`] so every index-candidate iteration —
    /// planner and executor alike — sees one consistent visibility
    /// decision without threading a flag through dozens of call sites.
    /// Parallel workers never choose index access paths themselves
    /// (they execute already-planned scans), so a thread-local on the
    /// coordinating session thread is sound.
    static INVISIBLE_INDEXES_ENABLED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[inline]
fn invisible_indexes_enabled() -> bool {
    INVISIBLE_INDEXES_ENABLED.with(|cell| cell.get())
}

/// RAII scope making INVISIBLE indexes eligible for planning on this
/// thread while held. Restores the previous value on drop so nested
/// statement execution (subqueries, stored programs) stays balanced.
pub struct InvisibleIndexScope {
    prev: bool,
}

impl InvisibleIndexScope {
    pub fn install(enabled: bool) -> Self {
        let prev = INVISIBLE_INDEXES_ENABLED.with(|cell| cell.replace(enabled));
        Self { prev }
    }
}

impl Drop for InvisibleIndexScope {
    fn drop(&mut self) {
        let prev = self.prev;
        INVISIBLE_INDEXES_ENABLED.with(|cell| cell.set(prev));
    }
}

thread_local! {
    static ZONE_MAP_OPTIMIZER_ENABLED: std::cell::Cell<bool> = const { std::cell::Cell::new(true) };
}

pub struct ZoneMapOptimizerScope {
    prev: bool,
}

impl ZoneMapOptimizerScope {
    pub fn install(enabled: bool) -> Self {
        let prev = ZONE_MAP_OPTIMIZER_ENABLED.with(|cell| {
            let prev = cell.get();
            cell.set(enabled);
            prev
        });
        Self { prev }
    }
}

impl Drop for ZoneMapOptimizerScope {
    fn drop(&mut self) {
        ZONE_MAP_OPTIMIZER_ENABLED.with(|cell| cell.set(self.prev));
    }
}

pub(crate) fn zone_map_optimizer_enabled() -> bool {
    ZONE_MAP_OPTIMIZER_ENABLED.with(std::cell::Cell::get)
}

thread_local! {
    static LIKE_NGRAM_PREFILTER_ENABLED: std::cell::Cell<bool> = const { std::cell::Cell::new(true) };
}

pub struct LikeNgramPrefilterScope {
    prev: bool,
}

impl LikeNgramPrefilterScope {
    pub fn install(enabled: bool) -> Self {
        let prev = LIKE_NGRAM_PREFILTER_ENABLED.with(|cell| cell.replace(enabled));
        Self { prev }
    }
}

impl Drop for LikeNgramPrefilterScope {
    fn drop(&mut self) {
        LIKE_NGRAM_PREFILTER_ENABLED.with(|cell| cell.set(self.prev));
    }
}

pub(crate) fn like_ngram_prefilter_enabled() -> bool {
    crate::engine::expr::with_active_session(|session| {
        session
            .map(|session| {
                !session
                    .sysvars
                    .optimizer_switch
                    .contains("like_ngram_prefilter=off")
            })
            .unwrap_or_else(|| LIKE_NGRAM_PREFILTER_ENABLED.with(std::cell::Cell::get))
    })
}

impl TableSchema {
    /// Zone maps are enabled by default for persisted user tables.  The
    /// option is additive catalog metadata, so catalogs written before the
    /// feature naturally receive the new default.
    pub fn zone_map_enabled(&self) -> bool {
        if self.system || self.table_type.eq_ignore_ascii_case("TEMPORARY") {
            return false;
        }
        !self
            .mysql_table_options
            .iter()
            .rev()
            .find(|(key, _)| key.eq_ignore_ascii_case("ZONE_MAP"))
            .is_some_and(|(_, value)| value.eq_ignore_ascii_case("OFF"))
    }

    /// Configured zone size in bytes.  Parser validation guarantees a
    /// power-of-two multiple of the 16 KiB heap page size; this defensive
    /// fallback keeps manually-produced/legacy catalogs conservative.
    pub fn zone_size_bytes(&self) -> u64 {
        const DEFAULT: u64 = 1024 * 1024;
        self.mysql_table_options
            .iter()
            .rev()
            .find(|(key, _)| key.eq_ignore_ascii_case("ZONE_SIZE"))
            .and_then(|(_, value)| value.parse::<u64>().ok())
            .filter(|bytes| {
                (64 * 1024..=64 * 1024 * 1024).contains(bytes)
                    && bytes % (16 * 1024) == 0
                    && (bytes / (16 * 1024)).is_power_of_two()
            })
            .unwrap_or(DEFAULT)
    }

    pub fn zone_pages(&self) -> u32 {
        (self.zone_size_bytes() / (16 * 1024)) as u32
    }

    pub fn auto_increment_column_index(&self) -> Option<usize> {
        self.columns.iter().position(|column| column.auto_increment)
    }

    pub fn column_index(&self, name: &str) -> Result<usize, QueryError> {
        self.find_column_index(name).ok_or_else(|| {
            error(
                crate::error::catalog::ER_BAD_FIELD_ERROR,
                format!("unknown column '{name}'"),
            )
        })
    }

    /// [`Self::column_index`] for probes that only need presence: a miss
    /// builds no error message.
    pub fn find_column_index(&self, name: &str) -> Option<usize> {
        if !self.column_name_map.is_empty() {
            if name.bytes().all(|b| !b.is_ascii_uppercase()) {
                if let Some(&idx) = self.column_name_map.get(name) {
                    return Some(idx);
                }
            } else if let Some(&idx) = self.column_name_map.get(&name.to_ascii_lowercase()) {
                return Some(idx);
            }
        }
        // Slow-path linear scan also filters tombstone (Dropped)
        // columns so the failure mode for "name was dropped" matches
        // "name was never declared".
        self.columns.iter().position(|column| {
            column.state != ColumnState::Dropped && column.name.eq_ignore_ascii_case(name)
        })
    }

    /// Iterator over columns currently visible to SQL (`Public` and
    /// `WriteOnly`). Skips tombstones left behind by INSTANT DROP
    /// COLUMN. Used by SELECT *, SHOW COLUMNS, INSERT-without-column-
    /// list, and other operations that enumerate the table's logical
    /// columns. Yields `(physical_index, &column)` so callers can
    /// continue to project from `row.values[idx]`.
    pub fn iter_visible_columns(&self) -> impl Iterator<Item = (usize, &ColumnSchema)> {
        let mut columns = self
            .columns
            .iter()
            .enumerate()
            .filter(|(_, col)| col.state != ColumnState::Dropped)
            .collect::<Vec<_>>();
        columns.sort_by_key(|(idx, col)| (col.logical_pos, *idx));
        columns.into_iter()
    }

    /// Iterator over the columns `SELECT *` (and INSERT without a
    /// column list) expands to: logically visible columns minus the
    /// MySQL 8.0.23+ INVISIBLE ones. Explicit name references still
    /// resolve through `column_index`, which sees invisible columns.
    pub fn iter_select_columns(&self) -> impl Iterator<Item = (usize, &ColumnSchema)> {
        self.iter_visible_columns()
            .filter(|(_, col)| !col.invisible)
    }

    /// Number of columns visible to SQL — i.e. `columns.len()` minus
    /// any tombstones. After INSTANT DROP COLUMN this differs from
    /// `columns.len()`; user-facing surfaces (SHOW COLUMNS, INSERT
    /// arity check, SELECT * width) must use this rather than the
    /// raw array length.
    pub fn visible_column_count(&self) -> usize {
        self.columns
            .iter()
            .filter(|col| col.state != ColumnState::Dropped)
            .count()
    }

    /// Iterator over indexes that the read path may legitimately use.
    /// Filters out `Building` entries left by an in-flight ADD INDEX:
    /// the index's heap-scan backfill has not yet completed, so the
    /// B-tree is missing pre-DDL rows and a planner that picked it
    /// would silently miss data. Concurrent DML still maintains the
    /// `Building` index (so it stays current with respect to writes),
    /// but it must not be queryable until ADD INDEX flips it to
    /// `Public`.
    pub fn iter_public_indexes(&self) -> impl Iterator<Item = &IndexSchema> {
        self.indexes
            .iter()
            .filter(|idx| idx.state == IndexState::Public)
    }

    #[inline]
    pub fn iter_queryable_indexes(&self) -> impl Iterator<Item = &IndexSchema> {
        // Flatten the filter chain so LLVM can inline this through
        // query planning hot paths. INVISIBLE indexes are excluded
        // unless the executing statement runs with the session
        // `optimizer_switch` flag `use_invisible_indexes=on` (installed
        // via [`InvisibleIndexScope`]).
        let include_invisible = invisible_indexes_enabled();
        self.indexes.iter().filter(move |idx| {
            idx.state == IndexState::Public && (idx.visible || include_invisible)
                // A writer-session TIMESTAMP conversion cannot provide a
                // reader-session predicate or ordering. Retain the index for
                // maintenance but never let a hint bypass this safety guard.
                && !idx.key_expressions.iter().flatten().any(|part|
                    part.referenced_columns.iter().any(|name|
                        self.column_index(name).ok().is_some_and(|i|
                            matches!(self.columns[i].data_type, DataTypeKind::Timestamp { .. }))))
        })
    }

    pub fn result_type(&self, idx: usize) -> ColumnType {
        self.columns[idx].data_type.column_type()
    }

    /// Build the wire-protocol `ColumnSource` for column index `idx`. Captures:
    ///   * org_table / org_name — real catalog names so JDBC's
    ///     `getTableName()` / `getColumnName()` return the underlying
    ///     identifier rather than the SELECT-list alias;
    ///   * flags — NOT_NULL / PRI_KEY / UNIQUE_KEY / MULTIPLE_KEY /
    ///     UNSIGNED / AUTO_INCREMENT / ENUM / SET / NO_DEFAULT / BINARY,
    ///     OR-ed from the catalog's per-column attributes;
    ///   * length — the declared CHAR/VARCHAR/BINARY/VARBINARY/BIT length
    ///     in bytes, or the TEXT/BLOB cap; `None` falls back to the
    ///     type-derived default.
    pub fn wire_column_source(&self, idx: usize) -> crate::types::ColumnSource {
        use crate::mysql::protocol::{
            AUTO_INCREMENT_FLAG, BINARY_FLAG, BLOB_FLAG, ENUM_FLAG, MULTIPLE_KEY_FLAG,
            NOT_NULL_FLAG, NO_DEFAULT_VALUE_FLAG, PRI_KEY_FLAG, SET_FLAG, UNIQUE_KEY_FLAG,
            UNSIGNED_FLAG,
        };
        let column = &self.columns[idx];
        let mut flags: u16 = 0;
        if !column.nullable {
            flags |= NOT_NULL_FLAG;
        }
        if column.unsigned {
            flags |= UNSIGNED_FLAG;
        }
        if column.zerofill {
            flags |= crate::mysql::protocol::ZEROFILL_FLAG | UNSIGNED_FLAG;
        }
        if column.auto_increment {
            flags |= AUTO_INCREMENT_FLAG;
        }
        if column.default.is_none()
            && column.default_expr.is_none()
            && !column.nullable
            && !column.auto_increment
        {
            // MySQL sets NO_DEFAULT_VALUE for NOT NULL columns without
            // an explicit default. Nullable columns implicitly default
            // to NULL so the flag stays clear.
            flags |= NO_DEFAULT_VALUE_FLAG;
        }
        // Type-derived attribute flags. ENUM_FLAG / SET_FLAG are not in
        // `column_type_metadata` because the wire ColumnType collapses
        // both into VarString; we recover them from catalog here.
        match &column.data_type {
            DataTypeKind::Enum { .. } => flags |= ENUM_FLAG,
            DataTypeKind::Set { .. } => flags |= SET_FLAG,
            DataTypeKind::Binary { .. } | DataTypeKind::VarBinary { .. } => flags |= BINARY_FLAG,
            DataTypeKind::Text { .. } => flags |= BLOB_FLAG,
            DataTypeKind::Blob { .. } => flags |= BLOB_FLAG | BINARY_FLAG,
            _ => {}
        }
        if column
            .collation
            .as_deref()
            .and_then(|name| crate::charset::resolve_collation(name).ok())
            .is_some_and(|collation| collation.binary)
        {
            flags |= BINARY_FLAG;
        }
        if matches!(column.data_type, DataTypeKind::Timestamp { .. }) {
            flags |= crate::mysql::protocol::TIMESTAMP_FLAG;
        }
        if column.on_update_expr.is_some() {
            flags |= crate::mysql::protocol::ON_UPDATE_NOW_FLAG;
        }
        // All primary-key parts carry PRI. Secondary-key UNIQUE/MULTIPLE
        // flags describe only the leading part (MySQL setup_key_part_field).
        // Accumulate independently when a column leads several indexes.
        if self
            .primary_key
            .iter()
            .any(|c| c.eq_ignore_ascii_case(&column.name))
        {
            flags |= PRI_KEY_FLAG | NOT_NULL_FLAG;
        }
        for index in self.iter_public_indexes().filter(|index| !index.primary) {
            if index
                .columns
                .first()
                .is_some_and(|c| c.eq_ignore_ascii_case(&column.name))
            {
                flags |= if index.unique && index.columns.len() == 1 {
                    UNIQUE_KEY_FLAG
                } else {
                    MULTIPLE_KEY_FLAG
                };
            }
        }
        // Declared length for CHAR/VARCHAR/BINARY/VARBINARY/BIT. `None`
        // for unparameterised flavours so the protocol falls back to
        // the type-derived default (e.g. 65_535 for VARCHAR).
        let text_max_bytes_per_character = || {
            let collation = column
                .collation
                .as_deref()
                .unwrap_or(crate::charset::DEFAULT_COLLATION);
            crate::charset::resolve_collation(collation)
                .ok()
                .and_then(|collation| crate::charset::resolve_charset(collation.charset).ok())
                .map(|charset| u32::from(charset.max_len))
                .unwrap_or(1)
        };
        let length: Option<u32> = match &column.data_type {
            DataTypeKind::Bool => Some(1),
            DataTypeKind::Char { len } | DataTypeKind::VarChar { len } => {
                len.map(|n| (n as u32).saturating_mul(text_max_bytes_per_character()))
            }
            DataTypeKind::Binary { len } | DataTypeKind::VarBinary { len } => len.map(|n| n as u32),
            DataTypeKind::Bit { len } => len.map(|n| n as u32),
            // TEXT/BLOB families already store their declared byte cap in
            // the catalog.  Preserve it in plan-bound protocol metadata so
            // schema-sensitive expressions (for example UNHEX(LONGTEXT)) do
            // not need to reopen the table schema merely to recover width.
            DataTypeKind::Text { max_len } | DataTypeKind::Blob { max_len } => Some(*max_len),
            DataTypeKind::Enum { values } => Some(
                values
                    .iter()
                    .map(|value| value.chars().count() as u32)
                    .max()
                    .unwrap_or(0)
                    .saturating_mul(text_max_bytes_per_character()),
            ),
            DataTypeKind::Set { values } => Some(
                values
                    .iter()
                    .map(|value| value.chars().count() as u32)
                    .sum::<u32>()
                    .saturating_add(values.len().saturating_sub(1) as u32)
                    .saturating_mul(text_max_bytes_per_character()),
            ),
            _ => None,
        };
        // Decimals override: FLOAT(M,D)/DOUBLE(M,D) carry D; TIME/DATETIME/
        // TIMESTAMP carry fsp. DECIMAL goes through ColumnType.scale.
        let decimals: Option<u8> = match &column.data_type {
            DataTypeKind::Float { scale, .. } | DataTypeKind::Double { scale, .. } => *scale,
            DataTypeKind::Time { fsp }
            | DataTypeKind::DateTime { fsp }
            | DataTypeKind::Timestamp { fsp } => Some(*fsp),
            _ => None,
        };
        let numeric_precision: Option<u8> = match &column.data_type {
            DataTypeKind::Float {
                precision,
                scale: Some(_),
            }
            | DataTypeKind::Double {
                precision,
                scale: Some(_),
            } => *precision,
            _ => None,
        };
        // Real per-column collation for the wire column definition:
        // binary types report 63, text types their declared (or default)
        // collation. Non-string types keep the type-derived default.
        let collation_id: Option<u16> = match &column.data_type {
            DataTypeKind::Binary { .. }
            | DataTypeKind::VarBinary { .. }
            | DataTypeKind::Blob { .. } => Some(crate::charset::collation_id("binary")),
            DataTypeKind::Char { .. }
            | DataTypeKind::VarChar { .. }
            | DataTypeKind::Text { .. }
            | DataTypeKind::Enum { .. }
            | DataTypeKind::Set { .. } => Some(crate::charset::collation_id(
                column
                    .collation
                    .as_deref()
                    .unwrap_or(crate::charset::DEFAULT_COLLATION),
            )),
            _ => None,
        };
        // A materialized derived/CTE column has a visible table name, but no
        // catalog origin of its own.  Advertising the synthetic schema name
        // as `org_table` made clients believe it was a base column and also
        // destroyed expression coercibility information at the next query
        // block (for example a charset-introduced expression versus an ASCII
        // catalog column).  MySQL leaves the origin fields empty here.
        let (org_table, org_name) = if self.table_type.eq_ignore_ascii_case("DERIVED") {
            (String::new(), String::new())
        } else {
            (self.name.clone(), column.name.clone())
        };
        let wire_type = match column.data_type {
            DataTypeKind::Binary { .. } | DataTypeKind::Char { .. } => {
                Some(crate::mysql::protocol::MYSQL_TYPE_STRING)
            }
            DataTypeKind::VarBinary { .. } => Some(crate::mysql::protocol::MYSQL_TYPE_VAR_STRING),
            // Field_blob::type() in MySQL uses BLOB for every TEXT/BLOB
            // family; the declared capacity is carried separately in length.
            DataTypeKind::Text { .. } | DataTypeKind::Blob { .. } => {
                Some(crate::mysql::protocol::MYSQL_TYPE_BLOB)
            }
            _ => None,
        };
        crate::types::ColumnSource {
            vector_type: if let DataTypeKind::Vector { datatype, dim } = column.data_type {
                Some((datatype, dim))
            } else {
                None
            },
            collation_coercibility: Some(2),
            dynamic_parameter: false,
            wire_type,
            schema: self.database.clone(),
            table: self.name.clone(),
            org_table,
            org_name,
            flags,
            length,
            numeric_precision,
            decimals,
            collation_id,
        }
    }

    pub fn wire_column_source_with_alias(
        &self,
        idx: usize,
        alias: Option<&str>,
    ) -> crate::types::ColumnSource {
        let mut source = self.wire_column_source(idx);
        if let Some(alias) = alias.filter(|alias| !alias.is_empty()) {
            source.table = alias.to_string();
        }
        source
    }

    pub fn index_key(&self, index: &IndexSchema, values: &[Value]) -> Result<IndexKey, QueryError> {
        if index.has_multi_valued_parts() {
            return self
                .index_keys(index, values)?
                .into_iter()
                .next()
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "multi-valued index produced no keys",
                    )
                });
        }
        if index.is_spatial() {
            return self.spatial_index_key(index, values);
        }
        if index.is_vector() {
            // A vector index keeps only a degenerate B-tree (constant key,
            // unique per row via the trailing row_id) as a durable row-id
            // registry. The discriminating data is the in-memory HNSW graph,
            // so the B-tree key carries no vector bytes (avoiding multi-KB
            // keys for high-dimensional vectors).
            let mut parts = SmallVec::<[KeyPart; 4]>::new();
            parts.push(KeyPart::Null);
            return Ok(IndexKey::from(parts));
        }
        let mut parts = SmallVec::<[KeyPart; 4]>::new();
        // Declared key parts: pos in [0, columns.len()). Expression
        // parts evaluate `program(values)` and treat the result as the
        // key value; column parts continue to read `values[col_idx]`.
        // INCLUDE columns (pos >= columns.len()) are always plain
        // column references — MySQL/standard SQL forbid expressions in
        // INCLUDE, and our parser enforces that.
        let key_len = index.columns.len();
        for pos in 0..key_len {
            let part = match index.key_part(pos) {
                Some(IndexKeyPart::Column(name)) => {
                    let idx = self.column_index(name)?;
                    let column = self.columns.get(idx).ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_BAD_FIELD_ERROR,
                            "index column is out of range",
                        )
                    })?;
                    KeyPart::from_column_value_with_collation(
                        &values[idx],
                        &column.data_type,
                        column.collation.as_deref(),
                    )
                }
                Some(IndexKeyPart::Expression(expr_part)) => {
                    let value = self.eval_index_expression(expr_part, values)?;
                    // Canonicalise the runtime value to the expression's
                    // catalogued key type. Functions may return a more
                    // specific signedness variant than the coarse static
                    // descriptor (YEAR() returns UInt while its functional
                    // index descriptor is BIGINT). Planner bounds already use
                    // this ParamTarget; storing the raw Value variant would
                    // make otherwise equal Int/UInt keys sort into disjoint
                    // ranges and produce false-negative index scans.
                    let target = index_expression_param_target(expr_part);
                    KeyPart::for_target(value, target)?
                }
                None => {
                    return Err(error(
                        crate::error::catalog::ER_BAD_FIELD_ERROR,
                        "index key part is out of range",
                    ))
                }
            };
            parts.push(index.apply_prefix_to_key_part(pos, part));
        }
        for include_col in index.included_columns.iter() {
            let idx = self.column_index(include_col)?;
            let column = self.columns.get(idx).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_BAD_FIELD_ERROR,
                    "INCLUDE column is out of range",
                )
            })?;
            let pos = parts.len();
            let part = KeyPart::from_column_value_with_collation(
                &values[idx],
                &column.data_type,
                column.collation.as_deref(),
            );
            parts.push(index.apply_prefix_to_key_part(pos, part));
        }
        Ok(IndexKey::from(parts))
    }

    pub fn index_keys(
        &self,
        index: &IndexSchema,
        values: &[Value],
    ) -> Result<Vec<IndexKey>, QueryError> {
        if index.is_fulltext() {
            return self.fulltext_index_keys(index, values);
        }
        if !index.has_multi_valued_parts() {
            return Ok(vec![self.index_key(index, values)?]);
        }
        if !index.included_columns.is_empty() {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "multi-valued indexes with INCLUDE columns are not supported",
            ));
        }
        let Some((multi_pos, expr_part)) =
            index
                .key_expressions
                .iter()
                .enumerate()
                .find_map(|(pos, expr)| {
                    expr.as_ref()
                        .filter(|expr| expr.multi_valued)
                        .map(|expr| (pos, expr))
                })
        else {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "multi-valued index requires an ARRAY expression key part",
            ));
        };
        let elements = self
            .eval_multi_valued_index_expression(expr_part, values)
            .map_err(|err| {
                if matches!(err.code(), 3903 | 3904) {
                    err.map_message(|message| format!("{message} '{}'", index.name))
                } else {
                    err
                }
            })?;
        let mut keys = Vec::new();
        for element in elements {
            let mut parts = SmallVec::<[KeyPart; 4]>::new();
            for pos in 0..index.columns.len() {
                let part = if pos == multi_pos {
                    let target = index_expression_param_target(expr_part);
                    KeyPart::for_target(element.clone(), target)?
                } else {
                    match index.key_part(pos) {
                        Some(IndexKeyPart::Column(name)) => {
                            let column_idx = self.column_index(name)?;
                            let column = self.columns.get(column_idx).ok_or_else(|| {
                                error(
                                    crate::error::catalog::ER_BAD_FIELD_ERROR,
                                    "index column is out of range",
                                )
                            })?;
                            KeyPart::from_column_value_with_collation(
                                &values[column_idx],
                                &column.data_type,
                                column.collation.as_deref(),
                            )
                        }
                        Some(IndexKeyPart::Expression(expr)) => {
                            let value = self.eval_index_expression(expr, values)?;
                            KeyPart::for_target(value, index_expression_param_target(expr))?
                        }
                        None => {
                            return Err(error(
                                crate::error::catalog::ER_BAD_FIELD_ERROR,
                                "index key part is out of range",
                            ))
                        }
                    }
                };
                parts.push(index.apply_prefix_to_key_part(pos, part));
            }
            keys.push(IndexKey::from(parts));
        }
        keys.sort_unstable();
        keys.dedup();
        Ok(keys)
    }

    pub(crate) fn multi_valued_index_lookup_part(
        &self,
        index: &IndexSchema,
        pos: usize,
        value: Value,
    ) -> Result<KeyPart, QueryError> {
        let expr_part = index
            .key_expression(pos)
            .filter(|expr| expr.multi_valued)
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "multi-valued index requires an expression key part",
                )
            })?;
        let target = multi_valued_index_cast_target(expr_part).ok_or_else(|| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "multi-valued index requires CAST target",
            )
        })?;
        let element = cast_multi_valued_index_element(value, &target)?;
        let key_target = index_expression_param_target(expr_part);
        let part = KeyPart::for_target(element, key_target)?;
        Ok(index.apply_prefix_to_key_part(pos, part))
    }

    pub(crate) fn multi_valued_index_param_target(
        &self,
        index: &IndexSchema,
    ) -> Option<ParamTarget> {
        let expr_part = index
            .key_expressions
            .iter()
            .flatten()
            .find(|expr| expr.multi_valued)?;
        let target = multi_valued_index_cast_target(expr_part)?.to_ascii_uppercase();
        let base = target
            .split_once('(')
            .map_or(target.as_str(), |(base, _)| base);
        Some(match base {
            "SIGNED" => ParamTarget::Int,
            "UNSIGNED" => ParamTarget::UInt,
            "DECIMAL" => {
                let params = decimal_target_params(&target);
                ParamTarget::Decimal {
                    precision: params.first().copied().unwrap_or(65),
                    scale: params.get(1).copied().unwrap_or(30),
                }
            }
            "CHAR" | "DATE" | "DATETIME" | "TIME" => ParamTarget::String,
            _ => ParamTarget::String,
        })
    }

    /// Generate all inverted keys of one row for a FULLTEXT index: take each indexed column's
    /// text, tokenize it, and encode every distinct token as `IndexKey::from([KeyPart::String(token), KeyPart::Bytes(payload)])`,
    /// where the payload is encoded by [`crate::storage::fulltext::payload::FtPayload`] with the token's
    /// per-row `doc_len` / `tf` / positions.
    /// Multi-column FULLTEXT merges all columns' tokens (natural-language mode does not distinguish
    /// source columns); a position gap is inserted between columns to prevent cross-column phrase hits.
    ///
    /// Putting the payload into the key (rather than the B-tree value) keeps INSERT/DELETE/UPDATE
    /// maintenance entirely on the existing `index_keys` path: DELETE recomputes a **byte-identical**
    /// key from the same row values for exact deletion; UPDATE's old∖new / new∖old diff naturally
    /// detects positions/tf changes because the payload is part of the key. NULL / empty text emit no token.
    pub fn fulltext_index_keys(
        &self,
        index: &IndexSchema,
        values: &[Value],
    ) -> Result<Vec<IndexKey>, QueryError> {
        use crate::storage::fulltext::payload::{config_from_meta, FtPayload};
        // Build tokenizer from the index's persisted meta so DML maintenance
        // and query search always agree on the tokenization rules — even
        // across restarts that change MYDB_FT_* env. Falls back to a default
        // meta (all None → global env) for legacy indexes without a meta.
        let default_meta = crate::storage::schema::FulltextMeta::default();
        let meta = index.fulltext.as_ref().unwrap_or(&default_meta);
        let tokenizer = crate::storage::fulltext::build_tokenizer_from_meta(meta);
        let cfg = config_from_meta(meta);
        // token -> its position list in this row (ascending, with inter-column gaps). doc_len is the total token count.
        let mut positions_by_token: std::collections::HashMap<String, Vec<u32>> =
            std::collections::HashMap::new();
        let mut doc_len: u64 = 0;
        let mut next_pos: u64 = 0;
        for (col_ord, col_name) in index.columns.iter().enumerate() {
            let idx = self.column_index(col_name)?;
            let text: std::borrow::Cow<'_, str> = match values.get(idx) {
                Some(Value::String(s)) => std::borrow::Cow::Borrowed(s),
                Some(Value::SmallString(b)) => {
                    std::borrow::Cow::Borrowed(unsafe { std::str::from_utf8_unchecked(b) })
                }
                Some(Value::Null) | None => continue,
                Some(other) => std::borrow::Cow::Owned(other.to_display_string()),
            };
            if col_ord > 0 {
                // Leave a gap between columns: tokens in different columns are not adjacent, so phrases never match across columns.
                next_pos = next_pos.saturating_add(u64::from(cfg.position_gap));
            }
            let tokens = tokenizer.tokenize(&text);
            doc_len = doc_len.saturating_add(tokens.len() as u64);
            let last_position = (cfg.positions && !tokens.is_empty())
                .then(|| next_pos.saturating_add(tokens.len() as u64 - 1));
            crate::storage::fulltext::payload::validate_document_extent(doc_len, last_position)?;
            for token in tokens {
                positions_by_token
                    .entry(token)
                    .or_default()
                    .push(next_pos as u32);
                next_pos = next_pos.saturating_add(1);
            }
        }
        Ok(positions_by_token
            .into_iter()
            .map(|(token, positions)| {
                let payload = FtPayload {
                    doc_len: doc_len as u32,
                    tf: positions.len() as u32,
                    positions,
                };
                IndexKey::from(vec![
                    KeyPart::String(token),
                    KeyPart::Bytes(payload.encode(cfg.positions)),
                ])
            })
            .collect())
    }

    /// Encode the geometry MBR for shared DML, undo and visibility-repair
    /// interfaces. The spatial backend stores these coordinates in R-tree
    /// pages; this key is not maintained in a separate B-tree.
    pub fn spatial_index_key(
        &self,
        index: &IndexSchema,
        values: &[Value],
    ) -> Result<IndexKey, QueryError> {
        let col = index.columns.first().ok_or_else(|| {
            error(
                crate::error::catalog::ER_BAD_FIELD_ERROR,
                "spatial index has no column",
            )
        })?;
        let idx = self.column_index(col)?;
        let mbr = match values.get(idx) {
            Some(Value::Geometry(b)) => crate::engine::geo::mbr_of(b),
            _ => None,
        };
        // Empty / missing geometry → degenerate box at the origin (spatial
        // columns are NOT NULL; an empty geometry has no extent).
        let (minx, miny, maxx, maxy) = mbr
            .map(|m| (m.minx, m.miny, m.maxx, m.maxy))
            .unwrap_or((0.0, 0.0, 0.0, 0.0));
        let enc = crate::storage::core::f64_to_order_u64;
        Ok(IndexKey::from(vec![
            KeyPart::Float64(enc(minx)),
            KeyPart::Float64(enc(miny)),
            KeyPart::Float64(enc(maxx)),
            KeyPart::Float64(enc(maxy)),
        ]))
    }

    /// Evaluate a functional index expression on a row's value array.
    /// `program` is rebuilt on catalog load; if for any reason it's
    /// absent here (skipped serde field paired with a missing rebuild
    /// path) we fall back to interpreting the AST directly.
    fn eval_index_expression(
        &self,
        expr: &IndexExpression,
        values: &[Value],
    ) -> Result<Value, QueryError> {
        let _dml = crate::engine::expr::DmlExpressionGuard::new();
        let result = if let Some(program) = expr.program.as_ref() {
            Some(program.eval(self, values))
        } else {
            expr.ast
                .as_ref()
                .map(|ast| crate::engine::expr::eval_expr_legacy(ast, self, values))
        };
        if let Some(result) = result {
            return result.map_err(|err| {
                if err.code() == 1292 || err.code() == 1366 {
                    crate::error::catalog::ER_WARN_DATA_TRUNCATED_FUNCTIONAL_INDEX.error(format!(
                        "Data truncated for functional index expression '{}'",
                        expr.expr_sql
                    ))
                } else {
                    err
                }
            });
        }
        Err(error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            format!(
                "functional index expression `{}` is not compiled — corrupt catalog?",
                expr.expr_sql
            ),
        ))
    }

    pub(crate) fn eval_multi_valued_index_expression(
        &self,
        expr: &IndexExpression,
        values: &[Value],
    ) -> Result<Vec<Value>, QueryError> {
        let Some(Expr::Function { name, args, .. }) = expr.ast.as_ref() else {
            return multi_valued_index_elements(&self.eval_index_expression(expr, values)?);
        };
        if !name.eq_ignore_ascii_case("CAST") || args.len() < 2 {
            return multi_valued_index_elements(&self.eval_index_expression(expr, values)?);
        }
        let source = crate::engine::expr::eval_expr_legacy(&args[0], self, values)?;
        let target = match &args[1] {
            Expr::Literal(crate::parser::Literal::String(s)) => {
                cast_target_with_params(s, &args[2..])
            }
            _ => "JSON".to_string(),
        };
        let mut out = Vec::new();
        for element in multi_valued_index_elements(&source)? {
            out.push(cast_multi_valued_index_element(element, &target)?);
        }
        Ok(out)
    }

    pub fn best_index_for_columns(&self, columns: &[String]) -> Option<&IndexSchema> {
        // Only queryable indexes are eligible: `Building` entries may be
        // incomplete, and invisible indexes must be ignored by the optimizer.
        self.iter_queryable_indexes().find(|index| {
            columns.len() <= index.columns.len()
                && columns
                    .iter()
                    .zip(index.columns.iter())
                    .all(|(left, right)| normalize_name(left) == normalize_name(right))
        })
    }

    pub(crate) fn index_on_column_prefix(&self, column: &str) -> Option<&IndexSchema> {
        let column = normalize_name(column);
        self.iter_queryable_indexes()
            .find(|index| index.columns.first().is_some_and(|left| left == &column))
    }

    pub fn qualified_name(&self) -> String {
        table_key(&self.database, &self.name)
    }

    pub fn index_key_len(&self, index: &IndexSchema, prefix_len: usize) -> usize {
        (0..prefix_len.min(index.columns.len()))
            .map(|pos| match index.key_part(pos) {
                Some(IndexKeyPart::Column(name)) => {
                    let col = self
                        .columns
                        .iter()
                        .find(|c| c.name.eq_ignore_ascii_case(name));
                    match col {
                        Some(c) => {
                            index_column_key_byte_length(index, pos, &c.data_type)
                                + if c.nullable { 1 } else { 0 }
                        }
                        None => 8,
                    }
                }
                Some(IndexKeyPart::Expression(expr)) => {
                    index_column_key_byte_length(index, pos, &expr.data_type)
                        + if expr.nullable { 1 } else { 0 }
                }
                None => 8,
            })
            .sum()
    }

    pub fn index_prefix_cardinality(&self, index_name: &str, prefix_len: usize) -> Option<u64> {
        if prefix_len == 0 {
            return None;
        }
        self.stats
            .indexes
            .get(&normalize_name(index_name))
            .and_then(|stats| stats.prefix_distinct_counts.get(prefix_len - 1).copied())
    }

    /// Auto-ANALYZE staleness trigger. Unlike [`stats_needs_refresh_with_delta`]
    /// (which keys off a fixed 1/5 fraction and a never-analyzed table being
    /// always-stale), this uses a caller-supplied ratio so background
    /// Auto-ANALYZE re-samples a table once its accumulated modifications
    /// exceed `ratio` of its analyzed row count. Small tables (< 64 rows)
    /// re-sample on any change since a full scan is cheap. A table with zero
    /// pending modifications is never considered stale here — the background
    /// loop must not churn idle tables every interval.
    pub fn stats_needs_auto_analyze(&self, extra_modified: u64, ratio: f64) -> bool {
        let total = self
            .stats
            .modified_rows_since_analyze
            .saturating_add(extra_modified);
        if total == 0 {
            return false;
        }
        let rows = self.stats.row_count;
        if rows < 64 {
            return true;
        }
        let threshold = (((rows as f64) * ratio).ceil() as u64).max(1);
        total >= threshold
    }

    /// Threshold check that incorporates a sidecar "modified-rows delta"
    /// kept outside the schema. Callers that have access to a
    /// `Database` should pass `db.table_modification_delta(self.id)` —
    /// it counts the rows touched by DML since the last persisted
    /// `modified_rows_since_analyze` reset, so the refresh trigger fires
    /// at the same threshold the eager pre-sidecar path used.
    pub fn stats_needs_refresh_with_delta(&self, extra_modified: u64) -> bool {
        if self.stats.analyzed_at_unix_ms.is_none() {
            return true;
        }
        let threshold = (self.stats.row_count / 5).max(64);
        let threshold = if self.stats.row_count < 64 {
            1
        } else {
            threshold
        };
        self.stats
            .modified_rows_since_analyze
            .saturating_add(extra_modified)
            >= threshold
    }
}

impl DataTypeKind {
    pub fn column_type(&self) -> ColumnType {
        match self {
            DataTypeKind::TinyInt | DataTypeKind::Bool => ColumnType::Tiny,
            DataTypeKind::SmallInt => ColumnType::Short,
            DataTypeKind::Bit { len } => ColumnType::Bit {
                bits: len.unwrap_or(1).clamp(1, 64) as u8,
            },
            DataTypeKind::MediumInt => ColumnType::MediumInt,
            DataTypeKind::Int => ColumnType::Long,
            DataTypeKind::BigInt => ColumnType::LongLong,
            DataTypeKind::Year => ColumnType::Year,
            DataTypeKind::Float { .. } => ColumnType::Float,
            DataTypeKind::Double { .. } => ColumnType::Double,
            DataTypeKind::Decimal { precision, scale } => ColumnType::Decimal {
                precision: precision.unwrap_or(crate::types::MAX_DECIMAL_PRECISION),
                scale: scale.unwrap_or(0),
            },
            DataTypeKind::Date => ColumnType::Date,
            DataTypeKind::Time { .. } => ColumnType::Time,
            DataTypeKind::DateTime { .. } => ColumnType::DateTime,
            DataTypeKind::Timestamp { .. } => ColumnType::Timestamp,
            DataTypeKind::Blob { .. }
            | DataTypeKind::Binary { .. }
            | DataTypeKind::VarBinary { .. } => ColumnType::Blob,
            DataTypeKind::Json => ColumnType::Json,
            DataTypeKind::Geometry { .. } => ColumnType::Geometry,
            DataTypeKind::Vector { .. } => ColumnType::Vector,
            DataTypeKind::Char { .. }
            | DataTypeKind::VarChar { .. }
            | DataTypeKind::Text { .. }
            | DataTypeKind::Enum { .. }
            | DataTypeKind::Set { .. } => ColumnType::VarString,
        }
    }

    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            DataTypeKind::TinyInt
                | DataTypeKind::SmallInt
                | DataTypeKind::MediumInt
                | DataTypeKind::Int
                | DataTypeKind::BigInt
                // BOOL is a signed TINYINT(1) alias — an integer type, so
                // `BOOL UNSIGNED` renders its `unsigned` suffix like TINYINT.
                | DataTypeKind::Bool
        )
    }

    pub fn key_byte_length(&self) -> usize {
        match self {
            DataTypeKind::TinyInt | DataTypeKind::Bool | DataTypeKind::Year => 1,
            DataTypeKind::SmallInt => 2,
            DataTypeKind::MediumInt => 3,
            DataTypeKind::Int => 4,
            DataTypeKind::BigInt => 8,
            DataTypeKind::Float { .. } => 4,
            DataTypeKind::Double { .. } => 8,
            DataTypeKind::Decimal { precision, .. } => {
                let p = precision.unwrap_or(10) as usize;
                (p + 1) / 2 + 1
            }
            DataTypeKind::Date => 3,
            DataTypeKind::Time { .. } => 3,
            DataTypeKind::DateTime { .. } => 5,
            // TIMESTAMP shares DATETIME's on-disk layout — row_codec
            // `temporal_width(fsp, 5)` gives both a 5..=8 byte width.
            // Old value 4 was inconsistent and skewed the cost model.
            DataTypeKind::Timestamp { .. } => 5,
            DataTypeKind::Char { len } => len.unwrap_or(1) * 4,
            DataTypeKind::VarChar { len } => len.unwrap_or(255) * 4 + 2,
            DataTypeKind::Binary { len } => len.unwrap_or(1),
            DataTypeKind::VarBinary { len } => len.unwrap_or(255) + 2,
            DataTypeKind::Text { .. } | DataTypeKind::Blob { .. } | DataTypeKind::Json => 768 + 2,
            // Geometry is not a normal B-tree key column (it is indexed via a
            // SPATIAL R-tree over its MBR); report the 4-double MBR key width.
            DataTypeKind::Geometry { .. } => 32,
            // Vector is not a normal B-tree key column (it is indexed via the
            // HNSW vector index); report the declared byte width for width
            // estimation only.
            DataTypeKind::Vector { dim, datatype } => datatype.payload_len(dim.unwrap_or(192)) + 2,
            // ENUM stores a 1- or 2-byte ordinal (1 byte when ≤255
            // members, else 2). SET stores a bitmask of ceil(N/8) bytes,
            // capped at 8. Reporting the tight width keeps CBO key-space
            // estimates aligned with the actual on-disk footprint.
            DataTypeKind::Enum { values } => {
                if values.len() <= u8::MAX as usize {
                    1
                } else {
                    2
                }
            }
            DataTypeKind::Set { values } => ((values.len().max(1) + 7) / 8).min(8),
            DataTypeKind::Bit { len } => (len.unwrap_or(1) + 7) / 8,
        }
    }
}

fn index_column_key_byte_length(
    index: &IndexSchema,
    pos: usize,
    data_type: &DataTypeKind,
) -> usize {
    let Some(prefix_len) = index.column_prefix_len(pos) else {
        return data_type.key_byte_length();
    };
    match data_type {
        DataTypeKind::Char { .. } => prefix_len * 4,
        DataTypeKind::VarChar { .. } | DataTypeKind::Text { .. } => prefix_len * 4 + 2,
        DataTypeKind::Binary { .. } => prefix_len,
        DataTypeKind::VarBinary { .. } | DataTypeKind::Blob { .. } => prefix_len + 2,
        _ => data_type.key_byte_length(),
    }
}

impl From<DataType> for DataTypeKind {
    fn from(value: DataType) -> Self {
        match value {
            DataType::TinyInt => Self::TinyInt,
            DataType::SmallInt => Self::SmallInt,
            DataType::MediumInt => Self::MediumInt,
            DataType::Int => Self::Int,
            DataType::BigInt => Self::BigInt,
            DataType::Bool => Self::Bool,
            DataType::Float(v) => Self::Float {
                precision: v.map(|(p, _)| p),
                scale: v.map(|(_, s)| s),
            },
            DataType::Double(v) => Self::Double {
                precision: v.map(|(p, _)| p),
                scale: v.map(|(_, s)| s),
            },
            DataType::Decimal(v) => Self::Decimal {
                precision: v.map(|(p, _)| p),
                scale: v.map(|(_, s)| s),
            },
            DataType::Char(len) => Self::Char { len },
            DataType::VarChar(len) => Self::VarChar { len },
            DataType::Binary(len) => Self::Binary { len },
            DataType::VarBinary(len) => Self::VarBinary { len },
            DataType::Text(max_len) => Self::Text { max_len },
            DataType::Blob(max_len) => Self::Blob { max_len },
            DataType::Date => Self::Date,
            DataType::Time { fsp } => Self::Time { fsp },
            DataType::DateTime { fsp } => Self::DateTime { fsp },
            DataType::Timestamp { fsp } => Self::Timestamp { fsp },
            DataType::Year => Self::Year,
            DataType::Json => Self::Json,
            DataType::Geometry(subtype, srid) => Self::Geometry { subtype, srid },
            DataType::Vector(dim, datatype) => Self::Vector { dim, datatype },
            DataType::Enum(values) => Self::Enum { values },
            DataType::Set(values) => Self::Set { values },
            // Preserve the declaration exactly until DDL validation. In
            // particular BIT(0) is invalid (3013), while a bare BIT (`None`)
            // means BIT(1). Normalizing zero here made an invalid definition
            // indistinguishable from the valid default form.
            DataType::Bit(len) => Self::Bit { len },
        }
    }
}

impl KeyPart {
    /// Convert a `Value` into the corresponding `KeyPart`. Each binary
    /// `Value` variant maps to its peer `KeyPart` variant — no ASCII
    /// detour through `to_display_string`. `Value::Decimal(String)`
    /// (the large-magnitude fallback) is parsed back into the scaled
    /// magnitude form by inferring its scale from the literal.
    pub fn from_value(value: &Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Int(v) => Self::Int(*v),
            Value::UInt(v) => Self::UInt(*v),
            Value::Float(v) => Self::Float32(crate::storage::core::f32_to_order_u32(*v)),
            Value::Double(v) => Self::Float64(crate::storage::core::f64_to_order_u64(*v)),
            Value::DecimalCompact(mag, scale) => Self::Decimal {
                value: *mag as i128,
                scale: *scale,
            },
            Value::Decimal128(mag, scale) => Self::Decimal {
                value: *mag,
                scale: *scale,
            },
            Value::DecimalWide(value) => Self::DecimalWide {
                value: value.magnitude.clone(),
                scale: value.scale,
            },
            Value::Decimal(s) => {
                let scale = decimal_string_scale(s);
                match crate::util::decimal::parse_exact_decimal_big(s) {
                    Some((value, _)) => value.to_i128().map_or_else(
                        || Self::DecimalWide { value, scale },
                        |value| Self::Decimal { value, scale },
                    ),
                    None => Self::Decimal { value: 0, scale },
                }
            }
            Value::Bytes(v) => Self::Bytes(v.clone()),
            Value::String(v) => Self::String(v.clone()),
            Value::SmallString(b) => {
                Self::String(unsafe { std::str::from_utf8_unchecked(b) }.to_string())
            }
            // A JSON column is never a plain b-tree key in MySQL (it must be
            // indexed via a functional or multi-valued index whose key is the
            // *extracted* scalar). For the rare direct path we key on the
            // canonical jsonb bytes so equal documents share a key.
            Value::Json(v) => Self::Bytes(v.clone()),
            // Geometry, likewise, keys on its internal bytes for the rare
            // direct path (spatial columns are indexed via the R-tree).
            Value::Geometry(v) => Self::Bytes(v.clone()),
            // Vector keys on its internal bytes for the rare direct path
            // (vector columns are indexed via the HNSW graph).
            Value::Vector(v) => Self::Bytes(v.encode()),
            Value::Date(d) => Self::Date(*d),
            Value::Time(t) => Self::Time(*t),
            Value::DateTime(dt) => Self::DateTime(*dt),
            Value::Timestamp(dt) => Self::DateTime(*dt),
            Value::Year(y) => Self::Year(*y),
        }
    }

    pub fn from_column_value_with_collation(
        value: &Value,
        data_type: &DataTypeKind,
        collation: Option<&str>,
    ) -> Self {
        if let Some(collation) = collation.filter(|_| is_collated_key_type(data_type)) {
            let collation_id = crate::charset::collation_id(collation);
            return match value {
                Value::Null => Self::Null,
                _ => Self::CollatedString {
                    collation_id,
                    value: value_to_key_string(
                        value.clone(),
                        matches!(data_type, DataTypeKind::Char { .. }),
                    ),
                },
            };
        }
        if matches!(data_type, DataTypeKind::Char { .. }) {
            return match value {
                Value::Null => Self::Null,
                Value::String(v) | Value::Decimal(v) => {
                    Self::String(compact_char_storage_str(v).to_string())
                }
                Value::Bytes(v) => {
                    let text = String::from_utf8_lossy(v);
                    Self::String(compact_char_storage_str(&text).to_string())
                }
                other => {
                    let text = other.to_display_string();
                    Self::String(compact_char_storage_str(&text).to_string())
                }
            };
        }
        if let DataTypeKind::Decimal { precision, scale } = data_type {
            return decimal_keypart_for_value(value, precision.unwrap_or(10), scale.unwrap_or(0))
                .unwrap_or_else(|_| Self::from_value(value));
        }
        Self::from_value(value)
    }

    /// Reverse of `from_value`. The optional `data_type` hint disambiguates
    /// the few cases where the same KeyPart variant could decode to
    /// different `Value` forms (e.g. a `KeyPart::Decimal` may be returned
    /// as `Value::DecimalCompact` or `Value::Decimal` depending on
    /// magnitude).
    pub fn to_value(&self, data_type: &DataTypeKind) -> Value {
        match self {
            KeyPart::Null => Value::Null,
            KeyPart::Int(v) => Value::Int(*v),
            KeyPart::UInt(v) => Value::UInt(*v),
            KeyPart::Float32(bits) => Value::Float(crate::storage::core::f32_from_order_u32(*bits)),
            KeyPart::Float64(bits) => {
                Value::Double(crate::storage::core::f64_from_order_u64(*bits))
            }
            KeyPart::Decimal { value, scale } => {
                if *value >= i64::MIN as i128 && *value <= i64::MAX as i128 {
                    Value::DecimalCompact(*value as i64, *scale)
                } else {
                    Value::Decimal(format_decimal_i128(*value, *scale))
                }
            }
            KeyPart::DecimalWide { value, scale } => {
                Value::DecimalWide(Box::new(crate::types::DecimalWide {
                    magnitude: value.clone(),
                    scale: *scale,
                }))
            }
            KeyPart::Bytes(b) => Value::Bytes(b.clone()),
            KeyPart::String(s) => match data_type {
                DataTypeKind::Decimal { .. } => Value::Decimal(s.clone()),
                DataTypeKind::Char { len } => {
                    let mut out = s.clone();
                    normalize_char_string_for_read(&mut out, *len);
                    Value::String(out)
                }
                _ => Value::String(s.clone()),
            },
            KeyPart::CollatedString { value, .. } => match data_type {
                DataTypeKind::Char { len } => {
                    let mut out = value.clone();
                    normalize_char_string_for_read(&mut out, *len);
                    Value::String(out)
                }
                _ => Value::String(value.clone()),
            },
            KeyPart::Date(d) => Value::Date(*d),
            KeyPart::Time(t) => Value::Time(*t),
            KeyPart::DateTime(dt) => {
                if matches!(data_type, DataTypeKind::Timestamp { .. }) {
                    Value::Timestamp(*dt)
                } else {
                    Value::DateTime(*dt)
                }
            }
            KeyPart::Year(y) => Value::Year(*y),
            KeyPart::Param { .. } => {
                panic!("internal error: KeyPart::Param has no concrete value")
            }
        }
    }
}

#[inline]
pub fn apply_key_part_prefix(part: KeyPart, prefix_len: Option<usize>) -> KeyPart {
    let Some(prefix_len) = prefix_len.filter(|len| *len > 0) else {
        return part;
    };
    match part {
        KeyPart::String(value) => {
            let truncated = value.chars().take(prefix_len).collect::<String>();
            KeyPart::String(truncated)
        }
        KeyPart::CollatedString {
            collation_id,
            value,
        } => {
            let truncated = value.chars().take(prefix_len).collect::<String>();
            KeyPart::CollatedString {
                collation_id,
                value: truncated,
            }
        }
        KeyPart::Bytes(mut value) => {
            value.truncate(prefix_len);
            KeyPart::Bytes(value)
        }
        other => other,
    }
}

fn multi_valued_index_elements(value: &Value) -> Result<Vec<Value>, QueryError> {
    if matches!(value, Value::Null) {
        return Ok(Vec::new());
    }
    let bytes = match value {
        Value::Json(bytes) => bytes.clone(),
        other => {
            crate::engine::json::parse_text(&other.to_display_string(), 1, "multi-valued index")?
        }
    };
    let root = crate::engine::json::JsonbRef::new(&bytes)?;
    if root.is_array() {
        return Ok(root
            .array_iter()
            .filter(|node| !node.is_null())
            .map(|node| Value::Json(node.to_owned_bytes()))
            .collect());
    }
    if root.is_null() {
        return Ok(Vec::new());
    }
    if root.is_object() {
        return Err(error(
            crate::error::catalog::ER_INVALID_JSON_TEXT_IN_PARAM,
            "multi-valued index expression did not return a JSON array or scalar",
        ));
    }
    Ok(vec![Value::Json(bytes)])
}

fn cast_multi_valued_index_element(value: Value, target: &str) -> Result<Value, QueryError> {
    let target = target.to_ascii_uppercase();
    let base = target
        .split_once('(')
        .map_or(target.as_str(), |(base, _)| base.trim());
    match base {
        "SIGNED" => cast_mvi_int(&value, false),
        "UNSIGNED" => cast_mvi_int(&value, true),
        "CHAR" => Ok(Value::String(match value {
            Value::Json(bytes) => crate::engine::json::unquote_text(&bytes),
            other => other.to_display_string(),
        })),
        "JSON" => Ok(value),
        "DATE" | "DATETIME" | "TIME" | "DECIMAL" => {
            let text = multi_valued_index_element_text(value);
            if text.trim().is_empty() {
                return Err(mvi_cast_error(base));
            }
            match base {
                "DATE" => crate::storage::parse_date_to_days_bytes(text.as_bytes())
                    .map(Value::Date)
                    .map_err(|_| mvi_cast_error(base)),
                "TIME" => crate::storage::parse_time_to_micros(&text)
                    .map(Value::Time)
                    .map_err(|_| mvi_cast_error(base)),
                "DATETIME" => crate::storage::parse_datetime_to_micros(&text)
                    .map(Value::DateTime)
                    .map_err(|_| mvi_cast_error(base)),
                "DECIMAL" => {
                    let params = target
                        .split_once('(')
                        .and_then(|(_, rest)| rest.strip_suffix(')'))
                        .map(|rest| {
                            rest.split(',')
                                .filter_map(|v| v.trim().parse::<u8>().ok())
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    let precision = params.first().copied().unwrap_or(65);
                    let scale = params.get(1).copied().unwrap_or(30);
                    let (magnitude, source_scale) =
                        crate::util::decimal::parse_exact_decimal_big(&text)
                            .ok_or_else(|| mvi_cast_error(base))?;
                    let rounded = crate::util::decimal::rescale_exact(
                        crate::util::decimal::ExactDecimal::from_big(magnitude, source_scale),
                        scale,
                        true,
                    );
                    let (magnitude, _) = rounded.into_big_parts();
                    let digits = magnitude
                        .to_str_radix(10)
                        .trim_start_matches('-')
                        .trim_start_matches('0')
                        .len();
                    if digits > usize::from(precision) {
                        return Err(mvi_cast_error(base));
                    }
                    crate::engine::expr::cast_to_decimal(&Value::String(text), precision, scale)
                        .map_err(|_| mvi_cast_error(base))
                }
                _ => unreachable!(),
            }
        }
        _ => Ok(value),
    }
}

fn mvi_cast_error(target: &str) -> QueryError {
    error(
        crate::error::catalog::ER_INVALID_JSON_VALUE_FOR_FUNC_INDEX,
        format!("Invalid JSON value for CAST to {target} for functional index"),
    )
}

/// Strict integer cast of one multi-valued-index element. Silently
/// substituting `0` here (the previous `.unwrap_or(0)` / `.max(0)`
/// behaviour) would insert wrong keys into the index — a subsequent
/// `MEMBER OF(0)` would match rows that never actually contained 0.
/// MySQL raises ER_INVALID_JSON_VALUE_FOR_FUNC_INDEX (3903) for any
/// element that cannot be losslessly cast; do the same.
fn cast_mvi_int(value: &Value, unsigned: bool) -> Result<Value, QueryError> {
    let target = if unsigned { "UNSIGNED" } else { "SIGNED" };
    // For JSON inputs, inspect the tag to reject arrays / objects / strings
    // that don't parse. `coerce_i64` used to swallow all of these as 0.
    if let Value::Json(bytes) = value {
        let r = crate::engine::json::JsonbRef::new(bytes)?;
        if r.is_number() {
            // number_text gives the canonical decimal (integer literal for
            // I64/U64, canonical decimal text for DECIMAL, shortest
            // round-trip decimal for DOUBLE). Parse it as the requested
            // signedness; fractional / out-of-range parses fail cleanly.
            let Some(txt) = r.number_text() else {
                return Err(mvi_cast_error(target));
            };
            return parse_mvi_int(&txt, unsigned).ok_or_else(|| mvi_integer_error(&txt, target));
        }
        if let Some(b) = r.as_bool() {
            let i = i64::from(b);
            return int_to_mvi(i, unsigned).ok_or_else(|| mvi_cast_error(target));
        }
        if let Some(s) = r.as_str() {
            return parse_mvi_int(s, unsigned).ok_or_else(|| mvi_integer_error(s, target));
        }
        // Arrays / objects / null reach here — no numeric interpretation.
        return Err(mvi_cast_error(target));
    }
    // Non-JSON SQL values (e.g. an expression that already yielded a scalar).
    match value {
        Value::Int(v) => int_to_mvi(*v, unsigned).ok_or_else(|| mvi_cast_error(target)),
        Value::UInt(v) => {
            if unsigned {
                Ok(Value::UInt(*v))
            } else if *v <= i64::MAX as u64 {
                Ok(Value::Int(*v as i64))
            } else {
                Err(mvi_cast_error(target))
            }
        }
        other => parse_mvi_int(&other.to_display_string(), unsigned)
            .ok_or_else(|| mvi_cast_error(target)),
    }
}

fn int_to_mvi(v: i64, unsigned: bool) -> Option<Value> {
    if unsigned {
        if v < 0 {
            None
        } else {
            Some(Value::UInt(v as u64))
        }
    } else {
        Some(Value::Int(v))
    }
}

fn parse_mvi_int(s: &str, unsigned: bool) -> Option<Value> {
    crate::util::decimal::checked_integer_text(s, unsigned)
}

fn mvi_integer_error(text: &str, target: &str) -> QueryError {
    if crate::util::decimal::parse_exact_decimal_big(text).is_some()
        || text.trim().parse::<f64>().is_ok()
    {
        crate::error::catalog::ER_JSON_VALUE_OUT_OF_RANGE_FOR_FUNC_INDEX.error(format!(
            "JSON value is out of range for CAST to {target} for functional index"
        ))
    } else {
        mvi_cast_error(target)
    }
}

fn multi_valued_index_cast_target(expr: &IndexExpression) -> Option<String> {
    let Expr::Function { name, args, .. } = expr.ast.as_ref()? else {
        return None;
    };
    if !name.eq_ignore_ascii_case("CAST") || args.len() < 2 {
        return None;
    }
    match &args[1] {
        Expr::Literal(crate::parser::Literal::String(s)) => {
            Some(cast_target_with_params(s, &args[2..]))
        }
        _ => None,
    }
}

fn cast_target_with_params(base: &str, params: &[Expr]) -> String {
    let values = params
        .iter()
        .filter_map(|param| match param {
            Expr::Literal(crate::parser::Literal::Int(value)) => Some(value.to_string()),
            Expr::Literal(crate::parser::Literal::UInt(value)) => Some(value.to_string()),
            _ => None,
        })
        .collect::<Vec<_>>();
    if values.is_empty() {
        base.to_string()
    } else {
        format!("{base}({})", values.join(","))
    }
}

fn decimal_target_params(target: &str) -> Vec<u8> {
    target
        .split_once('(')
        .and_then(|(_, rest)| rest.strip_suffix(')'))
        .map(|rest| {
            rest.split(',')
                .filter_map(|value| value.trim().parse().ok())
                .collect()
        })
        .unwrap_or_default()
}

fn multi_valued_index_element_text(value: Value) -> String {
    match value {
        Value::Json(bytes) => crate::engine::json::unquote_text(&bytes),
        other => other.to_display_string(),
    }
}

pub(crate) fn compact_char_storage_str(value: &str) -> &str {
    crate::util::pad_space::pad_space_trim_end_str(value)
}

pub(crate) fn normalize_char_string_for_read(value: &mut String, _len: Option<usize>) {
    let compact_len = crate::util::pad_space::trim_trailing_spaces_len(value.as_bytes());
    value.truncate(compact_len);
}

fn is_collated_key_type(data_type: &DataTypeKind) -> bool {
    matches!(
        data_type,
        DataTypeKind::Char { .. }
            | DataTypeKind::VarChar { .. }
            | DataTypeKind::Text { .. }
            | DataTypeKind::Enum { .. }
            | DataTypeKind::Set { .. }
    )
}

/// Number of fractional digits in a decimal string literal. `"12.34"`
/// returns 2, `"100"` returns 0. Trailing whitespace and leading sign
/// are ignored.
fn decimal_string_scale(s: &str) -> u8 {
    match s.trim().split_once('.') {
        Some((_, frac)) => frac.len().min(255) as u8,
        None => 0,
    }
}

/// Format a signed scaled magnitude back into MySQL DECIMAL ASCII form.
/// Mirrors `format_decimal_v3` in the row codec but without the u128
/// upper-bound assumption — i128 covers any legal DECIMAL(38,*) value.
fn format_decimal_i128(value: i128, scale: u8) -> String {
    crate::util::decimal::format_decimal_parts(value, scale)
}

fn default_nullable() -> bool {
    true
}

fn default_charset() -> String {
    crate::charset::DEFAULT_CHARSET.to_string()
}

fn default_collation() -> String {
    crate::charset::DEFAULT_COLLATION.to_string()
}

fn default_engine() -> String {
    "mydb".to_string()
}

fn default_table_type() -> String {
    "BASE TABLE".to_string()
}

fn default_index_type() -> String {
    "BTREE".to_string()
}

fn default_index_visible() -> bool {
    true
}

/// Build the per-key-part [`IndexExpression`] vector for a parser-level
/// index definition by compiling each `Some(sql)` slot against
/// `table`. Returns an empty Vec when no key part is a functional
/// expression — matches the on-disk representation and keeps
/// [`IndexSchema::has_expression_parts`] cheap.
///
/// Steps per slot:
///   1. Parse the canonical SQL text we round-trip through.
///   2. Reject subqueries / aggregates / window functions /
///      nondeterministic builtins (only safe-by-construction
///      expressions can appear in an index key).
///   3. Collect referenced base columns (lowercased) — used by DML to
///      decide whether a row update affects this functional index.
///   4. Compile the expression once against `table`.
///
/// `columns[pos]` must equal the canonical SQL text for expression
/// parts (the parser maintains that invariant); the caller is
/// responsible for passing matching slices.
pub fn compile_index_key_expressions(
    table: &TableSchema,
    columns: &[String],
    parser_key_expressions: &[(Option<String>, bool)],
) -> Result<Vec<Option<IndexExpression>>, QueryError> {
    if parser_key_expressions
        .iter()
        .all(|(expr, _)| expr.is_none())
    {
        return Ok(Vec::new());
    }
    let mut out: Vec<Option<IndexExpression>> = Vec::with_capacity(parser_key_expressions.len());
    let mut multi_valued_count = 0usize;
    for (pos, (slot, multi_valued)) in parser_key_expressions.iter().enumerate() {
        let Some(expr_sql) = slot.as_ref() else {
            out.push(None);
            continue;
        };
        let ast = crate::parser::parse_catalog_expr(expr_sql).map_err(|err| {
            error(
                crate::error::catalog::ER_PARSE_ERROR,
                format!(
                    "cannot parse functional index expression `{expr_sql}`: {}",
                    err.message
                ),
            )
        })?;
        validate_functional_index_expression(&ast, expr_sql)?;
        let referenced_columns = collect_index_expression_columns(&ast, table)?;
        if referenced_columns.is_empty() {
            return Err(error(
                crate::error::catalog::ER_FUNCTIONAL_INDEX_REF_AUTO_INCREMENT,
                format!(
                    "functional index expression `{expr_sql}` does not reference any base column"
                ),
            ));
        }
        debug_assert_eq!(
            columns.get(pos).map(String::as_str),
            Some(expr_sql.as_str())
        );
        if *multi_valued {
            multi_valued_count += 1;
            validate_multi_valued_index_expression(&ast, expr_sql)?;
        }
        let program = crate::engine::expr_program::CompiledTableExpr::compile_once(&ast, table);
        // This descriptor controls persisted keys as well as planner bounds.
        // Numeric precision and scale must match the expression evaluator.
        let data_type = if *multi_valued {
            multi_valued_index_target_type(&ast)
        } else {
            infer_index_expression_type(&ast, table)
        };
        if !*multi_valued
            && (matches!(
                data_type,
                DataTypeKind::Json | DataTypeKind::Geometry { .. }
            ) || matches!(
                crate::engine::expr_type::infer_expr_type_in_table(&ast, table).kind,
                crate::engine::expr_type::ExprTypeKind::Json
                    | crate::engine::expr_type::ExprTypeKind::Geometry { .. }
            ))
        {
            return Err(error(
                crate::error::catalog::ER_FUNCTIONAL_INDEX_ON_JSON_OR_GEOMETRY_FUNCTION,
                "Cannot create a functional index on a function that returns a JSON or GEOMETRY value.",
            ));
        }
        let nullable = infer_index_expression_nullable(&ast, table);
        out.push(Some(IndexExpression {
            expr_sql: expr_sql.clone(),
            data_type,
            nullable,
            referenced_columns,
            multi_valued: *multi_valued,
            collation_id: crate::engine::expr::expr_collation(&ast, table).map(|c| c.collation_id),
            ast: Some(ast),
            program: Some(program),
        }));
    }
    if multi_valued_count > 1 {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "only one multi-valued index key part is supported",
        ));
    }
    Ok(out)
}

fn validate_multi_valued_index_expression(expr: &Expr, source: &str) -> Result<(), QueryError> {
    let Expr::Function { name, args, .. } = expr else {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            format!("multi-valued index expression `{source}` must be CAST(... AS ... ARRAY)"),
        ));
    };
    if !name.eq_ignore_ascii_case("CAST") || args.len() < 2 {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            format!("multi-valued index expression `{source}` must be CAST(... AS ... ARRAY)"),
        ));
    }
    let target = match args.get(1) {
        Some(Expr::Literal(crate::parser::Literal::String(s))) => s.to_ascii_uppercase(),
        _ => String::new(),
    };
    if !matches!(
        target.as_str(),
        "SIGNED" | "UNSIGNED" | "CHAR" | "DATE" | "DATETIME" | "TIME" | "DECIMAL"
    ) {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            format!("unsupported multi-valued index cast target `{target}`"),
        ));
    }
    Ok(())
}

fn multi_valued_index_target_type(expr: &Expr) -> DataTypeKind {
    let Expr::Function { args, .. } = expr else {
        return DataTypeKind::VarChar { len: None };
    };
    let target = match args.get(1) {
        Some(Expr::Literal(crate::parser::Literal::String(s))) => s.to_ascii_uppercase(),
        _ => String::new(),
    };
    match target.as_str() {
        "SIGNED" => DataTypeKind::BigInt,
        "UNSIGNED" => DataTypeKind::BigInt,
        "CHAR" => DataTypeKind::VarChar { len: None },
        "DATE" => DataTypeKind::Date,
        "DATETIME" => DataTypeKind::DateTime { fsp: 0 },
        "TIME" => DataTypeKind::Time { fsp: 0 },
        "DECIMAL" => {
            let params = decimal_target_params(&cast_target_with_params(&target, &args[2..]));
            DataTypeKind::Decimal {
                precision: Some(params.first().copied().unwrap_or(65)),
                scale: Some(params.get(1).copied().unwrap_or(0)),
            }
        }
        _ => DataTypeKind::VarChar { len: None },
    }
}

fn validate_functional_index_expression(expr: &Expr, source: &str) -> Result<(), QueryError> {
    crate::parser::visit::try_for_each_expr(expr, |expr| match expr {
        Expr::Subquery(_)
        | Expr::Exists { .. }
        | Expr::InSubquery { .. }
        | Expr::QuantifiedComparison { .. } => Err(error(
            crate::error::catalog::ER_FUNCTIONAL_INDEX_REF_AUTO_INCREMENT,
            format!("functional index expression `{source}` cannot contain a subquery"),
        )),
        Expr::WindowFunction { .. } => Err(error(
            crate::error::catalog::ER_FUNCTIONAL_INDEX_REF_AUTO_INCREMENT,
            format!("functional index expression `{source}` cannot contain a window function"),
        )),
        Expr::Placeholder(_)
        | Expr::UserVariable(_)
        | Expr::SystemVariable { .. }
        | Expr::Default(_)
        | Expr::DefaultQualified(_) => Err(error(
            crate::error::catalog::ER_FUNCTIONAL_INDEX_REF_AUTO_INCREMENT,
            format!("functional index expression `{source}` is not deterministic"),
        )),
        Expr::Grouping(_) => Err(error(
            crate::error::catalog::ER_FUNCTIONAL_INDEX_REF_AUTO_INCREMENT,
            format!("functional index expression `{source}` cannot use GROUPING()"),
        )),
        Expr::Function { name, .. } => {
            if is_nondeterministic_function(name) {
                return Err(error(crate::error::catalog::ER_FUNCTIONAL_INDEX_REF_AUTO_INCREMENT,
                        format!(
                            "functional index expression `{source}` uses nondeterministic function {name}()"
                        ),
                    ));
            }
            if is_aggregate_function_name(name) {
                return Err(error(
                    crate::error::catalog::ER_FUNCTIONAL_INDEX_REF_AUTO_INCREMENT,
                    format!("functional index expression `{source}` cannot use aggregate {name}()"),
                ));
            }
            Ok(true)
        }
        _ => Ok(true),
    })
}

pub(crate) fn is_nondeterministic_function(name: &str) -> bool {
    if crate::engine::http::is_function(name) {
        return true;
    }
    matches!(
        name.to_ascii_uppercase().as_str(),
        "NOW"
            | "CURRENT_TIMESTAMP"
            | "LOCALTIME"
            | "LOCALTIMESTAMP"
            | "CURDATE"
            | "CURRENT_DATE"
            | "CURTIME"
            | "CURRENT_TIME"
            | "UNIX_TIMESTAMP"
            | "SYSDATE"
            | "RAND"
            | "UUID"
            | "UUID_SHORT"
            | "USER"
            | "CURRENT_USER"
            | "SESSION_USER"
            | "DATABASE"
            | "SCHEMA"
            | "VERSION"
            | "CONNECTION_ID"
            | "LAST_INSERT_ID"
            | "SLEEP"
            | "LOAD_FILE"
    )
}

/// Recursively checks whether an expression calls a named function. Kept in
/// the schema layer so every persistent-expression validator can reject
/// side-effecting/volatile functions without evaluating them.
pub(crate) fn expr_contains_function_named(expr: &Expr, target: &str) -> bool {
    crate::parser::visit::try_for_each_expr(expr, |expr| {
        match expr {
            Expr::Function { name, .. } | Expr::WindowFunction { name, .. }
                if name.eq_ignore_ascii_case(target) =>
            {
                Err(())
            }
            // Schema validators reject these independently; retain the scope boundary.
            Expr::Subquery(_)
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::QuantifiedComparison { .. }
            | Expr::Grouping(_) => Ok(false),
            _ => Ok(true),
        }
    })
    .is_err()
}

fn is_aggregate_function_name(name: &str) -> bool {
    matches!(
        name.to_ascii_uppercase().as_str(),
        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "GROUP_CONCAT"
    )
}

/// Best-effort static type inference for a functional index
/// expression. Drives `IndexExpression.data_type`, which the cost
/// model uses for index_key_len / row width estimates. Returns
/// `VarChar { len: None }` for anything we can't classify; that's a
/// safe key-encoding placeholder.
fn infer_index_expression_type(expr: &Expr, table: &TableSchema) -> DataTypeKind {
    fn fallback() -> DataTypeKind {
        DataTypeKind::VarChar { len: None }
    }
    match expr {
        Expr::Literal(crate::parser::Literal::Int(_))
        | Expr::Literal(crate::parser::Literal::UInt(_))
        | Expr::Literal(crate::parser::Literal::Bool(_)) => DataTypeKind::BigInt,
        Expr::Literal(crate::parser::Literal::Float(_)) => DataTypeKind::Double {
            precision: None,
            scale: None,
        },
        Expr::Literal(crate::parser::Literal::String(_)) => fallback(),
        Expr::Literal(crate::parser::Literal::Null) => fallback(),
        Expr::Identifier(name) => match table.column_index(name) {
            Ok(idx) => table.columns[idx].data_type.clone(),
            Err(_) => fallback(),
        },
        Expr::CompoundIdentifier(parts) => match parts.last() {
            Some(last) => match table.column_index(last) {
                Ok(idx) => table.columns[idx].data_type.clone(),
                Err(_) => fallback(),
            },
            None => fallback(),
        },
        Expr::Unary {
            op: crate::parser::UnaryOp::Neg,
            expr,
        } => infer_index_expression_type(expr, table),
        Expr::Binary { left, op, right } => match op {
            crate::parser::BinaryOp::Add
            | crate::parser::BinaryOp::Sub
            | crate::parser::BinaryOp::Mul
            | crate::parser::BinaryOp::Mod => {
                // Arithmetic over DECIMAL must keep its fractional key;
                // character operands enter the approximate numeric domain.
                let lt = infer_index_expression_type(left, table);
                let rt = infer_index_expression_type(right, table);
                if !lt.is_integer() && !matches!(lt, DataTypeKind::Decimal { .. })
                    || !rt.is_integer() && !matches!(rt, DataTypeKind::Decimal { .. })
                {
                    DataTypeKind::Double {
                        precision: None,
                        scale: None,
                    }
                } else if matches!(lt, DataTypeKind::Decimal { .. })
                    || matches!(rt, DataTypeKind::Decimal { .. })
                {
                    let column_type =
                        crate::engine::expr_type::column_type::schema_expr_result_type(
                            expr,
                            &mut |operand| {
                                infer_index_expression_type(operand, table).column_type()
                            },
                        );
                    data_type_kind_for_column_type(
                        column_type,
                        &crate::types::ColumnSource::default(),
                    )
                } else {
                    DataTypeKind::BigInt
                }
            }
            crate::parser::BinaryOp::Div => DataTypeKind::Double {
                precision: None,
                scale: None,
            },
            crate::parser::BinaryOp::IntDiv => DataTypeKind::BigInt,
            // M2/C5 (fixes P2-10): `->` returns a JSON fragment, `->>`
            // returns TEXT (JSON_UNQUOTE result). Both were previously
            // hitting the generic BigInt fallback, so a functional index
            // over `j->'$.a'` was type-inferred as BigInt and CBO
            // width/selectivity estimates were badly wrong.
            crate::parser::BinaryOp::Arrow => DataTypeKind::Json,
            crate::parser::BinaryOp::LongArrow => fallback(),
            _ => DataTypeKind::BigInt,
        },
        Expr::Function { .. } => {
            let column_type = crate::engine::expr_type::column_type::schema_expr_result_type(
                expr,
                &mut |column| infer_index_expression_type(column, table).column_type(),
            );
            if column_type == ColumnType::VarString {
                fallback()
            } else {
                data_type_kind_for_column_type(column_type, &crate::types::ColumnSource::default())
            }
        }
        Expr::Case { .. } | Expr::Tuple(_) => fallback(),
        _ => fallback(),
    }
}

/// Best-effort static NULL analysis. Returns false only when we can
/// prove the expression never produces NULL on this table's data:
/// every column it reads must be NOT NULL, no nullable literals, and
/// every function in the call tree must be in the NULL-preserving
/// allow-list (function returns NULL iff some input is NULL, and we
/// already know none of the inputs are). Otherwise returns true.
fn infer_index_expression_nullable(expr: &Expr, table: &TableSchema) -> bool {
    fn nullable(expr: &Expr, table: &TableSchema) -> bool {
        match expr {
            Expr::Literal(crate::parser::Literal::Null) => true,
            Expr::Literal(_) | Expr::BoundValue(_) => false,
            Expr::Identifier(name) => table
                .column_index(name)
                .map(|idx| table.columns[idx].nullable)
                .unwrap_or(true),
            Expr::CompoundIdentifier(parts) => parts
                .last()
                .and_then(|last| table.column_index(last).ok())
                .map(|idx| table.columns[idx].nullable)
                .unwrap_or(true),
            Expr::Unary {
                op: crate::parser::UnaryOp::Neg,
                expr,
            } => nullable(expr, table),
            Expr::Binary { left, op, right } => match op {
                crate::parser::BinaryOp::Add
                | crate::parser::BinaryOp::Sub
                | crate::parser::BinaryOp::Mul
                | crate::parser::BinaryOp::Mod => nullable(left, table) || nullable(right, table),
                // Division by zero yields NULL in MySQL — always treat
                // as nullable conservatively.
                crate::parser::BinaryOp::Div | crate::parser::BinaryOp::IntDiv => true,
                _ => true,
            },
            Expr::Function { name, args, .. } => {
                if is_null_preserving_function(name) {
                    args.iter().any(|a| nullable(a, table))
                } else {
                    // Functions that may inject NULL on their own
                    // (NULLIF, IFNULL semantics, CAST overflow, …) —
                    // be conservative.
                    true
                }
            }
            // CASE / IFNULL / COALESCE: rely on conservative fallback.
            _ => true,
        }
    }
    nullable(expr, table)
}

/// Functions whose result is NULL iff at least one argument is NULL.
/// (Used by `infer_index_expression_nullable` to prove a functional
/// index expression is NOT NULL when all its base columns are NOT
/// NULL.) Strict allow-list — additions need a careful review.
fn is_null_preserving_function(name: &str) -> bool {
    matches!(
        name.to_ascii_uppercase().as_str(),
        // String: pure transforms
        "LOWER" | "UPPER" | "LCASE" | "UCASE" | "TRIM" | "LTRIM" | "RTRIM"
            | "SUBSTRING" | "SUBSTR" | "LEFT" | "RIGHT" | "REVERSE"
            | "LPAD" | "RPAD" | "REPEAT" | "SPACE" | "QUOTE" | "REPLACE"
            | "CONCAT" | "CONCAT_WS"
            | "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH"
            | "OCTET_LENGTH" | "BIT_LENGTH"
            | "LOCATE" | "INSTR" | "POSITION" | "ASCII" | "ORD"
            | "HEX" | "UNHEX" | "BIN" | "OCT" | "CONV"
            // Numeric
            | "ABS" | "ROUND" | "CEIL" | "CEILING" | "FLOOR" | "TRUNCATE"
            | "SIGN" | "POW" | "POWER" | "SQRT" | "EXP" | "LN"
            | "LOG" | "LOG2" | "LOG10" | "PI"
            | "DEGREES" | "RADIANS"
            | "SIN" | "COS" | "TAN" | "ASIN" | "ACOS" | "ATAN" | "ATAN2" | "COT"
            // Date/time
            | "YEAR" | "MONTH" | "DAY" | "DAYOFMONTH" | "DAYOFWEEK"
            | "DAYOFYEAR" | "WEEKDAY" | "WEEK" | "WEEKOFYEAR" | "QUARTER"
            | "HOUR" | "MINUTE" | "SECOND" | "DATEDIFF" | "TIMESTAMPDIFF"
            | "DATE_FORMAT" | "DATE" | "TIME" | "TO_DAYS" | "TIME_TO_SEC"
            | "EXTRACT" | "FROM_UNIXTIME" | "MAKEDATE" | "MAKETIME"
            | "DAYNAME" | "MONTHNAME"
    )
}

fn collect_index_expression_columns(
    expr: &Expr,
    table: &TableSchema,
) -> Result<Vec<String>, QueryError> {
    let mut seen = std::collections::BTreeSet::new();
    crate::parser::visit::try_for_each_expr(expr, |expr| {
        let name = match expr {
            Expr::Identifier(name) => Some(name),
            Expr::CompoundIdentifier(parts) => parts.last(),
            // These nodes are rejected by functional-index validation.
            Expr::Subquery(_)
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::QuantifiedComparison { .. }
            | Expr::WindowFunction { .. }
            | Expr::Grouping(_)
            | Expr::GroupKeyRef { .. } => return Ok(false),
            _ => None,
        };
        if let Some(name) = name {
            let idx = table.column_index(name)?;
            seen.insert(table.columns[idx].name.to_ascii_lowercase());
        }
        Ok(true)
    })?;
    Ok(seen.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_comment_configuration_is_rejected() {
        use crate::storage::vector_config::VectorIndexConfig;
        for c in [
            "M=16",
            "NLIST = 32",
            "DISTANCE=COSINE",
            "EFSEARCH=64",
            "QUANTIZATION=sq8",
        ] {
            assert!(VectorIndexConfig::validate_comment(c)
                .unwrap_err()
                .message()
                .contains("WITH"));
        }
        assert!(VectorIndexConfig::validate_comment("Embedding index; owner=search").is_ok());
    }

    fn legacy_table_json() -> &'static str {
        // Catalog persisted before stable column identity was introduced:
        // ColumnSchema entries carry no column_id / state / physical_pos /
        // logical_pos, and TableSchema has no next_column_id. Deserializing
        // this must succeed via #[serde(default)], and build_column_index
        // must populate the missing fields on first call.
        r#"{
            "id": 42,
            "database": "test",
            "name": "legacy_t",
            "columns": [
                {"name": "a", "data_type": "Int"},
                {"name": "b", "data_type": "BigInt"},
                {"name": "c", "data_type": {"Text": {"max_len": 65535}}}
            ],
            "primary_key": ["a"],
            "indexes": [],
            "stats": {
                "row_count": 0,
                "columns": {},
                "indexes": {},
                "modified_rows_since_analyze": 0,
                "analyzed_at_unix_ms": null
            }
        }"#
    }

    #[test]
    fn build_column_index_migrates_legacy_columns() {
        let mut table: TableSchema = serde_json::from_str(legacy_table_json()).unwrap();
        // Legacy entries deserialize with zero identity fields.
        for col in &table.columns {
            assert_eq!(col.column_id, 0);
            assert_eq!(col.physical_pos, 0);
            assert_eq!(col.logical_pos, 0);
            assert_eq!(col.state, ColumnState::Public);
        }
        assert_eq!(table.next_column_id, 0);

        table.build_column_index();

        // Migration assigns sequential ids starting at 1 and positions in
        // declaration order.
        assert_eq!(table.columns[0].column_id, 1);
        assert_eq!(table.columns[1].column_id, 2);
        assert_eq!(table.columns[2].column_id, 3);
        assert_eq!(table.columns[0].physical_pos, 0);
        assert_eq!(table.columns[1].physical_pos, 1);
        assert_eq!(table.columns[2].physical_pos, 2);
        assert_eq!(table.columns[0].logical_pos, 0);
        assert_eq!(table.columns[1].logical_pos, 1);
        assert_eq!(table.columns[2].logical_pos, 2);
        assert_eq!(table.next_column_id, 4);

        // Lookup maps populated.
        assert_eq!(table.column_name_map.get("a"), Some(&0));
        assert_eq!(table.column_name_map.get("c"), Some(&2));
        assert_eq!(table.column_id_map.get(&1), Some(&0));
        assert_eq!(table.column_id_map.get(&3), Some(&2));
    }

    #[test]
    fn build_column_index_is_idempotent() {
        let mut table: TableSchema = serde_json::from_str(legacy_table_json()).unwrap();
        table.build_column_index();
        let snapshot: Vec<(u32, u32, u32)> = table
            .columns
            .iter()
            .map(|c| (c.column_id, c.physical_pos, c.logical_pos))
            .collect();
        let next_id = table.next_column_id;

        // Second call must observe the populated state and leave it
        // alone; the migration branch keys off `column_id == 0`.
        table.build_column_index();

        let after: Vec<(u32, u32, u32)> = table
            .columns
            .iter()
            .map(|c| (c.column_id, c.physical_pos, c.logical_pos))
            .collect();
        assert_eq!(snapshot, after);
        assert_eq!(table.next_column_id, next_id);
    }

    #[test]
    fn allocate_column_id_does_not_collide_with_existing_max() {
        // Simulate a catalog that has already migrated and then had some
        // columns dropped — leaving a higher max(column_id) than columns.len().
        let mut table = TableSchema {
            id: 1,
            object_id: 1,
            active_generation: 1,
            database: "test".into(),
            name: "t".into(),
            engine: "mydb".into(),
            charset: "utf8mb4".into(),
            collation: crate::charset::DEFAULT_COLLATION.into(),
            comment: String::new(),
            created_ms: 0,
            updated_ms: 0,
            table_type: "BASE TABLE".into(),
            columns: vec![ColumnSchema {
                not_null_from_scn: 0,
                name: "a".into(),
                data_type: DataTypeKind::Int,
                nullable: true,
                unsigned: false,
                invisible: false,
                zerofill: false,
                auto_increment: false,
                default: None,
                read_backfill_default: None,
                default_expr: None,
                on_update_expr: None,
                column_id: 7,
                state: ColumnState::Public,
                physical_pos: 6,
                logical_pos: 0,
                ordinal_position: 1,
                charset: None,
                collation: None,
                comment: String::new(),
                generated_expr: None,
                generated_stored: false,
                generated_column_names: Vec::new(),
                generated_expr_ast: None,
                generated_program: None,
            }],
            primary_key: Vec::new(),
            indexes: Vec::new(),
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            fk_referenced_by: Vec::new(),
            stats: crate::storage::TableStats::empty(),
            system: false,
            schema_version: 0,
            system_time_epoch_us: 0,
            row_generation_txn_id: 0,
            next_column_id: 0,
            active_layout_id: 1,
            row_layouts: Vec::new(),
            pending_primary_key: None,
            primary_definition_scn: 0,
            next_index_id: 0,
            primary_publication_job_id: 0,
            column_name_map: Default::default(),
            column_id_map: HashMap::new(),
            row_codec: Default::default(),
            partitioning: None,
            pctfree: 0,
            pctused: 0,
            mysql_table_options: Vec::new(),
        };
        table.build_column_index();
        // Migration must seed next_column_id beyond the existing max id,
        // not reset to 1.
        assert!(table.next_column_id > 7);
        let new_id = table.allocate_column_id();
        assert_eq!(new_id, 8);
    }

    // -- KeyPart / index encoding correctness ---------------------------

    #[test]
    fn keypart_float_sorts_by_numeric_order_even_for_negatives() {
        // The pre-v6 KeyPart encoded Float / Double as `format!("{:020.8}")`
        // ASCII strings; for any negative value the leading `-` byte made
        // `"-0000001.50000000"` sort *after* `"-0000000.50000000"` so an
        // index on a column with negative values silently returned rows
        // in the wrong order. The new KeyPart::Float64 stores
        // order-preserving u64 bits; the derived Ord now reflects real
        // numeric order across the whole range.
        let values = [-1.5_f64, -0.5_f64, 0.0_f64, 0.5_f64, 1.5_f64];
        let mut parts: Vec<KeyPart> = values
            .iter()
            .map(|v| KeyPart::from_value(&Value::Double(*v)))
            .collect();
        parts.sort();
        let decoded: Vec<f64> = parts
            .iter()
            .map(|p| match p {
                KeyPart::Float64(bits) => crate::storage::core::f64_from_order_u64(*bits),
                other => panic!("unexpected variant {:?}", other),
            })
            .collect();
        assert_eq!(decoded, values);
    }

    #[test]
    fn keypart_decimal_round_trip_for_compact_and_string() {
        // DecimalCompact comes off the heap codec; large-magnitude values
        // arrive as Value::Decimal(String). Both must produce a
        // KeyPart::Decimal that round-trips back to an equivalent Value
        // — pre-v6 they both went to KeyPart::String, which lost the
        // distinction between same-scale decimals stored at different
        // widths (e.g. "10.00" vs "2.00" sorted lexicographically wrong).
        let compact = KeyPart::from_value(&Value::DecimalCompact(-12_345, 2));
        assert!(matches!(
            compact,
            KeyPart::Decimal {
                value: -12345,
                scale: 2,
            }
        ));
        let back = compact.to_value(&DataTypeKind::Decimal {
            precision: Some(10),
            scale: Some(2),
        });
        assert_eq!(back, Value::DecimalCompact(-12_345, 2));

        // Large-magnitude path: i128 covers any legal DECIMAL(38,*).
        let huge = "12345678901234567890.12";
        let big = KeyPart::from_value(&Value::Decimal(huge.to_string()));
        let KeyPart::Decimal { value, scale } = big else {
            panic!("expected KeyPart::Decimal");
        };
        assert_eq!(scale, 2);
        assert_eq!(value, 1_234_567_890_123_456_789_012i128);
    }

    #[test]
    fn keypart_temporal_variants_are_binary_not_ascii() {
        // Date / Time / DateTime / Year used to detour through
        // KeyPart::String — observing the canonical ASCII form. v6
        // stores them in their native i32/i64/u8 form so an index entry
        // on a DATE column compares without parsing.
        assert!(matches!(
            KeyPart::from_value(&Value::Date(19_500)),
            KeyPart::Date(19_500)
        ));
        assert!(matches!(
            KeyPart::from_value(&Value::Time(-1_500_000)),
            KeyPart::Time(-1_500_000)
        ));
        assert!(matches!(
            KeyPart::from_value(&Value::DateTime(1_700_000_000_000_000)),
            KeyPart::DateTime(1_700_000_000_000_000)
        ));
        assert!(matches!(
            KeyPart::from_value(&Value::Year(123)),
            KeyPart::Year(123)
        ));
    }

    #[test]
    fn index_key_uniform_integer_fast_cmp_matches_lexicographic_order() {
        let a = IndexKey::from(vec![KeyPart::Int(1), KeyPart::Int(2), KeyPart::Int(3)]);
        let b = IndexKey::from(vec![KeyPart::Int(1), KeyPart::Int(2), KeyPart::Int(4)]);
        let c = IndexKey::from(vec![KeyPart::Int(1), KeyPart::Int(3), KeyPart::Int(0)]);
        assert!(a < b);
        assert!(b < c);
        assert_eq!(
            a,
            IndexKey::from(vec![KeyPart::Int(1), KeyPart::Int(2), KeyPart::Int(3)])
        );

        let ua = IndexKey::from(vec![KeyPart::UInt(10), KeyPart::UInt(20)]);
        let ub = IndexKey::from(vec![KeyPart::UInt(10), KeyPart::UInt(21)]);
        assert!(ua < ub);
    }

    #[test]
    fn index_key_fast_cmp_falls_back_for_prefix_and_mixed_shapes() {
        let prefix = IndexKey::from(vec![KeyPart::Int(1), KeyPart::Int(2)]);
        let longer = IndexKey::from(vec![KeyPart::Int(1), KeyPart::Int(2), KeyPart::Int(0)]);
        assert!(prefix < longer);

        let mixed_left = IndexKey::from(vec![KeyPart::Int(1), KeyPart::String("a".into())]);
        let mixed_right = IndexKey::from(vec![KeyPart::Int(1), KeyPart::String("b".into())]);
        assert!(mixed_left < mixed_right);

        let rank_left = IndexKey::from(vec![KeyPart::Int(1)]);
        let rank_right = IndexKey::from(vec![KeyPart::UInt(1)]);
        assert!(rank_left < rank_right);
    }

    #[test]
    fn for_target_date_parses_ascii_literal() {
        // A literal `DATE '1998-09-02'` constant-folds to
        // Value::String("1998-09-02") inside the planner. for_target must
        // parse it into KeyPart::Date(days) so it compares against the
        // stored entries — pre-fix this routed through KeyPart::from(v)
        // and produced a KeyPart::String, which the derived Ord ranked
        // as `> KeyPart::Date(_)` (variant order), yielding zero rows
        // from a `<= literal` range scan.
        let kp = KeyPart::for_target(Value::String("1998-09-02".to_string()), ParamTarget::Date)
            .unwrap();
        // 1998-09-02 = 28_369 days after 1970-01-01.
        assert_eq!(kp, KeyPart::Date(10_471));
    }

    #[test]
    fn paramtarget_for_column_routes_each_data_type_to_its_keypart() {
        // ParamTarget::for_column drives both planner constraints and
        // prepared-statement parameter resolution; the variant it
        // chooses must match the variant the index stored, otherwise
        // cross-variant compares silently miss rows.
        assert_eq!(
            ParamTarget::for_column(&DataTypeKind::Date, false),
            ParamTarget::Date
        );
        assert_eq!(
            ParamTarget::for_column(&DataTypeKind::Year, false),
            ParamTarget::Year
        );
        assert_eq!(
            ParamTarget::for_column(
                &DataTypeKind::Float {
                    precision: None,
                    scale: None,
                },
                false,
            ),
            ParamTarget::Float32
        );
        assert_eq!(
            ParamTarget::for_column(
                &DataTypeKind::Double {
                    precision: None,
                    scale: None,
                },
                false,
            ),
            ParamTarget::Float64
        );
        assert_eq!(
            ParamTarget::for_column(
                &DataTypeKind::Decimal {
                    precision: Some(10),
                    scale: Some(2),
                },
                false,
            ),
            ParamTarget::Decimal {
                precision: 10,
                scale: 2,
            }
        );
        assert_eq!(
            ParamTarget::for_column(&DataTypeKind::Char { len: Some(25) }, false),
            ParamTarget::CharString
        );
    }

    fn table_with_stats(row_count: u64, modified: u64) -> TableSchema {
        let json = format!(
            r#"{{
                "id": 7,
                "database": "test",
                "name": "aa_t",
                "columns": [{{"name": "a", "data_type": "Int"}}],
                "primary_key": ["a"],
                "indexes": [],
                "stats": {{
                    "row_count": {row_count},
                    "columns": {{}},
                    "indexes": {{}},
                    "modified_rows_since_analyze": {modified},
                    "analyzed_at_unix_ms": 1
                }}
            }}"#
        );
        serde_json::from_str(&json).unwrap()
    }

    #[test]
    fn stats_needs_auto_analyze_small_table_any_change() {
        // < 64 rows: any pending modification is stale (cheap full scan).
        let t = table_with_stats(10, 1);
        assert!(t.stats_needs_auto_analyze(0, 0.1));
        // No pending modification (delta or persisted) -> never stale.
        let t = table_with_stats(10, 0);
        assert!(!t.stats_needs_auto_analyze(0, 0.1));
    }

    #[test]
    fn stats_needs_auto_analyze_zero_modified_never_stale() {
        let t = table_with_stats(1_000_000, 0);
        assert!(!t.stats_needs_auto_analyze(0, 0.1));
    }

    #[test]
    fn stats_needs_auto_analyze_large_table_ratio_threshold() {
        // 1000 rows, ratio 0.1 -> threshold = ceil(100) = 100.
        let t = table_with_stats(1000, 0);
        // Just below threshold: not stale.
        assert!(!t.stats_needs_auto_analyze(99, 0.1));
        // At threshold: stale.
        assert!(t.stats_needs_auto_analyze(100, 0.1));
        // Above threshold: stale.
        assert!(t.stats_needs_auto_analyze(250, 0.1));
    }

    #[test]
    fn stats_needs_auto_analyze_folds_persisted_and_delta() {
        // 1000 rows, ratio 0.1 -> threshold 100. Persisted 60 + delta 40
        // = 100 reaches the threshold.
        let t = table_with_stats(1000, 60);
        assert!(!t.stats_needs_auto_analyze(39, 0.1));
        assert!(t.stats_needs_auto_analyze(40, 0.1));
    }
}
