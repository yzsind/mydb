// Foreign-key support: DDL validation (CREATE / ALTER ADD / ALTER DROP),
// reverse-dependency guards (DROP TABLE / COLUMN / INDEX, TRUNCATE), and
// DML enforcement (child-side existence checks + parent-side cascade /
// restrict / set-null). All of this is included into the `core` module
// (see `core.rs`), so it shares that module's imports.
//
// Enforcement is gated so tables with no foreign-key relationship pay
// nothing on the DML hot path: the child check short-circuits on
// `table.foreign_keys.is_empty()` and the parent check on
// `table.fk_referenced_by.is_empty()` — both plain `Vec` length reads on
// an `Arc<TableSchema>` field the caller already holds.

use std::cell::Cell;

use crate::parser::{ForeignKeyDef, ReferentialActionDef};

thread_local! {
    /// Recursion depth of the current cascade chain. Retained as a
    /// secondary safety net for runaway recursion; the primary
    /// short-circuit is now the [`FK_CASCADE_VISITED`] set, which detects
    /// real cycles and lets legitimate deep chains run to completion.
    static FK_CASCADE_DEPTH: Cell<u32> = const { Cell::new(0) };
    /// M5.C5 — per-statement visited `(table_id, row_id)` set used by FK
    /// cascade to detect cycles precisely. Without this, a CASCADE that
    /// loops back into a previously-touched row (A→B→A) would either
    /// retry forever or bail with `ER_FK_DEPTH_EXCEEDED`.
    /// We instead record each row visited; a second visit to the same
    /// (table, row) short-circuits with no further work. The set is
    /// cleared on the outermost cascade exit so it never leaks into the
    /// next statement.
    static FK_CASCADE_VISITED: RefCell<HashSet<(u64, RowId, u64)>> =
        RefCell::new(HashSet::new());
    /// Session `foreign_key_checks`. Set per statement by the executor
    /// from `SessionContext::foreign_key_checks`; defaults to enabled so
    /// internal/storage-level callers enforce by default.
    static FK_CHECKS_ENABLED: Cell<bool> = const { Cell::new(true) };
}

/// Reserve a `(table, row)` pair against the per-statement visited set.
/// Returns `true` on first visit (caller proceeds), `false` if already
/// visited (caller skips). Always called inside [`Database::fk_with_depth`]
/// so we observe the same outermost-exit cleanup that depth tracking does.
pub(crate) fn fk_mark_visited(table_id: u64, row_id: RowId, fk_disc: u64) -> bool {
    FK_CASCADE_VISITED.with(|set| set.borrow_mut().insert((table_id, row_id, fk_disc)))
}

/// Stable per-statement discriminator for one inbound FK edge, so a child
/// row reachable through two *different* FKs (e.g. a diamond where the same
/// row is both CASCADE-updated by one FK and SET NULL'd by another) has each
/// action applied instead of the second being swallowed by the visited set.
/// A true single-edge cycle (A→B→A via the same FK) still short-circuits
/// because the `(table, row, disc)` triple repeats. Hashes the FK name once
/// per inbound edge (outside the row loop), never per row.
fn fk_visited_disc(fk_name: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    fk_name.hash(&mut h);
    h.finish()
}

/// Test-only helper: peek at the size of the visited set. The outermost
/// cascade exit clears it back to empty, so it should normally be zero
/// outside of an active statement.
#[cfg(test)]
pub(crate) fn fk_visited_len() -> usize {
    FK_CASCADE_VISITED.with(|set| set.borrow().len())
}

/// Whether FK enforcement is active on this thread (mirrors the session's
/// `foreign_key_checks`).
pub(crate) fn fk_checks_enabled() -> bool {
    FK_CHECKS_ENABLED.with(|c| c.get())
}

/// Set the thread-local `foreign_key_checks` flag; returns the previous
/// value so the caller can restore it (RAII at the statement boundary).
pub(crate) fn set_fk_checks_enabled(enabled: bool) -> bool {
    FK_CHECKS_ENABLED.with(|c| c.replace(enabled))
}

/// RAII guard that installs the statement's `foreign_key_checks` on the
/// thread-local and restores the previous value on drop. Tokio worker
/// threads are pooled and reused across sessions, so a statement that ran
/// with `foreign_key_checks=0` and returned without restoring would leave
/// the flag OFF for whatever internal/background DML the same worker later
/// runs (which expects the enabled default). Binding this guard at every
/// statement-dispatch entry point closes that leak.
pub(crate) struct FkChecksGuard(bool);

impl FkChecksGuard {
    pub(crate) fn new(enabled: bool) -> Self {
        FkChecksGuard(set_fk_checks_enabled(enabled))
    }
}

impl Drop for FkChecksGuard {
    fn drop(&mut self) {
        set_fk_checks_enabled(self.0);
    }
}

// ---------------------------------------------------------------------------
// FK reference locks (fine-grained parent-existence coordination)
//
// The child-side existence check must serialize against a concurrent DELETE
// (or referenced-key UPDATE) of the parent row, otherwise it could insert a
// child against a parent that is about to vanish (orphan). The obvious way —
// take a SHARED lock on the parent *data row* — is correct but over-broad: it
// also conflicts with a plain non-key UPDATE of the parent (e.g. TPC-C's
// `UPDATE district SET d_ytd = d_ytd + ?`), which cannot orphan anything. Under
// concurrent cross-warehouse PAYMENTs that false S-vs-X conflict forms genuine
// deadlock cycles (see `examples/payment_deadlock_repro.rs`).
//
// Instead we coordinate on a dedicated lock keyed by the *referenced key value*,
// living in a reserved table-id namespace so it never collides with real data
// row locks:
//   * child INSERT / FK-column UPDATE  -> SHARED  ref-lock on the referenced key
//   * parent DELETE / referenced-key UPDATE -> EXCLUSIVE ref-lock on that key
//   * plain non-key parent UPDATE       -> nothing (only its own data row lock)
// The two sides derive the identical key, so a delete still blocks against an
// in-flight child check; a plain `d_ytd` update no longer does. Orphan
// prevention is preserved; the false conflict is gone.
//
// Implemented on top of the existing row-lock machinery (blocking acquire,
// deadlock detection, release, savepoint, sharded/legacy, XA) by encoding the
// referenced key as a synthetic `(table_id, RowId)`. Hash collisions are
// correctness-safe — at worst two distinct keys share a lock and serialize
// unnecessarily; they never allow an orphan.

/// Reserved high bit marking a synthetic FK-reference lock table id. Distinct
/// from `TEMP_TABLE_ID_BASE` (`1 << 63`); real parent table ids are small.
const FK_REF_LOCK_TABLE_BASE: u64 = 1 << 62;

/// Synthetic table id for the reference-lock namespace of `parent_table_id`.
fn fk_ref_lock_table_id(parent_table_id: u64) -> u64 {
    FK_REF_LOCK_TABLE_BASE | parent_table_id
}

/// Map a referenced index key to a synthetic `RowId` in the reference-lock
/// namespace. `DefaultHasher` uses fixed keys, so the mapping is stable within
/// and across processes — both the child and parent sides land on the same id.
fn fk_ref_lock_row_id(index_name: &str, key: &IndexKey) -> RowId {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    index_name.to_ascii_lowercase().hash(&mut hasher);
    key.hash(&mut hasher);
    let h = hasher.finish();
    RowId::new((h >> 16) as u32, (h & 0xFFFF) as u16)
}

/// Whether fine-grained FK reference locks are enabled (default on).
/// `MYDB_FK_REFERENCE_LOCK=0|off|false|no` restores the legacy parent-data-row
/// shared lock — kept as a differential oracle.
pub(crate) fn fk_reference_lock_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| match std::env::var("MYDB_FK_REFERENCE_LOCK") {
        Ok(v) => {
            let t = v.trim().to_ascii_lowercase();
            !(t == "0" || t == "off" || t == "false" || t == "no")
        }
        Err(_) => true,
    })
}

/// Sequential child/parent index merge for ADD FK validation (default on).
/// The legacy projected-scan + batched-probe path remains available as a
/// differential oracle and for pending index-repair states.
fn fk_ddl_index_merge_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| match std::env::var("MYDB_FK_DDL_INDEX_MERGE") {
        Ok(value) => !matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "0" | "off" | "false" | "no"
        ),
        Err(_) => true,
    })
}

/// Runtime override for the FK cascade depth limit, settable via
/// `SET GLOBAL mydb_fk_max_depth = N`. `0` means "not overridden — fall back
/// to the `MYDB_FK_MAX_DEPTH` env var (else the built-in default)". Kept as a
/// per-call env fallback (rather than a frozen cache) so tests that set the
/// env var per process still take effect.
static FK_MAX_DEPTH_OVERRIDE: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

/// Max cascade depth (`mydb_fk_max_depth` sysvar / `MYDB_FK_MAX_DEPTH` env,
/// default 64). The legacy default was 15 (matching InnoDB's
/// `FK_MAX_CASCADE_DEL`); the visited-set guard now handles cycles precisely,
/// so the depth limit is left as a runaway-recursion backstop and bumped to
/// allow deeper acyclic chains.
fn fk_max_depth() -> u32 {
    let ov = FK_MAX_DEPTH_OVERRIDE.load(std::sync::atomic::Ordering::Relaxed);
    if ov != 0 {
        return ov;
    }
    std::env::var("MYDB_FK_MAX_DEPTH")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|d| *d > 0)
        .unwrap_or(64)
}

/// Current effective FK cascade depth limit (for the `mydb_fk_max_depth`
/// sysvar getter).
pub(crate) fn fk_max_depth_value() -> u64 {
    fk_max_depth() as u64
}

/// Install a runtime FK cascade depth limit (`SET GLOBAL mydb_fk_max_depth`).
/// Clamped to at least 1 (`0` is the "not overridden" sentinel).
pub(crate) fn set_fk_max_depth(v: u64) {
    let v = v.clamp(1, u32::MAX as u64) as u32;
    FK_MAX_DEPTH_OVERRIDE.store(v, std::sync::atomic::Ordering::Relaxed);
}

// ===========================================================================
// DDL: validation / construction
// ===========================================================================

/// Resolve a parser referential action to a stored one, rejecting
/// `SET DEFAULT` (parsed for fidelity, refused like InnoDB → error 1215).
fn fk_resolve_action(
    action: Option<ReferentialActionDef>,
    fk_name: &str,
) -> Result<ReferentialAction, QueryError> {
    Ok(match action {
        None | Some(ReferentialActionDef::NoAction) => ReferentialAction::NoAction,
        Some(ReferentialActionDef::Restrict) => ReferentialAction::Restrict,
        Some(ReferentialActionDef::Cascade) => ReferentialAction::Cascade,
        Some(ReferentialActionDef::SetNull) => ReferentialAction::SetNull,
        Some(ReferentialActionDef::SetDefault) => {
            return Err(error(
                crate::error::catalog::ER_CANNOT_ADD_FOREIGN,
                format!(
                    "Cannot add foreign key constraint '{fk_name}': SET DEFAULT is not supported"
                ),
            ))
        }
    })
}

/// True when child column type `(a, a_unsigned)` is FK-compatible with
/// parent column type `(b, b_unsigned)`. Integers must match exactly
/// (kind and sign); ENUM/SET must have identical element lists; other
/// families match by family ignoring length (MySQL allows e.g. differing
/// VARCHAR lengths as long as charset matches — approximated here).
fn fk_types_compatible(
    a: &DataTypeKind,
    a_unsigned: bool,
    b: &DataTypeKind,
    b_unsigned: bool,
) -> bool {
    use DataTypeKind as D;
    match (a, b) {
        (D::Decimal { scale: x, .. }, D::Decimal { scale: y, .. }) => {
            // Precision can differ without changing equality. Scale cannot:
            // probing a lower-scale parent would round a nonexistent child.
            return x.unwrap_or(0) == y.unwrap_or(0) && a_unsigned == b_unsigned;
        }
        (D::Enum { values: x }, D::Enum { values: y }) => return x == y,
        (D::Set { values: x }, D::Set { values: y }) => return x == y,
        _ => {}
    }
    fn family(k: &D) -> u8 {
        match k {
            D::Bool | D::TinyInt => 1,
            D::SmallInt => 2,
            D::MediumInt => 3,
            D::Int => 4,
            D::BigInt => 5,
            D::Float { .. } => 10,
            D::Double { .. } => 11,
            D::Decimal { .. } => 12,
            D::Char { .. } | D::VarChar { .. } | D::Text { .. } => 20,
            D::Binary { .. } | D::VarBinary { .. } | D::Blob { .. } => 21,
            D::Date => 30,
            D::Time { .. } => 31,
            D::DateTime { .. } => 32,
            D::Timestamp { .. } => 33,
            D::Year => 34,
            D::Json => 40,
            D::Geometry { .. } => 41,
            D::Vector { .. } => 42,
            D::Enum { .. } => 50,
            D::Set { .. } => 51,
            D::Bit { .. } => 60,
        }
    }
    let is_int = |k: &D| {
        matches!(
            k,
            D::Bool | D::TinyInt | D::SmallInt | D::MediumInt | D::Int | D::BigInt
        )
    };
    if is_int(a) && is_int(b) {
        return family(a) == family(b) && a_unsigned == b_unsigned;
    }
    family(a) == family(b)
}

/// True for character-string types (CHAR/VARCHAR/TEXT). Used to gate the
/// FK collation-compatibility check — binary string types carry no
/// collation and integer/temporal/etc. types are unaffected.
fn fk_is_string_type(k: &DataTypeKind) -> bool {
    matches!(
        k,
        DataTypeKind::Char { .. } | DataTypeKind::VarChar { .. } | DataTypeKind::Text { .. }
    )
}

/// Find a Public UNIQUE / PRIMARY index on `parent` whose leftmost
/// columns are exactly `referenced` (in order). This is the parent-side
/// index that proves the referenced columns are a key (MySQL 8.0
/// requirement) and that the child/parent FK probes use.
///
/// MySQL 8.0 additionally rejects prefix-truncated key parts in the
/// referenced columns: a prefix index can map several distinct full-string
/// rows to the same B-tree key, so it can't uniquely identify a parent
/// row, and the FK match semantics become ambiguous. We mirror that by
/// skipping any candidate whose leading `referenced.len()` parts carry a
/// non-NULL prefix length.
fn fk_find_referenced_index<'a>(
    parent: &'a TableSchema,
    referenced: &[String],
) -> Option<&'a IndexSchema> {
    parent.indexes.iter().find(|idx| {
        // The referenced columns must constitute the *complete* key, not a
        // prefix: a prefix of a composite UNIQUE/PRIMARY key is not itself
        // guaranteed unique, so it can't identify a single parent row and
        // the FK's referential semantics become ambiguous. InnoDB requires
        // the referenced columns to exactly match a key's column list; we
        // mirror that with `==` (previously `>=`, which admitted unsound
        // prefix references).
        (idx.unique || idx.primary)
            && idx.state == IndexState::Public
            && !idx.is_fulltext()
            && idx.columns.len() == referenced.len()
            && referenced
                .iter()
                .zip(idx.columns.iter())
                .all(|(r, c)| r.eq_ignore_ascii_case(c))
            && (0..referenced.len()).all(|pos| idx.column_prefix_len(pos).is_none())
    })
}

/// Find an existing Public index on `child` (including any `pending`
/// indexes being created in the same statement) whose leftmost columns
/// are exactly `columns` — usable as the FK's backing index.
fn fk_find_child_backing_index(
    child: &TableSchema,
    pending: &[IndexSchema],
    columns: &[String],
) -> Option<String> {
    child
        .indexes
        .iter()
        .chain(pending.iter())
        .find(|idx| {
            idx.state == IndexState::Public
                && !idx.is_fulltext()
                && idx.columns.len() >= columns.len()
                && columns
                    .iter()
                    .zip(idx.columns.iter())
                    .all(|(c, ic)| c.eq_ignore_ascii_case(ic))
        })
        .map(|idx| idx.name.clone())
}

/// Build one [`ForeignKeySchema`] from a parsed [`ForeignKeyDef`],
/// validating against the (already index-built) child table and the
/// catalog of parent tables. Returns the FK plus an optional backing
/// [`IndexSchema`] to create when no existing index covers the child
/// columns. `used_names` / `index_names` track names claimed so far in a
/// multi-FK statement; `gen` advances the `<table>_ibfk_<n>` counter.
fn fk_build_one(
    catalog: &Catalog,
    child: &TableSchema,
    pending_indexes: &[IndexSchema],
    def: ForeignKeyDef,
    used_names: &mut BTreeSet<String>,
    gen: &mut usize,
    index_names: &mut BTreeSet<String>,
) -> Result<(ForeignKeySchema, Option<IndexSchema>), QueryError> {
    // Partitioned child tables cannot carry foreign keys.
    if child.is_partitioned() {
        return Err(error(
            crate::error::catalog::ER_FOREIGN_KEY_ON_PARTITIONED,
            "Foreign keys are not yet supported in conjunction with partitioning",
        ));
    }

    // Name: explicit (validated/deduped) or generated `<table>_ibfk_<n>`.
    let name = match def.name {
        Some(name) => {
            if name.len() > 64 {
                return Err(error(
                    crate::error::catalog::ER_TOO_LONG_IDENT,
                    "identifier name is too long for a foreign key",
                ));
            }
            name
        }
        None => loop {
            let candidate = format!("{}_ibfk_{}", child.name, *gen);
            *gen += 1;
            if !used_names.contains(&candidate) {
                break candidate;
            }
        },
    };
    if !used_names.insert(name.clone()) {
        return Err(error(
            crate::error::catalog::ER_FK_DUP_NAME,
            format!("Duplicate foreign key constraint name '{name}'"),
        ));
    }

    // Child columns: resolve + canonicalize + detect duplicates.
    if def.columns.is_empty() {
        return Err(error(
            crate::error::catalog::ER_WRONG_FK_DEF,
            format!("Incorrect foreign key definition for '{name}': no columns"),
        ));
    }
    let mut child_cols = Vec::with_capacity(def.columns.len());
    let mut child_col_idx = Vec::with_capacity(def.columns.len());
    let mut seen = BTreeSet::new();
    for col in &def.columns {
        let idx = child.column_index(col).map_err(|_| {
            error(
                crate::error::catalog::ER_KEY_COLUMN_DOES_NOT_EXITS,
                format!("Key column '{col}' doesn't exist in table"),
            )
        })?;
        let canonical = child.columns[idx].name.clone();
        if !seen.insert(normalize_name(&canonical)) {
            return Err(error(
                crate::error::catalog::ER_DUP_FIELDNAME,
                format!("Duplicate column '{col}' in foreign key"),
            ));
        }
        child_cols.push(canonical);
        child_col_idx.push(idx);
    }

    let on_delete = fk_resolve_action(def.on_delete, &name)?;
    let on_update = fk_resolve_action(def.on_update, &name)?;

    // `SET NULL` requires every child key column to be nullable — a parent
    // delete/update would otherwise null a NOT NULL column. MySQL 8.0
    // rejects this at DDL time with 1830 rather than deferring to a runtime
    // 1048 the first time the action fires.
    if matches!(on_delete, ReferentialAction::SetNull)
        || matches!(on_update, ReferentialAction::SetNull)
    {
        for &ci in &child_col_idx {
            if !child.columns[ci].nullable {
                return Err(error(crate::error::catalog::ER_FK_COLUMN_NOT_NULL,
                    format!(
                        "Column '{}' cannot be NOT NULL: needed in a foreign key constraint '{}' SET NULL",
                        child.columns[ci].name, name
                    ),
                ));
            }
        }
    }

    // Referenced table: default schema = child's. Self-reference uses the
    // child being built (it is not yet in the catalog on CREATE).
    let (ref_db_raw, ref_tbl_raw) = split_table_name(&child.database, &def.referenced_table);
    let ref_db = normalize_name(&ref_db_raw);
    let ref_tbl = normalize_name(&ref_tbl_raw);
    let self_ref =
        ref_db == normalize_name(&child.database) && ref_tbl == normalize_name(&child.name);
    // Resolve the referenced (parent) table. A self-reference resolves to the
    // child being built (it is not yet in the catalog on CREATE). When the
    // parent is absent and `foreign_key_checks` is OFF, MySQL defers the
    // referenced-side validation: it records the constraint verbatim (and still
    // creates the child backing index) so a mysqldump that emits tables out of
    // dependency order — every dump wraps its body in `SET FOREIGN_KEY_CHECKS=0`
    // — imports cleanly. `build_fk_runtime` re-links the reverse edge once the
    // parent table is later created. Keep the Arc alive for the borrow below.
    let parent_arc: Option<Arc<TableSchema>> = if self_ref {
        None
    } else {
        let pkey = table_key(&ref_db, &ref_tbl);
        match catalog.tables.get(&pkey).cloned() {
            Some(parent) => Some(parent),
            None if !fk_checks_enabled() => None,
            None => {
                return Err(error(
                    crate::error::catalog::ER_FK_CANNOT_OPEN_PARENT,
                    format!("Failed to open the referenced table '{ref_tbl}'"),
                ));
            }
        }
    };
    // `Some` → the parent is present (or this is a self-reference), so the
    // referenced side is validated now. `None` → deferred (parent absent, FK
    // checks off).
    let parent: Option<&TableSchema> = if self_ref {
        Some(child)
    } else {
        parent_arc.as_deref()
    };

    // Column count must match — a purely definitional check MySQL enforces
    // regardless of `foreign_key_checks` or whether the parent exists yet.
    if def.referenced_columns.len() != child_cols.len() {
        return Err(error(crate::error::catalog::ER_WRONG_FK_DEF,
            format!(
                "Incorrect foreign key definition for '{name}': Key reference and table reference don't match"
            ),
        ));
    }

    let ref_cols = match parent {
        Some(parent) => {
            if parent.is_partitioned() {
                return Err(error(
                    crate::error::catalog::ER_FOREIGN_KEY_ON_PARTITIONED,
                    "Foreign keys are not yet supported in conjunction with partitioning",
                ));
            }
            // Canonicalize referenced columns + per-pair type compatibility.
            let mut ref_cols = Vec::with_capacity(def.referenced_columns.len());
            for (k, rcol) in def.referenced_columns.iter().enumerate() {
                let ridx = parent.column_index(rcol).map_err(|_| {
                    error(
                        crate::error::catalog::ER_KEY_COLUMN_DOES_NOT_EXITS,
                        format!("Key column '{rcol}' doesn't exist in referenced table"),
                    )
                })?;
                let parent_col = &parent.columns[ridx];
                let child_col = &child.columns[child_col_idx[k]];
                if !fk_types_compatible(
                    &child_col.data_type,
                    child_col.unsigned,
                    &parent_col.data_type,
                    parent_col.unsigned,
                ) {
                    return Err(error(crate::error::catalog::ER_FK_INCOMPATIBLE_COLUMNS,
                        format!(
                            "Referencing column '{}' and referenced column '{}' in foreign key constraint '{}' are incompatible.",
                            child_col.name, parent_col.name, name
                        ),
                    ));
                }
                // Virtual generated columns cannot participate in a foreign
                // key (either side): their value isn't stored, so it can't
                // back an index the FK relies on. STORED generated columns
                // are allowed. MySQL reports 3733.
                if (child_col.generated_expr.is_some() && !child_col.generated_stored)
                    || (parent_col.generated_expr.is_some() && !parent_col.generated_stored)
                {
                    return Err(error(
                        crate::error::catalog::ER_FK_CANNOT_USE_VIRTUAL_COLUMN,
                        format!(
                            "Foreign key '{name}' uses virtual column '{}' which is not supported.",
                            if child_col.generated_expr.is_some() && !child_col.generated_stored {
                                &child_col.name
                            } else {
                                &parent_col.name
                            }
                        ),
                    ));
                }
                // Character columns must share a collation (hence charset).
                // Resolve the effective collation (column override else table
                // default) on each side; a mismatch is 3780, matching MySQL
                // 8.0 which rejects string FKs across differing collations.
                if fk_is_string_type(&child_col.data_type) {
                    let child_coll = child_col.collation.as_deref().unwrap_or(&child.collation);
                    let parent_coll = parent_col.collation.as_deref().unwrap_or(&parent.collation);
                    if !child_coll.eq_ignore_ascii_case(parent_coll) {
                        return Err(error(crate::error::catalog::ER_FK_INCOMPATIBLE_COLUMNS,
                            format!(
                                "Referencing column '{}' and referenced column '{}' in foreign key constraint '{}' are incompatible.",
                                child_col.name, parent_col.name, name
                            ),
                        ));
                    }
                }
                ref_cols.push(parent_col.name.clone());
            }
            // Referenced columns must be the leftmost prefix of a UNIQUE/PRIMARY key.
            if fk_find_referenced_index(parent, &ref_cols).is_none() {
                return Err(error(crate::error::catalog::ER_FK_NO_INDEX_PARENT,
                    format!(
                        "Failed to add the foreign key constraint. Missing index for constraint '{name}' in the referenced table '{ref_tbl}'"
                    ),
                ));
            }
            ref_cols
        }
        // Deferred: store the referenced column names as written; the parent's
        // columns/index are validated when it appears (never retroactively, matching
        // MySQL — re-enabling FOREIGN_KEY_CHECKS does not re-check existing rows).
        None => def.referenced_columns.clone(),
    };

    // Backing index on the child: reuse an existing one or create it.
    let (backing_index, new_index) =
        match fk_find_child_backing_index(child, pending_indexes, &child_cols) {
            Some(existing) => (existing, None),
            None => {
                let mut idx_name = name.clone();
                let mut suffix = 2;
                while index_names.contains(&normalize_name(&idx_name)) {
                    idx_name = format!("{name}_{suffix}");
                    suffix += 1;
                }
                index_names.insert(normalize_name(&idx_name));
                let n = child_cols.len();
                let index = IndexSchema {
                    readable_from_scn: 0,
                    index_id: 0,
                    name: idx_name.clone(),
                    columns: child_cols.clone(),
                    column_prefix_lengths: vec![None; n],
                    column_directions: vec![crate::storage::IndexColumnDirection::Asc; n],
                    key_expressions: Vec::new(),
                    unique: false,
                    primary: false,
                    included_columns: Vec::new(),
                    state: IndexState::Public,
                    index_type: "BTREE".to_string(),
                    visible: true,
                    comment: String::new(),
                    fulltext: None,
                };
                (idx_name, Some(index))
            }
        };

    Ok((
        ForeignKeySchema {
            name,
            columns: child_cols,
            referenced_database: ref_db,
            referenced_table: ref_tbl,
            referenced_columns: ref_cols,
            on_delete,
            on_update,
            backing_index,
            backing_index_id: 0,
            child_col_idx,
        },
        new_index,
    ))
}

/// Build all foreign keys for a CREATE TABLE. Returns the FK schemas plus
/// any backing indexes that must be appended to the table (the table is
/// empty at CREATE time, so no orphan scan is needed and indexes are born
/// `Public`).
fn build_foreign_keys_for_table(
    catalog: &Catalog,
    child: &TableSchema,
    defs: Vec<ForeignKeyDef>,
) -> Result<(Vec<ForeignKeySchema>, Vec<IndexSchema>), QueryError> {
    let mut out = Vec::with_capacity(defs.len());
    let mut new_indexes: Vec<IndexSchema> = Vec::new();
    let mut used_names: BTreeSet<String> =
        child.foreign_keys.iter().map(|f| f.name.clone()).collect();
    let mut index_names: BTreeSet<String> = child
        .indexes
        .iter()
        .map(|i| normalize_name(&i.name))
        .collect();
    let mut gen = 1usize;
    for def in defs {
        let (fk, maybe_index) = fk_build_one(
            catalog,
            child,
            &new_indexes,
            def,
            &mut used_names,
            &mut gen,
            &mut index_names,
        )?;
        if let Some(idx) = maybe_index {
            new_indexes.push(idx);
        }
        out.push(fk);
    }
    Ok((out, new_indexes))
}

// ===========================================================================
// Error / message helpers
// ===========================================================================

fn fk_constraint_clause(child: &TableSchema, fk: &ForeignKeySchema) -> String {
    format!(
        "(`{}`.`{}`, CONSTRAINT `{}` FOREIGN KEY (`{}`) REFERENCES `{}` (`{}`))",
        child.database,
        child.name,
        fk.name,
        fk.columns.join("`, `"),
        fk.referenced_table,
        fk.referenced_columns.join("`, `"),
    )
}

fn fk_child_violation_err(child: &TableSchema, fk: &ForeignKeySchema) -> QueryError {
    error(
        crate::error::catalog::ER_NO_REFERENCED_ROW_2,
        format!(
            "Cannot add or update a child row: a foreign key constraint fails {}",
            fk_constraint_clause(child, fk)
        ),
    )
}

fn fk_parent_violation_err(child: &TableSchema, inbound: &InboundFk) -> QueryError {
    let clause = child
        .foreign_keys
        .iter()
        .find(|f| f.name.eq_ignore_ascii_case(&inbound.fk_name))
        .map(|fk| fk_constraint_clause(child, fk))
        .unwrap_or_else(|| format!("(CONSTRAINT `{}`)", inbound.fk_name));
    error(
        crate::error::catalog::ER_ROW_IS_REFERENCED_2,
        format!("Cannot delete or update a parent row: a foreign key constraint fails {clause}"),
    )
}

// ===========================================================================
// Key building + MVCC "latest committed or own" probing
// ===========================================================================

/// Build an index key from `vals` (one per `ref_cols`, in order) using
/// `table`'s column types and `index`'s prefix metadata. Produces a key
/// of length `ref_cols.len()` — a prefix of `index` when it is composite.
fn fk_build_key(
    table: &TableSchema,
    index: &IndexSchema,
    ref_cols: &[String],
    vals: &[Value],
) -> Result<IndexKey, QueryError> {
    let mut parts = Vec::with_capacity(ref_cols.len());
    for (k, col) in ref_cols.iter().enumerate() {
        let idx = table.column_index(col)?;
        let column = &table.columns[idx];
        let part = KeyPart::from_column_value_with_collation(
            &vals[k],
            &column.data_type,
            column.collation.as_deref(),
        );
        parts.push(index.apply_prefix_to_key_part(k, part));
    }
    Ok(IndexKey::from(parts))
}

/// How `read_row_committed_or_own` should fold an undo record's
/// before-image into the working row as it walks back through versions.
enum UndoApply {
    /// Walking back through an insert undo means there is no
    /// before-version — the row didn't exist before this writer.
    Insert,
    /// The undo record carries the entire pre-update row in its
    /// `before_values` (full-row UPDATE rewrite or DELETE tombstone).
    /// We replace the working row outright.
    FullRow,
    /// Phase 2 column-level UPDATE delta: `before_values[i]` is the
    /// pre-update value of column `before_cols[i]`. We patch only
    /// those positions on the working row — overwriting the whole
    /// row with the sparse delta would corrupt every other column.
    UpdateDelta(Vec<u16>),
}

impl Database {
    pub(crate) fn fk_parent_update_may_affect_references(
        parent: &TableSchema,
        changed_columns: Option<&[usize]>,
    ) -> bool {
        if parent.fk_referenced_by.is_empty() || !fk_checks_enabled() {
            return false;
        }
        let Some(changed_columns) = changed_columns else {
            return true;
        };
        if changed_columns.is_empty() {
            return false;
        }
        parent.fk_referenced_by.iter().any(|inbound| {
            inbound
                .referenced_col_idx
                .iter()
                .any(|idx| changed_columns.contains(idx))
        })
    }

    /// Read the latest committed-or-own version of `row_id`, walking past
    /// versions written by other still-active transactions. Returns the
    /// effective row values if that version is live (not delete-marked),
    /// else `None`. Unlike the snapshot read, this reflects rows committed
    /// after the statement snapshot — required so FK probes (taken under a
    /// row lock) cannot miss a just-committed parent/child and orphan a
    /// row. FK key columns are scalar (indexed PK/unique), so no off-page
    /// LOB materialization is needed.
    fn read_row_committed_or_own(
        &self,
        table: &TableSchema,
        row_id: RowId,
        txn: TxnView,
    ) -> Result<Option<Vec<Value>>, QueryError> {
        let Some(row) = self.read_mvcc_row(table.id, row_id)? else {
            return Ok(None);
        };
        let mut header = row.header;
        let mut values = row.values;
        loop {
            let owned_or_committed = header.last_txn_id == txn.txn_id
                || matches!(
                    self.txns_read()?.status(header.last_txn_id),
                    Some(TxnStatus::Committed) | None
                );
            if owned_or_committed {
                if header.is_deleted() {
                    return Ok(None);
                }
                Self::pad_full_row_to_schema(&mut values, table);
                return Ok(Some(values));
            }
            let Some(undo_ptr) = header.undo_ptr else {
                return Ok(None);
            };
            // Capture the undo kind too, not just the before-image fields:
            // `UpdateDelta` records carry only the *changed* columns
            // (`before_values[i]` is the pre-image of `before_cols[i]`),
            // so we must patch `values` column-by-column instead of
            // replacing the whole row. Replacing the row with a sparse
            // delta corrupts every other column for downstream consumers —
            // FK enforcement then computes the parent's index key from
            // garbage positions and reports "child row references a row
            // that doesn't exist" against rows that are perfectly fine.
            let (apply, before_header, before_values) = {
                match self.load_undo_owned(undo_ptr)? {
                    Some(rec) => {
                        let apply = match &rec.kind {
                            UndoKind::Insert | UndoKind::InsertBatch { .. } => UndoApply::Insert,
                            UndoKind::UpdateDelta { before_cols } => {
                                UndoApply::UpdateDelta(before_cols.clone())
                            }
                            UndoKind::Update | UndoKind::Delete => UndoApply::FullRow,
                        };
                        (apply, rec.before_header, rec.before_values)
                    }
                    None => return Ok(None),
                }
            };
            match apply {
                UndoApply::Insert => return Ok(None),
                UndoApply::UpdateDelta(before_cols) => {
                    let Some(bh) = before_header else {
                        return Ok(None);
                    };
                    header = bh;
                    if let Some(bv) = before_values {
                        let n = before_cols.len().min(bv.len());
                        for i in 0..n {
                            let col = before_cols[i] as usize;
                            if col < values.len() {
                                values[col] = bv[i].clone();
                            }
                        }
                    }
                }
                UndoApply::FullRow => {
                    let Some(bh) = before_header else {
                        return Ok(None);
                    };
                    header = bh;
                    if let Some(bv) = before_values {
                        values = bv;
                    }
                }
            }
        }
    }

    /// True if `row_id` (latest committed-or-own version) is live and its
    /// `index` key matches `key` on the first `key.len()` parts.
    fn fk_row_key_matches(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        row_id: RowId,
        key: &IndexKey,
        txn: TxnView,
    ) -> Result<bool, QueryError> {
        let Some(vals) = self.read_row_committed_or_own(table, row_id, txn)? else {
            return Ok(false);
        };
        let full = table.index_key(index, &vals)?;
        Ok(full.0.len() >= key.0.len() && full.0[..key.0.len()] == key.0[..])
    }

    /// Row ids of `child` rows whose `index` key matches `key` (committed
    /// or own, live).
    fn fk_matching_child_rows(
        &self,
        child: &TableSchema,
        index: &IndexSchema,
        key: &IndexKey,
        txn: TxnView,
    ) -> Result<Vec<RowId>, QueryError> {
        let mut out = Vec::new();
        for rid in self.prefix_index(child, index, key.clone())? {
            if self.fk_row_key_matches(child, index, rid, key, txn)? {
                out.push(rid);
            }
        }
        Ok(out)
    }

    /// True if *any* live child row matches `key`. RESTRICT / NO ACTION only
    /// needs existence, so this short-circuits on the first live match
    /// instead of materializing every referencing row like
    /// [`Self::fk_matching_child_rows`] — a win when a parent row has many
    /// children and the delete/update is going to be rejected anyway.
    fn fk_any_matching_child_row(
        &self,
        child: &TableSchema,
        index: &IndexSchema,
        key: &IndexKey,
        txn: TxnView,
    ) -> Result<bool, QueryError> {
        for rid in self.prefix_index(child, index, key.clone())? {
            if self.fk_row_key_matches(child, index, rid, key, txn)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Candidate parent rows for an FK reference probe.
    ///
    /// When the referenced columns cover the whole UNIQUE/PRIMARY index key,
    /// an exact lookup is enough and avoids the heavier prefix-scan path. FK
    /// DDL also permits referencing the leftmost prefix of a wider unique key,
    /// so keep the prefix scan for that case and re-check heap values below.
    fn fk_parent_reference_candidates(
        &self,
        parent: &TableSchema,
        index: &IndexSchema,
        key: &IndexKey,
        txn: TxnView,
    ) -> Result<Vec<RowId>, QueryError> {
        if key.0.len() == index.columns.len() {
            self.lookup_index_visible(parent, index, key.clone(), txn)
        } else {
            self.prefix_index_visible(parent, index, key.clone(), txn)
        }
    }

    /// Run `f` one cascade level deeper. Two safety guards run in tandem:
    ///   * **Visited set** ([`FK_CASCADE_VISITED`]) — populated by callers
    ///     via [`fk_mark_visited`] right before re-entering the cascade for
    ///     a particular `(table, row_id)` pair. Repeated visits short-circuit.
    ///   * **Depth limit** (`MYDB_FK_MAX_DEPTH`, default 64) — backstop for
    ///     runaway recursion that the visited set somehow misses. Raised
    ///     from the historical 15 because legitimate deep but acyclic
    ///     chains (M5.C5 motivating case) used to false-trip the depth limit.
    /// The outermost exit drains the visited set so it never bleeds into
    /// the next statement.
    fn fk_with_depth<R>(&self, f: impl FnOnce() -> Result<R, QueryError>) -> Result<R, QueryError> {
        let depth = FK_CASCADE_DEPTH.with(|c| {
            let d = c.get() + 1;
            c.set(d);
            d
        });
        let result = if depth > fk_max_depth() {
            Err(error(
                crate::error::catalog::ER_FK_DEPTH_EXCEEDED,
                format!(
                    "Foreign key cascade delete/update exceeds max depth of {}.",
                    fk_max_depth()
                ),
            ))
        } else {
            f()
        };
        let outer = FK_CASCADE_DEPTH.with(|c| {
            let d = c.get().saturating_sub(1);
            c.set(d);
            d
        });
        if outer == 0 {
            FK_CASCADE_VISITED.with(|set| set.borrow_mut().clear());
        }
        result
    }

    // =======================================================================
    // DML enforcement — child side (INSERT / UPDATE)
    // =======================================================================

    /// Enforce that every non-NULL foreign key on `table` (the child)
    /// references an existing parent row. `changed` (when present) gates
    /// the check to FKs whose columns actually changed. Locks the matched
    /// parent row in shared mode so a concurrent parent DELETE/UPDATE
    /// (which takes the row exclusively) blocks until this txn commits —
    /// preventing orphans.
    pub(crate) fn fk_check_child_references(
        &self,
        table: &TableSchema,
        txn: TxnView,
        values: &[Value],
        changed: Option<&[usize]>,
    ) -> Result<(), QueryError> {
        if table.foreign_keys.is_empty() || !fk_checks_enabled() {
            return Ok(());
        }
        for fk in &table.foreign_keys {
            if let Some(changed) = changed {
                if !fk.affected_by_changed(changed) {
                    continue;
                }
            }
            // Resolve the parent table + referenced index for this FK, then
            // enforce the single row. `None` means the parent/index is not
            // (yet) present — the constraint is treated as satisfied, matching
            // the deferred-FK contract.
            let Some((parent, index)) = self.fk_resolve_parent_index(fk)? else {
                continue;
            };
            self.fk_enforce_child_row_for_fk(table, fk, &parent, &index, txn, values)?;
        }
        Ok(())
    }

    /// Batch child-side FK check for a full INSERT batch. Resolves each FK's
    /// parent table + referenced index **once** (instead of per row), then
    /// enforces every row against the pre-resolved handles. The per-row path
    /// re-took the catalog read lock, re-cloned the parent `Arc`, and
    /// re-scanned the parent's index list for every row × FK; a 10k-row batch
    /// with one FK issued 10k of each. Behaviourally identical to calling
    /// `fk_check_child_references(row, None)` per row (same NULL semantics,
    /// same locks, same first-violation error), so INSERT ordering and error
    /// messages are unchanged.
    pub(crate) fn fk_check_child_references_batch(
        &self,
        table: &TableSchema,
        txn: TxnView,
        rows: &[Vec<Value>],
    ) -> Result<(), QueryError> {
        if table.foreign_keys.is_empty() || !fk_checks_enabled() || rows.is_empty() {
            return Ok(());
        }
        // Resolve (parent, index) once per FK; skip unresolved (deferred) FKs.
        let mut resolved: Vec<(
            &ForeignKeySchema,
            Arc<TableSchema>,
            IndexSchema,
            smallvec::SmallVec<[usize; 4]>,
        )> = Vec::with_capacity(table.foreign_keys.len());
        for fk in &table.foreign_keys {
            if let Some((parent, index)) = self.fk_resolve_parent_index(fk)? {
                let referenced_columns = fk
                    .referenced_columns
                    .iter()
                    .map(|column| parent.column_index(column))
                    .collect::<Result<smallvec::SmallVec<[usize; 4]>, _>>()?;
                resolved.push((fk, parent, index, referenced_columns));
            }
        }
        if resolved.is_empty() {
            return Ok(());
        }
        // Transaction-duration reference locks make a successfully checked
        // parent key stable for the rest of this INSERT statement/txn. Batch
        // producers commonly group adjacent rows by parent; remember only
        // the most recent success per FK so a repeated key avoids another
        // lock-manager, B-tree and heap round trip. This is deliberately a
        // constant-space cache: row/FK order and the first failing key remain
        // identical to the scalar oracle, while arbitrary large batches do
        // not acquire a second row-proportional memory footprint.
        let mut last_satisfied = vec![None::<IndexKey>; resolved.len()];
        // Row-outer / FK-inner preserves the single-row path's report order.
        for values in rows {
            for (position, (fk, parent, index, referenced_columns)) in resolved.iter().enumerate() {
                let Some(key) = self.fk_child_reference_key(fk, parent, index, values)? else {
                    continue;
                };
                if last_satisfied[position].as_ref() == Some(&key) {
                    continue;
                }
                self.fk_enforce_child_key(
                    table,
                    fk,
                    parent,
                    index,
                    referenced_columns,
                    txn,
                    &key,
                )?;
                last_satisfied[position] = Some(key);
            }
        }
        Ok(())
    }

    // Preserve the warm descriptor fast path, but never confuse an evicted
    // header with a missing table/index. Drop the catalog guard before lazy
    // loading, and propagate decode/I/O failures for tables known to exist.
    fn fk_table_descriptor(&self, key: &str) -> Result<Option<Arc<TableSchema>>, QueryError> {
        let existing = self.catalog_read()?.tables.get(key).cloned();
        match existing {
            Some(table) if table.columns.is_empty() => self.table(key).map(Some),
            other => Ok(other),
        }
    }

    /// Resolve an FK's parent table (`Arc` clone) and its referenced
    /// UNIQUE/PRIMARY index. Returns `None` when the parent table or a
    /// suitable index is absent (deferred FK / dangling reference), for which
    /// the child-side check is a no-op.
    fn fk_resolve_parent_index(
        &self,
        fk: &ForeignKeySchema,
    ) -> Result<Option<(Arc<TableSchema>, IndexSchema)>, QueryError> {
        let parent_key = table_key(&fk.referenced_database, &fk.referenced_table);
        let Some(parent) = self.fk_table_descriptor(&parent_key)? else {
            return Ok(None);
        };
        let Some(index) = fk_find_referenced_index(&parent, &fk.referenced_columns) else {
            return Ok(None);
        };
        let index = index.clone();
        Ok(Some((parent, index)))
    }

    /// Enforce a single child row against one already-resolved FK
    /// (`parent`/`index`). MATCH SIMPLE: any NULL key column satisfies the
    /// constraint. Otherwise probes the parent under a SHARED reference (or
    /// row) lock and raises 1452 if no live parent row matches.
    fn fk_enforce_child_row_for_fk(
        &self,
        table: &TableSchema,
        fk: &ForeignKeySchema,
        parent: &TableSchema,
        index: &IndexSchema,
        txn: TxnView,
        values: &[Value],
    ) -> Result<(), QueryError> {
        let Some(key) = self.fk_child_reference_key(fk, parent, index, values)? else {
            return Ok(());
        };
        let referenced_columns = fk
            .referenced_columns
            .iter()
            .map(|column| parent.column_index(column))
            .collect::<Result<smallvec::SmallVec<[usize; 4]>, _>>()?;
        self.fk_enforce_child_key(
            table,
            fk,
            parent,
            index,
            &referenced_columns,
            txn,
            &key,
        )
    }

    /// Build one child reference key. `None` implements MATCH SIMPLE: any
    /// NULL component satisfies the FK without probing the parent.
    fn fk_child_reference_key(
        &self,
        fk: &ForeignKeySchema,
        parent: &TableSchema,
        index: &IndexSchema,
        values: &[Value],
    ) -> Result<Option<IndexKey>, QueryError> {
        // MATCH SIMPLE: any NULL key column → constraint satisfied.
        let mut keyvals = Vec::with_capacity(fk.child_col_idx.len());
        for &ci in &fk.child_col_idx {
            match values.get(ci) {
                Some(Value::Null) | None => return Ok(None),
                Some(v) => keyvals.push(v.clone()),
            }
        }
        fk_build_key(parent, index, &fk.referenced_columns, &keyvals).map(Some)
    }

    fn fk_enforce_child_key(
        &self,
        table: &TableSchema,
        fk: &ForeignKeySchema,
        parent: &TableSchema,
        index: &IndexSchema,
        referenced_columns: &[usize],
        txn: TxnView,
        key: &IndexKey,
    ) -> Result<(), QueryError> {
        // Fine-grained path: take a SHARED reference lock keyed on the
        // referenced key *before* the existence scan. It serializes only
        // against a parent DELETE / referenced-key UPDATE (which take the
        // matching EXCLUSIVE ref-lock) — not against a plain non-key parent
        // UPDATE — so the confirmed-existence result cannot be invalidated
        // by a concurrent delete before we commit, while avoiding the
        // over-broad parent-data-row lock that deadlocks under TPC-C.
        let use_ref_lock = txn.txn_id != 0 && fk_reference_lock_enabled();
        if use_ref_lock {
            self.acquire_row_lock_blocking(
                txn.txn_id,
                fk_ref_lock_table_id(parent.id),
                fk_ref_lock_row_id(&index.name, key),
                LockMode::Shared,
            )?;
        }
        let mut satisfied = false;
        for rid in self.fk_parent_reference_candidates(parent, index, key, txn)? {
            if !self.fk_parent_row_key_matches_projected(
                parent,
                index,
                &fk.referenced_columns,
                referenced_columns,
                rid,
                key,
                txn,
            )? {
                continue;
            }
            if use_ref_lock {
                // The ref-lock already blocks a concurrent delete of this
                // key from committing, so a visible match is durable.
                satisfied = true;
                break;
            }
            // Legacy path: pin the specific parent row against concurrent
            // delete/update, then re-confirm after acquiring the lock.
            if txn.txn_id != 0 {
                self.acquire_row_lock_blocking(txn.txn_id, parent.id, rid, LockMode::Shared)?;
            }
            if self.fk_parent_row_key_matches_projected(
                parent,
                index,
                &fk.referenced_columns,
                referenced_columns,
                rid,
                key,
                txn,
            )? {
                satisfied = true;
                break;
            }
        }
        if !satisfied {
            return Err(fk_child_violation_err(table, fk));
        }
        Ok(())
    }

    /// FK parent probes need only the referenced unique-key columns. Decode
    /// those columns directly when the newest heap version is committed (or
    /// ours); an in-flight foreign writer retains the full undo-walking oracle
    /// so commit-frontier holes and sparse undo remain correct.
    #[allow(clippy::too_many_arguments)]
    fn fk_parent_row_key_matches_projected(
        &self,
        parent: &TableSchema,
        index: &IndexSchema,
        referenced_column_names: &[String],
        referenced_columns: &[usize],
        row_id: RowId,
        key: &IndexKey,
        txn: TxnView,
    ) -> Result<bool, QueryError> {
        let Some((header, mut values, stored_count)) = self
            .read_mvcc_row_columns_with_plan(parent.id, row_id, referenced_columns, None)?
        else {
            return Ok(false);
        };
        let newest_is_visible = header.last_txn_id == txn.txn_id
            || matches!(
                self.txns_read()?.status(header.last_txn_id),
                Some(TxnStatus::Committed) | None
            );
        if !newest_is_visible {
            return self.fk_row_key_matches(parent, index, row_id, key, txn);
        }
        if header.is_deleted() {
            return Ok(false);
        }
        Self::backfill_projected_with_defaults(
            &mut values,
            referenced_columns,
            stored_count,
            parent,
        );
        let current = fk_build_key(parent, index, referenced_column_names, &values)?;
        Ok(current.0.len() >= key.0.len() && current.0[..key.0.len()] == key.0[..])
    }

    // =======================================================================
    // DML enforcement — parent side (DELETE / UPDATE)
    // =======================================================================

    /// Map a parent row's referenced-column values into the child backing
    /// index key. Returns `None` when any referenced value is NULL (no
    /// child can match — MATCH SIMPLE).
    fn fk_child_lookup_key(
        &self,
        child: &TableSchema,
        index: &IndexSchema,
        inbound: &InboundFk,
        parent_vals: &[Value],
    ) -> Result<Option<IndexKey>, QueryError> {
        let mut parts = Vec::with_capacity(inbound.child_col_idx.len());
        for (k, (&ci, &ri)) in inbound
            .child_col_idx
            .iter()
            .zip(inbound.referenced_col_idx.iter())
            .enumerate()
        {
            let v = parent_vals.get(ri).cloned().unwrap_or(Value::Null);
            if matches!(v, Value::Null) {
                return Ok(None);
            }
            let column = &child.columns[ci];
            let part = KeyPart::from_column_value_with_collation(
                &v,
                &column.data_type,
                column.collation.as_deref(),
            );
            parts.push(index.apply_prefix_to_key_part(k, part));
        }
        Ok(Some(IndexKey::from(parts)))
    }

    /// Take the EXCLUSIVE reference lock for the key `values` presents to
    /// `inbound`'s referenced index, so an in-flight child existence check
    /// (holding the matching SHARED ref-lock) blocks this parent
    /// delete/key-change until it commits. No-op when ref-locks are disabled,
    /// for the system txn, or when any referenced column is NULL (no child can
    /// reference a NULL key under MATCH SIMPLE).
    fn fk_lock_parent_reference_key(
        &self,
        parent: &TableSchema,
        inbound: &InboundFk,
        values: &[Value],
        txn: TxnView,
    ) -> Result<(), QueryError> {
        if !fk_reference_lock_enabled() || txn.txn_id == 0 {
            return Ok(());
        }
        let mut ref_cols = Vec::with_capacity(inbound.referenced_col_idx.len());
        let mut vals = Vec::with_capacity(inbound.referenced_col_idx.len());
        for &ci in &inbound.referenced_col_idx {
            match values.get(ci) {
                Some(Value::Null) | None => return Ok(()),
                Some(v) => {
                    ref_cols.push(parent.columns[ci].name.clone());
                    vals.push(v.clone());
                }
            }
        }
        let Some(index) = fk_find_referenced_index(parent, &ref_cols) else {
            return Ok(());
        };
        let key = fk_build_key(parent, index, &ref_cols, &vals)?;
        self.acquire_row_lock_blocking(
            txn.txn_id,
            fk_ref_lock_table_id(parent.id),
            fk_ref_lock_row_id(&index.name, &key),
            LockMode::Exclusive,
        )
    }

    /// Apply parent-side referential actions before a parent row is
    /// deleted. RESTRICT / NO ACTION raise 1451 if any child references
    /// it; CASCADE deletes the children; SET NULL nulls their FK columns.
    /// All within the same transaction (one undo chain → atomic rollback).
    pub(crate) fn fk_enforce_parent_delete(
        &self,
        parent: &TableSchema,
        txn: TxnView,
        old_values: &[Value],
    ) -> Result<(), QueryError> {
        if parent.fk_referenced_by.is_empty() || !fk_checks_enabled() {
            return Ok(());
        }
        self.fk_with_depth(|| {
            for inbound in &parent.fk_referenced_by {
                // Serialize against concurrent child existence checks on the
                // key being removed before we look for (or cascade to) children.
                self.fk_lock_parent_reference_key(parent, inbound, old_values, txn)?;
                let Some(child) = self.fk_table_descriptor(&inbound.child_table_key)? else {
                    continue;
                };
                let Some(index) = child
                    .indexes
                    .iter()
                    .find(|i| i.index_id == inbound.child_backing_index_id)
                    .cloned()
                else {
                    continue;
                };
                let Some(key) = self.fk_child_lookup_key(&child, &index, inbound, old_values)?
                else {
                    continue;
                };
                // RESTRICT / NO ACTION only need existence — probe with a
                // short-circuit instead of materializing every child row.
                if matches!(
                    inbound.on_delete,
                    ReferentialAction::NoAction | ReferentialAction::Restrict
                ) {
                    if self.fk_any_matching_child_row(&child, &index, &key, txn)? {
                        return Err(fk_parent_violation_err(&child, inbound));
                    }
                    continue;
                }
                let rows = self.fk_matching_child_rows(&child, &index, &key, txn)?;
                if rows.is_empty() {
                    continue;
                }
                // Per-edge discriminator so the diamond case (same child row
                // reached via two distinct FKs) applies both actions.
                let fk_disc = fk_visited_disc(&inbound.fk_name);
                match inbound.on_delete {
                    ReferentialAction::NoAction | ReferentialAction::Restrict => {
                        return Err(fk_parent_violation_err(&child, inbound));
                    }
                    ReferentialAction::Cascade => {
                        for rid in rows {
                            // M5.C5 — skip rows this FK edge has already
                            // visited this statement to break A→B→A
                            // CASCADE loops without false-tripping the
                            // depth limit. Each skip counts in
                            // `fk_cycle_short_circuits` for forensics.
                            if !fk_mark_visited(child.id, rid, fk_disc) {
                                self.dml_stats.record_fk_cycle_short_circuit();
                                continue;
                            }
                            self.delete_row_by_id(txn, &child, rid)?;
                        }
                    }
                    ReferentialAction::SetNull => {
                        for rid in rows {
                            if !fk_mark_visited(child.id, rid, fk_disc) {
                                self.dml_stats.record_fk_cycle_short_circuit();
                                continue;
                            }
                            let setcols = inbound.child_col_idx.clone();
                            let changed = setcols.clone();
                            self.update_row_by_id_with_changed_columns(
                                txn,
                                &child,
                                rid,
                                Some(&changed),
                                move |old| {
                                    let mut nv = old.to_vec();
                                    for &ci in &setcols {
                                        nv[ci] = Value::Null;
                                    }
                                    Ok(nv)
                                },
                            )?;
                        }
                    }
                }
            }
            Ok(())
        })
    }

    /// Apply parent-side referential actions before a parent row is
    /// updated. Only fires for FKs whose referenced columns actually
    /// change. CASCADE propagates the new key to children; SET NULL nulls
    /// the children's FK columns; RESTRICT / NO ACTION raise 1451.
    pub(crate) fn fk_enforce_parent_update(
        &self,
        parent: &TableSchema,
        txn: TxnView,
        old_values: &[Value],
        new_values: &[Value],
    ) -> Result<(), QueryError> {
        if parent.fk_referenced_by.is_empty() || !fk_checks_enabled() {
            return Ok(());
        }
        self.fk_with_depth(|| {
            for inbound in &parent.fk_referenced_by {
                let changed_ref = inbound.referenced_col_idx.iter().any(|&ri| {
                    match (old_values.get(ri), new_values.get(ri)) {
                        (Some(a), Some(b)) if a != b => {
                            let column = &parent.columns[ri];
                            KeyPart::from_column_value_with_collation(
                                a,
                                &column.data_type,
                                column.collation.as_deref(),
                            ) != KeyPart::from_column_value_with_collation(
                                b,
                                &column.data_type,
                                column.collation.as_deref(),
                            )
                        }
                        _ => false,
                    }
                });
                if !changed_ref {
                    continue;
                }
                // The referenced key is changing — serialize against child
                // existence checks on the OLD key before we act on children.
                self.fk_lock_parent_reference_key(parent, inbound, old_values, txn)?;
                let Some(child) = self.fk_table_descriptor(&inbound.child_table_key)? else {
                    continue;
                };
                let Some(index) = child
                    .indexes
                    .iter()
                    .find(|i| i.index_id == inbound.child_backing_index_id)
                    .cloned()
                else {
                    continue;
                };
                let Some(key) = self.fk_child_lookup_key(&child, &index, inbound, old_values)?
                else {
                    continue;
                };
                // RESTRICT / NO ACTION only need existence — probe with a
                // short-circuit instead of materializing every child row.
                if matches!(
                    inbound.on_update,
                    ReferentialAction::NoAction | ReferentialAction::Restrict
                ) {
                    if self.fk_any_matching_child_row(&child, &index, &key, txn)? {
                        return Err(fk_parent_violation_err(&child, inbound));
                    }
                    continue;
                }
                let rows = self.fk_matching_child_rows(&child, &index, &key, txn)?;
                if rows.is_empty() {
                    continue;
                }
                // Per-edge discriminator (see fk_enforce_parent_delete).
                let fk_disc = fk_visited_disc(&inbound.fk_name);
                match inbound.on_update {
                    ReferentialAction::NoAction | ReferentialAction::Restrict => {
                        return Err(fk_parent_violation_err(&child, inbound));
                    }
                    ReferentialAction::Cascade => {
                        let newvals: Vec<Value> = inbound
                            .referenced_col_idx
                            .iter()
                            .map(|&ri| new_values.get(ri).cloned().unwrap_or(Value::Null))
                            .collect();
                        for rid in rows {
                            // M5.C5 — short-circuit on a revisited child row.
                            if !fk_mark_visited(child.id, rid, fk_disc) {
                                self.dml_stats.record_fk_cycle_short_circuit();
                                continue;
                            }
                            let setcols = inbound.child_col_idx.clone();
                            let changed = setcols.clone();
                            let nv = newvals.clone();
                            self.update_row_by_id_with_changed_columns(
                                txn,
                                &child,
                                rid,
                                Some(&changed),
                                move |old| {
                                    let mut row = old.to_vec();
                                    for (k, &ci) in setcols.iter().enumerate() {
                                        row[ci] = nv[k].clone();
                                    }
                                    Ok(row)
                                },
                            )?;
                        }
                    }
                    ReferentialAction::SetNull => {
                        for rid in rows {
                            if !fk_mark_visited(child.id, rid, fk_disc) {
                                self.dml_stats.record_fk_cycle_short_circuit();
                                continue;
                            }
                            let setcols = inbound.child_col_idx.clone();
                            let changed = setcols.clone();
                            self.update_row_by_id_with_changed_columns(
                                txn,
                                &child,
                                rid,
                                Some(&changed),
                                move |old| {
                                    let mut row = old.to_vec();
                                    for &ci in &setcols {
                                        row[ci] = Value::Null;
                                    }
                                    Ok(row)
                                },
                            )?;
                        }
                    }
                }
            }
            Ok(())
        })
    }

    // =======================================================================
    // Reverse-dependency guards (DDL)
    // =======================================================================

    /// Reject DROP TABLE / TRUNCATE when another table references this one
    /// via a foreign key. Self-references are ignored: DROP removes the
    /// whole table, and MySQL allows TRUNCATE on a self-referencing table.
    pub(crate) fn fk_reject_table_referenced(
        &self,
        table: &TableSchema,
        truncate: bool,
    ) -> Result<(), QueryError> {
        for inbound in &table.fk_referenced_by {
            let is_self = inbound.child_table_id == table.id;
            if is_self {
                continue;
            }
            return Err(if truncate {
                error(crate::error::catalog::ER_TRUNCATE_ILLEGAL_FK,
                    format!(
                        "Cannot truncate a table referenced in a foreign key constraint (`{}`, CONSTRAINT `{}`)",
                        inbound.child_table_key, inbound.fk_name
                    ),
                )
            } else {
                error(crate::error::catalog::ER_FK_CANNOT_DROP_PARENT,
                    format!(
                        "Cannot drop table '{}' referenced by a foreign key constraint '{}' on table '{}'.",
                        table.name, inbound.fk_name, inbound.child_table_key
                    ),
                )
            });
        }
        Ok(())
    }

    /// Reject DROP COLUMN when the column participates in a foreign key
    /// (this table's own FK, child side; or an inbound FK, parent side).
    pub(crate) fn fk_reject_column_referenced(
        &self,
        table: &TableSchema,
        column: &str,
    ) -> Result<(), QueryError> {
        for fk in &table.foreign_keys {
            if fk.references_column(column) {
                return Err(error(
                    crate::error::catalog::ER_FK_COLUMN_CANNOT_DROP,
                    format!(
                        "Cannot drop column '{column}': needed in a foreign key constraint '{}'",
                        fk.name
                    ),
                ));
            }
        }
        for inbound in &table.fk_referenced_by {
            if inbound.referenced_col_idx.iter().any(|&ri| {
                table
                    .columns
                    .get(ri)
                    .is_some_and(|c| c.name.eq_ignore_ascii_case(column))
            }) {
                return Err(error(crate::error::catalog::ER_FK_COLUMN_CANNOT_DROP_CHILD,
                    format!(
                        "Cannot drop column '{column}': needed in a foreign key constraint '{}' of table '{}'",
                        inbound.fk_name, inbound.child_table_key
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Reject DROP INDEX when it is the only index able to back a foreign
    /// key — either this table's own FK (child side) or an inbound FK's
    /// referenced key (parent side).
    pub(crate) fn fk_reject_index_drop(
        &self,
        table: &TableSchema,
        index_name: &str,
    ) -> Result<(), QueryError> {
        let dropping_index_id = table
            .indexes
            .iter()
            .find(|index| index.name.eq_ignore_ascii_case(index_name))
            .map(|index| index.index_id);
        // Child side: backing index for one of this table's FKs.
        for fk in &table.foreign_keys {
            if dropping_index_id == Some(fk.backing_index_id) {
                let alt = table.indexes.iter().any(|i| {
                    !i.name.eq_ignore_ascii_case(index_name)
                        && i.state == IndexState::Public
                        && !i.is_fulltext()
                        && i.columns.len() >= fk.columns.len()
                        && fk
                            .columns
                            .iter()
                            .zip(i.columns.iter())
                            .all(|(c, ic)| c.eq_ignore_ascii_case(ic))
                });
                if !alt {
                    return Err(error(
                        crate::error::catalog::ER_DROP_INDEX_FK,
                        format!(
                            "Cannot drop index '{index_name}': needed in a foreign key constraint"
                        ),
                    ));
                }
            }
        }
        // Parent side: index proves an inbound FK's referenced key.
        for inbound in &table.fk_referenced_by {
            let ref_cols: Vec<String> = inbound
                .referenced_col_idx
                .iter()
                .filter_map(|&ri| table.columns.get(ri).map(|c| c.name.clone()))
                .collect();
            if ref_cols.len() != inbound.referenced_col_idx.len() {
                continue;
            }
            let this_covers = table
                .indexes
                .iter()
                .find(|i| i.name.eq_ignore_ascii_case(index_name))
                .is_some_and(|i| {
                    (i.unique || i.primary)
                        && i.columns.len() >= ref_cols.len()
                        && ref_cols
                            .iter()
                            .zip(i.columns.iter())
                            .all(|(r, c)| r.eq_ignore_ascii_case(c))
                });
            if !this_covers {
                continue;
            }
            let alt = table.indexes.iter().any(|i| {
                !i.name.eq_ignore_ascii_case(index_name)
                    && (i.unique || i.primary)
                    && i.state == IndexState::Public
                    && i.columns.len() >= ref_cols.len()
                    && ref_cols
                        .iter()
                        .zip(i.columns.iter())
                        .all(|(r, c)| r.eq_ignore_ascii_case(c))
            });
            if !alt {
                return Err(error(
                    crate::error::catalog::ER_DROP_INDEX_FK,
                    format!("Cannot drop index '{index_name}': needed in a foreign key constraint"),
                ));
            }
        }
        Ok(())
    }

    // =======================================================================
    // ALTER TABLE ADD / DROP FOREIGN KEY
    // =======================================================================

    /// Scan every existing child row and verify a matching parent exists
    /// (non-NULL keys only). Used by ALTER ADD FOREIGN KEY (error 1452 on
    /// an orphan).
    fn fk_scan_orphans(
        &self,
        child: &TableSchema,
        fk: &ForeignKeySchema,
        txn: TxnView,
    ) -> Result<(), QueryError> {
        let parent_key = table_key(&fk.referenced_database, &fk.referenced_table);
        let parent = self
            .catalog_read()?
            .tables
            .get(&parent_key)
            .cloned()
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_FK_CANNOT_OPEN_PARENT,
                    format!(
                        "Failed to open the referenced table '{}'",
                        fk.referenced_table
                    ),
                )
            })?;
        let index = fk_find_referenced_index(&parent, &fk.referenced_columns)
            .ok_or_else(|| {
                error(crate::error::catalog::ER_FK_NO_INDEX_PARENT,
                    format!(
                        "Failed to add the foreign key constraint. Missing index for constraint '{}' in the referenced table '{}'",
                        fk.name, fk.referenced_table
                    ),
                )
            })?
            .clone();
        if fk_ddl_index_merge_enabled() && !self.has_pending_index_repairs() {
            if let Some(child_index) = child
                .indexes
                .iter()
                .find(|candidate| candidate.index_id == fk.backing_index_id)
                .filter(|candidate| {
                    candidate.state == IndexState::Public
                        && !candidate.has_expression_parts()
                        && !candidate.has_multi_valued_parts()
                        && (0..fk.child_col_idx.len())
                            .all(|position| candidate.column_prefix_len(position).is_none())
                })
            {
                return self.fk_scan_orphans_index_merge(child, child_index, &parent, &index, fk);
            }
        }
        // Stream only the FK columns and collapse repeated references before
        // touching the parent. The old path materialised every full child row
        // and performed one B-tree descent + parent heap decode per row. On a
        // loaded fact table that turns a linear validation into millions of
        // random probes even though many rows reference the same parent.
        let mut unique_keys = crate::util::fast_hash::ah_hash_set();
        self.scan_rows_columns_streaming(
            &child.qualified_name(),
            txn,
            &fk.child_col_idx,
            |values| {
                // MATCH SIMPLE: any NULL child part satisfies the FK.
                if values.iter().any(|value| matches!(value, Value::Null)) {
                    return Ok(());
                }
                unique_keys.insert(fk_build_key(
                    &parent,
                    &index,
                    &fk.referenced_columns,
                    values,
                )?);
                Ok(())
            },
        )?;
        if unique_keys.is_empty() {
            return Ok(());
        }
        let mut keys: Vec<IndexKey> = unique_keys.into_iter().collect();
        keys.sort_unstable();
        let index_handle = self.cached_index_handle(parent.id, &index.name)?;
        let matched = self.keys_with_visible_match_sorted_with_handle(
            &index_handle,
            &parent,
            &index,
            &keys,
            txn,
        )?;
        if matched.iter().any(|found| !*found) {
            return Err(fk_child_violation_err(child, fk));
        }
        Ok(())
    }

    /// Validate an existing FK by merge-walking the already-sorted child
    /// backing index and referenced UNIQUE index. ADD FK holds child X and
    /// parent S table locks for this entire walk, so both indexes are stable
    /// and contain no concurrent uncommitted DML. This turns N random parent
    /// probes into two sequential leaf scans and keeps memory bounded to two
    /// cursor chunks.
    fn fk_scan_orphans_index_merge(
        &self,
        child: &TableSchema,
        child_index: &IndexSchema,
        parent: &TableSchema,
        parent_index: &IndexSchema,
        fk: &ForeignKeySchema,
    ) -> Result<(), QueryError> {
        const CHUNK: usize = 16 * 1024;
        let prefix_len = fk.child_col_idx.len();
        let mut child_cursor = self.range_index_entries_cursor(child, child_index, None, None)?;
        let mut parent_cursor =
            self.range_index_entries_cursor(parent, parent_index, None, None)?;
        let mut parent_chunk = parent_cursor.next_chunk(CHUNK)?;
        let mut parent_pos = 0usize;

        loop {
            let child_chunk = child_cursor.next_chunk(CHUNK)?;
            if child_chunk.is_empty() {
                return Ok(());
            }
            for (child_key, _) in &child_chunk {
                let child_parts = child_key.0.get(..prefix_len).ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "foreign key backing index key is truncated",
                    )
                })?;
                if child_parts.iter().any(|part| matches!(part, KeyPart::Null)) {
                    continue;
                }
                loop {
                    if parent_pos >= parent_chunk.len() {
                        parent_chunk = parent_cursor.next_chunk(CHUNK)?;
                        parent_pos = 0;
                        if parent_chunk.is_empty() {
                            return Err(fk_child_violation_err(child, fk));
                        }
                    }
                    let parent_key = &parent_chunk[parent_pos].0;
                    match parent_key.0.as_slice().cmp(child_parts) {
                        std::cmp::Ordering::Less => parent_pos += 1,
                        std::cmp::Ordering::Equal => break,
                        std::cmp::Ordering::Greater => {
                            return Err(fk_child_violation_err(child, fk));
                        }
                    }
                }
            }
        }
    }

    /// `ALTER TABLE ... ADD [CONSTRAINT] FOREIGN KEY ...`.
    pub fn add_foreign_key(&self, table_name: &str, def: ForeignKeyDef) -> Result<(), QueryError> {
        let (database, table_only) = split_table_name(DEFAULT_DATABASE, table_name);
        if table_ddl_schema_is_read_only(&database) {
            return Err(error(
                crate::error::catalog::ER_DBACCESS_DENIED_ERROR,
                format!("access denied for database '{database}'"),
            ));
        }
        let key = table_key(&database, &table_only);

        // Phase 1: validate + decide whether a backing index is needed.
        let (mut fk_schema, backing_index) = {
            let catalog = self.catalog_read()?;
            let child = catalog.tables.get(&key).cloned().ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NO_SUCH_TABLE,
                    format!("Table '{table_name}' doesn't exist"),
                )
            })?;
            if child.foreign_keys.iter().any(|f| {
                def.name
                    .as_deref()
                    .is_some_and(|n| f.name.eq_ignore_ascii_case(n))
            }) {
                return Err(error(
                    crate::error::catalog::ER_FK_DUP_NAME,
                    format!(
                        "Duplicate foreign key constraint name '{}'",
                        def.name.as_deref().unwrap_or_default()
                    ),
                ));
            }
            let mut used: BTreeSet<String> =
                child.foreign_keys.iter().map(|f| f.name.clone()).collect();
            let mut index_names: BTreeSet<String> = child
                .indexes
                .iter()
                .map(|i| normalize_name(&i.name))
                .collect();
            let mut gen = 1usize;
            fk_build_one(
                &catalog,
                &child,
                &[],
                def,
                &mut used,
                &mut gen,
                &mut index_names,
            )?
        };

        // Phase 2: create the backing index (synchronous build) if needed.
        let created_backing_index = backing_index.as_ref().map(|index| index.name.clone());
        if let Some(index) = backing_index {
            self.add_index(table_name, index)?;
        }
        fk_schema.backing_index_id = self
            .table(table_name)?
            .indexes
            .iter()
            .find(|index| index.name.eq_ignore_ascii_case(&fk_schema.backing_index))
            .map(|index| index.index_id)
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_CANNOT_ADD_FOREIGN,
                    format!(
                        "backing index '{}' disappeared while adding foreign key",
                        fk_schema.backing_index
                    ),
                )
            })?;

        // Phase 3+4 run under stable table locks. Child X prevents a row from
        // slipping between validation and catalog publish; parent S prevents
        // a referenced row from disappearing during the index merge.
        let ddl_txn = self.begin_read_only_txn()?;
        let result = (|| {
            let child = self
                .catalog_read()?
                .tables
                .get(&key)
                .cloned()
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_NO_SUCH_TABLE,
                        format!("Table '{table_name}' doesn't exist"),
                    )
                })?;
            let parent_key = table_key(&fk_schema.referenced_database, &fk_schema.referenced_table);
            let parent = self
                .catalog_read()?
                .tables
                .get(&parent_key)
                .cloned()
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_FK_CANNOT_OPEN_PARENT,
                        format!(
                            "Failed to open the referenced table '{}'",
                            fk_schema.referenced_table
                        ),
                    )
                })?;
            if parent.id < child.id {
                self.acquire_table_shared(ddl_txn.txn_id, parent.id)?;
                self.acquire_table_exclusive(ddl_txn.txn_id, child.id)?;
            } else {
                self.acquire_table_exclusive(ddl_txn.txn_id, child.id)?;
                if parent.id != child.id {
                    self.acquire_table_shared(ddl_txn.txn_id, parent.id)?;
                }
            }
            self.fk_scan_orphans(&child, &fk_schema, ddl_txn)?;

            // Phase 4: install the FK, rebuild runtime caches, persist while
            // the same locks still close the validation/publish gap.
            let mut catalog = self.catalog_write()?;
            let table_arc = catalog.tables.get_mut(&key).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NO_SUCH_TABLE,
                    format!("Table '{table_name}' doesn't exist"),
                )
            })?;
            let table = Arc::make_mut(table_arc);
            if table
                .foreign_keys
                .iter()
                .any(|f| f.name.eq_ignore_ascii_case(&fk_schema.name))
            {
                return Err(error(
                    crate::error::catalog::ER_FK_DUP_NAME,
                    format!("Duplicate foreign key constraint name '{}'", fk_schema.name),
                ));
            }
            table.foreign_keys.push(fk_schema);
            table.bump_schema_version();
            let table_id = table.id;
            build_fk_runtime(&mut catalog);
            drop(catalog);
            self.note_catalog_table(table_id);
            self.commit_catalog_txn()
        })();
        let release = self.rollback_txn(ddl_txn.txn_id);
        let outcome = match (result, release) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(err), _) => Err(err),
            (Ok(()), Err(err)) => Err(err),
        };
        if outcome.is_err() {
            if let Some(index_name) = created_backing_index {
                let _ = self.drop_index(table_name, &index_name);
            }
        }
        outcome
    }

    /// `ALTER TABLE ... DROP FOREIGN KEY name`. The backing index is kept
    /// (MySQL behaviour). Returns whether a constraint was removed.
    pub fn drop_foreign_key(&self, table_name: &str, name: &str) -> Result<(), QueryError> {
        if self.drop_foreign_key_if_present(table_name, name)? {
            Ok(())
        } else {
            Err(error(
                crate::error::catalog::ER_CANT_DROP_FIELD_OR_KEY,
                format!("Can't DROP FOREIGN KEY '{name}'; check that it exists"),
            ))
        }
    }

    /// Drop a foreign key by name if present; returns `true` if one was
    /// removed. Shared by `DROP FOREIGN KEY` and the `DROP CONSTRAINT`
    /// resolver (which falls back to FKs when the name is not a CHECK).
    pub(crate) fn drop_foreign_key_if_present(
        &self,
        table_name: &str,
        name: &str,
    ) -> Result<bool, QueryError> {
        let (database, table_only) = split_table_name(DEFAULT_DATABASE, table_name);
        if table_ddl_schema_is_read_only(&database) {
            return Err(error(
                crate::error::catalog::ER_DBACCESS_DENIED_ERROR,
                format!("access denied for database '{database}'"),
            ));
        }
        let key = table_key(&database, &table_only);
        let table_id;
        {
            let mut catalog = self.catalog_write()?;
            let table_arc = catalog.tables.get_mut(&key).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NO_SUCH_TABLE,
                    format!("Table '{table_name}' doesn't exist"),
                )
            })?;
            let table = Arc::make_mut(table_arc);
            let before = table.foreign_keys.len();
            table
                .foreign_keys
                .retain(|fk| !fk.name.eq_ignore_ascii_case(name));
            if table.foreign_keys.len() == before {
                return Ok(false);
            }
            table.bump_schema_version();
            table_id = table.id;
            build_fk_runtime(&mut catalog);
        }
        self.note_catalog_table(table_id);
        self.commit_catalog_txn()?;
        Ok(true)
    }
}
