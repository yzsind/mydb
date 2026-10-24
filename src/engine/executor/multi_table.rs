//! M5.C1 — Multi-table UPDATE / DELETE.
//!
//! Implements MySQL's multi-table forms:
//!   * `UPDATE t1 [JOIN t2 ON ...]+ SET t1.col = ..., t2.col = ... [WHERE ...]`
//!   * `UPDATE t1, t2 SET ... WHERE ...` (comma joins; WHERE supplies the equi-join)
//!   * `DELETE t1[, t2]+ FROM t1 [JOIN t2 ON ...]+ [WHERE ...]`
//!   * `DELETE t1, t2 FROM t1, t2 WHERE t1.k = t2.k`
//!
//! Only the multi-table case lands here — single-table UPDATE / DELETE stays
//! on the fast path in [`super::dml`]. The executor materialises rows from
//! every participating table (a nested-loop join over `scan_rows`), then for
//! each combined row that satisfies all join conditions + WHERE:
//!   * UPDATE — for every target participant, dedupes by `row_id` and applies
//!     each SET assignment via `Database::update_row_by_id_with_changed_columns`.
//!   * DELETE — dedupes target `row_id`s and removes them via
//!     `Database::delete_row_by_id`.
//!
//! Restrictions surfaced as a clear unsupported error:
//!   * Only INNER / CROSS joins. LEFT/RIGHT/SEMI/ANTI must use single-table.
//!   * No ORDER BY / LIMIT (MySQL also rejects these on multi-table forms).
//!   * No view-check predicate (multi-table UPDATE through a view is rare and
//!     hard to validate row-by-row).
//!   * No subqueries inside ON / WHERE / SET expressions.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use crate::engine::expr::truthy;
use crate::engine::expr_program::CompiledTableExpr;
use crate::engine::session_registry::SessionRegistry;
use crate::parser::{Delete, Expr, Join, JoinType, Update};
use crate::storage::{error, Database, RowId, TableSchema, TxnView};
use crate::types::{QueryError, Value};

use super::dml::changed_columns_after_trigger;
use super::{
    fire_after_delete, fire_after_update, fire_before_delete, fire_before_update,
    table_has_delete_triggers, table_has_update_triggers,
};
use crate::engine::api::{QueryResponse, SessionContext};

/// One table participating in the multi-table join: cached schema + all
/// visible rows materialised once at the top of execution. `base` is the
/// offset of this table's columns inside the joined combined row.
struct Participant {
    /// Lower-case alias used for `t.col` qualifier resolution. Defaults to
    /// the lowercase table name when no alias is written.
    alias: String,
    explicit_alias: bool,
    /// Storage key used by `Database::scan_rows` / `delete_row_by_id` —
    /// usually `"schema.name"`.
    storage_key: String,
    schema: TableSchema,
    rows: Vec<StoredJoinRow>,
    partitions: Vec<String>,
    width: usize,
    base: usize,
}

/// `(storage_id, row_id, values)` tuple cloned out of `StoredRow`. Owning
/// these locally avoids holding a `Database` borrow during the nested-loop
/// join. `storage_id` records the origin partition so a later by-id write
/// targets the exact partition (a bare `row_id` collides across partitions).
struct StoredJoinRow {
    storage_id: u64,
    row_id: RowId,
    values: Vec<Value>,
}

/// Multi-table evaluation context. Built once before the join enumeration
/// starts; immutable thereafter.
struct MultiContext {
    expression_schema: TableSchema,
    participants: Vec<Participant>,
    /// `(alias_lower, col_lower) -> position in the combined values vector`.
    qualified: HashMap<(String, String), usize>,
    /// `col_lower -> position in the combined values vector` — only present
    /// when the column name is unique across all participants. Ambiguous
    /// names appear in `ambiguous` and require a qualifier.
    unqualified: HashMap<String, usize>,
    ambiguous: HashSet<String>,
}

impl MultiContext {
    fn participant_for_combined(&self, combined_idx: usize) -> (usize, usize) {
        for (i, p) in self.participants.iter().enumerate() {
            if combined_idx >= p.base && combined_idx < p.base + p.width {
                return (i, combined_idx - p.base);
            }
        }
        unreachable!("combined column index out of range");
    }

    fn resolve_unqualified(&self, name: &str) -> Result<usize, QueryError> {
        let lc = name.to_ascii_lowercase();
        if self.ambiguous.contains(&lc) {
            return Err(error(
                crate::error::catalog::ER_NON_UNIQ_ERROR,
                format!("Column '{name}' in field list is ambiguous"),
            ));
        }
        self.unqualified.get(&lc).copied().ok_or_else(|| {
            error(
                crate::error::catalog::ER_BAD_FIELD_ERROR,
                format!("unknown column '{name}'"),
            )
        })
    }

    fn resolve_qualified(&self, qualifier: &str, name: &str) -> Result<usize, QueryError> {
        let key = (qualifier.to_ascii_lowercase(), name.to_ascii_lowercase());
        self.qualified.get(&key).copied().ok_or_else(|| {
            error(
                crate::error::catalog::ER_BAD_FIELD_ERROR,
                format!("unknown column '{qualifier}.{name}'"),
            )
        })
    }
}

/// B1 lost-update fix helper: produce the new row to write by starting from
/// `current` (the row re-read under the row lock inside the storage layer)
/// and overwriting only the SET columns (`changed`) with our computed values.
/// Columns not in `changed` keep the freshly-read committed values, so a
/// concurrent committed write to a non-target column is never clobbered by
/// the build-context snapshot. `new_values` and `current` are both full-width.
fn patch_changed_columns(current: &[Value], new_values: &[Value], changed: &[usize]) -> Vec<Value> {
    let mut row = current.to_vec();
    for &c in changed {
        if c < row.len() && c < new_values.len() {
            row[c] = new_values[c].clone();
        }
    }
    row
}

fn build_context(
    db: &Arc<Database>,
    _txn: TxnView,
    primary_table: &str,
    primary_alias: Option<&str>,
    primary_partitions: &[String],
    joins: &[Join],
) -> Result<MultiContext, QueryError> {
    let mut participants: Vec<Participant> = Vec::with_capacity(1 + joins.len());
    let mut seen_aliases: HashSet<String> = HashSet::new();

    let push_participant = |participants: &mut Vec<Participant>,
                            seen: &mut HashSet<String>,
                            storage_key: &str,
                            alias: Option<&str>,
                            partitions: &[String]|
     -> Result<(), QueryError> {
        let schema_arc = db.table(storage_key)?;
        let schema: TableSchema = (*schema_arc).clone();
        let alias_lc = alias
            .map(|a| a.to_ascii_lowercase())
            .unwrap_or_else(|| schema.name.to_ascii_lowercase());
        if !seen.insert(alias_lc.clone()) {
            return Err(error(
                crate::error::catalog::ER_NONUNIQ_TABLE,
                format!("Not unique table/alias: '{alias_lc}'"),
            ));
        }
        let width = schema.columns.len();
        let base = participants.iter().map(|p| p.width).sum::<usize>();
        participants.push(Participant {
            alias: alias_lc,
            explicit_alias: alias.is_some(),
            storage_key: storage_key.to_string(),
            schema,
            rows: Vec::new(),
            partitions: partitions.to_vec(),
            width,
            base,
        });
        Ok(())
    };

    push_participant(
        &mut participants,
        &mut seen_aliases,
        primary_table,
        primary_alias,
        primary_partitions,
    )?;
    for join in joins {
        push_participant(
            &mut participants,
            &mut seen_aliases,
            &join.table.name,
            join.table.alias.as_deref(),
            &join.table.partitions,
        )?;
    }

    let mut qualified: HashMap<(String, String), usize> = HashMap::new();
    let mut unqualified: HashMap<String, usize> = HashMap::new();
    let mut ambiguous: HashSet<String> = HashSet::new();
    for p in &participants {
        for (i, column) in p.schema.columns.iter().enumerate() {
            // Skip dropped columns — they have no logical position.
            if !matches!(column.state, crate::storage::ColumnState::Dropped) {
                let col_lc = column.name.to_ascii_lowercase();
                let combined = p.base + i;
                qualified.insert((p.alias.clone(), col_lc.clone()), combined);
                if ambiguous.contains(&col_lc) {
                    continue;
                }
                if let Some(prev) = unqualified.insert(col_lc.clone(), combined) {
                    if prev != combined {
                        ambiguous.insert(col_lc);
                    }
                }
            }
        }
    }

    let mut expression_schema = participants[0].schema.clone();
    expression_schema.columns = participants
        .iter()
        .flat_map(|p| {
            p.schema
                .columns
                .iter()
                .enumerate()
                .map(move |(index, column)| {
                    let mut column = column.clone();
                    column.name = slot_name(p.base + index);
                    column.column_id = (p.base + index + 1) as u32;
                    column.collation = Some(
                        crate::engine::expr_program::resolve_column_collation_id(
                            &p.schema,
                            &p.schema.columns[index],
                        )
                        .and_then(crate::charset::resolve_collation_id)
                        .map_or(p.schema.collation.as_str(), |info| info.name)
                        .to_string(),
                    );
                    column.generated_expr = None;
                    column
                })
        })
        .collect();
    expression_schema.indexes.clear();
    expression_schema.primary_key.clear();
    expression_schema.check_constraints.clear();
    expression_schema.partitioning = None;
    expression_schema.build_column_index();
    Ok(MultiContext {
        expression_schema,
        participants,
        qualified,
        unqualified,
        ambiguous,
    })
}

fn slot_name(index: usize) -> String {
    format!("__dml_slot_{index}")
}

struct BoundMultiExpr {
    expression: Expr,
    compiled: CompiledTableExpr,
}

impl MultiContext {
    /// Bind once against a full catalog-derived schema. Never replace columns
    /// with values: comparison, ENUM/SET and binary conversion require types.
    fn bind(
        &self,
        db: &Database,
        session: &SessionContext,
        expr: &Expr,
        target: Option<usize>,
    ) -> Result<BoundMultiExpr, QueryError> {
        self.bind_visible(db, session, expr, target, self.participants.len())
    }

    fn bind_visible(
        &self,
        db: &Database,
        session: &SessionContext,
        expr: &Expr,
        target: Option<usize>,
        visible: usize,
    ) -> Result<BoundMultiExpr, QueryError> {
        let resolve_name = |name: &str| {
            if visible == self.participants.len() {
                return self.resolve_unqualified(name);
            }
            let name_lc = name.to_ascii_lowercase();
            let mut found = None;
            for participant in &self.participants[..visible] {
                if let Some(&index) = self
                    .qualified
                    .get(&(participant.alias.clone(), name_lc.clone()))
                {
                    if found.replace(index).is_some() {
                        return Err(crate::error::catalog::ER_NON_UNIQ_ERROR
                            .error(format!("Column '{name}' in ON clause is ambiguous")));
                    }
                }
            }
            found.ok_or_else(|| {
                crate::error::catalog::ER_BAD_FIELD_ERROR
                    .error(format!("Unknown column '{name}' in ON clause"))
            })
        };
        let resolve_parts = |parts: &[String]| {
            if let [name] = parts {
                return resolve_name(name);
            }
            let index = self.resolve_parts(parts)?;
            let (participant, _) = self.participant_for_combined(index);
            if participant >= visible {
                return Err(crate::error::catalog::ER_BAD_FIELD_ERROR
                    .error(format!("Unknown column '{}' in ON clause", parts.join("."))));
            }
            Ok(index)
        };
        let expression = crate::parser::visit::try_map_value_expr(expr, &mut |node| {
            let column = match node {
                Expr::Identifier(name) => Some(resolve_name(name)?),
                Expr::CompoundIdentifier(parts) => Some(resolve_parts(parts)?),
                Expr::Default(Some(name)) => {
                    return Ok(Some(Expr::Default(Some(slot_name(resolve_name(name)?)))))
                }
                Expr::DefaultQualified(parts) => {
                    return Ok(Some(Expr::Default(Some(slot_name(resolve_parts(parts)?)))))
                }
                Expr::Default(None) => {
                    return target
                        .map(|index| Some(Expr::Default(Some(slot_name(index)))))
                        .ok_or_else(|| {
                            error(
                                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                                "bare DEFAULT requires an assignment target",
                            )
                        })
                }
                Expr::Function { name, .. } => {
                    if !crate::engine::cbo::function_cost::is_known_builtin_function(name)
                        && crate::parser::IntrinsicFunction::from_name(name).is_none()
                        && db
                            .routine_definition(
                                &session.current_database,
                                crate::storage::RoutineKindSchema::Function,
                                name,
                            )
                            .is_none()
                    {
                        return Err(error(
                            crate::error::catalog::ER_SP_DOES_NOT_EXIST,
                            format!("function '{name}' is not supported"),
                        ));
                    }
                    if crate::parser::is_aggregate_function(name) {
                        return Err(error(
                            crate::error::catalog::ER_INVALID_GROUP_FUNC_USE,
                            "Invalid use of group function",
                        ));
                    }
                    None
                }
                Expr::Subquery(_)
                | Expr::Exists { .. }
                | Expr::InSubquery { .. }
                | Expr::QuantifiedComparison { .. }
                | Expr::WindowFunction { .. }
                | Expr::Grouping(_)
                | Expr::GroupKeyRef { .. } => {
                    return Err(error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        "expression is not supported in multi-table DML",
                    ))
                }
                _ => None,
            };
            Ok(column.map(|index| Expr::Identifier(slot_name(index))))
        })?;
        crate::engine::expr::validate_expr_collations(&expression, &self.expression_schema)?;
        crate::engine::expr::validate_expr_numeric_operands(&expression, &self.expression_schema)?;
        crate::engine::expr::validate_expr_temporal_literals(&expression, &self.expression_schema)?;
        let compiled = CompiledTableExpr::compile_once(&expression, &self.expression_schema);
        Ok(BoundMultiExpr {
            expression,
            compiled,
        })
    }

    fn resolve_parts(&self, parts: &[String]) -> Result<usize, QueryError> {
        match parts {
            [column] => self.resolve_unqualified(column),
            [.., qualifier, column] => self.resolve_qualified(qualifier, column),
            _ => Err(error(
                crate::error::catalog::ER_BAD_FIELD_ERROR,
                "empty column reference",
            )),
        }
    }

    /// Open sources only after every expression and target has been bound.
    fn load_rows(&mut self, db: &Database, txn: TxnView) -> Result<(), QueryError> {
        for participant in &mut self.participants {
            let _outer_scope = super::partition_router::suspend_pruned_partitions();
            let ids = super::partition_clause::resolve_partition_clause(
                db,
                &participant.storage_key,
                &participant.partitions,
            )?;
            let _scope = ids.map(super::partition_router::push_pruned_partitions);
            participant.rows = db
                .scan_rows_with_storage(&participant.storage_key, txn)?
                .into_iter()
                .map(|(storage_id, row)| StoredJoinRow {
                    storage_id,
                    row_id: row.row_id,
                    values: row.values,
                })
                .collect();
        }
        Ok(())
    }
}

fn eval_in_context(
    expr: &BoundMultiExpr,
    ctx: &MultiContext,
    combined: &[Value],
) -> Result<Value, QueryError> {
    expr.compiled.eval(&ctx.expression_schema, combined)
}

/// Sentinel `row_id` indicating "no match" for a participant whose join is
/// LEFT and produced an outer (NULL-padded) tuple at this depth. The
/// visitor in UPDATE / DELETE must NOT try to mutate this participant's
/// row when its index slot equals this sentinel.
pub(super) const OUTER_NULL_PARTICIPANT: usize = usize::MAX;

/// Walk the N-way nested-loop join. For every combined row that satisfies
/// the per-join `ON` conditions and (optionally) `where_predicate`, calls
/// `visit(combined_values, per_participant_row_indices)`. Supports
/// LEFT-JOIN outer rows: when a LEFT join produces no inner match, the
/// joined-side participant's slot in `indices` carries the
/// [`OUTER_NULL_PARTICIPANT`] sentinel and the corresponding combined columns
/// are NULL.
fn enumerate_joined<F>(
    ctx: &MultiContext,
    join_types: &[JoinType],
    join_conditions: &[Option<BoundMultiExpr>],
    where_predicate: Option<&BoundMultiExpr>,
    mut visit: F,
) -> Result<(), QueryError>
where
    F: FnMut(&[Value], &[usize]) -> Result<(), QueryError>,
{
    let n = ctx.participants.len();
    let total_width: usize = ctx.participants.iter().map(|p| p.width).sum();
    let mut combined = vec![Value::Null; total_width];
    let mut indices = vec![0usize; n];
    enumerate_recursive(
        ctx,
        join_types,
        join_conditions,
        where_predicate,
        0,
        &mut indices,
        &mut combined,
        &mut visit,
    )
}

fn enumerate_recursive<F>(
    ctx: &MultiContext,
    join_types: &[JoinType],
    join_conditions: &[Option<BoundMultiExpr>],
    where_predicate: Option<&BoundMultiExpr>,
    depth: usize,
    indices: &mut [usize],
    combined: &mut [Value],
    visit: &mut F,
) -> Result<(), QueryError>
where
    F: FnMut(&[Value], &[usize]) -> Result<(), QueryError>,
{
    crate::engine::cancel::check_cancelled()?;
    let n = ctx.participants.len();
    if depth == n {
        if let Some(predicate) = where_predicate {
            let v = eval_in_context(predicate, ctx, combined)?;
            if !truthy(&v) {
                return Ok(());
            }
        }
        return visit(combined, indices);
    }
    let p = &ctx.participants[depth];
    let base = p.base;
    let mut matched_any = false;
    for (idx, stored) in p.rows.iter().enumerate() {
        indices[depth] = idx;
        for (i, v) in stored.values.iter().enumerate() {
            combined[base + i] = v.clone();
        }
        // Evaluate this participant's join `ON` condition (depth >= 1).
        // depth 0 has no incoming join clause.
        if depth >= 1 {
            if let Some(cond) = &join_conditions[depth] {
                let v = eval_in_context(cond, ctx, combined)?;
                if !truthy(&v) {
                    continue;
                }
            }
        }
        matched_any = true;
        enumerate_recursive(
            ctx,
            join_types,
            join_conditions,
            where_predicate,
            depth + 1,
            indices,
            combined,
            visit,
        )?;
    }
    // LEFT JOIN outer-row emission. When no inner-side row passed `ON` (or
    // the inner side is empty), MySQL still emits a tuple with the inner
    // side NULL-padded. WHERE is evaluated against this NULL-padded tuple
    // — the common `WHERE b.id IS NULL` pattern then selects rows that
    // had no match.
    if depth >= 1 && !matched_any && matches!(join_types[depth], JoinType::Left) {
        indices[depth] = OUTER_NULL_PARTICIPANT;
        for i in 0..p.width {
            combined[base + i] = Value::Null;
        }
        enumerate_recursive(
            ctx,
            join_types,
            join_conditions,
            where_predicate,
            depth + 1,
            indices,
            combined,
            visit,
        )?;
    }
    Ok(())
}

/// MySQL permits `RIGHT JOIN` in a multi-table UPDATE, but the executor's
/// N-way join only emits LEFT outer rows. Since `A RIGHT JOIN B` is exactly
/// `B LEFT JOIN A`, rewrite the common lone two-table case by swapping the
/// base and the joined table and flipping the type to LEFT. SET targets are
/// resolved by qualified name, so no target remapping is needed; the old
/// base simply becomes the optional (NULL-paddable) side, which is the
/// correct semantics (rows of B always present, A only where matched).
///
/// Only the single, plain (non-derived / non-lateral / non-natural /
/// non-partitioned) RIGHT join is rewritten. Join *chains* containing RIGHT
/// need a full reorder and are left for `validate_join_shape` to reject.
pub(super) fn flip_lone_right_join(stmt: &mut Update) {
    flip_lone_right_join_parts(
        &mut stmt.table,
        &mut stmt.primary_alias,
        &mut stmt.joins,
        !stmt.partitions.is_empty(),
    );
}

pub(super) fn flip_lone_right_join_delete(stmt: &mut Delete) {
    flip_lone_right_join_parts(
        &mut stmt.table,
        &mut stmt.primary_alias,
        &mut stmt.joins,
        !stmt.partitions.is_empty(),
    );
}

/// Normalise a single two-table `a RIGHT JOIN b` into the equivalent
/// `b LEFT JOIN a` by swapping the base and the join relation (the ON predicate
/// and all aliases are preserved, so target resolution by name/alias is
/// unaffected). Only the clean lone-join case is flipped; RIGHT inside a longer
/// chain needs a full reorder and is left for `validate_join_shape` to reject.
/// Shared by the multi-table UPDATE and DELETE paths.
fn flip_lone_right_join_parts(
    table: &mut String,
    primary_alias: &mut Option<String>,
    joins: &mut Vec<Join>,
    has_partitions: bool,
) {
    if joins.len() != 1 || has_partitions {
        return;
    }
    let join = &joins[0];
    if join.join_type != JoinType::Right
        || join.derived.is_some()
        || join.lateral
        || join.natural
        || !join.using_columns.is_empty()
        || !join.table.partitions.is_empty()
    {
        return;
    }
    let mut join = joins.remove(0);
    let old_base = crate::parser::TableRef {
        name: std::mem::take(table),
        alias: primary_alias.take(),
        system_time: None,
        index_hints: Vec::new(),
        partitions: Vec::new(),
        column_aliases: Vec::new(),
        generate_series: None,
    };
    *table = std::mem::take(&mut join.table.name);
    *primary_alias = join.table.alias.take();
    joins.push(Join {
        table: old_base,
        join_type: JoinType::Left,
        condition: join.condition.take(),
        using_columns: Vec::new(),
        natural: false,
        derived: None,
        straight: false,
        lateral: false,
    });
}

fn validate_join_shape(joins: &[Join], kind: &str) -> Result<(), QueryError> {
    for join in joins {
        match join.join_type {
            JoinType::Inner | JoinType::Cross | JoinType::Left => {}
            JoinType::Right => {
                // RIGHT JOIN in multi-table DML is rare and tricky to swap to
                // LEFT (the participants list and targets are ordered). Reject
                // for now with a clear hint; LEFT JOIN covers the typical
                // `DELETE a FROM a LEFT JOIN b WHERE b.id IS NULL` pattern.
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    format!(
                        "multi-table {kind} does not support RIGHT JOIN — \
                         rewrite as LEFT JOIN with the table order reversed"
                    ),
                ));
            }
            _ => {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    format!("multi-table {kind} supports only INNER / CROSS / LEFT joins"),
                ));
            }
        }
        if join.derived.is_some() {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("multi-table {kind} does not support derived/subquery sources"),
            ));
        }
        // NATURAL / USING are normalised away by
        // `normalize_natural_using_joins` before validate_join_shape
        // runs, so by this point a residual NATURAL/USING marker means
        // normalisation skipped the join (no common columns) — fall
        // back to the historical reject so silent NATURAL-no-op
        // doesn't introduce a Cartesian product on its own.
        if join.natural {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("multi-table {kind}: NATURAL JOIN found no shared columns to coalesce"),
            ));
        }
    }
    Ok(())
}

/// Replay `planner::resolve_join_using_natural` on a multi-table
/// UPDATE/DELETE so its NATURAL / USING joins resolve to plain `ON`
/// predicates before the dispatch reaches `validate_join_shape`.
///
/// Wraps the DML's table + joins in an ephemeral `Select` because the
/// planner helper expects that shape; the post-call `joins` payload is
/// copied back into the DML statement. The helper only mutates joins
/// (USING list, ON condition, natural flag) — no other Select fields
/// are read or assigned to from the temp wrapper.
pub(super) fn normalize_natural_using_joins(
    db: &Arc<Database>,
    base: &crate::parser::TableRef,
    joins: &mut Vec<Join>,
) -> Result<(), QueryError> {
    if joins
        .iter()
        .all(|j| !j.natural && j.using_columns.is_empty())
    {
        return Ok(());
    }
    // The planner helper signs for `&mut Select`, so wrap the DML's
    // (table, joins) in a throwaway Select whose other fields are all
    // empty defaults. The helper only mutates `joins` + walks `derived`
    // (we pass None) + reads `from[0]` — every other field is left
    // untouched.
    let mut wrapper = crate::parser::Select {
        from: vec![base.clone()],
        joins: std::mem::take(joins),
        ..crate::parser::Select::default()
    };
    crate::planner::resolve_join_using_natural(db, &mut wrapper)?;
    *joins = wrapper.joins;
    Ok(())
}

pub(super) fn execute_multi_update(
    db: &Arc<Database>,
    session: &mut SessionContext,
    txn: TxnView,
    mut stmt: Update,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    crate::engine::cancel::check_cancelled()?;
    if stmt.view_check_predicate.is_some() {
        return Err(error(
            crate::error::catalog::ER_VIEW_MULTIUPDATE,
            "multi-table UPDATE through a view is not supported",
        ));
    }
    if !stmt.order_by.is_empty() || stmt.limit.is_some() {
        return Err(error(
            crate::error::catalog::ER_WRONG_USAGE,
            "ORDER BY/LIMIT is not allowed in multi-table UPDATE",
        ));
    }
    // M3 — fold non-correlated subqueries in the WHERE clause and every SET
    // value into literals before the join machinery runs. `substitute`
    // evaluates SET/WHERE expressions against a per-row join context and
    // cannot execute a subquery there, so a subquery that does not depend on
    // the join rows is resolved once, up front (mirrors single-table
    // `execute_update`). Correlated subqueries reference outer columns that
    // won't resolve here and surface a clear error during materialisation.
    if let Some(selection) = &mut stmt.selection {
        super::dml::materialize_subqueries(db, txn, selection)?;
    }
    for assignment in &mut stmt.assignments {
        super::dml::materialize_subqueries(db, txn, &mut assignment.value)?;
    }
    // M4 — MySQL allows RIGHT JOIN in multi-table UPDATE; rewrite the common
    // lone two-table `a RIGHT JOIN b` into the equivalent `b LEFT JOIN a`
    // (order reversed) so the LEFT-only join machinery handles it. Must run
    // before `base_ref`/normalise/build so they see the rewritten shape.
    flip_lone_right_join(&mut stmt);
    // P1-A2: resolve NATURAL / USING into explicit `ON` predicates so
    // the rest of the multi-table machinery only has to handle plain
    // joins. The base table for the resolver is `stmt.table` (the
    // primary target — `UPDATE a JOIN b ...` parses as Update.table=a).
    let base_ref = crate::parser::TableRef {
        name: stmt.table.clone(),
        alias: stmt.primary_alias.clone(),
        system_time: None,
        index_hints: Vec::new(),
        partitions: Vec::new(),
        column_aliases: Vec::new(),
        generate_series: None,
    };
    normalize_natural_using_joins(db, &base_ref, &mut stmt.joins)?;
    validate_join_shape(&stmt.joins, "UPDATE")?;

    let mut ctx = build_context(
        db,
        txn,
        &stmt.table,
        stmt.primary_alias.as_deref(),
        &stmt.partitions,
        &stmt.joins,
    )?;

    // Collect SET assignments per target participant.
    let mut assignments_per_participant: BTreeMap<usize, Vec<(usize, BoundMultiExpr)>> =
        BTreeMap::new();
    for assignment in &stmt.assignments {
        let combined_idx = match &assignment.table_qualifier {
            Some(qual) => ctx.resolve_qualified(qual, &assignment.column)?,
            None => ctx.resolve_unqualified(&assignment.column)?,
        };
        let (p_idx, col_idx) = ctx.participant_for_combined(combined_idx);
        let participant = &ctx.participants[p_idx];
        if participant.schema.columns[col_idx].generated_expr.is_some() {
            return Err(error(
                crate::error::catalog::ER_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN,
                format!(
                    "value specified for generated column '{}' is not allowed",
                    participant.schema.columns[col_idx].name
                ),
            ));
        }
        assignments_per_participant.entry(p_idx).or_default().push((
            col_idx,
            ctx.bind(db, session, &assignment.value, Some(combined_idx))?,
        ));
    }

    let join_conditions: Vec<Option<BoundMultiExpr>> = std::iter::once(Ok(None))
        .chain(stmt.joins.iter().enumerate().map(|(join_idx, j)| {
            j.condition
                .as_ref()
                .map(|expr| ctx.bind_visible(db, session, expr, None, join_idx + 2))
                .transpose()
        }))
        .collect::<Result<_, QueryError>>()?;
    let selection = stmt
        .selection
        .as_ref()
        .map(|expr| ctx.bind(db, session, expr, None))
        .transpose()?;
    ctx.load_rows(db, txn)?;
    let join_types: Vec<JoinType> = std::iter::once(JoinType::Inner)
        .chain(stmt.joins.iter().map(|j| j.join_type.clone()))
        .collect();

    // A1 — per-target-table trigger activation. A target participant with
    // BEFORE/AFTER UPDATE triggers drops to a row-by-row path that reuses the
    // single-table `fire_*` helpers (single source of truth); participants
    // without triggers keep the batched fast path with zero overhead.
    let participant_has_update_triggers: Vec<bool> = ctx
        .participants
        .iter()
        .map(|p| table_has_update_triggers(db, &p.storage_key))
        .collect();

    // One physical row may appear through multiple target aliases. Merge
    // disjoint assignments and let the first table alias own overlapping
    // columns, independently of hash iteration. Triggers and writes run once.
    struct PlannedUpdate {
        participant: usize,
        aliases: smallvec::SmallVec<[usize; 2]>,
        old: Vec<Value>,
        new: Vec<Value>,
        user_columns: Vec<usize>,
        column_owners: Vec<usize>,
    }
    let mut planned: HashMap<(u64, RowId), PlannedUpdate> = HashMap::new();
    enumerate_joined(
        &ctx,
        &join_types,
        &join_conditions,
        selection.as_ref(),
        |combined, indices| {
            for (&p_idx, assignments) in &assignments_per_participant {
                let row_idx = indices[p_idx];
                if row_idx == OUTER_NULL_PARTICIPANT {
                    continue;
                }
                let participant = &ctx.participants[p_idx];
                let row = &participant.rows[row_idx];
                let entry = planned
                    .entry((row.storage_id, row.row_id))
                    .or_insert_with(|| PlannedUpdate {
                        participant: p_idx,
                        aliases: smallvec::SmallVec::new(),
                        old: row.values.clone(),
                        new: row.values.clone(),
                        user_columns: Vec::new(),
                        column_owners: Vec::new(),
                    });
                if entry.aliases.contains(&p_idx) {
                    continue;
                }
                entry.aliases.push(p_idx);
                entry.participant = entry.participant.min(p_idx);
                for (col_idx, expr) in assignments {
                    if let Some(pos) = entry.user_columns.iter().position(|col| col == col_idx) {
                        if entry.column_owners[pos] < p_idx {
                            continue;
                        }
                        entry.column_owners[pos] = p_idx;
                    } else {
                        entry.user_columns.push(*col_idx);
                        entry.column_owners.push(p_idx);
                    }
                    let value = eval_in_context(expr, &ctx, combined)?;
                    entry.new[*col_idx] = crate::engine::expr::contextual_numeric_literal(
                        value,
                        &expr.expression,
                        &participant.schema.columns[*col_idx].data_type,
                    );
                }
            }
            Ok(())
        },
    )?;

    let mut affected_rows = 0u64;
    let matched_rows = planned.len() as u64;
    let returning_target = if stmt.returning.is_some() {
        if assignments_per_participant.len() != 1 {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "RETURNING requires UPDATE to modify exactly one target table",
            ));
        }
        assignments_per_participant.keys().next().copied()
    } else {
        None
    };
    let mut returning = if let Some(p_idx) = returning_target {
        let participant = &ctx.participants[p_idx];
        Some(super::returning::ReturningCollector::new(
            stmt.returning.as_ref(),
            &participant.schema,
            super::returning::DefaultImage::New,
            Some(&participant.alias),
        )?)
    } else {
        None
    };
    let mut planned: Vec<_> = planned.into_iter().collect();
    planned.sort_unstable_by_key(|((storage_id, row_id), update)| {
        (update.participant, *storage_id, *row_id)
    });
    for ((storage_id, row_id), update) in planned {
        let p_idx = update.participant;
        let old_values = update.old;
        let mut new_values = update.new;
        let schema = ctx.participants[p_idx].schema.clone();
        let on_update = super::dml::on_update_timestamp_columns(&schema, &update.user_columns);
        super::dml::apply_on_update_timestamps(&schema, &mut new_values, &on_update);
        let mut write_columns = update.user_columns.clone();
        write_columns.extend_from_slice(&on_update);
        let changed = Some(write_columns.as_slice());
        if participant_has_update_triggers[p_idx] {
            new_values = fire_before_update(
                db,
                session,
                &schema,
                old_values.clone(),
                new_values,
                registry,
            )?;
            let trigger_changed =
                changed_columns_after_trigger(changed.unwrap_or(&[]), &old_values, &new_values);
            // B1 lost-update fix: the storage callback receives the row
            // re-read *under the row lock* (semi-consistent read). Patch
            // only the changed columns onto that fresh row so columns not
            // in the SET list keep the latest committed values instead of
            // being clobbered by our build-context snapshot. Mirrors the
            // FK-cascade `move |old| {...}` pattern.
            let updated = match db.update_row_by_id_with_changed_columns_noop_at(
                txn,
                &schema,
                Some(storage_id),
                row_id,
                Some(&trigger_changed),
                Some(update.user_columns.as_slice()),
                |current| {
                    Ok(patch_changed_columns(
                        current,
                        &new_values,
                        &trigger_changed,
                    ))
                },
            ) {
                Ok(updated) => updated,
                Err(err) if stmt.ignore && crate::storage::ignorable_update_error(&err) => {
                    crate::engine::api::push_statement_condition(
                        crate::error::ConditionLevel::Warning,
                        &err,
                    );
                    false
                }
                Err(err) => return Err(err),
            };
            if updated {
                fire_after_update(
                    db,
                    session,
                    &schema,
                    old_values.clone(),
                    new_values.clone(),
                    registry,
                )?;
                affected_rows += 1;
                if returning_target == Some(p_idx) {
                    if let Some(collector) = &mut returning {
                        collector.push(Some(&old_values), Some(&new_values))?;
                    }
                }
            }
            continue;
        }
        let updated = match db.update_row_by_id_with_changed_columns_noop_at(
            txn,
            &schema,
            Some(storage_id),
            row_id,
            changed,
            Some(update.user_columns.as_slice()),
            |current| {
                Ok(match changed {
                    Some(cols) => patch_changed_columns(current, &new_values, cols),
                    None => new_values.clone(),
                })
            },
        ) {
            Ok(updated) => updated,
            Err(err) if stmt.ignore && crate::storage::ignorable_update_error(&err) => {
                crate::engine::api::push_statement_condition(
                    crate::error::ConditionLevel::Warning,
                    &err,
                );
                false
            }
            Err(err) => return Err(err),
        };
        if updated {
            affected_rows += 1;
            if returning_target == Some(p_idx) {
                if let Some(collector) = &mut returning {
                    collector.push(Some(&old_values), Some(&new_values))?;
                }
            }
        }
    }
    crate::engine::api::set_statement_matched_rows(matched_rows);
    Ok(match returning {
        Some(collector) => collector.finish(QueryResponse::OkAffected { affected_rows }),
        None => QueryResponse::OkAffected { affected_rows },
    })
}

pub(super) fn execute_multi_delete(
    db: &Arc<Database>,
    session: &mut SessionContext,
    txn: TxnView,
    mut stmt: Delete,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    crate::engine::cancel::check_cancelled()?;
    if !stmt.order_by.is_empty() || stmt.limit.is_some() {
        return Err(error(
            crate::error::catalog::ER_WRONG_USAGE,
            "ORDER BY/LIMIT is not allowed in multi-table DELETE",
        ));
    }
    // Normalise a lone `a RIGHT JOIN b` to `b LEFT JOIN a` before building the
    // participant context (targets resolve by name/alias, unaffected by the
    // flip). RIGHT inside a chain is left for `validate_join_shape` to reject.
    flip_lone_right_join_delete(&mut stmt);
    let base_ref = crate::parser::TableRef {
        name: stmt.table.clone(),
        alias: stmt.primary_alias.clone(),
        system_time: None,
        index_hints: Vec::new(),
        partitions: Vec::new(),
        column_aliases: Vec::new(),
        generate_series: None,
    };
    normalize_natural_using_joins(db, &base_ref, &mut stmt.joins)?;
    validate_join_shape(&stmt.joins, "DELETE")?;

    let mut ctx = build_context(
        db,
        txn,
        &stmt.table,
        stmt.primary_alias.as_deref(),
        &stmt.partitions,
        &stmt.joins,
    )?;

    // Resolve targets. An empty `targets` list (e.g. `DELETE FROM t1 USING ...`)
    // would have already errored at parse; multi-table DELETE always supplies
    // an explicit target list. Once aliased, a participant is named only by
    // its alias, just as in expressions elsewhere in the statement.
    let mut target_participants: Vec<usize> = Vec::with_capacity(stmt.targets.len());
    if stmt.targets.is_empty() {
        // Single explicit target: the primary table itself (the rare
        // `DELETE FROM t1, t2 WHERE ...` form is rejected by the parser).
        target_participants.push(0);
    } else {
        for target in &stmt.targets {
            let lc = target.to_ascii_lowercase();
            if let Some(idx) = ctx.participants.iter().position(|p| {
                p.alias == lc || (!p.explicit_alias && p.storage_key.eq_ignore_ascii_case(&lc))
            }) {
                if !target_participants.contains(&idx) {
                    target_participants.push(idx);
                }
            } else {
                return Err(error(
                    crate::error::catalog::ER_UNKNOWN_TABLE,
                    format!("Unknown table '{target}' in MULTI DELETE"),
                ));
            }
        }
    }

    let join_conditions: Vec<Option<BoundMultiExpr>> = std::iter::once(Ok(None))
        .chain(stmt.joins.iter().enumerate().map(|(join_idx, j)| {
            j.condition
                .as_ref()
                .map(|expr| ctx.bind_visible(db, session, expr, None, join_idx + 2))
                .transpose()
        }))
        .collect::<Result<_, QueryError>>()?;
    let selection = stmt
        .selection
        .as_ref()
        .map(|expr| ctx.bind(db, session, expr, None))
        .transpose()?;
    ctx.load_rows(db, txn)?;
    let join_types: Vec<JoinType> = std::iter::once(JoinType::Inner)
        .chain(stmt.joins.iter().map(|j| j.join_type.clone()))
        .collect();

    // Dedup key includes `storage_id`: same-numbered rows in different
    // partitions are distinct and must both be deletable.
    let mut planned: HashMap<usize, HashSet<(u64, RowId)>> = HashMap::new();
    enumerate_joined(
        &ctx,
        &join_types,
        &join_conditions,
        selection.as_ref(),
        |_combined, indices| {
            for &p_idx in &target_participants {
                let row_idx = indices[p_idx];
                // Outer-null tuple for this target: nothing to delete; this
                // is exactly the case captured by `WHERE b.id IS NULL` in the
                // standard LEFT-JOIN anti-semi-delete pattern.
                if row_idx == OUTER_NULL_PARTICIPANT {
                    continue;
                }
                let storage_id = ctx.participants[p_idx].rows[row_idx].storage_id;
                let row_id = ctx.participants[p_idx].rows[row_idx].row_id;
                planned
                    .entry(p_idx)
                    .or_default()
                    .insert((storage_id, row_id));
            }
            Ok(())
        },
    )?;

    // A1 — per-target-table trigger activation on multi-table DELETE. A
    // target with BEFORE/AFTER DELETE triggers drops to a row-by-row path
    // reusing the single-table `fire_*` helpers; other targets keep the
    // batched fast path.
    let participant_has_delete_triggers: Vec<bool> = ctx
        .participants
        .iter()
        .map(|p| table_has_delete_triggers(db, &p.storage_key))
        .collect();

    if stmt.returning.is_some() && target_participants.len() != 1 {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "RETURNING requires DELETE to modify exactly one target table",
        ));
    }
    let returning_target = stmt.returning.as_ref().map(|_| target_participants[0]);
    let mut returning = if let Some(p_idx) = returning_target {
        let participant = &ctx.participants[p_idx];
        Some(super::returning::ReturningCollector::new(
            stmt.returning.as_ref(),
            &participant.schema,
            super::returning::DefaultImage::Old,
            Some(&participant.alias),
        )?)
    } else {
        None
    };
    let mut affected_rows = 0u64;
    for (p_idx, row_ids) in planned {
        let schema = ctx.participants[p_idx].schema.clone();
        let has_triggers = participant_has_delete_triggers[p_idx];
        for (storage_id, row_id) in row_ids {
            crate::engine::cancel::check_cancelled()?;
            let mut old_image = None;
            let mut trigger_failed = false;
            let mut row_savepoint = None;
            let rollback_cascade = stmt.ignore
                && txn.txn_id != 0
                && schema.fk_referenced_by.iter().any(|fk| {
                    !matches!(
                        fk.on_delete,
                        crate::storage::ReferentialAction::Restrict
                            | crate::storage::ReferentialAction::NoAction
                    )
                });
            let result = db.delete_row_by_id_at_if(txn, &schema, storage_id, row_id, |values| {
                if has_triggers {
                    fire_before_delete(db, session, &schema, values.to_vec(), registry).map_err(
                        |err| {
                            trigger_failed = true;
                            err
                        },
                    )?;
                }
                if has_triggers || returning_target == Some(p_idx) {
                    old_image = Some(values.to_vec());
                }
                // A later RESTRICT edge may fail after an earlier cascade.
                // Keep BEFORE-trigger semantics but undo partial FK actions
                // before downgrading that row's failure to a warning.
                if rollback_cascade {
                    row_savepoint = Some(db.statement_savepoint(txn.txn_id)?);
                }
                Ok(true)
            });
            let deleted = match result {
                Ok(deleted) => deleted,
                Err(err)
                    if stmt.ignore
                        && !trigger_failed
                        && super::dml::is_ignorable_delete_error(&err) =>
                {
                    if let Some(savepoint) = row_savepoint {
                        db.rollback_to_statement_savepoint(txn.txn_id, savepoint)?;
                    }
                    crate::engine::api::push_statement_condition(
                        crate::error::ConditionLevel::Warning,
                        &err,
                    );
                    continue;
                }
                Err(err) => return Err(err),
            };
            if deleted {
                if let Some(old_values) = old_image {
                    if has_triggers {
                        fire_after_delete(db, session, &schema, old_values.clone(), registry)?;
                    }
                    if returning_target == Some(p_idx) {
                        if let Some(collector) = &mut returning {
                            collector.push(Some(&old_values), None)?;
                        }
                    }
                }
                affected_rows += 1;
            }
        }
    }
    Ok(match returning {
        Some(collector) => collector.finish(QueryResponse::OkAffected { affected_rows }),
        None => QueryResponse::OkAffected { affected_rows },
    })
}

/// `true` iff at least one statement-side path needs the multi-table executor.
pub(super) fn is_multi_table_update(stmt: &Update) -> bool {
    !stmt.joins.is_empty()
}

/// Same for DELETE — multi-target list or any join lifts the statement out
/// of the single-table fast path.
pub(super) fn is_multi_table_delete(stmt: &Delete) -> bool {
    !stmt.joins.is_empty() || !stmt.targets.is_empty()
}
