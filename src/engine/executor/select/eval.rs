use super::super::*;
use super::*;
use crate::engine::session_registry::SessionRegistry;
use std::cell::RefCell;
use std::rc::Rc;

pub(super) use super::row_schema::field_matches_qualifier;
pub(super) use crate::engine::expr::{current_session_expr_info, with_session_expr_info_option};

static SUBQUERY_REGISTRY: OnceLock<SessionRegistry> = OnceLock::new();

fn subquery_registry() -> &'static SessionRegistry {
    SUBQUERY_REGISTRY.get_or_init(SessionRegistry::new)
}

/// Per-`with_subquery_context` frame: (db, txn) plus a shared cache for
/// scalar / IN / EXISTS subqueries so that the cache survives across
/// repeated `eval_plan_expr` calls (e.g., HAVING evaluation across many
/// aggregation groups). Without this, a non-correlated scalar subquery in
/// HAVING is re-executed once per group.
struct SubqueryFrame {
    db: Arc<Database>,
    txn: TxnView,
    ctes: std::collections::HashMap<String, Arc<crate::engine::api::CteResult>>,
    cache: RefCell<EvalQueryContext>,
}

thread_local! {
    static SUBQUERY_CTX: RefCell<Option<Rc<SubqueryFrame>>> = const { RefCell::new(None) };
}

/// Install a (db, txn) pair so subqueries can be evaluated even from
/// call sites that only have access to `eval_plan_expr` / `eval_plan_bool`.
/// Restores the previous value on drop, so nested calls compose correctly.
pub(in crate::engine::executor) fn with_subquery_context<R>(
    db: &Arc<Database>,
    txn: TxnView,
    f: impl FnOnce() -> R,
) -> R {
    let ctes = capture_subquery_ctes();
    with_subquery_context_ctes(db, txn, ctes, f)
}

pub(in crate::engine::executor) fn capture_subquery_ctes(
) -> std::collections::HashMap<String, Arc<crate::engine::api::CteResult>> {
    current_subquery_frame()
        .map(|frame| frame.ctes.clone())
        .unwrap_or_else(crate::engine::executor::cte::current_cte_registry)
}

pub(in crate::engine::executor) fn with_subquery_context_ctes<R>(
    db: &Arc<Database>,
    txn: TxnView,
    ctes: std::collections::HashMap<String, Arc<crate::engine::api::CteResult>>,
    f: impl FnOnce() -> R,
) -> R {
    let frame = Rc::new(SubqueryFrame {
        db: Arc::clone(db),
        txn,
        ctes,
        cache: RefCell::new(EvalQueryContext::default()),
    });
    let prev = SUBQUERY_CTX.with(|c| c.borrow_mut().replace(frame));
    let result = f();
    SUBQUERY_CTX.with(|c| *c.borrow_mut() = prev);
    result
}

fn current_subquery_frame() -> Option<Rc<SubqueryFrame>> {
    SUBQUERY_CTX.with(|c| c.borrow().clone())
}

pub(crate) fn current_scalar_subquery_type(
    select: &Select,
) -> Option<crate::engine::expr_type::ExprType> {
    let frame = current_subquery_frame()?;
    let columns = super::describe_select_columns(&frame.db, select).ok()?;
    let column = columns.first()?;
    Some(crate::engine::expr_type::ExprType::from_column_type(
        column.column_type,
        &column.source,
    ))
}

pub(super) fn project_item(row: &PlanRow, item: &SelectItem) -> Vec<Result<Value, QueryError>> {
    match item {
        SelectItem::Wildcard => row
            .fields
            .iter()
            .filter(|field| !field.invisible)
            .map(|field| Ok((field.value).clone()))
            .collect(),
        SelectItem::QualifiedWildcard(qualifier) => row
            .fields
            .iter()
            .filter(|field| field_matches_qualifier(field, qualifier) && !field.invisible)
            .map(|field| Ok((field.value).clone()))
            .collect(),
        SelectItem::Expr { expr, .. } => vec![if let Some(group) = &row.group {
            eval_aggregate_expr(expr, group, row)
        } else if row.agg_values.is_some() {
            // Vectorized HashAggregate output: aggregates have been
            // precomputed into `agg_values`; non-aggregate references
            // resolve against the group key columns in `row.fields`.
            // `eval_aggregate_expr` handles both via its `agg_values`
            // cache check at the top.
            eval_aggregate_expr(expr, &[], row)
        } else {
            eval_plan_expr(expr, row)
        }],
    }
}

pub(super) fn eval_plan_expr(expr: &Expr, row: &PlanRow) -> Result<Value, QueryError> {
    eval_plan_expr_legacy(expr, row)
}

/// Evaluate an expression that owns a subquery from a non-SELECT execution
/// surface (stored routine/trigger control flow) using the statement's
/// installed database and transaction. Callers bind their local variables to
/// `BoundValue` first, so an empty outer row is the correct correlation
/// environment; table-column correlation remains inside the subquery plan.
pub(in crate::engine::executor) fn eval_contextual_subquery_expr(
    session: &SessionContext,
    expr: &mut Expr,
) -> Result<Value, QueryError> {
    // Routine expressions bypass statement dispatch. Qualify their subqueries
    // against the routine/trigger database before planning, just like body SQL.
    if let Some(frame) = current_subquery_frame() {
        super::super::qualification::qualify_expr_tables(&frame.db, session, expr)?;
        // The caller owns a temporary bound AST. Its addresses can be reused
        // by the next routine expression, and bound locals can change between
        // evaluations. Keep pointer-keyed plans/results within this AST's
        // lifetime rather than the surrounding routine/trigger invocation.
        return eval_plan_expr_with_db(
            &frame.db,
            frame.txn,
            expr,
            &PlanRow::default(),
            &mut EvalQueryContext::default(),
        );
    }
    eval_plan_expr(expr, &PlanRow::default())
}

struct PlanRowSource<'a>(&'a PlanRow);

impl crate::engine::expr_program::RowSource for PlanRowSource<'_> {
    fn value_ref(&self, idx: usize) -> Option<&Value> {
        self.0.fields.get(idx).map(|field| field.value_ref())
    }

    fn len(&self) -> usize {
        self.0.fields.len()
    }
}

pub(super) fn input_schema_for_plan_row(row: &PlanRow) -> crate::engine::expr_program::InputSchema {
    let mut columns = Vec::with_capacity(row.fields.len());
    let mut bound_types = Vec::with_capacity(row.fields.len());
    for field in &row.fields {
        columns.push(crate::engine::expr_program::InputColumn {
            table: field.table.clone(),
            alias: field.alias.clone(),
            name: field.column.clone(),
            column_type: field.column_type,
            unsigned: field.meta.source.flags & crate::mysql::protocol::UNSIGNED_FLAG != 0
                || matches!(field.column_type, ColumnType::Year | ColumnType::Bit { .. }),
            // Plain catalog columns retain their resolved collation in
            // `ColMeta::source`; computed/projected values keep `None`.
            // Preserving it here lets ExprProgram bind comparison/LIKE
            // semantics once instead of re-resolving the PlanRow field
            // and catalog metadata for every input row.
            collation_id: field.meta.source.collation_id,
        });
        bound_types.push(crate::engine::expr_type::ExprType::from_column_type(
            field.column_type,
            &field.meta.source,
        ));
    }
    let mut schema =
        crate::engine::expr_program::InputSchema::new_with_bound_types(columns, bound_types);
    schema.mark_silent_string_columns(row.fields.iter().enumerate().filter_map(
        |(index, field)| {
            plan_expr_uses_silent_implicit_string_conversion(
                &Expr::Identifier(field.column.clone()),
                row,
            )
            .then_some(index)
        },
    ));
    schema
}

#[derive(Debug, Clone)]
pub(super) enum CompiledPlanExpr {
    SimpleField(usize),
    Program(std::sync::Arc<crate::engine::expr_program::ExprProgram>),
    Legacy(Expr),
}

impl CompiledPlanExpr {
    pub(super) fn compile_once(
        expr: &Expr,
        schema: &crate::engine::expr_program::InputSchema,
    ) -> Self {
        if matches!(
            expr,
            Expr::Function { name, .. }
                if name.eq_ignore_ascii_case("__DISTINCT_COLLATION_KEY")
                    || name.eq_ignore_ascii_case("__DISTINCT_TUPLE")
        ) {
            // A plain source column's collation is present in InputSchema,
            // but these synthetic DISTINCT nodes may combine computed
            // expressions whose coercibility and PAD SPACE metadata lives
            // only in the PlanRow descriptor. Keep the schema-aware legacy
            // binder until ExprProgram carries that complete descriptor.
            return Self::Legacy(expr.clone());
        }
        match expr {
            Expr::Identifier(name) => {
                if let Ok(idx) = schema.column_index(None, name) {
                    return Self::SimpleField(idx);
                }
            }
            Expr::CompoundIdentifier(parts) => {
                if let Some(column) = parts.last() {
                    let qualifier = if parts.len() >= 2 {
                        Some(parts[parts.len() - 2].as_str())
                    } else {
                        None
                    };
                    if let Ok(idx) = schema.column_index(qualifier, column) {
                        return Self::SimpleField(idx);
                    }
                }
            }
            _ => {}
        }
        // Runtime DECIMAL values do not carry catalog signedness. Keep an
        // unsigned subtraction on the schema-aware legacy path so ORDER BY
        // and other compiled-only consumers enforce error 1690 exactly like
        // projection evaluation.
        if let Expr::Binary {
            left,
            op: BinaryOp::Sub,
            right,
        } = expr
        {
            let unsigned = |column_expr: &Expr| {
                let index = match column_expr {
                    Expr::Identifier(name) => schema.column_index(None, name).ok()?,
                    Expr::CompoundIdentifier(parts) => {
                        let column = parts.last()?;
                        let qualifier = (parts.len() >= 2).then(|| parts[parts.len() - 2].as_str());
                        schema.column_index(qualifier, column).ok()?
                    }
                    _ => return None,
                };
                schema.column_unsigned(index)
            };
            if crate::engine::expr::expr_result_unsigned(left, &unsigned)
                || crate::engine::expr::expr_result_unsigned(right, &unsigned)
            {
                return Self::Legacy(expr.clone());
            }
        }
        let bound = crate::engine::expr_program::ExprCompiler::bind_vector_constants(expr);
        match crate::engine::expr_program::ExprCompiler::compile_cached(
            bound.as_ref().unwrap_or(expr),
            schema,
        ) {
            Ok(program) => Self::Program(program),
            Err(_) => Self::Legacy(expr.clone()),
        }
    }

    pub(super) fn eval(&self, row: &PlanRow) -> Result<Value, QueryError> {
        match self {
            Self::SimpleField(idx) => row
                .fields
                .get(*idx)
                .map(|field| (field.value).clone())
                .ok_or_else(|| error(crate::error::catalog::ER_BAD_FIELD_ERROR, "unknown column")),
            Self::Program(program) => {
                let source = PlanRowSource(row);
                crate::engine::expr_program::with_eval_scratch(|scratch| {
                    program.eval_source(&source, scratch)
                })
            }
            Self::Legacy(expr) => {
                crate::engine::expr_program::record_legacy_fallback_row();
                eval_plan_expr_legacy(expr, row)
            }
        }
    }

    /// Borrow the underlying `Value` directly when this expression is a
    /// simple column reference. Used by hot loops (e.g. GROUP BY key
    /// extraction) so the caller can build a `ValueKey` from the borrow
    /// without an intermediate `Value::clone`. Returns `None` for
    /// `Program` / `Legacy` shapes — the caller must fall back to
    /// `eval(row)` and own the resulting value.
    #[inline]
    pub(super) fn try_borrow_field<'a>(&self, row: &'a PlanRow) -> Option<&'a Value> {
        match self {
            Self::SimpleField(idx) => row.fields.get(*idx).map(|field| &field.value),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct CompiledPlanPredicate {
    expr: Expr,
    compiled: CompiledPlanExpr,
}

impl CompiledPlanPredicate {
    pub(super) fn compile_once(expr: &Expr, row: &PlanRow) -> Self {
        let schema = input_schema_for_plan_row(row);
        Self {
            expr: expr.clone(),
            compiled: CompiledPlanExpr::compile_once(expr, &schema),
        }
    }

    pub(super) fn eval_bool(&self, row: &PlanRow) -> Result<bool, QueryError> {
        if let CompiledPlanExpr::Program(program) = &self.compiled {
            let source = PlanRowSource(row);
            if let Some(result) = program.eval_simple_predicate_source(&source) {
                return result;
            }
        }
        Ok(truthy(&self.compiled.eval(row)?))
    }

    pub(super) fn eval_bool_with_db(
        &self,
        db: &Arc<Database>,
        txn: TxnView,
        row: &PlanRow,
        eval_ctx: &mut EvalQueryContext,
    ) -> Result<bool, QueryError> {
        match &self.compiled {
            CompiledPlanExpr::Legacy(_) => {
                eval_plan_bool_with_db(db, txn, &self.expr, row, eval_ctx)
            }
            _ => self.eval_bool(row),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) enum CompiledPlanSelectItem {
    Wildcard,
    QualifiedWildcard(String),
    Expr {
        expr: Expr,
        compiled: CompiledPlanExpr,
        /// Precomputed `aggregate_key(expr)` hash when `expr` is a bare
        /// aggregate function call. Aggregate output rows carry a
        /// `agg_values` cache keyed by this hash; without precomputation
        /// `eval_aggregate_expr` recomputes the key per row by walking the
        /// expression with `Debug` formatting (TPC-DS Q78: 3 `sum()`
        /// columns × millions of group rows were ~half of projection time).
        /// `None` for non-aggregate exprs and composite exprs that wrap an
        /// aggregate (those still take the recursive path).
        agg_cache_key: Option<u64>,
    },
}

/// `Some(hash)` when `expr` is a bare aggregate function call whose result
/// `eval_aggregate_expr` would look up directly in a row's `agg_values`
/// cache. Precomputing the key lets the projection skip the per-row
/// `Debug`-format walk inside [`super::aggregate::aggregate_key`]. Composite
/// exprs that merely *contain* an aggregate return `None` — they still need
/// the recursive interpreter, which keys each aggregate sub-node itself.
fn bare_aggregate_cache_key(expr: &Expr) -> Option<u64> {
    match expr {
        Expr::Function { name, .. } if crate::planner::is_aggregate_function(name) => {
            Some(super::aggregate::aggregate_key(expr))
        }
        _ => None,
    }
}

#[derive(Debug, Clone)]
pub(super) struct PlanProjectionProgram {
    items: Vec<CompiledPlanSelectItem>,
    // A plain projection of distinct source fields can transfer Values out of
    // an owned PlanRow.  The streaming SELECT path owns every input row and
    // used to clone each Value into an intermediate Vec<Result<..>>, then
    // allocate a second Vec<Value> while collecting it.
    owned_simple_fields: Option<Vec<usize>>,
}

impl PlanProjectionProgram {
    pub(super) fn compile_once(row: &PlanRow, projection: &[SelectItem]) -> Self {
        let schema = input_schema_for_plan_row(row);
        let items: Vec<_> = projection
            .iter()
            .map(|item| match item {
                SelectItem::Wildcard => CompiledPlanSelectItem::Wildcard,
                SelectItem::QualifiedWildcard(qualifier) => {
                    CompiledPlanSelectItem::QualifiedWildcard(qualifier.clone())
                }
                SelectItem::Expr { expr, .. } => CompiledPlanSelectItem::Expr {
                    expr: expr.clone(),
                    compiled: CompiledPlanExpr::compile_once(expr, &schema),
                    agg_cache_key: bare_aggregate_cache_key(expr),
                },
            })
            .collect();
        let mut seen = vec![false; row.fields.len()];
        let mut simple_fields = Vec::with_capacity(items.len());
        let all_distinct_simple_fields = items.iter().all(|item| {
            let CompiledPlanSelectItem::Expr {
                compiled: CompiledPlanExpr::SimpleField(idx),
                agg_cache_key: None,
                ..
            } = item
            else {
                return false;
            };
            let Some(was_seen) = seen.get_mut(*idx) else {
                return false;
            };
            if *was_seen {
                return false;
            }
            *was_seen = true;
            simple_fields.push(*idx);
            true
        });
        Self {
            items,
            owned_simple_fields: all_distinct_simple_fields.then_some(simple_fields),
        }
    }

    /// Project directly into the result representation.  This avoids the
    /// per-row `Vec<Result<Value, _>>` allocation paid by callers that always
    /// immediately collected it into `Result<Vec<Value>, _>`.
    #[inline]
    pub(super) fn project_row_values(&self, row: &PlanRow) -> Result<Vec<Value>, QueryError> {
        self.project_row_values_with_capacity(row, 0)
    }

    /// A caller with an explicit scalar projection can reserve its known
    /// output width without changing wildcard allocation or program layout.
    pub(super) fn project_row_values_with_capacity(
        &self,
        row: &PlanRow,
        capacity: usize,
    ) -> Result<Vec<Value>, QueryError> {
        let mut out = Vec::with_capacity(capacity);
        for item in &self.items {
            match item {
                CompiledPlanSelectItem::Wildcard => out.extend(
                    row.fields
                        .iter()
                        .filter(|field| {
                            !super::plan::is_ft_score_column(&field.column) && !field.invisible
                        })
                        .map(|field| (field.value).clone()),
                ),
                CompiledPlanSelectItem::QualifiedWildcard(qualifier) => out.extend(
                    row.fields
                        .iter()
                        .filter(|field| {
                            field_matches_qualifier(field, qualifier)
                                && !super::plan::is_ft_score_column(&field.column)
                                && !field.invisible
                        })
                        .map(|field| (field.value).clone()),
                ),
                CompiledPlanSelectItem::Expr {
                    expr,
                    compiled,
                    agg_cache_key,
                } => {
                    if let Expr::Function { name, args, .. } = expr {
                        if name.eq_ignore_ascii_case("GREATEST")
                            || name.eq_ignore_ascii_case("LEAST")
                        {
                            validate_plan_string_combinator(args, row)?;
                        }
                    }
                    // Fast path first: a bare aggregate column (`sum(x)`)
                    // whose key was hashed once at compile time resolves to a
                    // direct `agg_values` probe instead of re-formatting
                    // `expr` per row inside `aggregate_key`. HashAggregate
                    // emits rows with `group: Some(empty)` *and* a populated
                    // cache, so this must be checked before the `row.group`
                    // branch (which would otherwise always win and re-walk the
                    // expression). On a miss we fall back to the exact same
                    // interpreter paths, so semantics are unchanged.
                    let cached = match (agg_cache_key, row.agg_values.as_ref()) {
                        (Some(key), Some(cache)) => cache.get(key),
                        _ => None,
                    };
                    out.push(match cached {
                        Some(v) => coerce_aggregate_result_for_expr(expr, row, v.clone()),
                        None if row.group.is_some() => {
                            eval_aggregate_expr(expr, row.group.as_deref().unwrap(), row)
                        }
                        None if row.agg_values.is_some() => eval_aggregate_expr(expr, &[], row),
                        None => compiled.eval(row),
                    }?);
                }
            }
        }
        Ok(out)
    }

    /// Move plain projected fields out of an owned row.  Composite,
    /// aggregate, wildcard, and duplicate-field projections retain the exact
    /// borrowed evaluator as their correctness oracle.
    pub(super) fn project_row_owned(&self, mut row: PlanRow) -> Result<Vec<Value>, QueryError> {
        let Some(indices) = self
            .owned_simple_fields
            .as_ref()
            .filter(|_| row.group.is_none() && row.agg_values.is_none())
        else {
            return self.project_row_values(&row);
        };
        let mut out = Vec::with_capacity(indices.len());
        for idx in indices {
            let field = row.fields.get_mut(*idx).ok_or_else(|| {
                error(crate::error::catalog::ER_BAD_FIELD_ERROR, "unknown column")
            })?;
            out.push(std::mem::replace(&mut field.value, Value::Null));
        }
        Ok(out)
    }
}

/// Compiled per-key state for `PlanOrderProgram`. Variant is decided
/// once at `compile_once` against the first row's shape; all subsequent
/// rows in the same sort must share that shape (HashAggregate /
/// StreamingAggregate / etc. emit a homogeneous batch of either all
/// aggregate rows or all plain rows — never a mix).
#[derive(Debug, Clone)]
enum CompiledOrderKey {
    /// Plain row (no `group` / `agg_values`). Use the expression
    /// compiler, which falls back to the legacy interpreter only on
    /// shapes it doesn't support.
    Plain(CompiledPlanExpr),
    /// Aggregate row where the entire ORDER BY expression is one
    /// aggregate function call (e.g. `ORDER BY SUM(x)`, `ORDER BY
    /// COUNT(*)`). The fingerprint hash is precomputed so per-row
    /// lookup is a direct `HashMap::get` against `row.agg_values`,
    /// skipping the per-row `format!("{expr:?}")` that
    /// `aggregate_key()` would otherwise pay.
    AggCacheHash { hash: u64, fallback_expr: Expr },
    /// Aggregate row, but the ORDER BY expression is a non-aggregate or
    /// composite expression (binary, case, group_N ref, etc.). Falls
    /// through to the interpreter so we preserve the existing semantics
    /// without regressing anything.
    AggGeneric(Expr),
}

#[derive(Debug, Clone)]
pub(super) struct PlanOrderProgram {
    keys: Vec<CompiledOrderKey>,
}

impl PlanOrderProgram {
    pub(super) fn compile_once(row: &PlanRow, order_by: &[OrderBy]) -> Self {
        let is_aggregate_row = row.group.is_some() || row.agg_values.is_some();
        let schema = input_schema_for_plan_row(row);
        let keys = order_by
            .iter()
            .map(|item| Self::compile_key(&item.expr, &schema, is_aggregate_row))
            .collect();
        Self { keys }
    }

    fn compile_key(
        expr: &Expr,
        schema: &crate::engine::expr_program::InputSchema,
        is_aggregate_row: bool,
    ) -> CompiledOrderKey {
        if !is_aggregate_row {
            return CompiledOrderKey::Plain(CompiledPlanExpr::compile_once(expr, schema));
        }
        if let Expr::Function { name, .. } = expr {
            if crate::planner::is_aggregate_function(name) {
                return CompiledOrderKey::AggCacheHash {
                    hash: super::aggregate::aggregate_key(expr),
                    fallback_expr: expr.clone(),
                };
            }
        }
        CompiledOrderKey::AggGeneric(expr.clone())
    }

    pub(super) fn eval_keys(&self, row: &PlanRow) -> Result<Vec<Value>, QueryError> {
        let keys = self
            .keys
            .iter()
            .map(|key| Self::eval_one(key, row))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(keys)
    }

    pub(super) fn eval_keys_inline(
        &self,
        row: &PlanRow,
    ) -> Result<smallvec::SmallVec<[Value; 2]>, QueryError> {
        self.keys
            .iter()
            .map(|key| Self::eval_one(key, row))
            .collect()
    }

    pub(super) fn eval_single_key(&self, row: &PlanRow) -> Result<Value, QueryError> {
        debug_assert_eq!(self.keys.len(), 1);
        Self::eval_one(&self.keys[0], row)
    }

    #[inline]
    fn eval_one(key: &CompiledOrderKey, row: &PlanRow) -> Result<Value, QueryError> {
        match key {
            CompiledOrderKey::Plain(compiled) => compiled.eval(row),
            CompiledOrderKey::AggCacheHash {
                hash,
                fallback_expr,
            } => {
                if let Some(cache) = row.agg_values.as_ref() {
                    if let Some(v) = cache.get(hash) {
                        return coerce_aggregate_result_for_expr(fallback_expr, row, v.clone());
                    }
                }
                // Cache miss (e.g. group-iteration emitter that didn't
                // populate `agg_values` — paranoia path, the planner
                // should always provide a cache when emitting aggregate
                // rows). Fall back to the interpreter so semantics are
                // identical to the legacy path.
                let group: &[PlanRow] = row.group.as_deref().unwrap_or(&[]);
                eval_aggregate_expr(fallback_expr, group, row)
            }
            CompiledOrderKey::AggGeneric(expr) => {
                let group: &[PlanRow] = row.group.as_deref().unwrap_or(&[]);
                eval_aggregate_expr(expr, group, row)
            }
        }
    }
}

/// Lookup of GROUP BY hidden columns (ONLY_FULL_GROUP_BY off) on aggregate output rows:
/// the planner appends a `__hidden_first_value(col)` slot per hidden column reference (see
/// `planner::collect_aggregate_calls`), and the group's first-row value rides with the agg cache.
/// Fused aggregation output rows either omit non-grouped columns or fill NULL placeholders,
/// so probe the cache by slot fingerprint **only** when field resolution fails or yields NULL:
/// - non-NULL field value (grouping key columns, sample values on the generic path) → zero extra cost;
/// - generic-path sample itself NULL → the cached first-value is the same first-row NULL of
///   the group; results agree;
/// - non-aggregate rows (`agg_values` is None) → returned as-is after one Option check.
fn hidden_group_cache_value(
    expr: &Expr,
    row: &PlanRow,
    resolved: Result<Value, QueryError>,
) -> Result<Value, QueryError> {
    match &resolved {
        Ok(Value::Null) | Err(_) => {}
        _ => return resolved,
    }
    let Some(cache) = row.agg_values.as_ref() else {
        return resolved;
    };
    let Some(key) = super::aggregate::hidden_group_column_key(expr) else {
        return resolved;
    };
    match cache.get(&key) {
        Some(value) => Ok(value.clone()),
        None => resolved,
    }
}

#[inline]
fn eval_plan_in_list(
    expr: &Expr,
    list: &[Expr],
    negated: bool,
    row: &PlanRow,
    mut eval: impl FnMut(&Expr) -> Result<Value, QueryError>,
) -> Result<Value, QueryError> {
    if let Expr::Tuple(left_items) = expr {
        return eval_row_in_list_plan(left_items, list, negated, row, eval);
    }
    let value = eval(expr)?;
    let mut iter = list.iter();
    let collation_id = plan_in_list_collation(expr, list, row);
    crate::engine::expr::with_constant_in_warning_dedup(list, || {
        crate::engine::expr::with_compare_collation(collation_id, || {
            crate::engine::expr::eval_in_list(value, negated, || {
                iter.next().map(&mut eval).transpose()
            })
        })
    })
}

/// Shared PlanRow BETWEEN semantics. Each entry supplies its own recursive
/// evaluator; aggregate evaluation preserves its earlier collation-resolution
/// point, before evaluating bound expressions.
#[inline]
fn eval_plan_between(
    expr: &Expr,
    low: &Expr,
    high: &Expr,
    negated: bool,
    row: &PlanRow,
    collation_before_bounds: bool,
    mut eval: impl FnMut(&Expr) -> Result<Value, QueryError>,
) -> Result<Value, QueryError> {
    if let Some(value) =
        crate::engine::expr::try_eval_row_between(expr, low, high, negated, &mut eval, |a, b| {
            plan_binary_collation_id(a, b, row)
        })?
    {
        return Ok(value);
    }
    let value = eval(expr)?;
    let early_collation =
        collation_before_bounds.then(|| plan_between_collation(expr, low, high, row));
    let low_value = eval(low)?;
    let high_value = eval(high)?;
    let collation = early_collation.unwrap_or_else(|| plan_between_collation(expr, low, high, row));
    crate::engine::expr::with_compare_collation(collation, || {
        crate::engine::expr::eval_between(&value, &low_value, &high_value, negated)
    })
}

fn eval_plan_like(
    expr: &Expr,
    pattern: &Expr,
    negated: bool,
    escape_char: Option<char>,
    row: &PlanRow,
    mut eval: impl FnMut(&Expr) -> Result<Value, QueryError>,
) -> Result<Value, QueryError> {
    let value = eval(expr)?;
    let pattern_value = eval(pattern)?;
    let info = plan_operand_collation(&[expr, pattern], row, "like")
        .and_then(crate::charset::resolve_collation_id);
    Ok(crate::engine::expr::eval_like_values_with_collation(
        &value,
        &pattern_value,
        negated,
        escape_char,
        info,
    ))
}

/// Shared structural evaluation; context-specific column, aggregate and
/// subquery resolution is supplied through a monomorphized recursive callback.
#[inline]
fn eval_plan_structural_expr(
    expr: &Expr,
    row: &PlanRow,
    collation_before_bounds: bool,
    mut eval: impl FnMut(&Expr) -> Result<Value, QueryError>,
) -> Result<Value, QueryError> {
    match expr {
        Expr::IsNull(value) => Ok(Value::Int(matches!(eval(value)?, Value::Null) as i64)),
        Expr::IsNotNull(value) => Ok(Value::Int(!matches!(eval(value)?, Value::Null) as i64)),
        Expr::InList {
            expr,
            list,
            negated,
        } => eval_plan_in_list(expr, list, *negated, row, eval),
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => eval_plan_between(
            expr,
            low,
            high,
            *negated,
            row,
            collation_before_bounds,
            eval,
        ),
        Expr::Like {
            expr,
            pattern,
            negated,
            escape_char,
        } => eval_plan_like(expr, pattern, *negated, *escape_char, row, eval),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => eval_case(operand, conditions, results, else_result, eval),
        _ => unreachable!("structural expression dispatch"),
    }
}

#[inline]
pub(super) fn eval_plan_expr_legacy(expr: &Expr, row: &PlanRow) -> Result<Value, QueryError> {
    crate::engine::expr::eval_scalar_context(expr, || eval_plan_expr_legacy_inner(expr, row))
}

/// Common PlanRow scalar-function binding. The caller owns recursion (plain
/// rows or database/subquery context); source charset and diagnostics stay here.
#[inline]
fn eval_plan_scalar_function(
    name: &str,
    args: &[Expr],
    row: &PlanRow,
    mut eval: impl FnMut(&Expr) -> Result<Value, QueryError>,
) -> Result<Value, QueryError> {
    if name.eq_ignore_ascii_case("CAST") {
        if let Some(descriptor) = typed_json_cast_source(args, row) {
            let value = eval(&args[0])?;
            return Ok(Value::Json(crate::engine::json::scalar_from_typed_value(
                &value,
                &descriptor,
            )));
        }
    }
    if name.eq_ignore_ascii_case("GREATEST") || name.eq_ignore_ascii_case("LEAST") {
        validate_plan_string_combinator(args, row)?;
    }
    // The scalar evaluator normally derives collation from the AST.
    // A bound column reference deliberately carries no catalog
    // metadata in that AST, so functions whose value is the actual
    // sort key must receive the resolved PlanField collation. Wrap
    // only the first argument in the existing metadata-preserving
    // marker; runtime evaluation of the marker is still the original
    // expression and therefore has no extra conversion/allocation.
    let bound_args;
    let args = if name.eq_ignore_ascii_case("WEIGHT_STRING")
        || name.eq_ignore_ascii_case("WEIGHT_STRING_TYPED")
        || name.eq_ignore_ascii_case("__DISTINCT_COLLATION_KEY")
    {
        bound_args = bind_weight_string_collation(args, row);
        bound_args.as_slice()
    } else if crate::engine::expr::is_string_search_function(name) {
        bound_args = bind_string_search_collations(args, row);
        bound_args.as_slice()
    } else if name.eq_ignore_ascii_case("CHARSET")
        || name.eq_ignore_ascii_case("COLLATION")
        || name.eq_ignore_ascii_case("COERCIBILITY")
    {
        bound_args = bind_metadata_inspection_collation(args, row);
        bound_args.as_slice()
    } else {
        args
    };
    let mut evaluate = || {
        eval_function(name, args, |arg| {
            let value = eval(arg)?;
            function_arg_with_source_octets(name, arg, row, value)
        })
    };
    if matches!(
        name.to_ascii_uppercase().as_str(),
        "ABS" | "CEIL" | "CEILING" | "FLOOR" | "ROUND" | "TRUNCATE"
    ) && args
        .iter()
        .any(|arg| plan_expr_uses_silent_implicit_string_conversion(arg, row))
    {
        crate::engine::expr::with_suppressed_expression_warnings(evaluate)
    } else {
        evaluate()
    }
}

#[inline(never)]
fn eval_plan_expr_legacy_inner(expr: &Expr, row: &PlanRow) -> Result<Value, QueryError> {
    match expr {
        Expr::IsNull(_)
        | Expr::IsNotNull(_)
        | Expr::InList { .. }
        | Expr::Like { .. }
        | Expr::Case { .. }
        | Expr::Between { .. } => {
            eval_plan_structural_expr(expr, row, false, |e| eval_plan_expr_legacy(e, row))
        }
        Expr::Wildcard => Ok(Value::Int(row.fields.len() as i64)),
        Expr::Identifier(name) => {
            let resolved = resolve_plan_field(row, None, name).map(|field| (field.value).clone());
            hidden_group_cache_value(expr, row, resolved)
        }
        Expr::CompoundIdentifier(parts) => {
            let column = parts.last().ok_or_else(|| {
                error(crate::error::catalog::ER_BAD_FIELD_ERROR, "unknown column")
            })?;
            let qualifier = if parts.len() >= 2 {
                Some(parts[parts.len() - 2].as_str())
            } else {
                None
            };
            let resolved =
                resolve_plan_field(row, qualifier, column).map(|field| (field.value).clone());
            hidden_group_cache_value(expr, row, resolved)
        }
        Expr::Literal(lit) => Ok(literal_value(lit)),
        Expr::BoundValue(value) => Ok(value.clone()),
        Expr::Placeholder(idx) => crate::engine::params::current_param(*idx).ok_or_else(|| {
            error(
                crate::error::catalog::ER_WRONG_ARGUMENTS,
                "parameter is not bound",
            )
        }),
        Expr::UserVariable(name) => {
            Ok(crate::engine::expr::session_user_variable(name).unwrap_or(Value::Null))
        }
        Expr::SystemVariable { scope, name } => {
            crate::engine::expr::eval_system_variable(*scope, name, None)
        }
        Expr::Unary {
            op: UnaryOp::Neg,
            expr,
        } => {
            let value = eval_plan_expr_legacy(expr, row)?;
            crate::engine::expr::duplicate_negated_int_div_zero_warning(expr, &value);
            let evaluate = || crate::engine::expr::eval_negate(value);
            if plan_expr_uses_silent_implicit_string_conversion(expr, row) {
                crate::engine::expr::with_suppressed_expression_warnings(evaluate)
            } else {
                evaluate()
            }
        }
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => Ok(crate::engine::expr::eval_logical_not(
            &eval_plan_expr_legacy(expr, row)?,
        )),
        Expr::Unary {
            op: UnaryOp::BitNot,
            expr,
        } => crate::engine::expr::eval_bit_not(eval_plan_expr_legacy(expr, row)?),
        Expr::Binary { left, op, right }
            if matches!(
                (left.as_ref(), right.as_ref()),
                (Expr::Tuple(_), Expr::Subquery(_)) | (Expr::Subquery(_), Expr::Tuple(_))
            ) =>
        {
            let frame = current_subquery_frame().ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "row subquery comparison requires query context",
                )
            })?;
            let mut cache = frame.cache.borrow_mut();
            eval_plan_expr_with_db(&frame.db, frame.txn, expr, row, &mut cache)
        }
        Expr::Binary { left, op, right } => eval_plan_binary_legacy(left, *op, right, row),
        Expr::InSubquery { .. }
        | Expr::QuantifiedComparison { .. }
        | Expr::Subquery(_)
        | Expr::Exists { .. } => {
            if let Some(frame) = current_subquery_frame() {
                let mut cache = frame.cache.borrow_mut();
                eval_plan_expr_with_db(&frame.db, frame.txn, expr, row, &mut cache)
            } else {
                let msg = match expr {
                    Expr::InSubquery { .. } => "IN subquery requires query context",
                    Expr::QuantifiedComparison { .. } => {
                        "ANY/ALL/SOME subquery requires query context"
                    }
                    Expr::Subquery(_) => "subquery requires query context",
                    Expr::Exists { .. } => "EXISTS requires query context",
                    _ => unreachable!(),
                };
                Err(error(crate::error::catalog::ER_NOT_SUPPORTED_YET, msg))
            }
        }
        // `MATCH(...) AGAINST(...)` resolves to the hidden BM25 score the
        // FulltextScan injected for this row, keyed by the MATCH expr's
        // fingerprint. WHERE relies on `truthy(Double)` (score != 0 ⇒ match);
        // SELECT / ORDER BY read the score directly.
        Expr::Function { name, .. } if name == crate::parser::FT_MATCH_FUNCTION => {
            resolve_fulltext_score(expr, row)
        }
        Expr::Function { name, args, .. } if planner::is_aggregate_function(name) => Err(error(
            crate::error::catalog::ER_INVALID_GROUP_FUNC_USE,
            format!("invalid use of group function '{name}'"),
        )),
        Expr::Function { name, args, .. } => {
            if name.eq_ignore_ascii_case("FIELD") {
                let domain = plan_field_domain(args, row);
                let collation = if domain == crate::engine::expr::FieldComparisonDomain::Strings {
                    plan_operand_collation(&args.iter().collect::<Vec<_>>(), row, "field")
                } else {
                    None
                };
                let mut values = args
                    .iter()
                    .map(|arg| eval_plan_expr_legacy(arg, row))
                    .collect::<Result<Vec<_>, _>>()?;
                let domain =
                    crate::engine::expr::bind_field_numeric_literals(args, &mut values, domain);
                return crate::engine::expr::with_compare_collation(collation, || {
                    crate::engine::expr::eval_field_values(&values, domain)
                });
            }
            eval_plan_scalar_function(name, args, row, |arg| eval_plan_expr_legacy(arg, row))
        }
        Expr::GroupKeyRef { index, .. } => {
            let column = format!("group_{index}");
            resolve_plan_field(row, Some("$group"), &column).map(|field| field.value.clone())
        }
        Expr::Grouping(indices) => eval_aggregate_expr(&Expr::Grouping(indices.clone()), &[], row),
        Expr::WindowFunction { .. } => row
            .fields
            .iter()
            .find(|f| f.column == "__window__")
            .map(|f| Ok((f.value).clone()))
            .unwrap_or_else(|| {
                Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "window function requires window evaluation context",
                ))
            }),
        Expr::Default(Some(name)) => resolve_plan_default(None, name, row),
        Expr::DefaultQualified(parts) => resolve_plan_default(
            parts.iter().rev().nth(1).map(String::as_str),
            parts.last().map(String::as_str).unwrap_or(""),
            row,
        ),
        Expr::Default(None) => Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "DEFAULT is only supported in DML default contexts",
        )),
        Expr::Tuple(_) => Err(crate::engine::expr::operand_columns_error(1)),
    }
}

fn bind_weight_string_collation(args: &[Expr], row: &PlanRow) -> Vec<Expr> {
    let mut bound = args.to_vec();
    let Some(first) = bound.first_mut() else {
        return bound;
    };
    let Some(collation) = plan_expr_collation(first, row) else {
        return bound;
    };
    let original = std::mem::replace(first, Expr::Literal(Literal::Null));
    *first = Expr::Function {
        name: "__COLLATED_CONST".to_string(),
        args: vec![
            original,
            Expr::Literal(Literal::String(collation.name.to_string())),
            Expr::Literal(Literal::Int(2)),
        ],
        distinct: false,
    };
    bound
}

fn bind_string_search_collations(args: &[Expr], row: &PlanRow) -> Vec<Expr> {
    let mut bound = args.to_vec();
    for arg in bound.iter_mut().take(2) {
        let Some(collation) = plan_expr_collation(arg, row) else {
            continue;
        };
        let original = std::mem::replace(arg, Expr::Literal(Literal::Null));
        *arg = Expr::Function {
            name: "__COLLATED_CONST".to_string(),
            args: vec![
                original,
                Expr::Literal(Literal::String(collation.name.to_string())),
                Expr::Literal(Literal::Int(2)),
            ],
            distinct: false,
        };
    }
    bound
}

fn bind_metadata_inspection_collation(args: &[Expr], row: &PlanRow) -> Vec<Expr> {
    if args.first().is_some_and(|arg| {
        matches!(arg, Expr::Function { name, .. }
        if name.eq_ignore_ascii_case("COLLATE") || name.eq_ignore_ascii_case("__COLLATED_CONST"))
    }) {
        return args.to_vec();
    }
    if args.len() != 1
        || !matches!(
            args[0],
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::GroupKeyRef { .. }
        )
    {
        return args.to_vec();
    }
    let Some(info) = plan_expr_collation(&args[0], row) else {
        return args.to_vec();
    };
    vec![Expr::Function {
        name: "__COLLATED_CONST".to_string(),
        args: vec![
            args[0].clone(),
            Expr::Literal(crate::parser::Literal::String(info.name.to_string())),
            Expr::Literal(crate::parser::Literal::Int(2)),
        ],
        distinct: false,
    }]
}

/// Functions that operate on string octets must see the bytes in the
/// operand's declared character set, not the UTF-8 bytes of mydb's internal
/// `String`.  This is observable for single-byte columns (notably latin1)
/// containing arbitrary AES ciphertext.
fn function_arg_with_source_octets(
    function_name: &str,
    expr: &Expr,
    row: &PlanRow,
    value: Value,
) -> Result<Value, QueryError> {
    if crate::engine::expr::is_charset_octet_function(function_name) {
        if let Some(collation) = plan_expr_collation(expr, row) {
            return crate::engine::expr::value_in_charset_octets(value, collation.charset);
        }
        return crate::engine::expr::value_in_charset_octets(
            value,
            crate::engine::expr::connection_charset(),
        );
    }
    let aes_function = function_name.eq_ignore_ascii_case("AES_ENCRYPT")
        || function_name.eq_ignore_ascii_case("AES_DECRYPT");
    let concat_function = function_name.eq_ignore_ascii_case("CONCAT")
        || function_name.eq_ignore_ascii_case("CONCAT_WS");
    let crc32_function = function_name.eq_ignore_ascii_case("CRC32");
    if !aes_function && !concat_function && !crc32_function {
        return Ok(value);
    }
    let field = match expr {
        Expr::Identifier(name) => resolve_plan_field(row, None, name).ok(),
        Expr::CompoundIdentifier(parts) => {
            let column = parts.last();
            let qualifier = (parts.len() >= 2).then(|| parts[parts.len() - 2].as_str());
            column.and_then(|column| resolve_plan_field(row, qualifier, column).ok())
        }
        Expr::GroupKeyRef { expr, .. } => {
            return function_arg_with_source_octets(function_name, expr, row, value);
        }
        _ => None,
    };
    if crc32_function {
        let Some(field) = field else {
            return Ok(value);
        };
        match (field.meta.column_type, value) {
            (ColumnType::Bit { bits }, Value::UInt(value)) => {
                let byte_width = usize::from(bits.div_ceil(8));
                let encoded = value.to_be_bytes();
                return Ok(Value::Bytes(encoded[encoded.len() - byte_width..].to_vec()));
            }
            (ColumnType::Float, Value::Float(value)) => {
                let scale = field.meta.source.decimals.unwrap_or(0);
                return Ok(Value::Bytes(
                    format!("{:.*}", usize::from(scale), value).into_bytes(),
                ));
            }
            (ColumnType::Double, Value::Double(value)) => {
                let scale = field.meta.source.decimals.unwrap_or(0);
                return Ok(Value::Bytes(
                    format!("{:.*}", usize::from(scale), value).into_bytes(),
                ));
            }
            (_, value) => return Ok(value),
        }
    }
    if concat_function {
        if let Some(field) = field {
            if let ColumnType::Bit { bits } = field.meta.column_type {
                let Value::UInt(value) = value else {
                    return Ok(value);
                };
                let byte_width = usize::from(bits.div_ceil(8));
                let encoded = value.to_be_bytes();
                return Ok(Value::Bytes(encoded[encoded.len() - byte_width..].to_vec()));
            }
        }
        return Ok(value);
    }
    let Some(charset) = field
        .and_then(|field| field.meta.source.collation_id)
        .and_then(crate::charset::resolve_collation_id)
        .map(|info| info.charset)
        .filter(|charset| *charset != "binary")
    else {
        return Ok(value);
    };
    let text = match value {
        Value::String(text) | Value::Decimal(text) => text,
        Value::SmallString(bytes) => std::str::from_utf8(bytes.as_ref())
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_INVALID_CHARACTER_STRING,
                    format!("Invalid character string for character set {charset}"),
                )
            })?
            .to_string(),
        other => return Ok(other),
    };
    crate::charset::encode_string(charset, &text).map(Value::Bytes)
}

pub(super) fn validate_plan_string_combinator(
    args: &[Expr],
    row: &PlanRow,
) -> Result<(), QueryError> {
    let field_for = |expr: &Expr| -> Option<&PlanField> {
        match expr {
            Expr::Identifier(name) => resolve_plan_field(row, None, name).ok(),
            Expr::CompoundIdentifier(parts) => {
                let column = parts.last()?;
                let qualifier = (parts.len() >= 2).then(|| parts[parts.len() - 2].as_str());
                resolve_plan_field(row, qualifier, column).ok()
            }
            _ => None,
        }
    };
    // A catalog column has implicit coercibility 2. A derived column that
    // originated from a charset introducer retains the introducer's weaker
    // derivation even though it is now referenced by name, so prefer a real
    // catalog origin as the conversion target.
    let target = args
        .iter()
        .filter_map(field_for)
        .find(|field| !field.meta.source.org_table.is_empty())
        .or_else(|| args.iter().filter_map(field_for).next());
    let Some(target) = target else {
        return Ok(());
    };
    let Some(target_info) = target
        .meta
        .source
        .collation_id
        .and_then(crate::charset::resolve_collation_id)
    else {
        return Ok(());
    };
    if target_info.charset == "binary" {
        return Ok(());
    }
    for arg in args {
        let field = field_for(arg);
        let source_info = field
            .and_then(|source| source.meta.source.collation_id)
            .or_else(|| crate::engine::expr::expression_metadata_collation_id(arg))
            .and_then(crate::charset::resolve_collation_id);
        let Some(source_info) = source_info else {
            continue;
        };
        if source_info.charset == target_info.charset {
            continue;
        }
        let value = match field {
            Some(source) => source.value.clone(),
            None => eval_plan_expr_legacy(arg, row)?,
        };
        let text = crate::storage::value_to_string(&value);
        if crate::charset::encode_string(target_info.charset, &text).is_err() {
            return Err(error(
                crate::error::catalog::ER_CANNOT_CONVERT_STRING,
                format!(
                    "Cannot convert string from {} to {}",
                    source_info.charset, target_info.charset
                ),
            ));
        }
    }
    Ok(())
}

/// Cache key for a subquery result. The `usize` is the subquery's AST
/// pointer (identifies the textual subquery). The `Vec<ValueKey>` is the
/// list of outer-column values that were substituted into this evaluation
/// — empty when the subquery has no outer references (the legacy "cached
/// when uncorrelated" case), populated when it does. Two evaluations of
/// the same correlated subquery with the same outer-column values are
/// guaranteed to produce the same result, so memoising by this composite
/// key is safe and turns N×M re-evaluations into N (one per distinct
/// outer-key combination).
type SubqueryCacheKey = (usize, Vec<ValueKey>);
struct InSubqueryValueSet {
    keys: hashbrown::HashSet<ValueKey, ahash::RandomState>,
    values: Vec<Value>,
    class: Option<InSubqueryHashClass>,
    collation: Option<u16>,
}
/// Hash only within an equality domain. In particular, exact DECIMAL and
/// finite floating inputs keep O(1) probes without rounding mixed domains.
#[derive(Clone, Copy, PartialEq, Eq)]
enum InSubqueryHashClass {
    Ordinary(crate::engine::expr_program::InHashClass),
    ExactNumeric,
    Float,
}

fn in_subquery_hash_class(value: &Value) -> Option<InSubqueryHashClass> {
    match value {
        Value::Int(_)
        | Value::UInt(_)
        | Value::DecimalCompact(..)
        | Value::Decimal128(..)
        | Value::DecimalWide(_) => Some(InSubqueryHashClass::ExactNumeric),
        Value::Decimal(text) if crate::util::decimal::parse_exact_decimal_big(text).is_some() => {
            Some(InSubqueryHashClass::ExactNumeric)
        }
        Value::Float(value) if value.is_finite() => Some(InSubqueryHashClass::Float),
        Value::Double(value) if value.is_finite() => Some(InSubqueryHashClass::Float),
        other => {
            crate::engine::expr_program::in_hash_class(other).map(InSubqueryHashClass::Ordinary)
        }
    }
}

fn common_in_subquery_hash_class(values: &[Value]) -> Option<InSubqueryHashClass> {
    let mut values = values.iter().filter(|value| !matches!(value, Value::Null));
    let class = in_subquery_hash_class(values.next()?)?;
    values
        .all(|value| in_subquery_hash_class(value) == Some(class))
        .then_some(class)
}

type SubqueryMemo<V> = hashbrown::HashMap<SubqueryCacheKey, V, ahash::RandomState>;

/// Soft cap on entries per cache map. Each `EvalQueryContext` lives only
/// for the duration of one top-level query, so this is a safety valve
/// against pathological queries — a workload with millions of distinct
/// outer-key combinations would not gain from per-key memoisation anyway.
const SUBQUERY_CACHE_MAX_ENTRIES: usize = 4096;

/// Insert into a per-query subquery memo, evicting one arbitrary entry
/// at the soft cap. The previous policy — refuse inserts once full —
/// meant that whichever 4096 outer keys arrived first stayed pinned and
/// every later key re-executed its subquery for the rest of the query.
fn memo_insert<V>(map: &mut SubqueryMemo<V>, key: SubqueryCacheKey, value: V) {
    if map.len() >= SUBQUERY_CACHE_MAX_ENTRIES {
        if let Some(victim) = map.keys().next().cloned() {
            map.remove(&victim);
        }
    }
    map.insert(key, value);
}

/// Per-outer-key subquery result memoisation is on by default. Set
/// `MYDB_SUBQUERY_CACHE=off` (or `0`/`false`/`no`) to disable — useful
/// for differential testing and for measuring the cache's contribution
/// to a query's wall-clock without rebuilding the binary.
fn subquery_cache_enabled() -> bool {
    match crate::util::env::var("MYDB_SUBQUERY_CACHE")
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "off" | "0" | "false" | "no" => false,
        _ => true,
    }
}

#[derive(Default)]
pub(super) struct EvalQueryContext {
    /// Row-schema metadata bound lazily for legacy expression fragments.
    /// The DB-aware evaluator is used for expressions containing subqueries,
    /// which ExprProgram deliberately cannot own. Their row-only children
    /// used to repeat name lookup for signedness and silent-conversion rules
    /// on every input row. AST nodes and the row schema are stable for this
    /// per-query context, so bind those properties once while retaining the
    /// legacy evaluator as the value/diagnostic semantics oracle.
    row_expr_metadata: hashbrown::HashMap<usize, PlanExprRuntimeMetadata, ahash::RandomState>,
    /// Effective comparison collation per binary AST node. This is likewise
    /// a row-schema property, not a row-value property.
    binary_collations: hashbrown::HashMap<usize, Option<u16>, ahash::RandomState>,
    in_subqueries: SubqueryMemo<InSubqueryValueSet>,
    tuple_in_subqueries: SubqueryMemo<Arc<ResultSet>>,
    quantified_subqueries: SubqueryMemo<Vec<Value>>,
    scalar_subqueries: SubqueryMemo<Value>,
    row_subqueries: SubqueryMemo<(Vec<Value>, Vec<Option<u16>>)>,
    exists_subqueries: SubqueryMemo<bool>,
    /// Per-statement plan cache for correlated/uncorrelated subqueries
    /// invoked from the row-eval path. Keyed by the source AST pointer
    /// (`subquery as *const Select as usize`); `None` means we attempted
    /// and gave up (unsupported shape), so subsequent calls fall back to
    /// the legacy bind+execute_select path without re-attempting. Lives
    /// only for the duration of one outer query (an `EvalQueryContext`
    /// is per `with_subquery_context` frame).
    subquery_plans: hashbrown::HashMap<usize, Option<Arc<CachedSubqueryPlan>>, ahash::RandomState>,
    /// Stable, correlation-bound templates for legacy subqueries that have no
    /// outer bindings but cannot use `CachedSubqueryPlan` (most importantly a
    /// source-free wrapper containing another scalar subquery).  Volatile
    /// wrappers must still run for every outer row, but their deterministic,
    /// uncorrelated descendants are folded once before the template is stored.
    /// This prevents `RAND() * (SELECT MAX(...))` from rescanning the MAX input
    /// once per outer row while preserving RAND's required per-row evaluation.
    legacy_subquery_templates: hashbrown::HashMap<usize, Arc<Select>, ahash::RandomState>,
    /// Per-statement memo of the dedup-rewritten form of an uncorrelated
    /// IN subquery (keyed by the source AST pointer). `IN (...)` only needs
    /// set membership, so an explosion-prone subquery body (a join can
    /// multiply each kept value into millions of duplicate rows — TPC-DS
    /// Q95's `web_returns ⋈ ws_wh`) is rewritten to `... GROUP BY <proj>`,
    /// which routes through the streaming HashJoin→HashAggregate path and
    /// stays memory-bounded instead of materialising the whole product and
    /// 1037-ing. `None` caches an ineligible shape so we don't re-test it.
    in_subquery_dedup: hashbrown::HashMap<usize, Option<Arc<Select>>, ahash::RandomState>,
    /// AST pointers of subqueries proven to have NO outer references on a
    /// prior row (empty outer binding). Correlation is a static property of
    /// the subquery + its enclosing scope, so once empty it is always empty:
    /// the result set is constant across every outer row. Membership here
    /// lets the row-eval path skip `prepare_subquery_call` entirely (which
    /// otherwise clones the subquery AST and re-derives its column/table
    /// scope per row — ~180ms over TPC-DS Q58's 24k-row date_dim scan) and
    /// hit the result memo directly with the empty-binding key.
    uncorrelated_subqueries: hashbrown::HashSet<usize, ahash::RandomState>,
}

#[derive(Clone, Copy)]
struct PlanExprRuntimeMetadata {
    silent_implicit_string_conversion: bool,
    result_unsigned: bool,
}

impl EvalQueryContext {
    fn row_expr_metadata(&mut self, expr: &Expr, row: &PlanRow) -> PlanExprRuntimeMetadata {
        let key = expr as *const Expr as usize;
        *self
            .row_expr_metadata
            .entry(key)
            .or_insert_with(|| PlanExprRuntimeMetadata {
                silent_implicit_string_conversion: plan_expr_uses_silent_implicit_string_conversion(
                    expr, row,
                ),
                result_unsigned: plan_expr_result_unsigned(expr, row),
            })
    }

    fn binary_collation_id(
        &mut self,
        expr: &Expr,
        left: &Expr,
        right: &Expr,
        row: &PlanRow,
    ) -> Option<u16> {
        let key = expr as *const Expr as usize;
        *self
            .binary_collations
            .entry(key)
            .or_insert_with(|| plan_binary_collation_id(left, right, row))
    }
}

/// Parametric form of a correlated subquery + its compiled operator. Lifted
/// outer-column references become `Expr::Placeholder(idx)` in `parametric_stmt`;
/// `bindings[idx]` carries enough info to look up the outer column value
/// in the calling row on each invocation, so the same artifact can be
/// reused across thousands of outer rows without re-planning. This is
/// the per-iteration savings that closes the gap on TPC-H Q20 / Q4 / Q17.
struct CachedSubqueryPlan {
    parametric_stmt: Arc<Select>,
    compiled: Arc<crate::engine::cache::CompiledSelectPlan>,
    bindings: Vec<OuterBindingSpec>,
}

#[derive(Clone)]
struct OuterBindingSpec {
    param_index: usize,
    qualifier: Option<String>,
    column: String,
}

/// Result of preparing a subquery call. `Cached` means we have a
/// parametric plan and pre-collected outer params; `Legacy` means we
/// fell back to the eval-time substitute+plan path.
enum SubqueryCall {
    Cached {
        cached: Arc<CachedSubqueryPlan>,
        params: Vec<Value>,
        outer_binding: Vec<ValueKey>,
    },
    Legacy {
        bound: Select,
        outer_binding: Vec<ValueKey>,
    },
}

impl SubqueryCall {
    fn outer_binding(&self) -> &Vec<ValueKey> {
        match self {
            SubqueryCall::Cached { outer_binding, .. }
            | SubqueryCall::Legacy { outer_binding, .. } => outer_binding,
        }
    }

    fn normalize_exists_projection(&mut self) {
        let SubqueryCall::Legacy { bound, .. } = self else {
            return;
        };
        if !bound.from.is_empty() || !bound.joins.is_empty() {
            return;
        }
        // EXISTS observes only whether a row is produced; its select list is
        // semantically irrelevant.  A table-less `SELECT *` in a correlated
        // EXISTS denotes the outer row, but DML expression evaluation may
        // already have substituted all outer columns before constructing a
        // PlanRow, leaving no fields with which to expand `*`.  Lower it to a
        // constant here, after correlation binding, so ordinary source-free
        // `SELECT *` still keeps its normal error outside EXISTS.
        for item in &mut bound.projection {
            if matches!(item, SelectItem::Wildcard) {
                *item = SelectItem::Expr {
                    expr: Expr::Literal(crate::parser::Literal::Int(1)),
                    alias: None,
                };
            }
        }
    }
}

thread_local! {
    /// Stack of in-flight `bind_correlated_subquery` calls. Each frame
    /// collects the outer-column `ValueKey`s that `substitute_outer_in_expr`
    /// writes into `Expr::BoundValue` nodes, in lexical order. A stack
    /// (rather than a single slot) keeps nested calls — e.g. binding a
    /// subquery that itself contains another correlated subquery whose
    /// outer references resolve to the *same* outer row — composable.
    static OUTER_BIND_STACK: RefCell<Vec<Vec<ValueKey>>> = const { RefCell::new(Vec::new()) };
}

fn push_outer_binding(value: &Value) {
    OUTER_BIND_STACK.with(|s| {
        if let Some(top) = s.borrow_mut().last_mut() {
            top.push(ValueKey::from_with_collation(
                value,
                Some(crate::charset::BINARY_COLLATION_ID),
            ));
        }
    });
}

fn with_outer_bind_collection<R>(f: impl FnOnce() -> R) -> (R, Vec<ValueKey>) {
    OUTER_BIND_STACK.with(|s| s.borrow_mut().push(Vec::new()));
    let r = f();
    let bound = OUTER_BIND_STACK
        .with(|s| s.borrow_mut().pop())
        .unwrap_or_default();
    (r, bound)
}

/// Three-valued-logic evaluation of `value [NOT] IN (subquery)` over the
/// materialized result-set keys (issue #69). `values` is the subquery's
/// first-column key set, which includes `ValueKey::Null` for any NULL rows
/// (GROUP BY dedup keeps a single NULL group). SQL semantics:
///
///   x IN S   = TRUE  if x equals some non-NULL element;
///              NULL  if not found but (x IS NULL and S non-empty) or S has NULL;
///              FALSE otherwise (incl. empty S even when x IS NULL).
///   x NOT IN S = NOT (x IN S), with NULL propagating.
fn in_subquery_result_3vl(
    values: &InSubqueryValueSet,
    value: &Value,
    negated: bool,
) -> Result<Value, QueryError> {
    let value_is_null = matches!(value, Value::Null);
    let set_has_null = values.keys.contains(&ValueKey::Null);
    let had_rows = !values.values.is_empty();
    let mut found = false;
    if !value_is_null {
        if values.class.is_some() && values.class == in_subquery_hash_class(value) {
            found = values
                .keys
                .contains(&ValueKey::from_with_collation(value, values.collation));
        } else {
            for candidate in &values.values {
                if !matches!(candidate, Value::Null)
                    && crate::engine::expr::with_compare_collation(values.collation, || {
                        crate::engine::expr::compare_values(value, candidate)
                    })? == Ordering::Equal
                {
                    found = true;
                    break;
                }
            }
        }
    }
    let in_result: Option<bool> = if found {
        Some(true)
    } else if value_is_null {
        if had_rows {
            None
        } else {
            Some(false)
        }
    } else if set_has_null {
        None
    } else {
        Some(false)
    };
    Ok(match in_result {
        Some(b) => Value::Int((if negated { !b } else { b }) as i64),
        None => Value::Null,
    })
}

#[inline]
pub(super) fn eval_plan_expr_with_db(
    db: &Arc<Database>,
    txn: TxnView,
    expr: &Expr,
    row: &PlanRow,
    eval_ctx: &mut EvalQueryContext,
) -> Result<Value, QueryError> {
    crate::engine::expr::eval_scalar_context(expr, || {
        eval_plan_expr_with_db_inner(db, txn, expr, row, eval_ctx)
    })
}

#[inline(never)]
fn eval_plan_expr_with_db_inner(
    db: &Arc<Database>,
    txn: TxnView,
    expr: &Expr,
    row: &PlanRow,
    eval_ctx: &mut EvalQueryContext,
) -> Result<Value, QueryError> {
    match expr {
        Expr::IsNull(_) | Expr::IsNotNull(_) | Expr::InList { .. } | Expr::Between { .. } => {
            eval_plan_structural_expr(expr, row, false, |e| {
                eval_plan_expr_with_db(db, txn, e, row, eval_ctx)
            })
        }
        Expr::SystemVariable { scope, name } => {
            crate::engine::expr::eval_system_variable(*scope, name, Some(db))
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            if let Expr::Tuple(items) = expr.as_ref() {
                let left = items
                    .iter()
                    .map(|item| eval_plan_expr_with_db(db, txn, item, row, eval_ctx))
                    .collect::<Result<Vec<_>, _>>()?;
                let call = prepare_subquery_call(db, txn, eval_ctx, subquery, row)?;
                let key = (
                    subquery.as_ref() as *const Select as usize,
                    call.outer_binding().clone(),
                );
                let cacheable = !super::explain::select_is_uncacheable(subquery)
                    && (subquery_cache_enabled() || key.1.is_empty());
                let result = if let Some(result) = cacheable
                    .then(|| eval_ctx.tuple_in_subqueries.get(&key))
                    .flatten()
                {
                    Arc::clone(result)
                } else {
                    let QueryResponse::ResultSet(result) = execute_subquery_call(db, txn, call)?
                    else {
                        return Err(crate::engine::expr::operand_columns_error(items.len()));
                    };
                    if result.columns.len() != items.len() {
                        return Err(crate::engine::expr::operand_columns_error(items.len()));
                    }
                    let result = Arc::new(result);
                    if cacheable {
                        memo_insert(&mut eval_ctx.tuple_in_subqueries, key, Arc::clone(&result));
                    }
                    result
                };
                let mut unknown = false;
                for right in &result.rows {
                    match crate::engine::expr::eval_row_compare_collated(
                        &left,
                        BinaryOp::Eq,
                        right,
                        |i| {
                            plan_expr_collation(&items[i], row)
                                .map(|c| c.id)
                                .or(result.columns[i].source.collation_id)
                        },
                    )? {
                        Value::Int(1) => return Ok(Value::Int(!*negated as i64)),
                        Value::Null => unknown = true,
                        _ => {}
                    }
                }
                return Ok(if unknown {
                    Value::Null
                } else {
                    Value::Int(*negated as i64)
                });
            }
            let value = eval_plan_expr_with_db(db, txn, expr, row, eval_ctx)?;
            // `IN (...)` is pure set membership, so dedup an explosion-prone
            // subquery body via a pushed GROUP BY (see `build_dedup_in_subquery`).
            // Resolve the effective subquery once and memoise it.
            let dedup_key = subquery.as_ref() as *const Select as usize;
            let dedup_arc: Option<Arc<Select>> =
                if let Some(entry) = eval_ctx.in_subquery_dedup.get(&dedup_key) {
                    entry.clone()
                } else {
                    let built = (!super::explain::select_is_uncacheable(subquery))
                        .then(|| build_dedup_in_subquery(subquery))
                        .flatten()
                        .map(Arc::new);
                    eval_ctx.in_subquery_dedup.insert(dedup_key, built.clone());
                    built
                };
            let subquery: &Select = dedup_arc.as_deref().unwrap_or(subquery);
            let sub_ptr = subquery as *const Select as usize;
            // Fast path: a subquery proven uncorrelated on an earlier row has
            // a constant result set, so skip the per-row prepare/clone and hit
            // the memo with the empty-binding key directly.
            if eval_ctx.uncorrelated_subqueries.contains(&sub_ptr) {
                if let Some(values) = eval_ctx.in_subqueries.get(&(sub_ptr, Vec::new())) {
                    return in_subquery_result_3vl(values, &value, *negated);
                }
            }
            let call = prepare_subquery_call(db, txn, eval_ctx, subquery, row)?;
            let is_uncorrelated = call.outer_binding().is_empty();
            // When the kill-switch is off, only the legacy uncorrelated
            // shape participates in the cache. Correlated lookups bypass
            // the map and run a fresh subquery every time, matching the
            // pre-cache behaviour.
            let use_cache = !super::explain::select_is_uncacheable(subquery)
                && (subquery_cache_enabled() || is_uncorrelated);
            let key: SubqueryCacheKey = (sub_ptr, call.outer_binding().clone());
            if use_cache {
                if let Some(values) = eval_ctx.in_subqueries.get(&key) {
                    if is_uncorrelated {
                        eval_ctx.uncorrelated_subqueries.insert(sub_ptr);
                    }
                    return in_subquery_result_3vl(values, &value, *negated);
                }
            }
            let QueryResponse::ResultSet(result) = execute_subquery_call(db, txn, call)? else {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "IN subquery must return rows",
                ));
            };
            if result.columns.len() != 1 {
                return Err(error(
                    crate::error::catalog::ER_OPERAND_COLUMNS,
                    "Operand should contain 1 column(s)",
                ));
            }
            // These sets are probed once per outer row. SipHash's collision
            // resistance is unnecessary for query-local typed values and was
            // a measurable hot path for large fact-table IN predicates.
            let collation = plan_expr_collation(expr, row)
                .map(|c| c.id)
                .or(result.columns[0].source.collation_id);
            let raw: Vec<Value> = result
                .rows
                .into_iter()
                .filter_map(|row| row.into_iter().next())
                .collect();
            let class = common_in_subquery_hash_class(&raw);
            let keys = raw
                .iter()
                .map(|v| ValueKey::from_with_collation(v, collation))
                .collect();
            let values = InSubqueryValueSet {
                keys,
                values: raw,
                class,
                collation,
            };
            let result = in_subquery_result_3vl(&values, &value, *negated)?;
            if use_cache {
                memo_insert(&mut eval_ctx.in_subqueries, key, values);
            }
            if is_uncorrelated {
                eval_ctx.uncorrelated_subqueries.insert(sub_ptr);
            }
            Ok(result)
        }
        Expr::QuantifiedComparison {
            left,
            op,
            quantifier,
            right,
        } => {
            // MySQL's quantified-comparison materialization does not publish
            // conversion diagnostics produced while evaluating a scalar
            // subquery used as the left operand. The scalar value still uses
            // the ordinary coercion rules; only its nested diagnostic area is
            // discarded before ANY/ALL consumes it.
            let left_value = if matches!(left.as_ref(), Expr::Subquery(_)) {
                crate::engine::expr::with_suppressed_expression_warnings(|| {
                    eval_plan_expr_with_db(db, txn, left, row, eval_ctx)
                })?
            } else {
                eval_plan_expr_with_db(db, txn, left, row, eval_ctx)?
            };
            match right {
                QuantifiedComparisonRight::Subquery(subquery) => {
                    let sub_ptr = subquery.as_ref() as *const Select as usize;
                    if eval_ctx.uncorrelated_subqueries.contains(&sub_ptr) {
                        if let Some(values) =
                            eval_ctx.quantified_subqueries.get(&(sub_ptr, Vec::new()))
                        {
                            let mut iter = values.iter();
                            return crate::engine::expr::eval_quantified_comparison(
                                left_value,
                                *op,
                                *quantifier,
                                || match iter.next() {
                                    Some(value) => Ok(Some(value.clone())),
                                    None => Ok(None),
                                },
                            );
                        }
                    }
                    let call = prepare_subquery_call(db, txn, eval_ctx, subquery, row)?;
                    let is_uncorrelated = call.outer_binding().is_empty();
                    let use_cache = !super::explain::select_is_uncacheable(subquery)
                        && (subquery_cache_enabled() || is_uncorrelated);
                    let key: SubqueryCacheKey = (sub_ptr, call.outer_binding().clone());
                    if use_cache {
                        if let Some(values) = eval_ctx.quantified_subqueries.get(&key) {
                            if is_uncorrelated {
                                eval_ctx.uncorrelated_subqueries.insert(sub_ptr);
                            }
                            let mut iter = values.iter();
                            return crate::engine::expr::eval_quantified_comparison(
                                left_value,
                                *op,
                                *quantifier,
                                || match iter.next() {
                                    Some(value) => Ok(Some(value.clone())),
                                    None => Ok(None),
                                },
                            );
                        }
                    }
                    let QueryResponse::ResultSet(result) = execute_subquery_call(db, txn, call)?
                    else {
                        return Err(error(
                            crate::error::catalog::ER_NOT_SUPPORTED_YET,
                            "ANY/ALL/SOME subquery must return rows",
                        ));
                    };
                    if result.columns.len() != 1 {
                        return Err(error(
                            crate::error::catalog::ER_OPERAND_COLUMNS,
                            "Operand should contain 1 column(s)",
                        ));
                    }
                    let values: Vec<Value> = result
                        .rows
                        .iter()
                        .filter_map(|sub_row| sub_row.first().cloned())
                        .collect();
                    let mut iter = values.iter();
                    let result = crate::engine::expr::eval_quantified_comparison(
                        left_value,
                        *op,
                        *quantifier,
                        || match iter.next() {
                            Some(value) => Ok(Some(value.clone())),
                            None => Ok(None),
                        },
                    )?;
                    if use_cache {
                        memo_insert(&mut eval_ctx.quantified_subqueries, key, values);
                    }
                    if is_uncorrelated {
                        eval_ctx.uncorrelated_subqueries.insert(sub_ptr);
                    }
                    Ok(result)
                }
                QuantifiedComparisonRight::List(list) => {
                    let mut iter = list.iter();
                    crate::engine::expr::eval_quantified_comparison(
                        left_value,
                        *op,
                        *quantifier,
                        || match iter.next() {
                            Some(item) => {
                                Ok(Some(eval_plan_expr_with_db(db, txn, item, row, eval_ctx)?))
                            }
                            None => Ok(None),
                        },
                    )
                }
            }
        }
        Expr::Subquery(subquery) => {
            let sub_ptr = subquery.as_ref() as *const Select as usize;
            let cacheable = !super::explain::select_is_uncacheable(subquery);
            if cacheable && eval_ctx.uncorrelated_subqueries.contains(&sub_ptr) {
                if let Some(cached) = eval_ctx.scalar_subqueries.get(&(sub_ptr, Vec::new())) {
                    return Ok(cached.clone());
                }
            }
            let call = prepare_subquery_call(db, txn, eval_ctx, subquery, row)?;
            let is_uncorrelated = call.outer_binding().is_empty();
            let use_cache = cacheable && (subquery_cache_enabled() || is_uncorrelated);
            let key: SubqueryCacheKey = (sub_ptr, call.outer_binding().clone());
            if use_cache {
                if let Some(cached) = eval_ctx.scalar_subqueries.get(&key) {
                    if is_uncorrelated {
                        eval_ctx.uncorrelated_subqueries.insert(sub_ptr);
                    }
                    return Ok(cached.clone());
                }
            }
            let QueryResponse::ResultSet(result) = execute_subquery_call(db, txn, call)? else {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "scalar subquery must return rows",
                ));
            };
            if result.columns.len() != 1 {
                return Err(crate::engine::expr::operand_columns_error(1));
            }
            if result.rows.len() > 1 {
                return Err(error(
                    crate::error::catalog::ER_SUBQUERY_NO_1_ROW,
                    "Subquery returns more than 1 row",
                ));
            }
            let value = result
                .rows
                .first()
                .and_then(|row| row.first())
                .cloned()
                .unwrap_or(Value::Null);
            if use_cache {
                memo_insert(&mut eval_ctx.scalar_subqueries, key, value.clone());
            }
            if cacheable && is_uncorrelated {
                eval_ctx.uncorrelated_subqueries.insert(sub_ptr);
            }
            Ok(value)
        }
        Expr::Exists { subquery, negated } => {
            let sub_ptr = subquery.as_ref() as *const Select as usize;
            if eval_ctx.uncorrelated_subqueries.contains(&sub_ptr) {
                if let Some(&exists) = eval_ctx.exists_subqueries.get(&(sub_ptr, Vec::new())) {
                    return Ok(Value::Int((if *negated { !exists } else { exists }) as i64));
                }
            }
            let mut call = prepare_subquery_call(db, txn, eval_ctx, subquery, row)?;
            call.normalize_exists_projection();
            let is_uncorrelated = call.outer_binding().is_empty();
            let use_cache = !super::explain::select_is_uncacheable(subquery)
                && (subquery_cache_enabled() || is_uncorrelated);
            let key: SubqueryCacheKey = (sub_ptr, call.outer_binding().clone());
            if use_cache {
                if let Some(&exists) = eval_ctx.exists_subqueries.get(&key) {
                    if is_uncorrelated {
                        eval_ctx.uncorrelated_subqueries.insert(sub_ptr);
                    }
                    return Ok(Value::Int((if *negated { !exists } else { exists }) as i64));
                }
            }
            let QueryResponse::ResultSet(result) = execute_subquery_call(db, txn, call)? else {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "EXISTS subquery must return rows",
                ));
            };
            let exists = !result.rows.is_empty();
            if use_cache {
                memo_insert(&mut eval_ctx.exists_subqueries, key, exists);
            }
            if is_uncorrelated {
                eval_ctx.uncorrelated_subqueries.insert(sub_ptr);
            }
            Ok(Value::Int((if *negated { !exists } else { exists }) as i64))
        }
        Expr::Unary { op, expr } => match op {
            UnaryOp::Neg => {
                let value = eval_plan_expr_with_db(db, txn, expr, row, eval_ctx)?;
                let evaluate = || crate::engine::expr::eval_negate(value);
                if eval_ctx
                    .row_expr_metadata(expr, row)
                    .silent_implicit_string_conversion
                {
                    crate::engine::expr::with_suppressed_expression_warnings(evaluate)
                } else {
                    evaluate()
                }
            }
            UnaryOp::Not => Ok(crate::engine::expr::eval_logical_not(
                &eval_plan_expr_with_db(db, txn, expr, row, eval_ctx)?,
            )),
            UnaryOp::BitNot => crate::engine::expr::eval_bit_not(eval_plan_expr_with_db(
                db, txn, expr, row, eval_ctx,
            )?),
        },
        Expr::Binary { left, op, right } if *op == BinaryOp::And || *op == BinaryOp::Or => {
            let l = crate::engine::expr::logical_value(&eval_plan_expr_with_db(
                db, txn, left, row, eval_ctx,
            )?);
            if *op == BinaryOp::And && l == Some(false) {
                return Ok(Value::Int(0));
            }
            if *op == BinaryOp::Or && l == Some(true) {
                return Ok(Value::Int(1));
            }
            let r = crate::engine::expr::logical_value(&eval_plan_expr_with_db(
                db, txn, right, row, eval_ctx,
            )?);
            Ok(crate::engine::expr::combine_logical(*op, l, r))
        }
        Expr::Binary { left, op, right } => {
            if let Some(v) = eval_row_subquery_comparison(db, txn, left, *op, right, row, eval_ctx)?
            {
                return Ok(v);
            }
            if let Some(v) = try_eval_row_comparison(left, *op, right, row, |e| {
                eval_plan_expr_with_db(db, txn, e, row, eval_ctx)
            })? {
                return Ok(v);
            }
            let comparison = crate::parser::boolean::is_comparison!(*op);
            let mut left_value = if comparison {
                crate::engine::expr::exact_decimal_comparison_literal(left)
                    .map(Ok)
                    .unwrap_or_else(|| eval_plan_expr_with_db(db, txn, left, row, eval_ctx))?
            } else {
                eval_plan_expr_with_db(db, txn, left, row, eval_ctx)?
            };
            let mut right_value = if comparison {
                crate::engine::expr::exact_decimal_comparison_literal(right)
                    .map(Ok)
                    .unwrap_or_else(|| eval_plan_expr_with_db(db, txn, right, row, eval_ctx))?
            } else {
                eval_plan_expr_with_db(db, txn, right, row, eval_ctx)?
            };
            if matches!(
                op,
                BinaryOp::Add
                    | BinaryOp::Sub
                    | BinaryOp::Mul
                    | BinaryOp::Div
                    | BinaryOp::IntDiv
                    | BinaryOp::Mod
                    | BinaryOp::ShiftLeft
                    | BinaryOp::ShiftRight
            ) {
                if crate::engine::expr::expr_is_numeric_hexbit_literal(left) {
                    left_value = crate::engine::expr::coerce_hex_bytes_to_uint(left_value);
                }
                if crate::engine::expr::expr_is_numeric_hexbit_literal(right) {
                    right_value = crate::engine::expr::coerce_hex_bytes_to_uint(right_value);
                }
            } else if matches!(op, BinaryOp::BitAnd | BinaryOp::BitOr | BinaryOp::BitXor) {
                crate::engine::expr::coerce_hexbit_literals_for_bitwise(
                    left,
                    right,
                    &mut left_value,
                    &mut right_value,
                );
            } else if crate::parser::boolean::is_comparison!(*op) {
                if crate::engine::expr::expr_is_numeric_hexbit_literal(left)
                    && crate::engine::expr::value_is_numeric(&right_value)
                {
                    left_value = crate::engine::expr::coerce_hex_bytes_to_uint(left_value);
                }
                if crate::engine::expr::expr_is_numeric_hexbit_literal(right)
                    && crate::engine::expr::value_is_numeric(&left_value)
                {
                    right_value = crate::engine::expr::coerce_hex_bytes_to_uint(right_value);
                }
            }
            if matches!(
                op,
                BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod
            ) {
                let evaluate = || {
                    crate::engine::expr::eval_arithmetic_expression(
                        left,
                        right,
                        &left_value,
                        *op,
                        &right_value,
                    )
                };
                let left_metadata = eval_ctx.row_expr_metadata(left, row);
                let right_metadata = eval_ctx.row_expr_metadata(right, row);
                let value = if left_metadata.silent_implicit_string_conversion
                    || right_metadata.silent_implicit_string_conversion
                {
                    crate::engine::expr::with_suppressed_expression_warnings(evaluate)?
                } else {
                    evaluate()?
                };
                return crate::engine::expr::enforce_unsigned_subtraction_result(
                    value,
                    *op == BinaryOp::Sub
                        && (left_metadata.result_unsigned || right_metadata.result_unsigned),
                );
            }
            if crate::parser::boolean::is_comparison!(*op) {
                let collation_id = eval_ctx.binary_collation_id(expr, left, right, row);
                let evaluate = || {
                    crate::engine::expr::with_numeric_comparison_context(
                        left,
                        right,
                        &left_value,
                        &right_value,
                        || {
                            crate::engine::expr::with_compare_collation(collation_id, || {
                                crate::engine::expr::null_cmp_binary(&left_value, *op, &right_value)
                            })
                        },
                    )
                };
                let left_metadata = eval_ctx.row_expr_metadata(left, row);
                let right_metadata = eval_ctx.row_expr_metadata(right, row);
                return if left_metadata.silent_implicit_string_conversion
                    || right_metadata.silent_implicit_string_conversion
                {
                    crate::engine::expr::with_suppressed_expression_warnings(evaluate)
                } else {
                    evaluate()
                };
            }
            eval_binary_values(left_value, *op, right_value)
        }
        // See `eval_plan_expr_legacy`: MATCH resolves to the FulltextScan's
        // hidden BM25 score pseudo-column by fingerprint.
        Expr::Function { name, .. } if name == crate::parser::FT_MATCH_FUNCTION => {
            resolve_fulltext_score(expr, row)
        }
        Expr::Function { name, args, .. } if planner::is_aggregate_function(name) => Err(error(
            crate::error::catalog::ER_INVALID_GROUP_FUNC_USE,
            format!("invalid use of group function '{name}'"),
        )),
        Expr::Function { name, args, .. } => eval_plan_scalar_function(name, args, row, |arg| {
            eval_plan_expr_with_db(db, txn, arg, row, eval_ctx)
        }),
        Expr::Default(Some(name)) => resolve_plan_default(None, name, row),
        Expr::DefaultQualified(parts) => resolve_plan_default(
            parts.iter().rev().nth(1).map(String::as_str),
            parts.last().map(String::as_str).unwrap_or(""),
            row,
        ),
        Expr::Default(None) => Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "DEFAULT is only supported in DML default contexts",
        )),
        Expr::Tuple(_) => Err(crate::engine::expr::operand_columns_error(1)),
        other => eval_plan_expr(other, row),
    }
}

/// `DEFAULT(col)` in a plan-row context (SELECT projection / WHERE):
/// locate the column's source table through the plan-row metadata and
/// return its declared default. Requires the subquery (db, txn) frame —
/// installed by every storage-backed execution path — to reach the
/// catalog; aliased derived tables without a catalog identity keep the
/// legacy "DML contexts only" error.
fn resolve_plan_default(
    qualifier: Option<&str>,
    name: &str,
    row: &PlanRow,
) -> Result<Value, QueryError> {
    let field = resolve_plan_field(row, qualifier, name)?;
    let frame = current_subquery_frame().ok_or_else(|| {
        error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "DEFAULT is only supported in DML default contexts",
        )
    })?;
    let source = &field.meta.source;
    let table_name = if !source.schema.is_empty() && !source.org_table.is_empty() {
        format!("{}.{}", source.schema, source.org_table)
    } else {
        field.table.clone()
    };
    let table = frame.db.table(&table_name)?;
    let idx = table.column_index(&field.column)?;
    let column = &table.columns[idx];
    if column.default.is_none() && column.default_expr.is_none() && !column.nullable {
        return Err(error(
            crate::error::catalog::ER_NO_DEFAULT_FOR_FIELD,
            format!("Field '{name}' doesn't have a default value"),
        ));
    }
    Ok(crate::engine::expr::resolve_column_default_value(column))
}

#[inline]
pub(super) fn eval_aggregate_expr(
    expr: &Expr,
    group: &[PlanRow],
    row: &PlanRow,
) -> Result<Value, QueryError> {
    crate::engine::expr::eval_scalar_context(expr, || eval_aggregate_expr_inner(expr, group, row))
}

#[inline(never)]
fn eval_aggregate_expr_inner(
    expr: &Expr,
    group: &[PlanRow],
    row: &PlanRow,
) -> Result<Value, QueryError> {
    match expr {
        Expr::Case { .. }
        | Expr::InList { .. }
        | Expr::Between { .. }
        | Expr::Like { .. }
        | Expr::IsNull(_)
        | Expr::IsNotNull(_) => {
            eval_plan_structural_expr(expr, row, true, |e| eval_aggregate_expr(e, group, row))
        }
        Expr::Function { name, .. } if planner::is_aggregate_function(name) => {
            // Phase A: prefer pre-computed accumulator results when the
            // emitting operator (HashAggregate / CTE-join group-by) has
            // populated the cache. Falls back to the legacy group-iteration
            // path for callers that still pass a non-empty `group`.
            if let Some(cache) = row.agg_values.as_ref() {
                let key = super::aggregate::aggregate_key(expr);
                if let Some(v) = cache.get(&key) {
                    return coerce_aggregate_result_for_expr(expr, row, v.clone());
                }
            }
            coerce_aggregate_result_for_expr(expr, row, eval_aggregate_function(expr, group)?)
        }
        Expr::GroupKeyRef { index, .. } => {
            let column = format!("group_{index}");
            resolve_plan_field(row, Some("$group"), &column).map(|field| field.value.clone())
        }
        Expr::Grouping(indices) => {
            let mask = row
                .agg_values
                .as_ref()
                .and_then(|cache| cache.get(&super::aggregate::ROLLUP_MASK_AGG_KEY))
                .and_then(|value| match value {
                    Value::Int(v) => Some(*v as u64),
                    Value::UInt(v) => Some(*v),
                    _ => None,
                })
                .unwrap_or(0);
            let mut out = 0i64;
            for idx in indices {
                out = (out << 1) | (((mask >> idx) & 1) as i64);
            }
            Ok(Value::Int(out))
        }
        Expr::Unary { op, expr } => match op {
            UnaryOp::Neg => {
                let value = eval_aggregate_expr(expr, group, row)?;
                let evaluate = || crate::engine::expr::eval_negate(value);
                if plan_expr_uses_silent_implicit_string_conversion(expr, row) {
                    crate::engine::expr::with_suppressed_expression_warnings(evaluate)
                } else {
                    evaluate()
                }
            }
            UnaryOp::Not => Ok(crate::engine::expr::eval_logical_not(&eval_aggregate_expr(
                expr, group, row,
            )?)),
            UnaryOp::BitNot => {
                crate::engine::expr::eval_bit_not(eval_aggregate_expr(expr, group, row)?)
            }
        },
        Expr::Binary { left, op, right } => {
            if let Some(value) = try_eval_row_comparison(left, *op, right, row, |item| {
                eval_aggregate_expr(item, group, row)
            })? {
                return Ok(value);
            }
            let comparison = crate::parser::boolean::is_comparison!(*op);
            let mut left_value = if comparison {
                crate::engine::expr::exact_decimal_comparison_literal(left)
                    .map(Ok)
                    .unwrap_or_else(|| eval_aggregate_expr(left, group, row))?
            } else {
                eval_aggregate_expr(left, group, row)?
            };
            let mut right_value = if comparison {
                crate::engine::expr::exact_decimal_comparison_literal(right)
                    .map(Ok)
                    .unwrap_or_else(|| eval_aggregate_expr(right, group, row))?
            } else {
                eval_aggregate_expr(right, group, row)?
            };
            if crate::parser::boolean::is_comparison!(*op) {
                let collation_id = plan_binary_collation_id(left, right, row);
                let evaluate = || {
                    crate::engine::expr::with_numeric_comparison_context(
                        left,
                        right,
                        &left_value,
                        &right_value,
                        || {
                            crate::engine::expr::with_compare_collation(collation_id, || {
                                crate::engine::expr::null_cmp_binary(&left_value, *op, &right_value)
                            })
                        },
                    )
                };
                return if plan_expr_uses_silent_implicit_string_conversion(left, row)
                    || plan_expr_uses_silent_implicit_string_conversion(right, row)
                {
                    crate::engine::expr::with_suppressed_expression_warnings(evaluate)
                } else {
                    evaluate()
                };
            }
            let is_binary_bit_aggregate = |expr: &Expr| {
                matches!(
                    expr,
                    Expr::Function { name, .. }
                        if matches!(
                            name.to_ascii_uppercase().as_str(),
                            "BIT_AND" | "BIT_OR" | "BIT_XOR"
                        )
                )
            };
            if matches!(
                op,
                BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod
            ) {
                let silent_numeric = |value: &mut Value| {
                    if let Value::Bytes(bytes) = value {
                        let text = String::from_utf8_lossy(bytes);
                        *value =
                            Value::Double(crate::util::decimal::parse_lenient_f64(&text).value);
                    }
                };
                if is_binary_bit_aggregate(left) {
                    silent_numeric(&mut left_value);
                }
                if is_binary_bit_aggregate(right) {
                    silent_numeric(&mut right_value);
                }
            }
            if matches!(
                op,
                BinaryOp::BitAnd
                    | BinaryOp::BitOr
                    | BinaryOp::BitXor
                    | BinaryOp::ShiftLeft
                    | BinaryOp::ShiftRight
            ) {
                if crate::engine::expr::expr_is_numeric_hexbit_literal(left) {
                    left_value = crate::engine::expr::coerce_hex_bytes_to_uint(left_value);
                }
                if crate::engine::expr::expr_is_numeric_hexbit_literal(right) {
                    right_value = crate::engine::expr::coerce_hex_bytes_to_uint(right_value);
                }
                return crate::engine::expr::eval_bitwise_with_string_modes(
                    left_value,
                    *op,
                    right_value,
                    is_binary_bit_aggregate(left),
                    is_binary_bit_aggregate(right),
                );
            }
            let evaluate = || eval_binary_values(left_value, *op, right_value);
            let value = if matches!(
                op,
                BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod
            ) && (plan_expr_uses_silent_implicit_string_conversion(left, row)
                || plan_expr_uses_silent_implicit_string_conversion(right, row))
            {
                crate::engine::expr::with_suppressed_expression_warnings(evaluate)?
            } else {
                evaluate()?
            };
            if matches!(
                op,
                BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod
            ) && (is_binary_bit_aggregate(left) || is_binary_bit_aggregate(right))
            {
                let decimal_scale = |expr: &Expr| match expr {
                    Expr::Literal(Literal::Decimal(text)) => {
                        text.split_once('.').map(|(_, fraction)| fraction.len())
                    }
                    _ => None,
                };
                if let Some(scale) = decimal_scale(left).or_else(|| decimal_scale(right)) {
                    if let Value::Double(number) = value {
                        return Ok(Value::Decimal(format!("{number:.scale$}")));
                    }
                }
            }
            Ok(value)
        }
        // Scalar function (COALESCE/IFNULL/ROUND…) arguments can nest aggregate calls and
        // must recurse in the aggregate context; falling through to eval_plan_expr would raise 1111 on inner aggregates.
        // MATCH(...) still goes through BM25 pseudo-column resolution.
        Expr::Function { name, args, .. } if name != crate::parser::FT_MATCH_FUNCTION => {
            if name.eq_ignore_ascii_case("CONCAT") || name.eq_ignore_ascii_case("CONCAT_WS") {
                // A real column has coercibility 2 and therefore determines
                // the result charset over computed/aggregate operands. Check
                // the fully evaluated values here: a locale-sensitive
                // DATE_FORMAT nested under MIN may contain characters that
                // cannot be represented by an ASCII column's charset, and an
                // empty/static-only check cannot see that runtime text.
                let target = args
                    .iter()
                    .filter_map(|arg| {
                        plan_expr_collation(arg, row).or_else(|| {
                            group
                                .first()
                                .and_then(|input_row| plan_expr_collation(arg, input_row))
                        })
                    })
                    .next();
                let values = args
                    .iter()
                    .map(|arg| eval_aggregate_expr(arg, group, row))
                    .collect::<Result<Vec<_>, _>>()?;
                if let Some(target) = target.filter(|target| target.charset != "binary") {
                    for value in &values {
                        if matches!(value, Value::Null) {
                            continue;
                        }
                        let text = value_to_string(value);
                        if crate::charset::encode_string(target.charset, &text).is_err() {
                            return Err(error(
                                crate::error::catalog::ER_CANNOT_CONVERT_STRING,
                                format!(
                                    "Cannot convert string '{}' to {}",
                                    text.chars().take(6).collect::<String>(),
                                    target.charset
                                ),
                            ));
                        }
                    }
                }
                let mut values = values.into_iter();
                return eval_function(name, args, |_| Ok(values.next().unwrap_or(Value::Null)));
            }
            if name.eq_ignore_ascii_case("GREATEST") || name.eq_ignore_ascii_case("LEAST") {
                validate_plan_string_combinator(args, row)?;
            }
            let bound_args;
            let source_row = group.first().unwrap_or(row);
            let args = if name.eq_ignore_ascii_case("CHARSET")
                || name.eq_ignore_ascii_case("COLLATION")
                || name.eq_ignore_ascii_case("COERCIBILITY")
            {
                bound_args = bind_metadata_inspection_collation(args, source_row);
                bound_args.as_slice()
            } else if crate::engine::expr::is_string_search_function(name) {
                bound_args = bind_string_search_collations(args, source_row);
                bound_args.as_slice()
            } else {
                args
            };
            eval_function(name, args, |arg| {
                let value = eval_aggregate_expr(arg, group, row)?;
                function_arg_with_source_octets(name, arg, row, value)
            })
        }
        Expr::Subquery(subquery)
            if subquery.table.is_none()
                && subquery.from.is_empty()
                && subquery.derived.is_none()
                && subquery.joins.is_empty()
                && subquery.selection.is_none()
                && subquery.group_by.is_empty()
                && subquery.having.is_none()
                && subquery.projection.len() == 1 =>
        {
            let SelectItem::Expr { expr: inner, .. } = &subquery.projection[0] else {
                return eval_plan_expr(expr, row);
            };
            struct BindOuterAggregates<'a> {
                group: &'a [PlanRow],
                row: &'a PlanRow,
                depth: usize,
                bound: bool,
                error: Option<QueryError>,
            }
            impl crate::parser::visit::AstVisitorMut for BindOuterAggregates<'_> {
                fn enter_select(&mut self, _: &mut Select) {
                    self.depth += 1;
                }
                fn exit_select(&mut self, _: &mut Select) {
                    self.depth -= 1;
                }
                fn enter_expr(&mut self, expr: &mut Expr) {
                    if self.depth != 0 || self.error.is_some() {
                        return;
                    }
                    if matches!(expr, Expr::Function { name, args, .. }
                        if planner::is_aggregate_function(name) && args.iter().filter(|arg| !matches!(arg, Expr::Wildcard)).any(expr_contains_column_reference))
                    {
                        match eval_aggregate_expr(expr, self.group, self.row) {
                            Ok(value) => {
                                *expr = Expr::BoundValue(value);
                                self.bound = true;
                            }
                            Err(err) => self.error = Some(err),
                        }
                    }
                }
            }
            let mut bound_inner = inner.clone();
            let mut binder = BindOuterAggregates {
                group,
                row,
                depth: 0,
                bound: false,
                error: None,
            };
            crate::parser::visit::visit_expr_mut(&mut binder, &mut bound_inner);
            if let Some(err) = binder.error {
                return Err(err);
            }
            if binder.bound {
                // Local COUNT(*) still belongs to the implicit inner row.
                if planner::has_aggregate_expr(&bound_inner) {
                    let mut implicit_row = row.clone();
                    implicit_row.group = None;
                    implicit_row.agg_values = None;
                    return eval_aggregate_expr(
                        &bound_inner,
                        std::slice::from_ref(&implicit_row),
                        &implicit_row,
                    );
                }
                return eval_plan_expr(&bound_inner, row);
            }
            eval_plan_expr(expr, row)
        }
        other => eval_plan_expr(other, row),
    }
}

pub(super) fn eval_aggregate_bool(
    expr: &Expr,
    group: &[PlanRow],
    row: &PlanRow,
) -> Result<bool, QueryError> {
    Ok(truthy(&eval_aggregate_expr(expr, group, row)?))
}

/// Row-group fallback uses the same slot binding and accumulator semantics
/// as streamed/compiled aggregation, without retaining an extra value vector.
pub(super) fn eval_aggregate_function(expr: &Expr, group: &[PlanRow]) -> Result<Value, QueryError> {
    if let Expr::Function { name, args, .. } = expr {
        if name.eq_ignore_ascii_case("JSON_OBJECTAGG") && args.len() != 2 {
            return Err(error(
                crate::error::catalog::ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT,
                "incorrect parameter count in the call to native function 'JSON_OBJECTAGG'",
            ));
        }
    }
    let slot = super::aggregate::AggregateSlot::from_expr(expr).ok_or_else(|| {
        error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "expected aggregate function expression",
        )
    })?;
    let mut accumulator = group.first().map_or_else(
        || slot.make_accumulator(),
        |row| slot.make_accumulator_for_row(row),
    );
    if slot.is_count_star {
        accumulator.update_count_star_batch(group.len());
    } else {
        for row in group {
            crate::engine::cancel::check_cancelled()?;
            let value = slot
                .args
                .first()
                .map(|arg| eval_plan_expr(arg, row))
                .transpose()?
                .unwrap_or(Value::Null);
            if slot.has_order_by() {
                let keys = slot.eval_order_keys(|arg| eval_plan_expr(arg, row))?;
                accumulator.update_ordered(value, keys)?;
            } else {
                accumulator.update(value)?;
            }
        }
    }
    accumulator.finalize()
}

fn eval_row_subquery_comparison(
    db: &Arc<Database>,
    txn: TxnView,
    left: &Expr,
    op: BinaryOp,
    right: &Expr,
    row: &PlanRow,
    eval_ctx: &mut EvalQueryContext,
) -> Result<Option<Value>, QueryError> {
    if !crate::parser::boolean::is_comparison!(op) {
        return Ok(None);
    }
    let (tuple, subquery, tuple_on_left) = match (left, right) {
        (Expr::Tuple(tuple), Expr::Subquery(subquery)) => (tuple, subquery, true),
        (Expr::Subquery(subquery), Expr::Tuple(tuple)) => (tuple, subquery, false),
        _ => return Ok(None),
    };
    let tuple_values = tuple
        .iter()
        .map(|expr| eval_plan_expr_with_db(db, txn, expr, row, eval_ctx))
        .collect::<Result<Vec<_>, _>>()?;

    let sub_ptr = subquery.as_ref() as *const Select as usize;
    let call = prepare_subquery_call(db, txn, eval_ctx, subquery, row)?;
    let is_uncorrelated = call.outer_binding().is_empty();
    let use_cache = !super::explain::select_is_uncacheable(subquery)
        && (subquery_cache_enabled() || is_uncorrelated);
    let key: SubqueryCacheKey = (sub_ptr, call.outer_binding().clone());
    let cached = use_cache
        .then(|| eval_ctx.row_subqueries.get(&key).cloned())
        .flatten();
    let (subquery_values, collations) = match cached {
        Some(values) => values,
        None => {
            let QueryResponse::ResultSet(result) = execute_subquery_call(db, txn, call)? else {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "row subquery must return rows",
                ));
            };
            if result.columns.len() != tuple.len() {
                return Err(crate::engine::expr::operand_columns_error(tuple.len()));
            }
            if result.rows.len() > 1 {
                return Err(error(
                    crate::error::catalog::ER_SUBQUERY_NO_1_ROW,
                    "Subquery returns more than 1 row",
                ));
            }
            let collations: Vec<Option<u16>> = result
                .columns
                .iter()
                .map(|c| c.source.collation_id)
                .collect();
            let values = result
                .rows
                .into_iter()
                .next()
                .unwrap_or_else(|| vec![Value::Null; tuple.len()]);
            if use_cache {
                memo_insert(
                    &mut eval_ctx.row_subqueries,
                    key,
                    (values.clone(), collations.clone()),
                );
            }
            (values, collations)
        }
    };
    if is_uncorrelated {
        eval_ctx.uncorrelated_subqueries.insert(sub_ptr);
    }
    let (left_values, right_values) = if tuple_on_left {
        (&tuple_values, &subquery_values)
    } else {
        (&subquery_values, &tuple_values)
    };
    Ok(Some(crate::engine::expr::eval_row_compare_collated(
        left_values,
        op,
        right_values,
        |i| {
            plan_expr_collation(&tuple[i], row)
                .map(|c| c.id)
                .or(collations[i])
        },
    )?))
}

/// Row-constructor comparison over a plan row: `(a,b) op (c,d)`. Returns
/// `Some(result)` when this is a tuple comparison; `None` when it is an
/// ordinary scalar binary the caller should evaluate itself. `eval_item`
/// evaluates one operand against whatever row context the caller holds, so
/// this stays shared between the legacy and with-db plan eval paths.
/// Value semantics delegate to `expr::eval_row_compare_collated` (single source
/// of truth with the storage-row path).
pub(super) fn try_eval_row_comparison<F>(
    left: &Expr,
    op: BinaryOp,
    right: &Expr,
    row: &PlanRow,
    mut eval_item: F,
) -> Result<Option<Value>, QueryError>
where
    F: FnMut(&Expr) -> Result<Value, QueryError>,
{
    let is_cmp = crate::parser::boolean::is_comparison!(op);
    if !is_cmp || (!matches!(left, Expr::Tuple(_)) && !matches!(right, Expr::Tuple(_))) {
        return Ok(None);
    }
    let (Expr::Tuple(litems), Expr::Tuple(ritems)) = (left, right) else {
        let n = match left {
            Expr::Tuple(items) => items.len(),
            _ => 1,
        };
        return Err(crate::engine::expr::operand_columns_error(n));
    };
    if litems.len() != ritems.len() {
        return Err(crate::engine::expr::operand_columns_error(litems.len()));
    }
    let lv = litems
        .iter()
        .map(&mut eval_item)
        .collect::<Result<Vec<_>, _>>()?;
    let rv = ritems
        .iter()
        .map(&mut eval_item)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(crate::engine::expr::eval_row_compare_collated(
        &lv,
        op,
        &rv,
        |i| plan_binary_collation_id(&litems[i], &ritems[i], row),
    )?))
}

/// Row-constructor `IN` over a plan row: `(a,b) IN ((1,2),(3,4))`.
pub(super) fn eval_row_in_list_plan<F>(
    left_items: &[Expr],
    list: &[Expr],
    negated: bool,
    row: &PlanRow,
    mut eval_item: F,
) -> Result<Value, QueryError>
where
    F: FnMut(&Expr) -> Result<Value, QueryError>,
{
    let arity = left_items.len();
    // Arity is resolve-time in MySQL: validate every list element's column
    // count before any matching (independent of short-circuit).
    for item in list {
        match item {
            Expr::Tuple(ritems) if ritems.len() == arity => {}
            _ => return Err(crate::engine::expr::operand_columns_error(arity)),
        }
    }
    let lv = left_items
        .iter()
        .map(&mut eval_item)
        .collect::<Result<Vec<_>, _>>()?;
    let mut found = false;
    let mut has_null = false;
    for item in list {
        let Expr::Tuple(ritems) = item else {
            unreachable!("arity pre-validated above")
        };
        let rv = ritems
            .iter()
            .map(&mut eval_item)
            .collect::<Result<Vec<_>, _>>()?;
        match crate::engine::expr::eval_row_compare_collated(&lv, BinaryOp::Eq, &rv, |i| {
            plan_binary_collation_id(&left_items[i], &ritems[i], row)
        })? {
            Value::Int(1) => {
                found = true;
                break;
            }
            Value::Null => has_null = true,
            _ => {}
        }
    }
    if !found && has_null {
        return Ok(Value::Null);
    }
    Ok(Value::Int((if negated { !found } else { found }) as i64))
}

fn plan_expr_result_unsigned(expr: &Expr, row: &PlanRow) -> bool {
    crate::engine::expr::expr_result_unsigned(expr, &|column_expr| {
        let field = match column_expr {
            Expr::Identifier(name) => resolve_plan_field(row, None, name).ok()?,
            Expr::CompoundIdentifier(parts) => {
                let column = parts.last()?;
                let qualifier = (parts.len() >= 2).then(|| parts[parts.len() - 2].as_str());
                resolve_plan_field(row, qualifier, column).ok()?
            }
            _ => return None,
        };
        Some(
            field.meta.source.flags & crate::mysql::protocol::UNSIGNED_FLAG != 0
                || matches!(
                    field.meta.column_type,
                    ColumnType::Year | ColumnType::Bit { .. }
                ),
        )
    })
}

/// Preserve the declared TEXT/BLOB distinction after a base column has been
/// lowered into a PlanField. These types intentionally use MySQL's silent
/// legacy implicit numeric conversion, unlike CHAR/VARCHAR/BINARY/VARBINARY.
fn plan_expr_uses_silent_implicit_string_conversion(expr: &Expr, row: &PlanRow) -> bool {
    let field = match expr {
        Expr::Identifier(name) => resolve_plan_field(row, None, name).ok(),
        Expr::CompoundIdentifier(parts) => {
            let column = parts.last();
            let qualifier = (parts.len() >= 2).then(|| parts[parts.len() - 2].as_str());
            column.and_then(|column| resolve_plan_field(row, qualifier, column).ok())
        }
        Expr::GroupKeyRef { expr, .. } => {
            return plan_expr_uses_silent_implicit_string_conversion(expr, row);
        }
        _ => None,
    };
    field.is_some_and(|field| match field.meta.column_type {
        // The catalog source carries BLOB_FLAG for both TEXT and BLOB. It is
        // preserved through PlanField/BatchField even when an operator drops
        // the optional display length, unlike the old length heuristic which
        // misclassified unbounded VARCHAR as TEXT.
        ColumnType::Blob | ColumnType::VarString => {
            field.meta.source.flags & crate::mysql::protocol::BLOB_FLAG != 0
        }
        _ => false,
    })
}

pub(super) fn eval_plan_binary_legacy(
    left: &Expr,
    op: BinaryOp,
    right: &Expr,
    row: &PlanRow,
) -> Result<Value, QueryError> {
    if op == BinaryOp::Concat {
        let l = eval_plan_expr_legacy(left, row)?;
        let r = eval_plan_expr_legacy(right, row)?;
        return eval_binary_pipe_concat(l, r);
    }
    if op == BinaryOp::And || op == BinaryOp::Or {
        let l = crate::engine::expr::logical_value(&eval_plan_expr_legacy(left, row)?);
        if op == BinaryOp::And && l == Some(false) {
            return Ok(Value::Int(0));
        }
        if op == BinaryOp::Or && l == Some(true) {
            return Ok(Value::Int(1));
        }
        let r = crate::engine::expr::logical_value(&eval_plan_expr_legacy(right, row)?);
        return Ok(crate::engine::expr::combine_logical(op, l, r));
    }
    if let Some(v) =
        try_eval_row_comparison(left, op, right, row, |e| eval_plan_expr_legacy(e, row))?
    {
        return Ok(v);
    }
    let comparison = crate::parser::boolean::is_comparison!(op);
    let left_value = if comparison {
        crate::engine::expr::exact_decimal_comparison_literal(left)
            .map(Ok)
            .unwrap_or_else(|| eval_plan_expr_legacy(left, row))?
    } else {
        eval_plan_expr_legacy(left, row)?
    };
    let right_value = if comparison {
        crate::engine::expr::exact_decimal_comparison_literal(right)
            .map(Ok)
            .unwrap_or_else(|| eval_plan_expr_legacy(right, row))?
    } else {
        eval_plan_expr_legacy(right, row)?
    };
    if comparison {
        let collation_id = plan_binary_collation_id(left, right, row);
        let evaluate = || {
            crate::engine::expr::with_numeric_comparison_context(
                left,
                right,
                &left_value,
                &right_value,
                || {
                    crate::engine::expr::with_compare_collation(collation_id, || {
                        crate::engine::expr::null_cmp_binary(&left_value, op, &right_value)
                    })
                },
            )
        };
        return if plan_expr_uses_silent_implicit_string_conversion(left, row)
            || plan_expr_uses_silent_implicit_string_conversion(right, row)
        {
            crate::engine::expr::with_suppressed_expression_warnings(evaluate)
        } else {
            evaluate()
        };
    }
    if matches!(
        op,
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod
    ) {
        let evaluate = || {
            crate::engine::expr::eval_arithmetic_expression(
                left,
                right,
                &left_value,
                op,
                &right_value,
            )
        };
        let value = if plan_expr_uses_silent_implicit_string_conversion(left, row)
            || plan_expr_uses_silent_implicit_string_conversion(right, row)
        {
            crate::engine::expr::with_suppressed_expression_warnings(evaluate)?
        } else {
            evaluate()?
        };
        return crate::engine::expr::enforce_unsigned_subtraction_result(
            value,
            op == BinaryOp::Sub
                && (plan_expr_result_unsigned(left, row) || plan_expr_result_unsigned(right, row)),
        );
    }
    if op == BinaryOp::IntDiv {
        return crate::engine::expr::eval_int_div_expr_values(
            left,
            right,
            &left_value,
            &right_value,
        );
    }
    eval_binary_values(left_value, op, right_value)
}

fn eval_binary_pipe_concat(l: Value, r: Value) -> Result<Value, QueryError> {
    if matches!(l, Value::Null) || matches!(r, Value::Null) {
        return Ok(Value::Null);
    }
    let ls = crate::engine::expr::string_value(&l);
    let rs = crate::engine::expr::string_value(&r);
    let mut s = String::with_capacity(ls.len() + rs.len());
    s.push_str(&ls);
    s.push_str(&rs);
    Ok(Value::String(s))
}

pub(super) fn eval_binary_values(
    left: Value,
    op: BinaryOp,
    right: Value,
) -> Result<Value, QueryError> {
    match op {
        BinaryOp::Xor => crate::engine::expr::eval_logical_xor(&left, &right),
        BinaryOp::Eq
        | BinaryOp::NullSafeEq
        | BinaryOp::NotEq
        | BinaryOp::Lt
        | BinaryOp::LtEq
        | BinaryOp::Gt
        | BinaryOp::GtEq => crate::engine::expr::null_cmp_binary(&left, op, &right),
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
            eval_arithmetic(&left, op, &right)
        }
        BinaryOp::IntDiv => crate::engine::expr::eval_int_div_values(&left, &right),
        BinaryOp::BitAnd
        | BinaryOp::BitOr
        | BinaryOp::BitXor
        | BinaryOp::ShiftLeft
        | BinaryOp::ShiftRight => crate::engine::expr::eval_bitwise(left, op, right),
        BinaryOp::Arrow | BinaryOp::LongArrow => {
            crate::engine::expr::eval_json_arrow(left, right, op == BinaryOp::LongArrow)
        }
        BinaryOp::And | BinaryOp::Or => Ok(crate::engine::expr::combine_logical(
            op,
            crate::engine::expr::logical_value(&left),
            crate::engine::expr::logical_value(&right),
        )),
        BinaryOp::Concat => eval_binary_pipe_concat(left, right),
    }
}

pub(super) fn eval_plan_bool(expr: &Expr, row: &PlanRow) -> Result<bool, QueryError> {
    Ok(truthy(&eval_plan_expr(expr, row)?))
}

pub(super) fn eval_plan_bool_with_db(
    db: &Arc<Database>,
    txn: TxnView,
    expr: &Expr,
    row: &PlanRow,
    eval_ctx: &mut EvalQueryContext,
) -> Result<bool, QueryError> {
    Ok(truthy(&eval_plan_expr_with_db(
        db, txn, expr, row, eval_ctx,
    )?))
}

/// Schwartzian transform helpers for ORDER BY.
///
/// These helpers precompute the sort key once per row (O(N) evaluations)
/// and feed the slice sorter a cheap `Value` comparison. Evaluation happens
/// before sorting, so expression errors can be returned instead of being
/// hidden inside an infallible comparator.
pub(super) fn precompute_order_keys(
    row: &PlanRow,
    order_by: &[OrderBy],
) -> Result<Vec<Value>, QueryError> {
    let keys = order_by
        .iter()
        .map(|item| eval_row_or_aggregate_expr(&item.expr, row))
        .collect::<Result<Vec<_>, _>>()?;
    ORDER_BY_KEY_ROWS_PREPARED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Ok(keys)
}

#[inline]
fn uses_deterministic_group_ties(row: &PlanRow) -> bool {
    row.agg_values.is_some() && !row.fields.is_empty()
}

/// ORDER BY keys used by physical row sorts. Aggregate rows refine SQL peers
/// with their finalized group-key fields so in-memory, TopK and external-spill
/// execution all produce the same deterministic order.
pub(super) fn precompute_sort_keys_for_plan_row(
    row: &PlanRow,
    order_by: &[OrderBy],
) -> Result<Vec<Value>, QueryError> {
    let mut keys = precompute_order_keys(row, order_by)?;
    if uses_deterministic_group_ties(row) {
        keys.extend(row.fields.iter().map(|field| field.value.clone()));
    }
    Ok(keys)
}

pub(super) fn sort_fields_for_plan_rows(
    order_by: &[OrderBy],
    sample: Option<&PlanRow>,
) -> Vec<crate::engine::executor::sort::SortField> {
    let mut fields = crate::engine::executor::sort::fields_from_order_by(order_by);
    if let Some(row) = sample {
        for (field, item) in fields.iter_mut().zip(order_by) {
            field.collation_id = plan_expr_collation(&item.expr, row).map(|info| info.id);
        }
    }
    if let Some(sample) = sample.filter(|row| uses_deterministic_group_ties(row)) {
        fields.extend(deterministic_group_tie_fields(sample));
    }
    fields
}

pub(crate) static ORDER_BY_KEY_ROWS_PREPARED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[inline]
pub(crate) fn record_order_by_key_rows_prepared(rows: usize) {
    ORDER_BY_KEY_ROWS_PREPARED.fetch_add(rows as u64, std::sync::atomic::Ordering::Relaxed);
}

/// Schwartzian sort: precomputes ORDER BY keys per row, then sorts.
/// Replaces `rows.sort_by(|a, b| compare_plan_order_by(order_by, a, b))`
/// when `rows` is owned and we can safely shuffle it.
///
/// Two layers of progressively more specific optimisation:
///   1. Compile a `PlanOrderProgram`. For aggregate rows it
///      precomputes the `aggregate_key` fingerprint hash for any
///      ORDER BY expression that is itself a single aggregate function
///      call, so per-row evaluation is a direct hashmap lookup instead
///      of the legacy interpreter's `format!("{expr:?}")`-per-row.
///   2. The shared sorter detects all-integer prepared keys and uses its
///      encoded integer comparator; other shapes use the generic comparator.
pub(super) fn sort_plan_rows_by_order(
    rows: &mut Vec<PlanRow>,
    order_by: &[OrderBy],
) -> Result<(), QueryError> {
    if order_by.is_empty() || rows.is_empty() {
        return Ok(());
    }
    sort_plan_rows_by_order_generic(rows, order_by)
}

fn sort_plan_rows_by_order_generic(
    rows: &mut Vec<PlanRow>,
    order_by: &[OrderBy],
) -> Result<(), QueryError> {
    let mut fields = crate::engine::executor::sort::fields_from_order_by(order_by);
    if let Some(row) = rows.first() {
        for (field, item) in fields.iter_mut().zip(order_by) {
            field.collation_id = plan_expr_collation(&item.expr, row).map(|info| info.id);
        }
    }
    // Hash aggregation deliberately does not promise bucket iteration order.
    // A stable SQL sort alone therefore still exposes hash-table insertion
    // order for ORDER BY peers, which can vary between scalar/vector/spill
    // implementations. Refine peers by the finalized group-key fields. This
    // does not change SQL ordering or window peer semantics; it only chooses a
    // deterministic order inside an otherwise unordered peer set.
    let deterministic_group_ties = rows.first().is_some_and(uses_deterministic_group_ties);
    if deterministic_group_ties {
        fields.extend(deterministic_group_tie_fields(
            rows.first().expect("non-empty rows"),
        ));
    }
    let program = rows
        .first()
        .map(|row| PlanOrderProgram::compile_once(row, order_by));
    if order_by.len() == 1 && !deterministic_group_ties {
        let keys = prepare_single_order_keys(rows, program.as_ref().expect("non-empty rows"))?;
        ORDER_BY_KEY_ROWS_PREPARED
            .fetch_add(rows.len() as u64, std::sync::atomic::Ordering::Relaxed);
        match keys {
            PreparedSingleOrderKeys::Int(keys) => {
                crate::engine::executor::sort::sort_by_precomputed_i64_key(rows, fields[0], keys);
            }
            PreparedSingleOrderKeys::Generic(keys) => {
                crate::engine::executor::sort::sort_by_precomputed_single_key(
                    rows, fields[0], keys,
                );
            }
        }
        return Ok(());
    }
    let keys = rows
        .iter()
        .map(|row| {
            let mut keys = match program.as_ref() {
                Some(program) => program.eval_keys(row)?,
                None => precompute_order_keys(row, order_by)?,
            };
            if deterministic_group_ties {
                keys.extend(row.fields.iter().map(|field| field.value.clone()));
            }
            Ok(keys)
        })
        .collect::<Result<Vec<_>, _>>()?;
    ORDER_BY_KEY_ROWS_PREPARED.fetch_add(rows.len() as u64, std::sync::atomic::Ordering::Relaxed);
    crate::engine::executor::sort::sort_by_precomputed_keys(rows, &fields, keys);
    Ok(())
}

fn deterministic_group_tie_fields(
    sample: &PlanRow,
) -> impl Iterator<Item = crate::engine::executor::sort::SortField> + '_ {
    sample
        .fields
        .iter()
        .map(|field| crate::engine::executor::sort::SortField {
            ascending: true,
            nulls_first: true,
            collation_id: field.source.collation_id,
        })
}

enum PreparedSingleOrderKeys {
    Int(Vec<(i64, u32)>),
    Generic(Vec<Value>),
}

fn prepare_single_order_keys(
    rows: &[PlanRow],
    program: &PlanOrderProgram,
) -> Result<PreparedSingleOrderKeys, QueryError> {
    let mut integers = Vec::with_capacity(rows.len());
    let mut generic: Option<Vec<Value>> = None;
    for (index, row) in rows.iter().enumerate() {
        let value = program.eval_single_key(row)?;
        match generic.as_mut() {
            Some(keys) => keys.push(value),
            None => match value {
                Value::Int(value) => integers.push((value, index as u32)),
                value => {
                    let mut keys = Vec::with_capacity(rows.len());
                    keys.extend(integers.drain(..).map(|(value, _)| Value::Int(value)));
                    keys.push(value);
                    generic = Some(keys);
                }
            },
        }
    }
    Ok(match generic {
        Some(keys) => PreparedSingleOrderKeys::Generic(keys),
        None => PreparedSingleOrderKeys::Int(integers),
    })
}

/// Schwartzian variant of `select_nth_unstable_by`: partitions `rows`
/// so the first `nth` are the smallest, with precomputed keys.
pub(super) fn select_nth_plan_rows_by_order(
    rows: &mut Vec<PlanRow>,
    order_by: &[OrderBy],
    nth: usize,
) -> Result<(), QueryError> {
    if order_by.is_empty() || rows.len() < 2 || nth >= rows.len() {
        return Ok(());
    }
    select_nth_plan_rows_by_order_generic(rows, order_by, nth)
}

fn select_nth_plan_rows_by_order_generic(
    rows: &mut Vec<PlanRow>,
    order_by: &[OrderBy],
    nth: usize,
) -> Result<(), QueryError> {
    let mut fields = crate::engine::executor::sort::fields_from_order_by(order_by);
    let deterministic_group_ties = rows.first().is_some_and(uses_deterministic_group_ties);
    if deterministic_group_ties {
        fields.extend(deterministic_group_tie_fields(
            rows.first().expect("non-empty rows"),
        ));
    }
    let program = rows
        .first()
        .map(|row| PlanOrderProgram::compile_once(row, order_by));
    if order_by.len() == 1 && !deterministic_group_ties {
        let keys = prepare_single_order_keys(rows, program.as_ref().expect("non-empty rows"))?;
        ORDER_BY_KEY_ROWS_PREPARED
            .fetch_add(rows.len() as u64, std::sync::atomic::Ordering::Relaxed);
        match keys {
            PreparedSingleOrderKeys::Int(keys) => {
                crate::engine::executor::sort::select_nth_by_precomputed_i64_key(
                    rows, fields[0], nth, keys,
                );
            }
            PreparedSingleOrderKeys::Generic(keys) => {
                crate::engine::executor::sort::select_nth_by_precomputed_single_key(
                    rows, fields[0], nth, keys,
                );
            }
        }
        return Ok(());
    }
    let keys = rows
        .iter()
        .map(|row| {
            let mut keys = match program.as_ref() {
                Some(program) => program.eval_keys(row)?,
                None => precompute_order_keys(row, order_by)?,
            };
            if deterministic_group_ties {
                keys.extend(row.fields.iter().map(|field| field.value.clone()));
            }
            Ok(keys)
        })
        .collect::<Result<Vec<_>, _>>()?;
    ORDER_BY_KEY_ROWS_PREPARED.fetch_add(rows.len() as u64, std::sync::atomic::Ordering::Relaxed);
    crate::engine::executor::sort::select_nth_by_precomputed_keys(rows, &fields, nth, keys);
    Ok(())
}

pub(super) fn eval_row_or_aggregate_expr(expr: &Expr, row: &PlanRow) -> Result<Value, QueryError> {
    if let Some(group) = &row.group {
        eval_aggregate_expr(expr, group, row)
    } else if row.agg_values.is_some() {
        eval_aggregate_expr(expr, &[], row)
    } else {
        eval_plan_expr(expr, row)
    }
}

/// Read the BM25 relevance the `FulltextScan` injected for `match_expr` on
/// this row. The hidden pseudo-column is keyed by the MATCH expression's
/// fingerprint, so identical `MATCH(...) AGAINST(...)` occurrences across
/// WHERE / SELECT / ORDER BY all resolve to the same score. Errors clearly
/// when no FulltextScan produced a score (MATCH outside a WHERE full-text
/// search), rather than evaluating `__match_against` as a real function.
pub(super) fn resolve_fulltext_score(
    match_expr: &Expr,
    row: &PlanRow,
) -> Result<Value, QueryError> {
    let column = format!(
        "{}{:016x}",
        super::plan::FT_SCORE_PREFIX,
        crate::engine::expr_program::expr_fingerprint(match_expr)
    );
    match resolve_plan_field(row, None, &column) {
        Ok(field) => Ok(field.value.clone()),
        Err(_) => Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "MATCH() AGAINST() requires a matching FULLTEXT score provider in this query block",
        )),
    }
}

/// Resolve the effective collation info for a LIKE operand that is a
/// plain column reference (`Expr::Identifier` / `CompoundIdentifier`).
///
/// Returns `None` for computed expressions, unresolved names, or
/// columns that carry no catalog collation. Legacy eval paths use this
/// so `col LIKE 'foo'` honours `ColumnSchema.collation` without any
/// planner-side context — the interned `ColMeta.source.collation_id`
/// already carries the resolved id from wire-metadata construction.
pub(super) fn plan_expr_collation(
    expr: &Expr,
    row: &PlanRow,
) -> Option<&'static crate::charset::CollationInfo> {
    if let Expr::Function { name, args, .. } = expr {
        if name.eq_ignore_ascii_case("COLLATE") || name.eq_ignore_ascii_case("__COLLATED_CONST") {
            if let Some(Expr::Literal(Literal::String(collation))) = args.get(1) {
                return crate::charset::resolve_collation(collation).ok();
            }
        }
    }
    if let Expr::GroupKeyRef { expr, .. } = expr {
        return plan_expr_collation(expr, row);
    }
    if let Expr::Function { name, args, .. } = expr {
        if name == crate::parser::HIDDEN_FIRST_VALUE_FN {
            return args
                .first()
                .and_then(|inner| plan_expr_collation(inner, row));
        }
        if name.eq_ignore_ascii_case("GROUP_CONCAT") {
            let value = args.first()?;
            let value = match value {
                Expr::Function {
                    name,
                    args: ordered_args,
                    ..
                } if name.eq_ignore_ascii_case(crate::parser::GROUP_CONCAT_ORDERED_FN) => {
                    ordered_args.first().unwrap_or(value)
                }
                _ => value,
            };
            return plan_expr_collation(value, row).or_else(|| {
                crate::engine::expr::expression_metadata_collation_id(value)
                    .and_then(crate::charset::resolve_collation_id)
            });
        }
        // Control-flow functions return one of their value operands; the
        // condition of IF never contributes (matches `expr_collation`).
        let value_operands = match name.to_ascii_uppercase().as_str() {
            "IF" => args.get(1..),
            "IFNULL" | "COALESCE" => Some(&args[..]),
            "NULLIF" => args.get(..1),
            _ => None,
        };
        if let Some(operands) = value_operands {
            return operands
                .iter()
                .find_map(|arg| plan_expr_collation(arg, row));
        }
        if name.eq_ignore_ascii_case("CONCAT") || name.eq_ignore_ascii_case("CONCAT_WS") {
            return args
                .iter()
                .find_map(|arg| plan_expr_collation(arg, row))
                .or_else(|| {
                    args.iter().find_map(|arg| {
                        crate::engine::expr::expression_metadata_collation_id(arg)
                            .and_then(crate::charset::resolve_collation_id)
                    })
                });
        }
        // Other built-ins share the bind-time result-domain algebra. In
        // particular, string transforms retain their argument's collation,
        // while casts and formatters may use the connection collation.
        let resolve_column = |column_expr: &Expr| {
            let field = match column_expr {
                Expr::Identifier(name) => resolve_plan_field(row, None, name).ok(),
                Expr::CompoundIdentifier(parts) => {
                    let column = parts.last()?;
                    let qualifier = (parts.len() >= 2).then(|| parts[parts.len() - 2].as_str());
                    resolve_plan_field(row, qualifier, column).ok()
                }
                _ => None,
            }?;
            Some(crate::engine::expr_type::ExprType::from_column_type(
                field.column_type,
                &field.source,
            ))
        };
        let no_placeholder = |_: usize| None;
        let context =
            crate::engine::expr_type::TypeContext::context_free(&resolve_column, &no_placeholder);
        return crate::engine::expr_type::infer_expr_type(expr, &context)
            .collation_id
            .and_then(crate::charset::resolve_collation_id);
    }
    if let Expr::Case {
        results,
        else_result,
        ..
    } = expr
    {
        return results
            .iter()
            .chain(else_result.as_deref())
            .find_map(|result| plan_expr_collation(result, row));
    }
    let field = match expr {
        Expr::Identifier(name) => resolve_plan_field(row, None, name).ok()?,
        Expr::CompoundIdentifier(parts) => {
            let column = parts.last()?;
            let qualifier = (parts.len() >= 2).then(|| parts[parts.len() - 2].as_str());
            resolve_plan_field(row, qualifier, column).ok()?
        }
        _ => return None,
    };
    let cid = field.meta.source.collation_id?;
    // A collation id of 63 ("binary") on a text column is fine — the
    // resolver handles it; a stray unknown id is silently ignored.
    crate::charset::resolve_collation_id(cid)
}

fn plan_binary_collation_id(left: &Expr, right: &Expr, row: &PlanRow) -> Option<u16> {
    if [left, right]
        .iter()
        .any(|expr| crate::engine::expr::expr_forces_binary_comparison(expr))
    {
        return Some(63);
    }
    plan_operand_collation(&[left, right], row, "comparison")
}

fn plan_in_list_collation(expr: &Expr, list: &[Expr], row: &PlanRow) -> Option<u16> {
    let operands = std::iter::once(expr).chain(list.iter()).collect::<Vec<_>>();
    plan_operand_collation(&operands, row, "in")
}

fn plan_between_collation(expr: &Expr, low: &Expr, high: &Expr, row: &PlanRow) -> Option<u16> {
    plan_operand_collation(&[expr, low, high], row, "between")
}

fn plan_operand_collation(operands: &[&Expr], row: &PlanRow, operation: &str) -> Option<u16> {
    fn is_explicit(expr: &Expr) -> bool {
        matches!(
            expr,
            Expr::Function { name, args, .. }
                if name.eq_ignore_ascii_case("COLLATE")
                    || (name.eq_ignore_ascii_case("__COLLATED_CONST")
                        && matches!(
                            args.get(2),
                            Some(Expr::Literal(crate::parser::Literal::Int(0)
                                | crate::parser::Literal::UInt(0)))
                        ))
        )
    }

    // Explicit COLLATE in the AST is stronger than a row field's implicit
    // collation. For ordinary `column op literal`, keep preferring the field
    // metadata; only fall back to the static expression aggregation when no
    // field participates.
    if operands.iter().copied().any(is_explicit) {
        return crate::engine::expr::aggregate_static_expr_collations(operands, operation)
            .ok()
            .flatten();
    }
    operands
        .iter()
        .copied()
        .find_map(|expr| plan_expr_collation(expr, row))
        .map(|info| info.id)
        .or_else(|| {
            crate::engine::expr::aggregate_static_expr_collations(operands, operation)
                .ok()
                .flatten()
        })
}

fn typed_json_cast_source(
    args: &[Expr],
    row: &PlanRow,
) -> Option<crate::engine::expr::ExprTypeDescriptor> {
    if !matches!(
        args.get(1),
        Some(Expr::Literal(crate::parser::Literal::String(target)))
            if target.eq_ignore_ascii_case("JSON")
    ) {
        return None;
    }
    let source = args.first()?;
    let field = match source {
        Expr::Identifier(name) => resolve_plan_field(row, None, name).ok()?,
        Expr::CompoundIdentifier(parts) => {
            let column = parts.last()?;
            let qualifier = (parts.len() >= 2).then(|| parts[parts.len() - 2].as_str());
            resolve_plan_field(row, qualifier, column).ok()?
        }
        _ => return None,
    };
    use crate::engine::expr::{ExprCollation, ExprTypeClass, ExprTypeDescriptor};
    use crate::mysql::protocol::{ENUM_FLAG, NOT_NULL_FLAG, SET_FLAG};
    let class = if field.meta.source.flags & ENUM_FLAG != 0 {
        ExprTypeClass::Enum
    } else if field.meta.source.flags & SET_FLAG != 0 {
        ExprTypeClass::Set
    } else {
        match field.meta.column_type {
            ColumnType::Bit { bits } => ExprTypeClass::Bit { bits },
            ColumnType::Blob => ExprTypeClass::BinaryString,
            _ => return None,
        }
    };
    let collation = field
        .meta
        .source
        .collation_id
        .and_then(crate::charset::resolve_collation_id)
        .map(|info| ExprCollation {
            charset: info.charset,
            collation_id: info.id,
            coercibility: 2,
        });
    Some(ExprTypeDescriptor {
        class,
        collation,
        nullable: field.meta.source.flags & NOT_NULL_FLAG == 0,
    })
}

pub(super) fn resolve_plan_field<'a>(
    row: &'a PlanRow,
    qualifier: Option<&str>,
    column: &str,
) -> Result<&'a PlanField, QueryError> {
    let index = row.schema.resolve_index(&row.fields, qualifier, column)?;
    Ok(&row.fields[index])
}

/// Returns the rewritten subquery together with the ordered list of
/// outer-column `ValueKey`s that were substituted into it. The vector is
/// the discriminator that turns "memoise only when uncorrelated" into
/// "memoise per outer-key combination": two calls with the same vector
/// are guaranteed to produce the same subquery result and can share the
/// cached value. An empty vector means the subquery has no outer
/// references — equivalent to the legacy uncorrelated cache.
pub(super) fn bind_correlated_subquery(
    db: &Arc<Database>,
    subquery: &Select,
    outer_row: &PlanRow,
) -> (Select, Vec<ValueKey>) {
    let mut sub = subquery.clone();
    let bound = bind_correlated_subquery_in_place(db, &mut sub, outer_row);
    (sub, bound)
}

fn bind_correlated_subquery_in_place(
    db: &Arc<Database>,
    sub: &mut Select,
    outer_row: &PlanRow,
) -> Vec<ValueKey> {
    let inner_cols = inner_select_columns(db, sub);
    let inner_tables = inner_select_tables(sub);
    let (_, bound) = with_outer_bind_collection(|| {
        let _ = substitute_outer_in_select(db, sub, outer_row, &inner_cols, &inner_tables);
    });
    // MySQL assigns an aggregate in a nested query block to the nearest
    // outer block referenced by its arguments. For a source-free outer
    // SELECT there is one implicit input row, so a constant GROUP_CONCAT
    // owned by that block reduces to its argument. Lower it only after
    // correlation binding, and only when the outer row is genuinely the
    // source-free implicit row; ordinary `WHERE GROUP_CONCAT(col)` remains
    // an invalid local aggregate.
    if outer_row.fields.is_empty() {
        if let Some(selection) = sub.selection.as_mut() {
            lower_source_free_outer_group_concat(selection);
        }
    }
    bound
}

/// Bind every SELECT leaf of a derived subquery tree against one outer row.
/// This is the parametric lowering used by LATERAL/Apply: UNION/INTERSECT/
/// EXCEPT and WITH bodies keep their ordinary statement executor, while
/// correlation substitution is applied with each leaf's own inner scope.
pub(super) fn bind_correlated_subquery_tree(
    db: &Arc<Database>,
    subquery: &crate::parser::SubQuery,
    outer_row: &PlanRow,
) -> crate::parser::SubQuery {
    struct BindLeaves<'a> {
        db: &'a Arc<Database>,
        outer_row: &'a PlanRow,
    }
    impl crate::parser::visit::AstVisitorMut for BindLeaves<'_> {
        fn walk_query_children(&self, query: &crate::parser::QueryExpr) -> bool {
            !matches!(query, crate::parser::QueryExpr::Rrf(_))
        }
        fn enter_select(&mut self, select: &mut Select) {
            let _ = bind_correlated_subquery_in_place(self.db, select, self.outer_row);
        }
        fn walk_select_children(&self, _: &Select) -> bool {
            // The scope-aware binder already handled this leaf and its children.
            false
        }
        fn walk_order_expr(&self, _: &OrderBy) -> bool {
            // Set-operation ORDER BY belongs to the result scope. Binding it
            // against the enclosing row would change the existing semantics.
            false
        }
    }
    let mut query = subquery.clone();
    crate::parser::visit::visit_query_mut(&mut BindLeaves { db, outer_row }, &mut query);
    query
}

fn lower_source_free_outer_group_concat(expr: &mut Expr) {
    if let Expr::Function { name, args, .. } = expr {
        if name.eq_ignore_ascii_case("GROUP_CONCAT")
            && args.len() == 1
            && !expr_contains_column_reference(&args[0])
        {
            *expr = args[0].clone();
            return;
        }
    }
    let visit = |child: &mut Expr| lower_source_free_outer_group_concat(child);
    match expr {
        Expr::Unary { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::GroupKeyRef { expr, .. } => visit(expr),
        Expr::Binary { left, right, .. } => {
            visit(left);
            visit(right);
        }
        Expr::Function { args, .. } | Expr::Tuple(args) => {
            for arg in args {
                visit(arg);
            }
        }
        Expr::InList { expr, list, .. } => {
            visit(expr);
            for item in list {
                visit(item);
            }
        }
        Expr::InSubquery { expr, .. } => visit(expr),
        Expr::QuantifiedComparison { left, right, .. } => {
            visit(left);
            if let QuantifiedComparisonRight::List(items) = right {
                for item in items {
                    visit(item);
                }
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            visit(expr);
            visit(low);
            visit(high);
        }
        Expr::Like { expr, pattern, .. } => {
            visit(expr);
            visit(pattern);
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(operand) = operand {
                visit(operand);
            }
            for condition in conditions {
                visit(condition);
            }
            for result in results {
                visit(result);
            }
            if let Some(else_result) = else_result {
                visit(else_result);
            }
        }
        Expr::WindowFunction { args, spec, .. } => {
            let crate::parser::WindowSpec {
                partition_by,
                order_by,
                ..
            } = spec.as_mut();

            for arg in args {
                visit(arg);
            }
            for part in partition_by {
                visit(part);
            }
            for order in order_by {
                visit(&mut order.expr);
            }
        }
        Expr::Subquery(_)
        | Expr::Exists { .. }
        | Expr::Wildcard
        | Expr::Identifier(_)
        | Expr::CompoundIdentifier(_)
        | Expr::UserVariable(_)
        | Expr::SystemVariable { .. }
        | Expr::Literal(_)
        | Expr::BoundValue(_)
        | Expr::Placeholder(_)
        | Expr::Default(_)
        | Expr::DefaultQualified(_)
        | Expr::Grouping(_) => {}
    }
}

pub(super) fn expr_contains_column_reference(expr: &Expr) -> bool {
    crate::parser::visit::try_inspect_value_expr(expr, |node| {
        if matches!(
            node,
            Expr::Identifier(_)
                | Expr::CompoundIdentifier(_)
                | Expr::Default(Some(_))
                | Expr::DefaultQualified(_)
                | Expr::Wildcard
        ) {
            Err(())
        } else {
            Ok(true)
        }
    })
    .is_err()
}

/// Prepare a subquery invocation. On the first call for a given AST
/// pointer we attempt to build a parametric plan (outer refs lifted to
/// `Expr::Placeholder`); subsequent calls reuse the cached plan and only
/// re-collect parameter values from `outer_row`. When the shape isn't
/// supported (derived/CTE/system tables, body with nested subqueries,
/// planner refusal) we permanently fall back to the legacy
/// substitute+plan path for that AST address and remember it via the
/// `Some(None)` entry — avoids paying the failed-build cost on every row.
/// Rewrite an `IN (subquery)` body into a set-membership-equivalent form
/// that cannot blow up the materialised intermediate. `IN` ignores
/// duplicates, so a subquery whose FROM contains a join — where each kept
/// value can be multiplied into many duplicate output rows (TPC-DS Q95's
/// `select wr_order_number from web_returns, ws_wh where ...` fans
/// 71k web_returns rows out to 8.7M) — is rewritten to `... GROUP BY
/// <projection>`. That routes through the streaming HashJoin→HashAggregate
/// path (the same one a `COUNT(*)`/`GROUP BY` over the join already uses),
/// collapsing duplicates as they are produced instead of materialising the
/// whole product and raising 1037. Returns `None` for shapes that either
/// can't explode (single base table) or where adding a GROUP BY would
/// change semantics / can't be expressed.
fn build_dedup_in_subquery(sub: &Select) -> Option<Select> {
    // Only a join multiplies rows; a single-table IN subquery is already
    // bounded by its base table, so skip it to avoid a needless hash-agg.
    if sub.joins.is_empty() && sub.from.len() <= 1 {
        return None;
    }
    // Shapes where a pushed GROUP BY would be redundant or change meaning.
    if sub.distinct
        || !sub.group_by.is_empty()
        || !sub.grouping_sets.is_empty()
        || sub.group_by_modifier != crate::parser::GroupByModifier::None
        || sub.having.is_some()
        || sub.limit.is_some()
        || sub.offset.is_some()
        || sub.values.is_some()
        || sub.derived.is_some()
        || sub.json_table.is_some()
        || !sub.named_windows.is_empty()
    {
        return None;
    }
    // Build the GROUP BY key list from the projected expressions. Bail on
    // wildcards (can't name them) and on aggregate/window projections
    // (already grouped / need every row).
    let mut group_exprs = Vec::with_capacity(sub.projection.len());
    for item in &sub.projection {
        match item {
            SelectItem::Expr { expr, .. } => {
                if expr_has_aggregate(expr) || super::expr_has_window(expr) {
                    return None;
                }
                group_exprs.push(expr.clone());
            }
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => return None,
        }
    }
    if group_exprs.is_empty() {
        return None;
    }
    let mut deduped = sub.clone();
    deduped.group_by = group_exprs;
    // ORDER BY is meaningless for set membership and, after adding GROUP BY,
    // an ORDER BY on a non-grouped column would be rejected — drop it.
    deduped.order_by.clear();
    Some(deduped)
}

fn prepare_subquery_call(
    db: &Arc<Database>,
    txn: TxnView,
    eval_ctx: &mut EvalQueryContext,
    subquery: &Select,
    outer_row: &PlanRow,
) -> Result<SubqueryCall, QueryError> {
    let captured_ctes = current_subquery_frame()
        .map(|frame| frame.ctes.clone())
        .unwrap_or_else(crate::engine::executor::cte::current_cte_registry);
    let mut run = || -> Result<SubqueryCall, QueryError> {
        let key = subquery as *const Select as usize;
        if let Some(template) = eval_ctx.legacy_subquery_templates.get(&key) {
            return Ok(SubqueryCall::Legacy {
                bound: template.as_ref().clone(),
                outer_binding: Vec::new(),
            });
        }
        if !eval_ctx.subquery_plans.contains_key(&key) {
            let built = try_build_cached_subquery_plan(db, subquery, outer_row);
            eval_ctx.subquery_plans.insert(key, built);
        }
        let entry = eval_ctx.subquery_plans.get(&key).cloned().unwrap_or(None);
        match entry {
            Some(cached) => {
                let (params, outer_binding) = collect_outer_params(&cached.bindings, outer_row)?;
                Ok(SubqueryCall::Cached {
                    cached,
                    params,
                    outer_binding,
                })
            }
            None => {
                let (mut bound, outer_binding) = bind_correlated_subquery(db, subquery, outer_row);
                if outer_binding.is_empty() {
                    // `bound` is a temporary clone, so its descendant AST
                    // addresses must not enter the caller's pointer-keyed
                    // caches. Keep all planning/memo state used while folding
                    // inside a short-lived context; only the stable top-level
                    // source pointer (`key`) is retained below.
                    let mut fold_ctx = EvalQueryContext::default();
                    fold_legacy_nested_scalar_constants(db, txn, &mut bound, &mut fold_ctx)?;
                    let template = Arc::new(bound);
                    eval_ctx
                        .legacy_subquery_templates
                        .insert(key, Arc::clone(&template));
                    return Ok(SubqueryCall::Legacy {
                        bound: template.as_ref().clone(),
                        outer_binding,
                    });
                }
                Ok(SubqueryCall::Legacy {
                    bound,
                    outer_binding,
                })
            }
        }
    };
    if captured_ctes.is_empty() {
        run()
    } else {
        crate::engine::executor::cte::with_cte_registry_extended(captured_ctes, run)
    }
}

/// Fold deterministic scalar descendants inside an uncorrelated legacy
/// template. The enclosing query block itself may be volatile and therefore
/// must not be memoised (`SELECT RAND() * (...)`), but a nested independent
/// aggregate is still a statement constant. Keeping the folded template in
/// `EvalQueryContext::legacy_subquery_templates` makes that distinction
/// explicit instead of inheriting the volatile parent's per-row execution.
fn fold_legacy_nested_scalar_constants(
    db: &Arc<Database>,
    txn: TxnView,
    select: &mut Select,
    eval_ctx: &mut EvalQueryContext,
) -> Result<(), QueryError> {
    for item in &mut select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            fold_legacy_nested_scalar_constants_expr(db, txn, expr, eval_ctx)?;
        }
    }
    if let Some(json_table) = &mut select.json_table {
        fold_legacy_nested_scalar_constants_expr(db, txn, &mut json_table.expr, eval_ctx)?;
    }
    if let Some(selection) = &mut select.selection {
        fold_legacy_nested_scalar_constants_expr(db, txn, selection, eval_ctx)?;
    }
    if let Some(having) = &mut select.having {
        fold_legacy_nested_scalar_constants_expr(db, txn, having, eval_ctx)?;
    }
    for join in &mut select.joins {
        if let Some(condition) = &mut join.condition {
            fold_legacy_nested_scalar_constants_expr(db, txn, condition, eval_ctx)?;
        }
    }
    for expr in &mut select.group_by {
        fold_legacy_nested_scalar_constants_expr(db, txn, expr, eval_ctx)?;
    }
    for order in &mut select.order_by {
        fold_legacy_nested_scalar_constants_expr(db, txn, &mut order.expr, eval_ctx)?;
    }
    for window in &mut select.named_windows {
        for expr in &mut window.partition_by {
            fold_legacy_nested_scalar_constants_expr(db, txn, expr, eval_ctx)?;
        }
        for order in &mut window.order_by {
            fold_legacy_nested_scalar_constants_expr(db, txn, &mut order.expr, eval_ctx)?;
        }
    }
    Ok(())
}

fn fold_legacy_nested_scalar_constants_expr(
    db: &Arc<Database>,
    txn: TxnView,
    expr: &mut Expr,
    eval_ctx: &mut EvalQueryContext,
) -> Result<(), QueryError> {
    let visit = |child: &mut Expr, eval_ctx: &mut EvalQueryContext| {
        fold_legacy_nested_scalar_constants_expr(db, txn, child, eval_ctx)
    };
    match expr {
        Expr::Subquery(subquery) => {
            // Fold deeper independent blocks even when this intermediate block
            // is volatile or correlated.
            fold_legacy_nested_scalar_constants(db, txn, subquery, eval_ctx)?;
            if super::explain::select_is_uncacheable(subquery) {
                return Ok(());
            }
            let empty = PlanRow::default();
            let call = match prepare_subquery_call(db, txn, eval_ctx, subquery, &empty) {
                Ok(call) => call,
                Err(err)
                    if err.code() == crate::error::catalog::ER_BAD_FIELD_ERROR.code().get() =>
                {
                    return Ok(())
                }
                Err(err) => return Err(err),
            };
            if !call.outer_binding().is_empty() {
                return Ok(());
            }
            let response = match execute_subquery_call(db, txn, call) {
                Ok(response) => response,
                // A reference to the immediately enclosing query block is
                // intentionally still unresolved when probed against an empty
                // row. It will be bound when that block actually executes.
                Err(err)
                    if err.code() == crate::error::catalog::ER_BAD_FIELD_ERROR.code().get() =>
                {
                    return Ok(())
                }
                Err(err) => return Err(err),
            };
            let QueryResponse::ResultSet(result) = response else {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "scalar subquery must return rows",
                ));
            };
            if result.columns.len() != 1 {
                return Err(crate::engine::expr::operand_columns_error(1));
            }
            if result.rows.len() > 1 {
                return Err(error(
                    crate::error::catalog::ER_SUBQUERY_NO_1_ROW,
                    "Subquery returns more than 1 row",
                ));
            }
            *expr = Expr::BoundValue(
                result
                    .rows
                    .first()
                    .and_then(|row| row.first())
                    .cloned()
                    .unwrap_or(Value::Null),
            );
        }
        Expr::Unary { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::GroupKeyRef { expr, .. } => visit(expr, eval_ctx)?,
        Expr::Binary { left, right, .. }
            if matches!(
                (&**left, &**right),
                (Expr::Tuple(_), Expr::Subquery(_)) | (Expr::Subquery(_), Expr::Tuple(_))
            ) =>
        {
            // Row subqueries are cardinality-checked by the row evaluator;
            // scalar constant folding would reject their multiple columns.
        }
        Expr::Binary { left, right, .. } => {
            visit(left, eval_ctx)?;
            visit(right, eval_ctx)?;
        }
        Expr::Function { args, .. } | Expr::Tuple(args) => {
            for arg in args {
                visit(arg, eval_ctx)?;
            }
        }
        Expr::InList { expr, list, .. } => {
            visit(expr, eval_ctx)?;
            for item in list {
                visit(item, eval_ctx)?;
            }
        }
        Expr::InSubquery { expr, subquery, .. } => {
            visit(expr, eval_ctx)?;
            fold_legacy_nested_scalar_constants(db, txn, subquery, eval_ctx)?;
        }
        Expr::QuantifiedComparison { left, right, .. } => {
            visit(left, eval_ctx)?;
            match right {
                QuantifiedComparisonRight::Subquery(subquery) => {
                    fold_legacy_nested_scalar_constants(db, txn, subquery, eval_ctx)?;
                }
                QuantifiedComparisonRight::List(items) => {
                    for item in items {
                        visit(item, eval_ctx)?;
                    }
                }
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            visit(expr, eval_ctx)?;
            visit(low, eval_ctx)?;
            visit(high, eval_ctx)?;
        }
        Expr::Like { expr, pattern, .. } => {
            visit(expr, eval_ctx)?;
            visit(pattern, eval_ctx)?;
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(operand) = operand {
                visit(operand, eval_ctx)?;
            }
            for condition in conditions {
                visit(condition, eval_ctx)?;
            }
            for result in results {
                visit(result, eval_ctx)?;
            }
            if let Some(else_result) = else_result {
                visit(else_result, eval_ctx)?;
            }
        }
        Expr::WindowFunction { args, spec, .. } => {
            let crate::parser::WindowSpec {
                partition_by,
                order_by,
                ..
            } = spec.as_mut();

            for arg in args {
                visit(arg, eval_ctx)?;
            }
            for expr in partition_by {
                visit(expr, eval_ctx)?;
            }
            for order in order_by {
                visit(&mut order.expr, eval_ctx)?;
            }
        }
        Expr::Exists { subquery, .. } => {
            fold_legacy_nested_scalar_constants(db, txn, subquery, eval_ctx)?;
        }
        Expr::Wildcard
        | Expr::Identifier(_)
        | Expr::CompoundIdentifier(_)
        | Expr::UserVariable(_)
        | Expr::SystemVariable { .. }
        | Expr::Literal(_)
        | Expr::BoundValue(_)
        | Expr::Placeholder(_)
        | Expr::Default(_)
        | Expr::DefaultQualified(_)
        | Expr::Grouping(_) => {}
    }
    Ok(())
}

/// Resolve deterministic, uncorrelated scalar and bounded `IN` subqueries
/// before a batch predicate opens its outer input. Their results are statement
/// constants, so replacing them with `BoundValue` / `InList` preserves MySQL
/// semantics and lets the remaining predicate compile through
/// `BatchExprProgram`. Correlated, uncacheable, or oversized subqueries stay on
/// the semantic-complete scalar Apply lane.
pub(super) struct ResolvedBatchPredicate {
    pub(super) expr: Expr,
    pub(super) charge: Option<crate::engine::executor::mem_budget::OwnedMemCharge>,
}

/// Return the expression produced by a scalar query block that has exactly
/// one implicit input row and no relational operation of its own.  Replacing
/// such a wrapper with its projection is safe even when the expression is
/// volatile: the caller still evaluates it once per outer candidate row.  We
/// deliberately reject local aggregates (`SELECT COUNT(*)`) because their
/// implicit-single-group semantics are not plain expression semantics.
#[cfg(test)]
fn source_free_scalar_wrapper_expr(select: &Select) -> Option<&Expr> {
    if select.distinct
        || select.table.is_some()
        || !select.from.is_empty()
        || select.no_merge
        || !select.joins.is_empty()
        || select.selection.is_some()
        || !select.group_by.is_empty()
        || !select.grouping_sets.is_empty()
        || select.having.is_some()
        || !select.order_by.is_empty()
        || select.limit.is_some()
        || select.offset.is_some()
        || select.derived.is_some()
        || select.values.is_some()
        || select.json_table.is_some()
        || select.for_update
        || select.lock_clause.is_some()
        || select.sql_calc_found_rows
        || !select.named_windows.is_empty()
        || !select.query_hints.is_empty()
    {
        return None;
    }
    let [SelectItem::Expr { expr, .. }] = select.projection.as_slice() else {
        return None;
    };
    (!crate::planner::has_aggregate_expr(expr)).then_some(expr)
}

pub(super) fn resolve_batch_constant_scalar_subqueries(
    db: &Arc<Database>,
    txn: TxnView,
    expr: &Expr,
    outer_template: &PlanRow,
) -> Result<Option<ResolvedBatchPredicate>, QueryError> {
    fn rewrite(
        db: &Arc<Database>,
        txn: TxnView,
        expr: &mut Expr,
        outer_template: &PlanRow,
        eval_ctx: &mut EvalQueryContext,
        fully_resolved: &mut bool,
        retained_bytes: &mut usize,
    ) -> Result<(), QueryError> {
        match expr {
            Expr::Subquery(subquery) => {
                // mydb statement semantics deliberately materialize every
                // uncorrelated scalar query block once, including volatile
                // wrappers such as SELECT RAND() * (SELECT MAX(...)). The
                // correlation probe below remains the authority: a block
                // with outer bindings stays on the per-row Apply lane.
                let call = prepare_subquery_call(db, txn, eval_ctx, subquery, outer_template)?;
                if !call.outer_binding().is_empty() {
                    *fully_resolved = false;
                    return Ok(());
                }
                let QueryResponse::ResultSet(result) = execute_subquery_call(db, txn, call)? else {
                    return Err(error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        "scalar subquery must return rows",
                    ));
                };
                if result.columns.len() != 1 {
                    return Err(crate::engine::expr::operand_columns_error(1));
                }
                if result.rows.len() > 1 {
                    return Err(error(
                        crate::error::catalog::ER_SUBQUERY_NO_1_ROW,
                        "Subquery returns more than 1 row",
                    ));
                }
                let value = result
                    .rows
                    .first()
                    .and_then(|row| row.first())
                    .cloned()
                    .unwrap_or(Value::Null);
                *expr = Expr::BoundValue(value);
            }
            Expr::Unary { expr, .. }
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::GroupKeyRef { expr, .. } => {
                rewrite(
                    db,
                    txn,
                    expr,
                    outer_template,
                    eval_ctx,
                    fully_resolved,
                    retained_bytes,
                )?;
            }
            Expr::Binary { left, right, .. }
                if matches!(
                    (&**left, &**right),
                    (Expr::Tuple(_), Expr::Subquery(_)) | (Expr::Subquery(_), Expr::Tuple(_))
                ) =>
            {
                *fully_resolved = false;
            }
            Expr::Binary { left, right, .. } => {
                rewrite(
                    db,
                    txn,
                    left,
                    outer_template,
                    eval_ctx,
                    fully_resolved,
                    retained_bytes,
                )?;
                rewrite(
                    db,
                    txn,
                    right,
                    outer_template,
                    eval_ctx,
                    fully_resolved,
                    retained_bytes,
                )?;
            }
            Expr::InList { expr, list, .. } => {
                rewrite(
                    db,
                    txn,
                    expr,
                    outer_template,
                    eval_ctx,
                    fully_resolved,
                    retained_bytes,
                )?;
                for item in list {
                    rewrite(
                        db,
                        txn,
                        item,
                        outer_template,
                        eval_ctx,
                        fully_resolved,
                        retained_bytes,
                    )?;
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                rewrite(
                    db,
                    txn,
                    expr,
                    outer_template,
                    eval_ctx,
                    fully_resolved,
                    retained_bytes,
                )?;
                rewrite(
                    db,
                    txn,
                    low,
                    outer_template,
                    eval_ctx,
                    fully_resolved,
                    retained_bytes,
                )?;
                rewrite(
                    db,
                    txn,
                    high,
                    outer_template,
                    eval_ctx,
                    fully_resolved,
                    retained_bytes,
                )?;
            }
            Expr::Like { expr, pattern, .. } => {
                rewrite(
                    db,
                    txn,
                    expr,
                    outer_template,
                    eval_ctx,
                    fully_resolved,
                    retained_bytes,
                )?;
                rewrite(
                    db,
                    txn,
                    pattern,
                    outer_template,
                    eval_ctx,
                    fully_resolved,
                    retained_bytes,
                )?;
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                if let Some(operand) = operand {
                    rewrite(
                        db,
                        txn,
                        operand,
                        outer_template,
                        eval_ctx,
                        fully_resolved,
                        retained_bytes,
                    )?;
                }
                for condition in conditions {
                    rewrite(
                        db,
                        txn,
                        condition,
                        outer_template,
                        eval_ctx,
                        fully_resolved,
                        retained_bytes,
                    )?;
                }
                for result in results {
                    rewrite(
                        db,
                        txn,
                        result,
                        outer_template,
                        eval_ctx,
                        fully_resolved,
                        retained_bytes,
                    )?;
                }
                if let Some(else_result) = else_result {
                    rewrite(
                        db,
                        txn,
                        else_result,
                        outer_template,
                        eval_ctx,
                        fully_resolved,
                        retained_bytes,
                    )?;
                }
            }
            Expr::Function { args, .. } | Expr::Tuple(args) => {
                for arg in args {
                    rewrite(
                        db,
                        txn,
                        arg,
                        outer_template,
                        eval_ctx,
                        fully_resolved,
                        retained_bytes,
                    )?;
                }
            }
            Expr::WindowFunction { args, spec, .. } => {
                let crate::parser::WindowSpec {
                    partition_by,
                    order_by,
                    ..
                } = spec.as_mut();

                for arg in args {
                    rewrite(
                        db,
                        txn,
                        arg,
                        outer_template,
                        eval_ctx,
                        fully_resolved,
                        retained_bytes,
                    )?;
                }
                for partition in partition_by {
                    rewrite(
                        db,
                        txn,
                        partition,
                        outer_template,
                        eval_ctx,
                        fully_resolved,
                        retained_bytes,
                    )?;
                }
                for order in order_by {
                    rewrite(
                        db,
                        txn,
                        &mut order.expr,
                        outer_template,
                        eval_ctx,
                        fully_resolved,
                        retained_bytes,
                    )?;
                }
            }
            Expr::InSubquery {
                expr: tested_expr,
                subquery,
                negated,
            } => {
                if matches!(tested_expr.as_ref(), Expr::Tuple(_)) {
                    *fully_resolved = false;
                    return Ok(());
                }
                rewrite(
                    db,
                    txn,
                    tested_expr,
                    outer_template,
                    eval_ctx,
                    fully_resolved,
                    retained_bytes,
                )?;
                // Keep the rewrite bounded: the compiled InConstSet owns the
                // constants for the lifetime of this operator. Star-schema
                // dimension domains are normally well below this cap; a large
                // or allocation-heavy result keeps the existing memoized
                // scalar Apply path instead of creating unbudgeted query state.
                const MAX_BATCH_IN_VALUES: usize = 4096;
                if super::explain::select_is_uncacheable(subquery) {
                    *fully_resolved = false;
                    return Ok(());
                }
                let call = prepare_subquery_call(db, txn, eval_ctx, subquery, outer_template)?;
                if !call.outer_binding().is_empty() {
                    *fully_resolved = false;
                    return Ok(());
                }
                let QueryResponse::ResultSet(result) = execute_subquery_call(db, txn, call)? else {
                    return Err(error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        "IN subquery must return rows",
                    ));
                };
                if result.columns.len() != 1 {
                    return Err(crate::engine::expr::operand_columns_error(1));
                }
                if result.rows.len() > MAX_BATCH_IN_VALUES {
                    *fully_resolved = false;
                    return Ok(());
                }
                let mut list = Vec::with_capacity(result.rows.len());
                for row in result.rows {
                    let Some(value) = row.into_iter().next() else {
                        return Err(crate::engine::expr::operand_columns_error(1));
                    };
                    // Fixed-width values keep the bounded compiled set truly
                    // bounded. Strings/LOB/JSON retain the scalar memo, whose
                    // storage is already governed by subquery execution.
                    if !matches!(
                        value,
                        Value::Null
                            | Value::Int(_)
                            | Value::UInt(_)
                            | Value::Float(_)
                            | Value::Double(_)
                            | Value::DecimalCompact(..)
                            | Value::Decimal128(..)
                            | Value::Date(_)
                            | Value::Time(_)
                            | Value::DateTime(_)
                            | Value::Timestamp(_)
                            | Value::Year(_)
                    ) {
                        *fully_resolved = false;
                        return Ok(());
                    }
                    list.push(Expr::BoundValue(value));
                }
                // Account for both the retained AST constants and the
                // uncached ExprProgram's hash-table storage. The latter uses
                // an implementation-dependent capacity, so charge a
                // conservative two-key-plus-bucket allowance per AST slot.
                // The charge is transferred to BatchExprProgram and follows
                // the last operator owner; the uncached compile prevents the
                // statement-specific set from leaking into the global cache.
                *retained_bytes = retained_bytes.saturating_add(list.capacity().saturating_mul(
                    std::mem::size_of::<Expr>()
                        + 2 * std::mem::size_of::<crate::engine::expr::ValueKey>()
                        + 2 * std::mem::size_of::<usize>(),
                ));
                let tested =
                    std::mem::replace(tested_expr, Box::new(Expr::BoundValue(Value::Null)));
                *expr = Expr::InList {
                    expr: tested,
                    list,
                    negated: *negated,
                };
            }
            Expr::QuantifiedComparison { left, right, .. } => {
                rewrite(
                    db,
                    txn,
                    left,
                    outer_template,
                    eval_ctx,
                    fully_resolved,
                    retained_bytes,
                )?;
                match right {
                    QuantifiedComparisonRight::Subquery(_) => *fully_resolved = false,
                    QuantifiedComparisonRight::List(list) => {
                        for item in list {
                            rewrite(
                                db,
                                txn,
                                item,
                                outer_template,
                                eval_ctx,
                                fully_resolved,
                                retained_bytes,
                            )?;
                        }
                    }
                }
            }
            Expr::Exists { .. } => *fully_resolved = false,
            Expr::Literal(_)
            | Expr::Placeholder(_)
            | Expr::BoundValue(_)
            | Expr::Default(_)
            | Expr::DefaultQualified(_)
            | Expr::Identifier(_)
            | Expr::CompoundIdentifier(_)
            | Expr::UserVariable(_)
            | Expr::SystemVariable { .. }
            | Expr::Grouping(_)
            | Expr::Wildcard => {}
        }
        Ok(())
    }

    let mut resolved = expr.clone();
    let mut fully_resolved = true;
    let mut retained_bytes = 0usize;
    let mut eval_ctx = EvalQueryContext::default();
    rewrite(
        db,
        txn,
        &mut resolved,
        outer_template,
        &mut eval_ctx,
        &mut fully_resolved,
        &mut retained_bytes,
    )?;
    if !fully_resolved {
        return Ok(None);
    }
    let charge = if retained_bytes == 0 {
        None
    } else {
        let Some(budget) = crate::engine::executor::mem_budget::current_budget() else {
            return Ok(None);
        };
        let Ok(charge) =
            crate::engine::executor::mem_budget::OwnedMemCharge::new(budget, retained_bytes)
        else {
            // Compiling an IN result into constants is optional. A blocking
            // producer may already own the query's soft work-area budget;
            // preserve the memoized scalar Apply path in that case.
            return Ok(None);
        };
        Some(charge)
    };
    Ok(Some(ResolvedBatchPredicate {
        expr: resolved,
        charge,
    }))
}

fn execute_subquery_call(
    db: &Arc<Database>,
    txn: TxnView,
    call: SubqueryCall,
) -> Result<QueryResponse, QueryError> {
    let captured_ctes = current_subquery_frame()
        .map(|frame| frame.ctes.clone())
        .unwrap_or_else(crate::engine::executor::cte::current_cte_registry);
    let run = || -> Result<QueryResponse, QueryError> {
        match call {
            SubqueryCall::Cached { cached, params, .. } => {
                crate::engine::params::with_params(params, || {
                    super::plan::execute_compiled_select(
                        db,
                        txn,
                        &cached.parametric_stmt,
                        &cached.compiled,
                    )
                })
            }
            SubqueryCall::Legacy { bound, .. } => {
                super::super::dml::execute_expression_subquery(db, txn, bound, subquery_registry())
            }
        }
    };
    let response = if captured_ctes.is_empty() {
        run()?
    } else {
        crate::engine::executor::cte::with_cte_registry_extended(captured_ctes, run)?
    };
    // P1-1: scalar / IN / EXISTS / quantified subquery evaluators all
    // need a materialised row set so they can iterate / index. The
    // underlying SELECT may now run on the streaming path; collapse
    // the cursor here once so every call site stays index-friendly.
    response.materialize()
}

fn collect_outer_params(
    bindings: &[OuterBindingSpec],
    outer_row: &PlanRow,
) -> Result<(Vec<Value>, Vec<ValueKey>), QueryError> {
    let mut params = crate::engine::params::snapshot_current_params().unwrap_or_default();
    let mut keys = Vec::with_capacity(bindings.len());
    for spec in bindings {
        let field = resolve_plan_field(outer_row, spec.qualifier.as_deref(), &spec.column)?;
        let value = (field.value).clone();
        keys.push(ValueKey::from_with_collation(
            &value,
            Some(crate::charset::BINARY_COLLATION_ID),
        ));
        if spec.param_index < params.len() {
            params[spec.param_index] = value;
        } else if spec.param_index == params.len() {
            params.push(value);
        } else {
            return Err(error(
                crate::error::catalog::ER_WRONG_ARGUMENTS,
                "parameter is not bound",
            ));
        }
    }
    Ok((params, keys))
}

/// Per-statement plan cache for correlated/uncorrelated subqueries is
/// on by default. Set `MYDB_SUBQUERY_PLAN_CACHE=off` (or `0`/`false`/`no`)
/// to disable — useful for differential testing.
fn subquery_plan_cache_enabled() -> bool {
    if !crate::engine::executor::plan_cache_enabled_from_env() {
        return false;
    }
    match crate::util::env::var("MYDB_SUBQUERY_PLAN_CACHE")
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "off" | "0" | "false" | "no" => false,
        _ => true,
    }
}

/// Build a parametric plan if `subquery` is in a shape we know how to
/// cache. Returns `None` (no panic, no error) when the shape would be
/// unsafe or unsupported — the caller falls back to the legacy path.
fn try_build_cached_subquery_plan(
    db: &Arc<Database>,
    subquery: &Select,
    outer_row: &PlanRow,
) -> Option<Arc<CachedSubqueryPlan>> {
    if subquery.for_update || subquery.lock_clause.is_some() {
        return None;
    }
    if !subquery_plan_cache_enabled() {
        return None;
    }
    // Conservative gating: only cache shapes that execute_select itself
    // routes through the planner path. Derived/CTE/system-table flavours
    // take separate execution paths that don't accept an externally-built
    // PhysicalPlan, so caching there would be either incorrect or
    // pointless.
    if subquery.derived.is_some() {
        return None;
    }
    if subquery.table.as_deref().is_none() {
        // const SELECT — no outer refs to lift; the legacy path is fine
        // and cheap (no real planning happens).
        return None;
    }
    if select_references_system_table_for_caching(subquery) {
        return None;
    }
    if crate::engine::executor::cte::select_references_active_cte(subquery) {
        return None;
    }
    if crate::engine::executor::cte::select_uses_cte_in_joins(subquery)
        || crate::engine::executor::cte::select_has_join_derived(subquery)
    {
        return None;
    }
    if subquery
        .from
        .first()
        .map(|t| crate::engine::executor::cte::has_cte(&t.name))
        .unwrap_or(false)
    {
        return None;
    }
    // Refuse bodies that contain another correlated subquery — our
    // outer-ref walk doesn't track per-level scopes, mirroring the
    // safety gate in `planner_decorrelate::scalar`.
    if select_body_contains_subquery(subquery) {
        return None;
    }
    // Aggregate ownership in a WHERE predicate is finalized after binding.
    // Do not cache a premature plan that would compile such a call as a
    // stored scalar function (1305) before the legacy binder can relocate it.
    if subquery
        .selection
        .as_ref()
        .is_some_and(super::expr_has_aggregate)
    {
        return None;
    }

    let mut sub = subquery.clone();
    let inner_cols = inner_select_columns(db, &sub);
    let inner_tables = inner_select_tables(&sub);
    // A table-less `SELECT *` inside a correlated subquery expands to the
    // outer row. Its width depends on that row, so it cannot be represented
    // by the reusable placeholder plan; use the legacy binder below.
    if inner_tables.is_empty()
        && sub
            .projection
            .iter()
            .any(|item| matches!(item, SelectItem::Wildcard))
    {
        return None;
    }
    let active_param_count = crate::engine::params::snapshot_current_params()
        .map(|params| params.len())
        .unwrap_or(0);
    let existing_param_count = next_placeholder_index_in_select(&sub);
    if active_param_count < existing_param_count {
        return None;
    }
    let mut next_param_index = active_param_count;
    let mut bindings: Vec<OuterBindingSpec> = Vec::new();
    if !substitute_outer_in_select_placeholders(
        &mut sub,
        &inner_cols,
        &inner_tables,
        &mut next_param_index,
        &mut bindings,
    ) {
        // No outer references — caching still pays off (avoids replanning
        // per outer row that hits us). Continue.
    }
    bind_outer_placeholder_collations(&mut sub, &bindings, outer_row);
    super::prepare_select_for_planning(db, &mut sub).ok()?;
    let planned = crate::planner::plan_select(db, &sub).ok()?;
    let physical = Arc::new(planned.physical);
    let compiled = Arc::new(crate::engine::cache::CompiledSelectPlan::new(
        Arc::new(sub.clone()),
        physical,
        smallvec::SmallVec::new(),
        smallvec::SmallVec::new(),
        0,
    ));
    Some(Arc::new(CachedSubqueryPlan {
        parametric_stmt: Arc::new(sub),
        compiled,
        bindings,
    }))
}

/// A lifted outer string column keeps its collation, exactly like the
/// per-row binder's `bound_outer_field`: `outer.s > 'abc'` inside the reusable
/// plan must compare under `outer.s`'s collation, not the connection default.
/// The outer row shape is fixed for one statement, so its metadata may be
/// baked into the template.
fn bind_outer_placeholder_collations(
    select: &mut Select,
    bindings: &[OuterBindingSpec],
    outer_row: &PlanRow,
) {
    let collated: HashMap<usize, Expr> = bindings
        .iter()
        .filter_map(|binding| {
            let field =
                resolve_plan_field(outer_row, binding.qualifier.as_deref(), &binding.column)
                    .ok()?;
            let bound = bound_outer_field(field);
            matches!(bound, Expr::Function { .. }).then(|| {
                let Expr::Function {
                    name,
                    mut args,
                    distinct,
                } = bound
                else {
                    unreachable!("checked above")
                };
                args[0] = Expr::Placeholder(binding.param_index);
                (
                    binding.param_index,
                    Expr::Function {
                        name,
                        args,
                        distinct,
                    },
                )
            })
        })
        .collect();
    if collated.is_empty() {
        return;
    }
    let rewrite = |expr: &mut Expr| {
        crate::parser::visit::rewrite_value_expr(expr, |node| match node {
            Expr::Placeholder(index) => {
                if let Some(wrapped) = collated.get(index) {
                    *node = wrapped.clone();
                }
                false
            }
            _ => true,
        })
    };
    for item in &mut select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            rewrite(expr);
        }
    }
    for expr in select
        .selection
        .iter_mut()
        .chain(select.having.iter_mut())
        .chain(
            select
                .joins
                .iter_mut()
                .filter_map(|join| join.condition.as_mut()),
        )
        .chain(select.group_by.iter_mut())
    {
        rewrite(expr);
    }
    for order in &mut select.order_by {
        rewrite(&mut order.expr);
    }
}

fn select_references_system_table_for_caching(select: &Select) -> bool {
    let is_sys = |name: &str| {
        let lc = name.to_ascii_lowercase();
        lc.starts_with("information_schema.")
            || lc.starts_with("mysql.")
            || lc.starts_with("performance_schema.")
            || lc == "information_schema"
            || lc == "mysql"
            || lc == "performance_schema"
    };
    for t in &select.from {
        if is_sys(&t.name) {
            return true;
        }
    }
    for j in &select.joins {
        if is_sys(&j.table.name) {
            return true;
        }
    }
    false
}

/// True when the SELECT body still mentions any Expr::Subquery /
/// InSubquery / Exists in a position our outer-ref walk would visit.
/// We refuse to plan-cache such bodies because the nested subquery's
/// own inner scope isn't tracked by the single inner_cols/inner_tables
/// pair we compute here.
fn select_body_contains_subquery(select: &Select) -> bool {
    crate::parser::visit::select_body_contains_subquery(select)
}

fn next_placeholder_index_in_select(select: &Select) -> usize {
    crate::parser::visit::next_select_parameter_index(select)
}

/// Walks the SELECT and replaces every outer-column reference with a
/// fresh `Expr::Placeholder`, recording the lookup spec in `bindings`
/// (positionally matched to the placeholder index). Mirrors
/// [`substitute_outer_in_select`] but produces parametric output suitable
/// for reuse across many outer rows. Returns true if any reference was
/// lifted.
fn substitute_outer_in_select_placeholders(
    select: &mut Select,
    inner_cols: &HashSet<String>,
    inner_tables: &HashSet<String>,
    next_param_index: &mut usize,
    bindings: &mut Vec<OuterBindingSpec>,
) -> bool {
    let mut found = false;
    for item in &mut select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            found |= substitute_outer_in_expr_placeholders(
                expr,
                inner_cols,
                inner_tables,
                next_param_index,
                bindings,
            );
        }
    }
    if let Some(json_table) = &mut select.json_table {
        found |= substitute_outer_in_expr_placeholders(
            &mut json_table.expr,
            inner_cols,
            inner_tables,
            next_param_index,
            bindings,
        );
    }
    if let Some(selection) = &mut select.selection {
        found |= substitute_outer_in_expr_placeholders(
            selection,
            inner_cols,
            inner_tables,
            next_param_index,
            bindings,
        );
    }
    if let Some(having) = &mut select.having {
        found |= substitute_outer_in_expr_placeholders(
            having,
            inner_cols,
            inner_tables,
            next_param_index,
            bindings,
        );
    }
    for join in &mut select.joins {
        if let Some(condition) = &mut join.condition {
            found |= substitute_outer_in_expr_placeholders(
                condition,
                inner_cols,
                inner_tables,
                next_param_index,
                bindings,
            );
        }
    }
    for expr in &mut select.group_by {
        found |= substitute_outer_in_expr_placeholders(
            expr,
            inner_cols,
            inner_tables,
            next_param_index,
            bindings,
        );
    }
    let mut order_cols = inner_cols.clone();
    order_cols.extend(select.projection.iter().filter_map(|item| match item {
        SelectItem::Expr {
            alias: Some(alias), ..
        } => Some(alias.to_ascii_lowercase()),
        _ => None,
    }));
    for order in &mut select.order_by {
        found |= substitute_outer_in_expr_placeholders(
            &mut order.expr,
            &order_cols,
            inner_tables,
            next_param_index,
            bindings,
        );
    }
    found
}

fn substitute_outer_in_expr_placeholders(
    expr: &mut Expr,
    inner_cols: &HashSet<String>,
    inner_tables: &HashSet<String>,
    next_param_index: &mut usize,
    bindings: &mut Vec<OuterBindingSpec>,
) -> bool {
    match expr {
        Expr::Identifier(name) => {
            let lower = name.to_ascii_lowercase();
            if !inner_cols.contains(&lower) {
                let idx = *next_param_index;
                *next_param_index += 1;
                bindings.push(OuterBindingSpec {
                    param_index: idx,
                    qualifier: None,
                    column: name.clone(),
                });
                *expr = Expr::Placeholder(idx);
                return true;
            }
            false
        }
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let table = parts[parts.len() - 2].clone();
            let col = parts[parts.len() - 1].clone();
            if !inner_tables.contains(&table.to_ascii_lowercase()) {
                let idx = *next_param_index;
                *next_param_index += 1;
                bindings.push(OuterBindingSpec {
                    param_index: idx,
                    qualifier: Some(table),
                    column: col,
                });
                *expr = Expr::Placeholder(idx);
                return true;
            }
            false
        }
        _ if crate::parser::visit::scalar_expr_children(expr) => {
            let mut found = false;
            crate::parser::visit::for_each_expr_child_mut(expr, |child| {
                found |= substitute_outer_in_expr_placeholders(
                    child,
                    inner_cols,
                    inner_tables,
                    next_param_index,
                    bindings,
                );
            });
            found
        }
        // Nested subqueries — caller gates these off via
        // select_body_contains_subquery before we get here, so any hit
        // is a bug. Return false (no substitution) so we don't corrupt
        // the AST; the gate has already routed us to the legacy path.
        Expr::InSubquery { .. } | Expr::Subquery(_) | Expr::Exists { .. } => false,
        _ => false,
    }
}

fn inner_select_columns(db: &Arc<Database>, select: &Select) -> HashSet<String> {
    let mut cols = HashSet::new();
    let mut tables: Vec<&str> = Vec::new();
    for table in &select.from {
        tables.push(table.name.as_str());
    }
    for join in &select.joins {
        tables.push(join.table.name.as_str());
    }
    for name in tables {
        // CTEs win over real tables when the name overlaps; the
        // outer-correlation rewrite must treat the CTE's projection
        // columns as belonging to the inner scope, otherwise an inner
        // reference like `total_revenue` would be re-bound to the
        // outer row's value of the same name.
        if let Some(cte) = super::super::cte::lookup_cte(name) {
            for col in &cte.columns {
                cols.insert(col.name.to_ascii_lowercase());
            }
            continue;
        }
        if let Some(def) = crate::engine::information_schema::find_system_table_name(name) {
            for col in def.columns {
                cols.insert(col.name.to_ascii_lowercase());
            }
        } else if let Ok(schema) = db.table(name) {
            for col in &schema.columns {
                cols.insert(col.name.to_ascii_lowercase());
            }
        }
    }
    // A set-operation derived table's output column names are those of its
    // leftmost SELECT (SQL semantics).
    for derived in select
        .derived
        .iter()
        .chain(select.joins.iter().filter_map(|join| join.derived.as_ref()))
        .filter_map(SubQuery::leftmost_select)
    {
        if let Ok(names) = planner::query_output_column_names(db, derived) {
            cols.extend(names.into_iter().map(|name| name.to_ascii_lowercase()));
        } else {
            // Correlated nested derived bodies may require an outer scope to
            // describe wildcards, but explicit output names remain local.
            for item in &derived.projection {
                if let SelectItem::Expr { expr, alias } = item {
                    if let Some(name) = alias.as_deref().or_else(|| expr_column_name(expr)) {
                        cols.insert(name.to_ascii_lowercase());
                    }
                }
            }
        }
    }
    cols
}

fn inner_select_tables(select: &Select) -> HashSet<String> {
    // SQL scoping: once a source is aliased, the alias is the *only* valid
    // qualifier — the base table name is hidden (mirrors
    // `planner::ScopeTable::matches_name`). The correlated-subquery rewrite
    // uses this set to decide whether a qualified reference is inner (leave
    // it) or outer (substitute it). If we also registered the base name of
    // an aliased inner table, a self-correlation like
    // `SELECT (SELECT COUNT(*) FROM t t2 WHERE t2.id < t.id) FROM t` would
    // misclassify the outer ref `t.id` as inner — leaving it unsubstituted
    // so the inner scope (which only knows alias `t2`) rejects it with 1054.
    let mut names = HashSet::new();
    for table in &select.from {
        if let Some(alias) = &table.alias {
            names.insert(alias.to_ascii_lowercase());
        } else {
            let last = table.name.rsplit('.').next().unwrap_or(&table.name);
            names.insert(last.to_ascii_lowercase());
        }
    }
    for join in &select.joins {
        if let Some(alias) = &join.table.alias {
            names.insert(alias.to_ascii_lowercase());
        } else {
            let last = join
                .table
                .name
                .rsplit('.')
                .next()
                .unwrap_or(&join.table.name);
            names.insert(last.to_ascii_lowercase());
        }
    }
    names
}

// WITH and set-operation wrappers in expression subqueries are ordinary
// lexical scopes. Keep every substitution in the caller's binding collector
// so memoization keys include values referenced inside their definitions/body.
fn substitute_outer_in_query<'q>(
    db: &Arc<Database>,
    query: impl Into<crate::parser::QueryMut<'q>>,
    outer_row: &PlanRow,
) -> bool {
    let query = query.into();
    match query {
        crate::parser::QueryMut::Select(select) => {
            let columns = inner_select_columns(db, select);
            let tables = inner_select_tables(select);
            substitute_outer_in_select(db, select, outer_row, &columns, &tables)
        }
        // Fusion inputs are independent query scopes. Preserve unresolved
        // outer references so ordinary binding reports them as unsupported.
        crate::parser::QueryMut::Rrf(_) => false,
        crate::parser::QueryMut::SetOperation(set) => {
            let left = substitute_outer_in_query(db, &mut set.left, outer_row);
            substitute_outer_in_query(db, &mut set.right, outer_row) || left
        }
        crate::parser::QueryMut::WithCte(mut with) => {
            let stubs = super::super::cte::describe_definitions(&with.ctes, |query| {
                super::describe_subquery_columns(db, query)
            })
            .unwrap_or_default();
            super::super::cte::with_cte_registry_extended(stubs, || {
                let mut found = false;
                for cte in with.ctes.iter_mut() {
                    found |= substitute_outer_in_query(db, &mut cte.query, outer_row);
                }
                found | substitute_outer_in_query(db, &mut with.body, outer_row)
            })
        }
        _ => false,
    }
}

fn substitute_outer_in_derived(
    db: &Arc<Database>,
    sub: &mut SubQuery,
    outer_row: &PlanRow,
) -> bool {
    match sub {
        SubQuery::Select(select) => {
            let columns = inner_select_columns(db, select);
            let tables = inner_select_tables(select);
            substitute_outer_in_select(db, select, outer_row, &columns, &tables)
        }
        statement @ (SubQuery::SetOp(_) | SubQuery::Rrf(_) | SubQuery::WithCte(_)) => {
            substitute_outer_in_query(db, statement, outer_row)
        }
    }
}

fn substitute_outer_in_select(
    db: &Arc<Database>,
    select: &mut Select,
    outer_row: &PlanRow,
    inner_cols: &HashSet<String>,
    inner_tables: &HashSet<String>,
) -> bool {
    let mut found = false;
    if let Some(derived) = &mut select.derived {
        found |= substitute_outer_in_derived(db, derived, outer_row);
    }
    for join in &mut select.joins {
        if let Some(derived) = &mut join.derived {
            found |= substitute_outer_in_derived(db, derived, outer_row);
        }
    }

    if inner_tables.is_empty()
        && select.derived.is_none()
        && select.json_table.is_none()
        && select.values.is_none()
        && select
            .projection
            .iter()
            .any(|item| matches!(item, SelectItem::Wildcard))
        && !outer_row.fields.is_empty()
    {
        let mut projection = Vec::with_capacity(outer_row.fields.len());
        for item in std::mem::take(&mut select.projection) {
            if matches!(item, SelectItem::Wildcard) {
                projection.extend(outer_row.fields.iter().map(|field| SelectItem::Expr {
                    expr: Expr::BoundValue(field.value.clone()),
                    alias: Some(field.column.clone()),
                }));
                found = true;
            } else {
                projection.push(item);
            }
        }
        select.projection = projection;
    }
    for item in &mut select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            found |= substitute_outer_in_expr(db, expr, outer_row, inner_cols, inner_tables);
        }
    }
    if let Some(json_table) = &mut select.json_table {
        found |= substitute_outer_in_expr(
            db,
            &mut json_table.expr,
            outer_row,
            inner_cols,
            inner_tables,
        );
    }
    if let Some(selection) = &mut select.selection {
        found |= substitute_outer_in_expr(db, selection, outer_row, inner_cols, inner_tables);
    }
    if let Some(having) = &mut select.having {
        found |= substitute_outer_in_expr(db, having, outer_row, inner_cols, inner_tables);
    }
    for join in &mut select.joins {
        if let Some(condition) = &mut join.condition {
            found |= substitute_outer_in_expr(db, condition, outer_row, inner_cols, inner_tables);
        }
    }
    for expr in &mut select.group_by {
        found |= substitute_outer_in_expr(db, expr, outer_row, inner_cols, inner_tables);
    }
    let mut order_cols = inner_cols.clone();
    order_cols.extend(select.projection.iter().filter_map(|item| match item {
        SelectItem::Expr {
            alias: Some(alias), ..
        } => Some(alias.to_ascii_lowercase()),
        _ => None,
    }));
    for order in &mut select.order_by {
        found |=
            substitute_outer_in_expr(db, &mut order.expr, outer_row, &order_cols, inner_tables);
    }
    found
}

/// A substituted outer value still has the column's collation and implicit
/// coercibility. This also keeps lateral projection metadata independent of
/// the connection's default collation.
fn bound_outer_field(field: &PlanField) -> Expr {
    let value = Expr::BoundValue(field.value.clone());
    if matches!(field.column_type, ColumnType::VarString | ColumnType::Blob) {
        if let Some(collation) = field
            .source
            .collation_id
            .and_then(crate::charset::resolve_collation_id)
        {
            return Expr::Function {
                name: "__COLLATED_CONST".into(),
                args: vec![
                    value,
                    Expr::Literal(Literal::String(collation.name.into())),
                    Expr::Literal(Literal::Int(2)),
                ],
                distinct: false,
            };
        }
    }
    value
}

fn substitute_outer_in_expr(
    db: &Arc<Database>,
    expr: &mut Expr,
    outer_row: &PlanRow,
    inner_cols: &HashSet<String>,
    inner_tables: &HashSet<String>,
) -> bool {
    if matches!(expr, Expr::Function { name, .. } if planner::is_aggregate_function(name)) {
        let mut references = 0usize;
        let mut outer_only = true;
        crate::parser::visit::inspect_value_expr(
            expr,
            |node| {
                match node {
                    Expr::Identifier(name) => {
                        references += 1;
                        outer_only &= !inner_cols.contains(&name.to_ascii_lowercase());
                    }
                    Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                        references += 1;
                        outer_only &=
                            !inner_tables.contains(&parts[parts.len() - 2].to_ascii_lowercase());
                    }
                    Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => {
                        outer_only = false;
                        return false;
                    }
                    _ => {}
                }
                true
            },
            |_| {},
        );
        if outer_only && references > 0 {
            if let Some(value) = outer_row
                .agg_values
                .as_ref()
                .and_then(|values| values.get(&super::aggregate::aggregate_key(expr)))
                .cloned()
            {
                push_outer_binding(&value);
                *expr = Expr::BoundValue(value);
                return true;
            }
        }
    }
    match expr {
        Expr::Identifier(name) => {
            let lower = name.to_ascii_lowercase();
            if !inner_cols.contains(&lower) {
                if let Ok(field) = resolve_plan_field(outer_row, None, name) {
                    push_outer_binding(&field.value);
                    *expr = bound_outer_field(field);
                    return true;
                }
            }
            false
        }
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let table = &parts[parts.len() - 2];
            let col = &parts[parts.len() - 1];
            if !inner_tables.contains(&table.to_ascii_lowercase()) {
                if let Ok(field) = resolve_plan_field(outer_row, Some(table), col) {
                    push_outer_binding(&field.value);
                    *expr = bound_outer_field(field);
                    return true;
                }
            }
            false
        }
        _ if crate::parser::visit::scalar_expr_children(expr) => {
            let mut found = false;
            crate::parser::visit::for_each_expr_child_mut(expr, |child| {
                found |= substitute_outer_in_expr(db, child, outer_row, inner_cols, inner_tables);
            });
            found
        }
        Expr::InSubquery { expr, subquery, .. } => {
            let l = substitute_outer_in_expr(db, expr, outer_row, inner_cols, inner_tables);
            let r = substitute_outer_in_nested_select(
                db,
                subquery,
                outer_row,
                inner_cols,
                inner_tables,
            );
            l || r
        }
        Expr::QuantifiedComparison { left, right, .. } => {
            let mut found = substitute_outer_in_expr(db, left, outer_row, inner_cols, inner_tables);
            match right {
                QuantifiedComparisonRight::Subquery(subquery) => {
                    found |= substitute_outer_in_nested_select(
                        db,
                        subquery,
                        outer_row,
                        inner_cols,
                        inner_tables,
                    );
                }
                QuantifiedComparisonRight::List(list) => {
                    for item in list {
                        found |=
                            substitute_outer_in_expr(db, item, outer_row, inner_cols, inner_tables);
                    }
                }
            }
            found
        }
        Expr::Subquery(subquery) | Expr::Exists { subquery, .. } => {
            substitute_outer_in_nested_select(db, subquery, outer_row, inner_cols, inner_tables)
        }
        _ => false,
    }
}

/// Descend into a nested query block without losing either its local scope or
/// any nearer enclosing scope.  The legacy binder is invoked against the
/// top-level outer row, so a flat parent scope is insufficient: in
/// `SELECT (SELECT MAX(id) FROM inner_t)`, `id` belongs to the nested block,
/// while in `SELECT (SELECT outer_t.id)` it may legitimately skip one or more
/// source-free blocks and bind to the top-level row.  The union protects every
/// closer scope from accidental capture and still lets genuinely external
/// references reach `outer_row`.
fn substitute_outer_in_nested_select(
    db: &Arc<Database>,
    select: &mut Select,
    outer_row: &PlanRow,
    enclosing_cols: &HashSet<String>,
    enclosing_tables: &HashSet<String>,
) -> bool {
    let mut visible_cols = enclosing_cols.clone();
    visible_cols.extend(inner_select_columns(db, select));
    let mut visible_tables = enclosing_tables.clone();
    visible_tables.extend(inner_select_tables(select));
    substitute_outer_in_select(db, select, outer_row, &visible_cols, &visible_tables)
}

pub(super) fn plan_expr_result_type(expr: &Expr, row: &PlanRow) -> Result<ColumnType, QueryError> {
    match expr {
        Expr::Identifier(name) => Ok(resolve_plan_field(row, None, name)?.column_type),
        Expr::CompoundIdentifier(parts) => {
            let column = parts.last().ok_or_else(|| {
                error(crate::error::catalog::ER_BAD_FIELD_ERROR, "unknown column")
            })?;
            let qualifier = if parts.len() >= 2 {
                Some(parts[parts.len() - 2].as_str())
            } else {
                None
            };
            Ok(resolve_plan_field(row, qualifier, column)?.column_type)
        }
        // `MATCH(...) AGAINST(...)` yields a floating-point relevance score.
        Expr::Function { name, .. } if name == crate::parser::FT_MATCH_FUNCTION => {
            Ok(ColumnType::Double)
        }
        Expr::Function { name, args, .. } if planner::is_aggregate_function(name) => {
            let arg_type = args
                .first()
                .map(|arg| plan_expr_result_type(arg, row))
                .transpose()
                .ok()
                .flatten();
            Ok(aggregate_function_result_type(
                name,
                aggregate_argument_semantic_type(args.first(), arg_type),
            ))
        }
        // Conditional functions type from their branches; resolve column-ref
        // branches against this plan row so `COALESCE(int_col, ..)` → BIGINT.
        Expr::Function { name, args, .. } => Ok(
            crate::engine::expr::conditional_function_result_type(name, args, |e| {
                plan_expr_result_type(e, row).unwrap_or(ColumnType::VarString)
            })
            .unwrap_or_else(|| expr_result_type(expr)),
        ),
        Expr::Binary { left, op, right } => Ok(crate::engine::expr::binary_expression_result_type(
            *op,
            plan_expr_result_type(left, row)?,
            plan_expr_result_type(right, row)?,
        )),
        Expr::Case { .. } => {
            // CASE result domains depend on every result branch.  The
            // context-free fallback cannot resolve column-backed DECIMAL
            // branches and consequently promotes exact CASE expressions to
            // DOUBLE (for example CASE ... THEN decimal_expr ELSE 0).  Apart
            // from exposing incorrect metadata, that false approximate
            // domain forces SUM/AVG out of the typed batch accumulator.
            let resolve_column = |column_expr: &Expr| {
                let field = match column_expr {
                    Expr::Identifier(name) => resolve_plan_field(row, None, name).ok(),
                    Expr::CompoundIdentifier(parts) => {
                        let column = parts.last()?;
                        let qualifier = (parts.len() >= 2).then(|| parts[parts.len() - 2].as_str());
                        resolve_plan_field(row, qualifier, column).ok()
                    }
                    _ => None,
                }?;
                Some(crate::engine::expr_type::ExprType::from_column_type(
                    field.column_type,
                    &field.source,
                ))
            };
            let resolve_placeholder = |_: usize| None;
            let context = crate::engine::expr_type::TypeContext::context_free(
                &resolve_column,
                &resolve_placeholder,
            );
            Ok(crate::engine::expr_type::infer_expr_type(expr, &context).column_type())
        }
        _ => Ok(expr_result_type(expr)),
    }
}

fn plan_field_domain(args: &[Expr], row: &PlanRow) -> crate::engine::expr::FieldComparisonDomain {
    let resolve_column = |expr: &Expr| {
        let field = match expr {
            Expr::Identifier(name) => resolve_plan_field(row, None, name).ok(),
            Expr::CompoundIdentifier(parts) => {
                let name = parts.last()?;
                let qualifier = (parts.len() >= 2).then(|| parts[parts.len() - 2].as_str());
                resolve_plan_field(row, qualifier, name).ok()
            }
            _ => None,
        }?;
        Some(crate::engine::expr_type::ExprType::from_column_type(
            field.column_type,
            &field.source,
        ))
    };
    let no_placeholder = |_: usize| None;
    let context =
        crate::engine::expr_type::TypeContext::context_free(&resolve_column, &no_placeholder);
    crate::engine::expr::field_comparison_domain(
        args.iter()
            .map(|arg| crate::engine::expr_type::infer_expr_type(arg, &context)),
    )
}

pub(crate) fn aggregate_function_result_type(
    name: &str,
    arg_type: Option<ColumnType>,
) -> ColumnType {
    let no_column = |_: &Expr| None;
    let no_placeholder = |_: usize| None;
    let context = crate::engine::expr_type::TypeContext::context_free(&no_column, &no_placeholder);
    let argument = arg_type.map(|column_type| {
        crate::engine::expr_type::ExprType::from_column_type(
            column_type,
            &crate::types::ColumnSource::default(),
        )
    });
    crate::engine::expr_type::aggregate_result_type(name, argument, &context).column_type()
}

pub(crate) fn aggregate_argument_semantic_type(
    argument: Option<&Expr>,
    resolved: Option<ColumnType>,
) -> Option<ColumnType> {
    if argument.is_some_and(is_conditional_computed_year_domain) {
        // YEAR() is an ordinary integer scalar, but once wrapped in a
        // conditional expression MySQL's AVG resolver uses the approximate
        // numeric lane. This is observably different from AVG(year_column),
        // which retains the native YEAR domain.
        Some(ColumnType::Double)
    } else if argument.is_some_and(is_direct_computed_year_domain) {
        // YEAR(date) is an ordinary signed integer expression, not a native
        // YEAR column. AVG therefore applies div_precision_increment and
        // returns DECIMAL(...,4) under the default session setting.
        Some(ColumnType::LongLong)
    } else {
        resolved
    }
}

fn is_direct_computed_year_domain(expr: &Expr) -> bool {
    match expr {
        Expr::Function { name, .. } if name.eq_ignore_ascii_case("YEAR") => true,
        Expr::GroupKeyRef { expr, .. } | Expr::Unary { expr, .. } => {
            is_direct_computed_year_domain(expr)
        }
        _ => false,
    }
}

fn is_conditional_computed_year_domain(expr: &Expr) -> bool {
    match expr {
        Expr::Function { name, args, .. }
            if matches!(
                name.to_ascii_uppercase().as_str(),
                "NULLIF" | "IF" | "IFNULL" | "COALESCE"
            ) =>
        {
            args.iter().any(is_direct_computed_year_domain)
        }
        Expr::Case {
            results,
            else_result,
            ..
        } => {
            results.iter().any(is_direct_computed_year_domain)
                || else_result
                    .as_deref()
                    .is_some_and(is_direct_computed_year_domain)
        }
        Expr::GroupKeyRef { expr, .. } | Expr::Unary { expr, .. } => {
            is_conditional_computed_year_domain(expr)
        }
        _ => false,
    }
}

fn direct_argument_source_decimals(expr: &Expr, row: &PlanRow) -> Option<u8> {
    let field = match expr {
        Expr::Identifier(name) => resolve_plan_field(row, None, name).ok(),
        Expr::CompoundIdentifier(parts) => {
            let column = parts.last()?;
            let qualifier = (parts.len() >= 2).then(|| parts[parts.len() - 2].as_str());
            resolve_plan_field(row, qualifier, column).ok()
        }
        Expr::GroupKeyRef { expr, .. } => {
            return direct_argument_source_decimals(expr, row);
        }
        _ => None,
    }?;
    field.source.decimals.filter(|decimals| *decimals != 0x1f)
}

fn direct_argument_source_type(expr: &Expr, row: &PlanRow) -> Option<ColumnType> {
    let field = match expr {
        Expr::Identifier(name) => resolve_plan_field(row, None, name).ok(),
        Expr::CompoundIdentifier(parts) => {
            let column = parts.last()?;
            let qualifier = (parts.len() >= 2).then(|| parts[parts.len() - 2].as_str());
            resolve_plan_field(row, qualifier, column).ok()
        }
        Expr::GroupKeyRef { expr, .. } => return direct_argument_source_type(expr, row),
        _ => None,
    }?;
    Some(field.column_type)
}

fn coerce_aggregate_result_for_expr(
    expr: &Expr,
    row: &PlanRow,
    value: Value,
) -> Result<Value, QueryError> {
    let Expr::Function { name, args, .. } = expr else {
        return Ok(value);
    };
    if name.eq_ignore_ascii_case("SUM") {
        if let (Some(scale), Value::Double(value)) = (
            args.first()
                .and_then(|arg| direct_argument_source_decimals(arg, row)),
            &value,
        ) {
            // FLOAT(M,D)/DOUBLE(M,D) inputs are rounded to D digits at the
            // column boundary. MySQL also exposes SUM in that declared
            // decimal domain for projection and HAVING, so binary residues
            // below D digits must not turn a logical zero into a non-zero
            // group. Preserve signed zero because it is observable in the
            // text protocol.
            return Ok(Value::Decimal(format!(
                "{value:.scale$}",
                scale = usize::from(scale)
            )));
        }
    }
    if name.eq_ignore_ascii_case("AVG") {
        if let (Some(source_scale), Value::Double(value)) = (
            args.first()
                .and_then(|arg| direct_argument_source_decimals(arg, row)),
            &value,
        ) {
            // AVG over an approximate FLOAT(M,D)/DOUBLE(M,D) remains a
            // DOUBLE result, but MySQL renders it with D plus the session's
            // div_precision_increment fractional digits. Bare FLOAT/DOUBLE
            // have NOT_FIXED_DEC metadata and intentionally stay on the
            // compact DOUBLE display path.
            let scale = (u32::from(source_scale)
                + u32::from(crate::engine::expr::session_div_precision_increment()))
            .min(u32::from(crate::util::decimal::MAX_DECIMAL_SCALE))
                as usize;
            return Ok(Value::Decimal(format!("{value:.scale$}")));
        }
    }
    if name.eq_ignore_ascii_case("AVG")
        && args
            .first()
            .is_some_and(is_conditional_computed_year_domain)
    {
        return Ok(match value {
            Value::Null => Value::Null,
            value => Value::Double(crate::engine::expr::numeric(&value)?),
        });
    }
    if name.eq_ignore_ascii_case("AVG")
        && args.first().is_some_and(|arg| {
            direct_argument_source_type(arg, row) == Some(ColumnType::Year)
                || plan_expr_result_type(arg, row).ok() == Some(ColumnType::Year)
        })
    {
        return Ok(match value {
            Value::Null => Value::Null,
            value => Value::Decimal(format!("{:.0}", crate::engine::expr::numeric(&value)?)),
        });
    }
    if matches!(name.to_ascii_uppercase().as_str(), "MIN" | "MAX") {
        if let Value::Year(year) = value {
            // A YEAR column renders its zero sentinel as `0000`, but MySQL's
            // MIN/MAX result is the numeric year domain and renders zero as
            // `0`. Keep aggregate comparison on the compact Year lane and
            // normalize only the finalized scalar.
            return Ok(Value::Int(if year == 0 {
                0
            } else {
                1900 + i64::from(year)
            }));
        }
    }
    Ok(value)
}

#[cfg(test)]
mod compiled_plan_expr_tests {
    use super::*;
    use crate::parser::{parse_statement, SelectItem, Statement};

    fn first_projection_expr(sql: &str) -> Expr {
        let Statement::Select(select) = parse_statement(sql).unwrap() else {
            panic!("expected select for {sql}");
        };
        match select.projection.into_iter().next().unwrap() {
            SelectItem::Expr { expr, .. } => expr,
            other => panic!("unexpected projection: {other:?}"),
        }
    }

    fn field(name: &str, value: Value) -> PlanField {
        PlanField::new("t", None, name, ColumnType::VarString, value)
    }

    #[test]
    fn vector_constants_parse_once_per_operator_across_native_domains() {
        use crate::engine::vector::codec::{parse_typed_text, text_parse_calls};
        use crate::vector::VectorDataType;
        for datatype in [
            VectorDataType::Float32,
            VectorDataType::Float16,
            VectorDataType::BFloat16,
            VectorDataType::Int8,
            VectorDataType::Binary,
        ] {
            let row = PlanRow::from_fields(vec![PlanField::new(
                "t",
                None,
                "v",
                ColumnType::Vector,
                Value::Vector(parse_typed_text("[1,0,1,0]", datatype).unwrap()),
            )]);
            let schema = input_schema_for_plan_row(&row);
            let metrics: &[&str] = if datatype == VectorDataType::Binary {
                &["hamming", "jaccard"]
            } else {
                &["l2", "cosine", "dot"]
            };
            for constructor in ["TO_VECTOR", "STRING_TO_VECTOR"] {
                for metric in metrics {
                    let expr = first_projection_expr(&format!(
                        "SELECT DISTANCE(v,{constructor}(?,?),'{metric}')"
                    ));
                    crate::engine::params::with_params(
                        vec![
                            Value::String("[0,1,1,0]".into()),
                            Value::String(datatype.name().into()),
                        ],
                        || {
                            let expected = eval_plan_expr_legacy(&expr, &row).unwrap();
                            let before = text_parse_calls();
                            let compiled = CompiledPlanExpr::compile_once(&expr, &schema);
                            for _ in 0..100 {
                                assert_eq!(compiled.eval(&row).unwrap(), expected);
                            }
                            assert_eq!(
                                text_parse_calls() - before,
                                1,
                                "{constructor}/{datatype}/{metric}"
                            );
                        },
                    );
                }
            }
        }
    }

    #[test]
    fn vector_constant_binding_preserves_parameters_errors_and_literal_provenance() {
        let row = PlanRow::default();
        let schema = input_schema_for_plan_row(&row);
        let expr = first_projection_expr("SELECT TO_VECTOR(?,?)");
        for (text, datatype) in [
            ("[1,0]", "FLOAT32"),
            ("[0,1]", "BINARY"),
            ("[bad]", "FLOAT32"),
            ("[2,1]", "INT8"),
        ] {
            crate::engine::params::with_params(
                vec![Value::String(text.into()), Value::String(datatype.into())],
                || {
                    let expected = eval_plan_expr_legacy(&expr, &row);
                    let actual = CompiledPlanExpr::compile_once(&expr, &schema).eval(&row);
                    match (expected, actual) {
                        (Ok(left), Ok(right)) => assert_eq!(left, right),
                        (Err(left), Err(right)) => assert_eq!(left.code(), right.code()),
                        other => panic!("binding mismatch: {other:?}"),
                    }
                },
            );
        }
        let expr = first_projection_expr("SELECT TO_VECTOR(CONCAT('[',CAST(? AS UNSIGNED),']'))");
        for auto in [false, true] {
            let run = || {
                let expected = eval_plan_expr_legacy(&expr, &row).unwrap();
                assert_eq!(
                    CompiledPlanExpr::compile_once(&expr, &schema)
                        .eval(&row)
                        .unwrap(),
                    expected
                );
            };
            if auto {
                crate::engine::params::with_auto_params(vec![Value::Bytes(vec![0x31])], run);
            } else {
                crate::engine::params::with_params(vec![Value::Bytes(vec![0x31])], run);
            }
        }
    }

    #[test]
    fn vector_constant_probe_does_not_consume_runtime_warnings_or_fold_columns() {
        let row = PlanRow::from_fields(vec![field("txt", Value::String("[1,0]".into()))]);
        let schema = input_schema_for_plan_row(&row);
        let column = first_projection_expr("SELECT TO_VECTOR(txt)");
        assert!(
            crate::engine::expr_program::ExprCompiler::bind_vector_constants(&column).is_none()
        );
        let expr =
            first_projection_expr("SELECT TO_VECTOR(CONCAT('[',CAST('1tail' AS SIGNED),',0]'))");
        crate::engine::api::clear_statement_warnings();
        let compiled = CompiledPlanExpr::compile_once(&expr, &schema);
        assert!(crate::engine::api::statement_warnings_snapshot().is_empty());
        for _ in 0..3 {
            crate::engine::api::clear_statement_warnings();
            assert!(matches!(compiled.eval(&row).unwrap(), Value::Vector(_)));
            assert!(!crate::engine::api::statement_warnings_snapshot().is_empty());
        }
        crate::engine::api::clear_statement_warnings();
    }

    #[test]
    fn routine_expression_cache_does_not_outlive_bound_ast() {
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
            dir.path(),
            crate::engine::executor::tests::memory_test_storage_config(),
        )
        .unwrap();
        engine
            .execute("CREATE TABLE routine_cache_probe(v INT)")
            .unwrap();
        engine
            .execute("INSERT INTO routine_cache_probe VALUES(11)")
            .unwrap();
        let db = engine.db_for_tests();
        let txn = db.begin_read_only_txn().unwrap();
        let session = SessionContext::default();
        let mut expr = first_projection_expr("SELECT (SELECT v FROM routine_cache_probe)");
        with_subquery_context(db, txn, || {
            assert_eq!(
                eval_contextual_subquery_expr(&session, &mut expr).unwrap(),
                Value::Int(11),
            );
            // Reuse the exact Select allocation to deterministically model
            // two short-lived bound routine ASTs receiving the same address.
            let Statement::Select(replacement) =
                parse_statement("SELECT 42 FROM routine_cache_probe").unwrap()
            else {
                unreachable!()
            };
            let Expr::Subquery(query) = &mut expr else {
                unreachable!()
            };
            query.projection = replacement.projection;
            assert_eq!(
                eval_contextual_subquery_expr(&session, &mut expr).unwrap(),
                Value::Int(42),
            );
        });
        db.rollback_txn(txn.txn_id).unwrap();
    }

    #[test]
    fn aggregate_fallback_uses_ordered_slots_and_exact_empty_states() {
        let rows = [("alpha", 1), ("beta", 3), ("gamma", 2)]
            .into_iter()
            .map(|(a, b)| {
                PlanRow::from_fields(vec![
                    field("a", Value::String(a.into())),
                    field("b", Value::Int(b)),
                ])
            })
            .collect::<Vec<_>>();
        let expr = first_projection_expr("SELECT GROUP_CONCAT(a ORDER BY b DESC SEPARATOR '|')");
        assert_eq!(
            eval_aggregate_expr(&expr, &rows, &rows[0]).unwrap(),
            Value::String("beta|gamma|alpha".into())
        );
        for (sql, expected) in [
            ("SELECT COUNT(*)", Value::Int(0)),
            ("SELECT COUNT(DISTINCT a,b)", Value::Int(0)),
            ("SELECT SUM(b)", Value::Null),
            ("SELECT AVG(b)", Value::Null),
            ("SELECT MIN(b)", Value::Null),
            ("SELECT JSON_ARRAYAGG(a)", Value::Null),
        ] {
            let expr = first_projection_expr(sql);
            assert_eq!(
                eval_aggregate_expr(&expr, &[], &rows[0]).unwrap(),
                expected,
                "{sql}"
            );
        }
    }

    #[test]
    fn aggregate_fallback_count_distinct_preserves_tuple_and_null_semantics() {
        let expr = first_projection_expr("SELECT COUNT(DISTINCT a, b)");
        let rows = [(1, Some(1)), (1, Some(2)), (1, Some(2)), (1, None)]
            .into_iter()
            .map(|(a, b)| {
                PlanRow::from_fields(vec![
                    field("a", Value::Int(a)),
                    field("b", b.map(Value::Int).unwrap_or(Value::Null)),
                ])
            })
            .collect::<Vec<_>>();
        assert_eq!(
            eval_aggregate_expr(&expr, &rows, &rows[0]).unwrap(),
            Value::Int(2)
        );
    }

    #[test]
    fn aggregate_fallback_group_concat_obeys_session_limit() {
        let expr = first_projection_expr("SELECT GROUP_CONCAT(a)");
        let rows = ["aaa", "bbb"]
            .into_iter()
            .map(|value| PlanRow::from_fields(vec![field("a", Value::String(value.into()))]))
            .collect::<Vec<_>>();
        super::super::aggregate::with_group_concat_max_len(5, || {
            assert_eq!(
                eval_aggregate_expr(&expr, &rows, &rows[0]).unwrap(),
                Value::String("aaa,b".into())
            );
        });
    }

    #[test]
    fn binding_parameter_space_covers_all_query_variants() {
        for (sql, expected) in [
            ("SELECT ? FROM (SELECT ? UNION ALL SELECT ?) AS d", 3),
            (
                "SELECT ? FROM (WITH c AS (SELECT ? AS v) SELECT ? FROM c) AS d",
                3,
            ),
            ("SELECT ? FROM t JOIN (SELECT ? AS v) d ON t.id=d.v", 2),
        ] {
            let Statement::Select(select) = parse_statement(sql).unwrap() else {
                panic!()
            };
            assert_eq!(next_placeholder_index_in_select(&select), expected, "{sql}");
        }
    }

    #[test]
    fn batch_in_constant_rewrite_falls_back_under_query_pressure() {
        use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
        let root = tempfile::tempdir().unwrap();
        let db = Arc::new(Database::open(root.path()).unwrap());
        let txn = TxnView::read_only(0, crate::storage::IsolationLevel::RepeatableRead);
        let expr = first_projection_expr("SELECT 1 IN (SELECT 1)");
        let budget = Arc::new(MemBudget::new(1024 * 1024));
        with_query_budget(budget.clone(), || {
            assert!(
                resolve_batch_constant_scalar_subqueries(&db, txn, &expr, &PlanRow::default())
                    .unwrap()
                    .is_some()
            );
            budget.force_charge(budget.limit()).unwrap();
            assert!(
                resolve_batch_constant_scalar_subqueries(&db, txn, &expr, &PlanRow::default())
                    .unwrap()
                    .is_none()
            );
            assert!(budget.terminal_oom().is_none());
            budget.release(budget.limit());
        });
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn recognizes_volatile_source_free_scalar_wrapper() {
        let Expr::Subquery(select) =
            first_projection_expr("SELECT (SELECT ROUND(RAND(0) * (SELECT MAX(id) FROM t)))")
        else {
            panic!("expected scalar wrapper");
        };
        assert!(super::source_free_scalar_wrapper_expr(&select).is_some());
        assert!(super::super::explain::select_is_uncacheable(&select));
    }

    #[test]
    fn compiled_plan_expr_matches_legacy_eval() {
        let row = PlanRow {
            fields: vec![
                field("a", Value::Int(12)),
                field("b", Value::String("beta".into())),
                field("j", Value::String("{\"x\":3}".into())),
            ],
            group: None,
            agg_values: None,
            schema: PlanRow::empty_schema(),
        };
        let schema = input_schema_for_plan_row(&row);
        for expr in [
            first_projection_expr("SELECT IF(a > 10, UPPER(b), LOWER(b))"),
            first_projection_expr("SELECT (a IN (5, 12, NULL)) AND REGEXP_LIKE(b, '^[a-z]', 'i')"),
            first_projection_expr(
                "SELECT CASE WHEN a BETWEEN 5 AND 15 THEN JSON_EXTRACT(j, '$.x') ELSE 'out' END",
            ),
        ] {
            let legacy = eval_plan_expr_legacy(&expr, &row).unwrap();
            let compiled = CompiledPlanExpr::compile_once(&expr, &schema);
            assert_eq!(compiled.eval(&row).unwrap(), legacy);
        }
    }

    #[test]
    fn owned_projection_moves_distinct_simple_fields_in_projection_order() {
        let Statement::Select(select) = parse_statement("SELECT b, a FROM t").unwrap() else {
            panic!("expected select");
        };
        let row = PlanRow::from_fields(vec![
            field("a", Value::String("alpha".into())),
            field("b", Value::String("beta".into())),
        ]);
        let program = PlanProjectionProgram::compile_once(&row, &select.projection);
        assert_eq!(
            program.project_row_owned(row).unwrap(),
            vec![Value::String("beta".into()), Value::String("alpha".into())]
        );
    }

    #[test]
    fn owned_projection_preserves_duplicate_and_computed_expression_semantics() {
        let Statement::Select(select) =
            parse_statement("SELECT a, a, CONCAT(a, b) FROM t").unwrap()
        else {
            panic!("expected select");
        };
        let row = PlanRow::from_fields(vec![
            field("a", Value::String("x".into())),
            field("b", Value::String("y".into())),
        ]);
        let program = PlanProjectionProgram::compile_once(&row, &select.projection);
        let borrowed = program.project_row_values(&row).unwrap();
        assert_eq!(program.project_row_owned(row).unwrap(), borrowed);
        assert_eq!(
            borrowed,
            vec![
                Value::String("x".into()),
                Value::String("x".into()),
                Value::String("xy".into()),
            ]
        );
    }

    #[test]
    fn plan_row_input_schema_preserves_source_collation() {
        let collation_id = crate::charset::UTF8MB4_0900_AI_CI_ID;
        let row = PlanRow::from_fields(vec![PlanField {
            meta: intern_col_meta(ColMeta {
                table: "t".into(),
                alias: None,
                column: "s".into(),
                column_type: ColumnType::VarString,
                source: crate::types::ColumnSource {
                    collation_id: Some(collation_id),
                    ..Default::default()
                },
                invisible: false,
            }),
            value: Value::String("abc".into()),
        }]);
        let schema = input_schema_for_plan_row(&row);
        let expr = first_projection_expr("SELECT s");
        assert_eq!(
            schema
                .column_collation_info(&expr)
                .map(|collation| collation.id),
            Some(collation_id)
        );

        let comparison = first_projection_expr("SELECT s = 'ABC'");
        let legacy = eval_plan_expr_legacy(&comparison, &row).unwrap();
        let compiled = CompiledPlanExpr::compile_once(&comparison, &schema);
        assert_eq!(legacy, Value::Int(1));
        assert_eq!(compiled.eval(&row).unwrap(), legacy);
    }

    #[test]
    fn db_eval_binds_legacy_row_metadata_once_per_ast_node() {
        let row = PlanRow::from_fields(vec![PlanField {
            meta: intern_col_meta(ColMeta {
                table: "t".into(),
                alias: None,
                column: "a".into(),
                column_type: ColumnType::LongLong,
                source: crate::types::ColumnSource {
                    flags: crate::mysql::protocol::UNSIGNED_FLAG,
                    ..Default::default()
                },
                invisible: false,
            }),
            value: Value::UInt(12),
        }]);
        let expr = first_projection_expr("SELECT a");
        let comparison = first_projection_expr("SELECT a = 12");
        let Expr::Binary { left, right, .. } = &comparison else {
            panic!("expected binary comparison");
        };
        let mut eval_ctx = EvalQueryContext::default();

        assert!(eval_ctx.row_expr_metadata(&expr, &row).result_unsigned);
        assert!(eval_ctx.row_expr_metadata(&expr, &row).result_unsigned);
        assert_eq!(eval_ctx.row_expr_metadata.len(), 1);

        let collation_id = eval_ctx.binary_collation_id(&comparison, left, right, &row);
        assert_eq!(
            eval_ctx.binary_collation_id(&comparison, left, right, &row),
            collation_id
        );
        assert_eq!(eval_ctx.binary_collations.len(), 1);
    }

    #[test]
    fn compiled_plan_predicate_handles_expression_in_const_set() {
        let row = PlanRow {
            fields: vec![field("b", Value::String("beta".into()))],
            group: None,
            agg_values: None,
            schema: PlanRow::empty_schema(),
        };
        let expr = first_projection_expr("SELECT SUBSTRING(b, 1, 1) IN ('a', 'b', 'c', 'd', NULL)");
        let compiled = CompiledPlanPredicate::compile_once(&expr, &row);
        assert_eq!(
            compiled.eval_bool(&row).unwrap(),
            truthy(&eval_plan_expr_legacy(&expr, &row).unwrap())
        );

        let miss = PlanRow {
            fields: vec![field("b", Value::String("zeta".into()))],
            group: None,
            agg_values: None,
            schema: PlanRow::empty_schema(),
        };
        assert!(!compiled.eval_bool(&miss).unwrap());

        let not_in = first_projection_expr("SELECT SUBSTRING(b, 1, 1) NOT IN ('a', NULL)");
        let compiled_not_in = CompiledPlanPredicate::compile_once(&not_in, &row);
        assert_eq!(
            compiled_not_in.eval_bool(&row).unwrap(),
            truthy(&eval_plan_expr_legacy(&not_in, &row).unwrap())
        );
    }

    #[test]
    fn compiled_plan_predicate_handles_q47_numeric_case_filter() {
        let row = PlanRow::from_fields(vec![
            PlanField::new(
                "v2",
                None,
                "avg_monthly_sales",
                ColumnType::Decimal {
                    precision: 20,
                    scale: 2,
                },
                Value::DecimalCompact(10_000, 2),
            ),
            PlanField::new(
                "v2",
                None,
                "sum_sales",
                ColumnType::Decimal {
                    precision: 20,
                    scale: 2,
                },
                Value::DecimalCompact(12_000, 2),
            ),
        ]);
        let expr = first_projection_expr(
            "SELECT avg_monthly_sales > 0 AND \
             CASE WHEN avg_monthly_sales > 0 \
                  THEN ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales \
                  ELSE NULL END > 0.1",
        );
        let compiled = CompiledPlanPredicate::compile_once(&expr, &row);
        assert!(
            matches!(compiled.compiled, CompiledPlanExpr::Program(_)),
            "Q47 numeric CASE predicate should stay out of the per-row legacy evaluator"
        );
        assert_eq!(
            compiled.eval_bool(&row).unwrap(),
            truthy(&eval_plan_expr_legacy(&expr, &row).unwrap())
        );
    }

    #[test]
    fn borrowed_simple_predicate_matches_legacy_three_valued_logic() {
        let expr = first_projection_expr("SELECT a = 12 AND b = 'beta'");
        for row in [
            PlanRow::from_fields(vec![
                field("a", Value::Int(12)),
                field("b", Value::String("beta".into())),
            ]),
            PlanRow::from_fields(vec![field("a", Value::Int(7)), field("b", Value::Null)]),
            PlanRow::from_fields(vec![field("a", Value::Int(12)), field("b", Value::Null)]),
        ] {
            let compiled = CompiledPlanPredicate::compile_once(&expr, &row);
            assert_eq!(
                compiled.eval_bool(&row).unwrap(),
                truthy(&eval_plan_expr_legacy(&expr, &row).unwrap())
            );
        }
    }

    #[test]
    fn order_key_evaluation_error_is_not_coerced_to_null() {
        let Statement::Select(select) =
            parse_statement("SELECT j FROM t ORDER BY JSON_EXTRACT(j, '$.x')").unwrap()
        else {
            panic!("expected select");
        };
        let mut rows = vec![
            PlanRow::from_fields(vec![field("j", Value::String("not-json".into()))]),
            PlanRow::from_fields(vec![field("j", Value::String("{\"x\":1}".into()))]),
        ];
        let err = sort_plan_rows_by_order(&mut rows, &select.order_by).unwrap_err();
        assert_ne!(err.code(), 0);
    }

    #[test]
    fn aggregate_value_binary_logic_uses_three_valued_semantics() {
        assert_eq!(
            eval_binary_values(Value::Int(1), BinaryOp::And, Value::Null).unwrap(),
            Value::Null
        );
        assert_eq!(
            eval_binary_values(Value::Int(0), BinaryOp::And, Value::Null).unwrap(),
            Value::Int(0)
        );
        assert_eq!(
            eval_binary_values(Value::Null, BinaryOp::Or, Value::Int(1)).unwrap(),
            Value::Int(1)
        );
    }

    #[test]
    fn complex_order_key_is_prepared_once_per_input_row() {
        let Statement::Select(select) =
            parse_statement("SELECT j FROM t ORDER BY LENGTH(j) * 17 + 3").unwrap()
        else {
            panic!("expected select");
        };
        let mut rows = (0..64)
            .map(|index| {
                PlanRow::from_fields(vec![field(
                    "j",
                    Value::String("x".repeat((index % 11) + 1)),
                )])
            })
            .collect::<Vec<_>>();
        ORDER_BY_KEY_ROWS_PREPARED.store(0, std::sync::atomic::Ordering::Relaxed);
        sort_plan_rows_by_order(&mut rows, &select.order_by).unwrap();
        assert_eq!(
            ORDER_BY_KEY_ROWS_PREPARED.load(std::sync::atomic::Ordering::Relaxed),
            rows.len() as u64
        );
    }
}
