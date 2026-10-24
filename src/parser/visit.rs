//! Shared mutable traversal for mydb's parser AST.
//!
//! Traversal follows AST structure. Parameter identities are assigned before
//! conversion, so semantic reordering does not change their source bindings.
//! Shared traversal keeps nested query coverage consistent across consumers.

use super::*;

mod map;
pub(crate) use map::try_map_value_expr;

#[cfg(test)]
mod sqlproc_audit_tests {
    use super::*;
    #[test]
    fn sqlproc_sus_temporal_preflight_visits_json_table_and_values() {
        for sql in [
            "SELECT * FROM JSON_TABLE((SELECT doc FROM t FOR SYSTEM_TIME AS OF '2026-01-01'), '$' COLUMNS(x INT PATH '$.x')) jt",
            "SELECT * FROM (VALUES ROW((SELECT a FROM t FOR SYSTEM_TIME AS OF '2026-01-01'))) v",
        ] {
            let statement=parse_statement(sql).unwrap();
            assert_eq!(collect_system_time_bindings(&statement).len(),1,"{sql}");
        }
    }
}

/// Canonicalize only identifier-bearing SELECT fields for the compiled-plan
/// cache's collision guard. Literal payloads (including case-sensitive string
/// and JSON-path values) are deliberately left byte-exact. This is stricter
/// than the hash routing key and lets presentation variants such as `A.name`
/// and `a.name` share one semantic plan without accepting a real fingerprint
/// collision.
pub(crate) fn canonicalize_select_for_plan_cache(select: &mut Select) {
    visit_select_mut(&mut CanonicalizeIdentifiers, select);
}

/// Expression counterpart used by semantic equality checks; literal bytes
/// remain exact, including inside subqueries and window definitions.
pub(crate) fn canonicalize_expr_for_plan_cache(expr: &mut Expr) {
    visit_expr_mut(&mut CanonicalizeIdentifiers, expr);
}

struct CanonicalizeIdentifiers;

fn lower(value: &mut String) {
    value.make_ascii_lowercase();
}

fn table_ref(table: &mut TableRef) {
    lower(&mut table.name);
    if let Some(alias) = &mut table.alias {
        lower(alias);
    }
    for hint in &mut table.index_hints {
        for index in &mut hint.index_names {
            lower(index);
        }
    }
    for partition in &mut table.partitions {
        lower(partition);
    }
    if let Some(series) = &mut table.generate_series {
        for alias in &mut series.column_aliases {
            lower(alias);
        }
    }
}

impl AstVisitorMut for CanonicalizeIdentifiers {
    fn enter_select(&mut self, select: &mut Select) {
        if let Some(table) = &mut select.table {
            lower(table);
        }
        for item in &mut select.projection {
            match item {
                SelectItem::QualifiedWildcard(qualifier) => lower(qualifier),
                SelectItem::Expr {
                    alias: Some(alias), ..
                } => lower(alias),
                SelectItem::Wildcard | SelectItem::Expr { alias: None, .. } => {}
            }
        }
        for table in &mut select.from {
            table_ref(table);
        }
        for join in &mut select.joins {
            table_ref(&mut join.table);
            for column in &mut join.using_columns {
                lower(column);
            }
        }
        for window in &mut select.named_windows {
            lower(&mut window.name);
            if let Some(inherits) = &mut window.inherits {
                lower(inherits);
            }
        }
        for hint in &mut select.query_hints {
            match hint {
                QueryHint::UseIndex { table, indexes } => {
                    lower(table);
                    for index in indexes {
                        lower(index);
                    }
                }
                QueryHint::NoIndex { table, index } => {
                    lower(table);
                    lower(index);
                }
                QueryHint::IndexMerge { table, indexes }
                | QueryHint::NoIndexMerge { table, indexes } => {
                    lower(table);
                    for index in indexes {
                        lower(index);
                    }
                }
                QueryHint::FullScan { table }
                | QueryHint::ZoneMap { table }
                | QueryHint::NoZoneMap { table } => lower(table),
                QueryHint::HashJoin { tables }
                | QueryHint::NoHashJoin { tables }
                | QueryHint::NlJoin { tables }
                | QueryHint::IndexNlj { tables } => {
                    for table in tables {
                        lower(table);
                    }
                }
                QueryHint::IndexFfs { table, index } => {
                    lower(table);
                    if let Some(index) = index {
                        lower(index);
                    }
                }
                QueryHint::Ordered
                | QueryHint::MaxExecutionTime { .. }
                | QueryHint::SetVar { .. }
                | QueryHint::Parallel { .. }
                | QueryHint::NoParallel
                | QueryHint::HnswEf { .. }
                | QueryHint::IvfNprobe { .. }
                | QueryHint::NoVectorIndex => {}
            }
        }
    }

    fn enter_expr(&mut self, expr: &mut Expr) {
        match expr {
            Expr::Identifier(name) | Expr::UserVariable(name) => lower(name),
            Expr::SystemVariable { name, .. } => lower(name),
            Expr::CompoundIdentifier(parts) | Expr::DefaultQualified(parts) => {
                for part in parts {
                    lower(part);
                }
            }
            Expr::Default(Some(column)) => lower(column),
            Expr::Function { name, .. } => lower(name),
            Expr::WindowFunction { name, spec, .. } => {
                let crate::parser::WindowSpec { inherits, .. } = spec.as_mut();

                lower(name);
                if let Some(inherits) = inherits {
                    lower(inherits);
                }
            }
            Expr::Wildcard
            | Expr::Literal(_)
            | Expr::BoundValue(_)
            | Expr::Placeholder(_)
            | Expr::Default(None)
            | Expr::Unary { .. }
            | Expr::Binary { .. }
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::InList { .. }
            | Expr::InSubquery { .. }
            | Expr::QuantifiedComparison { .. }
            | Expr::Subquery(_)
            | Expr::Exists { .. }
            | Expr::Between { .. }
            | Expr::Like { .. }
            | Expr::Case { .. }
            | Expr::GroupKeyRef { .. }
            | Expr::Grouping(_)
            | Expr::Tuple(_) => {}
        }
    }
}

/// Collect every temporal base-table binding in a query graph in source
/// order.  The traversal includes derived tables, scalar/EXISTS/IN
/// subqueries, set-operation branches, CTE definitions and EXPLAIN targets.
/// Returning owned nodes keeps execution preflight independent from later
/// planner rewrites.
pub(crate) fn collect_system_time_bindings(
    statement: &Statement,
) -> Vec<(TableRef, SystemTimeAsOf, bool)> {
    struct Collect {
        bindings: Vec<(TableRef, SystemTimeAsOf, bool)>,
    }
    impl AstVisitor for Collect {
        fn enter_select(&mut self, select: &Select) {
            let locking = select.for_update || select.lock_clause.is_some();
            self.bindings.extend(select.from.iter().filter_map(|table| {
                table
                    .system_time
                    .clone()
                    .map(|binding| (table.clone(), binding, locking))
            }));
            self.bindings.extend(select.joins.iter().filter_map(|join| {
                join.table
                    .system_time
                    .clone()
                    .map(|binding| (join.table.clone(), binding, locking))
            }));
        }
    }

    let mut collector = Collect {
        bindings: Vec::new(),
    };
    visit_statement(&mut collector, statement);
    collector.bindings
}

pub(crate) trait AstVisitorMut {
    fn enter_statement(&mut self, _statement: &mut Statement) {}
    fn exit_statement(&mut self, _statement: &mut Statement) {}
    fn enter_query(&mut self, _query: &mut QueryExpr) {}
    fn exit_query(&mut self, _query: &mut QueryExpr) {}
    fn walk_query_children(&self, _query: &QueryExpr) -> bool {
        true
    }
    fn enter_cte(&mut self, _cte: &mut CteDef) {}
    fn exit_cte(&mut self, _cte: &mut CteDef) {}
    fn enter_select(&mut self, _select: &mut Select) {}
    fn exit_select(&mut self, _select: &mut Select) {}
    /// A scope-aware consumer may handle this query itself and skip its children.
    fn walk_select_children(&self, _select: &Select) -> bool {
        true
    }
    /// GROUP BY/HAVING/ORDER BY may resolve SELECT output aliases. WHERE,
    /// projection expressions and named window definitions may not.
    fn enter_select_alias_scope(&mut self, _projection: &[SelectItem]) {}
    /// A direct ORDER BY identifier prioritizes the output alias.
    fn enter_select_order_scope(&mut self, projection: &[SelectItem]) {
        self.enter_select_alias_scope(projection);
    }
    fn exit_select_alias_scope(&mut self) {}
    fn enter_expr(&mut self, _expr: &mut Expr) {}
    fn exit_expr(&mut self, _expr: &mut Expr) {}
    /// A semantic inspector may exclude children without rewriting the AST.
    fn walk_expr_children(&self, _expr: &Expr) -> bool {
        true
    }
    /// Stop before unvisited expressions while balancing existing exit hooks.
    fn continue_expr_walk(&self) -> bool {
        true
    }
    fn enter_limit(&mut self, _limit: &mut LimitValue) {}
    /// ORDER BY's direct alias has different name precedence from expressions.
    fn walk_order_expr(&self, _order: &OrderBy) -> bool {
        true
    }
    fn enter_window_frame(&mut self, _frame: &mut WindowFrame) {}
    fn walk_window_frame_children(&self, _frame: &WindowFrame) -> bool {
        true
    }
    /// Structural consumers may visit all CASE conditions before results.
    fn case_results_interleaved(&self) -> bool {
        true
    }
}
mod walk;
walk::define_walk!(mutable, AstVisitorMut, [mut], iter_mut, from_mut);
walk::define_walk!(immutable, AstVisitor, [], iter, from_ref);
pub(crate) use immutable::walk_expr as visit_expr;
pub(crate) use immutable::walk_query as visit_query;
pub(crate) use immutable::walk_routine_statements as visit_routine_statements;
pub(crate) use immutable::walk_select as visit_select;
pub(crate) use immutable::walk_set_operation as visit_set_operation;
pub(crate) use immutable::walk_statement as visit_statement;
pub(crate) use mutable::walk_expr as visit_expr_mut;
pub(crate) use mutable::walk_query as visit_query_mut;
pub(crate) use mutable::walk_select as visit_select_mut;
pub(crate) use mutable::walk_statement as visit_statement_mut;

pub(crate) trait AstVisitor {
    fn enter_statement(&mut self, _statement: &Statement) {}
    fn exit_statement(&mut self, _statement: &Statement) {}
    fn enter_query(&mut self, _query: &QueryExpr) {}
    fn exit_query(&mut self, _query: &QueryExpr) {}
    fn walk_query_children(&self, _query: &QueryExpr) -> bool {
        true
    }
    fn enter_cte(&mut self, _cte: &CteDef) {}
    fn exit_cte(&mut self, _cte: &CteDef) {}
    fn enter_select(&mut self, _select: &Select) {}
    fn exit_select(&mut self, _select: &Select) {}
    /// A scope-aware consumer may handle this query itself and skip its children.
    fn walk_select_children(&self, _select: &Select) -> bool {
        true
    }
    /// GROUP BY/HAVING/ORDER BY may resolve SELECT output aliases. WHERE,
    /// projection expressions and named window definitions may not.
    fn enter_select_alias_scope(&mut self, _projection: &[SelectItem]) {}
    /// A direct ORDER BY identifier prioritizes the output alias.
    fn enter_select_order_scope(&mut self, projection: &[SelectItem]) {
        self.enter_select_alias_scope(projection);
    }
    fn exit_select_alias_scope(&mut self) {}
    fn enter_expr(&mut self, _expr: &Expr) {}
    fn exit_expr(&mut self, _expr: &Expr) {}
    fn walk_expr_children(&self, _expr: &Expr) -> bool {
        true
    }
    /// Stop before unvisited expressions while balancing existing exit hooks.
    fn continue_expr_walk(&self) -> bool {
        true
    }
    fn enter_limit(&mut self, _limit: &LimitValue) {}
    /// ORDER BY's direct alias has different name precedence from expressions.
    fn walk_order_expr(&self, _order: &OrderBy) -> bool {
        true
    }
    fn enter_window_frame(&mut self, _frame: &WindowFrame) {}
    fn walk_window_frame_children(&self, _frame: &WindowFrame) -> bool {
        true
    }
    /// Structural consumers may visit all CASE conditions before results.
    fn case_results_interleaved(&self) -> bool {
        true
    }
}

/// Inspect scalar expression structure while leaving nested query scopes to
/// the caller. Returning false prunes the current expression's children.
/// Window frame expressions are excluded: this is the value-expression walk
/// used by planner collectors, not the full source/parameter traversal.
pub(crate) fn inspect_value_expr(
    expr: &Expr,
    inspect: impl FnMut(&Expr) -> bool,
    query: impl FnMut(&Select),
) {
    struct Inspect<F, Q> {
        inspect: F,
        query: Q,
        descend: bool,
    }
    impl<F: FnMut(&Expr) -> bool, Q: FnMut(&Select)> AstVisitor for Inspect<F, Q> {
        fn enter_expr(&mut self, expr: &Expr) {
            self.descend = (self.inspect)(expr);
        }
        fn walk_expr_children(&self, _: &Expr) -> bool {
            self.descend
        }
        fn enter_select(&mut self, select: &Select) {
            (self.query)(select);
        }
        fn walk_select_children(&self, _: &Select) -> bool {
            false
        }
        fn walk_window_frame_children(&self, _: &WindowFrame) -> bool {
            false
        }
        fn case_results_interleaved(&self) -> bool {
            false
        }
    }
    visit_expr(
        &mut Inspect {
            inspect,
            query,
            descend: true,
        },
        expr,
    );
}

/// Rewrite the current value scope in place. A replacement may return false
/// to remain opaque (for example an expanded alias must not expand itself).
/// Nested SELECTs and window frame bounds retain their own binding scopes.
pub(crate) fn rewrite_value_expr(expr: &mut Expr, rewrite: impl FnMut(&mut Expr) -> bool) {
    let mut rewrite = rewrite;
    let result: Result<(), std::convert::Infallible> =
        try_rewrite_value_expr(expr, |node| Ok(rewrite(node)));
    match result {
        Ok(()) => {}
        Err(never) => match never {},
    }
}

/// Rewrite a value scope, stopping at the first failure. A replaced subtree
/// may remain opaque; nested query scopes and frame bounds are not traversed.
pub(crate) fn try_rewrite_value_expr<E>(
    expr: &mut Expr,
    rewrite: impl FnMut(&mut Expr) -> Result<bool, E>,
) -> Result<(), E> {
    struct Rewrite<F, E> {
        rewrite: F,
        descend: bool,
        result: Result<(), E>,
    }
    impl<F: FnMut(&mut Expr) -> Result<bool, E>, E> AstVisitorMut for Rewrite<F, E> {
        fn enter_expr(&mut self, expr: &mut Expr) {
            match (self.rewrite)(expr) {
                Ok(descend) => self.descend = descend,
                Err(error) => {
                    self.result = Err(error);
                    self.descend = false;
                }
            }
        }
        fn continue_expr_walk(&self) -> bool {
            self.result.is_ok()
        }
        fn walk_expr_children(&self, _: &Expr) -> bool {
            self.descend
        }
        fn walk_select_children(&self, _: &Select) -> bool {
            false
        }
        fn walk_window_frame_children(&self, _: &WindowFrame) -> bool {
            false
        }
        fn case_results_interleaved(&self) -> bool {
            false
        }
    }
    let mut visitor = Rewrite {
        rewrite,
        descend: true,
        result: Ok(()),
    };
    visit_expr_mut(&mut visitor, expr);
    visitor.result
}

/// Full-source and value-scope inspections share first-error state. The
/// compile-time scope policy retains query/frame boundaries and CASE order.
struct FallibleExprInspector<F, E, const VALUE_SCOPE: bool> {
    inspect: F,
    result: Result<(), E>,
    descend: bool,
}

impl<F: FnMut(&Expr) -> Result<bool, E>, E, const VALUE_SCOPE: bool> AstVisitor
    for FallibleExprInspector<F, E, VALUE_SCOPE>
{
    fn enter_expr(&mut self, expr: &Expr) {
        // walk_expr has already checked that no earlier expression failed.
        match (self.inspect)(expr) {
            Ok(descend) => self.descend = descend,
            Err(error) => {
                self.result = Err(error);
                self.descend = false;
            }
        }
    }
    fn continue_expr_walk(&self) -> bool {
        self.result.is_ok()
    }
    fn walk_expr_children(&self, _: &Expr) -> bool {
        self.descend
    }
    fn walk_select_children(&self, _: &Select) -> bool {
        !VALUE_SCOPE
    }
    fn walk_window_frame_children(&self, _: &WindowFrame) -> bool {
        !VALUE_SCOPE
    }
    fn case_results_interleaved(&self) -> bool {
        !VALUE_SCOPE
    }
}

#[inline]
fn try_inspect_expr_scope<E, const VALUE_SCOPE: bool>(
    expr: &Expr,
    mut inspect: impl FnMut(&Expr) -> Result<bool, E>,
) -> Result<(), E> {
    // Most bind-time probes inspect a single column or constant. Leaves have
    // no scope transitions or child callbacks, so preserve the same callback
    // contract without building and dispatching a recursive visitor.
    if matches!(
        expr,
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
            | Expr::Grouping(_)
    ) {
        return inspect(expr).map(|_| ());
    }
    let mut visitor = FallibleExprInspector::<_, _, VALUE_SCOPE> {
        inspect,
        result: Ok(()),
        descend: true,
    };
    visit_expr(&mut visitor, expr);
    visitor.result
}

/// Inspect an expression in structural order. `false` prunes a subtree;
/// the first error stops callbacks and descent without allocating a node list.
pub(crate) fn try_for_each_expr<E>(
    expr: &Expr,
    inspect: impl FnMut(&Expr) -> Result<bool, E>,
) -> Result<(), E> {
    try_inspect_expr_scope::<_, false>(expr, inspect)
}

#[derive(Default)]
struct FunctionCollector(std::collections::BTreeSet<String>);
impl AstVisitor for FunctionCollector {
    fn enter_expr(&mut self, expr: &Expr) {
        if let Expr::Function { name, .. } = expr {
            self.0.insert(name.clone());
        }
    }
}

pub(crate) fn collect_statement_function_names(statement: &Statement) -> Vec<String> {
    let mut collector = FunctionCollector::default();
    visit_statement(&mut collector, statement);
    collector.0.into_iter().collect()
}

pub(crate) fn collect_select_function_names(select: &Select) -> Vec<String> {
    let mut collector = FunctionCollector::default();
    visit_select(&mut collector, select);
    collector.0.into_iter().collect()
}

pub(crate) fn collect_unqualified_identifier_names(statement: &Statement) -> Vec<String> {
    struct IdentifierCollector(std::collections::BTreeSet<String>);
    impl AstVisitor for IdentifierCollector {
        fn enter_expr(&mut self, expr: &Expr) {
            if let Expr::Identifier(name) = expr {
                self.0.insert(name.trim_matches('`').to_ascii_lowercase());
            }
        }
    }
    let mut collector = IdentifierCollector(Default::default());
    visit_statement(&mut collector, statement);
    collector.0.into_iter().collect()
}

pub(crate) fn bind_unqualified_identifiers_to_placeholders(
    statement: &mut Statement,
    slots: &std::collections::BTreeMap<String, usize>,
) {
    struct SlotBinder<'a>(&'a std::collections::BTreeMap<String, usize>);
    impl AstVisitorMut for SlotBinder<'_> {
        fn enter_select(&mut self, select: &mut Select) {
            for item in &mut select.projection {
                if let SelectItem::Expr { expr, alias } = item {
                    if alias.is_none() {
                        if let Expr::Identifier(name) = expr {
                            if self
                                .0
                                .contains_key(&name.trim_matches('`').to_ascii_lowercase())
                            {
                                *alias = Some(name.clone());
                            }
                        }
                    }
                }
            }
        }

        fn enter_expr(&mut self, expr: &mut Expr) {
            let Expr::Identifier(name) = expr else {
                return;
            };
            if let Some(slot) = self.0.get(&name.trim_matches('`').to_ascii_lowercase()) {
                *expr = Expr::Placeholder(*slot);
            }
        }
    }
    visit_statement_mut(&mut SlotBinder(slots), statement);
}

pub(crate) fn collect_routine_expression_function_names(statements: &[RoutineStmt]) -> Vec<String> {
    let mut collector = FunctionCollector::default();
    visit_routine_statements(&mut collector, statements);
    collector.0.into_iter().collect()
}

/// Assign stable query-local identifiers in source traversal order. Runtime
/// snapshot SCNs deliberately never enter the reusable AST or plan-cache key.
pub(crate) fn assign_system_time_bindings(statement: &mut Statement) {
    struct BindingAssigner(u32);
    impl AstVisitorMut for BindingAssigner {
        fn enter_select(&mut self, select: &mut Select) {
            for table in &mut select.from {
                if let Some(system_time) = &mut table.system_time {
                    system_time.binding_id = self.0;
                    self.0 = self.0.saturating_add(1);
                }
            }
            for join in &mut select.joins {
                if let Some(system_time) = &mut join.table.system_time {
                    system_time.binding_id = self.0;
                    self.0 = self.0.saturating_add(1);
                }
            }
        }
    }
    visit_statement_mut(&mut BindingAssigner(1), statement);
}

pub(crate) fn system_time_expr_is_statement_constant(expr: &Expr) -> bool {
    struct Validator(bool);
    impl AstVisitor for Validator {
        fn enter_expr(&mut self, expr: &Expr) {
            if !self.0 {
                return;
            }
            self.0 = match expr {
                Expr::Literal(_) | Expr::BoundValue(_) | Expr::Placeholder(_) => true,
                Expr::Unary { .. } | Expr::Binary { .. } => true,
                Expr::Function { name, .. } => matches!(
                    name.to_ascii_uppercase().as_str(),
                    "NOW"
                        | "CURRENT_TIMESTAMP"
                        | "UTC_TIMESTAMP"
                        | "LOCALTIME"
                        | "LOCALTIMESTAMP"
                        | "DATE_ADD"
                        | "DATE_SUB"
                        | "ADDDATE"
                        | "SUBDATE"
                        | "TIMESTAMPADD"
                        | "ADDTIME"
                        | "SUBTIME"
                        | "TIMESTAMP"
                        | "CAST"
                        | "__TIMESTAMP_LITERAL"
                        | "__INTERVAL"
                ),
                _ => false,
            };
        }
    }
    let mut validator = Validator(true);
    visit_expr(&mut validator, expr);
    validator.0
}

/// Return the first free parameter identity across every query child, including
/// CTE/set-operation branches, derived joins, table expressions and LIMIT.
pub(crate) fn next_select_parameter_index(select: &Select) -> usize {
    #[derive(Default)]
    struct Extent(usize);
    impl Extent {
        fn include(&mut self, index: usize) {
            self.0 = self.0.max(index + 1);
        }
    }
    impl AstVisitor for Extent {
        fn enter_expr(&mut self, expr: &Expr) {
            if let Expr::Placeholder(index) = expr {
                self.include(*index);
            }
        }
        fn enter_limit(&mut self, limit: &LimitValue) {
            if let LimitValue::Placeholder(index) | LimitValue::OffsetCommaPlaceholder(index) =
                limit
            {
                self.include(*index);
            }
        }
    }
    let mut extent = Extent::default();
    visit_select(&mut extent, select);
    extent.0
}

/// Rebase source-assigned parameter identities into a containing scope.
/// Repeated references keep the same identity even after semantic lowering.
pub(crate) fn number_statement_placeholders(statement: &mut Statement, next: &mut usize) {
    let mut visitor = ParameterRebaser::new(*next);
    visit_statement_mut(&mut visitor, statement);
    *next = visitor.next_index();
}

pub(crate) struct ParameterRebaser {
    base: usize,
    next: usize,
}
impl ParameterRebaser {
    pub(crate) fn new(base: usize) -> Self {
        Self { base, next: base }
    }
    pub(crate) fn next_index(&self) -> usize {
        self.next
    }
    fn rebase(&mut self, index: &mut usize) {
        *index += self.base;
        self.next = self.next.max(*index + 1);
    }
}
impl AstVisitorMut for ParameterRebaser {
    fn enter_expr(&mut self, expr: &mut Expr) {
        if let Expr::Placeholder(index) = expr {
            self.rebase(index);
        }
    }
    fn enter_limit(&mut self, limit: &mut LimitValue) {
        if let LimitValue::Placeholder(index) | LimitValue::OffsetCommaPlaceholder(index) = limit {
            self.rebase(index);
        }
    }
}

/// Fallible scalar inspection in structural order. Nested SELECT scopes and
/// window frame bounds belong to their caller; the first error stops callbacks.
pub(crate) fn try_inspect_value_expr<E>(
    expr: &Expr,
    inspect: impl FnMut(&Expr) -> Result<bool, E>,
) -> Result<(), E> {
    try_inspect_expr_scope::<_, true>(expr, inspect)
}

pub(crate) fn value_expr_contains_subquery(expr: &Expr) -> bool {
    if matches!(
        expr,
        Expr::Literal(_) | Expr::BoundValue(_) | Expr::Placeholder(_)
    ) {
        return false;
    }
    try_inspect_value_expr(expr, |node| {
        if matches!(
            node,
            Expr::Subquery(_)
                | Expr::InSubquery { .. }
                | Expr::Exists { .. }
                | Expr::QuantifiedComparison {
                    right: QuantifiedComparisonRight::Subquery(_),
                    ..
                }
        ) {
            Err(())
        } else {
            Ok(true)
        }
    })
    .is_err()
}

/// Whether any structurally reachable expression matches, including nested queries.
/// The first match stops descent and further predicate calls.
pub(crate) fn any_expr(expr: &Expr, mut predicate: impl FnMut(&Expr) -> bool) -> bool {
    try_for_each_expr(
        expr,
        |node| if predicate(node) { Err(()) } else { Ok(true) },
    )
    .is_err()
}

/// Subqueries in the expressions of this SELECT body. FROM-derived sources have
/// their own binding scopes and are inspected separately by the caller.
pub(crate) fn select_body_contains_subquery(select: &Select) -> bool {
    let contains = |expr: &Expr| {
        any_expr(expr, |expr| {
            matches!(
                expr,
                Expr::Subquery(_)
                    | Expr::Exists { .. }
                    | Expr::InSubquery { .. }
                    | Expr::QuantifiedComparison {
                        right: QuantifiedComparisonRight::Subquery(_),
                        ..
                    }
            )
        })
    };
    select
        .projection
        .iter()
        .any(|item| matches!(item, SelectItem::Expr { expr, .. } if contains(expr)))
        || select.selection.as_ref().is_some_and(contains)
        || select.having.as_ref().is_some_and(contains)
        || select.group_by.iter().any(contains)
        || select
            .joins
            .iter()
            .any(|join| join.condition.as_ref().is_some_and(contains))
        || select.order_by.iter().any(|order| contains(&order.expr))
}

#[cfg(test)]
mod scalar_inspection_tests {
    use super::*;

    fn projection_expr(sql: &str) -> Expr {
        let Statement::Select(select) = parse_statement(sql).unwrap() else {
            panic!("expected SELECT");
        };
        let SelectItem::Expr { expr, .. } = &select.projection[0] else {
            panic!("expected expression");
        };
        expr.clone()
    }

    #[test]
    fn scalar_inspection_keeps_case_order_and_first_failure() {
        let expr =
            projection_expr("SELECT CASE base WHEN c1 THEN r1 WHEN c2 THEN r2 ELSE other END");
        let mut names = Vec::new();
        try_inspect_value_expr::<()>(&expr, |node| {
            if let Expr::Identifier(name) = node {
                names.push(name.clone());
            }
            Ok(true)
        })
        .unwrap();
        assert_eq!(names, ["base", "c1", "c2", "r1", "r2", "other"]);
        names.clear();
        let result = try_inspect_value_expr(&expr, |node| {
            if let Expr::Identifier(name) = node {
                names.push(name.clone());
                if name == "c2" {
                    return Err(123);
                }
            }
            Ok(true)
        });
        assert_eq!(result, Err(123));
        assert_eq!(names, ["base", "c1", "c2"]);
    }

    #[test]
    fn scalar_inspection_leaves_nested_select_to_its_scope() {
        let expr =
            projection_expr("SELECT outer_col IN (SELECT inner_col FROM t WHERE hidden_col=1)");
        let mut names = Vec::new();
        try_inspect_value_expr::<()>(&expr, |node| {
            if let Expr::Identifier(name) = node {
                names.push(name.clone());
            }
            Ok(true)
        })
        .unwrap();
        assert_eq!(names, ["outer_col"]);
        assert!(value_expr_contains_subquery(&expr));
    }

    #[test]
    fn immediate_operands_preserve_case_order_and_query_ownership() {
        let mut expr =
            projection_expr("SELECT CASE base WHEN c1 THEN r1 WHEN c2 THEN r2 ELSE other END");
        let mut names = Vec::new();
        for_each_expr_child(&expr, |child| {
            if let Expr::Identifier(name) = child {
                names.push(name.clone());
            }
        });
        assert_eq!(names, ["base", "c1", "c2", "r1", "r2", "other"]);
        for_each_expr_child_mut(&mut expr, |child| {
            if let Expr::Identifier(name) = child {
                name.push_str("_bound");
            }
        });
        names.clear();
        for_each_expr_child(&expr, |child| {
            if let Expr::Identifier(name) = child {
                names.push(name.clone());
            }
        });
        assert_eq!(
            names,
            [
                "base_bound",
                "c1_bound",
                "c2_bound",
                "r1_bound",
                "r2_bound",
                "other_bound"
            ]
        );

        let mut nested =
            projection_expr("SELECT outer_col IN (SELECT inner_col FROM t WHERE hidden_col=1)");
        for_each_expr_child_mut(&mut nested, |child| {
            assert!(matches!(child, Expr::Identifier(name) if name == "outer_col"));
            *child = Expr::Literal(Literal::Int(7));
        });
        let Expr::InSubquery { expr, subquery, .. } = nested else {
            panic!("IN subquery");
        };
        assert!(matches!(*expr, Expr::Literal(Literal::Int(7))));
        assert!(
            matches!(&subquery.projection[0], SelectItem::Expr { expr: Expr::Identifier(name), .. } if name == "inner_col")
        );
    }

    #[test]
    fn validation_operands_visit_frame_bounds_last_and_stop_on_error() {
        let expr = projection_expr("SELECT SUM(v) OVER (PARTITION BY k ORDER BY id ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) FROM t");
        let mut names = Vec::new();
        try_for_each_expr_child_with_frame::<()>(&expr, |child| {
            match child {
                Expr::Identifier(name) => names.push(name.clone()),
                Expr::Literal(Literal::Int(n)) => names.push(n.to_string()),
                other => panic!("unexpected child: {other:?}"),
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(names, ["v", "k", "id", "2", "1"]);
        let mut visited = 0;
        let result = try_for_each_expr_child_with_frame(&expr, |child| {
            visited += 1;
            if matches!(child, Expr::Literal(_)) {
                Err("frame")
            } else {
                Ok(())
            }
        });
        assert_eq!(result, Err("frame"));
        assert_eq!(visited, 4);
    }

    #[test]
    fn scalar_inspection_visits_window_values_without_frame_bounds() {
        let expr = projection_expr("SELECT SUM(v) OVER (PARTITION BY k ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t");
        let mut names = Vec::new();
        try_inspect_value_expr(&expr, |node| {
            if matches!(node, Expr::Literal(_)) {
                return Err("frame entered");
            }
            if let Expr::Identifier(name) = node {
                names.push(name.clone());
            }
            Ok(true)
        })
        .unwrap();
        assert_eq!(names, ["v", "k", "id"]);
    }
}

#[cfg(test)]
mod short_circuit_walk_tests {
    use super::*;

    #[test]
    fn wide_expression_stops_in_bounded_work_and_balances_parent_exit() {
        struct StopAtLeaf {
            entered: usize,
            exited: usize,
            stopped: bool,
            probes: std::cell::Cell<usize>,
        }
        impl AstVisitor for StopAtLeaf {
            fn enter_expr(&mut self, expr: &Expr) {
                self.entered += 1;
                if matches!(expr, Expr::Identifier(name) if name == "stop") {
                    self.stopped = true;
                }
            }
            fn exit_expr(&mut self, _: &Expr) {
                self.exited += 1;
            }
            fn continue_expr_walk(&self) -> bool {
                self.probes.set(self.probes.get() + 1);
                !self.stopped
            }
        }
        let mut args = vec![Expr::Identifier("stop".into())];
        args.extend((0..10_000).map(|_| Expr::Identifier("unvisited".into())));
        let expression = Expr::Tuple(args);
        let mut visitor = StopAtLeaf {
            entered: 0,
            exited: 0,
            stopped: false,
            probes: std::cell::Cell::new(0),
        };
        visit_expr(&mut visitor, &expression);
        assert_eq!(visitor.entered, 2);
        assert_eq!(visitor.exited, visitor.entered, "parent cleanup must run");
        assert!(
            visitor.probes.get() <= 8,
            "unvisited children must not be scanned"
        );
    }
}

/// Search an expression while leaving nested query ownership to the caller.
/// `descend` explicitly selects traversable node kinds; this preserves the
/// scope of semantic checks that intentionally ignore windows or tuples.
pub(crate) fn any_expr_matching(
    expr: &Expr,
    matches: impl FnMut(&Expr) -> bool,
    descend: impl Fn(&Expr) -> bool,
) -> bool {
    struct Search<P, D> {
        matches: P,
        descend: D,
        found: bool,
    }
    impl<P: FnMut(&Expr) -> bool, D: Fn(&Expr) -> bool> AstVisitor for Search<P, D> {
        fn enter_expr(&mut self, expr: &Expr) {
            self.found |= (self.matches)(expr);
        }
        fn continue_expr_walk(&self) -> bool {
            !self.found
        }
        fn walk_expr_children(&self, expr: &Expr) -> bool {
            !self.found && (self.descend)(expr)
        }
        fn walk_select_children(&self, _: &Select) -> bool {
            false
        }
        fn walk_query_children(&self, _: &QueryExpr) -> bool {
            false
        }
        fn walk_window_frame_children(&self, _: &WindowFrame) -> bool {
            false
        }
        fn case_results_interleaved(&self) -> bool {
            false
        }
    }
    let mut search = Search {
        matches,
        descend,
        found: false,
    };
    visit_expr(&mut search, expr);
    search.found
}

/// Ordinary scalar expression edges, excluding tuple/group/window and
/// subquery boundaries. Consumers that own those scopes opt in explicitly.
pub(crate) fn scalar_expr_children(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Unary { .. }
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::Binary { .. }
            | Expr::Between { .. }
            | Expr::Like { .. }
            | Expr::InList { .. }
            | Expr::Function { .. }
            | Expr::Case { .. }
    )
}

/// Scalar edges plus operands of query predicates, without entering SELECTs.
/// Window arguments/definitions remain an explicit choice of each consumer.
pub(crate) fn scalar_query_operand_children(expr: &Expr) -> bool {
    scalar_expr_children(expr)
        || matches!(
            expr,
            Expr::InSubquery { .. } | Expr::QuantifiedComparison { .. }
        )
}

/// Aggregate calls owned by this value-expression block. A nested query and
/// a window definition own their aggregates and are inspected separately.
pub(crate) fn has_block_aggregate(expr: &Expr) -> bool {
    any_expr_matching(
        expr,
        |node| matches!(node, Expr::Function { name, .. } if super::is_aggregate_function(name)),
        |node| {
            scalar_query_operand_children(node)
                || matches!(node, Expr::Tuple(_) | Expr::GroupKeyRef { .. })
        },
    )
}

// Use the structural walker for immediate operands too. In particular CASE
// order, window value operands and query boundaries cannot drift between the
// read-only and rewriting consumers.
macro_rules! define_immediate_operands {
    ($name:ident, $try_name:ident, $visitor:ident, $walk:ident, [$($mutable:tt)*]) => {
        #[inline]
        pub(crate) fn $name(expr: & $($mutable)* Expr, mut f: impl FnMut(& $($mutable)* Expr)) {
            let result: Result<(), std::convert::Infallible> = $try_name(expr, |child| {
                f(child);
                Ok(())
            });
            match result { Ok(()) => {}, Err(never) => match never {} }
        }

        /// Visit direct operands in structural order, stopping at the first error.
        /// Query bodies and window frames stay owned by their semantic consumers.
        #[inline]
        pub(crate) fn $try_name<E>(expr: & $($mutable)* Expr, f: impl FnMut(& $($mutable)* Expr) -> Result<(), E>) -> Result<(), E> {
            struct Operands<F, E> { f: F, depth: usize, result: Result<(), E> }
            impl<E, F: FnMut(& $($mutable)* Expr) -> Result<(), E>> $visitor for Operands<F, E> {
                fn enter_expr(&mut self, expr: & $($mutable)* Expr) {
                    if self.depth == 1 { self.result = (self.f)(expr); }
                    self.depth += 1;
                }
                fn exit_expr(&mut self, _: & $($mutable)* Expr) { self.depth -= 1; }
                fn continue_expr_walk(&self) -> bool { self.result.is_ok() }
                fn walk_expr_children(&self, expr: &Expr) -> bool {
                    self.depth == 1 && self.result.is_ok() && !matches!(expr, Expr::Grouping(_))
                }
                fn walk_select_children(&self, _: &Select) -> bool { false }
                fn walk_query_children(&self, _: &QueryExpr) -> bool { false }
                fn walk_window_frame_children(&self, _: &WindowFrame) -> bool { false }
                fn case_results_interleaved(&self) -> bool { false }
            }
            let mut visitor = Operands { f, depth: 0, result: Ok(()) };
            $walk(&mut visitor, expr);
            visitor.result
        }
    };
}
define_immediate_operands!(
    for_each_expr_child,
    try_for_each_expr_child,
    AstVisitor,
    visit_expr,
    []
);
define_immediate_operands!(for_each_expr_child_mut, try_for_each_expr_child_mut, AstVisitorMut, visit_expr_mut, [mut]);

/// Direct value operands followed by window-frame bounds. SELECT bodies remain
/// separate scopes; validation callers retain their own recursion/depth guard.
#[inline]
pub(crate) fn try_for_each_expr_child_with_frame<E>(
    expr: &Expr,
    mut f: impl FnMut(&Expr) -> Result<(), E>,
) -> Result<(), E> {
    try_for_each_expr_child(expr, &mut f)?;
    if let Expr::WindowFunction { spec, .. } = expr {
        if let Some(frame) = &spec.frame {
            for bound in [&frame.start, &frame.end] {
                if let WindowFrameBound::Preceding(child) | WindowFrameBound::Following(child) =
                    bound
                {
                    f(child)?;
                }
            }
        }
    }
    Ok(())
}

/// Window search with the same scalar scope used by EXPLAIN and derived pushdown.
pub(crate) fn has_window_expression(expr: &Expr) -> bool {
    any_expr_matching(
        expr,
        |node| matches!(node, Expr::WindowFunction { .. }),
        |node| {
            matches!(
                node,
                Expr::Unary { .. }
                    | Expr::IsNull(_)
                    | Expr::IsNotNull(_)
                    | Expr::Binary { .. }
                    | Expr::Function { .. }
                    | Expr::Case { .. }
            )
        },
    )
}

#[cfg(test)]
mod expression_search_tests {
    use super::*;

    #[test]
    fn scalar_search_prunes_owned_scopes_and_stops_on_match() {
        let expr = parse_expr_fragment("COUNT(x) + SUM(y)").unwrap();
        let mut functions = Vec::new();
        assert!(any_expr_matching(
            &expr,
            |node| {
                if let Expr::Function { name, .. } = node {
                    functions.push(name.to_ascii_uppercase());
                    return name.eq_ignore_ascii_case("count");
                }
                false
            },
            scalar_expr_children
        ));
        assert_eq!(functions, ["COUNT"]);
        let expr = parse_expr_fragment("(SELECT COUNT(x) FROM t)").unwrap();
        assert!(!any_expr_matching(
            &expr,
            |node| matches!(node, Expr::Function { name, .. } if name.eq_ignore_ascii_case("count")),
            scalar_expr_children
        ));
        assert!(any_expr_matching(
            &expr,
            |node| matches!(node, Expr::Subquery(_)),
            scalar_expr_children
        ));
    }
}
