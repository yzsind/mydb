//! View expansion, validation and DML binding.
use super::*;

pub(in crate::engine::executor) fn expand_views(
    db: &Arc<Database>,
    session: &SessionContext,
    statement: &mut Statement,
    depth: usize,
) -> Result<(), QueryError> {
    let mut trail = ViewTrail::new();
    expand_views_in_statement(db, session, statement, depth, &mut trail)
}

pub(in crate::engine::executor) fn expand_views_in_statement<'q>(
    db: &Arc<Database>,
    session: &SessionContext,
    statement: impl Into<crate::parser::QueryMut<'q>>,
    depth: usize,
    trail: &mut ViewTrail,
) -> Result<(), QueryError> {
    let statement = statement.into();
    if depth > MAX_VIEW_DEPTH {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "view expansion exceeded maximum nesting depth",
        ));
    }
    match statement {
        crate::parser::QueryMut::Select(select) => {
            expand_views_in_select(db, session, select, depth, trail)
        }
        crate::parser::QueryMut::Rrf(rrf) => {
            for branch in &mut rrf.branches {
                expand_views_in_statement(db, session, &mut branch.query, depth, trail)?;
            }
            Ok(())
        }
        crate::parser::QueryMut::SetOperation(set) => {
            expand_views_in_statement(db, session, &mut set.left, depth, trail)?;
            expand_views_in_statement(db, session, &mut set.right, depth, trail)
        }
        crate::parser::QueryMut::WithCte(mut with) => {
            let stubs = with
                .ctes
                .iter()
                .map(|cte| {
                    (
                        cte.name.to_ascii_lowercase(),
                        Arc::new(crate::engine::api::CteResult::materialized(
                            Vec::new(),
                            Vec::new(),
                        )),
                    )
                })
                .collect();
            crate::engine::executor::cte::with_cte_registry_extended(stubs, || {
                for cte in with.ctes.iter_mut() {
                    expand_views_in_statement(db, session, &mut cte.query, depth, trail)?;
                }
                expand_views_in_statement(db, session, &mut with.body, depth, trail)
            })
        }
        crate::parser::QueryMut::Other(
            statement @ (Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)),
        ) => {
            struct Queries<'a> {
                db: &'a Arc<Database>,
                session: &'a SessionContext,
                trail: &'a mut ViewTrail,
                depth: usize,
                nesting: usize,
                result: Result<(), QueryError>,
            }
            impl crate::parser::visit::AstVisitorMut for Queries<'_> {
                fn enter_select(&mut self, select: &mut Select) {
                    if self.nesting == 0 && self.result.is_ok() {
                        self.result = expand_views_in_select(
                            self.db,
                            self.session,
                            select,
                            self.depth,
                            self.trail,
                        );
                    }
                    self.nesting += 1;
                }
                fn exit_select(&mut self, _: &mut Select) {
                    self.nesting -= 1;
                }
            }
            let mut visitor = Queries {
                db,
                session,
                trail,
                depth,
                nesting: 0,
                result: Ok(()),
            };
            crate::parser::visit::visit_statement_mut(&mut visitor, statement);
            visitor.result
        }
        crate::parser::QueryMut::Other(Statement::Explain { target, .. }) => {
            expand_views_in_statement(db, session, target, depth, trail)
        }
        crate::parser::QueryMut::Other(Statement::SelectIntoVars { inner, .. })
        | crate::parser::QueryMut::Other(Statement::SelectIntoFile { inner, .. }) => {
            expand_views_in_statement(db, session, inner, depth, trail)
        }
        crate::parser::QueryMut::Other(Statement::CreateTableAsSelect { query, .. }) => {
            expand_views_in_statement(db, session, query, depth, trail)
        }
        _ => Ok(()),
    }
}

thread_local! {
    // Only the synchronous expansion walk uses this stack. A caller's
    // identity remains a runtime expression for INVOKER views, so cached
    // plans cannot capture the first caller's CURRENT_USER().
    static VIEW_CURRENT_USER: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
}

struct ViewCurrentUserScope(Option<String>);
impl ViewCurrentUserScope {
    fn enter(view: &ViewSchema) -> Self {
        let next = if view.security_type == RoutineSecuritySchema::Definer {
            Some(
                AccountKey::new(view.definer_user.clone(), view.definer_host.clone())
                    .current_user_value(),
            )
        } else {
            VIEW_CURRENT_USER.with(|current| current.borrow().clone())
        };
        Self(VIEW_CURRENT_USER.with(|current| current.replace(next)))
    }
}
impl Drop for ViewCurrentUserScope {
    fn drop(&mut self) {
        VIEW_CURRENT_USER.with(|current| {
            current.replace(self.0.take());
        });
    }
}

fn bind_view_current_user(query: &mut Select, account: &str) {
    struct Bind<'a>(&'a str);
    impl crate::parser::visit::AstVisitorMut for Bind<'_> {
        fn enter_expr(&mut self, expr: &mut Expr) {
            if matches!(expr, Expr::Function { name, args, .. } if name.eq_ignore_ascii_case("CURRENT_USER") && args.is_empty())
            {
                *expr = Expr::Literal(Literal::String(self.0.to_string()));
            }
        }
    }
    crate::parser::visit::visit_select_mut(&mut Bind(account), query);
}

fn expand_view_body(
    db: &Arc<Database>,
    session: &SessionContext,
    view: &ViewSchema,
    source: &str,
    depth: usize,
    trail: &mut ViewTrail,
) -> Result<Select, QueryError> {
    let mut expanded = stored_view_query(view)?;
    push_trail(trail, view, source)?;
    let _scope = ViewCurrentUserScope::enter(view);
    VIEW_CURRENT_USER.with(|current| {
        if let Some(account) = current.borrow().as_deref() {
            bind_view_current_user(&mut expanded, account);
        }
    });
    let mut context = session.clone();
    context.current_database = view.database.clone();
    expand_views_in_select(db, &context, &mut expanded, depth + 1, trail)?;
    compact_expanded_view_body(&mut expanded)?;
    trail.remove(&trail_key(view));
    Ok(expanded)
}

fn expand_views_in_select(
    db: &Arc<Database>,
    session: &SessionContext,
    stmt: &mut Select,
    depth: usize,
    trail: &mut ViewTrail,
) -> Result<(), QueryError> {
    if depth > MAX_VIEW_DEPTH {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "view expansion exceeded maximum nesting depth",
        ));
    }
    // Lock a simple updatable view using exactly the base-row DML mapping.
    // Returning an unlocked derived result would violate FOR UPDATE/SHARE.
    if stmt.for_update && stmt.derived.is_none() && stmt.from.len() == 1 && stmt.joins.is_empty() {
        let plan = match resolve_updatable_view(db, session, &stmt.from[0].name) {
            Ok(plan) => plan,
            // DML updatability is stricter than locking-read eligibility.
            // Let the generic derived-query path preserve base-row identity
            // or explicitly reject shapes it cannot safely lock.
            Err(err) if err.is(crate::error::catalog::ER_NON_UPDATABLE_TABLE) => None,
            Err(err) => return Err(err),
        };
        if let Some(mut plan) = plan {
            // Bind against the view before replacing its namespace with the
            // base table. In particular, correlated subqueries must not gain
            // access to base columns through outer-scope fallback.
            let mut visible = stmt.clone();
            visible.for_update = false;
            visible.lock_clause = None;
            expand_views_in_select(db, session, &mut visible, depth, trail)?;
            // User references were qualified before expansion; qualifying again
            // here would rebind persistent view bodies to temporary shadows.
            crate::planner::validate_select_column_references(db, &visible)?;
            let source_ref = stmt.from[0].clone();
            let mut projection = Vec::new();
            for item in std::mem::take(&mut stmt.projection) {
                match item {
                    SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => {
                        for (visible, base) in &plan.returning_columns {
                            projection.push(SelectItem::Expr {
                                expr: Expr::Identifier(base.clone()),
                                alias: Some(visible.clone()),
                            });
                        }
                    }
                    SelectItem::Expr {
                        mut expr,
                        mut alias,
                    } => {
                        if alias.is_none() {
                            alias = match &expr {
                                Expr::Identifier(name) => Some(name.clone()),
                                Expr::CompoundIdentifier(parts) => parts.last().cloned(),
                                _ => None,
                            };
                        }
                        rewrite_scoped_view_expr(
                            db,
                            session,
                            &mut expr,
                            &source_ref.name,
                            source_ref.alias.as_deref(),
                            &plan.column_map,
                        )?;
                        projection.push(SelectItem::Expr { expr, alias });
                    }
                }
            }
            stmt.projection = projection;
            if stmt.from[0].alias.is_none() {
                stmt.from[0].alias = Some(
                    crate::storage::split_table_name(&session.current_database, &stmt.from[0].name)
                        .1,
                );
            }
            if let Some(expr) = &mut stmt.selection {
                rewrite_scoped_view_expr(
                    db,
                    session,
                    expr,
                    &source_ref.name,
                    source_ref.alias.as_deref(),
                    &plan.column_map,
                )?;
            }
            // SELECT aliases are visible to GROUP BY/HAVING/ORDER BY, but
            // never make an unexposed base column visible in WHERE/projection.
            let mut output_map = plan.column_map.clone();
            for item in &stmt.projection {
                if let SelectItem::Expr {
                    expr,
                    alias: Some(alias),
                } = item
                {
                    let mapped = match expr {
                        Expr::Identifier(name) => name.clone(),
                        Expr::CompoundIdentifier(parts) => {
                            parts.last().cloned().unwrap_or_else(|| alias.clone())
                        }
                        _ => alias.clone(),
                    };
                    output_map
                        .entry(alias.to_ascii_lowercase())
                        .or_insert(mapped);
                }
            }
            for expr in &mut stmt.group_by {
                rewrite_expr_view_columns(expr, &output_map)?;
            }
            if let Some(expr) = &mut stmt.having {
                rewrite_expr_view_columns(expr, &output_map)?;
            }
            for item in &mut stmt.order_by {
                rewrite_expr_view_columns(&mut item.expr, &output_map)?;
            }
            qualify_view_predicate(
                &mut plan.view_predicate,
                stmt.from[0].alias.as_deref().unwrap(),
            );
            stmt.selection = and_where(stmt.selection.take(), plan.view_predicate);
            stmt.table = Some(plan.base_qualified.clone());
            stmt.from[0].name = plan.base_qualified;
        }
    }
    // Recurse into any explicit derived table (`FROM (SELECT ...) AS d`)
    // first so nested views inside it are handled before we look at the
    // primary FROM.
    if let Some(derived) = &mut stmt.derived {
        match derived {
            SubQuery::Select(s) => expand_views_in_select(db, session, s, depth, trail)?,
            inner @ (SubQuery::SetOp(_) | SubQuery::Rrf(_) | SubQuery::WithCte(_)) => {
                expand_views_in_statement(db, session, inner, depth, trail)?
            }
        }
    }

    // Primary FROM: replace each view-typed entry with `select.derived`.
    // The Select struct holds at most one `derived`; if more than one
    // primary FROM resolves to a view, we move the extras to `joins` as
    // implicit CROSS JOINs (mirroring how the parser already lowers
    // `FROM t1, t2, ...` for tables).
    if !stmt.from.is_empty() && stmt.derived.is_none() {
        let primary = stmt.from[0].clone();
        if let Some(view) = lookup_view_definition(db, session, &primary.name)? {
            if primary.system_time.is_some() {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "FOR SYSTEM_TIME AS OF cannot qualify a view; place it on base tables inside the view definition",
                ));
            }
            let alias = primary.alias.clone().unwrap_or_else(|| view.name.clone());
            let expanded = expand_view_body(db, session, &view, &primary.name, depth, trail)?;
            // Replace primary FROM with a placeholder TableRef whose name
            // is the alias the outer query expects, and stash the inner
            // SELECT as `derived` for the planner.
            stmt.from[0] = TableRef {
                name: alias.clone(),
                alias: Some(alias),
                system_time: None,
                index_hints: vec![],
                partitions: vec![],
                column_aliases: Vec::new(),
                generate_series: None,
            };
            if stmt.derived.is_some() {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "view in FROM combined with explicit derived subquery not supported",
                ));
            }
            stmt.derived = Some(SubQuery::Select(Box::new(expanded)));
        }

        // Remaining FROM items (FROM t1, t2, t3 ...) — sqlparser usually
        // lowers these to JOINs already, but defend against the path
        // staying as multi-from. Convert any view reference into a CROSS
        // JOIN whose `derived` carries the parsed view body.
        if stmt.from.len() > 1 {
            let extras: Vec<TableRef> = stmt.from.drain(1..).collect();
            for extra in extras {
                if let Some(view) = lookup_view_definition(db, session, &extra.name)? {
                    if extra.system_time.is_some() {
                        return Err(error(
                            crate::error::catalog::ER_NOT_SUPPORTED_YET,
                            "FOR SYSTEM_TIME AS OF cannot qualify a view; place it on base tables inside the view definition",
                        ));
                    }
                    let alias = extra.alias.clone().unwrap_or_else(|| view.name.clone());
                    let expanded = expand_view_body(db, session, &view, &extra.name, depth, trail)?;
                    stmt.joins.insert(
                        0,
                        Join {
                            table: TableRef {
                                name: alias.clone(),
                                alias: Some(alias),
                                system_time: None,
                                index_hints: vec![],
                                partitions: vec![],
                                column_aliases: Vec::new(),
                                generate_series: None,
                            },
                            join_type: JoinType::Cross,
                            condition: None,
                            using_columns: Vec::new(),
                            natural: false,
                            derived: Some(SubQuery::Select(Box::new(expanded))),
                            straight: false,
                            lateral: false,
                        },
                    );
                } else {
                    stmt.joins.insert(
                        0,
                        Join {
                            table: extra,
                            join_type: JoinType::Cross,
                            condition: None,
                            using_columns: Vec::new(),
                            natural: false,
                            derived: None,
                            straight: false,
                            lateral: false,
                        },
                    );
                }
            }
        }
    }

    // JOIN list: rewrite each view-typed table into a derived join.
    for join in stmt.joins.iter_mut() {
        // Recurse into pre-existing derived first (nested views inside a
        // JOIN subquery).
        if let Some(inner) = &mut join.derived {
            match inner {
                SubQuery::Select(s) => expand_views_in_select(db, session, s, depth, trail)?,
                stmt @ (SubQuery::SetOp(_) | SubQuery::Rrf(_) | SubQuery::WithCte(_)) => {
                    expand_views_in_statement(db, session, stmt, depth, trail)?
                }
            }
            continue;
        }
        if let Some(view) = lookup_view_definition(db, session, &join.table.name)? {
            if join.table.system_time.is_some() {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "FOR SYSTEM_TIME AS OF cannot qualify a view; place it on base tables inside the view definition",
                ));
            }
            let alias = join
                .table
                .alias
                .clone()
                .unwrap_or_else(|| view.name.clone());
            let expanded = expand_view_body(db, session, &view, &join.table.name, depth, trail)?;
            join.table = TableRef {
                name: alias.clone(),
                alias: Some(alias),
                system_time: None,
                index_hints: vec![],
                partitions: vec![],
                column_aliases: Vec::new(),
                generate_series: None,
            };
            join.derived = Some(SubQuery::Select(Box::new(expanded)));
        }
    }

    for item in &mut stmt.projection {
        if let SelectItem::Expr { expr, .. } = item {
            expand_views_in_expr(db, session, expr, depth, trail)?;
        }
    }
    for expr in &mut stmt.group_by {
        expand_views_in_expr(db, session, expr, depth, trail)?;
    }
    for item in &mut stmt.order_by {
        expand_views_in_expr(db, session, &mut item.expr, depth, trail)?;
    }
    for join in &mut stmt.joins {
        if let Some(expr) = &mut join.condition {
            expand_views_in_expr(db, session, expr, depth, trail)?;
        }
    }
    for window in &mut stmt.named_windows {
        for expr in &mut window.partition_by {
            expand_views_in_expr(db, session, expr, depth, trail)?;
        }
        for item in &mut window.order_by {
            expand_views_in_expr(db, session, &mut item.expr, depth, trail)?;
        }
    }
    if let Some(selection) = &mut stmt.selection {
        expand_views_in_expr(db, session, selection, depth, trail)?;
    }
    if let Some(having) = &mut stmt.having {
        expand_views_in_expr(db, session, having, depth, trail)?;
    }
    Ok(())
}

/// Look up a view by `current_database`-qualified name; if the view
/// exists but is marked invalid (a referenced table was dropped or
/// altered), raise MySQL error 1356 immediately rather than handing
/// back a stale body.
/// Merge only bodies accepted by the existing semantic flattening rules.
/// Keep the outer view wrapper (EXPLAIN/metadata), and retain dependency
/// records already collected while expanding every referenced view.
fn compact_expanded_view_body(select: &mut Select) -> Result<(), QueryError> {
    if !select.no_merge {
        if let Some(flat) =
            crate::engine::executor::select::try_flatten_simple_derived_select(select)
        {
            *select = flat;
        }
    }
    fn depth(select: &Select) -> usize {
        select
            .derived
            .iter()
            .chain(select.joins.iter().filter_map(|j| j.derived.as_ref()))
            .filter_map(SubQuery::leftmost_select)
            .map(|s| 1 + depth(s))
            .max()
            .unwrap_or(0)
    }
    // Non-mergeable bodies still execute recursively. Bound the residual
    // execution depth independently of the number of source view names.
    if depth(select) >= 16 {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "view expansion exceeded maximum non-mergeable nesting depth",
        ));
    }
    Ok(())
}

fn lookup_view_definition(
    db: &Arc<Database>,
    session: &SessionContext,
    name: &str,
) -> Result<Option<ViewSchema>, QueryError> {
    if session.has_temp_table(name) || crate::engine::executor::cte::lookup_cte(name).is_some() {
        return Ok(None);
    }
    let Some(view) = db.view_definition(&session.current_database, name) else {
        return Ok(None);
    };
    if view.invalid {
        return Err(error(crate::error::catalog::ER_VIEW_INVALID,
            format!(
                "View '{}.{}' references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them",
                view.database, view.name
            ),
        ));
    }
    // Record this view (and its version) as a dependency of the plan being
    // built for the current statement, so ALTER / DROP VIEW invalidates only
    // the plans that expanded it (see `cache::record_plan_object_dep`). The
    // key matches the catalog's `views` map key (`db.name`).
    let view_key = crate::storage::naming::table_key(&view.database, &view.name);
    crate::engine::cache::record_plan_object_dep(
        Arc::from(view_key.as_str()),
        view.schema_version,
        view.plan_cache_identity(),
    );
    Ok(Some(view))
}

pub(super) fn trail_key(view: &ViewSchema) -> (String, String) {
    (
        view.database.to_ascii_lowercase(),
        view.name.to_ascii_lowercase(),
    )
}

fn push_trail(
    trail: &mut ViewTrail,
    view: &ViewSchema,
    referenced_as: &str,
) -> Result<(), QueryError> {
    let key = trail_key(view);
    if !trail.insert(key) {
        return Err(error(
            crate::error::catalog::ER_VIEW_RECURSIVE,
            format!("View '{}' contains a recursive reference", referenced_as),
        ));
    }
    Ok(())
}

fn expand_views_in_expr(
    db: &Arc<Database>,
    session: &SessionContext,
    expr: &mut Expr,
    depth: usize,
    trail: &mut ViewTrail,
) -> Result<(), QueryError> {
    crate::util::stack::with_stack(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::Executor,
        || expand_views_in_expr_checked(db, session, expr, depth, trail),
    )?
}

fn expand_views_in_expr_checked(
    db: &Arc<Database>,
    session: &SessionContext,
    expr: &mut Expr,
    depth: usize,
    trail: &mut ViewTrail,
) -> Result<(), QueryError> {
    match expr {
        Expr::InSubquery { expr, subquery, .. } => {
            expand_views_in_expr(db, session, expr, depth, trail)?;
            expand_views_in_select(db, session, subquery, depth, trail)
        }
        Expr::QuantifiedComparison { left, right, .. } => {
            expand_views_in_expr(db, session, left, depth, trail)?;
            match right {
                QuantifiedComparisonRight::Subquery(subquery) => {
                    expand_views_in_select(db, session, subquery, depth, trail)
                }
                QuantifiedComparisonRight::List(list) => {
                    for item in list {
                        expand_views_in_expr(db, session, item, depth, trail)?;
                    }
                    Ok(())
                }
            }
        }
        Expr::Subquery(subquery) => expand_views_in_select(db, session, subquery, depth, trail),
        Expr::Exists { subquery, .. } => {
            expand_views_in_select(db, session, subquery, depth, trail)
        }
        Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            expand_views_in_expr(db, session, expr, depth, trail)
        }
        Expr::Binary { left, right, .. } => {
            expand_views_in_expr(db, session, left, depth, trail)?;
            expand_views_in_expr(db, session, right, depth, trail)
        }
        Expr::InList { expr, list, .. } => {
            expand_views_in_expr(db, session, expr, depth, trail)?;
            for item in list {
                expand_views_in_expr(db, session, item, depth, trail)?;
            }
            Ok(())
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expand_views_in_expr(db, session, expr, depth, trail)?;
            expand_views_in_expr(db, session, low, depth, trail)?;
            expand_views_in_expr(db, session, high, depth, trail)
        }
        Expr::Function { args, .. } | Expr::Tuple(args) => {
            for arg in args {
                expand_views_in_expr(db, session, arg, depth, trail)?;
            }
            Ok(())
        }
        Expr::Like { expr, pattern, .. } => {
            expand_views_in_expr(db, session, expr, depth, trail)?;
            expand_views_in_expr(db, session, pattern, depth, trail)
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(operand) = operand {
                expand_views_in_expr(db, session, operand, depth, trail)?;
            }
            for cond in conditions {
                expand_views_in_expr(db, session, cond, depth, trail)?;
            }
            for result in results {
                expand_views_in_expr(db, session, result, depth, trail)?;
            }
            if let Some(else_result) = else_result {
                expand_views_in_expr(db, session, else_result, depth, trail)?;
            }
            Ok(())
        }
        Expr::WindowFunction { args, spec, .. } => {
            let crate::parser::WindowSpec {
                partition_by,
                order_by,
                ..
            } = spec.as_mut();

            for expr in args.iter_mut().chain(partition_by.iter_mut()) {
                expand_views_in_expr(db, session, expr, depth, trail)?;
            }
            for item in order_by {
                expand_views_in_expr(db, session, &mut item.expr, depth, trail)?;
            }
            Ok(())
        }
        Expr::GroupKeyRef { expr, .. } => expand_views_in_expr(db, session, expr, depth, trail),
        _ => Ok(()),
    }
}

pub(super) fn view_query_from_query(query: crate::parser::QueryExpr) -> Select {
    if let crate::parser::QueryExpr::WithCte(mut with) = query {
        crate::engine::executor::cte::inline_single_use_ctes(&mut with.ctes, &mut with.body);
        return if with.ctes.is_empty() {
            wrap_view_query(with.body)
        } else {
            wrap_view_query(crate::parser::QueryExpr::WithCte(with))
        };
    }
    wrap_view_query(query)
}

fn wrap_view_query(query: crate::parser::QueryExpr) -> Select {
    if let crate::parser::QueryExpr::Select(select) = query {
        return *select;
    }
    Select {
        projection: vec![SelectItem::Wildcard],
        table: Some("__view_query".to_owned()),
        from: vec![TableRef {
            name: "__view_query".to_owned(),
            alias: None,
            system_time: None,
            index_hints: vec![],
            partitions: vec![],
            column_aliases: Vec::new(),
            generate_series: None,
        }],
        derived: Some(query),
        ..Select::default()
    }
}

/// The catalog owns the immutable semantic query across sessions. No SQL
/// reparsing or process-global cache may determine a view's execution meaning.
pub(super) fn stored_view_query(view: &ViewSchema) -> Result<Select, QueryError> {
    let query = view.query.as_ref().ok_or_else(|| {
        error(
            crate::error::catalog::ER_VIEW_INVALID,
            format!(
                "View '{}.{}' has a missing or unsupported semantic query format",
                view.database, view.name
            ),
        )
    })?;
    Ok(query.select().clone())
}

/// Apply the view's stored output column names to its projection. We only
/// rename when the count matches — wildcard projections expand to a
/// variable number of columns and are left alone (they pick up the
/// underlying table's names, which is also MySQL's behaviour for views
/// without an explicit column list).
fn apply_view_column_aliases(select: &mut Select, columns: &[ViewColumnDef]) {
    if columns.is_empty() || columns.len() != select.projection.len() {
        return;
    }
    use crate::parser::SelectItem as PSelectItem;
    for (item, target) in select.projection.iter_mut().zip(columns.iter()) {
        if let PSelectItem::Expr { alias, .. } = item {
            *alias = Some(target.name.clone());
        }
    }
}

/// Validate a CREATE/ALTER VIEW body. Verifies the body parses as a SELECT,
/// recursively expands inner view references (catching cycles), and
/// confirms every base-table reference exists at the time the view is
/// created. Also derives the output column list (honouring an explicit
/// `CREATE VIEW v(c1, c2)` rename) and collects dependencies for
/// reverse-lookup invalidation.
///
/// Returning early on any failure mirrors MySQL's behaviour of refusing
/// to create a view with broken references — silently storing such a
/// view would defer the failure to the first SELECT and surprise users.
pub(super) struct ViewValidation {
    pub(super) columns: Vec<ViewColumnDef>,
    pub(super) dependencies: Vec<ViewDependency>,
    pub(super) routine_dependencies: Vec<ViewRoutineDependency>,
    pub(super) is_updatable: bool,
    pub(super) supports_check_option: bool,
    /// MySQL-style canonical view body, or empty when the body is not
    /// faithfully renderable (callers then echo the raw `sql`).
    pub(super) definition_normalized: String,
    pub(super) query: crate::storage::schema::ViewQuery,
}

/// Publish one validated definition for CREATE/ALTER. Callers retain existence,
/// definer and statement-specific checks; both paths share CHECK OPTION gating,
/// catalog publication and dependency/cache invalidation.
pub(super) fn publish_view_definition(
    db: &Arc<Database>,
    session: &SessionContext,
    name: &str,
    sql: &str,
    validation: ViewValidation,
    view_options: MaterializedViewOptions,
    check_option: Option<String>,
    or_replace: bool,
    if_not_exists: bool,
) -> Result<QueryResponse, QueryError> {
    if check_option.is_some() && !validation.is_updatable {
        return Err(error(
            crate::error::catalog::ER_VIEW_NONUPD_CHECK,
            format!("CHECK OPTION on non-updatable view '{name}'"),
        ));
    }
    if check_option.is_some() && !validation.supports_check_option {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "CHECK OPTION on join views is not supported",
        ));
    }
    db.create_view(
        &session.current_database,
        name,
        sql,
        or_replace,
        if_not_exists,
        validation.columns,
        validation.dependencies,
        validation.routine_dependencies,
        view_options.algorithm,
        view_options.definer_user,
        view_options.definer_host,
        view_options.security_type,
        check_option,
        validation.is_updatable,
        session.character_set_client.clone(),
        session.collation_connection.clone(),
        validation.definition_normalized,
        validation.query,
    )?;
    let (cache_db, cache_name) = crate::storage::split_table_name(&session.current_database, name);

    revalidate_altered_table_views(db, session, &cache_db, &cache_name)?;
    // The view's schema_version is bumped by create_view; SELECT
    // plans that expanded it carry it as a view object-dep and are
    // invalidated precisely on their next lookup — no global flush.
    // DML memos through an updatable view are cleared conservatively.
    crate::engine::cache::clear_prepared_dml_caches();
    Ok(QueryResponse::Ok)
}

/// Rebind dependent views after ALTER, without executing their SELECT bodies.
/// Invalid definitions remain catalogued and can become valid after a later ALTER.
pub(super) fn revalidate_altered_table_views(
    db: &Arc<Database>,
    session: &SessionContext,
    database: &str,
    table: &str,
) -> Result<(), QueryError> {
    let names = db.invalidate_views_referencing(database, table)?;
    revalidate_views(db, session, database, names)
}

pub(in crate::engine::executor) fn revalidate_routine_views(
    db: &Arc<Database>,
    session: &SessionContext,
    database: &str,
    routine: &str,
) -> Result<(), QueryError> {
    let names = db
        .catalog()
        .views
        .values()
        .filter(|view| {
            view.invalid
                && view.routine_dependencies.iter().any(|dep| {
                    dep.database.eq_ignore_ascii_case(database)
                        && dep.name.eq_ignore_ascii_case(routine)
                })
        })
        .map(|view| format!("{}.{}", view.database, view.name))
        .collect();
    revalidate_views(db, session, database, names)
}

fn revalidate_views(
    db: &Arc<Database>,
    session: &SessionContext,
    database: &str,
    names: Vec<String>,
) -> Result<(), QueryError> {
    let mut pending = names;
    loop {
        let mut next = Vec::new();
        let mut restored = false;
        for name in pending {
            let Some(view) = db.view_definition(database, &name) else {
                continue;
            };
            // A restored function must not mask another missing dependency.
            if view.routine_dependencies.iter().any(|dep| {
                db.routine_definition(&dep.database, RoutineKindSchema::Function, &dep.name)
                    .is_none()
            }) {
                next.push(name);
                continue;
            }
            let mut context = session.clone();
            context.current_database = view.database.clone();
            let columns: Vec<_> = view
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect();
            let validation = stored_view_query(&view).and_then(|select| {
                validate_view_definition(db, &context, &name, select, &columns, &view.algorithm)
            });
            let Ok(valid) = validation else {
                next.push(name);
                continue;
            };
            db.create_view(
                &view.database,
                &view.name,
                &view.sql,
                true,
                false,
                valid.columns,
                valid.dependencies,
                valid.routine_dependencies,
                view.algorithm.clone(),
                view.definer_user.clone(),
                view.definer_host.clone(),
                view.security_type.clone(),
                view.check_option.clone(),
                valid.is_updatable,
                view.character_set_client.clone(),
                view.collation_connection.clone(),
                valid.definition_normalized,
                valid.query,
            )?;

            restored = true;
        }
        if !restored || next.is_empty() {
            break;
        }
        pending = next;
    }
    Ok(())
}

pub(super) fn validate_view_definition(
    db: &Arc<Database>,
    session: &SessionContext,
    view_name: &str,
    mut select: Select,
    explicit_columns: &[String],
    algorithm: &str,
) -> Result<ViewValidation, QueryError> {
    // Validate the parsed query; source text is only for presentation.
    reject_unsupported_view_exprs(&select)?;
    reject_view_temp_table_refs(session, &select)?;
    let mut original_select = select.clone();

    // 2) Expand any nested view references inside the body so we can
    //    detect cycles and dangling refs at CREATE time. Seed the trail
    //    with the view-being-created so a self-reference (CREATE VIEW v
    //    AS SELECT * FROM v) is caught even before insertion.
    let (this_db, this_name) =
        crate::storage::split_table_name(&session.current_database, view_name);
    let mut trail: ViewTrail = std::collections::HashSet::new();
    trail.insert((this_db.to_ascii_lowercase(), this_name.to_ascii_lowercase()));
    expand_views_in_select(db, session, &mut select, 0, &mut trail)?;
    crate::engine::executor::qualification::qualify_select_tables(db, session, &mut select)?;
    crate::planner::resolve_join_using_natural(db, &mut select)?;
    crate::planner::validate_select_column_references(db, &select)?;
    for name in crate::parser::collect_select_function_names(&select) {
        if !crate::storage::sequence::is_sequence_function(&name)
            && crate::parser::IntrinsicFunction::from_name(&name).is_none()
            && !crate::engine::cbo::function_cost::is_known_builtin_function(&name)
            && !crate::parser::is_aggregate_function(&name)
            && db
                .routine_definition(
                    &session.current_database,
                    RoutineKindSchema::Function,
                    &name,
                )
                .is_none()
        {
            return Err(error(
                crate::error::catalog::ER_SP_DOES_NOT_EXIST,
                format!(
                    "FUNCTION {}.{} does not exist",
                    session.current_database, name
                ),
            ));
        }
    }
    // Freeze the output through the same catalog scope used by ordinary queries.
    let mut frozen = select.clone();
    crate::planner::freeze_select_wildcards(db, &mut frozen)?;
    original_select.projection = frozen.projection;
    crate::engine::executor::qualification::qualify_select_tables(
        db,
        session,
        &mut original_select,
    )?;

    // 3) Collect dependencies (base tables and views referenced by the
    //    expanded body). After expansion, view references in FROM/joins
    //    have been rewritten into derived subqueries, so the surviving
    //    `TableRef::name` entries are base tables. We also walk into
    //    derived subqueries to find tables they reference.
    let mut deps: Vec<ViewDependency> = Vec::new();
    collect_direct_view_dependencies(db, &session.current_database, &original_select, &mut deps);
    collect_select_dependencies(db, &session.current_database, &select, &mut deps);
    let mut routine_deps = Vec::new();
    collect_select_routine_dependencies(db, &session.current_database, &select, &mut routine_deps);
    // Re-add references that were view-typed: they were rewritten during
    // expansion, but the SQL string still contains the original names; we
    // also stored them in `trail` (excluding the view-being-created).
    for (dep_db, dep_name) in trail.iter() {
        if dep_db == &this_db.to_ascii_lowercase() && dep_name == &this_name.to_ascii_lowercase() {
            continue;
        }
        if !deps.iter().any(|d| {
            d.database.eq_ignore_ascii_case(dep_db) && d.name.eq_ignore_ascii_case(dep_name)
        }) {
            deps.push(ViewDependency {
                database: dep_db.clone(),
                name: dep_name.clone(),
                is_view: true,
            });
        }
    }

    // 4) Verify each non-view dependency points to a real base table.
    for dep in deps.iter().filter(|d| !d.is_view) {
        let qualified = format!("{}.{}", dep.database, dep.name);
        if !is_reserved_system_table_name(&qualified)
            && db
                .table_required(&session.current_database, &qualified)
                .is_err()
        {
            return Err(error(
                crate::error::catalog::ER_NO_SUCH_TABLE,
                format!(
                    "Table '{}.{}' doesn't exist (referenced by view '{}')",
                    dep.database, dep.name, view_name
                ),
            ));
        }
    }

    // 5) Derive output columns from the SELECT projection. We keep this
    //    best-effort: column names are reliable (they drive SHOW COLUMNS /
    //    information_schema.columns), but type inference is limited to
    //    direct column references — expressions get `data_type = None`
    //    and the result-set type at execution time wins, matching MySQL.
    let mut columns: Vec<ViewColumnDef> = Vec::new();
    derive_view_output_columns(db, &session.current_database, &select, &mut columns)?;
    if !explicit_columns.is_empty() {
        if explicit_columns.len() != columns.len() {
            return Err(error(
                crate::error::catalog::ER_VIEW_WRONG_LIST,
                format!(
                    "View's SELECT and view's field list have different column counts ({} vs {})",
                    columns.len(),
                    explicit_columns.len()
                ),
            ));
        }
        for (col, name) in columns.iter_mut().zip(explicit_columns.iter()) {
            col.name = name.clone();
        }
    }

    reject_duplicate_view_columns(&columns)?;
    let simple_updatable = !algorithm.eq_ignore_ascii_case("TEMPTABLE")
        && simple_updatable_view_shape(db, &session.current_database, &original_select, &columns)
            .is_ok();
    let is_updatable = simple_updatable
        || (!algorithm.eq_ignore_ascii_case("TEMPTABLE")
            && mergeable_join_view_shape(db, &session.current_database, &original_select));
    // CHECK OPTION currently uses the single-table post-image evaluator. An
    // outer simple projection must not hide a join deeper in its source chain.
    let mut supports_check_option = simple_updatable;
    if supports_check_option {
        let mut database = session.current_database.clone();
        let mut source = original_select.from[0].name.clone();
        for depth in 0..=MAX_VIEW_DEPTH {
            let Some(inner) = db.view_definition(&database, &source) else {
                break;
            };
            let body = stored_view_query(&inner)?;
            if depth == MAX_VIEW_DEPTH
                || simple_updatable_view_shape(db, &inner.database, &body, &inner.columns).is_err()
            {
                supports_check_option = false;
                break;
            }
            database = inner.database;
            source = body.from[0].name.clone();
        }
    }

    apply_view_column_aliases(&mut original_select, &columns);
    original_select.no_merge = algorithm.eq_ignore_ascii_case("TEMPTABLE");

    // Canonicalize presentation separately from the semantic query. Rendered
    // from the *original* (un-expanded) select so the echoed definition keeps
    // the user's view/table references rather than inlined subqueries.
    // Returns empty on any non-renderable construct → caller falls back to raw.
    let definition_normalized = {
        let _mode = crate::parser::ParserSqlModeGuard::activate(0);
        crate::engine::executor::view_normalize::normalize_view_body(
            db,
            &session.current_database,
            &original_select,
        )
        .unwrap_or_default()
    };

    Ok(ViewValidation {
        columns,
        dependencies: deps,
        routine_dependencies: routine_deps,
        is_updatable,
        supports_check_option,
        definition_normalized,
        query: crate::storage::schema::ViewQuery::new(original_select)?,
    })
}

fn collect_select_dependencies(
    db: &Arc<Database>,
    current_database: &str,
    select: &Select,
    out: &mut Vec<ViewDependency>,
) {
    let mut uses = Vec::new();
    collect_select_table_uses(select, false, &Default::default(), &mut uses);
    for usage in uses {
        let (database, name) = crate::storage::split_table_name(current_database, &usage.table);
        if usage.table.eq_ignore_ascii_case("dual") {
            continue;
        }
        if !out.iter().any(|d| {
            d.database.eq_ignore_ascii_case(&database) && d.name.eq_ignore_ascii_case(&name)
        }) {
            out.push(ViewDependency {
                is_view: db.view_definition(&database, &name).is_some(),
                database,
                name,
            });
        }
    }
}

fn collect_direct_view_dependencies(
    db: &Arc<Database>,
    current_database: &str,
    select: &Select,
    out: &mut Vec<ViewDependency>,
) {
    let mut uses = Vec::new();
    collect_select_table_uses(select, false, &Default::default(), &mut uses);
    for usage in uses {
        if let Some(view) = db.view_definition(current_database, &usage.table) {
            if !out.iter().any(|d| {
                d.database.eq_ignore_ascii_case(&view.database)
                    && d.name.eq_ignore_ascii_case(&view.name)
            }) {
                out.push(ViewDependency {
                    database: view.database,
                    name: view.name,
                    is_view: true,
                });
            }
        }
    }
}

fn reject_view_temp_table_refs(
    session: &SessionContext,
    select: &Select,
) -> Result<(), QueryError> {
    let mut uses = Vec::new();
    collect_select_table_uses(select, false, &Default::default(), &mut uses);
    if uses
        .iter()
        .any(|usage| session.has_temp_table(&usage.table))
    {
        return Err(error(
            crate::error::catalog::ER_VIEW_SELECT_TMPTABLE,
            "View's SELECT refers to a temporary table",
        ));
    }
    Ok(())
}

struct ViewVariables(bool, bool);
impl crate::parser::visit::AstVisitor for ViewVariables {
    fn enter_select(&mut self, select: &Select) {
        self.1 |= select.for_update || select.lock_clause.is_some();
    }
    fn enter_expr(&mut self, expr: &Expr) {
        self.0 |= matches!(
            expr,
            Expr::Placeholder(_) | Expr::UserVariable(_) | Expr::SystemVariable { .. }
        ) || matches!(expr, Expr::Function { name, .. }
                if crate::parser::IntrinsicFunction::from_name(name)
                    == Some(crate::parser::IntrinsicFunction::UserAssignment));
    }
    fn enter_limit(&mut self, limit: &LimitValue) {
        self.0 |= !matches!(limit, LimitValue::Rows(_));
    }
}
fn reject_unsupported_view_exprs(select: &Select) -> Result<(), QueryError> {
    inspect_view_expressions(|visitor| crate::parser::visit::visit_select(visitor, select))
}
pub(super) fn reject_unsupported_view_query(
    query: &crate::parser::QueryExpr,
) -> Result<(), QueryError> {
    inspect_view_expressions(|visitor| crate::parser::visit::visit_query(visitor, query))
}
fn inspect_view_expressions(visit: impl FnOnce(&mut ViewVariables)) -> Result<(), QueryError> {
    let mut validator = ViewVariables(false, false);
    visit(&mut validator);
    if validator.1 {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "locking clauses in view definitions are not yet supported",
        ));
    }
    if validator.0 {
        Err(error(
            crate::error::catalog::ER_VIEW_SELECT_VARIABLE,
            "View's SELECT contains a variable or parameter",
        ))
    } else {
        Ok(())
    }
}

fn collect_select_routine_dependencies(
    db: &Arc<Database>,
    current_database: &str,
    select: &Select,
    out: &mut Vec<ViewRoutineDependency>,
) {
    for name in crate::parser::collect_select_function_names(select) {
        // An unqualified native call cannot acquire a dependency merely
        // because a stored function happens to have the same name.
        if !name.contains('.')
            && (crate::storage::sequence::is_sequence_function(&name)
                || crate::parser::IntrinsicFunction::from_name(&name).is_some()
                || crate::engine::cbo::function_cost::is_known_builtin_function(&name)
                || crate::parser::is_aggregate_function(&name))
        {
            continue;
        }
        if let Some(routine) =
            db.routine_definition(current_database, RoutineKindSchema::Function, &name)
        {
            if !out.iter().any(|dep| {
                dep.database.eq_ignore_ascii_case(&routine.database)
                    && dep.name.eq_ignore_ascii_case(&routine.name)
            }) {
                out.push(ViewRoutineDependency {
                    database: routine.database.clone(),
                    name: routine.name.clone(),
                });
            }
        }
    }
}

fn reject_duplicate_view_columns(columns: &[ViewColumnDef]) -> Result<(), QueryError> {
    let mut seen = std::collections::HashSet::new();
    for column in columns {
        if !seen.insert(column.name.to_ascii_lowercase()) {
            return Err(error(
                crate::error::catalog::ER_DUP_FIELDNAME,
                format!("duplicate column name '{}'", column.name),
            ));
        }
    }
    Ok(())
}

fn simple_updatable_view_shape(
    db: &Arc<Database>,
    current_database: &str,
    select: &Select,
    columns: &[ViewColumnDef],
) -> Result<(), &'static str> {
    use crate::parser::{Expr as PExpr, SelectItem as PSelectItem};
    if select.derived.is_some() {
        return Err("derived subquery in view body");
    }
    if !select.joins.is_empty() || select.from.len() != 1 {
        return Err("view body must reference exactly one table");
    }
    if !select.group_by.is_empty() || select.having.is_some() {
        return Err("GROUP BY/HAVING");
    }
    if select.distinct
        || !select.named_windows.is_empty()
        || select.limit.is_some()
        || select.offset.is_some()
        || select.for_update
        || select.lock_clause.is_some()
    {
        return Err("view body is not simple-updatable");
    }
    let wildcard = select
        .projection
        .iter()
        .any(|i| matches!(i, PSelectItem::Wildcard | PSelectItem::QualifiedWildcard(_)));
    if !wildcard && select.projection.len() != columns.len() {
        return Err("projection length mismatch");
    }
    for item in &select.projection {
        let PSelectItem::Expr { expr, .. } = item else {
            continue;
        };
        match expr {
            PExpr::Identifier(_) | PExpr::CompoundIdentifier(_) => {}
            _ => return Err("expression column"),
        }
    }
    let source = &select.from[0].name;
    if db.is_base_table(current_database, source) {
        return Ok(());
    }
    if let Some(view) = db.view_definition(current_database, source) {
        if !view.algorithm.eq_ignore_ascii_case("TEMPTABLE") && view.is_updatable {
            return Ok(());
        }
        return Err("nested view is not updatable");
    }
    Err("view body does not reference an updatable source")
}

/// Shapes that can use the existing joined DML executor without materializing
/// a view or inventing a target-row identity. Keep metadata and UPDATE aligned.
fn mergeable_join_view_shape(db: &Arc<Database>, database: &str, body: &Select) -> bool {
    !body.joins.is_empty()
        && body.from.len() == 1
        && body.derived.is_none()
        && !body.distinct
        && body.group_by.is_empty()
        && body.having.is_none()
        && body.limit.is_none()
        && body.offset.is_none()
        && !body.for_update
        && body.lock_clause.is_none()
        && body.named_windows.is_empty()
        && body.projection.iter().all(|item| {
            matches!(
                item,
                SelectItem::Wildcard
                    | SelectItem::QualifiedWildcard(_)
                    | SelectItem::Expr {
                        expr: Expr::Identifier(_) | Expr::CompoundIdentifier(_),
                        ..
                    }
            )
        })
        && body.joins.iter().all(|j| {
            j.derived.is_none()
                && !j.natural
                && j.using_columns.is_empty()
                && matches!(j.join_type, JoinType::Inner | JoinType::Cross)
        })
        && body
            .from
            .iter()
            .chain(body.joins.iter().map(|j| &j.table))
            .all(|source| {
                db.is_base_table(database, &source.name)
                    || db.view_definition(database, &source.name).is_some_and(|v| {
                        v.is_updatable
                            && !v.invalid
                            && !v.algorithm.eq_ignore_ascii_case("TEMPTABLE")
                            && v.check_option.is_none()
                    })
            })
}

fn derive_view_output_columns(
    db: &Arc<Database>,
    current_database: &str,
    select: &Select,
    out: &mut Vec<ViewColumnDef>,
) -> Result<(), QueryError> {
    // System relations have no storage TableSchema. Describe their real
    // projection, including joins, aliases and wildcard expansion.
    if !select.joins.is_empty()
        || select
            .from
            .iter()
            .chain(select.joins.iter().map(|j| &j.table))
            .any(|t| {
                is_reserved_system_table_name(&qualify_table_name_in_database(
                    current_database,
                    &t.name,
                ))
            })
    {
        let mut context = SessionContext::default();
        context.current_database = current_database.to_string();
        let mut shape = select.clone();
        crate::engine::executor::qualification::qualify_select_tables(db, &context, &mut shape)?;
        for column in crate::engine::executor::select::describe_select_columns(db, &shape)? {
            let nullable = column.source.flags & crate::mysql::protocol::NOT_NULL_FLAG == 0;
            let definition = ctas_column_def(db, &context, &column, nullable);
            out.push(ViewColumnDef {
                name: column.name,
                data_type: Some(definition.data_type.into()),
                nullable,
                charset: definition.charset,
                collation: definition.collation,
            });
        }
        return Ok(());
    }
    if let Some(derived) = select.derived.as_ref().filter(|_| select.joins.is_empty()) {
        // Preserve the full declared type through each simple view layer.
        // Wire descriptors alone lose VARCHAR widths, temporal precision and
        // NOT NULL when converted through a synthetic derived table.
        let mut input = Vec::new();
        match derived {
            SubQuery::Select(inner) => {
                derive_view_output_columns(db, current_database, inner, &mut input)?
            }
            SubQuery::SetOp(_) | SubQuery::Rrf(_) | SubQuery::WithCte(_) => {
                let mut context = SessionContext::default();
                context.current_database = current_database.to_string();
                let mut shape = select.clone();
                crate::engine::executor::qualification::qualify_select_tables(
                    db, &context, &mut shape,
                )?;
                for column in crate::engine::executor::select::describe_subquery_columns(
                    db,
                    shape.derived.as_ref().unwrap(),
                )? {
                    let nullable = column.source.flags & crate::mysql::protocol::NOT_NULL_FLAG == 0;
                    let definition = ctas_column_def(db, &context, &column, nullable);
                    input.push(ViewColumnDef {
                        name: column.name,
                        data_type: Some(definition.data_type.into()),
                        nullable: definition.nullable,
                        charset: definition.charset,
                        collation: definition.collation,
                    });
                }
            }
        }
        for (index, item) in select.projection.iter().enumerate() {
            match item {
                SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => {
                    out.extend(input.iter().cloned())
                }
                SelectItem::Expr { expr, alias } => {
                    let referenced = match expr {
                        Expr::Identifier(name) => Some(name),
                        Expr::CompoundIdentifier(parts) => parts.last(),
                        _ => None,
                    };
                    if let Some(column) = referenced.and_then(|name| {
                        input
                            .iter()
                            .find(|column| column.name.eq_ignore_ascii_case(name))
                    }) {
                        let mut column = column.clone();
                        if let Some(alias) = alias {
                            column.name = alias.clone();
                        }
                        out.push(column);
                    } else {
                        let inferred = infer_view_expr_type(db, current_database, select, expr);
                        out.push(ViewColumnDef {
                            name: alias
                                .clone()
                                .or_else(|| referenced.cloned())
                                .unwrap_or_else(|| format!("Name_exp_{}", index + 1)),
                            data_type: inferred.as_ref().map(|(kind, _)| kind.clone()),
                            nullable: inferred.map(|(_, nullable)| nullable).unwrap_or(true),
                            charset: None,
                            collation: None,
                        });
                    }
                }
            }
        }
        return Ok(());
    }
    use crate::parser::{Expr as PExpr, SelectItem as PSelectItem};
    let primary_table = if select.derived.is_none() {
        select
            .from
            .first()
            .and_then(|t| db.table_required(current_database, t.name.as_str()).ok())
    } else {
        None
    };
    for (idx, item) in select.projection.iter().enumerate() {
        match item {
            PSelectItem::Wildcard => {
                if let Some(table) = &primary_table {
                    for (_, col) in table.iter_select_columns() {
                        out.push(ViewColumnDef {
                            name: col.name.clone(),
                            data_type: Some(col.data_type.clone()),
                            nullable: col.nullable,
                            charset: col.charset.clone(),
                            collation: col.collation.clone(),
                        });
                    }
                } else if let Some(inner) =
                    select.derived.as_ref().and_then(SubQuery::leftmost_select)
                {
                    derive_view_output_columns(db, current_database, inner, out)?;
                } else {
                    // Wildcard over a derived subquery: column names are
                    // determined at execute time. Emit a placeholder so
                    // the count is at least non-zero (will be overridden
                    // by an explicit column list, otherwise MySQL would
                    // also report `Name_exp_N`).
                    out.push(ViewColumnDef {
                        name: format!("Name_exp_{}", idx + 1),
                        data_type: None,
                        nullable: true,
                        charset: None,
                        collation: None,
                    });
                }
            }
            PSelectItem::QualifiedWildcard(_qualifier) => {
                if let Some(table) = &primary_table {
                    for (_, col) in table.iter_select_columns() {
                        out.push(ViewColumnDef {
                            name: col.name.clone(),
                            data_type: Some(col.data_type.clone()),
                            nullable: col.nullable,
                            charset: col.charset.clone(),
                            collation: col.collation.clone(),
                        });
                    }
                } else {
                    out.push(ViewColumnDef {
                        name: format!("Name_exp_{}", idx + 1),
                        data_type: None,
                        nullable: true,
                        charset: None,
                        collation: None,
                    });
                }
            }
            PSelectItem::Expr { expr, alias } => {
                let column_ref_name = match expr {
                    PExpr::Identifier(n) => Some(n.clone()),
                    PExpr::CompoundIdentifier(parts) => parts.last().cloned(),
                    _ => None,
                };
                let name = alias
                    .clone()
                    .or_else(|| column_ref_name.clone())
                    .unwrap_or_else(|| format!("Name_exp_{}", idx + 1));
                let (data_type, nullable) = match &column_ref_name {
                    // Direct column reference: resolve the exact source column
                    // type + nullability against the primary table (preferred)
                    // or any in-scope base table (joins).
                    Some(col_name) => primary_table
                        .as_ref()
                        .and_then(|t| t.find_column_index(col_name).map(|index| &t.columns[index]))
                        .map(|col| (Some(col.data_type.clone()), col.nullable))
                        .or_else(|| {
                            view_scope_column(db, current_database, select, col_name)
                                .map(|(dt, n)| (Some(dt), n))
                        })
                        .unwrap_or((None, true)),
                    // P2 — expression column: statically infer the output type
                    // (arithmetic / functions / CASE / CAST / column-typed
                    // operands), freeze it like MySQL does at CREATE time. Falls
                    // back to `None` (execute-time type wins) when uncertain.
                    None => infer_view_expr_type(db, current_database, select, expr)
                        .map(|(dt, n)| (Some(dt), n))
                        .unwrap_or((None, true)),
                };
                let (charset, collation) = column_ref_name
                    .as_ref()
                    .and_then(|col_name| {
                        primary_table.as_ref().and_then(|table| {
                            table
                                .find_column_index(col_name)
                                .map(|index| &table.columns[index])
                                .map(|column| (column.charset.clone(), column.collation.clone()))
                        })
                    })
                    .unwrap_or((None, None));
                out.push(ViewColumnDef {
                    name,
                    data_type,
                    nullable,
                    charset,
                    collation,
                });
            }
        }
    }
    Ok(())
}

/// Resolve a bare column name against every base table referenced by the view
/// body's FROM + JOINs, returning `(DataTypeKind, nullable)`. Used by view
/// column-type derivation when the primary-table lookup misses (join columns)
/// and by expression type inference. Returns `None` for ambiguous or
/// unresolvable names — callers then leave the type unfixed.
fn view_scope_column(
    db: &Arc<Database>,
    current_database: &str,
    select: &Select,
    col_name: &str,
) -> Option<(crate::storage::schema::DataTypeKind, bool)> {
    if select.derived.is_some() {
        return None;
    }
    let mut found: Option<(crate::storage::schema::DataTypeKind, bool)> = None;
    let mut tables: Vec<&str> = select.from.iter().map(|t| t.name.as_str()).collect();
    for join in &select.joins {
        if join.derived.is_some() {
            // A derived-table join hides its column types from us; bail to
            // avoid a wrong (or ambiguous) inference.
            return None;
        }
        tables.push(join.table.name.as_str());
    }
    // Whether the column sits on the nullable side of an outer join: a column
    // from the right side of a LEFT JOIN (or left side of a RIGHT JOIN) may be
    // NULL even if declared NOT NULL. We conservatively widen to nullable.
    let mut outer_nullable_tables: std::collections::HashSet<usize> =
        std::collections::HashSet::new();
    for (ji, join) in select.joins.iter().enumerate() {
        match join.join_type {
            crate::parser::JoinType::Left => {
                // join table index in `tables` = from.len() + ji
                outer_nullable_tables.insert(select.from.len() + ji);
            }
            crate::parser::JoinType::Right => {
                // Everything to the left becomes nullable.
                for k in 0..(select.from.len() + ji) {
                    outer_nullable_tables.insert(k);
                }
            }
            crate::parser::JoinType::Full => {
                for k in 0..=(select.from.len() + ji) {
                    outer_nullable_tables.insert(k);
                }
            }
            _ => {}
        }
    }
    for (ti, table_name) in tables.iter().enumerate() {
        let Ok(table) = db.table_required(current_database, table_name) else {
            continue;
        };
        if let Some(col) = table
            .find_column_index(col_name)
            .map(|index| &table.columns[index])
        {
            if found.is_some() {
                // Ambiguous across tables — don't guess.
                return None;
            }
            let nullable = col.nullable || outer_nullable_tables.contains(&ti);
            found = Some((col.data_type.clone(), nullable));
        }
    }
    found
}

/// P2 — best-effort static type inference for an expression projection in a
/// view body, mirroring MySQL's freeze-at-CREATE-time semantics. Resolves column
/// references against the in-scope base tables and otherwise classifies via
/// the shared `expr_result_type` kernel. Returns `None` (→ execute-time type)
/// for constructs whose type is not statically decidable (user variables,
/// subqueries, non-deterministic shapes).
fn infer_view_expr_type(
    db: &Arc<Database>,
    current_database: &str,
    select: &Select,
    expr: &Expr,
) -> Option<(crate::storage::schema::DataTypeKind, bool)> {
    use crate::parser::Expr as PExpr;
    match expr {
        PExpr::Identifier(n) => view_scope_column(db, current_database, select, n),
        PExpr::CompoundIdentifier(parts) => {
            let name = parts.last()?;
            view_scope_column(db, current_database, select, name)
        }
        // User variables / placeholders / bound params / subqueries: let the
        // execute-time type win, matching MySQL's treatment of these as
        // dynamically typed in view columns.
        PExpr::UserVariable(_)
        | PExpr::Placeholder(_)
        | PExpr::Subquery(_)
        | PExpr::Exists { .. }
        | PExpr::InSubquery { .. }
        | PExpr::QuantifiedComparison { .. } => None,
        // Aggregate functions are not classified by the scalar
        // `function_result_type` kernel; freeze the common ones here so a view
        // over `COUNT(*)` / `SUM(x)` reports an integer / numeric column rather
        // than the generic varchar fallback.
        PExpr::Function { name, args, .. }
            if matches!(
                name.to_ascii_uppercase().as_str(),
                "COUNT"
                    | "SUM"
                    | "AVG"
                    | "MIN"
                    | "MAX"
                    | "BIT_AND"
                    | "BIT_OR"
                    | "BIT_XOR"
                    | "STDDEV"
                    | "STDDEV_POP"
                    | "STDDEV_SAMP"
                    | "VARIANCE"
                    | "VAR_POP"
                    | "VAR_SAMP"
                    | "GROUP_CONCAT"
            ) =>
        {
            let upper = name.to_ascii_uppercase();
            let dt = match upper.as_str() {
                "COUNT" | "BIT_AND" | "BIT_OR" | "BIT_XOR" => {
                    crate::storage::schema::DataTypeKind::BigInt
                }
                "AVG" | "STDDEV" | "STDDEV_POP" | "STDDEV_SAMP" | "VARIANCE" | "VAR_POP"
                | "VAR_SAMP" => crate::storage::schema::DataTypeKind::Double {
                    precision: None,
                    scale: None,
                },
                "GROUP_CONCAT" => crate::storage::schema::DataTypeKind::Text {
                    max_len: crate::storage::schema::DEFAULT_LOB_MAX,
                },
                // MIN/MAX/SUM mirror the argument's type when it's a column ref.
                _ => {
                    if let Some(first) = args.first() {
                        match first {
                            PExpr::Identifier(n) => {
                                return view_scope_column(db, current_database, select, n)
                                    .map(|(dt, _)| (dt, true));
                            }
                            PExpr::CompoundIdentifier(parts) => {
                                if let Some(n) = parts.last() {
                                    return view_scope_column(db, current_database, select, n)
                                        .map(|(dt, _)| (dt, true));
                                }
                            }
                            _ => {}
                        }
                    }
                    // SUM of an expression widens to double; fall back there.
                    crate::storage::schema::DataTypeKind::Double {
                        precision: None,
                        scale: None,
                    }
                }
            };
            // COUNT is never NULL; the rest can be NULL over an empty group.
            let nullable = upper != "COUNT";
            Some((dt, nullable))
        }
        // Everything else (arithmetic, comparisons, functions, CASE, CAST,
        // literals, IS NULL, BETWEEN, LIKE, …) has a statically classifiable
        // result type. Predicates / arithmetic are non-null unless an operand
        // forces nullability; we conservatively mark the column nullable when
        // any referenced column is nullable, else non-null for predicates and
        // nullable for value expressions (MySQL marks most expression columns
        // nullable). To stay safe we mark expression columns nullable.
        _ => {
            let resolve = |column_expr: &Expr| {
                let name = match column_expr {
                    Expr::Identifier(name) => name.as_str(),
                    Expr::CompoundIdentifier(parts) => parts.last()?.as_str(),
                    _ => return None,
                };
                let (dt, nullable) = view_scope_column(db, current_database, select, name)?;
                Some(crate::engine::expr_type::ExprType::from_data_type(
                    &dt, false, nullable, None, None,
                ))
            };
            let placeholder = |_: usize| None;
            let context =
                crate::engine::expr_type::TypeContext::context_free(&resolve, &placeholder);
            let descriptor = crate::engine::expr_type::infer_expr_type(expr, &context);
            let ct = if matches!(
                descriptor.kind,
                crate::engine::expr_type::ExprTypeKind::Unknown
            ) {
                crate::engine::expr::expr_result_type(expr)
            } else {
                descriptor.column_type()
            };
            let dt = if let crate::engine::expr_type::ExprTypeKind::Vector { dim, datatype } =
                descriptor.kind
            {
                crate::storage::DataTypeKind::Vector { dim, datatype }
            } else {
                crate::engine::executor::select::data_type_kind_for_column_type(ct)
            };
            // Expression columns in MySQL views are generally nullable (an
            // operand can be NULL); marking nullable is the safe, MySQL-aligned
            // default and avoids over-constraining downstream metadata.
            Some((dt, true))
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct MaterializedViewOptions {
    pub(super) algorithm: String,
    pub(super) definer_user: String,
    pub(super) definer_host: String,
    pub(super) security_type: RoutineSecuritySchema,
}

pub(super) fn materialize_view_options(
    session: &SessionContext,
    algorithm: Option<String>,
    definer: Option<AccountIdent>,
    security_type: Option<RoutineSecurity>,
) -> MaterializedViewOptions {
    let definer = definer.unwrap_or_else(|| AccountIdent {
        user: session.authenticated_as.user.clone(),
        host: session.authenticated_as.host.clone(),
    });
    MaterializedViewOptions {
        algorithm: normalize_view_algorithm(algorithm),
        definer_user: definer.user,
        definer_host: definer.host,
        security_type: view_security_schema(security_type.unwrap_or(RoutineSecurity::Definer)),
    }
}

pub(super) fn materialize_alter_view_options(
    session: &SessionContext,
    existing: &ViewSchema,
    algorithm: Option<String>,
    definer: Option<AccountIdent>,
    security_type: Option<RoutineSecurity>,
) -> MaterializedViewOptions {
    let definer_user = definer
        .as_ref()
        .map(|d| d.user.clone())
        .unwrap_or_else(|| existing.definer_user.clone());
    let definer_host = definer
        .as_ref()
        .map(|d| d.host.clone())
        .unwrap_or_else(|| existing.definer_host.clone());
    let security_type = security_type
        .map(view_security_schema)
        .unwrap_or_else(|| existing.security_type.clone());
    let _ = session;
    MaterializedViewOptions {
        algorithm: normalize_view_algorithm(algorithm.or_else(|| Some(existing.algorithm.clone()))),
        definer_user,
        definer_host,
        security_type,
    }
}

fn normalize_view_algorithm(algorithm: Option<String>) -> String {
    let upper = algorithm
        .unwrap_or_else(|| "UNDEFINED".to_string())
        .to_ascii_uppercase();
    match upper.as_str() {
        "MERGE" | "TEMPTABLE" | "UNDEFINED" => upper,
        _ => "UNDEFINED".to_string(),
    }
}

fn view_security_schema(security: RoutineSecurity) -> RoutineSecuritySchema {
    match security {
        RoutineSecurity::Definer => RoutineSecuritySchema::Definer,
        RoutineSecurity::Invoker => RoutineSecuritySchema::Invoker,
    }
}

fn ensure_view_definer_exists(
    db: &Arc<Database>,
    options: &MaterializedViewOptions,
) -> Result<(), QueryError> {
    let definer = AccountKey::new(&options.definer_user, &options.definer_host);
    let exists = db.with_auth_catalog(|auth| auth.accounts.contains_key(&account_id(&definer)))?;
    if exists {
        Ok(())
    } else {
        Err(error(
            crate::error::catalog::ER_NO_SUCH_USER,
            format!(
                "The user specified as a definer ({}) does not exist",
                definer.display()
            ),
        ))
    }
}

/// P3.2 — CREATE/ALTER VIEW variant of [`ensure_view_definer_exists`]: a
/// missing definer account produces warning 1449 instead of an error, so the
/// view is still created. MySQL allows defining a view whose DEFINER does not
/// yet exist (e.g. restoring a dump before the users are created); the error
/// only surfaces when the view is later read under DEFINER security.
pub(super) fn warn_if_view_definer_missing(
    db: &Arc<Database>,
    options: &MaterializedViewOptions,
) -> Result<(), QueryError> {
    let definer = AccountKey::new(&options.definer_user, &options.definer_host);
    let exists = db.with_auth_catalog(|auth| auth.accounts.contains_key(&account_id(&definer)))?;
    if !exists {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Note,
            crate::error::catalog::ER_NO_SUCH_USER,
            format!(
                "The user specified as a definer ({}) does not exist",
                definer.display()
            ),
        );
    }
    Ok(())
}
/// Look up `table` and decide whether it's a view we can transparently
/// rewrite into its underlying base table for INSERT/UPDATE/DELETE.
/// Returns `Ok(Some(plan))` for a simple-updatable view, `Ok(None)` if
/// `table` is a base table (no rewrite needed), or `Err(1288)` if the
/// target is a view but does not satisfy the simple-updatable contract.
///
/// Simple-updatable contract (mirrors MySQL's narrowest definition):
/// - exactly one base table in FROM, no JOINs, no derived
/// - no DISTINCT / GROUP BY / HAVING / windows
/// - no LIMIT / OFFSET / ORDER BY / FOR UPDATE
/// - every projection item is a bare column reference whose visible
///   name (alias or column name) matches the corresponding view column
///   name — i.e. no expression columns, no rename mismatch
/// - the view must not be invalid (1356 takes precedence)
struct UpdatablePlan {
    /// Qualified base table name as the parser/planner expect it
    /// (`database.table`).
    base_qualified: String,
    column_map: std::collections::HashMap<String, String>,
    insert_columns: Vec<String>,
    /// View-visible name and final base-column name in view declaration order.
    returning_columns: Vec<(String, String)>,
    /// Predicate from the view body (`WHERE ...`); when present, the
    /// caller must AND it onto the rewritten WHERE so the DML only
    /// affects rows the view exposes.
    view_predicate: Option<Expr>,
    /// Predicate evaluated against INSERT/UPDATE post-images when one
    /// or more views in the chain have WITH CHECK OPTION.
    check_predicate: Option<Expr>,
    /// Public error identity for DEFINER failures; never expose the base table.
    definer_view_name: Option<String>,
    /// Account whose privileges authorize DML against the final base table.
    auth_context: crate::engine::auth::AuthorizationContext,
}

fn resolve_updatable_view(
    db: &Arc<Database>,
    session: &SessionContext,
    table: &str,
) -> Result<Option<UpdatablePlan>, QueryError> {
    if db.is_base_table(&session.current_database, table) {
        return Ok(None);
    }
    let view = match db.view_definition(&session.current_database, table) {
        Some(v) => v,
        None => return Ok(None),
    };
    let mut trail = ViewTrail::new();
    let effective_account = effective_view_security_account(db, session, &view, None)?;
    resolve_updatable_view_schema(db, session, view, effective_account, &mut trail).map(Some)
}

fn resolve_updatable_view_schema(
    db: &Arc<Database>,
    session: &SessionContext,
    view: ViewSchema,
    effective_account: crate::engine::auth::AuthorizationContext,
    trail: &mut ViewTrail,
) -> Result<UpdatablePlan, QueryError> {
    let reject = |reason: &str| -> QueryError { reject_view_not_updatable(&view, reason) };
    if view.invalid {
        return Err(error(
            crate::error::catalog::ER_VIEW_INVALID,
            format!(
                "View '{}.{}' references invalid table(s) or column(s)",
                view.database, view.name
            ),
        ));
    }
    if view.algorithm.eq_ignore_ascii_case("TEMPTABLE") {
        return Err(reject("TEMPTABLE algorithm"));
    }
    if !trail.insert(trail_key(&view)) {
        return Err(error(
            crate::error::catalog::ER_VIEW_RECURSIVE,
            format!(
                "View '{}.{}' contains a recursive reference",
                view.database, view.name
            ),
        ));
    }
    let mut body = stored_view_query(&view)?;
    bind_view_current_user(
        &mut body,
        &effective_account.authenticated_as.current_user_value(),
    );
    simple_updatable_view_shape(db, &view.database, &body, &view.columns).map_err(reject)?;
    use crate::parser::{Expr as PExpr, SelectItem as PSelectItem};
    if body.projection.len() != view.columns.len() {
        return Err(reject(
            "projection length does not match stored view column metadata",
        ));
    }
    let mut direct_columns = Vec::with_capacity(body.projection.len());
    for (item, col) in body.projection.iter().zip(view.columns.iter()) {
        let PSelectItem::Expr { expr, alias } = item else {
            return Err(reject("wildcard or qualified-wildcard projection"));
        };
        let referenced_name = match expr {
            PExpr::Identifier(n) => n.clone(),
            PExpr::CompoundIdentifier(parts) => parts.last().cloned().unwrap_or_default(),
            _ => return Err(reject("expression column")),
        };
        // Visible name = alias if any, else the column name. It must
        // match the stored view metadata; DML callers are remapped from
        // this view-visible name back to the underlying base column.
        let visible_name = alias.clone().unwrap_or_else(|| referenced_name.clone());
        if !visible_name.eq_ignore_ascii_case(&col.name) {
            return Err(reject("view column rename does not match stored metadata"));
        }
        direct_columns.push((visible_name, referenced_name));
    }
    let source_name = &body.from[0].name;
    if let Some(inner_view) = db.view_definition(&view.database, source_name) {
        let inner_account =
            effective_view_security_account(db, session, &inner_view, Some(&effective_account))?;
        let inner = resolve_updatable_view_schema(db, session, inner_view, inner_account, trail)?;
        let mut column_map = std::collections::HashMap::new();
        let mut insert_columns = Vec::with_capacity(direct_columns.len());
        let mut returning_columns = Vec::with_capacity(direct_columns.len());
        for (visible_name, inner_name) in direct_columns {
            let Some(base_name) = inner
                .column_map
                .get(&inner_name.to_ascii_lowercase())
                .cloned()
            else {
                return Err(reject("view column does not map through nested view"));
            };
            column_map.insert(visible_name.to_ascii_lowercase(), base_name.clone());
            insert_columns.push(base_name.clone());
            returning_columns.push((visible_name, base_name));
        }
        let mut current_predicate = body.selection.clone();
        if let Some(predicate) = &mut current_predicate {
            if super::super::prepared_dml::expr_contains_subquery(predicate) {
                bind_view_predicate_outer_row(
                    db,
                    session,
                    &view.database,
                    &body.from[0],
                    predicate,
                    &inner.column_map,
                )?;
            } else {
                rewrite_expr_view_columns(predicate, &inner.column_map)?;
            }
        }
        let view_predicate = and_where(inner.view_predicate.clone(), current_predicate.clone());
        let check_predicate = match view.check_option.as_deref() {
            Some(option) if option.eq_ignore_ascii_case("LOCAL") => {
                and_where(inner.check_predicate.clone(), current_predicate)
            }
            Some(_) => and_where(inner.view_predicate.clone(), current_predicate),
            None => inner.check_predicate.clone(),
        };
        return Ok(UpdatablePlan {
            base_qualified: inner.base_qualified,
            column_map,
            insert_columns,
            returning_columns,
            view_predicate,
            check_predicate,
            definer_view_name: (view.security_type == RoutineSecuritySchema::Definer)
                .then(|| format!("{}.{}", view.database, view.name)),
            auth_context: inner.auth_context,
        });
    }
    if !db.is_base_table(&view.database, source_name) {
        return Err(reject(
            "view body does not reference a base table or simple view",
        ));
    }
    let mut column_map = std::collections::HashMap::new();
    let mut insert_columns = Vec::with_capacity(direct_columns.len());
    let mut returning_columns = Vec::with_capacity(direct_columns.len());
    for (visible_name, referenced_name) in direct_columns {
        column_map.insert(visible_name.to_ascii_lowercase(), referenced_name.clone());
        insert_columns.push(referenced_name.clone());
        returning_columns.push((visible_name, referenced_name));
    }
    let base_qualified = qualify_table_name_in_database(&view.database, source_name);
    if let Some(predicate) = &mut body.selection {
        if super::super::prepared_dml::expr_contains_subquery(predicate) {
            let table = db.table_required(&view.database, source_name)?;
            let identity = table
                .iter_visible_columns()
                .map(|(_, column)| (column.name.to_ascii_lowercase(), column.name.clone()))
                .collect();
            bind_view_predicate_outer_row(
                db,
                session,
                &view.database,
                &body.from[0],
                predicate,
                &identity,
            )?;
        }
    }
    let check_predicate = view
        .check_option
        .as_ref()
        .and_then(|_| body.selection.clone());
    Ok(UpdatablePlan {
        base_qualified,
        column_map,
        insert_columns,
        returning_columns,
        view_predicate: body.selection.clone(),
        check_predicate,
        definer_view_name: (view.security_type == RoutineSecuritySchema::Definer)
            .then(|| format!("{}.{}", view.database, view.name)),
        auth_context: effective_account,
    })
}

fn qualify_table_name_in_database(default_database: &str, name: &str) -> String {
    let (database, table) = crate::storage::split_table_name(default_database, name);
    format!("{database}.{table}")
}

pub(super) fn effective_view_security_account(
    db: &Arc<Database>,
    session: &SessionContext,
    view: &ViewSchema,
    inherited_invoker: Option<&crate::engine::auth::AuthorizationContext>,
) -> Result<crate::engine::auth::AuthorizationContext, QueryError> {
    if view.security_type == RoutineSecuritySchema::Definer {
        ensure_view_definer_exists(
            db,
            &MaterializedViewOptions {
                algorithm: view.algorithm.clone(),
                definer_user: view.definer_user.clone(),
                definer_host: view.definer_host.clone(),
                security_type: view.security_type.clone(),
            },
        )?;
        let auth = db.auth_catalog();
        let account = AccountKey::new(&view.definer_user, &view.definer_host);
        Ok(crate::engine::auth::AuthorizationContext {
            active_roles: auth.login_active_roles(&account).into(),
            authenticated_as: account,
            epoch: auth.epoch,
        })
    } else {
        Ok(inherited_invoker
            .cloned()
            .unwrap_or_else(|| session.authorization_context()))
    }
}

fn authorize_updatable_view_base_privilege(
    db: &Arc<Database>,
    session: &SessionContext,
    plan: &UpdatablePlan,
    privilege: Privilege,
) -> Result<(), QueryError> {
    let (database, table) =
        crate::storage::split_table_name(&session.current_database, &plan.base_qualified);
    let required = crate::engine::auth::RequiredPrivilege {
        privilege,
        scope: PrivilegeScope::Table { database, table },
    };
    let ctx = &plan.auth_context;
    let auth = db.auth_catalog();
    if auth.account_has_privilege(&ctx, &required, false) {
        Ok(())
    } else if let Some(view) = &plan.definer_view_name {
        Err(error(crate::error::catalog::ER_VIEW_INVALID,
            format!("View '{view}' references invalid table(s) or column(s) or definer/invoker of view lack rights to use them")))
    } else {
        Err(privilege_denied(&required.privilege, &required.scope))
    }
}

fn reject_view_not_updatable(view: &ViewSchema, reason: &str) -> QueryError {
    error(
        crate::error::catalog::ER_NON_UPDATABLE_TABLE,
        format!(
            "The target view '{}.{}' of the DML is not updatable: {}",
            view.database, view.name, reason
        ),
    )
}

/// Combine `existing` AND `extra` into a single WHERE expression.
fn and_where(existing: Option<Expr>, extra: Option<Expr>) -> Option<Expr> {
    match (existing, extra) {
        (Some(a), Some(b)) => Some(Expr::Binary {
            op: crate::parser::BinaryOp::And,
            left: Box::new(a),
            right: Box::new(b),
        }),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn resolve_insert_view(
    db: &Arc<Database>,
    session: &SessionContext,
    stmt: &crate::parser::Insert,
) -> Result<Option<UpdatablePlan>, QueryError> {
    let normal = resolve_updatable_view(db, session, &stmt.table);
    if !matches!(&normal, Err(err) if err.is(crate::error::catalog::ER_NON_UPDATABLE_TABLE)) {
        return normal;
    }
    let view = db
        .view_definition(&session.current_database, &stmt.table)
        .unwrap();
    let not_insertable = || {
        error(
            crate::error::catalog::ER_NON_INSERTABLE_TABLE,
            format!(
                "The target table '{}.{}' of the INSERT is not insertable-into",
                view.database, view.name
            ),
        )
    };
    let body = stored_view_query(&view)?;
    if view.algorithm.eq_ignore_ascii_case("TEMPTABLE")
        || body.joins.is_empty()
        || body.distinct
        || !body.group_by.is_empty()
        || body.having.is_some()
        || body.limit.is_some()
        || body.derived.is_some()
        || !body.named_windows.is_empty()
        || select_projection_has_aggregate(&body.projection)
        || body.joins.iter().any(|j| {
            j.derived.is_some()
                || j.natural
                || !j.using_columns.is_empty()
                || !matches!(
                    j.join_type,
                    crate::parser::JoinType::Inner | crate::parser::JoinType::Cross
                )
        })
        || view.check_option.is_some()
    {
        return Err(not_insertable());
    }
    let Some(requested) = &stmt.columns else {
        return Err(error(
            crate::error::catalog::ER_VIEW_NO_INSERT_FIELD_LIST,
            format!(
                "Can not insert into join view '{}.{}' without fields list",
                view.database, view.name
            ),
        ));
    };
    let mut context = session.clone();
    context.current_database = view.database.clone();
    let refs = body
        .from
        .iter()
        .chain(body.joins.iter().map(|j| &j.table))
        .collect::<Vec<_>>();
    let bindings = refs
        .iter()
        .map(|t| view_dml_binding(db, &context, t, None, None))
        .collect::<Result<Vec<_>, _>>()?;
    let mut projected = Vec::new();
    for (item, col) in body.projection.iter().zip(&view.columns) {
        let (q, name) = match item {
            SelectItem::Expr {
                expr: Expr::Identifier(n),
                ..
            } => (None, n),
            SelectItem::Expr {
                expr: Expr::CompoundIdentifier(p),
                ..
            } if p.len() >= 2 => (Some(p[..p.len() - 1].join(".")), p.last().unwrap()),
            _ => continue,
        };
        let owners = bindings
            .iter()
            .enumerate()
            .filter(|(_, b)| {
                b.columns.contains(&name.to_ascii_lowercase())
                    && q.as_ref()
                        .map_or(true, |q| b.names.iter().any(|n| n.eq_ignore_ascii_case(q)))
            })
            .map(|(i, _)| i)
            .collect::<Vec<_>>();
        if owners.len() != 1 {
            return Err(not_insertable());
        }
        projected.push((col.name.clone(), name.clone(), owners[0]));
    }
    let mut owner = None;
    for column in requested.iter().chain(
        stmt.on_duplicate_key_update
            .iter()
            .flatten()
            .map(|a| &a.column),
    ) {
        let Some((_, _, source)) = projected
            .iter()
            .find(|(v, _, _)| v.eq_ignore_ascii_case(column))
        else {
            if view
                .columns
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case(column))
            {
                return Err(not_insertable());
            }
            return Err(error(
                crate::error::catalog::ER_BAD_FIELD_ERROR,
                format!("Unknown column '{column}' in view"),
            ));
        };
        if owner.is_some_and(|i| i != *source) {
            return Err(error(
                crate::error::catalog::ER_VIEW_MULTIUPDATE,
                format!(
                    "Can not modify more than one base table through join view '{}.{}'",
                    view.database, view.name
                ),
            ));
        }
        owner = Some(*source);
    }
    let Some(owner) = owner else {
        return Err(not_insertable());
    };
    let source = refs[owner];
    let mut auth_context = effective_view_security_account(db, session, &view, None)?;
    let inner = if let Some(inner_view) = db.view_definition(&view.database, &source.name) {
        let account =
            effective_view_security_account(db, session, &inner_view, Some(&auth_context))?;
        Some(resolve_updatable_view_schema(
            db,
            session,
            inner_view,
            account,
            &mut ViewTrail::new(),
        )?)
    } else {
        None
    };
    let mut column_map = std::collections::HashMap::new();
    let mut returning_columns = Vec::new();
    let mut insert_columns = Vec::new();
    for (visible, mut base, i) in projected {
        if i != owner {
            continue;
        }
        if let Some(inner) = &inner {
            base = mapped_view_column(&inner.column_map, &base)?;
        }
        if insert_columns.contains(&base) {
            return Err(not_insertable());
        }
        insert_columns.push(base.clone());
        column_map.insert(visible.to_ascii_lowercase(), base.clone());
        returning_columns.push((visible, base));
    }
    let (base_qualified, check_predicate) = if let Some(inner) = inner {
        auth_context = inner.auth_context;
        (inner.base_qualified, inner.check_predicate)
    } else {
        (
            qualify_table_name_in_database(&view.database, &source.name),
            None,
        )
    };
    Ok(Some(UpdatablePlan {
        base_qualified,
        column_map,
        returning_columns,
        insert_columns,
        check_predicate,
        view_predicate: None,
        definer_view_name: (view.security_type == RoutineSecuritySchema::Definer)
            .then(|| format!("{}.{}", view.database, view.name)),
        auth_context,
    }))
}

pub(in crate::engine::executor) fn rewrite_insert_through_view(
    db: &Arc<Database>,
    session: &SessionContext,
    mut stmt: crate::parser::Insert,
) -> Result<crate::parser::Insert, QueryError> {
    let Some(plan) = resolve_insert_view(db, session, &stmt)? else {
        return Ok(stmt);
    };
    if let Some(assignments) = &stmt.on_duplicate_key_update {
        validate_view_dml_subqueries(
            db,
            session,
            &stmt.table,
            stmt.target_alias.as_deref(),
            assignments.iter().map(|a| &a.value),
        )?;
    }
    rewrite_returning_through_view(&mut stmt.returning, &plan)?;
    authorize_updatable_view_base_privilege(db, session, &plan, Privilege::Insert)?;
    // INSERT through a view bypasses the view's WHERE unless WITH CHECK
    // OPTION is present; the check predicate is evaluated below against
    // the post-image row.
    if let Some(columns) = &mut stmt.columns {
        for column in columns {
            *column = mapped_view_column(&plan.column_map, column)?;
        }
    } else {
        stmt.columns = Some(plan.insert_columns.clone());
    }
    if let Some(assignments) = &mut stmt.on_duplicate_key_update {
        authorize_updatable_view_base_privilege(db, session, &plan, Privilege::Update)?;
        for assignment in assignments {
            assignment.column = mapped_view_column(&plan.column_map, &assignment.column)?;
            rewrite_scoped_view_expr(
                db,
                session,
                &mut assignment.value,
                &stmt.table,
                stmt.target_alias.as_deref(),
                &plan.column_map,
            )?;
        }
    }
    stmt.view_check_predicate = and_where(stmt.view_check_predicate, plan.check_predicate.clone());
    let _ = plan.view_predicate;
    stmt.table = plan.base_qualified;
    Ok(stmt)
}

fn rewrite_scoped_view_expr(
    db: &Arc<Database>,
    session: &SessionContext,
    expr: &mut Expr,
    table: &str,
    alias: Option<&str>,
    mapping: &std::collections::HashMap<String, String>,
) -> Result<(), QueryError> {
    let binding = view_dml_binding(
        db,
        session,
        &authorization_table_ref(table, alias),
        None,
        Some(mapping.clone()),
    )?;
    let mut visitor = ViewDmlRewriter {
        db,
        session,
        scopes: vec![vec![binding]],
        aliases: Vec::new(),
        result: Ok(()),
    };
    crate::parser::visit::visit_expr_mut(&mut visitor, expr);
    visitor.result
}

/// Preserve lexical ownership of captured outer columns through view aliases
/// and renamed nested views. Inner subquery sources retain their own bindings.
fn bind_view_predicate_outer_row(
    db: &Arc<Database>,
    session: &SessionContext,
    database: &str,
    source: &TableRef,
    predicate: &mut Expr,
    mapping: &std::collections::HashMap<String, String>,
) -> Result<(), QueryError> {
    let mut context = session.clone();
    context.current_database = database.to_string();
    let mut binding = view_dml_binding(db, &context, source, None, Some(mapping.clone()))?;
    binding.alias = super::super::dml::VIEW_CHECK_ROW_ALIAS.to_string();
    let mut visitor = ViewDmlRewriter {
        db,
        session: &context,
        scopes: vec![vec![binding]],
        aliases: Vec::new(),
        result: Ok(()),
    };
    crate::parser::visit::visit_expr_mut(&mut visitor, predicate);
    visitor.result
}

/// Bind column rewrites to the original lexical namespaces. Inner query
/// bindings shadow outer bindings; base-table columns never inherit a view map.
struct ViewDmlBinding {
    names: Vec<String>,
    columns: std::collections::HashSet<String>,
    mapping: Option<std::collections::HashMap<String, String>>,
    expressions: Option<std::collections::HashMap<String, Expr>>,
    alias: String,
}

fn view_dml_binding(
    db: &Arc<Database>,
    session: &SessionContext,
    table: &TableRef,
    derived: Option<&SubQuery>,
    mapping: Option<std::collections::HashMap<String, String>>,
) -> Result<ViewDmlBinding, QueryError> {
    let alias = table.alias.clone().unwrap_or_else(|| {
        crate::storage::split_table_name(&session.current_database, &table.name).1
    });
    let names = if table.alias.is_some() {
        vec![alias.clone()]
    } else {
        vec![alias.clone(), table.name.clone()]
    };
    let columns = if let Some(mapping) = &mapping {
        mapping.keys().cloned().collect()
    } else if let Some(derived) = derived {
        crate::engine::executor::select::describe_subquery_columns(db, derived)?
            .into_iter()
            .map(|c| c.name.to_ascii_lowercase())
            .collect()
    } else if let Some(view) = db.view_definition(&session.current_database, &table.name) {
        view.columns
            .iter()
            .map(|c| c.name.to_ascii_lowercase())
            .collect()
    } else if is_reserved_system_table_name(&table.name) {
        crate::engine::information_schema::columns_for_table_name(&table.name)?
            .into_iter()
            .map(|c| c.name.to_ascii_lowercase())
            .collect()
    } else {
        db.table_required(&session.current_database, &table.name)?
            .columns
            .iter()
            .map(|c| c.name.to_ascii_lowercase())
            .collect()
    };
    Ok(ViewDmlBinding {
        names,
        columns,
        mapping,
        expressions: None,
        alias,
    })
}

struct ViewDmlRewriter<'a> {
    db: &'a Arc<Database>,
    session: &'a SessionContext,
    scopes: Vec<Vec<ViewDmlBinding>>,
    aliases: Vec<(usize, std::collections::HashSet<String>)>,
    result: Result<(), QueryError>,
}

impl crate::parser::visit::AstVisitorMut for ViewDmlRewriter<'_> {
    fn enter_select(&mut self, select: &mut Select) {
        let mut bindings = Vec::new();
        for (table, derived) in select
            .from
            .iter()
            .enumerate()
            .map(|(i, t)| {
                (
                    t,
                    if i == 0 {
                        select.derived.as_ref()
                    } else {
                        None
                    },
                )
            })
            .chain(select.joins.iter().map(|j| (&j.table, j.derived.as_ref())))
        {
            match view_dml_binding(self.db, self.session, table, derived, None) {
                Ok(binding) => bindings.push(binding),
                Err(err) => {
                    if self.result.is_ok() {
                        self.result = Err(err);
                    }
                }
            }
        }
        self.scopes.push(bindings);
    }
    fn exit_select(&mut self, _: &mut Select) {
        self.scopes.pop();
    }
    fn enter_select_alias_scope(&mut self, projection: &[SelectItem]) {
        let aliases = projection
            .iter()
            .filter_map(|item| match item {
                SelectItem::Expr {
                    alias: Some(alias), ..
                } => Some(alias.to_ascii_lowercase()),
                _ => None,
            })
            .collect();
        self.aliases.push((self.scopes.len(), aliases));
    }
    fn exit_select_alias_scope(&mut self) {
        self.aliases.pop();
    }
    fn enter_expr(&mut self, expr: &mut Expr) {
        if self.result.is_err() {
            return;
        }
        if let Expr::Identifier(name) = expr {
            if self.aliases.last().is_some_and(|(depth, names)| {
                *depth == self.scopes.len() && names.contains(&name.to_ascii_lowercase())
            }) {
                return;
            }
        }
        if matches!(expr, Expr::Function { .. }) {
            return;
        }
        let (qualifier, column) = match expr {
            Expr::Identifier(name) => (None, name.clone()),
            Expr::CompoundIdentifier(parts) | Expr::DefaultQualified(parts) if parts.len() >= 2 => {
                (
                    Some(parts[..parts.len() - 1].join(".")),
                    parts.last().unwrap().clone(),
                )
            }
            Expr::Default(Some(name)) => (None, name.clone()),
            _ => return,
        };
        for scope in self.scopes.iter().rev() {
            let candidates = scope
                .iter()
                .filter(|b| match &qualifier {
                    Some(q) => b.names.iter().any(|name| name.eq_ignore_ascii_case(q)),
                    None => b.columns.contains(&column.to_ascii_lowercase()),
                })
                .collect::<Vec<_>>();
            if candidates.is_empty() {
                continue;
            }
            if candidates.len() > 1 {
                self.result = Err(error(
                    crate::error::catalog::ER_NON_UNIQ_ERROR,
                    format!("Column '{column}' is ambiguous"),
                ));
                return;
            }
            let binding = candidates[0];
            if let Some(expressions) = &binding.expressions {
                match expressions.get(&column.to_ascii_lowercase()) {
                    Some(Expr::CompoundIdentifier(parts))
                        if matches!(expr, Expr::Default(_) | Expr::DefaultQualified(_)) =>
                    {
                        *expr = Expr::DefaultQualified(parts.clone());
                    }
                    Some(replacement) => *expr = replacement.clone(),
                    None => {
                        self.result = Err(error(
                            crate::error::catalog::ER_BAD_FIELD_ERROR,
                            format!("Unknown column '{column}' in view"),
                        ))
                    }
                }
                return;
            }
            if let Some(mapping) = &binding.mapping {
                match mapped_view_column(mapping, &column) {
                    Ok(base) => {
                        *expr = if matches!(expr, Expr::Default(_) | Expr::DefaultQualified(_)) {
                            Expr::DefaultQualified(vec![binding.alias.clone(), base])
                        } else {
                            Expr::CompoundIdentifier(vec![binding.alias.clone(), base])
                        };
                    }
                    Err(err) => self.result = Err(err),
                }
            }
            return;
        }
        // Only the outer view can supply an otherwise unbound column.
        // Reject hidden columns instead of allowing base-schema fallback.
        if self
            .scopes
            .first()
            .is_some_and(|scope| scope.len() == 1 && scope[0].mapping.is_some())
        {
            self.result = Err(error(
                crate::error::catalog::ER_BAD_FIELD_ERROR,
                format!("Unknown column '{column}' in view"),
            ));
        }
    }
}

fn qualify_view_predicate(predicate: &mut Option<Expr>, alias: &str) {
    struct Qualify<'a>(&'a str, usize);
    impl crate::parser::visit::AstVisitorMut for Qualify<'_> {
        fn enter_select(&mut self, _: &mut Select) {
            self.1 += 1;
        }
        fn exit_select(&mut self, _: &mut Select) {
            self.1 -= 1;
        }
        fn enter_expr(&mut self, expr: &mut Expr) {
            if let Expr::CompoundIdentifier(parts) = expr {
                if parts.len() == 2 && parts[0] == super::super::dml::VIEW_CHECK_ROW_ALIAS {
                    parts[0] = self.0.to_string();
                    return;
                }
            }
            if self.1 != 0 {
                return;
            }
            let name = match expr {
                Expr::Identifier(name) => Some(name.clone()),
                Expr::CompoundIdentifier(parts) => parts.last().cloned(),
                _ => None,
            };
            if let Some(name) = name {
                *expr = Expr::CompoundIdentifier(vec![self.0.into(), name]);
            }
        }
    }
    if let Some(expr) = predicate {
        crate::parser::visit::visit_expr_mut(&mut Qualify(alias, 0), expr);
    }
}

fn rewrite_view_update_delete(
    db: &Arc<Database>,
    session: &SessionContext,
    mut statement: Statement,
) -> Result<Statement, QueryError> {
    let (table, alias, joins) = match &statement {
        Statement::Update(s) => (&s.table, s.primary_alias.as_deref(), &s.joins),
        Statement::Delete(s) => (&s.table, s.primary_alias.as_deref(), &s.joins),
        _ => unreachable!(),
    };
    let refs = std::iter::once(authorization_table_ref(table, alias))
        .chain(joins.iter().map(|j| j.table.clone()))
        .collect::<Vec<_>>();
    // Resolve each relation once; cloning the catalog view just to detect
    // its presence duplicated work on every simple view UPDATE/DELETE.
    let plans = refs
        .iter()
        .map(|reference| resolve_updatable_view(db, session, &reference.name))
        .collect::<Result<Vec<_>, _>>()?;
    if plans.iter().all(Option::is_none) {
        return Ok(statement);
    }
    let mut bindings = Vec::with_capacity(refs.len());
    for (reference, plan) in refs.iter().zip(&plans) {
        bindings.push(view_dml_binding(
            db,
            session,
            reference,
            None,
            plan.as_ref().map(|p| p.column_map.clone()),
        )?);
    }
    let mut rewriter = ViewDmlRewriter {
        db,
        session,
        scopes: vec![bindings],
        aliases: Vec::new(),
        result: Ok(()),
    };
    if let Statement::Update(s) = &mut statement {
        for assignment in &mut s.assignments {
            let mut expr = if let Some(q) = &assignment.table_qualifier {
                Expr::CompoundIdentifier(vec![q.clone(), assignment.column.clone()])
            } else {
                Expr::Identifier(assignment.column.clone())
            };
            crate::parser::visit::visit_expr_mut(&mut rewriter, &mut expr);
            match expr {
                Expr::CompoundIdentifier(parts) => {
                    assignment.column = parts.last().unwrap().clone();
                    assignment.table_qualifier = Some(parts[..parts.len() - 1].join("."));
                }
                Expr::Identifier(name) => assignment.column = name,
                _ => unreachable!(),
            }
        }
    }
    let returning = match &mut statement {
        Statement::Update(s) => s.returning.take(),
        Statement::Delete(s) => s.returning.take(),
        _ => None,
    };
    crate::parser::visit::visit_statement_mut(&mut rewriter, &mut statement);
    match &mut statement {
        Statement::Update(s) => s.returning = returning,
        Statement::Delete(s) => s.returning = returning,
        _ => {}
    }
    rewriter.result?;
    for (index, plan) in plans.into_iter().enumerate() {
        let Some(mut plan) = plan else {
            continue;
        };
        let alias = rewriter.scopes[0][index].alias.clone();
        qualify_view_predicate(&mut plan.view_predicate, &alias);
        // Correlated checks keep their private lexical outer-row name;
        // selection predicates instead bind to the statement's table alias.
        if !plan
            .check_predicate
            .as_ref()
            .is_some_and(super::super::prepared_dml::expr_contains_subquery)
        {
            qualify_view_predicate(&mut plan.check_predicate, &alias);
        }
        match &mut statement {
            Statement::Update(s) => {
                let writes = s.assignments.iter().any(|a| {
                    a.table_qualifier
                        .as_ref()
                        .map_or(index == 0, |q| q.eq_ignore_ascii_case(&alias))
                });
                authorize_updatable_view_base_privilege(
                    db,
                    session,
                    &plan,
                    if writes {
                        Privilege::Update
                    } else {
                        Privilege::Select
                    },
                )?;
                if index == 0 {
                    rewrite_returning_through_view(&mut s.returning, &plan)?;
                    s.table = plan.base_qualified;
                    s.primary_alias = Some(alias);
                } else {
                    s.joins[index - 1].table.name = plan.base_qualified;
                    s.joins[index - 1].table.alias = Some(alias);
                }
                s.selection = and_where(s.selection.take(), plan.view_predicate);
                if writes {
                    s.view_check_predicate =
                        and_where(s.view_check_predicate.take(), plan.check_predicate);
                }
            }
            Statement::Delete(s) => {
                let writes = if s.targets.is_empty() {
                    index == 0
                } else {
                    s.targets.iter().any(|t| {
                        rewriter.scopes[0][index]
                            .names
                            .iter()
                            .any(|n| n.eq_ignore_ascii_case(t))
                    })
                };
                authorize_updatable_view_base_privilege(
                    db,
                    session,
                    &plan,
                    if writes {
                        Privilege::Delete
                    } else {
                        Privilege::Select
                    },
                )?;
                for target in &mut s.targets {
                    if rewriter.scopes[0][index]
                        .names
                        .iter()
                        .any(|n| n.eq_ignore_ascii_case(target))
                    {
                        *target = alias.clone();
                    }
                }
                if index == 0 {
                    rewrite_returning_through_view(&mut s.returning, &plan)?;
                    s.table = plan.base_qualified;
                    s.primary_alias = Some(alias);
                } else {
                    s.joins[index - 1].table.name = plan.base_qualified;
                    s.joins[index - 1].table.alias = Some(alias);
                }
                s.selection = and_where(s.selection.take(), plan.view_predicate);
            }
            _ => unreachable!(),
        }
    }
    Ok(statement)
}

/// Expand a mergeable join view into the existing joined UPDATE executor. Keep
/// column resolution in the view namespace, and retain the complete join so
/// nonmatching rows cannot be updated and duplicate matches are deduplicated.
fn rewrite_join_view_update(
    db: &Arc<Database>,
    session: &SessionContext,
    mut stmt: crate::parser::Update,
    view: &ViewSchema,
    mut body: Select,
) -> Result<crate::parser::Update, QueryError> {
    let reject = |reason| reject_view_not_updatable(view, reason);
    if view.invalid {
        return Err(error(
            crate::error::catalog::ER_VIEW_INVALID,
            "Invalid view",
        ));
    }
    if view.algorithm.eq_ignore_ascii_case("TEMPTABLE")
        || !mergeable_join_view_shape(db, &view.database, &body)
        || view.check_option.is_some()
        || !stmt.joins.is_empty()
        || stmt.returning.is_some()
    {
        return Err(reject("join view is not mergeable for UPDATE"));
    }
    let account = effective_view_security_account(db, session, view, None)?;
    let mut context = session.clone();
    context.current_database = view.database.clone();
    context.authenticated_as = account.authenticated_as.clone();
    context.active_roles = account.active_roles.clone();
    context.view_security_overrides.clear();
    let security_error = |err: QueryError| {
        if view.security_type == RoutineSecuritySchema::Definer
            && (err.is(crate::error::catalog::ER_TABLEACCESS_DENIED_ERROR)
                || err.is(crate::error::catalog::ER_COLUMNACCESS_DENIED_ERROR))
        {
            error(
                crate::error::catalog::ER_VIEW_INVALID,
                format!(
                    "View '{}.{}' definer lacks rights to use its base tables",
                    view.database, view.name
                ),
            )
        } else {
            err
        }
    };
    authorize_statement(db, &context, &Statement::Select(body.clone())).map_err(security_error)?;

    // Reserve every user namespace, including subqueries, before inventing
    // aliases. This prevents capture of correlated expressions after expansion.
    struct SelectNamespaces<'a>(&'a mut std::collections::HashSet<String>);
    impl crate::parser::visit::AstVisitorMut for SelectNamespaces<'_> {
        fn enter_select(&mut self, select: &mut Select) {
            // The table-use collector deliberately excludes derived/CTE aliases;
            // they still shadow correlated references and must be reserved.
            for table in select
                .from
                .iter()
                .chain(select.joins.iter().map(|j| &j.table))
            {
                self.0.insert(table.name.to_ascii_lowercase());
                if let Some(alias) = &table.alias {
                    self.0.insert(alias.to_ascii_lowercase());
                }
            }
        }
    }
    let mut used = std::collections::HashSet::new();
    for mut query in [
        Statement::Select(body.clone()),
        Statement::Update(stmt.clone()),
    ] {
        crate::parser::visit::visit_statement_mut(&mut SelectNamespaces(&mut used), &mut query);
        for usage in collect_statement_table_uses(&query) {
            used.insert(usage.table.to_ascii_lowercase());
            if let Some(alias) = usage.alias {
                used.insert(alias.to_ascii_lowercase());
            }
        }
    }
    let refs = body
        .from
        .iter()
        .chain(body.joins.iter().map(|j| &j.table))
        .cloned()
        .collect::<Vec<_>>();
    let mut aliases = Vec::new();
    let mut bindings = Vec::new();
    let mut counter = 0;
    for reference in &refs {
        let alias = loop {
            let candidate = format!("__view_update_{counter}");
            counter += 1;
            if used.insert(candidate.clone()) {
                break candidate;
            }
        };
        let mut binding = view_dml_binding(db, &context, reference, None, None)?;
        binding.mapping = Some(
            binding
                .columns
                .iter()
                .map(|c| (c.clone(), c.clone()))
                .collect(),
        );
        binding.alias = alias.clone();
        aliases.push(alias);
        bindings.push(binding);
    }
    let mut source_rewriter = ViewDmlRewriter {
        db,
        session: &context,
        scopes: vec![bindings],
        aliases: Vec::new(),
        result: Ok(()),
    };
    let mut projection = std::collections::HashMap::new();
    if body.projection.len() != view.columns.len() {
        return Err(reject("join projection metadata mismatch"));
    }
    for (item, column) in body.projection.iter_mut().zip(&view.columns) {
        let SelectItem::Expr { expr, .. } = item else {
            return Err(reject("unexpanded join wildcard"));
        };
        if !matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
            return Err(reject("expression column in join view"));
        }
        crate::parser::visit::visit_expr_mut(&mut source_rewriter, expr);
        projection.insert(column.name.to_ascii_lowercase(), expr.clone());
    }
    if let Some(predicate) = &mut body.selection {
        crate::parser::visit::visit_expr_mut(&mut source_rewriter, predicate);
    }
    for join in &mut body.joins {
        if let Some(condition) = &mut join.condition {
            crate::parser::visit::visit_expr_mut(&mut source_rewriter, condition);
        }
    }
    source_rewriter.result?;
    let mut binding = view_dml_binding(
        db,
        session,
        &authorization_table_ref(&stmt.table, stmt.primary_alias.as_deref()),
        None,
        Some(
            view.columns
                .iter()
                .map(|c| (c.name.to_ascii_lowercase(), c.name.clone()))
                .collect(),
        ),
    )?;
    binding.expressions = Some(projection);
    let mut rewriter = ViewDmlRewriter {
        db,
        session,
        scopes: vec![vec![binding]],
        aliases: Vec::new(),
        result: Ok(()),
    };
    let mut owner = None;
    for assignment in &mut stmt.assignments {
        let mut target = assignment.table_qualifier.as_ref().map_or_else(
            || Expr::Identifier(assignment.column.clone()),
            |q| Expr::CompoundIdentifier(vec![q.clone(), assignment.column.clone()]),
        );
        crate::parser::visit::visit_expr_mut(&mut rewriter, &mut target);
        if let Err(err) = &rewriter.result {
            return Err(err.clone());
        }
        let Expr::CompoundIdentifier(parts) = target else {
            return Err(reject("assignment is not a base column"));
        };
        let source = aliases
            .iter()
            .position(|a| a.eq_ignore_ascii_case(&parts[0]))
            .ok_or_else(|| reject("assignment does not resolve to a base table"))?;
        if owner.is_some_and(|i| i != source) {
            return Err(error(
                crate::error::catalog::ER_VIEW_MULTIUPDATE,
                format!(
                    "Can not modify more than one base table through join view '{}.{}'",
                    view.database, view.name
                ),
            ));
        }
        owner = Some(source);
        assignment.column = parts.last().unwrap().clone();
        assignment.table_qualifier = Some(aliases[source].clone());
    }
    let mut statement = Statement::Update(stmt);
    crate::parser::visit::visit_statement_mut(&mut rewriter, &mut statement);
    rewriter.result?;
    let Statement::Update(mut stmt) = statement else {
        unreachable!()
    };
    if let Some(owner) = owner {
        let (database, table) = crate::storage::split_table_name(&view.database, &refs[owner].name);
        let required = crate::engine::auth::RequiredPrivilege {
            privilege: Privilege::Update,
            scope: PrivilegeScope::Table { database, table },
        };
        if !db
            .auth_catalog()
            .account_has_privilege(&account, &required, false)
        {
            return Err(security_error(privilege_denied(
                &required.privilege,
                &required.scope,
            )));
        }
    }
    stmt.table = refs[0].name.clone();
    stmt.primary_alias = Some(aliases[0].clone());
    for (i, join) in body.joins.iter_mut().enumerate() {
        join.table.alias = Some(aliases[i + 1].clone());
    }
    stmt.joins = body.joins;
    stmt.selection = and_where(stmt.selection, body.selection);
    // Existing simple-view rewriting handles nested source views, including
    // their own security context and CHECK OPTION, before joined execution.
    let Statement::Update(stmt) =
        rewrite_view_update_delete(db, &context, Statement::Update(stmt))?
    else {
        unreachable!()
    };
    Ok(stmt)
}

pub(in crate::engine::executor) fn rewrite_update_through_view(
    db: &Arc<Database>,
    session: &SessionContext,
    stmt: crate::parser::Update,
) -> Result<crate::parser::Update, QueryError> {
    if db.is_base_table(&session.current_database, &stmt.table)
        && stmt
            .joins
            .iter()
            .all(|join| db.is_base_table(&session.current_database, &join.table.name))
    {
        return Ok(stmt);
    }
    if let Some(view) = db
        .view_definition(&session.current_database, &stmt.table)
        .filter(|view| !view.invalid)
    {
        let body = stored_view_query(&view)?;
        if !body.joins.is_empty() {
            return rewrite_join_view_update(db, session, stmt, &view, body);
        }
    }
    let Statement::Update(stmt) = rewrite_view_update_delete(db, session, Statement::Update(stmt))?
    else {
        unreachable!()
    };
    Ok(stmt)
}

pub(in crate::engine::executor) fn rewrite_delete_through_view(
    db: &Arc<Database>,
    session: &SessionContext,
    stmt: crate::parser::Delete,
) -> Result<crate::parser::Delete, QueryError> {
    if db.is_base_table(&session.current_database, &stmt.table)
        && stmt
            .joins
            .iter()
            .all(|join| db.is_base_table(&session.current_database, &join.table.name))
    {
        return Ok(stmt);
    }
    if let Some(view) = db
        .view_definition(&session.current_database, &stmt.table)
        .filter(|view| !view.invalid)
    {
        if !stored_view_query(&view)?.joins.is_empty() {
            return Err(error(
                crate::error::catalog::ER_VIEW_DELETE_MERGE_VIEW,
                format!(
                    "Can not delete from join view '{}.{}'",
                    view.database, view.name
                ),
            ));
        }
    }
    let Statement::Delete(stmt) = rewrite_view_update_delete(db, session, Statement::Delete(stmt))?
    else {
        unreachable!()
    };
    Ok(stmt)
}

fn rewrite_returning_through_view(
    returning: &mut Option<crate::parser::ReturningClause>,
    plan: &UpdatablePlan,
) -> Result<(), QueryError> {
    let Some(returning) = returning else {
        return Ok(());
    };
    let old_alias = returning.old_alias.as_deref().unwrap_or("old");
    let new_alias = returning.new_alias.as_deref().unwrap_or("new");
    let mut rewritten = Vec::new();
    for item in std::mem::take(&mut returning.projection) {
        match item {
            SelectItem::Wildcard => {
                rewritten.extend(plan.returning_columns.iter().map(|(visible, base)| {
                    SelectItem::Expr {
                        expr: Expr::Identifier(base.clone()),
                        alias: Some(visible.clone()),
                    }
                }));
            }
            SelectItem::QualifiedWildcard(q)
                if q.eq_ignore_ascii_case(old_alias) || q.eq_ignore_ascii_case(new_alias) =>
            {
                rewritten.extend(plan.returning_columns.iter().map(|(visible, base)| {
                    SelectItem::Expr {
                        expr: Expr::CompoundIdentifier(vec![q.clone(), base.clone()]),
                        alias: Some(visible.clone()),
                    }
                }));
            }
            SelectItem::QualifiedWildcard(_) => {
                rewritten.extend(plan.returning_columns.iter().map(|(visible, base)| {
                    SelectItem::Expr {
                        expr: Expr::Identifier(base.clone()),
                        alias: Some(visible.clone()),
                    }
                }));
            }
            SelectItem::Expr { mut expr, alias } => {
                rewrite_expr_view_columns(&mut expr, &plan.column_map)?;
                rewritten.push(SelectItem::Expr { expr, alias });
            }
        }
    }
    returning.projection = rewritten;
    Ok(())
}

fn validate_view_dml_subqueries<'a>(
    db: &Arc<Database>,
    session: &SessionContext,
    table: &str,
    alias: Option<&str>,
    expressions: impl Iterator<Item = &'a Expr>,
) -> Result<(), QueryError> {
    let projection = expressions
        .filter(|expr| expr_has_subquery(expr))
        .map(|expr| SelectItem::Expr {
            expr: expr.clone(),
            alias: None,
        })
        .collect::<Vec<_>>();
    if projection.is_empty() {
        return Ok(());
    }
    static TEMPLATE: std::sync::LazyLock<Select> = std::sync::LazyLock::new(|| {
        let Statement::Select(select) =
            crate::parser::parse_statement("SELECT * FROM __view_target").unwrap()
        else {
            unreachable!()
        };
        select
    });
    let mut select = TEMPLATE.clone();
    select.table = Some(table.to_string());
    select.from[0].name = table.to_string();
    select.from[0].alias = alias.map(str::to_string);
    select.projection = projection;
    expand_views_in_select(db, session, &mut select, 0, &mut ViewTrail::new())?;
    crate::engine::executor::qualification::qualify_select_tables(db, session, &mut select)?;
    crate::planner::validate_select_column_references(db, &select)
}

fn mapped_view_column(
    column_map: &std::collections::HashMap<String, String>,
    name: &str,
) -> Result<String, QueryError> {
    column_map
        .get(&name.to_ascii_lowercase())
        .cloned()
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_BAD_FIELD_ERROR,
                format!("Unknown column '{name}' in view"),
            )
        })
}

fn rewrite_expr_view_columns(
    expr: &mut Expr,
    column_map: &std::collections::HashMap<String, String>,
) -> Result<(), QueryError> {
    match expr {
        Expr::Identifier(name) => {
            *name = mapped_view_column(column_map, name)?;
        }
        Expr::CompoundIdentifier(parts) => {
            if let Some(last) = parts.last_mut() {
                *last = mapped_view_column(column_map, last)?;
            }
        }
        Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            rewrite_expr_view_columns(expr, column_map)?;
        }
        Expr::Binary { left, right, .. } => {
            rewrite_expr_view_columns(left, column_map)?;
            rewrite_expr_view_columns(right, column_map)?;
        }
        Expr::Like { expr, pattern, .. } => {
            rewrite_expr_view_columns(expr, column_map)?;
            rewrite_expr_view_columns(pattern, column_map)?;
        }
        Expr::InList { expr, list, .. } => {
            rewrite_expr_view_columns(expr, column_map)?;
            for item in list {
                rewrite_expr_view_columns(item, column_map)?;
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            rewrite_expr_view_columns(expr, column_map)?;
            rewrite_expr_view_columns(low, column_map)?;
            rewrite_expr_view_columns(high, column_map)?;
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(operand) = operand {
                rewrite_expr_view_columns(operand, column_map)?;
            }
            for expr in conditions.iter_mut().chain(results.iter_mut()) {
                rewrite_expr_view_columns(expr, column_map)?;
            }
            if let Some(expr) = else_result {
                rewrite_expr_view_columns(expr, column_map)?;
            }
        }
        Expr::Function { args: items, .. } | Expr::Tuple(items) => {
            for item in items {
                rewrite_expr_view_columns(item, column_map)?;
            }
        }
        Expr::InSubquery { expr, .. } | Expr::GroupKeyRef { expr, .. } => {
            rewrite_expr_view_columns(expr, column_map)?;
        }
        Expr::QuantifiedComparison { left, right, .. } => {
            rewrite_expr_view_columns(left, column_map)?;
            if let QuantifiedComparisonRight::List(items) = right {
                for item in items {
                    rewrite_expr_view_columns(item, column_map)?;
                }
            }
        }
        Expr::WindowFunction { args, spec, .. } => {
            let crate::parser::WindowSpec {
                partition_by,
                order_by,
                ..
            } = spec.as_mut();

            for item in args.iter_mut().chain(partition_by.iter_mut()) {
                rewrite_expr_view_columns(item, column_map)?;
            }
            for item in order_by {
                rewrite_expr_view_columns(&mut item.expr, column_map)?;
            }
        }
        Expr::Default(Some(name)) => {
            *name = mapped_view_column(column_map, name)?;
        }
        _ => {}
    }
    Ok(())
}

// =====================================================================
// P1-4: Vec window fast path
// =====================================================================
