use super::*;

/// Resolve ALTER's unqualified destination against its source schema once.
/// Authorization, namespace locking and execution must protect this same key.
pub(crate) fn qualify_alter_rename_target(
    session: &SessionContext,
    source: &str,
    destination: &str,
) -> String {
    let (database, _) = crate::storage::split_table_name(&session.current_database, source);
    let (database, table) = crate::storage::split_table_name(&database, destination);
    crate::storage::table_key(&database, &table)
}

pub(crate) fn qualify_table_name(session: &SessionContext, name: &str) -> String {
    if let Some(temp) = session.resolve_temp_table_name(name) {
        return temp;
    }
    qualify_persistent_table_name(session, name)
}

pub(crate) fn qualify_persistent_table_name(session: &SessionContext, name: &str) -> String {
    let (database, table) = split_table_name(&session.current_database, name);
    format!("{database}.{table}")
}

pub(super) fn qualify_statement_tables<'q>(
    db: &Database,
    session: &SessionContext,
    statement: impl Into<crate::parser::QueryMut<'q>>,
) -> Result<(), QueryError> {
    let statement = statement.into();
    match statement {
        crate::parser::QueryMut::Other(Statement::Insert(stmt)) => {
            stmt.table = qualify_table_name(session, &stmt.table);
            match &mut stmt.source {
                InsertSource::Values(rows) => {
                    for row in rows {
                        for expr in row {
                            qualify_expr_tables(db, session, expr)?;
                        }
                    }
                }
                InsertSource::Query(query) => qualify_statement_tables(db, session, query)?,
            }
            if let Some(assignments) = &mut stmt.on_duplicate_key_update {
                for assignment in assignments {
                    qualify_expr_tables(db, session, &mut assignment.value)?;
                }
            }
        }
        crate::parser::QueryMut::Select(stmt) => qualify_select_tables(db, session, stmt)?,
        crate::parser::QueryMut::Other(Statement::Update(stmt)) => {
            stmt.table = qualify_table_name(session, &stmt.table);
            for join in &mut stmt.joins {
                join.table.name = qualify_table_name(session, &join.table.name);
                if let Some(condition) = &mut join.condition {
                    qualify_expr_tables(db, session, condition)?;
                }
            }
            for assignment in &mut stmt.assignments {
                qualify_expr_tables(db, session, &mut assignment.value)?;
            }
            if let Some(selection) = &mut stmt.selection {
                qualify_expr_tables(db, session, selection)?;
            }
            for OrderBy { expr, .. } in &mut stmt.order_by {
                qualify_expr_tables(db, session, expr)?;
            }
        }
        crate::parser::QueryMut::Other(Statement::Delete(stmt)) => {
            stmt.table = qualify_table_name(session, &stmt.table);
            for join in &mut stmt.joins {
                join.table.name = qualify_table_name(session, &join.table.name);
                if let Some(condition) = &mut join.condition {
                    qualify_expr_tables(db, session, condition)?;
                }
            }
            if let Some(selection) = &mut stmt.selection {
                qualify_expr_tables(db, session, selection)?;
            }
            for OrderBy { expr, .. } in &mut stmt.order_by {
                qualify_expr_tables(db, session, expr)?;
            }
        }
        crate::parser::QueryMut::Other(Statement::CreateTrigger(def)) => {
            def.table = qualify_table_name(session, &def.table);
        }
        crate::parser::QueryMut::Other(Statement::Explain { target, .. }) => {
            qualify_statement_tables(db, session, target)?
        }
        crate::parser::QueryMut::Other(Statement::SelectIntoVars { inner: target, .. })
        | crate::parser::QueryMut::Other(Statement::SelectIntoFile { inner: target, .. }) => {
            qualify_statement_tables(db, session, target)?
        }
        crate::parser::QueryMut::SetOperation(stmt) => {
            qualify_statement_tables(db, session, &mut stmt.left)?;
            qualify_statement_tables(db, session, &mut stmt.right)?;
            for OrderBy { expr, .. } in &mut stmt.order_by {
                qualify_expr_tables(db, session, expr)?;
            }
        }
        crate::parser::QueryMut::WithCte(mut with) => {
            // Install CTE name stubs so qualify_select_tables recognises CTE
            // references and leaves them unqualified.
            let mut stubs: std::collections::HashMap<
                String,
                std::sync::Arc<crate::engine::api::CteResult>,
            > = std::collections::HashMap::new();
            for cte in with.ctes.iter() {
                stubs.insert(
                    cte.name.to_ascii_lowercase(),
                    std::sync::Arc::new(crate::engine::api::CteResult::materialized(
                        Vec::new(),
                        Vec::new(),
                    )),
                );
            }
            for cte in with.ctes.iter_mut() {
                let mut visible = stubs.clone();
                // Without RECURSIVE, a same-named base relation inside the
                // definition remains a base relation (and requires its own
                // privileges). Do not reinterpret it as a recursive edge.
                if !cte.is_recursive
                    && db.relation_name_exists(&qualify_table_name(session, &cte.name))
                {
                    visible.remove(&cte.name.to_ascii_lowercase());
                }
                super::cte::with_cte_registry_extended(visible, || {
                    qualify_statement_tables(db, session, &mut cte.query)
                })?;
            }
            super::cte::with_cte_registry_extended(stubs, || {
                qualify_statement_tables(db, session, &mut with.body)
            })?;
        }
        _ => {}
    }
    Ok(())
}

/// Qualify relation names in this scope, then let the shared AST walker find
/// every nested query. Query/CTE scopes are handled by the same dispatcher.
pub(super) fn qualify_select_tables(
    db: &Database,
    session: &SessionContext,
    stmt: &mut Select,
) -> Result<(), QueryError> {
    for table in &mut stmt.from {
        if stmt.derived.is_some() {
            continue;
        }
        if db.is_system_table_name(&session.current_database, &table.name) {
            table.name = qualify_table_name(session, &table.name);
            continue;
        }
        if super::cte::has_cte(&table.name) {
            continue;
        }
        table.name = qualify_table_name(session, &table.name);
    }
    for join in &mut stmt.joins {
        if join.derived.is_none() && !super::cte::has_cte(&join.table.name) {
            join.table.name = qualify_table_name(session, &join.table.name);
        }
    }
    let mut qualifier = QueryQualifier::new(db, session, true);
    crate::parser::visit::visit_select_mut(&mut qualifier, stmt);
    qualifier.result?;
    stmt.table = stmt.from.first().map(|table| table.name.clone());
    Ok(())
}

struct QueryQualifier<'a> {
    db: &'a Database,
    session: &'a SessionContext,
    descend_root: bool,
    select_depth: usize,
    result: Result<(), QueryError>,
}

impl<'a> QueryQualifier<'a> {
    fn new(db: &'a Database, session: &'a SessionContext, descend_root: bool) -> Self {
        Self {
            db,
            session,
            descend_root,
            select_depth: 0,
            result: Ok(()),
        }
    }
}

impl crate::parser::visit::AstVisitorMut for QueryQualifier<'_> {
    fn enter_query(&mut self, query: &mut crate::parser::QueryExpr) {
        if self.result.is_ok() {
            self.result = qualify_statement_tables(self.db, self.session, query);
        }
    }
    fn walk_query_children(&self, _: &crate::parser::QueryExpr) -> bool {
        // The dispatcher already processed this query within its CTE registry.
        false
    }
    fn enter_select(&mut self, select: &mut Select) {
        if self.result.is_ok() && !(self.descend_root && self.select_depth == 0) {
            self.result = qualify_select_tables(self.db, self.session, select);
        }
        self.select_depth += 1;
    }
    fn exit_select(&mut self, _: &mut Select) {
        self.select_depth -= 1;
    }
    fn walk_select_children(&self, _: &Select) -> bool {
        self.result.is_ok() && self.descend_root && self.select_depth == 1
    }
    fn walk_expr_children(&self, expr: &Expr) -> bool {
        self.result.is_ok() && !matches!(expr, Expr::Grouping(_))
    }
}

pub(super) fn qualify_expr_tables(
    db: &Database,
    session: &SessionContext,
    expr: &mut Expr,
) -> Result<(), QueryError> {
    if matches!(
        expr,
        Expr::Literal(_) | Expr::BoundValue(_) | Expr::Placeholder(_)
    ) {
        return Ok(());
    }
    let mut qualifier = QueryQualifier::new(db, session, false);
    crate::parser::visit::visit_expr_mut(&mut qualifier, expr);
    qualifier.result
}
