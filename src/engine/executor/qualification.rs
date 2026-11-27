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
    crate::storage::table_key(&database, &table)
}

fn qualify_dml_source_name(session: &SessionContext, name: &str) -> String {
    if !name.contains('.') && super::cte::has_cte(name) {
        name.to_string()
    } else {
        qualify_table_name(session, name)
    }
}

fn validate_dml_expr_qualifiers(
    expr: &Expr,
    table: &str,
    alias: Option<&str>,
    proposed_alias: Option<&str>,
    clause: &str,
) -> Result<(), QueryError> {
    struct Check<'a> {
        table: &'a str,
        alias: Option<&'a str>,
        proposed: Option<&'a str>,
        clause: &'a str,
        result: Result<(), QueryError>,
    }
    impl crate::parser::visit::AstVisitor for Check<'_> {
        fn walk_query_children(&self, _: &crate::parser::QueryExpr) -> bool {
            false
        }
        fn walk_select_children(&self, _: &Select) -> bool {
            false
        }
        fn continue_expr_walk(&self) -> bool {
            self.result.is_ok()
        }
        fn enter_expr(&mut self, expr: &Expr) {
            if let Expr::CompoundIdentifier(parts) = expr {
                if parts.len() < 2 {
                    return;
                }
                let qualifier = &parts[parts.len() - 2];
                let (_, name) = split_table_name(DEFAULT_DATABASE, self.table);
                let accepted = self
                    .proposed
                    .is_some_and(|a| a.eq_ignore_ascii_case(qualifier))
                    || self.alias.map_or_else(
                        || name.eq_ignore_ascii_case(qualifier),
                        |a| a.eq_ignore_ascii_case(qualifier),
                    );
                if !accepted {
                    self.result = Err(crate::error::catalog::ER_BAD_FIELD_ERROR.error(format!(
                        "Unknown column '{}' in '{}'",
                        parts.join("."),
                        self.clause
                    )));
                }
            }
        }
    }
    let mut check = Check {
        table,
        alias,
        proposed: proposed_alias,
        clause,
        result: Ok(()),
    };
    crate::parser::visit::visit_expr(&mut check, expr);
    check.result
}

pub(super) fn qualify_statement_tables<'q>(
    db: &Database,
    session: &SessionContext,
    statement: impl Into<crate::parser::QueryMut<'q>>,
) -> Result<(), QueryError> {
    let statement = statement.into();
    crate::util::stack::with_stack(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::ParserTransient,
        || qualify_statement_tables_inner(db, session, statement),
    )?
}

fn qualify_statement_tables_inner(
    db: &Database,
    session: &SessionContext,
    statement: crate::parser::QueryMut<'_>,
) -> Result<(), QueryError> {
    match statement {
        crate::parser::QueryMut::Other(Statement::RowSecurity(command)) => {
            use crate::parser::rls::RlsStatement;
            match command {
                RlsStatement::Create { table, .. }
                | RlsStatement::Alter { table, .. }
                | RlsStatement::Drop { table, .. }
                | RlsStatement::Enable { table, .. }
                | RlsStatement::Show { table, .. } => {
                    *table = qualify_persistent_table_name(session, table)
                }
                _ => {}
            }
        }
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
                    // INSERT SELECT binds source references through its SELECT
                    // scope before executing ODKU. VALUES has no source scope.
                    if matches!(stmt.source, InsertSource::Values(_)) {
                        validate_dml_expr_qualifiers(
                            &assignment.value,
                            &stmt.table,
                            stmt.target_alias.as_deref(),
                            stmt.insert_alias.as_ref().map(|a| a.row_alias.as_str()),
                            "field list",
                        )?;
                    }
                    qualify_expr_tables(db, session, &mut assignment.value)?;
                }
            }
        }
        crate::parser::QueryMut::Select(stmt) => qualify_select_tables(db, session, stmt)?,
        crate::parser::QueryMut::Other(
            Statement::CreateTableAsSelect { query, .. }
            | Statement::CreateView { query, .. }
            | Statement::AlterView { query, .. },
        ) => {
            qualify_statement_tables(db, session, query)?;
        }
        crate::parser::QueryMut::Other(Statement::Do(expressions)) => {
            for expr in expressions {
                qualify_expr_tables(db, session, expr)?;
            }
        }
        crate::parser::QueryMut::Other(Statement::Update(stmt)) => {
            stmt.table = qualify_dml_source_name(session, &stmt.table);
            for join in &mut stmt.joins {
                if let Some(query) = &mut join.derived {
                    qualify_statement_tables(db, session, query)?;
                } else {
                    join.table.name = qualify_dml_source_name(session, &join.table.name);
                }
                if let Some(condition) = &mut join.condition {
                    qualify_expr_tables(db, session, condition)?;
                }
            }
            if stmt.joins.is_empty() {
                for assignment in &stmt.assignments {
                    validate_dml_expr_qualifiers(
                        &assignment.value,
                        &stmt.table,
                        stmt.primary_alias.as_deref(),
                        None,
                        "field list",
                    )?;
                }
                for expr in stmt
                    .selection
                    .iter()
                    .chain(stmt.order_by.iter().map(|o| &o.expr))
                {
                    validate_dml_expr_qualifiers(
                        expr,
                        &stmt.table,
                        stmt.primary_alias.as_deref(),
                        None,
                        "where clause",
                    )?;
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
            stmt.table = qualify_dml_source_name(session, &stmt.table);
            if stmt.joins.is_empty() {
                for expr in stmt
                    .selection
                    .iter()
                    .chain(stmt.order_by.iter().map(|o| &o.expr))
                {
                    validate_dml_expr_qualifiers(
                        expr,
                        &stmt.table,
                        stmt.primary_alias.as_deref(),
                        None,
                        "where clause",
                    )?;
                }
            }
            for join in &mut stmt.joins {
                if let Some(query) = &mut join.derived {
                    qualify_statement_tables(db, session, query)?;
                } else {
                    join.table.name = qualify_dml_source_name(session, &join.table.name);
                }
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
        crate::parser::QueryMut::Rrf(rrf) => {
            for branch in &mut rrf.branches {
                qualify_statement_tables(db, session, &mut branch.query)?;
            }
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
    fn qualified_source(
        table: &crate::parser::TableRef,
        derived: Option<&crate::parser::SubQuery>,
    ) -> bool {
        if table.name == "$nested_join_preserve" && table.alias.is_none() {
            return derived
                .and_then(crate::parser::SubQuery::leftmost_select)
                .is_some_and(qualified_block);
        }
        derived.is_none() && table.name.contains('.')
    }
    fn qualified_block(select: &Select) -> bool {
        select.from.iter().enumerate().any(|(index, table)| {
            qualified_source(
                table,
                if index == 0 {
                    select.derived.as_ref()
                } else {
                    None
                },
            )
        }) || select
            .joins
            .iter()
            .any(|join| qualified_source(&join.table, join.derived.as_ref()))
    }
    let check_bound_names = qualified_block(stmt);
    for table in &mut stmt.from {
        if stmt.derived.is_some() {
            continue;
        }
        if db.is_system_table_name(&session.current_database, &table.name) {
            table.name = qualify_table_name(session, &table.name);
            continue;
        }
        if super::cte::has_cte(&table.name) {
            if table.sample.is_some() {
                return Err(crate::error::catalog::ER_NOT_SUPPORTED_YET
                    .error("TABLESAMPLE requires a user base table"));
            }
            continue;
        }
        table.name = qualify_table_name(session, &table.name);
    }
    for join in &mut stmt.joins {
        if join.table.sample.is_some() && super::cte::has_cte(&join.table.name) {
            return Err(crate::error::catalog::ER_NOT_SUPPORTED_YET
                .error("TABLESAMPLE requires a user base table"));
        }
        if join.derived.is_none() && !super::cte::has_cte(&join.table.name) {
            join.table.name = qualify_table_name(session, &join.table.name);
        }
    }
    let mut qualifier = QueryQualifier::new(db, session, true);
    crate::parser::visit::visit_select_mut(&mut qualifier, stmt);
    qualifier.result?;
    if check_bound_names {
        crate::parser::validate_query_table_names(
            &stmt.from,
            stmt.derived.as_ref(),
            &stmt.joins,
            Some(&session.current_database),
        )
        .map_err(|err| err.spec.error(err.message))?;
    }
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
