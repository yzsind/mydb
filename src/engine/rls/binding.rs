//! Stable target-column references for policy expressions. Node ordinals use
//! the canonical parser visitor, including nested queries and their scopes.
use super::*;
use crate::parser::rls::PolicyColumnReference;
use crate::parser::visit::{visit_expr, visit_expr_mut, AstVisitor, AstVisitorMut};

#[derive(Default)]
struct Scope {
    names: Vec<String>,
    columns: std::collections::HashSet<String>,
    unknown: bool,
}

pub(crate) fn bind_target_columns(
    db: &Database,
    table: &TableSchema,
    expression: &Expr,
) -> Vec<PolicyColumnReference> {
    struct Binder<'a> {
        db: &'a Database,
        table: &'a TableSchema,
        scopes: Vec<Scope>,
        node: u32,
        columns: Vec<PolicyColumnReference>,
    }
    impl AstVisitor for Binder<'_> {
        fn enter_select(&mut self, select: &crate::parser::Select) {
            let mut scope = Scope::default();
            for source in select
                .from
                .iter()
                .chain(select.joins.iter().map(|j| &j.table))
            {
                if let Some(alias) = &source.alias {
                    scope.names.push(alias.to_ascii_lowercase());
                } else {
                    scope.names.push(source.name.to_ascii_lowercase());
                    scope.names.push(
                        source
                            .name
                            .rsplit('.')
                            .next()
                            .unwrap_or(&source.name)
                            .to_ascii_lowercase(),
                    );
                }
                if let Ok(referenced) = self.db.table(&source.name) {
                    scope.columns.extend(
                        referenced
                            .iter_visible_columns()
                            .map(|(_, c)| c.name.to_ascii_lowercase()),
                    );
                } else {
                    scope.unknown = true;
                }
            }
            self.scopes.push(scope);
        }
        fn exit_select(&mut self, _: &crate::parser::Select) {
            self.scopes.pop();
        }
        fn enter_expr(&mut self, expr: &Expr) {
            let node = self.node;
            self.node += 1;
            let name =
                match expr {
                    Expr::Identifier(name)
                        if !self.scopes.iter().any(|s| {
                            s.unknown || s.columns.contains(&name.to_ascii_lowercase())
                        }) =>
                    {
                        Some(name)
                    }
                    Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                        let qualifier = parts[..parts.len() - 1].join(".");
                        let target = qualifier.eq_ignore_ascii_case(&self.table.name)
                            || qualifier.eq_ignore_ascii_case(&self.table.qualified_name());
                        (target
                            && !self.scopes.iter().any(|s| {
                                s.names.iter().any(|n| n.eq_ignore_ascii_case(&qualifier))
                            }))
                        .then(|| parts.last().unwrap())
                    }
                    _ => None,
                };
            if let Some(column) = name.and_then(|name| self.table.column_index(name).ok()) {
                self.columns.push(PolicyColumnReference {
                    node,
                    column_id: self.table.columns[column].column_id,
                });
            }
        }
    }
    let mut binder = Binder {
        db,
        table,
        scopes: Vec::new(),
        node: 0,
        columns: Vec::new(),
    };
    visit_expr(&mut binder, expression);
    binder.columns
}

pub(crate) fn materialize_target_columns(
    table: &TableSchema,
    expression: &mut Expr,
    columns: &[PolicyColumnReference],
) -> Result<(), QueryError> {
    struct Resolve<'a> {
        table: &'a TableSchema,
        columns: std::slice::Iter<'a, PolicyColumnReference>,
        next: Option<&'a PolicyColumnReference>,
        node: u32,
        error: Option<QueryError>,
    }
    impl AstVisitorMut for Resolve<'_> {
        fn enter_expr(&mut self, expr: &mut Expr) {
            let node = self.node;
            self.node += 1;
            let Some(reference) = self.next.filter(|reference| reference.node == node) else {
                return;
            };
            self.next = self.columns.next();
            let Some(column) = self
                .table
                .column_id_map
                .get(&reference.column_id)
                .and_then(|index| self.table.columns.get(*index))
                .filter(|column| column.state == crate::storage::ColumnState::Public)
            else {
                self.error = Some(violation("Policy column identity changed or disappeared"));
                return;
            };
            match expr {
                Expr::Identifier(name) => *name = column.name.clone(),
                Expr::CompoundIdentifier(parts) => {
                    *parts = vec![
                        self.table.database.clone(),
                        self.table.name.clone(),
                        column.name.clone(),
                    ]
                }
                _ => self.error = Some(violation("Invalid policy column binding")),
            }
        }
    }
    let mut columns = columns.iter();
    let next = columns.next();
    let mut resolver = Resolve {
        table,
        columns,
        next,
        node: 0,
        error: None,
    };
    visit_expr_mut(&mut resolver, expression);
    if let Some(error) = resolver.error {
        return Err(error);
    }
    if resolver.next.is_some() {
        return Err(violation("Invalid policy column ordinal"));
    }
    Ok(())
}

pub(crate) fn materialize_policy(
    table: &TableSchema,
    policy: &mut Policy,
) -> Result<(), QueryError> {
    if let Some(expression) = &mut policy.using {
        materialize_target_columns(table, expression, &policy.using_columns)?;
    }
    if let Some(expression) = &mut policy.check {
        materialize_target_columns(table, expression, &policy.check_columns)?;
    }
    Ok(())
}

pub(crate) fn copied_security(
    source: &TableSchema,
    target: &TableSchema,
) -> Result<Arc<RowSecurity>, QueryError> {
    if source.row_security.version == 0 {
        return Ok(RowSecurity::empty());
    }
    let mut security = source.row_security.as_ref().clone();
    for policy in &mut security.policies {
        for reference in policy
            .using_columns
            .iter_mut()
            .chain(&mut policy.check_columns)
        {
            let old = source
                .column_id_map
                .get(&reference.column_id)
                .and_then(|i| source.columns.get(*i))
                .ok_or_else(|| violation("LIKE policy references a missing source column"))?;
            reference.column_id = target.columns[target.column_index(&old.name)?].column_id;
        }
        materialize_policy(target, policy)?;
    }
    Ok(Arc::new(security))
}
