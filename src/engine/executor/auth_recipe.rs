//! Column access recipes bound to catalog objects and lexical query scopes.
//! The shared parser visitor owns recursive expression traversal, so nested
//! queries cannot silently bypass access collection in a hand-written walker.
use super::*;
use crate::engine::auth::{
    AuthCatalog, AuthorizationContext, Privilege, PrivilegeScope, RequiredPrivilege,
};
use crate::parser::visit::{visit_query, visit_select, visit_statement, AstVisitor};
use std::collections::{BTreeMap, BTreeSet};
type Object = (String, String);
#[derive(Clone)]
struct Binding {
    object: Option<Object>,
    names: Vec<String>,
    columns: BTreeSet<String>,
    visible: BTreeSet<String>,
}
struct Scope {
    bindings: Vec<Binding>,
    aliases: BTreeSet<String>,
    order_aliases: bool,
}
pub(super) struct BoundAccessRecipe {
    reads: BTreeMap<Object, BTreeSet<String>>,
    available: BTreeMap<Object, BTreeSet<String>>,
    writes: BTreeMap<(Privilege, Object), BTreeSet<String>>,
}
struct Binder<'a> {
    db: &'a Database,
    database: &'a str,
    scopes: Vec<Scope>,
    ctes: Vec<BTreeMap<String, BTreeSet<String>>>,
    recipe: BoundAccessRecipe,
    complete: bool,
    root_bindings: Option<Vec<Binding>>,
    root_writes: BTreeMap<usize, BTreeSet<String>>,
}
impl BoundAccessRecipe {
    pub(super) fn build(
        db: &Database,
        session: &SessionContext,
        statement: &Statement,
    ) -> Option<Self> {
        if matches!(
            statement,
            Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
        ) {
            return Self::build_dml(db, session, statement);
        }
        if let Statement::CreateView { query, .. } | Statement::AlterView { query, .. } = statement
        {
            return Self::build_query(db, session, query);
        }
        if let Statement::CreateTableAsSelect { query, .. } = statement {
            return Self::build_query(db, session, query);
        }
        if !matches!(
            statement,
            Statement::Select(_)
                | Statement::WithCte(_)
                | Statement::SetOperation(_)
                | Statement::Explain { .. }
                | Statement::SelectIntoVars { .. }
                | Statement::SelectIntoFile { .. }
        ) {
            return None;
        }
        let mut binder = Binder::new(db, session);
        visit_statement(&mut binder, statement);
        binder.complete.then_some(binder.recipe)
    }
    fn build_query(
        db: &Database,
        session: &SessionContext,
        query: &crate::parser::QueryExpr,
    ) -> Option<Self> {
        let mut binder = Binder::new(db, session);
        visit_query(&mut binder, query);
        binder.complete.then_some(binder.recipe)
    }
    pub(super) fn requirements(&self) -> Vec<RequiredPrivilege> {
        self.reads
            .keys()
            .map(|(database, table)| RequiredPrivilege {
                privilege: Privilege::Select,
                scope: PrivilegeScope::Table {
                    database: database.clone(),
                    table: table.clone(),
                },
            })
            .chain(
                self.writes
                    .keys()
                    .map(|(privilege, (database, table))| RequiredPrivilege {
                        privilege: privilege.clone(),
                        scope: PrivilegeScope::Table {
                            database: database.clone(),
                            table: table.clone(),
                        },
                    }),
            )
            .collect()
    }
    fn build_dml(db: &Database, session: &SessionContext, statement: &Statement) -> Option<Self> {
        let mut binder = Binder::new(db, session);
        let mut select = Select::default();
        let (table, alias, joins, returning) = match statement {
            Statement::Update(u) => (
                &u.table,
                u.primary_alias.as_deref(),
                u.joins.clone(),
                u.returning.as_ref(),
            ),
            Statement::Delete(d) => (
                &d.table,
                d.primary_alias.as_deref(),
                d.joins.clone(),
                d.returning.as_ref(),
            ),
            Statement::Insert(i) => (
                &i.table,
                i.target_alias.as_deref(),
                Vec::new(),
                i.returning.as_ref(),
            ),
            _ => return None,
        };
        let primary = TableRef {
            name: table.clone(),
            alias: alias.map(str::to_string),
            system_time: None,
            index_hints: Vec::new(),
            partitions: Vec::new(),
            column_aliases: Vec::new(),
            generate_series: None,
        };
        let mut bindings = vec![binder.binding(&primary, None)?];
        for join in &joins {
            bindings.push(binder.binding(&join.table, join.derived.as_ref())?);
        }
        select.table = Some(table.clone());
        select.from = vec![primary];
        select.joins = joins;
        let mut assignments = Vec::new();
        match statement {
            Statement::Update(u) => {
                assignments = u.assignments.clone();
                select.selection = u.selection.clone();
                select.order_by = u.order_by.clone();
            }
            Statement::Delete(d) => {
                select.selection = d.selection.clone();
                select.order_by = d.order_by.clone();
                for (i, b) in bindings.iter().enumerate() {
                    if (d.targets.is_empty() && i == 0)
                        || d.targets.iter().any(|t| {
                            b.names
                                .contains(&t.trim_end_matches(".*").to_ascii_lowercase())
                        })
                    {
                        binder
                            .recipe
                            .writes
                            .entry((Privilege::Delete, b.object.clone()?))
                            .or_default();
                    }
                }
            }
            Statement::Insert(i) => {
                let columns = i
                    .columns
                    .as_ref()
                    .map(|c| c.iter().map(|c| c.to_ascii_lowercase()).collect())
                    .unwrap_or_else(|| bindings[0].visible.clone());
                binder
                    .recipe
                    .writes
                    .insert((Privilege::Insert, bindings[0].object.clone()?), columns);
                if i.replace {
                    binder
                        .recipe
                        .writes
                        .entry((Privilege::Delete, bindings[0].object.clone()?))
                        .or_default();
                }
                match &i.source {
                    InsertSource::Values(rows) => {
                        for expr in rows.iter().flatten() {
                            select.projection.push(SelectItem::Expr {
                                expr: expr.clone(),
                                alias: None,
                            });
                        }
                    }
                    InsertSource::Query(source) => visit_query(&mut binder, source),
                }
                assignments = i.on_duplicate_key_update.clone().unwrap_or_default();
                if let Some(alias) = &i.insert_alias {
                    let columns = if alias.column_aliases.is_empty() {
                        bindings[0].columns.clone()
                    } else {
                        alias
                            .column_aliases
                            .iter()
                            .map(|c| c.to_ascii_lowercase())
                            .collect()
                    };
                    bindings.push(Binding {
                        object: None,
                        names: vec![alias.row_alias.to_ascii_lowercase()],
                        visible: columns.clone(),
                        columns,
                    });
                }
            }
            _ => {}
        }
        for assignment in assignments {
            let name = assignment.column.to_ascii_lowercase();
            let matches: Vec<_> = bindings
                .iter()
                .enumerate()
                .filter(|(_, b)| {
                    b.object.is_some()
                        && b.columns.contains(&name)
                        && assignment
                            .table_qualifier
                            .as_ref()
                            .is_none_or(|q| b.names.contains(&q.to_ascii_lowercase()))
                })
                .map(|(i, _)| i)
                .collect();
            if matches.len() != 1 {
                return None;
            }
            let index = matches[0];
            binder
                .recipe
                .writes
                .entry((Privilege::Update, bindings[index].object.clone()?))
                .or_default()
                .insert(name.clone());
            binder.root_writes.entry(index).or_default().insert(name);
            select.projection.push(SelectItem::Expr {
                expr: assignment.value,
                alias: None,
            });
        }
        binder.root_bindings = Some(bindings.clone());
        visit_select(&mut binder, &select);
        let written: BTreeSet<_> = binder
            .recipe
            .writes
            .keys()
            .map(|(_, o)| o.clone())
            .collect();
        binder
            .recipe
            .reads
            .retain(|object, columns| !columns.is_empty() || !written.contains(object));
        if let Some(returning) = returning {
            binder.root_writes.clear();
            for b in &mut bindings {
                if b.object == bindings_object(statement, &binder.recipe.writes) {
                    b.names.push(
                        returning
                            .old_alias
                            .clone()
                            .unwrap_or_else(|| "old".into())
                            .to_ascii_lowercase(),
                    );
                    b.names.push(
                        returning
                            .new_alias
                            .clone()
                            .unwrap_or_else(|| "new".into())
                            .to_ascii_lowercase(),
                    );
                }
            }
            binder.root_bindings = Some(bindings);
            select.projection = returning.projection.clone();
            select.selection = None;
            select.order_by.clear();
            select.joins.clear();
            visit_select(&mut binder, &select);
        }
        binder.complete.then_some(binder.recipe)
    }

    /// Use column-denied diagnostics only when this account has column
    /// access to the object; accounts without access keep the table error.
    pub(super) fn denied_column(
        &self,
        auth: &AuthCatalog,
        ctx: &AuthorizationContext,
        required: &RequiredPrivilege,
    ) -> Option<String> {
        let PrivilegeScope::Table { database, table } = &required.scope else {
            return None;
        };
        let key = (database.to_ascii_lowercase(), table.to_ascii_lowercase());
        let allows = |column: &String| {
            auth.account_has_privilege(
                ctx,
                &RequiredPrivilege {
                    privilege: required.privilege.clone(),
                    scope: PrivilegeScope::Column {
                        database: database.clone(),
                        table: table.clone(),
                        column: column.clone(),
                    },
                },
                false,
            )
        };
        if !self.available.get(&key)?.iter().any(allows) {
            return None;
        }
        let columns = if required.privilege == Privilege::Select {
            self.reads.get(&key)
        } else {
            self.writes.get(&(required.privilege.clone(), key))
        }?;
        columns.iter().find(|column| !allows(column)).cloned()
    }

    pub(super) fn allows(
        &self,
        auth: &AuthCatalog,
        ctx: &AuthorizationContext,
        required: &RequiredPrivilege,
    ) -> bool {
        let PrivilegeScope::Table { database, table } = &required.scope else {
            return false;
        };
        let key = (database.to_ascii_lowercase(), table.to_ascii_lowercase());
        let columns = if matches!(required.privilege, Privilege::Select) {
            self.reads.get(&key)
        } else {
            self.writes.get(&(required.privilege.clone(), key.clone()))
        };
        let Some(columns) = columns else {
            return false;
        };
        let allows = |column: &String| {
            auth.account_has_privilege(
                ctx,
                &RequiredPrivilege {
                    privilege: required.privilege.clone(),
                    scope: PrivilegeScope::Column {
                        database: database.clone(),
                        table: table.clone(),
                        column: column.clone(),
                    },
                },
                false,
            )
        };
        if columns.is_empty() {
            self.available
                .get(&key)
                .is_some_and(|all| all.iter().any(allows))
        } else {
            columns.iter().all(allows)
        }
    }
}
impl<'a> Binder<'a> {
    fn new(db: &'a Database, session: &'a SessionContext) -> Self {
        Self {
            db,
            database: &session.current_database,
            scopes: Vec::new(),
            ctes: Vec::new(),
            recipe: BoundAccessRecipe {
                reads: BTreeMap::new(),
                available: BTreeMap::new(),
                writes: BTreeMap::new(),
            },
            complete: true,
            root_bindings: None,
            root_writes: BTreeMap::new(),
        }
    }
}
impl Binder<'_> {
    fn output_names<'q>(
        &self,
        statement: impl Into<crate::parser::QueryRef<'q>>,
    ) -> Option<BTreeSet<String>> {
        let statement = statement.into();
        match statement {
            crate::parser::QueryRef::Select(select) => self.select_output_names(select),
            crate::parser::QueryRef::Rrf(rrf) => {
                let mut names = self.output_names(&rrf.branches.first()?.query)?;
                names.extend(["rrf_score", "best_rank", "match_count"].map(String::from));
                if rrf.score_details {
                    names.insert("rrf_details".into());
                }
                Some(names)
            }
            crate::parser::QueryRef::SetOperation(set) => self.output_names(&set.left),
            crate::parser::QueryRef::WithCte(with) => self.output_names(&with.body),
            _ => None,
        }
    }
    fn select_output_names(&self, select: &Select) -> Option<BTreeSet<String>> {
        let mut out = BTreeSet::new();
        for item in &select.projection {
            match item {
                SelectItem::Expr {
                    alias: Some(alias), ..
                } => {
                    out.insert(alias.to_ascii_lowercase());
                }
                SelectItem::Expr {
                    expr: Expr::Identifier(name),
                    ..
                } => {
                    out.insert(name.to_ascii_lowercase());
                }
                SelectItem::Expr {
                    expr: Expr::CompoundIdentifier(parts),
                    ..
                } => {
                    out.insert(parts.last()?.to_ascii_lowercase());
                }
                SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => {
                    let qualifier = match item {
                        SelectItem::QualifiedWildcard(name) => Some(name.to_ascii_lowercase()),
                        _ => None,
                    };
                    let sources = select
                        .from
                        .iter()
                        .enumerate()
                        .map(|(i, source)| {
                            (
                                source,
                                if i == 0 {
                                    select.derived.as_ref()
                                } else {
                                    None
                                },
                            )
                        })
                        .chain(
                            select
                                .joins
                                .iter()
                                .map(|join| (&join.table, join.derived.as_ref())),
                        );
                    for (source, derived) in sources {
                        let binding = self.binding(source, derived)?;
                        if qualifier
                            .as_ref()
                            .is_none_or(|name| binding.names.contains(name))
                        {
                            out.extend(binding.visible);
                        }
                    }
                }
                _ => {}
            }
        }
        Some(out)
    }
    fn binding(&self, reference: &TableRef, derived: Option<&SubQuery>) -> Option<Binding> {
        let (database, table) = crate::storage::split_table_name(self.database, &reference.name);
        let names = vec![
            reference
                .alias
                .clone()
                .unwrap_or_else(|| table.clone())
                .to_ascii_lowercase(),
            reference.name.to_ascii_lowercase(),
            format!("{database}.{table}").to_ascii_lowercase(),
        ];
        if let Some(derived) = derived {
            let columns = match derived {
                SubQuery::Select(s) => self.select_output_names(s)?,
                SubQuery::Rrf(_) => self.output_names(derived)?,
                SubQuery::SetOp(s) => self.output_names(s)?,
                SubQuery::WithCte(with) => self.output_names(&with.body)?,
            };
            return Some(Binding {
                object: None,
                names,
                visible: columns.clone(),
                columns,
            });
        }
        if !reference.name.contains('.') {
            if let Some(columns) = self
                .ctes
                .iter()
                .rev()
                .find_map(|ctes| ctes.get(&reference.name.to_ascii_lowercase()))
            {
                return Some(Binding {
                    object: None,
                    names,
                    visible: columns.clone(),
                    columns: columns.clone(),
                });
            }
        }
        if let Some(cte) = super::cte::lookup_cte(&reference.name) {
            let columns: BTreeSet<_> = cte
                .columns
                .iter()
                .map(|c| c.name.to_ascii_lowercase())
                .collect();
            return Some(Binding {
                object: None,
                names,
                visible: columns.clone(),
                columns,
            });
        }
        let object = Some((database.to_ascii_lowercase(), table.to_ascii_lowercase()));
        if let Ok(schema) = self.db.table_in_database(&database, &table) {
            return Some(Binding {
                object,
                names,
                columns: schema
                    .columns
                    .iter()
                    .map(|c| c.name.to_ascii_lowercase())
                    .collect(),
                visible: schema
                    .columns
                    .iter()
                    .filter(|c| !c.invisible)
                    .map(|c| c.name.to_ascii_lowercase())
                    .collect(),
            });
        }
        if let Some(view) = self.db.view_definition(&database, &table) {
            let columns: BTreeSet<_> = view
                .columns
                .iter()
                .map(|c| c.name.to_ascii_lowercase())
                .collect();
            return Some(Binding {
                object,
                names,
                visible: columns.clone(),
                columns,
            });
        }
        if let Some(system) =
            crate::engine::information_schema::find_system_table(&database, &table)
        {
            let columns: BTreeSet<_> = system
                .columns
                .iter()
                .map(|c| c.name.to_ascii_lowercase())
                .collect();
            return Some(Binding {
                object: None,
                names,
                visible: columns.clone(),
                columns,
            });
        }
        None
    }
    fn reference(&mut self, qualifier: Option<&str>, column: &str) {
        let name = column.to_ascii_lowercase();
        let qualifier = qualifier.map(str::to_ascii_lowercase);
        for (depth, scope) in self.scopes.iter().enumerate().rev() {
            let bindings: Vec<_> = scope
                .bindings
                .iter()
                .enumerate()
                .filter(|(_, b)| {
                    qualifier.as_ref().is_none_or(|q| b.names.contains(q))
                        && b.columns.contains(&name)
                })
                .collect();
            if !bindings.is_empty() {
                for (index, binding) in bindings {
                    if depth == 0
                        && self
                            .root_writes
                            .get(&index)
                            .is_some_and(|columns| columns.contains(&name))
                    {
                        continue;
                    }
                    if let Some(object) = &binding.object {
                        self.recipe
                            .reads
                            .entry(object.clone())
                            .or_default()
                            .insert(name.clone());
                    }
                }
                return;
            }
            // A qualified alias in this scope shadows any identically named
            // outer alias, even when its requested column does not exist.
            if qualifier
                .as_ref()
                .is_some_and(|q| scope.bindings.iter().any(|b| b.names.contains(q)))
            {
                self.complete = false;
                return;
            }
        }
        if qualifier.is_none()
            && self
                .scopes
                .last()
                .is_some_and(|s| s.aliases.contains(&name))
        {
            return;
        }
        self.complete = false;
    }
}
impl AstVisitor for Binder<'_> {
    fn enter_statement(&mut self, statement: &Statement) {
        if matches!(statement, Statement::WithCte(_)) {
            self.ctes.push(BTreeMap::new());
        }
    }
    fn enter_query(&mut self, query: &crate::parser::QueryExpr) {
        if matches!(query, crate::parser::QueryExpr::WithCte(_)) {
            self.ctes.push(BTreeMap::new());
        }
    }
    fn exit_query(&mut self, query: &crate::parser::QueryExpr) {
        if matches!(query, crate::parser::QueryExpr::WithCte(_)) {
            self.ctes.pop();
        }
    }
    fn enter_cte(&mut self, cte: &crate::parser::CteDef) {
        if cte.is_recursive {
            self.exit_cte(cte);
        }
    }
    fn exit_cte(&mut self, cte: &crate::parser::CteDef) {
        let columns = if cte.columns.is_empty() {
            self.output_names(&cte.query).unwrap_or_default()
        } else {
            cte.columns.iter().map(|c| c.to_ascii_lowercase()).collect()
        };
        if let Some(names) = self.ctes.last_mut() {
            names.insert(cte.name.to_ascii_lowercase(), columns);
        }
    }
    fn exit_statement(&mut self, statement: &Statement) {
        if matches!(statement, Statement::WithCte(_)) {
            self.ctes.pop();
        }
    }
    fn enter_select(&mut self, select: &Select) {
        let aliases: BTreeSet<_> = select
            .projection
            .iter()
            .filter_map(|item| match item {
                SelectItem::Expr { alias: Some(a), .. } => Some(a.to_ascii_lowercase()),
                _ => None,
            })
            .collect();
        let mut bindings = Vec::new();
        for (i, reference) in select.from.iter().enumerate() {
            match self.binding(
                reference,
                if i == 0 {
                    select.derived.as_ref()
                } else {
                    None
                },
            ) {
                Some(binding) => bindings.push(binding),
                None => self.complete = false,
            }
        }
        for join in &select.joins {
            match self.binding(&join.table, join.derived.as_ref()) {
                Some(binding) => bindings.push(binding),
                None => self.complete = false,
            }
        }
        if self.scopes.is_empty() {
            if let Some(bound) = self.root_bindings.take() {
                bindings = bound;
            }
        }
        for binding in &bindings {
            if let Some(object) = &binding.object {
                self.recipe.reads.entry(object.clone()).or_default();
                self.recipe
                    .available
                    .entry(object.clone())
                    .or_default()
                    .extend(binding.columns.iter().cloned());
            }
        }
        for item in &select.projection {
            let qualifier = match item {
                SelectItem::Wildcard => Some(None),
                SelectItem::QualifiedWildcard(q) => Some(Some(q.to_ascii_lowercase())),
                _ => None,
            };
            if let Some(qualifier) = qualifier {
                for b in &bindings {
                    if qualifier.as_ref().is_none_or(|q| b.names.contains(q)) {
                        if let Some(object) = &b.object {
                            self.recipe
                                .reads
                                .entry(object.clone())
                                .or_default()
                                .extend(b.visible.iter().cloned());
                        }
                    }
                }
            }
        }
        // USING and NATURAL equality read both input columns, even when the
        // projection only exposes one side of the coalesced result.
        for join in &select.joins {
            for column in &join.using_columns {
                for b in &bindings {
                    if b.columns.contains(&column.to_ascii_lowercase()) {
                        if let Some(object) = &b.object {
                            self.recipe
                                .reads
                                .entry(object.clone())
                                .or_default()
                                .insert(column.to_ascii_lowercase());
                        }
                    }
                }
            }
            if join.natural {
                for b in &bindings {
                    for column in &b.columns {
                        if bindings
                            .iter()
                            .filter(|other| other.columns.contains(column))
                            .count()
                            > 1
                        {
                            if let Some(object) = &b.object {
                                self.recipe
                                    .reads
                                    .entry(object.clone())
                                    .or_default()
                                    .insert(column.clone());
                            }
                        }
                    }
                }
            }
        }
        self.scopes.push(Scope {
            bindings,
            aliases,
            order_aliases: false,
        });
    }
    fn enter_select_order_scope(&mut self, _: &[SelectItem]) {
        if let Some(scope) = self.scopes.last_mut() {
            scope.order_aliases = true;
        }
    }
    fn exit_select_alias_scope(&mut self) {
        if let Some(scope) = self.scopes.last_mut() {
            scope.order_aliases = false;
        }
    }
    fn walk_order_expr(&self, order: &crate::parser::OrderBy) -> bool {
        !self.scopes.last().is_some_and(|scope| {
            scope.order_aliases
                && matches!(&order.expr, Expr::Identifier(name)
                if scope.aliases.contains(&name.to_ascii_lowercase()))
        })
    }
    fn walk_expr_children(&self, expr: &Expr) -> bool {
        self.recipe.writes.is_empty()
            || !matches!(expr, Expr::Function { name, .. }
            if name.eq_ignore_ascii_case("VALUES"))
    }
    fn exit_select(&mut self, _select: &Select) {
        self.scopes.pop();
    }
    fn enter_expr(&mut self, expr: &Expr) {
        match expr {
            Expr::Identifier(name) => self.reference(None, name),
            Expr::CompoundIdentifier(parts) | Expr::DefaultQualified(parts) => {
                if let Some((name, qualifier)) = parts.split_last() {
                    self.reference(
                        (!qualifier.is_empty())
                            .then(|| qualifier.join("."))
                            .as_deref(),
                        name,
                    );
                }
            }
            Expr::Default(Some(name)) => self.reference(None, name),
            _ => {}
        }
    }
}

fn bindings_object(
    _statement: &Statement,
    writes: &BTreeMap<(Privilege, Object), BTreeSet<String>>,
) -> Option<Object> {
    writes.keys().next().map(|(_, o)| o.clone())
}
