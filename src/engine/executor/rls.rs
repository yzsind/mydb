//! SQL administration and statement safety for row security.
use super::*;
use crate::engine::auth::Privilege;
use crate::parser::rls::{Command, Policy, RlsStatement};
use crate::parser::visit::{visit_expr, AstVisitor};

/// Feed policy calls through the normal resolver before planning. This gives
/// them the same EXECUTE checks, routine/table MDL, serial execution and atomic
/// statement ownership as functions written explicitly in the user's SQL.
pub(super) fn policy_function_expressions(
    db: &Database,
    session: &crate::engine::api::SessionContext,
    statement: &crate::parser::Statement,
) -> Vec<Expr> {
    use crate::engine::rls::{can_bypass, predicate};
    use crate::parser::Statement as S;
    let auth = crate::engine::rls::authorization_snapshot(db);
    let context = session.authorization_context();
    if can_bypass(&auth, &context, session.internal_system_session) {
        return Vec::new();
    }
    fn body(statement: &S) -> &S {
        match statement {
            S::WithCte(with) => body(&with.body),
            S::Explain { target, .. } => body(target),
            other => other,
        }
    }
    let mut expressions = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for usage in crate::engine::executor::dispatch::collect_statement_table_uses(statement) {
        let (database, name) =
            crate::storage::split_table_name(&session.current_database, &usage.table);
        let Ok(table) = db.table(&format!("{database}.{name}")) else {
            continue;
        };
        if !crate::engine::rls::might_require_binding(db, &table)
            || !seen.insert((table.id, usage.write))
        {
            continue;
        }
        let Ok(security) = crate::engine::rls::statement_security(db, &table) else {
            continue;
        };
        if !security.enabled {
            continue;
        }
        let mut required = Vec::new();
        if !usage.write {
            required.push((Command::Select, false));
        } else {
            match body(statement) {
                S::Insert(insert) => {
                    required.push((Command::Insert, true));
                    if insert.replace {
                        required.push((Command::Delete, false));
                    }
                    if insert.on_duplicate_key_update.is_some() {
                        required.extend([
                            (Command::Update, false),
                            (Command::Update, true),
                            (Command::Select, false),
                        ]);
                    }
                    if insert.returning.is_some() {
                        required.push((Command::Select, false));
                    }
                }
                S::Update(update) => {
                    required.extend([(Command::Update, false), (Command::Update, true)]);
                    if crate::engine::executor::rls::update_requires_read(
                        db, session, update, &table,
                    ) {
                        required.push((Command::Select, false));
                    }
                }
                S::Delete(delete) => {
                    required.push((Command::Delete, false));
                    if delete.selection.is_some() || delete.returning.is_some() {
                        required.push((Command::Select, false));
                    }
                }
                S::LoadData(load) => {
                    required.push((Command::Insert, true));
                    if load.replace {
                        required.push((Command::Delete, false));
                    }
                }
                _ => required.extend([(Command::Select, false), (Command::Update, false)]),
            }
        }
        for (command, check) in required {
            // Binding reports malformed dependencies before execution. This
            // preflight only discovers optional stored-function MDL recipes.
            let Ok(expression) = predicate(&auth, &context, &table, &security, command, check)
            else {
                continue;
            };
            if crate::parser::visit::any_expr(&expression, |node| match node {
                Expr::Function { name, .. }
                    if name.contains('.')
                        || !crate::engine::cbo::function_cost::is_known_builtin_function(name) =>
                {
                    db.routine_definition(
                        &table.database,
                        crate::storage::RoutineKindSchema::Function,
                        name,
                    )
                    .is_some()
                }
                _ => false,
            }) {
                expressions.push(expression);
            }
        }
    }
    expressions
}

pub(crate) fn authorize_policy_dependencies(
    db: &Database,
    auth: &crate::engine::auth::AuthCatalog,
    context: &AuthorizationContext,
    table: &TableSchema,
    expression: &Expr,
) -> Result<(), QueryError> {
    let mut has_query = false;
    struct Queries<'a>(&'a mut bool);
    impl AstVisitor for Queries<'_> {
        fn enter_select(&mut self, _: &Select) {
            *self.0 = true;
        }
    }
    visit_expr(&mut Queries(&mut has_query), expression);
    if !has_query {
        return Ok(());
    }
    let mut session = SessionContext::without_sysvar_inheritance();
    session.current_database = table.database.clone();
    session.authenticated_as = context.authenticated_as.clone();
    session.active_roles = context.active_roles.clone();
    let sql = format!(
        "SELECT 1 FROM `{}`.`{}`",
        table.database.replace('`', "``"),
        table.name.replace('`', "``")
    );
    let Statement::Select(mut wrapper) =
        crate::parser::parse_statement(&sql).map_err(|e| policy_error(e.message))?
    else {
        unreachable!()
    };
    wrapper.projection = vec![SelectItem::Expr {
        expr: expression.clone(),
        alias: None,
    }];
    let statement = Statement::Select(wrapper);
    let recipe = super::auth_recipe::BoundAccessRecipe::build(db, &session, &statement)
        .ok_or_else(|| policy_error("Cannot authorize policy query dependencies"))?;
    for required in recipe.requirements() {
        if matches!(&required.scope, crate::engine::auth::PrivilegeScope::Table { database, table: name } if database.eq_ignore_ascii_case(&table.database) && name.eq_ignore_ascii_case(&table.name))
        {
            continue;
        }
        if !auth.account_has_privilege(context, &required, false)
            && !recipe.allows(auth, context, &required)
        {
            return Err(policy_error(
                "Access denied to a table or column referenced by a row security policy",
            ));
        }
    }
    Ok(())
}

thread_local! {
    static POLICY_STACK: std::cell::RefCell<Vec<u64>> = const { std::cell::RefCell::new(Vec::new()) };
}

pub(crate) fn evaluate_subquery_policy(
    db: &Database,
    txn: TxnView,
    table: &TableSchema,
    expression: &Expr,
    values: &[Value],
) -> Result<bool, QueryError> {
    let handle = crate::engine::expr::row_security_database()
        .filter(|handle| std::ptr::eq(handle.as_ref(), db))
        .ok_or_else(|| {
            crate::engine::rls::violation("Policy subquery requires a statement execution context")
        })?;
    let recursive = POLICY_STACK.with(|stack| stack.borrow().contains(&table.id));
    if recursive {
        return Err(policy_error("Recursive row security policy"));
    }
    POLICY_STACK.with(|stack| stack.borrow_mut().push(table.id));
    struct Pop;
    impl Drop for Pop {
        fn drop(&mut self) {
            POLICY_STACK.with(|stack| {
                stack.borrow_mut().pop();
            });
        }
    }
    let _pop = Pop;
    super::dml::eval_view_check(&handle, txn, expression, table, values, None)
}

fn policy_error(message: impl Into<String>) -> QueryError {
    crate::error::catalog::ER_ROW_SECURITY_POLICY.error(message)
}

pub(crate) fn update_requires_read(
    db: &Database,
    session: &SessionContext,
    update: &crate::parser::Update,
    table: &TableSchema,
) -> bool {
    if update.selection.is_some() || update.returning.is_some() {
        return true;
    }
    let statement = Statement::Update(update.clone());
    let Some(recipe) = super::auth_recipe::BoundAccessRecipe::build(db, session, &statement) else {
        return true;
    };
    recipe.requirements().iter().any(|r| r.privilege == Privilege::Select && matches!(&r.scope, crate::engine::auth::PrivilegeScope::Table { database, table: name } if database.eq_ignore_ascii_case(&table.database) && name.eq_ignore_ascii_case(&table.name)))
}

pub(super) fn dml_filter(
    db: &Database,
    table: &TableSchema,
    command: Command,
    read_required: bool,
    user: Option<Expr>,
) -> Result<Option<Expr>, QueryError> {
    let Some(bound) = crate::engine::rls::binding(db, table, command, false)? else {
        return Ok(user);
    };
    let mut guarded = bound.guarded;
    let mut policy = bound.expression.clone();
    if read_required {
        if let Some(read) = crate::engine::rls::binding(db, table, Command::Select, false)? {
            guarded |= read.guarded;
            policy = crate::engine::rls::and(policy, read.expression.clone());
        }
    }
    if guarded {
        policy = Expr::Function {
            name: "__MYDB_RLS_GUARD".into(),
            args: vec![
                Expr::Literal(crate::parser::Literal::UInt(table.id)),
                policy,
            ],
            distinct: false,
        };
    }
    Ok(Some(crate::engine::rls::protect_table_filter(
        table, policy, user,
    )))
}

pub(super) fn enforce_policy_statement(statement: &Statement) -> Result<(), QueryError> {
    if !crate::engine::rls::policy_active() {
        return Ok(());
    }
    let effects = crate::statement_effects::statement_effects(statement);
    let locking =
        matches!(statement, Statement::Select(s) if s.for_update || s.lock_clause.is_some());
    if effects.writes_data
        || effects.changes_catalog
        || locking
        || !matches!(
            statement,
            Statement::Select(_)
                | Statement::WithCte(_)
                | Statement::SetOperation(_)
                | Statement::Do(_)
        )
    {
        return Err(crate::engine::rls::violation(
            "Policy functions may only execute read-only queries",
        ));
    }
    Ok(())
}

fn require(db: &Database, session: &SessionContext, name: &str) -> Result<(), QueryError> {
    if crate::engine::rls::trusted_internal()
        || crate::engine::rls::has_global(
            &crate::engine::rls::authorization_snapshot(db),
            &session.authorization_context(),
            Privilege::Dynamic(name.into()),
        )
    {
        return Ok(());
    }
    Err(policy_error(format!(
        "Access denied; {name} privilege is required"
    )))
}

pub(super) fn enforce_statement(
    db: &Database,
    session: &SessionContext,
    statement: &Statement,
) -> Result<(), QueryError> {
    let mut whole_table = Vec::new();
    let mut dropped = Vec::new();
    match statement {
        Statement::TruncateTable { name } => whole_table.push(name.as_str()),
        Statement::AlterTable(alter)
            if !alter.partition_actions.is_empty()
                || alter.actions.iter().any(|action| {
                    !matches!(
                        action,
                        crate::parser::AlterAction::AddIndex(_)
                            | crate::parser::AlterAction::DropIndex(_)
                            | crate::parser::AlterAction::RenameIndex { .. }
                            | crate::parser::AlterAction::AlterIndexVisibility { .. }
                            | crate::parser::AlterAction::SetColumnComment { .. }
                    )
                }) =>
        {
            whole_table.push(alter.table.as_str());
            for action in &alter.partition_actions {
                if let crate::parser_partition::PartitionAction::ExchangePartition {
                    table, ..
                } = action
                {
                    whole_table.push(table.as_str());
                }
            }
        }
        Statement::DropTable { names, .. } => dropped.extend(names.iter().map(String::as_str)),
        _ => return Ok(()),
    }
    if !whole_table.is_empty() {
        let auth = crate::engine::rls::authorization_snapshot(db);
        if !crate::engine::rls::can_bypass(
            &auth,
            &session.authorization_context(),
            session.internal_system_session,
        ) {
            for name in whole_table {
                let name = super::qualification::qualify_persistent_table_name(session, name);
                if let Ok(table) = db.table(&name) {
                    if crate::engine::rls::statement_security_enabled(db, &table)? {
                        return Err(crate::engine::rls::violation(
                        "Whole-table changes to an RLS-protected table require SUPER or BYPASS_RLS",
                    ));
                    }
                }
            }
        }
    }
    if !dropped.is_empty() {
        let ids: Vec<_> = dropped
            .iter()
            .filter_map(|name| {
                db.table(&super::qualification::qualify_persistent_table_name(
                    session, name,
                ))
                .ok()
                .map(|t| t.object_id)
            })
            .collect();
        let keys: Vec<_> = db
            .catalog()
            .tables
            .values()
            .filter(|t| t.row_security.version != 0 && !ids.contains(&t.object_id))
            .map(|t| t.qualified_name())
            .collect();
        for key in keys {
            let table = db.table(&key)?;
            if table
                .row_security
                .policies
                .iter()
                .any(|p| p.table_dependencies.iter().any(|(id, _)| ids.contains(id)))
            {
                return Err(policy_error("Table is referenced by a row security policy"));
            }
        }
    }
    Ok(())
}

fn validate_policy(
    db: &Database,
    session: &SessionContext,
    table: &TableSchema,
    policy: &mut Policy,
) -> Result<(), QueryError> {
    if policy.using.is_some() && policy.using_settings.is_none() {
        policy.using_settings = Some(Arc::new(
            crate::engine::expr::PolicyExpressionSettings::capture(session),
        ));
    }
    if policy.check.is_some() && policy.check_settings.is_none() {
        policy.check_settings = Some(Arc::new(
            crate::engine::expr::PolicyExpressionSettings::capture(session),
        ));
    }
    if policy.command == Command::Insert && policy.using.is_some()
        || matches!(policy.command, Command::Select | Command::Delete) && policy.check.is_some()
    {
        return Err(policy_error("Policy clauses are invalid for this command"));
    }
    let mut context = SessionContext::without_sysvar_inheritance();
    context.current_database = table.database.clone();
    policy.table_dependencies.clear();
    for expression in policy.using.iter_mut().chain(policy.check.iter_mut()) {
        super::qualification::qualify_expr_tables(db, &context, expression)?;
        let statement = Statement::Select(Select {
            rls_security_context: None,
            rls_expression_settings: None,
            projection: vec![SelectItem::Expr {
                expr: expression.clone(),
                alias: None,
            }],
            ..Select::default()
        });
        for usage in super::dispatch::collect_statement_table_uses(&statement) {
            let referenced = db.table(&usage.table)?;
            if referenced.id == table.id {
                return Err(policy_error(
                    "A policy cannot recursively query its own table",
                ));
            }
            if !policy
                .table_dependencies
                .iter()
                .any(|(id, _)| *id == referenced.object_id)
            {
                policy
                    .table_dependencies
                    .push((referenced.object_id, referenced.qualified_name()));
            }
        }
    }
    let auth = db.auth_catalog();
    for role in &policy.roles {
        if !auth
            .accounts
            .contains_key(&crate::engine::auth::account_id(role))
        {
            return Err(policy_error(format!(
                "Policy account {} does not exist",
                role.display()
            )));
        }
    }
    struct Validator<'a> {
        db: &'a Database,
        table: &'a TableSchema,
        depth: usize,
        error: Option<QueryError>,
    }
    impl AstVisitor for Validator<'_> {
        fn enter_limit(&mut self, limit: &crate::parser::LimitValue) {
            if !matches!(limit, crate::parser::LimitValue::Rows(_)) {
                self.error = Some(policy_error("Policy cannot reference parameters"));
            }
        }
        fn enter_select(&mut self, select: &Select) {
            self.depth += 1;
            if select.for_update || select.lock_clause.is_some() {
                self.error = Some(policy_error("Policy subqueries cannot request row locks"));
            }
        }
        fn exit_select(&mut self, _: &Select) {
            self.depth -= 1;
        }
        fn enter_expr(&mut self, expr: &Expr) {
            if self.error.is_some() {
                return;
            }
            match expr {
                Expr::Placeholder(_) | Expr::UserVariable(_) | Expr::SystemVariable { .. } => {
                    self.error = Some(policy_error(
                        "Policy cannot reference parameters or mutable session variables",
                    ))
                }
                Expr::Identifier(name) if self.depth == 0 => {
                    if let Err(e) = self.table.column_index(name) {
                        self.error = Some(e);
                    }
                }
                Expr::WindowFunction { .. } if self.depth == 0 => {
                    self.error = Some(policy_error(
                        "Aggregate/window functions require a policy subquery",
                    ));
                }
                Expr::Function { name, .. } => {
                    let upper = name.to_ascii_uppercase();
                    if upper.starts_with("HTTP_")
                        || upper.starts_with("AI_")
                        || [
                            crate::parser::USER_VAR_ASSIGN_FUNCTION,
                            "NEXTVAL",
                            "SETVAL",
                            "GET_LOCK",
                            "RELEASE_LOCK",
                            "RELEASE_ALL_LOCKS",
                            "SLEEP",
                            "BENCHMARK",
                            "LOAD_FILE",
                            "LAST_INSERT_ID",
                        ]
                        .iter()
                        .any(|f| upper.eq_ignore_ascii_case(f))
                    {
                        self.error = Some(policy_error("Policy functions must be read-only and free of external or session side effects"));
                    } else if self.depth == 0
                        && (crate::parser::is_aggregate_function(name)
                            || crate::planner::expr_has_window_function(expr))
                    {
                        self.error = Some(policy_error(
                            "Aggregate/window functions require a policy subquery",
                        ));
                    } else if let Some(routine) = self.db.routine_definition(
                        &self.table.database,
                        crate::storage::RoutineKindSchema::Function,
                        name,
                    ) {
                        if !matches!(
                            routine.sql_data_access,
                            crate::storage::RoutineSqlDataAccessSchema::NoSql
                                | crate::storage::RoutineSqlDataAccessSchema::ReadsSqlData
                        ) {
                            self.error = Some(policy_error(
                                "Policy stored functions must declare NO SQL or READS SQL DATA",
                            ));
                        }
                    }
                }
                _ => {}
            }
        }
    }
    let mut validator = Validator {
        db,
        table,
        depth: 0,
        error: None,
    };
    for expr in policy.using.iter().chain(&policy.check) {
        visit_expr(&mut validator, expr);
    }
    if let Some(error) = validator.error {
        return Err(error);
    }
    struct BindFunctions<'a>(&'a Database, &'a str);
    impl crate::parser::visit::AstVisitorMut for BindFunctions<'_> {
        fn enter_expr(&mut self, expression: &mut Expr) {
            if let Expr::Function { name, .. } = expression {
                if !name.contains('.')
                    && crate::engine::cbo::function_cost::is_known_builtin_function(name)
                {
                    return;
                }
                if let Some(routine) = self.0.routine_definition(
                    self.1,
                    crate::storage::RoutineKindSchema::Function,
                    name,
                ) {
                    *name = format!("{}.{}", routine.database, routine.name);
                }
            }
        }
    }
    for expression in policy.using.iter_mut().chain(policy.check.iter_mut()) {
        crate::parser::visit::visit_expr_mut(&mut BindFunctions(db, &table.database), expression);
    }
    policy.using_columns = policy.using.as_ref().map_or_else(Vec::new, |expression| {
        crate::engine::rls::bind_target_columns(db, table, expression)
    });
    policy.check_columns = policy.check.as_ref().map_or_else(Vec::new, |expression| {
        crate::engine::rls::bind_target_columns(db, table, expression)
    });
    Ok(())
}

pub(super) fn execute(
    db: &Arc<Database>,
    session: &mut SessionContext,
    command: RlsStatement,
) -> Result<QueryResponse, QueryError> {
    match command {
        RlsStatement::SetContext { local, key, value } => {
            require(db, session, "RLS_CONTEXT_ADMIN")?;
            if super::routines::external_calls_forbidden() {
                return Err(policy_error(
                    "RLS context can only be changed by a top-level statement",
                ));
            }
            if local && !session.explicit_txn {
                return Err(policy_error(
                    "SET LOCAL RLS CONTEXT requires an explicit transaction",
                ));
            }
            let value = crate::engine::expr::eval_expr(
                &value,
                &TableSchema::system_table(0, "", Vec::new()),
                &[],
            )?;
            let text = if matches!(value, Value::Null) {
                None
            } else {
                Some(crate::storage::value_to_string(&value))
            };
            session.rls.set(key, text, local)?;
            Ok(QueryResponse::Ok)
        }
        RlsStatement::ResetContext { key } => {
            require(db, session, "RLS_CONTEXT_ADMIN")?;
            if super::routines::external_calls_forbidden() {
                return Err(policy_error(
                    "RLS context can only be changed by a top-level statement",
                ));
            }
            session.rls.reset(key.as_deref());
            Ok(QueryResponse::Ok)
        }
        command => {
            require(db, session, "ROW_SECURITY_ADMIN")?;
            let name = match &command {
                RlsStatement::Create { table, .. }
                | RlsStatement::Alter { table, .. }
                | RlsStatement::Drop { table, .. }
                | RlsStatement::Enable { table, .. }
                | RlsStatement::Show { table, .. } => table,
                _ => unreachable!(),
            };
            let (database, name) =
                crate::storage::split_table_name(&session.current_database, name);
            let qualified = format!("{database}.{name}");
            let table = db.table(&qualified)?;
            if table.system
                || database == "__mydb_temp"
                || session.temp_tables.contains_key(&name.to_ascii_lowercase())
            {
                return Err(policy_error(
                    "Row security requires a persistent user base table",
                ));
            }
            if let RlsStatement::Show { name, .. } = &command {
                let policy = table
                    .row_security
                    .policies
                    .iter()
                    .find(|p| p.name.eq_ignore_ascii_case(name))
                    .ok_or_else(|| policy_error("Policy does not exist"))?;
                let mut policy = policy.clone();
                crate::engine::rls::materialize_policy(&table, &mut policy)?;
                return Ok(QueryResponse::ResultSet(ResultSet {
                    columns: vec![
                        Column {
                            name: "Policy".into(),
                            column_type: crate::types::ColumnType::VarString,
                            source: Default::default(),
                        },
                        Column {
                            name: "Create Policy".into(),
                            column_type: crate::types::ColumnType::VarString,
                            source: Default::default(),
                        },
                    ],
                    rows: vec![vec![
                        Value::String(policy.name.clone()),
                        Value::String(policy_sql(&qualified, &policy)?),
                    ]],
                }));
            }
            super::tx::implicit_commit_for_ddl(db, session)?;
            db.acquire_mdl(
                session.connection_id as u64,
                Some(session.connection_id as u64),
                MdlDuration::Statement,
                vec![MdlRequest {
                    key: MdlObjectKey::table(&database, &name),
                    lock_type: MdlLockType::SharedUpgradable,
                }],
                "RLS DDL",
            )?;
            struct ReleaseMdl<'a>(&'a Database, u64);
            impl Drop for ReleaseMdl<'_> {
                fn drop(&mut self) {
                    self.0.release_mdl_duration(self.1, MdlDuration::Statement);
                }
            }
            let _mdl = ReleaseMdl(db, session.connection_id as u64);
            let _auth_mutation = crate::engine::auth::catalog_mutation_lock()
                .lock()
                .map_err(|_| policy_error("Authorization mutation lock poisoned"))?;
            let table = db.table(&qualified)?;
            let mut security = (*table.row_security).clone();
            match command {
                RlsStatement::Create { mut policy, .. } => {
                    if security
                        .policies
                        .iter()
                        .any(|p| p.name.eq_ignore_ascii_case(&policy.name))
                    {
                        return Err(policy_error("Policy already exists"));
                    }
                    validate_policy(db, session, &table, &mut policy)?;
                    security.policies.push(policy);
                }
                RlsStatement::Alter {
                    name,
                    roles,
                    using,
                    check,
                    rename,
                    ..
                } => {
                    if rename.as_ref().is_some_and(|n| {
                        security
                            .policies
                            .iter()
                            .any(|p| p.name.eq_ignore_ascii_case(n))
                    }) {
                        return Err(policy_error("Policy already exists"));
                    }
                    let policy = security
                        .policies
                        .iter_mut()
                        .find(|p| p.name.eq_ignore_ascii_case(&name))
                        .ok_or_else(|| policy_error("Policy does not exist"))?;
                    crate::engine::rls::materialize_policy(&table, policy)?;
                    if let Some((public, roles)) = roles {
                        policy.public = public;
                        policy.roles = roles;
                    }
                    if let Some(using) = using {
                        policy.using = Some(using);
                        policy.using_settings = None;
                    }
                    if let Some(check) = check {
                        policy.check = Some(check);
                        policy.check_settings = None;
                    }
                    if let Some(rename) = rename {
                        policy.name = rename;
                    }
                    validate_policy(db, session, &table, policy)?;
                }
                RlsStatement::Drop {
                    name, if_exists, ..
                } => {
                    let before = security.policies.len();
                    security
                        .policies
                        .retain(|p| !p.name.eq_ignore_ascii_case(&name));
                    if before == security.policies.len() && !if_exists {
                        return Err(policy_error("Policy does not exist"));
                    }
                }
                RlsStatement::Enable { enabled, .. } => security.enabled = enabled,
                _ => unreachable!(),
            }
            security.version = security
                .version
                .checked_add(1)
                .ok_or_else(|| policy_error("Policy version exhausted"))?;
            db.alter_row_security(&table, security)?;
            Ok(QueryResponse::Ok)
        }
    }
}

fn policy_sql(table: &str, p: &Policy) -> Result<String, QueryError> {
    let ident = |s: &str| format!("`{}`", s.replace('`', "``"));
    let roles = if p.public {
        "PUBLIC".to_string()
    } else {
        p.roles
            .iter()
            .map(|r| r.role_sql_name())
            .collect::<Vec<_>>()
            .join(", ")
    };
    let mut sql = format!(
        "CREATE POLICY {} ON {} AS {} FOR {:?} TO {}",
        ident(&p.name),
        table.split('.').map(ident).collect::<Vec<_>>().join("."),
        if p.permissive {
            "PERMISSIVE"
        } else {
            "RESTRICTIVE"
        },
        p.command,
        roles
    );
    if let Some(expr) = &p.using {
        sql.push_str(&format!(
            " USING ({})",
            crate::parser::try_query_expr_to_sql(expr)
                .map_err(|error| policy_error(error.to_string()))?
        ));
    }
    if let Some(expr) = &p.check {
        sql.push_str(&format!(
            " WITH CHECK ({})",
            crate::parser::try_query_expr_to_sql(expr)
                .map_err(|error| policy_error(error.to_string()))?
        ));
    }
    Ok(sql)
}
