//! External effects execute after the pure relational input is complete.
use super::*;
use crate::engine::ai::sql::{expr_has_ai, statement_has_ai, AiStatement};
use crate::engine::ai::{self, AiCatalog};

fn fail(message: &str) -> QueryError {
    ai::error("ER_AI_UNSUPPORTED_CONTEXT", message)
}
fn name(session: &SessionContext, value: &str) -> String {
    if value.contains('.') {
        value.to_ascii_lowercase()
    } else {
        format!(
            "{}.{}",
            session.current_database.to_ascii_lowercase(),
            value.to_ascii_lowercase()
        )
    }
}
fn model_in_schema(qualified_name: &str, schema: &str) -> bool {
    schema.is_empty()
        || qualified_name
            .rsplit_once('.')
            .is_some_and(|(actual, _)| actual == schema)
}
fn next_id(c: &mut AiCatalog) -> Result<u64, QueryError> {
    c.next_id = c
        .next_id
        .checked_add(1)
        .ok_or_else(|| ai::error("ER_AI_BUDGET_EXCEEDED", "AI object IDs exhausted"))?;
    Ok(c.next_id)
}
fn result(names: &[&str], rows: Vec<Vec<Value>>) -> QueryResponse {
    QueryResponse::ResultSet(ResultSet {
        columns: names
            .iter()
            .map(|n| Column {
                name: (*n).into(),
                column_type: ColumnType::VarString,
                source: Default::default(),
            })
            .collect(),
        rows,
    })
}
fn json_config<T: serde::de::DeserializeOwned>(a: &AiStatement) -> Result<T, QueryError> {
    let value = ai::parse_json(a.config.as_deref().unwrap_or("{}"), 65536)?;
    if value.as_object().is_some_and(|m| {
        ["id", "generation", "connection"]
            .iter()
            .any(|key| m.contains_key(*key))
    }) {
        return Err(ai::error(
            "ER_AI_INVALID_ARGUMENT",
            "IDs and connection are server assigned",
        ));
    }
    serde_json::from_value(value).map_err(|_| {
        ai::error(
            "ER_AI_INVALID_ARGUMENT",
            "invalid configuration fields or types",
        )
    })
}

pub(super) fn validate(session: &SessionContext, statement: &Statement) -> Result<(), QueryError> {
    if !statement_has_ai(statement) {
        return Ok(());
    }
    if crate::engine::executor::routines::external_calls_forbidden() {
        return Err(fail(
            "AI calls are not allowed in stored programs or triggers",
        ));
    }
    if matches!(statement, Statement::Ai(_)) {
        return Ok(());
    }
    if let Statement::Explain {
        target, analyze, ..
    } = statement
    {
        return if *analyze {
            validate(session, target)
        } else {
            validate_shape(target)
        };
    }
    if session.explicit_txn
        || !session.autocommit
        || session.current_txn.is_some()
        || session.xa_branch.is_some()
        || !session.explicit_table_locks.is_empty()
    {
        return Err(fail(
            "AI calls require autocommit outside explicit transactions and table locks",
        ));
    }
    validate_shape(statement)
}

pub(in crate::engine::executor) fn validate_shape(statement: &Statement) -> Result<(), QueryError> {
    if !statement_has_ai(statement) {
        return Ok(());
    }
    struct Effects {
        bad: bool,
    }
    impl crate::parser::visit::AstVisitor for Effects {
        fn enter_select(&mut self, s: &Select) {
            self.bad |= s.for_update || s.lock_clause.is_some();
        }
        fn enter_expr(&mut self, e: &Expr) {
            if let Expr::Function { name, .. } = e {
                self.bad |= [
                    "__set_user_var",
                    "NEXTVAL",
                    "SETVAL",
                    "GET_LOCK",
                    "RELEASE_LOCK",
                    "RELEASE_ALL_LOCKS",
                    "LAST_INSERT_ID",
                    "SLEEP",
                    "BENCHMARK",
                    "LOAD_FILE",
                ]
                .iter()
                .any(|f| name.eq_ignore_ascii_case(f))
                    || (!crate::engine::cbo::function_cost::is_known_builtin_function(name)
                        && !crate::parser::is_aggregate_function(name));
            }
        }
    }
    let mut effects = Effects { bad: false };
    crate::parser::visit::visit_statement(&mut effects, statement);
    if effects.bad {
        return Err(fail("AI query inputs must be free of locking reads, stored functions and session/external side effects"));
    }
    match statement {
        Statement::Ai(_) => Ok(()),
        Statement::Explain { target, .. } => validate_shape(target),
        Statement::WithCte(with) => {
            if with.recursive
                || with
                    .ctes
                    .iter()
                    .any(|cte| statement_has_ai(&cte.query.clone().into_statement()))
            {
                return Err(fail("AI calls cannot appear inside CTE definitions"));
            }
            validate_shape(&with.body)
        }
        Statement::Select(s) => {
            if s.for_update
                || s.lock_clause.is_some()
                || s.distinct
                || !s.group_by.is_empty()
                || s.having.is_some()
                || !s.named_windows.is_empty()
                || s.sql_calc_found_rows
            {
                return Err(fail("AI calls require a final projection; move aggregation/window/DISTINCT into a derived query"));
            }
            let mut relation = s.clone();
            relation.projection = vec![];
            if statement_has_ai(&Statement::Select(relation)) {
                return Err(fail(
                    "AI calls cannot appear in relational inputs or predicates",
                ));
            }
            for item in &s.projection {
                let SelectItem::Expr { expr, .. } = item else {
                    return Err(fail("use explicit output columns with AI functions"));
                };
                validate_expr(expr)?;
            }
            for order in &s.order_by {
                for item in &s.projection {
                    if let SelectItem::Expr {
                        expr,
                        alias: Some(alias),
                    } = item
                    {
                        if expr_has_ai(expr)
                            && matches!(&order.expr,Expr::Identifier(n)if n.eq_ignore_ascii_case(alias))
                        {
                            return Err(fail("ORDER BY cannot depend on AI output"));
                        }
                    }
                }
                if matches!(order.expr, Expr::Literal(Literal::Int(_))) {
                    return Err(fail(
                        "use source expressions rather than ordinal ORDER BY with AI",
                    ));
                }
            }
            Ok(())
        }
        Statement::Set(a) if a.len() == 1 && a[0].scope.is_none() && a[0].name.starts_with('@') => {
            validate_expr(&a[0].value)
        }
        _ => Err(fail(
            "AI calls are supported only in final SELECT projection or single user-variable SET",
        )),
    }
}
fn validate_expr(expr: &Expr) -> Result<(), QueryError> {
    let invalid = std::cell::Cell::new(None);
    crate::parser::visit::inspect_value_expr(
        expr,
        |e| {
            match e {
                Expr::Function {
                    name,
                    args,
                    distinct,
                } if ai::is_function(name) => {
                    if *distinct
                        || !(2..=3).contains(&args.len())
                        || !matches!(
                            &args[0],
                            Expr::Literal(Literal::String(_) | Literal::Null)
                                | Expr::Placeholder(_)
                                | Expr::UserVariable(_)
                                | Expr::BoundValue(
                                    Value::String(_) | Value::SmallString(_) | Value::Null
                                )
                        )
                    {
                        invalid.set(Some(
                            "model must be a literal, placeholder or user variable; arity is 2..3",
                        ));
                    }
                    if args.iter().any(expr_has_ai) {
                        invalid.set(Some("nested AI calls are not supported"));
                    }
                }
                Expr::Function { name, .. }
                    if crate::parser::is_aggregate_function(name)
                        || !crate::engine::cbo::function_cost::is_known_builtin_function(name) =>
                {
                    invalid.set(Some(
                        "stored functions and aggregate inputs require a pure inner query",
                    ))
                }
                Expr::Default(_) | Expr::DefaultQualified(_) => invalid.set(Some(
                    "evaluate DEFAULT in the pure inner query before external projection",
                )),
                Expr::WindowFunction { .. } | Expr::Subquery(_) | Expr::Exists { .. } => invalid
                    .set(Some(
                        "subqueries/windows cannot be evaluated inside external projection",
                    )),
                _ => {}
            }
            true
        },
        |_| {
            invalid.set(Some(
                "evaluate nested queries in the pure inner query before external projection",
            ))
        },
    );
    if let Some(message) = invalid.get() {
        return Err(fail(message));
    }
    Ok(())
}

pub(super) fn admin(
    db: &Arc<Database>,
    session: &mut SessionContext,
    a: AiStatement,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    if crate::binlog::enabled() && !matches!(a.action.as_str(), "show" | "show_create" | "reload") {
        return Err(fail(
            "AI catalog mutations cannot be written to MySQL binlog",
        ));
    }
    if matches!(a.action.as_str(), "grant" | "revoke") {
        let model = db
            .ai_catalog()?
            .models
            .get(&name(session, &a.name))
            .cloned()
            .ok_or_else(|| ai::error("ER_AI_MODEL_NOT_FOUND", "model does not exist"))?;
        let verb = if a.action == "grant" {
            "GRANT"
        } else {
            "REVOKE"
        };
        let sql = format!(
            "{verb} AI_MODEL_{}_USE ON *.* {}",
            model.id,
            a.tail.as_deref().unwrap_or("")
        );
        let translated = crate::parser::parse_statement(&sql)
            .map_err(|_| ai::error("ER_AI_INVALID_ARGUMENT", "invalid model grant"))?;
        return execute_qualified_statement(db, session, translated, registry);
    }
    if a.action == "show"
        && a.kind == "model"
        && ai::runtime::authorize(db, &session.authorization_context(), "AI_ADMIN").is_err()
    {
        let catalog = db.ai_catalog()?;
        let auth = session.authorization_context();
        let rows = catalog
            .models
            .iter()
            .filter(|(qualified, model)| {
                model_in_schema(qualified, &a.name)
                    && (ai::runtime::authorize(db, &auth, "AI_USE").is_ok()
                        || ai::runtime::authorize(db, &auth, &format!("AI_MODEL_{}_USE", model.id))
                            .is_ok())
            })
            .map(|(qualified, model)| {
                let definition = serde_json::json!({
                    "kind":model.kind,
                    "model_revision":model.model_revision,
                    "enabled":model.enabled,
                    "capabilities":model.capabilities,
                    "dimensions":model.embedding.as_ref().map(|embedding|embedding.dimensions)
                });
                vec![
                    Value::String(qualified.clone()),
                    Value::String(definition.to_string()),
                ]
            })
            .collect();
        return Ok(result(&["Name", "Definition"], rows));
    }
    ai::runtime::authorize(db, &session.authorization_context(), "AI_ADMIN")?;
    if a.action == "reload" {
        ai::LocalConfig::reload()?;
        return Ok(QueryResponse::Ok);
    }
    if a.action == "show" || a.action == "show_create" {
        let catalog = db.ai_catalog()?;
        let rows = match a.kind.as_str() {
            "secret" => catalog
                .secrets
                .iter()
                .filter(|(n, _)| a.action == "show" || **n == a.name)
                .map(|(n, s)| {
                    vec![
                        Value::String(n.clone()),
                        Value::String(format!(
                            "generation={}, key_id={}, value=<redacted>",
                            s.generation, s.key_id
                        )),
                    ]
                })
                .collect(),
            "connection" => catalog
                .connections
                .iter()
                .filter(|(n, _)| a.action == "show" || **n == a.name)
                .map(|(n, c)| {
                    vec![
                        Value::String(n.clone()),
                        Value::String(serde_json::to_string(c).unwrap()),
                    ]
                })
                .collect(),
            "model" => catalog
                .models
                .iter()
                .filter(|(n, _)| {
                    if a.action == "show_create" {
                        **n == name(session, &a.name)
                    } else {
                        model_in_schema(n, &a.name)
                    }
                })
                .map(|(n, m)| {
                    vec![
                        Value::String(n.clone()),
                        Value::String(serde_json::to_string(m).unwrap()),
                    ]
                })
                .collect(),
            _ => return Err(ai::error("ER_AI_INVALID_ARGUMENT", "unknown AI object")),
        };
        return Ok(result(&["Name", "Definition"], rows));
    }
    if crate::binlog::enabled() {
        return Err(fail(
            "AI catalog DDL is unavailable while MySQL binary logging is enabled",
        ));
    }
    implicit_commit_for_ddl(db, session)?;
    if a.action == "rotate" {
        return db
            .mutate_ai_catalog(|c| {
                let keys = ai::load_keys(db.data_dir(), c, false)?;
                ai::crypto::rewrap(c, &keys)
            })
            .map(|_| QueryResponse::Ok);
    }
    let object_name = if a.kind == "model" {
        name(session, &a.name)
    } else {
        a.name.clone()
    };
    if a.kind == "model" && matches!(a.action.as_str(), "create" | "alter") {
        let (schema, _) = object_name
            .rsplit_once('.')
            .ok_or_else(|| ai::error("ER_AI_INVALID_ARGUMENT", "model schema is required"))?;
        if !db.catalog().databases.contains_key(schema) {
            return Err(ai::error(
                "ER_AI_INVALID_ARGUMENT",
                "model schema does not exist",
            ));
        }
    }
    if a.kind != "model" && object_name.contains('.') {
        return Err(ai::error(
            "ER_AI_INVALID_ARGUMENT",
            "Secret and Connection names are instance scoped",
        ));
    }
    if a.kind == "secret"
        && matches!(a.action.as_str(), "create" | "alter")
        && session.ssl_cipher.is_none()
    {
        return Err(ai::error(
            "ER_AI_ACCESS_DENIED",
            "Secret VALUE requires a TLS connection",
        ));
    }
    let secret_value = if a.kind == "secret" && matches!(a.action.as_str(), "create" | "alter") {
        Some(crate::engine::expr::eval_const(
            a.value
                .as_ref()
                .ok_or_else(|| ai::error("ER_AI_INVALID_ARGUMENT", "missing Secret VALUE"))?,
        )?)
    } else {
        None
    };
    db.mutate_ai_catalog(|c| {
        let exists = match a.kind.as_str() {
            "secret" => c.secrets.contains_key(&object_name),
            "connection" => c.connections.contains_key(&object_name),
            "model" => c.models.contains_key(&object_name),
            _ => false,
        };
        if (a.action == "create" && exists) || (a.action == "drop" && !exists) {
            if a.if_exists {
                return Ok(());
            }
            return Err(ai::error(
                "ER_AI_INVALID_ARGUMENT",
                "AI object already exists or was not found",
            ));
        }
        if a.action == "alter" && !exists {
            return Err(ai::error(
                "ER_AI_INVALID_ARGUMENT",
                "AI object does not exist",
            ));
        }
        match (a.action.as_str(), a.kind.as_str()) {
            ("create" | "alter", "secret") => {
                let value = secret_value
                    .as_ref()
                    .and_then(crate::types::value_str)
                    .ok_or_else(|| {
                        ai::error("ER_AI_INVALID_ARGUMENT", "Secret VALUE must be text")
                    })?;
                let keys = ai::load_keys(db.data_dir(), c, true)?;
                if c.encryption_id.is_empty() {
                    use rand::RngCore;
                    let mut id = [0; 16];
                    rand::rngs::OsRng.fill_bytes(&mut id);
                    c.encryption_id = id.iter().map(|x| format!("{x:02x}")).collect();
                    c.active_key_id = keys.candidate.clone();
                }
                let (id, generation) = if let Some(old) = c.secrets.get(&object_name) {
                    (old.id, old.generation + 1)
                } else {
                    (next_id(c)?, 1)
                };
                let encrypted = ai::encrypt_secret(c, &keys, id, generation, value)?;
                c.secrets.insert(object_name.clone(), encrypted);
            }
            ("create" | "alter", "connection") => {
                let mut connection: ai::Connection = json_config(&a)?;
                if connection.id != 0 || connection.generation != 0 {
                    return Err(ai::error(
                        "ER_AI_INVALID_ARGUMENT",
                        "IDs are server assigned",
                    ));
                }
                match &mut connection.auth {
                    ai::Auth::Bearer { secret } | ai::Auth::Header { secret, .. } => {
                        secret.make_ascii_lowercase()
                    }
                    ai::Auth::None => {}
                }
                ai::validate_connection(&connection)?;
                if connection
                    .auth
                    .secret()
                    .is_some_and(|s| !c.secrets.contains_key(s))
                {
                    return Err(ai::error(
                        "ER_AI_CREDENTIAL_UNAVAILABLE",
                        "Secret does not exist",
                    ));
                }
                if let Some(old) = c.connections.get(&object_name) {
                    if old.protocol != connection.protocol {
                        return Err(ai::error(
                            "ER_AI_INVALID_ARGUMENT",
                            "connection protocol is immutable",
                        ));
                    }
                    connection.id = old.id;
                    connection.generation = old.generation + 1;
                } else {
                    connection.id = next_id(c)?;
                    connection.generation = 1;
                }
                for model in c.models.values().filter(|m| m.connection == object_name) {
                    ai::validate_model(model, &connection)?;
                }
                c.connections.insert(object_name.clone(), connection);
            }
            ("create" | "alter", "model") => {
                let mut model: ai::Model = json_config(&a)?;
                if model.id != 0 || model.generation != 0 || !model.connection.is_empty() {
                    return Err(ai::error(
                        "ER_AI_INVALID_ARGUMENT",
                        "model IDs and connection are server assigned",
                    ));
                }
                if let Some(old) = c.models.get(&object_name) {
                    if old.kind != model.kind
                        || old.remote_model != model.remote_model
                        || old.model_revision != model.model_revision
                        || old.embedding != model.embedding
                    {
                        return Err(ai::error(
                            "ER_AI_INVALID_ARGUMENT",
                            "model semantic identity is immutable",
                        ));
                    }
                    if model.capabilities.iter().any(|(key, value)| {
                        *value && !old.capabilities.get(key).copied().unwrap_or(false)
                    }) {
                        return Err(ai::error(
                            "ER_AI_INVALID_ARGUMENT",
                            "ALTER may only tighten model capabilities",
                        ));
                    }
                    model.id = old.id;
                    model.generation = old.generation + 1;
                    model.connection = old.connection.clone();
                } else {
                    model.id = next_id(c)?;
                    model.generation = 1;
                    model.connection = a.connection.clone().ok_or_else(|| {
                        ai::error("ER_AI_INVALID_ARGUMENT", "USING connection required")
                    })?;
                }
                let connection = c.connections.get(&model.connection).ok_or_else(|| {
                    ai::error("ER_AI_INVALID_ARGUMENT", "connection does not exist")
                })?;
                ai::validate_model(&model, connection)?;
                c.models.insert(object_name.clone(), model);
            }
            ("drop", "secret") => {
                if c.connections
                    .values()
                    .any(|connection| connection.auth.secret() == Some(object_name.as_str()))
                {
                    return Err(ai::error(
                        "ER_AI_INVALID_ARGUMENT",
                        "Secret is referenced by a connection",
                    ));
                }
                c.secrets.remove(&object_name);
            }
            ("drop", "connection") => {
                if c.models.values().any(|m| m.connection == object_name) {
                    return Err(ai::error(
                        "ER_AI_INVALID_ARGUMENT",
                        "connection is referenced by a model",
                    ));
                }
                c.connections.remove(&object_name);
            }
            ("drop", "model") => {
                c.models.remove(&object_name);
            }
            _ => {
                return Err(ai::error(
                    "ER_AI_INVALID_ARGUMENT",
                    "unsupported AI command",
                ))
            }
        }
        Ok(())
    })?;
    Ok(QueryResponse::Ok)
}

pub(super) fn project(
    db: &Arc<Database>,
    session: &mut SessionContext,
    s: Select,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    let statement = Statement::Select(s);
    validate_shape(&statement)?;
    let Statement::Select(mut s) = statement else {
        unreachable!()
    };
    let calls = ai::runtime::prepare_calls(db.clone(), session)?;
    bind_models_from_catalog(db, session, &s, calls.catalog())?;
    let output_columns = super::super::select::describe_statement_columns(
        db,
        session,
        &Statement::Select(s.clone()),
    )?;
    let mut input_memory = ai::admission::Memory::reserve(0)?;
    let mut ast_bytes = 0usize;
    for item in &s.projection {
        if let SelectItem::Expr { expr, .. } = item {
            crate::parser::visit::inspect_value_expr(
                expr,
                |node| {
                    ast_bytes = ast_bytes.saturating_add(192).saturating_add(match node {
                        Expr::Literal(Literal::String(s) | Literal::Decimal(s)) => s.capacity(),
                        Expr::Literal(Literal::HexBytes(bytes) | Literal::BitBytes(bytes)) => {
                            bytes.capacity()
                        }
                        Expr::BoundValue(v) => v.heap_capacity(),
                        Expr::Function { name, .. }
                        | Expr::Identifier(name)
                        | Expr::UserVariable(name) => name.capacity(),
                        Expr::CompoundIdentifier(parts) => parts.iter().map(String::capacity).sum(),
                        _ => 0,
                    });
                    true
                },
                |_| {},
            );
        }
    }
    Arc::get_mut(&mut input_memory).unwrap().grow(ast_bytes)?;
    let mut projection = s.projection.clone();
    let mut inputs = Vec::<Expr>::new();
    let mut parameters = std::collections::BTreeSet::new();
    for item in &mut projection {
        let SelectItem::Expr { expr, .. } = item else {
            return Err(fail("explicit projection columns required"));
        };
        crate::parser::visit::try_rewrite_value_expr(expr, |node| {
            match node {
                Expr::UserVariable(name) => {
                    let bytes = session.user_variable(name).map_or(0, Value::heap_capacity);
                    Arc::get_mut(&mut input_memory).unwrap().grow(bytes)?;
                    *node = Expr::BoundValue(
                        session.user_variable(name).cloned().unwrap_or(Value::Null),
                    );
                    return Ok(false);
                }
                Expr::SystemVariable { .. } => {
                    *node = Expr::BoundValue(crate::engine::expr::with_session_expr_info(
                        session,
                        || crate::engine::expr::eval_const(node),
                    )?);
                    return Ok(false);
                }
                Expr::Placeholder(index) => {
                    parameters.insert(*index);
                }
                Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
                    let index = inputs
                        .iter()
                        .position(|input| input == node)
                        .unwrap_or_else(|| {
                            inputs.push(node.clone());
                            inputs.len() - 1
                        });
                    *node = Expr::Identifier(format!("__ai_input_{index}"));
                    return Ok(false);
                }
                Expr::Function { name, .. }
                    if name.eq_ignore_ascii_case(crate::parser::FT_MATCH_FUNCTION) =>
                {
                    if crate::engine::ai::sql::expr_has_ai(node) {
                        return Err(fail("MATCH cannot consume external model output"));
                    }
                    let index = inputs.len();
                    inputs.push(node.clone());
                    *node = Expr::Identifier(format!("__ai_input_{index}"));
                    return Ok(false);
                }
                _ => {}
            }
            Ok::<_, QueryError>(true)
        })?;
    }
    let parameter_bytes = parameters.last().map_or(0, |last| {
        last.saturating_add(1)
            .saturating_mul(std::mem::size_of::<Value>())
    }) + parameters
        .iter()
        .map(|i| {
            crate::engine::params::with_current_param(*i, |v| v.map_or(0, Value::heap_capacity))
        })
        .sum::<usize>();
    let context_bytes = 2048
        + session
            .statement_query_attributes
            .iter()
            .map(|(k, v)| k.capacity() + v.heap_capacity() + 96)
            .sum::<usize>();
    Arc::get_mut(&mut input_memory)
        .unwrap()
        .grow(parameter_bytes + context_bytes)?;
    let params = crate::engine::params::snapshot_selected_context(&parameters)?;
    let expression_info = crate::engine::expr::capture_external_expr_info(session);
    let frozen_time = crate::engine::expr::with_session_expr_info(session, || {
        crate::engine::expr::session_utc_now()
            .and_utc()
            .timestamp_micros()
    });
    let cancellation = crate::engine::cancel::CancellationSnapshot::capture().fork_local();
    let budget = crate::engine::executor::mem_budget::current_budget()
        .unwrap_or_else(|| crate::engine::executor::mem_budget::MemBudget::for_database(db));
    for order in &mut s.order_by {
        for item in &s.projection {
            if let SelectItem::Expr {
                expr,
                alias: Some(alias),
            } = item
            {
                if matches!(&order.expr,Expr::Identifier(name)if name.eq_ignore_ascii_case(alias)) {
                    order.expr = expr.clone();
                }
            }
        }
    }
    s.projection = inputs
        .into_iter()
        .enumerate()
        .map(|(i, expr)| SelectItem::Expr {
            expr,
            alias: Some(format!("__ai_input_{i}")),
        })
        .collect();
    if s.projection.is_empty() {
        s.projection.push(SelectItem::Expr {
            expr: Expr::Literal(Literal::Int(1)),
            alias: Some("__ai_dummy".into()),
        });
    }
    apply_sql_select_limit(session, &mut s);
    let response = run_select_response(db, session, s, registry)?;
    let (columns, mut cursor) = response_into_cursor(response)?;
    let mut rows = Vec::new();
    while let Some(row) = cursor.next_row()? {
        if rows.len() >= 100 {
            return Err(ai::error(
                "ER_AI_BUDGET_EXCEEDED",
                "external projection exceeds 100 rows; use LIMIT",
            ));
        }
        let bytes = row
            .iter()
            .map(|v| v.heap_capacity() + std::mem::size_of::<Value>())
            .sum::<usize>();
        Arc::get_mut(&mut input_memory).unwrap().grow(bytes)?;
        rows.push(row);
    }
    let mut warnings = cursor.take_warnings();
    for warning in std::mem::take(&mut warnings) {
        crate::engine::api::push_sql_warning(warning);
    }
    drop(cursor);
    let schema = synthetic_set_op_schema(&columns);
    let only_embeddings=projection.iter().all(|item|matches!(item,SelectItem::Expr{expr:Expr::Function{name,..},..}if name.eq_ignore_ascii_case("AI_EMBED"))||matches!(item,SelectItem::Expr{expr,..}if !crate::engine::ai::sql::expr_has_ai(expr)));
    let direct_calls = projection.iter().all(|item| {
        matches!(item,SelectItem::Expr{expr:Expr::Function{name,..},..}if ai::is_function(name))
            || matches!(item,SelectItem::Expr{expr,..}if !crate::engine::ai::sql::expr_has_ai(expr))
    });
    Ok(QueryResponse::StreamingResultSet(StreamingResultSet {
        columns: output_columns,
        found_rows: None,
        cursor: Box::new(ExternalCursor {
            db: db.clone(),
            database: calls.database().to_owned(),
            catalog: calls.catalog().clone(),
            calls: Some(calls),
            rows: rows.into_iter(),
            buffer: std::collections::VecDeque::new(),
            projection,
            schema,
            params,
            info: Some(expression_info),
            cancellation,
            budget,
            frozen_time,
            env: crate::util::env::override_snapshot(),
            _input_memory: input_memory,
            window_memory: None,
            window: if only_embeddings {
                32
            } else if direct_calls {
                4
            } else {
                1
            },
            finished: false,
            warnings: Arc::new(parking_lot::Mutex::new(Default::default())),
        }),
    }))
}

struct ExternalCursor {
    db: Arc<Database>,
    database: String,
    catalog: Arc<AiCatalog>,
    calls: Option<ai::runtime::CallScope>,
    rows: std::vec::IntoIter<Vec<Value>>,
    buffer: std::collections::VecDeque<Vec<Value>>,
    projection: Vec<SelectItem>,
    schema: TableSchema,
    params: Option<crate::engine::params::ParamContextSnapshot>,
    info: Option<crate::engine::expr::SessionExprInfo>,
    cancellation: crate::engine::cancel::CancellationSnapshot,
    budget: Arc<crate::engine::executor::mem_budget::MemBudget>,
    frozen_time: i64,
    env: crate::util::env::OverrideSnapshot,
    _input_memory: Arc<ai::admission::Memory>,
    window_memory: Option<Arc<ai::admission::Memory>>,
    window: usize,
    finished: bool,
    warnings: crate::engine::api::StatementWarningSink,
}
impl std::fmt::Debug for ExternalCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("ExternalCursor")
            .field("remaining_rows", &self.rows.len())
            .field("window", &self.window)
            .finish_non_exhaustive()
    }
}
impl Drop for ExternalCursor {
    fn drop(&mut self) {
        self.cancellation.cancel_local();
    }
}
impl ExternalCursor {
    fn fill(&mut self) -> Result<(), QueryError> {
        self.window_memory = None;
        let rows: Vec<_> = self.rows.by_ref().take(self.window).collect();
        if rows.is_empty() {
            self.finished = true;
            return Ok(());
        }
        let mut memory = ai::admission::Memory::reserve(
            rows.len()
                .saturating_mul(self.projection.len())
                .saturating_mul(std::mem::size_of::<Option<Value>>()),
        )?;
        let output = evaluate_rows(
            &self.database,
            &self.catalog,
            &self.projection,
            &self.schema,
            rows,
            Arc::get_mut(&mut memory).unwrap(),
        )?;
        self.buffer = output.into();
        self.window_memory = Some(memory);
        Ok(())
    }
}
impl crate::engine::api::ResultCursor for ExternalCursor {
    fn external_io(&self) -> bool {
        true
    }
    fn external_batch_rows(&self) -> usize {
        self.window
    }
    fn next_row(&mut self) -> Result<Option<Vec<Value>>, QueryError> {
        if self.finished {
            return Ok(None);
        }
        self.cancellation.check()?;
        if let Some(row) = self.buffer.pop_front() {
            return Ok(Some(row));
        }
        let _cancel = self.cancellation.enter();
        let _reclaim = crate::engine::executor::mem_budget::ReclaimDatabaseScope::enter(&self.db);
        let mut scope = self.calls.take();
        let mut info = self.info.take();
        let budget = self.budget.clone();
        let env = self.env.clone();
        let params = self.params.take();
        let warning_sink = self.warnings.clone();
        let now = self.frozen_time;
        let result = crate::util::env::with_override_snapshot(env, || {
            crate::engine::executor::mem_budget::with_query_budget(budget, || {
                crate::engine::api::with_frozen_statement_time(now, || {
                    crate::engine::api::with_statement_warning_sink(warning_sink, || {
                        crate::engine::expr::with_saved_expr_info(&mut info, || {
                            ai::runtime::with_saved_calls(&mut scope, || match &params {
                                Some(params) => params.run(|| self.fill()),
                                None => self.fill(),
                            })
                        })
                    })
                })
            })
        });
        self.calls = scope;
        self.info = info;
        self.params = params;
        if let Err(error) = result {
            self.finished = true;
            self.cancellation.cancel_local();
            return Err(error);
        }
        Ok(self.buffer.pop_front())
    }
    fn take_warnings(&mut self) -> crate::engine::api::SqlWarnings {
        std::mem::take(&mut *self.warnings.lock())
    }
}

fn evaluate_rows(
    database: &str,
    catalog: &AiCatalog,
    projection: &[SelectItem],
    schema: &TableSchema,
    rows: Vec<Vec<Value>>,
    charge: &mut ai::admission::Memory,
) -> Result<Vec<Vec<Value>>, QueryError> {
    let mut ready: Vec<Vec<Option<Value>>> = (0..rows.len())
        .map(|_| vec![None; projection.len()])
        .collect();
    let mut output = Vec::with_capacity(rows.len());
    for (column, item) in projection.iter().enumerate() {
        let SelectItem::Expr {
            expr:
                Expr::Function {
                    name: function_name,
                    args,
                    ..
                },
            ..
        } = item
        else {
            continue;
        };
        if function_name.eq_ignore_ascii_case("AI_GENERATE")
            || function_name.eq_ignore_ascii_case("AI_GENERATE_DETAIL")
        {
            let arguments = rows
                .iter()
                .map(|row| {
                    args.iter()
                        .map(|arg| crate::engine::expr::eval_expr(arg, schema, row))
                        .collect::<Result<Vec<_>, _>>()
                })
                .collect::<Result<Vec<_>, _>>()?;
            let values = ai::runtime::call_many(function_name, arguments)?;
            for (index, value) in values.into_iter().enumerate() {
                charge.grow(value.heap_capacity())?;
                ready[index][column] = Some(value);
            }
            continue;
        }
        if !function_name.eq_ignore_ascii_case("AI_EMBED") {
            continue;
        }
        let mut groups: Vec<(Value, Value, Vec<(usize, String)>)> = Vec::new();
        for (row_index, row) in rows.iter().enumerate() {
            let values = args
                .iter()
                .map(|arg| crate::engine::expr::eval_expr(arg, &schema, row))
                .collect::<Result<Vec<_>, _>>()?;
            if matches!(values[0], Value::Null)
                || matches!(values[1], Value::Null)
                || crate::types::value_str(&values[1]).is_none()
            {
                continue;
            }
            let options = values.get(2).cloned().unwrap_or(Value::Null);
            let index = groups
                .iter()
                .position(|(model, opts, _)| *model == values[0] && *opts == options)
                .unwrap_or_else(|| {
                    groups.push((values[0].clone(), options, Vec::new()));
                    groups.len() - 1
                });
            groups[index].2.push((
                row_index,
                crate::types::value_str(&values[1]).unwrap().to_owned(),
            ));
        }
        for (model, options, members) in groups {
            let raw = crate::types::value_str(&model)
                .ok_or_else(|| ai::error("ER_AI_INVALID_ARGUMENT", "model must be text"))?;
            let definition = catalog.models.get(&if raw.contains('.') {
                raw.to_ascii_lowercase()
            } else {
                format!("{database}.{}", raw.to_ascii_lowercase())
            });
            let batch_size = definition
                .map(|m| {
                    m.limits.max_batch_items.unwrap_or(64).min(64).min(
                        (32 * 1024 * 1024usize
                            / (m.embedding.as_ref().map(|e| e.dimensions).unwrap_or(16383) * 1024
                                + 1))
                            .max(1),
                    )
                })
                .unwrap_or(1);
            for chunk in members.chunks(batch_size) {
                if chunk.len() < 2 {
                    continue;
                }
                let vectors = ai::runtime::embed_batch(
                    model.clone(),
                    chunk.iter().map(|(_, text)| text.clone()).collect(),
                    options.clone(),
                )?;
                for ((row, _), value) in chunk.iter().zip(vectors) {
                    charge.grow(value.heap_capacity())?;
                    ready[*row][column] = Some(value);
                }
            }
        }
    }
    for (index, row) in rows.into_iter().enumerate() {
        let mut values = Vec::with_capacity(projection.len());
        for (column, item) in projection.iter().enumerate() {
            let SelectItem::Expr { expr, .. } = item else {
                unreachable!()
            };
            let value = match ready[index][column].take() {
                Some(value) => value,
                None => {
                    let value = crate::engine::expr::eval_expr(expr, &schema, &row)?;
                    charge.grow(value.heap_capacity())?;
                    value
                }
            };
            values.push(value);
        }
        output.push(values);
    }

    Ok(output)
}
fn bind_models(
    db: &Arc<Database>,
    session: &SessionContext,
    select: &Select,
) -> Result<(), QueryError> {
    let catalog = db.ai_catalog()?;
    bind_models_from_catalog(db, session, select, &catalog)
}
fn bind_models_from_catalog(
    db: &Arc<Database>,
    session: &SessionContext,
    select: &Select,
    catalog: &AiCatalog,
) -> Result<(), QueryError> {
    let mut failure = None;
    for item in &select.projection {
        let SelectItem::Expr { expr, .. } = item else {
            continue;
        };
        crate::parser::visit::inspect_value_expr(
            expr,
            |node| {
                if let Expr::Function {
                    name: function,
                    args,
                    ..
                } = node
                {
                    if ai::is_function(function) {
                        let result = (|| {
                            let value =
                                crate::engine::expr::with_session_expr_info(session, || {
                                    crate::engine::expr::eval_const(&args[0])
                                })?;
                            if matches!(value, Value::Null) {
                                return Ok(());
                            }
                            let raw = crate::types::value_str(&value).ok_or_else(|| {
                                ai::error("ER_AI_INVALID_ARGUMENT", "model must be text")
                            })?;
                            let model =
                                catalog.models.get(&name(session, raw)).ok_or_else(|| {
                                    ai::error("ER_AI_MODEL_NOT_FOUND", "model does not exist")
                                })?;
                            if ai::runtime::authorize(
                                db,
                                &session.authorization_context(),
                                "AI_USE",
                            )
                            .is_err()
                            {
                                ai::runtime::authorize(
                                    db,
                                    &session.authorization_context(),
                                    &format!("AI_MODEL_{}_USE", model.id),
                                )?;
                            }
                            let embed = function.eq_ignore_ascii_case("AI_EMBED")
                                || function.eq_ignore_ascii_case("AI_EMBED_DETAIL");
                            if embed != (model.kind == "embedding") {
                                return Err(ai::error(
                                    "ER_AI_WRONG_MODEL_KIND",
                                    "model kind does not match function",
                                ));
                            }
                            Ok(())
                        })();
                        if let Err(e) = result {
                            failure = Some(e);
                        }
                    }
                }
                failure.is_none()
            },
            |_| {},
        );
        if failure.is_some() {
            break;
        }
    }
    failure.map_or(Ok(()), Err)
}

pub(super) fn explain(
    db: &Arc<Database>,
    session: &mut SessionContext,
    target: Statement,
    format: crate::parser::ExplainFormat,
    analyze: bool,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    validate(session, &target)?;
    if let Statement::Select(select) = &target {
        bind_models(db, session, select)?;
    }
    let _columns = super::super::select::describe_statement_columns(db, session, &target)?;
    if !analyze {
        return external_explain_result(format, false, None, (0, 0), 0.0);
    }
    let response = execute_statement(db, session, target, registry)?;
    let (_, cursor) = response_into_cursor(response)?;
    let metadata = external_explain_result(format, true, None, (0, 0), 0.0)?.columns();
    Ok(QueryResponse::StreamingResultSet(StreamingResultSet {
        columns: metadata,
        found_rows: None,
        cursor: Box::new(ExternalExplainCursor {
            inner: cursor,
            format,
            done: false,
        }),
    }))
}
struct ExternalExplainCursor {
    inner: Box<dyn crate::engine::api::ResultCursor>,
    format: crate::parser::ExplainFormat,
    done: bool,
}
impl std::fmt::Debug for ExternalExplainCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("ExternalExplainCursor")
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}
impl crate::engine::api::ResultCursor for ExternalExplainCursor {
    fn external_io(&self) -> bool {
        true
    }
    fn next_row(&mut self) -> Result<Option<Vec<Value>>, QueryError> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        let start = Instant::now();
        let mut rows = 0u64;
        while self.inner.next_row()?.is_some() {
            rows += 1;
        }
        let counts = ai::runtime::last_statement_stats();
        let response = external_explain_result(
            self.format,
            true,
            Some(rows),
            counts,
            start.elapsed().as_secs_f64() * 1000.0,
        )?;
        let QueryResponse::ResultSet(mut result) = response else {
            unreachable!()
        };
        Ok(result.rows.pop())
    }
    fn take_warnings(&mut self) -> crate::engine::api::SqlWarnings {
        self.inner.take_warnings()
    }
}

fn external_explain_result(
    format: crate::parser::ExplainFormat,
    analyze: bool,
    actual_rows: Option<u64>,
    counts: (usize, usize),
    elapsed: f64,
) -> Result<QueryResponse, QueryError> {
    let details=format!("ExternalProject; final projection after relational LIMIT; max_call_items=100; max_http_attempts=100; logical_items={}; attempts={}; external_io=true",counts.0,counts.1);
    use crate::parser::ExplainFormat;
    match format {
        ExplainFormat::Traditional => {
            let mut columns = crate::planner::explain_result_columns();
            let mut row = vec![
                Value::Int(1),
                Value::String("EXTERNAL".into()),
                Value::Null,
                Value::Null,
                Value::String("ALL".into()),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::String(details),
            ];
            if analyze {
                for (name, kind) in [
                    ("actual_rows", ColumnType::Double),
                    ("actual_loops", ColumnType::LongLong),
                    ("actual_first_row_ms", ColumnType::Double),
                    ("actual_last_row_ms", ColumnType::Double),
                    ("actual_rows_total", ColumnType::LongLong),
                ] {
                    columns.push(Column {
                        name: name.into(),
                        column_type: kind,
                        source: Default::default(),
                    });
                }
                row.extend([
                    Value::Double(actual_rows.unwrap_or(0) as f64),
                    Value::Int(1),
                    Value::Double(elapsed),
                    Value::Double(elapsed),
                    Value::UInt(actual_rows.unwrap_or(0)),
                ]);
            }
            Ok(QueryResponse::ResultSet(ResultSet {
                columns,
                rows: vec![row],
            }))
        }
        ExplainFormat::Tree | ExplainFormat::Json => {
            let text = if format == ExplainFormat::Json {
                serde_json::json!({"query_block":{"operator":"ExternalProject","external_io":true,"max_call_items":100,"max_http_attempts":100,"actual_rows":actual_rows,"logical_items":counts.0,"attempts":counts.1,"total_ms":if analyze{Some(elapsed)}else{None}}}).to_string()
            } else {
                format!("-> {details}; actual_rows={actual_rows:?}; total_ms={elapsed:.3}")
            };
            Ok(result(&["EXPLAIN"], vec![vec![Value::String(text)]]))
        }
    }
}
