use super::*;
use crate::engine::auth::AccountKey;
use crate::parser::{
    RoutineStmt, TriggerDef, TriggerEvent, TriggerOrder, TriggerOrderKind, TriggerTiming,
};
use crate::storage::{
    TriggerEventSchema, TriggerOrderKindSchema, TriggerOrderSchema, TriggerSchema,
    TriggerTimingSchema,
};

pub(super) fn execute_create_trigger(
    db: &Arc<Database>,
    session: &mut SessionContext,
    mut def: TriggerDef,
) -> Result<QueryResponse, QueryError> {
    super::implicit_commit_for_ddl(db, session)?;
    validate_trigger_body(session, &def.body.statements)?;
    validate_trigger_rows(&mut def.body.statements, def.event, def.timing)?;
    // B3 — setting DEFINER to another account requires SET_USER_ID/SUPER.
    super::routines::validate_definer_privilege(db, session, def.definer.as_ref())?;
    let schema = trigger_schema_from_def(session, &def);
    validate_trigger_definer(db, &schema)?;
    let order = def.order.as_ref().map(trigger_order_schema);
    if !db.create_trigger(&session.current_database, schema, def.if_not_exists, order)? {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Note,
            crate::error::catalog::ER_WARN_TRG_ALREADY_EXISTS,
            format!("Trigger '{}' already exists", def.name),
        );
    }
    super::routines::invalidate_trigger_cache();
    // No global SQL-cache flush: `create_trigger` bumps the target table's
    // schema_version (catalog_ops), so every cached plan/DML memo that
    // references that table is invalidated precisely through its
    // `(table_key, id, schema_version)` dependency — while hot plans on
    // unrelated tables survive. DML memos re-check table identity + version
    // (prepared_dml::memo_is_valid) and recompile, picking up the new trigger.
    Ok(QueryResponse::Ok)
}

pub(super) fn execute_drop_trigger(
    db: &Arc<Database>,
    session: &mut SessionContext,
    name: &str,
    if_exists: bool,
) -> Result<QueryResponse, QueryError> {
    super::implicit_commit_for_ddl(db, session)?;
    // The trigger may have been replaced while its name lock was pending.
    // Rebind both authority and the target-table lock under the trigger X
    // grant; a lock recipe computed before waiting is not a stable identity.
    super::dispatch::authorize_statement(
        db,
        session,
        &Statement::DropTrigger {
            name: name.to_owned(),
            if_exists,
        },
    )?;
    if let Some(trigger) = db.trigger_definition(&session.current_database, name) {
        db.acquire_alter_execution_mdl(
            session.connection_id as u64,
            &crate::storage::table_key(&trigger.table_database, &trigger.table_name),
            crate::storage::MdlLockType::SharedNoWrite,
            session
                .sysvars
                .lock_wait_timeout
                .map(std::time::Duration::from_secs)
                .unwrap_or_else(crate::storage::metadata_wait_timeout),
        )?;
    }
    db.drop_trigger(&session.current_database, name, if_exists)?;
    super::routines::invalidate_trigger_cache();
    // See `execute_create_trigger`: `drop_trigger` bumps the target table's
    // schema_version, so dependent plans/DML memos invalidate precisely via
    // their table dependency — no global flush of unrelated hot plans.
    Ok(QueryResponse::Ok)
}

pub(super) fn table_has_insert_triggers(db: &Arc<Database>, table_name: &str) -> bool {
    db.table_has_trigger_event(table_name, TriggerEventSchema::Insert)
}

pub(super) fn table_has_update_triggers(db: &Arc<Database>, table_name: &str) -> bool {
    db.table_has_trigger_event(table_name, TriggerEventSchema::Update)
}

pub(super) fn table_has_delete_triggers(db: &Arc<Database>, table_name: &str) -> bool {
    db.table_has_trigger_event(table_name, TriggerEventSchema::Delete)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn execute_insert_rows_with_triggers(
    db: &Arc<Database>,
    session: &mut SessionContext,
    txn: TxnView,
    table_name: &str,
    columns: Option<&[String]>,
    source_rows: Vec<Vec<Value>>,
    ignore: bool,
    replace: bool,
    on_duplicate: Option<&[crate::parser::Assignment]>,
    insert_alias: Option<&crate::parser::InsertAlias>,
    view_check_predicate: Option<&Expr>,
    returning_clause: Option<&crate::parser::ReturningClause>,
    target_alias: Option<&str>,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    super::insert::execute_insert_rows::<true>(
        db,
        Some(super::insert::InsertTriggers { session, registry }),
        txn,
        table_name,
        columns,
        source_rows,
        ignore,
        replace,
        on_duplicate,
        insert_alias,
        view_check_predicate,
        returning_clause,
        target_alias,
        false,
    )
}

pub(super) fn fire_before_insert(
    db: &Arc<Database>,
    session: &mut SessionContext,
    table: &TableSchema,
    values: Vec<Value>,
    registry: &SessionRegistry,
) -> Result<Vec<Value>, QueryError> {
    fire_row_triggers(
        db,
        session,
        table,
        TriggerEventSchema::Insert,
        TriggerTimingSchema::Before,
        None,
        Some(values),
        true,
        registry,
    )
    .map(|row| row.unwrap_or_default())
}

pub(super) fn fire_after_insert(
    db: &Arc<Database>,
    session: &mut SessionContext,
    table: &TableSchema,
    values: Vec<Value>,
    registry: &SessionRegistry,
) -> Result<(), QueryError> {
    fire_row_triggers(
        db,
        session,
        table,
        TriggerEventSchema::Insert,
        TriggerTimingSchema::After,
        None,
        Some(values),
        false,
        registry,
    )?;
    Ok(())
}

pub(super) fn fire_before_update(
    db: &Arc<Database>,
    session: &mut SessionContext,
    table: &TableSchema,
    old_values: Vec<Value>,
    new_values: Vec<Value>,
    registry: &SessionRegistry,
) -> Result<Vec<Value>, QueryError> {
    fire_row_triggers(
        db,
        session,
        table,
        TriggerEventSchema::Update,
        TriggerTimingSchema::Before,
        Some(old_values),
        Some(new_values),
        true,
        registry,
    )
    .map(|row| row.unwrap_or_default())
}

pub(super) fn fire_after_update(
    db: &Arc<Database>,
    session: &mut SessionContext,
    table: &TableSchema,
    old_values: Vec<Value>,
    new_values: Vec<Value>,
    registry: &SessionRegistry,
) -> Result<(), QueryError> {
    fire_row_triggers(
        db,
        session,
        table,
        TriggerEventSchema::Update,
        TriggerTimingSchema::After,
        Some(old_values),
        Some(new_values),
        false,
        registry,
    )?;
    Ok(())
}

pub(super) fn fire_before_delete(
    db: &Arc<Database>,
    session: &mut SessionContext,
    table: &TableSchema,
    old_values: Vec<Value>,
    registry: &SessionRegistry,
) -> Result<(), QueryError> {
    fire_row_triggers(
        db,
        session,
        table,
        TriggerEventSchema::Delete,
        TriggerTimingSchema::Before,
        Some(old_values),
        None,
        false,
        registry,
    )?;
    Ok(())
}

pub(super) fn fire_after_delete(
    db: &Arc<Database>,
    session: &mut SessionContext,
    table: &TableSchema,
    old_values: Vec<Value>,
    registry: &SessionRegistry,
) -> Result<(), QueryError> {
    fire_row_triggers(
        db,
        session,
        table,
        TriggerEventSchema::Delete,
        TriggerTimingSchema::After,
        Some(old_values),
        None,
        false,
        registry,
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn fire_row_triggers(
    db: &Arc<Database>,
    session: &mut SessionContext,
    table: &TableSchema,
    event: TriggerEventSchema,
    timing: TriggerTimingSchema,
    old_values: Option<Vec<Value>>,
    mut new_values: Option<Vec<Value>>,
    new_writable: bool,
    registry: &SessionRegistry,
) -> Result<Option<Vec<Value>>, QueryError> {
    let triggers = db.triggers_for_table_event(&table.qualified_name(), event, timing);
    for trigger in triggers {
        let result = super::routines::execute_trigger_body(
            db,
            session,
            &trigger,
            table,
            old_values.clone(),
            new_values.clone(),
            new_writable,
            registry,
        )?;
        if result.is_some() {
            new_values = result;
        }
    }
    Ok(new_values)
}

fn trigger_schema_from_def(session: &SessionContext, def: &TriggerDef) -> TriggerSchema {
    let definer = def
        .definer
        .clone()
        .unwrap_or_else(|| crate::parser::AccountIdent {
            user: session.authenticated_as.user.clone(),
            host: session.authenticated_as.host.clone(),
        });
    TriggerSchema {
        database: session.current_database.clone(),
        name: def.name.clone(),
        // Keep the (possibly schema-qualified) raw name; Database::create_trigger
        // splits it against the creating session's database, mirroring how the
        // trigger's own `name` is resolved. Pre-splitting here dropped the
        // schema for cross-schema targets (e.g. `ON db1.t` while USE test).
        table_database: String::new(),
        table_name: def.table.clone(),
        timing: trigger_timing_schema(def.timing),
        event: trigger_event_schema(def.event),
        definer_user: definer.user,
        definer_host: definer.host,
        body_sql: def.body.sql.clone(),
        create_sql: def.create_sql.clone(),
        action_order: 0,
        schema_version: 1,
        created_ms: 0,
        // B1 — capture the creating session's sql_mode; replayed at fire time
        // by execute_trigger_body_inner so body semantics are stable.
        sql_mode: session.sql_mode.clone(),
        character_set_client: session.character_set_client.clone(),
        collation_connection: session.collation_connection.clone(),
        // B5 — overwritten with the target schema's real collation by
        // Database::create_trigger; this is a placeholder fallback.
        database_collation: session.collation_connection.clone(),
    }
}

fn trigger_timing_schema(timing: TriggerTiming) -> TriggerTimingSchema {
    match timing {
        TriggerTiming::Before => TriggerTimingSchema::Before,
        TriggerTiming::After => TriggerTimingSchema::After,
    }
}

fn trigger_event_schema(event: TriggerEvent) -> TriggerEventSchema {
    match event {
        TriggerEvent::Insert => TriggerEventSchema::Insert,
        TriggerEvent::Update => TriggerEventSchema::Update,
        TriggerEvent::Delete => TriggerEventSchema::Delete,
    }
}

fn trigger_order_schema(order: &TriggerOrder) -> TriggerOrderSchema {
    TriggerOrderSchema {
        kind: match order.kind {
            TriggerOrderKind::Follows => TriggerOrderKindSchema::Follows,
            TriggerOrderKind::Precedes => TriggerOrderKindSchema::Precedes,
        },
        other_trigger: order.other_trigger.clone(),
    }
}

fn validate_trigger_definer(db: &Database, trigger: &TriggerSchema) -> Result<(), QueryError> {
    super::routines::warn_missing_definer(
        db,
        &AccountKey::new(&trigger.definer_user, &trigger.definer_host),
    )
}

fn validate_trigger_body(
    session: &SessionContext,
    statements: &[RoutineStmt],
) -> Result<(), QueryError> {
    for stmt in statements {
        validate_trigger_stmt(session, stmt)?;
    }
    Ok(())
}

fn validate_trigger_stmt(session: &SessionContext, stmt: &RoutineStmt) -> Result<(), QueryError> {
    match stmt {
        RoutineStmt::Return(_) => Err(error(
            crate::error::catalog::ER_SP_NO_RETSET,
            "RETURN is not allowed in triggers",
        )),
        RoutineStmt::Prepare { .. }
        | RoutineStmt::Execute { .. }
        | RoutineStmt::Deallocate { .. } => Err(error(
            crate::error::catalog::ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG,
            "dynamic SQL is not allowed in triggers",
        )),
        RoutineStmt::Sql(sql) => {
            let statement = parse_statement(sql).map_err(|err| err.spec.error(err.message))?;
            if trigger_statement_returns_rows(&statement) {
                return Err(error(
                    crate::error::catalog::ER_SP_NO_RETSET,
                    "Not allowed to return a result set from a trigger",
                ));
            }
            super::routines::validate_trigger_sql_statement(session, &statement)?;
            Ok(())
        }
        RoutineStmt::DeclareHandler { statement, .. } => validate_trigger_stmt(session, statement),
        RoutineStmt::Block { statements, .. }
        | RoutineStmt::Loop { statements, .. }
        | RoutineStmt::While { statements, .. }
        | RoutineStmt::Repeat { statements, .. } => validate_trigger_body(session, statements),
        RoutineStmt::If {
            branches,
            else_branch,
        } => {
            for (_, statements) in branches {
                validate_trigger_body(session, statements)?;
            }
            validate_trigger_body(session, else_branch)
        }
        RoutineStmt::Case {
            branches,
            else_branch,
            ..
        } => {
            for (_, statements) in branches {
                validate_trigger_body(session, statements)?;
            }
            validate_trigger_body(session, else_branch)
        }
        _ => Ok(()),
    }
}

fn trigger_statement_returns_rows(statement: &Statement) -> bool {
    match statement {
        Statement::Select(_)
        | Statement::SetOperation(_)
        | Statement::Explain { .. }
        | Statement::ExplainForConnection { .. }
        | Statement::Describe { .. } => true,
        Statement::WithCte(with) => trigger_statement_returns_rows(&with.body),
        Statement::Insert(insert) => insert.returning.is_some(),
        Statement::Update(update) => update.returning.is_some(),
        Statement::Delete(delete) => delete.returning.is_some(),
        _ => false,
    }
}

/// Validate pseudo-row references structurally, including SQL subqueries and
/// unreachable branches. String literals/comments are never interpreted as SQL.
struct TriggerRowValidator {
    event: TriggerEvent,
    timing: TriggerTiming,
    error: Option<QueryError>,
}

impl TriggerRowValidator {
    fn qualifier(&mut self, qualifier: &str, write: bool) {
        if self.error.is_some() {
            return;
        }
        let old = qualifier.eq_ignore_ascii_case("OLD");
        let new = qualifier.eq_ignore_ascii_case("NEW");
        if (old && self.event == TriggerEvent::Insert)
            || (new && self.event == TriggerEvent::Delete)
        {
            self.error = Some(error(
                crate::error::catalog::ER_TRG_NO_SUCH_ROW_IN_TRG,
                format!("There is no {qualifier} row in this trigger"),
            ));
        } else if write && (old || (new && self.timing == TriggerTiming::After)) {
            self.error = Some(error(
                crate::error::catalog::ER_TRG_CANT_CHANGE_ROW,
                format!("Updating of {qualifier} row is not allowed in this trigger"),
            ));
        }
    }

    fn target(&mut self, target: &crate::parser::RoutineTarget) {
        if let crate::parser::RoutineTarget::Local(name) = target {
            if let Some((qualifier, _)) = name.split_once('.') {
                self.qualifier(qualifier, true);
            }
        }
    }

    fn expr(&mut self, expr: &mut Expr) {
        crate::parser::visit::visit_expr_mut(self, expr);
    }

    fn sql(&mut self, sql: &str) -> Result<(), QueryError> {
        let mut statement = parse_statement(sql).map_err(|err| err.spec.error(err.message))?;
        crate::parser::visit::visit_statement_mut(self, &mut statement);
        Ok(())
    }

    fn statements(&mut self, statements: &mut [RoutineStmt]) -> Result<(), QueryError> {
        for stmt in statements {
            self.statement(stmt)?;
        }
        Ok(())
    }

    fn statement(&mut self, stmt: &mut RoutineStmt) -> Result<(), QueryError> {
        match stmt {
            RoutineStmt::Block { statements, .. } | RoutineStmt::Loop { statements, .. } => {
                self.statements(statements)?
            }
            RoutineStmt::While {
                condition,
                statements,
                ..
            } => {
                self.expr(condition);
                self.statements(statements)?;
            }
            RoutineStmt::Repeat {
                until, statements, ..
            } => {
                self.expr(until);
                self.statements(statements)?;
            }
            RoutineStmt::DeclareVar { default, .. } => {
                if let Some(expr) = default {
                    self.expr(expr);
                }
            }
            RoutineStmt::DeclareHandler { statement, .. } => self.statement(statement)?,
            RoutineStmt::Set { assignments } => {
                for (target, expr) in assignments {
                    self.target(target);
                    self.expr(expr);
                }
            }
            RoutineStmt::SelectInto {
                select_sql,
                targets,
            } => {
                self.sql(select_sql)?;
                for target in targets {
                    self.target(target);
                }
            }
            RoutineStmt::DeclareCursor { select_sql, .. } | RoutineStmt::Sql(select_sql) => {
                self.sql(select_sql)?
            }
            RoutineStmt::If {
                branches,
                else_branch,
            } => {
                for (expr, statements) in branches {
                    self.expr(expr);
                    self.statements(statements)?;
                }
                self.statements(else_branch)?;
            }
            RoutineStmt::Case {
                operand,
                branches,
                else_branch,
            } => {
                if let Some(expr) = operand {
                    self.expr(expr);
                }
                for (expr, statements) in branches {
                    self.expr(expr);
                    self.statements(statements)?;
                }
                self.statements(else_branch)?;
            }
            RoutineStmt::Fetch { targets, .. } => {
                for target in targets {
                    self.target(target);
                }
            }
            RoutineStmt::Signal {
                message_text,
                mysql_errno,
                information,
                ..
            }
            | RoutineStmt::Resignal {
                message_text,
                mysql_errno,
                information,
                ..
            } => {
                for (_, expr) in information {
                    self.expr(expr);
                }
                if let Some(expr) = message_text {
                    self.expr(expr);
                }
                if let Some(expr) = mysql_errno {
                    self.expr(expr);
                }
            }
            RoutineStmt::Call(call) => {
                for expr in &mut call.args {
                    self.expr(expr);
                }
            }
            RoutineStmt::GetDiagnostics {
                condition, items, ..
            } => {
                if let Some(expr) = condition {
                    self.expr(expr);
                }
                for (target, _) in items {
                    self.target(target);
                }
            }
            RoutineStmt::Return(expr) | RoutineStmt::Prepare { source: expr, .. } => {
                self.expr(expr)
            }
            RoutineStmt::Execute { using, .. } => {
                for target in using {
                    self.target(target);
                }
            }
            RoutineStmt::DeclareCondition { .. }
            | RoutineStmt::Leave(_)
            | RoutineStmt::Iterate(_)
            | RoutineStmt::Open(_)
            | RoutineStmt::Close(_)
            | RoutineStmt::Deallocate { .. } => {}
        }
        Ok(())
    }
}

impl crate::parser::visit::AstVisitorMut for TriggerRowValidator {
    fn enter_expr(&mut self, expr: &mut Expr) {
        match expr {
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => self.qualifier(&parts[0], false),
            _ => {}
        }
    }
}

fn validate_trigger_rows(
    statements: &mut [RoutineStmt],
    event: TriggerEvent,
    timing: TriggerTiming,
) -> Result<(), QueryError> {
    let mut validator = TriggerRowValidator {
        event,
        timing,
        error: None,
    };
    validator.statements(statements)?;
    match validator.error {
        Some(err) => Err(err),
        None => Ok(()),
    }
}
