//! Session statements, transaction settings and access policy.
use super::*;

pub(super) fn parser_isolation_to_storage(
    isolation: crate::parser::TransactionIsolation,
) -> Result<IsolationLevel, QueryError> {
    Ok(match isolation {
        crate::parser::TransactionIsolation::ReadUncommitted => IsolationLevel::ReadUncommitted,
        crate::parser::TransactionIsolation::ReadCommitted => IsolationLevel::ReadCommitted,
        crate::parser::TransactionIsolation::RepeatableRead => IsolationLevel::RepeatableRead,
        crate::parser::TransactionIsolation::Serializable => IsolationLevel::Serializable,
    })
}

fn active_transaction_characteristics_error() -> QueryError {
    error(
        crate::error::catalog::ER_CANT_CHANGE_TX_CHARACTERISTICS,
        "Transaction characteristics can't be changed while a transaction is in progress",
    )
}

pub(in crate::engine::executor) fn ensure_xa_statement_allowed(
    session: &SessionContext,
    statement: &Statement,
) -> Result<(), QueryError> {
    if session
        .xa_branch
        .as_ref()
        .is_some_and(|b| b.state == crate::xa::XaBranchState::Idle)
        && !matches!(
            statement,
            Statement::XaStart { .. }
                | Statement::XaEnd { .. }
                | Statement::XaPrepare { .. }
                | Statement::XaCommit { .. }
                | Statement::XaRollback { .. }
                | Statement::XaRecover { .. }
        )
    {
        return Err(error(
            crate::error::catalog::ER_XAER_RMFAIL,
            "XAER_RMFAIL: XA branch is IDLE",
        ));
    }
    Ok(())
}

/// Unified session-level write-access guard. Runs at the top of statement
/// dispatch so the same gate fires before any side effect:
///
///   1. Session-scoped `transaction_read_only` (= read-only txn marker).
///   2. Global `super_read_only` — rejects every write (even for super).
///   3. Global `read_only`        — rejects non-super writes.
///
/// Replica admission supplies the common write/locking-read classification.
/// Transaction access additionally permits shared locking reads; their row
/// locks still use the ordinary lock manager.
pub(in crate::engine::executor) fn enforce_session_write_access(
    db: &Arc<Database>,
    session: &mut SessionContext,
    statement: &Statement,
) -> Result<(), QueryError> {
    ensure_xa_statement_allowed(session, statement)?;
    // (1) Session's transaction_read_only / BEGIN READ ONLY.
    let transaction_read_only = if session.active_txn_isolation.is_none() {
        session.next_txn_read_only.unwrap_or(session.read_only_txn)
    } else {
        session.read_only_txn
    };
    let super_only = crate::engine::sysvar::builtin::replication::super_read_only();
    let read_only = crate::engine::sysvar::builtin::replication::read_only();
    // Classifying the statement walks its AST; with every read-only mode
    // off there is nothing to enforce, which is the common case.
    if (!transaction_read_only && !super_only && !read_only)
        || !crate::replication::is_replica_write_statement(statement)
    {
        return Ok(());
    }
    if transaction_read_only
        && crate::statement_effects::is_read_only_transaction_forbidden(statement)
    {
        return Err(error(
            crate::error::catalog::ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION,
            "Cannot execute statement in a READ ONLY transaction.",
        ));
    }
    // (2/3) Global read_only / super_read_only.
    if !super_only && !read_only {
        return Ok(());
    }
    if super_only {
        return Err(error(crate::error::catalog::ER_OPTION_PREVENTS_STATEMENT,
            "The MySQL server is running with the --super-read-only option so it cannot execute this statement",
        ));
    }
    // Plain read_only: accounts that can bypass it may still write.
    if !can_bypass_read_only(db, session) {
        return Err(error(crate::error::catalog::ER_OPTION_PREVENTS_STATEMENT,
            "The MySQL server is running with the --read-only option so it cannot execute this statement",
        ));
    }
    Ok(())
}

/// B2: MySQL 8.0 lets an account write while `read_only` is set only if it
/// holds `CONNECTION_ADMIN` (SUPER implies it). No `user == "root"` special
/// case — root bypasses solely because it is bootstrapped with the privilege,
/// so a renamed root or a granted non-root account behaves per its grants.
fn can_bypass_read_only(db: &Arc<Database>, session: &SessionContext) -> bool {
    if session.internal_system_session {
        return true;
    }
    let required = crate::engine::auth::RequiredPrivilege {
        privilege: Privilege::Dynamic("CONNECTION_ADMIN".to_string()),
        scope: PrivilegeScope::Global,
    };
    db.with_auth_catalog(|auth| {
        auth.account_has_privilege(&session.authorization_context(), &required, false)
    })
    .unwrap_or(false)
}

/// L8: per-account `max_questions` / `max_updates` charges. We tick the
/// per-account counter on every statement and reject with 1226 when the
/// hour window's budget is exhausted. The session's `account_limits` is
/// the snapshot taken at login (MySQL semantics — `ALTER USER ... WITH
/// MAX_QUERIES_PER_HOUR N` takes effect for new connections, current
/// sessions keep the old budget).
pub(in crate::engine::executor) fn enforce_account_resource_limits(
    session: &SessionContext,
    statement: &Statement,
) -> Result<(), QueryError> {
    if session.internal_system_session {
        return Ok(()); // bootstrap root is exempt, matches MySQL's `mysql.*` bypass
    }
    let limits = &session.account_limits;
    crate::engine::auth::charge_question(&session.authenticated_as, limits.max_questions)?;
    if crate::replication::is_replica_write_statement(statement) {
        crate::engine::auth::charge_update(&session.authenticated_as, limits.max_updates)?;
    }
    Ok(())
}

/// Apply the session's `sql_select_limit` to a SELECT that didn't
/// declare its own LIMIT clause. MySQL's "DEFAULT" sentinel (no cap)
/// is represented as `u64::MAX` in the sysvar layer; any value below
/// it acts as a hard ceiling on rows returned to the client.
pub(in crate::engine::executor) fn apply_sql_select_limit(
    session: &SessionContext,
    stmt: &mut crate::parser::Select,
) {
    let cap = session.sysvars.sql_select_limit;
    if cap == u64::MAX || stmt.limit.is_some() {
        return;
    }
    // `LimitValue::Rows` stores a `usize`; clamp to its range.
    let lim = cap.min(usize::MAX as u64) as usize;
    stmt.limit = Some(crate::parser::LimitValue::Rows(lim));
}

pub(super) fn apply_transaction_isolation_scope(
    session: &mut SessionContext,
    scope: Option<SetScope>,
    isolation: IsolationLevel,
) -> Result<(), QueryError> {
    match scope {
        None => {
            if session.in_transaction() {
                return Err(active_transaction_characteristics_error());
            }
            session.next_txn_isolation = Some(isolation);
        }
        Some(SetScope::Session) => {
            session.isolation = isolation;
            if !session.in_transaction() {
                session.next_txn_isolation = None;
            }
        }
        Some(SetScope::Global) => {
            crate::engine::api::set_global_transaction_isolation(isolation);
        }
        // PERSIST/PERSIST_ONLY on SET TRANSACTION ISOLATION LEVEL is not
        // syntactically reachable today (the manual fast-path rejects
        // them at parse time), but list them explicitly so the compiler
        // catches any future plumbing change.
        Some(SetScope::Persist) | Some(SetScope::PersistOnly) => {
            return Err(error(
                crate::error::catalog::ER_PARSE_ERROR,
                "PERSIST/PERSIST_ONLY not allowed on SET TRANSACTION",
            ));
        }
    }
    Ok(())
}

#[inline(always)]
pub(super) fn apply_autocommit(
    db: &Arc<Database>,
    session: &mut SessionContext,
    value: bool,
) -> Result<(), QueryError> {
    ensure_no_xa_branch_for_local_txn(session, "SET autocommit")?;
    if session.autocommit != value {
        if value {
            commit_session_txn(db, session)?;
        }
        session.autocommit = value;
        session.mark_pfs_variables_dirty();
    }
    Ok(())
}

fn write_set_variable(
    db: &Arc<Database>,
    session: &mut SessionContext,
    name: &str,
    scope: crate::engine::sysvar::SetScope,
    value: crate::engine::sysvar::SysVarValue,
) -> Result<(), QueryError> {
    if name == "autocommit" && scope.requested_scope() == crate::engine::sysvar::Scope::Session {
        return apply_autocommit(db, session, value.as_bool().unwrap_or(true));
    }
    crate::engine::sysvar::write_var(name, scope, session, Some(db.as_ref()), value)
}

pub(in crate::engine::executor) fn execute_set_statement(
    db: &Arc<Database>,
    session: &mut SessionContext,
    assignments: &[SetAssignment],
) -> Result<QueryResponse, QueryError> {
    use crate::engine::sysvar;

    // Two-phase atomic execution for SYSTEM variables (MySQL 8.0
    // multi-assignment guarantees we don't leave a partial update behind
    // when one of the later assignments fails). USER variables (`@x`)
    // are applied left-to-right inline because later assignments are
    // documented to see earlier ones — `SET @a := 1, @b := @a + 1`
    // must produce `@b = 2`, which requires immediate application.
    enum PreparedSet {
        SystemDefault {
            name: String,
            set_scope: sysvar::SetScope,
            coerced: sysvar::SysVarValue,
        },
        SystemValue {
            name: String,
            set_scope: sysvar::SetScope,
            coerced: sysvar::SysVarValue,
        },
        UnknownVarWarning {
            message: String,
        },
    }

    // A `SET` value may be a scalar subquery (`SET @@session.sql_mode =
    // (SELECT REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY', ''))`, the
    // standard MySQL idiom). Obtain a read snapshot up front — but only
    // when a subquery is actually present, so a plain `SET x = 5` never
    // starts a transaction. `TxnView` is `Copy`; it threads down the
    // recursive value evaluator to any nested subquery.
    let subquery_txn = if assignments
        .iter()
        .any(|a| crate::engine::executor::prepared_dml::expr_contains_subquery(&a.value))
    {
        Some(ensure_statement_txn(db, session)?)
    } else {
        None
    };

    let mut prepared: Vec<PreparedSet> = Vec::with_capacity(assignments.len());

    for assignment in assignments {
        // ---- Normalise the variable name and scope qualifier. -----------
        let mut name = assignment.name.to_ascii_lowercase();
        let mut set_scope = sysvar::SetScope::from_parser(assignment.scope);
        if let Some(rest) = name.strip_prefix("@@") {
            name = rest.to_string();
        }
        for (prefix, scope) in [
            ("session.", sysvar::SetScope::Session),
            ("local.", sysvar::SetScope::Session),
            ("global.", sysvar::SetScope::Global),
            ("persist.", sysvar::SetScope::Persist),
            ("persist_only.", sysvar::SetScope::PersistOnly),
        ] {
            if let Some(rest) = name.strip_prefix(prefix) {
                set_scope = scope;
                name = rest.to_string();
                break;
            }
        }

        // ---- User-defined variables (`@x`). ------------------------------
        // Applied immediately so a later assignment can read the value
        // (MySQL: left-to-right semantics).
        if name.starts_with('@') {
            if matches!(
                set_scope,
                sysvar::SetScope::Global
                    | sysvar::SetScope::Persist
                    | sysvar::SetScope::PersistOnly
            ) {
                return Err(error(
                    crate::error::catalog::ER_INCORRECT_GLOBAL_LOCAL_VAR,
                    "user variables are session scoped only",
                ));
            }
            if matches!(
                assignment.value,
                Expr::Default(_) | Expr::DefaultQualified(_)
            ) {
                // SET @x = DEFAULT — MySQL: clear the user variable.
                session.set_user_variable(&name, Value::Null);
                continue;
            }
            let value = eval_set_assignment_value(session, db, subquery_txn, &assignment.value)?;
            session.set_user_variable(&name, value);
            continue;
        }

        if name == "autocommit" && set_scope.requested_scope() == sysvar::Scope::Session {
            ensure_no_xa_branch_for_local_txn(session, "SET autocommit")?;
        }

        // ---- SET <var> = DEFAULT: reset to compiled-in default. ----------
        if matches!(
            assignment.value,
            Expr::Default(_) | Expr::DefaultQualified(_)
        ) {
            let registry = sysvar::registry();
            let value = if set_scope.requested_scope() == sysvar::Scope::Session
                && registry
                    .get(&name)
                    .is_some_and(|var| var.scope() == sysvar::Scope::Both)
            {
                registry.read(&name, sysvar::Scope::Global, None, Some(db.as_ref()))?
            } else {
                registry
                    .compiled_default(&name)
                    .ok_or_else(|| sysvar::unknown_system_variable(&name))?
            };
            let coerced = registry.validate(&name, set_scope, session, Some(db.as_ref()), value)?;
            prepared.push(PreparedSet::SystemDefault {
                name,
                set_scope,
                coerced,
            });
            continue;
        }

        let value = eval_set_assignment_value(session, db, subquery_txn, &assignment.value)?;
        // Validate (coerce + scope/access check) WITHOUT writing.
        match sysvar::registry().validate_sql_value(
            &name,
            set_scope,
            session,
            Some(db.as_ref()),
            &value,
        ) {
            Ok(coerced) => prepared.push(PreparedSet::SystemValue {
                name,
                set_scope,
                coerced,
            }),
            Err(err) if err.is(crate::error::catalog::ER_UNKNOWN_SYSTEM_VARIABLE) => {
                // Unknown variable. PERSIST / PERSIST_ONLY must always
                // error (MySQL behaviour); other paths honour
                // MYDB_SYSVAR_UNKNOWN.
                if set_scope.is_persist() {
                    return Err(err);
                }
                match sysvar_unknown_policy() {
                    SysVarUnknownPolicy::Error => return Err(err),
                    SysVarUnknownPolicy::Warn => prepared.push(PreparedSet::UnknownVarWarning {
                        message: err.message().to_string(),
                    }),
                    SysVarUnknownPolicy::Ignore => {}
                }
            }
            Err(err) => return Err(err),
        }
    }

    // Phase 2 — apply system variables. Validation already filtered
    // every common error; the only failure modes left are PERSIST I/O
    // (which MySQL also reports late) and back-end-specific guards.
    for step in prepared {
        match step {
            PreparedSet::SystemDefault {
                name,
                set_scope,
                coerced,
            } => {
                write_set_variable(db, session, &name, set_scope, coerced)?;
                track_sysvar_session_change(session, db.as_ref(), &name);
            }
            PreparedSet::SystemValue {
                name,
                set_scope,
                coerced,
            } => {
                write_set_variable(db, session, &name, set_scope, coerced)?;
                // Empty sql_mode has two distinct origins: the compiled
                // mydb default keeps the historical strict posture, while an
                // explicit `SET sql_mode=''` requests MySQL's non-strict
                // assignment semantics. The string value alone cannot encode
                // that distinction, so retain it in the cached mode word.
                // `SET sql_mode=DEFAULT` takes the SystemDefault arm above and
                // intentionally does not set this marker.
                if name.eq_ignore_ascii_case("sql_mode")
                    && set_scope.requested_scope() == sysvar::Scope::Session
                {
                    session.temporal_sql_mode_bits |= crate::engine::api::TEMPORAL_MODE_EXPLICIT;
                }
                track_sysvar_session_change(session, db.as_ref(), &name);
            }
            PreparedSet::UnknownVarWarning { message } => {
                crate::engine::api::push_statement_warning(
                    crate::error::ConditionLevel::Warning,
                    crate::error::catalog::ER_UNKNOWN_SYSTEM_VARIABLE,
                    message,
                );
            }
        }
    }
    Ok(QueryResponse::Ok)
}

/// CLIENT_SESSION_TRACK: after a `SET` of a subscribed system variable,
/// queue its new session-visible value for the next OK packet's
/// `SESSION_TRACK_SYSTEM_VARIABLES` block. Reads the SESSION-scope
/// effective value so the client mirrors exactly what it would observe.
fn track_sysvar_session_change(
    session: &mut crate::engine::api::SessionContext,
    db: &crate::storage::Database,
    name: &str,
) {
    if !session.tracks_sysvar(name) {
        return;
    }
    if let Ok(value) = crate::engine::sysvar::registry().read(
        name,
        crate::engine::sysvar::meta::Scope::Session,
        Some(session),
        Some(db),
    ) {
        session.note_sysvar_change(name.to_string(), value.to_display_string());
    }
}

#[derive(Debug, Clone, Copy)]
enum SysVarUnknownPolicy {
    Error,
    Warn,
    Ignore,
}

fn sysvar_unknown_policy() -> SysVarUnknownPolicy {
    use std::sync::OnceLock;
    static POLICY: OnceLock<SysVarUnknownPolicy> = OnceLock::new();
    *POLICY.get_or_init(|| {
        // Unknown names are errors by default. Explicit compatibility
        // overrides remain available; persisted unknown names always fail.
        let raw = crate::util::env::var("MYDB_SYSVAR_UNKNOWN").unwrap_or_default();
        match raw.to_ascii_lowercase().as_str() {
            "warn" => SysVarUnknownPolicy::Warn,
            "ignore" | "off" | "silent" => SysVarUnknownPolicy::Ignore,
            _ => SysVarUnknownPolicy::Error,
        }
    })
}

pub(super) fn execute_set_names_statement(
    session: &mut SessionContext,
    charset: Option<&str>,
    collation: Option<&str>,
) -> Result<QueryResponse, QueryError> {
    let charset_info = match charset {
        Some(charset) => crate::charset::resolve_client_charset(charset)?,
        None => crate::charset::resolve_client_charset(crate::charset::DEFAULT_CHARSET)?,
    };
    let collation_name = match collation {
        Some(collation) => crate::charset::resolve_collation(collation)?.name,
        None => crate::charset::default_collation_for_charset(charset_info.name)?,
    };
    crate::charset::validate_charset_collation_pair(charset_info.name, collation_name)?;
    if charset_info.name == "utf8mb3" {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Warning,
            crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX,
            "'utf8mb3' is deprecated and will be removed in a future release. Please use utf8mb4 instead",
        );
    }
    session.character_set_client = charset_info.name.to_string();
    session.character_set_connection = charset_info.name.to_string();
    session.character_set_results = Some(charset_info.name.to_string());
    session.collation_connection = collation_name.to_string();
    Ok(QueryResponse::Ok)
}

/// `SET CHARACTER SET cs` / `SET CHARSET cs` / `SET CHARACTER SET DEFAULT`.
///
/// Differs from SET NAMES on `character_set_connection`: the connection
/// charset tracks the current database (mydb does not store per-database
/// charsets in the catalog yet, so it falls back to `character_set_database`
/// from the sysvar layer, which is currently the session value or the
/// session client charset — close enough to MySQL behaviour to satisfy
/// the protocol surface).
pub(super) fn execute_set_character_set_statement(
    session: &mut SessionContext,
    charset: Option<&str>,
) -> Result<QueryResponse, QueryError> {
    let charset_info = match charset {
        Some(charset) => crate::charset::resolve_client_charset(charset)?,
        None => crate::charset::resolve_client_charset(crate::charset::DEFAULT_CHARSET)?,
    };
    session.character_set_client = charset_info.name.to_string();
    session.character_set_results = Some(charset_info.name.to_string());
    // MySQL: character_set_connection follows character_set_database; we
    // keep it on the previous value rather than resetting, matching the
    // documented difference vs `SET NAMES`.
    Ok(QueryResponse::Ok)
}

/// Apply a `SET TRANSACTION ...` characteristics list. Bare scope (None)
/// targets *the next* transaction on this session (using `next_txn_*`
/// slots); explicit SESSION / GLOBAL writes change defaults.
pub(super) fn apply_transaction_characteristics(
    session: &mut SessionContext,
    scope: Option<SetScope>,
    characteristics: &[crate::parser::TransactionCharacteristic],
) -> Result<QueryResponse, QueryError> {
    use crate::parser::TransactionCharacteristic;
    for ch in characteristics {
        match ch {
            TransactionCharacteristic::Isolation(level) => {
                let storage = parser_isolation_to_storage(*level)?;
                apply_transaction_isolation_scope(session, scope, storage)?;
            }
            TransactionCharacteristic::ReadOnly(read_only) => {
                match scope {
                    None => {
                        if session.in_transaction() {
                            return Err(active_transaction_characteristics_error());
                        }
                        session.next_txn_read_only = Some(*read_only);
                    }
                    Some(SetScope::Session) => {
                        session.sysvars.transaction_read_only = *read_only;
                        if !session.in_transaction() {
                            session.read_only_txn = *read_only;
                        }
                        if !session.in_transaction() {
                            session.next_txn_read_only = None;
                        }
                    }
                    Some(SetScope::Global) => {
                        crate::engine::api::set_global_transaction_read_only(*read_only);
                    }
                    // PERSIST / PERSIST_ONLY are not legal on SET TRANSACTION
                    // (MySQL parser rejects them); we treat them as global
                    // writes so the test plus persist file behave sensibly.
                    Some(SetScope::Persist) | Some(SetScope::PersistOnly) => {
                        return Err(error(
                            crate::error::catalog::ER_PARSE_ERROR,
                            "PERSIST/PERSIST_ONLY not allowed on SET TRANSACTION",
                        ));
                    }
                }
            }
        }
    }
    Ok(QueryResponse::Ok)
}

/// `RESET PERSIST [IF EXISTS] [var_name]`.
pub(super) fn execute_reset_persist(
    db: &Arc<Database>,
    var: Option<&str>,
    if_exists: bool,
) -> Result<QueryResponse, QueryError> {
    let store = db.persisted_variables();
    match var {
        None => {
            store
                .clear()
                .map_err(|e| crate::engine::sysvar::errors::persist_write_failed("(all)", e))?;
        }
        Some(name) => {
            let removed = store
                .remove(name)
                .map_err(|e| crate::engine::sysvar::errors::persist_write_failed(name, e))?;
            if !removed && !if_exists {
                return Err(error(crate::error::catalog::ER_VAR_DOES_NOT_EXIST,
                    format!("Variable {name} does not exist in persisted_variables (use IF EXISTS to suppress)"),
                ));
            }
        }
    }
    Ok(QueryResponse::Ok)
}

fn eval_set_assignment_value(
    session: &SessionContext,
    db: &Arc<Database>,
    txn: Option<TxnView>,
    expr: &Expr,
) -> Result<Value, QueryError> {
    match expr {
        // Bare identifiers are MySQL charset/enum-style barewords (`SET
        // names = utf8mb4`). `@@` references have their own AST node.
        Expr::Identifier(name) => Ok(Value::String(name.clone())),
        Expr::CompoundIdentifier(parts) => Ok(Value::String(parts.join("."))),
        Expr::UserVariable(name) => Ok(session.user_variable(name).cloned().unwrap_or(Value::Null)),
        Expr::SystemVariable { scope, name } => {
            crate::engine::expr::eval_system_variable(*scope, name, Some(db))
        }
        Expr::Literal(Literal::Decimal(text))
            if crate::engine::expr::decimal_literal_exceeds_storage_precision(text) =>
        {
            // User variables retain the exact DECIMAL token. This differs
            // from an ordinary expression literal, which is normalized to
            // MySQL's 65-digit expression domain; retaining provenance here
            // is what makes subsequent arithmetic report 1690.
            Ok(Value::Decimal(text.clone()))
        }
        Expr::Literal(lit) => Ok(crate::engine::expr::literal_value(lit)),
        Expr::BoundValue(value) => Ok(value.clone()),
        Expr::Placeholder(idx) => crate::engine::params::current_param(*idx).ok_or_else(|| {
            error(
                crate::error::catalog::ER_WRONG_ARGUMENTS,
                "parameter is not bound",
            )
        }),
        // A scalar subquery — `SET @x = (SELECT ...)` /
        // `SET @@session.sql_mode = (SELECT REPLACE(@@sql_mode, ...))`.
        // MySQL evaluates it and uses the single value; >1 row is 1242,
        // >1 column is 1241, 0 rows is NULL. The read snapshot is
        // supplied by the caller (`execute_set_statement` obtains one
        // when any assignment value contains a subquery).
        Expr::Subquery(select) => {
            let txn = txn.ok_or_else(|| {
                error(
                    crate::error::catalog::ER_BAD_FIELD_ERROR,
                    "only constant expressions are supported here",
                )
            })?;
            crate::engine::executor::dml::execute_scalar_subquery_strict(db, session, txn, select)
        }
        Expr::Unary {
            op: crate::parser::UnaryOp::Neg,
            expr,
        } => crate::engine::expr::eval_negate(eval_set_assignment_value(session, db, txn, expr)?),
        Expr::Unary {
            op: crate::parser::UnaryOp::Not,
            expr,
        } => Ok(Value::Int(
            (!crate::engine::expr::truthy(&eval_set_assignment_value(session, db, txn, expr)?))
                as i64,
        )),
        Expr::Binary { left, op, right } => {
            match op {
                crate::parser::BinaryOp::And => {
                    let l = crate::engine::expr::logical_value(&eval_set_assignment_value(
                        session, db, txn, left,
                    )?);
                    if l == Some(false) {
                        return Ok(Value::Int(0));
                    }
                    let r = crate::engine::expr::logical_value(&eval_set_assignment_value(
                        session, db, txn, right,
                    )?);
                    return Ok(crate::engine::expr::combine_logical(
                        crate::parser::BinaryOp::And,
                        l,
                        r,
                    ));
                }
                crate::parser::BinaryOp::Concat => {
                    let l = eval_set_assignment_value(session, db, txn, left)?;
                    let r = eval_set_assignment_value(session, db, txn, right)?;
                    if matches!(l, Value::Null) || matches!(r, Value::Null) {
                        return Ok(Value::Null);
                    }
                    let ls = crate::engine::expr::string_value(&l);
                    let rs = crate::engine::expr::string_value(&r);
                    let mut s = String::with_capacity(ls.len() + rs.len());
                    s.push_str(&ls);
                    s.push_str(&rs);
                    return Ok(Value::String(s));
                }
                crate::parser::BinaryOp::Or => {
                    let l = crate::engine::expr::logical_value(&eval_set_assignment_value(
                        session, db, txn, left,
                    )?);
                    if l == Some(true) {
                        return Ok(Value::Int(1));
                    }
                    let r = crate::engine::expr::logical_value(&eval_set_assignment_value(
                        session, db, txn, right,
                    )?);
                    return Ok(crate::engine::expr::combine_logical(
                        crate::parser::BinaryOp::Or,
                        l,
                        r,
                    ));
                }
                _ => {}
            }
            let l = eval_set_assignment_value(session, db, txn, left)?;
            let r = eval_set_assignment_value(session, db, txn, right)?;
            match op {
                crate::parser::BinaryOp::And
                | crate::parser::BinaryOp::Or
                | crate::parser::BinaryOp::Concat => unreachable!(),
                crate::parser::BinaryOp::Xor => crate::engine::expr::eval_logical_xor(&l, &r),
                crate::parser::BinaryOp::Eq
                | crate::parser::BinaryOp::NullSafeEq
                | crate::parser::BinaryOp::NotEq
                | crate::parser::BinaryOp::Lt
                | crate::parser::BinaryOp::LtEq
                | crate::parser::BinaryOp::Gt
                | crate::parser::BinaryOp::GtEq => {
                    crate::engine::expr::null_cmp_binary(&l, *op, &r)
                }
                crate::parser::BinaryOp::Add
                | crate::parser::BinaryOp::Sub
                | crate::parser::BinaryOp::Mul
                | crate::parser::BinaryOp::Div
                | crate::parser::BinaryOp::Mod => crate::engine::expr::eval_arithmetic(&l, *op, &r),
                crate::parser::BinaryOp::IntDiv => crate::engine::expr::eval_int_div_values(&l, &r),
                crate::parser::BinaryOp::BitAnd
                | crate::parser::BinaryOp::BitOr
                | crate::parser::BinaryOp::BitXor
                | crate::parser::BinaryOp::ShiftLeft
                | crate::parser::BinaryOp::ShiftRight => {
                    crate::engine::expr::eval_bitwise(l, *op, r)
                }
                crate::parser::BinaryOp::Arrow | crate::parser::BinaryOp::LongArrow => {
                    crate::engine::expr::eval_json_arrow(
                        l,
                        r,
                        *op == crate::parser::BinaryOp::LongArrow,
                    )
                }
            }
        }
        Expr::Like {
            expr,
            pattern,
            negated,
            escape_char,
        } => {
            // B2: `SET x = (col LIKE 'foo')` goes through the shared
            // matcher so it inherits the same NULL/collation rules as
            // WHERE/HAVING LIKE. The prior byte-exact call quietly lost
            // CI folding under `utf8mb4_0900_ai_ci`.
            let value = eval_set_assignment_value(session, db, txn, expr)?;
            let pat = eval_set_assignment_value(session, db, txn, pattern)?;
            Ok(crate::engine::expr::eval_like_values(
                &value,
                &pat,
                *negated,
                *escape_char,
            ))
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => crate::engine::expr::eval_case(operand, conditions, results, else_result, |e| {
            eval_set_assignment_value(session, db, txn, e)
        }),
        Expr::Function { name, args, .. } => {
            crate::engine::expr::eval_function(name, args, |arg| {
                eval_set_assignment_value(session, db, txn, arg)
            })
        }
        _ => Err(error(
            crate::error::catalog::ER_BAD_FIELD_ERROR,
            "only constant expressions are supported here",
        )),
    }
}

// `parse_u64_variable_value` / `parse_bool_variable_value` used to live
// here as helpers for the legacy SET dispatcher. With the sysvar
// registry now owning value coercion (via `DataType::coerce`), these
// shims are gone — leave the imports / functions removed.
