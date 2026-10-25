use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, OnceLock};

use lru::LruCache;

use crate::engine::api::{QueryResponse, SessionContext};
use crate::engine::auth::{account_id, AccountKey, Privilege, PrivilegeScope, RequiredPrivilege};
use crate::engine::expr::{
    eval_arithmetic, eval_bit_not, eval_bitwise, eval_case, eval_function, eval_json_arrow,
    eval_logical_xor, eval_negate, eval_system_variable, literal_value, null_cmp_binary, truthy,
};
use crate::engine::params;
use crate::engine::session_registry::SessionRegistry;
use crate::parser::{
    parse_routine_body_sql, parse_statement, BinaryOp, DiagItem, Expr, RoutineBody, RoutineCall,
    RoutineCondition, RoutineDef, RoutineHandlerAction, RoutineKind, RoutineParamMode,
    RoutineSecurity, RoutineSqlDataAccess, RoutineStmt, RoutineTarget, UnaryOp,
};
use crate::storage::{
    error, split_table_name, DataTypeKind, Database, RoutineKindSchema, RoutineParamModeSchema,
    RoutineParamSchema, RoutineSchema, RoutineSecuritySchema, RoutineSqlDataAccessSchema,
    TableSchema, TriggerSchema,
};
use crate::types::{QueryError, Value};

pub(crate) const PREPARED_CALL_OUT_MARKER: &str = "__mydb_ps_out_params";

#[derive(Debug)]
pub(crate) struct RoutineProgram {
    body: RoutineBody,
    /// Static SQL is parsed and placeholder-numbered once per routine schema
    /// version. Invocation still clones the AST because local binding is
    /// call-specific, but it never pays the parser cost on the hot path.
    static_statements: Arc<HashMap<String, CompiledStaticStatement>>,
    parameterized_statements:
        Arc<HashMap<String, HashMap<Vec<String>, ParameterizedStaticStatement>>>,
    /// Follows the last Arc<RoutineProgram> owner, including invocations that
    /// outlive cache eviction, so the unified governor never refunds early.
    _memory_charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

#[derive(Debug, Clone)]
struct CompiledStaticStatement {
    statement: Arc<crate::parser::Statement>,
    identifiers: Arc<[String]>,
}

#[derive(Debug, Clone)]
struct ParameterizedStaticStatement {
    statement: Arc<crate::parser::Statement>,
    slot_names: Arc<[String]>,
}

type RoutineCacheKey = (String, String, String, u64, u64, String);
type TriggerCacheKey = (String, String, u64, u64, String);

static ROUTINE_CACHE: OnceLock<Mutex<LruCache<RoutineCacheKey, Arc<RoutineProgram>>>> =
    OnceLock::new();
static TRIGGER_CACHE: OnceLock<Mutex<LruCache<TriggerCacheKey, Arc<RoutineProgram>>>> =
    OnceLock::new();
static ROUTINE_COMPILE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static TRIGGER_COMPILE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn routine_cache_capacity() -> NonZeroUsize {
    static CAPACITY: OnceLock<NonZeroUsize> = OnceLock::new();
    *CAPACITY.get_or_init(|| {
        let capacity = crate::util::env::var("MYDB_ROUTINE_CACHE_SIZE")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .unwrap_or(1024)
            .max(1);
        NonZeroUsize::new(capacity).expect("routine cache capacity is nonzero")
    })
}

fn routine_cache() -> &'static Mutex<LruCache<RoutineCacheKey, Arc<RoutineProgram>>> {
    ROUTINE_CACHE.get_or_init(|| Mutex::new(LruCache::new(routine_cache_capacity())))
}

fn trigger_cache() -> &'static Mutex<LruCache<TriggerCacheKey, Arc<RoutineProgram>>> {
    TRIGGER_CACHE.get_or_init(|| Mutex::new(LruCache::new(routine_cache_capacity())))
}

thread_local! {
    static ROUTINE_DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    static TRIGGER_STACK: std::cell::RefCell<Vec<(String, String)>> =
        const { std::cell::RefCell::new(Vec::new()) };
    /// Active routine invocations on this thread as `(is_function, db, name)`,
    /// used to enforce MySQL recursion rules: procedures may recurse up to
    /// `max_sp_recursion_depth`, functions not at all (error 1424).
    static ROUTINE_STACK: std::cell::RefCell<Vec<(bool, String, String)>> =
        const { std::cell::RefCell::new(Vec::new()) };
    /// Availability of the `OLD` / `NEW` pseudo-rows for the trigger body
    /// currently executing on this thread (B2). `None` outside any trigger.
    /// Set/cleared with save-restore in execute_trigger_body_inner (set) and
    /// execute_routine_inner (cleared, so a routine CALLed from a trigger
    /// does not inherit the trigger's OLD/NEW visibility).
    static TRIGGER_ACCESS: std::cell::RefCell<Option<TriggerAccess>> = const { std::cell::RefCell::new(None) };
    static TRIGGER_ROW_AVAIL: std::cell::Cell<Option<TriggerRowAvail>> =
        const { std::cell::Cell::new(None) };
}

struct TriggerAccess {
    auth: Arc<crate::engine::auth::AuthCatalog>,
    context: crate::engine::auth::AuthorizationContext,
    database: String,
    table: String,
}

struct TriggerAccessGuard(Option<TriggerAccess>);

impl Drop for TriggerAccessGuard {
    fn drop(&mut self) {
        TRIGGER_ACCESS.with(|cell| *cell.borrow_mut() = self.0.take());
    }
}

/// Pseudo-row values become routine locals before SQL authorization. Check
/// their source column before binding, or SELECT could be bypassed by NEW.x.
fn check_trigger_column(
    name: &str,
    privilege: crate::engine::auth::Privilege,
) -> Result<(), QueryError> {
    if trigger_row_avail().is_none() {
        return Ok(());
    }
    let Some((qualifier, column)) = name.split_once('.') else {
        return Ok(());
    };
    if !qualifier.eq_ignore_ascii_case("OLD") && !qualifier.eq_ignore_ascii_case("NEW") {
        return Ok(());
    }
    TRIGGER_ACCESS.with(|cell| {
        let access = cell.borrow();
        let Some(access) = access.as_ref() else {
            return Ok(());
        };
        let required = crate::engine::auth::RequiredPrivilege {
            privilege,
            scope: crate::engine::auth::PrivilegeScope::Column {
                database: access.database.clone(),
                table: access.table.clone(),
                column: column.to_string(),
            },
        };
        if access
            .auth
            .account_has_privilege(&access.context, &required, false)
        {
            Ok(())
        } else {
            Err(crate::engine::auth::privilege_denied(
                &required.privilege,
                &required.scope,
            ))
        }
    })
}

/// Which pseudo-rows a trigger body may reference, plus the event label used
/// to format the 1363 error text.
#[derive(Debug, Clone, Copy)]
struct TriggerRowAvail {
    has_old: bool,
    has_new: bool,
    event: &'static str,
}

fn trigger_row_avail() -> Option<TriggerRowAvail> {
    TRIGGER_ROW_AVAIL.with(|cell| cell.get())
}

fn set_trigger_row_avail(value: Option<TriggerRowAvail>) -> Option<TriggerRowAvail> {
    TRIGGER_ROW_AVAIL.with(|cell| cell.replace(value))
}

/// B2 — reject a reference to `OLD.x` in an INSERT trigger (no OLD row) or
/// `NEW.x` in a DELETE trigger (no NEW row) with MySQL's error 1363, instead
/// of the generic "unknown column" / "undeclared variable" errors. A no-op
/// outside a trigger or for non-OLD/NEW qualifiers.
fn check_trigger_row_qualifier(qualifier: &str) -> Result<(), QueryError> {
    let Some(avail) = trigger_row_avail() else {
        return Ok(());
    };
    let is_old = qualifier.eq_ignore_ascii_case("OLD");
    let is_new = qualifier.eq_ignore_ascii_case("NEW");
    if is_old && !avail.has_old {
        return Err(error(
            crate::error::catalog::ER_TRG_NO_SUCH_ROW_IN_TRG,
            format!("There is no OLD row in on {} trigger", avail.event),
        ));
    }
    if is_new && !avail.has_new {
        return Err(error(
            crate::error::catalog::ER_TRG_NO_SUCH_ROW_IN_TRG,
            format!("There is no NEW row in on {} trigger", avail.event),
        ));
    }
    Ok(())
}

pub(super) fn execute_create_routine(
    db: &Arc<Database>,
    session: &mut SessionContext,
    def: RoutineDef,
) -> Result<QueryResponse, QueryError> {
    super::implicit_commit_for_ddl(db, session)?;
    validate_routine_def(&def)?;
    validate_binlog_function_creator(db, session, &def)?;
    validate_routine_definer_privilege(db, session, &def)?;
    let if_not_exists = def.if_not_exists;
    let schema = routine_schema_from_def(db, session, def);
    validate_routine_definer(db, &schema)?;
    if crate::engine::sys_schema::routine_definition(
        &schema.database,
        schema.kind.clone(),
        &schema.name,
    )
    .is_some()
    {
        let message = format!("{} {} already exists", schema.kind.as_str(), schema.name);
        if if_not_exists {
            crate::engine::api::push_statement_warning(
                crate::error::ConditionLevel::Note,
                crate::error::catalog::ER_SP_ALREADY_EXISTS,
                message,
            );
            return Ok(QueryResponse::Ok);
        }
        return Err(error(crate::error::catalog::ER_SP_ALREADY_EXISTS, message));
    }
    let native_collision = schema.kind == RoutineKindSchema::Function
        && (crate::engine::cbo::function_cost::is_known_builtin_function(&schema.name)
            || crate::planner::is_aggregate_function(&schema.name));
    let routine_name = schema.name.clone();
    let target_database = schema.database.clone();
    // Compile and reserve the version-1 program before publishing catalog
    // state. CREATE therefore never succeeds only to make the first CALL fail
    // for parser/cache memory, and the first invocation is a cache hit.
    compile_routine(&schema)?;
    let created = db.create_routine(
        &target_database,
        schema,
        false,
        if_not_exists,
        Some(&session.authenticated_as),
        crate::engine::sysvar::builtin::transaction::automatic_sp_privileges(),
    )?;
    if !created {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Note,
            crate::error::catalog::ER_SP_ALREADY_EXISTS,
            "Routine already exists",
        );
        return Ok(QueryResponse::Ok);
    }
    if native_collision {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Note,
            crate::error::catalog::ER_NATIVE_FCT_NAME_COLLISION,
            format!("This function '{routine_name}' has the same name as a native function"),
        );
    }
    crate::engine::cache::clear_global_caches();
    super::dispatch::revalidate_routine_views(db, session, &target_database, &routine_name)?;
    Ok(QueryResponse::Ok)
}

pub(super) fn execute_alter_routine(
    db: &Arc<Database>,
    session: &mut SessionContext,
    kind: RoutineKind,
    name: &str,
    characteristics: crate::parser::RoutineCharacteristicsPatch,
) -> Result<QueryResponse, QueryError> {
    super::implicit_commit_for_ddl(db, session)?;
    if let Some(comment) = characteristics.comment.as_deref() {
        validate_routine_comment(comment)?;
    }
    let (database, routine_name) = split_table_name(&session.current_database, name);
    db.alter_routine(
        &session.current_database,
        routine_kind_schema(kind),
        name,
        |routine| {
            if let Some(deterministic) = characteristics.deterministic {
                routine.deterministic = deterministic;
            }
            if let Some(sql_data_access) = characteristics.sql_data_access {
                routine.sql_data_access = routine_data_access_schema(sql_data_access);
            }
            if let Some(security) = characteristics.security {
                routine.security_type = routine_security_schema(security);
            }
            if let Some(comment) = characteristics.comment {
                routine.comment = comment;
            }
            routine.sql_mode = session.sql_mode.clone();
        },
    )?;
    invalidate_routine_cache(&database, kind, &routine_name);
    crate::engine::cache::clear_global_caches();
    Ok(QueryResponse::Ok)
}

pub(super) fn execute_drop_routine(
    db: &Arc<Database>,
    session: &mut SessionContext,
    kind: RoutineKind,
    name: &str,
    if_exists: bool,
) -> Result<QueryResponse, QueryError> {
    super::implicit_commit_for_ddl(db, session)?;
    let (database, routine_name) = split_table_name(&session.current_database, name);
    let dropped = db.drop_routine(
        &session.current_database,
        routine_kind_schema(kind),
        name,
        if_exists,
    )?;
    if !dropped {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Note,
            crate::error::catalog::ER_SP_DOES_NOT_EXIST,
            format!("{} {} does not exist", kind.as_str(), name),
        );
        return Ok(QueryResponse::Ok);
    }
    invalidate_routine_cache(&database, kind, &routine_name);
    crate::engine::cache::clear_global_caches();
    Ok(QueryResponse::Ok)
}

pub(super) fn execute_call(
    db: &Arc<Database>,
    session: &mut SessionContext,
    call: RoutineCall,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    if let Some(result) = crate::engine::sys_schema::execute_call(db, session, &call) {
        return result;
    }
    let mut caller_locals = None;
    let result = execute_procedure_call(db, session, call, registry, caller_locals.as_mut(), None);
    // A procedure containing only local expressions still opens a read view.
    // Finish that implicit transaction, but honor any START TRANSACTION or
    // autocommit change deliberately left by the procedure for its caller.
    let result = super::tx::finish_statement_txn(db, session, result)?;
    let mut responses = result.responses;
    if !result.prepared_out_params.is_empty() {
        let mut columns = Vec::with_capacity(result.prepared_out_params.len());
        let mut row = Vec::with_capacity(result.prepared_out_params.len());
        for (name, column_type, value) in result.prepared_out_params {
            let mut source = crate::types::ColumnSource::default();
            source.org_table = PREPARED_CALL_OUT_MARKER.to_string();
            columns.push(crate::types::Column {
                source,
                name,
                column_type,
            });
            row.push(value);
        }
        responses.push(QueryResponse::ResultSet(crate::engine::api::ResultSet {
            columns,
            rows: vec![row],
        }));
    }
    responses.push(QueryResponse::Ok);
    Ok(QueryResponse::MultiResult(responses))
}

/// Resolve user-defined stored-function calls before optimization. Until the
/// compiler can prove purity from the complete (including nested) call graph,
/// every call remains row-by-row and pins execution to the serial connection
/// thread. User-supplied `DETERMINISTIC` / `NO SQL` characteristics are
/// metadata and are deliberately insufficient to authorize folding.
pub(super) fn resolve_stored_functions(
    db: &Arc<Database>,
    session: &mut SessionContext,
    statement: &mut crate::parser::Statement,
    registry: &SessionRegistry,
) -> Result<StoredFunctionResolution, QueryError> {
    if !matches!(statement, crate::parser::Statement::Call(_)) && !db.has_stored_functions() {
        return Ok(StoredFunctionResolution::default());
    }
    let mut resolution = StoredFunctionResolution::default();
    resolve_statement_functions(db, session, statement, registry, &mut resolution)?;
    Ok(resolution)
}

/// Prepared fast paths have no routine invocation context or dependency locks.
/// Classify the AST without evaluating expressions so those statements fall
/// back to the full dispatcher, including functions in predicates/subqueries.
pub(super) fn statement_references_stored_function(
    db: &Database,
    current_database: &str,
    statement: &crate::parser::Statement,
) -> bool {
    db.has_stored_functions()
        && crate::parser::collect_statement_function_names(statement)
            .iter()
            .any(|name| {
                (name.contains('.')
                    || (!crate::engine::cbo::function_cost::is_known_builtin_function(name)
                        && !crate::planner::is_aggregate_function(name)))
                    && db
                        .routine_definition(current_database, RoutineKindSchema::Function, name)
                        .is_some()
            })
}

/// Cheap source-free SELECT guard used by the protocol inline executor.
///
/// The full resolver still runs in the ordinary dispatcher because it also
/// performs privilege checks, pins routine versions and collects MDL. This
/// predicate only prevents the constant SELECT fast path from evaluating a
/// stored function without installing that machinery first.
pub(super) fn select_references_stored_function(
    db: &Database,
    current_database: &str,
    select: &crate::parser::Select,
) -> bool {
    if !db.has_stored_functions() {
        return false;
    }
    select.projection.iter().any(|item| match item {
        crate::parser::SelectItem::Expr { expr, .. } => {
            expr_references_stored_function(db, current_database, expr)
        }
        crate::parser::SelectItem::Wildcard | crate::parser::SelectItem::QualifiedWildcard(_) => {
            false
        }
    })
}

fn expr_references_stored_function(db: &Database, current_database: &str, expr: &Expr) -> bool {
    let nested = |expr: &Expr| expr_references_stored_function(db, current_database, expr);
    match expr {
        Expr::Function { name, args, .. } => {
            db.routine_definition(current_database, RoutineKindSchema::Function, name)
                .is_some()
                || args.iter().any(nested)
        }
        Expr::Unary { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::GroupKeyRef { expr, .. } => nested(expr),
        Expr::Binary { left, right, .. }
        | Expr::Like {
            expr: left,
            pattern: right,
            ..
        } => nested(left) || nested(right),
        Expr::InList { expr, list, .. } => nested(expr) || list.iter().any(nested),
        Expr::InSubquery { expr, subquery, .. } => {
            nested(expr)
                || subquery.projection.iter().any(|item| match item {
                    crate::parser::SelectItem::Expr { expr, .. } => nested(expr),
                    _ => false,
                })
        }
        Expr::QuantifiedComparison { left, right, .. } => {
            nested(left)
                || match right {
                    crate::parser::QuantifiedComparisonRight::Subquery(select) => {
                        select.projection.iter().any(|item| match item {
                            crate::parser::SelectItem::Expr { expr, .. } => nested(expr),
                            _ => false,
                        })
                    }
                    crate::parser::QuantifiedComparisonRight::List(list) => list.iter().any(nested),
                }
        }
        Expr::Subquery(select)
        | Expr::Exists {
            subquery: select, ..
        } => select.projection.iter().any(|item| match item {
            crate::parser::SelectItem::Expr { expr, .. } => nested(expr),
            _ => false,
        }),
        Expr::Between {
            expr, low, high, ..
        } => nested(expr) || nested(low) || nested(high),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand.as_deref().is_some_and(nested)
                || conditions.iter().any(nested)
                || results.iter().any(nested)
                || else_result.as_deref().is_some_and(nested)
        }
        Expr::WindowFunction { args, spec, .. } => {
            let crate::parser::WindowSpec {
                partition_by,
                order_by,
                frame,
                ..
            } = spec.as_ref();

            args.iter().any(nested)
                || partition_by.iter().any(nested)
                || order_by.iter().any(|order| nested(&order.expr))
                || frame.as_ref().is_some_and(|frame| {
                    let bound_has_function = |bound: &crate::parser::WindowFrameBound| match bound {
                        crate::parser::WindowFrameBound::Preceding(expr)
                        | crate::parser::WindowFrameBound::Following(expr) => nested(expr),
                        _ => false,
                    };
                    bound_has_function(&frame.start) || bound_has_function(&frame.end)
                })
        }
        Expr::Tuple(items) => items.iter().any(nested),
        _ => false,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct ResolvedRoutineMdl {
    pub(super) database: String,
    pub(super) name: String,
    pub(super) kind: &'static str,
}

#[derive(Debug, Default)]
pub(super) struct StoredFunctionResolution {
    pub(super) needs_serial: bool,
    pub(super) routines: BTreeSet<ResolvedRoutineMdl>,
    pub(super) tables: BTreeMap<(String, String), bool>,
    /// Tables reached from a stored-function root. Writes in this set may
    /// not overlap a table used by the invoking statement (MySQL 1442).
    /// Procedure-only CALL roots are intentionally excluded.
    pub(super) restricted_tables: BTreeMap<(String, String), bool>,
}

impl StoredFunctionResolution {
    pub(super) fn merge(&mut self, other: Self) {
        self.needs_serial |= other.needs_serial;
        self.routines.extend(other.routines);
        for (table, write) in other.tables {
            self.tables
                .entry(table)
                .and_modify(|existing| *existing |= write)
                .or_insert(write);
        }
        for (table, write) in other.restricted_tables {
            self.restricted_tables
                .entry(table)
                .and_modify(|existing| *existing |= write)
                .or_insert(write);
        }
    }
}

/// Executor entry used by the expression engine's stored-function callback:
/// run user function `name` with already-evaluated argument `args` and return
/// its scalar result. Errors 1305 when no such function exists.
pub(crate) fn eval_stored_function_row(
    db: &Arc<Database>,
    session: &mut SessionContext,
    name: &str,
    args: Vec<Value>,
    registry: &SessionRegistry,
) -> Result<Value, QueryError> {
    execute_function_value(db, session, name, args, registry)
}

pub(crate) fn eval_stored_function_resolved(
    db: &Arc<Database>,
    session: &mut SessionContext,
    routine: &RoutineSchema,
    program: &Arc<RoutineProgram>,
    args: Vec<Value>,
    registry: &SessionRegistry,
) -> Result<Value, QueryError> {
    execute_function_schema_with_program(db, session, routine, args, registry, Some(program))
}

fn routine_schema_from_def(
    db: &Database,
    session: &SessionContext,
    def: RoutineDef,
) -> RoutineSchema {
    let (database, name) = split_table_name(&session.current_database, &def.name);
    let database_collation = db
        .catalog()
        .databases
        .get(&crate::storage::normalize_name(&database))
        .map(|schema| schema.collation.clone())
        .unwrap_or_else(|| session.collation_connection.clone());
    let definer = def.definer.unwrap_or_else(|| crate::parser::AccountIdent {
        user: session.authenticated_as.user.clone(),
        host: session.authenticated_as.host.clone(),
    });
    RoutineSchema {
        database,
        name,
        kind: routine_kind_schema(def.kind),
        definer_user: definer.user,
        definer_host: definer.host,
        security_type: routine_security_schema(def.characteristics.security),
        params: def
            .params
            .into_iter()
            .map(|param| RoutineParamSchema {
                name: param.name,
                mode: routine_param_mode_schema(param.mode),
                data_type: DataTypeKind::from(param.data_type),
                unsigned: param.unsigned,
                data_type_sql: param.data_type_sql,
            })
            .collect(),
        return_type: def.return_type.map(DataTypeKind::from),
        return_type_sql: def.return_type_sql,
        return_unsigned: def.return_unsigned,
        deterministic: def.characteristics.deterministic,
        sql_data_access: routine_data_access_schema(def.characteristics.sql_data_access),
        comment: def.characteristics.comment,
        sql_mode: session.sql_mode.clone(),
        body_sql: def.body.sql,
        schema_version: 1,
        created_ms: 0,
        updated_ms: 0,
        character_set_client: session.character_set_client.clone(),
        collation_connection: session.collation_connection.clone(),
        database_collation,
    }
}

pub(crate) fn routine_kind_schema(kind: RoutineKind) -> RoutineKindSchema {
    match kind {
        RoutineKind::Procedure => RoutineKindSchema::Procedure,
        RoutineKind::Function => RoutineKindSchema::Function,
    }
}

fn routine_param_mode_schema(mode: RoutineParamMode) -> RoutineParamModeSchema {
    match mode {
        RoutineParamMode::In => RoutineParamModeSchema::In,
        RoutineParamMode::Out => RoutineParamModeSchema::Out,
        RoutineParamMode::InOut => RoutineParamModeSchema::InOut,
    }
}

fn routine_security_schema(security: RoutineSecurity) -> RoutineSecuritySchema {
    match security {
        RoutineSecurity::Definer => RoutineSecuritySchema::Definer,
        RoutineSecurity::Invoker => RoutineSecuritySchema::Invoker,
    }
}

fn routine_data_access_schema(access: RoutineSqlDataAccess) -> RoutineSqlDataAccessSchema {
    match access {
        RoutineSqlDataAccess::ContainsSql => RoutineSqlDataAccessSchema::ContainsSql,
        RoutineSqlDataAccess::NoSql => RoutineSqlDataAccessSchema::NoSql,
        RoutineSqlDataAccess::ReadsSqlData => RoutineSqlDataAccessSchema::ReadsSqlData,
        RoutineSqlDataAccess::ModifiesSqlData => RoutineSqlDataAccessSchema::ModifiesSqlData,
    }
}

fn validate_routine_def(def: &RoutineDef) -> Result<(), QueryError> {
    let (_, routine_name) = split_table_name("", &def.name);
    if routine_name.chars().count() > 64 {
        return Err(error(
            crate::error::catalog::ER_TOO_LONG_IDENT,
            format!("Identifier name '{routine_name}' is too long"),
        ));
    }
    validate_routine_comment(&def.characteristics.comment)?;
    let mut params = BTreeSet::new();
    for param in &def.params {
        if param.name.chars().count() > 64 {
            return Err(error(
                crate::error::catalog::ER_TOO_LONG_IDENT,
                format!("Identifier name '{}' is too long", param.name),
            ));
        }
        if !params.insert(normalize_local(&param.name)) {
            return Err(error(
                crate::error::catalog::ER_SP_DUP_PARAM,
                format!("Duplicate parameter: {}", param.name),
            ));
        }
    }
    validate_routine_structure(&def.body.statements, &mut Vec::new(), &BTreeMap::new())?;
    compile_static_statements(&def.body)?;
    if def.kind == RoutineKind::Function {
        if def.return_type.is_none() {
            return Err(error(
                crate::error::catalog::ER_SP_NO_RETSET,
                "CREATE FUNCTION requires RETURNS",
            ));
        }
        if !def.body.statements.iter().any(routine_stmt_has_return) {
            return Err(error(
                crate::error::catalog::ER_SP_NO_RETSET,
                "CREATE FUNCTION requires at least one RETURN",
            ));
        }
        for stmt in &def.body.statements {
            validate_function_stmt(stmt)?;
        }
    } else if def.body.statements.iter().any(routine_stmt_has_return) {
        return Err(error(
            crate::error::catalog::ER_SP_BADRETURN,
            "RETURN is only allowed in a FUNCTION",
        ));
    }
    Ok(())
}

fn validate_routine_comment(comment: &str) -> Result<(), QueryError> {
    if comment.chars().count() > 64 {
        Err(error(
            crate::error::catalog::ER_TOO_LONG_ROUTINE_COMMENT,
            "Comment for routine is too long (max = 64)",
        ))
    } else {
        Ok(())
    }
}

fn validate_routine_structure(
    statements: &[RoutineStmt],
    labels: &mut Vec<(String, bool)>,
    inherited_conditions: &BTreeMap<String, RoutineCondition>,
) -> Result<(), QueryError> {
    let mut declaration_phase = 0u8;
    let mut variables = BTreeSet::new();
    let mut conditions = inherited_conditions.clone();
    let mut cursors = BTreeSet::new();
    let mut handler_conditions = BTreeSet::new();
    for stmt in statements {
        let phase = match stmt {
            RoutineStmt::DeclareVar { .. } | RoutineStmt::DeclareCondition { .. } => 0,
            RoutineStmt::DeclareCursor { .. } => 1,
            RoutineStmt::DeclareHandler { .. } => 2,
            _ => 3,
        };
        if phase < declaration_phase {
            return Err(error(
                crate::error::catalog::ER_SP_VARCOND_AFTER_CURSHNDLR,
                "Variable or condition declaration after cursor or handler declaration",
            ));
        }
        declaration_phase = declaration_phase.max(phase);
        match stmt {
            RoutineStmt::DeclareVar { names, .. } => {
                for name in names {
                    if !variables.insert(normalize_local(name)) {
                        return Err(error(
                            crate::error::catalog::ER_SP_DUP_VAR,
                            format!("Duplicate variable: {name}"),
                        ));
                    }
                }
            }
            RoutineStmt::DeclareCondition { name, condition } => {
                let key = normalize_local(name);
                if conditions.contains_key(&key) {
                    return Err(error(
                        crate::error::catalog::ER_SP_DUP_COND,
                        format!("Duplicate condition: {name}"),
                    ));
                }
                if matches!(condition, RoutineCondition::Named(_)) {
                    return Err(error(
                        crate::error::catalog::ER_PARSE_ERROR,
                        "DECLARE CONDITION requires SQLSTATE or a MySQL error number",
                    ));
                }
                if !matches!(
                    condition,
                    RoutineCondition::MysqlError(_) | RoutineCondition::SqlState(_)
                ) {
                    return Err(error(
                        crate::error::catalog::ER_PARSE_ERROR,
                        "DECLARE CONDITION requires SQLSTATE or a MySQL error number",
                    ));
                }
                validate_declared_condition(condition)?;
                conditions.insert(key, condition.clone());
            }
            RoutineStmt::DeclareCursor { name, .. } => {
                if !cursors.insert(normalize_local(name)) {
                    return Err(error(
                        crate::error::catalog::ER_SP_DUP_CURS,
                        format!("Duplicate cursor: {name}"),
                    ));
                }
            }
            RoutineStmt::DeclareHandler {
                action,
                conditions: declared,
                statement,
            } => {
                if *action == RoutineHandlerAction::Undo {
                    return Err(error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        "UNDO handlers are not supported",
                    ));
                }
                for condition in declared {
                    let resolved = match condition {
                        RoutineCondition::Named(name) => {
                            conditions.get(&normalize_local(name)).ok_or_else(|| {
                                error(
                                    crate::error::catalog::ER_SP_COND_MISMATCH,
                                    format!("Undefined CONDITION: {name}"),
                                )
                            })?
                        }
                        condition => condition,
                    };
                    validate_declared_condition(resolved)?;
                    let key = routine_condition_key(resolved);
                    if !handler_conditions.insert(key) {
                        return Err(error(
                            crate::error::catalog::ER_SP_DUP_HANDLER,
                            "Duplicate handler declared in the same block",
                        ));
                    }
                }
                validate_routine_stmt_structure(statement, labels, &conditions)?;
            }
            _ => validate_routine_stmt_structure(stmt, labels, &conditions)?,
        }
    }
    Ok(())
}

fn routine_condition_key(condition: &RoutineCondition) -> String {
    match condition {
        RoutineCondition::SqlException => "class:exception".to_string(),
        RoutineCondition::SqlWarning => "class:warning".to_string(),
        RoutineCondition::NotFound => "class:not-found".to_string(),
        RoutineCondition::MysqlError(code) => format!("errno:{code}"),
        RoutineCondition::SqlState(state) => format!("state:{}", state.to_ascii_uppercase()),
        RoutineCondition::Named(name) => format!("name:{}", normalize_local(name)),
    }
}

fn validate_declared_condition(condition: &RoutineCondition) -> Result<(), QueryError> {
    match condition {
        RoutineCondition::SqlState(state) => validate_signal_sql_state(state)?,
        RoutineCondition::MysqlError(0) => {
            return Err(error(
                crate::error::catalog::ER_WRONG_VALUE,
                "Invalid condition number 0",
            ));
        }
        _ => {}
    }
    Ok(())
}

fn validate_signal_sql_state(state: &str) -> Result<(), QueryError> {
    if state.len() != 5
        || !state.bytes().all(|byte| byte.is_ascii_alphanumeric())
        || state.starts_with("00")
    {
        return Err(error(
            crate::error::catalog::ER_SP_BAD_SQLSTATE,
            format!("Bad SQLSTATE: '{state}'"),
        ));
    }
    Ok(())
}

fn validate_routine_stmt_structure(
    stmt: &RoutineStmt,
    labels: &mut Vec<(String, bool)>,
    conditions: &BTreeMap<String, RoutineCondition>,
) -> Result<(), QueryError> {
    match stmt {
        RoutineStmt::Block { label, statements } => {
            with_validated_label(label, false, labels, |labels| {
                validate_routine_structure(statements, labels, conditions)
            })
        }
        RoutineStmt::Loop { label, statements }
        | RoutineStmt::While {
            label, statements, ..
        }
        | RoutineStmt::Repeat {
            label, statements, ..
        } => with_validated_label(label, true, labels, |labels| {
            validate_routine_structure(statements, labels, conditions)
        }),
        RoutineStmt::If {
            branches,
            else_branch,
        }
        | RoutineStmt::Case {
            branches,
            else_branch,
            ..
        } => {
            for (_, branch) in branches {
                validate_routine_structure(branch, labels, conditions)?;
            }
            validate_routine_structure(else_branch, labels, conditions)
        }
        RoutineStmt::Leave(target) => {
            if labels
                .iter()
                .any(|(label, _)| label.eq_ignore_ascii_case(target))
            {
                Ok(())
            } else {
                Err(error(
                    crate::error::catalog::ER_SP_LILABEL_MISMATCH,
                    format!("LEAVE with no matching label: {target}"),
                ))
            }
        }
        RoutineStmt::Signal { condition, .. } => {
            validate_signal_condition_reference(condition, conditions)
        }
        RoutineStmt::Resignal {
            condition: Some(condition),
            ..
        } => validate_signal_condition_reference(condition, conditions),
        RoutineStmt::Iterate(target) => {
            if labels
                .iter()
                .any(|(label, is_loop)| *is_loop && label.eq_ignore_ascii_case(target))
            {
                Ok(())
            } else {
                Err(error(
                    crate::error::catalog::ER_SP_LILABEL_MISMATCH,
                    format!("ITERATE with no matching loop label: {target}"),
                ))
            }
        }
        _ => Ok(()),
    }
}

fn validate_signal_condition_reference(
    condition: &RoutineCondition,
    conditions: &BTreeMap<String, RoutineCondition>,
) -> Result<(), QueryError> {
    let resolved = match condition {
        RoutineCondition::Named(name) => {
            conditions.get(&normalize_local(name)).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_SP_COND_MISMATCH,
                    format!("Undefined CONDITION: {name}"),
                )
            })?
        }
        condition => condition,
    };
    match resolved {
        RoutineCondition::SqlState(state) => validate_signal_sql_state(state),
        _ => Err(error(
            crate::error::catalog::ER_SIGNAL_BAD_CONDITION_TYPE,
            "SIGNAL/RESIGNAL can only use a condition defined with SQLSTATE",
        )),
    }
}

fn with_validated_label(
    label: &Option<String>,
    is_loop: bool,
    labels: &mut Vec<(String, bool)>,
    f: impl FnOnce(&mut Vec<(String, bool)>) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    let Some(label) = label else {
        return f(labels);
    };
    if labels
        .iter()
        .any(|(active, _)| active.eq_ignore_ascii_case(label))
    {
        return Err(error(
            crate::error::catalog::ER_SP_LABEL_REDEFINE,
            format!("Redefining label {label}"),
        ));
    }
    labels.push((label.clone(), is_loop));
    let result = f(labels);
    labels.pop();
    result
}

fn validate_binlog_function_creator(
    db: &Database,
    session: &SessionContext,
    def: &RoutineDef,
) -> Result<(), QueryError> {
    if def.kind != RoutineKind::Function
        || !crate::binlog::enabled()
        || !session.sql_log_bin
        || crate::engine::sysvar::builtin::replication::log_bin_trust_function_creators()
        || session.internal_system_session
    {
        return Ok(());
    }
    let super_required = RequiredPrivilege {
        privilege: Privilege::Super,
        scope: PrivilegeScope::Global,
    };
    let has_super = db.with_auth_catalog(|auth| {
        auth.account_has_privilege(&session.authorization_context(), &super_required, false)
    })?;
    if has_super {
        return Ok(());
    }
    if !def.characteristics.deterministic
        || matches!(
            def.characteristics.sql_data_access,
            RoutineSqlDataAccess::ContainsSql | RoutineSqlDataAccess::ModifiesSqlData
        )
    {
        return Err(error(crate::error::catalog::ER_BINLOG_UNSAFE_ROUTINE,
            "This function has none of DETERMINISTIC, NO SQL, or READS SQL DATA in its declaration and binary logging is enabled",
        ));
    }
    Err(error(
        crate::error::catalog::ER_BINLOG_CREATE_ROUTINE_NEED_SUPER,
        "You do not have the SUPER privilege and binary logging is enabled",
    ))
}

/// Setting `DEFINER` to an account other than the current user requires the
/// `SET_USER_ID` dynamic privilege (which `SUPER` implies), matching MySQL 8.0.
/// The default (no explicit DEFINER → current user) is always permitted.
fn validate_routine_definer_privilege(
    db: &Database,
    session: &SessionContext,
    def: &RoutineDef,
) -> Result<(), QueryError> {
    validate_definer_privilege(db, session, def.definer.as_ref())
}

/// Setting `DEFINER` to an account other than the current user requires the
/// `SET_USER_ID` dynamic privilege (which `SUPER` implies), matching MySQL 8.0.
/// The default (no explicit DEFINER → current user) is always permitted.
/// Shared by CREATE PROCEDURE/FUNCTION and CREATE TRIGGER.
pub(super) fn validate_definer_privilege(
    db: &Database,
    session: &SessionContext,
    definer: Option<&crate::parser::AccountIdent>,
) -> Result<(), QueryError> {
    let Some(definer) = definer else {
        return Ok(());
    };
    let current = &session.authenticated_as;
    if definer.user == current.user && definer.host.eq_ignore_ascii_case(&current.host) {
        return Ok(());
    }
    let auth = db.auth_catalog();
    let ctx = session.authorization_context();
    let required = crate::engine::auth::RequiredPrivilege {
        privilege: crate::engine::auth::Privilege::Dynamic("SET_USER_ID".to_string()),
        scope: crate::engine::auth::PrivilegeScope::Global,
    };
    let system_user = crate::engine::auth::RequiredPrivilege {
        privilege: crate::engine::auth::Privilege::Dynamic("SYSTEM_USER".into()),
        scope: crate::engine::auth::PrivilegeScope::Global,
    };
    if auth.account_is_system_user(&AccountKey::new(&definer.user, &definer.host))
        && !auth.account_has_privilege(&ctx, &system_user, false)
    {
        return Err(error(
            crate::error::catalog::ER_SPECIFIC_ACCESS_DENIED_ERROR,
            "Access denied; you need (at least one of) the SYSTEM_USER privilege(s) for this operation",
        ));
    }
    if auth.account_has_privilege(&ctx, &required, false) {
        Ok(())
    } else {
        Err(error(crate::error::catalog::ER_SPECIFIC_ACCESS_DENIED_ERROR,
            "Access denied; you need (at least one of) the SUPER or SET_USER_ID privilege(s) for this operation",
        ))
    }
}

pub(super) fn warn_missing_definer(db: &Database, key: &AccountKey) -> Result<(), QueryError> {
    if !db.with_auth_catalog(|auth| auth.accounts.contains_key(&account_id(key)))? {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Note,
            crate::error::catalog::ER_NO_SUCH_USER,
            format!(
                "The user specified as a definer ({}) does not exist",
                key.display()
            ),
        );
    }
    Ok(())
}
fn validate_routine_definer(db: &Database, routine: &RoutineSchema) -> Result<(), QueryError> {
    warn_missing_definer(
        db,
        &AccountKey::new(&routine.definer_user, &routine.definer_host),
    )
}

fn routine_stmt_has_return(stmt: &RoutineStmt) -> bool {
    match stmt {
        RoutineStmt::Return(_) => true,
        RoutineStmt::Block { statements, .. }
        | RoutineStmt::Loop { statements, .. }
        | RoutineStmt::While { statements, .. }
        | RoutineStmt::Repeat { statements, .. } => statements.iter().any(routine_stmt_has_return),
        RoutineStmt::If {
            branches,
            else_branch,
        } => {
            branches
                .iter()
                .any(|(_, statements)| statements.iter().any(routine_stmt_has_return))
                || else_branch.iter().any(routine_stmt_has_return)
        }
        RoutineStmt::Case {
            branches,
            else_branch,
            ..
        } => {
            branches
                .iter()
                .any(|(_, statements)| statements.iter().any(routine_stmt_has_return))
                || else_branch.iter().any(routine_stmt_has_return)
        }
        _ => false,
    }
}

fn validate_function_stmt(stmt: &RoutineStmt) -> Result<(), QueryError> {
    match stmt {
        RoutineStmt::Sql(sql) if sql.trim_start().to_ascii_uppercase().starts_with("SELECT") => {
            Err(error(
                crate::error::catalog::ER_SP_NO_RETSET,
                "Not allowed to return a result set from a function",
            ))
        }
        RoutineStmt::Sql(sql) => {
            let upper = sql.trim_start().to_ascii_uppercase();
            if upper.starts_with("START TRANSACTION")
                || upper.starts_with("BEGIN")
                || upper.starts_with("COMMIT")
                || upper.starts_with("ROLLBACK")
                || upper.starts_with("CREATE ")
                || upper.starts_with("ALTER ")
                || upper.starts_with("DROP ")
                || upper.starts_with("TRUNCATE ")
            {
                return Err(error(
                    crate::error::catalog::ER_COMMIT_NOT_ALLOWED_IN_SF_OR_TRG,
                    "Explicit or implicit commit is not allowed in stored function",
                ));
            }
            Ok(())
        }
        RoutineStmt::Prepare { .. }
        | RoutineStmt::Execute { .. }
        | RoutineStmt::Deallocate { .. } => Err(error(
            crate::error::catalog::ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG,
            "Dynamic SQL is not allowed in stored function",
        )),
        RoutineStmt::Block { statements, .. }
        | RoutineStmt::Loop { statements, .. }
        | RoutineStmt::While { statements, .. }
        | RoutineStmt::Repeat { statements, .. } => {
            for stmt in statements {
                validate_function_stmt(stmt)?;
            }
            Ok(())
        }
        RoutineStmt::If {
            branches,
            else_branch,
        } => {
            for (_, stmts) in branches {
                for stmt in stmts {
                    validate_function_stmt(stmt)?;
                }
            }
            for stmt in else_branch {
                validate_function_stmt(stmt)?;
            }
            Ok(())
        }
        RoutineStmt::Case {
            branches,
            else_branch,
            ..
        } => {
            for (_, stmts) in branches {
                for stmt in stmts {
                    validate_function_stmt(stmt)?;
                }
            }
            for stmt in else_branch {
                validate_function_stmt(stmt)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Events share stored-program control flow, but have no CALL parameters or
/// EXECUTE privilege requirement. Their caller already established the definer.
pub(super) fn validate_event_body(sql: &str) -> Result<(), QueryError> {
    let body = parse_routine_body_sql(sql).map_err(|err| err.spec.error(format!("{err}")))?;
    validate_routine_structure(&body.statements, &mut Vec::new(), &BTreeMap::new())?;
    if body.statements.iter().any(routine_stmt_has_return) {
        return Err(error(
            crate::error::catalog::ER_SP_BADRETURN,
            "RETURN is only allowed in a FUNCTION",
        ));
    }
    compile_static_statements(&body)?;
    Ok(())
}

pub(super) fn execute_event_body(
    db: &Arc<Database>,
    session: &mut SessionContext,
    event: &crate::storage::EventSchema,
    registry: &SessionRegistry,
) -> Result<(), QueryError> {
    let routine = RoutineSchema {
        database: event.database.clone(),
        name: event.name.clone(),
        kind: RoutineKindSchema::Procedure,
        definer_user: event.definer_user.clone(),
        definer_host: event.definer_host.clone(),
        security_type: RoutineSecuritySchema::Invoker,
        params: Vec::new(),
        return_type: None,
        return_type_sql: None,
        return_unsigned: false,
        deterministic: false,
        sql_data_access: RoutineSqlDataAccessSchema::ContainsSql,
        comment: event.comment.clone(),
        sql_mode: event.sql_mode.clone(),
        body_sql: event.body_sql.clone(),
        schema_version: event.schema_version,
        created_ms: event.created_ms,
        updated_ms: event.last_altered_ms,
        character_set_client: event.character_set_client.clone(),
        collation_connection: event.collation_connection.clone(),
        database_collation: event.database_collation.clone(),
    };
    // Separate namespace prevents a same-named procedure's parameter bindings
    // from being reused for an event with identical SQL.
    let program = compile_routine_in_namespace(&routine, "event")?;
    let result = execute_routine_inner(
        db,
        session,
        &routine,
        Vec::new(),
        registry,
        None,
        None,
        Some(&program),
    );
    // Like CALL, a body with only local expressions still owns a read view.
    let result = super::tx::finish_statement_txn(db, session, result);
    // An event has no caller to inherit an unfinished explicit transaction.
    let cleanup = if session.current_txn.is_some() || session.explicit_txn {
        super::tx::rollback_session_txn(db, session)
    } else {
        Ok(())
    };
    let result = result?;
    cleanup?;
    for response in result.responses {
        response.materialize()?;
    }
    Ok(())
}

pub(crate) fn compile_routine(routine: &RoutineSchema) -> Result<Arc<RoutineProgram>, QueryError> {
    compile_routine_in_namespace(routine, routine.kind.lower())
}

fn compile_routine_in_namespace(
    routine: &RoutineSchema,
    namespace: &str,
) -> Result<Arc<RoutineProgram>, QueryError> {
    let key = (
        routine.database.clone(),
        namespace.to_string(),
        routine.name.clone(),
        routine.schema_version,
        stable_sql_hash(&routine.body_sql),
        routine.sql_mode.clone(),
    );
    let cache = routine_cache();
    if let Some(program) = cache
        .lock()
        .ok()
        .and_then(|mut cache| cache.get(&key).cloned())
    {
        return Ok(program);
    }
    let _compile = ROUTINE_COMPILE_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "routine compile lock poisoned",
            )
        })?;
    if let Some(program) = cache
        .lock()
        .ok()
        .and_then(|mut cache| cache.get(&key).cloned())
    {
        return Ok(program);
    }
    let memory_charge = reserve_compiled_program_memory(
        routine.body_sql.capacity(),
        routine.database.capacity()
            + routine.name.capacity()
            + routine.sql_mode.capacity()
            + std::mem::size_of::<RoutineProgram>(),
    )?;
    let mode_bits = crate::engine::api::parse_temporal_sql_mode_bits(&routine.sql_mode)
        | crate::engine::api::TEMPORAL_MODE_EXPLICIT;
    let _mode = crate::parser::ParserSqlModeGuard::activate(mode_bits);
    let body = parse_routine_body_sql(&routine.body_sql).map_err(|err| {
        error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            format!("invalid stored routine body: {}", err.message),
        )
    })?;
    validate_routine_structure(&body.statements, &mut Vec::new(), &BTreeMap::new())?;
    if routine.kind == RoutineKindSchema::Function {
        for stmt in &body.statements {
            validate_function_stmt(stmt)?;
        }
    } else if body.statements.iter().any(routine_stmt_has_return) {
        return Err(error(
            crate::error::catalog::ER_SP_BADRETURN,
            "RETURN is only allowed in a FUNCTION",
        ));
    }
    let static_statements = Arc::new(compile_static_statements(&body)?);
    let initial_scope = routine
        .params
        .iter()
        .map(|param| normalize_local(&param.name))
        .collect::<BTreeSet<_>>();
    let parameterized_statements = Arc::new(compile_parameterized_static_statements(
        &body,
        &initial_scope,
        &static_statements,
    ));
    let program = Arc::new(RoutineProgram {
        body,
        static_statements,
        parameterized_statements,
        _memory_charge: memory_charge,
    });
    if let Ok(mut cache) = cache.lock() {
        cache.put(key, Arc::clone(&program));
    }
    Ok(program)
}

fn compile_trigger(trigger: &TriggerSchema) -> Result<Arc<RoutineProgram>, QueryError> {
    let key = (
        trigger.database.clone(),
        trigger.name.clone(),
        trigger.schema_version,
        stable_sql_hash(&trigger.body_sql),
        trigger.sql_mode.clone(),
    );
    let cache = trigger_cache();
    if let Some(program) = cache
        .lock()
        .ok()
        .and_then(|mut cache| cache.get(&key).cloned())
    {
        return Ok(program);
    }
    let _compile = TRIGGER_COMPILE_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "trigger compile lock poisoned",
            )
        })?;
    if let Some(program) = cache
        .lock()
        .ok()
        .and_then(|mut cache| cache.get(&key).cloned())
    {
        return Ok(program);
    }
    let memory_charge = reserve_compiled_program_memory(
        trigger.body_sql.capacity(),
        trigger.database.capacity()
            + trigger.name.capacity()
            + trigger.sql_mode.capacity()
            + std::mem::size_of::<RoutineProgram>(),
    )?;
    let mode_bits = crate::engine::api::parse_temporal_sql_mode_bits(&trigger.sql_mode)
        | crate::engine::api::TEMPORAL_MODE_EXPLICIT;
    let _mode = crate::parser::ParserSqlModeGuard::activate(mode_bits);
    let body = parse_routine_body_sql(&trigger.body_sql).map_err(|err| {
        error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            format!("invalid trigger body: {}", err.message),
        )
    })?;
    validate_routine_structure(&body.statements, &mut Vec::new(), &BTreeMap::new())?;
    let static_statements = Arc::new(compile_static_statements(&body)?);
    let program = Arc::new(RoutineProgram {
        body,
        static_statements,
        parameterized_statements: Arc::new(HashMap::new()),
        _memory_charge: memory_charge,
    });
    if let Ok(mut cache) = cache.lock() {
        cache.put(key, Arc::clone(&program));
    }
    Ok(program)
}

fn stable_sql_hash(sql: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    sql.hash(&mut hasher);
    hasher.finish()
}

fn compile_static_statements(
    body: &RoutineBody,
) -> Result<HashMap<String, CompiledStaticStatement>, QueryError> {
    let mut statements = HashMap::new();
    collect_static_statements(&body.statements, &mut statements)?;
    Ok(statements)
}

fn collect_static_statements(
    body: &[RoutineStmt],
    compiled: &mut HashMap<String, CompiledStaticStatement>,
) -> Result<(), QueryError> {
    for stmt in body {
        match stmt {
            RoutineStmt::Sql(sql)
            | RoutineStmt::SelectInto {
                select_sql: sql, ..
            }
            | RoutineStmt::DeclareCursor {
                select_sql: sql, ..
            } => {
                if compiled.contains_key(sql) {
                    continue;
                }
                let mut statement =
                    parse_statement(sql).map_err(|err| err.spec.error(err.message))?;
                if let crate::parser::Statement::Unsupported { kind, .. } = &statement {
                    return Err(error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        format!("unsupported statement in stored routine: {kind}"),
                    ));
                }
                if matches!(statement, crate::parser::Statement::MultiStatement { .. }) {
                    return Err(error(
                        crate::error::catalog::ER_PARSE_ERROR,
                        "stored routine statement must contain exactly one SQL statement",
                    ));
                }
                let mut next_placeholder = 0;
                crate::parser::number_statement_placeholders(&mut statement, &mut next_placeholder);
                if next_placeholder != 0 {
                    return Err(error(
                        crate::error::catalog::ER_PARSE_ERROR,
                        "parameter markers are only allowed in dynamically prepared routine SQL",
                    ));
                }
                let identifiers =
                    crate::parser::collect_unqualified_identifier_names(&statement).into();
                compiled.insert(
                    sql.clone(),
                    CompiledStaticStatement {
                        statement: Arc::new(statement),
                        identifiers,
                    },
                );
            }
            RoutineStmt::DeclareHandler { statement, .. } => {
                collect_static_statements(std::slice::from_ref(statement.as_ref()), compiled)?;
            }
            RoutineStmt::Block { statements, .. }
            | RoutineStmt::Loop { statements, .. }
            | RoutineStmt::While { statements, .. }
            | RoutineStmt::Repeat { statements, .. } => {
                collect_static_statements(statements, compiled)?;
            }
            RoutineStmt::If {
                branches,
                else_branch,
            }
            | RoutineStmt::Case {
                branches,
                else_branch,
                ..
            } => {
                for (_, branch) in branches {
                    collect_static_statements(branch, compiled)?;
                }
                collect_static_statements(else_branch, compiled)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn compile_parameterized_static_statements(
    body: &RoutineBody,
    initial_scope: &BTreeSet<String>,
    compiled: &HashMap<String, CompiledStaticStatement>,
) -> HashMap<String, HashMap<Vec<String>, ParameterizedStaticStatement>> {
    fn register(
        sql: &str,
        scope: &BTreeSet<String>,
        compiled: &HashMap<String, CompiledStaticStatement>,
        out: &mut HashMap<String, HashMap<Vec<String>, ParameterizedStaticStatement>>,
    ) {
        let Some(base) = compiled.get(sql) else {
            return;
        };
        let slot_names = base
            .identifiers
            .iter()
            .filter(|name| scope.contains(name.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if slot_names.is_empty() {
            return;
        }
        let slots = slot_names
            .iter()
            .cloned()
            .enumerate()
            .map(|(slot, name)| (name, slot))
            .collect::<BTreeMap<_, _>>();
        let mut statement = (*base.statement).clone();
        crate::parser::bind_unqualified_identifiers_to_placeholders(&mut statement, &slots);
        out.entry(sql.to_string()).or_default().insert(
            slot_names.clone(),
            ParameterizedStaticStatement {
                statement: Arc::new(statement),
                slot_names: slot_names.into(),
            },
        );
    }

    fn walk(
        statements: &[RoutineStmt],
        inherited_scope: &BTreeSet<String>,
        compiled: &HashMap<String, CompiledStaticStatement>,
        out: &mut HashMap<String, HashMap<Vec<String>, ParameterizedStaticStatement>>,
    ) {
        let mut scope = inherited_scope.clone();
        for statement in statements {
            match statement {
                RoutineStmt::DeclareVar { names, .. } => {
                    scope.extend(names.iter().map(|name| normalize_local(name)));
                }
                RoutineStmt::DeclareCursor { select_sql, .. }
                | RoutineStmt::SelectInto { select_sql, .. } => {
                    register(select_sql, &scope, compiled, out);
                }
                RoutineStmt::Sql(sql) => register(sql, &scope, compiled, out),
                RoutineStmt::DeclareHandler { statement, .. } => {
                    walk(
                        std::slice::from_ref(statement.as_ref()),
                        &scope,
                        compiled,
                        out,
                    );
                }
                RoutineStmt::Block { statements, .. }
                | RoutineStmt::Loop { statements, .. }
                | RoutineStmt::While { statements, .. }
                | RoutineStmt::Repeat { statements, .. } => {
                    walk(statements, &scope, compiled, out);
                }
                RoutineStmt::If {
                    branches,
                    else_branch,
                }
                | RoutineStmt::Case {
                    branches,
                    else_branch,
                    ..
                } => {
                    for (_, branch) in branches {
                        walk(branch, &scope, compiled, out);
                    }
                    walk(else_branch, &scope, compiled, out);
                }
                _ => {}
            }
        }
    }

    let mut out = HashMap::new();
    walk(&body.statements, initial_scope, compiled, &mut out);
    out
}

fn reserve_compiled_program_memory(
    source_capacity: usize,
    fixed_capacity: usize,
) -> Result<crate::engine::executor::mem_budget::ControlledMemoryCharge, QueryError> {
    // Parsed routine bodies retain the source once in RoutineBody, once in
    // statement fragments, and again in parsed static AST strings. Charge a
    // deliberately conservative multiple of the actual source allocation
    // capacity, plus fixed/key capacities, before parsing so failure cannot
    // temporarily cross MYDB_MEMORY_TARGET.
    let retained_capacity = source_capacity
        .saturating_mul(16)
        .saturating_add(fixed_capacity)
        .saturating_add(4096);
    crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::DerivedCache,
        retained_capacity,
    )
}

fn invalidate_routine_cache(database: &str, kind: RoutineKind, name: &str) {
    if let Some(cache) = ROUTINE_CACHE.get() {
        if let Ok(mut cache) = cache.lock() {
            let keys = cache
                .iter()
                .filter_map(|(key, _)| {
                    (key.0.eq_ignore_ascii_case(database)
                        && key.1.eq_ignore_ascii_case(kind.lower())
                        && key.2.eq_ignore_ascii_case(name))
                    .then(|| key.clone())
                })
                .collect::<Vec<_>>();
            for key in keys {
                cache.pop(&key);
            }
        }
    }
}

pub(super) fn invalidate_trigger_cache() {
    if let Some(cache) = TRIGGER_CACHE.get() {
        if let Ok(mut cache) = cache.lock() {
            cache.clear();
        }
    }
}

#[derive(Debug)]
struct RoutineResult {
    responses: Vec<QueryResponse>,
    return_value: Option<Value>,
    prepared_out_params: Vec<(String, crate::types::ColumnType, Value)>,
}

#[derive(Debug, Default)]
struct RoutineFrame {
    locals: HashMap<String, Value>,
    local_types: HashMap<String, (DataTypeKind, bool)>,
    conditions: HashMap<String, RoutineCondition>,
    cursors: HashMap<String, CursorState>,
    static_statements: Arc<HashMap<String, CompiledStaticStatement>>,
    parameterized_statements:
        Arc<HashMap<String, HashMap<Vec<String>, ParameterizedStaticStatement>>>,
    responses: Vec<QueryResponse>,
    active_handler_error: Option<RoutineExecError>,
    /// Dynamic block depth. Unhandled-condition default actions are applied
    /// only after a condition has had a chance to reach every enclosing
    /// handler scope.
    block_depth: usize,
    /// Number of currently enclosing lexical blocks that declared at least
    /// one condition handler. Result-producing SQL may remain streaming only
    /// when this is zero; otherwise row errors must be observed before the
    /// block exits so its handler can run.
    active_handler_scopes: usize,
    outer_handlers: Vec<Vec<HandlerDecl>>,
    /// Rows affected by the most recent body statement, for
    /// `GET DIAGNOSTICS ... = ROW_COUNT`.
    diag_row_count: i64,
    /// Condition areas produced by the most recently executed routine
    /// statement. Unlike the historical single-error shortcut this retains
    /// every warning so NUMBER and CONDITION n have MySQL's 1-based meaning.
    diag_conditions: Vec<crate::engine::api::SqlWarning>,
    allow_result_sets: bool,
    allow_return: bool,
    allow_dynamic_sql: bool,
    forbid_transaction_control: bool,
    trigger_new_keys: BTreeSet<String>,
    // Expose 0 before allocation, but keep NULL as the storage allocation
    // request until the body explicitly assigns this column.
    trigger_pending_auto_increment: Option<String>,
    /// Normalized `new.<col>` keys that are generated columns (B4): readable
    /// in the body but assignment is rejected with 3105.
    trigger_generated_new_keys: BTreeSet<String>,
    trigger_old_keys: BTreeSet<String>,
    trigger_new_writable: bool,
    trigger_table: Option<String>,
}

#[derive(Debug, Clone)]
struct RoutineCapabilities {
    allow_result_sets: bool,
    allow_dynamic_sql: bool,
    forbid_transaction_control: bool,
    trigger_table: Option<String>,
}

impl RoutineCapabilities {
    fn from_frame(frame: &RoutineFrame) -> Self {
        Self {
            allow_result_sets: frame.allow_result_sets,
            allow_dynamic_sql: frame.allow_dynamic_sql,
            forbid_transaction_control: frame.forbid_transaction_control,
            trigger_table: frame.trigger_table.clone(),
        }
    }
}

#[derive(Debug)]
struct CursorState {
    select_sql: String,
    cursor: Option<Box<dyn crate::engine::api::ResultCursor>>,
}

#[derive(Debug)]
struct RoutineInvocationSnapshot {
    current_database: String,
    sql_mode: String,
    temporal_sql_mode_bits: u16,
    authenticated_as: AccountKey,
    active_roles: crate::engine::auth::AuthCow<BTreeSet<AccountKey>>,
    last_insert_id: u64,
    last_row_count: i64,
    statement_insert_id: crate::engine::api::StatementInsertIdSnapshot,
}

impl RoutineInvocationSnapshot {
    fn capture(session: &SessionContext) -> Self {
        Self {
            current_database: session.current_database.clone(),
            sql_mode: session.sql_mode.clone(),
            temporal_sql_mode_bits: session.temporal_sql_mode_bits,
            authenticated_as: session.authenticated_as.clone(),
            active_roles: session.active_roles.clone(),
            last_row_count: session.last_row_count,
            statement_insert_id: crate::engine::api::snapshot_statement_insert_id(),
            last_insert_id: session
                .last_insert_id
                .load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    fn restore(self, session: &mut SessionContext, restore_last_insert_id: bool) {
        session.current_database = self.current_database;
        session.sql_mode = self.sql_mode;
        session.temporal_sql_mode_bits = self.temporal_sql_mode_bits;
        session.authenticated_as = self.authenticated_as;
        session.active_roles = self.active_roles;
        if restore_last_insert_id {
            session.last_row_count = self.last_row_count;
            session
                .last_insert_id
                .store(self.last_insert_id, std::sync::atomic::Ordering::Relaxed);
            crate::engine::api::restore_statement_insert_id(self.statement_insert_id);
        }
    }
}

#[derive(Debug, Clone)]
struct HandlerDecl {
    action: RoutineHandlerAction,
    conditions: Vec<RoutineCondition>,
    statement: RoutineStmt,
}

#[derive(Debug)]
enum RoutineControl {
    None,
    Return(Value),
    Leave(String),
    Iterate(String),
}

#[derive(Debug, Clone)]
enum RoutineExecError {
    Query(QueryError),
    Condition(RoutineCondition, QueryError),
    Warning(RoutineCondition, QueryError),
}

impl From<QueryError> for RoutineExecError {
    fn from(value: QueryError) -> Self {
        RoutineExecError::Query(value)
    }
}

impl RoutineExecError {
    fn query_error(&self) -> QueryError {
        match self {
            RoutineExecError::Query(err)
            | RoutineExecError::Condition(_, err)
            | RoutineExecError::Warning(_, err) => err.clone(),
        }
    }

    fn condition(&self) -> RoutineCondition {
        match self {
            RoutineExecError::Condition(condition, _) | RoutineExecError::Warning(condition, _) => {
                condition.clone()
            }
            RoutineExecError::Query(err) => RoutineCondition::SqlState(err.sql_state().to_string()),
        }
    }
}

fn execute_procedure_call(
    db: &Arc<Database>,
    session: &mut SessionContext,
    call: RoutineCall,
    registry: &SessionRegistry,
    mut caller_locals: Option<&mut HashMap<String, Value>>,
    inherited_capabilities: Option<RoutineCapabilities>,
) -> Result<RoutineResult, QueryError> {
    let routine = db
        .routine_definition(
            &session.current_database,
            RoutineKindSchema::Procedure,
            &call.name,
        )
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_SP_DOES_NOT_EXIST,
                format!("PROCEDURE {} does not exist", call.name),
            )
        })?;
    authorize_routine_execute(db, session, &routine)?;
    execute_routine(
        db,
        session,
        &routine,
        call.args,
        registry,
        caller_locals.as_deref_mut(),
        inherited_capabilities,
        None,
    )
}

fn authorize_routine_execute(
    db: &Database,
    session: &SessionContext,
    routine: &RoutineSchema,
) -> Result<(), QueryError> {
    if session.internal_system_session {
        return Ok(());
    }
    let required = RequiredPrivilege {
        privilege: Privilege::Execute,
        scope: PrivilegeScope::Routine {
            database: routine.database.clone(),
            routine: routine.name.clone(),
            kind: routine.kind.lower().to_string(),
        },
    };
    let allowed = db.with_auth_catalog(|auth| {
        auth.account_has_privilege(&session.authorization_context(), &required, false)
    })?;
    if allowed {
        Ok(())
    } else {
        Err(error(
            crate::error::catalog::ER_PROCACCESS_DENIED_ERROR,
            format!(
                "execute command denied to user '{}'@'{}' for routine '{}.{}'",
                session.user, session.host, routine.database, routine.name
            ),
        ))
    }
}

fn execute_function_value(
    db: &Arc<Database>,
    session: &mut SessionContext,
    name: &str,
    args: Vec<Value>,
    registry: &SessionRegistry,
) -> Result<Value, QueryError> {
    let routine = db
        .routine_definition(&session.current_database, RoutineKindSchema::Function, name)
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_SP_DOES_NOT_EXIST,
                format!("FUNCTION {name} does not exist"),
            )
        })?;
    // Calls originating in a routine expression do not pass through the
    // outer statement resolver. Check against the *current* execution
    // identity here so INVOKER/DEFINER transitions and nested calls cannot
    // bypass routine-level EXECUTE.
    authorize_routine_execute(db, session, &routine)?;
    execute_function_schema(db, session, &routine, args, registry)
}

fn execute_function_schema(
    db: &Arc<Database>,
    session: &mut SessionContext,
    routine: &RoutineSchema,
    args: Vec<Value>,
    registry: &SessionRegistry,
) -> Result<Value, QueryError> {
    execute_function_schema_with_program(db, session, routine, args, registry, None)
}

fn execute_function_schema_with_program(
    db: &Arc<Database>,
    session: &mut SessionContext,
    routine: &RoutineSchema,
    args: Vec<Value>,
    registry: &SessionRegistry,
    program: Option<&Arc<RoutineProgram>>,
) -> Result<Value, QueryError> {
    // Procedure expressions (IF/SET/RETURN arguments) can invoke a function
    // without an owning SQL SELECT. Give that call the same atomic boundary;
    // resolved row-by-row calls already inside an owning statement reuse it.
    if session.routine_atomic_depth == 0 {
        return super::tx::execute_atomic_routine_statement(db, session, |db, session| {
            execute_function_schema_with_program(db, session, routine, args, registry, program)
        });
    }
    let warning_start = crate::engine::api::statement_warning_count();
    let result = execute_routine(
        db,
        session,
        routine,
        args.into_iter().map(Expr::BoundValue).collect(),
        registry,
        None,
        None,
        program,
    )?;
    // MySQL consumes a non-strict NOT NULL coercion raised by DML inside a
    // successfully completed stored function. The coerced value remains
    // observable, but ER_BAD_NULL_ERROR is not copied into the diagnostics
    // area of the outer SELECT. Other routine warnings still propagate.
    let mut all_warnings = crate::engine::api::take_statement_warnings();
    let mut suffix = all_warnings.split_off(warning_start.min(all_warnings.len()));
    suffix.retain(|warning| !warning.is(crate::error::catalog::ER_BAD_NULL_ERROR));
    all_warnings.append(&mut suffix);
    for warning in all_warnings {
        crate::engine::api::push_sql_warning(warning);
    }
    if !result.responses.is_empty() {
        return Err(error(
            crate::error::catalog::ER_SP_NO_RETSET,
            "Not allowed to return a result set from a function",
        ));
    }
    result.return_value.ok_or_else(|| {
        error(
            crate::error::catalog::ER_SP_NORETURN,
            format!("FUNCTION {} ended without RETURN", routine.name),
        )
    })
}

fn execute_routine(
    db: &Arc<Database>,
    session: &mut SessionContext,
    routine: &RoutineSchema,
    args: Vec<Expr>,
    registry: &SessionRegistry,
    caller_locals: Option<&mut HashMap<String, Value>>,
    inherited_capabilities: Option<RoutineCapabilities>,
    program: Option<&Arc<RoutineProgram>>,
) -> Result<RoutineResult, QueryError> {
    crate::util::stack::with_stack(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::Executor,
        || {
            execute_routine_checked(
                db,
                session,
                routine,
                args,
                registry,
                caller_locals,
                inherited_capabilities,
                program,
            )
        },
    )?
}

fn execute_routine_checked(
    db: &Arc<Database>,
    session: &mut SessionContext,
    routine: &RoutineSchema,
    args: Vec<Expr>,
    registry: &SessionRegistry,
    mut caller_locals: Option<&mut HashMap<String, Value>>,
    inherited_capabilities: Option<RoutineCapabilities>,
    program: Option<&Arc<RoutineProgram>>,
) -> Result<RoutineResult, QueryError> {
    let _hint_guard = crate::engine::cache::suspend_query_hint();
    let _partition_scope = super::partition_router::suspend_pruned_partitions();
    // Process-level hard cap guarding against stack overflow. Keep its default
    // aligned with the public MySQL range so an undocumented lower ceiling
    // cannot reject a value accepted by `max_sp_recursion_depth`.
    let hard_max = std::env::var("MYDB_ROUTINE_MAX_DEPTH")
        .ok()
        .and_then(|raw| raw.parse::<u32>().ok())
        .unwrap_or(255)
        .min(255);
    let is_function = routine.kind == RoutineKindSchema::Function;
    let name_key = normalize_local(&routine.name);
    let same_active = ROUTINE_STACK.with(|stack| {
        stack
            .borrow()
            .iter()
            .filter(|(f, d, n)| *f == is_function && d == &routine.database && n == &name_key)
            .count()
    });
    if is_function {
        // MySQL forbids stored-function recursion outright.
        if same_active > 0 {
            return Err(error(
                crate::error::catalog::ER_SP_NO_RECURSION,
                "Recursive stored functions and triggers are not allowed.",
            ));
        }
    } else {
        let max_sp = session.max_sp_recursion_depth as usize;
        if same_active > max_sp {
            return Err(error(crate::error::catalog::ER_SP_RECURSION_LIMIT,
                format!(
                    "Recursive limit {} (as set by the max_sp_recursion_depth variable) was exceeded for routine {}",
                    max_sp, routine.name
                ),
            ));
        }
    }
    ROUTINE_DEPTH.with(|depth| {
        let current = depth.get();
        // The public limit counts recursive calls, excluding the outer call.
        if current > hard_max {
            return Err(error(
                crate::error::catalog::ER_SP_RECURSION_LIMIT,
                format!("recursive stored routine limit {hard_max} was exceeded"),
            ));
        }
        depth.set(current + 1);
        ROUTINE_STACK.with(|stack| {
            stack
                .borrow_mut()
                .push((is_function, routine.database.clone(), name_key.clone()))
        });
        let result = execute_routine_inner(
            db,
            session,
            routine,
            args,
            registry,
            caller_locals.as_deref_mut(),
            inherited_capabilities,
            program,
        );
        ROUTINE_STACK.with(|stack| {
            stack.borrow_mut().pop();
        });
        depth.set(current);
        result
    })
}

fn execute_routine_inner(
    db: &Arc<Database>,
    session: &mut SessionContext,
    routine: &RoutineSchema,
    args: Vec<Expr>,
    registry: &SessionRegistry,
    mut caller_locals: Option<&mut HashMap<String, Value>>,
    inherited_capabilities: Option<RoutineCapabilities>,
    program: Option<&Arc<RoutineProgram>>,
) -> Result<RoutineResult, QueryError> {
    if args.len() != routine.params.len() {
        return Err(error(
            crate::error::catalog::ER_SP_WRONG_NO_OF_ARGS,
            format!(
                "Incorrect number of arguments for {}; expected {}, got {}",
                routine.name,
                routine.params.len(),
                args.len()
            ),
        ));
    }
    let program = match program {
        Some(program) => Arc::clone(program),
        None => compile_routine(routine)?,
    };
    let is_function = routine.kind == RoutineKindSchema::Function;
    let capabilities = inherited_capabilities.unwrap_or(RoutineCapabilities {
        allow_result_sets: !is_function,
        allow_dynamic_sql: !is_function,
        forbid_transaction_control: is_function,
        trigger_table: None,
    });
    let mut frame = RoutineFrame {
        locals: HashMap::new(),
        local_types: HashMap::new(),
        conditions: HashMap::new(),
        cursors: HashMap::new(),
        static_statements: Arc::clone(&program.static_statements),
        parameterized_statements: Arc::clone(&program.parameterized_statements),
        responses: Vec::new(),
        active_handler_error: None,
        block_depth: 0,
        active_handler_scopes: 0,
        outer_handlers: Vec::new(),
        diag_row_count: 0,
        diag_conditions: Vec::new(),
        allow_result_sets: capabilities.allow_result_sets && !is_function,
        allow_return: is_function,
        allow_dynamic_sql: capabilities.allow_dynamic_sql && !is_function,
        forbid_transaction_control: capabilities.forbid_transaction_control || is_function,
        trigger_new_keys: BTreeSet::new(),
        trigger_pending_auto_increment: None,
        trigger_generated_new_keys: BTreeSet::new(),
        trigger_old_keys: BTreeSet::new(),
        trigger_new_writable: false,
        trigger_table: capabilities.trigger_table,
    };
    // Evaluate argument expressions and resolve OUT l-values in the caller's
    // scope. MySQL also coerces IN/INOUT values before entering the routine's
    // captured sql_mode/security/database context.
    let mut prepared_args = Vec::with_capacity(args.len());
    for (param, arg) in routine.params.iter().zip(args.iter()) {
        let prepared = match param.mode {
            RoutineParamModeSchema::In => {
                let value = match caller_locals.as_deref() {
                    Some(caller) => eval_routine_expr(session, caller, arg)?,
                    None => eval_routine_expr(session, &frame.locals, arg)?,
                };
                (Some(value), None)
            }
            RoutineParamModeSchema::Out => {
                let binding = out_binding_from_expr(arg, caller_locals.as_deref())?;
                (None, Some(binding))
            }
            RoutineParamModeSchema::InOut => {
                let binding = out_binding_from_expr(arg, caller_locals.as_deref())?;
                let value = binding.read(session, caller_locals.as_deref())?;
                (Some(value), Some(binding))
            }
        };
        prepared_args.push(prepared);
    }
    // MySQL assigns IN/INOUT arguments before switching to the routine's
    // saved sql_mode. Consequently truncation/overflow here follows the
    // caller's mode; local assignments and RETURN below follow the routine's
    // creation/ALTER mode.
    let mut out_bindings = Vec::new();
    let parameter_result = (|| -> Result<(), QueryError> {
        for (param, (value, binding)) in routine.params.iter().zip(prepared_args) {
            let name = normalize_local(&param.name);
            frame
                .local_types
                .insert(name.clone(), (param.data_type.clone(), param.unsigned));
            let value = match value {
                Some(value) => {
                    coerce_routine_value(value, &param.name, &param.data_type, param.unsigned)?
                }
                None => Value::Null,
            };
            frame.locals.insert(name, value);
            if let Some(binding) = binding {
                out_bindings.push((param.name.clone(), binding));
            }
        }
        Ok(())
    })();
    if let Err(err) = parameter_result {
        return Err(err);
    }
    let invocation_snapshot = RoutineInvocationSnapshot::capture(session);
    session.current_database = routine.database.clone();
    // Empty sql_mode is a real captured value, not a legacy "missing" marker.
    // Replaying it is required when the invoker currently runs in strict mode.
    session.sql_mode = routine.sql_mode.clone();
    session.temporal_sql_mode_bits =
        crate::engine::api::parse_temporal_sql_mode_bits(&routine.sql_mode)
            | crate::engine::api::TEMPORAL_MODE_EXPLICIT;
    if let Err(err) = enter_routine_security(db, session, &routine) {
        invocation_snapshot.restore(session, routine.kind == RoutineKindSchema::Function);
        return Err(err);
    }
    // B2 — a routine CALLed from inside a trigger must not inherit the
    // trigger's OLD/NEW visibility; clear it for the duration of the body.
    let saved_avail = set_trigger_row_avail(None);
    let txn = match super::tx::ensure_statement_txn(db, session) {
        Ok(txn) => txn,
        Err(err) => {
            set_trigger_row_avail(saved_avail);
            invocation_snapshot.restore(session, routine.kind == RoutineKindSchema::Function);
            return Err(err);
        }
    };
    let control_result = super::select::eval::with_subquery_context(db, txn, || {
        exec_block(db, session, &mut frame, registry, &program.body.statements)
    });
    set_trigger_row_avail(saved_avail);
    let return_value = match control_result.map_err(|err| err.query_error()) {
        Ok(RoutineControl::None) => Ok(None),
        Ok(RoutineControl::Return(value)) if routine.kind == RoutineKindSchema::Function => {
            match routine.return_type.as_ref() {
                Some(data_type) => {
                    coerce_routine_value(value, &routine.name, data_type, routine.return_unsigned)
                        .map(Some)
                }
                None => Ok(Some(value)),
            }
        }
        Ok(RoutineControl::Return(_)) => Err(error(
            crate::error::catalog::ER_SP_BADRETURN,
            "RETURN is only allowed in a FUNCTION",
        )),
        Ok(RoutineControl::Leave(label)) | Ok(RoutineControl::Iterate(label)) => Err(error(
            crate::error::catalog::ER_SP_LILABEL_MISMATCH,
            format!("LEAVE/ITERATE target '{label}' is not valid in routine body"),
        )),
        Err(err) => Err(err),
    };
    invocation_snapshot.restore(session, routine.kind == RoutineKindSchema::Function);
    let return_value = return_value?;
    let mut prepared_out_params = Vec::new();
    for (param_name, binding) in out_bindings {
        let value = frame
            .locals
            .get(&normalize_local(&param_name))
            .cloned()
            .unwrap_or(Value::Null);
        if matches!(binding, OutBinding::PreparedParameter(_)) {
            let column_type = routine
                .params
                .iter()
                .find(|param| param.name.eq_ignore_ascii_case(&param_name))
                .map(|param| param.data_type.column_type())
                .unwrap_or(crate::types::ColumnType::VarString);
            prepared_out_params.push((param_name, column_type, value));
        } else {
            binding.write(session, caller_locals.as_deref_mut(), value)?;
        }
    }
    Ok(RoutineResult {
        responses: frame.responses,
        return_value,
        prepared_out_params,
    })
}

pub(super) fn execute_trigger_body(
    db: &Arc<Database>,
    session: &mut SessionContext,
    trigger: &TriggerSchema,
    table: &TableSchema,
    old_row: Option<Vec<Value>>,
    new_row: Option<Vec<Value>>,
    new_writable: bool,
    registry: &SessionRegistry,
) -> Result<Option<Vec<Value>>, QueryError> {
    let key = (
        trigger.database.to_ascii_lowercase(),
        trigger.name.to_ascii_lowercase(),
    );
    if TRIGGER_STACK.with(|stack| stack.borrow().contains(&key)) {
        return Err(error(
            crate::error::catalog::ER_SP_NO_RECURSION,
            "Recursive stored functions and triggers are not allowed.",
        ));
    }
    struct TriggerInvocation;
    impl Drop for TriggerInvocation {
        fn drop(&mut self) {
            TRIGGER_STACK.with(|stack| {
                stack.borrow_mut().pop();
            });
        }
    }
    TRIGGER_STACK.with(|stack| stack.borrow_mut().push(key));
    let _invocation = TriggerInvocation;
    crate::util::stack::with_stack(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::Executor,
        || {
            execute_trigger_body_checked(
                db,
                session,
                trigger,
                table,
                old_row,
                new_row,
                new_writable,
                registry,
            )
        },
    )?
}

pub(super) fn execute_trigger_body_checked(
    db: &Arc<Database>,
    session: &mut SessionContext,
    trigger: &TriggerSchema,
    table: &TableSchema,
    old_row: Option<Vec<Value>>,
    new_row: Option<Vec<Value>>,
    new_writable: bool,
    registry: &SessionRegistry,
) -> Result<Option<Vec<Value>>, QueryError> {
    let max_depth = std::env::var("MYDB_TRIGGER_MAX_DEPTH")
        .ok()
        .and_then(|raw| raw.parse::<u32>().ok())
        .unwrap_or(32);
    ROUTINE_DEPTH.with(|depth| {
        let current = depth.get();
        if current >= max_depth {
            return Err(error(
                crate::error::catalog::ER_SP_RECURSION_LIMIT,
                format!("recursive trigger limit {max_depth} was exceeded"),
            ));
        }
        depth.set(current + 1);
        let result = execute_trigger_body_inner(
            db,
            session,
            trigger,
            table,
            old_row,
            new_row,
            new_writable,
            registry,
        );
        depth.set(current);
        result
    })
}

#[allow(clippy::too_many_arguments)]
fn execute_trigger_body_inner(
    db: &Arc<Database>,
    session: &mut SessionContext,
    trigger: &TriggerSchema,
    table: &TableSchema,
    old_row: Option<Vec<Value>>,
    new_row: Option<Vec<Value>>,
    new_writable: bool,
    registry: &SessionRegistry,
) -> Result<Option<Vec<Value>>, QueryError> {
    let _partition_scope = super::partition_router::suspend_pruned_partitions();
    let program = compile_trigger(trigger)?;
    let mut frame = RoutineFrame {
        locals: HashMap::new(),
        local_types: HashMap::new(),
        conditions: HashMap::new(),
        cursors: HashMap::new(),
        static_statements: Arc::clone(&program.static_statements),
        parameterized_statements: Arc::clone(&program.parameterized_statements),
        responses: Vec::new(),
        active_handler_error: None,
        block_depth: 0,
        active_handler_scopes: 0,
        outer_handlers: Vec::new(),
        diag_row_count: 0,
        diag_conditions: Vec::new(),
        allow_result_sets: false,
        allow_return: false,
        allow_dynamic_sql: false,
        forbid_transaction_control: true,
        trigger_new_keys: BTreeSet::new(),
        trigger_pending_auto_increment: None,
        trigger_generated_new_keys: BTreeSet::new(),
        trigger_old_keys: BTreeSet::new(),
        trigger_new_writable: new_writable,
        trigger_table: Some(trigger.table_qualified_name()),
    };
    populate_trigger_row_locals(&mut frame, table, "OLD", old_row.as_deref());
    populate_trigger_row_locals(&mut frame, table, "NEW", new_row.as_deref());
    if trigger.event == crate::storage::TriggerEventSchema::Insert && new_writable {
        if let Some(idx) = table.auto_increment_column_index() {
            if new_row.as_ref().is_some_and(|row| row[idx] == Value::Null) {
                let key = normalize_local(&format!("NEW.{}", table.columns[idx].name));
                frame.locals.insert(key.clone(), Value::Int(0));
                frame.trigger_pending_auto_increment = Some(key);
            }
        }
    }

    // B1 — replay the sql_mode captured at CREATE time so the body's behavior
    // (division-by-zero, NO_ZERO_DATE, PIPES_AS_CONCAT, …) is independent of
    // the caller's session mode. Same enter/restore pattern as stored
    // routines. Empty is itself a captured mode and must override the caller.
    let invocation_snapshot = RoutineInvocationSnapshot::capture(session);
    session.current_database = trigger.database.clone();
    session.sql_mode = trigger.sql_mode.clone();
    session.temporal_sql_mode_bits =
        crate::engine::api::parse_temporal_sql_mode_bits(&trigger.sql_mode)
            | crate::engine::api::TEMPORAL_MODE_EXPLICIT;
    if let Err(err) =
        enter_definer_security(db, session, &trigger.definer_user, &trigger.definer_host)
    {
        invocation_snapshot.restore(session, true);
        return Err(err);
    }
    let required = crate::engine::auth::RequiredPrivilege {
        privilege: crate::engine::auth::Privilege::Trigger,
        scope: crate::engine::auth::PrivilegeScope::Table {
            database: trigger.table_database.clone(),
            table: trigger.table_name.clone(),
        },
    };
    let authorized = db.with_auth_catalog(|auth| {
        auth.account_has_privilege(&session.authorization_context(), &required, false)
    });
    if !matches!(authorized, Ok(true)) {
        invocation_snapshot.restore(session, true);
        return Err(match authorized {
            Err(err) => err,
            _ => crate::engine::auth::privilege_denied(&required.privilege, &required.scope),
        });
    }
    let _access_guard = TriggerAccessGuard(TRIGGER_ACCESS.with(|cell| {
        cell.replace(Some(TriggerAccess {
            auth: db.auth_catalog(),
            context: session.authorization_context(),
            database: trigger.table_database.clone(),
            table: trigger.table_name.clone(),
        }))
    }));
    // B2 — advertise which of OLD/NEW this trigger event exposes so nested
    // resolution can reject illegal references with 1363.
    let saved_avail = set_trigger_row_avail(Some(TriggerRowAvail {
        has_old: old_row.is_some(),
        has_new: new_row.is_some(),
        event: trigger.event.as_str(),
    }));
    let txn = match super::tx::ensure_statement_txn(db, session) {
        Ok(txn) => txn,
        Err(err) => {
            set_trigger_row_avail(saved_avail);
            invocation_snapshot.restore(session, true);
            return Err(err);
        }
    };
    let control_result = super::select::eval::with_subquery_context(db, txn, || {
        exec_block(db, session, &mut frame, registry, &program.body.statements)
    });
    set_trigger_row_avail(saved_avail);
    let control_result = control_result.map_err(|err| err.query_error());
    invocation_snapshot.restore(session, true);
    match control_result? {
        RoutineControl::None => {}
        RoutineControl::Return(_) => {
            return Err(error(
                crate::error::catalog::ER_SP_NO_RETSET,
                "RETURN is not allowed in triggers",
            ));
        }
        RoutineControl::Leave(label) | RoutineControl::Iterate(label) => {
            return Err(error(
                crate::error::catalog::ER_SP_LILABEL_MISMATCH,
                format!("LEAVE/ITERATE target '{label}' is not valid in trigger body"),
            ));
        }
    }
    if new_row.is_some() {
        Ok(Some(extract_trigger_new_row(&frame, table)?))
    } else {
        Ok(None)
    }
}

fn populate_trigger_row_locals(
    frame: &mut RoutineFrame,
    table: &TableSchema,
    qualifier: &str,
    row: Option<&[Value]>,
) {
    let Some(row) = row else {
        return;
    };
    for (idx, column) in table.columns.iter().enumerate() {
        if idx >= row.len() {
            continue;
        }
        // B4 — generated columns are readable in OLD/NEW (the row already
        // carries their computed value), but assignment to a generated NEW
        // column is rejected (3105). Record them so assign_target can tell a
        // generated column apart from a plain writable one.
        let generated = column.generated_expr.is_some();
        let key = normalize_local(&format!("{qualifier}.{}", column.name));
        frame.locals.insert(key.clone(), row[idx].clone());
        if qualifier.eq_ignore_ascii_case("NEW") {
            if generated {
                frame.trigger_generated_new_keys.insert(key.clone());
            }
            frame.trigger_new_keys.insert(key.clone());
            frame.local_types.insert(
                key,
                (
                    DataTypeKind::from(column.data_type.clone()),
                    column.unsigned,
                ),
            );
        } else {
            frame.trigger_old_keys.insert(key);
        }
    }
}

fn extract_trigger_new_row(
    frame: &RoutineFrame,
    table: &TableSchema,
) -> Result<Vec<Value>, QueryError> {
    let mut row = Vec::with_capacity(table.columns.len());
    for column in &table.columns {
        // B4 — generated columns are read-only in NEW; emit NULL here so the
        // storage write path (materialize_generated_columns) recomputes them
        // from the possibly-trigger-modified base columns rather than freezing
        // the pre-trigger value.
        if column.generated_expr.is_some() {
            row.push(Value::Null);
            continue;
        }
        let key = normalize_local(&format!("NEW.{}", column.name));
        if frame.trigger_pending_auto_increment.as_ref() == Some(&key) {
            row.push(Value::Null);
        } else {
            row.push(frame.locals.get(&key).cloned().unwrap_or(Value::Null));
        }
    }
    Ok(row)
}

fn enter_routine_security(
    db: &Database,
    session: &mut SessionContext,
    routine: &RoutineSchema,
) -> Result<(), QueryError> {
    if routine.security_type != RoutineSecuritySchema::Definer {
        return Ok(());
    }
    enter_definer_security(db, session, &routine.definer_user, &routine.definer_host)?;
    // MySQL requires EXECUTE both for the invoker and for the DEFINER whose
    // privileges are about to be used. This matters when a privileged creator
    // names a different account as DEFINER: automatic_sp_privileges grants the
    // creator, not that foreign account.
    authorize_routine_execute(db, session, routine)
}

fn enter_definer_security(
    db: &Database,
    session: &mut SessionContext,
    user: &str,
    host: &str,
) -> Result<(), QueryError> {
    let definer = AccountKey::new(user, host);
    let active_roles = db.with_auth_catalog(|auth| {
        auth.accounts
            .contains_key(&account_id(&definer))
            .then(|| auth.login_active_roles(&definer))
    })?;
    let Some(active_roles) = active_roles else {
        return Err(error(
            crate::error::catalog::ER_NO_SUCH_USER,
            format!(
                "The user specified as a definer ({}) does not exist",
                definer.display()
            ),
        ));
    };
    session.authenticated_as = definer;
    session.active_roles = active_roles.into();
    Ok(())
}

#[derive(Debug, Clone)]
enum OutBinding {
    UserVariable(String),
    Local(String),
    PreparedParameter(usize),
}

impl OutBinding {
    fn read(
        &self,
        session: &SessionContext,
        caller_locals: Option<&HashMap<String, Value>>,
    ) -> Result<Value, QueryError> {
        match self {
            OutBinding::UserVariable(name) => {
                Ok(session.user_variable(name).cloned().unwrap_or(Value::Null))
            }
            OutBinding::Local(name) => caller_locals
                .and_then(|locals| locals.get(&normalize_local(name)).cloned())
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_SP_UNDECLARED_VAR,
                        format!("Undeclared variable {name}"),
                    )
                }),
            OutBinding::PreparedParameter(index) => {
                params::current_param(*index).ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_WRONG_ARGUMENTS,
                        "prepared CALL parameter is not bound",
                    )
                })
            }
        }
    }

    fn write(
        &self,
        session: &mut SessionContext,
        caller_locals: Option<&mut HashMap<String, Value>>,
        value: Value,
    ) -> Result<(), QueryError> {
        match self {
            OutBinding::UserVariable(name) => {
                session.set_user_variable(name, value);
                Ok(())
            }
            OutBinding::Local(name) => {
                let Some(locals) = caller_locals else {
                    return Err(error(
                        crate::error::catalog::ER_SP_NOT_VAR_ARG,
                        "OUT or INOUT argument must be a variable",
                    ));
                };
                locals.insert(normalize_local(name), value);
                Ok(())
            }
            OutBinding::PreparedParameter(_) => Ok(()),
        }
    }
}

fn out_binding_from_expr(
    expr: &Expr,
    caller_locals: Option<&HashMap<String, Value>>,
) -> Result<OutBinding, QueryError> {
    match expr {
        Expr::UserVariable(name) => Ok(OutBinding::UserVariable(name.clone())),
        Expr::Identifier(name) if caller_locals.is_some() => Ok(OutBinding::Local(name.clone())),
        Expr::Placeholder(index) if params::has_active_param(*index) => {
            Ok(OutBinding::PreparedParameter(*index))
        }
        _ => Err(error(
            crate::error::catalog::ER_SP_NOT_VAR_ARG,
            "OUT or INOUT argument must be a variable",
        )),
    }
}

fn exec_block(
    db: &Arc<Database>,
    session: &mut SessionContext,
    frame: &mut RoutineFrame,
    registry: &SessionRegistry,
    statements: &[RoutineStmt],
) -> Result<RoutineControl, RoutineExecError> {
    // Nonempty blocks check before each statement. Empty loop bodies still
    // need a cancellation point on every iteration (LOOP/WHILE/REPEAT).
    if statements.is_empty() {
        session.check_cancelled().map_err(RoutineExecError::Query)?;
    }
    let is_root = frame.block_depth == 0;
    frame.block_depth += 1;
    let result = exec_block_inner(db, session, frame, registry, statements, is_root);
    frame.block_depth = frame.block_depth.saturating_sub(1);
    result
}

fn exec_block_inner(
    db: &Arc<Database>,
    session: &mut SessionContext,
    frame: &mut RoutineFrame,
    registry: &SessionRegistry,
    statements: &[RoutineStmt],
    is_root: bool,
) -> Result<RoutineControl, RoutineExecError> {
    let mut handlers: Vec<HandlerDecl> = Vec::new();
    for stmt in statements {
        session.check_cancelled().map_err(RoutineExecError::Query)?;
        let warning_start = crate::engine::api::statement_warning_count();
        let mut result = match stmt {
            RoutineStmt::DeclareHandler {
                action,
                conditions,
                statement,
            } => {
                handlers.push(HandlerDecl {
                    action: *action,
                    conditions: conditions.clone(),
                    statement: (**statement).clone(),
                });
                Ok(RoutineControl::None)
            }
            other => {
                let guarded = !handlers.is_empty();
                if guarded {
                    frame.active_handler_scopes += 1;
                    frame.outer_handlers.push(std::mem::take(&mut handlers));
                }
                let result = exec_stmt(db, session, frame, registry, other);
                if guarded {
                    frame.active_handler_scopes = frame.active_handler_scopes.saturating_sub(1);
                    handlers = frame.outer_handlers.pop().expect("active handler scope");
                }
                result
            }
        };
        if matches!(result, Ok(RoutineControl::None)) {
            let warnings = crate::engine::api::statement_warnings_snapshot();
            let new_warnings = warnings
                .iter()
                .skip(warning_start.min(warnings.len()))
                .cloned()
                .collect::<Vec<_>>();
            // Diagnostics retrieval and lexical declarations do not replace
            // the current area. A compound block retains its last statement's
            // area, including the condition that activated a handler.
            if !matches!(
                stmt,
                RoutineStmt::GetDiagnostics { .. }
                    | RoutineStmt::DeclareVar { .. }
                    | RoutineStmt::DeclareCondition { .. }
                    | RoutineStmt::DeclareCursor { .. }
                    | RoutineStmt::DeclareHandler { .. }
                    | RoutineStmt::Block { .. }
            ) {
                frame.diag_conditions = new_warnings.clone();
            }
            if let Some(warning) = new_warnings
                .iter()
                .rev()
                .find(|warning| warning.level() == crate::error::ConditionLevel::Warning)
            {
                let err = QueryError::user_defined(
                    warning.code(),
                    warning.sql_state().as_str(),
                    warning.message(),
                )
                .map_err(|invalid| {
                    RoutineExecError::Query(error(
                        crate::error::catalog::ER_WRONG_VALUE,
                        format!("invalid warning condition: {invalid}"),
                    ))
                })?;
                let warning = RoutineExecError::Warning(condition_from_query_error(&err), err);
                if find_matching_handler(frame, &handlers, &warning).is_some()
                    || frame.outer_handlers.iter().any(|scope| {
                        scope.iter().any(|handler| {
                            handler
                                .conditions
                                .iter()
                                .any(|condition| condition_matches(frame, condition, &warning))
                        })
                    })
                {
                    result = Err(warning);
                }
            }
        }
        match result {
            Ok(RoutineControl::None) => {}
            Ok(control) => return Ok(control),
            Err(err) => {
                if frame.diag_conditions.is_empty()
                    || !frame.diag_conditions.iter().any(|condition| {
                        let query = err.query_error();
                        condition.code() == query.code()
                            && condition.sql_state().as_str() == query.sql_state()
                    })
                {
                    frame.diag_conditions = vec![crate::engine::api::SqlWarning::from_error(
                        crate::error::ConditionLevel::Error,
                        &err.query_error(),
                    )];
                }
                if let Some(handler) = find_matching_handler(frame, &handlers, &err) {
                    let previous_handler_error = frame.active_handler_error.replace(err.clone());
                    // SHOW WARNINGS / SHOW ERRORS inside a handler reads the
                    // current session diagnostics area. Nested routine
                    // statements bypass the top-level begin/finish boundary,
                    // so expose the handled condition explicitly for the
                    // handler lifetime and restore the caller's area after it
                    // completes. GET [STACKED] DIAGNOSTICS continues to use
                    // `active_handler_error` as its lossless source.
                    let previous_handler_diagnostics = std::mem::replace(
                        &mut session.warnings,
                        vec![crate::engine::api::SqlWarning::from_error(
                            crate::error::ConditionLevel::Error,
                            &err.query_error(),
                        )]
                        .into(),
                    );
                    let handler_result =
                        exec_stmt(db, session, frame, registry, &handler.statement);
                    session.warnings = previous_handler_diagnostics;
                    frame.active_handler_error = previous_handler_error;
                    let handler_result = handler_result?;
                    crate::engine::perf_errors::record_handled(session, &err.query_error());
                    match handler_result {
                        RoutineControl::None => match handler.action {
                            RoutineHandlerAction::Continue => continue,
                            RoutineHandlerAction::Exit => return Ok(RoutineControl::None),
                            RoutineHandlerAction::Undo => {
                                return Err(RoutineExecError::Query(error(
                                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                                    "UNDO handlers are not supported",
                                )))
                            }
                        },
                        control => return Ok(control),
                    }
                } else if is_root {
                    match &err {
                        RoutineExecError::Warning(_, _) => continue,
                        RoutineExecError::Condition(
                            RoutineCondition::NotFound | RoutineCondition::SqlWarning,
                            warning,
                        ) => {
                            if !frame.diag_conditions.iter().any(|condition| {
                                condition.code() == warning.code()
                                    && condition.sql_state().as_str() == warning.sql_state()
                            }) {
                                crate::engine::api::push_statement_condition(
                                    crate::error::ConditionLevel::Warning,
                                    warning,
                                );
                            }
                            continue;
                        }
                        _ => return Err(err),
                    }
                } else {
                    return Err(err);
                }
            }
        }
    }
    Ok(RoutineControl::None)
}

/// Execute a BEGIN/END block with MySQL lexical declaration scope.
/// Assignments to outer variables remain visible, while declarations that
/// shadow an outer local/condition/cursor are restored when the block exits.
fn exec_scoped_block(
    db: &Arc<Database>,
    session: &mut SessionContext,
    frame: &mut RoutineFrame,
    registry: &SessionRegistry,
    statements: &[RoutineStmt],
) -> Result<RoutineControl, RoutineExecError> {
    let mut local_keys = BTreeSet::new();
    let mut condition_keys = BTreeSet::new();
    let mut cursor_keys = BTreeSet::new();
    for stmt in statements {
        match stmt {
            RoutineStmt::DeclareVar { names, .. } => {
                for name in names {
                    let key = normalize_local(name);
                    if !local_keys.insert(key) {
                        return Err(error(
                            crate::error::catalog::ER_SP_DUP_VAR,
                            format!("Duplicate variable: {name}"),
                        )
                        .into());
                    }
                }
            }
            RoutineStmt::DeclareCondition { name, .. } => {
                let key = normalize_local(name);
                if !condition_keys.insert(key) {
                    return Err(error(
                        crate::error::catalog::ER_SP_DUP_COND,
                        format!("Duplicate condition: {name}"),
                    )
                    .into());
                }
            }
            RoutineStmt::DeclareCursor { name, .. } => {
                let key = normalize_local(name);
                if !cursor_keys.insert(key) {
                    return Err(error(
                        crate::error::catalog::ER_SP_DUP_CURS,
                        format!("Duplicate cursor: {name}"),
                    )
                    .into());
                }
            }
            _ => {}
        }
    }

    let saved_locals = local_keys
        .iter()
        .map(|key| {
            (
                key.clone(),
                frame.locals.get(key).cloned(),
                frame.local_types.get(key).cloned(),
            )
        })
        .collect::<Vec<_>>();
    let saved_conditions = condition_keys
        .iter()
        .map(|key| (key.clone(), frame.conditions.get(key).cloned()))
        .collect::<Vec<_>>();
    let saved_cursors = cursor_keys
        .iter()
        .map(|key| (key.clone(), frame.cursors.remove(key)))
        .collect::<Vec<_>>();

    let result = exec_block(db, session, frame, registry, statements);

    for (key, value, data_type) in saved_locals {
        match value {
            Some(value) => {
                frame.locals.insert(key.clone(), value);
            }
            None => {
                frame.locals.remove(&key);
            }
        }
        match data_type {
            Some(data_type) => {
                frame.local_types.insert(key, data_type);
            }
            None => {
                frame.local_types.remove(&key);
            }
        }
    }
    for (key, condition) in saved_conditions {
        match condition {
            Some(condition) => {
                frame.conditions.insert(key, condition);
            }
            None => {
                frame.conditions.remove(&key);
            }
        }
    }
    for (key, cursor) in saved_cursors {
        frame.cursors.remove(&key);
        match cursor {
            Some(cursor) => {
                frame.cursors.insert(key, cursor);
            }
            None => {
                frame.cursors.remove(&key);
            }
        }
    }
    result
}

fn exec_stmt(
    db: &Arc<Database>,
    session: &mut SessionContext,
    frame: &mut RoutineFrame,
    registry: &SessionRegistry,
    stmt: &RoutineStmt,
) -> Result<RoutineControl, RoutineExecError> {
    match stmt {
        RoutineStmt::Block { label, statements } => {
            match exec_scoped_block(db, session, frame, registry, statements)? {
                RoutineControl::Leave(target) if label_matches(label, &target) => {
                    Ok(RoutineControl::None)
                }
                control => Ok(control),
            }
        }
        RoutineStmt::DeclareVar { names, default, .. } => {
            let value = match default {
                Some(expr) => eval_routine_expr(session, &frame.locals, expr)?,
                None => Value::Null,
            };
            let (data_type, unsigned) = match stmt {
                RoutineStmt::DeclareVar {
                    data_type,
                    unsigned,
                    ..
                } => (DataTypeKind::from(data_type.clone()), *unsigned),
                _ => unreachable!(),
            };
            for name in names {
                let key = normalize_local(name);
                frame
                    .local_types
                    .insert(key.clone(), (data_type.clone(), unsigned));
                frame.locals.insert(
                    key,
                    coerce_routine_value(value.clone(), name, &data_type, unsigned)?,
                );
            }
            Ok(RoutineControl::None)
        }
        RoutineStmt::DeclareCondition { name, condition } => {
            frame
                .conditions
                .insert(normalize_local(name), condition.clone());
            Ok(RoutineControl::None)
        }
        RoutineStmt::DeclareCursor { name, select_sql } => {
            frame.cursors.insert(
                normalize_local(name),
                CursorState {
                    select_sql: select_sql.clone(),
                    cursor: None,
                },
            );
            Ok(RoutineControl::None)
        }
        RoutineStmt::DeclareHandler { .. } => Ok(RoutineControl::None),
        RoutineStmt::Set { assignments } => {
            let incoming_row_count = session.last_row_count;
            for (target, expr) in assignments {
                let mut expr = expr.clone();
                bind_routine_string_function_types(&mut expr, &frame.locals, &frame.local_types);
                let value = eval_routine_expr(session, &frame.locals, &expr)?;
                // SET resolves declared locals first, then unqualified system
                // variables. Row qualifiers must retain the trigger checks.
                if let RoutineTarget::Local(name) = target {
                    if !name.contains('.') && !frame.locals.contains_key(&normalize_local(name)) {
                        if frame.forbid_transaction_control
                            && name.eq_ignore_ascii_case("autocommit")
                        {
                            return Err(error(
                                crate::error::catalog::ER_COMMIT_NOT_ALLOWED_IN_SF_OR_TRG,
                                "Explicit or implicit commit is not allowed in stored function or trigger",
                            ).into());
                        }
                        execute_parsed_statement_in_routine(
                            db,
                            session,
                            registry,
                            crate::parser::Statement::Set(vec![crate::parser::SetAssignment {
                                scope: None,
                                name: name.clone(),
                                value: Expr::BoundValue(value),
                            }]),
                            &[],
                            frame.forbid_transaction_control,
                            frame.trigger_table.as_deref(),
                        )?;
                        session.last_row_count = incoming_row_count;
                        continue;
                    }
                }
                assign_target(
                    session,
                    &mut frame.locals,
                    &frame.local_types,
                    &frame.trigger_new_keys,
                    &mut frame.trigger_pending_auto_increment,
                    &frame.trigger_generated_new_keys,
                    &frame.trigger_old_keys,
                    frame.trigger_new_writable,
                    target,
                    value,
                )?;
            }
            session.last_row_count = 0;
            frame.diag_row_count = 0;
            Ok(RoutineControl::None)
        }
        RoutineStmt::SelectInto {
            select_sql,
            targets,
        } => {
            let response = execute_sql_in_routine(
                db,
                session,
                registry,
                select_sql,
                &[],
                &frame.locals,
                &frame.static_statements,
                &frame.parameterized_statements,
                frame.forbid_transaction_control,
                frame.trigger_table.as_deref(),
            )?;
            let row = match select_into_single_row(response) {
                Ok(row) => row,
                Err(err) => {
                    session.last_row_count = if err
                        .query_error()
                        .is(crate::error::catalog::ER_SP_FETCH_NO_DATA)
                    {
                        0
                    } else {
                        -1
                    };
                    frame.diag_row_count = session.last_row_count;
                    return Err(err);
                }
            };
            if row.len() != targets.len() {
                return Err(error(
                    crate::error::catalog::ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT,
                    "SELECT ... INTO target count does not match result column count",
                )
                .into());
            }
            for (target, value) in targets.iter().zip(row.into_iter()) {
                assign_target(
                    session,
                    &mut frame.locals,
                    &frame.local_types,
                    &frame.trigger_new_keys,
                    &mut frame.trigger_pending_auto_increment,
                    &frame.trigger_generated_new_keys,
                    &frame.trigger_old_keys,
                    frame.trigger_new_writable,
                    target,
                    value,
                )?;
            }
            session.last_row_count = 1;
            frame.diag_row_count = 1;
            Ok(RoutineControl::None)
        }
        RoutineStmt::If {
            branches,
            else_branch,
        } => {
            for (cond, stmts) in branches {
                if truthy(&eval_routine_expr(session, &frame.locals, cond)?) {
                    return exec_block(db, session, frame, registry, stmts);
                }
            }
            exec_block(db, session, frame, registry, else_branch)
        }
        RoutineStmt::Case {
            operand,
            branches,
            else_branch,
        } => {
            let operand_value = operand
                .as_ref()
                .map(|expr| eval_routine_expr(session, &frame.locals, expr))
                .transpose()?;
            for (cond, stmts) in branches {
                let matched = if let Some(value) = &operand_value {
                    truthy(&null_cmp_binary(
                        value,
                        BinaryOp::Eq,
                        &eval_routine_expr(session, &frame.locals, cond)?,
                    )?)
                } else {
                    truthy(&eval_routine_expr(session, &frame.locals, cond)?)
                };
                if matched {
                    return exec_block(db, session, frame, registry, stmts);
                }
            }
            if else_branch.is_empty() {
                Err(error(
                    crate::error::catalog::ER_SP_CASE_NOT_FOUND,
                    "Case not found for CASE statement",
                )
                .into())
            } else {
                exec_block(db, session, frame, registry, else_branch)
            }
        }
        RoutineStmt::Loop { label, statements } => loop {
            match exec_block(db, session, frame, registry, statements)? {
                RoutineControl::None => {}
                RoutineControl::Leave(target) if label_matches(label, &target) => {
                    break Ok(RoutineControl::None)
                }
                RoutineControl::Iterate(target) if label_matches(label, &target) => continue,
                control => break Ok(control),
            }
        },
        RoutineStmt::While {
            label,
            condition,
            statements,
        } => {
            while truthy(&eval_routine_expr(session, &frame.locals, condition)?) {
                match exec_block(db, session, frame, registry, statements)? {
                    RoutineControl::None => {}
                    RoutineControl::Leave(target) if label_matches(label, &target) => break,
                    RoutineControl::Iterate(target) if label_matches(label, &target) => continue,
                    control => return Ok(control),
                }
            }
            Ok(RoutineControl::None)
        }
        RoutineStmt::Repeat {
            label,
            statements,
            until,
        } => {
            loop {
                match exec_block(db, session, frame, registry, statements)? {
                    RoutineControl::None => {}
                    RoutineControl::Leave(target) if label_matches(label, &target) => break,
                    RoutineControl::Iterate(target) if label_matches(label, &target) => {}
                    control => return Ok(control),
                }
                if truthy(&eval_routine_expr(session, &frame.locals, until)?) {
                    break;
                }
            }
            Ok(RoutineControl::None)
        }
        RoutineStmt::Leave(label) => Ok(RoutineControl::Leave(label.clone())),
        RoutineStmt::Iterate(label) => Ok(RoutineControl::Iterate(label.clone())),
        RoutineStmt::Return(expr) => {
            if !frame.allow_return {
                return Err(error(
                    crate::error::catalog::ER_SP_NO_RETSET,
                    "RETURN is not allowed in triggers",
                )
                .into());
            }
            let mut expr = expr.clone();
            bind_routine_string_function_types(&mut expr, &frame.locals, &frame.local_types);
            Ok(RoutineControl::Return(eval_routine_expr(
                session,
                &frame.locals,
                &expr,
            )?))
        }
        RoutineStmt::Open(name) => {
            let key = normalize_local(name);
            let Some(cursor) = frame.cursors.get_mut(&key) else {
                return Err(error(
                    crate::error::catalog::ER_SP_CURSOR_MISMATCH,
                    format!("Undefined CURSOR {name}"),
                )
                .into());
            };
            if cursor.cursor.is_some() {
                return Err(error(
                    crate::error::catalog::ER_SP_CURSOR_ALREADY_OPEN,
                    format!("Cursor {name} is already open"),
                )
                .into());
            }
            let response = execute_sql_in_routine(
                db,
                session,
                registry,
                &cursor.select_sql,
                &[],
                &frame.locals,
                &frame.static_statements,
                &frame.parameterized_statements,
                frame.forbid_transaction_control,
                frame.trigger_table.as_deref(),
            )?;
            let result_cursor: Box<dyn crate::engine::api::ResultCursor> = match response {
                QueryResponse::ResultSet(result) => {
                    Box::new(crate::engine::api::VecResultCursor::new(result.rows))
                }
                QueryResponse::StreamingResultSet(result) => result.cursor,
                _ => {
                    return Err(error(
                        crate::error::catalog::ER_SP_CURSOR_MISMATCH,
                        "cursor SELECT did not produce a result set",
                    )
                    .into())
                }
            };
            cursor.cursor = Some(result_cursor);
            Ok(RoutineControl::None)
        }
        RoutineStmt::Fetch { cursor, targets } => {
            let key = normalize_local(cursor);
            let Some(cursor_state) = frame.cursors.get_mut(&key) else {
                return Err(error(
                    crate::error::catalog::ER_SP_CURSOR_MISMATCH,
                    format!("Undefined CURSOR {cursor}"),
                )
                .into());
            };
            let Some(result_cursor) = cursor_state.cursor.as_mut() else {
                return Err(error(
                    crate::error::catalog::ER_SP_CURSOR_NOT_OPEN,
                    format!("Cursor {cursor} is not open"),
                )
                .into());
            };
            let Some(row) = result_cursor.next_row()? else {
                return Err(not_found_condition());
            };
            if row.len() != targets.len() {
                return Err(error(
                    crate::error::catalog::ER_SP_WRONG_NO_OF_FETCH_ARGS,
                    "FETCH target count mismatch",
                )
                .into());
            }
            for (target, value) in targets.iter().zip(row.into_iter()) {
                assign_target(
                    session,
                    &mut frame.locals,
                    &frame.local_types,
                    &frame.trigger_new_keys,
                    &mut frame.trigger_pending_auto_increment,
                    &frame.trigger_generated_new_keys,
                    &frame.trigger_old_keys,
                    frame.trigger_new_writable,
                    target,
                    value,
                )?;
            }
            Ok(RoutineControl::None)
        }
        RoutineStmt::Close(name) => {
            let Some(cursor) = frame.cursors.get_mut(&normalize_local(name)) else {
                return Err(error(
                    crate::error::catalog::ER_SP_CURSOR_MISMATCH,
                    format!("Undefined CURSOR {name}"),
                )
                .into());
            };
            if cursor.cursor.is_none() {
                return Err(error(
                    crate::error::catalog::ER_SP_CURSOR_NOT_OPEN,
                    format!("Cursor {name} is not open"),
                )
                .into());
            }
            cursor.cursor = None;
            Ok(RoutineControl::None)
        }
        RoutineStmt::Signal {
            condition,
            message_text,
            mysql_errno,
        } => {
            let (resolved_condition, sql_state) = resolve_signal_condition(frame, condition)?;
            let message = signal_message_value(session, &frame.locals, message_text.as_ref())?
                .unwrap_or_else(|| "Unhandled user-defined exception condition".to_string());
            let code =
                signal_errno_value(session, &frame.locals, mysql_errno.as_ref())?.unwrap_or(1644);
            let err = QueryError::user_defined(code, &sql_state, message).map_err(|invalid| {
                error(
                    crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                    format!("Invalid SIGNAL condition: {invalid}"),
                )
            })?;
            Err(RoutineExecError::Condition(resolved_condition, err))
        }
        RoutineStmt::Resignal {
            condition,
            message_text,
            mysql_errno,
        } => {
            let Some(active) = frame.active_handler_error.clone() else {
                return Err(error(
                    crate::error::catalog::ER_RESIGNAL_WITHOUT_ACTIVE_HANDLER,
                    "RESIGNAL when handler not active",
                )
                .into());
            };
            if condition.is_none() && message_text.is_none() && mysql_errno.is_none() {
                return Err(active);
            }
            let active_error = active.query_error();
            let resolved = condition
                .as_ref()
                .map(|condition| resolve_signal_condition(frame, condition))
                .transpose()?;
            let state = resolved
                .as_ref()
                .map(|(_, state)| state.clone())
                .unwrap_or_else(|| active_error.sql_state().to_string());
            let message = signal_message_value(session, &frame.locals, message_text.as_ref())?
                .unwrap_or_else(|| active_error.message().to_string());
            let code = signal_errno_value(session, &frame.locals, mysql_errno.as_ref())?
                .unwrap_or_else(|| active_error.code());
            let err = QueryError::user_defined(code, &state, message).map_err(|invalid| {
                error(
                    crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                    format!("Invalid RESIGNAL condition: {invalid}"),
                )
            })?;
            let condition = resolved
                .map(|(condition, _)| condition)
                .unwrap_or_else(|| active.condition());
            Err(RoutineExecError::Condition(condition, err))
        }
        RoutineStmt::Call(call) => {
            let inherited_capabilities = RoutineCapabilities::from_frame(frame);
            let result = execute_procedure_call(
                db,
                session,
                call.clone(),
                registry,
                Some(&mut frame.locals),
                Some(inherited_capabilities),
            )?;
            for response in result.responses {
                collect_routine_response(frame, response)?;
            }
            Ok(RoutineControl::None)
        }
        RoutineStmt::Prepare { name, source } => {
            let prepare_started = std::time::Instant::now();
            if !frame.allow_dynamic_sql {
                return Err(error(
                    crate::error::catalog::ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG,
                    "Dynamic SQL is not allowed in stored function or trigger",
                )
                .into());
            }
            if !matches!(source, Expr::Literal(_) | Expr::UserVariable(_)) {
                return Err(error(
                    crate::error::catalog::ER_PARSE_ERROR,
                    "PREPARE source must be a string literal or user variable",
                )
                .into());
            }
            let source_value = eval_routine_expr(session, &frame.locals, source)?;
            if matches!(source_value, Value::Null) {
                return Err(error(
                    crate::error::catalog::ER_PARSE_ERROR,
                    "PREPARE source may not be NULL",
                )
                .into());
            }
            let sql = crate::storage::value_to_string(&source_value);
            let key = normalize_local(name);
            // MySQL implicitly drops an existing statement with this name
            // before attempting the replacement; a failed PREPARE therefore
            // must not leave the old statement executable.
            session.remove_sql_prepared_statement(&key);
            let _mode = crate::parser::ParserSqlModeGuard::activate(session.temporal_sql_mode_bits);
            let mut statement = parse_statement(&sql)
                .map_err(|err| RoutineExecError::Query(err.spec.error(err.message)))?;
            if matches!(statement, crate::parser::Statement::MultiStatement { .. }) {
                return Err(error(
                    crate::error::catalog::ER_PARSE_ERROR,
                    "PREPARE does not support multiple statements",
                )
                .into());
            }
            if matches!(
                statement,
                crate::parser::Statement::Unsupported { .. }
                    | crate::parser::Statement::LoadData(_)
            ) {
                return Err(error(
                    crate::error::catalog::ER_UNSUPPORTED_PS,
                    "This command is not supported in the prepared statement protocol yet",
                )
                .into());
            }
            let mut next_placeholder = 0;
            crate::parser::number_statement_placeholders(&mut statement, &mut next_placeholder);
            let retained_capacity = key
                .capacity()
                .saturating_add(sql.capacity().saturating_mul(16))
                .saturating_add(session.current_database.capacity())
                .saturating_add(4096);
            let memory_charge = Arc::new(
                crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::DerivedCache,
                    retained_capacity,
                )
                .map_err(RoutineExecError::Query)?,
            );
            session
                .insert_sql_prepared_statement(
                    key,
                    crate::engine::api::SessionSqlPreparedStatement {
                        pfs: crate::engine::session_registry::PreparedStatementSnapshot::sql(
                            name.clone(),
                            Arc::from(sql.as_str()),
                            session.connection_id as u64,
                            retained_capacity as u64,
                            prepare_started.elapsed(),
                        ),
                        original_sql: Arc::from(sql.as_str()),
                        parser_sql_mode_bits: session.temporal_sql_mode_bits,
                        statement: Arc::new(statement),
                        database: session.current_database.clone(),
                        parameter_count: next_placeholder,
                        result_columns: Arc::from([]),
                        memory_charge,
                    },
                )
                .map_err(RoutineExecError::Query)?;
            Ok(RoutineControl::None)
        }
        RoutineStmt::Execute { name, using } => {
            if !frame.allow_dynamic_sql {
                return Err(error(
                    crate::error::catalog::ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG,
                    "Dynamic SQL is not allowed in stored function or trigger",
                )
                .into());
            }
            let Some(prepared) = session
                .sql_prepared_statements
                .get(&normalize_local(name))
                .cloned()
            else {
                return Err(error(
                    crate::error::catalog::ER_UNKNOWN_STMT_HANDLER,
                    "unknown prepared statement handler",
                )
                .into());
            };
            let execute_started = std::time::Instant::now();
            let params = using
                .iter()
                .map(|target| read_target(session, &frame.locals, target))
                .collect::<Result<Vec<_>, _>>()?;
            if params.len() != prepared.parameter_count {
                session.record_sql_prepared_execute(name, execute_started.elapsed(), true);
                return Err(error(
                    crate::error::catalog::ER_WRONG_ARGUMENTS,
                    "Incorrect arguments to EXECUTE",
                )
                .into());
            }
            let saved_database =
                std::mem::replace(&mut session.current_database, prepared.database);
            let overflow_context = routine_overflow_context(&prepared.statement);
            let mut execute = || {
                execute_parsed_statement_in_routine(
                    db,
                    session,
                    registry,
                    (*prepared.statement).clone(),
                    &params,
                    frame.forbid_transaction_control,
                    frame.trigger_table.as_deref(),
                )
            };
            let response = match overflow_context {
                Some(context) => {
                    crate::engine::expr::with_integer_overflow_context(context, execute)
                }
                None => execute(),
            };
            session.current_database = saved_database;
            session.record_sql_prepared_execute(name, execute_started.elapsed(), response.is_err());
            let response = response?;
            collect_routine_response(frame, response)?;
            Ok(RoutineControl::None)
        }
        RoutineStmt::Deallocate { name } => {
            if !frame.allow_dynamic_sql {
                return Err(error(
                    crate::error::catalog::ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG,
                    "Dynamic SQL is not allowed in stored function or trigger",
                )
                .into());
            }
            if !session.remove_sql_prepared_statement(&normalize_local(name)) {
                return Err(error(
                    crate::error::catalog::ER_UNKNOWN_STMT_HANDLER,
                    "unknown prepared statement handler",
                )
                .into());
            }
            Ok(RoutineControl::None)
        }
        RoutineStmt::Sql(sql) => {
            let response = match execute_sql_in_routine(
                db,
                session,
                registry,
                sql,
                &[],
                &frame.locals,
                &frame.static_statements,
                &frame.parameterized_statements,
                frame.forbid_transaction_control,
                frame.trigger_table.as_deref(),
            ) {
                Ok(response) if frame.active_handler_scopes == 0 => response,
                Ok(response) => response.materialize().map_err(RoutineExecError::Query)?,
                Err(err) => {
                    // A SELECT sends result-column metadata before it starts
                    // evaluating rows. If row evaluation then raises a
                    // handled condition, clients still observe that empty
                    // result-set header before the handler's response.
                    if let Some(columns) = routine_failed_select_columns(
                        db,
                        sql,
                        &frame.locals,
                        &frame.static_statements,
                    ) {
                        collect_routine_response(
                            frame,
                            QueryResponse::ResultSet(crate::engine::api::ResultSet {
                                columns,
                                rows: Vec::new(),
                            }),
                        )?;
                    }
                    return Err(err);
                }
            };
            frame.diag_row_count = response_row_count(&response);
            collect_routine_response(frame, response)?;
            Ok(RoutineControl::None)
        }
        RoutineStmt::GetDiagnostics {
            stacked,
            condition,
            items,
        } => exec_get_diagnostics(session, frame, *stacked, condition.as_ref(), items),
    }
}

fn resolve_signal_condition(
    frame: &RoutineFrame,
    condition: &RoutineCondition,
) -> Result<(RoutineCondition, String), RoutineExecError> {
    let resolved =
        match condition {
            RoutineCondition::Named(name) => frame
                .conditions
                .get(&normalize_local(name))
                .ok_or_else(|| {
                    RoutineExecError::Query(error(
                        crate::error::catalog::ER_SP_COND_MISMATCH,
                        format!("Undefined CONDITION: {name}"),
                    ))
                })?,
            condition => condition,
        };
    match resolved {
        RoutineCondition::SqlState(state) => {
            Ok((RoutineCondition::SqlState(state.clone()), state.clone()))
        }
        _ => Err(error(
            crate::error::catalog::ER_SIGNAL_BAD_CONDITION_TYPE,
            "SIGNAL/RESIGNAL can only use a condition defined with SQLSTATE",
        )
        .into()),
    }
}

fn routine_failed_select_columns(
    db: &Database,
    sql: &str,
    locals: &HashMap<String, Value>,
    static_statements: &HashMap<String, CompiledStaticStatement>,
) -> Option<Vec<crate::types::Column>> {
    let mut statement = (*static_statements.get(sql)?.statement).clone();
    bind_statement_locals(&mut statement, locals).ok()?;
    let crate::parser::Statement::Select(select) = statement else {
        return None;
    };
    super::select::describe_select_columns(db, &select).ok()
}

fn response_row_count(response: &QueryResponse) -> i64 {
    match response {
        QueryResponse::OkAffected { affected_rows } => *affected_rows as i64,
        QueryResponse::ResultSet(rs) => rs.rows.len() as i64,
        _ => 0,
    }
}

fn select_into_single_row(response: QueryResponse) -> Result<Vec<Value>, RoutineExecError> {
    match response {
        QueryResponse::ResultSet(mut result) => match result.rows.len() {
            0 => Err(not_found_condition()),
            1 => Ok(result.rows.pop().expect("single SELECT INTO row")),
            _ => Err(error(
                crate::error::catalog::ER_TOO_MANY_ROWS,
                "Result consisted of more than one row",
            )
            .into()),
        },
        QueryResponse::StreamingResultSet(mut result) => {
            let first = result.cursor.next_row()?;
            let second = result.cursor.next_row()?;
            for warning in result.cursor.take_warnings() {
                crate::engine::api::push_sql_warning(warning);
            }
            match (first, second) {
                (None, _) => Err(not_found_condition()),
                (Some(row), None) => Ok(row),
                (Some(_), Some(_)) => Err(error(
                    crate::error::catalog::ER_TOO_MANY_ROWS,
                    "Result consisted of more than one row",
                )
                .into()),
            }
        }
        _ => Err(error(
            crate::error::catalog::ER_TOO_MANY_ROWS,
            "SELECT ... INTO requires a result set",
        )
        .into()),
    }
}

/// Execute `GET [CURRENT|STACKED] DIAGNOSTICS`. Statement-area items
/// (`ROW_COUNT`, `NUMBER`) read the current frame's diagnostics; condition-area
/// items read the condition currently being handled (`active_handler_error`),
/// which is what `GET STACKED DIAGNOSTICS` — and `GET CURRENT DIAGNOSTICS
/// CONDITION` inside a handler — observe.
pub(super) fn execute_get_diagnostics(
    session: &mut SessionContext,
    stacked: bool,
    condition: Option<&Expr>,
    items: &[(RoutineTarget, DiagItem)],
) -> Result<QueryResponse, QueryError> {
    let mut frame = RoutineFrame {
        diag_row_count: session.diagnostics_row_count,
        diag_conditions: session.warnings.iter().cloned().collect(),
        ..Default::default()
    };
    exec_get_diagnostics(session, &mut frame, stacked, condition, items)
        .map_err(|err| err.query_error())?;
    Ok(QueryResponse::Ok)
}

fn exec_get_diagnostics(
    session: &mut SessionContext,
    frame: &mut RoutineFrame,
    stacked: bool,
    condition: Option<&Expr>,
    items: &[(RoutineTarget, DiagItem)],
) -> Result<RoutineControl, RoutineExecError> {
    if stacked && frame.active_handler_error.is_none() {
        return Err(error(
            crate::error::catalog::ER_GET_STACKED_DA_WITHOUT_ACTIVE_HANDLER,
            "GET STACKED DIAGNOSTICS when handler not active",
        )
        .into());
    }
    let stacked_condition = frame.active_handler_error.as_ref().map(|err| {
        crate::engine::api::SqlWarning::from_error(
            crate::error::ConditionLevel::Error,
            &err.query_error(),
        )
    });
    let conditions = if stacked {
        stacked_condition.into_iter().collect::<Vec<_>>()
    } else {
        frame.diag_conditions.clone()
    };
    let selected = if let Some(cond_expr) = condition {
        let number = crate::storage::to_i64(eval_routine_expr(session, &frame.locals, cond_expr)?)?;
        let index = usize::try_from(number)
            .ok()
            .and_then(|number| number.checked_sub(1))
            .filter(|index| *index < conditions.len())
            .ok_or_else(|| {
                RoutineExecError::Query(error(
                    crate::error::catalog::ER_DA_INVALID_CONDITION_NUMBER,
                    "Invalid condition number",
                ))
            })?;
        Some(conditions[index].clone())
    } else {
        None
    };
    let condition_count = conditions.len() as i64;
    for (target, item) in items {
        let value = match item {
            DiagItem::RowCount => Value::Int(frame.diag_row_count),
            DiagItem::Number => Value::Int(condition_count),
            DiagItem::MysqlErrno => Value::Int(selected.as_ref().map_or(0, |e| e.code() as i64)),
            DiagItem::ReturnedSqlState => Value::String(selected.as_ref().map_or_else(
                || "00000".to_string(),
                |e| e.sql_state().as_str().to_string(),
            )),
            DiagItem::MessageText => Value::String(
                selected
                    .as_ref()
                    .map_or_else(String::new, |e| e.message().to_string()),
            ),
            DiagItem::ClassOrigin | DiagItem::SubclassOrigin => {
                Value::String(selected.as_ref().map_or_else(String::new, |e| {
                    if e.sql_state().as_str().starts_with("01")
                        || e.sql_state().as_str().starts_with("02")
                    {
                        "ISO 9075".to_string()
                    } else {
                        "MySQL".to_string()
                    }
                }))
            }
            DiagItem::ConstraintCatalog
            | DiagItem::ConstraintSchema
            | DiagItem::ConstraintName
            | DiagItem::CatalogName
            | DiagItem::SchemaName
            | DiagItem::TableName
            | DiagItem::ColumnName
            | DiagItem::CursorName => Value::String(String::new()),
        };
        assign_target(
            session,
            &mut frame.locals,
            &frame.local_types,
            &frame.trigger_new_keys,
            &mut frame.trigger_pending_auto_increment,
            &frame.trigger_generated_new_keys,
            &frame.trigger_old_keys,
            frame.trigger_new_writable,
            target,
            value,
        )?;
    }
    Ok(RoutineControl::None)
}

fn execute_sql_in_routine(
    db: &Arc<Database>,
    session: &mut SessionContext,
    registry: &SessionRegistry,
    sql: &str,
    params: &[Value],
    locals: &HashMap<String, Value>,
    static_statements: &HashMap<String, CompiledStaticStatement>,
    parameterized_statements: &HashMap<String, HashMap<Vec<String>, ParameterizedStaticStatement>>,
    forbid_transaction_control: bool,
    trigger_table: Option<&str>,
) -> Result<QueryResponse, RoutineExecError> {
    let mut bound_params = params.to_vec();
    let mut used_parameterized_template = false;
    let source_overflow_context = static_statements
        .get(sql)
        .and_then(|compiled| routine_overflow_context(&compiled.statement));
    let mut statement = if let Some(compiled) = static_statements.get(sql) {
        let slot_names = compiled
            .identifiers
            .iter()
            .filter(|name| locals.contains_key(name.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if let Some(template) = parameterized_statements
            .get(sql)
            .and_then(|templates| templates.get(&slot_names))
        {
            bound_params = template
                .slot_names
                .iter()
                .map(|name| locals.get(name).cloned().unwrap_or(Value::Null))
                .collect();
            used_parameterized_template = true;
            (*template.statement).clone()
        } else {
            (*compiled.statement).clone()
        }
    } else {
        let _mode = crate::parser::ParserSqlModeGuard::activate(session.temporal_sql_mode_bits);
        let mut statement = parse_statement(sql).map_err(|err| {
            RoutineExecError::Query(
                err.spec
                    .error(format!("routine SQL parse error: {}", err.message)),
            )
        })?;
        let mut next_placeholder = 0;
        crate::parser::number_statement_placeholders(&mut statement, &mut next_placeholder);
        statement
    };
    let overflow_context = source_overflow_context.or_else(|| routine_overflow_context(&statement));
    if !used_parameterized_template || locals.keys().any(|name| name.contains('.')) {
        bind_statement_locals(&mut statement, locals).map_err(RoutineExecError::Query)?;
    }
    let execute = || {
        execute_parsed_statement_in_routine(
            db,
            session,
            registry,
            statement,
            &bound_params,
            forbid_transaction_control,
            trigger_table,
        )
    };
    match overflow_context {
        Some(context) => crate::engine::expr::with_integer_overflow_context(context, execute),
        None => execute(),
    }
}

/// Preserve the source expression in numeric-overflow diagnostics after
/// routine locals are bound to `BoundValue`.  MySQL numbers local references
/// by their argument position in the native function's internal item tree.
fn routine_overflow_context<'q>(
    statement: impl Into<crate::parser::QueryRef<'q>>,
) -> Option<String> {
    let statement = statement.into();
    fn find(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Function { name, args, .. }
                if matches!(
                    name.to_ascii_uppercase().as_str(),
                    "ABS" | "CEIL" | "CEILING" | "FLOOR" | "ROUND" | "TRUNCATE"
                ) =>
            {
                let rendered = args
                    .iter()
                    .enumerate()
                    .map(|(index, arg)| match arg {
                        Expr::Identifier(name) => format!("{name}@{index}"),
                        _ => crate::parser::expr_to_sql(arg),
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                Some(format!("{}({rendered})", name.to_ascii_lowercase()))
            }
            Expr::Function { args, .. } => args.iter().find_map(find),
            Expr::Unary { expr, .. }
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::GroupKeyRef { expr, .. } => find(expr),
            Expr::Binary { left, right, .. }
            | Expr::Like {
                expr: left,
                pattern: right,
                ..
            } => find(left).or_else(|| find(right)),
            _ => None,
        }
    }

    let crate::parser::QueryRef::Select(select) = statement else {
        return None;
    };
    select.projection.iter().find_map(|item| match item {
        crate::parser::SelectItem::Expr { expr, .. } => find(expr),
        _ => None,
    })
}

fn execute_parsed_statement_in_routine(
    db: &Arc<Database>,
    session: &mut SessionContext,
    registry: &SessionRegistry,
    statement: crate::parser::Statement,
    params: &[Value],
    forbid_transaction_control: bool,
    trigger_table: Option<&str>,
) -> Result<QueryResponse, RoutineExecError> {
    if forbid_transaction_control {
        reject_restricted_sql_statement(session, &statement, trigger_table)?;
    }
    let previous_explicit = session.explicit_txn;
    let held_trigger_transaction = trigger_table.is_some() && session.current_txn.is_some();
    if held_trigger_transaction {
        session.explicit_txn = true;
    }
    crate::mysql::connection_manager::record_internal_query();
    let session_ptr = std::ptr::NonNull::from(&mut *session);
    // SAFETY: the SessionContext stays pinned for this closure. The expression
    // layer only reads through its TLS pointer; statement mutations continue
    // through the unique `&mut session` on this connection thread.
    let outer_insert_id = crate::engine::api::snapshot_statement_insert_id();
    crate::engine::api::clear_statement_insert_id();
    let result = unsafe {
        crate::engine::expr::with_session_expr_info_raw(session_ptr, || {
            params::with_params(params.to_vec(), || {
                super::dispatch::execute_statement(db, session, statement, registry)
            })
        })
    };
    let insert_id = crate::engine::api::statement_insert_id_value();
    if insert_id != 0 {
        session
            .last_insert_id
            .store(insert_id, std::sync::atomic::Ordering::Relaxed);
    }
    // The child owns its diagnostics. Keep its generated id available to
    // LAST_INSERT_ID(), while preserving the caller's OK-packet accumulator.
    if !crate::engine::api::statement_insert_id_is_set() {
        crate::engine::api::restore_statement_insert_id(outer_insert_id);
    }
    session.last_row_count = result.as_ref().map(super::response_row_count).unwrap_or(-1);
    if held_trigger_transaction {
        session.explicit_txn = previous_explicit;
    }
    result.map_err(|err| RoutineExecError::Condition(condition_from_query_error(&err), err))
}

pub(super) fn validate_trigger_sql_statement(
    session: &SessionContext,
    statement: &crate::parser::Statement,
) -> Result<(), QueryError> {
    // The table self-write restriction is execution-dependent (1442), while
    // transaction/DDL restrictions can and should be checked at CREATE.
    reject_restricted_sql_statement(session, statement, None).map_err(|err| err.query_error())
}

fn reject_restricted_sql_statement(
    session: &SessionContext,
    statement: &crate::parser::Statement,
    trigger_table: Option<&str>,
) -> Result<(), RoutineExecError> {
    use crate::parser::Statement;
    let commit_error = || {
        error(
            crate::error::catalog::ER_COMMIT_NOT_ALLOWED_IN_SF_OR_TRG,
            "Explicit or implicit commit is not allowed in stored function or trigger",
        )
    };
    match statement {
        Statement::Begin { .. }
        | Statement::Commit { .. }
        | Statement::Rollback { .. }
        | Statement::Savepoint(_)
        | Statement::RollbackToSavepoint(_)
        | Statement::ReleaseSavepoint(_)
        | Statement::SetAutocommit(_)
        | Statement::SetTransactionIsolation { .. }
        | Statement::CreateTable(_)
        | Statement::CreateTableLike { .. }
        | Statement::AlterTable(_)
        | Statement::CreateDatabase { .. }
        | Statement::DropDatabase { .. }
        | Statement::DropTable { .. }
        | Statement::TruncateTable { .. }
        | Statement::CreateIndex(_)
        | Statement::DropIndex { .. }
        | Statement::AnalyzeTable { .. }
        | Statement::CreateView { .. }
        | Statement::AlterView { .. }
        | Statement::DropView { .. }
        | Statement::CreateRoutine(_)
        | Statement::AlterRoutine { .. }
        | Statement::DropRoutine { .. }
        | Statement::CreateTrigger(_)
        | Statement::DropTrigger { .. }
        | Statement::CreateUser(_)
        | Statement::AlterUser(_)
        | Statement::AlterUsers(_)
        | Statement::DropUser { .. }
        | Statement::RenameUser { .. }
        | Statement::CreateRole { .. }
        | Statement::DropRole { .. }
        | Statement::Grant(_)
        | Statement::Revoke(_)
        | Statement::SetPassword { .. }
        | Statement::SetDefaultRole(_)
        | Statement::LoadData(_) => return Err(commit_error().into()),
        Statement::Insert(insert)
            if trigger_table.is_some_and(|trigger_table| {
                same_table_name(&session.current_database, &insert.table, trigger_table)
            }) =>
        {
            return Err(trigger_self_table_error().into());
        }
        Statement::Update(update)
            if trigger_table.is_some_and(|trigger_table| {
                same_table_name(&session.current_database, &update.table, trigger_table)
            }) =>
        {
            return Err(trigger_self_table_error().into());
        }
        Statement::Delete(delete)
            if trigger_table.is_some_and(|trigger_table| {
                same_table_name(&session.current_database, &delete.table, trigger_table)
            }) =>
        {
            return Err(trigger_self_table_error().into());
        }
        _ => {}
    }
    Ok(())
}

fn same_table_name(current_database: &str, left: &str, right: &str) -> bool {
    let (left_db, left_table) = split_table_name(current_database, left);
    let (right_db, right_table) = split_table_name(current_database, right);
    left_db == right_db && left_table == right_table
}

fn trigger_self_table_error() -> QueryError {
    error(crate::error::catalog::ER_CANT_UPDATE_USED_TABLE_IN_SF_OR_TRG,
        "Can't update table in stored function/trigger because it is already used by statement which invoked this trigger",
    )
}

fn bind_statement_locals<'q>(
    statement: impl Into<crate::parser::QueryMut<'q>>,
    locals: &HashMap<String, Value>,
) -> Result<(), QueryError> {
    let statement = statement.into();
    use crate::parser::{InsertSource, Statement};
    match statement {
        crate::parser::QueryMut::Select(select) => bind_select_locals(select, locals)?,
        crate::parser::QueryMut::Rrf(rrf) => {
            for branch in &mut rrf.branches {
                bind_statement_locals(&mut branch.query, locals)?;
            }
        }
        crate::parser::QueryMut::SetOperation(set) => {
            bind_statement_locals(&mut set.left, locals)?;
            bind_statement_locals(&mut set.right, locals)?;
            for order in &mut set.order_by {
                bind_expr_locals(&mut order.expr, locals)?;
            }
        }
        crate::parser::QueryMut::WithCte(mut cte) => {
            for def in cte.ctes.iter_mut() {
                bind_statement_locals(&mut def.query, locals)?;
            }
            bind_statement_locals(&mut cte.body, locals)?;
        }
        crate::parser::QueryMut::Other(Statement::Explain { target, .. }) => {
            bind_statement_locals(target, locals)?;
        }
        crate::parser::QueryMut::Other(Statement::CreateTableAsSelect { query, .. }) => {
            bind_statement_locals(query, locals)?;
        }
        crate::parser::QueryMut::Other(Statement::SelectIntoVars { inner, .. })
        | crate::parser::QueryMut::Other(Statement::SelectIntoFile { inner, .. }) => {
            bind_statement_locals(inner, locals)?
        }
        crate::parser::QueryMut::Other(Statement::Insert(insert)) => {
            match &mut insert.source {
                InsertSource::Values(rows) => {
                    for row in rows {
                        for expr in row {
                            bind_expr_locals(expr, locals)?;
                        }
                    }
                }
                InsertSource::Query(query) => bind_statement_locals(query, locals)?,
            }
            if let Some(predicate) = &mut insert.view_check_predicate {
                bind_expr_locals(predicate, locals)?;
            }
            if let Some(assignments) = &mut insert.on_duplicate_key_update {
                for assignment in assignments {
                    bind_expr_locals(&mut assignment.value, locals)?;
                }
            }
            bind_returning_locals(&mut insert.returning, locals)?;
        }
        crate::parser::QueryMut::Other(Statement::Update(update)) => {
            for assignment in &mut update.assignments {
                bind_expr_locals(&mut assignment.value, locals)?;
            }
            if let Some(selection) = &mut update.selection {
                bind_expr_locals(selection, locals)?;
            }
            if let Some(predicate) = &mut update.view_check_predicate {
                bind_expr_locals(predicate, locals)?;
            }
            for join in &mut update.joins {
                if let Some(condition) = &mut join.condition {
                    bind_expr_locals(condition, locals)?;
                }
                if let Some(derived) = &mut join.derived {
                    bind_subquery_locals(derived, locals)?;
                }
            }
            for order in &mut update.order_by {
                bind_expr_locals(&mut order.expr, locals)?;
            }
            bind_returning_locals(&mut update.returning, locals)?;
        }
        crate::parser::QueryMut::Other(Statement::Delete(delete)) => {
            if let Some(selection) = &mut delete.selection {
                bind_expr_locals(selection, locals)?;
            }
            for join in &mut delete.joins {
                if let Some(condition) = &mut join.condition {
                    bind_expr_locals(condition, locals)?;
                }
                if let Some(derived) = &mut join.derived {
                    bind_subquery_locals(derived, locals)?;
                }
            }
            for order in &mut delete.order_by {
                bind_expr_locals(&mut order.expr, locals)?;
            }
            bind_returning_locals(&mut delete.returning, locals)?;
        }
        crate::parser::QueryMut::Other(Statement::Call(call)) => {
            for arg in &mut call.args {
                bind_expr_locals(arg, locals)?;
            }
        }
        crate::parser::QueryMut::Other(Statement::Set(assignments)) => {
            for assignment in assignments {
                bind_expr_locals(&mut assignment.value, locals)?;
            }
        }
        crate::parser::QueryMut::Other(Statement::Do(exprs)) => {
            for expr in exprs {
                bind_expr_locals(expr, locals)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn bind_returning_locals(
    returning: &mut Option<crate::parser::ReturningClause>,
    locals: &HashMap<String, Value>,
) -> Result<(), QueryError> {
    if let Some(returning) = returning {
        for item in &mut returning.projection {
            if let crate::parser::SelectItem::Expr { expr, .. } = item {
                bind_expr_locals(expr, locals)?;
            }
        }
    }
    Ok(())
}

fn bind_subquery_locals(
    subquery: &mut crate::parser::SubQuery,
    locals: &HashMap<String, Value>,
) -> Result<(), QueryError> {
    match subquery {
        crate::parser::SubQuery::Select(select) => bind_select_locals(select, locals),
        statement @ (crate::parser::SubQuery::SetOp(_)
        | crate::parser::SubQuery::Rrf(_)
        | crate::parser::SubQuery::WithCte(_)) => bind_statement_locals(statement, locals),
    }
}

fn bind_select_locals(
    select: &mut crate::parser::Select,
    locals: &HashMap<String, Value>,
) -> Result<(), QueryError> {
    use crate::parser::{JsonTableColumnDef, JsonTableOnResponse, SelectItem};
    for item in &mut select.projection {
        if let SelectItem::Expr { expr, alias } = item {
            // Binding a routine local to BoundValue must not erase the
            // implicit result-column label.  Capture it before substitution;
            // explicit aliases remain authoritative.
            if alias.is_none() {
                if let Expr::Identifier(name) = expr {
                    if locals.contains_key(&normalize_local(name)) {
                        *alias = Some(name.clone());
                    }
                }
            }
            bind_expr_locals(expr, locals)?;
        }
    }
    if let Some(derived) = &mut select.derived {
        bind_subquery_locals(derived, locals)?;
    }
    if let Some(values) = &mut select.values {
        for row in &mut values.rows {
            for expr in row {
                bind_expr_locals(expr, locals)?;
            }
        }
    }
    if let Some(json_table) = &mut select.json_table {
        bind_expr_locals(&mut json_table.expr, locals)?;
        fn bind_json_columns(
            columns: &mut [JsonTableColumnDef],
            locals: &HashMap<String, Value>,
        ) -> Result<(), QueryError> {
            for column in columns {
                match column {
                    JsonTableColumnDef::Named {
                        on_empty, on_error, ..
                    } => {
                        if let JsonTableOnResponse::Default(expr) = on_empty {
                            bind_expr_locals(expr, locals)?;
                        }
                        if let JsonTableOnResponse::Default(expr) = on_error {
                            bind_expr_locals(expr, locals)?;
                        }
                    }
                    JsonTableColumnDef::Nested { columns, .. } => {
                        bind_json_columns(columns, locals)?;
                    }
                    JsonTableColumnDef::Ordinality { .. } => {}
                }
            }
            Ok(())
        }
        bind_json_columns(&mut json_table.columns, locals)?;
    }
    for join in &mut select.joins {
        if let Some(condition) = &mut join.condition {
            bind_expr_locals(condition, locals)?;
        }
        if let Some(derived) = &mut join.derived {
            bind_subquery_locals(derived, locals)?;
        }
    }
    if let Some(selection) = &mut select.selection {
        bind_expr_locals(selection, locals)?;
    }
    for expr in &mut select.group_by {
        bind_expr_locals(expr, locals)?;
    }
    if let Some(having) = &mut select.having {
        bind_expr_locals(having, locals)?;
    }
    for order in &mut select.order_by {
        bind_expr_locals(&mut order.expr, locals)?;
    }
    for window in &mut select.named_windows {
        for expr in &mut window.partition_by {
            bind_expr_locals(expr, locals)?;
        }
        for order in &mut window.order_by {
            bind_expr_locals(&mut order.expr, locals)?;
        }
        if let Some(frame) = &mut window.frame {
            bind_window_bound_locals(&mut frame.start, locals)?;
            bind_window_bound_locals(&mut frame.end, locals)?;
        }
    }
    Ok(())
}

fn bind_window_bound_locals(
    bound: &mut crate::parser::WindowFrameBound,
    locals: &HashMap<String, Value>,
) -> Result<(), QueryError> {
    match bound {
        crate::parser::WindowFrameBound::Preceding(expr)
        | crate::parser::WindowFrameBound::Following(expr) => bind_expr_locals(expr, locals),
        _ => Ok(()),
    }
}

fn bind_expr_locals(expr: &mut Expr, locals: &HashMap<String, Value>) -> Result<(), QueryError> {
    match expr {
        Expr::Identifier(name) => {
            if let Some(value) = locals.get(&normalize_local(name)).cloned() {
                check_trigger_column(name, crate::engine::auth::Privilege::Select)?;
                *expr = Expr::BoundValue(value);
            }
        }
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            // B2 — reject OLD in INSERT / NEW in DELETE before it degrades to
            // an unknown-column error in the sub-statement executor.
            check_trigger_row_qualifier(&parts[0])?;
            let key = normalize_local(&format!("{}.{}", parts[0], parts[1]));
            if let Some(value) = locals.get(&key).cloned() {
                check_trigger_column(&key, crate::engine::auth::Privilege::Select)?;
                *expr = Expr::BoundValue(value);
            }
        }
        Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            bind_expr_locals(expr, locals)?;
        }
        Expr::Binary { left, right, .. } => {
            bind_expr_locals(left, locals)?;
            bind_expr_locals(right, locals)?;
        }
        Expr::Like { expr, pattern, .. } => {
            bind_expr_locals(expr, locals)?;
            bind_expr_locals(pattern, locals)?;
        }
        Expr::InList { expr, list, .. } => {
            bind_expr_locals(expr, locals)?;
            for item in list {
                bind_expr_locals(item, locals)?;
            }
        }
        Expr::InSubquery { expr, subquery, .. } => {
            bind_expr_locals(expr, locals)?;
            bind_select_locals(subquery, locals)?;
        }
        Expr::QuantifiedComparison { left, right, .. } => {
            bind_expr_locals(left, locals)?;
            match right {
                crate::parser::QuantifiedComparisonRight::Subquery(subquery) => {
                    bind_select_locals(subquery, locals)?;
                }
                crate::parser::QuantifiedComparisonRight::List(list) => {
                    for item in list {
                        bind_expr_locals(item, locals)?;
                    }
                }
            }
        }
        Expr::Subquery(subquery) | Expr::Exists { subquery, .. } => {
            bind_select_locals(subquery, locals)?;
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            bind_expr_locals(expr, locals)?;
            bind_expr_locals(low, locals)?;
            bind_expr_locals(high, locals)?;
        }
        Expr::Function { args, .. } => {
            for arg in args {
                bind_expr_locals(arg, locals)?;
            }
        }
        Expr::GroupKeyRef { expr, .. } => bind_expr_locals(expr, locals)?,
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(operand) = operand {
                bind_expr_locals(operand, locals)?;
            }
            for cond in conditions {
                bind_expr_locals(cond, locals)?;
            }
            for result in results {
                bind_expr_locals(result, locals)?;
            }
            if let Some(else_result) = else_result {
                bind_expr_locals(else_result, locals)?;
            }
        }
        Expr::WindowFunction { args, spec, .. } => {
            let crate::parser::WindowSpec {
                partition_by,
                order_by,
                frame,
                ..
            } = spec.as_mut();

            for arg in args {
                bind_expr_locals(arg, locals)?;
            }
            for expr in partition_by {
                bind_expr_locals(expr, locals)?;
            }
            for order in order_by {
                bind_expr_locals(&mut order.expr, locals)?;
            }
            if let Some(frame) = frame {
                bind_window_bound_locals(&mut frame.start, locals)?;
                bind_window_bound_locals(&mut frame.end, locals)?;
            }
        }
        Expr::Tuple(items) => {
            for item in items {
                bind_expr_locals(item, locals)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn collect_routine_response(
    frame: &mut RoutineFrame,
    response: QueryResponse,
) -> Result<(), QueryError> {
    match response {
        QueryResponse::ResultSet(result) => {
            if !frame.allow_result_sets {
                return Err(error(
                    crate::error::catalog::ER_SP_NO_RETSET,
                    "Not allowed to return a result set from stored function or trigger",
                ));
            }
            frame.responses.push(QueryResponse::ResultSet(result));
        }
        QueryResponse::SharedResultSet(result) => {
            if !frame.allow_result_sets {
                return Err(error(
                    crate::error::catalog::ER_SP_NO_RETSET,
                    "Not allowed to return a result set from stored function or trigger",
                ));
            }
            frame.responses.push(QueryResponse::SharedResultSet(result));
        }
        QueryResponse::StreamingResultSet(result) => {
            if !frame.allow_result_sets {
                return Err(error(
                    crate::error::catalog::ER_SP_NO_RETSET,
                    "Not allowed to return a result set from stored function or trigger",
                ));
            }
            frame
                .responses
                .push(QueryResponse::StreamingResultSet(result));
        }
        QueryResponse::MultiResult(results) => {
            for result in results {
                collect_routine_response(frame, result)?;
            }
        }
        QueryResponse::Ok | QueryResponse::OkAffected { .. } => {}
    }
    Ok(())
}

/// MySQL HANDLER matching selects the *most specific* handler, independent
/// of declaration order: a handler keyed on a concrete MySQL error number
/// (`FOR 1062`) beats one keyed on a `SQLSTATE` value, which in turn beats
/// the general condition classes (`SQLEXCEPTION` / `SQLWARNING` / `NOT
/// FOUND`). Named conditions (`DECLARE c CONDITION FOR ...`) inherit the
/// specificity of what they resolve to. See MySQL 8.0 reference,
/// "Condition Handling / Scope Rules for Handlers".
fn handler_specificity(frame: &RoutineFrame, declared: &RoutineCondition) -> u8 {
    let resolved = match declared {
        RoutineCondition::Named(name) => frame
            .conditions
            .get(&normalize_local(name))
            .unwrap_or(declared),
        other => other,
    };
    match resolved {
        RoutineCondition::MysqlError(_) => 3,
        RoutineCondition::SqlState(_) => 2,
        RoutineCondition::SqlException
        | RoutineCondition::SqlWarning
        | RoutineCondition::NotFound => 1,
        RoutineCondition::Named(_) => 0,
    }
}

fn find_matching_handler(
    frame: &RoutineFrame,
    handlers: &[HandlerDecl],
    err: &RoutineExecError,
) -> Option<HandlerDecl> {
    let mut best: Option<(u8, &HandlerDecl)> = None;
    for handler in handlers {
        // A handler may list several conditions; its effective specificity
        // for this error is the most specific of the conditions that match.
        let mut score = 0u8;
        for condition in &handler.conditions {
            if condition_matches(frame, condition, err) {
                score = score.max(handler_specificity(frame, condition));
            }
        }
        if score == 0 {
            continue;
        }
        // Strictly-more-specific wins; equal specificity keeps the earlier
        // declaration (MySQL rejects genuine duplicates at CREATE time, so
        // ties only arise for the general classes, which never overlap).
        if best.map_or(true, |(best_score, _)| score > best_score) {
            best = Some((score, handler));
        }
    }
    best.map(|(_, handler)| handler.clone())
}

fn condition_matches(
    frame: &RoutineFrame,
    declared: &RoutineCondition,
    err: &RoutineExecError,
) -> bool {
    let resolved = match declared {
        RoutineCondition::Named(name) => frame
            .conditions
            .get(&normalize_local(name))
            .unwrap_or(declared),
        other => other,
    };
    if let RoutineExecError::Condition(actual, _) = err {
        if routine_condition_eq(resolved, actual) {
            return true;
        }
    }
    let query = err.query_error();
    match resolved {
        RoutineCondition::SqlException => {
            !query.sql_state().starts_with("01") && !query.sql_state().starts_with("02")
        }
        RoutineCondition::SqlWarning => query.sql_state().starts_with("01"),
        RoutineCondition::NotFound => query.sql_state().starts_with("02"),
        RoutineCondition::MysqlError(code) => query.code() == *code,
        RoutineCondition::SqlState(state) => query.sql_state().eq_ignore_ascii_case(state),
        RoutineCondition::Named(_) => false,
    }
}

fn routine_condition_eq(left: &RoutineCondition, right: &RoutineCondition) -> bool {
    match (left, right) {
        (RoutineCondition::SqlException, RoutineCondition::SqlException)
        | (RoutineCondition::SqlWarning, RoutineCondition::SqlWarning)
        | (RoutineCondition::NotFound, RoutineCondition::NotFound) => true,
        (RoutineCondition::MysqlError(a), RoutineCondition::MysqlError(b)) => a == b,
        (RoutineCondition::SqlState(a), RoutineCondition::SqlState(b)) => a.eq_ignore_ascii_case(b),
        (RoutineCondition::Named(a), RoutineCondition::Named(b)) => a.eq_ignore_ascii_case(b),
        _ => false,
    }
}

fn condition_from_query_error(err: &QueryError) -> RoutineCondition {
    if err.sql_state().starts_with("02") {
        RoutineCondition::NotFound
    } else if err.sql_state().starts_with("01") {
        RoutineCondition::SqlWarning
    } else {
        RoutineCondition::SqlException
    }
}

fn not_found_condition() -> RoutineExecError {
    RoutineExecError::Condition(
        RoutineCondition::NotFound,
        error(crate::error::catalog::ER_SP_FETCH_NO_DATA, "No data"),
    )
}

#[allow(clippy::too_many_arguments)]
fn assign_target(
    session: &mut SessionContext,
    locals: &mut HashMap<String, Value>,
    local_types: &HashMap<String, (DataTypeKind, bool)>,
    trigger_new_keys: &BTreeSet<String>,
    trigger_pending_auto_increment: &mut Option<String>,
    trigger_generated_new_keys: &BTreeSet<String>,
    trigger_old_keys: &BTreeSet<String>,
    trigger_new_writable: bool,
    target: &RoutineTarget,
    mut value: Value,
) -> Result<(), RoutineExecError> {
    match target {
        RoutineTarget::Local(name) => {
            let key = normalize_local(name);
            // B2 — SET OLD.x in an INSERT trigger / SET NEW.x in a DELETE
            // trigger references a pseudo-row that does not exist → 1363.
            if let Some((qualifier, _)) = name.split_once('.') {
                check_trigger_row_qualifier(qualifier).map_err(RoutineExecError::from)?;
            }
            // B4 — a generated NEW column is read-only; assigning it is 3105
            // (checked before the writable path so it is not silently stored).
            if trigger_generated_new_keys.contains(&key) {
                let column = name.rsplit('.').next().unwrap_or(name.as_str());
                return Err(error(
                    crate::error::catalog::ER_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN,
                    format!("The value specified for generated column '{column}' is not allowed"),
                )
                .into());
            }
            if trigger_old_keys.contains(&key) {
                return Err(error(
                    crate::error::catalog::ER_TRG_CANT_CHANGE_ROW,
                    "Updating of OLD row is not allowed in trigger",
                )
                .into());
            }
            if trigger_new_keys.contains(&key) && !trigger_new_writable {
                return Err(error(
                    crate::error::catalog::ER_TRG_CANT_CHANGE_ROW,
                    "Updating of NEW row is not allowed in this trigger",
                )
                .into());
            }
            if !locals.contains_key(&key) {
                return Err(error(
                    crate::error::catalog::ER_SP_UNDECLARED_VAR,
                    format!("Undeclared variable {name}"),
                )
                .into());
            }
            if trigger_new_keys.contains(&key) {
                check_trigger_column(&key, crate::engine::auth::Privilege::Update)?;
            }
            if let Some((data_type, unsigned)) = local_types.get(&key) {
                value = coerce_routine_value(value, name, data_type, *unsigned)?;
            }
            if trigger_pending_auto_increment.as_ref() == Some(&key) {
                *trigger_pending_auto_increment = None;
            }
            locals.insert(key, value);
        }
        RoutineTarget::UserVariable(name) => session.set_user_variable(name, value),
    }
    Ok(())
}

fn coerce_routine_value(
    value: Value,
    name: &str,
    data_type: &DataTypeKind,
    unsigned: bool,
) -> Result<Value, QueryError> {
    crate::storage::coerce_routine_typed_value(value, name, data_type, unsigned)
}

/// Preserve declared stored-program numeric scale when a local variable is
/// consumed directly by a string-building function. Runtime `Value::Double`
/// deliberately carries no display metadata, while MySQL's routine local
/// slot retains the `DOUBLE(M,D)` descriptor: `CONCAT(v)` must therefore
/// render `100.000` for a `DOUBLE(10,3)` local even though ordinary
/// `CONCAT(100.0)` may render `100`.
///
/// The rewrite is type-driven and limited to the string-conversion boundary;
/// the value remains a floating-point value for arithmetic and comparisons.
fn bind_routine_string_function_types(
    expr: &mut Expr,
    locals: &HashMap<String, Value>,
    local_types: &HashMap<String, (DataTypeKind, bool)>,
) {
    fn format_direct_local(
        expr: &mut Expr,
        locals: &HashMap<String, Value>,
        local_types: &HashMap<String, (DataTypeKind, bool)>,
    ) {
        let name = match expr {
            Expr::Identifier(name) => name,
            Expr::CompoundIdentifier(parts) if parts.len() == 1 => &parts[0],
            _ => return,
        };
        let key = normalize_local(name);
        let Some((data_type, _)) = local_types.get(&key) else {
            return;
        };
        let scale = match data_type {
            DataTypeKind::Float {
                scale: Some(scale), ..
            }
            | DataTypeKind::Double {
                scale: Some(scale), ..
            } => *scale as usize,
            _ => return,
        };
        let Some(value) = locals.get(&key) else {
            return;
        };
        let rendered = match value {
            Value::Float(value) => format!("{value:.scale$}"),
            Value::Double(value) => format!("{value:.scale$}"),
            Value::Null => return,
            _ => return,
        };
        *expr = Expr::BoundValue(Value::String(rendered));
    }

    match expr {
        Expr::Function { name, args, .. }
            if name.eq_ignore_ascii_case("CONCAT") || name.eq_ignore_ascii_case("CONCAT_WS") =>
        {
            for arg in args {
                format_direct_local(arg, locals, local_types);
                bind_routine_string_function_types(arg, locals, local_types);
            }
        }
        Expr::Function { args, .. } | Expr::Tuple(args) => {
            for arg in args {
                bind_routine_string_function_types(arg, locals, local_types);
            }
        }
        Expr::Unary { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::GroupKeyRef { expr, .. } => {
            bind_routine_string_function_types(expr, locals, local_types);
        }
        Expr::Binary { left, right, .. }
        | Expr::Like {
            expr: left,
            pattern: right,
            ..
        } => {
            bind_routine_string_function_types(left, locals, local_types);
            bind_routine_string_function_types(right, locals, local_types);
        }
        Expr::InList { expr, list, .. } => {
            bind_routine_string_function_types(expr, locals, local_types);
            for item in list {
                bind_routine_string_function_types(item, locals, local_types);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            bind_routine_string_function_types(expr, locals, local_types);
            bind_routine_string_function_types(low, locals, local_types);
            bind_routine_string_function_types(high, locals, local_types);
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(operand) = operand {
                bind_routine_string_function_types(operand, locals, local_types);
            }
            for item in conditions.iter_mut().chain(results.iter_mut()) {
                bind_routine_string_function_types(item, locals, local_types);
            }
            if let Some(else_result) = else_result {
                bind_routine_string_function_types(else_result, locals, local_types);
            }
        }
        _ => {}
    }
}

fn read_target(
    session: &SessionContext,
    locals: &HashMap<String, Value>,
    target: &RoutineTarget,
) -> Result<Value, QueryError> {
    match target {
        RoutineTarget::Local(name) => {
            locals.get(&normalize_local(name)).cloned().ok_or_else(|| {
                error(
                    crate::error::catalog::ER_SP_UNDECLARED_VAR,
                    format!("Undeclared variable {name}"),
                )
            })
        }
        RoutineTarget::UserVariable(name) => {
            Ok(session.user_variable(name).cloned().unwrap_or(Value::Null))
        }
    }
}

fn eval_routine_expr(
    session: &SessionContext,
    locals: &HashMap<String, Value>,
    expr: &Expr,
) -> Result<Value, QueryError> {
    // Keep the ordinary expression engine as the semantic oracle. Routine
    // locals are the only extra namespace: bind them once, then reuse the
    // shared evaluator for every expression that does not require the
    // executor's contextual-subquery callback. The recursive implementation
    // below remains the narrow subquery bridge and correctness fallback.
    if !routine_expr_contains_subquery(expr) {
        let mut bound = expr.clone();
        bind_expr_locals(&mut bound, locals)?;
        match crate::engine::expr::eval_const_runtime(&bound) {
            Err(err) if err.is(crate::error::catalog::ER_BAD_FIELD_ERROR) => {}
            result => return result,
        }
    }
    match expr {
        Expr::Identifier(name) => locals.get(&normalize_local(name)).cloned().ok_or_else(|| {
            error(
                crate::error::catalog::ER_SP_UNDECLARED_VAR,
                format!("Undeclared variable {name}"),
            )
        }),
        Expr::CompoundIdentifier(parts) if parts.len() == 1 => locals
            .get(&normalize_local(&parts[0]))
            .cloned()
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_SP_UNDECLARED_VAR,
                    format!("Undeclared variable {}", parts[0]),
                )
            }),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            check_trigger_row_qualifier(&parts[0])?;
            let key = normalize_local(&format!("{}.{}", parts[0], parts[1]));
            check_trigger_column(&key, crate::engine::auth::Privilege::Select)?;
            locals.get(&key).cloned().ok_or_else(|| {
                error(
                    crate::error::catalog::ER_SP_UNDECLARED_VAR,
                    format!("Undeclared variable {key}"),
                )
            })
        }
        Expr::UserVariable(name) => Ok(session.user_variable(name).cloned().unwrap_or(Value::Null)),
        Expr::SystemVariable { scope, name } => eval_system_variable(*scope, name, None),
        Expr::Literal(lit) => Ok(literal_value(lit)),
        Expr::BoundValue(value) => Ok(value.clone()),
        Expr::Placeholder(idx) => params::current_param(*idx).ok_or_else(|| {
            error(
                crate::error::catalog::ER_WRONG_ARGUMENTS,
                "parameter is not bound",
            )
        }),
        Expr::Unary {
            op: UnaryOp::Neg,
            expr,
        } => {
            let value = eval_routine_expr(session, locals, expr)?;
            crate::engine::expr::duplicate_negated_int_div_zero_warning(expr, &value);
            eval_negate(value)
        }
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => Ok(crate::engine::expr::eval_logical_not(&eval_routine_expr(
            session, locals, expr,
        )?)),
        Expr::Unary {
            op: UnaryOp::BitNot,
            expr,
        } => eval_bit_not(eval_routine_expr(session, locals, expr)?),
        Expr::Binary { left, op, right } => {
            match op {
                BinaryOp::And => {
                    let l = crate::engine::expr::logical_value(&eval_routine_expr(
                        session, locals, left,
                    )?);
                    if l == Some(false) {
                        return Ok(Value::Int(0));
                    }
                    let r = crate::engine::expr::logical_value(&eval_routine_expr(
                        session, locals, right,
                    )?);
                    return Ok(crate::engine::expr::combine_logical(BinaryOp::And, l, r));
                }
                BinaryOp::Or => {
                    let l = crate::engine::expr::logical_value(&eval_routine_expr(
                        session, locals, left,
                    )?);
                    if l == Some(true) {
                        return Ok(Value::Int(1));
                    }
                    let r = crate::engine::expr::logical_value(&eval_routine_expr(
                        session, locals, right,
                    )?);
                    return Ok(crate::engine::expr::combine_logical(BinaryOp::Or, l, r));
                }
                BinaryOp::Concat => {
                    let l = eval_routine_expr(session, locals, left)?;
                    let r = eval_routine_expr(session, locals, right)?;
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
                _ => {}
            }
            let l = eval_routine_expr(session, locals, left)?;
            let r = eval_routine_expr(session, locals, right)?;
            match op {
                BinaryOp::And | BinaryOp::Or | BinaryOp::Concat => unreachable!(),
                BinaryOp::Xor => eval_logical_xor(&l, &r),
                BinaryOp::Eq
                | BinaryOp::NullSafeEq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq => null_cmp_binary(&l, *op, &r),
                BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
                    eval_arithmetic(&l, *op, &r)
                }
                BinaryOp::IntDiv => {
                    crate::engine::expr::eval_int_div_expr_values(left, right, &l, &r)
                }
                BinaryOp::BitAnd
                | BinaryOp::BitOr
                | BinaryOp::BitXor
                | BinaryOp::ShiftLeft
                | BinaryOp::ShiftRight => eval_bitwise(l, *op, r),
                BinaryOp::Arrow | BinaryOp::LongArrow => {
                    eval_json_arrow(l, r, *op == BinaryOp::LongArrow)
                }
            }
        }
        Expr::Like {
            expr,
            pattern,
            negated,
            escape_char,
        } => {
            // Route through the shared LIKE evaluator so NULL operands yield
            // NULL (three-valued logic) and binary/collation-aware matching
            // stays consistent with the SELECT path (issue: routine 3VL gap).
            let value = eval_routine_expr(session, locals, expr)?;
            let pat = eval_routine_expr(session, locals, pattern)?;
            Ok(crate::engine::expr::eval_like_values(
                &value,
                &pat,
                *negated,
                *escape_char,
            ))
        }
        Expr::IsNull(inner) => {
            let v = eval_routine_expr(session, locals, inner)?;
            Ok(Value::Int(matches!(v, Value::Null) as i64))
        }
        Expr::IsNotNull(inner) => {
            let v = eval_routine_expr(session, locals, inner)?;
            Ok(Value::Int(!matches!(v, Value::Null) as i64))
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let v = eval_routine_expr(session, locals, expr)?;
            let lo = eval_routine_expr(session, locals, low)?;
            let hi = eval_routine_expr(session, locals, high)?;
            crate::engine::expr::eval_between(&v, &lo, &hi, *negated)
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let v = eval_routine_expr(session, locals, expr)?;
            let mut iter = list.iter();
            crate::engine::expr::eval_in_list(v, *negated, || {
                iter.next()
                    .map(|item| eval_routine_expr(session, locals, item))
                    .transpose()
            })
        }
        Expr::InSubquery { .. }
        | Expr::Subquery(_)
        | Expr::Exists { .. }
        | Expr::QuantifiedComparison {
            right: crate::parser::QuantifiedComparisonRight::Subquery(_),
            ..
        } => {
            let mut bound = expr.clone();
            bind_expr_locals(&mut bound, locals)?;
            super::select::eval::eval_contextual_subquery_expr(session, &mut bound)
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => eval_case(operand, conditions, results, else_result, |expr| {
            eval_routine_expr(session, locals, expr)
        }),
        Expr::Function { name, args, .. } => {
            eval_function(name, args, |arg| eval_routine_expr(session, locals, arg))
        }
        other => Err(error(
            crate::error::catalog::ER_BAD_FIELD_ERROR,
            format!("expression is not supported in stored routine: {other:?}"),
        )),
    }
}

fn routine_expr_contains_subquery(expr: &Expr) -> bool {
    crate::parser::visit::value_expr_contains_subquery(expr)
}

fn signal_message_value(
    session: &SessionContext,
    locals: &HashMap<String, Value>,
    expr: Option<&Expr>,
) -> Result<Option<String>, QueryError> {
    expr.map(|expr| {
        let value = eval_routine_expr(session, locals, expr)?;
        if matches!(value, Value::Null) {
            return Err(error(
                crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                "MESSAGE_TEXT cannot be NULL",
            ));
        }
        Ok(crate::storage::value_to_string(&value))
    })
    .transpose()
}

fn signal_errno_value(
    session: &SessionContext,
    locals: &HashMap<String, Value>,
    expr: Option<&Expr>,
) -> Result<Option<u16>, QueryError> {
    expr.map(|expr| {
        let value = eval_routine_expr(session, locals, expr)?;
        let number = crate::storage::to_i64(value)?;
        u16::try_from(number)
            .ok()
            .filter(|number| *number != 0)
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                    "MYSQL_ERRNO must be between 1 and 65535",
                )
            })
    })
    .transpose()
}

fn label_matches(label: &Option<String>, target: &str) -> bool {
    label
        .as_ref()
        .is_some_and(|label| label.eq_ignore_ascii_case(target))
}

fn normalize_local(name: &str) -> String {
    name.trim_matches('`').to_ascii_lowercase()
}

fn resolve_statement_functions<'q>(
    db: &Arc<Database>,
    session: &mut SessionContext,
    statement: impl Into<crate::parser::QueryMut<'q>>,
    registry: &SessionRegistry,
    needs: &mut StoredFunctionResolution,
) -> Result<(), QueryError> {
    let statement = statement.into();
    use crate::parser::{InsertSource, Statement};
    match statement {
        crate::parser::QueryMut::Select(select) => {
            resolve_select_functions(db, session, select, registry, needs)?;
        }
        crate::parser::QueryMut::Rrf(rrf) => {
            for branch in &mut rrf.branches {
                resolve_statement_functions(db, session, &mut branch.query, registry, needs)?;
            }
        }
        crate::parser::QueryMut::SetOperation(set) => {
            resolve_statement_functions(db, session, &mut set.left, registry, needs)?;
            resolve_statement_functions(db, session, &mut set.right, registry, needs)?;
        }
        crate::parser::QueryMut::WithCte(mut cte) => {
            for def in cte.ctes.iter_mut() {
                resolve_statement_functions(db, session, &mut def.query, registry, needs)?;
            }
            resolve_statement_functions(db, session, &mut cte.body, registry, needs)?
        }
        crate::parser::QueryMut::Other(Statement::Explain { target, .. }) => {
            resolve_statement_functions(db, session, target, registry, needs)?
        }
        crate::parser::QueryMut::Other(Statement::SelectIntoVars { inner: target, .. })
        | crate::parser::QueryMut::Other(Statement::SelectIntoFile { inner: target, .. }) => {
            resolve_statement_functions(db, session, target, registry, needs)?
        }
        crate::parser::QueryMut::Other(Statement::Do(expressions)) => {
            for expr in expressions {
                resolve_expr_functions(db, session, expr, registry, needs)?;
            }
        }
        crate::parser::QueryMut::Other(Statement::Set(assignments)) => {
            for assignment in assignments {
                resolve_expr_functions(db, session, &mut assignment.value, registry, needs)?;
            }
        }
        crate::parser::QueryMut::Other(Statement::CreateTableAsSelect { query, .. }) => {
            resolve_statement_functions(db, session, query, registry, needs)?
        }
        crate::parser::QueryMut::Other(Statement::Insert(insert)) => {
            match &mut insert.source {
                InsertSource::Values(rows) => {
                    for row in rows {
                        for expr in row {
                            resolve_expr_functions(db, session, expr, registry, needs)?;
                        }
                    }
                }
                InsertSource::Query(select) => {
                    resolve_statement_functions(db, session, select, registry, needs)?;
                }
            }
            if let Some(assignments) = &mut insert.on_duplicate_key_update {
                for assignment in assignments {
                    resolve_expr_functions(db, session, &mut assignment.value, registry, needs)?;
                }
            }
        }
        crate::parser::QueryMut::Other(Statement::Update(update)) => {
            for assignment in &mut update.assignments {
                resolve_expr_functions(db, session, &mut assignment.value, registry, needs)?;
            }
            if let Some(selection) = &mut update.selection {
                resolve_expr_functions(db, session, selection, registry, needs)?;
            }
            for join in &mut update.joins {
                if let Some(condition) = &mut join.condition {
                    resolve_expr_functions(db, session, condition, registry, needs)?;
                }
            }
            for order in &mut update.order_by {
                resolve_expr_functions(db, session, &mut order.expr, registry, needs)?;
            }
        }
        crate::parser::QueryMut::Other(Statement::Delete(delete)) => {
            if let Some(selection) = &mut delete.selection {
                resolve_expr_functions(db, session, selection, registry, needs)?;
            }
            for join in &mut delete.joins {
                if let Some(condition) = &mut join.condition {
                    resolve_expr_functions(db, session, condition, registry, needs)?;
                }
            }
            for order in &mut delete.order_by {
                resolve_expr_functions(db, session, &mut order.expr, registry, needs)?;
            }
        }
        crate::parser::QueryMut::Other(Statement::Call(call)) => {
            for arg in &mut call.args {
                resolve_expr_functions(db, session, arg, registry, needs)?;
            }
            if let Some(routine) = db.routine_definition(
                &session.current_database,
                RoutineKindSchema::Procedure,
                &call.name,
            ) {
                collect_static_routine_calls(db, &routine, needs, &mut BTreeSet::new(), false)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn resolve_select_functions(
    db: &Arc<Database>,
    session: &mut SessionContext,
    select: &mut crate::parser::Select,
    registry: &SessionRegistry,
    needs: &mut StoredFunctionResolution,
) -> Result<(), QueryError> {
    use crate::parser::SelectItem;
    for item in &mut select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            resolve_expr_functions(db, session, expr, registry, needs)?;
        }
    }
    if let Some(selection) = &mut select.selection {
        resolve_expr_functions(db, session, selection, registry, needs)?;
    }
    for expr in &mut select.group_by {
        resolve_expr_functions(db, session, expr, registry, needs)?;
    }
    if let Some(having) = &mut select.having {
        resolve_expr_functions(db, session, having, registry, needs)?;
    }
    for order in &mut select.order_by {
        resolve_expr_functions(db, session, &mut order.expr, registry, needs)?;
    }
    if let Some(derived) = &mut select.derived {
        resolve_subquery_functions(db, session, derived, registry, needs)?;
    }
    for join in &mut select.joins {
        if let Some(condition) = &mut join.condition {
            resolve_expr_functions(db, session, condition, registry, needs)?;
        }
        if let Some(derived) = &mut join.derived {
            resolve_subquery_functions(db, session, derived, registry, needs)?;
        }
    }
    Ok(())
}

fn resolve_subquery_functions(
    db: &Arc<Database>,
    session: &mut SessionContext,
    subquery: &mut crate::parser::SubQuery,
    registry: &SessionRegistry,
    needs: &mut StoredFunctionResolution,
) -> Result<(), QueryError> {
    use crate::parser::SubQuery;
    match subquery {
        SubQuery::Select(select) => resolve_select_functions(db, session, select, registry, needs),
        stmt @ (SubQuery::SetOp(_) | SubQuery::Rrf(_) | SubQuery::WithCte(_)) => {
            resolve_statement_functions(db, session, stmt, registry, needs)
        }
    }
}

fn resolve_expr_functions(
    db: &Arc<Database>,
    session: &mut SessionContext,
    expr: &mut Expr,
    registry: &SessionRegistry,
    needs: &mut StoredFunctionResolution,
) -> Result<(), QueryError> {
    match expr {
        Expr::Function { name, args, .. } => {
            let function_name = name.clone();
            for arg in args.iter_mut() {
                resolve_expr_functions(db, session, arg, registry, needs)?;
            }
            if !function_name.contains('.')
                && (crate::engine::cbo::function_cost::is_known_builtin_function(&function_name)
                    || crate::planner::is_aggregate_function(&function_name))
            {
                return Ok(());
            }
            let Some(routine) = db.routine_definition(
                &session.current_database,
                RoutineKindSchema::Function,
                &function_name,
            ) else {
                return Ok(());
            };
            authorize_routine_execute(db, session, &routine)?;
            let program = compile_routine(&routine)?;
            crate::engine::expr::register_resolved_stored_function(
                &function_name,
                routine.clone(),
                program,
            );
            needs.routines.insert(ResolvedRoutineMdl {
                database: routine.database.clone(),
                name: routine.name.clone(),
                kind: "function",
            });
            collect_static_routine_calls(db, &routine, needs, &mut BTreeSet::new(), true)?;
            if routine.params.len() != args.len() {
                return Err(error(
                    crate::error::catalog::ER_SP_WRONG_NO_OF_ARGS,
                    format!(
                        "Incorrect number of arguments for FUNCTION {}; expected {}, got {}",
                        routine.name,
                        routine.params.len(),
                        args.len()
                    ),
                ));
            }
            // User declarations such as DETERMINISTIC and NO SQL are not a
            // proof of purity. Until the routine compiler can prove the body
            // has no volatile/session/SQL effects, keep every stored function
            // as a row-by-row call. This also ensures privilege checks and
            // CREATE-time execution context cannot be optimized away.
            needs.needs_serial = true;
        }
        Expr::InSubquery { expr, subquery, .. } => {
            resolve_expr_functions(db, session, expr, registry, needs)?;
            resolve_select_functions(db, session, subquery, registry, needs)?;
        }
        Expr::Subquery(select)
        | Expr::Exists {
            subquery: select, ..
        } => {
            resolve_select_functions(db, session, select, registry, needs)?;
        }
        Expr::QuantifiedComparison {
            left,
            right: crate::parser::QuantifiedComparisonRight::Subquery(select),
            ..
        } => {
            resolve_expr_functions(db, session, left, registry, needs)?;
            resolve_select_functions(db, session, select, registry, needs)?;
        }
        _ => crate::parser::visit::try_for_each_expr_child_mut(expr, |child| {
            resolve_expr_functions(db, session, child, registry, needs)
        })?,
    }
    Ok(())
}

fn collect_static_routine_calls(
    db: &Arc<Database>,
    routine: &RoutineSchema,
    needs: &mut StoredFunctionResolution,
    visited: &mut BTreeSet<(String, String, String)>,
    restricted_by_function: bool,
) -> Result<(), QueryError> {
    let kind = routine.kind.lower().to_string();
    let visit_key = (
        routine.database.to_ascii_lowercase(),
        kind.clone(),
        routine.name.to_ascii_lowercase(),
    );
    if !visited.insert(visit_key) {
        return Ok(());
    }
    needs.routines.insert(ResolvedRoutineMdl {
        database: routine.database.clone(),
        name: routine.name.clone(),
        kind: if routine.kind == RoutineKindSchema::Function {
            "function"
        } else {
            "procedure"
        },
    });
    let program = compile_routine(routine)?;
    for table_use in super::dispatch::collect_routine_table_uses(&program.body.statements) {
        let (database, table) = split_table_name(&routine.database, &table_use.table);
        needs
            .tables
            .entry((database.clone(), table.clone()))
            .and_modify(|write| *write |= table_use.write)
            .or_insert(table_use.write);
        if restricted_by_function {
            needs
                .restricted_tables
                .entry((database, table))
                .and_modify(|write| *write |= table_use.write)
                .or_insert(table_use.write);
        }
    }
    let mut function_names =
        crate::parser::collect_routine_expression_function_names(&program.body.statements);
    for statement in program.static_statements.values() {
        function_names.extend(crate::parser::collect_statement_function_names(
            &statement.statement,
        ));
    }
    function_names.sort_unstable();
    function_names.dedup();
    for function_name in function_names {
        if !function_name.contains('.')
            && (crate::engine::cbo::function_cost::is_known_builtin_function(&function_name)
                || crate::planner::is_aggregate_function(&function_name))
        {
            continue;
        }
        if let Some(callee) = db.routine_definition(
            &routine.database,
            RoutineKindSchema::Function,
            &function_name,
        ) {
            collect_static_routine_calls(db, &callee, needs, visited, restricted_by_function)?;
        }
    }
    collect_static_calls_from_statements(
        db,
        &routine.database,
        &program.body.statements,
        needs,
        visited,
        restricted_by_function,
    )
}

fn collect_static_calls_from_statements(
    db: &Arc<Database>,
    database: &str,
    statements: &[RoutineStmt],
    needs: &mut StoredFunctionResolution,
    visited: &mut BTreeSet<(String, String, String)>,
    restricted_by_function: bool,
) -> Result<(), QueryError> {
    for statement in statements {
        match statement {
            RoutineStmt::Call(call) => {
                if let Some(callee) =
                    db.routine_definition(database, RoutineKindSchema::Procedure, &call.name)
                {
                    collect_static_routine_calls(
                        db,
                        &callee,
                        needs,
                        visited,
                        restricted_by_function,
                    )?;
                }
            }
            RoutineStmt::Block { statements, .. }
            | RoutineStmt::Loop { statements, .. }
            | RoutineStmt::While { statements, .. }
            | RoutineStmt::Repeat { statements, .. } => {
                collect_static_calls_from_statements(
                    db,
                    database,
                    statements,
                    needs,
                    visited,
                    restricted_by_function,
                )?;
            }
            RoutineStmt::If {
                branches,
                else_branch,
            }
            | RoutineStmt::Case {
                branches,
                else_branch,
                ..
            } => {
                for (_, branch) in branches {
                    collect_static_calls_from_statements(
                        db,
                        database,
                        branch,
                        needs,
                        visited,
                        restricted_by_function,
                    )?;
                }
                collect_static_calls_from_statements(
                    db,
                    database,
                    else_branch,
                    needs,
                    visited,
                    restricted_by_function,
                )?;
            }
            RoutineStmt::DeclareHandler { statement, .. } => {
                collect_static_calls_from_statements(
                    db,
                    database,
                    std::slice::from_ref(statement.as_ref()),
                    needs,
                    visited,
                    restricted_by_function,
                )?;
            }
            _ => {}
        }
    }
    Ok(())
}
