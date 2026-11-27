//! Property graph execution on the transactional storage engine.

use super::*;
use crate::engine::auth::{Privilege, PrivilegeScope, RequiredPrivilege};
use crate::graph::{Cast, Clause, Command, Expression, Program};
use crate::storage::table_key;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

mod matching;
mod storage;
mod values;

#[derive(Clone, Debug)]
pub(crate) struct GraphRef {
    database: String,
    table: String,
    id: u64,
}

fn invalid(message: impl Into<String>) -> QueryError {
    crate::error::catalog::ER_WRONG_ARGUMENTS.error(message)
}

struct GraphReadWorker {
    _permit: tokio::sync::SemaphorePermit<'static>,
    _stack: super::mem_budget::ControlledMemoryCharge,
    _activity: GraphReadActivity,
}

static GRAPH_READ_ACTIVITY: AtomicUsize = AtomicUsize::new(0);

struct GraphReadActivity;

impl GraphReadActivity {
    fn enter() -> Self {
        GRAPH_READ_ACTIVITY.fetch_add(1, AtomicOrdering::Relaxed);
        Self
    }

    fn try_helper() -> Option<Self> {
        let capacity = crate::planner::cpu_bound_parallel_degree(u32::MAX) as usize;
        if crate::engine::memory_reclaimer::active_statement_count() >= capacity {
            return None;
        }
        GRAPH_READ_ACTIVITY
            .fetch_update(AtomicOrdering::Relaxed, AtomicOrdering::Relaxed, |active| {
                (active < capacity).then_some(active + 1)
            })
            .ok()?;
        Some(Self)
    }
}

impl Drop for GraphReadActivity {
    fn drop(&mut self) {
        GRAPH_READ_ACTIVITY.fetch_sub(1, AtomicOrdering::Relaxed);
    }
}

fn try_graph_read_worker(txn: TxnView) -> Option<GraphReadWorker> {
    if txn.txn_id != 0
        || crate::planner::cpu_bound_parallel_degree(2) < 2
        || super::mem_budget::current_budget().is_none()
    {
        return None;
    }
    // Count foreground queries and outstanding helpers together so a busy
    // graph does not create another operating-system thread for each phase.
    let activity = GraphReadActivity::try_helper()?;
    let permit = crate::util::exec_permit::global()?.try_acquire().ok()?;
    let stack = super::mem_budget::ControlledMemoryCharge::try_new_optional(
        super::mem_budget::ControlledMemoryCategory::StorageScratch,
        2 * 1024 * 1024,
    )?;
    Some(GraphReadWorker {
        _permit: permit,
        _stack: stack,
        _activity: activity,
    })
}

fn missing_graph() -> QueryError {
    crate::error::catalog::ER_NO_SUCH_TABLE.error("Selected property graph does not exist")
}

fn internal_name(graph: &str) -> String {
    format!(
        "{}{}",
        crate::graph::TABLE_PREFIX,
        graph.to_ascii_lowercase()
    )
}

fn target(session: &SessionContext, command: &Command) -> Option<(String, String)> {
    match command {
        Command::Create { graph, .. }
        | Command::Drop { graph, .. }
        | Command::CreateIndex { graph, .. }
        | Command::DropIndex { graph, .. }
        | Command::SetGraph(graph) => {
            Some((session.current_database.clone(), internal_name(graph)))
        }
        Command::Program(_) => session
            .current_graph
            .as_ref()
            .map(|g| (g.database.clone(), g.table.clone())),
        Command::ResetGraph => None,
    }
}

pub(super) fn mdl_requests(session: &SessionContext, command: &Command) -> Vec<MdlRequest> {
    let Some((database, table)) = target(session, command) else {
        return Vec::new();
    };
    let lock_type = if command.ddl() {
        MdlLockType::Exclusive
    } else if command.writes() {
        MdlLockType::SharedWrite
    } else {
        MdlLockType::SharedRead
    };
    let mut requests = vec![MdlRequest {
        key: MdlObjectKey::table(&database, &table),
        lock_type,
    }];
    if command.ddl() {
        requests.push(MdlRequest {
            key: MdlObjectKey::schema(&database),
            lock_type: MdlLockType::IntentionExclusive,
        });
    }
    requests
}

pub(super) fn privileges(session: &SessionContext, command: &Command) -> Vec<RequiredPrivilege> {
    let database =
        target(session, command).map_or_else(|| session.current_database.clone(), |(db, _)| db);
    let mut privileges = Vec::new();
    match command {
        Command::Create { .. } => privileges.push(Privilege::Create),
        Command::Drop { .. } => privileges.push(Privilege::Drop),
        Command::SetGraph(_) => privileges.push(Privilege::Select),
        Command::ResetGraph => (),
        Command::CreateIndex { .. } | Command::DropIndex { .. } => {
            privileges.push(Privilege::Index)
        }
        Command::Program(program) => {
            for clause in &program.clauses {
                let privilege = match clause {
                    Clause::Match { .. } => Privilege::Select,
                    Clause::Insert(_) => Privilege::Insert,
                    Clause::Set(_) | Clause::Remove(_) => Privilege::Update,
                    Clause::Delete { .. } => Privilege::Delete,
                    Clause::For(..) => continue,
                };
                if !privileges.contains(&privilege) {
                    privileges.push(privilege);
                }
            }
            if privileges.is_empty() {
                privileges.push(Privilege::Select);
            }
        }
    }
    privileges
        .into_iter()
        .map(|privilege| RequiredPrivilege {
            privilege,
            scope: PrivilegeScope::Database {
                database: database.clone(),
            },
        })
        .collect()
}

pub(super) fn columns(command: &Command) -> Vec<Column> {
    let Command::Program(program) = command else {
        return Vec::new();
    };
    program_columns(program)
}

fn program_columns(program: &Program) -> Vec<Column> {
    program
        .result
        .iter()
        .flat_map(|projection| &projection.items)
        .map(|(expression, name)| {
            let column_type = match expression {
                Expression::Cast(_, Cast::String) | Expression::ElementId(_) => {
                    ColumnType::VarString
                }
                Expression::Cast(_, Cast::Int64) | Expression::PathLength(_) => {
                    ColumnType::LongLong
                }
                Expression::Cast(_, Cast::Float64) => ColumnType::Double,
                Expression::Cast(_, Cast::Boolean)
                | Expression::Binary(..)
                | Expression::Not(_)
                | Expression::IsNull(..) => ColumnType::Tiny,
                _ => ColumnType::Json,
            };
            Column {
                name: name.clone(),
                column_type,
                source: Default::default(),
            }
        })
        .collect()
}

pub(super) fn validate_session(
    session: &SessionContext,
    command: &Command,
) -> Result<(), QueryError> {
    if session.has_explicit_table_locks() {
        return Err(crate::mysql::errors::unsupported("GQL with LOCK TABLES"));
    }
    if command.ddl()
        && (!session.autocommit
            || session.explicit_txn
            || session.current_txn.is_some()
            || session.xa_branch.is_some())
    {
        return Err(crate::error::catalog::ER_LOCK_OR_ACTIVE_TRANSACTION
            .error("Graph DDL requires autocommit outside a transaction"));
    }
    if command.writes() && session.sql_log_bin && crate::binlog::enabled() {
        return Err(crate::mysql::errors::unsupported(
            "Graph writes while MySQL binary logging is active for this session",
        ));
    }
    Ok(())
}

pub(super) fn execute(
    db: &Arc<Database>,
    session: &mut SessionContext,
    command: Command,
) -> Result<QueryResponse, QueryError> {
    if matches!(command, Command::ResetGraph) {
        session.current_graph = None;
        return Ok(QueryResponse::Ok);
    }
    let _deadline = if command.writes() {
        None
    } else {
        crate::engine::cancel::install_query_deadline(Duration::from_millis(
            session.sysvars.max_execution_time_ms,
        ))
    };
    let (database, table) = target(session, &command)
        .ok_or_else(|| invalid("Select a property graph with SESSION SET GRAPH first"))?;
    if database.is_empty() {
        return Err(crate::error::catalog::ER_NO_DB_ERROR.error("No database selected"));
    }
    if table.len() > 64
        || !table
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_')
    {
        return Err(invalid(
            "Graph names must be ASCII identifiers of at most 51 bytes",
        ));
    }
    let name = table_key(&database, &table);
    match command {
        Command::Create { conditional, .. } => {
            if !conditional || db.graph_descriptor(&name).is_err() {
                storage::create(db, &name)?;
            }
            Ok(QueryResponse::Ok)
        }
        Command::Drop { conditional, .. } => {
            let _no_recycle = crate::recyclebin::PolicyGuard::enter(Some(false));
            let identity = db.graph_descriptor(&name).ok().map(|table| table.id);
            if identity.is_some() || !db.relation_name_exists(&name) {
                db.drop_tables(&[name], conditional)?;
                if let Some(id) = identity {
                    storage::node_locators().discard(db, id);
                }
            } else {
                return Err(missing_graph());
            }
            Ok(QueryResponse::Ok)
        }
        Command::SetGraph(_) => {
            let descriptor = db.graph_descriptor(&name)?;
            session.current_graph = Some(GraphRef {
                database,
                table,
                id: descriptor.id,
            });
            Ok(QueryResponse::Ok)
        }
        command => {
            let descriptor = db.graph_descriptor(&name)?;
            if matches!(command, Command::Program(_))
                && session
                    .current_graph
                    .as_ref()
                    .is_none_or(|g| g.id != descriptor.id)
            {
                return Err(missing_graph());
            }
            let writes = command.writes();
            let run = |db: &Arc<Database>, _: &mut SessionContext, txn: TxnView| {
                if txn.isolation == IsolationLevel::ReadUncommitted {
                    return Err(crate::mysql::errors::unsupported(
                        "GQL at READ UNCOMMITTED isolation",
                    ));
                }
                if writes && txn.read_only {
                    return Err(
                        crate::error::catalog::ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION
                            .error("Graph writes require a read-write transaction"),
                    );
                }
                if txn.isolation == IsolationLevel::Serializable && txn.txn_id != 0 && !writes {
                    db.acquire_table_shared(txn.txn_id, descriptor.id)?;
                }
                let mut store = storage::GraphStore::new(db, descriptor, txn)?;
                match command {
                    Command::Program(program) => {
                        let _activity = (!writes).then(GraphReadActivity::enter);
                        matching::execute(&mut store, program)
                    }
                    Command::CreateIndex { name, spec, .. } => {
                        store.create_index(&name, spec)?;
                        Ok(QueryResponse::Ok)
                    }
                    Command::DropIndex { name, .. } => {
                        store.drop_index(&name)?;
                        Ok(QueryResponse::Ok)
                    }
                    _ => unreachable!(),
                }
            };
            if writes {
                super::tx::execute_dml_in_transaction_with_session(db, session, run)
            } else if super::tx::can_use_prepared_read(session, false) {
                let (txn, _guard) = super::tx::prepared_select_read_view(db, session, false)?;
                run(db, session, txn)
            } else {
                super::tx::execute_in_transaction_with_session(db, session, run)
            }
        }
    }
}
