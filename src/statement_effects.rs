//! Canonical SQL statement side-effect classification.
//!
//! Replication read-only guards, implicit-commit/DDL handling and binlog
//! admission must not maintain independent allowlists: adding an AST variant
//! to only one list can make a replica writable or skip required DDL handling.

use crate::parser::{
    visit::{self, AstVisitor},
    QueryRef, Select, SetScope, Statement,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct StatementEffects {
    pub(crate) writes_data: bool,
    pub(crate) changes_catalog: bool,
    pub(crate) requires_implicit_commit: bool,
    pub(crate) replica_forbidden: bool,
    pub(crate) ddl_or_maintenance: bool,
}

const READ_ONLY: StatementEffects = StatementEffects {
    writes_data: false,
    changes_catalog: false,
    requires_implicit_commit: false,
    replica_forbidden: false,
    ddl_or_maintenance: false,
};

const DATA_WRITE: StatementEffects = StatementEffects {
    writes_data: true,
    changes_catalog: false,
    requires_implicit_commit: false,
    replica_forbidden: true,
    ddl_or_maintenance: false,
};

const CATALOG_DDL: StatementEffects = StatementEffects {
    writes_data: false,
    changes_catalog: true,
    requires_implicit_commit: true,
    replica_forbidden: true,
    ddl_or_maintenance: true,
};

const CATALOG_NONDISPATCH: StatementEffects = StatementEffects {
    writes_data: false,
    changes_catalog: true,
    requires_implicit_commit: true,
    replica_forbidden: true,
    ddl_or_maintenance: false,
};

const MAINTENANCE_WRITE: StatementEffects = StatementEffects {
    writes_data: true,
    changes_catalog: false,
    requires_implicit_commit: true,
    replica_forbidden: true,
    ddl_or_maintenance: true,
};

const MAINTENANCE_NONDISPATCH: StatementEffects = StatementEffects {
    writes_data: true,
    changes_catalog: false,
    requires_implicit_commit: true,
    replica_forbidden: true,
    ddl_or_maintenance: false,
};

const MAINTENANCE_READ: StatementEffects = StatementEffects {
    writes_data: false,
    changes_catalog: false,
    requires_implicit_commit: false,
    replica_forbidden: false,
    ddl_or_maintenance: true,
};

const LOCKING_READ: StatementEffects = StatementEffects {
    writes_data: false,
    changes_catalog: false,
    requires_implicit_commit: false,
    replica_forbidden: true,
    ddl_or_maintenance: false,
};

pub(crate) fn statement_effects(statement: &Statement) -> StatementEffects {
    use Statement as S;
    match statement {
        S::AlterSystemReloadHttpConfig => READ_ONLY,
        S::ShowHttpStatus => READ_ONLY,
        S::Ai(command) => {
            if matches!(command.action.as_str(), "show" | "show_create" | "reload") {
                READ_ONLY
            } else {
                CATALOG_NONDISPATCH
            }
        }
        S::CreateTable(_)
        | S::CreateTableLike { .. }
        | S::AlterTable(_)
        | S::CreateDatabase { .. }
        | S::AlterDatabase { .. }
        | S::CreateSequence { .. }
        | S::AlterSequence { .. }
        | S::DropSequence { .. }
        | S::DropDatabase { .. }
        | S::DropTable { .. }
        | S::TruncateTable { .. }
        | S::CreateIndex(_)
        | S::DropIndex { .. }
        | S::CreateView { .. }
        | S::AlterView { .. }
        | S::DropView { .. }
        | S::RenameTables { .. }
        | S::CreateRoutine(_)
        | S::AlterRoutine { .. }
        | S::DropRoutine { .. }
        | S::CreateTrigger(_)
        | S::DropTrigger { .. }
        | S::CreateEvent(_)
        | S::AlterEvent(_)
        | S::DropEvent { .. }
        | S::CreateUser(_)
        | S::AlterUser(_)
        | S::AlterUsers(_)
        | S::DropUser { .. }
        | S::RenameUser { .. }
        | S::CreateRole { .. }
        | S::DropRole { .. }
        | S::Grant(_)
        | S::Revoke(_)
        | S::SetDefaultRole(_)
        | S::SetPassword { .. } => CATALOG_DDL,

        S::CreateTablespace { .. }
        | S::DropTablespace { .. }
        | S::AlterTablespace { .. }
        | S::CreateResourceGroup { .. }
        | S::DropResourceGroup { .. }
        | S::AlterResourceGroup { .. }
        | S::InstallComponent { .. }
        | S::UninstallComponent { .. } => CATALOG_NONDISPATCH,

        S::CreateTableAsSelect { .. } => StatementEffects {
            writes_data: true,
            ..CATALOG_NONDISPATCH
        },

        S::Insert(_) | S::Update(_) | S::Delete(_) | S::LoadData(_) | S::Call(_) => DATA_WRITE,

        S::AnalyzeTable { .. } | S::RepairTable { .. } | S::OptimizeTable { .. } => {
            MAINTENANCE_WRITE
        }
        S::CheckTable { .. } => MAINTENANCE_READ,
        S::CancelDdlJob(_)
        | S::ResetPersist { .. }
        | S::AlterSystemCheckpoint
        | S::AlterSystemFlushDataCache
        | S::AlterSystemFlushSqlCache => MAINTENANCE_NONDISPATCH,

        S::Select(_) | S::SetOperation(_) | S::WithCte(_) => query_effects(statement.into()),
        S::Explain {
            target, analyze, ..
        } => {
            if *analyze {
                statement_effects(target)
            } else {
                READ_ONLY
            }
        }
        S::SelectIntoVars { inner, .. } | S::SelectIntoFile { inner, .. } => {
            query_effects(inner.into())
        }
        S::DeclareSqlCursor { query, .. } => query_effects(query.into()),

        S::Set(assignments) => {
            let persists = assignments.iter().any(|assignment| {
                matches!(
                    assignment.scope,
                    Some(SetScope::Persist | SetScope::PersistOnly)
                )
            });
            if persists {
                MAINTENANCE_NONDISPATCH
            } else {
                READ_ONLY
            }
        }

        S::Unsupported { kind, .. } => {
            if is_ddl_kind(kind) {
                CATALOG_DDL
            } else {
                READ_ONLY
            }
        }

        S::Begin { .. }
        | S::Commit { .. }
        | S::Rollback { .. }
        | S::Savepoint(_)
        | S::RollbackToSavepoint(_)
        | S::ReleaseSavepoint(_)
        | S::Prepare { .. }
        | S::ExecutePrepared { .. }
        | S::DeallocatePrepared { .. }
        | S::SetAutocommit(_)
        | S::SetTransactionIsolation { .. }
        | S::LockTables(_)
        | S::UnlockTables
        | S::ShowCreateSequence { .. }
        | S::ShowSequences { .. }
        | S::ExplainForConnection { .. }
        | S::Describe { .. }
        | S::UseDatabase(_)
        | S::ShowDatabases { .. }
        | S::ShowTables { .. }
        | S::ShowTableStatus { .. }
        | S::ShowColumns { .. }
        | S::ShowIndex { .. }
        | S::ShowCreateDatabase { .. }
        | S::ShowCreateTable { .. }
        | S::ShowCreateRoutine { .. }
        | S::ShowRoutineStatus { .. }
        | S::ShowCreateTrigger { .. }
        | S::ShowTriggers { .. }
        | S::ShowCreateEvent { .. }
        | S::ShowEvents { .. }
        | S::ShowCharset { .. }
        | S::ShowCollation { .. }
        | S::ShowVariables { .. }
        | S::ShowStatus { .. }
        | S::ShowDiagnostics { .. }
        | S::GetDiagnostics { .. }
        | S::ShowCountDiagnostics { .. }
        | S::ShowEngines
        | S::ShowEngineMydbStatus
        | S::ShowMydbMemory
        | S::ShowMydbMemorySessions
        | S::ShowEngine { .. }
        | S::ShowPlugins
        | S::ShowOpenTables { .. }
        | S::ShowProcesslist { .. }
        | S::ShowGrants
        | S::ShowGrantsFor { .. }
        | S::ShowCreateUser { .. }
        | S::Kill { .. }
        | S::Shutdown
        | S::Restart
        | S::ShowPrivileges
        | S::SetRole(_)
        | S::SetNames { .. }
        | S::SetCharacterSet { .. }
        | S::SetResourceGroup { .. }
        | S::SetTransactionCharacteristics { .. }
        | S::FlushPrivileges
        | S::FlushUserResources
        | S::FlushHosts
        | S::FlushStatus
        | S::FlushTables { .. }
        | S::FlushQueryCache
        | S::FlushLogs { .. }
        | S::ReplicationAdmin { .. }
        | S::MydbReplicationAdmin { .. }
        | S::BackupAdmin { .. }
        | S::BinlogAdmin { .. }
        | S::Noop { .. }
        | S::Do(_)
        | S::MultiStatement { .. }
        | S::XaStart { .. }
        | S::XaEnd { .. }
        | S::XaPrepare { .. }
        | S::XaCommit { .. }
        | S::XaRollback { .. }
        | S::XaRecover { .. }
        | S::FetchSqlCursor { .. }
        | S::CloseSqlCursor { .. }
        | S::ChecksumTable { .. } => READ_ONLY,
    }
}

#[derive(Default)]
struct QueryLocks {
    any: bool,
    exclusive: bool,
    sequence_write: bool,
}
impl AstVisitor for QueryLocks {
    fn enter_expr(&mut self, expr: &crate::parser::Expr) {
        if let crate::parser::Expr::Function { name, .. } = expr {
            if name.eq_ignore_ascii_case("nextval") || name.eq_ignore_ascii_case("setval") {
                self.sequence_write = true;
                self.exclusive = true;
            }
        }
    }
    fn enter_select(&mut self, select: &Select) {
        if let Some(clause) = &select.lock_clause {
            self.any = true;
            self.exclusive |= clause.mode == crate::parser::LockClauseMode::ForUpdate
                || clause
                    .targets
                    .iter()
                    .any(|target| target.mode == crate::parser::LockClauseMode::ForUpdate);
        } else if select.for_update {
            self.any = true;
            self.exclusive = true;
        }
    }
}
fn select_locks(select: &Select) -> QueryLocks {
    let mut locks = QueryLocks::default();
    visit::visit_select(&mut locks, select);
    locks
}
fn set_locks(set: &crate::parser::SetOperation) -> QueryLocks {
    let mut locks = QueryLocks::default();
    visit::visit_set_operation(&mut locks, set);
    locks
}
fn query_effects(query: QueryRef<'_>) -> StatementEffects {
    match query {
        QueryRef::Select(select) => {
            let locks = select_locks(select);
            if locks.sequence_write {
                DATA_WRITE
            } else if locks.any {
                LOCKING_READ
            } else {
                READ_ONLY
            }
        }
        QueryRef::SetOperation(set) => {
            let locks = set_locks(set);
            if locks.sequence_write {
                DATA_WRITE
            } else if locks.any {
                LOCKING_READ
            } else {
                READ_ONLY
            }
        }
        QueryRef::Rrf(rrf) => {
            let mut effects = READ_ONLY;
            for branch in &rrf.branches {
                let branch = query_effects((&branch.query).into());
                effects.writes_data |= branch.writes_data;
                effects.replica_forbidden |= branch.replica_forbidden;
            }
            effects
        }
        QueryRef::WithCte(with) => {
            let mut effects = query_effects(with.body.as_ref());
            for cte in with.ctes {
                effects.replica_forbidden |= query_effects((&cte.query).into()).replica_forbidden;
            }
            effects
        }
        QueryRef::Other(statement) => statement_effects(statement),
    }
}
fn query_read_only_transaction_forbidden(query: QueryRef<'_>) -> bool {
    match query {
        QueryRef::Select(select) => select_locks(select).exclusive,
        QueryRef::SetOperation(set) => set_locks(set).exclusive,
        QueryRef::Rrf(rrf) => rrf
            .branches
            .iter()
            .any(|branch| query_read_only_transaction_forbidden((&branch.query).into())),
        QueryRef::WithCte(with) => {
            query_read_only_transaction_forbidden(with.body.as_ref())
                || with
                    .ctes
                    .iter()
                    .any(|cte| query_read_only_transaction_forbidden((&cte.query).into()))
        }
        QueryRef::Other(statement) => is_read_only_transaction_forbidden(statement),
    }
}

pub(crate) fn is_ddl_or_maintenance(statement: &Statement) -> bool {
    match statement {
        // Query effects are only ever READ_ONLY, LOCKING_READ or DATA_WRITE;
        // skip the lock-clause walk on this per-execution MDL/backup check.
        Statement::Select(_)
        | Statement::SetOperation(_)
        | Statement::WithCte(_)
        | Statement::Insert(_)
        | Statement::Update(_)
        | Statement::Delete(_) => false,
        _ => statement_effects(statement).ddl_or_maintenance,
    }
}

pub(crate) fn is_replica_forbidden(statement: &Statement) -> bool {
    statement_effects(statement).replica_forbidden
}

/// Shared locking reads acquire locks but do not require READ WRITE access.
/// Keep replica admission separate: replicas still reject locking reads.
pub(crate) fn is_read_only_transaction_forbidden(statement: &Statement) -> bool {
    match statement {
        Statement::Select(_) | Statement::SetOperation(_) | Statement::WithCte(_) => {
            query_read_only_transaction_forbidden(statement.into())
        }
        Statement::Explain {
            target,
            analyze: true,
            ..
        } => is_read_only_transaction_forbidden(target),
        Statement::SelectIntoVars { inner, .. } | Statement::SelectIntoFile { inner, .. } => {
            query_read_only_transaction_forbidden(inner.into())
        }
        Statement::DeclareSqlCursor { query, .. } => {
            query_read_only_transaction_forbidden(query.into())
        }
        _ => is_replica_forbidden(statement),
    }
}

pub(crate) fn is_ddl_kind(kind: &str) -> bool {
    let upper = kind.to_ascii_uppercase();
    ["CREATE", "ALTER", "DROP", "TRUNCATE"]
        .iter()
        .any(|prefix| upper.starts_with(prefix))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_statement;

    #[test]
    fn canonical_classifier_covers_high_risk_statement_families() {
        for sql in [
            "CREATE TABLE t (id INT)",
            "CREATE TABLE t2 AS SELECT 1",
            "RENAME TABLE t TO t2",
            "CREATE SEQUENCE s",
            "OPTIMIZE TABLE t",
            "INSERT INTO t VALUES (1)",
            "SELECT * FROM t FOR UPDATE",
            "SET PERSIST max_connections = 100",
        ] {
            let statement = parse_statement(sql).unwrap_or_else(|err| panic!("{sql}: {err:?}"));
            assert!(
                is_replica_forbidden(&statement),
                "must be forbidden on a replica: {sql}"
            );
        }
        assert!(!is_replica_forbidden(&parse_statement("SELECT 1").unwrap()));
    }
    #[test]
    fn locking_query_effects_include_all_query_slots() {
        for sql in [
            "SELECT * FROM t UNION ALL SELECT * FROM t FOR UPDATE",
            "WITH q AS(SELECT * FROM t FOR UPDATE) SELECT * FROM q",
            "SELECT * FROM (SELECT * FROM t FOR UPDATE) q",
            "SELECT (SELECT id FROM t FOR UPDATE)",
            "DECLARE q CURSOR FOR SELECT * FROM t UNION ALL SELECT * FROM t FOR UPDATE",
        ] {
            let query = parse_statement(sql).unwrap();
            assert!(
                is_replica_forbidden(&query),
                "locking query admitted on a replica: {sql}"
            );
            assert!(
                is_read_only_transaction_forbidden(&query),
                "FOR UPDATE admitted in a read-only transaction: {sql}"
            );
        }
    }
    #[test]
    fn shared_locking_query_slots_remain_legal_in_read_only_transactions() {
        for sql in [
            "SELECT * FROM t UNION ALL SELECT * FROM t FOR SHARE",
            "WITH q AS(SELECT * FROM t FOR SHARE) SELECT * FROM q",
            "SELECT * FROM (SELECT * FROM t FOR SHARE) q",
            "SELECT (SELECT id FROM t FOR SHARE)",
            "DECLARE q CURSOR FOR SELECT * FROM t UNION ALL SELECT * FROM t FOR SHARE",
        ] {
            let query = parse_statement(sql).unwrap();
            assert!(
                is_replica_forbidden(&query),
                "shared locks admitted on replica: {sql}"
            );
            assert!(
                !is_read_only_transaction_forbidden(&query),
                "shared locks incorrectly require READ WRITE: {sql}"
            );
        }
        let mixed =
            parse_statement("SELECT * FROM (SELECT * FROM t FOR UPDATE) q FOR SHARE").unwrap();
        assert!(is_read_only_transaction_forbidden(&mixed));
    }
}
