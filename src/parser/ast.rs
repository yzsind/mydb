//! Public mydb SQL AST and structured parse errors.

use super::*;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Statement {
    /// `START TRANSACTION`/`BEGIN` with the MySQL 8.0 characteristic list.
    /// `read_only`: `None` = session default access mode, `Some(true)` =
    /// `READ ONLY`, `Some(false)` = `READ WRITE`. `consistent_snapshot` =
    /// the `WITH CONSISTENT SNAPSHOT` modifier (only effective at RR).
    Begin {
        read_only: Option<bool>,
        consistent_snapshot: bool,
    },
    /// `COMMIT [WORK] [AND [NO] CHAIN] [[NO] RELEASE]`. `chain`/`release`
    /// are `None` when unspecified (default taken from `completion_type`).
    Commit {
        chain: Option<bool>,
        release: Option<bool>,
    },
    /// `ROLLBACK [WORK] [AND [NO] CHAIN] [[NO] RELEASE]`.
    Rollback {
        chain: Option<bool>,
        release: Option<bool>,
    },
    Savepoint(String),
    RollbackToSavepoint(String),
    ReleaseSavepoint(String),
    /// mydb extension: request cancellation of an online DDL before its
    /// final catalog swap. The numeric id is exposed by
    /// `information_schema.mydb_ddl_jobs`.
    CancelDdlJob(u64),
    /// Engine-native online backup administration. The backup subsystem owns
    /// strict parsing of BACKUP/SHOW/CANCEL clauses and repository URIs.
    BackupAdmin {
        command: crate::backup::BackupCommand,
    },
    /// Session SQL prepared statements (text protocol surface). These share
    /// the numbered AST/parameter execution machinery with COM_STMT_* while
    /// retaining MySQL's named-session lifecycle.
    Prepare {
        name: String,
        source: Expr,
    },
    ExecutePrepared {
        name: String,
        using: Vec<String>,
    },
    DeallocatePrepared {
        name: String,
    },
    SetAutocommit(bool),
    SetTransactionIsolation {
        scope: Option<SetScope>,
        isolation: TransactionIsolation,
    },
    LockTables(Vec<LockTableSpec>),
    UnlockTables,
    CreateTable(CreateTable),
    CreateTableLike {
        name: String,
        if_not_exists: bool,
        source: String,
        temporary: bool,
        on_commit: Option<TempTableOnCommit>,
    },
    AlterTable(AlterTable),
    CreateDatabase {
        name: String,
        if_not_exists: bool,
        default_charset: Option<String>,
        default_collation: Option<String>,
    },
    AlterDatabase {
        name: String,
        default_charset: Option<String>,
        default_collation: Option<String>,
    },
    DropDatabase {
        name: String,
        if_exists: bool,
    },
    DropTable {
        names: Vec<String>,
        if_exists: bool,
        temporary: bool,
        drop_behavior: Option<TableDropBehavior>,
    },
    /// `CREATE [TEMPORARY] SEQUENCE [IF NOT EXISTS] name [options...]`
    /// (PostgreSQL-style). `options` is already normalised into the storage
    /// option bag; the executor resolves defaults + validates.
    CreateSequence {
        name: String,
        if_not_exists: bool,
        temporary: bool,
        options: crate::storage::sequence::SeqOptions,
    },
    /// `ALTER SEQUENCE [IF EXISTS] name [options...] [RESTART [WITH n]]
    /// [RENAME TO new]`. Hand-parsed (sqlparser 0.61 has no ALTER SEQUENCE).
    AlterSequence {
        name: String,
        if_exists: bool,
        options: crate::storage::sequence::SeqOptions,
        restart: Option<SequenceRestart>,
        rename_to: Option<String>,
    },
    /// `DROP SEQUENCE [IF EXISTS] name [, name ...]`.
    DropSequence {
        names: Vec<String>,
        if_exists: bool,
    },
    /// `SHOW CREATE SEQUENCE name`.
    ShowCreateSequence {
        name: String,
    },
    /// `SHOW SEQUENCES [{FROM | IN} db] [LIKE 'pat' | WHERE expr]`.
    ShowSequences {
        database: Option<String>,
        filter: Option<ShowFilter>,
    },
    TruncateTable {
        name: String,
    },
    CreateIndex(CreateIndex),
    DropIndex {
        name: String,
        table: String,
    },
    AnalyzeTable {
        tables: Vec<String>,
        /// `ANALYZE TABLE t PARTITION (...)`; mutually exclusive with histogram.
        partitions: Option<crate::parser_partition::PartitionTargets>,
        /// `UPDATE HISTOGRAM ON ...` / `DROP HISTOGRAM ON ...`. `None` is
        /// a plain `ANALYZE TABLE` (full re-sample). Histogram operations
        /// require exactly one table.
        histogram: Option<HistogramAction>,
    },
    /// `CHECK TABLE name [, name ...]` — run integrity checks. mydb is a
    /// transactional MVCC engine, so this never returns "corrupt" under
    /// normal operation; the command exists for tooling parity (mysqldump
    /// `--check-tables`, ORM probes). One result row per table.
    CheckTable {
        tables: Vec<String>,
    },
    /// `REPAIR TABLE name [, name ...]` — accepted for parity. mydb has
    /// no MyISAM-style repair surface; we emit a Note row pointing
    /// operators at proper recovery (replica re-clone / point-in-time
    /// restore), same shape MySQL's InnoDB returns.
    RepairTable {
        tables: Vec<String>,
    },
    /// Online physical rebuild of the named tables, followed by statistics
    /// refresh. Bulk copying permits DML; prepare/publish use brief barriers.
    OptimizeTable {
        tables: Vec<String>,
        no_write_to_binlog: bool,
    },
    /// `CHECKSUM TABLE name [, name ...] [QUICK|EXTENDED]` — MySQL 8.0
    /// integrity command. mydb computes a CRC32-of-CRC32s rollup over
    /// each row's binary serialization for the named tables. The QUICK
    /// option is honoured as a fast path that returns NULL when the
    /// table never had a CHECKSUM=1 option (matching MySQL); we accept
    /// EXTENDED as a synonym for the default. One row per table.
    ChecksumTable {
        tables: Vec<String>,
        quick: bool,
    },
    /// `CREATE TABLESPACE name [ADD DATAFILE 'path'] [...]` — MySQL 8.0
    /// general tablespace DDL. mydb does not segregate storage by
    /// tablespace (every table lives in its own files under the
    /// schema directory), so the statement is accepted to keep
    /// mysqldump output replayable. The catalog records the name +
    /// optional datafile path so SHOW CREATE / information_schema can
    /// echo what the user asked for.
    CreateTablespace {
        name: String,
        datafile: Option<String>,
        undo: bool,
    },
    /// `DROP TABLESPACE name [ENGINE=...]` — symmetric of
    /// CreateTablespace. Errors with 3510 ER_TABLESPACE_MISSING_WITH_NAME when
    /// the name was never declared.
    DropTablespace {
        name: String,
        undo: bool,
    },
    /// `ALTER TABLESPACE name {RENAME TO ... | ADD DATAFILE ... | ...}`
    /// — MySQL 8.0 alterations. Accepted + ignored at the catalog
    /// level because mydb has no per-tablespace storage; reported via
    /// warning 1287 so callers know the intent isn't realised.
    AlterTablespace {
        name: String,
        body: String,
    },
    CreateView {
        name: super::QualifiedName,
        /// Original body for diagnostics and presentation; never reparsed for execution.
        sql: String,
        query: QueryExpr,
        or_replace: bool,
        if_not_exists: bool,
        algorithm: Option<String>,
        definer: Option<AccountIdent>,
        security_type: Option<RoutineSecurity>,
        /// Optional explicit column-name list: `CREATE VIEW v(a, b) AS ...`.
        /// Empty means inherit names from the SELECT projection.
        columns: Vec<String>,
        /// Parsed WITH [LOCAL|CASCADED] CHECK OPTION for catalog enforcement.
        check_option: Option<String>,
    },
    AlterView {
        name: super::QualifiedName,
        sql: String,
        query: QueryExpr,
        algorithm: Option<String>,
        definer: Option<AccountIdent>,
        security_type: Option<RoutineSecurity>,
        columns: Vec<String>,
        check_option: Option<String>,
    },
    DropView {
        names: Vec<super::QualifiedName>,
        if_exists: bool,
    },
    CreateTableAsSelect {
        /// Complete target-table definition. This is present even for bare
        /// `CREATE TABLE t AS SELECT ...` so table options and pre-parser
        /// extensions cannot be silently discarded.
        table: Box<CreateTable>,
        /// Whether the user supplied an explicit column/constraint list.
        /// The executor uses this to apply MySQL's declared-column merge
        /// rules; `table.columns` is empty in the bare form.
        explicit_definition: bool,
        conflict_policy: CtasConflictPolicy,
        query: QueryExpr,
    },
    Insert(Insert),
    Select(Select),
    SetOperation(SetOperation),
    WithCte(WithCte),
    Explain {
        target: Box<Statement>,
        /// Explicit `FORMAT=...` clause. For plain `EXPLAIN`, `None` means
        /// the executor falls back to the session-level `explain_format`
        /// setting; `EXPLAIN ANALYZE` defaults to `TREE`.
        format: Option<ExplainFormat>,
        /// Phase B #B5: when true, the planner output is supplemented
        /// by *actual* execution metrics — the query runs and EXPLAIN
        /// rows carry observed row counts and wall-clock time. When
        /// false (the default), no execution happens and only static
        /// plan information is returned.
        analyze: bool,
    },
    /// `EXPLAIN FOR CONNECTION <conn_id>` — looks up the live statement that
    /// the named connection is currently executing (via the session
    /// registry) and produces an EXPLAIN of its current SQL. Requires the
    /// `PROCESS` privilege. Returns error 1094 ("Unknown thread id") if the
    /// connection is gone or has no current statement.
    ExplainForConnection {
        connection_id: u32,
        format: Option<ExplainFormat>,
    },
    /// `SELECT … INTO @var1, @var2[, …] FROM …` — assigns the (single-row)
    /// SELECT result columns to user variables. The inner SELECT is parsed
    /// into a query-only syntax node so the
    /// existing executor handles streaming + projection. Per MySQL: ≥2 rows
    /// → error 1172; 0 rows → variables retain their previous value.
    SelectIntoVars {
        inner: QueryExpr,
        targets: Vec<String>,
    },
    /// MySQL server-side file export. The file clause is deliberately kept
    /// outside the query AST so every SELECT shape (CTE, UNION, TABLE, ...)
    /// continues through the normal planner and cursor implementation.
    SelectIntoFile {
        inner: QueryExpr,
        options: SelectIntoFileOptions,
    },
    /// `RENAME TABLE t1 TO t2, t3 TO t4[, …]` — multi-pair atomic table
    /// rename. Each pair is applied in declaration order; failure rolls back
    /// already-applied pairs. The single-pair form `RENAME TABLE t1 TO t2`
    /// also routes here for uniform handling.
    RenameTables {
        pairs: Vec<(String, String)>,
    },
    Update(Update),
    Delete(Delete),
    Describe {
        table: String,
    },
    UseDatabase(String),
    ShowDatabases {
        filter: Option<ShowFilter>,
    },
    ShowTables {
        database: Option<String>,
        full: bool,
        filter: Option<ShowFilter>,
    },
    ShowTableStatus {
        database: Option<String>,
        filter: Option<ShowFilter>,
    },
    ShowColumns {
        table: String,
        full: bool,
        filter: Option<ShowFilter>,
    },
    ShowIndex {
        table: String,
        filter: Option<ShowFilter>,
    },
    ShowCreateDatabase {
        name: String,
        if_not_exists: bool,
    },
    ShowCreateTable {
        table: String,
    },
    CreateRoutine(RoutineDef),
    AlterRoutine {
        kind: RoutineKind,
        name: String,
        characteristics: RoutineCharacteristicsPatch,
    },
    DropRoutine {
        kind: RoutineKind,
        name: String,
        if_exists: bool,
    },
    Call(RoutineCall),
    ShowCreateRoutine {
        kind: RoutineKind,
        name: String,
    },
    ShowRoutineStatus {
        kind: RoutineKind,
        filter: Option<ShowFilter>,
    },
    CreateTrigger(TriggerDef),
    DropTrigger {
        name: String,
        if_exists: bool,
    },
    ShowCreateTrigger {
        name: String,
    },
    ShowTriggers {
        database: Option<String>,
        filter: Option<ShowFilter>,
    },
    CreateEvent(EventDef),
    AlterEvent(AlterEventStmt),
    DropEvent {
        name: String,
        if_exists: bool,
    },
    ShowCreateEvent {
        name: String,
    },
    ShowEvents {
        database: Option<String>,
        filter: Option<ShowFilter>,
    },
    ShowCharset {
        filter: Option<ShowFilter>,
    },
    ShowCollation {
        filter: Option<ShowFilter>,
    },
    ShowVariables {
        scope: ShowScope,
        filter: Option<ShowFilter>,
    },
    ShowStatus {
        scope: ShowScope,
        filter: Option<ShowFilter>,
    },
    ShowDiagnostics {
        errors_only: bool,
        limit: Option<ShowLimit>,
    },
    GetDiagnostics {
        stacked: bool,
        condition: Option<Expr>,
        items: Vec<(RoutineTarget, DiagItem)>,
    },
    ShowCountDiagnostics {
        errors_only: bool,
    },
    ShowEngines,
    ShowEngineMydbStatus,
    /// mydb-native process/storage/cache memory summary.
    ShowMydbMemory,
    /// Per-connection execution and retained-cache memory summary.
    ShowMydbMemorySessions,
    /// Generalised `SHOW ENGINE engine_name {STATUS | MUTEX}`. The exact
    /// `SHOW ENGINE MYDB STATUS` form keeps its own [`Statement::ShowEngineMydbStatus`]
    /// fast path; this variant carries every other shape — `MUTEX`, and
    /// any non-`MYDB` engine name (which the executor rejects with 1286
    /// `ER_UNKNOWN_STORAGE_ENGINE` rather than a parse error, matching the
    /// way MySQL tooling probes `SHOW ENGINE INNODB STATUS`).
    ShowEngine {
        engine: String,
        kind: ShowEngineKind,
    },
    ShowPlugins,
    ShowOpenTables {
        database: Option<String>,
        filter: Option<ShowFilter>,
    },
    ShowProcesslist {
        full: bool,
    },
    ShowGrants,
    ShowGrantsFor {
        account: AccountIdent,
        using_roles: Vec<AccountIdent>,
    },
    ShowCreateUser {
        account: AccountIdent,
    },
    Kill {
        target: KillTarget,
        connection_id: u64,
    },
    /// MySQL 8.0 `SHUTDOWN` — controlled server shutdown. Requires the
    /// `SHUTDOWN` privilege.
    Shutdown,
    /// MySQL 8.0 `RESTART` — controlled shutdown + relaunch. Requires the
    /// `SHUTDOWN` privilege.
    Restart,
    AlterSystemCheckpoint,
    AlterSystemFlushDataCache,
    AlterSystemFlushSqlCache,
    ShowPrivileges,
    CreateUser(CreateUser),
    AlterUser(AlterUser),
    AlterUsers(Vec<AlterUser>),
    DropUser {
        accounts: Vec<AccountIdent>,
        if_exists: bool,
    },
    RenameUser {
        renames: Vec<(AccountIdent, AccountIdent)>,
    },
    CreateRole {
        roles: Vec<AccountIdent>,
        if_not_exists: bool,
    },
    DropRole {
        roles: Vec<AccountIdent>,
        if_exists: bool,
    },
    Grant(Grant),
    Revoke(Revoke),
    SetRole(SetRole),
    SetDefaultRole(SetDefaultRole),
    SetPassword {
        account: Option<AccountIdent>,
        password: String,
        random_password: bool,
        /// MySQL 8.0 `REPLACE 'old'` — verifies the old password before
        /// switching. Required when the account has
        /// `PASSWORD REQUIRE CURRENT`.
        replace: Option<String>,
        /// MySQL 8.0 `RETAIN CURRENT PASSWORD` — keep the previous hash as
        /// a secondary credential so existing connections / mid-rotation
        /// clients can still authenticate.
        retain_current: bool,
    },
    SetNames {
        charset: Option<String>,
        collation: Option<String>,
    },
    /// MySQL 8.0 `SET CHARACTER SET cs` / `SET CHARSET cs` / `SET CHARACTER SET DEFAULT`.
    /// Differs from SET NAMES in that connection charset is not changed
    /// (it tracks the current database's charset).
    SetCharacterSet {
        charset: Option<String>,
    },
    Set(Vec<SetAssignment>),
    /// MySQL 8.0 `RESET PERSIST [IF EXISTS] [var_name]` — drop one persisted
    /// variable entry (or the whole file when `var` is `None`).
    ResetPersist {
        var: Option<String>,
        if_exists: bool,
    },
    /// MySQL 8.0 `SET RESOURCE GROUP group_name [FOR thread_id_list]`.
    /// mydb has no resource-group enforcement; the dispatcher accepts the
    /// statement and emits warning 1287 (deprecated/no-op).
    SetResourceGroup {
        group: String,
        threads: Vec<u64>,
    },
    /// MySQL 8.0 `CREATE RESOURCE GROUP name TYPE = {SYSTEM|USER}
    /// [VCPU = ...] [THREAD_PRIORITY = ...] [ENABLE|DISABLE]`.
    /// mydb does not pin sessions to vCPUs; the catalog entry exists
    /// so mysqldump output replays and `SET RESOURCE GROUP` can name
    /// it. A warning 1287 is emitted on each statement.
    CreateResourceGroup {
        name: String,
        /// `SYSTEM` reserved for internal/server-side threads, `USER`
        /// for client sessions. mydb stores both kinds verbatim.
        is_system: bool,
        vcpu: Option<String>,
        thread_priority: Option<i64>,
        enabled: bool,
    },
    /// MySQL 8.0 `DROP RESOURCE GROUP name [FORCE]`. Errors with
    /// 3651 ER_RESOURCE_GROUP_NOT_EXISTS when the name was never
    /// declared.
    DropResourceGroup {
        name: String,
        force: bool,
    },
    /// MySQL 8.0 `ALTER RESOURCE GROUP name [VCPU = ...] [...]`.
    /// Accepted + emit warning 1287; catalog metadata updated.
    AlterResourceGroup {
        name: String,
        body: String,
    },
    /// MySQL 8.0 `SET TRANSACTION [SESSION|GLOBAL] characteristic, ...` — the
    /// access-mode form (`READ ONLY` / `READ WRITE`) plus optional isolation
    /// level. Bare `SET TRANSACTION` (no scope) affects only the next
    /// transaction; SESSION/GLOBAL change the relevant default.
    SetTransactionCharacteristics {
        scope: Option<SetScope>,
        characteristics: Vec<TransactionCharacteristic>,
    },
    FlushPrivileges,
    /// `FLUSH USER_RESOURCES` — L8 admin: reset per-account quota counters
    /// for every account.
    FlushUserResources,
    /// `FLUSH HOSTS` — clears the host cache and unblocks hosts that were
    /// blocked after too many connection errors. mydb has no host cache
    /// (failed-login state lives on the account), so this also resets every
    /// account's `failed_login_count` for parity.
    FlushHosts,
    /// `FLUSH STATUS` — resets process-global counters that MySQL exposes
    /// as session/global status. mydb only resets the per-process counters
    /// that have a meaningful zero (connection peaks); the running totals
    /// stay (matches MySQL 8.0 behaviour, where most Innodb_* counters are
    /// not affected by FLUSH STATUS either).
    FlushStatus,
    /// `FLUSH TABLES [tbl, ...] [WITH READ LOCK | FOR EXPORT]`. The
    /// no-table form flushes all dirty pages + WAL; the table-list form
    /// flushes only those tables. `WITH READ LOCK` / `FOR EXPORT` are
    /// rejected with 1235 (the global read-lock path is not yet wired
    /// through DDL/DML interlocks).
    FlushTables {
        tables: Vec<String>,
        with_read_lock: bool,
        for_export: bool,
    },
    /// `FLUSH QUERY CACHE` / `RESET QUERY CACHE` — query cache was removed
    /// in MySQL 8.0. Accepted for compatibility with old init scripts;
    /// emits deprecation warning 1287.
    FlushQueryCache,
    FlushLogs {
        kind: crate::engine::logging::FlushLogKind,
    },
    /// `INSTALL COMPONENT 'file://component_name' [, ...]`. mydb implements the
    /// `validate_password` component (opt-in password-strength enforcement);
    /// other component names are rejected (no silent no-op).
    InstallComponent {
        names: Vec<String>,
    },
    /// `UNINSTALL COMPONENT 'file://component_name' [, ...]`.
    UninstallComponent {
        names: Vec<String>,
    },
    ReplicationAdmin {
        sql: String,
    },
    /// mydb-native physical replication admin commands
    /// (`SHOW MYDB MASTER/REPLICA STATUS`, `CHANGE MYDB MASTER TO …`,
    /// `START/STOP/RESET MYDB REPLICATION`, `PROMOTE TO MASTER`, etc.).
    /// Routed through [`crate::repl::handle_admin_sql`]. Separate from
    /// `ReplicationAdmin` (MySQL-compatible logical replica commands).
    MydbReplicationAdmin {
        sql: String,
    },
    BinlogAdmin {
        sql: String,
    },
    Noop {
        kind: String,
    },
    LoadData(LoadData),
    /// `DO expr [, expr] ...` — evaluate each expression for its side
    /// effects and discard the result; no result set is returned. MySQL
    /// 13.2.3 — commonly used with `RELEASE_LOCK()` / `SLEEP()` so that
    /// the value can be ignored without raising an empty-result error.
    Do(Vec<Expr>),
    Unsupported {
        kind: String,
        sql: String,
    },
    MultiStatement {
        count: usize,
        sql: String,
    },
    XaStart {
        xid: Xid,
    },
    XaEnd {
        xid: Xid,
    },
    XaPrepare {
        xid: Xid,
    },
    XaCommit {
        xid: Xid,
        one_phase: bool,
    },
    XaRollback {
        xid: Xid,
    },
    XaRecover {
        convert_xid: bool,
    },
    /// `DECLARE <name> CURSOR FOR <select>` issued outside of a stored
    /// routine — a PostgreSQL-flavoured SQL-level cursor. Standard MySQL
    /// only allows DECLARE CURSOR inside `BEGIN ... END` blocks, but we
    /// recognise the form at the session level too because it pairs
    /// naturally with mydb's streaming `ResultCursor` infrastructure
    /// (the same plumbing that powers `useCursorFetch=true` over the
    /// MySQL binary protocol). The SELECT is executed eagerly; the
    /// cursor stays open until `CLOSE` or session disconnect.
    ///
    /// `sql` is the original DECLARE statement, kept so
    /// `information_schema.mydb_cursors.SQL` shows the user's input
    /// (we can't round-trip the parsed AST back to SQL without losing
    /// fidelity).
    DeclareSqlCursor {
        name: String,
        query: QueryExpr,
        sql: String,
    },
    /// `FETCH [N|NEXT] [FROM] <name>` — pulls up to N rows from a
    /// previously-DECLAREd SQL cursor. `count == 0` means "one row"
    /// (matches SQL `FETCH NEXT` semantics).
    FetchSqlCursor {
        name: String,
        count: u64,
    },
    /// `CLOSE <name>` — releases the SQL cursor and any MVCC snapshot /
    /// scan state it was pinning.
    CloseSqlCursor {
        name: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum CtasConflictPolicy {
    #[default]
    Error,
    Ignore,
    Replace,
}

/// `ANALYZE TABLE ... {UPDATE|DROP} HISTOGRAM ON col[, ...] [WITH N BUCKETS]`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum HistogramAction {
    /// `UPDATE HISTOGRAM ON c1, c2 [WITH N BUCKETS]` — (re)build the
    /// per-column histograms for the named columns.
    Update {
        columns: Vec<String>,
        /// Explicit `WITH N BUCKETS`. `None` uses the configured default.
        buckets: Option<usize>,
    },
    /// `DROP HISTOGRAM ON c1, c2` — discard the named columns' histograms.
    Drop { columns: Vec<String> },
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ShowFilter {
    Like(String),
    Where(Expr),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ShowScope {
    Session,
    Global,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ShowEngineKind {
    Status,
    Mutex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShowLimit {
    pub offset: usize,
    pub row_count: usize,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(deny_unknown_fields)]
pub enum RoutineKind {
    Procedure,
    Function,
}

impl RoutineKind {
    pub fn as_str(self) -> &'static str {
        match self {
            RoutineKind::Procedure => "PROCEDURE",
            RoutineKind::Function => "FUNCTION",
        }
    }

    pub fn lower(self) -> &'static str {
        match self {
            RoutineKind::Procedure => "procedure",
            RoutineKind::Function => "function",
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutineDef {
    pub kind: RoutineKind,
    pub name: String,
    pub if_not_exists: bool,
    pub definer: Option<AccountIdent>,
    pub params: Vec<RoutineParam>,
    pub return_type: Option<DataType>,
    pub return_unsigned: bool,
    pub return_type_sql: Option<String>,
    pub characteristics: RoutineCharacteristics,
    pub body: RoutineBody,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TriggerDef {
    pub name: String,
    pub definer: Option<AccountIdent>,
    pub timing: TriggerTiming,
    pub event: TriggerEvent,
    pub table: String,
    pub order: Option<TriggerOrder>,
    pub body: RoutineBody,
    pub create_sql: String,
    pub if_not_exists: bool,
}

/// Owner-supplied schedule clause attached to `CREATE EVENT` or
/// `ALTER EVENT ... ON SCHEDULE`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum EventSchedule {
    /// `AT <ts-expr>`.  Stored as the raw textual expression — the
    /// executor evaluates it through the normal `eval_const_runtime`
    /// path so `AT NOW() + INTERVAL 5 MINUTE` works.
    At { at_expr: String },
    /// `EVERY n unit [STARTS expr] [ENDS expr]`.
    Every {
        interval_value: String,
        interval_unit: String,
        starts_expr: Option<String>,
        ends_expr: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum EventStatus {
    Enabled,
    Disabled,
    DisabledOnSlave,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum EventOnCompletion {
    NotPreserve,
    Preserve,
}

/// `CREATE [DEFINER=…] [IF NOT EXISTS] EVENT [OR REPLACE] <name>
///   ON SCHEDULE <schedule>
///   [ON COMPLETION [NOT] PRESERVE]
///   [ENABLE | DISABLE | DISABLE ON SLAVE]
///   [COMMENT '...']
///   DO <stmt>;`
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventDef {
    pub name: String,
    pub definer: Option<AccountIdent>,
    pub schedule: EventSchedule,
    pub on_completion: Option<EventOnCompletion>,
    pub status: Option<EventStatus>,
    pub comment: Option<String>,
    pub body_sql: String,
    pub create_sql: String,
    pub if_not_exists: bool,
    pub or_replace: bool,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlterEventStmt {
    pub name: String,
    pub definer: Option<AccountIdent>,
    pub schedule: Option<EventSchedule>,
    pub on_completion: Option<EventOnCompletion>,
    pub status: Option<EventStatus>,
    pub comment: Option<String>,
    pub rename_to: Option<String>,
    pub body_sql: Option<String>,
    pub create_sql: String,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(deny_unknown_fields)]
pub enum TriggerTiming {
    Before,
    After,
}

impl TriggerTiming {
    pub fn as_str(self) -> &'static str {
        match self {
            TriggerTiming::Before => "BEFORE",
            TriggerTiming::After => "AFTER",
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(deny_unknown_fields)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
}

impl TriggerEvent {
    pub fn as_str(self) -> &'static str {
        match self {
            TriggerEvent::Insert => "INSERT",
            TriggerEvent::Update => "UPDATE",
            TriggerEvent::Delete => "DELETE",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TriggerOrder {
    pub kind: TriggerOrderKind,
    pub other_trigger: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum TriggerOrderKind {
    Follows,
    Precedes,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutineParam {
    pub name: String,
    pub mode: RoutineParamMode,
    pub data_type: DataType,
    pub unsigned: bool,
    pub data_type_sql: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum RoutineParamMode {
    In,
    Out,
    InOut,
}

impl RoutineParamMode {
    pub fn as_str(self) -> &'static str {
        match self {
            RoutineParamMode::In => "IN",
            RoutineParamMode::Out => "OUT",
            RoutineParamMode::InOut => "INOUT",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutineCharacteristics {
    pub deterministic: bool,
    pub sql_data_access: RoutineSqlDataAccess,
    pub security: RoutineSecurity,
    pub comment: String,
}

/// Partial characteristic update used by `ALTER PROCEDURE/FUNCTION`.
///
/// CREATE applies MySQL defaults to omitted characteristics, while ALTER must
/// preserve every characteristic that was not explicitly mentioned.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutineCharacteristicsPatch {
    pub deterministic: Option<bool>,
    pub sql_data_access: Option<RoutineSqlDataAccess>,
    pub security: Option<RoutineSecurity>,
    pub comment: Option<String>,
}

impl Default for RoutineCharacteristics {
    fn default() -> Self {
        Self {
            deterministic: false,
            sql_data_access: RoutineSqlDataAccess::ContainsSql,
            security: RoutineSecurity::Definer,
            comment: String::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum RoutineSqlDataAccess {
    ContainsSql,
    NoSql,
    ReadsSqlData,
    ModifiesSqlData,
}

impl RoutineSqlDataAccess {
    pub fn as_str(self) -> &'static str {
        match self {
            RoutineSqlDataAccess::ContainsSql => "CONTAINS SQL",
            RoutineSqlDataAccess::NoSql => "NO SQL",
            RoutineSqlDataAccess::ReadsSqlData => "READS SQL DATA",
            RoutineSqlDataAccess::ModifiesSqlData => "MODIFIES SQL DATA",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum RoutineSecurity {
    Definer,
    Invoker,
}

impl RoutineSecurity {
    pub fn as_str(self) -> &'static str {
        match self {
            RoutineSecurity::Definer => "DEFINER",
            RoutineSecurity::Invoker => "INVOKER",
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutineBody {
    pub sql: String,
    pub statements: Vec<RoutineStmt>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum RoutineStmt {
    Block {
        label: Option<String>,
        statements: Vec<RoutineStmt>,
    },
    DeclareVar {
        names: Vec<String>,
        data_type: DataType,
        unsigned: bool,
        data_type_sql: String,
        default: Option<Expr>,
    },
    DeclareCondition {
        name: String,
        condition: RoutineCondition,
    },
    DeclareCursor {
        name: String,
        select_sql: String,
    },
    DeclareHandler {
        action: RoutineHandlerAction,
        conditions: Vec<RoutineCondition>,
        statement: Box<RoutineStmt>,
    },
    Set {
        assignments: Vec<(RoutineTarget, Expr)>,
    },
    SelectInto {
        select_sql: String,
        targets: Vec<RoutineTarget>,
    },
    If {
        branches: Vec<(Expr, Vec<RoutineStmt>)>,
        else_branch: Vec<RoutineStmt>,
    },
    Case {
        operand: Option<Expr>,
        branches: Vec<(Expr, Vec<RoutineStmt>)>,
        else_branch: Vec<RoutineStmt>,
    },
    Loop {
        label: Option<String>,
        statements: Vec<RoutineStmt>,
    },
    While {
        label: Option<String>,
        condition: Expr,
        statements: Vec<RoutineStmt>,
    },
    Repeat {
        label: Option<String>,
        statements: Vec<RoutineStmt>,
        until: Expr,
    },
    Leave(String),
    Iterate(String),
    Return(Expr),
    Open(String),
    Fetch {
        cursor: String,
        targets: Vec<RoutineTarget>,
    },
    Close(String),
    Signal {
        condition: RoutineCondition,
        message_text: Option<Expr>,
        mysql_errno: Option<Expr>,
    },
    Resignal {
        condition: Option<RoutineCondition>,
        message_text: Option<Expr>,
        mysql_errno: Option<Expr>,
    },
    Call(RoutineCall),
    Prepare {
        name: String,
        source: Expr,
    },
    Execute {
        name: String,
        using: Vec<RoutineTarget>,
    },
    Deallocate {
        name: String,
    },
    /// `GET [CURRENT | STACKED] DIAGNOSTICS ...`. `stacked` selects the
    /// diagnostics area of the condition being handled (only valid inside a
    /// handler). `condition` is `Some(n)` for `... CONDITION n <items>`
    /// (condition-area items) and `None` for statement-area items
    /// (`ROW_COUNT` / `NUMBER`).
    GetDiagnostics {
        stacked: bool,
        condition: Option<Expr>,
        items: Vec<(RoutineTarget, DiagItem)>,
    },
    Sql(String),
}

/// Diagnostics-area item selectable by `GET DIAGNOSTICS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum DiagItem {
    /// Statement area: rows affected by the previous statement.
    RowCount,
    /// Statement area: number of conditions.
    Number,
    /// Condition area: MySQL error number.
    MysqlErrno,
    /// Condition area: five-character SQLSTATE value.
    ReturnedSqlState,
    /// Condition area: human-readable message.
    MessageText,
    /// Condition area: origin of the SQLSTATE class.
    ClassOrigin,
    /// Condition area: origin of the SQLSTATE subclass.
    SubclassOrigin,
    ConstraintCatalog,
    ConstraintSchema,
    ConstraintName,
    CatalogName,
    SchemaName,
    TableName,
    ColumnName,
    CursorName,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum RoutineCondition {
    SqlException,
    SqlWarning,
    NotFound,
    MysqlError(u16),
    SqlState(String),
    Named(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum RoutineHandlerAction {
    Continue,
    Exit,
    Undo,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum RoutineTarget {
    Local(String),
    UserVariable(String),
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutineCall {
    pub name: String,
    pub args: Vec<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountIdent {
    pub user: String,
    pub host: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateUser {
    pub accounts: Vec<UserSpec>,
    pub if_not_exists: bool,
    pub is_role: bool,
    pub default_roles: Vec<AccountIdent>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UserSpec {
    pub account: AccountIdent,
    pub password: Option<String>,
    pub random_password: bool,
    pub plugin: Option<String>,
    /// True when the auth credential supplied as `IDENTIFIED WITH plugin AS '<hash>'`
    /// is already a plugin-formatted authentication string and must be stored
    /// verbatim (skipping the `password_auth_string` re-hash). False for the
    /// usual `IDENTIFIED BY 'plain'` / `IDENTIFIED WITH plugin BY 'plain'`
    /// path where the plaintext is hashed at write time.
    pub password_is_pre_hashed: bool,
    pub account_locked: bool,
    pub password_expired: bool,
    pub ssl_requirement: Option<UserSslRequirement>,
    pub resource_options: ResourceOptions,
    pub failed_login_attempts: Option<u16>,
    pub password_lock_time_days: Option<u16>,
    pub password_lifetime_days: Option<u16>,
    /// Per-account password-history override. `None` means inherit the
    /// global `password_history` value.
    pub password_history: Option<u16>,
    /// Per-account password-reuse interval override. `None` means inherit
    /// the global `password_reuse_interval` value.
    pub password_reuse_interval_days: Option<u16>,
    /// Per-account current-password requirement. `None` means inherit the
    /// global `password_require_current` value.
    pub password_require_current: Option<bool>,
    /// `CREATE USER ... COMMENT '<text>'` (MySQL 8.0.14+). Merged into the
    /// account's `user_attributes` JSON at `metadata.comment`, mirroring
    /// `AlterUser::comment`.
    pub comment: Option<String>,
    /// `CREATE USER ... ATTRIBUTE '<json>'` (MySQL 8.0.14+). Merged into the
    /// account's `user_attributes` JSON, mirroring `AlterUser::attribute_json`.
    pub attribute_json: Option<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlterUser {
    pub account: AccountIdent,
    pub current_user: bool,
    /// L14: `ALTER USER IF EXISTS` (MySQL 8.0.29+). When true the executor
    /// emits warning 3162 ("Authorization ID '...' does not exist.") and
    /// returns success instead of failing with 1396.
    pub if_exists: bool,
    pub password: Option<String>,
    pub plugin: Option<String>,
    /// True for `IDENTIFIED WITH plugin AS '<hash>'` — store verbatim.
    pub password_is_pre_hashed: bool,
    /// Old password supplied as `REPLACE '<old>'` (L2). When `Some`, the
    /// executor must verify it matches the current authentication string
    /// before applying the change.
    pub replace_old_password: Option<String>,
    /// True for `IDENTIFIED BY RANDOM PASSWORD` (L4). When set, the
    /// executor generates a random password and returns it to the client.
    pub random_password: bool,
    /// True for `IDENTIFIED BY ... RETAIN CURRENT PASSWORD` — preserve the
    /// current authentication string as the secondary (L3).
    pub retain_current_password: bool,
    /// True for `ALTER USER ... DISCARD OLD PASSWORD` — drop the secondary
    /// authentication string.
    pub discard_old_password: bool,
    pub account_locked: Option<bool>,
    pub password_expired: Option<bool>,
    pub ssl_requirement: Option<UserSslRequirement>,
    pub resource_options: ResourceOptionsPatch,
    pub failed_login_attempts: Option<u16>,
    pub password_lock_time_days: Option<u16>,
    pub password_lifetime_days: Option<AccountPolicy<u16>>,
    /// `PASSWORD HISTORY N` per-account override (L1).
    pub password_history: Option<AccountPolicy<u16>>,
    /// `PASSWORD REUSE INTERVAL N DAY` (L1).
    pub password_reuse_interval_days: Option<AccountPolicy<u16>>,
    /// `PASSWORD REQUIRE CURRENT [OPTIONAL]` (L2). `Some(true)` → required,
    /// `Some(false)` → optional, `None` → unchanged.
    pub password_require_current: Option<AccountPolicy<bool>>,
    /// `COMMENT '...'` (L5) — merged into user_attributes_json under
    /// `metadata.comment`.
    pub comment: Option<String>,
    /// `ATTRIBUTE '<json>'` (L5) — JSON merge-patch into user_attributes_json.
    pub attribute_json: Option<String>,
}

/// ALTER USER policy patch. `None` on the containing field means the clause
/// was absent, `Default` clears a per-account override, and `Value` installs
/// an explicit override. Keeping these three states distinct prevents the
/// parser from accepting `... DEFAULT` while the executor silently leaves the
/// old value unchanged.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum AccountPolicy<T> {
    Default,
    Value(T),
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceOptions {
    pub max_queries_per_hour: u32,
    pub max_updates_per_hour: u32,
    pub max_connections_per_hour: u32,
    pub max_user_connections: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceOptionsPatch {
    pub max_queries_per_hour: Option<u32>,
    pub max_updates_per_hour: Option<u32>,
    pub max_connections_per_hour: Option<u32>,
    pub max_user_connections: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum UserSslRequirement {
    None,
    Ssl,
    X509,
    Specified {
        cipher: Option<String>,
        issuer: Option<String>,
        subject: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Grant {
    pub kind: GrantKind,
    pub targets: Vec<AccountIdent>,
    pub grant_option: bool,
    pub grant_as: Option<GrantAs>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrantAs {
    pub account: AccountIdent,
    pub roles: Option<SetRole>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum GrantKind {
    Privileges {
        privileges: Vec<PrivilegeSpec>,
        scope: GrantScope,
    },
    Roles(Vec<AccountIdent>),
    Proxy(AccountIdent),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrivilegeSpec {
    pub name: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Revoke {
    pub kind: GrantKind,
    pub targets: Vec<AccountIdent>,
    /// P0-5: `REVOKE ALL PRIVILEGES [, GRANT OPTION] FROM <user>` (no `ON`
    /// clause) means "strip every privilege on every scope from the target
    /// accounts". When set, `kind`'s payload is a placeholder and the
    /// executor purges all grant + restriction rows for the targets.
    pub revoke_all_from_target: bool,
    /// P1-5: `REVOKE GRANT OPTION FOR <priv> ON <scope> FROM <user>` means
    /// "clear the grant_option bit on matching rows" rather than delete
    /// them. Only meaningful with `GrantKind::Privileges`.
    pub grant_option_only: bool,
    pub if_exists: bool,
    pub ignore_unknown_user: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum GrantScope {
    Global,
    Database(String),
    Table {
        database: Option<String>,
        table: String,
    },
    Routine {
        database: Option<String>,
        name: String,
        kind: RoutineKind,
    },
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum SetRole {
    All,
    /// `SET ROLE ALL EXCEPT r1, r2, ...` — activate every granted role
    /// minus the named set. Unmatched names are NOT an error (matches
    /// MySQL 8.0: the exception list is purely subtractive).
    AllExcept(Vec<AccountIdent>),
    None,
    Default,
    Roles(Vec<AccountIdent>),
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum SetDefaultRole {
    All {
        to: Vec<AccountIdent>,
    },
    None {
        to: Vec<AccountIdent>,
    },
    Roles {
        roles: Vec<AccountIdent>,
        to: Vec<AccountIdent>,
    },
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetAssignment {
    pub scope: Option<SetScope>,
    pub name: String,
    pub value: Expr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum SetScope {
    Session,
    Global,
    /// MySQL 8.0 `SET PERSIST`: update the runtime global *and* persist the
    /// new value to `<data_dir>/system/mysqld-auto.cnf` so it survives a
    /// restart.
    Persist,
    /// MySQL 8.0 `SET PERSIST_ONLY`: persist the value to the on-disk file
    /// without touching the running value. Permitted on `READ_ONLY`
    /// variables as well (this is the only `SET` form that bypasses the
    /// read-only guard).
    PersistOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum KillTarget {
    Connection,
    Query,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum TransactionIsolation {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

/// One characteristic in a `SET TRANSACTION ...` list. MySQL 8.0 accepts
/// any mix of `ISOLATION LEVEL X`, `READ ONLY`, and `READ WRITE`, comma
/// separated; we stash them in the AST as a Vec so the dispatcher can
/// apply each one to the same scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum TransactionCharacteristic {
    Isolation(TransactionIsolation),
    /// `READ ONLY` (true) / `READ WRITE` (false).
    ReadOnly(bool),
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateTable {
    pub name: String,
    pub if_not_exists: bool,
    pub temporary: bool,
    pub on_commit: Option<TempTableOnCommit>,
    pub columns: Vec<ColumnDef>,
    pub column_comments: Vec<(String, String)>,
    pub primary_key: Vec<String>,
    pub indexes: Vec<IndexDef>,
    pub checks: Vec<CheckConstraintDef>,
    pub foreign_keys: Vec<ForeignKeyDef>,
    pub auto_increment_start: Option<u64>,
    pub default_charset: Option<String>,
    pub default_collation: Option<String>,
    pub table_comment: String,
    /// Oracle-style `PCTFREE = N` from the table options clause. `None`
    /// = follow process-wide default. Clamped to `[0, 50]` at parse
    /// time. Phase 4: persisted as part of the dictionary table's
    /// `table_options_json`.
    pub pctfree: Option<u8>,
    /// Oracle-style `PCTUSED = N`. Same semantics as `pctfree`.
    pub pctused: Option<u8>,
    /// `PARTITION BY ...` clause captured from the original SQL by the
    /// pre-parser (`parser_partition`). `None` means the table is not
    /// partitioned. Carried as the parser AST shape; storage converts
    /// it into [`crate::storage::partitioning::PartitionInfo`] at
    /// CREATE TABLE time.
    pub partition: Option<crate::parser_partition::PartitionDef>,
    /// MySQL table options that are accepted, persisted, and echoed by
    /// SHOW CREATE TABLE / information_schema but carry no physical
    /// behaviour in mydb (ROW_FORMAT, KEY_BLOCK_SIZE, STATS_*, MAX_ROWS,
    /// …) — matching InnoDB, which treats most of them as metadata-only
    /// hints. Keys are canonical uppercase; values are validated at
    /// parse time (see `accept_generic_table_option`).
    pub table_options: Vec<(String, String)>,
    /// The requested storage engine name from `ENGINE=...`, if any. mydb
    /// is single-engine, so this is metadata only — recorded so SHOW
    /// CREATE TABLE / SHOW TABLE STATUS echo the declared engine while
    /// every table physically uses mydb's MVCC engine. `None` = no ENGINE
    /// clause.
    pub engine: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum TempTableOnCommit {
    PreserveRows,
    DeleteRows,
    Drop,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlterTable {
    pub table: String,
    /// Statement-level optimizer hints written immediately after `ALTER`,
    /// for example `ALTER /*+ PARALLEL(8) */ TABLE ...`.  DDL currently
    /// consumes only PARALLEL / NO_PARALLEL; retaining the common QueryHint
    /// shape keeps parsing and diagnostics aligned with SELECT / LOAD DATA.
    pub query_hints: Vec<QueryHint>,
    pub hint_warnings: Vec<String>,
    /// One or more actions in declaration order. Most ALTER TABLE
    /// statements carry a single action (column add/drop, index ops);
    /// MySQL also allows several comma-separated actions, which mydb
    /// uses to fuse multi-index builds (`ADD INDEX a, ADD INDEX b`)
    /// onto one heap scan. The executor iterates in order and
    /// dispatches batched paths when the shape is amenable.
    pub actions: Vec<AlterAction>,
    /// Partition-related actions captured by `parser_partition` before
    /// sqlparser runs. Empty when the ALTER TABLE statement carries no
    /// partition operations. These are executed *after* the
    /// non-partition `actions`. Execution rejects mixed statements without
    /// an atomic plan before either action set changes persistent state.
    pub partition_actions: Vec<crate::parser_partition::PartitionAction>,
    /// User-supplied `ALGORITHM = INSTANT|INPLACE|COPY|DEFAULT` hint.
    /// `Default` means unspecified — the executor picks the fastest
    /// available algorithm. `Instant` is a contract: the user is
    /// asserting the operation must complete without a heap rewrite,
    /// and the executor errors out at dispatch time if the operation
    /// requires more than a catalog edit (e.g. ADD INDEX). MySQL
    /// 8.0-compatible: see <https://dev.mysql.com/doc/refman/8.0/en/alter-table.html>.
    pub algorithm: AlterAlgorithm,
    /// User-supplied `LOCK = DEFAULT|NONE|SHARED|EXCLUSIVE` hint.
    /// `Default` means unspecified. The executor validates this as a
    /// concurrency contract instead of silently ignoring it.
    pub lock: AlterLock,
}

/// Resolution semantics:
/// * `Default` — choose the least physical work the execution plan supports.
/// * `Instant` — no row scan or rewrite; generated columns and index rebuilds
///   do not qualify merely because the syntax is ADD/DROP COLUMN.
/// * `Inplace` — row validation or index construction is allowed, without a
///   shadow heap copy. Encoding-compatible domain tightening can use this.
/// * `Copy` — explicitly request a physical shadow rewrite. Unsupported action
///   combinations fail before mutation rather than silently taking INSTANT.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum AlterAlgorithm {
    #[default]
    Default,
    Instant,
    Inplace,
    Copy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum AlterLock {
    #[default]
    Default,
    None,
    Shared,
    Exclusive,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum AlterAction {
    /// Rebuild the physical generation without changing the logical schema.
    Rebuild,
    AddColumn(ColumnDef),
    AddIndex(IndexDef),
    AddCheck(CheckConstraintDef),
    AddForeignKey(ForeignKeyDef),
    MoveColumn {
        column: String,
        position: ColumnPosition,
    },
    SetColumnComment {
        column: String,
        comment: String,
    },
    SetColumnDefault {
        column: String,
        default: Option<Expr>,
    },
    SetTableOptions {
        default_charset: Option<String>,
        default_collation: Option<String>,
        comment: Option<String>,
        /// Accepted metadata-only options (ROW_FORMAT, MAX_ROWS, …).
        /// Value `DEFAULT` resets the option back to "not set".
        generic: Vec<(String, String)>,
    },
    /// Oracle-style per-table physical-storage knobs. `Some(N)` clamps
    /// the value to `[0, 50]` at parse time; `None` means "not in this
    /// ALTER statement". Either side can be set independently.
    SetStorageProperties {
        pctfree: Option<u8>,
        pctused: Option<u8>,
    },
    DropColumn(String),
    DropIndex(String),
    DropPrimaryKey,
    DropCheck {
        name: String,
        if_exists: bool,
        /// Only DROP CONSTRAINT may resolve a foreign key by this name.
        include_foreign_keys: bool,
    },
    DropForeignKey {
        name: String,
    },
    AlterIndexVisibility {
        name: String,
        visible: bool,
    },
    AlterCheck {
        name: String,
        enforced: bool,
    },
    ModifyColumn(ColumnDef),
    ChangeColumn {
        old_name: String,
        new_def: ColumnDef,
    },
    RenameColumn {
        old_name: String,
        new_name: String,
    },
    RenameIndex {
        old_name: String,
        new_name: String,
    },
    RenameTable {
        new_name: String,
    },
    SetAutoIncrement(u64),
    /// Accepted MySQL action with no physical effect in mydb
    /// (DISABLE/ENABLE KEYS, ORDER BY): executes as a no-op and
    /// surfaces the optional diagnostic so the client sees the same
    /// SHOW WARNINGS output as InnoDB.
    Noop {
        warning: Option<(String, u16, String)>,
    },
    /// MySQL 8.0.23+ `ALTER TABLE t ALTER COLUMN c SET {VISIBLE|INVISIBLE}`.
    SetColumnVisibility {
        column: String,
        invisible: bool,
    },
    /// `ALTER TABLE t CONVERT TO CHARACTER SET cs [COLLATE coll]` —
    /// table default + every text-typed column converts. mydb stores
    /// strings as UTF-8 regardless of declared charset, so this is a
    /// catalog-only (INSTANT) operation; comparison semantics follow
    /// the new collation immediately.
    ConvertToCharset {
        charset: String,
        collation: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum TableDropBehavior {
    Restrict,
    Cascade,
}

/// `ALTER SEQUENCE ... RESTART` operand: reset the live counter immediately
/// (transactional-looking but, like all mydb DDL, auto-committed).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum SequenceRestart {
    /// `RESTART` with no value — go back to the recorded `START` value.
    ToStart,
    /// `RESTART WITH n`.
    To(i64),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ColumnPosition {
    First,
    After(String),
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub unsigned: bool,
    /// MySQL display-only `ZEROFILL` attribute (implies UNSIGNED).
    /// Deprecated since 8.0.17 but still accepted; mydb persists the
    /// flag, echoes it from SHOW CREATE / SHOW COLUMNS, and zero-pads
    /// text-protocol output to the type's default display width.
    pub zerofill: bool,
    /// MySQL 8.0.23+ `INVISIBLE` column attribute: hidden from
    /// `SELECT *` and INSERT-without-column-list, addressable by name.
    pub invisible: bool,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub auto_increment: bool,
    /// Column DEFAULT. Plain literal defaults like `DEFAULT 42` and the
    /// shortcut `DEFAULT CURRENT_TIMESTAMP` are stored as-is. MySQL 8.0.13+
    /// `DEFAULT (<expr>)` (parenthesised expression form) is wrapped as
    /// `Expr::Function { name: __DEFAULT_EXPR, args: [inner] }` — see
    /// [`DEFAULT_EXPR_SENTINEL`] — so downstream layers (catalog
    /// `column_def_to_schema`) can tell them apart without growing the
    /// struct (147 ColumnDef construction sites in tests reuse the existing
    /// shape).
    pub default: Option<Expr>,
    pub on_update: Option<Expr>,
    pub checks: Vec<CheckConstraintDef>,
    pub generated: Option<GeneratedColumnDef>,
    pub charset: Option<String>,
    pub collation: Option<String>,
}

/// Sentinel function name used to wrap a `DEFAULT (<expr>)` clause so the
/// distinction between literal-default and expression-default survives the
/// `ColumnDef` round-trip without growing the struct. The wrapper is unwrapped
/// in `column_def_to_schema`.
pub const DEFAULT_EXPR_SENTINEL: &str = "__DEFAULT_EXPR";
/// Internal collation marker for MySQL's post-type `BINARY` attribute
/// (`VARCHAR(20) BINARY`). The DDL layer resolves it to the binary collation
/// of the effective column/table charset once table defaults are known.
pub const BINARY_ATTRIBUTE_COLLATION: &str = "__MYDB_BINARY_ATTRIBUTE";

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GeneratedColumnDef {
    pub expr: Expr,
    pub stored: bool,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckConstraintDef {
    pub name: Option<String>,
    pub expr: Expr,
    pub enforced: bool,
    /// `Some(column)` for column-level `CHECK` definitions. The storage
    /// layer uses this to enforce MySQL's rule that column checks only
    /// reference their own column.
    pub column: Option<String>,
}

/// Parser-level referential action. Distinct from
/// [`crate::storage::ReferentialAction`] because it must carry
/// `SetDefault` so the DDL layer can emit MySQL's standard error 1215
/// ("...SET DEFAULT...") rather than silently accepting it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ReferentialActionDef {
    Restrict,
    Cascade,
    SetNull,
    NoAction,
    SetDefault,
}

/// A table-level `FOREIGN KEY (...) REFERENCES t (...)` definition, as
/// seen at parse time. `referenced_table` keeps the object name exactly
/// as written (possibly `db.table`); the DDL layer resolves the schema.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForeignKeyDef {
    pub name: Option<String>,
    pub columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
    /// `None` means the clause was omitted (defaults to `NO ACTION`).
    pub on_delete: Option<ReferentialActionDef>,
    pub on_update: Option<ReferentialActionDef>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum DataType {
    TinyInt,
    SmallInt,
    MediumInt,
    Int,
    BigInt,
    Bool,
    /// `FLOAT[(M[,D])]` — `(M, D)` is the optional declared display
    /// width and decimals. MySQL 8.0 deprecates the form but still
    /// preserves it on the catalog and reports it back through
    /// `SHOW COLUMNS` and `information_schema.columns.numeric_*`.
    /// Storage is still 4-byte IEEE-754 regardless of `(M, D)`.
    Float(Option<(u8, u8)>),
    /// `DOUBLE[(M[,D])]` — see [`DataType::Float`]. Storage is still
    /// 8-byte IEEE-754.
    Double(Option<(u8, u8)>),
    Decimal(Option<(u8, u8)>),
    Char(Option<usize>),
    VarChar(Option<usize>),
    Binary(Option<usize>),
    VarBinary(Option<usize>),
    /// TEXT family; carries the MySQL byte cap (TINYTEXT=255 …
    /// LONGTEXT=4G-1, plain TEXT=65535).
    Text(u32),
    /// BLOB family; carries the MySQL byte cap (TINYBLOB=255 …
    /// LONGBLOB=4G-1, plain BLOB=65535).
    Blob(u32),
    Date,
    /// `TIME[(fsp)]` — fractional seconds precision in 0..=6. MySQL 8.0
    /// preserves fsp on the catalog (`SHOW COLUMNS` reports `time(3)`)
    /// and uses it to truncate the i64-microsecond payload on write and
    /// to size the binary-protocol length byte on the way out.
    Time {
        fsp: u8,
    },
    /// `DATETIME[(fsp)]` — see [`DataType::Time`].
    DateTime {
        fsp: u8,
    },
    /// `TIMESTAMP[(fsp)]` — see [`DataType::Time`].
    Timestamp {
        fsp: u8,
    },
    Year,
    Json,
    /// `GEOMETRY` / `POINT` / `POLYGON` / … with the declared subtype and an
    /// optional column SRID constraint.
    Geometry(crate::storage::schema::GeometrySubtype, Option<u32>),
    /// `VECTOR(n)` with an optional declared dimension (`None` = unconstrained).
    Vector(Option<u32>, crate::vector::VectorDataType),
    Enum(Vec<String>),
    Set(Vec<String>),
    Bit(Option<usize>),
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexDef {
    pub name: Option<String>,
    /// For column key parts, the column name. For MySQL 8.0 functional
    /// key parts (`((expr))`), the canonical SQL text of the expression
    /// (the real shape is carried by `key_expressions[i]`). The dual
    /// usage keeps `columns` / `column_prefix_lengths` /
    /// `column_descending` / `key_expressions` indexable by the same
    /// `i`.
    pub columns: Vec<String>,
    pub column_prefix_lengths: Vec<Option<usize>>,
    pub column_descending: Vec<bool>,
    /// Parallel to `columns`. `Some(sql)` marks a functional key part
    /// (the canonical SQL text of the expression); `None` is a plain
    /// column reference. Empty Vec is the conventional shape for
    /// non-functional indexes.
    pub key_expressions: Vec<Option<String>>,
    /// Parallel to `key_expressions`. `true` marks a MySQL 8.0
    /// multi-valued index part (`CAST(json_path AS <scalar> ARRAY)`).
    pub key_expression_multi_valued: Vec<bool>,
    pub unique: bool,
    /// Names of `INCLUDE (...)` columns for inline `CREATE TABLE` keys
    /// and `ALTER TABLE ADD INDEX` (standard SQL / SQL-Server spelling:
    /// `INDEX idx (k) INCLUDE (a, b)`). Empty when the clause is absent.
    /// sqlparser 0.62 does not understand `INCLUDE` in these positions,
    /// so [`strip_inline_index_includes`] removes the clause before the
    /// parse and [`apply_inline_index_includes`] re-attaches it here by
    /// sentinel key part. Forbidden on `UNIQUE` / `PRIMARY` keys (the
    /// uniqueness constraint must apply to the declared key alone).
    pub included_columns: Vec<String>,
    pub index_type: String,
    pub visible: bool,
    pub comment: String,
    /// `WITH PARSER` name for a FULLTEXT index ("" = default tokenizer, "ngram" = ngram).
    /// Always empty for non-FULLTEXT indexes. Extracted from SQL by [`strip_fulltext_parsers`] and
    /// injected by [`apply_fulltext_parsers`] after conversion.
    pub fulltext_parser: String,
}

impl IndexDef {
    /// Whether this is a FULLTEXT index (decided at parse time from an index-type string of `FULLTEXT`).
    pub fn is_fulltext(&self) -> bool {
        self.index_type.eq_ignore_ascii_case("FULLTEXT")
    }

    /// Return the concrete vector algorithm encoded by the parser-level
    /// index type. Callers must first establish that this is a vector index.
    pub fn vector_algo(&self) -> crate::storage::schema::VectorAlgo {
        if self.index_type.eq_ignore_ascii_case("IVF") {
            crate::storage::schema::VectorAlgo::Ivf
        } else if self.index_type.eq_ignore_ascii_case("IVF_SQ8") {
            crate::storage::schema::VectorAlgo::IvfSq8
        } else {
            crate::storage::schema::VectorAlgo::Hnsw
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateIndex {
    pub name: String,
    pub table: String,
    pub algorithm: AlterAlgorithm,
    pub lock: AlterLock,
    /// Statement-level hints written as
    /// `CREATE /*+ PARALLEL(n) */ [UNIQUE] INDEX ...`.
    pub query_hints: Vec<QueryHint>,
    pub hint_warnings: Vec<String>,
    pub columns: Vec<String>,
    pub column_prefix_lengths: Vec<Option<usize>>,
    pub column_descending: Vec<bool>,
    /// See [`IndexDef::key_expressions`].
    pub key_expressions: Vec<Option<String>>,
    /// See [`IndexDef::key_expression_multi_valued`].
    pub key_expression_multi_valued: Vec<bool>,
    pub unique: bool,
    /// Names of `INCLUDE (...)` columns, if specified. Empty when the
    /// statement omits the clause. Standard SQL spelling: `CREATE
    /// INDEX ... ON t (k) INCLUDE (a, b)`. Forbidden when `unique` is
    /// true so the unique constraint stays on the declared key.
    pub included_columns: Vec<String>,
    pub index_type: String,
    pub visible: bool,
    pub comment: String,
    /// `WITH PARSER` name for a FULLTEXT index ("" = default tokenizer, "ngram" = ngram).
    /// Always empty for non-FULLTEXT indexes. The standalone `CREATE FULLTEXT INDEX` path
    /// is extracted by [`strip_fulltext_parsers`] and injected by [`apply_fulltext_parsers`],
    /// aligned with the inline / ALTER [`IndexDef::fulltext_parser`].
    pub fulltext_parser: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ConvertedIndexColumns {
    pub(super) names: Vec<String>,
    pub(super) prefix_lengths: Vec<Option<usize>>,
    pub(super) descending: Vec<bool>,
    /// Parallel to `names`. `Some(sql)` for a functional key part; the
    /// canonical SQL text comes straight from sqlparser's `Expr::Display`
    /// so it round-trips through SHOW CREATE TABLE.
    pub(super) key_expressions: Vec<Option<String>>,
    pub(super) key_expression_multi_valued: Vec<bool>,
}

impl ConvertedIndexColumns {
    pub(super) fn bare(names: Vec<String>) -> Self {
        let len = names.len();
        Self {
            names,
            prefix_lengths: vec![None; len],
            descending: vec![false; len],
            key_expressions: vec![None; len],
            key_expression_multi_valued: vec![false; len],
        }
    }

    pub(super) fn has_prefix_or_desc(&self) -> bool {
        self.prefix_lengths.iter().any(Option::is_some) || self.descending.iter().any(|v| *v)
    }

    pub(super) fn has_expressions(&self) -> bool {
        self.key_expressions.iter().any(Option::is_some)
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Insert {
    pub table: String,
    /// PostgreSQL target-table alias (`INSERT INTO t AS x ...`).  This is
    /// distinct from MySQL's post-VALUES row alias in `insert_alias`.
    pub target_alias: Option<String>,
    pub columns: Option<Vec<String>>,
    pub source: InsertSource,
    /// Internal executor hook used after rewriting an updatable view INSERT.
    /// The parser never populates this; DML execution evaluates it against
    /// each post-image row for WITH CHECK OPTION.
    pub view_check_predicate: Option<Expr>,
    pub on_duplicate_key_update: Option<Vec<Assignment>>,
    pub insert_alias: Option<InsertAlias>,
    pub ignore: bool,
    pub replace: bool,
    /// `INSERT DELAYED ...`. mydb (like InnoDB) treats DELAYED as a no-op but
    /// surfaces deprecation warning 1287 once per statement so applications
    /// see the same diagnostic they would on MySQL 8.0.
    pub delayed: bool,
    /// M5.C2 — `INSERT INTO t PARTITION (p0, p1) VALUES (...)`. Empty = no
    /// restriction. Non-empty asserts every routed row's chosen partition is
    /// in this list; otherwise the executor raises MySQL error 1748.
    pub partitions: Vec<String>,
    pub query_hints: Vec<QueryHint>,
    pub hint_warnings: Vec<String>,
    pub returning: Option<ReturningClause>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InsertAlias {
    pub row_alias: String,
    pub column_aliases: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LockTableSpec {
    pub table: String,
    pub alias: Option<String>,
    pub mode: LockTableMode,
    pub low_priority_write: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum LockTableMode {
    Read,
    Write,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum InsertSource {
    Values(Vec<Vec<Expr>>),
    Query(QueryExpr),
}

/// PostgreSQL-style result projection attached to INSERT/UPDATE/DELETE.
/// `old_alias` / `new_alias` implement PostgreSQL 18's
/// `RETURNING WITH (OLD AS o, NEW AS n)` namespace aliases.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReturningClause {
    pub projection: Vec<SelectItem>,
    pub old_alias: Option<String>,
    pub new_alias: Option<String>,
    /// Parser bridge for PostgreSQL's pre-column-list INSERT target alias.
    /// `convert_insert` moves this into `Insert::target_alias`.
    pub target_alias: Option<String>,
}

/// A single entry in a LOAD DATA / LOAD XML column list: either a real
/// table column (the field value is stored directly) or a user variable
/// (`@var`) that the field value is bound to for later consumption by a
/// `SET` clause. `@var` targets do not occupy a table-column slot.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum LoadTarget {
    Column(String),
    UserVar(String),
}

/// `LOW_PRIORITY | CONCURRENT` prefix. Both are pure scheduling hints in
/// MySQL and are accepted-and-ignored here (no-op, per project policy).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum LoadPriority {
    #[default]
    None,
    LowPriority,
    Concurrent,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoadData {
    pub local: bool,
    pub priority: LoadPriority,
    /// `true` for `LOAD XML`, `false` for `LOAD DATA`.
    pub is_xml: bool,
    pub file_path: String,
    pub table: String,
    pub partitions: Vec<String>,
    /// Input file character set (`CHARACTER SET x`). `None` = use the
    /// connection default; `Some("binary")` = no transcoding.
    pub charset: Option<String>,
    /// Column/user-var target list. `None` = load into all table columns
    /// in declaration order.
    pub targets: Option<Vec<LoadTarget>>,
    /// `SET col = expr` assignments applied after field binding. `DEFAULT`
    /// is carried as `Expr::Default(None)`.
    pub set_clauses: Vec<(String, Expr)>,
    pub query_hints: Vec<QueryHint>,
    pub hint_warnings: Vec<String>,
    pub ignore_rows: usize,
    pub field_terminated_by: String,
    pub field_enclosed_by: Option<char>,
    pub field_optionally_enclosed: bool,
    /// Escape character. `None` = `ESCAPED BY ''` (escape processing
    /// disabled). `Some('\\')` is the MySQL default.
    pub field_escaped_by: Option<char>,
    pub line_terminated_by: String,
    pub line_starting_by: String,
    /// `ROWS IDENTIFIED BY '<tag>'` for LOAD XML. `None` otherwise.
    pub xml_rows_identified_by: Option<String>,
    pub ignore_duplicates: bool,
    pub replace: bool,
    pub data: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum SelectIntoFileKind {
    Outfile,
    Dumpfile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum SelectIntoFilePosition {
    /// Historical `SELECT expr INTO OUTFILE ... FROM ...` spelling.
    AfterSelectList,
    /// Immediately before `FOR UPDATE` / `LOCK IN SHARE MODE`.
    BeforeLockingClause,
    /// The preferred statement-tail position.
    StatementEnd,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelectIntoFileOptions {
    pub kind: SelectIntoFileKind,
    pub file_path: String,
    pub charset: Option<String>,
    pub field_terminated_by: String,
    pub field_enclosed_by: Option<char>,
    pub field_optionally_enclosed: bool,
    /// `None` represents `ESCAPED BY ''`.
    pub field_escaped_by: Option<char>,
    pub line_terminated_by: String,
    pub line_starting_by: String,
    pub position: SelectIntoFilePosition,
}

impl LoadData {
    /// The pure-column-name view of the target list, or `None` if there is
    /// no explicit list. Returns an error is any target is a `@user_var`
    /// (which has no column-name equivalent) — callers that reach here have
    /// already ensured the general path handles user vars.
    pub fn column_names(&self) -> Option<Vec<String>> {
        self.targets.as_ref().map(|targets| {
            targets
                .iter()
                .filter_map(|t| match t {
                    LoadTarget::Column(name) => Some(name.clone()),
                    LoadTarget::UserVar(_) => None,
                })
                .collect()
        })
    }

    /// True when the target list contains any `@user_var` entry.
    pub fn has_user_var_targets(&self) -> bool {
        self.targets
            .as_ref()
            .is_some_and(|t| t.iter().any(|t| matches!(t, LoadTarget::UserVar(_))))
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WithCte {
    pub ctes: Vec<CteDef>,
    pub recursive: bool,
    pub body: Box<Statement>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CteDef {
    pub name: String,
    pub columns: Vec<String>,
    pub is_recursive: bool,
    pub query: QueryExpr,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetOperation {
    pub op: SetOp,
    pub all: bool,
    pub left: QueryExpr,
    pub right: QueryExpr,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<LimitValue>,
    pub offset: Option<LimitValue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum SetOp {
    Union,
    Intersect,
    Except,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum LockClauseMode {
    ForUpdate,
    ForShare,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum LockWaitMode {
    Default,
    NoWait,
    SkipLocked,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LockTarget {
    pub table: String,
    pub mode: LockClauseMode,
    pub wait: LockWaitMode,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LockClause {
    pub mode: LockClauseMode,
    pub wait: LockWaitMode,
    /// Empty means every source of this query block; otherwise only these aliases.
    pub targets: Vec<LockTarget>,
}

impl LockClause {
    pub(crate) fn for_reference(&self, reference: &TableRef) -> Option<Self> {
        if self.targets.is_empty() {
            return Some(Self {
                mode: self.mode,
                wait: self.wait,
                targets: Vec::new(),
            });
        }
        self.targets
            .iter()
            .find(|target| {
                reference.alias.as_ref().map_or_else(
                    || {
                        target.table.eq_ignore_ascii_case(&reference.name)
                            || target.table.eq_ignore_ascii_case(
                                reference.name.rsplit('.').next().unwrap_or(&reference.name),
                            )
                    },
                    |alias| target.table.eq_ignore_ascii_case(alias),
                )
            })
            .map(|target| Self {
                mode: target.mode,
                wait: target.wait,
                targets: Vec::new(),
            })
    }
}

/// A query-only syntax node. DML/DDL and protocol statements cannot inhabit
/// derived tables, CTE definitions or set-operation operands.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum QueryExpr {
    Select(Box<Select>),
    SetOp(Box<SetOperation>),
    WithCte(Box<QueryWithCte>),
}

// Set-operation chains are left-deep, so structural traits recurse once per
// branch. These match the derived implementations but extend the stack.
impl Clone for QueryExpr {
    fn clone(&self) -> Self {
        crate::util::stack::ensure_stack(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::ParserTransient,
            || match self {
                Self::Select(select) => Self::Select(select.clone()),
                Self::SetOp(set) => Self::SetOp(set.clone()),
                Self::WithCte(with) => Self::WithCte(with.clone()),
            },
        )
    }
}

impl PartialEq for QueryExpr {
    fn eq(&self, other: &Self) -> bool {
        crate::util::stack::ensure_stack(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::ParserTransient,
            || match (self, other) {
                (Self::Select(left), Self::Select(right)) => left == right,
                (Self::SetOp(left), Self::SetOp(right)) => left == right,
                (Self::WithCte(left), Self::WithCte(right)) => left == right,
                _ => false,
            },
        )
    }
}

impl std::fmt::Debug for QueryExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        crate::util::stack::ensure_stack(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::ParserTransient,
            || match self {
                Self::Select(select) => f.debug_tuple("Select").field(select).finish(),
                Self::SetOp(set) => f.debug_tuple("SetOp").field(set).finish(),
                Self::WithCte(with) => f.debug_tuple("WithCte").field(with).finish(),
            },
        )
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryWithCte {
    pub ctes: Vec<CteDef>,
    pub recursive: bool,
    pub body: QueryExpr,
}

/// One representation for every query slot; retained as a spelling for FROM.
pub type SubQuery = QueryExpr;

impl QueryExpr {
    pub fn as_select(&self) -> Option<&Select> {
        match self {
            Self::Select(select) => Some(select),
            _ => None,
        }
    }
    pub fn as_select_mut(&mut self) -> Option<&mut Select> {
        match self {
            Self::Select(select) => Some(select),
            _ => None,
        }
    }
    pub fn leftmost_select(&self) -> Option<&Select> {
        let mut query = self;
        loop {
            match query {
                Self::Select(select) => return Some(select),
                Self::SetOp(set) => query = &set.left,
                Self::WithCte(with) => query = &with.body,
            }
        }
    }
    /// A move-only boundary into the statement dispatcher, with no SQL
    /// rendering or deep copy. Recursive query children keep their types.
    pub fn into_statement(self) -> Statement {
        match self {
            Self::Select(select) => Statement::Select(*select),
            Self::SetOp(set) => Statement::SetOperation(*set),
            Self::WithCte(with) => Statement::WithCte(WithCte {
                ctes: with.ctes,
                recursive: with.recursive,
                body: Box::new(with.body.into_statement()),
            }),
        }
    }
}

impl TryFrom<Statement> for QueryExpr {
    type Error = ParseError;
    fn try_from(statement: Statement) -> Result<Self, ParseError> {
        match statement {
            Statement::Select(select) => Ok(Self::Select(Box::new(select))),
            Statement::SetOperation(set) => Ok(Self::SetOp(Box::new(set))),
            Statement::WithCte(with) => Ok(Self::WithCte(Box::new(QueryWithCte {
                ctes: with.ctes,
                recursive: with.recursive,
                body: Self::try_from(*with.body)?,
            }))),
            Statement::Unsupported { kind, .. } => {
                let mut error = ParseError::at_zero(format!("unsupported query feature: {kind}"));
                error.spec = crate::error::catalog::ER_NOT_SUPPORTED_YET;
                Err(error)
            }
            _ => Err(ParseError::at_zero("expected a query expression")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Select {
    pub projection: Vec<SelectItem>,
    pub table: Option<String>,
    pub from: Vec<TableRef>,
    pub values: Option<ValuesSource>,
    pub json_table: Option<JsonTableSource>,
    pub derived: Option<SubQuery>,
    pub no_merge: bool,
    /// MySQL `SELECT STRAIGHT_JOIN`: keep this query block's FROM join order.
    pub straight_join: bool,
    pub joins: Vec<Join>,
    pub selection: Option<Expr>,
    pub group_by: Vec<Expr>,
    /// Normalized grouping sets over `group_by` key indices. Empty means
    /// ordinary GROUP BY semantics. Non-empty preserves source order, so
    /// duplicate grouping sets emit duplicate result groups.
    pub grouping_sets: Vec<Vec<usize>>,
    pub group_by_modifier: GroupByModifier,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<LimitValue>,
    pub offset: Option<LimitValue>,
    pub distinct: bool,
    pub for_update: bool,
    pub lock_clause: Option<LockClause>,
    /// MySQL `SELECT SQL_CALC_FOUND_ROWS ...` — deprecated in 8.0.17 but
    /// still accepted; instructs the optimizer to compute the row count
    /// the query would have returned without LIMIT/OFFSET, exposing it
    /// via `FOUND_ROWS()`. The dispatcher emits warning 1287 on each
    /// statement and routes execution through the strip-limit
    /// re-evaluation path before applying LIMIT/OFFSET.
    pub sql_calc_found_rows: bool,
    pub named_windows: Vec<NamedWindowDef>,
    pub query_hints: Vec<QueryHint>,
    /// Hint syntax / arity issues collected by `parse_query_hints`. Logical
    /// hint problems (conflicts, missing tables/indexes, CBO non-adoption)
    /// are appended by the planner; the combined list is surfaced in the
    /// first row of `EXPLAIN`'s `Extra` column.
    pub hint_warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValuesSource {
    pub rows: Vec<Vec<Expr>>,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JsonTableSource {
    pub expr: Expr,
    pub path: String,
    pub columns: Vec<JsonTableColumnDef>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum JsonTableColumnDef {
    Ordinality {
        name: String,
    },
    Named {
        name: String,
        data_type: DataType,
        unsigned: bool,
        path: String,
        exists: bool,
        on_empty: JsonTableOnResponse,
        on_error: JsonTableOnResponse,
    },
    Nested {
        path: String,
        columns: Vec<JsonTableColumnDef>,
    },
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum JsonTableOnResponse {
    Null,
    Default(Expr),
    Error,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum GroupByModifier {
    #[default]
    None,
    Rollup,
    Cube,
    GroupingSets,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum QueryHint {
    UseIndex {
        table: String,
        indexes: Vec<String>,
    },
    NoIndex {
        table: String,
        index: String,
    },
    IndexMerge {
        table: String,
        indexes: Vec<String>,
    },
    NoIndexMerge {
        table: String,
        indexes: Vec<String>,
    },
    FullScan {
        table: String,
    },
    /// Force/disable Zone Map access for the named table.  The planner still
    /// preserves the ordinary row predicate as the correctness oracle.
    ZoneMap {
        table: String,
    },
    NoZoneMap {
        table: String,
    },
    HashJoin {
        tables: Vec<String>,
    },
    NoHashJoin {
        tables: Vec<String>,
    },
    NlJoin {
        tables: Vec<String>,
    },
    IndexNlj {
        tables: Vec<String>,
    },
    Ordered,
    /// `/*+ MAX_EXECUTION_TIME(N) */` — MySQL 8.0 optimizer hint that caps
    /// the statement's wall-clock execution at `N` milliseconds, overriding
    /// the session/global `max_execution_time` sysvar. SELECT only.
    MaxExecutionTime {
        ms: u64,
    },
    /// Statement-local sysvar override. mydb currently exposes the three
    /// MySQL 8.0 SELECT-INTO writer controls through this hint.
    SetVar {
        name: String,
        value: String,
    },
    /// Oracle-style "index fast full scan": force the planner to read
    /// rows directly from a covering index without ever touching the
    /// heap. `index` is `None` when the user wrote `INDEX_FFS(t)`
    /// (planner picks the narrowest covering index) or `Some(name)`
    /// for `INDEX_FFS(t idx_name)`. Ignored when no index on the named
    /// table can satisfy the projected/filter columns from index data
    /// alone — same fallback behavior as MySQL's `USE INDEX` when the
    /// hint is unsatisfiable.
    IndexFfs {
        table: String,
        index: Option<String>,
    },
    /// `/*+ PARALLEL(n) */` — request parallel execution at degree `n`.
    /// `degree == 0` means "use the engine default" (e.g. `MYDB_PARALLEL_DEGREE`).
    /// The planner wraps eligible scans in `Exchange { ParallelSeqScan }` and
    /// honours the requested degree subject to safety checks
    /// (page count, subquery-in-filter, NO_PARALLEL, etc.).
    Parallel {
        degree: u32,
    },
    /// `/*+ NO_PARALLEL */` — force serial execution. Takes precedence
    /// over `PARALLEL(n)` when both appear, and over CBO auto-parallelism.
    NoParallel,
    /// `/*+ HNSW_EF(n) */` — override the HNSW `ef_search` candidate-pool
    /// size for this statement. Wins over the session/global
    /// `mydb_hnsw_ef_search`. `n == 0` is treated as "no override".
    HnswEf {
        ef: u64,
    },
    /// `/*+ IVF_NPROBE(n) */` — override the IVF_SQ8 `nprobe` (number of
    /// inverted cells probed) for this statement. Wins over the session/global
    /// `mydb_ivf_nprobe` and the per-index `NPROBE=`. `n == 0` = "no override".
    IvfNprobe {
        nprobe: u64,
    },
    /// `/*+ NO_VECTOR_INDEX */` — force the planner to skip the
    /// vector VectorIndexScan path (HNSW or IVF_SQ8) and fall back to a full
    /// scan + sort (brute-force KNN). Useful for small-table queries and
    /// recall debugging.
    NoVectorIndex,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum SelectItem {
    Wildcard,
    QualifiedWildcard(String),
    Expr { expr: Expr, alias: Option<String> },
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum IndexHintType {
    Use,
    Force,
    Ignore,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum IndexHintScope {
    All,
    Join,
    OrderBy,
    GroupBy,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexHint {
    pub hint_type: IndexHintType,
    pub scope: IndexHintScope,
    pub index_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
    /// Standard SQL table-version qualifier. The expression is evaluated once
    /// at statement start; `binding_id` is deterministic within the parsed
    /// statement and lets self-joins use independent historical snapshots.
    pub system_time: Option<SystemTimeAsOf>,
    pub index_hints: Vec<IndexHint>,
    /// M5.C2 — explicit `PARTITION (p0, p1, ...)` clause as written on a
    /// table reference. Empty means no restriction (visit all partitions).
    /// Carried through to DML / SELECT dispatch which installs the matching
    /// `storage_id`s via `partition_router::with_pruned_partitions`.
    pub partitions: Vec<String>,
    /// PostgreSQL `generate_series(...)` table function. `Some` when this
    /// FROM/JOIN item is a series generator rather than a catalog table;
    /// `name`/`alias` then carry the (aliased) output relation name and the
    /// planner routes it to `PhysicalPlan::GenerateSeries` instead of a
    /// catalog lookup. See `GenerateSeriesSpec` and
    /// `docs/generate-series-design.md`.
    pub generate_series: Option<Box<GenerateSeriesSpec>>,
    /// Derived output aliases whose width is known only after binding.
    pub column_aliases: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SystemTimeAsOf {
    /// Keep the recursive expression out of `TableRef` itself. Otherwise the
    /// temporal field inflates every `Statement`/`Expr` visitor stack frame,
    /// including queries that do not use time travel.
    pub expr: Box<Expr>,
    pub binding_id: u32,
}

/// Parsed `generate_series(start, stop [, step])` arguments. Carried verbatim
/// from the FROM clause; argument expressions are evaluated as constants by
/// the planner (`plan_generate_series`), which classifies the numeric/temporal
/// family, computes the exact row count, and emits a
/// `PhysicalPlan::GenerateSeries` leaf.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenerateSeriesSpec {
    pub start: Expr,
    pub stop: Expr,
    /// `None` → default step `+1` (numeric families). A missing step for the
    /// timestamp family is rejected at plan time.
    pub step: Option<Expr>,
    /// `WITH ORDINALITY` — append a 1-based `BIGINT` row-number column.
    pub with_ordinality: bool,
    /// Column aliases from `AS t(value [, ordinality])`. Empty → default names
    /// (`generate_series`, plus `ordinality` when `WITH ORDINALITY`).
    pub column_aliases: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Join {
    pub table: TableRef,
    pub join_type: JoinType,
    /// MySQL `left STRAIGHT_JOIN right`: left operand precedes this table.
    pub straight: bool,
    pub condition: Option<Expr>,
    /// Column names supplied by `JOIN ... USING (...)`, or resolved from a
    /// `NATURAL` join during planner preparation. These columns are
    /// coalesced for `SELECT *` output and bare-name references.
    pub using_columns: Vec<String>,
    /// True when parsed from `NATURAL ... JOIN`. The planner resolves it to
    /// `using_columns` once table schemas are available.
    pub natural: bool,
    /// When the right side of the JOIN is a parenthesised subquery
    /// (`JOIN (SELECT ...) AS sub ON ...`), this carries the inner SELECT.
    /// `table.alias` then names the derived rowset for column resolution.
    /// The executor materialises the subquery in memory and routes the
    /// join through the CTE-join path. `None` for a regular table or
    /// CTE reference. Required by standard TPC-H Q13/Q15.
    pub derived: Option<SubQuery>,
    /// True for `JOIN LATERAL (SELECT ...) AS ...`. The right side may
    /// reference tables already bound on its left and is executed with
    /// those outer values substituted per left row.
    pub lateral: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    /// `FULL [OUTER] JOIN` — emit matched rows plus unmatched rows from *both*
    /// sides (with the other side's columns NULL). Standard SQL (MySQL itself
    /// lacks it); mydb routes it through the in-memory join path, which already
    /// tracks unmatched rows on both sides.
    Full,
    Cross,
    /// Phase B #B1: Semi-join — emit each LEFT row at most once if at
    /// least one matching row exists on the RIGHT. Driven by IN-subquery
    /// or EXISTS rewrites; never produced directly by the parser.
    Semi,
    /// Phase B #B1: Anti-join — emit each LEFT row only when no matching
    /// row exists on the RIGHT. NULL handling depends on the source
    /// rewrite (NOT EXISTS keeps standard 2VL; NOT IN may demand NULL-aware
    /// behaviour signalled via HashJoin's `null_safe` keys).
    Anti,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ExplainFormat {
    Traditional,
    Tree,
    Json,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderBy {
    pub expr: Expr,
    pub asc: bool,
    /// Explicit `NULLS FIRST` / `NULLS LAST` (mydb extension; MySQL 8.0
    /// rejects this syntax with 1064). `None` follows the process default
    /// placement (see [`order_by_nulls_default_high`]).
    pub nulls_first: Option<bool>,
}

/// Process-level default for NULL placement in ORDER BY when no explicit
/// `NULLS FIRST`/`NULLS LAST` clause is given, driven by the
/// `MYDB_ORDER_BY_NULLS` environment variable:
///
/// - unset / `first` / `mysql` / `low` (default) → MySQL semantics: NULL is
///   treated as the lowest value, so ASC places it first, DESC last.
/// - `last` / `pg` / `high` → PostgreSQL semantics: NULL is treated as the
///   highest value, so ASC places it last, DESC first.
///
/// The PG mode exists so cross-engine comparisons (e.g. the TPC-DS harness
/// diffed against PostgreSQL) line up without per-query rewrites. It is a
/// process-wide knob: every plan in the process sees the same default, so
/// the plan cache stays coherent without a per-session fingerprint.
pub fn order_by_nulls_default_high() -> bool {
    #[cfg(not(test))]
    {
        static DEFAULT_HIGH: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *DEFAULT_HIGH.get_or_init(order_by_nulls_default_high_uncached)
    }
    #[cfg(test)]
    {
        order_by_nulls_default_high_uncached()
    }
}

fn order_by_nulls_default_high_uncached() -> bool {
    matches!(
        crate::util::env::var("MYDB_ORDER_BY_NULLS")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "last" | "pg" | "high"
    )
}

/// Effective NULL placement for one sort key given its direction and any
/// explicit `NULLS FIRST`/`NULLS LAST` clause. The single source of truth
/// for the `nulls_first: Option<bool>` → `bool` resolution; every sort path
/// (row + vectorized + aggregate ORDER BY) must route through here so the
/// `MYDB_ORDER_BY_NULLS` default is applied uniformly.
pub fn effective_nulls_first(asc: bool, explicit: Option<bool>) -> bool {
    match explicit {
        Some(nf) => nf,
        None => {
            if order_by_nulls_default_high() {
                !asc
            } else {
                asc
            }
        }
    }
}

impl OrderBy {
    /// Effective NULL placement for this key (explicit clause or the
    /// process default — see [`effective_nulls_first`]).
    pub fn nulls_first_effective(&self) -> bool {
        effective_nulls_first(self.asc, self.nulls_first)
    }

    /// True when the effective NULL placement deviates from the B-tree's
    /// native order (NULLs ranked lowest, i.e. first for ASC / last for
    /// DESC). Order-preserving plan rewrites (index order,
    /// streaming-aggregate absorption) must not fire in that case because
    /// B-tree scans deliver NULLs in MySQL default position only — this
    /// covers both an explicit NULLS FIRST/LAST clause and the PG-style
    /// process default.
    pub fn has_custom_nulls(&self) -> bool {
        self.nulls_first_effective() != self.asc
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum LimitValue {
    Rows(usize),
    Placeholder(usize),
    /// Count parameter in LIMIT offset, count. Retain source order across
    /// repeated placeholder numbering (protocol and SQL PREPARE).
    OffsetCommaPlaceholder(usize),
}

impl LimitValue {
    pub fn as_usize(&self) -> Option<usize> {
        match self {
            LimitValue::Rows(value) => Some(*value),
            LimitValue::Placeholder(_) | LimitValue::OffsetCommaPlaceholder(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Update {
    pub table: String,
    pub assignments: Vec<Assignment>,
    pub selection: Option<Expr>,
    /// Internal executor hook used after rewriting an updatable view UPDATE.
    /// The parser never populates this; DML execution evaluates it against
    /// the post-image row for WITH CHECK OPTION.
    pub view_check_predicate: Option<Expr>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<LimitValue>,
    pub ignore: bool,
    /// `UPDATE LOW_PRIORITY ...` — accepted; semantics no-op on mydb's
    /// MVCC engine (it doesn't queue against readers). Mirrors the
    /// `Delete::low_priority` shape.
    pub low_priority: bool,
    /// M5.C1 — alias of `table` when written as `UPDATE t AS a SET ...`.
    pub primary_alias: Option<String>,
    /// M5.C1 — additional joined tables. Empty for single-table UPDATE,
    /// which keeps the legacy fast path unchanged.
    pub joins: Vec<Join>,
    /// M5.C2 — `UPDATE t PARTITION (p0, p1) SET ...`. Empty = unrestricted.
    pub partitions: Vec<String>,
    pub query_hints: Vec<QueryHint>,
    pub hint_warnings: Vec<String>,
    pub returning: Option<ReturningClause>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Assignment {
    pub column: String,
    pub value: Expr,
    /// M5.C1 — table or alias qualifier when written as `SET t.col = ...`.
    /// `None` for bare-name assignments. Resolved against the alias set
    /// (primary + joins) in multi-table UPDATE; ignored in single-table.
    pub table_qualifier: Option<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Delete {
    pub table: String,
    pub selection: Option<Expr>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<LimitValue>,
    /// M5.C1 — alias of `table` when written as `DELETE FROM t AS a`.
    pub primary_alias: Option<String>,
    /// M5.C1 — additional joined tables for `DELETE ... FROM t1 JOIN t2 ON ...`.
    /// Empty for single-table DELETE.
    pub joins: Vec<Join>,
    /// M5.C1 — explicit target list for multi-table DELETE
    /// `DELETE t1, t2 FROM t1 JOIN t2 ON ...`. Each entry names a table or
    /// alias to delete rows from. Empty means the primary table is the
    /// only target (legacy single-table case).
    pub targets: Vec<String>,
    /// M5.C2 — `DELETE FROM t PARTITION (p0, p1) WHERE ...`.
    pub partitions: Vec<String>,
    /// `DELETE LOW_PRIORITY FROM ...` — accepted; semantics no-op on mydb's
    /// MVCC engine (it doesn't queue against readers).
    pub low_priority: bool,
    /// `DELETE QUICK FROM ...` — accepted; no-op (MyISAM-era hint).
    pub quick: bool,
    /// `DELETE IGNORE FROM ...` — per-row errors (FK violations, etc.) are
    /// swallowed; the row is skipped and the loop continues.
    pub ignore: bool,
    pub query_hints: Vec<QueryHint>,
    pub hint_warnings: Vec<String>,
    pub returning: Option<ReturningClause>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WindowSpec {
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<OrderBy>,
    pub frame: Option<WindowFrame>,
    pub inherits: Option<String>,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Expr {
    Wildcard,
    Identifier(String),
    CompoundIdentifier(Vec<String>),
    UserVariable(String),
    SystemVariable {
        scope: SystemVariableScope,
        name: String,
    },
    Literal(Literal),
    BoundValue(crate::types::Value),
    // Lexical parameter identity assigned in the token stream before
    // semantic conversion can reorder or duplicate expression nodes.
    Placeholder(usize),
    Default(Option<String>),
    /// Qualified DEFAULT reference; unlike a value reference it reads the catalog default.
    DefaultQualified(Vec<String>),
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<Select>,
        negated: bool,
    },
    QuantifiedComparison {
        left: Box<Expr>,
        op: BinaryOp,
        quantifier: SubqueryQuantifier,
        right: QuantifiedComparisonRight,
    },
    Subquery(Box<Select>),
    Exists {
        subquery: Box<Select>,
        negated: bool,
    },
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
        escape_char: Option<char>,
    },
    Case {
        operand: Option<Box<Expr>>,
        // CASE arity is fixed after parsing. A boxed slice keeps the variant
        // compact without a second allocation or changing its wire shape.
        conditions: Box<[Expr]>,
        results: Vec<Expr>,
        else_result: Option<Box<Expr>>,
    },
    Function {
        name: String,
        args: Vec<Expr>,
        distinct: bool,
    },
    GroupKeyRef {
        index: usize,
        expr: Box<Expr>,
    },
    Grouping(Vec<usize>),
    WindowFunction {
        name: String,
        args: Vec<Expr>,
        #[serde(flatten)]
        spec: Box<WindowSpec>,
    },
    Tuple(Vec<Expr>),
}

// Expressions nest up to `MAX_EXPRESSION_DEPTH` levels, and arbitrary code
// clones, compares and formats them (plan caches, fingerprints, rewrites).
// These match the derived implementations exactly, but each level can extend
// the stack instead of relying on the caller's thread size.
impl Clone for Expr {
    fn clone(&self) -> Self {
        crate::util::stack::ensure_stack(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::ParserTransient,
            || match self {
                Self::Wildcard => Self::Wildcard,
                Self::Identifier(a0) => Self::Identifier(a0.clone()),
                Self::CompoundIdentifier(a0) => Self::CompoundIdentifier(a0.clone()),
                Self::UserVariable(a0) => Self::UserVariable(a0.clone()),
                Self::SystemVariable { scope, name } => Self::SystemVariable {
                    scope: *scope,
                    name: name.clone(),
                },
                Self::Literal(a0) => Self::Literal(a0.clone()),
                Self::BoundValue(a0) => Self::BoundValue(a0.clone()),
                Self::Placeholder(a0) => Self::Placeholder(*a0),
                Self::Default(a0) => Self::Default(a0.clone()),
                Self::DefaultQualified(a0) => Self::DefaultQualified(a0.clone()),
                Self::Unary { op, expr } => Self::Unary {
                    op: *op,
                    expr: expr.clone(),
                },
                Self::Binary { left, op, right } => Self::Binary {
                    left: left.clone(),
                    op: *op,
                    right: right.clone(),
                },
                Self::IsNull(a0) => Self::IsNull(a0.clone()),
                Self::IsNotNull(a0) => Self::IsNotNull(a0.clone()),
                Self::InList {
                    expr,
                    list,
                    negated,
                } => Self::InList {
                    expr: expr.clone(),
                    list: list.clone(),
                    negated: *negated,
                },
                Self::InSubquery {
                    expr,
                    subquery,
                    negated,
                } => Self::InSubquery {
                    expr: expr.clone(),
                    subquery: subquery.clone(),
                    negated: *negated,
                },
                Self::QuantifiedComparison {
                    left,
                    op,
                    quantifier,
                    right,
                } => Self::QuantifiedComparison {
                    left: left.clone(),
                    op: *op,
                    quantifier: *quantifier,
                    right: right.clone(),
                },
                Self::Subquery(a0) => Self::Subquery(a0.clone()),
                Self::Exists { subquery, negated } => Self::Exists {
                    subquery: subquery.clone(),
                    negated: *negated,
                },
                Self::Between {
                    expr,
                    low,
                    high,
                    negated,
                } => Self::Between {
                    expr: expr.clone(),
                    low: low.clone(),
                    high: high.clone(),
                    negated: *negated,
                },
                Self::Like {
                    expr,
                    pattern,
                    negated,
                    escape_char,
                } => Self::Like {
                    expr: expr.clone(),
                    pattern: pattern.clone(),
                    negated: *negated,
                    escape_char: *escape_char,
                },
                Self::Case {
                    operand,
                    conditions,
                    results,
                    else_result,
                } => Self::Case {
                    operand: operand.clone(),
                    conditions: conditions.clone(),
                    results: results.clone(),
                    else_result: else_result.clone(),
                },
                Self::Function {
                    name,
                    args,
                    distinct,
                } => Self::Function {
                    name: name.clone(),
                    args: args.clone(),
                    distinct: *distinct,
                },
                Self::GroupKeyRef { index, expr } => Self::GroupKeyRef {
                    index: *index,
                    expr: expr.clone(),
                },
                Self::Grouping(a0) => Self::Grouping(a0.clone()),
                Self::WindowFunction { name, args, spec } => Self::WindowFunction {
                    name: name.clone(),
                    args: args.clone(),
                    spec: spec.clone(),
                },
                Self::Tuple(a0) => Self::Tuple(a0.clone()),
            },
        )
    }
}

impl PartialEq for Expr {
    fn eq(&self, other: &Self) -> bool {
        crate::util::stack::ensure_stack(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::ParserTransient,
            || match (self, other) {
                (Self::Wildcard, Self::Wildcard) => true,
                (Self::Identifier(a0), Self::Identifier(b0)) => a0 == b0,
                (Self::CompoundIdentifier(a0), Self::CompoundIdentifier(b0)) => a0 == b0,
                (Self::UserVariable(a0), Self::UserVariable(b0)) => a0 == b0,
                (
                    Self::SystemVariable {
                        scope: l_scope,
                        name: l_name,
                    },
                    Self::SystemVariable {
                        scope: r_scope,
                        name: r_name,
                    },
                ) => l_scope == r_scope && l_name == r_name,
                (Self::Literal(a0), Self::Literal(b0)) => a0 == b0,
                (Self::BoundValue(a0), Self::BoundValue(b0)) => a0 == b0,
                (Self::Placeholder(a0), Self::Placeholder(b0)) => a0 == b0,
                (Self::Default(a0), Self::Default(b0)) => a0 == b0,
                (Self::DefaultQualified(a0), Self::DefaultQualified(b0)) => a0 == b0,
                (
                    Self::Unary {
                        op: l_op,
                        expr: l_expr,
                    },
                    Self::Unary {
                        op: r_op,
                        expr: r_expr,
                    },
                ) => l_op == r_op && l_expr == r_expr,
                (
                    Self::Binary {
                        left: l_left,
                        op: l_op,
                        right: l_right,
                    },
                    Self::Binary {
                        left: r_left,
                        op: r_op,
                        right: r_right,
                    },
                ) => l_left == r_left && l_op == r_op && l_right == r_right,
                (Self::IsNull(a0), Self::IsNull(b0)) => a0 == b0,
                (Self::IsNotNull(a0), Self::IsNotNull(b0)) => a0 == b0,
                (
                    Self::InList {
                        expr: l_expr,
                        list: l_list,
                        negated: l_negated,
                    },
                    Self::InList {
                        expr: r_expr,
                        list: r_list,
                        negated: r_negated,
                    },
                ) => l_expr == r_expr && l_list == r_list && l_negated == r_negated,
                (
                    Self::InSubquery {
                        expr: l_expr,
                        subquery: l_subquery,
                        negated: l_negated,
                    },
                    Self::InSubquery {
                        expr: r_expr,
                        subquery: r_subquery,
                        negated: r_negated,
                    },
                ) => l_expr == r_expr && l_subquery == r_subquery && l_negated == r_negated,
                (
                    Self::QuantifiedComparison {
                        left: l_left,
                        op: l_op,
                        quantifier: l_quantifier,
                        right: l_right,
                    },
                    Self::QuantifiedComparison {
                        left: r_left,
                        op: r_op,
                        quantifier: r_quantifier,
                        right: r_right,
                    },
                ) => {
                    l_left == r_left
                        && l_op == r_op
                        && l_quantifier == r_quantifier
                        && l_right == r_right
                }
                (Self::Subquery(a0), Self::Subquery(b0)) => a0 == b0,
                (
                    Self::Exists {
                        subquery: l_subquery,
                        negated: l_negated,
                    },
                    Self::Exists {
                        subquery: r_subquery,
                        negated: r_negated,
                    },
                ) => l_subquery == r_subquery && l_negated == r_negated,
                (
                    Self::Between {
                        expr: l_expr,
                        low: l_low,
                        high: l_high,
                        negated: l_negated,
                    },
                    Self::Between {
                        expr: r_expr,
                        low: r_low,
                        high: r_high,
                        negated: r_negated,
                    },
                ) => {
                    l_expr == r_expr && l_low == r_low && l_high == r_high && l_negated == r_negated
                }
                (
                    Self::Like {
                        expr: l_expr,
                        pattern: l_pattern,
                        negated: l_negated,
                        escape_char: l_escape_char,
                    },
                    Self::Like {
                        expr: r_expr,
                        pattern: r_pattern,
                        negated: r_negated,
                        escape_char: r_escape_char,
                    },
                ) => {
                    l_expr == r_expr
                        && l_pattern == r_pattern
                        && l_negated == r_negated
                        && l_escape_char == r_escape_char
                }
                (
                    Self::Case {
                        operand: l_operand,
                        conditions: l_conditions,
                        results: l_results,
                        else_result: l_else_result,
                    },
                    Self::Case {
                        operand: r_operand,
                        conditions: r_conditions,
                        results: r_results,
                        else_result: r_else_result,
                    },
                ) => {
                    l_operand == r_operand
                        && l_conditions == r_conditions
                        && l_results == r_results
                        && l_else_result == r_else_result
                }
                (
                    Self::Function {
                        name: l_name,
                        args: l_args,
                        distinct: l_distinct,
                    },
                    Self::Function {
                        name: r_name,
                        args: r_args,
                        distinct: r_distinct,
                    },
                ) => l_name == r_name && l_args == r_args && l_distinct == r_distinct,
                (
                    Self::GroupKeyRef {
                        index: l_index,
                        expr: l_expr,
                    },
                    Self::GroupKeyRef {
                        index: r_index,
                        expr: r_expr,
                    },
                ) => l_index == r_index && l_expr == r_expr,
                (Self::Grouping(a0), Self::Grouping(b0)) => a0 == b0,
                (
                    Self::WindowFunction {
                        name: l_name,
                        args: l_args,
                        spec: l_spec,
                    },
                    Self::WindowFunction {
                        name: r_name,
                        args: r_args,
                        spec: r_spec,
                    },
                ) => l_name == r_name && l_args == r_args && l_spec == r_spec,
                (Self::Tuple(a0), Self::Tuple(b0)) => a0 == b0,
                _ => false,
            },
        )
    }
}

impl fmt::Debug for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        crate::util::stack::ensure_stack(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::ParserTransient,
            || match self {
                Self::Wildcard => f.write_str("Wildcard"),
                Self::Identifier(a0) => f.debug_tuple("Identifier").field(a0).finish(),
                Self::CompoundIdentifier(a0) => {
                    f.debug_tuple("CompoundIdentifier").field(a0).finish()
                }
                Self::UserVariable(a0) => f.debug_tuple("UserVariable").field(a0).finish(),
                Self::SystemVariable { scope, name } => f
                    .debug_struct("SystemVariable")
                    .field("scope", scope)
                    .field("name", name)
                    .finish(),
                Self::Literal(a0) => f.debug_tuple("Literal").field(a0).finish(),
                Self::BoundValue(a0) => f.debug_tuple("BoundValue").field(a0).finish(),
                Self::Placeholder(a0) => f.debug_tuple("Placeholder").field(a0).finish(),
                Self::Default(a0) => f.debug_tuple("Default").field(a0).finish(),
                Self::DefaultQualified(a0) => f.debug_tuple("DefaultQualified").field(a0).finish(),
                Self::Unary { op, expr } => f
                    .debug_struct("Unary")
                    .field("op", op)
                    .field("expr", expr)
                    .finish(),
                Self::Binary { left, op, right } => f
                    .debug_struct("Binary")
                    .field("left", left)
                    .field("op", op)
                    .field("right", right)
                    .finish(),
                Self::IsNull(a0) => f.debug_tuple("IsNull").field(a0).finish(),
                Self::IsNotNull(a0) => f.debug_tuple("IsNotNull").field(a0).finish(),
                Self::InList {
                    expr,
                    list,
                    negated,
                } => f
                    .debug_struct("InList")
                    .field("expr", expr)
                    .field("list", list)
                    .field("negated", negated)
                    .finish(),
                Self::InSubquery {
                    expr,
                    subquery,
                    negated,
                } => f
                    .debug_struct("InSubquery")
                    .field("expr", expr)
                    .field("subquery", subquery)
                    .field("negated", negated)
                    .finish(),
                Self::QuantifiedComparison {
                    left,
                    op,
                    quantifier,
                    right,
                } => f
                    .debug_struct("QuantifiedComparison")
                    .field("left", left)
                    .field("op", op)
                    .field("quantifier", quantifier)
                    .field("right", right)
                    .finish(),
                Self::Subquery(a0) => f.debug_tuple("Subquery").field(a0).finish(),
                Self::Exists { subquery, negated } => f
                    .debug_struct("Exists")
                    .field("subquery", subquery)
                    .field("negated", negated)
                    .finish(),
                Self::Between {
                    expr,
                    low,
                    high,
                    negated,
                } => f
                    .debug_struct("Between")
                    .field("expr", expr)
                    .field("low", low)
                    .field("high", high)
                    .field("negated", negated)
                    .finish(),
                Self::Like {
                    expr,
                    pattern,
                    negated,
                    escape_char,
                } => f
                    .debug_struct("Like")
                    .field("expr", expr)
                    .field("pattern", pattern)
                    .field("negated", negated)
                    .field("escape_char", escape_char)
                    .finish(),
                Self::Case {
                    operand,
                    conditions,
                    results,
                    else_result,
                } => f
                    .debug_struct("Case")
                    .field("operand", operand)
                    .field("conditions", conditions)
                    .field("results", results)
                    .field("else_result", else_result)
                    .finish(),
                Self::Function {
                    name,
                    args,
                    distinct,
                } => f
                    .debug_struct("Function")
                    .field("name", name)
                    .field("args", args)
                    .field("distinct", distinct)
                    .finish(),
                Self::GroupKeyRef { index, expr } => f
                    .debug_struct("GroupKeyRef")
                    .field("index", index)
                    .field("expr", expr)
                    .finish(),
                Self::Grouping(a0) => f.debug_tuple("Grouping").field(a0).finish(),
                Self::WindowFunction { name, args, spec } => f
                    .debug_struct("WindowFunction")
                    .field("name", name)
                    .field("args", args)
                    .field("partition_by", &spec.partition_by)
                    .field("order_by", &spec.order_by)
                    .field("frame", &spec.frame)
                    .field("inherits", &spec.inherits)
                    .finish(),
                Self::Tuple(a0) => f.debug_tuple("Tuple").field(a0).finish(),
            },
        )
    }
}

/// Source spelling of a MySQL `@@` variable scope. Keeping the implicit and
/// LOCAL forms distinct preserves expression-column presentation while the
/// evaluator maps all three non-global forms to session scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum SystemVariableScope {
    ImplicitSession,
    Session,
    Local,
    Global,
}

/// MySQL full-text search modes (the modifiers of `MATCH ... AGAINST`).
///
/// The parser lowers `MATCH(cols) AGAINST('q' [mode])` into the internal scalar
/// function [`FT_MATCH_FUNCTION`], reusing `Expr::Function`'s existing traversal /
/// column-reference collection / fingerprint / normalization paths instead of adding a dedicated `Expr` variant (the `__INTERVAL` convention).
/// The mode rides along as a string argument; planner / executor parse it back into this enum internally.
/// `WITH QUERY EXPANSION` is not supported yet (a clear error is raised at parse time).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum FtMode {
    /// `IN NATURAL LANGUAGE MODE` (also the default when no modifier is given).
    NaturalLanguage,
    /// `IN BOOLEAN MODE`.
    Boolean,
}

impl FtMode {
    /// Stable string encoding passed as the first argument of `__match_against`.
    pub fn as_str(self) -> &'static str {
        match self {
            FtMode::NaturalLanguage => "natural",
            FtMode::Boolean => "boolean",
        }
    }

    /// Recovers the value from the encoding produced by [`Self::as_str`].
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "natural" => Some(FtMode::NaturalLanguage),
            "boolean" => Some(FtMode::Boolean),
            _ => None,
        }
    }
}

/// Internal scalar function name: the function that `MATCH(cols) AGAINST('q' mode)` lowers to.
/// Argument layout: `(mode_str, query_str, col_ident, col_ident, ...)`. The `__` prefix avoids
/// clashes with user-defined functions (the `__INTERVAL` convention).
pub(crate) const FT_MATCH_FUNCTION: &str = "__match_against";

/// Internal function carrying a MySQL user-variable assignment expression
/// `@var := expr`. `args[0]` is a string literal naming the variable,
/// `args[1]` is the value expression. The evaluator assigns and returns the
/// value; the statement runs serially so the write reaches the session
/// (issue #86).
pub(crate) const USER_VAR_ASSIGN_FUNCTION: &str = "__set_user_var";

/// Internal scalar function name: the wrapper function that `GROUP_CONCAT(expr ORDER BY
/// ord1 [ASC|DESC], ord2, ...)` lowers to. `__GC_ORDERED(value, ord1, "ASC"|"DESC", ord2, "ASC"|"DESC", ...)`:
/// `AggregateSlot::from_expr` unpacks it into `(value, Vec<OrderBy>)` when resolving aggregate
/// slots, while every other AST path walks its arguments as a plain function.
pub const GROUP_CONCAT_ORDERED_FN: &str = "__gc_ordered";

/// Internal aggregate function name: value aggregation for GROUP BY hidden columns (column
/// references that are non-aggregated and not in GROUP BY, appearing in projection / HAVING /
/// ORDER BY when ONLY_FULL_GROUP_BY is off, i.e. empty sql_mode). While collecting aggregate
/// calls the planner appends `__hidden_first_value(col)` for each hidden column reference (see
/// `planner::collect_aggregate_calls`); the accumulator takes the group's first-row value
/// (NULL included), deterministic and consistent with MySQL's "free to choose any value from the group".
/// The executor looks the value up in the agg cache by the slot's fingerprint on aggregate output
/// rows (see the hidden lookup in `executor::select::eval`). User SQL must not use this name directly.
pub const HIDDEN_FIRST_VALUE_FN: &str = "__hidden_first_value";

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum SubqueryQuantifier {
    Any,
    All,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum QuantifiedComparisonRight {
    Subquery(Box<Select>),
    List(Vec<Expr>),
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WindowFrame {
    pub units: WindowFrameUnits,
    pub start: WindowFrameBound,
    pub end: WindowFrameBound,
    pub exclusion: WindowFrameExclusion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum WindowFrameUnits {
    Rows,
    Range,
    Groups,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum WindowFrameExclusion {
    NoOthers,
    CurrentRow,
    Group,
    Ties,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum WindowFrameBound {
    UnboundedPreceding,
    UnboundedFollowing,
    CurrentRow,
    Preceding(Box<Expr>),
    Following(Box<Expr>),
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamedWindowDef {
    pub name: String,
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<OrderBy>,
    pub frame: Option<WindowFrame>,
    pub inherits: Option<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Literal {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    /// Exact decimal literal, kept as its original textual form (e.g.
    /// `"123456789012345678.99"`). MySQL treats a numeric literal with a
    /// decimal point but no exponent as an exact `DECIMAL`; storing the text
    /// (rather than an `f64`) preserves full precision all the way to the
    /// `DecimalCompact` encoder. Also used for integer literals that overflow
    /// `u64`, which MySQL promotes to `DECIMAL`.
    Decimal(String),
    String(String),
    /// Hex literal `0xCAFEBABE` or `X'CAFE'`. MySQL stores the decoded bytes
    /// and produces a *binary string* by default; in *numeric context*
    /// (arithmetic, bitwise, unary plus) the bytes are big-endian interpreted
    /// as a `BIGINT UNSIGNED`. The provenance must be retained so generic
    /// BLOB / BINARY columns (which also produce `Value::Bytes`) are NOT
    /// auto-coerced to numeric. Length ≤8 bytes → coerces to `u64`; >8 stays
    /// as bytes (no numeric meaning).
    HexBytes(Vec<u8>),
    /// Bit literal `b'1010'` / `0b1010`. Same byte-vs-numeric duality as
    /// `HexBytes`.
    BitBytes(Vec<u8>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum UnaryOp {
    Not,
    Neg,
    /// Bitwise inversion `~expr`. MySQL evaluates the operand as an
    /// unsigned 64-bit integer and returns its one's complement as
    /// `BIGINT UNSIGNED`.
    BitNot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum BinaryOp {
    Or,
    And,
    /// Logical `XOR` (MySQL keyword). Three-valued: NULL if either side
    /// is NULL, else `1` when exactly one operand is truthy.
    Xor,
    Eq,
    NullSafeEq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Add,
    Sub,
    Mul,
    Div,
    /// MySQL integer division operator `DIV`. Kept distinct from `/` so
    /// validators, type inference and evaluators cannot accidentally accept
    /// real division where integer-only syntax is required (notably
    /// partition expressions).
    IntDiv,
    Mod,
    /// Bitwise `&`, `|`, `^` and the shifts `<<`, `>>`. MySQL converts
    /// both operands to `BIGINT UNSIGNED` and returns `BIGINT UNSIGNED`.
    BitAnd,
    BitOr,
    BitXor,
    ShiftLeft,
    ShiftRight,
    Arrow,
    LongArrow,
    /// String concatenation selected at parse time when sql_mode contains
    /// PIPES_AS_CONCAT. Default-mode `||` is normalized to `Or`, so prepared
    /// statements and persistent expressions never re-read live sql_mode.
    Concat,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    pub spec: crate::error::ErrorSpec,
    pub message: String,
    pub span: Span,
    pub line: usize,
    pub column: usize,
    pub expected: Vec<String>,
}

impl ParseError {
    pub const fn code(&self) -> u16 {
        self.spec.code().get()
    }

    pub const fn sql_state(&self) -> crate::error::SqlState {
        self.spec.sql_state()
    }

    /// Construct a parse error at the start of the input. Used by the
    /// out-of-tree mini-parsers (e.g. `parser_partition`) that don't
    /// carry per-character spans.
    pub fn at_zero(message: impl Into<String>) -> Self {
        Self {
            spec: crate::error::catalog::ER_PARSE_ERROR,
            message: message.into(),
            span: Span { start: 0, end: 0 },
            line: 1,
            column: 1,
            expected: Vec::new(),
        }
    }

    /// Preserve a shared lexer's byte coordinates in parser diagnostics.
    pub(crate) fn from_lex(input: &str, err: crate::sql_lex::SqlLexError) -> Self {
        let offset = err.offset.min(input.len());
        Self::new(
            input,
            Span {
                start: offset,
                end: offset,
            },
            err.message,
            Vec::new(),
        )
    }

    pub(super) fn new(
        input: &str,
        span: Span,
        message: impl Into<String>,
        expected: Vec<String>,
    ) -> Self {
        let (line, column) = line_column(input, span.start);
        Self {
            spec: crate::error::catalog::ER_PARSE_ERROR,
            message: message.into(),
            span,
            line,
            column,
            expected,
        }
    }

    /// Translate a child statement's coordinates into its enclosing source.
    pub(crate) fn rebase(mut self, source: &str, offset: usize) -> Self {
        self.span.start = self.span.start.saturating_add(offset).min(source.len());
        self.span.end = self.span.end.saturating_add(offset).min(source.len());
        (self.line, self.column) = line_column(source, self.span.start);
        self
    }

    pub(super) fn from_sqlparser(input: &str, err: ParserError) -> Self {
        if let ParserError::ParserError(message) = &err {
            for (prefix, spec) in [
                (
                    super::budget::STACK_OOM,
                    crate::error::catalog::ER_OUTOFMEMORY,
                ),
                (
                    super::budget::STACK_INTERRUPTED,
                    crate::error::catalog::ER_QUERY_INTERRUPTED,
                ),
                (
                    super::budget::STACK_TIMEOUT,
                    crate::error::catalog::ER_QUERY_TIMEOUT,
                ),
                (
                    super::budget::STACK_DEPTH,
                    crate::error::catalog::ER_STACK_OVERRUN_NEED_MORE,
                ),
            ] {
                if let Some(message) = message.strip_prefix(prefix) {
                    let mut error =
                        Self::new(input, Span { start: 0, end: 0 }, message, Vec::new());
                    error.spec = spec;
                    return error;
                }
            }
        }
        if matches!(err, ParserError::RecursionLimitExceeded) {
            let mut error = Self::new(
                input,
                Span { start: 0, end: 0 },
                "Statement nesting exceeds the parser recursion limit",
                Vec::new(),
            );
            error.spec = crate::error::catalog::ER_STACK_OVERRUN_NEED_MORE;
            return error;
        }
        let mut message = super::adapter::display_sql(&err);
        let location = sqlparser_error_location(&message);
        let offset = location
            .and_then(|(line, column)| position_in_source(input, line, column))
            // Location-less errors normally mean unexpected EOF. Point at the
            // end rather than reporting the unrelated first byte.
            .unwrap_or(input.len());
        // Display uses our mapped coordinates. Do not keep a second,
        // potentially transformed location embedded in the parser message.
        if location.is_some() {
            message.truncate(message.rfind(" at Line: ").unwrap());
        }
        Self::new(
            input,
            Span {
                start: offset,
                end: offset,
            },
            message,
            Vec::new(),
        )
    }

    pub(super) fn unsupported_at(input: &str, span: Span, message: impl Into<String>) -> Self {
        Self::new(input, span, message, Vec::new())
    }
}

/// sqlparser currently embeds its public location in the stable display tail
/// ` at Line: N, Column: M`. Parse from the end so user text containing the
/// same words cannot move the diagnostic.
fn sqlparser_error_location(message: &str) -> Option<(u64, u64)> {
    let marker = " at Line: ";
    let start = message.rfind(marker)? + marker.len();
    let (line, column) = message[start..].split_once(", Column: ")?;
    Some((line.parse().ok()?, column.parse().ok()?))
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} at line {}, column {}",
            self.message, self.line, self.column
        )
    }
}

impl std::error::Error for ParseError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

/// mydb's advertised MySQL server version id, in MySQL's `MMmmrr` integer
/// form (`8.0.46` → `80046`). Kept in lock-step with the version string the
/// handshake advertises (`8.0.46-mydb`, see `mysql::protocol`) and the `version`
/// system variable. Versioned executable comments (`/*!NNNNN … */`) whose gate
/// exceeds this id are inert, exactly as a real MySQL 8.0.0 server treats them.
pub(crate) const MYSQL_VERSION_ID: u32 = crate::server_version::MYSQL_COMPAT_VERSION_ID;

impl From<Select> for QueryExpr {
    fn from(select: Select) -> Self {
        Self::Select(Box::new(select))
    }
}
impl From<SetOperation> for QueryExpr {
    fn from(set: SetOperation) -> Self {
        Self::SetOp(Box::new(set))
    }
}
