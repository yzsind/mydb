# SQL Execution Contracts

This chapter defines shared privilege, transaction, locking, and diagnostic rules. A statement page adds its own exceptions. An OK packet means that the requested synchronous work completed, or that an explicitly asynchronous request was accepted; it does not by itself prove completion of a backup, cancellation, replication catch-up, restart, or shutdown.

## Privilege Scope

Table and column grants can authorize accesses within their scope; global grants apply across schemas. A view or stored program can change the effective authorization identity through SQL SECURITY. Referenced data, functions, files, and external services can require privileges in addition to the statement's entry privilege. RLS remains an additional row check; it does not grant SQL privileges.

| Operation | Required privilege at the entry point |
| --- | --- |
| CREATE / ALTER / DROP DATABASE | CREATE / ALTER / DROP on the database, respectively |
| CREATE TABLE; CREATE TABLE LIKE | CREATE on target; LIKE additionally needs SELECT on source |
| CREATE TEMPORARY TABLE / SEQUENCE | CREATE TEMPORARY TABLES in target schema |
| Persistent CREATE TABLE AS SELECT | CREATE and INSERT on target, SELECT on sources |
| ALTER TABLE | ALTER; adding foreign keys additionally needs REFERENCES on referenced tables |
| CREATE INDEX / DROP INDEX | INDEX on the table; ALTER alone does not replace it |
| DROP TABLE / TRUNCATE TABLE; DROP VIEW / SEQUENCE | DROP on each target |
| RENAME TABLE | ALTER and DROP on each source, CREATE and INSERT on each destination |
| CREATE / ALTER VIEW | CREATE VIEW plus source read privileges; replacement and ALTER also need DROP |
| CREATE / ALTER / DROP PROCEDURE or FUNCTION | CREATE ROUTINE on schema / ALTER ROUTINE on routine / ALTER ROUTINE on routine |
| CALL; stored function invocation | EXECUTE on the routine, then DEFINER/INVOKER body checks |
| CREATE / DROP TRIGGER | TRIGGER on the subject table; DROP resolves the trigger to that table |
| CREATE / ALTER / DROP EVENT | EVENT on schema; execution uses the event definer |
| ANALYZE / OPTIMIZE / REPAIR TABLE | SELECT and INSERT on each table |
| CHECK / CHECKSUM TABLE | SELECT on each table |
| LOCK TABLES | LOCK TABLES and SELECT on each declared table |
| DESCRIBE, SHOW COLUMNS/INDEX/CREATE TABLE | SELECT on target |
| SHOW CREATE SEQUENCE | SELECT on sequence |
| INSERT; REPLACE | INSERT; REPLACE also needs DELETE |
| ON DUPLICATE KEY UPDATE | INSERT plus UPDATE; reading existing target columns also needs SELECT |
| UPDATE / DELETE | UPDATE / DELETE on modified columns/tables; SELECT for source reads and read target columns |
| DML RETURNING | Also SELECT on target |
| LOAD DATA/XML | INSERT; REPLACE also DELETE; server-side INFILE also global FILE |
| SELECT INTO OUTFILE/DUMPFILE | Source SELECT plus global FILE |
| FLUSH forms, RESET QUERY CACHE | Global RELOAD |
| Checkpoint, SQL/data cache flush, CANCEL DDL JOB | Global SYSTEM_VARIABLES_ADMIN |
| INSTALL / UNINSTALL COMPONENT; RESET PERSIST | Global SYSTEM_VARIABLES_ADMIN |
| SHOW ENGINE; SHOW MYDB MEMORY | Global PROCESS |
| EXPLAIN FOR CONNECTION | PROCESS for another connection; self-inspection exception |
| SHUTDOWN / RESTART | Global SHUTDOWN |
| XA RECOVER | Global XA_RECOVER_ADMIN |
| MySQL replica SHOW / control | REPLICATION CLIENT / REPLICATION_SLAVE_ADMIN, respectively |
| SHOW BINARY LOG STATUS/LOGS | REPLICATION CLIENT |
| SHOW BINLOG EVENTS / SHOW REPLICAS | REPLICATION SLAVE |
| PURGE BINARY LOGS | BINLOG_ADMIN |
| FLUSH BINARY LOGS / RESET BINARY LOGS AND GTIDS | RELOAD |
| Native mydb replication administration and SHOW | SYSTEM_VARIABLES_ADMIN; FORCE_RESYNC additionally needs SHUTDOWN |
| All backup SQL, including SHOW | BACKUP_ADMIN |

The effective privilege set includes activated roles. SUPER implies some administration checks but is not a replacement for every extension capability: AI_ADMIN/AI_USE, HTTP_ADMIN, ROW_SECURITY_ADMIN, RLS_CONTEXT_ADMIN, and API_KEY_ADMIN have their own rules. See the relevant [extension references](../README.md). Authentication or privilege errors can occur before an unsupported-capability error.

## Transaction Boundaries

| Statement family | Transaction effect |
| --- | --- |
| Ordinary SELECT / DML | Uses an autocommit transaction or the current explicit/implicit transaction; does not commit unrelated earlier work in an explicit transaction |
| Persistent object DDL, stored program DDL, account/grant changes | Implicitly commits the existing transaction before publication; successful DDL is not undone by later ROLLBACK |
| CREATE TEMPORARY TABLE and DROP TEMPORARY TABLE | Do not use the ordinary persistent-DDL commit boundary, but successful temporary DDL itself is not user-rollbackable |
| Ordinary DROP TABLE resolving to temporary objects | Still implicitly commits; use the explicit TEMPORARY form when that distinction matters |
| TRUNCATE, including a session temporary table | Implicit commit; not equivalent to transactional DELETE |
| CHECK / ANALYZE / REPAIR / OPTIMIZE TABLE | Implicit commit before maintenance |
| FLUSH PRIVILEGES / HOSTS / STATUS / TABLES / QUERY CACHE / LOGS | Implicit commit before the operation; a subsequent validation or execution error can occur after that commit |
| FLUSH USER_RESOURCES | Resets runtime quotas without the above implicit-commit call |
| LOCK TABLES | Implicit commit, then explicit locks |
| UNLOCK TABLES | Commits if explicit LOCK TABLES locks are held; releasing FTWRL alone does not require that same commit |
| SET autocommit from 0 to 1 | Commits current transaction; setting the current value again does not imply a new data change |
| User variables / named prepared definitions | Session state; not restored by ordinary data ROLLBACK |
| Sequence allocation, created export files, HTTP/AI external calls | Side effects are not undone by a data ROLLBACK |

A failed DDL statement can leave the old object definition intact while earlier user DML has already committed. DDL atomic publication and rollback of the caller's transaction are separate guarantees. A live XA branch rejects commands that would implicitly commit it. Transaction-ending commands and access-mode restrictions are described in [Transactions](transaction/README.md).

COMMIT visibility and crash durability are also separate. Strict durability waits for the required WAL synchronization; relaxed or memory durability provides its configured guarantee. See [MVCC and locking](../mvcc-and-locking.md) and [persistence](../storage-format.md).

## Statement Failure and Locking

An explicit-transaction DML error rolls back the current statement. Earlier successful statements remain pending, except that a deadlock (1213/40001) ends the transaction. Lock timeouts (1205), NOWAIT conflicts (3572), and query interruption (1317) do not by themselves commit earlier work. Applications must decide whether to retry or roll back.

ROLLBACK TO SAVEPOINT restores later row changes and removes later savepoints; ordinary row, unique-key, and table locks remain until transaction end. RELEASE SAVEPOINT removes only the named savepoint. Plain reads and locking reads have different visibility and lock paths; RR does not provide InnoDB gap/next-key locks. See [isolation and wait strategies](../mvcc-and-locking.md#snapshot-lifecycle-and-isolation-levels).

Online DDL can allow DML during its build phase and still wait at preparation/publication barriers. Physical generation replacement can invalidate an older snapshot with 1412. Additive online indexes retain snapshot-specific facts instead. An accepted CANCEL request must be observed through the job view before treating cleanup as complete.

## Results and Diagnostics

| Result | Meaning |
| --- | --- |
| Ordinary result set | Columns and rows; a zero-row set still has a schema |
| OK packet | Statement status, affected rows and warnings; no ordinary row projection |
| DML RETURNING result set | One row per successful direct logical modification; see [row-image rules](../dml-returning.md) |
| Maintenance Table/Op/Msg_type/Msg_text | Per-object status; a Msg_type=error or note is a result row, not necessarily a protocol ERR or a retained SHOW WARNINGS row |
| Protocol ERR | Statement failure with errno, SQLSTATE and message |
| Async job result | Job accepted/current state; poll for completion and check Error |

Affected rows are operation-specific. INSERT normally counts one; an ODKU update counts two, or zero for a no-op unless CLIENT_FOUND_ROWS changes the no-op report to one. UPDATE normally reports changed rows; CLIENT_FOUND_ROWS requests matched rows. REPLACE counts inserted plus deleted conflicting rows. RETURNING uses its own row projection and does not redefine those underlying counts.

SHOW WARNINGS/ERRORS reads retained conditions. `warning_count`/`error_count` can exceed the number of retained rows when `max_error_count` limits storage. Read diagnostics immediately after the statement; intervening non-diagnostic statements can replace them. GET DIAGNOSTICS supports top-level @variables; STACKED is only available inside a handler.

## Session Lifetimes and Execution Context

| Resource | Lifetime |
| --- | --- |
| User variable and SQL PREPARE name | Current session; reset, reauthentication, or disconnect clears it |
| Session SQL cursor | Opened by DECLARE, advanced by FETCH; survives COMMIT; CLOSE/reset/reauthentication/disconnect releases it |
| Stored-program local variable/cursor/handler | Declaring block or invocation |
| RLS LOCAL context | Explicit transaction with savepoint restoration; ordinary context is session-scoped |
| Explicit table lock / FTWRL | Owning connection until release or session cleanup; see each lock statement |

PREPARE does not make an otherwise forbidden command legal. LOAD DATA is not preparable. Stored functions/triggers cannot use transaction boundaries, return arbitrary result sets, or execute dynamic SQL; ordinary procedures have broader capabilities. API Key administration is restricted to top-level MySQL sessions. REST independent requests use temporary sessions; persistent transaction/session state requires its session API. See [REST execution](../rest-api.md#execute-sql).
