# mydb SQL Statement Reference

This directory is a continuously maintained manual of the SQL statements currently visible to mydb users. Documentation is organized by canonical statement; aliases and
deprecated spellings are folded into the recommended syntax, and internal parser wrapper
nodes do not form part of the user interface. For syntax notation, see
[syntax conventions](syntax-conventions.md), [shared grammar terms](grammar.md),
[execution contracts](execution-contracts.md), and [runnable example setup](examples.md).

## Categories

| Category | Contents |
| --- | --- |
| [Query Statements](query/README.md) | SELECT, set queries, CTEs, EXPLAIN, result export, and expression execution |
| [Data Manipulation](dml/README.md) | INSERT, REPLACE, UPDATE, DELETE, LOAD DATA/XML |
| [Data Definition](ddl/README.md) | database, table, index, view, sequence; TABLESPACE recognition and rejection |
| [Transactions and Sessions](transaction/README.md) | Transaction control, savepoints, table locks, XA, variables, and session character sets |
| [Stored Programs](programmatic/README.md) | procedure/function, trigger, event, CALL, dynamic SQL, and program control flow |
| [Accounts and Privileges](account/README.md) | user, role, GRANT/REVOKE, passwords, and role activation |
| [Metadata and Diagnostics](metadata/README.md) | USE, SHOW, DESCRIBE, the diagnostics area, and mydb status views |
| [Maintenance and Server Administration](admin/README.md) | Table maintenance, FLUSH, KILL, checkpoint, components, and process control |
| [Replication and Binlog](replication/README.md) | MySQL replication, binlog management, and mydb native replication |
| [Backup](backup/README.md) | Online backup, jobs, repository listings, and cancellation |
| [Row Security](security/README.md) | Policies, enforcement and trusted RLS context |
| [AI and HTTP Administration](integration/README.md) | AI objects, external-call policy reload, encryption and HTTP status |

## Inclusion Criteria

- Each page describes one independently identifiable canonical statement with well-defined execution semantics; aliases with exactly the same semantics do not get a separate document.
- Subordinate constructs such as `WITH`, set operations, `RETURNING`, windows, partitioning, and hints are described within their owning statement and linked to dedicated documentation.
- Commands whose syntax is accepted but that only retain metadata, return an empty compatibility result, or produce a deprecation warning are still included, and their effect is stated explicitly.
- AST-internal wrapper states are not user statements. Explicit rejection references for RESOURCE GROUP and TABLESPACE are retained so imports and client probes have a documented error contract. DELIMITER is documented only as a client directive.
- The MySQL replica receiver is included in every build; receiving requires source and channel configuration.

## Statement Matrix

The table lists canonical statement entry points; slash-separated names share a page. In-routine constructs require a stored-program body. DELIMITER is client-only. Entries explicitly marked unsupported are recognized for error reporting and are not executable capabilities. Privileges and context restrictions still apply to every executable entry.

### Query and Data Manipulation

| Category | Statements |
| --- | --- |
| Query | [SELECT](query/select.md), [SELECT INTO variables](query/select-into-variables.md), [SELECT INTO OUTFILE/DUMPFILE](query/select-into-file.md), [EXPLAIN](query/explain.md), [EXPLAIN FOR CONNECTION](query/explain-for-connection.md), [DESCRIBE/DESC](query/describe.md), [DO](query/do.md) |
| DML | [INSERT](dml/insert.md), [REPLACE](dml/replace.md), [UPDATE](dml/update.md), [DELETE](dml/delete.md), [LOAD DATA](dml/load-data.md), [LOAD XML](dml/load-xml.md) |

### Data Definition

| Object | Statements |
| --- | --- |
| Database | [CREATE DATABASE/SCHEMA](ddl/create-database.md), [ALTER DATABASE/SCHEMA](ddl/alter-database.md), [DROP DATABASE/SCHEMA](ddl/drop-database.md) |
| Table | [CREATE TABLE](ddl/create-table.md), [CREATE TABLE LIKE](ddl/create-table-like.md), [CREATE TABLE AS SELECT](ddl/create-table-as-select.md), [ALTER TABLE](ddl/alter-table.md), [DROP TABLE](ddl/drop-table.md), [TRUNCATE TABLE](ddl/truncate-table.md), [RENAME TABLE](ddl/rename-table.md) |
| Index | [CREATE INDEX](ddl/create-index.md), [DROP INDEX](ddl/drop-index.md) |
| View | [CREATE VIEW](ddl/create-view.md), [ALTER VIEW](ddl/alter-view.md), [DROP VIEW](ddl/drop-view.md) |
| Sequence | [CREATE SEQUENCE](ddl/create-sequence.md), [ALTER SEQUENCE](ddl/alter-sequence.md), [DROP SEQUENCE](ddl/drop-sequence.md) |
| Tablespace (unsupported: 1235) | [CREATE TABLESPACE / UNDO TABLESPACE](ddl/create-tablespace.md), [ALTER TABLESPACE / UNDO TABLESPACE](ddl/alter-tablespace.md), [DROP TABLESPACE / UNDO TABLESPACE](ddl/drop-tablespace.md) |

### Transactions and Sessions

| Group | Statements |
| --- | --- |
| Transactions | [START TRANSACTION/BEGIN](transaction/start-transaction.md), [COMMIT](transaction/commit.md), [ROLLBACK](transaction/rollback.md), [SAVEPOINT](transaction/savepoint.md), [ROLLBACK TO SAVEPOINT](transaction/rollback-to-savepoint.md), [RELEASE SAVEPOINT](transaction/release-savepoint.md) |
| Locks and characteristics | [LOCK TABLES](transaction/lock-tables.md), [UNLOCK TABLES](transaction/unlock-tables.md), [SET TRANSACTION](transaction/set-transaction.md), [SET AUTOCOMMIT](transaction/set-autocommit.md) |
| Session settings | [SET](transaction/set.md), [RESET PERSIST](transaction/reset-persist.md), [SET NAMES](transaction/set-names.md), [SET CHARACTER SET/CHARSET](transaction/set-character-set.md), [SET RESOURCE GROUP](transaction/set-resource-group.md) |
| XA | [XA START](transaction/xa-start.md), [XA END](transaction/xa-end.md), [XA PREPARE](transaction/xa-prepare.md), [XA COMMIT](transaction/xa-commit.md), [XA ROLLBACK](transaction/xa-rollback.md), [XA RECOVER](transaction/xa-recover.md) |

### Stored Programs and Scheduling

| Group | Statements |
| --- | --- |
| Procedure | [CREATE PROCEDURE](programmatic/create-procedure.md), [ALTER PROCEDURE](programmatic/alter-procedure.md), [DROP PROCEDURE](programmatic/drop-procedure.md), [CALL](programmatic/call.md) |
| Function | [CREATE FUNCTION](programmatic/create-function.md), [ALTER FUNCTION](programmatic/alter-function.md), [DROP FUNCTION](programmatic/drop-function.md) |
| Trigger/Event | [CREATE TRIGGER](programmatic/create-trigger.md), [DROP TRIGGER](programmatic/drop-trigger.md), [CREATE EVENT](programmatic/create-event.md), [ALTER EVENT](programmatic/alter-event.md), [DROP EVENT](programmatic/drop-event.md) |
| Dynamic SQL | [PREPARE](programmatic/prepare.md), [EXECUTE](programmatic/execute.md), [DEALLOCATE/DROP PREPARE](programmatic/deallocate-prepare.md) |
| Session cursors | [DECLARE CURSOR](programmatic/declare-cursor.md), [FETCH](programmatic/fetch-cursor.md), [CLOSE](programmatic/close-cursor.md) |
| Client directive (not SQL) | [DELIMITER](programmatic/delimiter.md) |
| In-routine declarations | [BEGIN…END](programmatic/begin-end.md), [DECLARE variable](programmatic/declare-variable.md), [DECLARE CONDITION](programmatic/declare-condition.md), [DECLARE CURSOR](programmatic/declare-routine-cursor.md), [DECLARE HANDLER](programmatic/declare-handler.md) |
| In-routine assignment/query | [SET](programmatic/routine-set.md), [SELECT INTO](programmatic/select-into.md) |
| In-routine control flow | [IF](programmatic/if.md), [CASE](programmatic/case.md), [LOOP](programmatic/loop.md), [WHILE](programmatic/while.md), [REPEAT](programmatic/repeat.md), [LEAVE](programmatic/leave.md), [ITERATE](programmatic/iterate.md), [RETURN](programmatic/return.md) |
| In-routine cursors | [OPEN](programmatic/open.md), [FETCH](programmatic/fetch.md), [CLOSE](programmatic/close.md) |
| In-routine conditions | [SIGNAL](programmatic/signal.md), [RESIGNAL](programmatic/resignal.md) |
| Diagnostics (top-level and in routine) | [GET DIAGNOSTICS](programmatic/get-diagnostics.md); STACKED is handler-only |

### Accounts and Privileges

| Group | Statements |
| --- | --- |
| User | [CREATE USER](account/create-user.md), [ALTER USER](account/alter-user.md), [DROP USER](account/drop-user.md), [RENAME USER](account/rename-user.md), [SET PASSWORD](account/set-password.md) |
| Role | [CREATE ROLE](account/create-role.md), [DROP ROLE](account/drop-role.md), [SET ROLE](account/set-role.md), [SET DEFAULT ROLE](account/set-default-role.md) |
| Granting | [GRANT](account/grant.md), [REVOKE](account/revoke.md) |
| Viewing | [SHOW GRANTS](account/show-grants.md), [SHOW CREATE USER](account/show-create-user.md), [SHOW PRIVILEGES](account/show-privileges.md) |

### Metadata and Diagnostics

| Group | Statements |
| --- | --- |
| Schema/Table | [USE](metadata/use.md), [SHOW DATABASES/SCHEMAS](metadata/show-databases.md), [SHOW TABLES](metadata/show-tables.md), [SHOW TABLE STATUS](metadata/show-table-status.md), [SHOW COLUMNS/FIELDS](metadata/show-columns.md), [SHOW INDEX/KEYS](metadata/show-index.md) |
| SHOW CREATE | [DATABASE](metadata/show-create-database.md), [TABLE/VIEW](metadata/show-create-table.md), [SEQUENCE](metadata/show-create-sequence.md), [PROCEDURE/FUNCTION](metadata/show-create-routine.md), [TRIGGER](metadata/show-create-trigger.md), [EVENT](metadata/show-create-event.md) |
| Object listings | [SHOW SEQUENCES](metadata/show-sequences.md), [PROCEDURE/FUNCTION STATUS](metadata/show-routine-status.md), [SHOW TRIGGERS](metadata/show-triggers.md), [SHOW EVENTS](metadata/show-events.md) |
| Character sets/variables | [SHOW CHARACTER SET/CHARSET](metadata/show-character-set.md), [SHOW COLLATION](metadata/show-collation.md), [SHOW VARIABLES](metadata/show-variables.md), [SHOW STATUS](metadata/show-status.md) |
| Diagnostics/status | [SHOW WARNINGS/ERRORS/COUNT](metadata/show-diagnostics.md), [SHOW ENGINES](metadata/show-engines.md), [SHOW ENGINE](metadata/show-engine.md), [SHOW MYDB MEMORY](metadata/show-mydb-memory.md), [SHOW MYDB MEMORY SESSIONS](metadata/show-mydb-memory-sessions.md), [SHOW PLUGINS](metadata/show-plugins.md), [SHOW OPEN TABLES](metadata/show-open-tables.md), [SHOW PROCESSLIST](metadata/show-processlist.md) |

### Maintenance, Replication, and Backup

| Group | Statements |
| --- | --- |
| Table maintenance | [ANALYZE TABLE](admin/analyze-table.md), [CHECK TABLE](admin/check-table.md), [REPAIR TABLE](admin/repair-table.md), [OPTIMIZE TABLE](admin/optimize-table.md), [CHECKSUM TABLE](admin/checksum-table.md) |
| FLUSH | [PRIVILEGES](admin/flush-privileges.md), [USER_RESOURCES](admin/flush-user-resources.md), [HOSTS](admin/flush-hosts.md), [STATUS](admin/flush-status.md), [TABLES](admin/flush-tables.md), [LOGS](admin/flush-logs.md), [QUERY CACHE](admin/flush-query-cache.md) |
| Server administration | [KILL](admin/kill.md), [SHUTDOWN](admin/shutdown.md), [RESTART](admin/restart.md), [CHECKPOINT](admin/alter-system-checkpoint.md), [FLUSH DATA_CACHE](admin/alter-system-flush-data-cache.md), [FLUSH SQL_CACHE](admin/alter-system-flush-sql-cache.md), [CANCEL DDL JOB](admin/cancel-ddl-job.md) |
| Resource group (unsupported: 1235) / built-in component | [CREATE](admin/create-resource-group.md), [ALTER](admin/alter-resource-group.md), [DROP RESOURCE GROUP](admin/drop-resource-group.md), [INSTALL COMPONENT](admin/install-component.md), [UNINSTALL COMPONENT](admin/uninstall-component.md) |
| Binlog source | [SHOW BINARY LOG STATUS](replication/show-binary-log-status.md), [SHOW BINARY LOGS](replication/show-binary-logs.md), [SHOW REPLICAS](replication/show-replicas.md), [FLUSH BINARY LOGS](replication/flush-binary-logs.md), [RESET BINARY LOGS AND GTIDS](replication/reset-binary-logs.md), [SHOW BINLOG EVENTS](replication/show-binlog-events.md), [PURGE BINARY LOGS](replication/purge-binary-logs.md), [legacy profiler](replication/show-profiles.md) |
| MySQL→mydb | [CHANGE REPLICATION SOURCE TO](replication/change-replication-source.md), [START REPLICA](replication/start-replica.md), [STOP REPLICA](replication/stop-replica.md), [RESET REPLICA](replication/reset-replica.md), [SHOW REPLICA STATUS](replication/show-replica-status.md), [SHOW RELAYLOG EVENTS](replication/show-relaylog-events.md); the receiver is included in every build |
| mydb→mydb | [MASTER STATUS](replication/show-mydb-master-status.md), [REPLICA STATUS](replication/show-mydb-replica-status.md), [REPLICAS](replication/show-mydb-replicas.md), [CHANGE MASTER](replication/change-mydb-master.md), [CHANGE SEQUENCE SYNC](replication/change-mydb-sequence-sync.md), [START](replication/start-mydb-replication.md), [STOP](replication/stop-mydb-replication.md), [RESET](replication/reset-mydb-replication.md), [STOP WRITES](replication/stop-master-writes.md), [RESUME WRITES](replication/resume-master-writes.md), [PROMOTE](replication/promote-to-master.md), [DEMOTE](replication/demote-to-replica.md), [DROP REPLICA](replication/drop-mydb-replica.md) |
| Backup | [BACKUP DATABASE](backup/backup-database.md), [BACKUP WAL](backup/backup-wal.md), [ALTER MYDB BACKUP](backup/alter-backup.md), [SHOW MYDB BACKUP JOBS](backup/show-backup-jobs.md), [SHOW MYDB BACKUP STATUS](backup/show-backup-status.md), [SHOW MYDB BACKUPS](backup/show-backups.md), [CANCEL MYDB BACKUP](backup/cancel-backup.md) |

### mydb Extension Statements

| Family | Statements |
| --- | --- |
| API credentials | [CREATE API KEY](account/create-api-key.md), [SHOW API KEY](account/show-api-key.md), [SHOW API KEYS](account/show-api-keys.md), [DROP API KEY](account/drop-api-key.md), [ROTATE API KEY MASTER KEY](account/rotate-api-key-master-key.md) |
| Row policies | [CREATE POLICY](security/create-policy.md), [ALTER POLICY](security/alter-policy.md), [DROP POLICY](security/drop-policy.md), [SHOW CREATE POLICY](security/show-create-policy.md), [ENABLE/DISABLE ROW LEVEL SECURITY](security/alter-row-security.md) |
| Trusted context | [SET RLS CONTEXT](security/set-rls-context.md), [RESET RLS CONTEXT](security/reset-rls-context.md) |
| AI secret | [CREATE AI SECRET](integration/create-ai-secret.md), [ALTER AI SECRET](integration/alter-ai-secret.md), [DROP AI SECRET](integration/drop-ai-secret.md), [SHOW AI SECRETS](integration/show-ai-secrets.md), [SHOW CREATE AI SECRET](integration/show-create-ai-secret.md) |
| AI connection | [CREATE AI CONNECTION](integration/create-ai-connection.md), [ALTER AI CONNECTION](integration/alter-ai-connection.md), [DROP AI CONNECTION](integration/drop-ai-connection.md), [SHOW AI CONNECTIONS](integration/show-ai-connections.md), [SHOW CREATE AI CONNECTION](integration/show-create-ai-connection.md) |
| AI model | [CREATE AI MODEL](integration/create-ai-model.md), [ALTER AI MODEL](integration/alter-ai-model.md), [DROP AI MODEL](integration/drop-ai-model.md), [SHOW AI MODELS](integration/show-ai-models.md), [SHOW CREATE AI MODEL](integration/show-create-ai-model.md) |
| AI administration | [RELOAD AI CONFIG](integration/reload-ai-config.md), [ROTATE AI ENCRYPTION KEY](integration/rotate-ai-encryption-key.md), [GRANT](account/grant.md#behavior-and-limitations)/[REVOKE](account/revoke.md) AI_USE ON AI MODEL |
| HTTP administration | [RELOAD HTTP CONFIG](integration/reload-http-config.md), [SHOW HTTP STATUS](integration/show-http-status.md) |

## Support and Compatibility Status

| Status | Entries |
| --- | --- |
| Explicitly unsupported | RESOURCE GROUP, TABLESPACE, HANDLER, RESOURCE scheduling/management, X Protocol, plugin ABI and group replication; recognized management forms return an error rather than creating catalog objects |
| Client-only | DELIMITER; rejected if sent as server SQL |
| Diagnostic compatibility | SHOW PROFILE/PROFILES returns empty result schemas; FLUSH/RESET QUERY CACHE returns warning 1287 without a result cache |
| Accepted options with no distinct effect | CHECK/REPAIR compatibility modifiers, CHECKSUM QUICK, and DROP TABLE/VIEW RESTRICT/CASCADE; each owning page states the current limitation |
| Metadata-only/declarative properties | Declared routine determinism/data-access characteristics and documented table compatibility options do not promise a separate physical engine or purity proof |

Use the linked page for exact syntax, privileges, errors and transaction boundaries. The matrix describes SQL entry points, not a promise that all MySQL option combinations are supported.
