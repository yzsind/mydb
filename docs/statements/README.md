# mydb SQL Statement Reference

This directory is a continuously maintained manual of the SQL statements currently visible to mydb users. Documentation is organized by canonical statement; aliases and
deprecated spellings are folded into the recommended syntax, and internal parser wrapper
nodes do not form part of the user interface. For syntax notation, see
[`syntax-conventions.md`](syntax-conventions.md).

## Categories

| Category | Contents |
| --- | --- |
| [Query Statements](query/README.md) | SELECT, set queries, CTEs, EXPLAIN, result export, and expression execution |
| [Data Manipulation](dml/README.md) | INSERT, REPLACE, UPDATE, DELETE, LOAD DATA/XML |
| [Data Definition](ddl/README.md) | database, table, index, view, sequence, tablespace |
| [Transactions and Sessions](transaction/README.md) | Transaction control, savepoints, table locks, XA, variables, and session character sets |
| [Stored Programs](programmatic/README.md) | procedure/function, trigger, event, CALL, dynamic SQL, and program control flow |
| [Accounts and Privileges](account/README.md) | user, role, GRANT/REVOKE, passwords, and role activation |
| [Metadata and Diagnostics](metadata/README.md) | USE, SHOW, DESCRIBE, the diagnostics area, and mydb status views |
| [Maintenance and Server Administration](admin/README.md) | Table maintenance, FLUSH, KILL, checkpoint, components, and process control |
| [Replication and Binlog](replication/README.md) | MySQL replication, binlog management, and mydb native replication |
| [Backup](backup/README.md) | Online backup, jobs, repository listings, and cancellation |

## Inclusion Criteria

- Each page describes one independently identifiable canonical statement with well-defined execution semantics; aliases with exactly the same semantics do not get a separate document.
- Subordinate constructs such as `WITH`, set operations, `RETURNING`, windows, partitioning, and hints are described within their owning statement and linked to dedicated documentation.
- Commands whose syntax is accepted but that only retain metadata, return an empty compatibility result, or produce a deprecation warning are still included, and their effect is stated explicitly.
- AST-internal states such as `Unsupported` and `MultiStatement` are not included; adjacent syntax that is explicitly unsupported is listed only in the limitations section of the related documentation.
- Commands marked as "requires the `mysql-replica` feature" are still recognized in the default build, but their execution capability depends on build features and runtime configuration.

## Complete Statement Matrix

The table below lists all canonical statement entry points; slash-separated names are synonyms or old/new spellings within the same page. Statements internal
to stored programs are marked "in routine"; all others can be sent as top-level statements at the protocol layer.

### Query and Data Manipulation

| Category | Statements |
| --- | --- |
| Query | [SELECT](query/select.md), [SELECT INTO variables](query/select-into-variables.md), [SELECT INTO OUTFILE/DUMPFILE](query/select-into-file.md), [EXPLAIN](query/explain.md), [EXPLAIN FOR CONNECTION](query/explain-for-connection.md), [DESCRIBE/DESC](query/describe.md), [DO](query/do.md) |
| DML | [INSERT](dml/insert.md), [REPLACE](dml/replace.md), [UPDATE](dml/update.md), [DELETE](dml/delete.md), [LOAD DATA](dml/load-data.md), [LOAD XML](dml/load-xml.md) |

### Data Definition

| Object | Statements |
| --- | --- |
| Database | [CREATE DATABASE/SCHEMA](ddl/create-database.md), [DROP DATABASE/SCHEMA](ddl/drop-database.md) |
| Table | [CREATE TABLE](ddl/create-table.md), [CREATE TABLE LIKE](ddl/create-table-like.md), [CREATE TABLE AS SELECT](ddl/create-table-as-select.md), [ALTER TABLE](ddl/alter-table.md), [DROP TABLE](ddl/drop-table.md), [TRUNCATE TABLE](ddl/truncate-table.md), [RENAME TABLE](ddl/rename-table.md) |
| Index | [CREATE INDEX](ddl/create-index.md), [DROP INDEX](ddl/drop-index.md) |
| View | [CREATE VIEW](ddl/create-view.md), [ALTER VIEW](ddl/alter-view.md), [DROP VIEW](ddl/drop-view.md) |
| Sequence | [CREATE SEQUENCE](ddl/create-sequence.md), [ALTER SEQUENCE](ddl/alter-sequence.md), [DROP SEQUENCE](ddl/drop-sequence.md) |
| Tablespace | [CREATE TABLESPACE / UNDO TABLESPACE](ddl/create-tablespace.md), [ALTER TABLESPACE / UNDO TABLESPACE](ddl/alter-tablespace.md), [DROP TABLESPACE / UNDO TABLESPACE](ddl/drop-tablespace.md) |

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
| Client compatibility | [DELIMITER](programmatic/delimiter.md) |
| In-routine declarations | [BEGIN…END](programmatic/begin-end.md), [DECLARE variable](programmatic/declare-variable.md), [DECLARE CONDITION](programmatic/declare-condition.md), [DECLARE CURSOR](programmatic/declare-routine-cursor.md), [DECLARE HANDLER](programmatic/declare-handler.md) |
| In-routine assignment/query | [SET](programmatic/routine-set.md), [SELECT INTO](programmatic/select-into.md) |
| In-routine control flow | [IF](programmatic/if.md), [CASE](programmatic/case.md), [LOOP](programmatic/loop.md), [WHILE](programmatic/while.md), [REPEAT](programmatic/repeat.md), [LEAVE](programmatic/leave.md), [ITERATE](programmatic/iterate.md), [RETURN](programmatic/return.md) |
| In-routine cursors | [OPEN](programmatic/open.md), [FETCH](programmatic/fetch.md), [CLOSE](programmatic/close.md) |
| In-routine conditions | [SIGNAL](programmatic/signal.md), [RESIGNAL](programmatic/resignal.md), [GET DIAGNOSTICS](programmatic/get-diagnostics.md) |

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
| Resource group/Component | [CREATE](admin/create-resource-group.md), [ALTER](admin/alter-resource-group.md), [DROP RESOURCE GROUP](admin/drop-resource-group.md), [INSTALL COMPONENT](admin/install-component.md), [UNINSTALL COMPONENT](admin/uninstall-component.md) |
| Binlog source | [SHOW BINARY LOG STATUS](replication/show-binary-log-status.md), [SHOW BINARY LOGS](replication/show-binary-logs.md), [SHOW REPLICAS](replication/show-replicas.md), [FLUSH BINARY LOGS](replication/flush-binary-logs.md), [RESET BINARY LOGS AND GTIDS](replication/reset-binary-logs.md), [SHOW BINLOG EVENTS](replication/show-binlog-events.md), [PURGE BINARY LOGS](replication/purge-binary-logs.md), [SHOW PROFILES/PROFILE](replication/show-profiles.md) |
| MySQL→mydb | [CHANGE REPLICATION SOURCE TO](replication/change-replication-source.md), [START REPLICA](replication/start-replica.md), [STOP REPLICA](replication/stop-replica.md), [RESET REPLICA](replication/reset-replica.md), [SHOW REPLICA STATUS](replication/show-replica-status.md), [SHOW RELAYLOG EVENTS](replication/show-relaylog-events.md); running the receiving side requires the `mysql-replica` feature |
| mydb→mydb | [MASTER STATUS](replication/show-mydb-master-status.md), [REPLICA STATUS](replication/show-mydb-replica-status.md), [REPLICAS](replication/show-mydb-replicas.md), [CHANGE MASTER](replication/change-mydb-master.md), [CHANGE SEQUENCE SYNC](replication/change-mydb-sequence-sync.md), [START](replication/start-mydb-replication.md), [STOP](replication/stop-mydb-replication.md), [RESET](replication/reset-mydb-replication.md), [STOP WRITES](replication/stop-master-writes.md), [RESUME WRITES](replication/resume-master-writes.md), [PROMOTE](replication/promote-to-master.md), [DEMOTE](replication/demote-to-replica.md), [DROP REPLICA](replication/drop-mydb-replica.md) |
| Backup | [BACKUP DATABASE](backup/backup-database.md), [BACKUP WAL](backup/backup-wal.md), [ALTER MYDB BACKUP](backup/alter-backup.md), [SHOW BACKUP JOBS](backup/show-backup-jobs.md), [SHOW BACKUP STATUS](backup/show-backup-status.md), [SHOW BACKUPS](backup/show-backups.md), [CANCEL BACKUP](backup/cancel-backup.md) |
