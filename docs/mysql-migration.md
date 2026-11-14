# Migrating from MySQL 8.0

Use the [compatibility reference](mysql80-compatibility.md) to preflight the actual
application. MyDB has native heap storage; it cannot open an InnoDB data directory
or restore a MySQL physical backup. RC is the default and RR does not provide
InnoDB gap/next-key locking. Validate transaction behavior as well as SQL syntax.

## Preflight

Inventory tables, partitions, generated columns, character sets, collations,
constraints, routines, triggers, events, accounts and grants. Reject or adapt
unsupported TABLESPACE, RESOURCE GROUP, plugin and engine features before import.
Check unsigned/DECIMAL precision, zero dates, TIMESTAMP ranges, JSON semantics and
case-sensitive application assumptions. Disable destination scheduled events
while loading and ensure no independent destination writers are active.

## Consistent initial data

For the simplest rehearsal, stop all source writers, including jobs/events/DDL,
and keep them stopped until the snapshot and its binlog coordinate are recorded.
On MySQL configure `binlog_format=ROW` and `binlog_row_image=FULL`; existing sessions
must also use that format. Record SHOW MASTER STATUS and GTID state at the frozen
boundary. Retain all subsequent logs until the receiver has applied them.

Export SHOW CREATE definitions and typed table data with a driver, then apply
reviewed definitions and parameterized INSERTs on MyDB. Keep exact numerics and
binary values lossless. Load referenced tables before dependent ones. Import
stored programs with whole-statement API requests (DELIMITER is a client command).
Prevent trigger side effects from duplicating already-exported rows: install
triggers after loading data. Install routines, events, users/grants and RLS only
with reviewed semantics. Native features require separate handling.

A mysqldump file is SQL input, not a blanket compatibility guarantee. Inspect its
GTID, lock, tablespace, definer and session-setting statements; test the exact file
on a disposable target and fix unsupported statements explicitly. Do not silently
ignore client errors during import.

For an online snapshot, use tooling that establishes a consistent snapshot and
matching binlog/GTID coordinate, including DDL coordination. Separate SELECTs
under MyDB's RC or arbitrary source queries plus a later status read do not form
such a snapshot. If that coordination is unavailable, use the stopped-writer path.

## Continue from the captured coordinate

After loading the complete snapshot, configure a dedicated source replication
account and secure transport. Use the real captured file and position:

```sql
CHANGE REPLICATION SOURCE TO
  SOURCE_HOST='mysql.example', SOURCE_PORT=3306,
  SOURCE_USER='repl', SOURCE_PASSWORD='replace-with-protected-credential',
  SOURCE_LOG_FILE='binlog.000123', SOURCE_LOG_POS=456,
  SOURCE_SSL_MODE='VERIFY_IDENTITY', SOURCE_SSL_CA='/etc/mydb/source-ca.pem'
FOR CHANNEL 'migration';
START REPLICA FOR CHANNEL 'migration';
SHOW REPLICA STATUS FOR CHANNEL 'migration';
```

The filename/position are illustrative; never use them or the first available log
as a substitute for the snapshot boundary. GTID auto-position also requires the
correct imported executed state, not just SOURCE_AUTO_POSITION=1. See
[receiver configuration](statement/replication/change-replication-source.md).
Resume source writes only after its subsequent history is retained and the
snapshot boundary is fixed. Inspect both receiver and applier errors. Replication
SQL success alone does not mean data is caught up.

## Validate and switch

Stop source writers again, capture the final source coordinate, and wait until
MyDB applies it. Compare counts, key ranges and canonical ordered content digests
at this common boundary. Include multilingual text, bytes, exact DECIMAL, temporal
precision, JSON, FK relationships and application transactions. After stopping the receiver at the validated boundary, exercise imported
routines/triggers/events separately, with events running on only one writer.
The receiver's read-only protection can reject CALL even for a read-only procedure.
After confirming all relevant IO/SQL threads have stopped at the common boundary,
inspect `@@GLOBAL.mydb_replica_read_only`; STOP alone can leave it ON. For the
planned cutover, explicitly enable local execution:

```sql
STOP REPLICA FOR CHANNEL 'migration';
SHOW REPLICA STATUS FOR CHANNEL 'migration';
SET GLOBAL mydb_replica_read_only = OFF;
```

Do not disable this guard while a receiver is still applying. Preserve the final
status and configure `skip_replica_start=ON` for subsequent starts if retaining
the channel, or explicitly remove its configuration after recording recovery
coordinates. A process restart must not restart source apply over local writes.
Switch application connections only after validation and stop the migration
receiver before allowing independent destination writes.

Keep the frozen source until acceptance. Before destination writes, rollback can
return clients to that unchanged source. After destination writes, it requires
reconciliation or a separately validated reverse replication plan. Never assume
native features, RLS or vector values can flow to stock MySQL. See
[version changes](changing-versions.md) and [replication boundaries](mysql80-compatibility.md#10-replication).
