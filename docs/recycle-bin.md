# Recycle Bin

The recycle bin retains persistent tables removed by `DROP TABLE`, `TRUNCATE
TABLE`, and `DROP DATABASE`. It is enabled by default, with a seven-day target
retention. Expiration and disk pressure can permanently remove retained objects.
It complements physical backups and PITR; retention is not a guaranteed backup.

## Administration

`RECYCLEBIN_ADMIN` is required to list, query, restore, or purge retained tables.
The bootstrap root account receives this dynamic privilege. Restoring also
requires `CREATE` on the destination. Revoking the privilege takes effect on
subsequent executions, including prepared statements. With MySQL logical binlog
enabled, grant or revoke this native privilege locally under `sql_log_bin=OFF`;
it has no MySQL 8.0 counterpart. Native WAL catalog replication is unaffected.

```sql
GRANT RECYCLEBIN_ADMIN ON *.* TO recovery_admin;
SHOW RECYCLEBIN;
SHOW RECYCLEBIN FROM app LIKE 'orders%';
SELECT * FROM information_schema.MYDB_RECYCLEBIN;

SELECT id, payload FROM __mydb_recyclebin__.r_12345 WHERE id = 100;
RESTORE TABLE r_12345 TO app.orders_recovered;

PURGE RECYCLEBIN r_12346;
PURGE RECYCLEBIN ALL;
```

Use the actual `RECYCLE_NAME` returned by SHOW. Each recycling operation allocates
a fresh name; repeated truncations and repeated use of the original table name
produce independent entries. `OPERATION_ID` groups entries created by one DDL.

`RESTORE TABLE name` defaults to the original database and table name. The
destination database must exist and the destination name must be unused.
TRUNCATE leaves the original table active, so restoring its old contents normally
requires `TO` with a different name. Restore neither overwrites nor merges data.
A successful restore consumes the entry. The destination receives a new logical
identity while retaining its physical data and indexes.

The protected `__mydb_recyclebin__` namespace supports ordinary read-only queries,
including joins, CTEs and subqueries. A retained table can be the source of
`INSERT ... SELECT` or CTAS. Direct writes, DDL, locking reads, historical AS OF
queries, and persistent views referring to retained tables are rejected. An old
transaction snapshot cannot cross the recycle/restore publication boundary;
retry that transaction after error 1412.

## Retained contents and dependencies

Columns, physical row layouts, data, partitioning, primary and secondary indexes,
CHECK constraints, table options, optimizer statistics and AUTO_INCREMENT state
are retained. Partitioned tables are recycled as a whole. Temporary tables,
system tables and individual partition removal are outside this feature.

Triggers, foreign keys, row-security policies and grants are not restored.
Independently named sequences and routines are not recreated. Existing grants on
the destination name remain effective; review destination access before restoring
data without its original row-security policies. Owned sequences
follow the original DROP semantics. `OMITTED_OBJECTS` and RESTORE warnings identify
removed attached objects. Restoring does not relax existing DROP/TRUNCATE foreign
key guards. Surviving foreign keys in other tables that reference the destination
remain effective: restore validates the target definition and rebuilds derived
reverse dependencies without adding constraints or rescanning child data.
CHECK names are restored and must be available in the destination schema.

Default and generated expressions are not silently rewritten. Restore rejects
missing expression dependencies, so recreate the required external objects
before retrying. Only already-supported expression forms are accepted.

DROP DATABASE retains its tables but still removes the database and its other
objects. Restore those entries into another existing database, or explicitly
recreate the original database first. Previously recycled objects survive a
later DROP DATABASE.

## Configuration and reclamation

| System variable | Scope | Default | Meaning |
| --- | --- | --- | --- |
| `mydb_recyclebin` | GLOBAL, SESSION | ON | Recycle subsequent eligible DDL |
| `mydb_recyclebin_retention_seconds` | GLOBAL | 604800 | Retention target for new entries; 1–31536000 seconds |
| `mydb_recyclebin_min_free_pct` | GLOBAL | 10 | Start pressure reclamation below this free-volume percentage; 0 disables it |
| `mydb_recyclebin_target_free_pct` | GLOBAL | 15 | Stop pressure reclamation at this percentage; greater than the minimum, at most 90 |
| `mydb_recyclebin_gc_interval_ms` | GLOBAL | 10000 | Background interval; 1000–3600000 ms |

The corresponding startup options are `recyclebin`,
`recyclebin_retention_seconds`, `recyclebin_min_free_pct`,
`recyclebin_target_free_pct` and `recyclebin_gc_interval_ms`; environment names
use the `MYDB_` prefix and uppercase spelling. Normal SET PERSIST rules apply.

```sql
SET SESSION mydb_recyclebin = OFF; -- subsequent DROP/TRUNCATE is permanent
SET GLOBAL mydb_recyclebin_retention_seconds = 86400;
SET GLOBAL mydb_recyclebin_min_free_pct = 0;
```

Unmodified sessions follow the current GLOBAL recycling policy; an explicit
SESSION setting overrides it. Turning recycling off does not purge existing
entries or stop their cleanup.
Each entry keeps its original expiration timestamp when retention settings change.
An expired entry remains readable and restorable until reclamation wins the
exclusive object lock. Pressure reclamation removes expired objects first and
then the oldest retained objects, using monotonic recycle IDs as their creation
order; a busy reader can delay a particular object.
`SHOW RECYCLEBIN` and `information_schema.MYDB_RECYCLEBIN` expose committed
entries. An uncommitted restore or purge does not hide the previous committed entry.
Background reclamation skips busy objects and catalog operations. Manual
RESTORE and PURGE honor `lock_wait_timeout` and query cancellation while waiting
for metadata or recycle-bin catalog locks.

Purge first commits the irreversible `PURGING` state. Physical deletion is
asynchronous and retryable, and RESTORE is rejected after that transition.
`PURGE ... ALL` commits batches separately and can partially complete on error.
Concurrent recycling can leave new entries for a subsequent PURGE ALL.
`LAST_ERROR`, `PURGE_REASON`, and `STATE` expose pending cleanup. `DATA_BYTES` is
the observed file length at recycling, not an exact allocated-space guarantee.
SHOW STATUS exposes `Mydb_recyclebin_pressure`,
`Mydb_recyclebin_pressure_purges`, and `Mydb_recyclebin_cleanup_failures`.

## Durability, replication and recovery

Catalog ownership changes are transactional. File deletion follows a durable
purge decision and WAL records; interrupted cleanup is idempotent. Recycled data
remains subject to ordinary MVCC and LOB maintenance without extending global
undo retention. Existing durability-mode guarantees continue to apply.

Native WAL replicas follow source recycle, restore and purge decisions; they do
not independently expire objects or delete them under local disk pressure.
Physical backups and base backups include retained objects. A read-only backup
image permits queries but never runs reclamation or RESTORE; promote an
independent recovered image before restoring a table. Opening a writable
recovered image resumes normal expiration and pressure cleanup. For extraction
of already-expired entries, configure a longer GC interval before opening that
image, or inspect and copy data from its read-only form.

MySQL ROW/FULL input applies DROP/TRUNCATE using the receiver's local recycle
policy. Ordinary outgoing DROP/TRUNCATE events retain their original form.
With logical binlog enabled, RESTORE emits atomic CREATE plus full row events;
the destination does not need a matching recycle bin. Its event body uses
quota-controlled temporary storage, and recovery time includes a full data scan.
The existing 4 GiB binlog transaction/file-position bound still applies. Native
definitions that cannot be represented in MySQL 8.0 logical events are rejected
before publication; use native replication or explicitly disable `sql_log_bin`
for that restore. Without logical binlog, restore does not copy the table or
rebuild its indexes.
