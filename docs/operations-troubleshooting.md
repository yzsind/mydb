# Operations Troubleshooting

Use an administrative diagnostic account with PROCESS and relevant subsystem
privileges. Capture the exact build, time, error/SQLSTATE and affected query before
changing settings. These snapshots can change during collection; an empty or
disabled collector does not prove absence of a problem.

| Symptom | First inspection | Next action |
| --- | --- | --- |
| Slow SQL | EXPLAIN ANALYZE, mysql.slow_log | Inspect actual rows, estimates, access paths and spill; ANALYZE TABLE if statistics are stale |
| Data-lock wait | information_schema.INNODB_TRX / INNODB_LOCK_WAITS | Identify blocker transaction and its owner; finish or cancel deliberately |
| DDL wait | performance_schema.metadata_locks | Check long transactions, explicit table locks and pending MDL |
| Memory admission 1037 | SHOW MYDB MEMORY, SHOW MYDB MEMORY SESSIONS | Reduce concurrent work or per-query footprint; inspect OS/container capacity |
| Spill / temporary I/O | SHOW GLOBAL STATUS LIKE 'Mydb_setop_spill%' | Compare counter deltas and plans, verify temporary-disk headroom |
| WAL growth | SHOW ENGINE MYDB STATUS; replication and backup status | Find retaining transactions, replicas or archive obligations before cleanup |
| Replica behind | SHOW MYDB REPLICA STATUS / SHOW REPLICA STATUS | Inspect error and progress watermarks; do not compare a native LSN to a binlog position |
| Backup failed | SHOW MYDB BACKUP JOBS; SHOW MYDB BACKUP STATUS | Inspect Error/Phase and archive health, then rehearse recovery |
| Recycle-bin pressure | SHOW RECYCLEBIN; Mydb_recyclebin counters | Inspect expiration, pressure and cleanup failures; retention is not a backup guarantee |

## Ready-to-run snapshots

```sql
SELECT trx_id, trx_state, trx_started, trx_mysql_thread_id
FROM information_schema.INNODB_TRX ORDER BY trx_started;
SELECT * FROM information_schema.INNODB_LOCK_WAITS;
SELECT object_schema, object_name, lock_type, lock_status, owner_thread_id
FROM performance_schema.metadata_locks
WHERE lock_status IN ('GRANTED','PENDING');
SHOW MYDB MEMORY;
SHOW GLOBAL STATUS LIKE 'Mydb_setop_spill%';
SHOW GLOBAL STATUS LIKE 'Mydb_recyclebin%';
SHOW ENGINE MYDB STATUS;
```

Sample cumulative counters twice and compute deltas over a known time interval.
Do not treat a total as a rate. [SHOW STATUS](show-status.md) explains reset and
session/global scope; engine and spill counters need not reset with FLUSH STATUS.
Memory sizes are bytes unless the field explicitly uses another unit. PFS can
skip optional collection when its budget is exhausted; inspect setup consumers,
instruments and lost counters before interpreting empty history.

Use [query plans](optimizer-and-operators.md#how-to-read-and-diagnose-plans),
[locking diagnostics](mvcc-and-locking.md#diagnostics-and-troubleshooting),
[backup troubleshooting](backup-and-recovery.md#123-common-problems) and
[native replication](native-replication.md) for the next step. Preserve the data
directory and logs after corruption or failed recovery; do not delete WAL or index
files to make a status counter smaller. Follow [issue reporting](reporting-issues.md).
