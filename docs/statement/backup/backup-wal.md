# `BACKUP WAL`

## Syntax

```ebnf
<backup-wal> ::= 'BACKUP WAL TO' <string-literal>
```

## Behavior

```sql
BACKUP WAL TO 'file:///tmp/mydb/repository';
```

Captures a fixed source LSN/time target, transfers only new WAL and waits through
an asynchronous job for durable repository coverage. Poll `SHOW MYDB BACKUP JOBS`;
the completed row includes Requested_LSN, Requested_Source_Time,
Durable_Through_LSN, Covered_Time and Catalog_Generation. Idle intervals use
checked time fences. A receipt never claims unacknowledged source history.

Requires an enabled MANUAL or CONTINUOUS binding and an eligible full baseline.
DISABLED and WAITING_BASE reject the operation. No database image is copied or
scanned. In CONTINUOUS this is an explicit archive barrier; automatic transfer
also proceeds without this command. Requires backup administration privileges.

See [Backup and Recovery](../../backup-and-recovery.md).

## Repository and Results

Repository is an absolute server-side path, optionally prefixed with `file://`; a relative path is rejected. It is not a client upload or remote object-store URI. Requires global `BACKUP_ADMIN`. Returns a job row using the [full job result schema](show-backup-jobs.md#result-columns); poll that Job_ID for terminal state.

## Receipt Interpretation

Requested_LSN/Requested_Source_Time are fixed when work is accepted. Durable_Through_LSN/Covered_Time and Catalog_Generation are repository acknowledgments, not estimates of receiver progress. Compare them only after COMPLETED. WAITING_BASE and DISABLED reject WAL backup; archive errors remain visible and must not be treated as a successful zero-byte backup. A new request captures a new target rather than silently reusing a failed receipt.
