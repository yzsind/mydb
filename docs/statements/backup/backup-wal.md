# `BACKUP WAL`

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
