# `ALTER MYDB BACKUP`

```sql
ALTER MYDB BACKUP TO 'file:///tmp/mydb/repository'
  SET WAL_ARCHIVE=MANUAL, MAX_PENDING_WAL_BYTES=17179869184;
```

WAL_ARCHIVE accepts DISABLED, MANUAL and CONTINUOUS. Configuration can initialize
an empty repository but does not create a baseline. Enabled policies enter
WAITING_BASE until `BACKUP DATABASE` publishes a protected baseline. MANUAL keeps
unarchived WAL on the source; CONTINUOUS also transfers it automatically.
Switching between these enabled policies preserves the stream and retention.

DISABLED normally creates a drain job: a fixed final target is durably archived
and closed before retention is released. Failure preserves protection. To
explicitly abandon the pending tail, including an unavailable repository:

```sql
ALTER MYDB BACKUP TO 'file:///tmp/mydb/repository'
  SET WAL_ARCHIVE=DISABLED, DISCARD_PENDING=TRUE;
```

Re-enabling requires a new baseline and stream. The default pending WAL budget is
16 GiB. Exhaustion blocks new business writes while management, reads, archiving
and rollback remain available. No automatic policy downgrade occurs. Duplicate,
unknown or inconsistent options fail. Requires backup administration privileges.

See [Backup and Recovery](../../backup-and-recovery.md).
