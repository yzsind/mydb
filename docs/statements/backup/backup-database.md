# `BACKUP DATABASE`

Creates a full physical baseline in the unified local repository. Every baseline
is sealed once for both direct read-only queries and source WAL continuation.

```sql
BACKUP DATABASE TO 'file:///tmp/mydb/repository';
```

The asynchronous result returns a Job_ID. Poll `SHOW MYDB BACKUP JOBS` until
COMPLETED; Base_ID identifies the published image. Existing archive policy is
preserved. A new repository defaults to DISABLED, so this command alone does not
start ongoing WAL retention or transfer. Prepared XA is retained but invisible.
The job protects its required source WAL until publication or cancellation.

Requires BACKUP_ADMIN or equivalent administration privilege. Sources must be
standalone/master with persistent durability. Unknown options and retired
`FORMAT`, `TYPE`, `COMMENT`, and SQL encryption options fail explicitly.
Repository initialization configures WAL encoding and keys through the offline
CLI. Native baseline files are directly queryable and are not WAL-encrypted.

See [Backup and Recovery](../../backup-and-recovery.md) and
[ALTER MYDB BACKUP](alter-backup.md).
