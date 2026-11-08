# `BACKUP DATABASE`

## Syntax

```ebnf
<backup-database> ::= 'BACKUP DATABASE TO' <string-literal>
```

## Behavior

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

## Repository and Results

Repository is an absolute server-side path, optionally prefixed with `file://`; a relative path is rejected. It is not a client upload or remote object-store URI. Requires global `BACKUP_ADMIN`. Returns a job row using the [full job result schema](show-backup-jobs.md#result-columns); poll that Job_ID for terminal state.

## Consistency and Completion

The capture includes a final writer barrier/checkpoint; long conflicting work and resource/freeze budgets can prevent completion. This is online capture with bounded synchronization, not a promise of zero writer pause. Prepared XA is retained for recovery and remains invisible to ordinary baseline queries. Only a sealed published baseline is returned by SHOW MYDB BACKUPS; staging bytes do not establish recoverability.

For ongoing protection:

```sql
ALTER MYDB BACKUP TO 'file:///tmp/mydb/repository' SET WAL_ARCHIVE=MANUAL;
BACKUP DATABASE TO 'file:///tmp/mydb/repository';
SHOW MYDB BACKUP JOBS; -- Wait for the DATABASE job to complete.
SHOW MYDB BACKUPS FROM 'file:///tmp/mydb/repository';
BACKUP WAL TO 'file:///tmp/mydb/repository';
```

Poll each job before the dependent next operation. To query a sealed baseline or restore a receipt boundary, use the concrete commands in [Backup and Recovery](../../backup-and-recovery.md).
