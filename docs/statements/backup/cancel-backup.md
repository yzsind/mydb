# `CANCEL MYDB BACKUP`

## Description

Requests cancellation of a specified running online backup job.

## Syntax

```ebnf
<cancel-backup> ::= 'CANCEL MYDB BACKUP' <job-id>
```

## Behavior, Privileges, and Limitations

- Job_ID comes from SHOW MYDB BACKUP JOBS, not Backup_ID.
- Publication phases reject cancellation. Cancellation is cooperative: workers stop at a safe boundary, release snapshots/reservations, and mark the job as cancelled.
- Committed backups are not deleted; WAL blobs written but not published can later be reclaimed by the repository prune tool.
- Unknown, already completed, or non-cancellable jobs return an explicit error; backup administration privileges are required.

## Examples

```sql
SHOW MYDB BACKUP JOBS;
CANCEL MYDB BACKUP 'job-uuid';
```

## See Also

- [SHOW BACKUP JOBS](show-backup-jobs.md)
- [BACKUP DATABASE](backup-database.md)
