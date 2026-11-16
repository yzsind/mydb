# `CANCEL MYDB BACKUP`

## Description

Requests cancellation of a specified running online backup job.

## Syntax

```ebnf
<cancel-backup> ::= 'CANCEL MYDB BACKUP' <job-id>
```

## Behavior, Privileges, and Limitations

- Job_ID is a quoted job identifier from SHOW MYDB BACKUP JOBS. Base_ID identifies a published baseline and cannot be substituted for a job identifier.
- Publication phases reject cancellation. Cancellation is cooperative: workers stop at a safe boundary, release snapshots/reservations, and mark the job as cancelled.
- Committed backups are not deleted; WAL blobs written but not published can later be reclaimed by the repository prune tool.
- Unknown, already completed, or non-cancellable jobs return an explicit error; global BACKUP_ADMIN is required.

## Examples

```sql
SHOW MYDB BACKUP JOBS;
CANCEL MYDB BACKUP 'job-uuid';
```

## See Also

- [SHOW BACKUP JOBS](show-backup-jobs.md)
- [BACKUP DATABASE](backup-database.md)

## Cancellation Completion

Returns OK when a cancellation request is accepted. It does not wait for CANCELLED cleanup. Poll SHOW MYDB BACKUP JOBS until terminal state and inspect Error/Failure_Class. Requests in phases starting with PUBLISH or in QUERY_READY, unknown jobs and terminal jobs are rejected. A cooperative cancellation does not delete committed baselines or undo business writes.
