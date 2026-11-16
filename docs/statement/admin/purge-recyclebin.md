# `PURGE RECYCLEBIN`

## Syntax

```sql
PURGE RECYCLEBIN recycle_name;
PURGE RECYCLEBIN ALL;
```

## Behavior and privileges

Requires RECYCLEBIN_ADMIN and is an implicit-commit DDL boundary. Purge commits
the irreversible PURGING decision before asynchronous physical deletion. It
cannot be undone or restored. ALL commits batches separately and can partially
complete on failure; concurrent recycling can leave new entries. Poll STATE and
LAST_ERROR rather than treating SQL success as proof all disk space was returned.
Busy-object waits honor lock_wait_timeout and cancellation.

## Examples

Obtain an actual RECYCLE_NAME from SHOW RECYCLEBIN before substituting it into
RESTORE or PURGE; names in the syntax above are metavariables.

```sql
SHOW RECYCLEBIN FROM test LIKE 'orders%';
PURGE RECYCLEBIN r_12345;
-- Permanently purge every retained entry visible to this administration command:
PURGE RECYCLEBIN ALL;
```

See [Recycle Bin](../../recycle-bin.md) for read-only queries, retention, replication,
backup and recovery boundaries, and [execution contracts](../execution-contracts.md).
