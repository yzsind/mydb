# `RESTORE TABLE`

## Syntax

```sql
RESTORE TABLE recycle_name [TO [database.]table_name];
```

## Behavior and privileges

Requires RECYCLEBIN_ADMIN and CREATE on the destination. The destination must
be unused and its database must exist. With no TO, the original database/table
is used. Restore consumes the entry and gives the table a new logical identity,
retaining its physical data/indexes. It is an implicit-commit DDL boundary.
Triggers, foreign keys, RLS policies and grants are not restored; existing grants
on the destination name still apply. Check warnings and OMITTED_OBJECTS before
reopening application access. A PURGING entry cannot be restored. Errors in
restoration do not make earlier user DML rollbackable.

## Examples

Obtain an actual RECYCLE_NAME from SHOW RECYCLEBIN before substituting it into
RESTORE or PURGE; names in the syntax above are metavariables.

```sql
SHOW RECYCLEBIN FROM test LIKE 'orders%';
RESTORE TABLE r_12345 TO test.orders_recovered;
```

See [Recycle Bin](../../recycle-bin.md) for read-only queries, retention, replication,
backup and recovery boundaries, and [execution contracts](../execution-contracts.md).
