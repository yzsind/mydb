# `SHOW RECYCLEBIN`

## Syntax

```sql
SHOW RECYCLEBIN [FROM database | IN database] [LIKE 'pattern'];
```

## Behavior and privileges

Lists committed retained entries and requires RECYCLEBIN_ADMIN. LIKE filters
original table names. Result columns are RECYCLE_ID, RECYCLE_NAME, ORIGIN_SCHEMA,
ORIGIN_TABLE, OPERATION, RECYCLED_MS, EXPIRES_MS, STATE, OMITTED_OBJECTS, LAST_ERROR,
OPERATION_ID, DATA_BYTES and PURGE_REASON. Times are Unix milliseconds; DATA_BYTES
is observed file length, not exact allocated disk space. STATE is RECYCLED or
irreversible PURGING. Listing does not prevent concurrent expiry or reclamation.
Use the returned RECYCLE_NAME for subsequent reads or administration.

## Examples

Obtain an actual RECYCLE_NAME from SHOW RECYCLEBIN before substituting it into
RESTORE or PURGE; names in the syntax above are metavariables.

```sql
SHOW RECYCLEBIN FROM test LIKE 'orders%';
```

See [Recycle Bin](../../recycle-bin.md) for read-only queries, retention, replication,
backup and recovery boundaries, and [execution contracts](../execution-contracts.md).
