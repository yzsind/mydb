# `ALTER SYSTEM FLUSH DATA_CACHE`

## Description

Reclaims evictable data page cache.

## Syntax

```ebnf
<flush-data-cache> ::= 'ALTER SYSTEM FLUSH DATA_CACHE'
```

## Behavior, Privileges, and Limitations

- Honors pinned pages, active transactions, and the unified memory governor; it never discards dirty pages that have not been safely written back.
- The operation affects performance-related heat; it does not change table data or physical plan structures.

## Privileges, Transaction, and Result Details

Requires global SYSTEM_VARIABLES_ADMIN and returns OK/error after the engine flush/reclamation attempt; it does not implicitly commit the caller's DML transaction. Pinned/non-evictable pages can remain, so OK does not promise a zero-byte pool. Compare the same accounted components in SHOW MYDB MEMORY before/after, allowing for concurrent reads warming the cache.

## Examples

```sql
ALTER SYSTEM FLUSH DATA_CACHE;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
