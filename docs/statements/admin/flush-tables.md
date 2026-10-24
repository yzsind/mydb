# `FLUSH TABLES`

## Description

Flushes dirty pages for all or specified tables, and can request a global read lock or an export lock.

## Syntax

```ebnf
<flush-tables> ::= 'FLUSH TABLES'
                   [ <table-list> ]
                   [ 'WITH READ LOCK' | 'FOR EXPORT' ]
```

## Behavior, Privileges, and Limitations

- Without a table list, it flushes all dirty pages and the WAL; with a list, it flushes only the target tables.
- Bare FLUSH TABLES WITH READ LOCK establishes a global read lock owned by the current session via the binlog management path; it is released with UNLOCK TABLES.
- The full interlocking of a table list with WITH READ LOCK/FOR EXPORT is not yet implemented and returns 1235; it does not pretend that locking succeeded.

## Examples

```sql
FLUSH TABLES;
FLUSH TABLES orders, customer;
FLUSH TABLES WITH READ LOCK;
UNLOCK TABLES;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
