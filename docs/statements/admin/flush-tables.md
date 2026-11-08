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

- Both the bare and table-list forms invoke the global dirty-page/WAL flush. A table list first validates each named table; it does not restrict the physical flush to those tables.
- Bare FLUSH TABLES WITH READ LOCK establishes a global read lock owned by the current session via the binlog management path; it is released with UNLOCK TABLES.
- The full interlocking of a table list with WITH READ LOCK/FOR EXPORT is not yet implemented and returns 1235; it does not pretend that locking succeeded.

## Privileges, Transaction, and Result Details

Requires global RELOAD. Both bare and table-list forms implicitly commit first, including before an invalid target/unsupported table-list locking option is reported. A nonexistent named table returns 1146.

Bare FLUSH TABLES WITH READ LOCK uses a separate global fence path and is supported. A named table list combined with WITH READ LOCK, or FOR EXPORT, returns 1235. UNLOCK TABLES releases the current connection's global fence. Plain table-list FLUSH validates the names then performs a global dirty flush; it does not create an export-consistent table-file copy.

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
