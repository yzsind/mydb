# `ALTER SYSTEM FLUSH SQL_CACHE`

## Description

Clears the statement, plan, view resolution, prepared DML (single and batch), and expression compilation caches.

## Syntax

```ebnf
<flush-sql-cache> ::= 'ALTER SYSTEM FLUSH SQL_CACHE'
```

## Behavior, Privileges, and Limitations

- The shared expression L2 cache is cleared immediately; each worker thread's L1 cache is cleared via an invalidation generation on the next access or statistics read.
- Prepared statement definitions and user data are not deleted; subsequent executions re-parse/re-plan on demand.
- This is for the mydb SQL cache, not the deprecated MySQL query cache commands.

## Examples

```sql
ALTER SYSTEM FLUSH SQL_CACHE;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
