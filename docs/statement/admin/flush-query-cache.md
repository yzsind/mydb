# `FLUSH / RESET QUERY CACHE`

## Description

Provides compatibility for the query cache commands found in legacy MySQL initialization scripts.

## Syntax

```ebnf
<query-cache-admin> ::= 'FLUSH QUERY CACHE'
                         | 'RESET QUERY CACHE'
```

## Behavior, Privileges, and Limitations

- MySQL 8.0 removed the query cache; mydb does not implement the legacy result query cache.
- The command succeeds and produces deprecation warning 1287; it does not clear the plan cache or statement cache.
- Use ALTER SYSTEM FLUSH SQL_CACHE to clear the mydb SQL cache.

## Privileges, Transaction, and Result Details

Requires global RELOAD and implicitly commits preceding DML, even though the legacy cache operation is only compatibility behavior. FLUSH and RESET return OK with warning 1287. SHOW WARNINGS immediately afterward sees the condition. ALTER SYSTEM FLUSH SQL_CACHE instead requires SYSTEM_VARIABLES_ADMIN and clears actual SQL caches without this legacy result-cache meaning.

## Examples

```sql
FLUSH QUERY CACHE;
SHOW WARNINGS;
ALTER SYSTEM FLUSH SQL_CACHE;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
