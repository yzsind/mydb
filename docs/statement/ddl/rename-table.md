# `RENAME TABLE`

## Description

Atomically renames one or more tables in the declared order.

## Syntax

```ebnf
<rename-table> ::= 'RENAME TABLE' <rename-pair>
                   { ',' <rename-pair> }
<rename-pair> ::= <qualified-name> 'TO' <qualified-name>
```

## Behavior and Limitations

- All sources, targets, and dependencies are validated first, then the renames are applied; on mid-way failure, already applied pairs are rolled back.
- Permitted objects can be swapped or moved across databases within the same statement; privileges are checked against both the source and target schemas.
- Acquires exclusive metadata locks on the related objects and updates the data dictionary.

## Privileges, Transactions, and Dependencies

Requires ALTER+DROP on each source and CREATE+INSERT on each target. RENAME TABLE operates on the persistent namespace; use ALTER TABLE ... RENAME for a session temporary table. It is rejected under explicit LOCK TABLES (1192) and implicitly commits preceding DML.

Multiple pairs are applied in declaration order with atomic rollback on failure. For a name swap use an intermediate unused name. Both schemas must permit moves and all dependencies must remain valid. Foreign keys/triggers, stable sequence ownership and RLS dependencies can restrict cross-schema moves; referenced policy-subquery tables cannot be renamed while depended on. See [RLS dependencies](../../row-level-security.md#policies).

## Examples

```sql
RENAME TABLE current_orders TO old_orders,
             next_orders TO current_orders;
```

## See Also

- [DDL category index](README.md)
- [Syntax notation](../syntax-conventions.md)
- [SHOW CREATE TABLE](../metadata/show-create-table.md)
