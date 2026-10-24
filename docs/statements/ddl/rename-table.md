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

## Examples

```sql
RENAME TABLE current_orders TO old_orders,
             next_orders TO current_orders;
```

## See Also

- [DDL category index](README.md)
- [Syntax notation](../syntax-conventions.md)
- [SHOW CREATE TABLE](../metadata/show-create-table.md)
