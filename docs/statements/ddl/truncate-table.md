# `TRUNCATE TABLE`

## Description

Quickly empties a table and resets state tied to the table lifecycle.

## Syntax

```ebnf
<truncate-table> ::= 'TRUNCATE' [ 'TABLE' ] <qualified-name>
```

## Behavior and Limitations

- Requires DROP/ALTER-class DDL privileges and acquires the exclusive metadata lock.
- Unlike DELETE without WHERE, it does not return rows one by one and offers no RETURNING; auto-increment state is handled per the table-rebuild semantics.
- States that do not permit truncation, such as foreign key references, return an explicit error.
- This is a DDL boundary; do not rely on an outer ROLLBACK to recover.
- Persistent tables publish a fresh empty physical identity before retiring the old files. Retried cleanup for the old identity cannot remove rows inserted after TRUNCATE succeeds.

## Examples

```sql
TRUNCATE TABLE staging_event;
```

## See Also

- [DDL category index](README.md)
- [Syntax notation](../syntax-conventions.md)
- [SHOW CREATE TABLE](../metadata/show-create-table.md)
