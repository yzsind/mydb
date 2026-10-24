# `SHOW CREATE TABLE / VIEW`

## Description

Returns the canonical DDL to recreate a table, temporary table, or view.

## Syntax

```ebnf
<show-create-table> ::= 'SHOW CREATE'
                         ( 'TABLE' | 'VIEW' ) <qualified-name>
```

## Behavior, Privileges, and Results

- Table output includes columns, constraints, indexes, partitions, and saved table options; compatibility metadata is echoed verbatim.
- View output includes algorithm, definer, SQL SECURITY, column names, and check option.
- Requires the privilege to view the object definition.

## Examples

```sql
SHOW CREATE TABLE orders;
SHOW CREATE VIEW active_customer;
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
