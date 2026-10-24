# `DESCRIBE` / `DESC`

## Description

Returns column metadata for a table, view, or temporary table; it is a shorthand for `SHOW COLUMNS`.

## Syntax

```ebnf
<describe> ::= ( 'DESCRIBE' | 'DESC' ) <qualified-name>
```

## Behavior and Limitations

- Returns the MySQL-compatible columns Field, Type, Null, Key, Default, Extra, and so on.
- Object resolution follows the current database and the temporary-table-first rule; a missing object returns 1146.
- Requires the privilege to view the target object; it reads no user rows and modifies no transaction.
- The current top-level AST supports only the table form and does not treat `DESCRIBE SELECT ...` as an EXPLAIN alias.

## Examples

```sql
DESCRIBE customer;
DESC test.orders;
```

## Related Statements

- [SHOW COLUMNS](../metadata/show-columns.md)
- [SHOW CREATE TABLE](../metadata/show-create-table.md)
