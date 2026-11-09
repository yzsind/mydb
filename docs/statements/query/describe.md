# `DESCRIBE` / `DESC`

## Description

Returns column metadata for a table, view, or temporary table; it is a shorthand for `SHOW COLUMNS`.

## Syntax

```ebnf
<describe> ::= ( 'DESCRIBE' | 'DESC' ) <qualified-name>
```

## Behavior and Limitations

- Returns the MySQL-compatible columns Field, Type, Null, Key, Default, and Extra (six columns).
- Object resolution follows the current database and the temporary-table-first rule; a missing object returns 1146.
- Requires the privilege to view the target object; it reads no user rows and modifies no transaction.
- The current top-level AST supports only the table form and does not treat `DESCRIBE SELECT ...` as an EXPLAIN alias.

## Privileges, Results, and Boundaries

Requires SELECT on the resolved table/view. The exact columns are Field, Type, Null, Key, Default and Extra, all string metadata; there are no extra columns in plain DESC. FULL column metadata uses SHOW FULL COLUMNS instead. Invisible/generated attributes remain visible in the definition. A missing target returns 1146. See [column results](../metadata/result-columns.md#show-columns).

## Examples

```sql
DESCRIBE customer;
DESC test.orders;
```

## Related Statements

- [SHOW COLUMNS](../metadata/show-columns.md)
- [SHOW CREATE TABLE](../metadata/show-create-table.md)
