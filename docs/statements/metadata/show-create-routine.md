# `SHOW CREATE PROCEDURE / FUNCTION`

## Description

Returns a stored procedure's or function's definition, sql_mode, and character set metadata.

## Syntax

```ebnf
<show-create-routine> ::= 'SHOW CREATE'
                          ( 'PROCEDURE' | 'FUNCTION' ) <qualified-name>
```

## Behavior, Privileges, and Results

- The object type must match; the output includes definer, parameters, RETURNS, characteristics, and the routine body.
- Viewing the full definition of objects whose definer is another user requires the corresponding privileges.

## Examples

```sql
SHOW CREATE PROCEDURE transfer;
SHOW CREATE FUNCTION add_tax;
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
