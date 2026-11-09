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

## Result and Visibility Details

The FUNCTION form has the corresponding Function/Create Function names. A visible routine can return NULL in its Create column when the caller can list/invoke it but cannot see its definition. An invisible or missing routine returns 1305. Viewing the definition is separate from EXECUTE permission.

See [ordered columns and wire types](result-columns.md#show-create-procedure). Reading this metadata does not itself commit user DML.

## Examples

```sql
SHOW CREATE PROCEDURE transfer;
SHOW CREATE FUNCTION add_tax;
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
