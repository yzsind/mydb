# `EXTRACT_TABLE_FROM_FILE_NAME`

## Description

Extracts the table name from a server-internal file name.

## Syntax

```sql
EXTRACT_TABLE_FROM_FILE_NAME(value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | Yes | Numeric or integer | The 1st argument; the input value |

## Return Value

Extracts the table name from a server-internal file name. The return type is determined by the function semantics and the input type.

## Examples

```sql
SELECT EXTRACT_TABLE_FROM_FILE_NAME('/var/lib/mysql/test/t.ibd');
```

Result: t

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
