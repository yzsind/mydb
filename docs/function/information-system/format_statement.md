# `FORMAT_STATEMENT`

## Description

Truncates and formats an SQL statement into text suitable for diagnostic display.

## Syntax

```sql
FORMAT_STATEMENT(sql_text)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `sql_text` | Yes | String | The 1st argument; the SQL text to normalize or format |

## Return Value

Truncates and formats an SQL statement into text suitable for diagnostic display. The return type is determined by the function semantics and the input type.

## Examples

```sql
SELECT FORMAT_STATEMENT('SELECT * FROM a_very_long_table_name');
```

Result: returns the SQL text truncated according to the sys configuration.

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
