# `QUOTE`

## Description

Escapes a string into single-quoted text suitable for embedding in SQL.

## Syntax

```sql
QUOTE(str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | The 1st argument; the string to process |

## Return Value

Returns SQL-quoted text with escape sequences. SQL NULL returns the text `NULL` rather than SQL NULL.

## Examples

```sql
SELECT QUOTE('mydb');
```

Result:

```text
'mydb'
```

## Notes

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.
- `QUOTE(NULL)` returns the string `NULL`, not SQL NULL.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
