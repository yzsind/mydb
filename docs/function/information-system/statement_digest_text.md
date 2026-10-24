# `STATEMENT_DIGEST_TEXT`

## Description

Returns the normalized digest text of SQL text.

## Syntax

```sql
STATEMENT_DIGEST_TEXT(sql_text)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `sql_text` | Yes | String | The 1st argument; the SQL text to normalize or format |

## Return Value

Returns the normalized digest text of SQL text. The return type is determined by the function semantics and the input type.

## Examples

```sql
SELECT STATEMENT_DIGEST_TEXT('SELECT 1');
```

Result: returns the normalized SQL digest text.

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
