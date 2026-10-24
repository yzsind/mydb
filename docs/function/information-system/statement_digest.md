# `STATEMENT_DIGEST`

## Description

Returns the digest hash of normalized SQL text.

## Syntax

```sql
STATEMENT_DIGEST(sql_text)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `sql_text` | Yes | String | The 1st argument; the SQL text to normalize or format |

## Return Value

Returns the digest hash of normalized SQL text. The return type is determined by the function semantics and the input type.

## Examples

```sql
SELECT STATEMENT_DIGEST('SELECT 1');
```

Result: returns a digest consisting of 64 hexadecimal characters.

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
