# `REGEXP_SUBSTR`

## Description

Returns the substring matched by a regular expression.

## Syntax

```sql
REGEXP_SUBSTR(expr, pat [, pos [, occurrence [, match_type]]])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | String | The source text |
| `pat` | Yes | Regular expression | The pattern to match |
| `pos` | No | Positive integer | The starting position |
| `occurrence` | No | Positive integer | Which occurrence of the match |
| `match_type` | No | Flag string | The match mode |

## Return Value

Returns the matched text. No match or SQL NULL input returns SQL NULL.

## Examples

```sql
SELECT REGEXP_SUBSTR('mydb', 'd.');
```

Result: db

## Notes

- Text operands use character positions and the resolved collation. When all string operands are binary, matching, positions, and results preserve bytes; mixing binary and text operands returns error 3995. Unsupported pattern backreferences return an explicit error.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
