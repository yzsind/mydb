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

- Regular expression positions count text characters. Matching uses the resolved collation and match flags; unsupported backreferences return an explicit error. Do not treat a byte-string operand as a guarantee of byte-offset results.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
