# `REGEXP_REPLACE`

## Description

Replaces regular expression matches with replacement text.

## Syntax

```sql
REGEXP_REPLACE(expr, pat, repl [, pos [, occurrence [, match_type]]])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | String | The source text |
| `pat` | Yes | Regular expression | The pattern to match |
| `repl` | Yes | String | The replacement text |
| `pos` | No | Positive integer | The starting position |
| `occurrence` | No | Non-negative integer | 0 replaces all occurrences; otherwise replaces the specified occurrence |
| `match_type` | No | Flag string | The match mode |

## Return Value

Returns text after replacing the selected matches. No match preserves the source text; SQL NULL input returns SQL NULL.

## Examples

```sql
SELECT REGEXP_REPLACE('mydb', 'my', 'your');
```

Result: yourdb

## Notes

- Regular expression positions count text characters. Matching uses the resolved collation and match flags; unsupported backreferences return an explicit error. Do not treat a byte-string operand as a guarantee of byte-offset results.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
