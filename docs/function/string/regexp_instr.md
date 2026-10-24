# `REGEXP_INSTR`

## Description

Returns the position of a regular expression match.

## Syntax

```sql
REGEXP_INSTR(expr, pat [, pos [, occurrence [, return_option [, match_type]]]])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | String | The text to be searched |
| `pat` | Yes | Regular expression | The pattern to match |
| `pos` | No | Positive integer | The starting character position |
| `occurrence` | No | Positive integer | Which occurrence of the match |
| `return_option` | No | 0 or 1 | Returns the position after the match ends instead of where it starts |
| `match_type` | No | Flag string | The match mode |

## Return Value

Returns a one-based character position: the match start for return_option 0, or the first character after the match for 1. No match returns 0; SQL NULL input returns SQL NULL.

## Examples

```sql
SELECT REGEXP_INSTR('mydb', 'db');
```

Result: 3

## Notes

- Regular expression positions count text characters. Matching uses the resolved collation and match flags; unsupported backreferences return an explicit error. Do not treat a byte-string operand as a guarantee of byte-offset results.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
