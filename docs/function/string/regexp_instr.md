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

- Text operands use character positions and the resolved collation. When all string operands are binary, matching, positions, and results preserve bytes; mixing binary and text operands returns error 3995. Unsupported pattern backreferences return an explicit error.

- Defaults, flag precedence, supported pattern constructs and SQL escaping are described in [regular expression rules](README.md#regular-expression-rules). Invalid positions, flags or patterns raise SQL errors.

## Related Functions

[`REPLACE`](replace.md), [`REGEXP_REPLACE`](regexp_replace.md), [`REGEXP_SUBSTR`](regexp_substr.md), [`REGEXP_LIKE`](regexp_like.md).
