# `REGEXP_LIKE`

## Description

Determines whether a string matches a regular expression.

## Syntax

```sql
REGEXP_LIKE(expr, pat [, match_type])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | String | The text to be matched |
| `pat` | Yes | Regular expression | The pattern to match |
| `match_type` | No | Flag string | Modes such as case sensitivity and multiline |

## Return Value

Returns integer 1 for a match, 0 for no match, or SQL NULL for SQL NULL input.

## Examples

```sql
SELECT REGEXP_LIKE('mydb', '^my');
```

Result: 1

## Notes

- Text operands use character positions and the resolved collation. When all string operands are binary, matching, positions, and results preserve bytes; mixing binary and text operands returns error 3995. Unsupported pattern backreferences return an explicit error.

- Defaults, flag precedence, supported pattern constructs and SQL escaping are described in [regular expression rules](README.md#regular-expression-rules). Invalid positions, flags or patterns raise SQL errors.

## Related Functions

[`REPLACE`](replace.md), [`REGEXP_REPLACE`](regexp_replace.md), [`REGEXP_SUBSTR`](regexp_substr.md), [`REGEXP_INSTR`](regexp_instr.md).
