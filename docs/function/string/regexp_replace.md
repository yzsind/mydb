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

- Text operands use character positions and the resolved collation. When all string operands are binary, matching, positions, and results preserve bytes; mixing binary and text operands returns error 3995. Unsupported pattern backreferences return an explicit error.

- Defaults, flag precedence, supported pattern constructs and SQL escaping are described in [regular expression rules](README.md#regular-expression-rules). Invalid positions, flags or patterns raise SQL errors.

- For example, REGEXP_REPLACE('ab','(a)(b)','$2$1') returns ba. Dollar-number capture references are distinct from SQL backslash escapes.

## Related Functions

[`REPLACE`](replace.md), [`REGEXP_SUBSTR`](regexp_substr.md), [`REGEXP_INSTR`](regexp_instr.md), [`REGEXP_LIKE`](regexp_like.md).
