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

- Regular expression positions count text characters. Matching uses the resolved collation and match flags; unsupported backreferences return an explicit error. Do not treat a byte-string operand as a guarantee of byte-offset results.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
