# `NULLIF`

## Description

Returns `NULL` when the two arguments are equal, otherwise returns the first argument.

## Syntax

```sql
NULLIF(expr1, expr2)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr1` | Yes | Any SQL expression | The 1st argument; the meaning is determined by the function syntax and description |
| `expr2` | Yes | Any SQL expression | The 2nd argument; the meaning is determined by the function syntax and description |

## Return Value

Returns SQL NULL if the arguments compare equal; otherwise returns the first argument with its type. Equality follows the common comparison/coercion rules.

## Examples

```sql
SELECT NULLIF('example', 'example');
```

Result:

```text
NULL
```

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
