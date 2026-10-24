# `IFNULL`

## Description

Returns a fallback value when the first argument is `NULL`.

## Syntax

```sql
IFNULL(expr, fallback)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | Any SQL expression | The 1st argument; the expression to evaluate |
| `fallback` | Yes | Any SQL expression | The 2nd argument; returned when the main expression is NULL |

## Return Value

Returns the first argument when non-NULL and otherwise the fallback. The result domain combines both arguments; the example returns text.

## Examples

```sql
SELECT IFNULL('example', 'example');
```

Result:

```text
example
```

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
