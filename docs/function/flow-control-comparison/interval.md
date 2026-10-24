# `INTERVAL`

## Description

Returns the interval index of the first argument within an ordered list of boundaries.

## Syntax

```sql
INTERVAL(n, n1, n2, ...)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `n` | Yes | SQL expression matching the function semantics | The 1st argument; the meaning is determined by the function syntax and description |
| `boundary1` | Yes | SQL expression matching the function semantics | The 2nd argument; the meaning is determined by the function syntax and description |
| `boundary2 ...` | No, repeatable | SQL expression matching the function semantics | The 3rd argument; the meaning is determined by the function syntax and description |

## Return Value

Returns the interval index of the first argument within an ordered list of boundaries. The return type is determined by the function semantics and the input types.

## Examples

```sql
SELECT INTERVAL(5, 1, 3, 7);
```

Result: 2

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
