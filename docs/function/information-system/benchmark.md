# `BENCHMARK`

## Description

Repeatedly evaluates an expression a specified number of times and returns 0; used for microbenchmarks.

## Syntax

```sql
BENCHMARK(count, expr)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `count` | Yes | Integer | The 1st argument; the meaning is determined by the function syntax and description |
| `expr` | Yes | Any SQL expression | The 2nd argument; the expression to evaluate |

## Return Value

Repeatedly evaluates an expression a specified number of times and returns 0; used for microbenchmarks. The return type is determined by the function semantics and the input type.

## Examples

```sql
SELECT BENCHMARK(10, ABS(-1));
```

Result: 0

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
