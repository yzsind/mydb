# `DIV`

## Description

Performs integer division.

## Syntax

```sql
DIV(dividend, divisor)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `dividend` | Yes | Numeric | The dividend |
| `divisor` | Yes | Non-zero numeric | The divisor |

## Return Value

Returns an integer quotient truncated toward zero. SQL NULL input or a zero divisor returns NULL; zero-division diagnostics follow sql_mode. Out-of-range quotients return an explicit numeric error.

## Examples

```sql
SELECT DIV(4, 2);
```

Result:

```text
2
```

## Notes

- Numeric conversion, overflow, division by zero, and domain errors follow the current `sql_mode`; errors are never silently ignored.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
