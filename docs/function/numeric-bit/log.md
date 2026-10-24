# `LOG`

## Description

Returns the natural logarithm, or the logarithm to a specified base.

## Syntax

```sql
LOG(x)
LOG(base, x)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `base_or_x` | Yes | Positive number | The antilogarithm in the single-argument form; the base in the two-argument form |
| `x` | No | Positive number | The antilogarithm in the two-argument form |

## Return Value

Returns a DOUBLE logarithm: natural logarithm with one argument, or logarithm of x in the supplied base with two. SQL NULL input returns NULL; x must be positive and the base must be positive and different from 1. Invalid domains return NULL with the applicable diagnostic.

## Examples

```sql
SELECT LOG(10);
```

Result: approximately 2.302585092994046

## Notes

- Numeric conversion, overflow, division by zero, and domain errors follow the current `sql_mode`; errors are never silently ignored.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
