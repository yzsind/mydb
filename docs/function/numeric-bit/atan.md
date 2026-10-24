# `ATAN`

## Description

Returns the arc tangent; also supports two-argument quadrant computation.

## Syntax

```sql
ATAN(x)
ATAN(y, x)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `x` | Yes | Numeric | Computes the arc tangent in the single-argument form |
| `y_or_x` | No | Numeric | Participates in quadrant computation in the two-argument form |

## Return Value

Returns a DOUBLE angle in radians. The one-argument form is arctangent; the two-argument form uses atan2(y,x) to select the quadrant. Any SQL NULL argument returns NULL.

## Examples

```sql
SELECT ATAN(1);
```

Result: approximately 0.7853981633974483

## Notes

- Numeric conversion, overflow, division by zero, and domain errors follow the current `sql_mode`; errors are never silently ignored.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
