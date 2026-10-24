# `FLOOR`

## Description

Returns the largest integer not greater than the argument.

## Syntax

```sql
FLOOR(number)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Numeric or integer | The 1st argument; the meaning is determined by the function syntax and description |

## Return Value

Returns a rounded or truncated numeric value in the derived exact/approximate input domain; SQL NULL input returns NULL. ROUND/FLOOR/TRUNCATE differ in direction and decimal-place handling; the example uses exact integer input.

## Examples

```sql
SELECT FLOOR(2);
```

Result:

```text
2
```

## Notes

- Numeric conversion, overflow, division by zero, and domain errors follow the current `sql_mode`; errors are never silently ignored.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
