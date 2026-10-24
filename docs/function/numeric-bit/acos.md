# `ACOS`

## Description

Returns the arc cosine.

## Syntax

```sql
ACOS(number)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Numeric or integer | The 1st argument; the meaning is determined by the function syntax and description |

## Return Value

Returns a double-precision numeric value; SQL NULL input returns NULL. Invalid real-valued domains and overflow produce the applicable numeric diagnostic; an invalid domain can return NULL.

## Examples

```sql
SELECT ACOS(0);
```

Result:

```text
1.5707963267948966
```

## Notes

- The real-valued input domain is [-1,1]. An out-of-domain value such as 2 returns NULL with the applicable numeric diagnostic; it is not a successful trigonometric result.

- Numeric conversion, overflow, division by zero, and domain errors follow the current `sql_mode`; errors are never silently ignored.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
