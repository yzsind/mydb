# `BIT_COUNT`

## Description

Returns the number of set bits in an integer's binary representation.

## Syntax

```sql
BIT_COUNT(number)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Numeric or integer | The 1st argument; the meaning is determined by the function syntax and description |

## Return Value

Returns an integer (unsigned for CRC32 and BIT_COUNT). SQL NULL input returns NULL; argument conversion and overflow follow the numeric rules.

## Examples

```sql
SELECT BIT_COUNT(2);
```

Result:

```text
1
```

## Notes

- Numeric conversion, overflow, division by zero, and domain errors follow the current `sql_mode`; errors are never silently ignored.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
