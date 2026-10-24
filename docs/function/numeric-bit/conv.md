# `CONV`

## Description

Converts integer text between bases from 2 to 36.

## Syntax

```sql
CONV(number, from_base, to_base)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Integer text or numeric value | Digits interpreted in from_base; the result is returned as text |
| `from_base` | Yes | Numeric expression | The 2nd argument; the meaning is determined by the function syntax and description |
| `to_base` | Yes | Numeric expression | The 3rd argument; the meaning is determined by the function syntax and description |

## Return Value

Returns an integer representation as a string in the target base. SQL NULL input or a base outside the supported magnitude 2–36 returns NULL. Negative base signs select signed interpretation/output under the conversion rules.

## Examples

```sql
SELECT CONV('FF',16,10);
```

Result:

```text
255
```

## Notes

- Numeric conversion, overflow, division by zero, and domain errors follow the current `sql_mode`; errors are never silently ignored.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
