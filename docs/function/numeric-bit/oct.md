# `OCT`

## Description

Returns an octal string representation of an integer.

## Syntax

```sql
OCT(number)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Numeric or integer | The 1st argument; the meaning is determined by the function syntax and description |

## Return Value

Returns the unsigned integer representation as a string in base 8. SQL NULL input returns NULL.

## Examples

```sql
SELECT OCT(2);
```

Result:

```text
2
```

## Notes

- Numeric conversion, overflow, division by zero, and domain errors follow the current `sql_mode`; errors are never silently ignored.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
