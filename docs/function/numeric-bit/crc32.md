# `CRC32`

## Description

Returns the CRC-32 checksum of the input byte string.

## Syntax

```sql
CRC32(value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | Yes | String or binary string; scalar values are converted | The bytes to checksum |

## Return Value

Returns the unsigned CRC-32 checksum as an integer in 0..4294967295. SQL NULL input returns NULL. Numeric input is converted to its string bytes before hashing; CRC32(2) therefore checksums the text `2`.

## Examples

```sql
SELECT CRC32(2);
```

Result:

```text
450215437
```

## Notes

- Numeric conversion, overflow, division by zero, and domain errors follow the current `sql_mode`; errors are never silently ignored.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
