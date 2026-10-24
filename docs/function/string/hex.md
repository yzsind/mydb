# `HEX`

## Description

Returns a hexadecimal representation of a string's bytes or an integer.

## Syntax

```sql
HEX(value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | Yes | Numeric or integer | The 1st argument; the input value |

## Return Value

Returns uppercase hexadecimal text; numeric input is formatted as an integer, while string/binary input is encoded byte by byte. SQL NULL returns NULL.

## Examples

```sql
SELECT HEX(2);
```

Result:

```text
2
```

## Notes

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
