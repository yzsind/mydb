# `UNHEX`

## Description

Decodes hexadecimal text into a byte string.

## Syntax

```sql
UNHEX(str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | The 1st argument; the string to process |

## Return Value

Returns binary bytes decoded from hexadecimal text. SQL NULL or invalid hexadecimal input returns NULL. HEX in the example displays the bytes without character-encoding ambiguity.

## Examples

```sql
SELECT HEX(UNHEX('6D796462'));
```

Result:

```text
6D796462
```

## Notes

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
