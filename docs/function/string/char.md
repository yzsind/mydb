# `CHAR`

## Description

Converts a sequence of integer code values into a string.

## Syntax

```sql
CHAR(n [, n] ... [USING charset_name])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `n` | No, repeatable | Integer | Code value to convert into a character |
| `charset_name` | No | Charset name | Result charset specified by the USING clause |

## Return Value

Returns a string, binary string, or integer; returns `NULL` when a required argument is `NULL`, except where noted otherwise.

## Examples

```sql
SELECT CHAR(77, 121, 68, 66);
```

Result: MyDB

## Notes

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
