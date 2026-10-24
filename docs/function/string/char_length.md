# `CHAR_LENGTH`

## Description

Returns the number of characters in a string.

## Syntax

```sql
CHAR_LENGTH(str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | The 1st argument; the string to process |

## Return Value

Returns a string, binary string, or integer; returns `NULL` when a required argument is `NULL`, except where noted otherwise.

## Examples

```sql
SELECT CHAR_LENGTH('mydb');
```

Result: 4

## Notes

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.

## Related Functions

- [`CHARACTER_LENGTH`](character_length.md): alias of this function.
