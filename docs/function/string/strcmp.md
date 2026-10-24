# `STRCMP`

## Description

Compares two strings and returns -1, 0, or 1.

## Syntax

```sql
STRCMP(str1, str2)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str1` | Yes | String | The 1st argument; the meaning is determined by the function syntax and description |
| `str2` | Yes | String | The 2nd argument; the meaning is determined by the function syntax and description |

## Return Value

Returns an integer computed from the input string(s). SQL NULL arguments return NULL; character-versus-byte interpretation follows the function definition and input type.

## Examples

```sql
SELECT STRCMP('mydb', 'mydb');
```

Result:

```text
0
```

## Notes

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
