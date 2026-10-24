# `FIND_IN_SET`

## Description

Returns the position of a string within a comma-separated set.

## Syntax

```sql
FIND_IN_SET(str, strlist)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | The 1st argument; the string to process |
| `strlist` | Yes | String | The 2nd argument; the meaning is determined by the function syntax and description |

## Return Value

Returns an integer computed from the input string(s). SQL NULL arguments return NULL; character-versus-byte interpretation follows the function definition and input type.

## Examples

```sql
SELECT FIND_IN_SET('mydb', 'mydb');
```

Result:

```text
1
```

## Notes

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
