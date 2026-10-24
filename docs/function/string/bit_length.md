# `BIT_LENGTH`

## Description

Returns the length of a string in bits.

## Syntax

```sql
BIT_LENGTH(str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | The 1st argument; the string to process |

## Return Value

Returns an integer computed from the input string(s). SQL NULL arguments return NULL; character-versus-byte interpretation follows the function definition and input type.

## Examples

```sql
SELECT BIT_LENGTH('mydb');
```

Result:

```text
32
```

## Notes

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
