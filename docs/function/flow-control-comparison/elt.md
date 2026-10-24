# `ELT`

## Description

Returns the string from the argument list at a 1-based index.

## Syntax

```sql
ELT(index, str1 [, str2 [, ...]])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `index` | Yes | Integer | The 1st argument; the meaning is determined by the function syntax and description |
| `str1` | Yes | String | The 2nd argument; the meaning is determined by the function syntax and description |
| `str2 ...` | No, repeatable | String | The 3rd argument; the meaning is determined by the function syntax and description |

## Return Value

Returns the string at the one-based index. A NULL or out-of-range index returns NULL; selecting a NULL argument returns NULL.

## Examples

```sql
SELECT ELT(2,'first','second');
```

Result:

```text
second
```

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
