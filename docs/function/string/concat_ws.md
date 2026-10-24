# `CONCAT_WS`

## Description

Concatenates the remaining non-`NULL` arguments using the first argument as the separator.

## Syntax

```sql
CONCAT_WS(separator, str1 [, str2 [, ...]])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `separator` | Yes | String or binary string | The 1st argument; the separator used in the concatenated result |
| `str1` | Yes | String | The 2nd argument; the meaning is determined by the function syntax and description |
| `str2 ...` | No, repeatable | String | The 3rd argument; the meaning is determined by the function syntax and description |

## Return Value

Returns text/binary concatenation separated by the first argument. A NULL separator returns NULL; NULL value arguments are skipped rather than making the entire result NULL.

## Examples

```sql
SELECT CONCAT_WS(',', 'mydb');
```

Result:

```text
mydb
```

## Notes

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
