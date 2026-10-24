# `LIST_ADD`

## Description

Appends an element to a comma-separated list if it is not already present.

## Syntax

```sql
LIST_ADD(list, item)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `list` | Yes | String | The 1st argument; the comma-separated list |
| `item` | Yes | String | The 2nd argument; the list element to add or remove |

## Return Value

Returns a string, binary string, or integer; returns `NULL` when a required argument is `NULL`, except where noted otherwise.

## Examples

```sql
SELECT LIST_ADD('a,b', 'c');
```

Result: a,b,c

## Notes

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
