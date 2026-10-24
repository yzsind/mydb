# `LIST_DROP`

## Description

Removes a given element from a comma-separated list.

## Syntax

```sql
LIST_DROP(list, item)
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
SELECT LIST_DROP('a,b,c', 'b');
```

Result: a,c

## Notes

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
