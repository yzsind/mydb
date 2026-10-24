# `GTID_SUBTRACT`

## Description

Subtracts one GTID set from another.

## Syntax

```sql
GTID_SUBTRACT(set, set_to_remove)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `set` | Yes | String | The 1st argument; the meaning is determined by the function syntax and description |
| `set_to_remove` | Yes | String | The 2nd argument; the meaning is determined by the function syntax and description |

## Return Value

Subtracts one GTID set from another. The return type is determined by the function semantics and the input type.

## Examples

```sql
SELECT GTID_SUBTRACT('', '');
```

Result: returns an empty string.

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
