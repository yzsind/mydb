# `GTID_SUBSET`

## Description

Determines whether one GTID set is a subset of another.

## Syntax

```sql
GTID_SUBSET(subset, set)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `subset` | Yes | String | The 1st argument; the meaning is determined by the function syntax and description |
| `set` | Yes | String | The 2nd argument; the meaning is determined by the function syntax and description |

## Return Value

Determines whether one GTID set is a subset of another. The return type is determined by the function semantics and the input type.

## Examples

```sql
SELECT GTID_SUBSET('', '');
```

Result: 1

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
