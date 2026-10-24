# `IS_UUID`

## Description

Determines whether a string is a valid UUID.

## Syntax

```sql
IS_UUID(uuid)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `uuid` | Yes | Address, UUID, sequence name, integer, or flag value | The 1st argument; the meaning is determined by the function syntax and description |

## Return Value

Returns `1` when the condition holds and `0` otherwise; `NULL` input usually returns `NULL`.

## Examples

```sql
SELECT IS_UUID('6ccd780c-baba-1026-9564-5b8c656024db');
```

Result: 1

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
