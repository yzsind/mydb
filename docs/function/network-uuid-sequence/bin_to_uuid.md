# `BIN_TO_UUID`

## Description

Converts a 16-byte UUID to the standard text format.

## Syntax

```sql
BIN_TO_UUID(binary_uuid [, swap_flag])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `binary_uuid` | Yes | Address, UUID, sequence name, integer, or flag value | The 1st argument; the meaning is determined by the function syntax and description |
| `swap_flag` | No | Address, UUID, sequence name, integer, or flag value | The 2nd argument; the meaning is determined by the function syntax and description |

## Return Value

Converts a 16-byte UUID to the standard text format. The return type is determined by the function semantics and the input type.

## Examples

```sql
SELECT BIN_TO_UUID(UNHEX('6CCD780CBABA102695645B8C656024DB'));
```

Result: 6ccd780c-baba-1026-9564-5b8c656024db

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
