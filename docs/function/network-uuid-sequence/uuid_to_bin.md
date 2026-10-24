# `UUID_TO_BIN`

## Description

Converts a standard text UUID to 16-byte binary format.

## Syntax

```sql
UUID_TO_BIN(uuid [, swap_flag])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `uuid` | Yes | Address, UUID, sequence name, integer, or flag value | The 1st argument; the meaning is determined by the function syntax and description |
| `swap_flag` | No | Address, UUID, sequence name, integer, or flag value | The 2nd argument; the meaning is determined by the function syntax and description |

## Return Value

Converts a standard text UUID to 16-byte binary format. The return type is determined by the function semantics and the input type.

## Examples

```sql
SELECT HEX(UUID_TO_BIN('6ccd780c-baba-1026-9564-5b8c656024db'));
```

Result: 6CCD780CBABA102695645B8C656024DB

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
