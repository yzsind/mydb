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
| `uuid` | Yes | UUID text | UUID in a supported textual form. |
| `swap_flag` | No | Numeric flag | Default 0; nonzero swaps UUID time fields. Use the same flag in the inverse operation. |

## Return Value

Returns exactly 16 binary UUID bytes. SQL NULL returns NULL; invalid UUID text raises an argument error. swap_flag changes the arrangement of time fields and must be matched on inverse conversion.

## Examples

```sql
SELECT HEX(UUID_TO_BIN('6ccd780c-baba-1026-9564-5b8c656024db'));
```

Result: 6CCD780CBABA102695645B8C656024DB

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`UUID`](uuid.md), [`UUID_SHORT`](uuid_short.md), [`BIN_TO_UUID`](bin_to_uuid.md), [`IS_UUID`](is_uuid.md).
