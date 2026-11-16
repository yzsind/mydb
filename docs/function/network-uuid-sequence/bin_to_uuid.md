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
| `binary_uuid` | Yes | Exactly 16 binary bytes | UUID bytes, with optional matching time-part swap. |
| `swap_flag` | No | Numeric flag | Default 0; nonzero swaps UUID time fields. Use the same flag in the inverse operation. |

## Return Value

Returns lowercase hyphenated UUID text from exactly 16 binary bytes. SQL NULL returns NULL; invalid payloads raise an argument error. The inverse must use the same swap_flag.

## Examples

```sql
SELECT BIN_TO_UUID(UNHEX('6CCD780CBABA102695645B8C656024DB'));
```

Result: 6ccd780c-baba-1026-9564-5b8c656024db

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`UUID`](uuid.md), [`UUID_SHORT`](uuid_short.md), [`UUID_TO_BIN`](uuid_to_bin.md), [`IS_UUID`](is_uuid.md).
