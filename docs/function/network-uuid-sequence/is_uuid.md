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
| `uuid` | Yes | String | UUID text to validate; NULL returns NULL. |

## Return Value

Returns integer 1 for a valid UUID text representation, 0 otherwise. SQL NULL returns SQL NULL.

## Examples

```sql
SELECT IS_UUID('6ccd780c-baba-1026-9564-5b8c656024db');
```

Result: 1

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`UUID`](uuid.md), [`UUID_SHORT`](uuid_short.md), [`UUID_TO_BIN`](uuid_to_bin.md), [`BIN_TO_UUID`](bin_to_uuid.md).
