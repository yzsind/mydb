# `FORMAT_BYTES`

## Description

Formats a byte count into human-readable text.

## Syntax

```sql
FORMAT_BYTES(bytes)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `bytes` | Yes | Numeric or convertible text | Signed magnitude; output is rounded human-readable text, not an exact numeric conversion. |

## Return Value

Returns text using bytes and binary multiples (KiB, MiB, GiB, TiB, PiB, EiB). SQL NULL returns NULL. Thresholds are powers of 1024; formatted rounded text is not an exact stored byte count.

## Examples

```sql
SELECT FORMAT_BYTES(1024);
```

Result: 1.00 KiB

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`FORMAT_PICO_TIME`](format_pico_time.md), [`FORMAT_TIME`](format_time.md).
