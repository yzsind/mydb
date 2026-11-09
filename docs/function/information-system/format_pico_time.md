# `FORMAT_PICO_TIME`

## Description

Formats a picosecond duration into human-readable text.

## Syntax

```sql
FORMAT_PICO_TIME(picoseconds)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `picoseconds` | Yes | Numeric or convertible text | Signed magnitude; output is rounded human-readable text, not an exact numeric conversion. |

## Return Value

Returns human-readable text for a picosecond duration, selecting ps, ns, us, ms, s, min, h or d by magnitude. SQL NULL returns NULL. Unit conversion and rounding are for presentation, not timestamp arithmetic.

## Examples

```sql
SELECT FORMAT_PICO_TIME(1000000000000);
```

Result: 1.00 s

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`FORMAT_BYTES`](format_bytes.md), [`FORMAT_TIME`](format_time.md).
