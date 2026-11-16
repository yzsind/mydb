# `FORMAT_TIME`

## Description

Formats a picosecond duration into human-readable text; a compatibility function for `FORMAT_PICO_TIME`.

## Syntax

```sql
FORMAT_TIME(picoseconds)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `picoseconds` | Yes | Numeric or convertible text | Signed magnitude; output is rounded human-readable text, not an exact numeric conversion. |

## Return Value

Returns duration text using FORMAT_PICO_TIME formatting, with a trailing .00 fractional portion removed before the unit. SQL NULL returns NULL. It is not a SQL TIME formatter.

## Examples

```sql
SELECT FORMAT_TIME(1000000000000);
```

Result: `1 s`; FORMAT_TIME strips the `.00` fraction retained by FORMAT_PICO_TIME.

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`FORMAT_BYTES`](format_bytes.md), [`FORMAT_PICO_TIME`](format_pico_time.md).
