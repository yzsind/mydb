# `CONVERT_TZ`

## Description

Converts a datetime between two time zones.

## Syntax

```sql
CONVERT_TZ(datetime, from_tz, to_tz)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `datetime` | Yes | date, time, datetime, or the corresponding text/numeric value | Wall-clock datetime interpreted in from_tz. |
| `from_tz` | Yes | Time-zone string | Source fixed offset or supported IANA zone name |
| `to_tz` | Yes | Time-zone string | Destination fixed offset or supported IANA zone name |

## Return Value

Returns the datetime obtained by interpreting the input in from_tz and rendering it in to_tz. Invalid timezone arguments return NULL. The extended year-9999 range and pre-epoch behavior are described below. SQL NULL in a required input returns NULL.

## Examples

```sql
SELECT CONVERT_TZ('2026-08-30 12:00:00', '+00:00', '+08:00');
```

Result: 2026-08-30 20:00:00

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.
- Future conversions support mydb's extended UTC TIMESTAMP domain through
  year 9999, including named-zone future DST rules. UTC or target-wall-clock
  overflow beyond year 9999 returns `NULL`. Pre-epoch conversions retain the
  existing behavior of returning the input unchanged.

- Fixed offsets range from -13:59 through +14:00. Supported named zones use the host/platform timezone database. In ambiguous fall-back local time the earlier instant is selected; a spring-forward skipped local time is shifted forward. Use explicit UTC input when local wall time is ambiguous.

## Related Functions

[`UNIX_TIMESTAMP`](unix_timestamp.md), [`FROM_UNIXTIME`](from_unixtime.md).
