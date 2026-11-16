# `FROM_UNIXTIME`

## Description

Converts a Unix timestamp to a datetime in the session time zone, with optional formatted output.

## Syntax

```sql
FROM_UNIXTIME(unix_timestamp [, format])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `unix_timestamp` | Yes | numeric value | The number of Unix seconds |
| `format` | No | format string | When provided, returns formatted text |

## Return Value

Without format, returns a DATETIME in the session time zone; with format, returns formatted text. Fractional Unix seconds preserve up to six digits. Negative/out-of-range input returns NULL; see the extended upper bound below. SQL NULL in a required input returns NULL.

## Examples

```sql
SET SESSION time_zone='+00:00';
SELECT FROM_UNIXTIME(0), FROM_UNIXTIME(0,'%Y-%m-%d');
```

Result:

```text
1970-01-01 00:00:00	1970-01-01
```

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.
- mydb accepts Unix seconds from zero through `253402300799.999999`.
  Use exact decimal or text input to preserve microseconds near year 9999.
  Rounding into year 10000, or a session time-zone conversion beyond calendar
  years 0000–9999, returns `NULL`, including when a format argument is present.

## Related Functions

[`UNIX_TIMESTAMP`](unix_timestamp.md), [`CONVERT_TZ`](convert_tz.md).
