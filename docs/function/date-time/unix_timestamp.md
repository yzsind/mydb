# `UNIX_TIMESTAMP`

## Description

Returns the current Unix timestamp, or converts a datetime to a Unix timestamp.

## Syntax

```sql
UNIX_TIMESTAMP([datetime])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `temporal_value` | No | date, time, datetime, or the corresponding text/numeric value | Temporal value to process; accepted forms and result range are specified below. |

## Return Value

With no argument, returns the current statement Unix second count. With an argument, returns nonnegative epoch seconds; fractional input returns exact decimal seconds up to microseconds. TIMESTAMP input is already UTC, while text/DATETIME use the session time zone. Out-of-range datetime input returns zero.

## Examples

```sql
SET SESSION time_zone='+00:00';
SELECT UNIX_TIMESTAMP('1970-01-01 00:00:00');
```

Result:

```text
0
```

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.
- mydb supports nonnegative epoch seconds through `253402300799.999999`
  (`9999-12-31 23:59:59.999999` UTC). A TIMESTAMP argument is already UTC;
  DATETIME and text arguments use the session time zone. Exact decimal output
  preserves microseconds. Out-of-range datetime arguments return zero.
  The extended future range is intentional: unlike MySQL's function boundary
  near year 3001, a valid year-9999 input returns its epoch seconds. The native
  TIMESTAMP range and WAL replication preserve this domain; MySQL binlog
  encodability and destination-column ranges impose separate limits, as
  documented in [MySQL compatibility](../../mysql80-compatibility.md).

## Related Functions

[`FROM_UNIXTIME`](from_unixtime.md), [`CONVERT_TZ`](convert_tz.md).
