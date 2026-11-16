# `TIMESTAMP`

## Description

Combines a date and an optional time into a datetime.

## Syntax

```sql
TIMESTAMP(date [, time])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `date` | Yes | date or datetime | The base value |
| `time` | No | time | Added to the base date |

## Return Value

Returns a DATETIME value combining the date/time arguments. SQL NULL input returns NULL; invalid temporal values follow the conversion rules.

## Examples

```sql
SELECT TIMESTAMP('2026-08-30 12:34:56');
```

Result:

```text
2026-08-30 12:34:56
```

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`DATE`](date.md), [`TIME`](time.md), [`EXTRACT`](extract.md).
