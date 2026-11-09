# `UTC_DATE`

## Description

Returns the current UTC date.

## Syntax

```sql
UTC_DATE()
```

## Parameters

This function takes no arguments.

## Return Value

Returns a DATE for the statement-start instant in UTC, independent of the session display time zone.

## Examples

```sql
SELECT UTC_DATE();
```

Result: Returns the current UTC DATE, for example `2026-10-22`; it can differ from the date in the session time zone.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`CURDATE`](curdate.md), [`CURTIME`](curtime.md), [`NOW`](now.md), [`SYSDATE`](sysdate.md).
