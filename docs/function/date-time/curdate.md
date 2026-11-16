# `CURDATE`

## Description

Returns the current date in the session time zone.

## Syntax

```sql
CURDATE()
```

## Parameters

This function takes no arguments.

## Return Value

Returns a DATE for the statement-start instant in the session time zone. It has no arguments; use SET timestamp only when a reproducible test clock is needed.

## Examples

```sql
SELECT CURDATE();
```

Result: returns the current session date; the exact value varies with the execution date.

## Notes

- See [temporal inputs and precision](README.md#temporal-inputs-and-precision) for SQL mode, input conversion, fractional seconds, and range diagnostics.

## Related Functions

[`CURRENT_DATE`](current_date.md).
