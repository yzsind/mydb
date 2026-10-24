# `DAY`

## Description

Returns the day of the month for a date; an alias of `DAYOFMONTH`. This function shares the same implementation and semantics as [`DAYOFMONTH`](dayofmonth.md).

## Syntax

```sql
DAY(temporal_value)
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`DAYOFMONTH`](dayofmonth.md); refer to the primary function's documentation.

## Examples

```sql
SELECT DAY('2026-08-30 12:34:56');
```

Result:

```text
30
```

## Related Functions

- [`DAYOFMONTH`](dayofmonth.md): the primary function with full documentation.
