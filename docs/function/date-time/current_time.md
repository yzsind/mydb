# `CURRENT_TIME`

## Description

Returns the current time in the session time zone; an alias of `CURTIME`. This function shares the same implementation and semantics as [`CURTIME`](curtime.md).

## Syntax

```sql
CURRENT_TIME([fsp])
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`CURTIME`](curtime.md); refer to the primary function's documentation.

## Examples

```sql
SELECT CURRENT_TIME(3);
```

Result: Returns the current session-zone TIME with three fractional digits, for example `12:34:56.789`; the clock value changes between statements.

## Related Functions

- [`CURTIME`](curtime.md): the primary function with full documentation.
