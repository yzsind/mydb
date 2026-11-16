# `SCHEMA`

## Description

Returns the current default database; an alias of `DATABASE`. This function shares the same implementation and semantics as [`DATABASE`](database.md).

## Syntax

```sql
SCHEMA()
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`DATABASE`](database.md); refer to the primary function's documentation.

## Examples

```sql
SELECT SCHEMA();
```

Result: With the example connection selecting the `test` database, returns the string `test`. A connection with no selected database returns SQL NULL.

## Related Functions

- [`DATABASE`](database.md): the primary function with full documentation.
