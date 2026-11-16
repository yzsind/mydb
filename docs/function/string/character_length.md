# `CHARACTER_LENGTH`

## Description

Returns the number of characters in a string; an alias of `CHAR_LENGTH`. This function shares the same implementation and semantics as [`CHAR_LENGTH`](char_length.md).

## Syntax

```sql
CHARACTER_LENGTH(str)
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`CHAR_LENGTH`](char_length.md); refer to the primary function's documentation.

## Examples

```sql
SELECT CHARACTER_LENGTH('mydb');
```

Result: 4

## Related Functions

- [`CHAR_LENGTH`](char_length.md): the primary function with full documentation.
