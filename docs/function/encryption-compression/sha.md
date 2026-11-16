# `SHA`

## Description

Returns the SHA-1 hexadecimal digest of the input; an alias of `SHA1`. This function shares the same implementation and semantics as [`SHA1`](sha1.md).

## Syntax

```sql
SHA(value)
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`SHA1`](sha1.md); refer to the primary function's documentation.

## Examples

```sql
SELECT SHA(2);
```

Result:

```text
da4b9237bacccdf19c0760cab7aec4a8359010b0
```

## Related Functions

- [`SHA1`](sha1.md): the primary function with full documentation.
