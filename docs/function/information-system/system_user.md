# `SYSTEM_USER`

## Description

Returns the account that established the current connection; an alias of `USER`. This function shares the same implementation and semantics as [`USER`](user.md).

## Syntax

```sql
SYSTEM_USER()
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`USER`](user.md); refer to the primary function's documentation.

## Examples

```sql
SELECT SYSTEM_USER();
```

Result: For a root TCP connection to 127.0.0.1, returns `root@127.0.0.1`. The login name and peer address change with the connection.

## Related Functions

- [`USER`](user.md): the primary function with full documentation.
