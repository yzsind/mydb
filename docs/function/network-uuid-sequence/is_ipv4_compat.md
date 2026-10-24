# `IS_IPV4_COMPAT`

## Description

Determines whether a binary IPv6 address is an IPv4-compatible address.

## Syntax

```sql
IS_IPV4_COMPAT(ip_binary)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `ip_binary` | Yes | Address, UUID, sequence name, integer, or flag value | The 1st argument; the network-byte-order binary address |

## Return Value

Returns `1` when the condition holds and `0` otherwise; `NULL` input usually returns `NULL`.

## Examples

```sql
SELECT IS_IPV4_COMPAT(INET6_ATON('::192.0.2.1'));
```

Result: 1

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
