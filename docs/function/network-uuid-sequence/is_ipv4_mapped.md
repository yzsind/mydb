# `IS_IPV4_MAPPED`

## Description

Determines whether a binary IPv6 address is an IPv4-mapped address.

## Syntax

```sql
IS_IPV4_MAPPED(ip_binary)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `ip_binary` | Yes | 16-byte binary IPv6 value | Use INET6_ATON for textual addresses; other payload shapes return 0. SQL NULL returns NULL. |

## Return Value

Returns integer 1 for a valid 16-byte IPv4-mapped IPv6 address, 0 otherwise. SQL NULL returns SQL NULL.

## Examples

```sql
SELECT IS_IPV4_MAPPED(INET6_ATON('::ffff:192.0.2.1'));
```

Result: 1

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`INET6_ATON`](inet6_aton.md), [`INET6_NTOA`](inet6_ntoa.md), [`IS_IPV6`](is_ipv6.md), [`IS_IPV4_COMPAT`](is_ipv4_compat.md).
