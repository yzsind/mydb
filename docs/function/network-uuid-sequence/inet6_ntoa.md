# `INET6_NTOA`

## Description

Converts a binary IPv6 or IPv4 address to text.

## Syntax

```sql
INET6_NTOA(ip_binary)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `ip_binary` | Yes | 4-byte or 16-byte binary string | Network-byte-order address bytes. |

## Return Value

Returns canonical IPv4/IPv6 address text from exactly 4/16 bytes. SQL NULL returns NULL; other payload types/lengths produce the applicable invalid-address diagnostic.

## Examples

```sql
SELECT INET6_NTOA(UNHEX('00000000000000000000000000000001'));
```

Result: ::1

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`INET6_ATON`](inet6_aton.md), [`IS_IPV6`](is_ipv6.md), [`IS_IPV4_COMPAT`](is_ipv4_compat.md), [`IS_IPV4_MAPPED`](is_ipv4_mapped.md).
