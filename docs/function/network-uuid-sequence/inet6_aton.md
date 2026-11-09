# `INET6_ATON`

## Description

Converts an IPv6 or IPv4 text address to binary format.

## Syntax

```sql
INET6_ATON(ip_text)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `ip_text` | Yes | String | IPv4 or IPv6 address text. |

## Return Value

Returns 4 bytes for IPv4 or 16 bytes for IPv6, in network-byte order. SQL NULL or invalid address text returns NULL. A zone identifier or port is not an address literal accepted by this function.

## Examples

```sql
SELECT HEX(INET6_ATON('::1'));
```

Result: 00000000000000000000000000000001

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`INET6_NTOA`](inet6_ntoa.md), [`IS_IPV6`](is_ipv6.md), [`IS_IPV4_COMPAT`](is_ipv4_compat.md), [`IS_IPV4_MAPPED`](is_ipv4_mapped.md).
