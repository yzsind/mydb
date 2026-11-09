# `IS_IPV6`

## Description

Determines whether a string is a valid IPv6 address.

## Syntax

```sql
IS_IPV6(ip_text)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `ip_text` | Yes | String | IPv4 or IPv6 address text. |

## Return Value

Returns integer 1 for a valid IPv6 address literal, 0 otherwise. SQL NULL returns SQL NULL.

## Examples

```sql
SELECT IS_IPV6('::1');
```

Result: 1

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`INET6_ATON`](inet6_aton.md), [`INET6_NTOA`](inet6_ntoa.md), [`IS_IPV4_COMPAT`](is_ipv4_compat.md), [`IS_IPV4_MAPPED`](is_ipv4_mapped.md).
