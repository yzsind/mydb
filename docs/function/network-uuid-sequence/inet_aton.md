# `INET_ATON`

## Description

Converts an IPv4 text address to an integer.

## Syntax

```sql
INET_ATON(ipv4_text)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `ipv4_text` | Yes | String | IPv4 address text. |

## Return Value

Returns an unsigned integer IPv4 address in network-byte order. SQL NULL or invalid address text returns NULL. This legacy conversion accepts some abbreviated IPv4 forms; use IS_IPV4 for strict dotted-quad validation.

## Examples

```sql
SELECT INET_ATON('127.0.0.1');
```

Result: 2130706433

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`INET_NTOA`](inet_ntoa.md), [`IS_IPV4`](is_ipv4.md).
