# `IS_IPV4`

## Description

Determines whether a string is a valid IPv4 address.

## Syntax

```sql
IS_IPV4(ip_text)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `ip_text` | Yes | String | IPv4 or IPv6 address text. |

## Return Value

Returns integer 1 for a valid strict dotted-quad IPv4 address, 0 otherwise. SQL NULL returns SQL NULL.

## Examples

```sql
SELECT IS_IPV4('127.0.0.1');
```

Result: 1

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`INET_ATON`](inet_aton.md), [`INET_NTOA`](inet_ntoa.md).
