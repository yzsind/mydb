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
| `ip_binary` | Yes | Address, UUID, sequence name, integer, or flag value | The 1st argument; the network-byte-order binary address |

## Return Value

Converts a binary IPv6 or IPv4 address to text. The return type is determined by the function semantics and the input type.

## Examples

```sql
SELECT INET6_NTOA(UNHEX('00000000000000000000000000000001'));
```

Result: ::1

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
