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
| `ip_text` | Yes | String | The 1st argument; the IPv4 or IPv6 text address |

## Return Value

Converts an IPv6 or IPv4 text address to binary format. The return type is determined by the function semantics and the input type.

## Examples

```sql
SELECT HEX(INET6_ATON('::1'));
```

Result: 00000000000000000000000000000001

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
