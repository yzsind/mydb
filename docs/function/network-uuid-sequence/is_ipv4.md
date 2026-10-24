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
| `ip_text` | Yes | String | The 1st argument; the IPv4 or IPv6 text address |

## Return Value

Returns `1` when the condition holds and `0` otherwise; `NULL` input usually returns `NULL`.

## Examples

```sql
SELECT IS_IPV4('127.0.0.1');
```

Result: 1

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
