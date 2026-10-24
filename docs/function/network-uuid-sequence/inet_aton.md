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
| `ipv4_text` | Yes | String | The 1st argument; the dotted-quad IPv4 address |

## Return Value

Converts an IPv4 text address to an integer. The return type is determined by the function semantics and the input type.

## Examples

```sql
SELECT INET_ATON('127.0.0.1');
```

Result: 2130706433

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
