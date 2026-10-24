# `INET_NTOA`

## Description

Converts an integer IPv4 address to text.

## Syntax

```sql
INET_NTOA(ipv4_number)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `ipv4_number` | Yes | Address, UUID, sequence name, integer, or flag value | The 1st argument; the 32-bit IPv4 integer |

## Return Value

Converts an integer IPv4 address to text. The return type is determined by the function semantics and the input type.

## Examples

```sql
SELECT INET_NTOA(2130706433);
```

Result: 127.0.0.1

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
