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
| `ipv4_number` | Yes | Unsigned 32-bit integer | IPv4 address number in 0..4294967295. |

## Return Value

Returns dotted-quad IPv4 text for an unsigned 32-bit address. SQL NULL or an out-of-range number returns NULL with the applicable conversion diagnostic.

## Examples

```sql
SELECT INET_NTOA(2130706433);
```

Result: 127.0.0.1

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`INET_ATON`](inet_aton.md), [`IS_IPV4`](is_ipv4.md).
