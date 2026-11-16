# `BIN`

## Description

Returns a binary string representation of an integer.

## Syntax

```sql
BIN(number)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Numeric expression | Numeric operand; strings use numeric conversion. |

## Return Value

Returns the unsigned integer representation as a string in base 2. SQL NULL input returns NULL.

## Examples

```sql
SELECT BIN(2);
```

Result:

```text
10
```

## Notes

- See [numeric domains and diagnostics](README.md#domains-and-diagnostics) for conversion, domain, and overflow rules.

## Related Functions

[`OCT`](oct.md), [`CONV`](conv.md), [`BIT_COUNT`](bit_count.md), [`CRC32`](crc32.md).
