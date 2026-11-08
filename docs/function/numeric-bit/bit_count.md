# `BIT_COUNT`

## Description

Returns the number of set bits in an integer's binary representation.

## Syntax

```sql
BIT_COUNT(number)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Numeric expression | Numeric operand; strings use numeric conversion. |

## Return Value

Returns an integer count of set bits. Numeric evaluation uses an unsigned 64-bit representation; binary-string evaluation counts set bits in the bytes. SQL NULL returns NULL.

## Examples

```sql
SELECT BIT_COUNT(2);
```

Result:

```text
1
```

## Notes

- See [numeric domains and diagnostics](README.md#domains-and-diagnostics) for conversion, domain, and overflow rules.

- `BIT_COUNT(-1)` uses 64-bit numeric evaluation and returns 64. Explicit binary data is counted byte by byte; a numeric/hex literal and a typed binary string can therefore select different evaluation domains.

## Related Functions

[`BIN`](bin.md), [`OCT`](oct.md), [`CONV`](conv.md), [`CRC32`](crc32.md).
