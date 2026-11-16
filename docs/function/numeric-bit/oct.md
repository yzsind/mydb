# `OCT`

## Description

Returns an octal string representation of an integer.

## Syntax

```sql
OCT(number)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Numeric expression | Numeric operand; strings use numeric conversion. |

## Return Value

Returns the unsigned integer representation as a string in base 8. SQL NULL input returns NULL.

## Examples

```sql
SELECT OCT(2);
```

Result:

```text
2
```

## Notes

- See [numeric domains and diagnostics](README.md#domains-and-diagnostics) for conversion, domain, and overflow rules.

## Related Functions

[`BIN`](bin.md), [`CONV`](conv.md), [`BIT_COUNT`](bit_count.md), [`CRC32`](crc32.md).
