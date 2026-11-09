# `CONV`

## Description

Converts integer text between bases from 2 to 36.

## Syntax

```sql
CONV(number, from_base, to_base)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `number` | Yes | Integer text or numeric value | Digits interpreted in from_base; the result is returned as text |
| `from_base` | Yes | Numeric expression | Input radix; its absolute value must be 2 through 36. |
| `to_base` | Yes | Numeric expression | Output radix; its absolute value must be 2 through 36; a negative radix requests signed output. |

## Return Value

Returns an integer representation as a string in the target base. SQL NULL input or a base outside the supported magnitude 2–36 returns NULL. Negative base signs select signed interpretation/output under the conversion rules.

## Examples

```sql
SELECT CONV('FF',16,10);
```

Result:

```text
255
```

## Notes

- See [numeric domains and diagnostics](README.md#domains-and-diagnostics) for conversion, domain, and overflow rules.

- The result is text. Negative input is represented using the selected signed/unsigned 64-bit conversion rules; invalid bases return NULL. Trailing non-digit content can produce conversion diagnostics. Use a negative output base when signed output is required.

## Related Functions

[`BIN`](bin.md), [`OCT`](oct.md), [`BIT_COUNT`](bit_count.md), [`CRC32`](crc32.md).
