# `JACCARD_DISTANCE`

## Description

Returns `1 - popcount(a AND b)/popcount(a OR b)` for equal-dimension BINARY vectors, as a double-precision value. Two all-zero vectors have distance zero. Array text is interpreted as bits in this function; numeric VECTOR operands are rejected.

## Syntax

```sql
JACCARD_DISTANCE(vector1, vector2)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `vector1` | Yes | BINARY VECTOR or bit-array text | First operand. |
| `vector2` | Yes | BINARY VECTOR or bit-array text | Second operand with the same bit dimension. |

## Return Value

Returns DOUBLE 1-popcount(a AND b)/popcount(a OR b) for equal-dimension BINARY operands; two all-zero vectors return 0. Array text is parsed as bits. SQL NULL returns NULL; numeric native VECTOR operands or dimension mismatches raise a vector error.

## Examples

```sql
SELECT JACCARD_DISTANCE(TO_VECTOR('[1,0,1]','BINARY'),TO_VECTOR('[0,0,1]','BINARY')); -- 0.5
```

See [vector datatypes and indexes](../../vector-indexes.md).

## Notes

- See [native datatype limits](README.md#native-datatype-limits) for maximum dimensions, precision and wire/payload representation. Arity is checked before SQL NULL propagation.

Arguments containing NULL return NULL. Invalid dimensions, payloads or datatype combinations raise an explicit vector error.

## Related Functions

[`HAMMING_DISTANCE`](hamming_distance.md), [`DISTANCE`](distance.md).
