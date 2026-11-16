# `HAMMING_DISTANCE`

## Description

Returns the number of differing bits of two BINARY vectors with equal bit dimensions, as a double-precision value. Array text is interpreted as bits in this function. Numeric VECTOR operands are rejected.

## Syntax

```sql
HAMMING_DISTANCE(vector1, vector2)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `vector1` | Yes | BINARY VECTOR or bit-array text | First operand. |
| `vector2` | Yes | BINARY VECTOR or bit-array text | Second operand with the same bit dimension. |

## Return Value

Returns the number of XOR bits as DOUBLE for equal-dimension BINARY operands. Array text is interpreted as bits by this function. SQL NULL returns NULL; numeric native VECTOR operands, dimension mismatches and invalid payloads raise a vector error.

## Examples

```sql
SELECT HAMMING_DISTANCE(TO_VECTOR('[1,0,1]','BINARY'),TO_VECTOR('[0,0,1]','BINARY')); -- 1
```

See [vector datatypes and indexes](../../vector-indexes.md).

## Notes

- See [native datatype limits](README.md#native-datatype-limits) for maximum dimensions, precision and wire/payload representation. Arity is checked before SQL NULL propagation.

Arguments containing NULL return NULL. Invalid dimensions, payloads or datatype combinations raise an explicit vector error.

## Related Functions

[`JACCARD_DISTANCE`](jaccard_distance.md), [`DISTANCE`](distance.md).
