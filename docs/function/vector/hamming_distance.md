# `HAMMING_DISTANCE`

Returns the number of differing bits of two BINARY vectors with equal bit dimensions, as a double-precision value. Array text is interpreted as bits in this function. Numeric VECTOR operands are rejected.

## Syntax

```sql
HAMMING_DISTANCE(vector1, vector2)
```

Arguments containing NULL return NULL. Invalid dimensions, payloads or datatype combinations raise an explicit vector error.

## Example

```sql
SELECT HAMMING_DISTANCE(TO_VECTOR('[1,0,1]','BINARY'),TO_VECTOR('[0,0,1]','BINARY')); -- 1
```

See [vector datatypes and indexes](../../vector-indexes.md).
