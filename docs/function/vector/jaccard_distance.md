# `JACCARD_DISTANCE`

Returns `1 - popcount(a AND b)/popcount(a OR b)` for equal-dimension BINARY vectors, as a double-precision value. Two all-zero vectors have distance zero. Array text is interpreted as bits in this function; numeric VECTOR operands are rejected.

## Syntax

```sql
JACCARD_DISTANCE(vector1, vector2)
```

Arguments containing NULL return NULL. Invalid dimensions, payloads or datatype combinations raise an explicit vector error.

## Example

```sql
SELECT JACCARD_DISTANCE(TO_VECTOR('[1,0,1]','BINARY'),TO_VECTOR('[0,0,1]','BINARY')); -- 0.5
```

See [vector datatypes and indexes](../../vector-indexes.md).
