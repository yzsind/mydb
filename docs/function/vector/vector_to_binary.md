# `VECTOR_TO_BINARY`

Returns native payload bytes without a version/type envelope. Use `VECTOR_DATATYPE` and `VECTOR_DIM` alongside the payload when transporting it. Reconstruction requires explicit datatype and dimension.

## Syntax

```sql
VECTOR_TO_BINARY(vector)
```

Arguments containing NULL return NULL. Invalid dimensions, payloads or datatype combinations raise an explicit vector error.

## Example

```sql
SELECT HEX(VECTOR_TO_BINARY(TO_VECTOR('[-128,0,127]','INT8'))); -- 80007F
```

See [vector datatypes and indexes](../../vector-indexes.md).
