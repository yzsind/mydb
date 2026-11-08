# `VECTOR_TO_BINARY`

## Description

Returns native payload bytes without a version/type envelope. Use `VECTOR_DATATYPE` and `VECTOR_DIM` alongside the payload when transporting it. Reconstruction requires explicit datatype and dimension.

## Syntax

```sql
VECTOR_TO_BINARY(vector)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `vector` | Yes | Native VECTOR or vector array text | Numeric text defaults to FLOAT32; raw payload bytes require VECTOR_FROM_BINARY. |

## Return Value

Returns only native payload bytes, without a datatype/version envelope. SQL NULL returns NULL. Preserve VECTOR_DATATYPE and VECTOR_DIM separately for reconstruction; an unknown raw byte string is not automatically a typed vector.

## Examples

```sql
SELECT HEX(VECTOR_TO_BINARY(TO_VECTOR('[-128,0,127]','INT8'))); -- 80007F
```

See [vector datatypes and indexes](../../vector-indexes.md).

## Notes

- See [native datatype limits](README.md#native-datatype-limits) for maximum dimensions, precision and wire/payload representation. Arity is checked before SQL NULL propagation.

Arguments containing NULL return NULL. Invalid dimensions, payloads or datatype combinations raise an explicit vector error.

## Related Functions

[`VECTOR_FROM_BINARY`](vector_from_binary.md), [`VECTOR_DATATYPE`](vector_datatype.md), [`VECTOR_DIM`](vector_dim.md).
