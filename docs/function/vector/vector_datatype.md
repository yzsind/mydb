# `VECTOR_DATATYPE`

## Description

Returns `FLOAT32`, `FLOAT16`, `BFLOAT16`, `INT8` or `BINARY`. Plain array text is parsed as FLOAT32; typed vectors retain their datatype.

## Syntax

```sql
VECTOR_DATATYPE(vector)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `vector` | Yes | Native VECTOR or vector array text | Numeric text defaults to FLOAT32; raw payload bytes require VECTOR_FROM_BINARY. |

## Return Value

Returns the datatype-name text FLOAT32, FLOAT16, BFLOAT16, INT8 or BINARY. Text arrays are parsed as FLOAT32; native values preserve their datatype. SQL NULL returns NULL.

## Examples

```sql
SELECT VECTOR_DATATYPE(TO_VECTOR('[1,2]', 'FLOAT16')); -- FLOAT16
```

See [vector datatypes and indexes](../../vector-indexes.md).

## Notes

- See [native datatype limits](README.md#native-datatype-limits) for maximum dimensions, precision and wire/payload representation. Arity is checked before SQL NULL propagation.

Arguments containing NULL return NULL. Invalid dimensions, payloads or datatype combinations raise an explicit vector error.

## Related Functions

[`VECTOR_DIM`](vector_dim.md), [`VECTOR_TO_BINARY`](vector_to_binary.md).
