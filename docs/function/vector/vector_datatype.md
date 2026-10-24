# `VECTOR_DATATYPE`

Returns `FLOAT32`, `FLOAT16`, `BFLOAT16`, `INT8` or `BINARY`. Plain array text is parsed as FLOAT32; typed vectors retain their datatype.

## Syntax

```sql
VECTOR_DATATYPE(vector)
```

Arguments containing NULL return NULL. Invalid dimensions, payloads or datatype combinations raise an explicit vector error.

## Example

```sql
SELECT VECTOR_DATATYPE(TO_VECTOR('[1,2]', 'FLOAT16')); -- FLOAT16
```

See [vector datatypes and indexes](../../vector-indexes.md).
