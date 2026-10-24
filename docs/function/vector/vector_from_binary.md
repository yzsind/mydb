# `VECTOR_FROM_BINARY`

Constructs a typed vector from binary payload bytes. Datatype and dimension are mandatory; byte length is never used to guess the datatype. Numeric values use little endian IEEE binary32, IEEE binary16, bfloat16 or signed two's-complement INT8. BINARY packs bit i into bit i%8 of byte i/8. Length, dimension, finite floats and zero tail padding are validated.

## Syntax

```sql
VECTOR_FROM_BINARY(payload, datatype, dimension)
```

Arguments containing NULL return NULL. Invalid dimensions, payloads or datatype combinations raise an explicit vector error.

## Example

```sql
SELECT VECTOR_TO_STRING(VECTOR_FROM_BINARY(X'0501','BINARY',9)); -- [1,0,1,0,0,0,0,0,1]
```

See [vector datatypes and indexes](../../vector-indexes.md).
