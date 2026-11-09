# `VECTOR_FROM_BINARY`

## Description

Constructs a typed vector from binary payload bytes. Datatype and dimension are mandatory; byte length is never used to guess the datatype. Numeric values use little endian IEEE binary32, IEEE binary16, bfloat16 or signed two's-complement INT8. BINARY packs bit i into bit i%8 of byte i/8. Length, dimension, finite floats and zero tail padding are validated.

## Syntax

```sql
VECTOR_FROM_BINARY(payload, datatype, dimension)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `payload` | Yes | Binary string | Native payload bytes; ordinary text is rejected. |
| `datatype` | Yes | Datatype-name string | Explicit FLOAT32/FLOAT16/BFLOAT16/INT8/BINARY; never guessed from length. |
| `dimension` | Yes | Positive integral argument | Element count, or bit count for BINARY; payload length must match exactly. |

## Return Value

Returns a native VECTOR from the exact binary payload, datatype and dimension. Any SQL NULL argument returns NULL. Invalid payload length/type, dimensions, nonfinite floats or nonzero BINARY tail-padding bits raise a vector error.

## Examples

```sql
SELECT VECTOR_TO_STRING(VECTOR_FROM_BINARY(X'0501','BINARY',9)); -- [1,0,1,0,0,0,0,0,1]
```

See [vector datatypes and indexes](../../vector-indexes.md).

## Notes

- See [native datatype limits](README.md#native-datatype-limits) for maximum dimensions, precision and wire/payload representation. Arity is checked before SQL NULL propagation.

Arguments containing NULL return NULL. Invalid dimensions, payloads or datatype combinations raise an explicit vector error.

## Related Functions

[`VECTOR_TO_BINARY`](vector_to_binary.md), [`STRING_TO_VECTOR`](string_to_vector.md).
