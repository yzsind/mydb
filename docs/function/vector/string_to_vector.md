# `STRING_TO_VECTOR`

## Description

Parses a nonempty bracketed array, or converts an existing numeric VECTOR,
into the requested native datatype. TO_VECTOR is an alias.

## Syntax

```sql
STRING_TO_VECTOR(value [, datatype])
TO_VECTOR(value [, datatype])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | Yes | Vector array text or native VECTOR | Input coordinates or numeric datatype conversion |
| `datatype` | No | Case-insensitive datatype string | FLOAT32 (default), FLOAT16, BFLOAT16, INT8 or BINARY |

## Return Value

Returns a native VECTOR. Omitted datatype selects FLOAT32 even for a supplied typed
numeric VECTOR. SQL NULL in either supplied argument returns NULL. Nonfinite floats,
invalid dimensions, nonintegral/out-of-range INT8, non-bit BINARY elements and
incompatible numeric/BINARY conversions raise a vector error.

## Examples

```sql
SELECT VECTOR_DATATYPE(TO_VECTOR('[1,2.5,-3]','FLOAT16')),
       VECTOR_DIM(TO_VECTOR('[1,0,1]','BINARY'));
```

Result:

```text
FLOAT16	3
```

## Notes

- Floating elements must remain finite after conversion. Reduced float types round to their precision; INT8 requires integers in [-128,127] and BINARY requires 0/1.
- Raw payloads require VECTOR_FROM_BINARY(payload,datatype,dimension); raw byte length is not a datatype guess. Numeric casts also support CAST(value AS VECTOR(n,datatype)).
- See [native datatype limits](README.md#native-datatype-limits) for dimensions, payload layout and the default NATIVE client display. Use VECTOR_TO_STRING to display coordinates as text.

## Related Functions

[`TO_VECTOR`](to_vector.md), [`VECTOR_FROM_BINARY`](vector_from_binary.md), [`VECTOR_TO_STRING`](vector_to_string.md), [`VECTOR_DATATYPE`](vector_datatype.md).
