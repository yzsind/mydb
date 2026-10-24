# `STRING_TO_VECTOR`

Parses a nonempty bracketed array into a native vector.

```sql
STRING_TO_VECTOR(text [, datatype])
TO_VECTOR(text [, datatype])
```

Datatype is a case-insensitive string: `FLOAT32` (default), `FLOAT16`,
`BFLOAT16`, `INT8` or `BINARY`. Floating elements must be finite after
conversion. INT8 requires integers in [-128,127]; BINARY requires zeros and
ones. Invalid values and dimensions raise a vector error. Any NULL argument
returns NULL.

```sql
SELECT VECTOR_DATATYPE(TO_VECTOR('[1,2.5,-3]', 'FLOAT16'));
-- FLOAT16
SELECT VECTOR_DIM(TO_VECTOR('[1,0,1]', 'BINARY'));
-- 3
```

Raw payloads require `VECTOR_FROM_BINARY(payload, datatype, dimension)`.
Numeric datatype conversion also supports `CAST(value AS VECTOR(n, datatype))`.
Conversions between numeric and BINARY vectors are not implicit.

See [vector datatypes and indexes](../../vector-indexes.md).
