# `CAST`

## Description

Converts an expression to the specified SQL data type.

## Syntax

```sql
CAST(expr AS type)
CONVERT(expr, type)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | Any expression | The value to convert |
| `type` | Yes | Supported CAST target type | The target data type with optional length, precision, or charset |

## Return Value

Returns the target SQL type, with its requested length, precision/scale or fractional-second precision. SQL NULL returns NULL. Invalid target types are rejected; malformed values, truncation, rounding and overflow follow the target conversion and statement SQL-mode rules. See the supported targets below.

## Examples

```sql
SELECT CAST('42' AS SIGNED);
```

Result: 42

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

### Supported targets

| Target | Meaning and bounds |
| --- | --- |
| SIGNED [INTEGER], UNSIGNED [INTEGER] | Signed or unsigned 64-bit integer |
| DECIMAL[(M[,D])] | Exact decimal; default DECIMAL(10,0), precision up to 65, scale up to 30 and not greater than M |
| FLOAT, DOUBLE, REAL | Approximate numeric conversion; REAL maps to DOUBLE here |
| CHAR[(N)], NCHAR[(N)] | Text, optionally bounded in characters; NCHAR uses the national character set |
| BINARY[(N)] | Bytes, optionally truncated or zero-padded to N bytes |
| DATE, TIME[(fsp)], DATETIME[(fsp)] | Temporal conversion; fsp is 0 through 6 |
| YEAR | Year conversion with the supported YEAR domain |
| JSON | Parse/convert a JSON document or typed scalar |
| VECTOR(n[, datatype]) | Native mydb vector conversion; see the vector datatype reference |

The parser also maps integer type names to SIGNED, character/text aliases to
CHAR, binary/blob aliases to BINARY, and TIMESTAMP to DATETIME. BOOLEAN casts
are unsupported. Character-set clauses use the supported charset names.
Do not infer support for every table-column type as a CAST target.

`CONVERT(expr, type)` uses the same conversion path. `CONVERT(expr USING charset)`
is a separate character-set operation, documented under [CONVERT USING](convert_using.md).
See [Data types](../../data-types.md) for storage types and
[vector conversion](../vector/string_to_vector.md) for native datatype bounds.

## Related Functions

[`CONVERT_USING`](convert_using.md), [`COLLATE`](collate.md).
