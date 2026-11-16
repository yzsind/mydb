# `UNCOMPRESS`

## Description

Uncompresses data produced by `COMPRESS`.

## Syntax

```sql
UNCOMPRESS(value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | Yes | String or binary string; convertible scalar where accepted | Input bytes/text; numeric scalars are converted to their display bytes. |

## Return Value

Returns the original binary bytes for a valid COMPRESS payload, empty bytes for empty input, or NULL for SQL NULL/corrupt/oversized payloads. Corrupt streams produce warning 1259; an advertised size above 64 MiB produces the decompression-size diagnostic.

## Examples

```sql
SELECT UNCOMPRESS(COMPRESS('mydb'));
```

Result: mydb

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`COMPRESS`](compress.md), [`UNCOMPRESSED_LENGTH`](uncompressed_length.md).
