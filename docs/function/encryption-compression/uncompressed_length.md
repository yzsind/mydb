# `UNCOMPRESSED_LENGTH`

## Description

Returns the original length recorded in compressed data.

## Syntax

```sql
UNCOMPRESSED_LENGTH(value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | Yes | String or binary string; convertible scalar where accepted | Input bytes/text; numeric scalars are converted to their display bytes. |

## Return Value

Returns the integer length recorded in the first four compressed bytes, 0 for empty/too-short input, or SQL NULL for NULL input. It reads the header and does not validate or decompress the stream.

## Examples

```sql
SELECT UNCOMPRESSED_LENGTH(COMPRESS('mydb'));
```

Result: 4

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

- A nonempty payload of at most four bytes returns 0 with a corruption warning. A longer payload can have a readable length header even when its compressed stream is invalid; use UNCOMPRESS to validate/decode it.

## Related Functions

[`COMPRESS`](compress.md), [`UNCOMPRESS`](uncompress.md).
