# `COMPRESS`

## Description

Compresses the input string and returns binary data.

## Syntax

```sql
COMPRESS(value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | Yes | String or binary string; convertible scalar where accepted | Input bytes/text; numeric scalars are converted to their display bytes. |

## Return Value

Returns binary data containing a four-byte little-endian uncompressed byte length followed by a zlib stream. Empty input returns empty bytes; SQL NULL returns NULL.

## Examples

```sql
SELECT UNCOMPRESS(COMPRESS('mydb'));
```

Result: mydb

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`UNCOMPRESS`](uncompress.md), [`UNCOMPRESSED_LENGTH`](uncompressed_length.md).
