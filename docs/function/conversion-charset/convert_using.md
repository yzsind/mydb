# `CONVERT_USING`

## Description

Converts bytes or text using the named character set. Write `CONVERT(expr USING charset_name)` in SQL; CONVERT_USING is the implementation/index name for that syntax.

## Syntax

```sql
CONVERT(expr USING charset_name)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | String, binary string or convertible scalar | Content to convert |
| `charset_name` | Yes | Charset name | Target encoding; binary requests bytes |

## Return Value

Returns text in the named character set, or binary bytes for USING binary. SQL NULL input returns NULL. Binary input is decoded using the target encoding; text input is encoded into that charset. Invalid encodings and unrepresentable characters produce the applicable conversion error or replacement diagnostic.

## Examples

```sql
SELECT CONVERT('mydb' USING utf8mb4);
```

Result: mydb

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`CAST`](cast.md), [`COLLATE`](collate.md).
