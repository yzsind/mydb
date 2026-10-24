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
| `value` | Yes | Numeric or integer | The 1st argument; the input value |

## Return Value

Returns the original length recorded in compressed data. The return type is determined by the function semantics and the input types.

## Examples

```sql
SELECT UNCOMPRESSED_LENGTH(COMPRESS('mydb'));
```

Result: 4

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
