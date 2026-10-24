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
| `value` | Yes | Numeric or integer | The 1st argument; the input value |

## Return Value

Compresses the input string and returns binary data. The return type is determined by the function semantics and the input types.

## Examples

```sql
SELECT UNCOMPRESS(COMPRESS('mydb'));
```

Result: mydb

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
