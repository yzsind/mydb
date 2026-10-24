# `RANDOM_BYTES`

## Description

Returns cryptographically random bytes of the specified length.

## Syntax

```sql
RANDOM_BYTES(len)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `len` | Yes | Integer | The 1st argument; the length, interpreted according to the function's character or byte semantics |

## Return Value

Returns cryptographically random bytes of the specified length. The return type is determined by the function semantics and the input types.

## Examples

```sql
SELECT LENGTH(RANDOM_BYTES(16));
```

Result: 16

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
