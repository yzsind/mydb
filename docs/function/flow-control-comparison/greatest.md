# `GREATEST`

## Description

Returns the greatest value in the argument list.

## Syntax

```sql
GREATEST(value1, value2 [, ...])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value1` | Yes | Comparable expression | The first candidate value |
| `value2` | Yes, repeatable | Comparable expression | The remaining candidate values |

## Return Value

Returns the greatest candidate using the common comparison domain and collation. Any SQL NULL candidate returns NULL; the result type is derived from the candidate domains.

## Examples

```sql
SELECT GREATEST(2, 2);
```

Result:

```text
2
```

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
