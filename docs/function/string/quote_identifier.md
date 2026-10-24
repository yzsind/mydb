# `QUOTE_IDENTIFIER`

## Description

Quotes and escapes an SQL identifier.

## Syntax

```sql
QUOTE_IDENTIFIER(identifier)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `identifier` | Yes | String | The 1st argument; the meaning is determined by the function syntax and description |

## Return Value

Returns a string, binary string, or integer; returns `NULL` when a required argument is `NULL`, except where noted otherwise.

## Examples

```sql
SELECT QUOTE_IDENTIFIER('a`b');
```

Result:

```text
`a``b`
```

## Notes

- Character counts differ from byte counts; binary strings are processed by bytes, while text strings follow charset and collation semantics.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
