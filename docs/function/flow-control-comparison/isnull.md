# `ISNULL`

## Description

Determines whether the argument is `NULL`.

## Syntax

```sql
ISNULL(value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | Yes | Numeric or integer | The 1st argument; the input value |

## Return Value

Returns integer 1 for SQL NULL and 0 otherwise.

## Examples

```sql
SELECT ISNULL(2);
```

Result:

```text
0
```

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
