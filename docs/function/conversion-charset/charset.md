# `CHARSET`

## Description

Returns the name of the charset used by a string.

## Syntax

```sql
CHARSET(value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | Yes | Numeric or integer | The 1st argument; the input value |

## Return Value

Returns a charset name as text; numeric input has charset `binary`.

## Examples

```sql
SELECT CHARSET(2);
```

Result:

```text
binary
```

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
