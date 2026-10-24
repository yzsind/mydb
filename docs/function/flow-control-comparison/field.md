# `FIELD`

## Description

Returns the position of the first argument within the following argument list.

## Syntax

```sql
FIELD(value, value1 [, value2 [, ...]])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | Yes | Numeric or integer | The 1st argument; the input value |
| `value1` | Yes | SQL expression matching the function semantics | The 2nd argument; the meaning is determined by the function syntax and description |
| `value2 ...` | No, repeatable | SQL expression matching the function semantics | The 3rd argument; the meaning is determined by the function syntax and description |

## Return Value

Returns a one-based integer position, or 0 if no candidate matches. NULL search values return 0.

## Examples

```sql
SELECT FIELD(2, 2);
```

Result:

```text
1
```

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
