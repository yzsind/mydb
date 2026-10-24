# `IF`

## Description

Returns one of two candidate values depending on a condition.

## Syntax

```sql
IF(condition, value_if_true, value_if_false)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `condition` | Yes | SQL expression matching the function semantics | The 1st argument; determines which branch is returned |
| `value_if_true` | Yes | Any SQL expression | The 2nd argument; returned when the condition is true |
| `value_if_false` | Yes | Any SQL expression | The 3rd argument; returned when the condition is false |

## Return Value

Returns the true branch when the condition is true, otherwise the false branch; NULL conditions select the false branch. The result domain combines the two branches, and only the selected result branch is evaluated.

## Examples

```sql
SELECT IF('example', 2, 2);
```

Result:

```text
2
```

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
