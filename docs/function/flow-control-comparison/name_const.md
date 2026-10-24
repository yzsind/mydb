# `NAME_CONST`

## Description

Returns a literal value after validating the literal name and value arguments.

## Syntax

```sql
NAME_CONST(name, value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `name` | Yes | String | The 1st argument; the meaning is determined by the function syntax and description |
| `value` | Yes | Literal scalar | The value to return; arbitrary expressions are rejected |

## Return Value

Returns the second argument with its derived type. Unlike MySQL, mydb does not currently use the first argument as the result column label; use an explicit AS alias when applications require that label.

## Examples

```sql
SELECT NAME_CONST('answer', 42) AS answer;
```

Result: the return value is 42 and the result column name is answer.

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
