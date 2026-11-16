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
| `name` | Yes | String | Literal name string; use AS for the output column label in mydb. |
| `value` | Yes | Literal scalar | The value to return; arbitrary expressions are rejected |

## Return Value

Returns the second argument with its derived type. Unlike MySQL, mydb does not currently use the first argument as the result column label; use an explicit AS alias when applications require that label.

## Examples

```sql
SELECT NAME_CONST('answer', 42) AS answer;
```

Result: the return value is 42 and the result column name is answer.

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`IF`](if.md).
