# `COALESCE`

## Description

Returns the first non-`NULL` value in the argument list.

## Syntax

```sql
COALESCE(expr1 [, expr2 [, ...]])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr1` | Yes | Any SQL expression | The 1st argument; the meaning is determined by the function syntax and description |
| `expr2 ...` | No, repeatable | Any SQL expression | The 2nd argument; the meaning is determined by the function syntax and description |

## Return Value

Returns the first non-NULL argument; all-NULL input returns NULL. The declared result type combines the argument domains; the example returns text.

## Examples

```sql
SELECT COALESCE('example');
```

Result:

```text
example
```

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
