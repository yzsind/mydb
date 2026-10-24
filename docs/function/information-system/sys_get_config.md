# `SYS_GET_CONFIG`

## Description

Reads a value from the sys schema configuration, returning a default value when it does not exist.

## Syntax

```sql
SYS_GET_CONFIG(variable, default)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `variable` | Yes | String | The 1st argument; the name of the sys configuration option |
| `default` | Yes | An SQL expression matching the function semantics | The 2nd argument; the value to return when the configuration does not exist |

## Return Value

Reads a value from the sys schema configuration, returning a default value when it does not exist. The return type is determined by the function semantics and the input type.

## Examples

```sql
SELECT SYS_GET_CONFIG('statement_truncate_len', '64');
```

Result: returns the configured value; returns the default value 64 when it does not exist.

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
