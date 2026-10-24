# `PI`

## Description

Returns the value of π.

## Syntax

```sql
PI()
```

## Parameters

This function takes no arguments.

## Return Value

Returns the DOUBLE constant 3.141592653589793. The function takes no arguments; PI(NULL) is an argument-count error.

## Examples

```sql
SELECT PI();
```

Result:

```text
3.141592653589793
```

## Notes

- Numeric conversion, overflow, division by zero, and domain errors follow the current `sql_mode`; errors are never silently ignored.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
