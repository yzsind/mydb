# `UUID`

## Description

Generates a UUID in the standard text format.

## Syntax

```sql
UUID()
```

## Parameters

This function takes no arguments.

## Return Value

Generates a UUID in the standard text format. The return type is determined by the function semantics and the input type.

## Examples

```sql
SELECT UUID();
```

Result: returns a standard UUID text string; the result may differ on each call.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
