# `ICU_VERSION`

## Description

Returns the pinned ICU compatibility version string.

## Syntax

```sql
ICU_VERSION()
```

## Parameters

This function takes no arguments.

## Return Value

Returns the string `77.1`, the pinned MySQL 8.0.46 compatibility value. It is not evidence that mydb links ICU or implements the full ICU regex/collation engine.

## Examples

```sql
SELECT ICU_VERSION();
```

Result:

```text
77.1
```

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
