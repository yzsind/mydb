# `VERSION_PATCH`

## Description

Returns the patch version number of the version string.

## Syntax

```sql
VERSION_PATCH()
```

## Parameters

This function takes no arguments.

## Return Value

Returns an integer version component from the advertised MySQL compatibility version: major 8, minor 0, patch 46 in the current build.

## Examples

```sql
SELECT VERSION_PATCH();
```

Result:

```text
46
```

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
