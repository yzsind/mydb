# `VERSION_MINOR`

## Description

Returns the minor version number of the version string.

## Syntax

```sql
VERSION_MINOR()
```

## Parameters

This function takes no arguments.

## Return Value

Returns an integer version component from the advertised MySQL compatibility version: major 8, minor 0, patch 46 in the current build.

## Examples

```sql
SELECT VERSION_MINOR();
```

Result:

```text
0
```

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
