# `VERSION_MAJOR`

## Description

Returns the major version number of the version string.

## Syntax

```sql
VERSION_MAJOR()
```

## Parameters

This function takes no arguments.

## Return Value

Returns an integer version component from the advertised MySQL compatibility version: major 8, minor 0, patch 46 in the current build.

## Examples

```sql
SELECT VERSION_MAJOR();
```

Result:

```text
8
```

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
