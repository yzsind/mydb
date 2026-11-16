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

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`VERSION`](version.md), [`VERSION_MINOR`](version_minor.md), [`VERSION_PATCH`](version_patch.md), [`ICU_VERSION`](icu_version.md).
