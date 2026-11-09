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

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`VERSION`](version.md), [`VERSION_MAJOR`](version_major.md), [`VERSION_PATCH`](version_patch.md), [`ICU_VERSION`](icu_version.md).
