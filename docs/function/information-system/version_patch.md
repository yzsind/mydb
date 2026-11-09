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

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`VERSION`](version.md), [`VERSION_MAJOR`](version_major.md), [`VERSION_MINOR`](version_minor.md), [`ICU_VERSION`](icu_version.md).
