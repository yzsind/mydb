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

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`VERSION`](version.md), [`VERSION_MAJOR`](version_major.md), [`VERSION_MINOR`](version_minor.md), [`VERSION_PATCH`](version_patch.md).
