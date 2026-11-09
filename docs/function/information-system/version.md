# `VERSION`

## Description

Returns the server compatibility version string.

## Syntax

```sql
VERSION()
```

## Parameters

This function takes no arguments.

## Return Value

Returns a string containing the MySQL compatibility version, the mydb product version, and the compilation date, in the format
`8.0.46-mydb-<product version>-<YYYYMMDD>`. The compilation date is the build machine's local date, embedded at compile time.

## Examples

```sql
SELECT VERSION();
```

For example: `8.0.46-mydb-0.2.0-20260928`. `@@version` and the protocol handshake use the same string.

## Notes

- The version string of a given build artifact stays constant; it does not change with server startup or query date.
- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`VERSION_MAJOR`](version_major.md), [`VERSION_MINOR`](version_minor.md), [`VERSION_PATCH`](version_patch.md), [`ICU_VERSION`](icu_version.md).
