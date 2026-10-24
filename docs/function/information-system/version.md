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
- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
