# `PS_CURRENT_THREAD_ID`

## Description

Returns the current connection identity used by the compatibility helper.

## Syntax

```sql
PS_CURRENT_THREAD_ID()
```

## Parameters

This function takes no arguments.

## Return Value

Returns an unsigned integer equal to CONNECTION_ID() in the current SQL session. Its value changes between connections; it is not a server-wide constant.

## Examples

```sql
SELECT PS_CURRENT_THREAD_ID();
```

Result: Returns the current connection identity as an unsigned integer, for example `42`. The value changes on each new connection and equals CONNECTION_ID() here.

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
