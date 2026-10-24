# `PS_THREAD_ID`

## Description

Returns a supplied connection identity through the compatibility helper.

## Syntax

```sql
PS_THREAD_ID([connection_id])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `connection_id` | No | Integer | The 1st argument; the meaning is determined by the function syntax and description |

## Return Value

With no argument, returns the current connection ID. With one non-NULL argument, converts it numerically to an unsigned integer; NULL returns NULL. This is an identity/conversion helper, not an arbitrary live-thread registry lookup.

## Examples

```sql
SELECT PS_THREAD_ID(CONNECTION_ID()) = PS_CURRENT_THREAD_ID();
```

Result:

```text
1
```

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
