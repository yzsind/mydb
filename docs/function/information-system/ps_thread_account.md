# `PS_THREAD_ACCOUNT`

## Description

Returns the current authorization account through the compatibility helper.

## Syntax

```sql
PS_THREAD_ACCOUNT(thread_id)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `thread_id` | Yes | Integer | Live target connection identity; not an optional current-session default. |

## Return Value

Returns the authorization account text for the supplied live connection. SQL NULL, an unknown ID or an out-of-range ID returns SQL NULL. Exactly one argument is required; another connection can have a different account from CURRENT_USER().

## Examples

```sql
SELECT PS_THREAD_ACCOUNT(PS_CURRENT_THREAD_ID());
```

Result: For the loopback bootstrap account, returns `root@localhost`. The result follows CURRENT_USER(), not a lookup of the supplied thread argument.

## Notes

- This performs a connection-registry lookup. It does not ignore the argument or accept extra arbitrary expressions.

## Related Functions

[`CONNECTION_ID`](connection_id.md), [`PS_CURRENT_THREAD_ID`](ps_current_thread_id.md), [`PS_THREAD_ID`](ps_thread_id.md).
