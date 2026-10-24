# `PS_THREAD_ACCOUNT`

## Description

Returns the current authorization account through the compatibility helper.

## Syntax

```sql
PS_THREAD_ACCOUNT([thread_id [, ...]])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `thread_id` | No | Integer | The thread ID; defaults to the current thread when omitted |
| `args` | No, repeatable | Any expression | Additional arguments accepted and evaluated by the current compatibility entry point |

## Return Value

Returns CURRENT_USER() as text in a SQL session after evaluating any arguments. It does not select another account by the supplied thread ID.

## Examples

```sql
SELECT PS_THREAD_ACCOUNT(PS_CURRENT_THREAD_ID());
```

Result: For the loopback bootstrap account, returns `root@localhost`. The result follows CURRENT_USER(), not a lookup of the supplied thread argument.

## Notes

- Additional arguments are evaluated but are not validated as thread identities; the helper always uses the current session account.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
