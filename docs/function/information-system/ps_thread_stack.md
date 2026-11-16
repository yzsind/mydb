# `PS_THREAD_STACK`

## Description

MySQL-compatible function surface for `sys` schema tooling; returns NULL because thread-stack collection is not implemented.

## Syntax

```sql
PS_THREAD_STACK([thread_id [, ...]])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `thread_id` | No | Integer | The target thread ID |
| `args` | No, repeatable | Any expression | Additional arguments accepted and evaluated by the current compatibility entry point |

## Return Value

Evaluates its supplied arguments and returns SQL NULL; thread-stack collection is not implemented. A NULL result does not indicate that an identified thread has no stack.

## Examples

```sql
SELECT PS_THREAD_STACK(PS_CURRENT_THREAD_ID());
```

Result:

```text
NULL
```

## Notes

- This compatibility helper accepts and evaluates any number of arguments without looking up a thread; argument-expression errors still propagate.

## Related Functions

[`PS_IS_ACCOUNT_ENABLED`](ps_is_account_enabled.md), [`PS_IS_THREAD_INSTRUMENTED`](ps_is_thread_instrumented.md), [`PS_IS_CONSUMER_ENABLED`](ps_is_consumer_enabled.md), [`PS_IS_INSTRUMENT_DEFAULT_ENABLED`](ps_is_instrument_default_enabled.md).
