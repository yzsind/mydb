# `PS_THREAD_STACK`

## Description

Provides the current compatibility entry point for thread-stack diagnostics.

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

Evaluates its supplied arguments and returns SQL NULL. Thread-stack collection is not implemented by this helper; NULL is not proof that an identified thread has no stack.

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

This function has no direct alias in this category; see the category index for related functions.
