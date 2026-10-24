# `PS_THREAD_TRX_INFO`

## Description

Provides the current compatibility entry point for thread-transaction diagnostics.

## Syntax

```sql
PS_THREAD_TRX_INFO([thread_id [, ...]])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `thread_id` | No | Integer | The target thread ID |
| `args` | No, repeatable | Any expression | Additional arguments accepted and evaluated by the current compatibility entry point |

## Return Value

Evaluates its supplied arguments and returns SQL NULL. This helper does not retrieve transaction details; use the real transaction/lock system views for diagnosis.

## Examples

```sql
SELECT PS_THREAD_TRX_INFO(PS_CURRENT_THREAD_ID());
```

Result:

```text
NULL
```

## Notes

- This compatibility helper accepts and evaluates any number of arguments without looking up a transaction; argument-expression errors still propagate.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
