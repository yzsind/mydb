# `PS_IS_THREAD_INSTRUMENTED`

## Description

Returns the current thread-instrumentation compatibility response.

## Syntax

```sql
PS_IS_THREAD_INSTRUMENTED(thread_id [, args [, ...]])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `thread_id` | Yes | Integer | The target thread ID |
| `args` | No, repeatable | Any expression | Additional arguments accepted and evaluated by the current compatibility entry point |

## Return Value

Returns the string `YES` after evaluating its arguments. The current helper does not validate that the target thread exists or inspect its instrumentation flag.

## Examples

```sql
SELECT PS_IS_THREAD_INSTRUMENTED(PS_CURRENT_THREAD_ID());
```

Result:

```text
YES
```

## Notes

- At least one argument is required. All supplied expressions are evaluated, but their values are not validated as live thread identities or instrumentation settings.

## Related Functions

[`PS_IS_ACCOUNT_ENABLED`](ps_is_account_enabled.md), [`PS_IS_CONSUMER_ENABLED`](ps_is_consumer_enabled.md), [`PS_IS_INSTRUMENT_DEFAULT_ENABLED`](ps_is_instrument_default_enabled.md), [`PS_IS_INSTRUMENT_DEFAULT_TIMED`](ps_is_instrument_default_timed.md).
