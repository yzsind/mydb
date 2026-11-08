# `PS_IS_ACCOUNT_ENABLED`

## Description

Returns the current account-instrumentation compatibility response.

## Syntax

```sql
PS_IS_ACCOUNT_ENABLED(user [, host [, ...]])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `user` | Yes | Any SQL expression | Account name expression; this compatibility helper currently does not inspect its value. |
| `host ...` | No, repeatable | Any SQL expression | Additional evaluated expressions; this compatibility helper does not validate an account. |

## Return Value

Returns the string `YES` after evaluating its arguments. The current helper does not look up the supplied account in setup_actors and does not prove that a named account exists or is instrumented.

## Examples

```sql
SELECT PS_IS_ACCOUNT_ENABLED('root', 'localhost');
```

Result:

```text
YES
```

## Notes

- At least one argument is required. All supplied expressions are evaluated, but their values are not validated as existing accounts or instrumentation settings.

## Related Functions

[`PS_IS_THREAD_INSTRUMENTED`](ps_is_thread_instrumented.md), [`PS_IS_CONSUMER_ENABLED`](ps_is_consumer_enabled.md), [`PS_IS_INSTRUMENT_DEFAULT_ENABLED`](ps_is_instrument_default_enabled.md), [`PS_IS_INSTRUMENT_DEFAULT_TIMED`](ps_is_instrument_default_timed.md).
