# `PS_IS_CONSUMER_ENABLED`

## Description

Reports whether a named Performance Schema consumer is enabled.

## Syntax

```sql
PS_IS_CONSUMER_ENABLED(value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | Yes | String | Name in the corresponding Performance Schema setup table |

## Return Value

Returns the string YES when a case-insensitive name match in the current setup_consumers rows is enabled; otherwise NO. NULL input returns SQL NULL. Changing setup consumers can change the answer.

## Examples

```sql
SELECT PS_IS_CONSUMER_ENABLED('events_statements_current');
```

Result: With the default fresh-instance setup, returns the string `YES` for events_statements_current. Disabling that consumer changes the answer to `NO`.

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`PS_IS_ACCOUNT_ENABLED`](ps_is_account_enabled.md), [`PS_IS_THREAD_INSTRUMENTED`](ps_is_thread_instrumented.md), [`PS_IS_INSTRUMENT_DEFAULT_ENABLED`](ps_is_instrument_default_enabled.md), [`PS_IS_INSTRUMENT_DEFAULT_TIMED`](ps_is_instrument_default_timed.md).
