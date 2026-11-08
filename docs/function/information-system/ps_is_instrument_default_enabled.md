# `PS_IS_INSTRUMENT_DEFAULT_ENABLED`

## Description

Reports the enabled flag exposed for a named Performance Schema instrument.

## Syntax

```sql
PS_IS_INSTRUMENT_DEFAULT_ENABLED(value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | Yes | String | Name in the corresponding Performance Schema setup table |

## Return Value

Returns the string YES or NO from the current setup_instruments ENABLED flag, or NO for an unknown name; NULL input returns SQL NULL. Despite the function name, the implementation reads the current setup rows rather than an immutable historical default.

## Examples

```sql
SELECT PS_IS_INSTRUMENT_DEFAULT_ENABLED('statement/sql/select');
```

Result: With the default fresh-instance setup, returns the string `YES` for statement/sql/select. This helper reads the current setup_instruments flag; configuration changes can change the answer.

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`PS_IS_ACCOUNT_ENABLED`](ps_is_account_enabled.md), [`PS_IS_THREAD_INSTRUMENTED`](ps_is_thread_instrumented.md), [`PS_IS_CONSUMER_ENABLED`](ps_is_consumer_enabled.md), [`PS_IS_INSTRUMENT_DEFAULT_TIMED`](ps_is_instrument_default_timed.md).
