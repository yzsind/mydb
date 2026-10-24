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

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
