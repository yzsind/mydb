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

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
