# `SLEEP`

## Description

Pauses execution for the specified number of seconds.

## Syntax

```sql
SLEEP(seconds)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `seconds` | Yes | Numeric | Duration in seconds; may be fractional. NULL/negative/nonfinite values are invalid. |

## Return Value

Returns integer 0 when the requested sleep completes normally. Interruption follows the statement cancellation rules; duration is measured in seconds and may be fractional.

## Examples

```sql
SELECT SLEEP(2);
```

Result:

```text
0
```

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

- Invalid duration raises 1210 in strict SQL mode; otherwise it records a warning and returns 0. Normal completion returns 0. KILL and statement-deadline cancellation follow the SQL cancellation contract; do not rely on sleeping to release transactional locks.

## Related Functions

[`BENCHMARK`](benchmark.md), [`LOAD_FILE`](load_file.md), [`MYSQL_QUERY_ATTRIBUTE_STRING`](mysql_query_attribute_string.md).
