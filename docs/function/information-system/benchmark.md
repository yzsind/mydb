# `BENCHMARK`

## Description

Repeatedly evaluates an expression a specified number of times and returns 0; used for microbenchmarks.

## Syntax

```sql
BENCHMARK(count, expr)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `count` | Yes | Integer | Repetition count. |
| `expr` | Yes | Any SQL expression | Expression to evaluate. |

## Return Value

Returns integer 0 after count evaluations of expr. A SQL NULL count returns NULL; a negative count returns NULL with warning 1411. A zero count performs no expr evaluations. Expression errors and cancellation propagate.

## Examples

```sql
SELECT BENCHMARK(10, ABS(-1));
```

Result: 0

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`SLEEP`](sleep.md), [`LOAD_FILE`](load_file.md), [`MYSQL_QUERY_ATTRIBUTE_STRING`](mysql_query_attribute_string.md).
