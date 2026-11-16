# `FOUND_ROWS`

## Description

Returns the preceding SELECT row count tracked by the session.

## Syntax

```sql
FOUND_ROWS()
```

## Parameters

This function takes no arguments.

## Return Value

Returns an integer. After SELECT SQL_CALC_FOUND_ROWS ... LIMIT, it is the count before LIMIT; without SQL_CALC_FOUND_ROWS it follows the preceding query's delivered count. Subsequent statements can replace the session state.

## Examples

```sql
SELECT SQL_CALC_FOUND_ROWS v FROM (SELECT 1 v UNION ALL SELECT 2) AS t LIMIT 1;
SELECT FOUND_ROWS();
```

Result:

```text
1
2
```

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

- SQL_CALC_FOUND_ROWS measures rows before LIMIT; an ordinary SELECT tracks delivered rows. Read the value before another statement overwrites session state; it is not an independent snapshot or a replacement for an explicit COUNT query.

## Related Functions

[`ROW_COUNT`](row_count.md), [`LAST_INSERT_ID`](last_insert_id.md).
