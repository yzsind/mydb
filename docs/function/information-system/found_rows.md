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
- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
