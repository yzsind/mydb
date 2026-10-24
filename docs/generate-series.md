# The `generate_series` Table Function

> `generate_series` is a PostgreSQL-style
> mydb extension; MySQL 8.0.46 has no corresponding table function.

## Syntax and Types

The function can be used only as a table function in `FROM`/`JOIN`:

```sql
SELECT * FROM generate_series(1, 5);
SELECT n FROM generate_series(1.0, 2.0, 0.5) AS s(n);
SELECT *
FROM generate_series(
  '2026-01-01', '2026-01-04', INTERVAL '1' DAY
) WITH ORDINALITY AS s(d, ord);
```

| Family | Parameters | Output |
| --- | --- | --- |
| Integer | `start, stop [, step]`; step defaults to 1 | `BIGINT` |
| Exact fixed-point | Any parameter is an exact DECIMAL; unified to a common scale | `DECIMAL(p,s)` |
| Temporal | DATETIME start/stop plus a required `INTERVAL` step | `DATETIME` |

Both boundaries, start and stop, are inclusive. A step of 0 returns error
1210; a step whose direction contradicts the boundaries returns the empty
set; a NULL in any parameter also returns the empty set. Integer overflow
stops at the boundary rather than panicking. Month/year intervals reuse
the month-end clamping semantics of date arithmetic.

`WITH ORDINALITY` appends a BIGINT column starting at 1 whose default
name is `ordinality`. The default value-column name is `generate_series`;
`AS relation(value [, ordinality])` can rename the relation and the
columns at the same time.

## Query Capabilities and Boundaries

`generate_series` may appear on
either side of an INNER/LEFT/CROSS JOIN and take part in WHERE, ORDER BY,
LIMIT, GROUP BY, aggregation, CTEs, and EXPLAIN. Integer, fixed-point,
and temporal series can all enter the common batch/native vector
pipeline with consistent SQL semantics.

Parameters are evaluated as constant expressions at plan/execution
binding time. Correlated columns and LATERAL parameters are not supported
yet, nor are SELECT-list set-returning functions; the scaled coefficient
of an exact fixed-point series must also fit into `i128`, and anything
beyond that range is explicitly rejected. None of these shapes silently
produce wrong results. There is currently no implicit truncation switch
such as `MYDB_GENERATE_SERIES_MAX_ROWS`, nor dedicated parallel
value-range sharding. The common batch path produces rows in streaming
batches and responds to query cancellation. `MYDB_VECTORIZED=off` keeps
the batch pipeline and chooses scalar expression/aggregate processing;
it does not select the legacy row executor. If a plan actually falls back
to that executor, the series rows are materialized. Use sensible
bounds and do not assume LIMIT is always pushed into the generator; verify
the actual plan and route. See [executor boundaries](executor.md#5-vectorization-boundaries).
