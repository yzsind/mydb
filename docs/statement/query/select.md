# `SELECT`

## Description

Reads tables, views, table functions, or tableless expressions and returns a result set. mydb supports CTEs/recursive CTEs, joins, subqueries, set operations, aggregation, windows, ROLLUP/CUBE/GROUPING SETS, ordering and paging, table-level time travel, and locking reads.

## Syntax

```ebnf
<select-statement> ::= [ <with-clause> ] <query-expression>
                       [ <order-by> ] [ <limit> ] { <locking-clause> }
<with-clause>      ::= 'WITH' [ 'RECURSIVE' ] <cte> { ',' <cte> }
<cte>              ::= <identifier> [ '(' <identifier-list> ')' ]
                       'AS' '(' <query-expression> ')'
<select-query>     ::= 'SELECT' [ <select-modifier> { <select-modifier> } ]
                       <select-list> [ 'FROM' <table-reference-list> ]
                       [ 'WHERE' <expr> ] [ <group-by> ] [ 'HAVING' <expr> ]
                       [ <window-clause> ]
<select-list>      ::= <select-item> { ',' <select-item> }
<select-item>      ::= '*' | <qualified-name> '.*'
                       | <expr> [ [ 'AS' ] <identifier> ]
<set-operator>     ::= ( 'UNION' | 'INTERSECT' | 'EXCEPT' )
                       [ 'ALL' | 'DISTINCT' ]
<limit>            ::= 'LIMIT' ( <row-count> [ 'OFFSET' <offset> ]
                       | <offset> ',' <row-count> )
<locking-clause>   ::= 'FOR' ( 'UPDATE' | 'SHARE' )
                       [ 'OF' <table-list> ] [ 'NOWAIT' | 'SKIP LOCKED' ]
```

Top-level `TABLE name` and `VALUES (...)` are executed as query expressions; the final `ORDER BY/LIMIT` of a set operation applies to the whole result.

## Behavior and Limitations

- `OF` names table aliases in the query block; multiple locking clauses may assign different modes and NOWAIT/SKIP LOCKED policies to different sources. Ambiguous, unknown or duplicate targets fail explicitly. NATURAL/USING, lateral and grouped locking-join shapes retain restrictions; see [parser capabilities](../../sql-parser-capabilities.md).
- An ordinary query is a read-only statement; `FOR UPDATE/FOR SHARE` enters the transaction lock path and requires the corresponding privileges.
- The same SQL, schema, statistics, hints, and session semantics produce a stable physical plan; live memory pressure only adjusts operator batch sizes or spill.
- `FOR SYSTEM_TIME AS OF expr` binds to each table; see the time-travel documentation for history retention and cross-DDL-epoch limitations.
- Row execution is the semantic baseline; `MYDB_VECTORIZED=on|auto` can select vectorized execution; both should return the same results.
- Unsupported expression forms or MySQL syntax return explicit errors; execution never continues after silently dropping a clause.

## Examples

```sql
SELECT 1 AS answer;

WITH RECURSIVE n(x) AS (
  SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 5
)
SELECT x FROM n ORDER BY x;

SELECT c.customer_id, SUM(o.total) AS amount
FROM customer AS c
JOIN orders AS o ON o.customer_id = c.customer_id
GROUP BY c.customer_id
HAVING SUM(o.total) > 100
ORDER BY amount DESC
LIMIT 10;

START TRANSACTION;
SELECT * FROM jobs WHERE state = 'ready' LIMIT 1 FOR UPDATE SKIP LOCKED;
COMMIT;
```

The first example returns one row; the recursive example returns 1 through 5; the locking read skips candidate rows already locked by other transactions.

## Query Clauses

| Construct | Syntax and effect |
| --- | --- |
| Projection | SELECT [ALL/DISTINCT/DISTINCTROW] expressions, wildcards and aliases; DISTINCTROW is a DISTINCT spelling |
| Sources | Base table/view/CTE, parenthesized derived query with alias/column aliases, supported table functions (including JSON_TABLE), and permitted LATERAL dependencies |
| Joins | Comma/CROSS/INNER and supported LEFT/RIGHT/FULL/NATURAL/USING forms; ON is a scalar condition. Locking joins have additional restrictions linked above |
| Filters/grouping | WHERE before grouping; GROUP BY expression lists, WITH ROLLUP, ROLLUP, CUBE and GROUPING SETS; HAVING filters groups |
| Windows | Function OVER(...) or named WINDOW clauses; PARTITION BY, ORDER BY and supported ROWS/RANGE frames. [Window boundaries](../../mysql80-compatibility.md#window-function-boundaries) lists rejected combinations |
| Set queries | UNION/INTERSECT/EXCEPT [ALL/DISTINCT]; INTERSECT binds more tightly. Parentheses retain their own ORDER/LIMIT scopes |
| CTEs | WITH [RECURSIVE] name[(columns)] AS (query); recursive execution is bounded and unsupported recursive shapes fail explicitly |
| TABLE/VALUES | TABLE table_name and VALUES ROW(...),ROW(...) are query sources, with ordinary outer ORDER/LIMIT |
| Order/limit | ORDER BY expression [ASC/DESC]; LIMIT count OFFSET offset or LIMIT offset,count. Without ORDER BY no delivery order is guaranteed |
| Historical source | table FOR SYSTEM_TIME AS OF expression; [history retention](../../mysql80-compatibility.md#time-travel-queries) and DDL epochs limit availability |
| Plan modifiers | STRAIGHT_JOIN preserves the query block's join order; [hints](../../hints.md) select supported optimizer controls |

`SQL_CALC_FOUND_ROWS` preserves the qualifying count for FOUND_ROWS() despite LIMIT. Result-size/priority/cache compatibility words do not imply a separate MySQL result cache or native scheduling engine; use EXPLAIN for the actual plan. All expression types use the [operator and function rules](../../operators-and-expressions.md). Unqualified objects resolve through the current schema with temporary-table shadowing where supported.

Source reads require SELECT; locking references also follow locking-read authorization and read-only rules. Locks are transaction-duration when the transaction remains open. NOWAIT returns 3572 on conflict; SKIP LOCKED excludes conflicting candidates rather than waiting. RLS filters remain effective before user expressions. See [MVCC and locking](../../mvcc-and-locking.md).

```sql
TABLE customer ORDER BY id LIMIT 2;
VALUES ROW(1,'Ada'),ROW(2,'Lin');
SELECT customer_id,SUM(total) FROM orders
GROUP BY ROLLUP(customer_id);
```

## Table Sampling

Ordinary user tables and partitioned tables accept sampling after the table alias:

```ebnf
<table-sample> ::= 'TABLESAMPLE' [ 'SYSTEM' | 'BERNOULLI' | 'BLOCK' | 'ROW' ]
                   '(' <amount> [ 'PERCENT' | 'ROWS' ] ')'
                   [ 'REPEATABLE' '(' <seed> ')' ]
```

`SYSTEM` is the default method and `PERCENT` is the default unit. `BLOCK` is an alias for `SYSTEM`; `ROW` is an alias for `BERNOULLI`.

| Method and unit | Behavior |
| --- | --- |
| `SYSTEM (p PERCENT)` | Selects each physical heap page with probability `p / 100`, returning its visible rows. |
| `BERNOULLI (p PERCENT)` | Selects each visible row independently with probability `p / 100`; streaming prefixes use block rounds. |
| `SYSTEM (n ROWS)` | Visits pages in a random order until `n` visible rows have been selected; the last page can be truncated. |
| `BERNOULLI (n ROWS)` | Visits randomly ordered blocks in rounds, selecting at most one new visible root row per block per round, until `n` rows have been selected. |

Both `ROWS` methods return the complete visible population when fewer than `n` rows exist. Percentage samples have variable sizes. Page sampling can exhibit clustering when neighboring rows have related values. Exact row sampling uses a seeded random row permutation within each block and never selects the same root row twice. Small row-count samples stop after visiting enough nonempty blocks. Larger samples reuse compact page inventories across rounds; inventories and selected identities can spill under memory pressure. Each round takes at most one row per block, so blocks with fewer visible rows give their individual rows a higher selection probability in small exact samples. This block-balanced `ROWS` extension is not a globally uniform row sample. Percentage membership remains independent per visible row; the percentage refers to rows, not blocks. Aggregations that consume a complete percentage sample can combine physical reads and reorder delivery without changing membership. Streaming LIMIT inputs retain block rounds.

Sampling happens before WHERE, joins, aggregation, ordering, and LIMIT. A WHERE condition can therefore leave fewer than `n` output rows. An explicit `PARTITION (...)` clause defines the sampling population; predicate-derived partition pruning does not reduce that population. Sampling follows the query's MVCC read view, including transaction-local writes and supported historical reads.

Amounts and seeds accept numeric literals, constant arithmetic, numeric CAST expressions, and prepared-statement `?` parameters. They are resolved for each execution. Percentages must be finite values in `[0,100]`; row counts must be nonnegative integers within the unsigned 64-bit range. Seeds must be finite, non-NULL numeric values. Strings require an explicit numeric CAST. Invalid arguments produce errors, including when LIMIT is zero.

`REPEATABLE` selects the same sample when the seed, arguments, visible data, physical layout, and execution method remain unchanged. Changing parallel degree or execution mode is outside this guarantee. Physical reorganization or data changes can change a sample. Omitted seeds produce fresh samples on each statement execution. Plan caching never freezes a generated seed or stores a sample. Use ORDER BY when output order matters.

The optimizer can use parallel sampling, including within a single partition. Existing `PARALLEL(n)` and `NO_PARALLEL` hints control eligibility. Workers share the read view, memory budget, and cancellation state. Small row goals can execute serially to avoid worker startup and excessive prefetch. EXPLAIN TREE, JSON, and traditional output identify the sampling method and arguments; parallel plans show their degree. EXPLAIN ANALYZE exposes `block_round_row` and `row_percent_scan` runtime counters, including root pages, root-header checks, rounds, decoded rows, actual sampling workers, directory memory, and spill bytes.

```sql
-- Approximately 1% of the table, selected by physical page.
SELECT * FROM events TABLESAMPLE (1);

-- Repeatable row sample, followed by an ordinary predicate.
SELECT * FROM events AS e
TABLESAMPLE BERNOULLI (100 ROWS) REPEATABLE (42)
WHERE e.amount > 0;

-- Parallel sampling within an explicitly selected partition.
SELECT /*+ PARALLEL(4) */ COUNT(*)
FROM events PARTITION (p0)
TABLESAMPLE SYSTEM (10 PERCENT) REPEATABLE (42);
```

Direct sampling of views, CTE references, derived tables, table functions, and system tables is rejected. Base tables inside those query bodies can use TABLESAMPLE. Sampling cannot qualify an UPDATE/DELETE target. Index hints on sampled sources and forced index/Zone Map access hints are rejected. Unsupported `SAMPLE`, `SEED`, bucket, and sampling-offset forms also produce explicit errors.

The method names and REPEATABLE clause follow SQL-standard sampling conventions. Exact `ROWS` sampling, method aliases, and omitted-method defaults are mydb extensions. MySQL 8.0 does not provide this TABLESAMPLE syntax.

## Related Statements

- [SELECT INTO variables](select-into-variables.md)
- [SELECT INTO file](select-into-file.md)
- [EXPLAIN](explain.md)
- CTEs and RETURNING in [INSERT](../dml/insert.md), [UPDATE](../dml/update.md), and [DELETE](../dml/delete.md)

See [JSON_TABLE](../../json-table.md) for full table-function syntax and examples.
