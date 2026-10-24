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

## Related Statements

- [SELECT INTO variables](select-into-variables.md)
- [SELECT INTO file](select-into-file.md)
- [EXPLAIN](explain.md)
- CTEs and RETURNING in [INSERT](../dml/insert.md), [UPDATE](../dml/update.md), and [DELETE](../dml/delete.md)
