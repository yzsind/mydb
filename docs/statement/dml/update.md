# `UPDATE`

## Description

Modifies rows that match the condition. Supports single-table and joined/multi-table targets, CTEs, IGNORE, ORDER BY, LIMIT, partitions, and RETURNING.

## Syntax

```ebnf
<update> ::= [ <with-clause> ] 'UPDATE'
             [ 'LOW_PRIORITY' ] [ 'IGNORE' ]
             <table-reference-list>
             'SET' <assignment> { ',' <assignment> }
             [ 'WHERE' <expr> ]
             [ 'ORDER BY' <order-item> { ',' <order-item> } ]
             [ 'LIMIT' <row-count> ]
             [ <returning-clause> ]
```

## Behavior and Limitations

- Joined forms support INNER/CROSS/LEFT/RIGHT chains. ON clauses retain their original source scope, and NULL-padded outer rows never become writable targets.
- Uncorrelated derived SELECTs, including column aliases, aggregates and set operations, are read-only join sources. Updating or deleting a derived result returns 1288; LATERAL DML sources remain explicitly unsupported.
- USING/NATURAL merged columns are available to WHERE and assignment reads using the preserving side of an outer join; write targets still resolve independently.

- Single-table ORDER BY/LIMIT determines the order and cap of update candidates; UPDATE LIMIT does not support an offset.
- The multi-table form can join to filter rows and update the declared targets; RETURNING is supported only for execution forms that actually modify a single target table.
- Assignments honor column types, sql_mode, generated columns, CHECK, foreign keys, triggers, and unique key constraints.
- `IGNORE` downgrades only row-level errors that are permitted to become warnings.
- Returns affected rows; CLIENT_FOUND_ROWS affects whether matched rows or actually changed rows are reported.
- UPDATE acquires row locks and the necessary metadata locks; on failure, statement-level changes are rolled back, while committing or rolling back an explicit transaction remains the user's decision.

## Privileges, Results, and Boundaries

Assignments can use a target alias qualifier, such as c.value=c.value+1. Single-table assignments are evaluated with the implemented MySQL left-to-right semantics; do not rely on a portable assignment order for multiple targets. A joined target row is updated once even if it matches multiple joined source rows. ORDER BY/LIMIT must be valid for the chosen execution shape; a LIMIT offset is rejected.

UPDATE needs UPDATE on changed columns and SELECT on columns read by expressions/predicates. RETURNING additionally needs SELECT. It exposes OLD and final NEW, including no-op logical updates; multi-target writes with RETURNING return 1235. [DML RETURNING](../../dml-returning.md) describes projections and error rollback.

## Examples

```sql
UPDATE account SET balance = balance - 10 WHERE id = 1;

UPDATE jobs
SET state = 'expired'
WHERE state = 'ready'
ORDER BY created_at
LIMIT 100;

WITH delta AS (SELECT 1 AS id, 5 AS amount)
UPDATE counter AS c JOIN delta AS d ON d.id = c.id
SET c.value = c.value + d.amount
RETURNING OLD.value, NEW.value;
```

## Complete derived-source example

This example uses disposable tables. The derived result is read-only; the LEFT
JOIN preserves accounts without entries and COALESCE supplies their zero total.

```sql
DROP TABLE IF EXISTS test.dml_accounts;
DROP TABLE IF EXISTS test.dml_entries;
CREATE TABLE test.dml_accounts(id INT PRIMARY KEY, total INT);
CREATE TABLE test.dml_entries(account_id INT, amount INT);
INSERT INTO test.dml_accounts VALUES (1, 0), (2, 0);
INSERT INTO test.dml_entries VALUES (1, 10), (1, 5), (3, 7);

UPDATE test.dml_accounts AS a
LEFT JOIN (
    SELECT account_id, SUM(amount) AS amount
    FROM test.dml_entries GROUP BY account_id
) AS totals ON totals.account_id = a.id
SET a.total = COALESCE(totals.amount, 0);
SELECT id, total FROM test.dml_accounts ORDER BY id;
```

The result is `(1, 15)` and `(2, 0)`. No account is inserted for the unmatched
entry with `account_id=3`. Use INSERT or an explicit upsert for that behavior.

## Related Statements

- [SELECT](../query/select.md)
- [DELETE](delete.md)
- [EXPLAIN](../query/explain.md)
