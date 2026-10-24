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

- Single-table ORDER BY/LIMIT determines the order and cap of update candidates; UPDATE LIMIT does not support an offset.
- The multi-table form can join to filter rows and update the declared targets; RETURNING is supported only for execution forms that actually modify a single target table.
- Assignments honor column types, sql_mode, generated columns, CHECK, foreign keys, triggers, and unique key constraints.
- `IGNORE` downgrades only row-level errors that are permitted to become warnings.
- Returns affected rows; CLIENT_FOUND_ROWS affects whether matched rows or actually changed rows are reported.
- UPDATE acquires row locks and the necessary metadata locks; on failure, statement-level changes are rolled back, while committing or rolling back an explicit transaction remains the user's decision.

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

## Related Statements

- [SELECT](../query/select.md)
- [DELETE](delete.md)
- [EXPLAIN](../query/explain.md)
