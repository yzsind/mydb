# `INSERT`

## Description

Inserts rows into a table or an updatable view. Supports VALUES, SET, INSERT SELECT, multiple rows, IGNORE, partitions, ON DUPLICATE KEY UPDATE, target aliases, and PostgreSQL-style RETURNING.

## Syntax

```ebnf
<insert> ::= [ <with-clause> ] 'INSERT'
             [ 'LOW_PRIORITY' | 'DELAYED' | 'HIGH_PRIORITY' ]
             [ 'IGNORE' ] [ 'INTO' ] <qualified-name>
             [ 'PARTITION' '(' <identifier-list> ')' ]
             [ [ 'AS' ] <identifier> [ '(' <identifier-list> ')' ] ]
             ( [ '(' <identifier-list> ')' ] <insert-source>
             | 'SET' <assignment> { ',' <assignment> } )
             [ 'AS' <identifier> [ '(' <identifier-list> ')' ] ]
             [ 'ON DUPLICATE KEY UPDATE' <assignment>
               { ',' <assignment> } ]
             [ <returning-clause> ]
<insert-source> ::= 'VALUES' <row-value> { ',' <row-value> }
                    | 'VALUE' <row-value> { ',' <row-value> }
                    | <query-expression>
<row-value>     ::= '(' [ <expr> { ',' <expr> } ] ')'
<returning-clause> ::= 'RETURNING' [ <returning-aliases> ]
                       <select-item> { ',' <select-item> }
<returning-aliases> ::= 'WITH' '('
                        'OLD' 'AS' <identifier> ','
                        'NEW' 'AS' <identifier> ')'
```

## Behavior and Limitations

- Columns that are not listed use explicit DEFAULT, column defaults, generated column expressions, or implicit defaults; strict-mode violations are reported as errors or warnings through the unified DML diagnostics.
- `IGNORE` converts degradable row errors into warnings; it does not swallow structural errors, privilege errors, or hard memory-limit errors.
- `ON DUPLICATE KEY UPDATE` updates rows that conflict on the primary key or a unique key; affected rows and the OK packet info are reported per MySQL rules.
- `DELAYED` is accepted only for compatibility and produces a warning; no asynchronous insert queue is created.
- RETURNING observes NEW by default; explicit OLD/NEW can compare values before and after a conflict update. Forms that actually modify multiple target tables return 1235.
- Writes are subject to transactions, foreign keys, CHECK, triggers, generated columns, partition routing, binlog, and unified memory governance.

## Privileges, Results, and Boundaries

Requires INSERT on target, source SELECT for INSERT SELECT or expression subqueries, UPDATE for a conflict-update clause, and SELECT for read target columns/RETURNING. LOW_PRIORITY and HIGH_PRIORITY are accepted priority compatibility forms; they do not select a different native insert queue. DELAYED emits warning 1287.

The alias after the table names the target. `VALUES (...) AS incoming(c1,c2)` names the incoming row and its columns for conflict updates; its column-alias count must match. Omitted columns use defaults; an empty VALUES row requests defaults for all columns. Explicit generated-column values are restricted to DEFAULT.

ODKU reports 1 for a new row, 2 for a changed conflict row and 0 for a no-op (CLIENT_FOUND_ROWS makes that no-op 1). Auto-increment/last-insert state and warnings remain separate. Prefer an incoming-row alias to legacy VALUES(column):

```sql
INSERT INTO counter(id,value) VALUES(1,5) AS incoming
ON DUPLICATE KEY UPDATE value=incoming.value
RETURNING OLD.value,NEW.value;
```

See [RETURNING row images and rollback](../../dml-returning.md) and [statement failures](../execution-contracts.md#statement-failure-and-locking).

## Examples

```sql
INSERT INTO customer (id, name) VALUES (1, 'Ada');

INSERT INTO customer (id, name)
VALUES (1, 'Ada'), (2, 'Lin')
ON DUPLICATE KEY UPDATE name = VALUES(name);

INSERT INTO archive_order (id, total)
SELECT id, total FROM orders WHERE created_at < '2025-01-01';

INSERT INTO counter (id, value) VALUES (1, 1)
ON DUPLICATE KEY UPDATE value = value + 1
RETURNING OLD.value, NEW.value;
```

## Related Statements

- [REPLACE](replace.md)
- [SELECT](../query/select.md)
- [LOAD DATA](load-data.md)
