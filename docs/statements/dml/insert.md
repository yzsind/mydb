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
