# `DELETE`

## Description

Deletes rows that match the condition. Supports single-table deletes, MySQL multi-table DELETE, CTEs, ORDER BY, LIMIT, partitions, and RETURNING.

## Syntax

```ebnf
<delete> ::= [ <with-clause> ]
             ( 'DELETE' [ <delete-modifier> { <delete-modifier> } ]
               'FROM' <qualified-name>
               [ 'PARTITION' '(' <identifier-list> ')' ]
               [ 'WHERE' <expr> ]
               [ 'ORDER BY' <order-item> { ',' <order-item> } ]
               [ 'LIMIT' <row-count> ]
             | 'DELETE' <delete-target> { ',' <delete-target> }
               'FROM' <table-reference-list> [ 'WHERE' <expr> ]
             | 'DELETE' 'FROM' <delete-target> { ',' <delete-target> }
               'USING' <table-reference-list> [ 'WHERE' <expr> ] )
             [ <returning-clause> ]
<delete-modifier> ::= 'LOW_PRIORITY' | 'QUICK' | 'IGNORE'
```

## Behavior and Limitations

- Single-table ORDER BY/LIMIT deletes can run in batches; DELETE LIMIT does not accept an offset.
- A multi-table DELETE may declare one or more delete targets; RETURNING is not supported when more than one target is actually deleted.
- RETURNING observes OLD by default; NEW is empty under delete semantics or is handled per expression rules.
- Foreign key actions, BEFORE/AFTER DELETE triggers, partitions, binlog, and transaction undo all take effect normally.
- Without WHERE, all visible rows are deleted, but constraints and triggers still execute row by row; consider TRUNCATE when a fast clear is needed and row-by-row semantics are not.

## Examples

```sql
DELETE FROM session_token WHERE expires_at < NOW() LIMIT 1000;

DELETE o
FROM orders AS o
JOIN customer AS c ON c.id = o.customer_id
WHERE c.disabled = 1;

DELETE FROM audit_log
WHERE created_at < '2025-01-01'
RETURNING OLD.id, OLD.created_at;
```

## Related Statements

- [TRUNCATE TABLE](../ddl/truncate-table.md)
- [UPDATE](update.md)
