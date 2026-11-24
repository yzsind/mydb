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

- Joined forms support INNER/CROSS/LEFT/RIGHT chains. ON clauses retain their original source scope, and NULL-padded outer rows never become writable targets.
- Uncorrelated derived SELECTs, including column aliases, aggregates and set operations, are read-only join sources. Updating or deleting a derived result returns 1288; LATERAL DML sources remain explicitly unsupported.
- USING/NATURAL merged columns are available to WHERE and assignment reads using the preserving side of an outer join; write targets still resolve independently.

- Single-table ORDER BY/LIMIT deletes can run in batches; DELETE LIMIT does not accept an offset.
- A multi-table DELETE may declare one or more delete targets; RETURNING is not supported when more than one target is actually deleted.
- RETURNING observes OLD by default; NEW column values are SQL NULL because the deleted row has no new image.
- Foreign key actions, BEFORE/AFTER DELETE triggers, partitions, binlog, and transaction undo all take effect normally.
- Without WHERE, all visible rows are deleted, but constraints and triggers still execute row by row; consider TRUNCATE when a fast clear is needed and row-by-row semantics are not.

## Privileges, Results, and Boundaries

In joined forms, each delete target must resolve to a source table/alias; `alias.*` is a target spelling, not a projection. A qualifying row is deleted once even if multiple joined rows match. Single-table ORDER/LIMIT controls candidates; multi-target RETURNING is unsupported (1235).

DELETE requires DELETE on target plus SELECT for columns read by predicates/joins/RETURNING. NEW is NULL and OLD is the deleted row. FK-cascade rows are not RETURNING rows. A constraint, trigger or projection error restores the current statement, not earlier successful statements in an explicit transaction. See [execution contracts](../execution-contracts.md).

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
