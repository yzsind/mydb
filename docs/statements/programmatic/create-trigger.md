# `CREATE TRIGGER`

## Description

Creates a row-level BEFORE/AFTER INSERT, UPDATE, or DELETE trigger.

## Syntax

```ebnf
<create-trigger> ::= 'CREATE' [ 'DEFINER' [ '=' ] <account> ]
                     'TRIGGER' [ 'IF NOT EXISTS' ] <qualified-name>
                     ( 'BEFORE' | 'AFTER' )
                     ( 'INSERT' | 'UPDATE' | 'DELETE' )
                     'ON' <qualified-name> 'FOR EACH ROW'
                     [ ( 'FOLLOWS' | 'PRECEDES' ) <identifier> ]
                     <routine-statement>
```

## Behavior and Limitations

- The trigger body can reference OLD/NEW; writability depends on the timing and the event.
- FOLLOWS/PRECEDES establish a deterministic order within the same table, timing, and event.
- The table, definer, routine body, and recursion restrictions are validated at creation; DML and trigger effects belong to the same statement/transaction.

## Context, Errors, and Lifecycle

Requires TRIGGER on the subject table, a valid definer context and an implicit-commit DDL boundary. The trigger and business DML share statement rollback. The trigger cannot modify a table already used by the invoking statement in a forbidden self-write shape (1442), return a result set, commit a transaction or invoke dynamic SQL.

| Event/timing | OLD | NEW | SET NEW.column |
| --- | --- | --- | --- |
| BEFORE INSERT | Unavailable | Incoming row | Allowed on writable nongenerated fields |
| AFTER INSERT | Unavailable | Inserted row | Rejected |
| BEFORE UPDATE | Read-only previous row | Candidate row | Allowed on writable nongenerated fields |
| AFTER UPDATE | Read-only previous row | Updated row | Rejected |
| BEFORE/AFTER DELETE | Read-only deleted row | Unavailable | Rejected |

FOLLOWS/PRECEDES refers to another trigger of the same table/timing/event. Final NEW reflects coercion/generated/default/BEFORE changes; invalid OLD/NEW access is an error rather than a NULL substitute.

## Examples

```sql
DELIMITER //
CREATE TRIGGER orders_bi BEFORE INSERT ON orders FOR EACH ROW
BEGIN
  SET NEW.created_at = COALESCE(NEW.created_at, CURRENT_TIMESTAMP);
END//
DELIMITER ;
```

## See Also

- [Stored programs index](README.md)
- [Syntax conventions](../syntax-conventions.md)
