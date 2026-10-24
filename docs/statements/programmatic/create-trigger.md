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
