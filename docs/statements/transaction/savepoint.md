# `SAVEPOINT`

## Description

Creates or replaces a named savepoint within the current transaction.

## Syntax

```ebnf
<savepoint> ::= 'SAVEPOINT' <identifier>
```

## Behavior and Limitations

- Must be inside an active transaction. Reusing a name moves the savepoint to the current position.
- A savepoint records an undo boundary; it neither commits the transaction nor releases previously held locks.

## Examples

```sql
START TRANSACTION;
UPDATE account SET balance = balance - 10 WHERE id = 1;
SAVEPOINT after_debit;
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
