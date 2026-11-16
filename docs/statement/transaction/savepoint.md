# `SAVEPOINT`

## Description

Creates or replaces a named savepoint within the current transaction.

## Syntax

```ebnf
<savepoint> ::= 'SAVEPOINT' <identifier>
```

## Behavior and Limitations

- With autocommit enabled and no active explicit or implicit transaction, the statement returns OK without creating a savepoint; a later ROLLBACK TO or RELEASE then returns 1305. In a transaction, reusing a name moves the savepoint to the current position.
- A savepoint records an undo boundary; it neither commits the transaction nor releases previously held locks.

## Transaction and Error Details

With autocommit=0, a SAVEPOINT can start the session's implicit transaction lazily. Within a live transaction the name identifies a data-undo and RLS LOCAL context boundary. Savepoints disappear on full commit/rollback. They do not reverse successful temporary DDL or consumed sequence values.

## Examples

```sql
START TRANSACTION;
UPDATE account SET balance = balance - 10 WHERE id = 1;
SAVEPOINT after_debit;
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
