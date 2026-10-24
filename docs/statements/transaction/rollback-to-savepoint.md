# `ROLLBACK TO SAVEPOINT`

## Description

Undoes changes made after the specified savepoint while keeping the transaction active.

## Syntax

```ebnf
<rollback-to-savepoint> ::= 'ROLLBACK' 'TO' [ 'SAVEPOINT' ] <identifier>
```

## Behavior and Limitations

- The target must exist in the current transaction.
- After the undo, execution can continue and eventually COMMIT or ROLLBACK; the savepoint itself remains usable.
- Not all locks acquired since the transaction began are released; the exact lock lifecycle follows the MVCC/row-lock implementation.

## Examples

```sql
ROLLBACK TO SAVEPOINT after_debit;
ROLLBACK TO after_debit;
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
