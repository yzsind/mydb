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
- Ordinary row, unique-key, and table locks acquired after the savepoint remain until transaction end. Later savepoints are removed; the target itself is retained. Transaction-local RLS context is restored to its savepoint value.

## Transaction and Error Details

A missing savepoint returns 1305. Statement rollback and user ROLLBACK TO are different: the latter deliberately retains ordinary locks acquired after the user savepoint. It cannot reverse an implicit commit caused by intervening DDL or FLUSH.

## Examples

```sql
ROLLBACK TO SAVEPOINT after_debit;
ROLLBACK TO after_debit;
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
