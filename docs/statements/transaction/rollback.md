# `ROLLBACK`

## Description

Rolls back the current transaction, undoing uncommitted writes and releasing locks.

## Syntax

```ebnf
<rollback> ::= 'ROLLBACK' [ 'WORK' ]
               [ 'AND' [ 'NO' ] 'CHAIN' ]
               [ [ 'NO' ] 'RELEASE' ]
```

## Behavior and Limitations

- Without TO SAVEPOINT, the entire transaction is rolled back; see the separate document for the savepoint form.
- CHAIN/RELEASE behave the same as with COMMIT, but the new transaction starts after the undo.
- Non-transactional side effects such as sequence nextval, some DDL boundaries, and external files are not automatically reverted by a normal rollback.

## Examples

```sql
ROLLBACK;
ROLLBACK WORK AND CHAIN;
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
