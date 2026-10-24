# `RELEASE SAVEPOINT`

## Description

Removes a named savepoint without undoing data.

## Syntax

```ebnf
<release-savepoint> ::= 'RELEASE SAVEPOINT' <identifier>
```

## Behavior and Limitations

- The target must exist in the current transaction.
- Releasing a savepoint does not commit, modify rows, or release transaction locks.

## Examples

```sql
RELEASE SAVEPOINT after_debit;
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
