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

## Transaction and Error Details

A missing savepoint returns 1305. Releasing the named savepoint leaves later savepoints usable and does not release row locks. Use BEGIN/SAVEPOINT/RELEASE/COMMIT in one connection; a name in another connection does not identify this session's savepoint.

## Examples

```sql
RELEASE SAVEPOINT after_debit;
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
