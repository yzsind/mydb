# `XA COMMIT`

## Description

Commits a prepared XA branch, or commits an ended branch that was not prepared, using ONE PHASE.

## Syntax

```ebnf
<xa-commit> ::= 'XA COMMIT' <xid> [ 'ONE PHASE' ]
```

## Behavior and Limitations

- The normal form requires PREPARED; ONE PHASE requires a state consistent with one-phase commit.
- An unknown XID or mismatched state returns a dedicated XA error; the commit is never guessed.

## Examples

```sql
XA COMMIT 'order-42';
XA COMMIT 'single-branch' ONE PHASE;
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
