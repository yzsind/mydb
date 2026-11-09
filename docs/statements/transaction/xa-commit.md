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

## Transaction and Error Details

Plain XA COMMIT requires PREPARED. ONE PHASE commits the session's ended, unprepared branch. A successful decision removes its recovery record and releases branch locks; commit visibility/durability uses the configured WAL/binlog path. Unknown XID returns XAER_NOTA (1397); invalid state returns XAER_RMFAIL (1399). See [XA identifiers and state flow](xa-start.md#complete-two-phase-flow).

## Examples

```sql
XA COMMIT 'order-42';
XA COMMIT 'single-branch' ONE PHASE;
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
