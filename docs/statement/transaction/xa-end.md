# `XA END`

## Description

Ends the active phase of an XA branch.

## Syntax

```ebnf
<xa-end> ::= 'XA END' <xid> [ 'SUSPEND' [ 'FOR MIGRATE' ] ]
```

## Behavior and Limitations

- The branch must be owned by this session and be in the ACTIVE state.
- The SUSPEND form is currently unsupported and returns a clear error.

## Transaction and Error Details

The XID must identify this session's ACTIVE branch; the statement moves it to ENDED. It does not commit, release branch locks or make PREPARED recovery records visible. SUSPEND/FOR MIGRATE returns 1235. Use [the complete XA flow](xa-start.md#complete-two-phase-flow), then PREPARE or ONE PHASE COMMIT.

## Examples

```sql
XA END 'order-42';
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
