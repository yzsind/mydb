# `XA ROLLBACK`

## Description

Rolls back an ended or prepared XA branch.

## Syntax

```ebnf
<xa-rollback> ::= 'XA ROLLBACK' <xid>
```

## Behavior and Limitations

- Undoes the branch's writes and removes the recovery record.
- An unknown XID, or a state still not permitting it, returns a dedicated XA error.

## Transaction and Error Details

An ended or recoverable prepared branch can be explicitly rolled back. Unknown XID returns XAER_NOTA (1397); invalid state returns XAER_RMFAIL (1399). A prepared branch survives a session disconnect until a recovery decision. This is not ordinary ROLLBACK and does not target another session by connection ID.

## Examples

```sql
XA ROLLBACK 'order-42';
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
