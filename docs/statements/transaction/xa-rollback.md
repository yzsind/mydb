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

## Examples

```sql
XA ROLLBACK 'order-42';
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
