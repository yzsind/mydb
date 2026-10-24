# `XA RECOVER`

## Description

Lists prepared XA branches that are currently recoverable.

## Syntax

```ebnf
<xa-recover> ::= 'XA RECOVER' [ 'CONVERT XID' ]
```

## Behavior and Limitations

- Returns formatID, gtrid_length, bqual_length, and data.
- CONVERT XID requests a readable hexadecimal representation; no branch is modified.
- Usually requires XA_RECOVER_ADMIN or an equivalent administrative privilege.

## Examples

```sql
XA RECOVER;
XA RECOVER CONVERT XID;
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
