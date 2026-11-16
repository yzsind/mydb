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
- Requires global XA_RECOVER_ADMIN.

## Transaction and Error Details

Requires global XA_RECOVER_ADMIN. Returns `formatID`, `gtrid_length`, `bqual_length` (integer metadata), and `data` (string). Lengths are bytes, not character counts. Plain data is concatenated gtrid+bqual when printable ASCII; otherwise it is 0x-prefixed uppercase hex. CONVERT XID always uses hex. Empty output means there are no prepared recoverable branches; ACTIVE/ENDED branches are not a prepared recovery list.

## Examples

```sql
XA RECOVER;
XA RECOVER CONVERT XID;
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
