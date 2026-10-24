# `XA PREPARE`

## Description

Persists an ended XA branch as prepared.

## Syntax

```ebnf
<xa-prepare> ::= 'XA PREPARE' <xid>
```

## Behavior and Limitations

- Only ENDED branches can be prepared; WAL/recovery records guarantee that the branch can be found by XA RECOVER after a crash.
- After prepare, XA COMMIT or XA ROLLBACK must follow; the end of an ordinary connection does not decide it automatically.

## Examples

```sql
XA PREPARE 'order-42';
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
