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

## Transaction and Error Details

PREPARE moves ENDED to PREPARED and persists its recovery record. It retains row, unique-key, table and transaction-duration metadata locks until explicit XA decision, including after crash recovery. A disconnect does not decide a prepared branch. Recovery durability depends on the configured persistent WAL mode; memory durability is not crash recovery.

Use XA RECOVER from a privileged recovery connection and explicitly COMMIT or ROLLBACK each surviving branch. A live branch cannot execute implicit-commit DDL.

## Examples

```sql
XA PREPARE 'order-42';
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
