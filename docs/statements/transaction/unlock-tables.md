# `UNLOCK TABLES`

## Description

Releases the current session's explicit table locks or the global read lock established by FLUSH TABLES WITH READ LOCK.

## Syntax

```ebnf
<unlock-tables> ::= 'UNLOCK TABLES'
```

## Behavior and Limitations

- Only locks owned by the current session are released; other sessions are unaffected.
- Returns success if there are no explicit locks. The binlog global read lock is validated by owner and internal scope.

## Transaction and Error Details

If explicit LOCK TABLES locks are held, UNLOCK TABLES commits the current transaction before releasing them. With only a FLUSH TABLES WITH READ LOCK fence, it releases the current connection's fence through the binlog administration path; this alone does not take the LOCK TABLES commit branch. With no explicit locks/fence it returns OK.

## Examples

```sql
UNLOCK TABLES;
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
