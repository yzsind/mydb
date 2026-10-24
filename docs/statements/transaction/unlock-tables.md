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

## Examples

```sql
UNLOCK TABLES;
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
