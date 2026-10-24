# `LOCK TABLES`

## Description

Declares explicit table locks and access modes for the current session.

## Syntax

```ebnf
<lock-tables> ::= 'LOCK TABLES' <lock-spec> { ',' <lock-spec> }
<lock-spec> ::= <qualified-name> [ [ 'AS' ] <identifier> ]
                ( 'READ' [ 'LOCAL' ]
                | [ 'LOW_PRIORITY' ] 'WRITE' )
```

## Behavior and Limitations

- Each declared alias serves as the accessible name while the lock is held; access to objects that are not locked is rejected according to the explicit lock rules.
- READ permits reads; WRITE permits writes from this session and excludes conflicting access.
- The boundary between explicit LOCK TABLES and transactions/autocommit follows MySQL-compatible behavior; locks are released automatically when the connection drops.

## Examples

```sql
LOCK TABLES customer READ, orders AS o WRITE;
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
