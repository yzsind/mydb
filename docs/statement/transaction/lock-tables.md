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
- LOCK TABLES implicitly commits the existing transaction and replaces earlier explicit table locks. Locks are released by UNLOCK TABLES, a new explicit transaction boundary, or connection cleanup.

## Transaction and Error Details

Requires LOCK TABLES and SELECT on each declared table. READ maps to explicit metadata SHARED_NO_WRITE; WRITE maps to EXCLUSIVE. READ LOCAL does not introduce a separate nonconflicting MyISAM insert path in the native engine. LOW_PRIORITY WRITE is treated as WRITE and produces warning 1287.

While held, an undeclared table access returns 1100; writing under a READ lock returns 1099. Access must use the declared alias. Dependencies of views are locked too. Session temporary tables do not require persistent table-lock tickets. RENAME TABLE is rejected while explicit locks are active (1192).

## Examples

```sql
LOCK TABLES customer READ, orders AS o WRITE;
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
