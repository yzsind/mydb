# `COMMIT`

## Description

Commits the current transaction, making writes durable and releasing transaction locks.

## Syntax

```ebnf
<commit> ::= 'COMMIT' [ 'WORK' ]
             [ 'AND' [ 'NO' ] 'CHAIN' ]
             [ [ 'NO' ] 'RELEASE' ]
```

## Behavior and Limitations

- Returns success when there is no active transaction, with no data changes.
- AND CHAIN starts a new transaction with the same characteristics after the commit; AND NO CHAIN overrides completion_type.
- RELEASE requests that the connection be closed after the commit; NO RELEASE explicitly keeps the connection open.
- Durability follows the MYDB_DURABILITY, WAL, and group commit configuration, but never goes beyond the declared consistency semantics.

## Examples

```sql
COMMIT;
COMMIT AND CHAIN;
COMMIT WORK AND NO CHAIN NO RELEASE;
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
