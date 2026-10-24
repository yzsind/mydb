# `SET TRANSACTION`

## Description

Sets the isolation level and read/write mode for the next transaction, the current session, or the global default.

## Syntax

```ebnf
<set-transaction> ::= 'SET' [ 'GLOBAL' | 'SESSION' ]
                      'TRANSACTION' <transaction-setting>
                      { ',' <transaction-setting> }
<transaction-setting> ::= 'ISOLATION LEVEL'
                          ( 'READ UNCOMMITTED' | 'READ COMMITTED'
                          | 'REPEATABLE READ' | 'SERIALIZABLE' )
                          | 'READ ONLY' | 'READ WRITE'
```

## Behavior and Limitations

- SET TRANSACTION without a scope affects only the next transaction; SESSION changes the default for subsequent transactions, and GLOBAL affects new sessions.
- The default isolation level is READ COMMITTED. REPEATABLE READ is snapshot isolation and does not implement InnoDB gap/next-key locks; SERIALIZABLE currently uses table-level shared locks to constrain range writes.
- Characteristics that the current transaction does not allow changing cannot be modified after the transaction has started; the corresponding MySQL error is returned.

## Examples

```sql
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
SET GLOBAL TRANSACTION READ WRITE;
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
