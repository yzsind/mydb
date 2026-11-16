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

## Transaction and Error Details

Without a scope, changing characteristics during an active transaction returns 1568. SESSION defaults apply to subsequent transactions; GLOBAL requires SYSTEM_VARIABLES_ADMIN and affects subsequently created sessions. The command does not retroactively replace an active read view.

RU permits dirty reads. RC takes a fresh committed snapshot per statement. RR fixes a consistent snapshot on its first ordinary read and has no gap/next-key locks; current writes recheck after locking. SERIALIZABLE uses transaction-duration table S locks for multi-statement reads, blocking conflicting writes at table granularity. See [isolation matrix](../../mvcc-and-locking.md#snapshot-lifecycle-and-isolation-levels).

## Examples

```sql
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
SET GLOBAL TRANSACTION READ WRITE;
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
