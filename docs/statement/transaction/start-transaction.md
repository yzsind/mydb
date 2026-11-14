# `START TRANSACTION / BEGIN`

## Description

Starts an explicit transaction.

## Syntax

```ebnf
<start-transaction> ::= 'START TRANSACTION'
                        [ <transaction-characteristic>
                          { ',' <transaction-characteristic> } ]
                        | 'BEGIN' [ 'WORK' ]
<transaction-characteristic> ::= 'WITH CONSISTENT SNAPSHOT'
                                 | 'READ ONLY' | 'READ WRITE'
```

## Behavior and Limitations

- When the access mode is omitted, the session default applies. READ ONLY rejects persistent-table writes and FOR UPDATE; FOR SHARE is allowed. Session temporary-table writes retain their separate rules; see [transaction semantics](../../mvcc-and-locking.md).
- WITH CONSISTENT SNAPSHOT has consistent-snapshot semantics only under REPEATABLE READ; other isolation levels produce a warning and use their ordinary read-view rules.
- Starting a new explicit transaction commits an already active ordinary transaction first; it does not create a nested transaction. Inside a stored-program definition, BEGIN ... END instead denotes a compound block.

## Transaction and Error Details

BEGIN does not by itself pin an RR read view. WITH CONSISTENT SNAPSHOT pins it at transaction start only under RR. READ ONLY is an access mode, not a different isolation level. Persistent writes and FOR UPDATE are rejected in a read-only transaction; FOR SHARE is allowed. Plain RC reads still refresh snapshots per statement.

A successful new BEGIN/START TRANSACTION releases an earlier explicit table-lock region. There is no nested ordinary transaction stack. Use a savepoint for a rollback boundary inside one transaction.

## Examples

```sql
START TRANSACTION READ WRITE;
START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY;
BEGIN WORK;
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
