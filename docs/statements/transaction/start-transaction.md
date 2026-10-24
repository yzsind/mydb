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
- WITH CONSISTENT SNAPSHOT has consistent-snapshot semantics only under REPEATABLE READ; other isolation levels are diagnosed per implementation.
- Starting a transaction while one is already active follows MySQL boundaries and may end the current transaction first; do not use BEGIN to denote a stored program block.

## Examples

```sql
START TRANSACTION READ WRITE;
START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY;
BEGIN WORK;
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
