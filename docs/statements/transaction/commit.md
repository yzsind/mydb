# `COMMIT`

## Description

Commits the current transaction, publishing its writes and releasing transaction locks. Crash durability is determined by the configured durability mode.

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

## Transaction and Error Details

completion_type defaults to 0 (no chain/no release); 1 requests CHAIN and 2 requests RELEASE when not overridden. AND NO CHAIN/NO RELEASE explicitly override those defaults. CHAIN starts a new transaction with the ending isolation/access mode. RELEASE closes the connection after the response. Ordinary COMMIT cannot decide a live XA branch; use XA COMMIT.

With strict durability a successful commit waits for the required WAL synchronization. Under relaxed or memory durability, a successful response has that mode's guarantee. Handle a failed commit explicitly; it is not proof that an application may blindly repeat the write.

## Examples

```sql
COMMIT;
COMMIT AND CHAIN;
COMMIT WORK AND NO CHAIN NO RELEASE;
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
