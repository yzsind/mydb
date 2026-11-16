# `KILL`

## Description

Terminates a connection or cancels its current query.

## Syntax

```ebnf
<kill> ::= 'KILL' [ 'CONNECTION' | 'QUERY' ] <connection-id>
```

## Behavior, Privileges, and Limitations

- Omitting the CONNECTION/QUERY modifier is equivalent to KILL CONNECTION; the connection ID is still required. KILL QUERY only cancels the current statement and keeps the connection.
- Viewing/terminating connections of other accounts requires the corresponding privileges, such as CONNECTION_ADMIN/PROCESS.
- A nonexistent connection returns 1094; cancellation propagates through the execution context to parallel workers and spill operators.

## Privileges, Transaction, and Result Details

The current account can terminate its own connections. Terminating a different account requires CONNECTION_ADMIN; PROCESS only changes visibility. System-account protection additionally requires SYSTEM_USER where applicable. A missing target is 1094; a target owned by another account without authority is 1095.

KILL QUERY interrupts the current statement, with normal statement rollback in an explicit transaction. KILL CONNECTION closes the session and rolls back its uncommitted ordinary transaction; prepared XA follows explicit recovery semantics. Cancellation is cooperative through workers/lock waits, so an accepted request is not proof every target worker has already exited.

## Examples

```sql
KILL QUERY 123;
KILL CONNECTION 124;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
