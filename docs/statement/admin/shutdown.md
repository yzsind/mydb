# `SHUTDOWN`

## Description

Requests a controlled server shutdown.

## Syntax

```ebnf
<shutdown> ::= 'SHUTDOWN'
```

## Behavior, Privileges, and Limitations

- Requires the SHUTDOWN privilege; stops accepting connections and follows the controlled flush/exit path.
- The SQL OK acknowledges the shutdown request, not completion of the final drain/checkpoint. Existing connections are disconnected as shutdown proceeds.

## Privileges, Transaction, and Result Details

Requires global SHUTDOWN. The server stops accepting connections, drains/cancels work and performs its final checkpoint. MYDB_SHUTDOWN_TIMEOUT_MS controls the connection wait (default 60000 ms; 0 skips waiting). Checkpoint/drain failure causes a nonzero exit; an earlier SQL OK is not proof of a clean final checkpoint. Uncommitted ordinary session work is not committed by closing clients; prepared XA decisions remain recoverable. See [shutdown configuration](../../configuration.md#shutdown-corruption-recovery-and-replication-initialization).

## Examples

```sql
SHUTDOWN;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
