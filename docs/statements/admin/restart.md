# `RESTART`

## Description

Requests a controlled shutdown followed by relaunching the same server executable.

## Syntax

```ebnf
<restart> ::= 'RESTART'
```

## Behavior, Privileges, and Limitations

- Requires the SHUTDOWN privilege and disconnects existing clients.
- After draining connections and completing the final checkpoint, Unix re-executes the same executable with the same PID; Windows launches a replacement process with a new PID. The original arguments and working directory are retained, and startup configuration is read again.
- `MYDB_RESTART_DISABLE=1`, an unsupported platform, or an unavailable executable returns 3707 before shutdown begins. Use SHUTDOWN and the supervisor's restart procedure when process replacement is managed externally.
- A failed final checkpoint or shutdown drain prevents relaunch and makes the process exit nonzero. An SQL OK acknowledges the request; verify completion in the server logs and reconnect afterward. See [configuration](../../configuration.md#shutdown-corruption-recovery-and-replication-initialization).

## Examples

```sql
RESTART;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
