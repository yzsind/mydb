# `START MYDB REPLICATION`

## Description

Starts the native replica receiver/apply pipeline.

## Syntax

```ebnf
<start-mydb-replication> ::= 'START MYDB REPLICATION' [ 'FORCE_RESYNC' ]
```

## Behavior, Prerequisites, and Limitations

- The node must be a replica with a configured master endpoint and valid transport security settings.
- A plain START refuses to resume when a resync marker exists; repeated starts are idempotent.
- `FORCE_RESYNC` persists a reseed marker, drains connections and replication tasks, then performs a controlled restart that pulls the staged base backup before restarting replication. It disconnects existing connections and replaces local data; it is not a no-downtime online hot swap.
- `FORCE_RESYNC` requires both the `SYSTEM_VARIABLES_ADMIN` and `SHUTDOWN` privileges; the platform must support RESTART and `MYDB_RESTART_DISABLE` must not be set. When unsupported, an explicit error is returned before any state is changed.
- If the master is unreachable or the backup fails, the process stays alive and retries with exponential backoff starting from `MYDB_REPL_RECONNECT_MS` (100 ms to 30 s), capped at a 30 s interval; it does not listen for SQL until seeding completes.

## Examples

```sql
START MYDB REPLICATION;
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)
