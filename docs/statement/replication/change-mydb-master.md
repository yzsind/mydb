# `CHANGE MYDB MASTER TO`

## Description

Configures the master endpoint of a native replica.

## Syntax

```ebnf
<change-mydb-master> ::= 'CHANGE MYDB MASTER TO'
                         'HOST' '=' <string-literal>
                         [ ',' 'PORT' '=' <unsigned-integer> ]
```

## Behavior, Prerequisites, and Limitations

- If the receiver is running, STOP MYDB REPLICATION must be issued first; HOST is required, and when PORT is omitted the configured default is used.
- The configuration is persisted to role state; a persisted endpoint takes precedence over default sources from the startup environment. TLS/mTLS and certificates are controlled by native replication startup configuration.
- A standalone node (including after RESET) switches to replica and immediately forbids new writes; when entering replica state or changing the source, reseeding is mandatory to prevent local forked data from mixing with the master's WAL. Run `START MYDB REPLICATION FORCE_RESYNC` to complete the reseed.
- The master must use DEMOTE TO REPLICA; role switching cannot be bypassed through this command.

## Privileges, Results, and Operational Details

Requires global SYSTEM_VARIABLES_ADMIN. PORT is a u16 port; omission uses the configured native master_port (default 3316), not the MySQL listener port. STOP the receiver before changing endpoint. Changing a standalone/source identity persists a resync marker and replica read-only role before writes can resume. A master must use DEMOTE; this command cannot bypass a write fence or convert the old master's forked pages into a valid continuation.

For a source change, run FORCE_RESYNC only after arranging transport/auth token configuration and accepting replacement of local data. Poll [native status](status-fields.md#show-mydb-replica-status); an endpoint change alone is not successful reseeding.

## Examples

```sql
STOP MYDB REPLICATION;
CHANGE MYDB MASTER TO HOST='db-primary.example', PORT=3317;
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)
