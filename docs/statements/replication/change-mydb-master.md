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

## Examples

```sql
STOP MYDB REPLICATION;
CHANGE MYDB MASTER TO HOST='db-primary.example', PORT=3317;
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)
