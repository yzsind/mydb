# `RESET REPLICA`

## Description

Resets a channel's relay/progress state; RESET SLAVE is a deprecated alias.

## Syntax

```ebnf
<reset-replica> ::= ( 'RESET REPLICA' | 'RESET SLAVE' )
                    [ 'ALL' ] [ 'FOR CHANNEL' <string-literal> ]
```

## Behavior, Prerequisites, and Limitations

- The channel should be stopped first. Plain RESET keeps connection configuration; ALL also clears the persisted source configuration.
- Does not reset binlog/GTID on the source; the legacy spelling produces a deprecation warning.

## Privileges, Results, and Operational Details

Requires REPLICATION_SLAVE_ADMIN. RESET stops the selected channel before clearing relay/receive progress, retrieved GTIDs, counters and last errors. Plain RESET retains connection configuration. ALL removes its persisted source configuration too. Both preserve the server's executed GTID history; neither resets the source binlog. Prepared source XA in this channel can block reset until its decision is applied. Inspect and intentionally STOP first when planning a reset.

## Examples

```sql
STOP REPLICA FOR CHANNEL 'main';
RESET REPLICA ALL FOR CHANNEL 'main';
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)
