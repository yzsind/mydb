# `START REPLICA`

## Description

Starts the IO/SQL threads of a MySQL→mydb channel; START SLAVE is a deprecated alias.

## Syntax

```ebnf
<start-replica> ::= ( 'START REPLICA' | 'START SLAVE' )
                    [ <thread-selector> ]
                    [ 'UNTIL' <until-condition> ]
                    [ 'FOR CHANNEL' <string-literal> ]
<thread-selector> ::= 'IO_THREAD' | 'SQL_THREAD'
                      | 'IO_THREAD' ',' 'SQL_THREAD'
<until-condition> ::= ( 'SOURCE_LOG_FILE' '=' <string-literal> ','
                        'SOURCE_LOG_POS' '=' <unsigned-integer> )
                      | 'SQL_BEFORE_GTIDS' '=' <string-literal>
                      | 'SQL_AFTER_GTIDS' '=' <string-literal>
                      | 'SQL_AFTER_MTS_GAPS'
```

## Behavior, Prerequisites, and Limitations

- Omitting the selector starts both threads. UNTIL is meaningless for IO_THREAD and is rejected.
- Returns an explicit error when the channel is not configured, the feature is not enabled, or the relay state requires a resync.
- The legacy SLAVE spelling produces a deprecation warning.

## Examples

```sql
START REPLICA FOR CHANNEL 'main';
START REPLICA SQL_THREAD UNTIL SQL_AFTER_GTIDS='uuid:1-100'
FOR CHANNEL 'main';
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)
