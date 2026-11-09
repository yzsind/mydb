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
                      | 'SQL_THREAD' ',' 'IO_THREAD'
<until-condition> ::= ( 'SOURCE_LOG_FILE' '=' <string-literal> ','
                        'SOURCE_LOG_POS' '=' <unsigned-integer> )
                      | 'SQL_BEFORE_GTIDS' '=' <string-literal>
                      | 'SQL_AFTER_GTIDS' '=' <string-literal>
                      | 'SQL_AFTER_MTS_GAPS'
```

## Behavior, Prerequisites, and Limitations

- Omitting the selector starts both threads. UNTIL is meaningless for IO_THREAD and is rejected.
- Returns an explicit error when the channel is not configured, or the relay state requires a resync. The receiver is compiled into every build; there is no receiver Cargo feature to enable.
- The legacy SLAVE spelling produces a deprecation warning.

## Privileges, Results, and Operational Details

Requires REPLICATION_SLAVE_ADMIN. Omitted FOR CHANNEL means `default`, not every configured channel. IO_THREAD and SQL_THREAD can be selected in either comma-separated order. UNTIL is a SQL-apply condition and is invalid with IO_THREAD alone. SQL_BEFORE/AFTER_GTIDS use valid UUID interval sets; SQL_AFTER_MTS_GAPS is accepted in the single-applier model.

START returns after starting/requesting the configured threads. Check SHOW REPLICA STATUS for actual IO connection, SQL apply, errors and target GTID attainment. The source must provide ROW/FULL and a consistent initialized dataset.

## Examples

```sql
START REPLICA FOR CHANNEL 'main';
START REPLICA SQL_THREAD UNTIL SQL_AFTER_GTIDS='11111111-1111-4111-8111-111111111111:1-100'
FOR CHANNEL 'main';
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)
