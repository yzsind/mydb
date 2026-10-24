# `CHANGE REPLICATION SOURCE TO`

## Description

Configures a MySQL→mydb row replica channel; CHANGE MASTER TO is a deprecated alias.

## Syntax

```ebnf
<change-source> ::= ( 'CHANGE REPLICATION SOURCE TO' | 'CHANGE MASTER TO' )
                    <source-assignment> { ',' <source-assignment> }
                    [ 'FOR CHANNEL' <string-literal> ]
<source-assignment> ::= ( 'SOURCE_HOST' | 'SOURCE_PORT' | 'SOURCE_USER'
                         | 'SOURCE_PASSWORD' | 'SOURCE_LOG_FILE'
                         | 'SOURCE_LOG_POS' | 'SOURCE_AUTO_POSITION'
                         | 'SOURCE_SSL_MODE' | 'SOURCE_SSL_CA'
                         | 'SOURCE_SSL_CERT' | 'SOURCE_SSL_KEY'
                         | 'SOURCE_DELAY' | <replication-filter-option> )
                         '=' <literal>
```

## Behavior, Prerequisites, and Limitations

- The executing receiver must be built with the mysql-replica feature; the source requires ROW/FULL.
- CHANGE is an incremental modification: fields that do not appear keep the channel's current configuration. STOP REPLICA should be issued before configuring.
- SOURCE_SSL_MODE supports DISABLED, REQUIRED, VERIFY_CA, and VERIFY_IDENTITY; filter options support do/ignore db/table/wild table.
- A few MySQL transport/retry/compression advisory options are accepted for compatibility and produce warning 1287; they are never silently invalidated.

## Examples

```sql
CHANGE REPLICATION SOURCE TO
  SOURCE_HOST='mysql.example',
  SOURCE_PORT=3306,
  SOURCE_USER='repl',
  SOURCE_PASSWORD='secret',
  SOURCE_AUTO_POSITION=1,
  SOURCE_SSL_MODE='VERIFY_IDENTITY'
FOR CHANNEL 'main';
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)
