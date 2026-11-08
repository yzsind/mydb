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

- The receiver is included in every build; the source requires ROW/FULL.
- CHANGE is an incremental modification: fields that do not appear keep the channel's current configuration. STOP REPLICA should be issued before configuring.
- A new file/position channel with no SOURCE_LOG_FILE starts at the source's first available binary log; SOURCE_LOG_POS defaults to 4. Reconnects resume at the last committed transaction boundary. This does not create a snapshot of data whose binary logs have already been purged.
- SOURCE_SSL_MODE supports DISABLED, REQUIRED, VERIFY_CA, and VERIFY_IDENTITY; filter options support do/ignore db/table/wild table.
- A few MySQL transport/retry/compression advisory options are accepted for compatibility and produce warning 1287; they are never silently invalidated.

## Source Options

| Options | Type/default and effect |
| --- | --- |
| SOURCE_HOST, SOURCE_PORT | Host string and u16 port; new port defaults to 3306 |
| SOURCE_USER, SOURCE_PASSWORD | Credential strings; new user defaults to root, but a dedicated replication account should be configured |
| SOURCE_LOG_FILE, SOURCE_LOG_POS | File string and unsigned byte position (default 4); omitted file on a new file/position channel selects first available source log |
| SOURCE_AUTO_POSITION | Boolean, default false; use executed GTIDs to resume |
| SOURCE_SERVER_ID / SERVER_ID | u32 replica registration identity; new default 9001 |
| SOURCE_CONNECT_RETRY | Seconds, actually applied to reconnect backoff; zero is clamped to at least 100 ms after conversion |
| SOURCE_CONNECT_RETRY_MS / CONNECT_RETRY_MS | Native millisecond spelling; new default 1000 ms |
| SOURCE_DELAY | Nonnegative intentional SQL delay, seconds; default 0 |
| SOURCE_SSL / SOURCE_SSL_MODE | Boolean TLS shorthand / DISABLED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY; default DISABLED subject to secure transport policy |
| SOURCE_SSL_CA/CERT/KEY/CIPHER | TLS path/cipher strings; empty optional path clears that field; CERT/KEY must pair |
| SOURCE_SSL_VERIFY_SERVER_CERT | Compatibility verification boolean; enabling selects identity verification where needed |
| REPLICATE_DO_DB / REPLICATE_IGNORE_DB | Database include/exclude lists |
| REPLICATE_DO_TABLE / REPLICATE_IGNORE_TABLE | Exact db.table include/exclude lists |
| REPLICATE_WILD_DO_TABLE / REPLICATE_WILD_IGNORE_TABLE | LIKE-style db.table rules; %/_ are wildcards; deny rules take precedence |

Filters accept the implemented comma-separated string/list forms; quote names/values deliberately. SOURCE_* options have the documented MASTER_* legacy aliases. Advisory retry-count, heartbeat, compression, public-key-path, bind/network namespace and selected transport/probing options are accepted with warning 1287 where implemented; they do not add missing transport/applier capabilities. SOURCE_CONNECT_RETRY is an effective option, not one of those no-effect advisories. Unknown options remain errors.

## Privileges, Results, and Operational Details

Requires global REPLICATION_SLAVE_ADMIN. The default channel is named `default`; configure/inspect explicit named channels when several exist. Configuration is incremental and persisted. Changing source does not create a data snapshot: initialize rows from a consistent snapshot and retain the matching ROW/FULL history first. Never substitute the first available log for a missing historical snapshot.

A secure transport policy can reject DISABLED. VERIFY_CA needs valid CA material; VERIFY_IDENTITY additionally verifies endpoint identity. Client certificate/key must be supplied together. Password values are credentials, not suitable status or log output. Verify connection/application state with SHOW REPLICA STATUS after START, rather than assuming CHANGE established a live connection.

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
