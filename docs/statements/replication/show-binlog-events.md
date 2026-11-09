# `SHOW BINLOG EVENTS`

## Description

Shows the binlog event catalog by file and position.

## Syntax

```ebnf
<show-binlog-events> ::= 'SHOW BINLOG EVENTS'
                         [ 'IN' <string-literal> ]
                         [ 'FROM' <unsigned-integer> ]
                         [ 'LIMIT' ( <row-count> | <offset> ',' <row-count> ) ]
```

## Behavior, Prerequisites, and Limitations

- FROM values below the binlog header position are normalized to 4; invalid numbers or trailing tokens return an error.
- When the current mydb binlog manager has no enumerable events, a MySQL-compatible empty column shape is returned; events are never fabricated.
- A single result reads at most 100,000 events and 64 MiB of raw event data; exceeding the limit returns an error, and `LIMIT` and `FROM` can be used to query in segments.

## Privileges, Results, and Operational Details

Requires REPLICATION SLAVE. Columns are Log_name, Pos, Event_type, Server_id, End_log_pos and Info. FROM is a byte event coordinate and values below the header are normalized to 4. Enumerating is bounded to 100,000 events/64 MiB raw data; use the next event boundary and LIMIT to continue. A compatibility empty event schema is not a successful data-history export. See [event coordinate rules](status-fields.md#binlog-and-relay-event-listings).

## Examples

```sql
SHOW BINLOG EVENTS IN 'mydb-bin.000001' FROM 4 LIMIT 0, 100;
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)
