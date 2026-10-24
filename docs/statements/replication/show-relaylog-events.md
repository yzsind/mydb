# `SHOW RELAYLOG EVENTS`

## Description

Shows the event frames of a specified channel's relay log.

## Syntax

```ebnf
<show-relaylog-events> ::= 'SHOW RELAYLOG EVENTS'
                           [ 'IN' <string-literal> ]
                           [ 'FROM' <unsigned-integer> ]
                           [ 'LIMIT' ( <row-count> | <offset> ',' <row-count> ) ]
                           [ 'FOR CHANNEL' <string-literal> ]
```

## Behavior, Prerequisites, and Limitations

- The mydb relay log uses JSON-Lines framing, so Pos/End_log_pos are file offsets within that framing, not MySQL binary relay log offsets.
- Returns a compatible empty result when the manager is not initialized.

## Examples

```sql
SHOW RELAYLOG EVENTS IN 'relay-main.jsonl'
FROM 0 LIMIT 100 FOR CHANNEL 'main';
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)
