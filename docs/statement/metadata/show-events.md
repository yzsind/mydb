# `SHOW EVENTS`

## Description

Lists the visible scheduled events in a database.

## Syntax

```ebnf
<show-events> ::= 'SHOW EVENTS'
                 [ ( 'FROM' | 'IN' ) <identifier> ]
                 [ 'LIKE' <string-literal> | 'WHERE' <expr> ]
```

## Behavior, Privileges, and Results

- Returns compatibility columns such as Db, Name, Definer, Time zone, Type, Execute at/Interval, and Status.
- Status comes from the event catalog/scheduler.

## Result and Visibility Details

Rows are filtered by EVENT visibility; LIKE matches Name and WHERE uses the displayed column names. Execute at/Starts/Ends are formatted schedule values. Status is ENABLED/DISABLED/SLAVESIDE_DISABLED as stored; it does not prove the scheduler is enabled.

See [ordered columns and wire types](result-columns.md#show-events). Reading this metadata does not itself commit user DML.

## Examples

```sql
SHOW EVENTS FROM app WHERE Status = 'ENABLED';
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
