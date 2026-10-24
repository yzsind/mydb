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

## Examples

```sql
SHOW EVENTS FROM app WHERE Status = 'ENABLED';
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
