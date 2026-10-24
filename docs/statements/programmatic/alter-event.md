# `ALTER EVENT`

## Description

Modifies an event's schedule, state, name, attributes, or routine body.

## Syntax

```ebnf
<alter-event> ::= 'ALTER' [ 'DEFINER' [ '=' ] <account> ]
                  'EVENT' <qualified-name>
                  [ 'ON SCHEDULE' <event-schedule> ]
                  [ 'ON COMPLETION' [ 'NOT' ] 'PRESERVE' ]
                  [ 'RENAME TO' <qualified-name> ]
                  [ 'ENABLE' | 'DISABLE' | 'DISABLE ON SLAVE' ]
                  [ 'COMMENT' <string-literal> ]
                  [ 'DO' <routine-statement> ]
```

## Behavior and Limitations

- Unspecified attributes keep their current values; the new schedule and routine body are validated first and then updated atomically.
- Disabling an event does not delete its definition; renames and attribute changes update the data dictionary.

## Examples

```sql
ALTER EVENT purge_sessions
ON SCHEDULE EVERY 30 MINUTE
ENABLE;
```

## See Also

- [Stored programs index](README.md)
- [Syntax conventions](../syntax-conventions.md)
