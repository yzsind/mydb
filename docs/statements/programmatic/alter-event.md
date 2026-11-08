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

## Context, Errors, and Lifecycle

Requires EVENT on the schema; naming another definer has its own privilege check. This is an implicit-commit DDL operation. The [CREATE EVENT schedule](create-event.md) defines interval values/units and timestamp validation. At least the new requested definition must be valid before publication; omitted fields are retained. ENABLE makes a definition eligible, not proof the global scheduler is ON. A renamed/disabled/dropped definition does not roll back already committed event work.

## Examples

```sql
ALTER EVENT purge_sessions
ON SCHEDULE EVERY 30 MINUTE
ENABLE;
```

## See Also

- [Stored programs index](README.md)
- [Syntax conventions](../syntax-conventions.md)
