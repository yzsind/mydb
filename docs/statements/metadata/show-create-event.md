# `SHOW CREATE EVENT`

## Description

Returns the canonical CREATE statement and status of a scheduled event.

## Syntax

```ebnf
<show-create-event> ::= 'SHOW CREATE EVENT' <qualified-name>
```

## Behavior, Privileges, and Results

- The output includes definer, time zone, schedule, completion, status, comment, and the DO body.
- It does not trigger event execution.

## Result and Visibility Details

EVENT visibility controls access. Schedule, status and ON COMPLETION are encoded in Create Event; Status is not an additional standalone result column in this command. The saved time zone and character-set metadata accompany the body.

See [ordered columns and wire types](result-columns.md#show-create-event). Reading this metadata does not itself commit user DML.

## Examples

```sql
SHOW CREATE EVENT purge_sessions;
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
