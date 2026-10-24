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

## Examples

```sql
SHOW CREATE EVENT purge_sessions;
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
