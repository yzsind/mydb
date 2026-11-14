# `DROP EVENT`

## Description

Drops a scheduled event.

## Syntax

```ebnf
<drop-event> ::= 'DROP EVENT' [ 'IF EXISTS' ] <qualified-name>
```

## Behavior and Limitations

- IF EXISTS controls whether a missing object is an error or a Note.
- Dropping does not roll back already completed event executions and does not interrupt independent transactions that have entered their execution tail.

## Context, Errors, and Lifecycle

Requires EVENT on the event schema; preceding user DML is implicitly committed. IF EXISTS handles missing definitions with a Note; it does not cancel already committed body work. Disabling removes future eligibility while DROP also removes metadata. Polling clients should not interpret definition disappearance as a rollback of an execution already in progress.

## Examples

```sql
DROP EVENT IF EXISTS purge_sessions;
```

## See Also

- [Stored programs index](README.md)
- [Syntax conventions](../syntax-conventions.md)
