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

## Examples

```sql
DROP EVENT IF EXISTS purge_sessions;
```

## See Also

- [Stored programs index](README.md)
- [Syntax conventions](../syntax-conventions.md)
