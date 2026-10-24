# `KILL`

## Description

Terminates a connection or cancels its current query.

## Syntax

```ebnf
<kill> ::= 'KILL' [ 'CONNECTION' | 'QUERY' ] <connection-id>
```

## Behavior, Privileges, and Limitations

- Omitting the target is equivalent to KILL CONNECTION. KILL QUERY only cancels the current statement and keeps the connection.
- Viewing/terminating connections of other accounts requires the corresponding privileges, such as CONNECTION_ADMIN/PROCESS.
- A nonexistent connection returns 1094; cancellation propagates through the execution context to parallel workers and spill operators.

## Examples

```sql
KILL QUERY 123;
KILL CONNECTION 124;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
