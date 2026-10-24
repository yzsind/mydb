# `SHOW PROCESSLIST`

## Description

Lists current connections, commands, run time, state, and SQL summary.

## Syntax

```ebnf
<show-processlist> ::= 'SHOW' [ 'FULL' ] 'PROCESSLIST'
```

## Behavior, Privileges, and Results

- Without the PROCESS privilege, only your own connections are usually visible; FULL does not truncate Info.
- Connection IDs can be used with KILL or EXPLAIN FOR CONNECTION; the result is an instantaneous snapshot.

## Examples

```sql
SHOW FULL PROCESSLIST;
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
