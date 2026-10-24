# `SHOW MYDB MEMORY SESSIONS`

## Description

Shows per-connection executing and retained cache memory.

## Syntax

```ebnf
<show-mydb-memory-sessions> ::= 'SHOW MYDB MEMORY SESSIONS'
```

## Behavior, Privileges, and Results

- Results come from the session registry and the unified governor and may change with concurrent execution.
- Without the PROCESS privilege, only the current connection is returned (other connections of the same account are not returned either); with the PROCESS privilege, all sessions are returned. Connections that have already ended are not retained.
- STATE prefers the actual execution/wait state shown by SHOW PROCESSLIST; when the registry provides no state, it returns RUNNING if a query is active and IDLE otherwise.

## Examples

```sql
SHOW MYDB MEMORY SESSIONS;
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
