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

## Result and Visibility Details

Byte quantities are accounted observations, not independently summable process RSS. EXEC_CURRENT_BYTES and RETAINED_CACHE_BYTES distinguish running work and retained session caches; TOTAL_CURRENT_BYTES is their combined reported amount. NULL means the corresponding measurement is unavailable. QUERY_ID/state can change concurrently.

See [ordered columns and wire types](result-columns.md#show-mydb-memory-sessions). Reading this metadata does not itself commit user DML.

## Examples

```sql
SHOW MYDB MEMORY SESSIONS;
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
