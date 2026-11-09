# `EXPLAIN FOR CONNECTION`

## Description

Reads the SQL currently registered on another active connection and produces its execution plan in the format requested by this session.

## Syntax

```ebnf
<explain-for-connection> ::= 'EXPLAIN'
                             [ 'FORMAT' [ '=' ]
                               ( 'TRADITIONAL' | 'TREE' | 'JSON' ) ]
                             'FOR CONNECTION' <connection-id>
```

## Behavior and Limitations

- Inspecting another connection requires `PROCESS`; the current connection has a self-inspection exception. Connection IDs come from `SHOW PROCESSLIST`.
- The target connection must still exist and be executing an explainable statement; otherwise 1094 "Unknown thread id" is returned.
- Only generates the plan; it neither re-executes the target statement nor interrupts the target connection.
- The plan reflects the target SQL and the current catalog; short-lived statements may already have finished before the check.

## Privileges, Results, and Boundaries

This is a snapshot/replanning diagnostic, not a handle to pause or replay another connection. It reads the target's registered SQL and default database, and uses the requesting session's selected explain format with the current catalog. Short commands can finish before lookup; absent SQL or an absent target returns 1094. The PROCESS/self check authorizes inspection without separately requiring the caller's ordinary privileges on every target table. View expansion can still impose its disclosure rules. The target is never run with ANALYZE. Result columns follow the chosen EXPLAIN format.

## Examples

```sql
SHOW PROCESSLIST;
EXPLAIN FORMAT=TREE FOR CONNECTION 123;
```

Replace `123` with the active connection ID. The statement fails without the privilege or when the target has already ended.

## Related Statements

- [EXPLAIN](explain.md)
- [SHOW PROCESSLIST](../metadata/show-processlist.md)
