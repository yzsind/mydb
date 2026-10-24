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

- Requires the `PROCESS` privilege; connection IDs can be obtained from `SHOW PROCESSLIST`.
- The target connection must still exist and be executing an explainable statement; otherwise 1094 "Unknown thread id" is returned.
- Only generates the plan; it neither re-executes the target statement nor interrupts the target connection.
- The plan reflects the target SQL and the current catalog; short-lived statements may already have finished before the check.

## Examples

```sql
SHOW PROCESSLIST;
EXPLAIN FORMAT=TREE FOR CONNECTION 123;
```

Replace `123` with the active connection ID. The statement fails without the privilege or when the target has already ended.

## Related Statements

- [EXPLAIN](explain.md)
- [SHOW PROCESSLIST](../metadata/show-processlist.md)
