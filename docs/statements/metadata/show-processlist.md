# `SHOW PROCESSLIST`

## Description

Lists current connections, commands, run time, state, and SQL summary.

## Syntax

```ebnf
<show-processlist> ::= 'SHOW' [ 'FULL' ] 'PROCESSLIST'
```

## Behavior, Privileges, and Results

- Without `PROCESS`, the current connection and foreground connections with the same login user name are visible. With `PROCESS`, all registered connections are visible. Non-FULL Info is truncated to 100 Unicode characters; FULL keeps the complete available SQL text.
- Connection IDs can be used with KILL or EXPLAIN FOR CONNECTION; the result is an instantaneous snapshot.

## Result and Visibility Details

Id is an unsigned connection identifier. Time is elapsed command time in seconds. db, State and Info can be NULL; FULL only changes SQL truncation. The snapshot can race with KILL/EXPLAIN FOR CONNECTION. PROCESS widens visibility but is not the authority to terminate another account's connection.

See [ordered columns and wire types](result-columns.md#show-processlist). Reading this metadata does not itself commit user DML.

## Examples

```sql
SHOW FULL PROCESSLIST;
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
