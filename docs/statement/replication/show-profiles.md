# `SHOW PROFILES / PROFILE`

## Description

The deprecated MySQL statement profiler syntax is recognized, but legacy profiling is not implemented.

## Syntax

```ebnf
<show-profiles> ::= 'SHOW PROFILES'
                    | 'SHOW PROFILE' [ <profile-type> { ',' <profile-type> } ]
                      [ 'FOR QUERY' <unsigned-integer> ]
                      [ 'LIMIT' <limit-clause> ]
```

## Behavior, Prerequisites, and Limitations

- mydb does not collect legacy statement profiles.
- Execution and prepared-statement metadata binding return error 1235 (`ER_NOT_SUPPORTED_YET`). Use performance_schema and EXPLAIN ANALYZE for measured execution.

## Privileges, Results, and Operational Details

No legacy samples are collected. Valid PROFILE/PROFILES statements return an explicit unsupported-feature error. Profile types include ALL, BLOCK IO, CONTEXT SWITCHES, CPU, IPC, MEMORY, PAGE FAULTS, SOURCE and SWAPS. LIMIT uses the shared count/offset grammar. Invalid syntax returns error 1064.

## Examples

```sql
SHOW PROFILES;
SHOW PROFILE CPU FOR QUERY 1;
-- Both statements return error 1235.
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)
