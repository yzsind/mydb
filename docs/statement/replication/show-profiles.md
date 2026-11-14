# `SHOW PROFILES / PROFILE`

## Description

The deprecated statement profiler, kept for compatibility with MySQL 5.7 client probing.

## Syntax

```ebnf
<show-profiles> ::= 'SHOW PROFILES'
                    | 'SHOW PROFILE' [ <profile-type> { ',' <profile-type> } ]
                      [ 'FOR QUERY' <unsigned-integer> ]
                      [ 'LIMIT' <limit-clause> ]
```

## Behavior, Prerequisites, and Limitations

- mydb does not collect legacy statement profiles; its empty compatibility results do not represent measured execution.
- The commands return a canonical empty result set column shape, which is explicitly documented compatibility behavior; performance diagnosis should use performance_schema and EXPLAIN ANALYZE.

## Privileges, Results, and Operational Details

No legacy samples are collected. PROFILE/PROFILES returns their canonical empty result schema without changing transactions. Profile types include ALL, BLOCK IO, CONTEXT SWITCHES, CPU, IPC, MEMORY, PAGE FAULTS, SOURCE and SWAPS. LIMIT uses the shared count/offset grammar; it cannot create measurements. Use EXPLAIN ANALYZE for a measured execution and performance_schema for live instrumentation.

## Examples

```sql
SHOW PROFILES;
SHOW PROFILE CPU FOR QUERY 1;
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)
