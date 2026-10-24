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

## Examples

```sql
SHOW PROFILES;
SHOW PROFILE CPU FOR QUERY 1;
```

## See Also

- [Replication and binlog index](README.md)
- [Native replication deployment](../../native-replication.md)
- [Syntax conventions](../syntax-conventions.md)
