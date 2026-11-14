# `REPAIR TABLE`

## Description

Provides a table repair entry point for MySQL tooling compatibility.

## Syntax

```ebnf
<repair-table> ::= 'REPAIR TABLE' <table-list>
                   { 'QUICK' | 'EXTENDED'
                     | 'LOCAL' | 'NO_WRITE_TO_BINLOG' }
```

## Behavior, Privileges, and Limitations

- mydb has no MyISAM-style in-place REPAIR; for normal transactional tables it returns a result note that the engine does not support repair. Use a verified replica rebuild or PITR for recovery.
- It does not rewrite pages. The listed compatibility options do not change the operation. The returned note is a `Msg_type=note` result row, not a session warning; `SHOW WARNINGS` can still be empty.

## Privileges, Transaction, and Result Details

Requires SELECT and INSERT and implicitly commits preceding DML. The returned note is not evidence of a repaired file or index. No in-place repair algorithm or MyISAM page recovery is performed, and compatibility QUICK/EXTENDED/other recognized options do not add one. Use CHECK for the supported invariants and a verified restore/reseed for damage recovery.

## Examples

```sql
REPAIR TABLE orders;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
