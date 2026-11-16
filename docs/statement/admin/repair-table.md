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

- mydb tables are transactional MVCC tables; there is no in-place repair algorithm, and `REPAIR TABLE` returns a `Msg_type=note` result row stating that the engine does not support repair. For damage recovery, use a verified replica rebuild or point-in-time recovery.
- The statement does not rewrite pages, and the listed compatibility options do not change the operation. The note is a result row, not a session warning; `SHOW WARNINGS` can still be empty.

## Privileges, Transaction, and Result Details

Requires SELECT and INSERT and implicitly commits preceding DML. Use [CHECK TABLE](check-table.md) for the supported invariants and a verified restore/reseed for damage recovery.

## Examples

```sql
REPAIR TABLE orders;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
