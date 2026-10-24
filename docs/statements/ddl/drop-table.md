# `DROP TABLE`

## Description

Drops one or more persistent or temporary tables.

## Syntax

```ebnf
<drop-table> ::= 'DROP' [ 'TEMPORARY' ] 'TABLE'
                 [ 'IF EXISTS' ] <table-list>
                 [ 'RESTRICT' | 'CASCADE' ]
```

## Behavior and Limitations

- Requires the DROP privilege and acquires exclusive metadata locks; missing objects produce an error or a Note depending on IF EXISTS.
- TEMPORARY resolves only session temporary tables and never accidentally drops a persistent table of the same name.
- Foreign key dependencies and DROP behavior are validated by the current implementation; unsupported CASCADE forms are explicitly rejected.
- The drop is DDL; once committed, recovery relies on backups/PITR.

## Examples

```sql
DROP TABLE old_orders, old_customer;
DROP TEMPORARY TABLE IF EXISTS tmp_result;
```

## See Also

- [DDL category index](README.md)
- [Syntax notation](../syntax-conventions.md)
- [SHOW CREATE TABLE](../metadata/show-create-table.md)
