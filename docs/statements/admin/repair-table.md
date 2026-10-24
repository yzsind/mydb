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

- mydb has no MyISAM-style in-place REPAIR; for normal transactional tables it returns a Note directing you to re-clone from a replica or use PITR.
- It does not fake a "repaired" status or silently rewrite pages; genuinely unsupported options return an explicit diagnostic.

## Examples

```sql
REPAIR TABLE orders;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
