# `CHECK TABLE`

## Description

Checks the verifiable consistency of tables, heaps, indexes, and the catalog.

## Syntax

```ebnf
<check-table> ::= 'CHECK TABLE' <table-list>
                  { 'QUICK' | 'FAST' | 'MEDIUM' | 'EXTENDED'
                    | 'CHANGED' | 'FOR UPGRADE'
                    | 'LOCAL' | 'NO_WRITE_TO_BINLOG' }
```

## Behavior, Privileges, and Limitations

- Returns one row per table: Table/Op/Msg_type/Msg_text.
- mydb is a transactional MVCC engine; normal operation should not produce MyISAM-style repairable corruption. Severe recovery should use replica rebuilds or PITR.
- Trailing compatibility options affect the check only to the extent they can be expressed; physical differences that cannot be honored are diagnosed.

## Examples

```sql
CHECK TABLE orders EXTENDED;
CHECK TABLE customer, orders QUICK;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
