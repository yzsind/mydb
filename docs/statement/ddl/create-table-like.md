# `CREATE TABLE LIKE`

## Description

Creates a new table from the definition of an existing table, without copying rows.

## Syntax

```ebnf
<create-table-like> ::= 'CREATE' [ 'TEMPORARY' ] 'TABLE'
                        [ 'IF NOT EXISTS' ] <qualified-name>
                        'LIKE' <qualified-name>
                        [ 'ON COMMIT' ( 'PRESERVE ROWS' | 'DELETE ROWS' | 'DROP' ) ]
```

## Behavior and Limitations

- Copies columns, CHECK constraints (with new names), indexes, supported table options, and partition definitions. Foreign key definitions and DATA/INDEX DIRECTORY placement are not copied; no SELECT is executed and no data is copied.
- The CREATE privilege is required for the target, and `SELECT` is required on the source. A temporary target instead requires `CREATE TEMPORARY TABLES` in its schema.
- A temporary target follows the session lifecycle; IF NOT EXISTS only handles an already existing target.

## Privileges, Transactions, and Dependencies

The target is empty. The copy retains column types/defaults/generated/invisible attributes, indexes, CHECK constraints with new names, supported options and partitioning. It does not copy data, foreign keys or physical DATA/INDEX DIRECTORY placement. It is not a view-definition clone. CREATE TEMPORARY requires CREATE TEMPORARY TABLES, while a persistent target requires CREATE; source SELECT is checked in either case. The target lifecycle/ON COMMIT policy is independent of the source.

IF NOT EXISTS only handles an existing target; it neither replaces a schema nor checks equivalence. See [temporary DDL boundaries](../execution-contracts.md#transaction-boundaries).

## Examples

```sql
CREATE TABLE customer_archive LIKE customer;
CREATE TEMPORARY TABLE tmp_orders LIKE orders ON COMMIT DROP;
```

## See Also

- [DDL category index](README.md)
- [Syntax notation](../syntax-conventions.md)
- [SHOW CREATE TABLE](../metadata/show-create-table.md)
