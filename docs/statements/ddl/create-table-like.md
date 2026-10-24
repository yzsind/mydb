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
- The CREATE privilege is required for the target, and the privilege to view the definition is required for the source.
- A temporary target follows the session lifecycle; IF NOT EXISTS only handles an already existing target.

## Examples

```sql
CREATE TABLE customer_archive LIKE customer;
CREATE TEMPORARY TABLE tmp_orders LIKE orders ON COMMIT DROP;
```

## See Also

- [DDL category index](README.md)
- [Syntax notation](../syntax-conventions.md)
- [SHOW CREATE TABLE](../metadata/show-create-table.md)
