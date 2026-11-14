# `DROP DATABASE / SCHEMA`

## Description

Deletes a database and all objects in it. `SCHEMA` is a synonym.

## Syntax

```ebnf
<drop-database> ::= 'DROP' ( 'DATABASE' | 'SCHEMA' )
                    [ 'IF EXISTS' ] <identifier>
```

## Behavior and Limitations

- Requires the DROP privilege; a non-empty database is also deleted as a whole, making this a high-impact operation.
- Returns 1008 when the database does not exist; IF EXISTS turns this into a Note.
- A committed DROP cannot be recovered by transaction ROLLBACK; back up first.

## Privileges, Transactions, and Dependencies

Requires DROP on the schema. The current transaction is implicitly committed before the drop. A missing database is 1008, or a Note with IF EXISTS. Removing a persistent schema invalidates dependent cross-schema views; querying an invalid view returns 1356. RLS dependency checks and AI model ownership can block removal (drop schema AI models explicitly first). Session temporary objects are not a backup of the deleted persistent schema. Use SHOW CREATE and a verified backup before relying on a recoverable definition.

## Examples

```sql
DROP DATABASE obsolete_app;
DROP DATABASE IF EXISTS scratch;
```

## See Also

- [DDL category index](README.md)
- [Syntax notation](../syntax-conventions.md)
- [SHOW CREATE TABLE](../metadata/show-create-table.md)
