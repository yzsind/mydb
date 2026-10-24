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

## Examples

```sql
DROP DATABASE obsolete_app;
DROP DATABASE IF EXISTS scratch;
```

## See Also

- [DDL category index](README.md)
- [Syntax notation](../syntax-conventions.md)
- [SHOW CREATE TABLE](../metadata/show-create-table.md)
