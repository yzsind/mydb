# `CREATE DATABASE / SCHEMA`

## Description

Creates a database and registers its default character set and collation. `SCHEMA` is a synonym.

## Syntax

```ebnf
<create-database> ::= 'CREATE' ( 'DATABASE' | 'SCHEMA' )
                      [ 'IF NOT EXISTS' ] <identifier>
                      [ 'DEFAULT' ] [ 'CHARACTER SET' [ '=' ] <identifier> ]
                      [ [ 'DEFAULT' ] 'COLLATE' [ '=' ] <identifier> ]
```

## Behavior and Limitations

- Requires the CREATE privilege; if the database name already exists, 1007 is usually returned, and IF NOT EXISTS turns this into a Note.
- The character set and collation must be a valid registered combination in mydb; invalid values return an explicit error.
- The DDL updates the data dictionary and persists according to DDL transaction rules; it does not automatically switch the current database.

## Examples

```sql
CREATE DATABASE app DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
CREATE DATABASE IF NOT EXISTS app;
```

## See Also

- [DDL category index](README.md)
- [Syntax notation](../syntax-conventions.md)
- [SHOW CREATE TABLE](../metadata/show-create-table.md)
