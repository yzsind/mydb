# `ALTER DATABASE`

## Description

Changes a database's default character set and collation. `ALTER SCHEMA` is a synonym.

## Syntax

```ebnf
<alter-database> ::= 'ALTER' ( 'DATABASE' | 'SCHEMA' ) [ <identifier> ]
                     ( [ 'DEFAULT' ] 'CHARACTER SET' [ '=' ] <charset>
                     | [ 'DEFAULT' ] 'COLLATE' [ '=' ] <collation> ) { <option> }
```

## Behavior and Limitations

- An omitted name selects the current database; without a selected database, the statement fails.
- Requires the database ALTER privilege and an exclusive schema metadata lock. It is an implicit-commit DDL statement.
- Existing tables and stored routine definitions retain their character settings. New tables inherit the new defaults.
- The character set and collation must be compatible. A rejected definition leaves the previous defaults intact.
- Defaults are published durably with the logical DDL binlog event. Encryption and database read-only options are not implemented by this statement.

## Examples

```sql
ALTER DATABASE app DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
USE app;
ALTER SCHEMA DEFAULT COLLATE utf8mb4_0900_ai_ci;
```

## See Also

- [DDL category index](README.md)
- [CREATE DATABASE](create-database.md)
