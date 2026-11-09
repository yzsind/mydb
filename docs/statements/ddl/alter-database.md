# `ALTER DATABASE`

## Description

Changes a database's default character set and collation. `ALTER SCHEMA` is a synonym.

## Syntax

```ebnf
<alter-database> ::= 'ALTER' ( 'DATABASE' | 'SCHEMA' ) [ <identifier> ]
                     <database-option> { <database-option> }
```

## Behavior and Limitations

- An omitted name selects the current database; without a selected database, the statement fails.
- Requires the database ALTER privilege and an exclusive schema metadata lock. It is an implicit-commit DDL statement.
- Existing tables and stored routine definitions retain their character settings. New tables inherit the new defaults.
- The character set and collation must be compatible. A rejected definition leaves the previous defaults intact.
- Defaults use the DDL publication/binlog path. ENCRYPTION='N' is accepted alongside a charset/collation change and requests the existing unencrypted behavior; ENCRYPTION='Y' is rejected. Database read-only options are not implemented.

## Privileges, Transactions, and Dependencies

The database-option production is defined in [CREATE DATABASE](create-database.md). At least one CHARACTER SET/CHARSET or COLLATE change is required; ENCRYPTION='N' alone is not an attribute change. DEFAULT is an optional introducer, not an instruction to reset all attributes. An omitted database name uses the current schema; without one, the statement fails. Failure to validate the new pair preserves the old database defaults, but the implicit commit of previous user DML is a separate boundary.

## Examples

```sql
ALTER DATABASE app DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
USE app;
ALTER SCHEMA DEFAULT COLLATE utf8mb4_0900_ai_ci;
```

## See Also

- [DDL category index](README.md)
- [CREATE DATABASE](create-database.md)
