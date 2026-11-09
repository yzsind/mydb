# `CREATE DATABASE / SCHEMA`

## Description

Creates a database and registers its default character set and collation. `SCHEMA` is a synonym.

## Syntax

```ebnf
<create-database> ::= 'CREATE' ( 'DATABASE' | 'SCHEMA' )
                      [ 'IF NOT EXISTS' ] <identifier>
                      { <database-option> }
<database-option> ::= [ 'DEFAULT' ]
                      ( ( 'CHARACTER SET' | 'CHARSET' ) [ '=' ] <charset>
                      | 'COLLATE' [ '=' ] <collation>
                      | 'ENCRYPTION' [ '=' ] <string-literal> )
```

## Behavior and Limitations

- Requires the CREATE privilege; if the database name already exists, 1007 is returned, and IF NOT EXISTS turns this into a Note.
- The character set and collation must be a valid registered combination in mydb; invalid values return an explicit error.
- The DDL updates the data dictionary and uses the DDL implicit-commit publication path; it does not automatically switch the current database.

## Privileges, Transactions, and Dependencies

Requires CREATE on the target database and uses an implicit-commit DDL boundary. Omitted character settings are resolved by the server's database defaults; new tables inherit the stored database defaults. ENCRYPTION='N' is recognized as the existing unencrypted behavior; 'Y' returns 1235. Repeating CHARACTER SET is rejected; do not use repetition to patch an incompatible charset/collation pair.

An existing database returns 1007; IF NOT EXISTS emits a Note and preserves its definition. CREATE does not select the database. Inspect SHOW CREATE DATABASE and then issue USE separately.

## Examples

```sql
CREATE DATABASE app DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
CREATE DATABASE IF NOT EXISTS app;
```

## See Also

- [DDL category index](README.md)
- [Syntax notation](../syntax-conventions.md)
- [SHOW CREATE TABLE](../metadata/show-create-table.md)
