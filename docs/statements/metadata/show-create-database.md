# `SHOW CREATE DATABASE`

## Description

Returns the SQL to recreate the database along with its default character set/collation.

## Syntax

```ebnf
<show-create-database> ::= 'SHOW CREATE'
                            ( 'DATABASE' | 'SCHEMA' )
                            [ 'IF NOT EXISTS' ] <identifier>
```

## Behavior, Privileges, and Results

- IF NOT EXISTS only controls whether the output SQL contains that phrase; it does not change object lookup.
- The result uses canonical quoting and the current authoritative catalog source.

## Examples

```sql
SHOW CREATE DATABASE app;
SHOW CREATE DATABASE IF NOT EXISTS app;
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
