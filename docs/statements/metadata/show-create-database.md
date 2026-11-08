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

## Result and Visibility Details

The output is normalized SQL, including version-comment defaults. IF NOT EXISTS changes the returned SQL only; unknown database lookup still returns 1049. It does not select that database or commit user DML.

See [ordered columns and wire types](result-columns.md#show-create-database). Reading this metadata does not itself commit user DML.

## Examples

```sql
SHOW CREATE DATABASE app;
SHOW CREATE DATABASE IF NOT EXISTS app;
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
