# `CREATE ROLE`

## Description

Creates one or more role accounts.

## Syntax

```ebnf
<create-role> ::= 'CREATE ROLE' [ 'IF NOT EXISTS' ]
                  <account> { ',' <account> }
```

## Behavior and Limitations

- Roles use account identifiers and participate in the role graph, but by default cannot be used as ordinary password accounts.
- Multiple roles are created atomically; IF NOT EXISTS produces a Note for duplicates.

## Examples

```sql
CREATE ROLE 'reader', 'writer';
CREATE ROLE IF NOT EXISTS 'auditor';
```

## See Also

- [Account and privilege index](README.md)
- [Syntax notation](../syntax-conventions.md)
