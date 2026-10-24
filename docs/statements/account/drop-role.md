# `DROP ROLE`

## Description

Drops one or more roles and the related edges in the role graph.

## Syntax

```ebnf
<drop-role> ::= 'DROP ROLE' [ 'IF EXISTS' ]
                <account> { ',' <account> }
```

## Behavior and Limitations

- Multiple targets are processed atomically; IF EXISTS controls the diagnostics for missing roles.
- Dropping a role does not delete objects already granted to that role, nor does it copy the role's privileges to its members.

## Examples

```sql
DROP ROLE 'legacy_reader';
```

## See Also

- [Account and privilege index](README.md)
- [Syntax notation](../syntax-conventions.md)
