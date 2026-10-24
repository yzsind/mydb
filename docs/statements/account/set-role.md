# `SET ROLE`

## Description

Selects the set of roles active in the current session.

## Syntax

```ebnf
<set-role> ::= 'SET ROLE'
               ( 'DEFAULT' | 'NONE' | 'ALL'
               | 'ALL EXCEPT' <role-list>
               | <role-list> )
```

## Behavior and Limitations

- Only roles already granted to the current account can be activated; ALL EXCEPT excludes roles from the available set.
- This changes the privilege closure of the current connection without modifying the account's default roles.
- Role changes affect subsequent privilege checks and any caches that depend on privilege semantics.

## Examples

```sql
SET ROLE DEFAULT;
SET ROLE 'reader', 'auditor';
SET ROLE ALL EXCEPT 'writer';
```

## See Also

- [Account and privilege index](README.md)
- [Syntax notation](../syntax-conventions.md)
