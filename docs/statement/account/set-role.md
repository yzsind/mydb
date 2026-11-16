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

## Privileges and Result Details

No extra administration privilege is needed to activate applicable roles. An explicit role list is checked against direct/applicable and mandatory roles. ALL EXCEPT is subtractive; naming an ungranted exclusion does not grant it. DEFAULT uses the account default set; NONE clears explicit activation. mandatory_roles/activate_all_roles_on_login affect the applicable/default sets.

This is current-session state, not a grant mutation or DDL implicit commit. Use CURRENT_ROLE() and SHOW GRANTS ... USING ... to inspect role context. Changing roles revalidates subsequent privilege-dependent executions, including prepared statements.

## Examples

```sql
SET ROLE DEFAULT;
SET ROLE 'reader', 'auditor';
SET ROLE ALL EXCEPT 'writer';
```

## See Also

- [Account and privilege index](README.md)
- [Syntax notation](../syntax-conventions.md)
