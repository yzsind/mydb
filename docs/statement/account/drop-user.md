# `DROP USER`

## Description

Atomically drops one or more accounts and their direct grants.

## Syntax

```ebnf
<drop-user> ::= 'DROP USER' [ 'IF EXISTS' ]
                <account> { ',' <account> }
```

## Behavior and Limitations

- The entire list is validated first; any non-ignorable error prevents all deletions.
- IF EXISTS turns missing accounts into a 3162 Note.
- Dropping an account does not automatically drop the ordinary database objects it owns; definer dependencies are handled according to object rules.

## Privileges and Result Details

Requires CREATE USER and, for protected system accounts, SYSTEM_USER. It implicitly commits preceding user DML. The entire target list is validated before catalog removal, including mandatory-role, policy and definer dependency restrictions. Grants/default-role edges and owned API keys are cleaned up; already stored business objects are not deleted. Revoking credentials is not a way to roll back prior committed work.

## Examples

```sql
DROP USER 'old_app'@'%', 'temp'@'localhost';
DROP USER IF EXISTS 'missing'@'%';
```

## See Also

- [Account and privilege index](README.md)
- [Syntax notation](../syntax-conventions.md)
