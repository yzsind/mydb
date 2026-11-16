# `SHOW GRANTS`

## Description

Shows the replayable grant statements for the current account or a specified account.

## Syntax

```ebnf
<show-grants> ::= 'SHOW GRANTS'
                  [ 'FOR' ( <account> | 'CURRENT_USER' [ '()' ] ) ]
                  [ 'USING' <role-list> ]
```

## Behavior and Limitations

- Without FOR, the current account is shown; viewing another account requires SELECT on mysql.user.
- USING computes the effective privileges for the given granted roles.
- The output is canonical SQL text such as GRANT/SET DEFAULT ROLE and does not modify privileges.

## Privileges and Result Details

Returns one string column named for the displayed account, containing canonical GRANT/SET DEFAULT ROLE/restriction statements. No FOR means the current authorization account. USING requires roles applicable to that account (otherwise 3530) and expands effective grants; it is not a role-activation operation. Looking at another account needs SELECT on mysql.user. Empty privileges can still produce a USAGE statement; result text is not a numeric privilege bitset.

## Examples

```sql
SHOW GRANTS;
SHOW GRANTS FOR 'report'@'%' USING 'reader';
```

## See Also

- [Account and privilege index](README.md)
- [Syntax notation](../syntax-conventions.md)
