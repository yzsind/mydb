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

- Without FOR, the current account is shown; viewing another account usually requires the corresponding administrative privileges.
- USING computes the effective privileges for the given granted roles.
- The output is canonical SQL text such as GRANT/SET DEFAULT ROLE and does not modify privileges.

## Examples

```sql
SHOW GRANTS;
SHOW GRANTS FOR 'report'@'%' USING 'reader';
```

## See Also

- [Account and privilege index](README.md)
- [Syntax notation](../syntax-conventions.md)
