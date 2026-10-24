# `SHOW CREATE USER`

## Description

Returns the CREATE USER text that can rebuild an account's authentication and policy attributes.

## Syntax

```ebnf
<show-create-user> ::= 'SHOW CREATE USER'
                       ( <account> | 'CURRENT_USER' [ '()' ] )
```

## Behavior and Limitations

- Sensitive credentials are shown as the plugin-format authentication string; the plaintext password is never returned.
- Viewing another account requires administrative privileges; a nonexistent account returns an error.

## Examples

```sql
SHOW CREATE USER CURRENT_USER();
SHOW CREATE USER 'app'@'10.%';
```

## See Also

- [Account and privilege index](README.md)
- [Syntax notation](../syntax-conventions.md)
