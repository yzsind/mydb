# `SHOW PRIVILEGES`

## Description

Lists the privilege names the server recognizes, their applicable contexts, and their descriptions.

## Syntax

```ebnf
<show-privileges> ::= 'SHOW PRIVILEGES'
```

## Behavior and Limitations

- Returns the static/registered privilege catalog; it does not indicate that the current account holds these privileges.
- The result can be used for client-side privilege probing; it does not modify transactions.

## Examples

```sql
SHOW PRIVILEGES;
```

## See Also

- [Account and privilege index](README.md)
- [Syntax notation](../syntax-conventions.md)
