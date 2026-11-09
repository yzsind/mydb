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

## Privileges and Result Details

The columns are Privilege, Context and Comment, all VARCHAR metadata. It lists recognized static and registered dynamic privileges, not the effective role closure of the caller. Query SHOW GRANTS/CURRENT_ROLE for authorization state. SHOW PRIVILEGES does not commit or change privileges; a recognized privilege name can still be illegal at a table/column scope.

## Examples

```sql
SHOW PRIVILEGES;
```

## See Also

- [Account and privilege index](README.md)
- [Syntax notation](../syntax-conventions.md)
