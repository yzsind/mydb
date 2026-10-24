# `UNINSTALL COMPONENT`

## Description

Uninstalls supported installed built-in components.

## Syntax

```ebnf
<uninstall-component> ::= 'UNINSTALL COMPONENT' <string-literal>
                          { ',' <string-literal> }
```

## Behavior, Privileges, and Limitations

- Uninstalling validate_password stops subsequent password strength enforcement; existing password hashes are unchanged.
- Not-installed or unknown components return an explicit diagnostic.

## Examples

```sql
UNINSTALL COMPONENT 'file://component_validate_password';
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
