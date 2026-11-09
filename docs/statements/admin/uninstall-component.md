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

## Privileges, Transaction, and Result Details

Requires global SYSTEM_VARIABLES_ADMIN and implicitly commits preceding DML. Only the supported validate_password component may be uninstalled, and it must be installed. Multiple/duplicate/unknown names fail validation. Success persists the disabled component flag; it does not rehash existing passwords or retroactively strengthen/weaken their stored bytes.

## Examples

```sql
UNINSTALL COMPONENT 'file://component_validate_password';
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
