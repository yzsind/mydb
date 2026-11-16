# `INSTALL COMPONENT`

## Description

Installs supported built-in components.

## Syntax

```ebnf
<install-component> ::= 'INSTALL COMPONENT' <string-literal>
                        { ',' <string-literal> }
```

## Behavior, Privileges, and Limitations

- The current implementation supports the validate_password component; other names return an unsupported error.
- Component names use the file://component_name form; installation updates the component registry and affects subsequent password validation.
- Exactly one supported component is allowed per statement; duplicates and unknown names fail before component publication.

## Privileges, Transaction, and Result Details

Requires global SYSTEM_VARIABLES_ADMIN and implicitly commits preceding DML. Only one validate_password component name is accepted per statement; unknown names, duplicate entries and already-installed state fail. Successful installation persists the catalog flag and enables subsequent password-policy checks. Inspect validate_password.* system variables for the current rules. This is a built-in component, not a shared-library installation API.

## Examples

```sql
INSTALL COMPONENT 'file://component_validate_password';
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
