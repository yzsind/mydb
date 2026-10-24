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
- Multi-component input is validated as implemented; unknown components cannot be silently registered.

## Examples

```sql
INSTALL COMPONENT 'file://component_validate_password';
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
