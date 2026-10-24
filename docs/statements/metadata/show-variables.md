# `SHOW VARIABLES`

## Description

Shows session or global system variables.

## Syntax

```ebnf
<show-variables> ::= 'SHOW' [ 'GLOBAL' | 'SESSION' | 'LOCAL' ]
                    'VARIABLES'
                    [ 'LIKE' <string-literal> | 'WHERE' <expr> ]
```

## Behavior, Privileges, and Results

- SESSION is the default; LOCAL is a synonym scope for SESSION.
- Values come from the unified sysvar registry/backend; sensitive values are handled according to privileges and display policy.

## Examples

```sql
SHOW VARIABLES LIKE 'mydb_%';
SHOW GLOBAL VARIABLES WHERE Variable_name = 'max_connections';
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
