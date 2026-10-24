# `RENAME USER`

## Description

Atomically renames one or more account identifiers.

## Syntax

```ebnf
<rename-user> ::= 'RENAME USER' <account> 'TO' <account>
                  { ',' <account> 'TO' <account> }
```

## Behavior and Limitations

- The source must exist and the target must not; all pairs are prechecked and then committed in one step.
- Account attributes, role edges, direct grants, and authentication data migrate with the name.

## Examples

```sql
RENAME USER 'app'@'localhost' TO 'app'@'10.%';
```

## See Also

- [Account and privilege index](README.md)
- [Syntax notation](../syntax-conventions.md)
