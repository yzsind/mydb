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

## Privileges and Result Details

Requires CREATE USER plus system-account protection where applicable, and implicitly commits preceding DML. Source/destination duplicates and an already occupied destination are rejected before mutation; this is not a SQL account-name swap operation. Direct grants, role/default edges, restrictions, runtime quota identity and API key ownership follow the account.

Definer and RLS account dependencies can block rename; it must not be interpreted as rewriting every stored DEFINER string. Existing connections have their own authenticated identity and authorization revalidation. Inspect SHOW GRANTS/SHOW CREATE USER on the new exact account after success.

## Examples

```sql
RENAME USER 'app'@'localhost' TO 'app'@'10.%';
```

## See Also

- [Account and privilege index](README.md)
- [Syntax notation](../syntax-conventions.md)
