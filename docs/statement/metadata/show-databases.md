# `SHOW DATABASES / SCHEMAS`

## Description

Lists the databases visible to the current account.

## Syntax

```ebnf
<show-databases> ::= 'SHOW' ( 'DATABASES' | 'SCHEMAS' )
                    [ 'LIKE' <string-literal> | 'WHERE' <expr> ]
```

## Behavior, Privileges, and Results

- Privilege filtering is MySQL-compatible: without a global view privilege, only accessible schemas are shown.
- LIKE uses an SQL pattern; WHERE is evaluated against the result columns.

## Result and Visibility Details

With global SHOW DATABASES, all listable schemas are visible; otherwise the list is restricted to accessible schemas. LIKE matches Database. An empty filtered list is a successful zero-row result, not an authentication failure.

See [ordered columns and wire types](result-columns.md#show-databases). Reading this metadata does not itself commit user DML.

## Examples

```sql
SHOW DATABASES;
SHOW DATABASES LIKE 'app%';
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
