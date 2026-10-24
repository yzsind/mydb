# `SHOW TABLE STATUS`

## Description

Returns table-level status such as engine, row count, size, collation, timestamps, and options.

## Syntax

```ebnf
<show-table-status> ::= 'SHOW TABLE STATUS'
                        [ ( 'FROM' | 'IN' ) <identifier> ]
                        [ 'LIKE' <string-literal> | 'WHERE' <expr> ]
```

## Behavior, Privileges, and Results

- Statistics come from the catalog, storage, and current estimates, and are not guaranteed to be exactly in sync with concurrent writes at any instant.
- Compatibility columns without a physical counterpart return NULL/reasonable metadata instead of fabricated status.

## Examples

```sql
SHOW TABLE STATUS FROM app LIKE 'orders%';
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
