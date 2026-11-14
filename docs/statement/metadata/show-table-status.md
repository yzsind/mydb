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
- Unavailable physical values are NULL; compatibility fields describe the native engine catalog rather than a distinct InnoDB implementation.

## Result and Visibility Details

LIKE matches Name. Rows and size fields are catalog/storage estimates. Base-table lengths use the common table statistics view; a view has no heap/index statistics and returns NULL for physical fields. Missing physical timestamps/auto-increment/checksum values remain NULL. Do not treat Data_free as space guaranteed reclaimable by one OPTIMIZE.

See [ordered columns and wire types](result-columns.md#show-table-status). Reading this metadata does not itself commit user DML.

## Examples

```sql
SHOW TABLE STATUS FROM app LIKE 'orders%';
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
