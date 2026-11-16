# `SHOW OPEN TABLES`

## Description

Shows a compatibility view of the table cache/open state.

## Syntax

```ebnf
<show-open-tables> ::= 'SHOW OPEN TABLES'
                       [ ( 'FROM' | 'IN' ) <identifier> ]
                       [ 'LIKE' <string-literal> | 'WHERE' <expr> ]
```

## Behavior, Privileges, and Results

- Returns columns such as Database, Table, In_use, and Name_locked.
- The state is a snapshot taken at read time and cannot replace metadata_locks diagnostics.

## Result and Visibility Details

This is currently a catalog-based listing of visible persistent base tables. In_use and Name_locked are both 0 because mydb does not maintain MySQL table-cache use/name-lock counters. They are not live lock counts. Inspect performance_schema.metadata_locks for actual MDL. LIKE matches Table.

See [ordered columns and wire types](result-columns.md#show-open-tables). Reading this metadata does not itself commit user DML.

## Examples

```sql
SHOW OPEN TABLES FROM app WHERE In_use > 0;
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
