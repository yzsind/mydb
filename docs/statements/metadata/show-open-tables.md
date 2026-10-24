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

## Examples

```sql
SHOW OPEN TABLES FROM app WHERE In_use > 0;
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
