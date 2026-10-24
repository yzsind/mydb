# `SHOW TABLES`

## Description

Lists the visible tables and views in a database.

## Syntax

```ebnf
<show-tables> ::= 'SHOW' [ 'FULL' ] 'TABLES'
                 [ ( 'FROM' | 'IN' ) <identifier> ]
                 [ 'LIKE' <string-literal> | 'WHERE' <expr> ]
```

## Behavior, Privileges, and Results

- The current database is used when the database is omitted; FULL adds Table_type.
- Session temporary tables are presented according to name resolution and visibility rules.

## Examples

```sql
SHOW FULL TABLES FROM app WHERE Table_type = 'BASE TABLE';
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
