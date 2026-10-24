# `SHOW COLUMNS`

## Description

Shows the column definitions of a table or view; FIELDS is a synonym.

## Syntax

```ebnf
<show-columns> ::= 'SHOW' [ 'FULL' ]
                  ( 'COLUMNS' | 'FIELDS' )
                  ( 'FROM' | 'IN' ) <qualified-name>
                  [ ( 'FROM' | 'IN' ) <identifier> ]
                  [ 'LIKE' <string-literal> | 'WHERE' <expr> ]
```

## Behavior, Privileges, and Results

- FULL adds columns such as collation, privileges, and comment.
- The target can be a temporary table or a view; a nonexistent target returns 1146.

## Examples

```sql
SHOW FULL COLUMNS FROM app.customer LIKE 'email';
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
