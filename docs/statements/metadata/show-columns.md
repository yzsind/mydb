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

## Result and Visibility Details

Requires SELECT on the object; individual visible columns follow column grants. FULL inserts Collation after Type and appends Privileges and Comment. Invisible and generated columns are still metadata columns; Extra identifies their attributes. LIKE matches Field. A session temporary table shadows a persistent table for object lookup.

See [ordered columns and wire types](result-columns.md#show-columns). Reading this metadata does not itself commit user DML.

## Examples

```sql
SHOW FULL COLUMNS FROM app.customer LIKE 'email';
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
