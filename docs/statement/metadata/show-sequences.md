# `SHOW SEQUENCES`

## Description

Lists the sequence objects in a database.

## Syntax

```ebnf
<show-sequences> ::= 'SHOW SEQUENCES'
                     [ ( 'FROM' | 'IN' ) <identifier> ]
                     [ 'LIKE' <string-literal> | 'WHERE' <expr> ]
```

## Behavior, Privileges, and Results

- This is a mydb sequence extension; the current database is used when the database is omitted.
- The result is filtered by object visibility and does not advance sequence values.

## Result and Visibility Details

The single column is dynamically named Sequences_in_<db>. Results are sorted by name, filtered by schema/object visibility, and LIKE matches that column. The listing does not allocate sequence values.

See [ordered columns and wire types](result-columns.md#show-sequences). Reading this metadata does not itself commit user DML.

## Examples

```sql
SHOW SEQUENCES FROM app LIKE 'invoice%';
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
