# `SHOW PROCEDURE / FUNCTION STATUS`

## Description

Lists the status of visible stored procedures or functions.

## Syntax

```ebnf
<show-routine-status> ::= 'SHOW'
                          ( 'PROCEDURE' | 'FUNCTION' ) 'STATUS'
                          [ 'LIKE' <string-literal> | 'WHERE' <expr> ]
```

## Behavior, Privileges, and Results

- Returns compatibility columns such as Db, Name, Type, Definer, Modified, Created, Security_type, and Comment.
- LIKE/WHERE are applied to the privilege-filtered result.

## Examples

```sql
SHOW PROCEDURE STATUS WHERE Db = 'app';
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
