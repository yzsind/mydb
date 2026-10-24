# `DROP FUNCTION`

## Description

Drops an SQL stored function.

## Syntax

```ebnf
<drop-function> ::= 'DROP FUNCTION' [ 'IF EXISTS' ] <qualified-name>
```

## Behavior and Limitations

- IF EXISTS controls whether a missing object is an error or a Note.
- After the drop, dependent queries report the function as missing when re-resolved; existing persisted data is unchanged.

## Examples

```sql
DROP FUNCTION IF EXISTS add_tax;
```

## See Also

- [Stored programs index](README.md)
- [Syntax conventions](../syntax-conventions.md)
