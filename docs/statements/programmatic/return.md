# `RETURN`

## Description

Ends a stored function and returns the expression value.

## Syntax

```ebnf
<return> ::= 'RETURN' <expr>
```

## Behavior and Limitations

- Allowed only in function context; procedures should use LEAVE or end naturally.
- The return value is converted to the declared RETURNS type; conversion violations follow sql_mode.

## Examples

```sql
RETURN ROUND(p_amount * 1.06, 2);
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)
