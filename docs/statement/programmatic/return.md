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

## Context, Errors, and Lifecycle

RETURN is rejected outside stored-function context. It immediately ends that function, converts the value to RETURNS, and can return SQL NULL where permitted. A conversion error follows the captured SQL mode. A function must have a RETURN syntactically; a runtime path that reaches the end without executing one returns 1321.

## Examples

```sql
RETURN ROUND(p_amount * 1.06, 2);
```

## See Also

- [Stored programs index](README.md)
- [CREATE FUNCTION](create-function.md)
- [Syntax conventions](../syntax-conventions.md)
