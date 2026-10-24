# `ITERATE`

## Description

Jumps to the next iteration of a named loop.

## Syntax

```ebnf
<iterate> ::= 'ITERATE' <label>
```

## Behavior and Limitations

- The target must be a visible loop label; it cannot refer to a plain BEGIN block.

## Examples

```sql
IF v_skip THEN ITERATE read_loop; END IF;
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)
