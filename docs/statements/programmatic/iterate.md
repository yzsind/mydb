# `ITERATE`

## Description

Jumps to the next iteration of a named loop.

## Syntax

```ebnf
<iterate> ::= 'ITERATE' <label>
```

## Behavior and Limitations

- The target must be a visible loop label; it cannot refer to a plain BEGIN block.

## Context, Errors, and Lifecycle

The target must be a visible enclosing loop, not a plain BEGIN block or a label in a sibling/handler scope. A missing/invalid label returns 1308. In WHILE the next iteration rechecks its condition; REPEAT uses the executor's loop continuation rather than exposing a general GOTO. Pair ITERATE with a terminating condition or LEAVE. See [complete example](../examples.md#complete-stored-cursor-example).

## Examples

```sql
IF v_skip THEN ITERATE read_loop; END IF;
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)
