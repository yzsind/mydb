# `OPEN`

## Description

Opens a declared stored program cursor.

## Syntax

```ebnf
<open> ::= 'OPEN' <cursor-name>
```

## Behavior and Limitations

- The cursor must already be declared and not open; the SELECT runs at this point and read state is established.

## Context, Errors, and Lifecycle

Opening an already open cursor returns 1325; fetching/closing a declared unopened cursor returns 1326. The query reads the current routine parameter/local values at OPEN and follows the owning transaction's read-view rules. After CLOSE, OPEN starts a fresh scan. If query execution fails, do not assume an initialized fetch state. See [complete example](../examples.md#complete-stored-cursor-example).

## Examples

```sql
OPEN c;
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)
