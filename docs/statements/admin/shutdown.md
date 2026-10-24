# `SHUTDOWN`

## Description

Requests a controlled server shutdown.

## Syntax

```ebnf
<shutdown> ::= 'SHUTDOWN'
```

## Behavior, Privileges, and Limitations

- Requires the SHUTDOWN privilege; stops accepting connections and follows the controlled flush/exit path.
- This interrupts service; the example is intended for operations environments only and is not executed in the general documentation verification instance.

## Examples

```sql
SHUTDOWN;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
