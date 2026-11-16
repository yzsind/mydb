# `RESIGNAL`

## Description

Re-raises the current condition inside a handler, optionally overriding diagnostic items.

## Syntax

```ebnf
<resignal> ::= 'RESIGNAL' [ <condition-value> ]
               [ 'SET' <signal-item> { ',' <signal-item> } ]
```

## Behavior and Limitations

- With the condition omitted, the current handled condition is preserved; it can be used only within a valid handler context.

## Context, Errors, and Lifecycle

Outside an active handler RESIGNAL returns 1645. With no overrides, it propagates the original condition. With SET it preserves unspecified information and replaces the specified fields; an explicit SQLSTATE changes the condition identity and default errno unless MYSQL_ERRNO is supplied. Supported information fields and validation are the same as [SIGNAL](signal.md). The handler continuation/exit rules still determine where execution can resume.

## Examples

```sql
RESIGNAL SET MESSAGE_TEXT = 'transfer failed';
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)
