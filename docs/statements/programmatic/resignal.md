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

## Examples

```sql
RESIGNAL SET MESSAGE_TEXT = 'transfer failed';
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)
