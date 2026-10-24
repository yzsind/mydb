# `OPEN`

## Description

Opens a declared stored program cursor.

## Syntax

```ebnf
<open> ::= 'OPEN' <cursor-name>
```

## Behavior and Limitations

- The cursor must already be declared and not open; the SELECT runs at this point and read state is established.

## Examples

```sql
OPEN c;
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)
