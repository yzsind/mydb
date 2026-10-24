# `DECLARE CURSOR (stored program)`

## Description

Declares a read-only cursor inside a stored program.

## Syntax

```ebnf
<declare-cursor> ::= 'DECLARE' <identifier> 'CURSOR FOR' <select-statement>
```

## Behavior and Limitations

- The declaration does not run the query; the cursor is established at OPEN.
- The number of SELECT result columns must match the FETCH INTO targets; scope is limited to the current block.

## Examples

```sql
DECLARE c CURSOR FOR SELECT id, total FROM orders ORDER BY id;
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)
