# `DECLARE HANDLER`

## Description

Declares a CONTINUE or EXIT condition handler.

## Syntax

```ebnf
<declare-handler> ::= 'DECLARE'
                      ( 'CONTINUE' | 'EXIT' ) 'HANDLER FOR'
                      <handler-condition> { ',' <handler-condition> }
                      <routine-statement>
<handler-condition> ::= 'SQLEXCEPTION' | 'SQLWARNING' | 'NOT FOUND'
                        | <unsigned-integer>
                        | 'SQLSTATE' [ 'VALUE' ] <string-literal>
                        | <condition-name>
```

## Behavior and Limitations

- CONTINUE resumes after the handler; EXIT leaves the block that declares it. UNDO handlers are unsupported and return 1235 at routine creation.
- Handler selection resolves by priority from a specific errno/SQLSTATE/named condition to its category.
- STACKED diagnostics are available only while a handler executes.

## Examples

```sql
DECLARE done BOOLEAN DEFAULT FALSE;
DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)
