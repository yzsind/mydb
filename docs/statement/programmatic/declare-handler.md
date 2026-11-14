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

## Context, Errors, and Lifecycle

Handlers are selected by matching errno before SQLSTATE before category; a named condition resolves to its underlying errno/SQLSTATE priority. CONTINUE resumes at the continuation of the failing inner statement, not by retrying it. EXIT leaves the declaring block. Re-entering an active handler is not a way to catch itself indefinitely.

NOT FOUND handles cursor exhaustion and no-data SELECT INTO (1329/02000). Use STACKED while handling the condition and CURRENT for the current diagnostics area. UNDO is unsupported at creation (1235). [Complete cursor example](../examples.md#complete-stored-cursor-example) shows declaration ordering and end-of-data control.

## Examples

```sql
DECLARE done BOOLEAN DEFAULT FALSE;
DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)
