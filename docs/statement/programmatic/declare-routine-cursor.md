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

## Context, Errors, and Lifecycle

A routine cursor is read-only and forward-only. Declaration resolves its name/scope; OPEN evaluates the SELECT using current routine inputs. FETCH consumes one row and writes targets; no more rows raises 1329/02000. Closing and reopening reruns the query, unlike a session cursor that opens at DECLARE. See [complete cursor example](../examples.md#complete-stored-cursor-example).

## Examples

```sql
DECLARE c CURSOR FOR SELECT id, total FROM orders ORDER BY id;
```

## See Also

- [Stored programs index](README.md)
- [CREATE PROCEDURE](create-procedure.md)
- [Syntax conventions](../syntax-conventions.md)
