# `ALTER PROCEDURE`

## Description

Modifies stored procedure characteristics without replacing parameters or the routine body.

## Syntax

```ebnf
<alter-procedure> ::= 'ALTER PROCEDURE' <qualified-name>
                      <routine-characteristic> { <routine-characteristic> }
```

## Behavior and Limitations

- Only explicitly specified characteristics are updated; omitted items keep their current values.
- An error is returned when the target does not exist or is not a procedure; after the catalog is modified, related call caches are invalidated.
- To change parameters or the routine body, DROP and then CREATE again.

## Context, Errors, and Lifecycle

Requires ALTER ROUTINE and implicitly commits preceding user DML. A missing routine or wrong object kind is an error, not a conversion into a procedure. Characteristic syntax is shared with [CREATE PROCEDURE](create-procedure.md). Replacing the body requires DROP/CREATE and can change automatic routine grants; it is not an in-transaction rollbackable edit.

## Examples

```sql
ALTER PROCEDURE transfer SQL SECURITY INVOKER COMMENT 'account transfer';
```

## See Also

- [Stored programs index](README.md)
- [Syntax conventions](../syntax-conventions.md)
