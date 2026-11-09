# `ALTER FUNCTION`

## Description

Modifies stored function characteristics.

## Syntax

```ebnf
<alter-function> ::= 'ALTER FUNCTION' <qualified-name>
                     <routine-characteristic> { <routine-characteristic> }
```

## Behavior and Limitations

- Only explicitly listed characteristics are updated; parameters, RETURNS, and the routine body cannot be changed.
- To replace parameters, RETURNS, or the routine body, use CREATE OR REPLACE FUNCTION (a mydb extension), or DROP and then CREATE again.
- The target type and the ALTER ROUTINE privilege are strictly validated.

## Examples

```sql
ALTER FUNCTION add_tax DETERMINISTIC SQL SECURITY INVOKER;
```

## See Also

- [CREATE FUNCTION](create-function.md)
- [Stored programs index](README.md)
- [Syntax conventions](../syntax-conventions.md)
