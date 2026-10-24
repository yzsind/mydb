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
- The target type and the ALTER ROUTINE privilege are strictly validated.

## Examples

```sql
ALTER FUNCTION add_tax DETERMINISTIC SQL SECURITY INVOKER;
```

## See Also

- [Stored programs index](README.md)
- [Syntax conventions](../syntax-conventions.md)
