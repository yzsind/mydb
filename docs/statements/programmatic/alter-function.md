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

## Context, Errors, and Lifecycle

Requires ALTER ROUTINE on the function and implicitly commits preceding user DML. The routine-characteristic vocabulary is in [CREATE PROCEDURE](create-procedure.md). A characteristics change cannot replace parameters, RETURNS or body. Determinism/data-access declarations remain metadata; they do not verify purity. Drop/recreate to replace the body, observing dependency and privilege changes.

## Examples

```sql
ALTER FUNCTION add_tax DETERMINISTIC SQL SECURITY INVOKER;
```

## See Also

- [Stored programs index](README.md)
- [Syntax conventions](../syntax-conventions.md)
