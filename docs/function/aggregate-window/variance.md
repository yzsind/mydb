# `VARIANCE`

## Description

Returns the population variance; also usable as a window aggregate function; an alias of `VAR_POP`. This function shares the same implementation and semantics as [`VAR_POP`](var_pop.md).

## Syntax

```sql
VARIANCE(expr)
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`VAR_POP`](var_pop.md); refer to the primary function's documentation.

## Examples

```sql
SELECT VARIANCE(v) FROM (SELECT 'a' AS k, 1 AS v UNION ALL SELECT 'b', 2) AS t;
```

Result:

```text
0.25
```

## Related Functions

- [`VAR_POP`](var_pop.md): the primary function with full documentation.
