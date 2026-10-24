# `STD`

## Description

Returns the population standard deviation; also usable as a window aggregate function; an alias of `STDDEV_POP`. This function shares the same implementation and semantics as [`STDDEV_POP`](stddev_pop.md).

## Syntax

```sql
STD(expr)
```

## Parameters and Return Value

Arguments, return type, `NULL` behavior, errors, and limitations are identical to [`STDDEV_POP`](stddev_pop.md); refer to the primary function's documentation.

## Examples

```sql
SELECT STD(v) FROM (SELECT 'a' AS k, 1 AS v UNION ALL SELECT 'b', 2) AS t;
```

Result:

```text
0.5
```

## Related Functions

- [`STDDEV_POP`](stddev_pop.md): the primary function with full documentation.
