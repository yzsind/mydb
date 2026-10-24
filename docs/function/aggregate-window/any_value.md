# `ANY_VALUE`

## Description

Returns its argument unchanged; the current implementation does not exempt ungrouped columns from `ONLY_FULL_GROUP_BY`.

## Syntax

```sql
ANY_VALUE(expr)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | Any SQL expression | The 1st argument; the expression to evaluate |

## Return Value

Returns its argument unchanged, including its type and SQL NULL. ANY_VALUE
alone is a scalar identity, not an aggregate: an empty source produces no
output rows. The current grouping validator does not implement MySQL's
ONLY_FULL_GROUP_BY exemption; an otherwise ungrouped column inside ANY_VALUE
still returns 1055. It is not a window function.

## Examples

```sql
SELECT ANY_VALUE(42), ANY_VALUE(NULL);
```

Result:

```text
42	NULL
```

## Notes

- Use ordinary grouping or a proven functional dependency when ONLY_FULL_GROUP_BY requires it; wrapping a column in ANY_VALUE does not bypass validation in this implementation.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
