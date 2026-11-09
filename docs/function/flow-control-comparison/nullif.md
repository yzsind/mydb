# `NULLIF`

## Description

Returns `NULL` when the two arguments are equal, otherwise returns the first argument.

## Syntax

```sql
NULLIF(expr1, expr2)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr1` | Yes | Any SQL expression | First input expression. |
| `expr2` | Yes | Any SQL expression | Second input expression. |

## Return Value

Returns SQL NULL if the arguments compare equal; otherwise returns the first argument with its type. Equality follows the common comparison/coercion rules.

## Examples

```sql
SELECT NULLIF('example', 'example');
```

Result:

```text
NULL
```

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`COALESCE`](coalesce.md), [`IFNULL`](ifnull.md), [`ISNULL`](isnull.md), [`IF`](if.md).
