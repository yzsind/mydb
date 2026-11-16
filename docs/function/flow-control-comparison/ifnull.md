# `IFNULL`

## Description

Returns a fallback value when the first argument is `NULL`.

## Syntax

```sql
IFNULL(expr, fallback)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr` | Yes | Any SQL expression | Expression to evaluate. |
| `fallback` | Yes | Any SQL expression | Result expression used only when the first argument is SQL NULL. |

## Return Value

Returns the first argument when non-NULL and otherwise the fallback. The result domain combines both arguments; the example returns text.

## Examples

```sql
SELECT IFNULL(NULL,'fallback'), IFNULL('value','fallback');
```

Result:

```text
fallback	value
```

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`COALESCE`](coalesce.md), [`NULLIF`](nullif.md), [`ISNULL`](isnull.md), [`IF`](if.md).
