# `IF`

## Description

Returns one of two candidate values depending on a condition.

## Syntax

```sql
IF(condition, value_if_true, value_if_false)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `condition` | Yes | Convertible numeric expression | Boolean/numeric condition; SQL NULL selects the false branch. |
| `value_if_true` | Yes | Any SQL expression | Result expression used when the condition is true. |
| `value_if_false` | Yes | Any SQL expression | Result expression used when the condition is false. |

## Return Value

Returns the true branch when the condition is true, otherwise the false branch; NULL conditions select the false branch. The result domain combines the two branches, and only the selected result branch is evaluated.

## Examples

```sql
SELECT IF(1,'yes','no'), IF(0,'yes','no'), IF(NULL,'yes','no');
```

Result:

```text
yes	no	no
```

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`COALESCE`](coalesce.md), [`IFNULL`](ifnull.md), [`NULLIF`](nullif.md), [`ISNULL`](isnull.md).
