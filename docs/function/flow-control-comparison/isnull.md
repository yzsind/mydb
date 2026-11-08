# `ISNULL`

## Description

Determines whether the argument is `NULL`.

## Syntax

```sql
ISNULL(value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | Yes | Any SQL expression | Value tested for SQL NULL; JSON null and the text NULL are not SQL NULL. |

## Return Value

Returns integer 1 for SQL NULL and 0 otherwise.

## Examples

```sql
SELECT ISNULL(2);
```

Result:

```text
0
```

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`COALESCE`](coalesce.md), [`IFNULL`](ifnull.md), [`NULLIF`](nullif.md), [`IF`](if.md).
