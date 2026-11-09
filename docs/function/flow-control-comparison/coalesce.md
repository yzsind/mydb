# `COALESCE`

## Description

Returns the first non-`NULL` value in the argument list.

## Syntax

```sql
COALESCE(expr1 [, expr2 [, ...]])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `expr1` | Yes | Any SQL expression | First input expression. |
| `expr2 ...` | No, repeatable | Any SQL expression | Additional expressions considered from left to right. |

## Return Value

Returns the first non-NULL argument; all-NULL input returns NULL. The declared result type combines the argument domains; the example returns text.

## Examples

```sql
SELECT COALESCE(NULL,'fallback'), COALESCE(NULL,NULL);
```

Result:

```text
fallback	NULL
```

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`IFNULL`](ifnull.md), [`NULLIF`](nullif.md), [`ISNULL`](isnull.md), [`IF`](if.md).
