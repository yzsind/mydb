# `COERCIBILITY`

## Description

Returns the collation coercibility value of a string expression.

## Syntax

```sql
COERCIBILITY(value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | Yes | Any expression | Expression whose collation precedence is inspected. |

## Return Value

Returns an integer collation-coercibility level from 0 through 6; lower values have stronger precedence. See the category table. This is expression metadata, so COERCIBILITY(NULL) returns 6 rather than SQL NULL.

## Examples

```sql
SELECT COERCIBILITY(2);
```

Result:

```text
5
```

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`CHARSET`](charset.md), [`COLLATION`](collation.md), [`WEIGHT_STRING`](weight_string.md).
