# `COLLATION`

## Description

Returns the name of the collation used by a string.

## Syntax

```sql
COLLATION(value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | Yes | Any expression | Expression whose collation is inspected; numeric and binary values use binary. |

## Return Value

Returns a collation name as text. Numeric values, binary strings and SQL NULL have collation binary.

## Examples

```sql
SELECT COLLATION(2);
```

Result:

```text
binary
```

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`CHARSET`](charset.md), [`COERCIBILITY`](coercibility.md), [`WEIGHT_STRING`](weight_string.md).
