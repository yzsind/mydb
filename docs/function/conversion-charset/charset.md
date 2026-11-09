# `CHARSET`

## Description

Returns the name of the charset used by a string.

## Syntax

```sql
CHARSET(value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | Yes | Any expression | Expression whose character set is inspected; numeric and binary values use binary. |

## Return Value

Returns a character-set name as text. Numeric values, binary strings and SQL NULL have charset binary; the input need not be numeric.

## Examples

```sql
SELECT CHARSET(2);
```

Result:

```text
binary
```

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`COLLATION`](collation.md), [`COERCIBILITY`](coercibility.md), [`WEIGHT_STRING`](weight_string.md).
