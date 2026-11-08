# `WEIGHT_STRING`

## Description

Returns the comparison weight string of a string under its collation.

## Syntax

```sql
WEIGHT_STRING(str [AS {CHAR|BINARY}(N)])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | Text, binary string or numeric expression | Value whose sortable comparison bytes are requested. |
| `N` | Only with AS | Positive integer | CHAR counts characters and pads with spaces; BINARY counts bytes and pads with zeros. |

## Return Value

Returns binary comparison-weight bytes, or SQL NULL for SQL NULL input. Text uses its collation; binary input uses bytes. Numeric input uses a fixed-width sortable numeric key rather than the textual digits. The representation is a comparison aid, not a persistent cross-version key format.

## Examples

```sql
SELECT HEX(WEIGHT_STRING('A' AS BINARY(2)));
```

Result:

```text
4100
```

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

- AS CHAR(N) truncates/pads character content before weighting; AS BINARY(N) truncates/pads bytes. N must be positive. The LEVEL modifier is not implemented and is rejected. Use HEX to inspect the binary result.

## Related Functions

[`CHARSET`](charset.md), [`COLLATION`](collation.md), [`COERCIBILITY`](coercibility.md).
