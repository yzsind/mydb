# `INTERVAL`

## Description

Returns the interval index of the first argument within an ordered list of boundaries.

## Syntax

```sql
INTERVAL(n, n1, n2, ...)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `n` | Yes | Convertible numeric expression | Value to classify against ascending boundaries; SQL NULL returns -1. |
| `boundary1` | Yes | Convertible numeric expression | First ascending boundary. |
| `boundary2 ...` | No, repeatable | Convertible numeric expression | Remaining ascending boundaries. |

## Return Value

Returns an integer from 0 through the number of ascending boundaries. A value equal to a boundary belongs to the interval after it. A SQL NULL first argument returns -1. Boundaries must be supplied in ascending order for meaningful classification.

## Examples

```sql
SELECT INTERVAL(0,1,3,7), INTERVAL(3,1,3,7), INTERVAL(9,1,3,7), INTERVAL(NULL,1,3,7);
```

Result:

```text
0	2	3	-1
```

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`GREATEST`](greatest.md), [`LEAST`](least.md), [`FIELD`](field.md), [`ELT`](elt.md).
