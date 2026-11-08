# `FIELD`

## Description

Returns the position of the first argument within the following argument list.

## Syntax

```sql
FIELD(value, value1 [, value2 [, ...]])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | Yes | Comparable SQL expression | Value to locate; SQL NULL never matches. |
| `value1 ...` | Yes, repeatable | Comparable SQL expressions | Candidates in one-based order; all strings compare as strings, all numbers numerically, and mixed domains use conversion. |

## Return Value

Returns a one-based integer position, or 0 if no candidate matches. NULL search values return 0.

## Examples

```sql
SELECT FIELD('b','a','b'), FIELD('x','a','b'), FIELD(NULL,'a',NULL);
```

Result:

```text
2	0	0
```

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`GREATEST`](greatest.md), [`LEAST`](least.md), [`ELT`](elt.md), [`INTERVAL`](interval.md).
