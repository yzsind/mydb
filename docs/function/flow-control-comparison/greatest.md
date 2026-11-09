# `GREATEST`

## Description

Returns the greatest value in the argument list.

## Syntax

```sql
GREATEST(value1, value2 [, ...])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value1` | Yes | Comparable expression | The first candidate value |
| `value2` | Yes, repeatable | Comparable expression | The remaining candidate values |

## Return Value

Returns the greatest candidate using the common comparison domain and collation. Any SQL NULL candidate returns NULL; the result type is derived from the candidate domains.

## Examples

```sql
SELECT GREATEST(2, 2);
```

Result:

```text
2
```

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`LEAST`](least.md), [`FIELD`](field.md), [`ELT`](elt.md), [`INTERVAL`](interval.md).
