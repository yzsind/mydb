# `RAND`

## Description

Returns a pseudorandom number in the range `[0, 1)`, with an optional seed.

## Syntax

```sql
RAND([seed])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `seed` | No | Numeric | Seed converted to an integer; SQL NULL initializes seed zero. |

## Return Value

Returns a DOUBLE in [0,1). An omitted seed advances the unseeded generator. SQL NULL seed follows seed zero, rather than returning NULL.

## Examples

```sql
SELECT RAND(7);
```

Result: returns a `[0, 1)` value from a reproducible pseudorandom sequence.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- See [numeric domains and diagnostics](README.md#domains-and-diagnostics) for conversion, domain, and overflow rules.

- A constant seed initializes one sequence per call position per statement. Rows advance that sequence; repeating the statement reproduces its first value. A column-dependent seed initializes from that row's seed.
- Bound prepared parameters are statement-constant seeds. Do not assume seeded calls in different expression positions share generator state.
- This generator is not a cryptographic random source.

## Related Functions

[Category reference](README.md).
