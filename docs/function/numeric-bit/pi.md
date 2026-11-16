# `PI`

## Description

Returns the value of π.

## Syntax

```sql
PI()
```

## Parameters

This function takes no arguments.

## Return Value

Returns the DOUBLE constant 3.141592653589793. The function takes no arguments; PI(NULL) is an argument-count error.

## Examples

```sql
SELECT PI();
```

Result:

```text
3.141592653589793
```

## Notes

- See [numeric domains and diagnostics](README.md#domains-and-diagnostics) for conversion, domain, and overflow rules.

## Related Functions

[`SIN`](sin.md), [`COS`](cos.md), [`TAN`](tan.md), [`COT`](cot.md).
