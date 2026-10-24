# `FORMAT_PICO_TIME`

## Description

Formats a picosecond duration into human-readable text.

## Syntax

```sql
FORMAT_PICO_TIME(picoseconds)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `picoseconds` | Yes | Integer | The 1st argument; the meaning is determined by the function syntax and description |

## Return Value

Formats a picosecond duration into human-readable text. The return type is determined by the function semantics and the input type.

## Examples

```sql
SELECT FORMAT_PICO_TIME(1000000000000);
```

Result: 1.00 s

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
