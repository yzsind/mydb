# `FORMAT_TIME`

## Description

Formats a picosecond duration into human-readable text; a compatibility function for `FORMAT_PICO_TIME`.

## Syntax

```sql
FORMAT_TIME(picoseconds)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `picoseconds` | Yes | Integer | The 1st argument; the meaning is determined by the function syntax and description |

## Return Value

Formats a picosecond duration into human-readable text; a compatibility function for `FORMAT_PICO_TIME`. The return type is determined by the function semantics and the input type.

## Examples

```sql
SELECT FORMAT_TIME(1000000000000);
```

Result: `1 s`; FORMAT_TIME strips the `.00` fraction retained by FORMAT_PICO_TIME.

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
