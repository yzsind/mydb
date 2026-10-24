# `FORMAT_PATH`

## Description

Shortens and formats Performance Schema file paths.

## Syntax

```sql
FORMAT_PATH(path)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `path` | Yes | String | The 1st argument; a JSON path expression |

## Return Value

Shortens and formats Performance Schema file paths. The return type is determined by the function semantics and the input type.

## Examples

```sql
SELECT FORMAT_PATH('/var/lib/mysql/test/t.ibd');
```

Result: returns a shortened path suitable for diagnostic display.

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
