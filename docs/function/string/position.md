# `POSITION`

## Description

Returns the position of the first occurrence of a substring within a string.

## Syntax

```sql
POSITION(substr IN str)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `substr` | Yes | String | The substring to search for |
| `str` | Yes | String | The string to be searched |

## Return Value

Returns a one-based match position, 0 if no match, or SQL NULL for NULL input. POSITION(substr IN str) reverses the parameter order of INSTR(str,substr).

## Examples

```sql
SELECT POSITION('db' IN 'mydb');
```

Result: 3

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

[`LOCATE`](locate.md), [`INSTR`](instr.md), [`FIND_IN_SET`](find_in_set.md), [`STRCMP`](strcmp.md).
