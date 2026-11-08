# `FIND_IN_SET`

## Description

Returns the position of a string within a comma-separated set.

## Syntax

```sql
FIND_IN_SET(str, strlist)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | Source string; numeric scalar input is converted to text where accepted. |
| `strlist` | Yes | String | Comma-separated candidate list; positions start at 1. |

## Return Value

Returns a one-based integer position in the comma-separated list, 0 for no match or an empty list, and SQL NULL for SQL NULL input. A search item containing a comma cannot be represented as a single list member.

## Examples

```sql
SELECT FIND_IN_SET('mydb', 'mydb');
```

Result:

```text
1
```

## Notes

- See [text and binary semantics](README.md#text-and-binary-semantics) for encoding, indexing and result-size limits.

## Related Functions

[`LOCATE`](locate.md), [`INSTR`](instr.md), [`POSITION`](position.md), [`STRCMP`](strcmp.md).
