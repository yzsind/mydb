# `SHA1`

## Description

Returns the SHA-1 hexadecimal digest of the input.

## Syntax

```sql
SHA1(value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | Yes | Numeric or integer | The 1st argument; the input value |

## Return Value

Returns lowercase hexadecimal SHA-1 digest text; NULL input returns NULL.

## Examples

```sql
SELECT SHA1(2);
```

Result:

```text
da4b9237bacccdf19c0760cab7aec4a8359010b0
```

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

[`SHA`](sha.md).
