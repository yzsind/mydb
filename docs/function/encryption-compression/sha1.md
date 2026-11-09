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
| `value` | Yes | String or binary string; convertible scalar where accepted | Input bytes/text; numeric scalars are converted to their display bytes. |

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

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`SHA`](sha.md).
