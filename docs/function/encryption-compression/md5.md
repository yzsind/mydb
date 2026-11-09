# `MD5`

## Description

Returns the MD5 hexadecimal digest of the input.

## Syntax

```sql
MD5(value)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `value` | Yes | String or binary string; convertible scalar where accepted | Input bytes/text; numeric scalars are converted to their display bytes. |

## Return Value

Returns a lowercase 32-character hexadecimal MD5 digest. SQL NULL returns NULL. The digest is over input bytes, not a normalized Unicode representation.

## Examples

```sql
SELECT MD5('mydb');
```

Result:

```text
e04d88072f90f86a07481418b8ff4b6b
```

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`SHA1`](sha1.md), [`SHA2`](sha2.md).
