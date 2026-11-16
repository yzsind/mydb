# `SHA2`

## Description

Returns the SHA-2 hexadecimal digest with the specified digest length.

## Syntax

```sql
SHA2(str, hash_length)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String | Source string; numeric scalar input is converted to text where accepted. |
| `hash_length` | Yes | Integer | Digest bits: 224, 256, 384 or 512; 0 means 256; unsupported lengths return NULL |

## Return Value

Returns lowercase hexadecimal digest text; NULL input or an unsupported digest length returns NULL.

## Examples

```sql
SELECT SHA2('mydb', 256);
```

Result:

```text
b50d3038fdd554ed9b9d694bc52f73d9899d2576ed6d48899402b5923284edf1
```

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`MD5`](md5.md), [`SHA1`](sha1.md).
