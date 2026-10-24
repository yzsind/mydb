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
| `str` | Yes | String | The 1st argument; the string to process |
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

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
