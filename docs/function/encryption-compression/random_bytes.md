# `RANDOM_BYTES`

## Description

Returns cryptographically random bytes of the specified length.

## Syntax

```sql
RANDOM_BYTES(len)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `len` | Yes | Integer 1..1024 | Requested byte count, not bit count. |

## Return Value

Returns binary random data of exactly len bytes, where len is 1..1024. SQL NULL returns NULL; an out-of-range length raises 1690/22003.

## Examples

```sql
SELECT LENGTH(RANDOM_BYTES(16));
```

Result: 16

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`AES_ENCRYPT`](aes_encrypt.md), [`AES_DECRYPT`](aes_decrypt.md).
