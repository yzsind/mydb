# `AES_DECRYPT`

## Description

Decrypts data using the AES mode configured for the session.

## Syntax

```sql
AES_DECRYPT(crypt_str, key_str [, init_vector [, kdf_name [, salt [, info_or_iterations]]]])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `crypt_str` | Yes | Binary string | Ciphertext |
| `key_str` | Yes | String or binary string | Key material |
| `init_vector` | No | At least 16 bytes | Initialization vector for non-ECB modes |
| `kdf_name` | No | hkdf or pbkdf2_hmac | Key derivation method |
| `salt` | No | Fewer than 256 bytes | KDF salt |
| `info_or_iterations` | No | String or integer | HKDF info or PBKDF2 iteration count |

## Return Value

Decrypts data using the AES mode configured for the session. The return type is determined by the function semantics and the input types.

## Examples

```sql
SELECT AES_DECRYPT(AES_ENCRYPT('mydb', 'secret'), 'secret');
```

Result: mydb

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
