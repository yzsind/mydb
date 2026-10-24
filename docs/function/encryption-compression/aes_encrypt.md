# `AES_ENCRYPT`

## Description

Encrypts data using the AES mode configured for the session.

## Syntax

```sql
AES_ENCRYPT(str, key_str [, init_vector [, kdf_name [, salt [, info_or_iterations]]]])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `str` | Yes | String or binary string | Plaintext |
| `key_str` | Yes | String or binary string | Key material |
| `init_vector` | No | At least 16 bytes | Initialization vector for non-ECB modes |
| `kdf_name` | No | hkdf or pbkdf2_hmac | Key derivation method |
| `salt` | No | Fewer than 256 bytes | KDF salt |
| `info_or_iterations` | No | String or integer | HKDF info or PBKDF2 iteration count |

## Return Value

Encrypts data using the AES mode configured for the session. The return type is determined by the function semantics and the input types.

## Examples

```sql
SELECT HEX(AES_ENCRYPT('mydb', 'secret'));
```

Result: returns the hexadecimal representation of the AES ciphertext bytes; the exact value depends on block_encryption_mode.

## Notes

- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
