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

Returns plaintext as binary bytes using the session block_encryption_mode. SQL NULL data or key returns NULL. Decryption with invalid ciphertext/padding can return NULL; parameter/KDF/IV validation errors raise SQL errors. See the shared AES contract.

## Examples

```sql
SET SESSION block_encryption_mode='aes-128-ecb';
SELECT CONVERT(AES_DECRYPT(AES_ENCRYPT('mydb','secret'),'secret') USING utf8mb4);
```

Result:

```text
mydb
```

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

- See [AES modes and key derivation](README.md#aes-modes-and-key-derivation) for mode/IV/KDF defaults, limits, warnings and decryption failures. Use HEX for ciphertext and an explicit text conversion only when plaintext has that encoding.

## Related Functions

[`AES_ENCRYPT`](aes_encrypt.md), [`RANDOM_BYTES`](random_bytes.md).
