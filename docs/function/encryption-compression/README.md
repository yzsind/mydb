# Encryption, Hashing, and Compression Functions

| Function | Description |
| --- | --- |
| [`AES_DECRYPT`](aes_decrypt.md) | Decrypts data using the AES mode configured for the session. |
| [`AES_ENCRYPT`](aes_encrypt.md) | Encrypts data using the AES mode configured for the session. |
| [`COMPRESS`](compress.md) | Compresses the input string and returns binary data. |
| [`FROM_BASE64`](from_base64.md) | Decodes Base64 text. |
| [`MD5`](md5.md) | Returns the MD5 hexadecimal digest of the input. |
| [`RANDOM_BYTES`](random_bytes.md) | Returns cryptographically random bytes of the specified length. |
| [`SHA`](sha.md) | Returns the SHA-1 hexadecimal digest of the input, an alias of `SHA1`. |
| [`SHA1`](sha1.md) | Returns the SHA-1 hexadecimal digest of the input. |
| [`SHA2`](sha2.md) | Returns the SHA-2 hexadecimal digest with the specified digest length. |
| [`TO_BASE64`](to_base64.md) | Encodes the input bytes as Base64 text. |
| [`UNCOMPRESS`](uncompress.md) | Uncompresses data produced by `COMPRESS`. |
| [`UNCOMPRESSED_LENGTH`](uncompressed_length.md) | Returns the original length recorded in compressed data. |


## AES modes and key derivation

block_encryption_mode defaults to aes-128-ecb. AES widths 128, 192 and 256 accept
ecb, cbc, cfb1, cfb8, cfb128, ofb and the mydb ctr extension. Match the mode, key
material, IV and KDF options on decryption. ECB/CBC use block padding; stream-like
modes do not imply ciphertext authentication. An AES round trip is not a password
storage scheme or proof that corrupted plaintext will always be detected.

Non-ECB modes require an IV of at least 16 bytes and use its first 16 bytes.
ECB ignores an ordinary supplied IV with a warning. KDF syntax still reserves the
third argument for an IV placeholder; an empty/NULL placeholder is meaningful for
ECB and does not satisfy the IV requirement of other modes.

| KDF | Defaults and constraints |
| --- | --- |
| Omitted | MySQL-compatible folding of key bytes to the active AES width |
| hkdf | SHA-512 HKDF, empty salt and info when omitted |
| pbkdf2_hmac | SHA-512 PBKDF2, empty salt; iterations default 1000, accepted 1000..65535 |

KDF names are the literal lowercase names above. Supplied KDF/salt/info/iteration
options must not be SQL NULL and must be shorter than 256 bytes. Unknown names,
invalid IVs and invalid iteration counts are errors. Data/key SQL NULL returns
NULL. Bad decrypt padding/ciphertext returns NULL; a wrong key in an unauthenticated
mode can also yield bytes, so validate the application payload independently.


## Bytes encoding and compression

Hashes and codecs operate on bytes; text encoding affects the result. MD5/SHA1
are legacy compatibility digests, not password hashing or collision-resistant
security choices. SHA2 supports 224/256/384/512 bits (0 means 256). RANDOM_BYTES
returns bytes and takes a byte count; Base64 is encoding, not encryption.

COMPRESS uses a MySQL-compatible length header plus zlib. UNCOMPRESS bounds the
advertised size at 64 MiB and validates the decoded size; a length header is not
proof of a valid stream. UNCOMPRESSED_LENGTH only reads it. NULL and empty bytes
are distinct. Use HEX for arbitrary binary results and CONVERT(... USING charset)
only when the bytes are known to be encoded text. Packet/memory/cancellation
limits still apply to SQL expression evaluation.
