# Data Masking and Keyring Functions

| Function | Description |
| --- | --- |
| [`KEYRING_KEY_FETCH`](keyring_key_fetch.md) | Reads the specified key from the keyring. |
| [`KEYRING_KEY_GENERATE`](keyring_key_generate.md) | Generates a key of the specified type and length in the keyring. |
| [`KEYRING_KEY_LENGTH`](keyring_key_length.md) | Returns the length of the specified key in the keyring. |
| [`KEYRING_KEY_REMOVE`](keyring_key_remove.md) | Removes the specified key from the keyring. |
| [`KEYRING_KEY_STORE`](keyring_key_store.md) | Stores the given key data in the keyring. |
| [`KEYRING_KEY_TYPE`](keyring_key_type.md) | Returns the type of the specified key in the keyring. |
| [`MASK_INNER`](mask_inner.md) | Keeps both ends of a string and masks the middle characters. |
| [`MASK_OUTER`](mask_outer.md) | Masks both ends of a string and keeps the middle characters. |
| [`MASK_PAN`](mask_pan.md) | Masks digits according to payment card number rules. |
| [`MASK_PAN_RELAXED`](mask_pan_relaxed.md) | Masks digits according to relaxed payment card number rules. |
| [`MASK_SSN`](mask_ssn.md) | Masks digits according to social security number rules. |
| [`VALIDATE_PASSWORD_STRENGTH`](validate_password_strength.md) | Returns a password strength score. |

The keyring functions provide the MySQL keyring SQL surface over an in-memory
compatibility backend, and the masking and password-scoring functions provide
presentation-only data masking. The keyring is process-global and volatile:
keys live in server memory, are lost on restart, and are not storage-encryption
or AI-Secret master keys. For encrypted provider credentials, use the separate
[AI Secret interface](../../ai.md#encrypted-api-keys).

## Keyring boundaries

The keyring backend is a compatibility layer, not a protected key store. Its
boundaries are:

- **Lifetime**: keys exist only in process memory and are lost on restart.
- **Isolation**: entries have no per-account ownership; a caller that knows an
  ID can read or remove an entry created by another connection. Do not rely on
  it to isolate credentials between database users.
- **Capacity**: at most 1024 keys; 16 MiB combined IDs, types, and key data;
  64 KiB per key, 255-byte IDs, 64-byte types. Generating or storing a key
  returns an error when a limit is reached; capacity becomes reusable after
  keys are removed. Duplicate insertion is an error.
- **Byte fidelity**: the SQL wrappers convert key data through UTF-8 text.
  STORE can change invalid source bytes and FETCH replaces invalid UTF-8 in
  randomly generated keys, so STORE/FETCH do not promise arbitrary-byte round
  trips; manage binary key encodings in the application.
- **Label validation**: KEYRING_KEY_GENERATE uses floor(bits/8) bytes and
  requires a positive byte count, but does not validate that a label and key
  size match a concrete algorithm.

Examples use temporary IDs and clean up after success. An ID may already exist;
choose a fresh one instead of deleting another application's entry.


## Masking and password scoring

Margins count characters, not encoded bytes. MASK_INNER preserves the end margins;
MASK_OUTER masks them. PAN helpers mask positions without validating card digits,
and MASK_SSN only enforces eleven characters. Masking is presentation, not encryption.

The password heuristic returns 0 below 4 characters, 25 for length 4..7, and at
length 8 or more returns 50 for fewer than two classes, 75 for fewer than three,
otherwise 100. Classes are ASCII lowercase, uppercase, digits and non-alphanumeric
special characters. It is independent of MySQL validate_password components,
dictionaries and password-change enforcement.
