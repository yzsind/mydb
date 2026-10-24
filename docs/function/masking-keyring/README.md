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

Currently the keyring exists only in process memory, and keys are lost after a restart. At most 1024 keys are kept; the combined size of IDs, types, and key data is limited to 16 MiB. A single key's data is limited to 64 KiB, an ID to 255 bytes, and a type to 64 bytes. Generating or storing a key returns an error when a limit is reached; capacity becomes reusable after keys are removed.
