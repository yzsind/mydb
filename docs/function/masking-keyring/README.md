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


## Current keyring limitations

The backend is a process-global, volatile compatibility map, not a protected key
store. SQL callers currently have no per-account ownership/isolation: a caller
that knows an ID can read or delete an entry created by another connection.
Keys are lost on restart and are not storage-encryption or AI-Secret master keys.
Do not use it to isolate production credentials between database users.

The SQL wrappers also convert key data through UTF-8 text. STORE can change invalid
source bytes, and FETCH replaces invalid UTF-8 from random generated keys. Therefore
STORE/FETCH do not promise arbitrary-byte round trips. Hexadecimal-looking text is
still text unless the application manages its own encoding; it does not repair
server-side isolation or persistence.

Generation length and KEYRING_KEY_LENGTH are in bits. The current generator uses
floor(bits/8) bytes and requires a positive resulting byte count; it accepts labels
without complete algorithm/key-size validation. A successful call is not proof of
an algorithm-appropriate key. Limits are 1024 entries, 16 MiB combined IDs/types/data,
255-byte IDs, 64-byte type labels and 64 KiB per key data. Duplicate insertion and
quota violations are errors; removal releases capacity.

Examples use temporary IDs and clean up after success. An ID may already exist;
choose a fresh one instead of deleting another application's entry. For encrypted
provider credentials use the separate [AI Secret interface](../../ai.md#encrypted-api-keys).


## Masking and password scoring

Margins count characters, not encoded bytes. MASK_INNER preserves the end margins;
MASK_OUTER masks them. PAN helpers mask positions without validating card digits,
and MASK_SSN only enforces eleven characters. Masking is presentation, not encryption.

The password heuristic returns 0 below 4 characters, 25 for length 4..7, and at
length 8 or more returns 50 for fewer than two classes, 75 for fewer than three,
otherwise 100. Classes are ASCII lowercase, uppercase, digits and non-alphanumeric
special characters. It is independent of MySQL validate_password components,
dictionaries and password-change enforcement.
