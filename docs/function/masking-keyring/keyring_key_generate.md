# `KEYRING_KEY_GENERATE`

## Description

Generates a key of the specified type and length in the keyring.

## Syntax

```sql
KEYRING_KEY_GENERATE(key_id, key_type, length)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `key_id` | Yes | String | Nonempty process-global key identifier, at most 255 bytes. |
| `key_type` | Yes | String | Stored type label, at most 64 bytes; algorithm validity is not enforced. |
| `length` | Yes | Integer | Requested key length in bits; use 128 or 256 for an AES-sized example. |

## Return Value

Returns integer 1 when random key data is inserted; a duplicate/invalid ID or quota violation raises an error. SQL NULL arguments return NULL. Length is in bits and the current backend allocates floor(length/8) bytes; it does not enforce every algorithm-specific valid length.

## Examples

```sql
SELECT KEYRING_KEY_GENERATE('docs-demo-key','AES',256);
SELECT KEYRING_KEY_LENGTH('docs-demo-key');
SELECT KEYRING_KEY_REMOVE('docs-demo-key');
```

Result:

```text
1
256
1
```

Use an unused ID on an isolated test instance. The example removes its entry; it does not claim safe export of generated random bytes.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.
- Read [current keyring limitations](README.md#current-keyring-limitations) before using this compatibility API. It has no per-account isolation and does not preserve arbitrary non-UTF-8 key bytes.

## Related Functions

[`KEYRING_KEY_STORE`](keyring_key_store.md), [`KEYRING_KEY_FETCH`](keyring_key_fetch.md), [`KEYRING_KEY_REMOVE`](keyring_key_remove.md), [`KEYRING_KEY_LENGTH`](keyring_key_length.md).
