# `KEYRING_KEY_STORE`

## Description

Stores the given key data in the keyring.

## Syntax

```sql
KEYRING_KEY_STORE(key_id, key_type, key_data)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `key_id` | Yes | String | Nonempty process-global key identifier, at most 255 bytes. |
| `key_type` | Yes | String | Stored type label, at most 64 bytes; algorithm validity is not enforced. |
| `key_data` | Yes | String | Key data; see the current text-conversion and isolation limitations in the category reference. |

## Return Value

Returns integer 1 when the key is inserted. Duplicate IDs, empty ID/data and quota violations raise an error; SQL NULL arguments return NULL. The SQL wrapper converts data to text before storing UTF-8 bytes, so arbitrary binary input is not preserved.

## Examples

```sql
SELECT KEYRING_KEY_STORE('docs-demo-key','SECRET','temporary');
SELECT KEYRING_KEY_REMOVE('docs-demo-key');
```

Result:

```text
1
1
```

Use a fresh temporary ID in an isolated instance; this ASCII example does not demonstrate arbitrary-byte fidelity.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.
- Read [current keyring limitations](README.md#current-keyring-limitations) before using this compatibility API. It has no per-account isolation and does not preserve arbitrary non-UTF-8 key bytes.

## Related Functions

[`KEYRING_KEY_GENERATE`](keyring_key_generate.md), [`KEYRING_KEY_FETCH`](keyring_key_fetch.md), [`KEYRING_KEY_REMOVE`](keyring_key_remove.md), [`KEYRING_KEY_LENGTH`](keyring_key_length.md).
