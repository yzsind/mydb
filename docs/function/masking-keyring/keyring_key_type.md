# `KEYRING_KEY_TYPE`

## Description

Returns the type of the specified key in the keyring.

## Syntax

```sql
KEYRING_KEY_TYPE(key_id)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `key_id` | Yes | String | Nonempty process-global key identifier, at most 255 bytes. |

## Return Value

Returns the stored type-label text, or NULL for an absent key/SQL NULL argument. The label alone does not validate cryptographic algorithm/key-size suitability.

## Examples

```sql
SELECT KEYRING_KEY_TYPE('missing-docs-demo-key');
```

Result:

```text
NULL
```

Assumes this temporary example ID is absent from the process-global map.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.
- Read [current keyring limitations](README.md#current-keyring-limitations) before using this compatibility API. It has no per-account isolation and does not preserve arbitrary non-UTF-8 key bytes.

## Related Functions

[`KEYRING_KEY_GENERATE`](keyring_key_generate.md), [`KEYRING_KEY_STORE`](keyring_key_store.md), [`KEYRING_KEY_FETCH`](keyring_key_fetch.md), [`KEYRING_KEY_REMOVE`](keyring_key_remove.md).
