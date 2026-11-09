# `KEYRING_KEY_FETCH`

## Description

Reads the specified key from the keyring.

## Syntax

```sql
KEYRING_KEY_FETCH(key_id)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `key_id` | Yes | String | Nonempty process-global key identifier, at most 255 bytes. |

## Return Value

Returns stored key data converted to UTF-8 text, or NULL when the key does not exist or the argument is SQL NULL. Invalid UTF-8 bytes are replaced; this is not an arbitrary-byte export API.

## Examples

```sql
SELECT KEYRING_KEY_FETCH('missing-docs-demo-key');
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

[`KEYRING_KEY_GENERATE`](keyring_key_generate.md), [`KEYRING_KEY_STORE`](keyring_key_store.md), [`KEYRING_KEY_REMOVE`](keyring_key_remove.md), [`KEYRING_KEY_LENGTH`](keyring_key_length.md).
