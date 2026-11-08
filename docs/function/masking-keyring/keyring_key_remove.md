# `KEYRING_KEY_REMOVE`

## Description

Removes the specified key from the keyring.

## Syntax

```sql
KEYRING_KEY_REMOVE(key_id)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `key_id` | Yes | String | Nonempty process-global key identifier, at most 255 bytes. |

## Return Value

Returns integer 1 when an entry was removed, 0 when absent, or NULL for SQL NULL input. Removal releases its quota charge.

## Examples

```sql
SELECT KEYRING_KEY_REMOVE('missing-docs-demo-key');
```

Result:

```text
0
```

Assumes the example ID is absent. Do not remove another application's key when adapting an example.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.
- Read [current keyring limitations](README.md#current-keyring-limitations) before using this compatibility API. It has no per-account isolation and does not preserve arbitrary non-UTF-8 key bytes.

## Related Functions

[`KEYRING_KEY_GENERATE`](keyring_key_generate.md), [`KEYRING_KEY_STORE`](keyring_key_store.md), [`KEYRING_KEY_FETCH`](keyring_key_fetch.md), [`KEYRING_KEY_LENGTH`](keyring_key_length.md).
