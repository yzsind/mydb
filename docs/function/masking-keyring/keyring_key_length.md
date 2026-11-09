# `KEYRING_KEY_LENGTH`

## Description

Returns the length of the specified key in the keyring.

## Syntax

```sql
KEYRING_KEY_LENGTH(key_id)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `key_id` | Yes | String | Nonempty process-global key identifier, at most 255 bytes. |

## Return Value

Returns the stored data length in bits, not bytes, or NULL for a missing key/SQL NULL argument. Text conversion during STORE can make this differ from the original binary input length.

## Examples

```sql
SELECT KEYRING_KEY_LENGTH('missing-docs-demo-key');
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
