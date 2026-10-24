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
| `key_id` | Yes | String | The 1st argument; the unique key identifier in the keyring |
| `key_type` | Yes | String | The 2nd argument; the key type name |
| `key_data` | Yes | String | The 3rd argument; the key bytes to store |

## Return Value

Stores the given key data in the keyring. The return type is determined by the function semantics and the input types.

## Examples

```sql
SELECT KEYRING_KEY_STORE('docs-demo-key', 'SECRET', 'temporary');
SELECT KEYRING_KEY_REMOVE('docs-demo-key');
```

Result: returns 1 on success; the second statement immediately removes the example key.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.
- Requires an available keyring backend and the corresponding privileges; examples must not contain real production keys.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
