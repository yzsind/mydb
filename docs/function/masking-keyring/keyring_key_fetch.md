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
| `key_id` | Yes | String | The 1st argument; the unique key identifier in the keyring |

## Return Value

Reads the specified key from the keyring. The return type is determined by the function semantics and the input types.

## Examples

```sql
SELECT KEYRING_KEY_FETCH('missing-docs-demo-key');
```

Result: returns NULL when the key does not exist.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.
- Requires an available keyring backend and the corresponding privileges; examples must not contain real production keys.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
