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
| `key_id` | Yes | String | The 1st argument; the unique key identifier in the keyring |
| `key_type` | Yes | String | The 2nd argument; the key type name |
| `length` | Yes | Integer | The 3rd argument; the meaning is determined by the function syntax and description |

## Return Value

Generates a key of the specified type and length in the keyring. The return type is determined by the function semantics and the input types.

## Examples

```sql
SELECT KEYRING_KEY_GENERATE('docs-demo-key', 'AES', 16);
SELECT KEYRING_KEY_REMOVE('docs-demo-key');
```

Result: returns 1 on success; the second statement immediately removes the example key.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Returns a clear MySQL-compatible error when the number or type of arguments does not match the syntax.
- Requires an available keyring backend and the corresponding privileges; examples must not contain real production keys.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
