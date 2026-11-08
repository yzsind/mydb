# `SHOW API KEYS`

## Description

Lists an account's API keys without revealing credentials.

## Syntax

```ebnf
<show-api-keys> ::= 'SHOW API KEYS' [ 'FOR' <account> ]
```

## Behavior, Privileges, and Results

Only top-level authenticated MySQL sessions can administer API keys. Accounts manage their own keys; a FOR clause targeting another account requires global `API_KEY_ADMIN`, and system-account protection also applies. Roles and anonymous accounts cannot own keys. REST SQL and stored programs cannot administer keys. See [REST credentials](../../rest-api.md#create-list-reveal-and-revoke-api-keys) for encryption, revocation, and transport requirements.

Returns `key_id`, `name`, `owner`, `created_at`, `expires_at`, `status`, all string metadata with nullable expires_at. An account with no keys returns an empty result with these columns. Listing alone does not require the credential-reveal TLS check.

## Example

```sql
SHOW API KEYS;
```

## See Also

- [API key lifecycle](../../rest-api.md)
- [Account index](README.md)
