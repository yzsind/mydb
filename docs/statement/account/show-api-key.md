# `SHOW API KEY`

## Description

Reveals an existing API credential and its metadata.

## Syntax

```ebnf
<show-api-key> ::= 'SHOW API KEY' <string-literal> [ 'FOR' <account> ]
```

## Behavior, Privileges, and Results

Only top-level authenticated MySQL sessions can administer API keys. Accounts manage their own keys; a FOR clause targeting another account requires global `API_KEY_ADMIN`, and system-account protection also applies. Roles and anonymous accounts cannot own keys. REST SQL and stored programs cannot administer keys. See [REST credentials](../../rest-api.md#create-list-reveal-and-revoke-api-keys) for encryption, revocation, and transport requirements.

Revealing requires TLS. The credential is recoverable repeatedly while its encrypted envelope and master key are available. This is a sensitive result, not the plural metadata-only listing.

The result columns, in order, are `key_id`, `name`, `owner`, `created_at`, `expires_at`, `status`, and `api_key`. They are string metadata; expiration is NULL for a non-expiring key. Times are UTC `YYYY-MM-DD HH:MM:SS`; status is `active` or `expired`. The complete api_key credential must be protected as a password.

## Example

```sql
SHOW API KEY 'backend';
```

## See Also

- [API key lifecycle](../../rest-api.md)
- [Account index](README.md)
