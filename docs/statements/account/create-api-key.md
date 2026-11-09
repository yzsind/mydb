# `CREATE API KEY`

## Description

Creates a REST credential owned by an account.

## Syntax

```ebnf
<create-api-key> ::= 'CREATE API KEY' <string-literal>
                      [ 'FOR' <account> ]
                      [ 'EXPIRES AT' <string-literal> ]
```

## Behavior, Privileges, and Results

Only top-level authenticated MySQL sessions can administer API keys. Accounts manage their own keys; a FOR clause targeting another account requires global `API_KEY_ADMIN`, and system-account protection also applies. Roles and anonymous accounts cannot own keys. REST SQL and stored programs cannot administer keys. See [REST credentials](../../rest-api.md#create-list-reveal-and-revoke-api-keys) for encryption, revocation, and transport requirements.

Creation requires TLS. Names are unique per owner and must contain 1 through 128 UTF-8 bytes. Each account can retain at most 1024 keys, including expired keys. Omitted expiration means no expiration; an explicit UTC timestamp must be in the future. Creation returns the credential and metadata, not just OK.

The result columns, in order, are `key_id`, `name`, `owner`, `created_at`, `expires_at`, `status`, and `api_key`. They are string metadata; expiration is NULL for a non-expiring key. Times are UTC `YYYY-MM-DD HH:MM:SS`; status is `active` or `expired`. The complete api_key credential must be protected as a password.

## Transaction Boundary

This credential mutation implicitly commits preceding user DML. Catalog/envelope publication is atomic; a later ordinary ROLLBACK does not revoke or restore the credential change. Singular/plural SHOW only reads metadata and does not use that mutation boundary.

## Example

```sql
CREATE API KEY 'backend';
CREATE API KEY 'temporary' EXPIRES AT '2027-01-01 00:00:00';
```

## See Also

- [API key lifecycle](../../rest-api.md)
- [Account index](README.md)
