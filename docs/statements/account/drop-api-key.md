# `DROP API KEY`

## Description

Revokes and removes an existing credential.

## Syntax

```ebnf
<drop-api-key> ::= 'DROP API KEY' <string-literal> [ 'FOR' <account> ]
```

## Behavior, Privileges, and Results

Only top-level authenticated MySQL sessions can administer API keys. Accounts manage their own keys; a FOR clause targeting another account requires global `API_KEY_ADMIN`, and system-account protection also applies. Roles and anonymous accounts cannot own keys. REST SQL and stored programs cannot administer keys. See [REST credentials](../../rest-api.md#create-list-reveal-and-revoke-api-keys) for encryption, revocation, and transport requirements.

Returns OK. The removed key no longer authenticates; affected REST sessions are closed and their running work is cancelled. Already committed work is retained. Dropping releases one slot in the owner's 1024-key quota. No IF EXISTS clause is defined.

## Transaction Boundary

This credential mutation implicitly commits preceding user DML. Catalog/envelope publication is atomic; a later ordinary ROLLBACK does not revoke or restore the credential change. Singular/plural SHOW only reads metadata and does not use that mutation boundary.

## Example

```sql
DROP API KEY 'temporary';
```

## See Also

- [API key lifecycle](../../rest-api.md)
- [Account index](README.md)
