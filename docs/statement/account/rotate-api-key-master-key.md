# `ALTER INSTANCE ROTATE API KEY MASTER KEY`

## Description

Rewraps encrypted API credentials under the configured active REST master key.

## Syntax

```ebnf
<rotate-api-key-master-key> ::= 'ALTER INSTANCE ROTATE API KEY MASTER KEY'
```

## Behavior, Privileges, and Results

Requires TLS, a top-level authenticated MySQL session, and global `API_KEY_ADMIN`. Provision the new candidate and retain previous keys before rotation. Returns OK after atomic rewrapping; API key strings and digests do not change. Keep keys needed by older backups.

Credential metadata follows WAL, native replication, and physical backup. Master keys must be provisioned separately; API Key administration is not exported as MySQL logical binlog DDL.

## Transaction Boundary

This credential mutation implicitly commits preceding user DML. Catalog/envelope publication is atomic; a later ordinary ROLLBACK does not revoke or restore the credential change. Singular/plural SHOW only reads metadata and does not use that mutation boundary.

## Example

```sql
ALTER INSTANCE ROTATE API KEY MASTER KEY;
```

## See Also

- [API key lifecycle](../../rest-api.md)
- [Account index](README.md)
