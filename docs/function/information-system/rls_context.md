# `RLS_CONTEXT`

## Description

Reads a trusted row-security context key from the current connection.

## Syntax

```sql
RLS_CONTEXT(key)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `key` | Yes | String or convertible scalar | Context key; use literal keys in policy expressions |

## Return Value

Returns nullable text. A missing key returns SQL NULL; cast a present value to the
business type required by the policy. Reading context does not authenticate a
business tenant and does not itself bypass row security.

## Examples

A connection with no tenant_id context returns NULL:

```sql
SELECT RLS_CONTEXT('tenant_id');
```

Result:

```text
NULL
```

## Notes

- Context assignment/reset requires RLS_CONTEXT_ADMIN. A plain SQL user variable does not set trusted context.
- SET RLS CONTEXT is session-scoped; SET LOCAL RLS CONTEXT requires an explicit transaction and is restored on transaction completion or savepoint rollback. Connection reset/reauthentication clears context.
- See [Row-level security](../../row-level-security.md#connection-pools) for authorization, pooled connections, context statements and the current RLS validation status.

## Related Functions

[`CURRENT_USER`](current_user.md), [`CURRENT_ROLE`](current_role.md).
