# `PS_THREAD_ID`

## Description

Returns a supplied connection identity through the compatibility helper.

## Syntax

```sql
PS_THREAD_ID(connection_id)
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `connection_id` | Yes | Integer | Target connection ID; unknown/invalid IDs return NULL. |

## Return Value

Returns the supplied ID as an unsigned integer when it identifies a live registered connection, or SQL NULL for an unknown/out-of-range ID or SQL NULL input. Exactly one argument is required. In this implementation the returned thread identity equals the connection ID.

## Examples

```sql
SELECT PS_THREAD_ID(CONNECTION_ID()) = PS_CURRENT_THREAD_ID();
```

Result:

```text
1
```

## Notes

- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`CONNECTION_ID`](connection_id.md), [`PS_CURRENT_THREAD_ID`](ps_current_thread_id.md), [`PS_THREAD_ACCOUNT`](ps_thread_account.md).
