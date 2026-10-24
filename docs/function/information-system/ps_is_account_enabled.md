# `PS_IS_ACCOUNT_ENABLED`

## Description

Returns the current account-instrumentation compatibility response.

## Syntax

```sql
PS_IS_ACCOUNT_ENABLED(user [, host [, ...]])
```

## Parameters

| Parameter | Required | Accepted Types | Description |
| --- | --- | --- | --- |
| `user` | Yes | An SQL expression matching the function semantics | The 1st argument; the meaning is determined by the function syntax and description |
| `host ...` | No, repeatable | An SQL expression matching the function semantics | The 2nd argument; the meaning is determined by the function syntax and description |

## Return Value

Returns the string `YES` after evaluating its arguments. The current helper does not look up the supplied account in setup_actors and does not prove that a named account exists or is instrumented.

## Examples

```sql
SELECT PS_IS_ACCOUNT_ENABLED('root', 'localhost');
```

Result:

```text
YES
```

## Notes

- At least one argument is required. All supplied expressions are evaluated, but their values are not validated as existing accounts or instrumentation settings.

## Related Functions

This function has no direct alias in this category; see the category index for related functions.
