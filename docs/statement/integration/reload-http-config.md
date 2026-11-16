# `ALTER SYSTEM RELOAD HTTP CONFIG`

## Description

Updates node-local external-call configuration or key wrapping.

## Syntax

```ebnf
<reload-http-config> ::= 'ALTER SYSTEM RELOAD HTTP CONFIG'
```

## Behavior, Privileges, and Results

Requires global `HTTP_ADMIN`. Returns OK. Validates and atomically replaces node-local HTTP policy. Failure retains the previous policy. Successful reload cancels local queued/in-flight waits without resending; it cannot revoke an operation already received by an upstream service.

## Example

```sql
ALTER SYSTEM RELOAD HTTP CONFIG;
```

## See Also

- [AI deployment](../../ai.md)
- [HTTP node policy](../../http.md#node-configuration)
- [Integration index](README.md)
