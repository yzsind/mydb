# `ALTER SYSTEM RELOAD AI CONFIG`

## Description

Updates node-local external-call configuration or key wrapping.

## Syntax

```ebnf
<reload-ai-config> ::= 'ALTER SYSTEM RELOAD AI CONFIG'
```

## Behavior, Privileges, and Results

Requires global `AI_ADMIN`. Returns OK. Validates and atomically replaces the node-local AI policy. A failed reload preserves the old policy. It does not edit catalog model configurations or replay requests.

## Example

```sql
ALTER SYSTEM RELOAD AI CONFIG;
```

## See Also

- [AI deployment](../../ai.md)
- [HTTP node policy](../../http.md#node-configuration)
- [Integration index](README.md)
