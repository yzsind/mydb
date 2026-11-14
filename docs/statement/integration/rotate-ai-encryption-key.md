# `ALTER SYSTEM ROTATE AI ENCRYPTION KEY`

## Description

Updates node-local external-call configuration or key wrapping.

## Syntax

```ebnf
<rotate-ai-encryption-key> ::= 'ALTER SYSTEM ROTATE AI ENCRYPTION KEY'
```

## Behavior, Privileges, and Results

Requires global `AI_ADMIN`. Returns OK. Atomically rewraps AI data-encryption keys using the provisioned candidate. Requires old decryption keys; retain keys needed by backups. This is catalog administration with an implicit commit and is rejected while outgoing MySQL binary logging is enabled.

## Example

```sql
ALTER SYSTEM ROTATE AI ENCRYPTION KEY;
```

## See Also

- [AI deployment](../../ai.md)
- [HTTP node policy](../../http.md#node-configuration)
- [Integration index](README.md)
