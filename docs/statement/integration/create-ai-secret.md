# `CREATE AI SECRET`

## Description

Creates an AI secret.

## Syntax

```ebnf
<create-ai-secret> ::= 'CREATE AI SECRET' [ 'IF NOT EXISTS' ] <identifier> 'VALUE' ( <string-literal> | '?' )
```

## Behavior, Privileges, and Results

Requires global `AI_ADMIN`. AI catalog mutations implicitly commit the existing transaction and are rejected while outgoing MySQL binary logging is enabled. Native WAL replication carries encrypted metadata without repeating remote model calls. See [AI administration](../../ai.md#administration-and-visibility) for configuration schemas and dependency rules.

Names are instance-scoped; a schema-qualified Secret or Connection name is rejected. Returns OK after validated publication. Secret VALUE requires TLS; prefer a prepared parameter to avoid embedding a provider credential in client code. The server stores an encrypted value and never returns plaintext through SHOW. Rotation changes the generation used by subsequent calls.

## Example

```sql
PREPARE change_key FROM 'CREATE AI SECRET provider_key VALUE ?';
-- Bind @provider_key securely in a TLS session before executing.
EXECUTE change_key USING @provider_key;
DEALLOCATE PREPARE change_key;
```

## See Also

- [AI configuration and examples](../../ai.md)
- [AI and HTTP index](README.md)
