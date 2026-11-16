# `ALTER AI CONNECTION`

## Description

Replaces attributes of an AI connection.

## Syntax

```ebnf
<alter-ai-connection> ::= 'ALTER AI CONNECTION' <identifier> 'WITH CONFIG' <json-string>
```

## Behavior, Privileges, and Results

Requires global `AI_ADMIN`. AI catalog mutations implicitly commit the existing transaction and are rejected while outgoing MySQL binary logging is enabled. Native WAL replication carries encrypted metadata without repeating remote model calls. See [AI administration](../../ai.md#administration-and-visibility) for configuration schemas and dependency rules.

Names are instance-scoped; a schema-qualified Secret or Connection name is rejected. Returns OK after validated publication. WITH CONFIG supplies a complete validated JSON configuration, not a JSON patch. See the provider/model examples and option definitions in [AI configuration](../../ai.md). Changing the connection cancels queued and in-flight local waits; an upstream request already received can still execute and incur charges.

## Example

```sql
ALTER AI CONNECTION local_models WITH CONFIG '{
  "protocol":"ollama",
  "base_url":"http://127.0.0.1:11434",
  "auth":{"type":"none"}
}';
```

## See Also

- [AI configuration and examples](../../ai.md)
- [AI and HTTP index](README.md)
