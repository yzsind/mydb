# `CREATE AI CONNECTION`

## Description

Creates an AI connection.

## Syntax

```ebnf
<create-ai-connection> ::= 'CREATE AI CONNECTION' [ 'IF NOT EXISTS' ] <identifier> 'WITH CONFIG' <json-string>
```

## Behavior, Privileges, and Results

Requires global `AI_ADMIN`. AI catalog mutations implicitly commit the existing transaction and are rejected while outgoing MySQL binary logging is enabled. Native WAL replication carries encrypted metadata without repeating remote model calls. See [AI administration](../../ai.md#administration-and-visibility) for configuration schemas and dependency rules.

Names are instance-scoped; a schema-qualified Secret or Connection name is rejected. Returns OK after validated publication. WITH CONFIG supplies a complete validated JSON configuration, not a JSON patch. See the provider/model examples and option definitions in [AI configuration](../../ai.md).

## Example

```sql
CREATE AI CONNECTION local_models WITH CONFIG '{
  "protocol":"ollama",
  "base_url":"http://127.0.0.1:11434",
  "auth":{"type":"none"}
}';
```

## See Also

- [AI configuration and examples](../../ai.md)
- [AI and HTTP index](README.md)
