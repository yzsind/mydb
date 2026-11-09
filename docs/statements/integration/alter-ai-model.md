# `ALTER AI MODEL`

## Description

Replaces attributes of an AI model.

## Syntax

```ebnf
<alter-ai-model> ::= 'ALTER AI MODEL' <qualified-name> 'WITH CONFIG' <json-string>
```

## Behavior, Privileges, and Results

Requires global `AI_ADMIN`. AI catalog mutations implicitly commit the existing transaction and are rejected while outgoing MySQL binary logging is enabled. Native WAL replication carries encrypted metadata without repeating remote model calls. See [AI administration](../../ai.md#administration-and-visibility) for configuration schemas and dependency rules.

Model names are schema-scoped; an unqualified name uses the current database. Returns OK after validated publication. WITH CONFIG supplies a complete validated JSON configuration, not a JSON patch. See the provider/model examples and option definitions in [AI configuration](../../ai.md).

## Example

```sql
ALTER AI MODEL statement_examples.local_chat WITH CONFIG '{
  "kind":"text_generation",
  "remote_model":"qwen3:0.6b",
  "model_revision":"qwen3-local-v1",
  "capabilities":{"messages":true,"temperature":true,"json_schema":true},
  "defaults":{"max_output_tokens":256,"provider_options":{"think":false}},
  "provider_options_allowed":["think"]
}';
```

## See Also

- [AI configuration and examples](../../ai.md)
- [AI and HTTP index](README.md)
