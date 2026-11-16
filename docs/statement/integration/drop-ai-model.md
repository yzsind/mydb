# `DROP AI MODEL`

## Description

Drops an AI model.

## Syntax

```ebnf
<drop-ai-model> ::= 'DROP AI MODEL' [ 'IF EXISTS' ] <qualified-name>
```

## Behavior, Privileges, and Results

Requires global `AI_ADMIN`. AI catalog mutations implicitly commit the existing transaction and are rejected while outgoing MySQL binary logging is enabled. Native WAL replication carries encrypted metadata without repeating remote model calls. See [AI administration](../../ai.md#administration-and-visibility) for configuration schemas and dependency rules.

Model names are schema-scoped; an unqualified name uses the current database. Returns OK after validated publication. Dropping retires model-specific grants; recreating the name does not restore those grants.

## Example

```sql
DROP AI MODEL IF EXISTS statement_examples.local_chat;
```

## See Also

- [AI configuration and examples](../../ai.md)
- [AI and HTTP index](README.md)
